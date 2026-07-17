from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
import sys
from typing import Any, Literal, Never, cast
from uuid import UUID

import pytest
from pydantic import JsonValue

from cmo_agent_bridge.errors import ErrorCode
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.lua_delivery import render_idle_lua
from cmo_agent_bridge.protocol.models import ExchangeCommand, PreparedDelivery, ResponseExpectation
from cmo_agent_bridge.protocol.response import parse_inst_response
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CompletedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
)
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    PendingJournal,
    PendingPhase,
)
from cmo_agent_bridge.state.pending_journal import (
    JournalDeleteExpectation,
    JournalRevisions,
)
from cmo_agent_bridge.state.request_ledger import DeliveryRecord, RequestRecord
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
import cmo_agent_bridge.transports.file_bridge.recovery as recovery_module

sys.path.insert(0, str(Path(__file__).parent))

import test_recovery_contract as base  # noqa: E402


harness = base.harness

_ArtifactCase = Literal[
    "missing",
    "hash_mismatch",
    "size_mismatch",
    "accepted_response_mismatch",
    "valid",
]


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _raw_response(envelope: ResponseEnvelope) -> bytes:
    inner = _canonical_json_bytes(envelope.model_dump(mode="json"))
    return _canonical_json_bytes({"Comments": inner.decode("utf-8", errors="strict")})


def _result(command: ExchangeCommand, *, name: str) -> JsonValue:
    validated = command.invocation.result_adapter.validate_python(
        {
            "unit_guid": "UNIT-1",
            "name": name,
            "speed": None,
            "altitude": None,
            "heading": None,
            "course": None,
        }
    )
    return cast(
        JsonValue,
        command.invocation.result_adapter.dump_python(validated, mode="json"),
    )


def _raw_with_different_accepted_response(
    command: ExchangeCommand,
    artifact: ResponseArtifact,
) -> bytes:
    envelope_tree = artifact.accepted_response.envelope.model_dump(
        mode="python",
        round_trip=True,
        warnings=False,
    )
    envelope_tree["result"] = _result(command, name="Different durable response")
    return _raw_response(ResponseEnvelope.model_validate(envelope_tree))


def _request_record(
    fixture: base.Harness,
    command: ExchangeCommand,
    *,
    request_hash: str,
) -> RequestRecord:
    recovery_schema = command.invocation.recovery_schema
    return RequestRecord(
        request_id=command.request_id,
        root_key=fixture.paths.root_key,
        request_hash=request_hash,
        operation=command.body.operation,
        operation_class=command.invocation.effective_class,
        state=HostRequestState.PREPARED,
        runtime_snapshot=command.runtime_snapshot,
        result_schema_id=command.invocation.result_schema.schema_id,
        recovery_schema_id=(None if recovery_schema is None else recovery_schema.schema_id),
        body_json=canonical_body_bytes(command.body),
        lineage_id=command.body.expected_lineage_id,
        activation_id=command.body.expected_activation_id,
        result_json=None,
        error_json=None,
        resolution_json=None,
        created_at_ms=100,
        updated_at_ms=100,
        terminal_at_ms=None,
    )


def _save(
    fixture: base.Harness,
    journal: PendingJournal,
    predecessor: PendingJournal | None,
) -> None:
    expected = (
        None
        if predecessor is None
        else JournalRevisions(
            original=predecessor.original.revision,
            reconcile_attempt=None,
        )
    )
    assert fixture.journals.save(journal, expected_revisions=expected) == JournalRevisions(
        original=journal.original.revision,
        reconcile_attempt=None,
    )


@dataclass(frozen=True, slots=True)
class _SeededR4:
    command: ExchangeCommand
    r4: PendingJournal
    artifact: ResponseArtifact
    response_path: Path
    actual_raw: bytes | None
    cancel_inbox: bytes
    request_delivery_id: UUID
    cancel_delivery_id: UUID


def _seed_response_accepted_r4(
    fixture: base.Harness,
    *,
    case: _ArtifactCase,
) -> _SeededR4:
    fixture.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    command = base._command(fixture)  # pyright: ignore[reportPrivateUsage]
    r0, r1, r2, _unused_response = base._precondition_journals(  # pyright: ignore[reportPrivateUsage]
        fixture
    )
    request_intent = r2.original.delivery_intents[0]
    cancel_intent = r2.original.delivery_intents[1]
    assert request_intent.delivery_kind == "request"
    assert request_intent.published_at_ms == 101
    assert cancel_intent.delivery_kind == "cancel"
    assert cancel_intent.published_at_ms is None

    published_cancel = DeliveryIntent.model_validate(
        {
            **cancel_intent.model_dump(
                mode="python",
                round_trip=True,
                warnings=False,
            ),
            "published_at_ms": 103,
        }
    )
    r3 = base._journal_with_original(  # pyright: ignore[reportPrivateUsage]
        r2,
        delivery_intents=(request_intent, published_cancel),
        revision=3,
        state=PendingPhase.CANCEL_PUBLISHED,
        updated_at_ms=103,
    )

    semantic_artifact = base._completed_artifact(command).model_copy(  # pyright: ignore[reportPrivateUsage]
        update={"accepted_at_ms": 104}
    )
    matching_raw = _raw_response(semantic_artifact.accepted_response.envelope)
    if case == "accepted_response_mismatch":
        metadata_raw = _raw_with_different_accepted_response(command, semantic_artifact)
    else:
        metadata_raw = matching_raw
    artifact = semantic_artifact.model_copy(
        update={
            "sha256": hashlib.sha256(metadata_raw).hexdigest(),
            "size_bytes": len(metadata_raw),
        }
    )
    r4 = base._journal_with_original(  # pyright: ignore[reportPrivateUsage]
        r3,
        response_artifact=artifact,
        settlement=artifact.accepted_response.settlement,
        revision=4,
        state=PendingPhase.RESPONSE_ACCEPTED,
        updated_at_ms=104,
    )

    fixture.ledger.insert_prepared(
        _request_record(
            fixture,
            command,
            request_hash=request_intent.request_hash,
        )
    )
    fixture.ledger.insert_delivery(r0.original.delivery_intents[0])
    _save(fixture, r0, None)
    _save(fixture, r1, r0)
    assert fixture.ledger.mark_delivery_published(
        request_intent.delivery_id,
        published_at_ms=101,
    ) == DeliveryRecord(
        delivery_id=request_intent.delivery_id,
        request_id=request_intent.request_id,
        delivery_kind=request_intent.delivery_kind,
        original_request_delivery_id=request_intent.original_request_delivery_id,
        intended_at_ms=request_intent.intended_at_ms,
        published_at_ms=request_intent.published_at_ms,
        rendered_inbox_sha256=request_intent.rendered_inbox_sha256,
        rendered_inbox_size_bytes=request_intent.rendered_inbox_size_bytes,
        response_filename=request_intent.response_filename,
        response_artifact=None,
        settlement=None,
    )
    fixture.ledger.transition(
        command.request_id,
        expected_states=frozenset({HostRequestState.PREPARED}),
        new_state=HostRequestState.PUBLISHED,
        updated_at_ms=101,
    )

    _save(fixture, r2, r1)
    fixture.ledger.insert_delivery(cancel_intent)
    cancel_delivery = PreparedDelivery(
        request_id=published_cancel.request_id,
        delivery_id=published_cancel.delivery_id,
        delivery_kind="cancel",
        request_hash=published_cancel.request_hash,
        body_json=published_cancel.body_json.encode("utf-8", errors="strict"),
    )
    fixture.inbox.publish_delivery(
        cancel_delivery,
        runtime_snapshot=command.runtime_snapshot,
    )
    cancel_inbox = fixture.paths.inbox.read_bytes()
    assert hashlib.sha256(cancel_inbox).hexdigest() == (published_cancel.rendered_inbox_sha256)
    assert len(cancel_inbox) == published_cancel.rendered_inbox_size_bytes
    _save(fixture, r3, r2)
    fixture.ledger.mark_delivery_published(
        published_cancel.delivery_id,
        published_at_ms=103,
    )
    fixture.ledger.transition(
        command.request_id,
        expected_states=frozenset({HostRequestState.PUBLISHED}),
        new_state=HostRequestState.CANCEL_PUBLISHED,
        updated_at_ms=103,
    )
    _save(fixture, r4, r3)

    response_path = fixture.paths.response_path(command.request_id)
    actual_raw: bytes | None
    if case == "missing":
        actual_raw = None
    elif case == "hash_mismatch":
        actual_raw = matching_raw.replace(
            b"2026-07-12T13:00:00Z",
            b"2026-07-12T13:00:01Z",
            1,
        )
        assert len(actual_raw) == artifact.size_bytes
        assert hashlib.sha256(actual_raw).hexdigest() != artifact.sha256
    elif case == "size_mismatch":
        actual_raw = matching_raw + b" "
        assert len(actual_raw) != artifact.size_bytes
    elif case == "accepted_response_mismatch":
        actual_raw = metadata_raw
        assert len(actual_raw) == artifact.size_bytes
        assert hashlib.sha256(actual_raw).hexdigest() == artifact.sha256
        loaded_for_expectation = fixture.journals.load()
        assert loaded_for_expectation is not None
        parsed = parse_inst_response(
            actual_raw,
            loaded_for_expectation.original.expectation,
        )
        assert parsed != artifact.accepted_response
    else:
        assert case == "valid"
        actual_raw = matching_raw
        assert len(actual_raw) == artifact.size_bytes
        assert hashlib.sha256(actual_raw).hexdigest() == artifact.sha256
    if actual_raw is not None:
        response_path.write_bytes(actual_raw)

    loaded = fixture.journals.load()
    assert loaded is not None
    assert loaded.journal == r4
    request = fixture.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.CANCEL_PUBLISHED
    for delivery_id in (request_intent.delivery_id, published_cancel.delivery_id):
        delivery = fixture.ledger.get_delivery(delivery_id)
        assert delivery is not None
        assert delivery.response_artifact is None
        assert delivery.settlement is None

    return _SeededR4(
        command=command,
        r4=r4,
        artifact=artifact,
        response_path=response_path,
        actual_raw=actual_raw,
        cancel_inbox=cancel_inbox,
        request_delivery_id=request_intent.delivery_id,
        cancel_delivery_id=published_cancel.delivery_id,
    )


def _forbidden(*_args: object, **_kwargs: object) -> Never:
    raise AssertionError("unsafe startup recovery side effect was reached")


def _new_byte_buffers() -> list[bytes]:
    return []


@dataclass(slots=True)
class _RawBufferProbe:
    expected_path: Path
    expected_raw: bytes | None
    read_buffers: list[bytes] = field(default_factory=_new_byte_buffers)
    parsed_buffers: list[bytes] = field(default_factory=_new_byte_buffers)


def _install_raw_buffer_probe(
    monkeypatch: pytest.MonkeyPatch,
    probe: _RawBufferProbe,
) -> None:
    path_type = type(probe.expected_path)
    original_read_bytes = path_type.read_bytes
    original_parse = parse_inst_response

    def traced_read_bytes(path: Path) -> bytes:
        if path != probe.expected_path:
            return original_read_bytes(path)
        if probe.expected_raw is None:
            raise FileNotFoundError(path)
        assert probe.read_buffers == [], "startup recovery reread the response artifact"
        probe.read_buffers.append(probe.expected_raw)
        return probe.expected_raw

    def traced_parse(
        raw: bytes,
        expectation: ResponseExpectation,
    ) -> AcceptedResponse:
        assert probe.read_buffers == [probe.expected_raw]
        assert raw is probe.read_buffers[0]
        probe.parsed_buffers.append(raw)
        return original_parse(raw, expectation)

    monkeypatch.setattr(path_type, "read_bytes", traced_read_bytes)
    # The wished-for startup implementation owns a module-level parser seam. Patching
    # it with raising=False keeps this RED test importable before recover_pending exists.
    monkeypatch.setattr(recovery_module, "parse_inst_response", traced_parse, raising=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        pytest.param("missing", id="missing-response"),
        pytest.param("hash_mismatch", id="same-size-hash-mismatch"),
        pytest.param("size_mismatch", id="size-mismatch"),
        pytest.param(
            "accepted_response_mismatch",
            id="matching-metadata-different-accepted-response",
        ),
    ],
)
async def test_startup_r4_requires_exact_raw_artifact_before_any_settlement_side_effect(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: Literal[
        "missing",
        "hash_mismatch",
        "size_mismatch",
        "accepted_response_mismatch",
    ],
) -> None:
    async with harness.lock:
        seeded = _seed_response_accepted_r4(harness, case=case)
        manager = base._manager(harness)  # pyright: ignore[reportPrivateUsage]
        probe = _RawBufferProbe(seeded.response_path, seeded.actual_raw)
        _install_raw_buffer_probe(monkeypatch, probe)

        transitions: list[HostRequestState] = []
        saved: list[PendingJournal] = []
        response_unlinks: list[Path] = []
        original_transition = harness.ledger.transition
        original_save = harness.journals.save
        path_type = type(seeded.response_path)
        original_unlink = path_type.unlink

        def quarantine_only_transition(
            request_id: UUID,
            *,
            expected_states: frozenset[HostRequestState],
            new_state: HostRequestState,
            updated_at_ms: int,
            terminal_at_ms: int | None = None,
            result_json: str | None = None,
            error_json: str | None = None,
            resolution_json: str | None = None,
        ) -> RequestRecord:
            assert request_id == seeded.command.request_id
            assert new_state is HostRequestState.QUARANTINED
            assert terminal_at_ms is None
            transitions.append(new_state)
            return original_transition(
                request_id,
                expected_states=expected_states,
                new_state=new_state,
                updated_at_ms=updated_at_ms,
                terminal_at_ms=terminal_at_ms,
                result_json=result_json,
                error_json=error_json,
                resolution_json=resolution_json,
            )

        def quarantine_only_save(
            journal: PendingJournal,
            *,
            expected_revisions: JournalRevisions | None,
        ) -> JournalRevisions:
            assert journal.original.revision == 5
            assert journal.original.state is PendingPhase.QUARANTINED
            assert expected_revisions == JournalRevisions(
                original=4,
                reconcile_attempt=None,
            )
            saved.append(journal)
            return original_save(journal, expected_revisions=expected_revisions)

        def forbid_response_unlink(path: Path, missing_ok: bool = False) -> None:
            if path == seeded.response_path:
                response_unlinks.append(path)
                raise AssertionError("unbound startup response was queued for cleanup")
            original_unlink(path, missing_ok=missing_ok)

        monkeypatch.setattr(harness.ledger, "record_response", _forbidden)
        monkeypatch.setattr(harness.ledger, "transition", quarantine_only_transition)
        monkeypatch.setattr(harness.journals, "save", quarantine_only_save)
        monkeypatch.setattr(harness.journals, "delete", _forbidden)
        monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
        monkeypatch.setattr(path_type, "unlink", forbid_response_unlink)

        await cast(Any, manager).recover_pending()

        loaded = harness.journals.load()
        assert loaded is not None
        assert len(saved) == 1
        assert loaded.journal == saved[0]
        assert loaded.journal.original.state is PendingPhase.QUARANTINED
        assert loaded.journal.original.revision == 5
        assert loaded.journal.original.response_artifact == seeded.artifact
        assert loaded.journal.original.settlement == (seeded.artifact.accepted_response.settlement)
        request = harness.ledger.get_request(seeded.command.request_id)
        assert request is not None
        assert request.state is HostRequestState.QUARANTINED
        assert request.terminal_at_ms is None
        assert request.result_json is None
        assert request.error_json is not None
        error_payload = json.loads(request.error_json)
        assert error_payload["code"] == ErrorCode.INDETERMINATE_OUTCOME.value
        assert error_payload["details"]["request_id"] == str(seeded.command.request_id)
        assert transitions == [HostRequestState.QUARANTINED]
        for delivery_id in (seeded.request_delivery_id, seeded.cancel_delivery_id):
            delivery = harness.ledger.get_delivery(delivery_id)
            assert delivery is not None
            assert delivery.response_artifact is None
            assert delivery.settlement is None
        assert harness.paths.inbox.read_bytes() == seeded.cancel_inbox
        assert response_unlinks == []
        assert seeded.response_path.exists() is (case != "missing")

        if case == "missing":
            assert probe.read_buffers == []
            assert probe.parsed_buffers == []
        elif case in {"hash_mismatch", "size_mismatch"}:
            assert probe.read_buffers == [seeded.actual_raw]
            assert probe.parsed_buffers == []
        else:
            assert case == "accepted_response_mismatch"
            assert probe.read_buffers == [seeded.actual_raw]
            assert probe.parsed_buffers == [probe.read_buffers[0]]
            assert probe.parsed_buffers[0] is probe.read_buffers[0]


@pytest.mark.asyncio
async def test_startup_r4_valid_raw_buffer_is_parsed_once_and_resumes_exact_settlement(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with harness.lock:
        seeded = _seed_response_accepted_r4(harness, case="valid")
        manager = base._manager(harness)  # pyright: ignore[reportPrivateUsage]
        probe = _RawBufferProbe(seeded.response_path, seeded.actual_raw)
        _install_raw_buffer_probe(monkeypatch, probe)

        recorded: list[ResponseArtifact] = []
        transitions: list[RequestRecord] = []
        deleted: list[JournalDeleteExpectation] = []
        idle_publications: list[bytes] = []
        original_record = harness.ledger.record_response
        original_transition = harness.ledger.transition
        original_delete = harness.journals.delete
        original_idle = InboxPublisher.publish_idle

        def traced_record(artifact: ResponseArtifact) -> DeliveryRecord:
            recorded.append(artifact)
            return original_record(artifact)

        def traced_transition(
            request_id: UUID,
            *,
            expected_states: frozenset[HostRequestState],
            new_state: HostRequestState,
            updated_at_ms: int,
            terminal_at_ms: int | None = None,
            result_json: str | None = None,
            error_json: str | None = None,
            resolution_json: str | None = None,
        ) -> RequestRecord:
            result = original_transition(
                request_id,
                expected_states=expected_states,
                new_state=new_state,
                updated_at_ms=updated_at_ms,
                terminal_at_ms=terminal_at_ms,
                result_json=result_json,
                error_json=error_json,
                resolution_json=resolution_json,
            )
            transitions.append(result)
            return result

        def traced_delete(expected: JournalDeleteExpectation) -> None:
            original_delete(expected)
            deleted.append(expected)

        def traced_idle(publisher: InboxPublisher) -> None:
            original_idle(publisher)
            idle_publications.append(harness.paths.inbox.read_bytes())

        monkeypatch.setattr(harness.ledger, "record_response", traced_record)
        monkeypatch.setattr(harness.ledger, "transition", traced_transition)
        monkeypatch.setattr(harness.journals, "delete", traced_delete)
        monkeypatch.setattr(InboxPublisher, "publish_idle", traced_idle)

        await cast(Any, manager).recover_pending()

        assert probe.read_buffers == [seeded.actual_raw]
        assert probe.parsed_buffers == [probe.read_buffers[0]]
        assert probe.parsed_buffers[0] is probe.read_buffers[0]
        assert recorded == [seeded.artifact]
        responding = harness.ledger.get_delivery(seeded.request_delivery_id)
        assert responding is not None
        assert responding.response_artifact == seeded.artifact
        assert responding.settlement == seeded.artifact.accepted_response.settlement
        nonresponding = harness.ledger.get_delivery(seeded.cancel_delivery_id)
        assert nonresponding is not None
        assert nonresponding.response_artifact is None
        assert nonresponding.settlement is None
        assert [record.state for record in transitions] == [
            HostRequestState.RESPONSE_ACCEPTED,
            HostRequestState.IDLE_PUBLISHED,
            HostRequestState.COMPLETED,
        ]
        terminal = transitions[-1]
        settlement = seeded.artifact.accepted_response.settlement
        assert isinstance(settlement, CompletedSettlement)
        assert terminal.result_json == _canonical_json_bytes(settlement.result).decode("utf-8")
        assert terminal.error_json is None
        assert terminal.resolution_json is None
        assert terminal.terminal_at_ms == terminal.updated_at_ms
        assert idle_publications == [render_idle_lua()]
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        assert deleted == [
            JournalDeleteExpectation(
                root_key=harness.paths.root_key,
                required_release_id=seeded.command.runtime_snapshot.release_id,
                original_request_id=seeded.command.request_id,
                reconcile_attempt_request_id=None,
                revisions=JournalRevisions(original=5, reconcile_attempt=None),
            )
        ]
        assert harness.journals.load() is None
