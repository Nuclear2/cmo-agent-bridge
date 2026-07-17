from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Literal, Never, cast
from uuid import UUID

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.models import (
    AllowedDelivery,
    PreparedDelivery,
    ResponseExpectation,
)
from cmo_agent_bridge.protocol.response_models import AcceptedResponse, ResponseArtifact
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
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
from cmo_agent_bridge.transports.file_bridge.response_waiter import ResponseWaiter
import cmo_agent_bridge.transports.file_bridge.recovery as recovery_module
import cmo_agent_bridge.transports.file_bridge.response_waiter as waiter_module

sys.path.insert(0, str(Path(__file__).parent))

import test_recovery_contract as base  # noqa: E402
import test_startup_recovery_artifact_binding as artifact_base  # noqa: E402
import test_startup_recovery_prepared_published as early  # noqa: E402


harness = base.harness


@dataclass(frozen=True, slots=True)
class _SeededC3Lag:
    command_request_id: UUID
    r3: PendingJournal
    cancel_delivery_id: UUID
    cancel_inbox: bytes


@dataclass(frozen=True, slots=True)
class _SeededIdle:
    artifact_seed: artifact_base._SeededR4  # pyright: ignore[reportPrivateUsage]
    r5: PendingJournal


def _disposition(report: object) -> str:
    raw = getattr(report, "disposition", None)
    assert raw is not None
    value = getattr(raw, "value", raw)
    assert type(value) is str
    return value


def _published_cancel_journal(r2: PendingJournal) -> PendingJournal:
    request_intent, cancel_intent = r2.original.delivery_intents
    assert request_intent.delivery_kind == "request"
    assert request_intent.published_at_ms is not None
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
    return base._journal_with_original(  # pyright: ignore[reportPrivateUsage]
        r2,
        delivery_intents=(request_intent, published_cancel),
        revision=3,
        state=PendingPhase.CANCEL_PUBLISHED,
        updated_at_ms=103,
    )


def _seed_c3_with_sqlite_lag(fixture: base.Harness) -> _SeededC3Lag:
    fixture.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    command = base._command(fixture)  # pyright: ignore[reportPrivateUsage]
    r0, r1, r2, _response = base._precondition_journals(  # pyright: ignore[reportPrivateUsage]
        fixture
    )
    r3 = _published_cancel_journal(r2)
    request_intent, published_cancel = r3.original.delivery_intents
    unpublished_cancel = r2.original.delivery_intents[1]

    fixture.ledger.insert_prepared(
        artifact_base._request_record(  # pyright: ignore[reportPrivateUsage]
            fixture,
            command,
            request_hash=request_intent.request_hash,
        )
    )
    fixture.ledger.insert_delivery(r0.original.delivery_intents[0])
    artifact_base._save(fixture, r0, None)  # pyright: ignore[reportPrivateUsage]
    artifact_base._save(fixture, r1, r0)  # pyright: ignore[reportPrivateUsage]
    fixture.ledger.mark_delivery_published(
        request_intent.delivery_id,
        published_at_ms=cast(int, request_intent.published_at_ms),
    )
    fixture.ledger.transition(
        command.request_id,
        expected_states=frozenset({HostRequestState.PREPARED}),
        new_state=HostRequestState.PUBLISHED,
        updated_at_ms=r1.original.updated_at_ms,
    )
    artifact_base._save(fixture, r2, r1)  # pyright: ignore[reportPrivateUsage]
    fixture.ledger.insert_delivery(unpublished_cancel)

    cancel_delivery = PreparedDelivery(
        request_id=published_cancel.request_id,
        delivery_id=published_cancel.delivery_id,
        delivery_kind="cancel",
        request_hash=published_cancel.request_hash,
        body_json=published_cancel.body_json.encode("utf-8", errors="strict"),
    )
    fixture.inbox.publish_delivery(
        cancel_delivery,
        runtime_snapshot=published_cancel.runtime_snapshot,
    )
    cancel_inbox = fixture.paths.inbox.read_bytes()
    artifact_base._save(fixture, r3, r2)  # pyright: ignore[reportPrivateUsage]

    observed_cancel = fixture.ledger.get_delivery(published_cancel.delivery_id)
    assert observed_cancel is not None
    assert observed_cancel.published_at_ms is None
    observed_request = fixture.ledger.get_request(command.request_id)
    assert observed_request is not None
    assert observed_request.state is HostRequestState.PUBLISHED
    return _SeededC3Lag(
        command_request_id=command.request_id,
        r3=r3,
        cancel_delivery_id=published_cancel.delivery_id,
        cancel_inbox=cancel_inbox,
    )


def _advance_seed_to_response_aftercommit(
    fixture: base.Harness,
    seeded: artifact_base._SeededR4,  # pyright: ignore[reportPrivateUsage]
) -> RequestRecord:
    fixture.ledger.record_response(seeded.artifact)
    return fixture.ledger.transition(
        seeded.command.request_id,
        expected_states=frozenset({HostRequestState.CANCEL_PUBLISHED}),
        new_state=HostRequestState.RESPONSE_ACCEPTED,
        updated_at_ms=seeded.r4.original.updated_at_ms,
    )


def _seed_idle_with_response_request(fixture: base.Harness) -> _SeededIdle:
    seeded = artifact_base._seed_response_accepted_r4(  # pyright: ignore[reportPrivateUsage]
        fixture,
        case="valid",
    )
    response_request = _advance_seed_to_response_aftercommit(fixture, seeded)
    assert response_request.state is HostRequestState.RESPONSE_ACCEPTED
    fixture.inbox.publish_idle()
    r5 = base._journal_with_original(  # pyright: ignore[reportPrivateUsage]
        seeded.r4,
        revision=5,
        state=PendingPhase.IDLE_PUBLISHED,
        updated_at_ms=105,
    )
    artifact_base._save(fixture, r5, seeded.r4)  # pyright: ignore[reportPrivateUsage]
    return _SeededIdle(artifact_seed=seeded, r5=r5)


def _install_response_waiter_raw_probe(
    monkeypatch: pytest.MonkeyPatch,
    *,
    response_path: Path,
    expected_raw: bytes,
) -> tuple[list[bytes], list[bytes]]:
    path_type = type(response_path)
    original_read = path_type.read_bytes
    original_parse = waiter_module.parse_inst_response
    reads: list[bytes] = []
    parses: list[bytes] = []

    def traced_read(path: Path) -> bytes:
        if path != response_path:
            return original_read(path)
        assert reads == []
        reads.append(expected_raw)
        return expected_raw

    def traced_parse(
        raw: bytes,
        expectation: ResponseExpectation,
    ) -> AcceptedResponse:
        assert raw is reads[0]
        parses.append(raw)
        return original_parse(raw, expectation)

    monkeypatch.setattr(path_type, "read_bytes", traced_read)
    monkeypatch.setattr(recovery_module, "parse_inst_response", traced_parse)
    monkeypatch.setattr(waiter_module, "parse_inst_response", traced_parse)
    return reads, parses


@pytest.mark.asyncio
async def test_startup_c3_converges_cancel_sqlite_lag_before_dual_wait_and_timeout_quarantine(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace: list[str] = []
    async with harness.lock:
        seeded = _seed_c3_with_sqlite_lag(harness)
        manager = base._manager(harness)  # pyright: ignore[reportPrivateUsage]
        original_mark = harness.ledger.mark_delivery_published
        original_transition = harness.ledger.transition

        def traced_mark(delivery_id: UUID, *, published_at_ms: int) -> DeliveryRecord:
            result = original_mark(delivery_id, published_at_ms=published_at_ms)
            assert delivery_id == seeded.cancel_delivery_id
            trace.append("cancel.marker")
            return result

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
            trace.append(f"request.{new_state.value}")
            return result

        async def timeout(waiter: ResponseWaiter, timeout_seconds: float) -> ResponseArtifact:
            cancel = harness.ledger.get_delivery(seeded.cancel_delivery_id)
            request = harness.ledger.get_request(seeded.command_request_id)
            assert cancel is not None and cancel.published_at_ms is not None
            assert request is not None
            assert request.state is HostRequestState.CANCEL_PUBLISHED
            allowed = cast(ResponseExpectation, getattr(waiter, "_expectation")).allowed_deliveries
            assert allowed == tuple(
                AllowedDelivery(intent.delivery_id, intent.delivery_kind)
                for intent in seeded.r3.original.delivery_intents
            )
            trace.append("dual.wait")
            raise BridgeError(
                ErrorCode.REQUEST_TIMEOUT,
                "late-phase test timeout",
                {"timeout_seconds": timeout_seconds},
            )

        monkeypatch.setattr(harness.ledger, "mark_delivery_published", traced_mark)
        monkeypatch.setattr(harness.ledger, "transition", traced_transition)
        monkeypatch.setattr(ResponseWaiter, "wait", timeout)

        report = await manager.recover_pending()
        loaded = harness.journals.load()

        assert _disposition(report) == "quarantined"
        assert loaded is not None
        assert loaded.journal.original.state is PendingPhase.QUARANTINED
        assert loaded.journal.original.revision == 4
        assert harness.paths.inbox.read_bytes() == seeded.cancel_inbox

    assert trace.index("cancel.marker") < trace.index("request.cancel_published")
    assert trace.index("request.cancel_published") < trace.index("dual.wait")
    assert trace.index("dual.wait") < trace.index("request.quarantined")


@pytest.mark.asyncio
async def test_startup_response_accepted_aftercommit_is_idempotent_and_continues_settlement(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with harness.lock:
        seeded = artifact_base._seed_response_accepted_r4(  # pyright: ignore[reportPrivateUsage]
            harness,
            case="valid",
        )
        response_request = _advance_seed_to_response_aftercommit(harness, seeded)
        probe = artifact_base._RawBufferProbe(  # pyright: ignore[reportPrivateUsage]
            seeded.response_path,
            seeded.actual_raw,
        )
        artifact_base._install_raw_buffer_probe(  # pyright: ignore[reportPrivateUsage]
            monkeypatch,
            probe,
        )
        transitions: list[HostRequestState] = []
        original_transition = harness.ledger.transition

        def forbid_duplicate_record(_artifact: ResponseArtifact) -> Never:
            raise AssertionError("aftercommit response evidence was recorded twice")

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
            assert new_state is not HostRequestState.RESPONSE_ACCEPTED
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
            transitions.append(new_state)
            return result

        monkeypatch.setattr(harness.ledger, "record_response", forbid_duplicate_record)
        monkeypatch.setattr(harness.ledger, "transition", traced_transition)

        report = await base._manager(harness).recover_pending()  # pyright: ignore[reportPrivateUsage]

        assert _disposition(report) == "settled"
        assert getattr(report, "response_cleanup_required") is True
        assert transitions == [HostRequestState.IDLE_PUBLISHED, HostRequestState.COMPLETED]
        assert probe.read_buffers == [seeded.actual_raw]
        assert probe.parsed_buffers == [probe.read_buffers[0]]
        assert probe.parsed_buffers[0] is probe.read_buffers[0]
        assert harness.journals.load() is None
        assert response_request.state is HostRequestState.RESPONSE_ACCEPTED


@pytest.mark.asyncio
async def test_startup_idle_published_resumes_response_request_to_terminal_and_deletes(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with harness.lock:
        seeded = _seed_idle_with_response_request(harness)
        probe = artifact_base._RawBufferProbe(  # pyright: ignore[reportPrivateUsage]
            seeded.artifact_seed.response_path,
            seeded.artifact_seed.actual_raw,
        )
        artifact_base._install_raw_buffer_probe(  # pyright: ignore[reportPrivateUsage]
            monkeypatch,
            probe,
        )
        transitions: list[HostRequestState] = []
        deletes: list[JournalDeleteExpectation] = []
        original_transition = harness.ledger.transition
        original_delete = harness.journals.delete

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
            transitions.append(new_state)
            return result

        def traced_delete(expected: JournalDeleteExpectation) -> None:
            original_delete(expected)
            deletes.append(expected)

        monkeypatch.setattr(harness.ledger, "transition", traced_transition)
        monkeypatch.setattr(harness.journals, "delete", traced_delete)

        report = await base._manager(harness).recover_pending()  # pyright: ignore[reportPrivateUsage]

        assert _disposition(report) == "settled"
        assert transitions == [HostRequestState.IDLE_PUBLISHED, HostRequestState.COMPLETED]
        assert deletes == [
            JournalDeleteExpectation(
                root_key=harness.paths.root_key,
                required_release_id=seeded.r5.header.required_release_id,
                original_request_id=seeded.r5.original.request_id,
                reconcile_attempt_request_id=None,
                revisions=JournalRevisions(original=5, reconcile_attempt=None),
            )
        ]
        assert probe.read_buffers == [seeded.artifact_seed.actual_raw]
        assert probe.parsed_buffers == [probe.read_buffers[0]]
        assert probe.parsed_buffers[0] is probe.read_buffers[0]
        assert harness.journals.load() is None


@pytest.mark.parametrize("raw_case", ["missing", "mismatch"])
@pytest.mark.asyncio
async def test_startup_idle_unbound_raw_quarantines_without_delete_cleanup_or_inbox_overwrite(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
    raw_case: Literal["missing", "mismatch"],
) -> None:
    async with harness.lock:
        seeded = _seed_idle_with_response_request(harness)
        response_path = seeded.artifact_seed.response_path
        matching_raw = seeded.artifact_seed.actual_raw
        assert matching_raw is not None
        if raw_case == "missing":
            response_path.unlink()
            observed_raw = None
        else:
            observed_raw = matching_raw.replace(
                b"2026-07-12T13:00:00Z",
                b"2026-07-12T13:00:01Z",
                1,
            )
            assert len(observed_raw) == len(matching_raw)
            response_path.write_bytes(observed_raw)
        foreign_inbox = b"foreign inbox after durable idle\n"
        harness.paths.inbox.write_bytes(foreign_inbox)
        probe = artifact_base._RawBufferProbe(  # pyright: ignore[reportPrivateUsage]
            response_path,
            observed_raw,
        )
        artifact_base._install_raw_buffer_probe(  # pyright: ignore[reportPrivateUsage]
            monkeypatch,
            probe,
        )

        def forbid_delete(_expected: JournalDeleteExpectation) -> Never:
            raise AssertionError("unbound idle journal was deleted")

        def forbid_idle(_publisher: InboxPublisher) -> Never:
            raise AssertionError("unbound idle recovery overwrote the inbox")

        monkeypatch.setattr(harness.journals, "delete", forbid_delete)
        monkeypatch.setattr(InboxPublisher, "publish_idle", forbid_idle)

        report = await base._manager(harness).recover_pending()  # pyright: ignore[reportPrivateUsage]
        loaded = harness.journals.load()

        assert _disposition(report) == "quarantined"
        assert getattr(report, "response_cleanup_required") is False
        assert loaded is not None
        assert loaded.journal.original.state is PendingPhase.QUARANTINED
        assert loaded.journal.original.revision == 6
        assert harness.paths.inbox.read_bytes() == foreign_inbox
        assert response_path.exists() is (raw_case == "mismatch")
        if raw_case == "missing":
            assert probe.read_buffers == []
        else:
            assert probe.read_buffers == [observed_raw]
        assert probe.parsed_buffers == []


@pytest.mark.asyncio
async def test_startup_published_valid_original_response_settles_without_any_cancel(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with harness.lock:
        r0, r1 = early._journals(harness)  # pyright: ignore[reportPrivateUsage]
        early._persist(harness, r0, r1)  # pyright: ignore[reportPrivateUsage]
        early._seed_published_sqlite(harness, r0, r1)  # pyright: ignore[reportPrivateUsage]
        early._write_inbox(  # pyright: ignore[reportPrivateUsage]
            harness,
            early._render_request(r1),  # pyright: ignore[reportPrivateUsage]
        )
        command = base._command(harness)  # pyright: ignore[reportPrivateUsage]
        semantic = base._completed_artifact(command)  # pyright: ignore[reportPrivateUsage]
        raw = artifact_base._raw_response(  # pyright: ignore[reportPrivateUsage]
            semantic.accepted_response.envelope
        )
        response_path = harness.paths.response_path(command.request_id)
        response_path.write_bytes(raw)
        reads, parses = _install_response_waiter_raw_probe(
            monkeypatch,
            response_path=response_path,
            expected_raw=raw,
        )
        saved: list[PendingJournal] = []
        published: list[PreparedDelivery] = []
        original_save = harness.journals.save
        original_publish = InboxPublisher.publish_delivery

        def traced_save(
            journal: PendingJournal,
            *,
            expected_revisions: JournalRevisions | None,
        ) -> JournalRevisions:
            result = original_save(journal, expected_revisions=expected_revisions)
            saved.append(journal)
            return result

        def traced_publish(
            publisher: InboxPublisher,
            delivery: PreparedDelivery,
            *,
            runtime_snapshot: RuntimeSnapshot,
        ) -> None:
            if publisher is harness.inbox:
                published.append(delivery)
            original_publish(
                publisher,
                delivery,
                runtime_snapshot=runtime_snapshot,
            )

        monkeypatch.setattr(harness.journals, "save", traced_save)
        monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish)

        report = await base._manager(harness).recover_pending()  # pyright: ignore[reportPrivateUsage]

        assert _disposition(report) == "settled"
        assert reads == [raw]
        assert parses == [reads[0]]
        assert parses[0] is reads[0]
        assert all(journal.original.state is not PendingPhase.CANCEL_PUBLISHED for journal in saved)
        assert published == []
        assert harness.journals.load() is None


@pytest.mark.asyncio
async def test_startup_response_accepted_foreign_inbox_is_never_overwritten_or_deleted(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with harness.lock:
        seeded = artifact_base._seed_response_accepted_r4(  # pyright: ignore[reportPrivateUsage]
            harness,
            case="valid",
        )
        _advance_seed_to_response_aftercommit(harness, seeded)
        foreign = b"foreign inbox during response continuation\n"
        harness.paths.inbox.write_bytes(foreign)
        probe = artifact_base._RawBufferProbe(  # pyright: ignore[reportPrivateUsage]
            seeded.response_path,
            seeded.actual_raw,
        )
        artifact_base._install_raw_buffer_probe(  # pyright: ignore[reportPrivateUsage]
            monkeypatch,
            probe,
        )

        def forbid_idle(_publisher: InboxPublisher) -> Never:
            raise AssertionError("foreign inbox was overwritten")

        def forbid_delete(_expected: JournalDeleteExpectation) -> Never:
            raise AssertionError("journal was deleted while foreign inbox remained")

        monkeypatch.setattr(InboxPublisher, "publish_idle", forbid_idle)
        monkeypatch.setattr(harness.journals, "delete", forbid_delete)

        report = None
        error = None
        try:
            report = await base._manager(harness).recover_pending()  # pyright: ignore[reportPrivateUsage]
        except BridgeError as caught:
            error = caught
        loaded = harness.journals.load()
        request = harness.ledger.get_request(seeded.command.request_id)

        assert error is not None or (report is not None and _disposition(report) == "quarantined")
        if error is not None:
            assert error.code is ErrorCode.STATE_CONFLICT
        assert loaded is not None
        assert loaded.journal.original.state in {
            PendingPhase.RESPONSE_ACCEPTED,
            PendingPhase.QUARANTINED,
        }
        assert request is not None
        assert request.state in {
            HostRequestState.RESPONSE_ACCEPTED,
            HostRequestState.QUARANTINED,
        }
        assert harness.paths.inbox.read_bytes() == foreign
        assert seeded.response_path.exists()
        assert probe.read_buffers == [seeded.actual_raw]
        assert probe.parsed_buffers == [probe.read_buffers[0]]
        assert probe.parsed_buffers[0] is probe.read_buffers[0]


@pytest.mark.asyncio
async def test_startup_quarantine_records_only_an_exact_raw_bound_durable_artifact(
    harness: base.Harness,
) -> None:
    async with harness.lock:
        seeded = artifact_base._seed_response_accepted_r4(  # pyright: ignore[reportPrivateUsage]
            harness,
            case="valid",
        )
        quarantined = base._journal_with_original(  # pyright: ignore[reportPrivateUsage]
            seeded.r4,
            revision=5,
            state=PendingPhase.QUARANTINED,
            updated_at_ms=105,
        )
        artifact_base._save(  # pyright: ignore[reportPrivateUsage]
            harness,
            quarantined,
            seeded.r4,
        )

        report = await base._manager(harness).recover_pending()  # pyright: ignore[reportPrivateUsage]

        assert _disposition(report) == "quarantined"
        responding = harness.ledger.get_delivery(seeded.request_delivery_id)
        assert responding is not None
        assert responding.response_artifact == seeded.artifact
        assert responding.settlement == seeded.artifact.accepted_response.settlement
        nonresponding = harness.ledger.get_delivery(seeded.cancel_delivery_id)
        assert nonresponding is not None
        assert nonresponding.response_artifact is None
        request = harness.ledger.get_request(seeded.command.request_id)
        assert request is not None
        assert request.state is HostRequestState.QUARANTINED
        loaded = harness.journals.load()
        assert loaded is not None
        assert loaded.journal == quarantined


@pytest.mark.asyncio
async def test_startup_idle_published_preserves_foreign_inbox_and_finishes_terminal_state(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with harness.lock:
        seeded = _seed_idle_with_response_request(harness)
        foreign = b"foreign inbox written after durable idle publication\n"
        harness.paths.inbox.write_bytes(foreign)

        def forbid_idle(_publisher: InboxPublisher) -> Never:
            raise AssertionError("durable idle recovery overwrote a later foreign inbox")

        monkeypatch.setattr(InboxPublisher, "publish_idle", forbid_idle)

        report = await base._manager(harness).recover_pending()  # pyright: ignore[reportPrivateUsage]

        assert _disposition(report) == "settled"
        assert harness.paths.inbox.read_bytes() == foreign
        assert harness.journals.load() is None
        request = harness.ledger.get_request(seeded.artifact_seed.command.request_id)
        assert request is not None
        assert request.state is HostRequestState.COMPLETED
        assert request.terminal_at_ms is not None
