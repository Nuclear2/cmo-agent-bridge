from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Never, cast
from uuid import UUID

from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua, render_idle_lua
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from pydantic import JsonValue

from cmo_agent_bridge.protocol.models import (
    CancelAckResult,
    ExchangeCommand,
    PreparedDelivery,
    RequestBody,
)
from cmo_agent_bridge.protocol.response_models import ResponseArtifact, ResponseEnvelope
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    PendingJournal,
    PendingPhase,
)
from cmo_agent_bridge.state.pending_journal import JournalDeleteExpectation, JournalRevisions
from cmo_agent_bridge.state.request_ledger import DeliveryRecord, RequestRecord
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
import cmo_agent_bridge.transports.file_bridge.mutation as mutation_module
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo
import cmo_agent_bridge.transports.file_bridge.recovery as recovery_module
from cmo_agent_bridge.transports.file_bridge.response_waiter import ResponseWaiter
from cmo_agent_bridge.transports.file_bridge.transport import FileBridgeTransport


CRASH_CODE = 73
REQUEST_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeea201")
DELIVERY_ID = UUID("11111111-1111-4111-8111-11111111a201")
CANCEL_ID = UUID("22222222-2222-4222-8222-22222222a201")
LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")
CANCEL_CRASH_BOUNDARIES = frozenset(
    {
        "after_cancel_intent_r2",
        "after_cancel_delivery_insert",
        "after_cancel_replace",
        "after_cancel_r3",
        "after_cancel_delivery_marker",
        "after_request_cancel_published",
        "after_cancel_response_file",
        "after_response_r4",
        "after_response_record",
        "after_request_response_accepted",
        "after_idle_replace",
        "after_idle_r5",
        "after_request_idle_published",
        "after_terminal_cancelled",
        "after_journal_delete",
    }
)
CANCEL_PUBLISHED_CRASH_BOUNDARIES = frozenset(
    {
        "after_cancel_replace",
        "after_cancel_r3",
        "after_cancel_delivery_marker",
        "after_request_cancel_published",
        "after_cancel_response_file",
        "after_response_r4",
        "after_response_record",
        "after_request_response_accepted",
        "after_idle_replace",
        "after_idle_r5",
        "after_request_idle_published",
        "after_terminal_cancelled",
        "after_journal_delete",
    }
)
RESPONSE_CRASH_BOUNDARIES = frozenset(
    {
        "after_cancel_response_file",
        "after_response_r4",
        "after_response_record",
        "after_request_response_accepted",
        "after_idle_replace",
        "after_idle_r5",
        "after_request_idle_published",
        "after_terminal_cancelled",
        "after_journal_delete",
    }
)
LATE_HANDOFF_CRASH_BOUNDARIES = frozenset(
    {
        "after_idle_replace",
        "after_idle_r5",
        "after_request_idle_published",
        "after_terminal_cancelled",
        "after_journal_delete",
    }
)
RESPONSE_EVIDENCE: dict[str, str] = {}


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


class Inspector:
    def __init__(self, process: ProcessInfo) -> None:
        self._process = process

    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        if command_exe != self._process.executable:
            raise AssertionError("crash helper inspected the wrong Command executable")
        return (self._process,)


def _snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=MANIFEST_SHA256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )


def _command(snapshot: RuntimeSnapshot) -> ExchangeCommand:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "unit.set",
        {"unit_guid": "UNIT-1", "name": "Phase A"},
    )
    body = RequestBody(
        protocol=snapshot.protocol,
        release_id=snapshot.release_id,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=ACTIVATION_ID,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        operation="unit.set",
        arguments=invocation.wire_arguments.model_dump(mode="json"),
    )
    return ExchangeCommand(
        request_id=REQUEST_ID,
        body=body,
        invocation=invocation,
        runtime_snapshot=snapshot,
        timeout=30,
    )


def _cancelled_ack_bytes(snapshot: RuntimeSnapshot, request_hash: str) -> bytes:
    acknowledgement = CancelAckResult(
        request_id=REQUEST_ID,
        request_hash=request_hash,
        original_delivery_id=DELIVERY_ID,
        status="cancelled",
        result=None,
    )
    envelope = ResponseEnvelope(
        protocol=snapshot.protocol,
        request_id=REQUEST_ID,
        delivery_id=CANCEL_ID,
        request_hash=request_hash,
        ok=True,
        result=cast(JsonValue, acknowledgement.model_dump(mode="json")),
        error=None,
        scenario_time="2026-07-12T13:00:00Z",
        scenario_lineage_id=LINEAGE_ID,
        activation_id=ACTIVATION_ID,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        bridge_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
    )
    inner = _canonical_json_bytes(envelope.model_dump(mode="json", exclude_none=False)).decode(
        "utf-8"
    )
    return _canonical_json_bytes({"Comments": inner})


async def _write_cancel_response(
    transport: FileBridgeTransport,
    snapshot: RuntimeSnapshot,
    boundary: str,
    checkpoint: Path,
    publication_counts: dict[str, int],
    cancel_wait_started: asyncio.Event,
    allow_cancel_waiter: asyncio.Event,
) -> None:
    await asyncio.wait_for(cancel_wait_started.wait(), timeout=5)
    loaded = transport.journals.load()
    request = transport.ledger.get_request(REQUEST_ID)
    request_delivery = transport.ledger.get_delivery(DELIVERY_ID)
    cancel_delivery_record = transport.ledger.get_delivery(CANCEL_ID)
    if loaded is None:
        raise AssertionError("cancel responder lacks the R3 journal")
    original = loaded.journal.original
    if (
        original.state is not PendingPhase.CANCEL_PUBLISHED
        or original.revision != 3
        or len(original.delivery_intents) != 2
        or request is None
        or request.state is not HostRequestState.CANCEL_PUBLISHED
        or request_delivery is None
        or request_delivery.published_at_ms is None
        or cancel_delivery_record is None
        or cancel_delivery_record.published_at_ms is None
    ):
        raise AssertionError("cancel responder observed incomplete published-cancel evidence")
    request_intent, cancel_intent = original.delivery_intents
    cancel_delivery = PreparedDelivery(
        request_id=cancel_intent.request_id,
        delivery_id=cancel_intent.delivery_id,
        delivery_kind="cancel",
        request_hash=cancel_intent.request_hash,
        body_json=cancel_intent.body_json.encode("utf-8", errors="strict"),
    )
    if (
        request_intent.delivery_id != DELIVERY_ID
        or request_intent.published_at_ms is None
        or cancel_intent.delivery_id != CANCEL_ID
        or cancel_intent.published_at_ms is None
        or transport.paths.inbox.read_bytes() != render_delivery_lua(cancel_delivery, snapshot)
    ):
        raise AssertionError("cancel responder did not observe the exact rendered cancel")
    response_path = transport.paths.response_path(REQUEST_ID)
    if response_path.exists():
        raise AssertionError("cancel responder found a pre-existing response path")
    raw = _cancelled_ack_bytes(snapshot, cancel_intent.request_hash)
    with response_path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    if response_path.read_bytes() != raw:
        raise AssertionError("cancel responder could not reread its exact fsynced bytes")
    RESPONSE_EVIDENCE.update(
        {
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size_bytes": str(len(raw)),
        }
    )
    if boundary == "after_cancel_response_file":
        _checkpoint_and_exit(
            checkpoint,
            boundary,
            publication_counts,
        )
    allow_cancel_waiter.set()


def _checkpoint_and_exit(
    checkpoint: Path,
    boundary: str,
    publication_counts: dict[str, int],
) -> Never:
    payload_fields = {
        "boundary": boundary,
        "request_id": str(REQUEST_ID),
        "delivery_id": str(DELIVERY_ID),
        "request_publications": str(publication_counts["request"]),
        "cancel_publications": str(publication_counts["cancel"]),
        "idle_publications": str(publication_counts["idle"]),
        "request_uuid_calls": str(publication_counts["request_uuid_calls"]),
    }
    if boundary in CANCEL_CRASH_BOUNDARIES:
        expected = {
            "request": 1,
            "cancel": 1 if boundary in CANCEL_PUBLISHED_CRASH_BOUNDARIES else 0,
            "idle": 1 if boundary in LATE_HANDOFF_CRASH_BOUNDARIES else 0,
            "request_uuid_calls": 1,
            "cancel_uuid_calls": 1,
            "original_wait_calls": 1,
            "task_cancel_requests": 1,
        }
        if any(publication_counts[key] != value for key, value in expected.items()):
            raise AssertionError("cancel crash boundary lacks exact cancellation evidence")
        payload_fields.update(
            {
                "cancel_delivery_id": str(CANCEL_ID),
                "cancel_uuid_calls": str(publication_counts["cancel_uuid_calls"]),
                "original_wait_calls": str(publication_counts["original_wait_calls"]),
                "task_cancel_requests": str(publication_counts["task_cancel_requests"]),
            }
        )
    if boundary in RESPONSE_CRASH_BOUNDARIES:
        if publication_counts["cancel_wait_calls"] != 1:
            raise AssertionError("response crash boundary lacks one cancel wait call")
        response_sha256 = RESPONSE_EVIDENCE.get("sha256")
        response_size_bytes = RESPONSE_EVIDENCE.get("size_bytes")
        if (
            response_sha256 is None
            or len(response_sha256) != 64
            or response_size_bytes is None
            or not response_size_bytes.isdecimal()
            or int(response_size_bytes) <= 0
        ):
            raise AssertionError("response crash boundary lacks exact raw response evidence")
        payload_fields.update(
            {
                "cancel_wait_calls": str(publication_counts["cancel_wait_calls"]),
                "response_sha256": response_sha256,
                "response_size_bytes": response_size_bytes,
            }
        )
    payload = json.dumps(
        payload_fields,
        sort_keys=True,
        separators=(",", ":"),
    )
    with checkpoint.open("x", encoding="utf-8", newline="") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os._exit(CRASH_CODE)


def _install_after_r0_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_save = transport.journals.save

    def save_then_crash(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        result = installed_save(journal, expected_revisions=expected_revisions)
        if (
            journal.original.request_id == REQUEST_ID
            and journal.original.state is PendingPhase.PREPARED
            and journal.original.revision == 0
            and expected_revisions is None
        ):
            _checkpoint_and_exit(checkpoint, "after_r0", publication_counts)
        return result

    setattr(transport.journals, "save", save_then_crash)


def _install_after_request_insert_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_insert = transport.ledger.insert_prepared

    def insert_then_crash(record: RequestRecord) -> None:
        installed_insert(record)
        if record.request_id == REQUEST_ID and record.state is HostRequestState.PREPARED:
            _checkpoint_and_exit(checkpoint, "after_request_insert", publication_counts)

    setattr(transport.ledger, "insert_prepared", insert_then_crash)


def _install_after_request_delivery_insert_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_insert = transport.ledger.insert_delivery

    def insert_then_crash(intent: DeliveryIntent) -> None:
        installed_insert(intent)
        if (
            intent.request_id == REQUEST_ID
            and intent.delivery_id == DELIVERY_ID
            and intent.delivery_kind == "request"
        ):
            _checkpoint_and_exit(
                checkpoint,
                "after_request_delivery_insert",
                publication_counts,
            )

    setattr(transport.ledger, "insert_delivery", insert_then_crash)


def _install_after_request_replace_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_publish = InboxPublisher.publish_delivery

    def publish_then_crash(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)
        if (
            publisher is transport.inbox
            and delivery.request_id == REQUEST_ID
            and delivery.delivery_id == DELIVERY_ID
            and delivery.delivery_kind == "request"
        ):
            _checkpoint_and_exit(checkpoint, "after_request_replace", publication_counts)

    setattr(InboxPublisher, "publish_delivery", publish_then_crash)


def _install_after_r1_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_save = transport.journals.save

    def save_then_crash(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        result = installed_save(journal, expected_revisions=expected_revisions)
        original = journal.original
        if original.request_id == REQUEST_ID and original.state is PendingPhase.PUBLISHED:
            if expected_revisions != JournalRevisions(original=0, reconcile_attempt=None):
                raise AssertionError("R1 save used the wrong predecessor revision")
            if result != JournalRevisions(original=1, reconcile_attempt=None):
                raise AssertionError("R1 save returned the wrong durable revision")
            if original.revision != 1 or len(original.delivery_intents) != 1:
                raise AssertionError("R1 journal has the wrong shape")
            intent = original.delivery_intents[0]
            if (
                intent.request_id != REQUEST_ID
                or intent.delivery_id != DELIVERY_ID
                or intent.delivery_kind != "request"
                or intent.published_at_ms is None
                or original.updated_at_ms != intent.published_at_ms
            ):
                raise AssertionError("R1 journal has the wrong request publication identity")
            _checkpoint_and_exit(checkpoint, "after_r1", publication_counts)
        return result

    setattr(transport.journals, "save", save_then_crash)


def _install_after_request_delivery_marker_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_mark = transport.ledger.mark_delivery_published

    def mark_then_crash(delivery_id: UUID, *, published_at_ms: int) -> DeliveryRecord:
        result = installed_mark(delivery_id, published_at_ms=published_at_ms)
        if delivery_id == DELIVERY_ID:
            if (
                result.request_id != REQUEST_ID
                or result.delivery_id != DELIVERY_ID
                or result.delivery_kind != "request"
                or result.published_at_ms != published_at_ms
            ):
                raise AssertionError("request delivery marker returned the wrong durable row")
            _checkpoint_and_exit(
                checkpoint,
                "after_request_delivery_marker",
                publication_counts,
            )
        return result

    setattr(transport.ledger, "mark_delivery_published", mark_then_crash)


def _install_after_request_published_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_transition = transport.ledger.transition

    def transition_then_crash(
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
        result = installed_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        if request_id == REQUEST_ID and new_state is HostRequestState.PUBLISHED:
            if expected_states != frozenset({HostRequestState.PREPARED}):
                raise AssertionError("request publication transition used the wrong predecessor")
            if (
                result.request_id != REQUEST_ID
                or result.state is not HostRequestState.PUBLISHED
                or result.updated_at_ms != updated_at_ms
                or result.terminal_at_ms is not None
            ):
                raise AssertionError("request publication transition returned the wrong row")
            _checkpoint_and_exit(
                checkpoint,
                "after_request_published",
                publication_counts,
            )
        return result

    setattr(transport.ledger, "transition", transition_then_crash)


def _install_after_cancel_intent_r2_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_save = transport.journals.save

    def save_then_crash(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        result = installed_save(journal, expected_revisions=expected_revisions)
        original = journal.original
        if original.request_id == REQUEST_ID and original.revision == 2:
            if original.state is not PendingPhase.CANCEL_PUBLISHED:
                raise AssertionError("R2 journal has the wrong phase")
            if expected_revisions != JournalRevisions(original=1, reconcile_attempt=None):
                raise AssertionError("R2 save used the wrong predecessor revision")
            if result != JournalRevisions(original=2, reconcile_attempt=None):
                raise AssertionError("R2 save returned the wrong durable revision")
            if len(original.delivery_intents) != 2:
                raise AssertionError("R2 journal has the wrong delivery count")
            request_intent, cancel_intent = original.delivery_intents
            if (
                request_intent.request_id != REQUEST_ID
                or request_intent.delivery_id != DELIVERY_ID
                or request_intent.delivery_kind != "request"
                or request_intent.published_at_ms is None
                or cancel_intent.request_id != REQUEST_ID
                or cancel_intent.delivery_id != CANCEL_ID
                or cancel_intent.delivery_kind != "cancel"
                or cancel_intent.original_request_delivery_id != DELIVERY_ID
                or cancel_intent.published_at_ms is not None
                or original.updated_at_ms != cancel_intent.intended_at_ms
            ):
                raise AssertionError("R2 journal has the wrong durable cancel intent")
            _checkpoint_and_exit(
                checkpoint,
                "after_cancel_intent_r2",
                publication_counts,
            )
        return result

    setattr(transport.journals, "save", save_then_crash)


def _install_after_cancel_delivery_insert_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_insert = transport.ledger.insert_delivery

    def insert_then_crash(intent: DeliveryIntent) -> None:
        installed_insert(intent)
        if intent.delivery_kind == "cancel":
            observed = transport.ledger.get_delivery(intent.delivery_id)
            if (
                intent.request_id != REQUEST_ID
                or intent.delivery_id != CANCEL_ID
                or intent.original_request_delivery_id != DELIVERY_ID
                or intent.published_at_ms is not None
                or observed is None
                or observed.request_id != REQUEST_ID
                or observed.delivery_id != CANCEL_ID
                or observed.delivery_kind != "cancel"
                or observed.original_request_delivery_id != DELIVERY_ID
                or observed.published_at_ms is not None
            ):
                raise AssertionError("cancel delivery insert returned the wrong durable row")
            _checkpoint_and_exit(
                checkpoint,
                "after_cancel_delivery_insert",
                publication_counts,
            )

    setattr(transport.ledger, "insert_delivery", insert_then_crash)


def _install_after_cancel_replace_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_publish = InboxPublisher.publish_delivery

    def publish_then_crash(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)
        if publisher is transport.inbox and delivery.delivery_kind == "cancel":
            request = transport.ledger.get_request(REQUEST_ID)
            if (
                delivery.request_id != REQUEST_ID
                or delivery.delivery_id != CANCEL_ID
                or request is None
                or delivery.request_hash != request.request_hash
                or runtime_snapshot != _snapshot()
                or transport.paths.inbox.read_bytes()
                != render_delivery_lua(delivery, runtime_snapshot)
            ):
                raise AssertionError("cancel inbox replace returned the wrong durable bytes")
            loaded = transport.journals.load()
            cancel_record = transport.ledger.get_delivery(CANCEL_ID)
            if (
                loaded is None
                or loaded.journal.original.state is not PendingPhase.CANCEL_PUBLISHED
                or loaded.journal.original.revision != 2
                or request.state is not HostRequestState.PUBLISHED
                or cancel_record is None
                or cancel_record.published_at_ms is not None
            ):
                raise AssertionError("cancel inbox replace changed pre-R3 durable evidence")
            _checkpoint_and_exit(
                checkpoint,
                "after_cancel_replace",
                publication_counts,
            )

    setattr(InboxPublisher, "publish_delivery", publish_then_crash)


def _install_after_cancel_r3_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_save = transport.journals.save

    def save_then_crash(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        result = installed_save(journal, expected_revisions=expected_revisions)
        original = journal.original
        if original.request_id == REQUEST_ID and original.revision == 3:
            if (
                original.state is not PendingPhase.CANCEL_PUBLISHED
                or expected_revisions != JournalRevisions(original=2, reconcile_attempt=None)
                or result != JournalRevisions(original=3, reconcile_attempt=None)
                or len(original.delivery_intents) != 2
            ):
                raise AssertionError("R3 save returned the wrong durable revision")
            request_intent, cancel_intent = original.delivery_intents
            if (
                request_intent.delivery_id != DELIVERY_ID
                or request_intent.published_at_ms is None
                or cancel_intent.delivery_id != CANCEL_ID
                or cancel_intent.delivery_kind != "cancel"
                or cancel_intent.published_at_ms is None
                or original.updated_at_ms != cancel_intent.published_at_ms
            ):
                raise AssertionError("R3 journal has the wrong cancel publication intent")
            _checkpoint_and_exit(
                checkpoint,
                "after_cancel_r3",
                publication_counts,
            )
        return result

    setattr(transport.journals, "save", save_then_crash)


def _install_after_cancel_delivery_marker_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_mark = transport.ledger.mark_delivery_published

    def mark_then_crash(delivery_id: UUID, *, published_at_ms: int) -> DeliveryRecord:
        result = installed_mark(delivery_id, published_at_ms=published_at_ms)
        if delivery_id == CANCEL_ID:
            if (
                result.request_id != REQUEST_ID
                or result.delivery_id != CANCEL_ID
                or result.delivery_kind != "cancel"
                or result.original_request_delivery_id != DELIVERY_ID
                or result.published_at_ms != published_at_ms
            ):
                raise AssertionError("cancel delivery marker returned the wrong durable row")
            _checkpoint_and_exit(
                checkpoint,
                "after_cancel_delivery_marker",
                publication_counts,
            )
        return result

    setattr(transport.ledger, "mark_delivery_published", mark_then_crash)


def _install_after_request_cancel_published_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_transition = transport.ledger.transition

    def transition_then_crash(
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
        result = installed_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        if request_id == REQUEST_ID and new_state is HostRequestState.CANCEL_PUBLISHED:
            if (
                expected_states != frozenset({HostRequestState.PUBLISHED})
                or result.state is not HostRequestState.CANCEL_PUBLISHED
                or result.updated_at_ms != updated_at_ms
                or result.terminal_at_ms is not None
            ):
                raise AssertionError("request cancel transition returned the wrong durable row")
            _checkpoint_and_exit(
                checkpoint,
                "after_request_cancel_published",
                publication_counts,
            )
        return result

    setattr(transport.ledger, "transition", transition_then_crash)


def _install_after_cancel_response_file_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    del transport, checkpoint, publication_counts


def _install_after_response_r4_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_save = transport.journals.save

    def save_then_crash(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        result = installed_save(journal, expected_revisions=expected_revisions)
        original = journal.original
        if original.request_id == REQUEST_ID and original.revision == 4:
            artifact = original.response_artifact
            if (
                original.state is not PendingPhase.RESPONSE_ACCEPTED
                or expected_revisions != JournalRevisions(original=3, reconcile_attempt=None)
                or result != JournalRevisions(original=4, reconcile_attempt=None)
                or artifact is None
                or artifact.accepted_response.envelope.delivery_id != CANCEL_ID
                or artifact.sha256 != RESPONSE_EVIDENCE.get("sha256")
                or str(artifact.size_bytes) != RESPONSE_EVIDENCE.get("size_bytes")
                or original.settlement != artifact.accepted_response.settlement
            ):
                raise AssertionError("R4 save returned the wrong response artifact")
            _checkpoint_and_exit(
                checkpoint,
                "after_response_r4",
                publication_counts,
            )
        return result

    setattr(transport.journals, "save", save_then_crash)


def _install_after_response_record_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_record = transport.ledger.record_response

    def record_then_crash(artifact: ResponseArtifact) -> DeliveryRecord:
        result = installed_record(artifact)
        if artifact.accepted_response.envelope.delivery_id == CANCEL_ID:
            if (
                result.request_id != REQUEST_ID
                or result.delivery_id != CANCEL_ID
                or result.delivery_kind != "cancel"
                or result.response_artifact != artifact
                or result.settlement != artifact.accepted_response.settlement
                or artifact.sha256 != RESPONSE_EVIDENCE.get("sha256")
                or str(artifact.size_bytes) != RESPONSE_EVIDENCE.get("size_bytes")
            ):
                raise AssertionError("response record returned the wrong durable artifact")
            _checkpoint_and_exit(
                checkpoint,
                "after_response_record",
                publication_counts,
            )
        return result

    setattr(transport.ledger, "record_response", record_then_crash)


def _install_after_request_response_accepted_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_transition = transport.ledger.transition

    def transition_then_crash(
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
        result = installed_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        if request_id == REQUEST_ID and new_state is HostRequestState.RESPONSE_ACCEPTED:
            cancel_record = transport.ledger.get_delivery(CANCEL_ID)
            if (
                expected_states != frozenset({HostRequestState.CANCEL_PUBLISHED})
                or result.state is not HostRequestState.RESPONSE_ACCEPTED
                or result.updated_at_ms != updated_at_ms
                or result.terminal_at_ms is not None
                or cancel_record is None
                or cancel_record.response_artifact is None
                or cancel_record.response_artifact.sha256 != RESPONSE_EVIDENCE.get("sha256")
            ):
                raise AssertionError("response acceptance transition returned the wrong row")
            _checkpoint_and_exit(
                checkpoint,
                "after_request_response_accepted",
                publication_counts,
            )
        return result

    setattr(transport.ledger, "transition", transition_then_crash)


def _install_after_idle_replace_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_idle = InboxPublisher.publish_idle

    def publish_idle_then_crash(publisher: InboxPublisher) -> None:
        installed_idle(publisher)
        if publisher is transport.inbox:
            loaded = transport.journals.load()
            request = transport.ledger.get_request(REQUEST_ID)
            cancel_record = transport.ledger.get_delivery(CANCEL_ID)
            if (
                loaded is None
                or loaded.journal.original.state is not PendingPhase.RESPONSE_ACCEPTED
                or loaded.journal.original.revision != 4
                or request is None
                or request.state is not HostRequestState.RESPONSE_ACCEPTED
                or cancel_record is None
                or cancel_record.response_artifact is None
                or cancel_record.response_artifact.sha256 != RESPONSE_EVIDENCE.get("sha256")
                or transport.paths.inbox.read_bytes() != render_idle_lua()
            ):
                raise AssertionError("idle replace changed R4 response evidence")
            _checkpoint_and_exit(
                checkpoint,
                "after_idle_replace",
                publication_counts,
            )

    setattr(InboxPublisher, "publish_idle", publish_idle_then_crash)


def _install_after_idle_r5_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_save = transport.journals.save

    def save_then_crash(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        result = installed_save(journal, expected_revisions=expected_revisions)
        original = journal.original
        if original.request_id == REQUEST_ID and original.revision == 5:
            artifact = original.response_artifact
            if (
                original.state is not PendingPhase.IDLE_PUBLISHED
                or expected_revisions != JournalRevisions(original=4, reconcile_attempt=None)
                or result != JournalRevisions(original=5, reconcile_attempt=None)
                or artifact is None
                or artifact.sha256 != RESPONSE_EVIDENCE.get("sha256")
                or str(artifact.size_bytes) != RESPONSE_EVIDENCE.get("size_bytes")
            ):
                raise AssertionError("R5 save returned the wrong idle handoff journal")
            _checkpoint_and_exit(
                checkpoint,
                "after_idle_r5",
                publication_counts,
            )
        return result

    setattr(transport.journals, "save", save_then_crash)


def _install_after_request_idle_published_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_transition = transport.ledger.transition

    def transition_then_crash(
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
        result = installed_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        if request_id == REQUEST_ID and new_state is HostRequestState.IDLE_PUBLISHED:
            if (
                expected_states != frozenset({HostRequestState.RESPONSE_ACCEPTED})
                or result.state is not HostRequestState.IDLE_PUBLISHED
                or result.updated_at_ms != updated_at_ms
                or result.terminal_at_ms is not None
            ):
                raise AssertionError("idle request transition returned the wrong row")
            _checkpoint_and_exit(
                checkpoint,
                "after_request_idle_published",
                publication_counts,
            )
        return result

    setattr(transport.ledger, "transition", transition_then_crash)


def _install_after_terminal_cancelled_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_transition = transport.ledger.transition

    def transition_then_crash(
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
        result = installed_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        if request_id == REQUEST_ID and new_state is HostRequestState.CANCELLED:
            if (
                expected_states != frozenset({HostRequestState.IDLE_PUBLISHED})
                or result.state is not HostRequestState.CANCELLED
                or result.updated_at_ms != updated_at_ms
                or result.terminal_at_ms != updated_at_ms
                or result_json is not None
                or error_json is not None
                or resolution_json is not None
            ):
                raise AssertionError("terminal cancellation returned the wrong row")
            _checkpoint_and_exit(
                checkpoint,
                "after_terminal_cancelled",
                publication_counts,
            )
        return result

    setattr(transport.ledger, "transition", transition_then_crash)


def _install_after_journal_delete_crash(
    transport: FileBridgeTransport,
    checkpoint: Path,
    publication_counts: dict[str, int],
) -> None:
    installed_delete = transport.journals.delete

    def delete_then_crash(expected: JournalDeleteExpectation) -> None:
        loaded = transport.journals.load()
        installed_delete(expected)
        request = transport.ledger.get_request(REQUEST_ID)
        if loaded is None:
            raise AssertionError("journal delete lacked its exact R5 predecessor")
        journal = loaded.journal
        exact_expectation = JournalDeleteExpectation(
            root_key=transport.paths.root_key,
            required_release_id=journal.header.required_release_id,
            original_request_id=REQUEST_ID,
            reconcile_attempt_request_id=None,
            revisions=JournalRevisions(original=5, reconcile_attempt=None),
        )
        if (
            journal.original.state is not PendingPhase.IDLE_PUBLISHED
            or journal.original.revision != 5
            or expected != exact_expectation
            or transport.journals.load() is not None
            or transport.paths.pending_file.exists()
            or request is None
            or request.state is not HostRequestState.CANCELLED
            or request.terminal_at_ms is None
        ):
            raise AssertionError("journal delete returned without exact terminal evidence")
        _checkpoint_and_exit(
            checkpoint,
            "after_journal_delete",
            publication_counts,
        )

    setattr(transport.journals, "delete", delete_then_crash)


def _install_publication_counter(
    transport: FileBridgeTransport,
    publication_counts: dict[str, int],
) -> None:
    installed_publish = InboxPublisher.publish_delivery
    installed_idle = InboxPublisher.publish_idle

    def publish_and_count(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)
        if publisher is transport.inbox:
            publication_counts[delivery.delivery_kind] += 1

    def publish_idle_and_count(publisher: InboxPublisher) -> None:
        installed_idle(publisher)
        if publisher is transport.inbox:
            publication_counts["idle"] += 1

    setattr(InboxPublisher, "publish_delivery", publish_and_count)
    setattr(InboxPublisher, "publish_idle", publish_idle_and_count)


async def _run(game_root: Path, local_app_data: Path, boundary: str, checkpoint: Path) -> None:
    RESPONSE_EVIDENCE.clear()
    paths = FileBridgePaths.build(game_root, local_app_data)
    snapshot = _snapshot()
    catalog = ManifestCatalog(ReleaseBinding(snapshot=snapshot, registry=OPERATION_REGISTRY))
    process = ProcessInfo(pid=1234, create_time=1000.5, executable=paths.command_exe)
    transport = FileBridgeTransport(
        paths=paths,
        root_lock=RootLock(paths.lock_file, timeout_seconds=0),
        process_inspector=Inspector(process),
        catalog=catalog,
        database=StateDatabase(paths.sqlite_file),
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
        response_poll_seconds=0.001,
        cancel_ack_timeout_seconds=10,
    )
    publication_counts = {
        "request": 0,
        "cancel": 0,
        "idle": 0,
        "request_uuid_calls": 0,
        "cancel_uuid_calls": 0,
        "original_wait_calls": 0,
        "cancel_wait_calls": 0,
        "task_cancel_requests": 0,
    }
    _install_publication_counter(transport, publication_counts)
    installers = {
        "after_r0": _install_after_r0_crash,
        "after_request_insert": _install_after_request_insert_crash,
        "after_request_delivery_insert": _install_after_request_delivery_insert_crash,
        "after_request_replace": _install_after_request_replace_crash,
        "after_r1": _install_after_r1_crash,
        "after_request_delivery_marker": _install_after_request_delivery_marker_crash,
        "after_request_published": _install_after_request_published_crash,
        "after_cancel_intent_r2": _install_after_cancel_intent_r2_crash,
        "after_cancel_delivery_insert": _install_after_cancel_delivery_insert_crash,
        "after_cancel_replace": _install_after_cancel_replace_crash,
        "after_cancel_r3": _install_after_cancel_r3_crash,
        "after_cancel_delivery_marker": _install_after_cancel_delivery_marker_crash,
        "after_request_cancel_published": _install_after_request_cancel_published_crash,
        "after_cancel_response_file": _install_after_cancel_response_file_crash,
        "after_response_r4": _install_after_response_r4_crash,
        "after_response_record": _install_after_response_record_crash,
        "after_request_response_accepted": _install_after_request_response_accepted_crash,
        "after_idle_replace": _install_after_idle_replace_crash,
        "after_idle_r5": _install_after_idle_r5_crash,
        "after_request_idle_published": _install_after_request_idle_published_crash,
        "after_terminal_cancelled": _install_after_terminal_cancelled_crash,
        "after_journal_delete": _install_after_journal_delete_crash,
    }
    try:
        install = installers[boundary]
    except KeyError as error:
        raise ValueError(f"unsupported crash boundary: {boundary}") from error
    install(transport, checkpoint, publication_counts)

    def one_shot_request_uuid4() -> UUID:
        publication_counts["request_uuid_calls"] += 1
        if publication_counts["request_uuid_calls"] != 1:
            raise AssertionError("mutation exchange requested more than one delivery UUID")
        return DELIVERY_ID

    setattr(
        mutation_module,
        "uuid",
        SimpleNamespace(uuid4=one_shot_request_uuid4),
    )

    def one_shot_cancel_uuid4() -> UUID:
        publication_counts["cancel_uuid_calls"] += 1
        if publication_counts["cancel_uuid_calls"] != 1:
            raise AssertionError("cancellation recovery requested more than one delivery UUID")
        return CANCEL_ID

    setattr(
        recovery_module,
        "uuid",
        SimpleNamespace(uuid4=one_shot_cancel_uuid4),
    )

    original_wait_started = asyncio.Event()
    cancel_wait_started = asyncio.Event()
    allow_cancel_waiter = asyncio.Event()
    if boundary in CANCEL_CRASH_BOUNDARIES:
        installed_wait = ResponseWaiter.wait

        async def gated_response_wait(
            waiter: ResponseWaiter,
            timeout_seconds: float,
        ) -> ResponseArtifact:
            if publication_counts["original_wait_calls"] == 0:
                publication_counts["original_wait_calls"] = 1
                original_wait_started.set()
                await asyncio.Event().wait()
                raise AssertionError("gated original response wait returned without cancellation")
            if publication_counts["cancel_wait_calls"] == 0:
                publication_counts["cancel_wait_calls"] = 1
                cancel_wait_started.set()
                await allow_cancel_waiter.wait()
                return await installed_wait(waiter, timeout_seconds)
            raise AssertionError("crash helper reached an unexpected third response wait")

        setattr(ResponseWaiter, "wait", gated_response_wait)

    responder_task: asyncio.Task[None] | None = None
    if boundary in RESPONSE_CRASH_BOUNDARIES:
        responder_task = asyncio.create_task(
            _write_cancel_response(
                transport,
                snapshot,
                boundary,
                checkpoint,
                publication_counts,
                cancel_wait_started,
                allow_cancel_waiter,
            )
        )

    async with transport.session() as channel:
        if boundary not in CANCEL_CRASH_BOUNDARIES:
            await channel.exchange(_command(snapshot))
        else:
            exchange_task = asyncio.create_task(channel.exchange(_command(snapshot)))
            await asyncio.wait_for(original_wait_started.wait(), timeout=5)
            assert exchange_task.cancel("hard-crash helper cancellation") is True
            publication_counts["task_cancel_requests"] += 1
            await exchange_task
    if responder_task is not None:
        await responder_task
    raise AssertionError("mutation crash helper returned without reaching its boundary")


def main() -> None:
    if len(sys.argv) != 5:
        raise SystemExit(
            "usage: mutation_failure_process.py GAME_ROOT LOCAL_APP_DATA BOUNDARY CHECKPOINT"
        )
    asyncio.run(
        _run(
            Path(sys.argv[1]),
            Path(sys.argv[2]),
            sys.argv[3],
            Path(sys.argv[4]),
        )
    )


if __name__ == "__main__":
    main()
