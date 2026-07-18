from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import TYPE_CHECKING, TypeVar, cast
from uuid import UUID

import pytest
from pydantic import JsonValue

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.canonical import prepare_delivery
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua, render_idle_lua
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.models import (
    AllowedDelivery,
    CancelAckResult,
    ExchangeCommand,
    PreparedDelivery,
    RequestBody,
    ResponseExpectation,
)
from cmo_agent_bridge.protocol.response import parse_inst_response
from cmo_agent_bridge.protocol.response_models import (
    CancelledSettlement,
    CompletedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    PendingExchange,
    PendingJournal,
    PendingJournalHeader,
    PendingPhase,
)
from cmo_agent_bridge.state.request_ledger import DeliveryRecord, RequestLedger, RequestRecord
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.cleanup import (
    ArtifactJanitor,
    CleanupReport,
    PinnedResponseCleanup,
)
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
import cmo_agent_bridge.transports.file_bridge.mutation as mutation_module
from cmo_agent_bridge.transports.file_bridge.models import (
    RecoveryDisposition,
    RecoveryReport,
)
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo
import cmo_agent_bridge.transports.file_bridge.recovery as recovery_module
from cmo_agent_bridge.transports.file_bridge.recovery import RecoveryManager
from cmo_agent_bridge.transports.file_bridge.response_waiter import ResponseWaiter
from cmo_agent_bridge.transports.file_bridge.transport import FileBridgeTransport

if TYPE_CHECKING:
    from tests.helpers.fake_file_bridge_peer import FakeFileBridgePeer, Respond
else:
    sys.path.insert(0, str(Path(__file__).parents[1] / "helpers"))
    from fake_file_bridge_peer import FakeFileBridgePeer, Respond


CRASH_CODE = 73
REQUEST_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeea201")
DELIVERY_ID = UUID("11111111-1111-4111-8111-11111111a201")
CANCEL_ID = UUID("22222222-2222-4222-8222-22222222a201")
LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")
NEW_REQUEST_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeea209")
_T = TypeVar("_T")
_TASK_CANCEL_GRACE_SECONDS = 1.0
_CRASH_HELPER_EXIT_TIMEOUT_SECONDS = 30.0


async def _hard_task_result(
    task: asyncio.Task[_T],
    *,
    timeout_seconds: float,
    description: str,
) -> _T:
    done, pending = await asyncio.wait({task}, timeout=timeout_seconds)
    if not pending:
        assert done == {task}
        return task.result()

    task.cancel(f"{description} exceeded its hard deadline")
    grace_done, grace_pending = await asyncio.wait(
        {task},
        timeout=_TASK_CANCEL_GRACE_SECONDS,
    )
    if grace_done:
        assert grace_done == {task}
        try:
            task.result()
        except (asyncio.CancelledError, Exception):
            pass
    assert grace_pending in (set(), {task})
    pytest.fail(
        f"{description} exceeded {timeout_seconds:g} seconds"
        + (
            " and remained pending after cancellation grace"
            if grace_pending
            else " before cancellation completed"
        ),
        pytrace=False,
    )


async def _cancel_and_drain_task(
    task: asyncio.Task[_T],
    *,
    timeout_seconds: float,
    description: str,
) -> None:
    if not task.done():
        task.cancel(f"{description} cleanup")
    done, pending = await asyncio.wait({task}, timeout=timeout_seconds)
    if pending:
        task.cancel(f"{description} cleanup deadline")
        pytest.fail(
            f"{description} remained pending after {timeout_seconds:g}-second cleanup",
            pytrace=False,
        )
    assert done == {task}
    try:
        task.result()
    except asyncio.CancelledError:
        pass


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
        assert command_exe == self._process.executable
        return (self._process,)


@dataclass(frozen=True, slots=True)
class MatrixHarness:
    game_root: Path
    local_app_data: Path
    paths: FileBridgePaths
    process: ProcessInfo
    inspector: Inspector
    snapshot: RuntimeSnapshot
    catalog: ManifestCatalog


@pytest.fixture
def matrix_harness(tmp_path: Path) -> MatrixHarness:
    game_root = tmp_path / "game"
    game_root.mkdir()
    (game_root / "Command.exe").write_bytes(b"exe")
    (game_root / "Lua").mkdir()
    (game_root / "ImportExport").mkdir()
    local_app_data = tmp_path / "local"
    local_app_data.mkdir()

    paths = FileBridgePaths.build(game_root, local_app_data)
    process = ProcessInfo(pid=1234, create_time=1000.5, executable=paths.command_exe)
    snapshot = RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=MANIFEST_SHA256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )
    return MatrixHarness(
        game_root=game_root,
        local_app_data=local_app_data,
        paths=paths,
        process=process,
        inspector=Inspector(process),
        snapshot=snapshot,
        catalog=ManifestCatalog(ReleaseBinding(snapshot=snapshot, registry=OPERATION_REGISTRY)),
    )


def _make_transport(
    harness: MatrixHarness,
    *,
    cancel_ack_timeout_seconds: float = 10,
) -> FileBridgeTransport:
    return FileBridgeTransport(
        paths=harness.paths,
        root_lock=RootLock(harness.paths.lock_file, timeout_seconds=0),
        process_inspector=harness.inspector,
        catalog=harness.catalog,
        database=StateDatabase(harness.paths.sqlite_file),
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
        response_poll_seconds=0.001,
        cancel_ack_timeout_seconds=cancel_ack_timeout_seconds,
    )


def _command(
    harness: MatrixHarness,
    *,
    request_id: UUID = REQUEST_ID,
) -> ExchangeCommand:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "unit.set",
        {"unit_guid": "UNIT-1", "name": "Phase A"},
    )
    body = RequestBody(
        protocol=harness.snapshot.protocol,
        release_id=harness.snapshot.release_id,
        runtime_version=harness.snapshot.runtime_version,
        runtime_tag=harness.snapshot.runtime_tag,
        runtime_asset_sha256=harness.snapshot.runtime_asset_sha256,
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=ACTIVATION_ID,
        operation_manifest_sha256=harness.snapshot.operation_manifest_sha256,
        operation="unit.set",
        arguments=invocation.wire_arguments.model_dump(mode="json"),
    )
    return ExchangeCommand(
        request_id=request_id,
        body=body,
        invocation=invocation,
        runtime_snapshot=harness.snapshot,
        timeout=30,
    )


def _r0_snapshot_models(
    harness: MatrixHarness,
    command: ExchangeCommand,
    *,
    created_at_ms: int,
) -> tuple[PendingJournal, RequestRecord, DeliveryRecord]:
    prepared = prepare_delivery(
        command.body,
        request_id=REQUEST_ID,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
    )
    rendered = render_delivery_lua(prepared, harness.snapshot)
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    intent = DeliveryIntent(
        request_id=REQUEST_ID,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
        original_request_delivery_id=DELIVERY_ID,
        body_json=prepared.body_json.decode("utf-8", errors="strict"),
        request_hash=prepared.request_hash,
        runtime_snapshot=harness.snapshot,
        result_schema_id=command.invocation.result_schema.schema_id,
        recovery_schema_id=recovery_schema.schema_id,
        intended_at_ms=created_at_ms,
        published_at_ms=None,
        rendered_inbox_sha256=hashlib.sha256(rendered).hexdigest(),
        rendered_inbox_size_bytes=len(rendered),
        response_filename=harness.paths.response_path(REQUEST_ID).name,
    )
    journal = PendingJournal(
        header=PendingJournalHeader(
            format="cmo-agent-bridge/pending-journal",
            header_version=1,
            root_key=harness.paths.root_key,
            required_release_id=harness.snapshot.release_id,
        ),
        original=PendingExchange(
            request_id=REQUEST_ID,
            request_hash=prepared.request_hash,
            operation=command.body.operation,
            effective_class=command.invocation.effective_class,
            body_json=prepared.body_json.decode("utf-8", errors="strict"),
            runtime_snapshot=harness.snapshot,
            result_schema_id=command.invocation.result_schema.schema_id,
            recovery_schema_id=recovery_schema.schema_id,
            expected_lineage_id=command.body.expected_lineage_id,
            expected_activation_id=command.body.expected_activation_id,
            delivery_intents=(intent,),
            response_artifact=None,
            settlement=None,
            original_target_request_id=None,
            original_target_request_hash=None,
            revision=0,
            state=PendingPhase.PREPARED,
            created_at_ms=created_at_ms,
            updated_at_ms=created_at_ms,
        ),
        reconcile_attempt=None,
    )
    request = RequestRecord(
        request_id=REQUEST_ID,
        root_key=harness.paths.root_key,
        request_hash=prepared.request_hash,
        operation=command.body.operation,
        operation_class=command.invocation.effective_class,
        state=HostRequestState.PREPARED,
        runtime_snapshot=harness.snapshot,
        result_schema_id=command.invocation.result_schema.schema_id,
        recovery_schema_id=recovery_schema.schema_id,
        body_json=prepared.body_json,
        lineage_id=command.body.expected_lineage_id,
        activation_id=command.body.expected_activation_id,
        result_json=None,
        error_json=None,
        resolution_json=None,
        created_at_ms=created_at_ms,
        updated_at_ms=created_at_ms,
        terminal_at_ms=None,
    )
    delivery = DeliveryRecord(
        delivery_id=DELIVERY_ID,
        request_id=REQUEST_ID,
        delivery_kind="request",
        original_request_delivery_id=DELIVERY_ID,
        intended_at_ms=created_at_ms,
        published_at_ms=None,
        rendered_inbox_sha256=intent.rendered_inbox_sha256,
        rendered_inbox_size_bytes=intent.rendered_inbox_size_bytes,
        response_filename=intent.response_filename,
        response_artifact=None,
        settlement=None,
    )
    return journal, request, delivery


async def _assert_r0_crash_snapshot(
    diagnostic: FileBridgeTransport,
    harness: MatrixHarness,
    command: ExchangeCommand,
    *,
    request_present: bool,
    delivery_present: bool,
) -> None:
    assert not delivery_present or request_present
    async with diagnostic.root_lock:
        loaded = diagnostic.journals.load()
        assert loaded is not None
        expected_journal, expected_request, expected_delivery = _r0_snapshot_models(
            harness,
            command,
            created_at_ms=loaded.journal.original.created_at_ms,
        )
        assert loaded.journal == expected_journal
        observed_request = diagnostic.ledger.get_request(REQUEST_ID)
        observed_delivery = diagnostic.ledger.get_delivery(DELIVERY_ID)
        observed_deliveries = diagnostic.ledger.list_deliveries(REQUEST_ID)
        assert observed_request == (expected_request if request_present else None)
        assert observed_delivery == (expected_delivery if delivery_present else None)
        assert observed_deliveries == ((expected_delivery,) if delivery_present else ())


def _r1_snapshot_models(
    harness: MatrixHarness,
    command: ExchangeCommand,
    *,
    created_at_ms: int,
    published_at_ms: int,
) -> tuple[
    PendingJournal,
    RequestRecord,
    RequestRecord,
    DeliveryRecord,
    DeliveryRecord,
]:
    prepared_journal, prepared_request, prepared_delivery = _r0_snapshot_models(
        harness,
        command,
        created_at_ms=created_at_ms,
    )
    prepared_intent = prepared_journal.original.delivery_intents[0]
    published_intent = DeliveryIntent.model_validate(
        {
            **prepared_intent.model_dump(
                mode="python",
                round_trip=True,
                warnings=False,
            ),
            "published_at_ms": published_at_ms,
        }
    )
    published_original = PendingExchange.model_validate(
        {
            **prepared_journal.original.model_dump(
                mode="python",
                round_trip=True,
                warnings=False,
            ),
            "delivery_intents": (published_intent,),
            "revision": 1,
            "state": PendingPhase.PUBLISHED,
            "updated_at_ms": published_at_ms,
        }
    )
    published_journal = PendingJournal(
        header=prepared_journal.header,
        original=published_original,
        reconcile_attempt=None,
    )
    published_request = RequestRecord.model_validate(
        {
            **prepared_request.model_dump(
                mode="python",
                round_trip=True,
                warnings=False,
            ),
            "state": HostRequestState.PUBLISHED,
            "updated_at_ms": published_at_ms,
        }
    )
    published_delivery = DeliveryRecord.model_validate(
        {
            **prepared_delivery.model_dump(
                mode="python",
                round_trip=True,
                warnings=False,
            ),
            "published_at_ms": published_at_ms,
        }
    )
    return (
        published_journal,
        prepared_request,
        published_request,
        prepared_delivery,
        published_delivery,
    )


async def _assert_r1_crash_snapshot(
    diagnostic: FileBridgeTransport,
    harness: MatrixHarness,
    command: ExchangeCommand,
    *,
    crash_point: str,
    rendered_request: bytes,
) -> None:
    async with diagnostic.root_lock:
        loaded = diagnostic.journals.load()
        assert loaded is not None
        original = loaded.journal.original
        assert original.state is PendingPhase.PUBLISHED
        request_intent = original.delivery_intents[0]
        assert request_intent.published_at_ms is not None
        (
            expected_journal,
            prepared_request,
            published_request,
            prepared_delivery,
            published_delivery,
        ) = _r1_snapshot_models(
            harness,
            command,
            created_at_ms=original.created_at_ms,
            published_at_ms=request_intent.published_at_ms,
        )
        assert loaded.journal == expected_journal
        expected_request = (
            published_request if crash_point == "after_request_published" else prepared_request
        )
        expected_delivery = prepared_delivery if crash_point == "after_r1" else published_delivery
        assert diagnostic.ledger.get_request(REQUEST_ID) == expected_request
        assert diagnostic.ledger.get_delivery(DELIVERY_ID) == expected_delivery
        assert diagnostic.ledger.list_deliveries(REQUEST_ID) == (expected_delivery,)
        assert harness.paths.inbox.read_bytes() == rendered_request
        assert not harness.paths.response_path(REQUEST_ID).exists()


def _r2_snapshot_models(
    harness: MatrixHarness,
    command: ExchangeCommand,
    *,
    created_at_ms: int,
    request_published_at_ms: int,
    cancel_intended_at_ms: int,
) -> tuple[PendingJournal, RequestRecord, DeliveryRecord, DeliveryRecord]:
    (
        published_journal,
        _prepared_request,
        published_request,
        _prepared_request_delivery,
        published_request_delivery,
    ) = _r1_snapshot_models(
        harness,
        command,
        created_at_ms=created_at_ms,
        published_at_ms=request_published_at_ms,
    )
    cancel_delivery = prepare_delivery(
        command.body,
        request_id=REQUEST_ID,
        delivery_id=CANCEL_ID,
        delivery_kind="cancel",
    )
    rendered_cancel = render_delivery_lua(cancel_delivery, harness.snapshot)
    original = published_journal.original
    request_intent = original.delivery_intents[0]
    cancel_intent = DeliveryIntent(
        request_id=REQUEST_ID,
        delivery_id=CANCEL_ID,
        delivery_kind="cancel",
        original_request_delivery_id=DELIVERY_ID,
        body_json=cancel_delivery.body_json.decode("utf-8", errors="strict"),
        request_hash=cancel_delivery.request_hash,
        runtime_snapshot=harness.snapshot,
        result_schema_id=original.result_schema_id,
        recovery_schema_id=original.recovery_schema_id,
        intended_at_ms=cancel_intended_at_ms,
        published_at_ms=None,
        rendered_inbox_sha256=hashlib.sha256(rendered_cancel).hexdigest(),
        rendered_inbox_size_bytes=len(rendered_cancel),
        response_filename=harness.paths.response_path(REQUEST_ID).name,
    )
    cancel_intended_original = PendingExchange.model_validate(
        {
            **original.model_dump(
                mode="python",
                round_trip=True,
                warnings=False,
            ),
            "delivery_intents": (request_intent, cancel_intent),
            "revision": 2,
            "state": PendingPhase.CANCEL_PUBLISHED,
            "updated_at_ms": cancel_intended_at_ms,
        }
    )
    cancel_intended_journal = PendingJournal(
        header=published_journal.header,
        original=cancel_intended_original,
        reconcile_attempt=None,
    )
    cancel_delivery_record = DeliveryRecord(
        delivery_id=CANCEL_ID,
        request_id=REQUEST_ID,
        delivery_kind="cancel",
        original_request_delivery_id=DELIVERY_ID,
        intended_at_ms=cancel_intended_at_ms,
        published_at_ms=None,
        rendered_inbox_sha256=cancel_intent.rendered_inbox_sha256,
        rendered_inbox_size_bytes=cancel_intent.rendered_inbox_size_bytes,
        response_filename=cancel_intent.response_filename,
        response_artifact=None,
        settlement=None,
    )
    return (
        cancel_intended_journal,
        published_request,
        published_request_delivery,
        cancel_delivery_record,
    )


async def _assert_r2_crash_snapshot(
    diagnostic: FileBridgeTransport,
    harness: MatrixHarness,
    command: ExchangeCommand,
    *,
    crash_point: str,
    rendered_request: bytes,
) -> None:
    async with diagnostic.root_lock:
        loaded = diagnostic.journals.load()
        assert loaded is not None
        original = loaded.journal.original
        assert original.state is PendingPhase.CANCEL_PUBLISHED
        assert original.revision == 2
        assert len(original.delivery_intents) == 2
        request_intent, cancel_intent = original.delivery_intents
        assert request_intent.published_at_ms is not None
        assert cancel_intent.published_at_ms is None
        (
            expected_journal,
            expected_request,
            expected_request_delivery,
            expected_cancel_delivery,
        ) = _r2_snapshot_models(
            harness,
            command,
            created_at_ms=original.created_at_ms,
            request_published_at_ms=request_intent.published_at_ms,
            cancel_intended_at_ms=cancel_intent.intended_at_ms,
        )
        assert loaded.journal == expected_journal
        assert diagnostic.ledger.get_request(REQUEST_ID) == expected_request
        assert diagnostic.ledger.get_delivery(DELIVERY_ID) == expected_request_delivery
        expected_cancel = (
            expected_cancel_delivery if crash_point == "after_cancel_delivery_insert" else None
        )
        assert diagnostic.ledger.get_delivery(CANCEL_ID) == expected_cancel
        expected_deliveries = (
            (expected_request_delivery, expected_cancel_delivery)
            if expected_cancel is not None
            else (expected_request_delivery,)
        )
        assert diagnostic.ledger.list_deliveries(REQUEST_ID) == expected_deliveries
        assert harness.paths.inbox.read_bytes() == rendered_request
        assert not harness.paths.response_path(REQUEST_ID).exists()


async def _assert_cancel_replace_crash_snapshot(
    diagnostic: FileBridgeTransport,
    harness: MatrixHarness,
    command: ExchangeCommand,
    *,
    rendered_cancel: bytes,
) -> None:
    async with diagnostic.root_lock:
        loaded = diagnostic.journals.load()
        assert loaded is not None
        original = loaded.journal.original
        assert original.state is PendingPhase.CANCEL_PUBLISHED
        assert original.revision == 2
        assert len(original.delivery_intents) == 2
        request_intent, cancel_intent = original.delivery_intents
        assert request_intent.published_at_ms is not None
        assert cancel_intent.published_at_ms is None
        (
            expected_journal,
            expected_request,
            expected_request_delivery,
            expected_cancel_delivery,
        ) = _r2_snapshot_models(
            harness,
            command,
            created_at_ms=original.created_at_ms,
            request_published_at_ms=request_intent.published_at_ms,
            cancel_intended_at_ms=cancel_intent.intended_at_ms,
        )
        assert loaded.journal == expected_journal
        assert diagnostic.ledger.get_request(REQUEST_ID) == expected_request
        assert diagnostic.ledger.get_delivery(DELIVERY_ID) == expected_request_delivery
        assert diagnostic.ledger.get_delivery(CANCEL_ID) == expected_cancel_delivery
        assert diagnostic.ledger.list_deliveries(REQUEST_ID) == (
            expected_request_delivery,
            expected_cancel_delivery,
        )
        assert harness.paths.inbox.read_bytes() == rendered_cancel
        assert not harness.paths.response_path(REQUEST_ID).exists()


def _r3_snapshot_models(
    harness: MatrixHarness,
    command: ExchangeCommand,
    *,
    created_at_ms: int,
    request_published_at_ms: int,
    cancel_intended_at_ms: int,
    cancel_published_at_ms: int,
) -> tuple[
    PendingJournal,
    RequestRecord,
    RequestRecord,
    DeliveryRecord,
    DeliveryRecord,
    DeliveryRecord,
]:
    (
        cancel_intended_journal,
        published_request,
        published_request_delivery,
        unpublished_cancel_delivery,
    ) = _r2_snapshot_models(
        harness,
        command,
        created_at_ms=created_at_ms,
        request_published_at_ms=request_published_at_ms,
        cancel_intended_at_ms=cancel_intended_at_ms,
    )
    request_intent, cancel_intent = cancel_intended_journal.original.delivery_intents
    published_cancel_intent = DeliveryIntent.model_validate(
        {
            **cancel_intent.model_dump(
                mode="python",
                round_trip=True,
                warnings=False,
            ),
            "published_at_ms": cancel_published_at_ms,
        }
    )
    cancel_published_original = PendingExchange.model_validate(
        {
            **cancel_intended_journal.original.model_dump(
                mode="python",
                round_trip=True,
                warnings=False,
            ),
            "delivery_intents": (request_intent, published_cancel_intent),
            "revision": 3,
            "state": PendingPhase.CANCEL_PUBLISHED,
            "updated_at_ms": cancel_published_at_ms,
        }
    )
    cancel_published_journal = PendingJournal(
        header=cancel_intended_journal.header,
        original=cancel_published_original,
        reconcile_attempt=None,
    )
    cancel_published_request = RequestRecord.model_validate(
        {
            **published_request.model_dump(
                mode="python",
                round_trip=True,
                warnings=False,
            ),
            "state": HostRequestState.CANCEL_PUBLISHED,
            "updated_at_ms": cancel_published_at_ms,
        }
    )
    published_cancel_delivery = DeliveryRecord.model_validate(
        {
            **unpublished_cancel_delivery.model_dump(
                mode="python",
                round_trip=True,
                warnings=False,
            ),
            "published_at_ms": cancel_published_at_ms,
        }
    )
    return (
        cancel_published_journal,
        published_request,
        cancel_published_request,
        published_request_delivery,
        unpublished_cancel_delivery,
        published_cancel_delivery,
    )


async def _assert_r3_crash_snapshot(
    diagnostic: FileBridgeTransport,
    harness: MatrixHarness,
    command: ExchangeCommand,
    *,
    crash_point: str,
    rendered_cancel: bytes,
) -> None:
    async with diagnostic.root_lock:
        loaded = diagnostic.journals.load()
        assert loaded is not None
        original = loaded.journal.original
        assert original.state is PendingPhase.CANCEL_PUBLISHED
        assert original.revision == 3
        assert len(original.delivery_intents) == 2
        request_intent, cancel_intent = original.delivery_intents
        assert request_intent.published_at_ms is not None
        assert cancel_intent.published_at_ms is not None
        (
            expected_journal,
            published_request,
            cancel_published_request,
            expected_request_delivery,
            unpublished_cancel_delivery,
            published_cancel_delivery,
        ) = _r3_snapshot_models(
            harness,
            command,
            created_at_ms=original.created_at_ms,
            request_published_at_ms=request_intent.published_at_ms,
            cancel_intended_at_ms=cancel_intent.intended_at_ms,
            cancel_published_at_ms=cancel_intent.published_at_ms,
        )
        assert loaded.journal == expected_journal
        expected_request = (
            cancel_published_request
            if crash_point == "after_request_cancel_published"
            else published_request
        )
        expected_cancel_delivery = (
            unpublished_cancel_delivery
            if crash_point == "after_cancel_r3"
            else published_cancel_delivery
        )
        assert diagnostic.ledger.get_request(REQUEST_ID) == expected_request
        assert diagnostic.ledger.get_delivery(DELIVERY_ID) == expected_request_delivery
        assert diagnostic.ledger.get_delivery(CANCEL_ID) == expected_cancel_delivery
        assert diagnostic.ledger.list_deliveries(REQUEST_ID) == (
            expected_request_delivery,
            expected_cancel_delivery,
        )
        assert harness.paths.inbox.read_bytes() == rendered_cancel
        assert not harness.paths.response_path(REQUEST_ID).exists()


async def _assert_response_crash_snapshot(
    diagnostic: FileBridgeTransport,
    harness: MatrixHarness,
    command: ExchangeCommand,
    checkpoint_payload: dict[str, str],
    *,
    crash_point: str,
    rendered_cancel: bytes,
) -> ResponseArtifact | None:
    async with diagnostic.root_lock:
        loaded = diagnostic.journals.load()
        assert loaded is not None
        original = loaded.journal.original
        assert len(original.delivery_intents) == 2
        request_intent, cancel_intent = original.delivery_intents
        assert request_intent.published_at_ms is not None
        assert cancel_intent.published_at_ms is not None
        response_path = harness.paths.response_path(REQUEST_ID)
        raw = response_path.read_bytes()
        assert len(raw) == int(checkpoint_payload["response_size_bytes"])
        assert hashlib.sha256(raw).hexdigest() == checkpoint_payload["response_sha256"]
        accepted = parse_inst_response(raw, loaded.original.expectation)
        assert accepted.delivery_kind == "cancel"
        assert accepted.envelope.request_id == REQUEST_ID
        assert accepted.envelope.delivery_id == CANCEL_ID
        assert accepted.cancel_ack is not None
        assert accepted.cancel_ack.status == "cancelled"
        assert accepted.cancel_ack.original_delivery_id == DELIVERY_ID
        (
            expected_r3_journal,
            _published_request,
            cancel_published_request,
            expected_request_delivery,
            _unpublished_cancel_delivery,
            published_cancel_delivery,
        ) = _r3_snapshot_models(
            harness,
            command,
            created_at_ms=original.created_at_ms,
            request_published_at_ms=request_intent.published_at_ms,
            cancel_intended_at_ms=cancel_intent.intended_at_ms,
            cancel_published_at_ms=cancel_intent.published_at_ms,
        )
        if crash_point == "after_cancel_response_file":
            assert loaded.journal == expected_r3_journal
            assert original.response_artifact is None
            assert diagnostic.ledger.get_request(REQUEST_ID) == cancel_published_request
            assert diagnostic.ledger.get_delivery(DELIVERY_ID) == expected_request_delivery
            assert diagnostic.ledger.get_delivery(CANCEL_ID) == published_cancel_delivery
            assert diagnostic.ledger.list_deliveries(REQUEST_ID) == (
                expected_request_delivery,
                published_cancel_delivery,
            )
            assert harness.paths.inbox.read_bytes() == rendered_cancel
            return None

        artifact = original.response_artifact
        assert artifact is not None
        expected_artifact = ResponseArtifact(
            filename=response_path.name,
            sha256=checkpoint_payload["response_sha256"],
            size_bytes=int(checkpoint_payload["response_size_bytes"]),
            accepted_at_ms=artifact.accepted_at_ms,
            accepted_response=accepted,
        )
        assert artifact == expected_artifact
        response_accepted_original = PendingExchange.model_validate(
            {
                **expected_r3_journal.original.model_dump(
                    mode="python",
                    round_trip=True,
                    warnings=False,
                ),
                "response_artifact": expected_artifact,
                "settlement": accepted.settlement,
                "revision": 4,
                "state": PendingPhase.RESPONSE_ACCEPTED,
                "updated_at_ms": original.updated_at_ms,
            }
        )
        expected_r4_journal = PendingJournal(
            header=expected_r3_journal.header,
            original=response_accepted_original,
            reconcile_attempt=None,
        )
        assert loaded.journal == expected_r4_journal
        response_accepted_request = RequestRecord.model_validate(
            {
                **cancel_published_request.model_dump(
                    mode="python",
                    round_trip=True,
                    warnings=False,
                ),
                "state": HostRequestState.RESPONSE_ACCEPTED,
                "updated_at_ms": original.updated_at_ms,
            }
        )
        expected_request = (
            response_accepted_request
            if crash_point == "after_request_response_accepted"
            else cancel_published_request
        )
        response_cancel_delivery = DeliveryRecord.model_validate(
            {
                **published_cancel_delivery.model_dump(
                    mode="python",
                    round_trip=True,
                    warnings=False,
                ),
                "response_artifact": expected_artifact,
                "settlement": accepted.settlement,
            }
        )
        expected_cancel_delivery = (
            published_cancel_delivery
            if crash_point == "after_response_r4"
            else response_cancel_delivery
        )
        assert diagnostic.ledger.get_request(REQUEST_ID) == expected_request
        assert diagnostic.ledger.get_delivery(DELIVERY_ID) == expected_request_delivery
        assert diagnostic.ledger.get_delivery(CANCEL_ID) == expected_cancel_delivery
        assert diagnostic.ledger.list_deliveries(REQUEST_ID) == (
            expected_request_delivery,
            expected_cancel_delivery,
        )
        assert harness.paths.inbox.read_bytes() == rendered_cancel
        return expected_artifact


async def _assert_late_handoff_crash_snapshot(
    diagnostic: FileBridgeTransport,
    harness: MatrixHarness,
    command: ExchangeCommand,
    checkpoint_payload: dict[str, str],
    *,
    crash_point: str,
) -> tuple[RequestRecord, tuple[DeliveryRecord, ...], ResponseArtifact]:
    async with diagnostic.root_lock:
        loaded = diagnostic.journals.load()
        request = diagnostic.ledger.get_request(REQUEST_ID)
        deliveries = diagnostic.ledger.list_deliveries(REQUEST_ID)
        assert request is not None
        assert len(deliveries) == 2
        request_delivery, cancel_delivery = deliveries
        assert request_delivery.delivery_id == DELIVERY_ID
        assert request_delivery.published_at_ms is not None
        assert cancel_delivery.delivery_id == CANCEL_ID
        assert cancel_delivery.published_at_ms is not None
        artifact = cancel_delivery.response_artifact
        assert artifact is not None
        response_path = harness.paths.response_path(REQUEST_ID)
        raw = response_path.read_bytes()
        expected_sha256 = checkpoint_payload["response_sha256"]
        expected_size = int(checkpoint_payload["response_size_bytes"])
        assert hashlib.sha256(raw).hexdigest() == expected_sha256
        assert len(raw) == expected_size
        request_hash = prepare_delivery(
            command.body,
            request_id=REQUEST_ID,
            delivery_id=DELIVERY_ID,
            delivery_kind="request",
        ).request_hash
        expectation = ResponseExpectation(
            request_id=REQUEST_ID,
            allowed_deliveries=(
                AllowedDelivery(delivery_id=DELIVERY_ID, delivery_kind="request"),
                AllowedDelivery(delivery_id=CANCEL_ID, delivery_kind="cancel"),
            ),
            request_hash=request_hash,
            expected_lineage_id=LINEAGE_ID,
            expected_activation_id=ACTIVATION_ID,
            status_bootstrap=False,
            activation_candidate=None,
            runtime_snapshot=harness.snapshot,
            invocation=command.invocation,
        )
        accepted = parse_inst_response(raw, expectation)
        expected_artifact = ResponseArtifact(
            filename=response_path.name,
            sha256=expected_sha256,
            size_bytes=expected_size,
            accepted_at_ms=artifact.accepted_at_ms,
            accepted_response=accepted,
        )
        assert artifact == expected_artifact
        assert cancel_delivery.settlement == accepted.settlement
        (
            expected_r3_journal,
            _published_request,
            cancel_published_request,
            expected_request_delivery,
            _unpublished_cancel_delivery,
            published_cancel_delivery,
        ) = _r3_snapshot_models(
            harness,
            command,
            created_at_ms=request.created_at_ms,
            request_published_at_ms=request_delivery.published_at_ms,
            cancel_intended_at_ms=cancel_delivery.intended_at_ms,
            cancel_published_at_ms=cancel_delivery.published_at_ms,
        )
        expected_cancel_delivery = DeliveryRecord.model_validate(
            {
                **published_cancel_delivery.model_dump(
                    mode="python",
                    round_trip=True,
                    warnings=False,
                ),
                "response_artifact": expected_artifact,
                "settlement": accepted.settlement,
            }
        )
        assert request_delivery == expected_request_delivery
        assert cancel_delivery == expected_cancel_delivery

        expected_state = {
            "after_idle_replace": HostRequestState.RESPONSE_ACCEPTED,
            "after_idle_r5": HostRequestState.RESPONSE_ACCEPTED,
            "after_request_idle_published": HostRequestState.IDLE_PUBLISHED,
            "after_terminal_cancelled": HostRequestState.CANCELLED,
            "after_journal_delete": HostRequestState.CANCELLED,
        }[crash_point]
        expected_request = RequestRecord.model_validate(
            {
                **cancel_published_request.model_dump(
                    mode="python",
                    round_trip=True,
                    warnings=False,
                ),
                "state": expected_state,
                "updated_at_ms": request.updated_at_ms,
                "terminal_at_ms": (
                    request.updated_at_ms if expected_state is HostRequestState.CANCELLED else None
                ),
            }
        )
        assert request == expected_request

        if crash_point == "after_journal_delete":
            assert loaded is None
            assert not harness.paths.pending_file.exists()
        else:
            assert loaded is not None
            original = loaded.journal.original
            if crash_point == "after_idle_replace":
                expected_phase = PendingPhase.RESPONSE_ACCEPTED
                expected_revision = 4
            else:
                expected_phase = PendingPhase.IDLE_PUBLISHED
                expected_revision = 5
            expected_original = PendingExchange.model_validate(
                {
                    **expected_r3_journal.original.model_dump(
                        mode="python",
                        round_trip=True,
                        warnings=False,
                    ),
                    "response_artifact": expected_artifact,
                    "settlement": accepted.settlement,
                    "revision": expected_revision,
                    "state": expected_phase,
                    "updated_at_ms": original.updated_at_ms,
                }
            )
            expected_journal = PendingJournal(
                header=expected_r3_journal.header,
                original=expected_original,
                reconcile_attempt=None,
            )
            assert loaded.journal == expected_journal
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        assert response_path.is_file()
        return expected_request, deliveries, expected_artifact
    raise AssertionError("late handoff diagnostic lock exited without a snapshot")


def _completed_result(command: ExchangeCommand) -> JsonValue:
    validated = command.invocation.result_adapter.validate_python(
        {
            "unit_guid": "UNIT-1",
            "name": "Phase A",
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


def _normalized_recovery_result(command: ExchangeCommand) -> JsonValue:
    recovery_adapter = command.invocation.recovery_adapter
    assert recovery_adapter is not None
    validated = recovery_adapter.validate_python(
        {
            "unit_guid": "UNIT-1",
            "name": "Recovery cancellation",
            "speed": None,
            "altitude": None,
            "heading": None,
            "course": None,
        }
    )
    return cast(JsonValue, recovery_adapter.dump_python(validated, mode="json"))


def _terminal_response_bytes(
    harness: MatrixHarness,
    command: ExchangeCommand,
    case: str,
) -> tuple[bytes, JsonValue | None, CancelAckResult | None, UUID]:
    request_hash = prepare_delivery(
        command.body,
        request_id=REQUEST_ID,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
    ).request_hash
    acknowledgement: CancelAckResult | None
    terminal_result: JsonValue | None
    if case == "cancelled_ack":
        terminal_result = None
        acknowledgement = CancelAckResult(
            request_id=REQUEST_ID,
            request_hash=request_hash,
            original_delivery_id=DELIVERY_ID,
            status="cancelled",
            result=None,
        )
        responding_delivery_id = CANCEL_ID
        envelope_result = cast(JsonValue, acknowledgement.model_dump(mode="json"))
    elif case == "completed_ack":
        terminal_result = _normalized_recovery_result(command)
        acknowledgement = CancelAckResult(
            request_id=REQUEST_ID,
            request_hash=request_hash,
            original_delivery_id=DELIVERY_ID,
            status="completed",
            result=terminal_result,
        )
        responding_delivery_id = CANCEL_ID
        envelope_result = cast(JsonValue, acknowledgement.model_dump(mode="json"))
    elif case == "original_completion":
        terminal_result = _completed_result(command)
        acknowledgement = None
        responding_delivery_id = DELIVERY_ID
        envelope_result = terminal_result
    else:
        raise AssertionError("XS10 has an unknown terminal response case")
    envelope = ResponseEnvelope(
        protocol=harness.snapshot.protocol,
        request_id=REQUEST_ID,
        delivery_id=responding_delivery_id,
        request_hash=request_hash,
        ok=True,
        result=envelope_result,
        error=None,
        scenario_time="2026-07-12T13:00:00Z",
        scenario_lineage_id=LINEAGE_ID,
        activation_id=ACTIVATION_ID,
        operation_manifest_sha256=harness.snapshot.operation_manifest_sha256,
        bridge_version=harness.snapshot.runtime_version,
        runtime_tag=harness.snapshot.runtime_tag,
        runtime_asset_sha256=harness.snapshot.runtime_asset_sha256,
        release_id=harness.snapshot.release_id,
    )
    inner = _canonical_json_bytes(envelope.model_dump(mode="json", exclude_none=False)).decode(
        "utf-8"
    )
    return (
        _canonical_json_bytes({"Comments": inner}),
        terminal_result,
        acknowledgement,
        responding_delivery_id,
    )


async def _respond_after_dual_wait(
    harness: MatrixHarness,
    transport: FileBridgeTransport,
    command: ExchangeCommand,
    case: str,
    dual_wait_started: asyncio.Event,
) -> bytes:
    await asyncio.wait_for(dual_wait_started.wait(), timeout=2)
    loaded = transport.journals.load()
    request = transport.ledger.get_request(REQUEST_ID)
    deliveries = transport.ledger.list_deliveries(REQUEST_ID)
    assert loaded is not None
    assert loaded.journal.original.state is PendingPhase.CANCEL_PUBLISHED
    assert loaded.journal.original.revision == 3
    assert request is not None and request.state is HostRequestState.CANCEL_PUBLISHED
    assert tuple(delivery.delivery_id for delivery in deliveries) == (
        DELIVERY_ID,
        CANCEL_ID,
    )
    assert all(delivery.published_at_ms is not None for delivery in deliveries)
    request_intent, cancel_intent = loaded.journal.original.delivery_intents
    assert request_intent.delivery_id == DELIVERY_ID
    assert request_intent.published_at_ms is not None
    assert cancel_intent.delivery_id == CANCEL_ID
    assert cancel_intent.published_at_ms is not None
    cancel_delivery = PreparedDelivery(
        request_id=REQUEST_ID,
        delivery_id=CANCEL_ID,
        delivery_kind="cancel",
        request_hash=cancel_intent.request_hash,
        body_json=cancel_intent.body_json.encode("utf-8", errors="strict"),
    )
    assert harness.paths.inbox.read_bytes() == render_delivery_lua(
        cancel_delivery,
        harness.snapshot,
    )
    response_path = harness.paths.response_path(REQUEST_ID)
    assert not response_path.exists()
    raw, _terminal_result, _acknowledgement, _responding_id = _terminal_response_bytes(
        harness,
        command,
        case,
    )
    with response_path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    assert response_path.read_bytes() == raw
    return raw


def _peer(harness: MatrixHarness, trace: list[str]) -> FakeFileBridgePeer:
    return FakeFileBridgePeer(
        paths=harness.paths,
        runtime_snapshot=harness.snapshot,
        registry=OPERATION_REGISTRY,
        scenario_lineage_id=LINEAGE_ID,
        poll_seconds=0.001,
        trace=trace,
    )


async def _respond_to_exact_cancel(
    harness: MatrixHarness,
    command: ExchangeCommand,
    *,
    rendered_request: bytes,
    original_seen: asyncio.Event,
    observed_cancels: list[PreparedDelivery],
) -> CancelAckResult:
    cancel_delivery = prepare_delivery(
        command.body,
        request_id=REQUEST_ID,
        delivery_id=CANCEL_ID,
        delivery_kind="cancel",
    )
    rendered_cancel = render_delivery_lua(cancel_delivery, harness.snapshot)
    response_path = harness.paths.response_path(REQUEST_ID)
    deadline = time.monotonic() + 5
    while True:
        try:
            raw = harness.paths.inbox.read_bytes()
        except FileNotFoundError:
            raw = b""
        if raw == rendered_request:
            assert not response_path.exists()
            original_seen.set()
        elif raw == rendered_cancel:
            assert observed_cancels == []
            observed_cancels.append(cancel_delivery)
            acknowledgement = CancelAckResult(
                request_id=REQUEST_ID,
                request_hash=cancel_delivery.request_hash,
                original_delivery_id=DELIVERY_ID,
                status="cancelled",
                result=None,
            )
            envelope = ResponseEnvelope(
                protocol=harness.snapshot.protocol,
                request_id=REQUEST_ID,
                delivery_id=CANCEL_ID,
                request_hash=cancel_delivery.request_hash,
                ok=True,
                result=cast(
                    JsonValue,
                    acknowledgement.model_dump(mode="json"),
                ),
                error=None,
                scenario_time="2026-07-12T13:00:00Z",
                scenario_lineage_id=LINEAGE_ID,
                activation_id=ACTIVATION_ID,
                operation_manifest_sha256=harness.snapshot.operation_manifest_sha256,
                bridge_version=harness.snapshot.runtime_version,
                runtime_tag=harness.snapshot.runtime_tag,
                runtime_asset_sha256=harness.snapshot.runtime_asset_sha256,
                release_id=harness.snapshot.release_id,
            )
            inner = _canonical_json_bytes(
                envelope.model_dump(mode="json", exclude_none=False)
            ).decode("utf-8")
            response_raw = _canonical_json_bytes({"Comments": inner})
            with response_path.open("xb") as stream:
                stream.write(response_raw)
                stream.flush()
                os.fsync(stream.fileno())
            return acknowledgement
        elif raw != render_idle_lua():
            raise AssertionError("cancel responder observed unexplained inbox bytes")
        if time.monotonic() >= deadline:
            raise AssertionError(
                "cancel responder did not observe the exact cancel within five seconds"
            )
        await asyncio.sleep(0.001)


async def _assert_preexisting_cancel_recovers_without_delivery_publication(
    harness: MatrixHarness,
    command: ExchangeCommand,
    diagnostic: FileBridgeTransport,
    checkpoint_payload: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_delivery = prepare_delivery(
        command.body,
        request_id=REQUEST_ID,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
    )
    rendered_request = render_delivery_lua(request_delivery, harness.snapshot)
    original_seen = asyncio.Event()
    observed_cancels: list[PreparedDelivery] = []
    responder_task = asyncio.create_task(
        _respond_to_exact_cancel(
            harness,
            command,
            rendered_request=rendered_request,
            original_seen=original_seen,
            observed_cancels=observed_cancels,
        )
    )
    try:
        acknowledgement = await _hard_task_result(
            responder_task,
            timeout_seconds=1,
            description="preexisting-cancel responder",
        )
    finally:
        await _cancel_and_drain_task(
            responder_task,
            timeout_seconds=1,
            description="preexisting-cancel responder",
        )

    response_path = harness.paths.response_path(REQUEST_ID)
    assert response_path.is_file()
    assert original_seen.is_set() is False
    assert acknowledgement.request_id == REQUEST_ID
    assert acknowledgement.request_hash == request_delivery.request_hash
    assert acknowledgement.original_delivery_id == DELIVERY_ID
    assert acknowledgement.status == "cancelled"
    assert observed_cancels == [
        prepare_delivery(
            command.body,
            request_id=REQUEST_ID,
            delivery_id=CANCEL_ID,
            delivery_kind="cancel",
        )
    ]

    transport = _make_transport(harness)
    assert transport is not diagnostic
    assert transport.root_lock is not diagnostic.root_lock
    assert transport.database is not diagnostic.database
    reports: list[RecoveryReport] = []
    unexpected_publications: list[PreparedDelivery] = []
    second_session_idle_publications = 0
    forbidden_uuid_calls = 0
    cleanup_trace: list[str] = []
    terminal_request: RequestRecord | None = None
    terminal_deliveries: tuple[DeliveryRecord, ...] = ()
    installed_recover = RecoveryManager.recover_pending
    installed_publish = InboxPublisher.publish_delivery
    installed_idle = InboxPublisher.publish_idle
    installed_exit = RootLock.__aexit__
    installed_delete = PinnedResponseCleanup.delete

    async def capture_recovery(manager: RecoveryManager) -> RecoveryReport:
        report = await installed_recover(manager)
        reports.append(report)
        return report

    def forbid_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        if publisher is transport.inbox:
            unexpected_publications.append(delivery)
            raise AssertionError("startup recovery republished an already-present delivery")
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)

    def capture_idle(publisher: InboxPublisher) -> None:
        nonlocal second_session_idle_publications
        installed_idle(publisher)
        if publisher is transport.inbox:
            second_session_idle_publications += 1

    def forbid_new_uuid4() -> UUID:
        nonlocal forbidden_uuid_calls
        forbidden_uuid_calls += 1
        raise AssertionError("published cancel recovery requested a replacement UUID")

    async def capture_unlock(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is transport.root_lock:
            assert response_path.is_file()
        result = await installed_exit(lock, exc_type, exc, traceback)
        if lock is transport.root_lock:
            assert response_path.is_file()
            cleanup_trace.append("unlock.returned")
        return result

    def capture_cleanup(cleanup: PinnedResponseCleanup) -> None:
        cleanup_trace.append("cleanup.delete.entered")
        assert cleanup_trace == ["unlock.returned", "cleanup.delete.entered"]
        assert response_path.is_file()
        installed_delete(cleanup)
        assert not response_path.exists()
        cleanup_trace.append("cleanup.delete.returned")

    monkeypatch.setattr(RecoveryManager, "recover_pending", capture_recovery)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", forbid_publication)
    monkeypatch.setattr(InboxPublisher, "publish_idle", capture_idle)
    monkeypatch.setattr(
        recovery_module,
        "uuid",
        SimpleNamespace(uuid4=forbid_new_uuid4),
    )
    monkeypatch.setattr(RootLock, "__aexit__", capture_unlock)
    monkeypatch.setattr(PinnedResponseCleanup, "delete", capture_cleanup)

    async def run_second_session() -> None:
        nonlocal terminal_request, terminal_deliveries
        async with transport.session():
            assert reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.SETTLED,
                    request_id=REQUEST_ID,
                    request_state=HostRequestState.CANCELLED,
                    response_cleanup_required=True,
                )
            ]
            assert forbidden_uuid_calls == 0
            assert unexpected_publications == []
            assert second_session_idle_publications == 1
            request = transport.ledger.get_request(REQUEST_ID)
            deliveries = transport.ledger.list_deliveries(REQUEST_ID)
            assert request is not None and request.state is HostRequestState.CANCELLED
            assert request.terminal_at_ms is not None
            assert tuple(delivery.delivery_id for delivery in deliveries) == (
                DELIVERY_ID,
                CANCEL_ID,
            )
            request_record, cancel_record = deliveries
            assert request_record.delivery_kind == "request"
            assert request_record.published_at_ms is not None
            assert request_record.response_artifact is None
            assert cancel_record.delivery_kind == "cancel"
            assert cancel_record.original_request_delivery_id == DELIVERY_ID
            assert cancel_record.published_at_ms is not None
            artifact = cancel_record.response_artifact
            assert artifact is not None
            accepted = artifact.accepted_response
            assert accepted.delivery_kind == "cancel"
            assert accepted.envelope.delivery_id == CANCEL_ID
            assert accepted.cancel_ack is not None
            assert accepted.cancel_ack.status == "cancelled"
            assert accepted.cancel_ack.original_delivery_id == DELIVERY_ID
            assert accepted.settlement == CancelledSettlement(state="cancelled")
            assert transport.journals.load() is None
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            assert response_path.is_file()
            terminal_request = request
            terminal_deliveries = deliveries

    second_session_task = asyncio.create_task(run_second_session())
    await _hard_task_result(
        second_session_task,
        timeout_seconds=5,
        description="preexisting-cancel recovery session",
    )

    assert forbidden_uuid_calls == 0
    assert unexpected_publications == []
    assert int(checkpoint_payload["request_publications"]) == 1
    assert int(checkpoint_payload["cancel_publications"]) == 1
    assert int(checkpoint_payload["idle_publications"]) + second_session_idle_publications == 1
    assert cleanup_trace == [
        "unlock.returned",
        "cleanup.delete.entered",
        "cleanup.delete.returned",
    ]
    assert terminal_request is not None
    assert len(terminal_deliveries) == 2
    assert not response_path.exists()
    assert not harness.paths.pending_file.exists()

    audit = _make_transport(harness)
    assert audit is not diagnostic and audit is not transport
    assert audit.root_lock is not diagnostic.root_lock
    assert audit.root_lock is not transport.root_lock
    assert audit.database is not diagnostic.database
    assert audit.database is not transport.database
    async with audit.root_lock:
        assert audit.journals.load() is None
        assert audit.ledger.get_request(REQUEST_ID) == terminal_request
        assert audit.ledger.list_deliveries(REQUEST_ID) == terminal_deliveries
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        assert not response_path.exists()


async def _assert_response_acceptance_recovers_exact_raw_artifact(
    harness: MatrixHarness,
    diagnostic: FileBridgeTransport,
    checkpoint_payload: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    *,
    crash_point: str,
) -> None:
    expected_record_calls = {
        "after_cancel_response_file": 1,
        "after_response_r4": 1,
        "after_response_record": 0,
        "after_request_response_accepted": 0,
    }[crash_point]
    expected_response_transition_calls = {
        "after_cancel_response_file": 1,
        "after_response_r4": 1,
        "after_response_record": 1,
        "after_request_response_accepted": 0,
    }[crash_point]
    response_path = harness.paths.response_path(REQUEST_ID)
    raw = response_path.read_bytes()
    expected_sha256 = checkpoint_payload["response_sha256"]
    expected_size = int(checkpoint_payload["response_size_bytes"])
    assert hashlib.sha256(raw).hexdigest() == expected_sha256
    assert len(raw) == expected_size

    transport = _make_transport(harness)
    assert transport is not diagnostic
    assert transport.root_lock is not diagnostic.root_lock
    assert transport.database is not diagnostic.database
    reports: list[RecoveryReport] = []
    unexpected_publications: list[PreparedDelivery] = []
    second_session_idle_publications = 0
    forbidden_uuid_calls = 0
    record_calls: list[ResponseArtifact] = []
    response_transition_calls: list[RequestRecord] = []
    cleanup_trace: list[str] = []
    terminal_request: RequestRecord | None = None
    terminal_deliveries: tuple[DeliveryRecord, ...] = ()
    installed_recover = RecoveryManager.recover_pending
    installed_publish = InboxPublisher.publish_delivery
    installed_idle = InboxPublisher.publish_idle
    installed_record = RequestLedger.record_response
    installed_transition = RequestLedger.transition
    installed_exit = RootLock.__aexit__
    installed_delete = PinnedResponseCleanup.delete

    async def capture_recovery(manager: RecoveryManager) -> RecoveryReport:
        report = await installed_recover(manager)
        reports.append(report)
        return report

    def forbid_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        if publisher is transport.inbox:
            unexpected_publications.append(delivery)
            raise AssertionError("response recovery republished an existing delivery")
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)

    def capture_idle(publisher: InboxPublisher) -> None:
        nonlocal second_session_idle_publications
        installed_idle(publisher)
        if publisher is transport.inbox:
            second_session_idle_publications += 1

    def forbid_new_uuid4() -> UUID:
        nonlocal forbidden_uuid_calls
        forbidden_uuid_calls += 1
        raise AssertionError("response recovery requested a replacement UUID")

    def capture_record(
        ledger: RequestLedger,
        artifact: ResponseArtifact,
    ) -> DeliveryRecord:
        result = installed_record(ledger, artifact)
        if ledger is transport.ledger:
            assert artifact.filename == response_path.name
            assert artifact.sha256 == expected_sha256
            assert artifact.size_bytes == expected_size
            assert artifact.accepted_response.envelope.delivery_id == CANCEL_ID
            record_calls.append(artifact)
        return result

    def capture_transition(
        ledger: RequestLedger,
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
            ledger,
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        if ledger is transport.ledger and new_state is HostRequestState.RESPONSE_ACCEPTED:
            response_transition_calls.append(result)
        return result

    async def capture_unlock(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is transport.root_lock:
            assert response_path.is_file()
        result = await installed_exit(lock, exc_type, exc, traceback)
        if lock is transport.root_lock:
            assert response_path.is_file()
            cleanup_trace.append("unlock.returned")
        return result

    def capture_cleanup(cleanup: PinnedResponseCleanup) -> None:
        cleanup_trace.append("cleanup.delete.entered")
        assert cleanup_trace == ["unlock.returned", "cleanup.delete.entered"]
        assert response_path.is_file()
        installed_delete(cleanup)
        assert not response_path.exists()
        cleanup_trace.append("cleanup.delete.returned")

    monkeypatch.setattr(RecoveryManager, "recover_pending", capture_recovery)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", forbid_publication)
    monkeypatch.setattr(InboxPublisher, "publish_idle", capture_idle)
    monkeypatch.setattr(RequestLedger, "record_response", capture_record)
    monkeypatch.setattr(RequestLedger, "transition", capture_transition)
    monkeypatch.setattr(
        recovery_module,
        "uuid",
        SimpleNamespace(uuid4=forbid_new_uuid4),
    )
    monkeypatch.setattr(RootLock, "__aexit__", capture_unlock)
    monkeypatch.setattr(PinnedResponseCleanup, "delete", capture_cleanup)

    async def run_second_session() -> None:
        nonlocal terminal_request, terminal_deliveries
        async with transport.session():
            assert reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.SETTLED,
                    request_id=REQUEST_ID,
                    request_state=HostRequestState.CANCELLED,
                    response_cleanup_required=True,
                )
            ]
            assert forbidden_uuid_calls == 0
            assert unexpected_publications == []
            assert len(record_calls) == expected_record_calls
            assert len(response_transition_calls) == expected_response_transition_calls
            assert second_session_idle_publications == 1
            request = transport.ledger.get_request(REQUEST_ID)
            deliveries = transport.ledger.list_deliveries(REQUEST_ID)
            assert request is not None and request.state is HostRequestState.CANCELLED
            assert request.terminal_at_ms is not None
            assert tuple(delivery.delivery_id for delivery in deliveries) == (
                DELIVERY_ID,
                CANCEL_ID,
            )
            request_record, cancel_record = deliveries
            assert request_record.delivery_kind == "request"
            assert request_record.published_at_ms is not None
            assert request_record.response_artifact is None
            assert cancel_record.delivery_kind == "cancel"
            assert cancel_record.original_request_delivery_id == DELIVERY_ID
            assert cancel_record.published_at_ms is not None
            artifact = cancel_record.response_artifact
            assert artifact is not None
            assert artifact.filename == response_path.name
            assert artifact.sha256 == expected_sha256
            assert artifact.size_bytes == expected_size
            accepted = artifact.accepted_response
            assert accepted.delivery_kind == "cancel"
            assert accepted.envelope.delivery_id == CANCEL_ID
            assert accepted.cancel_ack is not None
            assert accepted.cancel_ack.status == "cancelled"
            assert accepted.cancel_ack.original_delivery_id == DELIVERY_ID
            assert accepted.settlement == CancelledSettlement(state="cancelled")
            assert transport.journals.load() is None
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            assert response_path.is_file()
            terminal_request = request
            terminal_deliveries = deliveries

    second_session_task = asyncio.create_task(run_second_session())
    await _hard_task_result(
        second_session_task,
        timeout_seconds=5,
        description=f"{crash_point} response-acceptance recovery session",
    )

    assert forbidden_uuid_calls == 0
    assert unexpected_publications == []
    assert int(checkpoint_payload["request_publications"]) == 1
    assert int(checkpoint_payload["cancel_publications"]) == 1
    assert int(checkpoint_payload["idle_publications"]) + second_session_idle_publications == 1
    assert cleanup_trace == [
        "unlock.returned",
        "cleanup.delete.entered",
        "cleanup.delete.returned",
    ]
    assert terminal_request is not None
    assert len(terminal_deliveries) == 2
    assert not response_path.exists()
    assert not harness.paths.pending_file.exists()

    audit = _make_transport(harness)
    assert audit is not diagnostic and audit is not transport
    assert audit.root_lock is not diagnostic.root_lock
    assert audit.root_lock is not transport.root_lock
    assert audit.database is not diagnostic.database
    assert audit.database is not transport.database
    async with audit.root_lock:
        assert audit.journals.load() is None
        assert audit.ledger.get_request(REQUEST_ID) == terminal_request
        assert audit.ledger.list_deliveries(REQUEST_ID) == terminal_deliveries
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        assert not response_path.exists()


async def _assert_late_pending_recovery_finishes_without_publication(
    harness: MatrixHarness,
    diagnostic: FileBridgeTransport,
    checkpoint_payload: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    *,
    crash_point: str,
) -> None:
    expected_transitions = {
        "after_idle_replace": [HostRequestState.IDLE_PUBLISHED, HostRequestState.CANCELLED],
        "after_idle_r5": [HostRequestState.IDLE_PUBLISHED, HostRequestState.CANCELLED],
        "after_request_idle_published": [HostRequestState.CANCELLED],
        "after_terminal_cancelled": [],
    }[crash_point]
    response_path = harness.paths.response_path(REQUEST_ID)
    expected_sha256 = checkpoint_payload["response_sha256"]
    expected_size = int(checkpoint_payload["response_size_bytes"])
    assert hashlib.sha256(response_path.read_bytes()).hexdigest() == expected_sha256

    transport = _make_transport(harness)
    assert transport is not diagnostic
    assert transport.root_lock is not diagnostic.root_lock
    assert transport.database is not diagnostic.database
    reports: list[RecoveryReport] = []
    unexpected_delivery_publications: list[PreparedDelivery] = []
    unexpected_idle_publications = 0
    forbidden_uuid_calls = 0
    unexpected_record_calls: list[ResponseArtifact] = []
    request_transitions: list[HostRequestState] = []
    cleanup_trace: list[str] = []
    terminal_request: RequestRecord | None = None
    terminal_deliveries: tuple[DeliveryRecord, ...] = ()
    installed_recover = RecoveryManager.recover_pending
    installed_publish = InboxPublisher.publish_delivery
    installed_idle = InboxPublisher.publish_idle
    installed_record = RequestLedger.record_response
    installed_transition = RequestLedger.transition
    installed_exit = RootLock.__aexit__
    installed_delete = PinnedResponseCleanup.delete

    async def capture_recovery(manager: RecoveryManager) -> RecoveryReport:
        report = await installed_recover(manager)
        reports.append(report)
        return report

    def forbid_delivery_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        if publisher is transport.inbox:
            unexpected_delivery_publications.append(delivery)
            raise AssertionError("late recovery republished a request or cancel")
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)

    def forbid_idle_publication(publisher: InboxPublisher) -> None:
        nonlocal unexpected_idle_publications
        if publisher is transport.inbox:
            unexpected_idle_publications += 1
            raise AssertionError("late recovery rewrote an already-idle inbox")
        installed_idle(publisher)

    def forbid_new_uuid4() -> UUID:
        nonlocal forbidden_uuid_calls
        forbidden_uuid_calls += 1
        raise AssertionError("late recovery requested a replacement UUID")

    def forbid_record(
        ledger: RequestLedger,
        artifact: ResponseArtifact,
    ) -> DeliveryRecord:
        if ledger is transport.ledger:
            unexpected_record_calls.append(artifact)
            raise AssertionError("late recovery re-recorded the accepted response")
        return installed_record(ledger, artifact)

    def capture_transition(
        ledger: RequestLedger,
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
            ledger,
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        if ledger is transport.ledger:
            request_transitions.append(new_state)
        return result

    async def capture_unlock(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is transport.root_lock:
            assert response_path.is_file()
        result = await installed_exit(lock, exc_type, exc, traceback)
        if lock is transport.root_lock:
            assert response_path.is_file()
            cleanup_trace.append("unlock.returned")
        return result

    def capture_cleanup(cleanup: PinnedResponseCleanup) -> None:
        cleanup_trace.append("cleanup.delete.entered")
        assert cleanup_trace == ["unlock.returned", "cleanup.delete.entered"]
        assert response_path.is_file()
        installed_delete(cleanup)
        assert not response_path.exists()
        cleanup_trace.append("cleanup.delete.returned")

    monkeypatch.setattr(RecoveryManager, "recover_pending", capture_recovery)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", forbid_delivery_publication)
    monkeypatch.setattr(InboxPublisher, "publish_idle", forbid_idle_publication)
    monkeypatch.setattr(RequestLedger, "record_response", forbid_record)
    monkeypatch.setattr(RequestLedger, "transition", capture_transition)
    monkeypatch.setattr(
        recovery_module,
        "uuid",
        SimpleNamespace(uuid4=forbid_new_uuid4),
    )
    monkeypatch.setattr(RootLock, "__aexit__", capture_unlock)
    monkeypatch.setattr(PinnedResponseCleanup, "delete", capture_cleanup)

    async def run_second_session() -> None:
        nonlocal terminal_request, terminal_deliveries
        async with transport.session():
            assert reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.SETTLED,
                    request_id=REQUEST_ID,
                    request_state=HostRequestState.CANCELLED,
                    response_cleanup_required=True,
                )
            ]
            assert forbidden_uuid_calls == 0
            assert unexpected_delivery_publications == []
            assert unexpected_idle_publications == 0
            assert unexpected_record_calls == []
            assert request_transitions == expected_transitions
            request = transport.ledger.get_request(REQUEST_ID)
            deliveries = transport.ledger.list_deliveries(REQUEST_ID)
            assert request is not None and request.state is HostRequestState.CANCELLED
            assert request.terminal_at_ms is not None
            assert tuple(delivery.delivery_id for delivery in deliveries) == (
                DELIVERY_ID,
                CANCEL_ID,
            )
            request_record, cancel_record = deliveries
            assert request_record.response_artifact is None
            artifact = cancel_record.response_artifact
            assert artifact is not None
            assert artifact.filename == response_path.name
            assert artifact.sha256 == expected_sha256
            assert artifact.size_bytes == expected_size
            assert artifact.accepted_response.envelope.delivery_id == CANCEL_ID
            assert artifact.accepted_response.settlement == CancelledSettlement(state="cancelled")
            assert transport.journals.load() is None
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            assert response_path.is_file()
            terminal_request = request
            terminal_deliveries = deliveries

    second_session_task = asyncio.create_task(run_second_session())
    await _hard_task_result(
        second_session_task,
        timeout_seconds=5,
        description=f"{crash_point} late-handoff recovery session",
    )

    assert int(checkpoint_payload["request_publications"]) == 1
    assert int(checkpoint_payload["cancel_publications"]) == 1
    assert int(checkpoint_payload["idle_publications"]) == 1
    assert cleanup_trace == [
        "unlock.returned",
        "cleanup.delete.entered",
        "cleanup.delete.returned",
    ]
    assert terminal_request is not None
    assert len(terminal_deliveries) == 2
    assert not response_path.exists()
    assert not harness.paths.pending_file.exists()

    audit = _make_transport(harness)
    assert audit is not diagnostic and audit is not transport
    assert audit.root_lock is not diagnostic.root_lock
    assert audit.root_lock is not transport.root_lock
    assert audit.database is not diagnostic.database
    assert audit.database is not transport.database
    async with audit.root_lock:
        assert audit.journals.load() is None
        assert audit.ledger.get_request(REQUEST_ID) == terminal_request
        assert audit.ledger.list_deliveries(REQUEST_ID) == terminal_deliveries
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        assert not response_path.exists()


def _start_crash_helper(
    harness: MatrixHarness,
    boundary: str,
    checkpoint: Path,
) -> subprocess.Popen[bytes]:
    helper = Path(__file__).parents[1] / "helpers" / "mutation_failure_process.py"
    return subprocess.Popen(
        [
            sys.executable,
            str(helper),
            str(harness.game_root),
            str(harness.local_app_data),
            boundary,
            str(checkpoint),
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


async def _wait_for_crash(
    process: subprocess.Popen[bytes],
    checkpoint: Path,
    boundary: str,
) -> dict[str, str]:
    try:
        stdout, stderr = await asyncio.to_thread(
            process.communicate,
            timeout=_CRASH_HELPER_EXIT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        if process.poll() is None:
            process.kill()
        stdout, stderr = await asyncio.to_thread(process.communicate, timeout=5)
        raise AssertionError(
            f"crash helper did not exit after {boundary} within "
            f"{_CRASH_HELPER_EXIT_TIMEOUT_SECONDS:g} seconds; "
            f"pid={process.pid}; returncode={process.returncode}; "
            f"checkpoint_exists={checkpoint.is_file()}; "
            f"stdout={stdout!r}; stderr={stderr!r}"
        ) from None
    assert process.returncode == CRASH_CODE, (
        f"crash helper exited at {boundary} with {process.returncode}; "
        f"stdout={stdout!r}; stderr={stderr!r}"
    )
    assert checkpoint.is_file(), f"crash helper exited without checkpointing {boundary}"
    payload = cast(object, json.loads(checkpoint.read_text(encoding="utf-8")))
    assert isinstance(payload, dict)
    result: dict[str, str] = {}
    for key, value in cast(dict[object, object], payload).items():
        assert isinstance(key, str) and isinstance(value, str)
        result[key] = value
    return result


def _kill_helper(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.kill()
    process.communicate(timeout=5)


async def _wait_for_peer_response(
    peer: FakeFileBridgePeer,
    response_path: Path,
) -> None:
    deadline = time.monotonic() + 5
    while True:
        observations = peer.observed_deliveries
        if len(observations) > 1:
            raise AssertionError("peer observed the crashed request more than once")
        if response_path.is_file() and len(observations) == 1:
            return
        if time.monotonic() >= deadline:
            raise AssertionError(
                "peer did not observe and answer the crashed request within five seconds"
            )
        await asyncio.sleep(0.01)


@pytest.mark.parametrize(
    "crash_point",
    ["after_r0", "after_request_insert", "after_request_delivery_insert"],
)
@pytest.mark.asyncio
async def test_process_crash_before_request_publication_recovers_failed_before_publish_in_fresh_session(
    matrix_harness: MatrixHarness,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    harness = matrix_harness
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(render_idle_lua())
    checkpoint = harness.local_app_data / f"{crash_point}.checkpoint"
    helper = _start_crash_helper(harness, crash_point, checkpoint)
    try:
        checkpoint_payload = await _wait_for_crash(helper, checkpoint, crash_point)
    finally:
        _kill_helper(helper)

    assert checkpoint_payload == {
        "boundary": crash_point,
        "request_id": str(REQUEST_ID),
        "delivery_id": str(DELIVERY_ID),
        "request_publications": "0",
        "cancel_publications": "0",
        "idle_publications": "0",
        "request_uuid_calls": "1",
    }
    assert harness.paths.inbox.read_bytes() == render_idle_lua()

    diagnostic = _make_transport(harness)
    await _assert_r0_crash_snapshot(
        diagnostic,
        harness,
        _command(harness),
        request_present=crash_point != "after_r0",
        delivery_present=crash_point == "after_request_delivery_insert",
    )
    transport = _make_transport(harness)
    assert transport is not diagnostic
    assert transport.root_lock is not diagnostic.root_lock
    assert transport.database is not diagnostic.database
    reports: list[RecoveryReport] = []
    second_session_publications: list[str] = []
    installed_recover = RecoveryManager.recover_pending
    installed_publish = InboxPublisher.publish_delivery

    async def capture_recovery(manager: RecoveryManager) -> RecoveryReport:
        report = await installed_recover(manager)
        reports.append(report)
        return report

    def capture_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        if publisher is transport.inbox:
            second_session_publications.append(delivery.delivery_kind)
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)

    monkeypatch.setattr(RecoveryManager, "recover_pending", capture_recovery)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", capture_publication)

    async def run_recovery_session() -> None:
        async with transport.session():
            assert reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.FAILED_BEFORE_PUBLISH,
                    request_id=REQUEST_ID,
                    request_state=HostRequestState.REJECTED,
                    response_cleanup_required=False,
                )
            ]
            request = transport.ledger.get_request(REQUEST_ID)
            delivery = transport.ledger.get_delivery(DELIVERY_ID)
            assert request is not None and request.state is HostRequestState.REJECTED
            assert request.terminal_at_ms is not None
            assert delivery is not None and delivery.delivery_kind == "request"
            assert delivery.published_at_ms is None
            assert transport.journals.load() is None
            assert not harness.paths.response_path(REQUEST_ID).exists()
            assert second_session_publications == []

    recovery_session_task = asyncio.create_task(run_recovery_session())
    await _hard_task_result(
        recovery_session_task,
        timeout_seconds=5,
        description=f"{crash_point} failed-before-publish recovery session",
    )

    assert not harness.paths.pending_file.exists()
    assert harness.paths.inbox.read_bytes() == render_idle_lua()


@pytest.mark.asyncio
async def test_process_crash_after_request_replace_never_republishes_original_mutation(
    matrix_harness: MatrixHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = matrix_harness
    command = _command(harness)
    prepared = prepare_delivery(
        command.body,
        request_id=REQUEST_ID,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
    )
    rendered_request = render_delivery_lua(prepared, harness.snapshot)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(render_idle_lua())
    checkpoint = harness.local_app_data / "after_request_replace.checkpoint"
    helper = _start_crash_helper(harness, "after_request_replace", checkpoint)
    try:
        checkpoint_payload = await _wait_for_crash(
            helper,
            checkpoint,
            "after_request_replace",
        )
    finally:
        _kill_helper(helper)

    assert checkpoint_payload == {
        "boundary": "after_request_replace",
        "request_id": str(REQUEST_ID),
        "delivery_id": str(DELIVERY_ID),
        "request_publications": "1",
        "cancel_publications": "0",
        "idle_publications": "0",
        "request_uuid_calls": "1",
    }
    assert harness.paths.inbox.read_bytes() == rendered_request
    response_path = harness.paths.response_path(REQUEST_ID)
    assert not response_path.exists()

    trace: list[str] = []
    peer = _peer(harness, trace)
    peer.enqueue(Respond(result=_completed_result(command)))
    await peer.start()
    try:
        await _wait_for_peer_response(peer, response_path)
    finally:
        peer_stop_task = asyncio.create_task(peer.stop())
        await _hard_task_result(
            peer_stop_task,
            timeout_seconds=2,
            description="after-request-replace fake peer stop",
        )

    observed = peer.observed_deliveries
    assert len(observed) == 1
    assert observed[0].request_id == REQUEST_ID
    assert observed[0].delivery_id == DELIVERY_ID
    assert observed[0].delivery_kind == "request"
    assert trace == ["peer.request_observed", "peer.response_written"]

    diagnostic = _make_transport(harness)
    await _assert_r0_crash_snapshot(
        diagnostic,
        harness,
        command,
        request_present=True,
        delivery_present=True,
    )
    assert response_path.is_file()
    transport = _make_transport(harness)
    assert transport is not diagnostic
    assert transport.root_lock is not diagnostic.root_lock
    assert transport.database is not diagnostic.database
    reports: list[RecoveryReport] = []
    second_session_publications: list[PreparedDelivery] = []
    cleanup_trace: list[str] = []
    terminal_request: RequestRecord | None = None
    terminal_delivery: DeliveryRecord | None = None
    installed_recover = RecoveryManager.recover_pending
    installed_publish = InboxPublisher.publish_delivery
    installed_exit = RootLock.__aexit__
    installed_delete = PinnedResponseCleanup.delete

    async def capture_recovery(manager: RecoveryManager) -> RecoveryReport:
        report = await installed_recover(manager)
        reports.append(report)
        return report

    def capture_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        if publisher is transport.inbox:
            second_session_publications.append(delivery)
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)

    async def capture_unlock(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is transport.root_lock:
            assert response_path.is_file()
        result = await installed_exit(lock, exc_type, exc, traceback)
        if lock is transport.root_lock:
            assert response_path.is_file()
            cleanup_trace.append("unlock.returned")
        return result

    def capture_cleanup(cleanup: PinnedResponseCleanup) -> None:
        cleanup_trace.append("cleanup.delete.entered")
        assert cleanup_trace == ["unlock.returned", "cleanup.delete.entered"]
        assert response_path.is_file()
        installed_delete(cleanup)
        assert not response_path.exists()
        cleanup_trace.append("cleanup.delete.returned")

    monkeypatch.setattr(RecoveryManager, "recover_pending", capture_recovery)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", capture_publication)
    monkeypatch.setattr(RootLock, "__aexit__", capture_unlock)
    monkeypatch.setattr(PinnedResponseCleanup, "delete", capture_cleanup)

    async def run_recovery_session() -> None:
        nonlocal terminal_request, terminal_delivery
        async with transport.session():
            assert reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.SETTLED,
                    request_id=REQUEST_ID,
                    request_state=HostRequestState.COMPLETED,
                    response_cleanup_required=True,
                )
            ]
            request = transport.ledger.get_request(REQUEST_ID)
            deliveries = transport.ledger.list_deliveries(REQUEST_ID)
            assert request is not None and request.state is HostRequestState.COMPLETED
            assert request.terminal_at_ms is not None
            assert len(deliveries) == 1
            assert deliveries[0].delivery_id == DELIVERY_ID
            assert deliveries[0].delivery_kind == "request"
            assert deliveries[0].published_at_ms is not None
            assert deliveries[0].response_artifact is not None
            terminal_request = request
            terminal_delivery = deliveries[0]
            assert transport.journals.load() is None
            assert response_path.is_file()
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            assert second_session_publications == []

    recovery_session_task = asyncio.create_task(run_recovery_session())
    await _hard_task_result(
        recovery_session_task,
        timeout_seconds=5,
        description="after-request-replace recovery session",
    )

    assert len(peer.observed_deliveries) == 1
    assert cleanup_trace == [
        "unlock.returned",
        "cleanup.delete.entered",
        "cleanup.delete.returned",
    ]
    assert not harness.paths.pending_file.exists()
    assert not response_path.exists()
    assert terminal_request is not None and terminal_delivery is not None

    audit = _make_transport(harness)
    assert audit is not diagnostic and audit is not transport
    assert audit.root_lock is not diagnostic.root_lock
    assert audit.root_lock is not transport.root_lock
    assert audit.database is not diagnostic.database
    assert audit.database is not transport.database
    async with audit.root_lock:
        assert audit.journals.load() is None
        assert audit.ledger.get_request(REQUEST_ID) == terminal_request
        assert audit.ledger.get_delivery(DELIVERY_ID) == terminal_delivery
        assert audit.ledger.list_deliveries(REQUEST_ID) == (terminal_delivery,)
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        assert not response_path.exists()


@pytest.mark.parametrize(
    "crash_point",
    ["after_r1", "after_request_delivery_marker", "after_request_published"],
)
@pytest.mark.asyncio
async def test_process_crash_at_published_sqlite_boundaries_converges_before_one_cancel(
    matrix_harness: MatrixHarness,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    harness = matrix_harness
    command = _command(harness)
    request_delivery = prepare_delivery(
        command.body,
        request_id=REQUEST_ID,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
    )
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(render_idle_lua())
    checkpoint = harness.local_app_data / f"{crash_point}.checkpoint"
    helper = _start_crash_helper(harness, crash_point, checkpoint)
    try:
        checkpoint_payload = await _wait_for_crash(helper, checkpoint, crash_point)
    finally:
        _kill_helper(helper)

    assert checkpoint_payload == {
        "boundary": crash_point,
        "request_id": str(REQUEST_ID),
        "delivery_id": str(DELIVERY_ID),
        "request_publications": "1",
        "cancel_publications": "0",
        "idle_publications": "0",
        "request_uuid_calls": "1",
    }
    rendered_request = render_delivery_lua(
        request_delivery,
        harness.snapshot,
    )
    assert harness.paths.inbox.read_bytes() == rendered_request
    assert not harness.paths.response_path(REQUEST_ID).exists()

    diagnostic = _make_transport(harness)
    await _assert_r1_crash_snapshot(
        diagnostic,
        harness,
        command,
        crash_point=crash_point,
        rendered_request=rendered_request,
    )

    transport = _make_transport(harness)
    assert transport is not diagnostic
    assert transport.root_lock is not diagnostic.root_lock
    assert transport.database is not diagnostic.database
    reports: list[RecoveryReport] = []
    second_session_publications: list[PreparedDelivery] = []
    cleanup_trace: list[str] = []
    cancel_uuid_calls = 0
    terminal_request: RequestRecord | None = None
    terminal_deliveries: tuple[DeliveryRecord, ...] = ()
    response_path = harness.paths.response_path(REQUEST_ID)
    installed_recover = RecoveryManager.recover_pending
    installed_publish = InboxPublisher.publish_delivery
    installed_exit = RootLock.__aexit__
    installed_delete = PinnedResponseCleanup.delete

    async def capture_recovery(manager: RecoveryManager) -> RecoveryReport:
        report = await installed_recover(manager)
        reports.append(report)
        return report

    def capture_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)
        if publisher is transport.inbox:
            second_session_publications.append(delivery)

    def one_shot_cancel_uuid4() -> UUID:
        nonlocal cancel_uuid_calls
        cancel_uuid_calls += 1
        if cancel_uuid_calls != 1:
            raise AssertionError("startup recovery requested more than one cancel UUID")
        loaded = transport.journals.load()
        assert loaded is not None
        original = loaded.journal.original
        assert original.state is PendingPhase.PUBLISHED
        assert original.revision == 1
        assert len(original.delivery_intents) == 1
        intent = original.delivery_intents[0]
        assert intent.delivery_id == DELIVERY_ID
        assert intent.delivery_kind == "request"
        assert intent.published_at_ms is not None
        request = transport.ledger.get_request(REQUEST_ID)
        delivery = transport.ledger.get_delivery(DELIVERY_ID)
        assert request is not None
        assert request.state is HostRequestState.PUBLISHED
        assert request.updated_at_ms == intent.published_at_ms
        assert delivery is not None
        assert delivery.published_at_ms == intent.published_at_ms
        assert second_session_publications == []
        assert harness.paths.inbox.read_bytes() == rendered_request
        return CANCEL_ID

    async def capture_unlock(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is transport.root_lock:
            assert response_path.is_file()
        result = await installed_exit(lock, exc_type, exc, traceback)
        if lock is transport.root_lock:
            assert response_path.is_file()
            cleanup_trace.append("unlock.returned")
        return result

    def capture_cleanup(cleanup: PinnedResponseCleanup) -> None:
        cleanup_trace.append("cleanup.delete.entered")
        assert cleanup_trace == ["unlock.returned", "cleanup.delete.entered"]
        assert response_path.is_file()
        installed_delete(cleanup)
        assert not response_path.exists()
        cleanup_trace.append("cleanup.delete.returned")

    monkeypatch.setattr(RecoveryManager, "recover_pending", capture_recovery)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", capture_publication)
    monkeypatch.setattr(
        recovery_module,
        "uuid",
        SimpleNamespace(uuid4=one_shot_cancel_uuid4),
    )
    monkeypatch.setattr(RootLock, "__aexit__", capture_unlock)
    monkeypatch.setattr(PinnedResponseCleanup, "delete", capture_cleanup)

    original_seen = asyncio.Event()
    observed_cancels: list[PreparedDelivery] = []
    responder_task = asyncio.create_task(
        _respond_to_exact_cancel(
            harness,
            command,
            rendered_request=rendered_request,
            original_seen=original_seen,
            observed_cancels=observed_cancels,
        )
    )

    async def run_second_session() -> None:
        nonlocal terminal_request, terminal_deliveries
        async with transport.session():
            assert reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.SETTLED,
                    request_id=REQUEST_ID,
                    request_state=HostRequestState.CANCELLED,
                    response_cleanup_required=True,
                )
            ]
            assert cancel_uuid_calls == 1
            assert len(second_session_publications) == 1
            cancel_publication = second_session_publications[0]
            assert cancel_publication.request_id == REQUEST_ID
            assert cancel_publication.delivery_id == CANCEL_ID
            assert cancel_publication.delivery_kind == "cancel"
            assert cancel_publication.request_hash == request_delivery.request_hash
            request = transport.ledger.get_request(REQUEST_ID)
            deliveries = transport.ledger.list_deliveries(REQUEST_ID)
            assert request is not None and request.state is HostRequestState.CANCELLED
            assert request.terminal_at_ms is not None
            assert tuple(delivery.delivery_id for delivery in deliveries) == (
                DELIVERY_ID,
                CANCEL_ID,
            )
            request_record, cancel_record = deliveries
            assert request_record.delivery_kind == "request"
            assert request_record.published_at_ms is not None
            assert request_record.response_artifact is None
            assert cancel_record.delivery_kind == "cancel"
            assert cancel_record.original_request_delivery_id == DELIVERY_ID
            assert cancel_record.published_at_ms is not None
            artifact = cancel_record.response_artifact
            assert artifact is not None
            accepted = artifact.accepted_response
            assert accepted.delivery_kind == "cancel"
            assert accepted.envelope.delivery_id == CANCEL_ID
            assert accepted.cancel_ack is not None
            assert accepted.cancel_ack.status == "cancelled"
            assert accepted.cancel_ack.original_delivery_id == DELIVERY_ID
            assert accepted.settlement == CancelledSettlement(state="cancelled")
            assert transport.journals.load() is None
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            assert response_path.is_file()
            terminal_request = request
            terminal_deliveries = deliveries

    try:
        await asyncio.wait_for(original_seen.wait(), timeout=1)
        assert observed_cancels == []
        assert not response_path.exists()
        second_session_task = asyncio.create_task(run_second_session())
        await _hard_task_result(
            second_session_task,
            timeout_seconds=5,
            description=f"{crash_point} published recovery session",
        )
        acknowledgement = await _hard_task_result(
            responder_task,
            timeout_seconds=1,
            description=f"{crash_point} cancel responder",
        )
    finally:
        await _cancel_and_drain_task(
            responder_task,
            timeout_seconds=1,
            description=f"{crash_point} cancel responder",
        )

    assert acknowledgement.request_id == REQUEST_ID
    assert acknowledgement.request_hash == request_delivery.request_hash
    assert acknowledgement.original_delivery_id == DELIVERY_ID
    assert acknowledgement.status == "cancelled"
    assert observed_cancels == [
        prepare_delivery(
            command.body,
            request_id=REQUEST_ID,
            delivery_id=CANCEL_ID,
            delivery_kind="cancel",
        )
    ]
    assert (
        int(checkpoint_payload["request_publications"])
        + sum(delivery.delivery_kind == "request" for delivery in second_session_publications)
        == 1
    )
    assert (
        int(checkpoint_payload["cancel_publications"])
        + sum(delivery.delivery_kind == "cancel" for delivery in second_session_publications)
        == 1
    )
    assert cleanup_trace == [
        "unlock.returned",
        "cleanup.delete.entered",
        "cleanup.delete.returned",
    ]
    assert terminal_request is not None
    assert len(terminal_deliveries) == 2
    assert not response_path.exists()
    assert not harness.paths.pending_file.exists()

    audit = _make_transport(harness)
    assert audit is not diagnostic and audit is not transport
    assert audit.root_lock is not diagnostic.root_lock
    assert audit.root_lock is not transport.root_lock
    assert audit.database is not diagnostic.database
    assert audit.database is not transport.database
    async with audit.root_lock:
        assert audit.journals.load() is None
        assert audit.ledger.get_request(REQUEST_ID) == terminal_request
        assert audit.ledger.list_deliveries(REQUEST_ID) == terminal_deliveries
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        assert not response_path.exists()


@pytest.mark.parametrize(
    "crash_point",
    ["after_cancel_intent_r2", "after_cancel_delivery_insert"],
)
@pytest.mark.asyncio
async def test_process_crash_before_cancel_replace_reuses_one_durable_cancel_id(
    matrix_harness: MatrixHarness,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    harness = matrix_harness
    command = _command(harness)
    request_delivery = prepare_delivery(
        command.body,
        request_id=REQUEST_ID,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
    )
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(render_idle_lua())
    checkpoint = harness.local_app_data / f"{crash_point}.checkpoint"
    helper = _start_crash_helper(harness, crash_point, checkpoint)
    try:
        checkpoint_payload = await _wait_for_crash(helper, checkpoint, crash_point)
    finally:
        _kill_helper(helper)

    assert checkpoint_payload == {
        "boundary": crash_point,
        "request_id": str(REQUEST_ID),
        "delivery_id": str(DELIVERY_ID),
        "cancel_delivery_id": str(CANCEL_ID),
        "request_publications": "1",
        "cancel_publications": "0",
        "idle_publications": "0",
        "request_uuid_calls": "1",
        "cancel_uuid_calls": "1",
        "original_wait_calls": "1",
        "task_cancel_requests": "1",
    }
    rendered_request = render_delivery_lua(
        request_delivery,
        harness.snapshot,
    )
    assert harness.paths.inbox.read_bytes() == rendered_request
    assert not harness.paths.response_path(REQUEST_ID).exists()

    diagnostic = _make_transport(harness)
    await _assert_r2_crash_snapshot(
        diagnostic,
        harness,
        command,
        crash_point=crash_point,
        rendered_request=rendered_request,
    )

    transport = _make_transport(harness)
    assert transport is not diagnostic
    assert transport.root_lock is not diagnostic.root_lock
    assert transport.database is not diagnostic.database
    reports: list[RecoveryReport] = []
    second_session_publications: list[PreparedDelivery] = []
    second_session_idle_publications = 0
    forbidden_uuid_calls = 0
    cleanup_trace: list[str] = []
    terminal_request: RequestRecord | None = None
    terminal_deliveries: tuple[DeliveryRecord, ...] = ()
    response_path = harness.paths.response_path(REQUEST_ID)
    installed_recover = RecoveryManager.recover_pending
    installed_publish = InboxPublisher.publish_delivery
    installed_idle = InboxPublisher.publish_idle
    installed_exit = RootLock.__aexit__
    installed_delete = PinnedResponseCleanup.delete

    async def capture_recovery(manager: RecoveryManager) -> RecoveryReport:
        report = await installed_recover(manager)
        reports.append(report)
        return report

    def capture_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        if publisher is transport.inbox:
            assert delivery.request_id == REQUEST_ID
            assert delivery.delivery_id == CANCEL_ID
            assert delivery.delivery_kind == "cancel"
            assert delivery.request_hash == request_delivery.request_hash
            assert forbidden_uuid_calls == 0
            loaded = transport.journals.load()
            assert loaded is not None
            original = loaded.journal.original
            assert original.state is PendingPhase.CANCEL_PUBLISHED
            assert original.revision == 2
            assert len(original.delivery_intents) == 2
            request_intent, cancel_intent = original.delivery_intents
            assert request_intent.delivery_id == DELIVERY_ID
            assert request_intent.published_at_ms is not None
            assert cancel_intent.delivery_id == CANCEL_ID
            assert cancel_intent.published_at_ms is None
            request = transport.ledger.get_request(REQUEST_ID)
            cancel_record = transport.ledger.get_delivery(CANCEL_ID)
            assert request is not None and request.state is HostRequestState.PUBLISHED
            assert cancel_record is not None and cancel_record.published_at_ms is None
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)
        if publisher is transport.inbox:
            second_session_publications.append(delivery)

    def capture_idle(publisher: InboxPublisher) -> None:
        nonlocal second_session_idle_publications
        installed_idle(publisher)
        if publisher is transport.inbox:
            second_session_idle_publications += 1

    def forbid_new_uuid4() -> UUID:
        nonlocal forbidden_uuid_calls
        forbidden_uuid_calls += 1
        raise AssertionError("R2 startup recovery attempted to replace the durable cancel UUID")

    async def capture_unlock(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is transport.root_lock:
            assert response_path.is_file()
        result = await installed_exit(lock, exc_type, exc, traceback)
        if lock is transport.root_lock:
            assert response_path.is_file()
            cleanup_trace.append("unlock.returned")
        return result

    def capture_cleanup(cleanup: PinnedResponseCleanup) -> None:
        cleanup_trace.append("cleanup.delete.entered")
        assert cleanup_trace == ["unlock.returned", "cleanup.delete.entered"]
        assert response_path.is_file()
        installed_delete(cleanup)
        assert not response_path.exists()
        cleanup_trace.append("cleanup.delete.returned")

    monkeypatch.setattr(RecoveryManager, "recover_pending", capture_recovery)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", capture_publication)
    monkeypatch.setattr(InboxPublisher, "publish_idle", capture_idle)
    monkeypatch.setattr(
        recovery_module,
        "uuid",
        SimpleNamespace(uuid4=forbid_new_uuid4),
    )
    monkeypatch.setattr(RootLock, "__aexit__", capture_unlock)
    monkeypatch.setattr(PinnedResponseCleanup, "delete", capture_cleanup)

    original_seen = asyncio.Event()
    observed_cancels: list[PreparedDelivery] = []
    responder_task = asyncio.create_task(
        _respond_to_exact_cancel(
            harness,
            command,
            rendered_request=rendered_request,
            original_seen=original_seen,
            observed_cancels=observed_cancels,
        )
    )

    async def run_second_session() -> None:
        nonlocal terminal_request, terminal_deliveries
        async with transport.session():
            assert reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.SETTLED,
                    request_id=REQUEST_ID,
                    request_state=HostRequestState.CANCELLED,
                    response_cleanup_required=True,
                )
            ]
            assert forbidden_uuid_calls == 0
            assert second_session_publications == [
                prepare_delivery(
                    command.body,
                    request_id=REQUEST_ID,
                    delivery_id=CANCEL_ID,
                    delivery_kind="cancel",
                )
            ]
            assert second_session_idle_publications == 1
            request = transport.ledger.get_request(REQUEST_ID)
            deliveries = transport.ledger.list_deliveries(REQUEST_ID)
            assert request is not None and request.state is HostRequestState.CANCELLED
            assert request.terminal_at_ms is not None
            assert tuple(delivery.delivery_id for delivery in deliveries) == (
                DELIVERY_ID,
                CANCEL_ID,
            )
            request_record, cancel_record = deliveries
            assert request_record.delivery_kind == "request"
            assert request_record.published_at_ms is not None
            assert request_record.response_artifact is None
            assert cancel_record.delivery_kind == "cancel"
            assert cancel_record.original_request_delivery_id == DELIVERY_ID
            assert cancel_record.published_at_ms is not None
            artifact = cancel_record.response_artifact
            assert artifact is not None
            accepted = artifact.accepted_response
            assert accepted.delivery_kind == "cancel"
            assert accepted.envelope.delivery_id == CANCEL_ID
            assert accepted.cancel_ack is not None
            assert accepted.cancel_ack.status == "cancelled"
            assert accepted.cancel_ack.original_delivery_id == DELIVERY_ID
            assert accepted.settlement == CancelledSettlement(state="cancelled")
            assert transport.journals.load() is None
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            assert response_path.is_file()
            terminal_request = request
            terminal_deliveries = deliveries

    try:
        await asyncio.wait_for(original_seen.wait(), timeout=1)
        assert observed_cancels == []
        assert not response_path.exists()
        second_session_task = asyncio.create_task(run_second_session())
        await _hard_task_result(
            second_session_task,
            timeout_seconds=5,
            description=f"{crash_point} durable-cancel recovery session",
        )
        acknowledgement = await _hard_task_result(
            responder_task,
            timeout_seconds=1,
            description=f"{crash_point} durable-cancel responder",
        )
    finally:
        await _cancel_and_drain_task(
            responder_task,
            timeout_seconds=1,
            description=f"{crash_point} durable-cancel responder",
        )

    assert acknowledgement.request_id == REQUEST_ID
    assert acknowledgement.request_hash == request_delivery.request_hash
    assert acknowledgement.original_delivery_id == DELIVERY_ID
    assert acknowledgement.status == "cancelled"
    assert observed_cancels == second_session_publications
    assert (
        int(checkpoint_payload["request_publications"])
        + sum(delivery.delivery_kind == "request" for delivery in second_session_publications)
        == 1
    )
    assert (
        int(checkpoint_payload["cancel_publications"])
        + sum(delivery.delivery_kind == "cancel" for delivery in second_session_publications)
        == 1
    )
    assert int(checkpoint_payload["idle_publications"]) + second_session_idle_publications == 1
    assert cleanup_trace == [
        "unlock.returned",
        "cleanup.delete.entered",
        "cleanup.delete.returned",
    ]
    assert terminal_request is not None
    assert len(terminal_deliveries) == 2
    assert not response_path.exists()
    assert not harness.paths.pending_file.exists()

    audit = _make_transport(harness)
    assert audit is not diagnostic and audit is not transport
    assert audit.root_lock is not diagnostic.root_lock
    assert audit.root_lock is not transport.root_lock
    assert audit.database is not diagnostic.database
    assert audit.database is not transport.database
    async with audit.root_lock:
        assert audit.journals.load() is None
        assert audit.ledger.get_request(REQUEST_ID) == terminal_request
        assert audit.ledger.list_deliveries(REQUEST_ID) == terminal_deliveries
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        assert not response_path.exists()


@pytest.mark.asyncio
async def test_process_crash_after_cancel_replace_does_not_publish_cancel_again(
    matrix_harness: MatrixHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = matrix_harness
    command = _command(harness)
    cancel_delivery = prepare_delivery(
        command.body,
        request_id=REQUEST_ID,
        delivery_id=CANCEL_ID,
        delivery_kind="cancel",
    )
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(render_idle_lua())
    checkpoint = harness.local_app_data / "after_cancel_replace.checkpoint"
    helper = _start_crash_helper(harness, "after_cancel_replace", checkpoint)
    try:
        checkpoint_payload = await _wait_for_crash(
            helper,
            checkpoint,
            "after_cancel_replace",
        )
    finally:
        _kill_helper(helper)

    assert checkpoint_payload == {
        "boundary": "after_cancel_replace",
        "request_id": str(REQUEST_ID),
        "delivery_id": str(DELIVERY_ID),
        "cancel_delivery_id": str(CANCEL_ID),
        "request_publications": "1",
        "cancel_publications": "1",
        "idle_publications": "0",
        "request_uuid_calls": "1",
        "cancel_uuid_calls": "1",
        "original_wait_calls": "1",
        "task_cancel_requests": "1",
    }
    rendered_cancel = render_delivery_lua(
        cancel_delivery,
        harness.snapshot,
    )
    assert harness.paths.inbox.read_bytes() == rendered_cancel
    assert not harness.paths.response_path(REQUEST_ID).exists()

    diagnostic = _make_transport(harness)
    await _assert_cancel_replace_crash_snapshot(
        diagnostic,
        harness,
        command,
        rendered_cancel=rendered_cancel,
    )
    await _assert_preexisting_cancel_recovers_without_delivery_publication(
        harness,
        command,
        diagnostic,
        checkpoint_payload,
        monkeypatch,
    )


@pytest.mark.parametrize(
    "crash_point",
    ["after_cancel_r3", "after_cancel_delivery_marker", "after_request_cancel_published"],
)
@pytest.mark.asyncio
async def test_process_crash_at_cancel_published_sqlite_boundaries_only_converges_markers(
    matrix_harness: MatrixHarness,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    harness = matrix_harness
    command = _command(harness)
    cancel_delivery = prepare_delivery(
        command.body,
        request_id=REQUEST_ID,
        delivery_id=CANCEL_ID,
        delivery_kind="cancel",
    )
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(render_idle_lua())
    checkpoint = harness.local_app_data / f"{crash_point}.checkpoint"
    helper = _start_crash_helper(harness, crash_point, checkpoint)
    try:
        checkpoint_payload = await _wait_for_crash(helper, checkpoint, crash_point)
    finally:
        _kill_helper(helper)

    assert checkpoint_payload == {
        "boundary": crash_point,
        "request_id": str(REQUEST_ID),
        "delivery_id": str(DELIVERY_ID),
        "cancel_delivery_id": str(CANCEL_ID),
        "request_publications": "1",
        "cancel_publications": "1",
        "idle_publications": "0",
        "request_uuid_calls": "1",
        "cancel_uuid_calls": "1",
        "original_wait_calls": "1",
        "task_cancel_requests": "1",
    }
    rendered_cancel = render_delivery_lua(
        cancel_delivery,
        harness.snapshot,
    )
    assert harness.paths.inbox.read_bytes() == rendered_cancel
    assert not harness.paths.response_path(REQUEST_ID).exists()

    diagnostic = _make_transport(harness)
    await _assert_r3_crash_snapshot(
        diagnostic,
        harness,
        command,
        crash_point=crash_point,
        rendered_cancel=rendered_cancel,
    )
    await _assert_preexisting_cancel_recovers_without_delivery_publication(
        harness,
        command,
        diagnostic,
        checkpoint_payload,
        monkeypatch,
    )


@pytest.mark.parametrize(
    "crash_point",
    [
        "after_cancel_response_file",
        "after_response_r4",
        "after_response_record",
        "after_request_response_accepted",
    ],
)
@pytest.mark.asyncio
async def test_process_crash_during_cancel_response_acceptance_resumes_from_exact_raw_artifact(
    matrix_harness: MatrixHarness,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    harness = matrix_harness
    command = _command(harness)
    cancel_delivery = prepare_delivery(
        command.body,
        request_id=REQUEST_ID,
        delivery_id=CANCEL_ID,
        delivery_kind="cancel",
    )
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(render_idle_lua())
    checkpoint = harness.local_app_data / f"{crash_point}.checkpoint"
    helper = _start_crash_helper(harness, crash_point, checkpoint)
    try:
        checkpoint_payload = await _wait_for_crash(helper, checkpoint, crash_point)
    finally:
        _kill_helper(helper)

    response_path = harness.paths.response_path(REQUEST_ID)
    assert response_path.is_file()
    raw = response_path.read_bytes()
    assert checkpoint_payload == {
        "boundary": crash_point,
        "request_id": str(REQUEST_ID),
        "delivery_id": str(DELIVERY_ID),
        "cancel_delivery_id": str(CANCEL_ID),
        "request_publications": "1",
        "cancel_publications": "1",
        "idle_publications": "0",
        "request_uuid_calls": "1",
        "cancel_uuid_calls": "1",
        "original_wait_calls": "1",
        "cancel_wait_calls": "1",
        "task_cancel_requests": "1",
        "response_sha256": hashlib.sha256(raw).hexdigest(),
        "response_size_bytes": str(len(raw)),
    }
    rendered_cancel = render_delivery_lua(
        cancel_delivery,
        harness.snapshot,
    )
    assert harness.paths.inbox.read_bytes() == rendered_cancel
    diagnostic = _make_transport(harness)
    await _assert_response_crash_snapshot(
        diagnostic,
        harness,
        command,
        checkpoint_payload,
        crash_point=crash_point,
        rendered_cancel=rendered_cancel,
    )
    await _assert_response_acceptance_recovers_exact_raw_artifact(
        harness,
        diagnostic,
        checkpoint_payload,
        monkeypatch,
        crash_point=crash_point,
    )


@pytest.mark.parametrize(
    "crash_point",
    [
        "after_idle_replace",
        "after_idle_r5",
        "after_request_idle_published",
        "after_terminal_cancelled",
    ],
)
@pytest.mark.asyncio
async def test_process_crash_during_late_terminal_handoff_never_republishes_delivery(
    matrix_harness: MatrixHarness,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    harness = matrix_harness
    command = _command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(render_idle_lua())
    checkpoint = harness.local_app_data / f"{crash_point}.checkpoint"
    helper = _start_crash_helper(harness, crash_point, checkpoint)
    try:
        checkpoint_payload = await _wait_for_crash(helper, checkpoint, crash_point)
    finally:
        _kill_helper(helper)

    response_path = harness.paths.response_path(REQUEST_ID)
    assert response_path.is_file()
    raw = response_path.read_bytes()
    assert checkpoint_payload == {
        "boundary": crash_point,
        "request_id": str(REQUEST_ID),
        "delivery_id": str(DELIVERY_ID),
        "cancel_delivery_id": str(CANCEL_ID),
        "request_publications": "1",
        "cancel_publications": "1",
        "idle_publications": "1",
        "request_uuid_calls": "1",
        "cancel_uuid_calls": "1",
        "original_wait_calls": "1",
        "cancel_wait_calls": "1",
        "task_cancel_requests": "1",
        "response_sha256": hashlib.sha256(raw).hexdigest(),
        "response_size_bytes": str(len(raw)),
    }
    assert harness.paths.inbox.read_bytes() == render_idle_lua()
    diagnostic = _make_transport(harness)
    await _assert_late_handoff_crash_snapshot(
        diagnostic,
        harness,
        command,
        checkpoint_payload,
        crash_point=crash_point,
    )
    await _assert_late_pending_recovery_finishes_without_publication(
        harness,
        diagnostic,
        checkpoint_payload,
        monkeypatch,
        crash_point=crash_point,
    )


@pytest.mark.asyncio
async def test_process_crash_after_terminal_journal_delete_hands_response_to_artifact_janitor(
    matrix_harness: MatrixHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = matrix_harness
    command = _command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(render_idle_lua())
    checkpoint = harness.local_app_data / "after_journal_delete.checkpoint"
    helper = _start_crash_helper(harness, "after_journal_delete", checkpoint)
    try:
        checkpoint_payload = await _wait_for_crash(
            helper,
            checkpoint,
            "after_journal_delete",
        )
    finally:
        _kill_helper(helper)

    response_path = harness.paths.response_path(REQUEST_ID)
    assert response_path.is_file()
    raw = response_path.read_bytes()
    assert checkpoint_payload == {
        "boundary": "after_journal_delete",
        "request_id": str(REQUEST_ID),
        "delivery_id": str(DELIVERY_ID),
        "cancel_delivery_id": str(CANCEL_ID),
        "request_publications": "1",
        "cancel_publications": "1",
        "idle_publications": "1",
        "request_uuid_calls": "1",
        "cancel_uuid_calls": "1",
        "original_wait_calls": "1",
        "cancel_wait_calls": "1",
        "task_cancel_requests": "1",
        "response_sha256": hashlib.sha256(raw).hexdigest(),
        "response_size_bytes": str(len(raw)),
    }
    assert not harness.paths.pending_file.exists()
    assert harness.paths.inbox.read_bytes() == render_idle_lua()
    diagnostic = _make_transport(harness)
    (
        terminal_request,
        terminal_deliveries,
        terminal_artifact,
    ) = await _assert_late_handoff_crash_snapshot(
        diagnostic,
        harness,
        command,
        checkpoint_payload,
        crash_point="after_journal_delete",
    )

    transport = _make_transport(harness)
    assert transport is not diagnostic
    assert transport.root_lock is not diagnostic.root_lock
    assert transport.database is not diagnostic.database
    reports: list[RecoveryReport] = []
    forbidden_uuid_calls = 0
    unexpected_delivery_publications: list[PreparedDelivery] = []
    unexpected_idle_publications = 0
    unexpected_cleanup_calls = 0
    unlock_trace: list[str] = []
    installed_recover = RecoveryManager.recover_pending
    installed_publish = InboxPublisher.publish_delivery
    installed_idle = InboxPublisher.publish_idle
    installed_exit = RootLock.__aexit__

    async def capture_recovery(manager: RecoveryManager) -> RecoveryReport:
        report = await installed_recover(manager)
        reports.append(report)
        return report

    def forbid_new_uuid4() -> UUID:
        nonlocal forbidden_uuid_calls
        forbidden_uuid_calls += 1
        raise AssertionError("NO_PENDING recovery requested a UUID")

    def forbid_delivery_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        if publisher is transport.inbox:
            unexpected_delivery_publications.append(delivery)
            raise AssertionError("NO_PENDING recovery published a delivery")
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)

    def forbid_idle_publication(publisher: InboxPublisher) -> None:
        nonlocal unexpected_idle_publications
        if publisher is transport.inbox:
            unexpected_idle_publications += 1
            raise AssertionError("NO_PENDING recovery published idle")
        installed_idle(publisher)

    def forbid_cleanup(cleanup: PinnedResponseCleanup) -> None:
        nonlocal unexpected_cleanup_calls
        del cleanup
        unexpected_cleanup_calls += 1
        raise AssertionError("NO_PENDING recovery queued immediate response cleanup")

    async def capture_unlock(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is transport.root_lock:
            assert response_path.is_file()
        result = await installed_exit(lock, exc_type, exc, traceback)
        if lock is transport.root_lock:
            assert response_path.is_file()
            unlock_trace.append("unlock.returned")
        return result

    monkeypatch.setattr(RecoveryManager, "recover_pending", capture_recovery)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", forbid_delivery_publication)
    monkeypatch.setattr(InboxPublisher, "publish_idle", forbid_idle_publication)
    monkeypatch.setattr(
        recovery_module,
        "uuid",
        SimpleNamespace(uuid4=forbid_new_uuid4),
    )
    monkeypatch.setattr(PinnedResponseCleanup, "delete", forbid_cleanup)
    monkeypatch.setattr(RootLock, "__aexit__", capture_unlock)

    async def run_no_pending_session() -> None:
        async with transport.session():
            assert reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.NO_PENDING,
                    request_id=None,
                    request_state=None,
                    response_cleanup_required=False,
                )
            ]
            assert forbidden_uuid_calls == 0
            assert unexpected_delivery_publications == []
            assert unexpected_idle_publications == 0
            assert unexpected_cleanup_calls == 0
            assert transport.journals.load() is None
            assert transport.ledger.get_request(REQUEST_ID) == terminal_request
            assert transport.ledger.list_deliveries(REQUEST_ID) == terminal_deliveries
            assert response_path.is_file()

    no_pending_session_task = asyncio.create_task(run_no_pending_session())
    await _hard_task_result(
        no_pending_session_task,
        timeout_seconds=5,
        description="post-delete NO_PENDING recovery session",
    )

    assert unlock_trace == ["unlock.returned"]
    assert unexpected_cleanup_calls == 0
    assert response_path.is_file()
    assert int(checkpoint_payload["request_publications"]) == 1
    assert int(checkpoint_payload["cancel_publications"]) == 1
    assert int(checkpoint_payload["idle_publications"]) == 1

    janitor_transport = _make_transport(harness)
    assert janitor_transport is not diagnostic and janitor_transport is not transport
    assert janitor_transport.root_lock is not diagnostic.root_lock
    assert janitor_transport.root_lock is not transport.root_lock
    assert janitor_transport.database is not diagnostic.database
    assert janitor_transport.database is not transport.database
    janitor = ArtifactJanitor(
        harness.paths,
        janitor_transport.root_lock,
        janitor_transport.ledger,
        janitor_transport.journals,
        retention_ms=1,
    )
    response_mtime_ms = response_path.stat().st_mtime_ns // 1_000_000
    assert terminal_request.terminal_at_ms is not None
    now_ms = (
        max(
            response_mtime_ms,
            terminal_artifact.accepted_at_ms,
            terminal_request.terminal_at_ms,
        )
        + 2
    )
    async with janitor_transport.root_lock:
        report = janitor.sweep(now_ms)
        assert report == CleanupReport(
            scanned=1,
            deleted=1,
            retained=0,
            failed=0,
            failed_paths=(),
        )
        assert janitor_transport.journals.load() is None
        assert janitor_transport.ledger.get_request(REQUEST_ID) == terminal_request
        assert janitor_transport.ledger.list_deliveries(REQUEST_ID) == terminal_deliveries
        assert not response_path.exists()


@pytest.mark.asyncio
async def test_cancelled_exchange_without_ack_is_quarantined_before_fresh_session_and_blocks_mutation(
    matrix_harness: MatrixHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = matrix_harness
    command = _command(harness)
    transport_a = _make_transport(harness, cancel_ack_timeout_seconds=0.05)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(render_idle_lua())
    request_uuid_calls = 0
    cancel_uuid_calls = 0
    publication_kinds: list[str] = []
    idle_publications = 0
    wait_calls = 0
    original_wait_started = asyncio.Event()
    wait_cancellations: list[asyncio.CancelledError] = []
    exchange_cancellations: list[asyncio.CancelledError] = []
    trace: list[str] = []
    unlock_observations: list[tuple[PendingPhase, HostRequestState]] = []
    installed_publish = InboxPublisher.publish_delivery
    installed_idle = InboxPublisher.publish_idle
    installed_wait = ResponseWaiter.wait
    installed_exit = RootLock.__aexit__

    def one_shot_request_uuid4() -> UUID:
        nonlocal request_uuid_calls
        request_uuid_calls += 1
        if request_uuid_calls != 1:
            raise AssertionError("session A requested more than one request UUID")
        return DELIVERY_ID

    def one_shot_cancel_uuid4() -> UUID:
        nonlocal cancel_uuid_calls
        cancel_uuid_calls += 1
        if cancel_uuid_calls != 1:
            raise AssertionError("session A requested more than one cancel UUID")
        return CANCEL_ID

    def capture_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)
        if publisher is transport_a.inbox:
            publication_kinds.append(delivery.delivery_kind)

    def capture_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_publications
        installed_idle(publisher)
        if publisher is transport_a.inbox:
            idle_publications += 1

    async def capture_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        nonlocal wait_calls
        wait_calls += 1
        if wait_calls == 1:
            original_wait_started.set()
            try:
                return await installed_wait(waiter, timeout_seconds)
            except asyncio.CancelledError as error:
                wait_cancellations.append(error)
                raise
        if wait_calls == 2:
            return await installed_wait(waiter, timeout_seconds)
        raise AssertionError("session A reached an unexpected third response wait")

    async def capture_unlock(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is transport_a.root_lock:
            lock.require_acquired()
            loaded = transport_a.journals.load()
            request = transport_a.ledger.get_request(REQUEST_ID)
            assert loaded is not None
            assert request is not None
            assert loaded.journal.original.state is PendingPhase.QUARANTINED
            assert request.state is HostRequestState.QUARANTINED
            assert harness.paths.pending_file.is_file()
            unlock_observations.append((loaded.journal.original.state, request.state))
            trace.append("a.unlock.enter")
        result = await installed_exit(lock, exc_type, exc, traceback)
        if lock is transport_a.root_lock:
            trace.append("a.unlock.returned")
        return result

    monkeypatch.setattr(
        mutation_module,
        "uuid",
        SimpleNamespace(uuid4=one_shot_request_uuid4),
    )
    monkeypatch.setattr(
        recovery_module,
        "uuid",
        SimpleNamespace(uuid4=one_shot_cancel_uuid4),
    )
    monkeypatch.setattr(InboxPublisher, "publish_delivery", capture_publication)
    monkeypatch.setattr(InboxPublisher, "publish_idle", capture_idle)
    monkeypatch.setattr(ResponseWaiter, "wait", capture_wait)
    monkeypatch.setattr(RootLock, "__aexit__", capture_unlock)

    caught_cancellation: asyncio.CancelledError | None = None
    quarantined_journal: PendingJournal | None = None
    quarantined_request: RequestRecord | None = None
    quarantined_deliveries: tuple[DeliveryRecord, ...] = ()
    cancellation_message = "XS09 cancel without acknowledgement"
    exchange_task: asyncio.Task[ResponseArtifact] | None = None

    async def run_session_a() -> None:
        nonlocal caught_cancellation
        nonlocal exchange_task
        nonlocal quarantined_deliveries, quarantined_journal, quarantined_request
        async with transport_a.session() as channel_a:

            async def run_exchange() -> ResponseArtifact:
                try:
                    return await channel_a.exchange(command)
                except asyncio.CancelledError as error:
                    exchange_cancellations.append(error)
                    raise

            exchange_task = asyncio.create_task(run_exchange())
            try:
                await asyncio.wait_for(original_wait_started.wait(), timeout=1)
                assert exchange_task.cancel(cancellation_message) is True
                with pytest.raises(asyncio.CancelledError) as caught:
                    await _hard_task_result(
                        exchange_task,
                        timeout_seconds=2,
                        description="XS09 cancelled exchange",
                    )
                caught_cancellation = caught.value
                trace.append("cancellation.reraised")
            finally:
                await _cancel_and_drain_task(
                    exchange_task,
                    timeout_seconds=1,
                    description="XS09 exchange",
                )

            assert caught_cancellation is not None
            assert caught_cancellation.args == (cancellation_message,)
            assert wait_cancellations == [caught_cancellation]
            assert wait_cancellations[0] is caught_cancellation
            assert exchange_cancellations == [caught_cancellation]
            assert exchange_cancellations[0] is caught_cancellation
            assert caught_cancellation.__cause__ is None
            assert caught_cancellation.__context__ is None
            assert request_uuid_calls == 1
            assert cancel_uuid_calls == 1
            assert wait_calls == 2
            assert publication_kinds == ["request", "cancel"]
            assert idle_publications == 0
            loaded = transport_a.journals.load()
            assert loaded is not None
            original = loaded.journal.original
            assert original.state is PendingPhase.QUARANTINED
            assert original.revision == 4
            assert original.response_artifact is None
            assert original.settlement is None
            assert len(original.delivery_intents) == 2
            request_intent, cancel_intent = original.delivery_intents
            assert request_intent.published_at_ms is not None
            assert cancel_intent.published_at_ms is not None
            (
                expected_r3_journal,
                _published_request,
                cancel_published_request,
                expected_request_delivery,
                _unpublished_cancel_delivery,
                expected_cancel_delivery,
            ) = _r3_snapshot_models(
                harness,
                command,
                created_at_ms=original.created_at_ms,
                request_published_at_ms=request_intent.published_at_ms,
                cancel_intended_at_ms=cancel_intent.intended_at_ms,
                cancel_published_at_ms=cancel_intent.published_at_ms,
            )
            expected_quarantined_original = PendingExchange.model_validate(
                {
                    **expected_r3_journal.original.model_dump(
                        mode="python",
                        round_trip=True,
                        warnings=False,
                    ),
                    "revision": 4,
                    "state": PendingPhase.QUARANTINED,
                    "updated_at_ms": original.updated_at_ms,
                }
            )
            expected_quarantined_journal = PendingJournal(
                header=expected_r3_journal.header,
                original=expected_quarantined_original,
                reconcile_attempt=None,
            )
            assert loaded.journal == expected_quarantined_journal
            expected_error_json = _canonical_json_bytes(
                {
                    "code": ErrorCode.INDETERMINATE_OUTCOME.value,
                    "details": {
                        "reason": "cancel_ack_timeout",
                        "request_id": str(REQUEST_ID),
                    },
                    "message": "mutation cancellation acknowledgement timed out",
                }
            ).decode("utf-8")
            expected_quarantined_request = RequestRecord.model_validate(
                {
                    **cancel_published_request.model_dump(
                        mode="python",
                        round_trip=True,
                        warnings=False,
                    ),
                    "state": HostRequestState.QUARANTINED,
                    "updated_at_ms": original.updated_at_ms,
                    "error_json": expected_error_json,
                }
            )
            request = transport_a.ledger.get_request(REQUEST_ID)
            deliveries = transport_a.ledger.list_deliveries(REQUEST_ID)
            assert request == expected_quarantined_request
            assert deliveries == (expected_request_delivery, expected_cancel_delivery)
            assert harness.paths.inbox.read_bytes() == render_delivery_lua(
                prepare_delivery(
                    command.body,
                    request_id=REQUEST_ID,
                    delivery_id=CANCEL_ID,
                    delivery_kind="cancel",
                ),
                harness.snapshot,
            )
            assert not harness.paths.response_path(REQUEST_ID).exists()
            quarantined_journal = loaded.journal
            quarantined_request = request
            quarantined_deliveries = deliveries

    session_a_task = asyncio.create_task(run_session_a())
    await _hard_task_result(
        session_a_task,
        timeout_seconds=5,
        description="XS09 session A",
    )

    assert exchange_task is not None and exchange_task.done()
    assert trace == ["cancellation.reraised", "a.unlock.enter", "a.unlock.returned"]
    assert unlock_observations == [(PendingPhase.QUARANTINED, HostRequestState.QUARANTINED)]
    assert quarantined_journal is not None
    assert quarantined_request is not None
    assert len(quarantined_deliveries) == 2
    journal_bytes = harness.paths.pending_file.read_bytes()
    inbox_bytes = harness.paths.inbox.read_bytes()

    transport_b = _make_transport(harness, cancel_ack_timeout_seconds=0.05)
    assert transport_b is not transport_a
    assert transport_b.root_lock is not transport_a.root_lock
    assert transport_b.database is not transport_a.database
    reports: list[RecoveryReport] = []
    forbidden_uuid_calls = 0
    unexpected_delivery_publications: list[PreparedDelivery] = []
    unexpected_idle_publications = 0
    unexpected_cleanup_calls = 0
    installed_recover = RecoveryManager.recover_pending
    installed_b_publish = InboxPublisher.publish_delivery
    installed_b_idle = InboxPublisher.publish_idle

    async def capture_recovery(manager: RecoveryManager) -> RecoveryReport:
        report = await installed_recover(manager)
        reports.append(report)
        return report

    def forbid_uuid4() -> UUID:
        nonlocal forbidden_uuid_calls
        forbidden_uuid_calls += 1
        raise AssertionError("fresh quarantined session requested a UUID")

    def forbid_b_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        if publisher is transport_b.inbox:
            unexpected_delivery_publications.append(delivery)
            raise AssertionError("fresh quarantined session published a delivery")
        installed_b_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)

    def forbid_b_idle(publisher: InboxPublisher) -> None:
        nonlocal unexpected_idle_publications
        if publisher is transport_b.inbox:
            unexpected_idle_publications += 1
            raise AssertionError("fresh quarantined session published idle")
        installed_b_idle(publisher)

    def forbid_b_cleanup(cleanup: PinnedResponseCleanup) -> None:
        nonlocal unexpected_cleanup_calls
        del cleanup
        unexpected_cleanup_calls += 1
        raise AssertionError("fresh quarantined session cleaned a response")

    monkeypatch.setattr(RecoveryManager, "recover_pending", capture_recovery)
    monkeypatch.setattr(
        mutation_module,
        "uuid",
        SimpleNamespace(uuid4=forbid_uuid4),
    )
    monkeypatch.setattr(
        recovery_module,
        "uuid",
        SimpleNamespace(uuid4=forbid_uuid4),
    )
    monkeypatch.setattr(InboxPublisher, "publish_delivery", forbid_b_publication)
    monkeypatch.setattr(InboxPublisher, "publish_idle", forbid_b_idle)
    monkeypatch.setattr(PinnedResponseCleanup, "delete", forbid_b_cleanup)

    async def run_session_b() -> None:
        async with transport_b.session() as channel_b:
            assert reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.QUARANTINED,
                    request_id=REQUEST_ID,
                    request_state=HostRequestState.QUARANTINED,
                    response_cleanup_required=False,
                )
            ]
            blocked_task = asyncio.create_task(
                channel_b.exchange(_command(harness, request_id=NEW_REQUEST_ID))
            )
            with pytest.raises(BridgeError) as blocked:
                await _hard_task_result(
                    blocked_task,
                    timeout_seconds=1,
                    description="XS09 quarantined mutation rejection",
                )
            assert blocked.value.code is ErrorCode.MUTATION_QUARANTINED
            assert blocked.value.details == {
                "request_id": str(REQUEST_ID),
                "state": PendingPhase.QUARANTINED.value,
                "required_release_id": harness.snapshot.release_id,
            }
            assert forbidden_uuid_calls == 0
            assert unexpected_delivery_publications == []
            assert unexpected_idle_publications == 0
            assert unexpected_cleanup_calls == 0
            loaded_b = transport_b.journals.load()
            assert loaded_b is not None and loaded_b.journal == quarantined_journal
            assert transport_b.ledger.get_request(REQUEST_ID) == quarantined_request
            assert transport_b.ledger.list_deliveries(REQUEST_ID) == quarantined_deliveries
            assert transport_b.ledger.get_request(NEW_REQUEST_ID) is None
            assert harness.paths.pending_file.read_bytes() == journal_bytes
            assert harness.paths.inbox.read_bytes() == inbox_bytes
            assert not harness.paths.response_path(REQUEST_ID).exists()
            assert not harness.paths.response_path(NEW_REQUEST_ID).exists()

    session_b_task = asyncio.create_task(run_session_b())
    await _hard_task_result(
        session_b_task,
        timeout_seconds=5,
        description="XS09 session B",
    )

    audit = _make_transport(harness)
    assert audit is not transport_a and audit is not transport_b
    assert audit.root_lock is not transport_a.root_lock
    assert audit.root_lock is not transport_b.root_lock
    assert audit.database is not transport_a.database
    assert audit.database is not transport_b.database
    async with audit.root_lock:
        loaded_audit = audit.journals.load()
        assert loaded_audit is not None and loaded_audit.journal == quarantined_journal
        assert audit.ledger.get_request(REQUEST_ID) == quarantined_request
        assert audit.ledger.list_deliveries(REQUEST_ID) == quarantined_deliveries
        assert audit.ledger.get_request(NEW_REQUEST_ID) is None
        assert harness.paths.pending_file.read_bytes() == journal_bytes
        assert harness.paths.inbox.read_bytes() == inbox_bytes
        assert not harness.paths.response_path(REQUEST_ID).exists()
        assert not harness.paths.response_path(NEW_REQUEST_ID).exists()


@pytest.mark.parametrize(
    ("case", "terminal_state"),
    [
        ("cancelled_ack", HostRequestState.CANCELLED),
        ("completed_ack", HostRequestState.COMPLETED),
        ("original_completion", HostRequestState.COMPLETED),
    ],
)
@pytest.mark.asyncio
async def test_cancelled_exchange_terminal_outcome_is_stable_in_fresh_session(
    matrix_harness: MatrixHarness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    terminal_state: HostRequestState,
) -> None:
    harness = matrix_harness
    command = _command(harness)
    transport_a = _make_transport(harness, cancel_ack_timeout_seconds=2)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(render_idle_lua())
    request_uuid_calls = 0
    cancel_uuid_calls = 0
    publication_kinds: list[str] = []
    idle_publications = 0
    wait_calls = 0
    original_wait_started = asyncio.Event()
    dual_wait_started = asyncio.Event()
    wait_cancellations: list[asyncio.CancelledError] = []
    exchange_cancellations: list[asyncio.CancelledError] = []
    cleanup_trace: list[str] = []
    response_path = harness.paths.response_path(REQUEST_ID)
    installed_publish = InboxPublisher.publish_delivery
    installed_idle = InboxPublisher.publish_idle
    installed_wait = ResponseWaiter.wait
    installed_exit = RootLock.__aexit__
    installed_delete = PinnedResponseCleanup.delete

    def one_shot_request_uuid4() -> UUID:
        nonlocal request_uuid_calls
        request_uuid_calls += 1
        if request_uuid_calls != 1:
            raise AssertionError("XS10 requested more than one request UUID")
        return DELIVERY_ID

    def one_shot_cancel_uuid4() -> UUID:
        nonlocal cancel_uuid_calls
        cancel_uuid_calls += 1
        if cancel_uuid_calls != 1:
            raise AssertionError("XS10 requested more than one cancel UUID")
        return CANCEL_ID

    def capture_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)
        if publisher is transport_a.inbox:
            publication_kinds.append(delivery.delivery_kind)

    def capture_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_publications
        installed_idle(publisher)
        if publisher is transport_a.inbox:
            idle_publications += 1

    async def capture_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        nonlocal wait_calls
        wait_calls += 1
        expectation = waiter._expectation  # pyright: ignore[reportPrivateUsage]
        if wait_calls == 1:
            assert expectation.allowed_deliveries == (
                AllowedDelivery(delivery_id=DELIVERY_ID, delivery_kind="request"),
            )
            original_wait_started.set()
            try:
                return await installed_wait(waiter, timeout_seconds)
            except asyncio.CancelledError as error:
                wait_cancellations.append(error)
                raise
        if wait_calls == 2:
            assert expectation.allowed_deliveries == (
                AllowedDelivery(delivery_id=DELIVERY_ID, delivery_kind="request"),
                AllowedDelivery(delivery_id=CANCEL_ID, delivery_kind="cancel"),
            )
            dual_wait_started.set()
            return await installed_wait(waiter, timeout_seconds)
        raise AssertionError("XS10 reached an unexpected third response wait")

    async def capture_unlock(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is transport_a.root_lock:
            assert response_path.is_file()
        result = await installed_exit(lock, exc_type, exc, traceback)
        if lock is transport_a.root_lock:
            assert response_path.is_file()
            cleanup_trace.append("unlock.returned")
        return result

    def capture_cleanup(cleanup: PinnedResponseCleanup) -> None:
        cleanup_trace.append("cleanup.delete.entered")
        assert cleanup_trace[-2:] == ["unlock.returned", "cleanup.delete.entered"]
        assert response_path.is_file()
        installed_delete(cleanup)
        assert not response_path.exists()
        cleanup_trace.append("cleanup.delete.returned")

    monkeypatch.setattr(
        mutation_module,
        "uuid",
        SimpleNamespace(uuid4=one_shot_request_uuid4),
    )
    monkeypatch.setattr(
        recovery_module,
        "uuid",
        SimpleNamespace(uuid4=one_shot_cancel_uuid4),
    )
    monkeypatch.setattr(InboxPublisher, "publish_delivery", capture_publication)
    monkeypatch.setattr(InboxPublisher, "publish_idle", capture_idle)
    monkeypatch.setattr(ResponseWaiter, "wait", capture_wait)
    monkeypatch.setattr(RootLock, "__aexit__", capture_unlock)
    monkeypatch.setattr(PinnedResponseCleanup, "delete", capture_cleanup)

    expected_raw, expected_terminal_result, expected_ack, responding_delivery_id = (
        _terminal_response_bytes(harness, command, case)
    )
    caught_cancellation: asyncio.CancelledError | None = None
    terminal_request: RequestRecord | None = None
    terminal_deliveries: tuple[DeliveryRecord, ...] = ()
    cancellation_message = f"XS10 cancel for {case}"
    exchange_task: asyncio.Task[ResponseArtifact] | None = None
    responder_task: asyncio.Task[bytes] | None = None
    responder_raw: bytes | None = None

    async def run_session_a() -> None:
        nonlocal caught_cancellation, exchange_task, responder_raw, responder_task
        nonlocal terminal_deliveries, terminal_request
        async with transport_a.session() as channel_a:

            async def run_exchange() -> ResponseArtifact:
                try:
                    return await channel_a.exchange(command)
                except asyncio.CancelledError as error:
                    exchange_cancellations.append(error)
                    raise

            responder_task = asyncio.create_task(
                _respond_after_dual_wait(
                    harness,
                    transport_a,
                    command,
                    case,
                    dual_wait_started,
                )
            )
            exchange_task = asyncio.create_task(run_exchange())
            try:
                await asyncio.wait_for(original_wait_started.wait(), timeout=1)
                assert exchange_task.cancel(cancellation_message) is True
                with pytest.raises(asyncio.CancelledError) as caught:
                    await _hard_task_result(
                        exchange_task,
                        timeout_seconds=3,
                        description=f"XS10 {case} cancelled exchange",
                    )
                caught_cancellation = caught.value
                cleanup_trace.append("cancellation.reraised")
                responder_raw = await _hard_task_result(
                    responder_task,
                    timeout_seconds=1,
                    description=f"XS10 {case} responder",
                )
            finally:
                await _cancel_and_drain_task(
                    exchange_task,
                    timeout_seconds=1,
                    description=f"XS10 {case} exchange",
                )
                await _cancel_and_drain_task(
                    responder_task,
                    timeout_seconds=1,
                    description=f"XS10 {case} responder",
                )

            assert responder_raw == expected_raw
            assert caught_cancellation is not None
            assert caught_cancellation.args == (cancellation_message,)
            assert wait_cancellations == [caught_cancellation]
            assert wait_cancellations[0] is caught_cancellation
            assert exchange_cancellations == [caught_cancellation]
            assert exchange_cancellations[0] is caught_cancellation
            assert caught_cancellation.__cause__ is None
            assert caught_cancellation.__context__ is None
            assert request_uuid_calls == 1
            assert cancel_uuid_calls == 1
            assert wait_calls == 2
            assert publication_kinds == ["request", "cancel"]
            assert idle_publications == 1
            assert transport_a.journals.load() is None
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            request = transport_a.ledger.get_request(REQUEST_ID)
            deliveries = transport_a.ledger.list_deliveries(REQUEST_ID)
            assert request is not None and request.state is terminal_state
            assert request.terminal_at_ms is not None
            assert request.error_json is None
            assert request.resolution_json is None
            expected_result_json = (
                None
                if expected_terminal_result is None
                else _canonical_json_bytes(expected_terminal_result).decode("utf-8")
            )
            assert request.result_json == expected_result_json
            assert tuple(delivery.delivery_id for delivery in deliveries) == (
                DELIVERY_ID,
                CANCEL_ID,
            )
            responding = next(
                delivery
                for delivery in deliveries
                if delivery.delivery_id == responding_delivery_id
            )
            nonresponding = next(
                delivery
                for delivery in deliveries
                if delivery.delivery_id != responding_delivery_id
            )
            artifact = responding.response_artifact
            assert artifact is not None
            assert artifact.filename == response_path.name
            assert artifact.sha256 == hashlib.sha256(expected_raw).hexdigest()
            assert artifact.size_bytes == len(expected_raw)
            assert artifact.accepted_response.envelope.delivery_id == responding_delivery_id
            assert artifact.accepted_response.cancel_ack == expected_ack
            assert artifact.accepted_response.result == expected_terminal_result
            if terminal_state is HostRequestState.CANCELLED:
                assert artifact.accepted_response.settlement == CancelledSettlement(
                    state="cancelled"
                )
            else:
                assert artifact.accepted_response.settlement == CompletedSettlement(
                    state="completed",
                    result=expected_terminal_result,
                )
            assert responding.settlement == artifact.accepted_response.settlement
            assert nonresponding.response_artifact is None
            assert nonresponding.settlement is None
            assert response_path.is_file()
            terminal_request = request
            terminal_deliveries = deliveries

    session_a_task = asyncio.create_task(run_session_a())
    await _hard_task_result(
        session_a_task,
        timeout_seconds=6,
        description=f"XS10 {case} session A",
    )

    assert exchange_task is not None and exchange_task.done()
    assert responder_task is not None and responder_task.done()
    assert cleanup_trace == [
        "cancellation.reraised",
        "unlock.returned",
        "cleanup.delete.entered",
        "cleanup.delete.returned",
    ]
    assert terminal_request is not None
    assert len(terminal_deliveries) == 2
    assert not response_path.exists()
    assert not harness.paths.pending_file.exists()

    transport_b = _make_transport(harness)
    assert transport_b is not transport_a
    assert transport_b.root_lock is not transport_a.root_lock
    assert transport_b.database is not transport_a.database
    reports: list[RecoveryReport] = []
    forbidden_uuid_calls = 0
    unexpected_delivery_publications: list[PreparedDelivery] = []
    unexpected_idle_publications = 0
    unexpected_cleanup_calls = 0
    installed_recover = RecoveryManager.recover_pending
    installed_b_publish = InboxPublisher.publish_delivery
    installed_b_idle = InboxPublisher.publish_idle

    async def capture_recovery(manager: RecoveryManager) -> RecoveryReport:
        report = await installed_recover(manager)
        reports.append(report)
        return report

    def forbid_uuid4() -> UUID:
        nonlocal forbidden_uuid_calls
        forbidden_uuid_calls += 1
        raise AssertionError("XS10 fresh terminal session requested a UUID")

    def forbid_b_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        if publisher is transport_b.inbox:
            unexpected_delivery_publications.append(delivery)
            raise AssertionError("XS10 fresh terminal session published a delivery")
        installed_b_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)

    def forbid_b_idle(publisher: InboxPublisher) -> None:
        nonlocal unexpected_idle_publications
        if publisher is transport_b.inbox:
            unexpected_idle_publications += 1
            raise AssertionError("XS10 fresh terminal session published idle")
        installed_b_idle(publisher)

    def forbid_b_cleanup(cleanup: PinnedResponseCleanup) -> None:
        nonlocal unexpected_cleanup_calls
        del cleanup
        unexpected_cleanup_calls += 1
        raise AssertionError("XS10 fresh terminal session cleaned a response")

    monkeypatch.setattr(RecoveryManager, "recover_pending", capture_recovery)
    monkeypatch.setattr(
        mutation_module,
        "uuid",
        SimpleNamespace(uuid4=forbid_uuid4),
    )
    monkeypatch.setattr(
        recovery_module,
        "uuid",
        SimpleNamespace(uuid4=forbid_uuid4),
    )
    monkeypatch.setattr(InboxPublisher, "publish_delivery", forbid_b_publication)
    monkeypatch.setattr(InboxPublisher, "publish_idle", forbid_b_idle)
    monkeypatch.setattr(PinnedResponseCleanup, "delete", forbid_b_cleanup)

    async def run_session_b() -> None:
        async with transport_b.session():
            assert reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.NO_PENDING,
                    request_id=None,
                    request_state=None,
                    response_cleanup_required=False,
                )
            ]
            assert forbidden_uuid_calls == 0
            assert unexpected_delivery_publications == []
            assert unexpected_idle_publications == 0
            assert unexpected_cleanup_calls == 0
            assert transport_b.journals.load() is None
            assert transport_b.ledger.get_request(REQUEST_ID) == terminal_request
            assert transport_b.ledger.list_deliveries(REQUEST_ID) == terminal_deliveries
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            assert not response_path.exists()

    session_b_task = asyncio.create_task(run_session_b())
    await _hard_task_result(
        session_b_task,
        timeout_seconds=5,
        description=f"XS10 {case} session B",
    )

    audit = _make_transport(harness)
    assert audit is not transport_a and audit is not transport_b
    assert audit.root_lock is not transport_a.root_lock
    assert audit.root_lock is not transport_b.root_lock
    assert audit.database is not transport_a.database
    assert audit.database is not transport_b.database
    async with audit.root_lock:
        assert audit.journals.load() is None
        assert audit.ledger.get_request(REQUEST_ID) == terminal_request
        assert audit.ledger.list_deliveries(REQUEST_ID) == terminal_deliveries
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        assert not response_path.exists()


@pytest.mark.asyncio
async def test_cancelled_fresh_session_entry_shields_real_pending_recovery_before_unlock(
    matrix_harness: MatrixHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = matrix_harness
    command = _command(harness)
    prepared = prepare_delivery(
        command.body,
        request_id=REQUEST_ID,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
    )
    rendered_request = render_delivery_lua(prepared, harness.snapshot)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(render_idle_lua())
    checkpoint = harness.local_app_data / "xs11-after-request-replace.checkpoint"
    helper = _start_crash_helper(harness, "after_request_replace", checkpoint)
    try:
        checkpoint_payload = await _wait_for_crash(
            helper,
            checkpoint,
            "after_request_replace",
        )
    finally:
        _kill_helper(helper)

    assert checkpoint_payload == {
        "boundary": "after_request_replace",
        "request_id": str(REQUEST_ID),
        "delivery_id": str(DELIVERY_ID),
        "request_publications": "1",
        "cancel_publications": "0",
        "idle_publications": "0",
        "request_uuid_calls": "1",
    }
    assert harness.paths.inbox.read_bytes() == rendered_request
    response_path = harness.paths.response_path(REQUEST_ID)
    assert not response_path.exists()

    peer_trace: list[str] = []
    peer = _peer(harness, peer_trace)
    peer.enqueue(Respond(result=_completed_result(command)))
    await peer.start()
    try:
        await _wait_for_peer_response(peer, response_path)
    finally:
        peer_stop_task = asyncio.create_task(peer.stop())
        await _hard_task_result(
            peer_stop_task,
            timeout_seconds=2,
            description="XS11 fake peer stop",
        )

    observed = peer.observed_deliveries
    assert len(observed) == 1
    assert observed[0] == prepared
    assert peer_trace == ["peer.request_observed", "peer.response_written"]
    with response_path.open("r+b") as stream:
        expected_raw = stream.read()
        stream.flush()
        os.fsync(stream.fileno())
    assert expected_raw
    assert response_path.read_bytes() == expected_raw
    expected_sha256 = hashlib.sha256(expected_raw).hexdigest()
    expected_size = len(expected_raw)

    diagnostic = _make_transport(harness)
    await _assert_r0_crash_snapshot(
        diagnostic,
        harness,
        command,
        request_present=True,
        delivery_present=True,
    )
    pre_b_journal: PendingJournal | None = None
    pre_b_request: RequestRecord | None = None
    pre_b_deliveries: tuple[DeliveryRecord, ...] = ()
    journal_bytes = b""
    inbox_bytes = b""
    raw_bytes = b""
    async with diagnostic.root_lock:
        pre_b_loaded = diagnostic.journals.load()
        pre_b_request = diagnostic.ledger.get_request(REQUEST_ID)
        pre_b_deliveries = diagnostic.ledger.list_deliveries(REQUEST_ID)
        assert pre_b_loaded is not None
        pre_b_journal = pre_b_loaded.journal
        assert pre_b_request is not None
        assert len(pre_b_deliveries) == 1
        journal_bytes = harness.paths.pending_file.read_bytes()
        inbox_bytes = harness.paths.inbox.read_bytes()
        raw_bytes = response_path.read_bytes()
    assert inbox_bytes == rendered_request
    assert raw_bytes == expected_raw
    assert pre_b_journal is not None

    transport_b = _make_transport(harness)
    transport_c = _make_transport(harness)
    assert transport_b is not diagnostic and transport_c is not diagnostic
    assert transport_b is not transport_c
    assert transport_b.root_lock is not diagnostic.root_lock
    assert transport_c.root_lock is not diagnostic.root_lock
    assert transport_b.root_lock is not transport_c.root_lock
    assert transport_b.database is not diagnostic.database
    assert transport_c.database is not diagnostic.database
    assert transport_b.database is not transport_c.database

    recovery_started = asyncio.Event()
    release_recovery = asyncio.Event()
    b_reports: list[RecoveryReport] = []
    c_reports: list[RecoveryReport] = []
    b_worker: asyncio.Task[object] | None = None
    entry_cancellations: list[asyncio.CancelledError] = []
    publication_calls: list[tuple[InboxPublisher, PreparedDelivery]] = []
    b_idle_calls = 0
    c_idle_calls = 0
    forbidden_uuid_calls = 0
    cleanup_trace: list[str] = []
    cleanup_calls = 0
    terminal_request: RequestRecord | None = None
    terminal_deliveries: tuple[DeliveryRecord, ...] = ()
    installed_recover = RecoveryManager.recover_pending
    installed_publish = InboxPublisher.publish_delivery
    installed_idle = InboxPublisher.publish_idle
    installed_exit = RootLock.__aexit__
    installed_delete = PinnedResponseCleanup.delete

    async def route_recovery(manager: RecoveryManager) -> RecoveryReport:
        nonlocal b_worker
        root_lock = manager._root_lock  # pyright: ignore[reportPrivateUsage]
        if root_lock is transport_b.root_lock:
            assert b_worker is None
            assert b_reports == []
            current = asyncio.current_task()
            assert current is not None
            b_worker = cast(asyncio.Task[object], current)
            recovery_started.set()
            await release_recovery.wait()
            report = await installed_recover(manager)
            b_reports.append(report)
            return report
        if root_lock is transport_c.root_lock:
            assert c_reports == []
            report = await installed_recover(manager)
            c_reports.append(report)
            return report
        raise AssertionError("XS11 recovery used an unknown transport")

    def capture_publication(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        if publisher is transport_b.inbox or publisher is transport_c.inbox:
            publication_calls.append((publisher, delivery))
        installed_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)

    def capture_idle(publisher: InboxPublisher) -> None:
        nonlocal b_idle_calls, c_idle_calls
        installed_idle(publisher)
        if publisher is transport_b.inbox:
            b_idle_calls += 1
        elif publisher is transport_c.inbox:
            c_idle_calls += 1

    def forbid_uuid4() -> UUID:
        nonlocal forbidden_uuid_calls
        forbidden_uuid_calls += 1
        raise AssertionError("XS11 startup recovery requested a UUID")

    async def capture_unlock(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        nonlocal terminal_request, terminal_deliveries
        if lock is transport_b.root_lock:
            assert b_reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.SETTLED,
                    request_id=REQUEST_ID,
                    request_state=HostRequestState.COMPLETED,
                    response_cleanup_required=True,
                )
            ]
            assert b_worker is not None and b_worker.done()
            assert not b_worker.cancelled()
            assert transport_b.journals.load() is None
            request = transport_b.ledger.get_request(REQUEST_ID)
            deliveries = transport_b.ledger.list_deliveries(REQUEST_ID)
            assert request is not None and request.state is HostRequestState.COMPLETED
            assert len(deliveries) == 1
            terminal_request = request
            terminal_deliveries = deliveries
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            assert response_path.is_file()
        result = await installed_exit(lock, exc_type, exc, traceback)
        if lock is transport_b.root_lock:
            assert response_path.is_file()
            cleanup_trace.append("unlock.returned")
        return result

    def capture_cleanup(cleanup: PinnedResponseCleanup) -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1
        cleanup_trace.append("cleanup.delete.entered")
        assert cleanup_calls == 1
        assert cleanup_trace == ["unlock.returned", "cleanup.delete.entered"]
        assert response_path.is_file()
        installed_delete(cleanup)
        assert not response_path.exists()
        cleanup_trace.append("cleanup.delete.returned")

    monkeypatch.setattr(RecoveryManager, "recover_pending", route_recovery)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", capture_publication)
    monkeypatch.setattr(InboxPublisher, "publish_idle", capture_idle)
    monkeypatch.setattr(
        mutation_module,
        "uuid",
        SimpleNamespace(uuid4=forbid_uuid4),
    )
    monkeypatch.setattr(
        recovery_module,
        "uuid",
        SimpleNamespace(uuid4=forbid_uuid4),
    )
    monkeypatch.setattr(RootLock, "__aexit__", capture_unlock)
    monkeypatch.setattr(PinnedResponseCleanup, "delete", capture_cleanup)

    session_b = transport_b.session()
    entered_channel: object | None = None

    async def enter_b() -> object:
        nonlocal entered_channel
        try:
            entered_channel = await session_b.__aenter__()
            return entered_channel
        except asyncio.CancelledError as error:
            entry_cancellations.append(error)
            cleanup_trace.append("cancellation.reraised")
            raise

    cancellation_message = "XS11 cancel fresh session entry"
    entry_task = asyncio.create_task(enter_b())
    caught_cancellation: asyncio.CancelledError | None = None
    accepted_cancellations = 0
    entry_outcome_consumed = False
    try:
        await asyncio.wait_for(recovery_started.wait(), timeout=2)
        assert b_worker is not None
        transport_b.root_lock.require_acquired()
        assert entry_task.cancel(cancellation_message) is True
        accepted_cancellations += 1
        await asyncio.sleep(0)
        assert not entry_task.done()
        assert not b_worker.done()
        transport_b.root_lock.require_acquired()
        loaded_while_gated = transport_b.journals.load()
        assert loaded_while_gated is not None
        assert loaded_while_gated.journal == pre_b_journal
        assert transport_b.ledger.get_request(REQUEST_ID) == pre_b_request
        assert transport_b.ledger.list_deliveries(REQUEST_ID) == pre_b_deliveries
        assert harness.paths.pending_file.read_bytes() == journal_bytes
        assert harness.paths.inbox.read_bytes() == inbox_bytes
        assert response_path.read_bytes() == raw_bytes

        release_recovery.set()
        with pytest.raises(asyncio.CancelledError) as caught:
            await _hard_task_result(
                entry_task,
                timeout_seconds=5,
                description="XS11 expected session-B entry completion",
            )
        caught_cancellation = caught.value
        entry_outcome_consumed = True
    finally:
        release_recovery.set()
        if not entry_outcome_consumed:
            await _cancel_and_drain_task(
                entry_task,
                timeout_seconds=2,
                description="XS11 session-B entry",
            )
        if entered_channel is not None:
            unexpected_exit_task = asyncio.create_task(session_b.__aexit__(None, None, None))
            await _hard_task_result(
                unexpected_exit_task,
                timeout_seconds=2,
                description="XS11 unexpected session-B exit",
            )

    assert caught_cancellation is not None
    assert caught_cancellation.args == (cancellation_message,)
    assert entry_cancellations == [caught_cancellation]
    assert entry_cancellations[0] is caught_cancellation
    assert caught_cancellation.__cause__ is None
    assert caught_cancellation.__context__ is None
    assert accepted_cancellations == 1
    assert b_worker is not None and b_worker.done()
    assert not b_worker.cancelled()
    assert b_worker.exception() is None
    assert b_reports == [
        RecoveryReport(
            disposition=RecoveryDisposition.SETTLED,
            request_id=REQUEST_ID,
            request_state=HostRequestState.COMPLETED,
            response_cleanup_required=True,
        )
    ]
    assert cleanup_trace == [
        "unlock.returned",
        "cleanup.delete.entered",
        "cleanup.delete.returned",
        "cancellation.reraised",
    ]
    assert cleanup_calls == 1
    assert publication_calls == []
    assert b_idle_calls == 1
    assert c_idle_calls == 0
    assert forbidden_uuid_calls == 0
    assert len(peer.observed_deliveries) == 1
    assert peer.observed_deliveries[0] == prepared
    assert terminal_request is not None
    assert terminal_request.state is HostRequestState.COMPLETED
    assert terminal_request.terminal_at_ms is not None
    assert len(terminal_deliveries) == 1
    terminal_delivery = terminal_deliveries[0]
    assert terminal_delivery.delivery_id == DELIVERY_ID
    assert terminal_delivery.delivery_kind == "request"
    assert terminal_delivery.published_at_ms is not None
    artifact = terminal_delivery.response_artifact
    assert artifact is not None
    assert artifact.filename == response_path.name
    assert artifact.sha256 == expected_sha256
    assert artifact.size_bytes == expected_size
    assert artifact.accepted_response.envelope.delivery_id == DELIVERY_ID
    assert artifact.accepted_response.result == _completed_result(command)
    assert artifact.accepted_response.settlement == CompletedSettlement(
        state="completed",
        result=_completed_result(command),
    )
    assert terminal_delivery.settlement == artifact.accepted_response.settlement
    assert not harness.paths.pending_file.exists()
    assert harness.paths.inbox.read_bytes() == render_idle_lua()
    assert not response_path.exists()

    async def run_c() -> None:
        async with transport_c.session():
            assert c_reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.NO_PENDING,
                    request_id=None,
                    request_state=None,
                    response_cleanup_required=False,
                )
            ]
            assert b_reports == [
                RecoveryReport(
                    disposition=RecoveryDisposition.SETTLED,
                    request_id=REQUEST_ID,
                    request_state=HostRequestState.COMPLETED,
                    response_cleanup_required=True,
                )
            ]
            assert transport_c.journals.load() is None
            assert transport_c.ledger.get_request(REQUEST_ID) == terminal_request
            assert transport_c.ledger.list_deliveries(REQUEST_ID) == terminal_deliveries
            assert publication_calls == []
            assert b_idle_calls == 1
            assert c_idle_calls == 0
            assert cleanup_calls == 1
            assert forbidden_uuid_calls == 0
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            assert not response_path.exists()

    c_task = asyncio.create_task(run_c())
    await _hard_task_result(
        c_task,
        timeout_seconds=3,
        description="XS11 fresh session C",
    )

    assert c_reports == [
        RecoveryReport(
            disposition=RecoveryDisposition.NO_PENDING,
            request_id=None,
            request_state=None,
            response_cleanup_required=False,
        )
    ]
    assert publication_calls == []
    assert b_idle_calls == 1
    assert c_idle_calls == 0
    assert cleanup_calls == 1
    assert forbidden_uuid_calls == 0
    assert cleanup_trace == [
        "unlock.returned",
        "cleanup.delete.entered",
        "cleanup.delete.returned",
        "cancellation.reraised",
    ]
    assert len(peer.observed_deliveries) == 1
    assert peer.observed_deliveries[0] == prepared
    assert not harness.paths.pending_file.exists()
    assert harness.paths.inbox.read_bytes() == render_idle_lua()
    assert not response_path.exists()
