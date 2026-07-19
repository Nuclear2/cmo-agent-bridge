from __future__ import annotations

import base64
import json
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from cmo_agent_bridge.application.confirmation import ConfirmationTokenStore
from cmo_agent_bridge.application.confirmation import (
    HOST_QUARANTINE_CONFIRMATION_FORMAT,
    HostQuarantineConfirmationDescriptor,
    host_quarantine_confirmation_binding,
)
from cmo_agent_bridge.application.host_quarantine import (
    HostQuarantineResolutionService,
)
from cmo_agent_bridge.application.obsolete_quarantine import (
    ObsoleteQuarantineAbandonmentService,
)
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.models import BridgeStatusResult
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.lua_delivery import render_idle_lua
from cmo_agent_bridge.state.host_resolution import (
    HOST_QUARANTINE_RESOLUTION_FORMAT,
    HostQuarantineResolutionMarker,
    canonical_host_quarantine_resolution,
)
from cmo_agent_bridge.state.models import (
    HostRequestState,
    PendingExchange,
    PendingJournal,
    PendingPhase,
)
from cmo_agent_bridge.state.pending_journal import (
    JournalRevisions,
    PendingJournalStore,
)
from cmo_agent_bridge.state.request_ledger import RequestLedger, RequestRecord
from cmo_agent_bridge.state.operation_queue import (
    OperationQueueRecord,
    OperationQueueState,
    OperationQueueStore,
)
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.models import (
    RecoveryDisposition,
    RecoveryReport,
)
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo
from cmo_agent_bridge.transports.file_bridge.recovery import RecoveryManager
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY


CURRENT_ACTIVATION = UUID("99999999-9999-4999-8999-999999999999")
TOKEN = base64.urlsafe_b64encode(bytes(range(32))).rstrip(b"=").decode("ascii")


class _Clock:
    def __init__(self, *values: int) -> None:
        self._values = list(values)

    def now_ms(self) -> int:
        if not self._values:
            raise AssertionError("unexpected clock read")
        return self._values.pop(0)


class _Inbox:
    def __init__(self) -> None:
        self.idle_calls = 0

    def publish_idle(self) -> None:
        self.idle_calls += 1


class _Channel:
    pass


class _SessionContext:
    def __init__(self, root_lock: RootLock) -> None:
        self._root_lock = root_lock
        self._channel = _Channel()

    async def __aenter__(self) -> _Channel:
        await self._root_lock.__aenter__()
        return self._channel

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        await self._root_lock.__aexit__(exc_type, exc, cast(Any, traceback))


class _Transport:
    def __init__(
        self,
        *,
        paths: FileBridgePaths,
        root_lock: RootLock,
        journals: PendingJournalStore,
        ledger: RequestLedger,
        inbox: _Inbox,
    ) -> None:
        self._paths = paths
        self._root_lock = root_lock
        self._journals = journals
        self._ledger = ledger
        self._inbox = inbox

    @property
    def root_key(self) -> str:
        return self._paths.root_key

    @property
    def journals(self) -> PendingJournalStore:
        return self._journals

    @property
    def ledger(self) -> RequestLedger:
        return self._ledger

    @property
    def inbox(self) -> InboxPublisher:
        return cast(InboxPublisher, self._inbox)

    def session(self) -> AbstractAsyncContextManager[Any]:
        return _SessionContext(self._root_lock)


class _Sessions:
    def __init__(
        self,
        runtime_snapshot: RuntimeSnapshot,
        status: BridgeStatusResult,
    ) -> None:
        self._runtime_snapshot = runtime_snapshot
        self._status = status
        self.calls = 0

    @property
    def runtime_snapshot(self) -> RuntimeSnapshot:
        return self._runtime_snapshot

    async def handshake(
        self,
        channel: object,
        *,
        accept_lineage_id: UUID | None = None,
        reserved_activation_candidate: UUID | None = None,
    ) -> Any:
        del channel, accept_lineage_id, reserved_activation_candidate
        self.calls += 1
        return SimpleNamespace(status=self._status)


class _Inspector:
    def __init__(self, process: ProcessInfo) -> None:
        self._process = process

    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        assert command_exe == self._process.executable
        return (self._process,)


def _token_factory(size: int) -> str:
    assert size == 32
    return TOKEN


def _status(
    snapshot: RuntimeSnapshot,
    journal: PendingJournal,
    *,
    pending_request_id: UUID | None = None,
    quarantined: bool = False,
) -> BridgeStatusResult:
    lineage_id = journal.original.expected_lineage_id
    assert lineage_id is not None
    return BridgeStatusResult(
        protocol=snapshot.protocol,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
        build=1,
        manifest_sha256=snapshot.operation_manifest_sha256,
        lineage_id=lineage_id,
        activation_id=CURRENT_ACTIVATION,
        installed_event_names=[],
        installed_action_names=[],
        installed_trigger_names=[],
        pending_request_id=pending_request_id,
        quarantined=quarantined,
        paused_capability=None,
        poll_interval_seconds=1,
        safe_payload_bytes=65_536,
        verified_ledger_entries=1,
        effective_ledger_capacity=1,
    )


def _request_record(exchange: PendingExchange, root_key: str) -> RequestRecord:
    return RequestRecord(
        request_id=exchange.request_id,
        root_key=root_key,
        request_hash=exchange.request_hash,
        operation=exchange.operation,
        operation_class=exchange.effective_class,
        state=HostRequestState.PREPARED,
        runtime_snapshot=exchange.runtime_snapshot,
        result_schema_id=exchange.result_schema_id,
        recovery_schema_id=exchange.recovery_schema_id,
        body_json=exchange.body_json.encode("utf-8"),
        lineage_id=exchange.expected_lineage_id,
        activation_id=exchange.expected_activation_id,
        result_json=None,
        error_json=None,
        resolution_json=None,
        created_at_ms=exchange.created_at_ms,
        updated_at_ms=exchange.created_at_ms,
        terminal_at_ms=None,
    )


async def _persist_quarantine(
    *,
    valid_journal: PendingJournal,
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    ledger: RequestLedger,
) -> PendingJournal:
    original = valid_journal.original
    published_intent = original.delivery_intents[0].model_copy(
        update={"published_at_ms": 101}
    )
    published_exchange = original.model_copy(
        update={
            "delivery_intents": (published_intent,),
            "revision": 1,
            "state": PendingPhase.PUBLISHED,
            "updated_at_ms": 101,
        }
    )
    published = valid_journal.model_copy(update={"original": published_exchange})
    quarantined_exchange = published_exchange.model_copy(
        update={
            "revision": 2,
            "state": PendingPhase.QUARANTINED,
            "updated_at_ms": 102,
        }
    )
    quarantined = published.model_copy(update={"original": quarantined_exchange})
    root_lock.path.parent.mkdir(parents=True, exist_ok=True)
    async with root_lock:
        assert journal_store.save(
            valid_journal,
            expected_revisions=None,
        ) == JournalRevisions(original=0, reconcile_attempt=None)
        assert journal_store.save(
            published,
            expected_revisions=JournalRevisions(
                original=0,
                reconcile_attempt=None,
            ),
        ) == JournalRevisions(original=1, reconcile_attempt=None)
        assert journal_store.save(
            quarantined,
            expected_revisions=JournalRevisions(
                original=1,
                reconcile_attempt=None,
            ),
        ) == JournalRevisions(original=2, reconcile_attempt=None)
    record = _request_record(original, valid_journal.header.root_key)
    ledger.insert_prepared(record)
    ledger.insert_delivery(original.delivery_intents[0])
    ledger.mark_delivery_published(
        original.delivery_intents[0].delivery_id,
        published_at_ms=101,
    )
    ledger.transition(
        original.request_id,
        expected_states=frozenset({HostRequestState.PREPARED}),
        new_state=HostRequestState.PUBLISHED,
        updated_at_ms=101,
    )
    ledger.transition(
        original.request_id,
        expected_states=frozenset({HostRequestState.PUBLISHED}),
        new_state=HostRequestState.QUARANTINED,
        updated_at_ms=102,
        error_json=json.dumps(
            {
                "code": ErrorCode.INDETERMINATE_OUTCOME.value,
                "details": {},
                "message": "manual test quarantine",
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    return quarantined


@pytest.mark.asyncio
async def test_host_only_resolution_requires_preview_token_and_exact_disposition(
    tmp_path: Path,
    runtime_snapshot: RuntimeSnapshot,
    valid_journal: PendingJournal,
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    manifest_catalog: ManifestCatalog,
) -> None:
    database = StateDatabase(tmp_path / "state.sqlite3")
    database.initialize()
    ledger = RequestLedger(database, manifest_catalog)
    quarantined = await _persist_quarantine(
        valid_journal=valid_journal,
        journal_store=journal_store,
        root_lock=root_lock,
        ledger=ledger,
    )
    inbox = _Inbox()
    transport = _Transport(
        paths=file_bridge_paths,
        root_lock=root_lock,
        journals=journal_store,
        ledger=ledger,
        inbox=inbox,
    )
    sessions = _Sessions(runtime_snapshot, _status(runtime_snapshot, quarantined))
    confirmations = ConfirmationTokenStore(
        database,
        token_factory=_token_factory,
    )
    service = HostQuarantineResolutionService(
        transport=cast(Any, transport),
        sessions=cast(Any, sessions),
        confirmations=confirmations,
        wall_clock=_Clock(1_000, 2_000, 3_000, 4_000, 5_000),
    )

    preview = await service.preview("applied")

    assert preview.mode == "host_only"
    assert preview.manual_evidence_required is True
    assert preview.runtime_barrier_absent is True
    assert preview.request_id == quarantined.original.request_id
    assert preview.disposition == "applied"
    assert preview.confirmation_token == TOKEN

    with pytest.raises(BridgeError) as wrong:
        await service.confirm("not_applied", TOKEN)
    assert wrong.value.code is ErrorCode.POLICY_DENIED

    resolved = await service.confirm("applied", TOKEN)

    assert resolved.mode == "host_only"
    assert resolved.manual_evidence is True
    assert resolved.disposition == "applied"
    assert inbox.idle_calls == 1
    assert not file_bridge_paths.pending_file.exists()
    record = ledger.get_request(quarantined.original.request_id)
    assert record is not None
    assert record.state is HostRequestState.RESOLVED
    assert record.resolution_json is not None
    marker = HostQuarantineResolutionMarker.model_validate_json(
        record.resolution_json,
        strict=True,
    )
    assert marker.format == HOST_QUARANTINE_RESOLUTION_FORMAT
    assert marker.manual_evidence is True
    assert marker.disposition == "applied"
    assert canonical_host_quarantine_resolution(marker) == record.resolution_json


@pytest.mark.asyncio
async def test_host_only_preview_refuses_a_runtime_barrier(
    tmp_path: Path,
    runtime_snapshot: RuntimeSnapshot,
    valid_journal: PendingJournal,
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    manifest_catalog: ManifestCatalog,
) -> None:
    database = StateDatabase(tmp_path / "state.sqlite3")
    database.initialize()
    ledger = RequestLedger(database, manifest_catalog)
    quarantined = await _persist_quarantine(
        valid_journal=valid_journal,
        journal_store=journal_store,
        root_lock=root_lock,
        ledger=ledger,
    )
    transport = _Transport(
        paths=file_bridge_paths,
        root_lock=root_lock,
        journals=journal_store,
        ledger=ledger,
        inbox=_Inbox(),
    )
    service = HostQuarantineResolutionService(
        transport=cast(Any, transport),
        sessions=cast(
            Any,
            _Sessions(
                runtime_snapshot,
                _status(
                    runtime_snapshot,
                    quarantined,
                    pending_request_id=quarantined.original.request_id,
                    quarantined=True,
                ),
            ),
        ),
        confirmations=ConfirmationTokenStore(database),
        wall_clock=_Clock(1_000),
    )

    with pytest.raises(BridgeError) as caught:
        await service.preview("applied")

    assert caught.value.code is ErrorCode.POLICY_DENIED
    assert file_bridge_paths.pending_file.exists()
    record = ledger.get_request(quarantined.original.request_id)
    assert record is not None
    assert record.state is HostRequestState.QUARANTINED


@pytest.mark.asyncio
async def test_startup_recovery_finishes_a_committed_host_only_resolution(
    runtime_snapshot: RuntimeSnapshot,
    valid_journal: PendingJournal,
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    manifest_catalog: ManifestCatalog,
) -> None:
    database = StateDatabase(file_bridge_paths.sqlite_file)
    database.initialize()
    ledger = RequestLedger(database, manifest_catalog)
    quarantined = await _persist_quarantine(
        valid_journal=valid_journal,
        journal_store=journal_store,
        root_lock=root_lock,
        ledger=ledger,
    )
    original = quarantined.original
    lineage_id = original.expected_lineage_id
    activation_id = original.expected_activation_id
    assert lineage_id is not None
    assert activation_id is not None
    marker = HostQuarantineResolutionMarker(
        format=HOST_QUARANTINE_RESOLUTION_FORMAT,
        mode="host_only",
        manual_evidence=True,
        root_key=quarantined.header.root_key,
        required_release_id=quarantined.header.required_release_id,
        request_id=original.request_id,
        request_hash=original.request_hash,
        original_journal_revision=original.revision,
        scenario_lineage_id=lineage_id,
        original_activation_id=activation_id,
        disposition="not_applied",
        resolved_at_ms=200,
    )
    ledger.transition(
        original.request_id,
        expected_states=frozenset({HostRequestState.QUARANTINED}),
        new_state=HostRequestState.RESOLVED,
        updated_at_ms=200,
        terminal_at_ms=200,
        resolution_json=canonical_host_quarantine_resolution(marker),
    )
    inbox = InboxPublisher(file_bridge_paths, 0)
    process = ProcessInfo(
        pid=123,
        create_time=1.0,
        executable=file_bridge_paths.command_exe,
    )
    manager = RecoveryManager(
        paths=file_bridge_paths,
        root_lock=root_lock,
        process_inspector=_Inspector(process),
        expected_process=process,
        journals=journal_store,
        ledger=ledger,
        inbox=inbox,
        response_poll_seconds=0.01,
        cancel_ack_timeout_seconds=1,
    )

    file_bridge_paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    report: RecoveryReport | None = None
    async with root_lock:
        loaded = journal_store.load()
        assert loaded is not None
        report = manager._recover_quarantined(  # pyright: ignore[reportPrivateUsage]
            loaded
        )

    assert report is not None
    assert report.disposition is RecoveryDisposition.SETTLED
    assert report.request_state is HostRequestState.RESOLVED
    assert report.response_cleanup_required is True
    assert not file_bridge_paths.pending_file.exists()
    assert file_bridge_paths.inbox.read_bytes() == render_idle_lua()


def _new_release_catalog() -> ManifestCatalog:
    snapshot = RuntimeSnapshot.create(
        runtime_version="9.9.9",
        runtime_asset_sha256="1" * 64,
        operation_manifest_sha256=OPERATION_REGISTRY.manifest_sha256,
        host_contract_sha256="3" * 64,
        dependency_lock_sha256="4" * 64,
    )
    return ManifestCatalog(ReleaseBinding(snapshot=snapshot, registry=OPERATION_REGISTRY))


def _obsolete_service(
    *,
    file_bridge_paths: FileBridgePaths,
    root_lock: RootLock,
    journals: PendingJournalStore,
    ledger: RequestLedger,
    queue_store: OperationQueueStore,
    confirmations: ConfirmationTokenStore,
    clock: _Clock,
    running_release_id: str | None = None,
) -> ObsoleteQuarantineAbandonmentService:
    file_bridge_paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    return ObsoleteQuarantineAbandonmentService(
        root_key=file_bridge_paths.root_key,
        running_release_id=(
            _new_release_catalog().running_release_id
            if running_release_id is None
            else running_release_id
        ),
        coordination_lock=RootLock(
            file_bridge_paths.lock_file.with_name(
                f"{file_bridge_paths.root_key}.ui.lock"
            ),
            timeout_seconds=0,
        ),
        root_lock=root_lock,
        journals=journals,
        ledger=ledger,
        queue_store=queue_store,
        confirmations=confirmations,
        inbox=InboxPublisher(file_bridge_paths, 0),
        wall_clock=clock,
    )


def _enqueue_matching_active_queue(
    queue_store: OperationQueueStore,
    journal: PendingJournal,
) -> OperationQueueRecord:
    original = journal.original
    body = json.loads(original.body_json)
    enqueued = queue_store.enqueue(
        OperationQueueRecord(
            queue_sequence=0,
            request_id=original.request_id,
            root_key=journal.header.root_key,
            operation=original.operation,
            arguments_json=json.dumps(
                body["arguments"],
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
            body_json=original.body_json.encode(),
            runtime_snapshot=original.runtime_snapshot,
            result_schema_id=original.result_schema_id,
            recovery_schema_id=original.recovery_schema_id,
            expected_lineage_id=original.expected_lineage_id,
            expected_activation_id=original.expected_activation_id,
            expected_process_pid=1,
            expected_process_create_time=1.0,
            state=OperationQueueState.QUEUED,
            result_json=None,
            error_json=None,
            created_at_ms=100,
            updated_at_ms=100,
            terminal_at_ms=None,
        )
    )
    claimed = queue_store.claim_next(root_key=journal.header.root_key, at_ms=101)
    assert claimed is not None
    assert claimed.request_id == enqueued.request_id
    assert claimed.state is OperationQueueState.ACTIVE
    return claimed


@pytest.mark.asyncio
async def test_obsolete_quarantine_preview_confirm_cross_release_without_cmo(
    file_bridge_paths: FileBridgePaths,
    root_lock: RootLock,
    journal_store: PendingJournalStore,
    manifest_catalog: ManifestCatalog,
    valid_journal: PendingJournal,
) -> None:
    database = StateDatabase(file_bridge_paths.sqlite_file)
    database.initialize()
    ledger = RequestLedger(database, manifest_catalog)
    quarantined = await _persist_quarantine(
        valid_journal=valid_journal,
        journal_store=journal_store,
        root_lock=root_lock,
        ledger=ledger,
    )
    cross_release_store = PendingJournalStore(
        file_bridge_paths,
        root_lock,
        _new_release_catalog(),
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
    )
    mismatch_error: BridgeError | None = None
    try:
        async with root_lock:
            cross_release_store.load()
    except BridgeError as error:
        mismatch_error = error
    assert mismatch_error is not None
    assert mismatch_error.code is ErrorCode.MANIFEST_MISMATCH

    queue_store = OperationQueueStore(database)
    active = _enqueue_matching_active_queue(queue_store, quarantined)
    service = _obsolete_service(
        file_bridge_paths=file_bridge_paths,
        root_lock=root_lock,
        journals=cross_release_store,
        ledger=RequestLedger(database, _new_release_catalog()),
        queue_store=queue_store,
        confirmations=ConfirmationTokenStore(database, token_factory=_token_factory),
        clock=_Clock(1_000, 2_000, 3_000, 4_000),
    )
    preview = await service.preview()
    assert preview.phase == "preview"
    assert preview.request_id == quarantined.original.request_id
    assert preview.required_release_id == quarantined.header.required_release_id
    assert preview.confirmation_expires_at_utc == datetime.fromtimestamp(61, tz=UTC)
    assert preview.target_active_queue_recovery_required is True

    resolved = await service.confirm(TOKEN)
    assert resolved.phase == "resolved"
    assert resolved.disposition == "not_applied"
    assert resolved.cmo_contacted is False
    assert resolved.original_operation_replayed is False
    assert resolved.target_active_queue_recovered is True
    assert not file_bridge_paths.pending_file.exists()
    assert file_bridge_paths.inbox.read_bytes() == render_idle_lua()
    record = ledger.get_request(quarantined.original.request_id)
    assert record is not None and record.state is HostRequestState.RESOLVED
    assert record.resolution_json is not None
    marker = HostQuarantineResolutionMarker.model_validate_json(record.resolution_json)
    assert marker.mode == "host_only"
    assert marker.disposition == "not_applied"
    queue_record = queue_store.get(active.request_id)
    assert queue_record is not None
    assert queue_record.state is OperationQueueState.QUARANTINED


@pytest.mark.asyncio
async def test_obsolete_quarantine_rejects_a_journal_from_the_running_release(
    file_bridge_paths: FileBridgePaths,
    root_lock: RootLock,
    journal_store: PendingJournalStore,
    manifest_catalog: ManifestCatalog,
    valid_journal: PendingJournal,
) -> None:
    database = StateDatabase(file_bridge_paths.sqlite_file)
    database.initialize()
    ledger = RequestLedger(database, manifest_catalog)
    await _persist_quarantine(
        valid_journal=valid_journal,
        journal_store=journal_store,
        root_lock=root_lock,
        ledger=ledger,
    )
    service = _obsolete_service(
        file_bridge_paths=file_bridge_paths,
        root_lock=root_lock,
        journals=journal_store,
        ledger=ledger,
        queue_store=OperationQueueStore(database),
        confirmations=ConfirmationTokenStore(database),
        clock=_Clock(),
        running_release_id=manifest_catalog.running_release_id,
    )

    with pytest.raises(BridgeError) as caught:
        await service.preview()

    assert caught.value.code is ErrorCode.POLICY_DENIED
    assert caught.value.details["required_release_id"] == manifest_catalog.running_release_id


@pytest.mark.asyncio
async def test_obsolete_quarantine_finishes_a_previously_committed_resolution(
    file_bridge_paths: FileBridgePaths,
    root_lock: RootLock,
    journal_store: PendingJournalStore,
    manifest_catalog: ManifestCatalog,
    valid_journal: PendingJournal,
) -> None:
    database = StateDatabase(file_bridge_paths.sqlite_file)
    database.initialize()
    ledger = RequestLedger(database, manifest_catalog)
    quarantined = await _persist_quarantine(
        valid_journal=valid_journal,
        journal_store=journal_store,
        root_lock=root_lock,
        ledger=ledger,
    )
    original = quarantined.original
    assert original.expected_lineage_id is not None
    assert original.expected_activation_id is not None
    marker = HostQuarantineResolutionMarker(
        format=HOST_QUARANTINE_RESOLUTION_FORMAT,
        mode="host_only",
        manual_evidence=True,
        root_key=quarantined.header.root_key,
        required_release_id=quarantined.header.required_release_id,
        request_id=original.request_id,
        request_hash=original.request_hash,
        original_journal_revision=original.revision,
        scenario_lineage_id=original.expected_lineage_id,
        original_activation_id=original.expected_activation_id,
        disposition="not_applied",
        resolved_at_ms=103,
    )
    ledger.transition(
        original.request_id,
        expected_states=frozenset({HostRequestState.QUARANTINED}),
        new_state=HostRequestState.RESOLVED,
        updated_at_ms=103,
        terminal_at_ms=103,
        resolution_json=canonical_host_quarantine_resolution(marker),
    )
    cross_release_store = PendingJournalStore(
        file_bridge_paths,
        root_lock,
        _new_release_catalog(),
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
    )
    service = _obsolete_service(
        file_bridge_paths=file_bridge_paths,
        root_lock=root_lock,
        journals=cross_release_store,
        ledger=RequestLedger(database, _new_release_catalog()),
        queue_store=OperationQueueStore(database),
        confirmations=ConfirmationTokenStore(database),
        clock=_Clock(),
    )

    recovered = await service.preview()

    assert recovered.phase == "resolved"
    assert recovered.resolved_at_utc == datetime.fromtimestamp(0.103, tz=UTC)
    assert not file_bridge_paths.pending_file.exists()
    assert file_bridge_paths.inbox.read_bytes() == render_idle_lua()


@pytest.mark.asyncio
async def test_obsolete_quarantine_rejects_ordinary_resolution_token(
    file_bridge_paths: FileBridgePaths,
    root_lock: RootLock,
    journal_store: PendingJournalStore,
    manifest_catalog: ManifestCatalog,
    valid_journal: PendingJournal,
) -> None:
    database = StateDatabase(file_bridge_paths.sqlite_file)
    database.initialize()
    ledger = RequestLedger(database, manifest_catalog)
    quarantined = await _persist_quarantine(
        valid_journal=valid_journal,
        journal_store=journal_store,
        root_lock=root_lock,
        ledger=ledger,
    )
    original = quarantined.original
    assert original.expected_lineage_id is not None
    assert original.expected_activation_id is not None
    confirmations = ConfirmationTokenStore(database, token_factory=_token_factory)
    ordinary = HostQuarantineConfirmationDescriptor(
        format=HOST_QUARANTINE_CONFIRMATION_FORMAT,
        root_key=quarantined.header.root_key,
        required_release_id=quarantined.header.required_release_id,
        request_id=original.request_id,
        request_hash=original.request_hash,
        original_journal_revision=original.revision,
        scenario_lineage_id=original.expected_lineage_id,
        original_activation_id=original.expected_activation_id,
        disposition="not_applied",
    )
    confirmations.issue(host_quarantine_confirmation_binding(ordinary), now_ms=1_000)
    service = _obsolete_service(
        file_bridge_paths=file_bridge_paths,
        root_lock=root_lock,
        journals=journal_store,
        ledger=ledger,
        queue_store=OperationQueueStore(database),
        confirmations=confirmations,
        clock=_Clock(2_000),
    )

    with pytest.raises(BridgeError) as caught:
        await service.confirm(TOKEN)
    assert caught.value.code is ErrorCode.POLICY_DENIED
    assert file_bridge_paths.pending_file.exists()
    assert ledger.get_request(original.request_id).state is HostRequestState.QUARANTINED  # type: ignore[union-attr]


@pytest.mark.asyncio
@pytest.mark.parametrize("queue_state", [OperationQueueState.QUEUED, OperationQueueState.ACTIVE])
async def test_obsolete_quarantine_blocks_queued_or_active_work(
    queue_state: OperationQueueState,
    file_bridge_paths: FileBridgePaths,
    root_lock: RootLock,
    journal_store: PendingJournalStore,
    manifest_catalog: ManifestCatalog,
    valid_journal: PendingJournal,
) -> None:
    database = StateDatabase(file_bridge_paths.sqlite_file)
    database.initialize()
    ledger = RequestLedger(database, manifest_catalog)
    await _persist_quarantine(
        valid_journal=valid_journal,
        journal_store=journal_store,
        root_lock=root_lock,
        ledger=ledger,
    )
    original = valid_journal.original
    body = json.loads(original.body_json)
    arguments_json = json.dumps(
        body["arguments"], sort_keys=True, separators=(",", ":")
    ).encode()
    queue_store = OperationQueueStore(database)
    queue_store.enqueue(
        OperationQueueRecord(
            queue_sequence=0,
            request_id=UUID("77777777-7777-4777-8777-777777777777"),
            root_key=file_bridge_paths.root_key,
            operation=original.operation,
            arguments_json=arguments_json,
            body_json=original.body_json.encode(),
            runtime_snapshot=original.runtime_snapshot,
            result_schema_id=original.result_schema_id,
            recovery_schema_id=original.recovery_schema_id,
            expected_lineage_id=original.expected_lineage_id,
            expected_activation_id=original.expected_activation_id,
            expected_process_pid=1,
            expected_process_create_time=1.0,
            state=OperationQueueState.QUEUED,
            result_json=None,
            error_json=None,
            created_at_ms=100,
            updated_at_ms=100,
            terminal_at_ms=None,
        )
    )
    if queue_state is OperationQueueState.ACTIVE:
        claimed = queue_store.claim_next(
            root_key=file_bridge_paths.root_key,
            at_ms=101,
        )
        assert claimed is not None and claimed.state is OperationQueueState.ACTIVE
    service = _obsolete_service(
        file_bridge_paths=file_bridge_paths,
        root_lock=root_lock,
        journals=journal_store,
        ledger=ledger,
        queue_store=queue_store,
        confirmations=ConfirmationTokenStore(database),
        clock=_Clock(1_000),
    )

    with pytest.raises(BridgeError) as caught:
        await service.preview()
    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.details["states"] == [queue_state.value]
