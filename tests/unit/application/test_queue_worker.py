from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncGenerator, Callable
from uuid import UUID, uuid4

import pytest
from pydantic import JsonValue

from cmo_agent_bridge.application.queue_models import (
    QueueError,
    QueuedOperationRecord,
    QueuedOperationState,
    canonical_queue_json,
)
from cmo_agent_bridge.application.queue_service import QueueService
from cmo_agent_bridge.application.queue_worker import (  # pyright: ignore[reportPrivateUsage]
    QueueWorker,
    _queue_error_json,  # pyright: ignore[reportPrivateUsage]
)
from cmo_agent_bridge.errors import ErrorCode
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.canonical import request_sha256
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.models import ExchangeCommand, RequestBody
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CompletedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.models import HostRequestState
from cmo_agent_bridge.state.operation_queue import OperationQueueStore
from cmo_agent_bridge.state.request_ledger import RequestLedger, RequestRecord
from cmo_agent_bridge.state.session_store import SessionRecord, SessionStore
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.models import RecoveryDisposition, RecoveryReport
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo


ROOT_KEY = "a" * 64
LINEAGE_ID = UUID("11111111-1111-4111-8111-111111111111")
ACTIVATION_ID = UUID("22222222-2222-4222-8222-222222222222")


class _Clock:
    def __init__(self) -> None:
        self._value = 1_000

    def now_ms(self) -> int:
        self._value += 1
        return self._value


class _ProcessInspector:
    def __init__(self, process: ProcessInfo) -> None:
        self._process = process

    def matching_processes(self, _command_exe: Path) -> tuple[ProcessInfo, ...]:
        return (self._process,)


@dataclass(frozen=True)
class _Paths:
    command_exe: Path


class _Channel:
    def __init__(
        self,
        *,
        report: RecoveryReport,
        responder: Callable[[ExchangeCommand], ResponseArtifact],
    ) -> None:
        self._report = report
        self._responder = responder
        self.commands: list[ExchangeCommand] = []

    @property
    def recovery_report(self) -> RecoveryReport:
        return self._report

    @property
    def process_identity(self) -> ProcessInfo:
        return ProcessInfo(
            pid=42,
            create_time=100.5,
            executable=Path(r"C:\CMO\Command.exe"),
        )

    def set_report(self, report: RecoveryReport) -> None:
        self._report = report

    async def exchange(self, command: ExchangeCommand) -> ResponseArtifact:
        self.commands.append(command)
        return self._responder(command)

    async def recover_pending(self) -> RecoveryReport:
        return self._report


class _BlockingChannel(_Channel):
    def __init__(self, *, report: RecoveryReport) -> None:
        super().__init__(report=report, responder=lambda _command: _unexpected_artifact())
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def exchange(self, command: ExchangeCommand) -> ResponseArtifact:
        self.commands.append(command)
        self.started.set()
        await self.release.wait()
        return _unexpected_artifact()


class _Transport:
    def __init__(self, *, channel: _Channel, process: ProcessInfo) -> None:
        self.root_key = ROOT_KEY
        self.process = process
        self.process_inspector = _ProcessInspector(process)
        self.paths = _Paths(command_exe=process.executable)
        self.channel = channel
        self.session_count = 0

    @asynccontextmanager
    async def worker_session(
        self,
        *,
        recovery_owner: Callable[[ProcessInfo], UUID | None],
    ) -> AsyncGenerator[_Channel, None]:
        recovery_owner(self.process)
        self.session_count += 1
        yield self.channel


class _DelayedOwnerTransport(_Transport):
    def __init__(self, *, channel: _Channel, process: ProcessInfo) -> None:
        super().__init__(channel=channel, process=process)
        self.owner_resolution_started = asyncio.Event()
        self.resolve_owner = asyncio.Event()
        self.resolved_owner: UUID | None = None

    @asynccontextmanager
    async def worker_session(
        self,
        *,
        recovery_owner: Callable[[ProcessInfo], UUID | None],
    ) -> AsyncGenerator[_Channel, None]:
        self.owner_resolution_started.set()
        await self.resolve_owner.wait()
        self.resolved_owner = recovery_owner(self.process)
        self.session_count += 1
        yield self.channel


@dataclass(frozen=True)
class _Rig:
    clock: _Clock
    queue: OperationQueueStore
    sessions: SessionStore
    ledger: RequestLedger
    service: QueueService
    worker: QueueWorker
    channel: _Channel
    transport: _Transport
    snapshot: RuntimeSnapshot
    process: ProcessInfo


def _snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.2.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=OPERATION_REGISTRY.manifest_sha256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )


def _no_pending() -> RecoveryReport:
    return RecoveryReport(
        disposition=RecoveryDisposition.NO_PENDING,
        request_id=None,
        request_state=None,
        response_cleanup_required=False,
    )


def _artifact(command: ExchangeCommand) -> ResponseArtifact:
    code = command.body.arguments["code"]
    if type(code) is not int:
        raise AssertionError("time compression code was not an exact integer")
    result = _artifact_result(code)
    lineage_id = command.body.expected_lineage_id
    activation_id = command.body.expected_activation_id
    if lineage_id is None or activation_id is None:
        raise AssertionError("queued mutation lost its scenario binding")
    envelope = ResponseEnvelope(
        protocol=command.body.protocol,
        request_id=command.request_id,
        delivery_id=uuid4(),
        request_hash=request_sha256(command.body),
        ok=True,
        result=result,
        error=None,
        scenario_time="2026-07-18T00:00:00Z",
        scenario_lineage_id=lineage_id,
        activation_id=activation_id,
        operation_manifest_sha256=command.runtime_snapshot.operation_manifest_sha256,
        bridge_version=command.runtime_snapshot.runtime_version,
        runtime_tag=command.runtime_snapshot.runtime_tag,
        runtime_asset_sha256=command.runtime_snapshot.runtime_asset_sha256,
        release_id=command.runtime_snapshot.release_id,
    )
    accepted = AcceptedResponse(
        envelope=envelope,
        delivery_kind="request",
        settlement=CompletedSettlement(state="completed", result=result),
        cancel_ack=None,
    )
    return ResponseArtifact(
        filename=f"CMOAgentBridge_Response_{command.request_id}.inst",
        sha256=hashlib.sha256(str(command.request_id).encode("ascii")).hexdigest(),
        size_bytes=256,
        accepted_at_ms=2_000,
        accepted_response=accepted,
    )


def _artifact_result(code: int) -> dict[str, JsonValue]:
    return {"accepted": True, "code": code, "observed_time_compression": float(code)}


def _unexpected_artifact() -> ResponseArtifact:
    raise AssertionError("worker must not exchange this queue entry")


@pytest.mark.parametrize("error_code", tuple(ErrorCode))
def test_worker_preserves_every_ledger_error_code(error_code: ErrorCode) -> None:
    ledger_error = canonical_queue_json(
        {
            "code": error_code.value,
            "message": "ledger error",
            "details": {"request_phase": "response_accepted"},
        }
    ).decode("utf-8")

    projected = QueueError.model_validate_json(
        _queue_error_json(
            ledger_error,
            fallback=ErrorCode.PROTOCOL_ERROR,
            message="fallback",
        )
    )

    assert projected.code is error_code
    assert projected.message == "ledger error"
    assert projected.details == {"request_phase": "response_accepted"}


def _rig(
    tmp_path: Path,
    *,
    report: RecoveryReport | None = None,
    channel: _Channel | None = None,
    running_process: ProcessInfo | None = None,
) -> _Rig:
    snapshot = _snapshot()
    process = ProcessInfo(pid=42, create_time=100.5, executable=Path(r"C:\CMO\Command.exe"))
    database = StateDatabase(tmp_path / "state.sqlite3")
    sessions = SessionStore(database)
    sessions.replace(
        SessionRecord(
            root_key=ROOT_KEY,
            scenario_lineage_id=LINEAGE_ID,
            activation_id=ACTIVATION_ID,
            build_number=1868,
            runtime_snapshot=snapshot,
            process_pid=process.pid,
            process_create_time=process.create_time,
            validated_at_ms=1,
        )
    )
    queue = OperationQueueStore(database)
    clock = _Clock()
    ledger = RequestLedger(
        database, ManifestCatalog(ReleaseBinding(snapshot=snapshot, registry=OPERATION_REGISTRY))
    )
    test_channel = channel or _Channel(report=report or _no_pending(), responder=_artifact)
    transport = _Transport(channel=test_channel, process=running_process or process)
    service = QueueService(
        root_key=ROOT_KEY,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=snapshot,
        allow_mutations=True,
        session_store=sessions,
        queue_store=queue,
        ledger=ledger,
        clock=clock,
    )
    worker = QueueWorker(
        root_key=ROOT_KEY,
        registry=OPERATION_REGISTRY,
        queue_store=queue,
        transport=transport,
        ledger=ledger,
        clock=clock,
        idle_poll_seconds=0.01,
    )
    return _Rig(
        clock=clock,
        queue=queue,
        sessions=sessions,
        ledger=ledger,
        service=service,
        worker=worker,
        channel=test_channel,
        transport=transport,
        snapshot=snapshot,
        process=process,
    )


def _submit(rig: _Rig, code: int) -> QueuedOperationRecord:
    receipt = rig.service.submit(
        operation="scenario.time_compression.set", arguments={"code": code}
    )
    record = rig.queue.get(receipt.request_id)
    assert record is not None
    return record


def _claim_next(rig: _Rig) -> QueuedOperationRecord:
    record = rig.queue.claim_next(root_key=ROOT_KEY, at_ms=rig.clock.now_ms())
    assert record is not None
    return record


def _insert_prepared_ledger(rig: _Rig, record: QueuedOperationRecord) -> None:
    body = RequestBody.model_validate_json(record.body_json)
    invocation = OPERATION_REGISTRY.resolve_invocation(
        record.operation, json.loads(record.arguments_json)
    )
    rig.ledger.insert_prepared(
        RequestRecord(
            request_id=record.request_id,
            root_key=ROOT_KEY,
            request_hash=request_sha256(body),
            operation=record.operation,
            operation_class=invocation.effective_class,
            state=HostRequestState.PREPARED,
            runtime_snapshot=record.runtime_snapshot,
            result_schema_id=record.result_schema_id,
            recovery_schema_id=record.recovery_schema_id,
            body_json=record.body_json,
            lineage_id=record.expected_lineage_id,
            activation_id=record.expected_activation_id,
            result_json=None,
            error_json=None,
            resolution_json=None,
            created_at_ms=1_100,
            updated_at_ms=1_100,
            terminal_at_ms=None,
        )
    )


def _settle_ledger_completed(rig: _Rig, record: QueuedOperationRecord) -> None:
    _insert_prepared_ledger(rig, record)
    result = {"accepted": True, "code": 3, "observed_time_compression": 3.0}
    rig.ledger.transition(
        record.request_id,
        expected_states=frozenset({HostRequestState.PREPARED}),
        new_state=HostRequestState.COMPLETED,
        updated_at_ms=1_200,
        terminal_at_ms=1_200,
        result_json=canonical_queue_json(result).decode("utf-8"),
    )


@pytest.mark.asyncio
async def test_worker_claims_and_completes_fifo_orders_with_unbounded_exchange_timeout(
    tmp_path: Path,
) -> None:
    rig = _rig(tmp_path)
    first = _submit(rig, 3)
    second = _submit(rig, 4)

    assert await rig.worker.run_once() is True
    assert await rig.worker.run_once() is True

    assert [command.request_id for command in rig.channel.commands] == [
        first.request_id,
        second.request_id,
    ]
    assert all(
        command.timeout == 0.0 and command.unbounded_wait for command in rig.channel.commands
    )
    assert rig.queue.get(first.request_id).state is QueuedOperationState.COMPLETED  # type: ignore[union-attr]
    assert rig.queue.get(second.request_id).state is QueuedOperationState.COMPLETED  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_worker_cancellation_during_published_wait_leaves_order_active(
    tmp_path: Path,
) -> None:
    channel = _BlockingChannel(report=_no_pending())
    rig = _rig(tmp_path, channel=channel)
    record = _submit(rig, 3)

    rig.worker.start()
    await asyncio.wait_for(channel.started.wait(), timeout=1)
    await rig.worker.stop()

    stored = rig.queue.get(record.request_id)
    assert stored is not None
    assert stored.state is QueuedOperationState.ACTIVE
    assert channel.commands[0].timeout == 0.0
    assert channel.commands[0].unbounded_wait is True


@pytest.mark.asyncio
async def test_startup_settles_active_from_completed_ledger_without_second_exchange(
    tmp_path: Path,
) -> None:
    rig = _rig(tmp_path)
    record = _submit(rig, 3)
    rig.channel.set_report(
        RecoveryReport(
            disposition=RecoveryDisposition.SETTLED,
            request_id=record.request_id,
            request_state=HostRequestState.COMPLETED,
            response_cleanup_required=True,
        )
    )
    assert _claim_next(rig).request_id == record.request_id
    _settle_ledger_completed(rig, record)

    assert await rig.worker.run_once() is False

    stored = rig.queue.get(record.request_id)
    assert stored is not None
    assert stored.state is QueuedOperationState.COMPLETED
    assert stored.result_json == canonical_queue_json(
        {"accepted": True, "code": 3, "observed_time_compression": 3.0}
    )
    assert rig.channel.commands == []


@pytest.mark.asyncio
async def test_active_without_journal_or_ledger_is_reset_and_executed_in_same_worker_session(
    tmp_path: Path,
) -> None:
    rig = _rig(tmp_path)
    record = _submit(rig, 3)
    assert _claim_next(rig).request_id == record.request_id

    assert await rig.worker.run_once() is True

    stored = rig.queue.get(record.request_id)
    assert stored is not None
    assert stored.state is QueuedOperationState.COMPLETED
    assert [command.request_id for command in rig.channel.commands] == [record.request_id]
    assert rig.transport.session_count == 1


@pytest.mark.asyncio
async def test_recovery_owner_is_resolved_after_waiting_for_worker_session(
    tmp_path: Path,
) -> None:
    process = ProcessInfo(pid=42, create_time=100.5, executable=Path(r"C:\CMO\Command.exe"))
    channel = _Channel(report=_no_pending(), responder=_artifact)
    transport = _DelayedOwnerTransport(channel=channel, process=process)
    rig = _rig(tmp_path, channel=channel)
    worker = QueueWorker(
        root_key=ROOT_KEY,
        registry=OPERATION_REGISTRY,
        queue_store=rig.queue,
        transport=transport,
        ledger=rig.ledger,
        clock=rig.clock,
        idle_poll_seconds=0.01,
    )
    first = _submit(rig, 3)
    second = _submit(rig, 4)
    assert _claim_next(rig).request_id == first.request_id

    running = asyncio.create_task(worker.run_once())
    await asyncio.wait_for(transport.owner_resolution_started.wait(), timeout=1)
    rig.queue.complete(
        first.request_id, canonical_queue_json(_artifact_result(3)), at_ms=rig.clock.now_ms()
    )
    assert _claim_next(rig).request_id == second.request_id
    transport.resolve_owner.set()

    assert await asyncio.wait_for(running, timeout=1) is True
    assert transport.resolved_owner == second.request_id
    assert [command.request_id for command in channel.commands] == [second.request_id]


@pytest.mark.asyncio
async def test_quarantined_recovery_blocks_following_fifo_order(tmp_path: Path) -> None:
    rig = _rig(tmp_path)
    first = _submit(rig, 3)
    second = _submit(rig, 4)
    rig.channel.set_report(
        RecoveryReport(
            disposition=RecoveryDisposition.QUARANTINED,
            request_id=first.request_id,
            request_state=HostRequestState.QUARANTINED,
            response_cleanup_required=False,
        )
    )
    assert _claim_next(rig).request_id == first.request_id
    _insert_prepared_ledger(rig, first)

    assert await rig.worker.run_once() is False

    first_stored = rig.queue.get(first.request_id)
    second_stored = rig.queue.get(second.request_id)
    assert first_stored is not None and first_stored.state is QueuedOperationState.QUARANTINED
    assert second_stored is not None and second_stored.state is QueuedOperationState.QUEUED
    assert rig.channel.commands == []


@pytest.mark.asyncio
async def test_process_mismatch_quarantines_active_and_blocks_later_queue_without_touching_ledger(
    tmp_path: Path,
) -> None:
    expected = ProcessInfo(pid=99, create_time=999.0, executable=Path(r"C:\CMO\Command.exe"))
    rig = _rig(tmp_path, running_process=expected)
    active = _submit(rig, 3)
    queued = _submit(rig, 4)
    assert _claim_next(rig).request_id == active.request_id
    _insert_prepared_ledger(rig, active)
    before = rig.ledger.get_request(active.request_id)
    assert before is not None

    assert await rig.worker.run_once() is False

    active_stored = rig.queue.get(active.request_id)
    queued_stored = rig.queue.get(queued.request_id)
    assert active_stored is not None and active_stored.state is QueuedOperationState.QUARANTINED
    assert queued_stored is not None and queued_stored.state is QueuedOperationState.QUEUED
    assert rig.ledger.get_request(active.request_id) == before
    assert rig.channel.commands == []
    assert await rig.worker.run_once() is False
    assert rig.queue.get(queued.request_id).state is QueuedOperationState.QUEUED  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_process_mismatch_rejects_unpublished_fifo_head(tmp_path: Path) -> None:
    replacement = ProcessInfo(
        pid=99,
        create_time=999.0,
        executable=Path(r"C:\CMO\Command.exe"),
    )
    rig = _rig(tmp_path, running_process=replacement)
    record = _submit(rig, 3)

    assert await rig.worker.run_once() is False

    stored = rig.queue.get(record.request_id)
    assert stored is not None
    assert stored.state is QueuedOperationState.REJECTED
    assert stored.error_json is not None
    assert rig.channel.commands == []
