from __future__ import annotations

import asyncio
import hashlib
import inspect
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any, cast, get_type_hints
from uuid import UUID

import pytest
from pydantic import ValidationError

from cmo_agent_bridge.application import (
    PreparedReadAttempt,
    SessionActivation,
    SessionScope,
    SessionService,
)
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.models import BridgeStatusResult, BridgeStatusWireArgs
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.canonical import request_sha256
from cmo_agent_bridge.protocol.models import ExchangeCommand
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CompletedSettlement,
    RejectedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
    ResponseError,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, Sha256
from cmo_agent_bridge.state.session_store import SessionRecord, SessionStore
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.models import (
    RecoveryDisposition,
    RecoveryReport,
)
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo


ROOT_KEY = "a" * 64
LINEAGE_1 = UUID("33333333-3333-4333-8333-333333333333")
LINEAGE_2 = UUID("66666666-6666-4666-8666-666666666666")
LINEAGE_3 = UUID("77777777-7777-4777-8777-777777777777")
ACTIVATION_OLD = UUID("44444444-4444-4444-8444-444444444444")
CANDIDATE_1 = UUID("55555555-5555-4555-8555-555555555555")
CANDIDATE_2 = UUID("88888888-8888-4888-8888-888888888888")
CANDIDATE_3 = UUID("99999999-9999-4999-8999-999999999999")
CANDIDATE_4 = UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
ACTIVATION_WINNER = UUID("12121212-1212-4212-8212-121212121212")
REQUEST_1 = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
REQUEST_2 = UUID("bbbbbbbb-cccc-4ddd-8eee-ffffffffffff")
REQUEST_3 = UUID("cccccccc-dddd-4eee-8fff-aaaaaaaaaaaa")
REQUEST_4 = UUID("dddddddd-eeee-4fff-8aaa-bbbbbbbbbbbb")
DELIVERY_1 = UUID("11111111-1111-4111-8111-111111111111")
DELIVERY_2 = UUID("22222222-2222-4222-8222-222222222222")


class _UuidSequence:
    def __init__(self, *values: UUID) -> None:
        self._values = list(values)
        self.calls = 0

    def __call__(self) -> UUID:
        self.calls += 1
        if not self._values:
            raise AssertionError("unexpected UUID allocation")
        return self._values.pop(0)


class _WallClock:
    def __init__(self, *epochs: int) -> None:
        self._epochs = list(epochs)

    def now_ms(self) -> int:
        if not self._epochs:
            raise AssertionError("unexpected wall-clock read")
        return self._epochs.pop(0)


class _RaisingWallClock:
    def __init__(self, error: BaseException) -> None:
        self._error = error

    def now_ms(self) -> int:
        raise self._error


class _Channel:
    def __init__(
        self,
        process_identity: ProcessInfo,
        responder: Callable[[ExchangeCommand], ResponseArtifact],
    ) -> None:
        self._process_identity = process_identity
        self._responder = responder
        self.commands: list[ExchangeCommand] = []

    @property
    def process_identity(self) -> ProcessInfo:
        return self._process_identity

    async def exchange(self, command: ExchangeCommand) -> ResponseArtifact:
        self.commands.append(command)
        return self._responder(command)

    async def recover_pending(self) -> RecoveryReport:
        return RecoveryReport(
            disposition=RecoveryDisposition.NO_PENDING,
            request_id=None,
            request_state=None,
            response_cleanup_required=False,
        )


class _ResponderSequence:
    def __init__(
        self,
        *responders: Callable[[ExchangeCommand], ResponseArtifact],
    ) -> None:
        self._responders = list(responders)

    def __call__(self, command: ExchangeCommand) -> ResponseArtifact:
        if not self._responders:
            raise AssertionError("unexpected status exchange")
        return self._responders.pop(0)(command)


class _BlockingChannel:
    def __init__(
        self,
        process_identity: ProcessInfo,
        *,
        block_on_call: int,
        responders: tuple[Callable[[ExchangeCommand], ResponseArtifact], ...] = (),
    ) -> None:
        self._process_identity = process_identity
        self._block_on_call = block_on_call
        self._responders = list(responders)
        self._started = asyncio.Event()
        self._never_release = asyncio.Event()
        self.commands: list[ExchangeCommand] = []

    @property
    def process_identity(self) -> ProcessInfo:
        return self._process_identity

    @property
    def started(self) -> asyncio.Event:
        return self._started

    async def exchange(self, command: ExchangeCommand) -> ResponseArtifact:
        self.commands.append(command)
        if len(self.commands) == self._block_on_call:
            self._started.set()
            await self._never_release.wait()
            raise AssertionError("blocked status exchange was unexpectedly released")
        if not self._responders:
            raise AssertionError("unexpected status exchange")
        return self._responders.pop(0)(command)

    async def recover_pending(self) -> RecoveryReport:
        return RecoveryReport(
            disposition=RecoveryDisposition.NO_PENDING,
            request_id=None,
            request_state=None,
            response_cleanup_required=False,
        )


@pytest.fixture
def running_snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=OPERATION_REGISTRY.manifest_sha256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )


@pytest.fixture
def stale_snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="e" * 64,
        operation_manifest_sha256=OPERATION_REGISTRY.manifest_sha256,
        host_contract_sha256="f" * 64,
        dependency_lock_sha256="1" * 64,
    )


@pytest.fixture
def session_store(tmp_path: Path) -> SessionStore:
    return SessionStore(StateDatabase(tmp_path / "state.sqlite3"))


def _scope(command_exe: Path) -> SessionScope:
    return SessionScope(root_key=ROOT_KEY, command_exe=command_exe)


def _status_result(
    snapshot: RuntimeSnapshot,
    *,
    lineage_id: UUID,
    activation_id: UUID,
    build: int = 1868,
) -> BridgeStatusResult:
    return BridgeStatusResult(
        protocol=snapshot.protocol,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
        build=build,
        manifest_sha256=snapshot.operation_manifest_sha256,
        lineage_id=lineage_id,
        activation_id=activation_id,
        installed_event_names=["CMOAgentBridge: Initialize", "CMOAgentBridge: Poll"],
        installed_action_names=["CMOAgentBridge: Initialize", "CMOAgentBridge: Poll"],
        installed_trigger_names=["CMOAgentBridge: Loaded", "CMOAgentBridge: Timer"],
        pending_request_id=None,
        quarantined=False,
        paused_capability=True,
        poll_interval_seconds=5,
        safe_payload_bytes=65_536,
        verified_ledger_entries=128,
        effective_ledger_capacity=128,
    )


def _successful_status_artifact(
    command: ExchangeCommand,
    snapshot: RuntimeSnapshot,
    *,
    lineage_id: UUID = LINEAGE_1,
    build: int = 1868,
    delivery_id: UUID = DELIVERY_1,
) -> ResponseArtifact:
    wire = cast(BridgeStatusWireArgs, command.invocation.wire_arguments)
    result = _status_result(
        snapshot,
        lineage_id=lineage_id,
        activation_id=wire.activation_candidate,
        build=build,
    )
    result_json = result.model_dump(mode="json")
    envelope = ResponseEnvelope(
        protocol=snapshot.protocol,
        request_id=command.request_id,
        delivery_id=delivery_id,
        request_hash=request_sha256(command.body),
        ok=True,
        result=result_json,
        error=None,
        scenario_time="2026-07-12T08:00:00Z",
        scenario_lineage_id=lineage_id,
        activation_id=wire.activation_candidate,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        bridge_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
    )
    accepted = AcceptedResponse(
        envelope=envelope,
        delivery_kind="request",
        settlement=CompletedSettlement(state="completed", result=result_json),
        cancel_ack=None,
    )
    return ResponseArtifact(
        filename=f"CMOAgentBridge_Response_{command.request_id}.inst",
        sha256=hashlib.sha256(str(command.request_id).encode("ascii")).hexdigest(),
        size_bytes=512,
        accepted_at_ms=1_752_400_000_000,
        accepted_response=accepted,
    )


def _failed_status_artifact(
    command: ExchangeCommand,
    snapshot: RuntimeSnapshot,
    *,
    code: ErrorCode,
    lineage_id: UUID,
    delivery_id: UUID = DELIVERY_1,
) -> ResponseArtifact:
    wire = cast(BridgeStatusWireArgs, command.invocation.wire_arguments)
    response_error = ResponseError(
        code=code,
        message=f"status failed with {code.value}",
        details={"reported_by": "fake-peer"},
        mutation_not_started=None,
    )
    envelope = ResponseEnvelope(
        protocol=snapshot.protocol,
        request_id=command.request_id,
        delivery_id=delivery_id,
        request_hash=request_sha256(command.body),
        ok=False,
        result=None,
        error=response_error,
        scenario_time="2026-07-12T08:00:00Z",
        scenario_lineage_id=lineage_id,
        activation_id=wire.activation_candidate,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        bridge_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
    )
    accepted = AcceptedResponse(
        envelope=envelope,
        delivery_kind="request",
        settlement=RejectedSettlement(state="rejected", error=response_error),
        cancel_ack=None,
    )
    return ResponseArtifact(
        filename=f"CMOAgentBridge_Response_{command.request_id}.inst",
        sha256=hashlib.sha256(f"{command.request_id}:{code.value}".encode("ascii")).hexdigest(),
        size_bytes=384,
        accepted_at_ms=1_752_400_000_000,
        accepted_response=accepted,
    )


def _failed_operation_artifact(
    command: ExchangeCommand,
    snapshot: RuntimeSnapshot,
    *,
    code: ErrorCode,
    delivery_id: UUID = DELIVERY_2,
) -> ResponseArtifact:
    lineage_id = command.body.expected_lineage_id
    activation_id = command.body.expected_activation_id
    if lineage_id is None or activation_id is None:
        raise AssertionError("operation error helper requires contextual command")
    response_error = ResponseError(
        code=code,
        message=f"operation failed with {code.value}",
        details={"reported_by": "fake-peer"},
        mutation_not_started=None,
    )
    envelope = ResponseEnvelope(
        protocol=snapshot.protocol,
        request_id=command.request_id,
        delivery_id=delivery_id,
        request_hash=request_sha256(command.body),
        ok=False,
        result=None,
        error=response_error,
        scenario_time="2026-07-12T08:00:01Z",
        scenario_lineage_id=lineage_id,
        activation_id=activation_id,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        bridge_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
    )
    accepted = AcceptedResponse(
        envelope=envelope,
        delivery_kind="request",
        settlement=RejectedSettlement(state="rejected", error=response_error),
        cancel_ack=None,
    )
    return ResponseArtifact(
        filename=f"CMOAgentBridge_Response_{command.request_id}.inst",
        sha256=hashlib.sha256(f"{command.request_id}:{code.value}".encode("ascii")).hexdigest(),
        size_bytes=384,
        accepted_at_ms=1_752_400_001_000,
        accepted_response=accepted,
    )


def _stored_session(
    session_store: SessionStore,
    snapshot: RuntimeSnapshot,
    process: ProcessInfo,
) -> SessionRecord:
    record = SessionRecord(
        root_key=ROOT_KEY,
        scenario_lineage_id=LINEAGE_1,
        activation_id=ACTIVATION_OLD,
        build_number=1868,
        runtime_snapshot=snapshot,
        process_pid=process.pid,
        process_create_time=process.create_time,
        validated_at_ms=100,
    )
    session_store.replace(record)
    return record


def _forbid_session_clear(
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_clear(
        _root_key: str,
        *,
        expected_activation_id: UUID | None = None,
    ) -> bool:
        del expected_activation_id
        pytest.fail("durable session guard must not be cleared")

    monkeypatch.setattr(session_store, "clear", forbidden_clear)


@pytest.mark.asyncio
async def test_bootstrap_handshake_builds_null_context_and_persists_exact_session(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original_conditional_replace = session_store.conditional_replace
    replace_calls: list[tuple[SessionRecord, UUID | None]] = []

    def traced_conditional_replace(
        candidate: SessionRecord,
        *,
        expected_activation_id: UUID | None,
    ) -> bool:
        assert session_store.load(ROOT_KEY) is None
        replace_calls.append((candidate, expected_activation_id))
        return original_conditional_replace(
            candidate,
            expected_activation_id=expected_activation_id,
        )

    monkeypatch.setattr(
        session_store,
        "conditional_replace",
        traced_conditional_replace,
    )
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(1_752_400_000_123),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1),
        status_timeout_seconds=0.5,
    )

    activation = await service.handshake(channel)

    assert type(activation) is SessionActivation
    assert activation.status_request_id == REQUEST_1
    assert activation.status.activation_id == CANDIDATE_1
    assert type(activation.response_artifact) is ResponseArtifact
    assert activation.response_artifact.accepted_response.envelope.request_id == REQUEST_1
    assert len(channel.commands) == 1
    command = channel.commands[0]
    assert command.request_id == REQUEST_1
    assert command.timeout == 0.5
    assert command.body.expected_lineage_id is None
    assert command.body.expected_activation_id is None
    assert command.runtime_snapshot == running_snapshot
    assert command.invocation.contract.name == "bridge.status"
    wire = cast(BridgeStatusWireArgs, command.invocation.wire_arguments)
    assert wire.accept_lineage_id is None
    assert wire.activation_candidate == CANDIDATE_1
    stored = SessionRecord(
        root_key=ROOT_KEY,
        scenario_lineage_id=LINEAGE_1,
        activation_id=CANDIDATE_1,
        build_number=1868,
        runtime_snapshot=running_snapshot,
        process_pid=process.pid,
        process_create_time=process.create_time,
        validated_at_ms=1_752_400_000_123,
    )
    assert session_store.load(ROOT_KEY) == stored
    assert replace_calls == [(stored, None)]


@pytest.mark.asyncio
async def test_bootstrap_conditional_insert_cas_loss_preserves_winner(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    winner = SessionRecord(
        root_key=ROOT_KEY,
        scenario_lineage_id=LINEAGE_1,
        activation_id=ACTIVATION_WINNER,
        build_number=1868,
        runtime_snapshot=running_snapshot,
        process_pid=process.pid,
        process_create_time=process.create_time,
        validated_at_ms=199,
    )
    expected_activations: list[UUID | None] = []

    def lose_conditional_insert(
        _candidate: SessionRecord,
        *,
        expected_activation_id: UUID | None,
    ) -> bool:
        expected_activations.append(expected_activation_id)
        session_store.replace(winner)
        return False

    monkeypatch.setattr(
        session_store,
        "conditional_replace",
        lose_conditional_insert,
    )
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert expected_activations == [None]
    assert len(channel.commands) == 1
    assert session_store.load(ROOT_KEY) == winner


@pytest.mark.asyncio
async def test_continuing_session_uses_stored_context_without_clearing_snapshot_drift(
    running_snapshot: RuntimeSnapshot,
    stale_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope_exe = Path(r"D:\Games\CMO\Command.exe")
    equivalent_process_exe = Path(r"\\?\d:\games\cmo\COMMAND.EXE")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=equivalent_process_exe)
    original = SessionRecord(
        root_key=ROOT_KEY,
        scenario_lineage_id=LINEAGE_1,
        activation_id=ACTIVATION_OLD,
        build_number=1867,
        runtime_snapshot=stale_snapshot,
        process_pid=process.pid,
        process_create_time=process.create_time,
        validated_at_ms=100,
    )
    session_store.replace(original)

    _forbid_session_clear(session_store, monkeypatch)
    original_conditional_replace = session_store.conditional_replace
    replace_calls: list[tuple[SessionRecord, UUID | None]] = []

    def traced_conditional_replace(
        candidate: SessionRecord,
        *,
        expected_activation_id: UUID | None,
    ) -> bool:
        assert session_store.load(ROOT_KEY) == original
        replace_calls.append((candidate, expected_activation_id))
        return original_conditional_replace(
            candidate,
            expected_activation_id=expected_activation_id,
        )

    monkeypatch.setattr(
        session_store,
        "conditional_replace",
        traced_conditional_replace,
    )
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(
            command,
            running_snapshot,
            build=1868,
        ),
    )
    service = SessionService(
        scope=_scope(scope_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1),
        status_timeout_seconds=0.5,
    )

    activation = await service.handshake(channel)

    assert activation.status.activation_id == CANDIDATE_1
    assert len(channel.commands) == 1
    command = channel.commands[0]
    assert command.body.expected_lineage_id == LINEAGE_1
    assert command.body.expected_activation_id == ACTIVATION_OLD
    assert command.runtime_snapshot == running_snapshot
    stored = session_store.load(ROOT_KEY)
    assert stored is not None
    assert stored.build_number == 1868
    assert stored.runtime_snapshot == running_snapshot
    assert stored.activation_id == CANDIDATE_1
    assert replace_calls == [(stored, ACTIVATION_OLD)]


@pytest.mark.asyncio
async def test_continuing_success_cas_loss_preserves_concurrent_winner(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, process)
    winner = original.model_copy(
        update={
            "activation_id": ACTIVATION_WINNER,
            "validated_at_ms": 199,
        }
    )
    expected_activations: list[UUID | None] = []

    def lose_conditional_replace(
        _candidate: SessionRecord,
        *,
        expected_activation_id: UUID | None,
    ) -> bool:
        expected_activations.append(expected_activation_id)
        session_store.replace(winner)
        return False

    monkeypatch.setattr(
        session_store,
        "conditional_replace",
        lose_conditional_replace,
    )
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert expected_activations == [ACTIVATION_OLD]
    assert len(channel.commands) == 1
    assert session_store.load(ROOT_KEY) == winner


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement_field", ["pid", "create_time"])
async def test_replaced_process_retains_guard_until_bootstrap_conditional_replace(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
    replacement_field: str,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    old_process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    process = ProcessInfo(
        pid=1201 if replacement_field == "pid" else old_process.pid,
        create_time=(1001.5 if replacement_field == "create_time" else old_process.create_time),
        executable=command_exe,
    )
    original = SessionRecord(
        root_key=ROOT_KEY,
        scenario_lineage_id=LINEAGE_1,
        activation_id=ACTIVATION_OLD,
        build_number=1868,
        runtime_snapshot=running_snapshot,
        process_pid=old_process.pid,
        process_create_time=old_process.create_time,
        validated_at_ms=100,
    )
    session_store.replace(original)

    _forbid_session_clear(session_store, monkeypatch)
    original_conditional_replace = session_store.conditional_replace
    replace_calls: list[tuple[SessionRecord, UUID | None]] = []

    def traced_conditional_replace(
        candidate: SessionRecord,
        *,
        expected_activation_id: UUID | None,
    ) -> bool:
        assert session_store.load(ROOT_KEY) == original
        replace_calls.append((candidate, expected_activation_id))
        return original_conditional_replace(
            candidate,
            expected_activation_id=expected_activation_id,
        )

    monkeypatch.setattr(
        session_store,
        "conditional_replace",
        traced_conditional_replace,
    )
    channel = _Channel(
        process,
        lambda command: (
            _successful_status_artifact(
                command,
                running_snapshot,
                lineage_id=LINEAGE_2,
            )
            if session_store.load(ROOT_KEY) == original
            else pytest.fail("process guard disappeared before bootstrap response")
        ),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1),
        status_timeout_seconds=0.5,
    )

    activation = await service.handshake(channel)

    assert activation.status.activation_id == CANDIDATE_1
    assert len(channel.commands) == 1
    assert channel.commands[0].body.expected_lineage_id is None
    assert channel.commands[0].body.expected_activation_id is None
    stored = session_store.load(ROOT_KEY)
    assert stored is not None
    assert stored.scenario_lineage_id == LINEAGE_2
    assert stored.process_pid == process.pid
    assert stored.process_create_time == process.create_time
    assert replace_calls == [(stored, ACTIVATION_OLD)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    [ErrorCode.ACTIVATION_REQUIRED, ErrorCode.ACTIVATION_MISMATCH],
)
async def test_replaced_process_error_retains_guard_without_stale_recovery(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
    error_code: ErrorCode,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    old_process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    process = ProcessInfo(pid=1300, create_time=1001.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, old_process)

    _forbid_session_clear(session_store, monkeypatch)
    channel = _Channel(
        process,
        lambda command: (
            _failed_status_artifact(
                command,
                running_snapshot,
                code=error_code,
                lineage_id=LINEAGE_2,
            )
            if session_store.load(ROOT_KEY) == original
            else pytest.fail("process guard disappeared before failed bootstrap response")
        ),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel)

    assert caught.value.code is error_code
    assert len(channel.commands) == 1
    assert session_store.load(ROOT_KEY) == original


@pytest.mark.asyncio
async def test_cancelled_replaced_process_status_retains_original_guard(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    old_process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    process = ProcessInfo(pid=1300, create_time=1001.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, old_process)
    channel = _BlockingChannel(process, block_on_call=1)
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1),
        status_timeout_seconds=0.5,
    )

    task = asyncio.create_task(service.handshake(channel))
    try:
        await asyncio.wait_for(channel.started.wait(), timeout=1.0)
        assert session_store.load(ROOT_KEY) == original
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(channel.commands) == 1
    assert session_store.load(ROOT_KEY) == original


@pytest.mark.asyncio
async def test_replaced_process_conditional_replace_cas_loss_preserves_the_winner(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    old_process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    process = ProcessInfo(pid=1201, create_time=1001.5, executable=command_exe)
    original = SessionRecord(
        root_key=ROOT_KEY,
        scenario_lineage_id=LINEAGE_1,
        activation_id=ACTIVATION_OLD,
        build_number=1868,
        runtime_snapshot=running_snapshot,
        process_pid=old_process.pid,
        process_create_time=old_process.create_time,
        validated_at_ms=100,
    )
    session_store.replace(original)
    winner = original.model_copy(
        update={
            "activation_id": ACTIVATION_WINNER,
            "process_pid": process.pid,
            "process_create_time": process.create_time,
            "validated_at_ms": 199,
        }
    )

    _forbid_session_clear(session_store, monkeypatch)
    replace_calls: list[UUID | None] = []

    def lose_conditional_replace(
        _candidate: SessionRecord,
        *,
        expected_activation_id: UUID | None,
    ) -> bool:
        replace_calls.append(expected_activation_id)
        session_store.replace(winner)
        return False

    monkeypatch.setattr(
        session_store,
        "conditional_replace",
        lose_conditional_replace,
    )
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert replace_calls == [ACTIVATION_OLD]
    assert len(channel.commands) == 1
    assert session_store.load(ROOT_KEY) == winner


@pytest.mark.asyncio
async def test_wrong_channel_executable_fails_before_status_and_preserves_the_session_row(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(
        pid=1200,
        create_time=1000.5,
        executable=Path(r"D:\Other\Command.exe"),
    )
    original = SessionRecord(
        root_key=ROOT_KEY,
        scenario_lineage_id=LINEAGE_1,
        activation_id=ACTIVATION_OLD,
        build_number=1868,
        runtime_snapshot=running_snapshot,
        process_pid=process.pid,
        process_create_time=process.create_time,
        validated_at_ms=100,
    )
    session_store.replace(original)

    def forbidden_clear(
        _root_key: str,
        *,
        expected_activation_id: UUID | None = None,
    ) -> bool:
        del expected_activation_id
        pytest.fail("wrong executable must not clear state")

    monkeypatch.setattr(
        session_store,
        "clear",
        forbidden_clear,
    )
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert channel.commands == []
    assert session_store.load(ROOT_KEY) == original


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["uuid", "clock"])
@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit])
async def test_synchronous_identity_and_clock_sources_never_swallow_control_flow(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    phase: str,
    error_type: type[BaseException],
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    sentinel = error_type("control-flow sentinel")

    def raising_uuid() -> UUID:
        raise sentinel

    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=(_RaisingWallClock(sentinel) if phase == "clock" else _WallClock(200)),
        uuid4_source=(_UuidSequence(CANDIDATE_1, REQUEST_1) if phase == "clock" else raising_uuid),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(error_type) as caught:
        await service.handshake(channel)

    assert caught.value is sentinel
    assert len(channel.commands) == (1 if phase == "clock" else 0)
    assert session_store.load(ROOT_KEY) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [None, LINEAGE_3])
async def test_lineage_mismatch_without_exact_acceptance_preserves_old_session(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    accepted: UUID | None,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, process)
    channel = _Channel(
        process,
        lambda command: _failed_status_artifact(
            command,
            running_snapshot,
            code=ErrorCode.SCENARIO_CHANGED,
            lineage_id=LINEAGE_2,
        ),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel, accept_lineage_id=accepted)

    assert caught.value.code is ErrorCode.SCENARIO_CHANGED
    assert caught.value.details["observed_lineage_id"] == str(LINEAGE_2)
    assert len(channel.commands) == 1
    wire = cast(BridgeStatusWireArgs, channel.commands[0].invocation.wire_arguments)
    assert wire.accept_lineage_id == accepted
    assert session_store.load(ROOT_KEY) == original


@pytest.mark.asyncio
async def test_lineage_acceptance_on_ordinary_success_rejects_without_persistence(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, process)
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel, accept_lineage_id=LINEAGE_1)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert len(channel.commands) == 1
    assert session_store.load(ROOT_KEY) == original


@pytest.mark.asyncio
async def test_exact_lineage_acceptance_persists_guard_before_second_status(
    running_snapshot: RuntimeSnapshot,
    stale_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = SessionRecord(
        root_key=ROOT_KEY,
        scenario_lineage_id=LINEAGE_1,
        activation_id=ACTIVATION_OLD,
        build_number=1867,
        runtime_snapshot=stale_snapshot,
        process_pid=process.pid,
        process_create_time=process.create_time,
        validated_at_ms=100,
    )
    session_store.replace(original)

    _forbid_session_clear(session_store, monkeypatch)
    original_conditional_replace = session_store.conditional_replace
    replace_calls: list[tuple[SessionRecord, UUID | None]] = []

    def traced_conditional_replace(
        candidate: SessionRecord,
        *,
        expected_activation_id: UUID | None,
    ) -> bool:
        current = session_store.load(ROOT_KEY)
        if not replace_calls:
            assert current == original
        else:
            assert current == replace_calls[-1][0]
        result = original_conditional_replace(
            candidate,
            expected_activation_id=expected_activation_id,
        )
        replace_calls.append((candidate, expected_activation_id))
        return result

    monkeypatch.setattr(
        session_store,
        "conditional_replace",
        traced_conditional_replace,
    )

    def second_status(command: ExchangeCommand) -> ResponseArtifact:
        assert len(replace_calls) == 1
        assert session_store.load(ROOT_KEY) == replace_calls[0][0]
        return _successful_status_artifact(
            command,
            running_snapshot,
            lineage_id=LINEAGE_2,
            delivery_id=DELIVERY_2,
        )

    channel = _Channel(
        process,
        _ResponderSequence(
            lambda command: _failed_status_artifact(
                command,
                running_snapshot,
                code=ErrorCode.SCENARIO_CHANGED,
                lineage_id=LINEAGE_2,
                delivery_id=DELIVERY_1,
            ),
            second_status,
        ),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(201),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, CANDIDATE_2, REQUEST_2),
        status_timeout_seconds=0.5,
    )

    activation = await service.handshake(channel, accept_lineage_id=LINEAGE_2)

    assert activation.status_request_id == REQUEST_2
    assert activation.status.lineage_id == LINEAGE_2
    assert activation.status.activation_id == CANDIDATE_2
    assert len(channel.commands) == 2
    first, second = channel.commands
    assert first.request_id == REQUEST_1
    assert first.body.expected_lineage_id == LINEAGE_1
    assert first.body.expected_activation_id == ACTIVATION_OLD
    assert second.request_id == REQUEST_2
    assert second.request_id != first.request_id
    assert second.body.expected_lineage_id == LINEAGE_2
    assert second.body.expected_activation_id == CANDIDATE_1
    second_wire = cast(BridgeStatusWireArgs, second.invocation.wire_arguments)
    assert second_wire.accept_lineage_id == LINEAGE_2
    assert second_wire.activation_candidate == CANDIDATE_2
    stored = session_store.load(ROOT_KEY)
    assert stored is not None
    assert stored.scenario_lineage_id == LINEAGE_2
    assert stored.activation_id == CANDIDATE_2
    expected_guard = SessionRecord(
        root_key=ROOT_KEY,
        scenario_lineage_id=LINEAGE_2,
        activation_id=CANDIDATE_1,
        build_number=original.build_number,
        runtime_snapshot=running_snapshot,
        process_pid=process.pid,
        process_create_time=process.create_time,
        validated_at_ms=original.validated_at_ms,
    )
    assert replace_calls == [
        (expected_guard, ACTIVATION_OLD),
        (stored, CANDIDATE_1),
    ]


@pytest.mark.asyncio
async def test_lineage_adoption_guard_cas_loss_preserves_winner_and_stops_before_second_status(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, process)
    winner = original.model_copy(
        update={
            "activation_id": ACTIVATION_WINNER,
            "validated_at_ms": 199,
        }
    )
    expected_activations: list[UUID | None] = []

    _forbid_session_clear(session_store, monkeypatch)

    def lose_conditional_replace(
        _candidate: SessionRecord,
        *,
        expected_activation_id: UUID | None,
    ) -> bool:
        expected_activations.append(expected_activation_id)
        session_store.replace(winner)
        return False

    monkeypatch.setattr(
        session_store,
        "conditional_replace",
        lose_conditional_replace,
    )
    channel = _Channel(
        process,
        lambda command: _failed_status_artifact(
            command,
            running_snapshot,
            code=ErrorCode.SCENARIO_CHANGED,
            lineage_id=LINEAGE_2,
        ),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel, accept_lineage_id=LINEAGE_2)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert expected_activations == [ACTIVATION_OLD]
    assert len(channel.commands) == 1
    assert session_store.load(ROOT_KEY) == winner


@pytest.mark.asyncio
async def test_lineage_adoption_final_cas_loss_preserves_concurrent_winner(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, process)
    guard = original.model_copy(
        update={
            "scenario_lineage_id": LINEAGE_2,
            "activation_id": CANDIDATE_1,
            "runtime_snapshot": running_snapshot,
        }
    )
    winner = guard.model_copy(
        update={
            "activation_id": ACTIVATION_WINNER,
            "validated_at_ms": 199,
        }
    )

    _forbid_session_clear(session_store, monkeypatch)
    original_conditional_replace = session_store.conditional_replace
    expected_activations: list[UUID | None] = []

    def conditional_replace_then_lose(
        candidate: SessionRecord,
        *,
        expected_activation_id: UUID | None,
    ) -> bool:
        expected_activations.append(expected_activation_id)
        if len(expected_activations) == 1:
            assert candidate == guard
            return original_conditional_replace(
                candidate,
                expected_activation_id=expected_activation_id,
            )
        assert session_store.load(ROOT_KEY) == guard
        session_store.replace(winner)
        return False

    monkeypatch.setattr(
        session_store,
        "conditional_replace",
        conditional_replace_then_lose,
    )
    channel = _Channel(
        process,
        _ResponderSequence(
            lambda command: _failed_status_artifact(
                command,
                running_snapshot,
                code=ErrorCode.SCENARIO_CHANGED,
                lineage_id=LINEAGE_2,
                delivery_id=DELIVERY_1,
            ),
            lambda command: _successful_status_artifact(
                command,
                running_snapshot,
                lineage_id=LINEAGE_2,
                delivery_id=DELIVERY_2,
            ),
        ),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(201),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, CANDIDATE_2, REQUEST_2),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel, accept_lineage_id=LINEAGE_2)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert expected_activations == [ACTIVATION_OLD, CANDIDATE_1]
    assert len(channel.commands) == 2
    assert session_store.load(ROOT_KEY) == winner


@pytest.mark.asyncio
async def test_failed_second_adoption_status_retains_accepted_lineage_guard(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, process)
    channel = _Channel(
        process,
        _ResponderSequence(
            lambda command: _failed_status_artifact(
                command,
                running_snapshot,
                code=ErrorCode.SCENARIO_CHANGED,
                lineage_id=LINEAGE_2,
                delivery_id=DELIVERY_1,
            ),
            lambda command: _failed_status_artifact(
                command,
                running_snapshot,
                code=ErrorCode.ACTIVATION_REQUIRED,
                lineage_id=LINEAGE_2,
                delivery_id=DELIVERY_2,
            ),
        ),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, CANDIDATE_2, REQUEST_2),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel, accept_lineage_id=LINEAGE_2)

    assert caught.value.code is ErrorCode.ACTIVATION_REQUIRED
    assert len(channel.commands) == 2
    assert session_store.load(ROOT_KEY) == SessionRecord(
        root_key=ROOT_KEY,
        scenario_lineage_id=LINEAGE_2,
        activation_id=CANDIDATE_1,
        build_number=original.build_number,
        runtime_snapshot=running_snapshot,
        process_pid=process.pid,
        process_create_time=process.create_time,
        validated_at_ms=original.validated_at_ms,
    )


@pytest.mark.asyncio
async def test_cancelled_second_adoption_status_retains_guard_across_service_restart(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, process)
    channel = _BlockingChannel(
        process,
        block_on_call=2,
        responders=(
            lambda command: _failed_status_artifact(
                command,
                running_snapshot,
                code=ErrorCode.SCENARIO_CHANGED,
                lineage_id=LINEAGE_2,
            ),
        ),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, CANDIDATE_2, REQUEST_2),
        status_timeout_seconds=0.5,
    )
    expected_guard = SessionRecord(
        root_key=ROOT_KEY,
        scenario_lineage_id=LINEAGE_2,
        activation_id=CANDIDATE_1,
        build_number=original.build_number,
        runtime_snapshot=running_snapshot,
        process_pid=process.pid,
        process_create_time=process.create_time,
        validated_at_ms=original.validated_at_ms,
    )

    task = asyncio.create_task(service.handshake(channel, accept_lineage_id=LINEAGE_2))
    try:
        await asyncio.wait_for(channel.started.wait(), timeout=1.0)
        assert session_store.load(ROOT_KEY) == expected_guard
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert session_store.load(ROOT_KEY) == expected_guard
    restart_channel = _Channel(
        process,
        lambda command: _failed_status_artifact(
            command,
            running_snapshot,
            code=ErrorCode.SCENARIO_CHANGED,
            lineage_id=LINEAGE_3,
        ),
    )
    restarted_service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(CANDIDATE_3, REQUEST_3),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await restarted_service.handshake(restart_channel)

    assert caught.value.code is ErrorCode.SCENARIO_CHANGED
    assert caught.value.details["observed_lineage_id"] == str(LINEAGE_3)
    assert len(restart_channel.commands) == 1
    restart_command = restart_channel.commands[0]
    assert restart_command.body.expected_lineage_id == LINEAGE_2
    assert restart_command.body.expected_activation_id == CANDIDATE_1
    assert session_store.load(ROOT_KEY) == expected_guard


@pytest.mark.asyncio
async def test_reserved_candidate_is_created_privately_and_reused_byte_for_byte(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200, 201),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, REQUEST_2),
        status_timeout_seconds=0.5,
    )

    reserved = service.reserve_activation_candidate()
    first = await service.handshake(
        channel,
        reserved_activation_candidate=reserved,
    )
    second = await service.handshake(
        channel,
        reserved_activation_candidate=reserved,
    )

    assert reserved == CANDIDATE_1
    assert first.status_request_id == REQUEST_1
    assert second.status_request_id == REQUEST_2
    assert [
        cast(BridgeStatusWireArgs, command.invocation.wire_arguments).activation_candidate
        for command in channel.commands
    ] == [reserved, reserved]
    assert channel.commands[0].body.expected_lineage_id is None
    assert channel.commands[0].body.expected_activation_id is None
    assert channel.commands[1].body.expected_lineage_id == LINEAGE_1
    assert channel.commands[1].body.expected_activation_id == reserved


class _DerivedUuid(UUID):
    pass


@pytest.mark.parametrize(
    "candidate",
    [
        _DerivedUuid(str(CANDIDATE_1)),
        UUID("11111111-1111-1111-8111-111111111111"),
        cast(UUID, str(CANDIDATE_1)),
    ],
)
def test_reservation_rejects_invalid_uuid_source_values(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    candidate: UUID,
) -> None:
    service = SessionService(
        scope=_scope(Path(r"D:\Games\CMO\Command.exe")),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=lambda: candidate,
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        service.reserve_activation_candidate()

    assert caught.value.code is ErrorCode.STATE_CONFLICT


@pytest.mark.asyncio
async def test_persisted_reserved_candidate_crosses_service_instances_but_not_lineage_acceptance(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    reserving_service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(CANDIDATE_1),
        status_timeout_seconds=0.5,
    )
    persisted_candidate = reserving_service.reserve_activation_candidate()
    resumed_service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=_UuidSequence(REQUEST_1),
        status_timeout_seconds=0.5,
    )

    activation = await resumed_service.handshake(
        channel,
        reserved_activation_candidate=persisted_candidate,
    )
    with pytest.raises(BridgeError) as combined:
        await resumed_service.handshake(
            channel,
            accept_lineage_id=LINEAGE_2,
            reserved_activation_candidate=persisted_candidate,
        )

    assert activation.status.activation_id == persisted_candidate
    assert len(channel.commands) == 1
    wire = cast(BridgeStatusWireArgs, channel.commands[0].invocation.wire_arguments)
    assert wire.activation_candidate == persisted_candidate
    assert combined.value.code is ErrorCode.INVALID_ARGUMENT


@pytest.mark.asyncio
async def test_ordinary_generated_candidate_can_never_be_reused(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200, 201),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, CANDIDATE_1, REQUEST_2),
        status_timeout_seconds=0.5,
    )
    await service.handshake(channel)

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert len(channel.commands) == 1


@pytest.mark.asyncio
async def test_prepare_read_attempt_handshakes_then_builds_a_fresh_contextual_command(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, REQUEST_2),
        status_timeout_seconds=0.5,
    )
    invocation = OPERATION_REGISTRY.resolve_invocation("scenario.get", {})

    attempt = await service.prepare_read_attempt(
        channel,
        invocation,
        timeout_seconds=1.25,
    )

    assert type(attempt) is PreparedReadAttempt
    assert attempt.rehandshakes_used == 0
    assert attempt.activation.status_request_id == REQUEST_1
    assert attempt.command.request_id == REQUEST_2
    assert attempt.command.invocation == invocation
    assert attempt.command.invocation.public_arguments == invocation.public_arguments
    assert attempt.command.timeout == 1.25
    assert attempt.command.runtime_snapshot == running_snapshot
    assert attempt.command.body.expected_lineage_id == LINEAGE_1
    assert attempt.command.body.expected_activation_id == CANDIDATE_1
    assert attempt.command.body.operation == "scenario.get"
    assert attempt.command.body.arguments == {}
    assert len(channel.commands) == 1


@pytest.mark.asyncio
async def test_prepare_read_attempt_rejects_nonread_before_handshake_or_uuid_allocation(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    channel = _Channel(
        process,
        lambda _command: pytest.fail("non-read must stop before status I/O"),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(),
        status_timeout_seconds=0.5,
    )
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "unit.set",
        {"unit_guid": "UNIT-1", "speed": 18},
    )

    with pytest.raises(BridgeError) as caught:
        await service.prepare_read_attempt(
            channel,
            invocation,
            timeout_seconds=1.25,
        )

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert channel.commands == []


@pytest.mark.asyncio
async def test_prepare_exchange_accepts_only_cmo_invocations_and_strict_positive_timeout(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, REQUEST_2),
        status_timeout_seconds=0.5,
    )
    activation = await service.handshake(channel)
    local = OPERATION_REGISTRY.resolve_invocation("bridge.doctor", {})

    with pytest.raises(BridgeError) as local_error:
        service.prepare_exchange(activation, local, timeout_seconds=1.0)
    with pytest.raises(BridgeError) as timeout_error:
        service.prepare_exchange(
            activation,
            OPERATION_REGISTRY.resolve_invocation("scenario.get", {}),
            timeout_seconds=0,
        )

    assert local_error.value.code is ErrorCode.INVALID_ARGUMENT
    assert timeout_error.value.code is ErrorCode.INVALID_ARGUMENT


@pytest.mark.asyncio
async def test_read_activation_mismatch_rehandshakes_once_with_a_new_operation_request(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200, 201),
        uuid4_source=_UuidSequence(
            CANDIDATE_1,
            REQUEST_1,
            REQUEST_2,
            CANDIDATE_2,
            REQUEST_3,
            REQUEST_4,
        ),
        status_timeout_seconds=0.5,
    )
    invocation = OPERATION_REGISTRY.resolve_invocation("scenario.get", {})
    prior = await service.prepare_read_attempt(
        channel,
        invocation,
        timeout_seconds=1.25,
    )
    mismatch = _failed_operation_artifact(
        prior.command,
        running_snapshot,
        code=ErrorCode.ACTIVATION_MISMATCH,
    )

    prepared = await service.reprepare_read_after_activation_mismatch(
        channel,
        prior,
        mismatch,
    )

    assert prepared.rehandshakes_used == 1
    assert prepared.activation.status_request_id == REQUEST_3
    assert prepared.activation.status.activation_id == CANDIDATE_2
    assert prepared.command.request_id == REQUEST_4
    assert prepared.command.request_id != prior.command.request_id
    assert prepared.command.invocation == prior.command.invocation
    assert prepared.command.timeout == prior.command.timeout
    assert prepared.command.body.expected_lineage_id == LINEAGE_1
    assert prepared.command.body.expected_activation_id == CANDIDATE_2
    assert len(channel.commands) == 2
    assert [command.request_id for command in channel.commands] == [REQUEST_1, REQUEST_3]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["second_attempt", "other_error", "unrelated", "malformed", "non_read"],
)
async def test_invalid_read_rehandshake_cases_stop_before_status_or_new_identity(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    case: str,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    ids = _UuidSequence(CANDIDATE_1, REQUEST_1, REQUEST_2, REQUEST_3)
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=ids,
        status_timeout_seconds=0.5,
    )
    prior = await service.prepare_read_attempt(
        channel,
        OPERATION_REGISTRY.resolve_invocation("scenario.get", {}),
        timeout_seconds=1.25,
    )
    passed_prior = prior
    passed_response: ResponseArtifact = _failed_operation_artifact(
        prior.command,
        running_snapshot,
        code=ErrorCode.ACTIVATION_MISMATCH,
    )
    expected_uuid_calls = 3
    if case == "second_attempt":
        passed_prior = prior.model_copy(update={"rehandshakes_used": 1})
    elif case == "other_error":
        passed_response = _failed_operation_artifact(
            prior.command,
            running_snapshot,
            code=ErrorCode.NOT_FOUND,
        )
    elif case == "unrelated":
        unrelated_command = replace(prior.command, request_id=REQUEST_3)
        passed_response = _failed_operation_artifact(
            unrelated_command,
            running_snapshot,
            code=ErrorCode.ACTIVATION_MISMATCH,
        )
    elif case == "malformed":
        passed_response = cast(ResponseArtifact, object())
    else:
        mutation = OPERATION_REGISTRY.resolve_invocation(
            "unit.set",
            {"unit_guid": "UNIT-1", "speed": 18},
        )
        mutation_command = service.prepare_exchange(
            prior.activation,
            mutation,
            timeout_seconds=1.25,
        )
        expected_uuid_calls += 1
        passed_prior = PreparedReadAttempt.model_construct(
            activation=prior.activation,
            command=mutation_command,
            rehandshakes_used=0,
        )

    with pytest.raises(BridgeError) as caught:
        await service.reprepare_read_after_activation_mismatch(
            channel,
            passed_prior,
            passed_response,
        )

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert len(channel.commands) == 1
    assert ids.calls == expected_uuid_calls
    stored = session_store.load(ROOT_KEY)
    assert stored is not None
    assert stored.activation_id == CANDIDATE_1


@pytest.mark.parametrize(
    "command_exe",
    [Path("Command.exe"), Path(r"D:\Games\CMO\not-command.exe")],
)
def test_session_scope_requires_an_absolute_command_executable(command_exe: Path) -> None:
    with pytest.raises(ValidationError):
        SessionScope(root_key=ROOT_KEY, command_exe=command_exe)


def test_session_service_rejects_registry_snapshot_manifest_drift_at_construction(
    session_store: SessionStore,
) -> None:
    mismatched_snapshot = RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256="e" * 64,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )

    with pytest.raises(BridgeError) as caught:
        SessionService(
            scope=_scope(Path(r"D:\Games\CMO\Command.exe")),
            session_store=session_store,
            registry=OPERATION_REGISTRY,
            runtime_snapshot=mismatched_snapshot,
            wall_clock=_WallClock(),
            uuid4_source=_UuidSequence(),
            status_timeout_seconds=0.5,
        )

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["failed", "runtime_drift", "result_drift"])
async def test_prepare_exchange_rejects_forged_activation_artifact(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    case: str,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, REQUEST_2),
        status_timeout_seconds=0.5,
    )
    activation = await service.handshake(channel)
    status_command = channel.commands[0]
    if case == "failed":
        forged_artifact = _failed_status_artifact(
            status_command,
            running_snapshot,
            code=ErrorCode.ACTIVATION_REQUIRED,
            lineage_id=LINEAGE_1,
        )
    elif case == "runtime_drift":
        alternate_snapshot = RuntimeSnapshot.create(
            runtime_version="0.1.0",
            runtime_asset_sha256="e" * 64,
            operation_manifest_sha256=OPERATION_REGISTRY.manifest_sha256,
            host_contract_sha256="c" * 64,
            dependency_lock_sha256="d" * 64,
        )
        forged_artifact = _successful_status_artifact(
            status_command,
            alternate_snapshot,
        )
    else:
        forged_artifact = _successful_status_artifact(
            status_command,
            running_snapshot,
            build=1867,
        )
    forged = SessionActivation.model_construct(
        status_request_id=activation.status_request_id,
        status=activation.status,
        response_artifact=forged_artifact,
    )

    with pytest.raises(BridgeError) as caught:
        service.prepare_exchange(
            forged,
            OPERATION_REGISTRY.resolve_invocation("scenario.get", {}),
            timeout_seconds=1.0,
        )

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


@pytest.mark.asyncio
async def test_reserved_candidate_role_may_repeat_but_never_collide_with_request_role(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    channel = _Channel(
        process,
        lambda command: _successful_status_artifact(command, running_snapshot),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200, 201),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, REQUEST_2),
        status_timeout_seconds=0.5,
    )
    await service.handshake(channel)
    await service.handshake(
        channel,
        reserved_activation_candidate=CANDIDATE_1,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(
            channel,
            reserved_activation_candidate=REQUEST_1,
        )

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert len(channel.commands) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("use_reserved", [False, True])
async def test_stale_continuing_activation_retains_guard_until_one_bootstrap(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    use_reserved: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, process)

    _forbid_session_clear(session_store, monkeypatch)

    def first_response(command: ExchangeCommand) -> ResponseArtifact:
        assert session_store.load(ROOT_KEY) == original
        return _failed_status_artifact(
            command,
            running_snapshot,
            code=ErrorCode.ACTIVATION_MISMATCH,
            lineage_id=LINEAGE_1,
            delivery_id=DELIVERY_1,
        )

    def bootstrap_response(command: ExchangeCommand) -> ResponseArtifact:
        assert session_store.load(ROOT_KEY) == original
        return _successful_status_artifact(
            command,
            running_snapshot,
            delivery_id=DELIVERY_2,
        )

    channel = _Channel(
        process,
        _ResponderSequence(
            first_response,
            bootstrap_response,
        ),
    )
    original_conditional_replace = session_store.conditional_replace
    replace_calls: list[tuple[SessionRecord, UUID | None]] = []

    def traced_conditional_replace(
        candidate: SessionRecord,
        *,
        expected_activation_id: UUID | None,
    ) -> bool:
        assert session_store.load(ROOT_KEY) == original
        replace_calls.append((candidate, expected_activation_id))
        return original_conditional_replace(
            candidate,
            expected_activation_id=expected_activation_id,
        )

    monkeypatch.setattr(
        session_store,
        "conditional_replace",
        traced_conditional_replace,
    )
    ids = (
        _UuidSequence(REQUEST_1, REQUEST_2)
        if use_reserved
        else _UuidSequence(CANDIDATE_1, REQUEST_1, CANDIDATE_2, REQUEST_2)
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=ids,
        status_timeout_seconds=0.5,
    )

    activation = await service.handshake(
        channel,
        reserved_activation_candidate=CANDIDATE_1 if use_reserved else None,
    )

    assert activation.status_request_id == REQUEST_2
    assert len(channel.commands) == 2
    first, second = channel.commands
    assert first.body.expected_lineage_id == LINEAGE_1
    assert first.body.expected_activation_id == ACTIVATION_OLD
    assert second.body.expected_lineage_id is None
    assert second.body.expected_activation_id is None
    first_candidate = cast(
        BridgeStatusWireArgs,
        first.invocation.wire_arguments,
    ).activation_candidate
    second_candidate = cast(
        BridgeStatusWireArgs,
        second.invocation.wire_arguments,
    ).activation_candidate
    assert first_candidate == CANDIDATE_1
    assert second_candidate == (CANDIDATE_1 if use_reserved else CANDIDATE_2)
    assert activation.status.activation_id == second_candidate
    stored = session_store.load(ROOT_KEY)
    assert stored is not None
    assert stored.scenario_lineage_id == LINEAGE_1
    assert stored.activation_id == second_candidate
    assert replace_calls == [(stored, ACTIVATION_OLD)]


@pytest.mark.asyncio
async def test_stale_activation_final_cas_loss_preserves_concurrent_winner(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, process)
    winner = original.model_copy(
        update={
            "activation_id": ACTIVATION_WINNER,
            "validated_at_ms": 199,
        }
    )

    _forbid_session_clear(session_store, monkeypatch)
    expected_activations: list[UUID | None] = []

    def lose_conditional_replace(
        _candidate: SessionRecord,
        *,
        expected_activation_id: UUID | None,
    ) -> bool:
        expected_activations.append(expected_activation_id)
        session_store.replace(winner)
        return False

    monkeypatch.setattr(
        session_store,
        "conditional_replace",
        lose_conditional_replace,
    )
    channel = _Channel(
        process,
        _ResponderSequence(
            lambda command: _failed_status_artifact(
                command,
                running_snapshot,
                code=ErrorCode.ACTIVATION_MISMATCH,
                lineage_id=LINEAGE_1,
                delivery_id=DELIVERY_1,
            ),
            lambda command: _successful_status_artifact(
                command,
                running_snapshot,
                delivery_id=DELIVERY_2,
            ),
        ),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(200),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, CANDIDATE_2, REQUEST_2),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert expected_activations == [ACTIVATION_OLD]
    assert len(channel.commands) == 2
    assert session_store.load(ROOT_KEY) == winner


@pytest.mark.asyncio
async def test_stale_activation_second_bootstrap_failure_retains_original_guard(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, process)

    _forbid_session_clear(session_store, monkeypatch)
    channel = _Channel(
        process,
        _ResponderSequence(
            lambda command: _failed_status_artifact(
                command,
                running_snapshot,
                code=ErrorCode.ACTIVATION_MISMATCH,
                lineage_id=LINEAGE_1,
                delivery_id=DELIVERY_1,
            ),
            lambda command: _failed_status_artifact(
                command,
                running_snapshot,
                code=ErrorCode.ACTIVATION_REQUIRED,
                lineage_id=LINEAGE_1,
                delivery_id=DELIVERY_2,
            ),
        ),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, CANDIDATE_2, REQUEST_2),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel)

    assert caught.value.code is ErrorCode.ACTIVATION_REQUIRED
    assert len(channel.commands) == 2
    assert session_store.load(ROOT_KEY) == original


@pytest.mark.asyncio
async def test_stale_activation_bootstrap_different_lineage_retains_guard(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, process)

    _forbid_session_clear(session_store, monkeypatch)
    channel = _Channel(
        process,
        _ResponderSequence(
            lambda command: _failed_status_artifact(
                command,
                running_snapshot,
                code=ErrorCode.ACTIVATION_MISMATCH,
                lineage_id=LINEAGE_1,
                delivery_id=DELIVERY_1,
            ),
            lambda command: _successful_status_artifact(
                command,
                running_snapshot,
                lineage_id=LINEAGE_2,
                delivery_id=DELIVERY_2,
            ),
        ),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, CANDIDATE_2, REQUEST_2),
        status_timeout_seconds=0.5,
    )

    with pytest.raises(BridgeError) as caught:
        await service.handshake(channel)

    assert caught.value.code is ErrorCode.SCENARIO_CHANGED
    assert caught.value.details["observed_lineage_id"] == str(LINEAGE_2)
    assert len(channel.commands) == 2
    assert session_store.load(ROOT_KEY) == original


@pytest.mark.asyncio
async def test_cancelled_stale_bootstrap_retains_guard_and_restart_converges(
    running_snapshot: RuntimeSnapshot,
    session_store: SessionStore,
) -> None:
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    original = _stored_session(session_store, running_snapshot, process)
    channel = _BlockingChannel(
        process,
        block_on_call=2,
        responders=(
            lambda command: _failed_status_artifact(
                command,
                running_snapshot,
                code=ErrorCode.ACTIVATION_MISMATCH,
                lineage_id=LINEAGE_1,
            ),
        ),
    )
    service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=_UuidSequence(CANDIDATE_1, REQUEST_1, CANDIDATE_2, REQUEST_2),
        status_timeout_seconds=0.5,
    )

    task = asyncio.create_task(service.handshake(channel))
    try:
        await asyncio.wait_for(channel.started.wait(), timeout=1.0)
        assert session_store.load(ROOT_KEY) == original
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert session_store.load(ROOT_KEY) == original
    restart_channel = _Channel(
        process,
        _ResponderSequence(
            lambda command: _failed_status_artifact(
                command,
                running_snapshot,
                code=ErrorCode.ACTIVATION_MISMATCH,
                lineage_id=LINEAGE_1,
                delivery_id=DELIVERY_1,
            ),
            lambda command: _successful_status_artifact(
                command,
                running_snapshot,
                delivery_id=DELIVERY_2,
            ),
        ),
    )
    restarted_service = SessionService(
        scope=_scope(command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(300),
        uuid4_source=_UuidSequence(CANDIDATE_3, REQUEST_3, CANDIDATE_4, REQUEST_4),
        status_timeout_seconds=0.5,
    )

    activation = await restarted_service.handshake(restart_channel)

    assert activation.status.activation_id == CANDIDATE_4
    assert len(restart_channel.commands) == 2
    first, second = restart_channel.commands
    assert first.body.expected_lineage_id == LINEAGE_1
    assert first.body.expected_activation_id == ACTIVATION_OLD
    assert second.body.expected_lineage_id is None
    assert second.body.expected_activation_id is None
    assert session_store.load(ROOT_KEY) == SessionRecord(
        root_key=ROOT_KEY,
        scenario_lineage_id=LINEAGE_1,
        activation_id=CANDIDATE_4,
        build_number=1868,
        runtime_snapshot=running_snapshot,
        process_pid=process.pid,
        process_create_time=process.create_time,
        validated_at_ms=300,
    )


def test_session_service_root_key_is_exact_read_only_and_side_effect_free(
    tmp_path: Path,
    running_snapshot: RuntimeSnapshot,
) -> None:
    root_property = cast(property, vars(SessionService)["root_key"])
    getter = cast(Callable[[SessionService], Sha256], root_property.fget)
    signature = inspect.signature(getter)
    assert tuple(signature.parameters) == ("self",)
    assert signature.parameters["self"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert signature.parameters["self"].default is inspect.Parameter.empty
    assert get_type_hints(getter, include_extras=True) == {"return": Sha256}
    assert root_property.fset is None
    assert not inspect.iscoroutinefunction(getter)

    database_path = tmp_path / "root-property.sqlite3"
    store = SessionStore(StateDatabase(database_path))
    ids = _UuidSequence()
    scope = _scope(Path(r"D:\Games\CMO\Command.exe"))
    service = SessionService(
        scope=scope,
        session_store=store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=running_snapshot,
        wall_clock=_WallClock(),
        uuid4_source=ids,
        status_timeout_seconds=0.5,
    )
    before = dict(vars(service))

    observed = service.root_key

    assert type(observed) is str
    assert observed == ROOT_KEY == scope.root_key
    assert dict(vars(service)) == before
    assert ids.calls == 0
    assert not database_path.exists()
    with pytest.raises(AttributeError):
        setattr(cast(Any, service), "root_key", "b" * 64)
    assert service.root_key == ROOT_KEY
    assert dict(vars(service)) == before
    assert not database_path.exists()
