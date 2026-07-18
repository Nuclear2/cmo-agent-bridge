from __future__ import annotations

import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID

import pytest
from pydantic import JsonValue

import cmo_agent_bridge.mcp_runtime as runtime_module
from cmo_agent_bridge import __version__
from cmo_agent_bridge.application.models import InvocationOutcome
from cmo_agent_bridge.application.queue_models import (
    QueuedOperationList,
    QueuedOperationStatus,
)
from cmo_agent_bridge.application.service import BridgeApplication
from cmo_agent_bridge.bootstrap import ApplicationRuntime
from cmo_agent_bridge.config import BridgeConfig
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.mcp_runtime import (
    McpRuntimeManager,
    McpSimulationPulseResult,
    McpTimeSetResult,
    McpTimeState,
)
from cmo_agent_bridge.mcp_server import McpApplicationPort, create_mcp_server
from cmo_agent_bridge.runtime_bundle import create_runtime_snapshot
from cmo_agent_bridge.state.operation_queue import OperationQueueState
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo
from cmo_agent_bridge.ui_time import (
    SimulationRunState,
    TimeRate,
    UiTimeState,
)


_REQUEST_A = UUID("00000000-0000-4000-8000-000000000101")
_REQUEST_B = UUID("00000000-0000-4000-8000-000000000102")


def _ui_state(state: SimulationRunState, rate: TimeRate) -> UiTimeState:
    return UiTimeState(
        process=ProcessInfo(
            pid=4321,
            create_time=1234.5,
            executable=Path("C:/CMO/Command.exe"),
        ),
        state=state,
        rate=rate,
        window_handle=9876,
        window_title="Test Scenario - Command: Modern Operations",
    )


class _FakeUiTimeController:
    def __init__(
        self,
        state: UiTimeState,
        *,
        pause_error: BridgeError | None = None,
        pause_result: UiTimeState | None = None,
        play_error: BridgeError | None = None,
        play_failure_state: UiTimeState | None = None,
    ) -> None:
        self.state = state
        self.pause_error = pause_error
        self.pause_result = pause_result
        self.play_error = play_error
        self.play_failure_state = play_failure_state
        self.calls: list[tuple[str, TimeRate | None]] = []
        self.played = asyncio.Event()

    async def get_state(self) -> UiTimeState:
        self.calls.append(("get_state", None))
        return self.state

    async def pause(self) -> UiTimeState:
        self.calls.append(("pause", None))
        if self.pause_error is not None:
            raise self.pause_error
        self.state = self.pause_result or _ui_state(SimulationRunState.PAUSED, self.state.rate)
        return self.state

    async def resume(self, rate: TimeRate | None = None) -> UiTimeState:
        self.calls.append(("resume", rate))
        self.state = _ui_state(SimulationRunState.RUNNING, rate or self.state.rate)
        return self.state

    async def set_rate(self, rate: TimeRate) -> UiTimeState:
        self.calls.append(("set_rate", rate))
        self.state = _ui_state(self.state.state, rate)
        return self.state

    async def play_1x(self) -> UiTimeState:
        self.calls.append(("play_1x", None))
        if self.play_error is not None:
            if self.play_failure_state is not None:
                self.state = self.play_failure_state
            raise self.play_error
        self.state = _ui_state(SimulationRunState.RUNNING, TimeRate.X1)
        self.played.set()
        return self.state


def _queued_status(
    request_id: UUID,
    state: OperationQueueState,
    *,
    sequence: int,
) -> QueuedOperationStatus:
    terminal = state in {
        OperationQueueState.COMPLETED,
        OperationQueueState.REJECTED,
        OperationQueueState.QUARANTINED,
        OperationQueueState.CANCELLED,
    }
    return QueuedOperationStatus(
        request_id=request_id,
        operation="mission.create",
        sequence=sequence,
        state=state,
        submitted_at_ms=10,
        started_at_ms=(20 if state is OperationQueueState.ACTIVE else None),
        completed_at_ms=(30 if terminal else None),
        result=({"accepted": True} if state is OperationQueueState.COMPLETED else None),
    )


class _FakeQueueService:
    def __init__(
        self,
        sequences: Mapping[UUID, tuple[QueuedOperationStatus, ...]] | None = None,
    ) -> None:
        self._sequences = dict(sequences or {})
        self._indices = {request_id: 0 for request_id in self._sequences}
        self._current = {
            request_id: statuses[0] for request_id, statuses in self._sequences.items()
        }
        self.get_calls: list[UUID] = []
        self.list_calls = 0
        self.nonterminal_list_calls = 0
        self.wait_calls = 0

    def get(self, *, request_id: UUID) -> QueuedOperationStatus:
        self.get_calls.append(request_id)
        statuses = self._sequences[request_id]
        index = min(self._indices[request_id], len(statuses) - 1)
        status = statuses[index]
        self._current[request_id] = status
        self._indices[request_id] += 1
        return status

    def list(self, *, limit: int | None = None) -> QueuedOperationList:
        self.list_calls += 1
        items = tuple(self._current.values())
        return QueuedOperationList(items=items if limit is None else items[:limit])

    def list_nonterminal(self) -> QueuedOperationList:
        self.nonterminal_list_calls += 1
        items = tuple(
            status
            for status in self._current.values()
            if status.state in {OperationQueueState.QUEUED, OperationQueueState.ACTIVE}
        )
        return QueuedOperationList(items=items)

    async def wait(self, *, request_id: UUID, timeout_seconds: float) -> None:
        del request_id, timeout_seconds
        self.wait_calls += 1
        raise AssertionError("simulation pulse must poll the local queue instead of queue_wait")


class _FakeQueueWorker:
    def __init__(self) -> None:
        self.start_count = 0
        self.wake_count = 0
        self.stop_count = 0

    def start(self) -> None:
        self.start_count += 1

    def wake(self) -> None:
        self.wake_count += 1

    async def stop(self) -> None:
        self.stop_count += 1


def _bridge_status_payload() -> dict[str, JsonValue]:
    asset_sha256 = "b" * 64
    runtime_version_tag = __version__.replace(".", "_").replace("-", "_")
    return {
        "protocol": "cmo-agent-bridge/1",
        "runtime_version": __version__,
        "runtime_tag": f"{runtime_version_tag}-{asset_sha256}",
        "runtime_asset_sha256": asset_sha256,
        "release_id": "c" * 64,
        "build": 1868,
        "manifest_sha256": "a" * 64,
        "lineage_id": "11111111-1111-4111-8111-111111111111",
        "activation_id": "22222222-2222-4222-8222-222222222222",
        "installed_event_names": ["CMOAgentBridge: Poll"],
        "installed_action_names": ["CMOAgentBridge: Poll"],
        "installed_trigger_names": ["CMOAgentBridge: Timer"],
        "pending_request_id": None,
        "quarantined": False,
        "paused_capability": True,
        "poll_interval_seconds": 1,
        "safe_payload_bytes": 65_536,
        "verified_ledger_entries": 32,
        "effective_ledger_capacity": 32,
    }


class _FakeBridgeApplication:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, JsonValue], str | None]] = []

    async def execute(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
        *,
        confirmation_token: str | None = None,
    ) -> InvocationOutcome:
        self.calls.append((operation, dict(arguments), confirmation_token))
        return InvocationOutcome(
            protocol="cmo-agent-bridge/1",
            request_id=None,
            ok=True,
            result=_bridge_status_payload(),
            error=None,
        )


class _ScenarioBridgeApplication:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, JsonValue], str | None]] = []

    async def execute(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
        *,
        confirmation_token: str | None = None,
    ) -> InvocationOutcome:
        self.calls.append((operation, dict(arguments), confirmation_token))
        result: dict[str, JsonValue] = {
            "guid": "SCENARIO-1",
            "title": "Test Scenario",
            "file_name": "test.scen",
            "file_name_path": "Scenarios\\Test",
            "current_time": "2026/7/18 12:00:00",
            "current_time_seconds": 1.0,
            "start_time": "2026/7/18 11:00:00",
            "start_time_seconds": 0.0,
            "duration": "01:00:00",
            "duration_seconds": 3600.0,
            "complexity": 1,
            "difficulty": 1,
            "setting": "Test",
            "database": "DB3000",
            "save_version": "1868",
            "started": True,
            "player_side_guid": "SIDE-BLUE",
            "time_compression": 15.0,
            "campaign_score": 0,
        }
        return InvocationOutcome(
            protocol="cmo-agent-bridge/1",
            request_id=None,
            ok=True,
            result=result,
            error=None,
        )


def _manager(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    controller: _FakeUiTimeController,
    queue_service: _FakeQueueService | None = None,
    application: _FakeBridgeApplication | None = None,
) -> tuple[McpRuntimeManager, _FakeQueueService, _FakeBridgeApplication]:
    game_root = tmp_path / "CMO"
    game_root.mkdir()
    command_exe = game_root / "Command.exe"
    command_exe.write_bytes(b"test")
    lua_root = game_root / "Lua" / "CMOAgentBridge"
    lua_root.mkdir(parents=True)
    import_export = game_root / "ImportExport"
    import_export.mkdir()
    state_root = tmp_path / "state"
    state_root.mkdir()
    paths = FileBridgePaths(
        game_root=game_root,
        root_key="d" * 64,
        command_exe=command_exe,
        lua_root=lua_root,
        inbox=lua_root / "inbox" / "request.lua",
        import_export=import_export,
        lock_file=state_root / "bridge.lock",
        pending_file=state_root / "pending.json",
        sqlite_file=state_root / "state.sqlite3",
    )
    queue = queue_service or _FakeQueueService()
    bridge_application = application or _FakeBridgeApplication()
    runtime = ApplicationRuntime(
        application=cast(BridgeApplication, bridge_application),
        host_quarantine=cast(Any, None),
        queue_service=cast(Any, queue),
        queue_worker=cast(Any, _FakeQueueWorker()),
        config=BridgeConfig(game_root=game_root, request_timeout_seconds=1.0),
        paths=paths,
        runtime_snapshot=create_runtime_snapshot(),
    )

    def fake_build(**_kwargs: object) -> ApplicationRuntime:
        return runtime

    monkeypatch.setattr(runtime_module, "build_application_runtime", fake_build)
    manager = McpRuntimeManager(
        game_root=game_root,
        local_app_data=tmp_path,
        ui_time_controller=controller,
    )
    return manager, queue, bridge_application


@pytest.mark.parametrize(
    ("operation", "arguments"),
    [
        ("bridge.status", {}),
        ("scenario.get", {}),
        ("unit.list", {"side_guid": "SIDE-BLUE"}),
    ],
)
@pytest.mark.asyncio
async def test_paused_cmo_backed_execute_fails_before_publish_or_retry(
    operation: str,
    arguments: dict[str, JsonValue],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.PAUSED, TimeRate.X15))
    manager, _queue, application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
    )

    outcome = await manager.execute(operation, arguments)

    assert outcome.ok is False
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.SCENARIO_NOT_ADVANCING.value
    assert outcome.error["details"] == {
        "operation": operation,
        "observed_state": "paused",
        "rate_code": TimeRate.X15.value,
        "requires_lua_poll": True,
        "retry_suppressed": True,
        "next_tool": "cmo_time_get_state",
        "next_step": (
            "Do not retry while CMO is paused. If fresh state is required, preserve "
            "the current rate, open an explicit 1x read window with cmo_time_set, "
            "complete the planned read batch, and restore pause in cleanup."
        ),
    }
    assert application.calls == []
    assert controller.calls == [("get_state", None)]


@pytest.mark.asyncio
async def test_paused_scenario_context_get_fails_before_its_direct_live_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.PAUSED, TimeRate.X15))
    manager, _queue, application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
    )

    with pytest.raises(BridgeError) as caught:
        await manager.scenario_context_get()

    assert caught.value.code is ErrorCode.SCENARIO_NOT_ADVANCING
    assert caught.value.details is not None
    assert caught.value.details["operation"] == "scenario.get"
    assert caught.value.details["retry_suppressed"] is True
    assert application.calls == []
    assert controller.calls == [("get_state", None)]


@pytest.mark.asyncio
async def test_running_cmo_backed_execute_reaches_bridge_application(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.RUNNING, TimeRate.X15))
    manager, _queue, application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
    )

    outcome = await manager.execute("bridge.status", {})

    assert outcome.ok is True
    assert application.calls == [("bridge.status", {}, None)]
    assert controller.calls == [("get_state", None)]


@pytest.mark.asyncio
async def test_running_scenario_context_get_checks_each_live_identity_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.RUNNING, TimeRate.X15))
    application = _ScenarioBridgeApplication()
    manager, _queue, _application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
        application=cast(_FakeBridgeApplication, application),
    )

    result = await manager.scenario_context_get()

    assert result.status == "file_missing"
    assert application.calls == [
        ("scenario.get", {}, None),
        ("scenario.get", {}, None),
    ]
    assert controller.calls == [
        ("get_state", None),
        ("get_state", None),
    ]


@pytest.mark.asyncio
async def test_time_get_and_idempotent_time_set_only_read_verified_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.PAUSED, TimeRate.X15))
    manager, _queue, _application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
    )

    state = await manager.time_get_state()

    assert state.state is SimulationRunState.PAUSED
    assert state.rate_code == TimeRate.X15.value
    assert controller.calls == [("get_state", None)]

    controller.calls.clear()
    result = await manager.time_set(state="paused", rate_code=TimeRate.X15.value)

    assert result.changed is False
    assert result.before == result.after
    assert controller.calls == [("get_state", None), ("get_state", None)]


@pytest.mark.asyncio
async def test_time_set_pauses_before_rate_change_and_reads_back_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.RUNNING, TimeRate.X15))
    manager, _queue, _application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
    )

    result = await manager.time_set(state="paused", rate_code=TimeRate.X5.value)

    assert result.changed is True
    assert result.before.state is SimulationRunState.RUNNING
    assert result.after.state is SimulationRunState.PAUSED
    assert result.after.rate_code == TimeRate.X5.value
    assert controller.calls == [
        ("get_state", None),
        ("pause", None),
        ("set_rate", TimeRate.X5),
        ("get_state", None),
    ]


@pytest.mark.asyncio
async def test_time_set_resumes_with_requested_rate_in_one_ui_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.PAUSED, TimeRate.X15))
    manager, _queue, _application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
    )

    result = await manager.time_set(state="running", rate_code=TimeRate.X5.value)

    assert result.changed is True
    assert result.after.state is SimulationRunState.RUNNING
    assert result.after.rate_code == TimeRate.X5.value
    assert controller.calls == [
        ("get_state", None),
        ("resume", TimeRate.X5),
        ("get_state", None),
    ]


@pytest.mark.asyncio
async def test_running_simulation_rejects_pulse_without_ui_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.RUNNING, TimeRate.X15))
    manager, _queue, application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
    )

    with pytest.raises(BridgeError) as caught:
        await manager.simulation_pulse(handshake=True)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert controller.calls == [("get_state", None)]
    assert application.calls == []


@pytest.mark.asyncio
async def test_paused_handshake_runs_at_1x_then_pauses_and_restores_prior_rate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.PAUSED, TimeRate.X15))
    manager, _queue, application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
    )

    result = await manager.simulation_pulse(
        handshake=True,
        accept_lineage_id="11111111-1111-4111-8111-111111111111",
    )

    assert result.ok is True
    assert result.released is True
    assert result.handshake is not None
    assert result.handshake.build == 1868
    assert result.final_pause_verified is True
    assert result.prior_rate_restored is True
    assert result.after is not None
    assert result.after.state is SimulationRunState.PAUSED
    assert result.after.rate_code == TimeRate.X15.value
    assert application.calls == [
        (
            "bridge.status",
            {"accept_lineage_id": "11111111-1111-4111-8111-111111111111"},
            None,
        )
    ]
    assert controller.calls == [
        ("get_state", None),
        ("play_1x", None),
        ("pause", None),
        ("set_rate", TimeRate.X15),
    ]


@pytest.mark.asyncio
async def test_request_pulse_polls_only_local_queue_until_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _FakeQueueService(
        {
            _REQUEST_A: (
                _queued_status(_REQUEST_A, OperationQueueState.QUEUED, sequence=1),
                _queued_status(_REQUEST_A, OperationQueueState.COMPLETED, sequence=1),
            )
        }
    )
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.PAUSED, TimeRate.X30))
    manager, queue, application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
        queue_service=queue,
    )

    result = await manager.simulation_pulse(request_ids=(_REQUEST_A,))

    assert result.ok is True
    assert result.requests[0].state is OperationQueueState.COMPLETED
    assert application.calls == []
    assert queue.wait_calls == 0
    assert queue.get_calls.count(_REQUEST_A) >= 3
    assert queue.nonterminal_list_calls == 1
    assert queue.list_calls == 0
    assert controller.calls == [
        ("get_state", None),
        ("play_1x", None),
        ("pause", None),
        ("set_rate", TimeRate.X30),
    ]


@pytest.mark.asyncio
async def test_request_pulse_does_not_materialize_terminal_queue_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _TerminalHistoryPoison(_FakeQueueService):
        def list(self, *, limit: int | None = None) -> QueuedOperationList:
            del limit
            raise AssertionError("pulse must not materialize rejected/quarantined history")

    queue = _TerminalHistoryPoison(
        {
            _REQUEST_A: (
                _queued_status(_REQUEST_A, OperationQueueState.QUEUED, sequence=1),
                _queued_status(_REQUEST_A, OperationQueueState.COMPLETED, sequence=1),
            )
        }
    )
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.PAUSED, TimeRate.X15))
    manager, _queue, _application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
        queue_service=queue,
    )

    result = await manager.simulation_pulse(request_ids=(_REQUEST_A,))

    assert result.ok is True
    assert result.requests[0].state is OperationQueueState.COMPLETED
    assert queue.nonterminal_list_calls == 1


@pytest.mark.asyncio
async def test_request_pulse_arms_fifo_head_even_when_ids_are_reversed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _FakeQueueService(
        {
            _REQUEST_A: (
                _queued_status(_REQUEST_A, OperationQueueState.QUEUED, sequence=1),
                _queued_status(_REQUEST_A, OperationQueueState.COMPLETED, sequence=1),
            ),
            _REQUEST_B: (
                _queued_status(_REQUEST_B, OperationQueueState.QUEUED, sequence=2),
                _queued_status(_REQUEST_B, OperationQueueState.COMPLETED, sequence=2),
            ),
        }
    )
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.PAUSED, TimeRate.X15))
    manager, _queue, _application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
        queue_service=queue,
    )
    armed: list[UUID] = []

    async def capture_armed(
        _runtime: ApplicationRuntime,
        request_id: UUID,
        *,
        timeout_seconds: float,
    ) -> None:
        del timeout_seconds
        armed.append(request_id)

    manager._queue_lifecycle_started = True  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(manager, "_wait_until_first_request_armed", capture_armed)

    result = await manager.simulation_pulse(request_ids=(_REQUEST_B, _REQUEST_A))

    assert result.ok is True
    assert armed == [_REQUEST_A]


@pytest.mark.asyncio
async def test_failed_pulse_start_repauses_and_restores_prior_rate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = BridgeError(ErrorCode.STATE_CONFLICT, "1x verification failed")
    controller = _FakeUiTimeController(
        _ui_state(SimulationRunState.PAUSED, TimeRate.X15),
        play_error=failure,
        play_failure_state=_ui_state(SimulationRunState.PAUSED, TimeRate.X1),
    )
    manager, _queue, _application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
    )

    with pytest.raises(BridgeError, match="1x verification failed"):
        await manager.simulation_pulse(handshake=True)

    assert controller.state.state is SimulationRunState.PAUSED
    assert controller.state.rate is TimeRate.X15
    assert controller.calls == [
        ("get_state", None),
        ("play_1x", None),
        ("pause", None),
        ("set_rate", TimeRate.X15),
    ]


@pytest.mark.asyncio
async def test_request_pulse_timeout_still_pauses_and_restores_prior_rate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _FakeQueueService(
        {_REQUEST_A: (_queued_status(_REQUEST_A, OperationQueueState.ACTIVE, sequence=1),)}
    )
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.PAUSED, TimeRate.X150))
    manager, queue, application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
        queue_service=queue,
    )

    result = await manager.simulation_pulse(
        request_ids=(_REQUEST_A,),
        timeout_seconds=0.01,
    )

    assert result.ok is False
    assert result.timed_out is True
    assert result.requests[0].state is OperationQueueState.ACTIVE
    assert result.final_pause_verified is True
    assert result.prior_rate_restored is True
    assert application.calls == []
    assert queue.wait_calls == 0
    assert controller.calls == [
        ("get_state", None),
        ("play_1x", None),
        ("pause", None),
        ("set_rate", TimeRate.X150),
    ]


@pytest.mark.asyncio
async def test_terminal_only_request_pulse_does_not_release_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _FakeQueueService(
        {_REQUEST_A: (_queued_status(_REQUEST_A, OperationQueueState.COMPLETED, sequence=1),)}
    )
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.PAUSED, TimeRate.X15))
    manager, _queue, _application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
        queue_service=queue,
    )

    result = await manager.simulation_pulse(request_ids=(_REQUEST_A,))

    assert result.ok is True
    assert result.released is False
    assert result.requests[0].state is OperationQueueState.COMPLETED
    assert controller.calls == [("get_state", None)]


@pytest.mark.asyncio
async def test_pulse_rejects_unselected_nonterminal_request_before_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _FakeQueueService(
        {
            _REQUEST_A: (_queued_status(_REQUEST_A, OperationQueueState.ACTIVE, sequence=1),),
            _REQUEST_B: (_queued_status(_REQUEST_B, OperationQueueState.QUEUED, sequence=2),),
        }
    )
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.PAUSED, TimeRate.X15))
    manager, _queue, _application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
        queue_service=queue,
    )

    with pytest.raises(BridgeError) as caught:
        await manager.simulation_pulse(request_ids=(_REQUEST_A,))

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.details["unselected_request_ids"] == [str(_REQUEST_B)]
    assert controller.calls == [("get_state", None)]


@pytest.mark.asyncio
async def test_final_pause_failure_is_returned_as_structured_pulse_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = _FakeUiTimeController(
        _ui_state(SimulationRunState.PAUSED, TimeRate.X15),
        pause_result=_ui_state(SimulationRunState.RUNNING, TimeRate.X1),
    )
    manager, _queue, _application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
    )

    result = await manager.simulation_pulse(handshake=True)

    assert result.ok is False
    assert result.released is True
    assert result.after is None
    assert result.final_pause_verified is False
    assert result.prior_rate_restored is False
    assert result.pause_error is not None
    assert result.pause_error.code is ErrorCode.STATE_CONFLICT
    assert result.rate_restore_error is None
    assert controller.calls == [
        ("get_state", None),
        ("play_1x", None),
        ("pause", None),
    ]


@pytest.mark.asyncio
async def test_cancelled_pulse_repauses_and_restores_before_propagating_cancel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _FakeQueueService(
        {_REQUEST_A: (_queued_status(_REQUEST_A, OperationQueueState.ACTIVE, sequence=1),)}
    )
    controller = _FakeUiTimeController(_ui_state(SimulationRunState.PAUSED, TimeRate.X30))
    manager, _queue, _application = _manager(
        tmp_path,
        monkeypatch,
        controller=controller,
        queue_service=queue,
    )
    task = asyncio.create_task(
        manager.simulation_pulse(request_ids=(_REQUEST_A,), timeout_seconds=120.0)
    )
    await asyncio.wait_for(controller.played.wait(), timeout=1.0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)
    assert controller.state.state is SimulationRunState.PAUSED
    assert controller.state.rate is TimeRate.X30
    assert ("pause", None) in controller.calls
    assert ("set_rate", TimeRate.X30) in controller.calls


class _FakeMcpTimeApplication:
    def __init__(self) -> None:
        self.state = McpTimeState(
            state=SimulationRunState.PAUSED,
            rate_code=TimeRate.X15.value,
            multiplier=TimeRate.X15.multiplier,
            process_pid=4321,
            process_create_time=1234.5,
            window_handle=9876,
            window_title="CMO",
        )
        self.calls: list[tuple[str, object]] = []

    async def time_get_state(self) -> McpTimeState:
        self.calls.append(("get", None))
        return self.state

    async def time_set(
        self,
        *,
        state: Literal["paused", "running"],
        rate_code: int | None = None,
    ) -> McpTimeSetResult:
        self.calls.append(("set", (state, rate_code)))
        before = self.state
        code = before.rate_code if rate_code is None else rate_code
        after = before.model_copy(
            update={
                "state": SimulationRunState(state),
                "rate_code": code,
                "multiplier": TimeRate(code).multiplier,
            }
        )
        self.state = after
        return McpTimeSetResult(before=before, after=after, changed=before != after)

    async def simulation_pulse(
        self,
        *,
        request_ids: tuple[UUID, ...] = (),
        handshake: bool = False,
        accept_lineage_id: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> McpSimulationPulseResult:
        self.calls.append(
            (
                "pulse",
                (request_ids, handshake, accept_lineage_id, timeout_seconds),
            )
        )
        return McpSimulationPulseResult(
            ok=True,
            released=True,
            before=self.state,
            after=self.state,
            requests=(),
            handshake=None,
            timed_out=False,
            final_pause_verified=True,
            prior_rate_restored=True,
            work_error=None,
            pause_error=None,
            rate_restore_error=None,
            elapsed_seconds=0.1,
        )


def _structured(value: object) -> dict[str, JsonValue]:
    assert isinstance(value, tuple)
    pair = cast(tuple[object, ...], value)
    assert len(pair) == 2
    _content, structured = pair
    assert isinstance(structured, dict)
    return cast(dict[str, JsonValue], structured)


@pytest.mark.asyncio
async def test_mcp_time_tools_forward_validated_arguments_and_return_structured_models() -> None:
    application = _FakeMcpTimeApplication()
    server = create_mcp_server(cast(McpApplicationPort, application))

    get_result = _structured(await server.call_tool("cmo_time_get_state", {}))
    set_result = _structured(
        await server.call_tool(
            "cmo_time_set",
            {"state": "running", "rate_code": TimeRate.X5.value},
        )
    )
    pulse_result = _structured(
        await server.call_tool(
            "cmo_simulation_pulse",
            {
                "request_ids": [str(_REQUEST_A)],
                "handshake": True,
                "accept_lineage_id": "11111111-1111-4111-8111-111111111111",
                "timeout_seconds": 1.5,
            },
        )
    )

    assert get_result["state"] == "paused"
    assert set_result["changed"] is True
    assert pulse_result["released"] is True
    assert application.calls == [
        ("get", None),
        ("set", ("running", TimeRate.X5.value)),
        (
            "pulse",
            (
                (_REQUEST_A,),
                True,
                "11111111-1111-4111-8111-111111111111",
                1.5,
            ),
        ),
    ]
