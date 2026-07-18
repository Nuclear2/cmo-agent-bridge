from __future__ import annotations

import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import JsonValue

import cmo_agent_bridge.mcp_runtime as runtime_module
from cmo_agent_bridge.application.models import InvocationOutcome
from cmo_agent_bridge.application.queue_models import (
    CancelQueuedOperationResult,
    QueueSummary,
    QueueWaitResult,
    QueuedOperationList,
    QueuedOperationReceipt,
    QueuedOperationStatus,
)
from cmo_agent_bridge.application.service import BridgeApplication
from cmo_agent_bridge.bootstrap import ApplicationRuntime, PreparedBridge
from cmo_agent_bridge.config import BridgeConfig
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.mcp_runtime import McpRuntimeManager
from cmo_agent_bridge.mcp_server import create_mcp_server
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.runtime_bundle import create_runtime_snapshot
from cmo_agent_bridge.scenario_context import ScenarioContext, ScenarioScoring
from cmo_agent_bridge.state.operation_queue import OperationQueueState
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths


_QUEUE_REQUEST_ID = UUID("00000000-0000-0000-0000-000000000456")


def _game_root(tmp_path: Path) -> Path:
    root = tmp_path / "CMO"
    root.mkdir()
    (root / "Command.exe").write_bytes(b"test marker")
    (root / "Lua").mkdir()
    (root / "ImportExport").mkdir()
    return root


class _ScoreApplication:
    async def execute(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
        *,
        confirmation_token: str | None = None,
    ) -> InvocationOutcome:
        del operation, arguments, confirmation_token
        result: JsonValue = {"side": "Blue", "score": 42}
        return InvocationOutcome(
            protocol="cmo-agent-bridge/1",
            request_id=None,
            ok=True,
            result=result,
            error=None,
        )


class _BlockingApplication:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
        *,
        confirmation_token: str | None = None,
    ) -> InvocationOutcome:
        del operation, arguments, confirmation_token
        self.started.set()
        await self.release.wait()
        return InvocationOutcome(
            protocol="cmo-agent-bridge/1",
            request_id=None,
            ok=True,
            result={"released": True},
            error=None,
        )


def _scenario_payload(
    *,
    player_side_guid: str | None = "SIDE-BLUE",
    started: bool = True,
) -> dict[str, JsonValue]:
    return {
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
        "started": started,
        "player_side_guid": player_side_guid,
        "time_compression": 1.0,
        "campaign_score": 0,
    }


class _ScenarioApplication:
    def __init__(self, scenarios: list[dict[str, JsonValue]]) -> None:
        self._scenarios = scenarios
        self.calls = 0

    async def execute(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
        *,
        confirmation_token: str | None = None,
    ) -> InvocationOutcome:
        assert operation == "scenario.get"
        assert arguments == {}
        assert confirmation_token is None
        scenario = self._scenarios[min(self.calls, len(self._scenarios) - 1)]
        self.calls += 1
        return InvocationOutcome(
            protocol="cmo-agent-bridge/1",
            request_id=None,
            ok=True,
            result=scenario,
            error=None,
        )


class _ScenarioContextReader:
    def __init__(self, result: ScenarioContext) -> None:
        self.result = result
        self.calls: list[tuple[Path, str, str, str]] = []

    async def read(
        self,
        *,
        game_root: Path,
        file_name_path: str,
        file_name: str,
        player_side_guid: str,
    ) -> ScenarioContext:
        self.calls.append((game_root, file_name_path, file_name, player_side_guid))
        return self.result


class _FakeQueueService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.status = QueuedOperationStatus(
            request_id=_QUEUE_REQUEST_ID,
            operation="mission.create",
            sequence=3,
            state=OperationQueueState.QUEUED,
            submitted_at_ms=10,
        )

    def submit(
        self,
        *,
        operation: str,
        arguments: Mapping[str, JsonValue],
    ) -> QueuedOperationReceipt:
        self.calls.append(("submit", (operation, dict(arguments))))
        return QueuedOperationReceipt(
            request_id=_QUEUE_REQUEST_ID,
            operation=operation,
            sequence=3,
            state=OperationQueueState.QUEUED,
            submitted_at_ms=10,
        )

    def get(self, *, request_id: UUID) -> QueuedOperationStatus:
        self.calls.append(("get", request_id))
        return self.status

    async def wait(
        self,
        *,
        request_id: UUID,
        timeout_seconds: float,
    ) -> QueueWaitResult:
        self.calls.append(("wait", (request_id, timeout_seconds)))
        return QueueWaitResult(operation=self.status, timed_out=True)

    def list(self, *, limit: int | None = None) -> QueuedOperationList:
        self.calls.append(("list", limit))
        return QueuedOperationList(items=(self.status,))

    def cancel(self, *, request_id: UUID) -> CancelQueuedOperationResult:
        self.calls.append(("cancel", request_id))
        return CancelQueuedOperationResult(operation=self.status, cancelled=False)

    def summary(self) -> QueueSummary:
        self.calls.append(("summary", None))
        return QueueSummary(
            queued=1,
            active=0,
            completed=0,
            rejected=0,
            quarantined=0,
            cancelled=0,
        )


class _FakeQueueWorker:
    def __init__(self) -> None:
        self.start_count = 0
        self.wake_count = 0
        self.stop_count = 0
        self.running = False

    def start(self) -> None:
        self.start_count += 1
        self.running = True

    def wake(self) -> None:
        self.wake_count += 1

    async def stop(self) -> None:
        self.stop_count += 1
        self.running = False


class _SlowStopQueueWorker(_FakeQueueWorker):
    def __init__(self) -> None:
        super().__init__()
        self.stop_started = asyncio.Event()
        self.release_stop = asyncio.Event()

    async def stop(self) -> None:
        self.stop_count += 1
        self.stop_started.set()
        await self.release_stop.wait()
        self.running = False


def _fake_runtime(
    *,
    application: object,
    queue_service: _FakeQueueService,
    queue_worker: _FakeQueueWorker,
    paths: FileBridgePaths,
    snapshot: RuntimeSnapshot,
) -> ApplicationRuntime:
    return ApplicationRuntime(
        application=cast(BridgeApplication, application),
        host_quarantine=cast(Any, None),
        queue_service=cast(Any, queue_service),
        queue_worker=cast(Any, queue_worker),
        config=BridgeConfig(game_root=paths.game_root),
        paths=paths,
        runtime_snapshot=snapshot,
    )


def _structured(value: object) -> dict[str, JsonValue]:
    assert isinstance(value, tuple)
    pair = cast(tuple[object, ...], value)
    assert len(pair) == 2
    _content, structured = pair
    assert isinstance(structured, dict)
    return cast(dict[str, JsonValue], structured)


@pytest.mark.asyncio
async def test_scenario_context_get_combines_live_identity_with_saved_player_briefing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    application = _ScenarioApplication([_scenario_payload(), _scenario_payload()])
    reader = _ScenarioContextReader(
        ScenarioContext(
            available=True,
            scenario_description="Regional crisis background.",
            player_side_guid="SIDE-BLUE",
            player_side_name="Blue",
            side_briefing="Destroy all hostile fighters.",
            scoring=ScenarioScoring(-100, -50, 0, 50, 100),
            description_truncated=False,
            briefing_truncated=False,
            warnings=(),
            unavailable_reason=None,
        )
    )
    runtime = _fake_runtime(
        application=application,
        queue_service=_FakeQueueService(),
        queue_worker=_FakeQueueWorker(),
        paths=paths,
        snapshot=create_runtime_snapshot(),
    )

    def fake_build(**_kwargs: object) -> ApplicationRuntime:
        return runtime

    monkeypatch.setattr(runtime_module, "build_application_runtime", fake_build)
    manager = McpRuntimeManager(
        game_root=game_root,
        local_app_data=local_app_data,
        scenario_context_reader=reader,
    )

    result = await manager.scenario_context_get()

    assert result.status == "available"
    assert result.player_side_guid == "SIDE-BLUE"
    assert result.player_side_name == "Blue"
    assert result.scenario_description == "Regional crisis background."
    assert result.side_briefing == "Destroy all hostile fighters."
    assert result.scoring_thresholds is not None
    assert result.scoring_thresholds.major_victory == 100
    assert result.saved_snapshot is True
    assert application.calls == 2
    assert reader.calls == [(paths.game_root, "Scenarios\\Test", "test.scen", "SIDE-BLUE")]


@pytest.mark.asyncio
async def test_scenario_context_get_discards_briefing_when_player_side_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    application = _ScenarioApplication(
        [
            _scenario_payload(player_side_guid="SIDE-BLUE"),
            _scenario_payload(player_side_guid="SIDE-RED"),
        ]
    )
    reader = _ScenarioContextReader(
        ScenarioContext(
            available=True,
            scenario_description="Background",
            player_side_guid="SIDE-BLUE",
            player_side_name="Blue",
            side_briefing="Blue-only orders",
            scoring=ScenarioScoring(-100, -50, 0, 50, 100),
            description_truncated=False,
            briefing_truncated=False,
            warnings=(),
            unavailable_reason=None,
        )
    )
    runtime = _fake_runtime(
        application=application,
        queue_service=_FakeQueueService(),
        queue_worker=_FakeQueueWorker(),
        paths=paths,
        snapshot=create_runtime_snapshot(),
    )

    def fake_build(**_kwargs: object) -> ApplicationRuntime:
        return runtime

    monkeypatch.setattr(runtime_module, "build_application_runtime", fake_build)
    manager = McpRuntimeManager(
        game_root=game_root,
        local_app_data=local_app_data,
        scenario_context_reader=reader,
    )

    result = await manager.scenario_context_get()

    assert result.status == "scenario_changed"
    assert result.player_side_guid == "SIDE-RED"
    assert result.side_briefing is None
    assert result.scenario_description is None
    assert result.saved_snapshot is False


@pytest.mark.asyncio
async def test_scenario_context_get_discards_briefing_when_play_mode_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    application = _ScenarioApplication(
        [_scenario_payload(started=False), _scenario_payload(started=True)]
    )
    reader = _ScenarioContextReader(
        ScenarioContext(
            available=True,
            scenario_description="Background",
            player_side_guid="SIDE-BLUE",
            player_side_name="Blue",
            side_briefing="Orders",
            scoring=ScenarioScoring(-100, -50, 0, 50, 100),
            description_truncated=False,
            briefing_truncated=False,
            warnings=(),
            unavailable_reason=None,
        )
    )
    runtime = _fake_runtime(
        application=application,
        queue_service=_FakeQueueService(),
        queue_worker=_FakeQueueWorker(),
        paths=paths,
        snapshot=create_runtime_snapshot(),
    )

    def fake_build(**_kwargs: object) -> ApplicationRuntime:
        return runtime

    monkeypatch.setattr(runtime_module, "build_application_runtime", fake_build)
    manager = McpRuntimeManager(
        game_root=game_root,
        local_app_data=local_app_data,
        scenario_context_reader=reader,
    )

    result = await manager.scenario_context_get()

    assert result.status == "scenario_changed"
    assert result.side_briefing is None
    assert result.saved_snapshot is False


@pytest.mark.asyncio
async def test_unconfigured_manager_is_diagnosable_and_returns_structured_failure(
    tmp_path: Path,
) -> None:
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    manager = McpRuntimeManager(local_app_data=local_app_data)
    server = create_mcp_server(manager)

    diagnostic = await manager.diagnose()
    outcome = await manager.execute("scenario.get", {})
    diagnostic_response = await server.call_tool("cmo_bridge_diagnose", {})
    with pytest.raises(ToolError) as caught:
        await server.call_tool("cmo_bridge_prepare", {})

    assert diagnostic.runtime_state == "unconfigured"
    assert diagnostic.error_code == ErrorCode.GAME_ROOT_INVALID.value
    assert "cmo_bridge_prepare" in diagnostic.required_next_action
    assert not outcome.ok
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.GAME_ROOT_INVALID.value
    assert _structured(diagnostic_response)["runtime_state"] == "unconfigured"
    assert ErrorCode.GAME_ROOT_INVALID.value in str(caught.value)
    assert not (local_app_data / "CMOAgentBridge").exists()


@pytest.mark.asyncio
async def test_prepare_hot_activates_the_same_mcp_server(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    snapshot = create_runtime_snapshot()
    prepared = PreparedBridge(
        config=BridgeConfig(game_root=paths.game_root),
        paths=paths,
        runtime_snapshot=snapshot,
        dispatcher_path=paths.lua_root / "versions" / snapshot.runtime_tag / "dispatcher.lua",
        poll_path=paths.lua_root / "poll.lua",
    )
    calls = {"prepare": 0, "build": 0}
    deployed = False

    def fake_prepare(**_kwargs: object) -> PreparedBridge:
        nonlocal deployed
        calls["prepare"] += 1
        deployed = True
        return prepared

    def fake_build(**_kwargs: object) -> ApplicationRuntime:
        calls["build"] += 1
        if not deployed:
            raise BridgeError(ErrorCode.BRIDGE_NOT_PREPARED, "runtime is not prepared")
        return ApplicationRuntime(
            application=cast(BridgeApplication, _ScoreApplication()),
            host_quarantine=cast(Any, None),
            queue_service=cast(Any, None),
            queue_worker=cast(Any, None),
            config=prepared.config,
            paths=paths,
            runtime_snapshot=snapshot,
        )

    monkeypatch.setattr(runtime_module, "prepare_bridge", fake_prepare)
    monkeypatch.setattr(runtime_module, "build_application_runtime", fake_build)
    manager = McpRuntimeManager(local_app_data=local_app_data)
    server = create_mcp_server(manager)
    tools_before = {tool.name for tool in await server.list_tools()}

    with pytest.raises(ToolError) as caught:
        await server.call_tool("cmo_score_get", {"side": "Blue"})
    assert ErrorCode.BRIDGE_NOT_PREPARED.value in str(caught.value)

    prepare_response = await server.call_tool(
        "cmo_bridge_prepare",
        {"game_root": str(game_root)},
    )
    score_response = await server.call_tool("cmo_score_get", {"side": "Blue"})

    assert _structured(prepare_response)["ready"] is True
    assert _structured(score_response) == {"side": "Blue", "score": 42}
    assert {tool.name for tool in await server.list_tools()} == tools_before
    assert calls == {"prepare": 1, "build": 2}


@pytest.mark.asyncio
async def test_concurrent_prepare_is_single_flight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    snapshot = create_runtime_snapshot()
    prepared = PreparedBridge(
        config=BridgeConfig(game_root=paths.game_root),
        paths=paths,
        runtime_snapshot=snapshot,
        dispatcher_path=paths.lua_root / "versions" / snapshot.runtime_tag / "dispatcher.lua",
        poll_path=paths.lua_root / "poll.lua",
    )
    calls = {"prepare": 0, "build": 0}

    def fake_prepare(**_kwargs: object) -> PreparedBridge:
        calls["prepare"] += 1
        return prepared

    def fake_build(**_kwargs: object) -> ApplicationRuntime:
        calls["build"] += 1
        return ApplicationRuntime(
            application=cast(BridgeApplication, _ScoreApplication()),
            host_quarantine=cast(Any, None),
            queue_service=cast(Any, None),
            queue_worker=cast(Any, None),
            config=prepared.config,
            paths=paths,
            runtime_snapshot=snapshot,
        )

    monkeypatch.setattr(runtime_module, "prepare_bridge", fake_prepare)
    monkeypatch.setattr(runtime_module, "build_application_runtime", fake_build)
    manager = McpRuntimeManager(local_app_data=local_app_data)

    first, second = await asyncio.gather(
        manager.prepare(game_root=str(game_root)),
        manager.prepare(game_root=str(game_root)),
    )

    assert first == second
    assert calls == {"prepare": 1, "build": 1}


@pytest.mark.asyncio
async def test_queue_surface_delegates_and_wakes_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    queue = _FakeQueueService()
    worker = _FakeQueueWorker()
    runtime = _fake_runtime(
        application=_ScoreApplication(),
        queue_service=queue,
        queue_worker=worker,
        paths=paths,
        snapshot=create_runtime_snapshot(),
    )

    def fake_build(**_kwargs: object) -> ApplicationRuntime:
        return runtime

    monkeypatch.setattr(runtime_module, "build_application_runtime", fake_build)
    manager = McpRuntimeManager(game_root=game_root, local_app_data=local_app_data)
    await manager.start_queue_worker()

    receipt = await manager.submit("mission.create", {"side": "Blue"})
    status = await manager.queue_get(_QUEUE_REQUEST_ID)
    waited = await manager.queue_wait(_QUEUE_REQUEST_ID, 0.5)
    listed = await manager.queue_list(10)
    cancelled = await manager.queue_cancel(_QUEUE_REQUEST_ID)
    summary = await manager.queue_summary()

    assert receipt.request_id == _QUEUE_REQUEST_ID
    assert status == queue.status
    assert waited.timed_out is True
    assert listed.items == (queue.status,)
    assert cancelled.cancelled is False
    assert summary.queued == 1
    assert queue.calls == [
        ("submit", ("mission.create", {"side": "Blue"})),
        ("get", _QUEUE_REQUEST_ID),
        ("wait", (_QUEUE_REQUEST_ID, 0.5)),
        ("list", 10),
        ("cancel", _QUEUE_REQUEST_ID),
        ("summary", None),
    ]
    assert worker.start_count == 6
    assert worker.wake_count == 2


@pytest.mark.asyncio
async def test_queue_lifecycle_is_lazy_and_stops_built_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    queue = _FakeQueueService()
    worker = _FakeQueueWorker()
    runtime = _fake_runtime(
        application=_ScoreApplication(),
        queue_service=queue,
        queue_worker=worker,
        paths=paths,
        snapshot=create_runtime_snapshot(),
    )
    build_count = 0

    def fake_build(**_kwargs: object) -> ApplicationRuntime:
        nonlocal build_count
        build_count += 1
        return runtime

    monkeypatch.setattr(runtime_module, "build_application_runtime", fake_build)
    manager = McpRuntimeManager(game_root=game_root, local_app_data=local_app_data)

    await manager.start_queue_worker()
    assert build_count == 0
    assert worker.start_count == 0

    summary = await manager.queue_summary()
    await manager.stop_queue_worker()

    assert summary.queued == 1
    assert build_count == 1
    assert worker.start_count == 1
    assert worker.stop_count == 1


@pytest.mark.asyncio
async def test_start_waits_for_slow_stop_before_restarting_queue_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    worker = _SlowStopQueueWorker()
    runtime = _fake_runtime(
        application=_ScoreApplication(),
        queue_service=_FakeQueueService(),
        queue_worker=worker,
        paths=paths,
        snapshot=create_runtime_snapshot(),
    )

    def fake_build(**_kwargs: object) -> ApplicationRuntime:
        return runtime

    monkeypatch.setattr(runtime_module, "build_application_runtime", fake_build)
    manager = McpRuntimeManager(game_root=game_root, local_app_data=local_app_data)
    await manager.start_queue_worker()
    await manager.queue_summary()
    assert worker.start_count == 1

    stopping = asyncio.create_task(manager.stop_queue_worker())
    await asyncio.wait_for(worker.stop_started.wait(), timeout=0.25)
    restarting = asyncio.create_task(manager.start_queue_worker())
    await asyncio.sleep(0)
    restart_waited_for_stop = not restarting.done()

    worker.release_stop.set()
    await asyncio.wait_for(asyncio.gather(stopping, restarting), timeout=0.25)

    assert restart_waited_for_stop
    assert worker.stop_count == 1
    assert worker.start_count == 2
    assert worker.running is True


@pytest.mark.asyncio
async def test_pending_cmo_exchange_does_not_hold_runtime_manager_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    application = _BlockingApplication()
    queue = _FakeQueueService()
    worker = _FakeQueueWorker()
    runtime = _fake_runtime(
        application=application,
        queue_service=queue,
        queue_worker=worker,
        paths=paths,
        snapshot=create_runtime_snapshot(),
    )

    def fake_build(**_kwargs: object) -> ApplicationRuntime:
        return runtime

    monkeypatch.setattr(runtime_module, "build_application_runtime", fake_build)
    manager = McpRuntimeManager(game_root=game_root, local_app_data=local_app_data)

    exchange = asyncio.create_task(manager.execute("scenario.get", {}))
    await application.started.wait()
    try:
        summary = await asyncio.wait_for(manager.queue_summary(), timeout=0.25)
    finally:
        application.release.set()
    outcome = await exchange

    assert summary.queued == 1
    assert outcome.ok is True


@pytest.mark.parametrize("action", ["submit", "cancel"])
@pytest.mark.asyncio
async def test_shutdown_racing_queue_change_cannot_restart_inactive_worker(
    action: Literal["submit", "cancel"],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    queue = _FakeQueueService()
    worker = _FakeQueueWorker()
    runtime = _fake_runtime(
        application=_ScoreApplication(),
        queue_service=queue,
        queue_worker=worker,
        paths=paths,
        snapshot=create_runtime_snapshot(),
    )

    def fake_build(**_kwargs: object) -> ApplicationRuntime:
        return runtime

    monkeypatch.setattr(runtime_module, "build_application_runtime", fake_build)
    manager = McpRuntimeManager(game_root=game_root, local_app_data=local_app_data)
    await manager.start_queue_worker()
    await manager.queue_summary()

    reached_after_ensure = asyncio.Event()
    release_queue_change = asyncio.Event()
    original_ensure = McpRuntimeManager._ensure_runtime  # pyright: ignore[reportPrivateUsage]

    async def gated_ensure(selected: McpRuntimeManager) -> ApplicationRuntime:
        selected_runtime = await original_ensure(selected)
        reached_after_ensure.set()
        await release_queue_change.wait()
        return selected_runtime

    monkeypatch.setattr(McpRuntimeManager, "_ensure_runtime", gated_ensure)

    async def change_queue() -> None:
        if action == "submit":
            await manager.submit("mission.create", {"side": "Blue"})
        else:
            await manager.queue_cancel(_QUEUE_REQUEST_ID)

    queue_change = asyncio.create_task(change_queue())

    await asyncio.wait_for(reached_after_ensure.wait(), timeout=0.25)
    await asyncio.wait_for(manager.stop_queue_worker(), timeout=0.25)
    starts_after_stop = worker.start_count
    wakes_after_stop = worker.wake_count
    release_queue_change.set()
    await asyncio.wait_for(queue_change, timeout=0.25)

    assert worker.running is False
    assert worker.stop_count == 1
    assert worker.start_count == starts_after_stop
    assert worker.wake_count == wakes_after_stop
