from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import JsonValue

import cmo_agent_bridge.mcp_runtime as runtime_module
from cmo_agent_bridge.application.models import InvocationOutcome
from cmo_agent_bridge.application.service import BridgeApplication
from cmo_agent_bridge.bootstrap import ApplicationRuntime, PreparedBridge
from cmo_agent_bridge.config import BridgeConfig
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.mcp_runtime import McpRuntimeManager
from cmo_agent_bridge.mcp_server import create_mcp_server
from cmo_agent_bridge.runtime_bundle import create_runtime_snapshot
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths


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
        arguments: object,
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


def _structured(value: object) -> dict[str, JsonValue]:
    assert isinstance(value, tuple)
    pair = cast(tuple[object, ...], value)
    assert len(pair) == 2
    _content, structured = pair
    assert isinstance(structured, dict)
    return cast(dict[str, JsonValue], structured)


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
