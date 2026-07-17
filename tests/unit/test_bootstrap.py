from pathlib import Path
from typing import Any, cast

import pytest

from cmo_agent_bridge.application.service import BridgeApplication
from cmo_agent_bridge.bootstrap import (
    POLL_ACTION_SCRIPT,
    TrustedLocalPolicy,
    build_application_runtime,
    prepare_bridge,
)
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.lua_delivery import render_idle_lua
from cmo_agent_bridge.runtime_bundle import render_dispatcher
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY


def _game_root(tmp_path: Path) -> Path:
    root = tmp_path / "CMO"
    root.mkdir()
    (root / "Command.exe").write_bytes(b"test marker")
    (root / "Lua").mkdir()
    (root / "ImportExport").mkdir()
    return root


def test_prepare_deploys_release_bound_runtime_and_builds_application(tmp_path: Path) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()

    prepared = prepare_bridge(
        game_root=game_root,
        local_app_data=local_app_data,
    )

    assert prepared.dispatcher_path.read_bytes() == render_dispatcher(prepared.runtime_snapshot)
    assert prepared.paths.inbox.read_bytes() == render_idle_lua()
    assert prepared.poll_path.read_text(encoding="ascii") == POLL_ACTION_SCRIPT + "\n"
    assert (local_app_data / "CMOAgentBridge" / "config.toml").is_file()

    runtime = build_application_runtime(local_app_data=local_app_data)
    assert type(runtime.application) is BridgeApplication
    assert runtime.paths == prepared.paths
    assert runtime.runtime_snapshot == prepared.runtime_snapshot
    assert runtime.paths.sqlite_file.is_file()


def test_build_application_rejects_a_drifted_dispatcher(tmp_path: Path) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    prepared = prepare_bridge(game_root=game_root, local_app_data=local_app_data)
    prepared.dispatcher_path.write_bytes(b"return false\n")

    with pytest.raises(BridgeError) as caught:
        build_application_runtime(local_app_data=local_app_data)

    assert caught.value.code is ErrorCode.BRIDGE_NOT_PREPARED


@pytest.mark.parametrize(
    ("allow_mutations", "allow_destructive", "denied_setting"),
    [
        (False, True, "allow_mutations"),
        (True, False, "allow_destructive"),
    ],
)
def test_trusted_local_policy_gates_confirmed_deletes_with_local_settings(
    allow_mutations: bool,
    allow_destructive: bool,
    denied_setting: str,
) -> None:
    policy = TrustedLocalPolicy(
        allow_mutations=allow_mutations,
        allow_destructive=allow_destructive,
    )

    with pytest.raises(BridgeError) as caught:
        policy.ensure_destructive_allowed(
            status=cast(Any, None),
            contract=OPERATION_REGISTRY.resolve("unit.delete"),
            runtime_snapshot=cast(Any, None),
        )

    assert caught.value.code is ErrorCode.POLICY_DENIED
    assert caught.value.details["setting"] == denied_setting


def test_trusted_local_policy_allows_confirmed_delete_when_enabled() -> None:
    policy = TrustedLocalPolicy(allow_mutations=True, allow_destructive=True)

    policy.ensure_destructive_allowed(
        status=cast(Any, None),
        contract=OPERATION_REGISTRY.resolve("mission.delete"),
        runtime_snapshot=cast(Any, None),
    )
