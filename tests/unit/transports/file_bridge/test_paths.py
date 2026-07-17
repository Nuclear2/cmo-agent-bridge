import hashlib
import ntpath
import os
import subprocess
from pathlib import Path
from uuid import UUID

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.transports.file_bridge.paths import (
    FileBridgePaths,
    _root_key,  # pyright: ignore[reportPrivateUsage]
)


def _make_cmo_root(parent: Path, name: str = "CMO") -> Path:
    root = parent / name
    root.mkdir()
    (root / "Command.exe").write_bytes(b"command")
    (root / "Lua").mkdir()
    (root / "ImportExport").mkdir()
    return root


def _make_directory_link(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        completed = subprocess.run(
            ["cmd.exe", "/d", "/c", "mklink", "/J", str(link), str(target)],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise AssertionError(completed.stderr or completed.stdout)


def test_build_derives_only_managed_paths(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()

    paths = FileBridgePaths.build(game_root, local_app_data)
    normalized = ntpath.normcase(ntpath.normpath(str(game_root.resolve(strict=True))))

    assert paths.game_root == game_root.resolve(strict=True)
    assert paths.root_key == hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    assert paths.command_exe == paths.game_root / "Command.exe"
    assert paths.lua_root == paths.game_root / "Lua" / "CMOAgentBridge"
    assert paths.inbox == paths.lua_root / "inbox" / "request.lua"
    assert paths.import_export == paths.game_root / "ImportExport"
    assert paths.lock_file == local_app_data.resolve() / "CMOAgentBridge" / "locks" / (
        paths.root_key + ".lock"
    )
    assert paths.pending_file == local_app_data.resolve() / "CMOAgentBridge" / "pending" / (
        paths.root_key + ".json"
    )
    assert paths.sqlite_file == local_app_data.resolve() / "CMOAgentBridge" / "state.sqlite3"


def test_response_path_is_derived_only_from_uuid(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    request_id = UUID("550e8400-e29b-41d4-a716-446655440000")

    assert paths.response_path(request_id) == (
        paths.import_export / "CMOAgentBridge_Response_550e8400-e29b-41d4-a716-446655440000.inst"
    )


def test_case_dot_and_trailing_separator_have_one_root_identity(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path, "MixedCaseRoot")
    dot_child = game_root / "DotChild"
    dot_child.mkdir()
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    aliases = (
        game_root,
        dot_child / "..",
        Path(str(game_root) + os.sep),
        Path(str(game_root).swapcase()),
        Path("\\\\?\\" + str(game_root.resolve(strict=True))),
    )

    built = tuple(FileBridgePaths.build(alias, local_app_data) for alias in aliases)

    assert {item.game_root for item in built} == {game_root.resolve(strict=True)}
    assert len({item.root_key for item in built}) == 1


def test_extended_unc_namespace_has_same_root_key() -> None:
    ordinary = Path(r"\\server\share\CMO")
    extended = Path(r"\\?\UNC\server\share\CMO")

    assert _root_key(ordinary) == _root_key(extended)


@pytest.mark.parametrize("marker", ["Command.exe", "Lua", "ImportExport"])
def test_wrong_marker_type_is_rejected(tmp_path: Path, marker: str) -> None:
    game_root = _make_cmo_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    path = game_root / marker
    if path.is_dir():
        path.rmdir()
        path.write_bytes(b"wrong")
    else:
        path.unlink()
        path.mkdir()

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(game_root, local_app_data)

    assert caught.value.code is ErrorCode.GAME_ROOT_INVALID


def test_missing_game_root_is_rejected_without_creating_it(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(missing, local_app_data)

    assert caught.value.code is ErrorCode.GAME_ROOT_INVALID
    assert not missing.exists()


def test_marker_link_escaping_game_root_is_rejected(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    outside = tmp_path / "outside-lua"
    outside.mkdir()
    (game_root / "Lua").rmdir()
    _make_directory_link(game_root / "Lua", outside)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(game_root, local_app_data)

    assert caught.value.code is ErrorCode.GAME_ROOT_INVALID


def test_managed_lua_child_link_escaping_game_root_is_rejected(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    outside = tmp_path / "outside-bridge"
    outside.mkdir()
    _make_directory_link(game_root / "Lua" / "CMOAgentBridge", outside)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(game_root, local_app_data)

    assert caught.value.code is ErrorCode.GAME_ROOT_INVALID


def test_managed_lua_root_cannot_redirect_to_sibling(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    sibling = game_root / "Lua" / "OtherBridge"
    sibling.mkdir()
    _make_directory_link(game_root / "Lua" / "CMOAgentBridge", sibling)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(game_root, local_app_data)

    assert caught.value.code is ErrorCode.GAME_ROOT_INVALID


def test_inbox_link_cannot_redirect_outside_managed_lua_root(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    bridge_root = game_root / "Lua" / "CMOAgentBridge"
    bridge_root.mkdir()
    redirected = game_root / "RedirectedInbox"
    redirected.mkdir()
    _make_directory_link(bridge_root / "inbox", redirected)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(game_root, local_app_data)

    assert caught.value.code is ErrorCode.GAME_ROOT_INVALID


def test_inbox_link_cannot_collapse_into_managed_lua_root(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    bridge_root = game_root / "Lua" / "CMOAgentBridge"
    bridge_root.mkdir()
    _make_directory_link(bridge_root / "inbox", bridge_root)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(game_root, local_app_data)

    assert caught.value.code is ErrorCode.GAME_ROOT_INVALID


def test_inbox_cannot_redirect_to_versions_sibling(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    bridge_root = game_root / "Lua" / "CMOAgentBridge"
    bridge_root.mkdir()
    versions = bridge_root / "versions"
    versions.mkdir()
    _make_directory_link(bridge_root / "inbox", versions)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(game_root, local_app_data)

    assert caught.value.code is ErrorCode.GAME_ROOT_INVALID


def test_local_state_link_escape_is_rejected(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    outside = tmp_path / "outside-state"
    outside.mkdir()
    _make_directory_link(local_app_data / "CMOAgentBridge", outside)

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(game_root, local_app_data)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_managed_state_root_cannot_redirect_within_local_app_data(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    redirected = local_app_data / "RedirectedState"
    redirected.mkdir()
    _make_directory_link(local_app_data / "CMOAgentBridge", redirected)

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(game_root, local_app_data)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_lock_directory_link_cannot_redirect_outside_managed_state_root(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    state_root = local_app_data / "CMOAgentBridge"
    state_root.mkdir()
    redirected = local_app_data / "RedirectedLocks"
    redirected.mkdir()
    _make_directory_link(state_root / "locks", redirected)

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(game_root, local_app_data)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_lock_directory_cannot_redirect_to_pending_sibling(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    state_root = local_app_data / "CMOAgentBridge"
    state_root.mkdir()
    pending = state_root / "pending"
    pending.mkdir()
    _make_directory_link(state_root / "locks", pending)

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(game_root, local_app_data)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_existing_managed_state_root_must_be_a_directory(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    (local_app_data / "CMOAgentBridge").write_bytes(b"not a directory")

    with pytest.raises(BridgeError) as caught:
        FileBridgePaths.build(game_root, local_app_data)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_build_does_not_create_managed_children(tmp_path: Path) -> None:
    game_root = _make_cmo_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()

    paths = FileBridgePaths.build(game_root, local_app_data)

    assert not paths.lua_root.exists()
    assert not paths.lock_file.parent.exists()
    assert not paths.pending_file.parent.exists()
    assert not paths.sqlite_file.exists()
