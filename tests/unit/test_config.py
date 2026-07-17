import subprocess
import tomllib
from pathlib import Path
from typing import Never

import pytest
from pydantic import ValidationError

from cmo_agent_bridge.config import (
    BridgeConfig,
    BridgeConfigStore,
    _toml_string,  # pyright: ignore[reportPrivateUsage]
)
from cmo_agent_bridge.errors import BridgeError, ErrorCode


def _make_cmo_root(parent: Path, name: str) -> Path:
    root = parent / name
    root.mkdir()
    (root / "Command.exe").write_bytes(b"command")
    (root / "Lua").mkdir()
    (root / "ImportExport").mkdir()
    return root


def _local_app_data(tmp_path: Path) -> Path:
    path = tmp_path / "LocalAppData"
    path.mkdir()
    return path


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


def test_missing_config_loads_frozen_defaults_without_game_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local_app_data = _local_app_data(tmp_path)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "hostile"))
    monkeypatch.setenv("CMO_GAME_ROOT", str(tmp_path / "also-hostile"))
    monkeypatch.setenv("CMO_AGENT_BRIDGE_CONFIG", str(tmp_path / "hostile.toml"))
    store = BridgeConfigStore(local_app_data=local_app_data)

    config = store.load()

    assert config == BridgeConfig(
        schema_version=1,
        game_root=None,
        request_timeout_seconds=30,
        replace_retry_seconds=2,
        cancel_ack_timeout_seconds=10,
        poll_interval_code="1",
        allow_mutations=True,
        allow_destructive=False,
        response_retention_hours=24,
        log_level="info",
    )
    assert store.config_path == local_app_data / "CMOAgentBridge" / "config.toml"
    with pytest.raises(ValidationError):
        config.allow_mutations = False  # type: ignore[misc]


def test_default_location_uses_known_folder_provider_not_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    known = _local_app_data(tmp_path)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "wrong"))
    monkeypatch.setattr("cmo_agent_bridge.config._known_local_app_data", lambda: known)

    store = BridgeConfigStore()

    assert store.config_path == known / "CMOAgentBridge" / "config.toml"


def test_config_store_rejects_redirected_managed_state_root(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    redirected = local_app_data / "RedirectedState"
    redirected.mkdir()
    _make_directory_link(local_app_data / "CMOAgentBridge", redirected)

    with pytest.raises(BridgeError) as caught:
        BridgeConfigStore(local_app_data=local_app_data)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_config_store_rejects_existing_non_directory_state_root(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    (local_app_data / "CMOAgentBridge").write_bytes(b"not a directory")

    with pytest.raises(BridgeError) as caught:
        BridgeConfigStore(local_app_data=local_app_data)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_load_rechecks_managed_state_root_type(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    store = BridgeConfigStore(local_app_data=local_app_data)
    (local_app_data / "CMOAgentBridge").write_bytes(b"not a directory")

    with pytest.raises(BridgeError) as caught:
        store.load()

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_save_maps_late_non_directory_state_root_structurally(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    game_root = _make_cmo_root(tmp_path, "CMO")
    store = BridgeConfigStore(local_app_data=local_app_data)
    (local_app_data / "CMOAgentBridge").write_bytes(b"not a directory")

    with pytest.raises(BridgeError) as caught:
        store.save(BridgeConfig(game_root=game_root))

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_save_and_load_round_trip_unicode_windows_path(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    game_root = _make_cmo_root(tmp_path, "海军 CMO")
    store = BridgeConfigStore(local_app_data=local_app_data)
    config = BridgeConfig(
        game_root=game_root,
        request_timeout_seconds=12.5,
        replace_retry_seconds=0.25,
        cancel_ack_timeout_seconds=4.5,
        poll_interval_code="8",
        allow_mutations=False,
        allow_destructive=True,
        response_retention_hours=2.5,
        log_level="warning",
    )

    saved = store.save(config)

    assert saved.game_root == game_root.resolve(strict=True)
    assert store.load() == saved
    text = store.config_path.read_text(encoding="utf-8")
    assert "海军 CMO" in text
    assert "\\\\" in text


def test_toml_string_round_trips_controls_quotes_backslashes_and_non_bmp() -> None:
    value = '\x00\x08\x09\x0a\x0c\x0d\x1f\x7f"\\\U0001f680'

    encoded = _toml_string(value)

    assert tomllib.loads(f"value = {encoded}")["value"] == value
    assert "\\u007F" in encoded


def test_config_round_trips_del_and_non_bmp_game_root(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    game_root = _make_cmo_root(tmp_path, "CMO-\x7f-\U0001f680")
    store = BridgeConfigStore(local_app_data=local_app_data)

    saved = store.save(BridgeConfig(game_root=game_root))

    assert store.load() == saved


def test_load_override_changes_only_current_process_root(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    saved_root = _make_cmo_root(tmp_path, "Saved")
    override_root = _make_cmo_root(tmp_path, "Override")
    store = BridgeConfigStore(local_app_data=local_app_data)
    store.save(
        BridgeConfig(
            game_root=saved_root,
            allow_mutations=False,
            allow_destructive=True,
        )
    )

    overridden = store.load(game_root_override=override_root)

    assert overridden.game_root == override_root.resolve(strict=True)
    assert overridden.allow_mutations is False
    assert overridden.allow_destructive is True
    assert store.load().game_root == saved_root.resolve(strict=True)


def test_override_takes_precedence_over_an_invalid_saved_root(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    saved_root = _make_cmo_root(tmp_path, "Saved")
    override_root = _make_cmo_root(tmp_path, "Override")
    store = BridgeConfigStore(local_app_data=local_app_data)
    store.save(BridgeConfig(game_root=saved_root, allow_mutations=False))
    (saved_root / "Command.exe").unlink()

    overridden = store.load(game_root_override=override_root)

    assert overridden.game_root == override_root.resolve(strict=True)
    assert overridden.allow_mutations is False
    with pytest.raises(BridgeError) as caught:
        store.load()
    assert caught.value.code is ErrorCode.GAME_ROOT_INVALID


def test_same_root_alias_is_idempotent_and_updates_policy(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    game_root = _make_cmo_root(tmp_path, "MixedCase")
    store = BridgeConfigStore(local_app_data=local_app_data)
    store.save(BridgeConfig(game_root=game_root))

    saved = store.save(
        BridgeConfig(game_root=Path(str(game_root).swapcase()), allow_destructive=True)
    )

    assert saved.game_root == game_root.resolve(strict=True)
    assert saved.allow_destructive is True
    assert store.load() == saved


def test_different_root_requires_explicit_replace_and_preserves_old_file(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    first = _make_cmo_root(tmp_path, "First")
    second = _make_cmo_root(tmp_path, "Second")
    store = BridgeConfigStore(local_app_data=local_app_data)
    original = store.save(BridgeConfig(game_root=first))
    original_bytes = store.config_path.read_bytes()

    with pytest.raises(BridgeError) as caught:
        store.save(BridgeConfig(game_root=second))

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert store.config_path.read_bytes() == original_bytes
    assert store.load() == original

    replaced = store.save(
        BridgeConfig(game_root=second),
        replace_saved_game_root=True,
    )
    assert replaced.game_root == second.resolve(strict=True)
    assert store.load() == replaced


def test_explicit_replace_recovers_an_invalid_saved_root(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    stale = _make_cmo_root(tmp_path, "Stale")
    replacement = _make_cmo_root(tmp_path, "Replacement")
    store = BridgeConfigStore(local_app_data=local_app_data)
    store.save(BridgeConfig(game_root=stale))
    (stale / "Command.exe").unlink()
    original = store.config_path.read_bytes()

    with pytest.raises(BridgeError):
        store.save(BridgeConfig(game_root=replacement))
    assert store.config_path.read_bytes() == original

    saved = store.save(
        BridgeConfig(game_root=replacement),
        replace_saved_game_root=True,
    )
    assert saved.game_root == replacement.resolve(strict=True)
    assert store.load() == saved


def test_explicit_config_path_may_be_read_but_not_saved_outside_managed_root(
    tmp_path: Path,
) -> None:
    local_app_data = _local_app_data(tmp_path)
    game_root = _make_cmo_root(tmp_path, "CMO")
    managed = BridgeConfigStore(local_app_data=local_app_data)
    expected = managed.save(BridgeConfig(game_root=game_root))
    external_path = tmp_path / "external" / "custom.toml"
    external_path.parent.mkdir()
    external_path.write_bytes(managed.config_path.read_bytes())
    external_store = BridgeConfigStore(
        config_path=external_path,
        local_app_data=local_app_data,
    )

    assert external_store.load() == expected
    old = external_path.read_bytes()
    with pytest.raises(BridgeError) as caught:
        external_store.save(BridgeConfig(game_root=game_root, allow_destructive=True))

    assert caught.value.code is ErrorCode.POLICY_DENIED
    assert external_path.read_bytes() == old


def test_save_requires_a_valid_game_root(tmp_path: Path) -> None:
    store = BridgeConfigStore(local_app_data=_local_app_data(tmp_path))

    with pytest.raises(BridgeError) as missing:
        store.save(BridgeConfig())
    with pytest.raises(BridgeError) as invalid:
        store.save(BridgeConfig(game_root=tmp_path / "not-cmo"))

    assert missing.value.code is ErrorCode.GAME_ROOT_INVALID
    assert invalid.value.code is ErrorCode.GAME_ROOT_INVALID
    assert not store.config_path.exists()


def test_atomic_save_failure_preserves_old_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local_app_data = _local_app_data(tmp_path)
    game_root = _make_cmo_root(tmp_path, "CMO")
    store = BridgeConfigStore(local_app_data=local_app_data)
    store.save(BridgeConfig(game_root=game_root))
    original = store.config_path.read_bytes()
    failure = OSError("replace failed")

    def fail_replace(_target: Path, _data: bytes, *, retry_seconds: float) -> None:
        del retry_seconds
        raise failure

    monkeypatch.setattr(
        "cmo_agent_bridge.config.atomic_replace_bytes",
        fail_replace,
    )

    with pytest.raises(OSError) as caught:
        store.save(BridgeConfig(game_root=game_root, allow_destructive=True))

    assert caught.value is failure
    assert store.config_path.read_bytes() == original


@pytest.mark.parametrize(
    ("replacement", "expected_code"),
    [
        ("schema_version = 2", ErrorCode.INVALID_ARGUMENT),
        ("request_timeout_seconds = 0", ErrorCode.INVALID_ARGUMENT),
        ("replace_retry_seconds = -1", ErrorCode.INVALID_ARGUMENT),
        ("cancel_ack_timeout_seconds = 0", ErrorCode.INVALID_ARGUMENT),
        ('poll_interval_code = "9"', ErrorCode.INVALID_ARGUMENT),
        ("allow_mutations = 1", ErrorCode.INVALID_ARGUMENT),
        ("response_retention_hours = 0", ErrorCode.INVALID_ARGUMENT),
        ('log_level = "INFO"', ErrorCode.INVALID_ARGUMENT),
    ],
)
def test_existing_config_rejects_wrong_versions_types_and_ranges(
    tmp_path: Path, replacement: str, expected_code: ErrorCode
) -> None:
    local_app_data = _local_app_data(tmp_path)
    game_root = _make_cmo_root(tmp_path, "CMO")
    store = BridgeConfigStore(local_app_data=local_app_data)
    store.save(BridgeConfig(game_root=game_root))
    lines = store.config_path.read_text(encoding="utf-8").splitlines()
    key = replacement.split(" = ", 1)[0]
    changed = [replacement if line.startswith(key + " = ") else line for line in lines]
    store.config_path.write_text("\n".join(changed) + "\n", encoding="utf-8")

    with pytest.raises(BridgeError) as caught:
        store.load()

    assert caught.value.code is expected_code


@pytest.mark.parametrize("mutation", ["missing", "unknown", "malformed"])
def test_existing_config_is_complete_and_rejects_unknown_or_malformed_content(
    tmp_path: Path, mutation: str
) -> None:
    local_app_data = _local_app_data(tmp_path)
    game_root = _make_cmo_root(tmp_path, "CMO")
    store = BridgeConfigStore(local_app_data=local_app_data)
    store.save(BridgeConfig(game_root=game_root))
    lines = store.config_path.read_text(encoding="utf-8").splitlines()
    if mutation == "missing":
        lines = [line for line in lines if not line.startswith("log_level = ")]
    elif mutation == "unknown":
        lines.append("surprise = true")
    else:
        lines.append('broken = "')
    store.config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(BridgeError) as caught:
        store.load()

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_existing_config_read_failure_is_structured(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    config_directory = tmp_path / "config-directory"
    config_directory.mkdir()
    store = BridgeConfigStore(
        config_path=config_directory,
        local_app_data=local_app_data,
    )

    with pytest.raises(BridgeError) as caught:
        store.load()

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_config_stat_failure_is_not_treated_as_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local_app_data = _local_app_data(tmp_path)
    store = BridgeConfigStore(local_app_data=local_app_data)
    failure = PermissionError("stat denied")

    def missing_exists(_path: Path) -> bool:
        return False

    def denied_stat(_path: Path, *, follow_symlinks: bool = True) -> Never:
        del follow_symlinks
        raise failure

    monkeypatch.setattr(Path, "exists", missing_exists)
    monkeypatch.setattr(Path, "stat", denied_stat)

    with pytest.raises(BridgeError) as caught:
        store.load()

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert caught.value.__cause__ is failure


def test_invalid_saved_root_fails_closed(tmp_path: Path) -> None:
    local_app_data = _local_app_data(tmp_path)
    game_root = _make_cmo_root(tmp_path, "CMO")
    store = BridgeConfigStore(local_app_data=local_app_data)
    store.save(BridgeConfig(game_root=game_root))
    text = store.config_path.read_text(encoding="utf-8")
    escaped_missing = str(tmp_path / "gone").replace("\\", "\\\\")
    lines = [
        f'game_root = "{escaped_missing}"' if line.startswith("game_root = ") else line
        for line in text.splitlines()
    ]
    store.config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(BridgeError) as caught:
        store.load()

    assert caught.value.code is ErrorCode.GAME_ROOT_INVALID
