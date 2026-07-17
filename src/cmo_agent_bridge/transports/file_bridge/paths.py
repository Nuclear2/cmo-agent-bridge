from __future__ import annotations

import hashlib
import ntpath
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

from cmo_agent_bridge.errors import BridgeError, ErrorCode


def _path_error(code: ErrorCode, message: str, path: Path) -> BridgeError:
    return BridgeError(code, message, {"path": str(path)})


def _strip_extended_namespace(value: str) -> str:
    windows_value = value.replace("/", "\\")
    folded = windows_value.casefold()
    extended_unc = "\\\\?\\unc\\"
    extended_dos = "\\\\?\\"
    if folded.startswith(extended_unc):
        return "\\\\" + windows_value[len(extended_unc) :]
    if folded.startswith(extended_dos):
        remainder = windows_value[len(extended_dos) :]
        if len(remainder) >= 2 and remainder[1] == ":":
            return remainder
    return windows_value


def _windows_normalized_path(path: Path) -> str:
    return ntpath.normcase(ntpath.normpath(_strip_extended_namespace(str(path))))


def _canonical_resolved_path(path: Path) -> Path:
    return Path(ntpath.normpath(_strip_extended_namespace(str(path))))


def _resolve_directory(path: Path, *, code: ErrorCode, description: str) -> Path:
    try:
        resolved = _canonical_resolved_path(path.resolve(strict=True))
    except (OSError, RuntimeError) as error:
        raise _path_error(
            code, f"{description} does not exist or cannot be resolved", path
        ) from error
    if not resolved.is_dir():
        raise _path_error(code, f"{description} must be a directory", resolved)
    return resolved


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _resolve_managed_child(
    path: Path,
    parent: Path,
    *,
    code: ErrorCode,
    description: str,
    expected_kind: Literal["file", "directory"] | None = None,
) -> Path:
    try:
        resolved = _canonical_resolved_path(path.resolve(strict=False))
    except (OSError, RuntimeError) as error:
        raise _path_error(code, f"{description} cannot be resolved", path) from error
    if not _is_within(resolved, parent):
        raise _path_error(code, f"{description} escapes its managed root", resolved)
    if _windows_normalized_path(resolved) != _windows_normalized_path(path):
        raise _path_error(code, f"{description} cannot be redirected", resolved)
    if resolved.exists() and expected_kind is not None:
        has_expected_type = resolved.is_file() if expected_kind == "file" else resolved.is_dir()
        if not has_expected_type:
            raise _path_error(code, f"{description} must be a {expected_kind}", resolved)
    return resolved


def _resolve_marker(
    game_root: Path,
    name: str,
    *,
    directory: bool,
) -> Path:
    candidate = game_root / name
    try:
        resolved = _canonical_resolved_path(candidate.resolve(strict=True))
    except (OSError, RuntimeError) as error:
        raise _path_error(
            ErrorCode.GAME_ROOT_INVALID,
            f"CMO marker {name} does not exist or cannot be resolved",
            candidate,
        ) from error
    if not _is_within(resolved, game_root):
        raise _path_error(
            ErrorCode.GAME_ROOT_INVALID,
            f"CMO marker {name} escapes the game root",
            resolved,
        )
    has_expected_type = resolved.is_dir() if directory else resolved.is_file()
    if not has_expected_type:
        expected = "directory" if directory else "file"
        raise _path_error(
            ErrorCode.GAME_ROOT_INVALID,
            f"CMO marker {name} must be a {expected}",
            resolved,
        )
    return resolved


def _root_key(game_root: Path) -> str:
    normalized = _windows_normalized_path(game_root)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class FileBridgePaths:
    game_root: Path
    root_key: str
    command_exe: Path
    lua_root: Path
    inbox: Path
    import_export: Path
    lock_file: Path
    pending_file: Path
    sqlite_file: Path

    @classmethod
    def build(cls, game_root: Path, local_app_data: Path) -> FileBridgePaths:
        resolved_root = _resolve_directory(
            Path(game_root),
            code=ErrorCode.GAME_ROOT_INVALID,
            description="game root",
        )
        command_exe = _resolve_marker(resolved_root, "Command.exe", directory=False)
        lua_directory = _resolve_marker(resolved_root, "Lua", directory=True)
        import_export = _resolve_marker(resolved_root, "ImportExport", directory=True)

        lua_root = _resolve_managed_child(
            lua_directory / "CMOAgentBridge",
            lua_directory,
            code=ErrorCode.GAME_ROOT_INVALID,
            description="managed Lua root",
            expected_kind="directory",
        )
        inbox_directory = _resolve_managed_child(
            lua_root / "inbox",
            lua_root,
            code=ErrorCode.GAME_ROOT_INVALID,
            description="managed inbox directory",
            expected_kind="directory",
        )
        inbox = _resolve_managed_child(
            inbox_directory / "request.lua",
            inbox_directory,
            code=ErrorCode.GAME_ROOT_INVALID,
            description="managed inbox",
            expected_kind="file",
        )

        resolved_local_app_data = _resolve_directory(
            Path(local_app_data),
            code=ErrorCode.INVALID_ARGUMENT,
            description="Local AppData",
        )
        state_root_path = resolved_local_app_data / "CMOAgentBridge"
        state_root = _resolve_managed_child(
            state_root_path,
            resolved_local_app_data,
            code=ErrorCode.INVALID_ARGUMENT,
            description="managed local state root",
            expected_kind="directory",
        )

        root_key = _root_key(resolved_root)
        lock_directory = _resolve_managed_child(
            state_root / "locks",
            state_root,
            code=ErrorCode.INVALID_ARGUMENT,
            description="root lock directory",
            expected_kind="directory",
        )
        lock_file = _resolve_managed_child(
            lock_directory / f"{root_key}.lock",
            lock_directory,
            code=ErrorCode.INVALID_ARGUMENT,
            description="root lock file",
            expected_kind="file",
        )
        pending_directory = _resolve_managed_child(
            state_root / "pending",
            state_root,
            code=ErrorCode.INVALID_ARGUMENT,
            description="pending journal directory",
            expected_kind="directory",
        )
        pending_file = _resolve_managed_child(
            pending_directory / f"{root_key}.json",
            pending_directory,
            code=ErrorCode.INVALID_ARGUMENT,
            description="pending journal file",
            expected_kind="file",
        )
        sqlite_file = _resolve_managed_child(
            state_root / "state.sqlite3",
            state_root,
            code=ErrorCode.INVALID_ARGUMENT,
            description="SQLite state file",
            expected_kind="file",
        )

        return cls(
            game_root=resolved_root,
            root_key=root_key,
            command_exe=command_exe,
            lua_root=lua_root,
            inbox=inbox,
            import_export=import_export,
            lock_file=lock_file,
            pending_file=pending_file,
            sqlite_file=sqlite_file,
        )

    def response_path(self, request_id: UUID) -> Path:
        request_value = cast(object, request_id)
        if not isinstance(request_value, UUID):
            raise BridgeError(ErrorCode.INVALID_ARGUMENT, "request ID must be a UUID")
        return _resolve_managed_child(
            self.import_export / f"CMOAgentBridge_Response_{request_value}.inst",
            self.import_export,
            code=ErrorCode.INVALID_ARGUMENT,
            description="response file",
            expected_kind="file",
        )
