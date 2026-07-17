from __future__ import annotations

import ctypes
import tomllib
from ctypes import wintypes
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictBool, ValidationError
from typing_extensions import Annotated

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.transports.file_bridge.atomic_io import atomic_replace_bytes
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths


PositiveNumber = Annotated[float, Field(strict=True, gt=0, allow_inf_nan=False)]
NonNegativeNumber = Annotated[float, Field(strict=True, ge=0, allow_inf_nan=False)]
PollIntervalCode = Literal["0", "1", "2", "3", "4", "5", "6", "7", "8"]
LogLevel = Literal["debug", "info", "warning", "error", "critical"]


class BridgeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1] = 1
    game_root: Path | None = None
    request_timeout_seconds: PositiveNumber = 30
    replace_retry_seconds: NonNegativeNumber = 2
    cancel_ack_timeout_seconds: PositiveNumber = 10
    poll_interval_code: PollIntervalCode = "1"
    allow_mutations: StrictBool = True
    allow_destructive: StrictBool = False
    response_retention_hours: PositiveNumber = 24
    log_level: LogLevel = "info"


class _Guid(ctypes.Structure):
    _fields_ = [
        ("data1", wintypes.DWORD),
        ("data2", wintypes.WORD),
        ("data3", wintypes.WORD),
        ("data4", ctypes.c_ubyte * 8),
    ]

    @classmethod
    def from_uuid(cls, value: UUID) -> _Guid:
        fields = value.fields
        tail = (ctypes.c_ubyte * 8)(fields[3], fields[4], *fields[5].to_bytes(6, "big"))
        return cls(fields[0], fields[1], fields[2], tail)


def _known_local_app_data() -> Path:
    folder_id = _Guid.from_uuid(UUID("f1b32785-6fba-4fcf-9d55-7b8e7f157091"))
    path_pointer = ctypes.c_wchar_p()
    shell32: Any = ctypes.WinDLL("shell32", use_last_error=True)
    ole32: Any = ctypes.WinDLL("ole32", use_last_error=True)
    shell32.SHGetKnownFolderPath.argtypes = [
        ctypes.POINTER(_Guid),
        wintypes.DWORD,
        wintypes.HANDLE,
        ctypes.POINTER(ctypes.c_wchar_p),
    ]
    shell32.SHGetKnownFolderPath.restype = ctypes.c_long
    ole32.CoTaskMemFree.argtypes = [ctypes.c_void_p]
    ole32.CoTaskMemFree.restype = None
    result = shell32.SHGetKnownFolderPath(
        ctypes.byref(folder_id),
        0,
        None,
        ctypes.byref(path_pointer),
    )
    if result != 0 or path_pointer.value is None:
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "Windows Local AppData known folder is unavailable",
            {"hresult": result},
        )
    try:
        return Path(path_pointer.value)
    finally:
        ole32.CoTaskMemFree(ctypes.cast(path_pointer, ctypes.c_void_p))


def _resolve_local_app_data(path: Path) -> Path:
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "Local AppData does not exist or cannot be resolved",
            {"path": str(path)},
        ) from error
    if not resolved.is_dir():
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "Local AppData must be a directory",
            {"path": str(resolved)},
        )
    return resolved


def _managed_state_root(local_app_data: Path) -> Path:
    expected = local_app_data / "CMOAgentBridge"
    try:
        candidate = expected.resolve(strict=False)
    except (OSError, RuntimeError) as error:
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "managed local state root cannot be resolved",
            {"path": str(expected)},
        ) from error
    try:
        candidate.relative_to(local_app_data)
    except ValueError as error:
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "managed local state root escapes Local AppData",
            {"path": str(candidate)},
        ) from error
    if candidate != expected:
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "managed local state root cannot be redirected",
            {"path": str(candidate)},
        )
    if candidate.exists() and not candidate.is_dir():
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "managed local state root must be a directory",
            {"path": str(candidate)},
        )
    return candidate


def _config_error(message: str, details: dict[str, object] | None = None) -> BridgeError:
    return BridgeError(ErrorCode.INVALID_ARGUMENT, message, details)


def _validation_details(error: ValidationError) -> dict[str, object]:
    return {
        "validation_errors": error.errors(
            include_url=False,
            include_context=False,
        )
    }


class BridgeConfigStore:
    def __init__(
        self,
        config_path: Path | None = None,
        *,
        local_app_data: Path | None = None,
    ) -> None:
        selected_local_app_data = (
            _known_local_app_data() if local_app_data is None else Path(local_app_data)
        )
        self._local_app_data = _resolve_local_app_data(selected_local_app_data)
        self._state_root = _managed_state_root(self._local_app_data)
        selected_config = self._state_root / "config.toml" if config_path is None else config_path
        self._config_path = Path(selected_config).resolve(strict=False)

    @property
    def config_path(self) -> Path:
        return self._config_path

    @property
    def local_app_data(self) -> Path:
        """Resolved Windows Local AppData root used by this config store."""
        return self._local_app_data

    def load(self, *, game_root_override: Path | None = None) -> BridgeConfig:
        _managed_state_root(self._local_app_data)
        config = BridgeConfig() if not self._config_file_exists() else self._parse_existing()
        if game_root_override is not None:
            paths = FileBridgePaths.build(Path(game_root_override), self._local_app_data)
            return config.model_copy(update={"game_root": paths.game_root})
        if config.game_root is None:
            return config
        return self._validate_config_root(config)

    def save(
        self,
        config: BridgeConfig,
        *,
        replace_saved_game_root: bool = False,
    ) -> BridgeConfig:
        replace_value = cast(object, replace_saved_game_root)
        if not isinstance(replace_value, bool):
            raise _config_error("replace-saved-game-root flag must be a boolean")
        self._require_managed_write_path()
        if config.game_root is None:
            raise BridgeError(ErrorCode.GAME_ROOT_INVALID, "a valid game root is required to save")

        candidate_paths = FileBridgePaths.build(config.game_root, self._local_app_data)
        candidate = config.model_copy(update={"game_root": candidate_paths.game_root})
        if self._config_file_exists():
            existing = self._parse_existing()
            if existing.game_root is None:
                raise _config_error("saved config is missing its game root")
            if not replace_value:
                existing_paths = FileBridgePaths.build(existing.game_root, self._local_app_data)
                if existing_paths.root_key != candidate_paths.root_key:
                    raise BridgeError(
                        ErrorCode.STATE_CONFLICT,
                        "saved game root differs; explicit replacement is required",
                        {
                            "saved_root": str(existing_paths.game_root),
                            "requested_root": str(candidate_paths.game_root),
                        },
                    )

        self._create_state_root()
        atomic_replace_bytes(
            self._config_path,
            _render_config(candidate),
            retry_seconds=candidate.replace_retry_seconds,
        )
        return candidate

    def _config_file_exists(self) -> bool:
        try:
            self._config_path.stat()
        except FileNotFoundError:
            return False
        except OSError as error:
            raise _config_error(
                "config existence cannot be determined",
                {"path": str(self._config_path)},
            ) from error
        return True

    def _parse_existing(self) -> BridgeConfig:
        try:
            decoded = self._config_path.read_bytes().decode("utf-8")
            value = tomllib.loads(decoded)
        except OSError as error:
            raise _config_error(
                "config cannot be read",
                {"path": str(self._config_path)},
            ) from error
        except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
            raise _config_error("config is not valid UTF-8 TOML") from error
        expected_keys = set(BridgeConfig.model_fields)
        actual_keys = set(value)
        if actual_keys != expected_keys:
            raise _config_error(
                "config must contain exactly the schema-version-1 fields",
                {
                    "missing": sorted(expected_keys - actual_keys),
                    "unknown": sorted(actual_keys - expected_keys),
                },
            )
        root_value = value.get("game_root")
        if type(root_value) is not str or not root_value:
            raise _config_error("config game_root must be a non-empty string")
        value["game_root"] = Path(root_value)
        try:
            config = BridgeConfig.model_validate(value)
        except ValidationError as error:
            raise _config_error(
                "config does not match schema version 1", _validation_details(error)
            ) from error

        return config

    def _validate_config_root(self, config: BridgeConfig) -> BridgeConfig:
        if config.game_root is None:
            raise _config_error("saved config is missing its game root")
        paths = FileBridgePaths.build(config.game_root, self._local_app_data)
        return config.model_copy(update={"game_root": paths.game_root})

    def _require_managed_write_path(self) -> None:
        current_state_root = _managed_state_root(self._local_app_data)
        candidate_parent = self._config_path.parent.resolve(strict=False)
        if candidate_parent != current_state_root:
            raise BridgeError(
                ErrorCode.POLICY_DENIED,
                "config persistence is limited to the managed local state root",
                {"path": str(self._config_path)},
            )

    def _create_state_root(self) -> None:
        try:
            self._state_root.mkdir(exist_ok=True)
            resolved = self._state_root.resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "managed local state root cannot be created",
                {"path": str(self._state_root)},
            ) from error
        if resolved != _managed_state_root(self._local_app_data):
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "managed local state root changed during creation",
                {"path": str(resolved)},
            )


def _toml_string(value: str) -> str:
    escapes = {
        '"': '\\"',
        "\\": "\\\\",
        "\b": "\\b",
        "\t": "\\t",
        "\n": "\\n",
        "\f": "\\f",
        "\r": "\\r",
    }
    encoded = ['"']
    for character in value:
        escape = escapes.get(character)
        if escape is not None:
            encoded.append(escape)
            continue
        codepoint = ord(character)
        if codepoint <= 0x1F or codepoint == 0x7F:
            encoded.append(f"\\u{codepoint:04X}")
        elif 0xD800 <= codepoint <= 0xDFFF:
            raise _config_error("TOML strings cannot contain Unicode surrogate code points")
        else:
            encoded.append(character)
    encoded.append('"')
    return "".join(encoded)


def _render_config(config: BridgeConfig) -> bytes:
    if config.game_root is None:
        raise BridgeError(ErrorCode.GAME_ROOT_INVALID, "saved config requires a game root")
    lines = (
        f"schema_version = {config.schema_version}",
        f"game_root = {_toml_string(str(config.game_root))}",
        f"request_timeout_seconds = {config.request_timeout_seconds!r}",
        f"replace_retry_seconds = {config.replace_retry_seconds!r}",
        f"cancel_ack_timeout_seconds = {config.cancel_ack_timeout_seconds!r}",
        f"poll_interval_code = {_toml_string(config.poll_interval_code)}",
        f"allow_mutations = {str(config.allow_mutations).lower()}",
        f"allow_destructive = {str(config.allow_destructive).lower()}",
        f"response_retention_hours = {config.response_retention_hours!r}",
        f"log_level = {_toml_string(config.log_level)}",
    )
    return ("\n".join(lines) + "\n").encode("utf-8")
