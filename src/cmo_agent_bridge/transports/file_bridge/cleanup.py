from __future__ import annotations

import ctypes
import hashlib
import hmac
import ntpath
import re
import stat
from ctypes import wintypes
from pathlib import Path
from typing import Self, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator, model_validator

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua
from cmo_agent_bridge.protocol.models import PreparedDelivery
from cmo_agent_bridge.protocol.response_models import ResponseArtifact
from cmo_agent_bridge.state.models import HostRequestState, PendingExchange
from cmo_agent_bridge.state.pending_journal import PendingJournalStore
from cmo_agent_bridge.state.request_ledger import DeliveryRecord, RequestLedger, RequestRecord
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths


_DEFAULT_RETENTION_MS = 24 * 60 * 60 * 1000
_TERMINAL_STATES = frozenset(
    {
        HostRequestState.COMPLETED,
        HostRequestState.CANCELLED,
        HostRequestState.REJECTED,
        HostRequestState.RESOLVED,
    }
)
_RESPONSE_NAME_RE = re.compile(
    r"^CMOAgentBridge_Response_"
    r"([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})"
    r"\.inst$"
)

_DELETE = 0x00010000
_GENERIC_READ = 0x80000000
_FILE_SHARE_READ = 0x00000001
_OPEN_EXISTING = 3
_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_FILE_ATTRIBUTE_NORMAL = 0x00000080
_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_FILE_TYPE_DISK = 0x0001
_FILE_DISPOSITION_INFO_CLASS = 4
_FILE_BEGIN = 0
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
_WINDOWS_TO_UNIX_100NS = 116_444_736_000_000_000


class _ByHandleFileInformation(ctypes.Structure):
    _fields_ = [
        ("dwFileAttributes", wintypes.DWORD),
        ("ftCreationTime", wintypes.FILETIME),
        ("ftLastAccessTime", wintypes.FILETIME),
        ("ftLastWriteTime", wintypes.FILETIME),
        ("dwVolumeSerialNumber", wintypes.DWORD),
        ("nFileSizeHigh", wintypes.DWORD),
        ("nFileSizeLow", wintypes.DWORD),
        ("nNumberOfLinks", wintypes.DWORD),
        ("nFileIndexHigh", wintypes.DWORD),
        ("nFileIndexLow", wintypes.DWORD),
    ]


class _FileDispositionInformation(ctypes.Structure):
    _fields_ = [("DeleteFile", wintypes.BOOLEAN)]


_kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
_create_file_w = _kernel32.CreateFileW
_create_file_w.argtypes = [
    wintypes.LPCWSTR,
    wintypes.DWORD,
    wintypes.DWORD,
    ctypes.c_void_p,
    wintypes.DWORD,
    wintypes.DWORD,
    wintypes.HANDLE,
]
_create_file_w.restype = wintypes.HANDLE
_get_file_information_by_handle = _kernel32.GetFileInformationByHandle
_get_file_information_by_handle.argtypes = [
    wintypes.HANDLE,
    ctypes.POINTER(_ByHandleFileInformation),
]
_get_file_information_by_handle.restype = wintypes.BOOL
_get_final_path_name_by_handle_w = _kernel32.GetFinalPathNameByHandleW
_get_final_path_name_by_handle_w.argtypes = [
    wintypes.HANDLE,
    wintypes.LPWSTR,
    wintypes.DWORD,
    wintypes.DWORD,
]
_get_final_path_name_by_handle_w.restype = wintypes.DWORD
_get_file_type = _kernel32.GetFileType
_get_file_type.argtypes = [wintypes.HANDLE]
_get_file_type.restype = wintypes.DWORD
_read_file = _kernel32.ReadFile
_read_file.argtypes = [
    wintypes.HANDLE,
    ctypes.c_void_p,
    wintypes.DWORD,
    ctypes.POINTER(wintypes.DWORD),
    ctypes.c_void_p,
]
_read_file.restype = wintypes.BOOL
_set_file_pointer_ex = _kernel32.SetFilePointerEx
_set_file_pointer_ex.argtypes = [
    wintypes.HANDLE,
    ctypes.c_longlong,
    ctypes.POINTER(ctypes.c_longlong),
    wintypes.DWORD,
]
_set_file_pointer_ex.restype = wintypes.BOOL
_set_file_information_by_handle = _kernel32.SetFileInformationByHandle
_set_file_information_by_handle.argtypes = [
    wintypes.HANDLE,
    ctypes.c_int,
    ctypes.c_void_p,
    wintypes.DWORD,
]
_set_file_information_by_handle.restype = wintypes.BOOL
_close_handle_w = _kernel32.CloseHandle
_close_handle_w.argtypes = [wintypes.HANDLE]
_close_handle_w.restype = wintypes.BOOL


class _RetainCandidate(Exception):
    pass


class CleanupReport(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    scanned: StrictInt = Field(ge=0)
    deleted: StrictInt = Field(ge=0)
    retained: StrictInt = Field(ge=0)
    failed: StrictInt = Field(ge=0)
    failed_paths: tuple[Path, ...]

    @field_validator("failed_paths")
    @classmethod
    def validate_failed_paths(cls, value: tuple[Path, ...]) -> tuple[Path, ...]:
        if type(value) is not tuple:
            raise ValueError("failed cleanup paths must be an exact tuple")
        validated: list[Path] = []
        for path in value:
            if not path.is_absolute():
                raise ValueError("failed cleanup paths must be absolute Path values")
            try:
                resolved = path.resolve(strict=False)
            except (OSError, RuntimeError, ValueError) as error:
                raise ValueError("failed cleanup path cannot be canonicalized") from error
            if path != resolved:
                raise ValueError("failed cleanup paths must be canonical")
            validated.append(path)
        if len(set(validated)) != len(validated):
            raise ValueError("failed cleanup paths must be unique")
        return tuple(validated)

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if self.scanned != self.deleted + self.retained + self.failed:
            raise ValueError("cleanup report counters are inconsistent")
        if self.failed != len(self.failed_paths):
            raise ValueError("cleanup failure count does not match failed paths")
        return self


def _windows_normalized(path: Path) -> str:
    value = str(path).replace("/", "\\")
    folded = value.casefold()
    if folded.startswith("\\\\?\\unc\\"):
        value = "\\\\" + value[len("\\\\?\\unc\\") :]
    elif folded.startswith("\\\\?\\"):
        value = value[len("\\\\?\\") :]
    return ntpath.normcase(ntpath.normpath(value))


def _last_win_error() -> OSError:
    return ctypes.WinError(ctypes.get_last_error())


def _open_candidate(path: Path) -> int:
    raw_handle = _create_file_w(
        str(path),
        _GENERIC_READ | _DELETE,
        _FILE_SHARE_READ,
        None,
        _OPEN_EXISTING,
        _FILE_ATTRIBUTE_NORMAL | _FILE_FLAG_OPEN_REPARSE_POINT | _FILE_FLAG_BACKUP_SEMANTICS,
        None,
    )
    handle = cast(int | None, raw_handle)
    if handle is None or handle == _INVALID_HANDLE_VALUE:
        raise _last_win_error()
    return handle


def _close_handle(handle: int) -> None:
    if not _close_handle_w(handle):
        raise _last_win_error()


def _handle_information(handle: int) -> _ByHandleFileInformation:
    information = _ByHandleFileInformation()
    if not _get_file_information_by_handle(handle, ctypes.byref(information)):
        raise _last_win_error()
    return information


def _handle_file_type(handle: int) -> int:
    ctypes.set_last_error(0)
    file_type = int(_get_file_type(handle))
    error_code = ctypes.get_last_error()
    if file_type == 0 and error_code != 0:
        raise ctypes.WinError(error_code)
    return file_type


def _final_handle_path(handle: int) -> Path:
    size = 512
    while True:
        buffer = ctypes.create_unicode_buffer(size)
        result = int(_get_final_path_name_by_handle_w(handle, buffer, size, 0))
        if result == 0:
            raise _last_win_error()
        if result < size:
            return Path(buffer.value)
        size = result + 1


def _file_size(information: _ByHandleFileInformation) -> int:
    return (int(information.nFileSizeHigh) << 32) | int(information.nFileSizeLow)


def _last_write_ms(information: _ByHandleFileInformation) -> int:
    windows_value = (int(information.ftLastWriteTime.dwHighDateTime) << 32) | int(
        information.ftLastWriteTime.dwLowDateTime
    )
    if windows_value < _WINDOWS_TO_UNIX_100NS:
        raise _RetainCandidate
    return (windows_value - _WINDOWS_TO_UNIX_100NS) // 10_000


def _identity(information: _ByHandleFileInformation) -> tuple[int, ...]:
    return (
        int(information.dwVolumeSerialNumber),
        int(information.nFileIndexHigh),
        int(information.nFileIndexLow),
        int(information.dwFileAttributes),
        int(information.nNumberOfLinks),
        int(information.nFileSizeHigh),
        int(information.nFileSizeLow),
        int(information.ftLastWriteTime.dwHighDateTime),
        int(information.ftLastWriteTime.dwLowDateTime),
    )


def _read_handle_sha256(handle: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    total = 0
    buffer = ctypes.create_string_buffer(64 * 1024)
    while True:
        read = wintypes.DWORD()
        if not _read_file(handle, buffer, len(buffer), ctypes.byref(read), None):
            raise _last_win_error()
        count = int(read.value)
        if count == 0:
            return total, digest.hexdigest()
        digest.update(buffer.raw[:count])
        total += count


def _rewind_handle(handle: int) -> None:
    if not _set_file_pointer_ex(handle, 0, None, _FILE_BEGIN):
        raise _last_win_error()


def _set_delete_disposition(handle: int, expected_path: Path) -> None:
    del expected_path
    information = _FileDispositionInformation(DeleteFile=True)
    if not _set_file_information_by_handle(
        handle,
        _FILE_DISPOSITION_INFO_CLASS,
        ctypes.byref(information),
        ctypes.sizeof(information),
    ):
        raise _last_win_error()


def _delete_bound_candidate(
    path: Path,
    *,
    expected_size: int,
    expected_sha256: str,
    cutoff_ms: int,
) -> None:
    try:
        status = path.lstat()
    except FileNotFoundError as error:
        raise _RetainCandidate from error
    if not stat.S_ISREG(status.st_mode):
        raise _RetainCandidate
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as error:
        raise _RetainCandidate from error
    except (OSError, RuntimeError, ValueError) as error:
        raise OSError(f"cleanup candidate path cannot be resolved: {path}") from error
    if _windows_normalized(resolved) != _windows_normalized(path):
        raise _RetainCandidate

    handle: int | None = None
    primary: BaseException | None = None
    try:
        handle = _open_candidate(resolved)
        if _handle_file_type(handle) != _FILE_TYPE_DISK:
            raise _RetainCandidate
        before = _handle_information(handle)
        attributes = int(before.dwFileAttributes)
        if attributes & (_FILE_ATTRIBUTE_DIRECTORY | _FILE_ATTRIBUTE_REPARSE_POINT):
            raise _RetainCandidate
        if int(before.nNumberOfLinks) != 1:
            raise _RetainCandidate
        if _windows_normalized(_final_handle_path(handle)) != _windows_normalized(resolved):
            raise _RetainCandidate
        if _file_size(before) != expected_size or _last_write_ms(before) >= cutoff_ms:
            raise _RetainCandidate
        actual_size, actual_sha256 = _read_handle_sha256(handle)
        if actual_size != expected_size or not hmac.compare_digest(actual_sha256, expected_sha256):
            raise _RetainCandidate
        after = _handle_information(handle)
        if _identity(after) != _identity(before):
            raise _RetainCandidate
        _set_delete_disposition(handle, resolved)
    except BaseException as error:
        primary = error
        raise
    finally:
        if handle is not None:
            try:
                _close_handle(handle)
            except BaseException as close_error:
                if primary is None:
                    raise
                primary.add_note(
                    f"cleanup candidate handle close failed: "
                    f"{type(close_error).__name__}: {close_error}"
                )


class PinnedResponseCleanup:
    """One terminal response held open until its post-unlock deletion attempt."""

    def __init__(
        self,
        handle: int,
        path: Path,
        identity: tuple[int, ...],
        size: int,
        sha256: str,
    ) -> None:
        self._handle: int | None = handle
        self._path = path
        self._identity = identity
        self._size = size
        self._sha256 = sha256

    def delete(self) -> None:
        handle = self._handle
        if handle is None:
            raise BridgeError(ErrorCode.STATE_CONFLICT, "pinned response cleanup was already used")
        self._handle = None
        primary: BaseException | None = None
        try:
            if _handle_file_type(handle) != _FILE_TYPE_DISK:
                raise BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "pinned response cleanup handle is no longer a disk file",
                )
            current = _handle_information(handle)
            if _identity(current) != self._identity or _windows_normalized(
                _final_handle_path(handle)
            ) != _windows_normalized(self._path):
                raise BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "pinned response cleanup identity changed before deletion",
                )
            _rewind_handle(handle)
            current_size, current_sha256 = _read_handle_sha256(handle)
            if current_size != self._size or not hmac.compare_digest(current_sha256, self._sha256):
                raise BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "pinned response cleanup content changed before deletion",
                )
            if _identity(_handle_information(handle)) != self._identity:
                raise BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "pinned response cleanup metadata changed during verification",
                )
            _set_delete_disposition(handle, self._path)
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                _close_handle(handle)
            except BaseException as close_error:
                if primary is None:
                    raise
                primary.add_note(
                    f"pinned response handle close failed: "
                    f"{type(close_error).__name__}: {close_error}"
                )


def pin_terminal_response_cleanup(
    paths: FileBridgePaths,
    root_lock: RootLock,
    path: Path,
    expected_artifact: ResponseArtifact,
) -> PinnedResponseCleanup | None:
    """Pin a trusted exchange's exact response slot for deletion after unlock."""
    if (
        type(paths) is not FileBridgePaths
        or type(root_lock) is not RootLock
        or not isinstance(cast(object, path), Path)
        or type(expected_artifact) is not ResponseArtifact
    ):
        raise BridgeError(ErrorCode.INVALID_ARGUMENT, "response cleanup dependencies are invalid")
    root_lock.require_acquired()
    if root_lock.path != paths.lock_file:
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "response cleanup dependencies do not belong to one bridge root",
        )
    match = _RESPONSE_NAME_RE.fullmatch(path.name)
    if match is None:
        raise BridgeError(ErrorCode.STATE_CONFLICT, "response cleanup path is not canonical")
    request_id = UUID(match.group(1))
    expected_path = paths.import_export / path.name
    if (
        str(request_id) != match.group(1)
        or request_id.version != 4
        or _windows_normalized(path) != _windows_normalized(expected_path)
        or expected_artifact.filename != path.name
    ):
        raise BridgeError(ErrorCode.STATE_CONFLICT, "response cleanup path identity is invalid")

    try:
        status = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(status.st_mode):
        return None
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError:
        return None
    except (OSError, RuntimeError, ValueError) as error:
        conflict = BridgeError(
            ErrorCode.STATE_CONFLICT,
            "response cleanup path cannot be resolved safely",
        )
        conflict.__cause__ = error
        raise conflict
    if _windows_normalized(resolved) != _windows_normalized(expected_path):
        return None

    handle: int | None = None
    primary: BaseException | None = None
    try:
        handle = _open_candidate(resolved)
        if _handle_file_type(handle) != _FILE_TYPE_DISK:
            raise _RetainCandidate
        before = _handle_information(handle)
        attributes = int(before.dwFileAttributes)
        if attributes & (_FILE_ATTRIBUTE_DIRECTORY | _FILE_ATTRIBUTE_REPARSE_POINT):
            raise _RetainCandidate
        if int(before.nNumberOfLinks) != 1:
            raise _RetainCandidate
        if _windows_normalized(_final_handle_path(handle)) != _windows_normalized(resolved):
            raise _RetainCandidate
        actual_size, actual_sha256 = _read_handle_sha256(handle)
        if actual_size != expected_artifact.size_bytes or not hmac.compare_digest(
            actual_sha256, expected_artifact.sha256
        ):
            raise _RetainCandidate
        after = _handle_information(handle)
        if _identity(after) != _identity(before):
            raise _RetainCandidate
        pinned = PinnedResponseCleanup(
            handle,
            resolved,
            _identity(after),
            expected_artifact.size_bytes,
            expected_artifact.sha256,
        )
        handle = None
        return pinned
    except FileNotFoundError:
        return None
    except _RetainCandidate:
        return None
    except BaseException as error:
        primary = error
        raise
    finally:
        if handle is not None:
            try:
                _close_handle(handle)
            except BaseException as close_error:
                if primary is None:
                    raise
                primary.add_note(
                    f"response cleanup pin close failed: "
                    f"{type(close_error).__name__}: {close_error}"
                )


def _protected_response_names(exchange: PendingExchange) -> set[str]:
    protected = {intent.response_filename for intent in exchange.delivery_intents}
    if exchange.response_artifact is not None:
        protected.add(exchange.response_artifact.filename)
    return protected


def _render_durable_delivery(request: RequestRecord, delivery: DeliveryRecord) -> bytes:
    try:
        rendered = render_delivery_lua(
            PreparedDelivery(
                request_id=delivery.request_id,
                delivery_id=delivery.delivery_id,
                delivery_kind=delivery.delivery_kind,
                request_hash=request.request_hash,
                body_json=request.body_json,
            ),
            request.runtime_snapshot,
        )
    except (BridgeError, TypeError, ValueError) as error:
        conflict = BridgeError(
            ErrorCode.STATE_CONFLICT,
            "persisted delivery cannot reconstruct its exact inbox rendering",
        )
        conflict.__cause__ = error
        raise conflict
    if len(rendered) != delivery.rendered_inbox_size_bytes or not hmac.compare_digest(
        hashlib.sha256(rendered).hexdigest(),
        delivery.rendered_inbox_sha256,
    ):
        raise BridgeError(
            ErrorCode.STATE_CONFLICT,
            "persisted delivery rendering differs from its durable digest",
        )
    return rendered


class ArtifactJanitor:
    def __init__(
        self,
        paths: FileBridgePaths,
        root_lock: RootLock,
        ledger: RequestLedger,
        journals: PendingJournalStore,
        *,
        retention_ms: int = _DEFAULT_RETENTION_MS,
    ) -> None:
        if (
            type(paths) is not FileBridgePaths
            or type(root_lock) is not RootLock
            or type(ledger) is not RequestLedger
            or type(journals) is not PendingJournalStore
        ):
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT, "artifact janitor dependencies are invalid"
            )
        if type(retention_ms) is not int or retention_ms <= 0:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "artifact retention must be an exact positive integer",
            )
        database = ledger._database  # pyright: ignore[reportPrivateUsage]
        if (
            type(database) is not StateDatabase
            or journals._paths is not paths  # pyright: ignore[reportPrivateUsage]
            or journals._root_lock is not root_lock  # pyright: ignore[reportPrivateUsage]
            or ledger._catalog is not journals._catalog  # pyright: ignore[reportPrivateUsage]
            or database.path != paths.sqlite_file
            or root_lock.path != paths.lock_file
        ):
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "artifact janitor dependencies do not belong to one bridge root",
            )
        self._paths = paths
        self._root_lock = root_lock
        self._ledger = ledger
        self._journals = journals
        self._retention_ms = retention_ms

    def sweep(self, now_ms: int) -> CleanupReport:
        self._root_lock.require_acquired()
        if type(now_ms) is not int or now_ms < 0:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "cleanup epoch must be an exact non-negative integer",
            )
        cutoff_ms = now_ms - self._retention_ms
        loaded = self._journals.load()
        protected_names: set[str] = set()
        if loaded is not None:
            protected_names.update(_protected_response_names(loaded.journal.original))
            if loaded.journal.reconcile_attempt is not None:
                protected_names.update(_protected_response_names(loaded.journal.reconcile_attempt))
        try:
            inbox = self._paths.inbox.read_bytes()
            candidates = tuple(
                sorted(self._paths.import_export.iterdir(), key=lambda item: item.name)
            )
        except (OSError, RuntimeError, ValueError) as error:
            failure = BridgeError(
                ErrorCode.STATE_CONFLICT,
                "artifact cleanup inputs cannot be read safely",
            )
            failure.__cause__ = error
            raise failure
        deleted = 0
        retained = 0
        failed_paths: list[Path] = []
        for candidate in candidates:
            match = _RESPONSE_NAME_RE.fullmatch(candidate.name)
            if match is None:
                retained += 1
                continue
            try:
                request_id = UUID(match.group(1))
            except (ValueError, AttributeError):
                retained += 1
                continue
            if str(request_id) != match.group(1) or request_id.version != 4:
                retained += 1
                continue
            expected_path = self._paths.import_export / candidate.name
            request = self._ledger.get_request(request_id)
            if (
                request is None
                or request.root_key != self._paths.root_key
                or request.state not in _TERMINAL_STATES
                or request.terminal_at_ms is None
            ):
                retained += 1
                continue
            deliveries = self._ledger.list_deliveries(request_id)
            artifacts = tuple(
                delivery.response_artifact
                for delivery in deliveries
                if delivery.response_artifact is not None
            )
            if len(artifacts) != 1:
                retained += 1
                continue
            artifact = artifacts[0]
            if artifact.filename != candidate.name or candidate.name in protected_names:
                retained += 1
                continue
            rendered_deliveries = tuple(
                _render_durable_delivery(request, delivery) for delivery in deliveries
            )
            if any(inbox == rendered for rendered in rendered_deliveries):
                retained += 1
                continue
            if artifact.accepted_at_ms >= cutoff_ms or request.terminal_at_ms >= cutoff_ms:
                retained += 1
                continue
            try:
                _delete_bound_candidate(
                    expected_path,
                    expected_size=artifact.size_bytes,
                    expected_sha256=artifact.sha256,
                    cutoff_ms=cutoff_ms,
                )
            except _RetainCandidate:
                retained += 1
            except OSError:
                try:
                    canonical = expected_path.resolve(strict=False)
                except (OSError, RuntimeError, ValueError):
                    canonical = expected_path.absolute()
                failed_paths.append(canonical)
            else:
                deleted += 1
        return CleanupReport(
            scanned=len(candidates),
            deleted=deleted,
            retained=retained,
            failed=len(failed_paths),
            failed_paths=tuple(failed_paths),
        )
