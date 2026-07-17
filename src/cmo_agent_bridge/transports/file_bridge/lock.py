from __future__ import annotations

import asyncio
import ctypes
import errno
import math
import msvcrt
import ntpath
import os
import stat
import time
from ctypes import wintypes
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Self, cast

from cmo_agent_bridge.errors import BridgeError, ErrorCode


_RETRY_DELAY_SECONDS = 0.01
_LOCK_VIOLATION_WINERROR = 33
_GENERIC_READ = 0x80000000
_GENERIC_WRITE = 0x40000000
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_OPEN_EXISTING = 3
_OPEN_ALWAYS = 4
_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_FILE_ATTRIBUTE_NORMAL = 0x00000080
_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_FILE_TYPE_DISK = 0x0001
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value


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
_close_handle_w = _kernel32.CloseHandle
_close_handle_w.argtypes = [wintypes.HANDLE]
_close_handle_w.restype = wintypes.BOOL


def _windows_normalized(path: Path) -> str:
    value = str(path).replace("/", "\\")
    folded = value.casefold()
    if folded.startswith("\\\\?\\unc\\"):
        value = "\\\\" + value[len("\\\\?\\unc\\") :]
    elif folded.startswith("\\\\?\\"):
        value = value[len("\\\\?\\") :]
    return ntpath.normcase(ntpath.normpath(value))


def _path_error(message: str, path: Path, cause: BaseException | None = None) -> BridgeError:
    error = BridgeError(ErrorCode.INVALID_ARGUMENT, message, {"path": str(path)})
    if cause is not None:
        error.__cause__ = cause
    return error


def _lock_error(
    message: str,
    path: Path,
    cause: BaseException,
    **details: object,
) -> BridgeError:
    payload = {"path": str(path), "type": type(cause).__name__, **details}
    error = BridgeError(ErrorCode.STATE_CONFLICT, message, payload)
    error.__cause__ = cause
    return error


def _record_cleanup_failure(
    primary: BaseException,
    cleanup: BaseException,
    *,
    operation: str,
    path: Path,
) -> None:
    note = f"root lock {operation} cleanup failed for {path}: {type(cleanup).__name__}: {cleanup}"
    primary.add_note(note)
    if isinstance(primary, BridgeError):
        record = {
            "operation": operation,
            "path": str(path),
            "type": type(cleanup).__name__,
            "message": str(cleanup),
        }
        errors_value = primary.details.get("cleanup_errors")
        if isinstance(errors_value, list):
            errors = cast(list[dict[str, str]], errors_value)
        else:
            errors = []
            primary.details["cleanup_errors"] = errors
        errors.append(record)


def _is_contention(error: OSError) -> bool:
    return error.errno == errno.EACCES or getattr(error, "winerror", None) == (
        _LOCK_VIOLATION_WINERROR
    )


def _last_win_error() -> OSError:
    return ctypes.WinError(ctypes.get_last_error())


def _create_file_handle(
    path: Path,
    *,
    desired_access: int,
    creation_disposition: int,
    flags_and_attributes: int,
) -> int:
    raw_handle = _create_file_w(
        str(path),
        desired_access,
        _FILE_SHARE_READ | _FILE_SHARE_WRITE,
        None,
        creation_disposition,
        flags_and_attributes,
        None,
    )
    handle = cast(int | None, raw_handle)
    if handle is None or handle == _INVALID_HANDLE_VALUE:
        raise _last_win_error()
    return handle


def _close_handle(handle: int) -> None:
    if not _close_handle_w(handle):
        raise _last_win_error()


def _get_final_handle_path(handle: int) -> Path:
    buffer_size = 512
    while True:
        buffer = ctypes.create_unicode_buffer(buffer_size)
        result = int(
            _get_final_path_name_by_handle_w(
                handle,
                buffer,
                buffer_size,
                0,
            )
        )
        if result == 0:
            raise _last_win_error()
        if result < buffer_size:
            return Path(buffer.value)
        buffer_size = result + 1


def _validate_handle(
    handle: int,
    expected_path: Path,
    *,
    directory: bool,
) -> None:
    file_type = int(_get_file_type(handle))
    if file_type != _FILE_TYPE_DISK:
        description = "directory" if directory else "file"
        raise _path_error(
            f"root lock {description} handle must refer to a disk object",
            expected_path,
        )

    information = _ByHandleFileInformation()
    if not _get_file_information_by_handle(handle, ctypes.byref(information)):
        error = _last_win_error()
        raise _path_error(
            "root lock handle attributes cannot be queried",
            expected_path,
            error,
        ) from error
    attributes = int(information.dwFileAttributes)
    if attributes & _FILE_ATTRIBUTE_REPARSE_POINT:
        raise _path_error("root lock path cannot be a reparse point", expected_path)
    is_directory = bool(attributes & _FILE_ATTRIBUTE_DIRECTORY)
    if is_directory != directory:
        expected_kind = "directory" if directory else "regular file"
        raise _path_error(f"root lock path must be a {expected_kind}", expected_path)

    try:
        final_path = _get_final_handle_path(handle)
    except OSError as error:
        raise _path_error(
            "root lock handle path cannot be resolved",
            expected_path,
            error,
        ) from error
    if _windows_normalized(final_path) != _windows_normalized(expected_path):
        raise _path_error("root lock handle path was redirected", final_path)


def _open_pinned_lock(
    lock_path: Path,
    expected_parent: Path,
    expected_target: Path,
) -> tuple[BinaryIO, int]:
    parent_handle: int | None = None
    file_handle: int | None = None
    descriptor: int | None = None
    try:
        try:
            parent_handle = _create_file_handle(
                expected_parent,
                desired_access=0,
                creation_disposition=_OPEN_EXISTING,
                flags_and_attributes=(_FILE_FLAG_BACKUP_SEMANTICS | _FILE_FLAG_OPEN_REPARSE_POINT),
            )
        except OSError as error:
            raise _path_error(
                "root lock parent could not be pinned",
                expected_parent,
                error,
            ) from error
        _validate_handle(parent_handle, expected_parent, directory=True)

        try:
            file_handle = _create_file_handle(
                expected_target,
                desired_access=_GENERIC_READ | _GENERIC_WRITE,
                creation_disposition=_OPEN_ALWAYS,
                flags_and_attributes=(
                    _FILE_ATTRIBUTE_NORMAL
                    | _FILE_FLAG_OPEN_REPARSE_POINT
                    | _FILE_FLAG_BACKUP_SEMANTICS
                ),
            )
        except OSError as error:
            raise _lock_error(
                "root lock file could not be opened",
                lock_path,
                error,
            ) from error
        _validate_handle(file_handle, expected_target, directory=False)

        try:
            descriptor = msvcrt.open_osfhandle(
                file_handle,
                os.O_RDWR | os.O_BINARY | os.O_APPEND,
            )
        except (OSError, ValueError) as error:
            raise _lock_error(
                "root lock handle could not be transferred to the CRT",
                lock_path,
                error,
            ) from error
        file_handle = None
        try:
            stream = cast(BinaryIO, os.fdopen(descriptor, "a+b"))
        except (OSError, ValueError) as error:
            raise _lock_error(
                "root lock descriptor could not be opened as a stream",
                lock_path,
                error,
            ) from error
        descriptor = None
        return stream, parent_handle
    except BaseException as primary:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except BaseException as cleanup:
                _record_cleanup_failure(
                    primary,
                    cleanup,
                    operation="close CRT descriptor",
                    path=lock_path,
                )
        elif file_handle is not None:
            try:
                _close_handle(file_handle)
            except BaseException as cleanup:
                _record_cleanup_failure(
                    primary,
                    cleanup,
                    operation="close file handle",
                    path=lock_path,
                )
        if parent_handle is not None:
            try:
                _close_handle(parent_handle)
            except BaseException as cleanup:
                _record_cleanup_failure(
                    primary,
                    cleanup,
                    operation="close parent handle",
                    path=expected_parent,
                )
        raise


class RootLock:
    """A Windows byte-range lock serializing one bridge exchange per CMO root."""

    def __init__(self, lock_path: Path, *, timeout_seconds: float) -> None:
        raw_timeout = cast(object, timeout_seconds)
        if isinstance(raw_timeout, bool) or not isinstance(raw_timeout, (int, float)):
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "root lock timeout must be a finite non-negative number",
            )
        try:
            timeout_value = float(raw_timeout)
        except (OverflowError, ValueError) as error:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "root lock timeout must be a finite non-negative number",
            ) from error
        if not math.isfinite(timeout_value) or timeout_value < 0:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "root lock timeout must be a finite non-negative number",
            )
        try:
            self.path = Path(lock_path)
        except (TypeError, ValueError) as error:
            raise BridgeError(ErrorCode.INVALID_ARGUMENT, "root lock path is invalid") from error
        self.timeout_seconds = timeout_value
        self._stream: BinaryIO | None = None
        self._parent_handle: int | None = None
        self._acquired = False
        self._held_path: Path | None = None

    def require_acquired(self) -> None:
        """Fail closed unless this exact lock instance currently owns its byte lock."""
        path_detail = "<unavailable>"
        try:
            path_value = self.path
            current_path = Path(path_value)
            current_target = Path(os.path.abspath(current_path.parent)) / current_path.name
            path_detail = str(path_value)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            current_target = None
        if (
            not self._acquired
            or self._stream is None
            or self._held_path is None
            or current_target is None
            or _windows_normalized(current_target) != _windows_normalized(self._held_path)
        ):
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "the root lock must be acquired by this instance",
                {"path": path_detail},
            )

    async def __aenter__(self) -> Self:
        if self._stream is not None:
            raise BridgeError(ErrorCode.STATE_CONFLICT, "root lock is already in use")
        expected_parent, expected_target = self._prepare_path()
        try:
            try:
                self._stream, self._parent_handle = _open_pinned_lock(
                    self.path,
                    expected_parent,
                    expected_target,
                )
            except OSError as error:
                raise _lock_error(
                    "root lock file could not be opened",
                    self.path,
                    error,
                ) from error
            try:
                self._initialize_stream(self._stream)
            except OSError as error:
                raise _lock_error(
                    "root lock file could not be initialized",
                    self.path,
                    error,
                ) from error
            await self._acquire(self._stream)
            self._held_path = expected_target
        except BaseException as primary:
            stream = self._stream
            parent_handle = self._parent_handle
            self._stream = None
            self._parent_handle = None
            self._acquired = False
            self._held_path = None
            if stream is not None:
                try:
                    stream.close()
                except BaseException as cleanup:
                    _record_cleanup_failure(
                        primary,
                        cleanup,
                        operation="close after acquisition failure",
                        path=self.path,
                    )
            if parent_handle is not None:
                try:
                    _close_handle(parent_handle)
                except BaseException as cleanup:
                    _record_cleanup_failure(
                        primary,
                        cleanup,
                        operation="close parent handle after acquisition failure",
                        path=expected_parent,
                    )
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del exc_type, traceback
        cleanup_error = self._release(exc)
        if exc is None and cleanup_error is not None:
            raise cleanup_error
        return False

    def _prepare_path(self) -> tuple[Path, Path]:
        if "\0" in str(self.path):
            raise _path_error("root lock path cannot contain NUL", self.path)
        try:
            expected_parent = Path(os.path.abspath(self.path.parent))
        except (OSError, RuntimeError, ValueError) as error:
            raise _path_error(
                "root lock parent cannot be derived", self.path.parent, error
            ) from error
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            actual_parent = self.path.parent.resolve(strict=True)
        except (OSError, RuntimeError, ValueError) as error:
            raise _path_error(
                "root lock parent cannot be created or resolved",
                self.path.parent,
                error,
            ) from error
        if not actual_parent.is_dir():
            raise _path_error("root lock parent must be a directory", actual_parent)
        if _windows_normalized(actual_parent) != _windows_normalized(expected_parent):
            raise _path_error("root lock parent cannot be redirected", actual_parent)

        expected_target = expected_parent / self.path.name
        try:
            target_status = self.path.lstat()
        except FileNotFoundError:
            return expected_parent, expected_target
        except (OSError, ValueError) as error:
            raise _path_error("root lock target cannot be inspected", self.path, error) from error
        if not stat.S_ISREG(target_status.st_mode):
            raise _path_error("root lock target must be a regular file", self.path)
        try:
            actual_target = self.path.resolve(strict=True)
        except (OSError, RuntimeError, ValueError) as error:
            raise _path_error("root lock target cannot be resolved", self.path, error) from error
        if _windows_normalized(actual_target) != _windows_normalized(expected_target):
            raise _path_error("root lock target cannot be redirected", actual_target)
        return expected_parent, expected_target

    @staticmethod
    def _initialize_stream(stream: BinaryIO) -> None:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()

    async def _acquire(self, stream: BinaryIO) -> None:
        deadline = time.monotonic() + self.timeout_seconds
        first_attempt = True
        last_contention: OSError | None = None
        while True:
            if not first_attempt and time.monotonic() >= deadline:
                timeout = BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "timed out waiting for the root lock",
                    {
                        "path": str(self.path),
                        "timeout_seconds": self.timeout_seconds,
                    },
                )
                if last_contention is not None:
                    raise timeout from last_contention
                raise timeout
            first_attempt = False
            try:
                stream.seek(0)
                descriptor = stream.fileno()
            except (OSError, ValueError) as error:
                raise _lock_error(
                    "root lock stream could not be positioned",
                    self.path,
                    error,
                ) from error
            try:
                msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
            except OSError as error:
                if not _is_contention(error):
                    raise _lock_error(
                        "root lock acquisition failed",
                        self.path,
                        error,
                    ) from error
                last_contention = error
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BridgeError(
                        ErrorCode.STATE_CONFLICT,
                        "timed out waiting for the root lock",
                        {
                            "path": str(self.path),
                            "timeout_seconds": self.timeout_seconds,
                        },
                    ) from error
                await asyncio.sleep(min(_RETRY_DELAY_SECONDS, remaining))
                continue
            except ValueError as error:
                raise _lock_error(
                    "root lock acquisition failed",
                    self.path,
                    error,
                ) from error
            self._acquired = True
            return

    def _release(self, body_error: BaseException | None) -> BaseException | None:
        stream = self._stream
        parent_handle = self._parent_handle
        self._stream = None
        self._parent_handle = None
        self._held_path = None
        if stream is None and parent_handle is None:
            return None

        primary = body_error
        positioned = False
        if stream is not None and self._acquired:
            try:
                stream.seek(0)
                positioned = True
            except BaseException as cleanup:
                primary = self._merge_cleanup(primary, cleanup, "seek before unlock")
            if positioned:
                try:
                    msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
                except BaseException as cleanup:
                    primary = self._merge_cleanup(primary, cleanup, "unlock")
        self._acquired = False
        if stream is not None:
            try:
                stream.close()
            except BaseException as cleanup:
                primary = self._merge_cleanup(primary, cleanup, "close")
        if parent_handle is not None:
            try:
                _close_handle(parent_handle)
            except BaseException as cleanup:
                primary = self._merge_cleanup(primary, cleanup, "close parent handle")
        if body_error is not None:
            return None
        return primary

    def _merge_cleanup(
        self,
        primary: BaseException | None,
        cleanup: BaseException,
        operation: str,
    ) -> BaseException:
        if primary is None:
            return _lock_error(
                f"root lock {operation} failed",
                self.path,
                cleanup,
                operation=operation,
            )
        _record_cleanup_failure(primary, cleanup, operation=operation, path=self.path)
        return primary
