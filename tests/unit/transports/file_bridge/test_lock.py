from __future__ import annotations

import asyncio
import errno
import math
import os
import subprocess
import threading
from pathlib import Path
from types import TracebackType
from typing import Never, Self
from unittest.mock import Mock

import msvcrt
import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.transports.file_bridge import lock as lock_module
from cmo_agent_bridge.transports.file_bridge.lock import RootLock


class _FakeLockStream:
    def __init__(
        self,
        data: bytes = b"\0",
        *,
        close_error: BaseException | None = None,
    ) -> None:
        self.data = bytearray(data)
        self.position = 0
        self.closed = False
        self.close_calls = 0
        self.close_error = close_error
        self.seek_calls: list[tuple[int, int]] = []
        self.flush_calls = 0

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        self.seek_calls.append((offset, whence))
        if whence == os.SEEK_SET:
            self.position = offset
        elif whence == os.SEEK_END:
            self.position = len(self.data) + offset
        else:
            self.position += offset
        return self.position

    def tell(self) -> int:
        return self.position

    def write(self, data: bytes) -> int:
        if self.position >= len(self.data):
            self.data.extend(b"\0" * (self.position - len(self.data)))
            self.data.extend(data)
        else:
            end = self.position + len(data)
            self.data[self.position : end] = data
        self.position += len(data)
        return len(data)

    def flush(self) -> None:
        self.flush_calls += 1

    def fileno(self) -> int:
        return 42

    def close(self) -> None:
        self.close_calls += 1
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


def _install_fake_stream(
    monkeypatch: pytest.MonkeyPatch,
    stream: _FakeLockStream,
) -> None:
    def provide_stream(
        _lock_path: Path,
        _expected_parent: Path,
        _expected_target: Path,
    ) -> tuple[_FakeLockStream, None]:
        return stream, None

    monkeypatch.setattr(lock_module, "_open_pinned_lock", provide_stream)


def _contention_error() -> OSError:
    error = OSError(errno.EACCES, "lock violation")
    error.winerror = 33
    return error


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


@pytest.mark.parametrize(
    "timeout",
    [-0.1, math.inf, -math.inf, math.nan, True, "1", 10**400],
)
@pytest.mark.asyncio
async def test_invalid_timeout_is_rejected_before_creating_parent(
    tmp_path: Path,
    timeout: object,
) -> None:
    lock_path = tmp_path / "state" / "locks" / "root.lock"

    with pytest.raises(BridgeError) as caught:
        async with RootLock(lock_path, timeout_seconds=timeout):  # type: ignore[arg-type]
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert not lock_path.parent.exists()


@pytest.mark.asyncio
async def test_lock_creates_exact_parent_initializes_one_byte_and_never_deletes(
    tmp_path: Path,
) -> None:
    lock_path = tmp_path / "state" / "locks" / "root.lock"

    async with RootLock(lock_path, timeout_seconds=0):
        assert lock_path.exists()

    assert lock_path.read_bytes() == b"\0"
    assert set(lock_path.parent.iterdir()) == {lock_path}


@pytest.mark.asyncio
async def test_existing_lock_contents_are_not_truncated_or_replaced(tmp_path: Path) -> None:
    lock_path = tmp_path / "root.lock"
    lock_path.write_bytes(b"persistent lock metadata")

    async with RootLock(lock_path, timeout_seconds=0):
        assert lock_path.exists()

    assert lock_path.read_bytes() == b"persistent lock metadata"


@pytest.mark.asyncio
async def test_parent_and_file_handles_deny_rename_until_context_exit(tmp_path: Path) -> None:
    lock_parent = tmp_path / "locks"
    lock_path = lock_parent / "root.lock"
    renamed_file = lock_parent / "renamed.lock"
    renamed_parent = tmp_path / "renamed-locks"

    async with RootLock(lock_path, timeout_seconds=0):
        with pytest.raises(OSError):
            lock_path.rename(renamed_file)
        with pytest.raises(OSError):
            lock_parent.rename(renamed_parent)

    lock_path.rename(renamed_file)
    lock_parent.rename(renamed_parent)
    assert (renamed_parent / renamed_file.name).read_bytes() == b"\0"


def test_osfhandle_failure_closes_file_handle_before_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handles = iter([101, 202])
    closed: list[int] = []
    failure = OSError("CRT transfer failed")

    def create_handle(
        _path: Path,
        *,
        desired_access: int,
        creation_disposition: int,
        flags_and_attributes: int,
    ) -> int:
        del desired_access, creation_disposition, flags_and_attributes
        return next(handles)

    def validate_handle(_handle: int, _path: Path, *, directory: bool) -> None:
        del directory

    def fail_transfer(_handle: int, _flags: int) -> Never:
        raise failure

    monkeypatch.setattr(lock_module, "_create_file_handle", create_handle)
    monkeypatch.setattr(lock_module, "_validate_handle", validate_handle)
    monkeypatch.setattr(msvcrt, "open_osfhandle", fail_transfer)
    monkeypatch.setattr(lock_module, "_close_handle", closed.append)

    with pytest.raises(BridgeError) as caught:
        lock_module._open_pinned_lock(  # pyright: ignore[reportPrivateUsage]
            tmp_path / "locks" / "root.lock",
            tmp_path / "locks",
            tmp_path / "locks" / "root.lock",
        )

    assert caught.value.__cause__ is failure
    assert closed == [202, 101]


def test_fdopen_failure_closes_transferred_descriptor_then_parent_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handles = iter([101, 202])
    cleanup: list[tuple[str, int]] = []
    failure = OSError("fdopen failed")

    def create_handle(
        _path: Path,
        *,
        desired_access: int,
        creation_disposition: int,
        flags_and_attributes: int,
    ) -> int:
        del desired_access, creation_disposition, flags_and_attributes
        return next(handles)

    def validate_handle(_handle: int, _path: Path, *, directory: bool) -> None:
        del directory

    def transfer(_handle: int, _flags: int) -> int:
        return 303

    def fail_fdopen(_descriptor: int, _mode: str) -> Never:
        raise failure

    def close_descriptor(descriptor: int) -> None:
        cleanup.append(("descriptor", descriptor))

    def close_handle(handle: int) -> None:
        cleanup.append(("handle", handle))

    monkeypatch.setattr(lock_module, "_create_file_handle", create_handle)
    monkeypatch.setattr(lock_module, "_validate_handle", validate_handle)
    monkeypatch.setattr(msvcrt, "open_osfhandle", transfer)
    monkeypatch.setattr(os, "fdopen", fail_fdopen)
    monkeypatch.setattr(os, "close", close_descriptor)
    monkeypatch.setattr(lock_module, "_close_handle", close_handle)

    with pytest.raises(BridgeError) as caught:
        lock_module._open_pinned_lock(  # pyright: ignore[reportPrivateUsage]
            tmp_path / "locks" / "root.lock",
            tmp_path / "locks",
            tmp_path / "locks" / "root.lock",
        )

    assert caught.value.__cause__ is failure
    assert cleanup == [("descriptor", 303), ("handle", 101)]


@pytest.mark.asyncio
async def test_normal_release_closes_stream_before_pinned_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class OrderedStream(_FakeLockStream):
        def close(self) -> None:
            events.append("stream")
            super().close()

    stream = OrderedStream()

    def open_pinned(
        _lock_path: Path,
        _expected_parent: Path,
        _expected_target: Path,
    ) -> tuple[OrderedStream, int]:
        return stream, 101

    def close_parent(_handle: int) -> None:
        events.append("parent")

    def locking(_descriptor: int, _mode: int, _byte_count: int) -> None:
        pass

    monkeypatch.setattr(lock_module, "_open_pinned_lock", open_pinned)
    monkeypatch.setattr(lock_module, "_close_handle", close_parent)
    monkeypatch.setattr(msvcrt, "locking", locking)

    async with RootLock(tmp_path / "root.lock", timeout_seconds=0):
        pass

    assert events == ["stream", "parent"]


@pytest.mark.asyncio
async def test_every_lock_and_unlock_attempt_is_positioned_at_byte_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _FakeLockStream()
    _install_fake_stream(monkeypatch, stream)
    calls: list[tuple[int, int, int]] = []

    def locking(descriptor: int, mode: int, byte_count: int) -> None:
        calls.append((descriptor, mode, stream.position))
        assert stream.position == 0
        assert byte_count == 1
        stream.position = 99

    monkeypatch.setattr(msvcrt, "locking", locking)

    async with RootLock(tmp_path / "root.lock", timeout_seconds=0):
        pass

    assert calls == [(42, msvcrt.LK_NBLCK, 0), (42, msvcrt.LK_UNLCK, 0)]
    assert stream.seek_calls[-1] == (0, os.SEEK_SET)
    assert stream.close_calls == 1


@pytest.mark.asyncio
async def test_locking_runs_synchronously_and_only_backoff_yields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _FakeLockStream()
    _install_fake_stream(monkeypatch, stream)
    caller_thread = threading.get_ident()
    locking_threads: list[int] = []
    sleeps: list[float] = []
    attempts = 0

    def locking(_descriptor: int, mode: int, _byte_count: int) -> None:
        nonlocal attempts
        locking_threads.append(threading.get_ident())
        if mode == msvcrt.LK_NBLCK:
            attempts += 1
            if attempts == 1:
                raise _contention_error()

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(msvcrt, "locking", locking)
    monkeypatch.setattr(
        "cmo_agent_bridge.transports.file_bridge.lock.asyncio.sleep",
        sleep,
    )

    async with RootLock(tmp_path / "root.lock", timeout_seconds=1):
        pass

    assert attempts == 2
    assert sleeps and all(delay > 0 for delay in sleeps)
    assert locking_threads == [caller_thread, caller_thread, caller_thread]


@pytest.mark.asyncio
async def test_second_root_lock_times_out_then_can_be_reacquired(tmp_path: Path) -> None:
    lock_path = tmp_path / "root.lock"

    with pytest.raises(BridgeError) as caught:
        async with RootLock(lock_path, timeout_seconds=0.2):
            async with RootLock(lock_path, timeout_seconds=0.02):
                raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    async with RootLock(lock_path, timeout_seconds=0.2):
        pass


@pytest.mark.asyncio
async def test_deadline_is_rechecked_before_retrying_byte_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _FakeLockStream()
    _install_fake_stream(monkeypatch, stream)
    clock = {"now": 0.0}
    attempts = 0

    def monotonic() -> float:
        return clock["now"]

    async def sleep(delay: float) -> None:
        clock["now"] += delay

    def contend(_descriptor: int, mode: int, _byte_count: int) -> None:
        nonlocal attempts
        if mode == msvcrt.LK_NBLCK:
            attempts += 1
            raise _contention_error()

    monkeypatch.setattr(msvcrt, "locking", contend)
    monkeypatch.setattr(
        "cmo_agent_bridge.transports.file_bridge.lock.time.monotonic",
        monotonic,
    )
    monkeypatch.setattr(
        "cmo_agent_bridge.transports.file_bridge.lock.asyncio.sleep",
        sleep,
    )

    with pytest.raises(BridgeError) as caught:
        async with RootLock(tmp_path / "root.lock", timeout_seconds=0.005):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert attempts == 1
    assert stream.close_calls == 1


@pytest.mark.asyncio
async def test_zero_timeout_still_makes_one_initial_lock_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _FakeLockStream()
    _install_fake_stream(monkeypatch, stream)
    attempts = 0

    def contend(_descriptor: int, mode: int, _byte_count: int) -> None:
        nonlocal attempts
        if mode == msvcrt.LK_NBLCK:
            attempts += 1
            raise _contention_error()

    monkeypatch.setattr(msvcrt, "locking", contend)

    with pytest.raises(BridgeError) as caught:
        async with RootLock(tmp_path / "root.lock", timeout_seconds=0):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert attempts == 1


@pytest.mark.asyncio
async def test_cancelled_acquisition_closes_handle_and_preserves_cancellation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _FakeLockStream()
    _install_fake_stream(monkeypatch, stream)
    sleeping = asyncio.Event()

    def contend(_descriptor: int, _mode: int, _byte_count: int) -> None:
        raise _contention_error()

    async def blocked_sleep(_delay: float) -> None:
        sleeping.set()
        await asyncio.Future[None]()

    monkeypatch.setattr(msvcrt, "locking", contend)
    monkeypatch.setattr(
        "cmo_agent_bridge.transports.file_bridge.lock.asyncio.sleep",
        blocked_sleep,
    )

    async def acquire() -> None:
        async with RootLock(tmp_path / "root.lock", timeout_seconds=10):
            raise AssertionError("unreachable")

    task = asyncio.create_task(acquire())
    await asyncio.wait_for(sleeping.wait(), timeout=1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert stream.close_calls == 1
    assert stream.closed


@pytest.mark.asyncio
async def test_open_failure_is_structured_and_does_not_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = PermissionError("open denied")
    sleep = Mock()

    def fail_open(
        _lock_path: Path,
        _expected_parent: Path,
        _expected_target: Path,
    ) -> Never:
        raise failure

    monkeypatch.setattr(lock_module, "_open_pinned_lock", fail_open)
    monkeypatch.setattr(
        "cmo_agent_bridge.transports.file_bridge.lock.asyncio.sleep",
        sleep,
    )

    with pytest.raises(BridgeError) as caught:
        async with RootLock(tmp_path / "root.lock", timeout_seconds=1):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.__cause__ is failure
    sleep.assert_not_called()


@pytest.mark.asyncio
async def test_lock_file_initialization_failure_is_structured_and_closes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = OSError("seek failed")

    class FailingStream(_FakeLockStream):
        def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
            del offset, whence
            raise failure

    stream = FailingStream()
    _install_fake_stream(monkeypatch, stream)

    with pytest.raises(BridgeError) as caught:
        async with RootLock(tmp_path / "root.lock", timeout_seconds=1):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.__cause__ is failure
    assert stream.close_calls == 1


@pytest.mark.asyncio
async def test_base_exception_during_acquisition_closes_handle_without_masking(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _FakeLockStream()
    _install_fake_stream(monkeypatch, stream)
    primary = KeyboardInterrupt("stop")

    def fail(_descriptor: int, _mode: int, _byte_count: int) -> None:
        raise primary

    monkeypatch.setattr(msvcrt, "locking", fail)

    with pytest.raises(KeyboardInterrupt) as caught:
        async with RootLock(tmp_path / "root.lock", timeout_seconds=0):
            raise AssertionError("unreachable")

    assert caught.value is primary
    assert stream.close_calls == 1


@pytest.mark.asyncio
async def test_non_contention_lock_failure_aborts_without_backoff_and_closes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _FakeLockStream()
    _install_fake_stream(monkeypatch, stream)
    failure = OSError(errno.EBADF, "invalid descriptor")
    sleep = Mock()

    def fail(_descriptor: int, _mode: int, _byte_count: int) -> None:
        raise failure

    monkeypatch.setattr(msvcrt, "locking", fail)
    monkeypatch.setattr(
        "cmo_agent_bridge.transports.file_bridge.lock.asyncio.sleep",
        sleep,
    )

    with pytest.raises(BridgeError) as caught:
        async with RootLock(tmp_path / "root.lock", timeout_seconds=1):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.__cause__ is failure
    sleep.assert_not_called()
    assert stream.close_calls == 1


@pytest.mark.asyncio
async def test_invalid_lock_argument_is_structured_without_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _FakeLockStream()
    _install_fake_stream(monkeypatch, stream)
    failure = ValueError("invalid lock argument")
    sleep = Mock()

    def fail(_descriptor: int, _mode: int, _byte_count: int) -> None:
        raise failure

    monkeypatch.setattr(msvcrt, "locking", fail)
    monkeypatch.setattr(
        "cmo_agent_bridge.transports.file_bridge.lock.asyncio.sleep",
        sleep,
    )

    with pytest.raises(BridgeError) as caught:
        async with RootLock(tmp_path / "root.lock", timeout_seconds=1):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.__cause__ is failure
    sleep.assert_not_called()
    assert stream.close_calls == 1


@pytest.mark.asyncio
async def test_seek_permission_error_is_not_mistaken_for_lock_contention(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = PermissionError(errno.EACCES, "seek denied")

    class SeekFailingStream(_FakeLockStream):
        def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
            if self.seek_calls:
                raise failure
            return super().seek(offset, whence)

    stream = SeekFailingStream()
    _install_fake_stream(monkeypatch, stream)

    async def forbidden_sleep(_delay: float) -> None:
        raise AssertionError("seek failures must not enter lock-contention backoff")

    monkeypatch.setattr(
        "cmo_agent_bridge.transports.file_bridge.lock.asyncio.sleep",
        forbidden_sleep,
    )

    with pytest.raises(BridgeError) as caught:
        async with RootLock(tmp_path / "root.lock", timeout_seconds=1):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.__cause__ is failure
    assert stream.close_calls == 1


@pytest.mark.asyncio
async def test_body_exception_survives_unlock_and_close_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_failure = OSError("close failed")
    unlock_failure = OSError("unlock failed")
    stream = _FakeLockStream(close_error=close_failure)
    _install_fake_stream(monkeypatch, stream)
    body_failure = RuntimeError("body failed")

    def locking(_descriptor: int, mode: int, _byte_count: int) -> None:
        if mode == msvcrt.LK_UNLCK:
            raise unlock_failure

    monkeypatch.setattr(msvcrt, "locking", locking)

    with pytest.raises(RuntimeError) as caught:
        async with RootLock(tmp_path / "root.lock", timeout_seconds=0):
            raise body_failure

    assert caught.value is body_failure
    assert stream.close_calls == 1
    notes = getattr(body_failure, "__notes__", [])
    assert any("unlock failed" in note for note in notes)
    assert any("close failed" in note for note in notes)


@pytest.mark.asyncio
async def test_cleanup_failure_without_body_is_structured_and_still_closes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _FakeLockStream(close_error=OSError("close failed"))
    _install_fake_stream(monkeypatch, stream)

    def locking(_descriptor: int, mode: int, _byte_count: int) -> None:
        if mode == msvcrt.LK_UNLCK:
            raise OSError("unlock failed")

    monkeypatch.setattr(msvcrt, "locking", locking)

    with pytest.raises(BridgeError) as caught:
        async with RootLock(tmp_path / "root.lock", timeout_seconds=0):
            pass

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert stream.close_calls == 1
    assert any("close failed" in note for note in getattr(caught.value, "__notes__", []))


@pytest.mark.asyncio
async def test_redirected_lock_parent_is_rejected_without_writing_target(tmp_path: Path) -> None:
    managed = tmp_path / "managed"
    managed.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    redirected_parent = managed / "locks"
    _make_directory_link(redirected_parent, outside)

    with pytest.raises(BridgeError) as caught:
        async with RootLock(redirected_parent / "root.lock", timeout_seconds=0):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert not (outside / "root.lock").exists()


@pytest.mark.asyncio
async def test_parent_junction_swap_between_validation_and_open_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    managed = tmp_path / "managed"
    lock_parent = managed / "locks"
    lock_parent.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel.bin"
    sentinel.write_bytes(b"do not touch")
    lock_path = lock_parent / "root.lock"
    real_resolve = Path.resolve
    swapped = False

    def resolve_then_swap(path: Path, *, strict: bool = False) -> Path:
        nonlocal swapped
        resolved = real_resolve(path, strict=strict)
        if path == lock_parent and strict and not swapped:
            lock_parent.rmdir()
            _make_directory_link(lock_parent, outside)
            swapped = True
        return resolved

    monkeypatch.setattr(Path, "resolve", resolve_then_swap)

    with pytest.raises(BridgeError) as caught:
        async with RootLock(lock_path, timeout_seconds=0):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert swapped
    assert {path.name for path in outside.iterdir()} == {sentinel.name}
    assert not (outside / lock_path.name).exists()
    lock_parent.rmdir()
    assert sentinel.read_bytes() == b"do not touch"


@pytest.mark.asyncio
async def test_target_reparse_swap_between_validation_and_open_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_parent = tmp_path / "locks"
    lock_parent.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel.bin"
    sentinel.write_bytes(b"do not touch")
    lock_path = lock_parent / "root.lock"
    real_lstat = Path.lstat
    injected = False

    def missing_then_link(path: Path) -> os.stat_result:
        nonlocal injected
        try:
            return real_lstat(path)
        except FileNotFoundError:
            if path == lock_path and not injected:
                _make_directory_link(lock_path, outside)
                injected = True
            raise

    monkeypatch.setattr(Path, "lstat", missing_then_link)

    with pytest.raises(BridgeError) as caught:
        async with RootLock(lock_path, timeout_seconds=0):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert injected
    assert {path.name for path in outside.iterdir()} == {sentinel.name}
    assert sentinel.read_bytes() == b"do not touch"
    lock_path.rmdir()
    lock_parent.rmdir()


@pytest.mark.asyncio
async def test_embedded_nul_lock_path_is_structured_before_file_creation(
    tmp_path: Path,
) -> None:
    lock_path = tmp_path / "locks" / "bad\0name.lock"

    with pytest.raises(BridgeError) as caught:
        async with RootLock(lock_path, timeout_seconds=0):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert not lock_path.parent.exists()


@pytest.mark.asyncio
async def test_existing_non_regular_lock_target_is_rejected(tmp_path: Path) -> None:
    lock_path = tmp_path / "root.lock"
    lock_path.mkdir()

    with pytest.raises(BridgeError) as caught:
        async with RootLock(lock_path, timeout_seconds=0):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert lock_path.is_dir()


@pytest.mark.asyncio
async def test_existing_redirected_lock_target_is_rejected(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    lock_path = tmp_path / "root.lock"
    _make_directory_link(lock_path, outside)

    with pytest.raises(BridgeError) as caught:
        async with RootLock(lock_path, timeout_seconds=0):
            raise AssertionError("unreachable")

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert list(outside.iterdir()) == []


def test_root_lock_is_an_async_context_manager() -> None:
    assert hasattr(RootLock, "__aenter__")
    assert hasattr(RootLock, "__aexit__")

    enter_annotation = RootLock.__aenter__.__annotations__.get("return")
    exit_annotations = RootLock.__aexit__.__annotations__
    assert enter_annotation in {"Self", Self}
    assert exit_annotations.get("exc_type") in {
        "type[BaseException] | None",
        type[BaseException] | None,
    }
    assert exit_annotations.get("traceback") in {"TracebackType | None", TracebackType | None}


@pytest.mark.asyncio
async def test_require_acquired_is_bound_to_exact_context_lifetime(tmp_path: Path) -> None:
    root_lock = RootLock(tmp_path / "root.lock", timeout_seconds=0)

    for phase in ("before", "after"):
        if phase == "after":
            async with root_lock:
                root_lock.require_acquired()
        with pytest.raises(BridgeError) as caught:
            root_lock.require_acquired()
        assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_root_lock_path_remains_publicly_readable_and_rebindable(tmp_path: Path) -> None:
    original = tmp_path / "root.lock"
    root_lock = RootLock(original, timeout_seconds=0)
    assert root_lock.path == original

    rebound = tmp_path / "other.lock"
    root_lock.path = rebound
    assert root_lock.path == rebound


@pytest.mark.asyncio
async def test_require_acquired_rejects_rebind_to_store_path_after_other_path_was_held(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "store.lock"
    held_path = tmp_path / "other.lock"
    root_lock = RootLock(store_path, timeout_seconds=0)
    root_lock.path = held_path

    async with root_lock:
        root_lock.path = store_path
        with pytest.raises(BridgeError) as caught:
            root_lock.require_acquired()
        assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_require_acquired_maps_missing_public_path_to_state_conflict(tmp_path: Path) -> None:
    root_lock = RootLock(tmp_path / "root.lock", timeout_seconds=0)
    del root_lock.path

    with pytest.raises(BridgeError) as caught:
        root_lock.require_acquired()
    assert caught.value.code is ErrorCode.STATE_CONFLICT
