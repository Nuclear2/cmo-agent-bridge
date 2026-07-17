import os
from pathlib import Path
from typing import Never, Self
from unittest.mock import Mock

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.transports.file_bridge.atomic_io import atomic_replace_bytes


def _sharing_error(winerror: int) -> OSError:
    error = OSError(f"sharing violation {winerror}")
    error.winerror = winerror
    return error


class _CleanupChainStream:
    def __init__(
        self,
        *,
        write_error: OSError | None = None,
        close_errors: list[OSError] | None = None,
    ) -> None:
        self.write_error = write_error
        self.close_errors = list(close_errors or [])
        self.close_calls = 0
        self.descriptor: int | None = None

    def take_descriptor(self, descriptor: int) -> None:
        self.descriptor = descriptor

    def __enter__(self) -> Self:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> bool:
        self.close()
        return False

    def write(self, data: bytes) -> int:
        if self.write_error is not None:
            raise self.write_error
        return len(data)

    def flush(self) -> None:
        pass

    def fileno(self) -> int:
        return 123

    def close(self) -> None:
        self.close_calls += 1
        if self.descriptor is not None:
            os.close(self.descriptor)
            self.descriptor = None
        if self.close_errors:
            raise self.close_errors.pop(0)


def test_atomic_replace_writes_new_target(tmp_path: Path) -> None:
    target = tmp_path / "request.lua"

    atomic_replace_bytes(target, b"new", retry_seconds=0)

    assert target.read_bytes() == b"new"
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.parametrize("winerror", [32, 33])
def test_atomic_replace_retries_sharing_errors_with_one_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, winerror: int
) -> None:
    target = tmp_path / "request.lua"
    target.write_bytes(b"old")
    real_replace = os.replace
    sources: list[Path] = []
    calls = 0

    def flaky_replace(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
        nonlocal calls
        calls += 1
        sources.append(Path(source))
        if calls < 3:
            raise _sharing_error(winerror)
        real_replace(source, destination)

    monkeypatch.setattr(os, "replace", flaky_replace)

    atomic_replace_bytes(target, b"new", retry_seconds=1)

    assert target.read_bytes() == b"new"
    assert calls == 3
    assert len(set(sources)) == 1
    assert not sources[0].exists()


def test_atomic_replace_preserves_old_target_on_sharing_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "request.lua"
    target.write_bytes(b"old")
    unrelated = tmp_path / ".request.lua.cmo-agent-bridge-unrelated.tmp"
    unrelated.write_bytes(b"keep")
    sharing = _sharing_error(32)
    replace = Mock(side_effect=sharing)
    monkeypatch.setattr(os, "replace", replace)

    with pytest.raises(BridgeError) as caught:
        atomic_replace_bytes(target, b"new", retry_seconds=0)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert target.read_bytes() == b"old"
    assert unrelated.read_bytes() == b"keep"
    temp = Path(replace.call_args.args[0])
    assert not temp.exists()


def test_deadline_is_rechecked_before_retry_after_sleep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "request.lua"
    target.write_bytes(b"old")
    clock = {"now": 0.0}
    replace = Mock(side_effect=_sharing_error(32))

    def monotonic() -> float:
        return clock["now"]

    def sleep(seconds: float) -> None:
        clock["now"] += seconds

    monkeypatch.setattr(os, "replace", replace)
    monkeypatch.setattr(
        "cmo_agent_bridge.transports.file_bridge.atomic_io.time.monotonic", monotonic
    )
    monkeypatch.setattr("cmo_agent_bridge.transports.file_bridge.atomic_io.time.sleep", sleep)

    with pytest.raises(BridgeError) as caught:
        atomic_replace_bytes(target, b"new", retry_seconds=0.005)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert replace.call_count == 1
    assert target.read_bytes() == b"old"


def test_atomic_replace_propagates_non_sharing_error_immediately(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "request.lua"
    target.write_bytes(b"old")
    denied = PermissionError("denied")
    denied.winerror = 5
    replace = Mock(side_effect=denied)
    sleep = Mock()
    monkeypatch.setattr(os, "replace", replace)
    monkeypatch.setattr("cmo_agent_bridge.transports.file_bridge.atomic_io.time.sleep", sleep)

    with pytest.raises(PermissionError) as caught:
        atomic_replace_bytes(target, b"new", retry_seconds=1)

    assert caught.value is denied
    assert replace.call_count == 1
    sleep.assert_not_called()
    assert target.read_bytes() == b"old"
    assert not Path(replace.call_args.args[0]).exists()


def test_atomic_replace_cleans_exact_temp_after_fsync_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "request.lua"
    target.write_bytes(b"old")
    unrelated = tmp_path / ".request.lua.cmo-agent-bridge-unrelated.tmp"
    unrelated.write_bytes(b"keep")
    replace = Mock()
    failure = OSError("fsync failed")
    monkeypatch.setattr(os, "fsync", Mock(side_effect=failure))
    monkeypatch.setattr(os, "replace", replace)

    with pytest.raises(OSError) as caught:
        atomic_replace_bytes(target, b"new", retry_seconds=1)

    assert caught.value is failure
    replace.assert_not_called()
    assert target.read_bytes() == b"old"
    assert unrelated.read_bytes() == b"keep"
    assert sorted(path.name for path in tmp_path.iterdir()) == sorted([target.name, unrelated.name])


def _assert_cleanup_failure_recorded(primary: BaseException, cleanup: OSError) -> None:
    notes = getattr(primary, "__notes__", [])
    assert any("temporary file cleanup failed" in note for note in notes)
    assert any(str(cleanup) in note for note in notes)


def _assert_note_contains(primary: BaseException, text: str) -> None:
    notes = getattr(primary, "__notes__", [])
    assert any(text in note for note in notes)


def test_fdopen_primary_survives_descriptor_close_and_unlink_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "request.lua"
    primary = OSError("fdopen failed")
    close_cleanup = OSError("descriptor close failed")
    unlink_cleanup = OSError("unlink failed")
    close_calls: list[int] = []
    unlink_calls: list[Path] = []
    real_close = os.close
    real_unlink = Path.unlink

    def fail_fdopen(_descriptor: int, _mode: str) -> Never:
        raise primary

    def fail_close(descriptor: int) -> None:
        close_calls.append(descriptor)
        raise close_cleanup

    def fail_unlink(path: Path, missing_ok: bool = False) -> None:
        del missing_ok
        unlink_calls.append(path)
        raise unlink_cleanup

    monkeypatch.setattr(os, "fdopen", fail_fdopen)
    monkeypatch.setattr(os, "close", fail_close)
    monkeypatch.setattr(Path, "unlink", fail_unlink)

    with pytest.raises(BaseException) as caught:
        atomic_replace_bytes(target, b"new", retry_seconds=0)

    real_close(close_calls[0])
    real_unlink(unlink_calls[0])
    assert caught.value is primary
    assert len(close_calls) == 1
    assert len(unlink_calls) == 1
    _assert_note_contains(primary, "descriptor close failed")
    _assert_note_contains(primary, "unlink failed")


def test_write_primary_survives_stream_close_and_unlink_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "request.lua"
    primary = OSError("write failed")
    close_cleanup = OSError("stream close failed")
    unlink_cleanup = OSError("unlink failed")
    stream = _CleanupChainStream(write_error=primary, close_errors=[close_cleanup])
    unlink_calls: list[Path] = []
    real_unlink = Path.unlink

    def provide_stream(_descriptor: int, _mode: str) -> _CleanupChainStream:
        stream.take_descriptor(_descriptor)
        return stream

    def fail_unlink(path: Path, missing_ok: bool = False) -> None:
        del missing_ok
        unlink_calls.append(path)
        raise unlink_cleanup

    monkeypatch.setattr(os, "fdopen", provide_stream)
    monkeypatch.setattr(Path, "unlink", fail_unlink)

    with pytest.raises(BaseException) as caught:
        atomic_replace_bytes(target, b"new", retry_seconds=0)

    real_unlink(unlink_calls[0])
    assert caught.value is primary
    assert stream.close_calls == 1
    assert len(unlink_calls) == 1
    _assert_note_contains(primary, "stream close failed")
    _assert_note_contains(primary, "unlink failed")


def test_fsync_primary_survives_stream_close_failure_and_unlinks_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "request.lua"
    primary = OSError("fsync failed")
    close_cleanup = OSError("stream close failed")
    stream = _CleanupChainStream(close_errors=[close_cleanup])

    def provide_stream(_descriptor: int, _mode: str) -> _CleanupChainStream:
        stream.take_descriptor(_descriptor)
        return stream

    def fail_fsync(_descriptor: int) -> Never:
        raise primary

    monkeypatch.setattr(os, "fdopen", provide_stream)
    monkeypatch.setattr(os, "fsync", fail_fsync)

    with pytest.raises(BaseException) as caught:
        atomic_replace_bytes(target, b"new", retry_seconds=0)

    assert caught.value is primary
    assert stream.close_calls == 1
    _assert_note_contains(primary, "stream close failed")
    assert list(tmp_path.iterdir()) == []


def test_stream_close_primary_survives_retry_close_and_unlink_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "request.lua"
    primary = OSError("initial stream close failed")
    retry_cleanup = OSError("retry stream close failed")
    unlink_cleanup = OSError("unlink failed")
    stream = _CleanupChainStream(close_errors=[primary, retry_cleanup])
    unlink_calls: list[Path] = []
    real_unlink = Path.unlink

    def provide_stream(_descriptor: int, _mode: str) -> _CleanupChainStream:
        stream.take_descriptor(_descriptor)
        return stream

    def successful_fsync(_descriptor: int) -> None:
        pass

    def fail_unlink(path: Path, missing_ok: bool = False) -> None:
        del missing_ok
        unlink_calls.append(path)
        raise unlink_cleanup

    monkeypatch.setattr(os, "fdopen", provide_stream)
    monkeypatch.setattr(os, "fsync", successful_fsync)
    monkeypatch.setattr(Path, "unlink", fail_unlink)

    with pytest.raises(BaseException) as caught:
        atomic_replace_bytes(target, b"new", retry_seconds=0)

    real_unlink(unlink_calls[0])
    assert caught.value is primary
    assert stream.close_calls == 2
    assert len(unlink_calls) == 1
    _assert_note_contains(primary, "retry stream close failed")
    _assert_note_contains(primary, "unlink failed")


def test_cleanup_failure_does_not_mask_replace_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "request.lua"
    target.write_bytes(b"old")
    primary = PermissionError("replace failed")
    primary.winerror = 5
    cleanup = OSError("cleanup failed")
    monkeypatch.setattr(os, "replace", Mock(side_effect=primary))
    monkeypatch.setattr(Path, "unlink", Mock(side_effect=cleanup))

    with pytest.raises(BaseException) as caught:
        atomic_replace_bytes(target, b"new", retry_seconds=1)

    assert caught.value is primary
    _assert_cleanup_failure_recorded(primary, cleanup)


def test_cleanup_failure_does_not_mask_fsync_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "request.lua"
    target.write_bytes(b"old")
    primary = OSError("fsync failed")
    cleanup = OSError("cleanup failed")
    monkeypatch.setattr(os, "fsync", Mock(side_effect=primary))
    monkeypatch.setattr(Path, "unlink", Mock(side_effect=cleanup))

    with pytest.raises(BaseException) as caught:
        atomic_replace_bytes(target, b"new", retry_seconds=1)

    assert caught.value is primary
    _assert_cleanup_failure_recorded(primary, cleanup)


def test_cleanup_failure_does_not_mask_sharing_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "request.lua"
    target.write_bytes(b"old")
    cleanup = OSError("cleanup failed")
    monkeypatch.setattr(os, "replace", Mock(side_effect=_sharing_error(32)))
    monkeypatch.setattr(Path, "unlink", Mock(side_effect=cleanup))

    with pytest.raises(BaseException) as caught:
        atomic_replace_bytes(target, b"new", retry_seconds=0)

    assert isinstance(caught.value, BridgeError)
    assert caught.value.code is ErrorCode.STATE_CONFLICT
    _assert_cleanup_failure_recorded(caught.value, cleanup)


def test_atomic_replace_does_not_create_target_parent(tmp_path: Path) -> None:
    parent = tmp_path / "missing"

    with pytest.raises(FileNotFoundError):
        atomic_replace_bytes(parent / "request.lua", b"new", retry_seconds=1)

    assert not parent.exists()


def test_atomic_replace_rejects_negative_retry_before_writing(tmp_path: Path) -> None:
    target = tmp_path / "request.lua"

    with pytest.raises(BridgeError) as caught:
        atomic_replace_bytes(target, b"new", retry_seconds=-0.1)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert list(tmp_path.iterdir()) == []
