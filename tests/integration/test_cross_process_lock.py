from __future__ import annotations

import asyncio
import subprocess
import sys
import time
from pathlib import Path

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.transports.file_bridge.lock import RootLock


_HELPER = r"""
import asyncio
import sys
from pathlib import Path

from cmo_agent_bridge.transports.file_bridge.lock import RootLock


async def main() -> None:
    lock_path = Path(sys.argv[1])
    ready_path = Path(sys.argv[2])
    release_path = Path(sys.argv[3])
    async with RootLock(lock_path, timeout_seconds=2):
        ready_path.write_text("ready", encoding="utf-8")
        while not release_path.exists():
            await asyncio.sleep(0.01)


asyncio.run(main())
"""


async def _wait_for_ready(process: subprocess.Popen[bytes], ready_path: Path) -> None:
    deadline = time.monotonic() + 5
    while not ready_path.exists():
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise AssertionError(
                f"lock helper exited before ready: {process.returncode}; "
                f"stdout={stdout!r}; stderr={stderr!r}"
            )
        if time.monotonic() >= deadline:
            raise AssertionError("lock helper did not signal ready within five seconds")
        await asyncio.sleep(0.01)


def _start_helper(lock_path: Path, ready_path: Path, release_path: Path) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-c", _HELPER, str(lock_path), str(ready_path), str(release_path)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


async def _bounded_reacquire(lock_path: Path) -> None:
    async def acquire() -> None:
        async with RootLock(lock_path, timeout_seconds=1):
            pass

    await asyncio.wait_for(acquire(), timeout=2)


@pytest.mark.asyncio
async def test_lock_reacquires_after_helper_exits_normally(tmp_path: Path) -> None:
    lock_path = tmp_path / "root.lock"
    ready_path = tmp_path / "ready"
    release_path = tmp_path / "release"
    helper = _start_helper(lock_path, ready_path, release_path)
    try:
        await _wait_for_ready(helper, ready_path)
        with pytest.raises(BridgeError) as caught:
            async with RootLock(lock_path, timeout_seconds=0.05):
                raise AssertionError("unreachable")
        assert caught.value.code is ErrorCode.STATE_CONFLICT

        release_path.write_text("release", encoding="utf-8")
        assert helper.wait(timeout=5) == 0
        await _bounded_reacquire(lock_path)
    finally:
        if helper.poll() is None:
            helper.kill()
            helper.wait(timeout=5)


@pytest.mark.asyncio
async def test_lock_reacquires_after_helper_is_forcibly_terminated(tmp_path: Path) -> None:
    lock_path = tmp_path / "root.lock"
    ready_path = tmp_path / "ready"
    release_path = tmp_path / "release"
    helper = _start_helper(lock_path, ready_path, release_path)
    try:
        await _wait_for_ready(helper, ready_path)
        with pytest.raises(BridgeError) as caught:
            async with RootLock(lock_path, timeout_seconds=0.05):
                raise AssertionError("unreachable")
        assert caught.value.code is ErrorCode.STATE_CONFLICT

        helper.kill()
        helper.wait(timeout=5)
        await _bounded_reacquire(lock_path)
    finally:
        if helper.poll() is None:
            helper.kill()
            helper.wait(timeout=5)
