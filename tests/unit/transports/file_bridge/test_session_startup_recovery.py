from __future__ import annotations

import asyncio
from pathlib import Path
import sys
from types import TracebackType
from typing import Any, cast

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.models import ExchangeCommand
from cmo_agent_bridge.protocol.response_models import ResponseArtifact
from cmo_agent_bridge.state.models import HostRequestState
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.models import (
    RecoveryDisposition,
    RecoveryReport,
)
from cmo_agent_bridge.transports.file_bridge.recovery import RecoveryManager
from cmo_agent_bridge.transports.file_bridge.transport import (
    _FileBridgeChannel,  # pyright: ignore[reportPrivateUsage]
)

sys.path.insert(0, str(Path(__file__).parent))

import test_read_exchange as base  # noqa: E402


harness = base.harness


def _settled_report() -> RecoveryReport:
    return RecoveryReport(
        disposition=RecoveryDisposition.SETTLED,
        request_id=base.REQUEST_ID,
        request_state=HostRequestState.COMPLETED,
        response_cleanup_required=True,
    )


def _quarantined_report() -> RecoveryReport:
    return RecoveryReport(
        disposition=RecoveryDisposition.QUARANTINED,
        request_id=base.REQUEST_ID,
        request_state=HostRequestState.QUARANTINED,
        response_cleanup_required=False,
    )


def _forbid_bare_journal_gate(*_args: object, **_kwargs: object) -> None:
    raise AssertionError("session entry used a bare journal load instead of startup recovery")


async def _require_event_before_task(
    event: asyncio.Event,
    task: asyncio.Task[object],
    *,
    label: str,
) -> None:
    gate = asyncio.create_task(event.wait())
    try:
        done, _pending = await asyncio.wait(
            {gate, task},
            timeout=2,
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert done, f"{label} stalled"
        if task in done:
            await task
            pytest.fail(f"{label} task completed before its event")
        assert event.is_set(), label
    finally:
        if not gate.done():
            gate.cancel("event race cleanup")
        try:
            await gate
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_session_fake_settlement_queues_path_but_retains_without_durable_artifact(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = base._transport(harness)  # pyright: ignore[reportPrivateUsage]
    response_path = harness.paths.response_path(base.REQUEST_ID)
    response_path.write_bytes(b"settled startup response")
    trace: list[str] = []
    managers: list[RecoveryManager] = []
    installed_exit = RootLock.__aexit__

    async def recover(manager: RecoveryManager) -> RecoveryReport:
        harness.root_lock.require_acquired()
        managers.append(manager)
        trace.append("recover")
        return _settled_report()

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is harness.root_lock:
            trace.append("unlock")
        return await installed_exit(lock, exc_type, exc, traceback)

    monkeypatch.setattr(RecoveryManager, "recover_pending", recover)
    monkeypatch.setattr(transport.journals, "load", _forbid_bare_journal_gate)
    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)

    async with transport.session() as channel:
        concrete = cast(_FileBridgeChannel, channel)
        assert managers == [concrete._recovery_manager]  # pyright: ignore[reportPrivateUsage]
        assert (
            concrete._mutation_exchange._recovery_manager  # pyright: ignore[reportPrivateUsage]
            is concrete._recovery_manager  # pyright: ignore[reportPrivateUsage]
        )
        assert concrete._cleanup_paths == [response_path]  # pyright: ignore[reportPrivateUsage]
        assert concrete._poisoned is False  # pyright: ignore[reportPrivateUsage]
        assert response_path.exists()

    assert trace == ["recover", "unlock"]
    assert response_path.read_bytes() == b"settled startup response"


@pytest.mark.asyncio
async def test_quarantined_startup_report_does_not_poison_reads_and_public_recovery_is_guarded(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = base._transport(harness)  # pyright: ignore[reportPrivateUsage]
    reports: list[RecoveryReport] = []
    artifact = cast(ResponseArtifact, object())

    async def recover(manager: RecoveryManager) -> RecoveryReport:
        harness.root_lock.require_acquired()
        report = _quarantined_report()
        reports.append(report)
        return report

    async def read_through_quarantine(
        _channel: _FileBridgeChannel,
        _command: ExchangeCommand,
    ) -> ResponseArtifact:
        return artifact

    monkeypatch.setattr(RecoveryManager, "recover_pending", recover)
    monkeypatch.setattr(transport.journals, "load", _forbid_bare_journal_gate)
    monkeypatch.setattr(_FileBridgeChannel, "_perform_exchange", read_through_quarantine)

    concrete: _FileBridgeChannel | None = None
    async with transport.session() as channel:
        concrete = cast(_FileBridgeChannel, channel)
        assert reports == [_quarantined_report()]
        assert concrete._poisoned is False  # pyright: ignore[reportPrivateUsage]
        assert concrete._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
        assert await channel.exchange(cast(ExchangeCommand, object())) is artifact

        current = asyncio.current_task()
        assert current is not None
        concrete._active_task = current  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(BridgeError) as active:
            await concrete.recover_pending()
        assert active.value.code is ErrorCode.STATE_CONFLICT
        concrete._active_task = None  # pyright: ignore[reportPrivateUsage]

        concrete._closing = True  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(BridgeError) as closing:
            await concrete.recover_pending()
        assert closing.value.code is ErrorCode.STATE_CONFLICT
        concrete._closing = False  # pyright: ignore[reportPrivateUsage]

        assert await concrete.recover_pending() == _quarantined_report()
        assert reports == [_quarantined_report(), _quarantined_report()]
        assert concrete._poisoned is False  # pyright: ignore[reportPrivateUsage]

    assert concrete is not None
    with pytest.raises(BridgeError) as closed:
        await concrete.recover_pending()
    assert closed.value.code is ErrorCode.STATE_CONFLICT
    assert reports == [_quarantined_report(), _quarantined_report()]


@pytest.mark.asyncio
async def test_cancelled_fake_startup_recovery_retains_response_without_durable_artifact(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = base._transport(harness)  # pyright: ignore[reportPrivateUsage]
    response_path = harness.paths.response_path(base.REQUEST_ID)
    response_path.write_bytes(b"cancelled startup response")
    started = asyncio.Event()
    release = asyncio.Event()
    trace: list[str] = []
    worker_tasks: list[asyncio.Task[Any]] = []
    observed_cancellations: list[asyncio.CancelledError] = []
    installed_exit = RootLock.__aexit__

    async def recover(_manager: RecoveryManager) -> RecoveryReport:
        harness.root_lock.require_acquired()
        worker = asyncio.current_task()
        assert worker is not None
        worker_tasks.append(worker)
        trace.append("worker.started")
        started.set()
        await release.wait()
        harness.root_lock.require_acquired()
        trace.append("worker.durable")
        return _settled_report()

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is harness.root_lock:
            trace.append("unlock")
        return await installed_exit(lock, exc_type, exc, traceback)

    monkeypatch.setattr(RecoveryManager, "recover_pending", recover)
    monkeypatch.setattr(transport.journals, "load", _forbid_bare_journal_gate)
    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)
    session = transport.session()

    async def enter() -> object:
        try:
            return await session.__aenter__()
        except asyncio.CancelledError as error:
            observed_cancellations.append(error)
            raise

    task = asyncio.create_task(enter())
    await _require_event_before_task(
        started,
        task,
        label="startup recovery worker start",
    )
    message = "cancel session startup while recovery is durable"
    assert task.cancel(message) is True
    await asyncio.sleep(0)
    assert not task.done()
    assert worker_tasks and not worker_tasks[0].done()
    harness.root_lock.require_acquired()

    release.set()
    with pytest.raises(asyncio.CancelledError) as caught:
        async with asyncio.timeout(2):
            await task

    assert caught.value.args == (message,)
    assert observed_cancellations == [caught.value]
    assert observed_cancellations[0] is caught.value
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert worker_tasks[0].done() and not worker_tasks[0].cancelled()
    assert trace == ["worker.started", "worker.durable", "unlock"]
    assert response_path.read_bytes() == b"cancelled startup response"


@pytest.mark.asyncio
async def test_session_startup_propagates_exact_recovery_worker_failure_and_releases_lock(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = base._transport(harness)  # pyright: ignore[reportPrivateUsage]
    sentinel = RuntimeError("startup recovery worker sentinel")
    calls = 0

    async def fail(_manager: RecoveryManager) -> RecoveryReport:
        nonlocal calls
        harness.root_lock.require_acquired()
        calls += 1
        raise sentinel

    monkeypatch.setattr(RecoveryManager, "recover_pending", fail)
    monkeypatch.setattr(transport.journals, "load", _forbid_bare_journal_gate)
    session = transport.session()

    with pytest.raises(RuntimeError) as caught:
        await session.__aenter__()
    assert caught.value is sentinel
    assert calls == 1

    async with harness.root_lock:
        harness.root_lock.require_acquired()
