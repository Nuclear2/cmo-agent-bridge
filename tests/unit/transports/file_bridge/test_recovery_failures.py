from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import AbstractAsyncContextManager
from pathlib import Path
import sys
from types import TracebackType
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.lua_delivery import render_idle_lua
from cmo_agent_bridge.protocol.models import ExchangeCommand, PreparedDelivery
from cmo_agent_bridge.protocol.response_models import CompletedSettlement, ResponseArtifact
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    PendingJournal,
    PendingPhase,
)
from cmo_agent_bridge.state.pending_journal import JournalDeleteExpectation, JournalRevisions
from cmo_agent_bridge.state.request_ledger import DeliveryRecord, RequestRecord
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.models import BridgeChannel
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo
from cmo_agent_bridge.transports.file_bridge.response_waiter import ResponseWaiter
from cmo_agent_bridge.transports.file_bridge.transport import (
    FileBridgeTransport,
    _FileBridgeChannel,  # pyright: ignore[reportPrivateUsage]
)

sys.path.insert(0, str(Path(__file__).parent))

import test_recovery as base  # noqa: E402

if TYPE_CHECKING:
    from tests.helpers.fake_file_bridge_peer import Respond, StaySilent
else:
    sys.path.insert(0, str(Path(__file__).parents[3] / "helpers"))
    from fake_file_bridge_peer import Respond, StaySilent  # noqa: E402


harness = base.harness

_TEST_TIMEOUT_SECONDS = 2


async def _bounded(awaitable: Any, *, label: str) -> Any:
    try:
        async with asyncio.timeout(_TEST_TIMEOUT_SECONDS):
            return await awaitable
    except TimeoutError as error:
        raise AssertionError(f"{label} exceeded the test deadline") from error


async def _finish_quietly(task: asyncio.Task[Any]) -> None:
    if not task.done():
        task.cancel("recovery failure test cleanup")
    try:
        await _bounded(asyncio.shield(task), label="recovery failure task cleanup")
    except BaseException:
        pass
    assert task.done()


async def _enter_session_bounded(
    session: AbstractAsyncContextManager[BridgeChannel],
    *,
    label: str,
) -> BridgeChannel:
    return cast(
        BridgeChannel,
        await _bounded(session.__aenter__(), label=f"{label} enter"),
    )


async def _exit_session_bounded(
    session: AbstractAsyncContextManager[BridgeChannel],
    channel: _FileBridgeChannel,
    *,
    label: str,
) -> bool | None:
    exit_task = asyncio.create_task(session.__aexit__(None, None, None))
    done, _ = await asyncio.wait({exit_task}, timeout=_TEST_TIMEOUT_SECONDS)
    if exit_task not in done:
        active = channel._active_task  # pyright: ignore[reportPrivateUsage]
        if active is not None and not active.done():
            active.cancel(f"{label} bounded-exit active-task cleanup")
        recovery = channel._mutation_exchange._recovery_task  # pyright: ignore[reportPrivateUsage]
        if recovery is not None and not recovery.done():
            recovery.cancel(f"{label} bounded-exit recovery-task cleanup")
        exit_task.cancel(f"{label} exceeded the session-exit deadline")
        done, _ = await asyncio.wait({exit_task}, timeout=_TEST_TIMEOUT_SECONDS)
        assert exit_task in done, f"{label} session exit remained live after forced cleanup"
        pytest.fail(f"{label} session exit exceeded the test deadline")
    return await exit_task


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _transport(harness: base.Harness) -> FileBridgeTransport:
    return base._transport(harness)  # pyright: ignore[reportPrivateUsage]


def _command(harness: base.Harness) -> ExchangeCommand:
    return base._command(harness)  # pyright: ignore[reportPrivateUsage]


def _install_unlock_probe(
    monkeypatch: pytest.MonkeyPatch,
    *,
    harness: base.Harness,
    transport: FileBridgeTransport,
    command: ExchangeCommand,
    trace: list[str],
) -> list[tuple[PendingPhase | None, HostRequestState | None]]:
    original_exit = RootLock.__aexit__
    observations: list[tuple[PendingPhase | None, HostRequestState | None]] = []

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is harness.lock:
            lock.require_acquired()
            loaded = transport.journals.load()
            request = transport.ledger.get_request(command.request_id)
            observations.append(
                (
                    None if loaded is None else loaded.journal.original.state,
                    None if request is None else request.state,
                )
            )
            trace.append("owner.unlock")
        return await original_exit(lock, exc_type, exc, traceback)

    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)
    return observations


def _assert_original_cancellation_identity(
    caught: asyncio.CancelledError,
    observed: list[asyncio.CancelledError],
    *,
    message: str,
) -> None:
    assert caught.args == (message,)
    assert observed == [caught]
    assert observed[0] is caught
    assert caught.__cause__ is None
    assert caught.__context__ is None


def _assert_quarantined_before_unlock(
    *,
    trace: list[str],
    observations: list[tuple[PendingPhase | None, HostRequestState | None]],
) -> None:
    assert observations == [(PendingPhase.QUARANTINED, HostRequestState.QUARANTINED)]
    assert trace.index("journal.r4_quarantined") < trace.index("cancellation.reraised")
    assert trace.index("cancellation.reraised") < trace.index("owner.unlock")


@pytest.mark.asyncio
async def test_cancel_wait_process_identity_drift_quarantines_before_unlock(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    transport = _transport(harness)
    command = _command(harness)
    trace: list[str] = []
    original_wait_started = asyncio.Event()
    wait_cancellations: list[asyncio.CancelledError] = []
    original_wait = ResponseWaiter.wait
    original_save = transport.journals.save

    async def traced_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        if len(waiter._expectation.allowed_deliveries) == 1:  # pyright: ignore[reportPrivateUsage]
            original_wait_started.set()
            try:
                return await original_wait(waiter, timeout_seconds)
            except asyncio.CancelledError as error:
                wait_cancellations.append(error)
                raise
        trace.append("wait.cancel.process_drift")
        return await original_wait(waiter, timeout_seconds)

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        revisions = original_save(journal, expected_revisions=expected_revisions)
        if journal.original.state is PendingPhase.QUARANTINED:
            trace.append("journal.r4_quarantined")
        return revisions

    monkeypatch.setattr(ResponseWaiter, "wait", traced_wait)
    monkeypatch.setattr(transport.journals, "save", traced_save)
    unlock_observations = _install_unlock_probe(
        monkeypatch,
        harness=harness,
        transport=transport,
        command=command,
        trace=trace,
    )
    message = "cancel before recovery process drift"
    caught_error: asyncio.CancelledError | None = None
    final_journal: PendingJournal | None = None
    final_request: RequestRecord | None = None
    task: asyncio.Task[ResponseArtifact] | None = None
    session = transport.session()
    channel: _FileBridgeChannel | None = None

    try:
        channel = cast(
            _FileBridgeChannel,
            await _enter_session_bounded(session, label="process-drift recovery session"),
        )
        task = asyncio.create_task(channel.exchange(command))
        try:
            await base._require_event_before_task(  # pyright: ignore[reportPrivateUsage]
                original_wait_started,
                task,
                "exchange ended before original waiter during process-drift setup",
            )
            assert task.cancel(message) is True
            harness.inspector.process = ProcessInfo(
                pid=harness.process.pid + 1,
                create_time=harness.process.create_time + 1,
                executable=harness.process.executable,
            )
            with pytest.raises(asyncio.CancelledError) as caught:
                await base._bounded_task_outcome(  # pyright: ignore[reportPrivateUsage]
                    task,
                    label="process-drift cancellation recovery",
                )
            caught_error = caught.value
            trace.append("cancellation.reraised")
            loaded = transport.journals.load()
            assert loaded is not None
            final_journal = loaded.journal
            final_request = transport.ledger.get_request(command.request_id)
        finally:
            if not task.done():
                await _finish_quietly(task)
    finally:
        if channel is not None:
            await _exit_session_bounded(
                session,
                channel,
                label="process-drift recovery session",
            )

    assert caught_error is not None
    _assert_original_cancellation_identity(caught_error, wait_cancellations, message=message)
    assert final_journal is not None
    assert final_journal.original.state is PendingPhase.QUARANTINED
    assert final_journal.original.response_artifact is None
    assert final_request is not None
    assert final_request.state is HostRequestState.QUARANTINED
    assert final_request.error_json is not None
    assert json.loads(final_request.error_json)["code"] == ErrorCode.INDETERMINATE_OUTCOME.value
    _assert_quarantined_before_unlock(trace=trace, observations=unlock_observations)


async def _run_no_ack_cancellation(
    *,
    harness: base.Harness,
    transport: FileBridgeTransport,
    command: ExchangeCommand,
    peer: Any,
    rig: Any,
    trace: list[str],
    message: str,
) -> tuple[asyncio.CancelledError, PendingJournal, _FileBridgeChannel]:
    task: asyncio.Task[ResponseArtifact] | None = None
    peer_running = False
    caught_error: asyncio.CancelledError | None = None
    journal: PendingJournal | None = None
    concrete_channel: _FileBridgeChannel | None = None
    session = transport.session()

    await _bounded(peer.start(), label="failure-matrix fake peer start")
    peer_running = True
    try:
        concrete_channel = cast(
            _FileBridgeChannel,
            await _enter_session_bounded(session, label="failure-matrix session"),
        )
        try:
            task = asyncio.create_task(concrete_channel.exchange(command))
            try:
                await base._require_event_before_task(  # pyright: ignore[reportPrivateUsage]
                    rig.original_wait_started,
                    task,
                    "exchange ended before original response wait",
                )
                await base._require_event_before_task(  # pyright: ignore[reportPrivateUsage]
                    rig.original_observed,
                    task,
                    "exchange ended before peer observed original delivery",
                )
                await _bounded(peer.stop(), label="failure-matrix fake peer stop")
                peer_running = False
                rig.allow_cancel_poll.set()
                assert task.cancel(message) is True
                with pytest.raises(asyncio.CancelledError) as caught:
                    await base._bounded_task_outcome(  # pyright: ignore[reportPrivateUsage]
                        task,
                        label="failure-matrix cancellation recovery",
                    )
                caught_error = caught.value
                trace.append("cancellation.reraised")
                loaded = transport.journals.load()
                assert loaded is not None
                journal = loaded.journal
            finally:
                if not task.done():
                    await _finish_quietly(task)
        finally:
            await _exit_session_bounded(
                session,
                concrete_channel,
                label="failure-matrix session",
            )
    finally:
        rig.allow_cancel_poll.set()
        if peer_running:
            await _bounded(peer.stop(), label="failure-matrix peer final stop")

    assert caught_error is not None
    assert journal is not None
    assert concrete_channel is not None
    return caught_error, journal, concrete_channel


@pytest.mark.asyncio
async def test_r3_journal_save_aftercommit_is_reread_and_no_ack_quarantines(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _command(harness)
    trace: list[str] = []
    peer = base._peer(harness, trace)  # pyright: ignore[reportPrivateUsage]
    peer.enqueue(StaySilent())
    rig = base._NoAckRig(trace=trace)  # pyright: ignore[reportPrivateUsage]
    base._install_no_ack_rig(  # pyright: ignore[reportPrivateUsage]
        monkeypatch,
        transport=transport,
        peer=peer,
        command=command,
        rig=rig,
    )
    installed_save = transport.journals.save
    installed_load = transport.journals.load
    aftercommit = RuntimeError("r3 journal save aftercommit sentinel")
    fault_injected = False

    def save_then_fail_once(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal fault_injected
        revisions = installed_save(journal, expected_revisions=expected_revisions)
        if journal.original.revision == 3 and not fault_injected:
            fault_injected = True
            trace.append("journal.r3.aftercommit")
            raise aftercommit
        return revisions

    def observe_recovery_reread() -> Any:
        loaded = installed_load()
        if (
            fault_injected
            and "cancellation.reraised" not in trace
            and loaded is not None
            and loaded.journal.original.revision == 3
        ):
            trace.append("journal.r3.reread_exact")
        return loaded

    monkeypatch.setattr(transport.journals, "save", save_then_fail_once)
    monkeypatch.setattr(transport.journals, "load", observe_recovery_reread)
    unlock_observations = _install_unlock_probe(
        monkeypatch,
        harness=harness,
        transport=transport,
        command=command,
        trace=trace,
    )
    message = "cancel across r3 journal aftercommit"

    caught, journal, channel = await _run_no_ack_cancellation(
        harness=harness,
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        trace=trace,
        message=message,
    )

    assert fault_injected is True
    _assert_original_cancellation_identity(caught, rig.wait_cancellations, message=message)
    assert "journal.r3.reread_exact" in trace
    assert trace.index("journal.r3.aftercommit") < trace.index("journal.r3.reread_exact")
    assert all(str(aftercommit) not in note for note in getattr(caught, "__notes__", ()))
    base._assert_no_ack_quarantine(  # pyright: ignore[reportPrivateUsage]
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        journal=journal,
    )
    assert channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    _assert_quarantined_before_unlock(trace=trace, observations=unlock_observations)


@pytest.mark.asyncio
async def test_r4_ledger_transition_aftercommit_is_reread_as_exact_convergence(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _command(harness)
    trace: list[str] = []
    peer = base._peer(harness, trace)  # pyright: ignore[reportPrivateUsage]
    peer.enqueue(StaySilent())
    rig = base._NoAckRig(trace=trace)  # pyright: ignore[reportPrivateUsage]
    base._install_no_ack_rig(  # pyright: ignore[reportPrivateUsage]
        monkeypatch,
        transport=transport,
        peer=peer,
        command=command,
        rig=rig,
    )
    installed_transition = transport.ledger.transition
    installed_get_request = transport.ledger.get_request
    aftercommit = RuntimeError("r4 ledger transition aftercommit sentinel")
    fault_injected = False

    def transition_then_fail_once(
        request_id: Any,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal fault_injected
        result = installed_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        if new_state is HostRequestState.QUARANTINED and not fault_injected:
            fault_injected = True
            trace.append("ledger.r4.aftercommit")
            raise aftercommit
        return result

    def observe_request_reread(request_id: Any) -> RequestRecord | None:
        request = installed_get_request(request_id)
        if (
            fault_injected
            and "cancellation.reraised" not in trace
            and request is not None
            and request.state is HostRequestState.QUARANTINED
        ):
            trace.append("ledger.r4.reread_exact")
        return request

    monkeypatch.setattr(transport.ledger, "transition", transition_then_fail_once)
    monkeypatch.setattr(transport.ledger, "get_request", observe_request_reread)
    unlock_observations = _install_unlock_probe(
        monkeypatch,
        harness=harness,
        transport=transport,
        command=command,
        trace=trace,
    )
    message = "cancel across r4 ledger aftercommit"

    caught, journal, channel = await _run_no_ack_cancellation(
        harness=harness,
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        trace=trace,
        message=message,
    )

    assert fault_injected is True
    _assert_original_cancellation_identity(caught, rig.wait_cancellations, message=message)
    assert "ledger.r4.reread_exact" in trace
    assert trace.index("ledger.r4.aftercommit") < trace.index("ledger.r4.reread_exact")
    assert all(str(aftercommit) not in note for note in getattr(caught, "__notes__", ()))
    base._assert_no_ack_quarantine(  # pyright: ignore[reportPrivateUsage]
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        journal=journal,
    )
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.error_json == _canonical_json(
        {
            "code": ErrorCode.INDETERMINATE_OUTCOME.value,
            "details": {
                "reason": "cancel_ack_timeout",
                "request_id": str(command.request_id),
            },
            "message": "mutation cancellation acknowledgement timed out",
        }
    )
    assert channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    _assert_quarantined_before_unlock(trace=trace, observations=unlock_observations)


_CANCEL_BOUNDARIES = (
    "r2_journal_beforecommit",
    "cancel_delivery_insert_beforecommit",
    "cancel_delivery_insert_aftercommit",
    "cancel_inbox_publish_beforecommit",
    "cancel_inbox_publish_aftercommit",
    "cancel_delivery_mark_beforecommit",
    "cancel_delivery_mark_aftercommit",
    "request_cancel_published_beforecommit",
    "request_cancel_published_aftercommit",
    "r4_journal_beforecommit",
    "r4_journal_aftercommit",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", _CANCEL_BOUNDARIES)
async def test_cancel_boundary_one_shot_reuses_candidate_and_quarantines_before_unlock(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    transport = _transport(harness)
    command = _command(harness)
    trace: list[str] = []
    peer = base._peer(harness, trace)  # pyright: ignore[reportPrivateUsage]
    peer.enqueue(StaySilent())
    rig = base._NoAckRig(trace=trace)  # pyright: ignore[reportPrivateUsage]
    base._install_no_ack_rig(  # pyright: ignore[reportPrivateUsage]
        monkeypatch,
        transport=transport,
        peer=peer,
        command=command,
        rig=rig,
    )
    sentinel = RuntimeError(f"{boundary} one-shot sentinel")
    fault_injected = False
    attempted_candidates: list[object] = []

    if boundary == "r2_journal_beforecommit":
        installed_save = transport.journals.save

        def save_r2_beforecommit_once(
            journal: PendingJournal,
            *,
            expected_revisions: JournalRevisions | None,
        ) -> JournalRevisions:
            nonlocal fault_injected
            if journal.original.revision == 2:
                attempted_candidates.append(journal)
                if not fault_injected:
                    fault_injected = True
                    trace.append("fault.r2_journal.beforecommit")
                    raise sentinel
            return installed_save(journal, expected_revisions=expected_revisions)

        monkeypatch.setattr(transport.journals, "save", save_r2_beforecommit_once)
    elif boundary in {
        "cancel_delivery_insert_beforecommit",
        "cancel_delivery_insert_aftercommit",
    }:
        installed_insert = transport.ledger.insert_delivery

        def insert_cancel_aftercommit_once(intent: DeliveryIntent) -> None:
            nonlocal fault_injected
            if intent.delivery_kind == "cancel":
                attempted_candidates.append(intent)
                if boundary.endswith("beforecommit") and not fault_injected:
                    fault_injected = True
                    trace.append(f"fault.cancel_delivery_insert.{boundary.rsplit('_', 1)[-1]}")
                    raise sentinel
            installed_insert(intent)
            if (
                intent.delivery_kind == "cancel"
                and boundary.endswith("aftercommit")
                and not fault_injected
            ):
                fault_injected = True
                trace.append("fault.cancel_delivery_insert.aftercommit")
                raise sentinel

        monkeypatch.setattr(transport.ledger, "insert_delivery", insert_cancel_aftercommit_once)
    elif boundary in {
        "cancel_inbox_publish_beforecommit",
        "cancel_inbox_publish_aftercommit",
    }:
        installed_publish = InboxPublisher.publish_delivery

        def publish_cancel_aftercommit_once(
            publisher: InboxPublisher,
            delivery: PreparedDelivery,
            *,
            runtime_snapshot: RuntimeSnapshot,
        ) -> None:
            nonlocal fault_injected
            if delivery.delivery_kind == "cancel":
                attempted_candidates.append(delivery)
                if boundary.endswith("beforecommit") and not fault_injected:
                    fault_injected = True
                    trace.append("fault.cancel_inbox_publish.beforecommit")
                    raise sentinel
            installed_publish(
                publisher,
                delivery,
                runtime_snapshot=runtime_snapshot,
            )
            if (
                delivery.delivery_kind == "cancel"
                and boundary.endswith("aftercommit")
                and not fault_injected
            ):
                fault_injected = True
                trace.append("fault.cancel_inbox_publish.aftercommit")
                raise sentinel

        monkeypatch.setattr(InboxPublisher, "publish_delivery", publish_cancel_aftercommit_once)
    elif boundary in {
        "cancel_delivery_mark_beforecommit",
        "cancel_delivery_mark_aftercommit",
    }:
        installed_mark = transport.ledger.mark_delivery_published

        def mark_cancel_aftercommit_once(
            delivery_id: UUID,
            *,
            published_at_ms: int,
        ) -> DeliveryRecord:
            nonlocal fault_injected
            if rig.cancel_delivery_id == delivery_id:
                attempted_candidates.append((delivery_id, published_at_ms))
                if boundary.endswith("beforecommit") and not fault_injected:
                    fault_injected = True
                    trace.append("fault.cancel_delivery_mark.beforecommit")
                    raise sentinel
            result = installed_mark(delivery_id, published_at_ms=published_at_ms)
            if (
                rig.cancel_delivery_id == delivery_id
                and boundary.endswith("aftercommit")
                and not fault_injected
            ):
                fault_injected = True
                trace.append("fault.cancel_delivery_mark.aftercommit")
                raise sentinel
            return result

        monkeypatch.setattr(
            transport.ledger, "mark_delivery_published", mark_cancel_aftercommit_once
        )
    elif boundary in {
        "request_cancel_published_beforecommit",
        "request_cancel_published_aftercommit",
    }:
        installed_transition = transport.ledger.transition

        def cancel_transition_aftercommit_once(
            request_id: UUID,
            *,
            expected_states: frozenset[HostRequestState],
            new_state: HostRequestState,
            updated_at_ms: int,
            terminal_at_ms: int | None = None,
            result_json: str | None = None,
            error_json: str | None = None,
            resolution_json: str | None = None,
        ) -> RequestRecord:
            nonlocal fault_injected
            candidate = (
                request_id,
                expected_states,
                new_state,
                updated_at_ms,
                terminal_at_ms,
                result_json,
                error_json,
                resolution_json,
            )
            if new_state is HostRequestState.CANCEL_PUBLISHED:
                attempted_candidates.append(candidate)
                if boundary.endswith("beforecommit") and not fault_injected:
                    fault_injected = True
                    trace.append("fault.request_cancel_published.beforecommit")
                    raise sentinel
            result = installed_transition(
                request_id,
                expected_states=expected_states,
                new_state=new_state,
                updated_at_ms=updated_at_ms,
                terminal_at_ms=terminal_at_ms,
                result_json=result_json,
                error_json=error_json,
                resolution_json=resolution_json,
            )
            if (
                new_state is HostRequestState.CANCEL_PUBLISHED
                and boundary.endswith("aftercommit")
                and not fault_injected
            ):
                fault_injected = True
                trace.append("fault.request_cancel_published.aftercommit")
                raise sentinel
            return result

        monkeypatch.setattr(transport.ledger, "transition", cancel_transition_aftercommit_once)
    else:
        assert boundary in {"r4_journal_beforecommit", "r4_journal_aftercommit"}
        installed_save = transport.journals.save

        def save_r4_aftercommit_once(
            journal: PendingJournal,
            *,
            expected_revisions: JournalRevisions | None,
        ) -> JournalRevisions:
            nonlocal fault_injected
            if journal.original.revision == 4:
                attempted_candidates.append(journal)
                if boundary.endswith("beforecommit") and not fault_injected:
                    fault_injected = True
                    trace.append("fault.r4_journal.beforecommit")
                    raise sentinel
            revisions = installed_save(journal, expected_revisions=expected_revisions)
            if (
                journal.original.revision == 4
                and boundary.endswith("aftercommit")
                and not fault_injected
            ):
                fault_injected = True
                trace.append("fault.r4_journal.aftercommit")
                raise sentinel
            return revisions

        monkeypatch.setattr(transport.journals, "save", save_r4_aftercommit_once)

    def forbidden_idle(_publisher: InboxPublisher) -> None:
        raise AssertionError("no-ACK quarantine must not publish idle")

    def forbidden_delete(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("no-ACK quarantine must retain its journal")

    monkeypatch.setattr(InboxPublisher, "publish_idle", forbidden_idle)
    monkeypatch.setattr(transport.journals, "delete", forbidden_delete)
    unlock_observations = _install_unlock_probe(
        monkeypatch,
        harness=harness,
        transport=transport,
        command=command,
        trace=trace,
    )
    message = f"cancel across {boundary}"

    caught, journal, channel = await _run_no_ack_cancellation(
        harness=harness,
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        trace=trace,
        message=message,
    )

    assert fault_injected is True
    assert attempted_candidates
    assert all(candidate == attempted_candidates[0] for candidate in attempted_candidates)
    _assert_original_cancellation_identity(caught, rig.wait_cancellations, message=message)
    assert all(str(sentinel) not in note for note in getattr(caught, "__notes__", ()))
    base._assert_no_ack_quarantine(  # pyright: ignore[reportPrivateUsage]
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        journal=journal,
    )
    revisions = {candidate.original.revision: candidate for candidate in rig.journals}
    r2 = revisions[2]
    r3 = revisions[3]
    r4 = revisions[4]
    r2_cancel = r2.original.delivery_intents[1]
    r3_cancel = r3.original.delivery_intents[1]
    assert r4.original.delivery_intents[1] == r3_cancel
    assert r2_cancel.delivery_id == r3_cancel.delivery_id == rig.cancel_delivery_id
    assert r2_cancel.intended_at_ms == r2.original.updated_at_ms
    assert r3_cancel.published_at_ms == r3.original.updated_at_ms

    attempt_count = 2 if boundary.endswith("beforecommit") else 1
    if boundary == "r2_journal_beforecommit":
        expected_candidate: object = r2
    elif boundary.startswith("cancel_delivery_insert_"):
        expected_candidate = r2_cancel
    elif boundary.startswith("cancel_inbox_publish_"):
        expected_candidate = rig.cancel_deliveries[0]
    elif boundary.startswith("cancel_delivery_mark_"):
        expected_candidate = (rig.cancel_delivery_id, r3.original.updated_at_ms)
    elif boundary.startswith("request_cancel_published_"):
        expected_candidate = (
            command.request_id,
            frozenset({HostRequestState.PUBLISHED}),
            HostRequestState.CANCEL_PUBLISHED,
            r3.original.updated_at_ms,
            None,
            None,
            None,
            None,
        )
    else:
        expected_candidate = r4
    assert attempted_candidates == [expected_candidate] * attempt_count

    assert channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    _assert_quarantined_before_unlock(trace=trace, observations=unlock_observations)


@pytest.mark.asyncio
async def test_continuous_r2_write_failure_stops_after_two_attempts_before_unlock(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _command(harness)
    trace: list[str] = []
    peer = base._peer(harness, trace)  # pyright: ignore[reportPrivateUsage]
    peer.enqueue(StaySilent())
    rig = base._NoAckRig(trace=trace)  # pyright: ignore[reportPrivateUsage]
    base._install_no_ack_rig(  # pyright: ignore[reportPrivateUsage]
        monkeypatch,
        transport=transport,
        peer=peer,
        command=command,
        rig=rig,
    )
    installed_save = transport.journals.save
    sentinel = RuntimeError("continuous r2 save sentinel")
    attempts: list[PendingJournal] = []
    workers: list[asyncio.Task[None]] = []

    def always_fail_r2(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        if journal.original.revision != 2:
            return installed_save(journal, expected_revisions=expected_revisions)
        attempts.append(journal)
        trace.append(f"fault.r2.continuous.{len(attempts)}")
        current = asyncio.current_task()
        assert current is not None
        worker = cast(asyncio.Task[None], current)
        if not workers:
            workers.append(worker)
        else:
            assert workers == [worker]
        raise sentinel

    def forbidden_idle(_publisher: InboxPublisher) -> None:
        raise AssertionError("failed recovery must not publish idle")

    monkeypatch.setattr(transport.journals, "save", always_fail_r2)
    monkeypatch.setattr(InboxPublisher, "publish_idle", forbidden_idle)
    unlock_observations = _install_unlock_probe(
        monkeypatch,
        harness=harness,
        transport=transport,
        command=command,
        trace=trace,
    )
    installed_exit = RootLock.__aexit__

    async def require_worker_terminated_before_unlock(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is harness.lock:
            assert len(workers) == 1
            assert workers[0].done()
            trace.append("worker.terminated")
        return await installed_exit(lock, exc_type, exc, traceback)

    monkeypatch.setattr(RootLock, "__aexit__", require_worker_terminated_before_unlock)
    message = "cancel with exhausted r2 retry budget"

    caught, journal, channel = await _run_no_ack_cancellation(
        harness=harness,
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        trace=trace,
        message=message,
    )

    _assert_original_cancellation_identity(caught, rig.wait_cancellations, message=message)
    assert len(attempts) == 2
    assert attempts[0] == attempts[1]
    assert journal.original.revision == 1
    assert journal.original.state is PendingPhase.PUBLISHED
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.PUBLISHED
    assert rig.cancel_deliveries == []
    assert [candidate.original.revision for candidate in rig.journals] == [0, 1]
    notes = tuple(getattr(caught, "__notes__", ()))
    matching_notes = tuple(note for note in notes if str(sentinel) in note)
    assert len(matching_notes) == 1
    assert "cancellation recovery failure" in matching_notes[0]
    assert channel._mutation_exchange._recovery_task is None  # pyright: ignore[reportPrivateUsage]
    assert channel._mutation_exchange._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]
    assert channel._poisoned is True  # pyright: ignore[reportPrivateUsage]
    assert channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    assert unlock_observations == [(PendingPhase.PUBLISHED, HostRequestState.PUBLISHED)]
    assert trace.index("fault.r2.continuous.2") < trace.index("cancellation.reraised")
    assert trace.index("cancellation.reraised") < trace.index("worker.terminated")
    assert trace.index("worker.terminated") < trace.index("owner.unlock")


@pytest.mark.asyncio
async def test_foreign_inbox_after_r2_is_never_overwritten_and_keeps_pending_barrier(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _command(harness)
    trace: list[str] = []
    peer = base._peer(harness, trace)  # pyright: ignore[reportPrivateUsage]
    peer.enqueue(StaySilent())
    rig = base._NoAckRig(trace=trace)  # pyright: ignore[reportPrivateUsage]
    base._install_no_ack_rig(  # pyright: ignore[reportPrivateUsage]
        monkeypatch,
        transport=transport,
        peer=peer,
        command=command,
        rig=rig,
    )
    installed_insert = transport.ledger.insert_delivery
    installed_publish = InboxPublisher.publish_delivery
    foreign_bytes = b"unexplained foreign inbox bytes\n"
    cancel_publish_calls: list[PreparedDelivery] = []
    unexpected_publish = AssertionError("cancel publisher touched an unexplained foreign inbox")

    def insert_cancel_then_replace_inbox(intent: DeliveryIntent) -> None:
        installed_insert(intent)
        if intent.delivery_kind == "cancel":
            transport.paths.inbox.write_bytes(foreign_bytes)
            trace.append("inbox.foreign_after_r2_insert")

    def reject_cancel_publish_over_foreign(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        if delivery.delivery_kind == "cancel":
            cancel_publish_calls.append(delivery)
            trace.append("fault.inbox.cancel_over_foreign")
            raise unexpected_publish
        installed_publish(
            publisher,
            delivery,
            runtime_snapshot=runtime_snapshot,
        )

    monkeypatch.setattr(transport.ledger, "insert_delivery", insert_cancel_then_replace_inbox)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", reject_cancel_publish_over_foreign)
    unlock_observations = _install_unlock_probe(
        monkeypatch,
        harness=harness,
        transport=transport,
        command=command,
        trace=trace,
    )
    message = "cancel after unexplained inbox replaced the original delivery"

    caught, journal, channel = await _run_no_ack_cancellation(
        harness=harness,
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        trace=trace,
        message=message,
    )

    _assert_original_cancellation_identity(caught, rig.wait_cancellations, message=message)
    assert transport.paths.inbox.read_bytes() == foreign_bytes
    assert journal.original.revision == 3
    assert journal.original.state is PendingPhase.QUARANTINED
    assert len(journal.original.delivery_intents) == 2
    cancel_intent = journal.original.delivery_intents[1]
    assert cancel_intent.delivery_kind == "cancel"
    assert cancel_intent.published_at_ms is None
    cancel_delivery = transport.ledger.get_delivery(cancel_intent.delivery_id)
    assert cancel_delivery is not None
    assert cancel_delivery.published_at_ms is None
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.QUARANTINED
    assert request.error_json is not None
    assert json.loads(request.error_json)["details"]["reason"] == ("cancel_intent_inbox_conflict")
    notes = tuple(getattr(caught, "__notes__", ()))
    recovery_notes = tuple(note for note in notes if "cancellation recovery failure" in note)
    assert recovery_notes == ()
    assert channel._mutation_exchange._recovery_task is None  # pyright: ignore[reportPrivateUsage]
    assert channel._mutation_exchange._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]
    assert channel._poisoned is True  # pyright: ignore[reportPrivateUsage]
    assert channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    assert unlock_observations == [(PendingPhase.QUARANTINED, HostRequestState.QUARANTINED)]
    assert trace.index("inbox.foreign_after_r2_insert") < trace.index("cancellation.reraised")
    assert trace.index("cancellation.reraised") < trace.index("owner.unlock")

    blocked_session = transport.session()
    blocked_channel = cast(
        _FileBridgeChannel,
        await _enter_session_bounded(
            blocked_session,
            label="pending-barrier follow-up session",
        ),
    )
    try:
        with pytest.raises(BridgeError) as blocked:
            await _bounded(
                blocked_channel.exchange(command),
                label="pending-barrier follow-up mutation",
            )
        assert blocked.value.code is ErrorCode.MUTATION_QUARANTINED
        assert blocked.value.details == {
            "request_id": str(command.request_id),
            "state": PendingPhase.QUARANTINED.value,
            "required_release_id": command.runtime_snapshot.release_id,
        }
    finally:
        await _exit_session_bounded(
            blocked_session,
            blocked_channel,
            label="pending-barrier follow-up session",
        )
    assert cancel_publish_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observation",
    ["journal_load_after_r3", "request_get_after_r4"],
)
async def test_one_shot_observation_failure_converges_without_secondary_note(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
    observation: str,
) -> None:
    transport = _transport(harness)
    command = _command(harness)
    trace: list[str] = []
    peer = base._peer(harness, trace)  # pyright: ignore[reportPrivateUsage]
    peer.enqueue(StaySilent())
    rig = base._NoAckRig(trace=trace)  # pyright: ignore[reportPrivateUsage]
    base._install_no_ack_rig(  # pyright: ignore[reportPrivateUsage]
        monkeypatch,
        transport=transport,
        peer=peer,
        command=command,
        rig=rig,
    )
    sentinel = RuntimeError(f"{observation} one-shot sentinel")
    armed = False
    injected = False
    observed_attempts = 0

    if observation == "journal_load_after_r3":
        installed_save = transport.journals.save
        installed_load = transport.journals.load

        def arm_after_r3(
            journal: PendingJournal,
            *,
            expected_revisions: JournalRevisions | None,
        ) -> JournalRevisions:
            nonlocal armed
            revisions = installed_save(journal, expected_revisions=expected_revisions)
            if journal.original.revision == 3:
                armed = True
            return revisions

        def fail_one_r3_load() -> Any:
            nonlocal injected, observed_attempts
            if armed and "cancellation.reraised" not in trace:
                observed_attempts += 1
                if not injected:
                    injected = True
                    trace.append("fault.journal_load_after_r3")
                    raise sentinel
                trace.append("observation.journal_load_after_r3.recovered")
            return installed_load()

        monkeypatch.setattr(transport.journals, "save", arm_after_r3)
        monkeypatch.setattr(transport.journals, "load", fail_one_r3_load)
    else:
        assert observation == "request_get_after_r4"
        installed_transition = transport.ledger.transition
        installed_get_request = transport.ledger.get_request

        def arm_after_r4_transition(
            request_id: UUID,
            *,
            expected_states: frozenset[HostRequestState],
            new_state: HostRequestState,
            updated_at_ms: int,
            terminal_at_ms: int | None = None,
            result_json: str | None = None,
            error_json: str | None = None,
            resolution_json: str | None = None,
        ) -> RequestRecord:
            nonlocal armed
            request = installed_transition(
                request_id,
                expected_states=expected_states,
                new_state=new_state,
                updated_at_ms=updated_at_ms,
                terminal_at_ms=terminal_at_ms,
                result_json=result_json,
                error_json=error_json,
                resolution_json=resolution_json,
            )
            if new_state is HostRequestState.QUARANTINED:
                armed = True
            return request

        def fail_one_r4_request_get(request_id: UUID) -> RequestRecord | None:
            nonlocal injected, observed_attempts
            if armed and "cancellation.reraised" not in trace:
                observed_attempts += 1
                if not injected:
                    injected = True
                    trace.append("fault.request_get_after_r4")
                    raise sentinel
                trace.append("observation.request_get_after_r4.recovered")
            return installed_get_request(request_id)

        monkeypatch.setattr(transport.ledger, "transition", arm_after_r4_transition)
        monkeypatch.setattr(transport.ledger, "get_request", fail_one_r4_request_get)

    def forbidden_idle(_publisher: InboxPublisher) -> None:
        raise AssertionError("observation recovery must not publish idle")

    monkeypatch.setattr(InboxPublisher, "publish_idle", forbidden_idle)
    unlock_observations = _install_unlock_probe(
        monkeypatch,
        harness=harness,
        transport=transport,
        command=command,
        trace=trace,
    )
    message = f"cancel across {observation}"

    caught, journal, channel = await _run_no_ack_cancellation(
        harness=harness,
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        trace=trace,
        message=message,
    )

    assert armed is True
    assert injected is True
    assert observed_attempts >= 2
    _assert_original_cancellation_identity(caught, rig.wait_cancellations, message=message)
    assert all(str(sentinel) not in note for note in getattr(caught, "__notes__", ()))
    base._assert_no_ack_quarantine(  # pyright: ignore[reportPrivateUsage]
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        journal=journal,
    )
    assert channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    _assert_quarantined_before_unlock(trace=trace, observations=unlock_observations)


class _WaiterBaseException(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("fault_kind", ["base_exception", "cancelled_error"])
async def test_dual_waiter_base_exception_is_quarantined_before_original_cancel_reraise(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
    fault_kind: str,
) -> None:
    transport = _transport(harness)
    command = _command(harness)
    trace: list[str] = []
    peer = base._peer(harness, trace)  # pyright: ignore[reportPrivateUsage]
    peer.enqueue(StaySilent())
    rig = base._NoAckRig(trace=trace)  # pyright: ignore[reportPrivateUsage]
    base._install_no_ack_rig(  # pyright: ignore[reportPrivateUsage]
        monkeypatch,
        transport=transport,
        peer=peer,
        command=command,
        rig=rig,
    )
    installed_wait = ResponseWaiter.wait
    waiter_fault: BaseException = (
        _WaiterBaseException("dual waiter custom BaseException")
        if fault_kind == "base_exception"
        else asyncio.CancelledError("dual waiter custom CancelledError")
    )

    async def fail_dual_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        expectation = waiter._expectation  # pyright: ignore[reportPrivateUsage]
        allowed = expectation.allowed_deliveries
        if len(allowed) == 1:
            return await installed_wait(waiter, timeout_seconds)
        assert len(allowed) == 2
        assert rig.request_delivery_id is not None
        assert allowed[0].delivery_id == rig.request_delivery_id
        assert allowed[1].delivery_kind == "cancel"
        assert allowed[1].delivery_id == rig.cancel_delivery_id
        rig.cancel_expectation = expectation
        rig.cancel_wait_timeouts.append(timeout_seconds)
        rig.cancel_wait_started.set()
        trace.append("wait.cancel.custom_fault")
        raise waiter_fault

    def forbidden_idle(_publisher: InboxPublisher) -> None:
        raise AssertionError("dual waiter failure must not publish idle")

    def forbidden_delete(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("dual waiter failure must retain its journal")

    monkeypatch.setattr(ResponseWaiter, "wait", fail_dual_wait)
    monkeypatch.setattr(InboxPublisher, "publish_idle", forbidden_idle)
    monkeypatch.setattr(transport.journals, "delete", forbidden_delete)
    unlock_observations = _install_unlock_probe(
        monkeypatch,
        harness=harness,
        transport=transport,
        command=command,
        trace=trace,
    )
    message = f"cancel across dual waiter {fault_kind}"

    caught, journal, channel = await _run_no_ack_cancellation(
        harness=harness,
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        trace=trace,
        message=message,
    )

    _assert_original_cancellation_identity(caught, rig.wait_cancellations, message=message)
    assert all(str(waiter_fault) not in note for note in getattr(caught, "__notes__", ()))
    assert journal.original.state is PendingPhase.QUARANTINED
    assert journal.original.response_artifact is None
    assert journal.original.settlement is None
    cancel_intent = journal.original.delivery_intents[1]
    assert cancel_intent.delivery_id == rig.cancel_delivery_id
    assert cancel_intent.published_at_ms is not None
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.QUARANTINED
    assert request.error_json == _canonical_json(
        {
            "code": ErrorCode.INDETERMINATE_OUTCOME.value,
            "details": {
                "reason": "cancel_wait_failed",
                "request_id": str(command.request_id),
            },
            "message": "mutation cancellation response wait failed",
        }
    )
    delivery = transport.ledger.get_delivery(cancel_intent.delivery_id)
    assert delivery is not None
    assert delivery.published_at_ms == cancel_intent.published_at_ms
    assert delivery.response_artifact is None
    inbox_bytes = transport.paths.inbox.read_bytes()
    assert hashlib.sha256(inbox_bytes).hexdigest() == cancel_intent.rendered_inbox_sha256
    assert len(inbox_bytes) == cancel_intent.rendered_inbox_size_bytes
    assert rig.cancel_wait_timeouts == [base.CANCEL_ACK_TIMEOUT_SECONDS]
    assert not transport.paths.response_path(command.request_id).exists()
    assert channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    _assert_quarantined_before_unlock(trace=trace, observations=unlock_observations)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "artifact_fault_mode",
    ["none", "record_response_aftercommit", "delivery_observation"],
    ids=["ordinary", "record-response-aftercommit", "delivery-observation"],
)
async def test_original_delivery_artifact_during_cancel_wait_resumes_ordinary_completion(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
    artifact_fault_mode: str,
) -> None:
    transport = _transport(harness)
    command = _command(harness)
    trace: list[str] = []
    peer = base._peer(harness, trace)  # pyright: ignore[reportPrivateUsage]
    original_wait_started = asyncio.Event()
    cancel_wait_started = asyncio.Event()
    wait_cancellations: list[asyncio.CancelledError] = []
    original_wait = ResponseWaiter.wait
    original_save = transport.journals.save
    original_delete = transport.journals.delete
    original_record_response = transport.ledger.record_response
    original_get_delivery = transport.ledger.get_delivery
    original_transition = transport.ledger.transition
    original_publish_idle = InboxPublisher.publish_idle
    record_aftercommit = RuntimeError("artifact record_response aftercommit sentinel")
    delivery_observation_error = RuntimeError("artifact delivery observation one-shot sentinel")
    record_fault_injected = False
    delivery_observation_injected = False
    delivery_observation_attempts = 0
    record_completed = False
    record_attempts: list[ResponseArtifact] = []
    journal_advances: list[tuple[PendingJournal, JournalRevisions | None]] = []
    delete_expectations: list[JournalDeleteExpectation] = []

    async def traced_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        allowed = waiter._expectation.allowed_deliveries  # pyright: ignore[reportPrivateUsage]
        if len(allowed) == 1:
            original_wait_started.set()
            try:
                return await original_wait(waiter, timeout_seconds)
            except asyncio.CancelledError as error:
                wait_cancellations.append(error)
                raise
        assert len(allowed) == 2
        cancel_wait_started.set()
        trace.append("wait.cancel.started")
        return await original_wait(waiter, timeout_seconds)

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        revisions = original_save(journal, expected_revisions=expected_revisions)
        journal_advances.append((journal, expected_revisions))
        if journal.original.revision == 4:
            trace.append("journal.r4_saved")
        elif journal.original.revision == 5:
            trace.append("journal.r5_saved")
        return revisions

    def traced_delete(expected: JournalDeleteExpectation) -> None:
        delete_expectations.append(expected)
        original_delete(expected)
        trace.append("journal.deleted")

    def traced_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        nonlocal record_completed, record_fault_injected
        record_attempts.append(artifact)
        result = original_record_response(artifact)
        record_completed = True
        trace.append("ledger.response.recorded")
        if artifact_fault_mode == "record_response_aftercommit" and not record_fault_injected:
            record_fault_injected = True
            trace.append("fault.ledger.response.aftercommit")
            raise record_aftercommit
        return result

    def traced_get_delivery(delivery_id: UUID) -> DeliveryRecord | None:
        nonlocal delivery_observation_attempts, delivery_observation_injected
        if (
            artifact_fault_mode == "delivery_observation"
            and record_completed
            and "cancellation.reraised" not in trace
        ):
            delivery_observation_attempts += 1
            if not delivery_observation_injected:
                delivery_observation_injected = True
                trace.append("fault.ledger.response_observation")
                raise delivery_observation_error
        delivery = original_get_delivery(delivery_id)
        if (
            artifact_fault_mode == "delivery_observation"
            and record_completed
            and "cancellation.reraised" not in trace
            and delivery is not None
            and delivery.response_artifact is not None
            and "ledger.response_observation.recovered" not in trace
        ):
            trace.append("ledger.response_observation.recovered")
        if (
            record_fault_injected
            and "cancellation.reraised" not in trace
            and delivery is not None
            and delivery.response_artifact is not None
            and "ledger.response.reread_exact" not in trace
        ):
            trace.append("ledger.response.reread_exact")
        return delivery

    def traced_transition(
        request_id: Any,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        result = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        transition_event = {
            HostRequestState.RESPONSE_ACCEPTED: "ledger.request.response_accepted",
            HostRequestState.IDLE_PUBLISHED: "ledger.request.idle_published",
            HostRequestState.COMPLETED: "ledger.request.completed",
            HostRequestState.QUARANTINED: "ledger.request.quarantined",
        }.get(new_state)
        if transition_event is not None:
            trace.append(transition_event)
        return result

    def traced_publish_idle(publisher: InboxPublisher) -> None:
        original_publish_idle(publisher)
        trace.append("inbox.idle")

    monkeypatch.setattr(ResponseWaiter, "wait", traced_wait)
    monkeypatch.setattr(transport.journals, "save", traced_save)
    monkeypatch.setattr(transport.journals, "delete", traced_delete)
    monkeypatch.setattr(transport.ledger, "record_response", traced_record_response)
    monkeypatch.setattr(transport.ledger, "get_delivery", traced_get_delivery)
    monkeypatch.setattr(transport.ledger, "transition", traced_transition)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_publish_idle)
    unlock_observations = _install_unlock_probe(
        monkeypatch,
        harness=harness,
        transport=transport,
        command=command,
        trace=trace,
    )
    message = "cancel while original response races"
    task: asyncio.Task[ResponseArtifact] | None = None
    caught_error: asyncio.CancelledError | None = None
    channel_ref: _FileBridgeChannel | None = None
    raw_response: bytes | None = None
    expected_result: Any = None
    original_delivery: PreparedDelivery | None = None
    loaded_before_exit: Any = "not observed"
    request_before_exit: RequestRecord | None = None
    cleanup_paths_before_exit: tuple[Path, ...] | None = None
    response_bytes_before_exit: bytes | None = None
    inbox_before_exit: bytes | None = None
    session = transport.session()

    try:
        channel_ref = cast(
            _FileBridgeChannel,
            await _enter_session_bounded(session, label="artifact recovery session"),
        )
        task = asyncio.create_task(channel_ref.exchange(command))
        try:
            await _bounded(
                original_wait_started.wait(),
                label="original waiter before artifact race",
            )
            original_delivery, body, invocation = peer._parse_delivery(  # pyright: ignore[reportPrivateUsage]
                transport.paths.inbox.read_bytes()
            )
            result_model = command.invocation.result_adapter.validate_python(
                {
                    "unit_guid": "UNIT-1",
                    "name": "Recovery cancellation",
                    "speed": None,
                    "altitude": None,
                    "heading": None,
                    "course": None,
                }
            )
            expected_result = command.invocation.result_adapter.dump_python(
                result_model,
                mode="json",
            )
            raw_response = peer._response_bytes(  # pyright: ignore[reportPrivateUsage]
                Respond(result=expected_result),
                original_delivery,
                body,
                invocation,
            )
            assert task.cancel(message) is True
            await _bounded(cancel_wait_started.wait(), label="dual waiter before artifact write")
            transport.paths.response_path(command.request_id).write_bytes(raw_response)
            trace.append("peer.original.response_written")
            with pytest.raises(asyncio.CancelledError) as caught:
                await base._bounded_task_outcome(  # pyright: ignore[reportPrivateUsage]
                    task,
                    label="original artifact cancellation recovery",
                )
            caught_error = caught.value
            trace.append("cancellation.reraised")
            loaded_before_exit = transport.journals.load()
            request_before_exit = transport.ledger.get_request(command.request_id)
            cleanup_paths_before_exit = tuple(
                channel_ref._cleanup_paths  # pyright: ignore[reportPrivateUsage]
            )
            response_path = transport.paths.response_path(command.request_id)
            response_bytes_before_exit = raw_response if response_path.exists() else None
            inbox_before_exit = transport.paths.inbox.read_bytes()
        finally:
            if not task.done():
                await _finish_quietly(task)
    finally:
        if channel_ref is not None:
            await _exit_session_bounded(
                session,
                channel_ref,
                label="artifact recovery session",
            )

    assert caught_error is not None
    _assert_original_cancellation_identity(caught_error, wait_cancellations, message=message)
    terminal_advances = [
        (journal, expected_revisions)
        for journal, expected_revisions in journal_advances
        if journal.original.revision in {4, 5}
    ]
    assert [
        (journal.original.revision, journal.original.state, expected_revisions)
        for journal, expected_revisions in terminal_advances
    ] == [
        (
            4,
            PendingPhase.RESPONSE_ACCEPTED,
            JournalRevisions(original=3, reconcile_attempt=None),
        ),
        (
            5,
            PendingPhase.IDLE_PUBLISHED,
            JournalRevisions(original=4, reconcile_attempt=None),
        ),
    ]
    response_accepted = terminal_advances[0][0]
    idle_published = terminal_advances[1][0]
    assert response_accepted.original.response_artifact is not None
    assert response_accepted.original.settlement is not None
    assert idle_published.original.response_artifact == (
        response_accepted.original.response_artifact
    )
    assert idle_published.original.settlement == response_accepted.original.settlement
    assert idle_published.original.delivery_intents == response_accepted.original.delivery_intents
    assert len(response_accepted.original.delivery_intents) == 2

    artifact = response_accepted.original.response_artifact
    assert record_attempts == [artifact]
    assert original_delivery is not None
    assert artifact.accepted_response.envelope.delivery_id == original_delivery.delivery_id
    assert artifact.accepted_response.delivery_kind == "request"
    assert artifact.accepted_response.cancel_ack is None
    settlement = artifact.accepted_response.settlement
    assert type(settlement) is CompletedSettlement
    assert settlement.result == expected_result

    if artifact_fault_mode == "record_response_aftercommit":
        assert record_fault_injected is True
        assert trace.count("ledger.response.recorded") == 1
        assert "ledger.response.reread_exact" in trace
        assert trace.index("fault.ledger.response.aftercommit") < trace.index(
            "ledger.response.reread_exact"
        )
        assert trace.index("ledger.response.reread_exact") < trace.index(
            "ledger.request.response_accepted"
        )
        assert all(
            str(record_aftercommit) not in note for note in getattr(caught_error, "__notes__", ())
        )
        assert delivery_observation_injected is False
    elif artifact_fault_mode == "delivery_observation":
        assert record_fault_injected is False
        assert delivery_observation_injected is True
        assert delivery_observation_attempts >= 2
        assert trace.index("fault.ledger.response_observation") < trace.index(
            "ledger.response_observation.recovered"
        )
        assert trace.index("ledger.response_observation.recovered") < trace.index(
            "ledger.request.response_accepted"
        )
        assert all(
            str(delivery_observation_error) not in note
            for note in getattr(caught_error, "__notes__", ())
        )
    else:
        assert artifact_fault_mode == "none"
        assert record_fault_injected is False
        assert delivery_observation_injected is False

    delivery = transport.ledger.get_delivery(original_delivery.delivery_id)
    assert delivery is not None
    assert delivery.response_artifact == artifact
    assert delivery.settlement == settlement
    cancel_intent = next(
        intent
        for intent in response_accepted.original.delivery_intents
        if intent.delivery_kind == "cancel"
    )
    cancel_delivery = transport.ledger.get_delivery(cancel_intent.delivery_id)
    assert cancel_delivery is not None
    assert cancel_delivery.published_at_ms == cancel_intent.published_at_ms
    assert cancel_delivery.response_artifact is None
    assert cancel_delivery.settlement is None

    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request == request_before_exit
    assert request.state is HostRequestState.COMPLETED
    assert request.result_json == _canonical_json(expected_result)
    assert request.error_json is None
    assert request.resolution_json is None
    assert request.terminal_at_ms == request.updated_at_ms

    assert loaded_before_exit is None
    assert delete_expectations == [
        JournalDeleteExpectation(
            root_key=harness.paths.root_key,
            required_release_id=command.runtime_snapshot.release_id,
            original_request_id=command.request_id,
            reconcile_attempt_request_id=None,
            revisions=JournalRevisions(original=5, reconcile_attempt=None),
        )
    ]
    assert raw_response is not None
    assert response_bytes_before_exit == raw_response
    assert cleanup_paths_before_exit == (transport.paths.response_path(command.request_id),)
    assert channel_ref is not None
    assert inbox_before_exit == render_idle_lua()
    assert transport.paths.inbox.read_bytes() == render_idle_lua()
    assert not transport.paths.response_path(command.request_id).exists()
    assert channel_ref._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    assert unlock_observations == [(None, HostRequestState.COMPLETED)]

    ordered_events = [
        "journal.r4_saved",
        "ledger.response.recorded",
        "ledger.request.response_accepted",
        "inbox.idle",
        "journal.r5_saved",
        "ledger.request.idle_published",
        "ledger.request.completed",
        "journal.deleted",
        "cancellation.reraised",
        "owner.unlock",
    ]
    assert [trace.index(event) for event in ordered_events] == sorted(
        trace.index(event) for event in ordered_events
    )
