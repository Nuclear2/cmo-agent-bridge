from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field, replace
from pathlib import Path
import sys
from typing import Any, Literal
from uuid import UUID

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.lua_delivery import render_idle_lua
from cmo_agent_bridge.protocol.response_models import ResponseArtifact
from cmo_agent_bridge.state.models import HostRequestState, PendingJournal, PendingPhase
from cmo_agent_bridge.state.pending_journal import (
    JournalDeleteExpectation,
    JournalRevisions,
)
from cmo_agent_bridge.state.request_ledger import DeliveryRecord, RequestRecord
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.recovery import RecoveryManager

sys.path.insert(0, str(Path(__file__).parent))

import test_recovery_settlement as settlement  # noqa: E402


harness = settlement.harness

_Boundary = Literal[
    "r4_journal",
    "record_response",
    "response_accepted_request",
    "idle_publication",
    "r5_journal",
    "idle_request",
    "cancelled_request",
    "journal_delete",
]
_FaultMode = Literal["before", "aftercommit"]

_BOUNDARIES: tuple[_Boundary, ...] = (
    "r4_journal",
    "record_response",
    "response_accepted_request",
    "idle_publication",
    "r5_journal",
    "idle_request",
    "cancelled_request",
    "journal_delete",
)


def _new_objects() -> list[object]:
    return []


@dataclass(slots=True)
class _FaultRig:
    boundary: _Boundary
    mode: _FaultMode
    sentinel: RuntimeError
    injected: bool = False
    calls: list[object] = field(default_factory=_new_objects)


def _new_fault(boundary: _Boundary, mode: _FaultMode) -> _FaultRig:
    return _FaultRig(
        boundary=boundary,
        mode=mode,
        sentinel=RuntimeError(f"one-shot {boundary} {mode} sentinel"),
    )


def _assert_order(trace: list[str], *events: str) -> None:
    indices = [trace.index(event) for event in events]
    assert indices == sorted(indices), trace


def _transition_fingerprint(
    request_id: UUID,
    *,
    expected_states: frozenset[HostRequestState],
    new_state: HostRequestState,
    updated_at_ms: int,
    terminal_at_ms: int | None,
    result_json: str | None,
    error_json: str | None,
    resolution_json: str | None,
) -> tuple[object, ...]:
    return (
        request_id,
        expected_states,
        new_state,
        updated_at_ms,
        terminal_at_ms,
        result_json,
        error_json,
        resolution_json,
    )


def _install_boundary_fault(
    monkeypatch: pytest.MonkeyPatch,
    *,
    transport: Any,
    fault: _FaultRig,
) -> None:
    def should_fail() -> bool:
        if fault.injected:
            return False
        fault.injected = True
        return True

    if fault.boundary in {"r4_journal", "r5_journal"}:
        installed_save = transport.journals.save
        target_revision = 4 if fault.boundary == "r4_journal" else 5
        target_phase = (
            PendingPhase.RESPONSE_ACCEPTED
            if fault.boundary == "r4_journal"
            else PendingPhase.IDLE_PUBLISHED
        )

        def faulty_save(
            journal: PendingJournal,
            *,
            expected_revisions: JournalRevisions | None,
        ) -> JournalRevisions:
            if (
                journal.original.revision != target_revision
                or journal.original.state is not target_phase
            ):
                return installed_save(journal, expected_revisions=expected_revisions)
            fault.calls.append((journal, expected_revisions))
            failing = should_fail()
            if failing and fault.mode == "before":
                raise fault.sentinel
            result = installed_save(journal, expected_revisions=expected_revisions)
            if failing:
                raise fault.sentinel
            return result

        monkeypatch.setattr(transport.journals, "save", faulty_save)
        return

    if fault.boundary == "record_response":
        installed_record = transport.ledger.record_response

        def faulty_record(artifact: ResponseArtifact) -> DeliveryRecord:
            fault.calls.append(artifact)
            failing = should_fail()
            if failing and fault.mode == "before":
                raise fault.sentinel
            result = installed_record(artifact)
            if failing:
                raise fault.sentinel
            return result

        monkeypatch.setattr(transport.ledger, "record_response", faulty_record)
        return

    transition_targets: dict[_Boundary, HostRequestState] = {
        "response_accepted_request": HostRequestState.RESPONSE_ACCEPTED,
        "idle_request": HostRequestState.IDLE_PUBLISHED,
        "cancelled_request": HostRequestState.CANCELLED,
    }
    if fault.boundary in transition_targets:
        installed_transition = transport.ledger.transition
        target_state = transition_targets[fault.boundary]

        def faulty_transition(
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
            if new_state is not target_state:
                return installed_transition(
                    request_id,
                    expected_states=expected_states,
                    new_state=new_state,
                    updated_at_ms=updated_at_ms,
                    terminal_at_ms=terminal_at_ms,
                    result_json=result_json,
                    error_json=error_json,
                    resolution_json=resolution_json,
                )
            fault.calls.append(
                _transition_fingerprint(
                    request_id,
                    expected_states=expected_states,
                    new_state=new_state,
                    updated_at_ms=updated_at_ms,
                    terminal_at_ms=terminal_at_ms,
                    result_json=result_json,
                    error_json=error_json,
                    resolution_json=resolution_json,
                )
            )
            failing = should_fail()
            if failing and fault.mode == "before":
                raise fault.sentinel
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
            if failing:
                raise fault.sentinel
            return result

        monkeypatch.setattr(transport.ledger, "transition", faulty_transition)
        return

    if fault.boundary == "idle_publication":
        installed_publish_idle = InboxPublisher.publish_idle

        def faulty_publish_idle(publisher: InboxPublisher) -> None:
            fault.calls.append(transport.paths.inbox.read_bytes())
            failing = should_fail()
            if failing and fault.mode == "before":
                raise fault.sentinel
            installed_publish_idle(publisher)
            if failing:
                raise fault.sentinel

        monkeypatch.setattr(InboxPublisher, "publish_idle", faulty_publish_idle)
        return

    if fault.boundary == "journal_delete":
        installed_delete = transport.journals.delete

        def faulty_delete(expected: JournalDeleteExpectation) -> None:
            fault.calls.append(expected)
            failing = should_fail()
            if failing and fault.mode == "before":
                raise fault.sentinel
            installed_delete(expected)
            if failing:
                raise fault.sentinel

        monkeypatch.setattr(transport.journals, "delete", faulty_delete)
        return

    raise AssertionError(f"unhandled recovery settlement fault boundary: {fault.boundary}")


def _assert_fixed_candidate(
    fault: _FaultRig,
    *,
    outcome: Any,
    r4: PendingJournal,
    r5: PendingJournal,
) -> None:
    assert fault.injected is True
    expected_attempts = 2 if fault.mode == "before" else 1
    assert len(fault.calls) == expected_attempts
    assert all(candidate == fault.calls[0] for candidate in fault.calls)

    if fault.boundary == "r4_journal":
        assert fault.calls[0] == (
            r4,
            JournalRevisions(original=3, reconcile_attempt=None),
        )
        return
    if fault.boundary == "r5_journal":
        assert fault.calls[0] == (
            r5,
            JournalRevisions(original=4, reconcile_attempt=None),
        )
        return
    if fault.boundary == "record_response":
        assert outcome.rig.artifact is not None
        assert fault.calls[0] == outcome.rig.artifact
        return
    if fault.boundary == "idle_publication":
        observed_predecessor = fault.calls[0]
        assert type(observed_predecessor) is bytes
        assert observed_predecessor != render_idle_lua()
        cancel_intent = next(
            intent for intent in r4.original.delivery_intents if intent.delivery_kind == "cancel"
        )
        assert hashlib.sha256(observed_predecessor).hexdigest() == (
            cancel_intent.rendered_inbox_sha256
        )
        assert len(observed_predecessor) == cancel_intent.rendered_inbox_size_bytes
        return
    if fault.boundary == "journal_delete":
        assert fault.calls[0] == JournalDeleteExpectation(
            root_key=outcome.transport.paths.root_key,
            required_release_id=outcome.command.runtime_snapshot.release_id,
            original_request_id=outcome.command.request_id,
            reconcile_attempt_request_id=None,
            revisions=JournalRevisions(original=5, reconcile_attempt=None),
        )
        return

    target_states: dict[_Boundary, tuple[HostRequestState, HostRequestState]] = {
        "response_accepted_request": (
            HostRequestState.CANCEL_PUBLISHED,
            HostRequestState.RESPONSE_ACCEPTED,
        ),
        "idle_request": (
            HostRequestState.RESPONSE_ACCEPTED,
            HostRequestState.IDLE_PUBLISHED,
        ),
        "cancelled_request": (
            HostRequestState.IDLE_PUBLISHED,
            HostRequestState.CANCELLED,
        ),
    }
    predecessor, target = target_states[fault.boundary]
    committed = next(record for record in outcome.rig.transitions if record.state is target)
    fingerprint = fault.calls[0]
    assert fingerprint == (
        outcome.command.request_id,
        frozenset({predecessor}),
        target,
        committed.updated_at_ms,
        committed.terminal_at_ms,
        committed.result_json,
        committed.error_json,
        committed.resolution_json,
    )


def _assert_cancelled_terminal(outcome: Any) -> tuple[PendingJournal, PendingJournal]:
    r4, r5 = settlement._assert_common_artifact_first(outcome)  # pyright: ignore[reportPrivateUsage]
    rig = outcome.rig
    response_path = outcome.transport.paths.response_path(outcome.command.request_id)

    assert r5.original.state is PendingPhase.IDLE_PUBLISHED
    assert outcome.final_journal is None
    assert outcome.final_request.state is HostRequestState.CANCELLED
    assert outcome.final_request.updated_at_ms == outcome.final_request.terminal_at_ms
    assert outcome.final_request.result_json is None
    assert outcome.final_request.error_json is None
    assert outcome.final_request.resolution_json is None
    assert rig.idle_publications == 1
    assert outcome.transport.paths.inbox.read_bytes() == render_idle_lua()
    assert rig.delete_expectations == [
        JournalDeleteExpectation(
            root_key=outcome.transport.paths.root_key,
            required_release_id=outcome.command.runtime_snapshot.release_id,
            original_request_id=outcome.command.request_id,
            reconcile_attempt_request_id=None,
            revisions=JournalRevisions(original=5, reconcile_attempt=None),
        )
    ]
    assert outcome.queued_before_exit == (response_path,)
    assert outcome.response_existed_before_exit is True
    assert outcome.response_exists_after_exit is False
    assert rig.channel is not None
    assert rig.channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    assert rig.unlock_observations == [(None, HostRequestState.CANCELLED, (response_path,), True)]
    assert rig.original_cancellations == [outcome.caught]
    assert rig.original_cancellations[0] is outcome.caught
    assert outcome.caught.args == (settlement._CANCEL_MESSAGE,)  # pyright: ignore[reportPrivateUsage]
    assert outcome.caught.__cause__ is None
    assert outcome.caught.__context__ is None
    _assert_order(
        rig.trace,
        "journal.r4.response_accepted",
        "ledger.response.cancel",
        "ledger.request.response_accepted",
        "inbox.idle",
        "journal.r5.idle_published",
        "ledger.request.idle_published",
        "ledger.request.cancelled",
        "journal.deleted",
        "cancellation.reraised",
        "owner.unlock",
        "response.cleanup",
    )
    return r4, r5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("boundary", "mode"),
    [
        pytest.param(boundary, mode, id=f"{boundary}-{mode}")
        for boundary in _BOUNDARIES
        for mode in ("before", "aftercommit")
    ],
)
async def test_cancelled_ack_converges_each_one_shot_settlement_boundary(
    harness: Any,
    monkeypatch: pytest.MonkeyPatch,
    boundary: _Boundary,
    mode: _FaultMode,
) -> None:
    fault = _new_fault(boundary, mode)
    original_install = settlement._install_settlement_rig  # pyright: ignore[reportPrivateUsage]

    def install_with_fault(
        installed_monkeypatch: pytest.MonkeyPatch,
        *,
        harness: Any,
        transport: Any,
        command: Any,
        rig: Any,
    ) -> None:
        original_install(
            installed_monkeypatch,
            harness=harness,
            transport=transport,
            command=command,
            rig=rig,
        )
        _install_boundary_fault(
            installed_monkeypatch,
            transport=transport,
            fault=fault,
        )

    monkeypatch.setattr(settlement, "_install_settlement_rig", install_with_fault)
    outcome = await settlement._exercise_settlement(  # pyright: ignore[reportPrivateUsage]
        harness=harness,
        monkeypatch=monkeypatch,
        case="cancelled",
    )
    r4, r5 = _assert_cancelled_terminal(outcome)
    _assert_fixed_candidate(fault, outcome=outcome, r4=r4, r5=r5)
    assert str(fault.sentinel) not in "\n".join(getattr(outcome.caught, "__notes__", ()))


@pytest.mark.asyncio
async def test_cancelled_ack_recovers_one_shot_response_delivery_observation_failure(
    harness: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = RuntimeError("one-shot response delivery observation sentinel")
    observation_injected = False
    record_committed = False
    original_install = settlement._install_settlement_rig  # pyright: ignore[reportPrivateUsage]

    def install_with_observation_fault(
        installed_monkeypatch: pytest.MonkeyPatch,
        *,
        harness: Any,
        transport: Any,
        command: Any,
        rig: Any,
    ) -> None:
        nonlocal observation_injected, record_committed
        original_install(
            installed_monkeypatch,
            harness=harness,
            transport=transport,
            command=command,
            rig=rig,
        )
        installed_record = transport.ledger.record_response
        installed_get_delivery = transport.ledger.get_delivery

        def traced_record(artifact: ResponseArtifact) -> DeliveryRecord:
            nonlocal record_committed
            result = installed_record(artifact)
            record_committed = True
            return result

        def faulty_observation(delivery_id: UUID) -> DeliveryRecord | None:
            nonlocal observation_injected
            if record_committed and not observation_injected:
                observation_injected = True
                raise sentinel
            return installed_get_delivery(delivery_id)

        installed_monkeypatch.setattr(transport.ledger, "record_response", traced_record)
        installed_monkeypatch.setattr(transport.ledger, "get_delivery", faulty_observation)

    monkeypatch.setattr(
        settlement,
        "_install_settlement_rig",
        install_with_observation_fault,
    )
    outcome = await settlement._exercise_settlement(  # pyright: ignore[reportPrivateUsage]
        harness=harness,
        monkeypatch=monkeypatch,
        case="cancelled",
    )
    _assert_cancelled_terminal(outcome)
    assert observation_injected is True
    assert outcome.rig.recorded_artifacts == [outcome.rig.artifact]
    assert str(sentinel) not in "\n".join(getattr(outcome.caught, "__notes__", ()))


@pytest.mark.asyncio
async def test_lost_r3_journal_after_recovery_failure_stays_poisoned_and_blocks_reuse(
    harness: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline_tasks = frozenset(asyncio.all_tasks())
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    transport = settlement.base._transport(harness)  # pyright: ignore[reportPrivateUsage]
    command = settlement.base._command(harness)  # pyright: ignore[reportPrivateUsage]
    rig = settlement._SettlementRig(case="cancelled")  # pyright: ignore[reportPrivateUsage]
    settlement._install_settlement_rig(  # pyright: ignore[reportPrivateUsage]
        monkeypatch,
        harness=harness,
        transport=transport,
        command=command,
        rig=rig,
    )

    installed_advance = RecoveryManager._save_journal_advance  # pyright: ignore[reportPrivateUsage]
    worker_failure = RuntimeError("pending journal disappeared after durable r3")
    lost_journals: list[PendingJournal] = []

    def lose_journal_after_r3(
        manager: RecoveryManager,
        predecessor: PendingJournal,
        target: PendingJournal,
        *,
        label: str,
    ) -> None:
        installed_advance(manager, predecessor, target, label=label)
        if target.original.revision != 3:
            return
        assert target.original.state is PendingPhase.CANCEL_PUBLISHED
        assert not lost_journals
        lost_journals.append(target)
        transport.paths.pending_file.unlink()
        assert not transport.paths.pending_file.exists()
        raise worker_failure

    monkeypatch.setattr(
        RecoveryManager,
        "_save_journal_advance",
        lose_journal_after_r3,
    )

    session = transport.session()
    channel: Any = None
    exchange_task: asyncio.Task[ResponseArtifact] | None = None
    session_entered = False
    cancellation: asyncio.CancelledError | None = None
    message = "cancel before recovery loses its r3 journal"

    try:
        channel = await settlement._bounded(  # pyright: ignore[reportPrivateUsage]
            session.__aenter__(),
            label="lost-r3 session enter",
        )
        session_entered = True
        rig.channel = channel
        exchange_task = asyncio.create_task(channel.exchange(command))
        await settlement.base._require_event_before_task(  # pyright: ignore[reportPrivateUsage]
            rig.original_wait_started,
            exchange_task,
            "exchange ended before the original waiter started",
        )
        assert exchange_task.cancel(message) is True
        with pytest.raises(asyncio.CancelledError) as caught:
            await settlement.base._bounded_task_outcome(  # pyright: ignore[reportPrivateUsage]
                exchange_task,
                label="lost-r3 cancellation recovery",
            )
        cancellation = caught.value

        request = transport.ledger.get_request(command.request_id)
        assert request is not None
        assert transport.journals.load() is None
        mutation_poisoned = (
            channel._mutation_exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]
        )
        channel_poisoned = channel._poisoned  # pyright: ignore[reportPrivateUsage]
        cleanup_paths = tuple(
            channel._cleanup_paths  # pyright: ignore[reportPrivateUsage]
        )

        second_command = replace(
            command,
            request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeea099"),
        )
        second_publications: list[UUID] = []

        def forbid_second_publication(
            _publisher: InboxPublisher,
            delivery: Any,
            *,
            runtime_snapshot: Any,
        ) -> None:
            assert runtime_snapshot == second_command.runtime_snapshot
            second_publications.append(delivery.request_id)
            raise AssertionError("poisoned channel attempted a second mutation publication")

        monkeypatch.setattr(
            InboxPublisher,
            "publish_delivery",
            forbid_second_publication,
        )
        with pytest.raises(BridgeError) as blocked:
            await settlement._bounded(  # pyright: ignore[reportPrivateUsage]
                channel.exchange(second_command),
                label="lost-r3 same-session reuse",
            )

        assert cancellation.args == (message,)
        assert rig.original_cancellations == [cancellation]
        assert rig.original_cancellations[0] is cancellation
        assert cancellation.__cause__ is None
        assert cancellation.__context__ is None
        recovery_notes = tuple(
            note
            for note in getattr(cancellation, "__notes__", ())
            if note.startswith("cancellation recovery failure:")
        )
        assert len(recovery_notes) == 1
        assert "RuntimeError" in recovery_notes[0]
        assert str(worker_failure) in recovery_notes[0]

        assert len(lost_journals) == 1
        lost_r3 = lost_journals[0]
        assert lost_r3.original.revision == 3
        assert lost_r3.original.state is PendingPhase.CANCEL_PUBLISHED
        cancel_intent = next(
            intent
            for intent in lost_r3.original.delivery_intents
            if intent.delivery_kind == "cancel"
        )
        assert cancel_intent.published_at_ms is not None
        assert request.state is HostRequestState.PUBLISHED
        assert request.terminal_at_ms is None
        assert request.result_json is None
        assert request.error_json is None
        assert request.resolution_json is None
        assert mutation_poisoned is True
        assert channel_poisoned is True
        assert cleanup_paths == ()
        assert second_publications == []
        assert blocked.value.code is ErrorCode.STATE_CONFLICT
        assert transport.ledger.get_request(second_command.request_id) is None
        assert transport.journals.load() is None
    finally:
        rig.allow_response.set()
        if exchange_task is not None and not exchange_task.done():
            await settlement._quietly_finish(  # pyright: ignore[reportPrivateUsage]
                exchange_task
            )
        if session_entered:
            assert (
                await settlement._bounded(  # pyright: ignore[reportPrivateUsage]
                    session.__aexit__(None, None, None),
                    label="lost-r3 session exit",
                )
                is False
            )
        await settlement._bounded(  # pyright: ignore[reportPrivateUsage]
            asyncio.sleep(0),
            label="lost-r3 task audit checkpoint",
        )
        current = asyncio.current_task()
        leaked = tuple(
            task
            for task in asyncio.all_tasks()
            if task not in baseline_tasks and task is not current and not task.done()
        )
        assert leaked == ()
