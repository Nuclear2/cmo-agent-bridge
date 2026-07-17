from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable
from dataclasses import dataclass, field
from pathlib import Path
import sys
import time
from types import TracebackType
from typing import Any, Literal, TypeVar, cast
from uuid import UUID

import pytest
from pydantic import JsonValue

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.lua_delivery import render_idle_lua
from cmo_agent_bridge.protocol.models import (
    AllowedDelivery,
    CancelAckResult,
    ExchangeCommand,
    ResponseExpectation,
)
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CancelledSettlement,
    CompletedSettlement,
    MutationNotStartedEvidence,
    RejectedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
    ResponseError,
)
from cmo_agent_bridge.state.models import HostRequestState, PendingJournal, PendingPhase
from cmo_agent_bridge.state.pending_journal import (
    JournalDeleteExpectation,
    JournalRevisions,
)
from cmo_agent_bridge.state.request_ledger import DeliveryRecord, RequestRecord
import cmo_agent_bridge.transports.file_bridge.cleanup as cleanup_module
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.response_waiter import ResponseWaiter
from cmo_agent_bridge.transports.file_bridge.transport import (
    FileBridgeTransport,
    _FileBridgeChannel,  # pyright: ignore[reportPrivateUsage]
)

sys.path.insert(0, str(Path(__file__).parent))

import test_recovery as base  # noqa: E402


harness = base.harness

_T = TypeVar("_T")
_TEST_TIMEOUT_SECONDS = 2
_CANCEL_MESSAGE = "settlement test published mutation cancellation"
_SettlementCase = Literal[
    "cancelled",
    "completed",
    "error",
    "request_rejected",
    "request_completed_invalid",
    "cancel_completed_invalid",
]
_UnlockObservation = tuple[
    PendingPhase | None,
    HostRequestState | None,
    tuple[Path, ...],
    bool,
]


def _new_strings() -> list[str]:
    return []


def _new_cancellations() -> list[asyncio.CancelledError]:
    return []


def _new_journals() -> list[PendingJournal]:
    return []


def _new_requests() -> list[RequestRecord]:
    return []


def _new_artifacts() -> list[ResponseArtifact]:
    return []


def _new_delete_expectations() -> list[JournalDeleteExpectation]:
    return []


def _new_unlock_observations() -> list[_UnlockObservation]:
    return []


async def _bounded(awaitable: Awaitable[_T], *, label: str) -> _T:
    try:
        async with asyncio.timeout(_TEST_TIMEOUT_SECONDS):
            return await awaitable
    except TimeoutError as error:
        raise AssertionError(f"{label} exceeded the test deadline") from error


async def _quietly_finish(task: asyncio.Task[Any]) -> None:
    if not task.done():
        task.cancel("recovery settlement test cleanup")
    try:
        await _bounded(asyncio.shield(task), label="recovery settlement task cleanup")
    except BaseException:
        pass
    assert task.done()


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _assert_order(trace: list[str], *events: str) -> None:
    indices = [trace.index(event) for event in events]
    assert indices == sorted(indices), trace


def _normalized_recovery_result(command: ExchangeCommand) -> JsonValue:
    recovery_adapter = command.invocation.recovery_adapter
    assert recovery_adapter is not None
    raw = {
        "course": None,
        "heading": None,
        "altitude": None,
        "speed": None,
        "name": "Recovery cancellation",
        "unit_guid": "UNIT-1",
    }
    validated = recovery_adapter.validate_python(raw)
    return cast(JsonValue, recovery_adapter.dump_python(validated, mode="json"))


def _response_artifact(
    *,
    case: _SettlementCase,
    command: ExchangeCommand,
    expectation: ResponseExpectation,
    request_delivery_id: UUID,
    cancel_delivery_id: UUID,
    response_path: Path,
) -> tuple[ResponseArtifact, JsonValue | None]:
    normalized_result: JsonValue | None = None
    acknowledgement: CancelAckResult | None = None
    error: ResponseError | None = None
    delivery_kind: Literal["request", "cancel"]
    response_delivery_id: UUID
    if case == "cancelled":
        delivery_kind = "cancel"
        response_delivery_id = cancel_delivery_id
        acknowledgement = CancelAckResult(
            request_id=command.request_id,
            request_hash=expectation.request_hash,
            original_delivery_id=expectation.allowed_deliveries[0].delivery_id,
            status="cancelled",
            result=None,
        )
        settlement = CancelledSettlement(state="cancelled")
        envelope_result: JsonValue = cast(
            JsonValue,
            acknowledgement.model_dump(mode="json"),
        )
        ok = True
    elif case == "completed":
        delivery_kind = "cancel"
        response_delivery_id = cancel_delivery_id
        normalized_result = _normalized_recovery_result(command)
        acknowledgement = CancelAckResult(
            request_id=command.request_id,
            request_hash=expectation.request_hash,
            original_delivery_id=expectation.allowed_deliveries[0].delivery_id,
            status="completed",
            result=normalized_result,
        )
        settlement = CompletedSettlement(state="completed", result=normalized_result)
        envelope_result = cast(JsonValue, acknowledgement.model_dump(mode="json"))
        ok = True
    elif case == "error":
        delivery_kind = "cancel"
        response_delivery_id = cancel_delivery_id
        error = ResponseError(
            code=ErrorCode.INDETERMINATE_OUTCOME,
            message="the mutation was already in progress",
            details={
                "ledger_state": "in_progress",
                "request_id": str(command.request_id),
            },
        )
        settlement = None
        envelope_result = None
        ok = False
    elif case == "request_rejected":
        delivery_kind = "request"
        response_delivery_id = request_delivery_id
        error = ResponseError(
            code=ErrorCode.CMO_LUA_ERROR,
            message="mutation preflight rejected the request",
            details={"stage": "handler_preflight"},
            mutation_not_started=MutationNotStartedEvidence(
                schema_version=1,
                stage="handler_preflight",
                request_id=command.request_id,
                request_hash=expectation.request_hash,
                operation=command.body.operation,
                mutation_barrier_written=False,
                execute_started=False,
            ),
        )
        settlement = RejectedSettlement(state="rejected", error=error)
        envelope_result = None
        ok = False
    else:
        invalid_result: JsonValue = {"unit_guid": "UNIT-1"}
        settlement = CompletedSettlement(state="completed", result=invalid_result)
        ok = True
        if case == "request_completed_invalid":
            delivery_kind = "request"
            response_delivery_id = request_delivery_id
            envelope_result = invalid_result
        else:
            assert case == "cancel_completed_invalid"
            delivery_kind = "cancel"
            response_delivery_id = cancel_delivery_id
            acknowledgement = CancelAckResult(
                request_id=command.request_id,
                request_hash=expectation.request_hash,
                original_delivery_id=request_delivery_id,
                status="completed",
                result=invalid_result,
            )
            envelope_result = cast(JsonValue, acknowledgement.model_dump(mode="json"))

    snapshot = command.runtime_snapshot
    envelope = ResponseEnvelope(
        protocol=snapshot.protocol,
        request_id=command.request_id,
        delivery_id=response_delivery_id,
        request_hash=expectation.request_hash,
        ok=ok,
        result=envelope_result,
        error=error,
        scenario_time="2026-07-12T12:00:00Z",
        scenario_lineage_id=base.LINEAGE_ID,
        activation_id=base.ACTIVATION_ID,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        bridge_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
    )
    accepted = AcceptedResponse(
        envelope=envelope,
        delivery_kind=delivery_kind,
        settlement=settlement,
        cancel_ack=acknowledgement,
    )
    inner = _canonical_json(envelope.model_dump(mode="json"))
    raw = _canonical_json({"Comments": inner}).encode("utf-8")
    response_path.write_bytes(raw)
    return (
        ResponseArtifact(
            filename=response_path.name,
            sha256=hashlib.sha256(raw).hexdigest(),
            size_bytes=len(raw),
            accepted_at_ms=time.time_ns() // 1_000_000,
            accepted_response=accepted,
        ),
        normalized_result,
    )


@dataclass(slots=True)
class _SettlementRig:
    case: _SettlementCase
    trace: list[str] = field(default_factory=_new_strings)
    original_wait_started: asyncio.Event = field(default_factory=asyncio.Event)
    cancel_wait_started: asyncio.Event = field(default_factory=asyncio.Event)
    allow_response: asyncio.Event = field(default_factory=asyncio.Event)
    original_cancellations: list[asyncio.CancelledError] = field(default_factory=_new_cancellations)
    journals: list[PendingJournal] = field(default_factory=_new_journals)
    transitions: list[RequestRecord] = field(default_factory=_new_requests)
    recorded_artifacts: list[ResponseArtifact] = field(default_factory=_new_artifacts)
    delete_expectations: list[JournalDeleteExpectation] = field(
        default_factory=_new_delete_expectations
    )
    unlock_observations: list[_UnlockObservation] = field(default_factory=_new_unlock_observations)
    original_expectation: ResponseExpectation | None = None
    request_delivery_id: UUID | None = None
    cancel_delivery_id: UUID | None = None
    artifact: ResponseArtifact | None = None
    normalized_result: JsonValue | None = None
    channel: _FileBridgeChannel | None = None
    response_journal_error: BridgeError | None = None
    response_record_error: BridgeError | None = None
    idle_publications: int = 0


def _install_settlement_rig(
    monkeypatch: pytest.MonkeyPatch,
    *,
    harness: base.Harness,
    transport: FileBridgeTransport,
    command: ExchangeCommand,
    rig: _SettlementRig,
) -> None:
    original_save = transport.journals.save
    original_record_response = transport.ledger.record_response
    original_transition = transport.ledger.transition
    original_publish_idle = InboxPublisher.publish_idle
    original_delete = transport.journals.delete
    original_lock_exit = RootLock.__aexit__
    response_path = transport.paths.response_path(command.request_id)
    original_response_delete = cleanup_module._set_delete_disposition  # pyright: ignore[reportPrivateUsage]

    async def controlled_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        expectation = waiter._expectation  # pyright: ignore[reportPrivateUsage]
        allowed = expectation.allowed_deliveries
        if len(allowed) == 1:
            assert allowed[0].delivery_kind == "request"
            assert timeout_seconds == command.timeout
            assert rig.original_expectation is None
            rig.original_expectation = expectation
            rig.request_delivery_id = allowed[0].delivery_id
            rig.trace.append("wait.original.started")
            rig.original_wait_started.set()
            try:
                await _bounded(
                    asyncio.Event().wait(),
                    label="original response waiter cancellation",
                )
            except asyncio.CancelledError as error:
                rig.original_cancellations.append(error)
                rig.trace.append("wait.original.cancelled")
                raise
            raise AssertionError("original response waiter unexpectedly resumed")

        assert timeout_seconds == base.CANCEL_ACK_TIMEOUT_SECONDS
        assert rig.request_delivery_id is not None
        assert allowed[0] == AllowedDelivery(rig.request_delivery_id, "request")
        assert len(allowed) == 2
        assert allowed[1].delivery_kind == "cancel"
        assert allowed[1].delivery_id not in {command.request_id, rig.request_delivery_id}
        assert expectation.request_id == command.request_id
        assert rig.original_expectation is not None
        assert expectation.request_hash == rig.original_expectation.request_hash
        assert expectation.runtime_snapshot == rig.original_expectation.runtime_snapshot
        recovered_invocation = expectation.invocation
        original_invocation = rig.original_expectation.invocation
        assert recovered_invocation.contract == original_invocation.contract
        assert recovered_invocation.wire_arguments == original_invocation.wire_arguments
        assert recovered_invocation.effective_class is original_invocation.effective_class
        assert recovered_invocation.result_schema == original_invocation.result_schema
        assert recovered_invocation.recovery_schema == original_invocation.recovery_schema
        assert not response_path.exists()
        rig.cancel_delivery_id = allowed[1].delivery_id
        rig.trace.append("wait.cancel.started")
        rig.cancel_wait_started.set()
        await _bounded(rig.allow_response.wait(), label="cancel response release gate")
        artifact, normalized_result = _response_artifact(
            case=rig.case,
            command=command,
            expectation=expectation,
            request_delivery_id=allowed[0].delivery_id,
            cancel_delivery_id=allowed[1].delivery_id,
            response_path=response_path,
        )
        rig.artifact = artifact
        rig.normalized_result = normalized_result
        rig.trace.append("wait.cancel.artifact")
        return artifact

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        try:
            revisions = original_save(journal, expected_revisions=expected_revisions)
        except BridgeError as error:
            if journal.original.revision == 4:
                rig.response_journal_error = error
                rig.trace.append("journal.r4.rejected")
            raise
        rig.journals.append(journal)
        rig.trace.append(f"journal.r{journal.original.revision}.{journal.original.state.value}")
        return revisions

    def traced_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        try:
            record = original_record_response(artifact)
        except BridgeError as error:
            rig.response_record_error = error
            rig.trace.append("ledger.response.rejected")
            raise
        rig.recorded_artifacts.append(artifact)
        rig.trace.append(f"ledger.response.{record.delivery_kind}")
        return record

    def traced_transition(
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
        record = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        rig.transitions.append(record)
        rig.trace.append(f"ledger.request.{new_state.value}")
        return record

    def traced_publish_idle(publisher: InboxPublisher) -> None:
        original_publish_idle(publisher)
        rig.idle_publications += 1
        rig.trace.append("inbox.idle")

    def traced_delete(expected: JournalDeleteExpectation) -> None:
        original_delete(expected)
        rig.delete_expectations.append(expected)
        rig.trace.append("journal.deleted")

    async def traced_lock_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if lock is harness.lock:
            lock.require_acquired()
            loaded = transport.journals.load()
            request = transport.ledger.get_request(command.request_id)
            cleanup_paths = ()
            if rig.channel is not None:
                cleanup_paths = tuple(
                    rig.channel._cleanup_paths  # pyright: ignore[reportPrivateUsage]
                )
            rig.unlock_observations.append(
                (
                    None if loaded is None else loaded.journal.original.state,
                    None if request is None else request.state,
                    cleanup_paths,
                    response_path.exists(),
                )
            )
            rig.trace.append("owner.unlock")
        return await original_lock_exit(lock, exc_type, exc, traceback)

    def traced_response_delete(handle: int, path: Path) -> None:
        if path == response_path.resolve(strict=True):
            rig.trace.append("response.cleanup")
        original_response_delete(handle, path)

    monkeypatch.setattr(ResponseWaiter, "wait", controlled_wait)
    monkeypatch.setattr(transport.journals, "save", traced_save)
    monkeypatch.setattr(transport.ledger, "record_response", traced_record_response)
    monkeypatch.setattr(transport.ledger, "transition", traced_transition)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_publish_idle)
    monkeypatch.setattr(transport.journals, "delete", traced_delete)
    monkeypatch.setattr(RootLock, "__aexit__", traced_lock_exit)
    monkeypatch.setattr(cleanup_module, "_set_delete_disposition", traced_response_delete)


@dataclass(frozen=True, slots=True)
class _SettlementOutcome:
    transport: FileBridgeTransport
    command: ExchangeCommand
    rig: _SettlementRig
    caught: asyncio.CancelledError
    final_journal: PendingJournal | None
    final_request: RequestRecord
    queued_before_exit: tuple[Path, ...]
    response_existed_before_exit: bool
    channel_poisoned_before_exit: bool
    mutation_poisoned_before_exit: bool
    response_exists_after_exit: bool


async def _exercise_settlement(
    *,
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: _SettlementCase,
) -> _SettlementOutcome:
    baseline_tasks = frozenset(asyncio.all_tasks())
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    transport = base._transport(harness)  # pyright: ignore[reportPrivateUsage]
    command = base._command(harness)  # pyright: ignore[reportPrivateUsage]
    rig = _SettlementRig(case=case)
    _install_settlement_rig(
        monkeypatch,
        harness=harness,
        transport=transport,
        command=command,
        rig=rig,
    )
    session = transport.session()
    exchange_task: asyncio.Task[ResponseArtifact] | None = None
    exit_task: asyncio.Task[bool | None] | None = None
    session_entered = False
    outcome: _SettlementOutcome | None = None

    try:
        channel = await _bounded(session.__aenter__(), label="settlement session enter")
        session_entered = True
        concrete_channel = cast(_FileBridgeChannel, channel)
        rig.channel = concrete_channel
        exchange_task = asyncio.create_task(channel.exchange(command))
        await base._require_event_before_task(  # pyright: ignore[reportPrivateUsage]
            rig.original_wait_started,
            exchange_task,
            "exchange finished before its original response waiter started",
        )
        assert exchange_task.cancel(_CANCEL_MESSAGE) is True
        cancel_wait_entered = await base._event_won_before_task(  # pyright: ignore[reportPrivateUsage]
            rig.cancel_wait_started,
            exchange_task,
            "published cancellation finished before the dual-delivery waiter started",
        )
        assert cancel_wait_entered, f"recovery failed before dual waiter: trace={rig.trace!r}"
        assert not exchange_task.done()
        loaded_r3 = transport.journals.load()
        assert loaded_r3 is not None
        assert loaded_r3.journal.original.revision == 3
        assert loaded_r3.journal.original.state is PendingPhase.CANCEL_PUBLISHED

        with pytest.raises(BridgeError) as blocked:
            async with RootLock(harness.paths.lock_file, timeout_seconds=0):
                raise AssertionError("contender acquired the root lock during settlement")
        assert blocked.value.code is ErrorCode.STATE_CONFLICT
        rig.trace.append("contender.blocked")

        rig.allow_response.set()
        with pytest.raises(asyncio.CancelledError) as caught:
            await base._bounded_task_outcome(  # pyright: ignore[reportPrivateUsage]
                exchange_task,
                label="published cancellation settlement",
            )
        rig.trace.append("cancellation.reraised")
        assert rig.original_cancellations == [caught.value]
        assert rig.original_cancellations[0] is caught.value
        assert caught.value.args == (_CANCEL_MESSAGE,)
        assert caught.value.__cause__ is None
        assert caught.value.__context__ is None

        loaded = transport.journals.load()
        final_journal = None if loaded is None else loaded.journal
        final_request = transport.ledger.get_request(command.request_id)
        assert final_request is not None
        queued_before_exit = tuple(
            concrete_channel._cleanup_paths  # pyright: ignore[reportPrivateUsage]
        )
        response_path = transport.paths.response_path(command.request_id)
        response_existed_before_exit = response_path.exists()
        channel_poisoned_before_exit = concrete_channel._poisoned  # pyright: ignore[reportPrivateUsage]
        mutation_poisoned_before_exit = (
            concrete_channel._mutation_exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]
        )

        exit_task = asyncio.create_task(session.__aexit__(None, None, None))
        assert (
            await base._bounded_task_outcome(  # pyright: ignore[reportPrivateUsage]
                exit_task,
                label="settlement session exit",
            )
            is False
        )
        session_entered = False
        outcome = _SettlementOutcome(
            transport=transport,
            command=command,
            rig=rig,
            caught=caught.value,
            final_journal=final_journal,
            final_request=final_request,
            queued_before_exit=queued_before_exit,
            response_existed_before_exit=response_existed_before_exit,
            channel_poisoned_before_exit=channel_poisoned_before_exit,
            mutation_poisoned_before_exit=mutation_poisoned_before_exit,
            response_exists_after_exit=response_path.exists(),
        )
    finally:
        rig.allow_response.set()
        if exit_task is None and session_entered:
            exit_task = asyncio.create_task(session.__aexit__(None, None, None))
        if exit_task is not None:
            await _quietly_finish(exit_task)
        if exchange_task is not None:
            await _quietly_finish(exchange_task)
        await _bounded(asyncio.sleep(0), label="settlement task audit checkpoint")
        current = asyncio.current_task()
        leaked = tuple(
            task
            for task in asyncio.all_tasks()
            if task not in baseline_tasks and task is not current and not task.done()
        )
        assert leaked == ()

    assert outcome is not None
    return outcome


def _assert_common_artifact_first(
    outcome: _SettlementOutcome,
) -> tuple[PendingJournal, PendingJournal]:
    rig = outcome.rig
    artifact = rig.artifact
    assert artifact is not None
    assert rig.request_delivery_id is not None
    assert rig.cancel_delivery_id is not None
    delivery_kind = artifact.accepted_response.delivery_kind
    response_delivery_id = (
        rig.request_delivery_id if delivery_kind == "request" else rig.cancel_delivery_id
    )
    assert artifact.accepted_response.envelope.delivery_id == response_delivery_id
    assert rig.recorded_artifacts == [artifact]

    revisions = [journal.original.revision for journal in rig.journals]
    assert revisions == [0, 1, 2, 3, 4, 5]
    r4, r5 = rig.journals[4:]
    assert r4.original.state is PendingPhase.RESPONSE_ACCEPTED
    assert r4.original.response_artifact == artifact
    assert r4.original.settlement == artifact.accepted_response.settlement
    assert r5.original.response_artifact == artifact
    assert r5.original.settlement == artifact.accepted_response.settlement
    _assert_order(
        rig.trace,
        "wait.cancel.started",
        "contender.blocked",
        "wait.cancel.artifact",
        "journal.r4.response_accepted",
        f"ledger.response.{delivery_kind}",
        "ledger.request.response_accepted",
    )

    request_delivery = outcome.transport.ledger.get_delivery(rig.request_delivery_id)
    cancel_delivery = outcome.transport.ledger.get_delivery(rig.cancel_delivery_id)
    assert request_delivery is not None
    assert cancel_delivery is not None
    responding_delivery, other_delivery = (
        (request_delivery, cancel_delivery)
        if delivery_kind == "request"
        else (cancel_delivery, request_delivery)
    )
    assert responding_delivery.response_artifact == artifact
    assert responding_delivery.settlement == artifact.accepted_response.settlement
    assert other_delivery.response_artifact is None
    assert other_delivery.settlement is None
    return r4, r5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "terminal_state"),
    [
        pytest.param("cancelled", HostRequestState.CANCELLED, id="cancelled-ack"),
        pytest.param("completed", HostRequestState.COMPLETED, id="completed-ack"),
    ],
)
async def test_cancel_ack_settles_artifact_first_before_unlock_and_cleanup(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: Literal["cancelled", "completed"],
    terminal_state: HostRequestState,
) -> None:
    outcome = await _exercise_settlement(
        harness=harness,
        monkeypatch=monkeypatch,
        case=case,
    )
    _r4, r5 = _assert_common_artifact_first(outcome)
    rig = outcome.rig
    response_path = outcome.transport.paths.response_path(outcome.command.request_id)

    assert r5.original.state is PendingPhase.IDLE_PUBLISHED
    assert outcome.final_journal is None
    assert rig.idle_publications == 1
    assert outcome.transport.paths.inbox.read_bytes() == render_idle_lua()
    assert len(rig.delete_expectations) == 1
    assert rig.delete_expectations[0].revisions == JournalRevisions(
        original=5,
        reconcile_attempt=None,
    )
    assert outcome.final_request.state is terminal_state
    assert outcome.final_request.terminal_at_ms is not None
    assert outcome.final_request.error_json is None
    assert outcome.final_request.resolution_json is None
    if terminal_state is HostRequestState.CANCELLED:
        assert rig.artifact is not None
        assert isinstance(rig.artifact.accepted_response.settlement, CancelledSettlement)
        assert rig.artifact.accepted_response.cancel_ack is not None
        assert rig.artifact.accepted_response.cancel_ack.status == "cancelled"
        assert rig.artifact.accepted_response.result is None
        assert outcome.final_request.result_json is None
        terminal_event = "ledger.request.cancelled"
    else:
        assert rig.artifact is not None
        assert isinstance(rig.artifact.accepted_response.settlement, CompletedSettlement)
        assert rig.artifact.accepted_response.cancel_ack is not None
        assert rig.artifact.accepted_response.cancel_ack.status == "completed"
        assert rig.normalized_result is not None
        assert rig.artifact.accepted_response.cancel_ack.result == rig.normalized_result
        assert rig.artifact.accepted_response.result == rig.normalized_result
        assert outcome.final_request.result_json == _canonical_json(rig.normalized_result)
        terminal_event = "ledger.request.completed"

    assert outcome.queued_before_exit == (response_path,)
    assert outcome.response_existed_before_exit is True
    assert outcome.channel_poisoned_before_exit is False
    assert outcome.mutation_poisoned_before_exit is False
    assert outcome.response_exists_after_exit is False
    assert rig.channel is not None
    assert rig.channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    assert rig.unlock_observations == [(None, terminal_state, (response_path,), True)]
    _assert_order(
        rig.trace,
        "journal.r4.response_accepted",
        "ledger.response.cancel",
        "ledger.request.response_accepted",
        "inbox.idle",
        "journal.r5.idle_published",
        "ledger.request.idle_published",
        terminal_event,
        "journal.deleted",
        "cancellation.reraised",
        "owner.unlock",
        "response.cleanup",
    )


@pytest.mark.asyncio
async def test_original_request_rejection_settles_rejected_before_unlock_and_cleanup(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome = await _exercise_settlement(
        harness=harness,
        monkeypatch=monkeypatch,
        case="request_rejected",
    )
    _r4, r5 = _assert_common_artifact_first(outcome)
    rig = outcome.rig
    artifact = rig.artifact
    assert artifact is not None
    response_path = outcome.transport.paths.response_path(outcome.command.request_id)

    accepted = artifact.accepted_response
    assert accepted.delivery_kind == "request"
    assert accepted.cancel_ack is None
    assert isinstance(accepted.settlement, RejectedSettlement)
    assert accepted.error is not None
    evidence = accepted.error.mutation_not_started
    assert evidence is not None
    assert evidence.request_id == outcome.command.request_id
    assert evidence.request_hash == accepted.envelope.request_hash
    assert evidence.operation == outcome.command.body.operation
    assert evidence.mutation_barrier_written is False
    assert evidence.execute_started is False

    assert r5.original.state is PendingPhase.IDLE_PUBLISHED
    assert outcome.final_journal is None
    assert outcome.final_request.state is HostRequestState.REJECTED
    assert outcome.final_request.terminal_at_ms is not None
    assert outcome.final_request.result_json is None
    assert outcome.final_request.error_json == _canonical_json(
        accepted.error.model_dump(mode="json")
    )
    assert outcome.final_request.resolution_json is None
    assert rig.idle_publications == 1
    assert outcome.transport.paths.inbox.read_bytes() == render_idle_lua()
    assert len(rig.delete_expectations) == 1
    assert rig.delete_expectations[0].revisions == JournalRevisions(
        original=5,
        reconcile_attempt=None,
    )
    assert outcome.queued_before_exit == (response_path,)
    assert outcome.response_existed_before_exit is True
    assert outcome.channel_poisoned_before_exit is False
    assert outcome.mutation_poisoned_before_exit is False
    assert outcome.response_exists_after_exit is False
    assert rig.channel is not None
    assert rig.channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    assert rig.unlock_observations == [(None, HostRequestState.REJECTED, (response_path,), True)]
    _assert_order(
        rig.trace,
        "journal.r4.response_accepted",
        "ledger.response.request",
        "ledger.request.response_accepted",
        "inbox.idle",
        "journal.r5.idle_published",
        "ledger.request.idle_published",
        "ledger.request.rejected",
        "journal.deleted",
        "cancellation.reraised",
        "owner.unlock",
        "response.cleanup",
    )


@pytest.mark.asyncio
async def test_cancel_error_is_artifact_first_r5_quarantine_without_idle_or_cleanup(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome = await _exercise_settlement(
        harness=harness,
        monkeypatch=monkeypatch,
        case="error",
    )
    _r4, r5 = _assert_common_artifact_first(outcome)
    rig = outcome.rig

    assert rig.artifact is not None
    assert rig.artifact.accepted_response.cancel_ack is None
    assert rig.artifact.accepted_response.settlement is None
    assert r5.original.state is PendingPhase.QUARANTINED
    assert outcome.final_journal == r5
    assert outcome.final_request.state is HostRequestState.QUARANTINED
    assert outcome.final_request.terminal_at_ms is None
    assert outcome.final_request.result_json is None
    assert outcome.final_request.error_json == _canonical_json(
        {
            "code": ErrorCode.INDETERMINATE_OUTCOME.value,
            "details": {
                "reason": "response_without_terminal_settlement",
                "request_id": str(outcome.command.request_id),
                "response_code": ErrorCode.INDETERMINATE_OUTCOME.value,
            },
            "message": "mutation response does not prove a terminal outcome",
        }
    )
    assert outcome.final_request.resolution_json is None
    assert rig.idle_publications == 0
    assert rig.delete_expectations == []
    assert outcome.queued_before_exit == ()
    assert outcome.response_existed_before_exit is True
    assert outcome.channel_poisoned_before_exit is True
    assert outcome.mutation_poisoned_before_exit is True
    assert outcome.response_exists_after_exit is True
    assert rig.channel is not None
    assert rig.channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    assert rig.unlock_observations == [
        (PendingPhase.QUARANTINED, HostRequestState.QUARANTINED, (), True)
    ]
    assert outcome.transport.paths.inbox.read_bytes() != render_idle_lua()
    assert "inbox.idle" not in rig.trace
    assert "journal.deleted" not in rig.trace
    assert "response.cleanup" not in rig.trace
    _assert_order(
        rig.trace,
        "journal.r4.response_accepted",
        "ledger.response.cancel",
        "ledger.request.response_accepted",
        "journal.r5.quarantined",
        "ledger.request.quarantined",
        "cancellation.reraised",
        "owner.unlock",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "delivery_kind"),
    [
        pytest.param("request_completed_invalid", "request", id="original-request"),
        pytest.param("cancel_completed_invalid", "cancel", id="cancel-ack"),
    ],
)
async def test_schema_invalid_completed_artifact_retains_r3_barrier_and_poison(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: Literal["request_completed_invalid", "cancel_completed_invalid"],
    delivery_kind: Literal["request", "cancel"],
) -> None:
    outcome = await _exercise_settlement(
        harness=harness,
        monkeypatch=monkeypatch,
        case=case,
    )
    rig = outcome.rig
    artifact = rig.artifact
    assert artifact is not None

    accepted = artifact.accepted_response
    assert accepted.delivery_kind == delivery_kind
    assert isinstance(accepted.settlement, CompletedSettlement)
    assert accepted.result == {"unit_guid": "UNIT-1"}
    if delivery_kind == "request":
        assert accepted.cancel_ack is None
    else:
        assert accepted.cancel_ack is not None
        assert accepted.cancel_ack.status == "completed"
        assert accepted.cancel_ack.result == accepted.result

    assert [journal.original.revision for journal in rig.journals] == [0, 1, 2, 3]
    r3 = rig.journals[-1]
    assert r3.original.state is PendingPhase.CANCEL_PUBLISHED
    assert r3.original.response_artifact is None
    assert r3.original.settlement is None
    assert outcome.final_journal == r3
    assert outcome.final_request.state is HostRequestState.CANCEL_PUBLISHED
    assert outcome.final_request.terminal_at_ms is None
    assert outcome.final_request.result_json is None
    assert outcome.final_request.error_json is None
    assert outcome.final_request.resolution_json is None
    assert rig.recorded_artifacts == []
    assert rig.response_record_error is None
    assert rig.response_journal_error is not None
    assert rig.response_journal_error.code is ErrorCode.STATE_CONFLICT
    assert rig.response_journal_error.message == (
        "pending journal candidate failed semantic validation"
    )
    assert rig.request_delivery_id is not None
    assert rig.cancel_delivery_id is not None
    for delivery_id in (rig.request_delivery_id, rig.cancel_delivery_id):
        delivery = outcome.transport.ledger.get_delivery(delivery_id)
        assert delivery is not None
        assert delivery.response_artifact is None
        assert delivery.settlement is None

    notes = tuple(getattr(outcome.caught, "__notes__", ()))
    recovery_notes = tuple(
        note for note in notes if note.startswith("cancellation recovery failure:")
    )
    assert len(recovery_notes) == 1
    assert "BridgeError" in recovery_notes[0]
    assert rig.response_journal_error.message in recovery_notes[0]
    assert rig.idle_publications == 0
    assert rig.delete_expectations == []
    assert outcome.queued_before_exit == ()
    assert outcome.response_existed_before_exit is True
    assert outcome.response_exists_after_exit is True
    assert rig.channel is not None
    assert rig.channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
    assert rig.channel._poisoned is True  # pyright: ignore[reportPrivateUsage]
    mutation_exchange = rig.channel._mutation_exchange  # pyright: ignore[reportPrivateUsage]
    assert mutation_exchange._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]
    assert rig.unlock_observations == [
        (PendingPhase.CANCEL_PUBLISHED, HostRequestState.CANCEL_PUBLISHED, (), True)
    ]
    assert outcome.transport.paths.inbox.read_bytes() != render_idle_lua()
    assert "inbox.idle" not in rig.trace
    assert "journal.deleted" not in rig.trace
    assert "response.cleanup" not in rig.trace
    _assert_order(
        rig.trace,
        "wait.cancel.artifact",
        "journal.r4.rejected",
        "cancellation.reraised",
        "owner.unlock",
    )
