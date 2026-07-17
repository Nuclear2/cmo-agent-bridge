from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import sys
from collections.abc import Awaitable
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, Never, TypeVar, cast
from uuid import UUID

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua
from cmo_agent_bridge.protocol.models import (
    AllowedDelivery,
    ExchangeCommand,
    PreparedDelivery,
    RequestBody,
    ResponseExpectation,
)
from cmo_agent_bridge.protocol.response_models import ResponseArtifact
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    PendingJournal,
    PendingPhase,
)
from cmo_agent_bridge.state.pending_journal import JournalRevisions
from cmo_agent_bridge.state.request_ledger import DeliveryRecord, RequestRecord
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo
from cmo_agent_bridge.transports.file_bridge.response_waiter import ResponseWaiter
from cmo_agent_bridge.transports.file_bridge.transport import (
    FileBridgeTransport,
    _FileBridgeChannel,  # pyright: ignore[reportPrivateUsage]
)

if TYPE_CHECKING:
    from tests.helpers.fake_file_bridge_peer import FakeFileBridgePeer, StaySilent
else:
    sys.path.insert(0, str(Path(__file__).parents[3] / "helpers"))
    from fake_file_bridge_peer import FakeFileBridgePeer, StaySilent


REQUEST_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeea001")
LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")
CANCEL_ACK_TIMEOUT_SECONDS = 7.25
TEST_TIMEOUT_SECONDS = 2

_T = TypeVar("_T")


async def _bounded(awaitable: Awaitable[_T], *, label: str) -> _T:
    try:
        async with asyncio.timeout(TEST_TIMEOUT_SECONDS):
            return await awaitable
    except TimeoutError as error:
        raise AssertionError(f"{label} exceeded the test deadline") from error


def _assert_uuid4(value: UUID) -> None:
    assert type(value) is UUID
    assert value.version == 4
    assert UUID(str(value)) == value


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _forbidden(*_args: object, **_kwargs: object) -> Never:
    raise AssertionError("forbidden recovery side effect was reached")


class Inspector:
    def __init__(self, process: ProcessInfo) -> None:
        self.process = process

    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        assert command_exe == self.process.executable
        return (self.process,)


@dataclass(frozen=True, slots=True)
class Harness:
    paths: FileBridgePaths
    lock: RootLock
    process: ProcessInfo
    inspector: Inspector
    snapshot: RuntimeSnapshot
    catalog: ManifestCatalog
    database: StateDatabase


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    game_root = tmp_path / "game"
    game_root.mkdir()
    command_exe = game_root / "Command.exe"
    command_exe.write_bytes(b"exe")
    (game_root / "Lua").mkdir()
    (game_root / "ImportExport").mkdir()
    local_app_data = tmp_path / "local"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    lock = RootLock(paths.lock_file, timeout_seconds=0)
    process = ProcessInfo(pid=1234, create_time=1000.5, executable=paths.command_exe)
    inspector = Inspector(process)
    snapshot = RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=MANIFEST_SHA256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )
    catalog = ManifestCatalog(ReleaseBinding(snapshot=snapshot, registry=OPERATION_REGISTRY))
    return Harness(
        paths=paths,
        lock=lock,
        process=process,
        inspector=inspector,
        snapshot=snapshot,
        catalog=catalog,
        database=StateDatabase(paths.sqlite_file),
    )


def _command(harness: Harness) -> ExchangeCommand:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "unit.set",
        {"unit_guid": "UNIT-1", "name": "Recovery cancellation"},
    )
    body = RequestBody(
        protocol=harness.snapshot.protocol,
        release_id=harness.snapshot.release_id,
        runtime_version=harness.snapshot.runtime_version,
        runtime_tag=harness.snapshot.runtime_tag,
        runtime_asset_sha256=harness.snapshot.runtime_asset_sha256,
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=ACTIVATION_ID,
        operation_manifest_sha256=harness.snapshot.operation_manifest_sha256,
        operation="unit.set",
        arguments=invocation.wire_arguments.model_dump(mode="json"),
    )
    return ExchangeCommand(
        request_id=REQUEST_ID,
        body=body,
        invocation=invocation,
        runtime_snapshot=harness.snapshot,
        timeout=30,
    )


def _transport(harness: Harness) -> FileBridgeTransport:
    values: dict[str, object] = {
        "paths": harness.paths,
        "root_lock": harness.lock,
        "process_inspector": harness.inspector,
        "catalog": harness.catalog,
        "database": harness.database,
        "max_journal_bytes": 1_000_000,
        "replace_retry_seconds": 0,
        "response_poll_seconds": 0.001,
    }
    if "cancel_ack_timeout_seconds" in inspect.signature(FileBridgeTransport.__init__).parameters:
        values["cancel_ack_timeout_seconds"] = CANCEL_ACK_TIMEOUT_SECONDS
    return cast(Any, FileBridgeTransport)(**values)


def _peer(harness: Harness, trace: list[str]) -> FakeFileBridgePeer:
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    return FakeFileBridgePeer(
        paths=harness.paths,
        runtime_snapshot=harness.snapshot,
        registry=OPERATION_REGISTRY,
        scenario_lineage_id=LINEAGE_ID,
        poll_seconds=0.001,
        trace=trace,
    )


class _NoAckRig:
    def __init__(self, *, trace: list[str]) -> None:
        self.trace = trace
        self.original_observed = asyncio.Event()
        self.original_wait_started = asyncio.Event()
        self.cancel_wait_started = asyncio.Event()
        self.allow_cancel_poll = asyncio.Event()
        self.quarantine_durable = asyncio.Event()
        self.original_expectation: ResponseExpectation | None = None
        self.cancel_expectation: ResponseExpectation | None = None
        self.request_delivery_id: UUID | None = None
        self.cancel_delivery_id: UUID | None = None
        self.wait_cancellations: list[asyncio.CancelledError] = []
        self.cancel_deliveries: list[PreparedDelivery] = []
        self.journals: list[PendingJournal] = []
        self.prepared_requests: list[RequestRecord] = []
        self.inserted_intents: list[DeliveryIntent] = []
        self.marked_deliveries: list[DeliveryRecord] = []
        self.transition_calls: list[
            tuple[
                frozenset[HostRequestState],
                HostRequestState,
                int,
                RequestRecord,
            ]
        ] = []
        self.cancel_wait_timeouts: list[float] = []


def _install_no_ack_rig(
    monkeypatch: pytest.MonkeyPatch,
    *,
    transport: FileBridgeTransport,
    peer: FakeFileBridgePeer,
    command: ExchangeCommand,
    rig: _NoAckRig,
) -> None:
    original_apply_action = peer._apply_action  # pyright: ignore[reportPrivateUsage]
    original_wait = ResponseWaiter.wait
    original_save = transport.journals.save
    original_publish = InboxPublisher.publish_delivery
    original_insert_prepared = transport.ledger.insert_prepared
    original_insert_delivery = transport.ledger.insert_delivery
    original_mark_published = transport.ledger.mark_delivery_published
    original_transition = transport.ledger.transition

    async def observe_original(
        action: object,
        delivery: PreparedDelivery,
        body: RequestBody,
        invocation: object,
    ) -> None:
        assert type(action) is StaySilent
        assert delivery.request_id == command.request_id
        assert delivery.delivery_kind == "request"
        assert body == command.body
        _assert_uuid4(delivery.delivery_id)
        assert delivery.delivery_id != command.request_id
        if rig.request_delivery_id is None:
            rig.request_delivery_id = delivery.delivery_id
        else:
            assert rig.request_delivery_id == delivery.delivery_id
        await _bounded(
            cast(Any, original_apply_action)(action, delivery, body, invocation),
            label="fake peer original action",
        )
        rig.trace.append("peer.original_observed")
        rig.original_observed.set()

    async def controlled_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        expectation = waiter._expectation  # pyright: ignore[reportPrivateUsage]
        allowed = expectation.allowed_deliveries
        if len(allowed) == 1 and allowed[0].delivery_kind == "request":
            assert rig.original_expectation is None
            assert timeout_seconds == command.timeout
            _assert_uuid4(allowed[0].delivery_id)
            assert allowed[0].delivery_id != command.request_id
            if rig.request_delivery_id is None:
                rig.request_delivery_id = allowed[0].delivery_id
            else:
                assert rig.request_delivery_id == allowed[0].delivery_id
            rig.original_expectation = expectation
            rig.trace.append("wait.original.started")
            rig.original_wait_started.set()
            try:
                await _bounded(
                    asyncio.Event().wait(),
                    label="original waiter cancellation",
                )
            except asyncio.CancelledError as error:
                rig.wait_cancellations.append(error)
                rig.trace.append("wait.original.cancelled")
                raise
            raise AssertionError("original response waiter unexpectedly resumed")

        assert rig.original_expectation is not None
        assert rig.request_delivery_id is not None
        assert rig.cancel_expectation is None
        assert timeout_seconds == CANCEL_ACK_TIMEOUT_SECONDS
        rig.cancel_wait_timeouts.append(timeout_seconds)
        assert len(allowed) == 2
        assert allowed[0] == AllowedDelivery(
            delivery_id=rig.request_delivery_id,
            delivery_kind="request",
        )
        assert allowed[1].delivery_kind == "cancel"
        _assert_uuid4(allowed[1].delivery_id)
        assert allowed[1].delivery_id not in {command.request_id, rig.request_delivery_id}
        rig.cancel_delivery_id = allowed[1].delivery_id
        assert expectation.request_id == rig.original_expectation.request_id
        assert expectation.request_hash == rig.original_expectation.request_hash
        assert expectation.expected_lineage_id == rig.original_expectation.expected_lineage_id
        assert expectation.expected_activation_id == rig.original_expectation.expected_activation_id
        assert expectation.status_bootstrap is False
        assert expectation.activation_candidate is None
        assert expectation.runtime_snapshot == rig.original_expectation.runtime_snapshot
        original_invocation = rig.original_expectation.invocation
        recovered_invocation = expectation.invocation
        assert recovered_invocation.contract == original_invocation.contract
        assert recovered_invocation.wire_arguments == original_invocation.wire_arguments
        assert recovered_invocation.effective_class is original_invocation.effective_class
        assert recovered_invocation.result_schema == original_invocation.result_schema
        assert recovered_invocation.recovery_schema == original_invocation.recovery_schema
        assert not waiter._response_path.exists()  # pyright: ignore[reportPrivateUsage]
        rig.cancel_expectation = expectation
        rig.trace.append("wait.cancel.started")
        rig.cancel_wait_started.set()
        await _bounded(
            rig.allow_cancel_poll.wait(),
            label="cancel waiter release gate",
        )
        rig.trace.append("wait.cancel.poll")
        try:
            return await _bounded(
                original_wait(waiter, 0),
                label="zero-timeout cancel waiter poll",
            )
        except BridgeError as error:
            assert error.code is ErrorCode.REQUEST_TIMEOUT
            assert error.details["timeout_seconds"] == 0.0
            rig.trace.append("wait.cancel.timeout")
            raise

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        revisions = original_save(journal, expected_revisions=expected_revisions)
        revision = journal.original.revision
        expected_phases = {
            0: PendingPhase.PREPARED,
            1: PendingPhase.PUBLISHED,
            2: PendingPhase.CANCEL_PUBLISHED,
            3: PendingPhase.CANCEL_PUBLISHED,
            4: PendingPhase.QUARANTINED,
        }
        assert revision in expected_phases
        assert journal.original.state is expected_phases[revision]
        assert revisions == JournalRevisions(original=revision, reconcile_attempt=None)
        assert expected_revisions == (
            None
            if revision == 0
            else JournalRevisions(original=revision - 1, reconcile_attempt=None)
        )
        assert all(candidate.original.revision != revision for candidate in rig.journals)
        rig.journals.append(journal)
        if revision == 0:
            rig.trace.append("journal.r0_prepared")
        elif revision == 1:
            rig.trace.append("journal.r1_published")
        elif journal.original.state is PendingPhase.CANCEL_PUBLISHED:
            cancel = tuple(
                intent
                for intent in journal.original.delivery_intents
                if intent.delivery_kind == "cancel"
            )
            assert len(cancel) == 1
            if cancel[0].published_at_ms is None:
                assert revision == 2
                rig.trace.append("journal.r2_cancel_intent")
            else:
                assert revision == 3
                rig.trace.append("journal.r3_cancel_marker")
        elif journal.original.state is PendingPhase.QUARANTINED:
            assert revision == 4
            rig.trace.append("journal.r4_quarantined")
            rig.quarantine_durable.set()
        return revisions

    def traced_publish(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        original_publish(
            publisher,
            delivery,
            runtime_snapshot=runtime_snapshot,
        )
        assert runtime_snapshot == command.runtime_snapshot
        if delivery.delivery_kind == "request":
            _assert_uuid4(delivery.delivery_id)
            assert rig.request_delivery_id in {None, delivery.delivery_id}
            rig.request_delivery_id = delivery.delivery_id
            rig.trace.append("inbox.request")
        else:
            assert delivery.delivery_kind == "cancel"
            _assert_uuid4(delivery.delivery_id)
            assert rig.request_delivery_id is not None
            assert delivery.delivery_id not in {command.request_id, rig.request_delivery_id}
            rig.cancel_delivery_id = delivery.delivery_id
            rig.cancel_deliveries.append(delivery)
            rig.trace.append("inbox.cancel")

    def traced_insert_prepared(record: RequestRecord) -> None:
        original_insert_prepared(record)
        rig.prepared_requests.append(record)
        rig.trace.append("ledger.request.prepared")

    def traced_insert_delivery(intent: DeliveryIntent) -> None:
        original_insert_delivery(intent)
        rig.inserted_intents.append(intent)
        rig.trace.append(f"ledger.delivery.{intent.delivery_kind}.inserted")

    def traced_mark_published(
        delivery_id: UUID,
        *,
        published_at_ms: int,
    ) -> DeliveryRecord:
        record = original_mark_published(
            delivery_id,
            published_at_ms=published_at_ms,
        )
        rig.marked_deliveries.append(record)
        rig.trace.append(f"ledger.delivery.{record.delivery_kind}.marked")
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
        rig.transition_calls.append((expected_states, new_state, updated_at_ms, record))
        rig.trace.append(f"ledger.request.{new_state.value}")
        return record

    monkeypatch.setattr(peer, "_apply_action", observe_original)
    monkeypatch.setattr(ResponseWaiter, "wait", controlled_wait)
    monkeypatch.setattr(transport.journals, "save", traced_save)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(transport.ledger, "insert_prepared", traced_insert_prepared)
    monkeypatch.setattr(transport.ledger, "insert_delivery", traced_insert_delivery)
    monkeypatch.setattr(transport.ledger, "mark_delivery_published", traced_mark_published)
    monkeypatch.setattr(transport.ledger, "transition", traced_transition)


async def _require_event_before_task(
    event: asyncio.Event,
    task: asyncio.Task[Any],
    message: str,
) -> None:
    if not await _event_won_before_task(event, task, message):
        await task
        pytest.fail(message)


async def _event_won_before_task(
    event: asyncio.Event,
    task: asyncio.Task[Any],
    message: str,
) -> bool:
    gate = asyncio.create_task(event.wait())
    try:
        done, _ = await asyncio.wait(
            {gate, task},
            timeout=TEST_TIMEOUT_SECONDS,
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert done, f"{message}: event and watched task both stalled"
        return event.is_set()
    finally:
        if not gate.done():
            gate.cancel("event race cleanup")
        try:
            await gate
        except asyncio.CancelledError:
            pass


async def _quietly_finish(task: asyncio.Task[Any]) -> None:
    if not task.done():
        try:
            await _bounded(asyncio.shield(task), label="task cleanup grace period")
        except BaseException:
            pass
    if not task.done():
        task.cancel("test cleanup drain")
        try:
            await _bounded(asyncio.shield(task), label="cancelled task cleanup drain")
        except BaseException:
            pass
    assert task.done()
    try:
        await task
    except BaseException:
        pass


async def _bounded_task_outcome(
    task: asyncio.Task[_T],
    *,
    label: str,
) -> _T:
    if task.done():
        return await task
    current = asyncio.current_task()
    assert current is not None
    baseline_cancellations = current.cancelling()
    try:
        await _bounded(asyncio.shield(task), label=label)
    except asyncio.CancelledError:
        if current.cancelling() > baseline_cancellations:
            raise
    except BaseException:
        if not task.done():
            raise
    if not task.done():
        done, _ = await asyncio.wait({task}, timeout=TEST_TIMEOUT_SECONDS)
        assert task in done, f"{label} did not reach a durable task outcome"
    assert task.done()
    return await task


def _task_baseline() -> frozenset[asyncio.Task[Any]]:
    return frozenset(asyncio.all_tasks())


def _assert_no_new_live_tasks(baseline: frozenset[asyncio.Task[Any]]) -> None:
    current = asyncio.current_task()
    leaked = tuple(
        task
        for task in asyncio.all_tasks()
        if task not in baseline and task is not current and not task.done()
    )
    assert leaked == ()


def _assert_order(trace: list[str], *events: str) -> None:
    indices = [trace.index(event) for event in events]
    assert indices == sorted(indices), trace


def _assert_complete_protocol_order(trace: list[str]) -> None:
    expected = [
        "journal.r0_prepared",
        "ledger.request.prepared",
        "ledger.delivery.request.inserted",
        "inbox.request",
        "journal.r1_published",
        "ledger.delivery.request.marked",
        "ledger.request.published",
        "wait.original.started",
        "wait.original.cancelled",
        "journal.r2_cancel_intent",
        "ledger.delivery.cancel.inserted",
        "inbox.cancel",
        "journal.r3_cancel_marker",
        "ledger.delivery.cancel.marked",
        "ledger.request.cancel_published",
        "wait.cancel.started",
        "wait.cancel.poll",
        "wait.cancel.timeout",
        "journal.r4_quarantined",
        "ledger.request.quarantined",
    ]
    expected_set = set(expected)
    assert [event for event in trace if event in expected_set] == expected


def _assert_no_ack_quarantine(
    *,
    transport: FileBridgeTransport,
    command: ExchangeCommand,
    peer: FakeFileBridgePeer,
    rig: _NoAckRig,
    journal: PendingJournal,
) -> None:
    assert rig.request_delivery_id is not None
    assert rig.cancel_delivery_id is not None
    _assert_uuid4(rig.request_delivery_id)
    _assert_uuid4(rig.cancel_delivery_id)
    assert len({command.request_id, rig.request_delivery_id, rig.cancel_delivery_id}) == 3
    assert journal.original.state is PendingPhase.QUARANTINED
    assert journal.original.revision == 4
    assert journal.original.response_artifact is None
    assert journal.original.settlement is None

    assert len(rig.journals) == 5
    by_revision = {candidate.original.revision: candidate for candidate in rig.journals}
    assert set(by_revision) == set(range(5))
    r0, r1, r2, r3, r4 = (by_revision[revision] for revision in range(5))
    assert r4 == journal
    assert [candidate.original.state for candidate in (r0, r1, r2, r3, r4)] == [
        PendingPhase.PREPARED,
        PendingPhase.PUBLISHED,
        PendingPhase.CANCEL_PUBLISHED,
        PendingPhase.CANCEL_PUBLISHED,
        PendingPhase.QUARANTINED,
    ]
    updates = [candidate.original.updated_at_ms for candidate in (r0, r1, r2, r3, r4)]
    assert updates == sorted(updates)

    r0_request = r0.original.delivery_intents[0]
    r1_request = r1.original.delivery_intents[0]
    r2_request, r2_cancel = r2.original.delivery_intents
    r3_request, r3_cancel = r3.original.delivery_intents
    r4_request, r4_cancel = r4.original.delivery_intents
    assert all(
        intent.delivery_id == rig.request_delivery_id
        for intent in (r0_request, r1_request, r2_request, r3_request, r4_request)
    )
    assert all(
        intent.delivery_id == rig.cancel_delivery_id for intent in (r2_cancel, r3_cancel, r4_cancel)
    )
    assert r2_cancel.original_request_delivery_id == rig.request_delivery_id
    assert r0.original.created_at_ms == r0.original.updated_at_ms == r0_request.intended_at_ms
    assert r0_request.published_at_ms is None
    assert r1_request.published_at_ms == r1.original.updated_at_ms
    assert r2_cancel.intended_at_ms == r2.original.updated_at_ms
    assert r2_cancel.published_at_ms is None
    assert r3_cancel.published_at_ms == r3.original.updated_at_ms
    assert r4.original.delivery_intents == r3.original.delivery_intents

    assert len(rig.cancel_deliveries) == 1
    cancel_delivery = rig.cancel_deliveries[0]
    assert cancel_delivery.request_id == command.request_id
    assert cancel_delivery.delivery_id == rig.cancel_delivery_id
    assert cancel_delivery.delivery_kind == "cancel"
    assert cancel_delivery.request_hash == r2_cancel.request_hash == r2.original.request_hash
    assert cancel_delivery.body_json == r2_cancel.body_json.encode("utf-8")
    rendered_cancel = render_delivery_lua(cancel_delivery, command.runtime_snapshot)
    rendered_sha256 = hashlib.sha256(rendered_cancel).hexdigest()
    response_filename = transport.paths.response_path(command.request_id).name
    for intent in (r2_cancel, r3_cancel, r4_cancel):
        assert intent.request_id == command.request_id
        assert intent.request_hash == cancel_delivery.request_hash
        assert intent.body_json.encode("utf-8") == cancel_delivery.body_json
        assert intent.runtime_snapshot == command.runtime_snapshot
        assert intent.rendered_inbox_sha256 == rendered_sha256
        assert intent.rendered_inbox_size_bytes == len(rendered_cancel)
        assert intent.response_filename == response_filename
    assert r3_cancel.model_copy(update={"published_at_ms": None}) == r2_cancel
    assert r4_cancel == r3_cancel

    assert len(rig.prepared_requests) == 1
    prepared_request = rig.prepared_requests[0]
    assert prepared_request.request_id == command.request_id
    assert prepared_request.state is HostRequestState.PREPARED
    assert (
        prepared_request.created_at_ms
        == prepared_request.updated_at_ms
        == r0.original.updated_at_ms
    )
    assert rig.inserted_intents == [r0_request, r2_cancel]
    assert len(rig.marked_deliveries) == 2
    request_mark, cancel_mark = rig.marked_deliveries
    assert request_mark.delivery_id == rig.request_delivery_id
    assert request_mark.intended_at_ms == r0_request.intended_at_ms
    assert request_mark.published_at_ms == r1.original.updated_at_ms
    assert cancel_mark.delivery_id == rig.cancel_delivery_id
    assert cancel_mark.intended_at_ms == r2.original.updated_at_ms
    assert cancel_mark.published_at_ms == r3.original.updated_at_ms
    assert cancel_mark.rendered_inbox_sha256 == rendered_sha256
    assert cancel_mark.rendered_inbox_size_bytes == len(rendered_cancel)
    assert cancel_mark.response_filename == response_filename

    assert [call[0] for call in rig.transition_calls] == [
        frozenset({HostRequestState.PREPARED}),
        frozenset({HostRequestState.PUBLISHED}),
        frozenset({HostRequestState.CANCEL_PUBLISHED}),
    ]
    assert [call[1] for call in rig.transition_calls] == [
        HostRequestState.PUBLISHED,
        HostRequestState.CANCEL_PUBLISHED,
        HostRequestState.QUARANTINED,
    ]
    assert [call[2] for call in rig.transition_calls] == [
        r1.original.updated_at_ms,
        r3.original.updated_at_ms,
        r4.original.updated_at_ms,
    ]
    assert [call[3].updated_at_ms for call in rig.transition_calls] == [
        r1.original.updated_at_ms,
        r3.original.updated_at_ms,
        r4.original.updated_at_ms,
    ]

    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.QUARANTINED
    assert request.terminal_at_ms is None
    assert request.result_json is None
    assert request.resolution_json is None
    assert request.updated_at_ms == r4.original.updated_at_ms
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
    for intent in (r4_request, r4_cancel):
        delivery = transport.ledger.get_delivery(intent.delivery_id)
        assert delivery is not None
        assert delivery.published_at_ms == intent.published_at_ms
        assert delivery.rendered_inbox_sha256 == intent.rendered_inbox_sha256
        assert delivery.rendered_inbox_size_bytes == intent.rendered_inbox_size_bytes
        assert delivery.response_filename == response_filename
        assert delivery.response_artifact is None
        assert delivery.settlement is None

    assert rig.original_expectation is not None
    assert rig.cancel_expectation is not None
    assert rig.cancel_expectation.allowed_deliveries == (
        AllowedDelivery(delivery_id=rig.request_delivery_id, delivery_kind="request"),
        AllowedDelivery(delivery_id=rig.cancel_delivery_id, delivery_kind="cancel"),
    )
    assert rig.cancel_wait_timeouts == [CANCEL_ACK_TIMEOUT_SECONDS]
    assert len(peer.observed_deliveries) == 1
    assert peer.observed_deliveries[0].delivery_id == rig.request_delivery_id
    assert rig.trace.count("peer.request_observed") == 1
    assert not transport.paths.response_path(command.request_id).exists()
    assert transport.paths.inbox.read_bytes() == rendered_cancel
    _assert_complete_protocol_order(rig.trace)


@pytest.mark.asyncio
async def test_direct_published_cancellation_without_ack_quarantines_before_reraise(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline_tasks = _task_baseline()
    transport = _transport(harness)
    command = _command(harness)
    trace: list[str] = []
    peer = _peer(harness, trace)
    peer.enqueue(StaySilent())
    rig = _NoAckRig(trace=trace)
    _install_no_ack_rig(
        monkeypatch,
        transport=transport,
        peer=peer,
        command=command,
        rig=rig,
    )
    session = transport.session()
    exchange_task: asyncio.Task[ResponseArtifact] | None = None
    exit_task: asyncio.Task[bool | None] | None = None
    session_entered = False
    peer_running = False

    await _bounded(peer.start(), label="fake peer start")
    peer_running = True
    try:
        channel = await _bounded(session.__aenter__(), label="direct recovery session enter")
        session_entered = True
        exchange_task = asyncio.create_task(channel.exchange(command))
        try:
            await _require_event_before_task(
                rig.original_wait_started,
                exchange_task,
                "exchange finished before its original response waiter started",
            )
            await _require_event_before_task(
                rig.original_observed,
                exchange_task,
                "exchange finished before the fake peer observed its original request",
            )
        except BaseException:
            await _bounded(peer.stop(), label="fake peer stop after setup failure")
            peer_running = False
            raise
        await _bounded(peer.stop(), label="fake peer stop before direct cancellation")
        peer_running = False

        assert exchange_task.cancel("first published mutation cancellation") is True
        cancel_wait_entered = await _event_won_before_task(
            rig.cancel_wait_started,
            exchange_task,
            "direct cancellation completed before entering the dual-delivery cancel wait",
        )
        second_cancel_sent = False
        if cancel_wait_entered:
            assert rig.wait_cancellations
            assert exchange_task.cancel("second cancellation during cancel settlement") is True
            second_cancel_sent = True
            await _bounded(asyncio.sleep(0), label="second cancellation delivery checkpoint")
            assert not exchange_task.done()
        rig.allow_cancel_poll.set()

        with pytest.raises(asyncio.CancelledError) as caught:
            await _bounded_task_outcome(
                exchange_task,
                label="shielded direct cancellation settlement",
            )
        trace.append("cancellation.reraised")
        assert caught.value.args == ("first published mutation cancellation",)
        assert rig.wait_cancellations == [caught.value]
        assert rig.wait_cancellations[0] is caught.value

        loaded = transport.journals.load()
        assert loaded is not None
        assert loaded.journal.original.state is PendingPhase.QUARANTINED
        assert cancel_wait_entered
        assert second_cancel_sent
        assert caught.value.__cause__ is None
        assert caught.value.__context__ is None
        cancellation_notes = tuple(getattr(caught.value, "__notes__", ()))
        second_cancellation_notes = tuple(
            note
            for note in cancellation_notes
            if "second cancellation during cancel settlement" in note
        )
        assert len(second_cancellation_notes) == 1
        assert "CancelledError" in second_cancellation_notes[0]
        assert all(ErrorCode.REQUEST_TIMEOUT.value not in note for note in cancellation_notes)
        assert all(
            "timed out waiting for a correlated CMO response" not in note
            for note in cancellation_notes
        )
        _assert_no_ack_quarantine(
            transport=transport,
            command=command,
            peer=peer,
            rig=rig,
            journal=loaded.journal,
        )
        assert rig.quarantine_durable.is_set()
        _assert_order(trace, "journal.r4_quarantined", "cancellation.reraised")

        concrete_channel = cast(_FileBridgeChannel, channel)
        assert concrete_channel._poisoned is True  # pyright: ignore[reportPrivateUsage]
        mutation_exchange = concrete_channel._mutation_exchange  # pyright: ignore[reportPrivateUsage]
        assert mutation_exchange._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]
        assert mutation_exchange._recovery_task is None  # pyright: ignore[reportPrivateUsage]
        assert concrete_channel._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]

        exit_task = asyncio.create_task(session.__aexit__(None, None, None))
        assert (
            await _bounded_task_outcome(
                exit_task,
                label="shielded direct recovery session exit",
            )
            is False
        )
        session_entered = False
    finally:
        rig.allow_cancel_poll.set()
        if peer_running:
            peer_stop_task = asyncio.create_task(peer.stop())
            await _quietly_finish(peer_stop_task)
        if exit_task is None and session_entered:
            exit_task = asyncio.create_task(session.__aexit__(None, None, None))
        if exit_task is not None:
            await _quietly_finish(exit_task)
        if exchange_task is not None and not exchange_task.done():
            exchange_task.cancel("direct cancellation test cleanup")
            await _quietly_finish(exchange_task)
        await _bounded(asyncio.sleep(0), label="direct task audit checkpoint")
        _assert_no_new_live_tasks(baseline_tasks)


@pytest.mark.asyncio
async def test_session_exit_holds_root_lock_until_cancel_no_ack_is_quarantined(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline_tasks = _task_baseline()
    transport = _transport(harness)
    command = _command(harness)
    trace: list[str] = []
    peer = _peer(harness, trace)
    peer.enqueue(StaySilent())
    rig = _NoAckRig(trace=trace)
    _install_no_ack_rig(
        monkeypatch,
        transport=transport,
        peer=peer,
        command=command,
        rig=rig,
    )
    original_exit = RootLock.__aexit__
    unlock_observations: list[tuple[PendingPhase | None, HostRequestState | None]] = []
    unlock_journals: list[PendingJournal] = []

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
            if loaded is not None:
                unlock_journals.append(loaded.journal)
            unlock_observations.append(
                (
                    None if loaded is None else loaded.journal.original.state,
                    None if request is None else request.state,
                )
            )
            trace.append("owner.unlock")
        return await _bounded(
            original_exit(lock, exc_type, exc, traceback),
            label="root lock exit",
        )

    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)
    session = transport.session()
    child: asyncio.Task[ResponseArtifact] | None = None
    exit_task: asyncio.Task[bool | None] | None = None
    session_entered = False
    peer_running = False

    await _bounded(peer.start(), label="fake peer start")
    peer_running = True
    try:
        channel = await _bounded(session.__aenter__(), label="session-exit recovery enter")
        session_entered = True
        child = asyncio.create_task(channel.exchange(command))
        try:
            await _require_event_before_task(
                rig.original_wait_started,
                child,
                "exchange finished before its original response waiter started",
            )
            await _require_event_before_task(
                rig.original_observed,
                child,
                "exchange finished before the fake peer observed its original request",
            )
        except BaseException:
            await _bounded(peer.stop(), label="fake peer stop after setup failure")
            peer_running = False
            raise
        await _bounded(peer.stop(), label="fake peer stop before session exit")
        peer_running = False

        exit_task = asyncio.create_task(session.__aexit__(None, None, None))
        await _require_event_before_task(
            rig.cancel_wait_started,
            exit_task,
            "session exit released the owner before entering the dual-delivery cancel wait",
        )
        loaded_before_quarantine = transport.journals.load()
        assert loaded_before_quarantine is not None
        assert loaded_before_quarantine.journal.original.state is PendingPhase.CANCEL_PUBLISHED
        assert not rig.quarantine_durable.is_set()
        assert not exit_task.done()
        assert child is not None and not child.done()

        with pytest.raises(BridgeError) as blocked:
            async with RootLock(harness.paths.lock_file, timeout_seconds=0):
                raise AssertionError("contender acquired the root lock before quarantine")
        assert blocked.value.code is ErrorCode.STATE_CONFLICT
        assert blocked.value.message == "timed out waiting for the root lock"
        trace.append("contender.blocked")

        rig.allow_cancel_poll.set()
        assert (
            await _bounded_task_outcome(
                exit_task,
                label="shielded session-exit cancel settlement",
            )
            is False
        )
        session_entered = False
        assert child.done() and child.cancelled()
        assert rig.quarantine_durable.is_set()
        concrete_channel = cast(_FileBridgeChannel, channel)
        mutation_exchange = concrete_channel._mutation_exchange  # pyright: ignore[reportPrivateUsage]
        assert mutation_exchange._recovery_task is None  # pyright: ignore[reportPrivateUsage]
        assert unlock_observations == [(PendingPhase.QUARANTINED, HostRequestState.QUARANTINED)]
        assert len(unlock_journals) == 1
        _assert_no_ack_quarantine(
            transport=transport,
            command=command,
            peer=peer,
            rig=rig,
            journal=unlock_journals[0],
        )

        async with RootLock(harness.paths.lock_file, timeout_seconds=0):
            trace.append("contender.acquired")
        _assert_order(
            trace,
            "wait.cancel.started",
            "contender.blocked",
            "wait.cancel.timeout",
            "journal.r4_quarantined",
            "owner.unlock",
            "contender.acquired",
        )
    finally:
        rig.allow_cancel_poll.set()
        if peer_running:
            peer_stop_task = asyncio.create_task(peer.stop())
            await _quietly_finish(peer_stop_task)
        if exit_task is None and session_entered:
            exit_task = asyncio.create_task(session.__aexit__(None, None, None))
        if exit_task is not None:
            await _quietly_finish(exit_task)
        if child is not None and not child.done():
            child.cancel("session-exit recovery test cleanup")
            await _quietly_finish(child)
        await _bounded(asyncio.sleep(0), label="session-exit task audit checkpoint")
        _assert_no_new_live_tasks(baseline_tasks)
