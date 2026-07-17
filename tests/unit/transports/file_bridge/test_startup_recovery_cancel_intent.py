from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any, Never

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua
from cmo_agent_bridge.protocol.models import ExchangeCommand, PreparedDelivery
from cmo_agent_bridge.protocol.response_models import ResponseArtifact
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    PendingJournal,
    PendingPhase,
)
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.recovery import RecoveryManager
from cmo_agent_bridge.transports.file_bridge.response_waiter import ResponseWaiter
from cmo_agent_bridge.transports.file_bridge.transport import FileBridgeTransport

sys.path.insert(0, str(Path(__file__).parent))

import test_recovery as base  # noqa: E402
import test_recovery_failures as failures  # noqa: E402


harness = base.harness


@dataclass(slots=True)
class _R2StartupSetup:
    transport: FileBridgeTransport
    command: ExchangeCommand
    peer: Any
    rig: Any
    trace: list[str]
    r2: PendingJournal
    cancel_intent: DeliveryIntent
    cancel_delivery: PreparedDelivery
    cancel_bytes: bytes
    manager: RecoveryManager


def _prepared_from_intent(intent: DeliveryIntent) -> PreparedDelivery:
    return PreparedDelivery(
        request_id=intent.request_id,
        delivery_id=intent.delivery_id,
        delivery_kind=intent.delivery_kind,
        request_hash=intent.request_hash,
        body_json=intent.body_json.encode("utf-8", errors="strict"),
    )


def _manager_for(
    harness: base.Harness,
    transport: FileBridgeTransport,
) -> RecoveryManager:
    return RecoveryManager(
        paths=transport.paths,
        root_lock=harness.lock,
        process_inspector=harness.inspector,
        expected_process=harness.process,
        journals=transport.journals,
        ledger=transport.ledger,
        inbox=transport.inbox,
        response_poll_seconds=0.001,
        cancel_ack_timeout_seconds=base.CANCEL_ACK_TIMEOUT_SECONDS,
    )


async def _prepare_exact_r2(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> _R2StartupSetup:
    transport = base._transport(harness)  # pyright: ignore[reportPrivateUsage]
    command = base._command(harness)  # pyright: ignore[reportPrivateUsage]
    trace: list[str] = []
    peer = base._peer(harness, trace)  # pyright: ignore[reportPrivateUsage]
    peer.enqueue(base.StaySilent())
    rig = base._NoAckRig(trace=trace)  # pyright: ignore[reportPrivateUsage]
    base._install_no_ack_rig(  # pyright: ignore[reportPrivateUsage]
        monkeypatch,
        transport=transport,
        peer=peer,
        command=command,
        rig=rig,
    )

    installed_insert = transport.ledger.insert_delivery
    insert_attempts: list[DeliveryIntent] = []
    preparation_fault = RuntimeError("stop live recovery after durable r2")

    def stop_cancel_insert(intent: DeliveryIntent) -> None:
        if intent.delivery_kind == "request":
            installed_insert(intent)
            return
        insert_attempts.append(intent)
        raise preparation_fault

    monkeypatch.setattr(transport.ledger, "insert_delivery", stop_cancel_insert)
    caught, r2, _channel = await failures._run_no_ack_cancellation(  # pyright: ignore[reportPrivateUsage]
        harness=harness,
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        trace=trace,
        message="prepare durable r2 for startup recovery",
    )
    monkeypatch.setattr(transport.ledger, "insert_delivery", installed_insert)

    assert r2.original.revision == 2
    assert r2.original.state is PendingPhase.CANCEL_PUBLISHED
    assert r2.original.response_artifact is None
    assert r2.original.settlement is None
    assert len(r2.original.delivery_intents) == 2
    request_intent, cancel_intent = r2.original.delivery_intents
    assert request_intent.delivery_kind == "request"
    assert request_intent.published_at_ms is not None
    assert cancel_intent.delivery_kind == "cancel"
    assert cancel_intent.published_at_ms is None
    assert cancel_intent.original_request_delivery_id == request_intent.delivery_id
    assert insert_attempts == [cancel_intent, cancel_intent]
    assert transport.ledger.get_delivery(cancel_intent.delivery_id) is None

    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.PUBLISHED
    assert request.terminal_at_ms is None
    async with harness.lock:
        loaded = transport.journals.load()
        assert loaded is not None and loaded.journal == r2
    assert rig.cancel_expectation is None
    assert rig.cancel_deliveries == []
    assert any(str(preparation_fault) in note for note in getattr(caught, "__notes__", ()))

    cancel_delivery = _prepared_from_intent(cancel_intent)
    cancel_bytes = render_delivery_lua(cancel_delivery, cancel_intent.runtime_snapshot)
    assert len(cancel_bytes) == cancel_intent.rendered_inbox_size_bytes

    return _R2StartupSetup(
        transport=transport,
        command=command,
        peer=peer,
        rig=rig,
        trace=trace,
        r2=r2,
        cancel_intent=cancel_intent,
        cancel_delivery=cancel_delivery,
        cancel_bytes=cancel_bytes,
        manager=_manager_for(harness, transport),
    )


def _assert_r2_resumed_through_dual_wait(setup: _R2StartupSetup) -> None:
    loaded = setup.transport.journals.load()
    assert loaded is not None
    final = loaded.journal
    assert final.original.revision == 4
    assert final.original.state is PendingPhase.QUARANTINED

    journals = {journal.original.revision: journal for journal in setup.rig.journals}
    assert set(journals) == {0, 1, 2, 3, 4}
    assert journals[2] == setup.r2
    r3 = journals[3]
    assert r3.original.state is PendingPhase.CANCEL_PUBLISHED
    r3_cancel = next(
        intent for intent in r3.original.delivery_intents if intent.delivery_kind == "cancel"
    )
    assert r3_cancel.delivery_id == setup.cancel_intent.delivery_id
    assert r3_cancel.model_copy(update={"published_at_ms": None}) == setup.cancel_intent
    assert r3_cancel.published_at_ms is not None
    assert final.original.delivery_intents == r3.original.delivery_intents

    inserted_cancel = [
        intent for intent in setup.rig.inserted_intents if intent.delivery_kind == "cancel"
    ]
    assert inserted_cancel == [setup.cancel_intent]
    marked_cancel = [
        record for record in setup.rig.marked_deliveries if record.delivery_kind == "cancel"
    ]
    assert len(marked_cancel) == 1
    assert marked_cancel[0].delivery_id == setup.cancel_intent.delivery_id
    assert marked_cancel[0].published_at_ms == r3_cancel.published_at_ms

    expectation = setup.rig.cancel_expectation
    assert expectation is not None
    assert expectation.allowed_deliveries[1].delivery_id == setup.cancel_intent.delivery_id
    assert expectation.allowed_deliveries[1].delivery_kind == "cancel"
    request = setup.transport.ledger.get_request(setup.command.request_id)
    assert request is not None
    assert request.state is HostRequestState.QUARANTINED
    assert request.terminal_at_ms is None
    assert setup.rig.cancel_wait_timeouts == [base.CANCEL_ACK_TIMEOUT_SECONDS]
    base._assert_order(  # pyright: ignore[reportPrivateUsage]
        setup.trace,
        "journal.r2_cancel_intent",
        "ledger.delivery.cancel.inserted",
        "journal.r3_cancel_marker",
        "ledger.delivery.cancel.marked",
        "ledger.request.cancel_published",
        "wait.cancel.started",
        "wait.cancel.timeout",
        "journal.r4_quarantined",
        "ledger.request.quarantined",
    )


@pytest.mark.asyncio
async def test_recover_pending_resumes_r2_with_durable_cancel_uuid_before_dual_wait(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = await _prepare_exact_r2(harness, monkeypatch)

    async with harness.lock:
        await setup.manager.recover_pending()  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
        _assert_r2_resumed_through_dual_wait(setup)

    assert len(setup.rig.cancel_deliveries) == 1
    assert setup.rig.cancel_deliveries[0] == setup.cancel_delivery
    base._assert_order(  # pyright: ignore[reportPrivateUsage]
        setup.trace,
        "ledger.delivery.cancel.inserted",
        "inbox.cancel",
        "journal.r3_cancel_marker",
        "wait.cancel.started",
    )


@pytest.mark.asyncio
async def test_recover_pending_treats_exact_cancel_inbox_as_publish_aftercommit(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = await _prepare_exact_r2(harness, monkeypatch)
    setup.transport.paths.inbox.write_bytes(setup.cancel_bytes)
    publish_attempts: list[PreparedDelivery] = []

    def forbid_duplicate_publish(
        _publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> Never:
        assert runtime_snapshot == setup.command.runtime_snapshot
        publish_attempts.append(delivery)
        raise AssertionError("exact durable cancel bytes must not be republished")

    monkeypatch.setattr(InboxPublisher, "publish_delivery", forbid_duplicate_publish)

    async with harness.lock:
        await setup.manager.recover_pending()  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
        _assert_r2_resumed_through_dual_wait(setup)

    assert publish_attempts == []
    assert setup.rig.cancel_deliveries == []
    assert setup.transport.paths.inbox.read_bytes() == setup.cancel_bytes
    base._assert_order(  # pyright: ignore[reportPrivateUsage]
        setup.trace,
        "ledger.delivery.cancel.inserted",
        "journal.r3_cancel_marker",
        "ledger.delivery.cancel.marked",
        "wait.cancel.started",
    )


@pytest.mark.asyncio
async def test_recover_pending_quarantines_r2_without_touching_foreign_inbox(
    harness: base.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = await _prepare_exact_r2(harness, monkeypatch)
    foreign_bytes = b"foreign startup inbox bytes\n"
    setup.transport.paths.inbox.write_bytes(foreign_bytes)
    publish_attempts: list[PreparedDelivery] = []
    wait_attempts: list[float] = []

    def forbid_publish(
        _publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> Never:
        assert runtime_snapshot == setup.command.runtime_snapshot
        publish_attempts.append(delivery)
        raise AssertionError("startup recovery overwrote a foreign inbox")

    async def forbid_wait(
        _waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        wait_attempts.append(timeout_seconds)
        raise AssertionError("startup recovery waited without publishing its cancel")

    monkeypatch.setattr(InboxPublisher, "publish_delivery", forbid_publish)
    monkeypatch.setattr(ResponseWaiter, "wait", forbid_wait)
    production_save = type(setup.transport.journals).save.__get__(setup.transport.journals)
    monkeypatch.setattr(setup.transport.journals, "save", production_save)

    loaded = None
    request = None
    async with harness.lock:
        await setup.manager.recover_pending()  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
        loaded = setup.transport.journals.load()
        request = setup.transport.ledger.get_request(setup.command.request_id)

    assert loaded is not None
    quarantined = loaded.journal
    assert quarantined.original.revision == 3
    assert quarantined.original.state is PendingPhase.QUARANTINED
    assert quarantined.original.delivery_intents == setup.r2.original.delivery_intents
    assert quarantined.original.response_artifact is None
    assert quarantined.original.settlement is None
    assert request is not None
    assert request.state is HostRequestState.QUARANTINED
    assert request.terminal_at_ms is None
    assert request.result_json is None
    assert request.error_json is not None
    assert json.loads(request.error_json)["code"] == ErrorCode.INDETERMINATE_OUTCOME.value
    assert setup.transport.paths.inbox.read_bytes() == foreign_bytes
    assert publish_attempts == []
    assert wait_attempts == []

    async with setup.transport.session() as channel:
        with pytest.raises(BridgeError) as blocked:
            await base._bounded(  # pyright: ignore[reportPrivateUsage]
                channel.exchange(setup.command),
                label="startup quarantine barrier",
            )
    assert blocked.value.code is ErrorCode.MUTATION_QUARANTINED
    assert blocked.value.details["request_id"] == str(setup.command.request_id)
    assert blocked.value.details["state"] == PendingPhase.QUARANTINED.value
    assert setup.transport.paths.inbox.read_bytes() == foreign_bytes
