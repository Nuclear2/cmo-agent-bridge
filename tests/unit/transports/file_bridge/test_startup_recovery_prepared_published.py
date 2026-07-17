from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
import sys
from typing import Any, Literal, cast
from uuid import UUID

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua, render_idle_lua
from cmo_agent_bridge.protocol.models import PreparedDelivery
from cmo_agent_bridge.state.models import HostRequestState, PendingJournal, PendingPhase
from cmo_agent_bridge.state.request_ledger import (
    DeliveryRecord,
    RequestRecord,
)
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.recovery import RecoveryManager
from cmo_agent_bridge.transports.file_bridge.response_waiter import ResponseWaiter

sys.path.insert(0, str(Path(__file__).parent))

import test_recovery_contract as contract  # noqa: E402


harness = contract.harness

_InboxCase = Literal["foreign", "idle", "missing"]


def _journals(harness: contract.Harness) -> tuple[PendingJournal, PendingJournal]:
    r0, r1, _r2, _response = contract._precondition_journals(  # pyright: ignore[reportPrivateUsage]
        harness
    )
    return r0, r1


def _persist(
    harness: contract.Harness,
    *journals: PendingJournal,
) -> None:
    contract._persist_journal_sequence(  # pyright: ignore[reportPrivateUsage]
        harness,
        journals,
    )


def _manager(harness: contract.Harness) -> RecoveryManager:
    return contract._manager(  # pyright: ignore[reportPrivateUsage]
        harness,
        response_poll_seconds=0.001,
        cancel_ack_timeout_seconds=0.001,
    )


def _request_from_journal(journal: PendingJournal) -> RequestRecord:
    exchange = journal.original
    return RequestRecord(
        request_id=exchange.request_id,
        root_key=journal.header.root_key,
        request_hash=exchange.request_hash,
        operation=exchange.operation,
        operation_class=exchange.effective_class,
        state=HostRequestState.PREPARED,
        runtime_snapshot=exchange.runtime_snapshot,
        result_schema_id=exchange.result_schema_id,
        recovery_schema_id=exchange.recovery_schema_id,
        body_json=exchange.body_json.encode("utf-8", errors="strict"),
        lineage_id=exchange.expected_lineage_id,
        activation_id=exchange.expected_activation_id,
        result_json=None,
        error_json=None,
        resolution_json=None,
        created_at_ms=exchange.created_at_ms,
        updated_at_ms=exchange.created_at_ms,
        terminal_at_ms=None,
    )


def _delivery_from_journal(journal: PendingJournal) -> DeliveryRecord:
    intent = journal.original.delivery_intents[0]
    return DeliveryRecord(
        delivery_id=intent.delivery_id,
        request_id=intent.request_id,
        delivery_kind=intent.delivery_kind,
        original_request_delivery_id=intent.original_request_delivery_id,
        intended_at_ms=intent.intended_at_ms,
        published_at_ms=intent.published_at_ms,
        rendered_inbox_sha256=intent.rendered_inbox_sha256,
        rendered_inbox_size_bytes=intent.rendered_inbox_size_bytes,
        response_filename=intent.response_filename,
        response_artifact=None,
        settlement=None,
    )


def _seed_prepared_sqlite(
    harness: contract.Harness,
    r0: PendingJournal,
) -> tuple[RequestRecord, DeliveryRecord]:
    request = _request_from_journal(r0)
    delivery = _delivery_from_journal(r0)
    harness.ledger.insert_prepared(request)
    harness.ledger.insert_delivery(r0.original.delivery_intents[0])
    assert harness.ledger.get_request(request.request_id) == request
    assert harness.ledger.get_delivery(delivery.delivery_id) == delivery
    return request, delivery


def _seed_published_sqlite(
    harness: contract.Harness,
    r0: PendingJournal,
    r1: PendingJournal,
) -> tuple[RequestRecord, DeliveryRecord]:
    prepared_request, prepared_delivery = _seed_prepared_sqlite(harness, r0)
    published_intent = r1.original.delivery_intents[0]
    published_at_ms = published_intent.published_at_ms
    assert published_at_ms is not None
    delivery = harness.ledger.mark_delivery_published(
        prepared_delivery.delivery_id,
        published_at_ms=published_at_ms,
    )
    request = harness.ledger.transition(
        prepared_request.request_id,
        expected_states=frozenset({HostRequestState.PREPARED}),
        new_state=HostRequestState.PUBLISHED,
        updated_at_ms=r1.original.updated_at_ms,
    )
    assert delivery == _delivery_from_journal(r1)
    assert request.state is HostRequestState.PUBLISHED
    return request, delivery


def _render_request(journal: PendingJournal) -> bytes:
    intent = journal.original.delivery_intents[0]
    delivery = PreparedDelivery(
        request_id=intent.request_id,
        delivery_id=intent.delivery_id,
        delivery_kind="request",
        request_hash=intent.request_hash,
        body_json=intent.body_json.encode("utf-8", errors="strict"),
    )
    rendered = render_delivery_lua(delivery, intent.runtime_snapshot)
    assert len(rendered) == intent.rendered_inbox_size_bytes
    return rendered


def _write_inbox(harness: contract.Harness, raw: bytes) -> None:
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.inbox.write_bytes(raw)


async def _recover(manager: RecoveryManager) -> Any:
    method = getattr(manager, "recover_pending", None)
    assert callable(method), "RecoveryManager.recover_pending() is missing"
    recover = cast(Callable[[], Awaitable[Any]], method)
    return await recover()


def _disposition(report: object) -> str:
    raw_disposition = getattr(report, "disposition", None)
    assert raw_disposition is not None
    disposition = getattr(raw_disposition, "value", raw_disposition)
    assert type(disposition) is str
    return disposition


def _install_immediate_cancel_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    async def timeout(_waiter: ResponseWaiter, timeout_seconds: float) -> Any:
        raise BridgeError(
            ErrorCode.REQUEST_TIMEOUT,
            "startup recovery test cancel timeout",
            {"timeout_seconds": timeout_seconds},
        )

    monkeypatch.setattr(ResponseWaiter, "wait", timeout)


def _assert_before(trace: list[str], before: str, after: str) -> None:
    assert before in trace, trace
    assert after in trace, trace
    assert trace.index(before) < trace.index(after), trace


@pytest.mark.asyncio
async def test_startup_without_pending_journal_reports_no_pending_without_side_effects(
    harness: contract.Harness,
) -> None:
    manager = _manager(harness)
    response_path = harness.paths.response_path(contract.REQUEST_ID)
    before = {
        "pending": harness.paths.pending_file.exists(),
        "sqlite": harness.paths.sqlite_file.exists(),
        "inbox": harness.paths.inbox.exists(),
        "response": response_path.exists(),
        "process_checks": harness.inspector.calls,
    }

    async with harness.lock:
        report = await _recover(manager)
        assert _disposition(report) == "no_pending"
        assert getattr(report, "request_id", None) is None
        assert {
            "pending": harness.paths.pending_file.exists(),
            "sqlite": harness.paths.sqlite_file.exists(),
            "inbox": harness.paths.inbox.exists(),
            "response": response_path.exists(),
            "process_checks": harness.inspector.calls,
        } == before


@pytest.mark.asyncio
async def test_startup_prepared_idle_without_response_fails_before_publish_and_deletes_safely(
    harness: contract.Harness,
) -> None:
    r0, _r1 = _journals(harness)
    idle = render_idle_lua()

    async with harness.lock:
        _persist(harness, r0)
        prepared_request, prepared_delivery = _seed_prepared_sqlite(harness, r0)
        _write_inbox(harness, idle)

        report = await _recover(_manager(harness))

        assert harness.journals.load() is None
        request = harness.ledger.get_request(r0.original.request_id)
        delivery = harness.ledger.get_delivery(prepared_delivery.delivery_id)
        assert _disposition(report) == "failed_before_publish"
        assert getattr(report, "request_id", None) == r0.original.request_id
        assert request is not None
        assert request.state is HostRequestState.REJECTED
        assert request.terminal_at_ms is not None
        assert request.error_json is not None
        assert request.created_at_ms == prepared_request.created_at_ms
        assert delivery == prepared_delivery
        assert harness.paths.inbox.read_bytes() == idle
        assert not harness.paths.response_path(r0.original.request_id).exists()


@pytest.mark.asyncio
async def test_startup_prepared_matching_request_promotes_published_before_cancel(
    harness: contract.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    r0, _r1 = _journals(harness)
    trace: list[str] = []
    published_kinds: list[str] = []

    async with harness.lock:
        _persist(harness, r0)
        _seed_prepared_sqlite(harness, r0)
        _write_inbox(harness, _render_request(r0))

        installed_save = harness.journals.save

        def traced_save(
            journal: PendingJournal,
            *,
            expected_revisions: object,
        ) -> object:
            result = installed_save(
                journal,
                expected_revisions=cast(Any, expected_revisions),
            )
            trace.append(f"journal.r{journal.original.revision}.{journal.original.state.value}")
            return result

        installed_publish = InboxPublisher.publish_delivery

        def traced_publish(
            publisher: InboxPublisher,
            delivery: PreparedDelivery,
            *,
            runtime_snapshot: object,
        ) -> None:
            if publisher is harness.inbox:
                published_kinds.append(delivery.delivery_kind)
                trace.append(f"inbox.{delivery.delivery_kind}")
            installed_publish(
                publisher,
                delivery,
                runtime_snapshot=cast(Any, runtime_snapshot),
            )

        monkeypatch.setattr(harness.journals, "save", traced_save)
        monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish)
        _install_immediate_cancel_timeout(monkeypatch)

        report = await _recover(_manager(harness))

        loaded = harness.journals.load()
        assert _disposition(report) == "quarantined"
        assert loaded is not None
        assert loaded.journal.original.state is PendingPhase.QUARANTINED

    assert trace[0].startswith("journal.r1.published"), trace
    _assert_before(trace, trace[0], "inbox.cancel")
    assert published_kinds == ["cancel"]


@pytest.mark.asyncio
async def test_startup_published_sqlite_lag_converges_before_cancel_intent_and_publication(
    harness: contract.Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    r0, r1 = _journals(harness)
    trace: list[str] = []

    async with harness.lock:
        _persist(harness, r0, r1)
        _seed_prepared_sqlite(harness, r0)
        _write_inbox(harness, _render_request(r1))

        installed_mark = harness.ledger.mark_delivery_published
        installed_transition = harness.ledger.transition
        installed_insert = harness.ledger.insert_delivery
        installed_save = harness.journals.save
        installed_publish = InboxPublisher.publish_delivery

        def traced_mark(delivery_id: UUID, *, published_at_ms: int) -> DeliveryRecord:
            result = installed_mark(delivery_id, published_at_ms=published_at_ms)
            kind = (
                "request"
                if delivery_id == r1.original.delivery_intents[0].delivery_id
                else "cancel"
            )
            trace.append(f"ledger.delivery.{kind}.published")
            return result

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
            trace.append(f"ledger.request.{new_state.value}")
            return result

        def traced_insert(intent: Any) -> None:
            installed_insert(intent)
            trace.append(f"ledger.delivery.{intent.delivery_kind}.inserted")

        def traced_save(
            journal: PendingJournal,
            *,
            expected_revisions: object,
        ) -> object:
            result = installed_save(
                journal,
                expected_revisions=cast(Any, expected_revisions),
            )
            if (
                journal.original.state is PendingPhase.CANCEL_PUBLISHED
                and journal.original.delivery_intents[-1].published_at_ms is None
            ):
                trace.append("journal.cancel_intent")
            return result

        def traced_publish(
            publisher: InboxPublisher,
            delivery: PreparedDelivery,
            *,
            runtime_snapshot: object,
        ) -> None:
            if publisher is harness.inbox:
                trace.append(f"inbox.{delivery.delivery_kind}")
            installed_publish(
                publisher,
                delivery,
                runtime_snapshot=cast(Any, runtime_snapshot),
            )

        monkeypatch.setattr(harness.ledger, "mark_delivery_published", traced_mark)
        monkeypatch.setattr(harness.ledger, "transition", traced_transition)
        monkeypatch.setattr(harness.ledger, "insert_delivery", traced_insert)
        monkeypatch.setattr(harness.journals, "save", traced_save)
        monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish)
        _install_immediate_cancel_timeout(monkeypatch)

        report = await _recover(_manager(harness))
        assert _disposition(report) == "quarantined"

    _assert_before(
        trace,
        "ledger.delivery.request.published",
        "ledger.request.published",
    )
    _assert_before(trace, "ledger.request.published", "journal.cancel_intent")
    _assert_before(
        trace,
        "journal.cancel_intent",
        "ledger.delivery.cancel.inserted",
    )
    _assert_before(trace, "ledger.delivery.cancel.inserted", "inbox.cancel")
    assert "inbox.request" not in trace


@pytest.mark.parametrize("inbox_case", ["foreign", "idle", "missing"])
@pytest.mark.asyncio
async def test_startup_published_without_owned_inbox_never_republishes_mutation_and_fails_closed(
    harness: contract.Harness,
    monkeypatch: pytest.MonkeyPatch,
    inbox_case: _InboxCase,
) -> None:
    r0, r1 = _journals(harness)
    published_kinds: list[str] = []
    foreign = b"foreign startup inbox bytes"
    initial: bytes | None = None

    async with harness.lock:
        _persist(harness, r0, r1)
        _seed_published_sqlite(harness, r0, r1)
        if inbox_case == "foreign":
            initial = foreign
            _write_inbox(harness, initial)
        elif inbox_case == "idle":
            initial = render_idle_lua()
            _write_inbox(harness, initial)
        else:
            initial = None
            assert not harness.paths.inbox.exists()

        installed_publish = InboxPublisher.publish_delivery

        def traced_publish(
            publisher: InboxPublisher,
            delivery: PreparedDelivery,
            *,
            runtime_snapshot: object,
        ) -> None:
            if publisher is harness.inbox:
                published_kinds.append(delivery.delivery_kind)
            installed_publish(
                publisher,
                delivery,
                runtime_snapshot=cast(Any, runtime_snapshot),
            )

        monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish)
        _install_immediate_cancel_timeout(monkeypatch)

        report = await _recover(_manager(harness))

        loaded = harness.journals.load()
        request = harness.ledger.get_request(r1.original.request_id)
        assert _disposition(report) == "quarantined"
        assert "request" not in published_kinds
        assert loaded is not None
        assert loaded.journal.original.state is PendingPhase.QUARANTINED
        assert request is not None
        assert request.state is HostRequestState.QUARANTINED
        if initial is not None:
            assert harness.paths.inbox.read_bytes() == initial
