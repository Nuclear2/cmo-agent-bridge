from __future__ import annotations

from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

import pytest

from cmo_agent_bridge.application.queue_models import (
    QueueError,
    QueuedOperationState,
    canonical_queue_json,
)
from cmo_agent_bridge.application.queue_service import QueueService
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.canonical import request_sha256
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.models import RequestBody
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.models import HostRequestState
from cmo_agent_bridge.state.host_resolution import (
    HostQuarantineResolutionMarker,
    canonical_host_quarantine_resolution,
)
from cmo_agent_bridge.state.operation_queue import OperationQueueStore
from cmo_agent_bridge.state.request_ledger import RequestLedger, RequestRecord
from cmo_agent_bridge.state.session_store import SessionRecord, SessionStore
from cmo_agent_bridge.state.sqlite import StateDatabase


ROOT_KEY = "a" * 64


class _Clock:
    def __init__(self) -> None:
        self.value = 1_000

    def now_ms(self) -> int:
        self.value += 1
        return self.value


def _snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.2.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=OPERATION_REGISTRY.manifest_sha256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )


def _ledger(database: StateDatabase) -> RequestLedger:
    snapshot = _snapshot()
    return RequestLedger(
        database,
        ManifestCatalog(ReleaseBinding(snapshot=snapshot, registry=OPERATION_REGISTRY)),
    )


def _service(tmp_path: Path, *, bound: bool = True, allow_mutations: bool = True) -> QueueService:
    database = StateDatabase(tmp_path / "state.sqlite3")
    sessions = SessionStore(database)
    if bound:
        sessions.replace(
            SessionRecord(
                root_key=ROOT_KEY,
                scenario_lineage_id=uuid4(),
                activation_id=uuid4(),
                build_number=1868,
                runtime_snapshot=_snapshot(),
                process_pid=42,
                process_create_time=1.0,
                validated_at_ms=1,
            )
        )
    return QueueService(
        root_key=ROOT_KEY,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=_snapshot(),
        allow_mutations=allow_mutations,
        session_store=sessions,
        queue_store=OperationQueueStore(database),
        ledger=_ledger(database),
        clock=_Clock(),
    )


def _insert_prepared_ledger(service: QueueService, request_id: UUID) -> RequestRecord:
    store = service._queue_store  # pyright: ignore[reportPrivateUsage]
    queued = store.get(request_id)
    assert queued is not None
    body = RequestBody.model_validate_json(queued.body_json)
    invocation = OPERATION_REGISTRY.resolve_invocation(
        queued.operation,
        {"code": 3},
    )
    prepared = RequestRecord(
        request_id=queued.request_id,
        root_key=queued.root_key,
        request_hash=request_sha256(body),
        operation=queued.operation,
        operation_class=invocation.effective_class,
        state=HostRequestState.PREPARED,
        runtime_snapshot=queued.runtime_snapshot,
        result_schema_id=queued.result_schema_id,
        recovery_schema_id=queued.recovery_schema_id,
        body_json=queued.body_json,
        lineage_id=queued.expected_lineage_id,
        activation_id=queued.expected_activation_id,
        result_json=None,
        error_json=None,
        resolution_json=None,
        created_at_ms=1_100,
        updated_at_ms=1_100,
        terminal_at_ms=None,
    )
    service._ledger.insert_prepared(prepared)  # pyright: ignore[reportPrivateUsage]
    return prepared


def _error_bytes(code: ErrorCode) -> bytes:
    return canonical_queue_json(
        QueueError(
            code=code,
            message=f"test {code.value}",
            details={"source": "test"},
        ).model_dump(mode="json")
    )


def _resolution_json(
    service: QueueService,
    request_id: UUID,
    *,
    disposition: Literal["applied", "not_applied"],
    resolved_at_ms: int,
) -> str:
    ledger_record = service._ledger.get_request(request_id)  # pyright: ignore[reportPrivateUsage]
    assert ledger_record is not None
    assert ledger_record.lineage_id is not None
    assert ledger_record.activation_id is not None
    return canonical_host_quarantine_resolution(
        HostQuarantineResolutionMarker(
            format="cmo-agent-bridge/host-quarantine-resolution/1",
            mode="host_only",
            manual_evidence=True,
            root_key=ledger_record.root_key,
            required_release_id=ledger_record.runtime_snapshot.release_id,
            request_id=ledger_record.request_id,
            request_hash=ledger_record.request_hash,
            original_journal_revision=4,
            scenario_lineage_id=ledger_record.lineage_id,
            original_activation_id=ledger_record.activation_id,
            disposition=disposition,
            resolved_at_ms=resolved_at_ms,
        )
    )


def test_submit_persists_bound_concrete_mutation_in_fifo_queue(tmp_path: Path) -> None:
    service = _service(tmp_path)

    receipt = service.submit(operation="scenario.time_compression.set", arguments={"code": 3})

    assert receipt.state is QueuedOperationState.QUEUED
    assert receipt.sequence == 1
    status = service.get(request_id=receipt.request_id)
    assert status.operation == "scenario.time_compression.set"
    assert status.state is QueuedOperationState.QUEUED
    assert service.summary().queued == 1


def test_submit_requires_persisted_session_binding(tmp_path: Path) -> None:
    service = _service(tmp_path, bound=False)

    with pytest.raises(BridgeError) as raised:
        service.submit(operation="scenario.time_compression.set", arguments={"code": 3})

    assert raised.value.code is ErrorCode.ACTIVATION_REQUIRED


def test_submit_respects_mutation_configuration(tmp_path: Path) -> None:
    service = _service(tmp_path, allow_mutations=False)

    with pytest.raises(BridgeError) as raised:
        service.submit(operation="scenario.time_compression.set", arguments={"code": 3})

    assert raised.value.code is ErrorCode.POLICY_DENIED
    assert raised.value.details == {"setting": "allow_mutations"}


def test_submit_refuses_a_session_from_another_bridge_release(tmp_path: Path) -> None:
    database = StateDatabase(tmp_path / "state.sqlite3")
    sessions = SessionStore(database)
    sessions.replace(
        SessionRecord(
            root_key=ROOT_KEY,
            scenario_lineage_id=uuid4(),
            activation_id=uuid4(),
            build_number=1868,
            runtime_snapshot=RuntimeSnapshot.create(
                runtime_version="0.1.4",
                runtime_asset_sha256="e" * 64,
                operation_manifest_sha256=OPERATION_REGISTRY.manifest_sha256,
                host_contract_sha256="f" * 64,
                dependency_lock_sha256="1" * 64,
            ),
            process_pid=42,
            process_create_time=1.0,
            validated_at_ms=1,
        )
    )
    service = QueueService(
        root_key=ROOT_KEY,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=_snapshot(),
        allow_mutations=True,
        session_store=sessions,
        queue_store=OperationQueueStore(database),
        ledger=_ledger(database),
        clock=_Clock(),
    )

    with pytest.raises(BridgeError) as raised:
        service.submit(operation="scenario.time_compression.set", arguments={"code": 3})

    assert raised.value.code is ErrorCode.MANIFEST_MISMATCH


def test_queue_refuses_reads_and_cancels_only_unpublished_orders(tmp_path: Path) -> None:
    service = _service(tmp_path)
    with pytest.raises(BridgeError) as raised:
        service.submit(operation="scenario.get", arguments={})
    assert raised.value.code is ErrorCode.POLICY_DENIED

    receipt = service.submit(operation="scenario.time_compression.set", arguments={"code": 3})
    cancelled = service.cancel(request_id=receipt.request_id)
    assert cancelled.cancelled is True
    assert cancelled.operation.state is QueuedOperationState.CANCELLED


def test_cancel_cas_loss_returns_current_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)
    receipt = service.submit(
        operation="scenario.time_compression.set",
        arguments={"code": 3},
    )
    store = service._queue_store  # pyright: ignore[reportPrivateUsage]

    def claimed_first(*_args: object, **_kwargs: object) -> None:
        assert store.claim_next(root_key=ROOT_KEY, at_ms=2_000) is not None
        return None

    monkeypatch.setattr(store, "cancel_queued", claimed_first)

    result = service.cancel(request_id=receipt.request_id)

    assert result.cancelled is False
    assert result.operation.state is QueuedOperationState.ACTIVE


@pytest.mark.parametrize("error_code", tuple(ErrorCode))
def test_durable_queue_error_round_trips_every_error_code(
    tmp_path: Path,
    error_code: ErrorCode,
) -> None:
    service = _service(tmp_path)
    receipt = service.submit(
        operation="scenario.time_compression.set",
        arguments={"code": 3},
    )
    store = service._queue_store  # pyright: ignore[reportPrivateUsage]
    rejected = store.reject_queued(
        receipt.request_id,
        _error_bytes(error_code),
        at_ms=2_000,
    )
    assert rejected is not None

    status = service.get(request_id=receipt.request_id)

    assert status.error is not None
    assert status.error.code is error_code
    assert status.error.message == f"test {error_code.value}"
    assert status.error.details == {"source": "test"}


def test_terminal_error_history_is_readable_and_excluded_from_nonterminal_list(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    store = service._queue_store  # pyright: ignore[reportPrivateUsage]
    rejected = service.submit(
        operation="scenario.time_compression.set",
        arguments={"code": 3},
    )
    assert (
        store.reject_queued(
            rejected.request_id,
            _error_bytes(ErrorCode.INVALID_ARGUMENT),
            at_ms=2_000,
        )
        is not None
    )
    quarantined = service.submit(
        operation="scenario.time_compression.set",
        arguments={"code": 4},
    )
    assert store.claim_next(root_key=ROOT_KEY, at_ms=2_001) is not None
    assert (
        store.quarantine(
            quarantined.request_id,
            _error_bytes(ErrorCode.INDETERMINATE_OUTCOME),
            at_ms=2_002,
        )
        is not None
    )
    queued = service.submit(
        operation="scenario.time_compression.set",
        arguments={"code": 5},
    )

    history = service.list().items

    assert [item.state for item in history] == [
        QueuedOperationState.REJECTED,
        QueuedOperationState.QUARANTINED,
        QueuedOperationState.QUEUED,
    ]
    assert history[0].error is not None
    assert history[0].error.code is ErrorCode.INVALID_ARGUMENT
    assert history[1].error is not None
    assert history[1].error.code is ErrorCode.INDETERMINATE_OUTCOME
    assert history[1].quarantine_resolution is not None
    assert history[1].quarantine_resolution.state == "unresolved"
    assert history[1].quarantine_resolution.barrier_active is False
    assert [item.request_id for item in service.list_nonterminal().items] == [queued.request_id]


def test_submit_fails_fast_for_unresolved_quarantine_and_allows_resolved_history(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    store = service._queue_store  # pyright: ignore[reportPrivateUsage]
    first = service.submit(
        operation="scenario.time_compression.set",
        arguments={"code": 3},
    )
    _insert_prepared_ledger(service, first.request_id)
    assert store.claim_next(root_key=ROOT_KEY, at_ms=1_101) is not None
    queue_error = _error_bytes(ErrorCode.INDETERMINATE_OUTCOME)
    assert store.quarantine(first.request_id, queue_error, at_ms=1_102) is not None
    ledger_error = queue_error.decode("utf-8")
    service._ledger.transition(  # pyright: ignore[reportPrivateUsage]
        first.request_id,
        expected_states=frozenset({HostRequestState.PREPARED}),
        new_state=HostRequestState.QUARANTINED,
        updated_at_ms=1_102,
        error_json=ledger_error,
    )

    with pytest.raises(BridgeError) as blocked:
        service.submit(
            operation="scenario.time_compression.set",
            arguments={"code": 4},
        )

    assert blocked.value.code is ErrorCode.MUTATION_QUARANTINED
    assert blocked.value.details["quarantined_request_ids"] == [str(first.request_id)]
    assert "resolve-quarantine" in blocked.value.details["next_step"]
    blocked_status = service.get(request_id=first.request_id)
    assert blocked_status.quarantine_resolution is not None
    assert blocked_status.quarantine_resolution.state == "unresolved"
    assert blocked_status.quarantine_resolution.barrier_active is True
    blocked_summary = service.summary()
    assert blocked_summary.queued == 0
    assert blocked_summary.quarantined == 1
    assert blocked_summary.unresolved_quarantined == 1
    assert blocked_summary.resolved_quarantined == 0
    assert blocked_summary.barrier_active is True

    service._ledger.transition(  # pyright: ignore[reportPrivateUsage]
        first.request_id,
        expected_states=frozenset({HostRequestState.QUARANTINED}),
        new_state=HostRequestState.RESOLVED,
        updated_at_ms=1_103,
        terminal_at_ms=1_103,
        resolution_json=_resolution_json(
            service,
            first.request_id,
            disposition="not_applied",
            resolved_at_ms=1_103,
        ),
    )

    resolved_status = service.get(request_id=first.request_id)
    assert resolved_status.quarantine_resolution is not None
    assert resolved_status.quarantine_resolution.state == "resolved"
    assert resolved_status.quarantine_resolution.disposition == "not_applied"
    assert resolved_status.quarantine_resolution.resolved_at_ms == 1_103
    assert resolved_status.quarantine_resolution.barrier_active is False
    resolved_summary = service.summary()
    assert resolved_summary.quarantined == 1
    assert resolved_summary.unresolved_quarantined == 0
    assert resolved_summary.resolved_quarantined == 1
    assert resolved_summary.barrier_active is False

    accepted = service.submit(
        operation="scenario.time_compression.set",
        arguments={"code": 4},
    )
    assert accepted.state is QueuedOperationState.QUEUED


def test_host_only_quarantine_blocks_submission_and_activates_summary_barrier(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    store = service._queue_store  # pyright: ignore[reportPrivateUsage]
    receipt = service.submit(
        operation="scenario.time_compression.set",
        arguments={"code": 3},
    )
    _insert_prepared_ledger(service, receipt.request_id)
    queue_error = _error_bytes(ErrorCode.INDETERMINATE_OUTCOME)
    service._ledger.transition(  # pyright: ignore[reportPrivateUsage]
        receipt.request_id,
        expected_states=frozenset({HostRequestState.PREPARED}),
        new_state=HostRequestState.QUARANTINED,
        updated_at_ms=1_101,
        error_json=queue_error.decode("utf-8"),
    )
    database = StateDatabase(tmp_path / "state.sqlite3")
    with database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
        deleted = connection.execute(
            "DELETE FROM operation_queue WHERE request_id=?",
            (str(receipt.request_id),),
        )
        assert deleted.rowcount == 1
    assert store.get(receipt.request_id) is None

    with pytest.raises(BridgeError) as blocked:
        service.submit(
            operation="scenario.time_compression.set",
            arguments={"code": 4},
        )

    assert blocked.value.code is ErrorCode.MUTATION_QUARANTINED
    assert blocked.value.details["quarantined_request_ids"] == [str(receipt.request_id)]
    summary = service.summary()
    assert summary.quarantined == 0
    assert summary.unresolved_quarantined == 0
    assert summary.resolved_quarantined == 0
    assert summary.barrier_active is True


@pytest.mark.parametrize(
    ("terminal_state", "expected_disposition"),
    (
        (HostRequestState.COMPLETED, "applied"),
        (HostRequestState.REJECTED, "not_applied"),
        (HostRequestState.CANCELLED, "not_applied"),
    ),
)
def test_quarantined_queue_projection_uses_terminal_host_disposition(
    tmp_path: Path,
    terminal_state: HostRequestState,
    expected_disposition: Literal["applied", "not_applied"],
) -> None:
    service = _service(tmp_path)
    store = service._queue_store  # pyright: ignore[reportPrivateUsage]
    receipt = service.submit(
        operation="scenario.time_compression.set",
        arguments={"code": 3},
    )
    _insert_prepared_ledger(service, receipt.request_id)
    queue_error = _error_bytes(ErrorCode.INDETERMINATE_OUTCOME)
    assert store.claim_next(root_key=ROOT_KEY, at_ms=1_101) is not None
    assert store.quarantine(receipt.request_id, queue_error, at_ms=1_102) is not None

    if terminal_state is HostRequestState.COMPLETED:
        service._ledger.transition(  # pyright: ignore[reportPrivateUsage]
            receipt.request_id,
            expected_states=frozenset({HostRequestState.PREPARED}),
            new_state=terminal_state,
            updated_at_ms=1_103,
            terminal_at_ms=1_103,
            result_json='{"accepted":true,"code":3,"observed_time_compression":null}',
        )
    elif terminal_state is HostRequestState.REJECTED:
        service._ledger.transition(  # pyright: ignore[reportPrivateUsage]
            receipt.request_id,
            expected_states=frozenset({HostRequestState.PREPARED}),
            new_state=terminal_state,
            updated_at_ms=1_103,
            terminal_at_ms=1_103,
            error_json=queue_error.decode("utf-8"),
        )
    else:
        service._ledger.transition(  # pyright: ignore[reportPrivateUsage]
            receipt.request_id,
            expected_states=frozenset({HostRequestState.PREPARED}),
            new_state=terminal_state,
            updated_at_ms=1_103,
            terminal_at_ms=1_103,
        )

    status = service.get(request_id=receipt.request_id)

    assert status.state is QueuedOperationState.QUARANTINED
    assert status.quarantine_resolution is not None
    assert status.quarantine_resolution.state == "resolved"
    assert status.quarantine_resolution.disposition == expected_disposition
    assert status.quarantine_resolution.resolved_at_ms == 1_103
    assert status.quarantine_resolution.barrier_active is False
    summary = service.summary()
    assert summary.quarantined == 1
    assert summary.unresolved_quarantined == 0
    assert summary.resolved_quarantined == 1
    assert summary.barrier_active is False
