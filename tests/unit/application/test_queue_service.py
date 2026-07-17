from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from cmo_agent_bridge.application.queue_models import QueuedOperationState
from cmo_agent_bridge.application.queue_service import QueueService
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.operation_queue import OperationQueueStore
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
        clock=_Clock(),
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
