"""Submission and observation API for the durable CMO operation queue."""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Mapping
from typing import Callable, Protocol, cast
from uuid import UUID, uuid4

from pydantic import BaseModel, JsonValue

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import ExecutionTarget, OperationClass
from cmo_agent_bridge.operations.registry import OperationRegistry, ResolvedInvocation
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.models import RequestBody
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, Sha256, revalidate_runtime_snapshot
from cmo_agent_bridge.state.session_store import SessionRecord

from .queue_models import (
    CancelQueuedOperationResult,
    QueueClock,
    QueueStore,
    QueueSummary,
    QueueWaitResult,
    QueuedOperationList,
    QueuedOperationReceipt,
    QueuedOperationRecord,
    QueuedOperationState,
    QueuedOperationStatus,
    canonical_queue_json,
    validate_wait_timeout,
)


class SessionBindingPort(Protocol):
    def load(self, root_key: str) -> SessionRecord | None: ...


def _invalid(message: str, details: dict[str, JsonValue] | None = None) -> BridgeError:
    return BridgeError(ErrorCode.INVALID_ARGUMENT, message, details)


def _json_model(value: BaseModel) -> dict[str, JsonValue]:
    dumped = value.model_dump(mode="json", warnings="error")
    encoded = json.dumps(
        dumped, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )
    parsed = json.loads(encoded)
    if not isinstance(parsed, dict):
        raise TypeError("operation arguments must be an object")
    return cast(dict[str, JsonValue], parsed)


class _SystemClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


class QueueService:
    """Validates and persists CMO mutations without waiting for game time.

    A previously established ``SessionStore`` binding is intentionally required:
    an order submitted while CMO is paused must not later execute against a
    different scenario just because it was resumed first.
    """

    def __init__(
        self,
        *,
        root_key: Sha256,
        registry: OperationRegistry,
        runtime_snapshot: RuntimeSnapshot,
        allow_mutations: bool,
        session_store: SessionBindingPort,
        queue_store: QueueStore,
        clock: QueueClock | None = None,
        new_uuid: Callable[[], UUID] = uuid4,
        poll_interval_seconds: float = 0.1,
    ) -> None:
        if type(root_key) is not str:
            raise TypeError("queue root key must be an exact string")
        if type(registry) is not OperationRegistry:
            raise TypeError("operation registry must be exact")
        if type(allow_mutations) is not bool:
            raise TypeError("allow_mutations must be an exact bool")
        if not math.isfinite(poll_interval_seconds) or poll_interval_seconds <= 0:
            raise ValueError("queue poll interval must be finite and positive")
        self._root_key = root_key
        self._registry = registry
        self._runtime_snapshot = revalidate_runtime_snapshot(runtime_snapshot)
        self._allow_mutations = allow_mutations
        self._session_store = session_store
        self._queue_store = queue_store
        self._clock = _SystemClock() if clock is None else clock
        self._new_uuid = new_uuid
        self._poll_interval_seconds = poll_interval_seconds

    def submit(self, *, operation: str, arguments: Mapping[str, object]) -> QueuedOperationReceipt:
        if not self._allow_mutations:
            raise BridgeError(
                ErrorCode.POLICY_DENIED,
                "CMO mutation queue is disabled by bridge configuration",
                {"setting": "allow_mutations"},
            )
        if type(operation) is not str or not operation:
            raise _invalid("operation must be a non-empty string")
        raw_arguments = cast(object, arguments)
        if not isinstance(raw_arguments, Mapping):
            raise _invalid("operation arguments must be an object", {"operation": operation})
        invocation = self._registry.resolve_invocation(operation, arguments)
        self._require_queueable(invocation)
        session = self._session_store.load(self._root_key)
        if session is None:
            raise BridgeError(
                ErrorCode.ACTIVATION_REQUIRED,
                "queue submission requires an established CMO session binding",
                {"operation": operation, "next_step": "cmo_bridge_status"},
            )
        try:
            snapshot = session.runtime_snapshot
            lineage_id = session.scenario_lineage_id
            activation_id = session.activation_id
            process_pid = session.process_pid
            process_create_time = session.process_create_time
        except AttributeError as error:
            raise TypeError("session store returned an invalid session binding") from error
        if snapshot != self._runtime_snapshot:
            raise BridgeError(
                ErrorCode.MANIFEST_MISMATCH,
                "established CMO session does not match the currently running bridge release",
                {"operation": operation, "next_step": "cmo_bridge_status"},
            )
        snapshot = self._runtime_snapshot
        body = RequestBody(
            protocol=snapshot.protocol,
            release_id=snapshot.release_id,
            runtime_version=snapshot.runtime_version,
            runtime_tag=snapshot.runtime_tag,
            runtime_asset_sha256=snapshot.runtime_asset_sha256,
            expected_lineage_id=lineage_id,
            expected_activation_id=activation_id,
            operation_manifest_sha256=snapshot.operation_manifest_sha256,
            operation=invocation.contract.name,
            arguments=_json_model(invocation.wire_arguments),
        )
        submitted_at_ms = self._clock.now_ms()
        record = QueuedOperationRecord(
            request_id=self._new_uuid(),
            root_key=self._root_key,
            queue_sequence=0,  # assigned atomically by the durable store
            operation=invocation.contract.name,
            arguments_json=canonical_queue_json(_json_model(invocation.public_arguments)),
            body_json=canonical_body_bytes(body),
            runtime_snapshot=snapshot,
            result_schema_id=invocation.result_schema.schema_id,
            recovery_schema_id=(
                None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
            ),
            expected_lineage_id=lineage_id,
            expected_activation_id=activation_id,
            expected_process_pid=process_pid,
            expected_process_create_time=process_create_time,
            state=QueuedOperationState.QUEUED,
            result_json=None,
            error_json=None,
            created_at_ms=submitted_at_ms,
            updated_at_ms=submitted_at_ms,
            terminal_at_ms=None,
        )
        persisted = self._queue_store.enqueue(record)
        return QueuedOperationReceipt(
            request_id=persisted.request_id,
            operation=persisted.operation,
            sequence=persisted.queue_sequence,
            state=persisted.state,
            submitted_at_ms=persisted.created_at_ms,
        )

    def get(self, *, request_id: UUID) -> QueuedOperationStatus:
        record = self._queue_store.get(request_id)
        if record is None or record.root_key != self._root_key:
            raise BridgeError(
                ErrorCode.NOT_FOUND,
                "queued operation was not found",
                {"request_id": str(request_id)},
            )
        return QueuedOperationStatus.from_record(record)

    def list(self, *, limit: int | None = None) -> QueuedOperationList:
        if limit is not None and (type(limit) is not int or limit < 1):
            raise _invalid("queue list limit must be a positive integer")
        records = self._queue_store.list(root_key=self._root_key)
        if limit is not None:
            records = records[:limit]
        return QueuedOperationList(
            items=tuple(QueuedOperationStatus.from_record(item) for item in records)
        )

    def summary(self) -> QueueSummary:
        return QueueSummary.from_counts(self._queue_store.counts(root_key=self._root_key))

    async def wait(self, *, request_id: UUID, timeout_seconds: float) -> QueueWaitResult:
        timeout = validate_wait_timeout(timeout_seconds)
        deadline = time.monotonic() + timeout
        while True:
            status = self.get(request_id=request_id)
            if status.state in {
                QueuedOperationState.COMPLETED,
                QueuedOperationState.REJECTED,
                QueuedOperationState.QUARANTINED,
                QueuedOperationState.CANCELLED,
            }:
                return QueueWaitResult(operation=status, timed_out=False)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return QueueWaitResult(operation=status, timed_out=True)
            await asyncio.sleep(min(remaining, self._poll_interval_seconds))

    def cancel(self, *, request_id: UUID) -> CancelQueuedOperationResult:
        existing = self._queue_store.get(request_id)
        if existing is None or existing.root_key != self._root_key:
            raise BridgeError(
                ErrorCode.NOT_FOUND,
                "queued operation was not found",
                {"request_id": str(request_id)},
            )
        record = self._queue_store.cancel_queued(request_id, at_ms=self._clock.now_ms())
        if record is None:
            record = self._queue_store.get(request_id)
            if record is None or record.root_key != self._root_key:
                raise BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "queued operation changed while cancellation was attempted",
                    {"request_id": str(request_id)},
                )
        status = QueuedOperationStatus.from_record(record)
        return CancelQueuedOperationResult(
            operation=status,
            cancelled=status.state is QueuedOperationState.CANCELLED,
        )

    @staticmethod
    def _require_queueable(invocation: ResolvedInvocation) -> None:
        contract = invocation.contract
        if (
            contract.target is not ExecutionTarget.CMO
            or invocation.effective_class is not OperationClass.MUTATION
        ):
            raise BridgeError(
                ErrorCode.POLICY_DENIED,
                "the durable queue accepts only concrete CMO mutation operations",
                {
                    "operation": contract.name,
                    "target": contract.target.value,
                    "operation_class": invocation.effective_class.value,
                },
            )
