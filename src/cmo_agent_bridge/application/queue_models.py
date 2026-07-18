"""Public application models for durable CMO operation queues."""

from __future__ import annotations

import json
import math
from typing import Literal, Protocol, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictInt, StrictStr, model_validator
from typing_extensions import Self

from cmo_agent_bridge.errors import ErrorCode
from cmo_agent_bridge.state.operation_queue import (
    OperationQueueRecord as QueuedOperationRecord,
    OperationQueueState as QueuedOperationState,
    QueueCounts,
)


_TERMINAL_STATES = frozenset(
    {
        QueuedOperationState.COMPLETED,
        QueuedOperationState.REJECTED,
        QueuedOperationState.QUARANTINED,
        QueuedOperationState.CANCELLED,
    }
)


class QueueError(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    code: ErrorCode
    message: StrictStr
    details: dict[str, JsonValue] = Field(default_factory=dict)


class QueueQuarantineResolution(BaseModel):
    """Current Host disposition of a historically quarantined queue item."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    state: Literal["unresolved", "resolved"]
    disposition: Literal["applied", "not_applied"] | None = None
    resolved_at_ms: StrictInt | None = Field(default=None, ge=0)
    barrier_active: bool


def canonical_queue_json(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _load_json(value: bytes | None) -> JsonValue | None:
    return None if value is None else cast(JsonValue, json.loads(value))


def _body_operation(record: QueuedOperationRecord) -> str:
    decoded: object = json.loads(record.body_json)
    if type(decoded) is not dict:
        raise RuntimeError("durable queue record has invalid canonical request body")
    body = cast(dict[object, object], decoded)
    operation = body.get("operation")
    if type(operation) is not str:
        raise RuntimeError("durable queue record has invalid canonical request body")
    return operation


class QueuedOperationReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    request_id: UUID
    operation: StrictStr
    sequence: StrictInt = Field(ge=1)
    state: QueuedOperationState
    submitted_at_ms: StrictInt = Field(ge=0)

    @model_validator(mode="after")
    def _must_be_queued(self) -> Self:
        if self.state is not QueuedOperationState.QUEUED:
            raise ValueError("new queue receipt must be queued")
        return self


class QueuedOperationStatus(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    request_id: UUID
    operation: StrictStr
    sequence: StrictInt = Field(ge=1)
    state: QueuedOperationState
    submitted_at_ms: StrictInt = Field(ge=0)
    started_at_ms: StrictInt | None = Field(default=None, ge=0)
    completed_at_ms: StrictInt | None = Field(default=None, ge=0)
    result: JsonValue | None = None
    error: QueueError | None = None
    quarantine_resolution: QueueQuarantineResolution | None = None

    @classmethod
    def from_record(
        cls,
        record: QueuedOperationRecord,
        *,
        quarantine_resolution: QueueQuarantineResolution | None = None,
    ) -> Self:
        error = None
        if record.error_json is not None:
            # Durable queue errors are JSON, so validate them in JSON mode. In
            # strict Python mode Pydantic refuses to coerce the decoded string
            # back into ``ErrorCode``; JSON mode is the matching inverse of the
            # JSON-mode dump used at persistence time.
            error = QueueError.model_validate_json(record.error_json)
        return cls(
            request_id=record.request_id,
            operation=_body_operation(record),
            sequence=record.queue_sequence,
            state=record.state,
            submitted_at_ms=record.created_at_ms,
            started_at_ms=(
                record.updated_at_ms if record.state is QueuedOperationState.ACTIVE else None
            ),
            completed_at_ms=record.terminal_at_ms,
            result=_load_json(record.result_json),
            error=error,
            quarantine_resolution=quarantine_resolution,
        )


class QueuedOperationList(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    items: tuple[QueuedOperationStatus, ...]


class QueueSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    queued: StrictInt = Field(ge=0)
    active: StrictInt = Field(ge=0)
    completed: StrictInt = Field(ge=0)
    rejected: StrictInt = Field(ge=0)
    quarantined: StrictInt = Field(ge=0)
    cancelled: StrictInt = Field(ge=0)
    unresolved_quarantined: StrictInt = Field(default=0, ge=0)
    resolved_quarantined: StrictInt = Field(default=0, ge=0)
    barrier_active: bool = False

    @classmethod
    def from_counts(
        cls,
        counts: QueueCounts,
        *,
        unresolved_quarantined: int = 0,
        resolved_quarantined: int = 0,
        barrier_active: bool = False,
    ) -> Self:
        return cls(
            queued=counts.queued,
            active=counts.active,
            completed=counts.completed,
            rejected=counts.rejected,
            quarantined=counts.quarantined,
            cancelled=counts.cancelled,
            unresolved_quarantined=unresolved_quarantined,
            resolved_quarantined=resolved_quarantined,
            barrier_active=barrier_active,
        )


class QueueWaitResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    operation: QueuedOperationStatus
    timed_out: bool

    @model_validator(mode="after")
    def _timeout_consistency(self) -> Self:
        if self.timed_out and self.operation.state in _TERMINAL_STATES:
            raise ValueError("terminal operation cannot time out a wait")
        if not self.timed_out and self.operation.state not in _TERMINAL_STATES:
            raise ValueError("non-terminal operation must report timed_out")
        return self


class CancelQueuedOperationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    operation: QueuedOperationStatus
    cancelled: bool

    @model_validator(mode="after")
    def _cancel_consistency(self) -> Self:
        if self.cancelled and self.operation.state is not QueuedOperationState.CANCELLED:
            raise ValueError("successful cancellation requires cancelled state")
        return self


class QueueStore(Protocol):
    """The deliberately narrow dependency implemented by state.operation_queue."""

    def enqueue(self, record: QueuedOperationRecord) -> QueuedOperationRecord: ...

    def get(self, request_id: UUID) -> QueuedOperationRecord | None: ...

    def list(
        self,
        *,
        root_key: str | None = None,
        states: frozenset[QueuedOperationState] | None = None,
    ) -> tuple[QueuedOperationRecord, ...]: ...

    def head(self, *, root_key: str | None = None) -> QueuedOperationRecord | None: ...

    def claim_next(self, *, root_key: str, at_ms: int) -> QueuedOperationRecord | None: ...

    def reset_active_before_publication(self, request_id: UUID, *, at_ms: int) -> bool: ...

    def cancel_queued(self, request_id: UUID, *, at_ms: int) -> QueuedOperationRecord | None: ...

    def complete(
        self, request_id: UUID, result_json: bytes, *, at_ms: int
    ) -> QueuedOperationRecord | None: ...

    def reject(
        self, request_id: UUID, error_json: bytes, *, at_ms: int
    ) -> QueuedOperationRecord | None: ...

    def reject_queued(
        self, request_id: UUID, error_json: bytes, *, at_ms: int
    ) -> QueuedOperationRecord | None: ...

    def quarantine(
        self, request_id: UUID, error_json: bytes, *, at_ms: int
    ) -> QueuedOperationRecord | None: ...

    def counts(self, *, root_key: str | None = None) -> QueueCounts: ...

    def summary_snapshot(
        self,
        *,
        root_key: str,
    ) -> tuple[QueueCounts, tuple[QueuedOperationRecord, ...]]: ...


class QueueClock(Protocol):
    def now_ms(self) -> int: ...


def validate_wait_timeout(value: object) -> float:
    if type(value) not in {int, float} or isinstance(value, bool):
        raise ValueError("wait timeout must be a finite non-negative number")
    timeout = float(cast(int | float, value))
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("wait timeout must be a finite non-negative number")
    return timeout
