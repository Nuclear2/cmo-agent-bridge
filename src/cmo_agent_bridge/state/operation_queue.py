from __future__ import annotations

import re
import sqlite3
import math
from enum import StrEnum
from typing import Mapping, cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictBytes,
    StrictInt,
    StrictFloat,
    StrictStr,
    ValidationError,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.models import RequestBody, validate_body_runtime_snapshot
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, Sha256, revalidate_runtime_snapshot
from cmo_agent_bridge.state.models import (
    _canonical_json_bytes,  # pyright: ignore[reportPrivateUsage]
    _parse_duplicate_free_json,  # pyright: ignore[reportPrivateUsage]
)
from cmo_agent_bridge.state.sqlite import StateDatabase


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _invalid_argument(message: str) -> BridgeError:
    return BridgeError(ErrorCode.INVALID_ARGUMENT, message)


def _state_conflict(message: str) -> BridgeError:
    return BridgeError(ErrorCode.STATE_CONFLICT, message)


class OperationQueueState(StrEnum):
    QUEUED = "queued"
    ACTIVE = "active"
    COMPLETED = "completed"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    QUARANTINED = "quarantined"


_TERMINAL_STATES = frozenset(
    {
        OperationQueueState.COMPLETED,
        OperationQueueState.REJECTED,
        OperationQueueState.CANCELLED,
        OperationQueueState.QUARANTINED,
    }
)


def _canonical_json_blob(value: object, label: str, *, require_object: bool = False) -> bytes:
    if type(value) is not bytes:
        raise ValueError(f"{label} must be exact bytes")
    raw = value
    try:
        parsed: object = _parse_duplicate_free_json(raw)
    except (TypeError, UnicodeDecodeError, ValueError) as error:
        raise ValueError(f"{label} is not valid JSON") from error
    if require_object and type(parsed) is not dict:
        raise ValueError(f"{label} must be a JSON object")
    if _canonical_json_bytes(cast(JsonValue, parsed)) != raw:
        raise ValueError(f"{label} must be canonical JSON")
    return raw


def _validated_body(value: object) -> RequestBody:
    raw = _canonical_json_blob(value, "queue body")
    try:
        parsed = _parse_duplicate_free_json(raw)
        body = RequestBody.model_validate(parsed)
    except (TypeError, ValidationError, ValueError) as error:
        raise ValueError("queue body is not a valid RequestBody") from error
    if canonical_body_bytes(body) != raw:
        raise ValueError("queue body is not canonical RequestBody JSON")
    return body


def _snapshot_json(value: RuntimeSnapshot) -> bytes:
    return _canonical_json_bytes(value.model_dump(mode="json"))


def _runtime_snapshot_from_json(value: object) -> RuntimeSnapshot:
    raw = _canonical_json_blob(value, "queue runtime snapshot", require_object=True)
    try:
        snapshot = RuntimeSnapshot.model_validate(_parse_duplicate_free_json(raw))
        snapshot = revalidate_runtime_snapshot(snapshot)
    except (TypeError, ValidationError, ValueError) as error:
        raise ValueError("queue runtime snapshot is invalid") from error
    if _snapshot_json(snapshot) != raw:
        raise ValueError("queue runtime snapshot is not canonical")
    return snapshot


def _require_uuid(value: object, label: str) -> UUID:
    if type(value) is not UUID:
        raise ValueError(f"{label} must be an exact UUID")
    return value


def _uuid_from_sql(value: object, label: str) -> UUID:
    if type(value) is not str:
        raise ValueError(f"{label} must be stored as text")
    parsed = UUID(value)
    if str(parsed) != value:
        raise ValueError(f"{label} is not a canonical UUID")
    return parsed


def _optional_uuid_from_sql(value: object, label: str) -> UUID | None:
    return None if value is None else _uuid_from_sql(value, label)


def _exact_sql_int(value: object, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an exact SQLite integer")
    return value


class OperationQueueRecord(BaseModel):
    """One durable CMO operation, in global submission order."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    # A caller supplies zero before enqueue; every persisted record is strictly positive.
    queue_sequence: StrictInt = Field(ge=0)
    request_id: UUID
    root_key: Sha256
    operation: StrictStr
    arguments_json: StrictBytes
    body_json: StrictBytes
    runtime_snapshot: RuntimeSnapshot
    result_schema_id: Sha256
    recovery_schema_id: Sha256 | None
    expected_lineage_id: UUID | None
    expected_activation_id: UUID | None
    expected_process_pid: StrictInt = Field(gt=0)
    expected_process_create_time: StrictFloat = Field(gt=0)
    state: OperationQueueState
    result_json: StrictBytes | None
    error_json: StrictBytes | None
    created_at_ms: StrictInt = Field(ge=0)
    updated_at_ms: StrictInt = Field(ge=0)
    terminal_at_ms: StrictInt | None

    @model_validator(mode="before")
    @classmethod
    def normalize_runtime_snapshot(cls, value: object) -> object:
        if isinstance(value, BaseModel):
            value = value.model_dump(mode="python", round_trip=True, warnings=False)
        if not isinstance(value, Mapping):
            return value
        normalized = dict(cast(Mapping[str, object], value))
        snapshot = normalized.get("runtime_snapshot")
        if isinstance(snapshot, BaseModel):
            normalized["runtime_snapshot"] = snapshot.model_dump(
                mode="python", round_trip=True, warnings=False
            )
        return normalized

    @field_validator("request_id", "expected_lineage_id", "expected_activation_id")
    @classmethod
    def validate_exact_uuids(cls, value: UUID | None) -> UUID | None:
        return None if value is None else _require_uuid(value, "queue UUID")

    @field_validator("root_key", "operation", "result_schema_id", "recovery_schema_id", mode="before")
    @classmethod
    def validate_exact_strings(cls, value: object) -> object:
        if value is not None and type(value) is not str:
            raise ValueError("queue strings must be exact built-in strings")
        return value

    @field_validator("arguments_json")
    @classmethod
    def validate_arguments(cls, value: bytes) -> bytes:
        return _canonical_json_blob(value, "queue arguments", require_object=True)

    @field_validator("body_json")
    @classmethod
    def validate_body(cls, value: bytes) -> bytes:
        _validated_body(value)
        return value

    @field_validator("result_json", "error_json")
    @classmethod
    def validate_terminal_json(cls, value: bytes | None) -> bytes | None:
        return None if value is None else _canonical_json_blob(value, "queue terminal JSON")

    @field_validator("terminal_at_ms")
    @classmethod
    def validate_terminal_epoch(cls, value: int | None) -> int | None:
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError("queue terminal epoch must be exact and non-negative")
        return value

    @field_validator("expected_process_create_time")
    @classmethod
    def validate_process_create_time(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("queue process create time must be finite")
        return value

    @model_validator(mode="after")
    def validate_record(self) -> Self:
        body = _validated_body(self.body_json)
        snapshot = revalidate_runtime_snapshot(self.runtime_snapshot)
        object.__setattr__(self, "runtime_snapshot", snapshot)
        validate_body_runtime_snapshot(body, snapshot)
        if body.operation != self.operation:
            raise ValueError("queue operation disagrees with body")
        if body.expected_lineage_id != self.expected_lineage_id:
            raise ValueError("queue lineage disagrees with body")
        if body.expected_activation_id != self.expected_activation_id:
            raise ValueError("queue activation disagrees with body")
        if self.updated_at_ms < self.created_at_ms:
            raise ValueError("queue update epoch precedes creation")
        if self.terminal_at_ms is not None and self.terminal_at_ms < self.updated_at_ms:
            raise ValueError("queue terminal epoch precedes update")
        if (self.state in _TERMINAL_STATES) != (self.terminal_at_ms is not None):
            raise ValueError("queue terminal state and epoch disagree")
        if self.state is OperationQueueState.COMPLETED:
            if self.result_json is None or self.error_json is not None:
                raise ValueError("completed queue record requires only result JSON")
        elif self.state in {OperationQueueState.REJECTED, OperationQueueState.QUARANTINED}:
            if self.error_json is None or self.result_json is not None:
                raise ValueError("failed queue record requires only error JSON")
        elif self.result_json is not None or self.error_json is not None:
            raise ValueError("non-result queue record cannot persist result JSON")
        return self


class QueueCounts(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    queued: StrictInt = Field(ge=0)
    active: StrictInt = Field(ge=0)
    completed: StrictInt = Field(ge=0)
    rejected: StrictInt = Field(ge=0)
    cancelled: StrictInt = Field(ge=0)
    quarantined: StrictInt = Field(ge=0)


class OperationQueueStore:
    """Strict SQLite persistence for the bridge's one-at-a-time operation queue."""

    def __init__(self, database: StateDatabase) -> None:
        if type(database) is not StateDatabase:
            raise _invalid_argument("operation queue requires an exact StateDatabase")
        self._database = database

    def enqueue(self, record: OperationQueueRecord) -> OperationQueueRecord:
        candidate = self._validated_record(record)
        if candidate.state is not OperationQueueState.QUEUED or candidate.queue_sequence != 0:
            raise _invalid_argument("enqueue requires a queued record with queue sequence zero")
        values = self._values(candidate)
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            try:
                cursor = connection.execute(
                    """
                    INSERT INTO operation_queue(
                        request_id,root_key,operation,arguments_json,body_json,runtime_snapshot_json,result_schema_id,
                        recovery_schema_id,expected_lineage_id,expected_activation_id,state,
                        expected_process_pid,expected_process_create_time,
                        result_json,error_json,created_at_ms,updated_at_ms,terminal_at_ms
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    values,
                )
            except sqlite3.IntegrityError as error:
                raise BridgeError(ErrorCode.REQUEST_ID_REUSED, "queue request ID is already in use") from error
            row = connection.execute(
                "SELECT * FROM operation_queue WHERE queue_sequence=?", (cursor.lastrowid,)
            ).fetchone()
            if row is None:
                raise _state_conflict("enqueued queue record was not persisted")
            return self._record_from_row(row)

    def get(self, request_id: UUID) -> OperationQueueRecord | None:
        validated_id = self._validated_request_id(request_id)
        with self._database._transaction(write=False) as connection:  # pyright: ignore[reportPrivateUsage]
            row = connection.execute(
                "SELECT * FROM operation_queue WHERE request_id=?", (str(validated_id),)
            ).fetchone()
            return None if row is None else self._record_from_row(row)

    def list(
        self,
        *,
        root_key: str | None = None,
        states: frozenset[OperationQueueState] | None = None,
    ) -> tuple[OperationQueueRecord, ...]:
        clauses: list[str] = []
        values: list[object] = []
        if root_key is not None:
            clauses.append("root_key=?")
            values.append(self._validated_root_key(root_key))
        if states is not None:
            validated_states = self._validated_states(states)
            if not validated_states:
                return ()
            clauses.append("state IN (" + ",".join("?" for _ in validated_states) + ")")
            values.extend(state.value for state in validated_states)
        where = "" if not clauses else " WHERE " + " AND ".join(clauses)
        with self._database._transaction(write=False) as connection:  # pyright: ignore[reportPrivateUsage]
            rows = connection.execute(
                "SELECT * FROM operation_queue" + where + " ORDER BY queue_sequence", values
            ).fetchall()
            return tuple(self._record_from_row(row) for row in rows)

    def head(self, *, root_key: str | None = None) -> OperationQueueRecord | None:
        values: tuple[object, ...] = ()
        where = "state='queued'"
        if root_key is not None:
            where += " AND root_key=?"
            values = (self._validated_root_key(root_key),)
        with self._database._transaction(write=False) as connection:  # pyright: ignore[reportPrivateUsage]
            row = connection.execute(
                "SELECT * FROM operation_queue WHERE " + where + " ORDER BY queue_sequence LIMIT 1",
                values,
            ).fetchone()
            return None if row is None else self._record_from_row(row)

    def claim_next(self, *, root_key: str, at_ms: int) -> OperationQueueRecord | None:
        validated_root = self._validated_root_key(root_key)
        timestamp = self._validated_epoch(at_ms, "claim epoch")
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            if connection.execute(
                "SELECT 1 FROM operation_queue WHERE root_key=? AND state='active' LIMIT 1",
                (validated_root,),
            ).fetchone() is not None:
                return None
            row = connection.execute(
                """
                SELECT request_id FROM operation_queue
                WHERE root_key=? AND state='queued'
                ORDER BY queue_sequence LIMIT 1
                """,
                (validated_root,),
            ).fetchone()
            if row is None:
                return None
            request_id = row["request_id"]
            cursor = connection.execute(
                """
                UPDATE operation_queue SET state='active',updated_at_ms=?
                WHERE request_id=? AND root_key=? AND state='queued'
                """,
                (timestamp, request_id, validated_root),
            )
            if cursor.rowcount != 1:
                return None
            claimed = connection.execute(
                "SELECT * FROM operation_queue WHERE request_id=?", (request_id,)
            ).fetchone()
            if claimed is None:
                raise _state_conflict("claimed queue record disappeared")
            return self._record_from_row(claimed)

    def reset_active_before_publication(self, request_id: UUID, *, at_ms: int) -> bool:
        return self._transition_active_to_queued(request_id, at_ms=at_ms)

    def cancel_queued(self, request_id: UUID, *, at_ms: int) -> OperationQueueRecord | None:
        return self._transition_terminal(
            request_id,
            target=OperationQueueState.CANCELLED,
            result_json=None,
            error_json=None,
            at_ms=at_ms,
            source=OperationQueueState.QUEUED,
        )

    def complete(self, request_id: UUID, result_json: bytes, *, at_ms: int) -> OperationQueueRecord | None:
        return self._transition_terminal(
            request_id,
            target=OperationQueueState.COMPLETED,
            result_json=_canonical_json_blob(result_json, "queue result"),
            error_json=None,
            at_ms=at_ms,
            source=OperationQueueState.ACTIVE,
        )

    def reject(self, request_id: UUID, error_json: bytes, *, at_ms: int) -> OperationQueueRecord | None:
        return self._transition_terminal(
            request_id,
            target=OperationQueueState.REJECTED,
            result_json=None,
            error_json=_canonical_json_blob(error_json, "queue error"),
            at_ms=at_ms,
            source=OperationQueueState.ACTIVE,
        )

    def reject_queued(
        self, request_id: UUID, error_json: bytes, *, at_ms: int
    ) -> OperationQueueRecord | None:
        """Reject an order that is known to be unsafe before publication."""
        return self._transition_terminal(
            request_id,
            target=OperationQueueState.REJECTED,
            result_json=None,
            error_json=_canonical_json_blob(error_json, "queue error"),
            at_ms=at_ms,
            source=OperationQueueState.QUEUED,
        )

    def quarantine(self, request_id: UUID, error_json: bytes, *, at_ms: int) -> OperationQueueRecord | None:
        return self._transition_terminal(
            request_id,
            target=OperationQueueState.QUARANTINED,
            result_json=None,
            error_json=_canonical_json_blob(error_json, "queue error"),
            at_ms=at_ms,
            source=OperationQueueState.ACTIVE,
        )

    def counts(self, *, root_key: str | None = None) -> QueueCounts:
        values: tuple[object, ...] = ()
        where = ""
        if root_key is not None:
            where = " WHERE root_key=?"
            values = (self._validated_root_key(root_key),)
        with self._database._transaction(write=False) as connection:  # pyright: ignore[reportPrivateUsage]
            rows = connection.execute(
                "SELECT state,COUNT(*) AS count FROM operation_queue" + where + " GROUP BY state",
                values,
            ).fetchall()
        raw = {state.value: 0 for state in OperationQueueState}
        for row in rows:
            state = row["state"]
            count = row["count"]
            if type(state) is not str or state not in raw or type(count) is not int or count < 0:
                raise _state_conflict("persisted queue count is malformed")
            raw[state] = count
        try:
            return QueueCounts.model_validate(raw)
        except ValidationError as error:
            raise _state_conflict("persisted queue counts are malformed") from error

    def _transition_active_to_queued(self, request_id: UUID, *, at_ms: int) -> bool:
        validated_id = self._validated_request_id(request_id)
        timestamp = self._validated_epoch(at_ms, "reset epoch")
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            cursor = connection.execute(
                "UPDATE operation_queue SET state='queued',updated_at_ms=? WHERE request_id=? AND state='active'",
                (timestamp, str(validated_id)),
            )
            return cursor.rowcount == 1

    def _transition_terminal(
        self,
        request_id: UUID,
        *,
        target: OperationQueueState,
        result_json: bytes | None,
        error_json: bytes | None,
        at_ms: int,
        source: OperationQueueState,
    ) -> OperationQueueRecord | None:
        validated_id = self._validated_request_id(request_id)
        timestamp = self._validated_epoch(at_ms, "terminal transition epoch")
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            cursor = connection.execute(
                """
                UPDATE operation_queue
                SET state=?,result_json=?,error_json=?,updated_at_ms=?,terminal_at_ms=?
                WHERE request_id=? AND state=?
                """,
                (
                    target.value,
                    None if result_json is None else sqlite3.Binary(result_json),
                    None if error_json is None else sqlite3.Binary(error_json),
                    timestamp,
                    timestamp,
                    str(validated_id),
                    source.value,
                ),
            )
            if cursor.rowcount != 1:
                return None
            row = connection.execute(
                "SELECT * FROM operation_queue WHERE request_id=?", (str(validated_id),)
            ).fetchone()
            if row is None:
                raise _state_conflict("terminal queue record disappeared")
            return self._record_from_row(row)

    @staticmethod
    def _values(record: OperationQueueRecord) -> tuple[object, ...]:
        return (
            str(record.request_id),
            record.root_key,
            record.operation,
            sqlite3.Binary(record.arguments_json),
            sqlite3.Binary(record.body_json),
            sqlite3.Binary(_snapshot_json(record.runtime_snapshot)),
            record.result_schema_id,
            record.recovery_schema_id,
            None if record.expected_lineage_id is None else str(record.expected_lineage_id),
            None if record.expected_activation_id is None else str(record.expected_activation_id),
            record.state.value,
            record.expected_process_pid,
            record.expected_process_create_time,
            None if record.result_json is None else sqlite3.Binary(record.result_json),
            None if record.error_json is None else sqlite3.Binary(record.error_json),
            record.created_at_ms,
            record.updated_at_ms,
            record.terminal_at_ms,
        )

    @staticmethod
    def _validated_record(record: OperationQueueRecord) -> OperationQueueRecord:
        try:
            if type(record) is not OperationQueueRecord:
                raise TypeError("queue record must be exact")
            return OperationQueueRecord.model_validate(
                record.model_dump(mode="python", round_trip=True, warnings=False)
            )
        except (AttributeError, ValidationError, TypeError, ValueError, OverflowError) as error:
            raise _invalid_argument("operation queue record is invalid") from error

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> OperationQueueRecord:
        try:
            return OperationQueueRecord(
                queue_sequence=_exact_sql_int(row["queue_sequence"], "queue sequence", minimum=1),
                request_id=_uuid_from_sql(row["request_id"], "queue request ID"),
                root_key=row["root_key"],
                operation=row["operation"],
                arguments_json=row["arguments_json"],
                body_json=row["body_json"],
                runtime_snapshot=_runtime_snapshot_from_json(row["runtime_snapshot_json"]),
                result_schema_id=row["result_schema_id"],
                recovery_schema_id=row["recovery_schema_id"],
                expected_lineage_id=_optional_uuid_from_sql(
                    row["expected_lineage_id"], "queue lineage ID"
                ),
                expected_activation_id=_optional_uuid_from_sql(
                    row["expected_activation_id"], "queue activation ID"
                ),
                expected_process_pid=_exact_sql_int(
                    row["expected_process_pid"], "queue process PID", minimum=1
                ),
                expected_process_create_time=row["expected_process_create_time"],
                state=OperationQueueState(row["state"]),
                result_json=row["result_json"],
                error_json=row["error_json"],
                created_at_ms=_exact_sql_int(row["created_at_ms"], "queue creation epoch"),
                updated_at_ms=_exact_sql_int(row["updated_at_ms"], "queue update epoch"),
                terminal_at_ms=(
                    None
                    if row["terminal_at_ms"] is None
                    else _exact_sql_int(row["terminal_at_ms"], "queue terminal epoch")
                ),
            )
        except (KeyError, TypeError, ValidationError, ValueError, OverflowError) as error:
            raise _state_conflict("persisted operation queue row is malformed") from error

    @staticmethod
    def _validated_request_id(value: UUID) -> UUID:
        try:
            return _require_uuid(value, "queue request ID")
        except ValueError as error:
            raise _invalid_argument(str(error)) from error

    @staticmethod
    def _validated_root_key(value: str) -> str:
        if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
            raise _invalid_argument("queue root key must be lowercase SHA-256")
        return value

    @staticmethod
    def _validated_epoch(value: int, label: str) -> int:
        if type(value) is not int or value < 0:
            raise _invalid_argument(f"{label} must be an exact non-negative integer")
        return value

    @staticmethod
    def _validated_states(value: frozenset[OperationQueueState]) -> tuple[OperationQueueState, ...]:
        if type(value) is not frozenset or any(type(state) is not OperationQueueState for state in value):
            raise _invalid_argument("queue states must be an exact frozenset of queue states")
        return tuple(sorted(value, key=lambda state: state.value))
