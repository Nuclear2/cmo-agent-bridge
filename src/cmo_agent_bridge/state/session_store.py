from __future__ import annotations

import math
import re
import sqlite3
from collections.abc import Mapping
from typing import cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    ValidationError,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, Sha256, revalidate_runtime_snapshot
from cmo_agent_bridge.state.sqlite import StateDatabase


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _state_conflict(message: str) -> BridgeError:
    return BridgeError(ErrorCode.STATE_CONFLICT, message)


def _invalid_argument(message: str) -> BridgeError:
    return BridgeError(ErrorCode.INVALID_ARGUMENT, message)


def _normalize_runtime_snapshot(value: object) -> object:
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


def _uuid_from_sql(value: object, label: str) -> UUID:
    if type(value) is not str:
        raise ValueError(f"{label} must be stored as text")
    parsed = UUID(value)
    if str(parsed) != value:
        raise ValueError(f"{label} is not a canonical UUID")
    return parsed


def _exact_sql_int(value: object, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an exact SQLite integer")
    return value


def _exact_sql_positive_float(value: object, label: str) -> float:
    if type(value) is not float or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{label} must be an exact finite positive SQLite real")
    return value


class SessionRecord(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    root_key: Sha256
    scenario_lineage_id: UUID
    activation_id: UUID
    build_number: StrictInt = Field(gt=0)
    runtime_snapshot: RuntimeSnapshot
    process_pid: StrictInt = Field(gt=0)
    process_create_time: StrictFloat
    validated_at_ms: StrictInt = Field(ge=0)

    @model_validator(mode="before")
    @classmethod
    def revalidate_nested_snapshot(cls, value: object) -> object:
        return _normalize_runtime_snapshot(value)

    @field_validator("root_key", mode="before")
    @classmethod
    def validate_exact_root_key(cls, value: object) -> object:
        if type(value) is not str:
            raise ValueError("session root key must be an exact built-in string")
        return value

    @field_validator("scenario_lineage_id", "activation_id")
    @classmethod
    def validate_exact_uuid(cls, value: UUID) -> UUID:
        if type(value) is not UUID:
            raise ValueError("session UUIDs must be exact UUID values")
        return value

    @field_validator("build_number", "process_pid", "validated_at_ms")
    @classmethod
    def validate_exact_integers(cls, value: int) -> int:
        if type(value) is not int:
            raise ValueError("session integer values must be exact built-in integers")
        return value

    @field_validator("process_create_time", mode="before")
    @classmethod
    def validate_exact_process_create_time(cls, value: object) -> object:
        if type(value) is not float or not math.isfinite(value) or value <= 0:
            raise ValueError("process creation time must be an exact finite positive float")
        return value

    @model_validator(mode="after")
    def validate_session(self) -> Self:
        snapshot = revalidate_runtime_snapshot(self.runtime_snapshot)
        object.__setattr__(self, "runtime_snapshot", snapshot)
        return self


class SessionStore:
    def __init__(self, database: StateDatabase) -> None:
        if type(database) is not StateDatabase:
            raise _invalid_argument("session store database is invalid")
        self._database = database

    def load(self, root_key: str) -> SessionRecord | None:
        validated_root = self._require_root_key(root_key)
        with self._database._transaction(write=False) as connection:  # pyright: ignore[reportPrivateUsage]
            row = connection.execute(
                "SELECT * FROM sessions WHERE root_key=?", (validated_root,)
            ).fetchone()
            return None if row is None else self._record_from_row(row)

    def replace(self, session: SessionRecord) -> None:
        candidate = self._validated_session(session)
        snapshot = candidate.runtime_snapshot
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            connection.execute(
                """
                INSERT INTO sessions(
                    root_key,scenario_lineage_id,activation_id,build_number,
                    runtime_version,runtime_tag,runtime_asset_sha256,release_id,
                    host_contract_sha256,dependency_lock_sha256,manifest_sha256,
                    process_pid,process_create_time,validated_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(root_key) DO UPDATE SET
                    scenario_lineage_id=excluded.scenario_lineage_id,
                    activation_id=excluded.activation_id,
                    build_number=excluded.build_number,
                    runtime_version=excluded.runtime_version,
                    runtime_tag=excluded.runtime_tag,
                    runtime_asset_sha256=excluded.runtime_asset_sha256,
                    release_id=excluded.release_id,
                    host_contract_sha256=excluded.host_contract_sha256,
                    dependency_lock_sha256=excluded.dependency_lock_sha256,
                    manifest_sha256=excluded.manifest_sha256,
                    process_pid=excluded.process_pid,
                    process_create_time=excluded.process_create_time,
                    validated_at_ms=excluded.validated_at_ms
                """,
                (
                    candidate.root_key,
                    str(candidate.scenario_lineage_id),
                    str(candidate.activation_id),
                    candidate.build_number,
                    snapshot.runtime_version,
                    snapshot.runtime_tag,
                    snapshot.runtime_asset_sha256,
                    snapshot.release_id,
                    snapshot.host_contract_sha256,
                    snapshot.dependency_lock_sha256,
                    snapshot.operation_manifest_sha256,
                    candidate.process_pid,
                    candidate.process_create_time,
                    candidate.validated_at_ms,
                ),
            )

    def conditional_replace(
        self,
        session: SessionRecord,
        *,
        expected_activation_id: UUID | None,
    ) -> bool:
        candidate = self._validated_session(session)
        if expected_activation_id is not None and type(expected_activation_id) is not UUID:
            raise _invalid_argument("expected activation ID must be an exact UUID")
        snapshot = candidate.runtime_snapshot
        values = (
            candidate.root_key,
            str(candidate.scenario_lineage_id),
            str(candidate.activation_id),
            candidate.build_number,
            snapshot.runtime_version,
            snapshot.runtime_tag,
            snapshot.runtime_asset_sha256,
            snapshot.release_id,
            snapshot.host_contract_sha256,
            snapshot.dependency_lock_sha256,
            snapshot.operation_manifest_sha256,
            candidate.process_pid,
            candidate.process_create_time,
            candidate.validated_at_ms,
        )
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            if expected_activation_id is None:
                cursor = connection.execute(
                    """
                    INSERT INTO sessions(
                        root_key,scenario_lineage_id,activation_id,build_number,
                        runtime_version,runtime_tag,runtime_asset_sha256,release_id,
                        host_contract_sha256,dependency_lock_sha256,manifest_sha256,
                        process_pid,process_create_time,validated_at_ms
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(root_key) DO NOTHING
                    """,
                    values,
                )
            else:
                cursor = connection.execute(
                    """
                    UPDATE sessions SET
                        scenario_lineage_id=?,activation_id=?,build_number=?,
                        runtime_version=?,runtime_tag=?,runtime_asset_sha256=?,release_id=?,
                        host_contract_sha256=?,dependency_lock_sha256=?,manifest_sha256=?,
                        process_pid=?,process_create_time=?,validated_at_ms=?
                    WHERE root_key=? AND activation_id=?
                    """,
                    (
                        *values[1:],
                        candidate.root_key,
                        str(expected_activation_id),
                    ),
                )
            return cursor.rowcount == 1

    def clear(self, root_key: str, *, expected_activation_id: UUID | None = None) -> bool:
        validated_root = self._require_root_key(root_key)
        if expected_activation_id is not None and type(expected_activation_id) is not UUID:
            raise _invalid_argument("expected activation ID must be an exact UUID")
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            if expected_activation_id is None:
                cursor = connection.execute(
                    "DELETE FROM sessions WHERE root_key=?", (validated_root,)
                )
            else:
                cursor = connection.execute(
                    "DELETE FROM sessions WHERE root_key=? AND activation_id=?",
                    (validated_root, str(expected_activation_id)),
                )
            return cursor.rowcount == 1

    @staticmethod
    def _validated_session(session: SessionRecord) -> SessionRecord:
        try:
            if type(session) is not SessionRecord:
                raise TypeError("session record must be exact")
            return SessionRecord.model_validate(
                session.model_dump(mode="python", round_trip=True, warnings=False)
            )
        except (AttributeError, ValidationError, TypeError, ValueError) as error:
            raise _state_conflict("session record is invalid") from error

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> SessionRecord:
        try:
            snapshot = RuntimeSnapshot.model_validate(
                {
                    "protocol": "cmo-agent-bridge/1",
                    "runtime_version": row["runtime_version"],
                    "runtime_tag": row["runtime_tag"],
                    "runtime_asset_sha256": row["runtime_asset_sha256"],
                    "release_id": row["release_id"],
                    "host_contract_sha256": row["host_contract_sha256"],
                    "dependency_lock_sha256": row["dependency_lock_sha256"],
                    "operation_manifest_sha256": row["manifest_sha256"],
                }
            )
            return SessionRecord(
                root_key=row["root_key"],
                scenario_lineage_id=_uuid_from_sql(
                    row["scenario_lineage_id"], "scenario lineage ID"
                ),
                activation_id=_uuid_from_sql(row["activation_id"], "activation ID"),
                build_number=_exact_sql_int(row["build_number"], "build number", minimum=1),
                runtime_snapshot=snapshot,
                process_pid=_exact_sql_int(row["process_pid"], "process PID", minimum=1),
                process_create_time=_exact_sql_positive_float(
                    row["process_create_time"], "process creation time"
                ),
                validated_at_ms=_exact_sql_int(row["validated_at_ms"], "validation epoch"),
            )
        except (ValidationError, TypeError, ValueError, OverflowError) as error:
            raise _state_conflict("persisted session row is malformed") from error

    @staticmethod
    def _require_root_key(value: str) -> str:
        if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
            raise _invalid_argument("session root key must be lowercase SHA-256")
        return value
