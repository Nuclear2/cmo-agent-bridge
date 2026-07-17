from __future__ import annotations

import hashlib
import re
import sqlite3
from collections.abc import Mapping
from typing import Literal, cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBytes,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.operations.models import BridgeStatusWireArgs
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.models import AllowedDelivery, RequestBody, ResponseExpectation
from cmo_agent_bridge.protocol.response_models import ResponseArtifact, Settlement
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, Sha256, revalidate_runtime_snapshot
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    _canonical_json_bytes,  # pyright: ignore[reportPrivateUsage]
    _canonical_model_bytes,  # pyright: ignore[reportPrivateUsage]
    _parse_duplicate_free_json,  # pyright: ignore[reportPrivateUsage]
)
from cmo_agent_bridge.state.revalidation import (
    DurableValidationError,
    RevalidatedExchange,
    revalidate_accepted_artifact,
)
from cmo_agent_bridge.state.sqlite import StateDatabase


_TERMINAL_STATES = frozenset(
    {
        HostRequestState.COMPLETED,
        HostRequestState.CANCELLED,
        HostRequestState.REJECTED,
        HostRequestState.RESOLVED,
    }
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RETENTION_MS = 30 * 24 * 60 * 60 * 1000
_NEWEST_RETENTION_COUNT = 10_000


def _state_conflict(message: str) -> BridgeError:
    return BridgeError(ErrorCode.STATE_CONFLICT, message)


def _invalid_argument(message: str) -> BridgeError:
    return BridgeError(ErrorCode.INVALID_ARGUMENT, message)


def _protocol_error(message: str) -> BridgeError:
    return BridgeError(ErrorCode.PROTOCOL_ERROR, message)


def _normalize_mapping_models(value: object, fields: tuple[str, ...]) -> object:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="python", round_trip=True, warnings=False)
    if not isinstance(value, Mapping):
        return value
    normalized = dict(cast(Mapping[str, object], value))
    for field in fields:
        if field in normalized and isinstance(normalized[field], BaseModel):
            normalized[field] = cast(BaseModel, normalized[field]).model_dump(
                mode="python", round_trip=True, warnings=False
            )
    return normalized


def _canonical_json_text(value: str) -> object:
    if type(value) is not str:
        raise ValueError("durable JSON text must be an exact string")
    parsed = _parse_duplicate_free_json(value)
    if _canonical_json_bytes(parsed) != value.encode("utf-8"):
        raise ValueError("durable JSON text must be canonical")
    return parsed


def _uuid_from_sql(value: object, label: str) -> UUID:
    if type(value) is not str:
        raise ValueError(f"{label} must be stored as text")
    parsed = UUID(value)
    if str(parsed) != value:
        raise ValueError(f"{label} is not a canonical UUID")
    return parsed


def _optional_uuid_from_sql(value: object, label: str) -> UUID | None:
    if value is None:
        return None
    return _uuid_from_sql(value, label)


def _exact_sql_int(value: object, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an exact SQLite integer")
    return value


class RequestRecord(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    request_id: UUID
    root_key: Sha256
    request_hash: Sha256
    operation: StrictStr
    operation_class: OperationClass
    state: HostRequestState
    runtime_snapshot: RuntimeSnapshot
    result_schema_id: Sha256
    recovery_schema_id: Sha256 | None
    body_json: StrictBytes
    lineage_id: UUID | None
    activation_id: UUID | None
    result_json: StrictStr | None
    error_json: StrictStr | None
    resolution_json: StrictStr | None
    created_at_ms: StrictInt = Field(ge=0)
    updated_at_ms: StrictInt = Field(ge=0)
    terminal_at_ms: StrictInt | None

    @model_validator(mode="before")
    @classmethod
    def revalidate_snapshot(cls, value: object) -> object:
        return _normalize_mapping_models(value, ("runtime_snapshot",))

    @field_validator(
        "root_key",
        "request_hash",
        "operation",
        "result_schema_id",
        "recovery_schema_id",
        "result_json",
        "error_json",
        "resolution_json",
        mode="before",
    )
    @classmethod
    def validate_exact_strings(cls, value: object) -> object:
        if value is not None and type(value) is not str:
            raise ValueError("request record strings must be exact")
        return value

    @field_validator("body_json", mode="before")
    @classmethod
    def validate_exact_bytes(cls, value: object) -> object:
        if type(value) is not bytes:
            raise ValueError("request body must be exact bytes")
        return value

    @field_validator("request_id", "lineage_id", "activation_id")
    @classmethod
    def validate_exact_uuids(cls, value: UUID | None) -> UUID | None:
        if value is not None and type(value) is not UUID:
            raise ValueError("request record UUIDs must be exact")
        return value

    @field_validator("terminal_at_ms")
    @classmethod
    def validate_terminal_epoch(cls, value: int | None) -> int | None:
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError("terminal epoch must be an exact non-negative integer")
        return value

    @model_validator(mode="after")
    def validate_record(self) -> Self:
        snapshot = revalidate_runtime_snapshot(self.runtime_snapshot)
        object.__setattr__(self, "runtime_snapshot", snapshot)
        if self.updated_at_ms < self.created_at_ms:
            raise ValueError("request update epoch precedes creation")
        if self.terminal_at_ms is not None and self.terminal_at_ms < self.updated_at_ms:
            raise ValueError("request terminal epoch precedes update")
        if (self.state in _TERMINAL_STATES) != (self.terminal_at_ms is not None):
            raise ValueError("request terminal state and epoch disagree")
        for text in (self.result_json, self.error_json, self.resolution_json):
            if text is not None:
                _canonical_json_text(text)
        if self.state is HostRequestState.RESOLVED:
            if (
                self.resolution_json is None
                or self.result_json is not None
                or self.error_json is not None
            ):
                raise ValueError("resolved request requires only resolution JSON")
        elif self.state is HostRequestState.COMPLETED:
            if self.error_json is not None or self.resolution_json is not None:
                raise ValueError("completed request permits only result JSON")
        elif self.state is HostRequestState.REJECTED:
            if self.result_json is not None or self.resolution_json is not None:
                raise ValueError("rejected request permits only error JSON")
        elif self.state is HostRequestState.CANCELLED:
            if any(
                value is not None
                for value in (self.result_json, self.error_json, self.resolution_json)
            ):
                raise ValueError("cancelled request forbids terminal JSON")
        elif self.state is HostRequestState.QUARANTINED:
            if self.result_json is not None or self.resolution_json is not None:
                raise ValueError("quarantined request permits only error JSON")
        elif any(
            value is not None for value in (self.result_json, self.error_json, self.resolution_json)
        ):
            raise ValueError("nonterminal request cannot persist terminal JSON")
        return self


class DeliveryRecord(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    delivery_id: UUID
    request_id: UUID
    delivery_kind: Literal["request", "cancel"]
    original_request_delivery_id: UUID
    intended_at_ms: StrictInt = Field(ge=0)
    published_at_ms: StrictInt | None
    rendered_inbox_sha256: Sha256
    rendered_inbox_size_bytes: StrictInt = Field(ge=0)
    response_filename: StrictStr
    response_artifact: ResponseArtifact | None
    settlement: Settlement | None

    @model_validator(mode="before")
    @classmethod
    def revalidate_nested_models(cls, value: object) -> object:
        return _normalize_mapping_models(value, ("response_artifact", "settlement"))

    @field_validator("delivery_kind", "rendered_inbox_sha256", "response_filename", mode="before")
    @classmethod
    def validate_exact_strings(cls, value: object) -> object:
        if type(value) is not str:
            raise ValueError("delivery record strings must be exact")
        return value

    @field_validator("delivery_id", "request_id", "original_request_delivery_id")
    @classmethod
    def validate_exact_uuids(cls, value: UUID) -> UUID:
        if type(value) is not UUID:
            raise ValueError("delivery record UUIDs must be exact")
        return value

    @field_validator("published_at_ms")
    @classmethod
    def validate_publication_epoch(cls, value: int | None) -> int | None:
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError("delivery publication epoch must be exact and non-negative")
        return value

    @model_validator(mode="after")
    def validate_record(self) -> Self:
        if self.published_at_ms is not None and self.published_at_ms < self.intended_at_ms:
            raise ValueError("delivery publication precedes intent")
        if self.response_artifact is None:
            if self.settlement is not None:
                raise ValueError("delivery settlement requires response artifact")
        else:
            if self.published_at_ms is None:
                raise ValueError("accepted response requires a durable publication marker")
            if _canonical_model_bytes(
                self.response_artifact.accepted_response.settlement
            ) != _canonical_model_bytes(self.settlement):
                raise ValueError("delivery settlement differs from accepted response")
        return self


class RequestLedger:
    def __init__(self, database: StateDatabase, catalog: ManifestCatalog) -> None:
        if type(database) is not StateDatabase or type(catalog) is not ManifestCatalog:
            raise BridgeError(ErrorCode.INVALID_ARGUMENT, "request ledger dependencies are invalid")
        self._database = database
        self._catalog = catalog

    def insert_prepared(self, record: RequestRecord) -> None:
        try:
            if type(record) is not RequestRecord:
                raise TypeError("request record must be exact")
            candidate = RequestRecord.model_validate(
                record.model_dump(mode="python", round_trip=True, warnings=False)
            )
        except (
            AttributeError,
            ValidationError,
            TypeError,
            ValueError,
            OverflowError,
            RecursionError,
        ) as error:
            raise _state_conflict("request record structure is invalid") from error
        if (
            candidate.state is not HostRequestState.PREPARED
            or candidate.result_json is not None
            or candidate.error_json is not None
            or candidate.resolution_json is not None
            or candidate.terminal_at_ms is not None
        ):
            raise _state_conflict("insert_prepared requires an exact prepared request")
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            row = connection.execute(
                "SELECT * FROM requests WHERE request_id=?", (str(candidate.request_id),)
            ).fetchone()
            if row is not None:
                existing = self._request_from_row(row)
                if existing.request_hash != candidate.request_hash:
                    raise BridgeError(
                        ErrorCode.REQUEST_ID_REUSED,
                        "request ID is already bound to a different request hash",
                        {"request_id": str(candidate.request_id)},
                    )
                candidate = self._validated_request(candidate, require_current=True)
                if existing == candidate:
                    return
                raise _state_conflict("request ID already exists with different metadata")
            candidate = self._validated_request(candidate, require_current=True)
            values = self._request_sql_values(candidate)
            connection.execute(
                """
                INSERT INTO requests(
                    request_id,root_key,request_hash,operation,operation_class,state,
                    runtime_version,runtime_tag,runtime_asset_sha256,release_id,
                    host_contract_sha256,dependency_lock_sha256,manifest_sha256,
                    result_schema_id,recovery_schema_id,body_json,lineage_id,activation_id,
                    result_json,error_json,resolution_json,created_at_ms,updated_at_ms,terminal_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                values,
            )

    def insert_delivery(self, intent: DeliveryIntent) -> None:
        try:
            candidate = DeliveryIntent.model_validate(
                intent.model_dump(mode="python", round_trip=True, warnings=False)
            )
        except (
            AttributeError,
            ValidationError,
            TypeError,
            ValueError,
            OverflowError,
            RecursionError,
        ) as error:
            raise _state_conflict("delivery intent is invalid") from error
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            request_row = connection.execute(
                "SELECT * FROM requests WHERE request_id=?", (str(candidate.request_id),)
            ).fetchone()
            if request_row is None:
                raise _state_conflict("delivery owner request does not exist")
            owner = self._validated_request(
                self._request_from_row(request_row), require_current=True
            )
            if (
                candidate.body_json.encode("utf-8") != owner.body_json
                or candidate.request_hash != owner.request_hash
                or candidate.runtime_snapshot != owner.runtime_snapshot
                or candidate.result_schema_id != owner.result_schema_id
                or candidate.recovery_schema_id != owner.recovery_schema_id
            ):
                raise _state_conflict("delivery identity differs from owning request")
            if candidate.delivery_kind == "cancel":
                original = connection.execute(
                    "SELECT request_id,delivery_kind,original_request_delivery_id "
                    "FROM deliveries WHERE delivery_id=?",
                    (str(candidate.original_request_delivery_id),),
                ).fetchone()
                if (
                    original is None
                    or original["request_id"] != str(candidate.request_id)
                    or original["delivery_kind"] != "request"
                    or original["original_request_delivery_id"]
                    != str(candidate.original_request_delivery_id)
                ):
                    raise _state_conflict("cancel delivery does not target the request delivery")
            existing_row = connection.execute(
                "SELECT * FROM deliveries WHERE delivery_id=?", (str(candidate.delivery_id),)
            ).fetchone()
            expected = self._delivery_from_intent(candidate)
            if existing_row is not None:
                existing = self._delivery_from_row(connection, existing_row)
                if (
                    existing.model_copy(update={"response_artifact": None, "settlement": None})
                    == expected
                ):
                    return
                raise _state_conflict("delivery ID already exists with different metadata")
            connection.execute(
                """
                INSERT INTO deliveries(
                    delivery_id,request_id,delivery_kind,original_request_delivery_id,
                    intended_at_ms,published_at_ms,rendered_inbox_sha256,
                    rendered_inbox_size_bytes,response_filename,response_at_ms,response_sha256,
                    response_size_bytes,accepted_response_json,settlement_json
                ) VALUES(?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,NULL,NULL)
                """,
                (
                    str(candidate.delivery_id),
                    str(candidate.request_id),
                    candidate.delivery_kind,
                    str(candidate.original_request_delivery_id),
                    candidate.intended_at_ms,
                    candidate.published_at_ms,
                    candidate.rendered_inbox_sha256,
                    candidate.rendered_inbox_size_bytes,
                    candidate.response_filename,
                ),
            )

    def get_request(self, request_id: UUID) -> RequestRecord | None:
        self._require_uuid(request_id, "request ID")
        with self._database._transaction(write=False) as connection:  # pyright: ignore[reportPrivateUsage]
            row = connection.execute(
                "SELECT * FROM requests WHERE request_id=?", (str(request_id),)
            ).fetchone()
            return None if row is None else self._request_from_row(row)

    def get_delivery(self, delivery_id: UUID) -> DeliveryRecord | None:
        self._require_uuid(delivery_id, "delivery ID")
        with self._database._transaction(write=False) as connection:  # pyright: ignore[reportPrivateUsage]
            row = connection.execute(
                "SELECT * FROM deliveries WHERE delivery_id=?", (str(delivery_id),)
            ).fetchone()
            return None if row is None else self._delivery_from_row(connection, row)

    def list_deliveries(self, request_id: UUID) -> tuple[DeliveryRecord, ...]:
        self._require_uuid(request_id, "request ID")
        with self._database._transaction(write=False) as connection:  # pyright: ignore[reportPrivateUsage]
            rows = connection.execute(
                "SELECT * FROM deliveries WHERE request_id=? ORDER BY intended_at_ms,delivery_id",
                (str(request_id),),
            ).fetchall()
            return tuple(self._delivery_from_row(connection, row) for row in rows)

    def transition(
        self,
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
        self._require_uuid(request_id, "request ID")
        expected = self._require_states(expected_states)
        if type(new_state) is not HostRequestState:
            raise _state_conflict("new request state must be exact")
        self._validate_transition_values(
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        placeholders = ",".join("?" for _ in expected)
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            row = connection.execute(
                "SELECT created_at_ms,updated_at_ms FROM requests WHERE request_id=?",
                (str(request_id),),
            ).fetchone()
            if row is None:
                raise _state_conflict("request transition target does not exist")
            try:
                stored_created = _exact_sql_int(row["created_at_ms"], "request creation epoch")
                stored_updated = _exact_sql_int(row["updated_at_ms"], "request update epoch")
            except ValueError as error:
                raise _state_conflict("persisted request transition times are malformed") from error
            if updated_at_ms < stored_updated or updated_at_ms < stored_created:
                raise _state_conflict("request transition time moved backwards")
            parameters = (
                new_state.value,
                updated_at_ms,
                terminal_at_ms,
                result_json,
                error_json,
                resolution_json,
                str(request_id),
                *(state.value for state in expected),
            )
            cursor = connection.execute(
                f"""
                UPDATE requests SET state=?,updated_at_ms=?,terminal_at_ms=?,
                    result_json=?,error_json=?,resolution_json=?
                WHERE request_id=? AND state IN ({placeholders})
                """,
                parameters,
            )
            if cursor.rowcount != 1:
                raise _state_conflict("request state changed before conditional transition")
            result = connection.execute(
                "SELECT * FROM requests WHERE request_id=?", (str(request_id),)
            ).fetchone()
            if result is None:
                raise _state_conflict("request disappeared after transition")
            return self._request_from_row(result)

    def mark_delivery_published(self, delivery_id: UUID, *, published_at_ms: int) -> DeliveryRecord:
        self._require_uuid(delivery_id, "delivery ID")
        if type(published_at_ms) is not int or published_at_ms < 0:
            raise _state_conflict("publication epoch must be an exact non-negative integer")
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            cursor = connection.execute(
                """
                UPDATE deliveries SET published_at_ms=?
                WHERE delivery_id=? AND published_at_ms IS NULL AND intended_at_ms<=?
                """,
                (published_at_ms, str(delivery_id), published_at_ms),
            )
            if cursor.rowcount != 1:
                raise _state_conflict("delivery publication marker changed or is invalid")
            row = connection.execute(
                "SELECT * FROM deliveries WHERE delivery_id=?", (str(delivery_id),)
            ).fetchone()
            if row is None:
                raise _state_conflict("delivery disappeared after publication")
            return self._delivery_from_row(connection, row)

    def record_response(self, artifact: ResponseArtifact) -> DeliveryRecord:
        try:
            candidate = ResponseArtifact.model_validate(
                artifact.model_dump(mode="python", round_trip=True, warnings=False)
            )
        except (
            AttributeError,
            ValidationError,
            TypeError,
            ValueError,
            OverflowError,
            RecursionError,
        ) as error:
            raise _protocol_error("incoming response artifact is invalid") from error
        envelope = candidate.accepted_response.envelope
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            row = connection.execute(
                "SELECT * FROM deliveries WHERE delivery_id=? AND request_id=?",
                (str(envelope.delivery_id), str(envelope.request_id)),
            ).fetchone()
            if row is None or row["published_at_ms"] is None:
                raise _protocol_error("response does not identify a published delivery")
            try:
                validated = self._revalidated_request(connection, envelope.request_id)
            except (
                BridgeError,
                ValidationError,
                TypeError,
                ValueError,
                OverflowError,
                RecursionError,
            ) as error:
                raise _state_conflict(
                    "persisted response owner failed semantic reconstruction"
                ) from error
            try:
                revalidate_accepted_artifact(
                    candidate,
                    candidate.accepted_response.settlement,
                    validated=validated,
                )
            except (
                BridgeError,
                DurableValidationError,
                TypeError,
                ValueError,
                OverflowError,
                RecursionError,
            ) as error:
                raise _protocol_error(
                    "incoming response artifact failed semantic validation"
                ) from error
            accepted_bytes = _canonical_json_bytes(
                candidate.accepted_response.model_dump(mode="json")
            )
            settlement_bytes = _canonical_model_bytes(candidate.accepted_response.settlement)
            values = (
                candidate.accepted_at_ms,
                candidate.sha256,
                candidate.size_bytes,
                sqlite3.Binary(accepted_bytes),
                sqlite3.Binary(settlement_bytes),
            )
            cursor = connection.execute(
                """
                UPDATE deliveries SET response_at_ms=?,response_sha256=?,response_size_bytes=?,
                    accepted_response_json=?,settlement_json=?
                WHERE delivery_id=? AND response_at_ms IS NULL AND response_sha256 IS NULL
                    AND response_size_bytes IS NULL AND accepted_response_json IS NULL
                    AND settlement_json IS NULL
                """,
                (*values, str(envelope.delivery_id)),
            )
            if cursor.rowcount != 1:
                existing = tuple(
                    row_value
                    for row_value in connection.execute(
                        """
                        SELECT response_at_ms,response_sha256,response_size_bytes,
                               accepted_response_json,settlement_json
                        FROM deliveries WHERE delivery_id=?
                        """,
                        (str(envelope.delivery_id),),
                    ).fetchone()
                )
                expected = (
                    candidate.accepted_at_ms,
                    candidate.sha256,
                    candidate.size_bytes,
                    accepted_bytes,
                    settlement_bytes,
                )
                if existing != expected:
                    raise _state_conflict("delivery already contains different response evidence")
            stored = connection.execute(
                "SELECT * FROM deliveries WHERE delivery_id=?", (str(envelope.delivery_id),)
            ).fetchone()
            if stored is None:
                raise _state_conflict("delivery disappeared after recording response")
            return self._delivery_from_row(connection, stored)

    def resolve_reconciliation(
        self,
        *,
        attempt_request_id: UUID,
        original_request_id: UUID,
        expected_attempt_states: frozenset[HostRequestState],
        expected_original_states: frozenset[HostRequestState],
        resolution_json: str,
        completed_at_ms: int,
    ) -> tuple[RequestRecord, RequestRecord]:
        self._require_uuid(attempt_request_id, "attempt request ID")
        self._require_uuid(original_request_id, "original request ID")
        if attempt_request_id == original_request_id:
            raise _state_conflict("reconcile attempt and original IDs must differ")
        attempt_states = self._require_states(expected_attempt_states)
        original_states = self._require_states(expected_original_states)
        if type(completed_at_ms) is not int or completed_at_ms < 0:
            raise _state_conflict("reconcile completion epoch is invalid")
        try:
            _canonical_json_text(resolution_json)
        except (TypeError, ValueError, OverflowError, RecursionError) as error:
            raise _state_conflict("reconciliation resolution JSON is invalid") from error
        attempt_placeholders = ",".join("?" for _ in attempt_states)
        original_placeholders = ",".join("?" for _ in original_states)
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            attempt = connection.execute(
                f"""
                UPDATE requests SET state='completed',updated_at_ms=?,terminal_at_ms=?
                WHERE request_id=? AND state IN ({attempt_placeholders})
                  AND updated_at_ms<=?
                """,
                (
                    completed_at_ms,
                    completed_at_ms,
                    str(attempt_request_id),
                    *(state.value for state in attempt_states),
                    completed_at_ms,
                ),
            )
            if attempt.rowcount != 1:
                raise _state_conflict("reconcile attempt state changed")
            original = connection.execute(
                f"""
                UPDATE requests SET state='resolved',updated_at_ms=?,terminal_at_ms=?,
                    resolution_json=?,result_json=NULL,error_json=NULL
                WHERE request_id=? AND state IN ({original_placeholders})
                  AND updated_at_ms<=?
                """,
                (
                    completed_at_ms,
                    completed_at_ms,
                    resolution_json,
                    str(original_request_id),
                    *(state.value for state in original_states),
                    completed_at_ms,
                ),
            )
            if original.rowcount != 1:
                raise _state_conflict("reconcile original state changed")
            attempt_row = connection.execute(
                "SELECT * FROM requests WHERE request_id=?", (str(attempt_request_id),)
            ).fetchone()
            original_row = connection.execute(
                "SELECT * FROM requests WHERE request_id=?", (str(original_request_id),)
            ).fetchone()
            if attempt_row is None or original_row is None:
                raise _state_conflict("reconciliation rows disappeared")
            return self._request_from_row(attempt_row), self._request_from_row(original_row)

    def unresolved_release_ids(self) -> frozenset[str]:
        placeholders = ",".join("?" for _ in _TERMINAL_STATES)
        with self._database._transaction(write=False) as connection:  # pyright: ignore[reportPrivateUsage]
            rows = connection.execute(
                f"SELECT DISTINCT release_id FROM requests WHERE state NOT IN ({placeholders})",
                tuple(state.value for state in _TERMINAL_STATES),
            ).fetchall()
            values = frozenset(row["release_id"] for row in rows)
            if any(
                type(value) is not str or _SHA256_RE.fullmatch(value) is None for value in values
            ):
                raise _state_conflict("unresolved request release IDs are malformed")
            return cast(frozenset[str], values)

    def prune_terminal(self, *, now_ms: int) -> int:
        if type(now_ms) is not int or now_ms < 0:
            raise _invalid_argument("prune epoch must be an exact non-negative integer")
        cutoff = now_ms - _RETENTION_MS
        terminal_values = tuple(state.value for state in _TERMINAL_STATES)
        placeholders = ",".join("?" for _ in terminal_values)
        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            if cutoff < 0:
                return 0
            protected = {
                row["request_id"]
                for row in connection.execute(
                    "SELECT request_id FROM requests ORDER BY created_at_ms DESC,request_id DESC LIMIT ?",
                    (_NEWEST_RETENTION_COUNT,),
                ).fetchall()
            }
            rows = connection.execute(
                f"""
                SELECT request_id FROM requests
                WHERE state IN ({placeholders}) AND terminal_at_ms<?
                ORDER BY request_id
                """,
                (*terminal_values, cutoff),
            ).fetchall()
            victims = tuple(row["request_id"] for row in rows if row["request_id"] not in protected)
            for request_id in victims:
                connection.execute("DELETE FROM deliveries WHERE request_id=?", (request_id,))
                cursor = connection.execute(
                    "DELETE FROM requests WHERE request_id=?", (request_id,)
                )
                if cursor.rowcount != 1:
                    raise _state_conflict("terminal prune lost an exact request row")
            return len(victims)

    def _validated_request(self, record: RequestRecord, *, require_current: bool) -> RequestRecord:
        try:
            if type(record) is not RequestRecord:
                raise TypeError("request record must be exact")
            candidate = RequestRecord.model_validate(
                record.model_dump(mode="python", round_trip=True, warnings=False)
            )
            body = self._request_body(candidate)
            if require_current:
                binding = self._catalog.resolve_running(candidate.runtime_snapshot.release_id)
                invocation = binding.registry.resolve_wire_invocation(
                    body.operation, body.arguments
                )
                recovery_schema_id = (
                    None
                    if invocation.recovery_schema is None
                    else invocation.recovery_schema.schema_id
                )
                if (
                    invocation.contract.name != candidate.operation
                    or invocation.effective_class is not candidate.operation_class
                    or invocation.result_schema.schema_id != candidate.result_schema_id
                    or recovery_schema_id != candidate.recovery_schema_id
                ):
                    raise ValueError("request record schema binding is inconsistent")
            return candidate
        except (
            BridgeError,
            ValidationError,
            TypeError,
            ValueError,
            OverflowError,
            RecursionError,
        ) as error:
            raise _state_conflict("request record failed durable semantic validation") from error

    @staticmethod
    def _request_body(record: RequestRecord) -> RequestBody:
        parsed = _parse_duplicate_free_json(record.body_json)
        body = RequestBody.model_validate(parsed)
        if canonical_body_bytes(body) != record.body_json:
            raise ValueError("request body is not canonical")
        if hashlib.sha256(record.body_json).hexdigest() != record.request_hash:
            raise ValueError("request hash does not match body")
        if body.operation != record.operation:
            raise ValueError("request operation does not match body")
        snapshot = record.runtime_snapshot
        expected_identity = {
            "protocol": snapshot.protocol,
            "release_id": snapshot.release_id,
            "runtime_version": snapshot.runtime_version,
            "runtime_tag": snapshot.runtime_tag,
            "runtime_asset_sha256": snapshot.runtime_asset_sha256,
            "operation_manifest_sha256": snapshot.operation_manifest_sha256,
        }
        if any(getattr(body, field) != value for field, value in expected_identity.items()):
            raise ValueError("request body runtime identity differs from snapshot")
        if body.expected_lineage_id != record.lineage_id:
            raise ValueError("request body lineage differs from request record")
        if body.expected_activation_id != record.activation_id:
            raise ValueError("request body activation differs from request record")
        return body

    def _request_from_row(self, row: sqlite3.Row) -> RequestRecord:
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
            body_value = row["body_json"]
            if type(body_value) is not bytes:
                raise ValueError("request body SQLite value is not a BLOB")
            record = RequestRecord(
                request_id=_uuid_from_sql(row["request_id"], "request ID"),
                root_key=row["root_key"],
                request_hash=row["request_hash"],
                operation=row["operation"],
                operation_class=OperationClass(row["operation_class"]),
                state=HostRequestState(row["state"]),
                runtime_snapshot=snapshot,
                result_schema_id=row["result_schema_id"],
                recovery_schema_id=row["recovery_schema_id"],
                body_json=body_value,
                lineage_id=_optional_uuid_from_sql(row["lineage_id"], "lineage ID"),
                activation_id=_optional_uuid_from_sql(row["activation_id"], "activation ID"),
                result_json=row["result_json"],
                error_json=row["error_json"],
                resolution_json=row["resolution_json"],
                created_at_ms=_exact_sql_int(row["created_at_ms"], "request creation epoch"),
                updated_at_ms=_exact_sql_int(row["updated_at_ms"], "request update epoch"),
                terminal_at_ms=(
                    None
                    if row["terminal_at_ms"] is None
                    else _exact_sql_int(row["terminal_at_ms"], "request terminal epoch")
                ),
            )
            self._request_body(record)
            return record
        except (ValidationError, TypeError, ValueError, OverflowError, RecursionError) as error:
            raise _state_conflict("persisted request row is malformed") from error

    def _delivery_from_row(
        self, connection: sqlite3.Connection, row: sqlite3.Row
    ) -> DeliveryRecord:
        try:
            delivery_id = _uuid_from_sql(row["delivery_id"], "delivery ID")
            request_id = _uuid_from_sql(row["request_id"], "delivery request ID")
            original_delivery_id = _uuid_from_sql(
                row["original_request_delivery_id"], "original delivery ID"
            )
            delivery_kind = row["delivery_kind"]
            if delivery_kind == "request":
                if original_delivery_id != delivery_id:
                    raise ValueError("request delivery does not point to itself")
            elif delivery_kind == "cancel":
                if original_delivery_id == delivery_id:
                    raise ValueError("cancel delivery points to itself")
                original_row = connection.execute(
                    "SELECT request_id,delivery_kind,original_request_delivery_id "
                    "FROM deliveries WHERE delivery_id=?",
                    (str(original_delivery_id),),
                ).fetchone()
                if (
                    original_row is None
                    or original_row["request_id"] != str(request_id)
                    or original_row["delivery_kind"] != "request"
                    or original_row["original_request_delivery_id"] != str(original_delivery_id)
                ):
                    raise ValueError("cancel original delivery is not the owning request delivery")
            else:
                raise ValueError("delivery kind is malformed")
            response_values = tuple(
                row[field]
                for field in (
                    "response_at_ms",
                    "response_sha256",
                    "response_size_bytes",
                    "accepted_response_json",
                    "settlement_json",
                )
            )
            all_null = all(value is None for value in response_values)
            all_present = all(value is not None for value in response_values)
            if not all_null and not all_present:
                raise ValueError("delivery response group is partial")
            artifact: ResponseArtifact | None = None
            settlement: Settlement | None = None
            if all_present:
                response_at, response_hash, response_size, accepted_blob, settlement_blob = (
                    response_values
                )
                if type(response_hash) is not str or _SHA256_RE.fullmatch(response_hash) is None:
                    raise ValueError("response hash is malformed")
                if type(accepted_blob) is not bytes or type(settlement_blob) is not bytes:
                    raise ValueError("response JSON values are not BLOBs")
                accepted_tree = _parse_duplicate_free_json(accepted_blob)
                if _canonical_json_bytes(accepted_tree) != accepted_blob:
                    raise ValueError("accepted response BLOB is noncanonical")
                settlement_tree = _parse_duplicate_free_json(settlement_blob)
                if _canonical_json_bytes(settlement_tree) != settlement_blob:
                    raise ValueError("settlement BLOB is noncanonical")
                from cmo_agent_bridge.protocol.response_models import AcceptedResponse

                accepted = AcceptedResponse.model_validate_json(accepted_blob)
                if _canonical_json_bytes(accepted.model_dump(mode="json")) != accepted_blob:
                    raise ValueError("accepted response BLOB has noncanonical typed values")
                if _canonical_model_bytes(accepted.settlement) != settlement_blob:
                    raise ValueError("settlement BLOB differs from accepted response")
                artifact = ResponseArtifact(
                    filename=row["response_filename"],
                    sha256=response_hash,
                    size_bytes=_exact_sql_int(response_size, "response size"),
                    accepted_at_ms=_exact_sql_int(response_at, "response acceptance epoch"),
                    accepted_response=accepted,
                )
                validated = self._revalidated_request(
                    connection, artifact.accepted_response.envelope.request_id
                )
                artifact = revalidate_accepted_artifact(
                    artifact,
                    accepted.settlement,
                    validated=validated,
                )
                settlement = artifact.accepted_response.settlement
                envelope = artifact.accepted_response.envelope
                if (
                    envelope.delivery_id != delivery_id
                    or envelope.request_id != request_id
                    or artifact.accepted_response.delivery_kind != delivery_kind
                ):
                    raise ValueError(
                        "accepted artifact does not belong to the physical delivery row"
                    )
            return DeliveryRecord(
                delivery_id=delivery_id,
                request_id=request_id,
                delivery_kind=delivery_kind,
                original_request_delivery_id=original_delivery_id,
                intended_at_ms=_exact_sql_int(row["intended_at_ms"], "delivery intent epoch"),
                published_at_ms=(
                    None
                    if row["published_at_ms"] is None
                    else _exact_sql_int(row["published_at_ms"], "delivery publication epoch")
                ),
                rendered_inbox_sha256=row["rendered_inbox_sha256"],
                rendered_inbox_size_bytes=_exact_sql_int(
                    row["rendered_inbox_size_bytes"], "rendered inbox size"
                ),
                response_filename=row["response_filename"],
                response_artifact=artifact,
                settlement=settlement,
            )
        except (
            BridgeError,
            DurableValidationError,
            ValidationError,
            TypeError,
            ValueError,
            OverflowError,
            RecursionError,
        ) as error:
            if isinstance(error, BridgeError) and error.code is ErrorCode.STATE_CONFLICT:
                raise
            raise _state_conflict("persisted delivery row is malformed") from error

    def _revalidated_request(
        self, connection: sqlite3.Connection, request_id: UUID
    ) -> RevalidatedExchange:
        row = connection.execute(
            "SELECT * FROM requests WHERE request_id=?", (str(request_id),)
        ).fetchone()
        if row is None:
            raise _state_conflict("response owner request does not exist")
        record = self._request_from_row(row)
        body = self._request_body(record)
        binding: ReleaseBinding = self._catalog.resolve_running(record.runtime_snapshot.release_id)
        invocation = binding.registry.resolve_wire_invocation(body.operation, body.arguments)
        recovery_schema_id = (
            None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
        )
        if (
            invocation.effective_class is not record.operation_class
            or invocation.result_schema.schema_id != record.result_schema_id
            or recovery_schema_id != record.recovery_schema_id
        ):
            raise _state_conflict("request schema binding differs from current release")
        delivery_rows = connection.execute(
            "SELECT delivery_id,delivery_kind FROM deliveries WHERE request_id=? "
            "ORDER BY intended_at_ms,delivery_id",
            (str(request_id),),
        ).fetchall()
        allowed = tuple(
            AllowedDelivery(
                delivery_id=_uuid_from_sql(item["delivery_id"], "allowed delivery ID"),
                delivery_kind=item["delivery_kind"],
            )
            for item in delivery_rows
        )
        status_bootstrap = False
        activation_candidate: UUID | None = None
        if body.operation == "bridge.status":
            wire_arguments = invocation.wire_arguments
            if not isinstance(wire_arguments, BridgeStatusWireArgs):
                raise _state_conflict("status request has invalid wire arguments")
            activation_candidate = wire_arguments.activation_candidate
            status_bootstrap = (
                body.expected_lineage_id is None and body.expected_activation_id is None
            )
        expectation = ResponseExpectation(
            request_id=record.request_id,
            allowed_deliveries=allowed,
            request_hash=record.request_hash,
            expected_lineage_id=record.lineage_id,
            expected_activation_id=record.activation_id,
            status_bootstrap=status_bootstrap,
            activation_candidate=activation_candidate,
            runtime_snapshot=binding.snapshot,
            invocation=invocation,
        )
        return RevalidatedExchange(invocation=invocation, expectation=expectation)

    @staticmethod
    def _delivery_from_intent(intent: DeliveryIntent) -> DeliveryRecord:
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

    @staticmethod
    def _request_sql_values(record: RequestRecord) -> tuple[object, ...]:
        snapshot = record.runtime_snapshot
        return (
            str(record.request_id),
            record.root_key,
            record.request_hash,
            record.operation,
            record.operation_class.value,
            record.state.value,
            snapshot.runtime_version,
            snapshot.runtime_tag,
            snapshot.runtime_asset_sha256,
            snapshot.release_id,
            snapshot.host_contract_sha256,
            snapshot.dependency_lock_sha256,
            snapshot.operation_manifest_sha256,
            record.result_schema_id,
            record.recovery_schema_id,
            sqlite3.Binary(record.body_json),
            None if record.lineage_id is None else str(record.lineage_id),
            None if record.activation_id is None else str(record.activation_id),
            record.result_json,
            record.error_json,
            record.resolution_json,
            record.created_at_ms,
            record.updated_at_ms,
            record.terminal_at_ms,
        )

    @staticmethod
    def _require_uuid(value: UUID, label: str) -> None:
        if type(value) is not UUID:
            raise _invalid_argument(f"{label} must be an exact UUID")

    @staticmethod
    def _require_states(value: frozenset[HostRequestState]) -> tuple[HostRequestState, ...]:
        if type(value) is not frozenset or not value:
            raise _state_conflict("expected request states must be a nonempty exact frozenset")
        if any(type(state) is not HostRequestState for state in value):
            raise _state_conflict("expected request states contain invalid values")
        return tuple(sorted(value, key=lambda state: state.value))

    @staticmethod
    def _validate_transition_values(
        *,
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None,
        result_json: str | None,
        error_json: str | None,
        resolution_json: str | None,
    ) -> None:
        if type(updated_at_ms) is not int or updated_at_ms < 0:
            raise _state_conflict("request update epoch is invalid")
        if terminal_at_ms is not None and (
            type(terminal_at_ms) is not int or terminal_at_ms < updated_at_ms or terminal_at_ms < 0
        ):
            raise _state_conflict("request terminal epoch is invalid")
        if (new_state in _TERMINAL_STATES) != (terminal_at_ms is not None):
            raise _state_conflict("request terminal state and epoch disagree")
        for text in (result_json, error_json, resolution_json):
            if text is not None:
                try:
                    _canonical_json_text(text)
                except (TypeError, ValueError, OverflowError, RecursionError) as error:
                    raise _state_conflict("request transition JSON is invalid") from error
        if new_state is HostRequestState.RESOLVED:
            valid = resolution_json is not None and result_json is None and error_json is None
        elif new_state is HostRequestState.COMPLETED:
            valid = error_json is None and resolution_json is None
        elif new_state is HostRequestState.REJECTED:
            valid = result_json is None and resolution_json is None
        elif new_state is HostRequestState.CANCELLED:
            valid = result_json is None and error_json is None and resolution_json is None
        elif new_state is HostRequestState.QUARANTINED:
            valid = result_json is None and resolution_json is None
        else:
            valid = result_json is None and error_json is None and resolution_json is None
        if not valid:
            raise _state_conflict("request transition JSON does not match target state")
