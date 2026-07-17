from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from enum import StrEnum
from typing import Annotated, Literal, cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    ValidationInfo,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.models import RequestBody, validate_body_runtime_snapshot
from cmo_agent_bridge.protocol.response_models import ResponseArtifact, Settlement
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, Sha256, revalidate_runtime_snapshot


class PendingPhase(StrEnum):
    PREPARED = "prepared"
    PUBLISHED = "published"
    CANCEL_PUBLISHED = "cancel_published"
    RESPONSE_ACCEPTED = "response_accepted"
    IDLE_PUBLISHED = "idle_published"
    QUARANTINED = "quarantined"


class HostRequestState(StrEnum):
    PREPARED = "prepared"
    PUBLISHED = "published"
    CANCEL_PUBLISHED = "cancel_published"
    RESPONSE_ACCEPTED = "response_accepted"
    IDLE_PUBLISHED = "idle_published"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    QUARANTINED = "quarantined"
    RESOLVED = "resolved"


def _reject_constant(value: str) -> object:
    raise ValueError(f"invalid JSON constant: {value}")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("JSON number is outside the finite range")
    return parsed


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON member: {key}")
        result[key] = value
    return result


def _parse_duplicate_free_json(raw: bytes | str) -> object:
    return cast(
        object,
        json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
            parse_float=_finite_float,
        ),
    )


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _canonical_model_bytes(value: BaseModel | None) -> bytes:
    return _canonical_json_bytes(None if value is None else value.model_dump(mode="json"))


def _normalize_nested_model(value: object) -> object:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="python", round_trip=True, warnings=False)
    return value


def _normalize_mapping_models(value: object, fields: tuple[str, ...]) -> object:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="python", round_trip=True, warnings=False)
    if not isinstance(value, Mapping):
        return value
    normalized = dict(cast(Mapping[str, object], value))
    for field in fields:
        if field not in normalized:
            continue
        nested = normalized.get(field)
        if isinstance(nested, tuple):
            normalized[field] = tuple(
                _normalize_nested_model(item) for item in cast(tuple[object, ...], nested)
            )
        elif isinstance(nested, list):
            normalized[field] = [
                _normalize_nested_model(item) for item in cast(list[object], nested)
            ]
        else:
            normalized[field] = _normalize_nested_model(nested)
    return normalized


class PendingJournalHeader(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    format: Literal["cmo-agent-bridge/pending-journal"]
    header_version: StrictInt
    root_key: Sha256
    required_release_id: Sha256

    @field_validator("format", "root_key", "required_release_id", mode="before")
    @classmethod
    def validate_exact_strings(cls, value: object) -> object:
        if type(value) is not str:
            raise ValueError("journal header strings must be exact built-in strings")
        return value

    @field_validator("header_version")
    @classmethod
    def validate_header_version(cls, value: int) -> int:
        if type(value) is not int or value != 1:
            raise ValueError("journal header version must be exact integer 1")
        return value


class DeliveryIntent(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    request_id: UUID
    delivery_id: UUID
    delivery_kind: Literal["request", "cancel"]
    original_request_delivery_id: UUID
    body_json: StrictStr
    request_hash: Sha256
    runtime_snapshot: RuntimeSnapshot
    result_schema_id: Sha256
    recovery_schema_id: Sha256 | None
    intended_at_ms: StrictInt = Field(ge=0)
    published_at_ms: StrictInt | None
    rendered_inbox_sha256: Sha256
    rendered_inbox_size_bytes: StrictInt = Field(ge=0)
    response_filename: StrictStr

    @model_validator(mode="before")
    @classmethod
    def revalidate_nested_snapshot(cls, value: object) -> object:
        return _normalize_mapping_models(value, ("runtime_snapshot",))

    @field_validator(
        "delivery_kind",
        "body_json",
        "request_hash",
        "result_schema_id",
        "recovery_schema_id",
        "rendered_inbox_sha256",
        "response_filename",
        mode="before",
    )
    @classmethod
    def validate_exact_strings(cls, value: object) -> object:
        if value is not None and type(value) is not str:
            raise ValueError("delivery intent strings must be exact built-in strings")
        return value

    @field_validator("published_at_ms")
    @classmethod
    def validate_publication_epoch(cls, value: int | None) -> int | None:
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError("publication epoch must be a non-negative exact integer")
        return value

    @field_validator("request_id", "delivery_id", "original_request_delivery_id")
    @classmethod
    def validate_exact_uuids(cls, value: UUID) -> UUID:
        if type(value) is not UUID:
            raise ValueError("delivery intent UUIDs must be exact UUID values")
        return value

    @model_validator(mode="after")
    def validate_intent(self) -> Self:
        snapshot = revalidate_runtime_snapshot(self.runtime_snapshot)
        object.__setattr__(self, "runtime_snapshot", snapshot)
        if self.published_at_ms is not None and self.published_at_ms < self.intended_at_ms:
            raise ValueError("publication epoch cannot precede intent epoch")
        if self.delivery_kind == "request":
            if self.original_request_delivery_id != self.delivery_id:
                raise ValueError("request intent must point to its own delivery")
        elif self.original_request_delivery_id == self.delivery_id:
            raise ValueError("cancel intent must be distinct from its original request delivery")
        expected_filename = f"CMOAgentBridge_Response_{self.request_id}.inst"
        if self.response_filename != expected_filename:
            raise ValueError("response filename does not match the request ID")
        try:
            self.body_json.encode("utf-8")
        except UnicodeEncodeError as error:
            raise ValueError("delivery body must be valid UTF-8") from error
        return self


class PendingExchange(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    request_id: UUID
    request_hash: Sha256
    operation: StrictStr
    effective_class: OperationClass
    body_json: StrictStr
    runtime_snapshot: RuntimeSnapshot
    result_schema_id: Sha256
    recovery_schema_id: Sha256 | None
    expected_lineage_id: UUID | None
    expected_activation_id: UUID | None
    delivery_intents: Annotated[tuple[DeliveryIntent, ...], Field(min_length=1)]
    response_artifact: ResponseArtifact | None
    settlement: Settlement | None
    original_target_request_id: UUID | None
    original_target_request_hash: Sha256 | None
    revision: StrictInt = Field(ge=0)
    state: PendingPhase
    created_at_ms: StrictInt = Field(ge=0)
    updated_at_ms: StrictInt = Field(ge=0)

    @model_validator(mode="before")
    @classmethod
    def revalidate_nested_models(cls, value: object, info: ValidationInfo) -> object:
        normalized = _normalize_mapping_models(
            value,
            ("runtime_snapshot", "delivery_intents", "response_artifact", "settlement"),
        )
        if info.mode == "json" and isinstance(normalized, Mapping):
            mapped = dict(cast(Mapping[str, object], normalized))
            intents = mapped.get("delivery_intents")
            if isinstance(intents, list):
                mapped["delivery_intents"] = tuple(cast(list[object], intents))
            return mapped
        return normalized

    @field_validator(
        "request_hash",
        "operation",
        "body_json",
        "result_schema_id",
        "recovery_schema_id",
        "original_target_request_hash",
        mode="before",
    )
    @classmethod
    def validate_exact_strings(cls, value: object) -> object:
        if value is not None and type(value) is not str:
            raise ValueError("pending exchange strings must be exact built-in strings")
        return value

    @field_validator(
        "request_id",
        "expected_lineage_id",
        "expected_activation_id",
        "original_target_request_id",
    )
    @classmethod
    def validate_exact_uuids(cls, value: UUID | None) -> UUID | None:
        if value is not None and type(value) is not UUID:
            raise ValueError("pending exchange UUIDs must be exact UUID values")
        return value

    @model_validator(mode="after")
    def validate_exchange(self) -> Self:
        snapshot = revalidate_runtime_snapshot(self.runtime_snapshot)
        object.__setattr__(self, "runtime_snapshot", snapshot)
        if self.updated_at_ms < self.created_at_ms:
            raise ValueError("updated epoch cannot precede creation epoch")
        if (self.original_target_request_id is None) != (self.original_target_request_hash is None):
            raise ValueError("original target ID and hash must be both null or both present")
        if self.original_target_request_id is None:
            if self.effective_class not in {
                OperationClass.MUTATION,
                OperationClass.DESTRUCTIVE,
            }:
                raise ValueError("journal original requires a concrete mutating class")
        else:
            if self.effective_class is not OperationClass.RECONCILE:
                raise ValueError("an original target is valid only for a reconcile exchange")
            if self.original_target_request_id == self.request_id:
                raise ValueError("reconcile attempt request ID must differ from its target")

        body = self._validate_body(snapshot)
        intents = tuple(
            DeliveryIntent.model_validate(intent.model_dump(mode="python"))
            for intent in self.delivery_intents
        )
        object.__setattr__(self, "delivery_intents", intents)
        self._validate_intents(intents)
        self._validate_artifact_and_phase(intents)
        if body.expected_lineage_id != self.expected_lineage_id:
            raise ValueError("body lineage does not match pending exchange")
        if body.expected_activation_id != self.expected_activation_id:
            raise ValueError("body activation does not match pending exchange")
        return self

    def _validate_body(self, snapshot: RuntimeSnapshot) -> RequestBody:
        try:
            raw = self.body_json.encode("utf-8")
            parsed = _parse_duplicate_free_json(raw)
            body = RequestBody.model_validate(parsed)
            canonical = canonical_body_bytes(body)
        except (UnicodeError, TypeError, ValueError, OverflowError, RecursionError) as error:
            raise ValueError("pending request body is invalid") from error
        if canonical != raw:
            raise ValueError("pending request body is not canonical JSON")
        if hashlib.sha256(raw).hexdigest() != self.request_hash:
            raise ValueError("pending request hash does not match body")
        if body.operation != self.operation:
            raise ValueError("pending operation does not match body")
        try:
            validate_body_runtime_snapshot(body, snapshot)
        except ValueError as error:
            raise ValueError("pending body runtime identity does not match snapshot") from error
        return body

    def _validate_intents(self, intents: tuple[DeliveryIntent, ...]) -> None:
        delivery_ids = [intent.delivery_id for intent in intents]
        if len(delivery_ids) != len(set(delivery_ids)):
            raise ValueError("delivery IDs must be unique")
        request_intents = tuple(intent for intent in intents if intent.delivery_kind == "request")
        cancel_intents = tuple(intent for intent in intents if intent.delivery_kind == "cancel")
        if len(request_intents) != 1 or len(cancel_intents) > 1:
            raise ValueError("exchange requires one request intent and at most one cancel intent")
        request_delivery_id = request_intents[0].delivery_id
        if cancel_intents and cancel_intents[0].original_request_delivery_id != request_delivery_id:
            raise ValueError("cancel intent does not point to the original request delivery")
        for intent in intents:
            expected = (
                intent.request_id == self.request_id
                and intent.request_hash == self.request_hash
                and intent.body_json == self.body_json
                and intent.runtime_snapshot == self.runtime_snapshot
                and intent.result_schema_id == self.result_schema_id
                and intent.recovery_schema_id == self.recovery_schema_id
            )
            if not expected:
                raise ValueError("delivery identity does not match pending exchange")

    def _validate_artifact_and_phase(self, intents: tuple[DeliveryIntent, ...]) -> None:
        artifact = self.response_artifact
        if artifact is None:
            if self.settlement is not None:
                raise ValueError("settlement requires an accepted response artifact")
        else:
            accepted_settlement = artifact.accepted_response.settlement
            if _canonical_model_bytes(accepted_settlement) != _canonical_model_bytes(
                self.settlement
            ):
                raise ValueError("persisted settlement does not match accepted response")
            envelope = artifact.accepted_response.envelope
            if envelope.request_id != self.request_id or envelope.request_hash != self.request_hash:
                raise ValueError("response artifact does not match exchange request")
            matching = tuple(
                intent for intent in intents if intent.delivery_id == envelope.delivery_id
            )
            if len(matching) != 1:
                raise ValueError("response artifact delivery is not an exchange intent")
            if matching[0].delivery_kind != artifact.accepted_response.delivery_kind:
                raise ValueError("response artifact delivery kind does not match intent")
            if envelope.scenario_lineage_id != self.expected_lineage_id:
                raise ValueError("response artifact lineage does not match exchange")
            if envelope.activation_id != self.expected_activation_id:
                raise ValueError("response artifact activation does not match exchange")
            if matching[0].published_at_ms is None:
                raise ValueError("responding delivery must be durably published")

        requests = tuple(intent for intent in intents if intent.delivery_kind == "request")
        cancels = tuple(intent for intent in intents if intent.delivery_kind == "cancel")
        request_published = requests[0].published_at_ms is not None
        if self.state is PendingPhase.PREPARED:
            if len(intents) != 1 or request_published or artifact is not None:
                raise ValueError("prepared phase requires one unpublished request and no response")
        elif self.state is PendingPhase.PUBLISHED:
            if len(intents) != 1 or not request_published or artifact is not None:
                raise ValueError("published phase requires one published request and no response")
        elif self.state is PendingPhase.CANCEL_PUBLISHED:
            if len(cancels) != 1 or not request_published or artifact is not None:
                raise ValueError("cancel phase requires published request and one cancel intent")
        elif self.state is PendingPhase.RESPONSE_ACCEPTED:
            if not request_published or artifact is None:
                raise ValueError(
                    "response phase requires a published request and accepted artifact"
                )
        elif self.state is PendingPhase.IDLE_PUBLISHED:
            if not request_published or artifact is None or self.settlement is None:
                raise ValueError("idle phase requires a terminal accepted settlement")
        elif self.state is PendingPhase.QUARANTINED and not request_published:
            raise ValueError("quarantine requires a published request")


class PendingJournal(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    header: PendingJournalHeader
    original: PendingExchange
    reconcile_attempt: PendingExchange | None

    @model_validator(mode="before")
    @classmethod
    def revalidate_nested_models(cls, value: object) -> object:
        return _normalize_mapping_models(value, ("header", "original", "reconcile_attempt"))

    @model_validator(mode="after")
    def validate_journal(self) -> Self:
        header = PendingJournalHeader.model_validate(self.header.model_dump(mode="python"))
        original = PendingExchange.model_validate(self.original.model_dump(mode="python"))
        attempt = (
            None
            if self.reconcile_attempt is None
            else PendingExchange.model_validate(self.reconcile_attempt.model_dump(mode="python"))
        )
        object.__setattr__(self, "header", header)
        object.__setattr__(self, "original", original)
        object.__setattr__(self, "reconcile_attempt", attempt)
        if header.required_release_id != original.runtime_snapshot.release_id:
            raise ValueError("journal release does not match original snapshot")
        if original.original_target_request_id is not None:
            raise ValueError("journal original cannot target another request")
        if original.effective_class not in {
            OperationClass.MUTATION,
            OperationClass.DESTRUCTIVE,
        }:
            raise ValueError("journal original must be a mutation or destructive exchange")
        if attempt is not None:
            if attempt.runtime_snapshot != original.runtime_snapshot:
                raise ValueError("reconcile attempt snapshot does not match original")
            if attempt.expected_lineage_id != original.expected_lineage_id:
                raise ValueError("reconcile attempt lineage does not match original")
            if attempt.request_id == original.request_id:
                raise ValueError("reconcile attempt request ID must be distinct")
            if attempt.original_target_request_id != original.request_id:
                raise ValueError("reconcile attempt target ID does not match original")
            if attempt.original_target_request_hash != original.request_hash:
                raise ValueError("reconcile attempt target hash does not match original")
        return self
