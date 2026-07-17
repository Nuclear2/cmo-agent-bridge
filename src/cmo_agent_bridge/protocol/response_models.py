from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Annotated, Literal, cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from cmo_agent_bridge.errors import ErrorCode
from cmo_agent_bridge.protocol.models import CancelAckResult, DeliveryKind
from cmo_agent_bridge.protocol.runtime import Sha256


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _canonical_json_equal(left: object, right: object) -> bool:
    return _canonical_json_bytes(left) == _canonical_json_bytes(right)


class MutationNotStartedEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: StrictInt
    stage: Literal["dispatch_validation", "handler_preflight"]
    request_id: UUID
    request_hash: Sha256
    operation: StrictStr
    mutation_barrier_written: StrictBool
    execute_started: StrictBool

    @field_validator("request_hash", mode="before")
    @classmethod
    def validate_exact_request_hash_string(cls, value: object) -> object:
        if type(value) is not str:
            raise ValueError("mutation-not-started request hash must be an exact string")
        return value

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("mutation-not-started evidence schema version must be 1")
        return value

    @field_validator("mutation_barrier_written", "execute_started")
    @classmethod
    def validate_false_flag(cls, value: bool) -> bool:
        if value is not False:
            raise ValueError("mutation-not-started evidence flags must be false")
        return value


class ResponseError(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    code: ErrorCode
    message: StrictStr
    details: dict[str, JsonValue]
    mutation_not_started: MutationNotStartedEvidence | None = None

    @field_validator("details")
    @classmethod
    def reject_reserved_evidence_detail(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        if "mutation_not_started" in value:
            raise ValueError("details.mutation_not_started is reserved")
        return value


class ResponseCorrelation(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    request_id: UUID
    delivery_id: UUID
    request_hash: Sha256


class ResponseEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    protocol: Literal["cmo-agent-bridge/1"]
    request_id: UUID
    delivery_id: UUID
    request_hash: Sha256
    ok: StrictBool
    result: JsonValue
    error: ResponseError | None
    scenario_time: StrictStr
    scenario_lineage_id: UUID
    activation_id: UUID
    operation_manifest_sha256: Sha256
    bridge_version: StrictStr
    runtime_tag: StrictStr
    runtime_asset_sha256: Sha256
    release_id: Sha256

    @model_validator(mode="after")
    def validate_result_or_error(self) -> Self:
        if self.ok and (self.result is None or self.error is not None):
            raise ValueError("successful response requires result and forbids error")
        if not self.ok and (self.result is not None or self.error is None):
            raise ValueError("failed response requires error and forbids result")
        return self


class CompletedSettlement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    state: Literal["completed"]
    result: JsonValue

    @model_validator(mode="after")
    def reject_null_result(self) -> Self:
        if self.result is None:
            raise ValueError("completed settlement requires a non-null result")
        return self


class CancelledSettlement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    state: Literal["cancelled"]


class RejectedSettlement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    state: Literal["rejected"]
    error: ResponseError


Settlement = Annotated[
    CompletedSettlement | CancelledSettlement | RejectedSettlement,
    Field(discriminator="state"),
]


class AcceptedResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")

    envelope: ResponseEnvelope
    delivery_kind: DeliveryKind
    settlement: Settlement | None
    cancel_ack: CancelAckResult | None

    @model_validator(mode="before")
    @classmethod
    def revalidate_nested_protocol_models(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            return value
        normalized = dict(cast(Mapping[str, object], value))
        envelope = normalized.get("envelope")
        if isinstance(envelope, ResponseEnvelope):
            normalized["envelope"] = envelope.model_dump(
                mode="python", round_trip=True, warnings=False
            )
        settlement = normalized.get("settlement")
        if isinstance(
            settlement,
            CompletedSettlement | CancelledSettlement | RejectedSettlement,
        ):
            normalized["settlement"] = settlement.model_dump(
                mode="python", round_trip=True, warnings=False
            )
        acknowledgement = normalized.get("cancel_ack")
        if isinstance(acknowledgement, CancelAckResult):
            normalized["cancel_ack"] = acknowledgement.model_dump(
                mode="python", round_trip=True, warnings=False
            )
        return normalized

    @model_validator(mode="after")
    def validate_shape_and_canonical_identity(self) -> Self:
        settlement = self.settlement
        acknowledgement = self.cancel_ack

        try:
            _canonical_json_bytes(
                {
                    "cancel_ack": (
                        None if acknowledgement is None else acknowledgement.model_dump(mode="json")
                    ),
                    "delivery_kind": self.delivery_kind,
                    "envelope": self.envelope.model_dump(mode="json"),
                    "settlement": (
                        None if settlement is None else settlement.model_dump(mode="json")
                    ),
                }
            )
        except (TypeError, ValueError, OverflowError, RecursionError) as error:
            raise ValueError("accepted response must be canonical UTF-8 JSON") from error

        if self.delivery_kind == "request":
            if acknowledgement is not None:
                raise ValueError("request delivery cannot carry a cancel acknowledgement")
            if self.envelope.ok:
                if not isinstance(settlement, CompletedSettlement):
                    raise ValueError("successful request requires completed settlement")
                if not _canonical_json_equal(self.envelope.result, settlement.result):
                    raise ValueError("completed settlement result does not match envelope")
                return self
            if settlement is not None and not isinstance(settlement, RejectedSettlement):
                raise ValueError("failed request permits only rejected or no settlement")
            if isinstance(settlement, RejectedSettlement):
                envelope_error = self.envelope.error
                if envelope_error is None or not _canonical_json_equal(
                    envelope_error.model_dump(mode="json"),
                    settlement.error.model_dump(mode="json"),
                ):
                    raise ValueError("rejected settlement error does not match envelope")
            return self

        if not self.envelope.ok:
            if settlement is not None or acknowledgement is not None:
                raise ValueError("failed cancel cannot carry settlement or acknowledgement")
            return self

        if acknowledgement is None:
            raise ValueError("successful cancel requires an acknowledgement")
        if not _canonical_json_equal(
            self.envelope.result,
            acknowledgement.model_dump(mode="json"),
        ):
            raise ValueError("cancel acknowledgement does not match envelope result")
        if acknowledgement.status == "cancelled":
            if not isinstance(settlement, CancelledSettlement):
                raise ValueError("cancelled acknowledgement requires cancelled settlement")
            return self
        if not isinstance(settlement, CompletedSettlement):
            raise ValueError("completed acknowledgement requires completed settlement")
        if not _canonical_json_equal(acknowledgement.result, settlement.result):
            raise ValueError("completed cancel result does not match settlement")
        return self

    @property
    def ok(self) -> bool:
        return self.envelope.ok

    @property
    def result(self) -> JsonValue | None:
        if isinstance(self.settlement, CompletedSettlement):
            return self.settlement.result
        return None

    @property
    def error(self) -> ResponseError | None:
        return self.envelope.error


class ResponseArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")

    filename: StrictStr
    sha256: Sha256
    size_bytes: StrictInt = Field(ge=0)
    accepted_at_ms: StrictInt = Field(ge=0)
    accepted_response: AcceptedResponse

    @model_validator(mode="before")
    @classmethod
    def revalidate_accepted_response(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            return value
        normalized = dict(cast(Mapping[str, object], value))
        accepted = normalized.get("accepted_response")
        if isinstance(accepted, AcceptedResponse):
            normalized["accepted_response"] = accepted.model_dump(
                mode="python", round_trip=True, warnings=False
            )
        return normalized

    @field_validator("sha256", mode="before")
    @classmethod
    def validate_exact_sha256_string(cls, value: object) -> object:
        if type(value) is not str:
            raise ValueError("response artifact SHA-256 must be an exact string")
        return value

    @model_validator(mode="after")
    def validate_exact_filename(self) -> Self:
        expected = f"CMOAgentBridge_Response_{self.accepted_response.envelope.request_id}.inst"
        if self.filename != expected:
            raise ValueError("response artifact filename does not match request ID")
        return self
