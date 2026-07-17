from __future__ import annotations

from dataclasses import dataclass
import re
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    JsonValue,
    model_validator,
)
from typing_extensions import Self

from cmo_agent_bridge.protocol.runtime import (
    RuntimeSnapshot,
    RuntimeVersion,
    Sha256,
    revalidate_runtime_snapshot,
)

if TYPE_CHECKING:
    from cmo_agent_bridge.operations.registry import FrozenInvocation, ResolvedInvocation


DeliveryKind = Literal["request", "cancel"]


class RequestBody(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    protocol: Literal["cmo-agent-bridge/1"]
    release_id: Sha256
    runtime_version: RuntimeVersion
    runtime_tag: str
    runtime_asset_sha256: Sha256
    expected_lineage_id: UUID | None
    expected_activation_id: UUID | None
    operation_manifest_sha256: Sha256
    operation: str
    arguments: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class PreparedDelivery:
    request_id: UUID
    delivery_id: UUID
    delivery_kind: DeliveryKind
    request_hash: str
    body_json: bytes


@dataclass(frozen=True, slots=True)
class AllowedDelivery:
    delivery_id: UUID
    delivery_kind: DeliveryKind


@dataclass(frozen=True, slots=True)
class ResponseExpectation:
    request_id: UUID
    allowed_deliveries: tuple[AllowedDelivery, ...]
    request_hash: str
    expected_lineage_id: UUID | None
    expected_activation_id: UUID | None
    status_bootstrap: bool
    activation_candidate: UUID | None
    runtime_snapshot: RuntimeSnapshot
    invocation: "FrozenInvocation"

    def __post_init__(self) -> None:
        from cmo_agent_bridge.operations.registry import FrozenInvocation, ResolvedInvocation

        if type(self.request_id) is not UUID:
            raise TypeError("response expectation request ID must be an exact UUID")
        if (
            type(self.request_hash) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.request_hash) is None
        ):
            raise ValueError("response expectation request hash must be lowercase SHA-256")
        if type(self.allowed_deliveries) is not tuple:
            raise TypeError("response expectation allowed deliveries must be a tuple")
        delivery_ids: set[UUID] = set()
        for allowed in self.allowed_deliveries:
            if type(allowed) is not AllowedDelivery:
                raise TypeError("response expectation requires exact AllowedDelivery values")
            if type(allowed.delivery_id) is not UUID:
                raise TypeError("allowed delivery ID must be an exact UUID")
            if type(allowed.delivery_kind) is not str or allowed.delivery_kind not in {
                "request",
                "cancel",
            }:
                raise ValueError("response expectation has invalid delivery kind")
            if allowed.delivery_id in delivery_ids:
                raise ValueError("response expectation delivery IDs must be unique")
            delivery_ids.add(allowed.delivery_id)
        for label, value in (
            ("expected lineage", self.expected_lineage_id),
            ("expected activation", self.expected_activation_id),
            ("activation candidate", self.activation_candidate),
        ):
            if value is not None and type(value) is not UUID:
                raise TypeError(f"response expectation {label} must be an exact UUID")
        if type(self.status_bootstrap) is not bool:
            raise TypeError("response expectation status_bootstrap must be an exact bool")
        if type(self.invocation) not in {FrozenInvocation, ResolvedInvocation}:
            raise TypeError("response expectation requires a frozen invocation")
        snapshot = revalidate_runtime_snapshot(self.runtime_snapshot)
        object.__setattr__(self, "runtime_snapshot", snapshot)


def validate_body_runtime_snapshot(body: RequestBody, runtime_snapshot: RuntimeSnapshot) -> None:
    expected = {
        "protocol": runtime_snapshot.protocol,
        "release_id": runtime_snapshot.release_id,
        "runtime_version": runtime_snapshot.runtime_version,
        "runtime_tag": runtime_snapshot.runtime_tag,
        "runtime_asset_sha256": runtime_snapshot.runtime_asset_sha256,
        "operation_manifest_sha256": runtime_snapshot.operation_manifest_sha256,
    }
    for field, value in expected.items():
        if getattr(body, field) != value:
            raise ValueError(f"body {field} does not match runtime_snapshot")


@dataclass(frozen=True, slots=True)
class ExchangeCommand:
    request_id: UUID
    body: RequestBody
    invocation: "ResolvedInvocation"
    runtime_snapshot: RuntimeSnapshot
    timeout: float
    # Reserved for the persisted mutation worker. The numeric timeout remains
    # part of the ordinary command contract; this explicit flag prevents the
    # unbounded lane from leaking ``None`` into every read/status caller.
    unbounded_wait: bool = False

    def __post_init__(self) -> None:
        if type(self.unbounded_wait) is not bool:
            raise TypeError("unbounded wait flag must be an exact bool")
        snapshot = revalidate_runtime_snapshot(self.runtime_snapshot)
        validate_body_runtime_snapshot(self.body, snapshot)


class CancelAckResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: UUID
    request_hash: Sha256
    original_delivery_id: UUID
    status: Literal["cancelled", "completed"]
    result: JsonValue | None = None

    @model_validator(mode="after")
    def validate_completed_result(self) -> Self:
        if self.status == "completed" and self.result is None:
            raise ValueError("completed cancel acknowledgement requires the original result")
        if self.status == "cancelled" and self.result is not None:
            raise ValueError("cancelled acknowledgement cannot carry a result")
        return self
