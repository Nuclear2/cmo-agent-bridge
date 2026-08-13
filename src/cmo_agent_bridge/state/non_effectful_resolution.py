from __future__ import annotations

import json
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr

from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.protocol.runtime import Sha256


NON_EFFECTFUL_HOST_RESOLUTION_FORMAT = (
    "cmo-agent-bridge/non-effectful-host-resolution/1"
)


class NonEffectfulFailureEvidence(BaseModel):
    """Diagnostic evidence only; it never asserts whether CMO consumed a request."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
    )

    phase: StrictStr = Field(min_length=1)
    exception_type: StrictStr = Field(min_length=1)
    winerror: StrictInt | None = Field(default=None, ge=0)


class NonEffectfulHostResolutionMarker(BaseModel):
    """Terminal marker for a status/read request whose outcome has no CMO effect."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
    )

    format: Literal["cmo-agent-bridge/non-effectful-host-resolution/1"]
    mode: Literal["non_effectful_outcome_irrelevant"]
    disposition: Literal["abandoned"]
    root_key: Sha256
    request_id: UUID
    request_hash: Sha256
    operation: StrictStr = Field(min_length=1)
    operation_class: Literal[OperationClass.STATUS, OperationClass.READ]
    reason: StrictStr = Field(min_length=1)
    resolved_at_ms: StrictInt = Field(ge=0)
    failure: NonEffectfulFailureEvidence | None = None


def canonical_non_effectful_host_resolution(
    marker: NonEffectfulHostResolutionMarker,
) -> str:
    candidate = NonEffectfulHostResolutionMarker.model_validate(
        marker.model_dump(mode="python", round_trip=True, warnings=False)
    )
    return json.dumps(
        candidate.model_dump(mode="json", warnings="error"),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def parse_non_effectful_host_resolution(
    value: str,
) -> NonEffectfulHostResolutionMarker:
    if type(value) is not str:
        raise ValueError("non-effectful Host resolution must be an exact string")
    marker = NonEffectfulHostResolutionMarker.model_validate_json(value, strict=True)
    if canonical_non_effectful_host_resolution(marker) != value:
        raise ValueError("non-effectful Host resolution is not canonical JSON")
    return marker
