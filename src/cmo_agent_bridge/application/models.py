from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, JsonValue, model_validator
from typing_extensions import Self


class InvocationOutcome(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
    )

    protocol: Literal["cmo-agent-bridge/1"]
    request_id: UUID | None
    ok: bool
    result: JsonValue | None
    error: dict[str, JsonValue] | None

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.ok:
            if self.error is not None:
                raise ValueError("successful outcome cannot contain an error")
            return self
        if self.result is not None:
            raise ValueError("failed outcome cannot contain a result")
        if self.error is None:
            raise ValueError("failed outcome requires an error")
        return self
