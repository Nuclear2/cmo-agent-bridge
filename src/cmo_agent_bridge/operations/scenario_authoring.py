from __future__ import annotations

import re
from datetime import datetime
from typing import Annotated, Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    field_validator,
    model_validator,
)
from typing_extensions import Self


NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]
ScenarioDateTime = Annotated[
    str,
    StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"),
]


class AuthoringStrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ScenarioWeatherGetArgs(AuthoringStrictModel):
    pass


class ScenarioWeatherSetArgs(AuthoringStrictModel):
    temperature_c: float
    rainfall: float = Field(ge=0, le=50)
    undercloud_fraction: float = Field(ge=0, le=1)
    sea_state: int = Field(ge=0, le=9)


class ScenarioTitleSetArgs(AuthoringStrictModel):
    title: NonEmptyStr


class ScenarioTimelineSetArgs(AuthoringStrictModel):
    current_time: ScenarioDateTime | None = None
    start_time: ScenarioDateTime | None = None
    duration: Annotated[
        str,
        StringConstraints(pattern=r"^\d+:\d{1,2}:\d{1,2}$"),
    ] | None = None

    @field_validator("current_time", "start_time")
    @classmethod
    def validate_datetime_value(cls, value: str | None) -> str | None:
        if value is not None:
            datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
        return value

    @field_validator("duration")
    @classmethod
    def validate_duration_value(cls, value: str | None) -> str | None:
        if value is not None:
            _days, hours, minutes = (int(part) for part in value.split(":"))
            if hours > 23 or minutes > 59:
                raise ValueError("duration must use days:hours:minutes")
        return value

    @model_validator(mode="after")
    def validate_update(self) -> Self:
        if self.current_time is None and self.start_time is None and self.duration is None:
            raise ValueError("at least one scenario timeline update is required")
        return self


class SideAddArgs(AuthoringStrictModel):
    name: NonEmptyStr


class SideOptionsSetArgs(AuthoringStrictModel):
    side_guid: NonEmptyStr
    awareness: NonEmptyStr | int | None = None
    proficiency: NonEmptyStr | int | None = None
    auto_track_civilians: bool | None = None
    collective_responsibility: bool | None = None
    computer_controlled_only: bool | None = None

    @model_validator(mode="after")
    def validate_update(self) -> Self:
        if not any(
            value is not None
            for value in (
                self.awareness,
                self.proficiency,
                self.auto_track_civilians,
                self.collective_responsibility,
                self.computer_controlled_only,
            )
        ):
            raise ValueError("at least one side option update is required")
        return self


class SidePostureSetArgs(AuthoringStrictModel):
    side_a_guid: NonEmptyStr
    side_b_guid: NonEmptyStr
    posture: Literal["F", "H", "N", "U"]


class ScoreSetArgs(AuthoringStrictModel):
    side: NonEmptyStr
    score: int
    reason: NonEmptyStr


class EventListArgs(AuthoringStrictModel):
    level: Literal[0, 1, 2, 3, 4] = 4


class EventGetArgs(AuthoringStrictModel):
    event_id_or_name: NonEmptyStr
    level: Literal[0, 1, 2, 3, 4] = 4


class EventSetArgs(AuthoringStrictModel):
    mode: Literal["add", "update", "remove"]
    event_id_or_name: NonEmptyStr
    new_description: NonEmptyStr | None = None
    active: bool | None = None
    shown: bool | None = None
    repeatable: bool | None = None
    probability: int | None = Field(default=None, ge=0, le=100)

    @model_validator(mode="after")
    def validate_mode(self) -> Self:
        fields = (
            self.new_description,
            self.active,
            self.shown,
            self.repeatable,
            self.probability,
        )
        if self.mode == "remove" and any(value is not None for value in fields):
            raise ValueError("event removal forbids update fields")
        if self.mode == "update" and not any(value is not None for value in fields):
            raise ValueError("event update requires at least one changed field")
        return self


class EventComponentSetArgs(AuthoringStrictModel):
    mode: Literal["list", "add", "update", "remove"]
    component_id_or_name: NonEmptyStr
    component_type: NonEmptyStr | None = None
    new_description: NonEmptyStr | None = None
    parameters_json: str = "{}"

    @model_validator(mode="after")
    def validate_mode(self) -> Self:
        if self.mode == "add" and self.component_type is None:
            raise ValueError("component add requires component_type")
        if self.mode != "add" and self.component_type is not None:
            raise ValueError("component_type is allowed only for add")
        if self.mode in {"list", "remove"} and self.new_description is not None:
            raise ValueError("list and remove forbid new_description")
        if self.mode in {"list", "remove"} and self.parameters_json != "{}":
            raise ValueError("list and remove forbid component parameters")
        return self

    @model_validator(mode="after")
    def validate_parameters_json(self) -> Self:
        import json

        try:
            value = cast(JsonValue, json.loads(self.parameters_json))
        except (TypeError, ValueError) as error:
            raise ValueError("parameters_json must contain valid JSON") from error
        if not isinstance(value, dict):
            raise ValueError("parameters_json must contain a JSON object")
        if _contains_json_null(value):
            raise ValueError("parameters_json must not contain JSON null")
        return self


class EventComponentLinkArgs(AuthoringStrictModel):
    mode: Literal["add", "remove", "replace"]
    event_id_or_name: NonEmptyStr
    component_id_or_name: NonEmptyStr
    replacement_id_or_name: NonEmptyStr | None = None

    @model_validator(mode="after")
    def validate_replacement(self) -> Self:
        if (self.mode == "replace") != (self.replacement_id_or_name is not None):
            raise ValueError("replacement_id_or_name is required only for replace")
        return self


class SpecialActionAddArgs(AuthoringStrictModel):
    side_guid: NonEmptyStr
    name: NonEmptyStr
    description: str = ""
    active: bool = False
    repeatable: bool = False
    script_text: str


class SpecialActionSetArgs(AuthoringStrictModel):
    side_guid: NonEmptyStr
    action_id_or_name: NonEmptyStr
    mode: Literal["update", "remove"] = "update"
    new_name: NonEmptyStr | None = None
    description: str | None = None
    active: bool | None = None
    repeatable: bool | None = None
    script_text: str | None = None

    @model_validator(mode="after")
    def validate_update(self) -> Self:
        fields = (
            self.new_name,
            self.description,
            self.active,
            self.repeatable,
            self.script_text,
        )
        if self.mode == "remove" and any(value is not None for value in fields):
            raise ValueError("special-action removal forbids update fields")
        if self.mode == "update" and not any(value is not None for value in fields):
            raise ValueError("special-action update requires at least one changed field")
        return self


class ScenarioWeatherResult(AuthoringStrictModel):
    temperature_c: float
    rainfall: float
    undercloud_fraction: float
    sea_state: int


class AuthoringDataResult(AuthoringStrictModel):
    operation: NonEmptyStr
    accepted: bool
    data: JsonValue


def normalize_script_newlines(script: str) -> str:
    """Return the CRLF form expected by CMO's event and special-action editors."""

    return re.sub(r"\r\n?|\n", "\r\n", script)


def _contains_json_null(value: JsonValue) -> bool:
    if value is None:
        return True
    if isinstance(value, dict):
        return any(_contains_json_null(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_json_null(item) for item in value)
    return False
