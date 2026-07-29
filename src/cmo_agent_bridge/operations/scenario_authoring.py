from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Annotated, Literal, TypeAlias, cast

from pydantic import (
    AfterValidator,
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictBool,
    StrictFloat,
    StrictInt,
    StringConstraints,
    WithJsonSchema,
    field_validator,
    model_validator,
)
from typing_extensions import Self


NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]
ScenarioDateTime = Annotated[
    str,
    StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"),
]


def _validate_event_duration_text(value: str) -> str:
    _days, hours, minutes, seconds = (int(part) for part in value.split(":"))
    if hours > 23 or minutes > 59 or seconds > 59:
        raise ValueError("event duration must use days:hours:minutes:seconds")
    return value


def _validate_event_cargo_type_code(value: int) -> int:
    if value not in {0, 1000, 2000, 3000, 4000, 5000}:
        raise ValueError("cargo TargetType must be one of 0, 1000, 2000, 3000, 4000, 5000")
    return value


StrictAwarenessCode: TypeAlias = Annotated[
    StrictInt,
    Field(ge=-1, le=3),
    WithJsonSchema({"type": "integer", "enum": [-1, 0, 1, 2, 3]}),
]
StrictEventLevel: TypeAlias = Annotated[
    StrictInt,
    Field(ge=0, le=4),
    WithJsonSchema({"type": "integer", "enum": [0, 1, 2, 3, 4]}),
]
StrictProficiencyCode: TypeAlias = Annotated[
    StrictInt,
    Field(ge=0, le=4),
    WithJsonSchema({"type": "integer", "enum": [0, 1, 2, 3, 4]}),
]
SideAwarenessValue: TypeAlias = Annotated[
    Literal["Blind", "Normal", "AutoSideID", "AutoSideAndUnitID", "Omniscient"]
    | StrictAwarenessCode,
    Field(
        description=(
            "CMO side awareness: Blind/-1, Normal/0, AutoSideID/1, "
            "AutoSideAndUnitID/2, or Omniscient/3."
        )
    ),
]
SideProficiencyValue: TypeAlias = Annotated[
    Literal["Novice", "Cadet", "Regular", "Veteran", "Ace"] | StrictProficiencyCode,
    Field(description=("CMO side proficiency: Novice/0, Cadet/1, Regular/2, Veteran/3, or Ace/4.")),
]
EventTriggerType: TypeAlias = Literal[
    "Points",
    "RandomTime",
    "RegularTime",
    "ScenEnded",
    "ScenLoaded",
    "Time",
    "UnitDamaged",
    "UnitDestroyed",
    "UnitDetected",
    "UnitEmissions",
    "UnitEntersArea",
    "UnitRemainsInArea",
    "UnitBaseStatus",
    "UnitCargoMoved",
]
EventConditionType: TypeAlias = Literal["LuaScript", "ScenHasStarted", "SidePosture"]
EventActionType: TypeAlias = Literal[
    "ChangeMissionStatus",
    "EndScenario",
    "LuaScript",
    "Message",
    "Points",
    "TeleportInArea",
]
EventComponentType: TypeAlias = EventTriggerType | EventConditionType | EventActionType
StrictNonNegativeInt: TypeAlias = Annotated[StrictInt, Field(ge=0)]
StrictPositiveInt: TypeAlias = Annotated[StrictInt, Field(ge=1)]
StrictNumber: TypeAlias = StrictInt | StrictFloat
StrictPercent: TypeAlias = Annotated[StrictNumber, Field(ge=0, le=100)]
EventArea: TypeAlias = Annotated[list[NonEmptyStr], Field(min_length=1)]
EventIdentifierCode: TypeAlias = StrictNonNegativeInt | Annotated[
    str,
    StringConstraints(pattern=r"^\d+$"),
]
EventDurationText: TypeAlias = Annotated[
    str,
    StringConstraints(pattern=r"^\d+:\d{1,2}:\d{1,2}:\d{1,2}$"),
    AfterValidator(_validate_event_duration_text),
]
EventDuration: TypeAlias = StrictNonNegativeInt | EventDurationText
EventReachDirection: TypeAlias = Annotated[
    StrictInt,
    Field(ge=0, le=2),
    WithJsonSchema({"type": "integer", "enum": [0, 1, 2]}),
]
EventContactIdStatusCode: TypeAlias = Annotated[
    StrictInt,
    Field(ge=0, le=4),
    WithJsonSchema({"type": "integer", "enum": [0, 1, 2, 3, 4]}),
]
EventContactIdStatus: TypeAlias = (
    Literal["Unknown", "KnownDomain", "KnownType", "KnownClass", "PreciseID"]
    | EventContactIdStatusCode
)
EventUnitTargetTypeCode: TypeAlias = Annotated[
    StrictInt,
    Field(ge=1, le=7),
    WithJsonSchema({"type": "integer", "enum": [1, 2, 3, 4, 5, 6, 7]}),
]
EventUnitTargetType: TypeAlias = (
    Literal["Aircraft", "Ship", "Submarine", "Facility", "Aimpoint", "Weapon", "Satellite"]
    | EventUnitTargetTypeCode
)
EventCargoTargetTypeCode: TypeAlias = Annotated[
    StrictInt,
    AfterValidator(_validate_event_cargo_type_code),
    WithJsonSchema({"type": "integer", "enum": [0, 1000, 2000, 3000, 4000, 5000]}),
]
EventCargoTargetType: TypeAlias = (
    Literal["NoCargo", "Personnel", "SmallCargo", "MediumCargo", "LargeCargo", "VLargeCargo"]
    | EventCargoTargetTypeCode
)
EventTargetPostureCode: TypeAlias = Annotated[
    StrictInt,
    Field(ge=0, le=3),
    WithJsonSchema({"type": "integer", "enum": [0, 1, 2, 3]}),
]
EventTargetPosture: TypeAlias = (
    Literal["Neutral", "Friendly", "Unfriendly", "Hostile"] | EventTargetPostureCode
)
EventMissionStatus: TypeAlias = Annotated[
    StrictInt,
    Field(ge=0, le=1),
    WithJsonSchema({"type": "integer", "enum": [0, 1]}),
]

_EVENT_COMPONENT_TYPES_BY_KIND: dict[
    Literal["trigger", "condition", "action"],
    frozenset[str],
] = {
    "trigger": frozenset(
        {
            "Points",
            "RandomTime",
            "RegularTime",
            "ScenEnded",
            "ScenLoaded",
            "Time",
            "UnitDamaged",
            "UnitDestroyed",
            "UnitDetected",
            "UnitEmissions",
            "UnitEntersArea",
            "UnitRemainsInArea",
            "UnitBaseStatus",
            "UnitCargoMoved",
        }
    ),
    "condition": frozenset({"LuaScript", "ScenHasStarted", "SidePosture"}),
    "action": frozenset(
        {
            "ChangeMissionStatus",
            "EndScenario",
            "LuaScript",
            "Message",
            "Points",
            "TeleportInArea",
        }
    ),
}


def validate_event_component_type(
    kind: Literal["trigger", "condition", "action"],
    component_type: EventComponentType,
) -> EventComponentType:
    if component_type not in _EVENT_COMPONENT_TYPES_BY_KIND[kind]:
        raise ValueError(f"{component_type} is not a canonical CMO {kind} component type")
    return component_type


class AuthoringStrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        allow_inf_nan=False,
    )


class EventParameterModel(AuthoringStrictModel):
    """Official type-specific fields carried inside an event component descriptor."""

    @model_validator(mode="after")
    def reject_explicit_null(self) -> Self:
        if any(getattr(self, field_name) is None for field_name in self.model_fields_set):
            raise ValueError("event component parameters must omit fields instead of using null")
        return self


class EventTargetFilter(EventParameterModel):
    target_side: NonEmptyStr | None = Field(default=None, alias="TargetSide")
    target_type: EventUnitTargetType | None = Field(default=None, alias="TargetType")
    target_subtype: EventIdentifierCode | None = Field(default=None, alias="TargetSubType")
    specific_unit_class: EventIdentifierCode | None = Field(
        default=None,
        alias="SpecificUnitClass",
    )
    specific_unit_id: NonEmptyStr | None = Field(
        default=None,
        validation_alias=AliasChoices("SpecificUnitID", "SpecificUnit"),
        serialization_alias="SpecificUnitID",
    )

    @model_validator(mode="after")
    def validate_selector(self) -> Self:
        if self.specific_unit_id is not None:
            if any(
                value is not None
                for value in (
                    self.target_type,
                    self.target_subtype,
                    self.specific_unit_class,
                )
            ):
                raise ValueError(
                    "TargetFilter SpecificUnitID cannot be combined with "
                    "TargetType, TargetSubType, or SpecificUnitClass"
                )
            return self
        if self.target_side is None or self.target_type is None:
            raise ValueError(
                "TargetFilter requires TargetSide and TargetType unless "
                "SpecificUnitID selects one unit"
            )
        return self


class EventCargoFilter(EventParameterModel):
    target_type: EventCargoTargetType | None = Field(default=None, alias="TargetType")
    specific_unit_class: EventIdentifierCode | None = Field(
        default=None,
        alias="SpecificUnitClass",
    )
    specific_unit_id: NonEmptyStr | None = Field(
        default=None,
        validation_alias=AliasChoices("SpecificUnitID", "SpecificUnit"),
        serialization_alias="SpecificUnitID",
    )

    @model_validator(mode="after")
    def validate_selector(self) -> Self:
        if self.specific_unit_id is not None:
            if self.target_type is not None or self.specific_unit_class is not None:
                raise ValueError(
                    "CargoFilter SpecificUnitID cannot be combined with "
                    "TargetType or SpecificUnitClass"
                )
            return self
        if self.target_type is None and self.specific_unit_class is None:
            raise ValueError(
                "CargoFilter requires TargetType, SpecificUnitClass, or SpecificUnitID"
            )
        return self


class EventPointsTriggerParameters(EventParameterModel):
    side_id: NonEmptyStr | None = Field(default=None, alias="SideID")
    point_value: StrictNumber | None = Field(default=None, alias="PointValue")
    reach_direction: EventReachDirection | None = Field(default=None, alias="ReachDirection")


class EventRandomTimeTriggerParameters(EventParameterModel):
    earliest_time: NonEmptyStr | None = Field(default=None, alias="EarliestTime")
    latest_time: NonEmptyStr | None = Field(default=None, alias="LatestTime")


class EventRegularTimeTriggerParameters(EventParameterModel):
    interval: StrictPositiveInt | NonEmptyStr | None = Field(default=None, alias="Interval")


class EventScenEndedTriggerParameters(EventParameterModel):
    pass


class EventScenLoadedTriggerParameters(EventParameterModel):
    pass


class EventTimeTriggerParameters(EventParameterModel):
    time: NonEmptyStr | None = Field(default=None, alias="Time")


class EventUnitDamagedTriggerParameters(EventParameterModel):
    damage_percent: StrictPercent | None = Field(default=None, alias="DamagePercent")
    target_filter: EventTargetFilter | None = Field(default=None, alias="TargetFilter")


class EventUnitDestroyedTriggerParameters(EventParameterModel):
    target_filter: EventTargetFilter | None = Field(default=None, alias="TargetFilter")


class EventUnitDetectedTriggerParameters(EventParameterModel):
    target_filter: EventTargetFilter | None = Field(default=None, alias="TargetFilter")
    detector_side_id: NonEmptyStr | None = Field(default=None, alias="DetectorSideID")
    mcl: EventContactIdStatus | None = Field(default=None, alias="MCL")
    area: EventArea | None = Field(default=None, alias="Area")


class EventUnitEmissionsTriggerParameters(EventUnitDetectedTriggerParameters):
    pass


class EventUnitEntersAreaTriggerParameters(EventParameterModel):
    target_filter: EventTargetFilter | None = Field(default=None, alias="TargetFilter")
    area: EventArea | None = Field(default=None, alias="Area")
    earliest_arrival: NonEmptyStr | None = Field(default=None, alias="ETOA")
    latest_arrival: NonEmptyStr | None = Field(default=None, alias="LTOA")
    inverted: StrictBool | None = Field(default=None, alias="NOT")
    exit_area: StrictBool | None = Field(default=None, alias="ExitArea")


class EventUnitRemainsInAreaTriggerParameters(EventParameterModel):
    target_filter: EventTargetFilter | None = Field(default=None, alias="TargetFilter")
    area: EventArea | None = Field(default=None, alias="Area")
    duration: EventDuration | None = Field(default=None, alias="TD")


class EventUnitBaseStatusTriggerParameters(EventParameterModel):
    target_filter: EventTargetFilter | None = Field(default=None, alias="TargetFilter")
    target_base: NonEmptyStr | None = Field(default=None, alias="TargetBase")
    target_condition: StrictInt | None = Field(default=None, alias="TargetCondition")


class EventUnitCargoMovedTriggerParameters(EventParameterModel):
    cargo_filter: EventCargoFilter | None = Field(default=None, alias="CargoFilter")
    target_limit_received: StrictNonNegativeInt | None = Field(
        default=None,
        alias="TargetLimitReceived",
    )
    target_limit_sent: StrictNonNegativeInt | None = Field(
        default=None,
        alias="TargetLimitSent",
    )


class EventLuaScriptConditionParameters(EventParameterModel):
    script_text: NonEmptyStr | None = Field(default=None, alias="ScriptText")


class EventLuaScriptActionParameters(EventParameterModel):
    script_text: NonEmptyStr | None = Field(default=None, alias="ScriptText")


class EventScenHasStartedConditionParameters(EventParameterModel):
    inverted: StrictBool | None = Field(default=None, alias="NOT")


class EventSidePostureConditionParameters(EventParameterModel):
    observer_side_id: NonEmptyStr | None = Field(default=None, alias="ObserverSideID")
    target_side_id: NonEmptyStr | None = Field(default=None, alias="TargetSideID")
    target_posture: EventTargetPosture | None = Field(default=None, alias="TargetPosture")
    inverted: StrictBool | None = Field(default=None, alias="NOT")


class EventChangeMissionStatusActionParameters(EventParameterModel):
    mission_id: NonEmptyStr | None = Field(default=None, alias="MissionID")
    new_status: EventMissionStatus | None = Field(default=None, alias="NewStatus")


class EventEndScenarioActionParameters(EventParameterModel):
    pass


class EventMessageActionParameters(EventParameterModel):
    side_id: NonEmptyStr | None = Field(default=None, alias="SideID")
    text: NonEmptyStr | None = Field(default=None, alias="Text")


class EventPointsActionParameters(EventParameterModel):
    point_change: StrictNumber | None = Field(default=None, alias="PointChange")
    side_id: NonEmptyStr | None = Field(default=None, alias="SideID")


class EventTeleportInAreaActionParameters(EventParameterModel):
    unit_ids: Annotated[list[NonEmptyStr], Field(min_length=1)] | None = Field(
        default=None,
        alias="UnitIDs",
    )
    area: EventArea | None = Field(default=None, alias="Area")


EventComponentParameters: TypeAlias = (
    EventPointsTriggerParameters
    | EventRandomTimeTriggerParameters
    | EventRegularTimeTriggerParameters
    | EventScenEndedTriggerParameters
    | EventScenLoadedTriggerParameters
    | EventTimeTriggerParameters
    | EventUnitDamagedTriggerParameters
    | EventUnitDestroyedTriggerParameters
    | EventUnitDetectedTriggerParameters
    | EventUnitEmissionsTriggerParameters
    | EventUnitEntersAreaTriggerParameters
    | EventUnitRemainsInAreaTriggerParameters
    | EventUnitBaseStatusTriggerParameters
    | EventUnitCargoMovedTriggerParameters
    | EventLuaScriptConditionParameters
    | EventScenHasStartedConditionParameters
    | EventSidePostureConditionParameters
    | EventChangeMissionStatusActionParameters
    | EventEndScenarioActionParameters
    | EventLuaScriptActionParameters
    | EventMessageActionParameters
    | EventPointsActionParameters
    | EventTeleportInAreaActionParameters
)

_EVENT_PARAMETER_MODELS: dict[
    tuple[Literal["trigger", "condition", "action"], str],
    type[EventParameterModel],
] = {
    ("trigger", "Points"): EventPointsTriggerParameters,
    ("trigger", "RandomTime"): EventRandomTimeTriggerParameters,
    ("trigger", "RegularTime"): EventRegularTimeTriggerParameters,
    ("trigger", "ScenEnded"): EventScenEndedTriggerParameters,
    ("trigger", "ScenLoaded"): EventScenLoadedTriggerParameters,
    ("trigger", "Time"): EventTimeTriggerParameters,
    ("trigger", "UnitDamaged"): EventUnitDamagedTriggerParameters,
    ("trigger", "UnitDestroyed"): EventUnitDestroyedTriggerParameters,
    ("trigger", "UnitDetected"): EventUnitDetectedTriggerParameters,
    ("trigger", "UnitEmissions"): EventUnitEmissionsTriggerParameters,
    ("trigger", "UnitEntersArea"): EventUnitEntersAreaTriggerParameters,
    ("trigger", "UnitRemainsInArea"): EventUnitRemainsInAreaTriggerParameters,
    ("trigger", "UnitBaseStatus"): EventUnitBaseStatusTriggerParameters,
    ("trigger", "UnitCargoMoved"): EventUnitCargoMovedTriggerParameters,
    ("condition", "LuaScript"): EventLuaScriptConditionParameters,
    ("condition", "ScenHasStarted"): EventScenHasStartedConditionParameters,
    ("condition", "SidePosture"): EventSidePostureConditionParameters,
    ("action", "ChangeMissionStatus"): EventChangeMissionStatusActionParameters,
    ("action", "EndScenario"): EventEndScenarioActionParameters,
    ("action", "LuaScript"): EventLuaScriptActionParameters,
    ("action", "Message"): EventMessageActionParameters,
    ("action", "Points"): EventPointsActionParameters,
    ("action", "TeleportInArea"): EventTeleportInAreaActionParameters,
}

_EVENT_ADD_REQUIRED_PARAMETERS: dict[
    tuple[Literal["trigger", "condition", "action"], str],
    tuple[str, ...],
] = {
    ("trigger", "Points"): ("SideID", "PointValue", "ReachDirection"),
    ("trigger", "RandomTime"): ("EarliestTime", "LatestTime"),
    ("trigger", "RegularTime"): ("Interval",),
    ("trigger", "ScenEnded"): (),
    ("trigger", "ScenLoaded"): (),
    ("trigger", "Time"): ("Time",),
    ("trigger", "UnitDamaged"): ("DamagePercent", "TargetFilter"),
    ("trigger", "UnitDestroyed"): ("TargetFilter",),
    ("trigger", "UnitDetected"): ("TargetFilter", "DetectorSideID", "MCL"),
    ("trigger", "UnitEmissions"): ("TargetFilter", "DetectorSideID", "MCL"),
    ("trigger", "UnitEntersArea"): ("TargetFilter", "Area"),
    ("trigger", "UnitRemainsInArea"): ("TargetFilter", "Area", "TD"),
    ("trigger", "UnitBaseStatus"): ("TargetFilter", "TargetCondition"),
    ("trigger", "UnitCargoMoved"): ("CargoFilter",),
    ("condition", "LuaScript"): ("ScriptText",),
    ("condition", "ScenHasStarted"): (),
    ("condition", "SidePosture"): ("ObserverSideID", "TargetSideID", "TargetPosture"),
    ("action", "ChangeMissionStatus"): ("MissionID", "NewStatus"),
    ("action", "EndScenario"): (),
    ("action", "LuaScript"): ("ScriptText",),
    ("action", "Message"): ("SideID", "Text"),
    ("action", "Points"): ("PointChange", "SideID"),
    ("action", "TeleportInArea"): ("UnitIDs", "Area"),
}


def validate_event_component_parameters(
    kind: Literal["trigger", "condition", "action"],
    component_type: EventComponentType,
    parameters: dict[str, JsonValue],
    *,
    mode: Literal["list", "add", "update", "remove"] = "update",
) -> EventParameterModel:
    validate_event_component_type(kind, component_type)
    model = _EVENT_PARAMETER_MODELS[(kind, component_type)]
    validated = model.model_validate(parameters)
    if mode == "add":
        canonical = validated.model_dump(mode="json", by_alias=True, exclude_none=True)
        missing = [
            field_name
            for field_name in _EVENT_ADD_REQUIRED_PARAMETERS[(kind, component_type)]
            if field_name not in canonical
        ]
        if missing:
            raise ValueError(
                f"{kind} {component_type} add requires parameters: {', '.join(missing)}"
            )
        if component_type == "UnitCargoMoved" and not any(
            field_name in canonical
            for field_name in ("TargetLimitReceived", "TargetLimitSent")
        ):
            raise ValueError(
                "trigger UnitCargoMoved add requires at least one of "
                "TargetLimitReceived or TargetLimitSent"
            )
    return validated


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
    duration: (
        Annotated[
            str,
            StringConstraints(pattern=r"^\d+:\d{1,2}:\d{1,2}$"),
        ]
        | None
    ) = None

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
    awareness: SideAwarenessValue | None = None
    proficiency: SideProficiencyValue | None = None
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
    level: StrictEventLevel = 4


class EventGetArgs(AuthoringStrictModel):
    event_id_or_name: NonEmptyStr
    level: StrictEventLevel = 4


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
    component_type: EventComponentType | None = None
    new_description: NonEmptyStr | None = None
    parameters_json: str = Field(
        default="{}",
        description=(
            "JSON object containing only the official fields for the selected CMO component type."
        ),
    )

    @model_validator(mode="after")
    def validate_mode(self) -> Self:
        if self.mode == "add" and self.component_type is None:
            raise ValueError("component add requires component_type")
        parameters = _parse_event_component_parameters(self.parameters_json)
        if self.mode == "update" and parameters and self.component_type is None:
            raise ValueError("component update parameters require component_type")
        if self.mode == "update" and self.new_description is None and not parameters:
            raise ValueError(
                "component update requires new_description or non-empty parameters"
            )
        if self.mode in {"list", "remove"} and self.component_type is not None:
            raise ValueError("list and remove forbid component_type")
        if self.mode in {"list", "remove"} and self.new_description is not None:
            raise ValueError("list and remove forbid new_description")
        if self.mode in {"list", "remove"} and parameters:
            raise ValueError("list and remove forbid component parameters")
        return self

    @model_validator(mode="after")
    def validate_parameters_json(self) -> Self:
        _parse_event_component_parameters(self.parameters_json)
        return self


class EventTriggerSetArgs(EventComponentSetArgs):
    @model_validator(mode="after")
    def validate_trigger_type(self) -> Self:
        if self.component_type is not None:
            validate_event_component_parameters(
                "trigger",
                self.component_type,
                _parse_event_component_parameters(self.parameters_json),
                mode=self.mode,
            )
        return self


class EventConditionSetArgs(EventComponentSetArgs):
    @model_validator(mode="after")
    def validate_condition_type(self) -> Self:
        if self.component_type is not None:
            validate_event_component_parameters(
                "condition",
                self.component_type,
                _parse_event_component_parameters(self.parameters_json),
                mode=self.mode,
            )
        return self


class EventActionSetArgs(EventComponentSetArgs):
    @model_validator(mode="after")
    def validate_action_type(self) -> Self:
        if self.component_type is not None:
            validate_event_component_parameters(
                "action",
                self.component_type,
                _parse_event_component_parameters(self.parameters_json),
                mode=self.mode,
            )
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


def _parse_event_component_parameters(parameters_json: str) -> dict[str, JsonValue]:
    try:
        value = cast(JsonValue, json.loads(parameters_json))
    except (TypeError, ValueError) as error:
        raise ValueError("parameters_json must contain valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError("parameters_json must contain a JSON object")
    if _contains_json_null(value):
        raise ValueError("parameters_json must not contain JSON null")
    return value
