from datetime import datetime, timedelta
from typing import Annotated, Generic, Literal, TypeAlias, TypeVar
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StringConstraints,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from cmo_agent_bridge.operations.scenario_authoring import (
    AuthoringDataResult,
    EventComponentLinkArgs,
    EventComponentSetArgs,
    EventGetArgs,
    EventListArgs,
    EventSetArgs,
    ScenarioTimelineSetArgs,
    ScenarioTitleSetArgs,
    ScenarioWeatherGetArgs,
    ScenarioWeatherResult,
    ScenarioWeatherSetArgs,
    ScoreSetArgs,
    SideAddArgs,
    SideOptionsSetArgs,
    SidePostureSetArgs,
    SpecialActionAddArgs,
    SpecialActionSetArgs,
)
from cmo_agent_bridge.protocol.runtime import RuntimeVersion, derive_runtime_tag


_AUTHORING_MODEL_EXPORTS = (
    AuthoringDataResult,
    EventComponentLinkArgs,
    EventComponentSetArgs,
    EventGetArgs,
    EventListArgs,
    EventSetArgs,
    ScenarioTimelineSetArgs,
    ScenarioTitleSetArgs,
    ScenarioWeatherGetArgs,
    ScenarioWeatherResult,
    ScenarioWeatherSetArgs,
    ScoreSetArgs,
    SideAddArgs,
    SideOptionsSetArgs,
    SidePostureSetArgs,
    SpecialActionAddArgs,
    SpecialActionSetArgs,
)


NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
GuidList = Annotated[list[NonEmptyStr], Field(min_length=1)]
OrderedReferencePointGuidList = Annotated[list[NonEmptyStr], Field(max_length=64)]
Disposition = Literal["applied", "not_applied"]
MissionResultClass = Literal[
    "none",
    "strike",
    "patrol",
    "support",
    "ferry",
    "mining",
    "mine_clearing",
    "escort",
    "cargo",
]
EmconValue = Literal["Active", "Passive"]
WeaponControlValue = Literal["Free", "Tight", "Hold"]
WeaponControlUpdateValue = Literal["Free", "Tight", "Hold", "inherit"]
NuclearUseValue: TypeAlias = bool
RefuelUnrepValue = Literal[
    "Always_ExceptTankersRefuellingTankers",
    "Never",
    "Always_IncludingTankersRefuellingTankers",
]
MissionStageValue: TypeAlias = str | float
MissionLoopTypeResult: TypeAlias = str | int
DoctrineSettingValue: TypeAlias = str | int
WraSettingValue: TypeAlias = str | int | float
DoctrineBooleanUpdateValue: TypeAlias = StrictBool | Literal["inherit"]
ReferencePointAnchorType = Literal["unit", "contact", "reference_point"]
ReferencePointBearingType = Literal["fixed", "rotating"]
MissionCategory = Literal["mission", "package", "task_pool"]
TankerUsage = Literal["automatic", "mission"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _require_unique(values: list[str], field_name: str) -> list[str]:
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must contain unique values")
    return values


def _exclude_none(value: object) -> bool:
    return value is None


def _exclude_empty(value: object) -> bool:
    return not value


class BridgePrepareArgs(StrictModel):
    control_side: str | None = None
    poll_interval_code: Literal["0", "1", "2", "3", "4", "5", "6", "7", "8"] = "1"
    rotate_lineage: bool = False
    persist_game_root: bool = True
    replace_saved_game_root: bool = False


class BridgeDoctorArgs(StrictModel):
    live: bool = True


class BridgeUninstallArgs(StrictModel):
    phase: Literal["command", "files"]
    uninstall_marker: str | None = None

    @model_validator(mode="after")
    def validate_phase_marker(self) -> Self:
        if self.phase == "command" and self.uninstall_marker is not None:
            raise ValueError("command phase forbids uninstall_marker")
        if self.phase == "files" and self.uninstall_marker is None:
            raise ValueError("files phase requires uninstall_marker")
        return self


CompatCase = Literal[
    "activation-reload",
    "mutation-dedupe",
    "cancel-before-read",
    "mutation-indeterminate",
    "host-crash-recovery",
    "hundred-roundtrips",
    "high-speed",
    "lineage-switch",
]


class CompatProbeArgs(StrictModel):
    phase: Literal["automatic", "arm-paused-special-action", "collect-paused-special-action"]
    case: CompatCase | None = None

    @model_validator(mode="after")
    def validate_phase_case(self) -> Self:
        if self.case is not None and self.phase != "automatic":
            raise ValueError("case is allowed only during the automatic phase")
        return self


class EmptyArgs(StrictModel):
    pass


class BridgeStatusArgs(StrictModel):
    accept_lineage_id: UUID | None = None


class BridgeStatusWireArgs(BridgeStatusArgs):
    activation_candidate: UUID


class ObservationalProbeStepArgs(StrictModel):
    step: Literal["observational"]


class PayloadProbeStepArgs(StrictModel):
    step: Literal["payload"]
    candidate_bytes: int = Field(ge=1, le=1_048_576)


class HighSpeedProbeStepArgs(StrictModel):
    step: Literal["high-speed"]


class LineageProbeStepArgs(StrictModel):
    step: Literal["lineage"]


class KeyValueProbeStepArgs(StrictModel):
    step: Literal["key-value"]


class DedupeProbeStepArgs(StrictModel):
    step: Literal["dedupe"]


class IndeterminateProbeStepArgs(StrictModel):
    step: Literal["indeterminate"]


class LedgerCapacityProbeStepArgs(StrictModel):
    step: Literal["ledger-capacity"]
    candidate_entries: int = Field(ge=1, le=256)


class ApplyProfileProbeStepArgs(StrictModel):
    step: Literal["apply-profile"]
    safe_payload_bytes: int = Field(ge=1, le=1_048_576)
    verified_ledger_entries: int = Field(ge=1, le=256)
    effective_ledger_capacity: int = Field(ge=1, le=256)


CompatProbeStepArgs: TypeAlias = Annotated[
    ObservationalProbeStepArgs
    | PayloadProbeStepArgs
    | HighSpeedProbeStepArgs
    | LineageProbeStepArgs
    | KeyValueProbeStepArgs
    | DedupeProbeStepArgs
    | IndeterminateProbeStepArgs
    | LedgerCapacityProbeStepArgs
    | ApplyProfileProbeStepArgs,
    Field(discriminator="step"),
]


class PageArgs(StrictModel):
    page_size: int = Field(default=100, ge=1, le=500)
    cursor: str | None = None
    fields: list[NonEmptyStr] | None = None

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        if not value:
            raise ValueError("fields cannot be empty")
        return _require_unique(value, "fields")


class SideSelector(StrictModel):
    side_guid: str | None = None
    side_name: str | None = None

    @model_validator(mode="after")
    def validate_exactly_one_side(self) -> Self:
        if (self.side_guid is None) == (self.side_name is None):
            raise ValueError("exactly one Side selector is required")
        return self


class UnitListArgs(SideSelector, PageArgs):
    unit_type: str | None = None
    name_contains: str | None = None


class UnitCatalogArgs(SideSelector):
    page_size: int = Field(default=500, ge=1, le=500)
    cursor: str | None = None
    unit_type: str | None = None
    name_contains: str | None = None


class UnitOverviewArgs(SideSelector):
    page_size: int = Field(default=40, ge=1, le=50)
    cursor: str | None = None
    unit_guids: list[NonEmptyStr] | None = Field(default=None, min_length=1, max_length=500)
    unit_type: str | None = None
    name_contains: str | None = None

    @field_validator("unit_guids")
    @classmethod
    def validate_unit_guids(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        return _require_unique(value, "unit_guids")


class UnitGetArgs(StrictModel):
    unit_guid: str | None = None
    side_guid: str | None = None
    side_name: str | None = None
    unit_name: str | None = None

    @model_validator(mode="after")
    def validate_selector_form(self) -> Self:
        guid_form = self.unit_guid is not None
        name_form = self.unit_name is not None and (
            (self.side_guid is None) != (self.side_name is None)
        )
        if guid_form == name_form:
            raise ValueError("select by unit_guid or by one Side selector plus unit_name")
        if guid_form and any(
            value is not None for value in (self.side_guid, self.side_name, self.unit_name)
        ):
            raise ValueError("unit_guid form cannot include name-form fields")
        if not guid_form and self.unit_guid is not None:
            raise ValueError("name form cannot include unit_guid")
        return self


class UnitCombatStatusArgs(StrictModel):
    unit_guid: NonEmptyStr


class UnitOperationalStatusBatchArgs(StrictModel):
    unit_guids: list[NonEmptyStr] = Field(min_length=1, max_length=20)

    @field_validator("unit_guids")
    @classmethod
    def validate_unit_guids(cls, value: list[str]) -> list[str]:
        return _require_unique(value, "unit_guids")


class UnitLoadoutGetArgs(StrictModel):
    unit_guid: NonEmptyStr


class ContactListArgs(SideSelector, PageArgs):
    contact_type: str | None = None


class ReferencePointListArgs(SideSelector, PageArgs):
    pass


class MissionListArgs(SideSelector, PageArgs):
    mission_class: str | None = None
    category: MissionCategory | None = None


class MissionGetArgs(SideSelector):
    mission_guid: str | None = None
    mission_name: str | None = None

    @model_validator(mode="after")
    def validate_mission_selector(self) -> Self:
        if (self.mission_guid is None) == (self.mission_name is None):
            raise ValueError("exactly one mission selector is required")
        return self


class DoctrineSelector(StrictModel):
    scope: Literal["side", "unit", "mission"]
    side_guid: str | None = None
    unit_guid: str | None = None
    mission_guid: str | None = None

    @model_validator(mode="after")
    def validate_scope_fields(self) -> Self:
        required = {
            "side": (
                self.side_guid is not None and self.unit_guid is None and self.mission_guid is None
            ),
            "unit": (
                self.side_guid is not None
                and self.unit_guid is not None
                and self.mission_guid is None
            ),
            "mission": (
                self.side_guid is not None
                and self.unit_guid is None
                and self.mission_guid is not None
            ),
        }
        if not required[self.scope]:
            raise ValueError("doctrine selector fields do not match scope")
        return self


class DoctrineGetArgs(DoctrineSelector):
    actual: bool = True


class CourseWaypoint(StrictModel):
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    altitude: float | None = None


class UnitSetArgs(StrictModel):
    unit_guid: NonEmptyStr
    name: str | None = None
    speed: float | None = Field(default=None, ge=0)
    altitude: float | None = None
    depth: float | None = None
    throttle: str | None = None
    force_speed: bool | None = None
    heading: float | None = Field(default=None, ge=0, le=360)
    desired_heading: float | None = Field(default=None, ge=0, le=360)
    course: list[CourseWaypoint] | None = None
    move_to: bool | None = None
    manual_throttle: str | float | None = None
    manual_speed: str | float | None = None
    manual_altitude: str | float | None = None
    hold_position: bool | None = None
    hold_fire: bool | None = None
    sprint_drift: bool | None = None
    avoid_cavitation: bool | None = None
    obey_emcon: bool | None = None

    @model_validator(mode="after")
    def validate_update(self) -> Self:
        if self.speed is not None and self.throttle is not None:
            raise ValueError("speed and throttle are mutually exclusive")
        update_fields = {
            "name",
            "speed",
            "altitude",
            "depth",
            "throttle",
            "force_speed",
            "heading",
            "desired_heading",
            "course",
            "move_to",
            "manual_throttle",
            "manual_speed",
            "manual_altitude",
            "hold_position",
            "hold_fire",
            "sprint_drift",
            "avoid_cavitation",
            "obey_emcon",
        }
        if not any(getattr(self, field) is not None for field in update_fields):
            raise ValueError("at least one unit update is required")
        return self


class UnitAddArgs(StrictModel):
    side_guid: NonEmptyStr
    unit_type: NonEmptyStr
    dbid: int = Field(ge=1)
    name: NonEmptyStr
    base_guid: NonEmptyStr | None = None
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)
    altitude: float | None = None
    loadout_dbid: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_location(self) -> Self:
        has_latitude = self.latitude is not None
        has_longitude = self.longitude is not None
        if has_latitude != has_longitude:
            raise ValueError("unit coordinates require both latitude and longitude")
        has_coordinates = has_latitude and has_longitude
        if (self.base_guid is not None) == has_coordinates:
            raise ValueError("exactly one of base_guid or coordinates is required")
        return self


class ReferencePointAddArgs(StrictModel):
    side_guid: NonEmptyStr
    name: NonEmptyStr
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)
    relative_to_type: ReferencePointAnchorType | None = Field(
        default=None,
        description=(
            "Build-sensitive field selecting a unit, contact, or reference-point anchor; "
            "verify the returned anchor after mutation."
        ),
    )
    relative_to_guid: NonEmptyStr | None = Field(
        default=None,
        description="Build-sensitive field containing the relative anchor GUID.",
    )
    relative_bearing_deg: float | None = Field(
        default=None,
        ge=0,
        le=360,
        description="Bearing in degrees from the relative anchor.",
    )
    relative_distance_nm: float | None = Field(
        default=None,
        ge=0,
        description="Distance in nautical miles from the relative anchor.",
    )
    bearing_type: ReferencePointBearingType | None = Field(
        default=None,
        description=(
            "Fixed uses true bearing and rotating follows anchor heading. Relative points "
            "default to rotating; verify effective behavior on the installed CMO build."
        ),
    )

    @model_validator(mode="after")
    def validate_position(self) -> Self:
        absolute_values = (self.latitude, self.longitude)
        relative_values = (
            self.relative_to_type,
            self.relative_to_guid,
            self.relative_bearing_deg,
            self.relative_distance_nm,
        )
        has_absolute = any(value is not None for value in absolute_values)
        has_relative = any(value is not None for value in relative_values)
        if has_absolute and has_relative:
            raise ValueError(
                "absolute and relative reference-point positions are mutually exclusive"
            )
        if has_absolute:
            if any(value is None for value in absolute_values):
                raise ValueError(
                    "absolute reference-point position requires latitude and longitude"
                )
            if self.bearing_type is not None:
                raise ValueError("bearing_type requires a relative reference-point position")
            return self
        if has_relative:
            if any(value is None for value in relative_values):
                raise ValueError(
                    "relative reference-point position requires anchor type, anchor GUID, "
                    "bearing, and distance"
                )
            return self
        raise ValueError("reference point requires an absolute or relative position")


class ReferencePointUpdateArgs(StrictModel):
    side_guid: NonEmptyStr
    reference_point_guid: NonEmptyStr
    name: NonEmptyStr | None = None
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)
    relative_to_type: ReferencePointAnchorType | None = Field(
        default=None,
        description="Build-sensitive field selecting the new relative anchor type.",
    )
    relative_to_guid: NonEmptyStr | None = Field(
        default=None,
        description="Build-sensitive field selecting the new relative anchor GUID.",
    )
    relative_bearing_deg: float | None = Field(
        default=None,
        ge=0,
        le=360,
        description="Update the bearing in degrees from the relative anchor.",
    )
    relative_distance_nm: float | None = Field(
        default=None,
        ge=0,
        description="Update the distance in nautical miles from the relative anchor.",
    )
    bearing_type: ReferencePointBearingType | None = Field(
        default=None,
        description="Update fixed or rotating bearing behavior.",
    )
    clear_relative: bool = False

    @model_validator(mode="after")
    def validate_update(self) -> Self:
        update_values = (
            self.name,
            self.latitude,
            self.longitude,
            self.relative_to_type,
            self.relative_to_guid,
            self.relative_bearing_deg,
            self.relative_distance_nm,
            self.bearing_type,
        )
        if not self.clear_relative and not any(value is not None for value in update_values):
            raise ValueError("at least one reference point update is required")
        anchor_values = (self.relative_to_type, self.relative_to_guid)
        if any(value is not None for value in anchor_values) and any(
            value is None for value in anchor_values
        ):
            raise ValueError("relative anchor update requires both anchor type and anchor GUID")
        if self.relative_to_guid is not None and (
            self.relative_bearing_deg is None or self.relative_distance_nm is None
        ):
            raise ValueError("changing the relative anchor requires bearing and distance")
        relative_updates = (
            self.relative_to_type,
            self.relative_to_guid,
            self.relative_bearing_deg,
            self.relative_distance_nm,
            self.bearing_type,
        )
        if self.clear_relative and any(value is not None for value in relative_updates):
            raise ValueError("clear_relative cannot be combined with relative updates")
        if any(value is not None for value in (self.latitude, self.longitude)) and any(
            value is not None for value in relative_updates
        ):
            raise ValueError("absolute and relative reference-point updates are mutually exclusive")
        return self


class UnitAssignMissionArgs(StrictModel):
    unit_guid: NonEmptyStr
    mission_guid: NonEmptyStr
    escort: bool = False


class UnitUnassignMissionArgs(StrictModel):
    unit_guid: NonEmptyStr


class UnitLoadoutSetArgs(StrictModel):
    unit_guid: NonEmptyStr
    loadout_dbid: int = Field(ge=1)
    time_to_ready_minutes: float | None = Field(default=None, ge=0)
    ignore_magazines: bool = False
    exclude_optional_weapons: bool = False


class UnitCommandArgs(StrictModel):
    unit_guid: NonEmptyStr


class UnitRefuelArgs(StrictModel):
    unit_guid: NonEmptyStr
    tanker_guid: NonEmptyStr | None = None
    tanker_mission_guids: Annotated[list[NonEmptyStr], Field(min_length=1)] | None = None

    @model_validator(mode="after")
    def validate_tanker_selector(self) -> Self:
        if self.tanker_guid is not None and self.tanker_mission_guids is not None:
            raise ValueError("tanker_guid and tanker_mission_guids are mutually exclusive")
        return self


class UnitAttackContactArgs(StrictModel):
    side_guid: NonEmptyStr
    attacker_unit_guid: NonEmptyStr
    contact_guid: NonEmptyStr
    mode: Literal["auto", "manual_weapon", "manual_target"]
    mount_dbid: int | None = Field(default=None, ge=1)
    weapon_dbid: int | None = Field(default=None, ge=1)
    quantity: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_mode_fields(self) -> Self:
        weapon_fields = (self.mount_dbid, self.weapon_dbid, self.quantity)
        if self.mode == "manual_weapon":
            if self.weapon_dbid is None or self.quantity is None:
                raise ValueError("manual_weapon mode requires weapon_dbid and quantity")
        elif any(value is not None for value in weapon_fields):
            raise ValueError("weapon allocation fields require manual_weapon mode")
        return self


PatrolType = Literal["asw", "naval", "aaw", "land", "mixed", "sead", "sea"]
StrikeType = Literal["air", "land", "sea", "sub"]
FlightSize = Literal[1, 2, 3, 4, 6]
MinimumAircraftRequired = Literal[0, 1, 2, 3, 4, 6, 8, 12]


class PatrolMissionDetails(StrictModel):
    mission_class: Literal["patrol"]
    patrol_type: PatrolType
    reference_point_guids: Annotated[list[NonEmptyStr], Field(min_length=3, max_length=64)]


class SupportMissionDetails(StrictModel):
    mission_class: Literal["support"]
    reference_point_guids: Annotated[list[NonEmptyStr], Field(min_length=2, max_length=64)]


class StrikeMissionDetails(StrictModel):
    mission_class: Literal["strike"]
    strike_type: StrikeType
    target_guids: list[NonEmptyStr] = Field(default_factory=list)


class FerryMissionDetails(StrictModel):
    mission_class: Literal["ferry"]
    destination_guid: NonEmptyStr


class MiningMissionDetails(StrictModel):
    mission_class: Literal["mining"]
    reference_point_guids: Annotated[list[NonEmptyStr], Field(min_length=3, max_length=64)]


class MineClearingMissionDetails(StrictModel):
    mission_class: Literal["mine_clearing"]
    reference_point_guids: Annotated[list[NonEmptyStr], Field(min_length=3, max_length=64)]


class CargoMissionDetails(StrictModel):
    mission_class: Literal["cargo"]
    cargo_subtype: Literal["transfer", "delivery"]
    destination_guid: NonEmptyStr | None = None
    reference_point_guids: (
        Annotated[list[NonEmptyStr], Field(min_length=3, max_length=64)] | None
    ) = None

    @model_validator(mode="after")
    def validate_cargo_form(self) -> Self:
        if self.cargo_subtype == "transfer":
            if self.destination_guid is None:
                raise ValueError("transfer cargo mission requires destination_guid")
            if self.reference_point_guids is not None:
                raise ValueError("transfer cargo mission forbids reference_point_guids")
        else:
            if self.reference_point_guids is None:
                raise ValueError("delivery cargo mission requires reference_point_guids")
            if self.destination_guid is not None:
                raise ValueError("delivery cargo mission forbids destination_guid")
        return self


MissionDetails: TypeAlias = Annotated[
    PatrolMissionDetails
    | SupportMissionDetails
    | StrikeMissionDetails
    | FerryMissionDetails
    | MiningMissionDetails
    | MineClearingMissionDetails
    | CargoMissionDetails,
    Field(discriminator="mission_class"),
]


class MissionCreateArgs(StrictModel):
    side_guid: NonEmptyStr
    name: NonEmptyStr
    category: MissionCategory = "mission"
    parent_task_pool_guid: NonEmptyStr | None = None
    details: MissionDetails

    @model_validator(mode="after")
    def validate_category(self) -> Self:
        if self.category == "package":
            if self.parent_task_pool_guid is None:
                raise ValueError("package mission requires parent_task_pool_guid")
        elif self.parent_task_pool_guid is not None:
            raise ValueError("parent_task_pool_guid is only valid for package missions")
        return self


class MissionUpdateArgs(StrictModel):
    side_guid: NonEmptyStr
    mission_guid: NonEmptyStr
    active: bool | None = None
    start_time: str | None = None
    end_time: str | None = None
    flight_size: FlightSize | None = None
    use_flight_size: bool | None = None
    minimum_aircraft_required: MinimumAircraftRequired | None = None
    on_station: int | None = Field(default=None, ge=0)
    one_time_only: bool | None = None
    preplanned_only: bool | None = None
    one_third_rule: bool | None = None
    reference_point_guids: OrderedReferencePointGuidList | None = None
    prosecution_zone_reference_point_guids: OrderedReferencePointGuidList | None = None
    destination_guid: NonEmptyStr | None = None
    loop_type: int | None = Field(default=None, ge=0, le=2)
    active_emcon: bool | None = None
    check_opa: bool | None = None
    check_wwr: bool | None = None
    group_size: int | None = Field(default=None, ge=1)
    use_group_size: bool | None = None
    transit_throttle_aircraft: MissionStageValue | None = None
    transit_throttle_ship: MissionStageValue | None = None
    transit_throttle_submarine: MissionStageValue | None = None
    station_throttle_aircraft: MissionStageValue | None = None
    station_throttle_ship: MissionStageValue | None = None
    station_throttle_submarine: MissionStageValue | None = None
    attack_throttle_aircraft: MissionStageValue | None = None
    attack_throttle_ship: MissionStageValue | None = None
    attack_throttle_submarine: MissionStageValue | None = None
    transit_altitude_aircraft: MissionStageValue | None = None
    station_altitude_aircraft: MissionStageValue | None = None
    attack_altitude_aircraft: MissionStageValue | None = None
    transit_depth_submarine: MissionStageValue | None = None
    station_depth_submarine: MissionStageValue | None = None
    attack_depth_submarine: MissionStageValue | None = None
    strike_minimum_trigger: str | None = None
    strike_max_flights: int | None = Field(default=None, ge=0)
    strike_auto_planner: bool | None = None
    strike_min_distance_aircraft: MissionStageValue | None = None
    strike_max_distance_aircraft: MissionStageValue | None = None
    strike_min_distance_ship: MissionStageValue | None = None
    strike_max_distance_ship: MissionStageValue | None = None
    focus_on_strike: bool | None = None
    arming_delay: str | None = None
    mines_per_set: int | None = Field(default=None, ge=1)
    mine_spacing_m: float | None = Field(default=None, ge=0)
    set_spacing_m: float | None = Field(default=None, ge=0)
    laying_method: Literal[0, 1] | None = None
    cargo_subtype: Literal["transfer", "delivery"] | None = None
    move_all_cargo: bool | None = None
    allow_ground_self_delivery: bool | None = None

    @model_validator(mode="after")
    def validate_update(self) -> Self:
        fields = {
            "active",
            "start_time",
            "end_time",
            "flight_size",
            "use_flight_size",
            "minimum_aircraft_required",
            "on_station",
            "one_time_only",
            "preplanned_only",
            "one_third_rule",
            "reference_point_guids",
            "prosecution_zone_reference_point_guids",
            "destination_guid",
            "loop_type",
            "active_emcon",
            "check_opa",
            "check_wwr",
            "group_size",
            "use_group_size",
            "transit_throttle_aircraft",
            "transit_throttle_ship",
            "transit_throttle_submarine",
            "station_throttle_aircraft",
            "station_throttle_ship",
            "station_throttle_submarine",
            "attack_throttle_aircraft",
            "attack_throttle_ship",
            "attack_throttle_submarine",
            "transit_altitude_aircraft",
            "station_altitude_aircraft",
            "attack_altitude_aircraft",
            "transit_depth_submarine",
            "station_depth_submarine",
            "attack_depth_submarine",
            "strike_minimum_trigger",
            "strike_max_flights",
            "strike_auto_planner",
            "strike_min_distance_aircraft",
            "strike_max_distance_aircraft",
            "strike_min_distance_ship",
            "strike_max_distance_ship",
            "focus_on_strike",
            "arming_delay",
            "mines_per_set",
            "mine_spacing_m",
            "set_spacing_m",
            "laying_method",
            "cargo_subtype",
            "move_all_cargo",
            "allow_ground_self_delivery",
        }
        if not any(getattr(self, field) is not None for field in fields):
            raise ValueError("at least one mission update is required")
        return self


class MissionTargetArgs(StrictModel):
    side_guid: NonEmptyStr
    mission_guid: NonEmptyStr
    target_guid: NonEmptyStr


class MissionAirRefuelingUpdateArgs(StrictModel):
    side_guid: NonEmptyStr
    mission_guid: NonEmptyStr
    use_refuel_unrep: RefuelUnrepValue | None = None
    tanker_usage: TankerUsage | None = None
    launch_without_tankers_in_place: bool | None = None
    tanker_follows_receivers: bool | None = None
    keep_on_mission_without_tankers_in_place: bool | None = None
    tanker_mission_guids: list[NonEmptyStr] | None = None
    minimum_tankers_total: int | None = Field(default=None, ge=0)
    minimum_tankers_airborne: int | None = Field(default=None, ge=0)
    minimum_tankers_on_station: int | None = Field(default=None, ge=0)
    max_receivers_in_queue_per_tanker: int | None = Field(default=None, ge=0)
    fuel_percent_to_start_looking: float | None = Field(default=None, ge=0, le=100)
    tanker_max_distance_nm: Annotated[float, Field(ge=0)] | Literal["internal"] | None = None
    tanker_one_time: bool | None = None
    tanker_max_receivers: Annotated[int, Field(ge=0)] | NonEmptyStr | None = None

    @model_validator(mode="after")
    def validate_update(self) -> Self:
        fields = (
            "use_refuel_unrep",
            "tanker_usage",
            "launch_without_tankers_in_place",
            "tanker_follows_receivers",
            "keep_on_mission_without_tankers_in_place",
            "tanker_mission_guids",
            "minimum_tankers_total",
            "minimum_tankers_airborne",
            "minimum_tankers_on_station",
            "max_receivers_in_queue_per_tanker",
            "fuel_percent_to_start_looking",
            "tanker_max_distance_nm",
            "tanker_one_time",
            "tanker_max_receivers",
        )
        if not any(getattr(self, field) is not None for field in fields):
            raise ValueError("at least one air-refueling update is required")
        return self


class MissionFlightPlanListArgs(StrictModel):
    side_guid: NonEmptyStr
    mission_guid: NonEmptyStr


class MissionFlightPlanCreateArgs(MissionFlightPlanListArgs):
    date_on_target: str | None = None
    time_on_target: str | None = None
    takeoff_date: str | None = None
    takeoff_time: str | None = None

    @field_validator("date_on_target", "takeoff_date")
    @classmethod
    def validate_flight_plan_date(cls, value: str | None) -> str | None:
        if value is not None:
            try:
                parsed = datetime.strptime(value, "%Y/%m/%d")
            except ValueError as error:
                raise ValueError("flight-plan date must use YYYY/MM/DD") from error
            if parsed.strftime("%Y/%m/%d") != value:
                raise ValueError("flight-plan date must use YYYY/MM/DD")
        return value

    @field_validator("time_on_target", "takeoff_time")
    @classmethod
    def validate_flight_plan_time(cls, value: str | None) -> str | None:
        if value is not None:
            try:
                parsed = datetime.strptime(value, "%H:%M:%S")
            except ValueError as error:
                raise ValueError("flight-plan time must use HH:MM:SS") from error
            if parsed.strftime("%H:%M:%S") != value:
                raise ValueError("flight-plan time must use HH:MM:SS")
        return value

    @model_validator(mode="after")
    def validate_schedule(self) -> Self:
        target_values = (self.date_on_target, self.time_on_target)
        takeoff_values = (self.takeoff_date, self.takeoff_time)
        target_complete = all(value is not None for value in target_values)
        takeoff_complete = all(value is not None for value in takeoff_values)
        if any(value is not None for value in target_values) and not target_complete:
            raise ValueError("time-on-target schedule requires date_on_target and time_on_target")
        if any(value is not None for value in takeoff_values) and not takeoff_complete:
            raise ValueError("takeoff schedule requires takeoff_date and takeoff_time")
        if target_complete == takeoff_complete:
            raise ValueError("exactly one flight-plan schedule form is required")
        return self


class DoctrineSetArgs(DoctrineSelector):
    weapon_control_air: WeaponControlUpdateValue | None = None
    weapon_control_surface: WeaponControlUpdateValue | None = None
    weapon_control_subsurface: WeaponControlUpdateValue | None = None
    weapon_control_land: WeaponControlUpdateValue | None = None
    nuclear_use: NuclearUseValue | None = None
    refuel_unrep: RefuelUnrepValue | None = None
    engage_opportunity_targets: DoctrineBooleanUpdateValue | None = None
    automatic_evasion: DoctrineBooleanUpdateValue | None = None
    ignore_plotted_course: DoctrineBooleanUpdateValue | None = None
    ignore_emcon_while_under_attack: DoctrineBooleanUpdateValue | None = None
    maintain_standoff: DoctrineBooleanUpdateValue | None = None
    use_sams_in_anti_surface_mode: DoctrineBooleanUpdateValue | None = None
    engaging_ambiguous_targets: DoctrineSettingValue | None = None
    fuel_state_planned: DoctrineSettingValue | None = None
    fuel_state_rtb: DoctrineSettingValue | None = None
    weapon_state_planned: DoctrineSettingValue | None = None
    weapon_state_rtb: DoctrineSettingValue | None = None
    withdraw_on_attack: DoctrineSettingValue | None = None
    withdraw_on_damage: DoctrineSettingValue | None = None
    withdraw_on_defence: DoctrineSettingValue | None = None
    withdraw_on_fuel: DoctrineSettingValue | None = None
    bvr_logic: DoctrineSettingValue | None = None
    dipping_sonar: DoctrineSettingValue | None = None
    use_aip: DoctrineSettingValue | None = None
    recharge_on_attack: DoctrineSettingValue | None = None
    recharge_on_patrol: DoctrineSettingValue | None = None

    @model_validator(mode="after")
    def validate_update(self) -> Self:
        fields = {
            "weapon_control_air",
            "weapon_control_surface",
            "weapon_control_subsurface",
            "weapon_control_land",
            "nuclear_use",
            "refuel_unrep",
            "engage_opportunity_targets",
            "automatic_evasion",
            "ignore_plotted_course",
            "ignore_emcon_while_under_attack",
            "maintain_standoff",
            "use_sams_in_anti_surface_mode",
            "engaging_ambiguous_targets",
            "fuel_state_planned",
            "fuel_state_rtb",
            "weapon_state_planned",
            "weapon_state_rtb",
            "withdraw_on_attack",
            "withdraw_on_damage",
            "withdraw_on_defence",
            "withdraw_on_fuel",
            "bvr_logic",
            "dipping_sonar",
            "use_aip",
            "recharge_on_attack",
            "recharge_on_patrol",
        }
        if not any(getattr(self, field) is not None for field in fields):
            raise ValueError("at least one doctrine update is required")
        return self


class EmconSetArgs(StrictModel):
    scope: Literal["side", "mission", "group", "unit"]
    target_guid: NonEmptyStr
    inherit: bool = False
    radar: EmconValue | None = None
    sonar: EmconValue | None = None
    oecm: EmconValue | None = None

    @model_validator(mode="after")
    def validate_clause(self) -> Self:
        if not self.inherit and all(value is None for value in (self.radar, self.sonar, self.oecm)):
            raise ValueError("EMCON requires inherit or at least one transmitter clause")
        return self

    def to_wire_grammar(self) -> str:
        clauses = ["Inherit"] if self.inherit else []
        for name, value in (("Radar", self.radar), ("Sonar", self.sonar), ("OECM", self.oecm)):
            if value is not None:
                clauses.append(f"{name}={value}")
        return ";".join(clauses)


class EmconSetWireArgs(StrictModel):
    scope: Literal["side", "mission", "group", "unit"]
    target_guid: NonEmptyStr
    emcon: NonEmptyStr

    @field_validator("emcon")
    @classmethod
    def validate_grammar(cls, value: str) -> str:
        clauses = value.split(";")
        if any(not clause for clause in clauses):
            raise ValueError("EMCON grammar cannot contain an empty clause")
        if "Inherit" in clauses[1:]:
            raise ValueError("Inherit must be the first EMCON clause")
        transmitter_clauses = clauses[1:] if clauses[0] == "Inherit" else clauses
        if not transmitter_clauses and clauses[0] != "Inherit":
            raise ValueError("EMCON grammar requires a transmitter clause")
        seen: set[str] = set()
        for clause in transmitter_clauses:
            if "=" not in clause:
                raise ValueError("invalid EMCON transmitter clause")
            transmitter, status = clause.split("=", maxsplit=1)
            if transmitter not in {"Radar", "Sonar", "OECM"}:
                raise ValueError("invalid EMCON transmitter")
            if status not in {"Active", "Passive"}:
                raise ValueError("invalid EMCON status")
            if transmitter in seen:
                raise ValueError("duplicate EMCON transmitter clause")
            seen.add(transmitter)
        return value


class ScenarioTimeCompressionSetArgs(StrictModel):
    code: Literal[0, 1, 2, 3, 4, 5]


class SidePostureGetArgs(StrictModel):
    side_a_guid: NonEmptyStr
    side_b_guid: NonEmptyStr


class ContactGetArgs(SideSelector):
    contact_guid: NonEmptyStr


class ContactPostureSetArgs(StrictModel):
    side_guid: NonEmptyStr
    contact_guid: NonEmptyStr
    posture: Literal["F", "N", "U", "H"]


class ContactWeaponAllocationsGetArgs(StrictModel):
    side_guid: NonEmptyStr
    contact_guid: NonEmptyStr


class UnitInventoryGetArgs(StrictModel):
    unit_guid: NonEmptyStr


class UnitSensorSetArgs(StrictModel):
    unit_guid: NonEmptyStr
    sensor_guid: NonEmptyStr
    active: bool
    obey_emcon: bool | None = None


class InventoryAdjustmentArgs(StrictModel):
    unit_guid: NonEmptyStr
    weapon_dbid: int = Field(ge=1)
    mode: Literal["add", "remove", "fill"]
    quantity: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_mode_fields(self) -> Self:
        if self.mode == "fill":
            if self.quantity is not None:
                raise ValueError("fill mode forbids quantity")
        elif self.quantity is None:
            raise ValueError("add and remove modes require quantity")
        return self


class UnitMagazineAdjustArgs(InventoryAdjustmentArgs):
    magazine_guid: NonEmptyStr
    max_capacity: int | None = Field(default=None, ge=1)
    allow_new: bool | None = None


class UnitMountReloadAdjustArgs(InventoryAdjustmentArgs):
    mount_guid: NonEmptyStr
    add_as_cell: bool | None = None


class CargoTransferItem(StrictModel):
    cargo_guid: NonEmptyStr | None = None
    dbid: int | None = Field(default=None, ge=1)
    quantity: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_item_form(self) -> Self:
        guid_form = self.cargo_guid is not None
        dbid_form = self.dbid is not None and self.quantity is not None
        if guid_form == dbid_form:
            raise ValueError("select cargo by cargo_guid or by dbid and quantity")
        if guid_form and (self.dbid is not None or self.quantity is not None):
            raise ValueError("cargo_guid form forbids dbid and quantity")
        return self


class UnitCargoTransferArgs(StrictModel):
    from_unit_guid: NonEmptyStr
    to_unit_guid: NonEmptyStr
    items: Annotated[list[CargoTransferItem], Field(min_length=1)]


class UnitCargoUnloadArgs(StrictModel):
    unit_guid: NonEmptyStr


class MissionCargoUpdateArgs(StrictModel):
    side_guid: NonEmptyStr
    mission_guid: NonEmptyStr
    action: Literal["assign", "unassign"]
    cargo_kind: Literal["mount", "object"]
    dbid: int = Field(ge=1)
    quantity: int | None = Field(default=None, ge=1)
    object_type: int | None = Field(default=None, ge=1, le=5)
    cargo_guid: NonEmptyStr | None = None

    @model_validator(mode="after")
    def validate_cargo_form(self) -> Self:
        if self.cargo_kind == "mount":
            if self.quantity is None:
                raise ValueError("mount cargo requires quantity")
            if self.object_type is not None or self.cargo_guid is not None:
                raise ValueError("mount cargo forbids object_type and cargo_guid")
        else:
            if self.object_type is None or self.cargo_guid is None:
                raise ValueError("object cargo requires object_type and cargo_guid")
            if self.quantity is not None:
                raise ValueError("object cargo forbids quantity")
        return self


class DoctrineWraSelector(DoctrineSelector):
    contact_guid: NonEmptyStr | None = None
    target_type: NonEmptyStr | int | None = None

    @model_validator(mode="after")
    def validate_target_selector(self) -> Self:
        if (self.contact_guid is None) == (self.target_type is None):
            raise ValueError("exactly one of contact_guid or target_type is required")
        return self


class DoctrineWraGetArgs(DoctrineWraSelector):
    weapon_dbid: int | None = Field(default=None, ge=1)
    full_wra: bool = False


class DoctrineWraSetArgs(DoctrineWraSelector):
    weapon_dbid: int = Field(ge=1)
    weapons_per_salvo: WraSettingValue
    shooters_per_salvo: WraSettingValue
    firing_range: WraSettingValue
    self_defence_range: WraSettingValue


class SpecialActionListArgs(StrictModel):
    side_guid: str | None = None
    side_name: str | None = None

    @model_validator(mode="after")
    def validate_at_most_one_side(self) -> Self:
        if self.side_guid is not None and self.side_name is not None:
            raise ValueError("side_guid and side_name are mutually exclusive")
        return self


class SpecialActionExecuteArgs(StrictModel):
    side_guid: NonEmptyStr
    action_guid: NonEmptyStr


class DeleteUnitArgs(StrictModel):
    unit_guid: NonEmptyStr


class DeleteMissionArgs(StrictModel):
    side_guid: NonEmptyStr
    mission_guid: NonEmptyStr


class ConfirmedDeleteUnitWireArgs(DeleteUnitArgs):
    confirmation_proof: Sha256


class ConfirmedDeleteMissionWireArgs(DeleteMissionArgs):
    confirmation_proof: Sha256


class LuaCallArgs(StrictModel):
    function: NonEmptyStr
    arguments: dict[str, object]


class GetScoreArgs(StrictModel):
    side: NonEmptyStr


class ReconcileArgs(StrictModel):
    request_id: UUID | None = None
    disposition: Disposition | None = None

    @model_validator(mode="after")
    def validate_pair(self) -> Self:
        if (self.request_id is None) != (self.disposition is None):
            raise ValueError("request_id and disposition must be supplied together")
        return self


class ReconcileProbeWireArgs(StrictModel):
    pass


class ReconcileCommitWireArgs(StrictModel):
    request_id: UUID
    disposition: Disposition
    confirmation_proof: Sha256


ReconcileWireArgs: TypeAlias = ReconcileProbeWireArgs | ReconcileCommitWireArgs


class BridgeStatusResult(StrictModel):
    protocol: Literal["cmo-agent-bridge/1"]
    runtime_version: RuntimeVersion
    runtime_tag: NonEmptyStr
    runtime_asset_sha256: Sha256
    release_id: Sha256
    build: int = Field(ge=1)
    manifest_sha256: Sha256
    lineage_id: UUID
    activation_id: UUID
    installed_event_names: list[NonEmptyStr]
    installed_action_names: list[NonEmptyStr]
    installed_trigger_names: list[NonEmptyStr]
    pending_request_id: UUID | None
    quarantined: bool
    paused_capability: bool | None
    poll_interval_seconds: int = Field(ge=1)
    safe_payload_bytes: int = Field(ge=1, le=1_048_576)
    verified_ledger_entries: int = Field(ge=1, le=256)
    effective_ledger_capacity: int = Field(ge=1, le=256)

    @model_validator(mode="after")
    def validate_capacity(self) -> Self:
        if self.effective_ledger_capacity > self.verified_ledger_entries:
            raise ValueError("effective ledger capacity exceeds verified entries")
        if self.runtime_tag != derive_runtime_tag(self.runtime_version, self.runtime_asset_sha256):
            raise ValueError("runtime tag does not match runtime version and asset digest")
        return self


class ScenarioResult(StrictModel):
    guid: NonEmptyStr
    title: str
    file_name: str
    file_name_path: str
    current_time: str
    current_time_seconds: float
    start_time: str
    start_time_seconds: float
    duration: str
    duration_seconds: float
    complexity: int
    difficulty: int
    setting: str
    database: str
    save_version: str
    started: bool
    player_side_guid: NonEmptyStr | None
    time_compression: float
    campaign_score: int


ScenarioContextStatus = Literal[
    "available",
    "partial",
    "no_player_side",
    "unsaved_scenario",
    "file_missing",
    "side_not_found",
    "assembly_incompatible",
    "reader_failed",
    "scenario_changed",
]


class ScenarioScoringThresholds(StrictModel):
    major_defeat: int | None
    minor_defeat: int | None
    average: int | None
    minor_victory: int | None
    major_victory: int | None


class ScenarioContextResult(StrictModel):
    status: ScenarioContextStatus
    scenario_guid: NonEmptyStr
    title: str
    player_side_guid: NonEmptyStr | None
    player_side_name: str | None
    scenario_file: str | None
    scenario_description: str | None
    side_briefing: str | None
    scoring_thresholds: ScenarioScoringThresholds | None
    description_truncated: bool
    briefing_truncated: bool
    saved_snapshot: bool
    warnings: list[str]


class SideResult(StrictModel):
    guid: NonEmptyStr
    name: str
    awareness: str
    proficiency: str
    computer_controlled_only: bool
    unit_count: int = Field(ge=0)
    contact_count: int = Field(ge=0)
    mission_count: int = Field(ge=0)


class UnitDamageResult(StrictModel):
    dp: float | None
    start_dp: float | None
    dp_percent: float | None
    dp_percent_now: float | None
    fires: str | None
    flood: str | None


class UnitFuelResult(StrictModel):
    type_code: int
    name: str
    current: float
    max: float
    percent: float | None


class UnitCombatStatusResult(StrictModel):
    unit_guid: NonEmptyStr
    operating: bool
    destroyed: bool | None
    sinking: bool | None
    condition: str
    condition_code: str
    unit_state: str
    fuel_state: str
    weapon_state: str
    ready_time_seconds: float | None
    airborne_time_seconds: float | None
    loadout_dbid: int | None
    damage: UnitDamageResult | None
    fuels: list[UnitFuelResult]
    target_contact_guid: str | None
    firing_at_contact_guids: list[NonEmptyStr]
    fired_on_by_unit_guids: list[NonEmptyStr]
    targeted_by_unit_guids: list[NonEmptyStr]


class UnitLoadoutWeaponResult(StrictModel):
    guid: str | None
    dbid: int
    name: str
    type_code: int | None
    current: int = Field(ge=0)
    max_capacity: int = Field(ge=0)
    default: int = Field(ge=0)


class UnitLoadoutResult(StrictModel):
    unit_guid: NonEmptyStr
    loadout_dbid: int
    name: str
    ready_time_seconds: float | None
    weapons: list[UnitLoadoutWeaponResult]


class UnitResult(StrictModel):
    guid: NonEmptyStr
    dbid: int
    name: str
    side_name: str
    type: str
    subtype: str | None
    category: str
    class_name: str
    latitude: float
    longitude: float
    altitude: float
    speed: float
    heading: float
    throttle: str
    proficiency: str
    fuel_state: str
    weapon_state: str
    unit_state: str
    operating: bool
    mission_guid: str | None
    mission_name: str | None
    loadout_dbid: int | None
    depth: float | None = Field(default=None, exclude_if=_exclude_none)
    force_speed: bool | None = Field(default=None, exclude_if=_exclude_none)
    desired_heading: float | None = Field(default=None, exclude_if=_exclude_none)
    move_to: bool | None = Field(default=None, exclude_if=_exclude_none)
    manual_throttle: str | float | None = Field(default=None, exclude_if=_exclude_none)
    manual_speed: str | float | None = Field(default=None, exclude_if=_exclude_none)
    manual_altitude: str | float | None = Field(default=None, exclude_if=_exclude_none)
    hold_position: bool | None = Field(default=None, exclude_if=_exclude_none)
    hold_fire: bool | None = Field(default=None, exclude_if=_exclude_none)
    sprint_drift: bool | None = Field(default=None, exclude_if=_exclude_none)
    avoid_cavitation: bool | None = Field(default=None, exclude_if=_exclude_none)
    obey_emcon: bool | None = Field(default=None, exclude_if=_exclude_none)


class UnitCatalogItem(StrictModel):
    guid: NonEmptyStr
    name: str
    type: str


class UnitCatalogResult(StrictModel):
    side_guid: NonEmptyStr
    side_name: str
    total_count: int = Field(ge=0)
    items: list[UnitCatalogItem]
    next_cursor: str | None


class UnitOverviewResult(StrictModel):
    side_guid: NonEmptyStr
    side_name: str
    item_count: int = Field(ge=0, le=50)
    text: str
    next_cursor: str | None


class UnitOperationalStatusResult(StrictModel):
    unit_guid: NonEmptyStr
    name: str
    type: str
    latitude: float
    longitude: float
    altitude: float
    speed: float
    heading: float
    operating: bool
    condition: str
    unit_state: str
    fuel_state: str
    weapon_state: str
    mission_guid: str | None
    mission_name: str | None
    loadout_dbid: int | None


class UnitOperationalStatusBatchResult(StrictModel):
    items: list[UnitOperationalStatusResult] = Field(max_length=20)


class ContactResult(StrictModel):
    guid: NonEmptyStr
    name: str
    observer_side_guid: NonEmptyStr
    type: str
    type_description: str
    classification: str
    posture: str
    latitude: float | None
    longitude: float | None
    altitude: float | None
    speed: float | None
    heading: float | None
    actual_unit_guid: str | None
    actual_unit_dbid: int | None


class LatLonResult(StrictModel):
    latitude: float
    longitude: float


class DetectionByResult(StrictModel):
    visual: float | None = Field(default=None, exclude_if=_exclude_none)
    infrared: float | None = Field(default=None, exclude_if=_exclude_none)
    radar: float | None = Field(default=None, exclude_if=_exclude_none)
    esm: float | None = Field(default=None, exclude_if=_exclude_none)
    sonar_active: float | None = Field(default=None, exclude_if=_exclude_none)
    sonar_passive: float | None = Field(default=None, exclude_if=_exclude_none)


class ContactEmissionResult(StrictModel):
    name: str | None = Field(default=None, exclude_if=_exclude_none)
    age_seconds: float | None = Field(default=None, exclude_if=_exclude_none)
    solid: bool | None = Field(default=None, exclude_if=_exclude_none)
    sensor_dbid: int | None = Field(default=None, exclude_if=_exclude_none)
    sensor_name: str | None = Field(default=None, exclude_if=_exclude_none)
    sensor_type: str | int | None = Field(default=None, exclude_if=_exclude_none)
    sensor_role: str | int | None = Field(default=None, exclude_if=_exclude_none)
    sensor_max_range_nm: float | None = Field(default=None, exclude_if=_exclude_none)


class LastDetectionResult(StrictModel):
    detector_guid: str | None = Field(default=None, exclude_if=_exclude_none)
    detect_sensor_guid: str | None = Field(default=None, exclude_if=_exclude_none)
    age_seconds: float | None = Field(default=None, exclude_if=_exclude_none)
    range_nm: float | None = Field(default=None, exclude_if=_exclude_none)
    special_mode: str | None = Field(default=None, exclude_if=_exclude_none)


class PotentialMatchResult(StrictModel):
    dbid: int | None = Field(default=None, exclude_if=_exclude_none)
    name: str | None = Field(default=None, exclude_if=_exclude_none)
    category: str | None = Field(default=None, exclude_if=_exclude_none)
    type: str | None = Field(default=None, exclude_if=_exclude_none)
    subtype: str | int | None = Field(default=None, exclude_if=_exclude_none)
    missile_defence: float | None = Field(default=None, exclude_if=_exclude_none)


class ContactBdaResult(StrictModel):
    fires: str | float | None = Field(default=None, exclude_if=_exclude_none)
    flood: str | float | None = Field(default=None, exclude_if=_exclude_none)
    structural: str | float | None = Field(default=None, exclude_if=_exclude_none)


class ContactDetailResult(ContactResult):
    age_seconds: float | None
    area_of_uncertainty: list[LatLonResult]
    detection_by: DetectionByResult | None
    emissions: list[ContactEmissionResult]
    last_detections: list[LastDetectionResult]
    potential_matches: list[PotentialMatchResult]
    bda: ContactBdaResult | None
    missile_defence: float | None
    detected_by_side_guid: str | None
    observer_side_guid_shared: str | None
    observer_posture: str | None
    marked_as_decoy: bool | None
    filter_out: bool | None
    targeted_by_unit_guids: list[NonEmptyStr]
    fired_on_by_unit_guids: list[NonEmptyStr]
    firing_at_contact_guids: list[NonEmptyStr]


class CargoItemResult(StrictModel):
    object_type: int = Field(ge=1, le=5)
    dbid: int = Field(ge=1)
    guid: str | None
    quantity: int = Field(ge=0)


class MissionAirRefuelingSettingsResult(StrictModel):
    use_refuel_unrep: RefuelUnrepValue | None
    tanker_usage: TankerUsage | None
    launch_without_tankers_in_place: bool | None
    tanker_follows_receivers: bool | None
    keep_on_mission_without_tankers_in_place: bool | None
    tanker_mission_guids: list[str]
    minimum_tankers_total: int | None
    minimum_tankers_airborne: int | None
    minimum_tankers_on_station: int | None
    max_receivers_in_queue_per_tanker: int | None
    fuel_percent_to_start_looking: float | None
    tanker_max_distance_nm: float | Literal["internal"] | None
    tanker_one_time: bool | None
    tanker_max_receivers: int | str | None


class MissionResult(StrictModel):
    guid: NonEmptyStr
    name: str
    side_name: str
    mission_class: MissionResultClass
    mission_class_string: str
    category: MissionCategory = "mission"
    parent_task_pool_guid: str | None = None
    package_guids: list[str] = Field(default_factory=list)
    active: bool
    start_time: str | None
    end_time: str | None
    assigned_unit_guids: list[str]
    target_guids: list[str]
    patrol_type: PatrolType | None
    strike_type: StrikeType | None
    reference_point_guids: list[str] | None
    prosecution_zone_reference_point_guids: list[str] | None = None
    destination_guid: str | None
    flight_size: int | None
    use_flight_size: bool | None = None
    minimum_aircraft_required: int | Literal["all"] | None = None
    on_station: int | None = None
    one_time_only: bool | None = None
    preplanned_only: bool | None = None
    one_third_rule: bool | None
    loop_type: MissionLoopTypeResult | None = Field(default=None, exclude_if=_exclude_none)
    active_emcon: bool | None = Field(default=None, exclude_if=_exclude_none)
    check_opa: bool | None = Field(default=None, exclude_if=_exclude_none)
    check_wwr: bool | None = Field(default=None, exclude_if=_exclude_none)
    group_size: int | None = Field(default=None, exclude_if=_exclude_none)
    use_group_size: bool | None = Field(default=None, exclude_if=_exclude_none)
    transit_throttle_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    transit_throttle_ship: MissionStageValue | None = Field(default=None, exclude_if=_exclude_none)
    transit_throttle_submarine: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    station_throttle_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    station_throttle_ship: MissionStageValue | None = Field(default=None, exclude_if=_exclude_none)
    station_throttle_submarine: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    attack_throttle_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    attack_throttle_ship: MissionStageValue | None = Field(default=None, exclude_if=_exclude_none)
    attack_throttle_submarine: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    transit_altitude_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    station_altitude_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    attack_altitude_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    transit_depth_submarine: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    station_depth_submarine: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    attack_depth_submarine: MissionStageValue | None = Field(default=None, exclude_if=_exclude_none)
    strike_minimum_trigger: str | None = Field(default=None, exclude_if=_exclude_none)
    strike_max_flights: int | None = Field(default=None, exclude_if=_exclude_none)
    strike_auto_planner: bool | None = Field(default=None, exclude_if=_exclude_none)
    strike_min_distance_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    strike_max_distance_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    strike_min_distance_ship: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    strike_max_distance_ship: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    focus_on_strike: bool | None = Field(default=None, exclude_if=_exclude_none)
    arming_delay: str | None = Field(default=None, exclude_if=_exclude_none)
    mines_per_set: int | None = Field(default=None, exclude_if=_exclude_none)
    mine_spacing_m: float | None = Field(default=None, exclude_if=_exclude_none)
    set_spacing_m: float | None = Field(default=None, exclude_if=_exclude_none)
    laying_method: Literal[0, 1] | None = Field(default=None, exclude_if=_exclude_none)
    cargo_subtype: Literal["transfer", "delivery"] | None = Field(
        default=None, exclude_if=_exclude_none
    )
    move_all_cargo: bool | None = Field(default=None, exclude_if=_exclude_none)
    allow_ground_self_delivery: bool | None = Field(default=None, exclude_if=_exclude_none)
    assigned_cargo: list[CargoItemResult] = Field(
        default_factory=lambda: list[CargoItemResult](), exclude_if=_exclude_empty
    )
    air_refueling: MissionAirRefuelingSettingsResult | None = Field(
        default=None, exclude_if=_exclude_none
    )


class DoctrineResult(StrictModel):
    scope: Literal["side", "unit", "mission"]
    target_guid: NonEmptyStr
    actual: bool
    weapon_control_air: WeaponControlValue | None
    weapon_control_surface: WeaponControlValue | None
    weapon_control_subsurface: WeaponControlValue | None
    weapon_control_land: WeaponControlValue | None
    nuclear_use: NuclearUseValue | None
    refuel_unrep: RefuelUnrepValue | None
    radar: EmconValue | None
    sonar: EmconValue | None
    oecm: EmconValue | None
    engage_opportunity_targets: bool | None = Field(default=None, exclude_if=_exclude_none)
    automatic_evasion: bool | None = Field(default=None, exclude_if=_exclude_none)
    ignore_plotted_course: bool | None = Field(default=None, exclude_if=_exclude_none)
    ignore_emcon_while_under_attack: bool | None = Field(default=None, exclude_if=_exclude_none)
    maintain_standoff: bool | None = Field(default=None, exclude_if=_exclude_none)
    use_sams_in_anti_surface_mode: bool | None = Field(default=None, exclude_if=_exclude_none)
    engaging_ambiguous_targets: DoctrineSettingValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    fuel_state_planned: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    fuel_state_rtb: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    weapon_state_planned: DoctrineSettingValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    weapon_state_rtb: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    withdraw_on_attack: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    withdraw_on_damage: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    withdraw_on_defence: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    withdraw_on_fuel: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    bvr_logic: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    dipping_sonar: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    use_aip: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    recharge_on_attack: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    recharge_on_patrol: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)


class ReferencePointResult(StrictModel):
    guid: NonEmptyStr
    name: str
    side_guid: NonEmptyStr
    latitude: float
    longitude: float
    relative_to_type: Literal["unit", "contact", "reference_point", "unknown"] | None = None
    relative_to_guid: str | None = None
    relative_bearing_deg: float | None = None
    relative_distance_nm: float | None = None
    bearing_type: ReferencePointBearingType | None = None


class UnitSetResult(StrictModel):
    unit_guid: NonEmptyStr
    name: str
    speed: float | None
    altitude: float | None
    heading: float | None
    course: list[CourseWaypoint] | None
    depth: float | None = Field(default=None, exclude_if=_exclude_none)
    throttle: str | None = Field(default=None, exclude_if=_exclude_none)
    force_speed: bool | None = Field(default=None, exclude_if=_exclude_none)
    desired_heading: float | None = Field(default=None, exclude_if=_exclude_none)
    move_to: bool | None = Field(default=None, exclude_if=_exclude_none)
    manual_throttle: str | float | None = Field(default=None, exclude_if=_exclude_none)
    manual_speed: str | float | None = Field(default=None, exclude_if=_exclude_none)
    manual_altitude: str | float | None = Field(default=None, exclude_if=_exclude_none)
    hold_position: bool | None = Field(default=None, exclude_if=_exclude_none)
    hold_fire: bool | None = Field(default=None, exclude_if=_exclude_none)
    sprint_drift: bool | None = Field(default=None, exclude_if=_exclude_none)
    avoid_cavitation: bool | None = Field(default=None, exclude_if=_exclude_none)
    obey_emcon: bool | None = Field(default=None, exclude_if=_exclude_none)


class UnitAddResult(StrictModel):
    unit_guid: NonEmptyStr
    name: str
    side_guid: NonEmptyStr
    dbid: int
    latitude: float
    longitude: float


class UnitAssignMissionResult(StrictModel):
    unit_guid: NonEmptyStr
    mission_guid: NonEmptyStr
    escort: bool


class UnitUnassignMissionResult(StrictModel):
    unit_guid: NonEmptyStr
    mission_guid: None
    escort: Literal[False]


class UnitCommandResult(StrictModel):
    unit_guid: NonEmptyStr
    command: Literal["launch", "rtb", "refuel"]
    accepted: Literal[True]
    operating: bool
    condition: str
    condition_code: str
    unit_state: str


class UnitAttackContactResult(StrictModel):
    attacker_unit_guid: NonEmptyStr
    contact_guid: NonEmptyStr
    mode: Literal["auto", "manual_weapon", "manual_target"]
    accepted: Literal[True]
    primary_target_contact_guid: str | None
    firing_at_contact_guids: list[NonEmptyStr]
    targeted_by_attacker: bool


class MissionCreateResult(StrictModel):
    mission_guid: NonEmptyStr
    name: str
    side_guid: NonEmptyStr
    mission_class: Literal[
        "patrol", "support", "strike", "ferry", "mining", "mine_clearing", "cargo"
    ]
    subtype: str | None
    category: MissionCategory = "mission"
    parent_task_pool_guid: str | None = None
    active: Literal[False]


class MissionAirRefuelingResult(MissionAirRefuelingSettingsResult):
    mission_guid: NonEmptyStr
    mission_name: str


class MissionFlightPlanWaypointResult(StrictModel):
    index: int = Field(ge=1)
    guid: str | None
    name: str
    description: str | None
    waypoint_type: str | int | None
    latitude: float
    longitude: float
    scheduled_time: str | None
    desired_speed: float | None
    desired_altitude_m: float | None
    terrain_following: bool | None
    preset_throttle: str | int | float | None
    preset_altitude: str | int | float | None
    preset_depth: str | int | float | None
    action_guid: str | None


class MissionFlightPlanResult(StrictModel):
    guid: NonEmptyStr
    name: str
    waypoints: list[MissionFlightPlanWaypointResult]


class MissionFlightPlanListResult(StrictModel):
    mission_guid: NonEmptyStr
    mission_name: str
    takeoff_time: str | None
    time_on_target: str | None
    flights: list[MissionFlightPlanResult]


class MissionFlightPlanCreateResult(MissionFlightPlanListResult):
    created_flight_guids: list[NonEmptyStr]


class MissionUpdateResult(StrictModel):
    mission_guid: NonEmptyStr
    name: str
    active: bool | None
    start_time: str | None
    end_time: str | None
    flight_size: int | None
    use_flight_size: bool | None = None
    minimum_aircraft_required: int | Literal["all"] | None = None
    on_station: int | None = None
    one_time_only: bool | None = None
    preplanned_only: bool | None = None
    one_third_rule: bool | None
    reference_point_guids: list[str] | None
    prosecution_zone_reference_point_guids: list[str] | None
    destination_guid: str | None = Field(default=None, exclude_if=_exclude_none)
    loop_type: MissionLoopTypeResult | None = Field(default=None, exclude_if=_exclude_none)
    active_emcon: bool | None = Field(default=None, exclude_if=_exclude_none)
    check_opa: bool | None = Field(default=None, exclude_if=_exclude_none)
    check_wwr: bool | None = Field(default=None, exclude_if=_exclude_none)
    group_size: int | None = Field(default=None, exclude_if=_exclude_none)
    use_group_size: bool | None = Field(default=None, exclude_if=_exclude_none)
    transit_throttle_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    transit_throttle_ship: MissionStageValue | None = Field(default=None, exclude_if=_exclude_none)
    transit_throttle_submarine: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    station_throttle_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    station_throttle_ship: MissionStageValue | None = Field(default=None, exclude_if=_exclude_none)
    station_throttle_submarine: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    attack_throttle_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    attack_throttle_ship: MissionStageValue | None = Field(default=None, exclude_if=_exclude_none)
    attack_throttle_submarine: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    transit_altitude_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    station_altitude_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    attack_altitude_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    transit_depth_submarine: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    station_depth_submarine: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    attack_depth_submarine: MissionStageValue | None = Field(default=None, exclude_if=_exclude_none)
    strike_minimum_trigger: str | None = Field(default=None, exclude_if=_exclude_none)
    strike_max_flights: int | None = Field(default=None, exclude_if=_exclude_none)
    strike_auto_planner: bool | None = Field(default=None, exclude_if=_exclude_none)
    strike_min_distance_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    strike_max_distance_aircraft: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    strike_min_distance_ship: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    strike_max_distance_ship: MissionStageValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    focus_on_strike: bool | None = Field(default=None, exclude_if=_exclude_none)
    arming_delay: str | None = Field(default=None, exclude_if=_exclude_none)
    mines_per_set: int | None = Field(default=None, exclude_if=_exclude_none)
    mine_spacing_m: float | None = Field(default=None, exclude_if=_exclude_none)
    set_spacing_m: float | None = Field(default=None, exclude_if=_exclude_none)
    laying_method: Literal[0, 1] | None = Field(default=None, exclude_if=_exclude_none)
    cargo_subtype: Literal["transfer", "delivery"] | None = Field(
        default=None, exclude_if=_exclude_none
    )
    move_all_cargo: bool | None = Field(default=None, exclude_if=_exclude_none)
    allow_ground_self_delivery: bool | None = Field(default=None, exclude_if=_exclude_none)
    assigned_cargo: list[CargoItemResult] = Field(
        default_factory=lambda: list[CargoItemResult](), exclude_if=_exclude_empty
    )


class MissionTargetAddResult(StrictModel):
    mission_guid: NonEmptyStr
    target_guid: NonEmptyStr
    assigned: Literal[True]
    target_guids: list[str]


class MissionTargetRemoveResult(StrictModel):
    mission_guid: NonEmptyStr
    target_guid: NonEmptyStr
    assigned: Literal[False]
    target_guids: list[str]


class DoctrineSetResult(StrictModel):
    scope: Literal["side", "unit", "mission"]
    target_guid: NonEmptyStr
    weapon_control_air: WeaponControlValue | None
    weapon_control_surface: WeaponControlValue | None
    weapon_control_subsurface: WeaponControlValue | None
    weapon_control_land: WeaponControlValue | None
    nuclear_use: NuclearUseValue | None
    refuel_unrep: RefuelUnrepValue | None
    engage_opportunity_targets: bool | None = Field(default=None, exclude_if=_exclude_none)
    automatic_evasion: bool | None = Field(default=None, exclude_if=_exclude_none)
    ignore_plotted_course: bool | None = Field(default=None, exclude_if=_exclude_none)
    ignore_emcon_while_under_attack: bool | None = Field(default=None, exclude_if=_exclude_none)
    maintain_standoff: bool | None = Field(default=None, exclude_if=_exclude_none)
    use_sams_in_anti_surface_mode: bool | None = Field(default=None, exclude_if=_exclude_none)
    engaging_ambiguous_targets: DoctrineSettingValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    fuel_state_planned: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    fuel_state_rtb: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    weapon_state_planned: DoctrineSettingValue | None = Field(
        default=None, exclude_if=_exclude_none
    )
    weapon_state_rtb: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    withdraw_on_attack: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    withdraw_on_damage: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    withdraw_on_defence: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    withdraw_on_fuel: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    bvr_logic: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    dipping_sonar: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    use_aip: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    recharge_on_attack: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    recharge_on_patrol: DoctrineSettingValue | None = Field(default=None, exclude_if=_exclude_none)


class ScenarioTimeCompressionSetResult(StrictModel):
    code: Literal[0, 1, 2, 3, 4, 5]
    observed_time_compression: float | None
    accepted: Literal[True]


class SidePostureResult(StrictModel):
    side_a_guid: NonEmptyStr
    side_b_guid: NonEmptyStr
    posture: str


class ContactWeaponAllocationResult(StrictModel):
    shooter_guid: NonEmptyStr
    quantity_assigned: int = Field(ge=0)
    weapon_dbid: int = Field(ge=1)
    weapon_name: str
    target_guid: NonEmptyStr
    quantity_fired: int = Field(ge=0)


class ContactWeaponAllocationsResult(StrictModel):
    contact_guid: NonEmptyStr
    allocations: list[ContactWeaponAllocationResult]


class WeaponLoadedResult(StrictModel):
    guid: str | None = Field(default=None, exclude_if=_exclude_none)
    dbid: int
    name: str
    type_code: int | None = Field(default=None, exclude_if=_exclude_none)
    current: int | None = Field(default=None, ge=0, exclude_if=_exclude_none)
    max_capacity: int | None = Field(default=None, ge=0, exclude_if=_exclude_none)
    default: int | None = Field(default=None, ge=0, exclude_if=_exclude_none)


class SensorResult(StrictModel):
    guid: NonEmptyStr
    dbid: int
    name: str
    max_range_nm: float | None = Field(default=None, exclude_if=_exclude_none)
    active: bool | None = Field(default=None, exclude_if=_exclude_none)
    status: str | None = Field(default=None, exclude_if=_exclude_none)
    role: str | int | None = Field(default=None, exclude_if=_exclude_none)
    type: str | int | None = Field(default=None, exclude_if=_exclude_none)


class MountResult(StrictModel):
    guid: NonEmptyStr
    dbid: int
    name: str
    status: str | None = Field(default=None, exclude_if=_exclude_none)
    status_readable: str | None = Field(default=None, exclude_if=_exclude_none)
    damage: str | float | None = Field(default=None, exclude_if=_exclude_none)
    rate_of_fire: float | None = Field(default=None, exclude_if=_exclude_none)
    capacity: int | None = Field(default=None, ge=0, exclude_if=_exclude_none)
    weapons: list[WeaponLoadedResult] = Field(
        default_factory=lambda: list[WeaponLoadedResult](), exclude_if=_exclude_empty
    )


class MagazineResult(StrictModel):
    guid: NonEmptyStr
    dbid: int | None = Field(default=None, exclude_if=_exclude_none)
    name: str
    status: str | None = Field(default=None, exclude_if=_exclude_none)
    status_readable: str | None = Field(default=None, exclude_if=_exclude_none)
    damage: str | float | None = Field(default=None, exclude_if=_exclude_none)
    armor: str | float | None = Field(default=None, exclude_if=_exclude_none)
    is_aviation_magazine: bool | None = Field(default=None, exclude_if=_exclude_none)
    capacity: int | None = Field(default=None, ge=0, exclude_if=_exclude_none)
    weapons: list[WeaponLoadedResult] = Field(
        default_factory=lambda: list[WeaponLoadedResult](), exclude_if=_exclude_empty
    )


class CargoResult(StrictModel):
    guid: str | None = Field(default=None, exclude_if=_exclude_none)
    object_type: int | None = Field(default=None, exclude_if=_exclude_none)
    storage_type: str | int | None = Field(default=None, exclude_if=_exclude_none)
    dbid: int
    name: str
    status: str | None = Field(default=None, exclude_if=_exclude_none)
    status_readable: str | None = Field(default=None, exclude_if=_exclude_none)
    quantity: int | None = Field(default=None, ge=0, exclude_if=_exclude_none)
    damage: str | float | None = Field(default=None, exclude_if=_exclude_none)
    area: float | None = Field(default=None, exclude_if=_exclude_none)
    crew: int | None = Field(default=None, ge=0, exclude_if=_exclude_none)
    mass: float | None = Field(default=None, exclude_if=_exclude_none)
    required_size: float | None = Field(default=None, exclude_if=_exclude_none)
    container_cargo: bool | None = Field(default=None, exclude_if=_exclude_none)


class UnitInventoryResult(StrictModel):
    unit_guid: NonEmptyStr
    sensors: list[SensorResult]
    mounts: list[MountResult]
    magazines: list[MagazineResult]
    cargo: list[CargoResult]


class UnitSensorSetResult(StrictModel):
    unit_guid: NonEmptyStr
    sensor_guid: NonEmptyStr
    active: bool
    obey_emcon: bool | None


class UnitCargoTransferResult(StrictModel):
    from_unit_guid: NonEmptyStr
    to_unit_guid: NonEmptyStr
    command: Literal["transfer"]
    accepted: Literal[True]


class UnitCargoUnloadResult(StrictModel):
    unit_guid: NonEmptyStr
    command: Literal["unload"]
    accepted: Literal[True]


class MissionCargoUpdateResult(StrictModel):
    mission_guid: NonEmptyStr
    action: Literal["assign", "unassign"]
    assigned_cargo: list[CargoItemResult]


class DoctrineWraEntryResult(StrictModel):
    weapon_dbid: int = Field(ge=1)
    weapon_name: str
    weapons_per_salvo: WraSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    shooters_per_salvo: WraSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    firing_range: WraSettingValue | None = Field(default=None, exclude_if=_exclude_none)
    self_defence_range: WraSettingValue | None = Field(default=None, exclude_if=_exclude_none)


class DoctrineWraResult(StrictModel):
    scope: Literal["side", "unit", "mission"]
    target_guid: NonEmptyStr
    level: str
    target_type: str | int | None = Field(default=None, exclude_if=_exclude_none)
    entries: list[DoctrineWraEntryResult]


class SpecialActionResult(StrictModel):
    guid: NonEmptyStr
    name: str
    description: str
    active: bool
    repeatable: bool


class SpecialActionListResult(StrictModel):
    items: list[SpecialActionResult]


class SpecialActionExecuteResult(StrictModel):
    action_guid: NonEmptyStr
    name: str
    accepted: Literal[True]
    active: bool
    repeatable: bool


class EmconSetResult(StrictModel):
    scope: Literal["side", "mission", "group", "unit"]
    target_guid: NonEmptyStr
    inherit: bool | None
    radar: EmconValue | None
    sonar: EmconValue | None
    oecm: EmconValue | None


class DeleteResult(StrictModel):
    deleted_guid: NonEmptyStr
    deleted_name: str
    object_kind: Literal["unit", "mission"]


class DestructivePreviewResult(StrictModel):
    operation: Literal["unit.delete", "mission.delete", "bridge.reconcile"]
    target_guid: NonEmptyStr
    target_name: str
    target_type: str
    impact: str
    reserved_activation_candidate: UUID
    confirmation_token: NonEmptyStr
    expires_at_utc: datetime

    @field_validator("expires_at_utc")
    @classmethod
    def validate_utc_expiry(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != timedelta(0):
            raise ValueError("confirmation expiry must be timezone-aware UTC")
        return value


class ScoreResult(StrictModel):
    side: NonEmptyStr
    score: int


class PrepareResult(StrictModel):
    managed_asset_version: RuntimeVersion
    managed_asset_sha256: Sha256
    runtime_tag: NonEmptyStr
    runtime_asset_sha256: Sha256
    release_id: Sha256
    install_command: NonEmptyStr
    inbox_path: NonEmptyStr
    prepared: bool
    scenario_installed: bool

    @model_validator(mode="after")
    def validate_runtime_identity(self) -> Self:
        if self.runtime_tag != derive_runtime_tag(
            self.managed_asset_version, self.runtime_asset_sha256
        ):
            raise ValueError("runtime tag does not match runtime version and asset digest")
        return self


CheckState = Literal["ok", "warning", "error", "unknown"]


class DoctorCheck(StrictModel):
    state: CheckState
    detail: str | None


class DoctorResult(StrictModel):
    overall_state: CheckState
    build_check: DoctorCheck
    profile_check: DoctorCheck
    asset_check: DoctorCheck
    scenario_check: DoctorCheck
    process_check: DoctorCheck
    warnings: list[str]
    required_next_action: str | None


class PrimitiveObservations(StrictModel):
    run_script: bool
    nested_delivery_global: bool
    delivery_global_cleaned: bool
    export_inst_empty_list: bool
    unicode_roundtrip: bool
    manifest_match: bool
    special_action_while_paused: bool | None


class CapacityObservations(StrictModel):
    max_verified_comments_bytes: int = Field(ge=1, le=1_048_576)
    safe_payload_bytes: int = Field(ge=1, le=1_048_576)
    verified_ledger_entries: int = Field(ge=1, le=256)
    effective_ledger_capacity: int = Field(ge=1, le=256)

    @model_validator(mode="after")
    def validate_capacity(self) -> Self:
        if self.effective_ledger_capacity > self.verified_ledger_entries:
            raise ValueError("effective ledger capacity exceeds verified entries")
        return self


class CompatibilityProfileResult(StrictModel):
    build: int = Field(ge=1)
    runtime_version: RuntimeVersion
    runtime_tag: NonEmptyStr
    runtime_asset_sha256: Sha256
    release_id: Sha256
    protocol: Literal["cmo-agent-bridge/1"]
    manifest_sha256: Sha256
    primitive_observations: PrimitiveObservations
    capacity_observations: CapacityObservations
    profile_path: NonEmptyStr

    @model_validator(mode="after")
    def validate_runtime_identity(self) -> Self:
        if self.runtime_tag != derive_runtime_tag(self.runtime_version, self.runtime_asset_sha256):
            raise ValueError("runtime tag does not match runtime version and asset digest")
        return self


class CompatAutomaticResult(StrictModel):
    phase: Literal["automatic"]
    case: CompatCase | None
    completed: bool
    primitive_observations: PrimitiveObservations
    capacity_observations: CapacityObservations
    profile: CompatibilityProfileResult | None


class CompatArmResult(StrictModel):
    phase: Literal["arm-paused-special-action"]
    nonce: NonEmptyStr
    action_name: NonEmptyStr
    instructions: NonEmptyStr


class CompatCollectResult(StrictModel):
    phase: Literal["collect-paused-special-action"]
    special_action_while_paused: bool
    profile: CompatibilityProfileResult


CompatProbeResult: TypeAlias = Annotated[
    CompatAutomaticResult | CompatArmResult | CompatCollectResult,
    Field(discriminator="phase"),
]


PollSource = Literal["automatic", "special-action"]


class ObservationalProbeStepResult(StrictModel):
    step: Literal["observational"]
    nonce: NonEmptyStr
    poll_source: PollSource
    observed: bool


class PayloadProbeStepResult(StrictModel):
    step: Literal["payload"]
    nonce: NonEmptyStr
    poll_source: PollSource
    payload_bytes: int = Field(ge=1, le=1_048_576)


class HighSpeedProbeStepResult(StrictModel):
    step: Literal["high-speed"]
    nonce: NonEmptyStr
    poll_source: PollSource
    elapsed_seconds: float = Field(ge=0)


class LineageProbeStepResult(StrictModel):
    step: Literal["lineage"]
    nonce: NonEmptyStr
    poll_source: PollSource
    lineage_id: UUID


class KeyValueProbeStepResult(StrictModel):
    step: Literal["key-value"]
    nonce: NonEmptyStr
    poll_source: PollSource
    observed: bool


class DedupeProbeStepResult(StrictModel):
    step: Literal["dedupe"]
    nonce: NonEmptyStr
    poll_source: PollSource
    counter: int = Field(ge=0)


class IndeterminateProbeStepResult(StrictModel):
    step: Literal["indeterminate"]
    nonce: NonEmptyStr
    poll_source: PollSource
    quarantined: bool


class LedgerCapacityProbeStepResult(StrictModel):
    step: Literal["ledger-capacity"]
    nonce: NonEmptyStr
    poll_source: PollSource
    verified_ledger_entries: int = Field(ge=1, le=256)


class ApplyProfileProbeStepResult(StrictModel):
    step: Literal["apply-profile"]
    nonce: NonEmptyStr
    poll_source: PollSource
    applied: bool
    safe_payload_bytes: int = Field(ge=1, le=1_048_576)
    verified_ledger_entries: int = Field(ge=1, le=256)
    effective_ledger_capacity: int = Field(ge=1, le=256)


CompatProbeStepResult: TypeAlias = Annotated[
    ObservationalProbeStepResult
    | PayloadProbeStepResult
    | HighSpeedProbeStepResult
    | LineageProbeStepResult
    | KeyValueProbeStepResult
    | DedupeProbeStepResult
    | IndeterminateProbeStepResult
    | LedgerCapacityProbeStepResult
    | ApplyProfileProbeStepResult,
    Field(discriminator="step"),
]


class UninstallCommandResult(StrictModel):
    phase: Literal["command"]
    command: NonEmptyStr
    expected_lineage_id: UUID
    expected_activation_id: UUID


class UninstallFilesResult(StrictModel):
    phase: Literal["files"]
    verified_marker: NonEmptyStr
    removed_managed_paths: list[NonEmptyStr]
    removed_count: int = Field(ge=0)
    retained_nonempty_directories: list[NonEmptyStr]

    @model_validator(mode="after")
    def validate_removed_count(self) -> Self:
        if self.removed_count != len(self.removed_managed_paths):
            raise ValueError("removed_count does not match removed_managed_paths")
        return self


UninstallResult: TypeAlias = Annotated[
    UninstallCommandResult | UninstallFilesResult, Field(discriminator="phase")
]


class ReconciliationEvidence(StrictModel):
    present: bool
    request_id: UUID | None
    state: NonEmptyStr | None
    request_hash: Sha256 | None

    @model_validator(mode="after")
    def validate_presence(self) -> Self:
        evidence = (self.request_id, self.state, self.request_hash)
        if self.present and any(value is None for value in evidence):
            raise ValueError("present reconciliation evidence requires complete identity")
        if not self.present and any(value is not None for value in evidence):
            raise ValueError("absent reconciliation evidence forbids identity fields")
        return self


class ReconcileProbeResult(StrictModel):
    barrier_evidence: ReconciliationEvidence
    ledger_evidence: ReconciliationEvidence
    allowed_dispositions: list[Disposition]
    quarantined: bool

    @field_validator("allowed_dispositions")
    @classmethod
    def validate_unique_dispositions(cls, value: list[str]) -> list[str]:
        return _require_unique(value, "allowed_dispositions")

    @model_validator(mode="after")
    def validate_evidence_consistency(self) -> Self:
        barrier = self.barrier_evidence
        ledger = self.ledger_evidence
        if (
            barrier.present
            and ledger.present
            and (
                barrier.request_id != ledger.request_id
                or barrier.request_hash != ledger.request_hash
            )
        ):
            raise ValueError("barrier and ledger evidence identities do not match")
        present = [evidence for evidence in (barrier, ledger) if evidence.present]
        states = {evidence.state for evidence in present}
        uncertain = {"in_progress", "indeterminate"}
        terminal = {"completed", "cancelled", "resolved"}
        if not present:
            expected_dispositions: set[str] = set()
            expected_quarantined = False
        elif states <= uncertain:
            expected_dispositions = {"applied", "not_applied"}
            expected_quarantined = True
        elif states <= terminal and len(states) == 1:
            expected_dispositions = set()
            expected_quarantined = barrier.present
        else:
            expected_dispositions = set()
            expected_quarantined = True
        if set(self.allowed_dispositions) != expected_dispositions:
            raise ValueError("allowed dispositions do not match reconciliation evidence")
        if self.quarantined is not expected_quarantined:
            raise ValueError("quarantine state does not match reconciliation evidence")
        return self


class ReconcileCommitResult(StrictModel):
    request_id: UUID
    request_hash: Sha256
    disposition: Disposition
    resolved: Literal[True]


class ReconciliationResult(StrictModel):
    request_id: UUID | None
    journal_evidence: ReconciliationEvidence
    barrier_evidence: ReconciliationEvidence
    ledger_evidence: ReconciliationEvidence
    allowed_dispositions: list[Disposition]
    applied_disposition: Disposition | None
    quarantined: bool
    confirmation_token: str | None
    confirmation_expires_at_utc: datetime | None
    reserved_activation_candidate: UUID | None

    @field_validator("confirmation_expires_at_utc")
    @classmethod
    def validate_utc_expiry(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() != timedelta(0)):
            raise ValueError("confirmation expiry must be timezone-aware UTC")
        return value


ResultT = TypeVar("ResultT")


class PagedResult(StrictModel, Generic[ResultT]):
    items: list[ResultT]
    next_cursor: str | None
