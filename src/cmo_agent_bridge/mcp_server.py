from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Annotated, Literal, Protocol, TypeVar, cast

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field, JsonValue, ValidationError

from cmo_agent_bridge.application.models import InvocationOutcome
from cmo_agent_bridge.errors import BridgeError
from cmo_agent_bridge.mcp_runtime import McpBridgeDiagnostic, McpBridgePrepareResult
from cmo_agent_bridge.operations.models import (
    BridgeStatusResult,
    CargoTransferItem,
    ContactDetailResult,
    ContactResult,
    ContactWeaponAllocationsResult,
    CourseWaypoint,
    DoctrineSettingValue,
    DoctrineResult,
    DoctrineSetResult,
    DoctrineWraResult,
    EmconSetResult,
    EmconValue,
    FlightSize,
    MissionCargoUpdateResult,
    MissionAirRefuelingResult,
    MissionCategory,
    MissionCreateResult,
    MissionDetails,
    MissionFlightPlanCreateResult,
    MissionFlightPlanListResult,
    MissionResult,
    MissionStageValue,
    MissionTargetAddResult,
    MissionTargetRemoveResult,
    MissionUpdateResult,
    MinimumAircraftRequired,
    NuclearUseValue,
    OrderedReferencePointGuidList,
    PagedResult,
    ReferencePointResult,
    ReferencePointAnchorType,
    ReferencePointBearingType,
    RefuelUnrepValue,
    ScenarioResult,
    ScoreResult,
    SidePostureResult,
    SideResult,
    SpecialActionExecuteResult,
    SpecialActionListResult,
    ScenarioTimeCompressionSetResult,
    UnitAddResult,
    UnitAttackContactResult,
    UnitAssignMissionResult,
    UnitCargoTransferResult,
    UnitCargoUnloadResult,
    UnitCombatStatusResult,
    UnitCommandResult,
    UnitInventoryResult,
    UnitLoadoutResult,
    UnitResult,
    UnitSensorSetResult,
    UnitSetResult,
    UnitUnassignMissionResult,
    TankerUsage,
    WraSettingValue,
    WeaponControlUpdateValue,
)
from cmo_agent_bridge.scenario_authoring_mcp import register_scenario_authoring_tools


class ApplicationPort(Protocol):
    async def execute(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
        *,
        confirmation_token: str | None = None,
    ) -> InvocationOutcome: ...


class McpApplicationPort(ApplicationPort, Protocol):
    async def diagnose(self) -> McpBridgeDiagnostic: ...

    async def prepare(
        self,
        *,
        game_root: str | None = None,
        replace_saved_game_root: bool = False,
    ) -> McpBridgePrepareResult: ...


ResultModelT = TypeVar("ResultModelT", bound=BaseModel)
DoctrineBooleanUpdateValue = bool | Literal["inherit"]


def _read_only_annotations() -> ToolAnnotations:
    return ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )


def _mutation_annotations() -> ToolAnnotations:
    return ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )


def _create_annotations() -> ToolAnnotations:
    return ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=False,
    )


def _non_idempotent_mutation_annotations() -> ToolAnnotations:
    return ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=False,
    )


def _destructive_mutation_annotations() -> ToolAnnotations:
    return ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=False,
        openWorldHint=False,
    )


def _protocol_tool_error(operation: str) -> ToolError:
    return ToolError(
        json.dumps(
            {
                "code": "PROTOCOL_ERROR",
                "message": "CMO bridge returned an invalid result",
                "details": {"operation": operation},
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _bridge_tool_error(error: BridgeError) -> ToolError:
    return ToolError(
        json.dumps(
            error.to_payload(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )


async def _invoke(
    application: ApplicationPort,
    operation: str,
    arguments: Mapping[str, JsonValue],
    result_model: type[ResultModelT],
) -> ResultModelT:
    outcome = await application.execute(operation, arguments)
    if not outcome.ok:
        error = outcome.error
        if error is None:
            raise _protocol_tool_error(operation)
        raise ToolError(
            json.dumps(
                error,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    if outcome.result is None:
        raise _protocol_tool_error(operation)
    try:
        return result_model.model_validate(outcome.result)
    except ValidationError as error:
        raise _protocol_tool_error(operation) from error


def create_mcp_server(application: McpApplicationPort) -> FastMCP[None]:
    """Create the typed CMO MCP surface."""
    server = FastMCP(
        "CMO Agent Bridge",
        instructions=(
            "Read and update the running Command: Modern Operations scenario through a local "
            "file-backed bridge. Use cmo_bridge_diagnose and cmo_bridge_prepare when the "
            "release-bound runtime is not ready. Live CMO calls require the polling event."
        ),
        log_level="ERROR",
    )

    async def bridge_diagnose() -> McpBridgeDiagnostic:
        return await application.diagnose()

    server.add_tool(
        bridge_diagnose,
        name="cmo_bridge_diagnose",
        title="Diagnose CMO bridge setup",
        description=(
            "Inspect host-side game-root and release-runtime readiness without contacting CMO. "
            "Use the returned action when setup is incomplete."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def bridge_prepare(
        game_root: str | None = None,
        replace_saved_game_root: bool = False,
    ) -> McpBridgePrepareResult:
        try:
            return await application.prepare(
                game_root=game_root,
                replace_saved_game_root=replace_saved_game_root,
            )
        except BridgeError as error:
            raise _bridge_tool_error(error) from error

    server.add_tool(
        bridge_prepare,
        name="cmo_bridge_prepare",
        title="Prepare CMO bridge runtime",
        description=(
            "Deploy this release's local Lua runtime and activate ordinary CMO tools in the "
            "current MCP session. Omit game_root to use the server override or saved root."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def bridge_status(accept_lineage_id: str | None = None) -> BridgeStatusResult:
        return await _invoke(
            application,
            "bridge.status",
            {"accept_lineage_id": accept_lineage_id},
            BridgeStatusResult,
        )

    server.add_tool(
        bridge_status,
        name="cmo_bridge_status",
        title="CMO bridge status",
        description=(
            "Check the live CMO process, scenario bridge, and runtime identity. Pass an observed "
            "lineage ID to explicitly accept a newly loaded scenario."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def scenario_get() -> ScenarioResult:
        return await _invoke(application, "scenario.get", {}, ScenarioResult)

    server.add_tool(
        scenario_get,
        name="cmo_scenario_get",
        title="Get CMO scenario",
        description="Return metadata and time state for the currently loaded CMO scenario.",
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def scenario_time_compression_set(
        code: Annotated[int, Field(ge=0, le=5)],
    ) -> ScenarioTimeCompressionSetResult:
        return await _invoke(
            application,
            "scenario.time_compression.set",
            {"code": code},
            ScenarioTimeCompressionSetResult,
        )

    server.add_tool(
        scenario_time_compression_set,
        name="cmo_scenario_time_compression_set",
        title="Set CMO time compression",
        description=(
            "Set the running scenario's time-compression code and return the requested code plus "
            "the actual multiplier readback: "
            "0=1x, 1=2x, 2=5x, 3=15x, 4=coarse one-second slices, and "
            "5=coarse five-second slices. Before consequential multi-step planning or mutation, "
            "preserve cmo_scenario_get's multiplier, map 1/2/5/15/30/150 back to code "
            "0/1/2/3/4/5, set code 0, refresh the relevant state, complete and verify the work, "
            "then restore the mapped code."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def side_list(
        page_size: Annotated[int, Field(ge=1, le=500)] = 100,
        cursor: str | None = None,
    ) -> PagedResult[SideResult]:
        return await _invoke(
            application,
            "side.list",
            {"page_size": page_size, "cursor": cursor},
            PagedResult[SideResult],
        )

    server.add_tool(
        side_list,
        name="cmo_side_list",
        title="List CMO sides",
        description="List sides in the current CMO scenario, with cursor-based paging.",
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def side_posture_get(
        side_a_guid: Annotated[str, Field(min_length=1)],
        side_b_guid: Annotated[str, Field(min_length=1)],
    ) -> SidePostureResult:
        return await _invoke(
            application,
            "side.posture.get",
            {"side_a_guid": side_a_guid, "side_b_guid": side_b_guid},
            SidePostureResult,
        )

    server.add_tool(
        side_posture_get,
        name="cmo_side_posture_get",
        title="Get CMO side posture",
        description="Return how one side currently regards another side.",
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def unit_list(
        side_guid: str | None = None,
        side_name: str | None = None,
        page_size: Annotated[int, Field(ge=1, le=500)] = 100,
        cursor: str | None = None,
        unit_type: str | None = None,
        name_contains: str | None = None,
    ) -> PagedResult[UnitResult]:
        return await _invoke(
            application,
            "unit.list",
            {
                "side_guid": side_guid,
                "side_name": side_name,
                "page_size": page_size,
                "cursor": cursor,
                "unit_type": unit_type,
                "name_contains": name_contains,
            },
            PagedResult[UnitResult],
        )

    server.add_tool(
        unit_list,
        name="cmo_unit_list",
        title="List CMO units",
        description=(
            "List units for exactly one side, optionally filtering by unit type or a "
            "case-insensitive name fragment."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def unit_get(
        unit_guid: str | None = None,
        side_guid: str | None = None,
        side_name: str | None = None,
        unit_name: str | None = None,
    ) -> UnitResult:
        return await _invoke(
            application,
            "unit.get",
            {
                "unit_guid": unit_guid,
                "side_guid": side_guid,
                "side_name": side_name,
                "unit_name": unit_name,
            },
            UnitResult,
        )

    server.add_tool(
        unit_get,
        name="cmo_unit_get",
        title="Get CMO unit",
        description="Get one unit by GUID, or by one side selector plus its exact name.",
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def unit_combat_status_get(
        unit_guid: Annotated[str, Field(min_length=1)],
    ) -> UnitCombatStatusResult:
        return await _invoke(
            application,
            "unit.combat_status.get",
            {"unit_guid": unit_guid},
            UnitCombatStatusResult,
        )

    server.add_tool(
        unit_combat_status_get,
        name="cmo_unit_combat_status_get",
        title="Get CMO unit combat status",
        description=(
            "Return one unit's current operating condition, damage, fuel, weapon readiness, "
            "and combat-target relationships."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def unit_loadout_get(
        unit_guid: Annotated[str, Field(min_length=1)],
    ) -> UnitLoadoutResult:
        return await _invoke(
            application,
            "unit.loadout.get",
            {"unit_guid": unit_guid},
            UnitLoadoutResult,
        )

    server.add_tool(
        unit_loadout_get,
        name="cmo_unit_loadout_get",
        title="Get CMO unit loadout",
        description="Return one unit's current loadout, ready time, and weapon quantities.",
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def unit_inventory_get(
        unit_guid: Annotated[str, Field(min_length=1)],
    ) -> UnitInventoryResult:
        return await _invoke(
            application,
            "unit.inventory.get",
            {"unit_guid": unit_guid},
            UnitInventoryResult,
        )

    server.add_tool(
        unit_inventory_get,
        name="cmo_unit_inventory_get",
        title="Get CMO unit inventory",
        description=(
            "Return one unit's sensors, mounts, magazines, cargo, and weapon quantities with "
            "their observed operating state."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def unit_sensor_set(
        unit_guid: Annotated[str, Field(min_length=1)],
        sensor_guid: Annotated[str, Field(min_length=1)],
        active: bool,
        obey_emcon: bool | None = None,
    ) -> UnitSensorSetResult:
        return await _invoke(
            application,
            "unit.sensor.set",
            {
                "unit_guid": unit_guid,
                "sensor_guid": sensor_guid,
                "active": active,
                "obey_emcon": obey_emcon,
            },
            UnitSensorSetResult,
        )

    server.add_tool(
        unit_sensor_set,
        name="cmo_unit_sensor_set",
        title="Set CMO unit sensor state",
        description=(
            "Activate or deactivate one existing sensor by GUID, optionally changing whether "
            "the unit obeys EMCON."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def unit_magazine_adjust(
        unit_guid: Annotated[str, Field(min_length=1)],
        magazine_guid: Annotated[str, Field(min_length=1)],
        weapon_dbid: Annotated[int, Field(ge=1)],
        mode: Literal["add", "remove", "fill"],
        quantity: Annotated[int | None, Field(ge=1)] = None,
        max_capacity: Annotated[int | None, Field(ge=1)] = None,
        allow_new: bool = False,
    ) -> UnitInventoryResult:
        return await _invoke(
            application,
            "unit.magazine.adjust",
            {
                "unit_guid": unit_guid,
                "magazine_guid": magazine_guid,
                "weapon_dbid": weapon_dbid,
                "mode": mode,
                "quantity": quantity,
                "max_capacity": max_capacity,
                "allow_new": allow_new,
            },
            UnitInventoryResult,
        )

    server.add_tool(
        unit_magazine_adjust,
        name="cmo_unit_magazine_adjust",
        title="Adjust CMO unit magazine",
        description=(
            "Directly add, remove, or fill a weapon record in a unit magazine. This is an "
            "umpire or scenario-author inventory edit, not normal replenishment."
        ),
        annotations=_non_idempotent_mutation_annotations(),
        structured_output=True,
    )

    async def unit_mount_reload_adjust(
        unit_guid: Annotated[str, Field(min_length=1)],
        mount_guid: Annotated[str, Field(min_length=1)],
        weapon_dbid: Annotated[int, Field(ge=1)],
        mode: Literal["add", "remove", "fill"],
        quantity: Annotated[int | None, Field(ge=1)] = None,
        add_as_cell: bool = True,
    ) -> UnitInventoryResult:
        return await _invoke(
            application,
            "unit.mount_reload.adjust",
            {
                "unit_guid": unit_guid,
                "mount_guid": mount_guid,
                "weapon_dbid": weapon_dbid,
                "mode": mode,
                "quantity": quantity,
                "add_as_cell": add_as_cell,
            },
            UnitInventoryResult,
        )

    server.add_tool(
        unit_mount_reload_adjust,
        name="cmo_unit_mount_reload_adjust",
        title="Adjust CMO unit mount reloads",
        description=(
            "Directly add, remove, or fill reloads for one mount. This is an umpire or "
            "scenario-author inventory edit, not normal replenishment."
        ),
        annotations=_non_idempotent_mutation_annotations(),
        structured_output=True,
    )

    async def unit_set(
        unit_guid: Annotated[str, Field(min_length=1)],
        name: str | None = None,
        speed: Annotated[float | None, Field(ge=0)] = None,
        altitude: float | None = None,
        depth: float | None = None,
        heading: Annotated[float | None, Field(ge=0, le=360)] = None,
        throttle: str | None = None,
        force_speed: bool | None = None,
        desired_heading: Annotated[float | None, Field(ge=0, le=360)] = None,
        move_to: bool | None = None,
        manual_throttle: str | float | None = None,
        manual_speed: str | float | None = None,
        manual_altitude: str | float | None = None,
        hold_position: bool | None = None,
        hold_fire: bool | None = None,
        sprint_drift: bool | None = None,
        avoid_cavitation: bool | None = None,
        obey_emcon: bool | None = None,
        course: list[CourseWaypoint] | None = None,
    ) -> UnitSetResult:
        return await _invoke(
            application,
            "unit.set",
            {
                "unit_guid": unit_guid,
                "name": name,
                "speed": speed,
                "altitude": altitude,
                "depth": depth,
                "heading": heading,
                "throttle": throttle,
                "force_speed": force_speed,
                "desired_heading": desired_heading,
                "move_to": move_to,
                "manual_throttle": manual_throttle,
                "manual_speed": manual_speed,
                "manual_altitude": manual_altitude,
                "hold_position": hold_position,
                "hold_fire": hold_fire,
                "sprint_drift": sprint_drift,
                "avoid_cavitation": avoid_cavitation,
                "obey_emcon": obey_emcon,
                "course": (
                    [waypoint.model_dump(mode="json") for waypoint in course]
                    if course is not None
                    else None
                ),
            },
            UnitSetResult,
        )

    server.add_tool(
        unit_set,
        name="cmo_unit_set",
        title="Update CMO unit",
        description=(
            "Update a unit's name, movement or depth state, hold behavior, EMCON obedience, "
            "cavitation behavior, sprint-and-drift behavior, or plotted course by GUID."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def unit_assign_mission(
        unit_guid: Annotated[str, Field(min_length=1)],
        mission_guid: Annotated[str, Field(min_length=1)],
        escort: bool = False,
    ) -> UnitAssignMissionResult:
        return await _invoke(
            application,
            "unit.assign_mission",
            {
                "unit_guid": unit_guid,
                "mission_guid": mission_guid,
                "escort": escort,
            },
            UnitAssignMissionResult,
        )

    server.add_tool(
        unit_assign_mission,
        name="cmo_unit_assign_mission",
        title="Assign CMO unit to mission",
        description="Assign one unit to one mission by GUID, optionally as an escort.",
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def unit_unassign_mission(
        unit_guid: Annotated[str, Field(min_length=1)],
    ) -> UnitUnassignMissionResult:
        return await _invoke(
            application,
            "unit.unassign_mission",
            {"unit_guid": unit_guid},
            UnitUnassignMissionResult,
        )

    server.add_tool(
        unit_unassign_mission,
        name="cmo_unit_unassign_mission",
        title="Unassign CMO unit from mission",
        description="Remove one unit's current mission assignment by GUID.",
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def unit_loadout_set(
        unit_guid: Annotated[str, Field(min_length=1)],
        loadout_dbid: Annotated[int, Field(ge=1)],
        time_to_ready_minutes: Annotated[float | None, Field(ge=0)] = None,
        ignore_magazines: bool = False,
        exclude_optional_weapons: bool = False,
    ) -> UnitLoadoutResult:
        return await _invoke(
            application,
            "unit.loadout.set",
            {
                "unit_guid": unit_guid,
                "loadout_dbid": loadout_dbid,
                "time_to_ready_minutes": time_to_ready_minutes,
                "ignore_magazines": ignore_magazines,
                "exclude_optional_weapons": exclude_optional_weapons,
            },
            UnitLoadoutResult,
        )

    server.add_tool(
        unit_loadout_set,
        name="cmo_unit_loadout_set",
        title="Set CMO unit loadout",
        description=(
            "Change one unit's loadout and return the loadout state observed immediately "
            "after the update."
        ),
        annotations=_non_idempotent_mutation_annotations(),
        structured_output=True,
    )

    async def unit_launch(
        unit_guid: Annotated[str, Field(min_length=1)],
    ) -> UnitCommandResult:
        return await _invoke(
            application,
            "unit.launch",
            {"unit_guid": unit_guid},
            UnitCommandResult,
        )

    server.add_tool(
        unit_launch,
        name="cmo_unit_launch",
        title="Launch CMO unit",
        description=(
            "Submit a launch command for one unit. Success means the command was accepted, "
            "not completed; poll cmo_unit_combat_status_get for progress."
        ),
        annotations=_non_idempotent_mutation_annotations(),
        structured_output=True,
    )

    async def unit_rtb(
        unit_guid: Annotated[str, Field(min_length=1)],
    ) -> UnitCommandResult:
        return await _invoke(
            application,
            "unit.rtb",
            {"unit_guid": unit_guid},
            UnitCommandResult,
        )

    server.add_tool(
        unit_rtb,
        name="cmo_unit_rtb",
        title="Order CMO unit to return to base",
        description=(
            "Submit a return-to-base command for one unit. Success means the command was "
            "accepted, not completed; poll cmo_unit_combat_status_get for progress."
        ),
        annotations=_non_idempotent_mutation_annotations(),
        structured_output=True,
    )

    async def unit_refuel(
        unit_guid: Annotated[str, Field(min_length=1)],
        tanker_guid: Annotated[str | None, Field(min_length=1)] = None,
        tanker_mission_guids: Annotated[
            list[Annotated[str, Field(min_length=1)]] | None,
            Field(min_length=1),
        ] = None,
    ) -> UnitCommandResult:
        return await _invoke(
            application,
            "unit.refuel",
            {
                "unit_guid": unit_guid,
                "tanker_guid": tanker_guid,
                "tanker_mission_guids": cast(JsonValue, tanker_mission_guids),
            },
            UnitCommandResult,
        )

    server.add_tool(
        unit_refuel,
        name="cmo_unit_refuel",
        title="Order CMO unit to refuel",
        description=(
            "Submit a refuel command, optionally selecting a tanker or tanker missions. "
            "Success means the command was accepted, not completed; poll "
            "cmo_unit_combat_status_get for progress."
        ),
        annotations=_non_idempotent_mutation_annotations(),
        structured_output=True,
    )

    async def unit_cargo_transfer(
        from_unit_guid: Annotated[str, Field(min_length=1)],
        to_unit_guid: Annotated[str, Field(min_length=1)],
        items: Annotated[list[CargoTransferItem], Field(min_length=1)],
    ) -> UnitCargoTransferResult:
        return await _invoke(
            application,
            "unit.cargo.transfer",
            {
                "from_unit_guid": from_unit_guid,
                "to_unit_guid": to_unit_guid,
                "items": [item.model_dump(mode="json") for item in items],
            },
            UnitCargoTransferResult,
        )

    server.add_tool(
        unit_cargo_transfer,
        name="cmo_unit_cargo_transfer",
        title="Transfer CMO unit cargo",
        description=(
            "Transfer selected cargo records or database quantities between two eligible units. "
            "Repeated calls may move additional cargo."
        ),
        annotations=_non_idempotent_mutation_annotations(),
        structured_output=True,
    )

    async def unit_cargo_unload(
        unit_guid: Annotated[str, Field(min_length=1)],
    ) -> UnitCargoUnloadResult:
        return await _invoke(
            application,
            "unit.cargo.unload",
            {"unit_guid": unit_guid},
            UnitCargoUnloadResult,
        )

    server.add_tool(
        unit_cargo_unload,
        name="cmo_unit_cargo_unload",
        title="Unload CMO unit cargo",
        description=(
            "Unload all eligible cargo from one unit at its current location. Repeated calls are "
            "not safe after an uncertain result."
        ),
        annotations=_non_idempotent_mutation_annotations(),
        structured_output=True,
    )

    async def unit_attack_contact(
        side_guid: Annotated[str, Field(min_length=1)],
        attacker_unit_guid: Annotated[str, Field(min_length=1)],
        contact_guid: Annotated[str, Field(min_length=1)],
        mode: Literal["auto", "manual_weapon", "manual_target"],
        mount_dbid: Annotated[int | None, Field(ge=1)] = None,
        weapon_dbid: Annotated[int | None, Field(ge=1)] = None,
        quantity: Annotated[int | None, Field(ge=1)] = None,
    ) -> UnitAttackContactResult:
        return await _invoke(
            application,
            "unit.attack_contact",
            {
                "side_guid": side_guid,
                "attacker_unit_guid": attacker_unit_guid,
                "contact_guid": contact_guid,
                "mode": mode,
                "mount_dbid": mount_dbid,
                "weapon_dbid": weapon_dbid,
                "quantity": quantity,
            },
            UnitAttackContactResult,
        )

    server.add_tool(
        unit_attack_contact,
        name="cmo_unit_attack_contact",
        title="Order CMO unit to attack contact",
        description=(
            "Submit an auto, manual-target, or manual-weapon attack command. Success means the "
            "command was accepted, not completed; poll cmo_unit_combat_status_get for progress. "
            "Manual weapon allocation is non-idempotent and can allocate additional weapons if "
            "repeated."
        ),
        annotations=_destructive_mutation_annotations(),
        structured_output=True,
    )

    async def contact_list(
        side_guid: str | None = None,
        side_name: str | None = None,
        page_size: Annotated[int, Field(ge=1, le=500)] = 100,
        cursor: str | None = None,
        contact_type: str | None = None,
    ) -> PagedResult[ContactResult]:
        return await _invoke(
            application,
            "contact.list",
            {
                "side_guid": side_guid,
                "side_name": side_name,
                "page_size": page_size,
                "cursor": cursor,
                "contact_type": contact_type,
            },
            PagedResult[ContactResult],
        )

    server.add_tool(
        contact_list,
        name="cmo_contact_list",
        title="List CMO contacts",
        description=(
            "List the contacts visible to exactly one side, optionally filtering by contact type."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def contact_get(
        contact_guid: Annotated[str, Field(min_length=1)],
        side_guid: str | None = None,
        side_name: str | None = None,
    ) -> ContactDetailResult:
        return await _invoke(
            application,
            "contact.get",
            {
                "side_guid": side_guid,
                "side_name": side_name,
                "contact_guid": contact_guid,
            },
            ContactDetailResult,
        )

    server.add_tool(
        contact_get,
        name="cmo_contact_get",
        title="Get detailed CMO contact",
        description=(
            "Return bounded intelligence detail for one contact as observed by exactly one side, "
            "including uncertainty, detections, emissions, BDA, and combat relationships."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def contact_posture_set(
        side_guid: Annotated[str, Field(min_length=1)],
        contact_guid: Annotated[str, Field(min_length=1)],
        posture: Literal["F", "N", "U", "H"],
    ) -> ContactDetailResult:
        return await _invoke(
            application,
            "contact.posture.set",
            {
                "side_guid": side_guid,
                "contact_guid": contact_guid,
                "posture": posture,
            },
            ContactDetailResult,
        )

    server.add_tool(
        contact_posture_set,
        name="cmo_contact_posture_set",
        title="Set CMO contact posture",
        description=(
            "Set one observing side's posture toward one contact and return the observed contact "
            "state."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def contact_weapon_allocations_get(
        side_guid: Annotated[str, Field(min_length=1)],
        contact_guid: Annotated[str, Field(min_length=1)],
    ) -> ContactWeaponAllocationsResult:
        return await _invoke(
            application,
            "contact.weapon_allocations.get",
            {
                "side_guid": side_guid,
                "contact_guid": contact_guid,
            },
            ContactWeaponAllocationsResult,
        )

    server.add_tool(
        contact_weapon_allocations_get,
        name="cmo_contact_weapon_allocations_get",
        title="Get CMO contact weapon allocations",
        description="Return weapons currently allocated against one contact.",
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def mission_list(
        side_guid: str | None = None,
        side_name: str | None = None,
        page_size: Annotated[int, Field(ge=1, le=500)] = 100,
        cursor: str | None = None,
        mission_class: str | None = None,
        category: MissionCategory | None = None,
    ) -> PagedResult[MissionResult]:
        return await _invoke(
            application,
            "mission.list",
            {
                "side_guid": side_guid,
                "side_name": side_name,
                "page_size": page_size,
                "cursor": cursor,
                "mission_class": mission_class,
                "category": category,
            },
            PagedResult[MissionResult],
        )

    server.add_tool(
        mission_list,
        name="cmo_mission_list",
        title="List CMO missions",
        description=(
            "List missions for exactly one side, optionally filtering by mission class or "
            "ordinary mission, package, or task-pool category."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def mission_get(
        side_guid: str | None = None,
        side_name: str | None = None,
        mission_guid: str | None = None,
        mission_name: str | None = None,
    ) -> MissionResult:
        return await _invoke(
            application,
            "mission.get",
            {
                "side_guid": side_guid,
                "side_name": side_name,
                "mission_guid": mission_guid,
                "mission_name": mission_name,
            },
            MissionResult,
        )

    server.add_tool(
        mission_get,
        name="cmo_mission_get",
        title="Get CMO mission",
        description="Get one mission by GUID or exact name within exactly one side.",
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def reference_point_list(
        side_guid: str | None = None,
        side_name: str | None = None,
        page_size: Annotated[int, Field(ge=1, le=500)] = 100,
        cursor: str | None = None,
    ) -> PagedResult[ReferencePointResult]:
        return await _invoke(
            application,
            "reference_point.list",
            {
                "side_guid": side_guid,
                "side_name": side_name,
                "page_size": page_size,
                "cursor": cursor,
            },
            PagedResult[ReferencePointResult],
        )

    server.add_tool(
        reference_point_list,
        name="cmo_reference_point_list",
        title="List CMO reference points",
        description="List the reference points belonging to exactly one side.",
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def reference_point_add(
        side_guid: Annotated[str, Field(min_length=1)],
        name: Annotated[str, Field(min_length=1)],
        latitude: Annotated[float | None, Field(ge=-90, le=90)] = None,
        longitude: Annotated[float | None, Field(ge=-180, le=180)] = None,
        relative_to_type: ReferencePointAnchorType | None = None,
        relative_to_guid: Annotated[str | None, Field(min_length=1)] = None,
        relative_bearing_deg: Annotated[float | None, Field(ge=0, le=360)] = None,
        relative_distance_nm: Annotated[float | None, Field(ge=0)] = None,
        bearing_type: ReferencePointBearingType | None = None,
    ) -> ReferencePointResult:
        return await _invoke(
            application,
            "reference_point.add",
            {
                "side_guid": side_guid,
                "name": name,
                "latitude": latitude,
                "longitude": longitude,
                "relative_to_type": relative_to_type,
                "relative_to_guid": relative_to_guid,
                "relative_bearing_deg": relative_bearing_deg,
                "relative_distance_nm": relative_distance_nm,
                "bearing_type": bearing_type,
            },
            ReferencePointResult,
        )

    server.add_tool(
        reference_point_add,
        name="cmo_reference_point_add",
        title="Add CMO reference point",
        description=(
            "Add one absolute reference point, or a relative point anchored to a unit, contact, "
            "or another reference point. Relative field aliases can vary by CMO build, so verify "
            "the returned anchor. Repeated calls may create duplicates."
        ),
        annotations=_create_annotations(),
        structured_output=True,
    )

    async def reference_point_update(
        side_guid: Annotated[str, Field(min_length=1)],
        reference_point_guid: Annotated[str, Field(min_length=1)],
        name: Annotated[str | None, Field(min_length=1)] = None,
        latitude: Annotated[float | None, Field(ge=-90, le=90)] = None,
        longitude: Annotated[float | None, Field(ge=-180, le=180)] = None,
        relative_to_type: ReferencePointAnchorType | None = None,
        relative_to_guid: Annotated[str | None, Field(min_length=1)] = None,
        relative_bearing_deg: Annotated[float | None, Field(ge=0, le=360)] = None,
        relative_distance_nm: Annotated[float | None, Field(ge=0)] = None,
        bearing_type: ReferencePointBearingType | None = None,
        clear_relative: bool = False,
    ) -> ReferencePointResult:
        return await _invoke(
            application,
            "reference_point.update",
            {
                "side_guid": side_guid,
                "reference_point_guid": reference_point_guid,
                "name": name,
                "latitude": latitude,
                "longitude": longitude,
                "relative_to_type": relative_to_type,
                "relative_to_guid": relative_to_guid,
                "relative_bearing_deg": relative_bearing_deg,
                "relative_distance_nm": relative_distance_nm,
                "bearing_type": bearing_type,
                "clear_relative": clear_relative,
            },
            ReferencePointResult,
        )

    server.add_tool(
        reference_point_update,
        name="cmo_reference_point_update",
        title="Update CMO reference point",
        description=(
            "Update a reference point's name or absolute position, or set, adjust, or clear its "
            "relative anchor. Verify the returned anchor on the installed CMO build."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def doctrine_get(
        scope: Literal["side", "unit", "mission"],
        side_guid: str | None = None,
        unit_guid: str | None = None,
        mission_guid: str | None = None,
        actual: bool = True,
    ) -> DoctrineResult:
        return await _invoke(
            application,
            "doctrine.get",
            {
                "scope": scope,
                "side_guid": side_guid,
                "unit_guid": unit_guid,
                "mission_guid": mission_guid,
                "actual": actual,
            },
            DoctrineResult,
        )

    server.add_tool(
        doctrine_get,
        name="cmo_doctrine_get",
        title="Get CMO doctrine",
        description="Get the projected doctrine and EMCON state for one side, unit, or mission.",
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def doctrine_wra_get(
        scope: Literal["side", "unit", "mission"],
        side_guid: Annotated[str, Field(min_length=1)],
        unit_guid: Annotated[str | None, Field(min_length=1)] = None,
        mission_guid: Annotated[str | None, Field(min_length=1)] = None,
        weapon_dbid: Annotated[int | None, Field(ge=1)] = None,
        contact_guid: Annotated[str | None, Field(min_length=1)] = None,
        target_type: Annotated[str, Field(min_length=1)] | int | None = None,
        full_wra: bool = False,
    ) -> DoctrineWraResult:
        return await _invoke(
            application,
            "doctrine.wra.get",
            {
                "scope": scope,
                "side_guid": side_guid,
                "unit_guid": unit_guid,
                "mission_guid": mission_guid,
                "weapon_dbid": weapon_dbid,
                "contact_guid": contact_guid,
                "target_type": target_type,
                "full_wra": full_wra,
            },
            DoctrineWraResult,
        )

    server.add_tool(
        doctrine_wra_get,
        name="cmo_doctrine_wra_get",
        title="Get CMO weapon release authority",
        description=(
            "Return weapon release authority for one side, mission, or unit and one contact or "
            "target type, optionally filtering by weapon database ID."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def unit_add(
        side_guid: Annotated[str, Field(min_length=1)],
        unit_type: Annotated[str, Field(min_length=1)],
        dbid: Annotated[int, Field(ge=1)],
        name: Annotated[str, Field(min_length=1)],
        base_guid: Annotated[str | None, Field(min_length=1)] = None,
        latitude: Annotated[float | None, Field(ge=-90, le=90)] = None,
        longitude: Annotated[float | None, Field(ge=-180, le=180)] = None,
        altitude: float | None = None,
        loadout_dbid: Annotated[int | None, Field(ge=1)] = None,
    ) -> UnitAddResult:
        return await _invoke(
            application,
            "unit.add",
            {
                "side_guid": side_guid,
                "unit_type": unit_type,
                "dbid": dbid,
                "name": name,
                "base_guid": base_guid,
                "latitude": latitude,
                "longitude": longitude,
                "altitude": altitude,
                "loadout_dbid": loadout_dbid,
            },
            UnitAddResult,
        )

    server.add_tool(
        unit_add,
        name="cmo_unit_add",
        title="Add CMO unit",
        description=(
            "Add one unit from a database ID at coordinates or at a base. Repeated calls may "
            "create duplicates."
        ),
        annotations=_create_annotations(),
        structured_output=True,
    )

    async def mission_create(
        side_guid: Annotated[str, Field(min_length=1)],
        name: Annotated[str, Field(min_length=1)],
        details: MissionDetails,
        category: MissionCategory = "mission",
        parent_task_pool_guid: Annotated[str | None, Field(min_length=1)] = None,
    ) -> MissionCreateResult:
        return await _invoke(
            application,
            "mission.create",
            {
                "side_guid": side_guid,
                "name": name,
                "category": category,
                "parent_task_pool_guid": parent_task_pool_guid,
                "details": details.model_dump(mode="json"),
            },
            MissionCreateResult,
        )

    server.add_tool(
        mission_create,
        name="cmo_mission_create",
        title="Create CMO mission",
        description=(
            "Create an ordinary mission, task pool, or package for patrol, support, strike, "
            "ferry, mining, mine-clearing, or cargo work. Packages require a parent task-pool "
            "GUID. Repeated calls may create duplicates."
        ),
        annotations=_create_annotations(),
        structured_output=True,
    )

    async def mission_update(
        side_guid: Annotated[str, Field(min_length=1)],
        mission_guid: Annotated[str, Field(min_length=1)],
        active: bool | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        flight_size: FlightSize | None = None,
        use_flight_size: bool | None = None,
        minimum_aircraft_required: MinimumAircraftRequired | None = None,
        on_station: Annotated[int | None, Field(ge=0)] = None,
        one_time_only: bool | None = None,
        preplanned_only: bool | None = None,
        one_third_rule: bool | None = None,
        reference_point_guids: OrderedReferencePointGuidList | None = None,
        prosecution_zone_reference_point_guids: OrderedReferencePointGuidList | None = None,
        destination_guid: Annotated[str | None, Field(min_length=1)] = None,
        loop_type: Annotated[int | None, Field(ge=0, le=2)] = None,
        active_emcon: bool | None = None,
        check_opa: bool | None = None,
        check_wwr: bool | None = None,
        group_size: Annotated[int | None, Field(ge=1)] = None,
        use_group_size: bool | None = None,
        transit_throttle_aircraft: MissionStageValue | None = None,
        transit_throttle_ship: MissionStageValue | None = None,
        transit_throttle_submarine: MissionStageValue | None = None,
        station_throttle_aircraft: MissionStageValue | None = None,
        station_throttle_ship: MissionStageValue | None = None,
        station_throttle_submarine: MissionStageValue | None = None,
        attack_throttle_aircraft: MissionStageValue | None = None,
        attack_throttle_ship: MissionStageValue | None = None,
        attack_throttle_submarine: MissionStageValue | None = None,
        transit_altitude_aircraft: MissionStageValue | None = None,
        station_altitude_aircraft: MissionStageValue | None = None,
        attack_altitude_aircraft: MissionStageValue | None = None,
        transit_depth_submarine: MissionStageValue | None = None,
        station_depth_submarine: MissionStageValue | None = None,
        attack_depth_submarine: MissionStageValue | None = None,
        strike_minimum_trigger: Annotated[str, Field(min_length=1)] | None = None,
        strike_max_flights: Annotated[int | None, Field(ge=0)] = None,
        strike_auto_planner: bool | None = None,
        strike_min_distance_aircraft: MissionStageValue | None = None,
        strike_max_distance_aircraft: MissionStageValue | None = None,
        strike_min_distance_ship: MissionStageValue | None = None,
        strike_max_distance_ship: MissionStageValue | None = None,
        focus_on_strike: bool | None = None,
        arming_delay: str | None = None,
        mines_per_set: Annotated[int | None, Field(ge=1)] = None,
        mine_spacing_m: Annotated[float | None, Field(ge=0)] = None,
        set_spacing_m: Annotated[float | None, Field(ge=0)] = None,
        laying_method: Literal[0, 1] | None = None,
        cargo_subtype: Literal["transfer", "delivery"] | None = None,
        move_all_cargo: bool | None = None,
        allow_ground_self_delivery: bool | None = None,
    ) -> MissionUpdateResult:
        updates: dict[str, JsonValue] = {}
        for field_name, value in (
            ("active", active),
            ("start_time", start_time),
            ("end_time", end_time),
            ("flight_size", flight_size),
            ("use_flight_size", use_flight_size),
            ("minimum_aircraft_required", minimum_aircraft_required),
            ("on_station", on_station),
            ("one_time_only", one_time_only),
            ("preplanned_only", preplanned_only),
            ("one_third_rule", one_third_rule),
            ("reference_point_guids", reference_point_guids),
            (
                "prosecution_zone_reference_point_guids",
                prosecution_zone_reference_point_guids,
            ),
            ("destination_guid", destination_guid),
            ("loop_type", loop_type),
            ("active_emcon", active_emcon),
            ("check_opa", check_opa),
            ("check_wwr", check_wwr),
            ("group_size", group_size),
            ("use_group_size", use_group_size),
            ("transit_throttle_aircraft", transit_throttle_aircraft),
            ("transit_throttle_ship", transit_throttle_ship),
            ("transit_throttle_submarine", transit_throttle_submarine),
            ("station_throttle_aircraft", station_throttle_aircraft),
            ("station_throttle_ship", station_throttle_ship),
            ("station_throttle_submarine", station_throttle_submarine),
            ("attack_throttle_aircraft", attack_throttle_aircraft),
            ("attack_throttle_ship", attack_throttle_ship),
            ("attack_throttle_submarine", attack_throttle_submarine),
            ("transit_altitude_aircraft", transit_altitude_aircraft),
            ("station_altitude_aircraft", station_altitude_aircraft),
            ("attack_altitude_aircraft", attack_altitude_aircraft),
            ("transit_depth_submarine", transit_depth_submarine),
            ("station_depth_submarine", station_depth_submarine),
            ("attack_depth_submarine", attack_depth_submarine),
            ("strike_minimum_trigger", strike_minimum_trigger),
            ("strike_max_flights", strike_max_flights),
            ("strike_auto_planner", strike_auto_planner),
            ("strike_min_distance_aircraft", strike_min_distance_aircraft),
            ("strike_max_distance_aircraft", strike_max_distance_aircraft),
            ("strike_min_distance_ship", strike_min_distance_ship),
            ("strike_max_distance_ship", strike_max_distance_ship),
            ("focus_on_strike", focus_on_strike),
            ("arming_delay", arming_delay),
            ("mines_per_set", mines_per_set),
            ("mine_spacing_m", mine_spacing_m),
            ("set_spacing_m", set_spacing_m),
            ("laying_method", laying_method),
            ("cargo_subtype", cargo_subtype),
            ("move_all_cargo", move_all_cargo),
            ("allow_ground_self_delivery", allow_ground_self_delivery),
        ):
            if value is not None:
                updates[field_name] = cast(JsonValue, value)
        return await _invoke(
            application,
            "mission.update",
            {"side_guid": side_guid, "mission_guid": mission_guid, **updates},
            MissionUpdateResult,
        )

    server.add_tool(
        mission_update,
        name="cmo_mission_update",
        title="Update CMO mission",
        description=(
            "Update activation, schedule, force grouping, movement profiles, patrol behavior, "
            "strike limits, mining options, cargo options, or ordered mission zones."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def mission_air_refueling_update(
        side_guid: Annotated[str, Field(min_length=1)],
        mission_guid: Annotated[str, Field(min_length=1)],
        use_refuel_unrep: RefuelUnrepValue | None = None,
        tanker_usage: TankerUsage | None = None,
        launch_without_tankers_in_place: bool | None = None,
        tanker_follows_receivers: bool | None = None,
        keep_on_mission_without_tankers_in_place: bool | None = None,
        tanker_mission_guids: list[Annotated[str, Field(min_length=1)]] | None = None,
        minimum_tankers_total: Annotated[int | None, Field(ge=0)] = None,
        minimum_tankers_airborne: Annotated[int | None, Field(ge=0)] = None,
        minimum_tankers_on_station: Annotated[int | None, Field(ge=0)] = None,
        max_receivers_in_queue_per_tanker: Annotated[int | None, Field(ge=0)] = None,
        fuel_percent_to_start_looking: Annotated[
            float | None, Field(ge=0, le=100)
        ] = None,
        tanker_max_distance_nm: (
            Annotated[float, Field(ge=0)] | Literal["internal"] | None
        ) = None,
        tanker_one_time: bool | None = None,
        tanker_max_receivers: (
            Annotated[int, Field(ge=0)]
            | Annotated[str, Field(min_length=1)]
            | None
        ) = None,
    ) -> MissionAirRefuelingResult:
        updates: dict[str, JsonValue] = {}
        for field_name, value in (
            ("use_refuel_unrep", use_refuel_unrep),
            ("tanker_usage", tanker_usage),
            ("launch_without_tankers_in_place", launch_without_tankers_in_place),
            ("tanker_follows_receivers", tanker_follows_receivers),
            (
                "keep_on_mission_without_tankers_in_place",
                keep_on_mission_without_tankers_in_place,
            ),
            ("tanker_mission_guids", tanker_mission_guids),
            ("minimum_tankers_total", minimum_tankers_total),
            ("minimum_tankers_airborne", minimum_tankers_airborne),
            ("minimum_tankers_on_station", minimum_tankers_on_station),
            (
                "max_receivers_in_queue_per_tanker",
                max_receivers_in_queue_per_tanker,
            ),
            ("fuel_percent_to_start_looking", fuel_percent_to_start_looking),
            ("tanker_max_distance_nm", tanker_max_distance_nm),
            ("tanker_one_time", tanker_one_time),
            ("tanker_max_receivers", tanker_max_receivers),
        ):
            if value is not None:
                updates[field_name] = cast(JsonValue, value)
        return await _invoke(
            application,
            "mission.air_refueling.update",
            {"side_guid": side_guid, "mission_guid": mission_guid, **updates},
            MissionAirRefuelingResult,
        )

    server.add_tool(
        mission_air_refueling_update,
        name="cmo_mission_air_refueling_update",
        title="Update CMO mission air-refueling plan",
        description=(
            "Configure receiver refueling policy, assigned tanker missions, tanker readiness "
            "minimums, queue limits, search threshold, and support-mission tanker limits."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def mission_flight_plan_list(
        side_guid: Annotated[str, Field(min_length=1)],
        mission_guid: Annotated[str, Field(min_length=1)],
    ) -> MissionFlightPlanListResult:
        return await _invoke(
            application,
            "mission.flight_plan.list",
            {"side_guid": side_guid, "mission_guid": mission_guid},
            MissionFlightPlanListResult,
        )

    server.add_tool(
        mission_flight_plan_list,
        name="cmo_mission_flight_plan_list",
        title="List CMO mission flight plans",
        description=(
            "Read the mission-level takeoff/target timing, generated flights, and their "
            "waypoint courses."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def mission_flight_plan_create(
        side_guid: Annotated[str, Field(min_length=1)],
        mission_guid: Annotated[str, Field(min_length=1)],
        date_on_target: str | None = None,
        time_on_target: str | None = None,
        takeoff_date: str | None = None,
        takeoff_time: str | None = None,
    ) -> MissionFlightPlanCreateResult:
        return await _invoke(
            application,
            "mission.flight_plan.create",
            {
                "side_guid": side_guid,
                "mission_guid": mission_guid,
                "date_on_target": date_on_target,
                "time_on_target": time_on_target,
                "takeoff_date": takeoff_date,
                "takeoff_time": takeoff_time,
            },
            MissionFlightPlanCreateResult,
        )

    server.add_tool(
        mission_flight_plan_create,
        name="cmo_mission_flight_plan_create",
        title="Create CMO mission flight plan",
        description=(
            "Generate mission flights from exactly one schedule: YYYY/MM/DD plus HH:MM:SS "
            "for either time-on-target or takeoff. Waypoint mutation is intentionally absent."
        ),
        annotations=_create_annotations(),
        structured_output=True,
    )

    async def mission_target_add(
        side_guid: Annotated[str, Field(min_length=1)],
        mission_guid: Annotated[str, Field(min_length=1)],
        target_guid: Annotated[str, Field(min_length=1)],
    ) -> MissionTargetAddResult:
        return await _invoke(
            application,
            "mission.target.add",
            {
                "side_guid": side_guid,
                "mission_guid": mission_guid,
                "target_guid": target_guid,
            },
            MissionTargetAddResult,
        )

    server.add_tool(
        mission_target_add,
        name="cmo_mission_target_add",
        title="Add CMO mission target",
        description="Add one target GUID to a strike mission.",
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def mission_target_remove(
        side_guid: Annotated[str, Field(min_length=1)],
        mission_guid: Annotated[str, Field(min_length=1)],
        target_guid: Annotated[str, Field(min_length=1)],
    ) -> MissionTargetRemoveResult:
        return await _invoke(
            application,
            "mission.target.remove",
            {
                "side_guid": side_guid,
                "mission_guid": mission_guid,
                "target_guid": target_guid,
            },
            MissionTargetRemoveResult,
        )

    server.add_tool(
        mission_target_remove,
        name="cmo_mission_target_remove",
        title="Remove CMO mission target",
        description="Remove one target GUID from a strike mission.",
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def mission_cargo_update(
        side_guid: Annotated[str, Field(min_length=1)],
        mission_guid: Annotated[str, Field(min_length=1)],
        action: Literal["assign", "unassign"],
        cargo_kind: Literal["mount", "object"],
        dbid: Annotated[int, Field(ge=1)],
        object_type: Annotated[int | None, Field(ge=1, le=5)] = None,
        cargo_guid: Annotated[str | None, Field(min_length=1)] = None,
        quantity: Annotated[int | None, Field(ge=1)] = None,
    ) -> MissionCargoUpdateResult:
        return await _invoke(
            application,
            "mission.cargo.update",
            {
                "side_guid": side_guid,
                "mission_guid": mission_guid,
                "action": action,
                "cargo_kind": cargo_kind,
                "dbid": dbid,
                "object_type": object_type,
                "cargo_guid": cargo_guid,
                "quantity": quantity,
            },
            MissionCargoUpdateResult,
        )

    server.add_tool(
        mission_cargo_update,
        name="cmo_mission_cargo_update",
        title="Update CMO mission cargo",
        description=(
            "Assign or unassign one cargo object or mount quantity on a cargo mission. Repeated "
            "calls are not safe after an uncertain result."
        ),
        annotations=_non_idempotent_mutation_annotations(),
        structured_output=True,
    )

    async def doctrine_set(
        scope: Literal["side", "unit", "mission"],
        side_guid: str | None = None,
        unit_guid: str | None = None,
        mission_guid: str | None = None,
        weapon_control_air: WeaponControlUpdateValue | None = None,
        weapon_control_surface: WeaponControlUpdateValue | None = None,
        weapon_control_subsurface: WeaponControlUpdateValue | None = None,
        weapon_control_land: WeaponControlUpdateValue | None = None,
        nuclear_use: NuclearUseValue | None = None,
        refuel_unrep: RefuelUnrepValue | None = None,
        engage_opportunity_targets: DoctrineBooleanUpdateValue | None = None,
        automatic_evasion: DoctrineBooleanUpdateValue | None = None,
        ignore_plotted_course: DoctrineBooleanUpdateValue | None = None,
        ignore_emcon_while_under_attack: DoctrineBooleanUpdateValue | None = None,
        maintain_standoff: DoctrineBooleanUpdateValue | None = None,
        use_sams_in_anti_surface_mode: DoctrineBooleanUpdateValue | None = None,
        engaging_ambiguous_targets: DoctrineSettingValue | None = None,
        fuel_state_planned: DoctrineSettingValue | None = None,
        fuel_state_rtb: DoctrineSettingValue | None = None,
        weapon_state_planned: DoctrineSettingValue | None = None,
        weapon_state_rtb: DoctrineSettingValue | None = None,
        withdraw_on_attack: DoctrineSettingValue | None = None,
        withdraw_on_damage: DoctrineSettingValue | None = None,
        withdraw_on_defence: DoctrineSettingValue | None = None,
        withdraw_on_fuel: DoctrineSettingValue | None = None,
        bvr_logic: DoctrineSettingValue | None = None,
        dipping_sonar: DoctrineSettingValue | None = None,
        use_aip: DoctrineSettingValue | None = None,
        recharge_on_attack: DoctrineSettingValue | None = None,
        recharge_on_patrol: DoctrineSettingValue | None = None,
    ) -> DoctrineSetResult:
        updates: dict[str, JsonValue] = {}
        for field_name, value in (
            ("weapon_control_air", weapon_control_air),
            ("weapon_control_surface", weapon_control_surface),
            ("weapon_control_subsurface", weapon_control_subsurface),
            ("weapon_control_land", weapon_control_land),
            ("nuclear_use", nuclear_use),
            ("refuel_unrep", refuel_unrep),
            ("engage_opportunity_targets", engage_opportunity_targets),
            ("automatic_evasion", automatic_evasion),
            ("ignore_plotted_course", ignore_plotted_course),
            ("ignore_emcon_while_under_attack", ignore_emcon_while_under_attack),
            ("maintain_standoff", maintain_standoff),
            ("use_sams_in_anti_surface_mode", use_sams_in_anti_surface_mode),
            ("engaging_ambiguous_targets", engaging_ambiguous_targets),
            ("fuel_state_planned", fuel_state_planned),
            ("fuel_state_rtb", fuel_state_rtb),
            ("weapon_state_planned", weapon_state_planned),
            ("weapon_state_rtb", weapon_state_rtb),
            ("withdraw_on_attack", withdraw_on_attack),
            ("withdraw_on_damage", withdraw_on_damage),
            ("withdraw_on_defence", withdraw_on_defence),
            ("withdraw_on_fuel", withdraw_on_fuel),
            ("bvr_logic", bvr_logic),
            ("dipping_sonar", dipping_sonar),
            ("use_aip", use_aip),
            ("recharge_on_attack", recharge_on_attack),
            ("recharge_on_patrol", recharge_on_patrol),
        ):
            if value is not None:
                updates[field_name] = value
        return await _invoke(
            application,
            "doctrine.set",
            {
                "scope": scope,
                "side_guid": side_guid,
                "unit_guid": unit_guid,
                "mission_guid": mission_guid,
                **updates,
            },
            DoctrineSetResult,
        )

    server.add_tool(
        doctrine_set,
        name="cmo_doctrine_set",
        title="Update CMO doctrine",
        description=(
            "Update selected weapon-control, engagement, fuel, withdrawal, air-combat, sonar, "
            "or submarine doctrine fields for one side, unit, or mission."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def doctrine_wra_set(
        scope: Literal["side", "unit", "mission"],
        side_guid: Annotated[str, Field(min_length=1)],
        weapon_dbid: Annotated[int, Field(ge=1)],
        weapons_per_salvo: WraSettingValue,
        shooters_per_salvo: WraSettingValue,
        firing_range: WraSettingValue,
        self_defence_range: WraSettingValue,
        unit_guid: Annotated[str | None, Field(min_length=1)] = None,
        mission_guid: Annotated[str | None, Field(min_length=1)] = None,
        contact_guid: Annotated[str | None, Field(min_length=1)] = None,
        target_type: Annotated[str, Field(min_length=1)] | int | None = None,
    ) -> DoctrineWraResult:
        return await _invoke(
            application,
            "doctrine.wra.set",
            {
                "scope": scope,
                "side_guid": side_guid,
                "unit_guid": unit_guid,
                "mission_guid": mission_guid,
                "weapon_dbid": weapon_dbid,
                "contact_guid": contact_guid,
                "target_type": target_type,
                "weapons_per_salvo": weapons_per_salvo,
                "shooters_per_salvo": shooters_per_salvo,
                "firing_range": firing_range,
                "self_defence_range": self_defence_range,
            },
            DoctrineWraResult,
        )

    server.add_tool(
        doctrine_wra_set,
        name="cmo_doctrine_wra_set",
        title="Set CMO weapon release authority",
        description=(
            "Set salvo size, shooter count, firing range, and self-defence range for one weapon "
            "and one contact or target type at side, mission, or unit scope."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def emcon_set(
        scope: Literal["side", "mission", "group", "unit"],
        target_guid: Annotated[str, Field(min_length=1)],
        inherit: bool = False,
        radar: EmconValue | None = None,
        sonar: EmconValue | None = None,
        oecm: EmconValue | None = None,
    ) -> EmconSetResult:
        return await _invoke(
            application,
            "emcon.set",
            {
                "scope": scope,
                "target_guid": target_guid,
                "inherit": inherit,
                "radar": radar,
                "sonar": sonar,
                "oecm": oecm,
            },
            EmconSetResult,
        )

    server.add_tool(
        emcon_set,
        name="cmo_emcon_set",
        title="Update CMO EMCON",
        description="Set radar, sonar, or OECM emission control for a side, mission, group, or unit.",
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def special_action_list(
        side_guid: str | None = None,
        side_name: str | None = None,
    ) -> SpecialActionListResult:
        return await _invoke(
            application,
            "special_action.list",
            {"side_guid": side_guid, "side_name": side_name},
            SpecialActionListResult,
        )

    server.add_tool(
        special_action_list,
        name="cmo_special_action_list",
        title="List CMO special actions",
        description=(
            "List scenario-authored player special actions, optionally restricted to one side, "
            "without exposing their Lua source."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def special_action_execute(
        side_guid: Annotated[str, Field(min_length=1)],
        action_guid: Annotated[str, Field(min_length=1)],
    ) -> SpecialActionExecuteResult:
        return await _invoke(
            application,
            "special_action.execute",
            {"side_guid": side_guid, "action_guid": action_guid},
            SpecialActionExecuteResult,
        )

    server.add_tool(
        special_action_execute,
        name="cmo_special_action_execute",
        title="Execute CMO special action",
        description=(
            "Execute one existing active scenario-authored special action by GUID. Repeated calls "
            "may repeat scenario effects."
        ),
        annotations=_destructive_mutation_annotations(),
        structured_output=True,
    )

    async def score_get(side: Annotated[str, Field(min_length=1)]) -> ScoreResult:
        return await _invoke(
            application,
            "lua.call",
            {"function": "ScenEdit_GetScore", "arguments": {"side": side}},
            ScoreResult,
        )

    server.add_tool(
        score_get,
        name="cmo_score_get",
        title="Get CMO side score",
        description="Return the current victory-point score for one side name or GUID.",
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    register_scenario_authoring_tools(server, application)
    return server


def run_stdio(application: McpApplicationPort) -> None:
    create_mcp_server(application).run(transport="stdio")
