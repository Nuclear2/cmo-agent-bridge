from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import ConfigDict, JsonValue, TypeAdapter

from cmo_agent_bridge.application.models import InvocationOutcome
from cmo_agent_bridge.mcp_runtime import McpBridgeDiagnostic, McpBridgePrepareResult
from cmo_agent_bridge.mcp_server import create_mcp_server


_ERROR_ADAPTER: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(
    dict[str, JsonValue],
    config=ConfigDict(strict=True),
)


@dataclass(frozen=True, slots=True)
class _Call:
    operation: str
    arguments: dict[str, JsonValue]
    confirmation_token: str | None


class _FakeApplication:
    def __init__(self, outcomes: Mapping[str, InvocationOutcome]) -> None:
        self._outcomes = dict(outcomes)
        self.calls: list[_Call] = []

    async def execute(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
        *,
        confirmation_token: str | None = None,
    ) -> InvocationOutcome:
        self.calls.append(_Call(operation, dict(arguments), confirmation_token))
        return self._outcomes[operation]

    async def diagnose(self) -> McpBridgeDiagnostic:
        return McpBridgeDiagnostic(
            runtime_state="ready",
            ready=True,
            bridge_version="0.1.2",
            runtime_tag="0_1_2-" + "a" * 64,
            config_path="config.toml",
            game_root="CMO",
            dispatcher_path="dispatcher.lua",
            inbox_path="request.lua",
            poll_path="poll.lua",
            lua_action="return true",
            error_code=None,
            error_message=None,
            required_next_action="Call cmo_bridge_status.",
        )

    async def prepare(
        self,
        *,
        game_root: str | None = None,
        replace_saved_game_root: bool = False,
    ) -> McpBridgePrepareResult:
        del game_root, replace_saved_game_root
        return McpBridgePrepareResult(
            ready=True,
            bridge_version="0.1.2",
            runtime_tag="0_1_2-" + "a" * 64,
            game_root="CMO",
            dispatcher_path="dispatcher.lua",
            inbox_path="request.lua",
            poll_path="poll.lua",
            lua_action="return true",
            next_step="Call cmo_bridge_status.",
        )


def _success(result: JsonValue) -> InvocationOutcome:
    return InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=None,
        ok=True,
        result=result,
        error=None,
    )


def _failure(error: dict[str, JsonValue]) -> InvocationOutcome:
    return InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=None,
        ok=False,
        result=None,
        error=error,
    )


def _status_result() -> dict[str, JsonValue]:
    asset_sha256 = "b" * 64
    return {
        "protocol": "cmo-agent-bridge/1",
        "runtime_version": "0.1.0",
        "runtime_tag": f"0_1_0-{asset_sha256}",
        "runtime_asset_sha256": asset_sha256,
        "release_id": "c" * 64,
        "build": 1868,
        "manifest_sha256": "a" * 64,
        "lineage_id": "11111111-1111-4111-8111-111111111111",
        "activation_id": "22222222-2222-4222-8222-222222222222",
        "installed_event_names": ["CMOAgentBridge: Poll"],
        "installed_action_names": ["CMOAgentBridge: Poll"],
        "installed_trigger_names": ["CMOAgentBridge: Timer"],
        "pending_request_id": None,
        "quarantined": False,
        "paused_capability": True,
        "poll_interval_seconds": 5,
        "safe_payload_bytes": 65_536,
        "verified_ledger_entries": 32,
        "effective_ledger_capacity": 32,
    }


def _scenario_result() -> dict[str, JsonValue]:
    return {
        "guid": "SCENARIO-1",
        "title": "J-36 vs F-35",
        "file_name": "test.scen",
        "current_time": "2026/5/22 17:03:05",
        "current_time_seconds": 1_779_469_385.0,
        "start_time": "2026/5/22 16:58:49",
        "start_time_seconds": 1_779_469_129.0,
        "duration": "1.00:00:00",
        "duration_seconds": 86_400.0,
        "complexity": 1,
        "difficulty": 2,
        "setting": "Pacific",
        "database": "DB3K_517.db3",
        "save_version": "Command: Modern Operations Build 1868",
        "started": True,
        "player_side_guid": "SIDE-BLUE",
        "time_compression": 1.0,
        "campaign_score": 0,
    }


def _side_list_result() -> dict[str, JsonValue]:
    return {
        "items": [
            {
                "guid": "SIDE-1",
                "name": "PLAAF",
                "awareness": "AutoSideID",
                "proficiency": "Regular",
                "computer_controlled_only": False,
                "unit_count": 71,
                "contact_count": 1,
                "mission_count": 1,
            }
        ],
        "next_cursor": "1",
    }


def _unit_result() -> dict[str, JsonValue]:
    return {
        "guid": "UNIT-1",
        "dbid": 6611,
        "name": "J-36 #1",
        "side_name": "PLAAF",
        "type": "Aircraft",
        "subtype": "Fighter",
        "category": "Fighter",
        "class_name": "J-36",
        "latitude": 23.1,
        "longitude": 121.2,
        "altitude": 10_000.0,
        "speed": 480.0,
        "heading": 90.0,
        "throttle": "Cruise",
        "proficiency": "Regular",
        "fuel_state": "None",
        "weapon_state": "None",
        "unit_state": "Unassigned",
        "operating": True,
        "mission_guid": "MISSION-1",
        "mission_name": "CAP",
        "loadout_dbid": 12345,
    }


def _unit_list_result() -> dict[str, JsonValue]:
    return {"items": [_unit_result()], "next_cursor": "1"}


def _unit_set_result() -> dict[str, JsonValue]:
    return {
        "unit_guid": "UNIT-1",
        "name": "J-36 Lead",
        "speed": 500.0,
        "altitude": 11_000.0,
        "heading": 95.0,
        "course": [
            {
                "latitude": 24.0,
                "longitude": 122.0,
                "altitude": 12_000.0,
            }
        ],
    }


def _unit_assign_mission_result() -> dict[str, JsonValue]:
    return {
        "unit_guid": "UNIT-1",
        "mission_guid": "MISSION-1",
        "escort": True,
    }


def _unit_combat_status_result() -> dict[str, JsonValue]:
    return {
        "unit_guid": "UNIT-1",
        "operating": True,
        "destroyed": False,
        "sinking": False,
        "condition": "Airborne",
        "condition_code": "AIRBORNE",
        "unit_state": "On mission",
        "fuel_state": "None",
        "weapon_state": "None",
        "ready_time_seconds": 0.0,
        "airborne_time_seconds": 600.0,
        "loadout_dbid": 12345,
        "damage": {
            "dp": 100.0,
            "start_dp": 100.0,
            "dp_percent": 0.0,
            "dp_percent_now": 0.0,
            "fires": "None",
            "flood": "None",
        },
        "fuels": [
            {
                "type_code": 1001,
                "name": "JP-8",
                "current": 7_500.0,
                "max": 10_000.0,
                "percent": 75.0,
            }
        ],
        "target_contact_guid": "CONTACT-1",
        "firing_at_contact_guids": ["CONTACT-1"],
        "fired_on_by_unit_guids": ["UNIT-ENEMY-1"],
        "targeted_by_unit_guids": ["UNIT-ENEMY-1"],
    }


def _unit_loadout_result() -> dict[str, JsonValue]:
    return {
        "unit_guid": "UNIT-1",
        "loadout_dbid": 12345,
        "name": "Air Superiority",
        "ready_time_seconds": 1_800.0,
        "weapons": [
            {
                "guid": "WEAPON-RECORD-1",
                "dbid": 2001,
                "name": "AAM",
                "type_code": 200,
                "current": 4,
                "max_capacity": 6,
                "default": 6,
            }
        ],
    }


def _unit_command_result(command: str) -> dict[str, JsonValue]:
    return {
        "unit_guid": "UNIT-1",
        "command": command,
        "accepted": True,
        "operating": True,
        "condition": "Command accepted",
        "condition_code": "COMMAND_ACCEPTED",
        "unit_state": "Tasked",
    }


def _unit_attack_contact_result() -> dict[str, JsonValue]:
    return {
        "attacker_unit_guid": "UNIT-1",
        "contact_guid": "CONTACT-1",
        "mode": "manual_weapon",
        "accepted": True,
        "primary_target_contact_guid": "CONTACT-1",
        "firing_at_contact_guids": ["CONTACT-1"],
        "targeted_by_attacker": True,
    }


def _contact_list_result() -> dict[str, JsonValue]:
    return {
        "items": [
            {
                "guid": "CONTACT-1",
                "name": "Bogey #1",
                "observer_side_guid": "SIDE-1",
                "type": "Air",
                "type_description": "Aircraft",
                "classification": "KnownClass",
                "posture": "Hostile",
                "latitude": 24.1,
                "longitude": 122.2,
                "altitude": 9_000.0,
                "speed": 450.0,
                "heading": 270.0,
                "actual_unit_guid": None,
                "actual_unit_dbid": None,
            }
        ],
        "next_cursor": None,
    }


def _mission_result() -> dict[str, JsonValue]:
    return {
        "guid": "MISSION-1",
        "name": "CAP",
        "side_name": "PLAAF",
        "mission_class": "patrol",
        "mission_class_string": "Patrol",
        "category": "mission",
        "parent_task_pool_guid": None,
        "package_guids": [],
        "active": True,
        "start_time": None,
        "end_time": None,
        "assigned_unit_guids": ["UNIT-1"],
        "target_guids": [],
        "patrol_type": "aaw",
        "strike_type": None,
        "reference_point_guids": ["RP-1", "RP-2", "RP-3", "RP-4"],
        "prosecution_zone_reference_point_guids": None,
        "destination_guid": None,
        "flight_size": 2,
        "use_flight_size": True,
        "minimum_aircraft_required": 2,
        "on_station": 1,
        "one_time_only": None,
        "preplanned_only": None,
        "one_third_rule": True,
    }


def _mission_list_result() -> dict[str, JsonValue]:
    return {"items": [_mission_result()], "next_cursor": None}


def _reference_point_result() -> dict[str, JsonValue]:
    return {
        "guid": "RP-1",
        "name": "Northwest",
        "side_guid": "SIDE-1",
        "latitude": 24.0,
        "longitude": 122.0,
        "relative_to_type": None,
        "relative_to_guid": None,
        "relative_bearing_deg": None,
        "relative_distance_nm": None,
        "bearing_type": None,
    }


def _reference_point_list_result() -> dict[str, JsonValue]:
    return {"items": [_reference_point_result()], "next_cursor": None}


def _doctrine_result() -> dict[str, JsonValue]:
    return {
        "scope": "mission",
        "target_guid": "MISSION-1",
        "actual": True,
        "weapon_control_air": "Tight",
        "weapon_control_surface": "Hold",
        "weapon_control_subsurface": "Hold",
        "weapon_control_land": "Hold",
        "nuclear_use": False,
        "refuel_unrep": "Always_ExceptTankersRefuellingTankers",
        "radar": "Passive",
        "sonar": "Passive",
        "oecm": "Passive",
    }


def _unit_add_result() -> dict[str, JsonValue]:
    return {
        "unit_guid": "UNIT-2",
        "name": "J-36 #2",
        "side_guid": "SIDE-1",
        "dbid": 6611,
        "latitude": 23.2,
        "longitude": 121.3,
    }


def _mission_create_result() -> dict[str, JsonValue]:
    return {
        "mission_guid": "MISSION-2",
        "name": "North CAP",
        "side_guid": "SIDE-1",
        "mission_class": "patrol",
        "subtype": "aaw",
        "category": "mission",
        "parent_task_pool_guid": None,
        "active": False,
    }


def _mission_update_result() -> dict[str, JsonValue]:
    return {
        "mission_guid": "MISSION-2",
        "name": "North CAP",
        "active": True,
        "start_time": None,
        "end_time": None,
        "flight_size": 2,
        "use_flight_size": True,
        "minimum_aircraft_required": 2,
        "on_station": 1,
        "one_time_only": None,
        "preplanned_only": None,
        "one_third_rule": False,
        "reference_point_guids": ["RP-1", "RP-2", "RP-3", "RP-4"],
        "prosecution_zone_reference_point_guids": ["RP-5", "RP-6", "RP-7"],
    }


def _doctrine_set_result() -> dict[str, JsonValue]:
    return {
        "scope": "mission",
        "target_guid": "MISSION-2",
        "weapon_control_air": None,
        "weapon_control_surface": None,
        "weapon_control_subsurface": None,
        "weapon_control_land": None,
        "nuclear_use": None,
        "refuel_unrep": None,
    }


def _emcon_set_result() -> dict[str, JsonValue]:
    return {
        "scope": "mission",
        "target_guid": "MISSION-2",
        "inherit": True,
        "radar": None,
        "sonar": None,
        "oecm": None,
    }


def _unit_unassign_mission_result() -> dict[str, JsonValue]:
    return {
        "unit_guid": "UNIT-1",
        "mission_guid": None,
        "escort": False,
    }


def _mission_target_add_result() -> dict[str, JsonValue]:
    return {
        "mission_guid": "MISSION-2",
        "target_guid": "CONTACT-1",
        "assigned": True,
        "target_guids": ["CONTACT-1"],
    }


def _mission_target_remove_result() -> dict[str, JsonValue]:
    return {
        "mission_guid": "MISSION-2",
        "target_guid": "CONTACT-1",
        "assigned": False,
        "target_guids": [],
    }


def _score_result() -> dict[str, JsonValue]:
    return {"side": "PLAAF", "score": 100}


def _time_compression_result() -> dict[str, JsonValue]:
    return {"code": 2, "observed_time_compression": 5.0, "accepted": True}


def _side_posture_result() -> dict[str, JsonValue]:
    return {"side_a_guid": "SIDE-1", "side_b_guid": "SIDE-2", "posture": "H"}


def _contact_detail_result() -> dict[str, JsonValue]:
    items = cast(list[JsonValue], _contact_list_result()["items"])
    contact = cast(dict[str, JsonValue], items[0])
    return {
        **contact,
        "age_seconds": 45.0,
        "area_of_uncertainty": [{"latitude": 24.0, "longitude": 122.0}],
        "detection_by": {
            "radar": 5.0,
            "esm": 10.0,
        },
        "emissions": [
            {
                "name": "Fire-control radar",
                "age_seconds": 8.0,
                "solid": True,
                "sensor_dbid": 4001,
                "sensor_name": "Radar",
                "sensor_type": "Radar",
                "sensor_role": "FireControl",
                "sensor_max_range_nm": 120.0,
            }
        ],
        "last_detections": [
            {
                "detector_guid": "UNIT-1",
                "detect_sensor_guid": "SENSOR-1",
                "age_seconds": 5.0,
                "range_nm": 80.0,
            }
        ],
        "potential_matches": [
            {
                "dbid": 9001,
                "name": "Fighter",
                "category": "Aircraft",
                "type": "Fighter",
                "subtype": 1,
            }
        ],
        "bda": {"fires": "None", "structural": "Light"},
        "missile_defence": None,
        "detected_by_side_guid": "SIDE-1",
        "observer_side_guid_shared": None,
        "observer_posture": "H",
        "marked_as_decoy": False,
        "filter_out": False,
        "targeted_by_unit_guids": ["UNIT-1"],
        "fired_on_by_unit_guids": ["UNIT-1"],
        "firing_at_contact_guids": [],
    }


def _contact_weapon_allocations_result() -> dict[str, JsonValue]:
    return {
        "contact_guid": "CONTACT-1",
        "allocations": [
            {
                "shooter_guid": "UNIT-1",
                "quantity_assigned": 2,
                "weapon_dbid": 2001,
                "weapon_name": "AAM",
                "target_guid": "CONTACT-1",
                "quantity_fired": 1,
            }
        ],
    }


def _unit_inventory_result() -> dict[str, JsonValue]:
    return {
        "unit_guid": "UNIT-1",
        "sensors": [
            {
                "guid": "SENSOR-1",
                "dbid": 4001,
                "name": "Radar",
                "max_range_nm": 120.0,
                "active": True,
                "status": "Operational",
                "role": "Search",
                "type": "Radar",
            }
        ],
        "mounts": [
            {
                "guid": "MOUNT-1",
                "dbid": 3001,
                "name": "Launcher",
                "status": "Operational",
                "status_readable": "Operational",
                "rate_of_fire": 2.0,
                "capacity": 8,
                "weapons": [
                    {
                        "guid": "MOUNT-WEAPON-1",
                        "dbid": 2001,
                        "name": "AAM",
                        "type_code": 200,
                        "current": 6,
                        "max_capacity": 8,
                        "default": 8,
                    }
                ],
            }
        ],
        "magazines": [
            {
                "guid": "MAG-1",
                "dbid": 5001,
                "name": "Main Magazine",
                "status": "Operational",
                "status_readable": "Operational",
                "is_aviation_magazine": False,
                "capacity": 100,
                "weapons": [
                    {
                        "guid": "MAG-WEAPON-1",
                        "dbid": 2001,
                        "name": "AAM",
                        "type_code": 200,
                        "current": 40,
                        "max_capacity": 60,
                        "default": 60,
                    }
                ],
            }
        ],
        "cargo": [
            {
                "guid": "CARGO-1",
                "object_type": 1,
                "storage_type": "Internal",
                "dbid": 6001,
                "name": "Supplies",
                "status": "Ready",
                "status_readable": "Ready",
                "quantity": 2,
                "area": 10.0,
                "crew": 0,
                "mass": 5.0,
                "required_size": 2.0,
                "container_cargo": False,
            }
        ],
    }


def _unit_sensor_set_result() -> dict[str, JsonValue]:
    return {
        "unit_guid": "UNIT-1",
        "sensor_guid": "SENSOR-1",
        "active": True,
        "obey_emcon": False,
    }


def _unit_cargo_transfer_result() -> dict[str, JsonValue]:
    return {
        "from_unit_guid": "UNIT-1",
        "to_unit_guid": "UNIT-2",
        "command": "transfer",
        "accepted": True,
    }


def _unit_cargo_unload_result() -> dict[str, JsonValue]:
    return {"unit_guid": "UNIT-2", "command": "unload", "accepted": True}


def _mission_cargo_update_result() -> dict[str, JsonValue]:
    return {
        "mission_guid": "MISSION-2",
        "action": "assign",
        "assigned_cargo": [{"object_type": 1, "dbid": 6001, "guid": "CARGO-1", "quantity": 1}],
    }


def _doctrine_wra_result() -> dict[str, JsonValue]:
    return {
        "scope": "mission",
        "target_guid": "MISSION-2",
        "level": "Mission",
        "target_type": "Air_Contact_Unknown_Type",
        "entries": [
            {
                "weapon_dbid": 2001,
                "weapon_name": "AAM",
                "weapons_per_salvo": 2,
                "shooters_per_salvo": 1,
                "firing_range": "75ofmax",
                "self_defence_range": "max",
            }
        ],
    }


def _special_action_list_result() -> dict[str, JsonValue]:
    return {
        "items": [
            {
                "guid": "ACTION-1",
                "name": "Request reinforcements",
                "description": "Request available reinforcements.",
                "active": True,
                "repeatable": False,
            }
        ]
    }


def _special_action_execute_result() -> dict[str, JsonValue]:
    return {
        "action_guid": "ACTION-1",
        "name": "Request reinforcements",
        "accepted": True,
        "active": False,
        "repeatable": False,
    }


def _structured_result(value: object) -> dict[str, JsonValue]:
    assert isinstance(value, tuple)
    pair = cast(tuple[object, ...], value)
    assert len(pair) == 2
    content, structured = pair
    assert isinstance(content, list)
    assert isinstance(structured, dict)
    return cast(dict[str, JsonValue], structured)


@pytest.mark.asyncio
async def test_server_exposes_local_tools_with_operation_annotations() -> None:
    server = create_mcp_server(_FakeApplication({}))

    tools = await server.list_tools()
    tools_by_name = {tool.name: tool for tool in tools}

    assert set(tools_by_name) == {
        "cmo_bridge_diagnose",
        "cmo_bridge_prepare",
        "cmo_bridge_status",
        "cmo_scenario_get",
        "cmo_scenario_time_compression_set",
        "cmo_side_list",
        "cmo_side_posture_get",
        "cmo_unit_list",
        "cmo_unit_get",
        "cmo_unit_combat_status_get",
        "cmo_unit_loadout_get",
        "cmo_unit_inventory_get",
        "cmo_unit_sensor_set",
        "cmo_unit_magazine_adjust",
        "cmo_unit_mount_reload_adjust",
        "cmo_unit_set",
        "cmo_unit_assign_mission",
        "cmo_unit_loadout_set",
        "cmo_unit_launch",
        "cmo_unit_rtb",
        "cmo_unit_refuel",
        "cmo_unit_cargo_transfer",
        "cmo_unit_cargo_unload",
        "cmo_unit_attack_contact",
        "cmo_contact_list",
        "cmo_contact_get",
        "cmo_contact_posture_set",
        "cmo_contact_weapon_allocations_get",
        "cmo_mission_list",
        "cmo_mission_get",
        "cmo_reference_point_list",
        "cmo_reference_point_add",
        "cmo_reference_point_update",
        "cmo_doctrine_get",
        "cmo_unit_add",
        "cmo_unit_unassign_mission",
        "cmo_mission_create",
        "cmo_mission_update",
        "cmo_mission_air_refueling_update",
        "cmo_mission_flight_plan_list",
        "cmo_mission_flight_plan_create",
        "cmo_mission_target_add",
        "cmo_mission_target_remove",
        "cmo_mission_cargo_update",
        "cmo_doctrine_set",
        "cmo_doctrine_wra_get",
        "cmo_doctrine_wra_set",
        "cmo_emcon_set",
        "cmo_special_action_list",
        "cmo_special_action_execute",
        "cmo_score_get",
        "cmo_scenario_weather_get",
        "cmo_scenario_weather_set",
        "cmo_scenario_title_set",
        "cmo_scenario_timeline_set",
        "cmo_side_add",
        "cmo_side_options_set",
        "cmo_side_posture_set",
        "cmo_score_set",
        "cmo_event_list",
        "cmo_event_get",
        "cmo_event_set",
        "cmo_event_component_set",
        "cmo_event_component_link",
        "cmo_special_action_create",
        "cmo_special_action_update",
        "cmo_unit_delete_preview",
        "cmo_unit_delete_confirm",
        "cmo_mission_delete_preview",
        "cmo_mission_delete_confirm",
    }
    mutation_tools = {
        "cmo_bridge_prepare",
        "cmo_scenario_time_compression_set",
        "cmo_unit_sensor_set",
        "cmo_unit_magazine_adjust",
        "cmo_unit_mount_reload_adjust",
        "cmo_unit_set",
        "cmo_unit_assign_mission",
        "cmo_unit_loadout_set",
        "cmo_unit_launch",
        "cmo_unit_rtb",
        "cmo_unit_refuel",
        "cmo_unit_cargo_transfer",
        "cmo_unit_cargo_unload",
        "cmo_unit_attack_contact",
        "cmo_contact_posture_set",
        "cmo_unit_add",
        "cmo_unit_unassign_mission",
        "cmo_reference_point_add",
        "cmo_reference_point_update",
        "cmo_mission_create",
        "cmo_mission_update",
        "cmo_mission_air_refueling_update",
        "cmo_mission_flight_plan_create",
        "cmo_mission_target_add",
        "cmo_mission_target_remove",
        "cmo_mission_cargo_update",
        "cmo_doctrine_set",
        "cmo_doctrine_wra_set",
        "cmo_emcon_set",
        "cmo_special_action_execute",
        "cmo_scenario_weather_set",
        "cmo_scenario_title_set",
        "cmo_scenario_timeline_set",
        "cmo_side_add",
        "cmo_side_options_set",
        "cmo_side_posture_set",
        "cmo_score_set",
        "cmo_event_set",
        "cmo_event_component_set",
        "cmo_event_component_link",
        "cmo_special_action_create",
        "cmo_special_action_update",
        "cmo_unit_delete_confirm",
        "cmo_mission_delete_confirm",
    }
    non_idempotent_tools = {
        "cmo_unit_add",
        "cmo_unit_magazine_adjust",
        "cmo_unit_mount_reload_adjust",
        "cmo_unit_loadout_set",
        "cmo_unit_launch",
        "cmo_unit_rtb",
        "cmo_unit_refuel",
        "cmo_unit_cargo_transfer",
        "cmo_unit_cargo_unload",
        "cmo_reference_point_add",
        "cmo_mission_create",
        "cmo_mission_flight_plan_create",
        "cmo_mission_cargo_update",
        "cmo_unit_attack_contact",
        "cmo_special_action_execute",
        "cmo_side_add",
        "cmo_event_set",
        "cmo_event_component_set",
        "cmo_event_component_link",
        "cmo_special_action_create",
        "cmo_special_action_update",
        "cmo_unit_delete_confirm",
        "cmo_mission_delete_confirm",
    }
    destructive_tools = {
        "cmo_unit_attack_contact",
        "cmo_special_action_execute",
        "cmo_event_set",
        "cmo_event_component_set",
        "cmo_event_component_link",
        "cmo_special_action_update",
        "cmo_unit_delete_confirm",
        "cmo_mission_delete_confirm",
    }
    for name, tool in tools_by_name.items():
        assert tool.outputSchema is not None
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is (name not in mutation_tools)
        assert tool.annotations.destructiveHint is (name in destructive_tools)
        assert tool.annotations.idempotentHint is (name not in non_idempotent_tools)
        assert tool.annotations.openWorldHint is False

    for name in {
        "cmo_unit_launch",
        "cmo_unit_rtb",
        "cmo_unit_refuel",
        "cmo_unit_attack_contact",
    }:
        description = tools_by_name[name].description
        assert description is not None
        assert "accepted, not completed" in description

    compression_description = tools_by_name["cmo_scenario_time_compression_set"].description
    assert compression_description is not None
    assert "0=1x" in compression_description
    assert "3=15x" in compression_description
    assert "preserve cmo_scenario_get's multiplier" in compression_description
    assert "1/2/5/15/30/150" in compression_description

    scenario_tool = tools_by_name["cmo_scenario_get"]
    assert scenario_tool.description is not None
    assert "current player-side GUID" in scenario_tool.description
    scenario_schema = cast(dict[str, JsonValue], scenario_tool.outputSchema)
    scenario_properties = cast(dict[str, JsonValue], scenario_schema["properties"])
    assert "player_side_guid" in scenario_properties
    assert "player_side_guid" in cast(list[JsonValue], scenario_schema["required"])


@pytest.mark.asyncio
async def test_final_campaign_contract_is_reflected_in_mcp_input_schemas() -> None:
    server = create_mcp_server(_FakeApplication({}))
    tools_by_name = {tool.name: tool for tool in await server.list_tools()}

    def any_of(tool_name: str, field_name: str) -> list[dict[str, JsonValue]]:
        schema = cast(dict[str, JsonValue], tools_by_name[tool_name].inputSchema)
        properties = cast(dict[str, JsonValue], schema["properties"])
        field_schema = cast(dict[str, JsonValue], properties[field_name])
        return [
            cast(dict[str, JsonValue], branch)
            for branch in cast(list[JsonValue], field_schema["anyOf"])
        ]

    manual_throttle = any_of("cmo_unit_set", "manual_throttle")
    assert any(branch.get("type") == "string" for branch in manual_throttle)
    assert any(branch.get("type") == "number" for branch in manual_throttle)

    strike_trigger = any_of("cmo_mission_update", "strike_minimum_trigger")
    assert any(
        branch.get("type") == "string" and branch.get("minLength") == 1 for branch in strike_trigger
    )

    doctrine_boolean = any_of("cmo_doctrine_set", "engage_opportunity_targets")
    assert any(branch.get("type") == "boolean" for branch in doctrine_boolean)
    assert any(branch.get("const") == "inherit" for branch in doctrine_boolean)

    for tool_name in {"cmo_doctrine_wra_get", "cmo_doctrine_wra_set"}:
        target_type = any_of(tool_name, "target_type")
        assert any(
            branch.get("type") == "string" and branch.get("minLength") == 1
            for branch in target_type
        )
        assert any(branch.get("type") == "integer" for branch in target_type)


@pytest.mark.asyncio
async def test_campaign_extension_tools_map_to_operations_and_structured_results() -> None:
    time_compression = _time_compression_result()
    side_posture = _side_posture_result()
    contact = _contact_detail_result()
    allocations = _contact_weapon_allocations_result()
    inventory = _unit_inventory_result()
    sensor = _unit_sensor_set_result()
    cargo_transfer = _unit_cargo_transfer_result()
    cargo_unload = _unit_cargo_unload_result()
    mission_cargo = _mission_cargo_update_result()
    wra = _doctrine_wra_result()
    special_actions = _special_action_list_result()
    special_action = _special_action_execute_result()
    application = _FakeApplication(
        {
            "scenario.time_compression.set": _success(time_compression),
            "side.posture.get": _success(side_posture),
            "contact.get": _success(contact),
            "contact.posture.set": _success(contact),
            "contact.weapon_allocations.get": _success(allocations),
            "unit.inventory.get": _success(inventory),
            "unit.sensor.set": _success(sensor),
            "unit.magazine.adjust": _success(inventory),
            "unit.mount_reload.adjust": _success(inventory),
            "unit.cargo.transfer": _success(cargo_transfer),
            "unit.cargo.unload": _success(cargo_unload),
            "mission.cargo.update": _success(mission_cargo),
            "doctrine.wra.get": _success(wra),
            "doctrine.wra.set": _success(wra),
            "special_action.list": _success(special_actions),
            "special_action.execute": _success(special_action),
        }
    )
    server = create_mcp_server(application)

    responses = [
        await server.call_tool("cmo_scenario_time_compression_set", {"code": 2}),
        await server.call_tool(
            "cmo_side_posture_get",
            {"side_a_guid": "SIDE-1", "side_b_guid": "SIDE-2"},
        ),
        await server.call_tool(
            "cmo_contact_get",
            {"side_guid": "SIDE-1", "contact_guid": "CONTACT-1"},
        ),
        await server.call_tool(
            "cmo_contact_posture_set",
            {"side_guid": "SIDE-1", "contact_guid": "CONTACT-1", "posture": "H"},
        ),
        await server.call_tool(
            "cmo_contact_weapon_allocations_get",
            {"side_guid": "SIDE-1", "contact_guid": "CONTACT-1"},
        ),
        await server.call_tool("cmo_unit_inventory_get", {"unit_guid": "UNIT-1"}),
        await server.call_tool(
            "cmo_unit_sensor_set",
            {
                "unit_guid": "UNIT-1",
                "sensor_guid": "SENSOR-1",
                "active": True,
                "obey_emcon": False,
            },
        ),
        await server.call_tool(
            "cmo_unit_magazine_adjust",
            {
                "unit_guid": "UNIT-1",
                "magazine_guid": "MAG-1",
                "weapon_dbid": 2001,
                "mode": "add",
                "quantity": 2,
                "max_capacity": 60,
                "allow_new": True,
            },
        ),
        await server.call_tool(
            "cmo_unit_mount_reload_adjust",
            {
                "unit_guid": "UNIT-1",
                "mount_guid": "MOUNT-1",
                "weapon_dbid": 2001,
                "mode": "fill",
                "add_as_cell": False,
            },
        ),
        await server.call_tool(
            "cmo_unit_cargo_transfer",
            {
                "from_unit_guid": "UNIT-1",
                "to_unit_guid": "UNIT-2",
                "items": [
                    {"cargo_guid": "CARGO-1"},
                    {"dbid": 6001, "quantity": 1},
                ],
            },
        ),
        await server.call_tool("cmo_unit_cargo_unload", {"unit_guid": "UNIT-2"}),
        await server.call_tool(
            "cmo_mission_cargo_update",
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-2",
                "action": "assign",
                "cargo_kind": "object",
                "dbid": 6001,
                "object_type": 1,
                "cargo_guid": "CARGO-1",
            },
        ),
        await server.call_tool(
            "cmo_doctrine_wra_get",
            {
                "scope": "mission",
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-2",
                "target_type": "Air_Contact_Unknown_Type",
                "full_wra": True,
            },
        ),
        await server.call_tool(
            "cmo_doctrine_wra_set",
            {
                "scope": "mission",
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-2",
                "weapon_dbid": 2001,
                "target_type": "Air_Contact_Unknown_Type",
                "weapons_per_salvo": 2,
                "shooters_per_salvo": 1,
                "firing_range": "75ofmax",
                "self_defence_range": "max",
            },
        ),
        await server.call_tool("cmo_special_action_list", {"side_guid": "SIDE-1"}),
        await server.call_tool(
            "cmo_special_action_execute",
            {"side_guid": "SIDE-1", "action_guid": "ACTION-1"},
        ),
    ]

    expected_results = [
        time_compression,
        side_posture,
        contact,
        contact,
        allocations,
        inventory,
        sensor,
        inventory,
        inventory,
        cargo_transfer,
        cargo_unload,
        mission_cargo,
        wra,
        wra,
        special_actions,
        special_action,
    ]
    assert [_structured_result(response) for response in responses] == expected_results
    assert application.calls == [
        _Call("scenario.time_compression.set", {"code": 2}, None),
        _Call(
            "side.posture.get",
            {"side_a_guid": "SIDE-1", "side_b_guid": "SIDE-2"},
            None,
        ),
        _Call(
            "contact.get",
            {"side_guid": "SIDE-1", "side_name": None, "contact_guid": "CONTACT-1"},
            None,
        ),
        _Call(
            "contact.posture.set",
            {"side_guid": "SIDE-1", "contact_guid": "CONTACT-1", "posture": "H"},
            None,
        ),
        _Call(
            "contact.weapon_allocations.get",
            {"side_guid": "SIDE-1", "contact_guid": "CONTACT-1"},
            None,
        ),
        _Call("unit.inventory.get", {"unit_guid": "UNIT-1"}, None),
        _Call(
            "unit.sensor.set",
            {
                "unit_guid": "UNIT-1",
                "sensor_guid": "SENSOR-1",
                "active": True,
                "obey_emcon": False,
            },
            None,
        ),
        _Call(
            "unit.magazine.adjust",
            {
                "unit_guid": "UNIT-1",
                "magazine_guid": "MAG-1",
                "weapon_dbid": 2001,
                "mode": "add",
                "quantity": 2,
                "max_capacity": 60,
                "allow_new": True,
            },
            None,
        ),
        _Call(
            "unit.mount_reload.adjust",
            {
                "unit_guid": "UNIT-1",
                "mount_guid": "MOUNT-1",
                "weapon_dbid": 2001,
                "mode": "fill",
                "quantity": None,
                "add_as_cell": False,
            },
            None,
        ),
        _Call(
            "unit.cargo.transfer",
            {
                "from_unit_guid": "UNIT-1",
                "to_unit_guid": "UNIT-2",
                "items": [
                    {"cargo_guid": "CARGO-1", "dbid": None, "quantity": None},
                    {"cargo_guid": None, "dbid": 6001, "quantity": 1},
                ],
            },
            None,
        ),
        _Call("unit.cargo.unload", {"unit_guid": "UNIT-2"}, None),
        _Call(
            "mission.cargo.update",
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-2",
                "action": "assign",
                "cargo_kind": "object",
                "dbid": 6001,
                "object_type": 1,
                "cargo_guid": "CARGO-1",
                "quantity": None,
            },
            None,
        ),
        _Call(
            "doctrine.wra.get",
            {
                "scope": "mission",
                "side_guid": "SIDE-1",
                "unit_guid": None,
                "mission_guid": "MISSION-2",
                "weapon_dbid": None,
                "contact_guid": None,
                "target_type": "Air_Contact_Unknown_Type",
                "full_wra": True,
            },
            None,
        ),
        _Call(
            "doctrine.wra.set",
            {
                "scope": "mission",
                "side_guid": "SIDE-1",
                "unit_guid": None,
                "mission_guid": "MISSION-2",
                "weapon_dbid": 2001,
                "contact_guid": None,
                "target_type": "Air_Contact_Unknown_Type",
                "weapons_per_salvo": 2,
                "shooters_per_salvo": 1,
                "firing_range": "75ofmax",
                "self_defence_range": "max",
            },
            None,
        ),
        _Call(
            "special_action.list",
            {"side_guid": "SIDE-1", "side_name": None},
            None,
        ),
        _Call(
            "special_action.execute",
            {"side_guid": "SIDE-1", "action_guid": "ACTION-1"},
            None,
        ),
    ]


@pytest.mark.asyncio
async def test_existing_tools_forward_extended_campaign_options() -> None:
    unit_set = _unit_set_result()
    mission_update = _mission_update_result()
    doctrine_set = _doctrine_set_result()
    application = _FakeApplication(
        {
            "unit.set": _success(unit_set),
            "mission.update": _success(mission_update),
            "doctrine.set": _success(doctrine_set),
        }
    )
    server = create_mcp_server(application)

    unit_response = await server.call_tool(
        "cmo_unit_set",
        {
            "unit_guid": "UNIT-1",
            "depth": -80.0,
            "throttle": "Loiter",
            "force_speed": True,
            "desired_heading": 180.0,
            "move_to": True,
            "manual_throttle": 2.5,
            "manual_speed": 12.0,
            "manual_altitude": -60.0,
            "hold_position": False,
            "hold_fire": True,
            "sprint_drift": True,
            "avoid_cavitation": True,
            "obey_emcon": False,
        },
    )
    mission_response = await server.call_tool(
        "cmo_mission_update",
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-2",
            "destination_guid": "UNIT-DESTINATION",
            "loop_type": 1,
            "active_emcon": True,
            "check_opa": True,
            "check_wwr": False,
            "group_size": 2,
            "use_group_size": True,
            "transit_throttle_aircraft": "Cruise",
            "transit_throttle_ship": "Cruise",
            "transit_throttle_submarine": "Loiter",
            "station_throttle_aircraft": "Loiter",
            "station_throttle_ship": "Loiter",
            "station_throttle_submarine": "Creep",
            "attack_throttle_aircraft": "Military",
            "attack_throttle_ship": "Full",
            "attack_throttle_submarine": "Full",
            "transit_altitude_aircraft": 9_000.0,
            "station_altitude_aircraft": 10_000.0,
            "attack_altitude_aircraft": 6_000.0,
            "transit_depth_submarine": -80.0,
            "station_depth_submarine": -120.0,
            "attack_depth_submarine": -60.0,
            "strike_minimum_trigger": "3",
            "strike_max_flights": 4,
            "strike_auto_planner": True,
            "strike_min_distance_aircraft": 20.0,
            "strike_max_distance_aircraft": 500.0,
            "strike_min_distance_ship": 10.0,
            "strike_max_distance_ship": 300.0,
            "focus_on_strike": True,
            "arming_delay": "0:00:10:00",
            "mines_per_set": 4,
            "mine_spacing_m": 500.0,
            "set_spacing_m": 2_000.0,
            "laying_method": 1,
            "cargo_subtype": "transfer",
            "move_all_cargo": True,
            "allow_ground_self_delivery": False,
        },
    )
    doctrine_response = await server.call_tool(
        "cmo_doctrine_set",
        {
            "scope": "mission",
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-2",
            "engage_opportunity_targets": "inherit",
            "automatic_evasion": True,
            "ignore_plotted_course": False,
            "ignore_emcon_while_under_attack": "inherit",
            "maintain_standoff": True,
            "use_sams_in_anti_surface_mode": "inherit",
            "engaging_ambiguous_targets": "Optimistic",
            "fuel_state_planned": "Joker40Percent",
            "fuel_state_rtb": "YesLeaveGroup",
            "weapon_state_planned": "Winchester",
            "weapon_state_rtb": "YesLastUnit",
            "withdraw_on_attack": "Percent50",
            "withdraw_on_damage": "Percent25",
            "withdraw_on_defence": "Exhausted",
            "withdraw_on_fuel": "Bingo",
            "bvr_logic": 1,
            "dipping_sonar": 0,
            "use_aip": 1,
            "recharge_on_attack": 30,
            "recharge_on_patrol": 60,
        },
    )

    assert _structured_result(unit_response) == unit_set
    assert _structured_result(mission_response) == mission_update
    assert _structured_result(doctrine_response) == doctrine_set
    assert application.calls == [
        _Call(
            "unit.set",
            {
                "unit_guid": "UNIT-1",
                "name": None,
                "speed": None,
                "altitude": None,
                "depth": -80.0,
                "heading": None,
                "throttle": "Loiter",
                "force_speed": True,
                "desired_heading": 180.0,
                "move_to": True,
                "manual_throttle": 2.5,
                "manual_speed": 12.0,
                "manual_altitude": -60.0,
                "hold_position": False,
                "hold_fire": True,
                "sprint_drift": True,
                "avoid_cavitation": True,
                "obey_emcon": False,
                "course": None,
            },
            None,
        ),
        _Call(
            "mission.update",
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-2",
                "destination_guid": "UNIT-DESTINATION",
                "loop_type": 1,
                "active_emcon": True,
                "check_opa": True,
                "check_wwr": False,
                "group_size": 2,
                "use_group_size": True,
                "transit_throttle_aircraft": "Cruise",
                "transit_throttle_ship": "Cruise",
                "transit_throttle_submarine": "Loiter",
                "station_throttle_aircraft": "Loiter",
                "station_throttle_ship": "Loiter",
                "station_throttle_submarine": "Creep",
                "attack_throttle_aircraft": "Military",
                "attack_throttle_ship": "Full",
                "attack_throttle_submarine": "Full",
                "transit_altitude_aircraft": 9_000.0,
                "station_altitude_aircraft": 10_000.0,
                "attack_altitude_aircraft": 6_000.0,
                "transit_depth_submarine": -80.0,
                "station_depth_submarine": -120.0,
                "attack_depth_submarine": -60.0,
                "strike_minimum_trigger": "3",
                "strike_max_flights": 4,
                "strike_auto_planner": True,
                "strike_min_distance_aircraft": 20.0,
                "strike_max_distance_aircraft": 500.0,
                "strike_min_distance_ship": 10.0,
                "strike_max_distance_ship": 300.0,
                "focus_on_strike": True,
                "arming_delay": "0:00:10:00",
                "mines_per_set": 4,
                "mine_spacing_m": 500.0,
                "set_spacing_m": 2_000.0,
                "laying_method": 1,
                "cargo_subtype": "transfer",
                "move_all_cargo": True,
                "allow_ground_self_delivery": False,
            },
            None,
        ),
        _Call(
            "doctrine.set",
            {
                "scope": "mission",
                "side_guid": "SIDE-1",
                "unit_guid": None,
                "mission_guid": "MISSION-2",
                "engage_opportunity_targets": "inherit",
                "automatic_evasion": True,
                "ignore_plotted_course": False,
                "ignore_emcon_while_under_attack": "inherit",
                "maintain_standoff": True,
                "use_sams_in_anti_surface_mode": "inherit",
                "engaging_ambiguous_targets": "Optimistic",
                "fuel_state_planned": "Joker40Percent",
                "fuel_state_rtb": "YesLeaveGroup",
                "weapon_state_planned": "Winchester",
                "weapon_state_rtb": "YesLastUnit",
                "withdraw_on_attack": "Percent50",
                "withdraw_on_damage": "Percent25",
                "withdraw_on_defence": "Exhausted",
                "withdraw_on_fuel": "Bingo",
                "bvr_logic": 1,
                "dipping_sonar": 0,
                "use_aip": 1,
                "recharge_on_attack": 30,
                "recharge_on_patrol": 60,
            },
            None,
        ),
    ]


@pytest.mark.asyncio
async def test_tools_map_to_bridge_operations_and_return_structured_results() -> None:
    status = _status_result()
    scenario = _scenario_result()
    sides = _side_list_result()
    units = _unit_list_result()
    unit = _unit_result()
    unit_set = _unit_set_result()
    unit_assignment = _unit_assign_mission_result()
    combat_status = _unit_combat_status_result()
    loadout = _unit_loadout_result()
    launch = _unit_command_result("launch")
    rtb = _unit_command_result("rtb")
    refuel = _unit_command_result("refuel")
    attack = _unit_attack_contact_result()
    contacts = _contact_list_result()
    missions = _mission_list_result()
    mission = _mission_result()
    reference_points = _reference_point_list_result()
    reference_point = _reference_point_result()
    doctrine = _doctrine_result()
    unit_add = _unit_add_result()
    unit_unassignment = _unit_unassign_mission_result()
    mission_create = _mission_create_result()
    mission_update = _mission_update_result()
    target_add = _mission_target_add_result()
    target_remove = _mission_target_remove_result()
    doctrine_set = _doctrine_set_result()
    emcon_set = _emcon_set_result()
    score = _score_result()
    application = _FakeApplication(
        {
            "bridge.status": _success(status),
            "scenario.get": _success(scenario),
            "side.list": _success(sides),
            "unit.list": _success(units),
            "unit.get": _success(unit),
            "unit.combat_status.get": _success(combat_status),
            "unit.loadout.get": _success(loadout),
            "unit.set": _success(unit_set),
            "unit.assign_mission": _success(unit_assignment),
            "unit.loadout.set": _success(loadout),
            "unit.launch": _success(launch),
            "unit.rtb": _success(rtb),
            "unit.refuel": _success(refuel),
            "unit.attack_contact": _success(attack),
            "contact.list": _success(contacts),
            "mission.list": _success(missions),
            "mission.get": _success(mission),
            "reference_point.list": _success(reference_points),
            "reference_point.add": _success(reference_point),
            "reference_point.update": _success(reference_point),
            "doctrine.get": _success(doctrine),
            "unit.add": _success(unit_add),
            "unit.unassign_mission": _success(unit_unassignment),
            "mission.create": _success(mission_create),
            "mission.update": _success(mission_update),
            "mission.target.add": _success(target_add),
            "mission.target.remove": _success(target_remove),
            "doctrine.set": _success(doctrine_set),
            "emcon.set": _success(emcon_set),
            "lua.call": _success(score),
        }
    )
    server = create_mcp_server(application)

    status_response = await server.call_tool(
        "cmo_bridge_status",
        {"accept_lineage_id": "33333333-3333-4333-8333-333333333333"},
    )
    scenario_response = await server.call_tool("cmo_scenario_get", {})
    side_response = await server.call_tool(
        "cmo_side_list",
        {"page_size": 1, "cursor": "1"},
    )
    unit_list_response = await server.call_tool(
        "cmo_unit_list",
        {
            "side_name": "PLAAF",
            "unit_type": "Aircraft",
            "name_contains": "J-36",
            "page_size": 1,
            "cursor": "1",
        },
    )
    unit_get_response = await server.call_tool(
        "cmo_unit_get",
        {"unit_guid": "UNIT-1"},
    )
    unit_combat_status_response = await server.call_tool(
        "cmo_unit_combat_status_get",
        {"unit_guid": "UNIT-1"},
    )
    unit_loadout_get_response = await server.call_tool(
        "cmo_unit_loadout_get",
        {"unit_guid": "UNIT-1"},
    )
    unit_set_response = await server.call_tool(
        "cmo_unit_set",
        {
            "unit_guid": "UNIT-1",
            "name": "J-36 Lead",
            "speed": 500.0,
            "altitude": 11_000.0,
            "heading": 95.0,
            "course": [
                {
                    "latitude": 24.0,
                    "longitude": 122.0,
                    "altitude": 12_000.0,
                }
            ],
        },
    )
    unit_assign_mission_response = await server.call_tool(
        "cmo_unit_assign_mission",
        {
            "unit_guid": "UNIT-1",
            "mission_guid": "MISSION-1",
            "escort": True,
        },
    )
    unit_loadout_set_response = await server.call_tool(
        "cmo_unit_loadout_set",
        {
            "unit_guid": "UNIT-1",
            "loadout_dbid": 12345,
            "time_to_ready_minutes": 30.0,
            "ignore_magazines": True,
            "exclude_optional_weapons": True,
        },
    )
    unit_launch_response = await server.call_tool(
        "cmo_unit_launch",
        {"unit_guid": "UNIT-1"},
    )
    unit_rtb_response = await server.call_tool(
        "cmo_unit_rtb",
        {"unit_guid": "UNIT-1"},
    )
    unit_refuel_response = await server.call_tool(
        "cmo_unit_refuel",
        {
            "unit_guid": "UNIT-1",
            "tanker_mission_guids": ["MISSION-TANKER-1", "MISSION-TANKER-2"],
        },
    )
    unit_attack_contact_response = await server.call_tool(
        "cmo_unit_attack_contact",
        {
            "side_guid": "SIDE-1",
            "attacker_unit_guid": "UNIT-1",
            "contact_guid": "CONTACT-1",
            "mode": "manual_weapon",
            "mount_dbid": 3001,
            "weapon_dbid": 2001,
            "quantity": 2,
        },
    )
    contact_response = await server.call_tool(
        "cmo_contact_list",
        {
            "side_guid": "SIDE-1",
            "contact_type": "Air",
            "page_size": 1,
        },
    )
    mission_list_response = await server.call_tool(
        "cmo_mission_list",
        {
            "side_name": "PLAAF",
            "mission_class": "patrol",
            "page_size": 1,
        },
    )
    mission_get_response = await server.call_tool(
        "cmo_mission_get",
        {"side_guid": "SIDE-1", "mission_name": "CAP"},
    )
    reference_point_list_response = await server.call_tool(
        "cmo_reference_point_list",
        {"side_guid": "SIDE-1", "page_size": 25},
    )
    reference_point_add_response = await server.call_tool(
        "cmo_reference_point_add",
        {
            "side_guid": "SIDE-1",
            "name": "Northwest",
            "latitude": 24.0,
            "longitude": 122.0,
        },
    )
    reference_point_update_response = await server.call_tool(
        "cmo_reference_point_update",
        {
            "side_guid": "SIDE-1",
            "reference_point_guid": "RP-1",
            "latitude": 24.0,
            "longitude": 122.0,
        },
    )
    doctrine_get_response = await server.call_tool(
        "cmo_doctrine_get",
        {
            "scope": "mission",
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "actual": True,
        },
    )
    unit_add_response = await server.call_tool(
        "cmo_unit_add",
        {
            "side_guid": "SIDE-1",
            "unit_type": "Aircraft",
            "dbid": 6611,
            "name": "J-36 #2",
            "base_guid": "BASE-1",
            "loadout_dbid": 12345,
        },
    )
    unit_unassign_mission_response = await server.call_tool(
        "cmo_unit_unassign_mission",
        {"unit_guid": "UNIT-1"},
    )
    mission_create_response = await server.call_tool(
        "cmo_mission_create",
        {
            "side_guid": "SIDE-1",
            "name": "North CAP",
            "details": {
                "mission_class": "patrol",
                "patrol_type": "aaw",
                "reference_point_guids": ["RP-1", "RP-2", "RP-3", "RP-4"],
            },
        },
    )
    mission_update_response = await server.call_tool(
        "cmo_mission_update",
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-2",
            "active": True,
            "flight_size": 2,
            "use_flight_size": True,
            "minimum_aircraft_required": 2,
            "on_station": 1,
            "one_third_rule": False,
            "reference_point_guids": ["RP-1", "RP-2", "RP-3", "RP-4"],
            "prosecution_zone_reference_point_guids": ["RP-5", "RP-6", "RP-7"],
        },
    )
    mission_target_add_response = await server.call_tool(
        "cmo_mission_target_add",
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-2",
            "target_guid": "CONTACT-1",
        },
    )
    mission_target_remove_response = await server.call_tool(
        "cmo_mission_target_remove",
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-2",
            "target_guid": "CONTACT-1",
        },
    )
    doctrine_set_response = await server.call_tool(
        "cmo_doctrine_set",
        {
            "scope": "mission",
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-2",
            "weapon_control_air": "inherit",
        },
    )
    emcon_set_response = await server.call_tool(
        "cmo_emcon_set",
        {
            "scope": "mission",
            "target_guid": "MISSION-2",
            "inherit": True,
        },
    )
    score_response = await server.call_tool(
        "cmo_score_get",
        {"side": "PLAAF"},
    )

    assert _structured_result(status_response) == status
    assert _structured_result(scenario_response) == scenario
    assert _structured_result(side_response) == sides
    assert _structured_result(unit_list_response) == units
    assert _structured_result(unit_get_response) == unit
    assert _structured_result(unit_combat_status_response) == combat_status
    assert _structured_result(unit_loadout_get_response) == loadout
    assert _structured_result(unit_set_response) == unit_set
    assert _structured_result(unit_assign_mission_response) == unit_assignment
    assert _structured_result(unit_loadout_set_response) == loadout
    assert _structured_result(unit_launch_response) == launch
    assert _structured_result(unit_rtb_response) == rtb
    assert _structured_result(unit_refuel_response) == refuel
    assert _structured_result(unit_attack_contact_response) == attack
    assert _structured_result(contact_response) == contacts
    assert _structured_result(mission_list_response) == missions
    assert _structured_result(mission_get_response) == mission
    assert _structured_result(reference_point_list_response) == reference_points
    assert _structured_result(reference_point_add_response) == reference_point
    assert _structured_result(reference_point_update_response) == reference_point
    assert _structured_result(doctrine_get_response) == doctrine
    assert _structured_result(unit_add_response) == unit_add
    assert _structured_result(unit_unassign_mission_response) == unit_unassignment
    assert _structured_result(mission_create_response) == mission_create
    assert _structured_result(mission_update_response) == mission_update
    assert _structured_result(mission_target_add_response) == target_add
    assert _structured_result(mission_target_remove_response) == target_remove
    assert _structured_result(doctrine_set_response) == doctrine_set
    assert _structured_result(emcon_set_response) == emcon_set
    assert _structured_result(score_response) == score
    assert application.calls == [
        _Call(
            "bridge.status",
            {"accept_lineage_id": "33333333-3333-4333-8333-333333333333"},
            None,
        ),
        _Call("scenario.get", {}, None),
        _Call("side.list", {"page_size": 1, "cursor": "1"}, None),
        _Call(
            "unit.list",
            {
                "side_guid": None,
                "side_name": "PLAAF",
                "page_size": 1,
                "cursor": "1",
                "unit_type": "Aircraft",
                "name_contains": "J-36",
            },
            None,
        ),
        _Call(
            "unit.get",
            {
                "unit_guid": "UNIT-1",
                "side_guid": None,
                "side_name": None,
                "unit_name": None,
            },
            None,
        ),
        _Call(
            "unit.combat_status.get",
            {"unit_guid": "UNIT-1"},
            None,
        ),
        _Call(
            "unit.loadout.get",
            {"unit_guid": "UNIT-1"},
            None,
        ),
        _Call(
            "unit.set",
            {
                "unit_guid": "UNIT-1",
                "name": "J-36 Lead",
                "speed": 500.0,
                "altitude": 11_000.0,
                "depth": None,
                "heading": 95.0,
                "throttle": None,
                "force_speed": None,
                "desired_heading": None,
                "move_to": None,
                "manual_throttle": None,
                "manual_speed": None,
                "manual_altitude": None,
                "hold_position": None,
                "hold_fire": None,
                "sprint_drift": None,
                "avoid_cavitation": None,
                "obey_emcon": None,
                "course": [
                    {
                        "latitude": 24.0,
                        "longitude": 122.0,
                        "altitude": 12_000.0,
                    }
                ],
            },
            None,
        ),
        _Call(
            "unit.assign_mission",
            {
                "unit_guid": "UNIT-1",
                "mission_guid": "MISSION-1",
                "escort": True,
            },
            None,
        ),
        _Call(
            "unit.loadout.set",
            {
                "unit_guid": "UNIT-1",
                "loadout_dbid": 12345,
                "time_to_ready_minutes": 30.0,
                "ignore_magazines": True,
                "exclude_optional_weapons": True,
            },
            None,
        ),
        _Call(
            "unit.launch",
            {"unit_guid": "UNIT-1"},
            None,
        ),
        _Call(
            "unit.rtb",
            {"unit_guid": "UNIT-1"},
            None,
        ),
        _Call(
            "unit.refuel",
            {
                "unit_guid": "UNIT-1",
                "tanker_guid": None,
                "tanker_mission_guids": ["MISSION-TANKER-1", "MISSION-TANKER-2"],
            },
            None,
        ),
        _Call(
            "unit.attack_contact",
            {
                "side_guid": "SIDE-1",
                "attacker_unit_guid": "UNIT-1",
                "contact_guid": "CONTACT-1",
                "mode": "manual_weapon",
                "mount_dbid": 3001,
                "weapon_dbid": 2001,
                "quantity": 2,
            },
            None,
        ),
        _Call(
            "contact.list",
            {
                "side_guid": "SIDE-1",
                "side_name": None,
                "page_size": 1,
                "cursor": None,
                "contact_type": "Air",
            },
            None,
        ),
        _Call(
            "mission.list",
            {
                "side_guid": None,
                "side_name": "PLAAF",
                "page_size": 1,
                "cursor": None,
                "mission_class": "patrol",
                "category": None,
            },
            None,
        ),
        _Call(
            "mission.get",
            {
                "side_guid": "SIDE-1",
                "side_name": None,
                "mission_guid": None,
                "mission_name": "CAP",
            },
            None,
        ),
        _Call(
            "reference_point.list",
            {
                "side_guid": "SIDE-1",
                "side_name": None,
                "page_size": 25,
                "cursor": None,
            },
            None,
        ),
        _Call(
            "reference_point.add",
            {
                "side_guid": "SIDE-1",
                "name": "Northwest",
                "latitude": 24.0,
                "longitude": 122.0,
                "relative_to_type": None,
                "relative_to_guid": None,
                "relative_bearing_deg": None,
                "relative_distance_nm": None,
                "bearing_type": None,
            },
            None,
        ),
        _Call(
            "reference_point.update",
            {
                "side_guid": "SIDE-1",
                "reference_point_guid": "RP-1",
                "name": None,
                "latitude": 24.0,
                "longitude": 122.0,
                "relative_to_type": None,
                "relative_to_guid": None,
                "relative_bearing_deg": None,
                "relative_distance_nm": None,
                "bearing_type": None,
                "clear_relative": False,
            },
            None,
        ),
        _Call(
            "doctrine.get",
            {
                "scope": "mission",
                "side_guid": "SIDE-1",
                "unit_guid": None,
                "mission_guid": "MISSION-1",
                "actual": True,
            },
            None,
        ),
        _Call(
            "unit.add",
            {
                "side_guid": "SIDE-1",
                "unit_type": "Aircraft",
                "dbid": 6611,
                "name": "J-36 #2",
                "base_guid": "BASE-1",
                "latitude": None,
                "longitude": None,
                "altitude": None,
                "loadout_dbid": 12345,
            },
            None,
        ),
        _Call(
            "unit.unassign_mission",
            {"unit_guid": "UNIT-1"},
            None,
        ),
        _Call(
            "mission.create",
            {
                "side_guid": "SIDE-1",
                "name": "North CAP",
                "category": "mission",
                "parent_task_pool_guid": None,
                "details": {
                    "mission_class": "patrol",
                    "patrol_type": "aaw",
                    "reference_point_guids": ["RP-1", "RP-2", "RP-3", "RP-4"],
                },
            },
            None,
        ),
        _Call(
            "mission.update",
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-2",
                "active": True,
                "flight_size": 2,
                "use_flight_size": True,
                "minimum_aircraft_required": 2,
                "on_station": 1,
                "one_third_rule": False,
                "reference_point_guids": ["RP-1", "RP-2", "RP-3", "RP-4"],
                "prosecution_zone_reference_point_guids": ["RP-5", "RP-6", "RP-7"],
            },
            None,
        ),
        _Call(
            "mission.target.add",
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-2",
                "target_guid": "CONTACT-1",
            },
            None,
        ),
        _Call(
            "mission.target.remove",
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-2",
                "target_guid": "CONTACT-1",
            },
            None,
        ),
        _Call(
            "doctrine.set",
            {
                "scope": "mission",
                "side_guid": "SIDE-1",
                "unit_guid": None,
                "mission_guid": "MISSION-2",
                "weapon_control_air": "inherit",
            },
            None,
        ),
        _Call(
            "emcon.set",
            {
                "scope": "mission",
                "target_guid": "MISSION-2",
                "inherit": True,
                "radar": None,
                "sonar": None,
                "oecm": None,
            },
            None,
        ),
        _Call(
            "lua.call",
            {
                "function": "ScenEdit_GetScore",
                "arguments": {"side": "PLAAF"},
            },
            None,
        ),
    ]


@pytest.mark.asyncio
async def test_failed_outcome_is_exposed_as_a_structured_tool_error() -> None:
    error: dict[str, JsonValue] = {
        "code": "CMO_NOT_RUNNING",
        "message": "CMO is not running",
        "details": {"hint": "launch CMO and load a scenario"},
    }
    application = _FakeApplication({"scenario.get": _failure(error)})
    server = create_mcp_server(application)

    with pytest.raises(ToolError) as caught:
        await server.call_tool("cmo_scenario_get", {})

    prefix = "Error executing tool cmo_scenario_get: "
    message = str(caught.value)
    assert message.startswith(prefix)
    assert _ERROR_ADAPTER.validate_json(message.removeprefix(prefix)) == error
    assert application.calls == [_Call("scenario.get", {}, None)]
