from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from lupa import LuaRuntime
from pydantic import JsonValue

from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY, ResolvedInvocation
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.models import (
    AllowedDelivery,
    RequestBody,
    ResponseExpectation,
)
from cmo_agent_bridge.protocol.response import parse_inst_response
from cmo_agent_bridge.runtime_bundle import create_runtime_snapshot, render_dispatcher


LINEAGE_ID = UUID("11111111-1111-4111-8111-111111111111")


@dataclass(frozen=True, slots=True)
class _LuaRun:
    result: JsonValue
    export_side: str
    export_units: tuple[str, ...]
    score_sides: tuple[str, ...]
    set_unit_calls: tuple[dict[str, JsonValue], ...]
    assign_mission_calls: tuple[tuple[str, str, bool], ...]
    api_calls: dict[str, tuple[object, ...]]


@dataclass(frozen=True, slots=True)
class _CmoFixture:
    sides: Any
    units_by_guid: dict[str, Any]
    units_by_name: dict[str, Any]
    contacts: Any
    missions: Any
    missions_by_guid: dict[str, Any]
    missions_by_name: dict[str, Any]
    reference_points: Any
    reference_points_by_guid: dict[str, Any]


def _lua_table(lua: LuaRuntime, value: dict[str, object]) -> Any:
    return lua.table_from(value)


def _scenario(lua: LuaRuntime, *, player_side: object = "SIDE-BLUE") -> Any:
    return _lua_table(
        lua,
        {
            "guid": str(LINEAGE_ID),
            "Title": "最小桥接测试",
            "FileName": "bridge-test.scen",
            "CurrentTime": "2026-07-15T12:00:00Z",
            "StartTime": "2026-07-15T10:00:00Z",
            "StartTimeNum": 1_768_471_200,
            "Duration": "02:00:00",
            "DurationNum": 7_200,
            "Complexity": 2,
            "Difficulty": 3,
            "ScenSetting": "Test",
            "DBUsed": "DB3000",
            "SaveVersion": "1",
            "HasStarted": True,
            "PlayerSide": player_side,
            "TimeCompression": 1,
            "CampaignScore": 5,
        },
    )


def _cmo_fixture(lua: LuaRuntime) -> _CmoFixture:
    rp1 = _lua_table(
        lua,
        {"guid": "RP-1", "name": "CAP North", "latitude": 10.0, "longitude": 20.0},
    )
    rp2 = _lua_table(
        lua,
        {"guid": "RP-2", "name": "CAP South", "latitude": 11.0, "longitude": 21.0},
    )
    rp3 = _lua_table(
        lua,
        {"guid": "RP-3", "name": "CAP East", "latitude": 12.0, "longitude": 22.0},
    )
    rp4 = _lua_table(
        lua,
        {"guid": "RP-4", "name": "CAP West", "latitude": 13.0, "longitude": 23.0},
    )
    patrol_details = _lua_table(
        lua,
        {
            "subtype": "aaw",
            "PatrolZone": lua.table_from({0: rp2, 1: "CAP North"}),
            "ProsecutionZone": lua.table_from({0: "CAP West", 1: rp3}),
            "FlightSize": 2,
            "UseFlightSize": True,
            "MinAircraftReq": "Flight_x2",
            "OnStation": 1,
            "OneThirdRule": False,
            "LoopType": _lua_table(lua, {"value__": 2}),
        },
    )
    patrol = _lua_table(
        lua,
        {
            "guid": "MISSION-BLUE-1",
            "name": "Blue CAP",
            "side": "Blue",
            "type": 2,
            "typeS": "Patrol",
            "isactive": True,
            "starttime": "2026-07-15T10:15:00Z",
            "unitlist": lua.table_from(["UNIT-BLUE-1"]),
            "targetlist": lua.table_from(["CONTACT-BLUE-1"]),
            "patrolmission": patrol_details,
        },
    )
    strike = _lua_table(
        lua,
        {
            "guid": "MISSION-BLUE-0",
            "name": "Blue Strike",
            "side": "Blue",
            "type": 1,
            "typeS": "Strike",
            "isactive": False,
            "unitlist": lua.table_from([]),
            "targetlist": lua.table_from(["CONTACT-BLUE-0"]),
            "strikemission": _lua_table(
                lua,
                {
                    "subtype": "land",
                    "StrikeFlightSize": 4,
                    "StrikeUseFlightSize": True,
                    "StrikeMinAircraftReq": 2,
                    "StrikeOneTimeOnly": False,
                    "StrikePreplan": True,
                },
            ),
        },
    )
    cargo_mission = _lua_table(
        lua,
        {
            "guid": "MISSION-BLUE-2",
            "name": "Blue Cargo",
            "side": "Blue",
            "type": 8,
            "typeS": "Cargo",
            "isactive": False,
            "unitlist": lua.table_from([]),
            "targetlist": lua.table_from([]),
            "assignedCargo": lua.table_from(
                [
                    _lua_table(
                        lua,
                        {
                            "type": _lua_table(lua, {"value__": 2}),
                            "dbid": 801,
                            "GUID": "CARGO-BLUE-1",
                            "quantity": 1,
                        },
                    )
                ]
            ),
            "cargomission": _lua_table(
                lua,
                {
                    "subtype": "transfer",
                    "DestinationUnitID": "UNIT-BLUE-0",
                    "MoveAllCargo": False,
                    "AllowGroundUnitSelfDeliveryFromCargo": False,
                    "OneThirdRule": False,
                    "UseGroupSize": True,
                    "GroupSize": 1,
                },
            ),
        },
    )

    aircraft = _lua_table(
        lua,
        {
            "guid": "UNIT-BLUE-1",
            "dbid": 101,
            "name": "Alpha One",
            "side": "Blue",
            "type": "Aircraft",
            "subtype": "Fighter",
            "category": "Fighter",
            "classname": "Test Fighter",
            "latitude": 10.5,
            "longitude": 20.5,
            "altitude": 3000,
            "speed": 250,
            "heading": 90,
            "throttle": "Cruise",
            "forceSpeed": False,
            "desiredHeading": 95,
            "moveto": False,
            "manualThrottle": "OFF",
            "manualSpeed": "OFF",
            "manualAltitude": "OFF",
            "holdPosition": False,
            "holdFire": False,
            "sprintDrift": False,
            "avoidCavitation": False,
            "obeyEMCON": True,
            "proficiency": "Regular",
            "fuelstate": "Full",
            "weaponstate": "Ready",
            "unitstate": "OnPatrol",
            "condition": "Airborne",
            "condition_v": "Airborne",
            "readytime_v": 0,
            "airbornetime_v": 1_200,
            "IsDestroyed": False,
            "isSinking": False,
            "isOperating": True,
            "mission": patrol,
            "loadoutdbid": 501,
            "damage": _lua_table(
                lua,
                {
                    "DP": 100,
                    "STARTDP": 100,
                    "DP_PERCENT": 100,
                    "DP_PERCENT_NOW": 0,
                    "FIRES": "None",
                    "FLOOD": "None",
                },
            ),
            "fuels": lua.table_from(
                {
                    2001: _lua_table(
                        lua,
                        {"name": "AviationFuel", "current": 5_200, "max": 6_500},
                    )
                }
            ),
            "target": _lua_table(lua, {"guid": "CONTACT-BLUE-1"}),
            "firingAt": lua.table_from({0: "CONTACT-BLUE-1"}),
            "firedOn": lua.table_from({0: "UNIT-RED-1"}),
            "targetedBy": lua.table_from({0: "UNIT-RED-2"}),
            "loadout": _lua_table(
                lua,
                {
                    "dbid": 501,
                    "name": "CAP Loadout",
                    "weapons": lua.table_from(
                        [
                            _lua_table(
                                lua,
                                {
                                    "wpn_guid": "WEAPON-LOAD-1",
                                    "wpn_dbid": 301,
                                    "wpn_name": "Test AAM",
                                    "wpn_type": 2001,
                                    "wpn_current": 4,
                                    "wpn_maxcap": 4,
                                    "wpn_default": 4,
                                },
                            )
                        ]
                    ),
                },
            ),
            "sensors": lua.table_from(
                [
                    _lua_table(
                        lua,
                        {
                            "sensor_guid": "SENSOR-BLUE-1",
                            "sensor_dbid": 401,
                            "sensor_name": "Test AESA",
                            "sensor_maxrange": 180,
                            "sensor_isactive": False,
                            "sensor_status": "Operational",
                            "sensor_role": 2003,
                            "sensor_type": 2001,
                        },
                    )
                ]
            ),
            "mounts": lua.table_from(
                [
                    _lua_table(
                        lua,
                        {
                            "mount_GUID": "MOUNT-BLUE-1",
                            "mount_dbid": 601,
                            "mount_name": "Test Launcher",
                            "mount_status": "Operational",
                            "mount_statusR": "",
                            "mount_damage": "None",
                            "mount_weapons": lua.table_from(
                                [
                                    _lua_table(
                                        lua,
                                        {
                                            "wpn_guid": "MOUNT-WEAPON-1",
                                            "wpn_dbid": 301,
                                            "wpn_name": "Test AAM",
                                            "wpn_type": 2001,
                                            "wpn_current": 2,
                                            "wpn_maxcap": 4,
                                            "wpn_default": 4,
                                        },
                                    )
                                ]
                            ),
                        },
                    )
                ]
            ),
            "magazines": lua.table_from(
                [
                    _lua_table(
                        lua,
                        {
                            "mag_guid": "MAG-BLUE-1",
                            "mag_dbid": 701,
                            "mag_name": "Test Magazine",
                            "mag_capacity": 20,
                            "mag_weapons": lua.table_from(
                                [
                                    _lua_table(
                                        lua,
                                        {
                                            "wpn_guid": "MAG-WEAPON-1",
                                            "wpn_dbid": 301,
                                            "wpn_name": "Test AAM",
                                            "wpn_type": 2001,
                                            "wpn_current": 8,
                                            "wpn_maxcap": 20,
                                            "wpn_default": 12,
                                        },
                                    )
                                ]
                            ),
                        },
                    )
                ]
            ),
            "cargo": lua.table_from(
                [
                    _lua_table(
                        lua,
                        {
                            "GUID": "CARGO-BLUE-1",
                            "type": _lua_table(lua, {"value__": 2}),
                            "storageType": "Internal",
                            "dbid": 801,
                            "name": "Test Cargo",
                            "status": "Operational",
                            "quantity": 1,
                            "area": 4.5,
                            "crew": 3,
                            "mass": 2.5,
                        },
                    )
                ]
            ),
        },
    )
    ship = _lua_table(
        lua,
        {
            "guid": "UNIT-BLUE-0",
            "dbid": 202,
            "name": "Bravo Destroyer",
            "side": "Blue",
            "type": "Ship",
            "category": "SurfaceCombatant",
            "classname": "Test Destroyer",
            "latitude": 11,
            "longitude": 21,
            "altitude": 0,
            "speed": 18,
            "heading": 180,
            "throttle": "Cruise",
            "forceSpeed": False,
            "desiredHeading": 180,
            "moveto": False,
            "manualThrottle": "OFF",
            "manualSpeed": "OFF",
            "manualAltitude": "OFF",
            "holdPosition": False,
            "holdFire": False,
            "sprintDrift": False,
            "avoidCavitation": True,
            "obeyEMCON": True,
            "proficiency": "Veteran",
            "fuelstate": "None",
            "weaponstate": "Ready",
            "unitstate": "Unassigned",
            "isOperating": True,
        },
    )
    submarine = _lua_table(
        lua,
        {
            "guid": "UNIT-BLUE-2",
            "dbid": 303,
            "name": "Charlie Submarine",
            "side": "Blue",
            "type": "Submarine",
            "category": "AttackSubmarine",
            "classname": "Test Submarine",
            "latitude": 9,
            "longitude": 19,
            "altitude": -80,
            "depth": -80,
            "speed": 12,
            "heading": 45,
            "throttle": "Cruise",
            "forceSpeed": False,
            "desiredHeading": 45,
            "moveto": False,
            "manualThrottle": "OFF",
            "manualSpeed": "OFF",
            "manualAltitude": "OFF",
            "holdPosition": False,
            "holdFire": False,
            "sprintDrift": True,
            "avoidCavitation": True,
            "obeyEMCON": True,
            "proficiency": "Veteran",
            "fuelstate": "Full",
            "weaponstate": "Ready",
            "unitstate": "Unassigned",
            "isOperating": True,
        },
    )

    owner_side = _lua_table(lua, {"guid": "SIDE-BLUE", "name": "Blue"})
    air_contact = _lua_table(
        lua,
        {
            "guid": "CONTACT-BLUE-1",
            "name": "Bogey One",
            "fromside": owner_side,
            "type": "Air",
            "type_description": "Aircraft",
            "classificationlevel": "KnownClass",
            "posture": "U",
            "latitude": 12.5,
            "longitude": 22.5,
            "altitude": 5000,
            "speed": 320,
            "heading": 270,
            "actualunitid": "UNIT-RED-1",
            "actualunitdbid": 9001,
            "age": 90,
            "areaofuncertainty": lua.table_from(
                [
                    _lua_table(lua, {"latitude": 12.4, "longitude": 22.4}),
                    _lua_table(lua, {"latitude": 12.6, "longitude": 22.6}),
                ]
            ),
            "detectionBy": _lua_table(
                lua,
                {
                    "Visual": 30,
                    "Infrared": 20,
                    "Radar": 1,
                    "ESM": 4,
                    "SonarActive": None,
                    "SonarPassive": None,
                },
            ),
            "emissions": lua.table_from(
                [
                    _lua_table(
                        lua,
                        {
                            "name": "Test Radar",
                            "age": 2,
                            "solid": "Yes",
                            "sensor_dbid": 1484,
                            "sensor_name": "Test Radar",
                            "sensor_type": 2001,
                            "sensor_role": 2028,
                            "sensor_maxrange": 100,
                        },
                    )
                ]
            ),
            "lastDetections": lua.table_from(
                [
                    _lua_table(
                        lua,
                        {
                            "detector_guid": "UNIT-BLUE-1",
                            "detect_sensor_guid": "SENSOR-BLUE-1",
                            "age": 1,
                            "range": 75.5,
                            "special_mode": "None",
                        },
                    )
                ]
            ),
            "potentialmatches": lua.table_from(
                [
                    _lua_table(
                        lua,
                        {
                            "DBID": 9001,
                            "NAME": "Test Fighter",
                            "CATEGORY": "Fighter",
                            "TYPE": "Aircraft",
                            "SUBTYPE": 2001,
                            "MISSILE_DEFENCE": 2,
                        },
                    )
                ]
            ),
            "BDA": _lua_table(
                lua,
                {"FIRES": "None", "FLOOD": "None", "STRUCTURAL": "Light"},
            ),
            "missile_defence": 2,
            "detectedBySide": owner_side,
            "observer": _lua_table(lua, {"guid": "SIDE-ALLY", "name": "Ally"}),
            "observer_posture": "F",
            "markedAsDecoy": False,
            "FilterOut": False,
            "targetedBy": lua.table_from({0: "UNIT-BLUE-1"}),
            "firedOn": lua.table_from({0: "UNIT-BLUE-0"}),
            "firingAt": lua.table_from({0: "CONTACT-BLUE-0"}),
        },
    )
    surface_contact = _lua_table(
        lua,
        {
            "guid": "CONTACT-BLUE-0",
            "name": "Unknown Surface",
            "fromside": owner_side,
            "type": "Surface",
            "type_description": "Unknown surface contact",
            "classificationlevel": "KnownDomain",
            "posture": "N",
            "actualunitid": "",
            "actualunitdbid": 0,
        },
    )

    unit_refs = lua.table_from(
        [
            _lua_table(lua, {"guid": "UNIT-BLUE-1", "name": "Alpha One"}),
            _lua_table(lua, {"guid": "UNIT-BLUE-0", "name": "Bravo Destroyer"}),
            _lua_table(lua, {"guid": "UNIT-BLUE-2", "name": "Charlie Submarine"}),
        ]
    )
    contact_refs = lua.table_from(
        [
            _lua_table(lua, {"guid": "CONTACT-BLUE-1", "name": "Bogey One"}),
            _lua_table(lua, {"guid": "CONTACT-BLUE-0", "name": "Unknown Surface"}),
        ]
    )
    reference_points = lua.table_from([rp1, rp2, rp3, rp4])
    missions = lua.table_from([patrol, strike, cargo_mission])
    contacts = lua.table_from([air_contact, surface_contact])
    blue = _lua_table(
        lua,
        {
            "guid": "SIDE-BLUE",
            "name": "Blue",
            "awareness": "Omniscient",
            "proficiency": "Regular",
            "iscomputeronly": False,
            "units": unit_refs,
            "contacts": contact_refs,
            "missions": missions,
            "rps": reference_points,
        },
    )
    red = _lua_table(
        lua,
        {
            "guid": "SIDE-RED",
            "name": "Red",
            "awareness": "Normal",
            "proficiency": "Veteran",
            "iscomputeronly": True,
            "units": lua.table_from([]),
            "contacts": lua.table_from([]),
            "missions": lua.table_from([]),
        },
    )
    return _CmoFixture(
        sides=lua.table_from([red, blue]),
        units_by_guid={
            "UNIT-BLUE-0": ship,
            "UNIT-BLUE-1": aircraft,
            "UNIT-BLUE-2": submarine,
        },
        units_by_name={
            "Alpha One": aircraft,
            "Bravo Destroyer": ship,
            "Charlie Submarine": submarine,
        },
        contacts=contacts,
        missions=missions,
        missions_by_guid={
            "MISSION-BLUE-0": strike,
            "MISSION-BLUE-1": patrol,
            "MISSION-BLUE-2": cargo_mission,
        },
        missions_by_name={
            "Blue CAP": patrol,
            "Blue Strike": strike,
            "Blue Cargo": cargo_mission,
        },
        reference_points=reference_points,
        reference_points_by_guid={
            "RP-1": rp1,
            "RP-2": rp2,
            "RP-3": rp3,
            "RP-4": rp4,
        },
    )


def _invocation(
    operation: str,
    public_arguments: dict[str, object],
    activation_id: UUID,
) -> ResolvedInvocation:
    if operation == "bridge.status":
        trusted = {"activation_candidate": activation_id}
    elif operation in {"unit.delete", "mission.delete"}:
        trusted = {"confirmation_proof": "f" * 64}
    else:
        trusted = None
    return OPERATION_REGISTRY.resolve_invocation(
        operation,
        public_arguments,
        trusted,
    )


def _run_lua(
    operation: str,
    public_arguments: dict[str, object],
    *,
    player_side: object = "SIDE-BLUE",
    wire_argument_overrides: dict[str, object] | None = None,
    expect_ok: bool = True,
    repeat_dispatches: int = 1,
    clear_session_cache_between_dispatches: bool = False,
    remove_g_table_before_dispatch: bool = False,
    export_failures_before_success: int = 0,
    zero_export_results_before_success: int = 0,
) -> _LuaRun:
    assert not (
        export_failures_before_success and zero_export_results_before_success
    )
    snapshot = create_runtime_snapshot()
    activation_id = uuid4()
    invocation = _invocation(operation, public_arguments, activation_id)
    status_bootstrap = operation == "bridge.status"
    wire_arguments = cast(
        dict[str, JsonValue],
        invocation.wire_arguments.model_dump(mode="json"),
    )
    if wire_argument_overrides is not None:
        wire_arguments.update(cast(dict[str, JsonValue], wire_argument_overrides))
    body = RequestBody(
        protocol=snapshot.protocol,
        release_id=snapshot.release_id,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        expected_lineage_id=None if status_bootstrap else LINEAGE_ID,
        expected_activation_id=None if status_bootstrap else activation_id,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        operation=operation,
        arguments=wire_arguments,
    )
    body_bytes = canonical_body_bytes(body)
    request_id = uuid4()
    delivery_id = uuid4()
    request_hash = hashlib.sha256(body_bytes).hexdigest()

    lua = LuaRuntime(unpack_returned_tuples=True)
    globals_ = cast(Any, lua.globals())
    scenario = _scenario(lua, player_side=player_side)
    fixture = _cmo_fixture(lua)
    captured: dict[str, object] = {
        "score_sides": [],
        "set_unit_calls": [],
        "assign_mission_calls": [],
        "api_calls": {
            "add_unit": [],
            "delete_unit": [],
            "get_reference_points": [],
            "add_reference_point": [],
            "set_reference_point": [],
            "delete_reference_point": [],
            "add_mission": [],
            "set_mission": [],
            "delete_mission": [],
            "assign_target": [],
            "remove_target": [],
            "get_doctrine": [],
            "set_doctrine": [],
            "set_emcon": [],
            "get_loadout": [],
            "set_loadout": [],
            "refuel_unit": [],
            "attack_contact": [],
            "get_contact": [],
            "set_time_compression": [],
            "get_side_posture": [],
            "weapon_allocation": [],
            "add_weapon_to_magazine": [],
            "add_reloads_to_unit": [],
            "transfer_cargo": [],
            "unload_cargo": [],
            "get_doctrine_wra": [],
            "set_doctrine_wra": [],
            "get_special_action": [],
            "execute_special_action": [],
            "create_mission_flight_plan": [],
            "get_weather": [],
            "set_weather": [],
            "set_scenario_title": [],
            "set_time": [],
            "add_side": [],
            "set_side_options": [],
            "set_side_posture": [],
            "set_score": [],
            "get_events": [],
            "get_event": [],
            "set_event": [],
            "set_trigger": [],
            "set_condition": [],
            "set_action": [],
            "set_event_trigger": [],
            "set_event_condition": [],
            "set_event_action": [],
            "add_special_action": [],
            "set_special_action": [],
            "export_inst": [],
        },
    }

    def capture_call(name: str, value: object) -> None:
        calls = cast(dict[str, list[object]], captured["api_calls"])
        calls[name].append(value)

    def table_dict(value: Any) -> dict[str, object]:
        return {str(key): item for key, item in value.items()}

    def ordered_table_values(value: Any) -> list[object]:
        indexed = sorted(
            ((int(key), item) for key, item in value.items()),
            key=lambda entry: entry[0],
        )
        return [item for _, item in indexed]

    def is_blue_side(value: object) -> bool:
        return value in {"SIDE-BLUE", "Blue"}

    def get_unit(selector: Any) -> Any | None:
        guid = cast(str | None, selector["guid"])
        if guid is not None:
            return fixture.units_by_guid.get(guid)
        side = selector["side"]
        unit_name = cast(str | None, selector["unitname"])
        if not is_blue_side(side) or unit_name is None:
            return None
        return fixture.units_by_name.get(unit_name)

    def get_contacts(side: object) -> Any | None:
        return fixture.contacts if is_blue_side(side) else None

    def get_contact(descriptor: Any) -> Any | None:
        call = table_dict(descriptor)
        capture_call("get_contact", call)
        if not is_blue_side(call.get("side")):
            return None
        guid = str(call.get("guid"))
        for contact in ordered_table_values(fixture.contacts):
            contact_wrapper = cast(Any, contact)
            if str(contact_wrapper["guid"]) == guid:
                return contact_wrapper
        return None

    def get_missions(side: object) -> Any | None:
        return fixture.missions if is_blue_side(side) else None

    def get_mission(side: object, selector: object) -> Any | None:
        if not is_blue_side(side):
            return None
        value = str(selector)
        return fixture.missions_by_guid.get(value) or fixture.missions_by_name.get(value)

    score_state = {"Blue": 42, "SIDE-BLUE": 42}
    def get_score(side: object) -> int:
        score_sides = cast(list[str], captured["score_sides"])
        selector = str(side)
        score_sides.append(selector)
        return score_state.get(selector, 0)

    weather_state = _lua_table(
        lua,
        {
            "temp": -12.5,
            "rainfall": 3.25,
            "undercloud": 0.6,
            "seastate": 4,
        },
    )

    def get_weather() -> Any:
        capture_call("get_weather", ())
        return weather_state

    def set_weather(
        temperature: object,
        rainfall: object,
        undercloud: object,
        sea_state: object,
    ) -> bool:
        call = (
            float(cast(float, temperature)),
            float(cast(float, rainfall)),
            float(cast(float, undercloud)),
            int(cast(int, sea_state)),
        )
        capture_call("set_weather", call)
        (
            weather_state["temp"],
            weather_state["rainfall"],
            weather_state["undercloud"],
            weather_state["seastate"],
        ) = call
        return True

    def set_scenario_title(title: object) -> bool:
        value = str(title)
        capture_call("set_scenario_title", value)
        scenario["Title"] = value
        return True

    def set_time(descriptor: Any) -> Any:
        call = table_dict(descriptor)
        capture_call("set_time", call)
        if call.get("Date") is not None:
            date = str(call["Date"]).replace(".", "-")
            scenario["CurrentTime"] = f"{date}T{call['Time']}"
        if call.get("StartDate") is not None:
            date = str(call["StartDate"]).replace(".", "-")
            scenario["StartTime"] = f"{date}T{call['StartTime']}"
        if call.get("Duration") is not None:
            scenario["Duration"] = str(call["Duration"])
        return scenario

    def side_by_selector(selector: object) -> Any | None:
        value = str(selector)
        for side in ordered_table_values(fixture.sides):
            wrapper = cast(Any, side)
            if value in {str(wrapper["guid"]), str(wrapper["name"])}:
                return wrapper
        return None

    def add_side(descriptor: Any) -> Any:
        call = table_dict(descriptor)
        capture_call("add_side", call)
        name = str(call["side"])
        side = _lua_table(
            lua,
            {
                "guid": f"SIDE-ADDED-{len(fixture.sides) + 1}",
                "name": name,
                "awareness": "Normal",
                "proficiency": "Regular",
                "iscomputeronly": False,
                "units": lua.table_from([]),
                "contacts": lua.table_from([]),
                "missions": lua.table_from([]),
            },
        )
        fixture.sides[len(fixture.sides) + 1] = side
        return side

    def set_side_options(descriptor: Any) -> Any | None:
        call = table_dict(descriptor)
        capture_call("set_side_options", call)
        side = side_by_selector(call.get("side", ""))
        if side is None:
            return None
        field_mapping = {
            "awareness": "awareness",
            "proficiency": "proficiency",
            "computerControlledOnly": "iscomputeronly",
            "AutoTrackCivillians": "AutoTrackCivillians",
            "collectiveResponsibility": "collectiveResponsibility",
        }
        for descriptor_field, wrapper_field in field_mapping.items():
            if call.get(descriptor_field) is not None:
                side[wrapper_field] = call[descriptor_field]
        return side

    posture_state = {("SIDE-BLUE", "SIDE-RED"): "H"}

    def set_side_posture(
        side_a: object,
        side_b: object,
        posture: object,
    ) -> bool:
        call = (str(side_a), str(side_b), str(posture))
        capture_call("set_side_posture", call)
        posture_state[(call[0], call[1])] = call[2]
        return True

    def set_score(side: object, score: object, reason: object) -> int:
        call = (str(side), int(cast(int, score)), str(reason))
        capture_call("set_score", call)
        score_state[call[0]] = call[1]
        return call[1]

    def set_unit(descriptor: Any) -> Any | None:
        call: dict[str, JsonValue] = {}
        for key, value in descriptor.items():
            key_text = str(key)
            if key_text == "course":
                waypoints: list[dict[str, JsonValue]] = []
                for index in range(1, len(value) + 1):
                    waypoint = value[index]
                    waypoints.append(
                        {
                            str(field): cast(JsonValue, field_value)
                            for field, field_value in waypoint.items()
                        }
                    )
                call[key_text] = cast(JsonValue, waypoints)
            elif key_text == "sensors":
                call[key_text] = cast(
                    JsonValue,
                    {
                        str(field): cast(JsonValue, field_value)
                        for field, field_value in value.items()
                    },
                )
            else:
                call[key_text] = cast(JsonValue, value)
        set_unit_calls = cast(list[dict[str, JsonValue]], captured["set_unit_calls"])
        set_unit_calls.append(call)

        guid = cast(str | None, descriptor["guid"])
        if guid is None:
            return None
        unit = fixture.units_by_guid.get(guid)
        if unit is None:
            return None
        new_name = descriptor["newname"]
        if new_name is not None:
            unit["name"] = new_name
        for field in ("speed", "altitude", "depth", "heading", "course", "throttle"):
            value = descriptor[field]
            if value is not None:
                unit[field] = value
        wrapper_mapping = {
            "forceSpeed": "forceSpeed",
            "desiredHeading": "desiredHeading",
            "moveto": "moveto",
            "manualthrottle": "manualThrottle",
            "manualspeed": "manualSpeed",
            "manualaltitude": "manualAltitude",
            "holdPosition": "holdPosition",
            "holdFire": "holdFire",
            "sprintDrift": "sprintDrift",
            "avoidcavitation": "avoidCavitation",
            "obeyEMCON": "obeyEMCON",
        }
        for descriptor_field, wrapper_field in wrapper_mapping.items():
            value = descriptor[descriptor_field]
            if value is not None:
                unit[wrapper_field] = value
        sensor_update = descriptor["sensors"]
        if sensor_update is not None:
            sensor_guid = str(sensor_update["sensor_guid"])
            for sensor in ordered_table_values(unit["sensors"]):
                sensor_wrapper = cast(Any, sensor)
                if str(sensor_wrapper["sensor_guid"]) == sensor_guid:
                    sensor_wrapper["sensor_isactive"] = bool(sensor_update["sensor_isactive"])
                    break
        if descriptor["unassign"] is not None:
            unit["mission"] = None
            unit["isEscort"] = False
        return unit

    def assign_unit_to_mission(
        unit_selector: object,
        mission_selector: object,
        escort: object = False,
    ) -> bool:
        call = (str(unit_selector), str(mission_selector), bool(escort))
        assign_calls = cast(
            list[tuple[str, str, bool]],
            captured["assign_mission_calls"],
        )
        assign_calls.append(call)
        unit = fixture.units_by_guid.get(call[0])
        mission = fixture.missions_by_guid.get(call[1])
        if unit is None or mission is None:
            return False
        unit["mission"] = mission
        unit["isEscort"] = call[2]
        return True

    def add_unit(descriptor: Any) -> Any | None:
        call = table_dict(descriptor)
        capture_call("add_unit", call)
        if not is_blue_side(call.get("side")):
            return None
        guid = f"UNIT-ADDED-{len(fixture.units_by_guid) + 1}"
        base_guid = cast(str | None, call.get("base"))
        if base_guid is not None:
            base = fixture.units_by_guid.get(base_guid)
            if base is None:
                return None
            latitude = float(base["latitude"])
            longitude = float(base["longitude"])
        else:
            latitude = float(cast(float, call["latitude"])) + 0.25
            longitude = float(cast(float, call["longitude"])) - 0.25
        unit = _lua_table(
            lua,
            {
                "guid": guid,
                "dbid": int(cast(int, call["dbid"])),
                "name": str(call["unitname"]),
                "side": "Blue",
                "type": str(call["type"]),
                "category": "Added",
                "classname": "Added Class",
                "latitude": latitude,
                "longitude": longitude,
                "altitude": float(cast(float, call.get("altitude", 0))),
                "speed": 0,
                "heading": 0,
                "isOperating": False,
                "loadoutdbid": call.get("loadoutid"),
            },
        )
        fixture.units_by_guid[guid] = unit
        fixture.units_by_name[str(call["unitname"])] = unit
        return unit

    def get_loadout(descriptor: Any) -> Any | None:
        call = table_dict(descriptor)
        capture_call("get_loadout", call)
        unit = fixture.units_by_guid.get(str(call.get("unitname")))
        return None if unit is None else unit["loadout"]

    def set_loadout(descriptor: Any) -> bool:
        call = table_dict(descriptor)
        capture_call("set_loadout", call)
        unit = fixture.units_by_guid.get(str(call.get("unitname")))
        if unit is None:
            return False
        requested = int(cast(int, call["LoadoutID"]))
        unit["loadoutdbid"] = requested
        loadout = unit["loadout"]
        loadout["dbid"] = requested
        loadout["name"] = f"Loadout {requested}"
        return True

    def refuel_unit(descriptor: Any) -> str:
        call = table_dict(descriptor)
        missions = descriptor["missions"]
        if missions is not None:
            call["missions"] = tuple(str(item) for item in ordered_table_values(missions))
        capture_call("refuel_unit", call)
        return "" if str(call.get("guid")) in fixture.units_by_guid else "unit not found"

    def attack_contact(
        attacker_guid: object,
        contact_guid: object,
        options: Any,
    ) -> bool:
        option_call = table_dict(options)
        capture_call(
            "attack_contact",
            (str(attacker_guid), str(contact_guid), option_call),
        )
        attacker = fixture.units_by_guid.get(str(attacker_guid))
        contact = get_contact(_lua_table(lua, {"side": "SIDE-BLUE", "guid": contact_guid}))
        if attacker is None or contact is None:
            return False
        attacker["target"] = contact
        attacker["firingAt"] = lua.table_from({0: str(contact_guid)})
        contact["targetedBy"] = lua.table_from({0: str(attacker_guid)})
        return True

    def delete_unit(descriptor: Any) -> bool:
        guid = str(descriptor["guid"])
        capture_call("delete_unit", guid)
        return fixture.units_by_guid.pop(guid, None) is not None

    def get_reference_points(descriptor: Any) -> Any:
        call = table_dict(descriptor)
        area = descriptor["area"]
        selectors = [] if area is None else [str(item) for item in ordered_table_values(area)]
        capture_call(
            "get_reference_points",
            {"side": call.get("side"), "area": tuple(selectors)},
        )
        found: dict[int, Any] = {}
        for index, selector in enumerate(selectors):
            reference_point = fixture.reference_points_by_guid.get(selector)
            if reference_point is not None:
                found[index] = reference_point
        return lua.table_from(found)

    def add_reference_point(descriptor: Any) -> Any | None:
        call = table_dict(descriptor)
        capture_call("add_reference_point", call)
        if not is_blue_side(call.get("side")):
            return None
        guid = f"RP-{len(fixture.reference_points_by_guid) + 1}"
        latitude = call.get("latitude")
        longitude = call.get("longitude")
        relative_fields: dict[str, object] = {}
        if latitude is None or longitude is None:
            latitude = 30.0
            longitude = 120.0
            for field in ("relativeto", "relativeto_contact", "relativeto_rp"):
                if call.get(field) is not None:
                    relative_fields[field] = call[field]
            relative_fields["bearing"] = call["bearing"]
            relative_fields["distance"] = call["distance"]
            relative_fields["bearingtype"] = call["bearingtype"]
        reference_point = _lua_table(
            lua,
            {
                "guid": guid,
                "name": str(call["name"]),
                "latitude": float(cast(float, latitude)) + 0.125,
                "longitude": float(cast(float, longitude)) - 0.125,
                **relative_fields,
            },
        )
        fixture.reference_points_by_guid[guid] = reference_point
        fixture.reference_points[len(fixture.reference_points) + 1] = reference_point
        return reference_point

    def set_reference_point(descriptor: Any) -> Any | None:
        call = table_dict(descriptor)
        capture_call("set_reference_point", call)
        reference_point = fixture.reference_points_by_guid.get(str(call.get("guid")))
        if reference_point is None:
            return None
        if call.get("newname") is not None:
            reference_point["name"] = call["newname"]
        if call.get("latitude") is not None:
            reference_point["latitude"] = float(cast(float, call["latitude"]))
        if call.get("longitude") is not None:
            reference_point["longitude"] = float(cast(float, call["longitude"]))
        if call.get("clear") is True:
            for field in (
                "relativeto",
                "relativeto_contact",
                "relativeto_rp",
                "bearing",
                "distance",
                "bearingtype",
            ):
                reference_point[field] = None
        for field in (
            "relativeto",
            "relativeto_contact",
            "relativeto_rp",
            "bearing",
            "distance",
            "bearingtype",
        ):
            if call.get(field) is not None:
                reference_point[field] = call[field]
        return reference_point

    def delete_reference_point(descriptor: Any) -> bool:
        guid = str(descriptor["guid"])
        capture_call("delete_reference_point", guid)
        return fixture.reference_points_by_guid.pop(guid, None) is not None

    def zone_wrapper(zone: Any) -> Any:
        wrapped: dict[int, object] = {}
        for index, raw_guid in enumerate(ordered_table_values(zone)):
            guid = str(raw_guid)
            reference_point = fixture.reference_points_by_guid.get(guid)
            if reference_point is None:
                wrapped[index] = guid
            elif index % 2 == 0:
                wrapped[index] = reference_point
            else:
                wrapped[index] = str(reference_point["name"])
        return lua.table_from(wrapped)

    def add_mission(
        side: object,
        name: object,
        mission_class: object,
        options: Any,
    ) -> Any | None:
        option_call = table_dict(options)
        if options["zone"] is not None:
            option_call["zone"] = tuple(str(item) for item in ordered_table_values(options["zone"]))
        capture_call(
            "add_mission",
            (str(side), str(name), str(mission_class), option_call),
        )
        if not is_blue_side(side):
            return None
        class_name = str(mission_class).lower()
        class_number = {
            "strike": 1,
            "patrol": 2,
            "support": 3,
            "ferry": 4,
            "mining": 5,
            "mineclearing": 6,
            "cargo": 8,
        }[class_name]
        guid = f"MISSION-ADDED-{len(fixture.missions_by_guid) + 1}"
        mission_data: dict[str, object] = {
            "guid": guid,
            "name": str(name),
            "side": "Blue",
            "type": class_number,
            "typeS": class_name.title(),
            "isactive": False,
            "unitlist": lua.table_from([]),
            "targetlist": lua.table_from([]),
        }
        if option_call.get("category") == "taskpool":
            mission_data["category"] = "TaskPool"
            mission_data["packagelist"] = lua.table_from([])
        elif option_call.get("category") == "package":
            mission_data["category"] = "Package"
            mission_data["parentTaskPool"] = option_call["pool"]
        if class_name == "patrol":
            mission_data["patrolmission"] = _lua_table(
                lua,
                {
                    "subtype": f"{option_call['type']}: 2",
                    "PatrolZone": zone_wrapper(options["zone"]),
                },
            )
        elif class_name == "support":
            mission_data["supportmission"] = _lua_table(
                lua,
                {"Zone": zone_wrapper(options["zone"])},
            )
        elif class_name == "strike":
            mission_data["strikemission"] = _lua_table(
                lua,
                {"subtype": f"{option_call['type']}: 1"},
            )
        elif class_name == "ferry":
            mission_data["ferrymission"] = _lua_table(
                lua,
                {"DestinationUnitID": option_call["destination"]},
            )
        elif class_name == "mining":
            mission_data["minemission"] = _lua_table(
                lua,
                {"Zone": zone_wrapper(options["zone"])},
            )
        elif class_name == "mineclearing":
            zone = options["zone"]
            mission_data["mineclearmission"] = _lua_table(
                lua,
                {"Zone": None if zone is None else zone_wrapper(zone)},
            )
        else:
            zone = options["zone"]
            mission_data["assignedCargo"] = lua.table_from([])
            mission_data["cargomission"] = _lua_table(
                lua,
                {
                    "subtype": None,
                    "Zone": None if zone is None else zone_wrapper(zone),
                },
            )
        mission = _lua_table(lua, mission_data)
        fixture.missions_by_guid[guid] = mission
        fixture.missions_by_name[str(name)] = mission
        fixture.missions[len(fixture.missions) + 1] = mission
        return mission

    def set_mission(side: object, mission_guid: object, descriptor: Any) -> Any | None:
        call = table_dict(descriptor)
        for zone_field in ("PatrolZone", "ProsecutionZone", "Zone"):
            if descriptor[zone_field] is not None:
                call[zone_field] = tuple(
                    str(item) for item in ordered_table_values(descriptor[zone_field])
                )
        if descriptor["tankerMissionList"] is not None:
            call["tankerMissionList"] = tuple(
                str(item) for item in ordered_table_values(descriptor["tankerMissionList"])
            )
        capture_call("set_mission", (str(side), str(mission_guid), call))
        mission = get_mission(side, mission_guid)
        if mission is None:
            return None
        for field in ("isactive", "starttime", "endtime"):
            if descriptor[field] is not None:
                mission[field] = descriptor[field]
        mission_type = int(mission["type"])
        details_name = {
            1: "strikemission",
            2: "patrolmission",
            3: "supportmission",
            4: "ferrymission",
            5: "minemission",
            6: "mineclearmission",
            8: "cargomission",
        }[mission_type]
        details = mission[details_name]
        for field in (
            "FlightSize",
            "StrikeFlightSize",
            "UseFlightSize",
            "StrikeUseFlightSize",
            "MinAircraftReq",
            "StrikeMinAircraftReq",
            "OnStation",
            "OneTimeOnly",
            "StrikeOneTimeOnly",
            "StrikePreplan",
            "OneThirdRule",
            "LoopType",
            "ActiveEMCON",
            "CheckOPA",
            "CheckWWR",
            "GroupSize",
            "StrikeGroupSize",
            "UseGroupSize",
            "StrikeUseGroupSize",
            "TransitThrottleAircraft",
            "TransitThrottleShip",
            "TransitThrottleSubmarine",
            "StationThrottleAircraft",
            "StationThrottleShip",
            "StationThrottleSubmarine",
            "AttackThrottleAircraft",
            "AttackThrottleShip",
            "AttackThrottleSubmarine",
            "TransitAltitudeAircraft",
            "StationAltitudeAircraft",
            "AttackAltitudeAircraft",
            "TransitDepthSubmarine",
            "StationDepthSubmarine",
            "AttackDepthSubmarine",
            "StrikeMinimumTrigger",
            "StrikeMax",
            "StrikeAutoPlanner",
            "StrikeMinDistAircraft",
            "StrikeMaxDistAircraft",
            "StrikeMinDistShip",
            "StrikeMaxDistShip",
            "FocusOnStrike",
            "Armingdelay",
            "MinesLaidInSet",
            "MinesLaidInterval",
            "MinesLaidSetInterval",
            "MinesLaidMethod",
            "subtype",
            "DestinationUnitID",
            "MoveAllCargo",
            "AllowGroundUnitSelfDeliveryFromCargo",
            "use_refuel_unrep",
            "tankerUsage",
            "launchMissionWithoutTankersInPlace",
            "TankerFollowsReceivers",
            "KeepOnMissionWithoutTankersInPlace",
            "tankerMinNumber_total",
            "tankerMinNumber_airborne",
            "tankerMinNumber_station",
            "maxReceiversInQueuePerTanker_airborne",
            "fuelQtyToStartLookingForTanker_airborne",
            "tankerMaxDistance_airborne",
            "TankerOneTime",
            "TankerMaxReceivers",
        ):
            if descriptor[field] is not None:
                details[field] = descriptor[field]
        if descriptor["tankerMissionList"] is not None:
            details["tankerMissionList"] = lua.table_from(
                ordered_table_values(descriptor["tankerMissionList"])
            )
        for zone_field in ("PatrolZone", "ProsecutionZone", "Zone"):
            if descriptor[zone_field] is not None:
                details[zone_field] = zone_wrapper(descriptor[zone_field])
        return mission

    def create_mission_flight_plan(
        side: object,
        mission_guid: object,
        options: Any,
    ) -> Any | None:
        option_call = table_dict(options)
        capture_call(
            "create_mission_flight_plan",
            (str(side), str(mission_guid), option_call),
        )
        mission = get_mission(side, mission_guid)
        if mission is None:
            return None
        existing = (
            []
            if mission["flightlist"] is None
            else ordered_table_values(mission["flightlist"])
        )
        flight_number = len(existing) + 1
        flight = _lua_table(
            lua,
            {
                "GUID": f"FLIGHT-{flight_number}",
                "Name": f"Flight {flight_number}",
                "course": lua.table_from(
                    [
                        _lua_table(
                            lua,
                            {
                                "GUID": f"WAYPOINT-{flight_number}-1",
                                "Name": "Ingress",
                                "Description": "Generated ingress",
                                "TypeOf": "TurningPoint",
                                "latitude": 20.5,
                                "longitude": 120.5,
                                "Zulu": "2026-07-16T12:00:00Z",
                                "DesiredSpeed": 450,
                                "DesiredAltitude": 9000,
                                "DesiredAltitudeTF": False,
                                "PresetThrottle": "Cruise",
                            },
                        )
                    ]
                ),
            },
        )
        mission["flightlist"] = lua.table_from([*existing, flight])
        if option_call.get("TAKEOFFTIME") is not None:
            mission["TakeOffTime"] = option_call["TAKEOFFTIME"]
        if option_call.get("TIMEONTARGET") is not None:
            mission["TimeOnTargetStation"] = option_call["TIMEONTARGET"]
        return flight

    def delete_mission(side: object, mission_guid: object) -> bool:
        capture_call("delete_mission", (str(side), str(mission_guid)))
        return fixture.missions_by_guid.pop(str(mission_guid), None) is not None

    def target_values(value: object) -> list[str]:
        if hasattr(value, "items"):
            return [str(item) for item in ordered_table_values(value)]
        return [str(value)]

    def assign_target(value: object, mission_guid: object) -> Any | None:
        values = target_values(value)
        capture_call("assign_target", (tuple(values), str(mission_guid)))
        mission = fixture.missions_by_guid.get(str(mission_guid))
        if mission is None:
            return None
        existing = [str(item) for item in ordered_table_values(mission["targetlist"])]
        for target in values:
            if target not in existing:
                existing.append(target)
        mission["targetlist"] = lua.table_from(existing)
        return lua.table_from(values)

    def remove_target(value: object, mission_guid: object) -> Any | None:
        values = target_values(value)
        capture_call("remove_target", (tuple(values), str(mission_guid)))
        mission = fixture.missions_by_guid.get(str(mission_guid))
        if mission is None:
            return None
        existing = [str(item) for item in ordered_table_values(mission["targetlist"])]
        mission["targetlist"] = lua.table_from(
            [target for target in existing if target not in values]
        )
        return lua.table_from(values)

    def assigned_cargo_values(mission: Any) -> list[Any]:
        return list(ordered_table_values(mission["assignedCargo"]))

    def replace_assigned_cargo(mission: Any, values: list[Any]) -> Any:
        mission["assignedCargo"] = lua.table_from(values)
        return mission["assignedCargo"]

    def add_assigned_cargo(
        mission: Any,
        object_type: object,
        dbid: object,
        cargo_guid: object,
        *_unused: object,
    ) -> Any:
        values = assigned_cargo_values(mission)
        values.append(
            _lua_table(
                lua,
                {
                    "type": int(cast(int, object_type)),
                    "dbid": int(cast(int, dbid)),
                    "GUID": str(cargo_guid),
                    "quantity": 1,
                },
            )
        )
        return replace_assigned_cargo(mission, values)

    def remove_assigned_cargo(
        mission: Any,
        object_type: object,
        dbid: object,
        cargo_guid: object,
        *_unused: object,
    ) -> Any:
        values = [
            item
            for item in assigned_cargo_values(mission)
            if not (
                int(item["type"]) == int(cast(int, object_type))
                and int(item["dbid"]) == int(cast(int, dbid))
                and str(item["GUID"]) == str(cargo_guid)
            )
        ]
        return replace_assigned_cargo(mission, values)

    def add_assigned_cargo_mount(
        mission: Any,
        dbid: object,
        quantity: object,
        *_unused: object,
    ) -> Any:
        values = assigned_cargo_values(mission)
        values.append(
            _lua_table(
                lua,
                {
                    "type": 1,
                    "dbid": int(cast(int, dbid)),
                    "GUID": "",
                    "quantity": int(cast(int, quantity)),
                },
            )
        )
        return replace_assigned_cargo(mission, values)

    def remove_assigned_cargo_mount(
        mission: Any,
        dbid: object,
        quantity: object,
        *_unused: object,
    ) -> Any:
        remaining = int(cast(int, quantity))
        values: list[Any] = []
        for item in assigned_cargo_values(mission):
            wrapper = item
            if int(wrapper["dbid"]) == int(cast(int, dbid)) and remaining > 0:
                current = int(wrapper["quantity"])
                removed = min(current, remaining)
                current -= removed
                remaining -= removed
                if current > 0:
                    wrapper["quantity"] = current
                    values.append(wrapper)
            else:
                values.append(wrapper)
        return replace_assigned_cargo(mission, values)

    cargo_fixture = fixture.missions_by_guid["MISSION-BLUE-2"]
    cargo_fixture["addAssignedCargo"] = add_assigned_cargo
    cargo_fixture["removeAssignedCargo"] = remove_assigned_cargo
    cargo_fixture["addAssignedCargoMount"] = add_assigned_cargo_mount
    cargo_fixture["removeAssignedCargoMount"] = remove_assigned_cargo_mount

    doctrine_states = {
        target: _lua_table(
            lua,
            {
                "weapon_control_status_air": 0,
                "weapon_control_status_surface": 1,
                "weapon_control_status_subsurface": 2,
                "weapon_control_status_land": "Free: 0",
                "use_nuclear_weapons": False,
                "use_refuel_unrep": "Never: 1",
                "engage_opportunity_targets": True,
                "automatic_evasion": True,
                "ignore_plotted_course": False,
                "ignore_emcon_while_under_attack": True,
                "maintain_standoff": False,
                "use_sams_in_anti_surface_mode": True,
                "engaging_ambiguous_targets": 1,
                "fuel_state_planned": "Joker",
                "fuel_state_rtb": 2,
                "weapon_state_planned": "Winchester",
                "weapon_state_rtb": 1,
                "withdraw_on_attack": 2,
                "withdraw_on_damage": 1,
                "withdraw_on_defence": 3,
                "withdraw_on_fuel": 2,
                "bvr_logic": 1,
                "dipping_sonar": 0,
                "use_aip": 2,
                "recharge_on_attack": 30,
                "recharge_on_patrol": 50,
                "emcon": _lua_table(
                    lua,
                    {"radar": 0, "sonar": 1, "oecm": 0},
                ),
            },
        )
        for target in ("SIDE-BLUE", "UNIT-BLUE-1", "MISSION-BLUE-1")
    }

    def doctrine_target(selector: Any) -> str:
        return str(selector["mission"] or selector["guid"] or selector["side"])

    def get_doctrine(selector: Any) -> Any | None:
        target = doctrine_target(selector)
        capture_call(
            "get_doctrine",
            {"target": target, "actual": bool(selector["actual"])},
        )
        return doctrine_states.get(target)

    def set_doctrine(selector: Any, updates: Any) -> Any | None:
        target = doctrine_target(selector)
        call = table_dict(updates)
        capture_call("set_doctrine", (target, call))
        doctrine = doctrine_states.get(target)
        if doctrine is None:
            return None
        wcs = {"Free": 0, "Tight": 1, "Hold": 2}
        for field, value in call.items():
            if value == "inherit":
                doctrine[field] = None
            elif field.startswith("weapon_control_status_"):
                doctrine[field] = wcs[str(value)]
            else:
                doctrine[field] = value
        return doctrine

    def set_emcon(scope: object, target: object, grammar: object) -> bool:
        capture_call("set_emcon", (str(scope), str(target), str(grammar)))
        doctrine = doctrine_states.get(str(target))
        if doctrine is None:
            return False
        clauses = str(grammar).split(";")
        inherit = clauses[0] == "Inherit"
        clauses = clauses[1:] if inherit else clauses
        emcon = doctrine["emcon"]
        for clause in clauses:
            transmitter, status = clause.split("=", maxsplit=1)
            emcon[transmitter.lower()] = 1 if status == "Active" else 0
        return True

    def set_time_compression(code: object) -> None:
        numeric = int(cast(int, code))
        capture_call("set_time_compression", numeric)
        scenario["TimeCompression"] = {0: 1, 1: 2, 2: 5, 3: 15, 4: 30, 5: 150}[numeric]

    def get_side_posture(side_a: object, side_b: object) -> str:
        call = (str(side_a), str(side_b))
        capture_call("get_side_posture", call)
        return posture_state.get(call, "N")

    def weapon_allocation(
        attacker_guid: object,
        contact_guid: object,
        side_guid: object,
    ) -> Any:
        capture_call(
            "weapon_allocation",
            (
                None if attacker_guid is None else str(attacker_guid),
                str(contact_guid),
                str(side_guid),
            ),
        )
        return lua.table_from(
            [
                _lua_table(
                    lua,
                    {
                        "shooter": "UNIT-BLUE-1",
                        "qtyAssigned": 2,
                        "weapon": 301,
                        "weaponName": "Test AAM",
                        "target": str(contact_guid),
                        "qtyFired": 1,
                    },
                )
            ]
        )

    def adjust_weapon_record(
        weapons: Any,
        weapon_dbid: int,
        descriptor: dict[str, object],
    ) -> int:
        for item in ordered_table_values(weapons):
            weapon = cast(Any, item)
            if int(weapon["wpn_dbid"]) != weapon_dbid:
                continue
            current = int(weapon["wpn_current"])
            if descriptor.get("fillout") is True:
                current = int(
                    cast(
                        int | str | float,
                        descriptor.get("maxcap") or weapon["wpn_maxcap"],
                    )
                )
            elif descriptor.get("remove") is True:
                current = max(0, current - int(cast(int, descriptor["number"])))
            else:
                current += int(cast(int, descriptor["number"]))
            weapon["wpn_current"] = current
            return current
        return 0

    def add_weapon_to_magazine(descriptor: Any) -> int:
        call = table_dict(descriptor)
        capture_call("add_weapon_to_magazine", call)
        unit = fixture.units_by_guid.get(str(call.get("guid")))
        if unit is None:
            return 0
        for item in ordered_table_values(unit["magazines"]):
            magazine = cast(Any, item)
            if str(magazine["mag_guid"]) == str(call.get("mag_guid")):
                return adjust_weapon_record(
                    magazine["mag_weapons"],
                    int(cast(int, call["wpn_dbid"])),
                    call,
                )
        return 0

    def add_reloads_to_unit(descriptor: Any) -> int:
        call = table_dict(descriptor)
        capture_call("add_reloads_to_unit", call)
        unit = fixture.units_by_guid.get(str(call.get("guid")))
        if unit is None:
            return 0
        for item in ordered_table_values(unit["mounts"]):
            mount = cast(Any, item)
            if str(mount["mount_GUID"]) == str(call.get("mount_guid")):
                return adjust_weapon_record(
                    mount["mount_weapons"],
                    int(cast(int, call["wpn_dbid"])),
                    call,
                )
        return 0

    def transfer_cargo(
        from_unit: object,
        to_unit: object,
        cargo_list: Any,
    ) -> bool:
        normalized: list[object] = []
        for item in ordered_table_values(cargo_list):
            if hasattr(item, "items"):
                normalized.append(tuple(ordered_table_values(item)))
            else:
                normalized.append(str(item))
        capture_call(
            "transfer_cargo",
            (str(from_unit), str(to_unit), tuple(normalized)),
        )
        return True

    def unload_cargo(unit_guid: object) -> bool:
        capture_call("unload_cargo", str(unit_guid))
        return True

    wra_state = _lua_table(
        lua,
        {
            "level": "Unit",
            "target_type": _lua_table(lua, {"value__": 2001}),
            "WRA_301": _lua_table(
                lua,
                {
                    "weapon_dbid": 301,
                    "weapon_name": "Test AAM",
                    "qty_salvo": 2,
                    "shooter_salvo": 1,
                    "firing_range": "75ofmax",
                    "self_defence": "Max",
                },
            ),
        },
    )

    def get_doctrine_wra(selector: Any) -> Any:
        capture_call("get_doctrine_wra", table_dict(selector))
        if selector["target_type"] == "NoMatch":
            return None
        return wra_state

    def set_doctrine_wra(selector: Any, settings: Any) -> Any:
        call = table_dict(selector)
        values = tuple(ordered_table_values(settings))
        capture_call("set_doctrine_wra", (call, values))
        entry = wra_state["WRA_301"]
        (
            entry["qty_salvo"],
            entry["shooter_salvo"],
            entry["firing_range"],
            entry["self_defence"],
        ) = values
        return wra_state

    event_components: dict[str, dict[str, Any]] = {
        "trigger": {
            "TRIGGER-BASE": _lua_table(
                lua,
                {
                    "guid": "TRIGGER-BASE",
                    "description": "Existing Trigger",
                    "type": "Time",
                },
            )
        },
        "condition": {
            "CONDITION-BASE": _lua_table(
                lua,
                {
                    "guid": "CONDITION-BASE",
                    "description": "Existing Condition",
                    "type": "LuaScript",
                    "ScriptText": "return true\r\n",
                },
            )
        },
        "action": {
            "ACTION-COMPONENT-BASE": _lua_table(
                lua,
                {
                    "guid": "ACTION-COMPONENT-BASE",
                    "description": "Existing Action",
                    "type": "Message",
                },
            )
        },
    }
    events: dict[str, Any] = {
        "EVENT-BASE": _lua_table(
            lua,
            {
                "guid": "EVENT-BASE",
                "description": "Existing Event",
                "isActive": False,
                "isShown": False,
                "IsRepeatable": False,
                "Probability": 100,
                "triggers": lua.table_from([]),
                "conditions": lua.table_from([]),
                "actions": lua.table_from([]),
            },
        )
    }

    def find_wrapper(items: dict[str, Any], selector: object) -> Any | None:
        value = str(selector)
        direct = items.get(value)
        if direct is not None:
            return direct
        for wrapper in items.values():
            if value in {
                str(wrapper["guid"]),
                str(wrapper["name"]),
                str(wrapper["description"]),
            }:
                return wrapper
        return None

    def get_events(level: object) -> Any:
        numeric_level = int(cast(int, level))
        capture_call("get_events", numeric_level)
        return lua.table_from(list(events.values()))

    def get_event(selector: object, level: object) -> Any | None:
        call = (str(selector), int(cast(int, level)))
        capture_call("get_event", call)
        return find_wrapper(events, call[0])

    def set_event(selector: object, descriptor: Any) -> Any | None:
        call = table_dict(descriptor)
        selector_text = str(selector)
        capture_call("set_event", (selector_text, call))
        mode = str(call["mode"])
        if mode == "add":
            event = _lua_table(
                lua,
                {
                    "guid": f"EVENT-ADDED-{len(events) + 1}",
                    "name": selector_text,
                    "description": selector_text,
                    "isActive": bool(call.get("isActive", False)),
                    "isShown": bool(call.get("isShown", False)),
                    "IsRepeatable": bool(call.get("IsRepeatable", False)),
                    "Probability": int(cast(int, call.get("Probability", 100))),
                    "triggers": lua.table_from([]),
                    "conditions": lua.table_from([]),
                    "actions": lua.table_from([]),
                },
            )
            events[str(event["guid"])] = event
            return event
        event = find_wrapper(events, selector_text)
        if event is None:
            return None
        if mode == "remove":
            events.pop(str(event["guid"]), None)
            return event
        if call.get("newname") is not None:
            event["name"] = call["newname"]
            event["description"] = call["newname"]
        for field in ("isActive", "isShown", "IsRepeatable", "Probability"):
            if call.get(field) is not None:
                event[field] = call[field]
        return event

    def set_event_component(kind: str, descriptor: Any) -> Any | None:
        call = table_dict(descriptor)
        capture_call(f"set_{kind}", call)
        store = event_components[kind]
        selector = str(call["description"])
        mode = str(call["mode"])
        if mode == "add":
            prefix = {
                "trigger": "TRIGGER",
                "condition": "CONDITION",
                "action": "ACTION-COMPONENT",
            }[kind]
            component_data: dict[str, object] = {
                "guid": f"{prefix}-ADDED-{len(store) + 1}",
                "name": selector,
                "description": selector,
                "type": str(call["type"]),
            }
            for key, value in call.items():
                if key not in {"mode", "description", "type", "newname"}:
                    component_data[key] = value
            component = _lua_table(lua, component_data)
            store[str(component["guid"])] = component
            return component
        component = find_wrapper(store, selector)
        if component is None:
            return None
        if mode == "remove":
            store.pop(str(component["guid"]), None)
            return component
        if mode == "update":
            if call.get("newname") is not None:
                component["name"] = call["newname"]
                component["description"] = call["newname"]
            for key, value in call.items():
                if key not in {"mode", "description", "newname"}:
                    component[key] = value
        return component

    def set_trigger(descriptor: Any) -> Any | None:
        return set_event_component("trigger", descriptor)

    def set_condition(descriptor: Any) -> Any | None:
        return set_event_component("condition", descriptor)

    def set_action(descriptor: Any) -> Any | None:
        return set_event_component("action", descriptor)

    def set_event_component_link(
        kind: str,
        event_selector: object,
        descriptor: Any,
    ) -> Any | None:
        call = table_dict(descriptor)
        event_selector_text = str(event_selector)
        capture_call(f"set_event_{kind}", (event_selector_text, call))
        event = find_wrapper(events, event_selector_text)
        component = find_wrapper(event_components[kind], call["description"])
        if event is None or component is None:
            return None
        event_field = f"{kind}s"
        linked = ordered_table_values(event[event_field])
        mode = str(call["mode"])
        if mode == "add":
            if component not in linked:
                linked.append(component)
        elif mode == "remove":
            linked = [item for item in linked if item is not component]
        elif mode == "replace":
            replacement = find_wrapper(
                event_components[kind],
                call.get("ReplacedBy", ""),
            )
            if replacement is None:
                return None
            linked = [
                replacement if item is component else item
                for item in linked
            ]
        event[event_field] = lua.table_from(linked)
        return event

    def set_event_trigger(event_selector: object, descriptor: Any) -> Any | None:
        return set_event_component_link("trigger", event_selector, descriptor)

    def set_event_condition(event_selector: object, descriptor: Any) -> Any | None:
        return set_event_component_link("condition", event_selector, descriptor)

    def set_event_action(event_selector: object, descriptor: Any) -> Any | None:
        return set_event_component_link("action", event_selector, descriptor)

    special_actions = {
        "ACTION-BLUE-1": _lua_table(
            lua,
            {
                "guid": "ACTION-BLUE-1",
                "name": "Emergency Reinforcement",
                "description": "Deploy reserve aircraft",
                "isActive": True,
                "IsRepeatable": False,
                "ScriptText": "ScenEdit_AddUnit(...)",
                "side": "SIDE-BLUE",
            },
        )
    }

    def find_special_action(selector: object) -> Any | None:
        return find_wrapper(special_actions, selector)

    def get_special_action(descriptor: Any) -> Any | None:
        call = table_dict(descriptor)
        capture_call("get_special_action", call)
        if call.get("side") is not None and not is_blue_side(call.get("side")):
            return None
        if call.get("mode") == "list":
            return lua.table_from(list(special_actions.values()))
        return find_special_action(call.get("ActionNameOrID", ""))

    def add_special_action(descriptor: Any) -> Any:
        call = table_dict(descriptor)
        capture_call("add_special_action", call)
        action = _lua_table(
            lua,
            {
                "guid": f"ACTION-BLUE-{len(special_actions) + 1}",
                "name": str(call["ActionNameOrID"]),
                "description": str(call.get("description", "")),
                "isActive": bool(call.get("IsActive", False)),
                "IsRepeatable": bool(call.get("IsRepeatable", False)),
                "ScriptText": str(call.get("ScriptText", "")),
                "side": str(call["side"]),
            },
        )
        special_actions[str(action["guid"])] = action
        return action

    def set_special_action(descriptor: Any) -> Any | None:
        call = table_dict(descriptor)
        capture_call("set_special_action", call)
        action = find_special_action(call["ActionNameOrID"])
        if action is None:
            return None
        if call.get("mode") == "remove":
            special_actions.pop(str(action["guid"]), None)
            return action
        field_mapping = {
            "newname": "name",
            "description": "description",
            "isActive": "isActive",
            "IsRepeatable": "IsRepeatable",
            "ScriptText": "ScriptText",
        }
        for descriptor_field, wrapper_field in field_mapping.items():
            if call.get(descriptor_field) is not None:
                action[wrapper_field] = call[descriptor_field]
        return action

    def execute_special_action(action_guid: object) -> str | None:
        guid = str(action_guid)
        capture_call("execute_special_action", guid)
        return "Ok" if guid in special_actions else None

    globals_.VP_GetScenario = lambda: scenario
    globals_.VP_SetTimeCompression = set_time_compression
    globals_.VP_GetSides = lambda: fixture.sides
    globals_.GetBuildNumber = lambda: "Command: Modern Operations Build 1868.4"
    globals_.ScenEdit_CurrentTime = lambda: 1_768_478_400
    globals_.ScenEdit_GetUnit = get_unit
    globals_.ScenEdit_GetContacts = get_contacts
    globals_.ScenEdit_GetContact = get_contact
    globals_.ScenEdit_GetSidePosture = get_side_posture
    globals_.ScenEdit_WeaponAllocation = weapon_allocation
    globals_.ScenEdit_GetMissions = get_missions
    globals_.ScenEdit_GetMission = get_mission
    globals_.ScenEdit_GetScore = get_score
    globals_.ScenEdit_GetWeather = get_weather
    globals_.ScenEdit_SetWeather = set_weather
    globals_.SetScenarioTitle = set_scenario_title
    globals_.ScenEdit_SetTime = set_time
    globals_.ScenEdit_AddSide = add_side
    globals_.ScenEdit_SetSideOptions = set_side_options
    globals_.ScenEdit_SetSidePosture = set_side_posture
    globals_.ScenEdit_SetScore = set_score
    globals_.ScenEdit_SetUnit = set_unit
    globals_.ScenEdit_AddUnit = add_unit
    globals_.ScenEdit_GetLoadout = get_loadout
    globals_.ScenEdit_SetLoadout = set_loadout
    globals_.ScenEdit_AddWeaponToUnitMagazine = add_weapon_to_magazine
    globals_.ScenEdit_AddReloadsToUnit = add_reloads_to_unit
    globals_.ScenEdit_TransferCargo = transfer_cargo
    globals_.ScenEdit_UnloadCargo = unload_cargo
    globals_.ScenEdit_RefuelUnit = refuel_unit
    globals_.ScenEdit_AttackContact = attack_contact
    globals_.ScenEdit_DeleteUnit = delete_unit
    globals_.ScenEdit_AssignUnitToMission = assign_unit_to_mission
    globals_.ScenEdit_GetReferencePoints = get_reference_points
    globals_.ScenEdit_AddReferencePoint = add_reference_point
    globals_.ScenEdit_SetReferencePoint = set_reference_point
    globals_.ScenEdit_DeleteReferencePoint = delete_reference_point
    globals_.ScenEdit_AddMission = add_mission
    globals_.ScenEdit_SetMission = set_mission
    globals_.ScenEdit_CreateMissionFlightPlan = create_mission_flight_plan
    globals_.ScenEdit_DeleteMission = delete_mission
    globals_.ScenEdit_AssignUnitAsTarget = assign_target
    globals_.ScenEdit_RemoveUnitAsTarget = remove_target
    globals_.ScenEdit_GetDoctrine = get_doctrine
    globals_.ScenEdit_SetDoctrine = set_doctrine
    globals_.ScenEdit_GetDoctrineWRA = get_doctrine_wra
    globals_.ScenEdit_SetDoctrineWRA = set_doctrine_wra
    globals_.ScenEdit_SetEMCON = set_emcon
    globals_.ScenEdit_GetEvents = get_events
    globals_.ScenEdit_GetEvent = get_event
    globals_.ScenEdit_SetEvent = set_event
    globals_.ScenEdit_SetTrigger = set_trigger
    globals_.ScenEdit_SetCondition = set_condition
    globals_.ScenEdit_SetAction = set_action
    globals_.ScenEdit_SetEventTrigger = set_event_trigger
    globals_.ScenEdit_SetEventCondition = set_event_condition
    globals_.ScenEdit_SetEventAction = set_event_action
    globals_.ScenEdit_GetSpecialAction = get_special_action
    globals_.ScenEdit_AddSpecialAction = add_special_action
    globals_.ScenEdit_SetSpecialAction = set_special_action
    globals_.ScenEdit_ExecuteSpecialAction = execute_special_action
    export_attempts = 0

    def export_inst(side: object, units: Any, file_data: Any) -> int:
        nonlocal export_attempts
        export_attempts += 1
        capture_call(
            "export_inst",
            (str(file_data["filename"]), str(file_data["comment"])),
        )
        if export_attempts <= export_failures_before_success:
            raise RuntimeError("simulated ExportInst failure")
        if export_attempts <= zero_export_results_before_success:
            return 0
        captured["side"] = side
        captured["units"] = tuple(cast(str, units[index]) for index in range(1, len(units) + 1))
        captured["comment"] = file_data["comment"]
        return 1

    globals_.ScenEdit_ExportInst = export_inst
    globals_.CMO_AGENT_BRIDGE_DELIVERY = lua.table_from(
        {
            "request_id": str(request_id),
            "delivery_id": str(delivery_id),
            "delivery_kind": "request",
            "request_hash": request_hash,
            "runtime_version": snapshot.runtime_version,
            "runtime_tag": snapshot.runtime_tag,
            "runtime_asset_sha256": snapshot.runtime_asset_sha256,
            "release_id": snapshot.release_id,
            "body_json": body_bytes.decode("utf-8"),
        }
    )

    dispatcher = render_dispatcher(snapshot).decode("utf-8")
    if remove_g_table_before_dispatch:
        globals_._G = None
    failed_export_attempts = (
        export_failures_before_success + zero_export_results_before_success
    )
    for index in range(repeat_dispatches):
        if index > 0 and clear_session_cache_between_dispatches:
            globals_.CMO_AGENT_BRIDGE_RESPONSE_CACHE = None
        expected_dispatch = index >= failed_export_attempts
        assert lua.execute(dispatcher) is expected_dispatch
    comments = cast(str, captured["comment"])
    raw_inst = json.dumps(
        {"Comments": comments, "Units": ["ignored export anchor"]},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    expectation = ResponseExpectation(
        request_id=request_id,
        allowed_deliveries=(AllowedDelivery(delivery_id, "request"),),
        request_hash=request_hash,
        expected_lineage_id=None if status_bootstrap else LINEAGE_ID,
        expected_activation_id=None if status_bootstrap else activation_id,
        status_bootstrap=status_bootstrap,
        activation_candidate=activation_id if status_bootstrap else None,
        runtime_snapshot=snapshot,
        invocation=invocation,
    )
    accepted = parse_inst_response(raw_inst, expectation)
    assert accepted.ok is expect_ok
    if accepted.ok:
        assert accepted.result is not None
        payload = accepted.result
    else:
        assert accepted.error is not None
        payload = cast(JsonValue, accepted.error.model_dump(mode="json"))
    return _LuaRun(
        result=payload,
        export_side=cast(str, captured["side"]),
        export_units=cast(tuple[str, ...], captured["units"]),
        score_sides=tuple(cast(list[str], captured["score_sides"])),
        set_unit_calls=tuple(cast(list[dict[str, JsonValue]], captured["set_unit_calls"])),
        assign_mission_calls=tuple(
            cast(list[tuple[str, str, bool]], captured["assign_mission_calls"])
        ),
        api_calls={
            name: tuple(values)
            for name, values in cast(
                dict[str, list[object]],
                captured["api_calls"],
            ).items()
        },
    )


def test_lua_status_round_trip_uses_official_string_build_shape() -> None:
    run = _run_lua("bridge.status", {})
    result = cast(dict[str, JsonValue], run.result)

    assert result["build"] == 1868
    assert result["lineage_id"] == str(LINEAGE_ID)
    assert run.export_side == "SIDE-BLUE"
    assert run.export_units == ("UNIT-BLUE-1",)


def test_lua_scenario_get_round_trip_maps_scenario_wrapper() -> None:
    run = _run_lua("scenario.get", {})
    result = cast(dict[str, JsonValue], run.result)

    assert result["guid"] == str(LINEAGE_ID)
    assert result["title"] == "最小桥接测试"
    assert result["database"] == "DB3000"
    assert result["current_time_seconds"] == 1_768_478_400.0
    assert result["start_time_seconds"] == 1_768_471_200.0
    assert result["duration_seconds"] == 7_200.0
    assert result["setting"] == "Test"
    assert result["player_side_guid"] == "SIDE-BLUE"


@pytest.mark.parametrize("player_side", [None, ""])
def test_lua_scenario_get_missing_player_side_maps_null(player_side: object) -> None:
    run = _run_lua("scenario.get", {}, player_side=player_side)
    result = cast(dict[str, JsonValue], run.result)

    assert result["player_side_guid"] is None


@pytest.mark.parametrize(
    ("arguments", "expected_items", "next_cursor"),
    [
        ({"page_size": 100}, 2, None),
        ({"page_size": 1, "fields": ["guid", "name"]}, 1, "1"),
    ],
)
def test_lua_side_list_round_trip_supports_projection_and_paging(
    arguments: dict[str, object],
    expected_items: int,
    next_cursor: str | None,
) -> None:
    run = _run_lua("side.list", arguments)
    result = cast(dict[str, JsonValue], run.result)
    items = cast(list[dict[str, JsonValue]], result["items"])

    assert len(items) == expected_items
    assert result["next_cursor"] == next_cursor
    if "fields" in arguments:
        assert set(items[0]) == {"guid", "name"}


@pytest.mark.parametrize(
    ("arguments", "expected_guid", "expected_next_cursor", "projected"),
    [
        (
            {
                "side_guid": "SIDE-BLUE",
                "page_size": 1,
                "fields": ["guid", "name", "type"],
            },
            "UNIT-BLUE-0",
            "1",
            True,
        ),
        (
            {
                "side_name": "Blue",
                "unit_type": "Aircraft",
                "name_contains": "Alpha",
            },
            "UNIT-BLUE-1",
            None,
            False,
        ),
    ],
)
def test_lua_unit_list_round_trip_filters_projects_and_pages(
    arguments: dict[str, object],
    expected_guid: str,
    expected_next_cursor: str | None,
    projected: bool,
) -> None:
    run = _run_lua("unit.list", arguments)
    result = cast(dict[str, JsonValue], run.result)
    items = cast(list[dict[str, JsonValue]], result["items"])

    assert [item["guid"] for item in items] == [expected_guid]
    assert result["next_cursor"] == expected_next_cursor
    if projected:
        assert set(items[0]) == {"guid", "name", "type"}
    else:
        assert items[0]["dbid"] == 101
        assert items[0]["class_name"] == "Test Fighter"
        assert items[0]["mission_guid"] == "MISSION-BLUE-1"
        assert items[0]["mission_name"] == "Blue CAP"
        assert items[0]["loadout_dbid"] == 501


@pytest.mark.parametrize(
    "arguments",
    [
        {"unit_guid": "UNIT-BLUE-1"},
        {"side_name": "Blue", "unit_name": "Alpha One"},
    ],
)
def test_lua_unit_get_round_trip_supports_guid_and_name_selectors(
    arguments: dict[str, object],
) -> None:
    run = _run_lua("unit.get", arguments)
    result = cast(dict[str, JsonValue], run.result)

    assert result["guid"] == "UNIT-BLUE-1"
    assert result["name"] == "Alpha One"
    assert result["side_name"] == "Blue"
    assert result["type"] == "Aircraft"
    assert result["operating"] is True
    assert result["mission_guid"] == "MISSION-BLUE-1"
    assert result["mission_name"] == "Blue CAP"


def test_lua_unit_combat_status_returns_damage_fuel_and_engagement_state() -> None:
    run = _run_lua("unit.combat_status.get", {"unit_guid": "UNIT-BLUE-1"})
    result = cast(dict[str, JsonValue], run.result)

    assert result["unit_guid"] == "UNIT-BLUE-1"
    assert result["condition_code"] == "Airborne"
    assert result["ready_time_seconds"] == 0
    assert result["airborne_time_seconds"] == 1_200
    assert result["loadout_dbid"] == 501
    assert result["damage"] == {
        "dp": 100,
        "start_dp": 100,
        "dp_percent": 100,
        "dp_percent_now": 0,
        "fires": "None",
        "flood": "None",
    }
    assert result["fuels"] == [
        {
            "type_code": 2001,
            "name": "AviationFuel",
            "current": 5_200,
            "max": 6_500,
            "percent": 80,
        }
    ]
    assert result["target_contact_guid"] == "CONTACT-BLUE-1"
    assert result["firing_at_contact_guids"] == ["CONTACT-BLUE-1"]
    assert result["fired_on_by_unit_guids"] == ["UNIT-RED-1"]
    assert result["targeted_by_unit_guids"] == ["UNIT-RED-2"]


def test_lua_unit_loadout_get_returns_current_weapon_counts() -> None:
    run = _run_lua("unit.loadout.get", {"unit_guid": "UNIT-BLUE-1"})

    assert run.api_calls["get_loadout"] == ({"unitname": "UNIT-BLUE-1", "LoadoutID": 0},)
    assert run.result == {
        "unit_guid": "UNIT-BLUE-1",
        "loadout_dbid": 501,
        "name": "CAP Loadout",
        "ready_time_seconds": 0,
        "weapons": [
            {
                "guid": "WEAPON-LOAD-1",
                "dbid": 301,
                "name": "Test AAM",
                "type_code": 2001,
                "current": 4,
                "max_capacity": 4,
                "default": 4,
            }
        ],
    }


def test_lua_unit_loadout_set_uses_official_descriptor_and_rereads_actual() -> None:
    run = _run_lua(
        "unit.loadout.set",
        {
            "unit_guid": "UNIT-BLUE-1",
            "loadout_dbid": 777,
            "time_to_ready_minutes": 30,
            "ignore_magazines": True,
            "exclude_optional_weapons": False,
        },
    )

    assert run.api_calls["set_loadout"] == (
        {
            "unitname": "UNIT-BLUE-1",
            "LoadoutID": 777,
            "TimeToReady_Minutes": 30,
            "IgnoreMagazines": True,
            "ExcludeOptionalWeapons": False,
        },
    )
    assert cast(dict[str, JsonValue], run.result)["loadout_dbid"] == 777


@pytest.mark.parametrize(
    ("operation", "field"),
    [("unit.launch", "launch"), ("unit.rtb", "rtb")],
)
def test_lua_unit_async_commands_report_acceptance_without_claiming_completion(
    operation: str,
    field: str,
) -> None:
    run = _run_lua(operation, {"unit_guid": "UNIT-BLUE-1"})

    assert run.set_unit_calls == ({"guid": "UNIT-BLUE-1", field: True},)
    assert run.result == {
        "unit_guid": "UNIT-BLUE-1",
        "command": operation.removeprefix("unit."),
        "accepted": True,
        "operating": True,
        "condition": "Airborne",
        "condition_code": "Airborne",
        "unit_state": "OnPatrol",
    }


def test_lua_unit_refuel_accepts_a_mission_set_and_returns_command_snapshot() -> None:
    run = _run_lua(
        "unit.refuel",
        {
            "unit_guid": "UNIT-BLUE-1",
            "tanker_mission_guids": ["MISSION-BLUE-1", "MISSION-BLUE-0"],
        },
    )

    assert run.api_calls["refuel_unit"] == (
        {
            "guid": "UNIT-BLUE-1",
            "missions": ("MISSION-BLUE-1", "MISSION-BLUE-0"),
        },
    )
    assert cast(dict[str, JsonValue], run.result)["command"] == "refuel"
    assert cast(dict[str, JsonValue], run.result)["accepted"] is True


def test_lua_unit_attack_contact_preserves_contact_guid_and_manual_allocation() -> None:
    run = _run_lua(
        "unit.attack_contact",
        {
            "side_guid": "SIDE-BLUE",
            "attacker_unit_guid": "UNIT-BLUE-1",
            "contact_guid": "CONTACT-BLUE-1",
            "mode": "manual_weapon",
            "mount_dbid": 88,
            "weapon_dbid": 301,
            "quantity": 2,
        },
    )

    assert run.api_calls["attack_contact"] == (
        (
            "UNIT-BLUE-1",
            "CONTACT-BLUE-1",
            {"mode": 1, "mount": 88, "weapon": 301, "qty": 2},
        ),
    )
    assert run.result == {
        "attacker_unit_guid": "UNIT-BLUE-1",
        "contact_guid": "CONTACT-BLUE-1",
        "mode": "manual_weapon",
        "accepted": True,
        "primary_target_contact_guid": "CONTACT-BLUE-1",
        "firing_at_contact_guids": ["CONTACT-BLUE-1"],
        "targeted_by_attacker": True,
    }


def test_lua_unit_attack_contact_rejects_attacker_from_another_side() -> None:
    run = _run_lua(
        "unit.attack_contact",
        {
            "side_guid": "SIDE-RED",
            "attacker_unit_guid": "UNIT-BLUE-1",
            "contact_guid": "CONTACT-BLUE-1",
            "mode": "auto",
        },
        expect_ok=False,
    )

    error = cast(dict[str, JsonValue], run.result)
    assert error["code"] == "CMO_LUA_ERROR"
    details = cast(dict[str, JsonValue], error["details"])
    assert "contact-observing side" in str(details["reason"])
    assert run.api_calls["attack_contact"] == ()


@pytest.mark.parametrize(
    ("arguments", "expected_guid", "expected_next_cursor", "projected"),
    [
        (
            {"side_guid": "SIDE-BLUE", "page_size": 1},
            "CONTACT-BLUE-0",
            "1",
            False,
        ),
        (
            {
                "side_name": "Blue",
                "contact_type": "Air",
                "fields": ["guid", "name", "type"],
            },
            "CONTACT-BLUE-1",
            None,
            True,
        ),
    ],
)
def test_lua_contact_list_round_trip_filters_projects_and_pages(
    arguments: dict[str, object],
    expected_guid: str,
    expected_next_cursor: str | None,
    projected: bool,
) -> None:
    run = _run_lua("contact.list", arguments)
    result = cast(dict[str, JsonValue], run.result)
    items = cast(list[dict[str, JsonValue]], result["items"])

    assert [item["guid"] for item in items] == [expected_guid]
    assert result["next_cursor"] == expected_next_cursor
    if projected:
        assert set(items[0]) == {"guid", "name", "type"}
    else:
        assert items[0]["observer_side_guid"] == "SIDE-BLUE"
        assert items[0]["latitude"] is None
        assert items[0]["actual_unit_guid"] is None
        assert items[0]["actual_unit_dbid"] is None


@pytest.mark.parametrize(
    ("arguments", "expected_guid", "expected_next_cursor", "projected"),
    [
        (
            {"side_name": "Blue", "mission_class": "patrol"},
            "MISSION-BLUE-1",
            None,
            False,
        ),
        (
            {
                "side_guid": "SIDE-BLUE",
                "page_size": 1,
                "fields": ["guid", "name", "mission_class"],
            },
            "MISSION-BLUE-0",
            "1",
            True,
        ),
    ],
)
def test_lua_mission_list_round_trip_filters_projects_and_pages(
    arguments: dict[str, object],
    expected_guid: str,
    expected_next_cursor: str | None,
    projected: bool,
) -> None:
    run = _run_lua("mission.list", arguments)
    result = cast(dict[str, JsonValue], run.result)
    items = cast(list[dict[str, JsonValue]], result["items"])

    assert [item["guid"] for item in items] == [expected_guid]
    assert result["next_cursor"] == expected_next_cursor
    if projected:
        assert set(items[0]) == {"guid", "name", "mission_class"}
    else:
        assert items[0]["mission_class"] == "patrol"
        assert items[0]["mission_class_string"] == "Patrol"
        assert items[0]["assigned_unit_guids"] == ["UNIT-BLUE-1"]
        assert items[0]["target_guids"] == ["CONTACT-BLUE-1"]
        assert items[0]["patrol_type"] == "aaw"
        assert items[0]["reference_point_guids"] == ["RP-2", "RP-1"]
        assert items[0]["prosecution_zone_reference_point_guids"] == ["RP-4", "RP-3"]
        assert items[0]["flight_size"] == 2
        assert items[0]["use_flight_size"] is True
        assert items[0]["minimum_aircraft_required"] == 2
        assert items[0]["on_station"] == 1
        assert items[0]["one_third_rule"] is False
        assert items[0]["loop_type"] == 2


@pytest.mark.parametrize(
    "arguments",
    [
        {"side_guid": "SIDE-BLUE", "mission_guid": "MISSION-BLUE-1"},
        {"side_name": "Blue", "mission_name": "Blue CAP"},
    ],
)
def test_lua_mission_get_round_trip_supports_guid_and_name_selectors(
    arguments: dict[str, object],
) -> None:
    run = _run_lua("mission.get", arguments)
    result = cast(dict[str, JsonValue], run.result)

    assert result["guid"] == "MISSION-BLUE-1"
    assert result["name"] == "Blue CAP"
    assert result["side_name"] == "Blue"
    assert result["mission_class"] == "patrol"
    assert result["patrol_type"] == "aaw"
    assert result["reference_point_guids"] == ["RP-2", "RP-1"]
    assert result["prosecution_zone_reference_point_guids"] == ["RP-4", "RP-3"]
    assert result["loop_type"] == 2


def test_lua_call_get_score_invokes_allowlisted_cmo_function() -> None:
    run = _run_lua(
        "lua.call",
        {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}},
    )
    result = cast(dict[str, JsonValue], run.result)

    assert result == {"side": "Blue", "score": 42}
    assert run.score_sides == ("Blue",)


def test_lua_call_weather_reads_and_writes_negative_temperature() -> None:
    observed = _run_lua(
        "lua.call",
        {"function": "ScenEdit_GetWeather", "arguments": {}},
    )
    assert observed.result == {
        "temperature_c": -12.5,
        "rainfall": 3.25,
        "undercloud_fraction": 0.6,
        "sea_state": 4,
    }
    assert observed.api_calls["get_weather"] == ((),)

    changed = _run_lua(
        "lua.call",
        {
            "function": "ScenEdit_SetWeather",
            "arguments": {
                "temperature_c": -28.75,
                "rainfall": 7.5,
                "undercloud_fraction": 0.85,
                "sea_state": 6,
            },
        },
    )
    assert changed.api_calls["set_weather"] == ((-28.75, 7.5, 0.85, 6),)
    assert changed.result == {
        "temperature_c": -28.75,
        "rainfall": 7.5,
        "undercloud_fraction": 0.85,
        "sea_state": 6,
    }


def test_lua_call_title_and_timeline_mutations_return_scenario_readback() -> None:
    title = _run_lua(
        "lua.call",
        {
            "function": "SetScenarioTitle",
            "arguments": {"title": "Northern Shield"},
        },
    )
    assert title.api_calls["set_scenario_title"] == ("Northern Shield",)
    assert cast(dict[str, JsonValue], title.result)["title"] == "Northern Shield"

    timeline = _run_lua(
        "lua.call",
        {
            "function": "ScenEdit_SetTime",
            "arguments": {
                "current_time": "2026-07-16T13:14:15",
                "start_time": "2026-07-16T10:11:12",
                "duration": "2:03:45",
            },
        },
    )
    assert timeline.api_calls["set_time"] == (
        {
            "DateFormat": "YYYYMMDD",
            "Date": "2026.07.16",
            "Time": "13:14:15",
            "StartDate": "2026.07.16",
            "StartTime": "10:11:12",
            "Duration": "2:03:45",
        },
    )
    result = cast(dict[str, JsonValue], timeline.result)
    assert result["current_time"] == "2026-07-16T13:14:15"
    assert result["start_time"] == "2026-07-16T10:11:12"
    assert result["duration"] == "2:03:45"


def test_lua_call_side_add_returns_new_side_wrapper() -> None:
    run = _run_lua(
        "lua.call",
        {
            "function": "ScenEdit_AddSide",
            "arguments": {"name": "Green"},
        },
    )

    assert run.api_calls["add_side"] == ({"side": "Green"},)
    assert run.result == {
        "guid": "SIDE-ADDED-3",
        "name": "Green",
        "awareness": "Normal",
        "proficiency": "Regular",
        "computer_controlled_only": False,
        "unit_count": 0,
        "contact_count": 0,
        "mission_count": 0,
    }


def test_lua_call_side_options_maps_official_descriptor_and_reads_back() -> None:
    run = _run_lua(
        "lua.call",
        {
            "function": "ScenEdit_SetSideOptions",
            "arguments": {
                "side_guid": "SIDE-BLUE",
                "awareness": "AutoSideID",
                "proficiency": "Ace",
                "auto_track_civilians": True,
                "collective_responsibility": True,
                "computer_controlled_only": True,
            },
        },
    )

    assert run.api_calls["set_side_options"] == (
        {
            "side": "SIDE-BLUE",
            "awareness": "AutoSideID",
            "proficiency": "Ace",
            "AutoTrackCivillians": True,
            "collectiveResponsibility": True,
            "computerControlledOnly": True,
        },
    )
    result = cast(dict[str, JsonValue], run.result)
    assert result["guid"] == "SIDE-BLUE"
    assert result["awareness"] == "AutoSideID"
    assert result["proficiency"] == "Ace"
    assert result["computer_controlled_only"] is True


def test_lua_call_side_posture_and_score_mutations_verify_readback() -> None:
    posture = _run_lua(
        "lua.call",
        {
            "function": "ScenEdit_SetSidePosture",
            "arguments": {
                "side_a_guid": "SIDE-BLUE",
                "side_b_guid": "SIDE-RED",
                "posture": "U",
            },
        },
    )
    assert posture.api_calls["set_side_posture"] == (
        ("SIDE-BLUE", "SIDE-RED", "U"),
    )
    assert posture.result == {
        "side_a_guid": "SIDE-BLUE",
        "side_b_guid": "SIDE-RED",
        "posture": "U",
    }

    score = _run_lua(
        "lua.call",
        {
            "function": "ScenEdit_SetScore",
            "arguments": {
                "side": "Blue",
                "score": -125,
                "reason": "Civilian losses",
            },
        },
    )
    assert score.api_calls["set_score"] == (("Blue", -125, "Civilian losses"),)
    assert score.result == {"side": "Blue", "score": -125}


def test_lua_call_event_create_preserves_editor_flags() -> None:
    run = _run_lua(
        "lua.call",
        {
            "function": "ScenEdit_SetEvent",
            "arguments": {
                "mode": "add",
                "event_id_or_name": "Enemy detected",
                "active": False,
                "shown": True,
                "repeatable": True,
                "probability": 75,
            },
        },
    )

    assert run.api_calls["set_event"] == (
        (
            "Enemy detected",
            {
                "mode": "add",
                "isActive": False,
                "isShown": True,
                "IsRepeatable": True,
                "Probability": 75,
            },
        ),
    )
    result = cast(dict[str, JsonValue], run.result)
    assert result["operation"] == "ScenEdit_SetEvent"
    assert result["accepted"] is True
    event = cast(dict[str, JsonValue], result["data"])
    assert event["guid"] == "EVENT-ADDED-2"
    assert event["description"] == "Enemy detected"
    assert event["isShown"] is True
    assert event["IsRepeatable"] is True
    assert event["Probability"] == 75


def test_lua_call_event_component_normalizes_lua_script_to_crlf() -> None:
    run = _run_lua(
        "lua.call",
        {
            "function": "ScenEdit_SetAction",
            "arguments": {
                "mode": "add",
                "component_id_or_name": "Notify Blue",
                "component_type": "LuaScript",
                "parameters_json": json.dumps(
                    {"ScriptText": "line one\nline two\rline three\r\n"}
                ),
            },
        },
    )
    expected_script = "line one\r\nline two\r\nline three\r\n"

    assert run.api_calls["set_action"] == (
        {
            "mode": "add",
            "description": "Notify Blue",
            "type": "LuaScript",
            "ScriptText": expected_script,
        },
    )
    result = cast(dict[str, JsonValue], run.result)
    component = cast(dict[str, JsonValue], result["data"])
    assert component["ScriptText"] == expected_script


def test_lua_call_event_component_keeps_trusted_envelope_authoritative() -> None:
    run = _run_lua(
        "lua.call",
        {
            "function": "ScenEdit_SetAction",
            "arguments": {
                "mode": "add",
                "component_id_or_name": "Safe action",
                "component_type": "LuaScript",
                "parameters_json": json.dumps(
                    {
                        "mode": "remove",
                        "description": "Bridge action",
                        "id": "ACTION-COMPONENT-BASE",
                        "type": "Points",
                        "ScriptText": "return true",
                    }
                ),
            },
        },
    )

    assert run.api_calls["set_action"] == (
        {
            "mode": "add",
            "description": "Safe action",
            "type": "LuaScript",
            "ScriptText": "return true",
        },
    )


@pytest.mark.parametrize(
    ("function_name", "call_name", "component_guid", "event_field"),
    [
        ("ScenEdit_SetEventTrigger", "set_event_trigger", "TRIGGER-BASE", "triggers"),
        (
            "ScenEdit_SetEventCondition",
            "set_event_condition",
            "CONDITION-BASE",
            "conditions",
        ),
        (
            "ScenEdit_SetEventAction",
            "set_event_action",
            "ACTION-COMPONENT-BASE",
            "actions",
        ),
    ],
)
def test_lua_call_event_component_link_updates_event_state(
    function_name: str,
    call_name: str,
    component_guid: str,
    event_field: str,
) -> None:
    run = _run_lua(
        "lua.call",
        {
            "function": function_name,
            "arguments": {
                "mode": "add",
                "event_id_or_name": "EVENT-BASE",
                "component_id_or_name": component_guid,
            },
        },
    )

    assert run.api_calls[call_name] == (
        (
            "EVENT-BASE",
            {"mode": "add", "description": component_guid},
        ),
    )
    result = cast(dict[str, JsonValue], run.result)
    event = cast(dict[str, JsonValue], result["data"])
    linked = cast(list[dict[str, JsonValue]], event[event_field])
    assert [item["guid"] for item in linked] == [component_guid]


def test_lua_call_special_action_create_normalizes_script_and_reads_back() -> None:
    run = _run_lua(
        "lua.call",
        {
            "function": "ScenEdit_AddSpecialAction",
            "arguments": {
                "side_guid": "SIDE-BLUE",
                "name": "Launch reserve",
                "description": "Launch the reserve package.",
                "active": True,
                "repeatable": False,
                "script_text": "print('launch')\nreturn true\r",
            },
        },
    )
    expected_script = "print('launch')\r\nreturn true\r\n"

    assert run.api_calls["add_special_action"] == (
        {
            "side": "SIDE-BLUE",
            "ActionNameOrID": "Launch reserve",
            "description": "Launch the reserve package.",
            "IsActive": True,
            "IsRepeatable": False,
            "ScriptText": expected_script,
        },
    )
    result = cast(dict[str, JsonValue], run.result)
    action = cast(dict[str, JsonValue], result["data"])
    assert action["name"] == "Launch reserve"
    assert action["isActive"] is True
    assert action["ScriptText"] == expected_script


def test_lua_call_special_action_update_mutates_existing_state() -> None:
    run = _run_lua(
        "lua.call",
        {
            "function": "ScenEdit_SetSpecialAction",
            "arguments": {
                "side_guid": "SIDE-BLUE",
                "action_id_or_name": "ACTION-BLUE-1",
                "new_name": "Emergency Reserve",
                "description": "Deploy the ready reserve.",
                "active": False,
                "repeatable": True,
                "script_text": "return false\nprint('complete')",
            },
        },
    )
    expected_script = "return false\r\nprint('complete')"

    assert run.api_calls["set_special_action"] == (
        {
            "side": "SIDE-BLUE",
            "ActionNameOrID": "ACTION-BLUE-1",
            "newname": "Emergency Reserve",
            "description": "Deploy the ready reserve.",
            "isActive": False,
            "IsRepeatable": True,
            "ScriptText": expected_script,
        },
    )
    result = cast(dict[str, JsonValue], run.result)
    action = cast(dict[str, JsonValue], result["data"])
    assert action["name"] == "Emergency Reserve"
    assert action["description"] == "Deploy the ready reserve."
    assert action["isActive"] is False
    assert action["IsRepeatable"] is True
    assert action["ScriptText"] == expected_script


def test_lua_call_received_envelope_cannot_select_non_allowlisted_function() -> None:
    run = _run_lua(
        "lua.call",
        {"function": "ScenEdit_GetWeather", "arguments": {}},
        wire_argument_overrides={
            "function": "load",
            "arguments": {"source": "return ScenEdit_DeleteUnit('UNIT-BLUE-1')"},
        },
        expect_ok=False,
    )

    error = cast(dict[str, JsonValue], run.result)
    assert error["code"] == "CMO_LUA_ERROR"
    details = cast(dict[str, JsonValue], error["details"])
    assert "lua.call function is not enabled" in str(details["reason"])
    assert run.api_calls["get_weather"] == ()
    assert run.api_calls["delete_unit"] == ()


def test_lua_unit_set_uses_official_descriptor_and_returns_updated_state() -> None:
    run = _run_lua(
        "unit.set",
        {
            "unit_guid": "UNIT-BLUE-1",
            "name": "Alpha Prime",
            "speed": 310,
            "altitude": 4_200,
            "heading": 123,
            "course": [
                {"latitude": 30.5, "longitude": 40.5},
                {"latitude": 31.5, "longitude": 41.5, "altitude": 5_500},
            ],
        },
    )
    result = cast(dict[str, JsonValue], run.result)

    assert run.set_unit_calls == (
        {
            "guid": "UNIT-BLUE-1",
            "newname": "Alpha Prime",
            "speed": 310,
            "altitude": 4_200,
            "heading": 123,
            "course": [
                {"latitude": 30.5, "longitude": 40.5},
                {"latitude": 31.5, "longitude": 41.5, "altitude": 5_500},
            ],
        },
    )
    assert result["unit_guid"] == "UNIT-BLUE-1"
    assert result["name"] == "Alpha Prime"
    assert result["speed"] == 310
    assert result["altitude"] == 4_200
    assert result["heading"] == 123
    assert result["course"] == [
        {"latitude": 30.5, "longitude": 40.5, "altitude": None},
        {"latitude": 31.5, "longitude": 41.5, "altitude": 5_500},
    ]
    assert result["force_speed"] is False
    assert result["obey_emcon"] is True


def test_lua_unit_set_preserves_empty_course_as_an_explicit_clear() -> None:
    run = _run_lua(
        "unit.set",
        {"unit_guid": "UNIT-BLUE-1", "course": []},
    )
    result = cast(dict[str, JsonValue], run.result)

    assert run.set_unit_calls == ({"guid": "UNIT-BLUE-1", "course": []},)
    assert result["unit_guid"] == "UNIT-BLUE-1"
    assert result["name"] == "Alpha One"
    assert result["speed"] == 250
    assert result["altitude"] == 3_000
    assert result["heading"] == 90
    assert result["course"] == []


@pytest.mark.parametrize("escort", [False, True])
def test_lua_unit_assign_mission_uses_positional_api_and_reports_assignment(
    escort: bool,
) -> None:
    run = _run_lua(
        "unit.assign_mission",
        {
            "unit_guid": "UNIT-BLUE-1",
            "mission_guid": "MISSION-BLUE-1",
            "escort": escort,
        },
    )
    result = cast(dict[str, JsonValue], run.result)

    assert run.assign_mission_calls == (("UNIT-BLUE-1", "MISSION-BLUE-1", escort),)
    assert result == {
        "unit_guid": "UNIT-BLUE-1",
        "mission_guid": "MISSION-BLUE-1",
        "escort": escort,
    }


def test_lua_reference_point_list_projects_live_side_wrappers() -> None:
    run = _run_lua(
        "reference_point.list",
        {"side_guid": "SIDE-BLUE", "fields": ["guid", "name", "latitude"]},
    )
    result = cast(dict[str, JsonValue], run.result)
    items = cast(list[dict[str, JsonValue]], result["items"])

    assert items[0] == {"guid": "RP-1", "name": "CAP North", "latitude": 10.0}
    assert [item["guid"] for item in items] == ["RP-1", "RP-2", "RP-3", "RP-4"]


def test_lua_reference_point_add_uses_official_descriptor_and_rereads_actual() -> None:
    run = _run_lua(
        "reference_point.add",
        {
            "side_guid": "SIDE-BLUE",
            "name": "Bridge RP",
            "latitude": 40.0,
            "longitude": 120.0,
        },
    )
    result = cast(dict[str, JsonValue], run.result)

    assert run.api_calls["add_reference_point"] == (
        {
            "side": "SIDE-BLUE",
            "name": "Bridge RP",
            "latitude": 40.0,
            "longitude": 120.0,
        },
    )
    assert result == {
        "guid": "RP-5",
        "name": "Bridge RP",
        "side_guid": "SIDE-BLUE",
        "latitude": 40.125,
        "longitude": 119.875,
        "relative_to_type": None,
        "relative_to_guid": None,
        "relative_bearing_deg": None,
        "relative_distance_nm": None,
        "bearing_type": None,
    }
    assert run.api_calls["get_reference_points"][-1] == {
        "side": "SIDE-BLUE",
        "area": ("RP-5",),
    }


def test_lua_reference_point_update_renames_with_newname_and_rereads() -> None:
    run = _run_lua(
        "reference_point.update",
        {
            "side_guid": "SIDE-BLUE",
            "reference_point_guid": "RP-1",
            "name": "CAP North Updated",
            "latitude": 15.5,
        },
    )
    result = cast(dict[str, JsonValue], run.result)

    assert run.api_calls["set_reference_point"] == (
        {
            "side": "SIDE-BLUE",
            "guid": "RP-1",
            "newname": "CAP North Updated",
            "latitude": 15.5,
        },
    )
    assert result["name"] == "CAP North Updated"
    assert result["latitude"] == 15.5
    assert result["longitude"] == 20.0


def test_lua_relative_reference_point_add_maps_anchor_and_reads_it_back() -> None:
    run = _run_lua(
        "reference_point.add",
        {
            "side_guid": "SIDE-BLUE",
            "name": "Carrier CAP",
            "relative_to_type": "unit",
            "relative_to_guid": "UNIT-BLUE-0",
            "relative_bearing_deg": 135,
            "relative_distance_nm": 45,
            "bearing_type": "rotating",
        },
    )
    result = cast(dict[str, JsonValue], run.result)

    assert run.api_calls["add_reference_point"] == (
        {
            "side": "SIDE-BLUE",
            "name": "Carrier CAP",
            "relativeto": "UNIT-BLUE-0",
            "bearing": 135.0,
            "distance": 45.0,
            "bearingtype": 1,
        },
    )
    assert result["relative_to_type"] == "unit"
    assert result["relative_to_guid"] == "UNIT-BLUE-0"
    assert result["relative_bearing_deg"] == 135.0
    assert result["relative_distance_nm"] == 45.0
    assert result["bearing_type"] == "rotating"


@pytest.mark.parametrize(
    ("location", "expected_descriptor", "expected_position"),
    [
        (
            {"latitude": 30.0, "longitude": 50.0},
            {"latitude": 30.0, "longitude": 50.0},
            (30.25, 49.75),
        ),
        (
            {"base_guid": "UNIT-BLUE-0"},
            {"base": "UNIT-BLUE-0"},
            (11.0, 21.0),
        ),
    ],
)
def test_lua_unit_add_supports_coordinate_or_base_and_rereads_actual(
    location: dict[str, object],
    expected_descriptor: dict[str, object],
    expected_position: tuple[float, float],
) -> None:
    arguments = {
        "side_guid": "SIDE-BLUE",
        "unit_type": "Aircraft",
        "dbid": 9001,
        "name": "Bridge Aircraft",
        "altitude": 2_000,
        "loadout_dbid": 7001,
        **location,
    }
    run = _run_lua("unit.add", arguments)
    result = cast(dict[str, JsonValue], run.result)
    descriptor = cast(dict[str, object], run.api_calls["add_unit"][0])

    assert descriptor == {
        "side": "SIDE-BLUE",
        "type": "Aircraft",
        "dbid": 9001,
        "unitname": "Bridge Aircraft",
        "altitude": 2_000,
        "loadoutid": 7001,
        **expected_descriptor,
    }
    assert (result["latitude"], result["longitude"]) == expected_position
    assert result["unit_guid"] == "UNIT-ADDED-4"


def test_lua_unit_unassign_mission_uses_unassign_and_verifies_empty_assignment() -> None:
    run = _run_lua("unit.unassign_mission", {"unit_guid": "UNIT-BLUE-1"})

    assert run.set_unit_calls == ({"guid": "UNIT-BLUE-1", "unassign": True},)
    assert run.result == {
        "unit_guid": "UNIT-BLUE-1",
        "mission_guid": None,
        "escort": False,
    }


def test_lua_unit_delete_removes_exact_guid_and_returns_deleted_identity() -> None:
    run = _run_lua("unit.delete", {"unit_guid": "UNIT-BLUE-0"})

    assert run.api_calls["delete_unit"] == ("UNIT-BLUE-0",)
    assert run.result == {
        "deleted_guid": "UNIT-BLUE-0",
        "deleted_name": "Bravo Destroyer",
        "object_kind": "unit",
    }


@pytest.mark.parametrize(
    ("operation", "arguments", "delete_call"),
    [
        ("unit.delete", {"unit_guid": "UNIT-BLUE-0"}, "delete_unit"),
        (
            "mission.delete",
            {"side_guid": "SIDE-BLUE", "mission_guid": "MISSION-BLUE-0"},
            "delete_mission",
        ),
    ],
)
@pytest.mark.parametrize("invalid_proof", [None, "F" * 64, "f" * 63, "z" * 64])
def test_lua_delete_rejects_invalid_confirmation_before_calling_cmo(
    operation: str,
    arguments: dict[str, object],
    delete_call: str,
    invalid_proof: object,
) -> None:
    run = _run_lua(
        operation,
        arguments,
        wire_argument_overrides={"confirmation_proof": invalid_proof},
        expect_ok=False,
    )

    error = cast(dict[str, JsonValue], run.result)
    assert error["code"] == "CMO_LUA_ERROR"
    assert run.api_calls[delete_call] == ()


@pytest.mark.parametrize(
    ("details", "expected_class", "expected_subtype", "expected_option"),
    [
        (
            {
                "mission_class": "patrol",
                "patrol_type": "aaw",
                "reference_point_guids": ["RP-3", "RP-1", "RP-2"],
            },
            "Patrol",
            "aaw",
            {"type": "aaw", "zone": ("RP-3", "RP-1", "RP-2")},
        ),
        (
            {
                "mission_class": "support",
                "reference_point_guids": ["RP-4", "RP-2"],
            },
            "Support",
            None,
            {"zone": ("RP-4", "RP-2")},
        ),
        (
            {
                "mission_class": "strike",
                "strike_type": "land",
                "target_guids": ["CONTACT-BLUE-0", "CONTACT-BLUE-1"],
            },
            "Strike",
            "land",
            {"type": "land"},
        ),
        (
            {"mission_class": "ferry", "destination_guid": "UNIT-BLUE-0"},
            "Ferry",
            None,
            {"destination": "UNIT-BLUE-0"},
        ),
    ],
)
def test_lua_mission_create_builds_class_options_and_returns_wrapper_state(
    details: dict[str, object],
    expected_class: str,
    expected_subtype: str | None,
    expected_option: dict[str, object],
) -> None:
    run = _run_lua(
        "mission.create",
        {"side_guid": "SIDE-BLUE", "name": "Bridge Mission", "details": details},
    )
    result = cast(dict[str, JsonValue], run.result)

    assert run.api_calls["add_mission"] == (
        ("SIDE-BLUE", "Bridge Mission", expected_class, expected_option),
    )
    assert result == {
        "mission_guid": "MISSION-ADDED-4",
        "name": "Bridge Mission",
        "side_guid": "SIDE-BLUE",
        "mission_class": expected_class.lower(),
        "subtype": expected_subtype,
        "category": "mission",
        "parent_task_pool_guid": None,
        "active": False,
    }
    assert run.api_calls["set_mission"] == (("SIDE-BLUE", "MISSION-ADDED-4", {"isactive": False}),)
    if details["mission_class"] == "strike":
        assert run.api_calls["assign_target"] == (
            (("CONTACT-BLUE-0", "CONTACT-BLUE-1"), "MISSION-ADDED-4"),
        )


def test_lua_mission_create_allows_an_inactive_strike_without_initial_targets() -> None:
    run = _run_lua(
        "mission.create",
        {
            "side_guid": "SIDE-BLUE",
            "name": "Dynamic Strike",
            "details": {"mission_class": "strike", "strike_type": "land"},
        },
    )

    assert run.api_calls["assign_target"] == ()
    assert cast(dict[str, JsonValue], run.result)["active"] is False


@pytest.mark.parametrize(
    ("category", "parent_guid", "expected_options"),
    [
        ("task_pool", None, {"category": "taskpool", "type": "land"}),
        (
            "package",
            "POOL-1",
            {"category": "package", "pool": "POOL-1", "type": "land"},
        ),
    ],
)
def test_lua_mission_create_maps_task_pool_and_package_categories(
    category: str,
    parent_guid: str | None,
    expected_options: dict[str, object],
) -> None:
    arguments: dict[str, object] = {
        "side_guid": "SIDE-BLUE",
        "name": f"Bridge {category}",
        "category": category,
        "details": {
            "mission_class": "strike",
            "strike_type": "land",
            "target_guids": [],
        },
    }
    if parent_guid is not None:
        arguments["parent_task_pool_guid"] = parent_guid
    run = _run_lua("mission.create", arguments)
    result = cast(dict[str, JsonValue], run.result)

    assert run.api_calls["add_mission"] == (
        ("SIDE-BLUE", f"Bridge {category}", "Strike", expected_options),
    )
    assert result["category"] == category
    assert result["parent_task_pool_guid"] == parent_guid


def test_lua_mission_air_refueling_update_maps_and_reads_back_settings() -> None:
    run = _run_lua(
        "mission.air_refueling.update",
        {
            "side_guid": "SIDE-BLUE",
            "mission_guid": "MISSION-BLUE-1",
            "use_refuel_unrep": "Always_IncludingTankersRefuellingTankers",
            "tanker_usage": "mission",
            "launch_without_tankers_in_place": False,
            "tanker_follows_receivers": True,
            "keep_on_mission_without_tankers_in_place": False,
            "tanker_mission_guids": ["MISSION-BLUE-0"],
            "minimum_tankers_total": 2,
            "minimum_tankers_airborne": 1,
            "minimum_tankers_on_station": 1,
            "max_receivers_in_queue_per_tanker": 3,
            "fuel_percent_to_start_looking": 40,
            "tanker_max_distance_nm": 250,
        },
    )
    result = cast(dict[str, JsonValue], run.result)
    _, _, descriptor = cast(
        tuple[object, object, dict[str, object]],
        run.api_calls["set_mission"][0],
    )

    assert descriptor["tankerUsage"] == "Mission"
    assert descriptor["tankerMissionList"] == ("MISSION-BLUE-0",)
    assert descriptor["tankerMinNumber_total"] == 2
    assert descriptor["fuelQtyToStartLookingForTanker_airborne"] == 40.0
    assert result["mission_guid"] == "MISSION-BLUE-1"
    assert result["tanker_usage"] == "mission"
    assert result["tanker_mission_guids"] == ["MISSION-BLUE-0"]
    assert result["minimum_tankers_total"] == 2
    assert result["tanker_max_distance_nm"] == 250.0


def test_lua_mission_flight_plan_create_uses_tot_and_returns_generated_course() -> None:
    run = _run_lua(
        "mission.flight_plan.create",
        {
            "side_guid": "SIDE-BLUE",
            "mission_guid": "MISSION-BLUE-1",
            "date_on_target": "2026/07/16",
            "time_on_target": "12:30:00",
        },
    )
    result = cast(dict[str, JsonValue], run.result)

    assert run.api_calls["create_mission_flight_plan"] == (
        (
            "SIDE-BLUE",
            "MISSION-BLUE-1",
            {"DATEONTARGET": "2026/07/16", "TIMEONTARGET": "12:30:00"},
        ),
    )
    assert result["created_flight_guids"] == ["FLIGHT-1"]
    assert result["time_on_target"] == "12:30:00"
    flights = cast(list[dict[str, JsonValue]], result["flights"])
    assert flights[0]["guid"] == "FLIGHT-1"
    waypoints = cast(list[dict[str, JsonValue]], flights[0]["waypoints"])
    assert waypoints[0]["name"] == "Ingress"
    assert waypoints[0]["desired_altitude_m"] == 9000.0


def test_lua_mission_delete_removes_exact_guid_and_returns_deleted_identity() -> None:
    run = _run_lua(
        "mission.delete",
        {"side_guid": "SIDE-BLUE", "mission_guid": "MISSION-BLUE-0"},
    )

    assert run.api_calls["delete_mission"] == (("SIDE-BLUE", "MISSION-BLUE-0"),)
    assert run.result == {
        "deleted_guid": "MISSION-BLUE-0",
        "deleted_name": "Blue Strike",
        "object_kind": "mission",
    }


def test_lua_mission_update_preserves_zone_order_and_returns_actual_wrapper() -> None:
    run = _run_lua(
        "mission.update",
        {
            "side_guid": "SIDE-BLUE",
            "mission_guid": "MISSION-BLUE-1",
            "active": False,
            "start_time": "2026-07-15T11:00:00Z",
            "end_time": "2026-07-15T13:00:00Z",
            "flight_size": 3,
            "use_flight_size": True,
            "minimum_aircraft_required": 2,
            "on_station": 1,
            "one_third_rule": True,
            "reference_point_guids": ["RP-3", "RP-1", "RP-2"],
            "prosecution_zone_reference_point_guids": ["RP-4", "RP-2"],
        },
    )
    result = cast(dict[str, JsonValue], run.result)

    assert run.api_calls["set_mission"] == (
        (
            "SIDE-BLUE",
            "MISSION-BLUE-1",
            {
                "isactive": False,
                "starttime": "2026-07-15T11:00:00Z",
                "endtime": "2026-07-15T13:00:00Z",
                "FlightSize": 3,
                "UseFlightSize": True,
                "MinAircraftReq": 2,
                "OnStation": 1,
                "OneThirdRule": True,
                "PatrolZone": ("RP-3", "RP-1", "RP-2"),
                "ProsecutionZone": ("RP-4", "RP-2"),
            },
        ),
    )
    assert result["active"] is False
    assert result["flight_size"] == 3
    assert result["use_flight_size"] is True
    assert result["minimum_aircraft_required"] == 2
    assert result["on_station"] == 1
    assert result["reference_point_guids"] == ["RP-3", "RP-1", "RP-2"]
    assert result["prosecution_zone_reference_point_guids"] == ["RP-4", "RP-2"]


def test_lua_mission_update_maps_strike_execution_controls_and_verifies_them() -> None:
    run = _run_lua(
        "mission.update",
        {
            "side_guid": "SIDE-BLUE",
            "mission_guid": "MISSION-BLUE-0",
            "flight_size": 4,
            "use_flight_size": True,
            "minimum_aircraft_required": 2,
            "one_time_only": True,
            "preplanned_only": False,
        },
    )

    assert run.api_calls["set_mission"] == (
        (
            "SIDE-BLUE",
            "MISSION-BLUE-0",
            {
                "StrikeFlightSize": 4,
                "StrikeUseFlightSize": True,
                "StrikeMinAircraftReq": 2,
                "StrikeOneTimeOnly": True,
                "StrikePreplan": False,
            },
        ),
    )
    result = cast(dict[str, JsonValue], run.result)
    assert result["flight_size"] == 4
    assert result["use_flight_size"] is True
    assert result["minimum_aircraft_required"] == 2
    assert result["one_time_only"] is True
    assert result["preplanned_only"] is False


@pytest.mark.parametrize(
    ("operation", "target_guid", "assigned", "expected_targets"),
    [
        (
            "mission.target.add",
            "CONTACT-BLUE-1",
            True,
            ["CONTACT-BLUE-0", "CONTACT-BLUE-1"],
        ),
        ("mission.target.remove", "CONTACT-BLUE-0", False, []),
    ],
)
def test_lua_mission_target_updates_are_verified_on_strike_wrapper(
    operation: str,
    target_guid: str,
    assigned: bool,
    expected_targets: list[str],
) -> None:
    run = _run_lua(
        operation,
        {
            "side_guid": "SIDE-BLUE",
            "mission_guid": "MISSION-BLUE-0",
            "target_guid": target_guid,
        },
    )

    assert run.result == {
        "mission_guid": "MISSION-BLUE-0",
        "target_guid": target_guid,
        "assigned": assigned,
        "target_guids": expected_targets,
    }


def test_lua_doctrine_get_normalizes_numeric_and_wrapper_values() -> None:
    run = _run_lua(
        "doctrine.get",
        {"scope": "side", "side_guid": "SIDE-BLUE", "actual": True},
    )

    assert run.result == {
        "scope": "side",
        "target_guid": "SIDE-BLUE",
        "actual": True,
        "weapon_control_air": "Free",
        "weapon_control_surface": "Tight",
        "weapon_control_subsurface": "Hold",
        "weapon_control_land": "Free",
        "nuclear_use": False,
        "refuel_unrep": "Never",
        "engage_opportunity_targets": True,
        "automatic_evasion": True,
        "ignore_plotted_course": False,
        "ignore_emcon_while_under_attack": True,
        "maintain_standoff": False,
        "use_sams_in_anti_surface_mode": True,
        "engaging_ambiguous_targets": 1,
        "fuel_state_planned": "Joker",
        "fuel_state_rtb": 2,
        "weapon_state_planned": "Winchester",
        "weapon_state_rtb": 1,
        "withdraw_on_attack": 2,
        "withdraw_on_damage": 1,
        "withdraw_on_defence": 3,
        "withdraw_on_fuel": 2,
        "bvr_logic": 1,
        "dipping_sonar": 0,
        "use_aip": 2,
        "recharge_on_attack": 30,
        "recharge_on_patrol": 50,
        "radar": "Passive",
        "sonar": "Active",
        "oecm": "Passive",
    }


def test_lua_doctrine_set_uses_official_keys_and_returns_actual_readback() -> None:
    run = _run_lua(
        "doctrine.set",
        {
            "scope": "mission",
            "side_guid": "SIDE-BLUE",
            "mission_guid": "MISSION-BLUE-1",
            "weapon_control_air": "Hold",
            "nuclear_use": True,
            "refuel_unrep": "Always_IncludingTankersRefuellingTankers",
        },
    )
    result = cast(dict[str, JsonValue], run.result)

    assert run.api_calls["set_doctrine"] == (
        (
            "MISSION-BLUE-1",
            {
                "weapon_control_status_air": "Hold",
                "use_nuclear_weapons": True,
                "use_refuel_unrep": "Always_IncludingTankersRefuellingTankers",
            },
        ),
    )
    assert result["weapon_control_air"] == "Hold"
    assert result["nuclear_use"] is True
    assert result["refuel_unrep"] == "Always_IncludingTankersRefuellingTankers"


def test_lua_emcon_set_reads_back_transmitters_when_inheritance_is_unobservable() -> None:
    run = _run_lua(
        "emcon.set",
        {
            "scope": "unit",
            "target_guid": "UNIT-BLUE-1",
            "inherit": True,
            "radar": "Active",
            "sonar": "Passive",
        },
    )

    assert run.api_calls["set_emcon"] == (
        ("Unit", "UNIT-BLUE-1", "Inherit;Radar=Active;Sonar=Passive"),
    )
    assert run.result == {
        "scope": "unit",
        "target_guid": "UNIT-BLUE-1",
        "inherit": None,
        "radar": "Active",
        "sonar": "Passive",
        "oecm": "Passive",
    }


@pytest.mark.parametrize(
    ("code", "expected_multiplier"),
    [(0, 1), (1, 2), (2, 5), (3, 15), (4, 30), (5, 150)],
)
def test_lua_scenario_time_compression_set_uses_code_and_verifies_multiplier_readback(
    code: int,
    expected_multiplier: int,
) -> None:
    run = _run_lua("scenario.time_compression.set", {"code": code})

    assert run.api_calls["set_time_compression"] == (code,)
    assert run.result == {
        "code": code,
        "observed_time_compression": expected_multiplier,
        "accepted": True,
    }


def test_lua_repeated_delivery_executes_handler_and_exports_response_once() -> None:
    dispatcher = render_dispatcher(create_runtime_snapshot()).decode("utf-8")
    assert "CMO_AGENT_BRIDGE_RESPONSE_CACHE" in dispatcher
    assert "CMO_AGENT_BRIDGE_RESPONSE_CACHE =" in dispatcher
    assert "_G.CMO_AGENT_BRIDGE_RESPONSE_CACHE" not in dispatcher
    assert 'rawget(_G, "CMO_AGENT_BRIDGE_RESPONSE_CACHE")' not in dispatcher
    assert 'rawset(_G, "CMO_AGENT_BRIDGE_RESPONSE_CACHE"' not in dispatcher

    run = _run_lua(
        "scenario.time_compression.set",
        {"code": 5},
        repeat_dispatches=20,
    )

    assert run.api_calls["set_time_compression"] == (5,)
    assert len(run.api_calls["export_inst"]) == 1
    assert isinstance(run.result, dict)
    assert run.result["observed_time_compression"] == 150


def test_lua_named_global_cache_does_not_require_g_table() -> None:
    run = _run_lua(
        "scenario.time_compression.set",
        {"code": 4},
        repeat_dispatches=20,
        remove_g_table_before_dispatch=True,
    )

    assert run.api_calls["set_time_compression"] == (4,)
    assert len(run.api_calls["export_inst"]) == 1
    assert isinstance(run.result, dict)
    assert run.result["observed_time_compression"] == 30


def test_lua_engine_reload_boundary_clears_session_cache_and_allows_reexecution() -> None:
    run = _run_lua(
        "scenario.time_compression.set",
        {"code": 3},
        repeat_dispatches=2,
        clear_session_cache_between_dispatches=True,
    )

    assert run.api_calls["set_time_compression"] == (3, 3)
    assert len(run.api_calls["export_inst"]) == 2


def test_lua_export_failure_retries_cached_envelope_without_rerunning_handler() -> None:
    run = _run_lua(
        "scenario.time_compression.set",
        {"code": 2},
        repeat_dispatches=3,
        export_failures_before_success=1,
    )

    assert run.api_calls["set_time_compression"] == (2,)
    assert len(run.api_calls["export_inst"]) == 2
    assert run.api_calls["export_inst"][0] == run.api_calls["export_inst"][1]
    assert isinstance(run.result, dict)
    assert run.result["observed_time_compression"] == 5


def test_lua_zero_export_result_retries_cached_envelope_without_rerunning_handler() -> None:
    run = _run_lua(
        "scenario.time_compression.set",
        {"code": 1},
        repeat_dispatches=3,
        zero_export_results_before_success=1,
    )

    assert run.api_calls["set_time_compression"] == (1,)
    assert len(run.api_calls["export_inst"]) == 2
    assert run.api_calls["export_inst"][0] == run.api_calls["export_inst"][1]
    assert isinstance(run.result, dict)
    assert run.result["observed_time_compression"] == 2


def test_lua_side_posture_get_uses_both_side_guids() -> None:
    run = _run_lua(
        "side.posture.get",
        {"side_a_guid": "SIDE-BLUE", "side_b_guid": "SIDE-RED"},
    )

    assert run.api_calls["get_side_posture"] == (("SIDE-BLUE", "SIDE-RED"),)
    assert run.result == {
        "side_a_guid": "SIDE-BLUE",
        "side_b_guid": "SIDE-RED",
        "posture": "H",
    }


def test_lua_contact_get_projects_detection_emission_and_engagement_details() -> None:
    run = _run_lua(
        "contact.get",
        {"side_guid": "SIDE-BLUE", "contact_guid": "CONTACT-BLUE-1"},
    )
    result = cast(dict[str, JsonValue], run.result)

    assert result["guid"] == "CONTACT-BLUE-1"
    assert result["age_seconds"] == 90
    assert result["area_of_uncertainty"] == [
        {"latitude": 12.4, "longitude": 22.4},
        {"latitude": 12.6, "longitude": 22.6},
    ]
    assert result["detection_by"] == {
        "visual": 30,
        "infrared": 20,
        "radar": 1,
        "esm": 4,
    }
    assert cast(list[dict[str, JsonValue]], result["emissions"])[0]["sensor_dbid"] == 1484
    assert (
        cast(list[dict[str, JsonValue]], result["last_detections"])[0]["detector_guid"]
        == "UNIT-BLUE-1"
    )
    assert cast(list[dict[str, JsonValue]], result["potential_matches"])[0]["subtype"] == 2001
    assert result["bda"] == {
        "fires": "None",
        "flood": "None",
        "structural": "Light",
    }
    assert result["detected_by_side_guid"] == "SIDE-BLUE"
    assert result["observer_side_guid_shared"] == "SIDE-ALLY"
    assert result["targeted_by_unit_guids"] == ["UNIT-BLUE-1"]
    assert result["fired_on_by_unit_guids"] == ["UNIT-BLUE-0"]
    assert result["firing_at_contact_guids"] == ["CONTACT-BLUE-0"]


def test_lua_contact_posture_set_mutates_wrapper_and_returns_detailed_readback() -> None:
    run = _run_lua(
        "contact.posture.set",
        {
            "side_guid": "SIDE-BLUE",
            "contact_guid": "CONTACT-BLUE-1",
            "posture": "H",
        },
    )

    result = cast(dict[str, JsonValue], run.result)
    assert result["guid"] == "CONTACT-BLUE-1"
    assert result["posture"] == "H"
    assert len(run.api_calls["get_contact"]) == 2


def test_lua_contact_weapon_allocations_get_normalizes_official_six_fields() -> None:
    run = _run_lua(
        "contact.weapon_allocations.get",
        {"side_guid": "SIDE-BLUE", "contact_guid": "CONTACT-BLUE-1"},
    )

    assert run.api_calls["weapon_allocation"] == ((None, "CONTACT-BLUE-1", "SIDE-BLUE"),)
    assert run.result == {
        "contact_guid": "CONTACT-BLUE-1",
        "allocations": [
            {
                "shooter_guid": "UNIT-BLUE-1",
                "quantity_assigned": 2,
                "weapon_dbid": 301,
                "weapon_name": "Test AAM",
                "target_guid": "CONTACT-BLUE-1",
                "quantity_fired": 1,
            }
        ],
    }


def test_lua_unit_inventory_get_normalizes_sensor_mount_magazine_and_cargo() -> None:
    run = _run_lua("unit.inventory.get", {"unit_guid": "UNIT-BLUE-1"})
    result = cast(dict[str, JsonValue], run.result)

    assert result["unit_guid"] == "UNIT-BLUE-1"
    assert cast(list[dict[str, JsonValue]], result["sensors"])[0] == {
        "guid": "SENSOR-BLUE-1",
        "dbid": 401,
        "name": "Test AESA",
        "max_range_nm": 180,
        "active": False,
        "status": "Operational",
        "role": 2003,
        "type": 2001,
    }
    assert cast(list[dict[str, JsonValue]], result["mounts"])[0]["guid"] == "MOUNT-BLUE-1"
    assert cast(list[dict[str, JsonValue]], result["magazines"])[0]["guid"] == "MAG-BLUE-1"
    assert cast(list[dict[str, JsonValue]], result["cargo"])[0]["guid"] == "CARGO-BLUE-1"
    assert cast(list[dict[str, JsonValue]], result["cargo"])[0]["object_type"] == 2


def test_lua_unit_sensor_set_uses_nested_sensor_descriptor_and_rereads() -> None:
    run = _run_lua(
        "unit.sensor.set",
        {
            "unit_guid": "UNIT-BLUE-1",
            "sensor_guid": "SENSOR-BLUE-1",
            "active": True,
            "obey_emcon": False,
        },
    )

    assert run.set_unit_calls == (
        {
            "guid": "UNIT-BLUE-1",
            "sensors": {
                "sensor_guid": "SENSOR-BLUE-1",
                "sensor_isactive": True,
            },
            "obeyEMCON": False,
        },
    )
    assert run.result == {
        "unit_guid": "UNIT-BLUE-1",
        "sensor_guid": "SENSOR-BLUE-1",
        "active": True,
        "obey_emcon": False,
    }


def test_lua_unit_magazine_adjust_uses_official_descriptor_and_inventory_readback() -> None:
    run = _run_lua(
        "unit.magazine.adjust",
        {
            "unit_guid": "UNIT-BLUE-1",
            "magazine_guid": "MAG-BLUE-1",
            "weapon_dbid": 301,
            "mode": "add",
            "quantity": 3,
            "max_capacity": 20,
            "allow_new": True,
        },
    )

    assert run.api_calls["add_weapon_to_magazine"] == (
        {
            "guid": "UNIT-BLUE-1",
            "mag_guid": "MAG-BLUE-1",
            "wpn_dbid": 301,
            "number": 3,
            "maxcap": 20,
            "new": True,
        },
    )
    magazine = cast(
        list[dict[str, JsonValue]],
        cast(dict[str, JsonValue], run.result)["magazines"],
    )[0]
    assert cast(list[dict[str, JsonValue]], magazine["weapons"])[0]["current"] == 11


def test_lua_unit_mount_reload_adjust_maps_remove_and_addascell() -> None:
    run = _run_lua(
        "unit.mount_reload.adjust",
        {
            "unit_guid": "UNIT-BLUE-1",
            "mount_guid": "MOUNT-BLUE-1",
            "weapon_dbid": 301,
            "mode": "remove",
            "quantity": 1,
            "add_as_cell": False,
        },
    )

    assert run.api_calls["add_reloads_to_unit"] == (
        {
            "guid": "UNIT-BLUE-1",
            "mount_guid": "MOUNT-BLUE-1",
            "wpn_dbid": 301,
            "number": 1,
            "remove": True,
            "addascell": False,
        },
    )
    mount = cast(
        list[dict[str, JsonValue]],
        cast(dict[str, JsonValue], run.result)["mounts"],
    )[0]
    assert cast(list[dict[str, JsonValue]], mount["weapons"])[0]["current"] == 1


def test_lua_unit_cargo_transfer_preserves_guid_and_quantity_dbid_forms() -> None:
    run = _run_lua(
        "unit.cargo.transfer",
        {
            "from_unit_guid": "UNIT-BLUE-1",
            "to_unit_guid": "UNIT-BLUE-0",
            "items": [
                {"cargo_guid": "CARGO-BLUE-1"},
                {"quantity": 3, "dbid": 801},
            ],
        },
    )

    assert run.api_calls["transfer_cargo"] == (
        (
            "UNIT-BLUE-1",
            "UNIT-BLUE-0",
            ("CARGO-BLUE-1", (3, 801)),
        ),
    )
    assert run.result == {
        "from_unit_guid": "UNIT-BLUE-1",
        "to_unit_guid": "UNIT-BLUE-0",
        "command": "transfer",
        "accepted": True,
    }


def test_lua_unit_cargo_unload_uses_unit_guid_and_reports_acceptance() -> None:
    run = _run_lua("unit.cargo.unload", {"unit_guid": "UNIT-BLUE-1"})

    assert run.api_calls["unload_cargo"] == ("UNIT-BLUE-1",)
    assert run.result == {
        "unit_guid": "UNIT-BLUE-1",
        "command": "unload",
        "accepted": True,
    }


def test_lua_unit_set_maps_submarine_navigation_and_manual_controls() -> None:
    run = _run_lua(
        "unit.set",
        {
            "unit_guid": "UNIT-BLUE-2",
            "depth": -120,
            "throttle": "Full",
            "force_speed": True,
            "desired_heading": 225,
            "move_to": True,
            "manual_throttle": "Flank",
            "manual_speed": 18,
            "manual_altitude": -120,
            "hold_position": True,
            "hold_fire": True,
            "sprint_drift": False,
            "avoid_cavitation": False,
            "obey_emcon": False,
        },
    )

    assert run.set_unit_calls == (
        {
            "guid": "UNIT-BLUE-2",
            "depth": -120,
            "throttle": "Full",
            "forceSpeed": True,
            "desiredHeading": 225,
            "moveto": True,
            "manualthrottle": "Flank",
            "manualspeed": 18,
            "manualaltitude": -120,
            "holdPosition": True,
            "holdFire": True,
            "sprintDrift": False,
            "avoidcavitation": False,
            "obeyEMCON": False,
        },
    )
    result = cast(dict[str, JsonValue], run.result)
    assert result["depth"] == -120
    assert result["desired_heading"] == 225
    assert result["sprint_drift"] is False
    assert result["avoid_cavitation"] is False
    assert result["obey_emcon"] is False


@pytest.mark.parametrize(
    ("details", "expected_class", "expected_options", "expected_set"),
    [
        (
            {
                "mission_class": "mining",
                "reference_point_guids": ["RP-1", "RP-2", "RP-3"],
            },
            "Mining",
            {"zone": ("RP-1", "RP-2", "RP-3")},
            {"isactive": False},
        ),
        (
            {
                "mission_class": "mine_clearing",
                "reference_point_guids": ["RP-2", "RP-3", "RP-4"],
            },
            "MineClearing",
            {},
            {"isactive": False, "Zone": ("RP-2", "RP-3", "RP-4")},
        ),
        (
            {
                "mission_class": "cargo",
                "cargo_subtype": "transfer",
                "destination_guid": "UNIT-BLUE-0",
            },
            "Cargo",
            {},
            {
                "isactive": False,
                "subtype": "transfer",
                "DestinationUnitID": "UNIT-BLUE-0",
            },
        ),
        (
            {
                "mission_class": "cargo",
                "cargo_subtype": "delivery",
                "reference_point_guids": ["RP-1", "RP-3", "RP-4"],
            },
            "Cargo",
            {"zone": ("RP-1", "RP-3", "RP-4")},
            {"isactive": False, "subtype": "delivery"},
        ),
    ],
)
def test_lua_mission_create_supports_mining_mineclearing_and_cargo(
    details: dict[str, object],
    expected_class: str,
    expected_options: dict[str, object],
    expected_set: dict[str, object],
) -> None:
    run = _run_lua(
        "mission.create",
        {"side_guid": "SIDE-BLUE", "name": "Extended Mission", "details": details},
    )

    assert run.api_calls["add_mission"] == (
        ("SIDE-BLUE", "Extended Mission", expected_class, expected_options),
    )
    assert run.api_calls["set_mission"] == (("SIDE-BLUE", "MISSION-ADDED-4", expected_set),)
    result = cast(dict[str, JsonValue], run.result)
    assert result["mission_guid"] == "MISSION-ADDED-4"
    assert result["active"] is False


def test_lua_mission_update_maps_extended_strike_controls() -> None:
    run = _run_lua(
        "mission.update",
        {
            "side_guid": "SIDE-BLUE",
            "mission_guid": "MISSION-BLUE-0",
            "group_size": 2,
            "use_group_size": True,
            "attack_throttle_aircraft": "Full",
            "attack_altitude_aircraft": 150,
            "strike_minimum_trigger": "Hostile",
            "strike_max_flights": 3,
            "strike_auto_planner": True,
            "strike_min_distance_aircraft": 25,
            "strike_max_distance_aircraft": 500,
            "strike_min_distance_ship": 15,
            "strike_max_distance_ship": 300,
            "focus_on_strike": True,
        },
    )

    descriptor = cast(tuple[object, object, dict[str, object]], run.api_calls["set_mission"][0])
    assert descriptor[2] == {
        "StrikeGroupSize": 2,
        "StrikeUseGroupSize": True,
        "AttackThrottleAircraft": "Full",
        "AttackAltitudeAircraft": 150,
        "StrikeMinimumTrigger": "Hostile",
        "StrikeMax": 3,
        "StrikeAutoPlanner": True,
        "StrikeMinDistAircraft": 25,
        "StrikeMaxDistAircraft": 500,
        "StrikeMinDistShip": 15,
        "StrikeMaxDistShip": 300,
        "FocusOnStrike": True,
    }
    result = cast(dict[str, JsonValue], run.result)
    assert result["group_size"] == 2
    assert result["strike_minimum_trigger"] == "Hostile"
    assert result["focus_on_strike"] is True


def test_lua_mission_cargo_update_calls_wrapper_method_and_rereads() -> None:
    run = _run_lua(
        "mission.cargo.update",
        {
            "side_guid": "SIDE-BLUE",
            "mission_guid": "MISSION-BLUE-2",
            "action": "assign",
            "cargo_kind": "object",
            "object_type": 2,
            "dbid": 802,
            "cargo_guid": "CARGO-BLUE-2",
        },
    )

    result = cast(dict[str, JsonValue], run.result)
    assert result["mission_guid"] == "MISSION-BLUE-2"
    assert result["action"] == "assign"
    assert cast(list[dict[str, JsonValue]], result["assigned_cargo"])[-1] == {
        "object_type": 2,
        "dbid": 802,
        "guid": "CARGO-BLUE-2",
        "quantity": 1,
    }


def test_lua_doctrine_set_maps_extended_fields_and_returns_readback() -> None:
    run = _run_lua(
        "doctrine.set",
        {
            "scope": "unit",
            "side_guid": "SIDE-BLUE",
            "unit_guid": "UNIT-BLUE-1",
            "engage_opportunity_targets": False,
            "automatic_evasion": False,
            "maintain_standoff": True,
            "bvr_logic": 2,
            "recharge_on_patrol": 80,
        },
    )

    assert run.api_calls["set_doctrine"] == (
        (
            "UNIT-BLUE-1",
            {
                "engage_opportunity_targets": False,
                "automatic_evasion": False,
                "maintain_standoff": True,
                "bvr_logic": 2,
                "recharge_on_patrol": 80,
            },
        ),
    )
    result = cast(dict[str, JsonValue], run.result)
    assert result["engage_opportunity_targets"] is False
    assert result["automatic_evasion"] is False
    assert result["maintain_standoff"] is True
    assert result["bvr_logic"] == 2
    assert result["recharge_on_patrol"] == 80


def test_lua_doctrine_set_preserves_inherit_for_boolean_fields() -> None:
    run = _run_lua(
        "doctrine.set",
        {
            "scope": "unit",
            "side_guid": "SIDE-BLUE",
            "unit_guid": "UNIT-BLUE-1",
            "maintain_standoff": "inherit",
        },
    )

    assert run.api_calls["set_doctrine"] == (("UNIT-BLUE-1", {"maintain_standoff": "inherit"}),)
    assert cast(dict[str, JsonValue], run.result).get("maintain_standoff") is None


def test_lua_doctrine_wra_get_uses_weapon_id_and_normalizes_entries() -> None:
    run = _run_lua(
        "doctrine.wra.get",
        {
            "scope": "unit",
            "side_guid": "SIDE-BLUE",
            "unit_guid": "UNIT-BLUE-1",
            "target_type": "Aircraft",
            "weapon_dbid": 301,
            "full_wra": True,
        },
    )

    assert run.api_calls["get_doctrine_wra"] == (
        {
            "side": "SIDE-BLUE",
            "guid": "UNIT-BLUE-1",
            "target_type": "Aircraft",
            "weapon_id": 301,
            "full_wra": True,
        },
    )
    assert run.result == {
        "scope": "unit",
        "target_guid": "UNIT-BLUE-1",
        "level": "Unit",
        "target_type": 2001,
        "entries": [
            {
                "weapon_dbid": 301,
                "weapon_name": "Test AAM",
                "weapons_per_salvo": 2,
                "shooters_per_salvo": 1,
                "firing_range": "75ofmax",
                "self_defence_range": "Max",
            }
        ],
    }


def test_lua_doctrine_wra_get_normalizes_documented_nil_as_empty_result() -> None:
    run = _run_lua(
        "doctrine.wra.get",
        {
            "scope": "side",
            "side_guid": "SIDE-BLUE",
            "target_type": "NoMatch",
            "full_wra": True,
        },
    )

    assert run.api_calls["get_doctrine_wra"] == (
        {
            "side": "SIDE-BLUE",
            "target_type": "NoMatch",
            "full_wra": True,
        },
    )
    assert run.result == {
        "scope": "side",
        "target_guid": "SIDE-BLUE",
        "level": "side",
        "target_type": "NoMatch",
        "entries": [],
    }


def test_lua_doctrine_wra_set_uses_ordered_four_element_array_and_rereads() -> None:
    run = _run_lua(
        "doctrine.wra.set",
        {
            "scope": "mission",
            "side_guid": "SIDE-BLUE",
            "mission_guid": "MISSION-BLUE-1",
            "contact_guid": "CONTACT-BLUE-1",
            "weapon_dbid": 301,
            "weapons_per_salvo": "Max",
            "shooters_per_salvo": 2,
            "firing_range": 90,
            "self_defence_range": "inherit",
        },
    )

    assert run.api_calls["set_doctrine_wra"] == (
        (
            {
                "side": "SIDE-BLUE",
                "mission": "MISSION-BLUE-1",
                "contact_id": "CONTACT-BLUE-1",
                "weapon_dbid": 301,
            },
            ("Max", 2, 90, "inherit"),
        ),
    )
    result = cast(dict[str, JsonValue], run.result)
    entry = cast(list[dict[str, JsonValue]], result["entries"])[0]
    assert entry["weapons_per_salvo"] == "Max"
    assert entry["shooters_per_salvo"] == 2
    assert entry["firing_range"] == 90
    assert entry["self_defence_range"] == "inherit"


def test_lua_special_action_list_all_sides_omits_script_text() -> None:
    run = _run_lua("special_action.list", {})

    assert run.api_calls["get_special_action"] == ({"mode": "list"},)
    assert run.result == {
        "items": [
            {
                "guid": "ACTION-BLUE-1",
                "name": "Emergency Reinforcement",
                "description": "Deploy reserve aircraft",
                "active": True,
                "repeatable": False,
            }
        ]
    }


def test_lua_special_action_execute_verifies_side_executes_guid_and_rereads() -> None:
    run = _run_lua(
        "special_action.execute",
        {"side_guid": "SIDE-BLUE", "action_guid": "ACTION-BLUE-1"},
    )

    assert run.api_calls["execute_special_action"] == ("ACTION-BLUE-1",)
    assert run.api_calls["get_special_action"] == (
        {"side": "SIDE-BLUE", "ActionNameOrID": "ACTION-BLUE-1"},
        {"side": "SIDE-BLUE", "ActionNameOrID": "ACTION-BLUE-1"},
    )
    assert run.result == {
        "action_guid": "ACTION-BLUE-1",
        "name": "Emergency Reinforcement",
        "accepted": True,
        "active": True,
        "repeatable": False,
    }
