from __future__ import annotations

import argparse
import hashlib
import json
import pprint
from pathlib import Path
from typing import TYPE_CHECKING, TypeAliasType

from pydantic import TypeAdapter, ValidationError

from cmo_agent_bridge.errors import BridgeError
from cmo_agent_bridge.operations import models

if TYPE_CHECKING:
    from cmo_agent_bridge.operations.registry import OperationRegistry, ResolvedInvocation


ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = ROOT / "protocol" / "operations.json"
PYTHON_PATH = ROOT / "src" / "cmo_agent_bridge" / "operations" / "generated_manifest.py"
CORPUS_PATH = ROOT / "protocol" / "schema-corpus.json"

INVARIANT_IDS = {
    "exactly_one_side_selector",
    "unit_get_selector_form",
    "mission_get_selector_form",
    "doctrine_scope_fields",
    "at_least_one_update",
    "mission_details_discriminator",
    "reconcile_disposition_pair",
    "compat_phase_case",
    "uninstall_phase_marker",
    "emcon_has_clause",
    "trusted_confirmation",
    "lua_function_arguments",
    "projection_allowlist",
    "unit_add_location_form",
    "unit_refuel_tanker_selector",
    "unit_attack_contact_mode_fields",
    "unit_speed_throttle_exclusive",
    "cargo_mission_form",
    "inventory_adjust_mode_fields",
    "cargo_transfer_item_form",
    "mission_cargo_form",
    "doctrine_wra_target_selector",
    "at_most_one_side_selector",
    "reference_point_location_form",
    "reference_point_relative_form",
    "mission_category_parent_pool",
    "mission_flight_plan_schedule_form",
}

ENTRY_KEYS = {
    "name",
    "target",
    "base_class",
    "public_arguments_model",
    "wire_arguments_model",
    "wire_resolver",
    "effective_class_resolver",
    "resolver_data",
    "wire_result_factory",
    "public_result_factory",
    "result_factory_data",
    "recovery_factory",
    "confirmation_required",
    "expose_mcp",
    "trusted_fields",
    "invariant_ids",
    "arguments_ast",
}
REQUIRED_ENTRY_KEYS = ENTRY_KEYS - {"resolver_data", "result_factory_data"}
AST_KEYS = {
    "type",
    "properties",
    "required",
    "no_extra_fields",
    "enum",
    "minimum",
    "maximum",
    "min_length",
    "max_length",
    "items",
    "min_items",
    "max_items",
    "unique_items",
    "nullable",
    "discriminator",
    "one_of",
}
AST_TYPES = {"object", "string", "uuid", "integer", "number", "boolean", "array"}

VALID_ARGUMENTS: dict[str, dict[str, object]] = {
    "bridge.status": {},
    "bridge.reconcile": {},
    "scenario.get": {},
    "side.list": {},
    "reference_point.list": {"side_guid": "SIDE-1"},
    "unit.list": {"side_guid": "SIDE-1"},
    "unit.get": {"unit_guid": "UNIT-1"},
    "unit.combat_status.get": {"unit_guid": "UNIT-1"},
    "unit.loadout.get": {"unit_guid": "UNIT-1"},
    "contact.list": {"side_name": "Blue"},
    "mission.list": {"side_guid": "SIDE-1"},
    "mission.get": {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"},
    "doctrine.get": {"scope": "side", "side_guid": "SIDE-1"},
    "reference_point.add": {
        "side_guid": "SIDE-1",
        "name": "CAP-1",
        "latitude": 1,
        "longitude": 2,
    },
    "reference_point.update": {
        "side_guid": "SIDE-1",
        "reference_point_guid": "RP-1",
        "latitude": 3,
    },
    "unit.set": {"unit_guid": "UNIT-1", "heading": 90},
    "unit.add": {
        "side_guid": "SIDE-1",
        "unit_type": "Aircraft",
        "dbid": 1,
        "name": "Alpha",
        "latitude": 1,
        "longitude": 2,
    },
    "unit.assign_mission": {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1"},
    "unit.unassign_mission": {"unit_guid": "UNIT-1"},
    "unit.loadout.set": {"unit_guid": "UNIT-1", "loadout_dbid": 1001},
    "unit.launch": {"unit_guid": "UNIT-1"},
    "unit.rtb": {"unit_guid": "UNIT-1"},
    "unit.refuel": {"unit_guid": "UNIT-1"},
    "unit.attack_contact": {
        "side_guid": "SIDE-1",
        "attacker_unit_guid": "UNIT-1",
        "contact_guid": "CONTACT-1",
        "mode": "manual_weapon",
        "weapon_dbid": 2001,
        "quantity": 2,
    },
    "mission.create": {
        "side_guid": "SIDE-1",
        "name": "CAP",
        "details": {
            "mission_class": "patrol",
            "patrol_type": "aaw",
            "reference_point_guids": ["RP-1", "RP-2", "RP-3"],
        },
    },
    "mission.update": {"side_guid": "SIDE-1", "mission_guid": "MISSION-1", "active": True},
    "mission.air_refueling.update": {
        "side_guid": "SIDE-1",
        "mission_guid": "MISSION-1",
        "tanker_usage": "mission",
        "tanker_mission_guids": ["TANKER-MISSION-1"],
    },
    "mission.flight_plan.list": {
        "side_guid": "SIDE-1",
        "mission_guid": "MISSION-1",
    },
    "mission.flight_plan.create": {
        "side_guid": "SIDE-1",
        "mission_guid": "MISSION-1",
        "date_on_target": "2026/07/16",
        "time_on_target": "12:30:00",
    },
    "mission.target.add": {
        "side_guid": "SIDE-1",
        "mission_guid": "MISSION-1",
        "target_guid": "TARGET-1",
    },
    "mission.target.remove": {
        "side_guid": "SIDE-1",
        "mission_guid": "MISSION-1",
        "target_guid": "TARGET-1",
    },
    "doctrine.set": {"scope": "side", "side_guid": "SIDE-1", "weapon_control_air": "Hold"},
    "emcon.set": {"scope": "unit", "target_guid": "UNIT-1", "radar": "Passive"},
    "scenario.time_compression.set": {"code": 2},
    "side.posture.get": {"side_a_guid": "SIDE-1", "side_b_guid": "SIDE-2"},
    "contact.get": {"side_guid": "SIDE-1", "contact_guid": "CONTACT-1"},
    "contact.posture.set": {
        "side_guid": "SIDE-1",
        "contact_guid": "CONTACT-1",
        "posture": "H",
    },
    "contact.weapon_allocations.get": {
        "side_guid": "SIDE-1",
        "contact_guid": "CONTACT-1",
    },
    "unit.inventory.get": {"unit_guid": "UNIT-1"},
    "unit.sensor.set": {
        "unit_guid": "UNIT-1",
        "sensor_guid": "SENSOR-1",
        "active": True,
    },
    "unit.magazine.adjust": {
        "unit_guid": "UNIT-1",
        "magazine_guid": "MAG-1",
        "weapon_dbid": 2001,
        "mode": "add",
        "quantity": 4,
    },
    "unit.mount_reload.adjust": {
        "unit_guid": "UNIT-1",
        "mount_guid": "MOUNT-1",
        "weapon_dbid": 2001,
        "mode": "fill",
    },
    "unit.cargo.transfer": {
        "from_unit_guid": "UNIT-1",
        "to_unit_guid": "UNIT-2",
        "items": [{"cargo_guid": "CARGO-1"}],
    },
    "unit.cargo.unload": {"unit_guid": "UNIT-1"},
    "mission.cargo.update": {
        "side_guid": "SIDE-1",
        "mission_guid": "MISSION-1",
        "action": "assign",
        "cargo_kind": "mount",
        "dbid": 3001,
        "quantity": 2,
    },
    "doctrine.wra.get": {
        "scope": "side",
        "side_guid": "SIDE-1",
        "target_type": "Air_Contact_Unknown_Type",
    },
    "doctrine.wra.set": {
        "scope": "side",
        "side_guid": "SIDE-1",
        "target_type": "Air_Contact_Unknown_Type",
        "weapon_dbid": 2001,
        "weapons_per_salvo": "inherit",
        "shooters_per_salvo": 2,
        "firing_range": 50.0,
        "self_defence_range": "max",
    },
    "special_action.list": {},
    "special_action.execute": {"side_guid": "SIDE-1", "action_guid": "ACTION-1"},
    "unit.delete": {"unit_guid": "UNIT-1"},
    "mission.delete": {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"},
    "lua.call": {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}},
    "compat.probe.step": {"step": "observational"},
    "bridge.prepare": {},
    "bridge.doctor": {},
    "bridge.uninstall": {"phase": "command"},
    "compat.probe": {"phase": "automatic"},
}

VALID_TRUSTED_ENRICHMENT: dict[str, dict[str, object]] = {
    "bridge.status": {"activation_candidate": "00000000-0000-0000-0000-000000000001"},
    "unit.delete": {"confirmation_proof": "a" * 64},
    "mission.delete": {"confirmation_proof": "a" * 64},
}

ADDITIONAL_VALID_INVOCATIONS: dict[str, list[tuple[str, dict[str, object], dict[str, object]]]] = {
    "bridge.reconcile": [
        (
            "explicit-null-probe",
            {"request_id": None, "disposition": None},
            {},
        ),
        (
            "confirmed-applied",
            {
                "request_id": "00000000-0000-0000-0000-000000000002",
                "disposition": "applied",
            },
            {"confirmation_proof": "b" * 64},
        ),
        (
            "confirmed-not-applied",
            {
                "request_id": "00000000-0000-0000-0000-000000000002",
                "disposition": "not_applied",
            },
            {"confirmation_proof": "b" * 64},
        ),
    ]
}

ADDITIONAL_VALID_ARGUMENTS: dict[str, list[dict[str, object]]] = {
    "doctrine.get": [
        {"scope": "mission", "side_guid": "SIDE-1", "mission_guid": "M-1", "actual": True}
    ],
    "mission.create": [
        {
            "side_guid": "SIDE-1",
            "name": "Strike pool",
            "category": "task_pool",
            "details": {
                "mission_class": "strike",
                "strike_type": "land",
            },
        },
        {
            "side_guid": "SIDE-1",
            "name": "Strike package",
            "category": "package",
            "parent_task_pool_guid": "POOL-1",
            "details": {
                "mission_class": "strike",
                "strike_type": "land",
                "target_guids": ["TARGET-1"],
            },
        },
        {
            "side_guid": "SIDE-1",
            "name": "Support",
            "details": {
                "mission_class": "support",
                "reference_point_guids": ["RP-1", "RP-2"],
            },
        },
        {
            "side_guid": "SIDE-1",
            "name": "Strike",
            "details": {
                "mission_class": "strike",
                "strike_type": "air",
                "target_guids": ["TARGET-1"],
            },
        },
        {
            "side_guid": "SIDE-1",
            "name": "Ferry",
            "details": {"mission_class": "ferry", "destination_guid": "BASE-1"},
        },
        {
            "side_guid": "SIDE-1",
            "name": "Mining",
            "details": {
                "mission_class": "mining",
                "reference_point_guids": ["RP-1", "RP-2", "RP-3"],
            },
        },
        {
            "side_guid": "SIDE-1",
            "name": "Mine clearing",
            "details": {
                "mission_class": "mine_clearing",
                "reference_point_guids": ["RP-1", "RP-2", "RP-3"],
            },
        },
        {
            "side_guid": "SIDE-1",
            "name": "Cargo transfer",
            "details": {
                "mission_class": "cargo",
                "cargo_subtype": "transfer",
                "destination_guid": "BASE-1",
            },
        },
        {
            "side_guid": "SIDE-1",
            "name": "Cargo delivery",
            "details": {
                "mission_class": "cargo",
                "cargo_subtype": "delivery",
                "reference_point_guids": ["RP-1", "RP-2", "RP-3"],
            },
        },
    ],
    "compat.probe.step": [
        {"step": "payload", "candidate_bytes": 1024},
        {"step": "high-speed"},
        {"step": "lineage"},
        {"step": "key-value"},
        {"step": "dedupe"},
        {"step": "indeterminate"},
        {"step": "ledger-capacity", "candidate_entries": 32},
        {
            "step": "apply-profile",
            "safe_payload_bytes": 4096,
            "verified_ledger_entries": 32,
            "effective_ledger_capacity": 32,
        },
        {
            "step": "apply-profile",
            "safe_payload_bytes": 4096,
            "verified_ledger_entries": 1,
            "effective_ledger_capacity": 2,
        },
    ],
    "unit.set": [
        {"unit_guid": "UNIT-1", "course": []},
        {"unit_guid": "UNIT-1", "throttle": "Cruise", "move_to": True},
        {"unit_guid": "UNIT-1", "manual_altitude": "Low"},
    ],
    "unit.add": [
        {
            "side_guid": "SIDE-1",
            "unit_type": "Aircraft",
            "dbid": 1,
            "name": "Based Alpha",
            "base_guid": "BASE-1",
        }
    ],
    "unit.loadout.set": [
        {
            "unit_guid": "UNIT-1",
            "loadout_dbid": 1001,
            "time_to_ready_minutes": 30,
            "ignore_magazines": True,
            "exclude_optional_weapons": True,
        }
    ],
    "unit.refuel": [
        {"unit_guid": "UNIT-1", "tanker_guid": "TANKER-1"},
        {"unit_guid": "UNIT-1", "tanker_mission_guids": ["MISSION-1", "MISSION-2"]},
    ],
    "unit.attack_contact": [
        {
            "side_guid": "SIDE-1",
            "attacker_unit_guid": "UNIT-1",
            "contact_guid": "CONTACT-1",
            "mode": "auto",
        },
        {
            "side_guid": "SIDE-1",
            "attacker_unit_guid": "UNIT-1",
            "contact_guid": "CONTACT-1",
            "mode": "manual_target",
        },
        {
            "side_guid": "SIDE-1",
            "attacker_unit_guid": "UNIT-1",
            "contact_guid": "CONTACT-1",
            "mode": "manual_weapon",
            "mount_dbid": 3001,
            "weapon_dbid": 2001,
            "quantity": 1,
        },
    ],
    "mission.update": [
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "reference_point_guids": ["RP-3", "RP-1", "RP-2", "RP-1"],
            "prosecution_zone_reference_point_guids": [],
        },
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "strike_minimum_trigger": "hostile",
        },
    ],
    "reference_point.add": [
        {
            "side_guid": "SIDE-1",
            "name": "Carrier CAP corner",
            "relative_to_type": "unit",
            "relative_to_guid": "UNIT-1",
            "relative_bearing_deg": 45,
            "relative_distance_nm": 50,
            "bearing_type": "rotating",
        }
    ],
    "reference_point.update": [
        {
            "side_guid": "SIDE-1",
            "reference_point_guid": "RP-1",
            "relative_bearing_deg": 90,
            "relative_distance_nm": 60,
            "bearing_type": "fixed",
        },
        {
            "side_guid": "SIDE-1",
            "reference_point_guid": "RP-1",
            "clear_relative": True,
        },
    ],
    "mission.air_refueling.update": [
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "use_refuel_unrep": "Always_IncludingTankersRefuellingTankers",
            "tanker_usage": "automatic",
            "launch_without_tankers_in_place": False,
            "tanker_follows_receivers": True,
            "keep_on_mission_without_tankers_in_place": False,
            "tanker_mission_guids": [],
            "minimum_tankers_total": 2,
            "minimum_tankers_airborne": 1,
            "minimum_tankers_on_station": 1,
            "max_receivers_in_queue_per_tanker": 3,
            "fuel_percent_to_start_looking": 40,
            "tanker_max_distance_nm": "internal",
        },
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "tanker_one_time": True,
            "tanker_max_receivers": "Unlimited",
        },
    ],
    "mission.flight_plan.create": [
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "takeoff_date": "2026/07/16",
            "takeoff_time": "10:00:00",
        }
    ],
    "doctrine.set": [
        {
            "scope": "unit",
            "side_guid": "SIDE-1",
            "unit_guid": "UNIT-1",
            "weapon_control_air": "inherit",
        },
        {
            "scope": "mission",
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "engage_opportunity_targets": "inherit",
            "automatic_evasion": "inherit",
            "ignore_plotted_course": "inherit",
            "ignore_emcon_while_under_attack": "inherit",
            "maintain_standoff": "inherit",
            "use_sams_in_anti_surface_mode": "inherit",
        },
    ],
    "emcon.set": [{"scope": "unit", "target_guid": "UNIT-1", "inherit": True}],
    "unit.sensor.set": [
        {
            "unit_guid": "UNIT-1",
            "sensor_guid": "SENSOR-1",
            "active": False,
            "obey_emcon": False,
        }
    ],
    "unit.magazine.adjust": [
        {
            "unit_guid": "UNIT-1",
            "magazine_guid": "MAG-1",
            "weapon_dbid": 2001,
            "mode": "fill",
            "max_capacity": 20,
            "allow_new": True,
        },
        {
            "unit_guid": "UNIT-1",
            "magazine_guid": "MAG-1",
            "weapon_dbid": 2001,
            "mode": "remove",
            "quantity": 1,
        },
    ],
    "unit.mount_reload.adjust": [
        {
            "unit_guid": "UNIT-1",
            "mount_guid": "MOUNT-1",
            "weapon_dbid": 2001,
            "mode": "add",
            "quantity": 1,
            "add_as_cell": True,
        }
    ],
    "unit.cargo.transfer": [
        {
            "from_unit_guid": "UNIT-1",
            "to_unit_guid": "UNIT-2",
            "items": [{"dbid": 4001, "quantity": 3}],
        }
    ],
    "mission.cargo.update": [
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "action": "unassign",
            "cargo_kind": "object",
            "dbid": 4001,
            "object_type": 2,
            "cargo_guid": "CARGO-1",
        }
    ],
    "doctrine.wra.get": [
        {
            "scope": "unit",
            "side_guid": "SIDE-1",
            "unit_guid": "UNIT-1",
            "contact_guid": "CONTACT-1",
            "weapon_dbid": 2001,
            "full_wra": True,
        }
    ],
    "doctrine.wra.set": [
        {
            "scope": "mission",
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "contact_guid": "CONTACT-1",
            "weapon_dbid": 2001,
            "weapons_per_salvo": "max",
            "shooters_per_salvo": "inherit",
            "firing_range": 75,
            "self_defence_range": "system",
        }
    ],
    "special_action.list": [{"side_guid": "SIDE-1"}, {"side_name": "Blue"}],
    "lua.call": [
        {"function": "ScenEdit_GetWeather", "arguments": {}},
        {
            "function": "ScenEdit_SetWeather",
            "arguments": {
                "temperature_c": -20,
                "rainfall": 5,
                "undercloud_fraction": 0.4,
                "sea_state": 3,
            },
        },
        {
            "function": "SetScenarioTitle",
            "arguments": {"title": "Northern Shield"},
        },
        {
            "function": "ScenEdit_SetTime",
            "arguments": {
                "current_time": "2026-07-16T12:00:00",
                "duration": "1:06:00",
            },
        },
        {
            "function": "ScenEdit_AddSide",
            "arguments": {"name": "OPFOR"},
        },
        {
            "function": "ScenEdit_SetSideOptions",
            "arguments": {
                "side_guid": "SIDE-1",
                "awareness": "Blind",
                "computer_controlled_only": True,
            },
        },
        {
            "function": "ScenEdit_SetSidePosture",
            "arguments": {
                "side_a_guid": "SIDE-1",
                "side_b_guid": "SIDE-2",
                "posture": "H",
            },
        },
        {
            "function": "ScenEdit_SetScore",
            "arguments": {
                "side": "Blue",
                "score": 250,
                "reason": "Primary objective achieved",
            },
        },
        {
            "function": "ScenEdit_GetEvents",
            "arguments": {"level": 4},
        },
        {
            "function": "ScenEdit_GetEvent",
            "arguments": {"event_id_or_name": "EVENT-1", "level": 4},
        },
        {
            "function": "ScenEdit_SetEvent",
            "arguments": {
                "mode": "add",
                "event_id_or_name": "Enemy detected",
                "active": False,
                "shown": False,
                "repeatable": False,
                "probability": 100,
            },
        },
        {
            "function": "ScenEdit_SetTrigger",
            "arguments": {
                "mode": "add",
                "component_id_or_name": "Every five minutes",
                "component_type": "RegularTime",
                "parameters_json": "{\"Interval\":300}",
            },
        },
        {
            "function": "ScenEdit_SetCondition",
            "arguments": {
                "mode": "add",
                "component_id_or_name": "Scenario started",
                "component_type": "ScenHasStarted",
                "parameters_json": "{}",
            },
        },
        {
            "function": "ScenEdit_SetAction",
            "arguments": {
                "mode": "add",
                "component_id_or_name": "Notify Blue",
                "component_type": "LuaScript",
                "parameters_json": "{\"ScriptText\":\"return true\\r\\n\"}",
            },
        },
        {
            "function": "ScenEdit_SetEventTrigger",
            "arguments": {
                "mode": "add",
                "event_id_or_name": "EVENT-1",
                "component_id_or_name": "TRIGGER-1",
            },
        },
        {
            "function": "ScenEdit_SetEventCondition",
            "arguments": {
                "mode": "add",
                "event_id_or_name": "EVENT-1",
                "component_id_or_name": "CONDITION-1",
            },
        },
        {
            "function": "ScenEdit_SetEventAction",
            "arguments": {
                "mode": "add",
                "event_id_or_name": "EVENT-1",
                "component_id_or_name": "ACTION-1",
            },
        },
        {
            "function": "ScenEdit_AddSpecialAction",
            "arguments": {
                "side_guid": "SIDE-1",
                "name": "Launch reserve",
                "description": "Launch the reserve package.",
                "active": False,
                "repeatable": False,
                "script_text": "return true\r\n",
            },
        },
        {
            "function": "ScenEdit_SetSpecialAction",
            "arguments": {
                "side_guid": "SIDE-1",
                "action_id_or_name": "ACTION-1",
                "mode": "update",
                "active": True,
            },
        },
    ],
    "bridge.uninstall": [{"phase": "files", "uninstall_marker": "verified-marker"}],
    "compat.probe": [
        {"phase": "arm-paused-special-action"},
        {"phase": "collect-paused-special-action"},
    ],
}

INVALID_INVOCATIONS: dict[str, list[tuple[str | None, dict[str, object]]]] = {
    "bridge.status": [(None, {})],
    "bridge.reconcile": [("reconcile_disposition_pair", {"disposition": "applied"})],
    "side.list": [("projection_allowlist", {"fields": ["not_a_side_field"]})],
    "reference_point.list": [("exactly_one_side_selector", {})],
    "unit.list": [("exactly_one_side_selector", {"side_guid": "SIDE-1", "side_name": "Blue"})],
    "unit.get": [("unit_get_selector_form", {})],
    "contact.list": [("exactly_one_side_selector", {})],
    "mission.list": [("exactly_one_side_selector", {})],
    "mission.get": [("mission_get_selector_form", {"side_guid": "SIDE-1"})],
    "doctrine.get": [("doctrine_scope_fields", {"scope": "unit", "unit_guid": "UNIT-1"})],
    "reference_point.update": [
        (
            "at_least_one_update",
            {"side_guid": "SIDE-1", "reference_point_guid": "RP-1"},
        ),
        (
            "reference_point_relative_form",
            {
                "side_guid": "SIDE-1",
                "reference_point_guid": "RP-1",
                "relative_to_type": "unit",
            },
        ),
        (
            "reference_point_relative_form",
            {
                "side_guid": "SIDE-1",
                "reference_point_guid": "RP-1",
                "clear_relative": True,
                "relative_bearing_deg": 30,
            },
        ),
    ],
    "reference_point.add": [
        (
            "reference_point_location_form",
            {"side_guid": "SIDE-1", "name": "Missing position"},
        ),
        (
            "reference_point_location_form",
            {
                "side_guid": "SIDE-1",
                "name": "Mixed position",
                "latitude": 1,
                "longitude": 2,
                "relative_to_type": "unit",
                "relative_to_guid": "UNIT-1",
                "relative_bearing_deg": 90,
                "relative_distance_nm": 10,
            },
        ),
    ],
    "unit.set": [
        ("at_least_one_update", {"unit_guid": "UNIT-1"}),
        (
            "unit_speed_throttle_exclusive",
            {"unit_guid": "UNIT-1", "speed": 450, "throttle": "Military"},
        ),
    ],
    "unit.add": [
        (
            "unit_add_location_form",
            {
                "side_guid": "SIDE-1",
                "unit_type": "Aircraft",
                "dbid": 1,
                "name": "No location",
            },
        ),
        (
            "unit_add_location_form",
            {
                "side_guid": "SIDE-1",
                "unit_type": "Aircraft",
                "dbid": 1,
                "name": "Partial coordinates",
                "latitude": 1,
            },
        ),
        (
            "unit_add_location_form",
            {
                "side_guid": "SIDE-1",
                "unit_type": "Aircraft",
                "dbid": 1,
                "name": "Conflicting location",
                "base_guid": "BASE-1",
                "latitude": 1,
                "longitude": 2,
            },
        ),
    ],
    "unit.refuel": [
        (
            "unit_refuel_tanker_selector",
            {
                "unit_guid": "UNIT-1",
                "tanker_guid": "TANKER-1",
                "tanker_mission_guids": ["MISSION-1"],
            },
        )
    ],
    "unit.attack_contact": [
        (
            "unit_attack_contact_mode_fields",
            {
                "side_guid": "SIDE-1",
                "attacker_unit_guid": "UNIT-1",
                "contact_guid": "CONTACT-1",
                "mode": "manual_weapon",
            },
        ),
        (
            "unit_attack_contact_mode_fields",
            {
                "side_guid": "SIDE-1",
                "attacker_unit_guid": "UNIT-1",
                "contact_guid": "CONTACT-1",
                "mode": "auto",
                "weapon_dbid": 2001,
                "quantity": 1,
            },
        ),
    ],
    "mission.create": [
        (
            "mission_category_parent_pool",
            {
                "side_guid": "SIDE-1",
                "name": "Orphan package",
                "category": "package",
                "details": {
                    "mission_class": "strike",
                    "strike_type": "land",
                },
            },
        ),
        (
            "mission_details_discriminator",
            {
                "side_guid": "SIDE-1",
                "name": "CAP",
                "details": {
                    "mission_class": "patrol",
                    "patrol_type": "aaw",
                    "reference_point_guids": ["RP-1", "RP-2"],
                },
            },
        ),
        (
            "cargo_mission_form",
            {
                "side_guid": "SIDE-1",
                "name": "Invalid cargo transfer",
                "details": {
                    "mission_class": "cargo",
                    "cargo_subtype": "transfer",
                },
            },
        ),
    ],
    "mission.update": [
        ("at_least_one_update", {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"})
    ],
    "mission.air_refueling.update": [
        (
            "at_least_one_update",
            {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"},
        )
    ],
    "mission.flight_plan.create": [
        (
            "mission_flight_plan_schedule_form",
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-1",
                "date_on_target": "2026/07/16",
            },
        ),
        (
            "mission_flight_plan_schedule_form",
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-1",
                "date_on_target": "2026/07/16",
                "time_on_target": "12:00:00",
                "takeoff_date": "2026/07/16",
                "takeoff_time": "10:00:00",
            },
        ),
    ],
    "doctrine.set": [
        ("at_least_one_update", {"scope": "side", "side_guid": "SIDE-1"}),
        (
            None,
            {
                "scope": "side",
                "side_guid": "SIDE-1",
                "engage_opportunity_targets": "true",
            },
        ),
    ],
    "emcon.set": [
        ("emcon_has_clause", {"scope": "unit", "target_guid": "UNIT-1"}),
        (
            "emcon_has_clause",
            {"scope": "unit", "target_guid": "UNIT-1", "emcon": "Inherit=Yes"},
        ),
    ],
    "contact.get": [
        ("exactly_one_side_selector", {"contact_guid": "CONTACT-1"}),
    ],
    "unit.magazine.adjust": [
        (
            "inventory_adjust_mode_fields",
            {
                "unit_guid": "UNIT-1",
                "magazine_guid": "MAG-1",
                "weapon_dbid": 2001,
                "mode": "fill",
                "quantity": 1,
            },
        )
    ],
    "unit.mount_reload.adjust": [
        (
            "inventory_adjust_mode_fields",
            {
                "unit_guid": "UNIT-1",
                "mount_guid": "MOUNT-1",
                "weapon_dbid": 2001,
                "mode": "add",
            },
        )
    ],
    "unit.cargo.transfer": [
        (
            "cargo_transfer_item_form",
            {
                "from_unit_guid": "UNIT-1",
                "to_unit_guid": "UNIT-2",
                "items": [{"cargo_guid": "CARGO-1", "dbid": 4001, "quantity": 1}],
            },
        )
    ],
    "mission.cargo.update": [
        (
            "mission_cargo_form",
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-1",
                "action": "assign",
                "cargo_kind": "mount",
                "dbid": 3001,
            },
        )
    ],
    "doctrine.wra.get": [
        (
            "doctrine_wra_target_selector",
            {
                "scope": "side",
                "side_guid": "SIDE-1",
                "contact_guid": "CONTACT-1",
                "target_type": "Air_Contact_Unknown_Type",
            },
        )
    ],
    "doctrine.wra.set": [
        (
            "doctrine_wra_target_selector",
            {
                "scope": "side",
                "side_guid": "SIDE-1",
                "weapon_dbid": 2001,
                "weapons_per_salvo": "inherit",
                "shooters_per_salvo": "inherit",
                "firing_range": "max",
                "self_defence_range": "inherit",
            },
        )
    ],
    "special_action.list": [
        (
            "at_most_one_side_selector",
            {"side_guid": "SIDE-1", "side_name": "Blue"},
        )
    ],
    "unit.delete": [("trusted_confirmation", {"unit_guid": "UNIT-1"})],
    "mission.delete": [
        ("trusted_confirmation", {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"})
    ],
    "lua.call": [
        ("lua_function_arguments", {"function": "ScenEdit_DeleteUnit", "arguments": {}}),
        ("lua_function_arguments", {"function": "ScenEdit_GetScore", "arguments": {}}),
    ],
    "bridge.uninstall": [
        ("uninstall_phase_marker", {"phase": "command", "uninstall_marker": "marker"})
    ],
    "compat.probe": [
        (
            "compat_phase_case",
            {"phase": "collect-paused-special-action", "case": "high-speed"},
        )
    ],
}

ADDITIONAL_INVALID_INVOCATIONS: dict[
    str, list[tuple[str, str | None, dict[str, object], dict[str, object]]]
] = {
    "bridge.reconcile": [
        (
            "confirmed-missing-proof",
            "trusted_confirmation",
            {
                "request_id": "00000000-0000-0000-0000-000000000002",
                "disposition": "applied",
            },
            {},
        ),
        (
            "confirmed-invalid-proof",
            "trusted_confirmation",
            {
                "request_id": "00000000-0000-0000-0000-000000000002",
                "disposition": "not_applied",
            },
            {"confirmation_proof": "g" * 64},
        ),
    ]
}

PUBLIC_ONLY_INVALID_INVOCATIONS: dict[str, list[tuple[str, str | None, dict[str, object]]]] = {
    "bridge.reconcile": [
        (
            "caller-proof",
            "trusted_confirmation",
            {
                "request_id": "00000000-0000-0000-0000-000000000002",
                "disposition": "applied",
                "confirmation_proof": "b" * 64,
            },
        )
    ]
}

RAW_INVALID_INVOCATIONS: dict[str, list[tuple[str, dict[str, object]]]] = {
    "bridge.reconcile": [
        (
            "explicit-null-all",
            {"request_id": None, "disposition": None, "confirmation_proof": None},
        ),
        (
            "null-request",
            {"request_id": None, "disposition": "applied", "confirmation_proof": "b" * 64},
        ),
        (
            "null-disposition",
            {
                "request_id": "00000000-0000-0000-0000-000000000002",
                "disposition": None,
                "confirmation_proof": "b" * 64,
            },
        ),
        (
            "null-proof",
            {
                "request_id": "00000000-0000-0000-0000-000000000002",
                "disposition": "applied",
                "confirmation_proof": None,
            },
        ),
        ("request-only", {"request_id": "00000000-0000-0000-0000-000000000002"}),
        ("proof-only", {"confirmation_proof": "b" * 64}),
    ]
}


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def load_and_validate_source() -> list[dict[str, object]]:
    value = json.loads(SOURCE_PATH.read_text(encoding="utf-8"))
    if not isinstance(value, list) or not all(isinstance(entry, dict) for entry in value):
        raise ValueError("operations.json must be a list of objects")
    entries = value
    names: set[str] = set()
    for index, entry in enumerate(entries):
        keys = set(entry)
        if missing := REQUIRED_ENTRY_KEYS - keys:
            raise ValueError(f"entry {index} is missing keys: {sorted(missing)}")
        if unsupported := keys - ENTRY_KEYS:
            raise ValueError(f"entry {index} has unsupported keys: {sorted(unsupported)}")
        name = entry["name"]
        if not isinstance(name, str) or name in names:
            raise ValueError(f"entry {index} has an invalid or duplicate name")
        names.add(name)
        if entry["target"] not in {"local", "cmo"}:
            raise ValueError(f"{name}: unsupported target")
        if entry["base_class"] not in {
            "status",
            "read",
            "mutation",
            "destructive",
            "reconcile",
            "dynamic",
        }:
            raise ValueError(f"{name}: unsupported operation class")
        invariants = entry["invariant_ids"]
        if not isinstance(invariants, list) or not all(
            isinstance(item, str) for item in invariants
        ):
            raise ValueError(f"{name}: invariant_ids must be strings")
        if unsupported_invariants := set(invariants) - INVARIANT_IDS:
            raise ValueError(f"{name}: unsupported invariant IDs: {sorted(unsupported_invariants)}")
        _validate_ast(entry["arguments_ast"], f"{name}.arguments_ast")
        _validate_model_reference(entry["public_arguments_model"], name)
        _validate_model_reference(entry["wire_arguments_model"], name)
        if entry["wire_resolver"] not in {
            "model",
            "emcon",
            "reconcile",
            "lua_allowlist",
        }:
            raise ValueError(f"{name}: unsupported wire resolver")
        for factory_field in ("wire_result_factory", "public_result_factory"):
            _validate_result_factory(entry, name, factory_field)

    _validate_surface(entries)
    return entries


def _validate_result_factory(entry: dict[str, object], operation: str, factory_field: str) -> None:
    result_factory = entry[factory_field]
    if isinstance(result_factory, str) and result_factory.startswith("paged:"):
        projected_model = result_factory.partition(":")[2]
        _validate_model_reference(projected_model, operation)
        _validate_projection_allowlist(entry, operation, projected_model)
        return
    if result_factory == "discriminated":
        result_data = _as_string_mapping(
            entry.get("result_factory_data"), f"{operation}.result_factory_data"
        )
        argument_field = result_data.get("argument_field")
        model_mapping = result_data.get("models")
        if not isinstance(argument_field, str) or not isinstance(model_mapping, dict):
            raise ValueError(f"{operation}: invalid discriminated result factory data")
        for result_model in model_mapping.values():
            _validate_model_reference(result_model, operation)
        return
    if result_factory == "reconcile":
        result_data = _as_string_mapping(
            entry.get("result_factory_data"), f"{operation}.result_factory_data"
        )
        if set(result_data) != {"probe", "commit"}:
            raise ValueError(f"{operation}: invalid reconcile result factory data")
        for result_model in result_data.values():
            _validate_model_reference(result_model, operation)
        return
    if result_factory == "lua_allowlist":
        return
    _validate_model_reference(result_factory, operation)


def _as_string_mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be a string-keyed object")
    return value


def _validate_projection_allowlist(
    entry: dict[str, object], operation: str, model_name: str
) -> None:
    model_type = getattr(models, model_name)
    expected = list(model_type.model_fields)
    ast = _as_string_mapping(entry["arguments_ast"], f"{operation}.arguments_ast")
    properties = _as_string_mapping(ast.get("properties"), f"{operation}.properties")
    fields = _as_string_mapping(properties.get("fields"), f"{operation}.fields")
    items = _as_string_mapping(fields.get("items"), f"{operation}.fields.items")
    if items.get("enum") != expected:
        raise ValueError(
            f"{operation}: projection allowlist must exactly match {model_name} fields"
        )


def _validate_model_reference(reference: object, operation: str) -> None:
    if not isinstance(reference, str) or not hasattr(models, reference):
        raise ValueError(f"{operation}: unknown Pydantic model {reference!r}")
    value = getattr(models, reference)
    if not isinstance(value, (type, TypeAliasType)) and getattr(value, "__origin__", None) is None:
        TypeAdapter(value)


def _validate_ast(value: object, path: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be an object")
    if unsupported := set(value) - AST_KEYS:
        raise ValueError(f"{path} uses unsupported schema keywords: {sorted(unsupported)}")
    node_type = value.get("type")
    if node_type not in AST_TYPES:
        raise ValueError(f"{path} has unsupported type {node_type!r}")
    properties = value.get("properties")
    if properties is not None:
        if not isinstance(properties, dict):
            raise ValueError(f"{path}.properties must be an object")
        for key, child in properties.items():
            _validate_ast(child, f"{path}.properties.{key}")
    items = value.get("items")
    if items is not None:
        _validate_ast(items, f"{path}.items")
    one_of = value.get("one_of")
    if one_of is not None:
        if not isinstance(one_of, list) or not one_of:
            raise ValueError(f"{path}.one_of must be a nonempty array")
        for index, child in enumerate(one_of):
            _validate_ast(child, f"{path}.one_of[{index}]")


def _validate_surface(entries: list[dict[str, object]]) -> None:
    actual = {
        "entries": len(entries),
        "cmo": sum(entry["target"] == "cmo" for entry in entries),
        "local": sum(entry["target"] == "local" for entry in entries),
        "mcp": sum(entry["expose_mcp"] is True for entry in entries),
    }
    expected = {"entries": 57, "cmo": 53, "local": 4, "mcp": 49}
    if actual != expected:
        raise ValueError(f"operation surface mismatch: expected {expected}, got {actual}")
    hidden = {str(entry["name"]) for entry in entries if entry["expose_mcp"] is False}
    expected_hidden = {
        "bridge.reconcile",
        "unit.delete",
        "mission.delete",
        "compat.probe.step",
        "bridge.prepare",
        "bridge.doctor",
        "bridge.uninstall",
        "compat.probe",
    }
    if hidden != expected_hidden:
        raise ValueError(f"hidden operation surface mismatch: {sorted(hidden)}")


def render_python(entries: list[dict[str, object]], manifest_hash: str) -> str:
    operations = pprint.pformat(tuple(entries), width=100, sort_dicts=True)
    cmo_names = tuple(str(entry["name"]) for entry in entries if entry["target"] == "cmo")
    mcp_names = tuple(str(entry["name"]) for entry in entries if entry["expose_mcp"] is True)
    compat_entry = next(entry for entry in entries if entry["name"] == "compat.probe.step")
    lua_entry = next(entry for entry in entries if entry["name"] == "lua.call")
    compat_map = pprint.pformat(compat_entry["resolver_data"], width=100, sort_dicts=True)
    lua_allowlist = pprint.pformat(lua_entry["resolver_data"], width=100, sort_dicts=True)
    return (
        '"""Generated from protocol/operations.json; do not edit."""\n\n'
        "from typing import Final\n\n"
        f'MANIFEST_SHA256: Final = "{manifest_hash}"\n'
        f"OPERATIONS: Final[tuple[dict[str, object], ...]] = {operations}\n"
        f"CMO_OPERATION_NAMES: Final = {cmo_names!r}\n"
        f"MCP_OPERATION_NAMES: Final = {mcp_names!r}\n"
        f"COMPAT_PROBE_STEP_CLASSES: Final = {compat_map}\n"
        f"LUA_CALL_ALLOWLIST: Final = {lua_allowlist}\n"
    )


def build_corpus(entries: list[dict[str, object]], manifest_hash: str) -> dict[str, object]:
    from cmo_agent_bridge.operations.registry import OperationRegistry

    registry = OperationRegistry(entries)
    cases: list[dict[str, object]] = []
    for entry in entries:
        name = str(entry["name"])
        valid_arguments = [VALID_ARGUMENTS[name], *ADDITIONAL_VALID_ARGUMENTS.get(name, [])]
        trusted = VALID_TRUSTED_ENRICHMENT.get(name, {})
        for index, valid in enumerate(valid_arguments, start=1):
            invocation = registry.resolve_invocation(name, valid, trusted)
            _assert_raw_parity(registry, entry, invocation)
            suffix = "" if index == 1 else f"-{index}"
            cases.append(
                {
                    "id": f"{name}:valid{suffix}",
                    "operation": name,
                    "surface": "registry-wire",
                    "valid": True,
                    "invariant_id": None,
                    "caller_arguments": valid,
                    "trusted_enrichment": trusted,
                    "wire_arguments": invocation.wire_arguments.model_dump(mode="json"),
                }
            )

        for suffix, valid, case_trusted in ADDITIONAL_VALID_INVOCATIONS.get(name, []):
            invocation = registry.resolve_invocation(name, valid, case_trusted)
            _assert_raw_parity(registry, entry, invocation)
            cases.append(
                {
                    "id": f"{name}:valid-{suffix}",
                    "operation": name,
                    "surface": "registry-wire",
                    "valid": True,
                    "invariant_id": None,
                    "caller_arguments": valid,
                    "trusted_enrichment": case_trusted,
                    "wire_arguments": invocation.wire_arguments.model_dump(mode="json"),
                }
            )

        invalid_extra = dict(VALID_ARGUMENTS[name])
        invalid_extra["__unexpected__"] = True
        _assert_invalid_invocation(registry, name, invalid_extra, trusted, f"{name}:invalid-extra")
        _assert_invalid_raw(
            registry,
            entry,
            _wire_candidate(invalid_extra, trusted),
            f"{name}:invalid-extra",
        )
        cases.append(
            {
                "id": f"{name}:invalid-extra",
                "operation": name,
                "surface": "registry-wire",
                "valid": False,
                "invariant_id": None,
                "caller_arguments": invalid_extra,
                "trusted_enrichment": trusted,
                "wire_arguments": _wire_candidate(invalid_extra, trusted),
            }
        )
        for index, (invariant_id, invalid) in enumerate(INVALID_INVOCATIONS.get(name, []), start=1):
            _assert_invalid_invocation(
                registry, name, invalid, {}, f"{name}:invalid-invariant-{index}"
            )
            _assert_invalid_raw(
                registry,
                entry,
                _wire_candidate(invalid, {}),
                f"{name}:invalid-invariant-{index}",
            )
            cases.append(
                {
                    "id": f"{name}:invalid-invariant-{index}",
                    "operation": name,
                    "surface": "registry-wire",
                    "valid": False,
                    "invariant_id": invariant_id,
                    "caller_arguments": invalid,
                    "trusted_enrichment": {},
                    "wire_arguments": _wire_candidate(invalid, {}),
                }
            )
        for suffix, invariant_id, invalid, case_trusted in ADDITIONAL_INVALID_INVOCATIONS.get(
            name, []
        ):
            case_id = f"{name}:invalid-{suffix}"
            _assert_invalid_invocation(registry, name, invalid, case_trusted, case_id)
            _assert_invalid_raw(
                registry,
                entry,
                _wire_candidate(invalid, case_trusted),
                case_id,
            )
            cases.append(
                {
                    "id": case_id,
                    "operation": name,
                    "surface": "registry-wire",
                    "valid": False,
                    "invariant_id": invariant_id,
                    "caller_arguments": invalid,
                    "trusted_enrichment": case_trusted,
                    "wire_arguments": _wire_candidate(invalid, case_trusted),
                }
            )
        for suffix, invariant_id, invalid in PUBLIC_ONLY_INVALID_INVOCATIONS.get(name, []):
            case_id = f"{name}:invalid-public-{suffix}"
            _assert_invalid_invocation(registry, name, invalid, {}, case_id)
            registry.resolve_wire_invocation(name, _wire_candidate(invalid, {}))
            cases.append(
                {
                    "id": case_id,
                    "operation": name,
                    "surface": "registry-public",
                    "valid": False,
                    "invariant_id": invariant_id,
                    "caller_arguments": invalid,
                    "trusted_enrichment": {},
                    "wire_arguments": _wire_candidate(invalid, {}),
                }
            )
        for suffix, invalid_wire in RAW_INVALID_INVOCATIONS.get(name, []):
            case_id = f"{name}:invalid-raw-{suffix}"
            _assert_invalid_wire(registry, name, invalid_wire, case_id)
            cases.append(
                {
                    "id": case_id,
                    "operation": name,
                    "surface": "raw-wire",
                    "valid": False,
                    "invariant_id": None,
                    "caller_arguments": {},
                    "trusted_enrichment": {},
                    "wire_arguments": invalid_wire,
                }
            )
    return {"manifest_sha256": manifest_hash, "cases": cases}


def _wire_candidate(
    caller_arguments: dict[str, object], trusted_enrichment: dict[str, object]
) -> dict[str, object]:
    candidate = dict(caller_arguments)
    candidate.update(trusted_enrichment)
    return candidate


def _assert_raw_parity(
    registry: "OperationRegistry",
    entry: dict[str, object],
    public: "ResolvedInvocation",
) -> None:
    operation = str(entry["name"])
    wire_arguments = public.wire_arguments.model_dump(mode="json")
    if entry["target"] == "local":
        _assert_invalid_wire(registry, operation, wire_arguments, f"{operation}:local-raw")
        return
    raw = registry.resolve_wire_invocation(operation, wire_arguments)
    if raw.wire_arguments.model_dump(mode="json") != wire_arguments:
        raise ValueError(f"raw wire parity drift for {operation}")
    if raw.effective_class is not public.effective_class:
        raise ValueError(f"raw effective-class parity drift for {operation}")
    if raw.result_schema.schema_id != public.result_schema.schema_id:
        raise ValueError(f"raw result-schema parity drift for {operation}")
    raw_recovery = None if raw.recovery_schema is None else raw.recovery_schema.schema_id
    public_recovery = None if public.recovery_schema is None else public.recovery_schema.schema_id
    if raw_recovery != public_recovery:
        raise ValueError(f"raw recovery-schema parity drift for {operation}")


def _assert_invalid_raw(
    registry: "OperationRegistry",
    entry: dict[str, object],
    wire_arguments: dict[str, object],
    case_id: str,
) -> None:
    _assert_invalid_wire(registry, str(entry["name"]), wire_arguments, case_id)


def _assert_invalid_wire(
    registry: "OperationRegistry",
    operation: str,
    wire_arguments: dict[str, object],
    case_id: str,
) -> None:
    try:
        registry.resolve_wire_invocation(operation, wire_arguments)
    except (ValidationError, BridgeError):
        return
    raise ValueError(f"corpus raw case {case_id} was expected to be invalid")


def _assert_invalid_invocation(
    registry: "OperationRegistry",
    operation: str,
    arguments: dict[str, object],
    trusted_enrichment: dict[str, object],
    case_id: str,
) -> None:
    try:
        registry.resolve_invocation(operation, arguments, trusted_enrichment)
    except (ValidationError, BridgeError):
        return
    raise ValueError(f"corpus case {case_id} was expected to be invalid")


def render_corpus(entries: list[dict[str, object]], manifest_hash: str) -> str:
    return (
        json.dumps(
            build_corpus(entries, manifest_hash),
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def write_or_check(path: Path, content: str, *, check: bool) -> None:
    if check:
        if not path.exists() or path.read_text(encoding="utf-8") != content:
            raise SystemExit(f"generated artifact drift: {path.relative_to(ROOT)}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-python", action="store_true")
    parser.add_argument("--write-corpus", action="store_true")
    parser.add_argument("--check-python", action="store_true")
    parser.add_argument("--check-corpus", action="store_true")
    args = parser.parse_args()
    if not any(vars(args).values()):
        parser.error("select at least one write/check target")

    entries = load_and_validate_source()
    manifest_hash = hashlib.sha256(canonical_json_bytes(entries)).hexdigest()
    python_content = render_python(entries, manifest_hash)
    if args.write_python:
        write_or_check(PYTHON_PATH, python_content, check=False)
    if args.check_python:
        write_or_check(PYTHON_PATH, python_content, check=True)
    if args.write_corpus or args.check_corpus:
        corpus_content = render_corpus(entries, manifest_hash)
        if args.write_corpus:
            write_or_check(CORPUS_PATH, corpus_content, check=False)
        if args.check_corpus:
            write_or_check(CORPUS_PATH, corpus_content, check=True)


if __name__ == "__main__":
    main()
