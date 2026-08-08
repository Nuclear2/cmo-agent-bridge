import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256, OPERATIONS
from cmo_agent_bridge.operations.kinds import ExecutionTarget, OperationClass
from cmo_agent_bridge.operations.models import (
    BridgeStatusResult,
    BridgeStatusWireArgs,
    CompatProbeArgs,
    ConfirmedDeleteUnitWireArgs,
    ContactListArgs,
    DestructivePreviewResult,
    DoctrineGetArgs,
    DoctrineSelector,
    DoctrineSetArgs,
    DoctrineSetResult,
    DoctrineWraSetArgs,
    EmconSetArgs,
    MissionCreateArgs,
    MissionCargoUpdateArgs,
    MissionGetArgs,
    MissionListArgs,
    MissionResult,
    MissionUpdateArgs,
    MissionUpdateResult,
    ReferencePointListArgs,
    ReferencePointUpdateArgs,
    ReconcileArgs,
    ReconciliationResult,
    ScenarioTimeCompressionSetArgs,
    StrikeMissionDetails,
    UnitAttackContactArgs,
    UnitAddArgs,
    UnitCombatStatusResult,
    UnitEngagementOptionsGetArgs,
    UnitEngagementOptionsResult,
    UnitGetArgs,
    UnitLoadoutResult,
    UnitRefuelArgs,
    UnitResult,
    UnitSetArgs,
)
from cmo_agent_bridge.operations.registry import OperationRegistry


EXPECTED_NAMES = {
    "bridge.status",
    "bridge.reconcile",
    "scenario.get",
    "scenario.time_compression.set",
    "side.list",
    "side.losses.get",
    "side.posture.get",
    "reference_point.list",
    "unit.list",
    "unit.catalog",
    "unit.overview",
    "unit.get",
    "unit.combat_status.get",
    "unit.operational_status.batch",
    "unit.loadout.get",
    "unit.engagement_options.get",
    "unit.inventory.get",
    "contact.list",
    "contact.get",
    "contact.posture.set",
    "contact.weapon_allocations.get",
    "mission.list",
    "mission.get",
    "doctrine.get",
    "doctrine.wra.get",
    "special_action.list",
    "reference_point.add",
    "reference_point.update",
    "unit.set",
    "unit.add",
    "unit.assign_mission",
    "unit.unassign_mission",
    "unit.loadout.set",
    "unit.launch",
    "unit.rtb",
    "unit.refuel",
    "unit.attack_contact",
    "unit.sensor.set",
    "unit.magazine.adjust",
    "unit.mount_reload.adjust",
    "unit.cargo.transfer",
    "unit.cargo.unload",
    "mission.create",
    "mission.update",
    "mission.air_refueling.update",
    "mission.flight_plan.list",
    "mission.flight_plan.create",
    "mission.cargo.update",
    "mission.target.add",
    "mission.target.remove",
    "doctrine.set",
    "doctrine.wra.set",
    "emcon.set",
    "special_action.execute",
    "unit.delete",
    "mission.delete",
    "lua.call",
    "compat.probe.step",
    "bridge.prepare",
    "bridge.doctor",
    "bridge.uninstall",
    "compat.probe",
}


def test_operation_kind_values_are_stable() -> None:
    assert [item.value for item in OperationClass] == [
        "status",
        "read",
        "mutation",
        "destructive",
        "reconcile",
        "dynamic",
    ]
    assert [item.value for item in ExecutionTarget] == ["local", "cmo"]


def test_registry_surface_is_locked(registry: OperationRegistry) -> None:
    assert len(registry) == 62
    assert registry.names == EXPECTED_NAMES
    assert registry.count(target="cmo") == 58
    assert registry.count(target="local") == 4
    assert registry.count(expose_mcp=True) == 54
    assert registry.hidden_names == {
        "bridge.reconcile",
        "unit.delete",
        "mission.delete",
        "compat.probe.step",
        "bridge.prepare",
        "bridge.doctor",
        "bridge.uninstall",
        "compat.probe",
    }


def test_local_operation_is_not_emitted_as_lua_handler(registry: OperationRegistry) -> None:
    contract = registry.resolve("bridge.prepare")
    assert contract.target is ExecutionTarget.LOCAL
    assert contract.base_class is OperationClass.MUTATION
    assert contract.expose_mcp is False


def test_unknown_operation_uses_the_public_bridge_error(registry: OperationRegistry) -> None:
    with pytest.raises(BridgeError) as caught:
        registry.resolve("unknown.operation")
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_public_input_is_validated_before_trusted_enrichment(registry: OperationRegistry) -> None:
    candidate = uuid4()
    invocation = registry.resolve_invocation(
        "bridge.status",
        {"accept_lineage_id": None},
        {"activation_candidate": candidate},
    )
    assert invocation.public_arguments.model_dump() == {"accept_lineage_id": None}
    assert isinstance(invocation.wire_arguments, BridgeStatusWireArgs)
    assert invocation.wire_arguments.activation_candidate == candidate

    with pytest.raises(ValidationError):
        registry.resolve_invocation(
            "bridge.status",
            {"activation_candidate": str(candidate)},
            {"activation_candidate": candidate},
        )
    with pytest.raises(BridgeError) as caught:
        registry.resolve_invocation("bridge.status", {}, {"confirmation_proof": "a" * 64})
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_destructive_confirmation_is_host_only(registry: OperationRegistry) -> None:
    with pytest.raises(ValidationError):
        registry.resolve_invocation(
            "unit.delete",
            {"unit_guid": "UNIT-1", "confirmation_proof": "a" * 64},
        )

    invocation = registry.resolve_invocation(
        "unit.delete",
        {"unit_guid": "UNIT-1"},
        {"confirmation_proof": "a" * 64},
    )
    assert isinstance(invocation.wire_arguments, ConfirmedDeleteUnitWireArgs)
    assert invocation.wire_arguments.confirmation_proof == "a" * 64
    assert invocation.effective_class is OperationClass.DESTRUCTIVE


def test_lua_call_is_allowlisted_and_inherits_manifest_class(registry: OperationRegistry) -> None:
    invocation = registry.resolve_invocation(
        "lua.call", {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}}
    )
    assert invocation.effective_class is OperationClass.READ
    invocation.result_adapter.validate_python({"side": "Blue", "score": 12})

    with pytest.raises(BridgeError) as caught:
        registry.resolve_invocation(
            "lua.call", {"function": "ScenEdit_DeleteUnit", "arguments": {}}
        )
    assert caught.value.code is ErrorCode.POLICY_DENIED


@pytest.mark.parametrize(
    ("function", "component_type"),
    [
        ("ScenEdit_SetTrigger", "Message"),
        ("ScenEdit_SetCondition", "UnitDestroyed"),
        ("ScenEdit_SetAction", "ScenHasStarted"),
    ],
)
def test_lua_event_components_reject_cross_kind_types_before_queueing(
    registry: OperationRegistry,
    function: str,
    component_type: str,
) -> None:
    with pytest.raises(ValidationError):
        registry.resolve_invocation(
            "lua.call",
            {
                "function": function,
                "arguments": {
                    "mode": "add",
                    "component_id_or_name": "Invalid component",
                    "component_type": component_type,
                    "parameters_json": "{}",
                },
            },
        )


@pytest.mark.parametrize(
    ("function", "arguments"),
    [
        (
            "ScenEdit_SetTrigger",
            {
                "mode": "add",
                "component_id_or_name": "Invalid score threshold",
                "component_type": "Points",
                "parameters_json": (
                    '{"SideID":"SIDE-1","PointValue":10,"ReachDirection":3}'
                ),
            },
        ),
        (
            "ScenEdit_SetAction",
            {
                "mode": "update",
                "component_id_or_name": "ACTION-1",
                "parameters_json": '{"Text":"Missing type hint"}',
            },
        ),
        (
            "ScenEdit_SetTrigger",
            {
                "mode": "add",
                "component_id_or_name": "Invalid cargo type",
                "component_type": "UnitCargoMoved",
                "parameters_json": '{"CargoFilter":{"TargetType":123}}',
            },
        ),
    ],
)
def test_lua_event_component_parameters_fail_before_queueing(
    registry: OperationRegistry,
    function: str,
    arguments: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        registry.resolve_invocation(
            "lua.call",
            {"function": function, "arguments": arguments},
        )


@pytest.mark.parametrize(
    ("step", "expected"),
    [
        ("observational", OperationClass.READ),
        ("payload", OperationClass.READ),
        ("high-speed", OperationClass.READ),
        ("lineage", OperationClass.READ),
        ("key-value", OperationClass.MUTATION),
        ("dedupe", OperationClass.MUTATION),
        ("indeterminate", OperationClass.MUTATION),
        ("ledger-capacity", OperationClass.MUTATION),
        ("apply-profile", OperationClass.MUTATION),
    ],
)
def test_compat_step_class_is_manifest_driven(
    registry: OperationRegistry, step: str, expected: OperationClass
) -> None:
    arguments: dict[str, object] = {"step": step}
    if step == "payload":
        arguments["candidate_bytes"] = 4096
    elif step == "ledger-capacity":
        arguments["candidate_entries"] = 32
    elif step == "apply-profile":
        arguments.update(
            safe_payload_bytes=4096,
            verified_ledger_entries=32,
            effective_ledger_capacity=32,
        )
    invocation = registry.resolve_invocation("compat.probe.step", arguments)
    assert invocation.effective_class is expected


def test_partial_scenario_result_is_rejected(registry: OperationRegistry) -> None:
    with pytest.raises(ValidationError):
        registry.resolve_invocation("scenario.get", {}).result_adapter.validate_python(
            {"title": "Probe"}
        )


def test_projection_adapter_requires_exact_requested_fields(
    registry: OperationRegistry,
) -> None:
    invocation = registry.resolve_invocation(
        "unit.list", {"side_guid": "SIDE-1", "fields": ["guid", "name"]}
    )
    invocation.result_adapter.validate_python(
        {"items": [{"guid": "UNIT-1", "name": "Alpha"}], "next_cursor": None}
    )
    with pytest.raises(ValidationError):
        invocation.result_adapter.validate_python(
            {
                "items": [{"guid": "UNIT-1", "name": "Alpha", "speed": 12}],
                "next_cursor": None,
            }
        )
    with pytest.raises(ValidationError):
        invocation.result_adapter.validate_python(
            {"items": [{"guid": "", "name": "Alpha"}], "next_cursor": None}
        )
    with pytest.raises(ValidationError):
        registry.resolve_invocation(
            "unit.list", {"side_guid": "SIDE-1", "fields": ["guid", "unknown"]}
        )


def test_projection_fields_are_unique_nonempty_and_keep_guid(
    registry: OperationRegistry,
) -> None:
    for fields in ([], ["name", "name"]):
        with pytest.raises(ValidationError):
            registry.resolve_invocation("unit.list", {"side_guid": "SIDE-1", "fields": fields})
    invocation = registry.resolve_invocation(
        "unit.list", {"side_guid": "SIDE-1", "fields": ["name"]}
    )
    invocation.result_adapter.validate_python(
        {"items": [{"guid": "UNIT-1", "name": "Alpha"}], "next_cursor": None}
    )


def test_unit_discovery_arguments_enforce_page_and_guid_batch_bounds(
    registry: OperationRegistry,
) -> None:
    registry.resolve_invocation(
        "unit.catalog",
        {"side_guid": "SIDE-1", "page_size": 500},
    )
    registry.resolve_invocation(
        "unit.overview",
        {
            "side_guid": "SIDE-1",
            "page_size": 50,
            "unit_guids": [f"UNIT-{index}" for index in range(500)],
        },
    )
    registry.resolve_invocation(
        "unit.operational_status.batch",
        {"unit_guids": [f"UNIT-{index}" for index in range(20)]},
    )

    invalid_invocations: tuple[tuple[str, dict[str, object]], ...] = (
        ("unit.catalog", {"side_guid": "SIDE-1", "page_size": 501}),
        ("unit.overview", {"side_guid": "SIDE-1", "page_size": 51}),
        ("unit.overview", {"side_guid": "SIDE-1", "unit_guids": []}),
        (
            "unit.overview",
            {"side_guid": "SIDE-1", "unit_guids": ["UNIT-1", "UNIT-1"]},
        ),
        (
            "unit.overview",
            {
                "side_guid": "SIDE-1",
                "unit_guids": [f"UNIT-{index}" for index in range(501)],
            },
        ),
        ("unit.operational_status.batch", {"unit_guids": []}),
        (
            "unit.operational_status.batch",
            {"unit_guids": ["UNIT-1", "UNIT-1"]},
        ),
        (
            "unit.operational_status.batch",
            {"unit_guids": [f"UNIT-{index}" for index in range(21)]},
        ),
    )
    for operation, arguments in invalid_invocations:
        with pytest.raises(ValidationError):
            registry.resolve_invocation(operation, arguments)


def test_projection_uses_documented_result_field_names(registry: OperationRegistry) -> None:
    unit = registry.resolve_invocation("unit.list", {"side_guid": "SIDE-1", "fields": ["type"]})
    unit.result_adapter.validate_python(
        {"items": [{"guid": "UNIT-1", "type": "Aircraft"}], "next_cursor": None}
    )
    contact = registry.resolve_invocation(
        "contact.list", {"side_guid": "SIDE-1", "fields": ["type"]}
    )
    contact.result_adapter.validate_python(
        {"items": [{"guid": "CONTACT-1", "type": "Air"}], "next_cursor": None}
    )


def test_mission_read_accepts_every_official_mission_class(
    registry: OperationRegistry,
) -> None:
    invocation = registry.resolve_invocation(
        "mission.get", {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"}
    )
    base_result: dict[str, object] = {
        "guid": "MISSION-1",
        "name": "Mission",
        "side_name": "Blue",
        "active": True,
        "start_time": None,
        "end_time": None,
        "assigned_unit_guids": [],
        "target_guids": [],
        "patrol_type": None,
        "strike_type": None,
        "reference_point_guids": None,
        "destination_guid": None,
        "flight_size": None,
        "one_third_rule": None,
    }
    official_classes = {
        "none": "None",
        "strike": "Strike",
        "patrol": "Patrol",
        "support": "Support",
        "ferry": "Ferry",
        "mining": "Mining",
        "mine_clearing": "MineClearing",
        "escort": "Escort",
        "cargo": "Cargo",
    }

    for normalized, official in official_classes.items():
        result = invocation.result_adapter.validate_python(
            {
                **base_result,
                "mission_class": normalized,
                "mission_class_string": official,
            }
        )
        assert isinstance(result, MissionResult)
        assert result.mission_class == normalized


def test_unit_read_keeps_inapplicable_subtype_as_explicit_null(
    registry: OperationRegistry,
) -> None:
    invocation = registry.resolve_invocation("unit.get", {"unit_guid": "UNIT-1"})
    result = invocation.result_adapter.validate_python(
        {
            "guid": "UNIT-1",
            "dbid": 1,
            "name": "Alpha",
            "side_name": "Blue",
            "type": "Facility",
            "subtype": None,
            "category": "Building",
            "class_name": "Command Post",
            "latitude": 1.0,
            "longitude": 2.0,
            "altitude": 0.0,
            "speed": 0.0,
            "heading": 0.0,
            "throttle": "Full Stop",
            "proficiency": "Regular",
            "fuel_state": "None",
            "weapon_state": "None",
            "unit_state": "Operating",
            "operating": True,
            "mission_guid": None,
            "mission_name": None,
            "loadout_dbid": None,
        }
    )
    assert isinstance(result, UnitResult)
    assert result.subtype is None


def test_unit_operational_read_results_keep_wrapper_details_typed(
    registry: OperationRegistry,
) -> None:
    combat = registry.resolve_invocation("unit.combat_status.get", {"unit_guid": "UNIT-1"})
    combat_result = combat.result_adapter.validate_python(
        {
            "unit_guid": "UNIT-1",
            "operating": True,
            "destroyed": False,
            "sinking": None,
            "condition": "Airborne",
            "condition_code": "0",
            "unit_state": "On Mission",
            "fuel_state": "Joker",
            "weapon_state": "Winchester",
            "ready_time_seconds": None,
            "airborne_time_seconds": 120.5,
            "loadout_dbid": 1001,
            "damage": {
                "dp": 90,
                "start_dp": 100,
                "dp_percent": 10,
                "dp_percent_now": 90,
                "fires": "None",
                "flood": "None",
            },
            "fuels": [
                {
                    "type_code": 2001,
                    "name": "Aviation Fuel",
                    "current": 750,
                    "max": 1000,
                    "percent": 75,
                }
            ],
            "target_contact_guid": "CONTACT-1",
            "firing_at_contact_guids": ["CONTACT-1"],
            "fired_on_by_unit_guids": ["UNIT-2"],
            "targeted_by_unit_guids": ["UNIT-3"],
        }
    )
    assert isinstance(combat_result, UnitCombatStatusResult)
    assert combat_result.fuels[0].type_code == 2001

    loadout = registry.resolve_invocation("unit.loadout.get", {"unit_guid": "UNIT-1"})
    loadout_result = loadout.result_adapter.validate_python(
        {
            "unit_guid": "UNIT-1",
            "loadout_dbid": 1001,
            "name": "Air Superiority",
            "ready_time_seconds": 1800,
            "weapons": [
                {
                    "guid": "WEAPON-LOAD-1",
                    "dbid": 2001,
                    "name": "AAM",
                    "type_code": 2002,
                    "current": 4,
                    "max_capacity": 6,
                    "default": 6,
                }
            ],
        }
    )
    assert isinstance(loadout_result, UnitLoadoutResult)
    assert loadout_result.weapons[0].current == 4

    engagement = registry.resolve_invocation(
        "unit.engagement_options.get",
        {
            "side_guid": "SIDE-1",
            "attacker_unit_guid": "UNIT-1",
            "contact_guid": "CONTACT-1",
        },
    )
    assert engagement.public_arguments == UnitEngagementOptionsGetArgs(
        side_guid="SIDE-1",
        attacker_unit_guid="UNIT-1",
        contact_guid="CONTACT-1",
    )
    engagement_result = engagement.result_adapter.validate_python(
        {
            "attacker_unit_guid": "UNIT-1",
            "contact_guid": "CONTACT-1",
            "contact_name": "Bogey",
            "contact_type": "Air",
            "contact_type_code": 0,
            "target_domain": "air",
            "horizontal_range_nm": 45.0,
            "slant_range_nm": 45.1,
            "requested_quantity": 1,
            "weapon_dbid": 2001,
            "mount_dbid": None,
            "options": [
                {
                    "weapon_dbid": 2001,
                    "weapon_name": "AAM",
                    "weapon_type_code": 2002,
                    "requested_quantity": 1,
                    "available_quantity": 4,
                    "quantity_sufficient": True,
                    "sources": [
                        {
                            "source": "loadout",
                            "mount_guid": None,
                            "mount_dbid": None,
                            "mount_name": None,
                            "mount_status": None,
                            "quantity": 4,
                            "available": True,
                        }
                    ],
                    "nominal_min_range_nm": 2.0,
                    "nominal_max_range_nm": 100.0,
                    "valid_target_types": ["Aircraft"],
                    "range_status": "within_nominal_range",
                    "assessment": "appears_possible",
                    "limitations": [],
                }
            ],
            "limitations": ["Not a complete firing solution."],
        }
    )
    assert isinstance(engagement_result, UnitEngagementOptionsResult)
    assert engagement_result.options[0].assessment == "appears_possible"


def test_unit_command_and_attack_results_require_acceptance(
    registry: OperationRegistry,
) -> None:
    for operation, arguments, command in (
        ("unit.launch", {"unit_guid": "UNIT-1"}, "launch"),
        ("unit.rtb", {"unit_guid": "UNIT-1"}, "rtb"),
        ("unit.refuel", {"unit_guid": "UNIT-1"}, "refuel"),
    ):
        invocation = registry.resolve_invocation(operation, arguments)
        result = {
            "unit_guid": "UNIT-1",
            "command": command,
            "accepted": True,
            "operating": True,
            "condition": "Airborne",
            "condition_code": "0",
            "unit_state": "On Mission",
        }
        invocation.result_adapter.validate_python(result)
        with pytest.raises(ValidationError):
            invocation.result_adapter.validate_python({**result, "accepted": False})

    attack = registry.resolve_invocation(
        "unit.attack_contact",
        {
            "side_guid": "SIDE-1",
            "attacker_unit_guid": "UNIT-1",
            "contact_guid": "CONTACT-1",
            "mode": "auto",
        },
    )
    attack.result_adapter.validate_python(
        {
            "attacker_unit_guid": "UNIT-1",
            "contact_guid": "CONTACT-1",
            "mode": "auto",
            "accepted": True,
            "primary_target_contact_guid": "CONTACT-1",
            "firing_at_contact_guids": [],
            "targeted_by_attacker": True,
        }
    )


def test_mutation_recovery_uses_the_exact_result_adapter(registry: OperationRegistry) -> None:
    invocation = registry.resolve_invocation(
        "unit.assign_mission",
        {
            "unit_guid": "UNIT-1",
            "mission_guid": "MISSION-1",
        },
    )
    result_adapter = invocation.result_adapter
    recovery_adapter = invocation.recovery_adapter
    assert recovery_adapter is not None
    assert recovery_adapter is not result_adapter
    assert recovery_adapter.json_schema() == result_adapter.json_schema()
    valid = {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1", "escort": False}
    recovery_adapter.validate_python(valid)
    with pytest.raises(ValidationError):
        recovery_adapter.validate_python({"unit_guid": "UNIT-1"})


def test_discriminated_results_are_narrowed_to_the_resolved_request(
    registry: OperationRegistry,
) -> None:
    observational = registry.resolve_invocation("compat.probe.step", {"step": "observational"})
    observational.result_adapter.validate_python(
        {
            "step": "observational",
            "nonce": "nonce",
            "poll_source": "automatic",
            "observed": True,
        }
    )
    with pytest.raises(ValidationError):
        observational.result_adapter.validate_python(
            {
                "step": "payload",
                "nonce": "nonce",
                "poll_source": "automatic",
                "payload_bytes": 4096,
            }
        )

    apply_profile = registry.resolve_invocation(
        "compat.probe.step",
        {
            "step": "apply-profile",
            "safe_payload_bytes": 4096,
            "verified_ledger_entries": 32,
            "effective_ledger_capacity": 32,
        },
    )
    result_adapter = apply_profile.result_adapter
    recovery_adapter = apply_profile.recovery_adapter
    assert recovery_adapter is not None
    assert recovery_adapter is not result_adapter
    assert recovery_adapter.json_schema() == result_adapter.json_schema()
    assert apply_profile.recovery_adapter is not None
    with pytest.raises(ValidationError):
        apply_profile.recovery_adapter.validate_python(
            {
                "step": "observational",
                "nonce": "nonce",
                "poll_source": "automatic",
                "observed": True,
            }
        )

    automatic = registry.resolve_invocation("compat.probe", {"phase": "automatic"})
    with pytest.raises(ValidationError):
        automatic.result_adapter.validate_python(
            {
                "phase": "arm-paused-special-action",
                "nonce": "nonce",
                "action_name": "Poll now",
                "instructions": "Run it",
            }
        )

    uninstall = registry.resolve_invocation("bridge.uninstall", {"phase": "command"})
    with pytest.raises(ValidationError):
        uninstall.result_adapter.validate_python(
            {
                "phase": "files",
                "verified_marker": "marker",
                "removed_managed_paths": [],
                "removed_count": 0,
                "retained_nonempty_directories": [],
            }
        )


def test_selector_and_update_invariants_are_strict() -> None:
    for invalid in ({}, {"side_guid": "A", "side_name": "Blue"}):
        with pytest.raises(ValidationError):
            MissionGetArgs.model_validate({**invalid, "mission_guid": "M-1"})

    UnitGetArgs.model_validate({"unit_guid": "U-1"})
    UnitGetArgs.model_validate({"side_name": "Blue", "unit_name": "Alpha"})
    with pytest.raises(ValidationError):
        UnitGetArgs.model_validate({"unit_guid": "U-1", "unit_name": "Alpha"})
    with pytest.raises(ValidationError):
        UnitSetArgs.model_validate({"unit_guid": "U-1"})
    with pytest.raises(ValidationError):
        UnitSetArgs.model_validate({"unit_guid": "U-1", "heading": None})

    ReferencePointListArgs.model_validate({"side_guid": "SIDE-1"})
    with pytest.raises(ValidationError):
        ReferencePointListArgs.model_validate({})
    with pytest.raises(ValidationError):
        ReferencePointUpdateArgs.model_validate(
            {"side_guid": "SIDE-1", "reference_point_guid": "RP-1"}
        )


def test_mutation_scalar_inputs_are_strict_and_finite() -> None:
    assert UnitSetArgs.model_config.get("strict") is True
    assert UnitSetArgs.model_config.get("allow_inf_nan") is False

    for invalid_bool in (0, 1, "false", "true"):
        with pytest.raises(ValidationError):
            UnitSetArgs.model_validate(
                {"unit_guid": "UNIT-1", "force_speed": invalid_bool}
            )

    for invalid_number in (True, False, "12.5", float("inf"), float("-inf"), float("nan")):
        with pytest.raises(ValidationError):
            UnitSetArgs.model_validate(
                {"unit_guid": "UNIT-1", "speed": invalid_number}
            )

    for invalid_integer in (True, False, "2"):
        with pytest.raises(ValidationError):
            UnitAttackContactArgs.model_validate(
                {
                    "side_guid": "SIDE-1",
                    "attacker_unit_guid": "UNIT-1",
                    "contact_guid": "CONTACT-1",
                    "mode": "manual_weapon",
                    "weapon_dbid": 2001,
                    "quantity": invalid_integer,
                }
            )

    for invalid_stage_value in (True, False):
        with pytest.raises(ValidationError):
            MissionUpdateArgs.model_validate(
                {
                    "side_guid": "SIDE-1",
                    "mission_guid": "MISSION-1",
                    "transit_altitude_aircraft": invalid_stage_value,
                }
            )


def test_unit_add_requires_exactly_one_complete_location_form() -> None:
    common = {
        "side_guid": "SIDE-1",
        "unit_type": "Aircraft",
        "dbid": 1,
        "name": "Alpha",
    }
    UnitAddArgs.model_validate({**common, "base_guid": "BASE-1"})
    UnitAddArgs.model_validate({**common, "latitude": 1, "longitude": 2})
    for invalid in (
        common,
        {**common, "latitude": 1},
        {**common, "longitude": 2},
        {**common, "base_guid": "BASE-1", "latitude": 1, "longitude": 2},
    ):
        with pytest.raises(ValidationError):
            UnitAddArgs.model_validate(invalid)


def test_public_enum_inputs_reject_unknown_aliases_before_invocation() -> None:
    for throttle in ("FullStop", "Loiter", "Cruise", "Full", "Flank"):
        UnitSetArgs.model_validate({"unit_guid": "UNIT-1", "throttle": throttle})
    for manual_throttle in ("OFF", "FullStop", "Loiter", "Cruise", "Full", "Flank", 0, 1, 2, 3, 4):
        UnitSetArgs.model_validate({"unit_guid": "UNIT-1", "manual_throttle": manual_throttle})
    for unsupported in ("Military", "Creep", "None", 3, 5, 2.5):
        with pytest.raises(ValidationError):
            UnitSetArgs.model_validate({"unit_guid": "UNIT-1", "throttle": unsupported})
    for unsupported in ("Military", "Creep", 5, 2.5):
        with pytest.raises(ValidationError):
            UnitSetArgs.model_validate({"unit_guid": "UNIT-1", "manual_throttle": unsupported})

    for throttle in ("FullStop", "Loiter", "Cruise", "Full", "Flank", "None", 0, 1, 2, 3, 4, 5):
        MissionUpdateArgs.model_validate(
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-1",
                "attack_throttle_aircraft": throttle,
            }
        )
    for unsupported in ("Military", "Creep", -1, 6, 2.5):
        with pytest.raises(ValidationError):
            MissionUpdateArgs.model_validate(
                {
                    "side_guid": "SIDE-1",
                    "mission_guid": "MISSION-1",
                    "attack_throttle_aircraft": unsupported,
                }
            )

    for unit_type in ("Aircraft", "Ship", "Sub", "Facility", "Satellite", "Weapon"):
        UnitAddArgs.model_validate(
            {
                "side_guid": "SIDE-1",
                "unit_type": unit_type,
                "dbid": 1,
                "name": "Alpha",
                "latitude": 1,
                "longitude": 2,
            }
        )
    for unsupported in ("Air", "Submarine", "Ground unit", "Aimpoint"):
        with pytest.raises(ValidationError):
            UnitAddArgs.model_validate(
                {
                    "side_guid": "SIDE-1",
                    "unit_type": unsupported,
                    "dbid": 1,
                    "name": "Alpha",
                    "latitude": 1,
                    "longitude": 2,
                }
            )

    ContactListArgs.model_validate({"side_guid": "SIDE-1", "contact_type": "Air"})
    MissionListArgs.model_validate({"side_guid": "SIDE-1", "mission_class": "patrol"})
    with pytest.raises(ValidationError):
        ContactListArgs.model_validate({"side_guid": "SIDE-1", "contact_type": "Bogey"})
    with pytest.raises(ValidationError):
        MissionListArgs.model_validate({"side_guid": "SIDE-1", "mission_class": "CAP"})


def test_unit_refuel_selector_is_optional_but_unambiguous() -> None:
    UnitRefuelArgs.model_validate({"unit_guid": "UNIT-1"})
    UnitRefuelArgs.model_validate({"unit_guid": "UNIT-1", "tanker_guid": "TANKER-1"})
    UnitRefuelArgs.model_validate(
        {"unit_guid": "UNIT-1", "tanker_mission_guids": ["MISSION-1", "MISSION-2"]}
    )
    for invalid in (
        {"unit_guid": "UNIT-1", "tanker_mission_guids": list[str]()},
        {
            "unit_guid": "UNIT-1",
            "tanker_guid": "TANKER-1",
            "tanker_mission_guids": ["MISSION-1"],
        },
    ):
        with pytest.raises(ValidationError):
            UnitRefuelArgs.model_validate(invalid)


def test_unit_attack_contact_mode_controls_weapon_allocation_fields() -> None:
    common = {
        "side_guid": "SIDE-1",
        "attacker_unit_guid": "UNIT-1",
        "contact_guid": "CONTACT-1",
    }
    UnitAttackContactArgs.model_validate({**common, "mode": "auto"})
    UnitAttackContactArgs.model_validate({**common, "mode": "manual_target"})
    UnitAttackContactArgs.model_validate(
        {
            **common,
            "mode": "manual_weapon",
            "mount_dbid": 3001,
            "weapon_dbid": 2001,
            "quantity": 2,
            "allow_out_of_nominal_range": True,
        }
    )
    for invalid in (
        {**common, "mode": "manual_weapon"},
        {**common, "mode": "manual_weapon", "weapon_dbid": 2001},
        {**common, "mode": "manual_weapon", "quantity": 2},
        {**common, "mode": "auto", "weapon_dbid": 2001, "quantity": 2},
        {**common, "mode": "manual_target", "mount_dbid": 3001},
        {**common, "mode": "auto", "allow_out_of_nominal_range": True},
    ):
        with pytest.raises(ValidationError):
            UnitAttackContactArgs.model_validate(invalid)


def test_scope_phase_and_emcon_invariants_are_strict() -> None:
    DoctrineSelector.model_validate({"scope": "side", "side_guid": "SIDE-1"})
    actual = DoctrineGetArgs.model_validate(
        {"scope": "mission", "side_guid": "SIDE-1", "mission_guid": "MISSION-1", "actual": True}
    )
    assert actual.actual is True
    inherited_actual = DoctrineGetArgs.model_validate({"scope": "side", "side_guid": "SIDE-1"})
    assert inherited_actual.actual is True
    with pytest.raises(ValidationError):
        DoctrineSelector.model_validate({"scope": "unit", "unit_guid": "UNIT-1"})
    with pytest.raises(ValidationError):
        CompatProbeArgs.model_validate(
            {"phase": "collect-paused-special-action", "case": "high-speed"}
        )
    ReconcileArgs.model_validate({})
    with pytest.raises(ValidationError):
        ReconcileArgs.model_validate({"disposition": "applied"})
    with pytest.raises(ValidationError):
        EmconSetArgs.model_validate({"scope": "unit", "target_guid": "U-1"})


def test_doctrine_set_uses_official_allowlisted_values() -> None:
    DoctrineSetArgs.model_validate(
        {
            "scope": "side",
            "side_guid": "SIDE-1",
            "weapon_control_air": "Hold",
            "nuclear_use": True,
            "refuel_unrep": "Never",
            "engaging_ambiguous_targets": "Optimistic",
            "fuel_state_planned": "Joker40Percent",
            "fuel_state_rtb": "YesLeaveGroup",
            "weapon_state_planned": "Winchester",
            "weapon_state_rtb": "YesLastUnit",
            "withdraw_on_attack": "Percent50",
            "withdraw_on_damage": "Percent25",
            "withdraw_on_defence": "Exhausted",
            "withdraw_on_fuel": "Bingo",
            "bvr_logic": "Crank",
            "dipping_sonar": "ManualAndMissionOnly",
            "use_aip": "Yes_AttackOnly",
            "recharge_on_attack": 30,
            "recharge_on_patrol": "Recharge_60_Percent",
        }
    )
    inherited = DoctrineSetArgs.model_validate(
        {
            "scope": "unit",
            "side_guid": "SIDE-1",
            "unit_guid": "UNIT-1",
            "weapon_control_air": "inherit",
        }
    )
    assert inherited.weapon_control_air == "inherit"
    with pytest.raises(ValidationError):
        DoctrineSetArgs.model_validate(
            {"scope": "side", "side_guid": "SIDE-1", "refuel_unrep": "Allowed"}
        )
    for field_name in (
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
    ):
        with pytest.raises(ValidationError):
            DoctrineSetArgs.model_validate(
                {
                    "scope": "side",
                    "side_guid": "SIDE-1",
                    field_name: "GuessMe",
                }
            )


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("flight_size", True),
        ("flight_size", 3.0),
        ("minimum_aircraft_required", False),
        ("minimum_aircraft_required", 4.0),
        ("group_size", 5),
        ("group_size", True),
        ("loop_type", 1.0),
        ("laying_method", False),
        ("attack_throttle_aircraft", True),
        ("attack_throttle_aircraft", 3.0),
        ("strike_min_distance_aircraft", -1),
        ("strike_min_distance_aircraft", "20"),
        ("strike_minimum_trigger", "hostile"),
        ("strike_minimum_trigger", "3"),
        ("strike_minimum_trigger", True),
        ("strike_minimum_trigger", 3.0),
        ("arming_delay", "0:24:00:00"),
        ("arming_delay", "0:00:60:00"),
        ("arming_delay", "00:00:00"),
    ],
)
def test_mission_update_rejects_noncanonical_finite_values(
    field_name: str,
    invalid_value: object,
) -> None:
    with pytest.raises(ValidationError):
        MissionUpdateArgs.model_validate(
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-1",
                field_name: invalid_value,
            }
        )


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("weapon_control_air", True),
        ("weapon_control_air", 1.0),
        ("nuclear_use", 1),
        ("refuel_unrep", True),
        ("refuel_unrep", 2.0),
        ("engaging_ambiguous_targets", False),
        ("fuel_state_planned", 4.0),
        ("weapon_state_planned", 2001.0),
        ("withdraw_on_fuel", True),
        ("bvr_logic", 1.0),
        ("dipping_sonar", False),
        ("recharge_on_patrol", 30.0),
    ],
)
def test_doctrine_update_codes_are_strict_integers(
    field_name: str,
    invalid_value: object,
) -> None:
    with pytest.raises(ValidationError):
        DoctrineSetArgs.model_validate(
            {
                "scope": "side",
                "side_guid": "SIDE-1",
                field_name: invalid_value,
            }
        )


def test_finite_integer_selectors_reject_booleans_and_integral_floats() -> None:
    for invalid_code in (True, 2.0):
        with pytest.raises(ValidationError):
            ScenarioTimeCompressionSetArgs.model_validate({"code": invalid_code})
    with pytest.raises(ValidationError):
        MissionCargoUpdateArgs.model_validate(
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-1",
                "action": "assign",
                "cargo_kind": "object",
                "dbid": 6001,
                "object_type": 1,
                "cargo_guid": "CARGO-1",
            }
        )
    with pytest.raises(ValidationError):
        DoctrineSelector.model_validate({"scope": "side", "side_guid": ""})


def test_doctrine_boolean_updates_accept_only_bool_or_inherit() -> None:
    inherited = DoctrineSetArgs.model_validate(
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
        }
    )
    assert inherited.engage_opportunity_targets == "inherit"
    assert inherited.use_sams_in_anti_surface_mode == "inherit"

    for unsupported in ("true", "default", 1):
        with pytest.raises(ValidationError):
            DoctrineSetArgs.model_validate(
                {
                    "scope": "side",
                    "side_guid": "SIDE-1",
                    "engage_opportunity_targets": unsupported,
                }
            )

    with pytest.raises(ValidationError):
        DoctrineSetResult.model_validate(
            {
                "scope": "side",
                "target_guid": "SIDE-1",
                "weapon_control_air": None,
                "weapon_control_surface": None,
                "weapon_control_subsurface": None,
                "weapon_control_land": None,
                "nuclear_use": None,
                "refuel_unrep": None,
                "engage_opportunity_targets": "inherit",
            }
        )


def test_doctrine_wra_set_uses_field_specific_official_values() -> None:
    canonical = DoctrineWraSetArgs.model_validate(
        {
            "scope": "side",
            "side_guid": "SIDE-1",
            "target_type": "Air_Contact_Unknown_Type",
            "weapon_dbid": 2001,
            "weapons_per_salvo": "system",
            "shooters_per_salvo": 0,
            "firing_range": "75ofmax",
            "self_defence_range": 12.5,
        }
    )
    assert canonical.weapons_per_salvo == "system"
    assert canonical.shooters_per_salvo == 0
    assert canonical.firing_range == "75ofmax"
    assert canonical.self_defence_range == 12.5

    common = {
        "scope": "side",
        "side_guid": "SIDE-1",
        "target_type": "Air_Contact_Unknown_Type",
        "weapon_dbid": 2001,
        "weapons_per_salvo": "inherit",
        "shooters_per_salvo": "max",
        "firing_range": "none",
        "self_defence_range": "system",
    }
    for field_name, invalid_value in (
        ("weapons_per_salvo", "automatic"),
        ("weapons_per_salvo", "SYSTEM"),
        ("weapons_per_salvo", True),
        ("shooters_per_salvo", -1),
        ("firing_range", "system"),
        ("firing_range", "100ofmax"),
        ("self_defence_range", -0.1),
    ):
        with pytest.raises(ValidationError):
            DoctrineWraSetArgs.model_validate({**common, field_name: invalid_value})


def test_emcon_wire_adapter_emits_official_grammar(registry: OperationRegistry) -> None:
    invocation = registry.resolve_invocation(
        "emcon.set",
        {
            "scope": "unit",
            "target_guid": "UNIT-1",
            "inherit": True,
            "radar": "Active",
            "oecm": "Passive",
        },
    )
    assert invocation.wire_arguments.model_dump() == {
        "scope": "unit",
        "target_guid": "UNIT-1",
        "emcon": "Inherit;Radar=Active;OECM=Passive",
    }
    inherit_only = registry.resolve_invocation(
        "emcon.set",
        {"scope": "unit", "target_guid": "UNIT-1", "inherit": True},
    )
    assert inherit_only.wire_arguments.model_dump() == {
        "scope": "unit",
        "target_guid": "UNIT-1",
        "emcon": "Inherit",
    }
    with pytest.raises(ValidationError):
        registry.resolve_invocation(
            "emcon.set",
            {"scope": "unit", "target_guid": "UNIT-1", "emcon": "Radar=Active"},
        )


def test_mission_details_are_discriminated_and_bounded() -> None:
    MissionCreateArgs.model_validate(
        {
            "side_guid": "SIDE-1",
            "name": "CAP",
            "details": {
                "mission_class": "patrol",
                "patrol_type": "aaw",
                "reference_point_guids": ["RP-1", "RP-2", "RP-3"],
            },
        }
    )


def test_mission_routes_and_targets_have_only_the_brief_bounds() -> None:
    MissionCreateArgs.model_validate(
        {
            "side_guid": "SIDE-1",
            "name": "Patrol revisits a point",
            "details": {
                "mission_class": "patrol",
                "patrol_type": "aaw",
                "reference_point_guids": ["RP-1", "RP-1", "RP-2"],
            },
        }
    )
    MissionCreateArgs.model_validate(
        {
            "side_guid": "SIDE-1",
            "name": "Support revisits a point",
            "details": {
                "mission_class": "support",
                "reference_point_guids": ["RP-1", "RP-1"],
            },
        }
    )
    MissionCreateArgs.model_validate(
        {
            "side_guid": "SIDE-1",
            "name": "Large strike",
            "details": {
                "mission_class": "strike",
                "strike_type": "air",
                "target_guids": [f"TARGET-{index}" for index in range(65)],
            },
        }
    )
    MissionCreateArgs.model_validate(
        {
            "side_guid": "SIDE-1",
            "name": "Repeated target",
            "details": {
                "mission_class": "strike",
                "strike_type": "air",
                "target_guids": ["TARGET-1", "TARGET-1"],
            },
        }
    )


def test_mission_update_preserves_ordered_zones_and_explicit_clear() -> None:
    update = MissionUpdateArgs.model_validate(
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "reference_point_guids": ["RP-3", "RP-1", "RP-2", "RP-1"],
            "prosecution_zone_reference_point_guids": [],
        }
    )
    assert update.reference_point_guids == ["RP-3", "RP-1", "RP-2", "RP-1"]
    assert update.prosecution_zone_reference_point_guids == []
    with pytest.raises(ValidationError):
        MissionUpdateArgs.model_validate(
            {"side_guid": "SIDE-1", "mission_guid": "MISSION-1", "start_time": None}
        )
    with pytest.raises(ValidationError):
        MissionUpdateArgs.model_validate(
            {"side_guid": "SIDE-1", "mission_guid": "MISSION-1", "flight_size": 5}
        )


def test_mission_loop_type_keeps_bounded_integer_input_and_scalar_result() -> None:
    for loop_type in (0, 2):
        update = MissionUpdateArgs.model_validate(
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-1",
                "loop_type": loop_type,
            }
        )
        assert update.loop_type == loop_type

    for unsupported in ("RepeatableLoop", 3):
        with pytest.raises(ValidationError):
            MissionUpdateArgs.model_validate(
                {
                    "side_guid": "SIDE-1",
                    "mission_guid": "MISSION-1",
                    "loop_type": unsupported,
                }
            )

    result_base: dict[str, object] = {
        "mission_guid": "MISSION-1",
        "name": "Mission",
        "active": True,
        "start_time": None,
        "end_time": None,
        "flight_size": None,
        "one_third_rule": None,
        "reference_point_guids": None,
        "prosecution_zone_reference_point_guids": None,
    }
    for loop_type in (1, "RepeatableLoop"):
        result = MissionUpdateResult.model_validate({**result_base, "loop_type": loop_type})
        assert result.loop_type == loop_type

    mission = MissionResult.model_validate(
        {
            "guid": "MISSION-1",
            "name": "Mission",
            "side_name": "Blue",
            "mission_class": "patrol",
            "mission_class_string": "Patrol",
            "active": True,
            "start_time": None,
            "end_time": None,
            "assigned_unit_guids": [],
            "target_guids": [],
            "patrol_type": "aaw",
            "strike_type": None,
            "reference_point_guids": None,
            "destination_guid": None,
            "flight_size": None,
            "one_third_rule": None,
            "loop_type": "RandomInArea",
        }
    )
    assert mission.loop_type == "RandomInArea"


def test_strike_mission_can_be_created_without_preplanned_targets() -> None:
    arguments = MissionCreateArgs.model_validate(
        {
            "side_guid": "SIDE-1",
            "name": "Dynamic Strike",
            "details": {"mission_class": "strike", "strike_type": "land"},
        }
    )

    assert isinstance(arguments.details, StrikeMissionDetails)
    assert arguments.details.target_guids == []


def test_doctrine_update_rejects_null_only_change() -> None:
    with pytest.raises(ValidationError):
        DoctrineSetArgs.model_validate(
            {"scope": "side", "side_guid": "SIDE-1", "weapon_control_air": None}
        )


def test_new_mutation_result_contracts_verify_actual_terminal_state(
    registry: OperationRegistry,
) -> None:
    reference_point = registry.resolve_invocation(
        "reference_point.add",
        {
            "side_guid": "SIDE-1",
            "name": "CAP-1",
            "latitude": 1,
            "longitude": 2,
        },
    )
    reference_point.result_schema.adapter.validate_python(
        {
            "guid": "RP-1",
            "name": "CAP-1",
            "side_guid": "SIDE-1",
            "latitude": 1,
            "longitude": 2,
        }
    )

    unassign = registry.resolve_invocation("unit.unassign_mission", {"unit_guid": "UNIT-1"})
    unassign.result_schema.adapter.validate_python(
        {"unit_guid": "UNIT-1", "mission_guid": None, "escort": False}
    )
    with pytest.raises(ValidationError):
        unassign.result_schema.adapter.validate_python(
            {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1", "escort": False}
        )

    target_add = registry.resolve_invocation(
        "mission.target.add",
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "target_guid": "TARGET-1",
        },
    )
    target_add.result_schema.adapter.validate_python(
        {
            "mission_guid": "MISSION-1",
            "target_guid": "TARGET-1",
            "assigned": True,
            "target_guids": ["TARGET-1"],
        }
    )
    with pytest.raises(ValidationError):
        target_add.result_schema.adapter.validate_python(
            {
                "mission_guid": "MISSION-1",
                "target_guid": "TARGET-1",
                "assigned": False,
                "target_guids": [],
            }
        )

    target_remove = registry.resolve_invocation(
        "mission.target.remove",
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "target_guid": "TARGET-1",
        },
    )
    target_remove.result_schema.adapter.validate_python(
        {
            "mission_guid": "MISSION-1",
            "target_guid": "TARGET-1",
            "assigned": False,
            "target_guids": [],
        }
    )


def test_empty_course_is_a_typed_clear_update() -> None:
    UnitSetArgs.model_validate({"unit_guid": "UNIT-1", "course": []})


def test_apply_profile_candidates_use_independent_brief_bounds(
    registry: OperationRegistry,
) -> None:
    registry.resolve_invocation(
        "compat.probe.step",
        {
            "step": "apply-profile",
            "safe_payload_bytes": 4096,
            "verified_ledger_entries": 1,
            "effective_ledger_capacity": 2,
        },
    )
    with pytest.raises(ValidationError):
        MissionCreateArgs.model_validate(
            {
                "side_guid": "SIDE-1",
                "name": "CAP",
                "details": {
                    "mission_class": "patrol",
                    "patrol_type": "aaw",
                    "reference_point_guids": ["RP-1", "RP-2"],
                },
            }
        )
    with pytest.raises(ValidationError):
        MissionCreateArgs.model_validate(
            {
                "side_guid": "SIDE-1",
                "name": "Display label is not a wire subtype",
                "details": {
                    "mission_class": "patrol",
                    "patrol_type": "AAW",
                    "reference_point_guids": ["RP-1", "RP-2", "RP-3"],
                },
            }
        )
    MissionCreateArgs.model_validate(
        {
            "side_guid": "SIDE-1",
            "name": "Strike",
            "details": {
                "mission_class": "strike",
                "strike_type": "air",
                "target_guids": ["TARGET-1"],
            },
        }
    )


def test_status_capacities_are_required_bounded_and_related() -> None:
    base = {
        "protocol": "cmo-agent-bridge/1",
        "runtime_version": "0.1.0",
        "runtime_tag": f"0_1_0-{'b' * 64}",
        "runtime_asset_sha256": "b" * 64,
        "release_id": "c" * 64,
        "build": 1868,
        "manifest_sha256": "a" * 64,
        "lineage_id": str(uuid4()),
        "activation_id": str(uuid4()),
        "installed_event_names": ["Initialize", "Poll"],
        "installed_action_names": ["Initialize", "Poll"],
        "installed_trigger_names": ["Loaded", "Regular"],
        "pending_request_id": None,
        "quarantined": False,
        "paused_capability": None,
        "poll_interval_seconds": 5,
        "safe_payload_bytes": 4096,
        "verified_ledger_entries": 32,
        "effective_ledger_capacity": 32,
    }
    BridgeStatusResult.model_validate(base)
    for field in (
        "runtime_tag",
        "runtime_asset_sha256",
        "release_id",
        "safe_payload_bytes",
        "verified_ledger_entries",
        "effective_ledger_capacity",
    ):
        invalid = dict(base)
        invalid.pop(field)
        with pytest.raises(ValidationError):
            BridgeStatusResult.model_validate(invalid)
    with pytest.raises(ValidationError):
        BridgeStatusResult.model_validate({**base, "effective_ledger_capacity": 33})


def test_confirmation_expiries_require_utc() -> None:
    preview = {
        "operation": "unit.delete",
        "target_guid": "UNIT-1",
        "target_name": "Alpha",
        "target_type": "Aircraft",
        "impact": "Deletes the unit",
        "reserved_activation_candidate": str(uuid4()),
        "confirmation_token": "token",
    }
    DestructivePreviewResult.model_validate(
        {**preview, "expires_at_utc": datetime(2026, 1, 1, tzinfo=timezone.utc)}
    )
    for invalid in (
        datetime(2026, 1, 1),
        datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=8))),
    ):
        with pytest.raises(ValidationError):
            DestructivePreviewResult.model_validate({**preview, "expires_at_utc": invalid})

    evidence = {"present": False, "state": None, "request_hash": None}
    reconciliation: dict[str, object] = {
        "request_id": None,
        "journal_evidence": evidence,
        "barrier_evidence": evidence,
        "ledger_evidence": evidence,
        "allowed_dispositions": [],
        "applied_disposition": None,
        "quarantined": False,
        "confirmation_token": None,
        "reserved_activation_candidate": None,
    }
    with pytest.raises(ValidationError):
        ReconciliationResult.model_validate(
            {**reconciliation, "confirmation_expires_at_utc": datetime(2026, 1, 1)}
        )


def test_generated_manifest_is_canonical_and_complete() -> None:
    root = Path(__file__).parents[3]
    raw = json.loads((root / "protocol" / "operations.json").read_text(encoding="utf-8"))
    canonical = json.dumps(
        raw, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode()
    assert MANIFEST_SHA256 == hashlib.sha256(canonical).hexdigest()
    assert len(OPERATIONS) == 62
    assert {entry["name"] for entry in OPERATIONS} == EXPECTED_NAMES
