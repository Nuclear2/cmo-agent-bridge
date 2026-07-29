from __future__ import annotations

import json
from collections.abc import Callable

import pytest
from pydantic import BaseModel, ValidationError

from cmo_agent_bridge.operations.scenario_authoring import (
    EventActionSetArgs,
    EventComponentLinkArgs,
    EventComponentSetArgs,
    EventListArgs,
    EventSetArgs,
    EventTriggerSetArgs,
    ScenarioTimelineSetArgs,
    ScenarioTitleSetArgs,
    ScenarioWeatherSetArgs,
    SideOptionsSetArgs,
    SpecialActionSetArgs,
    normalize_script_newlines,
    validate_event_component_parameters,
)


def test_authoring_models_forbid_extra_fields_and_are_frozen() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ScenarioTitleSetArgs.model_validate({"title": "Test", "unexpected": True})

    model = ScenarioTitleSetArgs(title="Test")
    with pytest.raises(ValidationError, match="Instance is frozen"):
        setattr(model, "title", "Changed")


@pytest.mark.parametrize(
    "build",
    [
        lambda: ScenarioWeatherSetArgs.model_validate(
            {
                "temperature_c": 20,
                "rainfall": 51,
                "undercloud_fraction": 0,
                "sea_state": 0,
            }
        ),
        lambda: ScenarioWeatherSetArgs.model_validate(
            {
                "temperature_c": 20,
                "rainfall": 0,
                "undercloud_fraction": 1.1,
                "sea_state": 0,
            }
        ),
        lambda: ScenarioWeatherSetArgs.model_validate(
            {
                "temperature_c": 20,
                "rainfall": 0,
                "undercloud_fraction": 0,
                "sea_state": 10,
            }
        ),
    ],
)
def test_weather_model_enforces_official_ranges(build: Callable[[], BaseModel]) -> None:
    with pytest.raises(ValidationError):
        build()


def test_weather_model_accepts_negative_baseline_temperature() -> None:
    assert (
        ScenarioWeatherSetArgs(
            temperature_c=-35,
            rainfall=0,
            undercloud_fraction=0.2,
            sea_state=2,
        ).temperature_c
        == -35
    )


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (
            ScenarioWeatherSetArgs,
            {
                "temperature_c": "20",
                "rainfall": 0,
                "undercloud_fraction": 0.2,
                "sea_state": 2,
            },
        ),
        (
            ScenarioWeatherSetArgs,
            {
                "temperature_c": 20,
                "rainfall": True,
                "undercloud_fraction": 0.2,
                "sea_state": 2,
            },
        ),
        (
            ScenarioWeatherSetArgs,
            {
                "temperature_c": float("nan"),
                "rainfall": 0,
                "undercloud_fraction": 0.2,
                "sea_state": 2,
            },
        ),
        (
            ScenarioWeatherSetArgs,
            {
                "temperature_c": 20,
                "rainfall": float("inf"),
                "undercloud_fraction": 0.2,
                "sea_state": 2,
            },
        ),
        (
            EventSetArgs,
            {
                "mode": "update",
                "event_id_or_name": "EVENT-1",
                "active": "false",
            },
        ),
        (
            EventSetArgs,
            {
                "mode": "update",
                "event_id_or_name": "EVENT-1",
                "probability": True,
            },
        ),
        (
            EventSetArgs,
            {
                "mode": "update",
                "event_id_or_name": "EVENT-1",
                "probability": "75",
            },
        ),
    ],
)
def test_authoring_models_reject_coercion_and_non_finite_numbers(
    model: type[BaseModel],
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(payload)


def test_timeline_and_side_option_models_require_an_actual_update() -> None:
    with pytest.raises(ValidationError, match="at least one scenario timeline update"):
        ScenarioTimelineSetArgs()
    with pytest.raises(ValidationError, match="at least one side option update"):
        SideOptionsSetArgs(side_guid="SIDE-1")
    with pytest.raises(ValidationError):
        ScenarioTimelineSetArgs(current_time="2026/07/16 12:00:00")

    assert (
        ScenarioTimelineSetArgs(
            current_time="2026-07-16T12:00:00",
            duration="1:23:59",
        ).duration
        == "1:23:59"
    )
    assert (
        SideOptionsSetArgs(
            side_guid="SIDE-1",
            computer_controlled_only=False,
        ).computer_controlled_only
        is False
    )
    with pytest.raises(ValidationError, match="days:hours:minutes"):
        ScenarioTimelineSetArgs(duration="1:24:00")
    with pytest.raises(ValidationError):
        ScenarioTimelineSetArgs(current_time="2026-02-30T12:00:00")


def test_side_option_models_use_official_awareness_and_proficiency_values() -> None:
    named = SideOptionsSetArgs(
        side_guid="SIDE-1",
        awareness="AutoSideAndUnitID",
        proficiency="Veteran",
    )
    coded = SideOptionsSetArgs(side_guid="SIDE-1", awareness=-1, proficiency=4)
    assert named.awareness == "AutoSideAndUnitID"
    assert named.proficiency == "Veteran"
    assert coded.awareness == -1
    assert coded.proficiency == 4

    for field_name, invalid_value in (
        ("awareness", "Godlike"),
        ("awareness", 4),
        ("awareness", True),
        ("awareness", 1.0),
        ("proficiency", "Elite"),
        ("proficiency", 5),
        ("proficiency", False),
        ("proficiency", 4.0),
    ):
        with pytest.raises(ValidationError):
            SideOptionsSetArgs.model_validate({"side_guid": "SIDE-1", field_name: invalid_value})

    for invalid_level in (True, 2.0):
        with pytest.raises(ValidationError):
            EventListArgs.model_validate({"level": invalid_level})


@pytest.mark.parametrize(
    ("model", "payload", "message"),
    [
        (
            EventSetArgs,
            {"mode": "update", "event_id_or_name": "EVENT-1"},
            "event update requires at least one changed field",
        ),
        (
            EventSetArgs,
            {
                "mode": "remove",
                "event_id_or_name": "EVENT-1",
                "active": False,
            },
            "event removal forbids update fields",
        ),
        (
            EventComponentSetArgs,
            {
                "mode": "add",
                "component_id_or_name": "Trigger 1",
            },
            "component add requires component_type",
        ),
        (
            EventComponentSetArgs,
            {
                "mode": "remove",
                "component_id_or_name": "TRIGGER-1",
                "parameters_json": '{"Key":"Value"}',
            },
            "list and remove forbid component parameters",
        ),
        (
            EventComponentSetArgs,
            {
                "mode": "update",
                "component_id_or_name": "TRIGGER-1",
                "parameters_json": "[]",
            },
            "parameters_json must contain a JSON object",
        ),
        (
            EventComponentSetArgs,
            {
                "mode": "update",
                "component_id_or_name": "TRIGGER-1",
                "parameters_json": '{"Value":null}',
            },
            "parameters_json must not contain JSON null",
        ),
        (
            EventComponentLinkArgs,
            {
                "mode": "replace",
                "event_id_or_name": "EVENT-1",
                "component_id_or_name": "TRIGGER-1",
            },
            "replacement_id_or_name is required only for replace",
        ),
        (
            EventComponentLinkArgs,
            {
                "mode": "add",
                "event_id_or_name": "EVENT-1",
                "component_id_or_name": "TRIGGER-1",
                "replacement_id_or_name": "TRIGGER-2",
            },
            "replacement_id_or_name is required only for replace",
        ),
        (
            SpecialActionSetArgs,
            {
                "side_guid": "SIDE-1",
                "action_id_or_name": "ACTION-1",
            },
            "special-action update requires at least one changed field",
        ),
        (
            SpecialActionSetArgs,
            {
                "side_guid": "SIDE-1",
                "action_id_or_name": "ACTION-1",
                "mode": "remove",
                "active": False,
            },
            "special-action removal forbids update fields",
        ),
    ],
)
def test_authoring_models_enforce_cross_field_rules(
    model: type[BaseModel],
    payload: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        model.model_validate(payload)


def test_event_component_update_parameters_require_type_and_validate_exact_fields() -> None:
    with pytest.raises(
        ValidationError,
        match="component update parameters require component_type",
    ):
        EventActionSetArgs.model_validate(
            {
                "mode": "update",
                "component_id_or_name": "ACTION-1",
                "parameters_json": '{"Text":"Updated"}',
            }
        )

    valid = EventActionSetArgs.model_validate(
        {
            "mode": "update",
            "component_id_or_name": "ACTION-1",
            "component_type": "Message",
            "parameters_json": '{"SideID":"SIDE-1","Text":"Updated"}',
        }
    )
    assert valid.component_type == "Message"

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        EventActionSetArgs.model_validate(
            {
                "mode": "update",
                "component_id_or_name": "ACTION-1",
                "component_type": "LuaScript",
                "parameters_json": '{"Text":"not a LuaScript field"}',
            }
        )


def test_event_component_add_requires_safe_minimum_parameters() -> None:
    with pytest.raises(
        ValidationError,
        match="action Message add requires parameters: SideID, Text",
    ):
        EventActionSetArgs.model_validate(
            {
                "mode": "add",
                "component_id_or_name": "Empty message",
                "component_type": "Message",
            }
        )

    for limit_field in ("TargetLimitReceived", "TargetLimitSent"):
        valid = EventTriggerSetArgs.model_validate(
            {
                "mode": "add",
                "component_id_or_name": f"Cargo {limit_field}",
                "component_type": "UnitCargoMoved",
                "parameters_json": json.dumps(
                    {
                        "CargoFilter": {"TargetType": "Personnel"},
                        limit_field: 0,
                    }
                ),
            }
        )
        assert valid.component_type == "UnitCargoMoved"

    with pytest.raises(
        ValidationError,
        match="requires at least one of TargetLimitReceived or TargetLimitSent",
    ):
        EventTriggerSetArgs.model_validate(
            {
                "mode": "add",
                "component_id_or_name": "Cargo without limit",
                "component_type": "UnitCargoMoved",
                "parameters_json": json.dumps(
                    {"CargoFilter": {"TargetType": "Personnel"}}
                ),
            }
        )


def test_event_component_update_requires_rename_or_non_empty_parameters() -> None:
    with pytest.raises(
        ValidationError,
        match="component update requires new_description or non-empty parameters",
    ):
        EventActionSetArgs.model_validate(
            {
                "mode": "update",
                "component_id_or_name": "ACTION-1",
            }
        )


@pytest.mark.parametrize("invalid_reach_direction", [True, 1.0, 3, "1"])
def test_event_trigger_enum_codes_are_strict(invalid_reach_direction: object) -> None:
    with pytest.raises(ValidationError):
        EventTriggerSetArgs.model_validate(
            {
                "mode": "add",
                "component_id_or_name": "Score threshold",
                "component_type": "Points",
                "parameters_json": json.dumps(
                    {
                        "SideID": "SIDE-1",
                        "PointValue": 100,
                        "ReachDirection": invalid_reach_direction,
                    }
                ),
            }
        )


def test_event_nested_filters_enforce_documented_selector_shapes() -> None:
    valid_payloads = [
        (
            "UnitDestroyed",
            {"TargetFilter": {"SpecificUnitID": "UNIT-1"}},
        ),
        (
            "UnitDetected",
            {
                "TargetFilter": {
                    "TargetSide": "Red",
                    "TargetType": "Aircraft",
                    "TargetSubType": "2001",
                },
                "DetectorSideID": "Blue",
                "MCL": "KnownClass",
            },
        ),
        (
            "UnitCargoMoved",
            {
                "CargoFilter": {"TargetType": "Personnel"},
                "TargetLimitReceived": 1,
                "TargetLimitSent": 0,
            },
        ),
    ]
    for component_type, parameters in valid_payloads:
        EventTriggerSetArgs.model_validate(
            {
                "mode": "add",
                "component_id_or_name": "Valid filter",
                "component_type": component_type,
                "parameters_json": json.dumps(parameters),
            }
        )

    invalid_payloads = [
        (
            "UnitDestroyed",
            {"TargetFilter": {"TargetSide": "Red", "TargetType": 8}},
        ),
        (
            "UnitDestroyed",
            {
                "TargetFilter": {
                    "SpecificUnitID": "UNIT-1",
                    "TargetType": "Aircraft",
                }
            },
        ),
        (
            "UnitCargoMoved",
            {"CargoFilter": {"TargetType": 1}},
        ),
        (
            "UnitCargoMoved",
            {"CargoFilter": {"TargetType": 123}},
        ),
        (
            "UnitDetected",
            {
                "TargetFilter": {
                    "TargetSide": "Red",
                    "TargetType": "Aircraft",
                    "TargetSubType": "fighter",
                }
            },
        ),
        (
            "UnitEntersArea",
            {"NOT": 1},
        ),
        (
            "UnitRemainsInArea",
            {"TD": "0:24:00:00"},
        ),
        (
            "UnitRemainsInArea",
            {"TD": "ten minutes"},
        ),
        (
            "RegularTime",
            {"Interval": 0},
        ),
    ]
    for component_type, parameters in invalid_payloads:
        with pytest.raises(ValidationError):
            EventTriggerSetArgs.model_validate(
                {
                    "mode": "add",
                    "component_id_or_name": "Invalid filter",
                    "component_type": component_type,
                    "parameters_json": json.dumps(parameters),
                }
            )


def test_event_parameter_aliases_serialize_to_canonical_official_keys() -> None:
    parameters = validate_event_component_parameters(
        "trigger",
        "UnitDestroyed",
        {"TargetFilter": {"SpecificUnit": "UNIT-1"}},
    )
    assert parameters.model_dump(mode="json", by_alias=True, exclude_none=True) == {
        "TargetFilter": {"SpecificUnitID": "UNIT-1"}
    }


def test_script_newlines_are_normalized_to_crlf_and_normalization_is_idempotent() -> None:
    mixed = "line 1\nline 2\rline 3\r\nline 4"
    expected = "line 1\r\nline 2\r\nline 3\r\nline 4"

    assert normalize_script_newlines(mixed) == expected
    assert normalize_script_newlines(expected) == expected
