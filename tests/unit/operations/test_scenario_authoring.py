from __future__ import annotations

from collections.abc import Callable

import pytest
from pydantic import BaseModel, ValidationError

from cmo_agent_bridge.operations.scenario_authoring import (
    EventComponentLinkArgs,
    EventComponentSetArgs,
    EventSetArgs,
    ScenarioTimelineSetArgs,
    ScenarioTitleSetArgs,
    ScenarioWeatherSetArgs,
    SideOptionsSetArgs,
    SpecialActionSetArgs,
    normalize_script_newlines,
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


def test_timeline_and_side_option_models_require_an_actual_update() -> None:
    with pytest.raises(ValidationError, match="at least one scenario timeline update"):
        ScenarioTimelineSetArgs()
    with pytest.raises(ValidationError, match="at least one side option update"):
        SideOptionsSetArgs(side_guid="SIDE-1")
    with pytest.raises(ValidationError):
        ScenarioTimelineSetArgs(current_time="2026/07/16 12:00:00")

    assert ScenarioTimelineSetArgs(
        current_time="2026-07-16T12:00:00",
        duration="1:23:59",
    ).duration == "1:23:59"
    assert SideOptionsSetArgs(
        side_guid="SIDE-1",
        computer_controlled_only=False,
    ).computer_controlled_only is False
    with pytest.raises(ValidationError, match="days:hours:minutes"):
        ScenarioTimelineSetArgs(duration="1:24:00")
    with pytest.raises(ValidationError):
        ScenarioTimelineSetArgs(current_time="2026-02-30T12:00:00")


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


def test_script_newlines_are_normalized_to_crlf_and_normalization_is_idempotent() -> None:
    mixed = "line 1\nline 2\rline 3\r\nline 4"
    expected = "line 1\r\nline 2\r\nline 3\r\nline 4"

    assert normalize_script_newlines(mixed) == expected
    assert normalize_script_newlines(expected) == expected
