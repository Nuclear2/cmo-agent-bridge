from __future__ import annotations

import json
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from uuid import UUID

import pytest
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import JsonValue

from cmo_agent_bridge.application.models import InvocationOutcome
from cmo_agent_bridge.application.queue_models import (
    QueuedOperationReceipt,
    QueuedOperationState,
)
from cmo_agent_bridge.scenario_authoring_mcp import register_scenario_authoring_tools


_TOOL_NAMES = {
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


@dataclass(frozen=True, slots=True)
class _Call:
    operation: str
    arguments: dict[str, JsonValue]
    confirmation_token: str | None


class _FakeApplication:
    def __init__(self, outcomes: Sequence[InvocationOutcome]) -> None:
        self._outcomes = deque(outcomes)
        self.calls: list[_Call] = []
        self.submissions: list[_Call] = []
        self._submitted_count = 0

    async def execute(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
        *,
        confirmation_token: str | None = None,
    ) -> InvocationOutcome:
        self.calls.append(_Call(operation, dict(arguments), confirmation_token))
        if not self._outcomes:
            raise AssertionError(f"no queued outcome for {operation}")
        return self._outcomes.popleft()

    async def submit(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
    ) -> QueuedOperationReceipt:
        call = _Call(operation, dict(arguments), None)
        self.calls.append(call)
        self.submissions.append(call)
        if self._outcomes:
            self._outcomes.popleft()
        self._submitted_count += 1
        return QueuedOperationReceipt(
            request_id=UUID(int=self._submitted_count),
            operation=operation,
            sequence=self._submitted_count,
            state=QueuedOperationState.QUEUED,
            submitted_at_ms=0,
        )


def _success(result: JsonValue) -> InvocationOutcome:
    return InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=None,
        ok=True,
        result=result,
        error=None,
    )


def _server(application: _FakeApplication) -> FastMCP[None]:
    server: FastMCP[None] = FastMCP("Scenario authoring test", log_level="ERROR")
    register_scenario_authoring_tools(server, application)
    return server


def _weather_result() -> dict[str, JsonValue]:
    return {
        "temperature_c": 18.5,
        "rainfall": 4.0,
        "undercloud_fraction": 0.25,
        "sea_state": 3,
    }


def _scenario_result() -> dict[str, JsonValue]:
    return {
        "guid": "SCENARIO-1",
        "title": "Northern Shield",
        "file_name": "northern-shield.scen",
        "file_name_path": "C:\\CMO\\Scenarios",
        "current_time": "2026/7/16 12:00:00",
        "current_time_seconds": 1_784_203_200.0,
        "start_time": "2026/7/16 10:00:00",
        "start_time_seconds": 1_784_196_000.0,
        "duration": "1.00:00:00",
        "duration_seconds": 86_400.0,
        "complexity": 2,
        "difficulty": 3,
        "setting": "Pacific",
        "database": "DB3K_517.db3",
        "save_version": "Command: Modern Operations Build 1868",
        "started": False,
        "player_side_guid": "SIDE-BLUE",
        "time_compression": 0.0,
        "campaign_score": 0,
    }


def _side_result() -> dict[str, JsonValue]:
    return {
        "guid": "SIDE-1",
        "name": "Blue",
        "awareness": "Normal",
        "proficiency": "Veteran",
        "computer_controlled_only": False,
        "unit_count": 0,
        "contact_count": 0,
        "mission_count": 0,
    }


def _authoring_result(operation: str = "authoring") -> dict[str, JsonValue]:
    return {"operation": operation, "accepted": True, "data": {}}


def _preview_result(
    operation: str,
    target_guid: str,
    target_type: str,
    token: str,
) -> dict[str, JsonValue]:
    return {
        "operation": operation,
        "target_guid": target_guid,
        "target_name": "Target",
        "target_type": target_type,
        "impact": "Permanently deletes the selected object.",
        "reserved_activation_candidate": "11111111-1111-4111-8111-111111111111",
        "confirmation_token": token,
        "expires_at_utc": "2026-07-16T12:05:00Z",
    }


def _lua_call(function: str, arguments: dict[str, JsonValue]) -> _Call:
    return _Call(
        "lua.call",
        {"function": function, "arguments": arguments},
        None,
    )


@pytest.mark.asyncio
async def test_registers_the_complete_scenario_authoring_surface() -> None:
    server = _server(_FakeApplication([]))

    tools = await server.list_tools()

    assert {tool.name for tool in tools} == _TOOL_NAMES


@pytest.mark.asyncio
async def test_all_authoring_tools_map_arguments_to_the_application_port() -> None:
    weather = _weather_result()
    scenario = _scenario_result()
    side = _side_result()
    authoring = _authoring_result()
    application = _FakeApplication(
        [
            _success(weather),
            _success(weather),
            _success(scenario),
            _success(scenario),
            _success(side),
            _success(side),
            _success({"side_a_guid": "SIDE-1", "side_b_guid": "SIDE-2", "posture": "H"}),
            _success({"side": "Blue", "score": 250}),
            *[_success(authoring) for _ in range(7)],
            _success(_preview_result("unit.delete", "UNIT-1", "unit", "UNIT-TOKEN")),
            _success(
                {
                    "deleted_guid": "UNIT-1",
                    "deleted_name": "Aircraft 1",
                    "object_kind": "unit",
                }
            ),
            _success(_preview_result("mission.delete", "MISSION-1", "mission", "MISSION-TOKEN")),
            _success(
                {
                    "deleted_guid": "MISSION-1",
                    "deleted_name": "CAP",
                    "object_kind": "mission",
                }
            ),
        ]
    )
    server = _server(application)

    await server.call_tool("cmo_scenario_weather_get", {})
    await server.call_tool(
        "cmo_scenario_weather_set",
        {
            "temperature_c": 18.5,
            "rainfall": 4.0,
            "undercloud_fraction": 0.25,
            "sea_state": 3,
        },
    )
    await server.call_tool("cmo_scenario_title_set", {"title": "Northern Shield"})
    await server.call_tool(
        "cmo_scenario_timeline_set",
        {
            "current_time": "2026-07-16T12:00:00",
            "start_time": "2026-07-16T10:00:00",
            "duration": "1:00:00",
        },
    )
    await server.call_tool("cmo_side_add", {"name": "Blue"})
    await server.call_tool(
        "cmo_side_options_set",
        {
            "side_guid": "SIDE-1",
            "awareness": "Normal",
            "proficiency": 4,
            "auto_track_civilians": True,
            "collective_responsibility": False,
            "computer_controlled_only": False,
        },
    )
    await server.call_tool(
        "cmo_side_posture_set",
        {"side_a_guid": "SIDE-1", "side_b_guid": "SIDE-2", "posture": "H"},
    )
    await server.call_tool(
        "cmo_score_set",
        {"side": "Blue", "score": 250, "reason": "Primary objective achieved"},
    )
    await server.call_tool("cmo_event_list", {"level": 4})
    await server.call_tool(
        "cmo_event_get",
        {"event_id_or_name": "EVENT-1", "level": 2},
    )
    await server.call_tool(
        "cmo_event_set",
        {
            "mode": "update",
            "event_id_or_name": "EVENT-1",
            "new_description": "Enemy detected",
            "active": True,
            "shown": True,
            "repeatable": False,
            "probability": 75,
        },
    )
    await server.call_tool(
        "cmo_event_component_set",
        {
            "kind": "action",
            "mode": "add",
            "component_id_or_name": "Run Lua",
            "component_type": "LuaScript",
            "new_description": "Update state",
            "parameters": {
                "ScriptText": "local x = 1\nreturn x",
                "Value": 2,
            },
        },
    )
    await server.call_tool(
        "cmo_event_component_link",
        {
            "kind": "action",
            "mode": "replace",
            "event_id_or_name": "EVENT-1",
            "component_id_or_name": "ACTION-1",
            "replacement_id_or_name": "ACTION-2",
        },
    )
    await server.call_tool(
        "cmo_special_action_create",
        {
            "side_guid": "SIDE-1",
            "name": "Request support",
            "script_text": "print('request')\nreturn true",
            "description": "Request external support",
        },
    )
    await server.call_tool(
        "cmo_special_action_update",
        {
            "side_guid": "SIDE-1",
            "action_id_or_name": "ACTION-1",
            "mode": "update",
            "new_name": "Request immediate support",
            "description": "Updated",
            "active": True,
            "repeatable": True,
            "script_text": "print('updated')\rreturn true",
        },
    )
    await server.call_tool("cmo_unit_delete_preview", {"unit_guid": "UNIT-1"})
    await server.call_tool(
        "cmo_unit_delete_confirm",
        {"unit_guid": "UNIT-1", "confirmation_token": "UNIT-TOKEN"},
    )
    await server.call_tool(
        "cmo_mission_delete_preview",
        {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"},
    )
    await server.call_tool(
        "cmo_mission_delete_confirm",
        {
            "side_guid": "SIDE-1",
            "mission_guid": "MISSION-1",
            "confirmation_token": "MISSION-TOKEN",
        },
    )

    component_json = json.dumps(
        {"ScriptText": "local x = 1\r\nreturn x", "Value": 2},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    assert application.calls == [
        _lua_call("ScenEdit_GetWeather", {}),
        _lua_call(
            "ScenEdit_SetWeather",
            {
                "temperature_c": 18.5,
                "rainfall": 4.0,
                "undercloud_fraction": 0.25,
                "sea_state": 3,
            },
        ),
        _lua_call("SetScenarioTitle", {"title": "Northern Shield"}),
        _lua_call(
            "ScenEdit_SetTime",
            {
                "current_time": "2026-07-16T12:00:00",
                "start_time": "2026-07-16T10:00:00",
                "duration": "1:00:00",
            },
        ),
        _lua_call("ScenEdit_AddSide", {"name": "Blue"}),
        _lua_call(
            "ScenEdit_SetSideOptions",
            {
                "side_guid": "SIDE-1",
                "awareness": "Normal",
                "proficiency": 4,
                "auto_track_civilians": True,
                "collective_responsibility": False,
                "computer_controlled_only": False,
            },
        ),
        _lua_call(
            "ScenEdit_SetSidePosture",
            {"side_a_guid": "SIDE-1", "side_b_guid": "SIDE-2", "posture": "H"},
        ),
        _lua_call(
            "ScenEdit_SetScore",
            {"side": "Blue", "score": 250, "reason": "Primary objective achieved"},
        ),
        _lua_call("ScenEdit_GetEvents", {"level": 4}),
        _lua_call(
            "ScenEdit_GetEvent",
            {"event_id_or_name": "EVENT-1", "level": 2},
        ),
        _lua_call(
            "ScenEdit_SetEvent",
            {
                "mode": "update",
                "event_id_or_name": "EVENT-1",
                "new_description": "Enemy detected",
                "active": True,
                "shown": True,
                "repeatable": False,
                "probability": 75,
            },
        ),
        _lua_call(
            "ScenEdit_SetAction",
            {
                "mode": "add",
                "component_id_or_name": "Run Lua",
                "component_type": "LuaScript",
                "new_description": "Update state",
                "parameters_json": component_json,
            },
        ),
        _lua_call(
            "ScenEdit_SetEventAction",
            {
                "mode": "replace",
                "event_id_or_name": "EVENT-1",
                "component_id_or_name": "ACTION-1",
                "replacement_id_or_name": "ACTION-2",
            },
        ),
        _lua_call(
            "ScenEdit_AddSpecialAction",
            {
                "side_guid": "SIDE-1",
                "name": "Request support",
                "description": "Request external support",
                "active": False,
                "repeatable": False,
                "script_text": "print('request')\r\nreturn true",
            },
        ),
        _lua_call(
            "ScenEdit_SetSpecialAction",
            {
                "side_guid": "SIDE-1",
                "action_id_or_name": "ACTION-1",
                "mode": "update",
                "new_name": "Request immediate support",
                "description": "Updated",
                "active": True,
                "repeatable": True,
                "script_text": "print('updated')\r\nreturn true",
            },
        ),
        _Call("unit.delete", {"unit_guid": "UNIT-1"}, None),
        _Call("unit.delete", {"unit_guid": "UNIT-1"}, "UNIT-TOKEN"),
        _Call(
            "mission.delete",
            {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"},
            None,
        ),
        _Call(
            "mission.delete",
            {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"},
            "MISSION-TOKEN",
        ),
    ]
    assert [call.arguments["function"] for call in application.submissions] == [
        "ScenEdit_SetWeather",
        "SetScenarioTitle",
        "ScenEdit_SetTime",
        "ScenEdit_AddSide",
        "ScenEdit_SetSideOptions",
        "ScenEdit_SetSidePosture",
        "ScenEdit_SetScore",
        "ScenEdit_SetEvent",
        "ScenEdit_SetAction",
        "ScenEdit_SetEventAction",
        "ScenEdit_AddSpecialAction",
        "ScenEdit_SetSpecialAction",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "component_function", "link_function"),
    [
        ("trigger", "ScenEdit_SetTrigger", "ScenEdit_SetEventTrigger"),
        ("condition", "ScenEdit_SetCondition", "ScenEdit_SetEventCondition"),
        ("action", "ScenEdit_SetAction", "ScenEdit_SetEventAction"),
    ],
)
async def test_event_kind_selects_the_official_component_and_link_functions(
    kind: str,
    component_function: str,
    link_function: str,
) -> None:
    application = _FakeApplication([_success(_authoring_result()), _success(_authoring_result())])
    server = _server(application)

    await server.call_tool(
        "cmo_event_component_set",
        {
            "kind": kind,
            "mode": "list",
            "component_id_or_name": "COMPONENT-1",
        },
    )
    await server.call_tool(
        "cmo_event_component_link",
        {
            "kind": kind,
            "mode": "add",
            "event_id_or_name": "EVENT-1",
            "component_id_or_name": "COMPONENT-1",
        },
    )

    assert application.calls == [
        _lua_call(
            component_function,
            {
                "mode": "list",
                "component_id_or_name": "COMPONENT-1",
                "component_type": None,
                "new_description": None,
                "parameters_json": "{}",
            },
        ),
        _lua_call(
            link_function,
            {
                "mode": "add",
                "event_id_or_name": "EVENT-1",
                "component_id_or_name": "COMPONENT-1",
                "replacement_id_or_name": None,
            },
        ),
    ]


@pytest.mark.asyncio
async def test_event_creation_is_inactive_hidden_and_nonrepeatable_by_default() -> None:
    application = _FakeApplication([_success(_authoring_result())])
    server = _server(application)

    await server.call_tool(
        "cmo_event_set",
        {"mode": "add", "event_id_or_name": "Reinforcements arrive"},
    )

    assert application.calls == [
        _lua_call(
            "ScenEdit_SetEvent",
            {
                "mode": "add",
                "event_id_or_name": "Reinforcements arrive",
                "new_description": None,
                "active": False,
                "shown": False,
                "repeatable": False,
                "probability": 100,
            },
        )
    ]


@pytest.mark.asyncio
async def test_component_parameters_cannot_override_trusted_envelope_fields() -> None:
    application = _FakeApplication([])
    server = _server(application)

    with pytest.raises(ToolError, match="reserved envelope fields"):
        await server.call_tool(
            "cmo_event_component_set",
            {
                "kind": "action",
                "mode": "add",
                "component_id_or_name": "Safe action",
                "component_type": "LuaScript",
                "parameters": {
                    "Mode": "remove",
                    "ScriptText": "return true",
                },
            },
        )

    assert application.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "payload"),
    [
        ("cmo_scenario_timeline_set", {}),
        ("cmo_side_options_set", {"side_guid": "SIDE-1"}),
        (
            "cmo_event_set",
            {"mode": "update", "event_id_or_name": "EVENT-1"},
        ),
        (
            "cmo_event_component_link",
            {
                "kind": "trigger",
                "mode": "replace",
                "event_id_or_name": "EVENT-1",
                "component_id_or_name": "TRIGGER-1",
            },
        ),
        (
            "cmo_special_action_update",
            {
                "side_guid": "SIDE-1",
                "action_id_or_name": "ACTION-1",
            },
        ),
    ],
)
async def test_cross_field_invariants_are_enforced_at_the_mcp_boundary(
    tool_name: str,
    payload: dict[str, JsonValue],
) -> None:
    application = _FakeApplication([])
    server = _server(application)

    with pytest.raises(ToolError):
        await server.call_tool(tool_name, payload)

    assert application.calls == []
