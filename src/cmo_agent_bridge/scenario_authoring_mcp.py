from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Annotated, Literal, Protocol, TypeVar

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field, JsonValue, ValidationError

from cmo_agent_bridge.application.models import InvocationOutcome
from cmo_agent_bridge.application.queue_models import QueuedOperationReceipt
from cmo_agent_bridge.errors import BridgeError
from cmo_agent_bridge.operations.models import (
    DeleteResult,
    DestructivePreviewResult,
)
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
    normalize_script_newlines,
)


class ApplicationPort(Protocol):
    async def execute(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
        *,
        confirmation_token: str | None = None,
    ) -> InvocationOutcome: ...

    async def submit(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
    ) -> QueuedOperationReceipt: ...


ResultModelT = TypeVar("ResultModelT", bound=BaseModel)


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


def _destructive_annotations() -> ToolAnnotations:
    return ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=False,
        openWorldHint=False,
    )


def _mixed_destructive_annotations() -> ToolAnnotations:
    return ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=False,
        openWorldHint=False,
    )


async def _invoke(
    application: ApplicationPort,
    operation: str,
    arguments: Mapping[str, JsonValue],
    result_model: type[ResultModelT],
    *,
    confirmation_token: str | None = None,
) -> ResultModelT:
    outcome = await application.execute(
        operation,
        arguments,
        confirmation_token=confirmation_token,
    )
    if not outcome.ok:
        raise ToolError(
            json.dumps(
                outcome.error
                or {
                    "code": "PROTOCOL_ERROR",
                    "message": "CMO bridge returned an invalid result",
                    "details": {"operation": operation},
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    if outcome.result is None:
        raise ToolError(
            json.dumps(
                {
                    "code": "PROTOCOL_ERROR",
                    "message": "CMO bridge returned no result",
                    "details": {"operation": operation},
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    try:
        return result_model.model_validate(outcome.result)
    except ValidationError as error:
        raise ToolError(
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
        ) from error


async def _submit(
    application: ApplicationPort,
    operation: str,
    arguments: Mapping[str, JsonValue],
) -> QueuedOperationReceipt:
    try:
        return await application.submit(operation, arguments)
    except BridgeError as error:
        raise ToolError(
            json.dumps(
                error.to_payload(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        ) from error


def _queued_mutation_description(description: str) -> str:
    return (
        f"{description} This tool only submits the mutation to CMO's durable queue; it does "
        "not return CMO's eventual result. Use cmo_request_get or cmo_request_wait with the "
        "returned request_id to inspect that result. A wait timeout does not cancel the request."
    )


def _lua_arguments(function: str, arguments: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    return {"function": function, "arguments": dict(arguments)}


def _component_parameters_json(parameters: Mapping[str, JsonValue] | None) -> str:
    normalized = dict(parameters or {})
    reserved = {
        "mode",
        "description",
        "name",
        "id",
        "type",
        "newname",
        "rename",
    }
    collisions = sorted(key for key in normalized if key.lower() in reserved)
    if collisions:
        raise ToolError(
            json.dumps(
                {
                    "code": "INVALID_ARGUMENT",
                    "message": "event component parameters contain reserved envelope fields",
                    "details": {"fields": collisions},
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    for key, value in tuple(normalized.items()):
        if key.lower() in {"scripttext", "scripttest"} and isinstance(value, str):
            normalized[key] = normalize_script_newlines(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def register_scenario_authoring_tools(
    server: FastMCP[None],
    application: ApplicationPort,
) -> None:
    """Register the scenario-authoring surface on the shared local MCP server."""

    async def scenario_weather_get() -> ScenarioWeatherResult:
        arguments = ScenarioWeatherGetArgs()
        return await _invoke(
            application,
            "lua.call",
            _lua_arguments(
                "ScenEdit_GetWeather",
                arguments.model_dump(mode="json"),
            ),
            ScenarioWeatherResult,
        )

    server.add_tool(
        scenario_weather_get,
        name="cmo_scenario_weather_get",
        title="Get CMO scenario weather",
        description=(
            "Scenario-authoring/umpire tool. Return the global temperature, rainfall, cloud "
            "fraction, and sea state."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def scenario_weather_set(
        temperature_c: float,
        rainfall: Annotated[float, Field(ge=0, le=50)],
        undercloud_fraction: Annotated[float, Field(ge=0, le=1)],
        sea_state: Annotated[int, Field(ge=0, le=9)],
    ) -> QueuedOperationReceipt:
        arguments = ScenarioWeatherSetArgs(
            temperature_c=temperature_c,
            rainfall=rainfall,
            undercloud_fraction=undercloud_fraction,
            sea_state=sea_state,
        )
        return await _submit(
            application,
            "lua.call",
            _lua_arguments(
                "ScenEdit_SetWeather",
                arguments.model_dump(mode="json"),
            ),
        )

    server.add_tool(
        scenario_weather_set,
        name="cmo_scenario_weather_set",
        title="Set CMO scenario weather",
        description=_queued_mutation_description(
            "Scenario-authoring/umpire tool. Replace all four global weather values."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def scenario_title_set(
        title: Annotated[str, Field(min_length=1)],
    ) -> QueuedOperationReceipt:
        arguments = ScenarioTitleSetArgs(title=title)
        return await _submit(
            application,
            "lua.call",
            _lua_arguments(
                "SetScenarioTitle",
                arguments.model_dump(mode="json"),
            ),
        )

    server.add_tool(
        scenario_title_set,
        name="cmo_scenario_title_set",
        title="Set CMO scenario title",
        description=_queued_mutation_description(
            "Scenario-authoring tool. Set the loaded scenario title."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def scenario_timeline_set(
        current_time: Annotated[
            str | None,
            Field(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"),
        ] = None,
        start_time: Annotated[
            str | None,
            Field(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"),
        ] = None,
        duration: Annotated[
            str | None,
            Field(pattern=r"^\d+:\d{1,2}:\d{1,2}$"),
        ] = None,
    ) -> QueuedOperationReceipt:
        arguments = ScenarioTimelineSetArgs(
            current_time=current_time,
            start_time=start_time,
            duration=duration,
        )
        return await _submit(
            application,
            "lua.call",
            _lua_arguments(
                "ScenEdit_SetTime",
                arguments.model_dump(mode="json"),
            ),
        )

    server.add_tool(
        scenario_timeline_set,
        name="cmo_scenario_timeline_set",
        title="Set CMO scenario timeline",
        description=_queued_mutation_description(
            "Scenario-authoring tool. Set the current time, start time, or duration using "
            "timezone-free scenario-local values."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def side_add(name: Annotated[str, Field(min_length=1)]) -> QueuedOperationReceipt:
        arguments = SideAddArgs(name=name)
        return await _submit(
            application,
            "lua.call",
            _lua_arguments(
                "ScenEdit_AddSide",
                arguments.model_dump(mode="json"),
            ),
        )

    server.add_tool(
        side_add,
        name="cmo_side_add",
        title="Add CMO side",
        description=_queued_mutation_description(
            "Scenario-authoring tool. Add one side by name. Repeated calls may create duplicates."
        ),
        annotations=_create_annotations(),
        structured_output=True,
    )

    async def side_options_set(
        side_guid: Annotated[str, Field(min_length=1)],
        awareness: str | int | None = None,
        proficiency: str | int | None = None,
        auto_track_civilians: bool | None = None,
        collective_responsibility: bool | None = None,
        computer_controlled_only: bool | None = None,
    ) -> QueuedOperationReceipt:
        arguments = SideOptionsSetArgs(
            side_guid=side_guid,
            awareness=awareness,
            proficiency=proficiency,
            auto_track_civilians=auto_track_civilians,
            collective_responsibility=collective_responsibility,
            computer_controlled_only=computer_controlled_only,
        )
        return await _submit(
            application,
            "lua.call",
            _lua_arguments(
                "ScenEdit_SetSideOptions",
                arguments.model_dump(mode="json"),
            ),
        )

    server.add_tool(
        side_options_set,
        name="cmo_side_options_set",
        title="Set CMO side options",
        description=_queued_mutation_description(
            "Scenario-authoring/umpire tool. Update awareness, proficiency, civilian tracking, "
            "collective responsibility, or AI-only control for one side."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def side_posture_set(
        side_a_guid: Annotated[str, Field(min_length=1)],
        side_b_guid: Annotated[str, Field(min_length=1)],
        posture: Literal["F", "H", "N", "U"],
    ) -> QueuedOperationReceipt:
        arguments = SidePostureSetArgs(
            side_a_guid=side_a_guid,
            side_b_guid=side_b_guid,
            posture=posture,
        )
        return await _submit(
            application,
            "lua.call",
            _lua_arguments(
                "ScenEdit_SetSidePosture",
                arguments.model_dump(mode="json"),
            ),
        )

    server.add_tool(
        side_posture_set,
        name="cmo_side_posture_set",
        title="Set CMO side posture",
        description=_queued_mutation_description(
            "Scenario-authoring/umpire tool. Set side A's one-way posture toward side B; this "
            "does not automatically change the reverse relationship."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def score_set(
        side: Annotated[str, Field(min_length=1)],
        score: int,
        reason: Annotated[str, Field(min_length=1)],
    ) -> QueuedOperationReceipt:
        arguments = ScoreSetArgs(side=side, score=score, reason=reason)
        return await _submit(
            application,
            "lua.call",
            _lua_arguments(
                "ScenEdit_SetScore",
                arguments.model_dump(mode="json"),
            ),
        )

    server.add_tool(
        score_set,
        name="cmo_score_set",
        title="Set CMO side score",
        description=_queued_mutation_description(
            "Scenario-authoring/umpire tool. Set one side's absolute score and record a reason."
        ),
        annotations=_mutation_annotations(),
        structured_output=True,
    )

    async def event_list(
        level: Literal[0, 1, 2, 3, 4] = 4,
    ) -> AuthoringDataResult:
        arguments = EventListArgs(level=level)
        return await _invoke(
            application,
            "lua.call",
            _lua_arguments(
                "ScenEdit_GetEvents",
                arguments.model_dump(mode="json"),
            ),
            AuthoringDataResult,
        )

    server.add_tool(
        event_list,
        name="cmo_event_list",
        title="List CMO events",
        description=(
            "Scenario-authoring tool. List events at detail level 0=all, 1=triggers, "
            "2=conditions, 3=actions, or 4=event properties."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def event_get(
        event_id_or_name: Annotated[str, Field(min_length=1)],
        level: Literal[0, 1, 2, 3, 4] = 4,
    ) -> AuthoringDataResult:
        arguments = EventGetArgs(
            event_id_or_name=event_id_or_name,
            level=level,
        )
        return await _invoke(
            application,
            "lua.call",
            _lua_arguments(
                "ScenEdit_GetEvent",
                arguments.model_dump(mode="json"),
            ),
            AuthoringDataResult,
        )

    server.add_tool(
        event_get,
        name="cmo_event_get",
        title="Get CMO event",
        description="Scenario-authoring tool. Read one event by GUID or exact description.",
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def event_set(
        mode: Literal["add", "update", "remove"],
        event_id_or_name: Annotated[str, Field(min_length=1)],
        new_description: Annotated[str | None, Field(min_length=1)] = None,
        active: bool | None = None,
        shown: bool | None = None,
        repeatable: bool | None = None,
        probability: Annotated[int | None, Field(ge=0, le=100)] = None,
    ) -> QueuedOperationReceipt:
        if mode == "add":
            active = False if active is None else active
            shown = False if shown is None else shown
            repeatable = False if repeatable is None else repeatable
            probability = 100 if probability is None else probability
        arguments = EventSetArgs(
            mode=mode,
            event_id_or_name=event_id_or_name,
            new_description=new_description,
            active=active,
            shown=shown,
            repeatable=repeatable,
            probability=probability,
        )
        return await _submit(
            application,
            "lua.call",
            _lua_arguments(
                "ScenEdit_SetEvent",
                arguments.model_dump(mode="json"),
            ),
        )

    server.add_tool(
        event_set,
        name="cmo_event_set",
        title="Create, update, or remove CMO event",
        description=_queued_mutation_description(
            "Scenario-authoring tool. Create events inactive by default, update their properties, "
            "or remove them after detaching reusable components."
        ),
        annotations=_mixed_destructive_annotations(),
        structured_output=True,
    )

    async def event_component_set(
        kind: Literal["trigger", "condition", "action"],
        mode: Literal["list", "add", "update", "remove"],
        component_id_or_name: Annotated[str, Field(min_length=1)],
        component_type: Annotated[str | None, Field(min_length=1)] = None,
        new_description: Annotated[str | None, Field(min_length=1)] = None,
        parameters: dict[str, JsonValue] | None = None,
    ) -> QueuedOperationReceipt:
        function = {
            "trigger": "ScenEdit_SetTrigger",
            "condition": "ScenEdit_SetCondition",
            "action": "ScenEdit_SetAction",
        }[kind]
        arguments = EventComponentSetArgs(
            mode=mode,
            component_id_or_name=component_id_or_name,
            component_type=component_type,
            new_description=new_description,
            parameters_json=_component_parameters_json(parameters),
        )
        return await _submit(
            application,
            "lua.call",
            _lua_arguments(
                function,
                arguments.model_dump(mode="json"),
            ),
        )

    server.add_tool(
        event_component_set,
        name="cmo_event_component_set",
        title="Manage CMO event component",
        description=_queued_mutation_description(
            "Scenario-authoring tool. List, add, update, or remove a trigger, condition, or event "
            "action. Pass official type-specific fields in parameters."
        ),
        annotations=_mixed_destructive_annotations(),
        structured_output=True,
    )

    async def event_component_link(
        kind: Literal["trigger", "condition", "action"],
        mode: Literal["add", "remove", "replace"],
        event_id_or_name: Annotated[str, Field(min_length=1)],
        component_id_or_name: Annotated[str, Field(min_length=1)],
        replacement_id_or_name: Annotated[str | None, Field(min_length=1)] = None,
    ) -> QueuedOperationReceipt:
        function = {
            "trigger": "ScenEdit_SetEventTrigger",
            "condition": "ScenEdit_SetEventCondition",
            "action": "ScenEdit_SetEventAction",
        }[kind]
        arguments = EventComponentLinkArgs(
            mode=mode,
            event_id_or_name=event_id_or_name,
            component_id_or_name=component_id_or_name,
            replacement_id_or_name=replacement_id_or_name,
        )
        return await _submit(
            application,
            "lua.call",
            _lua_arguments(
                function,
                arguments.model_dump(mode="json"),
            ),
        )

    server.add_tool(
        event_component_link,
        name="cmo_event_component_link",
        title="Attach CMO event component",
        description=_queued_mutation_description(
            "Scenario-authoring tool. Add, remove, or replace one trigger, condition, or action "
            "attached to an event."
        ),
        annotations=_mixed_destructive_annotations(),
        structured_output=True,
    )

    async def special_action_create(
        side_guid: Annotated[str, Field(min_length=1)],
        name: Annotated[str, Field(min_length=1)],
        script_text: str,
        description: str = "",
        active: bool = False,
        repeatable: bool = False,
    ) -> QueuedOperationReceipt:
        arguments = SpecialActionAddArgs(
            side_guid=side_guid,
            name=name,
            description=description,
            active=active,
            repeatable=repeatable,
            script_text=normalize_script_newlines(script_text),
        )
        return await _submit(
            application,
            "lua.call",
            _lua_arguments(
                "ScenEdit_AddSpecialAction",
                arguments.model_dump(mode="json"),
            ),
        )

    server.add_tool(
        special_action_create,
        name="cmo_special_action_create",
        title="Create CMO special action",
        description=_queued_mutation_description(
            "Scenario-authoring tool. Create a side-owned Lua special action, inactive by default."
        ),
        annotations=_create_annotations(),
        structured_output=True,
    )

    async def special_action_update(
        side_guid: Annotated[str, Field(min_length=1)],
        action_id_or_name: Annotated[str, Field(min_length=1)],
        mode: Literal["update", "remove"] = "update",
        new_name: Annotated[str | None, Field(min_length=1)] = None,
        description: str | None = None,
        active: bool | None = None,
        repeatable: bool | None = None,
        script_text: str | None = None,
    ) -> QueuedOperationReceipt:
        arguments = SpecialActionSetArgs(
            side_guid=side_guid,
            action_id_or_name=action_id_or_name,
            mode=mode,
            new_name=new_name,
            description=description,
            active=active,
            repeatable=repeatable,
            script_text=(None if script_text is None else normalize_script_newlines(script_text)),
        )
        return await _submit(
            application,
            "lua.call",
            _lua_arguments(
                "ScenEdit_SetSpecialAction",
                arguments.model_dump(mode="json"),
            ),
        )

    server.add_tool(
        special_action_update,
        name="cmo_special_action_update",
        title="Update or remove CMO special action",
        description=_queued_mutation_description(
            "Scenario-authoring tool. Readably update the properties or Lua source of a special "
            "action, or remove it."
        ),
        annotations=_mixed_destructive_annotations(),
        structured_output=True,
    )

    async def unit_delete_preview(
        unit_guid: Annotated[str, Field(min_length=1)],
    ) -> DestructivePreviewResult:
        return await _invoke(
            application,
            "unit.delete",
            {"unit_guid": unit_guid},
            DestructivePreviewResult,
        )

    server.add_tool(
        unit_delete_preview,
        name="cmo_unit_delete_preview",
        title="Preview CMO unit deletion",
        description=(
            "Scenario-authoring/umpire tool. Resolve a unit deletion and issue a short-lived "
            "confirmation token without deleting the unit."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def unit_delete_confirm(
        unit_guid: Annotated[str, Field(min_length=1)],
        confirmation_token: Annotated[str, Field(min_length=1)],
    ) -> DeleteResult:
        return await _invoke(
            application,
            "unit.delete",
            {"unit_guid": unit_guid},
            DeleteResult,
            confirmation_token=confirmation_token,
        )

    server.add_tool(
        unit_delete_confirm,
        name="cmo_unit_delete_confirm",
        title="Confirm CMO unit deletion",
        description=(
            "Scenario-authoring/umpire tool. Permanently delete the exact unit bound to a valid "
            "preview token."
        ),
        annotations=_destructive_annotations(),
        structured_output=True,
    )

    async def mission_delete_preview(
        side_guid: Annotated[str, Field(min_length=1)],
        mission_guid: Annotated[str, Field(min_length=1)],
    ) -> DestructivePreviewResult:
        return await _invoke(
            application,
            "mission.delete",
            {"side_guid": side_guid, "mission_guid": mission_guid},
            DestructivePreviewResult,
        )

    server.add_tool(
        mission_delete_preview,
        name="cmo_mission_delete_preview",
        title="Preview CMO mission deletion",
        description=(
            "Scenario-authoring/umpire tool. Resolve a mission deletion and issue a short-lived "
            "confirmation token without deleting the mission."
        ),
        annotations=_read_only_annotations(),
        structured_output=True,
    )

    async def mission_delete_confirm(
        side_guid: Annotated[str, Field(min_length=1)],
        mission_guid: Annotated[str, Field(min_length=1)],
        confirmation_token: Annotated[str, Field(min_length=1)],
    ) -> DeleteResult:
        return await _invoke(
            application,
            "mission.delete",
            {"side_guid": side_guid, "mission_guid": mission_guid},
            DeleteResult,
            confirmation_token=confirmation_token,
        )

    server.add_tool(
        mission_delete_confirm,
        name="cmo_mission_delete_confirm",
        title="Confirm CMO mission deletion",
        description=(
            "Scenario-authoring/umpire tool. Permanently delete the exact mission bound to a "
            "valid preview token."
        ),
        annotations=_destructive_annotations(),
        structured_output=True,
    )
