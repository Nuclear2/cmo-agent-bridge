from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from collections import deque
from dataclasses import dataclass
from typing import Self, TypeAlias, cast
from uuid import UUID, uuid4

from pydantic import JsonValue, ValidationError

from cmo_agent_bridge.errors import ErrorCode
from cmo_agent_bridge.operations.models import BridgeStatusWireArgs, LuaCallArgs
from cmo_agent_bridge.operations.registry import (
    FrozenInvocation,
    OperationRegistry,
    ResolvedInvocation,
)
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua, render_idle_lua
from cmo_agent_bridge.protocol.models import PreparedDelivery, RequestBody
from cmo_agent_bridge.protocol.response_models import ResponseEnvelope, ResponseError
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, revalidate_runtime_snapshot
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths


@dataclass(frozen=True, slots=True)
class Respond:
    result: JsonValue | None = None
    error: dict[str, JsonValue] | None = None
    envelope_overrides: tuple[tuple[str, JsonValue], ...] = ()


@dataclass(frozen=True, slots=True)
class Delay:
    seconds: float
    then: PeerAction


@dataclass(frozen=True, slots=True)
class WritePartialThenComplete:
    response: Respond
    delay_seconds: float = 0.02


@dataclass(frozen=True, slots=True)
class WriteMismatchedDelivery:
    response: Respond
    delay_seconds: float = 0.02


@dataclass(frozen=True, slots=True)
class WriteMalformedComments:
    comments: str = "{malformed"


@dataclass(frozen=True, slots=True)
class StaySilent:
    pass


PeerAction: TypeAlias = (
    Respond
    | Delay
    | WritePartialThenComplete
    | WriteMismatchedDelivery
    | WriteMalformedComments
    | StaySilent
)


_RUNTIME_OVERRIDE_KEYS = frozenset(
    {
        "operation_manifest_sha256",
        "bridge_version",
        "runtime_tag",
        "runtime_asset_sha256",
        "release_id",
    }
)
_CONTEXT_OVERRIDE_KEYS = frozenset({"scenario_lineage_id", "activation_id"})
_FORBIDDEN_OVERRIDE_KEYS = frozenset(
    {
        "protocol",
        "request_id",
        "delivery_id",
        "request_hash",
        "ok",
        "result",
        "error",
    }
)
_DELIVERY_PATTERN = re.compile(
    r"\ACMO_AGENT_BRIDGE_DELIVERY = \{\n"
    r'    request_id = "(?P<request_id>[^"]+)",\n'
    r'    delivery_id = "(?P<delivery_id>[^"]+)",\n'
    r'    delivery_kind = "(?P<delivery_kind>[^"]+)",\n'
    r'    request_hash = "(?P<request_hash>[^"]+)",\n'
    r'    runtime_version = "(?P<runtime_version>[^"]+)",\n'
    r'    runtime_tag = "(?P<runtime_tag>[^"]+)",\n'
    r'    runtime_asset_sha256 = "(?P<runtime_asset_sha256>[^"]+)",\n'
    r'    release_id = "(?P<release_id>[^"]+)",\n'
    r'    body_json = "(?P<body_json>(?:\\[0-9]{3})*)",\n'
    r"\}\n"
    r"local ok = pcall\(ScenEdit_RunScript, "
    r'"CMOAgentBridge/versions/(?P<dispatcher_tag>[^"]+)/dispatcher\.lua"\)\n'
    r"CMO_AGENT_BRIDGE_DELIVERY = nil\n"
    r"if not ok then\n"
    r'    print\("CMO_AGENT_BRIDGE\|DISPATCH_FAILED"\)\n'
    r"end\n"
    r"return ok\n\Z"
)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _deep_json_snapshot(value: object) -> JsonValue:
    try:
        raw = _canonical_bytes(value)
        return cast(JsonValue, json.loads(raw))
    except (TypeError, ValueError, OverflowError, RecursionError) as error:
        raise ValueError("fake peer payload must be finite canonical JSON") from error


def _validated_number(value: object, description: str, *, allow_zero: bool) -> float:
    if type(value) not in {int, float}:
        raise ValueError(f"{description} must be an exact int or float")
    try:
        result = float(cast(int | float, value))
    except OverflowError as error:
        raise ValueError(f"{description} must be finite") from error
    if not math.isfinite(result) or result < 0 or (result == 0 and not allow_zero):
        raise ValueError(f"{description} is outside its allowed range")
    return result


def _exact_uuid4(value: str) -> UUID:
    try:
        parsed = UUID(value)
    except (AttributeError, ValueError) as error:
        raise AssertionError("fake peer observed an invalid UUID") from error
    if str(parsed) != value or parsed.version != 4:
        raise AssertionError("fake peer observed a non-canonical UUIDv4")
    return parsed


def _decode_decimal_bytes(value: str) -> bytes:
    pieces = re.findall(r"\\([0-9]{3})", value)
    if "".join(f"\\{piece}" for piece in pieces) != value:
        raise AssertionError("fake peer observed malformed Lua decimal byte escapes")
    integers = [int(piece) for piece in pieces]
    if any(number > 255 for number in integers):
        raise AssertionError("fake peer observed an out-of-range Lua byte escape")
    return bytes(integers)


def _same_frozen_projection(
    frozen: FrozenInvocation,
    resolved: ResolvedInvocation,
) -> bool:
    return (
        frozen.contract == resolved.contract
        and frozen.wire_arguments == resolved.wire_arguments
        and frozen.effective_class is resolved.effective_class
        and frozen.result_schema == resolved.result_schema
        and frozen.recovery_schema == resolved.recovery_schema
    )


class FakeFileBridgePeer:
    def __init__(
        self,
        *,
        paths: FileBridgePaths,
        runtime_snapshot: RuntimeSnapshot,
        registry: OperationRegistry,
        scenario_lineage_id: UUID,
        build_number: int = 1868,
        poll_seconds: float = 0.005,
        trace: list[str] | None = None,
    ) -> None:
        if type(paths) is not FileBridgePaths:
            raise ValueError("fake peer paths must be exact FileBridgePaths")
        try:
            snapshot = revalidate_runtime_snapshot(runtime_snapshot)
        except (TypeError, ValueError) as error:
            raise ValueError("fake peer runtime snapshot is invalid") from error
        if type(registry) is not OperationRegistry:
            raise ValueError("fake peer registry must be exact OperationRegistry")
        if registry.manifest_sha256 != snapshot.operation_manifest_sha256:
            raise ValueError("fake peer registry does not match runtime snapshot")
        if type(scenario_lineage_id) is not UUID or scenario_lineage_id.version != 4:
            raise ValueError("fake peer scenario lineage must be an exact UUIDv4")
        if type(build_number) is not int or build_number <= 0:
            raise ValueError("fake peer build number must be a positive exact integer")
        poll = _validated_number(poll_seconds, "fake peer poll interval", allow_zero=False)
        if trace is not None:
            if type(trace) is not list or any(type(item) is not str for item in trace):
                raise ValueError("fake peer trace must be a list of strings")
            observed_order = trace
        else:
            observed_order = []

        self._paths = paths
        self._runtime_snapshot = snapshot
        self._registry = registry
        self._scenario_lineage_id = scenario_lineage_id
        self._build_number = build_number
        self._poll_seconds = poll
        self._trace = observed_order
        self._actions: deque[PeerAction] = deque()
        self._observed: list[PreparedDelivery] = []
        self._seen_delivery_ids: set[UUID] = set()
        self._task: asyncio.Task[None] | None = None

    def enqueue(self, *actions: PeerAction) -> None:
        self._actions.extend(self._snapshot_action(action) for action in actions)

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("fake peer is already running")
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is None:
            return
        requested_cancel = False
        if not task.done():
            requested_cancel = task.cancel("fake peer stopping")
        try:
            await task
        except asyncio.CancelledError:
            if not requested_cancel:
                raise

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> bool:
        del exc_type, exc, traceback
        await self.stop()
        return False

    def result_for(
        self,
        invocation: ResolvedInvocation,
        **overrides: JsonValue,
    ) -> JsonValue:
        rebuilt = self._rebuild_invocation(invocation)
        baseline = self._baseline_result(rebuilt)
        candidate = dict(baseline)
        candidate.update({key: _deep_json_snapshot(value) for key, value in overrides.items()})
        try:
            validated = invocation.result_adapter.validate_python(candidate)
            dumped = invocation.result_adapter.dump_python(validated, mode="json")
            return _deep_json_snapshot(dumped)
        except (ValidationError, TypeError, ValueError, OverflowError) as error:
            raise AssertionError(
                "fake peer result does not satisfy the exact invocation"
            ) from error

    @property
    def observed_deliveries(self) -> tuple[PreparedDelivery, ...]:
        return tuple(self._observed)

    @property
    def observed_order(self) -> tuple[str, ...]:
        return tuple(self._trace)

    def _snapshot_action(self, action: object) -> PeerAction:
        if type(action) is Respond:
            response = action
            if (response.result is None) == (response.error is None):
                raise ValueError("Respond requires exactly one of result or error")
            result = None if response.result is None else _deep_json_snapshot(response.result)
            error: dict[str, JsonValue] | None = None
            if response.error is not None:
                snapshot = _deep_json_snapshot(response.error)
                if not isinstance(snapshot, dict):
                    raise ValueError("Respond error must be an object")
                try:
                    validated_error = ResponseError.model_validate(snapshot)
                except ValidationError as validation_error:
                    raise ValueError(
                        "Respond error is not a complete strict response error"
                    ) from validation_error
                error = cast(
                    dict[str, JsonValue],
                    validated_error.model_dump(mode="json", exclude_none=False),
                )
            overrides = self._snapshot_overrides(response.envelope_overrides, error)
            return Respond(result=result, error=error, envelope_overrides=overrides)
        if type(action) is Delay:
            delay = action
            seconds = _validated_number(delay.seconds, "fake peer delay", allow_zero=True)
            return Delay(seconds=seconds, then=self._snapshot_action(delay.then))
        if type(action) is WritePartialThenComplete:
            partial = action
            if type(partial.response) is not Respond:
                raise ValueError("partial response action requires an exact Respond")
            delay = _validated_number(
                partial.delay_seconds,
                "fake peer partial-write delay",
                allow_zero=False,
            )
            response = self._snapshot_action(partial.response)
            if type(response) is not Respond:
                raise AssertionError("fake peer action normalization drifted")
            return WritePartialThenComplete(response=response, delay_seconds=delay)
        if type(action) is WriteMismatchedDelivery:
            mismatch = action
            if type(mismatch.response) is not Respond:
                raise ValueError("mismatched response action requires an exact Respond")
            delay = _validated_number(
                mismatch.delay_seconds,
                "fake peer mismatch delay",
                allow_zero=False,
            )
            response = self._snapshot_action(mismatch.response)
            if type(response) is not Respond:
                raise AssertionError("fake peer action normalization drifted")
            return WriteMismatchedDelivery(response=response, delay_seconds=delay)
        if type(action) is WriteMalformedComments:
            malformed = action
            if type(malformed.comments) is not str:
                raise ValueError("malformed Comments action requires an exact string")
            return malformed
        if type(action) is StaySilent:
            return action
        raise ValueError("fake peer action has an unsupported exact type")

    @staticmethod
    def _snapshot_overrides(
        values: object,
        error: dict[str, JsonValue] | None,
    ) -> tuple[tuple[str, JsonValue], ...]:
        if type(values) is not tuple:
            raise ValueError("envelope overrides must be an exact tuple")
        overrides: list[tuple[str, JsonValue]] = []
        keys: set[str] = set()
        for entry in cast(tuple[object, ...], values):
            if type(entry) is not tuple:
                raise ValueError("each envelope override must be an exact pair")
            pair = cast(tuple[object, ...], entry)
            if len(pair) != 2:
                raise ValueError("each envelope override must be an exact pair")
            key, raw_value = pair
            if type(key) is not str or key in keys:
                raise ValueError("envelope override keys must be unique exact strings")
            keys.add(key)
            overrides.append((key, _deep_json_snapshot(raw_value)))
        if keys & _FORBIDDEN_OVERRIDE_KEYS:
            raise ValueError("primary response correlation fields cannot be overridden")
        unknown = keys - _RUNTIME_OVERRIDE_KEYS - _CONTEXT_OVERRIDE_KEYS
        if unknown:
            raise ValueError("envelope override key is not allowlisted")
        runtime_keys = keys & _RUNTIME_OVERRIDE_KEYS
        context_keys = keys & _CONTEXT_OVERRIDE_KEYS
        if runtime_keys and runtime_keys != set(_RUNTIME_OVERRIDE_KEYS):
            raise ValueError("runtime identity overrides must be one complete tuple")
        if runtime_keys:
            if context_keys:
                raise ValueError("diagnostic override modes cannot be combined")
            if error is None or error.get("code") != ErrorCode.MANIFEST_MISMATCH.value:
                raise ValueError("runtime identity may differ only for MANIFEST_MISMATCH")
        return tuple(overrides)

    async def _run(self) -> None:
        while True:
            try:
                raw = self._paths.inbox.read_bytes()
            except (FileNotFoundError, PermissionError):
                await asyncio.sleep(self._poll_seconds)
                continue
            if raw == render_idle_lua():
                await asyncio.sleep(self._poll_seconds)
                continue
            delivery, body, invocation = self._parse_delivery(raw)
            if delivery.delivery_id in self._seen_delivery_ids:
                await asyncio.sleep(self._poll_seconds)
                continue
            self._seen_delivery_ids.add(delivery.delivery_id)
            self._observed.append(delivery)
            self._trace.append("peer.request_observed")
            if not self._actions:
                raise AssertionError("fake peer observed a delivery without an enqueued action")
            action = self._actions.popleft()
            await self._apply_action(action, delivery, body, invocation)
            await asyncio.sleep(self._poll_seconds)

    def _parse_delivery(
        self,
        raw: bytes,
    ) -> tuple[PreparedDelivery, RequestBody, FrozenInvocation]:
        try:
            rendered = raw.decode("ascii")
        except UnicodeDecodeError as error:
            raise AssertionError("fake peer inbox is not ASCII Lua") from error
        match = _DELIVERY_PATTERN.fullmatch(rendered)
        if match is None:
            raise AssertionError("fake peer inbox does not match the complete fixed Lua template")
        groups = match.groupdict()
        request_id = _exact_uuid4(groups["request_id"])
        delivery_id = _exact_uuid4(groups["delivery_id"])
        if groups["delivery_kind"] != "request":
            raise AssertionError("H8 fake peer accepts request deliveries only")
        body_raw = _decode_decimal_bytes(groups["body_json"])
        try:
            body = RequestBody.model_validate_json(body_raw)
        except (ValidationError, ValueError) as error:
            raise AssertionError("fake peer observed an invalid request body") from error
        if canonical_body_bytes(body) != body_raw:
            raise AssertionError("fake peer observed noncanonical request body bytes")
        request_hash = hashlib.sha256(body_raw).hexdigest()
        if request_hash != groups["request_hash"]:
            raise AssertionError("fake peer observed a request hash mismatch")
        snapshot = self._runtime_snapshot
        expected_lua = {
            "runtime_version": snapshot.runtime_version,
            "runtime_tag": snapshot.runtime_tag,
            "runtime_asset_sha256": snapshot.runtime_asset_sha256,
            "release_id": snapshot.release_id,
            "dispatcher_tag": snapshot.runtime_tag,
        }
        if any(groups[field] != expected for field, expected in expected_lua.items()):
            raise AssertionError("fake peer Lua runtime identity differs from configured snapshot")
        expected_body = {
            "protocol": snapshot.protocol,
            "runtime_version": snapshot.runtime_version,
            "runtime_tag": snapshot.runtime_tag,
            "runtime_asset_sha256": snapshot.runtime_asset_sha256,
            "release_id": snapshot.release_id,
            "operation_manifest_sha256": snapshot.operation_manifest_sha256,
        }
        if any(getattr(body, field) != expected for field, expected in expected_body.items()):
            raise AssertionError("fake peer body runtime identity differs from configured snapshot")
        if (
            body.expected_lineage_id is not None
            and body.expected_lineage_id != self._scenario_lineage_id
        ):
            raise AssertionError("fake peer request lineage differs from configured scenario")
        invocation = self._registry.resolve_wire_invocation(body.operation, body.arguments)
        delivery = PreparedDelivery(
            request_id=request_id,
            delivery_id=delivery_id,
            delivery_kind="request",
            request_hash=request_hash,
            body_json=body_raw,
        )
        if render_delivery_lua(delivery, snapshot) != raw:
            raise AssertionError("fake peer delivery is not the real renderer's exact output")
        return delivery, body, invocation

    async def _apply_action(
        self,
        action: PeerAction,
        delivery: PreparedDelivery,
        body: RequestBody,
        invocation: FrozenInvocation,
    ) -> None:
        if type(action) is Delay:
            delayed = action
            await asyncio.sleep(delayed.seconds)
            await self._apply_action(delayed.then, delivery, body, invocation)
            return
        if type(action) is Respond:
            raw = self._response_bytes(action, delivery, body, invocation)
            self._write_response(delivery.request_id, raw)
            return
        if type(action) is WritePartialThenComplete:
            partial = action
            raw = self._response_bytes(partial.response, delivery, body, invocation)
            split = max(1, len(raw) // 2)
            if split >= len(raw):
                raise AssertionError("fake peer partial response is not a strict prefix")
            self._write_response(delivery.request_id, raw[:split], trace=False)
            await asyncio.sleep(partial.delay_seconds)
            self._write_response(delivery.request_id, raw)
            return
        if type(action) is WriteMismatchedDelivery:
            mismatch = action
            wrong_id = uuid4()
            while wrong_id == delivery.delivery_id:
                wrong_id = uuid4()
            wrong = self._response_bytes(
                mismatch.response,
                delivery,
                body,
                invocation,
                delivery_id=wrong_id,
            )
            self._write_response(delivery.request_id, wrong, trace=False)
            await asyncio.sleep(mismatch.delay_seconds)
            correct = self._response_bytes(mismatch.response, delivery, body, invocation)
            self._write_response(delivery.request_id, correct)
            return
        if type(action) is WriteMalformedComments:
            malformed = action
            self._write_response(
                delivery.request_id,
                _canonical_bytes({"Comments": malformed.comments}),
            )
            return
        if type(action) is StaySilent:
            return
        raise AssertionError("fake peer action dispatch received an unknown exact type")

    def _response_bytes(
        self,
        response: Respond,
        delivery: PreparedDelivery,
        body: RequestBody,
        invocation: FrozenInvocation,
        *,
        delivery_id: UUID | None = None,
    ) -> bytes:
        if response.result is not None:
            try:
                validated = invocation.result_adapter.validate_python(response.result)
                result = cast(
                    JsonValue,
                    invocation.result_adapter.dump_python(validated, mode="json"),
                )
            except (ValidationError, TypeError, ValueError) as error:
                raise AssertionError(
                    "fake peer Respond result is invalid for observed invocation"
                ) from error
            normalized_error = None
            ok = True
        else:
            if response.error is None:
                raise AssertionError("fake peer Respond lost its error")
            try:
                validated_error = ResponseError.model_validate(response.error)
            except ValidationError as error:
                raise AssertionError("fake peer Respond error became invalid") from error
            result = None
            normalized_error = validated_error.model_dump(mode="json", exclude_none=False)
            ok = False

        is_status = body.operation == "bridge.status"
        if is_status:
            if not isinstance(invocation.wire_arguments, BridgeStatusWireArgs):
                raise AssertionError("fake peer status invocation has wrong typed wire arguments")
            activation_id = invocation.wire_arguments.activation_candidate
        else:
            activation_id = body.expected_activation_id
        if activation_id is None:
            raise AssertionError("fake peer cannot respond without a correlated activation")
        envelope: dict[str, object] = {
            "protocol": self._runtime_snapshot.protocol,
            "request_id": str(delivery.request_id),
            "delivery_id": str(delivery.delivery_id if delivery_id is None else delivery_id),
            "request_hash": delivery.request_hash,
            "ok": ok,
            "result": result,
            "error": normalized_error,
            "scenario_time": "2026-07-10T13:00:00Z",
            "scenario_lineage_id": str(self._scenario_lineage_id),
            "activation_id": str(activation_id),
            "operation_manifest_sha256": self._runtime_snapshot.operation_manifest_sha256,
            "bridge_version": self._runtime_snapshot.runtime_version,
            "runtime_tag": self._runtime_snapshot.runtime_tag,
            "runtime_asset_sha256": self._runtime_snapshot.runtime_asset_sha256,
            "release_id": self._runtime_snapshot.release_id,
        }
        for key, value in response.envelope_overrides:
            envelope[key] = value
        try:
            normalized = ResponseEnvelope.model_validate(envelope).model_dump(
                mode="json", exclude_none=False
            )
        except ValidationError as error:
            raise AssertionError("fake peer envelope overrides are structurally invalid") from error
        inner = _canonical_bytes(normalized).decode("utf-8")
        return _canonical_bytes({"Comments": inner})

    def _write_response(self, request_id: UUID, raw: bytes, *, trace: bool = True) -> None:
        path = self._paths.response_path(request_id)
        path.write_bytes(raw)
        if trace:
            self._trace.append("peer.response_written")

    def _rebuild_invocation(self, invocation: object) -> ResolvedInvocation:
        if type(invocation) is not ResolvedInvocation:
            raise ValueError("result_for requires an exact ResolvedInvocation")
        resolved = invocation
        public = resolved.public_arguments.model_dump(mode="json", exclude_unset=True)
        trusted = None
        if resolved.contract.name == "bridge.status":
            if not isinstance(resolved.wire_arguments, BridgeStatusWireArgs):
                raise AssertionError("status invocation has invalid wire arguments")
            trusted = {"activation_candidate": resolved.wire_arguments.activation_candidate}
        rebuilt = self._registry.resolve_invocation(
            resolved.contract.name,
            public,
            trusted,
        )
        if rebuilt != resolved:
            raise AssertionError("result_for invocation does not rebuild exactly")
        frozen = self._registry.resolve_wire_invocation(
            resolved.contract.name,
            resolved.wire_arguments.model_dump(mode="json"),
        )
        if not _same_frozen_projection(frozen, rebuilt):
            raise AssertionError("result_for frozen invocation projection drifted")
        return rebuilt

    def _baseline_result(self, invocation: ResolvedInvocation) -> dict[str, JsonValue]:
        name = invocation.contract.name
        if name == "bridge.status":
            wire = invocation.wire_arguments
            if not isinstance(wire, BridgeStatusWireArgs):
                raise AssertionError("status baseline requires BridgeStatusWireArgs")
            return cast(
                dict[str, JsonValue],
                {
                    "protocol": self._runtime_snapshot.protocol,
                    "runtime_version": self._runtime_snapshot.runtime_version,
                    "runtime_tag": self._runtime_snapshot.runtime_tag,
                    "runtime_asset_sha256": self._runtime_snapshot.runtime_asset_sha256,
                    "release_id": self._runtime_snapshot.release_id,
                    "build": self._build_number,
                    "manifest_sha256": self._runtime_snapshot.operation_manifest_sha256,
                    "lineage_id": str(self._scenario_lineage_id),
                    "activation_id": str(wire.activation_candidate),
                    "installed_event_names": ["CMOAgentBridge Poll"],
                    "installed_action_names": ["CMOAgentBridge Poll Action"],
                    "installed_trigger_names": ["CMOAgentBridge Regular Time"],
                    "pending_request_id": None,
                    "quarantined": False,
                    "paused_capability": True,
                    "poll_interval_seconds": 5,
                    "safe_payload_bytes": 65_536,
                    "verified_ledger_entries": 64,
                    "effective_ledger_capacity": 64,
                },
            )
        if name == "scenario.get":
            return cast(
                dict[str, JsonValue],
                {
                    "guid": "SCENARIO-1",
                    "title": "Bridge test scenario",
                    "file_name": "bridge-test.scen",
                    "file_name_path": "C:\\CMO\\Scenarios",
                    "current_time": "2026-07-10T13:00:00Z",
                    "current_time_seconds": 100.0,
                    "start_time": "2026-07-10T12:00:00Z",
                    "start_time_seconds": 0.0,
                    "duration": "01:00:00",
                    "duration_seconds": 3600.0,
                    "complexity": 2,
                    "difficulty": 3,
                    "setting": "Test",
                    "database": "DB3000",
                    "save_version": "1",
                    "started": True,
                    "player_side_guid": "SIDE-1",
                    "time_compression": 1.0,
                    "campaign_score": 42,
                },
            )
        side = cast(
            dict[str, JsonValue],
            {
                "guid": "SIDE-1",
                "name": "Blue",
                "awareness": "Omniscient",
                "proficiency": "Regular",
                "computer_controlled_only": False,
                "unit_count": 1,
                "contact_count": 1,
                "mission_count": 1,
            },
        )
        unit = cast(
            dict[str, JsonValue],
            {
                "guid": "UNIT-1",
                "dbid": 1,
                "name": "Alpha",
                "side_name": "Blue",
                "type": "Aircraft",
                "subtype": None,
                "category": "Fighter",
                "class_name": "Test Class",
                "latitude": 1.0,
                "longitude": 2.0,
                "altitude": 3000.0,
                "speed": 250.0,
                "heading": 90.0,
                "throttle": "Cruise",
                "proficiency": "Regular",
                "fuel_state": "Full",
                "weapon_state": "Ready",
                "unit_state": "Unassigned",
                "operating": True,
                "mission_guid": None,
                "mission_name": None,
                "loadout_dbid": None,
            },
        )
        contact = cast(
            dict[str, JsonValue],
            {
                "guid": "CONTACT-1",
                "name": "Unknown",
                "observer_side_guid": "SIDE-1",
                "type": "Air",
                "type_description": "Aircraft",
                "classification": "Known",
                "posture": "Unfriendly",
                "latitude": 3.0,
                "longitude": 4.0,
                "altitude": 1000.0,
                "speed": 200.0,
                "heading": 180.0,
                "actual_unit_guid": None,
                "actual_unit_dbid": None,
            },
        )
        mission = cast(
            dict[str, JsonValue],
            {
                "guid": "MISSION-1",
                "name": "Patrol",
                "side_name": "Blue",
                "mission_class": "patrol",
                "mission_class_string": "Patrol",
                "active": True,
                "start_time": None,
                "end_time": None,
                "assigned_unit_guids": ["UNIT-1"],
                "target_guids": [],
                "patrol_type": "aaw",
                "strike_type": None,
                "reference_point_guids": ["RP-1"],
                "destination_guid": None,
                "flight_size": 2,
                "one_third_rule": False,
            },
        )
        doctrine = cast(
            dict[str, JsonValue],
            {
                "scope": "side",
                "target_guid": "SIDE-1",
                "actual": True,
                "weapon_control_air": "Tight",
                "weapon_control_surface": "Tight",
                "weapon_control_subsurface": "Hold",
                "weapon_control_land": "Hold",
                "nuclear_use": False,
                "refuel_unrep": "Never",
                "radar": "Active",
                "sonar": "Passive",
                "oecm": "Passive",
            },
        )
        if name in {"side.list", "unit.list", "contact.list", "mission.list"}:
            item = {
                "side.list": side,
                "unit.list": unit,
                "contact.list": contact,
                "mission.list": mission,
            }[name]
            fields = getattr(invocation.wire_arguments, "fields", None)
            if fields is not None:
                allowed = {"guid", *cast(list[str], fields)}
                item = {key: value for key, value in item.items() if key in allowed}
            return cast(dict[str, JsonValue], {"items": [item], "next_cursor": None})
        if name == "unit.get":
            return unit
        if name == "mission.get":
            return mission
        if name == "doctrine.get":
            scope = getattr(invocation.wire_arguments, "scope", "side")
            target = {
                "side": getattr(invocation.wire_arguments, "side_guid", None),
                "unit": getattr(invocation.wire_arguments, "unit_guid", None),
                "mission": getattr(invocation.wire_arguments, "mission_guid", None),
            }[scope]
            doctrine["scope"] = scope
            doctrine["target_guid"] = cast(str, target)
            return doctrine
        if name == "lua.call":
            wire_arguments = invocation.wire_arguments
            if not isinstance(wire_arguments, LuaCallArgs):
                raise AssertionError("lua.call baseline requires LuaCallArgs")
            side = wire_arguments.arguments.get("side")
            if type(side) is not str:
                raise AssertionError("lua.call baseline requires typed score arguments")
            return cast(
                dict[str, JsonValue],
                {"side": side, "score": 42},
            )
        if name == "bridge.reconcile":
            absent = {
                "present": False,
                "request_id": None,
                "state": None,
                "request_hash": None,
            }
            return cast(
                dict[str, JsonValue],
                {
                    "barrier_evidence": absent,
                    "ledger_evidence": absent,
                    "allowed_dispositions": [],
                    "quarantined": False,
                },
            )
        if name == "compat.probe.step":
            step = getattr(invocation.wire_arguments, "step")
            common: dict[str, JsonValue] = {
                "step": step,
                "nonce": "nonce-1",
                "poll_source": "automatic",
            }
            common.update(
                {
                    "observational": {"observed": True},
                    "payload": {
                        "payload_bytes": getattr(invocation.wire_arguments, "candidate_bytes")
                    },
                    "high-speed": {"elapsed_seconds": 0.1},
                    "lineage": {"lineage_id": str(self._scenario_lineage_id)},
                }[step]
            )
            return common
        raise AssertionError(f"fake peer has no complete H8 baseline for {name}")
