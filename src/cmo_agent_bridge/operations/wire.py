from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol, cast

from pydantic import BaseModel, TypeAdapter

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations import models


Adapter = TypeAdapter[object]
SchemaSelector = Mapping[str, object] | None
_PathPart = str | int
_FieldsSetEntry = tuple[tuple[_PathPart, ...], frozenset[str]]


class AdapterRecipe(Protocol):
    def materialize(self) -> Adapter: ...


def materialize_type_adapter(target: object) -> Adapter:
    adapter = cast(Adapter, TypeAdapter(cast(Any, target)))
    adapter.core_schema = deepcopy(adapter.core_schema)
    return adapter


@dataclass(frozen=True, slots=True)
class StaticAdapterRecipe:
    _target: object

    def materialize(self) -> Adapter:
        return materialize_type_adapter(self._target)


RecipeLookup = Callable[[str], AdapterRecipe]


def _collect_fields_sets(
    value: object,
    path: tuple[_PathPart, ...],
    entries: list[_FieldsSetEntry],
) -> None:
    if isinstance(value, BaseModel):
        entries.append((path, frozenset(value.model_fields_set)))
        for field in type(value).model_fields:
            _collect_fields_sets(getattr(value, field), (*path, field), entries)
        return
    if isinstance(value, (list, tuple)):
        sequence = cast(Sequence[object], value)
        for index, item in enumerate(sequence):
            _collect_fields_sets(item, (*path, index), entries)
        return
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        for key, item in mapping.items():
            if not isinstance(key, (str, int)):
                raise RuntimeError("model snapshot paths require string or integer mapping keys")
            _collect_fields_sets(item, (*path, key), entries)


def _value_at_path(root: BaseModel, path: tuple[_PathPart, ...]) -> object:
    current: object = root
    for part in path:
        if isinstance(current, BaseModel):
            if not isinstance(part, str):
                raise RuntimeError("model snapshot field path is invalid")
            current = getattr(current, part)
        elif isinstance(current, (list, tuple)):
            if not isinstance(part, int):
                raise RuntimeError("model snapshot sequence path is invalid")
            sequence = cast(Sequence[object], current)
            current = sequence[part]
        elif isinstance(current, dict):
            mapping = cast(dict[object, object], current)
            current = mapping[part]
        else:
            raise RuntimeError("model snapshot path cannot be restored")
    return current


@dataclass(frozen=True, slots=True)
class ModelSnapshot:
    model_type: type[BaseModel]
    canonical_json: bytes
    fields_sets: tuple[_FieldsSetEntry, ...]

    def thaw(self) -> BaseModel:
        restored = self.model_type.model_validate_json(self.canonical_json)
        if type(restored) is not self.model_type:
            raise RuntimeError("model snapshot restored the wrong concrete type")
        for path, fields_set in self.fields_sets:
            nested = _value_at_path(restored, path)
            if not isinstance(nested, BaseModel):
                raise RuntimeError("model snapshot fields-set path is invalid")
            object.__setattr__(nested, "__pydantic_fields_set__", set(fields_set))
        return restored


def snapshot_model(model: BaseModel) -> ModelSnapshot:
    fields_sets: list[_FieldsSetEntry] = []
    _collect_fields_sets(model, (), fields_sets)
    canonical_json = json.dumps(
        model.model_dump(mode="json"),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return ModelSnapshot(
        model_type=type(model),
        canonical_json=canonical_json,
        fields_sets=tuple(fields_sets),
    )


class WireResolver(Protocol):
    def from_public(
        self,
        public_arguments: BaseModel,
        trusted_enrichment: Mapping[str, object],
    ) -> BaseModel: ...

    def from_wire(self, wire_arguments: Mapping[str, object]) -> BaseModel: ...


def _validated_model(adapter: Adapter, value: object) -> BaseModel:
    validated = adapter.validate_python(value)
    if not isinstance(validated, BaseModel):
        raise RuntimeError("wire adapter did not produce a Pydantic model")
    return validated


@dataclass(frozen=True, slots=True)
class ModelWireResolver:
    _wire_recipe: AdapterRecipe

    @property
    def wire_adapter(self) -> Adapter:
        return self._wire_recipe.materialize()

    def from_public(
        self,
        public_arguments: BaseModel,
        trusted_enrichment: Mapping[str, object],
    ) -> BaseModel:
        wire_value = public_arguments.model_dump(mode="python", exclude_unset=True)
        wire_value.update(trusted_enrichment)
        return _validated_model(self.wire_adapter, wire_value)

    def from_wire(self, wire_arguments: Mapping[str, object]) -> BaseModel:
        return _validated_model(self.wire_adapter, dict(wire_arguments))


@dataclass(frozen=True, slots=True)
class EmconWireResolver:
    _wire_recipe: AdapterRecipe

    @property
    def wire_adapter(self) -> Adapter:
        return self._wire_recipe.materialize()

    def from_public(
        self,
        public_arguments: BaseModel,
        trusted_enrichment: Mapping[str, object],
    ) -> BaseModel:
        if not isinstance(public_arguments, models.EmconSetArgs):
            raise RuntimeError("EMCON public adapter produced the wrong model")
        wire_value: dict[str, object] = {
            "scope": public_arguments.scope,
            "target_guid": public_arguments.target_guid,
            "emcon": public_arguments.to_wire_grammar(),
        }
        wire_value.update(trusted_enrichment)
        return _validated_model(self.wire_adapter, wire_value)

    def from_wire(self, wire_arguments: Mapping[str, object]) -> BaseModel:
        return _validated_model(self.wire_adapter, dict(wire_arguments))


@dataclass(frozen=True, slots=True)
class ReconcileWireResolver:
    _wire_recipe: AdapterRecipe

    @property
    def wire_adapter(self) -> Adapter:
        return self._wire_recipe.materialize()

    def from_public(
        self,
        public_arguments: BaseModel,
        trusted_enrichment: Mapping[str, object],
    ) -> BaseModel:
        if not isinstance(public_arguments, models.ReconcileArgs):
            raise RuntimeError("reconcile public adapter produced the wrong model")
        wire_value: dict[str, object] = {}
        if public_arguments.request_id is not None:
            wire_value = {
                "request_id": public_arguments.request_id,
                "disposition": public_arguments.disposition,
            }
        wire_value.update(trusted_enrichment)
        return _validated_model(self.wire_adapter, wire_value)

    def from_wire(self, wire_arguments: Mapping[str, object]) -> BaseModel:
        return _validated_model(self.wire_adapter, dict(wire_arguments))


@dataclass(frozen=True, slots=True)
class _LuaAllowlistPolicy:
    _arguments_recipe: AdapterRecipe

    @property
    def arguments_adapter(self) -> Adapter:
        return self._arguments_recipe.materialize()


@dataclass(frozen=True, slots=True)
class LuaAllowlistWireResolver:
    _wire_recipe: AdapterRecipe
    _policies: Mapping[str, _LuaAllowlistPolicy]

    @property
    def wire_adapter(self) -> Adapter:
        return self._wire_recipe.materialize()

    @classmethod
    def build(
        cls,
        wire_recipe: AdapterRecipe,
        policies: Mapping[str, Mapping[str, object]],
        recipe_lookup: RecipeLookup,
    ) -> LuaAllowlistWireResolver:
        frozen_policies: dict[str, _LuaAllowlistPolicy] = {}
        for function, policy in policies.items():
            arguments_model = policy.get("arguments_model")
            if not isinstance(arguments_model, str):
                raise RuntimeError("Lua allowlist arguments_model must be a string")
            frozen_policies[function] = _LuaAllowlistPolicy(
                _arguments_recipe=recipe_lookup(arguments_model)
            )
        return cls(wire_recipe, MappingProxyType(frozen_policies))

    def from_public(
        self,
        public_arguments: BaseModel,
        trusted_enrichment: Mapping[str, object],
    ) -> BaseModel:
        wire_value = public_arguments.model_dump(mode="python")
        wire_value.update(trusted_enrichment)
        return self._resolve(wire_value)

    def from_wire(self, wire_arguments: Mapping[str, object]) -> BaseModel:
        return self._resolve(dict(wire_arguments))

    def _resolve(self, value: Mapping[str, object]) -> BaseModel:
        outer = _validated_model(self.wire_adapter, dict(value))
        function = getattr(outer, "function")
        if not isinstance(function, str):
            raise RuntimeError("Lua function selector is not a validated string")
        try:
            policy = self._policies[function]
        except KeyError as error:
            raise BridgeError(
                ErrorCode.POLICY_DENIED,
                f"Lua function {function!r} is not allowlisted",
                {"function": function},
            ) from error
        specific_arguments = _validated_model(policy.arguments_adapter, getattr(outer, "arguments"))
        return _validated_model(
            self.wire_adapter,
            {
                "function": function,
                "arguments": specific_arguments.model_dump(mode="python"),
            },
        )


def _selector_json_value(value: object) -> object:
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        if not all(isinstance(key, str) for key in mapping):
            raise TypeError("schema selector object keys must be strings")
        return {cast(str, key): _selector_json_value(item) for key, item in mapping.items()}
    if isinstance(value, (list, tuple)):
        sequence = cast(Sequence[object], value)
        return [_selector_json_value(item) for item in sequence]
    return value


def schema_descriptor_bytes(
    *,
    manifest_sha256: str,
    operation: str,
    role: str,
    factory: str,
    model: str,
    selector: SchemaSelector,
) -> bytes:
    descriptor: dict[str, object] = {
        "format": "cmo-agent-bridge/schema-binding/1",
        "manifest_sha256": manifest_sha256,
        "operation": operation,
        "role": role,
        "factory": factory,
        "model": model,
        "selector": _selector_json_value(selector),
    }
    return json.dumps(
        descriptor,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
