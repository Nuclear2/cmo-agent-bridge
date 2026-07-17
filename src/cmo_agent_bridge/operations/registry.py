from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from copy import copy
from dataclasses import dataclass, field
from functools import lru_cache
from types import GenericAlias, MappingProxyType
from typing import Any, Literal, cast
from weakref import ReferenceType, ref

from pydantic import BaseModel, TypeAdapter, ValidationError, create_model
from pydantic_core import InitErrorDetails, PydanticCustomError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations import models
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256, OPERATIONS
from cmo_agent_bridge.operations.kinds import ExecutionTarget, OperationClass
from cmo_agent_bridge.operations.wire import (
    AdapterRecipe,
    EmconWireResolver,
    LuaAllowlistWireResolver,
    ModelSnapshot,
    ModelWireResolver,
    ReconcileWireResolver,
    SchemaSelector,
    StaticAdapterRecipe,
    WireResolver,
    materialize_type_adapter,
    schema_descriptor_bytes,
    snapshot_model,
)


Adapter = TypeAdapter[object]
EffectiveClassResolver = Callable[[BaseModel], OperationClass]
SchemaRole = Literal["result", "recovery"]
_MISSING_ADAPTER_TARGET = object()
_MISSING_RECIPE_TOKEN = object()
_RECIPE_TOKEN_ATTRIBUTE = "_cmo_agent_bridge_recipe_token"


@dataclass(frozen=True, slots=True)
class _RecipeToken:
    _adapter_ref: ReferenceType[TypeAdapter[object]]
    _adapter_id: int
    _recipe: AdapterRecipe
    _schema_sha256: str

    def recipe_for(self, adapter: Adapter) -> AdapterRecipe:
        if (
            self._adapter_id != id(adapter)
            or self._adapter_ref() is not adapter
            or not _is_supported_internal_recipe(self._recipe)
            or self._schema_sha256 != _adapter_schema_sha256(adapter)
        ):
            raise TypeError("adapter recipe token does not match its current adapter")
        return self._recipe


def _is_supported_internal_recipe(value: object) -> bool:
    return type(value) is StaticAdapterRecipe or type(value) is _ProjectedPageRecipe


def _adapter_schema_sha256(adapter: Adapter) -> str:
    try:
        encoded = json.dumps(
            adapter.json_schema(),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise TypeError("adapter schema cannot be fingerprinted") from error
    return hashlib.sha256(encoded).hexdigest()


def _recipe_from_adapter_input(value: object) -> AdapterRecipe:
    if type(value) is not TypeAdapter:
        raise TypeError("adapter fields require an exact Pydantic TypeAdapter")
    adapter = cast(Adapter, value)
    token = adapter.__dict__.get(_RECIPE_TOKEN_ATTRIBUTE, _MISSING_RECIPE_TOKEN)
    if token is not _MISSING_RECIPE_TOKEN:
        if type(token) is not _RecipeToken:
            raise TypeError("adapter carries an invalid recipe token")
        return token.recipe_for(adapter)
    target = getattr(adapter, "_type", _MISSING_ADAPTER_TARGET)
    if target is _MISSING_ADAPTER_TARGET:
        raise TypeError("adapter does not expose a reusable validation target")
    if not isinstance(target, type) or not issubclass(target, BaseModel):
        raise TypeError("adapter fields require a Pydantic model target")
    return StaticAdapterRecipe(target)


def _materialize_public_adapter(recipe: AdapterRecipe) -> Adapter:
    if not _is_supported_internal_recipe(recipe):
        raise TypeError("public adapter requires a supported immutable recipe")
    adapter = recipe.materialize()
    token = _RecipeToken(
        ref(adapter),
        id(adapter),
        recipe,
        _adapter_schema_sha256(adapter),
    )
    setattr(adapter, _RECIPE_TOKEN_ATTRIBUTE, token)
    return adapter


@dataclass(frozen=True, slots=True)
class _AdapterField:
    _slot: str

    def __get__(self, instance: object | None, _owner: type[object] | None = None) -> Adapter:
        if instance is None:
            raise AttributeError
        recipe = cast(AdapterRecipe, object.__getattribute__(instance, self._slot))
        return _materialize_public_adapter(recipe)

    def __set__(self, instance: object, value: object) -> None:
        object.__setattr__(instance, self._slot, _recipe_from_adapter_input(value))


@dataclass(frozen=True, slots=True)
class _ModelSnapshotField:
    _slot: str

    def __get__(self, instance: object | None, _owner: type[object] | None = None) -> BaseModel:
        if instance is None:
            raise AttributeError
        snapshot = cast(ModelSnapshot, object.__getattribute__(instance, self._slot))
        return snapshot.thaw()

    def __set__(self, instance: object, value: object) -> None:
        if not isinstance(value, BaseModel):
            raise TypeError("argument fields require a Pydantic model")
        object.__setattr__(instance, self._slot, snapshot_model(value))


@dataclass(frozen=True, slots=True)
class _ResultSelection:
    factory: str
    model: str
    selector: SchemaSelector
    _adapter_recipe: AdapterRecipe

    def __post_init__(self) -> None:
        if self.selector is None:
            return
        frozen = _freeze_selector_value(self.selector)
        if not isinstance(frozen, Mapping):
            raise RuntimeError("result selector must be a JSON object or null")
        object.__setattr__(self, "selector", cast(Mapping[str, object], frozen))

    @property
    def adapter(self) -> Adapter:
        return self._adapter_recipe.materialize()


def _freeze_selector_value(value: object) -> object:
    if value is None or type(value) in {str, int, float, bool}:
        try:
            json.dumps(value, allow_nan=False)
        except (TypeError, ValueError) as error:
            raise RuntimeError("result selector contains a non-finite scalar") from error
        return value
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        if not all(isinstance(key, str) for key in mapping):
            raise RuntimeError("result selector object keys must be strings")
        copied = {cast(str, key): _freeze_selector_value(item) for key, item in mapping.items()}
        return MappingProxyType(copied)
    if isinstance(value, (list, tuple)):
        sequence = cast(Sequence[object], value)
        return tuple(_freeze_selector_value(item) for item in sequence)
    raise RuntimeError("result selector contains a non-JSON value")


ResultFactory = Callable[[BaseModel], _ResultSelection]
RecoveryFactory = Callable[[BaseModel], _ResultSelection | None]


@dataclass(frozen=True, slots=True)
class OperationContract:
    name: str
    target: ExecutionTarget
    base_class: OperationClass
    _public_arguments_recipe: AdapterRecipe = field(repr=False)
    _wire_arguments_recipe: AdapterRecipe = field(repr=False)
    wire_resolver: WireResolver
    effective_class_resolver: EffectiveClassResolver
    wire_result_factory: ResultFactory
    public_result_factory: ResultFactory
    recovery_factory: RecoveryFactory
    confirmation_required: bool
    expose_mcp: bool

    @property
    def public_arguments_adapter(self) -> Adapter:
        return self._public_arguments_recipe.materialize()

    @property
    def wire_arguments_adapter(self) -> Adapter:
        return self._wire_arguments_recipe.materialize()


@dataclass(frozen=True, eq=False)
class SchemaBinding:
    __slots__ = ("schema_id", "_adapter_recipe")

    schema_id: models.Sha256
    adapter: Adapter = cast(Adapter, _AdapterField("_adapter_recipe"))

    @classmethod
    def _from_recipe(cls, schema_id: models.Sha256, adapter_recipe: AdapterRecipe) -> SchemaBinding:
        instance = object.__new__(cls)
        object.__setattr__(instance, "schema_id", schema_id)
        object.__setattr__(instance, "_adapter_recipe", adapter_recipe)
        return instance

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        other_binding = cast(SchemaBinding, other)
        return self.schema_id == other_binding.schema_id and object.__getattribute__(
            self, "_adapter_recipe"
        ) == object.__getattribute__(other_binding, "_adapter_recipe")

    def __hash__(self) -> int:
        return hash((self.schema_id, object.__getattribute__(self, "_adapter_recipe")))


@dataclass(frozen=True, eq=False)
class FrozenInvocation:
    __slots__ = (
        "contract",
        "_wire_arguments_snapshot",
        "effective_class",
        "result_schema",
        "recovery_schema",
        "_public_result_recipe",
    )

    contract: OperationContract
    wire_arguments: BaseModel = cast(BaseModel, _ModelSnapshotField("_wire_arguments_snapshot"))
    effective_class: OperationClass  # pyright: ignore[reportGeneralTypeIssues]
    result_schema: SchemaBinding  # pyright: ignore[reportGeneralTypeIssues]
    recovery_schema: SchemaBinding | None  # pyright: ignore[reportGeneralTypeIssues]
    public_result_adapter: Adapter = cast(Adapter, _AdapterField("_public_result_recipe"))

    @classmethod
    def _from_recipe_parts(
        cls,
        *,
        contract: OperationContract,
        wire_arguments: BaseModel,
        effective_class: OperationClass,
        result_schema: SchemaBinding,
        recovery_schema: SchemaBinding | None,
        public_result_recipe: AdapterRecipe,
    ) -> FrozenInvocation:
        instance = object.__new__(cls)
        object.__setattr__(instance, "contract", contract)
        object.__setattr__(instance, "_wire_arguments_snapshot", snapshot_model(wire_arguments))
        object.__setattr__(instance, "effective_class", effective_class)
        object.__setattr__(instance, "result_schema", result_schema)
        object.__setattr__(instance, "recovery_schema", recovery_schema)
        object.__setattr__(instance, "_public_result_recipe", public_result_recipe)
        return instance

    def _comparison_state(self) -> tuple[object, ...]:
        return (
            self.contract,
            object.__getattribute__(self, "_wire_arguments_snapshot"),
            self.effective_class,
            self.result_schema,
            self.recovery_schema,
            object.__getattribute__(self, "_public_result_recipe"),
        )

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        return self._comparison_state() == cast(FrozenInvocation, other)._comparison_state()

    @property
    def result_adapter(self) -> Adapter:
        return self.result_schema.adapter

    @property
    def recovery_adapter(self) -> Adapter | None:
        if self.recovery_schema is None:
            return None
        return self.recovery_schema.adapter


@dataclass(frozen=True, eq=False)
class ResolvedInvocation(FrozenInvocation):
    __slots__ = ("_public_arguments_snapshot",)

    public_arguments: BaseModel = cast(BaseModel, _ModelSnapshotField("_public_arguments_snapshot"))

    @classmethod
    def _from_resolved_recipe_parts(
        cls,
        *,
        contract: OperationContract,
        wire_arguments: BaseModel,
        effective_class: OperationClass,
        result_schema: SchemaBinding,
        recovery_schema: SchemaBinding | None,
        public_result_recipe: AdapterRecipe,
        public_arguments: BaseModel,
    ) -> ResolvedInvocation:
        instance = object.__new__(cls)
        object.__setattr__(instance, "contract", contract)
        object.__setattr__(instance, "_wire_arguments_snapshot", snapshot_model(wire_arguments))
        object.__setattr__(instance, "effective_class", effective_class)
        object.__setattr__(instance, "result_schema", result_schema)
        object.__setattr__(instance, "recovery_schema", recovery_schema)
        object.__setattr__(instance, "_public_result_recipe", public_result_recipe)
        object.__setattr__(instance, "_public_arguments_snapshot", snapshot_model(public_arguments))
        return instance

    def _comparison_state(self) -> tuple[object, ...]:
        return (
            *super()._comparison_state(),
            object.__getattribute__(self, "_public_arguments_snapshot"),
        )


def _create_strict_model(
    name: str, field_definitions: Mapping[str, tuple[object, object]]
) -> type[BaseModel]:
    dynamic_fields = cast(dict[str, Any], dict(field_definitions))
    return create_model(name, __base__=models.StrictModel, **dynamic_fields)


@lru_cache(maxsize=None)
def _model_recipe(reference: str) -> AdapterRecipe:
    try:
        model_type = getattr(models, reference)
    except AttributeError as error:
        raise RuntimeError(f"generated manifest references unknown model {reference!r}") from error
    return StaticAdapterRecipe(model_type)


def _validated_model(adapter: Adapter, value: object) -> BaseModel:
    validated = adapter.validate_python(value)
    if not isinstance(validated, BaseModel):
        raise RuntimeError("operation argument adapter did not produce a Pydantic model")
    return validated


@dataclass(frozen=True, slots=True)
class _ConstantClassResolver:
    operation_class: OperationClass

    def __call__(self, _arguments: BaseModel) -> OperationClass:
        return self.operation_class


@dataclass(frozen=True, slots=True)
class _MappedClassResolver:
    classes: Mapping[str, OperationClass]
    field: str

    def __call__(self, arguments: BaseModel) -> OperationClass:
        selected = getattr(arguments, self.field)
        if not isinstance(selected, str):
            raise RuntimeError(f"{self.field} did not resolve to a validated exact string")
        try:
            return self.classes[selected]
        except KeyError as error:
            raise BridgeError(
                ErrorCode.POLICY_DENIED,
                f"{selected!r} is not permitted by the operation manifest",
                {self.field: selected},
            ) from error


def _constant_resolver(base_class: OperationClass) -> EffectiveClassResolver:
    return _ConstantClassResolver(base_class)


def _mapped_resolver(mapping: Mapping[str, object], field: str) -> EffectiveClassResolver:
    classes = {
        key: OperationClass(value["class"] if isinstance(value, dict) else value)
        for key, value in mapping.items()
    }
    return _MappedClassResolver(MappingProxyType(classes), field)


def _reconcile_class_resolver(arguments: BaseModel) -> OperationClass:
    if isinstance(arguments, models.ReconcileProbeWireArgs):
        return OperationClass.READ
    if isinstance(arguments, models.ReconcileCommitWireArgs):
        return OperationClass.RECONCILE
    raise RuntimeError("reconcile wire resolver produced an unknown branch")


@dataclass(frozen=True, slots=True)
class _StaticResultFactory:
    selection: _ResultSelection

    def __call__(self, _arguments: BaseModel) -> _ResultSelection:
        return self.selection


@dataclass(frozen=True, slots=True)
class _LuaResultFactory:
    selections: Mapping[str, _ResultSelection]

    def __call__(self, arguments: BaseModel) -> _ResultSelection:
        function = getattr(arguments, "function")
        if not isinstance(function, str):
            raise RuntimeError("Lua result selector is not a validated exact string")
        try:
            return self.selections[function]
        except KeyError as error:
            raise BridgeError(
                ErrorCode.POLICY_DENIED,
                f"Lua function {function!r} is not allowlisted",
                {"function": function},
            ) from error


@dataclass(frozen=True, slots=True)
class _DiscriminatedResultFactory:
    argument_field: str
    selections: Mapping[object, _ResultSelection]

    def __call__(self, arguments: BaseModel) -> _ResultSelection:
        selected = getattr(arguments, self.argument_field)
        _validate_json_scalar(selected, self.argument_field)
        try:
            return self.selections[selected]
        except (KeyError, TypeError) as error:
            raise RuntimeError(
                f"no result adapter for {self.argument_field}={selected!r}"
            ) from error


@dataclass(frozen=True, slots=True)
class _PagedResultFactory:
    reference: str
    result_model: type[BaseModel]

    def __call__(self, arguments: BaseModel) -> _ResultSelection:
        fields = getattr(arguments, "fields", None)
        selected_fields: tuple[str, ...] | None
        selector: SchemaSelector
        if fields is None:
            selected_fields = None
            selector = None
        else:
            raw_fields = cast(list[str], fields)
            selected_fields = tuple(sorted(set(("guid", *raw_fields))))
            selector = {"fields": list(selected_fields)}
        return _ResultSelection(
            "paged",
            self.reference,
            selector,
            _projected_page_recipe(self.reference, self.result_model, selected_fields),
        )


@dataclass(frozen=True, slots=True)
class _ReconcileResultFactory:
    probe: _ResultSelection
    commit: _ResultSelection

    def __call__(self, arguments: BaseModel) -> _ResultSelection:
        if isinstance(arguments, models.ReconcileProbeWireArgs):
            return self.probe
        if isinstance(arguments, models.ReconcileCommitWireArgs):
            return self.commit
        raise RuntimeError("reconcile wire resolver produced an unknown result branch")


def _named_result_factory(reference: str) -> ResultFactory:
    return _StaticResultFactory(
        _ResultSelection("named", reference, None, _model_recipe(reference))
    )


def _lua_result_factory(mapping: Mapping[str, object]) -> ResultFactory:
    selections: dict[str, _ResultSelection] = {}
    for function, policy_value in mapping.items():
        policy = _as_mapping(policy_value, f"lua.call resolver policy for {function}")
        model = _as_string(policy["result_model"], "result_model")
        selections[function] = _ResultSelection(
            "lua_allowlist",
            model,
            {"function": function},
            _model_recipe(model),
        )
    return _LuaResultFactory(MappingProxyType(selections))


def _discriminated_result_factory(data: Mapping[str, object]) -> ResultFactory:
    argument_field = _as_string(data["argument_field"], "result argument_field")
    raw_models = _as_mapping(data["models"], "discriminated result models")
    selections: dict[object, _ResultSelection] = {
        value: _ResultSelection(
            "discriminated",
            _as_string(model_name, f"result model for {value}"),
            {"field": argument_field, "value": value},
            _model_recipe(_as_string(model_name, f"result model for {value}")),
        )
        for value, model_name in raw_models.items()
    }
    return _DiscriminatedResultFactory(argument_field, MappingProxyType(selections))


def _paged_result_factory(reference: str) -> ResultFactory:
    try:
        result_model = getattr(models, reference)
    except AttributeError as error:
        raise RuntimeError(f"unknown projected result model {reference!r}") from error
    if not isinstance(result_model, type) or not issubclass(result_model, BaseModel):
        raise RuntimeError(f"projected result {reference!r} is not a Pydantic model")
    return _PagedResultFactory(reference, result_model)


def _reconcile_result_factory(data: Mapping[str, object]) -> ResultFactory:
    probe_model = _as_string(data["probe"], "reconcile probe result model")
    commit_model = _as_string(data["commit"], "reconcile commit result model")
    return _ReconcileResultFactory(
        _ResultSelection(
            "reconcile",
            probe_model,
            {"branch": "probe"},
            _model_recipe(probe_model),
        ),
        _ResultSelection(
            "reconcile",
            commit_model,
            {"branch": "commit"},
            _model_recipe(commit_model),
        ),
    )


@dataclass(frozen=True, slots=True)
class _ProjectedPageRecipe:
    reference: str
    result_model: type[BaseModel]
    requested_fields: tuple[str, ...] | None

    def materialize(self) -> Adapter:
        page_model = _build_projected_page_model(
            self.reference, self.result_model, self.requested_fields
        )
        return materialize_type_adapter(page_model)


def _validate_projected_fields(
    result_model: type[BaseModel], requested_fields: tuple[str, ...] | None
) -> None:
    if requested_fields is None:
        return
    allowed = set(result_model.model_fields)
    unknown = [field for field in requested_fields if field not in allowed]
    if unknown:
        error = PydanticCustomError(
            "projection_allowlist",
            "projection fields are not allowlisted: {fields}",
            {"fields": ", ".join(unknown)},
        )
        details: list[InitErrorDetails] = [
            {"type": error, "loc": ("fields",), "input": list(requested_fields)}
        ]
        raise ValidationError.from_exception_data("ProjectionFields", details)


@lru_cache(maxsize=None)
def _projected_page_recipe(
    reference: str,
    result_model: type[BaseModel],
    requested_fields: tuple[str, ...] | None,
) -> AdapterRecipe:
    _validate_projected_fields(result_model, requested_fields)
    return _ProjectedPageRecipe(reference, result_model, requested_fields)


def _build_projected_page_model(
    reference: str,
    result_model: type[BaseModel],
    requested_fields: tuple[str, ...] | None,
) -> type[BaseModel]:
    if requested_fields is None:
        item_model: type[BaseModel] = result_model
        suffix = "Full"
    else:
        _validate_projected_fields(result_model, requested_fields)
        field_definitions: dict[str, tuple[object, object]] = {
            field: (
                result_model.model_fields[field].annotation,
                copy(result_model.model_fields[field]),
            )
            for field in requested_fields
        }
        suffix = "_".join(field.replace("-", "_") for field in requested_fields)
        item_model = _create_strict_model(f"{reference}Projection_{suffix}", field_definitions)

    return _create_strict_model(
        f"{reference}Page_{suffix}",
        {
            "items": (GenericAlias(list, item_model), ...),
            "next_cursor": (str | None, ...),
        },
    )


@dataclass(frozen=True, slots=True)
class _SameRecoveryFactory:
    result_factory: ResultFactory

    def __call__(self, arguments: BaseModel) -> _ResultSelection:
        return self.result_factory(arguments)


@dataclass(frozen=True, slots=True)
class _EffectiveRecoveryFactory:
    resolver: EffectiveClassResolver
    result_factory: ResultFactory

    def __call__(self, arguments: BaseModel) -> _ResultSelection | None:
        effective = self.resolver(arguments)
        if effective in {
            OperationClass.MUTATION,
            OperationClass.DESTRUCTIVE,
            OperationClass.RECONCILE,
        }:
            return self.result_factory(arguments)
        return None


def _same_recovery_factory(result_factory: ResultFactory) -> RecoveryFactory:
    return _SameRecoveryFactory(result_factory)


def _no_recovery_factory(_arguments: BaseModel) -> None:
    return None


def _effective_recovery_factory(
    resolver: EffectiveClassResolver,
    result_factory: ResultFactory,
) -> RecoveryFactory:
    return _EffectiveRecoveryFactory(resolver, result_factory)


def _validate_json_scalar(value: object, label: str) -> None:
    if type(value) not in {str, int, float, bool} and value is not None:
        raise RuntimeError(f"{label} must be a validated JSON scalar")
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise RuntimeError(f"{label} must be a finite JSON scalar") from error


def _as_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} must be a string-keyed mapping")
    untyped_mapping = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in untyped_mapping):
        raise RuntimeError(f"{label} must be a string-keyed mapping")
    return cast(Mapping[str, object], untyped_mapping)


def _as_string(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"{label} must be a string")
    return value


def _as_string_sequence(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise RuntimeError(f"{label} must be an array of strings")
    untyped_items = cast(list[object], value)
    if not all(isinstance(item, str) for item in untyped_items):
        raise RuntimeError(f"{label} must be an array of strings")
    return tuple(cast(str, item) for item in untyped_items)


@dataclass(frozen=True, slots=True, init=False)
class OperationRegistry:
    _manifest_sha256: models.Sha256
    _contracts: Mapping[str, OperationContract]
    _trusted_fields: Mapping[str, frozenset[str]]

    def __init__(self, entries: Sequence[Mapping[str, object]] = OPERATIONS):
        manifest_bytes = json.dumps(
            entries,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        if entries is OPERATIONS and manifest_sha256 != MANIFEST_SHA256:
            raise RuntimeError("generated operation manifest hash is inconsistent")
        contracts: dict[str, OperationContract] = {}
        trusted_fields: dict[str, frozenset[str]] = {}
        for entry in entries:
            name = _as_string(entry["name"], "operation name")
            if name in contracts:
                raise RuntimeError(f"duplicate operation contract: {name}")
            contracts[name] = self._build_contract(entry)
            trusted_fields[name] = frozenset(
                _as_string_sequence(entry["trusted_fields"], f"{name}.trusted_fields")
            )
        object.__setattr__(self, "_manifest_sha256", manifest_sha256)
        object.__setattr__(self, "_contracts", MappingProxyType(contracts))
        object.__setattr__(self, "_trusted_fields", MappingProxyType(trusted_fields))
        self._verify_surface()

    def _build_contract(self, entry: Mapping[str, object]) -> OperationContract:
        name = _as_string(entry["name"], "operation name")
        target = ExecutionTarget(_as_string(entry["target"], f"{name}.target"))
        base_class = OperationClass(_as_string(entry["base_class"], f"{name}.base_class"))
        public_recipe = _model_recipe(
            _as_string(entry["public_arguments_model"], f"{name}.public_arguments_model")
        )
        wire_recipe = _model_recipe(
            _as_string(entry["wire_arguments_model"], f"{name}.wire_arguments_model")
        )
        wire_resolver = self._build_wire_resolver(entry, wire_recipe)

        resolver_name = _as_string(
            entry["effective_class_resolver"], f"{name}.effective_class_resolver"
        )
        if resolver_name == "constant":
            resolver = _constant_resolver(base_class)
        elif resolver_name in {"lua_allowlist", "compat_step"}:
            field = "function" if resolver_name == "lua_allowlist" else "step"
            resolver = _mapped_resolver(
                _as_mapping(entry.get("resolver_data"), f"{name}.resolver_data"), field
            )
        elif resolver_name == "reconcile":
            resolver = _reconcile_class_resolver
        else:
            raise RuntimeError(f"{name}: unknown effective class resolver {resolver_name!r}")

        wire_result_factory = self._build_result_factory(entry, "wire_result_factory")
        public_result_factory = self._build_result_factory(entry, "public_result_factory")

        recovery_name = _as_string(entry["recovery_factory"], f"{name}.recovery_factory")
        if recovery_name == "same":
            recovery_factory = _same_recovery_factory(wire_result_factory)
        elif recovery_name == "none":
            recovery_factory = _no_recovery_factory
        elif recovery_name == "effective":
            recovery_factory = _effective_recovery_factory(resolver, wire_result_factory)
        else:
            raise RuntimeError(f"{name}: unknown recovery factory {recovery_name!r}")

        confirmation_required = entry["confirmation_required"]
        expose_mcp = entry["expose_mcp"]
        if not isinstance(confirmation_required, bool) or not isinstance(expose_mcp, bool):
            raise RuntimeError(f"{name}: boolean contract flags are invalid")
        return OperationContract(
            name=name,
            target=target,
            base_class=base_class,
            _public_arguments_recipe=public_recipe,
            _wire_arguments_recipe=wire_recipe,
            wire_resolver=wire_resolver,
            effective_class_resolver=resolver,
            wire_result_factory=wire_result_factory,
            public_result_factory=public_result_factory,
            recovery_factory=recovery_factory,
            confirmation_required=confirmation_required,
            expose_mcp=expose_mcp,
        )

    def _build_wire_resolver(
        self, entry: Mapping[str, object], wire_recipe: AdapterRecipe
    ) -> WireResolver:
        name = _as_string(entry["name"], "operation name")
        resolver_name = _as_string(entry["wire_resolver"], f"{name}.wire_resolver")
        if resolver_name == "model":
            return ModelWireResolver(wire_recipe)
        if resolver_name == "emcon":
            return EmconWireResolver(wire_recipe)
        if resolver_name == "reconcile":
            return ReconcileWireResolver(wire_recipe)
        if resolver_name == "lua_allowlist":
            raw_policies = _as_mapping(entry.get("resolver_data"), f"{name}.resolver_data")
            policies = {
                function: _as_mapping(policy, f"{name} policy {function}")
                for function, policy in raw_policies.items()
            }
            return LuaAllowlistWireResolver.build(wire_recipe, policies, _model_recipe)
        raise RuntimeError(f"{name}: unknown wire resolver {resolver_name!r}")

    def _build_result_factory(self, entry: Mapping[str, object], field: str) -> ResultFactory:
        name = _as_string(entry["name"], "operation name")
        result_name = _as_string(entry[field], f"{name}.{field}")
        if result_name.startswith("paged:"):
            return _paged_result_factory(result_name.partition(":")[2])
        if result_name == "discriminated":
            return _discriminated_result_factory(
                _as_mapping(entry.get("result_factory_data"), f"{name}.result_factory_data")
            )
        if result_name == "lua_allowlist":
            return _lua_result_factory(
                _as_mapping(entry.get("resolver_data"), f"{name}.resolver_data")
            )
        if result_name == "reconcile":
            return _reconcile_result_factory(
                _as_mapping(entry.get("result_factory_data"), f"{name}.result_factory_data")
            )
        return _named_result_factory(result_name)

    def _verify_surface(self) -> None:
        actual = (
            len(self._contracts),
            self.count(target=ExecutionTarget.CMO),
            self.count(target=ExecutionTarget.LOCAL),
            self.count(expose_mcp=True),
        )
        expected = (57, 53, 4, 49)
        if actual != expected:
            raise RuntimeError(
                f"operation registry surface mismatch: expected {expected}, got {actual}"
            )
        expected_hidden = frozenset(
            {
                "bridge.reconcile",
                "unit.delete",
                "mission.delete",
                "compat.probe.step",
                "bridge.prepare",
                "bridge.doctor",
                "bridge.uninstall",
                "compat.probe",
            }
        )
        if self.hidden_names != expected_hidden:
            raise RuntimeError("operation registry hidden surface mismatch")

    @property
    def manifest_sha256(self) -> str:
        return self._manifest_sha256

    @property
    def names(self) -> frozenset[str]:
        return frozenset(self._contracts)

    @property
    def hidden_names(self) -> frozenset[str]:
        return frozenset(
            name for name, contract in self._contracts.items() if not contract.expose_mcp
        )

    @property
    def cmo_contracts(self) -> tuple[OperationContract, ...]:
        return tuple(
            contract
            for contract in self._contracts.values()
            if contract.target is ExecutionTarget.CMO
        )

    @property
    def mcp_contracts(self) -> tuple[OperationContract, ...]:
        return tuple(contract for contract in self._contracts.values() if contract.expose_mcp)

    def __len__(self) -> int:
        return len(self._contracts)

    def __iter__(self) -> Iterator[OperationContract]:
        return iter(self._contracts.values())

    def resolve(self, name: str) -> OperationContract:
        try:
            return self._contracts[name]
        except KeyError as error:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                f"unknown operation: {name}",
                {"operation": name},
            ) from error

    def count(
        self,
        *,
        target: ExecutionTarget | str | None = None,
        expose_mcp: bool | None = None,
    ) -> int:
        expected_target = None if target is None else ExecutionTarget(target)
        return sum(
            (expected_target is None or contract.target is expected_target)
            and (expose_mcp is None or contract.expose_mcp is expose_mcp)
            for contract in self._contracts.values()
        )

    def resolve_invocation(
        self,
        name: str,
        arguments: Mapping[str, object],
        trusted_enrichment: Mapping[str, object] | None = None,
    ) -> ResolvedInvocation:
        contract = self.resolve(name)
        public_arguments = _validated_model(contract.public_arguments_adapter, dict(arguments))
        enrichment = dict(trusted_enrichment or {})
        unsupported = set(enrichment) - self._trusted_fields[name]
        if unsupported:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "trusted enrichment contains fields not declared by the operation contract",
                {"operation": name, "fields": sorted(unsupported)},
            )
        wire_arguments = contract.wire_resolver.from_public(public_arguments, enrichment)
        frozen = self._freeze(contract, wire_arguments)
        return ResolvedInvocation._from_resolved_recipe_parts(  # pyright: ignore[reportPrivateUsage]
            contract=frozen.contract,
            wire_arguments=wire_arguments,
            effective_class=frozen.effective_class,
            result_schema=frozen.result_schema,
            recovery_schema=frozen.recovery_schema,
            public_result_recipe=cast(
                AdapterRecipe,
                object.__getattribute__(frozen, "_public_result_recipe"),
            ),
            public_arguments=public_arguments,
        )

    def resolve_wire_invocation(
        self,
        name: str,
        wire_arguments: Mapping[str, object],
    ) -> FrozenInvocation:
        contract = self.resolve(name)
        if contract.target is ExecutionTarget.LOCAL:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "raw wire invocation is available only for CMO operations",
                {"operation": name},
            )
        resolved_wire = contract.wire_resolver.from_wire(wire_arguments)
        return self._freeze(contract, resolved_wire)

    def _freeze(self, contract: OperationContract, wire_arguments: BaseModel) -> FrozenInvocation:
        effective_class = contract.effective_class_resolver(wire_arguments)
        result = contract.wire_result_factory(wire_arguments)
        recovery = contract.recovery_factory(wire_arguments)
        public_result = contract.public_result_factory(wire_arguments)
        return FrozenInvocation._from_recipe_parts(  # pyright: ignore[reportPrivateUsage]
            contract=contract,
            wire_arguments=wire_arguments,
            effective_class=effective_class,
            result_schema=self._bind(contract.name, "result", result),
            recovery_schema=(
                None if recovery is None else self._bind(contract.name, "recovery", recovery)
            ),
            public_result_recipe=public_result._adapter_recipe,  # pyright: ignore[reportPrivateUsage]
        )

    def _bind(
        self,
        operation: str,
        role: SchemaRole,
        selection: _ResultSelection,
    ) -> SchemaBinding:
        descriptor = schema_descriptor_bytes(
            manifest_sha256=self.manifest_sha256,
            operation=operation,
            role=role,
            factory=selection.factory,
            model=selection.model,
            selector=selection.selector,
        )
        return SchemaBinding._from_recipe(  # pyright: ignore[reportPrivateUsage]
            hashlib.sha256(descriptor).hexdigest(),
            selection._adapter_recipe,  # pyright: ignore[reportPrivateUsage]
        )


OPERATION_REGISTRY = OperationRegistry()
