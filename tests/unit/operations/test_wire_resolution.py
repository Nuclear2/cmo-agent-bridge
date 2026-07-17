import ast
import copy
import hashlib
import inspect
import textwrap
from collections.abc import Iterable, Mapping
from dataclasses import fields as dataclass_fields
from dataclasses import replace
from typing import cast
from uuid import UUID

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.operations.models import (
    CompatibilityProfileResult,
    PrepareResult,
    ReconcileCommitResult,
    ReconcileCommitWireArgs,
    ReconcileProbeResult,
    ReconcileProbeWireArgs,
    ReconciliationEvidence,
)
from cmo_agent_bridge.operations.registry import (
    FrozenInvocation,
    OperationRegistry,
    ResolvedInvocation,
    SchemaBinding,
)
from cmo_agent_bridge.operations.wire import schema_descriptor_bytes


REQUEST_ID = "00000000-0000-0000-0000-000000000002"
PROOF = "b" * 64
REQUEST_HASH = "c" * 64


def _reachable_type_adapters(root: object) -> list[TypeAdapter[object]]:
    found: list[TypeAdapter[object]] = []
    seen: set[int] = set()

    def visit(value: object) -> None:
        identity = id(value)
        if identity in seen:
            return
        seen.add(identity)
        if isinstance(value, TypeAdapter):
            found.append(cast(TypeAdapter[object], value))
            return
        if (
            isinstance(value, type)
            or value is None
            or type(value) in {str, int, float, bool, bytes}
        ):
            return
        if isinstance(value, Mapping):
            mapping = cast(Mapping[object, object], value)
            for item in mapping.values():
                visit(item)
            return
        if isinstance(value, (tuple, list, set, frozenset)):
            for item in cast(Iterable[object], value):
                visit(item)
            return
        for cls in type(value).__mro__:
            slots = getattr(cls, "__slots__", ())
            if isinstance(slots, str):
                slots = (slots,)
            for slot in cast(Iterable[object], slots):
                if isinstance(slot, str) and hasattr(value, slot):
                    visit(getattr(value, slot))
        attributes = getattr(value, "__dict__", None)
        if isinstance(attributes, dict):
            for item in cast(dict[object, object], attributes).values():
                visit(item)

    visit(root)
    return found


def _mark_nested_schema(value: object) -> bool:
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        for item in mapping.values():
            if isinstance(item, dict):
                cast(dict[object, object], item)["cmo_agent_bridge_tampered"] = True
                return True
            if _mark_nested_schema(item):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_mark_nested_schema(item) for item in cast(Iterable[object], value))
    return False


def _schema_contains_tamper(value: object) -> bool:
    if isinstance(value, dict):
        if "cmo_agent_bridge_tampered" in value:
            return True
        mapping = cast(dict[object, object], value)
        return any(_schema_contains_tamper(item) for item in mapping.values())
    if isinstance(value, (list, tuple)):
        return any(_schema_contains_tamper(item) for item in cast(Iterable[object], value))
    return False


def _assert_disposable_adapter_schema_equal(
    left: TypeAdapter[object], right: TypeAdapter[object]
) -> None:
    assert left is not right
    assert left.json_schema() == right.json_schema()


def test_manifest_explicitly_declares_both_wire_and_result_resolution() -> None:
    from cmo_agent_bridge.operations.generated_manifest import OPERATIONS

    assert {entry["wire_resolver"] for entry in OPERATIONS} <= {
        "model",
        "emcon",
        "reconcile",
        "lua_allowlist",
    }
    assert all("wire_result_factory" in entry for entry in OPERATIONS)
    assert all("public_result_factory" in entry for entry in OPERATIONS)


def test_registry_trusted_fields_table_rejects_reachable_mutation(
    registry: OperationRegistry,
) -> None:
    trusted_fields = cast(dict[str, frozenset[str]], getattr(registry, "_trusted_fields"))
    with pytest.raises(TypeError):
        trusted_fields["unit.assign_mission"] = frozenset({"mission_guid"})

    with pytest.raises(BridgeError) as caught:
        registry.resolve_invocation(
            "unit.assign_mission",
            {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1"},
            {"mission_guid": "MISSION-2"},
        )
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_registry_contract_table_rejects_local_raw_bypass_mutation(
    registry: OperationRegistry,
) -> None:
    contracts = cast(dict[str, object], getattr(registry, "_contracts"))
    with pytest.raises(TypeError):
        contracts["bridge.prepare"] = registry.resolve("scenario.get")

    with pytest.raises(BridgeError) as caught:
        registry.resolve_wire_invocation("bridge.prepare", {})
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


@pytest.mark.parametrize(
    ("attribute", "replacement"),
    [
        ("_trusted_fields", {"unit.assign_mission": frozenset({"mission_guid"})}),
        ("_contracts", {}),
        ("_manifest_sha256", "0" * 64),
        ("_injected_override", object()),
    ],
)
def test_registry_instance_rejects_identity_rebinding_and_new_slots(
    attribute: str,
    replacement: object,
) -> None:
    registry = OperationRegistry()
    manifest_sha256 = registry.manifest_sha256

    with pytest.raises((AttributeError, TypeError)):
        setattr(registry, attribute, replacement)

    assert not hasattr(registry, "_injected_override")
    assert registry.manifest_sha256 == manifest_sha256
    with pytest.raises(BridgeError) as trusted:
        registry.resolve_invocation(
            "unit.assign_mission",
            {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1"},
            {"mission_guid": "MISSION-2"},
        )
    assert trusted.value.code is ErrorCode.INVALID_ARGUMENT
    with pytest.raises(BridgeError) as local_raw:
        registry.resolve_wire_invocation("bridge.prepare", {})
    assert local_raw.value.code is ErrorCode.INVALID_ARGUMENT


@pytest.mark.parametrize(
    ("invocation_type", "expected_fields"),
    [
        (SchemaBinding, ["schema_id", "adapter"]),
        (
            FrozenInvocation,
            [
                "contract",
                "wire_arguments",
                "effective_class",
                "result_schema",
                "recovery_schema",
                "public_result_adapter",
            ],
        ),
        (
            ResolvedInvocation,
            [
                "contract",
                "wire_arguments",
                "effective_class",
                "result_schema",
                "recovery_schema",
                "public_result_adapter",
                "public_arguments",
            ],
        ),
    ],
)
def test_public_dataclass_signature_and_fields_match_brief(
    invocation_type: type[SchemaBinding] | type[FrozenInvocation] | type[ResolvedInvocation],
    expected_fields: list[str],
) -> None:
    assert list(inspect.signature(invocation_type).parameters) == expected_fields
    assert [item.name for item in dataclass_fields(invocation_type)] == expected_fields


def test_public_adapter_field_rejects_forged_recipe_input() -> None:
    from cmo_agent_bridge.operations import registry as registry_module

    class EvilRecipe:
        def materialize(self) -> TypeAdapter[object]:
            return cast(TypeAdapter[object], TypeAdapter(dict[str, object]))

    recipe_input_type = getattr(registry_module, "_RecipeInput", None)
    forged: object = (
        recipe_input_type(EvilRecipe()) if isinstance(recipe_input_type, type) else EvilRecipe()
    )
    with pytest.raises(TypeError):
        SchemaBinding(schema_id="0" * 64, adapter=cast(TypeAdapter[object], forged))
    with pytest.raises(TypeError):
        SchemaBinding(
            schema_id="0" * 64,
            adapter=cast(TypeAdapter[object], TypeAdapter(dict[str, object])),
        )


@pytest.mark.parametrize(
    ("owner", "field_name"),
    [
        (SchemaBinding, "adapter"),
        (FrozenInvocation, "wire_arguments"),
        (FrozenInvocation, "public_result_adapter"),
        (ResolvedInvocation, "public_arguments"),
    ],
)
def test_public_field_descriptor_state_is_frozen(owner: type[object], field_name: str) -> None:
    descriptor = vars(owner)[field_name]
    original_slot = getattr(descriptor, "_slot")
    try:
        with pytest.raises(AttributeError):
            setattr(descriptor, "_slot", "_forged_slot")
    finally:
        object.__setattr__(descriptor, "_slot", original_slot)


def test_public_dataclass_kwargs_construct_all_brief_types(
    registry: OperationRegistry,
) -> None:
    source = registry.resolve_invocation(
        "unit.assign_mission",
        {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1"},
    )
    result_schema = SchemaBinding(
        schema_id=source.result_schema.schema_id,
        adapter=source.result_schema.adapter,
    )
    recovery_schema = (
        SchemaBinding(
            schema_id=source.recovery_schema.schema_id,
            adapter=source.recovery_schema.adapter,
        )
        if source.recovery_schema is not None
        else None
    )
    frozen = FrozenInvocation(
        contract=source.contract,
        wire_arguments=source.wire_arguments,
        effective_class=source.effective_class,
        result_schema=result_schema,
        recovery_schema=recovery_schema,
        public_result_adapter=source.public_result_adapter,
    )
    resolved = ResolvedInvocation(
        contract=source.contract,
        wire_arguments=source.wire_arguments,
        effective_class=source.effective_class,
        result_schema=result_schema,
        recovery_schema=recovery_schema,
        public_result_adapter=source.public_result_adapter,
        public_arguments=source.public_arguments,
    )

    assert frozen.wire_arguments == source.wire_arguments
    assert resolved.public_arguments == source.public_arguments
    assert frozen.result_schema.adapter is not source.result_schema.adapter
    assert _reachable_type_adapters(frozen) == []
    assert _reachable_type_adapters(resolved) == []


def test_public_dataclass_equality_and_hash_do_not_materialize_identity(
    registry: OperationRegistry,
) -> None:
    from cmo_agent_bridge.operations import models

    binding = SchemaBinding(schema_id="0" * 64, adapter=TypeAdapter(models.EmptyArgs))
    copied = replace(binding)

    assert copied == binding
    assert hash(binding) == hash(binding) == hash(copied)
    invocation = registry.resolve_invocation(
        "unit.assign_mission",
        {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1"},
    )
    assert replace(invocation) == invocation


def test_projected_public_replace_preserves_recipe_value_semantics(
    registry: OperationRegistry,
) -> None:
    invocation = registry.resolve_invocation(
        "unit.list", {"side_guid": "SIDE-1", "fields": ["name"]}
    )
    binding = invocation.result_schema
    copied_binding = replace(binding)
    copied_invocation = replace(invocation)
    value = {"items": [{"guid": "UNIT-1", "name": "Alpha"}], "next_cursor": None}

    assert copied_binding == binding
    assert hash(copied_binding) == hash(binding)
    assert copied_invocation == invocation
    result_types: list[type[object]] = []
    for owner in (binding, copied_binding):
        for _ in range(2):
            validated = owner.adapter.validate_python(value)
            result_types.append(type(validated))
    public_types: list[type[object]] = []
    for owner in (invocation, copied_invocation):
        for _ in range(2):
            validated = owner.public_result_adapter.validate_python(value)
            public_types.append(type(validated))
    assert len(set(result_types)) == len(result_types)
    assert len(set(public_types)) == len(public_types)
    assert copied_binding.schema_id == binding.schema_id
    for root in (binding, copied_binding, invocation, copied_invocation):
        assert _reachable_type_adapters(root) == []


def test_projected_replace_detaches_exposed_adapter_and_dynamic_class_mutation(
    registry: OperationRegistry,
) -> None:
    from cmo_agent_bridge.operations import models

    invocation = registry.resolve_invocation(
        "unit.list", {"side_guid": "SIDE-1", "fields": ["name"]}
    )
    binding = invocation.result_schema
    exposed = binding.adapter
    assert type(exposed) is TypeAdapter
    dynamic_target = getattr(exposed, "_type")
    assert isinstance(dynamic_target, type)
    copied = replace(binding, adapter=exposed)
    schema_id = copied.schema_id
    stable_hash = hash(copied)
    token_attribute = "_cmo_agent_bridge_recipe_token"
    token = getattr(exposed, token_attribute)
    with pytest.raises(AttributeError):
        setattr(token, "_recipe", object())
    with pytest.raises(AttributeError):
        delattr(token, "_recipe")
    setattr(exposed, token_attribute, object())
    with pytest.raises(TypeError):
        replace(binding, adapter=exposed)
    setattr(exposed, token_attribute, token)
    transplanted = cast(TypeAdapter[object], TypeAdapter(dynamic_target))
    setattr(transplanted, token_attribute, token)
    with pytest.raises(TypeError):
        replace(binding, adapter=transplanted)

    class AdapterSubclass(
        TypeAdapter[object]  # pyright: ignore[reportGeneralTypeIssues]
    ):
        pass

    subclass = object.__new__(AdapterSubclass)
    subclass.__dict__.update(exposed.__dict__)
    with pytest.raises(TypeError):
        replace(binding, adapter=subclass)

    poison = TypeAdapter(models.EmptyArgs)
    setattr(exposed, "_type", models.EmptyArgs)
    exposed.validator = poison.validator
    exposed.serializer = poison.serializer
    assert _mark_nested_schema(exposed.core_schema)
    setattr(dynamic_target, "__pydantic_validator__", poison.validator)
    setattr(dynamic_target, "__pydantic_serializer__", poison.serializer)
    dynamic_core_schema = getattr(dynamic_target, "__pydantic_core_schema__")
    assert _mark_nested_schema(dynamic_core_schema)
    polluted_input_copy = replace(binding, adapter=exposed)
    assert polluted_input_copy == binding

    value = {"items": [{"guid": "UNIT-1", "name": "Alpha"}], "next_cursor": None}
    for owner in (copied, polluted_input_copy):
        for _ in range(2):
            restored = owner.adapter
            with pytest.raises(ValidationError):
                restored.validate_python({})
            validated = restored.validate_python(value)
            assert restored.dump_python(validated, mode="json") == value
            assert not _schema_contains_tamper(restored.core_schema)
            assert getattr(restored, "_type") is not dynamic_target
    assert copied.schema_id == schema_id
    assert hash(copied) == stable_hash
    for owner in (copied, polluted_input_copy):
        assert _reachable_type_adapters(owner) == []


def test_projected_token_rejects_in_place_adapter_class_swap(
    registry: OperationRegistry,
) -> None:
    binding = registry.resolve_invocation(
        "unit.list", {"side_guid": "SIDE-1", "fields": ["name"]}
    ).result_schema
    adapter = binding.adapter

    class AdapterSubclass(
        TypeAdapter[object]  # pyright: ignore[reportGeneralTypeIssues]
    ):
        __slots__ = ()

    setattr(adapter, "__class__", AdapterSubclass)
    assert type(adapter) is AdapterSubclass
    with pytest.raises(TypeError):
        replace(binding, adapter=adapter)


def test_public_dataclass_replace_uses_only_brief_fields(
    registry: OperationRegistry,
) -> None:
    from cmo_agent_bridge.operations import models

    source = registry.resolve_invocation(
        "unit.assign_mission",
        {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1"},
    )
    replacement_adapter = TypeAdapter(models.EmptyArgs)
    replacement_wire = source.contract.wire_arguments_adapter.validate_python(
        {"unit_guid": "UNIT-2", "mission_guid": "MISSION-2", "escort": False}
    )
    replacement_public = source.contract.public_arguments_adapter.validate_python(
        {"unit_guid": "UNIT-3", "mission_guid": "MISSION-3", "escort": False}
    )
    binding = replace(source.result_schema, adapter=replacement_adapter)
    frozen = replace(
        source,
        wire_arguments=replacement_wire,
        result_schema=binding,
        public_result_adapter=replacement_adapter,
        public_arguments=replacement_public,
    )

    validated_binding = binding.adapter.validate_python({})
    assert isinstance(validated_binding, BaseModel)
    assert validated_binding.model_dump() == {}
    assert frozen.wire_arguments.model_dump()["unit_guid"] == "UNIT-2"
    assert frozen.public_arguments.model_dump()["unit_guid"] == "UNIT-3"
    validated_public = frozen.public_result_adapter.validate_python({})
    assert isinstance(validated_public, BaseModel)
    assert validated_public.model_dump() == {}
    assert _reachable_type_adapters(frozen) == []


def test_public_dataclass_inputs_are_snapshotted_and_adapter_isolated(
    registry: OperationRegistry,
) -> None:
    from cmo_agent_bridge.operations import models

    source = registry.resolve_invocation("unit.list", {"side_guid": "SIDE-1", "fields": ["name"]})
    wire_arguments = source.wire_arguments
    public_arguments = source.public_arguments
    adapter = source.result_schema.adapter
    binding = SchemaBinding(schema_id=source.result_schema.schema_id, adapter=adapter)
    constructed = ResolvedInvocation(
        contract=source.contract,
        wire_arguments=wire_arguments,
        effective_class=source.effective_class,
        result_schema=binding,
        recovery_schema=None,
        public_result_adapter=adapter,
        public_arguments=public_arguments,
    )

    list.append(  # pyright: ignore[reportUnknownMemberType]
        cast(list[str], getattr(wire_arguments, "fields")), "type"
    )
    list.append(  # pyright: ignore[reportUnknownMemberType]
        cast(list[str], getattr(public_arguments, "fields")), "type"
    )
    poison = TypeAdapter(models.EmptyArgs)
    adapter.validator = poison.validator
    adapter.serializer = poison.serializer
    assert _mark_nested_schema(adapter.core_schema)

    assert getattr(constructed.wire_arguments, "fields") == ["name"]
    assert getattr(constructed.public_arguments, "fields") == ["name"]
    valid = {"items": [{"guid": "UNIT-1", "name": "Alpha"}], "next_cursor": None}
    for disposable in (constructed.result_schema.adapter, constructed.public_result_adapter):
        with pytest.raises(ValidationError):
            disposable.validate_python({})
        validated = disposable.validate_python(valid)
        assert disposable.dump_python(validated, mode="json") == valid
        assert not _schema_contains_tamper(disposable.core_schema)
    assert _reachable_type_adapters(constructed) == []


def test_result_binding_adapters_are_disposable_and_role_isolated(
    registry: OperationRegistry,
) -> None:
    from cmo_agent_bridge.operations import models

    invocation = registry.resolve_invocation(
        "unit.assign_mission",
        {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1"},
    )
    assert invocation.recovery_schema is not None
    schema_ids = (
        invocation.result_schema.schema_id,
        invocation.recovery_schema.schema_id,
    )
    manifest_sha256 = registry.manifest_sha256
    exposed = invocation.result_schema.adapter
    original_validator = exposed.validator
    original_serializer = exposed.serializer
    exposed_core_schema = cast(dict[str, object], exposed.core_schema)
    original_core_schema = copy.deepcopy(exposed_core_schema)
    try:
        poison = TypeAdapter(models.EmptyArgs)
        exposed.validator = poison.validator
        exposed.serializer = poison.serializer
        assert _mark_nested_schema(exposed.core_schema)
        exposed.validate_python({})
        valid = {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1", "escort": False}
        for adapter in (
            invocation.result_schema.adapter,
            invocation.recovery_schema.adapter,
            invocation.public_result_adapter,
        ):
            with pytest.raises(ValidationError):
                adapter.validate_python({})
            validated = adapter.validate_python(valid)
            assert adapter.dump_python(validated, mode="json") == valid
            assert not _schema_contains_tamper(adapter.core_schema)
    finally:
        exposed.validator = original_validator
        exposed.serializer = original_serializer
        exposed_core_schema.clear()
        exposed_core_schema.update(original_core_schema)

    assert invocation.result_schema.adapter is not exposed
    assert (
        invocation.result_schema.schema_id,
        invocation.recovery_schema.schema_id,
    ) == schema_ids
    assert registry.manifest_sha256 == manifest_sha256


def test_projected_page_adapter_materializes_fresh_dynamic_models(
    registry: OperationRegistry,
) -> None:
    from cmo_agent_bridge.operations import models

    invocation = registry.resolve_wire_invocation(
        "unit.list", {"side_guid": "SIDE-1", "fields": ["name"]}
    )
    value = {
        "items": [{"guid": "UNIT-1", "name": "Alpha"}],
        "next_cursor": None,
    }
    exposed = invocation.result_schema.adapter
    first_model = exposed.validate_python(value)
    exposed.validator = TypeAdapter(models.EmptyArgs).validator
    exposed.serializer = TypeAdapter(models.EmptyArgs).serializer
    assert _mark_nested_schema(exposed.core_schema)

    restored = invocation.result_schema.adapter
    restored_model = restored.validate_python(value)
    assert type(restored_model) is not type(first_model)
    assert restored.dump_python(restored_model, mode="json") == value
    assert not _schema_contains_tamper(restored.core_schema)
    with pytest.raises(ValidationError):
        restored.validate_python({})


def test_contract_argument_adapters_are_disposable_for_existing_and_new_registry(
    registry: OperationRegistry,
) -> None:
    from cmo_agent_bridge.operations import models

    contract = registry.resolve("unit.assign_mission")
    public_adapter = contract.public_arguments_adapter
    wire_adapter = contract.wire_arguments_adapter
    original_validators = {
        id(adapter): (adapter, adapter.validator) for adapter in (public_adapter, wire_adapter)
    }
    try:
        poison = TypeAdapter(models.EmptyArgs).validator
        public_adapter.validator = poison
        wire_adapter.validator = poison
        for candidate in (registry, OperationRegistry()):
            with pytest.raises(ValidationError):
                candidate.resolve_invocation("unit.assign_mission", {})
            with pytest.raises(ValidationError):
                candidate.resolve_wire_invocation("unit.assign_mission", {})
    finally:
        for adapter, validator in original_validators.values():
            adapter.validator = validator

    assert contract.public_arguments_adapter is not public_adapter
    assert contract.wire_arguments_adapter is not wire_adapter


def test_lua_function_adapter_mutation_does_not_bypass_missing_arguments(
    registry: OperationRegistry,
) -> None:
    from cmo_agent_bridge.operations import models

    resolver = registry.resolve("lua.call").wire_resolver
    policies = getattr(resolver, "_policies")
    policy = policies["ScenEdit_GetScore"]
    adapter = getattr(policy, "arguments_adapter", None)
    original_validator = None
    if isinstance(adapter, TypeAdapter):
        original_validator = adapter.validator
        adapter.validator = TypeAdapter(models.EmptyArgs).validator
    try:
        invalid: dict[str, object] = {"function": "ScenEdit_GetScore", "arguments": {}}
        for candidate in (registry, OperationRegistry()):
            with pytest.raises(ValidationError):
                candidate.resolve_invocation("lua.call", invalid)
            with pytest.raises(ValidationError):
                candidate.resolve_wire_invocation("lua.call", invalid)
    finally:
        if isinstance(adapter, TypeAdapter) and original_validator is not None:
            adapter.validator = original_validator


def test_long_lived_resolution_graph_contains_no_type_adapters(
    registry: OperationRegistry,
) -> None:
    resolved = registry.resolve_invocation("unit.list", {"side_guid": "SIDE-1", "fields": ["name"]})
    frozen = registry.resolve_wire_invocation(
        "unit.list", {"side_guid": "SIDE-1", "fields": ["name"]}
    )
    for root in (registry, resolved, frozen):
        assert _reachable_type_adapters(root) == []


def test_projection_cache_cannot_inject_adapter_into_existing_or_new_registry(
    registry: OperationRegistry,
) -> None:
    from cmo_agent_bridge.operations import models, registry as registry_module

    arguments = {"side_guid": "SIDE-1", "fields": ["name"]}
    before = registry.resolve_wire_invocation("unit.list", arguments)
    raw_cache = getattr(registry_module, "_PROJECTION_CACHE", None)
    cache = (
        cast(dict[tuple[str, tuple[str, ...] | None], TypeAdapter[object]], raw_cache)
        if isinstance(raw_cache, dict)
        else None
    )
    key = ("UnitResult", ("guid", "name"))
    previous: TypeAdapter[object] | None = None
    if cache is not None:
        previous = cache[key]
        cache[key] = TypeAdapter(models.EmptyArgs)
    try:
        existing_after = registry.resolve_wire_invocation("unit.list", arguments)
        new_after = OperationRegistry().resolve_wire_invocation("unit.list", arguments)
        assert existing_after.result_schema.schema_id == before.result_schema.schema_id
        assert new_after.result_schema.schema_id == before.result_schema.schema_id
        _assert_disposable_adapter_schema_equal(
            existing_after.result_schema.adapter, before.result_schema.adapter
        )
        _assert_disposable_adapter_schema_equal(
            new_after.result_schema.adapter, before.result_schema.adapter
        )
    finally:
        if cache is not None and previous is not None:
            cache[key] = previous


def test_model_adapter_cache_cannot_inject_lua_arguments_into_new_registry() -> None:
    from cmo_agent_bridge.operations import models, registry as registry_module

    existing = OperationRegistry()
    valid: dict[str, object] = {
        "function": "ScenEdit_GetScore",
        "arguments": {"side": "Blue"},
    }
    invalid: dict[str, object] = {"function": "ScenEdit_GetScore", "arguments": {}}
    before_raw = existing.resolve_wire_invocation("lua.call", valid)
    before_public = existing.resolve_invocation("lua.call", valid)
    raw_cache = getattr(registry_module, "_ADAPTER_CACHE", None)
    cache = cast(dict[str, TypeAdapter[object]], raw_cache) if isinstance(raw_cache, dict) else None
    previous: TypeAdapter[object] | None = None
    if cache is not None:
        previous = cache["GetScoreArgs"]
        cache["GetScoreArgs"] = TypeAdapter(models.EmptyArgs)
    try:
        new_registry = OperationRegistry()
        for candidate in (existing, new_registry):
            with pytest.raises(ValidationError):
                candidate.resolve_wire_invocation("lua.call", invalid)
            with pytest.raises(ValidationError):
                candidate.resolve_invocation("lua.call", invalid)
            raw = candidate.resolve_wire_invocation("lua.call", valid)
            public = candidate.resolve_invocation("lua.call", valid)
            assert raw.wire_arguments == before_raw.wire_arguments
            assert public.wire_arguments == before_public.wire_arguments
            assert raw.effective_class is before_raw.effective_class
            assert raw.result_schema.schema_id == before_raw.result_schema.schema_id
            _assert_disposable_adapter_schema_equal(
                raw.result_schema.adapter, before_raw.result_schema.adapter
            )
            assert raw.recovery_schema == before_raw.recovery_schema
            _assert_disposable_adapter_schema_equal(
                raw.public_result_adapter, before_raw.public_result_adapter
            )
    finally:
        if cache is not None and previous is not None:
            cache["GetScoreArgs"] = previous


def test_public_and_raw_resolution_freeze_the_same_wire_contract(
    registry: OperationRegistry,
) -> None:
    public = registry.resolve_invocation(
        "unit.delete",
        {"unit_guid": "UNIT-1"},
        {"confirmation_proof": PROOF},
    )
    raw = registry.resolve_wire_invocation(
        "unit.delete",
        {"unit_guid": "UNIT-1", "confirmation_proof": PROOF},
    )

    assert isinstance(public, ResolvedInvocation)
    assert isinstance(raw, FrozenInvocation)
    assert public.wire_arguments == raw.wire_arguments
    assert public.effective_class is raw.effective_class is OperationClass.DESTRUCTIVE
    assert public.result_schema.schema_id == raw.result_schema.schema_id
    assert public.recovery_schema is not None
    assert raw.recovery_schema is not None
    assert public.recovery_schema.schema_id == raw.recovery_schema.schema_id


def test_raw_resolution_rejects_local_operations_and_never_accepts_public_shape(
    registry: OperationRegistry,
) -> None:
    with pytest.raises(BridgeError) as caught:
        registry.resolve_wire_invocation("bridge.prepare", {})
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT

    with pytest.raises(ValidationError):
        registry.resolve_wire_invocation("unit.delete", {"unit_guid": "UNIT-1"})
    with pytest.raises(ValidationError):
        registry.resolve_wire_invocation(
            "emcon.set",
            {"scope": "unit", "target_guid": "UNIT-1", "radar": "Active"},
        )


def test_trusted_fields_cannot_bypass_the_public_adapter_but_are_raw_wire_fields(
    registry: OperationRegistry,
) -> None:
    with pytest.raises(ValidationError):
        registry.resolve_invocation(
            "unit.delete",
            {"unit_guid": "UNIT-1", "confirmation_proof": PROOF},
        )

    raw = registry.resolve_wire_invocation(
        "unit.delete",
        {"unit_guid": "UNIT-1", "confirmation_proof": PROOF},
    )
    assert raw.wire_arguments.model_dump()["confirmation_proof"] == PROOF


def test_emcon_raw_resolution_reapplies_exact_grammar_and_recovery(
    registry: OperationRegistry,
) -> None:
    public = registry.resolve_invocation(
        "emcon.set",
        {
            "scope": "unit",
            "target_guid": "UNIT-1",
            "inherit": True,
            "radar": "Active",
            "oecm": "Passive",
        },
    )
    raw = registry.resolve_wire_invocation(
        "emcon.set",
        {
            "scope": "unit",
            "target_guid": "UNIT-1",
            "emcon": "Inherit;Radar=Active;OECM=Passive",
        },
    )
    assert public.wire_arguments == raw.wire_arguments
    assert raw.recovery_schema is not None

    with pytest.raises(ValidationError):
        registry.resolve_wire_invocation(
            "emcon.set",
            {
                "scope": "unit",
                "target_guid": "UNIT-1",
                "emcon": "Radar=active",
            },
        )


def test_lua_raw_resolution_reapplies_allowlist_and_specific_argument_model(
    registry: OperationRegistry,
) -> None:
    raw = registry.resolve_wire_invocation(
        "lua.call",
        {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}},
    )
    assert raw.effective_class is OperationClass.READ
    assert raw.recovery_schema is None
    raw.result_schema.adapter.validate_python({"side": "Blue", "score": 12})

    with pytest.raises(BridgeError) as caught:
        registry.resolve_wire_invocation(
            "lua.call",
            {"function": "ScenEdit_DeleteUnit", "arguments": {}},
        )
    assert caught.value.code is ErrorCode.POLICY_DENIED
    with pytest.raises(ValidationError):
        registry.resolve_wire_invocation(
            "lua.call",
            {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue", "extra": 1}},
        )


def test_reconcile_public_and_raw_branch_matrix(registry: OperationRegistry) -> None:
    public_probe = registry.resolve_invocation("bridge.reconcile", {})
    raw_probe = registry.resolve_wire_invocation("bridge.reconcile", {})
    assert isinstance(public_probe.wire_arguments, ReconcileProbeWireArgs)
    assert isinstance(raw_probe.wire_arguments, ReconcileProbeWireArgs)
    assert public_probe.effective_class is raw_probe.effective_class is OperationClass.READ
    assert public_probe.recovery_schema is raw_probe.recovery_schema is None
    public_probe.result_schema.adapter.validate_python(
        {
            "barrier_evidence": _absent_evidence(),
            "ledger_evidence": _absent_evidence(),
            "allowed_dispositions": [],
            "quarantined": False,
        }
    )

    public_commit = registry.resolve_invocation(
        "bridge.reconcile",
        {"request_id": REQUEST_ID, "disposition": "applied"},
        {"confirmation_proof": PROOF},
    )
    raw_commit = registry.resolve_wire_invocation(
        "bridge.reconcile",
        {
            "request_id": REQUEST_ID,
            "disposition": "applied",
            "confirmation_proof": PROOF,
        },
    )
    assert isinstance(public_commit.wire_arguments, ReconcileCommitWireArgs)
    assert isinstance(raw_commit.wire_arguments, ReconcileCommitWireArgs)
    assert public_commit.effective_class is OperationClass.RECONCILE
    assert raw_commit.effective_class is OperationClass.RECONCILE
    assert public_commit.recovery_schema is not None
    assert raw_commit.recovery_schema is not None
    public_commit.result_schema.adapter.validate_python(
        {
            "request_id": REQUEST_ID,
            "request_hash": REQUEST_HASH,
            "disposition": "applied",
            "resolved": True,
        }
    )


def test_reconcile_public_null_probe_is_canonical_but_caller_proof_and_halves_fail(
    registry: OperationRegistry,
) -> None:
    probe = registry.resolve_invocation(
        "bridge.reconcile", {"request_id": None, "disposition": None}
    )
    assert isinstance(probe.wire_arguments, ReconcileProbeWireArgs)
    assert probe.wire_arguments.model_dump(mode="json") == {}

    for public in (
        {"request_id": REQUEST_ID, "disposition": None},
        {"request_id": None, "disposition": "applied"},
        {"confirmation_proof": PROOF},
        {
            "request_id": REQUEST_ID,
            "disposition": "applied",
            "confirmation_proof": PROOF,
        },
    ):
        with pytest.raises(ValidationError):
            registry.resolve_invocation("bridge.reconcile", public)


@pytest.mark.parametrize(
    "wire",
    [
        {"request_id": REQUEST_ID},
        {"disposition": "applied"},
        {"confirmation_proof": PROOF},
        {"request_id": REQUEST_ID, "disposition": "applied"},
        {
            "request_id": REQUEST_ID,
            "disposition": "applied",
            "confirmation_proof": PROOF,
            "extra": True,
        },
        {"request_id": None, "disposition": None, "confirmation_proof": None},
        {"request_id": None, "disposition": "applied", "confirmation_proof": PROOF},
        {"request_id": REQUEST_ID, "disposition": None, "confirmation_proof": PROOF},
        {
            "request_id": REQUEST_ID,
            "disposition": "applied",
            "confirmation_proof": None,
        },
    ],
)
def test_reconcile_raw_branches_are_strict_and_complete(
    registry: OperationRegistry, wire: dict[str, object]
) -> None:
    with pytest.raises(ValidationError):
        registry.resolve_wire_invocation("bridge.reconcile", wire)


def test_reconcile_public_result_is_distinct_from_wire_branch_results(
    registry: OperationRegistry,
) -> None:
    probe = registry.resolve_invocation("bridge.reconcile", {})
    with pytest.raises(ValidationError):
        probe.public_result_adapter.validate_python(
            {
                "barrier_evidence": _absent_evidence(),
                "ledger_evidence": _absent_evidence(),
                "allowed_dispositions": [],
                "quarantined": False,
            }
        )
    probe.public_result_adapter.validate_python(
        {
            "request_id": None,
            "journal_evidence": _absent_evidence(),
            "barrier_evidence": _absent_evidence(),
            "ledger_evidence": _absent_evidence(),
            "allowed_dispositions": [],
            "applied_disposition": None,
            "quarantined": False,
            "confirmation_token": None,
            "confirmation_expires_at_utc": None,
            "reserved_activation_candidate": None,
        }
    )


def test_reconcile_manifest_ast_has_only_absent_or_complete_nonnull_wire_fields() -> None:
    from cmo_agent_bridge.operations.generated_manifest import OPERATIONS

    entry = next(item for item in OPERATIONS if item["name"] == "bridge.reconcile")
    arguments_ast = cast(dict[str, object], entry["arguments_ast"])
    properties = cast(dict[str, dict[str, object]], arguments_ast["properties"])
    for field in ("request_id", "disposition", "confirmation_proof"):
        assert "nullable" not in properties[field]


def test_reconciliation_evidence_and_probe_identity_are_closed() -> None:
    ReconciliationEvidence.model_validate(_absent_evidence())
    present = _present_evidence()
    ReconciliationEvidence.model_validate(present)

    for invalid in (
        {**_absent_evidence(), "request_id": REQUEST_ID},
        {**present, "state": None},
        {**present, "state": ""},
        {**present, "request_hash": None},
    ):
        with pytest.raises(ValidationError):
            ReconciliationEvidence.model_validate(invalid)

    valid = {
        "barrier_evidence": present,
        "ledger_evidence": present,
        "allowed_dispositions": ["applied", "not_applied"],
        "quarantined": True,
    }
    ReconcileProbeResult.model_validate(valid)
    with pytest.raises(ValidationError):
        ReconcileProbeResult.model_validate(
            {
                **valid,
                "ledger_evidence": {
                    **present,
                    "request_id": "00000000-0000-0000-0000-000000000003",
                },
            }
        )
    with pytest.raises(ValidationError):
        ReconcileProbeResult.model_validate(
            {
                **valid,
                "ledger_evidence": {
                    **present,
                    "request_hash": "d" * 64,
                },
            }
        )
    with pytest.raises(ValidationError):
        ReconcileProbeResult.model_validate(
            {**valid, "allowed_dispositions": ["applied", "applied"]}
        )
    with pytest.raises(ValidationError):
        ReconcileProbeResult.model_validate({**valid, "allowed_dispositions": []})
    with pytest.raises(ValidationError):
        ReconcileProbeResult.model_validate(
            {
                "barrier_evidence": _absent_evidence(),
                "ledger_evidence": _absent_evidence(),
                "allowed_dispositions": ["applied"],
                "quarantined": False,
            }
        )


@pytest.mark.parametrize(
    ("barrier_state", "ledger_state", "allowed", "quarantined"),
    [
        (None, None, [], False),
        ("in_progress", None, ["applied", "not_applied"], True),
        (None, "indeterminate", ["applied", "not_applied"], True),
        ("in_progress", "indeterminate", ["not_applied", "applied"], True),
        ("completed", None, [], True),
        (None, "completed", [], False),
        ("completed", "completed", [], True),
        ("completed", "resolved", [], True),
        (None, "cancelled", [], False),
        (None, "resolved", [], False),
        ("in_progress", "completed", [], True),
        (None, "future_state", [], True),
    ],
)
def test_reconcile_probe_fail_closed_state_matrix(
    barrier_state: str | None,
    ledger_state: str | None,
    allowed: list[str],
    quarantined: bool,
) -> None:
    value = {
        "barrier_evidence": _evidence(barrier_state),
        "ledger_evidence": _evidence(ledger_state),
        "allowed_dispositions": allowed,
        "quarantined": quarantined,
    }
    ReconcileProbeResult.model_validate(value)

    with pytest.raises(ValidationError):
        ReconcileProbeResult.model_validate({**value, "quarantined": not quarantined})
    wrong_allowed = [] if allowed else ["applied", "not_applied"]
    with pytest.raises(ValidationError):
        ReconcileProbeResult.model_validate({**value, "allowed_dispositions": wrong_allowed})


def test_commit_result_keeps_original_mutation_identity() -> None:
    result = ReconcileCommitResult.model_validate(
        {
            "request_id": REQUEST_ID,
            "request_hash": REQUEST_HASH,
            "disposition": "not_applied",
            "resolved": True,
        }
    )
    assert result.request_id == UUID(REQUEST_ID)
    with pytest.raises(ValidationError):
        ReconcileCommitResult.model_validate(
            {
                "request_id": REQUEST_ID,
                "request_hash": REQUEST_HASH,
                "disposition": "not_applied",
                "resolved": False,
            }
        )


def test_reconcile_commit_binding_is_static_across_wire_targets(
    registry: OperationRegistry,
) -> None:
    first_target = REQUEST_ID
    second_target = "00000000-0000-0000-0000-000000000003"
    first = registry.resolve_wire_invocation(
        "bridge.reconcile",
        {
            "request_id": first_target,
            "disposition": "applied",
            "confirmation_proof": PROOF,
        },
    )
    second = registry.resolve_wire_invocation(
        "bridge.reconcile",
        {
            "request_id": second_target,
            "disposition": "not_applied",
            "confirmation_proof": PROOF,
        },
    )
    assert first.wire_arguments != second.wire_arguments
    assert first.result_schema.schema_id == second.result_schema.schema_id
    _assert_disposable_adapter_schema_equal(
        first.result_schema.adapter, second.result_schema.adapter
    )
    cross_target = first.result_schema.adapter.validate_python(
        {
            "request_id": second_target,
            "request_hash": REQUEST_HASH,
            "disposition": "not_applied",
            "resolved": True,
        }
    )
    assert isinstance(cross_target, ReconcileCommitResult)
    assert cross_target.request_id == UUID(second_target)


@pytest.mark.parametrize(
    ("operation", "factory", "model", "selector", "expected", "expected_hash"),
    [
        (
            "scenario.get",
            "named",
            "ScenarioResult",
            None,
            '{"factory":"named","format":"cmo-agent-bridge/schema-binding/1",'
            '"manifest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            '"model":"ScenarioResult","operation":"scenario.get","role":"result",'
            '"selector":null}',
            "db3931a04b06a821ebe5f33db881ba5a6a8143e20b88c5fc5ab0f6ec654a7904",
        ),
        (
            "unit.list",
            "paged",
            "UnitResult",
            {"fields": ["guid", "name"]},
            '{"factory":"paged","format":"cmo-agent-bridge/schema-binding/1",'
            '"manifest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            '"model":"UnitResult","operation":"unit.list","role":"result",'
            '"selector":{"fields":["guid","name"]}}',
            "aae494c42444b604ca6e361bfc3abdae56df3ac8790494d257d390d7c334535d",
        ),
        (
            "compat.probe.step",
            "discriminated",
            "ObservationalProbeStepResult",
            {"field": "step", "value": "observational"},
            '{"factory":"discriminated","format":"cmo-agent-bridge/schema-binding/1",'
            '"manifest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            '"model":"ObservationalProbeStepResult","operation":"compat.probe.step",'
            '"role":"result","selector":{"field":"step","value":"observational"}}',
            "2d0c27bc1d684b7c52fe78d6764a034cb04b077221bdf92cde84a095d3b6d8e7",
        ),
        (
            "lua.call",
            "lua_allowlist",
            "ScoreResult",
            {"function": "ScenEdit_GetScore"},
            '{"factory":"lua_allowlist","format":"cmo-agent-bridge/schema-binding/1",'
            '"manifest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            '"model":"ScoreResult","operation":"lua.call","role":"result",'
            '"selector":{"function":"ScenEdit_GetScore"}}',
            "24296c989fd924e2aef79728e3f21ac28f40f7753c4707307dcd832d524270a1",
        ),
        (
            "bridge.reconcile",
            "reconcile",
            "ReconcileCommitResult",
            {"branch": "commit"},
            '{"factory":"reconcile","format":"cmo-agent-bridge/schema-binding/1",'
            '"manifest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            '"model":"ReconcileCommitResult","operation":"bridge.reconcile",'
            '"role":"result","selector":{"branch":"commit"}}',
            "4bac17f0fed08e4d37fc792ae072e77d2dd17b9ffda12f35bc19fd1dfd5eec07",
        ),
    ],
)
def test_schema_descriptor_has_literal_canonical_vectors(
    operation: str,
    factory: str,
    model: str,
    selector: dict[str, object] | None,
    expected: str,
    expected_hash: str,
) -> None:
    encoded = schema_descriptor_bytes(
        manifest_sha256="a" * 64,
        operation=operation,
        role="result",
        factory=factory,
        model=model,
        selector=selector,
    )
    assert encoded == expected.encode()
    assert hashlib.sha256(encoded).hexdigest() == expected_hash


@pytest.mark.parametrize(
    ("value", "expected_hash"),
    [
        (7, "3ab052abaeeb4571ab47695262559a1ec478005eb1922e36d4d08748c3541c9d"),
        ("Exact", "e2085a4340b0fb6b197bc2681b1be04c2566a3cbf1aa826cd340daf3fbeecac0"),
        (True, "8c6eb1286db006132f37190a629bf974207ecfad86b241a298dd8a832517e694"),
        ("\u00e9", "b7813cefce45507a8e0afdb0ea11fd5334a685a7805baf7c35cd23bbaadaf7a3"),
        ("e\u0301", "b2340cd40f0b1af43c032396fc1e5b44b4e6dd1bd87f7cbe9ea12fce5147d90b"),
    ],
)
def test_selector_scalars_preserve_exact_json_type_and_unicode_bytes(
    value: object, expected_hash: str
) -> None:
    encoded = schema_descriptor_bytes(
        manifest_sha256="a" * 64,
        operation="selector.scalar",
        role="result",
        factory="discriminated",
        model="SyntheticResult",
        selector={"field": "kind", "value": value},
    )
    assert hashlib.sha256(encoded).hexdigest() == expected_hash


@pytest.mark.parametrize(
    (
        "operation",
        "arguments",
        "trusted",
        "factory",
        "model",
        "selector",
    ),
    [
        ("scenario.get", {}, {}, "named", "ScenarioResult", None),
        ("side.list", {}, {}, "paged", "SideResult", None),
        (
            "unit.list",
            {"side_guid": "SIDE-1", "fields": ["name"]},
            {},
            "paged",
            "UnitResult",
            {"fields": ["guid", "name"]},
        ),
        (
            "compat.probe.step",
            {"step": "observational"},
            {},
            "discriminated",
            "ObservationalProbeStepResult",
            {"field": "step", "value": "observational"},
        ),
        (
            "lua.call",
            {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}},
            {},
            "lua_allowlist",
            "ScoreResult",
            {"function": "ScenEdit_GetScore"},
        ),
        (
            "bridge.reconcile",
            {"request_id": REQUEST_ID, "disposition": "applied"},
            {"confirmation_proof": PROOF},
            "reconcile",
            "ReconcileCommitResult",
            {"branch": "commit"},
        ),
    ],
)
def test_registry_schema_binding_uses_the_normalized_descriptor(
    registry: OperationRegistry,
    operation: str,
    arguments: dict[str, object],
    trusted: dict[str, object],
    factory: str,
    model: str,
    selector: dict[str, object] | None,
) -> None:
    invocation = registry.resolve_invocation(operation, arguments, trusted)
    expected = schema_descriptor_bytes(
        manifest_sha256=registry.manifest_sha256,
        operation=operation,
        role="result",
        factory=factory,
        model=model,
        selector=selector,
    )
    assert invocation.result_schema.schema_id == hashlib.sha256(expected).hexdigest()


def test_schema_ids_are_order_invariant_for_projection_but_field_sensitive(
    registry: OperationRegistry,
) -> None:
    first = registry.resolve_invocation(
        "unit.list", {"side_guid": "SIDE-1", "fields": ["name", "type"]}
    )
    reordered = registry.resolve_invocation(
        "unit.list", {"side_guid": "SIDE-1", "fields": ["type", "name"]}
    )
    narrower = registry.resolve_invocation("unit.list", {"side_guid": "SIDE-1", "fields": ["name"]})
    assert first.result_schema.schema_id == reordered.result_schema.schema_id
    assert first.result_schema.schema_id != narrower.result_schema.schema_id


def test_result_and_recovery_roles_have_distinct_schema_ids(
    registry: OperationRegistry,
) -> None:
    invocation = registry.resolve_invocation(
        "unit.assign_mission",
        {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1"},
    )
    assert invocation.recovery_schema is not None
    _assert_disposable_adapter_schema_equal(
        invocation.result_schema.adapter, invocation.recovery_schema.adapter
    )
    assert invocation.result_schema.schema_id != invocation.recovery_schema.schema_id


def test_raw_lua_mutation_fixture_reapplies_effective_recovery_and_role_binding() -> None:
    from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256, OPERATIONS

    entries = copy.deepcopy(OPERATIONS)
    lua_entry = next(entry for entry in entries if entry["name"] == "lua.call")
    policies = cast(dict[str, object], lua_entry["resolver_data"])
    score_policy = cast(dict[str, object], policies["ScenEdit_GetScore"])
    score_policy["class"] = "mutation"

    registry = OperationRegistry(entries)
    assert registry.manifest_sha256 != MANIFEST_SHA256
    invocation = registry.resolve_wire_invocation(
        "lua.call",
        {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}},
    )
    assert invocation.effective_class is OperationClass.MUTATION
    assert invocation.recovery_schema is not None
    _assert_disposable_adapter_schema_equal(
        invocation.result_schema.adapter, invocation.recovery_schema.adapter
    )
    assert invocation.result_schema.schema_id != invocation.recovery_schema.schema_id


def test_lua_result_and_recovery_selector_reachable_from_contract_is_immutable() -> None:
    from cmo_agent_bridge.operations.generated_manifest import OPERATIONS

    entries = copy.deepcopy(OPERATIONS)
    lua_entry = next(entry for entry in entries if entry["name"] == "lua.call")
    policies = cast(dict[str, object], lua_entry["resolver_data"])
    score_policy = cast(dict[str, object], policies["ScenEdit_GetScore"])
    score_policy["class"] = "mutation"
    registry = OperationRegistry(entries)
    invocation = registry.resolve_wire_invocation(
        "lua.call",
        {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}},
    )
    result = invocation.contract.wire_result_factory(invocation.wire_arguments)
    recovery = invocation.contract.recovery_factory(invocation.wire_arguments)
    assert recovery is not None
    assert recovery is result
    public_result = invocation.contract.public_result_factory(invocation.wire_arguments)

    for selection in (result, recovery, public_result):
        assert selection.selector is not None
        with pytest.raises(TypeError):
            cast(dict[str, object], selection.selector)["function"] = "ScenEdit_Other"

    after = registry.resolve_wire_invocation(
        "lua.call",
        {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}},
    )
    assert after.effective_class is invocation.effective_class is OperationClass.MUTATION
    assert after.result_schema.schema_id == invocation.result_schema.schema_id
    _assert_disposable_adapter_schema_equal(
        after.result_schema.adapter, invocation.result_schema.adapter
    )
    assert invocation.recovery_schema is not None
    assert after.recovery_schema is not None
    assert after.recovery_schema.schema_id == invocation.recovery_schema.schema_id
    _assert_disposable_adapter_schema_equal(
        after.recovery_schema.adapter, invocation.recovery_schema.adapter
    )
    _assert_disposable_adapter_schema_equal(
        after.public_result_adapter, invocation.public_result_adapter
    )


@pytest.mark.parametrize(
    ("operation", "wire_arguments", "selector_key", "replacement"),
    [
        (
            "bridge.reconcile",
            {
                "request_id": REQUEST_ID,
                "disposition": "applied",
                "confirmation_proof": PROOF,
            },
            "branch",
            "probe",
        ),
        ("compat.probe.step", {"step": "dedupe"}, "value", "observational"),
    ],
)
def test_cached_result_selectors_reachable_from_contract_are_immutable(
    registry: OperationRegistry,
    operation: str,
    wire_arguments: dict[str, object],
    selector_key: str,
    replacement: str,
) -> None:
    before = registry.resolve_wire_invocation(operation, wire_arguments)
    contract = before.contract
    result = contract.wire_result_factory(before.wire_arguments)
    recovery = contract.recovery_factory(before.wire_arguments)
    assert recovery is not None
    assert recovery is result
    selections = [result, recovery, contract.public_result_factory(before.wire_arguments)]

    for selection in selections:
        if selection.selector is None:
            continue
        with pytest.raises(TypeError):
            cast(dict[str, object], selection.selector)[selector_key] = replacement

    after = registry.resolve_wire_invocation(operation, wire_arguments)
    assert after.effective_class is before.effective_class
    assert after.result_schema.schema_id == before.result_schema.schema_id
    _assert_disposable_adapter_schema_equal(
        after.result_schema.adapter, before.result_schema.adapter
    )
    assert before.recovery_schema is not None
    assert after.recovery_schema is not None
    assert after.recovery_schema.schema_id == before.recovery_schema.schema_id
    _assert_disposable_adapter_schema_equal(
        after.recovery_schema.adapter, before.recovery_schema.adapter
    )
    _assert_disposable_adapter_schema_equal(
        after.public_result_adapter, before.public_result_adapter
    )


def test_paged_selector_mapping_and_nested_fields_are_immutable(
    registry: OperationRegistry,
) -> None:
    arguments = {"side_guid": "SIDE-1", "fields": ["name"]}
    before = registry.resolve_wire_invocation("unit.list", arguments)
    contract = before.contract
    source_arguments = before.wire_arguments
    wire_selection = contract.wire_result_factory(source_arguments)
    public_selection = contract.public_result_factory(source_arguments)

    for selection in (wire_selection, public_selection):
        assert selection.selector is not None
        selector = cast(dict[str, object], selection.selector)
        fields = cast(list[str], selector["fields"])
        with pytest.raises(TypeError):
            selector["fields"] = []
        with pytest.raises(TypeError):
            fields[0] = "type"
        with pytest.raises(AttributeError):
            fields.append("type")

    source_fields = cast(list[str], getattr(source_arguments, "fields"))
    source_fields.append("type")
    assert wire_selection.selector == {"fields": ("guid", "name")}

    after = registry.resolve_wire_invocation("unit.list", arguments)
    assert after.result_schema.schema_id == before.result_schema.schema_id
    _assert_disposable_adapter_schema_equal(
        after.result_schema.adapter, before.result_schema.adapter
    )
    _assert_disposable_adapter_schema_equal(
        after.public_result_adapter, before.public_result_adapter
    )


def test_lua_policy_is_detached_from_source_manifest_after_registry_construction() -> None:
    from cmo_agent_bridge.operations.generated_manifest import OPERATIONS

    entries = copy.deepcopy(OPERATIONS)
    lua_entry = next(entry for entry in entries if entry["name"] == "lua.call")
    policies = cast(dict[str, object], lua_entry["resolver_data"])
    score_policy = cast(dict[str, object], policies["ScenEdit_GetScore"])
    score_policy["class"] = "mutation"
    registry = OperationRegistry(entries)
    manifest_sha256 = registry.manifest_sha256
    arguments = {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}}
    before_public = registry.resolve_invocation("lua.call", arguments)
    before_raw = registry.resolve_wire_invocation("lua.call", arguments)

    score_policy["arguments_model"] = "EmptyArgs"
    score_policy["class"] = "read"
    score_policy["result_model"] = "EmptyArgs"

    after_public = registry.resolve_invocation("lua.call", arguments)
    after_raw = registry.resolve_wire_invocation("lua.call", arguments)
    assert registry.manifest_sha256 == manifest_sha256
    assert after_public.wire_arguments == before_public.wire_arguments
    assert after_raw.wire_arguments == before_raw.wire_arguments
    assert after_public.effective_class is before_public.effective_class is OperationClass.MUTATION
    assert after_raw.effective_class is before_raw.effective_class is OperationClass.MUTATION
    assert after_public.result_schema.schema_id == before_public.result_schema.schema_id
    assert after_raw.result_schema.schema_id == before_raw.result_schema.schema_id
    _assert_disposable_adapter_schema_equal(
        after_public.result_schema.adapter, before_public.result_schema.adapter
    )
    _assert_disposable_adapter_schema_equal(
        after_raw.result_schema.adapter, before_raw.result_schema.adapter
    )
    _assert_disposable_adapter_schema_equal(
        after_public.public_result_adapter, before_public.public_result_adapter
    )
    _assert_disposable_adapter_schema_equal(
        after_raw.public_result_adapter, before_raw.public_result_adapter
    )
    assert before_public.recovery_schema is not None
    assert before_raw.recovery_schema is not None
    assert after_public.recovery_schema is not None
    assert after_raw.recovery_schema is not None
    assert after_public.recovery_schema.schema_id == before_public.recovery_schema.schema_id
    assert after_raw.recovery_schema.schema_id == before_raw.recovery_schema.schema_id
    _assert_disposable_adapter_schema_equal(
        after_public.recovery_schema.adapter, before_public.recovery_schema.adapter
    )
    _assert_disposable_adapter_schema_equal(
        after_raw.recovery_schema.adapter, before_raw.recovery_schema.adapter
    )


def test_lua_policy_reachable_from_contract_is_recursively_immutable(
    registry: OperationRegistry,
) -> None:
    arguments = {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}}
    before_public = registry.resolve_invocation("lua.call", arguments)
    before_raw = registry.resolve_wire_invocation("lua.call", arguments)
    resolver = registry.resolve("lua.call").wire_resolver
    reachable = getattr(resolver, "_policies", None)
    if reachable is None:
        reachable = getattr(resolver, "policies")
    policies = cast(dict[str, object], reachable)
    policy = policies["ScenEdit_GetScore"]

    with pytest.raises(TypeError):
        policies["ScenEdit_GetScore"] = policy
    with pytest.raises(TypeError):
        cast(dict[str, object], policy)["arguments_model"] = "EmptyArgs"
    assert not hasattr(resolver, "policies")

    after_public = registry.resolve_invocation("lua.call", arguments)
    after_raw = registry.resolve_wire_invocation("lua.call", arguments)
    assert after_public.wire_arguments == before_public.wire_arguments
    assert after_raw.wire_arguments == before_raw.wire_arguments
    assert after_public.effective_class is before_public.effective_class
    assert after_raw.effective_class is before_raw.effective_class
    assert after_public.result_schema.schema_id == before_public.result_schema.schema_id
    assert after_raw.result_schema.schema_id == before_raw.result_schema.schema_id
    assert after_public.recovery_schema == before_public.recovery_schema
    assert after_raw.recovery_schema == before_raw.recovery_schema


def test_public_resolution_source_has_no_operation_name_branches() -> None:
    from cmo_agent_bridge.operations.generated_manifest import OPERATIONS

    source = textwrap.dedent(inspect.getsource(OperationRegistry.resolve_invocation))
    tree = ast.parse(source)
    string_constants = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    operation_names = {cast(str, entry["name"]) for entry in OPERATIONS}
    assert string_constants.isdisjoint(operation_names)


def test_frozen_invocation_snapshots_survive_bound_and_unbound_collection_mutation(
    registry: OperationRegistry,
) -> None:
    projected = registry.resolve_invocation(
        "unit.list", {"side_guid": "SIDE-1", "fields": ["name"]}
    )
    projected_before = projected.wire_arguments.model_dump(mode="json")
    public_before = projected.public_arguments.model_dump(mode="json")
    projected_schema = projected.result_schema.schema_id
    projected_copy = projected.wire_arguments
    fields = cast(list[object], getattr(projected_copy, "fields"))
    assert isinstance(fields, list)
    list.append(fields, "type")  # pyright: ignore[reportUnknownMemberType]
    projected_copy.model_fields_set.add("cursor")
    assert projected.wire_arguments.model_dump(mode="json") == projected_before
    assert projected.wire_arguments.model_fields_set == {"side_guid", "fields"}
    assert projected.result_schema.schema_id == projected_schema
    public_fields = cast(list[object], getattr(projected.public_arguments, "fields"))
    list.append(  # pyright: ignore[reportUnknownMemberType]
        public_fields, "type"
    )
    assert projected.public_arguments.model_dump(mode="json") == public_before

    lua = registry.resolve_wire_invocation(
        "lua.call",
        {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}},
    )
    lua_before = lua.wire_arguments.model_dump(mode="json")
    lua_copy = lua.wire_arguments
    lua_arguments = cast(dict[str, object], getattr(lua_copy, "arguments"))
    assert isinstance(lua_arguments, dict)
    dict.__setitem__(  # pyright: ignore[reportUnknownMemberType]
        lua_arguments, "side", "Red"
    )
    dict.__init__(  # pyright: ignore[reportUnknownMemberType]
        lua_arguments, {"side": "Green"}
    )
    assert lua.wire_arguments.model_dump(mode="json") == lua_before

    course = registry.resolve_invocation(
        "unit.set",
        {
            "unit_guid": "UNIT-1",
            "course": [{"latitude": 1.0, "longitude": 2.0}],
        },
    )
    course_before = course.wire_arguments.model_dump(mode="json")
    course_copy = course.wire_arguments
    waypoints = cast(list[object], getattr(course_copy, "course"))
    waypoint = cast(BaseModel, waypoints[0])
    assert course_copy.model_fields_set == {"unit_guid", "course"}
    assert waypoint.model_fields_set == {"latitude", "longitude"}
    list.__init__(waypoints, [])  # pyright: ignore[reportUnknownMemberType]
    waypoint.model_fields_set.add("altitude")
    restored = course.wire_arguments
    restored_waypoints = cast(list[object], getattr(restored, "course"))
    restored_waypoint = cast(BaseModel, restored_waypoints[0])
    assert restored.model_dump(mode="json") == course_before
    assert restored.model_fields_set == {"unit_guid", "course"}
    assert restored_waypoint.model_fields_set == {"latitude", "longitude"}


def test_prepare_and_profile_results_require_exact_release_identity() -> None:
    identity = {
        "runtime_tag": f"0_1_0-{'d' * 64}",
        "runtime_asset_sha256": "d" * 64,
        "release_id": "e" * 64,
    }
    prepare = {
        "managed_asset_version": "0.1.0",
        "managed_asset_sha256": "f" * 64,
        **identity,
        "install_command": "install",
        "inbox_path": "inbox",
        "prepared": True,
        "scenario_installed": True,
    }
    PrepareResult.model_validate(prepare)

    profile = {
        "build": 1868,
        "runtime_version": "0.1.0",
        **identity,
        "protocol": "cmo-agent-bridge/1",
        "manifest_sha256": "a" * 64,
        "primitive_observations": {
            "run_script": True,
            "nested_delivery_global": True,
            "delivery_global_cleaned": True,
            "export_inst_empty_list": True,
            "unicode_roundtrip": True,
            "manifest_match": True,
            "special_action_while_paused": None,
        },
        "capacity_observations": {
            "max_verified_comments_bytes": 4096,
            "safe_payload_bytes": 4096,
            "verified_ledger_entries": 32,
            "effective_ledger_capacity": 32,
        },
        "profile_path": "profile.json",
    }
    CompatibilityProfileResult.model_validate(profile)

    for model, value in ((PrepareResult, prepare), (CompatibilityProfileResult, profile)):
        for field in ("runtime_tag", "runtime_asset_sha256", "release_id"):
            invalid = dict(value)
            invalid.pop(field)
            with pytest.raises(ValidationError):
                model.model_validate(invalid)
        with pytest.raises(ValidationError):
            model.model_validate({**value, "runtime_tag": "wrong"})


def _absent_evidence() -> dict[str, object]:
    return {"present": False, "request_id": None, "state": None, "request_hash": None}


def _present_evidence() -> dict[str, object]:
    return {
        "present": True,
        "request_id": REQUEST_ID,
        "state": "in_progress",
        "request_hash": REQUEST_HASH,
    }


def _evidence(state: str | None) -> dict[str, object]:
    if state is None:
        return _absent_evidence()
    return {**_present_evidence(), "state": state}
