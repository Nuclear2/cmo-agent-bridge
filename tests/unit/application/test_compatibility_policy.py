from __future__ import annotations

import inspect
from dataclasses import replace
from typing import cast, get_type_hints
from uuid import UUID

import pytest
from pydantic import BaseModel

from cmo_agent_bridge.application import CompatibilityPolicy
from cmo_agent_bridge.application.ports import CompatibilityPolicyPort
from cmo_agent_bridge.config import BridgeConfig
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import ExecutionTarget, OperationClass
from cmo_agent_bridge.operations.models import (
    BridgeStatusResult,
    CapacityObservations,
    CompatibilityProfileResult,
    PrimitiveObservations,
)
from cmo_agent_bridge.operations.registry import (
    FrozenInvocation,
    OperationContract,
    OperationRegistry,
    ResolvedInvocation,
)
from cmo_agent_bridge.operations.wire import AdapterRecipe
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, derive_runtime_tag


ACTIVATION_ID = UUID("11111111-1111-4111-8111-111111111111")
LINEAGE_ID = UUID("22222222-2222-4222-8222-222222222222")
REQUEST_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
PROOF = "f" * 64
BUILD = 1868


class FakeProfileLookup:
    def __init__(self, profile: CompatibilityProfileResult | None) -> None:
        self.profile = profile
        self.calls: list[tuple[int, str]] = []

    def load(self, *, build: int, release_id: str) -> CompatibilityProfileResult | None:
        self.calls.append((build, release_id))
        return self.profile


@pytest.fixture
def registry() -> OperationRegistry:
    return OperationRegistry()


@pytest.fixture
def snapshot() -> RuntimeSnapshot:
    return _snapshot()


@pytest.fixture
def status(snapshot: RuntimeSnapshot) -> BridgeStatusResult:
    return _status(snapshot)


@pytest.fixture
def profile(snapshot: RuntimeSnapshot) -> CompatibilityProfileResult:
    return _profile(snapshot)


def _snapshot(
    *,
    runtime_version: str = "0.1.0",
    asset: str = "a" * 64,
    manifest: str = "b" * 64,
    host: str = "c" * 64,
    dependency: str = "d" * 64,
) -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version=runtime_version,
        runtime_asset_sha256=asset,
        operation_manifest_sha256=manifest,
        host_contract_sha256=host,
        dependency_lock_sha256=dependency,
    )


def _status(
    snapshot: RuntimeSnapshot,
    *,
    pending_request_id: UUID | None = None,
    quarantined: bool = False,
    paused_capability: bool | None = False,
    safe_payload_bytes: int = 65_536,
    verified_ledger_entries: int = 32,
    effective_ledger_capacity: int = 32,
) -> BridgeStatusResult:
    return BridgeStatusResult(
        protocol=snapshot.protocol,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
        build=BUILD,
        manifest_sha256=snapshot.operation_manifest_sha256,
        lineage_id=LINEAGE_ID,
        activation_id=ACTIVATION_ID,
        installed_event_names=[],
        installed_action_names=[],
        installed_trigger_names=[],
        pending_request_id=pending_request_id,
        quarantined=quarantined,
        paused_capability=paused_capability,
        poll_interval_seconds=5,
        safe_payload_bytes=safe_payload_bytes,
        verified_ledger_entries=verified_ledger_entries,
        effective_ledger_capacity=effective_ledger_capacity,
    )


def _primitives(*, paused: bool | None = False) -> PrimitiveObservations:
    return PrimitiveObservations(
        run_script=True,
        nested_delivery_global=True,
        delivery_global_cleaned=True,
        export_inst_empty_list=True,
        unicode_roundtrip=True,
        manifest_match=True,
        special_action_while_paused=paused,
    )


def _capacity(
    *,
    maximum: int = 131_072,
    safe: int = 65_536,
    verified: int = 32,
    effective: int = 32,
) -> CapacityObservations:
    return CapacityObservations(
        max_verified_comments_bytes=maximum,
        safe_payload_bytes=safe,
        verified_ledger_entries=verified,
        effective_ledger_capacity=effective,
    )


def _profile(
    snapshot: RuntimeSnapshot,
    *,
    build: int = BUILD,
    primitives: PrimitiveObservations | None = None,
    capacity: CapacityObservations | None = None,
) -> CompatibilityProfileResult:
    return CompatibilityProfileResult(
        build=build,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
        protocol=snapshot.protocol,
        manifest_sha256=snapshot.operation_manifest_sha256,
        primitive_observations=_primitives() if primitives is None else primitives,
        capacity_observations=_capacity() if capacity is None else capacity,
        profile_path="profile.json",
    )


def _lookup_policy(
    profile: CompatibilityProfileResult | None,
    *,
    allow_mutations: bool = True,
    allow_destructive: bool = True,
) -> tuple[CompatibilityPolicy, FakeProfileLookup]:
    lookup = FakeProfileLookup(profile)
    policy = CompatibilityPolicy(
        config=BridgeConfig(
            allow_mutations=allow_mutations,
            allow_destructive=allow_destructive,
        ),
        profiles=lookup,
    )
    return policy, lookup


def _read(registry: OperationRegistry) -> ResolvedInvocation:
    return registry.resolve_invocation("scenario.get", {})


def _mutation(registry: OperationRegistry) -> ResolvedInvocation:
    return registry.resolve_invocation("unit.set", {"unit_guid": "UNIT-1", "speed": 10})


def _destructive(registry: OperationRegistry) -> ResolvedInvocation:
    return registry.resolve_invocation(
        "unit.delete",
        {"unit_guid": "UNIT-1"},
        {"confirmation_proof": PROOF},
    )


def _lua_read(registry: OperationRegistry) -> ResolvedInvocation:
    return registry.resolve_invocation(
        "lua.call",
        {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}},
    )


def _forged_invocation(
    invocation: ResolvedInvocation,
    *,
    contract: OperationContract | None = None,
    effective_class: OperationClass | None = None,
    public_result_recipe: AdapterRecipe | None = None,
    public_arguments: BaseModel | None = None,
) -> ResolvedInvocation:
    return ResolvedInvocation._from_resolved_recipe_parts(  # pyright: ignore[reportPrivateUsage]
        contract=invocation.contract if contract is None else contract,
        wire_arguments=invocation.wire_arguments,
        effective_class=(
            invocation.effective_class if effective_class is None else effective_class
        ),
        result_schema=invocation.result_schema,
        recovery_schema=invocation.recovery_schema,
        public_result_recipe=(
            object.__getattribute__(invocation, "_public_result_recipe")
            if public_result_recipe is None
            else public_result_recipe
        ),
        public_arguments=(
            invocation.public_arguments if public_arguments is None else public_arguments
        ),
    )


def _forged_frozen_invocation(
    invocation: FrozenInvocation,
    *,
    public_result_recipe: AdapterRecipe,
) -> FrozenInvocation:
    return FrozenInvocation._from_recipe_parts(  # pyright: ignore[reportPrivateUsage]
        contract=invocation.contract,
        wire_arguments=invocation.wire_arguments,
        effective_class=invocation.effective_class,
        result_schema=invocation.result_schema,
        recovery_schema=invocation.recovery_schema,
        public_result_recipe=public_result_recipe,
    )


def _error(call: object) -> BridgeError:
    assert callable(call)
    with pytest.raises(BridgeError) as caught:
        call()
    return caught.value


def _assert_unsupported(
    error: BridgeError,
    *,
    operation: str,
    status: BridgeStatusResult,
    snapshot: RuntimeSnapshot,
    reason: str,
    mismatches: list[str],
) -> None:
    assert error.code is ErrorCode.UNSUPPORTED_BUILD
    assert error.message == "operation requires an exact completed compatibility profile"
    assert error.details == {
        "operation": operation,
        "build": status.build,
        "release_id": snapshot.release_id,
        "reason": reason,
        "mismatches": sorted(set(mismatches)),
    }


def _assert_policy_denied(
    error: BridgeError,
    *,
    invocation: FrozenInvocation,
    setting: str,
) -> None:
    assert error.code is ErrorCode.POLICY_DENIED
    assert error.message == "operation is disabled by bridge policy"
    assert error.details == {
        "operation": invocation.contract.name,
        "operation_class": invocation.effective_class.value,
        "policy": setting,
    }


def test_all_name_bound_bootstrap_recovery_exceptions_skip_profile_and_switches(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
) -> None:
    policy, lookup = _lookup_policy(
        None,
        allow_mutations=False,
        allow_destructive=False,
    )
    invocations = (
        registry.resolve_invocation("bridge.prepare", {}),
        registry.resolve_invocation("bridge.doctor", {}),
        registry.resolve_invocation("bridge.status", {}, {"activation_candidate": ACTIVATION_ID}),
        registry.resolve_invocation("compat.probe", {"phase": "automatic"}),
        registry.resolve_invocation("compat.probe.step", {"step": "observational"}),
        registry.resolve_invocation("compat.probe.step", {"step": "key-value"}),
        registry.resolve_invocation("bridge.uninstall", {"phase": "command"}),
        registry.resolve_invocation("bridge.reconcile", {}),
        registry.resolve_invocation(
            "bridge.reconcile",
            {"request_id": REQUEST_ID, "disposition": "applied"},
            {"confirmation_proof": PROOF},
        ),
    )

    for invocation in invocations:
        policy.ensure_allowed(
            status=status,
            invocation=invocation,
            runtime_snapshot=snapshot,
        )

    assert lookup.calls == []


def test_forged_same_name_contract_and_effective_class_fail_before_lookup(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
) -> None:
    real = _read(registry)
    forged_contract = _forged_invocation(
        real,
        contract=replace(real.contract, base_class=OperationClass.MUTATION),
    )
    forged_class = _forged_invocation(real, effective_class=OperationClass.DYNAMIC)
    policy, lookup = _lookup_policy(None)

    for invocation in (forged_contract, forged_class):
        error = _error(
            lambda invocation=invocation: policy.ensure_allowed(
                status=status,
                invocation=invocation,
                runtime_snapshot=snapshot,
            )
        )
        assert error.code is ErrorCode.INVALID_ARGUMENT
        assert error.message == "compatibility policy inputs are invalid"
        assert error.details == {"operation": "scenario.get"}

    assert lookup.calls == []


def test_unknown_exact_contract_name_fails_before_lookup(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
) -> None:
    real = _read(registry)
    forged = _forged_invocation(
        real,
        contract=replace(real.contract, name="unknown.operation"),
    )
    policy, lookup = _lookup_policy(None)

    error = _error(
        lambda: policy.ensure_allowed(
            status=status,
            invocation=forged,
            runtime_snapshot=snapshot,
        )
    )

    assert error.code is ErrorCode.INVALID_ARGUMENT
    assert error.message == "compatibility policy inputs are invalid"
    assert error.details == {"operation": "unknown.operation"}
    assert lookup.calls == []


def test_forged_public_result_recipe_fails_before_lookup(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    read = _read(registry)
    mutation = _mutation(registry)
    different_recipe = object.__getattribute__(mutation, "_public_result_recipe")
    forged_recipe = _forged_invocation(
        read,
        public_result_recipe=different_recipe,
    )

    policy, lookup = _lookup_policy(profile)
    error = _error(
        lambda: policy.ensure_allowed(
            status=status,
            invocation=forged_recipe,
            runtime_snapshot=snapshot,
        )
    )
    assert error.code is ErrorCode.INVALID_ARGUMENT
    assert error.message == "compatibility policy inputs are invalid"
    assert error.details == {"operation": "scenario.get"}
    assert lookup.calls == []


def test_canonical_frozen_invocation_passes_and_forged_recipe_fails_before_lookup(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    omitted_defaults = registry.resolve_wire_invocation("side.list", {})
    explicit_default = registry.resolve_wire_invocation("side.list", {"page_size": 100})
    assert type(omitted_defaults) is FrozenInvocation
    assert omitted_defaults != explicit_default

    for canonical in (omitted_defaults, explicit_default):
        allowed, allowed_lookup = _lookup_policy(profile)
        allowed.ensure_allowed(
            status=status,
            invocation=canonical,
            runtime_snapshot=snapshot,
        )

        assert allowed_lookup.calls == [(BUILD, snapshot.release_id)]

    different_recipe = object.__getattribute__(_mutation(registry), "_public_result_recipe")
    forged = _forged_frozen_invocation(
        omitted_defaults,
        public_result_recipe=different_recipe,
    )
    denied, denied_lookup = _lookup_policy(profile)

    error = _error(
        lambda: denied.ensure_allowed(
            status=status,
            invocation=forged,
            runtime_snapshot=snapshot,
        )
    )

    assert error.code is ErrorCode.INVALID_ARGUMENT
    assert error.message == "compatibility policy inputs are invalid"
    assert error.details == {"operation": "side.list"}
    assert denied_lookup.calls == []


def test_forged_public_arguments_inconsistent_with_wire_fail_before_lookup(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    mutation = _mutation(registry)
    different_public = registry.resolve_invocation(
        "unit.set",
        {"unit_guid": "UNIT-1", "speed": 20},
    )
    forged_public = _forged_invocation(
        mutation,
        public_arguments=different_public.public_arguments,
    )

    policy, lookup = _lookup_policy(profile)
    error = _error(
        lambda: policy.ensure_allowed(
            status=status,
            invocation=forged_public,
            runtime_snapshot=snapshot,
        )
    )
    assert error.code is ErrorCode.INVALID_ARGUMENT
    assert error.message == "compatibility policy inputs are invalid"
    assert error.details == {"operation": "unit.set"}
    assert lookup.calls == []


def test_exact_profile_allows_domain_read_and_uses_exact_lookup_key(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    policy, lookup = _lookup_policy(profile)

    policy.ensure_allowed(
        status=status,
        invocation=_read(registry),
        runtime_snapshot=snapshot,
    )

    assert lookup.calls == [(BUILD, snapshot.release_id)]


def test_missing_profile_has_stable_unsupported_build_error(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
) -> None:
    policy, lookup = _lookup_policy(None)
    invocation = _read(registry)

    error = _error(
        lambda: policy.ensure_allowed(
            status=status,
            invocation=invocation,
            runtime_snapshot=snapshot,
        )
    )

    _assert_unsupported(
        error,
        operation="scenario.get",
        status=status,
        snapshot=snapshot,
        reason="profile_missing",
        mismatches=[],
    )
    assert lookup.calls == [(BUILD, snapshot.release_id)]


def test_status_snapshot_identity_drift_fails_before_profile_lookup(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    alternate_version = "0.2.0"
    alternate_asset = "9" * 64
    cases = (
        (
            "version",
            status.model_copy(
                update={
                    "runtime_version": alternate_version,
                    "runtime_tag": derive_runtime_tag(
                        alternate_version, snapshot.runtime_asset_sha256
                    ),
                }
            ),
            snapshot,
            ["runtime_tag", "runtime_version"],
        ),
        (
            "asset",
            status.model_copy(
                update={
                    "runtime_asset_sha256": alternate_asset,
                    "runtime_tag": derive_runtime_tag(snapshot.runtime_version, alternate_asset),
                }
            ),
            snapshot,
            ["runtime_asset_sha256", "runtime_tag"],
        ),
        (
            "release",
            status.model_copy(update={"release_id": "8" * 64}),
            snapshot,
            ["release_id"],
        ),
        (
            "manifest",
            status.model_copy(update={"manifest_sha256": "7" * 64}),
            snapshot,
            ["manifest_sha256"],
        ),
        (
            "host-derived-release",
            status,
            _snapshot(host="6" * 64),
            ["release_id"],
        ),
        (
            "dependency-derived-release",
            status,
            _snapshot(dependency="5" * 64),
            ["release_id"],
        ),
    )

    for _label, candidate_status, candidate_snapshot, mismatches in cases:
        policy, lookup = _lookup_policy(profile)
        error = _error(
            lambda candidate_status=candidate_status, candidate_snapshot=candidate_snapshot: (
                policy.ensure_allowed(
                    status=candidate_status,
                    invocation=_read(registry),
                    runtime_snapshot=candidate_snapshot,
                )
            )
        )
        _assert_unsupported(
            error,
            operation="scenario.get",
            status=candidate_status,
            snapshot=candidate_snapshot,
            reason="status_runtime_mismatch",
            mismatches=mismatches,
        )
        assert lookup.calls == []


def test_forged_status_protocol_fails_input_validation_before_lookup(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    forged = status.model_copy(update={"protocol": "cmo-agent-bridge/2"})
    policy, lookup = _lookup_policy(profile)

    error = _error(
        lambda: policy.ensure_allowed(
            status=forged,
            invocation=_read(registry),
            runtime_snapshot=snapshot,
        )
    )

    assert error.code is ErrorCode.INVALID_ARGUMENT
    assert lookup.calls == []


def test_profile_identity_and_same_semver_release_drifts_are_rejected(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    version = "0.2.0"
    asset = "9" * 64
    asset_snapshot = _snapshot(asset=asset)
    manifest_snapshot = _snapshot(manifest="8" * 64)
    cases = (
        (profile.model_copy(update={"build": BUILD + 1}), ["build"]),
        (
            profile.model_copy(
                update={
                    "runtime_version": version,
                    "runtime_tag": derive_runtime_tag(version, snapshot.runtime_asset_sha256),
                }
            ),
            ["runtime_tag", "runtime_version"],
        ),
        (
            profile.model_copy(
                update={
                    "runtime_asset_sha256": asset,
                    "runtime_tag": derive_runtime_tag(snapshot.runtime_version, asset),
                }
            ),
            ["runtime_asset_sha256", "runtime_tag"],
        ),
        (profile.model_copy(update={"release_id": "7" * 64}), ["release_id"]),
        (profile.model_copy(update={"manifest_sha256": "6" * 64}), ["manifest_sha256"]),
        (
            _profile(asset_snapshot),
            ["release_id", "runtime_asset_sha256", "runtime_tag"],
        ),
        (
            _profile(manifest_snapshot),
            ["manifest_sha256", "release_id"],
        ),
        (_profile(_snapshot(host="4" * 64)), ["release_id"]),
        (_profile(_snapshot(dependency="3" * 64)), ["release_id"]),
    )

    for candidate, mismatches in cases:
        policy, lookup = _lookup_policy(candidate)
        error = _error(
            lambda: policy.ensure_allowed(
                status=status,
                invocation=_read(registry),
                runtime_snapshot=snapshot,
            )
        )
        _assert_unsupported(
            error,
            operation="scenario.get",
            status=status,
            snapshot=snapshot,
            reason="profile_identity_mismatch",
            mismatches=mismatches,
        )
        assert lookup.calls == [(BUILD, snapshot.release_id)]


@pytest.mark.parametrize(
    "field",
    [
        "run_script",
        "nested_delivery_global",
        "delivery_global_cleaned",
        "export_inst_empty_list",
        "unicode_roundtrip",
        "manifest_match",
    ],
)
def test_each_mandatory_primitive_must_be_exact_true(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
    field: str,
) -> None:
    primitives = profile.primitive_observations.model_copy(update={field: False})
    candidate = profile.model_copy(update={"primitive_observations": primitives})
    policy, _lookup = _lookup_policy(candidate)

    error = _error(
        lambda: policy.ensure_allowed(
            status=status,
            invocation=_read(registry),
            runtime_snapshot=snapshot,
        )
    )

    _assert_unsupported(
        error,
        operation="scenario.get",
        status=status,
        snapshot=snapshot,
        reason="profile_primitives_incomplete",
        mismatches=[field],
    )


def test_paused_capability_requires_complete_exact_profile_status_pair(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    cases = (
        (
            status,
            profile.model_copy(update={"primitive_observations": _primitives(paused=None)}),
            ["special_action_while_paused"],
        ),
        (
            status.model_copy(update={"paused_capability": None}),
            profile,
            ["paused_capability"],
        ),
        (
            status.model_copy(update={"paused_capability": True}),
            profile,
            ["paused_capability", "special_action_while_paused"],
        ),
    )

    for candidate_status, candidate_profile, mismatches in cases:
        policy, _lookup = _lookup_policy(candidate_profile)
        error = _error(
            lambda: policy.ensure_allowed(
                status=candidate_status,
                invocation=_read(registry),
                runtime_snapshot=snapshot,
            )
        )
        _assert_unsupported(
            error,
            operation="scenario.get",
            status=candidate_status,
            snapshot=snapshot,
            reason="profile_primitives_incomplete",
            mismatches=mismatches,
        )

    for paused in (False, True):
        candidate_status = status.model_copy(update={"paused_capability": paused})
        candidate_profile = profile.model_copy(
            update={"primitive_observations": _primitives(paused=paused)}
        )
        policy, _lookup = _lookup_policy(candidate_profile)
        policy.ensure_allowed(
            status=candidate_status,
            invocation=_read(registry),
            runtime_snapshot=snapshot,
        )


def test_capacity_derivation_minimum_and_status_matches_are_enforced(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    derivation_cases = (
        (
            status,
            profile.model_copy(update={"capacity_observations": _capacity(maximum=131_070)}),
            ["safe_payload_bytes"],
        ),
        (
            status.model_copy(update={"safe_payload_bytes": 2_048}),
            profile.model_copy(
                update={
                    "capacity_observations": _capacity(
                        maximum=4_096,
                        safe=2_048,
                    )
                }
            ),
            ["safe_payload_bytes"],
        ),
        (
            status.model_copy(
                update={
                    "verified_ledger_entries": 31,
                    "effective_ledger_capacity": 31,
                }
            ),
            profile.model_copy(
                update={
                    "capacity_observations": _capacity(
                        verified=31,
                        effective=31,
                    )
                }
            ),
            ["verified_ledger_entries"],
        ),
        (
            status.model_copy(update={"effective_ledger_capacity": 31}),
            profile.model_copy(update={"capacity_observations": _capacity(effective=31)}),
            ["effective_ledger_capacity"],
        ),
    )
    status_match_cases = (
        (
            status.model_copy(update={"safe_payload_bytes": 65_535}),
            profile,
            ["safe_payload_bytes"],
        ),
        (
            status.model_copy(
                update={
                    "verified_ledger_entries": 33,
                    "effective_ledger_capacity": 33,
                }
            ),
            profile,
            ["effective_ledger_capacity", "verified_ledger_entries"],
        ),
    )

    for candidate_status, candidate_profile, mismatches in derivation_cases:
        capacity = candidate_profile.capacity_observations
        assert candidate_status.safe_payload_bytes == capacity.safe_payload_bytes
        assert candidate_status.verified_ledger_entries == capacity.verified_ledger_entries
        assert candidate_status.effective_ledger_capacity == capacity.effective_ledger_capacity
        policy, lookup = _lookup_policy(candidate_profile)
        error = _error(
            lambda: policy.ensure_allowed(
                status=candidate_status,
                invocation=_read(registry),
                runtime_snapshot=snapshot,
            )
        )
        _assert_unsupported(
            error,
            operation="scenario.get",
            status=candidate_status,
            snapshot=snapshot,
            reason="profile_capacity_mismatch",
            mismatches=mismatches,
        )
        assert lookup.calls == [(BUILD, snapshot.release_id)]

    for candidate_status, candidate_profile, mismatches in status_match_cases:
        policy, lookup = _lookup_policy(candidate_profile)
        error = _error(
            lambda: policy.ensure_allowed(
                status=candidate_status,
                invocation=_read(registry),
                runtime_snapshot=snapshot,
            )
        )
        _assert_unsupported(
            error,
            operation="scenario.get",
            status=candidate_status,
            snapshot=snapshot,
            reason="profile_capacity_mismatch",
            mismatches=mismatches,
        )
        assert lookup.calls == [(BUILD, snapshot.release_id)]

    for maximum, safe in ((8_192, 4_096), (131_072, 65_536), (1_048_576, 65_536)):
        candidate_status = status.model_copy(update={"safe_payload_bytes": safe})
        candidate_profile = profile.model_copy(
            update={"capacity_observations": _capacity(maximum=maximum, safe=safe)}
        )
        policy, lookup = _lookup_policy(candidate_profile)
        policy.ensure_allowed(
            status=candidate_status,
            invocation=_read(registry),
            runtime_snapshot=snapshot,
        )
        assert lookup.calls == [(BUILD, snapshot.release_id)]


def test_mutation_switch_and_allowlisted_dynamic_read_use_effective_class(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    denied, _lookup = _lookup_policy(profile, allow_mutations=False)
    mutation = _mutation(registry)

    error = _error(
        lambda: denied.ensure_allowed(
            status=status,
            invocation=mutation,
            runtime_snapshot=snapshot,
        )
    )
    _assert_policy_denied(error, invocation=mutation, setting="allow_mutations")

    denied.ensure_allowed(
        status=status,
        invocation=_lua_read(registry),
        runtime_snapshot=snapshot,
    )
    allowed, _lookup = _lookup_policy(profile, allow_mutations=True)
    allowed.ensure_allowed(
        status=status,
        invocation=mutation,
        runtime_snapshot=snapshot,
    )


@pytest.mark.parametrize(
    ("allow_mutations", "allow_destructive", "denied_setting"),
    [
        (False, False, "allow_mutations"),
        (False, True, "allow_mutations"),
        (True, False, "allow_destructive"),
        (True, True, None),
    ],
)
def test_destructive_policy_requires_both_switches_in_order(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
    allow_mutations: bool,
    allow_destructive: bool,
    denied_setting: str | None,
) -> None:
    policy, _lookup = _lookup_policy(
        profile,
        allow_mutations=allow_mutations,
        allow_destructive=allow_destructive,
    )
    invocation = _destructive(registry)

    if denied_setting is None:
        policy.ensure_allowed(
            status=status,
            invocation=invocation,
            runtime_snapshot=snapshot,
        )
    else:
        error = _error(
            lambda: policy.ensure_allowed(
                status=status,
                invocation=invocation,
                runtime_snapshot=snapshot,
            )
        )
        _assert_policy_denied(error, invocation=invocation, setting=denied_setting)


def test_quarantine_allows_read_blocks_mutation_before_config_and_preserves_reconcile(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    profile: CompatibilityProfileResult,
) -> None:
    quarantined = _status(
        snapshot,
        pending_request_id=REQUEST_ID,
        quarantined=True,
    )
    policy, lookup = _lookup_policy(
        profile,
        allow_mutations=False,
        allow_destructive=False,
    )
    policy.ensure_allowed(
        status=quarantined,
        invocation=_read(registry),
        runtime_snapshot=snapshot,
    )

    for invocation in (_mutation(registry), _destructive(registry)):
        error = _error(
            lambda invocation=invocation: policy.ensure_allowed(
                status=quarantined,
                invocation=invocation,
                runtime_snapshot=snapshot,
            )
        )
        assert error.code is ErrorCode.MUTATION_QUARANTINED
        assert error.message == "mutation is blocked while an earlier outcome is quarantined"
        assert error.details == {
            "operation": invocation.contract.name,
            "operation_class": invocation.effective_class.value,
            "pending_request_id": str(REQUEST_ID),
        }

    calls_before_reconcile = list(lookup.calls)
    policy.ensure_allowed(
        status=quarantined,
        invocation=registry.resolve_invocation("bridge.reconcile", {}),
        runtime_snapshot=snapshot,
    )
    assert lookup.calls == calls_before_reconcile


def test_quarantine_without_pending_request_omits_pending_detail(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    profile: CompatibilityProfileResult,
) -> None:
    status = _status(snapshot, quarantined=True)
    policy, _lookup = _lookup_policy(profile)
    invocation = _mutation(registry)

    error = _error(
        lambda: policy.ensure_allowed(
            status=status,
            invocation=invocation,
            runtime_snapshot=snapshot,
        )
    )

    assert error.code is ErrorCode.MUTATION_QUARANTINED
    assert error.details == {
        "operation": "unit.set",
        "operation_class": "mutation",
    }


def test_forged_profile_scalar_fails_closed_after_exactly_one_lookup(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    forged = profile.model_copy(update={"build": "1868"})
    policy, lookup = _lookup_policy(forged)

    error = _error(
        lambda: policy.ensure_allowed(
            status=status,
            invocation=_read(registry),
            runtime_snapshot=snapshot,
        )
    )

    _assert_unsupported(
        error,
        operation="scenario.get",
        status=status,
        snapshot=snapshot,
        reason="profile_identity_mismatch",
        mismatches=["profile"],
    )
    assert lookup.calls == [(BUILD, snapshot.release_id)]


def test_destructive_preflight_port_and_concrete_surface_are_exact_and_synchronous() -> None:
    public_methods = {
        name
        for name, member in vars(CompatibilityPolicyPort).items()
        if not name.startswith("_") and callable(member)
    }
    assert public_methods == {"ensure_allowed", "ensure_destructive_allowed"}

    for owner in (CompatibilityPolicyPort, CompatibilityPolicy):
        method = owner.ensure_destructive_allowed
        signature = inspect.signature(method)
        assert tuple(signature.parameters) == (
            "self",
            "status",
            "contract",
            "runtime_snapshot",
        )
        assert signature.parameters["self"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for name in ("status", "contract", "runtime_snapshot"):
            parameter = signature.parameters[name]
            assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
            assert parameter.default is inspect.Parameter.empty
        assert not inspect.iscoroutinefunction(method)
        assert get_type_hints(method) == {
            "status": BridgeStatusResult,
            "contract": OperationContract,
            "runtime_snapshot": RuntimeSnapshot,
            "return": type(None),
        }


def test_destructive_preflight_accepts_both_canonical_contracts_without_invocation_or_proof(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contracts = (
        registry.resolve("unit.delete"),
        registry.resolve("mission.delete"),
    )
    policy, lookup = _lookup_policy(profile)

    def forbidden_resolution(*_args: object, **_kwargs: object) -> object:
        pytest.fail("destructive preflight must not resolve a wire/public invocation or proof")

    monkeypatch.setattr(OperationRegistry, "resolve_invocation", forbidden_resolution)
    monkeypatch.setattr(OperationRegistry, "resolve_wire_invocation", forbidden_resolution)

    for contract in contracts:
        policy.ensure_destructive_allowed(
            status=status,
            contract=contract,
            runtime_snapshot=snapshot,
        )

    assert lookup.calls == [
        (BUILD, snapshot.release_id),
        (BUILD, snapshot.release_id),
    ]


def test_destructive_preflight_accepts_equal_nonidentical_current_registry_contracts(
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    first = OperationRegistry()
    second = OperationRegistry()
    policy, lookup = _lookup_policy(profile)

    for name in ("unit.delete", "mission.delete"):
        first_contract = first.resolve(name)
        second_contract = second.resolve(name)
        assert first_contract == second_contract
        assert first_contract is not second_contract
        policy.ensure_destructive_allowed(
            status=status,
            contract=first_contract,
            runtime_snapshot=snapshot,
        )
        policy.ensure_destructive_allowed(
            status=status,
            contract=second_contract,
            runtime_snapshot=snapshot,
        )

    assert lookup.calls == [(BUILD, snapshot.release_id)] * 4


def test_destructive_preflight_rejects_nonexact_wrong_and_every_field_drift_before_lookup(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    canonical = registry.resolve("unit.delete")
    other = registry.resolve("scenario.get")
    mission = registry.resolve("mission.delete")
    forged_cases: tuple[tuple[str, OperationContract], ...] = (
        ("non-exact", cast(OperationContract, object())),
        ("unknown-name", replace(canonical, name="unknown.delete")),
        ("wrong-known-name", replace(canonical, name=mission.name)),
        ("target", replace(canonical, target=ExecutionTarget.LOCAL)),
        ("base-class", replace(canonical, base_class=OperationClass.MUTATION)),
        (
            "public-arguments-recipe",
            replace(
                canonical,
                _public_arguments_recipe=object.__getattribute__(other, "_public_arguments_recipe"),
            ),
        ),
        (
            "wire-arguments-recipe",
            replace(
                canonical,
                _wire_arguments_recipe=object.__getattribute__(other, "_wire_arguments_recipe"),
            ),
        ),
        ("wire-resolver", replace(canonical, wire_resolver=other.wire_resolver)),
        (
            "effective-class-resolver",
            replace(canonical, effective_class_resolver=other.effective_class_resolver),
        ),
        (
            "wire-result-factory",
            replace(canonical, wire_result_factory=other.wire_result_factory),
        ),
        (
            "public-result-factory",
            replace(canonical, public_result_factory=other.public_result_factory),
        ),
        ("recovery-factory", replace(canonical, recovery_factory=other.recovery_factory)),
        ("confirmation-required", replace(canonical, confirmation_required=False)),
        ("expose-mcp", replace(canonical, expose_mcp=True)),
        ("canonical-nondestructive", registry.resolve("unit.set")),
        ("canonical-wrong-class", registry.resolve("bridge.reconcile")),
    )

    for label, candidate in forged_cases:
        policy, lookup = _lookup_policy(profile)
        error = _error(
            lambda candidate=candidate: policy.ensure_destructive_allowed(
                status=status,
                contract=candidate,
                runtime_snapshot=snapshot,
            )
        )
        assert error.code is ErrorCode.INVALID_ARGUMENT, label
        assert error.message == "compatibility policy inputs are invalid", label
        assert lookup.calls == [], label


def test_destructive_preflight_reuses_exact_status_profile_primitive_and_capacity_denials(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    contract = registry.resolve("unit.delete")
    primitive_profile = profile.model_copy(
        update={
            "primitive_observations": profile.primitive_observations.model_copy(
                update={"unicode_roundtrip": False}
            )
        }
    )
    capacity_profile = profile.model_copy(
        update={
            "capacity_observations": profile.capacity_observations.model_copy(
                update={"effective_ledger_capacity": 31}
            )
        }
    )
    cases: tuple[
        tuple[
            str,
            BridgeStatusResult,
            RuntimeSnapshot,
            CompatibilityProfileResult | None,
            str,
            list[str],
            int,
        ],
        ...,
    ] = (
        (
            "status-runtime-mismatch",
            status.model_copy(update={"release_id": "9" * 64}),
            snapshot,
            profile,
            "status_runtime_mismatch",
            ["release_id"],
            0,
        ),
        (
            "profile-missing",
            status,
            snapshot,
            None,
            "profile_missing",
            [],
            1,
        ),
        (
            "profile-stale",
            status,
            snapshot,
            profile.model_copy(update={"build": BUILD - 1}),
            "profile_identity_mismatch",
            ["build"],
            1,
        ),
        (
            "profile-mismatched",
            status,
            snapshot,
            profile.model_copy(update={"release_id": "8" * 64}),
            "profile_identity_mismatch",
            ["release_id"],
            1,
        ),
        (
            "profile-primitive",
            status,
            snapshot,
            primitive_profile,
            "profile_primitives_incomplete",
            ["unicode_roundtrip"],
            1,
        ),
        (
            "profile-capacity",
            status,
            snapshot,
            capacity_profile,
            "profile_capacity_mismatch",
            ["effective_ledger_capacity"],
            1,
        ),
    )

    for (
        label,
        candidate_status,
        candidate_snapshot,
        candidate_profile,
        reason,
        mismatches,
        calls,
    ) in cases:
        policy, lookup = _lookup_policy(candidate_profile)
        error = _error(
            lambda: policy.ensure_destructive_allowed(
                status=candidate_status,
                contract=contract,
                runtime_snapshot=candidate_snapshot,
            )
        )
        _assert_unsupported(
            error,
            operation="unit.delete",
            status=candidate_status,
            snapshot=candidate_snapshot,
            reason=reason,
            mismatches=mismatches,
        )
        assert lookup.calls == [(BUILD, snapshot.release_id)] * calls, label


def test_destructive_preflight_reuses_exact_input_quarantine_and_config_denials(
    registry: OperationRegistry,
    snapshot: RuntimeSnapshot,
    status: BridgeStatusResult,
    profile: CompatibilityProfileResult,
) -> None:
    contract = registry.resolve("unit.delete")

    for candidate_status, candidate_snapshot in (
        (cast(BridgeStatusResult, object()), snapshot),
        (status, cast(RuntimeSnapshot, object())),
    ):
        policy, lookup = _lookup_policy(profile)
        error = _error(
            lambda: policy.ensure_destructive_allowed(
                status=candidate_status,
                contract=contract,
                runtime_snapshot=candidate_snapshot,
            )
        )
        assert error.code is ErrorCode.INVALID_ARGUMENT
        assert error.message == "compatibility policy inputs are invalid"
        assert error.details == {"operation": "unit.delete"}
        assert lookup.calls == []

    quarantined = status.model_copy(update={"quarantined": True, "pending_request_id": REQUEST_ID})
    quarantine_policy, quarantine_lookup = _lookup_policy(profile)
    quarantine_error = _error(
        lambda: quarantine_policy.ensure_destructive_allowed(
            status=quarantined,
            contract=contract,
            runtime_snapshot=snapshot,
        )
    )
    assert quarantine_error.code is ErrorCode.MUTATION_QUARANTINED
    assert quarantine_error.message == (
        "mutation is blocked while an earlier outcome is quarantined"
    )
    assert quarantine_error.details == {
        "operation": "unit.delete",
        "operation_class": "destructive",
        "pending_request_id": str(REQUEST_ID),
    }
    assert quarantine_lookup.calls == [(BUILD, snapshot.release_id)]

    for allow_mutations, allow_destructive, setting in (
        (False, True, "allow_mutations"),
        (True, False, "allow_destructive"),
    ):
        policy, lookup = _lookup_policy(
            profile,
            allow_mutations=allow_mutations,
            allow_destructive=allow_destructive,
        )
        error = _error(
            lambda: policy.ensure_destructive_allowed(
                status=status,
                contract=contract,
                runtime_snapshot=snapshot,
            )
        )
        assert error.code is ErrorCode.POLICY_DENIED
        assert error.message == "operation is disabled by bridge policy"
        assert error.details == {
            "operation": "unit.delete",
            "operation_class": "destructive",
            "policy": setting,
        }
        assert lookup.calls == [(BUILD, snapshot.release_id)]
