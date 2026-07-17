from __future__ import annotations

from collections.abc import Mapping
from typing import cast
from uuid import UUID

from pydantic import ValidationError

from cmo_agent_bridge.application.ports import (
    CompatibilityPolicyPort,
    CompatibilityProfileLookupPort,
)
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
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, revalidate_runtime_snapshot


_BOOTSTRAP_RECOVERY_OPERATIONS = frozenset(
    {
        "bridge.prepare",
        "bridge.doctor",
        "bridge.status",
        "compat.probe",
        "compat.probe.step",
        "bridge.uninstall",
        "bridge.reconcile",
    }
)
_REQUIRED_TRUE_PRIMITIVES = (
    "run_script",
    "nested_delivery_global",
    "delivery_global_cleaned",
    "export_inst_empty_list",
    "unicode_roundtrip",
    "manifest_match",
)
_CURRENT_REGISTRY = OperationRegistry()
_DESTRUCTIVE_OPERATIONS = frozenset({"unit.delete", "mission.delete"})


def _invalid_inputs(operation: str | None = None) -> BridgeError:
    details = {} if operation is None else {"operation": operation}
    return BridgeError(
        ErrorCode.INVALID_ARGUMENT,
        "compatibility policy inputs are invalid",
        details,
    )


def _unsupported(
    *,
    operation: str,
    status: BridgeStatusResult,
    snapshot: RuntimeSnapshot,
    reason: str,
    mismatches: set[str],
) -> BridgeError:
    return BridgeError(
        ErrorCode.UNSUPPORTED_BUILD,
        "operation requires an exact completed compatibility profile",
        {
            "operation": operation,
            "build": status.build,
            "release_id": snapshot.release_id,
            "reason": reason,
            "mismatches": sorted(mismatches),
        },
    )


def _policy_denied(
    *,
    operation: str,
    operation_class: OperationClass,
    setting: str,
) -> BridgeError:
    return BridgeError(
        ErrorCode.POLICY_DENIED,
        "operation is disabled by bridge policy",
        {
            "operation": operation,
            "operation_class": operation_class.value,
            "policy": setting,
        },
    )


def _quarantine_denied(
    *,
    operation: str,
    operation_class: OperationClass,
    pending_request_id: UUID | None,
) -> BridgeError:
    details = {
        "operation": operation,
        "operation_class": operation_class.value,
    }
    if pending_request_id is not None:
        details["pending_request_id"] = str(pending_request_id)
    return BridgeError(
        ErrorCode.MUTATION_QUARANTINED,
        "mutation is blocked while an earlier outcome is quarantined",
        details,
    )


def _revalidate_status(value: BridgeStatusResult) -> BridgeStatusResult:
    if type(value) is not BridgeStatusResult:
        raise TypeError("status must be exact")
    string_fields = (
        "protocol",
        "runtime_version",
        "runtime_tag",
        "runtime_asset_sha256",
        "release_id",
        "manifest_sha256",
    )
    integer_fields = (
        "build",
        "poll_interval_seconds",
        "safe_payload_bytes",
        "verified_ledger_entries",
        "effective_ledger_capacity",
    )
    if any(type(getattr(value, field)) is not str for field in string_fields):
        raise TypeError("status identity strings must be exact")
    if any(type(getattr(value, field)) is not int for field in integer_fields):
        raise TypeError("status integers must be exact")
    if type(value.quarantined) is not bool:
        raise TypeError("status quarantine must be exact")
    if value.paused_capability is not None and type(value.paused_capability) is not bool:
        raise TypeError("status paused capability must be exact")
    if value.pending_request_id is not None and type(value.pending_request_id) is not UUID:
        raise TypeError("status pending request ID must be exact")
    if type(value.lineage_id) is not UUID or type(value.activation_id) is not UUID:
        raise TypeError("status context UUIDs must be exact")
    for names in (
        value.installed_event_names,
        value.installed_action_names,
        value.installed_trigger_names,
    ):
        if type(names) is not list or any(type(name) is not str for name in names):
            raise TypeError("status installed-name lists must be exact")
    return BridgeStatusResult.model_validate(
        value.model_dump(mode="python", round_trip=True, warnings=False)
    )


def _revalidate_invocation(value: FrozenInvocation) -> FrozenInvocation:
    if type(value) not in {FrozenInvocation, ResolvedInvocation}:
        raise TypeError("invocation must be a frozen registry product")
    contract = value.contract
    if type(contract) is not OperationContract or type(contract.name) is not str:
        raise TypeError("invocation contract must be exact")
    wire_values = value.wire_arguments.model_dump(mode="python", round_trip=True, warnings=False)
    if type(value) is ResolvedInvocation:
        canonical_contract = _CURRENT_REGISTRY.resolve(contract.name)
        trusted_by_operation = cast(
            Mapping[str, frozenset[str]],
            object.__getattribute__(_CURRENT_REGISTRY, "_trusted_fields"),
        )
        declared_trusted_fields = trusted_by_operation.get(canonical_contract.name)
        if declared_trusted_fields is None:
            raise ValueError("registry trusted field declaration is missing")
        trusted_enrichment = {
            field: wire_values[field] for field in declared_trusted_fields if field in wire_values
        }
        canonical = _CURRENT_REGISTRY.resolve_invocation(
            canonical_contract.name,
            value.public_arguments.model_dump(
                mode="python",
                round_trip=True,
                warnings=False,
                exclude_unset=True,
            ),
            trusted_enrichment,
        )
    else:
        canonical = _CURRENT_REGISTRY.resolve_wire_invocation(
            contract.name,
            value.wire_arguments.model_dump(
                mode="python",
                round_trip=True,
                warnings=False,
                exclude_unset=True,
            ),
        )
    if value != canonical:
        raise ValueError("invocation differs from the current registry")
    return value


def _revalidate_destructive_contract(value: OperationContract) -> OperationContract:
    if type(value) is not OperationContract or type(value.name) is not str:
        raise TypeError("contract must be exact")
    canonical = _CURRENT_REGISTRY.resolve(value.name)
    if (
        value != canonical
        or canonical.name not in _DESTRUCTIVE_OPERATIONS
        or canonical.target is not ExecutionTarget.CMO
        or canonical.base_class is not OperationClass.DESTRUCTIVE
        or canonical.confirmation_required is not True
    ):
        raise ValueError("contract differs from a canonical destructive operation")
    return canonical


def _revalidate_profile(value: object) -> CompatibilityProfileResult:
    if type(value) is not CompatibilityProfileResult:
        raise TypeError("profile must be exact")
    profile = value
    if type(profile.build) is not int:
        raise TypeError("profile build must be exact")
    for field in (
        "runtime_version",
        "runtime_tag",
        "runtime_asset_sha256",
        "release_id",
        "protocol",
        "manifest_sha256",
        "profile_path",
    ):
        if type(getattr(profile, field)) is not str:
            raise TypeError("profile strings must be exact")
    primitives = profile.primitive_observations
    capacity = profile.capacity_observations
    if type(primitives) is not PrimitiveObservations or type(capacity) is not CapacityObservations:
        raise TypeError("profile evidence models must be exact")
    for field in _REQUIRED_TRUE_PRIMITIVES:
        if type(getattr(primitives, field)) is not bool:
            raise TypeError("profile primitive booleans must be exact")
    if (
        primitives.special_action_while_paused is not None
        and type(primitives.special_action_while_paused) is not bool
    ):
        raise TypeError("profile paused capability must be exact")
    for field in (
        "max_verified_comments_bytes",
        "safe_payload_bytes",
        "verified_ledger_entries",
        "effective_ledger_capacity",
    ):
        if type(getattr(capacity, field)) is not int:
            raise TypeError("profile capacity integers must be exact")
    return CompatibilityProfileResult.model_validate(
        profile.model_dump(mode="python", round_trip=True, warnings=False)
    )


class CompatibilityPolicy(CompatibilityPolicyPort):
    def __init__(
        self,
        *,
        config: BridgeConfig,
        profiles: CompatibilityProfileLookupPort,
    ) -> None:
        try:
            if type(config) is not BridgeConfig:
                raise TypeError("config must be exact")
            validated_config = BridgeConfig.model_validate(
                config.model_dump(mode="python", round_trip=True, warnings=False)
            )
            if not callable(getattr(profiles, "load", None)):
                raise TypeError("profile lookup must implement load")
        except (AttributeError, TypeError, ValidationError, ValueError) as error:
            raise _invalid_inputs() from error
        self._config = validated_config
        self._profiles = profiles

    def ensure_allowed(
        self,
        *,
        status: BridgeStatusResult,
        invocation: FrozenInvocation,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        operation = self._operation_hint(invocation)
        try:
            current_status = _revalidate_status(status)
            snapshot = revalidate_runtime_snapshot(runtime_snapshot)
            current_invocation = _revalidate_invocation(invocation)
        except (
            AttributeError,
            BridgeError,
            TypeError,
            ValidationError,
            ValueError,
        ) as error:
            raise _invalid_inputs(operation) from error

        operation = current_invocation.contract.name
        if operation in _BOOTSTRAP_RECOVERY_OPERATIONS:
            return

        self._ensure_compatible_and_allowed(
            status=current_status,
            snapshot=snapshot,
            operation=operation,
            operation_class=current_invocation.effective_class,
        )

    def ensure_destructive_allowed(
        self,
        *,
        status: BridgeStatusResult,
        contract: OperationContract,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        operation = self._contract_operation_hint(contract)
        try:
            current_status = _revalidate_status(status)
            snapshot = revalidate_runtime_snapshot(runtime_snapshot)
            current_contract = _revalidate_destructive_contract(contract)
        except (
            AttributeError,
            BridgeError,
            TypeError,
            ValidationError,
            ValueError,
        ) as error:
            raise _invalid_inputs(operation) from error

        self._ensure_compatible_and_allowed(
            status=current_status,
            snapshot=snapshot,
            operation=current_contract.name,
            operation_class=OperationClass.DESTRUCTIVE,
        )

    def _ensure_compatible_and_allowed(
        self,
        *,
        status: BridgeStatusResult,
        snapshot: RuntimeSnapshot,
        operation: str,
        operation_class: OperationClass,
    ) -> None:
        status_mismatches = self._status_snapshot_mismatches(status, snapshot)
        if status_mismatches:
            raise _unsupported(
                operation=operation,
                status=status,
                snapshot=snapshot,
                reason="status_runtime_mismatch",
                mismatches=status_mismatches,
            )

        raw_profile = self._profiles.load(
            build=status.build,
            release_id=snapshot.release_id,
        )
        if raw_profile is None:
            raise _unsupported(
                operation=operation,
                status=status,
                snapshot=snapshot,
                reason="profile_missing",
                mismatches=set(),
            )
        try:
            profile = _revalidate_profile(raw_profile)
        except (AttributeError, TypeError, ValidationError, ValueError) as error:
            raise _unsupported(
                operation=operation,
                status=status,
                snapshot=snapshot,
                reason="profile_identity_mismatch",
                mismatches={"profile"},
            ) from error

        profile_mismatches = self._profile_identity_mismatches(
            profile,
            status,
            snapshot,
        )
        if profile_mismatches:
            raise _unsupported(
                operation=operation,
                status=status,
                snapshot=snapshot,
                reason="profile_identity_mismatch",
                mismatches=profile_mismatches,
            )

        primitive_mismatches = self._primitive_mismatches(profile, status)
        if primitive_mismatches:
            raise _unsupported(
                operation=operation,
                status=status,
                snapshot=snapshot,
                reason="profile_primitives_incomplete",
                mismatches=primitive_mismatches,
            )

        capacity_mismatches = self._capacity_mismatches(profile, status)
        if capacity_mismatches:
            raise _unsupported(
                operation=operation,
                status=status,
                snapshot=snapshot,
                reason="profile_capacity_mismatch",
                mismatches=capacity_mismatches,
            )

        if status.quarantined and operation_class in {
            OperationClass.MUTATION,
            OperationClass.DESTRUCTIVE,
        }:
            raise _quarantine_denied(
                operation=operation,
                operation_class=operation_class,
                pending_request_id=status.pending_request_id,
            )
        if operation_class in {OperationClass.STATUS, OperationClass.READ}:
            return
        if operation_class is OperationClass.MUTATION:
            if not self._config.allow_mutations:
                raise _policy_denied(
                    operation=operation,
                    operation_class=operation_class,
                    setting="allow_mutations",
                )
            return
        if operation_class is OperationClass.DESTRUCTIVE:
            if not self._config.allow_mutations:
                raise _policy_denied(
                    operation=operation,
                    operation_class=operation_class,
                    setting="allow_mutations",
                )
            if not self._config.allow_destructive:
                raise _policy_denied(
                    operation=operation,
                    operation_class=operation_class,
                    setting="allow_destructive",
                )
            return
        raise _policy_denied(
            operation=operation,
            operation_class=operation_class,
            setting="effective_class",
        )

    @staticmethod
    def _operation_hint(value: object) -> str | None:
        contract = getattr(value, "contract", None)
        name = getattr(contract, "name", None)
        return name if type(name) is str else None

    @staticmethod
    def _contract_operation_hint(value: object) -> str | None:
        name = getattr(value, "name", None)
        return name if type(name) is str else None

    @staticmethod
    def _status_snapshot_mismatches(
        status: BridgeStatusResult,
        snapshot: RuntimeSnapshot,
    ) -> set[str]:
        expected = {
            "protocol": snapshot.protocol,
            "runtime_version": snapshot.runtime_version,
            "runtime_tag": snapshot.runtime_tag,
            "runtime_asset_sha256": snapshot.runtime_asset_sha256,
            "release_id": snapshot.release_id,
            "manifest_sha256": snapshot.operation_manifest_sha256,
        }
        return {field for field, value in expected.items() if getattr(status, field) != value}

    @staticmethod
    def _profile_identity_mismatches(
        profile: CompatibilityProfileResult,
        status: BridgeStatusResult,
        snapshot: RuntimeSnapshot,
    ) -> set[str]:
        expected = {
            "build": status.build,
            "protocol": snapshot.protocol,
            "runtime_version": snapshot.runtime_version,
            "runtime_tag": snapshot.runtime_tag,
            "runtime_asset_sha256": snapshot.runtime_asset_sha256,
            "release_id": snapshot.release_id,
            "manifest_sha256": snapshot.operation_manifest_sha256,
        }
        return {field for field, value in expected.items() if getattr(profile, field) != value}

    @staticmethod
    def _primitive_mismatches(
        profile: CompatibilityProfileResult,
        status: BridgeStatusResult,
    ) -> set[str]:
        primitives = profile.primitive_observations
        mismatches = {
            field for field in _REQUIRED_TRUE_PRIMITIVES if getattr(primitives, field) is not True
        }
        paused = primitives.special_action_while_paused
        if paused is None:
            mismatches.add("special_action_while_paused")
        if status.paused_capability is None:
            mismatches.add("paused_capability")
        elif paused is not None and paused is not status.paused_capability:
            mismatches.update({"paused_capability", "special_action_while_paused"})
        return mismatches

    @staticmethod
    def _capacity_mismatches(
        profile: CompatibilityProfileResult,
        status: BridgeStatusResult,
    ) -> set[str]:
        capacity = profile.capacity_observations
        mismatches: set[str] = set()
        expected_safe = min(65_536, capacity.max_verified_comments_bytes // 2)
        if expected_safe < 4_096 or capacity.safe_payload_bytes != expected_safe:
            mismatches.add("safe_payload_bytes")
        if capacity.verified_ledger_entries < 32:
            mismatches.add("verified_ledger_entries")
        expected_effective = min(256, capacity.verified_ledger_entries)
        if capacity.effective_ledger_capacity != expected_effective:
            mismatches.add("effective_ledger_capacity")
        for field in (
            "safe_payload_bytes",
            "verified_ledger_entries",
            "effective_ledger_capacity",
        ):
            if getattr(status, field) != getattr(capacity, field):
                mismatches.add(field)
        return mismatches
