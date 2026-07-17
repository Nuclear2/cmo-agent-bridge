from __future__ import annotations

from datetime import datetime
from inspect import signature
from typing import Literal, Optional, TypeAlias, get_args, get_overloads, get_type_hints
from uuid import UUID

import pytest
from pydantic import JsonValue, ValidationError

from cmo_agent_bridge.application import (
    AuditPort,
    AuditStatus,
    CompatibilityPolicyPort,
    CompatibilityProfileLookupPort,
    CompatibilityProbePort,
    InternalCmoRunner,
    InvocationOutcome,
    LocalOperationPort,
    MonotonicClockPort,
    WallClockPort,
)
from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.operations.models import (
    BridgeDoctorArgs,
    BridgePrepareArgs,
    BridgeStatusResult,
    BridgeUninstallArgs,
    CompatProbeArgs,
    CompatProbeResult,
    CompatProbeStepArgs,
    CompatProbeStepResult,
    CompatibilityProfileResult,
    DoctorResult,
    PrepareResult,
    Sha256,
    UninstallResult,
)
from cmo_agent_bridge.operations.registry import FrozenInvocation, OperationContract
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot


REQUEST_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
LocalOperationName: TypeAlias = Literal[
    "bridge.prepare",
    "bridge.doctor",
    "bridge.uninstall",
]
LocalOperationArguments: TypeAlias = BridgePrepareArgs | BridgeDoctorArgs | BridgeUninstallArgs
LocalOperationResult: TypeAlias = PrepareResult | DoctorResult | UninstallResult


def _public_methods(protocol: type[object]) -> set[str]:
    return {
        name
        for name, member in vars(protocol).items()
        if not name.startswith("_") and callable(member)
    }


def _hints(protocol: type[object], method: str) -> dict[str, object]:
    return get_type_hints(getattr(protocol, method), include_extras=True)


def test_invocation_outcome_has_the_locked_strict_shape() -> None:
    assert tuple(InvocationOutcome.model_fields) == (
        "protocol",
        "request_id",
        "ok",
        "result",
        "error",
    )

    outcome = InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=REQUEST_ID,
        ok=True,
        result={"items": ["alpha", 1, True]},
        error=None,
    )

    assert outcome.model_dump(mode="json") == {
        "protocol": "cmo-agent-bridge/1",
        "request_id": str(REQUEST_ID),
        "ok": True,
        "result": {"items": ["alpha", 1, True]},
        "error": None,
    }


@pytest.mark.parametrize(
    "candidate",
    [
        {
            "protocol": "cmo-agent-bridge/2",
            "request_id": REQUEST_ID,
            "ok": True,
            "result": None,
            "error": None,
        },
        {
            "protocol": "cmo-agent-bridge/1",
            "request_id": str(REQUEST_ID),
            "ok": True,
            "result": None,
            "error": None,
        },
        {
            "protocol": "cmo-agent-bridge/1",
            "request_id": REQUEST_ID,
            "ok": 1,
            "result": None,
            "error": None,
        },
        {
            "protocol": "cmo-agent-bridge/1",
            "request_id": REQUEST_ID,
            "ok": True,
            "result": None,
            "error": None,
            "trusted_confirmation_proof": "f" * 64,
        },
    ],
)
def test_invocation_outcome_rejects_coercion_drift_and_extra_fields(
    candidate: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        InvocationOutcome.model_validate(candidate)


@pytest.mark.parametrize(
    "candidate",
    [
        {
            "protocol": "cmo-agent-bridge/1",
            "request_id": REQUEST_ID,
            "ok": True,
            "result": {"guid": "UNIT-1"},
            "error": {"code": "PROTOCOL_ERROR"},
        },
        {
            "protocol": "cmo-agent-bridge/1",
            "request_id": REQUEST_ID,
            "ok": False,
            "result": {"guid": "UNIT-1"},
            "error": {"code": "PROTOCOL_ERROR"},
        },
        {
            "protocol": "cmo-agent-bridge/1",
            "request_id": REQUEST_ID,
            "ok": False,
            "result": None,
            "error": None,
        },
    ],
)
def test_invocation_outcome_enforces_success_error_exclusivity(
    candidate: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        InvocationOutcome.model_validate(candidate)


def test_invocation_outcome_is_frozen_and_copies_input_payloads() -> None:
    result: dict[str, JsonValue] = {"items": ["original"]}
    error: dict[str, JsonValue] = {
        "code": "PROTOCOL_ERROR",
        "details": {"reason": "original"},
    }
    success = InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=REQUEST_ID,
        ok=True,
        result=result,
        error=None,
    )
    failure = InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=REQUEST_ID,
        ok=False,
        result=None,
        error=error,
    )

    result["items"] = ["changed"]
    error["details"] = {"reason": "changed"}

    assert success.result == {"items": ["original"]}
    assert failure.error == {
        "code": "PROTOCOL_ERROR",
        "details": {"reason": "original"},
    }
    with pytest.raises(ValidationError, match="frozen"):
        setattr(success, "ok", False)


def test_local_operation_port_exposes_only_three_typed_execute_routes() -> None:
    assert _public_methods(LocalOperationPort) == {"execute"}
    assert not any(
        hasattr(LocalOperationPort, stale_method)
        for stale_method in ("prepare", "doctor", "uninstall")
    )

    overloads = get_overloads(LocalOperationPort.execute)
    assert [_hints(type("Route", (), {"execute": route}), "execute") for route in overloads] == [
        {
            "operation": Literal["bridge.prepare"],
            "arguments": BridgePrepareArgs,
            "return": PrepareResult,
        },
        {
            "operation": Literal["bridge.doctor"],
            "arguments": BridgeDoctorArgs,
            "return": DoctorResult,
        },
        {
            "operation": Literal["bridge.uninstall"],
            "arguments": BridgeUninstallArgs,
            "return": UninstallResult,
        },
    ]
    assert _hints(LocalOperationPort, "execute") == {
        "operation": LocalOperationName,
        "arguments": LocalOperationArguments,
        "return": LocalOperationResult,
    }
    assert get_args(_hints(LocalOperationPort, "execute")["operation"]) == (
        "bridge.prepare",
        "bridge.doctor",
        "bridge.uninstall",
    )


def test_compatibility_probe_port_exposes_only_typed_execute() -> None:
    assert _public_methods(CompatibilityProbePort) == {"execute"}
    assert _hints(CompatibilityProbePort, "execute") == {
        "arguments": CompatProbeArgs,
        "return": CompatProbeResult,
    }


def test_internal_runner_exposes_only_status_and_manifest_probe_step() -> None:
    assert _public_methods(InternalCmoRunner) == {
        "run_status",
        "run_compatibility_probe_step",
    }
    assert tuple(signature(InternalCmoRunner.run_status).parameters) == ("self",)
    assert _hints(InternalCmoRunner, "run_status") == {"return": BridgeStatusResult}
    assert _hints(InternalCmoRunner, "run_compatibility_probe_step") == {
        "arguments": CompatProbeStepArgs,
        "return": CompatProbeStepResult,
    }


def test_profile_and_policy_ports_preserve_release_and_invocation_types() -> None:
    assert _public_methods(CompatibilityProfileLookupPort) == {"load"}
    assert _hints(CompatibilityProfileLookupPort, "load") == {
        "build": int,
        "release_id": Sha256,
        "return": CompatibilityProfileResult | None,
    }
    assert _public_methods(CompatibilityPolicyPort) == {
        "ensure_allowed",
        "ensure_destructive_allowed",
    }
    assert _hints(CompatibilityPolicyPort, "ensure_allowed") == {
        "status": BridgeStatusResult,
        "invocation": FrozenInvocation,
        "runtime_snapshot": RuntimeSnapshot,
        "return": type(None),
    }
    assert _hints(CompatibilityPolicyPort, "ensure_destructive_allowed") == {
        "status": BridgeStatusResult,
        "contract": OperationContract,
        "runtime_snapshot": RuntimeSnapshot,
        "return": type(None),
    }


def test_audit_and_clock_ports_have_no_arbitrary_payload_escape_hatch() -> None:
    assert _public_methods(AuditPort) == {"write"}
    assert tuple(signature(AuditPort.write).parameters) == (
        "self",
        "occurred_at_utc",
        "request_id",
        "operation",
        "operation_class",
        "duration_ms",
        "target_guids",
        "status",
        "response_size_bytes",
        "response_sha256",
    )
    assert _hints(AuditPort, "write") == {
        "occurred_at_utc": datetime,
        "request_id": UUID | None,
        "operation": str,
        "operation_class": OperationClass | None,
        "duration_ms": int,
        "target_guids": tuple[str, ...],
        "status": AuditStatus,
        "response_size_bytes": int | None,
        "response_sha256": Optional[Sha256],
        "return": type(None),
    }

    assert _public_methods(WallClockPort) == {"now_ms"}
    assert _hints(WallClockPort, "now_ms") == {"return": int}
    assert _public_methods(MonotonicClockPort) == {"now"}
    assert _hints(MonotonicClockPort, "now") == {"return": float}
