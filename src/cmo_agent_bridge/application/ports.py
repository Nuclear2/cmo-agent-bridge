from __future__ import annotations

from datetime import datetime
from typing import Literal, Protocol, TypeAlias, overload
from uuid import UUID

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


AuditStatus: TypeAlias = Literal["succeeded", "failed", "cancelled"]
LocalOperationName: TypeAlias = Literal[
    "bridge.prepare",
    "bridge.doctor",
    "bridge.uninstall",
]
LocalOperationArguments: TypeAlias = BridgePrepareArgs | BridgeDoctorArgs | BridgeUninstallArgs
LocalOperationResult: TypeAlias = PrepareResult | DoctorResult | UninstallResult


class LocalOperationPort(Protocol):
    @overload
    async def execute(
        self,
        operation: Literal["bridge.prepare"],
        arguments: BridgePrepareArgs,
    ) -> PrepareResult: ...

    @overload
    async def execute(
        self,
        operation: Literal["bridge.doctor"],
        arguments: BridgeDoctorArgs,
    ) -> DoctorResult: ...

    @overload
    async def execute(
        self,
        operation: Literal["bridge.uninstall"],
        arguments: BridgeUninstallArgs,
    ) -> UninstallResult: ...

    async def execute(
        self,
        operation: LocalOperationName,
        arguments: LocalOperationArguments,
    ) -> LocalOperationResult: ...


class CompatibilityProbePort(Protocol):
    async def execute(self, arguments: CompatProbeArgs) -> CompatProbeResult: ...


class InternalCmoRunner(Protocol):
    async def run_status(self) -> BridgeStatusResult: ...

    async def run_compatibility_probe_step(
        self,
        arguments: CompatProbeStepArgs,
    ) -> CompatProbeStepResult: ...


class CompatibilityProfileLookupPort(Protocol):
    def load(
        self,
        *,
        build: int,
        release_id: Sha256,
    ) -> CompatibilityProfileResult | None: ...


class CompatibilityPolicyPort(Protocol):
    def ensure_allowed(
        self,
        *,
        status: BridgeStatusResult,
        invocation: FrozenInvocation,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None: ...

    def ensure_destructive_allowed(
        self,
        *,
        status: BridgeStatusResult,
        contract: OperationContract,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None: ...


class AuditPort(Protocol):
    def write(
        self,
        *,
        occurred_at_utc: datetime,
        request_id: UUID | None,
        operation: str,
        operation_class: OperationClass | None,
        duration_ms: int,
        target_guids: tuple[str, ...],
        status: AuditStatus,
        response_size_bytes: int | None,
        response_sha256: Sha256 | None,
    ) -> None: ...


class WallClockPort(Protocol):
    def now_ms(self) -> int: ...


class MonotonicClockPort(Protocol):
    def now(self) -> float: ...
