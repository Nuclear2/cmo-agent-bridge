from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from enum import StrEnum
from typing import Callable, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, model_validator
from typing_extensions import Self

from cmo_agent_bridge.protocol.models import ExchangeCommand
from cmo_agent_bridge.protocol.response_models import ResponseArtifact
from cmo_agent_bridge.protocol.runtime import Sha256
from cmo_agent_bridge.state.models import HostRequestState
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo


class RecoveryDisposition(StrEnum):
    NO_PENDING = "no_pending"
    FAILED_BEFORE_PUBLISH = "failed_before_publish"
    SETTLED = "settled"
    QUARANTINED = "quarantined"


class RecoveryReport(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
    )

    disposition: RecoveryDisposition
    request_id: UUID | None
    request_state: HostRequestState | None
    response_cleanup_required: bool

    @model_validator(mode="after")
    def validate_consistency(self) -> Self:
        if self.disposition is RecoveryDisposition.NO_PENDING:
            if (
                self.request_id is not None
                or self.request_state is not None
                or self.response_cleanup_required
            ):
                raise ValueError("no-pending recovery cannot identify a request or cleanup")
            return self

        if self.request_id is None or self.request_state is None:
            raise ValueError("recovery outcome requires request identity and state")

        if self.disposition is RecoveryDisposition.FAILED_BEFORE_PUBLISH:
            if (
                self.request_state is not HostRequestState.REJECTED
                or self.response_cleanup_required
            ):
                raise ValueError("prepublication failure requires rejected state without cleanup")
            return self

        if self.disposition is RecoveryDisposition.SETTLED:
            if (
                self.request_state
                not in {
                    HostRequestState.COMPLETED,
                    HostRequestState.CANCELLED,
                    HostRequestState.REJECTED,
                    HostRequestState.RESOLVED,
                }
                or not self.response_cleanup_required
            ):
                raise ValueError("settled recovery requires a terminal state and response cleanup")
            return self

        if self.request_state is not HostRequestState.QUARANTINED or self.response_cleanup_required:
            raise ValueError("quarantined recovery requires quarantined state without cleanup")
        return self


class BridgeChannel(Protocol):
    @property
    def process_identity(self) -> ProcessInfo: ...

    async def exchange(self, command: ExchangeCommand) -> ResponseArtifact: ...

    async def recover_pending(self) -> RecoveryReport: ...


class DurableBridgeChannel(BridgeChannel, Protocol):
    """Worker-only channel that exposes the startup recovery outcome."""

    @property
    def recovery_report(self) -> RecoveryReport: ...


class BridgeTransport(Protocol):
    @property
    def root_key(self) -> Sha256: ...

    def session(self) -> AbstractAsyncContextManager[BridgeChannel]: ...


class DurableBridgeTransport(BridgeTransport, Protocol):
    """Transport extension used only by the persisted mutation worker."""

    def worker_session(
        self,
        *,
        recovery_owner: Callable[[ProcessInfo], UUID | None],
    ) -> AbstractAsyncContextManager[DurableBridgeChannel]: ...
