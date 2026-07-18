from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import math
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Callable, Mapping
from contextlib import suppress
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier, Lock, get_ident
from types import SimpleNamespace, TracebackType
from typing import Any, cast
from uuid import UUID

import pytest
from pydantic import BaseModel, ConfigDict, JsonValue

from cmo_agent_bridge.application import BridgeApplication
from cmo_agent_bridge.application.confirmation import (
    DESTRUCTIVE_CONFIRMATION_FORMAT,
    ConfirmationBinding,
    ConfirmationTokenStore,
    DestructiveConfirmationDescriptor,
    DestructiveTarget,
    IssuedConfirmation,
    destructive_confirmation_binding,
)
from cmo_agent_bridge.application.models import InvocationOutcome
from cmo_agent_bridge.application.session_service import (
    PreparedReadAttempt,
    SessionActivation,
    SessionScope,
    SessionService,
)
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.models import (
    BridgeDoctorArgs,
    BridgePrepareArgs,
    BridgeStatusArgs,
    BridgeStatusResult,
    BridgeStatusWireArgs,
    BridgeUninstallArgs,
    CompatArmResult,
    CompatProbeArgs,
    DeleteResult,
    DestructivePreviewResult,
    DoctorCheck,
    DoctorResult,
    MissionResult,
    PrepareResult,
    ScenarioResult,
    UninstallCommandResult,
    UnitResult,
    UnitSetResult,
)
from cmo_agent_bridge.operations.generated_manifest import OPERATIONS
from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.operations.registry import (
    OPERATION_REGISTRY,
    OperationContract,
    OperationRegistry,
    ResolvedInvocation,
)
from cmo_agent_bridge.protocol.canonical import request_sha256
from cmo_agent_bridge.protocol.models import ExchangeCommand, RequestBody
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CompletedSettlement,
    MutationNotStartedEvidence,
    RejectedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
    ResponseError,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, Sha256
from cmo_agent_bridge.state.session_store import SessionStore
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.models import (
    RecoveryDisposition,
    RecoveryReport,
)
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo

import cmo_agent_bridge.application.service as service_module


LINEAGE_ID = UUID("11111111-1111-4111-8111-111111111111")
LINEAGE_2 = UUID("99999999-9999-4999-8999-999999999999")
REQUEST_1 = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
REQUEST_2 = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
REQUEST_3 = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
REQUEST_4 = UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
REQUEST_5 = UUID("77777777-7777-4777-8777-777777777777")
ACTIVATION_1 = UUID("22222222-2222-4222-8222-222222222222")
ACTIVATION_2 = UUID("33333333-3333-4333-8333-333333333333")
DELIVERY_1 = UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
ROOT_KEY = "a" * 64
RESERVED_CANDIDATE = UUID("55555555-5555-4555-8555-555555555555")
RESERVED_CANDIDATE_2 = UUID("66666666-6666-4666-8666-666666666666")
PREVIEW_NOW_MS = 1_752_400_000_123
CONFIRMATION_TOKEN = base64.urlsafe_b64encode(bytes(range(32))).rstrip(b"=").decode("ascii")
CONFIRMATION_PROOF = hashlib.sha256(CONFIRMATION_TOKEN.encode("utf-8")).hexdigest()
LOOKUP_NOW_MS = PREVIEW_NOW_MS + 1_000
CONSUME_NOW_MS = PREVIEW_NOW_MS + 2_000
_DEFAULT_ISSUED = object()
_DEFAULT_LOOKUP = object()
_DEFAULT_CONSUME = object()


def _fixed_confirmation_token_factory(size: int) -> str:
    assert size == 32
    return CONFIRMATION_TOKEN


class _Coordinator:
    def __init__(
        self,
        runtime_snapshot: RuntimeSnapshot,
        trace: list[object],
        *,
        handshakes: tuple[SessionActivation | BaseException, ...] = (),
        read_request_ids: tuple[UUID, ...] = (),
        mutation_request_ids: tuple[UUID, ...] = (),
        mutation_command_factory: Callable[
            [SessionActivation, ResolvedInvocation, float], ExchangeCommand
        ]
        | None = None,
        root_key: object = ROOT_KEY,
        reserved_candidates: tuple[object, ...] = (),
    ) -> None:
        self._runtime_snapshot = runtime_snapshot
        self._trace = trace
        self._handshakes = list(handshakes)
        self._read_request_ids = list(read_request_ids)
        self._mutation_request_ids = list(mutation_request_ids)
        self._mutation_command_factory = mutation_command_factory
        self._root_key = root_key
        self._reserved_candidates = list(reserved_candidates)
        self.reserve_calls = 0
        self.handshake_calls: list[tuple[UUID | None, UUID | None]] = []
        self.handshake_channels: list[object] = []
        self.prepare_calls: list[tuple[SessionActivation, ResolvedInvocation, float]] = []
        self.read_calls: list[tuple[ResolvedInvocation, float]] = []
        self.read_channels: list[object] = []
        self.reprepare_calls: list[tuple[PreparedReadAttempt, ResponseArtifact]] = []
        self.reprepare_channels: list[object] = []

    @property
    def runtime_snapshot(self) -> RuntimeSnapshot:
        return self._runtime_snapshot

    @property
    def root_key(self) -> Sha256:
        return cast(Sha256, self._root_key)

    def reserve_activation_candidate(self) -> UUID:
        self.reserve_calls += 1
        self._trace.append("reserve")
        if not self._reserved_candidates:
            raise AssertionError("unexpected activation-candidate reservation")
        candidate = self._reserved_candidates.pop(0)
        if isinstance(candidate, BaseException):
            raise candidate
        return cast(UUID, candidate)

    def set_runtime_snapshot_for_test(self, snapshot: RuntimeSnapshot) -> None:
        self._runtime_snapshot = snapshot

    def set_root_key_for_test(self, root_key: object) -> None:
        self._root_key = root_key

    async def handshake(
        self,
        channel: object,
        *,
        accept_lineage_id: UUID | None = None,
        reserved_activation_candidate: UUID | None = None,
    ) -> SessionActivation:
        self.handshake_channels.append(channel)
        self.handshake_calls.append((accept_lineage_id, reserved_activation_candidate))
        return self._next_handshake()

    def prepare_exchange(
        self,
        activation: SessionActivation,
        invocation: ResolvedInvocation,
        *,
        timeout_seconds: float,
    ) -> ExchangeCommand:
        self.prepare_calls.append((activation, invocation, timeout_seconds))
        if self._mutation_command_factory is not None:
            return self._mutation_command_factory(
                activation,
                invocation,
                timeout_seconds,
            )
        if not self._mutation_request_ids:
            raise AssertionError("unexpected exchange preparation")
        return _exchange_command(
            invocation,
            self._runtime_snapshot,
            activation,
            self._mutation_request_ids.pop(0),
            timeout_seconds,
        )

    async def prepare_read_attempt(
        self,
        channel: object,
        invocation: ResolvedInvocation,
        *,
        timeout_seconds: float,
    ) -> PreparedReadAttempt:
        self.read_channels.append(channel)
        self.read_calls.append((invocation, timeout_seconds))
        activation = self._next_handshake()
        if not self._read_request_ids:
            raise AssertionError("unexpected read request preparation")
        return PreparedReadAttempt(
            activation=activation,
            command=_exchange_command(
                invocation,
                self._runtime_snapshot,
                activation,
                self._read_request_ids.pop(0),
                timeout_seconds,
            ),
            rehandshakes_used=0,
        )

    async def reprepare_read_after_activation_mismatch(
        self,
        channel: object,
        prior: PreparedReadAttempt,
        response: ResponseArtifact,
    ) -> PreparedReadAttempt:
        self.reprepare_channels.append(channel)
        self.reprepare_calls.append((prior, response))
        activation = self._next_handshake()
        if not self._read_request_ids:
            raise AssertionError("unexpected read re-preparation")
        return PreparedReadAttempt(
            activation=activation,
            command=_exchange_command(
                prior.command.invocation,
                self._runtime_snapshot,
                activation,
                self._read_request_ids.pop(0),
                prior.command.timeout,
            ),
            rehandshakes_used=1,
        )

    def _next_handshake(self) -> SessionActivation:
        self._trace.append("status")
        if not self._handshakes:
            raise AssertionError("unexpected handshake")
        candidate = self._handshakes.pop(0)
        if isinstance(candidate, BaseException):
            raise candidate
        if type(candidate) is not SessionActivation:
            raise AssertionError("test coordinator requires an exact activation")
        return candidate


class _Channel:
    def __init__(
        self,
        trace: list[object],
        responses: tuple[Callable[[ExchangeCommand], ResponseArtifact] | BaseException, ...],
    ) -> None:
        self._trace = trace
        self._responses = list(responses)
        self.commands: list[ExchangeCommand] = []

    async def exchange(self, command: ExchangeCommand) -> ResponseArtifact:
        self.commands.append(command)
        self._trace.append(command.invocation.effective_class.value)
        if not self._responses:
            raise AssertionError("unexpected domain exchange")
        response = self._responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        artifact = response(command)
        if type(artifact) is not ResponseArtifact:
            raise AssertionError("test channel requires an exact response artifact")
        return artifact


class _SessionContext:
    def __init__(self, trace: list[object], channel: _Channel) -> None:
        self._trace = trace
        self._channel = channel

    async def __aenter__(self) -> _Channel:
        self._trace.append("open")
        return self._channel

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc, traceback
        self._trace.append("close")


class _Transport:
    def __init__(
        self,
        trace: list[object],
        responses: tuple[Callable[[ExchangeCommand], ResponseArtifact] | BaseException, ...] = (),
        *,
        root_key: object = ROOT_KEY,
    ) -> None:
        self._trace = trace
        self._channel = _Channel(trace, responses)
        self._root_key = root_key

    @property
    def root_key(self) -> Sha256:
        return cast(Sha256, self._root_key)

    @property
    def channel(self) -> _Channel:
        return self._channel

    def set_root_key_for_test(self, root_key: object) -> None:
        self._root_key = root_key

    def session(self) -> _SessionContext:
        return _SessionContext(self._trace, self._channel)


class _SessionOpenFailureTransport(_Transport):
    def __init__(self, trace: list[object], error: BaseException) -> None:
        super().__init__(trace)
        self._error = error

    def session(self) -> _SessionContext:
        self._trace.append("open_error")
        raise self._error


class _BlockingDomainChannel(_Channel):
    def __init__(
        self,
        trace: list[object],
        *,
        block_operation: str,
        responses: tuple[Callable[[ExchangeCommand], ResponseArtifact] | BaseException, ...] = (),
    ) -> None:
        super().__init__(trace, responses)
        self._block_operation = block_operation
        self.started = asyncio.Event()
        self._never_release = asyncio.Event()

    async def exchange(self, command: ExchangeCommand) -> ResponseArtifact:
        if command.invocation.contract.name != self._block_operation:
            return await super().exchange(command)
        self.commands.append(command)
        self._trace.append(command.invocation.effective_class.value)
        self.started.set()
        await self._never_release.wait()
        raise AssertionError("blocked domain exchange was unexpectedly released")


class _BlockingDomainTransport(_Transport):
    def __init__(
        self,
        trace: list[object],
        *,
        block_operation: str,
        responses: tuple[Callable[[ExchangeCommand], ResponseArtifact] | BaseException, ...] = (),
    ) -> None:
        super().__init__(trace)
        self._channel = _BlockingDomainChannel(
            trace,
            block_operation=block_operation,
            responses=responses,
        )

    @property
    def channel(self) -> _BlockingDomainChannel:
        return self._channel


class _DelegatingConfirmationStore:
    def __init__(self, delegate: ConfirmationTokenStore) -> None:
        self._delegate = delegate
        self.lookup_calls: list[tuple[str, int]] = []
        self.consume_calls: list[tuple[str, ConfirmationBinding, int]] = []

    def issue(self, binding: ConfirmationBinding, *, now_ms: int) -> IssuedConfirmation:
        return self._delegate.issue(binding, now_ms=now_ms)

    def lookup_active(self, token: str, *, now_ms: int) -> ConfirmationBinding:
        self.lookup_calls.append((token, now_ms))
        return self._delegate.lookup_active(token, now_ms=now_ms)

    def consume(
        self,
        token: str,
        binding: ConfirmationBinding,
        *,
        now_ms: int,
    ) -> Sha256:
        self.consume_calls.append((token, binding, now_ms))
        return self._delegate.consume(token, binding, now_ms=now_ms)


class _BarrierConfirmationStore(_DelegatingConfirmationStore):
    def __init__(
        self,
        delegate: ConfirmationTokenStore,
        barrier: Barrier,
    ) -> None:
        super().__init__(delegate)
        self._barrier = barrier

    def consume(
        self,
        token: str,
        binding: ConfirmationBinding,
        *,
        now_ms: int,
    ) -> Sha256:
        self.consume_calls.append((token, binding, now_ms))
        self._barrier.wait(timeout=5)
        return self._delegate.consume(token, binding, now_ms=now_ms)


class _SerializedDomainChannel:
    def __init__(self, trace: list[object], *, hold_target: bool) -> None:
        self._trace = trace
        self._hold_target = hold_target
        self.target_started = asyncio.Event()
        self.release_target = asyncio.Event()
        self.commands: list[ExchangeCommand] = []

    async def exchange(self, command: ExchangeCommand) -> ResponseArtifact:
        self.commands.append(command)
        self._trace.append(command.invocation.effective_class.value)
        operation = command.invocation.contract.name
        if operation == "unit.get":
            if self._hold_target:
                self.target_started.set()
                await self.release_target.wait()
            return _unit_target_artifact(command)
        if operation == "unit.delete":
            return _unit_delete_artifact(command)
        raise AssertionError(f"unexpected serialized domain operation: {operation}")


class _RootLockedSessionContext:
    def __init__(self, transport: _RootLockedTestTransport) -> None:
        self._transport = transport

    async def __aenter__(self) -> _SerializedDomainChannel:
        self._transport.attempted.set()
        await self._transport.root_lock.__aenter__()
        self._transport.entered.set()
        return self._transport.channel

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._transport.root_lock.__aexit__(exc_type, exc, traceback)


class _RootLockedTestTransport:
    def __init__(
        self,
        trace: list[object],
        lock_path: Path,
        *,
        hold_target: bool,
    ) -> None:
        self._root_key = ROOT_KEY
        self.root_lock = RootLock(lock_path, timeout_seconds=2)
        self.channel = _SerializedDomainChannel(trace, hold_target=hold_target)
        self.attempted = asyncio.Event()
        self.entered = asyncio.Event()

    @property
    def root_key(self) -> Sha256:
        return self._root_key

    def session(self) -> _RootLockedSessionContext:
        return _RootLockedSessionContext(self)


class _UuidSequence:
    def __init__(self, *values: UUID) -> None:
        self._values = list(values)

    def __call__(self) -> UUID:
        if not self._values:
            raise AssertionError("unexpected UUID allocation")
        return self._values.pop(0)


class _WallClock:
    def __init__(self, *epochs: object, trace: list[object] | None = None) -> None:
        self._epochs = list(epochs)
        self._trace = trace
        self.calls = 0

    def now_ms(self) -> int:
        self.calls += 1
        if self._trace is not None:
            self._trace.append("clock")
        if not self._epochs:
            raise AssertionError("unexpected wall-clock read")
        epoch = self._epochs.pop(0)
        if isinstance(epoch, BaseException):
            raise epoch
        return cast(int, epoch)


class _ConcreteSessionChannel:
    def __init__(
        self,
        trace: list[object],
        process_identity: ProcessInfo,
        responders: tuple[Callable[[ExchangeCommand], ResponseArtifact], ...],
    ) -> None:
        self._trace = trace
        self._process_identity = process_identity
        self._responders = list(responders)
        self.commands: list[ExchangeCommand] = []

    @property
    def process_identity(self) -> ProcessInfo:
        return self._process_identity

    async def exchange(self, command: ExchangeCommand) -> ResponseArtifact:
        self.commands.append(command)
        self._trace.append(
            "status"
            if command.invocation.contract.name == "bridge.status"
            else command.invocation.effective_class.value
        )
        if not self._responders:
            raise AssertionError("unexpected concrete session exchange")
        return self._responders.pop(0)(command)

    async def recover_pending(self) -> RecoveryReport:
        return RecoveryReport(
            disposition=RecoveryDisposition.NO_PENDING,
            request_id=None,
            request_state=None,
            response_cleanup_required=False,
        )


class _ConcreteSessionContext:
    def __init__(self, trace: list[object], channel: _ConcreteSessionChannel) -> None:
        self._trace = trace
        self._channel = channel

    async def __aenter__(self) -> _ConcreteSessionChannel:
        self._trace.append("open")
        return self._channel

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc, traceback
        self._trace.append("close")


class _ConcreteTransport:
    def __init__(
        self,
        trace: list[object],
        channel: _ConcreteSessionChannel,
        *,
        root_key: object = ROOT_KEY,
    ) -> None:
        self._trace = trace
        self.channel = channel
        self._root_key = root_key

    @property
    def root_key(self) -> Sha256:
        return cast(Sha256, self._root_key)

    def session(self) -> _ConcreteSessionContext:
        return _ConcreteSessionContext(self._trace, self.channel)


class _Policy:
    def __init__(
        self,
        trace: list[object],
        errors: tuple[BaseException | None, ...] = (),
        *,
        destructive_errors: tuple[BaseException | None, ...] = (),
    ) -> None:
        self._trace = trace
        self._errors = list(errors)
        self._destructive_errors = list(destructive_errors)
        self.calls: list[tuple[object, object, object]] = []
        self.destructive_calls: list[tuple[object, object, object]] = []

    def ensure_allowed(
        self,
        *,
        status: object,
        invocation: object,
        runtime_snapshot: object,
    ) -> None:
        self._trace.append("policy")
        self.calls.append((status, invocation, runtime_snapshot))
        if self._errors:
            error = self._errors.pop(0)
            if error is not None:
                raise error

    def ensure_destructive_allowed(
        self,
        *,
        status: object,
        contract: object,
        runtime_snapshot: object,
    ) -> None:
        self._trace.append("destructive_policy")
        self.destructive_calls.append((status, contract, runtime_snapshot))
        if self._destructive_errors:
            error = self._destructive_errors.pop(0)
            if error is not None:
                raise error


class _ConfirmationStore:
    def __init__(
        self,
        trace: list[object],
        *,
        issued: object = _DEFAULT_ISSUED,
        issue_error: BaseException | None = None,
        active_binding: object = _DEFAULT_LOOKUP,
        lookup_error: BaseException | None = None,
        consume_result: object = _DEFAULT_CONSUME,
        consume_error: BaseException | None = None,
    ) -> None:
        self._trace = trace
        self._issued = (
            IssuedConfirmation(
                token=CONFIRMATION_TOKEN,
                expires_at_ms=PREVIEW_NOW_MS + 60_000,
            )
            if issued is _DEFAULT_ISSUED
            else issued
        )
        self._issue_error = issue_error
        self._active_binding = active_binding
        self._lookup_error = lookup_error
        self._consume_result = consume_result
        self._consume_error = consume_error
        self.issue_calls: list[tuple[object, object]] = []
        self.lookup_calls: list[tuple[object, object]] = []
        self.consume_calls: list[tuple[object, object, object]] = []

    def issue(self, binding: ConfirmationBinding, *, now_ms: int) -> IssuedConfirmation:
        self._trace.append("issue")
        self.issue_calls.append((binding, now_ms))
        if self._issue_error is not None:
            raise self._issue_error
        return cast(IssuedConfirmation, self._issued)

    def lookup_active(self, token: str, *, now_ms: int) -> ConfirmationBinding:
        self._trace.append("lookup")
        self.lookup_calls.append((token, now_ms))
        if self._lookup_error is not None:
            raise self._lookup_error
        if self._active_binding is _DEFAULT_LOOKUP:
            raise AssertionError("unexpected confirmation lookup")
        return cast(ConfirmationBinding, self._active_binding)

    def consume(
        self,
        token: str,
        binding: ConfirmationBinding,
        *,
        now_ms: int,
    ) -> Sha256:
        self._trace.append("consume")
        self.consume_calls.append((token, binding, now_ms))
        if self._consume_error is not None:
            raise self._consume_error
        if self._consume_result is _DEFAULT_CONSUME:
            raise AssertionError("unexpected confirmation consume")
        return cast(Sha256, self._consume_result)


class _LocalOperations:
    def __init__(
        self,
        trace: list[object],
        results: Mapping[str, object],
        error: BaseException | None = None,
    ) -> None:
        self._trace = trace
        self._results = results
        self._error = error

    async def execute(self, operation: str, arguments: BaseModel) -> object:
        self._trace.append(("local", operation, arguments))
        if self._error is not None:
            raise self._error
        return self._results[operation]


class _CompatibilityProbe:
    def __init__(
        self,
        trace: list[object],
        result: object,
        error: BaseException | None = None,
    ) -> None:
        self._trace = trace
        self._result = result
        self._error = error

    async def execute(self, arguments: CompatProbeArgs) -> object:
        self._trace.append(("probe", arguments))
        if self._error is not None:
            raise self._error
        return self._result


class _AsyncSessionTransport(_Transport):
    async def session(self) -> _SessionContext:  # type: ignore[override]
        return super().session()


class _AsyncPolicy(_Policy):
    async def ensure_allowed(  # type: ignore[override]
        self,
        *,
        status: object,
        invocation: object,
        runtime_snapshot: object,
    ) -> None:
        del status, invocation, runtime_snapshot


class _AsyncDestructivePolicy(_Policy):
    async def ensure_destructive_allowed(  # type: ignore[override]
        self,
        *,
        status: object,
        contract: object,
        runtime_snapshot: object,
    ) -> None:
        del status, contract, runtime_snapshot


class _AsyncReserveCoordinator(_Coordinator):
    async def reserve_activation_candidate(self) -> UUID:  # type: ignore[override]
        return RESERVED_CANDIDATE


class _AsyncConfirmationStore(_ConfirmationStore):
    async def issue(  # type: ignore[override]
        self,
        binding: ConfirmationBinding,
        *,
        now_ms: int,
    ) -> IssuedConfirmation:
        del binding, now_ms
        raise AssertionError("constructor validation called the confirmation store")


class _AsyncConfirmationLookupStore(_ConfirmationStore):
    async def lookup_active(  # type: ignore[override]
        self,
        token: str,
        *,
        now_ms: int,
    ) -> ConfirmationBinding:
        del token, now_ms
        raise AssertionError("constructor validation called the confirmation store")


class _AsyncConfirmationConsumeStore(_ConfirmationStore):
    async def consume(  # type: ignore[override]
        self,
        token: str,
        binding: ConfirmationBinding,
        *,
        now_ms: int,
    ) -> Sha256:
        del token, binding, now_ms
        raise AssertionError("constructor validation called the confirmation store")


class _AsyncWallClock(_WallClock):
    async def now_ms(self) -> int:  # type: ignore[override]
        raise AssertionError("constructor validation called the wall clock")


class _MethodRootCoordinator(_Coordinator):
    def root_key(self) -> Sha256:  # type: ignore[override]
        return cast(Sha256, ROOT_KEY)


class _MethodRootTransport(_Transport):
    def root_key(self) -> Sha256:  # type: ignore[override]
        return cast(Sha256, ROOT_KEY)


class _SyncLocalOperations:
    def execute(self, operation: str, arguments: BaseModel) -> object:
        del operation, arguments
        raise AssertionError("constructor validation called the local port")


class _SyncCompatibilityProbe:
    def execute(self, arguments: CompatProbeArgs) -> object:
        del arguments
        raise AssertionError("constructor validation called the probe port")


class _FloatSubclass(float):
    pass


class _IntSubclass(int):
    pass


class _StringSubclass(str):
    pass


class _ForgedDeleteUnitArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    unit_guid: str


class _ForgedConfirmedDeleteUnitWireArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    unit_guid: str
    confirmation_proof: str


class _CountingMapping(Mapping[str, JsonValue]):
    def __init__(self, values: dict[str, JsonValue], *, copy_error: BaseException | None = None):
        self._values = values
        self._copy_error = copy_error
        self.iterations = 0

    def __getitem__(self, key: str) -> JsonValue:
        return self._values[key]

    def __iter__(self):  # type: ignore[no-untyped-def]
        self.iterations += 1
        if self._copy_error is not None:
            raise self._copy_error
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


class _BlockingLocalOperations:
    def __init__(self, trace: list[object]) -> None:
        self._trace = trace
        self.started = asyncio.Event()
        self._never_release = asyncio.Event()

    async def execute(self, operation: str, arguments: BaseModel) -> object:
        self._trace.append(("local", operation, arguments))
        self.started.set()
        await self._never_release.wait()
        raise AssertionError("blocking local operation was unexpectedly released")


@pytest.fixture
def runtime_snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=OPERATION_REGISTRY.manifest_sha256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )


def _prepare_result(snapshot: RuntimeSnapshot) -> PrepareResult:
    return PrepareResult(
        managed_asset_version=snapshot.runtime_version,
        managed_asset_sha256="e" * 64,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
        install_command="ScenEdit_RunScript('CMOAgentBridge.lua')",
        inbox_path=r"D:\Games\CMO\Lua\CMOAgentBridge\inbox",
        prepared=True,
        scenario_installed=True,
    )


def _doctor_result() -> DoctorResult:
    ok = DoctorCheck(state="ok", detail=None)
    return DoctorResult(
        overall_state="ok",
        build_check=ok,
        profile_check=ok,
        asset_check=ok,
        scenario_check=ok,
        process_check=ok,
        warnings=[],
        required_next_action=None,
    )


def _uninstall_result() -> UninstallCommandResult:
    return UninstallCommandResult(
        phase="command",
        command="ScenEdit_RunScript('CMOAgentBridge_Uninstall.lua')",
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=REQUEST_1,
    )


def _probe_result() -> CompatArmResult:
    return CompatArmResult(
        phase="arm-paused-special-action",
        nonce="nonce-1",
        action_name="CMOAgentBridge: Compatibility Probe",
        instructions="Pause the scenario and invoke the action once.",
    )


def _application(
    runtime_snapshot: RuntimeSnapshot,
    trace: list[object],
    *,
    registry: object = OPERATION_REGISTRY,
    transport: object | None = None,
    sessions: object | None = None,
    policy: object | None = None,
    confirmations: object | None = None,
    wall_clock: object | None = None,
    local_operations: object | None = None,
    compatibility_probe: object | None = None,
    request_timeout_seconds: object = 1.25,
) -> BridgeApplication:
    local_results = {
        "bridge.prepare": _prepare_result(runtime_snapshot),
        "bridge.doctor": _doctor_result(),
        "bridge.uninstall": _uninstall_result(),
    }
    dependencies: dict[str, object] = {
        "registry": registry,
        "transport": _Transport(trace) if transport is None else transport,
        "sessions": (_Coordinator(runtime_snapshot, trace) if sessions is None else sessions),
        "policy": _Policy(trace) if policy is None else policy,
        "local_operations": (
            _LocalOperations(trace, local_results) if local_operations is None else local_operations
        ),
        "compatibility_probe": (
            _CompatibilityProbe(trace, _probe_result())
            if compatibility_probe is None
            else compatibility_probe
        ),
        "request_timeout_seconds": request_timeout_seconds,
    }
    constructor_parameters = inspect.signature(BridgeApplication.__init__).parameters
    if "confirmations" in constructor_parameters and "wall_clock" in constructor_parameters:
        dependencies["confirmations"] = (
            _ConfirmationStore(trace) if confirmations is None else confirmations
        )
        dependencies["wall_clock"] = (
            _WallClock(PREVIEW_NOW_MS) if wall_clock is None else wall_clock
        )
    return cast(Any, BridgeApplication)(
        **dependencies,
    )


def _f3_application(
    runtime_snapshot: RuntimeSnapshot,
    trace: list[object],
    **overrides: object,
) -> BridgeApplication:
    dependencies: dict[str, object] = {
        "registry": OPERATION_REGISTRY,
        "transport": _Transport(trace),
        "sessions": _Coordinator(runtime_snapshot, trace),
        "policy": _Policy(trace),
        "confirmations": _ConfirmationStore(trace),
        "wall_clock": _WallClock(PREVIEW_NOW_MS),
        "local_operations": _LocalOperations(
            trace,
            {
                "bridge.prepare": _prepare_result(runtime_snapshot),
                "bridge.doctor": _doctor_result(),
                "bridge.uninstall": _uninstall_result(),
            },
        ),
        "compatibility_probe": _CompatibilityProbe(trace, _probe_result()),
        "request_timeout_seconds": 1.25,
    }
    dependencies.update(overrides)
    return cast(Any, BridgeApplication)(**dependencies)


def _unit_preview_application(
    runtime_snapshot: RuntimeSnapshot,
    trace: list[object],
    *,
    reserved_candidate: object = RESERVED_CANDIDATE,
    activation: SessionActivation | BaseException | None = None,
    handshakes: tuple[SessionActivation | BaseException, ...] | None = None,
    responses: tuple[Callable[[ExchangeCommand], ResponseArtifact] | BaseException, ...]
    | None = None,
    policy: _Policy | None = None,
    confirmations: _ConfirmationStore | None = None,
    wall_clock: _WallClock | None = None,
    transport: _Transport | None = None,
    mutation_command_factory: Callable[
        [SessionActivation, ResolvedInvocation, float], ExchangeCommand
    ]
    | None = None,
    mutation_request_ids: tuple[UUID, ...] = (REQUEST_2,),
) -> tuple[
    BridgeApplication,
    _Coordinator,
    _Transport,
    _Policy,
    _ConfirmationStore,
    _WallClock,
]:
    selected_activation = (
        _activation(
            runtime_snapshot,
            request_id=REQUEST_1,
            activation_id=RESERVED_CANDIDATE,
        )
        if activation is None
        else activation
    )
    selected_handshakes = (selected_activation,) if handshakes is None else handshakes
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=selected_handshakes,
        mutation_request_ids=mutation_request_ids,
        mutation_command_factory=mutation_command_factory,
        reserved_candidates=(reserved_candidate,),
    )
    selected_responses: tuple[
        Callable[[ExchangeCommand], ResponseArtifact] | BaseException, ...
    ] = (_unit_target_artifact,) if responses is None else responses
    selected_transport = (
        _Transport(trace, responses=selected_responses) if transport is None else transport
    )
    selected_policy = _Policy(trace) if policy is None else policy
    selected_confirmations = _ConfirmationStore(trace) if confirmations is None else confirmations
    selected_wall_clock = (
        _WallClock(PREVIEW_NOW_MS, trace=trace) if wall_clock is None else wall_clock
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=selected_transport,
        sessions=coordinator,
        policy=selected_policy,
        confirmations=selected_confirmations,
        wall_clock=selected_wall_clock,
    )
    return (
        application,
        coordinator,
        selected_transport,
        selected_policy,
        selected_confirmations,
        selected_wall_clock,
    )


def _error_payload(
    code: ErrorCode, message: str, details: dict[str, JsonValue]
) -> dict[str, object]:
    return {"code": code.value, "message": message, "details": details}


def _status_result(
    snapshot: RuntimeSnapshot,
    *,
    lineage_id: UUID = LINEAGE_ID,
    activation_id: UUID = ACTIVATION_1,
) -> BridgeStatusResult:
    return BridgeStatusResult(
        protocol=snapshot.protocol,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
        build=1868,
        manifest_sha256=snapshot.operation_manifest_sha256,
        lineage_id=lineage_id,
        activation_id=activation_id,
        installed_event_names=["CMOAgentBridge: Initialize", "CMOAgentBridge: Poll"],
        installed_action_names=["CMOAgentBridge: Initialize", "CMOAgentBridge: Poll"],
        installed_trigger_names=["CMOAgentBridge: Loaded", "CMOAgentBridge: Timer"],
        pending_request_id=None,
        quarantined=False,
        paused_capability=True,
        poll_interval_seconds=5,
        safe_payload_bytes=65_536,
        verified_ledger_entries=128,
        effective_ledger_capacity=128,
    )


def _response_artifact(
    snapshot: RuntimeSnapshot,
    *,
    request_id: UUID,
    lineage_id: UUID,
    activation_id: UUID,
    result: JsonValue | None = None,
    error: ResponseError | None = None,
) -> ResponseArtifact:
    ok = error is None
    envelope = ResponseEnvelope(
        protocol=snapshot.protocol,
        request_id=request_id,
        delivery_id=DELIVERY_1,
        request_hash="a" * 64,
        ok=ok,
        result=result,
        error=error,
        scenario_time="2026-07-12T08:00:00Z",
        scenario_lineage_id=lineage_id,
        activation_id=activation_id,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        bridge_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
    )
    settlement = (
        CompletedSettlement(state="completed", result=result)
        if ok
        else RejectedSettlement(state="rejected", error=error)
    )
    accepted = AcceptedResponse(
        envelope=envelope,
        delivery_kind="request",
        settlement=settlement,
        cancel_ack=None,
    )
    return ResponseArtifact(
        filename=f"CMOAgentBridge_Response_{request_id}.inst",
        sha256=hashlib.sha256(str(request_id).encode("ascii")).hexdigest(),
        size_bytes=512,
        accepted_at_ms=1_752_400_000_000,
        accepted_response=accepted,
    )


def _activation(
    snapshot: RuntimeSnapshot,
    *,
    request_id: UUID = REQUEST_1,
    lineage_id: UUID = LINEAGE_ID,
    activation_id: UUID = ACTIVATION_1,
) -> SessionActivation:
    status = _status_result(
        snapshot,
        lineage_id=lineage_id,
        activation_id=activation_id,
    )
    return SessionActivation(
        status_request_id=request_id,
        status=status,
        response_artifact=_response_artifact(
            snapshot,
            request_id=request_id,
            lineage_id=lineage_id,
            activation_id=activation_id,
            result=cast(JsonValue, status.model_dump(mode="json")),
        ),
    )


def _status_artifact_for_command(
    command: ExchangeCommand,
    snapshot: RuntimeSnapshot,
) -> ResponseArtifact:
    wire = command.invocation.wire_arguments
    if type(wire) is not BridgeStatusWireArgs:
        raise AssertionError("status command lost typed wire arguments")
    status = _status_result(snapshot, activation_id=wire.activation_candidate)
    result = cast(JsonValue, status.model_dump(mode="json"))
    envelope = ResponseEnvelope(
        protocol=snapshot.protocol,
        request_id=command.request_id,
        delivery_id=DELIVERY_1,
        request_hash=request_sha256(command.body),
        ok=True,
        result=result,
        error=None,
        scenario_time="2026-07-12T08:00:00Z",
        scenario_lineage_id=LINEAGE_ID,
        activation_id=wire.activation_candidate,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        bridge_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
    )
    return ResponseArtifact(
        filename=f"CMOAgentBridge_Response_{command.request_id}.inst",
        sha256=hashlib.sha256(str(command.request_id).encode("ascii")).hexdigest(),
        size_bytes=512,
        accepted_at_ms=1_752_400_000_000,
        accepted_response=AcceptedResponse(
            envelope=envelope,
            delivery_kind="request",
            settlement=CompletedSettlement(state="completed", result=result),
            cancel_ack=None,
        ),
    )


def _exchange_command(
    invocation: ResolvedInvocation,
    snapshot: RuntimeSnapshot,
    activation: SessionActivation,
    request_id: UUID,
    timeout_seconds: float,
) -> ExchangeCommand:
    return ExchangeCommand(
        request_id=request_id,
        body=RequestBody(
            protocol=snapshot.protocol,
            release_id=snapshot.release_id,
            runtime_version=snapshot.runtime_version,
            runtime_tag=snapshot.runtime_tag,
            runtime_asset_sha256=snapshot.runtime_asset_sha256,
            expected_lineage_id=activation.status.lineage_id,
            expected_activation_id=activation.status.activation_id,
            operation_manifest_sha256=snapshot.operation_manifest_sha256,
            operation=invocation.contract.name,
            arguments=cast(
                dict[str, JsonValue],
                invocation.wire_arguments.model_dump(mode="json"),
            ),
        ),
        invocation=invocation,
        runtime_snapshot=snapshot,
        timeout=float(timeout_seconds),
    )


def _forge_resolved_invocation(
    original: ResolvedInvocation,
    *,
    contract: OperationContract | None = None,
    wire_arguments: BaseModel | None = None,
    effective_class: OperationClass | None = None,
    public_arguments: BaseModel | None = None,
) -> ResolvedInvocation:
    return ResolvedInvocation._from_resolved_recipe_parts(  # pyright: ignore[reportPrivateUsage]
        contract=original.contract if contract is None else contract,
        wire_arguments=(original.wire_arguments if wire_arguments is None else wire_arguments),
        effective_class=(original.effective_class if effective_class is None else effective_class),
        result_schema=original.result_schema,
        recovery_schema=original.recovery_schema,
        public_result_recipe=object.__getattribute__(original, "_public_result_recipe"),
        public_arguments=(
            original.public_arguments if public_arguments is None else public_arguments
        ),
    )


def _scenario_result() -> ScenarioResult:
    return ScenarioResult(
        guid="scenario-1",
        title="Test Scenario",
        file_name="test.scen",
        file_name_path="C:\\CMO\\Scenarios",
        current_time="2026-07-12T08:00:00Z",
        current_time_seconds=100.0,
        start_time="2026-07-12T07:00:00Z",
        start_time_seconds=0.0,
        duration="01:00:00",
        duration_seconds=3600.0,
        complexity=1,
        difficulty=2,
        setting="Modern",
        database="DB3000",
        save_version="1",
        started=True,
        player_side_guid="SIDE-BLUE",
        time_compression=1.0,
        campaign_score=10,
    )


def _unit_set_result() -> UnitSetResult:
    return UnitSetResult(
        unit_guid="unit-1",
        name="Unit One",
        speed=350.0,
        altitude=1000.0,
        heading=90.0,
        course=None,
    )


def _unit_result(*, guid: str = "UNIT-1") -> UnitResult:
    return UnitResult(
        guid=guid,
        dbid=1,
        name="Alpha",
        side_name="Blue",
        type="Aircraft",
        subtype=None,
        category="Air",
        class_name="F-16C",
        latitude=1.0,
        longitude=2.0,
        altitude=10_000.0,
        speed=350.0,
        heading=90.0,
        throttle="Cruise",
        proficiency="Regular",
        fuel_state="Bingo",
        weapon_state="Winchester",
        unit_state="OnPatrol",
        operating=True,
        mission_guid=None,
        mission_name=None,
        loadout_dbid=None,
    )


def _mission_result(*, guid: str = "MISSION-1") -> MissionResult:
    return MissionResult(
        guid=guid,
        name="CAP North",
        side_name="Blue",
        mission_class="patrol",
        mission_class_string="Patrol",
        active=True,
        start_time=None,
        end_time=None,
        assigned_unit_guids=[],
        target_guids=[],
        patrol_type=None,
        strike_type=None,
        reference_point_guids=None,
        destination_guid=None,
        flight_size=None,
        one_third_rule=None,
    )


def _delete_result(
    *,
    guid: str = "UNIT-1",
    name: str = "Alpha at deletion",
    object_kind: str = "unit",
) -> DeleteResult:
    return DeleteResult(
        deleted_guid=guid,
        deleted_name=name,
        object_kind=cast(Any, object_kind),
    )


def _destructive_descriptor(
    snapshot: RuntimeSnapshot,
    *,
    operation: str,
    arguments: dict[str, JsonValue],
    target: DestructiveTarget,
    root_key: str = ROOT_KEY,
    lineage_id: UUID = LINEAGE_ID,
    reserved_candidate: UUID = RESERVED_CANDIDATE,
    release_id: str | None = None,
) -> DestructiveConfirmationDescriptor:
    return DestructiveConfirmationDescriptor(
        format=DESTRUCTIVE_CONFIRMATION_FORMAT,
        root_key=root_key,
        operation=cast(Any, operation),
        public_arguments=arguments,
        resolved_target=target,
        scenario_lineage_id=lineage_id,
        reserved_activation_id=reserved_candidate,
        release_id=snapshot.release_id if release_id is None else release_id,
    )


def _confirmation_application(
    runtime_snapshot: RuntimeSnapshot,
    trace: list[object],
    *,
    operation: str = "unit.delete",
    active_binding: object = _DEFAULT_LOOKUP,
    lookup_error: BaseException | None = None,
    consume_result: object = _DEFAULT_CONSUME,
    consume_error: BaseException | None = None,
    activation: SessionActivation | BaseException | None = None,
    handshakes: tuple[SessionActivation | BaseException, ...] | None = None,
    responses: tuple[Callable[[ExchangeCommand], ResponseArtifact] | BaseException, ...]
    | None = None,
    policy: _Policy | None = None,
    wall_clock: _WallClock | None = None,
    transport: _Transport | None = None,
    mutation_command_factory: Callable[
        [SessionActivation, ResolvedInvocation, float], ExchangeCommand
    ]
    | None = None,
    mutation_request_ids: tuple[UUID, ...] = (REQUEST_2, REQUEST_3),
) -> tuple[
    BridgeApplication,
    _Coordinator,
    _Transport,
    _Policy,
    _ConfirmationStore,
    _WallClock,
    dict[str, JsonValue],
]:
    if operation == "unit.delete":
        arguments: dict[str, JsonValue] = {"unit_guid": "UNIT-1"}
        target = DestructiveTarget(guid="UNIT-1", name="Alpha", type="Aircraft")
        default_responses: tuple[
            Callable[[ExchangeCommand], ResponseArtifact] | BaseException, ...
        ] = (_unit_target_artifact, _unit_delete_artifact)
    elif operation == "mission.delete":
        arguments = {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"}
        target = DestructiveTarget(guid="MISSION-1", name="CAP North", type="patrol")
        default_responses = (_mission_target_artifact, _mission_delete_artifact)
    else:
        raise AssertionError(f"unsupported confirmation helper operation: {operation}")
    selected_binding = (
        destructive_confirmation_binding(
            _destructive_descriptor(
                runtime_snapshot,
                operation=operation,
                arguments=arguments,
                target=target,
            )
        )
        if active_binding is _DEFAULT_LOOKUP
        else active_binding
    )
    selected_consume_result = (
        CONFIRMATION_PROOF if consume_result is _DEFAULT_CONSUME else consume_result
    )
    confirmations = _ConfirmationStore(
        trace,
        active_binding=selected_binding,
        lookup_error=lookup_error,
        consume_result=selected_consume_result,
        consume_error=consume_error,
    )
    selected_activation = (
        _activation(
            runtime_snapshot,
            request_id=REQUEST_1,
            activation_id=RESERVED_CANDIDATE,
        )
        if activation is None
        else activation
    )
    selected_handshakes = (selected_activation,) if handshakes is None else handshakes
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=selected_handshakes,
        mutation_request_ids=mutation_request_ids,
        mutation_command_factory=mutation_command_factory,
    )
    selected_transport = (
        _Transport(trace, responses=default_responses if responses is None else responses)
        if transport is None
        else transport
    )
    selected_policy = _Policy(trace) if policy is None else policy
    selected_wall_clock = (
        _WallClock(LOOKUP_NOW_MS, CONSUME_NOW_MS, trace=trace) if wall_clock is None else wall_clock
    )
    application = _application(
        runtime_snapshot,
        trace,
        sessions=coordinator,
        transport=selected_transport,
        policy=selected_policy,
        confirmations=confirmations,
        wall_clock=selected_wall_clock,
    )
    return (
        application,
        coordinator,
        selected_transport,
        selected_policy,
        confirmations,
        selected_wall_clock,
        arguments,
    )


def _destructive_preview_result(
    *,
    operation: str,
    target: DestructiveTarget,
    impact: str,
) -> dict[str, JsonValue]:
    preview = DestructivePreviewResult(
        operation=cast(Any, operation),
        target_guid=target.guid,
        target_name=target.name,
        target_type=target.type,
        impact=impact,
        reserved_activation_candidate=RESERVED_CANDIDATE,
        confirmation_token=CONFIRMATION_TOKEN,
        expires_at_utc=datetime.fromtimestamp(
            (PREVIEW_NOW_MS + 60_000) / 1000,
            tz=UTC,
        ),
    )
    return cast(dict[str, JsonValue], preview.model_dump(mode="json"))


def _domain_artifact(
    command: ExchangeCommand,
    *,
    result: JsonValue | None = None,
    code: ErrorCode | None = None,
    message: str | None = None,
    details: dict[str, JsonValue] | None = None,
    request_id: UUID | None = None,
    request_hash: str | None = None,
    lineage_id: UUID | None = None,
    activation_id: UUID | None = None,
    delivery_kind: str = "request",
    rejected_settlement: bool = True,
    mutation_not_started: bool = False,
    runtime_tag: str | None = None,
    reported_snapshot: RuntimeSnapshot | None = None,
) -> ResponseArtifact:
    snapshot = command.runtime_snapshot
    reported = reported_snapshot or snapshot
    correlated_request_id = request_id or command.request_id
    correlated_request_hash = request_hash or request_sha256(command.body)
    correlated_lineage = lineage_id or command.body.expected_lineage_id
    correlated_activation = activation_id or command.body.expected_activation_id
    if correlated_lineage is None or correlated_activation is None:
        raise AssertionError("domain artifact requires a contextual command")
    response_error: ResponseError | None = None
    if code is not None:
        evidence = None
        if mutation_not_started:
            evidence = MutationNotStartedEvidence(
                schema_version=1,
                stage="dispatch_validation",
                request_id=correlated_request_id,
                request_hash=correlated_request_hash,
                operation=command.body.operation,
                mutation_barrier_written=False,
                execute_started=False,
            )
        response_error = ResponseError(
            code=code,
            message=message or f"operation failed with {code.value}",
            details=details or {"reported_by": "test-peer"},
            mutation_not_started=evidence,
        )
    envelope = ResponseEnvelope(
        protocol=reported.protocol,
        request_id=correlated_request_id,
        delivery_id=DELIVERY_1,
        request_hash=correlated_request_hash,
        ok=response_error is None,
        result=result,
        error=response_error,
        scenario_time="2026-07-12T08:00:01Z",
        scenario_lineage_id=correlated_lineage,
        activation_id=correlated_activation,
        operation_manifest_sha256=reported.operation_manifest_sha256,
        bridge_version=reported.runtime_version,
        runtime_tag=runtime_tag or reported.runtime_tag,
        runtime_asset_sha256=reported.runtime_asset_sha256,
        release_id=reported.release_id,
    )
    if response_error is None:
        settlement = CompletedSettlement(
            state="completed",
            result=result,
        )
    elif rejected_settlement:
        settlement = RejectedSettlement(state="rejected", error=response_error)
    else:
        settlement = None
    accepted = AcceptedResponse(
        envelope=envelope,
        delivery_kind=cast(object, delivery_kind),  # type: ignore[arg-type]
        settlement=settlement,
        cancel_ack=None,
    )
    return ResponseArtifact(
        filename=f"CMOAgentBridge_Response_{correlated_request_id}.inst",
        sha256=hashlib.sha256(
            f"{correlated_request_id}:{correlated_request_hash}".encode("ascii")
        ).hexdigest(),
        size_bytes=384,
        accepted_at_ms=1_752_400_001_000,
        accepted_response=accepted,
    )


def _unit_target_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        result=cast(JsonValue, _unit_result().model_dump(mode="json")),
    )


def _target_not_found_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(command, code=ErrorCode.NOT_FOUND)


def _target_activation_mismatch_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(command, code=ErrorCode.ACTIVATION_MISMATCH)


def _target_correlated_malformed_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.NOT_FOUND,
        rejected_settlement=False,
    )


def _target_token_echo_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.NOT_FOUND,
        message=f"target missing token={CONFIRMATION_TOKEN}",
        details={
            f"key-{CONFIRMATION_TOKEN}": [
                {"nested": f"value-{CONFIRMATION_TOKEN}"},
            ]
        },
    )


def _foreign_target_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        result=cast(JsonValue, _unit_result().model_dump(mode="json")),
        request_id=REQUEST_5,
    )


def _foreign_unit_target_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        result=cast(JsonValue, _unit_result().model_dump(mode="json")),
        request_id=REQUEST_3,
    )


def _malformed_unit_target_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(command, result={})


def _wrong_guid_unit_target_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        result=cast(
            JsonValue,
            _unit_result(guid="UNIT-2").model_dump(mode="json"),
        ),
    )


def _wrong_type_unit_target_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        result=cast(JsonValue, _mission_result().model_dump(mode="json")),
    )


def _unit_delete_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        result=cast(JsonValue, _delete_result().model_dump(mode="json")),
    )


def _mission_target_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        result=cast(JsonValue, _mission_result().model_dump(mode="json")),
    )


def _mission_delete_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        result=cast(
            JsonValue,
            _delete_result(
                guid="MISSION-1",
                name="CAP North at deletion",
                object_kind="mission",
            ).model_dump(mode="json"),
        ),
    )


def _wrong_guid_delete_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        result=cast(
            JsonValue,
            _delete_result(guid="UNIT-2").model_dump(mode="json"),
        ),
    )


def _wrong_kind_delete_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        result=cast(
            JsonValue,
            _delete_result(object_kind="mission").model_dump(mode="json"),
        ),
    )


def _malformed_delete_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(command, result={})


def _foreign_delete_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        result=cast(JsonValue, _delete_result().model_dump(mode="json")),
        request_id=REQUEST_5,
    )


def _destructive_error_with_evidence(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.CMO_LUA_ERROR,
        message=f"engine echoed {CONFIRMATION_TOKEN} {CONFIRMATION_PROOF}",
        details={
            "token": CONFIRMATION_TOKEN,
            "nested": {"proof": CONFIRMATION_PROOF},
        },
        mutation_not_started=True,
    )


def _destructive_error_without_evidence(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.CMO_LUA_ERROR,
        message=f"engine echoed {CONFIRMATION_TOKEN} {CONFIRMATION_PROOF}",
        details={"secret": [CONFIRMATION_TOKEN, CONFIRMATION_PROOF]},
        rejected_settlement=False,
    )


def _destructive_error_invalid_settlement(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.CMO_LUA_ERROR,
        message=f"engine echoed {CONFIRMATION_TOKEN}",
        details={"proof": CONFIRMATION_PROOF},
    )


def _destructive_activation_mismatch(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.ACTIVATION_MISMATCH,
        rejected_settlement=False,
    )


def _destructive_indeterminate(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.INDETERMINATE_OUTCOME,
        rejected_settlement=False,
    )


def _destructive_ordinary_error(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.NOT_FOUND,
        rejected_settlement=False,
    )


def _destructive_top_level_echo(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.CMO_LUA_ERROR,
        message="engine failure",
        details={
            "confirmation_token": CONFIRMATION_TOKEN,
            "confirmation_proof": CONFIRMATION_PROOF,
        },
        rejected_settlement=False,
    )


def _destructive_nested_object_echo(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.CMO_LUA_ERROR,
        message="engine failure",
        details={
            "nested": {
                "token": CONFIRMATION_TOKEN,
                "proof": CONFIRMATION_PROOF,
                "wire": {"confirmation_proof": CONFIRMATION_PROOF},
            }
        },
        rejected_settlement=False,
    )


def _destructive_nested_array_echo(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.CMO_LUA_ERROR,
        message="engine failure",
        details={
            "items": [
                CONFIRMATION_TOKEN,
                {"proof": CONFIRMATION_PROOF},
            ]
        },
        rejected_settlement=False,
    )


def _destructive_message_echo(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.CMO_LUA_ERROR,
        message=(
            f"engine echoed token={CONFIRMATION_TOKEN} proof={CONFIRMATION_PROOF} "
            "confirmation_proof"
        ),
        details={"safe": "not forwarded"},
        rejected_settlement=False,
    )


def _constructed_invalid_artifact(command: ExchangeCommand) -> ResponseArtifact:
    valid = _domain_artifact(
        command,
        result=cast(JsonValue, _scenario_result().model_dump(mode="json")),
    )
    return ResponseArtifact.model_construct(
        filename="foreign-name.inst",
        sha256=valid.sha256,
        size_bytes=valid.size_bytes,
        accepted_at_ms=valid.accepted_at_ms,
        accepted_response=valid.accepted_response,
    )


def _cross_command_success_artifact(command: ExchangeCommand) -> ResponseArtifact:
    other = replace(command, request_id=REQUEST_2)
    return _domain_artifact(
        other,
        result=cast(JsonValue, _scenario_result().model_dump(mode="json")),
    )


def _mismatch_foreign_request(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.ACTIVATION_MISMATCH,
        request_id=REQUEST_2,
    )


def _mismatch_foreign_hash(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.ACTIVATION_MISMATCH,
        request_hash="f" * 64,
    )


def _mismatch_foreign_lineage(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.ACTIVATION_MISMATCH,
        lineage_id=LINEAGE_2,
    )


def _mismatch_foreign_activation(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.ACTIVATION_MISMATCH,
        activation_id=ACTIVATION_2,
    )


def _mismatch_foreign_runtime_tag(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.ACTIVATION_MISMATCH,
        runtime_tag="0_1_0-" + "f" * 64,
    )


def _mismatch_cancel_delivery(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.ACTIVATION_MISMATCH,
        delivery_kind="cancel",
        rejected_settlement=False,
    )


def _mismatch_without_settlement(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.ACTIVATION_MISMATCH,
        rejected_settlement=False,
    )


def _mismatch_with_mutation_evidence(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.ACTIVATION_MISMATCH,
        mutation_not_started=True,
    )


def _foreign_runtime_success(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        result=cast(JsonValue, _scenario_result().model_dump(mode="json")),
        reported_snapshot=_foreign_snapshot(),
    )


def _foreign_ordinary_error(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.NOT_FOUND,
        request_hash="f" * 64,
    )


def _scenario_changed_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.SCENARIO_CHANGED,
        lineage_id=LINEAGE_2,
    )


def _manifest_mismatch_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _domain_artifact(
        command,
        code=ErrorCode.MANIFEST_MISMATCH,
        rejected_settlement=False,
        reported_snapshot=_foreign_snapshot(manifest_sha256="f" * 64),
    )


def _snapshot_for_registry(registry: OperationRegistry) -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=registry.manifest_sha256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )


def _foreign_snapshot(
    *, manifest_sha256: str = OPERATION_REGISTRY.manifest_sha256
) -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="9.8.7",
        runtime_asset_sha256="e" * 64,
        operation_manifest_sha256=manifest_sha256,
        host_contract_sha256="f" * 64,
        dependency_lock_sha256="1" * 64,
    )


def _forged_mutation_command_factory(
    kind: str,
    snapshot: RuntimeSnapshot,
) -> Callable[[SessionActivation, ResolvedInvocation, float], ExchangeCommand]:
    def factory(
        activation: SessionActivation,
        invocation: ResolvedInvocation,
        timeout_seconds: float,
    ) -> ExchangeCommand:
        command = _exchange_command(
            invocation,
            snapshot,
            activation,
            REQUEST_1,
            timeout_seconds,
        )
        if kind == "request_uuid_version":
            return replace(
                command,
                request_id=UUID("aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa"),
            )
        if kind == "request_id_reuse":
            return replace(command, request_id=activation.status_request_id)
        if kind == "activation_id_reuse":
            return replace(command, request_id=activation.status.activation_id)
        if kind == "invocation":
            return replace(
                command,
                invocation=OPERATION_REGISTRY.resolve_invocation("scenario.get", {}),
            )
        if kind == "operation":
            return replace(
                command,
                body=command.body.model_copy(update={"operation": "scenario.get"}),
            )
        if kind == "arguments":
            return replace(
                command,
                body=command.body.model_copy(update={"arguments": {"unit_guid": "foreign"}}),
            )
        if kind == "activation":
            return replace(
                command,
                body=command.body.model_copy(update={"expected_activation_id": ACTIVATION_2}),
            )
        if kind == "snapshot":
            foreign = _foreign_snapshot()
            body = RequestBody(
                protocol=foreign.protocol,
                release_id=foreign.release_id,
                runtime_version=foreign.runtime_version,
                runtime_tag=foreign.runtime_tag,
                runtime_asset_sha256=foreign.runtime_asset_sha256,
                expected_lineage_id=activation.status.lineage_id,
                expected_activation_id=activation.status.activation_id,
                operation_manifest_sha256=foreign.operation_manifest_sha256,
                operation=invocation.contract.name,
                arguments=cast(
                    dict[str, JsonValue],
                    invocation.wire_arguments.model_dump(mode="json"),
                ),
            )
            return ExchangeCommand(
                request_id=REQUEST_1,
                body=body,
                invocation=invocation,
                runtime_snapshot=foreign,
                timeout=timeout_seconds,
            )
        if kind == "timeout":
            return replace(command, timeout=math.nextafter(timeout_seconds, math.inf))
        if kind == "constructed_body":
            body_values = command.body.model_dump(mode="python")
            body_values["operation"] = 123
            body = RequestBody.model_construct(**body_values)
            return replace(command, body=body)
        raise AssertionError(f"unknown forged command kind {kind}")

    return factory


def _registry_for_closed_class(operation_class: str) -> OperationRegistry:
    entries = deepcopy(OPERATIONS)
    if operation_class == "destructive":
        entry = next(item for item in entries if item["name"] == "unit.delete")
        entry["wire_arguments_model"] = "DeleteUnitArgs"
        entry["trusted_fields"] = []
    elif operation_class == "reconcile":
        entry = next(item for item in entries if item["name"] == "bridge.reconcile")
        entry["public_arguments_model"] = "ReconcileCommitWireArgs"
        entry["wire_resolver"] = "model"
        entry["trusted_fields"] = []
    elif operation_class == "dynamic":
        entry = next(item for item in entries if item["name"] == "lua.call")
        resolver_data = cast(dict[str, dict[str, object]], entry["resolver_data"])
        resolver_data["ScenEdit_GetScore"]["class"] = "dynamic"
    else:
        raise AssertionError(f"unknown closed class {operation_class}")
    return OperationRegistry(entries)


def _registry_with_unrecognized_local() -> OperationRegistry:
    entries = deepcopy(OPERATIONS)
    next(item for item in entries if item["name"] == "scenario.get")["target"] = "local"
    next(item for item in entries if item["name"] == "bridge.prepare")["target"] = "cmo"
    return OperationRegistry(entries)


def _registry_with_scenario_doctor_result() -> OperationRegistry:
    entries = deepcopy(OPERATIONS)
    entry = next(item for item in entries if item["name"] == "bridge.doctor")
    entry["wire_result_factory"] = "ScenarioResult"
    entry["public_result_factory"] = "ScenarioResult"
    return OperationRegistry(entries)


def test_bridge_application_is_exported() -> None:
    import cmo_agent_bridge.application as application

    assert application.BridgeApplication.__name__ == "BridgeApplication"


def test_f3_constructor_declares_exact_destructive_dependency_surface() -> None:
    parameters = tuple(inspect.signature(BridgeApplication.__init__).parameters)

    assert parameters == (
        "self",
        "registry",
        "transport",
        "sessions",
        "policy",
        "confirmations",
        "wall_clock",
        "local_operations",
        "compatibility_probe",
        "request_timeout_seconds",
    )


def test_f3_constructor_accepts_new_dependencies_without_performing_io(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(runtime_snapshot, trace)
    confirmations = _ConfirmationStore(trace)
    wall_clock = _WallClock(PREVIEW_NOW_MS, trace=trace)

    application = _f3_application(
        runtime_snapshot,
        trace,
        sessions=coordinator,
        confirmations=confirmations,
        wall_clock=wall_clock,
    )

    assert type(application) is BridgeApplication
    assert coordinator.reserve_calls == 0
    assert confirmations.issue_calls == []
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 0
    assert trace == []


@pytest.mark.parametrize(
    "case",
    [
        "sessions-missing-shape",
        "sessions-root-method",
        "sessions-root-uppercase",
        "sessions-root-nonhex",
        "sessions-root-subclass",
        "sessions-reserve-async",
        "transport-root-method",
        "transport-root-uppercase",
        "root-mismatch",
        "confirmations-missing-shape",
        "confirmation-issue-async",
        "confirmation-lookup-async",
        "confirmation-consume-async",
        "clock-missing-shape",
        "clock-async",
        "destructive-policy-async",
    ],
)
def test_f3_constructor_rejects_invalid_new_dependencies_before_io(
    runtime_snapshot: RuntimeSnapshot,
    case: str,
) -> None:
    trace: list[object] = []
    overrides: dict[str, object]
    if case == "sessions-missing-shape":
        overrides = {"sessions": object()}
    elif case == "sessions-root-method":
        overrides = {"sessions": _MethodRootCoordinator(runtime_snapshot, trace)}
    elif case == "sessions-root-uppercase":
        overrides = {"sessions": _Coordinator(runtime_snapshot, trace, root_key="A" * 64)}
    elif case == "sessions-root-nonhex":
        overrides = {"sessions": _Coordinator(runtime_snapshot, trace, root_key="g" * 64)}
    elif case == "sessions-root-subclass":
        overrides = {
            "sessions": _Coordinator(
                runtime_snapshot,
                trace,
                root_key=_StringSubclass(ROOT_KEY),
            )
        }
    elif case == "sessions-reserve-async":
        overrides = {"sessions": _AsyncReserveCoordinator(runtime_snapshot, trace)}
    elif case == "transport-root-method":
        overrides = {"transport": _MethodRootTransport(trace)}
    elif case == "transport-root-uppercase":
        overrides = {"transport": _Transport(trace, root_key="A" * 64)}
    elif case == "root-mismatch":
        overrides = {"transport": _Transport(trace, root_key="b" * 64)}
    elif case == "confirmations-missing-shape":
        overrides = {"confirmations": object()}
    elif case == "confirmation-issue-async":
        overrides = {"confirmations": _AsyncConfirmationStore(trace)}
    elif case == "confirmation-lookup-async":
        overrides = {"confirmations": _AsyncConfirmationLookupStore(trace)}
    elif case == "confirmation-consume-async":
        overrides = {"confirmations": _AsyncConfirmationConsumeStore(trace)}
    elif case == "clock-missing-shape":
        overrides = {"wall_clock": object()}
    elif case == "clock-async":
        overrides = {"wall_clock": _AsyncWallClock(PREVIEW_NOW_MS)}
    elif case == "destructive-policy-async":
        overrides = {"policy": _AsyncDestructivePolicy(trace)}
    else:
        raise AssertionError(f"unhandled constructor case: {case}")

    with pytest.raises(BridgeError) as caught:
        _f3_application(runtime_snapshot, trace, **overrides)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert caught.value.message == "bridge application dependencies are invalid"
    assert caught.value.details == {}
    assert trace == []


def test_constructor_accepts_exact_dependencies_without_performing_io(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []

    application = _application(runtime_snapshot, trace)

    assert application.__class__.__name__ == "BridgeApplication"
    assert trace == []


@pytest.mark.parametrize(
    ("override", "value"),
    [
        ("registry", object()),
        ("sessions", object()),
        ("transport", _AsyncSessionTransport([])),
        ("policy", _AsyncPolicy([])),
        ("local_operations", _SyncLocalOperations()),
        ("compatibility_probe", _SyncCompatibilityProbe()),
        ("request_timeout_seconds", 0),
        ("request_timeout_seconds", -1.0),
        ("request_timeout_seconds", math.inf),
        ("request_timeout_seconds", math.nan),
        ("request_timeout_seconds", True),
        ("request_timeout_seconds", _FloatSubclass(1.0)),
    ],
)
def test_constructor_rejects_invalid_dependencies(
    runtime_snapshot: RuntimeSnapshot,
    override: str,
    value: object,
) -> None:
    dependencies = {override: value}

    with pytest.raises(BridgeError) as caught:
        _application(runtime_snapshot, [], **dependencies)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert caught.value.message == "bridge application dependencies are invalid"
    assert caught.value.details == {}


def test_constructor_revalidates_snapshot_and_requires_manifest_match(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    invalid_snapshot = RuntimeSnapshot.model_construct(
        protocol=runtime_snapshot.protocol,
        runtime_version=runtime_snapshot.runtime_version,
        runtime_asset_sha256=runtime_snapshot.runtime_asset_sha256,
        operation_manifest_sha256=runtime_snapshot.operation_manifest_sha256,
        host_contract_sha256=runtime_snapshot.host_contract_sha256,
        dependency_lock_sha256=runtime_snapshot.dependency_lock_sha256,
        runtime_tag="not-the-derived-runtime-tag",
        release_id=runtime_snapshot.release_id,
    )
    mismatched_snapshot = RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256="f" * 64,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )

    for snapshot in (invalid_snapshot, mismatched_snapshot):
        with pytest.raises(BridgeError) as caught:
            _application(
                runtime_snapshot,
                [],
                sessions=_Coordinator(snapshot, []),
            )
        assert caught.value.code is ErrorCode.INVALID_ARGUMENT
        assert caught.value.message == "bridge application dependencies are invalid"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "arguments", "argument_type", "result"),
    [
        (
            "bridge.prepare",
            {"control_side": "Blue", "rotate_lineage": True},
            BridgePrepareArgs,
            "prepare",
        ),
        ("bridge.doctor", {"live": False}, BridgeDoctorArgs, "doctor"),
        (
            "bridge.uninstall",
            {"phase": "command"},
            BridgeUninstallArgs,
            "uninstall",
        ),
        (
            "compat.probe",
            {"phase": "arm-paused-special-action"},
            CompatProbeArgs,
            "probe",
        ),
    ],
)
async def test_local_routes_receive_typed_arguments_and_return_public_json(
    runtime_snapshot: RuntimeSnapshot,
    operation: str,
    arguments: dict[str, JsonValue],
    argument_type: type[BaseModel],
    result: str,
) -> None:
    trace: list[object] = []
    expected_results: dict[str, BaseModel] = {
        "prepare": _prepare_result(runtime_snapshot),
        "doctor": _doctor_result(),
        "uninstall": _uninstall_result(),
        "probe": _probe_result(),
    }
    application = _application(
        runtime_snapshot,
        trace,
        local_operations=_LocalOperations(
            trace,
            {
                "bridge.prepare": expected_results["prepare"],
                "bridge.doctor": expected_results["doctor"],
                "bridge.uninstall": expected_results["uninstall"],
            },
        ),
        compatibility_probe=_CompatibilityProbe(trace, expected_results["probe"]),
    )

    outcome = await application.execute(operation, arguments)

    assert type(outcome) is InvocationOutcome
    assert outcome == InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=None,
        ok=True,
        result=cast(JsonValue, expected_results[result].model_dump(mode="json")),
        error=None,
    )
    assert len(trace) == 1
    call = cast(tuple[object, ...], trace[0])
    assert call[0] == ("probe" if operation == "compat.probe" else "local")
    typed_arguments = call[-1]
    assert type(typed_arguments) is argument_type
    assert typed_arguments.model_dump(mode="python", exclude_unset=True) == arguments


@pytest.mark.asyncio
async def test_local_bridge_error_becomes_failed_outcome(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    application = _application(
        runtime_snapshot,
        trace,
        local_operations=_LocalOperations(
            trace,
            {},
            BridgeError(ErrorCode.GAME_ROOT_INVALID, "game root is invalid", {"path": "bad"}),
        ),
    )

    outcome = await application.execute("bridge.doctor", {})

    assert outcome == InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=None,
        ok=False,
        result=None,
        error=cast(
            dict[str, JsonValue],
            _error_payload(
                ErrorCode.GAME_ROOT_INVALID,
                "game root is invalid",
                {"path": "bad"},
            ),
        ),
    )
    assert len(trace) == 1
    assert cast(tuple[object, ...], trace[0])[0] == "local"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "malformed",
    [
        {"overall_state": "ok"},
        DoctorResult.model_construct(overall_state="ok"),
    ],
)
async def test_malformed_local_result_fails_public_validation(
    runtime_snapshot: RuntimeSnapshot,
    malformed: object,
) -> None:
    trace: list[object] = []
    application = _application(
        runtime_snapshot,
        trace,
        local_operations=_LocalOperations(trace, {"bridge.doctor": malformed}),
    )

    outcome = await application.execute("bridge.doctor", {})

    assert outcome == InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=None,
        ok=False,
        result=None,
        error=cast(
            dict[str, JsonValue],
            _error_payload(
                ErrorCode.PROTOCOL_ERROR,
                "operation result failed public validation",
                {"operation": "bridge.doctor"},
            ),
        ),
    )
    assert len(trace) == 1
    assert cast(tuple[object, ...], trace[0])[0] == "local"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "arguments"),
    [
        (None, {}),
        ("", {}),
        (_StringSubclass("bridge.doctor"), {}),
        ("bridge.doctor", []),
        ("bridge.doctor", {1: False}),
        ("bridge.doctor", {"unexpected": True}),
        ("bridge.status", {"activation_candidate": str(REQUEST_1)}),
        ("unit.delete", {"unit_guid": "unit-1", "confirmation_proof": "a" * 64}),
    ],
)
async def test_invalid_public_input_fails_before_any_io(
    runtime_snapshot: RuntimeSnapshot,
    operation: object,
    arguments: object,
) -> None:
    trace: list[object] = []
    application = _application(runtime_snapshot, trace)

    outcome = await application.execute(
        cast(str, operation),
        cast(Mapping[str, JsonValue], arguments),
    )

    assert type(outcome) is InvocationOutcome
    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.INVALID_ARGUMENT.value
    assert trace == []


@pytest.mark.asyncio
async def test_public_mapping_is_frozen_once_before_local_io(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    arguments = _CountingMapping({"live": False})
    application = _application(runtime_snapshot, trace)

    outcome = await application.execute("bridge.doctor", arguments)

    assert outcome.ok is True
    assert arguments.iterations == 1
    assert len(trace) == 1
    assert cast(tuple[object, ...], trace[0])[0] == "local"


@pytest.mark.asyncio
async def test_public_mapping_copy_failure_becomes_invalid_argument(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    arguments = _CountingMapping({}, copy_error=ValueError("mapping changed while copying"))
    application = _application(runtime_snapshot, trace)

    outcome = await application.execute("bridge.doctor", arguments)

    assert outcome.ok is False
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.INVALID_ARGUMENT.value
    assert arguments.iterations == 1
    assert trace == []


@pytest.mark.asyncio
async def test_argument_validation_error_has_json_safe_stable_details(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    application = _application(runtime_snapshot, [])

    outcome = await application.execute("bridge.doctor", {"unexpected": True})

    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.INVALID_ARGUMENT.value
    assert outcome.error["message"] == "operation arguments are invalid"
    details = cast(dict[str, JsonValue], outcome.error["details"])
    assert details["operation"] == "bridge.doctor"
    validation_errors = cast(list[dict[str, JsonValue]], details["validation_errors"])
    assert validation_errors
    assert all("url" not in error and "ctx" not in error for error in validation_errors)


@pytest.mark.asyncio
async def test_unknown_operation_preserves_registry_bridge_error(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    application = _application(runtime_snapshot, trace)

    outcome = await application.execute("not.registered", {})

    assert outcome.error == _error_payload(
        ErrorCode.INVALID_ARGUMENT,
        "unknown operation: not.registered",
        {"operation": "not.registered"},
    )
    assert outcome.request_id is None
    assert trace == []


@pytest.mark.asyncio
async def test_internal_probe_step_is_rejected_only_after_argument_validation(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    application = _application(runtime_snapshot, trace)

    invalid = await application.execute("compat.probe.step", {"step": "not-a-step"})
    internal = await application.execute("compat.probe.step", {"step": "observational"})

    assert invalid.error is not None
    assert invalid.error["code"] == ErrorCode.INVALID_ARGUMENT.value
    assert invalid.error["message"] != "operation is internal-only"
    assert internal.error == _error_payload(
        ErrorCode.INVALID_ARGUMENT,
        "operation is internal-only",
        {"operation": "compat.probe.step"},
    )
    assert trace == []


@pytest.mark.asyncio
async def test_non_null_confirmation_token_is_rejected_before_local_io(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    application = _application(runtime_snapshot, trace)

    outcome = await application.execute("bridge.doctor", {}, confirmation_token="")

    assert outcome.error == _error_payload(
        ErrorCode.INVALID_ARGUMENT,
        "confirmation token is not accepted for this operation",
        {"operation": "bridge.doctor"},
    )
    assert outcome.request_id is None
    assert trace == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation_class", "operation", "arguments"),
    [
        (
            "reconcile",
            "bridge.reconcile",
            {"request_id": str(REQUEST_1), "disposition": "applied"},
        ),
    ],
)
async def test_unimplemented_effective_classes_fail_closed_before_transport(
    operation_class: str,
    operation: str,
    arguments: dict[str, JsonValue],
) -> None:
    registry = OPERATION_REGISTRY
    snapshot = _snapshot_for_registry(registry)
    trace: list[object] = []
    application = _application(snapshot, trace, registry=registry)

    outcome = await application.execute(operation, arguments)

    assert outcome.error == _error_payload(
        ErrorCode.POLICY_DENIED,
        "operation requires a dedicated safety workflow",
        {"operation": operation, "operation_class": operation_class},
    )
    assert outcome.request_id is None
    assert trace == []


@pytest.mark.asyncio
async def test_residual_dynamic_class_fails_closed_before_transport() -> None:
    registry = _registry_for_closed_class("dynamic")
    snapshot = _snapshot_for_registry(registry)
    trace: list[object] = []
    application = _application(snapshot, trace, registry=registry)

    outcome = await application.execute(
        "lua.call",
        {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}},
    )

    assert outcome.error == _error_payload(
        ErrorCode.POLICY_DENIED,
        "operation requires a dedicated safety workflow",
        {"operation": "lua.call", "operation_class": "dynamic"},
    )
    assert trace == []


@pytest.mark.asyncio
async def test_unrecognized_local_registry_product_fails_closed_without_io() -> None:
    registry = _registry_with_unrecognized_local()
    snapshot = _snapshot_for_registry(registry)
    trace: list[object] = []
    application = _application(snapshot, trace, registry=registry)

    outcome = await application.execute("scenario.get", {})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert trace == []


@pytest.mark.asyncio
async def test_unexpected_runtime_error_from_local_port_propagates(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    application = _application(
        runtime_snapshot,
        trace,
        local_operations=_LocalOperations(trace, {}, RuntimeError("programming bug")),
    )

    with pytest.raises(RuntimeError, match="programming bug"):
        await application.execute("bridge.doctor", {})

    assert len(trace) == 1
    assert cast(tuple[object, ...], trace[0])[0] == "local"


@pytest.mark.asyncio
async def test_non_json_bridge_error_details_propagate_validation_failure(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    application = _application(
        runtime_snapshot,
        trace,
        local_operations=_LocalOperations(
            trace,
            {},
            BridgeError(ErrorCode.GAME_ROOT_INVALID, "bad details", {"value": object()}),
        ),
    )

    with pytest.raises(Exception) as caught:
        await application.execute("bridge.doctor", {})

    assert not isinstance(caught.value, BridgeError)
    assert len(trace) == 1
    assert cast(tuple[object, ...], trace[0])[0] == "local"


@pytest.mark.asyncio
@pytest.mark.parametrize("nonfinite", [math.nan, math.inf, -math.inf])
async def test_nonfinite_bridge_error_details_propagate(
    runtime_snapshot: RuntimeSnapshot,
    nonfinite: float,
) -> None:
    trace: list[object] = []
    application = _application(
        runtime_snapshot,
        trace,
        local_operations=_LocalOperations(
            trace,
            {},
            BridgeError(ErrorCode.INVALID_ARGUMENT, "nonfinite details", {"x": nonfinite}),
        ),
    )

    with pytest.raises(ValueError):
        await application.execute("bridge.doctor", {})

    assert len(trace) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("nonfinite", [math.nan, math.inf, -math.inf])
async def test_adapter_valid_nonfinite_local_result_fails_closed(nonfinite: float) -> None:
    registry = _registry_with_scenario_doctor_result()
    snapshot = _snapshot_for_registry(registry)
    trace: list[object] = []
    result = _scenario_result().model_copy(update={"current_time_seconds": nonfinite})
    application = _application(
        snapshot,
        trace,
        registry=registry,
        local_operations=_LocalOperations(trace, {"bridge.doctor": result}),
    )

    outcome = await application.execute("bridge.doctor", {})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error == _error_payload(
        ErrorCode.PROTOCOL_ERROR,
        "operation result failed public validation",
        {"operation": "bridge.doctor"},
    )


@pytest.mark.asyncio
async def test_cyclic_caller_mapping_becomes_invalid_argument_before_io(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    cyclic: dict[str, object] = {}
    cyclic["self"] = cyclic
    application = _application(runtime_snapshot, trace)

    outcome = await application.execute(
        "bridge.doctor",
        cast(Mapping[str, JsonValue], cyclic),
    )

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.INVALID_ARGUMENT.value
    assert trace == []


@pytest.mark.asyncio
async def test_real_task_cancellation_propagates_from_local_port(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    local_operations = _BlockingLocalOperations(trace)
    application = _application(
        runtime_snapshot,
        trace,
        local_operations=local_operations,
    )
    task = asyncio.create_task(application.execute("bridge.doctor", {}))
    started = asyncio.create_task(local_operations.started.wait())
    await asyncio.wait({task, started}, return_when=asyncio.FIRST_COMPLETED)
    if task.done():
        started.cancel()
        with suppress(asyncio.CancelledError):
            await started
        await task

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(trace) == 1
    assert cast(tuple[object, ...], trace[0])[0] == "local"


@pytest.mark.asyncio
async def test_status_routes_accept_lineage_through_one_session_and_policy_check(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    activation = _activation(runtime_snapshot)
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(activation,),
    )
    policy = _Policy(trace)
    application = _application(
        runtime_snapshot,
        trace,
        sessions=coordinator,
        policy=policy,
    )

    outcome = await application.execute(
        "bridge.status",
        {"accept_lineage_id": str(LINEAGE_ID)},
    )

    assert outcome == InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=REQUEST_1,
        ok=True,
        result=cast(JsonValue, activation.status.model_dump(mode="json")),
        error=None,
    )
    assert trace == ["open", "status", "policy", "close"]
    assert coordinator.handshake_calls == [(LINEAGE_ID, None)]
    assert len(policy.calls) == 1
    status, invocation, snapshot = policy.calls[0]
    assert status == activation.status
    assert type(invocation) is ResolvedInvocation
    assert invocation.contract.name == "bridge.status"
    assert type(invocation.public_arguments) is BridgeStatusArgs
    assert invocation.public_arguments.accept_lineage_id == LINEAGE_ID
    wire_arguments = invocation.wire_arguments
    assert type(wire_arguments) is BridgeStatusWireArgs
    assert wire_arguments.activation_candidate == ACTIVATION_1
    assert snapshot == runtime_snapshot


@pytest.mark.asyncio
async def test_status_policy_bridge_error_retains_status_request_id(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    activation = _activation(runtime_snapshot)
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(activation,),
    )
    policy = _Policy(
        trace,
        errors=(BridgeError(ErrorCode.UNSUPPORTED_BUILD, "profile is missing", {"build": 1868}),),
    )
    application = _application(
        runtime_snapshot,
        trace,
        sessions=coordinator,
        policy=policy,
    )

    outcome = await application.execute("bridge.status", {})

    assert outcome == InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=REQUEST_1,
        ok=False,
        result=None,
        error=cast(
            dict[str, JsonValue],
            _error_payload(
                ErrorCode.UNSUPPORTED_BUILD,
                "profile is missing",
                {"build": 1868},
            ),
        ),
    )
    assert trace == ["open", "status", "policy", "close"]


@pytest.mark.asyncio
async def test_status_handshake_bridge_error_has_no_observable_request_id(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(BridgeError(ErrorCode.BRIDGE_UNRESPONSIVE, "status failed"),),
    )
    application = _application(runtime_snapshot, trace, sessions=coordinator)

    outcome = await application.execute("bridge.status", {})

    assert outcome == InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=None,
        ok=False,
        result=None,
        error=cast(
            dict[str, JsonValue],
            _error_payload(ErrorCode.BRIDGE_UNRESPONSIVE, "status failed", {}),
        ),
    )
    assert trace == ["open", "status", "close"]


@pytest.mark.asyncio
async def test_successful_read_uses_one_session_status_policy_and_exchange(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    activation = _activation(runtime_snapshot, request_id=REQUEST_3)
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(activation,),
        read_request_ids=(REQUEST_1,),
    )
    policy = _Policy(trace)
    result = _scenario_result()
    transport = _Transport(
        trace,
        responses=(
            lambda command: _domain_artifact(
                command,
                result=cast(JsonValue, result.model_dump(mode="json")),
            ),
        ),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=transport,
        sessions=coordinator,
        policy=policy,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome == InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=REQUEST_1,
        ok=True,
        result=cast(JsonValue, result.model_dump(mode="json")),
        error=None,
    )
    assert trace == ["open", "status", "policy", "read", "close"]
    assert coordinator.read_channels == [transport.channel]
    assert coordinator.read_calls[0][1] == 1.25
    assert coordinator.reprepare_calls == []
    assert len(policy.calls) == 1


@pytest.mark.asyncio
async def test_read_policy_denial_sends_no_domain_exchange(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_3),),
        read_request_ids=(REQUEST_1,),
    )
    policy = _Policy(
        trace,
        errors=(BridgeError(ErrorCode.POLICY_DENIED, "read denied"),),
    )
    application = _application(
        runtime_snapshot,
        trace,
        sessions=coordinator,
        policy=policy,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome.request_id == REQUEST_1
    assert outcome.error == _error_payload(ErrorCode.POLICY_DENIED, "read denied", {})
    assert trace == ["open", "status", "policy", "close"]


@pytest.mark.asyncio
async def test_one_exact_activation_mismatch_reprepares_once_then_succeeds(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    first_activation = _activation(runtime_snapshot, request_id=REQUEST_3)
    second_activation = _activation(
        runtime_snapshot,
        request_id=REQUEST_4,
        activation_id=ACTIVATION_2,
    )
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(first_activation, second_activation),
        read_request_ids=(REQUEST_1, REQUEST_2),
    )
    result = _scenario_result()
    transport = _Transport(
        trace,
        responses=(
            lambda command: _domain_artifact(command, code=ErrorCode.ACTIVATION_MISMATCH),
            lambda command: _domain_artifact(
                command,
                result=cast(JsonValue, result.model_dump(mode="json")),
            ),
        ),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=transport,
        sessions=coordinator,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome.ok is True
    assert outcome.request_id == REQUEST_2
    assert REQUEST_2 != REQUEST_1
    assert trace == [
        "open",
        "status",
        "policy",
        "read",
        "status",
        "policy",
        "read",
        "close",
    ]
    assert len(coordinator.reprepare_calls) == 1
    assert coordinator.read_channels == [transport.channel]
    assert coordinator.reprepare_channels == [transport.channel]
    prior, mismatch = coordinator.reprepare_calls[0]
    assert prior.command.request_id == REQUEST_1
    assert mismatch.accepted_response.error is not None
    assert mismatch.accepted_response.error.code is ErrorCode.ACTIVATION_MISMATCH


@pytest.mark.asyncio
async def test_second_activation_mismatch_returns_failure_without_third_attempt(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(
            _activation(runtime_snapshot, request_id=REQUEST_3),
            _activation(
                runtime_snapshot,
                request_id=REQUEST_4,
                activation_id=ACTIVATION_2,
            ),
        ),
        read_request_ids=(REQUEST_1, REQUEST_2),
    )
    transport = _Transport(
        trace,
        responses=(
            lambda command: _domain_artifact(command, code=ErrorCode.ACTIVATION_MISMATCH),
            lambda command: _domain_artifact(command, code=ErrorCode.ACTIVATION_MISMATCH),
        ),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=transport,
        sessions=coordinator,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.ACTIVATION_MISMATCH.value
    assert trace == [
        "open",
        "status",
        "policy",
        "read",
        "status",
        "policy",
        "read",
        "close",
    ]
    assert len(coordinator.reprepare_calls) == 1


@pytest.mark.asyncio
async def test_non_mismatch_read_error_never_rehandshakes(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_3),),
        read_request_ids=(REQUEST_1,),
    )
    transport = _Transport(
        trace,
        responses=(lambda command: _domain_artifact(command, code=ErrorCode.NOT_FOUND),),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=transport,
        sessions=coordinator,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome.request_id == REQUEST_1
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.NOT_FOUND.value
    assert trace == ["open", "status", "policy", "read", "close"]
    assert coordinator.reprepare_calls == []


@pytest.mark.asyncio
async def test_policy_denial_after_rehandshake_prevents_second_read(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(
            _activation(runtime_snapshot, request_id=REQUEST_3),
            _activation(
                runtime_snapshot,
                request_id=REQUEST_4,
                activation_id=ACTIVATION_2,
            ),
        ),
        read_request_ids=(REQUEST_1, REQUEST_2),
    )
    policy = _Policy(
        trace,
        errors=(None, BridgeError(ErrorCode.POLICY_DENIED, "recovered read denied")),
    )
    transport = _Transport(
        trace,
        responses=(lambda command: _domain_artifact(command, code=ErrorCode.ACTIVATION_MISMATCH),),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=transport,
        sessions=coordinator,
        policy=policy,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome.request_id == REQUEST_2
    assert outcome.error == _error_payload(
        ErrorCode.POLICY_DENIED,
        "recovered read denied",
        {},
    )
    assert trace == [
        "open",
        "status",
        "policy",
        "read",
        "status",
        "policy",
        "close",
    ]
    assert len(coordinator.reprepare_calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "artifact_factory",
    [
        _mismatch_foreign_request,
        _mismatch_foreign_hash,
        _mismatch_foreign_lineage,
        _mismatch_foreign_activation,
        _mismatch_foreign_runtime_tag,
        _mismatch_cancel_delivery,
    ],
)
async def test_foreign_or_non_request_mismatch_artifact_fails_before_reprepare(
    runtime_snapshot: RuntimeSnapshot,
    artifact_factory: Callable[[ExchangeCommand], ResponseArtifact],
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_3),),
        read_request_ids=(REQUEST_1,),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=_Transport(trace, responses=(artifact_factory,)),
        sessions=coordinator,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert coordinator.reprepare_calls == []
    assert trace == ["open", "status", "policy", "read", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "artifact_factory",
    [
        _mismatch_without_settlement,
        _mismatch_with_mutation_evidence,
    ],
)
async def test_malformed_correlated_mismatch_fails_without_reprepare(
    runtime_snapshot: RuntimeSnapshot,
    artifact_factory: Callable[[ExchangeCommand], ResponseArtifact],
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_3),),
        read_request_ids=(REQUEST_1,),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=_Transport(trace, responses=(artifact_factory,)),
        sessions=coordinator,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_1
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert coordinator.reprepare_calls == []
    assert trace == ["open", "status", "policy", "read", "close"]


@pytest.mark.asyncio
async def test_mutation_uses_one_status_policy_and_exchange_on_same_channel(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    activation = _activation(runtime_snapshot, request_id=REQUEST_3)
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(activation,),
        mutation_request_ids=(REQUEST_1,),
    )
    result = _unit_set_result()
    transport = _Transport(
        trace,
        responses=(
            lambda command: _domain_artifact(
                command,
                result=cast(JsonValue, result.model_dump(mode="json")),
            ),
        ),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=transport,
        sessions=coordinator,
    )

    outcome = await application.execute(
        "unit.set",
        {"unit_guid": "unit-1", "speed": 350.0},
    )

    assert outcome == InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=REQUEST_1,
        ok=True,
        result=cast(JsonValue, result.model_dump(mode="json")),
        error=None,
    )
    assert trace == ["open", "status", "policy", "mutation", "close"]
    assert coordinator.handshake_channels == [transport.channel]
    assert len(coordinator.prepare_calls) == 1
    assert transport.channel.commands[0].request_id == REQUEST_1


@pytest.mark.asyncio
async def test_mutation_policy_denial_sends_no_domain_exchange(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_3),),
        mutation_request_ids=(REQUEST_1,),
    )
    policy = _Policy(
        trace,
        errors=(BridgeError(ErrorCode.POLICY_DENIED, "mutation denied"),),
    )
    application = _application(
        runtime_snapshot,
        trace,
        sessions=coordinator,
        policy=policy,
    )

    outcome = await application.execute(
        "unit.set",
        {"unit_guid": "unit-1", "speed": 350.0},
    )

    assert outcome.request_id is None
    assert outcome.error == _error_payload(ErrorCode.POLICY_DENIED, "mutation denied", {})
    assert trace == ["open", "status", "policy", "close"]
    assert coordinator.prepare_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [
        ErrorCode.INDETERMINATE_OUTCOME,
        ErrorCode.ACTIVATION_MISMATCH,
        ErrorCode.NOT_FOUND,
    ],
)
async def test_mutation_response_failure_is_never_retried_and_retains_request_id(
    runtime_snapshot: RuntimeSnapshot,
    code: ErrorCode,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_3),),
        mutation_request_ids=(REQUEST_1,),
    )
    transport = _Transport(
        trace,
        responses=(
            lambda command: _domain_artifact(
                command,
                code=code,
                rejected_settlement=False,
            ),
        ),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=transport,
        sessions=coordinator,
    )

    outcome = await application.execute(
        "unit.set",
        {"unit_guid": "unit-1", "speed": 350.0},
    )

    assert outcome.request_id == REQUEST_1
    assert outcome.ok is False
    assert outcome.error is not None
    assert outcome.error["code"] == code.value
    assert trace == ["open", "status", "policy", "mutation", "close"]
    assert len(transport.channel.commands) == 1
    assert len(coordinator.prepare_calls) == 1


@pytest.mark.asyncio
async def test_mutation_transport_bridge_error_retains_validated_command_id(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_3),),
        mutation_request_ids=(REQUEST_1,),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=_Transport(
            trace,
            responses=(BridgeError(ErrorCode.REQUEST_TIMEOUT, "mutation timed out"),),
        ),
        sessions=coordinator,
    )

    outcome = await application.execute(
        "unit.set",
        {"unit_guid": "unit-1", "speed": 350.0},
    )

    assert outcome.request_id == REQUEST_1
    assert outcome.error == _error_payload(
        ErrorCode.REQUEST_TIMEOUT,
        "mutation timed out",
        {},
    )
    assert trace == ["open", "status", "policy", "mutation", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "forgery",
    [
        "request_uuid_version",
        "request_id_reuse",
        "activation_id_reuse",
        "invocation",
        "operation",
        "arguments",
        "activation",
        "snapshot",
        "timeout",
        "constructed_body",
    ],
)
async def test_semantically_forged_mutation_command_fails_before_exchange(
    runtime_snapshot: RuntimeSnapshot,
    forgery: str,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_3),),
        mutation_command_factory=_forged_mutation_command_factory(
            forgery,
            runtime_snapshot,
        ),
    )
    transport = _Transport(trace)
    application = _application(
        runtime_snapshot,
        trace,
        transport=transport,
        sessions=coordinator,
    )

    outcome = await application.execute(
        "unit.set",
        {"unit_guid": "unit-1", "speed": 350.0},
    )

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert trace == ["open", "status", "policy", "close"]
    assert len(coordinator.prepare_calls) == 1
    assert transport.channel.commands == []


@pytest.mark.asyncio
async def test_status_round_trip_rejects_constructed_activation_before_policy(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    valid = _activation(runtime_snapshot)
    constructed = SessionActivation.model_construct(
        status_request_id="not-a-uuid",
        status=valid.status,
        response_artifact=valid.response_artifact,
    )
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(constructed,),
    )
    application = _application(runtime_snapshot, trace, sessions=coordinator)

    outcome = await application.execute("bridge.status", {})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert trace == ["open", "status", "close"]


@pytest.mark.asyncio
async def test_status_rejects_request_and_activation_candidate_collision(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(
            _activation(
                runtime_snapshot,
                request_id=ACTIVATION_1,
                activation_id=ACTIVATION_1,
            ),
        ),
    )
    application = _application(runtime_snapshot, trace, sessions=coordinator)

    outcome = await application.execute("bridge.status", {})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert trace == ["open", "status", "close"]


@pytest.mark.asyncio
async def test_read_rejects_foreign_status_snapshot_before_policy_or_exchange(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    foreign = _foreign_snapshot()
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(foreign, request_id=REQUEST_3),),
        read_request_ids=(REQUEST_1,),
    )
    application = _application(runtime_snapshot, trace, sessions=coordinator)

    outcome = await application.execute("scenario.get", {})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert trace == ["open", "status", "close"]


@pytest.mark.asyncio
async def test_read_rejects_status_and_domain_request_id_reuse_before_policy(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_1),),
        read_request_ids=(REQUEST_1,),
    )
    application = _application(runtime_snapshot, trace, sessions=coordinator)

    outcome = await application.execute("scenario.get", {})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert trace == ["open", "status", "close"]


@pytest.mark.asyncio
async def test_mutation_rejects_foreign_status_snapshot_before_policy(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    foreign = _foreign_snapshot()
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(foreign, request_id=REQUEST_3),),
        mutation_request_ids=(REQUEST_1,),
    )
    application = _application(runtime_snapshot, trace, sessions=coordinator)

    outcome = await application.execute(
        "unit.set",
        {"unit_guid": "unit-1", "speed": 350.0},
    )

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert trace == ["open", "status", "close"]


@pytest.mark.asyncio
async def test_second_phase_lua_validation_fails_before_transport(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    application = _application(runtime_snapshot, trace)

    outcome = await application.execute(
        "lua.call",
        {"function": "ScenEdit_GetScore", "arguments": {}},
    )

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.INVALID_ARGUMENT.value
    assert outcome.error["message"] == "operation arguments are invalid"
    assert trace == []


@pytest.mark.asyncio
async def test_constructor_pins_snapshot_against_later_coordinator_drift(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot),),
    )
    policy = _Policy(trace)
    application = _application(
        runtime_snapshot,
        trace,
        sessions=coordinator,
        policy=policy,
    )
    coordinator.set_runtime_snapshot_for_test(_foreign_snapshot())

    outcome = await application.execute("bridge.status", {})

    assert outcome.ok is True
    assert outcome.request_id == REQUEST_1
    assert trace == ["open", "status", "policy", "close"]
    assert policy.calls[0][2] == runtime_snapshot


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "artifact_factory",
    [
        _foreign_runtime_success,
        _foreign_ordinary_error,
        _constructed_invalid_artifact,
        _cross_command_success_artifact,
    ],
)
async def test_foreign_success_error_or_constructed_artifact_clears_local_id(
    runtime_snapshot: RuntimeSnapshot,
    artifact_factory: Callable[[ExchangeCommand], ResponseArtifact],
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_3),),
        read_request_ids=(REQUEST_1,),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=_Transport(trace, responses=(artifact_factory,)),
        sessions=coordinator,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert trace == ["open", "status", "policy", "read", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("artifact_factory", "expected_code"),
    [
        (_scenario_changed_artifact, ErrorCode.SCENARIO_CHANGED),
        (_manifest_mismatch_artifact, ErrorCode.MANIFEST_MISMATCH),
    ],
)
async def test_exact_context_exceptions_remain_accepted_response_failures(
    runtime_snapshot: RuntimeSnapshot,
    artifact_factory: Callable[[ExchangeCommand], ResponseArtifact],
    expected_code: ErrorCode,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_3),),
        read_request_ids=(REQUEST_1,),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=_Transport(trace, responses=(artifact_factory,)),
        sessions=coordinator,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_1
    assert outcome.error is not None
    assert outcome.error["code"] == expected_code.value
    assert trace == ["open", "status", "policy", "read", "close"]


@pytest.mark.asyncio
async def test_successful_artifact_with_malformed_result_fails_closed(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_3),),
        read_request_ids=(REQUEST_1,),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=_Transport(
            trace,
            responses=(lambda command: _domain_artifact(command, result={}),),
        ),
        sessions=coordinator,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_1
    assert outcome.error == _error_payload(
        ErrorCode.PROTOCOL_ERROR,
        "operation result failed public validation",
        {"operation": "scenario.get"},
    )


@pytest.mark.asyncio
async def test_failed_mutation_exposes_no_mutation_evidence_or_settlement(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(_activation(runtime_snapshot, request_id=REQUEST_3),),
        mutation_request_ids=(REQUEST_1,),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=_Transport(
            trace,
            responses=(
                lambda command: _domain_artifact(
                    command,
                    code=ErrorCode.INVALID_ARGUMENT,
                    message="mutation rejected before execution",
                    details={"field": "speed"},
                    mutation_not_started=True,
                ),
            ),
        ),
        sessions=coordinator,
    )

    outcome = await application.execute(
        "unit.set",
        {"unit_guid": "unit-1", "speed": 350.0},
    )

    assert outcome.error == _error_payload(
        ErrorCode.INVALID_ARGUMENT,
        "mutation rejected before execution",
        {"field": "speed"},
    )
    assert "mutation_not_started" not in cast(dict[str, JsonValue], outcome.error)
    assert "settlement" not in cast(dict[str, JsonValue], outcome.error)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("second_status_id", "second_domain_id"),
    [
        (REQUEST_4, REQUEST_3),
        (REQUEST_1, REQUEST_2),
    ],
)
async def test_recovered_read_rejects_cross_role_identity_reuse(
    runtime_snapshot: RuntimeSnapshot,
    second_status_id: UUID,
    second_domain_id: UUID,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(
            _activation(runtime_snapshot, request_id=REQUEST_3),
            _activation(
                runtime_snapshot,
                request_id=second_status_id,
                activation_id=ACTIVATION_2,
            ),
        ),
        read_request_ids=(REQUEST_1, second_domain_id),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=_Transport(
            trace,
            responses=(
                lambda command: _domain_artifact(
                    command,
                    code=ErrorCode.ACTIVATION_MISMATCH,
                ),
            ),
        ),
        sessions=coordinator,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_1
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert trace == ["open", "status", "policy", "read", "status", "close"]


@pytest.mark.asyncio
async def test_recovered_read_rejects_reused_activation_candidate(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(
            _activation(runtime_snapshot, request_id=REQUEST_3),
            _activation(
                runtime_snapshot,
                request_id=REQUEST_4,
                activation_id=ACTIVATION_1,
            ),
        ),
        read_request_ids=(REQUEST_1, REQUEST_2),
    )
    application = _application(
        runtime_snapshot,
        trace,
        transport=_Transport(
            trace,
            responses=(
                lambda command: _domain_artifact(
                    command,
                    code=ErrorCode.ACTIVATION_MISMATCH,
                ),
            ),
        ),
        sessions=coordinator,
    )

    outcome = await application.execute("scenario.get", {})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_1
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert trace == ["open", "status", "policy", "read", "status", "close"]


@pytest.mark.asyncio
async def test_concrete_session_service_fits_application_on_one_channel(
    runtime_snapshot: RuntimeSnapshot,
    tmp_path: Path,
) -> None:
    trace: list[object] = []
    command_exe = Path(r"D:\Games\CMO\Command.exe")
    process = ProcessInfo(pid=1200, create_time=1000.5, executable=command_exe)
    result = _scenario_result()
    channel = _ConcreteSessionChannel(
        trace,
        process,
        responders=(
            lambda command: _status_artifact_for_command(command, runtime_snapshot),
            lambda command: _domain_artifact(
                command,
                result=cast(JsonValue, result.model_dump(mode="json")),
            ),
        ),
    )
    transport = _ConcreteTransport(trace, channel)
    sessions = SessionService(
        scope=SessionScope(root_key="a" * 64, command_exe=command_exe),
        session_store=SessionStore(StateDatabase(tmp_path / "state.sqlite3")),
        registry=OPERATION_REGISTRY,
        runtime_snapshot=runtime_snapshot,
        wall_clock=_WallClock(1_752_400_000_123),
        uuid4_source=_UuidSequence(ACTIVATION_1, REQUEST_3, REQUEST_1),
        status_timeout_seconds=0.5,
    )
    policy = _Policy(trace)
    application = _application(
        runtime_snapshot,
        trace,
        transport=transport,
        sessions=sessions,
        policy=policy,
    )

    outcome = await application.execute("scenario.get", {})

    assert sessions.runtime_snapshot == runtime_snapshot
    assert outcome.ok is True
    assert outcome.request_id == REQUEST_1
    assert trace == ["open", "status", "policy", "read", "close"]
    assert [command.request_id for command in channel.commands] == [REQUEST_3, REQUEST_1]
    assert policy.calls[0][2] == runtime_snapshot


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "operation",
        "arguments",
        "lookup_operation",
        "lookup_arguments",
        "target_result",
        "target",
        "impact",
    ),
    [
        pytest.param(
            "unit.delete",
            {"unit_guid": "UNIT-1"},
            "unit.get",
            {"unit_guid": "UNIT-1"},
            _unit_result(),
            DestructiveTarget(guid="UNIT-1", name="Alpha", type="Aircraft"),
            "Permanently deletes the resolved unit from the scenario. "
            "The bridge cannot undo this action.",
            id="unit",
        ),
        pytest.param(
            "mission.delete",
            {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"},
            "mission.get",
            {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"},
            _mission_result(),
            DestructiveTarget(guid="MISSION-1", name="CAP North", type="patrol"),
            "Permanently deletes the resolved mission from the scenario. "
            "The bridge cannot undo this action.",
            id="mission",
        ),
    ],
)
async def test_destructive_preview_resolves_exact_target_and_issues_bound_token(
    runtime_snapshot: RuntimeSnapshot,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    arguments: dict[str, JsonValue],
    lookup_operation: str,
    lookup_arguments: dict[str, JsonValue],
    target_result: UnitResult | MissionResult,
    target: DestructiveTarget,
    impact: str,
) -> None:
    trace: list[object] = []
    activation = _activation(
        runtime_snapshot,
        request_id=REQUEST_1,
        activation_id=RESERVED_CANDIDATE,
    )
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(activation,),
        mutation_request_ids=(REQUEST_2,),
        reserved_candidates=(RESERVED_CANDIDATE,),
    )
    transport = _Transport(
        trace,
        responses=(
            lambda command: _domain_artifact(
                command,
                result=cast(JsonValue, target_result.model_dump(mode="json")),
            ),
        ),
    )
    policy = _Policy(trace)
    confirmations = _ConfirmationStore(trace)
    wall_clock = _WallClock(PREVIEW_NOW_MS, trace=trace)
    resolution_calls: list[tuple[str, dict[str, object], dict[str, object] | None]] = []
    original_resolve = OperationRegistry.resolve_invocation

    def resolve_spy(
        registry: OperationRegistry,
        name: str,
        public_arguments: Mapping[str, object],
        trusted_enrichment: Mapping[str, object] | None = None,
    ) -> ResolvedInvocation:
        if name in {"unit.delete", "mission.delete"}:
            raise AssertionError("preview resolved a destructive invocation")
        resolution_calls.append(
            (
                name,
                dict(public_arguments),
                None if trusted_enrichment is None else dict(trusted_enrichment),
            )
        )
        return original_resolve(
            registry,
            name,
            public_arguments,
            trusted_enrichment,
        )

    monkeypatch.setattr(OperationRegistry, "resolve_invocation", resolve_spy)
    application = _application(
        runtime_snapshot,
        trace,
        transport=transport,
        sessions=coordinator,
        policy=policy,
        confirmations=confirmations,
        wall_clock=wall_clock,
    )

    outcome = await application.execute(operation, arguments)

    descriptor = _destructive_descriptor(
        runtime_snapshot,
        operation=operation,
        arguments=arguments,
        target=target,
    )
    expected_binding = destructive_confirmation_binding(descriptor)
    assert outcome == InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=REQUEST_2,
        ok=True,
        result=_destructive_preview_result(
            operation=operation,
            target=target,
            impact=impact,
        ),
        error=None,
    )
    assert trace == [
        "reserve",
        "open",
        "status",
        "destructive_policy",
        "read",
        "clock",
        "issue",
        "close",
    ]
    assert coordinator.reserve_calls == 1
    assert coordinator.handshake_calls == [(None, RESERVED_CANDIDATE)]
    assert coordinator.read_calls == []
    assert coordinator.reprepare_calls == []
    assert len(coordinator.prepare_calls) == 1
    prepared_invocation = coordinator.prepare_calls[0][1]
    assert prepared_invocation.contract.name == lookup_operation
    assert (
        prepared_invocation.public_arguments.model_dump(
            mode="json",
            exclude_none=True,
        )
        == lookup_arguments
    )
    assert resolution_calls == [(lookup_operation, lookup_arguments, None)]
    assert policy.calls == []
    assert policy.destructive_calls == [
        (
            activation.status,
            OPERATION_REGISTRY.resolve(operation),
            runtime_snapshot,
        )
    ]
    assert confirmations.issue_calls == [(expected_binding, PREVIEW_NOW_MS)]
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 1
    assert len(transport.channel.commands) == 1
    command = transport.channel.commands[0]
    assert command.request_id == REQUEST_2
    assert command.invocation.contract.name == lookup_operation
    assert {
        key: value for key, value in command.body.arguments.items() if value is not None
    } == lookup_arguments
    assert "confirmation_proof" not in command.body.arguments


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "arguments"),
    [
        ("unit.delete", {}),
        ("unit.delete", {"unit_guid": "UNIT-1", "unexpected": "x"}),
        (
            "unit.delete",
            {"unit_guid": "UNIT-1", "confirmation_proof": "a" * 64},
        ),
        ("mission.delete", {"side_guid": "SIDE-1"}),
        (
            "mission.delete",
            {
                "side_guid": "SIDE-1",
                "mission_guid": "MISSION-1",
                "confirmation_proof": "a" * 64,
            },
        ),
    ],
)
async def test_destructive_preview_validates_public_arguments_before_reservation_or_io(
    runtime_snapshot: RuntimeSnapshot,
    operation: str,
    arguments: dict[str, JsonValue],
) -> None:
    trace: list[object] = []
    application, coordinator, transport, policy, confirmations, wall_clock = (
        _unit_preview_application(runtime_snapshot, trace)
    )

    outcome = await application.execute(operation, arguments)

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.INVALID_ARGUMENT.value
    assert coordinator.reserve_calls == 0
    assert coordinator.handshake_calls == []
    assert transport.channel.commands == []
    assert policy.destructive_calls == []
    assert confirmations.issue_calls == []
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 0
    assert trace == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "candidate",
    [
        object(),
        UUID("11111111-1111-1111-8111-111111111111"),
    ],
    ids=["not-an-exact-uuid", "not-uuid4"],
)
async def test_destructive_preview_rejects_invalid_reserved_candidate_before_open(
    runtime_snapshot: RuntimeSnapshot,
    candidate: object,
) -> None:
    trace: list[object] = []
    application, coordinator, transport, policy, confirmations, wall_clock = (
        _unit_preview_application(
            runtime_snapshot,
            trace,
            reserved_candidate=candidate,
        )
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert coordinator.reserve_calls == 1
    assert coordinator.handshake_calls == []
    assert transport.channel.commands == []
    assert policy.destructive_calls == []
    assert confirmations.issue_calls == []
    assert wall_clock.calls == 0
    assert trace == ["reserve"]


@pytest.mark.asyncio
async def test_destructive_preview_open_failure_has_no_request_id_or_store_io(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    transport = _SessionOpenFailureTransport(
        trace,
        BridgeError(ErrorCode.REQUEST_TIMEOUT, "session open failed"),
    )
    application, coordinator, _, policy, confirmations, wall_clock = _unit_preview_application(
        runtime_snapshot, trace, transport=transport
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.error == _error_payload(
        ErrorCode.REQUEST_TIMEOUT,
        "session open failed",
        {},
    )
    assert outcome.request_id is None
    assert coordinator.reserve_calls == 1
    assert coordinator.handshake_calls == []
    assert policy.destructive_calls == []
    assert confirmations.issue_calls == []
    assert wall_clock.calls == 0
    assert trace == ["reserve", "open_error"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("activation", "expected_code"),
    [
        (
            BridgeError(ErrorCode.BRIDGE_UNRESPONSIVE, "status failed"),
            ErrorCode.BRIDGE_UNRESPONSIVE,
        ),
        (
            "wrong-candidate",
            ErrorCode.PROTOCOL_ERROR,
        ),
    ],
)
async def test_destructive_preview_handshake_failure_precedes_policy_and_read(
    runtime_snapshot: RuntimeSnapshot,
    activation: SessionActivation | BaseException | str,
    expected_code: ErrorCode,
) -> None:
    trace: list[object] = []
    selected_activation = (
        _activation(
            runtime_snapshot,
            request_id=REQUEST_1,
            activation_id=ACTIVATION_1,
        )
        if activation == "wrong-candidate"
        else cast(SessionActivation | BaseException, activation)
    )
    application, coordinator, transport, policy, confirmations, wall_clock = (
        _unit_preview_application(
            runtime_snapshot,
            trace,
            activation=selected_activation,
        )
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == expected_code.value
    assert coordinator.handshake_calls == [(None, RESERVED_CANDIDATE)]
    assert coordinator.prepare_calls == []
    assert transport.channel.commands == []
    assert policy.destructive_calls == []
    assert confirmations.issue_calls == []
    assert wall_clock.calls == 0
    assert trace == ["reserve", "open", "status", "close"]


@pytest.mark.asyncio
async def test_destructive_preview_policy_denial_is_proof_free_and_precedes_target_read(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    policy = _Policy(
        trace,
        destructive_errors=(
            BridgeError(ErrorCode.POLICY_DENIED, "destructive operations are disabled"),
        ),
    )
    application, coordinator, transport, _, confirmations, wall_clock = _unit_preview_application(
        runtime_snapshot, trace, policy=policy
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.error == _error_payload(
        ErrorCode.POLICY_DENIED,
        "destructive operations are disabled",
        {},
    )
    assert outcome.request_id is None
    assert coordinator.prepare_calls == []
    assert transport.channel.commands == []
    assert policy.calls == []
    assert len(policy.destructive_calls) == 1
    assert policy.destructive_calls[0][1] is OPERATION_REGISTRY.resolve("unit.delete")
    assert confirmations.issue_calls == []
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 0
    assert trace == [
        "reserve",
        "open",
        "status",
        "destructive_policy",
        "close",
    ]


@pytest.mark.asyncio
async def test_destructive_preview_invalid_lookup_command_never_becomes_public_request_id(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []

    def invalid_command(
        activation: SessionActivation,
        invocation: ResolvedInvocation,
        timeout_seconds: float,
    ) -> ExchangeCommand:
        return replace(
            _exchange_command(
                invocation,
                runtime_snapshot,
                activation,
                REQUEST_2,
                timeout_seconds,
            ),
            timeout=timeout_seconds + 1.0,
        )

    application, coordinator, transport, _, confirmations, wall_clock = _unit_preview_application(
        runtime_snapshot,
        trace,
        mutation_command_factory=invalid_command,
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert len(coordinator.prepare_calls) == 1
    assert transport.channel.commands == []
    assert confirmations.issue_calls == []
    assert wall_clock.calls == 0
    assert trace == [
        "reserve",
        "open",
        "status",
        "destructive_policy",
        "close",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected_code", "expected_request_id"),
    [
        pytest.param(
            _target_not_found_artifact,
            ErrorCode.NOT_FOUND,
            REQUEST_2,
            id="correlated-ordinary-error",
        ),
        pytest.param(
            BridgeError(ErrorCode.REQUEST_TIMEOUT, "target read timed out"),
            ErrorCode.REQUEST_TIMEOUT,
            REQUEST_2,
            id="transport-bridge-error",
        ),
        pytest.param(
            _foreign_unit_target_artifact,
            ErrorCode.PROTOCOL_ERROR,
            None,
            id="foreign-artifact-clears-id",
        ),
        pytest.param(
            _malformed_unit_target_artifact,
            ErrorCode.PROTOCOL_ERROR,
            REQUEST_2,
            id="malformed-correlated-result",
        ),
        pytest.param(
            _wrong_guid_unit_target_artifact,
            ErrorCode.PROTOCOL_ERROR,
            REQUEST_2,
            id="target-guid-mismatch",
        ),
        pytest.param(
            _wrong_type_unit_target_artifact,
            ErrorCode.PROTOCOL_ERROR,
            REQUEST_2,
            id="wrong-exact-target-type",
        ),
    ],
)
async def test_destructive_preview_target_failures_follow_request_id_rules(
    runtime_snapshot: RuntimeSnapshot,
    response: Callable[[ExchangeCommand], ResponseArtifact] | BaseException,
    expected_code: ErrorCode,
    expected_request_id: UUID | None,
) -> None:
    trace: list[object] = []
    application, _, transport, _, confirmations, wall_clock = _unit_preview_application(
        runtime_snapshot,
        trace,
        responses=(response,),
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id == expected_request_id
    assert outcome.error is not None
    assert outcome.error["code"] == expected_code.value
    assert len(transport.channel.commands) == 1
    assert transport.channel.commands[0].invocation.contract.name == "unit.get"
    assert confirmations.issue_calls == []
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 0
    assert trace == [
        "reserve",
        "open",
        "status",
        "destructive_policy",
        "read",
        "close",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_id",
    [
        REQUEST_1,
        RESERVED_CANDIDATE,
        UUID("11111111-1111-1111-8111-111111111111"),
    ],
    ids=["reuses-status-id", "reuses-reserved-candidate", "not-uuid4"],
)
async def test_destructive_preview_rejects_invalid_lookup_identity_before_exchange(
    runtime_snapshot: RuntimeSnapshot,
    request_id: UUID,
) -> None:
    trace: list[object] = []
    application, _, transport, _, confirmations, wall_clock = _unit_preview_application(
        runtime_snapshot,
        trace,
        mutation_request_ids=(request_id,),
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert transport.channel.commands == []
    assert confirmations.issue_calls == []
    assert wall_clock.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [asyncio.CancelledError(), RuntimeError("target exchange programming failure")],
    ids=["cancellation", "unexpected-runtime-error"],
)
async def test_destructive_preview_target_cancellation_or_programming_error_propagates(
    runtime_snapshot: RuntimeSnapshot,
    failure: BaseException,
) -> None:
    trace: list[object] = []
    application, _, transport, _, confirmations, wall_clock = _unit_preview_application(
        runtime_snapshot,
        trace,
        responses=(failure,),
    )

    with pytest.raises(
        type(failure), match=None if isinstance(failure, asyncio.CancelledError) else "programming"
    ):
        await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert len(transport.channel.commands) == 1
    assert confirmations.issue_calls == []
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 0
    assert trace == [
        "reserve",
        "open",
        "status",
        "destructive_policy",
        "read",
        "close",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "epoch",
    [
        True,
        -1,
        1.0,
        _IntSubclass(PREVIEW_NOW_MS),
        2**63 - 60_000,
    ],
    ids=["bool", "negative", "float", "int-subclass", "expiry-overflow"],
)
async def test_destructive_preview_rejects_invalid_clock_epoch_before_issue(
    runtime_snapshot: RuntimeSnapshot,
    epoch: object,
) -> None:
    trace: list[object] = []
    confirmations = _ConfirmationStore(trace)
    wall_clock = _WallClock(epoch, trace=trace)
    application, _, _, _, _, _ = _unit_preview_application(
        runtime_snapshot,
        trace,
        confirmations=confirmations,
        wall_clock=wall_clock,
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert wall_clock.calls == 1
    assert confirmations.issue_calls == []
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []
    assert trace == [
        "reserve",
        "open",
        "status",
        "destructive_policy",
        "read",
        "clock",
        "close",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "issued",
    [
        object(),
        None,
        IssuedConfirmation.model_construct(
            token="",
            expires_at_ms=PREVIEW_NOW_MS + 60_000,
        ),
        IssuedConfirmation.model_construct(
            token=CONFIRMATION_TOKEN[:-1] + "=",
            expires_at_ms=PREVIEW_NOW_MS + 60_000,
        ),
        IssuedConfirmation.model_construct(
            token=CONFIRMATION_TOKEN,
            expires_at_ms=PREVIEW_NOW_MS + 59_999,
        ),
        IssuedConfirmation.model_construct(
            token=CONFIRMATION_TOKEN,
            expires_at_ms=True,
        ),
    ],
    ids=[
        "wrong-type",
        "none",
        "empty-token",
        "noncanonical-token",
        "wrong-expiry",
        "bool-expiry",
    ],
)
async def test_destructive_preview_rejects_corrupt_issuance_and_retains_lookup_id(
    runtime_snapshot: RuntimeSnapshot,
    issued: object,
) -> None:
    trace: list[object] = []
    confirmations = _ConfirmationStore(trace, issued=issued)
    application, _, transport, _, _, wall_clock = _unit_preview_application(
        runtime_snapshot,
        trace,
        confirmations=confirmations,
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert len(transport.channel.commands) == 1
    assert wall_clock.calls == 1
    assert len(confirmations.issue_calls) == 1
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []
    assert trace == [
        "reserve",
        "open",
        "status",
        "destructive_policy",
        "read",
        "clock",
        "issue",
        "close",
    ]


@pytest.mark.asyncio
async def test_destructive_preview_preserves_issue_bridge_error_and_lookup_id(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    confirmations = _ConfirmationStore(
        trace,
        issue_error=BridgeError(ErrorCode.STATE_CONFLICT, "token collision"),
    )
    application, _, _, _, _, _ = _unit_preview_application(
        runtime_snapshot,
        trace,
        confirmations=confirmations,
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.error == _error_payload(
        ErrorCode.STATE_CONFLICT,
        "token collision",
        {},
    )
    assert outcome.request_id == REQUEST_2
    assert len(confirmations.issue_calls) == 1
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt_binding", [object(), None], ids=["object", "none"])
async def test_destructive_preview_revalidates_binding_before_clock_or_issue(
    runtime_snapshot: RuntimeSnapshot,
    monkeypatch: pytest.MonkeyPatch,
    corrupt_binding: object,
) -> None:
    trace: list[object] = []
    confirmations = _ConfirmationStore(trace)

    def corrupt_binding_factory(
        descriptor: DestructiveConfirmationDescriptor,
    ) -> object:
        del descriptor
        return corrupt_binding

    monkeypatch.setattr(
        service_module,
        "destructive_confirmation_binding",
        corrupt_binding_factory,
        raising=False,
    )
    application, _, _, _, _, wall_clock = _unit_preview_application(
        runtime_snapshot,
        trace,
        confirmations=confirmations,
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert wall_clock.calls == 0
    assert confirmations.issue_calls == []
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []


@pytest.mark.asyncio
async def test_destructive_preview_revalidates_standalone_result_after_issue(
    runtime_snapshot: RuntimeSnapshot,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace: list[object] = []
    corrupt_result = DestructivePreviewResult.model_construct(
        operation="unit.delete",
        target_guid="UNIT-1",
        target_name="Alpha",
        target_type="Aircraft",
        impact="",
        reserved_activation_candidate=RESERVED_CANDIDATE,
        confirmation_token="",
        expires_at_utc=datetime.fromtimestamp(
            (PREVIEW_NOW_MS + 60_000) / 1000,
            tz=UTC,
        ),
    )

    def corrupt_preview_factory(**values: object) -> DestructivePreviewResult:
        del values
        return corrupt_result

    monkeypatch.setattr(
        service_module,
        "DestructivePreviewResult",
        corrupt_preview_factory,
        raising=False,
    )
    application, _, _, _, confirmations, _ = _unit_preview_application(
        runtime_snapshot,
        trace,
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert len(confirmations.issue_calls) == 1
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []


@pytest.mark.asyncio
async def test_destructive_preview_revalidates_descriptor_before_clock_or_issue(
    runtime_snapshot: RuntimeSnapshot,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace: list[object] = []
    confirmations = _ConfirmationStore(trace)

    def corrupt_descriptor_factory(**values: object) -> object:
        del values
        return object()

    monkeypatch.setattr(
        service_module,
        "DestructiveConfirmationDescriptor",
        corrupt_descriptor_factory,
    )
    application, _, _, _, _, wall_clock = _unit_preview_application(
        runtime_snapshot,
        trace,
        confirmations=confirmations,
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert wall_clock.calls == 0
    assert confirmations.issue_calls == []
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []


@pytest.mark.asyncio
async def test_destructive_preview_uses_constructor_pinned_root_after_dependency_drift(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    activation = _activation(
        runtime_snapshot,
        request_id=REQUEST_1,
        activation_id=RESERVED_CANDIDATE,
    )
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(activation,),
        mutation_request_ids=(REQUEST_2,),
        reserved_candidates=(RESERVED_CANDIDATE,),
    )
    transport = _Transport(trace, responses=(_unit_target_artifact,))
    confirmations = _ConfirmationStore(trace)
    application = _application(
        runtime_snapshot,
        trace,
        sessions=coordinator,
        transport=transport,
        confirmations=confirmations,
        wall_clock=_WallClock(PREVIEW_NOW_MS, trace=trace),
    )
    coordinator.set_root_key_for_test("b" * 64)
    transport.set_root_key_for_test("c" * 64)

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is True
    assert outcome.request_id == REQUEST_2
    assert len(confirmations.issue_calls) == 1
    binding = confirmations.issue_calls[0][0]
    assert type(binding) is ConfirmationBinding
    assert binding.root_key == ROOT_KEY


@pytest.mark.asyncio
async def test_destructive_preview_request_id_is_local_to_each_execute_call(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    coordinator = _Coordinator(
        runtime_snapshot,
        trace,
        handshakes=(
            _activation(
                runtime_snapshot,
                request_id=REQUEST_1,
                activation_id=RESERVED_CANDIDATE,
            ),
            _activation(
                runtime_snapshot,
                request_id=REQUEST_3,
                activation_id=RESERVED_CANDIDATE_2,
            ),
        ),
        mutation_request_ids=(REQUEST_2,),
        reserved_candidates=(RESERVED_CANDIDATE, RESERVED_CANDIDATE_2),
    )
    policy = _Policy(
        trace,
        destructive_errors=(
            None,
            BridgeError(ErrorCode.POLICY_DENIED, "second preview denied"),
        ),
    )
    confirmations = _ConfirmationStore(trace)
    application = _application(
        runtime_snapshot,
        trace,
        sessions=coordinator,
        transport=_Transport(trace, responses=(_unit_target_artifact,)),
        policy=policy,
        confirmations=confirmations,
        wall_clock=_WallClock(PREVIEW_NOW_MS, trace=trace),
    )

    first = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})
    second = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert first.ok is True
    assert first.request_id == REQUEST_2
    assert second.ok is False
    assert second.request_id is None
    assert second.error == _error_payload(
        ErrorCode.POLICY_DENIED,
        "second preview denied",
        {},
    )
    assert len(confirmations.issue_calls) == 1
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []
    assert trace == [
        "reserve",
        "open",
        "status",
        "destructive_policy",
        "read",
        "clock",
        "issue",
        "close",
        "reserve",
        "open",
        "status",
        "destructive_policy",
        "close",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "expected_result", "lookup_operation", "object_kind"),
    [
        (
            "unit.delete",
            _delete_result(),
            "unit.get",
            "unit",
        ),
        (
            "mission.delete",
            _delete_result(
                guid="MISSION-1",
                name="CAP North at deletion",
                object_kind="mission",
            ),
            "mission.get",
            "mission",
        ),
    ],
    ids=["unit-intervening-activation", "mission-intervening-activation"],
)
async def test_confirmed_delete_consumes_binding_then_sends_exactly_one_delete(
    runtime_snapshot: RuntimeSnapshot,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    expected_result: DeleteResult,
    lookup_operation: str,
    object_kind: str,
) -> None:
    trace: list[object] = []
    (
        application,
        coordinator,
        transport,
        policy,
        confirmations,
        wall_clock,
        arguments,
    ) = _confirmation_application(runtime_snapshot, trace, operation=operation)
    resolve_calls: list[tuple[str, dict[str, object], dict[str, object] | None]] = []
    original_resolve = OperationRegistry.resolve_invocation

    def resolve_spy(
        registry: OperationRegistry,
        name: str,
        public_arguments: Mapping[str, object],
        trusted_enrichment: Mapping[str, object] | None = None,
    ) -> ResolvedInvocation:
        enrichment = None if trusted_enrichment is None else dict(trusted_enrichment)
        resolve_calls.append((name, dict(public_arguments), enrichment))
        if name == operation:
            trace.append("trusted_resolve")
            assert enrichment == {"confirmation_proof": CONFIRMATION_PROOF}
        return original_resolve(
            registry,
            name,
            public_arguments,
            trusted_enrichment,
        )

    original_binding = service_module.destructive_confirmation_binding

    def binding_spy(
        descriptor: DestructiveConfirmationDescriptor,
    ) -> ConfirmationBinding:
        trace.append("binding")
        return original_binding(descriptor)

    monkeypatch.setattr(OperationRegistry, "resolve_invocation", resolve_spy)
    monkeypatch.setattr(
        service_module,
        "destructive_confirmation_binding",
        binding_spy,
    )

    outcome = await application.execute(
        operation,
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    expected_binding = destructive_confirmation_binding(
        _destructive_descriptor(
            runtime_snapshot,
            operation=operation,
            arguments=arguments,
            target=(
                DestructiveTarget(guid="UNIT-1", name="Alpha", type="Aircraft")
                if operation == "unit.delete"
                else DestructiveTarget(
                    guid="MISSION-1",
                    name="CAP North",
                    type="patrol",
                )
            ),
        )
    )
    assert outcome == InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=REQUEST_3,
        ok=True,
        result=cast(dict[str, JsonValue], expected_result.model_dump(mode="json")),
        error=None,
    )
    assert trace == [
        "open",
        "clock",
        "lookup",
        "status",
        "destructive_policy",
        "read",
        "binding",
        "clock",
        "consume",
        "trusted_resolve",
        "destructive",
        "close",
    ]
    assert coordinator.reserve_calls == 0
    assert coordinator.handshake_calls == [(None, RESERVED_CANDIDATE)]
    assert coordinator.read_calls == []
    assert coordinator.reprepare_calls == []
    assert [call[1].contract.name for call in coordinator.prepare_calls] == [
        lookup_operation,
        operation,
    ]
    assert policy.calls == []
    assert len(policy.destructive_calls) == 1
    assert policy.destructive_calls[0][1] is OPERATION_REGISTRY.resolve(operation)
    assert confirmations.issue_calls == []
    assert confirmations.lookup_calls == [(CONFIRMATION_TOKEN, LOOKUP_NOW_MS)]
    assert confirmations.consume_calls == [(CONFIRMATION_TOKEN, expected_binding, CONSUME_NOW_MS)]
    assert wall_clock.calls == 2
    assert resolve_calls == [
        (
            lookup_operation,
            (
                {"unit_guid": "UNIT-1"}
                if operation == "unit.delete"
                else {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"}
            ),
            None,
        ),
        (operation, arguments, {"confirmation_proof": CONFIRMATION_PROOF}),
    ]
    assert len(transport.channel.commands) == 2
    target_command, delete_command = transport.channel.commands
    assert target_command.request_id == REQUEST_2
    assert target_command.invocation.contract.name == lookup_operation
    assert delete_command.request_id == REQUEST_3
    assert delete_command.invocation.contract.name == operation
    assert delete_command.invocation.effective_class.value == "destructive"
    assert delete_command.body.arguments["confirmation_proof"] == CONFIRMATION_PROOF
    assert delete_command.body.arguments.get("unit_guid") == (
        "UNIT-1" if object_kind == "unit" else None
    )
    assert delete_command.body.arguments.get("mission_guid") == (
        "MISSION-1" if object_kind == "mission" else None
    )
    exposed = repr(outcome.model_dump(mode="json"))
    assert CONFIRMATION_TOKEN not in exposed
    assert CONFIRMATION_PROOF not in exposed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "wrong-type",
        "root",
        "operation",
        "format",
        "candidate-not-uuid4",
    ],
)
async def test_confirmed_delete_denies_invalid_stored_binding_before_handshake(
    runtime_snapshot: RuntimeSnapshot,
    case: str,
) -> None:
    trace: list[object] = []
    valid = destructive_confirmation_binding(
        _destructive_descriptor(
            runtime_snapshot,
            operation="unit.delete",
            arguments={"unit_guid": "UNIT-1"},
            target=DestructiveTarget(guid="UNIT-1", name="Alpha", type="Aircraft"),
        )
    )
    if case == "wrong-type":
        stored: object = object()
    elif case == "root":
        stored = valid.model_copy(update={"root_key": "b" * 64})
    elif case == "operation":
        stored = valid.model_copy(update={"operation": "mission.delete"})
    elif case == "format":
        stored = valid.model_copy(
            update={"binding_format": "cmo-agent-bridge/destructive-confirmation/2"}
        )
    elif case == "candidate-not-uuid4":
        stored = valid.model_copy(
            update={"activation_id": UUID("11111111-1111-1111-8111-111111111111")}
        )
    else:
        raise AssertionError(f"unhandled stored binding case: {case}")
    (
        application,
        coordinator,
        transport,
        policy,
        confirmations,
        wall_clock,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        active_binding=stored,
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.error == _error_payload(
        ErrorCode.POLICY_DENIED,
        "confirmation denied",
        {},
    )
    assert outcome.request_id is None
    assert coordinator.handshake_calls == []
    assert coordinator.prepare_calls == []
    assert transport.channel.commands == []
    assert policy.destructive_calls == []
    assert confirmations.lookup_calls == [(CONFIRMATION_TOKEN, LOOKUP_NOW_MS)]
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 1
    assert trace == ["open", "clock", "lookup", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize("token", [CONFIRMATION_TOKEN, ""], ids=["expired-or-reused", "empty"])
async def test_confirmed_delete_lookup_denial_is_generic_and_precedes_handshake(
    runtime_snapshot: RuntimeSnapshot,
    token: str,
) -> None:
    trace: list[object] = []
    (
        application,
        coordinator,
        transport,
        _,
        confirmations,
        wall_clock,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        lookup_error=BridgeError(ErrorCode.POLICY_DENIED, "confirmation denied"),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=token,
    )

    assert outcome.error == _error_payload(
        ErrorCode.POLICY_DENIED,
        "confirmation denied",
        {},
    )
    assert outcome.request_id is None
    assert coordinator.handshake_calls == []
    assert transport.channel.commands == []
    assert confirmations.lookup_calls == [(token, LOOKUP_NOW_MS)]
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 1
    assert trace == ["open", "clock", "lookup", "close"]


@pytest.mark.asyncio
async def test_confirmed_delete_stored_lineage_mismatch_is_scenario_changed_before_read(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    stored = destructive_confirmation_binding(
        _destructive_descriptor(
            runtime_snapshot,
            operation="unit.delete",
            arguments={"unit_guid": "UNIT-1"},
            target=DestructiveTarget(guid="UNIT-1", name="Alpha", type="Aircraft"),
            lineage_id=LINEAGE_2,
        )
    )
    (
        application,
        coordinator,
        transport,
        policy,
        confirmations,
        wall_clock,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        active_binding=stored,
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.SCENARIO_CHANGED.value
    assert outcome.error["details"] == {"observed_lineage_id": str(LINEAGE_ID)}
    assert coordinator.handshake_calls == [(None, RESERVED_CANDIDATE)]
    assert coordinator.prepare_calls == []
    assert transport.channel.commands == []
    assert policy.destructive_calls == []
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 1
    assert trace == ["open", "clock", "lookup", "status", "close"]


@pytest.mark.asyncio
async def test_confirmed_delete_invalid_first_clock_precedes_lookup_and_handshake(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    (
        application,
        coordinator,
        transport,
        _,
        confirmations,
        wall_clock,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        wall_clock=_WallClock(True, trace=trace),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert coordinator.handshake_calls == []
    assert transport.channel.commands == []
    assert confirmations.lookup_calls == []
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 1
    assert trace == ["open", "clock", "close"]


@pytest.mark.asyncio
async def test_confirmed_delete_handshake_must_return_stored_candidate(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    (
        application,
        coordinator,
        transport,
        policy,
        confirmations,
        _,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        activation=_activation(
            runtime_snapshot,
            request_id=REQUEST_1,
            activation_id=ACTIVATION_1,
        ),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert coordinator.handshake_calls == [(None, RESERVED_CANDIDATE)]
    assert transport.channel.commands == []
    assert policy.destructive_calls == []
    assert confirmations.consume_calls == []


@pytest.mark.asyncio
async def test_confirmed_delete_profile_denial_precedes_target_read_and_consume(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    policy = _Policy(
        trace,
        destructive_errors=(
            BridgeError(ErrorCode.UNSUPPORTED_BUILD, "compatibility profile denied delete"),
        ),
    )
    (
        application,
        coordinator,
        transport,
        _,
        confirmations,
        wall_clock,
        arguments,
    ) = _confirmation_application(runtime_snapshot, trace, policy=policy)

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.error == _error_payload(
        ErrorCode.UNSUPPORTED_BUILD,
        "compatibility profile denied delete",
        {},
    )
    assert outcome.request_id is None
    assert len(policy.destructive_calls) == 1
    assert coordinator.prepare_calls == []
    assert transport.channel.commands == []
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 1
    assert trace == [
        "open",
        "clock",
        "lookup",
        "status",
        "destructive_policy",
        "close",
    ]


@pytest.mark.asyncio
async def test_confirmed_delete_target_failure_retains_target_id_without_consume(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    (
        application,
        _,
        transport,
        _,
        confirmations,
        wall_clock,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        responses=(_target_not_found_artifact,),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.NOT_FOUND.value
    assert len(transport.channel.commands) == 1
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["release", "arguments-and-guid", "target-name", "target-type"],
)
async def test_confirmed_delete_denies_descriptor_drift_after_target_read(
    runtime_snapshot: RuntimeSnapshot,
    case: str,
) -> None:
    trace: list[object] = []
    arguments: dict[str, JsonValue] = {"unit_guid": "UNIT-1"}
    target = DestructiveTarget(guid="UNIT-1", name="Alpha", type="Aircraft")
    release_id: str | None = None
    if case == "release":
        release_id = "f" * 64
    elif case == "arguments-and-guid":
        arguments = {"unit_guid": "UNIT-2"}
        target = DestructiveTarget(guid="UNIT-2", name="Alpha", type="Aircraft")
    elif case == "target-name":
        target = DestructiveTarget(guid="UNIT-1", name="Bravo", type="Aircraft")
    elif case == "target-type":
        target = DestructiveTarget(guid="UNIT-1", name="Alpha", type="Ship")
    else:
        raise AssertionError(f"unhandled descriptor drift case: {case}")
    stored = destructive_confirmation_binding(
        _destructive_descriptor(
            runtime_snapshot,
            operation="unit.delete",
            arguments=arguments,
            target=target,
            release_id=release_id,
        )
    )
    (
        application,
        _,
        transport,
        _,
        confirmations,
        wall_clock,
        current_arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        active_binding=stored,
    )

    outcome = await application.execute(
        "unit.delete",
        current_arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.error == _error_payload(
        ErrorCode.POLICY_DENIED,
        "confirmation denied",
        {},
    )
    assert outcome.request_id == REQUEST_2
    assert len(transport.channel.commands) == 1
    assert transport.channel.commands[0].invocation.contract.name == "unit.get"
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 1
    assert trace == [
        "open",
        "clock",
        "lookup",
        "status",
        "destructive_policy",
        "read",
        "close",
    ]


@pytest.mark.asyncio
async def test_confirmed_delete_invalid_second_clock_retains_target_id_without_consume(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    (
        application,
        _,
        transport,
        _,
        confirmations,
        wall_clock,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        wall_clock=_WallClock(LOOKUP_NOW_MS, True, trace=trace),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert len(transport.channel.commands) == 1
    assert confirmations.consume_calls == []
    assert wall_clock.calls == 2
    assert trace == [
        "open",
        "clock",
        "lookup",
        "status",
        "destructive_policy",
        "read",
        "clock",
        "close",
    ]


@pytest.mark.asyncio
async def test_confirmed_delete_consume_cas_denial_retains_target_id_and_sends_nothing(
    runtime_snapshot: RuntimeSnapshot,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace: list[object] = []
    (
        application,
        _,
        transport,
        _,
        confirmations,
        _,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        consume_error=BridgeError(ErrorCode.POLICY_DENIED, "confirmation denied"),
    )
    original_resolve = OperationRegistry.resolve_invocation

    def forbid_trusted_resolve(
        registry: OperationRegistry,
        name: str,
        public_arguments: Mapping[str, object],
        trusted_enrichment: Mapping[str, object] | None = None,
    ) -> ResolvedInvocation:
        if name == "unit.delete":
            raise AssertionError("delete resolved before successful consume")
        return original_resolve(registry, name, public_arguments, trusted_enrichment)

    monkeypatch.setattr(OperationRegistry, "resolve_invocation", forbid_trusted_resolve)

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.error == _error_payload(
        ErrorCode.POLICY_DENIED,
        "confirmation denied",
        {},
    )
    assert outcome.request_id == REQUEST_2
    assert len(confirmations.consume_calls) == 1
    assert len(transport.channel.commands) == 1
    assert transport.channel.commands[0].invocation.contract.name == "unit.get"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "proof",
    [
        object(),
        _StringSubclass(CONFIRMATION_PROOF),
        CONFIRMATION_PROOF.upper(),
        "b" * 64,
    ],
    ids=["wrong-type", "string-subclass", "uppercase", "token-hash-mismatch"],
)
async def test_confirmed_delete_rejects_corrupt_consumed_proof_before_trusted_resolve(
    runtime_snapshot: RuntimeSnapshot,
    monkeypatch: pytest.MonkeyPatch,
    proof: object,
) -> None:
    trace: list[object] = []
    (
        application,
        _,
        transport,
        _,
        confirmations,
        _,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        consume_result=proof,
    )
    original_resolve = OperationRegistry.resolve_invocation

    def forbid_trusted_resolve(
        registry: OperationRegistry,
        name: str,
        public_arguments: Mapping[str, object],
        trusted_enrichment: Mapping[str, object] | None = None,
    ) -> ResolvedInvocation:
        if name == "unit.delete":
            raise AssertionError("delete resolved with a corrupt proof")
        return original_resolve(registry, name, public_arguments, trusted_enrichment)

    monkeypatch.setattr(OperationRegistry, "resolve_invocation", forbid_trusted_resolve)

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert len(confirmations.consume_calls) == 1
    assert len(transport.channel.commands) == 1
    assert transport.channel.commands[0].invocation.contract.name == "unit.get"


@pytest.mark.asyncio
async def test_confirmed_delete_rejects_noncanonical_token_after_consume(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    token = "not-canonical"
    (
        application,
        _,
        transport,
        _,
        confirmations,
        _,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        consume_result=hashlib.sha256(token.encode("utf-8")).hexdigest(),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=token,
    )

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert confirmations.lookup_calls == [(token, LOOKUP_NOW_MS)]
    assert len(confirmations.consume_calls) == 1
    assert len(transport.channel.commands) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["resolve-error", "wrong-invocation"])
async def test_confirmed_delete_post_consume_trusted_resolution_failure_keeps_target_id(
    runtime_snapshot: RuntimeSnapshot,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    trace: list[object] = []
    (
        application,
        _,
        transport,
        _,
        confirmations,
        _,
        arguments,
    ) = _confirmation_application(runtime_snapshot, trace)
    original_resolve = OperationRegistry.resolve_invocation

    def corrupt_trusted_resolve(
        registry: OperationRegistry,
        name: str,
        public_arguments: Mapping[str, object],
        trusted_enrichment: Mapping[str, object] | None = None,
    ) -> ResolvedInvocation:
        if name != "unit.delete":
            return original_resolve(registry, name, public_arguments, trusted_enrichment)
        if case == "resolve-error":
            raise BridgeError(ErrorCode.STATE_CONFLICT, "trusted resolve failed")
        return original_resolve(registry, "unit.get", {"unit_guid": "UNIT-1"})

    monkeypatch.setattr(OperationRegistry, "resolve_invocation", corrupt_trusted_resolve)

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] in {
        ErrorCode.STATE_CONFLICT.value,
        ErrorCode.PROTOCOL_ERROR.value,
    }
    assert len(confirmations.consume_calls) == 1
    assert len(transport.channel.commands) == 1
    assert transport.channel.commands[0].invocation.contract.name == "unit.get"


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["semantic-command", "reused-target-id"])
async def test_confirmed_delete_invalid_delete_command_keeps_target_id_after_consume(
    runtime_snapshot: RuntimeSnapshot,
    case: str,
) -> None:
    trace: list[object] = []
    prepared = 0

    def command_factory(
        activation: SessionActivation,
        invocation: ResolvedInvocation,
        timeout_seconds: float,
    ) -> ExchangeCommand:
        nonlocal prepared
        prepared += 1
        request_id = REQUEST_2 if prepared == 1 or case == "reused-target-id" else REQUEST_3
        command = _exchange_command(
            invocation,
            runtime_snapshot,
            activation,
            request_id,
            timeout_seconds,
        )
        if prepared == 2 and case == "semantic-command":
            return replace(command, timeout=timeout_seconds + 1.0)
        return command

    (
        application,
        coordinator,
        transport,
        _,
        confirmations,
        _,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        mutation_command_factory=command_factory,
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert len(confirmations.consume_calls) == 1
    assert len(coordinator.prepare_calls) == 2
    assert len(transport.channel.commands) == 1
    assert transport.channel.commands[0].request_id == REQUEST_2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "delete_response",
    [
        _wrong_guid_delete_artifact,
        _wrong_kind_delete_artifact,
        _malformed_delete_artifact,
    ],
    ids=["wrong-guid", "wrong-object-kind", "malformed-result"],
)
async def test_confirmed_delete_result_corruption_keeps_delete_request_id(
    runtime_snapshot: RuntimeSnapshot,
    delete_response: Callable[[ExchangeCommand], ResponseArtifact],
) -> None:
    trace: list[object] = []
    (
        application,
        _,
        transport,
        _,
        confirmations,
        _,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        responses=(_unit_target_artifact, delete_response),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_3
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert len(confirmations.consume_calls) == 1
    assert len(transport.channel.commands) == 2
    assert transport.channel.commands[1].invocation.contract.name == "unit.delete"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "delete_response",
    [_destructive_error_with_evidence, _destructive_error_without_evidence],
    ids=["mutation-not-started", "indeterminate-settlement"],
)
async def test_confirmed_delete_sanitizes_correlated_engine_failure(
    runtime_snapshot: RuntimeSnapshot,
    delete_response: Callable[[ExchangeCommand], ResponseArtifact],
) -> None:
    trace: list[object] = []
    (
        application,
        _,
        transport,
        _,
        confirmations,
        _,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        responses=(_unit_target_artifact, delete_response),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.error == _error_payload(
        ErrorCode.CMO_LUA_ERROR,
        "destructive operation failed",
        {},
    )
    assert outcome.request_id == REQUEST_3
    assert len(confirmations.consume_calls) == 1
    assert len(transport.channel.commands) == 2
    exposed = repr(outcome.model_dump(mode="json"))
    assert CONFIRMATION_TOKEN not in exposed
    assert CONFIRMATION_PROOF not in exposed


@pytest.mark.asyncio
async def test_confirmed_delete_rejects_destructive_rejected_settlement_without_evidence(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    (
        application,
        _,
        transport,
        _,
        confirmations,
        _,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        responses=(
            _unit_target_artifact,
            _destructive_error_invalid_settlement,
        ),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_3
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    exposed = repr(outcome.model_dump(mode="json"))
    assert CONFIRMATION_TOKEN not in exposed
    assert CONFIRMATION_PROOF not in exposed
    assert len(confirmations.consume_calls) == 1
    assert len(transport.channel.commands) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["public-model", "wire-model", "effective-class", "contract"],
)
async def test_confirmed_delete_rejects_isolated_trusted_invocation_drift_after_consume(
    runtime_snapshot: RuntimeSnapshot,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    trace: list[object] = []
    (
        application,
        _,
        transport,
        _,
        confirmations,
        _,
        arguments,
    ) = _confirmation_application(runtime_snapshot, trace)
    original_resolve = OperationRegistry.resolve_invocation

    def forge_trusted_invocation(
        registry: OperationRegistry,
        name: str,
        public_arguments: Mapping[str, object],
        trusted_enrichment: Mapping[str, object] | None = None,
    ) -> ResolvedInvocation:
        invocation = original_resolve(
            registry,
            name,
            public_arguments,
            trusted_enrichment,
        )
        if name != "unit.delete":
            return invocation
        if case == "public-model":
            return _forge_resolved_invocation(
                invocation,
                public_arguments=_ForgedDeleteUnitArgs(unit_guid="UNIT-1"),
            )
        if case == "wire-model":
            return _forge_resolved_invocation(
                invocation,
                wire_arguments=_ForgedConfirmedDeleteUnitWireArgs(
                    unit_guid="UNIT-1",
                    confirmation_proof=CONFIRMATION_PROOF,
                ),
            )
        if case == "effective-class":
            return _forge_resolved_invocation(
                invocation,
                effective_class=OperationClass.MUTATION,
            )
        if case == "contract":
            return _forge_resolved_invocation(
                invocation,
                contract=replace(
                    invocation.contract,
                    expose_mcp=not invocation.contract.expose_mcp,
                ),
            )
        raise AssertionError(f"unhandled trusted invocation drift: {case}")

    monkeypatch.setattr(OperationRegistry, "resolve_invocation", forge_trusted_invocation)

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert len(confirmations.consume_calls) == 1
    assert len(transport.channel.commands) == 1
    assert transport.channel.commands[0].invocation.contract.name == "unit.get"


@pytest.mark.asyncio
async def test_preview_recovers_one_exact_activation_mismatch_with_same_candidate(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    application, coordinator, transport, policy, confirmations, wall_clock = (
        _unit_preview_application(
            runtime_snapshot,
            trace,
            handshakes=(
                _activation(
                    runtime_snapshot,
                    request_id=REQUEST_1,
                    activation_id=RESERVED_CANDIDATE,
                ),
                _activation(
                    runtime_snapshot,
                    request_id=REQUEST_3,
                    activation_id=RESERVED_CANDIDATE,
                ),
            ),
            responses=(
                _target_activation_mismatch_artifact,
                _unit_target_artifact,
            ),
            mutation_request_ids=(REQUEST_2, REQUEST_4),
        )
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is True
    assert outcome.request_id == REQUEST_4
    assert coordinator.reserve_calls == 1
    assert coordinator.handshake_calls == [
        (None, RESERVED_CANDIDATE),
        (None, RESERVED_CANDIDATE),
    ]
    assert coordinator.read_calls == []
    assert coordinator.reprepare_calls == []
    assert [call[1].contract.name for call in coordinator.prepare_calls] == [
        "unit.get",
        "unit.get",
    ]
    assert len(policy.destructive_calls) == 2
    assert [command.request_id for command in transport.channel.commands] == [
        REQUEST_2,
        REQUEST_4,
    ]
    assert len(confirmations.issue_calls) == 1
    assert wall_clock.calls == 1
    assert trace == [
        "reserve",
        "open",
        "status",
        "destructive_policy",
        "read",
        "status",
        "destructive_policy",
        "read",
        "clock",
        "issue",
        "close",
    ]


@pytest.mark.asyncio
async def test_preview_second_activation_mismatch_is_returned_without_a_third_read(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    application, coordinator, transport, _, confirmations, wall_clock = _unit_preview_application(
        runtime_snapshot,
        trace,
        handshakes=(
            _activation(
                runtime_snapshot,
                request_id=REQUEST_1,
                activation_id=RESERVED_CANDIDATE,
            ),
            _activation(
                runtime_snapshot,
                request_id=REQUEST_3,
                activation_id=RESERVED_CANDIDATE,
            ),
        ),
        responses=(
            _target_activation_mismatch_artifact,
            _target_activation_mismatch_artifact,
        ),
        mutation_request_ids=(REQUEST_2, REQUEST_4),
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_4
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.ACTIVATION_MISMATCH.value
    assert len(coordinator.handshake_calls) == 2
    assert len(coordinator.prepare_calls) == 2
    assert [command.request_id for command in transport.channel.commands] == [
        REQUEST_2,
        REQUEST_4,
    ]
    assert confirmations.issue_calls == []
    assert wall_clock.calls == 0


@pytest.mark.asyncio
async def test_confirmation_recovers_target_once_then_consumes_and_deletes(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    (
        application,
        coordinator,
        transport,
        policy,
        confirmations,
        wall_clock,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        handshakes=(
            _activation(
                runtime_snapshot,
                request_id=REQUEST_1,
                activation_id=RESERVED_CANDIDATE,
            ),
            _activation(
                runtime_snapshot,
                request_id=REQUEST_3,
                activation_id=RESERVED_CANDIDATE,
            ),
        ),
        responses=(
            _target_activation_mismatch_artifact,
            _unit_target_artifact,
            _unit_delete_artifact,
        ),
        mutation_request_ids=(REQUEST_2, REQUEST_4, REQUEST_5),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is True
    assert outcome.request_id == REQUEST_5
    assert coordinator.handshake_calls == [
        (None, RESERVED_CANDIDATE),
        (None, RESERVED_CANDIDATE),
    ]
    assert [call[1].contract.name for call in coordinator.prepare_calls] == [
        "unit.get",
        "unit.get",
        "unit.delete",
    ]
    assert len(policy.destructive_calls) == 2
    assert [command.request_id for command in transport.channel.commands] == [
        REQUEST_2,
        REQUEST_4,
        REQUEST_5,
    ]
    assert len(confirmations.consume_calls) == 1
    assert wall_clock.calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["status-reuses-status", "status-reuses-read", "read-reuses-status", "read-reuses-read"],
)
async def test_preview_recovery_rejects_cross_attempt_identity_reuse(
    runtime_snapshot: RuntimeSnapshot,
    case: str,
) -> None:
    trace: list[object] = []
    second_status_id = (
        REQUEST_1
        if case == "status-reuses-status"
        else REQUEST_2
        if case == "status-reuses-read"
        else REQUEST_3
    )
    second_read_id = REQUEST_1 if case == "read-reuses-status" else REQUEST_2
    if case.startswith("status-"):
        second_read_id = REQUEST_4
    application, coordinator, transport, _, confirmations, _ = _unit_preview_application(
        runtime_snapshot,
        trace,
        handshakes=(
            _activation(
                runtime_snapshot,
                request_id=REQUEST_1,
                activation_id=RESERVED_CANDIDATE,
            ),
            _activation(
                runtime_snapshot,
                request_id=second_status_id,
                activation_id=RESERVED_CANDIDATE,
            ),
        ),
        responses=(
            _target_activation_mismatch_artifact,
            _unit_target_artifact,
        ),
        mutation_request_ids=(REQUEST_2, second_read_id),
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert len(coordinator.handshake_calls) == 2
    assert len(transport.channel.commands) == 1
    assert transport.channel.commands[0].request_id == REQUEST_2
    assert confirmations.issue_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["handshake", "policy"])
async def test_preview_recovery_pre_read_failure_retains_first_target_id(
    runtime_snapshot: RuntimeSnapshot,
    stage: str,
) -> None:
    trace: list[object] = []
    policy = (
        _Policy(
            trace,
            destructive_errors=(
                None,
                BridgeError(ErrorCode.POLICY_DENIED, "recovery policy denied"),
            ),
        )
        if stage == "policy"
        else _Policy(trace)
    )
    second_handshake: SessionActivation | BaseException = (
        BridgeError(ErrorCode.BRIDGE_UNRESPONSIVE, "recovery status failed")
        if stage == "handshake"
        else _activation(
            runtime_snapshot,
            request_id=REQUEST_3,
            activation_id=RESERVED_CANDIDATE,
        )
    )
    application, coordinator, transport, _, confirmations, _ = _unit_preview_application(
        runtime_snapshot,
        trace,
        handshakes=(
            _activation(
                runtime_snapshot,
                request_id=REQUEST_1,
                activation_id=RESERVED_CANDIDATE,
            ),
            second_handshake,
        ),
        responses=(_target_activation_mismatch_artifact,),
        mutation_request_ids=(REQUEST_2, REQUEST_4),
        policy=policy,
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == (
        ErrorCode.BRIDGE_UNRESPONSIVE.value
        if stage == "handshake"
        else ErrorCode.POLICY_DENIED.value
    )
    assert len(coordinator.handshake_calls) == 2
    assert len(transport.channel.commands) == 1
    assert confirmations.issue_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected_request_id"),
    [
        (_foreign_target_artifact, None),
        (_constructed_invalid_artifact, None),
        (_target_correlated_malformed_artifact, REQUEST_2),
    ],
    ids=["foreign", "model-corrupt-foreign", "correlated-malformed"],
)
async def test_preview_target_artifact_request_id_clearing_is_precise(
    runtime_snapshot: RuntimeSnapshot,
    response: Callable[[ExchangeCommand], ResponseArtifact],
    expected_request_id: UUID | None,
) -> None:
    trace: list[object] = []
    application, coordinator, transport, _, confirmations, wall_clock = _unit_preview_application(
        runtime_snapshot,
        trace,
        responses=(response,),
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id == expected_request_id
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert len(coordinator.handshake_calls) == 1
    assert len(transport.channel.commands) == 1
    assert confirmations.issue_calls == []
    assert wall_clock.calls == 0


@pytest.mark.asyncio
async def test_preview_foreign_second_target_artifact_clears_recovered_id(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    application, coordinator, transport, _, confirmations, _ = _unit_preview_application(
        runtime_snapshot,
        trace,
        handshakes=(
            _activation(
                runtime_snapshot,
                request_id=REQUEST_1,
                activation_id=RESERVED_CANDIDATE,
            ),
            _activation(
                runtime_snapshot,
                request_id=REQUEST_3,
                activation_id=RESERVED_CANDIDATE,
            ),
        ),
        responses=(
            _target_activation_mismatch_artifact,
            _foreign_target_artifact,
        ),
        mutation_request_ids=(REQUEST_2, REQUEST_4),
    )

    outcome = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})

    assert outcome.ok is False
    assert outcome.request_id is None
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.PROTOCOL_ERROR.value
    assert len(coordinator.handshake_calls) == 2
    assert len(transport.channel.commands) == 2
    assert confirmations.issue_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("delete_response", "expected_code", "expected_request_id"),
    [
        (_destructive_error_with_evidence, ErrorCode.CMO_LUA_ERROR, REQUEST_3),
        (_destructive_error_without_evidence, ErrorCode.CMO_LUA_ERROR, REQUEST_3),
        (_destructive_activation_mismatch, ErrorCode.ACTIVATION_MISMATCH, REQUEST_3),
        (_destructive_indeterminate, ErrorCode.INDETERMINATE_OUTCOME, REQUEST_3),
        (_destructive_ordinary_error, ErrorCode.NOT_FOUND, REQUEST_3),
        (
            BridgeError(ErrorCode.REQUEST_TIMEOUT, "delete transport timeout"),
            ErrorCode.REQUEST_TIMEOUT,
            REQUEST_3,
        ),
        (_foreign_delete_artifact, ErrorCode.PROTOCOL_ERROR, None),
        (_constructed_invalid_artifact, ErrorCode.PROTOCOL_ERROR, None),
    ],
    ids=[
        "mutation-not-started",
        "engine-failure",
        "activation-mismatch",
        "indeterminate",
        "ordinary-error",
        "transport-error",
        "foreign",
        "model-corrupt-foreign",
    ],
)
async def test_every_delete_failure_path_has_exactly_one_delete_and_no_retry(
    runtime_snapshot: RuntimeSnapshot,
    delete_response: Callable[[ExchangeCommand], ResponseArtifact] | BaseException,
    expected_code: ErrorCode,
    expected_request_id: UUID | None,
) -> None:
    trace: list[object] = []
    (
        application,
        coordinator,
        transport,
        _,
        confirmations,
        _,
        arguments,
    ) = _confirmation_application(
        runtime_snapshot,
        trace,
        responses=(_unit_target_artifact, delete_response),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id == expected_request_id
    assert outcome.error is not None
    assert outcome.error["code"] == expected_code.value
    assert len(confirmations.consume_calls) == 1
    assert len(coordinator.handshake_calls) == 1
    assert len(coordinator.prepare_calls) == 2
    assert [command.invocation.contract.name for command in transport.channel.commands] == [
        "unit.get",
        "unit.delete",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "delete_response",
    [
        _destructive_top_level_echo,
        _destructive_nested_object_echo,
        _destructive_nested_array_echo,
        _destructive_message_echo,
    ],
    ids=["top-level", "nested-object", "nested-array", "message"],
)
async def test_destructive_failure_sanitizer_removes_every_echo_shape(
    runtime_snapshot: RuntimeSnapshot,
    delete_response: Callable[[ExchangeCommand], ResponseArtifact],
) -> None:
    trace: list[object] = []
    application, _, transport, _, confirmations, _, arguments = _confirmation_application(
        runtime_snapshot,
        trace,
        responses=(_unit_target_artifact, delete_response),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.error == _error_payload(
        ErrorCode.CMO_LUA_ERROR,
        "destructive operation failed",
        {},
    )
    assert outcome.request_id == REQUEST_3
    assert len(confirmations.consume_calls) == 1
    assert len(transport.channel.commands) == 2
    exposed = repr(outcome.model_dump(mode="json"))
    for secret in (
        CONFIRMATION_TOKEN,
        CONFIRMATION_PROOF,
        "confirmation_proof",
        "engine echoed",
    ):
        assert secret not in exposed


@pytest.mark.asyncio
async def test_confirmation_cancellation_before_consume_propagates_and_leaves_token_unused(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    transport = _BlockingDomainTransport(
        trace,
        block_operation="unit.get",
    )
    application, coordinator, _, _, confirmations, _, arguments = _confirmation_application(
        runtime_snapshot,
        trace,
        transport=transport,
    )

    task = asyncio.create_task(
        application.execute(
            "unit.delete",
            arguments,
            confirmation_token=CONFIRMATION_TOKEN,
        )
    )
    await transport.channel.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert confirmations.lookup_calls == [(CONFIRMATION_TOKEN, LOOKUP_NOW_MS)]
    assert confirmations.consume_calls == []
    assert len(coordinator.handshake_calls) == 1
    assert len(transport.channel.commands) == 1


@pytest.mark.asyncio
async def test_confirmation_post_consume_exception_propagates_with_token_burned(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    failure = RuntimeError("post-consume delete failure")
    application, coordinator, transport, _, confirmations, _, arguments = _confirmation_application(
        runtime_snapshot,
        trace,
        responses=(_unit_target_artifact, failure),
    )

    with pytest.raises(type(failure)):
        await application.execute(
            "unit.delete",
            arguments,
            confirmation_token=CONFIRMATION_TOKEN,
        )

    assert len(confirmations.consume_calls) == 1
    assert len(coordinator.handshake_calls) == 1
    assert len(coordinator.prepare_calls) == 2
    assert [command.invocation.contract.name for command in transport.channel.commands] == [
        "unit.get",
        "unit.delete",
    ]


@pytest.mark.asyncio
async def test_confirmation_real_task_cancellation_during_delete_burns_token_without_retry(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    transport = _BlockingDomainTransport(
        trace,
        block_operation="unit.delete",
        responses=(_unit_target_artifact,),
    )
    application, coordinator, _, _, confirmations, _, arguments = _confirmation_application(
        runtime_snapshot,
        trace,
        transport=transport,
    )
    task = asyncio.create_task(
        application.execute(
            "unit.delete",
            arguments,
            confirmation_token=CONFIRMATION_TOKEN,
        )
    )
    await transport.channel.started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(confirmations.consume_calls) == 1
    assert len(coordinator.handshake_calls) == 1
    assert len(coordinator.prepare_calls) == 2
    assert [command.invocation.contract.name for command in transport.channel.commands] == [
        "unit.get",
        "unit.delete",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stage", "expected_code", "expected_request_id", "consumed"),
    [
        ("lookup", ErrorCode.STATE_CONFLICT, None, False),
        ("policy", ErrorCode.UNSUPPORTED_BUILD, None, False),
        ("target", ErrorCode.NOT_FOUND, REQUEST_2, False),
        ("delete-transport", ErrorCode.REQUEST_TIMEOUT, REQUEST_3, True),
    ],
)
async def test_confirmation_sanitizes_token_echoes_from_every_bridge_error_stage(
    runtime_snapshot: RuntimeSnapshot,
    stage: str,
    expected_code: ErrorCode,
    expected_request_id: UUID | None,
    consumed: bool,
) -> None:
    trace: list[object] = []
    malicious_details = {
        f"key-{CONFIRMATION_TOKEN}": [
            {"nested": f"value-{CONFIRMATION_TOKEN}"},
        ]
    }
    lookup_error = (
        BridgeError(
            ErrorCode.STATE_CONFLICT,
            f"lookup echoed {CONFIRMATION_TOKEN}",
            malicious_details,
        )
        if stage == "lookup"
        else None
    )
    policy = (
        _Policy(
            trace,
            destructive_errors=(
                BridgeError(
                    ErrorCode.UNSUPPORTED_BUILD,
                    f"policy echoed {CONFIRMATION_TOKEN}",
                    malicious_details,
                ),
            ),
        )
        if stage == "policy"
        else None
    )
    if stage == "target":
        responses: tuple[Callable[[ExchangeCommand], ResponseArtifact] | BaseException, ...] = (
            _target_token_echo_artifact,
        )
    elif stage == "delete-transport":
        responses = (
            _unit_target_artifact,
            BridgeError(
                ErrorCode.REQUEST_TIMEOUT,
                f"delete transport echoed {CONFIRMATION_TOKEN} {CONFIRMATION_PROOF}",
                {
                    f"proof-{CONFIRMATION_PROOF}": [
                        CONFIRMATION_TOKEN,
                        {"nested": CONFIRMATION_PROOF},
                    ]
                },
            ),
        )
    else:
        responses = (_unit_target_artifact, _unit_delete_artifact)
    application, _, _, _, confirmations, _, arguments = _confirmation_application(
        runtime_snapshot,
        trace,
        lookup_error=lookup_error,
        policy=policy,
        responses=responses,
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id == expected_request_id
    assert outcome.error is not None
    assert outcome.error["code"] == expected_code.value
    exposed = repr(outcome.model_dump(mode="json"))
    assert CONFIRMATION_TOKEN not in exposed
    if consumed:
        assert CONFIRMATION_PROOF not in exposed
    assert len(confirmations.consume_calls) == (1 if consumed else 0)


@pytest.mark.asyncio
async def test_confirmation_sanitizes_post_consume_trusted_resolve_error(
    runtime_snapshot: RuntimeSnapshot,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace: list[object] = []
    application, _, transport, _, confirmations, _, arguments = _confirmation_application(
        runtime_snapshot,
        trace,
    )
    original_resolve = OperationRegistry.resolve_invocation

    def malicious_resolve(
        registry: OperationRegistry,
        name: str,
        public_arguments: Mapping[str, object],
        trusted_enrichment: Mapping[str, object] | None = None,
    ) -> ResolvedInvocation:
        if name == "unit.delete":
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                f"resolve echoed {CONFIRMATION_TOKEN} {CONFIRMATION_PROOF}",
                {
                    f"token-{CONFIRMATION_TOKEN}": [
                        {"proof": CONFIRMATION_PROOF},
                    ]
                },
            )
        return original_resolve(registry, name, public_arguments, trusted_enrichment)

    monkeypatch.setattr(OperationRegistry, "resolve_invocation", malicious_resolve)

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.ok is False
    assert outcome.request_id == REQUEST_2
    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.STATE_CONFLICT.value
    exposed = repr(outcome.model_dump(mode="json"))
    assert CONFIRMATION_TOKEN not in exposed
    assert CONFIRMATION_PROOF not in exposed
    assert len(confirmations.consume_calls) == 1
    assert len(transport.channel.commands) == 1


@pytest.mark.asyncio
async def test_empty_confirmation_token_skips_redaction_and_preserves_safe_error(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    application, _, _, _, _, _, arguments = _confirmation_application(
        runtime_snapshot,
        trace,
        lookup_error=BridgeError(
            ErrorCode.STATE_CONFLICT,
            "safe lookup failure",
            {"safe": ["unchanged"]},
        ),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token="",
    )

    assert outcome.error == _error_payload(
        ErrorCode.STATE_CONFLICT,
        "safe lookup failure",
        {"safe": ["unchanged"]},
    )


@pytest.mark.asyncio
async def test_confirmation_redaction_cannot_reexpose_token_as_its_marker(
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    trace: list[object] = []
    token = "[REDACTED]"
    application, _, _, _, _, _, arguments = _confirmation_application(
        runtime_snapshot,
        trace,
        lookup_error=BridgeError(
            ErrorCode.STATE_CONFLICT,
            f"lookup echoed {token}",
            {f"key-{token}": [token]},
        ),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=token,
    )

    assert outcome.error is not None
    assert outcome.error["code"] == ErrorCode.STATE_CONFLICT.value
    assert token not in repr(outcome.model_dump(mode="json"))


@pytest.mark.asyncio
async def test_preconsume_denial_does_not_derive_confirmation_proof(
    runtime_snapshot: RuntimeSnapshot,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace: list[object] = []
    valid = destructive_confirmation_binding(
        _destructive_descriptor(
            runtime_snapshot,
            operation="unit.delete",
            arguments={"unit_guid": "UNIT-1"},
            target=DestructiveTarget(guid="UNIT-1", name="Alpha", type="Aircraft"),
        )
    )
    invalid_root = valid.model_copy(update={"root_key": "b" * 64})
    application, _, _, _, confirmations, _, arguments = _confirmation_application(
        runtime_snapshot,
        trace,
        active_binding=invalid_root,
    )

    def forbidden_sha256(value: object = b"") -> object:
        del value
        raise AssertionError("confirmation proof was derived before consume")

    monkeypatch.setattr(
        service_module,
        "hashlib",
        SimpleNamespace(sha256=forbidden_sha256),
    )

    outcome = await application.execute(
        "unit.delete",
        arguments,
        confirmation_token=CONFIRMATION_TOKEN,
    )

    assert outcome.error == _error_payload(
        ErrorCode.POLICY_DENIED,
        "confirmation denied",
        {},
    )
    assert confirmations.consume_calls == []


@pytest.mark.asyncio
async def test_real_root_lock_serializes_same_root_confirmation_applications(
    runtime_snapshot: RuntimeSnapshot,
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "state.sqlite3"
    issuer = ConfirmationTokenStore(
        StateDatabase(database_path),
        token_factory=_fixed_confirmation_token_factory,
    )
    binding = destructive_confirmation_binding(
        _destructive_descriptor(
            runtime_snapshot,
            operation="unit.delete",
            arguments={"unit_guid": "UNIT-1"},
            target=DestructiveTarget(guid="UNIT-1", name="Alpha", type="Aircraft"),
        )
    )
    issued = issuer.issue(binding, now_ms=100)
    first_store = _DelegatingConfirmationStore(ConfirmationTokenStore(StateDatabase(database_path)))
    second_store = _DelegatingConfirmationStore(
        ConfirmationTokenStore(StateDatabase(database_path))
    )
    first_trace: list[object] = []
    second_trace: list[object] = []
    lock_path = tmp_path / "root.lock"
    first_transport = _RootLockedTestTransport(
        first_trace,
        lock_path,
        hold_target=True,
    )
    second_transport = _RootLockedTestTransport(
        second_trace,
        lock_path,
        hold_target=False,
    )
    first_application = _application(
        runtime_snapshot,
        first_trace,
        transport=first_transport,
        sessions=_Coordinator(
            runtime_snapshot,
            first_trace,
            handshakes=(
                _activation(
                    runtime_snapshot,
                    request_id=REQUEST_1,
                    activation_id=RESERVED_CANDIDATE,
                ),
            ),
            mutation_request_ids=(REQUEST_2, REQUEST_3),
        ),
        policy=_Policy(first_trace),
        confirmations=first_store,
        wall_clock=_WallClock(200, 300),
    )
    second_coordinator = _Coordinator(runtime_snapshot, second_trace)
    second_application = _application(
        runtime_snapshot,
        second_trace,
        transport=second_transport,
        sessions=second_coordinator,
        policy=_Policy(second_trace),
        confirmations=second_store,
        wall_clock=_WallClock(400),
    )
    arguments: dict[str, JsonValue] = {"unit_guid": "UNIT-1"}

    first_task = asyncio.create_task(
        first_application.execute(
            "unit.delete",
            arguments,
            confirmation_token=issued.token,
        )
    )
    await first_transport.channel.target_started.wait()
    second_task = asyncio.create_task(
        second_application.execute(
            "unit.delete",
            arguments,
            confirmation_token=issued.token,
        )
    )
    await second_transport.attempted.wait()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(second_transport.entered.wait(), timeout=0.05)
    assert second_store.lookup_calls == []

    first_transport.channel.release_target.set()
    first, second = await asyncio.gather(first_task, second_task)

    assert first.ok is True
    assert first.request_id == REQUEST_3
    assert second.error == _error_payload(
        ErrorCode.POLICY_DENIED,
        "confirmation denied",
        {},
    )
    assert second.request_id is None
    assert second_transport.entered.is_set()
    assert first_store.lookup_calls == [(issued.token, 200)]
    assert len(first_store.consume_calls) == 1
    assert second_store.lookup_calls == [(issued.token, 400)]
    assert second_store.consume_calls == []
    assert second_coordinator.handshake_calls == []
    all_commands = first_transport.channel.commands + second_transport.channel.commands
    assert [command.invocation.contract.name for command in all_commands] == [
        "unit.get",
        "unit.delete",
    ]


def test_forced_confirmation_store_two_consumer_cas_has_one_winner(
    runtime_snapshot: RuntimeSnapshot,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "state.sqlite3"
    issuer = ConfirmationTokenStore(
        StateDatabase(database_path),
        token_factory=_fixed_confirmation_token_factory,
    )
    binding = destructive_confirmation_binding(
        _destructive_descriptor(
            runtime_snapshot,
            operation="unit.delete",
            arguments={"unit_guid": "UNIT-1"},
            target=DestructiveTarget(guid="UNIT-1", name="Alpha", type="Aircraft"),
        )
    )
    issued = issuer.issue(binding, now_ms=100)
    barrier = Barrier(2)
    stores = (
        _BarrierConfirmationStore(
            ConfirmationTokenStore(StateDatabase(database_path)),
            barrier,
        ),
        _BarrierConfirmationStore(
            ConfirmationTokenStore(StateDatabase(database_path)),
            barrier,
        ),
    )
    trusted_resolves: list[int] = []
    resolve_lock = Lock()
    original_resolve = OperationRegistry.resolve_invocation

    def resolve_spy(
        registry: OperationRegistry,
        name: str,
        public_arguments: Mapping[str, object],
        trusted_enrichment: Mapping[str, object] | None = None,
    ) -> ResolvedInvocation:
        if name == "unit.delete":
            with resolve_lock:
                trusted_resolves.append(get_ident())
        return original_resolve(registry, name, public_arguments, trusted_enrichment)

    monkeypatch.setattr(OperationRegistry, "resolve_invocation", resolve_spy)

    async def run(index: int) -> tuple[InvocationOutcome, list[ExchangeCommand]]:
        trace: list[object] = []
        transport = _Transport(
            trace,
            responses=(_unit_target_artifact, _unit_delete_artifact),
        )
        application = _application(
            runtime_snapshot,
            trace,
            transport=transport,
            sessions=_Coordinator(
                runtime_snapshot,
                trace,
                handshakes=(
                    _activation(
                        runtime_snapshot,
                        request_id=REQUEST_1,
                        activation_id=RESERVED_CANDIDATE,
                    ),
                ),
                mutation_request_ids=(REQUEST_2, REQUEST_3),
            ),
            policy=_Policy(trace),
            confirmations=stores[index],
            wall_clock=_WallClock(200, 300),
        )
        outcome = await application.execute(
            "unit.delete",
            {"unit_guid": "UNIT-1"},
            confirmation_token=issued.token,
        )
        return outcome, transport.channel.commands

    def run_in_thread(index: int) -> tuple[InvocationOutcome, list[ExchangeCommand]]:
        return asyncio.run(run(index))

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run_in_thread, index) for index in range(2)]
        results = [future.result(timeout=10) for future in futures]

    outcomes = [outcome for outcome, _ in results]
    assert sum(outcome.ok for outcome in outcomes) == 1
    denied = next(outcome for outcome in outcomes if not outcome.ok)
    assert denied.error == _error_payload(
        ErrorCode.POLICY_DENIED,
        "confirmation denied",
        {},
    )
    assert len(trusted_resolves) == 1
    all_commands = [command for _, commands in results for command in commands]
    assert sum(command.invocation.contract.name == "unit.get" for command in all_commands) == 2
    assert sum(command.invocation.contract.name == "unit.delete" for command in all_commands) == 1
    assert [len(store.lookup_calls) for store in stores] == [1, 1]
    assert [len(store.consume_calls) for store in stores] == [1, 1]
