from __future__ import annotations

import base64
import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest
from pydantic import BaseModel, JsonValue

from cmo_agent_bridge.application import BridgeApplication
from cmo_agent_bridge.application.compatibility_policy import CompatibilityPolicy
from cmo_agent_bridge.application.confirmation import (
    ConfirmationBinding,
    ConfirmationTokenStore,
)
from cmo_agent_bridge.application.ports import (
    CompatibilityProbePort,
    LocalOperationPort,
)
from cmo_agent_bridge.application.session_service import SessionScope, SessionService
from cmo_agent_bridge.config import BridgeConfig
from cmo_agent_bridge.errors import ErrorCode
from cmo_agent_bridge.operations.models import (
    BridgeStatusResult,
    BridgeStatusWireArgs,
    CapacityObservations,
    CompatibilityProfileResult,
    ConfirmedDeleteUnitWireArgs,
    DeleteResult,
    PrimitiveObservations,
    UnitResult,
)
from cmo_agent_bridge.operations.registry import (
    OPERATION_REGISTRY,
    OperationRegistry,
    ResolvedInvocation,
)
from cmo_agent_bridge.protocol.canonical import request_sha256
from cmo_agent_bridge.protocol.models import ExchangeCommand
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CompletedSettlement,
    RejectedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
    ResponseError,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, Sha256
from cmo_agent_bridge.state.session_store import SessionStore
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.models import (
    BridgeTransport,
    RecoveryDisposition,
    RecoveryReport,
)
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo


LINEAGE_ID = UUID("11111111-1111-4111-8111-111111111111")
CANDIDATE = UUID("55555555-5555-4555-8555-555555555555")
STATUS_1 = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
READ_1 = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
STATUS_2 = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
STATUS_3 = UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
READ_2 = UUID("77777777-7777-4777-8777-777777777777")
DELETE_1 = UUID("88888888-8888-4888-8888-888888888888")
DELIVERY_ID = UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
ROOT_KEY = "a" * 64
COMMAND_EXE = Path(r"D:\Games\CMO\Command.exe")
ISSUE_AT_MS = 1_752_400_000_123
TOKEN = base64.urlsafe_b64encode(bytes(range(32))).rstrip(b"=").decode("ascii")


class _UuidSequence:
    def __init__(self, *values: UUID) -> None:
        self._values = list(values)

    def __call__(self) -> UUID:
        if not self._values:
            raise AssertionError("unexpected UUID allocation")
        return self._values.pop(0)

    @property
    def remaining(self) -> tuple[UUID, ...]:
        return tuple(self._values)


class _Clock:
    def __init__(self, *epochs: int) -> None:
        self._epochs = list(epochs)

    def now_ms(self) -> int:
        if not self._epochs:
            raise AssertionError("unexpected wall-clock read")
        return self._epochs.pop(0)

    @property
    def remaining(self) -> tuple[int, ...]:
        return tuple(self._epochs)


class _ProfileLookup:
    def __init__(self, profile: CompatibilityProfileResult) -> None:
        self._profile = profile
        self.calls: list[tuple[int, Sha256]] = []

    def load(self, *, build: int, release_id: Sha256) -> CompatibilityProfileResult:
        self.calls.append((build, release_id))
        return self._profile


class _UnusedLocalOperations:
    async def execute(self, operation: str, arguments: BaseModel) -> object:
        del operation, arguments
        raise AssertionError("local operation was unexpectedly executed")


class _UnusedCompatibilityProbe:
    async def execute(self, arguments: BaseModel) -> object:
        del arguments
        raise AssertionError("compatibility probe was unexpectedly executed")


class _SameChannel:
    def __init__(self, snapshot: RuntimeSnapshot) -> None:
        self._snapshot = snapshot
        self.commands: list[ExchangeCommand] = []

    @property
    def process_identity(self) -> ProcessInfo:
        return ProcessInfo(pid=1200, create_time=1000.5, executable=COMMAND_EXE)

    async def exchange(self, command: ExchangeCommand) -> ResponseArtifact:
        self.commands.append(command)
        operation = command.invocation.contract.name
        if operation == "bridge.status":
            wire = command.invocation.wire_arguments
            assert type(wire) is BridgeStatusWireArgs
            assert wire.activation_candidate == CANDIDATE
            if command.request_id == STATUS_2:
                assert command.body.expected_lineage_id == LINEAGE_ID
                assert command.body.expected_activation_id == CANDIDATE
                return _activation_mismatch_artifact(command)
            assert command.request_id in {STATUS_1, STATUS_3}
            assert command.body.expected_lineage_id is None
            assert command.body.expected_activation_id is None
            return _status_artifact(command, self._snapshot)
        if operation == "unit.get":
            assert command.request_id in {READ_1, READ_2}
            return _domain_artifact(command, _unit_result())
        if operation == "unit.delete":
            assert command.request_id == DELETE_1
            return _domain_artifact(command, _delete_result())
        raise AssertionError(f"unexpected operation: {operation}")

    async def recover_pending(self) -> RecoveryReport:
        return RecoveryReport(
            disposition=RecoveryDisposition.NO_PENDING,
            request_id=None,
            request_state=None,
            response_cleanup_required=False,
        )


class _SameChannelContext:
    def __init__(self, transport: _SameChannelTransport) -> None:
        self._transport = transport

    async def __aenter__(self) -> _SameChannel:
        self._transport.entered_channels.append(self._transport.channel)
        return self._transport.channel

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc, traceback


class _SameChannelTransport:
    def __init__(self, channel: _SameChannel) -> None:
        self.channel = channel
        self.entered_channels: list[_SameChannel] = []

    @property
    def root_key(self) -> Sha256:
        return cast(Sha256, ROOT_KEY)

    def session(self) -> _SameChannelContext:
        return _SameChannelContext(self)


def _fixed_token_factory(size: int) -> str:
    assert size == 32
    return TOKEN


def _runtime_snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=OPERATION_REGISTRY.manifest_sha256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )


def _status_result(snapshot: RuntimeSnapshot) -> BridgeStatusResult:
    return BridgeStatusResult(
        protocol=snapshot.protocol,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
        build=1868,
        manifest_sha256=snapshot.operation_manifest_sha256,
        lineage_id=LINEAGE_ID,
        activation_id=CANDIDATE,
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


def _compatibility_profile(snapshot: RuntimeSnapshot) -> CompatibilityProfileResult:
    return CompatibilityProfileResult(
        build=1868,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        release_id=snapshot.release_id,
        protocol=snapshot.protocol,
        manifest_sha256=snapshot.operation_manifest_sha256,
        primitive_observations=PrimitiveObservations(
            run_script=True,
            nested_delivery_global=True,
            delivery_global_cleaned=True,
            export_inst_empty_list=True,
            unicode_roundtrip=True,
            manifest_match=True,
            special_action_while_paused=True,
        ),
        capacity_observations=CapacityObservations(
            max_verified_comments_bytes=131_072,
            safe_payload_bytes=65_536,
            verified_ledger_entries=128,
            effective_ledger_capacity=128,
        ),
        profile_path="profile.json",
    )


def _unit_result() -> UnitResult:
    return UnitResult(
        guid="UNIT-1",
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


def _delete_result() -> DeleteResult:
    return DeleteResult(
        deleted_guid="UNIT-1",
        deleted_name="Alpha at deletion",
        object_kind="unit",
    )


def _response_artifact(
    command: ExchangeCommand,
    *,
    result: JsonValue | None,
    error: ResponseError | None,
    lineage_id: UUID,
    activation_id: UUID,
) -> ResponseArtifact:
    snapshot = command.runtime_snapshot
    envelope = ResponseEnvelope(
        protocol=snapshot.protocol,
        request_id=command.request_id,
        delivery_id=DELIVERY_ID,
        request_hash=request_sha256(command.body),
        ok=error is None,
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
        if error is None
        else RejectedSettlement(state="rejected", error=error)
    )
    return ResponseArtifact(
        filename=f"CMOAgentBridge_Response_{command.request_id}.inst",
        sha256=hashlib.sha256(
            f"{command.request_id}:{request_sha256(command.body)}".encode("ascii")
        ).hexdigest(),
        size_bytes=512,
        accepted_at_ms=ISSUE_AT_MS,
        accepted_response=AcceptedResponse(
            envelope=envelope,
            delivery_kind="request",
            settlement=settlement,
            cancel_ack=None,
        ),
    )


def _status_artifact(
    command: ExchangeCommand,
    snapshot: RuntimeSnapshot,
) -> ResponseArtifact:
    status = _status_result(snapshot)
    return _response_artifact(
        command,
        result=cast(JsonValue, status.model_dump(mode="json")),
        error=None,
        lineage_id=LINEAGE_ID,
        activation_id=CANDIDATE,
    )


def _activation_mismatch_artifact(command: ExchangeCommand) -> ResponseArtifact:
    return _response_artifact(
        command,
        result=None,
        error=ResponseError(
            code=ErrorCode.ACTIVATION_MISMATCH,
            message="continuing activation is stale",
            details={"reported_by": "same-channel-integration"},
            mutation_not_started=None,
        ),
        lineage_id=LINEAGE_ID,
        activation_id=CANDIDATE,
    )


def _domain_artifact(command: ExchangeCommand, result_model: BaseModel) -> ResponseArtifact:
    lineage_id = command.body.expected_lineage_id
    activation_id = command.body.expected_activation_id
    assert lineage_id is not None
    assert activation_id is not None
    return _response_artifact(
        command,
        result=cast(JsonValue, result_model.model_dump(mode="json")),
        error=None,
        lineage_id=lineage_id,
        activation_id=activation_id,
    )


@pytest.mark.asyncio
async def test_preview_and_confirmation_keep_recovery_and_delete_on_same_channel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _runtime_snapshot()
    database = StateDatabase(tmp_path / "state.sqlite3")
    uuid_source = _UuidSequence(
        CANDIDATE,
        STATUS_1,
        READ_1,
        STATUS_2,
        STATUS_3,
        READ_2,
        DELETE_1,
    )
    session_clock = _Clock(ISSUE_AT_MS, ISSUE_AT_MS + 1_000)
    application_clock = _Clock(
        ISSUE_AT_MS,
        ISSUE_AT_MS + 1_000,
        ISSUE_AT_MS + 2_000,
    )
    sessions = SessionService(
        scope=SessionScope(root_key=ROOT_KEY, command_exe=COMMAND_EXE),
        session_store=SessionStore(database),
        registry=OPERATION_REGISTRY,
        runtime_snapshot=snapshot,
        wall_clock=session_clock,
        uuid4_source=uuid_source,
        status_timeout_seconds=0.5,
    )
    channel = _SameChannel(snapshot)
    transport = _SameChannelTransport(channel)
    confirmations = ConfirmationTokenStore(
        database,
        token_factory=_fixed_token_factory,
    )
    profile_lookup = _ProfileLookup(_compatibility_profile(snapshot))
    policy = CompatibilityPolicy(
        config=BridgeConfig(allow_mutations=True, allow_destructive=True),
        profiles=profile_lookup,
    )

    consume_completed = False
    trusted_delete_resolutions = 0
    original_consume = ConfirmationTokenStore.consume
    original_resolve = OperationRegistry.resolve_invocation

    def tracked_consume(
        owner: ConfirmationTokenStore,
        token: str,
        binding: ConfirmationBinding,
        *,
        now_ms: int,
    ) -> Sha256:
        nonlocal consume_completed
        proof = original_consume(owner, token, binding, now_ms=now_ms)
        consume_completed = True
        return proof

    def tracked_resolve(
        registry: OperationRegistry,
        name: str,
        arguments: Mapping[str, object],
        trusted_enrichment: Mapping[str, object] | None = None,
    ) -> ResolvedInvocation:
        nonlocal trusted_delete_resolutions
        if name == "unit.delete" and trusted_enrichment is not None:
            assert consume_completed is True
            assert trusted_enrichment == {
                "confirmation_proof": hashlib.sha256(TOKEN.encode("utf-8")).hexdigest()
            }
            trusted_delete_resolutions += 1
        return original_resolve(registry, name, arguments, trusted_enrichment)

    monkeypatch.setattr(ConfirmationTokenStore, "consume", tracked_consume)
    monkeypatch.setattr(OperationRegistry, "resolve_invocation", tracked_resolve)

    application = BridgeApplication(
        registry=OPERATION_REGISTRY,
        transport=cast(BridgeTransport, transport),
        sessions=sessions,
        policy=policy,
        confirmations=confirmations,
        wall_clock=application_clock,
        local_operations=cast(LocalOperationPort, _UnusedLocalOperations()),
        compatibility_probe=cast(CompatibilityProbePort, _UnusedCompatibilityProbe()),
        request_timeout_seconds=0.5,
    )

    preview = await application.execute("unit.delete", {"unit_guid": "UNIT-1"})
    assert preview.ok is True
    assert preview.request_id == READ_1
    assert isinstance(preview.result, dict)
    assert preview.result["confirmation_token"] == TOKEN
    assert preview.result["reserved_activation_candidate"] == str(CANDIDATE)

    confirmed = await application.execute(
        "unit.delete",
        {"unit_guid": "UNIT-1"},
        confirmation_token=TOKEN,
    )

    assert confirmed.ok is True
    assert confirmed.request_id == DELETE_1
    assert confirmed.result == _delete_result().model_dump(mode="json")
    assert consume_completed is True
    assert trusted_delete_resolutions == 1
    assert transport.entered_channels == [channel, channel]
    assert all(entered is channel for entered in transport.entered_channels)

    expected_request_ids = [STATUS_1, READ_1, STATUS_2, STATUS_3, READ_2, DELETE_1]
    assert [command.request_id for command in channel.commands] == expected_request_ids
    assert [command.invocation.contract.name for command in channel.commands] == [
        "bridge.status",
        "unit.get",
        "bridge.status",
        "bridge.status",
        "unit.get",
        "unit.delete",
    ]
    all_visible_ids = [CANDIDATE, *expected_request_ids]
    assert len(set(all_visible_ids)) == len(all_visible_ids)
    status_commands = [
        command
        for command in channel.commands
        if command.invocation.contract.name == "bridge.status"
    ]
    assert [command.request_id for command in status_commands] == [
        STATUS_1,
        STATUS_2,
        STATUS_3,
    ]
    assert all(
        type(command.invocation.wire_arguments) is BridgeStatusWireArgs
        and command.invocation.wire_arguments.activation_candidate == CANDIDATE
        for command in status_commands
    )
    assert status_commands[0].body.expected_lineage_id is None
    assert status_commands[0].body.expected_activation_id is None
    assert status_commands[1].body.expected_lineage_id == LINEAGE_ID
    assert status_commands[1].body.expected_activation_id == CANDIDATE
    assert status_commands[2].body.expected_lineage_id is None
    assert status_commands[2].body.expected_activation_id is None
    delete_commands = [
        command for command in channel.commands if command.invocation.contract.name == "unit.delete"
    ]
    assert len(delete_commands) == 1
    assert type(delete_commands[0].invocation.wire_arguments) is ConfirmedDeleteUnitWireArgs
    assert uuid_source.remaining == ()
    assert session_clock.remaining == ()
    assert application_clock.remaining == ()
    assert profile_lookup.calls == [
        (1868, snapshot.release_id),
        (1868, snapshot.release_id),
    ]
