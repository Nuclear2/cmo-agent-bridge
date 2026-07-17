from __future__ import annotations

import asyncio
import hashlib
import inspect
import math
import sqlite3
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType, TracebackType
from typing import TYPE_CHECKING, Any, Never, cast, get_type_hints
from uuid import UUID

import pytest
from pydantic import JsonValue, ValidationError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256
from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.operations.registry import (
    OPERATION_REGISTRY,
    OperationRegistry,
    ResolvedInvocation,
    SchemaBinding,
)
from cmo_agent_bridge.operations.wire import ModelWireResolver
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes, prepare_delivery
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua, render_idle_lua
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.models import (
    AllowedDelivery,
    ExchangeCommand,
    PreparedDelivery,
    RequestBody,
    ResponseExpectation,
)
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CompletedSettlement,
    RejectedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
    ResponseError,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    PendingExchange,
    PendingJournal,
    PendingJournalHeader,
    PendingPhase,
)
from cmo_agent_bridge.state.pending_journal import (
    JournalDeleteExpectation,
    JournalRevisions,
    PendingJournalStore,
)
from cmo_agent_bridge.state.request_ledger import DeliveryRecord, RequestLedger, RequestRecord
from cmo_agent_bridge.state.sqlite import StateDatabase
import cmo_agent_bridge.transports.file_bridge.cleanup as cleanup_module
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.models import (
    BridgeChannel,
    BridgeTransport,
    DurableBridgeChannel,
    DurableBridgeTransport,
    RecoveryReport,
)
from cmo_agent_bridge.transports.file_bridge.mutation import MutationExchange
import cmo_agent_bridge.transports.file_bridge.models as bridge_models_module
import cmo_agent_bridge.transports.file_bridge.mutation as mutation_module
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.process_guard import (
    CmoProcessInspector,
    ProcessInfo,
)
from cmo_agent_bridge.transports.file_bridge.recovery import RecoveryManager
from cmo_agent_bridge.transports.file_bridge.response_waiter import ResponseWaiter
from cmo_agent_bridge.transports.file_bridge.transport import (
    FileBridgeTransport,
    _FileBridgeChannel,  # pyright: ignore[reportPrivateUsage]
)
import cmo_agent_bridge.transports.file_bridge.transport as transport_module

if TYPE_CHECKING:
    from tests.helpers.fake_file_bridge_peer import (
        FakeFileBridgePeer,
        Respond,
        StaySilent,
        WritePartialThenComplete,
    )
else:
    sys.path.insert(0, str(Path(__file__).parents[3] / "helpers"))
    from fake_file_bridge_peer import (
        FakeFileBridgePeer,
        Respond,
        StaySilent,
        WritePartialThenComplete,
    )


REQUEST_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee9001")
DELIVERY_ID = UUID("11111111-1111-4111-8111-111111119001")
LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")


def _forbidden(*_args: object, **_kwargs: object) -> Never:
    raise AssertionError("forbidden side effect was reached")


def _ignore_cleanup(_path: Path) -> None:
    return None


def _canonical_json(value: object) -> bytes:
    import json

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


class Inspector:
    def __init__(self, process: ProcessInfo) -> None:
        self.process = process
        self.calls = 0

    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        assert command_exe == self.process.executable
        self.calls += 1
        return (self.process,)


class FixedInspector:
    def __init__(self, observations: tuple[ProcessInfo, ...]) -> None:
        self.observations = observations
        self.calls = 0
        self.command_exes: list[Path] = []

    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        self.command_exes.append(command_exe)
        self.calls += 1
        return self.observations


@dataclass(frozen=True, slots=True)
class Harness:
    paths: FileBridgePaths
    lock: RootLock
    process: ProcessInfo
    inspector: Inspector
    snapshot: RuntimeSnapshot
    catalog: ManifestCatalog
    database: StateDatabase
    journals: PendingJournalStore
    ledger: RequestLedger
    inbox: InboxPublisher


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    game_root = tmp_path / "game"
    game_root.mkdir()
    command_exe = game_root / "Command.exe"
    command_exe.write_bytes(b"exe")
    (game_root / "Lua").mkdir()
    (game_root / "ImportExport").mkdir()
    local_app_data = tmp_path / "local"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    lock = RootLock(paths.lock_file, timeout_seconds=0)
    process = ProcessInfo(pid=1234, create_time=1000.5, executable=paths.command_exe)
    inspector = Inspector(process)
    snapshot = RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=MANIFEST_SHA256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )
    catalog = ManifestCatalog(ReleaseBinding(snapshot=snapshot, registry=OPERATION_REGISTRY))
    database = StateDatabase(paths.sqlite_file)
    journals = PendingJournalStore(
        paths,
        lock,
        catalog,
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
    )
    ledger = RequestLedger(database, catalog)
    inbox = InboxPublisher(paths, 0)
    return Harness(
        paths=paths,
        lock=lock,
        process=process,
        inspector=inspector,
        snapshot=snapshot,
        catalog=catalog,
        database=database,
        journals=journals,
        ledger=ledger,
        inbox=inbox,
    )


def _command(
    harness: Harness,
    operation: str = "unit.set",
    arguments: dict[str, object] | None = None,
    *,
    request_id: UUID = REQUEST_ID,
    trusted_enrichment: dict[str, object] | None = None,
    timeout: float = 0.1,
) -> ExchangeCommand:
    public = {"unit_guid": "UNIT-1", "name": "Renamed"} if arguments is None else arguments
    invocation = OPERATION_REGISTRY.resolve_invocation(
        operation,
        public,
        trusted_enrichment,
    )
    body = RequestBody(
        protocol=harness.snapshot.protocol,
        release_id=harness.snapshot.release_id,
        runtime_version=harness.snapshot.runtime_version,
        runtime_tag=harness.snapshot.runtime_tag,
        runtime_asset_sha256=harness.snapshot.runtime_asset_sha256,
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=ACTIVATION_ID,
        operation_manifest_sha256=harness.snapshot.operation_manifest_sha256,
        operation=operation,
        arguments=invocation.wire_arguments.model_dump(mode="json"),
    )
    return ExchangeCommand(
        request_id=request_id,
        body=body,
        invocation=invocation,
        runtime_snapshot=harness.snapshot,
        timeout=timeout,
    )


def _exchange(harness: Harness, **overrides: object) -> MutationExchange:
    values: dict[str, object] = {
        "paths": harness.paths,
        "root_lock": harness.lock,
        "process_inspector": harness.inspector,
        "expected_process": harness.process,
        "catalog": harness.catalog,
        "journals": harness.journals,
        "ledger": harness.ledger,
        "inbox": harness.inbox,
        "response_poll_seconds": 0.001,
        "queue_response_cleanup": _ignore_cleanup,
    }
    values.update(overrides)
    if "recovery_manager" not in values:
        values["recovery_manager"] = _recovery_manager(
            harness,
            paths=values["paths"],
            root_lock=values["root_lock"],
            process_inspector=values["process_inspector"],
            expected_process=values["expected_process"],
            journals=values["journals"],
            ledger=values["ledger"],
            inbox=values["inbox"],
            response_poll_seconds=values["response_poll_seconds"],
        )
    return cast(Any, MutationExchange)(**values)


def _recovery_manager(harness: Harness, **overrides: object) -> RecoveryManager:
    values: dict[str, object] = {
        "paths": harness.paths,
        "root_lock": harness.lock,
        "process_inspector": harness.inspector,
        "expected_process": harness.process,
        "journals": harness.journals,
        "ledger": harness.ledger,
        "inbox": harness.inbox,
        "response_poll_seconds": 0.001,
        "cancel_ack_timeout_seconds": 10,
    }
    values.update(overrides)
    return cast(Any, RecoveryManager)(**values)


def _transport(harness: Harness, **overrides: object) -> FileBridgeTransport:
    values: dict[str, object] = {
        "paths": harness.paths,
        "root_lock": harness.lock,
        "process_inspector": harness.inspector,
        "catalog": harness.catalog,
        "database": harness.database,
        "max_journal_bytes": 1_000_000,
        "replace_retry_seconds": 0,
        "response_poll_seconds": 0.001,
        "cancel_ack_timeout_seconds": 10,
    }
    values.update(overrides)
    return cast(Any, FileBridgeTransport)(**values)


def _peer(harness: Harness, *, trace: list[str]) -> FakeFileBridgePeer:
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    return FakeFileBridgePeer(
        paths=harness.paths,
        runtime_snapshot=harness.snapshot,
        registry=OPERATION_REGISTRY,
        scenario_lineage_id=LINEAGE_ID,
        poll_seconds=0.001,
        trace=trace,
    )


def _install_constructor_tripwires(
    monkeypatch: pytest.MonkeyPatch,
    harness: Harness,
) -> None:
    managed = {
        harness.paths.game_root,
        harness.paths.command_exe,
        harness.paths.lua_root,
        harness.paths.inbox,
        harness.paths.import_export,
        harness.paths.lock_file,
        harness.paths.pending_file,
        harness.paths.sqlite_file,
        harness.paths.response_path(REQUEST_ID),
    }
    original_exists = Path.exists
    original_stat = Path.stat
    original_open = Path.open
    original_read_bytes = Path.read_bytes
    original_mkdir = Path.mkdir

    def guard(path: Path) -> None:
        if path in managed:
            raise AssertionError(f"constructor touched managed path: {path}")

    def guarded_exists(path: Path) -> bool:
        guard(path)
        return original_exists(path)

    def guarded_stat(path: Path, *args: object, **kwargs: object) -> object:
        guard(path)
        return original_stat(path, *args, **kwargs)

    def guarded_open(path: Path, *args: object, **kwargs: object) -> object:
        guard(path)
        return cast(Any, original_open)(path, *args, **kwargs)

    def guarded_read_bytes(path: Path) -> bytes:
        guard(path)
        return original_read_bytes(path)

    def guarded_mkdir(path: Path, *args: object, **kwargs: object) -> None:
        guard(path)
        cast(Any, original_mkdir)(path, *args, **kwargs)

    monkeypatch.setattr(RootLock, "require_acquired", _forbidden)
    monkeypatch.setattr(RootLock, "__aenter__", _forbidden)
    monkeypatch.setattr(Inspector, "matching_processes", _forbidden)
    monkeypatch.setattr(ManifestCatalog, "resolve_running", _forbidden)
    monkeypatch.setattr(PendingJournalStore, "load", _forbidden)
    monkeypatch.setattr(PendingJournalStore, "save", _forbidden)
    monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(Path, "exists", guarded_exists)
    monkeypatch.setattr(Path, "stat", guarded_stat)
    monkeypatch.setattr(Path, "open", guarded_open)
    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)
    monkeypatch.setattr(Path, "mkdir", guarded_mkdir)
    assert harness.inspector.calls == 0


def _prepared_journal(harness: Harness, command: ExchangeCommand) -> PendingJournal:
    prepared = prepare_delivery(
        command.body,
        request_id=command.request_id,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
    )
    rendered = render_delivery_lua(prepared, command.runtime_snapshot)
    recovery = command.invocation.recovery_schema
    intent = DeliveryIntent(
        request_id=command.request_id,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
        original_request_delivery_id=DELIVERY_ID,
        body_json=prepared.body_json.decode("utf-8"),
        request_hash=prepared.request_hash,
        runtime_snapshot=command.runtime_snapshot,
        result_schema_id=command.invocation.result_schema.schema_id,
        recovery_schema_id=None if recovery is None else recovery.schema_id,
        intended_at_ms=100,
        published_at_ms=None,
        rendered_inbox_sha256=hashlib.sha256(rendered).hexdigest(),
        rendered_inbox_size_bytes=len(rendered),
        response_filename=harness.paths.response_path(command.request_id).name,
    )
    original = PendingExchange(
        request_id=command.request_id,
        request_hash=prepared.request_hash,
        operation=command.body.operation,
        effective_class=command.invocation.effective_class,
        body_json=prepared.body_json.decode("utf-8"),
        runtime_snapshot=command.runtime_snapshot,
        result_schema_id=command.invocation.result_schema.schema_id,
        recovery_schema_id=None if recovery is None else recovery.schema_id,
        expected_lineage_id=command.body.expected_lineage_id,
        expected_activation_id=command.body.expected_activation_id,
        delivery_intents=(intent,),
        response_artifact=None,
        settlement=None,
        original_target_request_id=None,
        original_target_request_hash=None,
        revision=0,
        state=PendingPhase.PREPARED,
        created_at_ms=100,
        updated_at_ms=100,
    )
    return PendingJournal(
        header=PendingJournalHeader(
            format="cmo-agent-bridge/pending-journal",
            header_version=1,
            root_key=harness.paths.root_key,
            required_release_id=command.runtime_snapshot.release_id,
        ),
        original=original,
        reconcile_attempt=None,
    )


def _request_record(
    harness: Harness,
    command: ExchangeCommand,
    *,
    created_at_ms: int = 100,
) -> RequestRecord:
    body = canonical_body_bytes(command.body)
    recovery = command.invocation.recovery_schema
    return RequestRecord(
        request_id=command.request_id,
        root_key=harness.paths.root_key,
        request_hash=hashlib.sha256(body).hexdigest(),
        operation=command.body.operation,
        operation_class=command.invocation.effective_class,
        state=HostRequestState.PREPARED,
        runtime_snapshot=command.runtime_snapshot,
        result_schema_id=command.invocation.result_schema.schema_id,
        recovery_schema_id=None if recovery is None else recovery.schema_id,
        body_json=body,
        lineage_id=command.body.expected_lineage_id,
        activation_id=command.body.expected_activation_id,
        result_json=None,
        error_json=None,
        resolution_json=None,
        created_at_ms=created_at_ms,
        updated_at_ms=created_at_ms,
        terminal_at_ms=None,
    )


def _completed_artifact(
    command: ExchangeCommand,
    *,
    delivery_id: UUID = DELIVERY_ID,
    accepted_at_ms: int = 102,
) -> ResponseArtifact:
    result_input = {
        "unit_guid": "UNIT-1",
        "name": "Renamed",
        "speed": None,
        "altitude": None,
        "heading": None,
        "course": None,
    }
    validated = command.invocation.result_adapter.validate_python(result_input)
    result = command.invocation.result_adapter.dump_python(validated, mode="json")
    envelope = ResponseEnvelope(
        protocol=command.runtime_snapshot.protocol,
        request_id=command.request_id,
        delivery_id=delivery_id,
        request_hash=hashlib.sha256(canonical_body_bytes(command.body)).hexdigest(),
        ok=True,
        result=result,
        error=None,
        scenario_time="2026-07-10T13:00:00Z",
        scenario_lineage_id=cast(UUID, command.body.expected_lineage_id),
        activation_id=cast(UUID, command.body.expected_activation_id),
        operation_manifest_sha256=command.runtime_snapshot.operation_manifest_sha256,
        bridge_version=command.runtime_snapshot.runtime_version,
        runtime_tag=command.runtime_snapshot.runtime_tag,
        runtime_asset_sha256=command.runtime_snapshot.runtime_asset_sha256,
        release_id=command.runtime_snapshot.release_id,
    )
    settlement = CompletedSettlement(state="completed", result=result)
    accepted = AcceptedResponse(
        envelope=envelope,
        delivery_kind="request",
        settlement=settlement,
        cancel_ack=None,
    )
    return ResponseArtifact(
        filename=f"CMOAgentBridge_Response_{command.request_id}.inst",
        sha256="e" * 64,
        size_bytes=512,
        accepted_at_ms=accepted_at_ms,
        accepted_response=accepted,
    )


def _journal_with_original(
    journal: PendingJournal,
    **changes: object,
) -> PendingJournal:
    original_tree = journal.original.model_dump(
        mode="python",
        round_trip=True,
        warnings=False,
    )
    original_tree.update(changes)
    original = PendingExchange.model_validate(original_tree)
    return PendingJournal(
        header=journal.header,
        original=original,
        reconcile_attempt=None,
    )


def _persist_journal_at_phase(
    harness: Harness,
    command: ExchangeCommand,
    phase: PendingPhase,
) -> bytes:
    journal = _prepared_journal(harness, command)
    revisions = harness.journals.save(journal, expected_revisions=None)
    assert revisions == JournalRevisions(original=0, reconcile_attempt=None)
    if phase is PendingPhase.PREPARED:
        return harness.paths.pending_file.read_bytes()

    intended = journal.original.delivery_intents[0]
    published_intent = DeliveryIntent.model_validate(
        {
            **intended.model_dump(mode="python", round_trip=True, warnings=False),
            "published_at_ms": 101,
        }
    )
    published = _journal_with_original(
        journal,
        delivery_intents=(published_intent,),
        revision=1,
        state=PendingPhase.PUBLISHED,
        updated_at_ms=101,
    )
    revisions = harness.journals.save(
        published,
        expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
    )
    assert revisions == JournalRevisions(original=1, reconcile_attempt=None)
    if phase is PendingPhase.PUBLISHED:
        return harness.paths.pending_file.read_bytes()
    if phase is PendingPhase.QUARANTINED:
        quarantined = _journal_with_original(
            published,
            revision=2,
            state=PendingPhase.QUARANTINED,
            updated_at_ms=102,
        )
        harness.journals.save(
            quarantined,
            expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
        )
        return harness.paths.pending_file.read_bytes()

    artifact = _completed_artifact(command)
    response = _journal_with_original(
        published,
        response_artifact=artifact,
        settlement=artifact.accepted_response.settlement,
        revision=2,
        state=PendingPhase.RESPONSE_ACCEPTED,
        updated_at_ms=102,
    )
    revisions = harness.journals.save(
        response,
        expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
    )
    assert revisions == JournalRevisions(original=2, reconcile_attempt=None)
    if phase is PendingPhase.RESPONSE_ACCEPTED:
        return harness.paths.pending_file.read_bytes()
    assert phase is PendingPhase.IDLE_PUBLISHED
    idle = _journal_with_original(
        response,
        revision=3,
        state=PendingPhase.IDLE_PUBLISHED,
        updated_at_ms=103,
    )
    harness.journals.save(
        idle,
        expected_revisions=JournalRevisions(original=2, reconcile_attempt=None),
    )
    return harness.paths.pending_file.read_bytes()


def test_mutation_exchange_and_transport_protocol_signatures_are_exact() -> None:
    constructor = inspect.signature(MutationExchange.__init__)
    assert list(constructor.parameters) == [
        "self",
        "paths",
        "root_lock",
        "process_inspector",
        "expected_process",
        "catalog",
        "journals",
        "ledger",
        "inbox",
        "response_poll_seconds",
        "queue_response_cleanup",
        "recovery_manager",
        "durable_worker",
    ]
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for name, parameter in constructor.parameters.items()
        if name != "self"
    )
    assert constructor.parameters["durable_worker"].default is False
    assert all(
        parameter.default is inspect.Parameter.empty
        for name, parameter in constructor.parameters.items()
        if name not in {"self", "durable_worker"}
    )
    constructor_hints = get_type_hints(MutationExchange.__init__)
    assert constructor_hints == {
        "paths": FileBridgePaths,
        "root_lock": RootLock,
        "process_inspector": CmoProcessInspector,
        "expected_process": ProcessInfo,
        "catalog": ManifestCatalog,
        "journals": PendingJournalStore,
        "ledger": RequestLedger,
        "inbox": InboxPublisher,
        "response_poll_seconds": float,
        "queue_response_cleanup": Callable[[Path], None],
        "recovery_manager": RecoveryManager,
        "durable_worker": bool,
        "return": type(None),
    }
    run = inspect.signature(MutationExchange.run)
    assert list(run.parameters) == ["self", "command"]
    assert run.parameters["command"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert run.parameters["command"].default is inspect.Parameter.empty
    assert inspect.iscoroutinefunction(MutationExchange.run)
    assert get_type_hints(MutationExchange.run) == {
        "command": ExchangeCommand,
        "return": ResponseArtifact,
    }

    channel_exchange = inspect.signature(BridgeChannel.exchange)
    assert list(channel_exchange.parameters) == ["self", "command"]
    assert inspect.iscoroutinefunction(BridgeChannel.exchange)
    assert get_type_hints(BridgeChannel.exchange) == {
        "command": ExchangeCommand,
        "return": ResponseArtifact,
    }
    channel_recovery = inspect.signature(BridgeChannel.recover_pending)
    assert list(channel_recovery.parameters) == ["self"]
    assert inspect.iscoroutinefunction(BridgeChannel.recover_pending)
    assert get_type_hints(BridgeChannel.recover_pending) == {"return": RecoveryReport}
    channel_report = inspect.getattr_static(DurableBridgeChannel, "recovery_report")
    assert isinstance(channel_report, property)
    assert get_type_hints(channel_report.fget) == {"return": RecoveryReport}
    transport_session = inspect.signature(BridgeTransport.session)
    assert list(transport_session.parameters) == ["self"]
    assert not inspect.iscoroutinefunction(BridgeTransport.session)
    worker_session = inspect.signature(DurableBridgeTransport.worker_session)
    assert list(worker_session.parameters) == ["self", "recovery_owner"]
    assert worker_session.parameters["recovery_owner"].kind is inspect.Parameter.KEYWORD_ONLY
    assert not inspect.iscoroutinefunction(DurableBridgeTransport.worker_session)
    concrete = inspect.signature(FileBridgeTransport.__init__)
    assert list(concrete.parameters) == [
        "self",
        "paths",
        "root_lock",
        "process_inspector",
        "catalog",
        "database",
        "max_journal_bytes",
        "replace_retry_seconds",
        "response_poll_seconds",
        "cancel_ack_timeout_seconds",
    ]
    assert concrete.parameters["response_poll_seconds"].default == 0.05
    assert concrete.parameters["cancel_ack_timeout_seconds"].default == 10
    assert get_type_hints(FileBridgeTransport.__init__) == {
        "paths": FileBridgePaths,
        "root_lock": RootLock,
        "process_inspector": CmoProcessInspector,
        "catalog": ManifestCatalog,
        "database": StateDatabase,
        "max_journal_bytes": int,
        "replace_retry_seconds": float,
        "response_poll_seconds": float,
        "cancel_ack_timeout_seconds": float,
        "return": type(None),
    }
    assert list(inspect.signature(FileBridgeTransport.session).parameters) == ["self"]
    assert list(inspect.signature(FileBridgeTransport.worker_session).parameters) == [
        "self",
        "recovery_owner",
    ]
    for module in (bridge_models_module, mutation_module, transport_module):
        assert isinstance(module, ModuleType)
    assert bridge_models_module.RecoveryReport is RecoveryReport
    assert not hasattr(mutation_module, "RecoveryReport")
    assert not hasattr(transport_module, "RecoveryReport")
    assert hasattr(BridgeChannel, "recover_pending")
    assert not hasattr(BridgeChannel, "recovery_report")
    assert hasattr(DurableBridgeChannel, "recovery_report")
    for value in (BridgeTransport, MutationExchange, FileBridgeTransport):
        assert not hasattr(value, "recover_pending")


@pytest.mark.parametrize("poll_seconds", [1, 0.001])
def test_constructor_is_strictly_side_effect_free_and_accepts_positive_int_or_float_poll(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    poll_seconds: int | float,
) -> None:
    response_path = harness.paths.response_path(REQUEST_ID)
    before = {
        "sqlite": harness.paths.sqlite_file.exists(),
        "pending": harness.paths.pending_file.exists(),
        "inbox": harness.paths.inbox.exists(),
        "response": response_path.exists(),
    }

    def cleanup_forbidden(_path: Path) -> None:
        raise AssertionError("constructor invoked the cleanup callback")

    with monkeypatch.context() as guarded:
        _install_constructor_tripwires(guarded, harness)
        exchange = _exchange(
            harness,
            response_poll_seconds=poll_seconds,
            queue_response_cleanup=cleanup_forbidden,
        )
        assert type(exchange) is MutationExchange
        recovery_manager = exchange._recovery_manager  # pyright: ignore[reportPrivateUsage]
        assert type(recovery_manager) is RecoveryManager
        assert recovery_manager._is_bound_to(  # pyright: ignore[reportPrivateUsage]
            paths=harness.paths,
            root_lock=harness.lock,
            process_inspector=harness.inspector,
            expected_process=harness.process,
            journals=harness.journals,
            ledger=harness.ledger,
            inbox=harness.inbox,
            response_poll_seconds=float(poll_seconds),
        )
        assert recovery_manager._cancel_ack_timeout_seconds == 10.0  # pyright: ignore[reportPrivateUsage]
    assert harness.inspector.calls == 0
    assert {
        "sqlite": harness.paths.sqlite_file.exists(),
        "pending": harness.paths.pending_file.exists(),
        "inbox": harness.paths.inbox.exists(),
        "response": response_path.exists(),
    } == before


class _PathsSubclass(FileBridgePaths):
    pass


class _LockSubclass(RootLock):
    pass


class _CatalogSubclass(ManifestCatalog):
    pass


class _JournalSubclass(PendingJournalStore):
    pass


class _LedgerSubclass(RequestLedger):
    pass


class _InboxSubclass(InboxPublisher):
    pass


class _RecoveryManagerSubclass(RecoveryManager):
    pass


class _ProcessSubclass(ProcessInfo):
    pass


class _IntSubclass(int):
    pass


class _UuidSubclass(UUID):
    pass


class _ExchangeCommandSubclass(ExchangeCommand):
    pass


class _RequestBodySubclass(RequestBody):
    pass


class _ResolvedInvocationSubclass(ResolvedInvocation):
    pass


class _AsyncInspector:
    async def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        del command_exe
        return ()


@pytest.mark.parametrize(
    "case",
    [
        "paths_subclass",
        "lock_subclass",
        "cross_root_lock",
        "missing_inspector",
        "async_inspector",
        "catalog_subclass",
        "journal_subclass",
        "ledger_subclass",
        "inbox_subclass",
        "process_subclass",
        "pid_bool",
        "pid_zero",
        "pid_subclass",
        "create_time_int",
        "create_time_bool",
        "create_time_zero",
        "create_time_nan",
        "create_time_inf",
        "executable_string",
        "poll_bool",
        "poll_zero",
        "poll_negative",
        "poll_nan",
        "poll_inf",
        "poll_string",
        "cleanup_noncallable",
        "recovery_manager_subclass",
        "recovery_manager_unbound",
    ],
)
def test_constructor_rejects_exact_dependency_process_binding_and_numeric_matrix_without_io(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    overrides: dict[str, object] = {}
    if case == "paths_subclass":
        overrides["paths"] = _PathsSubclass(
            **{
                field: getattr(harness.paths, field)
                for field in FileBridgePaths.__dataclass_fields__
            }
        )
    elif case == "lock_subclass":
        overrides["root_lock"] = _LockSubclass(harness.paths.lock_file, timeout_seconds=0)
    elif case == "cross_root_lock":
        overrides["root_lock"] = RootLock(
            harness.paths.lock_file.with_name("other.lock"),
            timeout_seconds=0,
        )
    elif case == "missing_inspector":
        overrides["process_inspector"] = object()
    elif case == "async_inspector":
        overrides["process_inspector"] = _AsyncInspector()
    elif case == "catalog_subclass":
        overrides["catalog"] = _CatalogSubclass(
            ReleaseBinding(snapshot=harness.snapshot, registry=OPERATION_REGISTRY)
        )
    elif case == "journal_subclass":
        overrides["journals"] = _JournalSubclass(
            harness.paths,
            harness.lock,
            harness.catalog,
            max_journal_bytes=1_000_000,
            replace_retry_seconds=0,
        )
    elif case == "ledger_subclass":
        overrides["ledger"] = _LedgerSubclass(harness.database, harness.catalog)
    elif case == "inbox_subclass":
        overrides["inbox"] = _InboxSubclass(harness.paths, 0)
    elif case == "process_subclass":
        overrides["expected_process"] = _ProcessSubclass(
            pid=1,
            create_time=1.0,
            executable=harness.paths.command_exe,
        )
    elif case == "pid_bool":
        overrides["expected_process"] = ProcessInfo(
            pid=cast(int, True),
            create_time=1.0,
            executable=harness.paths.command_exe,
        )
    elif case == "pid_zero":
        overrides["expected_process"] = ProcessInfo(
            pid=0,
            create_time=1.0,
            executable=harness.paths.command_exe,
        )
    elif case == "pid_subclass":
        overrides["expected_process"] = ProcessInfo(
            pid=_IntSubclass(1),
            create_time=1.0,
            executable=harness.paths.command_exe,
        )
    elif case.startswith("create_time_"):
        create_time: object = {
            "create_time_int": 1,
            "create_time_bool": True,
            "create_time_zero": 0.0,
            "create_time_nan": math.nan,
            "create_time_inf": math.inf,
        }[case]
        overrides["expected_process"] = ProcessInfo(
            pid=1,
            create_time=cast(float, create_time),
            executable=harness.paths.command_exe,
        )
    elif case == "executable_string":
        overrides["expected_process"] = ProcessInfo(
            pid=1,
            create_time=1.0,
            executable=cast(Path, "Command.exe"),
        )
    elif case.startswith("poll_"):
        overrides["response_poll_seconds"] = {
            "poll_bool": True,
            "poll_zero": 0,
            "poll_negative": -1,
            "poll_nan": math.nan,
            "poll_inf": math.inf,
            "poll_string": "0.1",
        }[case]
    elif case == "cleanup_noncallable":
        overrides["queue_response_cleanup"] = object()
    elif case == "recovery_manager_subclass":
        overrides["recovery_manager"] = object.__new__(_RecoveryManagerSubclass)
    else:
        assert case == "recovery_manager_unbound"
        overrides["recovery_manager"] = _recovery_manager(
            harness,
            response_poll_seconds=0.002,
        )

    if "recovery_manager" not in overrides:
        # Keep the recovery dependency valid so this matrix continues to exercise
        # MutationExchange's own validation order for the selected bad argument.
        overrides["recovery_manager"] = _recovery_manager(harness)

    with monkeypatch.context() as guarded:
        _install_constructor_tripwires(guarded, harness)
        with pytest.raises(BridgeError) as caught:
            _exchange(harness, **overrides)
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert harness.inspector.calls == 0
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.pending_file.exists()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("paths", object()),
        ("root_lock", object()),
        ("expected_process", object()),
        ("catalog", object()),
        ("journals", object()),
        ("ledger", object()),
        ("inbox", object()),
        ("response_poll_seconds", True),
        ("response_poll_seconds", 0),
        ("queue_response_cleanup", object()),
        ("recovery_manager", object()),
    ],
)
def test_constructor_rejects_invalid_exact_dependencies_without_io(
    harness: Harness,
    name: str,
    value: object,
) -> None:
    overrides = {name: value}
    if name != "recovery_manager":
        overrides["recovery_manager"] = _recovery_manager(harness)
    with pytest.raises(BridgeError) as caught:
        _exchange(harness, **overrides)
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.pending_file.exists()
    assert harness.inspector.calls == 0


@pytest.mark.asyncio
async def test_run_requires_acquired_lock_before_journal_catalog_sqlite_path_or_cleanup(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exchange = _exchange(harness)
    trace: list[str] = []
    original_require = RootLock.require_acquired
    original_exists = Path.exists
    response_path = harness.paths.response_path(REQUEST_ID)

    def traced_require(lock: RootLock) -> None:
        trace.append("lock.require")
        original_require(lock)

    def guarded_exists(path: Path) -> bool:
        if path in {
            harness.paths.sqlite_file,
            harness.paths.pending_file,
            harness.paths.inbox,
            response_path,
        }:
            raise AssertionError("run touched a managed path before lock proof")
        return original_exists(path)

    monkeypatch.setattr(RootLock, "require_acquired", traced_require)
    monkeypatch.setattr(RootLock, "__aenter__", _forbidden)
    monkeypatch.setattr(PendingJournalStore, "load", _forbidden)
    monkeypatch.setattr(ManifestCatalog, "resolve_running", _forbidden)
    monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(Path, "exists", guarded_exists)
    with pytest.raises(BridgeError) as caught:
        await exchange.run(_command(harness))
    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert trace == ["lock.require"]
    assert not original_exists(harness.paths.sqlite_file)
    assert not original_exists(harness.paths.pending_file)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase",
    [
        PendingPhase.PREPARED,
        PendingPhase.PUBLISHED,
        PendingPhase.RESPONSE_ACCEPTED,
        PendingPhase.IDLE_PUBLISHED,
        PendingPhase.QUARANTINED,
    ],
)
async def test_existing_pending_phase_wins_before_invalid_command_catalog_sqlite_and_preserves_bytes(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    phase: PendingPhase,
) -> None:
    owner = _command(harness)
    attacker = _command(
        harness,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee9002"),
    )
    attacker.body.arguments["name"] = cast(Any, {"nested": "forgery"})
    cleanup: list[Path] = []
    exchange = _exchange(harness, queue_response_cleanup=cleanup.append)
    original_exists = Path.exists
    attacker_response_path = harness.paths.response_path(attacker.request_id)

    def guarded_exists(path: Path) -> bool:
        if path == attacker_response_path:
            raise AssertionError("pending gate reached response-slot inspection")
        return original_exists(path)

    async with harness.lock:
        before = _persist_journal_at_phase(harness, owner, phase)
        before_mtime = harness.paths.pending_file.stat().st_mtime_ns
        original_resolve = ManifestCatalog.resolve_running
        catalog_calls = 0

        def traced_resolve(catalog: ManifestCatalog, release_id: str) -> ReleaseBinding:
            nonlocal catalog_calls
            catalog_calls += 1
            return original_resolve(catalog, release_id)

        monkeypatch.setattr(ManifestCatalog, "resolve_running", traced_resolve)
        monkeypatch.setattr(PendingJournalStore, "save", _forbidden)
        monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
        monkeypatch.setattr(sqlite3, "connect", _forbidden)
        monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
        monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
        monkeypatch.setattr(Path, "exists", guarded_exists)
        with pytest.raises(BridgeError) as caught:
            await exchange.run(attacker)
        assert caught.value.code is ErrorCode.MUTATION_QUARANTINED
        assert caught.value.message == "an unresolved mutation blocks a new mutation"
        assert caught.value.details == {
            "request_id": str(owner.request_id),
            "state": phase.value,
            "required_release_id": harness.snapshot.release_id,
        }
        assert harness.paths.pending_file.read_bytes() == before
        assert harness.paths.pending_file.stat().st_mtime_ns == before_mtime
        assert catalog_calls == 1
        assert exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]
    assert not original_exists(harness.paths.sqlite_file)
    assert not original_exists(harness.paths.inbox)
    assert cleanup == []


_FOREIGN_OR_CORRUPT: tuple[tuple[Callable[[Harness], bytes], ErrorCode], ...] = (
    (
        lambda h: _canonical_json(
            {
                "header": {
                    "format": "cmo-agent-bridge/pending-journal",
                    "header_version": 1,
                    "root_key": h.paths.root_key,
                    "required_release_id": "f" * 64,
                },
                "original": {"semantically": "poisonous"},
                "reconcile_attempt": None,
            }
        ),
        ErrorCode.MANIFEST_MISMATCH,
    ),
    (lambda _h: b"{", ErrorCode.JOURNAL_CORRUPT),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(("raw_factory", "code"), _FOREIGN_OR_CORRUPT)
async def test_foreign_or_corrupt_session_gate_precedes_mutation_construction_and_preserves_all_managed_state(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    raw_factory: Callable[[Harness], bytes],
    code: ErrorCode,
) -> None:
    raw = raw_factory(harness)
    harness.paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.pending_file.write_bytes(raw)
    pending_mtime = harness.paths.pending_file.stat().st_mtime_ns
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    inbox_raw = b"inbox-sentinel"
    harness.paths.inbox.write_bytes(inbox_raw)
    inbox_mtime = harness.paths.inbox.stat().st_mtime_ns
    response_path = harness.paths.response_path(REQUEST_ID)
    response_path.parent.mkdir(parents=True, exist_ok=True)
    response_raw = b"response-sentinel"
    response_path.write_bytes(response_raw)
    response_mtime = response_path.stat().st_mtime_ns
    transport = FileBridgeTransport(
        paths=harness.paths,
        root_lock=harness.lock,
        process_inspector=harness.inspector,
        catalog=harness.catalog,
        database=harness.database,
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
        response_poll_seconds=0.001,
    )
    original_read = Path.read_bytes
    original_unlink = Path.unlink

    def guarded_read(path: Path) -> bytes:
        if path in {harness.paths.inbox, response_path}:
            raise AssertionError("session gate read managed request/response bytes")
        return original_read(path)

    def guarded_unlink(path: Path, *, missing_ok: bool = False) -> None:
        if path in {harness.paths.inbox, response_path}:
            raise AssertionError("session gate unlinked managed request/response bytes")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(MutationExchange, "__init__", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(ManifestCatalog, "resolve_running", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(Path, "read_bytes", guarded_read)
    monkeypatch.setattr(Path, "unlink", guarded_unlink)

    with pytest.raises(BridgeError) as caught:
        async with transport.session():
            pytest.fail("foreign/corrupt session unexpectedly entered")
    assert caught.value.code is code
    assert original_read(harness.paths.pending_file) == raw
    assert harness.paths.pending_file.stat().st_mtime_ns == pending_mtime
    assert original_read(harness.paths.inbox) == inbox_raw
    assert harness.paths.inbox.stat().st_mtime_ns == inbox_mtime
    assert original_read(response_path) == response_raw
    assert response_path.stat().st_mtime_ns == response_mtime
    assert not harness.paths.sqlite_file.exists()
    async with harness.lock:
        harness.lock.require_acquired()


@pytest.mark.parametrize(
    ("operation", "arguments", "trusted"),
    [
        ("unit.set", {"unit_guid": "UNIT-1", "name": "Renamed"}, None),
        (
            "unit.set",
            {
                "unit_guid": "UNIT-1",
                "course": [{"latitude": 1.5, "longitude": 2.5}],
            },
            None,
        ),
        (
            "unit.set",
            {
                "unit_guid": "UNIT-1",
                "course": [{"latitude": 1.5, "longitude": 2.5, "altitude": None}],
            },
            None,
        ),
        (
            "unit.delete",
            {"unit_guid": "UNIT-1"},
            {"confirmation_proof": "f" * 64},
        ),
        (
            "compat.probe.step",
            {
                "step": "apply-profile",
                "safe_payload_bytes": 4096,
                "verified_ledger_entries": 32,
                "effective_ledger_capacity": 32,
            },
            None,
        ),
    ],
)
@pytest.mark.asyncio
async def test_mutation_destructive_and_dynamic_to_mutation_cross_command_boundary(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    arguments: dict[str, object],
    trusted: dict[str, object] | None,
) -> None:
    command = _command(
        harness,
        operation,
        arguments,
        trusted_enrichment=trusted,
    )
    sentinel = BridgeError(ErrorCode.STATE_CONFLICT, "reached duplicate check")
    trace: list[str] = []
    cleanup: list[Path] = []
    original_load = PendingJournalStore.load
    original_resolve = ManifestCatalog.resolve_running

    def traced_load(store: PendingJournalStore) -> object:
        trace.append("journal.load")
        return original_load(store)

    def traced_resolve(catalog: ManifestCatalog, release_id: str) -> ReleaseBinding:
        trace.append("catalog.resolve_running")
        return original_resolve(catalog, release_id)

    def stop_at_duplicate_check(_request_id: UUID) -> None:
        trace.append("ledger.get_request")
        raise sentinel

    monkeypatch.setattr(PendingJournalStore, "load", traced_load)
    monkeypatch.setattr(ManifestCatalog, "resolve_running", traced_resolve)
    monkeypatch.setattr(harness.ledger, "get_request", stop_at_duplicate_check)
    monkeypatch.setattr(PendingJournalStore, "save", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    if operation == "unit.delete":
        assert "confirmation_proof" not in command.invocation.public_arguments.model_dump()
        assert command.invocation.wire_arguments.model_dump()["confirmation_proof"] == "f" * 64
    if operation == "unit.set" and "course" in arguments:
        public_course = cast(Any, command.invocation.public_arguments).course
        wire_course = cast(Any, command.invocation.wire_arguments).course
        assert len(public_course) == len(wire_course) == 1
        expected_fields = {"latitude", "longitude"}
        if "altitude" in cast(list[dict[str, object]], arguments["course"])[0]:
            expected_fields.add("altitude")
        assert public_course[0].model_fields_set == expected_fields
        assert wire_course[0].model_fields_set == expected_fields
    if operation == "compat.probe.step":
        assert command.invocation.contract.base_class is OperationClass.DYNAMIC
        assert command.invocation.effective_class is OperationClass.MUTATION
        assert command.invocation.recovery_schema is not None
        assert (
            command.invocation.result_schema.schema_id
            != command.invocation.recovery_schema.schema_id
        )
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await _exchange(harness, queue_response_cleanup=cleanup.append).run(command)
    assert caught is not None
    assert caught.value is sentinel
    assert trace == [
        "journal.load",
        "catalog.resolve_running",
        "ledger.get_request",
    ]
    assert cleanup == []


def _unchecked_command_update(
    command: ExchangeCommand,
    **changes: object,
) -> ExchangeCommand:
    for field, value in changes.items():
        object.__setattr__(command, field, value)
    return command


def _clone_invocation(
    invocation: ResolvedInvocation,
    **changes: object,
) -> ResolvedInvocation:
    values: dict[str, object] = {
        "contract": invocation.contract,
        "wire_arguments": invocation.wire_arguments,
        "effective_class": invocation.effective_class,
        "result_schema": invocation.result_schema,
        "recovery_schema": invocation.recovery_schema,
        "public_result_recipe": object.__getattribute__(
            invocation,
            "_public_result_recipe",
        ),
        "public_arguments": invocation.public_arguments,
    }
    values.update(changes)
    factory = cast(Any, ResolvedInvocation._from_resolved_recipe_parts)  # pyright: ignore[reportPrivateUsage]
    return cast(ResolvedInvocation, factory(**values))


def _exact_type_subclass_command(harness: Harness, case: str) -> ExchangeCommand:
    command = _command(harness)
    if case == "command":
        return _ExchangeCommandSubclass(
            request_id=command.request_id,
            body=command.body,
            invocation=command.invocation,
            runtime_snapshot=command.runtime_snapshot,
            timeout=command.timeout,
        )
    if case == "body":
        body = _RequestBodySubclass.model_validate(
            command.body.model_dump(mode="python", round_trip=True, warnings=False)
        )
        return _unchecked_command_update(command, body=body)
    if case == "invocation":
        invocation = command.invocation
        factory = cast(Any, _ResolvedInvocationSubclass._from_resolved_recipe_parts)  # pyright: ignore[reportPrivateUsage]
        subclass = cast(
            ResolvedInvocation,
            factory(
                contract=invocation.contract,
                wire_arguments=invocation.wire_arguments,
                effective_class=invocation.effective_class,
                result_schema=invocation.result_schema,
                recovery_schema=invocation.recovery_schema,
                public_result_recipe=object.__getattribute__(
                    invocation,
                    "_public_result_recipe",
                ),
                public_arguments=invocation.public_arguments,
            ),
        )
        return _unchecked_command_update(command, invocation=subclass)
    if case == "request_id":
        return _unchecked_command_update(
            command,
            request_id=_UuidSubclass(str(command.request_id)),
        )
    if case == "lineage_id":
        body = command.body.model_copy(
            update={"expected_lineage_id": _UuidSubclass(str(command.body.expected_lineage_id))}
        )
        return _unchecked_command_update(command, body=body)
    assert case == "activation_id"
    body = command.body.model_copy(
        update={"expected_activation_id": _UuidSubclass(str(command.body.expected_activation_id))}
    )
    return _unchecked_command_update(command, body=body)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["command", "body", "invocation", "request_id", "lineage_id", "activation_id"],
)
async def test_exact_runtime_command_body_invocation_and_uuid_types_reject_subclasses_before_io(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    command = _exact_type_subclass_command(harness, case)
    response_path = harness.paths.response_path(command.request_id)
    original_exists = Path.exists
    cleanup: list[Path] = []
    exchange = _exchange(harness, queue_response_cleanup=cleanup.append)

    def guarded_exists(path: Path) -> bool:
        if path == response_path:
            raise AssertionError("subclass command reached response-slot inspection")
        return original_exists(path)

    monkeypatch.setattr(ManifestCatalog, "resolve_running", _forbidden)
    monkeypatch.setattr(PendingJournalStore, "save", _forbidden)
    monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(Path, "exists", guarded_exists)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
    assert caught is not None
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert caught.value.message == "invalid file-bridge mutation command"
    assert not original_exists(harness.paths.pending_file)
    assert not original_exists(harness.paths.sqlite_file)
    assert not original_exists(harness.paths.inbox)
    assert cleanup == []
    assert not exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]


def _invalid_boundary_command(harness: Harness, case: str) -> ExchangeCommand:
    command = _command(harness)
    if case == "status":
        return _command(
            harness,
            "bridge.status",
            {},
            trusted_enrichment={
                "activation_candidate": UUID("55555555-5555-4555-8555-555555555555")
            },
        )
    if case == "read":
        return _command(harness, "scenario.get", {})
    if case == "local_mutation":
        return _command(harness, "bridge.uninstall", {"phase": "command"})
    if case in {"reconcile", "dynamic"}:
        object.__setattr__(
            command.invocation,
            "effective_class",
            OperationClass.RECONCILE if case == "reconcile" else OperationClass.DYNAMIC,
        )
        return command
    if case == "null_context":
        body = command.body.model_copy(
            update={"expected_lineage_id": None, "expected_activation_id": None}
        )
        return _unchecked_command_update(command, body=body)
    if case == "lineage_only":
        body = command.body.model_copy(update={"expected_activation_id": None})
        return _unchecked_command_update(command, body=body)
    if case == "activation_only":
        body = command.body.model_copy(update={"expected_lineage_id": None})
        return _unchecked_command_update(command, body=body)
    if case == "uuid_v1":
        return _unchecked_command_update(
            command,
            request_id=UUID("aaaaaaaa-0000-1000-8000-000000000001"),
        )
    if case.startswith("timeout_"):
        value: object = {
            "timeout_bool": True,
            "timeout_string": "0.1",
            "timeout_negative": -0.1,
            "timeout_nan": math.nan,
            "timeout_inf": math.inf,
        }[case]
        return _unchecked_command_update(command, timeout=value)
    if case == "nested_arguments_mutation":
        command.body.arguments["name"] = cast(Any, {"nested": "drift"})
        return command
    if case == "constructed_operation_drift":
        body_tree = command.body.model_dump(mode="python", round_trip=True)
        body_tree["operation"] = "unit.add"
        body = RequestBody.model_construct(**body_tree)
        return _unchecked_command_update(command, body=body)
    if case == "body_runtime_drift":
        body = command.body.model_copy(update={"release_id": "e" * 64})
        return _unchecked_command_update(command, body=body)
    assert case == "snapshot_derived_identity_drift"
    snapshot_tree = command.runtime_snapshot.model_dump(mode="python", round_trip=True)
    snapshot_tree["release_id"] = "e" * 64
    snapshot = RuntimeSnapshot.model_construct(**snapshot_tree)
    return _unchecked_command_update(command, runtime_snapshot=snapshot)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "status",
        "read",
        "local_mutation",
        "reconcile",
        "dynamic",
        "null_context",
        "lineage_only",
        "activation_only",
        "uuid_v1",
        "timeout_bool",
        "timeout_string",
        "timeout_negative",
        "timeout_nan",
        "timeout_inf",
        "nested_arguments_mutation",
        "constructed_operation_drift",
        "body_runtime_drift",
        "snapshot_derived_identity_drift",
        "catalog_snapshot_mismatch",
    ],
)
async def test_invalid_class_target_context_uuid_timeout_and_body_snapshot_are_rejected_pre_durability(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    command = (
        _command(harness)
        if case == "catalog_snapshot_mismatch"
        else _invalid_boundary_command(harness, case)
    )
    if case == "catalog_snapshot_mismatch":
        other = RuntimeSnapshot.create(
            runtime_version="0.1.1",
            runtime_asset_sha256="a" * 64,
            operation_manifest_sha256=MANIFEST_SHA256,
            host_contract_sha256="c" * 64,
            dependency_lock_sha256="d" * 64,
        )
        binding = ReleaseBinding(snapshot=other, registry=OPERATION_REGISTRY)

        def resolve_other(_release_id: str) -> ReleaseBinding:
            return binding

        monkeypatch.setattr(harness.catalog, "resolve_running", resolve_other)
    original_exists = Path.exists
    cleanup: list[Path] = []
    response_path = harness.paths.response_path(command.request_id)

    def guarded_exists(path: Path) -> bool:
        if path == response_path:
            raise AssertionError("invalid command reached response-slot inspection")
        return original_exists(path)

    monkeypatch.setattr(PendingJournalStore, "save", _forbidden)
    monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(Path, "exists", guarded_exists)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await _exchange(harness, queue_response_cleanup=cleanup.append).run(command)
    assert caught is not None
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert not original_exists(harness.paths.sqlite_file)
    assert not original_exists(harness.paths.pending_file)
    assert not original_exists(harness.paths.inbox)
    assert cleanup == []


def _forged_invocation_command(harness: Harness, case: str) -> ExchangeCommand:
    if case == "nested_public_fields_set_drift":
        command = _command(
            harness,
            arguments={
                "unit_guid": "UNIT-1",
                "course": [{"latitude": 1.5, "longitude": 2.5}],
            },
        )
        explicit_default = _command(
            harness,
            arguments={
                "unit_guid": "UNIT-1",
                "course": [{"latitude": 1.5, "longitude": 2.5, "altitude": None}],
            },
        )
        public = cast(Any, command.invocation.public_arguments)
        explicit_public = cast(Any, explicit_default.invocation.public_arguments)
        assert public.model_dump(mode="json") == explicit_public.model_dump(mode="json")
        assert public.model_fields_set == explicit_public.model_fields_set
        assert public.course[0].model_fields_set == {"latitude", "longitude"}
        assert explicit_public.course[0].model_fields_set == {
            "latitude",
            "longitude",
            "altitude",
        }
        forged = _clone_invocation(
            command.invocation,
            public_arguments=explicit_default.invocation.public_arguments,
        )
        return _unchecked_command_update(command, invocation=forged)

    command = _command(harness)
    invocation = command.invocation
    other_value = _command(
        harness,
        arguments={"unit_guid": "UNIT-1", "name": "Other name"},
    ).invocation
    explicit_default = _command(
        harness,
        arguments={"unit_guid": "UNIT-1", "name": "Renamed", "speed": None},
    ).invocation
    unit_add = _command(
        harness,
        "unit.add",
        {
            "side_guid": "SIDE-1",
            "unit_type": "Aircraft",
            "dbid": 1,
            "name": "Created unit",
            "latitude": 1.5,
            "longitude": 2.5,
        },
    ).invocation
    if case == "wrong_contract":
        read = _command(harness, "scenario.get", {}).invocation
        forged = _clone_invocation(invocation, contract=read.contract)
    elif case == "wire_snapshot_drift":
        forged = _clone_invocation(invocation, wire_arguments=other_value.wire_arguments)
    elif case == "public_value_drift":
        forged = _clone_invocation(invocation, public_arguments=other_value.public_arguments)
    elif case == "public_fields_set_drift":
        assert invocation.public_arguments.model_dump(
            mode="json"
        ) == explicit_default.public_arguments.model_dump(mode="json")
        assert (
            invocation.public_arguments.model_fields_set
            != explicit_default.public_arguments.model_fields_set
        )
        forged = _clone_invocation(
            invocation,
            public_arguments=explicit_default.public_arguments,
        )
    elif case == "effective_class_drift":
        forged = _clone_invocation(
            invocation,
            effective_class=OperationClass.DESTRUCTIVE,
        )
    elif case == "result_is_recovery_role":
        assert invocation.recovery_schema is not None
        forged = _clone_invocation(
            invocation,
            result_schema=invocation.recovery_schema,
        )
    elif case == "recovery_is_result_role":
        forged = _clone_invocation(
            invocation,
            recovery_schema=invocation.result_schema,
        )
    elif case == "foreign_schema_binding":
        forged = _clone_invocation(
            invocation,
            result_schema=unit_add.result_schema,
        )
    else:
        assert case == "missing_recovery_schema"
        forged = _clone_invocation(invocation, recovery_schema=None)
    return _unchecked_command_update(command, invocation=forged)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "wrong_contract",
        "wire_snapshot_drift",
        "public_value_drift",
        "public_fields_set_drift",
        "nested_public_fields_set_drift",
        "effective_class_drift",
        "result_is_recovery_role",
        "recovery_is_result_role",
        "foreign_schema_binding",
        "missing_recovery_schema",
    ],
)
async def test_forged_resolved_invocation_public_wire_result_recovery_and_role_bindings_are_rejected(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    command = _forged_invocation_command(harness, case)
    original_exists = Path.exists
    cleanup: list[Path] = []
    response_path = harness.paths.response_path(command.request_id)

    def guarded_exists(path: Path) -> bool:
        if path == response_path:
            raise AssertionError("forged invocation reached response-slot inspection")
        return original_exists(path)

    monkeypatch.setattr(PendingJournalStore, "save", _forbidden)
    monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(Path, "exists", guarded_exists)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await _exchange(harness, queue_response_cleanup=cleanup.append).run(command)
    assert caught is not None
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert not original_exists(harness.paths.sqlite_file)
    assert not original_exists(harness.paths.pending_file)
    assert not original_exists(harness.paths.inbox)
    assert cleanup == []


@pytest.mark.asyncio
async def test_preexisting_response_slot_precedes_ledger_and_is_never_touched(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command(harness)
    response_path = harness.paths.response_path(command.request_id)
    response_path.parent.mkdir(parents=True, exist_ok=True)
    sentinel = b"caller-owned-response-slot"
    response_path.write_bytes(sentinel)
    before_mtime = response_path.stat().st_mtime_ns
    cleanup: list[Path] = []
    response_unlink_attempts = 0
    original_unlink = Path.unlink

    def guarded_unlink(path: Path, *, missing_ok: bool = False) -> None:
        nonlocal response_unlink_attempts
        if path == response_path:
            response_unlink_attempts += 1
            return
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
    monkeypatch.setattr(PendingJournalStore, "save", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(Path, "unlink", guarded_unlink)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await _exchange(harness, queue_response_cleanup=cleanup.append).run(command)
    assert caught is not None
    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.message == "response path already exists before mutation publication"
    assert caught.value.details == {"response_path": str(response_path)}
    assert response_path.read_bytes() == sentinel
    assert response_path.stat().st_mtime_ns == before_mtime
    assert response_unlink_attempts == 0
    assert not harness.paths.pending_file.exists()
    assert not harness.paths.sqlite_file.exists()
    assert cleanup == []


@pytest.mark.asyncio
@pytest.mark.parametrize("same_hash", [True, False])
async def test_any_existing_request_row_exact_or_different_hash_blocks_without_mutation(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    same_hash: bool,
) -> None:
    command = _command(harness)
    old_command = (
        command
        if same_hash
        else _command(
            harness,
            arguments={"unit_guid": "UNIT-1", "name": "Older request"},
        )
    )
    harness.ledger.insert_prepared(_request_record(harness, old_command))
    before = harness.ledger.get_request(command.request_id)
    assert before is not None
    cleanup: list[Path] = []
    monkeypatch.setattr(PendingJournalStore, "save", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await _exchange(harness, queue_response_cleanup=cleanup.append).run(command)
    assert caught is not None
    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert harness.ledger.get_request(command.request_id) == before
    assert not harness.paths.pending_file.exists()
    assert not harness.paths.inbox.exists()
    assert cleanup == []


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["response_exists", "duplicate_lookup"])
async def test_predurable_preflight_ordinary_error_is_fixed_and_channel_remains_reusable(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    transport = FileBridgeTransport(
        paths=harness.paths,
        root_lock=harness.lock,
        process_inspector=harness.inspector,
        catalog=harness.catalog,
        database=harness.database,
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
        response_poll_seconds=0.001,
    )
    first = _command(harness)
    second = _command(
        harness,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee9002"),
    )
    injected: Exception = OSError() if boundary == "response_exists" else RuntimeError()
    sentinel = BridgeError(ErrorCode.STATE_CONFLICT, "safe follow-up reached duplicate lookup")
    first_response_path = harness.paths.response_path(first.request_id)
    original_exists = Path.exists
    duplicate_calls = 0

    def injected_exists(path: Path) -> bool:
        if boundary == "response_exists" and path == first_response_path:
            raise injected
        return original_exists(path)

    def injected_get_request(_request_id: UUID) -> None:
        nonlocal duplicate_calls
        duplicate_calls += 1
        if boundary == "duplicate_lookup" and duplicate_calls == 1:
            raise injected
        raise sentinel

    monkeypatch.setattr(Path, "exists", injected_exists)
    monkeypatch.setattr(transport.ledger, "get_request", injected_get_request)
    monkeypatch.setattr(transport.journals, "save", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)

    async with transport.session() as channel:
        concrete = cast(_FileBridgeChannel, channel)
        with pytest.raises(BridgeError) as first_error:
            await channel.exchange(first)
        assert first_error.value.code is ErrorCode.STATE_CONFLICT
        assert first_error.value.message == "file-bridge mutation exchange failed"
        assert first_error.value.details == {"type": type(injected).__name__}
        assert first_error.value.__cause__ is injected
        assert not concrete._poisoned  # pyright: ignore[reportPrivateUsage]
        assert not concrete._mutation_exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]
        assert concrete._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]

        monkeypatch.setattr(Path, "exists", original_exists)
        with pytest.raises(BridgeError) as second_error:
            await channel.exchange(second)
        assert second_error.value is sentinel
        assert not concrete._poisoned  # pyright: ignore[reportPrivateUsage]
        assert not concrete._mutation_exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]
        assert concrete._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]

    assert duplicate_calls == (2 if boundary == "duplicate_lookup" else 1)
    assert not original_exists(harness.paths.pending_file)
    assert not original_exists(harness.paths.sqlite_file)
    assert not original_exists(harness.paths.inbox)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["request_id", "uuid_v1", "non_uuid"])
async def test_invalid_generated_delivery_id_fails_once_before_durability_without_poison(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    command = _command(harness)
    generated = {
        "request_id": command.request_id,
        "uuid_v1": UUID("aaaaaaaa-0000-1000-8000-000000000001"),
        "non_uuid": cast(UUID, "not-a-uuid"),
    }[case]
    calls = 0
    cleanup: list[Path] = []
    exchange = _exchange(harness, queue_response_cleanup=cleanup.append)

    def invalid_uuid4() -> UUID:
        nonlocal calls
        calls += 1
        return generated

    def no_duplicate(_request_id: UUID) -> None:
        return None

    monkeypatch.setattr(mutation_module.uuid, "uuid4", invalid_uuid4)
    monkeypatch.setattr(harness.ledger, "get_request", no_duplicate)
    monkeypatch.setattr(PendingJournalStore, "save", _forbidden)
    monkeypatch.setattr(RequestLedger, "insert_prepared", _forbidden)
    monkeypatch.setattr(RequestLedger, "insert_delivery", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
    assert caught is not None
    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.message == "UUID generator returned an invalid mutation delivery ID"
    assert calls == 1
    assert not harness.paths.pending_file.exists()
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.inbox.exists()
    assert cleanup == []
    assert not exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]


class _StopAfterPublication(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [1, 0], ids=["exact_int", "zero"])
async def test_exact_int_and_zero_timeout_reach_h7_first_poll(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    timeout: int,
) -> None:
    command = _unchecked_command_update(_command(harness), timeout=timeout)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    stop = _StopAfterPublication("H7 first poll reached")
    process_calls = 0

    def stop_on_waiter_process_check(command_exe: Path) -> tuple[ProcessInfo, ...]:
        nonlocal process_calls
        assert command_exe == harness.paths.command_exe
        process_calls += 1
        if process_calls == 1:
            return (harness.process,)
        raise stop

    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(harness.inspector, "matching_processes", stop_on_waiter_process_check)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    caught: pytest.ExceptionInfo[_StopAfterPublication] | None = None
    loaded = None
    async with harness.lock:
        with pytest.raises(_StopAfterPublication) as caught:
            await _exchange(harness).run(command)
        loaded = harness.journals.load()
    assert caught is not None
    assert caught.value is stop
    assert process_calls == 2
    assert loaded is not None
    assert loaded.journal.original.state is PendingPhase.PUBLISHED
    assert harness.paths.inbox.exists()


@pytest.mark.asyncio
async def test_revision_zero_request_intent_process_publish_and_revision_one_order_are_exact(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    clock_values = (100_000_000, 101_000_000)
    clock_calls = 0
    uuid_calls = 0
    prepare_calls = 0
    render_calls = 0

    original_prepare_delivery = mutation_module.prepare_delivery
    original_render_delivery_lua = mutation_module.render_delivery_lua

    def counted_prepare_delivery(
        body: RequestBody,
        *,
        request_id: UUID,
        delivery_id: UUID,
        delivery_kind: str,
    ) -> PreparedDelivery:
        nonlocal prepare_calls
        prepare_calls += 1
        return cast(Any, original_prepare_delivery)(
            body,
            request_id=request_id,
            delivery_id=delivery_id,
            delivery_kind=delivery_kind,
        )

    def counted_render_delivery_lua(
        delivery: PreparedDelivery,
        runtime_snapshot: RuntimeSnapshot,
    ) -> bytes:
        nonlocal render_calls
        render_calls += 1
        return original_render_delivery_lua(delivery, runtime_snapshot)

    def fixed_time_ns() -> int:
        nonlocal clock_calls
        value = clock_values[clock_calls]
        clock_calls += 1
        return value

    def fixed_uuid4() -> UUID:
        nonlocal uuid_calls
        uuid_calls += 1
        return DELIVERY_ID

    monkeypatch.setattr(mutation_module.time, "time_ns", fixed_time_ns)
    monkeypatch.setattr(mutation_module.uuid, "uuid4", fixed_uuid4)
    monkeypatch.setattr(mutation_module, "prepare_delivery", counted_prepare_delivery)
    monkeypatch.setattr(mutation_module, "render_delivery_lua", counted_render_delivery_lua)
    trace: list[str] = []
    journal_candidates: list[PendingJournal] = []
    request_candidates: list[RequestRecord] = []
    intent_candidates: list[DeliveryIntent] = []
    waiter_arguments: list[dict[str, object]] = []
    stop = _StopAfterPublication("publication boundary reached")
    original_load = harness.journals.load
    original_save = harness.journals.save
    original_get_request = harness.ledger.get_request
    original_insert_prepared = harness.ledger.insert_prepared
    original_insert_delivery = harness.ledger.insert_delivery
    original_mark_published = harness.ledger.mark_delivery_published
    original_transition = harness.ledger.transition
    original_process = harness.inspector.matching_processes
    original_publish = harness.inbox.publish_delivery

    def traced_load() -> object:
        if not journal_candidates:
            trace.append("journal.load")
        return original_load()

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        trace.append(f"journal.{journal.original.state.value}")
        journal_candidates.append(journal)
        return original_save(journal, expected_revisions=expected_revisions)

    def traced_get_request(request_id: UUID) -> RequestRecord | None:
        trace.append("ledger.duplicate_check")
        return original_get_request(request_id)

    def traced_insert_prepared(record: RequestRecord) -> None:
        trace.append("ledger.request_prepared")
        request_candidates.append(record)
        original_insert_prepared(record)

    def traced_insert_delivery(intent: DeliveryIntent) -> None:
        trace.append("ledger.intent_prepared")
        intent_candidates.append(intent)
        original_insert_delivery(intent)

    def traced_process(command_exe: Path) -> tuple[ProcessInfo, ...]:
        trace.append("process.prepublication")
        return original_process(command_exe)

    def traced_publish(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        assert publisher is harness.inbox
        loaded = original_load()
        assert loaded is not None
        assert loaded.journal.original.state is PendingPhase.PREPARED
        assert loaded.journal.original.revision == 0
        request = original_get_request(command.request_id)
        marker = harness.ledger.get_delivery(DELIVERY_ID)
        assert request is not None and request.state is HostRequestState.PREPARED
        assert marker is not None and marker.published_at_ms is None
        assert loaded.journal.original.delivery_intents[0].published_at_ms is None
        assert clock_calls == 1
        trace.append("inbox.request_replaced")
        original_publish(cast(Any, delivery), runtime_snapshot=runtime_snapshot)

    def traced_mark(delivery_id: UUID, *, published_at_ms: int) -> object:
        trace.append("ledger.delivery_published")
        return original_mark_published(delivery_id, published_at_ms=published_at_ms)

    def traced_transition(request_id: UUID, **kwargs: object) -> RequestRecord:
        trace.append(f"ledger.request_{cast(HostRequestState, kwargs['new_state']).value}")
        return cast(Any, original_transition)(request_id, **kwargs)

    class StopWaiter:
        def __init__(self, **kwargs: object) -> None:
            trace.append("waiter.constructed")
            waiter_arguments.append(dict(kwargs))
            assert kwargs["expected_process"] is harness.process
            assert kwargs["response_path"] == harness.paths.response_path(command.request_id)

        async def wait(self, timeout_seconds: float) -> ResponseArtifact:
            trace.append("waiter.wait")
            assert timeout_seconds == command.timeout
            raise stop

    monkeypatch.setattr(harness.journals, "load", traced_load)
    monkeypatch.setattr(harness.journals, "save", traced_save)
    monkeypatch.setattr(harness.ledger, "get_request", traced_get_request)
    monkeypatch.setattr(harness.ledger, "insert_prepared", traced_insert_prepared)
    monkeypatch.setattr(harness.ledger, "insert_delivery", traced_insert_delivery)
    monkeypatch.setattr(harness.ledger, "mark_delivery_published", traced_mark)
    monkeypatch.setattr(harness.ledger, "transition", traced_transition)
    monkeypatch.setattr(harness.inspector, "matching_processes", traced_process)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish)
    monkeypatch.setattr(mutation_module, "ResponseWaiter", StopWaiter, raising=False)

    caught: pytest.ExceptionInfo[_StopAfterPublication] | None = None
    async with harness.lock:
        with pytest.raises(_StopAfterPublication) as caught:
            await _exchange(harness).run(command)
    assert caught is not None
    assert caught.value is stop
    assert trace[:12] == [
        "journal.load",
        "ledger.duplicate_check",
        "journal.prepared",
        "ledger.request_prepared",
        "ledger.intent_prepared",
        "process.prepublication",
        "inbox.request_replaced",
        "journal.published",
        "ledger.delivery_published",
        "ledger.request_published",
        "waiter.constructed",
        "waiter.wait",
    ]
    assert len(journal_candidates) == 2
    prepared_journal, published_journal = journal_candidates
    expected_prepared = _prepared_journal(harness, command)
    assert prepared_journal == expected_prepared
    assert prepared_journal.original.revision == 0
    assert prepared_journal.original.state is PendingPhase.PREPARED
    assert published_journal.original.revision == 1
    assert published_journal.original.state is PendingPhase.PUBLISHED
    assert prepared_journal.header == published_journal.header
    assert prepared_journal.header == PendingJournalHeader(
        format="cmo-agent-bridge/pending-journal",
        header_version=1,
        root_key=harness.paths.root_key,
        required_release_id=command.runtime_snapshot.release_id,
    )
    assert len(request_candidates) == len(intent_candidates) == 1
    request = request_candidates[0]
    intent = intent_candidates[0]
    assert request == _request_record(
        harness,
        command,
        created_at_ms=100,
    )
    assert tuple(DeliveryIntent.model_fields) == (
        "request_id",
        "delivery_id",
        "delivery_kind",
        "original_request_delivery_id",
        "body_json",
        "request_hash",
        "runtime_snapshot",
        "result_schema_id",
        "recovery_schema_id",
        "intended_at_ms",
        "published_at_ms",
        "rendered_inbox_sha256",
        "rendered_inbox_size_bytes",
        "response_filename",
    )
    assert tuple(PendingExchange.model_fields) == (
        "request_id",
        "request_hash",
        "operation",
        "effective_class",
        "body_json",
        "runtime_snapshot",
        "result_schema_id",
        "recovery_schema_id",
        "expected_lineage_id",
        "expected_activation_id",
        "delivery_intents",
        "response_artifact",
        "settlement",
        "original_target_request_id",
        "original_target_request_hash",
        "revision",
        "state",
        "created_at_ms",
        "updated_at_ms",
    )
    assert intent == expected_prepared.original.delivery_intents[0]
    assert intent.request_id == command.request_id
    assert intent.delivery_id.version == 4
    assert intent.delivery_id != command.request_id
    assert intent.delivery_kind == "request"
    assert intent.original_request_delivery_id == intent.delivery_id
    assert intent.body_json == canonical_body_bytes(command.body).decode("utf-8")
    assert intent.request_hash == hashlib.sha256(canonical_body_bytes(command.body)).hexdigest()
    assert intent.runtime_snapshot == command.runtime_snapshot
    assert intent.result_schema_id == command.invocation.result_schema.schema_id
    assert command.invocation.recovery_schema is not None
    assert intent.recovery_schema_id == command.invocation.recovery_schema.schema_id
    assert intent.published_at_ms is None
    expected_delivery = prepare_delivery(
        command.body,
        request_id=command.request_id,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
    )
    expected_rendered = render_delivery_lua(expected_delivery, command.runtime_snapshot)
    rendered = harness.paths.inbox.read_bytes()
    assert rendered == expected_rendered
    assert hashlib.sha256(rendered).hexdigest() == intent.rendered_inbox_sha256
    assert len(rendered) == intent.rendered_inbox_size_bytes
    assert intent.response_filename == harness.paths.response_path(command.request_id).name
    published_intent = published_journal.original.delivery_intents[0]
    assert intent.intended_at_ms == request.created_at_ms == request.updated_at_ms == 100
    assert published_intent.published_at_ms == 101
    assert published_journal.original.created_at_ms == 100
    assert published_journal.original.updated_at_ms == 101
    expected_published_intent = DeliveryIntent.model_validate(
        {
            **intent.model_dump(mode="python", round_trip=True, warnings=False),
            "published_at_ms": 101,
        }
    )
    expected_published = _journal_with_original(
        expected_prepared,
        delivery_intents=(expected_published_intent,),
        revision=1,
        state=PendingPhase.PUBLISHED,
        updated_at_ms=101,
    )
    assert published_journal == expected_published
    delivery = harness.ledger.get_delivery(intent.delivery_id)
    durable_request = harness.ledger.get_request(command.request_id)
    assert delivery is not None and durable_request is not None
    assert delivery == DeliveryRecord(
        delivery_id=DELIVERY_ID,
        request_id=command.request_id,
        delivery_kind="request",
        original_request_delivery_id=DELIVERY_ID,
        intended_at_ms=100,
        published_at_ms=101,
        rendered_inbox_sha256=intent.rendered_inbox_sha256,
        rendered_inbox_size_bytes=intent.rendered_inbox_size_bytes,
        response_filename=intent.response_filename,
        response_artifact=None,
        settlement=None,
    )
    expected_request_tree = request.model_dump(mode="python", round_trip=True, warnings=False)
    expected_request_tree.update(
        state=HostRequestState.PUBLISHED,
        updated_at_ms=101,
    )
    assert durable_request == RequestRecord.model_validate(expected_request_tree)
    assert len(waiter_arguments) == 1
    expectation = cast(ResponseExpectation, waiter_arguments[0]["expectation"])
    assert expectation.request_id == command.request_id
    assert expectation.allowed_deliveries == (
        AllowedDelivery(delivery_id=DELIVERY_ID, delivery_kind="request"),
    )
    assert expectation.request_hash == intent.request_hash
    assert expectation.status_bootstrap is False
    assert expectation.activation_candidate is None
    assert expectation.runtime_snapshot == command.runtime_snapshot
    assert expectation.invocation is command.invocation
    assert clock_calls == 2
    assert uuid_calls == 1
    assert prepare_calls == render_calls == 1


@pytest.mark.asyncio
async def test_unit_set_real_peer_completed_is_artifact_first_terminal_and_unlinked_after_unlock(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace: list[str] = []
    transport = _transport(harness)
    command = _command(
        harness,
        arguments={
            "unit_guid": "UNIT-1",
            "name": "海鹰 Alpha",
            "speed": 18.0,
        },
        timeout=0.3,
    )
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    schema_identity = (
        command.invocation.result_schema.schema_id,
        recovery_schema.schema_id,
    )
    assert schema_identity[0] != schema_identity[1]
    result_model = command.invocation.result_adapter.validate_python(
        {
            "unit_guid": "UNIT-1",
            "name": "海鹰 Alpha",
            "speed": 18.0,
            "altitude": None,
            "heading": None,
            "course": None,
        }
    )
    expected_result = cast(
        JsonValue,
        command.invocation.result_adapter.dump_python(result_model, mode="json"),
    )
    assert isinstance(expected_result, dict)
    expected_result_json = _canonical_json(expected_result).decode("utf-8")
    response_path = harness.paths.response_path(command.request_id)
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected_result))
    monkeypatch.setattr(peer, "result_for", _forbidden)

    journal_candidates: list[PendingJournal] = []
    request_candidates: list[RequestRecord] = []
    intent_candidates: list[DeliveryIntent] = []
    waiter_artifacts: list[ResponseArtifact] = []
    waiter_paths: list[Path] = []
    recorded_deliveries: list[DeliveryRecord] = []
    queued_paths: list[Path] = []
    uuid_calls = 0
    journal_load_calls = 0
    duplicate_check_calls = 0
    response_postcondition_reads = 0
    process_calls = 0
    publish_calls = 0
    mark_calls = 0
    response_record_calls = 0
    waiter_calls = 0
    recovery_validate_calls = 0
    journal_r2_saved = False
    artifact_sql_recorded = False
    request_response_accepted = False
    idle_returned = False
    journal_r3_saved = False
    journal_deleted = False
    unlock_finished = False

    original_enter = RootLock.__aenter__
    original_exit = RootLock.__aexit__
    original_load = transport.journals.load
    original_save = transport.journals.save
    original_delete = transport.journals.delete
    original_get_request = transport.ledger.get_request
    original_insert_prepared = transport.ledger.insert_prepared
    original_insert_delivery = transport.ledger.insert_delivery
    original_mark = transport.ledger.mark_delivery_published
    original_record_response = transport.ledger.record_response
    original_transition = transport.ledger.transition
    original_process = harness.inspector.matching_processes
    original_publish_delivery = InboxPublisher.publish_delivery
    original_publish_idle = InboxPublisher.publish_idle
    original_wait = ResponseWaiter.wait
    original_response_delete = cleanup_module._set_delete_disposition  # pyright: ignore[reportPrivateUsage]
    adapter_descriptor = inspect.getattr_static(SchemaBinding, "adapter")
    descriptor_get = getattr(adapter_descriptor, "__get__", None)
    assert callable(descriptor_get)

    def raw_response_columns() -> tuple[object, object, object, object, object]:
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            row = connection.execute(
                """
                SELECT response_at_ms,response_sha256,response_size_bytes,
                       accepted_response_json,settlement_json
                FROM deliveries WHERE delivery_id=?
                """,
                (str(DELIVERY_ID),),
            ).fetchone()
        assert row is not None
        return cast(tuple[object, object, object, object, object], tuple(row))

    def expected_response_columns(
        artifact: ResponseArtifact,
    ) -> tuple[int, str, int, bytes, bytes]:
        settlement = artifact.accepted_response.settlement
        assert settlement is not None
        return (
            artifact.accepted_at_ms,
            artifact.sha256,
            artifact.size_bytes,
            _canonical_json(artifact.accepted_response.model_dump(mode="json")),
            _canonical_json(settlement.model_dump(mode="json")),
        )

    async def traced_enter(lock: RootLock) -> RootLock:
        assert lock is harness.lock
        entered = await original_enter(lock)
        trace.append("session.lock_acquired")
        return entered

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        nonlocal unlock_finished
        assert lock is harness.lock
        lock.require_acquired()
        assert response_path.exists()
        if exc is None:
            assert journal_deleted
            assert len(queued_paths) == 1
        result = await original_exit(lock, exc_type, exc, traceback)
        unlock_finished = True
        trace.append("session.unlock")
        assert response_path.exists()
        return result

    def traced_load() -> object:
        nonlocal journal_load_calls
        result = original_load()
        journal_load_calls += 1
        if journal_load_calls == 1:
            trace.append("session.journal_gate")
        elif journal_load_calls == 2:
            trace.append("mutation.journal_recheck")
        return result

    def traced_get_request(request_id: UUID) -> RequestRecord | None:
        nonlocal duplicate_check_calls, response_postcondition_reads
        observed = original_get_request(request_id)
        if not request_response_accepted:
            duplicate_check_calls += 1
            assert duplicate_check_calls == 1
            assert request_id == command.request_id
            trace.append("mutation.ledger_duplicate_check")
        else:
            response_postcondition_reads += 1
            assert response_postcondition_reads == 1
            assert request_id == command.request_id
            assert observed is not None
            assert observed.state is HostRequestState.RESPONSE_ACCEPTED
            trace.append("ledger.request_response_observed")
        return observed

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal journal_r2_saved, journal_r3_saved
        phase = journal.original.state
        journal_candidates.append(journal)
        assert (
            journal.original.result_schema_id,
            journal.original.recovery_schema_id,
        ) == schema_identity
        assert all(
            (intent.result_schema_id, intent.recovery_schema_id) == schema_identity
            for intent in journal.original.delivery_intents
        )
        if phase is PendingPhase.PREPARED:
            assert journal.original.revision == 0
            assert expected_revisions is None
        elif phase is PendingPhase.PUBLISHED:
            assert journal.original.revision == 1
            assert expected_revisions == JournalRevisions(
                original=0,
                reconcile_attempt=None,
            )
        elif phase is PendingPhase.RESPONSE_ACCEPTED:
            assert journal.original.revision == 2
            assert expected_revisions == JournalRevisions(
                original=1,
                reconcile_attempt=None,
            )
            assert len(waiter_artifacts) == 1
            artifact = waiter_artifacts[0]
            assert response_path.exists()
            assert journal.original.response_artifact == artifact
            assert journal.original.settlement == artifact.accepted_response.settlement
            assert raw_response_columns() == (None, None, None, None, None)
            request = original_get_request(command.request_id)
            assert request is not None and request.state is HostRequestState.PUBLISHED
            assert recovery_validate_calls == 0
        elif phase is PendingPhase.IDLE_PUBLISHED:
            assert journal.original.revision == 3
            assert expected_revisions == JournalRevisions(
                original=2,
                reconcile_attempt=None,
            )
            assert response_path.exists()
            assert idle_returned
            assert recovery_validate_calls == 1
            assert request_response_accepted
            request = original_get_request(command.request_id)
            assert request is not None and request.state is HostRequestState.RESPONSE_ACCEPTED
        revisions = original_save(journal, expected_revisions=expected_revisions)
        durable_journal = original_load()
        assert durable_journal is not None and durable_journal.journal == journal
        assert (
            durable_journal.journal.original.result_schema_id,
            durable_journal.journal.original.recovery_schema_id,
        ) == schema_identity
        assert all(
            (intent.result_schema_id, intent.recovery_schema_id) == schema_identity
            for intent in durable_journal.journal.original.delivery_intents
        )
        event = {
            PendingPhase.PREPARED: "journal.prepared",
            PendingPhase.PUBLISHED: "journal.published",
            PendingPhase.RESPONSE_ACCEPTED: "journal.response_accepted",
            PendingPhase.IDLE_PUBLISHED: "journal.idle_published",
            PendingPhase.QUARANTINED: "journal.quarantined",
        }[phase]
        trace.append(event)
        if phase is PendingPhase.RESPONSE_ACCEPTED:
            journal_r2_saved = True
        elif phase is PendingPhase.IDLE_PUBLISHED:
            journal_r3_saved = True
        return revisions

    def traced_delete(expected: JournalDeleteExpectation) -> None:
        nonlocal journal_deleted
        assert expected == JournalDeleteExpectation(
            root_key=harness.paths.root_key,
            required_release_id=command.runtime_snapshot.release_id,
            original_request_id=command.request_id,
            reconcile_attempt_request_id=None,
            revisions=JournalRevisions(original=3, reconcile_attempt=None),
        )
        harness.lock.require_acquired()
        assert response_path.exists()
        assert journal_r3_saved
        request = original_get_request(command.request_id)
        assert request is not None and request.state is HostRequestState.COMPLETED
        loaded = original_load()
        assert loaded is not None
        assert loaded.journal.original.state is PendingPhase.IDLE_PUBLISHED
        assert loaded.journal.original.revision == 3
        original_delete(expected)
        assert not harness.paths.pending_file.exists()
        journal_deleted = True
        trace.append("journal.deleted")

    def traced_insert_prepared(record: RequestRecord) -> None:
        assert (record.result_schema_id, record.recovery_schema_id) == schema_identity
        request_candidates.append(record)
        original_insert_prepared(record)
        durable = original_get_request(record.request_id)
        assert durable is not None
        assert (durable.result_schema_id, durable.recovery_schema_id) == schema_identity
        trace.append("ledger.request_prepared")

    def traced_insert_delivery(intent: DeliveryIntent) -> None:
        assert (intent.result_schema_id, intent.recovery_schema_id) == schema_identity
        intent_candidates.append(intent)
        original_insert_delivery(intent)
        trace.append("ledger.intent_prepared")

    def traced_mark(delivery_id: UUID, *, published_at_ms: int) -> DeliveryRecord:
        nonlocal mark_calls
        mark_calls += 1
        result = original_mark(delivery_id, published_at_ms=published_at_ms)
        trace.append("ledger.delivery_published")
        return result

    def traced_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        nonlocal response_record_calls, artifact_sql_recorded
        response_record_calls += 1
        assert len(waiter_artifacts) == 1
        assert artifact is waiter_artifacts[0]
        assert journal_r2_saved
        assert not request_response_accepted
        assert recovery_validate_calls == 0
        assert response_path.exists()
        assert raw_response_columns() == (None, None, None, None, None)
        result = original_record_response(artifact)
        assert raw_response_columns() == expected_response_columns(artifact)
        assert result.delivery_id == DELIVERY_ID
        assert result.request_id == command.request_id
        assert result.response_artifact == artifact
        assert result.settlement == artifact.accepted_response.settlement
        recorded_deliveries.append(result)
        artifact_sql_recorded = True
        trace.append("ledger.response_recorded")
        return result

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal request_response_accepted
        assert request_id == command.request_id
        if new_state is HostRequestState.PUBLISHED:
            assert expected_states == frozenset({HostRequestState.PREPARED})
        elif new_state is HostRequestState.RESPONSE_ACCEPTED:
            assert expected_states == frozenset({HostRequestState.PUBLISHED})
            assert journal_r2_saved and artifact_sql_recorded
            assert recovery_validate_calls == 0
            assert response_path.exists()
            assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])
        elif new_state is HostRequestState.IDLE_PUBLISHED:
            assert expected_states == frozenset({HostRequestState.RESPONSE_ACCEPTED})
            assert recovery_validate_calls == 1
            assert idle_returned and journal_r3_saved
            assert response_path.exists()
        elif new_state is HostRequestState.COMPLETED:
            assert expected_states == frozenset({HostRequestState.IDLE_PUBLISHED})
            assert terminal_at_ms == updated_at_ms
            assert result_json == expected_result_json
            assert error_json is None and resolution_json is None
            assert response_path.exists()
        result = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        assert (result.result_schema_id, result.recovery_schema_id) == schema_identity
        event = {
            HostRequestState.PUBLISHED: "ledger.request_published",
            HostRequestState.RESPONSE_ACCEPTED: "ledger.request_response_accepted",
            HostRequestState.IDLE_PUBLISHED: "ledger.request_idle_published",
            HostRequestState.COMPLETED: "ledger.request_terminal",
            HostRequestState.QUARANTINED: "ledger.request_quarantined",
        }.get(new_state)
        if event is not None:
            trace.append(event)
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            request_response_accepted = True
        return result

    def traced_process(command_exe: Path) -> tuple[ProcessInfo, ...]:
        nonlocal process_calls
        process_calls += 1
        result = original_process(command_exe)
        if process_calls == 1:
            trace.append("session.process_pinned")
        elif process_calls == 2:
            trace.append("process.prepublication")
        else:
            trace.append("waiter.process_check")
        return result

    def traced_publish_delivery(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal publish_calls
        assert publisher is transport.inbox
        publish_calls += 1
        original_publish_delivery(
            publisher,
            delivery,
            runtime_snapshot=runtime_snapshot,
        )
        trace.append("inbox.request_replaced")

    def traced_publish_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_returned
        assert publisher is transport.inbox
        harness.lock.require_acquired()
        assert response_path.exists()
        assert journal_r2_saved and artifact_sql_recorded and request_response_accepted
        assert recovery_validate_calls == 1
        original_publish_idle(publisher)
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        idle_returned = True
        trace.append("inbox.idle_replaced")

    async def traced_wait(waiter: ResponseWaiter, timeout_seconds: float) -> ResponseArtifact:
        nonlocal waiter_calls
        waiter_calls += 1
        assert timeout_seconds == command.timeout
        waiter_path = waiter._response_path  # pyright: ignore[reportPrivateUsage]
        assert waiter_path == response_path
        waiter_paths.append(waiter_path)
        artifact = await original_wait(waiter, timeout_seconds)
        assert response_path.exists()
        assert "journal.published" in trace
        assert "peer.response_written" in trace
        raw_response = response_path.read_bytes()
        assert artifact.sha256 == hashlib.sha256(raw_response).hexdigest()
        assert artifact.size_bytes == len(raw_response)
        waiter_artifacts.append(artifact)
        trace.append("waiter.returned")
        return artifact

    class RecoveryAdapterSpy:
        def __init__(self, delegate: object) -> None:
            self._delegate = delegate

        def validate_python(self, value: object, *args: object, **kwargs: object) -> object:
            nonlocal recovery_validate_calls
            assert recovery_validate_calls == 0
            assert value == expected_result
            assert schema_identity == (
                command.invocation.result_schema.schema_id,
                cast(Any, command.invocation.recovery_schema).schema_id,
            )
            assert command.invocation.recovery_schema is recovery_schema
            assert journal_r2_saved and artifact_sql_recorded and request_response_accepted
            assert response_postcondition_reads == 1
            assert not idle_returned
            assert response_path.exists()
            assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])
            harness.lock.require_acquired()
            recovery_validate_calls += 1
            validated = cast(Any, self._delegate).validate_python(value, *args, **kwargs)
            trace.append("recovery_result_validated")
            return validated

        def __getattr__(self, name: str) -> object:
            return getattr(self._delegate, name)

    recovery_spy: RecoveryAdapterSpy | None = None

    def traced_schema_adapter(binding: SchemaBinding) -> object:
        nonlocal recovery_spy
        adapter = cast(Any, descriptor_get)(binding, SchemaBinding)
        if binding is recovery_schema:
            if recovery_spy is None:
                recovery_spy = RecoveryAdapterSpy(adapter)
            return recovery_spy
        return adapter

    def traced_response_delete(handle: int, path: Path) -> None:
        if path == response_path.resolve(strict=True):
            assert len(queued_paths) == 1
            assert queued_paths[0] == response_path
            assert len(waiter_paths) == 1 and waiter_paths[0] == response_path
            assert unlock_finished
            with pytest.raises(BridgeError):
                harness.lock.require_acquired()
            assert path.exists()
        original_response_delete(handle, path)
        if path == response_path.resolve(strict=False):
            trace.append("response.unlinked")

    def fixed_uuid4() -> UUID:
        nonlocal uuid_calls
        uuid_calls += 1
        return DELIVERY_ID

    monkeypatch.setattr(RootLock, "__aenter__", traced_enter)
    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)
    monkeypatch.setattr(transport.journals, "load", traced_load)
    monkeypatch.setattr(transport.journals, "save", traced_save)
    monkeypatch.setattr(transport.journals, "delete", traced_delete)
    monkeypatch.setattr(transport.ledger, "get_request", traced_get_request)
    monkeypatch.setattr(transport.ledger, "insert_prepared", traced_insert_prepared)
    monkeypatch.setattr(transport.ledger, "insert_delivery", traced_insert_delivery)
    monkeypatch.setattr(transport.ledger, "mark_delivery_published", traced_mark)
    monkeypatch.setattr(transport.ledger, "record_response", traced_record_response)
    monkeypatch.setattr(transport.ledger, "transition", traced_transition)
    monkeypatch.setattr(harness.inspector, "matching_processes", traced_process)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish_delivery)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_publish_idle)
    monkeypatch.setattr(ResponseWaiter, "wait", traced_wait)
    monkeypatch.setattr(
        SchemaBinding,
        "adapter",
        property(traced_schema_adapter),
        raising=False,
    )
    monkeypatch.setattr(mutation_module.uuid, "uuid4", fixed_uuid4)
    monkeypatch.setattr(cleanup_module, "_set_delete_disposition", traced_response_delete)
    assert recovery_validate_calls == 0

    returned: ResponseArtifact | None = None
    async with peer:
        async with transport.session() as channel:
            concrete = cast(_FileBridgeChannel, channel)
            runner = concrete._mutation_exchange  # pyright: ignore[reportPrivateUsage]
            original_queue = runner._queue_response_cleanup  # pyright: ignore[reportPrivateUsage]

            def traced_queue(path: Path) -> None:
                harness.lock.require_acquired()
                assert path == response_path
                assert len(waiter_paths) == 1 and path is waiter_paths[0]
                assert response_path.exists()
                assert journal_deleted
                assert not harness.paths.pending_file.exists()
                request = original_get_request(command.request_id)
                assert request is not None and request.state is HostRequestState.COMPLETED
                queued_paths.append(path)
                original_queue(path)
                trace.append("cleanup.queued")

            monkeypatch.setattr(runner, "_queue_response_cleanup", traced_queue)
            returned = await channel.exchange(command)
            assert response_path.exists()
            assert len(queued_paths) == 1
            assert concrete._cleanup_paths == [queued_paths[0]]  # pyright: ignore[reportPrivateUsage]

    assert returned is not None
    assert len(waiter_artifacts) == 1
    assert returned is waiter_artifacts[0]
    assert isinstance(returned.accepted_response.settlement, CompletedSettlement)
    assert returned.accepted_response.settlement.result == expected_result
    assert schema_identity == (
        command.invocation.result_schema.schema_id,
        cast(Any, command.invocation.recovery_schema).schema_id,
    )
    assert command.invocation.recovery_schema is recovery_schema
    assert recovery_validate_calls == 1
    assert not response_path.exists()
    assert harness.paths.inbox.read_bytes() == render_idle_lua()
    assert not harness.paths.pending_file.exists()

    assert [candidate.original.state for candidate in journal_candidates] == [
        PendingPhase.PREPARED,
        PendingPhase.PUBLISHED,
        PendingPhase.RESPONSE_ACCEPTED,
        PendingPhase.IDLE_PUBLISHED,
    ]
    assert [candidate.original.revision for candidate in journal_candidates] == [0, 1, 2, 3]
    prepared_journal, published_journal, accepted_journal, idle_journal = journal_candidates
    assert prepared_journal.header == published_journal.header
    assert published_journal.header == accepted_journal.header == idle_journal.header
    assert prepared_journal.original.response_artifact is None
    assert prepared_journal.original.settlement is None
    assert published_journal.original.response_artifact is None
    assert published_journal.original.settlement is None
    assert accepted_journal.original.response_artifact == returned
    assert accepted_journal.original.settlement == returned.accepted_response.settlement
    assert idle_journal.original.response_artifact == returned
    assert idle_journal.original.settlement == returned.accepted_response.settlement
    assert prepared_journal.original.delivery_intents[0].published_at_ms is None
    assert accepted_journal.original.updated_at_ms >= published_journal.original.updated_at_ms
    assert (
        published_journal.original.delivery_intents
        == accepted_journal.original.delivery_intents
        == idle_journal.original.delivery_intents
    )
    immutable_fields = (
        "request_id",
        "request_hash",
        "operation",
        "effective_class",
        "body_json",
        "runtime_snapshot",
        "result_schema_id",
        "recovery_schema_id",
        "expected_lineage_id",
        "expected_activation_id",
        "original_target_request_id",
        "original_target_request_hash",
        "created_at_ms",
    )
    for field in immutable_fields:
        values = [getattr(candidate.original, field) for candidate in journal_candidates]
        assert values == [values[0]] * 4

    assert len(request_candidates) == len(intent_candidates) == 1
    initial_request = request_candidates[0]
    intent = intent_candidates[0]
    final_request = original_get_request(command.request_id)
    final_delivery = transport.ledger.get_delivery(DELIVERY_ID)
    assert final_request is not None and final_delivery is not None
    assert final_request.state is HostRequestState.COMPLETED
    assert (initial_request.result_schema_id, initial_request.recovery_schema_id) == schema_identity
    assert (final_request.result_schema_id, final_request.recovery_schema_id) == schema_identity
    assert (intent.result_schema_id, intent.recovery_schema_id) == schema_identity
    for candidate in journal_candidates:
        assert (
            candidate.original.result_schema_id,
            candidate.original.recovery_schema_id,
        ) == schema_identity
        assert all(
            (entry.result_schema_id, entry.recovery_schema_id) == schema_identity
            for entry in candidate.original.delivery_intents
        )
    assert final_request.result_json == expected_result_json
    assert final_request.error_json is None and final_request.resolution_json is None
    assert final_request.terminal_at_ms == final_request.updated_at_ms
    for field in (
        "request_id",
        "root_key",
        "request_hash",
        "operation",
        "operation_class",
        "runtime_snapshot",
        "result_schema_id",
        "recovery_schema_id",
        "body_json",
        "lineage_id",
        "activation_id",
        "created_at_ms",
    ):
        assert getattr(final_request, field) == getattr(initial_request, field)
    assert final_delivery.delivery_id == DELIVERY_ID
    assert final_delivery.request_id == command.request_id
    assert final_delivery.delivery_kind == "request"
    assert final_delivery.original_request_delivery_id == DELIVERY_ID
    assert final_delivery.intended_at_ms == intent.intended_at_ms
    assert (
        final_delivery.published_at_ms
        == published_journal.original.delivery_intents[0].published_at_ms
    )
    assert final_delivery.rendered_inbox_sha256 == intent.rendered_inbox_sha256
    assert final_delivery.rendered_inbox_size_bytes == intent.rendered_inbox_size_bytes
    assert final_delivery.response_filename == response_path.name
    assert final_delivery.response_artifact == returned
    assert final_delivery.settlement == returned.accepted_response.settlement
    assert raw_response_columns() == expected_response_columns(returned)
    assert recorded_deliveries == [final_delivery]

    with sqlite3.connect(harness.paths.sqlite_file) as connection:
        assert connection.execute("SELECT COUNT(*) FROM requests").fetchone() == (1,)
        assert connection.execute("SELECT COUNT(*) FROM deliveries").fetchone() == (1,)
    assert uuid_calls == 1
    assert duplicate_check_calls == 1
    assert response_postcondition_reads == 1
    assert publish_calls == 1
    assert mark_calls == 1
    assert response_record_calls == 1
    assert waiter_calls == 1
    assert process_calls >= 3
    assert len(peer.observed_deliveries) == 1
    assert peer.observed_deliveries[0].request_id == command.request_id
    assert peer.observed_deliveries[0].delivery_id == DELIVERY_ID
    assert trace.count("peer.request_observed") == 1
    assert trace.count("peer.response_written") == 1
    assert len(queued_paths) == 1

    def before(left: str, right: str) -> None:
        assert trace.index(left) < trace.index(right), trace

    for left, right in (
        ("session.lock_acquired", "session.process_pinned"),
        ("session.process_pinned", "session.journal_gate"),
        ("session.journal_gate", "mutation.journal_recheck"),
        ("mutation.journal_recheck", "mutation.ledger_duplicate_check"),
        ("mutation.ledger_duplicate_check", "journal.prepared"),
        ("journal.prepared", "ledger.request_prepared"),
        ("ledger.request_prepared", "ledger.intent_prepared"),
        ("ledger.intent_prepared", "process.prepublication"),
        ("process.prepublication", "inbox.request_replaced"),
        ("inbox.request_replaced", "journal.published"),
        ("journal.published", "ledger.delivery_published"),
        ("ledger.delivery_published", "ledger.request_published"),
        ("ledger.request_published", "waiter.process_check"),
        ("ledger.request_published", "peer.request_observed"),
        ("peer.request_observed", "peer.response_written"),
        ("journal.published", "peer.response_written"),
        ("peer.response_written", "waiter.returned"),
        ("waiter.returned", "journal.response_accepted"),
        ("journal.response_accepted", "ledger.response_recorded"),
        ("ledger.response_recorded", "ledger.request_response_accepted"),
        ("ledger.request_response_accepted", "ledger.request_response_observed"),
        ("ledger.request_response_observed", "recovery_result_validated"),
        ("recovery_result_validated", "inbox.idle_replaced"),
        ("inbox.idle_replaced", "journal.idle_published"),
        ("journal.idle_published", "ledger.request_idle_published"),
        ("ledger.request_idle_published", "ledger.request_terminal"),
        ("ledger.request_terminal", "journal.deleted"),
        ("journal.deleted", "cleanup.queued"),
        ("cleanup.queued", "session.unlock"),
        ("session.unlock", "response.unlinked"),
    ):
        before(left, right)


@pytest.mark.asyncio
async def test_mutation_partial_response_outlives_three_polls_without_quarantine(
    harness: Harness,
) -> None:
    command = _command(harness, timeout=0.5)
    expected = _completed_artifact(command).accepted_response.settlement
    assert type(expected) is CompletedSettlement
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(
        WritePartialThenComplete(
            response=Respond(result=expected.result),
            delay_seconds=0.2,
        )
    )
    harness.paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    transport = _transport(harness, response_poll_seconds=0.01)

    artifact: ResponseArtifact | None = None
    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)
            concrete = cast(_FileBridgeChannel, channel)
            assert not concrete._poisoned  # pyright: ignore[reportPrivateUsage]
            assert not concrete._mutation_exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]

    assert artifact is not None
    assert artifact.accepted_response.settlement == expected
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.COMPLETED
    assert not harness.paths.pending_file.exists()
    assert harness.paths.inbox.read_bytes() == render_idle_lua()
    assert len(peer.observed_deliveries) == 1


async def _assert_real_peer_terminal_settlement(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    *,
    command: ExchangeCommand,
    expected_result: JsonValue,
    expected_base_class: OperationClass,
    expected_effective_class: OperationClass,
    expected_from_public_calls: int,
    peer: FakeFileBridgePeer,
    trace: list[str],
    expected_error: ResponseError | None = None,
) -> None:
    invocation = command.invocation
    recovery_schema = invocation.recovery_schema
    assert recovery_schema is not None
    schema_identity = (
        invocation.result_schema.schema_id,
        recovery_schema.schema_id,
    )
    assert invocation.contract.base_class is expected_base_class
    assert invocation.effective_class is expected_effective_class
    assert schema_identity[0] != schema_identity[1]
    assert invocation.result_adapter.json_schema() == recovery_schema.adapter.json_schema()
    is_rejected = expected_error is not None
    if is_rejected:
        assert expected_result is None
        expected_result_json = None
        expected_error_json = _canonical_json(expected_error.model_dump(mode="json")).decode(
            "utf-8"
        )
        expected_terminal_state = HostRequestState.REJECTED
        expected_recovery_validate_calls = 0
    else:
        assert isinstance(expected_result, dict)
        expected_result_json = _canonical_json(expected_result).decode("utf-8")
        expected_error_json = None
        expected_terminal_state = HostRequestState.COMPLETED
        expected_recovery_validate_calls = 1

    transport = _transport(harness)
    response_path = harness.paths.response_path(command.request_id)
    journal_candidates: list[PendingJournal] = []
    waiter_artifacts: list[ResponseArtifact] = []
    waiter_paths: list[Path] = []
    queued_paths: list[Path] = []
    uuid_calls = 0
    request_publish_calls = 0
    response_record_calls = 0
    waiter_calls = 0
    publish_calls = 0
    from_public_calls = 0
    recovery_validate_calls = 0
    journal_r2_saved = False
    response_sql_recorded = False
    request_response_accepted = False
    idle_returned = False
    journal_deleted = False
    unlock_finished = False
    runner_ref: MutationExchange | None = None

    original_enter = RootLock.__aenter__
    original_exit = RootLock.__aexit__
    original_save = transport.journals.save
    original_delete = transport.journals.delete
    original_get_request = transport.ledger.get_request
    original_record_response = transport.ledger.record_response
    original_transition = transport.ledger.transition
    original_publish_delivery = InboxPublisher.publish_delivery
    original_publish_idle = InboxPublisher.publish_idle
    original_wait = ResponseWaiter.wait
    original_response_delete = cleanup_module._set_delete_disposition  # pyright: ignore[reportPrivateUsage]
    original_from_public = ModelWireResolver.from_public
    adapter_descriptor = inspect.getattr_static(SchemaBinding, "adapter")
    descriptor_get = getattr(adapter_descriptor, "__get__", None)
    assert callable(descriptor_get)

    def raw_response_columns() -> tuple[object, object, object, object, object]:
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            row = connection.execute(
                """
                SELECT response_at_ms,response_sha256,response_size_bytes,
                       accepted_response_json,settlement_json
                FROM deliveries WHERE delivery_id=?
                """,
                (str(DELIVERY_ID),),
            ).fetchone()
        assert row is not None
        return cast(tuple[object, object, object, object, object], tuple(row))

    def expected_response_columns(
        artifact: ResponseArtifact,
    ) -> tuple[int, str, int, bytes, bytes]:
        settlement = artifact.accepted_response.settlement
        assert settlement is not None
        return (
            artifact.accepted_at_ms,
            artifact.sha256,
            artifact.size_bytes,
            _canonical_json(artifact.accepted_response.model_dump(mode="json")),
            _canonical_json(settlement.model_dump(mode="json")),
        )

    async def traced_enter(lock: RootLock) -> RootLock:
        result = await original_enter(lock)
        trace.append("session.lock_acquired")
        return result

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        nonlocal unlock_finished
        if waiter_artifacts:
            assert response_path.exists()
        if exc is None:
            assert journal_deleted
            assert len(queued_paths) == 1
        result = await original_exit(lock, exc_type, exc, traceback)
        unlock_finished = True
        trace.append("session.unlock")
        if waiter_artifacts:
            assert response_path.exists()
        return result

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal journal_r2_saved
        original = journal.original
        assert original.operation == command.body.operation
        assert original.effective_class is expected_effective_class
        assert original.body_json == canonical_body_bytes(command.body).decode("utf-8")
        assert (original.result_schema_id, original.recovery_schema_id) == schema_identity
        assert all(
            (intent.result_schema_id, intent.recovery_schema_id) == schema_identity
            for intent in original.delivery_intents
        )
        if original.state is PendingPhase.RESPONSE_ACCEPTED:
            assert len(journal_candidates) == 2
            prior = journal_candidates[-1].original
            assert prior.state is PendingPhase.PUBLISHED and prior.revision == 1
            assert expected_revisions == JournalRevisions(
                original=1,
                reconcile_attempt=None,
            )
            assert len(waiter_artifacts) == 1
            artifact = waiter_artifacts[0]
            assert original.response_artifact == artifact
            assert original.settlement == artifact.accepted_response.settlement
            assert artifact.accepted_response.delivery_kind == "request"
            assert raw_response_columns() == (None, None, None, None, None)
            request = original_get_request(command.request_id)
            assert request is not None and request.state is HostRequestState.PUBLISHED
            assert recovery_validate_calls == 0
        elif original.state is PendingPhase.IDLE_PUBLISHED:
            assert expected_revisions == JournalRevisions(
                original=2,
                reconcile_attempt=None,
            )
            assert runner_ref is not None
            state = runner_ref._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            assert getattr(state, "idle_publication_entered", None) is True
            assert getattr(state, "idle_publication_returned", None) is True
            assert idle_returned
            assert request_response_accepted
            assert recovery_validate_calls == expected_recovery_validate_calls
        journal_candidates.append(journal)
        revisions = original_save(journal, expected_revisions=expected_revisions)
        trace.append(f"journal.{original.state.value}")
        if original.state is PendingPhase.RESPONSE_ACCEPTED:
            assert raw_response_columns() == (None, None, None, None, None)
            journal_r2_saved = True
        return revisions

    def traced_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        nonlocal response_record_calls, response_sql_recorded
        response_record_calls += 1
        assert artifact is waiter_artifacts[0]
        assert journal_r2_saved
        assert not request_response_accepted
        assert recovery_validate_calls == 0
        assert raw_response_columns() == (None, None, None, None, None)
        result = original_record_response(artifact)
        assert raw_response_columns() == expected_response_columns(artifact)
        response_sql_recorded = True
        trace.append("ledger.response_recorded")
        return result

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal request_publish_calls, request_response_accepted
        assert request_id == command.request_id
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            assert expected_states == frozenset({HostRequestState.PUBLISHED})
            assert journal_r2_saved and response_sql_recorded
            assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])
            assert recovery_validate_calls == 0
        elif new_state is HostRequestState.IDLE_PUBLISHED:
            assert expected_states == frozenset({HostRequestState.RESPONSE_ACCEPTED})
            assert idle_returned
            assert journal_candidates[-1].original.state is PendingPhase.IDLE_PUBLISHED
            assert recovery_validate_calls == expected_recovery_validate_calls
        elif new_state is expected_terminal_state:
            assert expected_states == frozenset({HostRequestState.IDLE_PUBLISHED})
            assert terminal_at_ms == updated_at_ms
            assert result_json == expected_result_json
            assert error_json == expected_error_json
            assert resolution_json is None
        result = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        event = {
            HostRequestState.PUBLISHED: "ledger.request_published",
            HostRequestState.RESPONSE_ACCEPTED: "ledger.request_response_accepted",
            HostRequestState.IDLE_PUBLISHED: "ledger.request_idle_published",
            HostRequestState.COMPLETED: "ledger.request_terminal",
            HostRequestState.REJECTED: "ledger.request_terminal",
        }.get(new_state)
        if event is not None:
            trace.append(event)
        if new_state is HostRequestState.PUBLISHED:
            request_publish_calls += 1
        elif new_state is HostRequestState.RESPONSE_ACCEPTED:
            request_response_accepted = True
        return result

    def traced_publish_delivery(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal publish_calls
        assert publisher is transport.inbox
        publish_calls += 1
        original_publish_delivery(
            publisher,
            delivery,
            runtime_snapshot=runtime_snapshot,
        )
        trace.append("inbox.request_replaced")

    def traced_publish_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_returned
        assert publisher is transport.inbox
        assert recovery_validate_calls == expected_recovery_validate_calls
        assert request_response_accepted
        assert response_path.exists()
        original_publish_idle(publisher)
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        idle_returned = True
        trace.append("inbox.idle_replaced")

    async def traced_wait(waiter: ResponseWaiter, timeout_seconds: float) -> ResponseArtifact:
        nonlocal waiter_calls
        waiter_calls += 1
        waiter_path = waiter._response_path  # pyright: ignore[reportPrivateUsage]
        assert waiter_path == response_path
        waiter_paths.append(waiter_path)
        artifact = await original_wait(waiter, timeout_seconds)
        raw = waiter_path.read_bytes()
        assert artifact.sha256 == hashlib.sha256(raw).hexdigest()
        assert artifact.size_bytes == len(raw)
        assert artifact.accepted_response.delivery_kind == "request"
        settlement = artifact.accepted_response.settlement
        if is_rejected:
            assert type(settlement) is RejectedSettlement
            assert settlement.error == expected_error
        else:
            assert type(settlement) is CompletedSettlement
            assert settlement.result == expected_result
        waiter_artifacts.append(artifact)
        trace.append("waiter.returned")
        return artifact

    def traced_delete(expected: JournalDeleteExpectation) -> None:
        nonlocal journal_deleted
        assert expected.revisions == JournalRevisions(original=3, reconcile_attempt=None)
        request = original_get_request(command.request_id)
        assert request is not None and request.state is expected_terminal_state
        assert response_path.exists()
        original_delete(expected)
        journal_deleted = True
        trace.append("journal.deleted")

    def traced_from_public(
        resolver: ModelWireResolver,
        public_arguments: object,
        trusted_enrichment: object,
    ) -> object:
        nonlocal from_public_calls
        from_public_calls += 1
        assert dict(cast(Any, trusted_enrichment)) == {}
        return original_from_public(
            resolver,
            cast(Any, public_arguments),
            cast(Any, trusted_enrichment),
        )

    class RecoveryAdapterSpy:
        def __init__(self, delegate: object) -> None:
            self._delegate = delegate

        def validate_python(self, value: object, *args: object, **kwargs: object) -> object:
            nonlocal recovery_validate_calls
            if is_rejected:
                raise AssertionError("rejected settlement must not validate recovery data")
            assert recovery_validate_calls == 0
            assert value == expected_result
            assert journal_r2_saved and response_sql_recorded and request_response_accepted
            recovery_validate_calls += 1
            result = cast(Any, self._delegate).validate_python(value, *args, **kwargs)
            trace.append("recovery_result_validated")
            return result

        def dump_python(self, value: object, *args: object, **kwargs: object) -> object:
            if is_rejected:
                raise AssertionError("rejected settlement must not dump recovery data")
            return cast(Any, self._delegate).dump_python(value, *args, **kwargs)

        def __getattr__(self, name: str) -> object:
            return getattr(self._delegate, name)

    recovery_spy: RecoveryAdapterSpy | None = None

    def traced_schema_adapter(binding: SchemaBinding) -> object:
        nonlocal recovery_spy
        adapter = cast(Any, descriptor_get)(binding, SchemaBinding)
        if binding is recovery_schema:
            if recovery_spy is None:
                recovery_spy = RecoveryAdapterSpy(adapter)
            return recovery_spy
        return adapter

    def traced_response_delete(handle: int, path: Path) -> None:
        if path == response_path.resolve(strict=True):
            assert len(queued_paths) == 1
            assert queued_paths[0] == response_path
            assert len(waiter_paths) == 1 and waiter_paths[0] == response_path
            assert unlock_finished
            assert path.exists()
        original_response_delete(handle, path)
        if path == response_path.resolve(strict=False):
            trace.append("response.unlinked")

    def fixed_uuid4() -> UUID:
        nonlocal uuid_calls
        uuid_calls += 1
        return DELIVERY_ID

    monkeypatch.setattr(RootLock, "__aenter__", traced_enter)
    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)
    monkeypatch.setattr(transport.journals, "save", traced_save)
    monkeypatch.setattr(transport.journals, "delete", traced_delete)
    monkeypatch.setattr(transport.ledger, "record_response", traced_record_response)
    monkeypatch.setattr(transport.ledger, "transition", traced_transition)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish_delivery)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_publish_idle)
    monkeypatch.setattr(ResponseWaiter, "wait", traced_wait)
    if expected_from_public_calls:
        monkeypatch.setattr(ModelWireResolver, "from_public", traced_from_public)
    monkeypatch.setattr(
        SchemaBinding,
        "adapter",
        property(traced_schema_adapter),
        raising=False,
    )
    monkeypatch.setattr(mutation_module.uuid, "uuid4", fixed_uuid4)
    monkeypatch.setattr(cleanup_module, "_set_delete_disposition", traced_response_delete)

    returned: ResponseArtifact | None = None
    async with peer:
        async with transport.session() as channel:
            concrete = cast(_FileBridgeChannel, channel)
            runner = concrete._mutation_exchange  # pyright: ignore[reportPrivateUsage]
            runner_ref = runner
            original_queue = runner._queue_response_cleanup  # pyright: ignore[reportPrivateUsage]

            def traced_queue(path: Path) -> None:
                harness.lock.require_acquired()
                assert path == response_path
                assert len(waiter_paths) == 1 and path is waiter_paths[0]
                assert journal_deleted
                assert response_path.exists()
                queued_paths.append(path)
                original_queue(path)
                trace.append("cleanup.queued")

            monkeypatch.setattr(runner, "_queue_response_cleanup", traced_queue)
            returned = await channel.exchange(command)
            assert returned is not None
            assert returned is waiter_artifacts[0]
            assert response_path.exists()
            assert concrete._cleanup_paths == [queued_paths[0]]  # pyright: ignore[reportPrivateUsage]
            assert concrete._cleanup_paths[0] is queued_paths[0]  # pyright: ignore[reportPrivateUsage]
            assert concrete._active_task is None  # pyright: ignore[reportPrivateUsage]
            assert concrete._poisoned is False  # pyright: ignore[reportPrivateUsage]
            assert runner._requires_fresh_session() is False  # pyright: ignore[reportPrivateUsage]
            state = runner._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            assert getattr(state, "idle_publication_entered", None) is True
            assert getattr(state, "idle_publication_returned", None) is True
            assert state.safety_finished is True

    assert returned is not None
    assert returned is waiter_artifacts[0]
    settlement = returned.accepted_response.settlement
    if is_rejected:
        assert type(settlement) is RejectedSettlement
        assert settlement.error == expected_error
    else:
        assert type(settlement) is CompletedSettlement
        assert settlement.result == expected_result
    assert not response_path.exists()
    assert harness.paths.inbox.read_bytes() == render_idle_lua()
    assert not harness.paths.pending_file.exists()
    assert [journal.original.state for journal in journal_candidates] == [
        PendingPhase.PREPARED,
        PendingPhase.PUBLISHED,
        PendingPhase.RESPONSE_ACCEPTED,
        PendingPhase.IDLE_PUBLISHED,
    ]
    assert [journal.original.revision for journal in journal_candidates] == [0, 1, 2, 3]
    for journal in journal_candidates:
        original = journal.original
        assert original.operation == command.body.operation
        assert original.effective_class is expected_effective_class
        assert (original.result_schema_id, original.recovery_schema_id) == schema_identity
        assert all(
            (intent.result_schema_id, intent.recovery_schema_id) == schema_identity
            for intent in original.delivery_intents
        )
    assert journal_candidates[2].original.response_artifact == returned
    assert journal_candidates[3].original.response_artifact == returned
    assert journal_candidates[2].original.settlement == settlement
    assert journal_candidates[3].original.settlement == settlement

    final_request = original_get_request(command.request_id)
    final_delivery = transport.ledger.get_delivery(DELIVERY_ID)
    assert final_request is not None and final_delivery is not None
    assert final_request.state is expected_terminal_state
    assert final_request.operation == command.body.operation
    assert final_request.operation_class is expected_effective_class
    assert final_request.body_json == canonical_body_bytes(command.body)
    assert (final_request.result_schema_id, final_request.recovery_schema_id) == schema_identity
    assert final_request.result_json == expected_result_json
    assert final_request.error_json == expected_error_json
    assert final_request.resolution_json is None
    assert final_request.terminal_at_ms == final_request.updated_at_ms
    assert final_delivery.request_id == command.request_id
    assert final_delivery.delivery_id == DELIVERY_ID
    assert final_delivery.response_artifact == returned
    assert final_delivery.settlement == returned.accepted_response.settlement
    assert settlement is not None
    assert raw_response_columns() == expected_response_columns(returned)
    with sqlite3.connect(harness.paths.sqlite_file) as connection:
        assert connection.execute("SELECT COUNT(*) FROM requests").fetchone() == (1,)
        assert connection.execute("SELECT COUNT(*) FROM deliveries").fetchone() == (1,)

    assert uuid_calls == 1
    assert publish_calls == 1
    assert request_publish_calls == 1
    assert response_record_calls == 1
    assert waiter_calls == 1
    assert recovery_validate_calls == expected_recovery_validate_calls
    assert from_public_calls == expected_from_public_calls
    assert harness.inspector.calls >= 3
    assert len(peer.observed_deliveries) == 1
    assert peer.observed_deliveries[0].request_id == command.request_id
    assert peer.observed_deliveries[0].delivery_id == DELIVERY_ID
    assert trace.count("peer.request_observed") == 1
    assert trace.count("peer.response_written") == 1

    def before(left: str, right: str) -> None:
        assert trace.index(left) < trace.index(right), trace

    order_pairs = [
        ("inbox.request_replaced", "journal.published"),
        ("journal.published", "ledger.request_published"),
        ("ledger.request_published", "peer.request_observed"),
        ("peer.request_observed", "peer.response_written"),
        ("peer.response_written", "waiter.returned"),
        ("waiter.returned", "journal.response_accepted"),
        ("journal.response_accepted", "ledger.response_recorded"),
        ("ledger.response_recorded", "ledger.request_response_accepted"),
        ("inbox.idle_replaced", "journal.idle_published"),
        ("journal.idle_published", "ledger.request_idle_published"),
        ("ledger.request_idle_published", "ledger.request_terminal"),
        ("ledger.request_terminal", "journal.deleted"),
        ("journal.deleted", "cleanup.queued"),
        ("cleanup.queued", "session.unlock"),
        ("session.unlock", "response.unlinked"),
    ]
    if is_rejected:
        order_pairs.append(("ledger.request_response_accepted", "inbox.idle_replaced"))
        assert "recovery_result_validated" not in trace
    else:
        order_pairs.extend(
            [
                ("ledger.request_response_accepted", "recovery_result_validated"),
                ("recovery_result_validated", "inbox.idle_replaced"),
            ]
        )
    for left, right in order_pairs:
        before(left, right)


@pytest.mark.asyncio
async def test_completed_cleanup_queue_runtime_error_before_append_is_nonfatal_and_retains_response(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _command(harness, timeout=0.3)
    response_path = harness.paths.response_path(command.request_id)
    expected_settlement = _completed_artifact(command).accepted_response.settlement
    assert type(expected_settlement) is CompletedSettlement
    peer = _peer(harness, trace=[])
    peer.enqueue(Respond(result=expected_settlement.result))
    cleanup_error = RuntimeError("cleanup queue failed before append")
    waiter_artifacts: list[ResponseArtifact] = []
    waiter_paths: list[Path] = []
    queue_paths: list[Path] = []
    delete_calls = 0
    response_unlink_attempts = 0
    journal_deleted = False
    original_wait = ResponseWaiter.wait
    original_delete = transport.journals.delete
    original_unlink = Path.unlink

    async def capture_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        waiter_path = waiter._response_path  # pyright: ignore[reportPrivateUsage]
        assert waiter_path == response_path
        waiter_paths.append(waiter_path)
        artifact = await original_wait(waiter, timeout_seconds)
        waiter_artifacts.append(artifact)
        return artifact

    def traced_delete(expected: JournalDeleteExpectation) -> None:
        nonlocal delete_calls, journal_deleted
        assert response_path.exists()
        original_delete(expected)
        delete_calls += 1
        journal_deleted = True

    def guarded_unlink(path: Path, *, missing_ok: bool = False) -> None:
        nonlocal response_unlink_attempts
        if path == response_path:
            response_unlink_attempts += 1
            return
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(ResponseWaiter, "wait", capture_wait)
    monkeypatch.setattr(transport.journals, "delete", traced_delete)
    monkeypatch.setattr(Path, "unlink", guarded_unlink)

    returned: ResponseArtifact | None = None
    async with peer:
        async with transport.session() as channel:
            concrete = cast(_FileBridgeChannel, channel)
            runner = concrete._mutation_exchange  # pyright: ignore[reportPrivateUsage]

            def fail_before_append(path: Path) -> None:
                harness.lock.require_acquired()
                assert path == response_path
                assert len(waiter_paths) == 1 and path is waiter_paths[0]
                assert journal_deleted
                assert not harness.paths.pending_file.exists()
                request = transport.ledger.get_request(command.request_id)
                assert request is not None
                assert request.state is HostRequestState.COMPLETED
                assert request.terminal_at_ms == request.updated_at_ms
                assert response_path.exists()
                assert concrete._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
                queue_paths.append(path)
                raise cleanup_error

            monkeypatch.setattr(runner, "_queue_response_cleanup", fail_before_append)
            returned = await channel.exchange(command)
            assert len(waiter_artifacts) == 1
            assert returned is waiter_artifacts[0]
            assert concrete._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
            assert concrete._active_task is None  # pyright: ignore[reportPrivateUsage]
            assert concrete._poisoned is False  # pyright: ignore[reportPrivateUsage]
            assert runner._requires_fresh_session() is False  # pyright: ignore[reportPrivateUsage]
            state = runner._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None and state.safety_finished is True
            assert response_path.exists()

    assert returned is not None
    assert type(returned.accepted_response.settlement) is CompletedSettlement
    assert returned.accepted_response.settlement.result == expected_settlement.result
    assert queue_paths == [response_path]
    assert queue_paths[0] is waiter_paths[0]
    assert delete_calls == 1
    assert not harness.paths.pending_file.exists()
    assert harness.paths.inbox.read_bytes() == render_idle_lua()
    request = transport.ledger.get_request(command.request_id)
    delivery = transport.ledger.get_delivery(returned.accepted_response.envelope.delivery_id)
    assert request is not None and request.state is HostRequestState.COMPLETED
    assert request.terminal_at_ms == request.updated_at_ms
    assert delivery is not None
    assert delivery.response_artifact == returned
    assert delivery.settlement == returned.accepted_response.settlement
    assert response_unlink_attempts == 0
    assert response_path.exists()


@pytest.mark.asyncio
async def test_unit_add_real_peer_completed_preserves_nonempty_guid_and_terminal_cleanup(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command(
        harness,
        "unit.add",
        {
            "side_guid": "SIDE-1",
            "unit_type": "Aircraft",
            "dbid": 1,
            "name": "海鹰 Alpha",
            "latitude": 1.0,
            "longitude": 2.0,
        },
    )
    explicit_result: JsonValue = {
        "unit_guid": "UNIT-ADDED",
        "name": "海鹰 Alpha",
        "side_guid": "SIDE-1",
        "dbid": 1,
        "latitude": 1.0,
        "longitude": 2.0,
    }
    validated = command.invocation.result_adapter.validate_python(explicit_result)
    expected_result = cast(
        JsonValue,
        command.invocation.result_adapter.dump_python(validated, mode="json"),
    )
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    assert command.invocation.result_schema.schema_id == (
        "266a32aa0a8d2e06f172f71c9fab3150e6ad95771af6abbbc6278c886751d78e"
    )
    assert recovery_schema.schema_id == (
        "90bd097de0262a91044d867a8c24ae186fc2a23967f71077b1bd123199dd4544"
    )
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected_result))
    monkeypatch.setattr(peer, "result_for", _forbidden)
    monkeypatch.setattr(OperationRegistry, "resolve_invocation", _forbidden)

    await _assert_real_peer_terminal_settlement(
        harness,
        monkeypatch,
        command=command,
        expected_result=expected_result,
        expected_base_class=OperationClass.MUTATION,
        expected_effective_class=OperationClass.MUTATION,
        expected_from_public_calls=1,
        peer=peer,
        trace=trace,
    )


@pytest.mark.asyncio
async def test_dynamic_apply_profile_completed_freezes_effective_mutation_and_role_schemas(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments: dict[str, object] = {
        "step": "apply-profile",
        "safe_payload_bytes": 4096,
        "verified_ledger_entries": 32,
        "effective_ledger_capacity": 32,
    }
    command = _command(harness, "compat.probe.step", arguments)
    explicit_result: JsonValue = {
        "step": "apply-profile",
        "nonce": "nonce-red3b",
        "poll_source": "automatic",
        "applied": True,
        "safe_payload_bytes": 4096,
        "verified_ledger_entries": 32,
        "effective_ledger_capacity": 32,
    }
    validated = command.invocation.result_adapter.validate_python(explicit_result)
    expected_result = cast(
        JsonValue,
        command.invocation.result_adapter.dump_python(validated, mode="json"),
    )
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    assert command.invocation.contract.base_class is OperationClass.DYNAMIC
    assert command.invocation.effective_class is OperationClass.MUTATION
    assert command.invocation.result_schema.schema_id == (
        "c7aea77b1f0178ec85147bfeec7276ab3854a07efa06e16cad67b6f5c6a9024f"
    )
    assert recovery_schema.schema_id == (
        "a42d935e3b3a088e306de9fa5f60085e0434a550ddb3292eeff054ca7369e681"
    )
    different_step = OPERATION_REGISTRY.resolve_invocation(
        "compat.probe.step",
        {"step": "dedupe"},
    )
    assert different_step.effective_class is OperationClass.MUTATION
    assert different_step.recovery_schema is not None
    assert different_step.result_schema.schema_id != command.invocation.result_schema.schema_id
    assert different_step.recovery_schema.schema_id != recovery_schema.schema_id
    assert command.body.arguments == command.invocation.wire_arguments.model_dump(mode="json")
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected_result))
    monkeypatch.setattr(peer, "result_for", _forbidden)
    monkeypatch.setattr(OperationRegistry, "resolve_invocation", _forbidden)

    await _assert_real_peer_terminal_settlement(
        harness,
        monkeypatch,
        command=command,
        expected_result=expected_result,
        expected_base_class=OperationClass.DYNAMIC,
        expected_effective_class=OperationClass.MUTATION,
        expected_from_public_calls=1,
        peer=peer,
        trace=trace,
    )


@pytest.mark.asyncio
async def test_trusted_unit_delete_completed_uses_frozen_proof_without_reauthorization(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proof = "f" * 64
    command = _command(
        harness,
        "unit.delete",
        {"unit_guid": "UNIT-DELETE"},
        trusted_enrichment={"confirmation_proof": proof},
    )
    explicit_result: JsonValue = {
        "deleted_guid": "UNIT-DELETE",
        "deleted_name": "退役 海鹰",
        "object_kind": "unit",
    }
    validated = command.invocation.result_adapter.validate_python(explicit_result)
    expected_result = cast(
        JsonValue,
        command.invocation.result_adapter.dump_python(validated, mode="json"),
    )
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    assert command.invocation.contract.confirmation_required
    assert command.invocation.contract.base_class is OperationClass.DESTRUCTIVE
    assert command.invocation.effective_class is OperationClass.DESTRUCTIVE
    assert command.invocation.result_schema.schema_id == (
        "45a9027ee2b274e4c12db7aa98ce53441e3d7932c33d12413dccb175b725e844"
    )
    assert recovery_schema.schema_id == (
        "fbde6843ef6d2644f07a33c5f016bd6a38104a37a7467eceaadc249680c8b88f"
    )
    assert command.invocation.public_arguments.model_dump(mode="json") == {
        "unit_guid": "UNIT-DELETE"
    }
    assert command.invocation.wire_arguments.model_dump(mode="json") == {
        "unit_guid": "UNIT-DELETE",
        "confirmation_proof": proof,
    }
    assert command.body.arguments == command.invocation.wire_arguments.model_dump(mode="json")
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected_result))
    monkeypatch.setattr(peer, "result_for", _forbidden)

    mutation_names = set(vars(mutation_module))
    assert not mutation_names.intersection(
        {
            "BridgeConfig",
            "BridgeConfigStore",
            "ConfirmationTokenStore",
            "DestructivePolicy",
            "authorize_destructive",
        }
    )
    constructor_parameters = inspect.signature(MutationExchange.__init__).parameters
    assert not any(
        marker in name
        for name in constructor_parameters
        for marker in ("policy", "token", "confirmation", "authorization")
    )
    monkeypatch.setattr(OperationRegistry, "resolve_invocation", _forbidden)
    monkeypatch.setattr(ModelWireResolver, "from_public", _forbidden)

    await _assert_real_peer_terminal_settlement(
        harness,
        monkeypatch,
        command=command,
        expected_result=expected_result,
        expected_base_class=OperationClass.DESTRUCTIVE,
        expected_effective_class=OperationClass.DESTRUCTIVE,
        expected_from_public_calls=0,
        peer=peer,
        trace=trace,
    )


@pytest.mark.asyncio
async def test_real_peer_not_started_error_is_exact_rejected_terminal_without_recovery(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command(
        harness,
        "unit.set",
        {
            "unit_guid": "UNIT-REJECTED",
            "name": "Rejected before execution",
        },
    )
    request_hash = hashlib.sha256(canonical_body_bytes(command.body)).hexdigest()
    error_payload = cast(
        dict[str, JsonValue],
        {
            "code": ErrorCode.CMO_LUA_ERROR.value,
            "message": "runtime rejected request before mutation execution",
            "details": {},
            "mutation_not_started": {
                "schema_version": 1,
                "stage": "handler_preflight",
                "request_id": str(command.request_id),
                "request_hash": request_hash,
                "operation": command.body.operation,
                "mutation_barrier_written": False,
                "execute_started": False,
            },
        },
    )
    expected_error = ResponseError.model_validate(error_payload)
    evidence = expected_error.mutation_not_started
    assert command.invocation.effective_class is OperationClass.MUTATION
    assert expected_error.details == {}
    assert evidence is not None
    assert evidence.stage == "handler_preflight"
    assert evidence.request_id == command.request_id
    assert evidence.request_hash == request_hash
    assert evidence.operation == command.body.operation
    assert evidence.mutation_barrier_written is False
    assert evidence.execute_started is False

    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(error=error_payload))
    monkeypatch.setattr(peer, "result_for", _forbidden)
    monkeypatch.setattr(OperationRegistry, "resolve_invocation", _forbidden)

    await _assert_real_peer_terminal_settlement(
        harness,
        monkeypatch,
        command=command,
        expected_result=None,
        expected_base_class=OperationClass.MUTATION,
        expected_effective_class=OperationClass.MUTATION,
        expected_from_public_calls=1,
        peer=peer,
        trace=trace,
        expected_error=expected_error,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response_case",
    ["cmo_lua_error", "manifest_mismatch"],
)
async def test_real_peer_error_without_terminal_proof_is_artifact_first_quarantined(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    response_case: str,
) -> None:
    command = _command(
        harness,
        "unit.set",
        {
            "unit_guid": "UNIT-INDETERMINATE",
            "name": "Outcome unknown 海峡",
        },
    )
    foreign_snapshot: RuntimeSnapshot | None = None
    if response_case == "cmo_lua_error":
        response_code = ErrorCode.CMO_LUA_ERROR
        response_message = "runtime failed after mutation may have started"
        response_details: dict[str, JsonValue] = {}
        envelope_overrides: tuple[tuple[str, JsonValue], ...] = ()
    else:
        assert response_case == "manifest_mismatch"
        response_code = ErrorCode.MANIFEST_MISMATCH
        response_message = "running runtime identity differs from request"
        response_details = {}
        foreign_snapshot = RuntimeSnapshot.create(
            runtime_version="0.2.0",
            runtime_asset_sha256="c" * 64,
            operation_manifest_sha256="d" * 64,
            host_contract_sha256="e" * 64,
            dependency_lock_sha256="f" * 64,
        )
        assert foreign_snapshot.release_id == (
            "a9ef9bbc6b47137a20ba4a6e4c8c63410f110f755cb6f1c238f7854ba2ed1227"
        )
        envelope_overrides = (
            (
                "operation_manifest_sha256",
                foreign_snapshot.operation_manifest_sha256,
            ),
            ("bridge_version", foreign_snapshot.runtime_version),
            ("runtime_tag", foreign_snapshot.runtime_tag),
            ("runtime_asset_sha256", foreign_snapshot.runtime_asset_sha256),
            ("release_id", foreign_snapshot.release_id),
        )

    error_payload = cast(
        dict[str, JsonValue],
        {
            "code": response_code.value,
            "message": response_message,
            "details": response_details,
            "mutation_not_started": None,
        },
    )
    expected_response_error = ResponseError.model_validate(error_payload)
    assert expected_response_error.mutation_not_started is None
    expected_primary_payload = {
        "code": ErrorCode.INDETERMINATE_OUTCOME.value,
        "message": "mutation response does not prove a terminal outcome",
        "details": {
            "request_id": str(command.request_id),
            "reason": "response_without_terminal_settlement",
            "response_code": response_code.value,
        },
    }
    expected_primary_json = _canonical_json(expected_primary_payload).decode("utf-8")

    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(
        Respond(
            error=error_payload,
            envelope_overrides=envelope_overrides,
        )
    )
    monkeypatch.setattr(peer, "result_for", _forbidden)
    monkeypatch.setattr(OperationRegistry, "resolve_invocation", _forbidden)

    transport = _transport(harness)
    response_path = harness.paths.response_path(command.request_id)
    journals: list[PendingJournal] = []
    waiter_artifacts: list[ResponseArtifact] = []
    original_save = transport.journals.save
    original_record_response = transport.ledger.record_response
    original_transition = transport.ledger.transition
    original_wait = ResponseWaiter.wait

    def raw_response_columns() -> tuple[object, object, object, object, object]:
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            row = connection.execute(
                """
                SELECT response_at_ms,response_sha256,response_size_bytes,
                       accepted_response_json,settlement_json
                FROM deliveries WHERE delivery_id=?
                """,
                (str(DELIVERY_ID),),
            ).fetchone()
        assert row is not None
        return cast(tuple[object, object, object, object, object], tuple(row))

    def expected_response_columns(
        artifact: ResponseArtifact,
    ) -> tuple[int, str, int, bytes, bytes]:
        assert artifact.accepted_response.settlement is None
        return (
            artifact.accepted_at_ms,
            artifact.sha256,
            artifact.size_bytes,
            _canonical_json(artifact.accepted_response.model_dump(mode="json")),
            b"null",
        )

    async def traced_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        assert waiter._response_path == response_path  # pyright: ignore[reportPrivateUsage]
        artifact = await original_wait(waiter, timeout_seconds)
        accepted = artifact.accepted_response
        assert accepted.delivery_kind == "request"
        assert accepted.settlement is None
        assert accepted.envelope.error == expected_response_error
        assert accepted.envelope.request_id == command.request_id
        assert accepted.envelope.delivery_id == DELIVERY_ID
        assert (
            accepted.envelope.request_hash
            == hashlib.sha256(canonical_body_bytes(command.body)).hexdigest()
        )
        if foreign_snapshot is None:
            assert accepted.envelope.release_id == harness.snapshot.release_id
        else:
            assert accepted.envelope.operation_manifest_sha256 == (
                foreign_snapshot.operation_manifest_sha256
            )
            assert accepted.envelope.bridge_version == foreign_snapshot.runtime_version
            assert accepted.envelope.runtime_tag == foreign_snapshot.runtime_tag
            assert accepted.envelope.runtime_asset_sha256 == (foreign_snapshot.runtime_asset_sha256)
            assert accepted.envelope.release_id == foreign_snapshot.release_id
        waiter_artifacts.append(artifact)
        trace.append("waiter.returned")
        return artifact

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        original = journal.original
        if original.state is PendingPhase.RESPONSE_ACCEPTED:
            assert expected_revisions == JournalRevisions(
                original=1,
                reconcile_attempt=None,
            )
            assert len(waiter_artifacts) == 1
            assert original.revision == 2
            assert original.response_artifact == waiter_artifacts[0]
            assert original.settlement is None
            assert raw_response_columns() == (None, None, None, None, None)
            request = transport.ledger.get_request(command.request_id)
            assert request is not None and request.state is HostRequestState.PUBLISHED
        elif original.state is PendingPhase.QUARANTINED:
            assert expected_revisions == JournalRevisions(
                original=2,
                reconcile_attempt=None,
            )
            assert len(journals) == 3
            prior = journals[-1].original
            assert prior.state is PendingPhase.RESPONSE_ACCEPTED
            assert prior.revision == 2
            assert original.revision == 3
            assert original.response_artifact == prior.response_artifact
            assert original.response_artifact == waiter_artifacts[0]
            assert original.settlement is None
            assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])
            request = transport.ledger.get_request(command.request_id)
            assert request is not None
            assert request.state is HostRequestState.RESPONSE_ACCEPTED
            assert request.result_json is None
            assert request.error_json is None
            assert request.resolution_json is None
            assert request.terminal_at_ms is None
        journals.append(journal)
        revisions = original_save(journal, expected_revisions=expected_revisions)
        trace.append(f"journal.{original.state.value}")
        return revisions

    def traced_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        assert artifact is waiter_artifacts[0]
        assert journals[-1].original.state is PendingPhase.RESPONSE_ACCEPTED
        assert raw_response_columns() == (None, None, None, None, None)
        recorded = original_record_response(artifact)
        assert recorded.response_artifact == artifact
        assert recorded.settlement is None
        assert raw_response_columns() == expected_response_columns(artifact)
        trace.append("ledger.response_recorded")
        return recorded

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        assert request_id == command.request_id
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            assert expected_states == frozenset({HostRequestState.PUBLISHED})
            assert journals[-1].original.state is PendingPhase.RESPONSE_ACCEPTED
            assert trace[-1] == "ledger.response_recorded"
            assert terminal_at_ms is None
            assert result_json is error_json is resolution_json is None
        elif new_state is HostRequestState.QUARANTINED:
            assert expected_states == frozenset({HostRequestState.RESPONSE_ACCEPTED})
            assert journals[-1].original.state is PendingPhase.QUARANTINED
            assert trace[-1] == "journal.quarantined"
            assert updated_at_ms == journals[-1].original.updated_at_ms
            assert terminal_at_ms is None
            assert result_json is None
            assert error_json == expected_primary_json
            assert resolution_json is None
        observed = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        trace.append(f"ledger.request_{new_state.value}")
        return observed

    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    recovery_delegate = recovery_schema.adapter
    adapter_descriptor = inspect.getattr_static(SchemaBinding, "adapter")
    descriptor_get = getattr(adapter_descriptor, "__get__", None)
    assert callable(descriptor_get)

    class RecoveryAdapterTripwire:
        def validate_python(self, *_args: object, **_kwargs: object) -> Never:
            raise AssertionError("settlement-null response reached recovery validation")

        def dump_python(self, *_args: object, **_kwargs: object) -> Never:
            raise AssertionError("settlement-null response reached recovery serialization")

        def __getattr__(self, name: str) -> object:
            return getattr(recovery_delegate, name)

    recovery_tripwire = RecoveryAdapterTripwire()

    def guarded_schema_adapter(binding: SchemaBinding) -> object:
        if binding is recovery_schema:
            return recovery_tripwire
        return cast(Any, descriptor_get)(binding, SchemaBinding)

    monkeypatch.setattr(transport.journals, "save", traced_save)
    monkeypatch.setattr(transport.journals, "delete", _forbidden)
    monkeypatch.setattr(transport.ledger, "record_response", traced_record_response)
    monkeypatch.setattr(transport.ledger, "transition", traced_transition)
    monkeypatch.setattr(ResponseWaiter, "wait", traced_wait)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(
        SchemaBinding,
        "adapter",
        property(guarded_schema_adapter),
        raising=False,
    )
    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    pending_before_second = b""
    response_before_second = b""
    inbox_before_second = b""
    pending_mtime = 0
    response_mtime = 0
    inbox_mtime = 0
    async with peer:
        async with transport.session() as channel:
            concrete = cast(_FileBridgeChannel, channel)
            runner = concrete._mutation_exchange  # pyright: ignore[reportPrivateUsage]
            monkeypatch.setattr(runner, "_queue_response_cleanup", _forbidden)
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

            assert caught.value.code is ErrorCode.INDETERMINATE_OUTCOME
            assert caught.value.message == expected_primary_payload["message"]
            assert caught.value.details == expected_primary_payload["details"]
            assert caught.value.__cause__ is None
            assert caught.value.__context__ is None
            assert concrete._active_task is None  # pyright: ignore[reportPrivateUsage]
            assert concrete._poisoned is True  # pyright: ignore[reportPrivateUsage]
            assert concrete._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
            assert runner._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]

            state = runner._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            assert state.artifact_observed is True
            assert state.response_artifact is waiter_artifacts[0]
            assert state.quarantined_journal is journals[-1]
            assert state.safety_finished is True
            loaded = transport.journals.load()
            assert loaded is not None
            assert loaded.journal == journals[-1]

            final_request = transport.ledger.get_request(command.request_id)
            final_delivery = transport.ledger.get_delivery(DELIVERY_ID)
            assert final_request is not None and final_delivery is not None
            assert final_request.state is HostRequestState.QUARANTINED
            assert final_request.updated_at_ms == journals[-1].original.updated_at_ms
            assert final_request.terminal_at_ms is None
            assert final_request.result_json is None
            assert final_request.error_json == expected_primary_json
            assert final_request.resolution_json is None
            assert final_delivery.response_artifact == waiter_artifacts[0]
            assert final_delivery.settlement is None
            assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])

            pending_before_second = harness.paths.pending_file.read_bytes()
            response_before_second = response_path.read_bytes()
            inbox_before_second = harness.paths.inbox.read_bytes()
            pending_mtime = harness.paths.pending_file.stat().st_mtime_ns
            response_mtime = response_path.stat().st_mtime_ns
            inbox_mtime = harness.paths.inbox.stat().st_mtime_ns
            trace_before_second = tuple(trace)
            inspector_calls = harness.inspector.calls

            with monkeypatch.context() as second_guard:
                second_guard.setattr(concrete, "_perform_exchange", _forbidden)
                second_guard.setattr(PendingJournalStore, "load", _forbidden)
                second_guard.setattr(ManifestCatalog, "resolve_running", _forbidden)
                second_guard.setattr(RequestLedger, "get_request", _forbidden)
                second_guard.setattr(RequestLedger, "insert_prepared", _forbidden)
                second_guard.setattr(Inspector, "matching_processes", _forbidden)
                second_guard.setattr(sqlite3, "connect", _forbidden)
                second_guard.setattr(InboxPublisher, "publish_delivery", _forbidden)
                second_guard.setattr(InboxPublisher, "publish_idle", _forbidden)
                second_guard.setattr(Path, "exists", _forbidden)
                second_guard.setattr(Path, "read_bytes", _forbidden)
                second_guard.setattr(Path, "write_bytes", _forbidden)
                second_guard.setattr(Path, "stat", _forbidden)
                second_guard.setattr(Path, "open", _forbidden)
                second_guard.setattr(Path, "unlink", _forbidden)
                with pytest.raises(BridgeError) as second:
                    await channel.exchange(command)
            assert second.value.code is ErrorCode.STATE_CONFLICT
            assert second.value.message == "file-bridge channel requires a fresh session"
            assert tuple(trace) == trace_before_second
            assert harness.inspector.calls == inspector_calls
            assert harness.paths.pending_file.read_bytes() == pending_before_second
            assert response_path.read_bytes() == response_before_second
            assert harness.paths.inbox.read_bytes() == inbox_before_second
            assert harness.paths.pending_file.stat().st_mtime_ns == pending_mtime
            assert response_path.stat().st_mtime_ns == response_mtime
            assert harness.paths.inbox.stat().st_mtime_ns == inbox_mtime

    assert caught is not None
    assert response_path.exists()
    assert harness.paths.pending_file.exists()
    assert harness.paths.pending_file.read_bytes() == pending_before_second
    assert response_path.read_bytes() == response_before_second
    assert harness.paths.inbox.read_bytes() == inbox_before_second
    assert harness.paths.pending_file.stat().st_mtime_ns == pending_mtime
    assert response_path.stat().st_mtime_ns == response_mtime
    assert harness.paths.inbox.stat().st_mtime_ns == inbox_mtime
    assert [journal.original.state for journal in journals] == [
        PendingPhase.PREPARED,
        PendingPhase.PUBLISHED,
        PendingPhase.RESPONSE_ACCEPTED,
        PendingPhase.QUARANTINED,
    ]
    assert [journal.original.revision for journal in journals] == [0, 1, 2, 3]
    assert journals[2].original.response_artifact == waiter_artifacts[0]
    assert journals[3].original.response_artifact == waiter_artifacts[0]
    assert journals[2].original.settlement is None
    assert journals[3].original.settlement is None
    assert trace.count("peer.request_observed") == 1
    assert trace.count("peer.response_written") == 1

    def before(left: str, right: str) -> None:
        assert trace.index(left) < trace.index(right), trace

    for left, right in (
        ("peer.response_written", "waiter.returned"),
        ("waiter.returned", "journal.response_accepted"),
        ("journal.response_accepted", "ledger.response_recorded"),
        ("ledger.response_recorded", "ledger.request_response_accepted"),
        ("ledger.request_response_accepted", "journal.quarantined"),
        ("journal.quarantined", "ledger.request_quarantined"),
    ):
        before(left, right)
    assert "ledger.request_idle_published" not in trace
    assert "ledger.request_completed" not in trace
    assert "ledger.request_rejected" not in trace


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_mode",
    ["precommit", "aftercommit", "drift", "returned_forgery"],
)
async def test_idle_published_journal_save_failure_uses_exact_cas_handoff_without_resave(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    command = _command(harness, timeout=0.3)
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    expected_settlement = _completed_artifact(command).accepted_response.settlement
    assert type(expected_settlement) is CompletedSettlement
    expected_result_json = _canonical_json(expected_settlement.result).decode("utf-8")
    converges = failure_mode in {"aftercommit", "returned_forgery"}
    injected = (
        None
        if failure_mode == "returned_forgery"
        else RuntimeError(f"{failure_mode} idle-published journal save failure")
    )

    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected_settlement.result))
    monkeypatch.setattr(peer, "result_for", _forbidden)
    transport = _transport(harness)
    response_path = harness.paths.response_path(command.request_id)
    journal_candidates: list[PendingJournal] = []
    waiter_artifacts: list[ResponseArtifact] = []
    published_deliveries: list[PreparedDelivery] = []
    post_fault_loads: list[PendingJournal | None] = []
    classified_errors: list[tuple[BaseException, BaseException]] = []
    request_transitions: list[RequestRecord] = []
    queued_paths: list[Path] = []
    response_delivery: DeliveryRecord | None = None
    response_accepted_request: RequestRecord | None = None
    frozen_idle_request: RequestRecord | None = None
    frozen_terminal_request: RequestRecord | None = None
    intended_r3: PendingJournal | None = None
    durable_r3: PendingJournal | None = None
    target_save_calls = 0
    recovery_validate_calls = 0
    recovery_dump_calls = 0
    idle_calls = 0
    journal_delete_calls = 0
    fault_boundary_crossed = False
    journal_r2_bytes = b""
    journal_r2_mtime = 0
    durable_r3_bytes = b""
    durable_r3_mtime = 0
    request_inbox_bytes = b""
    request_inbox_mtime = 0
    idle_inbox_bytes = b""
    idle_inbox_mtime = 0
    response_bytes = b""
    response_mtime = 0
    unlock_finished = False
    runner_ref: MutationExchange | None = None

    original_enter = RootLock.__aenter__
    original_exit = RootLock.__aexit__
    original_save = transport.journals.save
    original_load = transport.journals.load
    original_delete = transport.journals.delete
    original_record_response = transport.ledger.record_response
    original_transition = transport.ledger.transition
    original_publish_delivery = InboxPublisher.publish_delivery
    original_publish_idle = InboxPublisher.publish_idle
    original_wait = ResponseWaiter.wait
    original_response_delete = cleanup_module._set_delete_disposition  # pyright: ignore[reportPrivateUsage]
    original_classify = mutation_module._classify_exchange_error  # pyright: ignore[reportPrivateUsage]

    def raw_response_columns() -> tuple[object, object, object, object, object]:
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            row = connection.execute(
                """
                SELECT response_at_ms,response_sha256,response_size_bytes,
                       accepted_response_json,settlement_json
                FROM deliveries WHERE delivery_id=?
                """,
                (str(DELIVERY_ID),),
            ).fetchone()
        assert row is not None
        return cast(tuple[object, object, object, object, object], tuple(row))

    def expected_response_columns(
        artifact: ResponseArtifact,
    ) -> tuple[int, str, int, bytes, bytes]:
        return (
            artifact.accepted_at_ms,
            artifact.sha256,
            artifact.size_bytes,
            _canonical_json(artifact.accepted_response.model_dump(mode="json")),
            _canonical_json(expected_settlement.model_dump(mode="json")),
        )

    def request_with(record: RequestRecord, **changes: object) -> RequestRecord:
        tree = record.model_dump(mode="python", round_trip=True, warnings=False)
        tree.update(changes)
        return RequestRecord.model_validate(tree)

    async def traced_enter(lock: RootLock) -> RootLock:
        result = await original_enter(lock)
        trace.append("session.lock_acquired")
        return result

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        nonlocal unlock_finished
        if waiter_artifacts:
            assert response_path.exists()
        result = await original_exit(lock, exc_type, exc, traceback)
        unlock_finished = True
        trace.append("session.unlock")
        if waiter_artifacts:
            assert response_path.exists()
        return result

    async def traced_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        nonlocal response_bytes, response_mtime
        assert waiter._response_path == response_path  # pyright: ignore[reportPrivateUsage]
        artifact = await original_wait(waiter, timeout_seconds)
        settlement = artifact.accepted_response.settlement
        assert type(settlement) is CompletedSettlement
        assert settlement.result == expected_settlement.result
        response_bytes = response_path.read_bytes()
        response_mtime = response_path.stat().st_mtime_ns
        assert artifact.sha256 == hashlib.sha256(response_bytes).hexdigest()
        assert artifact.size_bytes == len(response_bytes)
        waiter_artifacts.append(artifact)
        trace.append("waiter.returned")
        return artifact

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal target_save_calls, intended_r3, durable_r3
        nonlocal frozen_idle_request, frozen_terminal_request
        nonlocal fault_boundary_crossed, journal_r2_bytes, journal_r2_mtime
        nonlocal durable_r3_bytes, durable_r3_mtime
        original = journal.original
        if original.state is not PendingPhase.IDLE_PUBLISHED:
            journal_candidates.append(journal)
            revisions = original_save(journal, expected_revisions=expected_revisions)
            trace.append(f"journal.{original.state.value}")
            if original.state is PendingPhase.RESPONSE_ACCEPTED:
                assert expected_revisions == JournalRevisions(
                    original=1,
                    reconcile_attempt=None,
                )
                assert original.revision == 2
                assert len(waiter_artifacts) == 1
                assert original.response_artifact == waiter_artifacts[0]
                assert original.settlement == expected_settlement
                journal_r2_bytes = harness.paths.pending_file.read_bytes()
                journal_r2_mtime = harness.paths.pending_file.stat().st_mtime_ns
            return revisions

        target_save_calls += 1
        assert target_save_calls == 1, "idle-published journal save was retried"
        assert expected_revisions == JournalRevisions(
            original=2,
            reconcile_attempt=None,
        )
        assert original.revision == 3
        assert len(journal_candidates) == 3
        assert journal_candidates[-1].original.state is PendingPhase.RESPONSE_ACCEPTED
        assert original.response_artifact == waiter_artifacts[0]
        assert original.settlement == expected_settlement
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        assert harness.paths.inbox.read_bytes() == idle_inbox_bytes
        assert harness.paths.inbox.stat().st_mtime_ns == idle_inbox_mtime
        assert runner_ref is not None
        state = runner_ref._current_state  # pyright: ignore[reportPrivateUsage]
        assert state is not None
        assert state.idle_publication_entered is True
        assert state.idle_publication_returned is True
        assert state.response_accepted_journal is journal_candidates[-1]
        assert state.response_artifact is waiter_artifacts[0]
        assert state.safety_finished is False
        response_request = state.response_accepted_request
        assert response_request is not None
        frozen_idle_request = state.idle_published_request
        frozen_terminal_request = state.terminal_request
        if frozen_idle_request is not None and frozen_terminal_request is not None:
            assert frozen_idle_request is state.idle_published_request
            assert frozen_terminal_request is state.terminal_request
            assert frozen_idle_request == request_with(
                response_request,
                state=HostRequestState.IDLE_PUBLISHED,
                updated_at_ms=original.updated_at_ms,
            )
            assert frozen_terminal_request == request_with(
                frozen_idle_request,
                state=HostRequestState.COMPLETED,
                updated_at_ms=frozen_terminal_request.updated_at_ms,
                terminal_at_ms=frozen_terminal_request.updated_at_ms,
                result_json=expected_result_json,
                error_json=None,
                resolution_json=None,
            )
            assert frozen_terminal_request.updated_at_ms >= original.updated_at_ms
            assert frozen_terminal_request.terminal_at_ms == (frozen_terminal_request.updated_at_ms)
        intended_r3 = journal
        journal_candidates.append(journal)
        trace.append("journal.idle_published_entered")

        if failure_mode == "precommit":
            assert injected is not None
            fault_boundary_crossed = True
            trace.append("journal.idle_published_fault")
            raise injected

        if failure_mode == "drift":
            assert injected is not None
            drift = _journal_with_original(
                journal,
                updated_at_ms=journal.original.updated_at_ms + 1,
            )
            assert drift != journal
            revisions = original_save(drift, expected_revisions=expected_revisions)
            assert revisions == JournalRevisions(original=3, reconcile_attempt=None)
            durable_r3 = drift
            durable_r3_bytes = harness.paths.pending_file.read_bytes()
            durable_r3_mtime = harness.paths.pending_file.stat().st_mtime_ns
            fault_boundary_crossed = True
            trace.append("journal.drift_committed")
            trace.append("journal.idle_published_fault")
            raise injected

        revisions = original_save(journal, expected_revisions=expected_revisions)
        assert revisions == JournalRevisions(original=3, reconcile_attempt=None)
        durable_r3 = journal
        durable_r3_bytes = harness.paths.pending_file.read_bytes()
        durable_r3_mtime = harness.paths.pending_file.stat().st_mtime_ns
        fault_boundary_crossed = True
        trace.append("journal.idle_published_committed")
        if failure_mode == "aftercommit":
            assert injected is not None
            trace.append("journal.idle_published_fault")
            raise injected
        assert failure_mode == "returned_forgery"
        trace.append("journal.idle_published_returned_forgery")
        return JournalRevisions(original=4, reconcile_attempt=None)

    def traced_load() -> object:
        loaded = original_load()
        if fault_boundary_crossed and journal_delete_calls == 0:
            post_fault_loads.append(None if loaded is None else loaded.journal)
            trace.append("journal.post_fault_load")
        return loaded

    def traced_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        nonlocal response_delivery
        assert len(waiter_artifacts) == 1 and artifact is waiter_artifacts[0]
        assert journal_candidates[-1].original.state is PendingPhase.RESPONSE_ACCEPTED
        assert raw_response_columns() == (None, None, None, None, None)
        response_delivery = original_record_response(artifact)
        assert response_delivery.response_artifact == artifact
        assert response_delivery.settlement == expected_settlement
        assert raw_response_columns() == expected_response_columns(artifact)
        trace.append("ledger.response_recorded")
        return response_delivery

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal response_accepted_request
        assert request_id == command.request_id
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            assert expected_states == frozenset({HostRequestState.PUBLISHED})
            assert response_delivery is not None
            assert terminal_at_ms is None
            assert result_json is error_json is resolution_json is None
        elif new_state is HostRequestState.IDLE_PUBLISHED:
            assert converges
            assert expected_states == frozenset({HostRequestState.RESPONSE_ACCEPTED})
            assert frozen_idle_request is not None
            assert updated_at_ms == frozen_idle_request.updated_at_ms
            assert target_save_calls == 1
            assert len(post_fault_loads) == 1
            assert intended_r3 is not None and durable_r3 is intended_r3
            assert post_fault_loads[0] == intended_r3
            assert journal_delete_calls == 0
            assert terminal_at_ms is None
            assert result_json is error_json is resolution_json is None
        elif new_state is HostRequestState.COMPLETED:
            assert converges
            assert expected_states == frozenset({HostRequestState.IDLE_PUBLISHED})
            assert frozen_terminal_request is not None
            assert updated_at_ms == frozen_terminal_request.updated_at_ms
            assert terminal_at_ms == frozen_terminal_request.terminal_at_ms
            assert result_json == frozen_terminal_request.result_json
            assert error_json == frozen_terminal_request.error_json
            assert resolution_json == frozen_terminal_request.resolution_json
            assert journal_delete_calls == 0
        elif new_state is not HostRequestState.PUBLISHED:
            raise AssertionError(f"unexpected request transition to {new_state.value}")
        observed = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        request_transitions.append(observed)
        trace.append(f"ledger.request_{new_state.value}")
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            response_accepted_request = observed
        elif new_state is HostRequestState.IDLE_PUBLISHED:
            assert observed == frozen_idle_request
        elif new_state is HostRequestState.COMPLETED:
            assert observed == frozen_terminal_request
        return observed

    def traced_publish_delivery(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal request_inbox_bytes, request_inbox_mtime
        assert publisher is transport.inbox
        assert not published_deliveries
        original_publish_delivery(
            publisher,
            delivery,
            runtime_snapshot=runtime_snapshot,
        )
        request_inbox_bytes = harness.paths.inbox.read_bytes()
        request_inbox_mtime = harness.paths.inbox.stat().st_mtime_ns
        assert request_inbox_bytes == render_delivery_lua(delivery, runtime_snapshot)
        published_deliveries.append(delivery)
        trace.append("inbox.request_replaced")

    def traced_publish_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_calls, idle_inbox_bytes, idle_inbox_mtime
        assert publisher is transport.inbox
        assert runner_ref is not None
        state = runner_ref._current_state  # pyright: ignore[reportPrivateUsage]
        assert state is not None
        assert state.idle_publication_entered is True
        assert state.idle_publication_returned is False
        assert state.response_accepted_journal is journal_candidates[-1]
        assert state.idle_published_journal is None
        assert state.safety_finished is False
        assert response_accepted_request is not None
        assert state.response_accepted_request == response_accepted_request
        assert response_delivery is not None
        assert state.response_delivery == response_delivery
        assert recovery_validate_calls == recovery_dump_calls == 1
        assert harness.paths.inbox.read_bytes() == request_inbox_bytes
        assert harness.paths.inbox.stat().st_mtime_ns == request_inbox_mtime
        assert response_path.exists()
        idle_calls += 1
        assert idle_calls == 1
        original_publish_idle(publisher)
        idle_inbox_bytes = harness.paths.inbox.read_bytes()
        idle_inbox_mtime = harness.paths.inbox.stat().st_mtime_ns
        assert idle_inbox_bytes == render_idle_lua()
        assert state.idle_publication_entered is True
        assert state.idle_publication_returned is False
        trace.append("inbox.idle_replaced")

    recovery_delegate = recovery_schema.adapter

    class RecoveryAdapterSpy:
        def validate_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_validate_calls
            assert value == expected_settlement.result
            assert response_accepted_request is not None
            assert response_delivery is not None
            assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])
            assert runner_ref is not None
            state = runner_ref._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            assert state.idle_publication_entered is False
            assert state.idle_publication_returned is False
            assert state.safety_finished is False
            recovery_validate_calls += 1
            assert recovery_validate_calls == 1
            trace.append("recovery.validate")
            return cast(Any, recovery_delegate).validate_python(value, *args, **kwargs)

        def dump_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_dump_calls
            assert recovery_validate_calls == 1
            normalized = cast(
                JsonValue,
                cast(Any, recovery_delegate).dump_python(value, *args, **kwargs),
            )
            assert _canonical_json(normalized) == _canonical_json(expected_settlement.result)
            assert runner_ref is not None
            state = runner_ref._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            assert state.idle_publication_entered is False
            assert state.idle_publication_returned is False
            assert state.safety_finished is False
            recovery_dump_calls += 1
            assert recovery_dump_calls == 1
            trace.append("recovery.dump")
            return normalized

        def __getattr__(self, name: str) -> object:
            return getattr(recovery_delegate, name)

    recovery_spy = RecoveryAdapterSpy()
    adapter_descriptor = inspect.getattr_static(SchemaBinding, "adapter")
    descriptor_get = getattr(adapter_descriptor, "__get__", None)
    assert callable(descriptor_get)

    def guarded_schema_adapter(binding: SchemaBinding) -> object:
        if binding is recovery_schema:
            return recovery_spy
        return cast(Any, descriptor_get)(binding, SchemaBinding)

    def traced_delete(expected: JournalDeleteExpectation) -> None:
        nonlocal journal_delete_calls
        assert converges
        assert intended_r3 is not None and durable_r3 is intended_r3
        assert expected.root_key == harness.paths.root_key
        assert expected.required_release_id == command.runtime_snapshot.release_id
        assert expected.original_request_id == command.request_id
        assert expected.reconcile_attempt_request_id is None
        assert expected.revisions == JournalRevisions(
            original=3,
            reconcile_attempt=None,
        )
        terminal = transport.ledger.get_request(command.request_id)
        assert terminal is not None and terminal.state is HostRequestState.COMPLETED
        assert response_path.exists()
        journal_delete_calls += 1
        assert journal_delete_calls == 1
        original_delete(expected)
        assert not harness.paths.pending_file.exists()
        trace.append("journal.deleted")

    def traced_classify(error: BaseException) -> BaseException:
        primary = original_classify(error)
        classified_errors.append((error, primary))
        trace.append("error.classified")
        return primary

    def traced_response_delete(handle: int, path: Path) -> None:
        if path == response_path.resolve(strict=True):
            assert converges
            assert len(queued_paths) == 1
            assert queued_paths[0] == response_path
            assert unlock_finished
            assert path.exists()
        original_response_delete(handle, path)
        if path == response_path.resolve(strict=False):
            trace.append("response.unlinked")

    monkeypatch.setattr(RootLock, "__aenter__", traced_enter)
    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)
    monkeypatch.setattr(transport.journals, "save", traced_save)
    monkeypatch.setattr(transport.journals, "load", traced_load)
    monkeypatch.setattr(transport.journals, "delete", traced_delete)
    monkeypatch.setattr(transport.ledger, "record_response", traced_record_response)
    monkeypatch.setattr(transport.ledger, "transition", traced_transition)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish_delivery)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_publish_idle)
    monkeypatch.setattr(ResponseWaiter, "wait", traced_wait)
    monkeypatch.setattr(mutation_module, "_classify_exchange_error", traced_classify)
    monkeypatch.setattr(
        SchemaBinding,
        "adapter",
        property(guarded_schema_adapter),
        raising=False,
    )
    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(cleanup_module, "_set_delete_disposition", traced_response_delete)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with peer:
        async with transport.session() as channel:
            concrete = cast(_FileBridgeChannel, channel)
            runner = concrete._mutation_exchange  # pyright: ignore[reportPrivateUsage]
            runner_ref = runner
            original_queue = runner._queue_response_cleanup  # pyright: ignore[reportPrivateUsage]

            def traced_queue(path: Path) -> None:
                harness.lock.require_acquired()
                assert converges
                assert path == response_path
                assert path is runner._current_state.response_path  # pyright: ignore[reportOptionalMemberAccess,reportPrivateUsage]
                assert journal_delete_calls == 1
                assert not harness.paths.pending_file.exists()
                terminal = transport.ledger.get_request(command.request_id)
                assert terminal is not None and terminal.state is HostRequestState.COMPLETED
                assert response_path.exists()
                queued_paths.append(path)
                assert len(queued_paths) == 1
                original_queue(path)
                trace.append("cleanup.queued")

            monkeypatch.setattr(runner, "_queue_response_cleanup", traced_queue)
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

            assert frozen_idle_request is not None
            assert frozen_terminal_request is not None
            assert target_save_calls == 1
            assert len(post_fault_loads) == 1
            assert len(classified_errors) == 1
            classified_input, classified_primary = classified_errors[0]
            assert caught.value is classified_primary
            if failure_mode == "returned_forgery":
                assert injected is None
                assert classified_input is caught.value
                assert isinstance(classified_input, BridgeError)
                assert classified_input.code is ErrorCode.STATE_CONFLICT
                assert classified_input.message == (
                    "idle-published mutation journal save returned drift"
                )
                assert caught.value.__cause__ is None
            else:
                assert injected is not None
                assert classified_input is injected
                assert caught.value.__cause__ is injected
                assert caught.value.to_payload() == {
                    "code": ErrorCode.STATE_CONFLICT.value,
                    "message": "file-bridge mutation exchange failed",
                    "details": {"type": "RuntimeError"},
                }

            notes_value = cast(list[str] | None, getattr(caught.value, "__notes__", None))
            notes = () if notes_value is None else tuple(notes_value)
            if failure_mode == "drift":
                assert notes == (
                    "artifact safety failure: BridgeError: "
                    "idle-published mutation journal evidence drifted",
                )
            else:
                assert notes == ()

            assert concrete._active_task is None  # pyright: ignore[reportPrivateUsage]
            state = runner._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            assert state.artifact_observed is True
            assert len(waiter_artifacts) == 1
            assert state.response_artifact is waiter_artifacts[0]
            assert state.response_accepted_journal is journal_candidates[2]
            assert intended_r3 is journal_candidates[3]
            assert state.idle_published_journal is intended_r3
            assert state.response_accepted_request == response_accepted_request
            assert frozen_idle_request is not None
            assert frozen_terminal_request is not None
            assert state.idle_published_request is frozen_idle_request
            assert state.terminal_request is frozen_terminal_request
            assert state.response_delivery == response_delivery
            assert state.idle_publication_entered is True
            assert state.idle_publication_returned is True
            assert idle_calls == recovery_validate_calls == recovery_dump_calls == 1
            assert harness.paths.inbox.read_bytes() == idle_inbox_bytes == render_idle_lua()
            assert harness.paths.inbox.stat().st_mtime_ns == idle_inbox_mtime
            assert response_path.exists()
            assert hashlib.sha256(response_bytes).hexdigest() == waiter_artifacts[0].sha256
            assert response_path.stat().st_mtime_ns == response_mtime
            assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])

            final_request = transport.ledger.get_request(command.request_id)
            final_delivery = transport.ledger.get_delivery(DELIVERY_ID)
            assert final_request is not None and final_delivery is not None
            assert final_delivery.response_artifact == waiter_artifacts[0]
            assert final_delivery.settlement == expected_settlement

            if converges:
                assert failure_mode in {"aftercommit", "returned_forgery"}
                assert intended_r3 is not None and durable_r3 is intended_r3
                assert post_fault_loads[0] == intended_r3
                assert state.journal is intended_r3
                assert original_load() is None
                assert not harness.paths.pending_file.exists()
                assert journal_delete_calls == 1
                assert len(queued_paths) == 1
                assert concrete._cleanup_paths == queued_paths  # pyright: ignore[reportPrivateUsage]
                assert concrete._cleanup_paths[0] is queued_paths[0]  # pyright: ignore[reportPrivateUsage]
                assert concrete._poisoned is False  # pyright: ignore[reportPrivateUsage]
                assert runner._requires_fresh_session() is False  # pyright: ignore[reportPrivateUsage]
                assert state.safety_finished is True
                assert final_request.state is HostRequestState.COMPLETED
                assert final_request.terminal_at_ms == final_request.updated_at_ms
                assert final_request.result_json == expected_result_json
                assert final_request.error_json is None
                assert final_request.resolution_json is None
                idle_records = [
                    record
                    for record in request_transitions
                    if record.state is HostRequestState.IDLE_PUBLISHED
                ]
                terminal_records = [
                    record
                    for record in request_transitions
                    if record.state is HostRequestState.COMPLETED
                ]
                assert len(idle_records) == len(terminal_records) == 1
                assert frozen_idle_request == idle_records[0]
                assert frozen_terminal_request == terminal_records[0]
                assert response_path.exists()
            else:
                assert failure_mode in {"precommit", "drift"}
                assert journal_delete_calls == 0
                assert queued_paths == []
                assert concrete._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
                assert concrete._poisoned is True  # pyright: ignore[reportPrivateUsage]
                assert runner._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]
                assert state.safety_finished is (failure_mode == "precommit")
                assert final_request.state is HostRequestState.RESPONSE_ACCEPTED
                assert final_request.terminal_at_ms is None
                assert final_request.result_json is None
                assert final_request.error_json is None
                assert final_request.resolution_json is None
                assert all(
                    record.state
                    not in {HostRequestState.IDLE_PUBLISHED, HostRequestState.COMPLETED}
                    for record in request_transitions
                )
                loaded = original_load()
                assert loaded is not None
                expected_durable = (
                    journal_candidates[2] if failure_mode == "precommit" else durable_r3
                )
                assert expected_durable is not None
                assert loaded.journal == expected_durable
                assert post_fault_loads[0] == expected_durable
                assert state.journal is post_fault_loads[0]
                expected_bytes = (
                    journal_r2_bytes if failure_mode == "precommit" else durable_r3_bytes
                )
                expected_mtime = (
                    journal_r2_mtime if failure_mode == "precommit" else durable_r3_mtime
                )
                assert harness.paths.pending_file.read_bytes() == expected_bytes
                assert harness.paths.pending_file.stat().st_mtime_ns == expected_mtime
                if failure_mode == "drift":
                    assert durable_r3 is not None and durable_r3 != intended_r3
                assert response_path.exists()

    assert caught is not None
    assert unlock_finished
    assert target_save_calls == 1
    assert len(post_fault_loads) == 1
    assert [journal.original.state for journal in journal_candidates] == [
        PendingPhase.PREPARED,
        PendingPhase.PUBLISHED,
        PendingPhase.RESPONSE_ACCEPTED,
        PendingPhase.IDLE_PUBLISHED,
    ]
    assert [journal.original.revision for journal in journal_candidates] == [0, 1, 2, 3]
    assert journal_candidates[2].original.response_artifact == waiter_artifacts[0]
    assert journal_candidates[3].original.response_artifact == waiter_artifacts[0]
    assert journal_candidates[2].original.settlement == expected_settlement
    assert journal_candidates[3].original.settlement == expected_settlement
    assert harness.paths.inbox.read_bytes() == idle_inbox_bytes == render_idle_lua()
    assert harness.paths.inbox.stat().st_mtime_ns == idle_inbox_mtime
    if converges:
        assert not response_path.exists()
        assert not harness.paths.pending_file.exists()
        assert journal_delete_calls == 1
        assert len(queued_paths) == 1
    else:
        assert response_path.exists()
        assert response_path.read_bytes() == response_bytes
        assert response_path.stat().st_mtime_ns == response_mtime
        assert harness.paths.pending_file.exists()
        assert journal_delete_calls == 0
        assert queued_paths == []
    assert trace.count("peer.request_observed") == 1
    assert trace.count("peer.response_written") == 1
    assert trace.count("journal.idle_published_entered") == 1
    assert trace.count("journal.post_fault_load") == 1

    def before(left: str, right: str) -> None:
        assert trace.index(left) < trace.index(right), trace

    for left, right in (
        ("inbox.request_replaced", "peer.request_observed"),
        ("peer.response_written", "waiter.returned"),
        ("waiter.returned", "journal.response_accepted"),
        ("journal.response_accepted", "ledger.response_recorded"),
        ("ledger.response_recorded", "ledger.request_response_accepted"),
        ("ledger.request_response_accepted", "recovery.validate"),
        ("recovery.validate", "recovery.dump"),
        ("recovery.dump", "inbox.idle_replaced"),
        ("inbox.idle_replaced", "journal.idle_published_entered"),
        ("journal.idle_published_entered", "journal.post_fault_load"),
    ):
        before(left, right)
    if converges:
        for left, right in (
            ("journal.post_fault_load", "ledger.request_idle_published"),
            ("ledger.request_idle_published", "ledger.request_completed"),
            ("ledger.request_completed", "journal.deleted"),
            ("journal.deleted", "cleanup.queued"),
            ("cleanup.queued", "session.unlock"),
            ("session.unlock", "response.unlinked"),
        ):
            before(left, right)
    else:
        assert "ledger.request_idle_published" not in trace
        assert "ledger.request_completed" not in trace
        assert "journal.deleted" not in trace
        assert "cleanup.queued" not in trace
        assert "response.unlinked" not in trace


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("boundary", "failure_mode", "settlement_kind"),
    [
        (boundary, failure_mode, "completed")
        for failure_mode in ("precommit", "aftercommit", "drift", "returned_forgery")
        for boundary in ("idle", "terminal")
    ]
    + [("terminal", "aftercommit", "rejected")],
)
async def test_terminal_sqlite_transition_failure_rereads_once_without_retry(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    failure_mode: str,
    settlement_kind: str,
) -> None:
    command = _command(harness, timeout=0.3)
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    expected_error: ResponseError | None = None
    if settlement_kind == "completed":
        expected = _completed_artifact(command).accepted_response.settlement
        assert type(expected) is CompletedSettlement
        peer.enqueue(Respond(result=expected.result))
        terminal_state = HostRequestState.COMPLETED
    else:
        assert settlement_kind == "rejected"
        assert boundary == "terminal" and failure_mode == "aftercommit"
        error_payload = cast(
            dict[str, JsonValue],
            {
                "code": ErrorCode.CMO_LUA_ERROR.value,
                "message": "request rejected before mutation execution",
                "details": {},
                "mutation_not_started": {
                    "schema_version": 1,
                    "stage": "handler_preflight",
                    "request_id": str(command.request_id),
                    "request_hash": hashlib.sha256(canonical_body_bytes(command.body)).hexdigest(),
                    "operation": command.body.operation,
                    "mutation_barrier_written": False,
                    "execute_started": False,
                },
            },
        )
        expected_error = ResponseError.model_validate(error_payload)
        peer.enqueue(Respond(error=error_payload))
        terminal_state = HostRequestState.REJECTED
    response_path = harness.paths.response_path(command.request_id)
    queued_paths: list[Path] = []
    exchange = _exchange(harness, queue_response_cleanup=queued_paths.append)
    injected = (
        None
        if failure_mode == "returned_forgery"
        else RuntimeError(f"{boundary} transition {failure_mode}")
    )
    converges = failure_mode in {"aftercommit", "returned_forgery"}

    original_transition = harness.ledger.transition
    original_get_request = harness.ledger.get_request
    original_save = harness.journals.save
    original_load = harness.journals.load
    original_delete = harness.journals.delete
    original_publish_idle = harness.inbox.publish_idle
    original_classify = mutation_module._classify_exchange_error  # pyright: ignore[reportPrivateUsage]
    boundary_calls = 0
    idle_transition_calls = 0
    terminal_transition_calls = 0
    post_fault_request_reads = 0
    post_fault_journal_loads = 0
    idle_journal_saves = 0
    delete_calls = 0
    idle_publish_calls = 0
    fault_crossed = False
    r3_bytes = b""
    r3_mtime = 0
    response_candidate: RequestRecord | None = None
    idle_candidate: RequestRecord | None = None
    terminal_candidate: RequestRecord | None = None
    intended_r3: PendingJournal | None = None
    actual_drift: RequestRecord | None = None
    post_fault_observations: list[RequestRecord | None] = []
    classified_errors: list[tuple[BaseException, BaseException]] = []
    response_bytes = b""
    response_mtime = 0

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal idle_journal_saves
        if journal.original.state is PendingPhase.IDLE_PUBLISHED:
            idle_journal_saves += 1
        return original_save(journal, expected_revisions=expected_revisions)

    def failing_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal boundary_calls, fault_crossed, r3_bytes, r3_mtime
        nonlocal idle_transition_calls, terminal_transition_calls
        nonlocal response_candidate, idle_candidate, terminal_candidate
        nonlocal intended_r3, actual_drift, response_bytes, response_mtime
        if new_state is HostRequestState.IDLE_PUBLISHED:
            idle_transition_calls += 1
        elif new_state in {HostRequestState.COMPLETED, HostRequestState.REJECTED}:
            terminal_transition_calls += 1

        is_boundary = (boundary == "idle" and new_state is HostRequestState.IDLE_PUBLISHED) or (
            boundary == "terminal" and new_state is terminal_state
        )
        if not is_boundary:
            return original_transition(
                request_id,
                expected_states=expected_states,
                new_state=new_state,
                updated_at_ms=updated_at_ms,
                terminal_at_ms=terminal_at_ms,
                result_json=result_json,
                error_json=error_json,
                resolution_json=resolution_json,
            )

        boundary_calls += 1
        assert boundary_calls == 1, "failed SQLite transition was retried"
        state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
        assert state is not None
        response_candidate = state.response_accepted_request
        idle_candidate = state.idle_published_request
        terminal_candidate = state.terminal_request
        assert response_candidate is not None
        assert idle_candidate is not None
        assert terminal_candidate is not None
        assert state.journal is state.idle_published_journal
        assert state.journal is not None
        intended_r3 = state.journal
        assert state.journal.original.state is PendingPhase.IDLE_PUBLISHED
        assert state.journal.original.revision == 3
        r3_bytes = harness.paths.pending_file.read_bytes()
        r3_mtime = harness.paths.pending_file.stat().st_mtime_ns
        response_bytes = response_path.read_bytes()
        response_mtime = response_path.stat().st_mtime_ns

        kwargs: dict[str, object] = {
            "expected_states": expected_states,
            "new_state": new_state,
            "updated_at_ms": updated_at_ms,
            "terminal_at_ms": terminal_at_ms,
            "result_json": result_json,
            "error_json": error_json,
            "resolution_json": resolution_json,
        }
        if failure_mode == "precommit":
            assert injected is not None
            fault_crossed = True
            raise injected
        if failure_mode == "drift":
            assert injected is not None
            kwargs["updated_at_ms"] = updated_at_ms + 1
            if terminal_at_ms is not None:
                kwargs["terminal_at_ms"] = terminal_at_ms + 1
            actual_drift = cast(
                RequestRecord,
                cast(Any, original_transition)(request_id, **kwargs),
            )
            fault_crossed = True
            raise injected

        committed = cast(RequestRecord, cast(Any, original_transition)(request_id, **kwargs))
        fault_crossed = True
        if failure_mode == "aftercommit":
            assert injected is not None
            raise injected
        assert failure_mode == "returned_forgery"
        tree = committed.model_dump(mode="python", round_trip=True, warnings=False)
        tree["updated_at_ms"] = committed.updated_at_ms + 1
        if committed.terminal_at_ms is not None:
            tree["terminal_at_ms"] = committed.terminal_at_ms + 1
        return RequestRecord.model_validate(tree)

    def traced_get_request(request_id: UUID) -> RequestRecord | None:
        nonlocal post_fault_request_reads
        observed = original_get_request(request_id)
        if fault_crossed:
            post_fault_request_reads += 1
            post_fault_observations.append(observed)
        return observed

    def traced_load() -> object:
        nonlocal post_fault_journal_loads
        if fault_crossed and delete_calls == 0:
            post_fault_journal_loads += 1
        return original_load()

    def traced_delete(expected_delete: JournalDeleteExpectation) -> None:
        nonlocal delete_calls
        delete_calls += 1
        original_delete(expected_delete)

    def traced_publish_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_publish_calls
        assert publisher is harness.inbox
        idle_publish_calls += 1
        original_publish_idle()

    def traced_classify(error: BaseException) -> BaseException:
        primary = original_classify(error)
        classified_errors.append((error, primary))
        return primary

    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(harness.journals, "save", traced_save)
    monkeypatch.setattr(harness.journals, "load", traced_load)
    monkeypatch.setattr(harness.journals, "delete", traced_delete)
    monkeypatch.setattr(harness.ledger, "transition", failing_transition)
    monkeypatch.setattr(harness.ledger, "get_request", traced_get_request)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_publish_idle)
    monkeypatch.setattr(mutation_module, "_classify_exchange_error", traced_classify)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    loaded = None
    state = None
    async with peer:
        async with harness.lock:
            with pytest.raises(BridgeError) as caught:
                await exchange.run(command)
            state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            loaded = original_load()

    assert caught is not None
    assert state is not None
    assert boundary_calls == 1
    assert post_fault_request_reads == 1
    assert post_fault_journal_loads == 0
    assert len(post_fault_observations) == 1
    assert len(classified_errors) == 1
    classified_input, classified_primary = classified_errors[0]
    assert caught.value is classified_primary
    assert idle_journal_saves == 1
    assert idle_publish_calls == 1
    assert idle_transition_calls == 1
    expected_terminal_calls = 1 if boundary == "terminal" or converges else 0
    assert terminal_transition_calls == expected_terminal_calls
    assert trace.count("peer.request_observed") == 1
    assert trace.count("peer.response_written") == 1
    assert response_path.exists()
    assert response_path.read_bytes() == response_bytes
    assert response_path.stat().st_mtime_ns == response_mtime

    if failure_mode == "returned_forgery":
        assert classified_input is caught.value
        assert caught.value.code is ErrorCode.STATE_CONFLICT
        assert caught.value.message == (
            "idle-published request transition returned drift"
            if boundary == "idle"
            else "terminal request transition returned drift"
        )
        assert caught.value.__cause__ is None
    else:
        assert injected is not None
        assert classified_input is injected
        assert caught.value.to_payload() == {
            "code": ErrorCode.STATE_CONFLICT.value,
            "message": "file-bridge mutation exchange failed",
            "details": {"type": "RuntimeError"},
        }
        assert caught.value.__cause__ is injected

    notes_value = cast(list[str] | None, getattr(caught.value, "__notes__", None))
    notes = () if notes_value is None else tuple(notes_value)
    if failure_mode == "drift":
        assert notes == (
            "artifact safety failure: BridgeError: terminal continuation request evidence drifted",
        )
    else:
        assert notes == ()

    assert getattr(state, "idle_request_transition_entered") is True
    if boundary == "idle":
        assert getattr(state, "idle_request_transition_returned") is (
            failure_mode == "returned_forgery"
        )
        assert getattr(state, "terminal_request_transition_entered") is converges
        assert getattr(state, "terminal_request_transition_returned") is converges
    else:
        assert getattr(state, "idle_request_transition_returned") is True
        assert getattr(state, "terminal_request_transition_entered") is True
        assert getattr(state, "terminal_request_transition_returned") is (
            failure_mode == "returned_forgery"
        )
    assert state.terminal_continuation_finished is converges
    assert state.journal_delete_entered is converges
    assert state.journal_delete_returned is converges
    assert state.safety_finished is (failure_mode != "drift")
    assert exchange._requires_fresh_session() is (not converges)  # pyright: ignore[reportPrivateUsage]

    final_request = original_get_request(command.request_id)
    assert final_request is not None
    assert response_candidate is not None
    assert idle_candidate is not None
    assert terminal_candidate is not None
    assert intended_r3 is not None
    assert state.journal is intended_r3
    assert state.response_accepted_request is response_candidate
    assert state.idle_published_request is idle_candidate
    assert state.terminal_request is terminal_candidate
    expected_observation = (
        terminal_candidate
        if boundary == "terminal" and converges
        else idle_candidate
        if boundary == "idle" and converges
        else response_candidate
        if boundary == "idle" and failure_mode == "precommit"
        else idle_candidate
        if boundary == "terminal" and failure_mode == "precommit"
        else actual_drift
    )
    assert expected_observation is not None
    assert post_fault_observations == [expected_observation]
    assert getattr(state, "terminal_safety_reread_entered") is True
    assert getattr(state, "terminal_safety_reread_returned") is True
    assert getattr(state, "terminal_safety_observed_request") == expected_observation
    if converges:
        assert loaded is None
        assert delete_calls == 1
        assert queued_paths == [response_path]
        assert final_request == terminal_candidate
        if settlement_kind == "rejected":
            assert expected_error is not None
            assert final_request.state is HostRequestState.REJECTED
            assert final_request.result_json is None
            assert final_request.error_json == _canonical_json(
                expected_error.model_dump(mode="json")
            ).decode("utf-8")
    else:
        assert loaded is not None
        assert loaded.journal.original.state is PendingPhase.IDLE_PUBLISHED
        assert loaded.journal.original.revision == 3
        assert harness.paths.pending_file.read_bytes() == r3_bytes
        assert harness.paths.pending_file.stat().st_mtime_ns == r3_mtime
        assert delete_calls == 0
        assert queued_paths == []
        if failure_mode == "precommit":
            expected_predecessor = response_candidate if boundary == "idle" else idle_candidate
            assert final_request == expected_predecessor
        else:
            assert failure_mode == "drift"
            assert final_request == actual_drift
            assert final_request not in {
                response_candidate,
                idle_candidate,
                terminal_candidate,
            }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_mode",
    ["precommit", "aftercommit", "returned_forgery", "returned_noop"],
)
async def test_terminal_journal_delete_failure_rereads_once_without_retry(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    command = _command(harness, timeout=0.3)
    expected = _completed_artifact(command).accepted_response.settlement
    assert type(expected) is CompletedSettlement
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected.result))
    response_path = harness.paths.response_path(command.request_id)
    queued_paths: list[Path] = []
    injected = (
        None
        if failure_mode in {"returned_forgery", "returned_noop"}
        else RuntimeError(f"terminal journal delete {failure_mode}")
    )
    converges = failure_mode in {"aftercommit", "returned_forgery"}

    original_delete = harness.journals.delete
    original_load = harness.journals.load
    original_save = harness.journals.save
    original_get_request = harness.ledger.get_request
    original_transition = harness.ledger.transition
    original_classify = mutation_module._classify_exchange_error  # pyright: ignore[reportPrivateUsage]
    delete_calls = 0
    post_fault_journal_loads: list[PendingJournal | None] = []
    post_fault_request_reads = 0
    post_fault_transitions = 0
    post_fault_saves = 0
    idle_journal_saves = 0
    fault_crossed = False
    intended_r3: PendingJournal | None = None
    terminal_candidate: RequestRecord | None = None
    response_artifact: ResponseArtifact | None = None
    r3_bytes = b""
    r3_mtime = 0
    response_bytes = b""
    response_mtime = 0
    classified_errors: list[tuple[BaseException, BaseException]] = []

    def traced_queue(path: Path) -> None:
        assert converges
        assert path == response_path
        assert len(post_fault_journal_loads) == 1
        assert post_fault_journal_loads[0] is None
        assert delete_calls == 1
        assert original_get_request(command.request_id) == terminal_candidate
        queued_paths.append(path)

    exchange = _exchange(harness, queue_response_cleanup=traced_queue)

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal idle_journal_saves, post_fault_saves
        if fault_crossed:
            post_fault_saves += 1
        if journal.original.state is PendingPhase.IDLE_PUBLISHED:
            idle_journal_saves += 1
        return original_save(journal, expected_revisions=expected_revisions)

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal post_fault_transitions
        if fault_crossed:
            post_fault_transitions += 1
        return original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )

    def traced_get_request(request_id: UUID) -> RequestRecord | None:
        nonlocal post_fault_request_reads
        if fault_crossed:
            post_fault_request_reads += 1
        return original_get_request(request_id)

    def traced_load() -> object:
        loaded = original_load()
        if fault_crossed:
            post_fault_journal_loads.append(None if loaded is None else loaded.journal)
        return loaded

    def failing_delete(expected_delete: JournalDeleteExpectation) -> object:
        nonlocal delete_calls, fault_crossed, intended_r3, terminal_candidate
        nonlocal response_artifact, r3_bytes, r3_mtime, response_bytes, response_mtime
        delete_calls += 1
        assert delete_calls == 1, "failed terminal journal delete was retried"
        state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
        assert state is not None
        intended_r3 = state.idle_published_journal
        terminal_candidate = state.terminal_request
        response_artifact = state.response_artifact
        assert intended_r3 is not None
        assert state.journal is intended_r3
        assert terminal_candidate is not None
        assert original_get_request(command.request_id) == terminal_candidate
        assert response_artifact is not None
        assert state.journal_delete_entered is True
        assert state.journal_delete_returned is False
        r3_bytes = harness.paths.pending_file.read_bytes()
        r3_mtime = harness.paths.pending_file.stat().st_mtime_ns
        response_bytes = response_path.read_bytes()
        response_mtime = response_path.stat().st_mtime_ns
        if failure_mode == "precommit":
            assert injected is not None
            fault_crossed = True
            raise injected
        if failure_mode == "returned_noop":
            assert injected is None
            fault_crossed = True
            return None
        original_delete(expected_delete)
        fault_crossed = True
        if failure_mode == "aftercommit":
            assert injected is not None
            raise injected
        assert failure_mode == "returned_forgery"
        return object()

    def traced_classify(error: BaseException) -> BaseException:
        primary = original_classify(error)
        classified_errors.append((error, primary))
        return primary

    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(harness.journals, "save", traced_save)
    monkeypatch.setattr(harness.journals, "load", traced_load)
    monkeypatch.setattr(harness.journals, "delete", failing_delete)
    monkeypatch.setattr(harness.ledger, "get_request", traced_get_request)
    monkeypatch.setattr(harness.ledger, "transition", traced_transition)
    monkeypatch.setattr(mutation_module, "_classify_exchange_error", traced_classify)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    loaded = None
    state = None
    async with peer:
        async with harness.lock:
            with pytest.raises(BridgeError) as caught:
                await exchange.run(command)
            state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            loaded = original_load()

    assert caught is not None
    assert state is not None
    assert delete_calls == 1
    assert len(post_fault_journal_loads) == 1
    assert post_fault_request_reads == 0
    assert post_fault_transitions == 0
    assert post_fault_saves == 0
    assert idle_journal_saves == 1
    assert len(classified_errors) == 1
    classified_input, classified_primary = classified_errors[0]
    assert caught.value is classified_primary
    assert intended_r3 is not None
    assert terminal_candidate is not None
    assert response_artifact is not None
    assert state.idle_published_journal is intended_r3
    assert state.terminal_request is terminal_candidate
    assert state.response_artifact is response_artifact
    assert original_get_request(command.request_id) == terminal_candidate
    assert state.terminal_safety_reread_entered is False
    assert state.terminal_safety_reread_returned is False
    assert state.terminal_safety_observed_request is None
    assert response_path.exists()
    assert response_path.read_bytes() == response_bytes
    assert response_path.stat().st_mtime_ns == response_mtime

    if failure_mode in {"returned_forgery", "returned_noop"}:
        assert injected is None
        assert classified_input is caught.value
        assert caught.value.code is ErrorCode.STATE_CONFLICT
        assert caught.value.message == (
            "terminal mutation journal delete returned drift"
            if failure_mode == "returned_forgery"
            else "terminal mutation journal delete postcondition failed"
        )
        assert caught.value.__cause__ is None
    else:
        assert injected is not None
        assert classified_input is injected
        assert caught.value.to_payload() == {
            "code": ErrorCode.STATE_CONFLICT.value,
            "message": "file-bridge mutation exchange failed",
            "details": {"type": "RuntimeError"},
        }
        assert caught.value.__cause__ is injected
    assert tuple(getattr(caught.value, "__notes__", ())) == ()

    assert state.journal_delete_entered is True
    assert state.journal_delete_returned is (failure_mode in {"returned_forgery", "returned_noop"})
    assert state.safety_finished is True
    assert exchange._requires_fresh_session() is (not converges)  # pyright: ignore[reportPrivateUsage]
    if converges:
        assert loaded is None
        assert post_fault_journal_loads == [None]
        assert queued_paths == [response_path]
        assert state.terminal_continuation_finished is True
    else:
        assert loaded is not None
        assert loaded.journal == intended_r3
        assert post_fault_journal_loads[0] == intended_r3
        assert harness.paths.pending_file.read_bytes() == r3_bytes
        assert harness.paths.pending_file.stat().st_mtime_ns == r3_mtime
        assert queued_paths == []
        assert state.terminal_continuation_finished is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_mode",
    ["precommit", "aftercommit", "drift", "returned_forgery"],
)
async def test_response_accepted_journal_save_failure_rereads_once_without_resave(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    command = _command(harness, timeout=0.3)
    expected = _completed_artifact(command).accepted_response.settlement
    assert type(expected) is CompletedSettlement
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected.result))
    response_path = harness.paths.response_path(command.request_id)
    queued_paths: list[Path] = []
    injected = (
        None
        if failure_mode == "returned_forgery"
        else RuntimeError(f"response-accepted journal save {failure_mode}")
    )
    converges = failure_mode in {"aftercommit", "returned_forgery"}

    original_save = harness.journals.save
    original_load = harness.journals.load
    original_delete = harness.journals.delete
    original_record_response = harness.ledger.record_response
    original_get_request = harness.ledger.get_request
    original_get_delivery = harness.ledger.get_delivery
    original_transition = harness.ledger.transition
    original_publish_idle = harness.inbox.publish_idle
    original_classify = mutation_module._classify_exchange_error  # pyright: ignore[reportPrivateUsage]
    response_save_calls = 0
    idle_save_calls = 0
    delete_calls = 0
    record_response_calls = 0
    response_accepted_transitions = 0
    idle_transitions = 0
    terminal_transitions = 0
    idle_publish_calls = 0
    post_fault_request_reads = 0
    post_fault_request_observations: list[RequestRecord | None] = []
    fault_crossed = False
    r1_journal: PendingJournal | None = None
    intended_r2: PendingJournal | None = None
    actual_drift: PendingJournal | None = None
    intended_r3: PendingJournal | None = None
    published_delivery_candidate: DeliveryRecord | None = None
    published_request_candidate: RequestRecord | None = None
    response_delivery: DeliveryRecord | None = None
    response_request: RequestRecord | None = None
    response_artifact: ResponseArtifact | None = None
    r1_bytes = b""
    r1_mtime = 0
    drift_bytes = b""
    drift_mtime = 0
    response_bytes = b""
    response_mtime = 0
    post_fault_journal_loads: list[PendingJournal | None] = []
    classified_errors: list[tuple[BaseException, BaseException]] = []

    def traced_queue(path: Path) -> None:
        assert converges
        assert path == response_path
        assert delete_calls == 1
        assert len(post_fault_journal_loads) == 1
        queued_paths.append(path)

    exchange = _exchange(harness, queue_response_cleanup=traced_queue)

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal response_save_calls, idle_save_calls, fault_crossed
        nonlocal r1_journal, intended_r2, actual_drift, intended_r3
        nonlocal published_delivery_candidate, published_request_candidate
        nonlocal response_delivery, response_request, response_artifact
        nonlocal r1_bytes, r1_mtime, drift_bytes, drift_mtime
        nonlocal response_bytes, response_mtime
        phase = journal.original.state
        if phase is PendingPhase.PUBLISHED:
            revisions = original_save(journal, expected_revisions=expected_revisions)
            r1_journal = journal
            r1_bytes = harness.paths.pending_file.read_bytes()
            r1_mtime = harness.paths.pending_file.stat().st_mtime_ns
            return revisions
        if phase is PendingPhase.IDLE_PUBLISHED:
            idle_save_calls += 1
            intended_r3 = journal
            return original_save(journal, expected_revisions=expected_revisions)
        if phase is not PendingPhase.RESPONSE_ACCEPTED:
            return original_save(journal, expected_revisions=expected_revisions)

        response_save_calls += 1
        assert response_save_calls == 1, "response-accepted journal save was retried"
        assert expected_revisions == JournalRevisions(original=1, reconcile_attempt=None)
        intended_r2 = journal
        state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
        assert state is not None
        published_delivery_candidate = state.published_delivery
        published_request_candidate = state.published_request
        response_delivery = state.response_delivery
        response_request = state.response_accepted_request
        response_artifact = state.response_artifact
        assert state.response_accepted_journal is intended_r2
        assert published_delivery_candidate is not None
        assert published_request_candidate is not None
        assert response_artifact is not None
        published_request = original_get_request(command.request_id)
        published_delivery = original_get_delivery(DELIVERY_ID)
        assert published_request is not None
        assert published_request.state is HostRequestState.PUBLISHED
        assert published_delivery is not None
        assert published_delivery.response_artifact is None
        response_bytes = response_path.read_bytes()
        response_mtime = response_path.stat().st_mtime_ns
        if failure_mode == "precommit":
            assert injected is not None
            fault_crossed = True
            raise injected
        if failure_mode == "drift":
            assert injected is not None
            actual_drift = _journal_with_original(
                journal,
                updated_at_ms=journal.original.updated_at_ms + 1,
            )
            revisions = original_save(actual_drift, expected_revisions=expected_revisions)
            assert revisions == JournalRevisions(original=2, reconcile_attempt=None)
            drift_bytes = harness.paths.pending_file.read_bytes()
            drift_mtime = harness.paths.pending_file.stat().st_mtime_ns
            fault_crossed = True
            raise injected
        revisions = original_save(journal, expected_revisions=expected_revisions)
        assert revisions == JournalRevisions(original=2, reconcile_attempt=None)
        fault_crossed = True
        if failure_mode == "aftercommit":
            assert injected is not None
            raise injected
        assert failure_mode == "returned_forgery"
        return JournalRevisions(original=3, reconcile_attempt=None)

    def traced_load() -> object:
        loaded = original_load()
        if fault_crossed and idle_save_calls == 0 and delete_calls == 0:
            post_fault_journal_loads.append(None if loaded is None else loaded.journal)
        return loaded

    def traced_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        nonlocal record_response_calls
        assert intended_r2 is not None
        assert post_fault_journal_loads == [intended_r2]
        record_response_calls += 1
        assert artifact is response_artifact
        return original_record_response(artifact)

    def traced_get_request(request_id: UUID) -> RequestRecord | None:
        nonlocal post_fault_request_reads
        observed = original_get_request(request_id)
        if fault_crossed:
            post_fault_request_reads += 1
            post_fault_request_observations.append(observed)
        return observed

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal response_accepted_transitions, idle_transitions, terminal_transitions
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            response_accepted_transitions += 1
        elif new_state is HostRequestState.IDLE_PUBLISHED:
            idle_transitions += 1
        elif new_state in {HostRequestState.COMPLETED, HostRequestState.REJECTED}:
            terminal_transitions += 1
        return original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )

    def traced_delete(expected_delete: JournalDeleteExpectation) -> None:
        nonlocal delete_calls
        delete_calls += 1
        original_delete(expected_delete)

    def traced_publish_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_publish_calls
        assert publisher is harness.inbox
        idle_publish_calls += 1
        original_publish_idle()

    def traced_classify(error: BaseException) -> BaseException:
        primary = original_classify(error)
        classified_errors.append((error, primary))
        return primary

    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(harness.journals, "save", traced_save)
    monkeypatch.setattr(harness.journals, "load", traced_load)
    monkeypatch.setattr(harness.journals, "delete", traced_delete)
    monkeypatch.setattr(harness.ledger, "record_response", traced_record_response)
    monkeypatch.setattr(harness.ledger, "get_request", traced_get_request)
    monkeypatch.setattr(harness.ledger, "transition", traced_transition)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_publish_idle)
    monkeypatch.setattr(mutation_module, "_classify_exchange_error", traced_classify)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    loaded = None
    state = None
    async with peer:
        async with harness.lock:
            with pytest.raises(BridgeError) as caught:
                await exchange.run(command)
            state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            loaded = original_load()

    assert caught is not None
    assert state is not None
    assert intended_r2 is not None
    assert published_delivery_candidate is not None
    assert published_request_candidate is not None
    assert response_delivery is not None
    assert response_request is not None
    assert response_artifact is not None
    assert response_save_calls == 1
    assert len(post_fault_journal_loads) == 1
    assert len(classified_errors) == 1
    classified_input, classified_primary = classified_errors[0]
    assert caught.value is classified_primary
    assert state.response_accepted_journal is intended_r2
    assert state.response_delivery is response_delivery
    assert state.response_accepted_request is response_request
    assert state.response_artifact is response_artifact
    delivery_tree = published_delivery_candidate.model_dump(
        mode="python",
        round_trip=True,
        warnings=False,
    )
    delivery_tree.update(
        response_artifact=response_artifact,
        settlement=response_artifact.accepted_response.settlement,
    )
    assert response_delivery == DeliveryRecord.model_validate(delivery_tree)
    request_tree = published_request_candidate.model_dump(
        mode="python",
        round_trip=True,
        warnings=False,
    )
    request_tree.update(
        state=HostRequestState.RESPONSE_ACCEPTED,
        updated_at_ms=intended_r2.original.updated_at_ms,
    )
    assert response_request == RequestRecord.model_validate(request_tree)
    assert response_path.exists()
    assert response_path.read_bytes() == response_bytes
    assert response_path.stat().st_mtime_ns == response_mtime

    if failure_mode == "returned_forgery":
        assert injected is None
        assert classified_input is caught.value
        assert caught.value.code is ErrorCode.STATE_CONFLICT
        assert caught.value.message == "response-accepted mutation journal save returned drift"
        assert caught.value.__cause__ is None
    else:
        assert injected is not None
        assert classified_input is injected
        assert caught.value.to_payload() == {
            "code": ErrorCode.STATE_CONFLICT.value,
            "message": "file-bridge mutation exchange failed",
            "details": {"type": "RuntimeError"},
        }
        assert caught.value.__cause__ is injected
    notes_value = cast(list[str] | None, getattr(caught.value, "__notes__", None))
    notes = () if notes_value is None else tuple(notes_value)
    if failure_mode == "drift":
        assert notes == (
            "artifact safety failure: BridgeError: "
            "response-accepted mutation journal evidence drifted",
        )
    else:
        assert notes == ()

    assert getattr(state, "response_journal_save_entered") is True
    assert getattr(state, "response_journal_save_returned") is (failure_mode == "returned_forgery")
    assert getattr(state, "response_journal_safety_reread_entered") is True
    assert getattr(state, "response_journal_safety_reread_returned") is True
    expected_observed = (
        r1_journal
        if failure_mode == "precommit"
        else actual_drift
        if failure_mode == "drift"
        else intended_r2
    )
    assert expected_observed is not None
    assert post_fault_journal_loads == [expected_observed]
    assert getattr(state, "response_journal_safety_observed_journal") == expected_observed
    assert state.safety_finished is (failure_mode != "drift")
    assert exchange._requires_fresh_session() is (not converges)  # pyright: ignore[reportPrivateUsage]
    assert post_fault_request_reads == (1 if converges else 0)
    assert post_fault_request_observations == ([response_request] if converges else [])
    assert state.response_request_safety_reread_entered is converges
    assert state.response_request_safety_reread_returned is converges
    assert state.response_request_safety_observed_request == (
        response_request if converges else None
    )
    assert state.response_acceptance_continuation_entered is converges
    if converges:
        assert loaded is None
        assert record_response_calls == 1
        assert response_accepted_transitions == 1
        assert idle_publish_calls == 1
        assert idle_save_calls == 1
        assert idle_transitions == 1
        assert terminal_transitions == 1
        assert delete_calls == 1
        assert queued_paths == [response_path]
        assert intended_r3 is not None
        assert state.journal is intended_r3
        final_request = original_get_request(command.request_id)
        final_delivery = original_get_delivery(DELIVERY_ID)
        assert final_request is not None and final_request.state is HostRequestState.COMPLETED
        assert final_delivery is not None
        assert final_delivery.response_artifact == response_artifact
    else:
        assert loaded is not None
        assert state.journal == expected_observed
        assert record_response_calls == 0
        assert response_accepted_transitions == 0
        assert idle_publish_calls == 0
        assert idle_save_calls == 0
        assert idle_transitions == 0
        assert terminal_transitions == 0
        assert delete_calls == 0
        assert queued_paths == []
        final_request = original_get_request(command.request_id)
        final_delivery = original_get_delivery(DELIVERY_ID)
        assert final_request is not None and final_request.state is HostRequestState.PUBLISHED
        assert final_delivery is not None and final_delivery.response_artifact is None
        if failure_mode == "precommit":
            assert r1_journal is not None
            assert loaded.journal == r1_journal
            assert harness.paths.pending_file.read_bytes() == r1_bytes
            assert harness.paths.pending_file.stat().st_mtime_ns == r1_mtime
        else:
            assert failure_mode == "drift"
            assert actual_drift is not None
            assert loaded.journal == actual_drift
            assert harness.paths.pending_file.read_bytes() == drift_bytes
            assert harness.paths.pending_file.stat().st_mtime_ns == drift_mtime


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_mode",
    [
        "precommit",
        "aftercommit",
        "drift",
        "returned_forgery",
        "returned_noop",
        "partial",
    ],
)
async def test_response_record_failure_rereads_delivery_once_without_retry(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    command = _command(harness, timeout=0.3)
    expected = _completed_artifact(command).accepted_response.settlement
    assert type(expected) is CompletedSettlement
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected.result))
    response_path = harness.paths.response_path(command.request_id)
    queued_paths: list[Path] = []
    injected = (
        None
        if failure_mode in {"returned_forgery", "returned_noop"}
        else RuntimeError(f"response record {failure_mode}")
    )
    converges = failure_mode in {"aftercommit", "returned_forgery"}

    original_record_response = harness.ledger.record_response
    original_get_delivery = harness.ledger.get_delivery
    original_get_request = harness.ledger.get_request
    original_transition = harness.ledger.transition
    original_save = harness.journals.save
    original_load = harness.journals.load
    original_delete = harness.journals.delete
    original_publish_idle = harness.inbox.publish_idle
    original_classify = mutation_module._classify_exchange_error  # pyright: ignore[reportPrivateUsage]
    record_calls = 0
    post_fault_delivery_read_entries = 0
    post_fault_delivery_read_returns = 0
    post_fault_request_reads = 0
    post_fault_request_observations: list[RequestRecord | None] = []
    post_fault_transitions = 0
    post_fault_saves = 0
    post_fault_journal_loads = 0
    delete_calls = 0
    idle_publish_calls = 0
    fault_crossed = False
    intended_r2: PendingJournal | None = None
    intended_r3: PendingJournal | None = None
    published_delivery: DeliveryRecord | None = None
    response_delivery: DeliveryRecord | None = None
    response_request: RequestRecord | None = None
    response_artifact: ResponseArtifact | None = None
    actual_drift_delivery: DeliveryRecord | None = None
    post_fault_observations: list[DeliveryRecord | None] = []
    classified_errors: list[tuple[BaseException, BaseException]] = []
    r2_bytes = b""
    r2_mtime = 0
    response_bytes = b""
    response_mtime = 0

    def raw_response_columns() -> tuple[object, object, object, object, object]:
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            row = connection.execute(
                """
                SELECT response_at_ms,response_sha256,response_size_bytes,
                       accepted_response_json,settlement_json
                FROM deliveries WHERE delivery_id=?
                """,
                (str(DELIVERY_ID),),
            ).fetchone()
        assert row is not None
        return cast(tuple[object, object, object, object, object], tuple(row))

    def traced_queue(path: Path) -> None:
        assert converges
        assert path == response_path
        assert delete_calls == 1
        assert post_fault_observations == [response_delivery]
        queued_paths.append(path)

    exchange = _exchange(harness, queue_response_cleanup=traced_queue)

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal post_fault_saves, intended_r2, intended_r3
        if fault_crossed:
            post_fault_saves += 1
        if journal.original.state is PendingPhase.RESPONSE_ACCEPTED:
            intended_r2 = journal
        elif journal.original.state is PendingPhase.IDLE_PUBLISHED:
            intended_r3 = journal
        return original_save(journal, expected_revisions=expected_revisions)

    def failing_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        nonlocal record_calls, fault_crossed, intended_r2, published_delivery
        nonlocal response_delivery, response_request, response_artifact
        nonlocal actual_drift_delivery, r2_bytes, r2_mtime
        nonlocal response_bytes, response_mtime
        record_calls += 1
        assert record_calls == 1, "failed response record was retried"
        state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
        assert state is not None
        intended_r2 = state.response_accepted_journal
        published_delivery = state.published_delivery
        response_delivery = state.response_delivery
        response_request = state.response_accepted_request
        response_artifact = state.response_artifact
        assert intended_r2 is not None
        assert state.journal is intended_r2
        assert published_delivery is not None
        assert response_delivery is not None
        assert response_request is not None
        assert response_artifact is artifact
        assert raw_response_columns() == (None, None, None, None, None)
        r2_bytes = harness.paths.pending_file.read_bytes()
        r2_mtime = harness.paths.pending_file.stat().st_mtime_ns
        response_bytes = response_path.read_bytes()
        response_mtime = response_path.stat().st_mtime_ns
        if failure_mode == "precommit":
            assert injected is not None
            fault_crossed = True
            raise injected
        if failure_mode == "returned_noop":
            assert injected is None
            fault_crossed = True
            return response_delivery
        if failure_mode == "partial":
            assert injected is not None
            with sqlite3.connect(harness.paths.sqlite_file) as connection:
                connection.execute("PRAGMA ignore_check_constraints=ON")
                updated = connection.execute(
                    "UPDATE deliveries SET response_at_ms=? WHERE delivery_id=?",
                    (artifact.accepted_at_ms, str(DELIVERY_ID)),
                )
                assert updated.rowcount == 1
                connection.commit()
            fault_crossed = True
            raise injected
        if failure_mode == "drift":
            assert injected is not None
            tree = artifact.model_dump(mode="python", round_trip=True, warnings=False)
            tree["accepted_at_ms"] = artifact.accepted_at_ms + 1
            drift_artifact = ResponseArtifact.model_validate(tree)
            actual_drift_delivery = original_record_response(drift_artifact)
            assert actual_drift_delivery != response_delivery
            fault_crossed = True
            raise injected
        committed = original_record_response(artifact)
        assert committed == response_delivery
        fault_crossed = True
        if failure_mode == "aftercommit":
            assert injected is not None
            raise injected
        assert failure_mode == "returned_forgery"
        return published_delivery

    def traced_get_delivery(delivery_id: UUID) -> DeliveryRecord | None:
        nonlocal post_fault_delivery_read_entries, post_fault_delivery_read_returns
        if fault_crossed:
            post_fault_delivery_read_entries += 1
        observed = original_get_delivery(delivery_id)
        if fault_crossed:
            post_fault_delivery_read_returns += 1
            post_fault_observations.append(observed)
        return observed

    def traced_get_request(request_id: UUID) -> RequestRecord | None:
        nonlocal post_fault_request_reads
        observed = original_get_request(request_id)
        if fault_crossed:
            post_fault_request_reads += 1
            post_fault_request_observations.append(observed)
        return observed

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal post_fault_transitions
        if fault_crossed:
            post_fault_transitions += 1
            if new_state is HostRequestState.RESPONSE_ACCEPTED:
                assert post_fault_observations == [response_delivery]
        return original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )

    def traced_load() -> object:
        nonlocal post_fault_journal_loads
        if fault_crossed:
            post_fault_journal_loads += 1
        return original_load()

    def traced_delete(expected_delete: JournalDeleteExpectation) -> None:
        nonlocal delete_calls
        delete_calls += 1
        original_delete(expected_delete)

    def traced_publish_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_publish_calls
        assert publisher is harness.inbox
        idle_publish_calls += 1
        original_publish_idle()

    def traced_classify(error: BaseException) -> BaseException:
        primary = original_classify(error)
        classified_errors.append((error, primary))
        return primary

    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(harness.ledger, "record_response", failing_record_response)
    monkeypatch.setattr(harness.ledger, "get_delivery", traced_get_delivery)
    monkeypatch.setattr(harness.ledger, "get_request", traced_get_request)
    monkeypatch.setattr(harness.ledger, "transition", traced_transition)
    monkeypatch.setattr(harness.journals, "save", traced_save)
    monkeypatch.setattr(harness.journals, "load", traced_load)
    monkeypatch.setattr(harness.journals, "delete", traced_delete)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_publish_idle)
    monkeypatch.setattr(mutation_module, "_classify_exchange_error", traced_classify)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    loaded = None
    state = None
    async with peer:
        async with harness.lock:
            with pytest.raises(BridgeError) as caught:
                await exchange.run(command)
            state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            loaded = original_load()

    assert caught is not None
    assert state is not None
    assert intended_r2 is not None
    assert published_delivery is not None
    assert response_delivery is not None
    assert response_request is not None
    assert response_artifact is not None
    assert record_calls == 1
    assert post_fault_delivery_read_entries == 1
    assert len(classified_errors) == 1
    classified_input, classified_primary = classified_errors[0]
    assert caught.value is classified_primary
    assert state.response_accepted_journal is intended_r2
    assert state.response_delivery is response_delivery
    assert state.response_accepted_request is response_request
    assert state.response_artifact is response_artifact
    assert response_path.exists()
    assert response_path.read_bytes() == response_bytes
    assert response_path.stat().st_mtime_ns == response_mtime

    if failure_mode in {"returned_forgery", "returned_noop"}:
        assert injected is None
        assert classified_input is caught.value
        assert caught.value.code is ErrorCode.STATE_CONFLICT
        assert caught.value.message == (
            "recorded mutation response delivery returned drift"
            if failure_mode == "returned_forgery"
            else "recorded mutation response delivery postcondition failed"
        )
        assert caught.value.__cause__ is None
    else:
        assert injected is not None
        assert classified_input is injected
        assert caught.value.to_payload() == {
            "code": ErrorCode.STATE_CONFLICT.value,
            "message": "file-bridge mutation exchange failed",
            "details": {"type": "RuntimeError"},
        }
        assert caught.value.__cause__ is injected
    notes_value = cast(list[str] | None, getattr(caught.value, "__notes__", None))
    notes = () if notes_value is None else tuple(notes_value)
    if failure_mode == "drift":
        assert notes == (
            "artifact safety failure: BridgeError: recorded mutation response evidence drifted",
        )
    elif failure_mode == "partial":
        assert notes == (
            "artifact safety failure: BridgeError: persisted delivery row is malformed",
        )
    else:
        assert notes == ()

    assert state.response_record_entered is True
    assert state.response_record_returned is (failure_mode in {"returned_forgery", "returned_noop"})
    assert getattr(state, "response_record_safety_reread_entered") is True
    if failure_mode == "partial":
        assert post_fault_delivery_read_returns == 0
        assert post_fault_observations == []
        assert getattr(state, "response_record_safety_reread_entered") is True
        assert getattr(state, "response_record_safety_reread_returned") is False
        assert getattr(state, "response_record_safety_observed_delivery") is None
    else:
        expected_observation = (
            published_delivery
            if failure_mode in {"precommit", "returned_noop"}
            else actual_drift_delivery
            if failure_mode == "drift"
            else response_delivery
        )
        assert expected_observation is not None
        assert post_fault_delivery_read_returns == 1
        assert post_fault_observations == [expected_observation]
        assert getattr(state, "response_record_safety_reread_entered") is True
        assert getattr(state, "response_record_safety_reread_returned") is True
        assert getattr(state, "response_record_safety_observed_delivery") == (expected_observation)
    assert state.safety_finished is (failure_mode not in {"drift", "partial"})
    assert exchange._requires_fresh_session() is (not converges)  # pyright: ignore[reportPrivateUsage]
    assert post_fault_request_reads == (1 if converges else 0)
    assert post_fault_request_observations == ([response_request] if converges else [])
    assert state.response_request_safety_reread_entered is converges
    assert state.response_request_safety_reread_returned is converges
    assert state.response_request_safety_observed_request == (
        response_request if converges else None
    )
    assert state.response_acceptance_continuation_entered is converges
    if converges:
        assert loaded is None
        assert post_fault_transitions == 3
        assert post_fault_saves == 1
        assert idle_publish_calls == 1
        assert delete_calls == 1
        assert queued_paths == [response_path]
        assert intended_r3 is not None
        assert state.journal is intended_r3
        final_request = original_get_request(command.request_id)
        final_delivery = original_get_delivery(DELIVERY_ID)
        assert final_request is not None and final_request.state is HostRequestState.COMPLETED
        assert final_delivery == response_delivery
    else:
        assert loaded is not None
        assert loaded.journal == intended_r2
        assert state.journal is intended_r2
        assert post_fault_transitions == 0
        assert post_fault_saves == 0
        assert post_fault_journal_loads == 0
        assert idle_publish_calls == 0
        assert delete_calls == 0
        assert queued_paths == []
        assert harness.paths.pending_file.read_bytes() == r2_bytes
        assert harness.paths.pending_file.stat().st_mtime_ns == r2_mtime
        final_request = original_get_request(command.request_id)
        assert final_request is not None and final_request.state is HostRequestState.PUBLISHED
        if failure_mode == "partial":
            assert raw_response_columns() == (
                response_artifact.accepted_at_ms,
                None,
                None,
                None,
                None,
            )
        else:
            final_delivery = original_get_delivery(DELIVERY_ID)
            assert final_delivery is not None
        if failure_mode in {"precommit", "returned_noop"}:
            assert final_delivery == published_delivery
            assert raw_response_columns() == (None, None, None, None, None)
        elif failure_mode == "drift":
            assert failure_mode == "drift"
            assert actual_drift_delivery is not None
            assert final_delivery == actual_drift_delivery
            assert final_delivery != response_delivery


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_mode",
    ["precommit", "aftercommit", "drift", "returned_forgery", "returned_noop"],
)
async def test_response_accepted_request_transition_failure_rereads_once_without_retry(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    command = _command(harness, timeout=0.3)
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    expected = _completed_artifact(command).accepted_response.settlement
    assert type(expected) is CompletedSettlement
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected.result))
    response_path = harness.paths.response_path(command.request_id)
    queued_paths: list[Path] = []
    injected = (
        None
        if failure_mode in {"returned_forgery", "returned_noop"}
        else RuntimeError(f"response-accepted request transition {failure_mode}")
    )
    converges = failure_mode in {"aftercommit", "returned_forgery"}

    original_transition = harness.ledger.transition
    original_get_request = harness.ledger.get_request
    original_get_delivery = harness.ledger.get_delivery
    original_record_response = harness.ledger.record_response
    original_save = harness.journals.save
    original_load = harness.journals.load
    original_delete = harness.journals.delete
    original_publish_idle = harness.inbox.publish_idle
    original_classify = mutation_module._classify_exchange_error  # pyright: ignore[reportPrivateUsage]
    response_transition_calls = 0
    tail_transition_calls = 0
    post_fault_request_reads = 0
    post_fault_delivery_reads = 0
    post_fault_record_calls = 0
    post_fault_saves = 0
    post_fault_journal_loads = 0
    pre_continuation_journal_loads = 0
    delete_calls = 0
    idle_publish_calls = 0
    recovery_validate_calls = 0
    recovery_dump_calls = 0
    fault_crossed = False
    intended_r2: PendingJournal | None = None
    intended_r3: PendingJournal | None = None
    published_request: RequestRecord | None = None
    response_request: RequestRecord | None = None
    response_delivery: DeliveryRecord | None = None
    response_artifact: ResponseArtifact | None = None
    actual_drift: RequestRecord | None = None
    post_fault_observations: list[RequestRecord | None] = []
    classified_errors: list[tuple[BaseException, BaseException]] = []
    r2_bytes = b""
    r2_mtime = 0
    response_bytes = b""
    response_mtime = 0

    def traced_queue(path: Path) -> None:
        assert converges
        assert path == response_path
        assert post_fault_observations == [response_request]
        assert recovery_validate_calls == recovery_dump_calls == 1
        queued_paths.append(path)

    exchange = _exchange(harness, queue_response_cleanup=traced_queue)

    def failing_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal response_transition_calls, tail_transition_calls, fault_crossed
        nonlocal intended_r2, published_request, response_request
        nonlocal response_delivery, response_artifact, actual_drift
        nonlocal r2_bytes, r2_mtime, response_bytes, response_mtime
        if new_state is not HostRequestState.RESPONSE_ACCEPTED:
            if fault_crossed:
                tail_transition_calls += 1
            return original_transition(
                request_id,
                expected_states=expected_states,
                new_state=new_state,
                updated_at_ms=updated_at_ms,
                terminal_at_ms=terminal_at_ms,
                result_json=result_json,
                error_json=error_json,
                resolution_json=resolution_json,
            )

        response_transition_calls += 1
        assert response_transition_calls == 1, "failed response request transition was retried"
        state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
        assert state is not None
        intended_r2 = state.response_accepted_journal
        published_request = state.published_request
        response_request = state.response_accepted_request
        response_delivery = state.response_delivery
        response_artifact = state.response_artifact
        assert intended_r2 is not None
        assert state.journal is intended_r2
        assert published_request is not None
        assert response_request is not None
        assert response_delivery is not None
        assert response_artifact is not None
        assert original_get_delivery(DELIVERY_ID) == response_delivery
        assert expected_states == frozenset({HostRequestState.PUBLISHED})
        assert updated_at_ms == response_request.updated_at_ms
        r2_bytes = harness.paths.pending_file.read_bytes()
        r2_mtime = harness.paths.pending_file.stat().st_mtime_ns
        response_bytes = response_path.read_bytes()
        response_mtime = response_path.stat().st_mtime_ns
        kwargs: dict[str, object] = {
            "expected_states": expected_states,
            "new_state": new_state,
            "updated_at_ms": updated_at_ms,
            "terminal_at_ms": terminal_at_ms,
            "result_json": result_json,
            "error_json": error_json,
            "resolution_json": resolution_json,
        }
        if failure_mode == "precommit":
            assert injected is not None
            fault_crossed = True
            raise injected
        if failure_mode == "returned_noop":
            assert injected is None
            fault_crossed = True
            return response_request
        if failure_mode == "drift":
            assert injected is not None
            kwargs["updated_at_ms"] = updated_at_ms + 1
            actual_drift = cast(
                RequestRecord,
                cast(Any, original_transition)(request_id, **kwargs),
            )
            fault_crossed = True
            raise injected
        committed = cast(RequestRecord, cast(Any, original_transition)(request_id, **kwargs))
        assert committed == response_request
        fault_crossed = True
        if failure_mode == "aftercommit":
            assert injected is not None
            raise injected
        assert failure_mode == "returned_forgery"
        return published_request

    def traced_get_request(request_id: UUID) -> RequestRecord | None:
        nonlocal post_fault_request_reads
        observed = original_get_request(request_id)
        if fault_crossed:
            post_fault_request_reads += 1
            post_fault_observations.append(observed)
        return observed

    def traced_get_delivery(delivery_id: UUID) -> DeliveryRecord | None:
        nonlocal post_fault_delivery_reads
        if fault_crossed:
            post_fault_delivery_reads += 1
        return original_get_delivery(delivery_id)

    def traced_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        nonlocal post_fault_record_calls
        if fault_crossed:
            post_fault_record_calls += 1
        return original_record_response(artifact)

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal post_fault_saves, intended_r3
        if fault_crossed:
            post_fault_saves += 1
        if journal.original.state is PendingPhase.IDLE_PUBLISHED:
            intended_r3 = journal
        return original_save(journal, expected_revisions=expected_revisions)

    def traced_load() -> object:
        nonlocal post_fault_journal_loads, pre_continuation_journal_loads
        if fault_crossed:
            post_fault_journal_loads += 1
            state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
            if state is None or not state.response_acceptance_continuation_entered:
                pre_continuation_journal_loads += 1
        return original_load()

    def traced_delete(expected_delete: JournalDeleteExpectation) -> None:
        nonlocal delete_calls
        delete_calls += 1
        original_delete(expected_delete)

    def traced_publish_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_publish_calls
        assert publisher is harness.inbox
        idle_publish_calls += 1
        original_publish_idle()

    recovery_delegate = recovery_schema.adapter

    class RecoveryAdapterSpy:
        def validate_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_validate_calls
            assert post_fault_observations == [response_request]
            recovery_validate_calls += 1
            assert recovery_validate_calls == 1
            return cast(Any, recovery_delegate).validate_python(value, *args, **kwargs)

        def dump_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_dump_calls
            recovery_dump_calls += 1
            assert recovery_dump_calls == 1
            return cast(Any, recovery_delegate).dump_python(value, *args, **kwargs)

        def __getattr__(self, name: str) -> object:
            return getattr(recovery_delegate, name)

    recovery_spy = RecoveryAdapterSpy()
    adapter_descriptor = inspect.getattr_static(SchemaBinding, "adapter")
    descriptor_get = getattr(adapter_descriptor, "__get__", None)
    assert callable(descriptor_get)

    def guarded_schema_adapter(binding: SchemaBinding) -> object:
        if binding is recovery_schema:
            return recovery_spy
        return cast(Any, descriptor_get)(binding, SchemaBinding)

    def traced_classify(error: BaseException) -> BaseException:
        primary = original_classify(error)
        classified_errors.append((error, primary))
        return primary

    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(harness.ledger, "transition", failing_transition)
    monkeypatch.setattr(harness.ledger, "get_request", traced_get_request)
    monkeypatch.setattr(harness.ledger, "get_delivery", traced_get_delivery)
    monkeypatch.setattr(harness.ledger, "record_response", traced_record_response)
    monkeypatch.setattr(harness.journals, "save", traced_save)
    monkeypatch.setattr(harness.journals, "load", traced_load)
    monkeypatch.setattr(harness.journals, "delete", traced_delete)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_publish_idle)
    monkeypatch.setattr(
        SchemaBinding,
        "adapter",
        property(guarded_schema_adapter),
        raising=False,
    )
    monkeypatch.setattr(mutation_module, "_classify_exchange_error", traced_classify)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    loaded = None
    state = None
    async with peer:
        async with harness.lock:
            with pytest.raises(BridgeError) as caught:
                await exchange.run(command)
            state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            loaded = original_load()

    assert caught is not None
    assert state is not None
    assert intended_r2 is not None
    assert published_request is not None
    assert response_request is not None
    assert response_delivery is not None
    assert response_artifact is not None
    assert response_transition_calls == 1
    assert post_fault_request_reads == 1
    assert len(post_fault_observations) == 1
    assert len(classified_errors) == 1
    classified_input, classified_primary = classified_errors[0]
    assert caught.value is classified_primary
    assert state.response_accepted_journal is intended_r2
    assert state.response_accepted_request is response_request
    assert state.response_delivery is response_delivery
    assert state.response_artifact is response_artifact
    assert response_path.exists()
    assert response_path.read_bytes() == response_bytes
    assert response_path.stat().st_mtime_ns == response_mtime

    if failure_mode in {"returned_forgery", "returned_noop"}:
        assert injected is None
        assert classified_input is caught.value
        assert caught.value.code is ErrorCode.STATE_CONFLICT
        assert caught.value.message == (
            "response-accepted request transition returned drift"
            if failure_mode == "returned_forgery"
            else "response-accepted request transition postcondition failed"
        )
        assert caught.value.__cause__ is None
    else:
        assert injected is not None
        assert classified_input is injected
        assert caught.value.to_payload() == {
            "code": ErrorCode.STATE_CONFLICT.value,
            "message": "file-bridge mutation exchange failed",
            "details": {"type": "RuntimeError"},
        }
        assert caught.value.__cause__ is injected
    notes_value = cast(list[str] | None, getattr(caught.value, "__notes__", None))
    notes = () if notes_value is None else tuple(notes_value)
    if failure_mode == "drift":
        assert notes == (
            "artifact safety failure: BridgeError: response-accepted request evidence drifted",
        )
    else:
        assert notes == ()

    assert state.response_request_transition_entered is True
    assert state.response_request_transition_returned is (
        failure_mode in {"returned_forgery", "returned_noop"}
    )
    assert getattr(state, "response_request_safety_reread_entered") is True
    assert getattr(state, "response_request_safety_reread_returned") is True
    expected_observation = (
        published_request
        if failure_mode in {"precommit", "returned_noop"}
        else actual_drift
        if failure_mode == "drift"
        else response_request
    )
    assert expected_observation is not None
    assert post_fault_observations == [expected_observation]
    assert getattr(state, "response_request_safety_observed_request") == expected_observation
    assert getattr(state, "response_acceptance_continuation_entered") is converges
    assert state.safety_finished is (failure_mode != "drift")
    assert exchange._requires_fresh_session() is (not converges)  # pyright: ignore[reportPrivateUsage]
    assert post_fault_delivery_reads == 0
    assert post_fault_record_calls == 0
    assert state.response_journal_safety_reread_entered is False
    assert state.response_record_safety_reread_entered is True
    assert state.response_record_safety_observed_delivery == response_delivery
    assert pre_continuation_journal_loads == 0
    if converges:
        assert loaded is None
        assert tail_transition_calls == 2
        assert post_fault_saves == 1
        assert idle_publish_calls == 1
        assert delete_calls == 1
        assert queued_paths == [response_path]
        assert recovery_validate_calls == recovery_dump_calls == 1
        assert intended_r3 is not None
        assert state.journal is intended_r3
        final_request = original_get_request(command.request_id)
        assert final_request is not None and final_request.state is HostRequestState.COMPLETED
    else:
        assert loaded is not None
        assert loaded.journal == intended_r2
        assert state.journal is intended_r2
        assert tail_transition_calls == 0
        assert post_fault_saves == 0
        assert post_fault_journal_loads == 0
        assert idle_publish_calls == 0
        assert delete_calls == 0
        assert queued_paths == []
        assert recovery_validate_calls == recovery_dump_calls == 0
        assert harness.paths.pending_file.read_bytes() == r2_bytes
        assert harness.paths.pending_file.stat().st_mtime_ns == r2_mtime
        final_request = original_get_request(command.request_id)
        assert final_request == expected_observation


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["normal", "aftercommit"])
@pytest.mark.parametrize("observation", ["runtime_error", "missing", "malformed"])
async def test_response_request_observation_edges_fail_closed(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    observation: str,
) -> None:
    command = _command(harness, timeout=0.3)
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    expected = _completed_artifact(command).accepted_response.settlement
    assert type(expected) is CompletedSettlement
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected.result))
    response_path = harness.paths.response_path(command.request_id)
    queued_paths: list[Path] = []
    exchange = _exchange(harness, queue_response_cleanup=queued_paths.append)
    transition_error = RuntimeError("response request transition aftercommit")
    observation_error = RuntimeError("response request observation")

    original_transition = harness.ledger.transition
    original_get_request = harness.ledger.get_request
    original_get_delivery = harness.ledger.get_delivery
    original_load = harness.journals.load
    original_classify = mutation_module._classify_exchange_error  # pyright: ignore[reportPrivateUsage]
    request_transition_calls = 0
    request_observation_calls = 0
    recovery_validate_calls = 0
    recovery_dump_calls = 0
    transition_committed = False
    classified_errors: list[tuple[BaseException, BaseException]] = []
    intended_r2: PendingJournal | None = None
    response_request: RequestRecord | None = None
    response_delivery: DeliveryRecord | None = None
    r2_bytes = b""
    r2_mtime = 0
    response_bytes = b""
    response_mtime = 0
    inbox_bytes = b""
    inbox_mtime = 0
    response_columns: tuple[object, ...] | None = None
    request_columns: tuple[object, ...] | None = None

    def raw_response_columns() -> tuple[object, ...]:
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            row = connection.execute(
                """
                SELECT request_id,delivery_kind,response_at_ms,response_sha256,
                       response_size_bytes,accepted_response_json,settlement_json
                FROM deliveries WHERE delivery_id=?
                """,
                (str(DELIVERY_ID),),
            ).fetchone()
        assert row is not None
        return tuple(row)

    def raw_request_columns() -> tuple[object, ...]:
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            row = connection.execute(
                """
                SELECT state,operation,updated_at_ms,terminal_at_ms,
                       result_json,error_json,resolution_json
                FROM requests WHERE request_id=?
                """,
                (str(command.request_id),),
            ).fetchone()
        assert row is not None
        return tuple(row)

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal request_transition_calls, transition_committed
        nonlocal intended_r2, response_request, response_delivery
        nonlocal r2_bytes, r2_mtime, response_bytes, response_mtime
        nonlocal inbox_bytes, inbox_mtime, response_columns, request_columns
        if new_state is not HostRequestState.RESPONSE_ACCEPTED:
            return original_transition(
                request_id,
                expected_states=expected_states,
                new_state=new_state,
                updated_at_ms=updated_at_ms,
                terminal_at_ms=terminal_at_ms,
                result_json=result_json,
                error_json=error_json,
                resolution_json=resolution_json,
            )

        request_transition_calls += 1
        assert request_transition_calls == 1, "response request transition was retried"
        state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
        assert state is not None
        intended_r2 = state.response_accepted_journal
        response_request = state.response_accepted_request
        response_delivery = state.response_delivery
        assert intended_r2 is not None
        assert state.journal is intended_r2
        assert response_request is not None
        assert response_delivery is not None
        r2_bytes = harness.paths.pending_file.read_bytes()
        r2_mtime = harness.paths.pending_file.stat().st_mtime_ns
        response_bytes = response_path.read_bytes()
        response_mtime = response_path.stat().st_mtime_ns
        inbox_bytes = harness.paths.inbox.read_bytes()
        inbox_mtime = harness.paths.inbox.stat().st_mtime_ns
        observed = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        assert observed == response_request
        assert original_get_delivery(DELIVERY_ID) == response_delivery
        response_columns = raw_response_columns()
        request_columns = raw_request_columns()
        assert request_columns == (
            HostRequestState.RESPONSE_ACCEPTED.value,
            command.invocation.contract.name,
            response_request.updated_at_ms,
            None,
            None,
            None,
            None,
        )
        transition_committed = True
        if observation == "malformed":
            with sqlite3.connect(harness.paths.sqlite_file) as connection:
                updated = connection.execute(
                    "UPDATE requests SET operation='forged' WHERE request_id=?",
                    (str(command.request_id),),
                )
                assert updated.rowcount == 1
                connection.commit()
        if boundary == "aftercommit":
            raise transition_error
        assert boundary == "normal"
        return observed

    def traced_get_request(request_id: UUID) -> RequestRecord | None:
        nonlocal request_observation_calls
        if not transition_committed:
            return original_get_request(request_id)
        request_observation_calls += 1
        assert request_observation_calls == 1, "response request observation was retried"
        if observation == "runtime_error":
            raise observation_error
        if observation == "missing":
            return None
        assert observation == "malformed"
        return original_get_request(request_id)

    recovery_delegate = recovery_schema.adapter

    class RecoveryAdapterSpy:
        def validate_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_validate_calls
            recovery_validate_calls += 1
            return cast(Any, recovery_delegate).validate_python(value, *args, **kwargs)

        def dump_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_dump_calls
            recovery_dump_calls += 1
            return cast(Any, recovery_delegate).dump_python(value, *args, **kwargs)

        def __getattr__(self, name: str) -> object:
            return getattr(recovery_delegate, name)

    recovery_spy = RecoveryAdapterSpy()
    adapter_descriptor = inspect.getattr_static(SchemaBinding, "adapter")
    descriptor_get = getattr(adapter_descriptor, "__get__", None)
    assert callable(descriptor_get)

    def guarded_schema_adapter(binding: SchemaBinding) -> object:
        if binding is recovery_schema:
            return recovery_spy
        return cast(Any, descriptor_get)(binding, SchemaBinding)

    def traced_classify(error: BaseException) -> BaseException:
        primary = original_classify(error)
        classified_errors.append((error, primary))
        return primary

    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(harness.ledger, "transition", traced_transition)
    monkeypatch.setattr(harness.ledger, "get_request", traced_get_request)
    monkeypatch.setattr(
        SchemaBinding,
        "adapter",
        property(guarded_schema_adapter),
        raising=False,
    )
    monkeypatch.setattr(mutation_module, "_classify_exchange_error", traced_classify)

    async with peer:
        async with harness.lock:
            with pytest.raises(BridgeError) as caught:
                await exchange.run(command)
            state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            loaded = original_load()

    assert intended_r2 is not None
    assert response_request is not None
    assert response_delivery is not None
    assert response_columns is not None
    assert request_columns is not None
    assert request_transition_calls == request_observation_calls == 1
    assert state.response_request_transition_entered is True
    assert state.response_request_transition_returned is (boundary == "normal")
    assert state.response_request_safety_reread_entered is True
    assert state.response_request_safety_reread_returned is (observation == "missing")
    assert state.response_request_safety_observed_request is None
    assert state.response_acceptance_continuation_entered is False
    assert state.safety_finished is False
    assert exchange._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]

    assert len(classified_errors) == 1
    classified_input, classified_primary = classified_errors[0]
    assert caught.value is classified_primary
    assert caught.value.code is ErrorCode.STATE_CONFLICT
    if boundary == "aftercommit":
        assert classified_input is transition_error
        assert caught.value.message == "file-bridge mutation exchange failed"
        assert caught.value.details == {"type": "RuntimeError"}
        assert caught.value.__cause__ is transition_error
    elif observation == "runtime_error":
        assert classified_input is observation_error
        assert caught.value.message == "file-bridge mutation exchange failed"
        assert caught.value.details == {"type": "RuntimeError"}
        assert caught.value.__cause__ is observation_error
    else:
        assert classified_input is caught.value
        if observation == "missing":
            assert caught.value.__cause__ is None
        else:
            assert caught.value.__cause__ is not None
        assert caught.value.message == (
            "response-accepted request evidence drifted"
            if observation == "missing"
            else "persisted request row is malformed"
        )
        if observation == "malformed":
            assert str(caught.value.__cause__) == "request operation does not match body"

    note = (
        f"artifact safety failure: RuntimeError: {observation_error}"
        if boundary == "aftercommit" and observation == "runtime_error"
        else "artifact safety failure: BridgeError: persisted request row is malformed"
        if boundary == "aftercommit" and observation == "malformed"
        else "artifact safety failure: BridgeError: response-accepted request evidence drifted"
        if observation == "missing"
        else "artifact safety failure: BridgeError: "
        "response-accepted request observation did not return"
    )
    assert tuple(getattr(caught.value, "__notes__", ())) == (note,)

    assert loaded is not None
    assert loaded.journal == intended_r2
    assert state.journal is intended_r2
    assert harness.paths.pending_file.read_bytes() == r2_bytes
    assert harness.paths.pending_file.stat().st_mtime_ns == r2_mtime
    assert response_path.read_bytes() == response_bytes
    assert response_path.stat().st_mtime_ns == response_mtime
    assert harness.paths.inbox.read_bytes() == inbox_bytes
    assert harness.paths.inbox.stat().st_mtime_ns == inbox_mtime
    assert raw_response_columns() == response_columns
    assert raw_request_columns() == (
        request_columns[0],
        "forged" if observation == "malformed" else request_columns[1],
        *request_columns[2:],
    )
    if observation == "malformed":
        with pytest.raises(BridgeError) as malformed_read:
            original_get_request(command.request_id)
        assert malformed_read.value.message == "persisted request row is malformed"
        assert str(malformed_read.value.__cause__) == "request operation does not match body"
    else:
        assert original_get_request(command.request_id) == response_request

    assert recovery_validate_calls == recovery_dump_calls == 0
    assert state.idle_publication_entered is False
    assert state.idle_journal_save_entered is False
    assert state.terminal_continuation_entered is False
    assert state.idle_request_transition_entered is False
    assert state.terminal_request_transition_entered is False
    assert state.journal_delete_entered is False
    assert state.terminal_cleanup_entered is False
    assert queued_paths == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("boundary", "signal_kind"),
    [
        pytest.param("response_journal", "cancelled", id="response_journal_cancelled"),
        pytest.param("response_record", "cancelled", id="response_record_cancelled"),
        pytest.param("response_request", "cancelled", id="response_request_cancelled"),
        pytest.param("idle_journal", "cancelled", id="idle_journal_cancelled"),
        pytest.param(
            "terminal_transition",
            "cancelled",
            id="terminal_transition_cancelled",
        ),
        pytest.param("journal_delete", "cancelled", id="journal_delete_cancelled"),
        pytest.param(
            "response_record",
            "custom_base_exception",
            id="response_record_custom_base_exception",
        ),
    ],
)
async def test_artifact_aftercommit_base_exception_finishes_safety_before_identity_rethrow(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    signal_kind: str,
) -> None:
    command = _command(harness, timeout=0.3)
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    expected = _completed_artifact(command).accepted_response.settlement
    assert type(expected) is CompletedSettlement
    peer_trace: list[str] = []
    peer = _peer(harness, trace=peer_trace)
    peer.enqueue(Respond(result=expected.result))
    response_path = harness.paths.response_path(command.request_id)
    queued_paths: list[Path] = []
    post_fault_trace: list[str] = []
    post_fault_journals: list[PendingJournal | None] = []
    post_fault_deliveries: list[DeliveryRecord | None] = []
    post_fault_requests: list[RequestRecord | None] = []

    signal: BaseException = (
        asyncio.CancelledError(f"artifact {boundary} aftercommit")
        if signal_kind == "cancelled"
        else _StopAfterPublication("artifact response record aftercommit")
    )
    assert signal_kind in {"cancelled", "custom_base_exception"}

    original_save = harness.journals.save
    original_load = harness.journals.load
    original_delete = harness.journals.delete
    original_record_response = harness.ledger.record_response
    original_get_delivery = harness.ledger.get_delivery
    original_transition = harness.ledger.transition
    original_get_request = harness.ledger.get_request
    original_publish_idle = harness.inbox.publish_idle
    phase_save_calls: dict[PendingPhase, int] = {}
    transition_calls: dict[HostRequestState, int] = {}
    record_calls = 0
    idle_publish_calls = 0
    delete_calls = 0
    queue_calls = 0
    recovery_validate_calls = 0
    recovery_dump_calls = 0
    boundary_calls = 0
    target_committed = False
    fault_crossed = False

    def assert_locked() -> None:
        harness.lock.require_acquired()

    def enter_target() -> None:
        nonlocal boundary_calls
        boundary_calls += 1
        assert boundary_calls == 1, f"{boundary} boundary was retried"

    def cancel_after_commit() -> Never:
        nonlocal target_committed, fault_crossed
        target_committed = True
        fault_crossed = True
        raise signal

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        assert_locked()
        phase = journal.original.state
        phase_save_calls[phase] = phase_save_calls.get(phase, 0) + 1
        is_target = (boundary, phase) in {
            ("response_journal", PendingPhase.RESPONSE_ACCEPTED),
            ("idle_journal", PendingPhase.IDLE_PUBLISHED),
        }
        if is_target:
            enter_target()
        elif fault_crossed:
            post_fault_trace.append(f"journal.save.{phase.value}")
        revisions = original_save(journal, expected_revisions=expected_revisions)
        if is_target:
            expected_revision = 2 if phase is PendingPhase.RESPONSE_ACCEPTED else 3
            assert revisions == JournalRevisions(
                original=expected_revision,
                reconcile_attempt=None,
            )
            durable = original_load()
            assert durable is not None and durable.journal == journal
            cancel_after_commit()
        return revisions

    def traced_load() -> object:
        assert_locked()
        loaded = original_load()
        if fault_crossed:
            post_fault_trace.append("journal.load")
            post_fault_journals.append(None if loaded is None else loaded.journal)
        return loaded

    def traced_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        nonlocal record_calls
        assert_locked()
        record_calls += 1
        is_target = boundary == "response_record"
        if is_target:
            enter_target()
        elif fault_crossed:
            post_fault_trace.append("ledger.record_response")
        observed = original_record_response(artifact)
        state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
        assert state is not None and state.response_delivery is not None
        assert observed == state.response_delivery
        if is_target:
            assert original_get_delivery(DELIVERY_ID) == state.response_delivery
            cancel_after_commit()
        return observed

    def traced_get_delivery(delivery_id: UUID) -> DeliveryRecord | None:
        assert_locked()
        observed = original_get_delivery(delivery_id)
        if fault_crossed:
            post_fault_trace.append("ledger.get_delivery")
            post_fault_deliveries.append(observed)
        return observed

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        assert_locked()
        transition_calls[new_state] = transition_calls.get(new_state, 0) + 1
        is_target = (
            boundary == "response_request" and new_state is HostRequestState.RESPONSE_ACCEPTED
        ) or (boundary == "terminal_transition" and new_state is HostRequestState.COMPLETED)
        if is_target:
            enter_target()
        elif fault_crossed:
            post_fault_trace.append(f"ledger.transition.{new_state.value}")
        observed = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        if is_target:
            state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            candidate = (
                state.response_accepted_request
                if new_state is HostRequestState.RESPONSE_ACCEPTED
                else state.terminal_request
            )
            assert candidate is not None and observed == candidate
            assert original_get_request(command.request_id) == candidate
            cancel_after_commit()
        return observed

    def traced_get_request(request_id: UUID) -> RequestRecord | None:
        assert_locked()
        observed = original_get_request(request_id)
        if fault_crossed:
            post_fault_trace.append("ledger.get_request")
            post_fault_requests.append(observed)
        return observed

    def traced_publish_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_publish_calls
        assert_locked()
        assert publisher is harness.inbox
        idle_publish_calls += 1
        if fault_crossed:
            post_fault_trace.append("inbox.publish_idle")
        original_publish_idle()

    def traced_delete(expected_delete: JournalDeleteExpectation) -> None:
        nonlocal delete_calls
        assert_locked()
        delete_calls += 1
        is_target = boundary == "journal_delete"
        if is_target:
            enter_target()
        elif fault_crossed:
            post_fault_trace.append("journal.delete")
        result = cast(object, original_delete(expected_delete))
        assert result is None
        if is_target:
            assert original_load() is None
            cancel_after_commit()

    def traced_queue(path: Path) -> None:
        nonlocal queue_calls
        assert_locked()
        assert path == response_path
        queue_calls += 1
        if fault_crossed:
            post_fault_trace.append("cleanup.queue")
        queued_paths.append(path)

    exchange = _exchange(harness, queue_response_cleanup=traced_queue)
    recovery_delegate = recovery_schema.adapter

    class RecoveryAdapterSpy:
        def validate_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_validate_calls
            assert_locked()
            recovery_validate_calls += 1
            if fault_crossed:
                post_fault_trace.append("recovery.validate")
            return cast(Any, recovery_delegate).validate_python(value, *args, **kwargs)

        def dump_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_dump_calls
            assert_locked()
            recovery_dump_calls += 1
            if fault_crossed:
                post_fault_trace.append("recovery.dump")
            return cast(Any, recovery_delegate).dump_python(value, *args, **kwargs)

        def __getattr__(self, name: str) -> object:
            return getattr(recovery_delegate, name)

    recovery_spy = RecoveryAdapterSpy()
    adapter_descriptor = inspect.getattr_static(SchemaBinding, "adapter")
    descriptor_get = getattr(adapter_descriptor, "__get__", None)
    assert callable(descriptor_get)

    def guarded_schema_adapter(binding: SchemaBinding) -> object:
        if binding is recovery_schema:
            return recovery_spy
        return cast(Any, descriptor_get)(binding, SchemaBinding)

    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(harness.journals, "save", traced_save)
    monkeypatch.setattr(harness.journals, "load", traced_load)
    monkeypatch.setattr(harness.journals, "delete", traced_delete)
    monkeypatch.setattr(harness.ledger, "record_response", traced_record_response)
    monkeypatch.setattr(harness.ledger, "get_delivery", traced_get_delivery)
    monkeypatch.setattr(harness.ledger, "transition", traced_transition)
    monkeypatch.setattr(harness.ledger, "get_request", traced_get_request)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_publish_idle)
    monkeypatch.setattr(
        SchemaBinding,
        "adapter",
        property(guarded_schema_adapter),
        raising=False,
    )

    caught: pytest.ExceptionInfo[BaseException] | None = None
    state = None
    async with peer:
        async with harness.lock:
            with pytest.raises(type(signal)) as caught:
                await exchange.run(command)
            state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            assert caught.value is signal
            assert caught.value.__cause__ is None
            assert tuple(getattr(caught.value, "__notes__", ())) == ()
            assert target_committed is True
            assert boundary_calls == 1
            assert response_path.exists()

            terminal_tail = (
                "ledger.transition.idle_published",
                "ledger.transition.completed",
                "journal.delete",
                "journal.load",
                "journal.load",
                "cleanup.queue",
            )
            continuation_tail = (
                "recovery.validate",
                "recovery.dump",
                "inbox.publish_idle",
                "journal.save.idle_published",
                "journal.load",
                *terminal_tail,
            )
            expected_post_fault_trace = {
                "response_journal": (
                    "journal.load",
                    "ledger.record_response",
                    "ledger.get_delivery",
                    "ledger.transition.response_accepted",
                    "ledger.get_request",
                    *continuation_tail,
                ),
                "response_record": (
                    "ledger.get_delivery",
                    "ledger.transition.response_accepted",
                    "ledger.get_request",
                    *continuation_tail,
                ),
                "response_request": (
                    "ledger.get_request",
                    *continuation_tail,
                ),
                "idle_journal": (
                    "journal.load",
                    *terminal_tail,
                ),
                "terminal_transition": (
                    "ledger.get_request",
                    "journal.delete",
                    "journal.load",
                    "journal.load",
                    "cleanup.queue",
                ),
                "journal_delete": (
                    "journal.load",
                    "cleanup.queue",
                ),
            }[boundary]
            assert tuple(post_fault_trace) == expected_post_fault_trace

            final_request = original_get_request(command.request_id)
            final_delivery = original_get_delivery(DELIVERY_ID)
            assert original_load() is None
            assert final_request is not None
            assert final_request == state.terminal_request
            assert final_request.state is HostRequestState.COMPLETED
            assert final_delivery is not None
            assert final_delivery == state.response_delivery
            assert final_delivery.response_artifact == state.response_artifact
            assert final_delivery.settlement == expected
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            assert not harness.paths.pending_file.exists()

    assert caught is not None
    assert state is not None
    assert phase_save_calls == {
        PendingPhase.PREPARED: 1,
        PendingPhase.PUBLISHED: 1,
        PendingPhase.RESPONSE_ACCEPTED: 1,
        PendingPhase.IDLE_PUBLISHED: 1,
    }
    assert transition_calls == {
        HostRequestState.PUBLISHED: 1,
        HostRequestState.RESPONSE_ACCEPTED: 1,
        HostRequestState.IDLE_PUBLISHED: 1,
        HostRequestState.COMPLETED: 1,
    }
    assert record_calls == 1
    assert idle_publish_calls == 1
    assert delete_calls == 1
    assert queue_calls == 1
    assert queued_paths == [response_path]
    assert recovery_validate_calls == recovery_dump_calls == 1

    gate_flags = {
        "response_journal": (
            state.response_journal_save_entered,
            state.response_journal_save_returned,
        ),
        "response_record": (
            state.response_record_entered,
            state.response_record_returned,
        ),
        "response_request": (
            state.response_request_transition_entered,
            state.response_request_transition_returned,
        ),
        "idle_journal": (
            state.idle_journal_save_entered,
            state.idle_journal_save_returned,
        ),
        "terminal_transition": (
            state.terminal_request_transition_entered,
            state.terminal_request_transition_returned,
        ),
        "journal_delete": (
            state.journal_delete_entered,
            state.journal_delete_returned,
        ),
    }
    for gate, (entered, returned) in gate_flags.items():
        assert entered is True
        assert returned is (gate != boundary)

    assert state.response_journal_safety_reread_entered is (boundary == "response_journal")
    assert state.response_journal_safety_reread_returned is (boundary == "response_journal")
    assert state.response_record_safety_reread_entered is True
    assert state.response_record_safety_reread_returned is True
    assert state.response_record_safety_observed_delivery == state.response_delivery
    assert state.response_request_safety_reread_entered is True
    assert state.response_request_safety_reread_returned is True
    assert state.response_request_safety_observed_request == state.response_accepted_request
    assert state.response_acceptance_continuation_entered is True
    assert state.terminal_safety_reread_entered is (boundary == "terminal_transition")
    assert state.terminal_safety_reread_returned is (boundary == "terminal_transition")
    assert state.journal_delete_observation_entered is True
    assert state.journal_delete_observation_returned is True
    assert state.journal_delete_observed_journal is None
    assert state.terminal_cleanup_entered is True
    assert state.terminal_cleanup_returned is True
    assert state.terminal_continuation_finished is True
    assert state.safety_finished is True
    assert exchange._requires_fresh_session() is False  # pyright: ignore[reportPrivateUsage]

    r2 = state.response_accepted_journal
    r3 = state.idle_published_journal
    assert r2 is not None and r3 is not None
    expected_journal_observations = {
        "response_journal": [r2, r2, r3, None],
        "response_record": [r2, r3, None],
        "response_request": [r2, r3, None],
        "idle_journal": [r3, r3, None],
        "terminal_transition": [r3, None],
        "journal_delete": [None],
    }[boundary]
    assert post_fault_journals == expected_journal_observations
    assert post_fault_deliveries == (
        [state.response_delivery] if boundary in {"response_journal", "response_record"} else []
    )
    expected_request_observations = {
        "response_journal": [state.response_accepted_request],
        "response_record": [state.response_accepted_request],
        "response_request": [state.response_accepted_request],
        "idle_journal": [],
        "terminal_transition": [state.terminal_request],
        "journal_delete": [],
    }[boundary]
    assert post_fault_requests == expected_request_observations


@pytest.mark.asyncio
@pytest.mark.parametrize("request_failure_mode", ["precommit", "aftercommit"])
async def test_response_record_finalizer_hands_nested_request_failure_to_next_gate(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    request_failure_mode: str,
) -> None:
    command = _command(harness, timeout=0.3)
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    expected = _completed_artifact(command).accepted_response.settlement
    assert type(expected) is CompletedSettlement
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected.result))
    response_path = harness.paths.response_path(command.request_id)
    queued_paths: list[Path] = []
    exchange = _exchange(harness, queue_response_cleanup=queued_paths.append)
    response_record_primary = RuntimeError("nested response record aftercommit primary")
    request_transition_loser = RuntimeError(
        f"nested response request transition {request_failure_mode} loser"
    )

    original_record_response = harness.ledger.record_response
    original_transition = harness.ledger.transition
    original_get_delivery = harness.ledger.get_delivery
    original_get_request = harness.ledger.get_request
    original_load = harness.journals.load
    original_classify = mutation_module._classify_exchange_error  # pyright: ignore[reportPrivateUsage]
    response_record_calls = 0
    request_transition_calls = 0
    tail_transition_calls = 0
    recovery_validate_calls = 0
    recovery_dump_calls = 0
    response_record_fault_crossed = False
    request_transition_fault_crossed = False
    response_delivery_observations: list[DeliveryRecord | None] = []
    response_request_observations: list[RequestRecord | None] = []
    classified_errors: list[tuple[BaseException, BaseException]] = []
    intended_r2: PendingJournal | None = None
    published_request: RequestRecord | None = None
    response_request: RequestRecord | None = None
    response_delivery: DeliveryRecord | None = None
    r2_bytes = b""
    r2_mtime = 0

    def failing_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        nonlocal response_record_calls, response_record_fault_crossed
        nonlocal intended_r2, published_request, response_request, response_delivery
        nonlocal r2_bytes, r2_mtime
        response_record_calls += 1
        assert response_record_calls == 1, "response recording was retried"
        observed = original_record_response(artifact)
        state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
        assert state is not None
        intended_r2 = state.response_accepted_journal
        published_request = state.published_request
        response_request = state.response_accepted_request
        response_delivery = state.response_delivery
        assert intended_r2 is not None
        assert state.journal is intended_r2
        assert published_request is not None
        assert response_request is not None
        assert response_delivery is not None
        assert observed == response_delivery
        r2_bytes = harness.paths.pending_file.read_bytes()
        r2_mtime = harness.paths.pending_file.stat().st_mtime_ns
        response_record_fault_crossed = True
        raise response_record_primary

    def failing_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal request_transition_calls, tail_transition_calls
        nonlocal request_transition_fault_crossed
        if new_state is not HostRequestState.RESPONSE_ACCEPTED:
            if response_record_fault_crossed:
                tail_transition_calls += 1
            return original_transition(
                request_id,
                expected_states=expected_states,
                new_state=new_state,
                updated_at_ms=updated_at_ms,
                terminal_at_ms=terminal_at_ms,
                result_json=result_json,
                error_json=error_json,
                resolution_json=resolution_json,
            )

        request_transition_calls += 1
        assert request_transition_calls == 1, "response request transition was retried"
        assert response_record_fault_crossed
        assert response_delivery_observations == [response_delivery]
        assert response_request is not None
        if request_failure_mode == "aftercommit":
            observed = original_transition(
                request_id,
                expected_states=expected_states,
                new_state=new_state,
                updated_at_ms=updated_at_ms,
                terminal_at_ms=terminal_at_ms,
                result_json=result_json,
                error_json=error_json,
                resolution_json=resolution_json,
            )
            assert observed == response_request
        else:
            assert request_failure_mode == "precommit"
        request_transition_fault_crossed = True
        raise request_transition_loser

    def traced_get_delivery(delivery_id: UUID) -> DeliveryRecord | None:
        observed = original_get_delivery(delivery_id)
        if response_record_fault_crossed:
            response_delivery_observations.append(observed)
        return observed

    def traced_get_request(request_id: UUID) -> RequestRecord | None:
        observed = original_get_request(request_id)
        if request_transition_fault_crossed:
            response_request_observations.append(observed)
        return observed

    recovery_delegate = recovery_schema.adapter

    class RecoveryAdapterSpy:
        def validate_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_validate_calls
            assert response_request_observations == [response_request]
            recovery_validate_calls += 1
            assert recovery_validate_calls == 1
            return cast(Any, recovery_delegate).validate_python(value, *args, **kwargs)

        def dump_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_dump_calls
            recovery_dump_calls += 1
            assert recovery_dump_calls == 1
            return cast(Any, recovery_delegate).dump_python(value, *args, **kwargs)

        def __getattr__(self, name: str) -> object:
            return getattr(recovery_delegate, name)

    recovery_spy = RecoveryAdapterSpy()
    adapter_descriptor = inspect.getattr_static(SchemaBinding, "adapter")
    descriptor_get = getattr(adapter_descriptor, "__get__", None)
    assert callable(descriptor_get)

    def guarded_schema_adapter(binding: SchemaBinding) -> object:
        if binding is recovery_schema:
            return recovery_spy
        return cast(Any, descriptor_get)(binding, SchemaBinding)

    def traced_classify(error: BaseException) -> BaseException:
        primary = original_classify(error)
        classified_errors.append((error, primary))
        return primary

    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(harness.ledger, "record_response", failing_record_response)
    monkeypatch.setattr(harness.ledger, "transition", failing_transition)
    monkeypatch.setattr(harness.ledger, "get_delivery", traced_get_delivery)
    monkeypatch.setattr(harness.ledger, "get_request", traced_get_request)
    monkeypatch.setattr(
        SchemaBinding,
        "adapter",
        property(guarded_schema_adapter),
        raising=False,
    )
    monkeypatch.setattr(mutation_module, "_classify_exchange_error", traced_classify)

    async with peer:
        async with harness.lock:
            with pytest.raises(BridgeError) as caught:
                await exchange.run(command)
            state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            loaded = original_load()

    assert intended_r2 is not None
    assert published_request is not None
    assert response_request is not None
    assert response_delivery is not None
    expected_request_observation = (
        response_request if request_failure_mode == "aftercommit" else published_request
    )
    assert response_record_calls == 1
    assert request_transition_calls == 1
    assert response_delivery_observations == [response_delivery]
    assert response_request_observations == [expected_request_observation]
    assert state.response_record_entered is True
    assert state.response_record_returned is False
    assert state.response_record_safety_reread_entered is True
    assert state.response_record_safety_reread_returned is True
    assert state.response_record_safety_observed_delivery == response_delivery
    assert state.response_request_transition_entered is True
    assert state.response_request_transition_returned is False
    assert state.response_request_safety_reread_entered is True
    assert state.response_request_safety_reread_returned is True
    assert state.response_request_safety_observed_request == expected_request_observation

    assert len(classified_errors) == 1
    classified_input, classified_primary = classified_errors[0]
    assert classified_input is response_record_primary
    assert caught.value is classified_primary
    assert caught.value.__cause__ is response_record_primary
    assert tuple(getattr(caught.value, "__notes__", ())) == (
        f"artifact safety failure: RuntimeError: {request_transition_loser}",
    )

    final_request = original_get_request(command.request_id)
    final_delivery = original_get_delivery(DELIVERY_ID)
    assert final_delivery == response_delivery
    assert state.safety_finished is True
    if request_failure_mode == "aftercommit":
        assert loaded is None
        assert final_request is not None
        assert final_request.state is HostRequestState.COMPLETED
        assert state.response_acceptance_continuation_entered is True
        assert state.terminal_continuation_finished is True
        assert recovery_validate_calls == recovery_dump_calls == 1
        assert tail_transition_calls == 2
        assert queued_paths == [response_path]
        assert exchange._requires_fresh_session() is False  # pyright: ignore[reportPrivateUsage]
    else:
        assert loaded is not None
        assert loaded.journal == intended_r2
        assert state.journal is intended_r2
        assert final_request == published_request
        assert state.response_acceptance_continuation_entered is False
        assert state.terminal_continuation_entered is False
        assert recovery_validate_calls == recovery_dump_calls == 0
        assert tail_transition_calls == 0
        assert queued_paths == []
        assert harness.paths.pending_file.read_bytes() == r2_bytes
        assert harness.paths.pending_file.stat().st_mtime_ns == r2_mtime
        assert exchange._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
async def test_recovery_adapter_runtime_error_after_response_acceptance_is_not_retried(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command(harness, timeout=0.3)
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    expected = _completed_artifact(command).accepted_response.settlement
    assert type(expected) is CompletedSettlement
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected.result))
    response_path = harness.paths.response_path(command.request_id)
    queued_paths: list[Path] = []
    exchange = _exchange(harness, queue_response_cleanup=queued_paths.append)
    injected = RuntimeError("recovery adapter runtime failure")

    original_transition = harness.ledger.transition
    original_get_request = harness.ledger.get_request
    original_get_delivery = harness.ledger.get_delivery
    original_record_response = harness.ledger.record_response
    original_save = harness.journals.save
    original_load = harness.journals.load
    original_delete = harness.journals.delete
    original_publish_idle = harness.inbox.publish_idle
    original_classify = mutation_module._classify_exchange_error  # pyright: ignore[reportPrivateUsage]
    response_transition_calls = 0
    post_transition_request_reads = 0
    post_transition_transitions = 0
    post_transition_journal_loads = 0
    record_calls = 0
    delivery_read_calls = 0
    response_save_calls = 0
    idle_save_calls = 0
    quarantine_save_calls = 0
    delete_calls = 0
    idle_publish_calls = 0
    recovery_validate_calls = 0
    recovery_dump_calls = 0
    response_transition_returned = False
    adapter_entry: tuple[object, int, int] | None = None
    intended_r2: PendingJournal | None = None
    response_delivery: DeliveryRecord | None = None
    response_request: RequestRecord | None = None
    response_artifact: ResponseArtifact | None = None
    r2_bytes = b""
    r2_mtime = 0
    response_bytes = b""
    response_mtime = 0
    classified_errors: list[tuple[BaseException, BaseException]] = []

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal response_transition_calls, response_transition_returned
        nonlocal post_transition_transitions
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            response_transition_calls += 1
            assert response_transition_calls == 1
        elif response_transition_returned:
            post_transition_transitions += 1
        observed = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            response_transition_returned = True
        return observed

    def traced_get_request(request_id: UUID) -> RequestRecord | None:
        nonlocal post_transition_request_reads
        if response_transition_returned:
            post_transition_request_reads += 1
        return original_get_request(request_id)

    def traced_get_delivery(delivery_id: UUID) -> DeliveryRecord | None:
        nonlocal delivery_read_calls
        delivery_read_calls += 1
        return original_get_delivery(delivery_id)

    def traced_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        nonlocal record_calls
        record_calls += 1
        return original_record_response(artifact)

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal response_save_calls, idle_save_calls, quarantine_save_calls, intended_r2
        nonlocal r2_bytes, r2_mtime, response_bytes, response_mtime
        if journal.original.state is PendingPhase.RESPONSE_ACCEPTED:
            response_save_calls += 1
            assert response_save_calls == 1
            intended_r2 = journal
        elif journal.original.state is PendingPhase.IDLE_PUBLISHED:
            idle_save_calls += 1
        elif journal.original.state is PendingPhase.QUARANTINED:
            quarantine_save_calls += 1
        revisions = original_save(journal, expected_revisions=expected_revisions)
        if journal.original.state is PendingPhase.RESPONSE_ACCEPTED:
            durable = original_load()
            assert durable is not None and durable.journal == journal
            r2_bytes = harness.paths.pending_file.read_bytes()
            r2_mtime = harness.paths.pending_file.stat().st_mtime_ns
            response_bytes = response_path.read_bytes()
            response_mtime = response_path.stat().st_mtime_ns
        return revisions

    def traced_load() -> object:
        nonlocal post_transition_journal_loads
        if response_transition_returned:
            post_transition_journal_loads += 1
        return original_load()

    def traced_delete(expected_delete: JournalDeleteExpectation) -> None:
        nonlocal delete_calls
        delete_calls += 1
        original_delete(expected_delete)

    def traced_publish_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_publish_calls
        assert publisher is harness.inbox
        idle_publish_calls += 1
        original_publish_idle()

    recovery_delegate = recovery_schema.adapter

    class RecoveryAdapterSpy:
        def validate_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            del value, args, kwargs
            nonlocal recovery_validate_calls, adapter_entry
            state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            recovery_validate_calls += 1
            adapter_entry = (
                getattr(state, "response_acceptance_continuation_entered", None),
                post_transition_request_reads,
                response_transition_calls,
            )
            raise injected

        def dump_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            del value, args, kwargs
            nonlocal recovery_dump_calls
            recovery_dump_calls += 1
            raise AssertionError("recovery dump ran after validate RuntimeError")

        def __getattr__(self, name: str) -> object:
            return getattr(recovery_delegate, name)

    recovery_spy = RecoveryAdapterSpy()
    adapter_descriptor = inspect.getattr_static(SchemaBinding, "adapter")
    descriptor_get = getattr(adapter_descriptor, "__get__", None)
    assert callable(descriptor_get)

    def guarded_schema_adapter(binding: SchemaBinding) -> object:
        if binding is recovery_schema:
            return recovery_spy
        return cast(Any, descriptor_get)(binding, SchemaBinding)

    def traced_classify(error: BaseException) -> BaseException:
        primary = original_classify(error)
        classified_errors.append((error, primary))
        return primary

    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(harness.ledger, "transition", traced_transition)
    monkeypatch.setattr(harness.ledger, "get_request", traced_get_request)
    monkeypatch.setattr(harness.ledger, "get_delivery", traced_get_delivery)
    monkeypatch.setattr(harness.ledger, "record_response", traced_record_response)
    monkeypatch.setattr(harness.journals, "save", traced_save)
    monkeypatch.setattr(harness.journals, "load", traced_load)
    monkeypatch.setattr(harness.journals, "delete", traced_delete)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_publish_idle)
    monkeypatch.setattr(
        SchemaBinding,
        "adapter",
        property(guarded_schema_adapter),
        raising=False,
    )
    monkeypatch.setattr(mutation_module, "_classify_exchange_error", traced_classify)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    state = None
    async with peer:
        async with harness.lock:
            with pytest.raises(BridgeError) as caught:
                await exchange.run(command)
            state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            response_delivery = state.response_delivery
            response_request = state.response_accepted_request
            response_artifact = state.response_artifact
            assert intended_r2 is not None
            assert state.response_accepted_journal is intended_r2
            assert response_delivery is not None
            assert response_request is not None
            assert response_artifact is not None
            durable = original_load()
            assert durable is not None and durable.journal == intended_r2

    assert caught is not None
    assert state is not None
    assert adapter_entry == (True, 1, 1)
    assert recovery_validate_calls == 1
    assert recovery_dump_calls == 0
    assert response_transition_calls == 1
    assert post_transition_request_reads == 1
    assert post_transition_transitions == 0
    assert post_transition_journal_loads == 0
    assert record_calls == 1
    assert delivery_read_calls == 1
    assert response_save_calls == 1
    assert idle_save_calls == 0
    assert quarantine_save_calls == 0
    assert delete_calls == 0
    assert idle_publish_calls == 0
    assert queued_paths == []
    assert len(classified_errors) == 1
    classified_input, classified_primary = classified_errors[0]
    assert classified_input is injected
    assert caught.value is classified_primary
    assert caught.value.to_payload() == {
        "code": ErrorCode.STATE_CONFLICT.value,
        "message": "file-bridge mutation exchange failed",
        "details": {"type": "RuntimeError"},
    }
    assert caught.value.__cause__ is injected
    assert tuple(getattr(caught.value, "__notes__", ())) == ()
    assert state.safety_finished is False
    assert exchange._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]
    assert intended_r2 is not None
    assert state.journal is intended_r2
    assert response_delivery is not None
    assert response_request is not None
    assert response_artifact is not None
    assert state.response_delivery is response_delivery
    assert state.response_accepted_request is response_request
    assert state.response_artifact is response_artifact
    final_request = original_get_request(command.request_id)
    final_delivery = original_get_delivery(DELIVERY_ID)
    assert final_request == response_request
    assert final_delivery == response_delivery
    assert harness.paths.pending_file.read_bytes() == r2_bytes
    assert harness.paths.pending_file.stat().st_mtime_ns == r2_mtime
    assert response_path.exists()
    assert response_path.read_bytes() == response_bytes
    assert response_path.stat().st_mtime_ns == response_mtime


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_mode",
    ["validate_error", "canonical_dump_drift"],
)
async def test_completed_recovery_failure_is_artifact_first_quarantined_without_idle(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    command = _command(harness, timeout=0.3)
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    expected_settlement = _completed_artifact(command).accepted_response.settlement
    assert type(expected_settlement) is CompletedSettlement
    expected_primary_payload = {
        "code": ErrorCode.PROTOCOL_ERROR.value,
        "message": "mutation result failed frozen recovery validation",
        "details": {
            "request_id": str(command.request_id),
            "operation": command.body.operation,
            "recovery_schema_id": recovery_schema.schema_id,
        },
    }
    expected_primary_json = _canonical_json(expected_primary_payload).decode("utf-8")
    injected = ValueError("injected frozen recovery validation failure")
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected_settlement.result))
    monkeypatch.setattr(peer, "result_for", _forbidden)

    transport = _transport(harness)
    response_path = harness.paths.response_path(command.request_id)
    journals: list[PendingJournal] = []
    waiter_artifacts: list[ResponseArtifact] = []
    recovery_causes: list[BaseException] = []
    recovery_primaries: list[BridgeError] = []
    recovery_validate_calls = 0
    recovery_dump_calls = 0
    journal_r2_saved = False
    response_recorded = False
    request_response_accepted = False
    original_save = transport.journals.save
    original_record_response = transport.ledger.record_response
    original_transition = transport.ledger.transition
    original_wait = ResponseWaiter.wait
    original_recovery_error = mutation_module._recovery_validation_error  # pyright: ignore[reportPrivateUsage]

    def raw_response_columns() -> tuple[object, object, object, object, object]:
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            row = connection.execute(
                """
                SELECT response_at_ms,response_sha256,response_size_bytes,
                       accepted_response_json,settlement_json
                FROM deliveries WHERE delivery_id=?
                """,
                (str(DELIVERY_ID),),
            ).fetchone()
        assert row is not None
        return cast(tuple[object, object, object, object, object], tuple(row))

    def expected_response_columns(
        artifact: ResponseArtifact,
    ) -> tuple[int, str, int, bytes, bytes]:
        settlement = artifact.accepted_response.settlement
        assert type(settlement) is CompletedSettlement
        return (
            artifact.accepted_at_ms,
            artifact.sha256,
            artifact.size_bytes,
            _canonical_json(artifact.accepted_response.model_dump(mode="json")),
            _canonical_json(settlement.model_dump(mode="json")),
        )

    async def traced_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        assert waiter._response_path == response_path  # pyright: ignore[reportPrivateUsage]
        artifact = await original_wait(waiter, timeout_seconds)
        settlement = artifact.accepted_response.settlement
        assert type(settlement) is CompletedSettlement
        assert settlement.result == expected_settlement.result
        assert artifact.accepted_response.delivery_kind == "request"
        waiter_artifacts.append(artifact)
        trace.append("waiter.returned")
        return artifact

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal journal_r2_saved
        original = journal.original
        if original.state is PendingPhase.RESPONSE_ACCEPTED:
            assert expected_revisions == JournalRevisions(
                original=1,
                reconcile_attempt=None,
            )
            assert len(waiter_artifacts) == 1
            assert original.revision == 2
            assert original.response_artifact == waiter_artifacts[0]
            assert original.settlement == expected_settlement
            assert raw_response_columns() == (None, None, None, None, None)
            request = transport.ledger.get_request(command.request_id)
            assert request is not None and request.state is HostRequestState.PUBLISHED
            assert recovery_validate_calls == recovery_dump_calls == 0
        elif original.state is PendingPhase.QUARANTINED:
            assert expected_revisions == JournalRevisions(
                original=2,
                reconcile_attempt=None,
            )
            assert len(journals) == 3
            prior = journals[-1]
            assert prior.original.state is PendingPhase.RESPONSE_ACCEPTED
            assert original.revision == 3
            assert journal == _journal_with_original(
                prior,
                revision=3,
                state=PendingPhase.QUARANTINED,
                updated_at_ms=original.updated_at_ms,
            )
            assert original.response_artifact == waiter_artifacts[0]
            assert original.settlement == expected_settlement
            assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])
            request = transport.ledger.get_request(command.request_id)
            assert request is not None
            assert request.state is HostRequestState.RESPONSE_ACCEPTED
            assert request.terminal_at_ms is None
            assert request.result_json is None
            assert request.error_json is None
            assert request.resolution_json is None
            assert len(recovery_causes) == 1
            assert len(recovery_primaries) == 1
        journals.append(journal)
        revisions = original_save(journal, expected_revisions=expected_revisions)
        trace.append(f"journal.{original.state.value}")
        if original.state is PendingPhase.RESPONSE_ACCEPTED:
            journal_r2_saved = True
        return revisions

    def traced_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        nonlocal response_recorded
        assert artifact is waiter_artifacts[0]
        assert journal_r2_saved
        assert raw_response_columns() == (None, None, None, None, None)
        recorded = original_record_response(artifact)
        assert recorded.response_artifact == artifact
        assert recorded.settlement == expected_settlement
        assert raw_response_columns() == expected_response_columns(artifact)
        response_recorded = True
        trace.append("ledger.response_recorded")
        return recorded

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal request_response_accepted
        assert request_id == command.request_id
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            assert expected_states == frozenset({HostRequestState.PUBLISHED})
            assert journal_r2_saved and response_recorded
            assert recovery_validate_calls == recovery_dump_calls == 0
            assert terminal_at_ms is None
            assert result_json is error_json is resolution_json is None
        elif new_state is HostRequestState.QUARANTINED:
            assert expected_states == frozenset({HostRequestState.RESPONSE_ACCEPTED})
            assert journals[-1].original.state is PendingPhase.QUARANTINED
            assert trace[-1] == "journal.quarantined"
            assert updated_at_ms == journals[-1].original.updated_at_ms
            assert terminal_at_ms is None
            assert result_json is None
            assert error_json == expected_primary_json
            assert resolution_json is None
            assert len(recovery_causes) == 1
            assert len(recovery_primaries) == 1
        observed = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        trace.append(f"ledger.request_{new_state.value}")
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            request_response_accepted = True
        return observed

    recovery_delegate = recovery_schema.adapter

    class RecoveryAdapterFailure:
        def validate_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_validate_calls
            assert value == expected_settlement.result
            assert journal_r2_saved and response_recorded and request_response_accepted
            assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])
            recovery_validate_calls += 1
            trace.append("recovery.validate")
            if failure_mode == "validate_error":
                raise injected
            assert failure_mode == "canonical_dump_drift"
            return cast(Any, recovery_delegate).validate_python(value, *args, **kwargs)

        def dump_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_dump_calls
            assert failure_mode == "canonical_dump_drift"
            assert recovery_validate_calls == 1
            recovery_dump_calls += 1
            normalized = cast(
                JsonValue,
                cast(Any, recovery_delegate).dump_python(value, *args, **kwargs),
            )
            assert isinstance(normalized, dict)
            drifted: dict[str, JsonValue] = dict(normalized)
            drifted["name"] = "canonical recovery drift"
            trace.append("recovery.dump_drift")
            return drifted

        def __getattr__(self, name: str) -> object:
            return getattr(recovery_delegate, name)

    recovery_failure = RecoveryAdapterFailure()
    adapter_descriptor = inspect.getattr_static(SchemaBinding, "adapter")
    descriptor_get = getattr(adapter_descriptor, "__get__", None)
    assert callable(descriptor_get)

    def guarded_schema_adapter(binding: SchemaBinding) -> object:
        if binding is recovery_schema:
            return recovery_failure
        return cast(Any, descriptor_get)(binding, SchemaBinding)

    def capture_recovery_error(
        validated: object,
        cause: BaseException,
        *,
        recovery_schema_id: str,
    ) -> BridgeError:
        recovery_causes.append(cause)
        primary = cast(Any, original_recovery_error)(
            validated,
            cause,
            recovery_schema_id=recovery_schema_id,
        )
        assert isinstance(primary, BridgeError)
        recovery_primaries.append(primary)
        return primary

    monkeypatch.setattr(transport.journals, "save", traced_save)
    monkeypatch.setattr(transport.journals, "delete", _forbidden)
    monkeypatch.setattr(transport.ledger, "record_response", traced_record_response)
    monkeypatch.setattr(transport.ledger, "transition", traced_transition)
    monkeypatch.setattr(ResponseWaiter, "wait", traced_wait)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(
        SchemaBinding,
        "adapter",
        property(guarded_schema_adapter),
        raising=False,
    )
    monkeypatch.setattr(mutation_module, "_recovery_validation_error", capture_recovery_error)
    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    pending_before_second = b""
    response_before_second = b""
    inbox_before_second = b""
    pending_mtime = 0
    response_mtime = 0
    inbox_mtime = 0
    async with peer:
        async with transport.session() as channel:
            concrete = cast(_FileBridgeChannel, channel)
            runner = concrete._mutation_exchange  # pyright: ignore[reportPrivateUsage]
            monkeypatch.setattr(runner, "_queue_response_cleanup", _forbidden)
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

            assert caught.value.to_payload() == expected_primary_payload
            assert len(recovery_causes) == 1
            assert len(recovery_primaries) == 1
            assert caught.value is recovery_primaries[0]
            assert caught.value.__cause__ is recovery_causes[0]
            if failure_mode == "validate_error":
                assert recovery_causes[0] is injected
                assert recovery_validate_calls == 1
                assert recovery_dump_calls == 0
            else:
                assert failure_mode == "canonical_dump_drift"
                assert type(recovery_causes[0]) is ValueError
                assert str(recovery_causes[0]) == (
                    "normalized recovery result changed canonical bytes"
                )
                assert recovery_validate_calls == recovery_dump_calls == 1
            assert concrete._active_task is None  # pyright: ignore[reportPrivateUsage]
            assert concrete._poisoned is True  # pyright: ignore[reportPrivateUsage]
            assert concrete._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
            assert runner._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]

            state = runner._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            assert state.artifact_observed is True
            assert state.response_artifact is waiter_artifacts[0]
            assert state.quarantined_journal is journals[-1]
            assert state.journal is journals[-1]
            assert state.response_accepted_journal is journals[2]
            assert state.response_accepted_request is not None
            assert state.response_accepted_request.state is HostRequestState.RESPONSE_ACCEPTED
            assert state.response_delivery is not None
            assert state.response_delivery.response_artifact == waiter_artifacts[0]
            assert state.safety_finished is True
            loaded = transport.journals.load()
            assert loaded is not None and loaded.journal == journals[-1]

            final_request = transport.ledger.get_request(command.request_id)
            final_delivery = transport.ledger.get_delivery(DELIVERY_ID)
            assert final_request is not None and final_delivery is not None
            assert final_request.state is HostRequestState.QUARANTINED
            assert final_request.updated_at_ms == journals[-1].original.updated_at_ms
            assert final_request.terminal_at_ms is None
            assert final_request.result_json is None
            assert final_request.error_json == expected_primary_json
            assert final_request.resolution_json is None
            assert final_delivery.response_artifact == waiter_artifacts[0]
            assert final_delivery.settlement == expected_settlement
            assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])

            pending_before_second = harness.paths.pending_file.read_bytes()
            response_before_second = response_path.read_bytes()
            inbox_before_second = harness.paths.inbox.read_bytes()
            pending_mtime = harness.paths.pending_file.stat().st_mtime_ns
            response_mtime = response_path.stat().st_mtime_ns
            inbox_mtime = harness.paths.inbox.stat().st_mtime_ns
            trace_before_second = tuple(trace)
            inspector_calls = harness.inspector.calls

            with monkeypatch.context() as second_guard:
                second_guard.setattr(concrete, "_perform_exchange", _forbidden)
                second_guard.setattr(PendingJournalStore, "load", _forbidden)
                second_guard.setattr(ManifestCatalog, "resolve_running", _forbidden)
                second_guard.setattr(RequestLedger, "get_request", _forbidden)
                second_guard.setattr(RequestLedger, "insert_prepared", _forbidden)
                second_guard.setattr(Inspector, "matching_processes", _forbidden)
                second_guard.setattr(sqlite3, "connect", _forbidden)
                second_guard.setattr(InboxPublisher, "publish_delivery", _forbidden)
                second_guard.setattr(InboxPublisher, "publish_idle", _forbidden)
                second_guard.setattr(Path, "exists", _forbidden)
                second_guard.setattr(Path, "read_bytes", _forbidden)
                second_guard.setattr(Path, "write_bytes", _forbidden)
                second_guard.setattr(Path, "stat", _forbidden)
                second_guard.setattr(Path, "open", _forbidden)
                second_guard.setattr(Path, "unlink", _forbidden)
                with pytest.raises(BridgeError) as second:
                    await channel.exchange(command)
            assert second.value.code is ErrorCode.STATE_CONFLICT
            assert second.value.message == "file-bridge channel requires a fresh session"
            assert tuple(trace) == trace_before_second
            assert harness.inspector.calls == inspector_calls
            assert harness.paths.pending_file.read_bytes() == pending_before_second
            assert response_path.read_bytes() == response_before_second
            assert harness.paths.inbox.read_bytes() == inbox_before_second
            assert harness.paths.pending_file.stat().st_mtime_ns == pending_mtime
            assert response_path.stat().st_mtime_ns == response_mtime
            assert harness.paths.inbox.stat().st_mtime_ns == inbox_mtime

    assert caught is not None
    assert response_path.exists()
    assert harness.paths.pending_file.exists()
    assert harness.paths.pending_file.read_bytes() == pending_before_second
    assert response_path.read_bytes() == response_before_second
    assert harness.paths.inbox.read_bytes() == inbox_before_second
    assert harness.paths.pending_file.stat().st_mtime_ns == pending_mtime
    assert response_path.stat().st_mtime_ns == response_mtime
    assert harness.paths.inbox.stat().st_mtime_ns == inbox_mtime
    assert [journal.original.state for journal in journals] == [
        PendingPhase.PREPARED,
        PendingPhase.PUBLISHED,
        PendingPhase.RESPONSE_ACCEPTED,
        PendingPhase.QUARANTINED,
    ]
    assert [journal.original.revision for journal in journals] == [0, 1, 2, 3]
    assert journals[2].original.response_artifact == waiter_artifacts[0]
    assert journals[3].original.response_artifact == waiter_artifacts[0]
    assert journals[2].original.settlement == expected_settlement
    assert journals[3].original.settlement == expected_settlement
    assert trace.count("peer.request_observed") == 1
    assert trace.count("peer.response_written") == 1

    def before(left: str, right: str) -> None:
        assert trace.index(left) < trace.index(right), trace

    recovery_event = (
        "recovery.validate" if failure_mode == "validate_error" else "recovery.dump_drift"
    )
    for left, right in (
        ("peer.response_written", "waiter.returned"),
        ("waiter.returned", "journal.response_accepted"),
        ("journal.response_accepted", "ledger.response_recorded"),
        ("ledger.response_recorded", "ledger.request_response_accepted"),
        ("ledger.request_response_accepted", recovery_event),
        (recovery_event, "journal.quarantined"),
        ("journal.quarantined", "ledger.request_quarantined"),
    ):
        before(left, right)
    assert "ledger.request_idle_published" not in trace
    assert "ledger.request_completed" not in trace
    assert "ledger.request_rejected" not in trace


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_case",
    ["before_runtime", "after_runtime", "before_bridge"],
)
async def test_idle_publication_failure_preserves_response_accepted_without_retry(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    failure_case: str,
) -> None:
    command = _command(harness, timeout=0.3)
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    expected_settlement = _completed_artifact(command).accepted_response.settlement
    assert type(expected_settlement) is CompletedSettlement
    delegate_idle = failure_case == "after_runtime"
    if failure_case == "before_bridge":
        injected: Exception = BridgeError(
            ErrorCode.STATE_CONFLICT,
            "injected idle publication failure",
            {"stage": "before_delegate"},
        )
        expected_primary_payload = injected.to_payload()
    else:
        assert failure_case in {"before_runtime", "after_runtime"}
        injected = RuntimeError(f"{failure_case} idle publication failure")
        expected_primary_payload = {
            "code": ErrorCode.STATE_CONFLICT.value,
            "message": "file-bridge mutation exchange failed",
            "details": {"type": "RuntimeError"},
        }

    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=expected_settlement.result))
    monkeypatch.setattr(peer, "result_for", _forbidden)
    transport = _transport(harness)
    response_path = harness.paths.response_path(command.request_id)
    journals: list[PendingJournal] = []
    waiter_artifacts: list[ResponseArtifact] = []
    recovery_validate_calls = 0
    recovery_dump_calls = 0
    idle_calls = 0
    real_idle_returns = 0
    publish_calls = 0
    journal_r2_saved = False
    response_recorded = False
    request_response_accepted = False
    request_inbox_bytes = b""
    request_inbox_mtime = 0
    published_deliveries: list[PreparedDelivery] = []
    classified_errors: list[tuple[BaseException, BaseException]] = []
    runner_ref: MutationExchange | None = None
    original_save = transport.journals.save
    original_record_response = transport.ledger.record_response
    original_transition = transport.ledger.transition
    original_wait = ResponseWaiter.wait
    original_publish = InboxPublisher.publish_delivery
    original_idle = InboxPublisher.publish_idle
    original_classify = mutation_module._classify_exchange_error  # pyright: ignore[reportPrivateUsage]

    def raw_response_columns() -> tuple[object, object, object, object, object]:
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            row = connection.execute(
                """
                SELECT response_at_ms,response_sha256,response_size_bytes,
                       accepted_response_json,settlement_json
                FROM deliveries WHERE delivery_id=?
                """,
                (str(DELIVERY_ID),),
            ).fetchone()
        assert row is not None
        return cast(tuple[object, object, object, object, object], tuple(row))

    def expected_response_columns(
        artifact: ResponseArtifact,
    ) -> tuple[int, str, int, bytes, bytes]:
        settlement = artifact.accepted_response.settlement
        assert type(settlement) is CompletedSettlement
        return (
            artifact.accepted_at_ms,
            artifact.sha256,
            artifact.size_bytes,
            _canonical_json(artifact.accepted_response.model_dump(mode="json")),
            _canonical_json(settlement.model_dump(mode="json")),
        )

    async def traced_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        assert waiter._response_path == response_path  # pyright: ignore[reportPrivateUsage]
        artifact = await original_wait(waiter, timeout_seconds)
        settlement = artifact.accepted_response.settlement
        assert type(settlement) is CompletedSettlement
        assert settlement.result == expected_settlement.result
        waiter_artifacts.append(artifact)
        trace.append("waiter.returned")
        return artifact

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal journal_r2_saved
        original = journal.original
        if original.state is PendingPhase.RESPONSE_ACCEPTED:
            assert expected_revisions == JournalRevisions(
                original=1,
                reconcile_attempt=None,
            )
            assert len(waiter_artifacts) == 1
            assert original.revision == 2
            assert original.response_artifact == waiter_artifacts[0]
            assert original.settlement == expected_settlement
            assert raw_response_columns() == (None, None, None, None, None)
            assert recovery_validate_calls == recovery_dump_calls == 0
        elif original.state in {PendingPhase.IDLE_PUBLISHED, PendingPhase.QUARANTINED}:
            raise AssertionError("idle publication failure rewrote the revision-two journal")
        journals.append(journal)
        revisions = original_save(journal, expected_revisions=expected_revisions)
        trace.append(f"journal.{original.state.value}")
        if original.state is PendingPhase.RESPONSE_ACCEPTED:
            journal_r2_saved = True
        return revisions

    def traced_record_response(artifact: ResponseArtifact) -> DeliveryRecord:
        nonlocal response_recorded
        assert artifact is waiter_artifacts[0]
        assert journal_r2_saved
        assert raw_response_columns() == (None, None, None, None, None)
        recorded = original_record_response(artifact)
        assert recorded.response_artifact == artifact
        assert recorded.settlement == expected_settlement
        assert raw_response_columns() == expected_response_columns(artifact)
        response_recorded = True
        trace.append("ledger.response_recorded")
        return recorded

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal request_response_accepted
        assert request_id == command.request_id
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            assert expected_states == frozenset({HostRequestState.PUBLISHED})
            assert journal_r2_saved and response_recorded
            assert recovery_validate_calls == recovery_dump_calls == 0
            assert terminal_at_ms is None
            assert result_json is error_json is resolution_json is None
        elif new_state is not HostRequestState.PUBLISHED:
            raise AssertionError(
                f"idle publication failure changed request state to {new_state.value}"
            )
        observed = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        trace.append(f"ledger.request_{new_state.value}")
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            request_response_accepted = True
        return observed

    recovery_delegate = recovery_schema.adapter

    class RecoveryAdapterSpy:
        def validate_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_validate_calls
            assert value == expected_settlement.result
            assert journal_r2_saved and response_recorded and request_response_accepted
            assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])
            recovery_validate_calls += 1
            trace.append("recovery.validate")
            return cast(Any, recovery_delegate).validate_python(value, *args, **kwargs)

        def dump_python(
            self,
            value: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal recovery_dump_calls
            assert recovery_validate_calls == 1
            recovery_dump_calls += 1
            normalized = cast(
                JsonValue,
                cast(Any, recovery_delegate).dump_python(value, *args, **kwargs),
            )
            assert _canonical_json(normalized) == _canonical_json(expected_settlement.result)
            assert runner_ref is not None
            state = runner_ref._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            assert getattr(state, "idle_publication_entered", None) is False
            assert getattr(state, "idle_publication_returned", None) is False
            assert state.safety_finished is False
            trace.append("recovery.dump")
            return normalized

        def __getattr__(self, name: str) -> object:
            return getattr(recovery_delegate, name)

    recovery_spy = RecoveryAdapterSpy()
    adapter_descriptor = inspect.getattr_static(SchemaBinding, "adapter")
    descriptor_get = getattr(adapter_descriptor, "__get__", None)
    assert callable(descriptor_get)

    def guarded_schema_adapter(binding: SchemaBinding) -> object:
        if binding is recovery_schema:
            return recovery_spy
        return cast(Any, descriptor_get)(binding, SchemaBinding)

    def traced_publish(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal publish_calls, request_inbox_bytes, request_inbox_mtime
        assert publisher is transport.inbox
        publish_calls += 1
        assert publish_calls == 1
        original_publish(
            publisher,
            delivery,
            runtime_snapshot=runtime_snapshot,
        )
        expected = render_delivery_lua(delivery, runtime_snapshot)
        request_inbox_bytes = harness.paths.inbox.read_bytes()
        request_inbox_mtime = harness.paths.inbox.stat().st_mtime_ns
        assert request_inbox_bytes == expected
        published_deliveries.append(delivery)
        trace.append("inbox.request_replaced")

    def traced_classify(error: BaseException) -> BaseException:
        primary = original_classify(error)
        classified_errors.append((error, primary))
        return primary

    def fail_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_calls, real_idle_returns
        assert publisher is transport.inbox
        transport.root_lock.require_acquired()
        assert runner_ref is not None
        state = runner_ref._current_state  # pyright: ignore[reportPrivateUsage]
        assert state is not None
        assert getattr(state, "idle_publication_entered", None) is True
        assert getattr(state, "idle_publication_returned", None) is False
        idle_calls += 1
        assert idle_calls == 1
        assert publish_calls == 1
        assert len(published_deliveries) == 1
        assert state.rendered == request_inbox_bytes
        assert request_inbox_bytes == render_delivery_lua(
            published_deliveries[0],
            command.runtime_snapshot,
        )
        assert harness.paths.inbox.read_bytes() == request_inbox_bytes
        assert harness.paths.inbox.stat().st_mtime_ns == request_inbox_mtime
        assert recovery_validate_calls == recovery_dump_calls == 1
        assert len(journals) == 3
        assert journals[-1].original.state is PendingPhase.RESPONSE_ACCEPTED
        assert journals[-1].original.revision == 2
        assert journals[-1].original.response_artifact == waiter_artifacts[0]
        assert journals[-1].original.settlement == expected_settlement
        request = transport.ledger.get_request(command.request_id)
        delivery = transport.ledger.get_delivery(DELIVERY_ID)
        assert request is not None and request.state is HostRequestState.RESPONSE_ACCEPTED
        assert delivery is not None
        assert state.response_accepted_journal is journals[-1]
        assert state.response_accepted_request == request
        assert state.response_delivery == delivery
        assert request.terminal_at_ms is None
        assert request.result_json is request.error_json is request.resolution_json is None
        assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])
        assert response_path.exists()
        trace.append("idle.entered")
        if delegate_idle:
            original_idle(publisher)
            real_idle_returns += 1
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            assert getattr(state, "idle_publication_entered", None) is True
            assert getattr(state, "idle_publication_returned", None) is False
            trace.append("idle.delegate_returned")
        trace.append("idle.raised")
        raise injected

    monkeypatch.setattr(transport.journals, "save", traced_save)
    monkeypatch.setattr(transport.journals, "delete", _forbidden)
    monkeypatch.setattr(transport.ledger, "record_response", traced_record_response)
    monkeypatch.setattr(transport.ledger, "transition", traced_transition)
    monkeypatch.setattr(ResponseWaiter, "wait", traced_wait)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish)
    monkeypatch.setattr(InboxPublisher, "publish_idle", fail_idle)
    monkeypatch.setattr(mutation_module, "_classify_exchange_error", traced_classify)
    monkeypatch.setattr(
        SchemaBinding,
        "adapter",
        property(guarded_schema_adapter),
        raising=False,
    )
    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    pending_before_second = b""
    response_before_second = b""
    inbox_before_second = b""
    pending_mtime = 0
    response_mtime = 0
    inbox_mtime = 0
    async with peer:
        async with transport.session() as channel:
            concrete = cast(_FileBridgeChannel, channel)
            runner = concrete._mutation_exchange  # pyright: ignore[reportPrivateUsage]
            runner_ref = runner
            monkeypatch.setattr(runner, "_queue_response_cleanup", _forbidden)
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

            assert len(classified_errors) == 1
            classified_input, classified_primary = classified_errors[0]
            assert classified_input is injected
            assert caught.value is classified_primary
            if isinstance(injected, BridgeError):
                assert caught.value is injected
                assert caught.value.__cause__ is None
            else:
                assert caught.value.to_payload() == expected_primary_payload
                assert caught.value.__cause__ is injected
            assert concrete._active_task is None  # pyright: ignore[reportPrivateUsage]
            assert concrete._poisoned is True  # pyright: ignore[reportPrivateUsage]
            assert concrete._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
            assert runner._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]

            state = runner._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            assert state.artifact_observed is True
            assert state.response_artifact is waiter_artifacts[0]
            assert state.journal is journals[-1]
            assert state.response_accepted_journal is journals[-1]
            assert state.idle_published_journal is None
            assert state.quarantined_journal is None
            assert getattr(state, "idle_publication_entered", None) is True
            assert getattr(state, "idle_publication_returned", None) is False
            assert state.safety_finished is False

            loaded = transport.journals.load()
            assert loaded is not None and loaded.journal == journals[-1]
            final_request = transport.ledger.get_request(command.request_id)
            final_delivery = transport.ledger.get_delivery(DELIVERY_ID)
            assert final_request is not None and final_delivery is not None
            assert state.response_accepted_request == final_request
            assert state.response_delivery == final_delivery
            assert final_request.state is HostRequestState.RESPONSE_ACCEPTED
            assert final_request.terminal_at_ms is None
            assert final_request.result_json is None
            assert final_request.error_json is None
            assert final_request.resolution_json is None
            assert final_delivery.response_artifact == waiter_artifacts[0]
            assert final_delivery.settlement == expected_settlement
            assert raw_response_columns() == expected_response_columns(waiter_artifacts[0])
            assert response_path.exists()
            if delegate_idle:
                assert harness.paths.inbox.read_bytes() == render_idle_lua()
            else:
                assert harness.paths.inbox.read_bytes() == request_inbox_bytes

            pending_before_second = harness.paths.pending_file.read_bytes()
            response_before_second = response_path.read_bytes()
            inbox_before_second = harness.paths.inbox.read_bytes()
            pending_mtime = harness.paths.pending_file.stat().st_mtime_ns
            response_mtime = response_path.stat().st_mtime_ns
            inbox_mtime = harness.paths.inbox.stat().st_mtime_ns
            trace_before_second = tuple(trace)
            inspector_calls = harness.inspector.calls

            with monkeypatch.context() as second_guard:
                second_guard.setattr(concrete, "_perform_exchange", _forbidden)
                second_guard.setattr(PendingJournalStore, "load", _forbidden)
                second_guard.setattr(ManifestCatalog, "resolve_running", _forbidden)
                second_guard.setattr(RequestLedger, "get_request", _forbidden)
                second_guard.setattr(RequestLedger, "insert_prepared", _forbidden)
                second_guard.setattr(Inspector, "matching_processes", _forbidden)
                second_guard.setattr(sqlite3, "connect", _forbidden)
                second_guard.setattr(InboxPublisher, "publish_delivery", _forbidden)
                second_guard.setattr(InboxPublisher, "publish_idle", _forbidden)
                second_guard.setattr(Path, "exists", _forbidden)
                second_guard.setattr(Path, "read_bytes", _forbidden)
                second_guard.setattr(Path, "write_bytes", _forbidden)
                second_guard.setattr(Path, "stat", _forbidden)
                second_guard.setattr(Path, "open", _forbidden)
                second_guard.setattr(Path, "unlink", _forbidden)
                with pytest.raises(BridgeError) as second:
                    await channel.exchange(command)
            assert second.value.code is ErrorCode.STATE_CONFLICT
            assert second.value.message == "file-bridge channel requires a fresh session"
            assert tuple(trace) == trace_before_second
            assert harness.inspector.calls == inspector_calls
            assert harness.paths.pending_file.read_bytes() == pending_before_second
            assert response_path.read_bytes() == response_before_second
            assert harness.paths.inbox.read_bytes() == inbox_before_second
            assert harness.paths.pending_file.stat().st_mtime_ns == pending_mtime
            assert response_path.stat().st_mtime_ns == response_mtime
            assert harness.paths.inbox.stat().st_mtime_ns == inbox_mtime

    assert caught is not None
    assert publish_calls == 1
    assert len(published_deliveries) == 1
    assert request_inbox_bytes == render_delivery_lua(
        published_deliveries[0],
        command.runtime_snapshot,
    )
    assert idle_calls == 1
    assert real_idle_returns == (1 if delegate_idle else 0)
    assert response_path.exists()
    assert harness.paths.pending_file.exists()
    assert harness.paths.pending_file.read_bytes() == pending_before_second
    assert response_path.read_bytes() == response_before_second
    assert harness.paths.inbox.read_bytes() == inbox_before_second
    assert harness.paths.pending_file.stat().st_mtime_ns == pending_mtime
    assert response_path.stat().st_mtime_ns == response_mtime
    assert harness.paths.inbox.stat().st_mtime_ns == inbox_mtime
    if delegate_idle:
        assert inbox_before_second == render_idle_lua()
    else:
        assert inbox_before_second == request_inbox_bytes
        assert inbox_mtime == request_inbox_mtime
    assert [journal.original.state for journal in journals] == [
        PendingPhase.PREPARED,
        PendingPhase.PUBLISHED,
        PendingPhase.RESPONSE_ACCEPTED,
    ]
    assert [journal.original.revision for journal in journals] == [0, 1, 2]
    assert journals[-1].original.response_artifact == waiter_artifacts[0]
    assert journals[-1].original.settlement == expected_settlement
    assert trace.count("peer.request_observed") == 1
    assert trace.count("peer.response_written") == 1

    def before(left: str, right: str) -> None:
        assert trace.index(left) < trace.index(right), trace

    for left, right in (
        ("inbox.request_replaced", "peer.request_observed"),
        ("peer.response_written", "waiter.returned"),
        ("waiter.returned", "journal.response_accepted"),
        ("journal.response_accepted", "ledger.response_recorded"),
        ("ledger.response_recorded", "ledger.request_response_accepted"),
        ("ledger.request_response_accepted", "recovery.validate"),
        ("recovery.validate", "recovery.dump"),
        ("recovery.dump", "idle.entered"),
        ("idle.entered", "idle.raised"),
    ):
        before(left, right)
    if delegate_idle:
        before("idle.entered", "idle.delegate_returned")
        before("idle.delegate_returned", "idle.raised")
    else:
        assert "idle.delegate_returned" not in trace
    assert "journal.idle_published" not in trace
    assert "journal.quarantined" not in trace
    assert "ledger.request_idle_published" not in trace
    assert "ledger.request_quarantined" not in trace
    assert "ledger.request_completed" not in trace
    assert "ledger.request_rejected" not in trace


@pytest.mark.asyncio
async def test_unit_add_missing_guid_raw_response_is_parser_failure_without_artifact(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command(
        harness,
        "unit.add",
        {
            "side_guid": "SIDE-1",
            "unit_type": "Aircraft",
            "dbid": 1,
            "name": "Raw Missing Guid",
            "latitude": 1.0,
            "longitude": 2.0,
        },
        timeout=0.5,
    )
    recovery_schema = command.invocation.recovery_schema
    assert recovery_schema is not None
    assert command.invocation.effective_class is OperationClass.MUTATION
    assert command.invocation.result_schema.schema_id == (
        "266a32aa0a8d2e06f172f71c9fab3150e6ad95771af6abbbc6278c886751d78e"
    )
    assert recovery_schema.schema_id == (
        "90bd097de0262a91044d867a8c24ae186fc2a23967f71077b1bd123199dd4544"
    )
    expected_error_payload: dict[str, JsonValue] = {
        "code": ErrorCode.PROTOCOL_ERROR.value,
        "message": "response result does not match frozen operation schema",
        "details": {},
    }
    expected_error_json = _canonical_json(expected_error_payload).decode("utf-8")

    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(StaySilent())
    monkeypatch.setattr(peer, "result_for", _forbidden)
    monkeypatch.setattr(peer, "_write_response", _forbidden)
    monkeypatch.setattr(OperationRegistry, "resolve_invocation", _forbidden)

    transport = _transport(harness)
    response_path = harness.paths.response_path(command.request_id)
    delivery_observed = asyncio.Event()
    observed_deliveries: list[PreparedDelivery] = []
    raw_responses: list[bytes] = []
    response_reads: list[bytes] = []
    journal_candidates: list[PendingJournal] = []
    original_apply_action = peer._apply_action  # pyright: ignore[reportPrivateUsage]
    original_save = transport.journals.save
    original_transition = transport.ledger.transition
    original_read_bytes = Path.read_bytes
    original_unlink = Path.unlink
    response_unlink_calls = 0

    def raw_response_columns() -> tuple[object, object, object, object, object]:
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            row = connection.execute(
                """
                SELECT response_at_ms,response_sha256,response_size_bytes,
                       accepted_response_json,settlement_json
                FROM deliveries WHERE delivery_id=?
                """,
                (str(DELIVERY_ID),),
            ).fetchone()
        assert row is not None
        return cast(tuple[object, object, object, object, object], tuple(row))

    async def traced_apply_action(
        action: object,
        delivery: PreparedDelivery,
        body: RequestBody,
        invocation: object,
    ) -> None:
        assert type(action) is StaySilent
        assert delivery.request_id == command.request_id
        assert delivery.delivery_id == DELIVERY_ID
        assert delivery.delivery_kind == "request"
        assert (
            delivery.request_hash == hashlib.sha256(canonical_body_bytes(command.body)).hexdigest()
        )
        assert body == command.body
        frozen = cast(Any, invocation)
        assert frozen.contract == command.invocation.contract
        assert frozen.wire_arguments == command.invocation.wire_arguments
        assert frozen.effective_class is command.invocation.effective_class
        assert frozen.result_schema == command.invocation.result_schema
        assert frozen.recovery_schema == command.invocation.recovery_schema
        await cast(Any, original_apply_action)(action, delivery, body, invocation)
        observed_deliveries.append(delivery)
        trace.append("peer.stay_silent_observed")
        delivery_observed.set()

    async def write_raw_response_after_observation() -> None:
        await delivery_observed.wait()
        assert observed_deliveries == [peer.observed_deliveries[0]]
        delivery = observed_deliveries[0]
        assert not response_path.exists()
        incomplete_result: dict[str, JsonValue] = {
            "name": "Raw Missing Guid",
            "side_guid": "SIDE-1",
            "dbid": 1,
            "latitude": 1.0,
            "longitude": 2.0,
        }
        inner: dict[str, object] = {
            "protocol": harness.snapshot.protocol,
            "request_id": str(delivery.request_id),
            "delivery_id": str(delivery.delivery_id),
            "request_hash": delivery.request_hash,
            "ok": True,
            "result": incomplete_result,
            "error": None,
            "scenario_time": "2026-07-10T13:00:00Z",
            "scenario_lineage_id": str(command.body.expected_lineage_id),
            "activation_id": str(command.body.expected_activation_id),
            "operation_manifest_sha256": harness.snapshot.operation_manifest_sha256,
            "bridge_version": harness.snapshot.runtime_version,
            "runtime_tag": harness.snapshot.runtime_tag,
            "runtime_asset_sha256": harness.snapshot.runtime_asset_sha256,
            "release_id": harness.snapshot.release_id,
        }
        inner_json = _canonical_json(inner).decode("utf-8")
        raw = _canonical_json({"Comments": inner_json})
        assert b'"unit_guid"' not in raw
        response_path.write_bytes(raw)
        assert response_path.read_bytes() == raw
        raw_responses.append(raw)
        trace.append("test.raw_response_written")

    def traced_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        original = journal.original
        if original.state is PendingPhase.PREPARED:
            assert original.revision == 0
            assert expected_revisions is None
        elif original.state is PendingPhase.PUBLISHED:
            assert original.revision == 1
            assert expected_revisions == JournalRevisions(
                original=0,
                reconcile_attempt=None,
            )
        elif original.state is PendingPhase.QUARANTINED:
            assert len(journal_candidates) == 2
            prior = journal_candidates[-1]
            assert prior.original.state is PendingPhase.PUBLISHED
            assert prior.original.revision == 1
            assert original.revision == 2
            assert expected_revisions == JournalRevisions(
                original=1,
                reconcile_attempt=None,
            )
            assert original.response_artifact is None
            assert original.settlement is None
            assert original.delivery_intents == prior.original.delivery_intents
            assert journal.header == prior.header
            assert len(raw_responses) == 1
            assert original_read_bytes(response_path) == raw_responses[0]
            assert len(response_reads) == 1
            assert raw_response_columns() == (None, None, None, None, None)
            request = transport.ledger.get_request(command.request_id)
            assert request is not None and request.state is HostRequestState.PUBLISHED
            assert request.result_json is None
            assert request.error_json is None
            assert request.resolution_json is None
            assert request.terminal_at_ms is None
        journal_candidates.append(journal)
        revisions = original_save(journal, expected_revisions=expected_revisions)
        trace.append(f"journal.{original.state.value}")
        return revisions

    def traced_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        assert request_id == command.request_id
        if new_state is HostRequestState.PUBLISHED:
            assert expected_states == frozenset({HostRequestState.PREPARED})
        elif new_state is HostRequestState.QUARANTINED:
            assert expected_states == frozenset({HostRequestState.PUBLISHED})
            assert journal_candidates[-1].original.state is PendingPhase.QUARANTINED
            assert trace[-1] == "journal.quarantined"
            assert updated_at_ms >= journal_candidates[-1].original.updated_at_ms
            assert terminal_at_ms is None
            assert result_json is None
            assert error_json == expected_error_json
            assert resolution_json is None
        else:
            raise AssertionError(f"unexpected request transition: {new_state.value}")
        observed = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        trace.append(f"ledger.request_{new_state.value}")
        return observed

    recovery_delegate = recovery_schema.adapter
    adapter_descriptor = inspect.getattr_static(SchemaBinding, "adapter")
    descriptor_get = getattr(adapter_descriptor, "__get__", None)
    assert callable(descriptor_get)

    class RecoveryAdapterTripwire:
        def validate_python(self, *_args: object, **_kwargs: object) -> Never:
            raise AssertionError("parser-invalid response reached recovery validation")

        def dump_python(self, *_args: object, **_kwargs: object) -> Never:
            raise AssertionError("parser-invalid response reached recovery serialization")

        def __getattr__(self, name: str) -> object:
            return getattr(recovery_delegate, name)

    recovery_tripwire = RecoveryAdapterTripwire()

    def guarded_schema_adapter(binding: SchemaBinding) -> object:
        if binding is recovery_schema:
            return recovery_tripwire
        return cast(Any, descriptor_get)(binding, SchemaBinding)

    def traced_read_bytes(path: Path) -> bytes:
        raw = original_read_bytes(path)
        if path == response_path and raw_responses and raw == raw_responses[0]:
            response_reads.append(raw)
        return raw

    def guarded_unlink(path: Path, *, missing_ok: bool = False) -> None:
        nonlocal response_unlink_calls
        if path == response_path:
            response_unlink_calls += 1
            raise AssertionError("parser-invalid raw response was unlinked")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(peer, "_apply_action", traced_apply_action)
    monkeypatch.setattr(transport.journals, "save", traced_save)
    monkeypatch.setattr(transport.journals, "delete", _forbidden)
    monkeypatch.setattr(transport.ledger, "record_response", _forbidden)
    monkeypatch.setattr(transport.ledger, "transition", traced_transition)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(
        SchemaBinding,
        "adapter",
        property(guarded_schema_adapter),
        raising=False,
    )
    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(Path, "read_bytes", traced_read_bytes)
    monkeypatch.setattr(Path, "unlink", guarded_unlink)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    pending_before_second = b""
    response_before_second = b""
    inbox_before_second = b""
    pending_mtime = 0
    response_mtime = 0
    inbox_mtime = 0
    async with peer:
        async with transport.session() as channel:
            concrete = cast(_FileBridgeChannel, channel)
            runner = concrete._mutation_exchange  # pyright: ignore[reportPrivateUsage]
            monkeypatch.setattr(runner, "_queue_response_cleanup", _forbidden)
            writer_task = asyncio.create_task(write_raw_response_after_observation())
            try:
                with pytest.raises(BridgeError) as caught:
                    await channel.exchange(command)
            finally:
                if not writer_task.done():
                    writer_task.cancel()
                try:
                    await writer_task
                except asyncio.CancelledError:
                    pass
            assert not writer_task.cancelled(), "fake peer never reached StaySilent observation"
            assert writer_task.exception() is None

            assert caught.value.code is ErrorCode.PROTOCOL_ERROR
            assert caught.value.message == expected_error_payload["message"]
            assert caught.value.details == {}
            parser_cause = caught.value.__cause__
            assert isinstance(parser_cause, ValidationError)
            parser_errors = parser_cause.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            )
            assert [(tuple(error["loc"]), error["type"]) for error in parser_errors] == [
                (("unit_guid",), "missing")
            ]
            assert len(response_reads) == 1
            assert concrete._active_task is None  # pyright: ignore[reportPrivateUsage]
            assert concrete._poisoned is True  # pyright: ignore[reportPrivateUsage]
            assert concrete._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
            assert runner._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]

            state = runner._current_state  # pyright: ignore[reportPrivateUsage]
            assert state is not None
            assert state.publication_boundary == "waiter"
            assert state.artifact_observed is False
            assert state.response_artifact is None
            assert state.response_accepted_journal is None
            assert state.response_accepted_request is None
            assert state.response_delivery is None
            assert state.quarantined_journal is journal_candidates[-1]
            assert state.journal is journal_candidates[-1]
            assert state.safety_finished is True

            loaded = transport.journals.load()
            assert loaded is not None and loaded.journal == journal_candidates[-1]
            final_request = transport.ledger.get_request(command.request_id)
            final_delivery = transport.ledger.get_delivery(DELIVERY_ID)
            assert final_request is not None and final_delivery is not None
            assert final_request.state is HostRequestState.QUARANTINED
            assert final_request.updated_at_ms >= journal_candidates[-1].original.updated_at_ms
            assert final_request.terminal_at_ms is None
            assert final_request.result_json is None
            assert final_request.error_json == expected_error_json
            assert final_request.resolution_json is None
            assert final_delivery.response_artifact is None
            assert final_delivery.settlement is None
            assert raw_response_columns() == (None, None, None, None, None)

            pending_before_second = original_read_bytes(harness.paths.pending_file)
            response_before_second = original_read_bytes(response_path)
            inbox_before_second = original_read_bytes(harness.paths.inbox)
            pending_mtime = harness.paths.pending_file.stat().st_mtime_ns
            response_mtime = response_path.stat().st_mtime_ns
            inbox_mtime = harness.paths.inbox.stat().st_mtime_ns
            trace_before_second = tuple(trace)
            inspector_calls = harness.inspector.calls

            with monkeypatch.context() as second_guard:
                second_guard.setattr(concrete, "_perform_exchange", _forbidden)
                second_guard.setattr(PendingJournalStore, "load", _forbidden)
                second_guard.setattr(ManifestCatalog, "resolve_running", _forbidden)
                second_guard.setattr(RequestLedger, "get_request", _forbidden)
                second_guard.setattr(RequestLedger, "insert_prepared", _forbidden)
                second_guard.setattr(Inspector, "matching_processes", _forbidden)
                second_guard.setattr(sqlite3, "connect", _forbidden)
                second_guard.setattr(InboxPublisher, "publish_delivery", _forbidden)
                second_guard.setattr(InboxPublisher, "publish_idle", _forbidden)
                second_guard.setattr(Path, "exists", _forbidden)
                second_guard.setattr(Path, "read_bytes", _forbidden)
                second_guard.setattr(Path, "write_bytes", _forbidden)
                second_guard.setattr(Path, "stat", _forbidden)
                second_guard.setattr(Path, "open", _forbidden)
                second_guard.setattr(Path, "unlink", _forbidden)
                with pytest.raises(BridgeError) as second:
                    await channel.exchange(command)
            assert second.value.code is ErrorCode.STATE_CONFLICT
            assert second.value.message == "file-bridge channel requires a fresh session"
            assert tuple(trace) == trace_before_second
            assert harness.inspector.calls == inspector_calls
            assert original_read_bytes(harness.paths.pending_file) == pending_before_second
            assert original_read_bytes(response_path) == response_before_second
            assert original_read_bytes(harness.paths.inbox) == inbox_before_second
            assert harness.paths.pending_file.stat().st_mtime_ns == pending_mtime
            assert response_path.stat().st_mtime_ns == response_mtime
            assert harness.paths.inbox.stat().st_mtime_ns == inbox_mtime

    assert caught is not None
    assert response_unlink_calls == 0
    assert response_path.exists()
    assert harness.paths.pending_file.exists()
    assert original_read_bytes(harness.paths.pending_file) == pending_before_second
    assert original_read_bytes(response_path) == response_before_second == raw_responses[0]
    assert original_read_bytes(harness.paths.inbox) == inbox_before_second
    assert harness.paths.pending_file.stat().st_mtime_ns == pending_mtime
    assert response_path.stat().st_mtime_ns == response_mtime
    assert harness.paths.inbox.stat().st_mtime_ns == inbox_mtime
    assert [journal.original.state for journal in journal_candidates] == [
        PendingPhase.PREPARED,
        PendingPhase.PUBLISHED,
        PendingPhase.QUARANTINED,
    ]
    assert [journal.original.revision for journal in journal_candidates] == [0, 1, 2]
    published = journal_candidates[1]
    quarantined = journal_candidates[2]
    assert quarantined.header == published.header
    assert quarantined.original.delivery_intents == published.original.delivery_intents
    assert quarantined.original.response_artifact is None
    assert quarantined.original.settlement is None
    immutable_fields = (
        "request_id",
        "request_hash",
        "operation",
        "effective_class",
        "body_json",
        "runtime_snapshot",
        "result_schema_id",
        "recovery_schema_id",
        "expected_lineage_id",
        "expected_activation_id",
        "original_target_request_id",
        "original_target_request_hash",
        "created_at_ms",
    )
    for field in immutable_fields:
        assert getattr(quarantined.original, field) == getattr(published.original, field)
    assert quarantined.original.updated_at_ms >= published.original.updated_at_ms
    assert len(peer.observed_deliveries) == 1
    assert peer.observed_deliveries[0] == observed_deliveries[0]
    assert original_read_bytes(harness.paths.inbox) == render_delivery_lua(
        peer.observed_deliveries[0],
        harness.snapshot,
    )
    assert trace.count("peer.request_observed") == 1
    assert trace.count("peer.stay_silent_observed") == 1
    assert trace.count("test.raw_response_written") == 1
    assert "peer.response_written" not in trace

    def before(left: str, right: str) -> None:
        assert trace.index(left) < trace.index(right), trace

    for left, right in (
        ("peer.request_observed", "peer.stay_silent_observed"),
        ("peer.stay_silent_observed", "test.raw_response_written"),
        ("test.raw_response_written", "journal.quarantined"),
        ("journal.quarantined", "ledger.request_quarantined"),
    ):
        before(left, right)
    assert "journal.response_accepted" not in trace
    assert "ledger.request_response_accepted" not in trace
    assert "ledger.request_idle_published" not in trace
    assert "ledger.request_completed" not in trace
    assert "ledger.request_rejected" not in trace


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "expected_message", "expected_location", "expected_validation_message"),
    [
        pytest.param(
            "reserved_details",
            "invalid response envelope",
            ("error", "details"),
            "Value error, details.mutation_not_started is reserved",
            id="reserved-details-with-null-sibling",
        ),
        pytest.param(
            "wrong_evidence_hash",
            "mutation-not-started evidence request hash does not match",
            None,
            None,
            id="wrong-evidence-hash-with-exact-outer-correlation",
        ),
    ],
)
async def test_invalid_not_started_raw_response_is_parser_quarantined_without_artifact(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    expected_message: str,
    expected_location: tuple[str, ...] | None,
    expected_validation_message: str | None,
) -> None:
    command = _command(harness, timeout=0.3)
    response_path = harness.paths.response_path(command.request_id)
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    peer.enqueue(StaySilent())
    exchange = _exchange(harness, queue_response_cleanup=_forbidden)
    delivery_observed = asyncio.Event()
    observed_deliveries: list[PreparedDelivery] = []
    raw_responses: list[bytes] = []
    original_apply_action = peer._apply_action  # pyright: ignore[reportPrivateUsage]

    expected_details: dict[str, object] = {}
    if expected_location is not None:
        assert expected_validation_message is not None
        expected_details = {
            "validation_errors": [
                {
                    "type": "value_error",
                    "loc": expected_location,
                    "msg": expected_validation_message,
                }
            ]
        }
    expected_error_payload = {
        "code": ErrorCode.PROTOCOL_ERROR.value,
        "message": expected_message,
        "details": expected_details,
    }
    expected_error_json = _canonical_json(expected_error_payload).decode("utf-8")

    async def observe_stay_silent(
        action: object,
        delivery: PreparedDelivery,
        body: RequestBody,
        invocation: object,
    ) -> None:
        assert type(action) is StaySilent
        assert delivery.request_id == command.request_id
        assert delivery.delivery_id == DELIVERY_ID
        assert body == command.body
        await cast(Any, original_apply_action)(action, delivery, body, invocation)
        observed_deliveries.append(delivery)
        delivery_observed.set()

    async def write_raw_response_after_observation() -> None:
        await delivery_observed.wait()
        delivery = observed_deliveries[0]
        evidence: dict[str, object] = {
            "schema_version": 1,
            "stage": "handler_preflight",
            "request_id": str(delivery.request_id),
            "request_hash": delivery.request_hash,
            "operation": command.body.operation,
            "mutation_barrier_written": False,
            "execute_started": False,
        }
        if case == "reserved_details":
            response_error: dict[str, object] = {
                "code": ErrorCode.CMO_LUA_ERROR.value,
                "message": "runtime rejected request before mutation execution",
                "details": {"mutation_not_started": evidence},
                "mutation_not_started": None,
            }
        else:
            assert case == "wrong_evidence_hash"
            evidence["request_hash"] = "e" * 64
            assert evidence["request_hash"] != delivery.request_hash
            response_error = {
                "code": ErrorCode.CMO_LUA_ERROR.value,
                "message": "runtime rejected request before mutation execution",
                "details": {},
                "mutation_not_started": evidence,
            }
        inner: dict[str, object] = {
            "protocol": harness.snapshot.protocol,
            "request_id": str(delivery.request_id),
            "delivery_id": str(delivery.delivery_id),
            "request_hash": delivery.request_hash,
            "ok": False,
            "result": None,
            "error": response_error,
            "scenario_time": "2026-07-10T13:00:00Z",
            "scenario_lineage_id": str(command.body.expected_lineage_id),
            "activation_id": str(command.body.expected_activation_id),
            "operation_manifest_sha256": harness.snapshot.operation_manifest_sha256,
            "bridge_version": harness.snapshot.runtime_version,
            "runtime_tag": harness.snapshot.runtime_tag,
            "runtime_asset_sha256": harness.snapshot.runtime_asset_sha256,
            "release_id": harness.snapshot.release_id,
        }
        inner_raw = _canonical_json(inner)
        if case == "reserved_details":
            assert b'"mutation_not_started":null' in inner_raw
        else:
            assert ("e" * 64).encode("ascii") in inner_raw
        raw = _canonical_json({"Comments": inner_raw.decode("utf-8")})
        response_path.write_bytes(raw)
        raw_responses.append(raw)

    monkeypatch.setattr(peer, "_apply_action", observe_stay_silent)
    monkeypatch.setattr(peer, "_write_response", _forbidden)
    monkeypatch.setattr(peer, "result_for", _forbidden)
    monkeypatch.setattr(exchange._ledger, "record_response", _forbidden)  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(exchange._journals, "delete", _forbidden)  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with peer:
        async with harness.lock:
            writer_task = asyncio.create_task(write_raw_response_after_observation())
            try:
                with pytest.raises(BridgeError) as caught:
                    await exchange.run(command)
            finally:
                if not writer_task.done():
                    writer_task.cancel()
                try:
                    await writer_task
                except asyncio.CancelledError:
                    pass
            assert not writer_task.cancelled(), "fake peer never reached StaySilent observation"
            assert writer_task.exception() is None

    assert caught is not None
    assert caught.value.to_payload() == expected_error_payload
    parser_cause = caught.value.__cause__
    if expected_location is None:
        assert expected_validation_message is None
        assert parser_cause is None
    else:
        assert expected_validation_message is not None
        assert isinstance(parser_cause, ValidationError)
        parser_errors = parser_cause.errors(
            include_url=False,
            include_context=False,
            include_input=False,
        )
        assert [(tuple(error["loc"]), error["type"], error["msg"]) for error in parser_errors] == [
            (expected_location, "value_error", expected_validation_message)
        ]

    state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
    assert state is not None
    assert state.publication_boundary == "waiter"
    assert state.artifact_observed is False
    assert state.response_artifact is None
    assert state.response_delivery is None
    assert state.safety_finished is True
    assert exchange._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]

    async with harness.lock:
        loaded = harness.journals.load()
        assert loaded is not None
        assert loaded.journal.original.state is PendingPhase.QUARANTINED
        assert loaded.journal.original.revision == 2
        assert loaded.journal.original.response_artifact is None
        assert loaded.journal.original.settlement is None
        request = harness.ledger.get_request(command.request_id)
        delivery = harness.ledger.get_delivery(DELIVERY_ID)
        assert request is not None and request.state is HostRequestState.QUARANTINED
        assert request.terminal_at_ms is None
        assert request.result_json is None
        assert request.error_json == expected_error_json
        assert request.resolution_json is None
        assert delivery is not None
        assert delivery.response_artifact is None
        assert delivery.settlement is None
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            response_columns = connection.execute(
                """
                SELECT response_at_ms,response_sha256,response_size_bytes,
                       accepted_response_json,settlement_json
                FROM deliveries WHERE delivery_id=?
                """,
                (str(DELIVERY_ID),),
            ).fetchone()
        assert response_columns == (None, None, None, None, None)

    assert len(raw_responses) == 1
    assert response_path.read_bytes() == raw_responses[0]
    assert harness.paths.pending_file.exists()
    assert harness.paths.inbox.read_bytes() == render_delivery_lua(
        observed_deliveries[0], harness.snapshot
    )
    assert peer.observed_deliveries == (observed_deliveries[0],)
    assert trace.count("peer.request_observed") == 1
    assert "peer.response_written" not in trace


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["timeout", "process_zero", "process_multiple", "process_replacement"],
)
async def test_real_waiter_no_artifact_failure_quarantines_once_without_retry_or_cancel(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    command = _command(harness, timeout=0 if case == "timeout" else 0.3)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    response_path = harness.paths.response_path(command.request_id)
    cleanup: list[Path] = []
    exchange = _exchange(harness, queue_response_cleanup=cleanup.append)
    replacement = ProcessInfo(
        pid=harness.process.pid + 1000,
        create_time=harness.process.create_time + 1.0,
        executable=harness.process.executable,
    )
    second = ProcessInfo(
        pid=harness.process.pid + 2000,
        create_time=harness.process.create_time + 2.0,
        executable=harness.process.executable,
    )

    if case == "timeout":
        expected_error_payload = {
            "code": ErrorCode.INDETERMINATE_OUTCOME.value,
            "message": "mutation outcome is indeterminate after response timeout",
            "details": {
                "request_id": str(command.request_id),
                "reason": "response_timeout",
            },
        }
    elif case == "process_zero":
        expected_error_payload = {
            "code": ErrorCode.CMO_NOT_RUNNING.value,
            "message": "no running CMO process matches the configured Command.exe",
            "details": {"command_exe": str(harness.paths.command_exe)},
        }
    elif case == "process_multiple":
        expected_error_payload = {
            "code": ErrorCode.MULTIPLE_CMO_INSTANCES.value,
            "message": "multiple CMO processes match the configured Command.exe",
            "details": {
                "command_exe": str(harness.paths.command_exe),
                "count": 2,
                "pids": [harness.process.pid, second.pid],
            },
        }
    else:
        assert case == "process_replacement"
        expected_error_payload = {
            "code": ErrorCode.STATE_CONFLICT.value,
            "message": "CMO process identity changed while waiting for response",
            "details": {
                "expected_process": {
                    "pid": harness.process.pid,
                    "create_time": harness.process.create_time,
                    "executable": str(harness.process.executable),
                },
                "actual_process": {
                    "pid": replacement.pid,
                    "create_time": replacement.create_time,
                    "executable": str(replacement.executable),
                },
            },
        }
    expected_error_json = _canonical_json(expected_error_payload).decode("utf-8")

    uuid_calls = 0
    prepare_calls = 0
    publish_calls = 0
    waiter_calls = 0
    process_calls = 0
    published_deliveries: list[PreparedDelivery] = []
    waiter_expectations: list[ResponseExpectation] = []
    waiter_errors: list[BridgeError] = []
    original_prepare = mutation_module.prepare_delivery
    original_publish = InboxPublisher.publish_delivery
    original_wait = ResponseWaiter.wait
    original_transition = harness.ledger.transition
    quarantine_transitions = 0

    def fixed_uuid4() -> UUID:
        nonlocal uuid_calls
        uuid_calls += 1
        return DELIVERY_ID

    def counted_prepare(
        body: RequestBody,
        *,
        request_id: UUID,
        delivery_id: UUID,
        delivery_kind: str,
    ) -> PreparedDelivery:
        nonlocal prepare_calls
        prepare_calls += 1
        return cast(Any, original_prepare)(
            body,
            request_id=request_id,
            delivery_id=delivery_id,
            delivery_kind=delivery_kind,
        )

    def counted_publish(
        publisher: InboxPublisher,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal publish_calls
        publish_calls += 1
        published_deliveries.append(delivery)
        original_publish(publisher, delivery, runtime_snapshot=runtime_snapshot)

    async def counted_wait(
        waiter: ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        nonlocal waiter_calls
        waiter_calls += 1
        expectation = waiter._expectation  # pyright: ignore[reportPrivateUsage]
        waiter_expectations.append(expectation)
        try:
            return await original_wait(waiter, timeout_seconds)
        except BridgeError as error:
            waiter_errors.append(error)
            raise

    def counted_transition(
        request_id: UUID,
        *,
        expected_states: frozenset[HostRequestState],
        new_state: HostRequestState,
        updated_at_ms: int,
        terminal_at_ms: int | None = None,
        result_json: str | None = None,
        error_json: str | None = None,
        resolution_json: str | None = None,
    ) -> RequestRecord:
        nonlocal quarantine_transitions
        if new_state is HostRequestState.QUARANTINED:
            assert expected_states == frozenset({HostRequestState.PUBLISHED})
            quarantine_transitions += 1
        return original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )

    def scripted_processes(command_exe: Path) -> tuple[ProcessInfo, ...]:
        nonlocal process_calls
        assert command_exe == harness.paths.command_exe
        process_calls += 1
        if process_calls == 1 or case == "timeout":
            return (harness.process,)
        if case == "process_zero":
            return ()
        if case == "process_multiple":
            return (harness.process, second)
        assert case == "process_replacement"
        return (replacement,)

    monkeypatch.setattr(mutation_module.uuid, "uuid4", fixed_uuid4)
    monkeypatch.setattr(mutation_module, "prepare_delivery", counted_prepare)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", counted_publish)
    monkeypatch.setattr(ResponseWaiter, "wait", counted_wait)
    monkeypatch.setattr(harness.inspector, "matching_processes", scripted_processes)
    monkeypatch.setattr(harness.ledger, "record_response", _forbidden)
    monkeypatch.setattr(harness.ledger, "transition", counted_transition)
    monkeypatch.setattr(harness.journals, "delete", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    loaded = None
    request: RequestRecord | None = None
    delivery: DeliveryRecord | None = None
    response_columns: tuple[object, ...] | None = None
    row_counts: tuple[int, int] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
        loaded = harness.journals.load()
        request = harness.ledger.get_request(command.request_id)
        delivery = harness.ledger.get_delivery(DELIVERY_ID)
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            raw_response_columns = connection.execute(
                """
                SELECT response_at_ms,response_sha256,response_size_bytes,
                       accepted_response_json,settlement_json
                FROM deliveries WHERE delivery_id=?
                """,
                (str(DELIVERY_ID),),
            ).fetchone()
            counts = connection.execute(
                "SELECT (SELECT COUNT(*) FROM requests),       (SELECT COUNT(*) FROM deliveries)"
            ).fetchone()
        assert raw_response_columns is not None and counts is not None
        response_columns = tuple(raw_response_columns)
        row_counts = cast(tuple[int, int], tuple(counts))

    assert caught is not None
    assert loaded is not None
    assert request is not None and delivery is not None
    assert loaded.journal.original.state is PendingPhase.QUARANTINED
    assert loaded.journal.original.revision == 2
    assert loaded.journal.original.response_artifact is None
    assert loaded.journal.original.settlement is None
    assert request.state is HostRequestState.QUARANTINED
    assert request.terminal_at_ms is None
    assert request.result_json is None
    assert request.resolution_json is None
    assert delivery.delivery_kind == "request"
    assert delivery.original_request_delivery_id == DELIVERY_ID
    assert delivery.published_at_ms is not None
    assert delivery.response_artifact is None
    assert delivery.settlement is None
    assert response_columns == (None, None, None, None, None)
    assert row_counts == (1, 1)

    assert uuid_calls == prepare_calls == publish_calls == waiter_calls == 1
    assert process_calls == 2
    assert quarantine_transitions == 1
    assert len(published_deliveries) == 1
    assert published_deliveries[0].delivery_id == DELIVERY_ID
    assert len(waiter_expectations) == 1
    assert waiter_expectations[0].allowed_deliveries == (
        AllowedDelivery(delivery_id=DELIVERY_ID, delivery_kind="request"),
    )
    assert cleanup == []
    assert exchange._requires_fresh_session() is True  # pyright: ignore[reportPrivateUsage]
    state = exchange._current_state  # pyright: ignore[reportPrivateUsage]
    assert state is not None
    assert state.publication_boundary == "waiter"
    assert state.artifact_observed is False
    assert state.response_artifact is None
    assert state.response_delivery is None
    assert state.safety_finished is True
    assert not response_path.exists()
    assert harness.paths.pending_file.exists()
    assert harness.paths.inbox.read_bytes() == render_delivery_lua(
        published_deliveries[0], harness.snapshot
    )

    assert caught.value.to_payload() == expected_error_payload
    assert request.error_json == expected_error_json
    assert len(waiter_errors) == 1
    if case == "timeout":
        timeout = caught.value.__cause__
        assert isinstance(timeout, BridgeError)
        assert timeout is waiter_errors[0]
        assert timeout.code is ErrorCode.REQUEST_TIMEOUT
        assert timeout.message == "timed out waiting for a correlated CMO response"
        assert timeout.details == {
            "request_id": str(command.request_id),
            "response_path": str(response_path),
            "timeout_seconds": 0.0,
        }
    else:
        assert caught.value is waiter_errors[0]
        assert caught.value.__cause__ is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "clock_values",
    [
        (100_000_000, 100_000_000),
        (100_000_000, 99_000_000),
    ],
    ids=["stalled", "backward"],
)
async def test_stalled_or_backward_wall_clock_keeps_host_epochs_nondecreasing(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    clock_values: tuple[int, int],
) -> None:
    command = _command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    stop = _StopAfterPublication("epoch boundary reached")
    clock_calls = 0

    def fixed_time_ns() -> int:
        nonlocal clock_calls
        value = clock_values[clock_calls]
        clock_calls += 1
        return value

    class StopWaiter:
        def __init__(self, **kwargs: object) -> None:
            expectation = cast(ResponseExpectation, kwargs["expectation"])
            assert expectation.allowed_deliveries == (
                AllowedDelivery(delivery_id=DELIVERY_ID, delivery_kind="request"),
            )

        async def wait(self, timeout_seconds: float) -> ResponseArtifact:
            assert timeout_seconds == command.timeout
            raise stop

    monkeypatch.setattr(mutation_module.time, "time_ns", fixed_time_ns)
    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(mutation_module, "ResponseWaiter", StopWaiter)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    caught: pytest.ExceptionInfo[_StopAfterPublication] | None = None
    loaded = None
    async with harness.lock:
        with pytest.raises(_StopAfterPublication) as caught:
            await _exchange(harness).run(command)
        loaded = harness.journals.load()
        assert loaded is not None
    assert caught is not None
    assert loaded is not None
    assert caught.value is stop
    original = loaded.journal.original
    assert original.state is PendingPhase.PUBLISHED
    assert original.created_at_ms == original.updated_at_ms == 100
    intent = original.delivery_intents[0]
    assert intent.intended_at_ms == intent.published_at_ms == 100
    request = harness.ledger.get_request(command.request_id)
    delivery = harness.ledger.get_delivery(DELIVERY_ID)
    assert request is not None and delivery is not None
    assert request.created_at_ms == request.updated_at_ms == 100
    assert delivery.intended_at_ms == delivery.published_at_ms == 100
    assert clock_calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["zero", "multiple", "pid", "create_time", "executable_text"],
)
async def test_prepublication_process_failure_safely_rejects_and_deletes_owned_prepared_state(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    time_replacement = ProcessInfo(
        pid=harness.process.pid,
        create_time=math.nextafter(harness.process.create_time, math.inf),
        executable=harness.process.executable,
    )
    pid_replacement = ProcessInfo(
        pid=harness.process.pid + 1,
        create_time=harness.process.create_time,
        executable=harness.process.executable,
    )
    lexical_path = Path(str(harness.process.executable).upper())
    assert str(lexical_path) != str(harness.process.executable)
    executable_replacement = ProcessInfo(
        pid=harness.process.pid,
        create_time=harness.process.create_time,
        executable=lexical_path,
    )
    observations = {
        "zero": (),
        "multiple": (harness.process, time_replacement),
        "pid": (pid_replacement,),
        "create_time": (time_replacement,),
        "executable_text": (executable_replacement,),
    }[case]
    inspector = FixedInspector(observations)
    cleanup: list[Path] = []
    intents: list[DeliveryIntent] = []
    original_insert_delivery = harness.ledger.insert_delivery

    def traced_insert_delivery(intent: DeliveryIntent) -> None:
        intents.append(intent)
        original_insert_delivery(intent)

    monkeypatch.setattr(harness.ledger, "insert_delivery", traced_insert_delivery)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    exchange = _exchange(
        harness,
        process_inspector=inspector,
        queue_response_cleanup=cleanup.append,
    )
    command = _command(harness)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
        assert harness.journals.load() is None
    expected_code = {
        "zero": ErrorCode.CMO_NOT_RUNNING,
        "multiple": ErrorCode.MULTIPLE_CMO_INSTANCES,
        "pid": ErrorCode.STATE_CONFLICT,
        "create_time": ErrorCode.STATE_CONFLICT,
        "executable_text": ErrorCode.STATE_CONFLICT,
    }[case]
    assert caught is not None
    assert caught.value.code is expected_code
    request = harness.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.REJECTED
    assert request.error_json == _canonical_json(caught.value.to_payload()).decode("utf-8")
    assert request.terminal_at_ms == request.updated_at_ms
    assert inspector.calls == 1
    assert len(intents) == 1
    delivery = harness.ledger.get_delivery(intents[0].delivery_id)
    assert delivery is not None and delivery.published_at_ms is None
    assert inspector.command_exes == [harness.paths.command_exe]
    assert str(inspector.command_exes[0]) == str(harness.paths.command_exe)
    assert not harness.paths.inbox.exists()
    assert cleanup == []
    assert not exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["process_subclass", "hostile_fields", "ordinary_error"])
async def test_prepublication_process_invalid_return_or_ordinary_error_is_fixed_and_safely_rejected(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    injected = RuntimeError()
    actual: object = {
        "process_subclass": _ProcessSubclass(
            pid=harness.process.pid,
            create_time=harness.process.create_time,
            executable=harness.process.executable,
        ),
        "hostile_fields": ProcessInfo(
            pid=cast(int, True),
            create_time=cast(float, 1),
            executable=cast(Path, "Command.exe"),
        ),
        "ordinary_error": None,
    }[case]
    calls = 0

    class InjectedInspector:
        def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
            nonlocal calls
            assert command_exe == harness.paths.command_exe
            calls += 1
            if case == "ordinary_error":
                raise injected
            return cast(tuple[ProcessInfo, ...], (actual,))

    command = _command(harness)
    cleanup: list[Path] = []
    intents: list[DeliveryIntent] = []
    original_insert_delivery = harness.ledger.insert_delivery

    def traced_insert_delivery(intent: DeliveryIntent) -> None:
        intents.append(intent)
        original_insert_delivery(intent)

    monkeypatch.setattr(mutation_module.uuid, "uuid4", lambda: DELIVERY_ID)
    monkeypatch.setattr(harness.ledger, "insert_delivery", traced_insert_delivery)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    exchange = _exchange(
        harness,
        process_inspector=InjectedInspector(),
        queue_response_cleanup=cleanup.append,
    )
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
        assert harness.journals.load() is None
    assert caught is not None
    assert caught.value.code is ErrorCode.STATE_CONFLICT
    if case == "ordinary_error":
        assert caught.value.message == "CMO process inspection failed"
        assert caught.value.details == {"type": "RuntimeError"}
        assert caught.value.__cause__ is injected
    else:
        assert caught.value.message == ("CMO process inspection returned invalid process identity")
        assert caught.value.details == {"type": type(actual).__name__}
        assert caught.value.__cause__ is None
    request = harness.ledger.get_request(command.request_id)
    delivery = harness.ledger.get_delivery(DELIVERY_ID)
    assert request is not None and delivery is not None
    assert request.state is HostRequestState.REJECTED
    assert request.error_json == _canonical_json(caught.value.to_payload()).decode("utf-8")
    assert request.terminal_at_ms == request.updated_at_ms
    assert delivery.published_at_ms is None
    assert len(intents) == 1
    assert calls == 1
    assert not harness.paths.inbox.exists()
    assert cleanup == []
    assert not exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["journal_r0", "request", "intent"])
@pytest.mark.parametrize("commit", ["precommit", "aftercommit"])
async def test_prepublication_failure_converges_rejection_without_idle(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    commit: str,
) -> None:
    command = _command(harness)
    injected = BridgeError(ErrorCode.STATE_CONFLICT, f"{boundary} injected")
    cleanup: list[Path] = []
    exchange = _exchange(harness, queue_response_cleanup=cleanup.append)
    original_save = harness.journals.save
    original_request = harness.ledger.insert_prepared
    original_intent = harness.ledger.insert_delivery
    observed_intent: DeliveryIntent | None = None

    def save_then_raise(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        if boundary == "journal_r0" and journal.original.state is PendingPhase.PREPARED:
            if commit == "aftercommit":
                original_save(journal, expected_revisions=expected_revisions)
            raise injected
        return original_save(journal, expected_revisions=expected_revisions)

    def request_then_raise(record: RequestRecord) -> None:
        if boundary == "request":
            if commit == "aftercommit":
                original_request(record)
            raise injected
        original_request(record)

    def intent_then_raise(intent: DeliveryIntent) -> None:
        nonlocal observed_intent
        observed_intent = intent
        if boundary == "intent":
            if commit == "aftercommit":
                original_intent(intent)
            raise injected
        original_intent(intent)

    monkeypatch.setattr(harness.journals, "save", save_then_raise)
    monkeypatch.setattr(harness.ledger, "insert_prepared", request_then_raise)
    monkeypatch.setattr(harness.ledger, "insert_delivery", intent_then_raise)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
        assert harness.journals.load() is None
    assert caught is not None
    assert caught.value is injected
    request = harness.ledger.get_request(command.request_id)
    if boundary == "journal_r0" or (boundary == "request" and commit == "precommit"):
        assert request is None
    else:
        assert request is not None
        assert request.state is HostRequestState.REJECTED
        assert request.error_json == _canonical_json(injected.to_payload()).decode("utf-8")
        assert request.terminal_at_ms == request.updated_at_ms
    delivery = (
        None
        if observed_intent is None
        else harness.ledger.get_delivery(observed_intent.delivery_id)
    )
    assert (delivery is not None) is (boundary == "intent" and commit == "aftercommit")
    assert not harness.paths.inbox.exists()
    assert cleanup == []
    assert not exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "drift"),
    [
        ("operation_class", OperationClass.DESTRUCTIVE),
        ("result_schema_id", "e" * 64),
        ("recovery_schema_id", "e" * 64),
        ("body_json", b"{}"),
        ("lineage_id", UUID("66666666-6666-4666-8666-666666666666")),
        ("activation_id", UUID("77777777-7777-4777-8777-777777777777")),
        ("created_at_ms", 101),
    ],
)
async def test_prepublication_safety_never_rejects_or_deletes_when_owned_request_bytes_drift(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    drift: object,
) -> None:
    command = _command(harness)
    injected = BridgeError(ErrorCode.STATE_CONFLICT, "request insert after-commit failure")
    exchange = _exchange(harness)
    original_insert = harness.ledger.insert_prepared
    original_get = harness.ledger.get_request
    forged: RequestRecord | None = None

    def insert_then_raise(record: RequestRecord) -> None:
        nonlocal forged
        original_insert(record)
        tree = record.model_dump(mode="python", round_trip=True, warnings=False)
        tree[field] = drift
        if field == "created_at_ms":
            tree["updated_at_ms"] = drift
        forged = RequestRecord.model_validate(tree)
        raise injected

    def drifted_get(request_id: UUID) -> RequestRecord | None:
        if forged is not None:
            assert request_id == command.request_id
            return forged
        return original_get(request_id)

    monkeypatch.setattr(harness.ledger, "insert_prepared", insert_then_raise)
    monkeypatch.setattr(harness.ledger, "get_request", drifted_get)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
        loaded = harness.journals.load()
        assert loaded is not None
        assert loaded.journal.original.state is PendingPhase.PREPARED
        assert loaded.journal.original.revision == 0
    assert caught is not None
    assert caught.value is injected
    assert any("ownership" in note for note in getattr(caught.value, "__notes__", ()))
    durable = original_get(command.request_id)
    assert durable is not None
    assert durable.state is HostRequestState.PREPARED
    assert durable.error_json is None
    assert exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
async def test_prepublication_real_prepared_timestamp_drift_is_preserved_with_revision_zero(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command(harness)
    injected = BridgeError(ErrorCode.STATE_CONFLICT, "intent after timestamp drift")
    exchange = _exchange(harness)
    original_intent = harness.ledger.insert_delivery
    original_transition = harness.ledger.transition
    observed_intent: DeliveryIntent | None = None
    drifted_at_ms: int | None = None

    def insert_drift_then_raise(intent: DeliveryIntent) -> None:
        nonlocal observed_intent, drifted_at_ms
        observed_intent = intent
        original_intent(intent)
        request = harness.ledger.get_request(command.request_id)
        assert request is not None
        drifted_at_ms = request.updated_at_ms + 1
        original_transition(
            command.request_id,
            expected_states=frozenset({HostRequestState.PREPARED}),
            new_state=HostRequestState.PREPARED,
            updated_at_ms=drifted_at_ms,
        )
        raise injected

    monkeypatch.setattr(harness.ledger, "insert_delivery", insert_drift_then_raise)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
        loaded = harness.journals.load()
        assert loaded is not None
        assert loaded.journal.original.state is PendingPhase.PREPARED
        assert loaded.journal.original.revision == 0
    assert caught is not None
    assert caught.value is injected
    request = harness.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.PREPARED
    assert request.updated_at_ms == drifted_at_ms
    assert request.error_json is None
    assert observed_intent is not None
    delivery = harness.ledger.get_delivery(observed_intent.delivery_id)
    assert delivery is not None and delivery.published_at_ms is None
    assert exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
async def test_prepublication_request_missing_after_insert_return_is_drift_and_preserves_revision_zero(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command(harness)
    injected = BridgeError(ErrorCode.STATE_CONFLICT, "intent failed after request returned")
    exchange = _exchange(harness)
    original_insert = harness.ledger.insert_prepared
    original_get = harness.ledger.get_request
    request_returned = False

    def traced_insert(record: RequestRecord) -> None:
        nonlocal request_returned
        original_insert(record)
        request_returned = True

    def missing_after_return(request_id: UUID) -> RequestRecord | None:
        if request_returned:
            assert request_id == command.request_id
            return None
        return original_get(request_id)

    def fail_intent(_intent: DeliveryIntent) -> None:
        raise injected

    monkeypatch.setattr(harness.ledger, "insert_prepared", traced_insert)
    monkeypatch.setattr(harness.ledger, "get_request", missing_after_return)
    monkeypatch.setattr(harness.ledger, "insert_delivery", fail_intent)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
        loaded = harness.journals.load()
        assert loaded is not None
        assert loaded.journal.original.state is PendingPhase.PREPARED
        assert loaded.journal.original.revision == 0
    assert caught is not None
    assert caught.value is injected
    durable = original_get(command.request_id)
    assert durable is not None and durable.state is HostRequestState.PREPARED
    assert exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
async def test_prepublication_journal_missing_after_revision_zero_return_is_drift(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command(harness)
    injected = BridgeError(ErrorCode.STATE_CONFLICT, "request returned after journal disappeared")
    exchange = _exchange(harness)
    original_insert = harness.ledger.insert_prepared

    def insert_remove_journal_then_raise(record: RequestRecord) -> None:
        original_insert(record)
        harness.paths.pending_file.unlink()
        raise injected

    monkeypatch.setattr(
        harness.ledger,
        "insert_prepared",
        insert_remove_journal_then_raise,
    )
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
        assert harness.journals.load() is None
    assert caught is not None
    assert caught.value is injected
    request = harness.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.PREPARED
    assert request.error_json is None
    assert exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["predecessor", "aftercommit", "drift"])
async def test_prepublication_rejection_transition_is_reread_without_retry(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    command = _command(harness)
    inspector = FixedInspector(())
    exchange = _exchange(harness, process_inspector=inspector)
    injected = BridgeError(ErrorCode.STATE_CONFLICT, f"rejection {mode}")
    original_transition = harness.ledger.transition
    rejection_calls = 0

    def failing_rejection(request_id: UUID, **kwargs: object) -> RequestRecord:
        nonlocal rejection_calls
        if kwargs["new_state"] is not HostRequestState.REJECTED:
            return cast(Any, original_transition)(request_id, **kwargs)
        rejection_calls += 1
        if mode == "predecessor":
            raise injected
        target = dict(kwargs)
        if mode == "drift":
            target["updated_at_ms"] = cast(int, target["updated_at_ms"]) + 1
            target["terminal_at_ms"] = cast(int, target["terminal_at_ms"]) + 1
        cast(Any, original_transition)(request_id, **target)
        raise injected

    monkeypatch.setattr(harness.ledger, "transition", failing_rejection)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    loaded = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
        loaded = harness.journals.load()
    assert caught is not None
    assert caught.value.code is ErrorCode.CMO_NOT_RUNNING
    assert rejection_calls == 1
    request = harness.ledger.get_request(command.request_id)
    assert request is not None
    if mode == "aftercommit":
        assert loaded is None
        assert request.state is HostRequestState.REJECTED
        assert not exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]
    elif mode == "predecessor":
        assert loaded is not None and loaded.journal.original.state is PendingPhase.PREPARED
        assert request.state is HostRequestState.PREPARED
        assert exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]
    else:
        assert loaded is not None and loaded.journal.original.state is PendingPhase.PREPARED
        assert request.state is HostRequestState.REJECTED
        assert exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]
    assert any("rejection" in note for note in getattr(caught.value, "__notes__", ()))


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["bridge", "ordinary"])
async def test_entered_publisher_error_is_fixed_indeterminate_and_preserves_revision_zero_and_slots(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    command = _command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    injected: Exception = (
        BridgeError(ErrorCode.STATE_CONFLICT, "publisher raised after replace")
        if kind == "bridge"
        else RuntimeError("ordinary publisher failure after replace")
    )
    response_path = harness.paths.response_path(command.request_id)
    response_sentinel = b"ambiguous-publisher-response-slot"
    response_mtime: int | None = None
    cleanup: list[Path] = []
    exchange = _exchange(harness, queue_response_cleanup=cleanup.append)
    original_publish = harness.inbox.publish_delivery
    publish_calls = 0
    uuid_calls = 0

    def fixed_uuid4() -> UUID:
        nonlocal uuid_calls
        uuid_calls += 1
        return DELIVERY_ID

    def publish_then_raise(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal publish_calls, response_mtime
        assert publisher is harness.inbox
        publish_calls += 1
        original_publish(cast(Any, delivery), runtime_snapshot=runtime_snapshot)
        response_path.write_bytes(response_sentinel)
        response_mtime = response_path.stat().st_mtime_ns
        raise injected

    monkeypatch.setattr(mutation_module.uuid, "uuid4", fixed_uuid4)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", publish_then_raise)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
        loaded = harness.journals.load()
        assert loaded is not None
        original = loaded.journal.original
        assert original.state is PendingPhase.PREPARED
        assert original.revision == 0
        assert original.delivery_intents[0].published_at_ms is None
    assert caught is not None
    assert caught.value.code is ErrorCode.INDETERMINATE_OUTCOME
    assert caught.value.message == "mutation publication outcome is indeterminate"
    assert caught.value.details == {
        "request_id": str(command.request_id),
        "reason": "publication_outcome_unknown",
    }
    assert caught.value.__cause__ is injected
    request = harness.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.PREPARED
    delivery = harness.ledger.get_delivery(DELIVERY_ID)
    assert delivery is not None and delivery.published_at_ms is None
    expected_delivery = prepare_delivery(
        command.body,
        request_id=command.request_id,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
    )
    assert harness.paths.inbox.read_bytes() == render_delivery_lua(
        expected_delivery,
        command.runtime_snapshot,
    )
    assert response_path.read_bytes() == response_sentinel
    assert response_path.stat().st_mtime_ns == response_mtime
    assert publish_calls == uuid_calls == 1
    assert cleanup == []
    assert exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "expected_phase", "expected_revision"),
    [
        ("precommit", PendingPhase.PREPARED, 0),
        ("returned_revision_drift", PendingPhase.PREPARED, 0),
        ("aftercommit", PendingPhase.QUARANTINED, 2),
        ("different_candidate", PendingPhase.PUBLISHED, 1),
        ("missing", None, None),
    ],
)
async def test_publication_journal_failure_rereads_exact_prior_candidate_or_drift_without_retry(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    expected_phase: PendingPhase | None,
    expected_revision: int | None,
) -> None:
    command = _command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    injected = BridgeError(ErrorCode.STATE_CONFLICT, f"journal r1 {mode}")
    cleanup: list[Path] = []
    exchange = _exchange(harness, queue_response_cleanup=cleanup.append)
    original_save = harness.journals.save
    original_load = harness.journals.load
    original_insert_delivery = harness.ledger.insert_delivery
    original_publish = harness.inbox.publish_delivery
    boundary_finished = False
    save_calls = 0
    quarantine_saves = 0
    post_failure_loads = 0
    delivery_inserts = 0
    publish_calls = 0
    uuid_calls = 0

    def fixed_uuid4() -> UUID:
        nonlocal uuid_calls
        uuid_calls += 1
        return DELIVERY_ID

    def traced_load() -> object:
        nonlocal post_failure_loads
        if boundary_finished:
            post_failure_loads += 1
        return original_load()

    def failing_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        nonlocal boundary_finished, quarantine_saves, save_calls
        if journal.original.state is PendingPhase.QUARANTINED:
            quarantine_saves += 1
            return original_save(journal, expected_revisions=expected_revisions)
        if journal.original.state is not PendingPhase.PUBLISHED:
            return original_save(journal, expected_revisions=expected_revisions)
        save_calls += 1
        if mode == "precommit":
            boundary_finished = True
            raise injected
        if mode == "returned_revision_drift":
            boundary_finished = True
            return JournalRevisions(original=0, reconcile_attempt=None)
        if mode == "aftercommit":
            original_save(journal, expected_revisions=expected_revisions)
            boundary_finished = True
            raise injected
        if mode == "missing":
            harness.paths.pending_file.unlink()
            boundary_finished = True
            raise injected
        intent = journal.original.delivery_intents[0]
        assert intent.published_at_ms is not None
        drift_epoch = intent.published_at_ms + 1
        drift_intent = DeliveryIntent.model_validate(
            {
                **intent.model_dump(mode="python", round_trip=True, warnings=False),
                "published_at_ms": drift_epoch,
            }
        )
        drift = _journal_with_original(
            journal,
            delivery_intents=(drift_intent,),
            updated_at_ms=drift_epoch,
        )
        original_save(drift, expected_revisions=expected_revisions)
        boundary_finished = True
        raise injected

    def traced_insert_delivery(intent: DeliveryIntent) -> None:
        nonlocal delivery_inserts
        delivery_inserts += 1
        original_insert_delivery(intent)

    def traced_publish(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal publish_calls
        assert publisher is harness.inbox
        publish_calls += 1
        original_publish(cast(Any, delivery), runtime_snapshot=runtime_snapshot)

    monkeypatch.setattr(mutation_module.uuid, "uuid4", fixed_uuid4)
    monkeypatch.setattr(harness.journals, "load", traced_load)
    monkeypatch.setattr(harness.journals, "save", failing_save)
    monkeypatch.setattr(harness.ledger, "insert_delivery", traced_insert_delivery)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(mutation_module, "ResponseWaiter", _forbidden)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    loaded = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
        loaded = original_load()
    assert caught is not None
    if mode == "returned_revision_drift":
        assert caught.value.code is ErrorCode.STATE_CONFLICT
        assert caught.value.message == "published mutation journal save returned drift"
    else:
        assert caught.value is injected, repr(caught.value.__cause__)
    assert post_failure_loads >= 1
    assert save_calls == 1
    assert quarantine_saves == (1 if mode == "aftercommit" else 0)
    assert uuid_calls == delivery_inserts == publish_calls == 1
    if expected_phase is None:
        assert loaded is None
    else:
        assert loaded is not None
        original = loaded.journal.original
        assert original.state is expected_phase
        assert original.revision == expected_revision
    request = harness.ledger.get_request(command.request_id)
    delivery = harness.ledger.get_delivery(DELIVERY_ID)
    assert request is not None and delivery is not None
    if expected_phase in {None, PendingPhase.PREPARED}:
        assert request.state is HostRequestState.PREPARED
        assert delivery.published_at_ms is None
    elif expected_phase is PendingPhase.QUARANTINED:
        assert request.state is HostRequestState.QUARANTINED
        assert request.error_json == _canonical_json(injected.to_payload()).decode("utf-8")
        assert request.terminal_at_ms is None
        assert delivery.published_at_ms is None
    else:
        assert request.state is HostRequestState.PREPARED
        assert delivery.published_at_ms is None
    assert harness.paths.inbox.exists()
    assert cleanup == []
    assert exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["delivery_marker", "request_published"])
@pytest.mark.parametrize(
    "mode",
    ["predecessor", "aftercommit", "drift", "returned_forgery"],
)
async def test_publication_sqlite_failure_rereads_exact_target_predecessor_or_drift_and_quarantines_journal_first(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    mode: str,
) -> None:
    command = _command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    injected = BridgeError(ErrorCode.STATE_CONFLICT, f"{boundary} {mode}")
    cleanup: list[Path] = []
    exchange = _exchange(harness, queue_response_cleanup=cleanup.append)
    original_mark = harness.ledger.mark_delivery_published
    original_transition = harness.ledger.transition
    original_journal_save = harness.journals.save
    original_get_delivery = harness.ledger.get_delivery
    original_get_request = harness.ledger.get_request
    original_insert_delivery = harness.ledger.insert_delivery
    original_publish = harness.inbox.publish_delivery
    boundary_finished = False
    marker_calls = 0
    published_transition_calls = 0
    quarantine_transition_calls = 0
    post_delivery_reads = 0
    post_request_reads = 0
    delivery_inserts = 0
    publish_calls = 0
    uuid_calls = 0
    trace: list[str] = []

    def fixed_uuid4() -> UUID:
        nonlocal uuid_calls
        uuid_calls += 1
        return DELIVERY_ID

    def failing_mark(delivery_id: UUID, *, published_at_ms: int) -> DeliveryRecord:
        nonlocal boundary_finished, marker_calls
        marker_calls += 1
        if boundary != "delivery_marker":
            return original_mark(delivery_id, published_at_ms=published_at_ms)
        if mode == "predecessor":
            boundary_finished = True
            raise injected
        committed = original_mark(
            delivery_id,
            published_at_ms=published_at_ms + (1 if mode == "drift" else 0),
        )
        boundary_finished = True
        if mode == "returned_forgery":
            tree = committed.model_dump(mode="python", round_trip=True, warnings=False)
            tree["published_at_ms"] = published_at_ms + 1
            return DeliveryRecord.model_validate(tree)
        raise injected

    def failing_transition(request_id: UUID, **kwargs: object) -> RequestRecord:
        nonlocal boundary_finished, published_transition_calls, quarantine_transition_calls
        new_state = cast(HostRequestState, kwargs["new_state"])
        if new_state is HostRequestState.QUARANTINED:
            quarantine_transition_calls += 1
            trace.append("ledger.request_quarantined")
            return cast(Any, original_transition)(request_id, **kwargs)
        assert new_state is HostRequestState.PUBLISHED
        published_transition_calls += 1
        if boundary != "request_published":
            return cast(Any, original_transition)(request_id, **kwargs)
        if mode == "predecessor":
            boundary_finished = True
            raise injected
        target = dict(kwargs)
        if mode == "drift":
            target["updated_at_ms"] = cast(int, target["updated_at_ms"]) + 1
        committed = cast(RequestRecord, cast(Any, original_transition)(request_id, **target))
        boundary_finished = True
        if mode == "returned_forgery":
            tree = committed.model_dump(mode="python", round_trip=True, warnings=False)
            tree["updated_at_ms"] = cast(int, tree["updated_at_ms"]) + 1
            return RequestRecord.model_validate(tree)
        raise injected

    def traced_get_delivery(delivery_id: UUID) -> DeliveryRecord | None:
        nonlocal post_delivery_reads
        if boundary_finished:
            post_delivery_reads += 1
            trace.append("ledger.delivery_reread")
        return original_get_delivery(delivery_id)

    def traced_get_request(request_id: UUID) -> RequestRecord | None:
        nonlocal post_request_reads
        if boundary_finished:
            post_request_reads += 1
            trace.append("ledger.request_reread")
        return original_get_request(request_id)

    def traced_insert_delivery(intent: DeliveryIntent) -> None:
        nonlocal delivery_inserts
        delivery_inserts += 1
        original_insert_delivery(intent)

    def traced_publish(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal publish_calls
        assert publisher is harness.inbox
        publish_calls += 1
        original_publish(cast(Any, delivery), runtime_snapshot=runtime_snapshot)

    def traced_journal_save(
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        trace.append(f"journal.{journal.original.state.value}")
        return original_journal_save(
            journal,
            expected_revisions=expected_revisions,
        )

    monkeypatch.setattr(mutation_module.uuid, "uuid4", fixed_uuid4)
    monkeypatch.setattr(harness.ledger, "mark_delivery_published", failing_mark)
    monkeypatch.setattr(harness.ledger, "transition", failing_transition)
    monkeypatch.setattr(harness.ledger, "get_delivery", traced_get_delivery)
    monkeypatch.setattr(harness.ledger, "get_request", traced_get_request)
    monkeypatch.setattr(harness.ledger, "insert_delivery", traced_insert_delivery)
    monkeypatch.setattr(harness.journals, "save", traced_journal_save)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(mutation_module, "ResponseWaiter", _forbidden)

    caught: pytest.ExceptionInfo[BridgeError] | None = None
    loaded = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await exchange.run(command)
        loaded = harness.journals.load()
        assert loaded is not None
    assert caught is not None
    assert loaded is not None
    if mode == "returned_forgery":
        assert caught.value.code is ErrorCode.STATE_CONFLICT
        assert (
            caught.value.message
            == {
                "delivery_marker": "published delivery marker returned drift",
                "request_published": "published request transition returned drift",
            }[boundary]
        )
    else:
        assert caught.value is injected
    assert loaded.journal.original.state is PendingPhase.QUARANTINED
    assert loaded.journal.original.revision == 2
    published_epoch = loaded.journal.original.delivery_intents[0].published_at_ms
    assert published_epoch is not None
    delivery = original_get_delivery(DELIVERY_ID)
    request = original_get_request(command.request_id)
    assert delivery is not None and request is not None
    if boundary == "delivery_marker":
        assert published_transition_calls == 0
        assert quarantine_transition_calls == 1
        assert request.state is HostRequestState.QUARANTINED
        assert request.error_json == _canonical_json(caught.value.to_payload()).decode("utf-8")
        expected_marker = None if mode == "predecessor" else published_epoch
        if mode == "drift":
            expected_marker = published_epoch + 1
        assert delivery.published_at_ms == expected_marker
    else:
        assert delivery.published_at_ms == published_epoch
        assert published_transition_calls == 1
        if mode == "drift":
            assert quarantine_transition_calls == 0
            assert request.state is HostRequestState.PUBLISHED
            assert request.updated_at_ms == published_epoch + 1
            assert request.error_json is None
        else:
            assert quarantine_transition_calls == 1
            assert request.state is HostRequestState.QUARANTINED
            assert request.error_json == _canonical_json(caught.value.to_payload()).decode("utf-8")
    assert marker_calls == 1
    assert post_delivery_reads >= 1
    assert post_request_reads >= 1
    assert uuid_calls == delivery_inserts == publish_calls == 1
    quarantine_index = trace.index("journal.quarantined")
    assert quarantine_index < trace.index("ledger.delivery_reread")
    assert quarantine_index < trace.index("ledger.request_reread")
    if quarantine_transition_calls:
        assert quarantine_index < trace.index("ledger.request_quarantined")
    assert request.terminal_at_ms is None
    assert harness.paths.inbox.exists()
    assert not harness.paths.response_path(command.request_id).exists()
    assert cleanup == []
    assert exchange._requires_fresh_session()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
async def test_status_is_rejected_before_duplicate_check(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command(
        harness,
        "bridge.status",
        {},
        trusted_enrichment={"activation_candidate": UUID("55555555-5555-4555-8555-555555555555")},
    )

    def forbidden(_request_id: UUID) -> None:
        raise AssertionError("invalid status reached SQLite")

    monkeypatch.setattr(harness.ledger, "get_request", forbidden)
    caught: pytest.ExceptionInfo[BridgeError] | None = None
    async with harness.lock:
        with pytest.raises(BridgeError) as caught:
            await _exchange(harness).run(command)
    assert caught is not None
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.inbox.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "arguments", "trusted"),
    [
        ("unit.set", {"unit_guid": "UNIT-1", "name": "Renamed"}, None),
        (
            "unit.delete",
            {"unit_guid": "UNIT-1"},
            {"confirmation_proof": "f" * 64},
        ),
        (
            "compat.probe.step",
            {
                "step": "apply-profile",
                "safe_payload_bytes": 4096,
                "verified_ledger_entries": 32,
                "effective_ledger_capacity": 32,
            },
            None,
        ),
    ],
)
async def test_channel_constructs_one_runner_from_exact_h8_products_and_uses_only_existing_active_guard(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    arguments: dict[str, object],
    trusted: dict[str, object] | None,
) -> None:
    original_init = MutationExchange.__init__
    original_recovery_init = RecoveryManager.__init__
    original_enter = RootLock.__aenter__
    constructor_calls: list[dict[str, object]] = []
    recovery_constructor_calls: list[tuple[RecoveryManager, dict[str, object]]] = []
    lock_enters = 0

    def captured_init(self: MutationExchange, **kwargs: object) -> None:
        constructor_calls.append(dict(kwargs))
        cast(Any, original_init)(self, **kwargs)

    def captured_recovery_init(self: RecoveryManager, **kwargs: object) -> None:
        recovery_constructor_calls.append((self, dict(kwargs)))
        cast(Any, original_recovery_init)(self, **kwargs)

    async def counted_enter(lock: RootLock) -> RootLock:
        nonlocal lock_enters
        lock_enters += 1
        return await original_enter(lock)

    monkeypatch.setattr(RecoveryManager, "__init__", captured_recovery_init)
    monkeypatch.setattr(MutationExchange, "__init__", captured_init)
    monkeypatch.setattr(RootLock, "__aenter__", counted_enter)
    transport = FileBridgeTransport(
        paths=harness.paths,
        root_lock=harness.lock,
        process_inspector=harness.inspector,
        catalog=harness.catalog,
        database=harness.database,
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
        response_poll_seconds=0.001,
    )
    command = _command(
        harness,
        operation,
        arguments,
        trusted_enrichment=trusted,
    )
    sentinel = BridgeError(ErrorCode.STATE_CONFLICT, "mutation dispatch sentinel")
    calls: list[ExchangeCommand] = []
    channels: list[_FileBridgeChannel] = []

    async def intercepted(self: MutationExchange, observed: ExchangeCommand) -> ResponseArtifact:
        del self
        calls.append(observed)
        channel = channels[0]
        harness.lock.require_acquired()
        assert channel._exchange_state is None  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(BridgeError) as reentrant:
            await channel.exchange(observed)
        assert reentrant.value.code is ErrorCode.STATE_CONFLICT
        raise sentinel

    monkeypatch.setattr(MutationExchange, "run", intercepted)
    async with transport.session() as channel:
        concrete = cast(_FileBridgeChannel, channel)
        channels.append(concrete)
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(command)
    assert caught.value is sentinel
    assert calls == [command]
    assert len(constructor_calls) == 1
    assert len(recovery_constructor_calls) == 1
    products = constructor_calls[0]
    recovery_manager, recovery_products = recovery_constructor_calls[0]
    assert set(products) == {
        "paths",
        "root_lock",
        "process_inspector",
        "expected_process",
        "catalog",
        "journals",
        "ledger",
        "inbox",
        "response_poll_seconds",
        "queue_response_cleanup",
        "recovery_manager",
    }
    assert set(recovery_products) == {
        "paths",
        "root_lock",
        "process_inspector",
        "expected_process",
        "journals",
        "ledger",
        "inbox",
        "response_poll_seconds",
        "cancel_ack_timeout_seconds",
    }
    assert recovery_products["paths"] is transport.paths
    assert recovery_products["root_lock"] is transport.root_lock
    assert recovery_products["process_inspector"] is transport.process_inspector
    assert recovery_products["expected_process"] is harness.process
    assert recovery_products["journals"] is transport.journals
    assert recovery_products["ledger"] is transport.ledger
    assert recovery_products["inbox"] is transport.inbox
    assert recovery_products["response_poll_seconds"] == 0.001
    assert recovery_products["cancel_ack_timeout_seconds"] == 10.0
    assert products["paths"] is transport.paths
    assert products["root_lock"] is transport.root_lock
    assert products["process_inspector"] is transport.process_inspector
    assert products["expected_process"] is harness.process
    assert products["catalog"] is transport.catalog
    assert products["journals"] is transport.journals
    assert products["ledger"] is transport.ledger
    assert products["inbox"] is transport.inbox
    assert products["response_poll_seconds"] == 0.001
    assert products["recovery_manager"] is recovery_manager
    assert recovery_manager._is_bound_to(  # pyright: ignore[reportPrivateUsage]
        paths=transport.paths,
        root_lock=transport.root_lock,
        process_inspector=transport.process_inspector,
        expected_process=harness.process,
        journals=transport.journals,
        ledger=transport.ledger,
        inbox=transport.inbox,
        response_poll_seconds=0.001,
    )
    callback = cast(Callable[[Path], None], products["queue_response_cleanup"])
    assert getattr(callback, "__self__", None) is channels[0]._cleanup_paths  # pyright: ignore[reportPrivateUsage]
    assert getattr(callback, "__name__", None) == "append"
    assert lock_enters == 1
    assert harness.inspector.calls == 1
    assert channels[0]._exchange_state is None  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
async def test_channel_wires_mutation_poison_to_next_exchange_without_read_finalizer(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = FileBridgeTransport(
        paths=harness.paths,
        root_lock=harness.lock,
        process_inspector=harness.inspector,
        catalog=harness.catalog,
        database=harness.database,
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
        response_poll_seconds=0.001,
    )
    mutation = _command(harness)
    read = _command(harness, "scenario.get", {})
    sentinel = BridgeError(ErrorCode.STATE_CONFLICT, "unresolved mutation sentinel")

    async def unresolved(
        self: MutationExchange,
        _command_value: ExchangeCommand,
    ) -> ResponseArtifact:
        del self
        raise sentinel

    def always_poisoned(_self: MutationExchange) -> bool:
        return True

    monkeypatch.setattr(MutationExchange, "run", unresolved)
    monkeypatch.setattr(MutationExchange, "_requires_fresh_session", always_poisoned)
    monkeypatch.setattr(_FileBridgeChannel, "_finalize_state", _forbidden)
    async with transport.session() as channel:
        concrete = cast(_FileBridgeChannel, channel)
        with pytest.raises(BridgeError) as first:
            await channel.exchange(mutation)
        assert first.value is sentinel
        assert concrete._exchange_state is None  # pyright: ignore[reportPrivateUsage]
        monkeypatch.setattr(ManifestCatalog, "resolve_running", _forbidden)
        monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
        monkeypatch.setattr(RequestLedger, "insert_prepared", _forbidden)
        monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
        monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
        with pytest.raises(BridgeError) as second:
            await channel.exchange(read)
        assert second.value.code is ErrorCode.STATE_CONFLICT
        assert concrete._exchange_state is None  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase",
    [
        PendingPhase.PREPARED,
        PendingPhase.PUBLISHED,
        PendingPhase.RESPONSE_ACCEPTED,
        PendingPhase.IDLE_PUBLISHED,
        PendingPhase.QUARANTINED,
    ],
)
async def test_pending_blocker_poisons_composed_channel_before_second_exchange_side_effects(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    phase: PendingPhase,
) -> None:
    transport = FileBridgeTransport(
        paths=harness.paths,
        root_lock=harness.lock,
        process_inspector=harness.inspector,
        catalog=harness.catalog,
        database=harness.database,
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
        response_poll_seconds=0.001,
    )
    owner = _command(harness)
    attacker = _command(
        harness,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee9002"),
    )
    async with transport.session() as channel:
        concrete = cast(_FileBridgeChannel, channel)
        before = _persist_journal_at_phase(harness, owner, phase)
        before_mtime = harness.paths.pending_file.stat().st_mtime_ns
        with pytest.raises(BridgeError) as first:
            await channel.exchange(attacker)
        assert first.value.code is ErrorCode.MUTATION_QUARANTINED
        assert first.value.details == {
            "request_id": str(owner.request_id),
            "state": phase.value,
            "required_release_id": harness.snapshot.release_id,
        }
        monkeypatch.setattr(PendingJournalStore, "load", _forbidden)
        monkeypatch.setattr(ManifestCatalog, "resolve_running", _forbidden)
        monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
        monkeypatch.setattr(sqlite3, "connect", _forbidden)
        monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
        monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
        with pytest.raises(BridgeError) as second:
            await channel.exchange(attacker)
        assert second.value.code is ErrorCode.STATE_CONFLICT
        assert second.value.message == "file-bridge channel requires a fresh session"
        assert concrete._exchange_state is None  # pyright: ignore[reportPrivateUsage]
        assert concrete._cleanup_paths == []  # pyright: ignore[reportPrivateUsage]
        assert harness.paths.pending_file.read_bytes() == before
        assert harness.paths.pending_file.stat().st_mtime_ns == before_mtime
