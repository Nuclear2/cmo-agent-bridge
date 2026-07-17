from __future__ import annotations

import hashlib
import inspect
import math
import sqlite3
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any, Never, cast, get_type_hints
from uuid import UUID

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes, prepare_delivery
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.models import ExchangeCommand, RequestBody
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CompletedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    PendingExchange,
    PendingJournal,
    PendingJournalHeader,
    PendingPhase,
)
from cmo_agent_bridge.state.pending_journal import JournalRevisions, PendingJournalStore
from cmo_agent_bridge.state.request_ledger import RequestLedger
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.models import RecoveryReport
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.process_guard import (
    CmoProcessInspector,
    ProcessInfo,
)
from cmo_agent_bridge.transports.file_bridge.recovery import RecoveryManager


REQUEST_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeea101")
REQUEST_DELIVERY_ID = UUID("11111111-1111-4111-8111-11111111a101")
CANCEL_DELIVERY_ID = UUID("22222222-2222-4222-8222-22222222a101")
LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")


class Inspector:
    def __init__(self, process: ProcessInfo) -> None:
        self.process = process
        self.calls = 0

    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        assert command_exe == self.process.executable
        self.calls += 1
        return (self.process,)


class _AsyncInspector:
    async def matching_processes(self, _command_exe: Path) -> tuple[ProcessInfo, ...]:
        return ()


class _NonCallableInspector:
    matching_processes = None


class _IntSubclass(int):
    pass


class _FloatSubclass(float):
    pass


class _PathsSubclass(FileBridgePaths):
    pass


class _RootLockSubclass(RootLock):
    pass


class _JournalStoreSubclass(PendingJournalStore):
    pass


class _RequestLedgerSubclass(RequestLedger):
    pass


class _InboxPublisherSubclass(InboxPublisher):
    pass


class _ProcessInfoSubclass(ProcessInfo):
    pass


class _PendingJournalSubclass(PendingJournal):
    pass


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
    (game_root / "Command.exe").write_bytes(b"exe")
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


def _command(harness: Harness) -> ExchangeCommand:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "unit.set",
        {"unit_guid": "UNIT-1", "name": "Recovery contract"},
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
        operation="unit.set",
        arguments=invocation.wire_arguments.model_dump(mode="json"),
    )
    return ExchangeCommand(
        request_id=REQUEST_ID,
        body=body,
        invocation=invocation,
        runtime_snapshot=harness.snapshot,
        timeout=30,
    )


def _journal_with_original(journal: PendingJournal, **changes: object) -> PendingJournal:
    tree = journal.original.model_dump(mode="python", round_trip=True, warnings=False)
    tree.update(changes)
    return PendingJournal(
        header=journal.header,
        original=PendingExchange.model_validate(tree),
        reconcile_attempt=None,
    )


def _completed_artifact(command: ExchangeCommand) -> ResponseArtifact:
    result_input = {
        "unit_guid": "UNIT-1",
        "name": "Recovery contract",
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
        delivery_id=REQUEST_DELIVERY_ID,
        request_hash=hashlib.sha256(canonical_body_bytes(command.body)).hexdigest(),
        ok=True,
        result=result,
        error=None,
        scenario_time="2026-07-12T13:00:00Z",
        scenario_lineage_id=cast(UUID, command.body.expected_lineage_id),
        activation_id=cast(UUID, command.body.expected_activation_id),
        operation_manifest_sha256=command.runtime_snapshot.operation_manifest_sha256,
        bridge_version=command.runtime_snapshot.runtime_version,
        runtime_tag=command.runtime_snapshot.runtime_tag,
        runtime_asset_sha256=command.runtime_snapshot.runtime_asset_sha256,
        release_id=command.runtime_snapshot.release_id,
    )
    settlement = CompletedSettlement(state="completed", result=result)
    return ResponseArtifact(
        filename=f"CMOAgentBridge_Response_{command.request_id}.inst",
        sha256="e" * 64,
        size_bytes=512,
        accepted_at_ms=102,
        accepted_response=AcceptedResponse(
            envelope=envelope,
            delivery_kind="request",
            settlement=settlement,
            cancel_ack=None,
        ),
    )


def _precondition_journals(
    harness: Harness,
) -> tuple[PendingJournal, PendingJournal, PendingJournal, PendingJournal]:
    command = _command(harness)
    prepared_delivery = prepare_delivery(
        command.body,
        request_id=command.request_id,
        delivery_id=REQUEST_DELIVERY_ID,
        delivery_kind="request",
    )
    rendered_request = render_delivery_lua(prepared_delivery, command.runtime_snapshot)
    recovery_schema = command.invocation.recovery_schema
    request_intent = DeliveryIntent(
        request_id=command.request_id,
        delivery_id=REQUEST_DELIVERY_ID,
        delivery_kind="request",
        original_request_delivery_id=REQUEST_DELIVERY_ID,
        body_json=prepared_delivery.body_json.decode("utf-8"),
        request_hash=prepared_delivery.request_hash,
        runtime_snapshot=command.runtime_snapshot,
        result_schema_id=command.invocation.result_schema.schema_id,
        recovery_schema_id=None if recovery_schema is None else recovery_schema.schema_id,
        intended_at_ms=100,
        published_at_ms=None,
        rendered_inbox_sha256=hashlib.sha256(rendered_request).hexdigest(),
        rendered_inbox_size_bytes=len(rendered_request),
        response_filename=harness.paths.response_path(command.request_id).name,
    )
    r0 = PendingJournal(
        header=PendingJournalHeader(
            format="cmo-agent-bridge/pending-journal",
            header_version=1,
            root_key=harness.paths.root_key,
            required_release_id=command.runtime_snapshot.release_id,
        ),
        original=PendingExchange(
            request_id=command.request_id,
            request_hash=prepared_delivery.request_hash,
            operation=command.body.operation,
            effective_class=command.invocation.effective_class,
            body_json=prepared_delivery.body_json.decode("utf-8"),
            runtime_snapshot=command.runtime_snapshot,
            result_schema_id=command.invocation.result_schema.schema_id,
            recovery_schema_id=(None if recovery_schema is None else recovery_schema.schema_id),
            expected_lineage_id=command.body.expected_lineage_id,
            expected_activation_id=command.body.expected_activation_id,
            delivery_intents=(request_intent,),
            response_artifact=None,
            settlement=None,
            original_target_request_id=None,
            original_target_request_hash=None,
            revision=0,
            state=PendingPhase.PREPARED,
            created_at_ms=100,
            updated_at_ms=100,
        ),
        reconcile_attempt=None,
    )
    published_request = DeliveryIntent.model_validate(
        {
            **request_intent.model_dump(mode="python", round_trip=True, warnings=False),
            "published_at_ms": 101,
        }
    )
    r1 = _journal_with_original(
        r0,
        delivery_intents=(published_request,),
        revision=1,
        state=PendingPhase.PUBLISHED,
        updated_at_ms=101,
    )

    cancel_delivery = prepare_delivery(
        command.body,
        request_id=command.request_id,
        delivery_id=CANCEL_DELIVERY_ID,
        delivery_kind="cancel",
    )
    rendered_cancel = render_delivery_lua(cancel_delivery, command.runtime_snapshot)
    cancel_intent = DeliveryIntent(
        request_id=command.request_id,
        delivery_id=CANCEL_DELIVERY_ID,
        delivery_kind="cancel",
        original_request_delivery_id=REQUEST_DELIVERY_ID,
        body_json=cancel_delivery.body_json.decode("utf-8"),
        request_hash=cancel_delivery.request_hash,
        runtime_snapshot=command.runtime_snapshot,
        result_schema_id=command.invocation.result_schema.schema_id,
        recovery_schema_id=None if recovery_schema is None else recovery_schema.schema_id,
        intended_at_ms=102,
        published_at_ms=None,
        rendered_inbox_sha256=hashlib.sha256(rendered_cancel).hexdigest(),
        rendered_inbox_size_bytes=len(rendered_cancel),
        response_filename=harness.paths.response_path(command.request_id).name,
    )
    r2 = _journal_with_original(
        r1,
        delivery_intents=(published_request, cancel_intent),
        revision=2,
        state=PendingPhase.CANCEL_PUBLISHED,
        updated_at_ms=102,
    )

    artifact = _completed_artifact(command)
    response = _journal_with_original(
        r1,
        response_artifact=artifact,
        settlement=artifact.accepted_response.settlement,
        revision=2,
        state=PendingPhase.RESPONSE_ACCEPTED,
        updated_at_ms=102,
    )
    return r0, r1, r2, response


def _persist_journal_sequence(
    harness: Harness,
    sequence: tuple[PendingJournal, ...],
) -> None:
    for index, journal in enumerate(sequence):
        expected = (
            None
            if index == 0
            else JournalRevisions(
                original=sequence[index - 1].original.revision,
                reconcile_attempt=None,
            )
        )
        revisions = harness.journals.save(journal, expected_revisions=expected)
        assert revisions == JournalRevisions(
            original=journal.original.revision,
            reconcile_attempt=None,
        )


def _manager(harness: Harness, **overrides: object) -> RecoveryManager:
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


def _forbidden(*_args: object, **_kwargs: object) -> Never:
    raise AssertionError("RecoveryManager constructor performed forbidden I/O")


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
    }
    original_exists = Path.exists
    original_stat = Path.stat
    original_open = Path.open
    original_read_bytes = Path.read_bytes
    original_mkdir = Path.mkdir

    def guard(path: Path) -> None:
        if path in managed:
            raise AssertionError(f"RecoveryManager constructor touched managed path: {path}")

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
    monkeypatch.setattr(RequestLedger, "get_delivery", _forbidden)
    monkeypatch.setattr(RequestLedger, "insert_delivery", _forbidden)
    monkeypatch.setattr(RequestLedger, "mark_delivery_published", _forbidden)
    monkeypatch.setattr(RequestLedger, "transition", _forbidden)
    monkeypatch.setattr(RequestLedger, "record_response", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(Path, "exists", guarded_exists)
    monkeypatch.setattr(Path, "stat", guarded_stat)
    monkeypatch.setattr(Path, "open", guarded_open)
    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)
    monkeypatch.setattr(Path, "mkdir", guarded_mkdir)


def _paths_subclass(paths: FileBridgePaths) -> FileBridgePaths:
    values = {field.name: getattr(paths, field.name) for field in fields(FileBridgePaths)}
    return cast(Any, _PathsSubclass)(**values)


def test_recovery_manager_constructor_and_public_protocols_are_exact() -> None:
    constructor = inspect.signature(RecoveryManager.__init__)
    assert list(constructor.parameters) == [
        "self",
        "paths",
        "root_lock",
        "process_inspector",
        "expected_process",
        "journals",
        "ledger",
        "inbox",
        "response_poll_seconds",
        "cancel_ack_timeout_seconds",
    ]
    assert constructor.parameters["self"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        and parameter.default is inspect.Parameter.empty
        for name, parameter in constructor.parameters.items()
        if name != "self"
    )
    assert get_type_hints(RecoveryManager.__init__) == {
        "paths": FileBridgePaths,
        "root_lock": RootLock,
        "process_inspector": CmoProcessInspector,
        "expected_process": ProcessInfo,
        "journals": PendingJournalStore,
        "ledger": RequestLedger,
        "inbox": InboxPublisher,
        "response_poll_seconds": float,
        "cancel_ack_timeout_seconds": float,
        "return": type(None),
    }

    startup = inspect.signature(RecoveryManager.recover_pending)
    assert list(startup.parameters) == ["self"]
    assert startup.parameters["self"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert startup.parameters["self"].default is inspect.Parameter.empty
    assert inspect.iscoroutinefunction(RecoveryManager.recover_pending)
    assert get_type_hints(RecoveryManager.recover_pending) == {
        "return": RecoveryReport,
    }

    settlement = inspect.signature(RecoveryManager.settle_published_cancellation)
    assert list(settlement.parameters) == ["self", "expected_journal"]
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        and parameter.default is inspect.Parameter.empty
        for parameter in settlement.parameters.values()
    )
    assert inspect.iscoroutinefunction(RecoveryManager.settle_published_cancellation)
    assert get_type_hints(RecoveryManager.settle_published_cancellation) == {
        "expected_journal": PendingJournal,
        "return": type(None),
    }


@pytest.mark.parametrize(
    ("response_poll_seconds", "cancel_ack_timeout_seconds"),
    [(1, 0.25), (0.125, 2)],
)
def test_constructor_is_side_effect_free_and_accepts_exact_positive_int_and_float_numbers(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    response_poll_seconds: int | float,
    cancel_ack_timeout_seconds: int | float,
) -> None:
    with monkeypatch.context() as guarded:
        _install_constructor_tripwires(guarded, harness)
        manager = _manager(
            harness,
            response_poll_seconds=response_poll_seconds,
            cancel_ack_timeout_seconds=cancel_ack_timeout_seconds,
        )

    assert type(manager) is RecoveryManager
    assert manager._is_bound_to(  # pyright: ignore[reportPrivateUsage]
        paths=harness.paths,
        root_lock=harness.lock,
        process_inspector=harness.inspector,
        expected_process=harness.process,
        journals=harness.journals,
        ledger=harness.ledger,
        inbox=harness.inbox,
        response_poll_seconds=float(response_poll_seconds),
    )
    assert manager._cancel_ack_timeout_seconds == float(  # pyright: ignore[reportPrivateUsage]
        cancel_ack_timeout_seconds
    )
    assert harness.inspector.calls == 0
    assert not harness.paths.lock_file.exists()
    assert not harness.paths.pending_file.exists()
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.inbox.exists()


@pytest.mark.parametrize(
    "case",
    [
        "paths_subclass",
        "root_lock_subclass",
        "journals_subclass",
        "ledger_subclass",
        "inbox_subclass",
        "root_lock_wrong_path",
        "journal_paths_identity",
        "journal_lock_identity",
        "ledger_database_path",
        "ledger_catalog_identity",
        "inbox_paths_identity",
        "inspector_missing",
        "inspector_noncallable",
        "inspector_async",
        "process_subclass",
        "process_bool_pid",
        "process_nonpositive_pid",
        "process_int_create_time",
        "process_nonpositive_create_time",
        "process_nonfinite_create_time",
        "process_wrong_executable",
        "process_nonpath_executable",
    ],
)
def test_constructor_rejects_strict_dependencies_bindings_inspector_and_process_without_io(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    overrides: dict[str, object]
    if case == "paths_subclass":
        overrides = {"paths": _paths_subclass(harness.paths)}
    elif case == "root_lock_subclass":
        overrides = {"root_lock": _RootLockSubclass(harness.paths.lock_file, timeout_seconds=0)}
    elif case == "journals_subclass":
        overrides = {
            "journals": _JournalStoreSubclass(
                harness.paths,
                harness.lock,
                harness.catalog,
                max_journal_bytes=1_000_000,
                replace_retry_seconds=0,
            )
        }
    elif case == "ledger_subclass":
        overrides = {"ledger": _RequestLedgerSubclass(harness.database, harness.catalog)}
    elif case == "inbox_subclass":
        overrides = {"inbox": _InboxPublisherSubclass(harness.paths, 0)}
    elif case == "root_lock_wrong_path":
        overrides = {"root_lock": RootLock(harness.paths.pending_file, timeout_seconds=0)}
    elif case == "journal_paths_identity":
        cloned_paths = replace(harness.paths)
        overrides = {
            "journals": PendingJournalStore(
                cloned_paths,
                harness.lock,
                harness.catalog,
                max_journal_bytes=1_000_000,
                replace_retry_seconds=0,
            )
        }
    elif case == "journal_lock_identity":
        other_lock = RootLock(harness.paths.lock_file, timeout_seconds=0)
        overrides = {
            "journals": PendingJournalStore(
                harness.paths,
                other_lock,
                harness.catalog,
                max_journal_bytes=1_000_000,
                replace_retry_seconds=0,
            )
        }
    elif case == "ledger_database_path":
        overrides = {
            "ledger": RequestLedger(
                StateDatabase(harness.paths.sqlite_file.with_name("other.sqlite3")),
                harness.catalog,
            )
        }
    elif case == "ledger_catalog_identity":
        other_catalog = ManifestCatalog(
            ReleaseBinding(snapshot=harness.snapshot, registry=OPERATION_REGISTRY)
        )
        overrides = {"ledger": RequestLedger(harness.database, other_catalog)}
    elif case == "inbox_paths_identity":
        overrides = {"inbox": InboxPublisher(replace(harness.paths), 0)}
    elif case == "inspector_missing":
        overrides = {"process_inspector": object()}
    elif case == "inspector_noncallable":
        overrides = {"process_inspector": _NonCallableInspector()}
    elif case == "inspector_async":
        overrides = {"process_inspector": _AsyncInspector()}
    elif case == "process_subclass":
        overrides = {
            "expected_process": _ProcessInfoSubclass(
                pid=1234,
                create_time=1000.5,
                executable=harness.paths.command_exe,
            )
        }
    elif case == "process_bool_pid":
        overrides = {
            "expected_process": ProcessInfo(
                pid=cast(Any, True),
                create_time=1000.5,
                executable=harness.paths.command_exe,
            )
        }
    elif case == "process_nonpositive_pid":
        overrides = {
            "expected_process": ProcessInfo(
                pid=0,
                create_time=1000.5,
                executable=harness.paths.command_exe,
            )
        }
    elif case == "process_int_create_time":
        overrides = {
            "expected_process": ProcessInfo(
                pid=1234,
                create_time=cast(Any, 1000),
                executable=harness.paths.command_exe,
            )
        }
    elif case == "process_nonpositive_create_time":
        overrides = {
            "expected_process": ProcessInfo(
                pid=1234,
                create_time=0.0,
                executable=harness.paths.command_exe,
            )
        }
    elif case == "process_nonfinite_create_time":
        overrides = {
            "expected_process": ProcessInfo(
                pid=1234,
                create_time=math.inf,
                executable=harness.paths.command_exe,
            )
        }
    elif case == "process_wrong_executable":
        overrides = {
            "expected_process": ProcessInfo(
                pid=1234,
                create_time=1000.5,
                executable=harness.paths.command_exe.with_name("Command-copy.exe"),
            )
        }
    else:
        assert case == "process_nonpath_executable"
        overrides = {
            "expected_process": ProcessInfo(
                pid=1234,
                create_time=1000.5,
                executable=cast(Any, str(harness.paths.command_exe)),
            )
        }

    with monkeypatch.context() as guarded:
        _install_constructor_tripwires(guarded, harness)
        with pytest.raises(BridgeError) as caught:
            _manager(harness, **overrides)
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert harness.inspector.calls == 0
    assert not harness.paths.lock_file.exists()
    assert not harness.paths.pending_file.exists()
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.inbox.exists()


_INVALID_NUMBERS: tuple[tuple[str, object], ...] = (
    ("true", True),
    ("false", False),
    ("int_subclass", _IntSubclass(1)),
    ("float_subclass", _FloatSubclass(0.25)),
    ("zero", 0),
    ("negative", -1),
    ("nan", math.nan),
    ("positive_inf", math.inf),
    ("negative_inf", -math.inf),
    ("overflowing_int", 10**10_000),
)


@pytest.mark.parametrize(
    "name",
    ["response_poll_seconds", "cancel_ack_timeout_seconds"],
)
@pytest.mark.parametrize(
    ("_case", "value"),
    _INVALID_NUMBERS,
    ids=[case for case, _value in _INVALID_NUMBERS],
)
def test_constructor_rejects_invalid_exact_numbers_without_io(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    _case: str,
    value: object,
) -> None:
    with monkeypatch.context() as guarded:
        _install_constructor_tripwires(guarded, harness)
        with pytest.raises(BridgeError) as caught:
            _manager(harness, **{name: value})
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert harness.inspector.calls == 0
    assert not harness.paths.lock_file.exists()
    assert not harness.paths.pending_file.exists()
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.inbox.exists()


@pytest.mark.asyncio
async def test_settlement_requires_its_exact_root_lock_to_be_acquired(
    harness: Harness,
) -> None:
    manager = _manager(harness)

    with pytest.raises(BridgeError) as caught:
        await manager.settle_published_cancellation(cast(Any, object()))

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.message == "the root lock must be acquired by this instance"
    assert not harness.paths.lock_file.exists()
    assert not harness.paths.pending_file.exists()
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.inbox.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["object", "subclass"])
async def test_settlement_rejects_a_non_exact_expected_journal_before_loading_state(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    manager = _manager(harness)
    invalid = object() if case == "object" else object.__new__(_PendingJournalSubclass)

    async with harness.lock:
        with monkeypatch.context() as guarded:
            guarded.setattr(PendingJournalStore, "load", _forbidden)
            with pytest.raises(BridgeError) as caught:
                await manager.settle_published_cancellation(cast(Any, invalid))
            assert caught.value.code is ErrorCode.INVALID_ARGUMENT

    assert not harness.paths.pending_file.exists()
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.inbox.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "expected_message"),
    [
        ("prepared_r0", "recovery requires an exact published revision-one journal"),
        ("cancel_r2", "recovery requires an exact published revision-one journal"),
        ("artifact", "recovery requires an exact published revision-one journal"),
        ("journal_identity", "published recovery journal identity changed"),
        (
            "missing_request_evidence",
            "published recovery request ledger evidence is inconsistent",
        ),
    ],
)
async def test_settlement_rejects_non_r1_identity_and_ledger_drift_without_side_effects(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    expected_message: str,
) -> None:
    r0, r1, r2, response = _precondition_journals(harness)
    if case == "prepared_r0":
        sequence = (r0,)
        expected = r0
    elif case == "cancel_r2":
        sequence = (r0, r1, r2)
        expected = r2
    elif case == "artifact":
        sequence = (r0, r1, response)
        expected = response
    elif case == "journal_identity":
        sequence = (r0, r1)
        expected = _journal_with_original(r1, updated_at_ms=102)
    else:
        assert case == "missing_request_evidence"
        sequence = (r0, r1)
        expected = r1

    manager = _manager(harness)

    def missing_request(_ledger: RequestLedger, request_id: UUID) -> None:
        assert request_id == REQUEST_ID
        return None

    async with harness.lock:
        _persist_journal_sequence(harness, sequence)
        before_bytes = harness.paths.pending_file.read_bytes()
        before_mtime_ns = harness.paths.pending_file.stat().st_mtime_ns

        with monkeypatch.context() as guarded:
            guarded.setattr(PendingJournalStore, "save", _forbidden)
            guarded.setattr(PendingJournalStore, "delete", _forbidden)
            guarded.setattr(RequestLedger, "insert_prepared", _forbidden)
            guarded.setattr(RequestLedger, "insert_delivery", _forbidden)
            guarded.setattr(RequestLedger, "mark_delivery_published", _forbidden)
            guarded.setattr(RequestLedger, "transition", _forbidden)
            guarded.setattr(RequestLedger, "record_response", _forbidden)
            guarded.setattr(RequestLedger, "get_delivery", _forbidden)
            guarded.setattr(InboxPublisher, "publish_delivery", _forbidden)
            guarded.setattr(InboxPublisher, "publish_idle", _forbidden)
            guarded.setattr(
                RequestLedger,
                "get_request",
                missing_request if case == "missing_request_evidence" else _forbidden,
            )

            with pytest.raises(BridgeError) as caught:
                await manager.settle_published_cancellation(expected)
            assert caught.value.code is ErrorCode.STATE_CONFLICT
            assert caught.value.message == expected_message

        assert harness.paths.pending_file.read_bytes() == before_bytes
        assert harness.paths.pending_file.stat().st_mtime_ns == before_mtime_ns

    assert harness.inspector.calls == 0
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.inbox.exists()
    assert not harness.paths.response_path(REQUEST_ID).exists()
