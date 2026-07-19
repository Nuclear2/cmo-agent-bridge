from __future__ import annotations

import asyncio
import builtins
import hashlib
import inspect
import json
import math
import sqlite3
import sys
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, replace
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, Never, cast, get_type_hints
from uuid import UUID

import pytest
from pydantic import JsonValue, ValidationError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.operations.models import BridgeStatusWireArgs
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes, prepare_delivery
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua, render_idle_lua
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.models import ExchangeCommand, RequestBody
from cmo_agent_bridge.protocol.response_models import (
    CompletedSettlement,
    RejectedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, Sha256
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    PendingExchange,
    PendingJournal,
    PendingJournalHeader,
    PendingPhase,
)
from cmo_agent_bridge.state.request_ledger import RequestLedger, RequestRecord
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.models import (
    BridgeChannel,
    BridgeTransport,
    RecoveryDisposition,
    RecoveryReport,
)
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo
from cmo_agent_bridge.transports.file_bridge.transport import (
    FileBridgeTransport,
    _FileBridgeChannel,  # pyright: ignore[reportPrivateUsage]
    _FileBridgeSession,  # pyright: ignore[reportPrivateUsage]
    _await_protected_task,  # pyright: ignore[reportPrivateUsage]
)
import cmo_agent_bridge.transports.file_bridge.transport as transport_module
import cmo_agent_bridge.transports.file_bridge.cleanup as cleanup_module

if TYPE_CHECKING:
    from tests.helpers.fake_file_bridge_peer import (
        Delay,
        FakeFileBridgePeer,
        Respond,
        StaySilent,
        WriteMalformedComments,
        WriteMismatchedDelivery,
        WritePartialThenComplete,
    )
else:
    sys.path.insert(0, str(Path(__file__).parents[3] / "helpers"))
    from fake_file_bridge_peer import (
        Delay,
        FakeFileBridgePeer,
        Respond,
        StaySilent,
        WriteMalformedComments,
        WriteMismatchedDelivery,
        WritePartialThenComplete,
    )


REQUEST_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
DELIVERY_ID = UUID("11111111-1111-4111-8111-111111111111")
LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")


def _forbidden(*_args: object, **_kwargs: object) -> Never:
    raise AssertionError("forbidden side effect was reached")


class Inspector:
    def __init__(
        self,
        observations: tuple[object, ...],
        *,
        trace: list[str] | None = None,
    ) -> None:
        self.observations = observations
        self.trace = trace
        self.calls = 0

    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        del command_exe
        self.calls += 1
        if self.trace is not None:
            self.trace.append(
                "process_pin"
                if self.calls == 1
                else "process_prepublication"
                if self.calls == 2
                else "waiter_process_check"
            )
        return cast(tuple[ProcessInfo, ...], self.observations)


class SequencedInspector:
    def __init__(self, observations: tuple[tuple[object, ...], ...]) -> None:
        self.observations = observations
        self.calls = 0

    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        del command_exe
        index = min(self.calls, len(self.observations) - 1)
        self.calls += 1
        return cast(tuple[ProcessInfo, ...], self.observations[index])


class AsyncInspector:
    async def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        del command_exe
        return ()


@dataclass(frozen=True, slots=True)
class Harness:
    paths: FileBridgePaths
    root_lock: RootLock
    process: ProcessInfo
    inspector: Inspector
    snapshot: RuntimeSnapshot
    catalog: ManifestCatalog
    database: StateDatabase


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
    root_lock = RootLock(paths.lock_file, timeout_seconds=0)
    process = ProcessInfo(pid=1234, create_time=1000.5, executable=paths.command_exe)
    inspector = Inspector((process,))
    snapshot = RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=MANIFEST_SHA256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )
    catalog = ManifestCatalog(ReleaseBinding(snapshot=snapshot, registry=OPERATION_REGISTRY))
    database = StateDatabase(paths.sqlite_file)
    return Harness(
        paths=paths,
        root_lock=root_lock,
        process=process,
        inspector=inspector,
        snapshot=snapshot,
        catalog=catalog,
        database=database,
    )


def _transport(harness: Harness, **overrides: object) -> FileBridgeTransport:
    arguments: dict[str, object] = {
        "paths": harness.paths,
        "root_lock": harness.root_lock,
        "process_inspector": harness.inspector,
        "catalog": harness.catalog,
        "database": harness.database,
        "max_journal_bytes": 1_000_000,
        "replace_retry_seconds": 0,
        "response_poll_seconds": 0.001,
        "cancel_ack_timeout_seconds": 10,
    }
    arguments.update(overrides)
    constructor = cast(Any, FileBridgeTransport)
    return cast(FileBridgeTransport, constructor(**arguments))


def test_bridge_transport_root_key_protocol_and_concrete_surface_are_exact_read_only() -> None:
    protocol_property = inspect.getattr_static(BridgeTransport, "root_key", None)
    concrete_property = inspect.getattr_static(FileBridgeTransport, "root_key", None)

    assert isinstance(protocol_property, property), "missing F2 BridgeTransport.root_key"
    assert isinstance(concrete_property, property), "missing F2 FileBridgeTransport.root_key"
    for candidate in (protocol_property, concrete_property):
        assert candidate.fset is None
        assert candidate.fget is not None
        assert get_type_hints(candidate.fget, include_extras=True) == {"return": Sha256}


def test_file_bridge_transport_root_key_returns_paths_key_without_io(harness: Harness) -> None:
    transport = _transport(harness)
    before = (
        harness.paths.sqlite_file.exists(),
        harness.paths.pending_file.exists(),
        harness.paths.inbox.exists(),
        harness.inspector.calls,
    )

    assert transport.root_key == harness.paths.root_key
    assert type(transport.root_key) is str
    with pytest.raises(AttributeError):
        setattr(transport, "root_key", "f" * 64)
    assert transport.root_key == harness.paths.root_key
    assert (
        harness.paths.sqlite_file.exists(),
        harness.paths.pending_file.exists(),
        harness.paths.inbox.exists(),
        harness.inspector.calls,
    ) == before


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _write_quarantined_journal(harness: Harness) -> bytes:
    arguments: dict[str, JsonValue] = {
        "side_guid": "SIDE-1",
        "unit_type": "Aircraft",
        "dbid": 1,
        "name": "Test unit",
        "latitude": 1.5,
        "longitude": 2.5,
        "altitude": None,
        "loadout_dbid": None,
    }
    invocation = OPERATION_REGISTRY.resolve_wire_invocation("unit.add", arguments)
    body = RequestBody(
        protocol=harness.snapshot.protocol,
        release_id=harness.snapshot.release_id,
        runtime_version=harness.snapshot.runtime_version,
        runtime_tag=harness.snapshot.runtime_tag,
        runtime_asset_sha256=harness.snapshot.runtime_asset_sha256,
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=ACTIVATION_ID,
        operation_manifest_sha256=harness.snapshot.operation_manifest_sha256,
        operation="unit.add",
        arguments=arguments,
    )
    body_json = canonical_body_bytes(body).decode("utf-8")
    request_hash = hashlib.sha256(body_json.encode("utf-8")).hexdigest()
    recovery_schema_id = (
        None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
    )
    intent = DeliveryIntent(
        request_id=REQUEST_ID,
        delivery_id=DELIVERY_ID,
        delivery_kind="request",
        original_request_delivery_id=DELIVERY_ID,
        body_json=body_json,
        request_hash=request_hash,
        runtime_snapshot=harness.snapshot,
        result_schema_id=invocation.result_schema.schema_id,
        recovery_schema_id=recovery_schema_id,
        intended_at_ms=100,
        published_at_ms=101,
        rendered_inbox_sha256="e" * 64,
        rendered_inbox_size_bytes=512,
        response_filename=f"CMOAgentBridge_Response_{REQUEST_ID}.inst",
    )
    exchange = PendingExchange(
        request_id=REQUEST_ID,
        request_hash=request_hash,
        operation="unit.add",
        effective_class=invocation.effective_class,
        body_json=body_json,
        runtime_snapshot=harness.snapshot,
        result_schema_id=invocation.result_schema.schema_id,
        recovery_schema_id=recovery_schema_id,
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=ACTIVATION_ID,
        delivery_intents=(intent,),
        response_artifact=None,
        settlement=None,
        original_target_request_id=None,
        original_target_request_hash=None,
        revision=2,
        state=PendingPhase.QUARANTINED,
        created_at_ms=100,
        updated_at_ms=102,
    )
    journal = PendingJournal(
        header=PendingJournalHeader(
            format="cmo-agent-bridge/pending-journal",
            header_version=1,
            root_key=harness.paths.root_key,
            required_release_id=harness.snapshot.release_id,
        ),
        original=exchange,
        reconcile_attempt=None,
    )
    raw = _canonical_json(journal.model_dump(mode="json"))
    harness.paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.pending_file.write_bytes(raw)
    return raw


def _patch_empty_journal(
    monkeypatch: pytest.MonkeyPatch,
    transport: FileBridgeTransport,
    trace: list[str] | None = None,
) -> None:
    def load() -> None:
        transport.root_lock.require_acquired()
        if trace is not None:
            trace.append("journal_gate")
        return None

    monkeypatch.setattr(transport.journals, "load", load)


def _dummy_command() -> ExchangeCommand:
    return cast(ExchangeCommand, object())


def test_public_protocols_and_transport_signature_are_exact() -> None:
    process_identity_property = cast(property, vars(BridgeChannel)["process_identity"])
    process_identity_getter = cast(
        Callable[[BridgeChannel], ProcessInfo],
        process_identity_property.fget,
    )
    process_identity = inspect.signature(process_identity_getter)
    assert tuple(process_identity.parameters) == ("self",)
    assert process_identity.return_annotation == "ProcessInfo"

    exchange = inspect.signature(BridgeChannel.exchange)
    assert tuple(exchange.parameters) == ("self", "command")
    assert exchange.parameters["command"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert exchange.return_annotation == "ResponseArtifact"

    recover_pending = inspect.signature(BridgeChannel.recover_pending)
    assert tuple(recover_pending.parameters) == ("self",)
    assert recover_pending.parameters["self"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert recover_pending.parameters["self"].default is inspect.Parameter.empty
    assert recover_pending.return_annotation == "RecoveryReport"

    session = inspect.signature(BridgeTransport.session)
    assert tuple(session.parameters) == ("self",)
    assert session.parameters["self"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert session.parameters["self"].default is inspect.Parameter.empty
    assert session.return_annotation == "AbstractAsyncContextManager[BridgeChannel]"

    constructor = inspect.signature(FileBridgeTransport.__init__)
    assert tuple(constructor.parameters) == (
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
    )
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for name, parameter in constructor.parameters.items()
        if name != "self"
    )
    assert constructor.parameters["self"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert constructor.parameters["self"].default is inspect.Parameter.empty
    assert constructor.parameters["response_poll_seconds"].default == 0.05
    assert constructor.parameters["cancel_ack_timeout_seconds"].default == 10
    assert not hasattr(FileBridgeTransport, "recover_pending")


def test_public_annotations_async_kinds_and_all_h10_types_are_exact() -> None:
    assert BridgeChannel._is_protocol is True  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    assert BridgeTransport._is_protocol is True  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    assert BridgeChannel._is_runtime_protocol is False  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    assert BridgeTransport._is_runtime_protocol is False  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    assert inspect.iscoroutinefunction(BridgeChannel.exchange)
    assert inspect.iscoroutinefunction(BridgeChannel.recover_pending)
    assert not inspect.iscoroutinefunction(BridgeTransport.session)
    assert not inspect.iscoroutinefunction(FileBridgeTransport.session)
    process_identity_property = cast(property, vars(BridgeChannel)["process_identity"])
    process_identity_getter = cast(
        Callable[[BridgeChannel], ProcessInfo],
        process_identity_property.fget,
    )
    assert get_type_hints(process_identity_getter) == {
        "return": ProcessInfo,
    }
    assert get_type_hints(BridgeChannel.exchange) == {
        "command": ExchangeCommand,
        "return": ResponseArtifact,
    }
    assert get_type_hints(BridgeChannel.recover_pending) == {"return": RecoveryReport}
    assert get_type_hints(BridgeTransport.session) == {
        "return": AbstractAsyncContextManager[BridgeChannel]
    }
    assert get_type_hints(FileBridgeTransport.session) == {
        "return": AbstractAsyncContextManager[BridgeChannel]
    }
    constructor_hints = get_type_hints(FileBridgeTransport.__init__)
    assert constructor_hints == {
        "paths": FileBridgePaths,
        "root_lock": RootLock,
        "process_inspector": transport_module.CmoProcessInspector,
        "catalog": ManifestCatalog,
        "database": StateDatabase,
        "max_journal_bytes": int,
        "replace_retry_seconds": float,
        "response_poll_seconds": float,
        "cancel_ack_timeout_seconds": float,
        "return": type(None),
    }
    concrete_session = inspect.signature(_FileBridgeSession.__aexit__)
    assert tuple(concrete_session.parameters) == (
        "self",
        "exc_type",
        "exc",
        "traceback",
    )
    assert inspect.iscoroutinefunction(_FileBridgeSession.__aenter__)
    assert inspect.iscoroutinefunction(_FileBridgeSession.__aexit__)
    concrete_exchange = inspect.signature(_FileBridgeChannel.exchange)
    assert tuple(concrete_exchange.parameters) == ("self", "command")
    assert inspect.iscoroutinefunction(_FileBridgeChannel.exchange)
    for signature in (
        inspect.signature(BridgeChannel.exchange),
        concrete_exchange,
    ):
        assert signature.parameters["self"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        assert signature.parameters["self"].default is inspect.Parameter.empty
        assert signature.parameters["command"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        assert signature.parameters["command"].default is inspect.Parameter.empty
    concrete_exchange_hints = get_type_hints(_FileBridgeChannel.exchange)
    assert concrete_exchange_hints == {
        "command": ExchangeCommand,
        "return": ResponseArtifact,
    }
    concrete_recovery = inspect.signature(_FileBridgeChannel.recover_pending)
    assert tuple(concrete_recovery.parameters) == ("self",)
    assert inspect.iscoroutinefunction(_FileBridgeChannel.recover_pending)
    assert get_type_hints(_FileBridgeChannel.recover_pending) == {
        "return": RecoveryReport,
    }
    concrete_enter = inspect.signature(_FileBridgeSession.__aenter__)
    assert tuple(concrete_enter.parameters) == ("self",)
    assert concrete_enter.parameters["self"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert concrete_enter.parameters["self"].default is inspect.Parameter.empty
    assert get_type_hints(_FileBridgeSession.__aenter__) == {"return": _FileBridgeChannel}
    expected_exit_hints = {
        "exc_type": type[BaseException] | None,
        "exc": BaseException | None,
        "traceback": TracebackType | None,
        "return": bool,
    }
    assert get_type_hints(_FileBridgeSession.__aexit__) == expected_exit_hints
    for parameter in concrete_session.parameters.values():
        assert parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        assert parameter.default is inspect.Parameter.empty
    concrete_transport_session = inspect.signature(FileBridgeTransport.session)
    assert tuple(concrete_transport_session.parameters) == ("self",)
    assert (
        concrete_transport_session.parameters["self"].kind
        is inspect.Parameter.POSITIONAL_OR_KEYWORD
    )
    assert concrete_transport_session.parameters["self"].default is inspect.Parameter.empty
    constructor = inspect.signature(FileBridgeTransport.__init__)
    for name in (
        "paths",
        "root_lock",
        "process_inspector",
        "catalog",
        "database",
        "max_journal_bytes",
        "replace_retry_seconds",
    ):
        assert constructor.parameters[name].default is inspect.Parameter.empty
    assert constructor.parameters["response_poll_seconds"].default == 0.05
    assert constructor.parameters["cancel_ack_timeout_seconds"].default == 10
    assert "recover_pending" in BridgeChannel.__dict__
    assert "recover_pending" in _FileBridgeChannel.__dict__
    for candidate in (
        BridgeTransport,
        FileBridgeTransport,
        _FileBridgeSession,
    ):
        assert "recover_pending" not in candidate.__dict__


def test_h10_recovery_report_is_strict_frozen_and_consistent() -> None:
    import cmo_agent_bridge.transports.file_bridge.models as models
    import cmo_agent_bridge.transports.file_bridge.transport as transport

    assert models.RecoveryDisposition is RecoveryDisposition
    assert models.RecoveryReport is RecoveryReport
    assert not hasattr(transport, "RecoveryReport")
    assert tuple(RecoveryDisposition) == (
        RecoveryDisposition.NO_PENDING,
        RecoveryDisposition.FAILED_BEFORE_PUBLISH,
        RecoveryDisposition.SETTLED,
        RecoveryDisposition.QUARANTINED,
    )
    assert [disposition.value for disposition in RecoveryDisposition] == [
        "no_pending",
        "failed_before_publish",
        "settled",
        "quarantined",
    ]
    assert tuple(RecoveryReport.model_fields) == (
        "disposition",
        "request_id",
        "request_state",
        "response_cleanup_required",
    )

    no_pending = RecoveryReport(
        disposition=RecoveryDisposition.NO_PENDING,
        request_id=None,
        request_state=None,
        response_cleanup_required=False,
    )
    failed = RecoveryReport(
        disposition=RecoveryDisposition.FAILED_BEFORE_PUBLISH,
        request_id=REQUEST_ID,
        request_state=HostRequestState.REJECTED,
        response_cleanup_required=False,
    )
    settled = RecoveryReport(
        disposition=RecoveryDisposition.SETTLED,
        request_id=REQUEST_ID,
        request_state=HostRequestState.COMPLETED,
        response_cleanup_required=True,
    )
    quarantined = RecoveryReport(
        disposition=RecoveryDisposition.QUARANTINED,
        request_id=REQUEST_ID,
        request_state=HostRequestState.QUARANTINED,
        response_cleanup_required=False,
    )
    assert no_pending.model_dump(mode="json") == {
        "disposition": "no_pending",
        "request_id": None,
        "request_state": None,
        "response_cleanup_required": False,
    }
    assert failed.request_state is HostRequestState.REJECTED
    assert settled.response_cleanup_required is True
    assert quarantined.request_state is HostRequestState.QUARANTINED
    with pytest.raises(ValidationError):
        setattr(settled, "response_cleanup_required", False)


@pytest.mark.parametrize(
    "candidate",
    [
        {
            "disposition": "no_pending",
            "request_id": None,
            "request_state": None,
            "response_cleanup_required": False,
        },
        {
            "disposition": RecoveryDisposition.NO_PENDING,
            "request_id": REQUEST_ID,
            "request_state": None,
            "response_cleanup_required": False,
        },
        {
            "disposition": RecoveryDisposition.FAILED_BEFORE_PUBLISH,
            "request_id": REQUEST_ID,
            "request_state": HostRequestState.PREPARED,
            "response_cleanup_required": False,
        },
        {
            "disposition": RecoveryDisposition.SETTLED,
            "request_id": REQUEST_ID,
            "request_state": HostRequestState.CANCELLED,
            "response_cleanup_required": False,
        },
        {
            "disposition": RecoveryDisposition.QUARANTINED,
            "request_id": REQUEST_ID,
            "request_state": HostRequestState.QUARANTINED,
            "response_cleanup_required": True,
        },
        {
            "disposition": RecoveryDisposition.NO_PENDING,
            "request_id": None,
            "request_state": None,
            "response_cleanup_required": 0,
        },
        {
            "disposition": RecoveryDisposition.NO_PENDING,
            "request_id": None,
            "request_state": None,
            "response_cleanup_required": False,
            "unexpected": "forbidden",
        },
    ],
)
def test_h10_recovery_report_rejects_inconsistent_or_coerced_values(
    candidate: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        RecoveryReport.model_validate(candidate)


def test_constructor_is_side_effect_free_and_exposes_exact_products(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(harness.catalog, "resolve_running", _forbidden)

    transport = _transport(harness)

    assert transport.paths is harness.paths
    assert transport.root_lock is harness.root_lock
    assert transport.process_inspector is harness.inspector
    assert transport.catalog is harness.catalog
    assert transport.database is harness.database
    assert transport.journals is not None
    assert transport.ledger is not None
    assert transport.inbox is not None
    assert transport._response_poll_seconds == 0.001  # pyright: ignore[reportPrivateUsage]
    assert transport._cancel_ack_timeout_seconds == 10.0  # pyright: ignore[reportPrivateUsage]
    assert harness.inspector.calls == 0
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.pending_file.exists()
    assert not harness.paths.inbox.exists()
    assert not hasattr(transport, "connection")


def test_constructor_performs_zero_path_or_file_io(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(Path, "resolve", _forbidden)
    monkeypatch.setattr(Path, "stat", _forbidden)
    monkeypatch.setattr(Path, "read_bytes", _forbidden)
    monkeypatch.setattr(Path, "open", _forbidden)
    monkeypatch.setattr(Path, "mkdir", _forbidden)
    monkeypatch.setattr(builtins, "open", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(harness.catalog, "resolve_running", _forbidden)

    transport = _transport(harness)

    assert type(transport.journals) is transport_module.PendingJournalStore
    assert type(transport.ledger) is RequestLedger
    assert type(transport.inbox) is InboxPublisher


def test_constructor_builds_each_exact_bound_product_once(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counts = {"journals": 0, "ledger": 0, "inbox": 0}
    original_journal_init = transport_module.PendingJournalStore.__init__
    original_ledger_init = RequestLedger.__init__
    original_inbox_init = InboxPublisher.__init__

    def journal_init(
        store: object,
        paths: FileBridgePaths,
        root_lock: RootLock,
        catalog: ManifestCatalog,
        *,
        max_journal_bytes: int,
        replace_retry_seconds: float,
    ) -> None:
        counts["journals"] += 1
        original_journal_init(
            cast(transport_module.PendingJournalStore, store),
            paths,
            root_lock,
            catalog,
            max_journal_bytes=max_journal_bytes,
            replace_retry_seconds=replace_retry_seconds,
        )

    def ledger_init(
        ledger: object,
        database: StateDatabase,
        catalog: ManifestCatalog,
    ) -> None:
        counts["ledger"] += 1
        original_ledger_init(cast(RequestLedger, ledger), database, catalog)

    def inbox_init(
        inbox: object,
        paths: FileBridgePaths,
        replace_retry_seconds: float,
    ) -> None:
        counts["inbox"] += 1
        original_inbox_init(cast(InboxPublisher, inbox), paths, replace_retry_seconds)

    monkeypatch.setattr(transport_module.PendingJournalStore, "__init__", journal_init)
    monkeypatch.setattr(RequestLedger, "__init__", ledger_init)
    monkeypatch.setattr(InboxPublisher, "__init__", inbox_init)

    transport = _transport(harness, max_journal_bytes=123_457)

    assert counts == {"journals": 1, "ledger": 1, "inbox": 1}
    assert transport.journals._paths is harness.paths  # pyright: ignore[reportPrivateUsage]
    assert transport.journals._root_lock is harness.root_lock  # pyright: ignore[reportPrivateUsage]
    assert transport.journals._catalog is harness.catalog  # pyright: ignore[reportPrivateUsage]
    assert transport.journals._max_journal_bytes == 123_457  # pyright: ignore[reportPrivateUsage]
    assert transport.journals._replace_retry_seconds == 0.0  # pyright: ignore[reportPrivateUsage]
    assert transport.ledger._database is harness.database  # pyright: ignore[reportPrivateUsage]
    assert transport.ledger._catalog is harness.catalog  # pyright: ignore[reportPrivateUsage]
    assert transport.inbox.paths is harness.paths
    assert transport.inbox.replace_retry_seconds == 0.0
    assert transport._response_poll_seconds == 0.001  # pyright: ignore[reportPrivateUsage]
    assert transport._cancel_ack_timeout_seconds == 10.0  # pyright: ignore[reportPrivateUsage]
    for name, value in (
        ("paths", harness.paths),
        ("root_lock", harness.root_lock),
        ("process_inspector", harness.inspector),
        ("catalog", harness.catalog),
        ("database", harness.database),
        ("journals", transport.journals),
        ("ledger", transport.ledger),
        ("inbox", transport.inbox),
    ):
        with pytest.raises(AttributeError):
            setattr(cast(Any, transport), name, value)


def test_constructor_accepts_exact_positive_int_and_float_numeric_options(
    harness: Harness,
) -> None:
    transport = _transport(
        harness,
        replace_retry_seconds=0.25,
        response_poll_seconds=1,
        cancel_ack_timeout_seconds=2.5,
    )

    assert transport.journals._replace_retry_seconds == 0.25  # pyright: ignore[reportPrivateUsage]
    assert transport.inbox.replace_retry_seconds == 0.25
    assert transport._response_poll_seconds == 1.0  # pyright: ignore[reportPrivateUsage]
    assert transport._cancel_ack_timeout_seconds == 2.5  # pyright: ignore[reportPrivateUsage]


class IntSubclass(int):
    pass


class FloatSubclass(float):
    pass


class PathsSubclass(FileBridgePaths):
    pass


class RootLockSubclass(RootLock):
    pass


class CatalogSubclass(ManifestCatalog):
    pass


class DatabaseSubclass(StateDatabase):
    pass


_INVALID_CONSTRUCTOR_CASES: tuple[tuple[str, Callable[[Harness], object]], ...] = (
    (
        "paths",
        lambda h: PathsSubclass(
            h.paths.game_root,
            h.paths.root_key,
            h.paths.command_exe,
            h.paths.lua_root,
            h.paths.inbox,
            h.paths.import_export,
            h.paths.lock_file,
            h.paths.pending_file,
            h.paths.sqlite_file,
        ),
    ),
    (
        "root_lock",
        lambda h: RootLockSubclass(h.paths.lock_file, timeout_seconds=0),
    ),
    ("catalog", lambda _h: object.__new__(CatalogSubclass)),
    ("database", lambda _h: object.__new__(DatabaseSubclass)),
    ("max_journal_bytes", lambda _h: IntSubclass(1)),
    ("max_journal_bytes", lambda _h: -1),
    ("replace_retry_seconds", lambda _h: IntSubclass(0)),
    ("replace_retry_seconds", lambda _h: FloatSubclass(0.1)),
    ("replace_retry_seconds", lambda _h: -math.inf),
    ("replace_retry_seconds", lambda _h: math.nan),
    ("replace_retry_seconds", lambda _h: 10**10_000),
    ("response_poll_seconds", lambda _h: IntSubclass(1)),
    ("response_poll_seconds", lambda _h: FloatSubclass(0.1)),
    ("response_poll_seconds", lambda _h: -1),
    ("response_poll_seconds", lambda _h: -math.inf),
    ("response_poll_seconds", lambda _h: math.inf),
    ("cancel_ack_timeout_seconds", lambda _h: IntSubclass(1)),
    ("cancel_ack_timeout_seconds", lambda _h: FloatSubclass(0.1)),
    ("cancel_ack_timeout_seconds", lambda _h: 0),
    ("cancel_ack_timeout_seconds", lambda _h: -1),
    ("cancel_ack_timeout_seconds", lambda _h: math.nan),
    ("cancel_ack_timeout_seconds", lambda _h: -math.inf),
    ("cancel_ack_timeout_seconds", lambda _h: math.inf),
    ("cancel_ack_timeout_seconds", lambda _h: 10**10_000),
)


@pytest.mark.parametrize(
    ("name", "value_factory"),
    _INVALID_CONSTRUCTOR_CASES,
)
def test_constructor_exact_type_and_numeric_edge_matrix(
    harness: Harness,
    name: str,
    value_factory: Callable[[Harness], object],
) -> None:
    with pytest.raises(BridgeError) as caught:
        _transport(harness, **{name: value_factory(harness)})

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_constructor_rejects_lexically_different_same_target_bindings_without_resolution(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lexical_lock = RootLock(
        harness.paths.lock_file.parent / "unused" / ".." / harness.paths.lock_file.name,
        timeout_seconds=0,
    )
    lexical_database = StateDatabase(
        harness.paths.sqlite_file.parent / "unused" / ".." / harness.paths.sqlite_file.name
    )
    monkeypatch.setattr(Path, "resolve", _forbidden)
    monkeypatch.setattr(Path, "stat", _forbidden)
    monkeypatch.setattr(Path, "read_bytes", _forbidden)
    monkeypatch.setattr(Path, "open", _forbidden)
    monkeypatch.setattr(Path, "mkdir", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)

    with pytest.raises(BridgeError) as lock_error:
        _transport(harness, root_lock=lexical_lock)
    with pytest.raises(BridgeError) as database_error:
        _transport(harness, database=lexical_database)

    assert lock_error.value.code is ErrorCode.INVALID_ARGUMENT
    assert database_error.value.code is ErrorCode.INVALID_ARGUMENT


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("paths", object()),
        ("root_lock", object()),
        ("catalog", object()),
        ("database", object()),
        ("process_inspector", object()),
        ("process_inspector", AsyncInspector()),
        ("max_journal_bytes", True),
        ("max_journal_bytes", 0),
        ("max_journal_bytes", 1.0),
        ("replace_retry_seconds", True),
        ("replace_retry_seconds", -1),
        ("replace_retry_seconds", math.inf),
        ("response_poll_seconds", False),
        ("response_poll_seconds", 0),
        ("response_poll_seconds", math.nan),
        ("cancel_ack_timeout_seconds", True),
        ("cancel_ack_timeout_seconds", False),
        ("cancel_ack_timeout_seconds", 0),
        ("cancel_ack_timeout_seconds", math.nan),
    ],
)
def test_constructor_rejects_invalid_dependencies_and_numbers_without_io(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: object,
) -> None:
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(harness.catalog, "resolve_running", _forbidden)

    with pytest.raises(BridgeError) as caught:
        _transport(harness, **{name: value})

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert harness.inspector.calls == 0
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.pending_file.exists()
    assert not harness.paths.inbox.exists()


def test_constructor_rejects_cross_root_lock_and_database(harness: Harness) -> None:
    wrong_lock = RootLock(harness.paths.lock_file.with_name("wrong.lock"), timeout_seconds=0)
    wrong_database = StateDatabase(harness.paths.sqlite_file.with_name("wrong.sqlite3"))

    with pytest.raises(BridgeError) as lock_error:
        _transport(harness, root_lock=wrong_lock)
    with pytest.raises(BridgeError) as database_error:
        _transport(harness, database=wrong_database)

    assert lock_error.value.code is ErrorCode.INVALID_ARGUMENT
    assert database_error.value.code is ErrorCode.INVALID_ARGUMENT


@pytest.mark.asyncio
async def test_session_entry_order_is_lock_process_pin_then_journal_gate(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace: list[str] = []
    inspector = Inspector((harness.process,), trace=trace)
    transport = _transport(harness, process_inspector=inspector)
    _patch_empty_journal(monkeypatch, transport, trace)
    original_enter = RootLock.__aenter__
    original_exit = RootLock.__aexit__

    async def traced_enter(lock: RootLock) -> RootLock:
        acquired = await original_enter(lock)
        trace.append("lock")
        return acquired

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        result = await original_exit(lock, exc_type, exc, traceback)
        trace.append("unlock")
        return result

    monkeypatch.setattr(RootLock, "__aenter__", traced_enter)
    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)

    async with transport.session() as channel:
        assert channel is not None
        transport.root_lock.require_acquired()

    assert trace == ["lock", "process_pin", "journal_gate", "unlock"]
    assert not harness.paths.sqlite_file.exists()


@pytest.mark.asyncio
async def test_session_channel_exposes_only_the_entry_time_process_identity(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replacement = ProcessInfo(
        pid=harness.process.pid + 1,
        create_time=harness.process.create_time + 1.0,
        executable=harness.process.executable,
    )
    inspector = Inspector((harness.process,))
    transport = _transport(harness, process_inspector=inspector)
    _patch_empty_journal(monkeypatch, transport)

    async with transport.session() as channel:
        assert channel.process_identity is harness.process
        inspector.observations = (replacement,)
        assert channel.process_identity is harness.process

    assert inspector.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("observations", "code"),
    [
        ((), ErrorCode.CMO_NOT_RUNNING),
        (
            (
                ProcessInfo(pid=10, create_time=1.0, executable=Path("Command.exe")),
                ProcessInfo(pid=11, create_time=2.0, executable=Path("Command.exe")),
            ),
            ErrorCode.MULTIPLE_CMO_INSTANCES,
        ),
    ],
)
async def test_zero_or_multiple_processes_propagate_before_journal_gate(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    observations: tuple[object, ...],
    code: ErrorCode,
) -> None:
    inspector = Inspector(observations)
    transport = _transport(harness, process_inspector=inspector)
    monkeypatch.setattr(transport.journals, "load", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)

    with pytest.raises(BridgeError) as caught:
        async with transport.session():
            pytest.fail("session unexpectedly entered")

    assert caught.value.code is code
    assert inspector.calls == 1
    assert not harness.paths.sqlite_file.exists()


class DerivedProcessInfo(ProcessInfo):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observation",
    [
        object(),
        DerivedProcessInfo(pid=1, create_time=1.0, executable=Path("Command.exe")),
        ProcessInfo(pid=True, create_time=1.0, executable=Path("Command.exe")),
        ProcessInfo(pid=0, create_time=1.0, executable=Path("Command.exe")),
        ProcessInfo(pid=-1, create_time=1.0, executable=Path("Command.exe")),
        ProcessInfo(pid=IntSubclass(1), create_time=1.0, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=True, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=1, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=0.0, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=-1.0, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=math.nan, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=-math.inf, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=math.inf, executable=Path("Command.exe")),
        ProcessInfo(
            pid=1,
            create_time=FloatSubclass(1.0),
            executable=Path("Command.exe"),
        ),
        ProcessInfo(pid=1, create_time=1.0, executable=cast(Path, "Command.exe")),
    ],
)
async def test_malformed_single_process_stops_the_entire_downstream_gate(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    observation: object,
) -> None:
    inspector = Inspector((observation,))
    transport = _transport(harness, process_inspector=inspector)
    monkeypatch.setattr(transport.journals, "load", _forbidden)
    monkeypatch.setattr(harness.catalog, "resolve_running", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)

    with pytest.raises(BridgeError) as caught:
        async with transport.session():
            pytest.fail("session unexpectedly entered")

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert inspector.calls == 1
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.inbox.exists()


_JOURNAL_GATE_CASES: tuple[tuple[Callable[[Harness], bytes], ErrorCode], ...] = (
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
@pytest.mark.parametrize(("raw_factory", "code"), _JOURNAL_GATE_CASES)
async def test_foreign_or_corrupt_journal_is_unchanged_and_performs_zero_db_catalog_inbox(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    raw_factory: Callable[[Harness], bytes],
    code: ErrorCode,
) -> None:
    raw = raw_factory(harness)
    harness.paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    harness.paths.pending_file.write_bytes(raw)
    before_mtime = harness.paths.pending_file.stat().st_mtime_ns
    response_path = harness.paths.response_path(REQUEST_ID)
    response_path.parent.mkdir(parents=True, exist_ok=True)
    response_bytes = b"foreign-gate-response-sentinel"
    response_path.write_bytes(response_bytes)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    inbox_bytes = b"foreign-gate-inbox-sentinel"
    harness.paths.inbox.write_bytes(inbox_bytes)
    transport = _transport(harness)
    original_read_bytes = Path.read_bytes
    original_unlink = Path.unlink

    def guarded_read_bytes(path: Path) -> bytes:
        if path in {response_path, harness.paths.inbox}:
            raise AssertionError("foreign/corrupt session gate read a managed sentinel")
        return original_read_bytes(path)

    def guarded_unlink(path: Path, *, missing_ok: bool = False) -> None:
        if path in {response_path, harness.paths.inbox}:
            raise AssertionError("foreign/corrupt session gate unlinked a managed sentinel")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)
    monkeypatch.setattr(Path, "unlink", guarded_unlink)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(harness.catalog, "resolve_running", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)

    with pytest.raises(BridgeError) as caught:
        async with transport.session():
            pytest.fail("session unexpectedly entered")

    assert caught.value.code is code
    assert original_read_bytes(harness.paths.pending_file) == raw
    assert harness.paths.pending_file.stat().st_mtime_ns == before_mtime
    assert not harness.paths.sqlite_file.exists()
    assert original_read_bytes(response_path) == response_bytes
    assert original_read_bytes(harness.paths.inbox) == inbox_bytes
    async with harness.root_lock:
        harness.root_lock.require_acquired()


class RaisingInspector:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        del command_exe
        raise self.error


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["process", "journal"])
async def test_session_gate_preserves_bridge_error_identity_sentinel_and_allows_lock_reacquire(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    sentinel = BridgeError(ErrorCode.MANIFEST_MISMATCH, f"{boundary} sentinel")
    response_path = harness.paths.response_path(REQUEST_ID)
    response_path.parent.mkdir(parents=True, exist_ok=True)
    response_bytes = b"sentinel-must-not-be-read-or-deleted"
    response_path.write_bytes(response_bytes)
    inspector: object = harness.inspector
    if boundary == "process":
        inspector = RaisingInspector(sentinel)
    transport = _transport(harness, process_inspector=inspector)
    if boundary == "process":
        monkeypatch.setattr(transport.journals, "load", _forbidden)
    else:
        monkeypatch.setattr(transport.journals, "load", lambda: (_ for _ in ()).throw(sentinel))
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(harness.catalog, "resolve_running", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
    monkeypatch.setattr(RequestLedger, "insert_prepared", _forbidden)

    with pytest.raises(BridgeError) as caught:
        async with transport.session():
            pytest.fail("session unexpectedly entered")

    assert caught.value is sentinel
    assert response_path.read_bytes() == response_bytes
    async with harness.root_lock:
        harness.root_lock.require_acquired()
    assert response_path.read_bytes() == response_bytes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [ErrorCode.CMO_NOT_RUNNING, ErrorCode.MULTIPLE_CMO_INSTANCES],
)
async def test_real_single_instance_boundary_error_identity_has_full_gate_tripwires(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    code: ErrorCode,
) -> None:
    sentinel = BridgeError(code, "exact require_single_instance sentinel")
    response_path = harness.paths.response_path(REQUEST_ID)
    response_path.parent.mkdir(parents=True, exist_ok=True)
    response_bytes = b"owned-by-nobody-sentinel"
    response_path.write_bytes(response_bytes)
    transport = _transport(harness)

    def raise_boundary(*_args: object, **_kwargs: object) -> Never:
        raise sentinel

    monkeypatch.setattr(transport_module, "require_single_instance", raise_boundary)
    monkeypatch.setattr(transport.journals, "load", _forbidden)
    monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
    monkeypatch.setattr(RequestLedger, "insert_prepared", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(transport_module.ResponseWaiter, "__init__", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(harness.catalog, "resolve_running", _forbidden)
    monkeypatch.setattr(Path, "unlink", _forbidden)

    with pytest.raises(BridgeError) as caught:
        async with transport.session():
            pytest.fail("session unexpectedly entered")

    assert caught.value is sentinel
    assert response_path.read_bytes() == response_bytes
    async with harness.root_lock:
        harness.root_lock.require_acquired()


@pytest.mark.asyncio
async def test_exact_release_quarantined_mutation_allows_channel_without_journal_change(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _write_quarantined_journal(harness)
    before_mtime = harness.paths.pending_file.stat().st_mtime_ns
    transport = _transport(harness)
    sentinel = cast(ResponseArtifact, object())

    async def controlled(
        _channel: _FileBridgeChannel,
        _command: ExchangeCommand,
    ) -> ResponseArtifact:
        return sentinel

    monkeypatch.setattr(_FileBridgeChannel, "_perform_exchange", controlled)

    async with transport.session() as channel:
        assert await channel.exchange(_dummy_command()) is sentinel
        transport.root_lock.require_acquired()

    assert harness.paths.pending_file.read_bytes() == raw
    assert harness.paths.pending_file.stat().st_mtime_ns == before_mtime
    assert harness.paths.sqlite_file.exists()
    request = transport.ledger.get_request(REQUEST_ID)
    delivery = transport.ledger.get_delivery(DELIVERY_ID)
    assert request is not None
    assert request.state is HostRequestState.QUARANTINED
    assert delivery is not None
    assert delivery.request_id == REQUEST_ID
    assert delivery.published_at_ms == 101
    assert not harness.paths.inbox.exists()


@pytest.mark.asyncio
async def test_channel_rejects_exchange_after_session_exit(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    _patch_empty_journal(monkeypatch, transport)

    async with transport.session() as channel:
        pass

    with pytest.raises(BridgeError) as caught:
        await channel.exchange(_dummy_command())

    assert caught.value.code is ErrorCode.STATE_CONFLICT


@pytest.mark.asyncio
async def test_channel_rejects_concurrent_exchange_and_allows_sequential_reuse(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    _patch_empty_journal(monkeypatch, transport)
    started = asyncio.Event()
    release = asyncio.Event()
    sentinel = cast(ResponseArtifact, object())
    calls = 0

    async def controlled(
        _channel: _FileBridgeChannel,
        _command: ExchangeCommand,
    ) -> ResponseArtifact:
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
        return sentinel

    monkeypatch.setattr(_FileBridgeChannel, "_perform_exchange", controlled)

    async with transport.session() as channel:
        first = asyncio.create_task(channel.exchange(_dummy_command()))
        await started.wait()
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(_dummy_command())
        assert caught.value.code is ErrorCode.STATE_CONFLICT
        release.set()
        assert await first is sentinel
        assert await channel.exchange(_dummy_command()) is sentinel

    assert calls == 2


@pytest.mark.asyncio
async def test_same_transport_opens_fresh_sequential_sessions_and_old_channel_stays_closed(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    _patch_empty_journal(monkeypatch, transport)
    sentinel = cast(ResponseArtifact, object())

    async def controlled(
        _channel: _FileBridgeChannel,
        _command: ExchangeCommand,
    ) -> ResponseArtifact:
        return sentinel

    monkeypatch.setattr(_FileBridgeChannel, "_perform_exchange", controlled)
    first_manager = transport.session()
    second_manager = transport.session()
    assert first_manager is not second_manager
    async with first_manager as first_channel:
        assert await first_channel.exchange(_dummy_command()) is sentinel
    async with second_manager as second_channel:
        assert second_channel is not first_channel
        assert await second_channel.exchange(_dummy_command()) is sentinel
    monkeypatch.setattr(transport.journals, "load", _forbidden)
    monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
    monkeypatch.setattr(RequestLedger, "insert_prepared", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)
    monkeypatch.setattr(harness.catalog, "resolve_running", _forbidden)
    with pytest.raises(BridgeError) as caught:
        await first_channel.exchange(_dummy_command())
    assert caught.value.code is ErrorCode.STATE_CONFLICT


@pytest.mark.asyncio
async def test_same_task_reentrant_exchange_is_rejected_before_second_callback(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    _patch_empty_journal(monkeypatch, transport)
    sentinel = cast(ResponseArtifact, object())
    callback_calls = 0
    inner_error: BridgeError | None = None

    async def reentrant(
        channel: _FileBridgeChannel,
        command: ExchangeCommand,
    ) -> ResponseArtifact:
        nonlocal callback_calls, inner_error
        callback_calls += 1
        try:
            await channel.exchange(command)
        except BridgeError as error:
            inner_error = error
        return sentinel

    monkeypatch.setattr(_FileBridgeChannel, "_perform_exchange", reentrant)
    async with transport.session() as channel:
        assert await channel.exchange(_dummy_command()) is sentinel

    assert callback_calls == 1
    assert inner_error is not None
    assert inner_error.code is ErrorCode.STATE_CONFLICT


async def _run_exit_case(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    *,
    body_error: BaseException | None,
) -> tuple[asyncio.Task[ResponseArtifact], list[str]]:
    trace: list[str] = []
    transport = _transport(harness)
    _patch_empty_journal(monkeypatch, transport)
    started = asyncio.Event()
    finalizer_finished = asyncio.Event()
    original_exit = RootLock.__aexit__

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        assert finalizer_finished.is_set()
        trace.append("unlock")
        return await original_exit(lock, exc_type, exc, traceback)

    async def stalled(
        _channel: _FileBridgeChannel,
        _command: ExchangeCommand,
    ) -> ResponseArtifact:
        try:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        finally:
            transport.root_lock.require_acquired()
            await asyncio.sleep(0)
            trace.append("child_finalizer")
            finalizer_finished.set()

    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)
    monkeypatch.setattr(_FileBridgeChannel, "_perform_exchange", stalled)
    child: asyncio.Task[ResponseArtifact] | None = None

    try:
        async with transport.session() as channel:
            child = asyncio.create_task(channel.exchange(_dummy_command()))
            await started.wait()
            if body_error is not None:
                raise body_error
    finally:
        assert child is not None
        assert child.done()
        assert child.cancelled()
        assert finalizer_finished.is_set()
        with pytest.raises(BridgeError):
            transport.root_lock.require_acquired()

    return child, trace


@pytest.mark.asyncio
async def test_session_exit_closes_then_cancels_and_joins_active_exchange_before_unlock(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    child, trace = await _run_exit_case(harness, monkeypatch, body_error=None)

    assert child.cancelled()
    assert trace == ["child_finalizer", "unlock"]

    later = _transport(harness)
    _patch_empty_journal(monkeypatch, later)
    async with later.session() as channel:
        assert channel is not None


@pytest.mark.asyncio
async def test_body_exception_remains_primary_while_active_child_is_safely_joined(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body_error = RuntimeError("body failed")

    with pytest.raises(RuntimeError) as caught:
        await _run_exit_case(harness, monkeypatch, body_error=body_error)

    assert caught.value is body_error


@pytest.mark.asyncio
async def test_body_cancelled_error_object_remains_primary_by_identity(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body_cancellation = asyncio.CancelledError("exact body cancellation")

    with pytest.raises(asyncio.CancelledError) as caught:
        await _run_exit_case(
            harness,
            monkeypatch,
            body_error=body_cancellation,
        )

    assert caught.value is body_cancellation


@pytest.mark.asyncio
async def test_body_cancellation_waits_for_active_child_finalizer_before_unlock(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    finalizer_finished = asyncio.Event()
    trace: list[str] = []
    transport = _transport(harness)
    _patch_empty_journal(monkeypatch, transport)
    original_exit = RootLock.__aexit__

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        assert finalizer_finished.is_set()
        trace.append("unlock")
        return await original_exit(lock, exc_type, exc, traceback)

    async def stalled(
        _channel: _FileBridgeChannel,
        _command: ExchangeCommand,
    ) -> ResponseArtifact:
        try:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        finally:
            transport.root_lock.require_acquired()
            await asyncio.sleep(0)
            trace.append("child_finalizer")
            finalizer_finished.set()

    observed_cancellation: list[asyncio.CancelledError] = []

    async def owner() -> None:
        try:
            async with transport.session() as channel:
                asyncio.create_task(channel.exchange(_dummy_command()))
                await started.wait()
                await asyncio.Event().wait()
        except asyncio.CancelledError as error:
            observed_cancellation.append(error)
            raise

    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)
    monkeypatch.setattr(_FileBridgeChannel, "_perform_exchange", stalled)
    task = asyncio.create_task(owner())
    await started.wait()
    task.cancel("body cancellation")

    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    assert observed_cancellation == [caught.value]
    assert observed_cancellation[0] is caught.value
    assert caught.value.args == ("body cancellation",)
    assert finalizer_finished.is_set()
    assert trace == ["child_finalizer", "unlock"]


class CountingExchangeTask(asyncio.Task[ResponseArtifact]):
    cancel_calls: int

    def __init__(self, coroutine: object, before_cancel: Callable[[], None]) -> None:
        self.cancel_calls = 0
        self.before_cancel = before_cancel
        super().__init__(cast(Any, coroutine), loop=asyncio.get_running_loop())

    def cancel(self, msg: object = None) -> bool:
        self.before_cancel()
        self.cancel_calls += 1
        return super().cancel(msg)


@pytest.mark.asyncio
async def test_session_exit_marks_closed_before_cancel_and_cancels_child_exactly_once(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace: list[str] = []
    transport = _transport(harness)
    _patch_empty_journal(monkeypatch, transport)
    started = asyncio.Event()
    finalizer_started = asyncio.Event()
    permit_finalizer = asyncio.Event()
    original_exit = RootLock.__aexit__

    async def stalled(
        _channel: _FileBridgeChannel,
        _command: ExchangeCommand,
    ) -> ResponseArtifact:
        try:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        finally:
            transport.root_lock.require_acquired()
            trace.append("child_finalizer_started")
            finalizer_started.set()
            await permit_finalizer.wait()
            trace.append("child_finalizer_finished")

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        assert trace[-1] == "child_finalizer_finished"
        result = await original_exit(lock, exc_type, exc, traceback)
        trace.append("unlock")
        return result

    monkeypatch.setattr(_FileBridgeChannel, "_perform_exchange", stalled)
    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)
    session = transport.session()
    channel = await session.__aenter__()
    concrete_channel = cast(_FileBridgeChannel, channel)

    def assert_close_won_before_cancel_entry() -> None:
        assert concrete_channel._closing is True  # pyright: ignore[reportPrivateUsage]
        assert concrete_channel._closed is False  # pyright: ignore[reportPrivateUsage]
        trace.append("cancel_entered_after_close")

    child = CountingExchangeTask(
        channel.exchange(_dummy_command()),
        assert_close_won_before_cancel_entry,
    )
    await started.wait()
    exit_task = asyncio.create_task(session.__aexit__(None, None, None))
    await finalizer_started.wait()

    with pytest.raises(BridgeError) as closed:
        await channel.exchange(_dummy_command())
    assert closed.value.code is ErrorCode.STATE_CONFLICT
    assert child.cancel_calls == 1
    assert not exit_task.done()

    permit_finalizer.set()
    assert await exit_task is False
    assert child.cancelled()
    assert child.cancel_calls == 1
    assert trace == [
        "cancel_entered_after_close",
        "child_finalizer_started",
        "child_finalizer_finished",
        "unlock",
    ]
    assert child not in asyncio.all_tasks()
    stable_trace = tuple(trace)
    await asyncio.sleep(0)
    assert tuple(trace) == stable_trace


@pytest.mark.asyncio
async def test_child_quiesces_then_blocked_safety_keeps_channel_closed_until_unlock(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    _patch_empty_journal(monkeypatch, transport)
    child_started = asyncio.Event()
    safety_started = asyncio.Event()
    permit_safety = asyncio.Event()
    trace: list[str] = []
    child_cancellations: list[asyncio.CancelledError] = []
    child: asyncio.Task[ResponseArtifact] | None = None
    original_exit = RootLock.__aexit__

    async def stalled(
        _channel: _FileBridgeChannel,
        _command: ExchangeCommand,
    ) -> ResponseArtifact:
        child_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as error:
            child_cancellations.append(error)
            trace.append("child_quiescent")
            raise
        raise AssertionError("stalled child resumed")

    async def blocked_safety(_channel: _FileBridgeChannel) -> None:
        assert child is not None and child.done()
        trace.append("safety_started")
        safety_started.set()
        await permit_safety.wait()
        trace.append("safety_finished")

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        assert trace[-1] == "safety_finished"
        result = await original_exit(lock, exc_type, exc, traceback)
        trace.append("unlock")
        return result

    monkeypatch.setattr(_FileBridgeChannel, "_perform_exchange", stalled)
    monkeypatch.setattr(_FileBridgeChannel, "_last_safety_attempt", blocked_safety)
    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)
    manager = transport.session()
    channel = await manager.__aenter__()
    child = asyncio.create_task(channel.exchange(_dummy_command()))
    await child_started.wait()
    exit_task = asyncio.create_task(manager.__aexit__(None, None, None))
    await safety_started.wait()

    assert child.done() and child.cancelled()
    assert len(child_cancellations) == 1
    with pytest.raises(BridgeError) as while_safety:
        await channel.exchange(_dummy_command())
    assert while_safety.value.code is ErrorCode.STATE_CONFLICT
    monkeypatch.setattr(transport.journals, "load", _forbidden)
    monkeypatch.setattr(RequestLedger, "get_request", _forbidden)
    monkeypatch.setattr(RequestLedger, "insert_prepared", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)

    permit_safety.set()
    assert await exit_task is False
    with pytest.raises(asyncio.CancelledError) as child_result:
        await child
    assert child_result.value is child_cancellations[0]
    with pytest.raises(BridgeError) as after_unlock:
        await channel.exchange(_dummy_command())
    assert after_unlock.value.code is ErrorCode.STATE_CONFLICT
    assert trace == [
        "child_quiescent",
        "safety_started",
        "safety_finished",
        "unlock",
    ]
    stable_trace = tuple(trace)
    await asyncio.sleep(0)
    assert tuple(trace) == stable_trace


def _command(
    harness: Harness,
    operation: str,
    public_arguments: dict[str, object],
    *,
    request_id: UUID,
    expected_lineage_id: UUID | None = LINEAGE_ID,
    expected_activation_id: UUID | None = ACTIVATION_ID,
    activation_candidate: UUID | None = None,
    trusted_enrichment: dict[str, object] | None = None,
    timeout: float = 0.3,
) -> ExchangeCommand:
    if activation_candidate is not None and trusted_enrichment is not None:
        raise AssertionError("command helper received two trusted enrichment sources")
    trusted = trusted_enrichment
    if activation_candidate is not None:
        trusted = {"activation_candidate": activation_candidate}
    invocation = OPERATION_REGISTRY.resolve_invocation(
        operation,
        public_arguments,
        trusted,
    )
    body = RequestBody(
        protocol=harness.snapshot.protocol,
        release_id=harness.snapshot.release_id,
        runtime_version=harness.snapshot.runtime_version,
        runtime_tag=harness.snapshot.runtime_tag,
        runtime_asset_sha256=harness.snapshot.runtime_asset_sha256,
        expected_lineage_id=expected_lineage_id,
        expected_activation_id=expected_activation_id,
        operation_manifest_sha256=harness.snapshot.operation_manifest_sha256,
        operation=operation,
        arguments=cast(
            dict[str, JsonValue],
            invocation.wire_arguments.model_dump(mode="json"),
        ),
    )
    return ExchangeCommand(
        request_id=request_id,
        body=body,
        invocation=invocation,
        runtime_snapshot=harness.snapshot,
        timeout=timeout,
    )


def _status_command(
    harness: Harness,
    *,
    request_id: UUID = REQUEST_ID,
    bootstrap: bool = True,
) -> ExchangeCommand:
    return _command(
        harness,
        "bridge.status",
        {},
        request_id=request_id,
        expected_lineage_id=None if bootstrap else LINEAGE_ID,
        expected_activation_id=None if bootstrap else ACTIVATION_ID,
        activation_candidate=UUID("55555555-5555-4555-8555-555555555555"),
    )


def _scenario_command(
    harness: Harness,
    *,
    request_id: UUID = REQUEST_ID,
) -> ExchangeCommand:
    return _command(
        harness,
        "scenario.get",
        {},
        request_id=request_id,
    )


def _peer(
    harness: Harness,
    *,
    trace: list[str] | None = None,
) -> FakeFileBridgePeer:
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    return FakeFileBridgePeer(
        paths=harness.paths,
        runtime_snapshot=harness.snapshot,
        registry=OPERATION_REGISTRY,
        scenario_lineage_id=LINEAGE_ID,
        poll_seconds=0.001,
        trace=trace,
    )


@pytest.mark.asyncio
async def test_bootstrap_status_persists_artifact_first_then_completes_and_cleans_up(
    harness: Harness,
) -> None:
    transport = _transport(harness)
    command = _status_command(harness)
    peer = _peer(harness)
    result = peer.result_for(command.invocation)
    peer.enqueue(Respond(result=result))
    response_path = harness.paths.response_path(command.request_id)
    artifact: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)
            assert response_path.exists()

    assert artifact is not None
    assert artifact.accepted_response.envelope.result == result
    assert isinstance(artifact.accepted_response.settlement, CompletedSettlement)
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.COMPLETED
    assert request.result_json == _canonical_json(result).decode("utf-8")
    assert request.error_json is None
    assert request.terminal_at_ms == request.updated_at_ms
    assert not response_path.exists()


@pytest.mark.asyncio
async def test_nonbootstrap_status_uses_known_lineage_and_new_activation_candidate(
    harness: Harness,
) -> None:
    transport = _transport(harness)
    command = _status_command(harness, bootstrap=False)
    candidate = cast(
        BridgeStatusWireArgs,
        command.invocation.wire_arguments,
    ).activation_candidate
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    artifact: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)

    assert artifact is not None
    envelope = artifact.accepted_response.envelope
    assert envelope.scenario_lineage_id == LINEAGE_ID
    assert envelope.activation_id == candidate
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.lineage_id == LINEAGE_ID
    assert request.activation_id == ACTIVATION_ID


def test_fake_peer_has_valid_complete_baselines_for_every_public_h8_read(
    harness: Harness,
) -> None:
    commands = (
        _scenario_command(harness),
        _command(
            harness,
            "side.list",
            {},
            request_id=UUID("aaaaaaaa-1000-4000-8000-000000000001"),
        ),
        _command(
            harness,
            "unit.list",
            {"side_guid": "SIDE-1"},
            request_id=UUID("aaaaaaaa-1000-4000-8000-000000000002"),
        ),
        _command(
            harness,
            "unit.get",
            {"unit_guid": "UNIT-1"},
            request_id=UUID("aaaaaaaa-1000-4000-8000-000000000003"),
        ),
        _command(
            harness,
            "contact.list",
            {"side_guid": "SIDE-1"},
            request_id=UUID("aaaaaaaa-1000-4000-8000-000000000004"),
        ),
        _command(
            harness,
            "mission.list",
            {"side_guid": "SIDE-1"},
            request_id=UUID("aaaaaaaa-1000-4000-8000-000000000005"),
        ),
        _command(
            harness,
            "mission.get",
            {"side_guid": "SIDE-1", "mission_guid": "MISSION-1"},
            request_id=UUID("aaaaaaaa-1000-4000-8000-000000000006"),
        ),
        _command(
            harness,
            "doctrine.get",
            {"scope": "side", "side_guid": "SIDE-1"},
            request_id=UUID("aaaaaaaa-1000-4000-8000-000000000007"),
        ),
        _command(
            harness,
            "lua.call",
            {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}},
            request_id=UUID("aaaaaaaa-1000-4000-8000-000000000008"),
        ),
        _command(
            harness,
            "bridge.reconcile",
            {},
            request_id=UUID("aaaaaaaa-1000-4000-8000-000000000009"),
        ),
    )
    peer = _peer(harness)

    for command in commands:
        assert command.invocation.effective_class is OperationClass.READ
        result = peer.result_for(command.invocation)
        command.invocation.result_adapter.validate_python(result)


@pytest.mark.asyncio
async def test_scenario_read_round_trips_unicode_multiline_and_exact_durable_records(
    harness: Harness,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    result = peer.result_for(
        command.invocation,
        title="海峡态势\n第二行",
        file_name="测试.scen",
        file_name_path="C:\\CMO\\Scenarios",
    )
    peer.enqueue(Respond(result=result))
    artifact: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)

    assert artifact is not None
    assert artifact.accepted_response.envelope.result == result
    observed = peer.observed_deliveries
    assert len(observed) == 1
    delivery = observed[0]
    request = transport.ledger.get_request(command.request_id)
    durable_delivery = transport.ledger.get_delivery(delivery.delivery_id)
    assert request is not None
    assert durable_delivery is not None
    rendered = render_delivery_lua(delivery, harness.snapshot)
    assert request.request_id == command.request_id
    assert request.root_key == harness.paths.root_key
    assert request.request_hash == delivery.request_hash
    assert request.operation == command.body.operation
    assert request.operation_class is OperationClass.READ
    assert request.runtime_snapshot == command.runtime_snapshot
    assert request.result_schema_id == command.invocation.result_schema.schema_id
    assert request.recovery_schema_id is None
    assert request.body_json == canonical_body_bytes(command.body)
    assert request.lineage_id == LINEAGE_ID
    assert request.activation_id == ACTIVATION_ID
    assert request.created_at_ms <= request.updated_at_ms
    assert durable_delivery.request_id == command.request_id
    assert durable_delivery.delivery_kind == "request"
    assert durable_delivery.original_request_delivery_id == delivery.delivery_id
    assert durable_delivery.rendered_inbox_sha256 == hashlib.sha256(rendered).hexdigest()
    assert durable_delivery.rendered_inbox_size_bytes == len(rendered)
    assert durable_delivery.response_filename == artifact.filename
    assert durable_delivery.response_artifact == artifact
    assert durable_delivery.settlement == artifact.accepted_response.settlement


@pytest.mark.asyncio
async def test_accepted_read_error_is_returned_and_terminally_rejected(
    harness: Harness,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    error = cast(
        dict[str, JsonValue],
        {
            "code": "NOT_FOUND",
            "message": "scenario result is unavailable",
            "details": {"selector": "current"},
            "mutation_not_started": None,
        },
    )
    peer.enqueue(Respond(error=error))
    artifact: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)

    assert artifact is not None
    assert isinstance(artifact.accepted_response.settlement, RejectedSettlement)
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.REJECTED
    assert artifact.accepted_response.envelope.error is not None
    assert request.error_json == _canonical_json(
        artifact.accepted_response.envelope.error.model_dump(mode="json")
    ).decode("utf-8")


@pytest.mark.asyncio
async def test_manifest_mismatch_keeps_literal_null_settlement_and_rejects_host_request(
    harness: Harness,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    error = cast(
        dict[str, JsonValue],
        {
            "code": "MANIFEST_MISMATCH",
            "message": "peer release differs",
            "details": {},
            "mutation_not_started": None,
        },
    )
    asset = "1" * 64
    peer.enqueue(
        Respond(
            error=error,
            envelope_overrides=(
                ("operation_manifest_sha256", "2" * 64),
                ("bridge_version", "9.8.7"),
                ("runtime_tag", f"9_8_7-{asset}"),
                ("runtime_asset_sha256", asset),
                ("release_id", "3" * 64),
            ),
        )
    )
    artifact: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)

    assert artifact is not None
    assert artifact.accepted_response.settlement is None
    delivery_id = artifact.accepted_response.envelope.delivery_id
    durable_delivery = transport.ledger.get_delivery(delivery_id)
    request = transport.ledger.get_request(command.request_id)
    assert durable_delivery is not None
    assert durable_delivery.settlement is None
    assert durable_delivery.response_artifact == artifact
    assert request is not None
    assert request.state is HostRequestState.REJECTED
    assert request.terminal_at_ms == request.updated_at_ms


def test_fake_peer_projection_is_nonempty_exact_and_argument_dependent(
    harness: Harness,
) -> None:
    peer = _peer(harness)
    names = _command(
        harness,
        "side.list",
        {"fields": ["name"]},
        request_id=UUID("aaaaaaaa-0000-4000-8000-000000000001"),
    )
    awareness = _command(
        harness,
        "side.list",
        {"fields": ["awareness"]},
        request_id=UUID("aaaaaaaa-0000-4000-8000-000000000002"),
    )

    names_result = cast(dict[str, JsonValue], peer.result_for(names.invocation))
    awareness_result = cast(dict[str, JsonValue], peer.result_for(awareness.invocation))
    names_items = cast(list[dict[str, JsonValue]], names_result["items"])
    awareness_items = cast(list[dict[str, JsonValue]], awareness_result["items"])
    assert len(names_items) == len(awareness_items) == 1
    assert set(names_items[0]) == {"guid", "name"}
    assert set(awareness_items[0]) == {"guid", "awareness"}
    bad_item = dict(names_items[0])
    bad_item["awareness"] = "Omniscient"
    with pytest.raises((AssertionError, ValidationError)):
        peer.result_for(names.invocation, items=[bad_item])


@pytest.mark.asyncio
async def test_omitted_and_explicit_defaults_rebuild_as_distinct_legitimate_invocations(
    harness: Harness,
) -> None:
    omitted = _command(
        harness,
        "side.list",
        {},
        request_id=UUID("aaaaaaaa-0000-4000-8000-000000000003"),
    )
    explicit = _command(
        harness,
        "side.list",
        {"page_size": 100, "cursor": None, "fields": None},
        request_id=UUID("aaaaaaaa-0000-4000-8000-000000000004"),
    )
    assert omitted.invocation.public_arguments.model_fields_set == set()
    assert explicit.invocation.public_arguments.model_fields_set == {
        "page_size",
        "cursor",
        "fields",
    }
    transport = _transport(harness)
    peer = _peer(harness)
    peer.enqueue(
        Respond(result=peer.result_for(omitted.invocation)),
        Respond(result=peer.result_for(explicit.invocation)),
    )
    first: ResponseArtifact | None = None
    second: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            first = await channel.exchange(omitted)
            second = await channel.exchange(explicit)

    assert first is not None
    assert second is not None
    assert first.accepted_response.envelope.ok is True
    assert second.accepted_response.envelope.ok is True


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["mutated_body", "model_construct", "invocation_drift"])
async def test_forged_command_boundaries_fail_before_sqlite_or_inbox(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    command = _scenario_command(harness)
    if case == "mutated_body":
        command.body.arguments["forged"] = True
    elif case == "model_construct":
        forged_values: dict[str, Any] = command.body.model_dump(mode="python")
        forged_values["operation"] = "unit.delete"
        forged_body = RequestBody.model_construct(**forged_values)
        command = replace(command, body=forged_body)
    else:
        wrong_invocation = OPERATION_REGISTRY.resolve_invocation(
            "unit.get", {"unit_guid": "UNIT-1"}
        )
        command = replace(command, invocation=wrong_invocation)
    transport = _transport(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)

    async with transport.session() as channel:
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(command)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.inbox.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "bool_timeout",
        "nonnumeric_timeout",
        "negative_timeout",
        "nonfinite_timeout",
        "nan_timeout",
        "non_v4_request",
        "half_context",
        "half_context_lineage",
        "status_half_context",
    ],
)
async def test_invalid_timeout_uuid_and_context_fail_before_sqlite(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    command = _scenario_command(harness)
    if case == "bool_timeout":
        command = replace(command, timeout=cast(float, True))
    elif case == "nonnumeric_timeout":
        command = replace(command, timeout=cast(float, "0.1"))
    elif case == "negative_timeout":
        command = replace(command, timeout=-0.1)
    elif case == "nonfinite_timeout":
        command = replace(command, timeout=math.inf)
    elif case == "nan_timeout":
        command = replace(command, timeout=math.nan)
    elif case == "non_v4_request":
        command = replace(command, request_id=UUID("aaaaaaaa-0000-1000-8000-000000000001"))
    elif case == "half_context":
        command = replace(
            command,
            body=command.body.model_copy(update={"expected_activation_id": None}),
        )
    elif case == "half_context_lineage":
        command = replace(
            command,
            body=command.body.model_copy(update={"expected_lineage_id": None}),
        )
    else:
        command = _status_command(harness)
        command = replace(
            command,
            body=command.body.model_copy(update={"expected_lineage_id": LINEAGE_ID}),
        )
    transport = _transport(harness)
    monkeypatch.setattr(sqlite3, "connect", _forbidden)

    async with transport.session() as channel:
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(command)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert not harness.paths.sqlite_file.exists()
    assert not harness.paths.inbox.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("different_hash", [False, True])
async def test_existing_request_id_is_always_live_exchange_conflict_without_second_delivery(
    harness: Harness,
    different_hash: bool,
) -> None:
    first = _status_command(harness)
    second = first
    if different_hash:
        second = _command(
            harness,
            "bridge.status",
            {"accept_lineage_id": LINEAGE_ID},
            request_id=first.request_id,
            expected_lineage_id=None,
            expected_activation_id=None,
            activation_candidate=cast(
                BridgeStatusWireArgs,
                first.invocation.wire_arguments,
            ).activation_candidate,
        )
    transport = _transport(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(first.invocation)))
    before: RequestRecord | None = None
    after: RequestRecord | None = None
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            await channel.exchange(first)
            before = transport.ledger.get_request(first.request_id)
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(second)
            after = transport.ledger.get_request(first.request_id)

    assert caught is not None
    assert before is not None
    assert after is not None
    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert before == after
    assert len(peer.observed_deliveries) == 1


@pytest.mark.asyncio
async def test_preexisting_unowned_response_path_is_never_published_or_removed(
    harness: Harness,
) -> None:
    command = _scenario_command(harness)
    response_path = harness.paths.response_path(command.request_id)
    response_path.parent.mkdir(parents=True, exist_ok=True)
    original = b"user-owned-response-slot"
    response_path.write_bytes(original)
    transport = _transport(harness)

    async with transport.session() as channel:
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(command)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert response_path.read_bytes() == original
    assert not harness.paths.inbox.exists()
    assert transport.ledger.get_request(command.request_id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("overwrite_boundary", ["after_artifact", "after_unlock"])
async def test_response_overwrite_before_pin_is_retained_and_after_pin_is_blocked(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    overwrite_boundary: str,
) -> None:
    command = _scenario_command(harness)
    response_path = harness.paths.response_path(command.request_id)
    replacement = b"later request-owned protocol material"
    transport = _transport(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    original_record = transport.ledger.record_response
    original_exit = RootLock.__aexit__
    overwrite_error: OSError | None = None

    def record_then_overwrite(artifact: ResponseArtifact) -> object:
        result = original_record(artifact)
        response_path.write_bytes(replacement)
        return result

    async def unlock_then_overwrite(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        nonlocal overwrite_error
        result = await original_exit(lock, exc_type, exc, traceback)
        try:
            response_path.write_bytes(replacement)
        except OSError as error:
            overwrite_error = error
        return result

    if overwrite_boundary == "after_artifact":
        monkeypatch.setattr(transport.ledger, "record_response", record_then_overwrite)
    else:
        monkeypatch.setattr(RootLock, "__aexit__", unlock_then_overwrite)
    artifact: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)
            if overwrite_boundary == "after_artifact":
                assert response_path.exists()

    assert artifact is not None
    assert artifact.sha256 != hashlib.sha256(replacement).hexdigest()
    if overwrite_boundary == "after_artifact":
        assert overwrite_error is None
        assert response_path.read_bytes() == replacement
    else:
        assert overwrite_error is not None
        assert isinstance(overwrite_error, PermissionError) or getattr(
            overwrite_error, "winerror", None
        ) in {5, 32}
        assert not response_path.exists()
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.COMPLETED


@pytest.mark.asyncio
async def test_status_timeout_retry_rebuilds_expectations_with_one_shared_deadline(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = replace(_status_command(harness), timeout=0.1)
    peer = _peer(harness)
    peer.enqueue(StaySilent(), Respond(result=peer.result_for(command.invocation)))
    expectations: list[object] = []
    timeouts: list[float] = []
    original_init = transport_module.ResponseWaiter.__init__
    original_wait = transport_module.ResponseWaiter.wait

    def capture_init(
        waiter: object,
        response_path: Path,
        expectation: object,
        expected_process: ProcessInfo,
        process_check: Callable[[], ProcessInfo],
        poll_seconds: float = 0.05,
    ) -> None:
        expectations.append(expectation)
        original_init(
            cast(transport_module.ResponseWaiter, waiter),
            response_path,
            cast(Any, expectation),
            expected_process,
            process_check,
            poll_seconds,
        )

    async def capture_wait(
        waiter: transport_module.ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        timeouts.append(timeout_seconds)
        return await original_wait(waiter, timeout_seconds)

    monkeypatch.setattr(transport_module.ResponseWaiter, "__init__", capture_init)
    monkeypatch.setattr(transport_module.ResponseWaiter, "wait", capture_wait)
    artifact: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)

    assert artifact is not None
    assert artifact.accepted_response.envelope.ok is True
    assert timeouts[0] == pytest.approx(command.timeout / 2)
    assert 0 <= timeouts[1] <= command.timeout / 2
    assert sum(timeouts) <= command.timeout + 0.001
    assert len(expectations) == 2
    first = cast(Any, expectations[0])
    second = cast(Any, expectations[1])
    assert first.request_id == second.request_id == command.request_id
    assert first.request_hash == second.request_hash
    assert first.runtime_snapshot == second.runtime_snapshot == command.runtime_snapshot
    assert first.invocation is second.invocation
    assert first.invocation == command.invocation
    assert first.invocation is not command.invocation
    assert first.expected_lineage_id is second.expected_lineage_id is None
    assert first.expected_activation_id is second.expected_activation_id is None
    assert first.status_bootstrap is second.status_bootstrap is True
    candidate = cast(
        BridgeStatusWireArgs,
        command.invocation.wire_arguments,
    ).activation_candidate
    assert first.activation_candidate == second.activation_candidate == candidate
    assert len(first.allowed_deliveries) == 1
    assert len(second.allowed_deliveries) == 2
    assert second.allowed_deliveries[0] == first.allowed_deliveries[0]


@pytest.mark.asyncio
async def test_post_unlock_cleanup_failure_is_nonfatal_and_leaves_owned_slot(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _scenario_command(harness)
    response_path = harness.paths.response_path(command.request_id)
    transport = _transport(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    original_delete = cleanup_module._set_delete_disposition  # pyright: ignore[reportPrivateUsage]

    def fail_owned_delete(handle: int, path: Path) -> None:
        if path == response_path.resolve(strict=True):
            raise PermissionError("cleanup blocked")
        original_delete(handle, path)

    monkeypatch.setattr(cleanup_module, "_set_delete_disposition", fail_owned_delete)
    artifact: ResponseArtifact | None = None
    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)

    assert artifact is not None
    assert artifact.accepted_response.envelope.ok is True
    assert response_path.exists()
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.COMPLETED


@pytest.mark.asyncio
async def test_terminal_cleanup_pins_response_before_unlock_and_never_uses_path_unlink(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    response_path = harness.paths.response_path(command.request_id)
    displaced = harness.paths.import_export / "attacker-displaced-response.inst"
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    original_exit = RootLock.__aexit__
    original_unlink = Path.unlink
    replacement_error: OSError | None = None

    async def replace_after_unlock(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        nonlocal replacement_error
        result = await original_exit(lock, exc_type, exc, traceback)
        try:
            response_path.replace(displaced)
            response_path.write_bytes(b"attacker replacement")
        except OSError as error:
            replacement_error = error
        return result

    def response_unlink_forbidden(path: Path, *, missing_ok: bool = False) -> None:
        if path == response_path:
            raise AssertionError("terminal cleanup must delete only through its pinned handle")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(RootLock, "__aexit__", replace_after_unlock)
    monkeypatch.setattr(Path, "unlink", response_unlink_forbidden)

    artifact: ResponseArtifact | None = None
    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)

    assert artifact is not None
    assert artifact.accepted_response.envelope.ok is True
    assert replacement_error is not None
    assert isinstance(replacement_error, PermissionError) or getattr(
        replacement_error, "winerror", None
    ) in {5, 32}
    assert not response_path.exists()
    assert not displaced.exists()


def test_fake_peer_rejects_primary_correlation_and_partial_runtime_overrides(
    harness: Harness,
) -> None:
    command = _scenario_command(harness)
    peer = _peer(harness)
    result = peer.result_for(command.invocation)

    with pytest.raises((AssertionError, ValueError)):
        peer.enqueue(
            Respond(
                result=result,
                envelope_overrides=(("request_id", str(UUID(int=1))),),
            )
        )


@pytest.mark.asyncio
async def test_deliberate_context_override_reaches_real_parser_mismatch_path(
    harness: Harness,
) -> None:
    command = _scenario_command(harness)
    transport = _transport(harness)
    peer = _peer(harness)
    peer.enqueue(
        Respond(
            result=peer.result_for(command.invocation),
            envelope_overrides=(
                (
                    "scenario_lineage_id",
                    str(UUID("99999999-9999-4999-8999-999999999999")),
                ),
            ),
        )
    )
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

    assert caught is not None
    assert caught.value.code is ErrorCode.PROTOCOL_ERROR
    assert len(peer.observed_deliveries) == 1
    with pytest.raises((AssertionError, ValueError)):
        peer.enqueue(
            Respond(
                error=cast(
                    dict[str, JsonValue],
                    {
                        "code": "MANIFEST_MISMATCH",
                        "message": "mismatch",
                        "details": {},
                        "mutation_not_started": None,
                    },
                ),
                envelope_overrides=(("release_id", "f" * 64),),
            )
        )


@pytest.mark.asyncio
async def test_success_happens_before_trace_and_cleanup_after_unlock(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace: list[str] = []
    inspector = Inspector((harness.process,), trace=trace)
    transport = _transport(harness, process_inspector=inspector)
    command = _scenario_command(harness)
    response_path = harness.paths.response_path(command.request_id)
    peer = _peer(harness, trace=trace)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    original_enter = RootLock.__aenter__
    original_exit = RootLock.__aexit__
    original_load = transport.journals.load
    original_prepared = transport.ledger.insert_prepared
    original_intent = transport.ledger.insert_delivery
    original_mark = transport.ledger.mark_delivery_published
    original_response = transport.ledger.record_response
    original_transition = transport.ledger.transition
    original_publish_delivery = InboxPublisher.publish_delivery
    original_publish_idle = InboxPublisher.publish_idle
    original_delete = cleanup_module._set_delete_disposition  # pyright: ignore[reportPrivateUsage]

    async def traced_enter(lock: RootLock) -> RootLock:
        result = await original_enter(lock)
        trace.append("lock")
        return result

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        assert response_path.exists()
        result = await original_exit(lock, exc_type, exc, traceback)
        trace.append("unlock")
        return result

    def traced_load() -> object:
        result = original_load()
        trace.append("journal_gate")
        return result

    def traced_prepared(record: RequestRecord) -> None:
        original_prepared(record)
        trace.append("ledger.prepared")

    def traced_intent(intent: DeliveryIntent) -> None:
        original_intent(intent)
        trace.append("ledger.intent")

    def traced_mark(delivery_id: UUID, *, published_at_ms: int) -> object:
        result = original_mark(delivery_id, published_at_ms=published_at_ms)
        trace.append("ledger.published")
        return result

    def traced_response(artifact: ResponseArtifact) -> object:
        result = original_response(artifact)
        trace.append("ledger.response")
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
            HostRequestState.RESPONSE_ACCEPTED: "ledger.response_accepted",
            HostRequestState.IDLE_PUBLISHED: "ledger.idle_published",
            HostRequestState.COMPLETED: "ledger.terminal",
        }.get(new_state)
        if event is not None:
            assert response_path.exists()
            trace.append(event)
        return result

    def traced_publish_delivery(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        original_publish_delivery(
            publisher,
            cast(Any, delivery),
            runtime_snapshot=runtime_snapshot,
        )
        trace.append("inbox.request_replaced")

    def traced_publish_idle(publisher: InboxPublisher) -> None:
        assert response_path.exists()
        original_publish_idle(publisher)
        trace.append("inbox.idle_replaced")

    def traced_delete(handle: int, path: Path) -> None:
        if path == response_path.resolve(strict=True):
            trace.append("cleanup")
        original_delete(handle, path)

    monkeypatch.setattr(RootLock, "__aenter__", traced_enter)
    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)
    monkeypatch.setattr(transport.journals, "load", traced_load)
    monkeypatch.setattr(transport.ledger, "insert_prepared", traced_prepared)
    monkeypatch.setattr(transport.ledger, "insert_delivery", traced_intent)
    monkeypatch.setattr(transport.ledger, "mark_delivery_published", traced_mark)
    monkeypatch.setattr(transport.ledger, "record_response", traced_response)
    monkeypatch.setattr(transport.ledger, "transition", traced_transition)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", traced_publish_delivery)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_publish_idle)
    monkeypatch.setattr(cleanup_module, "_set_delete_disposition", traced_delete)

    async with peer:
        async with transport.session() as channel:
            await channel.exchange(command)

    def before(left: str, right: str) -> None:
        assert trace.index(left) < trace.index(right), trace

    for left, right in (
        ("lock", "process_pin"),
        ("process_pin", "journal_gate"),
        ("journal_gate", "ledger.prepared"),
        ("ledger.prepared", "ledger.intent"),
        ("ledger.intent", "process_prepublication"),
        ("process_prepublication", "inbox.request_replaced"),
        ("inbox.request_replaced", "ledger.published"),
        ("inbox.request_replaced", "peer.request_observed"),
        ("peer.request_observed", "peer.response_written"),
        ("ledger.published", "waiter_process_check"),
        ("waiter_process_check", "ledger.response"),
        ("peer.response_written", "ledger.response"),
        ("ledger.response", "ledger.response_accepted"),
        ("ledger.response_accepted", "inbox.idle_replaced"),
        ("inbox.idle_replaced", "ledger.idle_published"),
        ("ledger.idle_published", "ledger.terminal"),
        ("ledger.terminal", "unlock"),
        ("unlock", "cleanup"),
    ):
        before(left, right)
    assert harness.paths.inbox.read_bytes() == render_idle_lua()


@pytest.mark.asyncio
async def test_waiter_accepts_partial_response_completion_across_shared_budget_halves(
    harness: Harness,
) -> None:
    transport = _transport(harness, response_poll_seconds=0.01)
    command = replace(_scenario_command(harness), timeout=0.5)
    peer = _peer(harness)
    response = Respond(result=peer.result_for(command.invocation))
    peer.enqueue(WritePartialThenComplete(response=response, delay_seconds=0.3))
    artifact: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)

    assert artifact is not None
    assert (
        artifact.accepted_response.envelope.delivery_id == peer.observed_deliveries[0].delivery_id
    )
    assert len(peer.observed_deliveries) == 1
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.COMPLETED
    assert harness.paths.inbox.read_bytes() == render_idle_lua()


@pytest.mark.asyncio
async def test_mismatched_delivery_fails_immediately_instead_of_accepting_later_rewrite(
    harness: Harness,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    response = Respond(result=peer.result_for(command.invocation))
    peer.enqueue(WriteMismatchedDelivery(response=response, delay_seconds=0.2))

    with pytest.raises(BridgeError) as caught:
        async with peer:
            async with transport.session() as channel:
                await channel.exchange(command)

    assert caught.value.code is ErrorCode.PROTOCOL_ERROR
    assert caught.value.message == "response delivery ID is not uniquely allowed"
    assert len(peer.observed_deliveries) == 1
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.REJECTED
    assert harness.paths.inbox.read_bytes() == render_idle_lua()


@pytest.mark.asyncio
async def test_first_timeout_retries_once_with_same_identity_and_second_delivery(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = replace(_scenario_command(harness), timeout=0.1)
    peer = _peer(harness)
    peer.enqueue(
        StaySilent(),
        Respond(result=peer.result_for(command.invocation)),
    )
    intents: list[DeliveryIntent] = []
    original_insert = transport.ledger.insert_delivery

    def record_intent(intent: DeliveryIntent) -> None:
        if intents:
            owner = transport.ledger.get_request(command.request_id)
            assert owner is not None
            assert owner.state is HostRequestState.PUBLISHED
            assert owner.terminal_at_ms is None
            with sqlite3.connect(harness.paths.sqlite_file) as connection:
                assert connection.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 1
                assert connection.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == 1
        intents.append(intent)
        original_insert(intent)

    monkeypatch.setattr(transport.ledger, "insert_delivery", record_intent)
    artifact: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)

    assert artifact is not None
    assert len(intents) == 2
    first, retry = intents
    assert first.request_id == retry.request_id == command.request_id
    assert first.delivery_id != retry.delivery_id
    assert first.delivery_id.version == retry.delivery_id.version == 4
    assert first.delivery_kind == retry.delivery_kind == "request"
    assert first.original_request_delivery_id == first.delivery_id
    assert retry.original_request_delivery_id == retry.delivery_id
    assert first.body_json == retry.body_json == canonical_body_bytes(command.body).decode("utf-8")
    assert first.request_hash == retry.request_hash
    assert first.runtime_snapshot == retry.runtime_snapshot == command.runtime_snapshot
    assert retry.intended_at_ms >= first.intended_at_ms + 1
    assert artifact.accepted_response.envelope.delivery_id == retry.delivery_id
    assert len(peer.observed_deliveries) == 2
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.COMPLETED


@pytest.mark.asyncio
async def test_retry_expectation_accepts_delayed_original_delivery_response(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = replace(_scenario_command(harness), timeout=0.02)
    peer = _peer(harness)
    response = Respond(result=peer.result_for(command.invocation))
    peer.enqueue(StaySilent(), StaySilent())
    intents: list[DeliveryIntent] = []
    waiter_calls = 0
    publish_calls = 0
    original_insert = transport.ledger.insert_delivery
    original_wait = transport_module.ResponseWaiter.wait
    original_publish = InboxPublisher.publish_delivery
    first_timeout = BridgeError(ErrorCode.REQUEST_TIMEOUT, "deterministic first timeout")

    def record_intent(intent: DeliveryIntent) -> None:
        intents.append(intent)
        original_insert(intent)

    async def deterministic_wait(
        waiter: transport_module.ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        nonlocal waiter_calls
        waiter_calls += 1
        if waiter_calls == 1:
            async with asyncio.timeout(2):
                while len(peer.observed_deliveries) != 1:
                    await asyncio.sleep(0)
            raise first_timeout
        return await original_wait(waiter, timeout_seconds)

    def publish_then_write_original_response(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal publish_calls
        publish_calls += 1
        original_publish(
            publisher,
            cast(Any, delivery),
            runtime_snapshot=runtime_snapshot,
        )
        if publish_calls == 2:
            original_delivery = peer.observed_deliveries[0]
            frozen = OPERATION_REGISTRY.resolve_wire_invocation(
                command.body.operation,
                command.body.arguments,
            )
            raw = peer._response_bytes(  # pyright: ignore[reportPrivateUsage]
                response,
                original_delivery,
                command.body,
                frozen,
            )
            harness.paths.response_path(command.request_id).write_bytes(raw)

    monkeypatch.setattr(transport.ledger, "insert_delivery", record_intent)
    monkeypatch.setattr(transport_module.ResponseWaiter, "wait", deterministic_wait)
    monkeypatch.setattr(
        InboxPublisher,
        "publish_delivery",
        publish_then_write_original_response,
    )
    artifact: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)

    assert artifact is not None
    assert len(intents) == 2
    assert waiter_calls == 2
    assert publish_calls == 2
    assert artifact.accepted_response.envelope.delivery_id == intents[0].delivery_id
    durable = transport.ledger.get_delivery(intents[0].delivery_id)
    assert durable is not None and durable.response_artifact == artifact


@pytest.mark.asyncio
async def test_retry_epochs_are_monotonic_while_artifact_epoch_is_preserved_exactly(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BackwardsClock:
        def __init__(self) -> None:
            self.values = iter(
                (
                    2_000_000_000_000_000_000,
                    1_900_000_000_000_000_000,
                    1_800_000_000_000_000_000,
                    1_700_000_000_000_000_000,
                    1_600_000_000_000_000_000,
                    1_500_000_000_000_000_000,
                )
            )
            self.last = 1_500_000_000_000_000_000

        def time_ns(self) -> int:
            self.last = next(self.values, self.last)
            return self.last

    monkeypatch.setattr(transport_module, "time", BackwardsClock())
    transport = _transport(harness)
    command = replace(_scenario_command(harness), timeout=0.1)
    peer = _peer(harness)
    peer.enqueue(StaySilent(), Respond(result=peer.result_for(command.invocation)))
    intents: list[DeliveryIntent] = []
    publication_epochs: list[int] = []
    transition_epochs: list[tuple[HostRequestState, int, int | None]] = []
    original_insert = transport.ledger.insert_delivery
    original_mark = transport.ledger.mark_delivery_published
    original_transition = transport.ledger.transition

    def record_intent(intent: DeliveryIntent) -> None:
        intents.append(intent)
        original_insert(intent)

    def record_publication(delivery_id: UUID, *, published_at_ms: int) -> object:
        publication_epochs.append(published_at_ms)
        return original_mark(delivery_id, published_at_ms=published_at_ms)

    def record_transition(
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
        transition_epochs.append((new_state, updated_at_ms, terminal_at_ms))
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

    monkeypatch.setattr(transport.ledger, "insert_delivery", record_intent)
    monkeypatch.setattr(transport.ledger, "mark_delivery_published", record_publication)
    monkeypatch.setattr(transport.ledger, "transition", record_transition)
    artifact: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            artifact = await channel.exchange(command)

    assert artifact is not None
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert len(intents) == 2
    assert intents[0].intended_at_ms == 2_000_000_000_000
    assert intents[1].intended_at_ms >= intents[0].intended_at_ms + 1
    assert len(publication_epochs) == 2
    assert intents[0].intended_at_ms <= publication_epochs[0]
    assert publication_epochs[0] <= intents[1].intended_at_ms <= publication_epochs[1]
    assert [state for state, _, _ in transition_epochs] == [
        HostRequestState.PUBLISHED,
        HostRequestState.RESPONSE_ACCEPTED,
        HostRequestState.IDLE_PUBLISHED,
        HostRequestState.COMPLETED,
    ]
    generated_epochs = [
        intents[0].intended_at_ms,
        publication_epochs[0],
        intents[1].intended_at_ms,
        publication_epochs[1],
        *(updated for _, updated, _ in transition_epochs[1:]),
    ]
    assert generated_epochs == sorted(generated_epochs)
    assert transition_epochs[-1][2] == transition_epochs[-1][1]
    assert request.created_at_ms <= intents[0].intended_at_ms
    assert intents[0].intended_at_ms <= intents[1].intended_at_ms <= request.updated_at_ms
    assert request.terminal_at_ms == request.updated_at_ms
    assert artifact.accepted_at_ms < request.updated_at_ms
    responder = transport.ledger.get_delivery(artifact.accepted_response.envelope.delivery_id)
    assert responder is not None
    assert responder.response_artifact is not None
    assert responder.response_artifact.accepted_at_ms == artifact.accepted_at_ms
    assert responder.response_artifact == artifact


@pytest.mark.asyncio
async def test_second_timeout_adds_no_third_delivery_idles_rejects_and_reraises(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = replace(_scenario_command(harness), timeout=0.01)
    peer = _peer(harness)
    peer.enqueue(StaySilent(), StaySilent())
    timeout_errors: list[BridgeError] = []
    wait_budgets: list[float] = []
    intents: list[DeliveryIntent] = []
    waiter_calls = 0
    publish_calls = 0
    original_wait = transport_module.ResponseWaiter.wait
    original_insert = transport.ledger.insert_delivery
    original_publish = InboxPublisher.publish_delivery

    async def capture_timeout(
        waiter: transport_module.ResponseWaiter,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        nonlocal waiter_calls
        waiter_calls += 1
        wait_budgets.append(timeout_seconds)
        async with asyncio.timeout(2):
            while len(peer.observed_deliveries) < waiter_calls:
                await asyncio.sleep(0)
        try:
            return await original_wait(waiter, timeout_seconds)
        except BridgeError as error:
            if error.code is ErrorCode.REQUEST_TIMEOUT:
                timeout_errors.append(error)
            raise

    def capture_intent(intent: DeliveryIntent) -> None:
        intents.append(intent)
        original_insert(intent)

    def capture_publish(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal publish_calls
        publish_calls += 1
        original_publish(
            publisher,
            cast(Any, delivery),
            runtime_snapshot=runtime_snapshot,
        )

    monkeypatch.setattr(transport_module.ResponseWaiter, "wait", capture_timeout)
    monkeypatch.setattr(transport.ledger, "insert_delivery", capture_intent)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", capture_publish)
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

    assert caught is not None
    assert caught.value.code is ErrorCode.REQUEST_TIMEOUT
    assert len(timeout_errors) == 2
    assert caught.value.__cause__ is timeout_errors[1]
    assert caught.value is not timeout_errors[0]
    assert wait_budgets[0] == pytest.approx(command.timeout / 2)
    assert 0 <= wait_budgets[1] <= command.timeout / 2
    assert sum(wait_budgets) <= command.timeout + 0.001
    assert caught.value.details["timeout_seconds"] == command.timeout
    assert caught.value.details["wait_attempts"] == 2
    assert caught.value.details["second_delivery_recovery"] is True
    assert caught.value.details["automatic_retry_exhausted"] is True
    assert caught.value.details["do_not_retry"] is True
    assert caught.value.details["next_tool"] == "cmo_time_get_state"
    assert "scenario_paused_after_publication" in caught.value.details["likely_causes"]
    assert len(intents) == 2
    assert publish_calls == 2
    for intent in intents:
        delivery = transport.ledger.get_delivery(intent.delivery_id)
        assert delivery is not None
        assert delivery.published_at_ms is not None
    assert len(peer.observed_deliveries) == 2
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.REJECTED
    assert request.error_json == _canonical_json(caught.value.to_payload()).decode("utf-8")
    assert request.terminal_at_ms == request.updated_at_ms
    assert harness.paths.inbox.read_bytes() == render_idle_lua()


@pytest.mark.asyncio
async def test_stable_protocol_error_never_retries_and_safely_rejects(
    harness: Harness,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(WriteMalformedComments())
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

    assert caught is not None
    assert caught.value.code is ErrorCode.PROTOCOL_ERROR
    assert len(peer.observed_deliveries) == 1
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.REJECTED
    assert harness.paths.inbox.read_bytes() == render_idle_lua()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["second_timeout", "protocol", "process"])
async def test_safe_failure_retains_unaccepted_response_after_terminal_unlock(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    response_path = harness.paths.response_path(command.request_id)
    unrelated = harness.paths.import_export / "unrelated-user-file.inst"
    unrelated.write_bytes(b"unrelated")
    first_timeout = BridgeError(ErrorCode.REQUEST_TIMEOUT, "first timeout")
    primary = {
        "second_timeout": BridgeError(ErrorCode.REQUEST_TIMEOUT, "second timeout"),
        "protocol": BridgeError(ErrorCode.PROTOCOL_ERROR, "stable protocol error"),
        "process": BridgeError(ErrorCode.CMO_NOT_RUNNING, "process disappeared"),
    }[failure_kind]
    waiter_calls = 0
    trace: list[str] = []
    original_transition = transport.ledger.transition
    original_exit = RootLock.__aexit__

    async def deterministic_failure(_waiter: object, _timeout: float) -> ResponseArtifact:
        nonlocal waiter_calls
        waiter_calls += 1
        if failure_kind == "second_timeout" and waiter_calls == 1:
            raise first_timeout
        response_path.write_bytes(b"request-owned failure material")
        raise primary

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
        record = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        if new_state is HostRequestState.REJECTED:
            assert response_path.exists()
            trace.append("terminal")
        return record

    async def traced_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        assert trace == ["terminal"]
        assert response_path.exists()
        result = await original_exit(lock, exc_type, exc, traceback)
        trace.append("unlock")
        return result

    monkeypatch.setattr(transport_module.ResponseWaiter, "wait", deterministic_failure)
    monkeypatch.setattr(transport.ledger, "transition", traced_transition)
    monkeypatch.setattr(RootLock, "__aexit__", traced_exit)

    async with transport.session() as channel:
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(command)
        if failure_kind == "second_timeout":
            assert caught.value.code is ErrorCode.REQUEST_TIMEOUT
            assert caught.value.__cause__ is primary
            assert caught.value.details["timeout_seconds"] == command.timeout
        else:
            assert caught.value is primary
        assert response_path.exists()

    assert waiter_calls == (2 if failure_kind == "second_timeout" else 1)
    assert trace == ["terminal", "unlock"]
    assert response_path.read_bytes() == b"request-owned failure material"
    assert unrelated.read_bytes() == b"unrelated"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("prepublication_observation", "expected_code"),
    [
        ((), ErrorCode.CMO_NOT_RUNNING),
        (
            (
                ProcessInfo(pid=2001, create_time=2.0, executable=Path("Command.exe")),
                ProcessInfo(pid=2002, create_time=3.0, executable=Path("Command.exe")),
            ),
            ErrorCode.MULTIPLE_CMO_INSTANCES,
        ),
        (
            (ProcessInfo(pid=9999, create_time=9.0, executable=Path("Command.exe")),),
            ErrorCode.STATE_CONFLICT,
        ),
    ],
)
async def test_first_prepublication_process_failure_rejects_prepared_without_idle(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    prepublication_observation: tuple[object, ...],
    expected_code: ErrorCode,
) -> None:
    inspector = SequencedInspector(
        (
            (harness.process,),
            prepublication_observation,
        )
    )
    transport = _transport(harness, process_inspector=inspector)
    command = _scenario_command(harness)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)

    async with transport.session() as channel:
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(command)

    assert caught.value.code is expected_code
    assert inspector.calls == 2
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.REJECTED
    assert request.error_json == _canonical_json(caught.value.to_payload()).decode("utf-8")
    assert not harness.paths.inbox.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("wait_observation", "expected_code"),
    [
        ((), ErrorCode.CMO_NOT_RUNNING),
        (
            (
                ProcessInfo(pid=2001, create_time=2.0, executable=Path("Command.exe")),
                ProcessInfo(pid=2002, create_time=3.0, executable=Path("Command.exe")),
            ),
            ErrorCode.MULTIPLE_CMO_INSTANCES,
        ),
        (
            (ProcessInfo(pid=9999, create_time=9.0, executable=Path("Command.exe")),),
            ErrorCode.STATE_CONFLICT,
        ),
    ],
)
async def test_waiter_process_failure_never_retries_and_safely_rejects(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    wait_observation: tuple[object, ...],
    expected_code: ErrorCode,
) -> None:
    inspector = SequencedInspector(
        (
            (harness.process,),
            (harness.process,),
            wait_observation,
        )
    )
    transport = _transport(harness, process_inspector=inspector)
    command = _scenario_command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    intents: list[DeliveryIntent] = []
    publish_calls = 0
    original_insert = transport.ledger.insert_delivery
    original_publish = InboxPublisher.publish_delivery

    def capture_intent(intent: DeliveryIntent) -> None:
        intents.append(intent)
        original_insert(intent)

    def capture_publish(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal publish_calls
        publish_calls += 1
        original_publish(
            publisher,
            cast(Any, delivery),
            runtime_snapshot=runtime_snapshot,
        )

    monkeypatch.setattr(transport.ledger, "insert_delivery", capture_intent)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", capture_publish)

    async with transport.session() as channel:
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(command)

    assert caught.value.code is expected_code
    assert len(intents) == 1
    assert publish_calls == 1
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.REJECTED
    assert harness.paths.inbox.read_bytes() == render_idle_lua()


@pytest.mark.asyncio
async def test_retry_prepublication_replacement_adds_intent_but_no_second_inbox_delivery(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replacement = ProcessInfo(
        pid=harness.process.pid + 1,
        create_time=harness.process.create_time + 1.0,
        executable=harness.process.executable,
    )
    inspector = SequencedInspector(
        (
            (harness.process,),
            (harness.process,),
            (replacement,),
        )
    )
    transport = _transport(harness, process_inspector=inspector)
    command = _scenario_command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    first_timeout = BridgeError(ErrorCode.REQUEST_TIMEOUT, "first exact timeout")
    waiter_calls = 0
    intents: list[DeliveryIntent] = []
    publish_calls = 0
    original_insert = transport.ledger.insert_delivery
    original_publish = InboxPublisher.publish_delivery

    async def timeout_once(_waiter: object, _timeout_seconds: float) -> ResponseArtifact:
        nonlocal waiter_calls
        waiter_calls += 1
        if waiter_calls == 1:
            raise first_timeout
        raise AssertionError("retry waiter must not be constructed after process replacement")

    def capture_intent(intent: DeliveryIntent) -> None:
        intents.append(intent)
        original_insert(intent)

    def capture_publish(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal publish_calls
        publish_calls += 1
        original_publish(
            publisher,
            cast(Any, delivery),
            runtime_snapshot=runtime_snapshot,
        )

    monkeypatch.setattr(transport_module.ResponseWaiter, "wait", timeout_once)
    monkeypatch.setattr(transport.ledger, "insert_delivery", capture_intent)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", capture_publish)

    async with transport.session() as channel:
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(command)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.message == "CMO process identity changed before request publication"
    assert waiter_calls == 1
    assert len(intents) == 2
    assert publish_calls == 1
    retry_delivery = transport.ledger.get_delivery(intents[1].delivery_id)
    assert retry_delivery is not None
    assert retry_delivery.published_at_ms is None
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.REJECTED
    assert harness.paths.inbox.read_bytes() == render_idle_lua()


class FatalExchangeSignal(BaseException):
    pass


def _expected_quarantine_json(reason: str) -> str:
    return _canonical_json(
        {
            "code": ErrorCode.STATE_CONFLICT.value,
            "details": {"reason": reason},
            "message": "file-bridge read exchange requires recovery",
        }
    ).decode("utf-8")


@pytest.mark.asyncio
async def test_waiter_cancellation_is_shielded_and_retains_unaccepted_response_identity(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    waiter_started = asyncio.Event()
    observed: list[asyncio.CancelledError] = []
    never = asyncio.Event()
    response_path = harness.paths.response_path(command.request_id)
    original_idle = InboxPublisher.publish_idle
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)

    async def stalled_waiter(
        _waiter: object,
        _timeout_seconds: float,
    ) -> ResponseArtifact:
        waiter_started.set()
        try:
            await never.wait()
        except asyncio.CancelledError as error:
            observed.append(error)
            raise
        raise AssertionError("unreachable")

    def lock_checked_idle(publisher: InboxPublisher) -> None:
        transport.root_lock.require_acquired()
        original_idle(publisher)

    monkeypatch.setattr(transport_module.ResponseWaiter, "wait", stalled_waiter)
    monkeypatch.setattr(InboxPublisher, "publish_idle", lock_checked_idle)

    async with transport.session() as channel:
        task = asyncio.create_task(channel.exchange(command))
        waiter_gate = asyncio.create_task(waiter_started.wait())
        done, _ = await asyncio.wait(
            {task, waiter_gate},
            timeout=2,
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert done, "exchange and waiter gate both stalled"
        if task in done:
            await task
        await waiter_gate
        response_path.write_bytes(b"request-owned cancellation material")
        task.cancel("cancel while waiting")
        with pytest.raises(asyncio.CancelledError) as caught:
            await task
        assert observed == [caught.value]
        assert observed[0] is caught.value
        transport.root_lock.require_acquired()
        assert response_path.exists()
        request = transport.ledger.get_request(command.request_id)
        assert request is not None
        assert request.state is HostRequestState.REJECTED
        assert request.error_json == _canonical_json(
            {
                "code": ErrorCode.STATE_CONFLICT.value,
                "details": {"reason": "exchange_cancelled"},
                "message": "file-bridge read exchange was cancelled",
            }
        ).decode("utf-8")
        assert request.terminal_at_ms == request.updated_at_ms
        assert harness.paths.inbox.read_bytes() == render_idle_lua()

    assert response_path.read_bytes() == b"request-owned cancellation material"


@pytest.mark.asyncio
async def test_custom_non_exception_waiter_failure_retains_identity_after_safe_rejection(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    signal = FatalExchangeSignal("fatal waiter signal")
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)

    async def fail_waiter(_waiter: object, _timeout_seconds: float) -> ResponseArtifact:
        raise signal

    monkeypatch.setattr(transport_module.ResponseWaiter, "wait", fail_waiter)

    async with transport.session() as channel:
        with pytest.raises(FatalExchangeSignal) as caught:
            await channel.exchange(command)
        assert caught.value is signal
        request = transport.ledger.get_request(command.request_id)
        assert request is not None and request.state is HostRequestState.REJECTED
        assert harness.paths.inbox.read_bytes() == render_idle_lua()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["prepared", "intent"])
async def test_prepublication_cancellation_after_durable_insert_rejects_without_idle(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    cancellation = asyncio.CancelledError(f"cancel after {boundary}")
    original_prepared = transport.ledger.insert_prepared
    original_intent = transport.ledger.insert_delivery

    def prepared_then_cancel(record: RequestRecord) -> None:
        original_prepared(record)
        raise cancellation

    def intent_then_cancel(intent: DeliveryIntent) -> None:
        original_intent(intent)
        raise cancellation

    monkeypatch.setattr(
        transport.ledger,
        "insert_prepared" if boundary == "prepared" else "insert_delivery",
        prepared_then_cancel if boundary == "prepared" else intent_then_cancel,
    )
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)

    async with transport.session() as channel:
        with pytest.raises(asyncio.CancelledError) as caught:
            await channel.exchange(command)

    assert caught.value is cancellation
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.REJECTED
    assert request.error_json == _canonical_json(
        {
            "code": ErrorCode.STATE_CONFLICT.value,
            "details": {"reason": "exchange_cancelled"},
            "message": "file-bridge read exchange was cancelled",
        }
    ).decode("utf-8")
    assert not harness.paths.inbox.exists()


@pytest.mark.asyncio
async def test_second_waiter_cancellation_retains_unaccepted_response_without_retry(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    second_started = asyncio.Event()
    first_timeout = BridgeError(ErrorCode.REQUEST_TIMEOUT, "first waiter timeout")
    waiter_calls = 0
    intents: list[DeliveryIntent] = []
    original_insert = transport.ledger.insert_delivery
    response_path = harness.paths.response_path(command.request_id)

    async def two_waiters(_waiter: object, _timeout: float) -> ResponseArtifact:
        nonlocal waiter_calls
        waiter_calls += 1
        if waiter_calls == 1:
            raise first_timeout
        second_started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    def capture_intent(intent: DeliveryIntent) -> None:
        intents.append(intent)
        original_insert(intent)

    monkeypatch.setattr(transport_module.ResponseWaiter, "wait", two_waiters)
    monkeypatch.setattr(transport.ledger, "insert_delivery", capture_intent)

    async with transport.session() as channel:
        task = asyncio.create_task(channel.exchange(command))
        async with asyncio.timeout(2):
            await second_started.wait()
        response_path.write_bytes(b"owned second-waiter cancellation material")
        task.cancel("cancel second waiter")
        with pytest.raises(asyncio.CancelledError):
            await task
        assert response_path.exists()

    assert waiter_calls == 2
    assert len(intents) == 2
    assert response_path.read_bytes() == b"owned second-waiter cancellation material"
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.REJECTED


@pytest.mark.asyncio
async def test_custom_non_exception_after_durable_artifact_is_authoritative(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    signal = FatalExchangeSignal("fatal after durable artifact")
    original_record = transport.ledger.record_response

    def record_then_signal(artifact: ResponseArtifact) -> object:
        original_record(artifact)
        raise signal

    monkeypatch.setattr(transport.ledger, "record_response", record_then_signal)
    response_path = harness.paths.response_path(command.request_id)

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(FatalExchangeSignal) as caught:
                await channel.exchange(command)
            assert caught.value is signal
            request = transport.ledger.get_request(command.request_id)
            assert request is not None and request.state is HostRequestState.COMPLETED
            assert response_path.exists()

    assert not response_path.exists()


@pytest.mark.asyncio
async def test_failed_safety_finalizer_poisons_without_overwriting_old_state(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    original_publish = InboxPublisher.publish_delivery
    original_transition = transport.ledger.transition
    publication_failure = RuntimeError("ambiguous primary")
    safety_failure = OSError("quarantine CAS unavailable")
    quarantine_calls = 0

    def ambiguous_publish(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        original_publish(
            publisher,
            cast(Any, delivery),
            runtime_snapshot=runtime_snapshot,
        )
        raise publication_failure

    def fail_quarantine(
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
        nonlocal quarantine_calls
        if new_state is HostRequestState.QUARANTINED:
            quarantine_calls += 1
            raise safety_failure
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

    monkeypatch.setattr(InboxPublisher, "publish_delivery", ambiguous_publish)
    monkeypatch.setattr(transport.ledger, "transition", fail_quarantine)

    with pytest.raises(OSError) as exit_failure:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)
            assert caught.value.__cause__ is publication_failure
            assert any("exchange safety failure" in note for note in caught.value.__notes__)
            concrete = cast(_FileBridgeChannel, channel)
            old_state = concrete._exchange_state  # pyright: ignore[reportPrivateUsage]
            assert old_state is not None and old_state.safety_finished is False
            with pytest.raises(BridgeError) as poisoned:
                await channel.exchange(
                    _scenario_command(
                        harness,
                        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee3001"),
                    )
                )
            assert poisoned.value.code is ErrorCode.STATE_CONFLICT
            assert concrete._exchange_state is old_state  # pyright: ignore[reportPrivateUsage]
            monkeypatch.setattr(transport.ledger, "transition", original_transition)

    assert exit_failure.value is safety_failure
    assert quarantine_calls == 1
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.QUARANTINED


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["precommit", "after_commit", "drift"])
async def test_idle_transition_convergence_never_skips_an_inexact_target(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    original_transition = transport.ledger.transition
    failure = RuntimeError(f"idle transition {mode} failure")
    idle_calls = 0

    def faulted_transition(
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
        nonlocal idle_calls
        if new_state is HostRequestState.IDLE_PUBLISHED:
            idle_calls += 1
            if idle_calls == 1:
                if mode in {"after_commit", "drift"}:
                    original_transition(
                        request_id,
                        expected_states=expected_states,
                        new_state=new_state,
                        updated_at_ms=updated_at_ms + (1 if mode == "drift" else 0),
                    )
                raise failure
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

    monkeypatch.setattr(transport.ledger, "transition", faulted_transition)
    response_path = harness.paths.response_path(command.request_id)
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

    assert caught is not None
    assert caught.value.__cause__ is failure
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    if mode == "drift":
        assert idle_calls == 1
        assert request.state is HostRequestState.IDLE_PUBLISHED
        assert request.terminal_at_ms is None
        assert response_path.exists()
    else:
        assert idle_calls == (2 if mode == "precommit" else 1)
        assert request.state is HostRequestState.COMPLETED
        assert not response_path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["after_commit", "payload_drift", "epoch_drift"])
async def test_terminal_after_commit_convergence_requires_exact_artifact_fields(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    original_transition = transport.ledger.transition
    failure = RuntimeError(f"terminal {mode} failure")
    terminal_calls = 0

    def faulted_transition(
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
        nonlocal terminal_calls
        if new_state is HostRequestState.COMPLETED:
            terminal_calls += 1
            if terminal_calls == 1:
                original_transition(
                    request_id,
                    expected_states=expected_states,
                    new_state=new_state,
                    updated_at_ms=(updated_at_ms + 1 if mode == "epoch_drift" else updated_at_ms),
                    terminal_at_ms=(
                        None
                        if terminal_at_ms is None
                        else terminal_at_ms + (1 if mode == "epoch_drift" else 0)
                    ),
                    result_json=(
                        _canonical_json({"drift": True}).decode("utf-8")
                        if mode == "payload_drift"
                        else result_json
                    ),
                    error_json=error_json,
                    resolution_json=resolution_json,
                )
                raise failure
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

    monkeypatch.setattr(transport.ledger, "transition", faulted_transition)
    response_path = harness.paths.response_path(command.request_id)
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

    assert caught is not None
    assert caught.value.__cause__ is failure
    assert terminal_calls == 1
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.COMPLETED
    if mode == "after_commit":
        assert not response_path.exists()
    elif mode == "payload_drift":
        assert request.result_json == _canonical_json({"drift": True}).decode("utf-8")
        assert response_path.exists()
    else:
        assert request.terminal_at_ms == request.updated_at_ms
        assert response_path.exists()


@pytest.mark.asyncio
async def test_no_artifact_terminal_convergence_rejects_epoch_drift(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    response_path = harness.paths.response_path(command.request_id)
    primary = BridgeError(ErrorCode.PROTOCOL_ERROR, "stable response failure")
    terminal_failure = RuntimeError("no-artifact terminal committed at wrong epoch")
    original_transition = transport.ledger.transition
    planned_epoch: int | None = None

    async def fail_without_artifact(_waiter: object, _timeout: float) -> ResponseArtifact:
        response_path.write_bytes(b"request-owned malformed response")
        raise primary

    def commit_wrong_terminal_epoch(
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
        nonlocal planned_epoch
        if new_state is HostRequestState.REJECTED and expected_states == frozenset(
            {HostRequestState.IDLE_PUBLISHED}
        ):
            planned_epoch = updated_at_ms
            assert terminal_at_ms == updated_at_ms
            original_transition(
                request_id,
                expected_states=expected_states,
                new_state=new_state,
                updated_at_ms=updated_at_ms + 1,
                terminal_at_ms=updated_at_ms + 1,
                result_json=result_json,
                error_json=error_json,
                resolution_json=resolution_json,
            )
            raise terminal_failure
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

    monkeypatch.setattr(transport_module.ResponseWaiter, "wait", fail_without_artifact)
    monkeypatch.setattr(transport.ledger, "transition", commit_wrong_terminal_epoch)
    manager = transport.session()
    channel = await manager.__aenter__()

    with pytest.raises(BridgeError) as caught:
        await channel.exchange(command)
    assert caught.value is primary
    assert any("exchange safety failure" in note for note in caught.value.__notes__)
    with pytest.raises(RuntimeError) as exit_failure:
        await manager.__aexit__(None, None, None)

    assert exit_failure.value is terminal_failure
    assert planned_epoch is not None
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.REJECTED
    assert request.updated_at_ms == planned_epoch + 1
    assert request.terminal_at_ms == planned_epoch + 1
    assert response_path.exists()


@pytest.mark.asyncio
async def test_prepublication_rejection_convergence_rejects_epoch_drift(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inspector = SequencedInspector(((harness.process,), ()))
    transport = _transport(harness, process_inspector=inspector)
    command = _scenario_command(harness)
    terminal_failure = RuntimeError("prepublication rejection committed at wrong epoch")
    original_transition = transport.ledger.transition
    planned_epoch: int | None = None

    def commit_wrong_rejection_epoch(
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
        nonlocal planned_epoch
        if new_state is HostRequestState.REJECTED and expected_states == frozenset(
            {HostRequestState.PREPARED}
        ):
            planned_epoch = updated_at_ms
            assert terminal_at_ms == updated_at_ms
            original_transition(
                request_id,
                expected_states=expected_states,
                new_state=new_state,
                updated_at_ms=updated_at_ms + 1,
                terminal_at_ms=updated_at_ms + 1,
                result_json=result_json,
                error_json=error_json,
                resolution_json=resolution_json,
            )
            raise terminal_failure
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

    monkeypatch.setattr(transport.ledger, "transition", commit_wrong_rejection_epoch)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
    manager = transport.session()
    channel = await manager.__aenter__()

    with pytest.raises(BridgeError) as caught:
        await channel.exchange(command)
    assert caught.value.code is ErrorCode.CMO_NOT_RUNNING
    assert any("exchange safety failure" in note for note in caught.value.__notes__)
    with pytest.raises(RuntimeError) as exit_failure:
        await manager.__aexit__(None, None, None)

    assert exit_failure.value is terminal_failure
    assert planned_epoch is not None
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.REJECTED
    assert request.updated_at_ms == planned_epoch + 1
    assert request.terminal_at_ms == planned_epoch + 1
    assert not harness.paths.inbox.exists()


@pytest.mark.asyncio
async def test_partial_durable_response_group_is_artifact_drift_quarantine(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    failure = RuntimeError("partial response group committed")

    def commit_partial_then_fail(artifact: ResponseArtifact) -> object:
        delivery_id = artifact.accepted_response.envelope.delivery_id
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            connection.execute("PRAGMA ignore_check_constraints=ON")
            connection.execute(
                "UPDATE deliveries SET response_at_ms=? WHERE delivery_id=?",
                (artifact.accepted_at_ms, str(delivery_id)),
            )
            connection.commit()
        raise failure

    monkeypatch.setattr(transport.ledger, "record_response", commit_partial_then_fail)
    response_path = harness.paths.response_path(command.request_id)
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

    assert caught is not None
    assert caught.value.__cause__ is failure
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.QUARANTINED
    assert request.error_json == _expected_quarantine_json("artifact_evidence_drift")
    assert request.terminal_at_ms is None
    assert response_path.exists()


@pytest.mark.asyncio
async def test_nonresponder_retry_row_drift_cannot_hide_behind_durable_responder_artifact(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(
        Respond(result=peer.result_for(command.invocation)),
        StaySilent(),
    )
    response_path = harness.paths.response_path(command.request_id)
    response_written = asyncio.Event()
    original_write_bytes = Path.write_bytes
    original_wait = transport_module.ResponseWaiter.wait
    original_insert = transport.ledger.insert_delivery
    original_record = transport.ledger.record_response
    first_timeout = BridgeError(ErrorCode.REQUEST_TIMEOUT, "forced first timeout")
    persistence_failure = RuntimeError("responder persisted; retry evidence drifted")
    intents: list[DeliveryIntent] = []
    waiter_calls = 0
    observed_artifact: ResponseArtifact | None = None

    def write_and_signal(path: Path, data: bytes) -> int:
        written = original_write_bytes(path, data)
        if path == response_path:
            response_written.set()
        return written

    def capture_intent(intent: DeliveryIntent) -> None:
        intents.append(intent)
        original_insert(intent)

    async def force_retry_after_first_response(
        waiter: Any,
        timeout_seconds: float,
    ) -> ResponseArtifact:
        nonlocal waiter_calls
        waiter_calls += 1
        if waiter_calls == 1:
            async with asyncio.timeout(2):
                await response_written.wait()
            raise first_timeout
        return await original_wait(waiter, timeout_seconds)

    def persist_responder_then_corrupt_retry(artifact: ResponseArtifact) -> object:
        nonlocal observed_artifact
        observed_artifact = artifact
        result = original_record(artifact)
        assert len(intents) == 2
        retry_delivery_id = intents[1].delivery_id
        with sqlite3.connect(harness.paths.sqlite_file) as connection:
            connection.execute("PRAGMA ignore_check_constraints=ON")
            connection.execute(
                "UPDATE deliveries SET response_at_ms=? WHERE delivery_id=?",
                (artifact.accepted_at_ms, str(retry_delivery_id)),
            )
            connection.commit()
        del result
        raise persistence_failure

    monkeypatch.setattr(Path, "write_bytes", write_and_signal)
    monkeypatch.setattr(transport.ledger, "insert_delivery", capture_intent)
    monkeypatch.setattr(transport_module.ResponseWaiter, "wait", force_retry_after_first_response)
    monkeypatch.setattr(
        transport.ledger,
        "record_response",
        persist_responder_then_corrupt_retry,
    )
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

    assert caught is not None
    assert caught.value.__cause__ is persistence_failure
    assert waiter_calls == 2
    assert len(intents) == 2
    assert observed_artifact is not None
    assert observed_artifact.accepted_response.envelope.delivery_id == intents[0].delivery_id
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.QUARANTINED
    assert request.error_json == _expected_quarantine_json("publication_evidence_drift")
    assert request.terminal_at_ms is None
    assert response_path.exists()


@pytest.mark.asyncio
async def test_later_state_drift_still_attempts_idle_before_poisoning(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    response_path = harness.paths.response_path(command.request_id)
    transition_failure = RuntimeError("response acceptance skipped into later state")
    original_transition = transport.ledger.transition
    original_publish_idle = InboxPublisher.publish_idle
    idle_calls = 0

    def skip_to_idle_then_fail(
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
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            original_transition(
                request_id,
                expected_states=expected_states,
                new_state=new_state,
                updated_at_ms=updated_at_ms,
                terminal_at_ms=terminal_at_ms,
                result_json=result_json,
                error_json=error_json,
                resolution_json=resolution_json,
            )
            original_transition(
                request_id,
                expected_states=frozenset({HostRequestState.RESPONSE_ACCEPTED}),
                new_state=HostRequestState.IDLE_PUBLISHED,
                updated_at_ms=updated_at_ms + 1,
            )
            raise transition_failure
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

    def capture_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_calls
        idle_calls += 1
        original_publish_idle(publisher)

    monkeypatch.setattr(transport.ledger, "transition", skip_to_idle_then_fail)
    monkeypatch.setattr(InboxPublisher, "publish_idle", capture_idle)
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

    assert caught is not None
    assert caught.value.__cause__ is transition_failure
    assert idle_calls == 1
    assert harness.paths.inbox.read_bytes() == render_idle_lua()
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.IDLE_PUBLISHED
    assert request.terminal_at_ms is None
    assert response_path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["prepared", "intent", "process_recheck"])
async def test_failure_proven_before_first_publisher_entry_rejects_without_idle(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    failure = RuntimeError(f"{boundary} failed after its safe boundary")
    original_prepared = transport.ledger.insert_prepared
    original_intent = transport.ledger.insert_delivery

    def prepared_then_fail(record: RequestRecord) -> None:
        original_prepared(record)
        raise failure

    def intent_then_fail(intent: DeliveryIntent) -> None:
        original_intent(intent)
        raise failure

    def fail_process(_channel: _FileBridgeChannel) -> None:
        raise failure

    if boundary == "prepared":
        monkeypatch.setattr(transport.ledger, "insert_prepared", prepared_then_fail)
    elif boundary == "intent":
        monkeypatch.setattr(transport.ledger, "insert_delivery", intent_then_fail)
    else:
        monkeypatch.setattr(
            _FileBridgeChannel,
            "_require_original_process_before_publication",
            fail_process,
        )
    monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
    monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)

    async with transport.session() as channel:
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(command)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.__cause__ is failure
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.REJECTED
    assert request.terminal_at_ms == request.updated_at_ms
    assert request.error_json == _canonical_json(caught.value.to_payload()).decode("utf-8")
    assert not harness.paths.inbox.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["ordinary", "bridge", "fatal"])
async def test_first_publisher_replace_then_raise_is_ambiguous_quarantine_and_poison(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    response_path = harness.paths.response_path(command.request_id)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    if failure_kind == "ordinary":
        failure: BaseException = RuntimeError("publisher returned no commit token")
    elif failure_kind == "bridge":
        failure = BridgeError(ErrorCode.STATE_CONFLICT, "publisher bridge sentinel")
    else:
        failure = FatalExchangeSignal("publisher fatal sentinel")
    original_publish = InboxPublisher.publish_delivery
    original_idle = InboxPublisher.publish_idle
    idle_calls = 0

    def replace_then_raise(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        original_publish(
            publisher,
            cast(Any, delivery),
            runtime_snapshot=runtime_snapshot,
        )
        response_path.write_bytes(b"possibly owned ambiguous response")
        raise failure

    def traced_idle(publisher: InboxPublisher) -> None:
        nonlocal idle_calls
        transport.root_lock.require_acquired()
        idle_calls += 1
        original_idle(publisher)

    monkeypatch.setattr(InboxPublisher, "publish_delivery", replace_then_raise)
    monkeypatch.setattr(InboxPublisher, "publish_idle", traced_idle)

    async with transport.session() as channel:
        try:
            await channel.exchange(command)
        except BaseException as caught:
            if failure_kind == "ordinary":
                assert isinstance(caught, BridgeError)
                assert caught.code is ErrorCode.STATE_CONFLICT
                assert caught.__cause__ is failure
            else:
                assert caught is failure
        else:
            pytest.fail("ambiguous publisher unexpectedly succeeded")
        with pytest.raises(BridgeError) as poisoned:
            await channel.exchange(
                _scenario_command(
                    harness,
                    request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0001"),
                )
            )
        assert poisoned.value.code is ErrorCode.STATE_CONFLICT

    assert idle_calls >= 1
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.QUARANTINED
    assert request.terminal_at_ms is None
    assert request.error_json == _expected_quarantine_json("publication_outcome_unknown")
    delivery = transport.ledger.get_delivery(DELIVERY_ID)
    if delivery is None:
        rows = (
            sqlite3.connect(harness.paths.sqlite_file)
            .execute(
                "SELECT delivery_id FROM deliveries WHERE request_id=?",
                (str(command.request_id),),
            )
            .fetchall()
        )
        assert len(rows) == 1
        delivery = transport.ledger.get_delivery(UUID(rows[0][0]))
    assert delivery is not None and delivery.published_at_ms is None
    assert response_path.read_bytes() == b"possibly owned ambiguous response"
    assert harness.paths.inbox.read_bytes() == render_idle_lua()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["ordinary", "bridge", "fatal"])
async def test_retry_replace_then_raise_uses_fresh_ambiguity_state_and_retains_slot(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
) -> None:
    transport = _transport(harness)
    command = replace(_scenario_command(harness), timeout=0.01)
    peer = _peer(harness)
    peer.enqueue(StaySilent(), StaySilent())
    response_path = harness.paths.response_path(command.request_id)
    original_publish = InboxPublisher.publish_delivery
    publish_calls = 0
    intents: list[DeliveryIntent] = []
    original_insert = transport.ledger.insert_delivery
    if failure_kind == "ordinary":
        failure: BaseException = RuntimeError("retry replace completed then wrapper failed")
    elif failure_kind == "bridge":
        failure = BridgeError(ErrorCode.PROTOCOL_ERROR, "exact retry publisher bridge error")
    else:
        failure = FatalExchangeSignal("exact retry publisher fatal signal")

    def record_intent(intent: DeliveryIntent) -> None:
        intents.append(intent)
        original_insert(intent)

    def fail_second_replace(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        nonlocal publish_calls
        publish_calls += 1
        original_publish(
            publisher,
            cast(Any, delivery),
            runtime_snapshot=runtime_snapshot,
        )
        if publish_calls == 2:
            response_path.write_bytes(b"retry ambiguity material")
            raise failure

    monkeypatch.setattr(transport.ledger, "insert_delivery", record_intent)
    monkeypatch.setattr(InboxPublisher, "publish_delivery", fail_second_replace)
    caught: pytest.ExceptionInfo[BaseException] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BaseException) as caught:
                await channel.exchange(command)

    assert caught is not None
    if failure_kind == "ordinary":
        assert isinstance(caught.value, BridgeError)
        assert caught.value.code is ErrorCode.STATE_CONFLICT
        assert caught.value.__cause__ is failure
    else:
        assert caught.value is failure
    assert publish_calls == 2
    assert len(intents) == 2
    first = transport.ledger.get_delivery(intents[0].delivery_id)
    retry = transport.ledger.get_delivery(intents[1].delivery_id)
    assert first is not None and first.published_at_ms is not None
    assert retry is not None and retry.published_at_ms is None
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.QUARANTINED
    assert request.error_json == _expected_quarantine_json("publication_outcome_unknown")
    assert response_path.read_bytes() == b"retry ambiguity material"


@pytest.mark.asyncio
@pytest.mark.parametrize("artifact_kind", ["completed", "rejected", "manifest_mismatch"])
async def test_cancellation_after_durable_artifact_is_artifact_authoritative(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    artifact_kind: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    if artifact_kind == "completed":
        peer.enqueue(Respond(result=peer.result_for(command.invocation)))
        expected_state = HostRequestState.COMPLETED
    else:
        error = cast(
            dict[str, JsonValue],
            {
                "code": (
                    ErrorCode.MANIFEST_MISMATCH.value
                    if artifact_kind == "manifest_mismatch"
                    else ErrorCode.NOT_FOUND.value
                ),
                "message": "authoritative response error",
                "details": {},
                "mutation_not_started": None,
            },
        )
        overrides: tuple[tuple[str, JsonValue], ...] = ()
        if artifact_kind == "manifest_mismatch":
            asset = "1" * 64
            overrides = (
                ("operation_manifest_sha256", "2" * 64),
                ("bridge_version", "9.8.7"),
                ("runtime_tag", f"9_8_7-{asset}"),
                ("runtime_asset_sha256", asset),
                ("release_id", "3" * 64),
            )
        peer.enqueue(Respond(error=error, envelope_overrides=overrides))
        expected_state = HostRequestState.REJECTED
    cancellation = asyncio.CancelledError(f"after artifact {artifact_kind}")
    original_record = transport.ledger.record_response

    def record_then_cancel(artifact: ResponseArtifact) -> object:
        original_record(artifact)
        raise cancellation

    monkeypatch.setattr(transport.ledger, "record_response", record_then_cancel)
    response_path = harness.paths.response_path(command.request_id)

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(asyncio.CancelledError) as caught:
                await channel.exchange(command)
            assert caught.value is cancellation
            request = transport.ledger.get_request(command.request_id)
            assert request is not None and request.state is expected_state
            assert response_path.exists()
            assert harness.paths.inbox.read_bytes() == render_idle_lua()

    assert not response_path.exists()


@pytest.mark.asyncio
async def test_idle_failure_quarantines_retains_and_poisons_without_masking_shape(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    failure = OSError("idle replace failed")
    idle_calls = 0

    def fail_idle(_publisher: InboxPublisher) -> None:
        nonlocal idle_calls
        transport.root_lock.require_acquired()
        idle_calls += 1
        raise failure

    monkeypatch.setattr(InboxPublisher, "publish_idle", fail_idle)
    response_path = harness.paths.response_path(command.request_id)

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)
            assert caught.value.code is ErrorCode.STATE_CONFLICT
            assert caught.value.__cause__ is failure
            request = transport.ledger.get_request(command.request_id)
            assert request is not None
            assert request.state is HostRequestState.QUARANTINED
            assert request.terminal_at_ms is None
            assert request.error_json == _expected_quarantine_json("idle_publication_failed")
            assert response_path.exists()
            original_sqlite_connect = sqlite3.connect
            for method_name in (
                "get_request",
                "get_delivery",
                "insert_prepared",
                "insert_delivery",
                "mark_delivery_published",
                "record_response",
                "transition",
            ):
                monkeypatch.setattr(transport.ledger, method_name, _forbidden)
            monkeypatch.setattr(sqlite3, "connect", _forbidden)
            monkeypatch.setattr(harness.catalog, "resolve_running", _forbidden)
            monkeypatch.setattr(InboxPublisher, "publish_delivery", _forbidden)
            monkeypatch.setattr(InboxPublisher, "publish_idle", _forbidden)
            with pytest.raises(BridgeError) as poisoned:
                await channel.exchange(
                    _scenario_command(
                        harness,
                        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0002"),
                    )
                )
            assert poisoned.value.code is ErrorCode.STATE_CONFLICT
            monkeypatch.setattr(sqlite3, "connect", original_sqlite_connect)

    assert idle_calls >= 1
    assert response_path.exists()
    async with transport.session() as channel:
        assert channel is not None


@pytest.mark.asyncio
async def test_terminal_transition_failure_stays_idle_published_and_retains_response(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    response_path = harness.paths.response_path(command.request_id)
    original_transition = transport.ledger.transition
    failure = BridgeError(ErrorCode.STATE_CONFLICT, "terminal transition sentinel")

    def fail_terminal(
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
        if new_state in {HostRequestState.COMPLETED, HostRequestState.REJECTED}:
            raise failure
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

    monkeypatch.setattr(transport.ledger, "transition", fail_terminal)
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

    assert caught is not None
    assert caught.value is failure
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.IDLE_PUBLISHED
    assert request.terminal_at_ms is None
    assert request.error_json is None
    assert response_path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["precommit", "after_commit", "drift"])
async def test_publication_marker_convergence_is_safety_only(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    original_mark = transport.ledger.mark_delivery_published
    failure = RuntimeError(f"marker {mode} failure")
    calls = 0
    observed_delivery_id: UUID | None = None
    observed_epoch: int | None = None

    def faulted_mark(delivery_id: UUID, *, published_at_ms: int) -> object:
        nonlocal calls, observed_delivery_id, observed_epoch
        calls += 1
        observed_delivery_id = delivery_id
        observed_epoch = published_at_ms
        if calls == 1:
            if mode == "after_commit":
                original_mark(delivery_id, published_at_ms=published_at_ms)
            elif mode == "drift":
                original_mark(delivery_id, published_at_ms=published_at_ms + 1)
            raise failure
        return original_mark(delivery_id, published_at_ms=published_at_ms)

    monkeypatch.setattr(transport.ledger, "mark_delivery_published", faulted_mark)
    monkeypatch.setattr(transport_module.ResponseWaiter, "__init__", _forbidden)

    async with transport.session() as channel:
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(command)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.__cause__ is failure
    assert observed_delivery_id is not None and observed_epoch is not None
    delivery = transport.ledger.get_delivery(observed_delivery_id)
    assert delivery is not None
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    if mode == "drift":
        assert calls == 1
        assert delivery.published_at_ms == observed_epoch + 1
        assert request.state is HostRequestState.QUARANTINED
        assert request.error_json == _expected_quarantine_json("publication_evidence_drift")
        assert request.terminal_at_ms is None
    else:
        assert calls == (2 if mode == "precommit" else 1)
        assert delivery.published_at_ms == observed_epoch
        assert request.state is HostRequestState.REJECTED
        assert request.error_json == _canonical_json(caught.value.to_payload()).decode("utf-8")
    assert harness.paths.inbox.read_bytes() == render_idle_lua()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["precommit", "after_commit", "drift"])
async def test_publication_request_transition_convergence_requires_exact_target(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    original_transition = transport.ledger.transition
    failure = RuntimeError(f"publication transition {mode} failure")
    publication_calls = 0
    expected_epoch: int | None = None

    def faulted_transition(
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
        nonlocal publication_calls, expected_epoch
        if new_state is HostRequestState.PUBLISHED:
            publication_calls += 1
            expected_epoch = updated_at_ms
            if publication_calls == 1:
                if mode in {"after_commit", "drift"}:
                    original_transition(
                        request_id,
                        expected_states=expected_states,
                        new_state=new_state,
                        updated_at_ms=updated_at_ms + (1 if mode == "drift" else 0),
                    )
                raise failure
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

    monkeypatch.setattr(transport.ledger, "transition", faulted_transition)
    monkeypatch.setattr(transport_module.ResponseWaiter, "__init__", _forbidden)

    async with transport.session() as channel:
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(command)

    assert caught.value.__cause__ is failure
    assert expected_epoch is not None
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    if mode == "drift":
        assert publication_calls == 1
        assert request.state is HostRequestState.QUARANTINED
        assert request.error_json == _expected_quarantine_json("publication_evidence_drift")
    else:
        assert publication_calls == (2 if mode == "precommit" else 1)
        assert request.state is HostRequestState.REJECTED
    assert harness.paths.inbox.read_bytes() == render_idle_lua()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["precommit", "after_commit", "drift"])
async def test_record_response_convergence_is_safety_only_and_exact(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    original_record = transport.ledger.record_response
    failure = RuntimeError(f"record response {mode} failure")
    calls = 0
    expected_artifact: ResponseArtifact | None = None

    def faulted_record(artifact: ResponseArtifact) -> object:
        nonlocal calls, expected_artifact
        calls += 1
        expected_artifact = artifact
        if calls == 1:
            if mode == "after_commit":
                original_record(artifact)
            elif mode == "drift":
                original_record(
                    artifact.model_copy(update={"accepted_at_ms": artifact.accepted_at_ms + 1})
                )
            raise failure
        return original_record(artifact)

    monkeypatch.setattr(transport.ledger, "record_response", faulted_record)
    response_path = harness.paths.response_path(command.request_id)
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)
            assert response_path.exists()

    assert caught is not None
    assert caught.value.__cause__ is failure
    assert expected_artifact is not None
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    if mode == "drift":
        assert calls == 1
        assert request.state is HostRequestState.QUARANTINED
        assert request.error_json == _expected_quarantine_json("artifact_evidence_drift")
        assert request.terminal_at_ms is None
        assert response_path.exists()
    else:
        assert calls == (2 if mode == "precommit" else 1)
        assert request.state is HostRequestState.COMPLETED
        responder = transport.ledger.get_delivery(
            expected_artifact.accepted_response.envelope.delivery_id
        )
        assert responder is not None and responder.response_artifact == expected_artifact
        assert not response_path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["precommit", "after_commit", "drift"])
async def test_response_accepted_transition_convergence_requires_exact_target(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    original_transition = transport.ledger.transition
    failure = RuntimeError(f"response transition {mode} failure")
    response_calls = 0

    def faulted_transition(
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
        nonlocal response_calls
        if new_state is HostRequestState.RESPONSE_ACCEPTED:
            response_calls += 1
            if response_calls == 1:
                if mode in {"after_commit", "drift"}:
                    original_transition(
                        request_id,
                        expected_states=expected_states,
                        new_state=new_state,
                        updated_at_ms=updated_at_ms + (1 if mode == "drift" else 0),
                    )
                raise failure
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

    monkeypatch.setattr(transport.ledger, "transition", faulted_transition)
    response_path = harness.paths.response_path(command.request_id)
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

    assert caught is not None
    assert caught.value.__cause__ is failure
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    if mode == "drift":
        assert response_calls == 1
        assert request.state is HostRequestState.QUARANTINED
        assert request.error_json == _expected_quarantine_json("artifact_evidence_drift")
        assert response_path.exists()
    else:
        assert response_calls == (2 if mode == "precommit" else 1)
        assert request.state is HostRequestState.COMPLETED
        assert not response_path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("earlier_reason", ["ambiguous", "publication_drift"])
async def test_idle_failure_wins_compound_quarantine_with_one_singleton_cas(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    earlier_reason: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    publication_failure = RuntimeError(f"{earlier_reason} primary")
    idle_failure = OSError("compound idle failure")
    original_publish = InboxPublisher.publish_delivery
    original_mark = transport.ledger.mark_delivery_published
    original_transition = transport.ledger.transition
    quarantine_calls: list[frozenset[HostRequestState]] = []

    def faulted_publish(
        publisher: InboxPublisher,
        delivery: object,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        original_publish(
            publisher,
            cast(Any, delivery),
            runtime_snapshot=runtime_snapshot,
        )
        if earlier_reason == "ambiguous":
            raise publication_failure

    def faulted_mark(delivery_id: UUID, *, published_at_ms: int) -> object:
        if earlier_reason == "publication_drift":
            original_mark(delivery_id, published_at_ms=published_at_ms + 1)
            raise publication_failure
        return original_mark(delivery_id, published_at_ms=published_at_ms)

    def fail_idle(_publisher: InboxPublisher) -> None:
        raise idle_failure

    def capture_transition(
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
        if new_state is HostRequestState.QUARANTINED:
            quarantine_calls.append(expected_states)
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

    monkeypatch.setattr(InboxPublisher, "publish_delivery", faulted_publish)
    monkeypatch.setattr(transport.ledger, "mark_delivery_published", faulted_mark)
    monkeypatch.setattr(InboxPublisher, "publish_idle", fail_idle)
    monkeypatch.setattr(transport.ledger, "transition", capture_transition)

    async with transport.session() as channel:
        with pytest.raises(BridgeError) as caught:
            await channel.exchange(command)

    assert caught.value.__cause__ is publication_failure
    assert any("idle publication failure" in note for note in caught.value.__notes__)
    assert len(quarantine_calls) == 1
    assert quarantine_calls[0] == frozenset({HostRequestState.PREPARED})
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.QUARANTINED
    assert request.error_json == _expected_quarantine_json("idle_publication_failed")
    assert request.terminal_at_ms is None
    assert request.result_json is None
    assert request.resolution_json is None


@pytest.mark.asyncio
async def test_artifact_drift_plus_idle_failure_uses_one_idle_reason_quarantine_cas(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    artifact_failure = RuntimeError("different artifact committed")
    idle_failure = OSError("idle failed after artifact drift")
    original_record = transport.ledger.record_response
    original_transition = transport.ledger.transition
    quarantine_calls: list[frozenset[HostRequestState]] = []

    def commit_different_artifact(artifact: ResponseArtifact) -> object:
        original_record(artifact.model_copy(update={"accepted_at_ms": artifact.accepted_at_ms + 1}))
        raise artifact_failure

    def fail_idle(_publisher: InboxPublisher) -> None:
        raise idle_failure

    def capture_transition(
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
        if new_state is HostRequestState.QUARANTINED:
            quarantine_calls.append(expected_states)
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

    monkeypatch.setattr(transport.ledger, "record_response", commit_different_artifact)
    monkeypatch.setattr(InboxPublisher, "publish_idle", fail_idle)
    monkeypatch.setattr(transport.ledger, "transition", capture_transition)
    response_path = harness.paths.response_path(command.request_id)
    caught: pytest.ExceptionInfo[BridgeError] | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as caught:
                await channel.exchange(command)

    assert caught is not None
    assert caught.value.__cause__ is artifact_failure
    assert any("idle publication failure" in note for note in caught.value.__notes__)
    assert len(quarantine_calls) == 1 and len(quarantine_calls[0]) == 1
    request = transport.ledger.get_request(command.request_id)
    assert request is not None
    assert request.state is HostRequestState.QUARANTINED
    assert request.error_json == _expected_quarantine_json("idle_publication_failed")
    assert request.terminal_at_ms is None
    assert request.result_json is None
    assert request.resolution_json is None
    assert response_path.exists()


@pytest.mark.asyncio
async def test_safely_rejected_channel_remains_reusable_for_later_read(
    harness: Harness,
) -> None:
    transport = _transport(harness)
    first = _scenario_command(harness)
    second = _scenario_command(
        harness,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee2001"),
    )
    peer = _peer(harness)
    peer.enqueue(
        WriteMalformedComments(),
        Respond(result=peer.result_for(second.invocation)),
    )
    artifact: ResponseArtifact | None = None

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(BridgeError) as first_error:
                await channel.exchange(first)
            assert first_error.value.code is ErrorCode.PROTOCOL_ERROR
            artifact = await channel.exchange(second)

    assert artifact is not None
    assert artifact.accepted_response.envelope.ok is True
    first_request = transport.ledger.get_request(first.request_id)
    second_request = transport.ledger.get_request(second.request_id)
    assert first_request is not None and first_request.state is HostRequestState.REJECTED
    assert second_request is not None and second_request.state is HostRequestState.COMPLETED


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["after_response_accepted", "after_idle_published"])
async def test_post_artifact_cancellation_before_next_boundary_finishes_authoritative_terminal(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    peer = _peer(harness)
    peer.enqueue(Respond(result=peer.result_for(command.invocation)))
    original_transition = transport.ledger.transition
    cancellation = asyncio.CancelledError(boundary)
    injected = False

    def cancel_after_transition(
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
        nonlocal injected
        record = original_transition(
            request_id,
            expected_states=expected_states,
            new_state=new_state,
            updated_at_ms=updated_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=resolution_json,
        )
        target = (
            HostRequestState.RESPONSE_ACCEPTED
            if boundary == "after_response_accepted"
            else HostRequestState.IDLE_PUBLISHED
        )
        if new_state is target and not injected:
            injected = True
            raise cancellation
        return record

    monkeypatch.setattr(transport.ledger, "transition", cancel_after_transition)
    response_path = harness.paths.response_path(command.request_id)

    async with peer:
        async with transport.session() as channel:
            with pytest.raises(asyncio.CancelledError) as caught:
                await channel.exchange(command)
            assert caught.value is cancellation
            request = transport.ledger.get_request(command.request_id)
            assert request is not None and request.state is HostRequestState.COMPLETED
            assert harness.paths.inbox.read_bytes() == render_idle_lua()
            assert response_path.exists()

    assert injected is True
    assert not response_path.exists()


@pytest.mark.asyncio
async def test_session_exit_cancels_real_published_exchange_and_joins_safety_before_unlock(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    waiter_started = asyncio.Event()
    trace: list[str] = []
    original_exit = RootLock.__aexit__

    async def stalled_waiter(_waiter: object, _timeout: float) -> ResponseArtifact:
        waiter_started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def checked_exit(
        lock: RootLock,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        request = transport.ledger.get_request(command.request_id)
        assert request is not None and request.state is HostRequestState.REJECTED
        assert harness.paths.inbox.read_bytes() == render_idle_lua()
        trace.append("safety_complete")
        result = await original_exit(lock, exc_type, exc, traceback)
        trace.append("unlock")
        return result

    monkeypatch.setattr(transport_module.ResponseWaiter, "wait", stalled_waiter)
    monkeypatch.setattr(RootLock, "__aexit__", checked_exit)
    child: asyncio.Task[ResponseArtifact] | None = None

    async with transport.session() as channel:
        child = asyncio.create_task(channel.exchange(command))
        async with asyncio.timeout(2):
            await waiter_started.wait()

    assert child is not None and child.done() and child.cancelled()
    assert trace == ["safety_complete", "unlock"]


@pytest.mark.asyncio
async def test_session_exit_propagates_internal_child_safety_failure_after_expected_cancel(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    command = _scenario_command(harness)
    harness.paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    waiter_started = asyncio.Event()
    safety_failure = OSError("terminal safety failed inside cancelled child")
    original_transition = transport.ledger.transition
    child: asyncio.Task[ResponseArtifact] | None = None

    async def stalled_waiter(_waiter: object, _timeout: float) -> ResponseArtifact:
        waiter_started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    def fail_terminal_rejection(
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
        if new_state is HostRequestState.REJECTED and expected_states == frozenset(
            {HostRequestState.IDLE_PUBLISHED}
        ):
            raise safety_failure
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

    monkeypatch.setattr(transport_module.ResponseWaiter, "wait", stalled_waiter)
    monkeypatch.setattr(transport.ledger, "transition", fail_terminal_rejection)

    with pytest.raises(OSError) as caught:
        async with transport.session() as channel:
            child = asyncio.create_task(channel.exchange(command))
            async with asyncio.timeout(2):
                await waiter_started.wait()

    assert caught.value is safety_failure
    assert child is not None and child.done() and child.cancelled()
    request = transport.ledger.get_request(command.request_id)
    assert request is not None and request.state is HostRequestState.IDLE_PUBLISHED
    assert request.terminal_at_ms is None
    assert harness.paths.inbox.read_bytes() == render_idle_lua()
    with pytest.raises(BridgeError):
        transport.root_lock.require_acquired()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    ["body_exit_cancel_child", "exit_cancel_child", "child", "safety"],
)
async def test_session_exit_total_error_priority_table(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    transport = _transport(harness)
    _patch_empty_journal(monkeypatch, transport)
    body_failure = RuntimeError("body primary")
    child_failure = LookupError("child failure")
    safety_failure = OSError("safety failure")
    captured_exit_cancellations: list[asyncio.CancelledError] = []
    original_protected = _await_protected_task

    async def capture_protected(
        task: asyncio.Task[object],
        *,
        baseline_cancellations: int,
        expected_task_cancellation: bool,
    ) -> tuple[BaseException | None, asyncio.CancelledError | None]:
        result = await original_protected(
            task,
            baseline_cancellations=baseline_cancellations,
            expected_task_cancellation=expected_task_cancellation,
        )
        if result[1] is not None:
            captured_exit_cancellations.append(result[1])
        return result

    monkeypatch.setattr(transport_module, "_await_protected_task", capture_protected)

    if mode == "safety":

        async def fail_safety(_channel: _FileBridgeChannel) -> None:
            raise safety_failure

        monkeypatch.setattr(_FileBridgeChannel, "_last_safety_attempt", fail_safety)
        manager = transport.session()
        await manager.__aenter__()
        with pytest.raises(OSError) as caught:
            await manager.__aexit__(None, None, None)
        assert caught.value is safety_failure
        return

    child_started = asyncio.Event()
    child_cancelled = asyncio.Event()
    permit_child_failure = asyncio.Event()

    async def fail_after_cancel(
        _channel: _FileBridgeChannel,
        _command: ExchangeCommand,
    ) -> ResponseArtifact:
        child_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            child_cancelled.set()
            await permit_child_failure.wait()
            raise child_failure
        raise AssertionError("unreachable")

    monkeypatch.setattr(_FileBridgeChannel, "_perform_exchange", fail_after_cancel)
    manager = transport.session()
    channel = await manager.__aenter__()
    child = asyncio.create_task(channel.exchange(_dummy_command()))
    await child_started.wait()
    body = body_failure if mode == "body_exit_cancel_child" else None
    exit_task = asyncio.create_task(
        manager.__aexit__(
            None if body is None else type(body),
            body,
            None if body is None else body.__traceback__,
        )
    )
    async with asyncio.timeout(2):
        await child_cancelled.wait()
    if mode in {"body_exit_cancel_child", "exit_cancel_child"}:
        exit_task.cancel("new exit cancellation")
        await asyncio.sleep(0)
    permit_child_failure.set()

    if mode == "body_exit_cancel_child":
        assert await exit_task is False
        assert any("session-exit cancellation" in note for note in body_failure.__notes__)
        assert any("active exchange failure" in note for note in body_failure.__notes__)
    elif mode == "exit_cancel_child":
        with pytest.raises(asyncio.CancelledError) as caught:
            await exit_task
        assert captured_exit_cancellations
        assert caught.value is captured_exit_cancellations[0]
        assert any("active exchange failure" in note for note in caught.value.__notes__)
    else:
        with pytest.raises(LookupError) as caught:
            await exit_task
        assert caught.value is child_failure
    assert child.done()


@pytest.mark.asyncio
async def test_already_done_active_child_failure_is_session_primary(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _transport(harness)
    _patch_empty_journal(monkeypatch, transport)
    manager = transport.session()
    channel = cast(_FileBridgeChannel, await manager.__aenter__())
    failure = RuntimeError("already done child")

    async def fail() -> ResponseArtifact:
        raise failure

    child = asyncio.create_task(fail())
    await asyncio.sleep(0)
    assert child.done()
    channel._active_task = child  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(RuntimeError) as caught:
        await manager.__aexit__(None, None, None)

    assert caught.value is failure


@pytest.mark.asyncio
async def test_fake_peer_observes_mutation_with_real_result_adapter(
    harness: Harness,
) -> None:
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    mutation = _command(
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
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee1001"),
    )
    destructive = _command(
        harness,
        "unit.delete",
        {"unit_guid": "UNIT-DELETE"},
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee1002"),
        trusted_enrichment={"confirmation_proof": "f" * 64},
    )
    mutation_result: dict[str, JsonValue] = {
        "unit_guid": "UNIT-NEW",
        "name": "Created unit",
        "side_guid": "SIDE-1",
        "dbid": 1,
        "latitude": 1.5,
        "longitude": 2.5,
    }
    destructive_result: dict[str, JsonValue] = {
        "deleted_guid": "UNIT-DELETE",
        "deleted_name": "Retired unit",
        "object_kind": "unit",
    }
    peer.enqueue(
        Respond(result=mutation_result),
        Respond(result=destructive_result),
    )
    mutation_result["dbid"] = "caller mutation must not leak"
    destructive_result["object_kind"] = "invalid caller mutation"

    async def wait_for_response_count(count: int) -> None:
        async with asyncio.timeout(2):
            while trace.count("peer.response_written") < count:
                await asyncio.sleep(0)

    envelopes: list[ResponseEnvelope] = []
    async with peer:
        for index, command in enumerate((mutation, destructive), start=1):
            delivery = prepare_delivery(
                command.body,
                request_id=command.request_id,
                delivery_id=UUID(f"11111111-1111-4111-8111-11111111100{index}"),
                delivery_kind="request",
            )
            harness.paths.inbox.write_bytes(render_delivery_lua(delivery, harness.snapshot))
            await wait_for_response_count(index)
            raw_outer: object = json.loads(
                harness.paths.response_path(command.request_id).read_bytes()
            )
            assert isinstance(raw_outer, dict)
            outer = cast(dict[object, object], raw_outer)
            comments = outer.get("Comments")
            assert isinstance(comments, str)
            envelopes.append(ResponseEnvelope.model_validate_json(comments))

    assert [delivery.request_id for delivery in peer.observed_deliveries] == [
        mutation.request_id,
        destructive.request_id,
    ]
    assert envelopes[0].result == {
        "unit_guid": "UNIT-NEW",
        "name": "Created unit",
        "side_guid": "SIDE-1",
        "dbid": 1,
        "latitude": 1.5,
        "longitude": 2.5,
    }
    assert envelopes[1].result == {
        "deleted_guid": "UNIT-DELETE",
        "deleted_name": "Retired unit",
        "object_kind": "unit",
    }

    invalid_trace: list[str] = []
    invalid_peer = _peer(harness, trace=invalid_trace)
    invalid_peer.enqueue(Respond(result={"unit_guid": "missing required mutation fields"}))
    invalid_delivery = prepare_delivery(
        mutation.body,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee1003"),
        delivery_id=UUID("11111111-1111-4111-8111-111111111003"),
        delivery_kind="request",
    )
    await invalid_peer.start()
    harness.paths.inbox.write_bytes(render_delivery_lua(invalid_delivery, harness.snapshot))
    async with asyncio.timeout(2):
        while "peer.request_observed" not in invalid_trace:
            await asyncio.sleep(0)
    with pytest.raises(AssertionError, match="observed invocation"):
        await invalid_peer.stop()


@pytest.mark.asyncio
async def test_fake_peer_deep_snapshots_nested_delay_result_error_and_override(
    harness: Harness,
) -> None:
    trace: list[str] = []
    peer = _peer(harness, trace=trace)
    result_command = _command(
        harness,
        "side.list",
        {"fields": ["name"]},
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee4001"),
    )
    error_command = _scenario_command(
        harness,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee4002"),
    )
    result = cast(dict[str, JsonValue], peer.result_for(result_command.invocation))
    error: dict[str, JsonValue] = {
        "code": ErrorCode.NOT_FOUND.value,
        "message": "nested error snapshot",
        "details": {"selectors": [{"guid": "ORIGINAL"}]},
        "mutation_not_started": None,
    }
    mutable_override: JsonValue = [{"candidate": "ORIGINAL"}]
    peer.enqueue(
        Delay(0, Respond(result=result)),
        Delay(0, Respond(error=error)),
        Delay(
            0,
            Respond(
                result=peer.result_for(error_command.invocation),
                envelope_overrides=(("scenario_lineage_id", mutable_override),),
            ),
        ),
    )
    original_result = json.loads(_canonical_json(result))
    original_error = json.loads(_canonical_json(error))
    cast(list[dict[str, JsonValue]], result["items"])[0]["name"] = "MUTATED"
    cast(dict[str, JsonValue], error["details"])["selectors"] = [{"guid": "MUTATED"}]
    cast(list[dict[str, JsonValue]], mutable_override)[0]["candidate"] = "MUTATED"

    queued = list(cast(Any, peer)._actions)
    queued_override = cast(Delay, queued[2])
    normalized_override = cast(Respond, queued_override.then)
    assert normalized_override.envelope_overrides == (
        ("scenario_lineage_id", [{"candidate": "ORIGINAL"}]),
    )

    async def wait_for_responses(count: int) -> None:
        async with asyncio.timeout(2):
            while trace.count("peer.response_written") < count:
                await asyncio.sleep(0)

    envelopes: list[ResponseEnvelope] = []
    async with peer:
        for index, command in enumerate((result_command, error_command), start=1):
            delivery = prepare_delivery(
                command.body,
                request_id=command.request_id,
                delivery_id=UUID(f"11111111-1111-4111-8111-11111111140{index}"),
                delivery_kind="request",
            )
            harness.paths.inbox.write_bytes(render_delivery_lua(delivery, harness.snapshot))
            await wait_for_responses(index)
            raw_outer: object = json.loads(
                harness.paths.response_path(command.request_id).read_bytes()
            )
            assert isinstance(raw_outer, dict)
            outer = cast(dict[object, object], raw_outer)
            comments = outer.get("Comments")
            assert isinstance(comments, str)
            envelopes.append(ResponseEnvelope.model_validate_json(comments))

    assert envelopes[0].result == original_result
    assert envelopes[1].error is not None
    assert envelopes[1].error.model_dump(mode="json") == original_error
