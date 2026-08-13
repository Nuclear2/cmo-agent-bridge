import json
import sqlite3
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from pydantic import JsonValue

from cmo_agent_bridge.application.service import BridgeApplication
from cmo_agent_bridge.application.queue_service import QueueService
from cmo_agent_bridge.application.queue_worker import QueueWorker
from cmo_agent_bridge.bootstrap import (
    POLL_ACTION_SCRIPT,
    PreparedBridge,
    TrustedLocalPolicy,
    build_application_runtime,
    prepare_bridge,
)
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes, request_sha256
from cmo_agent_bridge.protocol.lua_delivery import render_idle_lua
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.models import RequestBody
from cmo_agent_bridge.runtime_bundle import render_dispatcher
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.state.models import HostRequestState
from cmo_agent_bridge.state.non_effectful_resolution import (
    parse_non_effectful_host_resolution,
)
from cmo_agent_bridge.state.operation_queue import (
    OperationQueueRecord,
    OperationQueueState,
    OperationQueueStore,
)
from cmo_agent_bridge.state.request_ledger import RequestLedger, RequestRecord
from cmo_agent_bridge.state.sqlite import StateDatabase


_LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
_ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")
_ACTIVATION_CANDIDATE = UUID("55555555-5555-4555-8555-555555555555")


def _game_root(tmp_path: Path) -> Path:
    root = tmp_path / "CMO"
    root.mkdir()
    (root / "Command.exe").write_bytes(b"test marker")
    (root / "Lua").mkdir()
    (root / "ImportExport").mkdir()
    return root


def _state_stores(
    prepared: PreparedBridge,
) -> tuple[RequestLedger, OperationQueueStore]:
    database = StateDatabase(prepared.paths.sqlite_file)
    database.initialize()
    catalog = ManifestCatalog(
        ReleaseBinding(snapshot=prepared.runtime_snapshot, registry=OPERATION_REGISTRY)
    )
    return RequestLedger(database, catalog), OperationQueueStore(database)


def _quarantine_host_request(
    prepared: PreparedBridge,
    *,
    operation: str,
    public_arguments: dict[str, object],
    trusted_enrichment: dict[str, object] | None = None,
    request_id: UUID | None = None,
    state: HostRequestState = HostRequestState.QUARANTINED,
) -> RequestRecord:
    selected_id = uuid4() if request_id is None else request_id
    invocation = OPERATION_REGISTRY.resolve_invocation(
        operation,
        public_arguments,
        trusted_enrichment,
    )
    body = RequestBody(
        protocol=prepared.runtime_snapshot.protocol,
        release_id=prepared.runtime_snapshot.release_id,
        runtime_version=prepared.runtime_snapshot.runtime_version,
        runtime_tag=prepared.runtime_snapshot.runtime_tag,
        runtime_asset_sha256=prepared.runtime_snapshot.runtime_asset_sha256,
        expected_lineage_id=_LINEAGE_ID,
        expected_activation_id=_ACTIVATION_ID,
        operation_manifest_sha256=prepared.runtime_snapshot.operation_manifest_sha256,
        operation=operation,
        arguments=cast(
            dict[str, JsonValue],
            invocation.wire_arguments.model_dump(mode="json"),
        ),
    )
    record = RequestRecord(
        request_id=selected_id,
        root_key=prepared.paths.root_key,
        request_hash=request_sha256(body),
        operation=operation,
        operation_class=invocation.effective_class,
        state=HostRequestState.PREPARED,
        runtime_snapshot=prepared.runtime_snapshot,
        result_schema_id=invocation.result_schema.schema_id,
        recovery_schema_id=(
            None
            if invocation.recovery_schema is None
            else invocation.recovery_schema.schema_id
        ),
        body_json=canonical_body_bytes(body),
        lineage_id=_LINEAGE_ID,
        activation_id=_ACTIVATION_ID,
        result_json=None,
        error_json=None,
        resolution_json=None,
        created_at_ms=100,
        updated_at_ms=100,
        terminal_at_ms=None,
    )
    ledger, _queue_store = _state_stores(prepared)
    ledger.insert_prepared(record)
    if state is HostRequestState.PREPARED:
        return record
    return ledger.transition(
        selected_id,
        expected_states=frozenset({HostRequestState.PREPARED}),
        new_state=state,
        updated_at_ms=101,
        error_json=(
            json.dumps(
                {
                    "code": "STATE_CONFLICT",
                    "details": {"reason": "publication_outcome_unknown"},
                    "message": "test quarantine",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            if state is HostRequestState.QUARANTINED
            else None
        ),
    )


def test_prepare_deploys_release_bound_runtime_and_builds_application(tmp_path: Path) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()

    prepared = prepare_bridge(
        game_root=game_root,
        local_app_data=local_app_data,
    )

    assert prepared.dispatcher_path.read_bytes() == render_dispatcher(prepared.runtime_snapshot)
    assert prepared.paths.inbox.read_bytes() == render_idle_lua()
    assert prepared.poll_path.read_text(encoding="ascii") == POLL_ACTION_SCRIPT + "\n"
    assert (local_app_data / "CMOAgentBridge" / "config.toml").is_file()

    runtime = build_application_runtime(local_app_data=local_app_data)
    assert type(runtime.application) is BridgeApplication
    assert type(runtime.queue_service) is QueueService
    assert type(runtime.queue_worker) is QueueWorker
    assert runtime.paths == prepared.paths
    assert runtime.runtime_snapshot == prepared.runtime_snapshot
    assert runtime.paths.sqlite_file.is_file()
    with sqlite3.connect(runtime.paths.sqlite_file) as connection:
        queue_table = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'operation_queue'"
        ).fetchone()
    assert queue_table == ("operation_queue",)


def test_build_application_rejects_a_drifted_dispatcher(tmp_path: Path) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    prepared = prepare_bridge(game_root=game_root, local_app_data=local_app_data)
    prepared.dispatcher_path.write_bytes(b"return false\n")

    with pytest.raises(BridgeError) as caught:
        build_application_runtime(local_app_data=local_app_data)

    assert caught.value.code is ErrorCode.BRIDGE_NOT_PREPARED


def test_prepare_never_overwrites_inbox_while_pending_journal_exists(
    tmp_path: Path,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    prepared = prepare_bridge(game_root=game_root, local_app_data=local_app_data)
    owned_inbox = b"published durable request\n"
    prepared.paths.inbox.write_bytes(owned_inbox)
    prepared.paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    prepared.paths.pending_file.write_bytes(b"pending")

    with pytest.raises(BridgeError) as caught:
        prepare_bridge(game_root=game_root, local_app_data=local_app_data)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert prepared.paths.inbox.read_bytes() == owned_inbox


@pytest.mark.parametrize(
    ("operation", "trusted_enrichment", "host_state"),
    [
        ("bridge.status", {"activation_candidate": _ACTIVATION_CANDIDATE}, state)
        for state in (
            HostRequestState.PREPARED,
            HostRequestState.PUBLISHED,
            HostRequestState.RESPONSE_ACCEPTED,
            HostRequestState.IDLE_PUBLISHED,
            HostRequestState.QUARANTINED,
        )
    ]
    + [
        ("scenario.get", None, HostRequestState.QUARANTINED),
    ],
)
def test_prepare_auto_resolves_orphaned_non_effectful_host_quarantine(
    tmp_path: Path,
    operation: str,
    trusted_enrichment: dict[str, object] | None,
    host_state: HostRequestState,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    prepared = prepare_bridge(game_root=game_root, local_app_data=local_app_data)
    quarantined = _quarantine_host_request(
        prepared,
        operation=operation,
        public_arguments={},
        trusted_enrichment=trusted_enrichment,
        state=host_state,
    )

    prepare_bridge(game_root=game_root, local_app_data=local_app_data)

    ledger, queue_store = _state_stores(prepared)
    resolved = ledger.get_request(quarantined.request_id)
    assert resolved is not None
    assert resolved.state is HostRequestState.RESOLVED
    assert resolved.result_json is None
    assert resolved.error_json is None
    assert resolved.resolution_json is not None
    assert resolved.terminal_at_ms == resolved.updated_at_ms
    assert queue_store.get(quarantined.request_id) is None
    assert not prepared.paths.pending_file.exists()
    marker = parse_non_effectful_host_resolution(resolved.resolution_json)
    assert marker.request_id == quarantined.request_id
    assert marker.request_hash == quarantined.request_hash
    assert marker.operation == operation
    assert marker.operation_class is quarantined.operation_class
    assert marker.mode == "non_effectful_outcome_irrelevant"
    assert marker.disposition == "abandoned"
    assert marker.reason == "prepare_migrated_orphaned_non_effectful_request"
    assert marker.failure is None


def test_prepare_does_not_migrate_non_effectful_orphan_while_journal_exists(
    tmp_path: Path,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    prepared = prepare_bridge(game_root=game_root, local_app_data=local_app_data)
    quarantined = _quarantine_host_request(
        prepared,
        operation="bridge.status",
        public_arguments={},
        trusted_enrichment={"activation_candidate": _ACTIVATION_CANDIDATE},
    )
    prepared.paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    prepared.paths.pending_file.write_bytes(b"owned pending journal")

    with pytest.raises(BridgeError) as caught:
        prepare_bridge(game_root=game_root, local_app_data=local_app_data)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.details["pending_journal"] is True
    ledger, _queue_store = _state_stores(prepared)
    unchanged = ledger.get_request(quarantined.request_id)
    assert unchanged is not None
    assert unchanged.state is HostRequestState.QUARANTINED
    assert unchanged.resolution_json is None


def test_prepare_rejects_non_effectful_quarantine_with_any_queue_record(
    tmp_path: Path,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    prepared = prepare_bridge(game_root=game_root, local_app_data=local_app_data)
    request_id = uuid4()
    quarantined = _quarantine_host_request(
        prepared,
        operation="bridge.status",
        public_arguments={},
        trusted_enrichment={"activation_candidate": _ACTIVATION_CANDIDATE},
        request_id=request_id,
    )
    _ledger, queue_store = _state_stores(prepared)
    queued = queue_store.enqueue(
        OperationQueueRecord(
            queue_sequence=0,
            request_id=request_id,
            root_key=quarantined.root_key,
            operation=quarantined.operation,
            arguments_json=b"{}",
            body_json=quarantined.body_json,
            runtime_snapshot=quarantined.runtime_snapshot,
            result_schema_id=quarantined.result_schema_id,
            recovery_schema_id=quarantined.recovery_schema_id,
            expected_lineage_id=quarantined.lineage_id,
            expected_activation_id=quarantined.activation_id,
            expected_process_pid=42,
            expected_process_create_time=1.0,
            state=OperationQueueState.QUEUED,
            result_json=None,
            error_json=None,
            created_at_ms=102,
            updated_at_ms=102,
            terminal_at_ms=None,
        )
    )
    claimed = queue_store.claim_next(root_key=prepared.paths.root_key, at_ms=103)
    assert claimed is not None and claimed.request_id == queued.request_id
    assert queue_store.complete(request_id, b"{}", at_ms=104) is not None
    owned_inbox = b"owned inbox must survive failed prepare\n"
    prepared.paths.inbox.write_bytes(owned_inbox)

    with pytest.raises(BridgeError) as caught:
        prepare_bridge(game_root=game_root, local_app_data=local_app_data)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.details["pending_journal"] is False
    assert caught.value.details["nonterminal_queue_requests"] == 0
    assert caught.value.details["host_quarantine_barriers"] == [
        {
            "request_id": str(request_id),
            "operation": "bridge.status",
            "operation_class": "status",
            "host_request_state": "quarantined",
            "reason": "associated_operation_queue_record",
            "operation_queue_state": "completed",
        }
    ]
    assert prepared.paths.inbox.read_bytes() == owned_inbox
    ledger, _queue_store = _state_stores(prepared)
    unchanged = ledger.get_request(request_id)
    assert unchanged is not None and unchanged.state is HostRequestState.QUARANTINED


def test_prepare_rejects_impossible_non_effectful_cancel_published_state(
    tmp_path: Path,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    prepared = prepare_bridge(game_root=game_root, local_app_data=local_app_data)
    malformed = _quarantine_host_request(
        prepared,
        operation="bridge.status",
        public_arguments={},
        trusted_enrichment={"activation_candidate": _ACTIVATION_CANDIDATE},
        state=HostRequestState.CANCEL_PUBLISHED,
    )

    with pytest.raises(BridgeError) as caught:
        prepare_bridge(game_root=game_root, local_app_data=local_app_data)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.details["host_quarantine_barriers"] == [
        {
            "request_id": str(malformed.request_id),
            "operation": "bridge.status",
            "operation_class": "status",
            "host_request_state": "cancel_published",
            "reason": "unsupported_host_state_or_operation_class",
        }
    ]
    ledger, _queue_store = _state_stores(prepared)
    unchanged = ledger.get_request(malformed.request_id)
    assert unchanged is not None
    assert unchanged.state is HostRequestState.CANCEL_PUBLISHED


@pytest.mark.parametrize(
    (
        "operation",
        "public_arguments",
        "trusted_enrichment",
        "operation_class",
        "host_state",
    ),
    [
        (
            "scenario.time_compression.set",
            {"code": 3},
            None,
            "mutation",
            HostRequestState.PREPARED,
        ),
        (
            "unit.delete",
            {"unit_guid": "UNIT-1"},
            {"confirmation_proof": "a" * 64},
            "destructive",
            HostRequestState.IDLE_PUBLISHED,
        ),
        (
            "bridge.reconcile",
            {
                "request_id": UUID("66666666-6666-4666-8666-666666666666"),
                "disposition": "not_applied",
            },
            {"confirmation_proof": "b" * 64},
            "reconcile",
            HostRequestState.QUARANTINED,
        ),
    ],
)
def test_prepare_fails_closed_for_effectful_host_quarantine_without_journal(
    tmp_path: Path,
    operation: str,
    public_arguments: dict[str, object],
    trusted_enrichment: dict[str, object] | None,
    operation_class: str,
    host_state: HostRequestState,
) -> None:
    game_root = _game_root(tmp_path)
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    prepared = prepare_bridge(game_root=game_root, local_app_data=local_app_data)
    quarantined = _quarantine_host_request(
        prepared,
        operation=operation,
        public_arguments=public_arguments,
        trusted_enrichment=trusted_enrichment,
        state=host_state,
    )
    owned_inbox = b"effectful quarantine owns prepare barrier\n"
    prepared.paths.inbox.write_bytes(owned_inbox)
    assert not prepared.paths.pending_file.exists()

    with pytest.raises(BridgeError) as caught:
        prepare_bridge(game_root=game_root, local_app_data=local_app_data)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.details["pending_journal"] is False
    assert caught.value.details["nonterminal_queue_requests"] == 0
    assert caught.value.details["host_quarantine_barriers"] == [
        {
            "request_id": str(quarantined.request_id),
            "operation": operation,
            "operation_class": operation_class,
            "host_request_state": host_state.value,
            "reason": "effectful_host_nonterminal",
        }
    ]
    assert "missing mutation journal" in caught.value.details["next_step"]
    assert prepared.paths.inbox.read_bytes() == owned_inbox
    ledger, _queue_store = _state_stores(prepared)
    unchanged = ledger.get_request(quarantined.request_id)
    assert unchanged is not None and unchanged.state is host_state


@pytest.mark.parametrize(
    ("allow_mutations", "allow_destructive", "denied_setting"),
    [
        (False, True, "allow_mutations"),
        (True, False, "allow_destructive"),
    ],
)
def test_trusted_local_policy_gates_confirmed_deletes_with_local_settings(
    allow_mutations: bool,
    allow_destructive: bool,
    denied_setting: str,
) -> None:
    policy = TrustedLocalPolicy(
        allow_mutations=allow_mutations,
        allow_destructive=allow_destructive,
    )

    with pytest.raises(BridgeError) as caught:
        policy.ensure_destructive_allowed(
            status=cast(Any, None),
            contract=OPERATION_REGISTRY.resolve("unit.delete"),
            runtime_snapshot=cast(Any, None),
        )

    assert caught.value.code is ErrorCode.POLICY_DENIED
    assert caught.value.details["setting"] == denied_setting


def test_trusted_local_policy_allows_confirmed_delete_when_enabled() -> None:
    policy = TrustedLocalPolicy(allow_mutations=True, allow_destructive=True)

    policy.ensure_destructive_allowed(
        status=cast(Any, None),
        contract=OPERATION_REGISTRY.resolve("mission.delete"),
        runtime_snapshot=cast(Any, None),
    )
