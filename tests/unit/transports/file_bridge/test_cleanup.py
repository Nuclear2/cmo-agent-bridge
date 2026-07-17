from __future__ import annotations

import ctypes
import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Never, cast
from uuid import UUID

import pytest
from pydantic import JsonValue, ValidationError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua, render_idle_lua
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.models import PreparedDelivery, RequestBody
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CompletedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
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
from cmo_agent_bridge.state.pending_journal import PendingJournalStore
from cmo_agent_bridge.state.request_ledger import RequestLedger, RequestRecord
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge import cleanup as cleanup_module
from cmo_agent_bridge.transports.file_bridge.cleanup import ArtifactJanitor, CleanupReport
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths


DAY_MS = 24 * 60 * 60 * 1000
NOW_MS = 5 * DAY_MS
LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")


@dataclass(frozen=True, slots=True)
class _Rig:
    paths: FileBridgePaths
    lock: RootLock
    database: StateDatabase
    catalog: ManifestCatalog
    ledger: RequestLedger
    journals: PendingJournalStore
    snapshot: RuntimeSnapshot

    def janitor(self, *, retention_ms: int = DAY_MS) -> ArtifactJanitor:
        return ArtifactJanitor(
            self.paths,
            self.lock,
            self.ledger,
            self.journals,
            retention_ms=retention_ms,
        )


@dataclass(frozen=True, slots=True)
class _Seed:
    request_id: UUID
    delivery_id: UUID
    response_path: Path
    raw: bytes
    inbox_bytes: bytes
    pending_exchange: PendingExchange


@pytest.fixture
def rig(tmp_path: Path) -> _Rig:
    game_root = tmp_path / "game"
    game_root.mkdir()
    (game_root / "Command.exe").write_bytes(b"exe")
    (game_root / "Lua").mkdir()
    (game_root / "ImportExport").mkdir()
    local_app_data = tmp_path / "local"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    paths.inbox.parent.mkdir(parents=True)
    paths.inbox.write_bytes(render_idle_lua())
    snapshot = RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=MANIFEST_SHA256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )
    catalog = ManifestCatalog(ReleaseBinding(snapshot=snapshot, registry=OPERATION_REGISTRY))
    database = StateDatabase(paths.sqlite_file)
    database.initialize()
    lock = RootLock(paths.lock_file, timeout_seconds=0)
    ledger = RequestLedger(database, catalog)
    journals = PendingJournalStore(
        paths,
        lock,
        catalog,
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
    )
    return _Rig(paths, lock, database, catalog, ledger, journals, snapshot)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _seed_response(
    rig: _Rig,
    *,
    request_id: UUID,
    delivery_id: UUID,
    terminal_at_ms: int | None,
    raw: bytes | None = None,
) -> _Seed:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "unit.add",
        {
            "side_guid": "SIDE-1",
            "unit_type": "Aircraft",
            "dbid": 1,
            "name": "Janitor test",
            "latitude": 1.5,
            "longitude": 2.5,
            "altitude": None,
            "loadout_dbid": None,
        },
    )
    body = RequestBody(
        protocol=rig.snapshot.protocol,
        release_id=rig.snapshot.release_id,
        runtime_version=rig.snapshot.runtime_version,
        runtime_tag=rig.snapshot.runtime_tag,
        runtime_asset_sha256=rig.snapshot.runtime_asset_sha256,
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=ACTIVATION_ID,
        operation_manifest_sha256=rig.snapshot.operation_manifest_sha256,
        operation="unit.add",
        arguments=invocation.wire_arguments.model_dump(mode="json"),
    )
    body_bytes = canonical_body_bytes(body)
    request_hash = hashlib.sha256(body_bytes).hexdigest()
    prepared = PreparedDelivery(
        request_id=request_id,
        delivery_id=delivery_id,
        delivery_kind="request",
        request_hash=request_hash,
        body_json=body_bytes,
    )
    inbox_bytes = render_delivery_lua(prepared, rig.snapshot)
    response_filename = f"CMOAgentBridge_Response_{request_id}.inst"
    unpublished_intent = DeliveryIntent(
        request_id=request_id,
        delivery_id=delivery_id,
        delivery_kind="request",
        original_request_delivery_id=delivery_id,
        body_json=body_bytes.decode("utf-8"),
        request_hash=request_hash,
        runtime_snapshot=rig.snapshot,
        result_schema_id=invocation.result_schema.schema_id,
        recovery_schema_id=(
            None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
        ),
        intended_at_ms=100,
        published_at_ms=None,
        rendered_inbox_sha256=hashlib.sha256(inbox_bytes).hexdigest(),
        rendered_inbox_size_bytes=len(inbox_bytes),
        response_filename=response_filename,
    )
    published_intent = unpublished_intent.model_copy(update={"published_at_ms": 101})
    rig.ledger.insert_prepared(
        RequestRecord(
            request_id=request_id,
            root_key=rig.paths.root_key,
            request_hash=request_hash,
            operation="unit.add",
            operation_class=invocation.effective_class,
            state=HostRequestState.PREPARED,
            runtime_snapshot=rig.snapshot,
            result_schema_id=invocation.result_schema.schema_id,
            recovery_schema_id=(
                None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
            ),
            body_json=body_bytes,
            lineage_id=LINEAGE_ID,
            activation_id=ACTIVATION_ID,
            result_json=None,
            error_json=None,
            resolution_json=None,
            created_at_ms=100,
            updated_at_ms=100,
            terminal_at_ms=None,
        )
    )
    rig.ledger.insert_delivery(published_intent)
    result = cast(
        JsonValue,
        {
            "unit_guid": "UNIT-1",
            "name": "Janitor test",
            "side_guid": "SIDE-1",
            "dbid": 1,
            "latitude": 1.5,
            "longitude": 2.5,
        },
    )
    accepted = AcceptedResponse(
        envelope=ResponseEnvelope(
            protocol="cmo-agent-bridge/1",
            request_id=request_id,
            delivery_id=delivery_id,
            request_hash=request_hash,
            ok=True,
            result=result,
            error=None,
            scenario_time="2026-07-10T13:00:00Z",
            scenario_lineage_id=LINEAGE_ID,
            activation_id=ACTIVATION_ID,
            operation_manifest_sha256=rig.snapshot.operation_manifest_sha256,
            bridge_version=rig.snapshot.runtime_version,
            runtime_tag=rig.snapshot.runtime_tag,
            runtime_asset_sha256=rig.snapshot.runtime_asset_sha256,
            release_id=rig.snapshot.release_id,
        ),
        delivery_kind="request",
        settlement=CompletedSettlement(state="completed", result=result),
        cancel_ack=None,
    )
    response_raw = raw if raw is not None else f"response:{request_id}".encode()
    accepted_at_ms = 102 if terminal_at_ms is None else terminal_at_ms
    artifact = ResponseArtifact(
        filename=response_filename,
        sha256=hashlib.sha256(response_raw).hexdigest(),
        size_bytes=len(response_raw),
        accepted_at_ms=accepted_at_ms,
        accepted_response=accepted,
    )
    rig.ledger.record_response(artifact)
    if terminal_at_ms is None:
        rig.ledger.transition(
            request_id,
            expected_states=frozenset({HostRequestState.PREPARED}),
            new_state=HostRequestState.RESPONSE_ACCEPTED,
            updated_at_ms=accepted_at_ms,
        )
    else:
        rig.ledger.transition(
            request_id,
            expected_states=frozenset({HostRequestState.PREPARED}),
            new_state=HostRequestState.COMPLETED,
            updated_at_ms=terminal_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=_canonical_json(result),
        )
    response_path = rig.paths.response_path(request_id)
    response_path.write_bytes(response_raw)
    accepted_at_ns = accepted_at_ms * 1_000_000
    os.utime(response_path, ns=(accepted_at_ns, accepted_at_ns))
    pending_exchange = PendingExchange(
        request_id=request_id,
        request_hash=request_hash,
        operation="unit.add",
        effective_class=invocation.effective_class,
        body_json=body_bytes.decode("utf-8"),
        runtime_snapshot=rig.snapshot,
        result_schema_id=invocation.result_schema.schema_id,
        recovery_schema_id=(
            None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
        ),
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=ACTIVATION_ID,
        delivery_intents=(unpublished_intent,),
        response_artifact=None,
        settlement=None,
        original_target_request_id=None,
        original_target_request_hash=None,
        revision=0,
        state=PendingPhase.PREPARED,
        created_at_ms=100,
        updated_at_ms=100,
    )
    return _Seed(
        request_id=request_id,
        delivery_id=delivery_id,
        response_path=response_path,
        raw=response_raw,
        inbox_bytes=inbox_bytes,
        pending_exchange=pending_exchange,
    )


async def _sweep(rig: _Rig, *, now_ms: int = NOW_MS) -> CleanupReport:
    result: CleanupReport | None = None
    async with rig.lock:
        result = rig.janitor().sweep(now_ms)
    if result is None:
        raise AssertionError("RootLock unexpectedly suppressed the cleanup result")
    return result


@pytest.mark.asyncio
async def test_terminal_exact_response_older_than_default_retention_is_deleted_with_iterdir_only(
    rig: _Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0001"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110001"),
        terminal_at_ms=NOW_MS - DAY_MS - 1,
    )
    original_iterdir = Path.iterdir
    scans: list[Path] = []

    def traced_iterdir(path: Path) -> Any:
        scans.append(path)
        return original_iterdir(path)

    def wildcard_forbidden(*_args: object, **_kwargs: object) -> Never:
        raise AssertionError("ArtifactJanitor must not use wildcard filesystem APIs")

    monkeypatch.setattr(Path, "iterdir", traced_iterdir)
    monkeypatch.setattr(Path, "glob", wildcard_forbidden)
    monkeypatch.setattr(Path, "rglob", wildcard_forbidden)

    report = await _sweep(rig)

    assert report == CleanupReport(
        scanned=1,
        deleted=1,
        retained=0,
        failed=0,
        failed_paths=(),
    )
    assert scans == [rig.paths.import_export]
    assert not seed.response_path.exists()
    assert rig.ledger.get_request(seed.request_id) is not None
    assert rig.ledger.get_delivery(seed.delivery_id) is not None


@pytest.mark.parametrize("age_ms", [DAY_MS, DAY_MS - 1])
@pytest.mark.asyncio
async def test_terminal_response_at_or_inside_cutoff_is_retained(
    rig: _Rig,
    age_ms: int,
) -> None:
    seed = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0002"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110002"),
        terminal_at_ms=NOW_MS - age_ms,
    )

    report = await _sweep(rig)

    assert (report.scanned, report.deleted, report.retained, report.failed) == (1, 0, 1, 0)
    assert seed.response_path.read_bytes() == seed.raw


@pytest.mark.asyncio
async def test_unrecorded_and_nonterminal_responses_are_retained(rig: _Rig) -> None:
    unrecorded_id = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0003")
    unrecorded = rig.paths.response_path(unrecorded_id)
    unrecorded.write_bytes(b"unrecorded")
    nonterminal = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0004"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110004"),
        terminal_at_ms=None,
    )

    report = await _sweep(rig)

    assert (report.scanned, report.deleted, report.retained, report.failed) == (2, 0, 2, 0)
    assert unrecorded.read_bytes() == b"unrecorded"
    assert nonterminal.response_path.read_bytes() == nonterminal.raw


@pytest.mark.asyncio
async def test_pending_journal_exact_reference_retains_otherwise_eligible_response(
    rig: _Rig,
) -> None:
    seed = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0005"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110005"),
        terminal_at_ms=NOW_MS - DAY_MS - 1,
    )
    journal = PendingJournal(
        header=PendingJournalHeader(
            format="cmo-agent-bridge/pending-journal",
            header_version=1,
            root_key=rig.paths.root_key,
            required_release_id=rig.snapshot.release_id,
        ),
        original=seed.pending_exchange,
        reconcile_attempt=None,
    )
    async with rig.lock:
        rig.journals.save(journal, expected_revisions=None)
        report = rig.janitor().sweep(NOW_MS)
        assert (report.scanned, report.deleted, report.retained, report.failed) == (1, 0, 1, 0)
        assert seed.response_path.read_bytes() == seed.raw
        assert rig.paths.pending_file.exists()


@pytest.mark.asyncio
async def test_exact_inbox_reference_retains_otherwise_eligible_response(rig: _Rig) -> None:
    seed = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0006"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110006"),
        terminal_at_ms=NOW_MS - DAY_MS - 1,
    )
    rig.paths.inbox.write_bytes(seed.inbox_bytes)

    report = await _sweep(rig)

    assert (report.scanned, report.deleted, report.retained, report.failed) == (1, 0, 1, 0)
    assert seed.response_path.read_bytes() == seed.raw
    assert rig.paths.inbox.read_bytes() == seed.inbox_bytes


@pytest.mark.asyncio
async def test_durable_delivery_rendering_drift_fails_closed_before_inbox_cleanup(
    rig: _Rig,
) -> None:
    seed = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0015"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110015"),
        terminal_at_ms=NOW_MS - DAY_MS - 1,
    )
    rig.paths.inbox.write_bytes(seed.inbox_bytes)
    forged = render_idle_lua()
    with sqlite3.connect(rig.database.path) as connection:
        connection.execute(
            "UPDATE deliveries SET rendered_inbox_sha256=?,rendered_inbox_size_bytes=? "
            "WHERE delivery_id=?",
            (hashlib.sha256(forged).hexdigest(), len(forged), str(seed.delivery_id)),
        )
        connection.commit()

    with pytest.raises(BridgeError) as caught:
        await _sweep(rig)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert seed.response_path.read_bytes() == seed.raw
    assert rig.paths.inbox.read_bytes() == seed.inbox_bytes


@pytest.mark.parametrize("mismatch", ["hash", "size"])
@pytest.mark.asyncio
async def test_response_bytes_must_match_recorded_hash_and_size(
    rig: _Rig,
    mismatch: str,
) -> None:
    seed = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0007"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110007"),
        terminal_at_ms=NOW_MS - DAY_MS - 1,
        raw=b"recorded response bytes",
    )
    if mismatch == "hash":
        seed.response_path.write_bytes(b"Recorded response bytes")
    else:
        seed.response_path.write_bytes(seed.raw + b"!")

    report = await _sweep(rig)

    assert (report.scanned, report.deleted, report.retained, report.failed) == (1, 0, 1, 0)
    assert seed.response_path.exists()


@pytest.mark.asyncio
async def test_foreign_lookalike_uppercase_and_noncanonical_names_are_never_deleted(
    rig: _Rig,
) -> None:
    names = (
        "foreign.inst",
        "CMOAgentBridge_Response_aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0008.inst.bak",
        "CMOAgentBridge_Response_AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEE0008.inst",
        "CMOAgentBridge_Response_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.inst",
        "CMOAgentBridge_Response_aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0008.INST",
    )
    paths = tuple(rig.paths.import_export / name for name in names)
    for path in paths:
        path.write_bytes(path.name.encode("ascii"))

    report = await _sweep(rig)

    assert report.deleted == 0
    assert report.failed == 0
    assert report.scanned == report.retained
    assert all(path.exists() for path in paths)


@pytest.mark.asyncio
async def test_directory_and_redirected_candidate_are_retained_without_unlink(
    rig: _Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directory = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0009"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110009"),
        terminal_at_ms=NOW_MS - DAY_MS - 1,
    )
    directory.response_path.unlink()
    directory.response_path.mkdir()
    redirected = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0010"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110010"),
        terminal_at_ms=NOW_MS - DAY_MS - 1,
    )
    outside = rig.paths.game_root.parent / "outside.inst"
    outside.write_bytes(redirected.raw)
    original_resolve = Path.resolve

    def redirect_one(path: Path, strict: bool = False) -> Path:
        if path == redirected.response_path:
            return outside
        return original_resolve(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", redirect_one)

    report = await _sweep(rig)

    assert (report.scanned, report.deleted, report.retained, report.failed) == (2, 0, 2, 0)
    assert directory.response_path.is_dir()
    assert redirected.response_path.exists()
    assert outside.read_bytes() == redirected.raw


def test_constructor_is_zero_io_and_requires_exact_positive_retention(
    rig: _Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> Never:
        raise AssertionError("ArtifactJanitor constructor performed I/O")

    monkeypatch.setattr(Path, "iterdir", forbidden)
    monkeypatch.setattr(PendingJournalStore, "load", forbidden)
    monkeypatch.setattr(StateDatabase, "_transaction", forbidden)
    janitor = ArtifactJanitor(rig.paths, rig.lock, rig.ledger, rig.journals)
    assert type(janitor) is ArtifactJanitor

    for invalid in (0, -1, True, 1.0, "86400000"):
        with pytest.raises(BridgeError) as caught:
            ArtifactJanitor(
                rig.paths,
                rig.lock,
                rig.ledger,
                rig.journals,
                retention_ms=invalid,  # type: ignore[arg-type]
            )
        assert caught.value.code is ErrorCode.INVALID_ARGUMENT


@pytest.mark.asyncio
async def test_sweep_requires_owned_root_lock_before_directory_or_state_io(
    rig: _Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0011"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110011"),
        terminal_at_ms=NOW_MS - DAY_MS - 1,
    )

    def forbidden(*_args: object, **_kwargs: object) -> Never:
        raise AssertionError("sweep touched state before checking RootLock")

    monkeypatch.setattr(Path, "iterdir", forbidden)
    monkeypatch.setattr(PendingJournalStore, "load", forbidden)

    with pytest.raises(BridgeError) as caught:
        rig.janitor().sweep(NOW_MS)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert seed.response_path.read_bytes() == seed.raw


@pytest.mark.asyncio
async def test_unlink_failure_is_reported_and_does_not_stop_or_delete_safe_state(
    rig: _Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0012"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110012"),
        terminal_at_ms=NOW_MS - DAY_MS - 1,
    )
    deletable = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0013"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110013"),
        terminal_at_ms=NOW_MS - DAY_MS - 1,
    )
    protected = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0014"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110014"),
        terminal_at_ms=NOW_MS - DAY_MS - 1,
    )
    protected.response_path.write_bytes(protected.raw + b"mismatch")
    rig.paths.inbox.write_bytes(render_idle_lua())
    original_delete = cleanup_module._set_delete_disposition  # pyright: ignore[reportPrivateUsage]

    def fail_one(handle: int, path: Path) -> None:
        if path == failed.response_path:
            raise PermissionError("simulated sharing violation")
        original_delete(handle, path)

    monkeypatch.setattr(cleanup_module, "_set_delete_disposition", fail_one)

    report = await _sweep(rig)

    assert report == CleanupReport(
        scanned=3,
        deleted=1,
        retained=1,
        failed=1,
        failed_paths=(failed.response_path.resolve(strict=True),),
    )
    assert failed.response_path.read_bytes() == failed.raw
    assert not deletable.response_path.exists()
    assert protected.response_path.exists()
    assert rig.paths.inbox.read_bytes() == render_idle_lua()
    for seed in (failed, deletable, protected):
        assert rig.ledger.get_request(seed.request_id) is not None
        assert rig.ledger.get_delivery(seed.delivery_id) is not None


@pytest.mark.asyncio
async def test_get_file_type_system_error_is_reported_instead_of_retained(
    rig: _Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = _seed_response(
        rig,
        request_id=UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0016"),
        delivery_id=UUID("11111111-1111-4111-8111-111111110016"),
        terminal_at_ms=NOW_MS - DAY_MS - 1,
    )

    def fail_file_type(_handle: int) -> int:
        ctypes.set_last_error(5)
        return 0

    monkeypatch.setattr(cleanup_module, "_get_file_type", fail_file_type)
    try:
        report = await _sweep(rig)
    finally:
        ctypes.set_last_error(0)

    assert report == CleanupReport(
        scanned=1,
        deleted=0,
        retained=0,
        failed=1,
        failed_paths=(seed.response_path.resolve(strict=True),),
    )
    assert seed.response_path.read_bytes() == seed.raw


def test_cleanup_report_is_strict_frozen_and_rejects_noncanonical_failed_paths(
    tmp_path: Path,
) -> None:
    absolute = (tmp_path / "response.inst").resolve()
    report = CleanupReport(
        scanned=1,
        deleted=0,
        retained=0,
        failed=1,
        failed_paths=(absolute,),
    )
    with pytest.raises(ValidationError):
        report.deleted = 1  # type: ignore[misc]
    with pytest.raises(ValidationError):
        CleanupReport(
            scanned="1",  # type: ignore[arg-type]
            deleted=0,
            retained=0,
            failed=1,
            failed_paths=(absolute,),
        )
    with pytest.raises(ValidationError):
        CleanupReport(
            scanned=1,
            deleted=0,
            retained=0,
            failed=1,
            failed_paths=(Path("relative.inst"),),
        )


def test_constructor_rejects_mismatched_exact_dependencies(rig: _Rig, tmp_path: Path) -> None:
    other_lock = RootLock(rig.paths.lock_file, timeout_seconds=0)
    other_journals = PendingJournalStore(
        rig.paths,
        other_lock,
        rig.catalog,
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
    )
    other_ledger = RequestLedger(StateDatabase(tmp_path / "other.sqlite3"), rig.catalog)
    for ledger, journals in (
        (rig.ledger, other_journals),
        (other_ledger, rig.journals),
        (object(), rig.journals),
    ):
        with pytest.raises(BridgeError) as caught:
            ArtifactJanitor(
                rig.paths,
                rig.lock,
                ledger,  # type: ignore[arg-type]
                journals,
            )
        assert caught.value.code is ErrorCode.INVALID_ARGUMENT
