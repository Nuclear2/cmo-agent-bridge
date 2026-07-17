from __future__ import annotations

# pyright: reportPossiblyUnboundVariable=false

import hashlib
import json
import os
import sqlite3
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, cast
from unittest.mock import Mock
from uuid import UUID

import pytest
from pydantic import JsonValue

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.generated_manifest import OPERATIONS
from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.operations.registry import (
    OPERATION_REGISTRY,
    FrozenInvocation,
    OperationRegistry,
)
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.models import RequestBody
from cmo_agent_bridge.protocol.response_models import (
    CancelledSettlement,
    CompletedSettlement,
    RejectedSettlement,
    ResponseArtifact,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
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
from cmo_agent_bridge.state import pending_journal as pending_journal_module
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths

if TYPE_CHECKING:
    from tests.unit.state.conftest import SemanticHarness


HEADER_VECTOR = (
    '{"format":"cmo-agent-bridge/pending-journal","header_version":1,'
    '"required_release_id":"b73bcd297aa797e0a75557125e5f59261add5ab30bd963f8c23cb945a162edb6",'
    '"root_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'
)
HEADER_SHA256 = "0a964aba7ffaa434d339602f821be6af91908e42b9a21694b4f11eecba54e710"
DELIVERY_INTENT_VECTOR = (
    '{"body_json":"{\\"arguments\\":{},\\"expected_activation_id\\":'
    '\\"44444444-4444-4444-8444-444444444444\\",\\"expected_lineage_id\\":'
    '\\"33333333-3333-4333-8333-333333333333\\",\\"operation\\":\\"scenario.get\\",'
    '\\"operation_manifest_sha256\\":\\"31ae64749ee4e8cb3d7ad3ac98224cbd489606e75802b0de57a87d3a8c15e574\\",'
    '\\"protocol\\":\\"cmo-agent-bridge/1\\",\\"release_id\\":'
    '\\"b73bcd297aa797e0a75557125e5f59261add5ab30bd963f8c23cb945a162edb6\\",'
    '\\"runtime_asset_sha256\\":\\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\\",'
    '\\"runtime_tag\\":\\"0_1_0-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\\",'
    '\\"runtime_version\\":\\"0.1.0\\"}","delivery_id":"11111111-1111-4111-8111-111111111111",'
    '"delivery_kind":"request","intended_at_ms":1752142800000,'
    '"original_request_delivery_id":"11111111-1111-4111-8111-111111111111",'
    '"published_at_ms":null,"recovery_schema_id":null,'
    '"rendered_inbox_sha256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",'
    '"rendered_inbox_size_bytes":512,'
    '"request_hash":"40b3068d97ac199de42b0d0ea9df905ad70b22351c9a9c76dd1c648797af3b95",'
    '"request_id":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",'
    '"response_filename":"CMOAgentBridge_Response_aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.inst",'
    '"result_schema_id":"f33be4419a3404f63da50ae81ac8bed33a5cb17bbdc338f1ac5f995a6b058933",'
    '"runtime_snapshot":{"dependency_lock_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",'
    '"host_contract_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",'
    '"operation_manifest_sha256":"31ae64749ee4e8cb3d7ad3ac98224cbd489606e75802b0de57a87d3a8c15e574",'
    '"protocol":"cmo-agent-bridge/1","release_id":"b73bcd297aa797e0a75557125e5f59261add5ab30bd963f8c23cb945a162edb6",'
    '"runtime_asset_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'
    '"runtime_tag":"0_1_0-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'
    '"runtime_version":"0.1.0"}}'
)
DELIVERY_INTENT_SHA256 = "9410dc639cc0f147438ceb18115a835448c767bc18260a14f519cde2ee1d11c7"
FOREIGN_RELEASE_ID = "f" * 64


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def test_committed_header_vector_and_digest_are_exact() -> None:
    header = PendingJournalHeader.model_validate_json(HEADER_VECTOR)
    assert _canonical(header.model_dump(mode="json")) == HEADER_VECTOR.encode("utf-8")
    assert hashlib.sha256(HEADER_VECTOR.encode("utf-8")).hexdigest() == HEADER_SHA256


def test_committed_delivery_intent_vector_and_digest_are_exact() -> None:
    intent = DeliveryIntent.model_validate_json(DELIVERY_INTENT_VECTOR)
    assert _canonical(intent.model_dump(mode="json")) == DELIVERY_INTENT_VECTOR.encode("utf-8")
    assert (
        hashlib.sha256(DELIVERY_INTENT_VECTOR.encode("utf-8")).hexdigest() == DELIVERY_INTENT_SHA256
    )


def test_manifest_catalog_is_current_release_only_and_defensively_revalidates(
    runtime_snapshot: RuntimeSnapshot,
    manifest_catalog: ManifestCatalog,
) -> None:
    assert manifest_catalog.running_release_id == runtime_snapshot.release_id
    resolved = manifest_catalog.resolve_running(runtime_snapshot.release_id)
    assert resolved.snapshot == runtime_snapshot
    with pytest.raises(BridgeError) as foreign:
        manifest_catalog.resolve_running("f" * 64)
    assert _error_code(foreign) is ErrorCode.MANIFEST_MISMATCH

    forged = runtime_snapshot.model_copy(update={"runtime_tag": "forged"})
    with pytest.raises(ValueError):
        ManifestCatalog(ReleaseBinding(snapshot=forged, registry=resolved.registry))

    entries = [dict(entry) for entry in OPERATIONS]
    confirmation = entries[0]["confirmation_required"]
    assert type(confirmation) is bool
    entries[0]["confirmation_required"] = not confirmation
    mismatched_registry = OperationRegistry(entries)
    with pytest.raises(BridgeError) as mismatch:
        ManifestCatalog(ReleaseBinding(snapshot=runtime_snapshot, registry=mismatched_registry))
    assert _error_code(mismatch) is ErrorCode.MANIFEST_MISMATCH


def _error_code(error: pytest.ExceptionInfo[BridgeError]) -> ErrorCode:
    return error.value.code


def _foreign_journal_bytes(root_key: str, *, original_json: str) -> bytes:
    return (
        '{"header":{"format":"cmo-agent-bridge/pending-journal","header_version":1,'
        f'"required_release_id":"{FOREIGN_RELEASE_ID}","root_key":"{root_key}"}},'
        f'"original":{original_json},"reconcile_attempt":null}}'
    ).encode("utf-8")


@pytest.mark.asyncio
async def test_store_requires_its_exact_root_lock_for_every_public_operation(
    journal_store: PendingJournalStore,
    valid_journal: PendingJournal,
) -> None:
    delete = JournalDeleteExpectation(
        root_key=valid_journal.header.root_key,
        required_release_id=valid_journal.header.required_release_id,
        original_request_id=valid_journal.original.request_id,
        reconcile_attempt_request_id=None,
        revisions=JournalRevisions(original=0, reconcile_attempt=None),
    )
    for call in (
        journal_store.load,
        lambda: journal_store.save(valid_journal, expected_revisions=None),
        lambda: journal_store.delete(delete),
    ):
        with pytest.raises(BridgeError) as caught:
            call()
        assert _error_code(caught) is ErrorCode.STATE_CONFLICT


@pytest.mark.asyncio
async def test_store_rechecks_bound_lock_path_after_construction(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    tmp_path: Path,
) -> None:
    root_lock.path = tmp_path / "other.lock"
    async with root_lock:
        with pytest.raises(BridgeError) as caught:
            journal_store.load()
    assert _error_code(caught) is ErrorCode.STATE_CONFLICT


def test_store_constructor_rejects_wrong_lock_and_invalid_limits(
    file_bridge_paths: FileBridgePaths,
    manifest_catalog: ManifestCatalog,
    tmp_path: Path,
) -> None:
    wrong_lock = RootLock(tmp_path / "other.lock", timeout_seconds=0)
    with pytest.raises(BridgeError) as caught:
        PendingJournalStore(
            file_bridge_paths,
            wrong_lock,
            manifest_catalog,
            max_journal_bytes=100,
            replace_retry_seconds=0,
        )
    assert _error_code(caught) is ErrorCode.INVALID_ARGUMENT
    for limit in (0, -1, True, 1.5):
        with pytest.raises(BridgeError) as invalid:
            PendingJournalStore(
                file_bridge_paths,
                RootLock(file_bridge_paths.lock_file, timeout_seconds=0),
                manifest_catalog,
                max_journal_bytes=limit,  # type: ignore[arg-type]
                replace_retry_seconds=0,
            )
        assert _error_code(invalid) is ErrorCode.INVALID_ARGUMENT


@pytest.mark.asyncio
async def test_missing_journal_loads_none(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
) -> None:
    async with root_lock:
        assert journal_store.load() is None


@pytest.mark.asyncio
async def test_create_update_load_and_prepared_delete_are_dual_revision_cas(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    file_bridge_paths: FileBridgePaths,
) -> None:
    async with root_lock:
        assert journal_store.save(valid_journal, expected_revisions=None) == JournalRevisions(
            original=0, reconcile_attempt=None
        )
        header_bytes = _canonical(valid_journal.header.model_dump(mode="json"))
        assert header_bytes in file_bridge_paths.pending_file.read_bytes()
        loaded = journal_store.load()
        assert loaded is not None
        assert loaded.journal == valid_journal

        with pytest.raises(BridgeError) as duplicate:
            journal_store.save(valid_journal, expected_revisions=None)
        assert _error_code(duplicate) is ErrorCode.STATE_CONFLICT

        published = valid_journal.model_copy(
            update={
                "original": valid_journal.original.model_copy(
                    update={
                        "revision": 1,
                        "state": PendingPhase.PUBLISHED,
                        "updated_at_ms": 101,
                        "delivery_intents": (
                            valid_journal.original.delivery_intents[0].model_copy(
                                update={"published_at_ms": 101}
                            ),
                        ),
                    }
                )
            }
        )
        revisions = journal_store.save(
            published,
            expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
        )
        assert revisions == JournalRevisions(original=1, reconcile_attempt=None)
        old_bytes = file_bridge_paths.pending_file.read_bytes()
        assert header_bytes in old_bytes
        with pytest.raises(BridgeError) as stale:
            journal_store.save(
                published,
                expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
            )
        assert _error_code(stale) is ErrorCode.STATE_CONFLICT
        assert file_bridge_paths.pending_file.read_bytes() == old_bytes

    # A fresh prepared journal is the rollback-safe delete endpoint.
    file_bridge_paths.pending_file.unlink()
    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        journal_store.delete(
            JournalDeleteExpectation(
                root_key=valid_journal.header.root_key,
                required_release_id=valid_journal.header.required_release_id,
                original_request_id=valid_journal.original.request_id,
                reconcile_attempt_request_id=None,
                revisions=JournalRevisions(original=0, reconcile_attempt=None),
            )
        )
        assert not file_bridge_paths.pending_file.exists()


@pytest.mark.asyncio
async def test_foreign_release_gate_keeps_nested_poison_opaque_and_performs_zero_forbidden_calls(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    manifest_catalog: ManifestCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    foreign = "f" * 64
    raw = (
        '{"header":{"format":"cmo-agent-bridge/pending-journal","header_version":1,'
        f'"required_release_id":"{foreign}","root_key":"{file_bridge_paths.root_key}"}},'
        '"original":{"missing":true,"dup":1,"dup":2,"surrogate":"\\ud800"},'
        '"reconcile_attempt":null}'
    ).encode("utf-8")
    file_bridge_paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    file_bridge_paths.pending_file.write_bytes(raw)
    before_mtime = file_bridge_paths.pending_file.stat().st_mtime_ns
    resolve = Mock(side_effect=AssertionError("foreign release must not resolve a catalog binding"))
    connect = Mock(side_effect=AssertionError("foreign release must not open SQLite"))
    model_load = Mock(side_effect=AssertionError("foreign release must not build journal models"))
    revalidate = Mock(side_effect=AssertionError("foreign release must not validate exchange body"))
    replace = Mock(side_effect=AssertionError("foreign release must not rewrite journal"))
    monkeypatch.setattr(manifest_catalog, "resolve_running", resolve)
    monkeypatch.setattr(sqlite3, "connect", connect)
    monkeypatch.setattr(PendingJournal, "model_validate_json", model_load)
    monkeypatch.setattr(pending_journal_module, "revalidate_pending_exchange", revalidate)
    monkeypatch.setattr(pending_journal_module, "atomic_replace_bytes", replace)

    async with root_lock:
        with pytest.raises(BridgeError) as caught:
            journal_store.load()

    assert _error_code(caught) is ErrorCode.MANIFEST_MISMATCH
    assert caught.value.details == {
        "root_key": file_bridge_paths.root_key,
        "required_release_id": foreign,
        "running_release_id": manifest_catalog.running_release_id,
    }
    resolve.assert_not_called()
    connect.assert_not_called()
    model_load.assert_not_called()
    revalidate.assert_not_called()
    replace.assert_not_called()
    assert file_bridge_paths.pending_file.read_bytes() == raw
    assert file_bridge_paths.pending_file.stat().st_mtime_ns == before_mtime


@pytest.mark.asyncio
async def test_save_on_foreign_journal_classifies_header_before_candidate_or_replace(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    manifest_catalog: ManifestCatalog,
    valid_journal: PendingJournal,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    foreign = "f" * 64
    raw = (
        '{"header":{"format":"cmo-agent-bridge/pending-journal","header_version":1,'
        f'"required_release_id":"{foreign}","root_key":"{file_bridge_paths.root_key}"}},'
        '"original":{"poison":true,"dup":1,"dup":2},"reconcile_attempt":null}'
    ).encode("utf-8")
    file_bridge_paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    file_bridge_paths.pending_file.write_bytes(raw)
    before_mtime = file_bridge_paths.pending_file.stat().st_mtime_ns
    resolve = Mock(side_effect=AssertionError("foreign save must not resolve any release"))
    revalidate = Mock(side_effect=AssertionError("foreign save must not validate candidate/body"))
    replace = Mock(side_effect=AssertionError("foreign save must not replace journal"))
    monkeypatch.setattr(manifest_catalog, "resolve_running", resolve)
    monkeypatch.setattr(pending_journal_module, "revalidate_pending_exchange", revalidate)
    monkeypatch.setattr(pending_journal_module, "atomic_replace_bytes", replace)

    async with root_lock:
        with pytest.raises(BridgeError) as caught:
            journal_store.save(valid_journal, expected_revisions=None)

    assert _error_code(caught) is ErrorCode.MANIFEST_MISMATCH
    resolve.assert_not_called()
    revalidate.assert_not_called()
    replace.assert_not_called()
    assert file_bridge_paths.pending_file.read_bytes() == raw
    assert file_bridge_paths.pending_file.stat().st_mtime_ns == before_mtime


@pytest.mark.asyncio
async def test_foreign_release_with_wrong_root_is_journal_corrupt_before_release_gate(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    manifest_catalog: ManifestCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wrong_root = "0" * 64
    raw = _foreign_journal_bytes(wrong_root, original_json='{"opaque":true,"dup":1,"dup":2}')
    file_bridge_paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    file_bridge_paths.pending_file.write_bytes(raw)
    before_mtime = file_bridge_paths.pending_file.stat().st_mtime_ns
    resolve = Mock(side_effect=AssertionError("wrong-root journal must not reach release lookup"))
    model = Mock(side_effect=AssertionError("wrong-root journal must not build full models"))
    revalidate = Mock(side_effect=AssertionError("wrong-root journal must not inspect bodies"))
    monkeypatch.setattr(manifest_catalog, "resolve_running", resolve)
    monkeypatch.setattr(PendingJournal, "model_validate_json", model)
    monkeypatch.setattr(pending_journal_module, "revalidate_pending_exchange", revalidate)

    async with root_lock:
        with pytest.raises(BridgeError) as caught:
            journal_store.load()

    assert caught.value.to_payload() == {
        "code": ErrorCode.JOURNAL_CORRUPT.value,
        "message": "pending journal belongs to a different bridge root",
        "details": {},
    }
    resolve.assert_not_called()
    model.assert_not_called()
    revalidate.assert_not_called()
    assert file_bridge_paths.pending_file.read_bytes() == raw
    assert file_bridge_paths.pending_file.stat().st_mtime_ns == before_mtime


@pytest.mark.asyncio
async def test_foreign_header_with_unskippable_original_is_corrupt_before_release_gate(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    manifest_catalog: ManifestCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = (
        '{"header":{"format":"cmo-agent-bridge/pending-journal","header_version":1,'
        f'"required_release_id":"{FOREIGN_RELEASE_ID}",'
        f'"root_key":"{file_bridge_paths.root_key}"}},"original":[{{"open":true}}'
    ).encode("utf-8")
    file_bridge_paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    file_bridge_paths.pending_file.write_bytes(raw)
    before_mtime = file_bridge_paths.pending_file.stat().st_mtime_ns
    resolve = Mock(side_effect=AssertionError("unskippable JSON must not reach release lookup"))
    full_parse = Mock(side_effect=AssertionError("stage one must fail before full parsing"))
    monkeypatch.setattr(manifest_catalog, "resolve_running", resolve)
    monkeypatch.setattr(PendingJournal, "model_validate_json", full_parse)

    async with root_lock:
        with pytest.raises(BridgeError) as caught:
            journal_store.load()

    assert caught.value.to_payload() == {
        "code": ErrorCode.JOURNAL_CORRUPT.value,
        "message": "pending journal header or top-level JSON is corrupt",
        "details": {},
    }
    resolve.assert_not_called()
    full_parse.assert_not_called()
    assert file_bridge_paths.pending_file.read_bytes() == raw
    assert file_bridge_paths.pending_file.stat().st_mtime_ns == before_mtime


@pytest.mark.parametrize("operation", ["save", "delete"])
@pytest.mark.asyncio
async def test_foreign_save_and_delete_are_opaque_zero_call_read_only_gates(
    operation: str,
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    manifest_catalog: ManifestCatalog,
    valid_journal: PendingJournal,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    foreign = "f" * 64
    raw = _foreign_journal_bytes(
        file_bridge_paths.root_key,
        original_json='{"poison":true,"dup":1,"dup":2,"surrogate":"\\ud800"}',
    )
    file_bridge_paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    file_bridge_paths.pending_file.write_bytes(raw)
    file_bridge_paths.inbox.parent.mkdir(parents=True, exist_ok=True)
    file_bridge_paths.inbox.write_bytes(b"inbox-sentinel")
    file_bridge_paths.sqlite_file.parent.mkdir(parents=True, exist_ok=True)
    file_bridge_paths.sqlite_file.write_bytes(b"sqlite-sentinel")
    paths = (
        file_bridge_paths.pending_file,
        file_bridge_paths.inbox,
        file_bridge_paths.sqlite_file,
    )
    snapshots = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in paths}

    path_calls: list[tuple[str, Path]] = []
    real_open = Path.open
    real_read_bytes = Path.read_bytes
    real_write_bytes = Path.write_bytes
    real_stat = Path.stat

    def observed_open(
        path: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> IO[Any]:
        path_calls.append(("open", path))
        return real_open(path, mode, buffering, encoding, errors, newline)

    def observed_read_bytes(path: Path) -> bytes:
        path_calls.append(("read_bytes", path))
        return real_read_bytes(path)

    def observed_write_bytes(path: Path, data: bytes) -> int:
        path_calls.append(("write_bytes", path))
        return real_write_bytes(path, data)

    def observed_stat(path: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        path_calls.append(("stat", path))
        return real_stat(path, follow_symlinks=follow_symlinks)

    catalog = Mock(side_effect=AssertionError("foreign gate must not resolve a catalog binding"))
    registry = Mock(side_effect=AssertionError("foreign gate must not resolve a registry schema"))
    typed_json = Mock(side_effect=AssertionError("foreign gate must not build a journal model"))
    candidate_model = Mock(side_effect=AssertionError("foreign gate must not model a candidate"))
    body = Mock(side_effect=AssertionError("foreign gate must not reconstruct a request body"))
    sqlite_connect = Mock(side_effect=AssertionError("foreign gate must not open SQLite"))
    replace = Mock(side_effect=AssertionError("foreign gate must not rewrite the journal"))
    monkeypatch.setattr(manifest_catalog, "resolve_running", catalog)
    monkeypatch.setattr(OperationRegistry, "resolve_wire_invocation", registry)
    monkeypatch.setattr(PendingJournal, "model_validate_json", typed_json)
    monkeypatch.setattr(PendingJournal, "model_validate", candidate_model)
    monkeypatch.setattr(pending_journal_module, "revalidate_pending_exchange", body)
    monkeypatch.setattr(sqlite3, "connect", sqlite_connect)
    monkeypatch.setattr(pending_journal_module, "atomic_replace_bytes", replace)
    monkeypatch.setattr(Path, "open", observed_open)
    monkeypatch.setattr(Path, "read_bytes", observed_read_bytes)
    monkeypatch.setattr(Path, "write_bytes", observed_write_bytes)
    monkeypatch.setattr(Path, "stat", observed_stat)

    delete_expectation = JournalDeleteExpectation(
        root_key=valid_journal.header.root_key,
        required_release_id=valid_journal.header.required_release_id,
        original_request_id=valid_journal.original.request_id,
        reconcile_attempt_request_id=None,
        revisions=JournalRevisions(original=0, reconcile_attempt=None),
    )
    async with root_lock:
        with pytest.raises(BridgeError) as caught:
            if operation == "save":
                journal_store.save(valid_journal, expected_revisions=None)
            else:
                journal_store.delete(delete_expectation)

    # Restore Path I/O before the postcondition reads so only the operation interval is counted.
    monkeypatch.undo()

    assert caught.value.to_payload() == {
        "code": ErrorCode.MANIFEST_MISMATCH.value,
        "message": "pending journal requires a different bridge release",
        "details": {
            "root_key": file_bridge_paths.root_key,
            "required_release_id": foreign,
            "running_release_id": manifest_catalog.running_release_id,
        },
    }
    for forbidden in (
        catalog,
        registry,
        typed_json,
        candidate_model,
        body,
        sqlite_connect,
        replace,
    ):
        forbidden.assert_not_called()
    assert [call for call in path_calls if call[1] == file_bridge_paths.inbox] == []
    for path, snapshot in snapshots.items():
        assert (path.read_bytes(), path.stat().st_mtime_ns) == snapshot


@pytest.mark.parametrize(
    "raw",
    [
        b"{broken",
        b'{"header":{},"header":{},"original":{},"reconcile_attempt":null}',
        b'{"header":{"format":"cmo-agent-bridge/pending-journal","header_version":true,'
        b'"root_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"required_release_id":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},'
        b'"original":{},"reconcile_attempt":null}',
        b'{"header":{},"original":{},"reconcile_attempt":null,"extra":0}',
        b"\xff",
    ],
)
@pytest.mark.asyncio
async def test_stage_one_corruption_precedes_foreign_release(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    raw: bytes,
) -> None:
    file_bridge_paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    file_bridge_paths.pending_file.write_bytes(raw)
    async with root_lock:
        with pytest.raises(BridgeError) as caught:
            journal_store.load()
    assert _error_code(caught) is ErrorCode.JOURNAL_CORRUPT


@pytest.mark.asyncio
async def test_unreadable_journal_is_corrupt_without_replacement(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    file_bridge_paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    file_bridge_paths.pending_file.write_bytes(b"sentinel")
    real_open = Path.open

    def fail_pending_open(
        path: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> IO[Any]:
        if path == file_bridge_paths.pending_file:
            raise PermissionError("denied")
        return real_open(path, mode, buffering, encoding, errors, newline)

    async with root_lock:
        monkeypatch.setattr(Path, "open", fail_pending_open)
        with pytest.raises(BridgeError) as caught:
            journal_store.load()
    assert _error_code(caught) is ErrorCode.JOURNAL_CORRUPT


@pytest.mark.asyncio
async def test_exact_release_nested_duplicate_is_journal_corrupt(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    manifest_catalog: ManifestCatalog,
) -> None:
    raw = (
        '{"header":{"format":"cmo-agent-bridge/pending-journal","header_version":1,'
        f'"required_release_id":"{manifest_catalog.running_release_id}",'
        f'"root_key":"{file_bridge_paths.root_key}"}},'
        '"original":{"dup":1,"dup":2},"reconcile_attempt":null}'
    ).encode("utf-8")
    file_bridge_paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    file_bridge_paths.pending_file.write_bytes(raw)
    async with root_lock:
        with pytest.raises(BridgeError) as caught:
            journal_store.load()
    assert _error_code(caught) is ErrorCode.JOURNAL_CORRUPT


@pytest.mark.parametrize(
    "uuid_text",
    [
        str(UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")).upper(),
        UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee").hex,
    ],
)
@pytest.mark.asyncio
async def test_exact_release_rejects_noncanonical_typed_uuid_text_without_rewrite(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    file_bridge_paths: FileBridgePaths,
    uuid_text: str,
) -> None:
    canonical_uuid = str(valid_journal.original.request_id)
    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        raw = file_bridge_paths.pending_file.read_bytes()
        poisoned = raw.replace(
            f'"request_id":"{canonical_uuid}"'.encode(),
            f'"request_id":"{uuid_text}"'.encode(),
        )
        file_bridge_paths.pending_file.write_bytes(poisoned)
        with pytest.raises(BridgeError) as caught:
            journal_store.load()
    assert _error_code(caught) is ErrorCode.JOURNAL_CORRUPT
    assert file_bridge_paths.pending_file.read_bytes() == poisoned


@pytest.mark.asyncio
async def test_exact_release_load_rebuilds_frozen_invocation_and_expectation(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    file_bridge_paths: FileBridgePaths,
) -> None:
    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        loaded = journal_store.load()
    assert loaded is not None
    assert type(loaded.original.invocation) is FrozenInvocation
    assert loaded.original.expectation.status_bootstrap is False
    assert loaded.original.expectation.activation_candidate is None
    raw = file_bridge_paths.pending_file.read_bytes()
    assert b"status_bootstrap" not in raw
    assert b"activation_candidate" not in raw
    assert b"invocation" not in raw
    assert b"expectation" not in raw


@pytest.mark.asyncio
async def test_inconsistent_exact_running_binding_is_manifest_mismatch_before_body_reconstruction(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    manifest_catalog: ManifestCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        running = cast(ReleaseBinding, object.__getattribute__(manifest_catalog, "_running"))
        object.__setattr__(running.snapshot, "runtime_tag", "forged")
        body_revalidation = Mock(
            side_effect=AssertionError("inconsistent binding must fail before body reconstruction")
        )
        monkeypatch.setattr(
            pending_journal_module,
            "revalidate_pending_exchange",
            body_revalidation,
        )
        with pytest.raises(BridgeError) as caught:
            journal_store.load()
    assert _error_code(caught) is ErrorCode.MANIFEST_MISMATCH
    body_revalidation.assert_not_called()


@pytest.mark.asyncio
async def test_save_revalidates_model_copy_semantics_before_replacing(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    file_bridge_paths: FileBridgePaths,
) -> None:
    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        old_bytes = file_bridge_paths.pending_file.read_bytes()
        forged = valid_journal.model_copy(
            update={
                "original": valid_journal.original.model_copy(
                    update={"operation": "unit.set", "revision": 1, "updated_at_ms": 101}
                )
            }
        )
        with pytest.raises(BridgeError) as caught:
            journal_store.save(
                forged,
                expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
            )
    assert _error_code(caught) is ErrorCode.STATE_CONFLICT
    assert file_bridge_paths.pending_file.read_bytes() == old_bytes


@pytest.mark.asyncio
async def test_save_and_reload_semantically_revalidate_accepted_artifact(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    request = valid_journal.original.delivery_intents[0].model_copy(update={"published_at_ms": 101})
    published = valid_journal.model_copy(
        update={
            "original": valid_journal.original.model_copy(
                update={
                    "delivery_intents": (request,),
                    "revision": 1,
                    "state": PendingPhase.PUBLISHED,
                    "updated_at_ms": 101,
                }
            )
        }
    )
    accepted = published.model_copy(
        update={
            "original": published.original.model_copy(
                update={
                    "response_artifact": completed_artifact,
                    "settlement": completed_artifact.accepted_response.settlement,
                    "revision": 2,
                    "state": PendingPhase.RESPONSE_ACCEPTED,
                    "updated_at_ms": 200,
                }
            )
        }
    )
    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        journal_store.save(
            published,
            expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
        )
        journal_store.save(
            accepted,
            expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
        )
        loaded = journal_store.load()
    assert loaded is not None
    assert loaded.journal.original.response_artifact == completed_artifact


@pytest.mark.asyncio
async def test_stage_one_is_bounded_iterative_and_accepts_header_last_with_escaped_key(
    file_bridge_paths: FileBridgePaths,
    root_lock: RootLock,
    manifest_catalog: ManifestCatalog,
) -> None:
    max_bytes = 20_000
    store = PendingJournalStore(
        file_bridge_paths,
        root_lock,
        manifest_catalog,
        max_journal_bytes=max_bytes,
        replace_retry_seconds=0,
    )
    foreign = "f" * 64
    nested = "[" * 2_000 + "null" + "]" * 2_000
    raw = (
        '{"original":' + nested + ',"reconcile_attempt":null,"he\\u0061der":{'
        '"format":"cmo-agent-bridge/pending-journal","header_version":1,'
        f'"required_release_id":"{foreign}","root_key":"{file_bridge_paths.root_key}"}}}}'
    ).encode("utf-8")
    assert len(raw) < max_bytes
    file_bridge_paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    file_bridge_paths.pending_file.write_bytes(raw)
    async with root_lock:
        with pytest.raises(BridgeError) as caught:
            store.load()
    assert _error_code(caught) is ErrorCode.MANIFEST_MISMATCH

    file_bridge_paths.pending_file.write_bytes(b"x" * (max_bytes + 1))
    async with root_lock:
        with pytest.raises(BridgeError) as oversized:
            store.load()
    assert _error_code(oversized) is ErrorCode.JOURNAL_CORRUPT


def _published_journal(valid_journal: PendingJournal) -> PendingJournal:
    request = valid_journal.original.delivery_intents[0].model_copy(update={"published_at_ms": 101})
    return valid_journal.model_copy(
        update={
            "original": valid_journal.original.model_copy(
                update={
                    "delivery_intents": (request,),
                    "revision": 1,
                    "state": PendingPhase.PUBLISHED,
                    "updated_at_ms": 101,
                }
            )
        }
    )


def _quarantined_journal(valid_journal: PendingJournal) -> PendingJournal:
    published = _published_journal(valid_journal)
    return published.model_copy(
        update={
            "original": published.original.model_copy(
                update={
                    "revision": 2,
                    "state": PendingPhase.QUARANTINED,
                    "updated_at_ms": 200,
                }
            )
        }
    )


def _accepted_idle_journals(
    valid_journal: PendingJournal,
    artifact: ResponseArtifact,
) -> tuple[PendingJournal, PendingJournal, PendingJournal]:
    published = _published_journal(valid_journal)
    accepted = published.model_copy(
        update={
            "original": published.original.model_copy(
                update={
                    "response_artifact": artifact,
                    "settlement": artifact.accepted_response.settlement,
                    "revision": 2,
                    "state": PendingPhase.RESPONSE_ACCEPTED,
                    "updated_at_ms": 200,
                }
            )
        }
    )
    idle = accepted.model_copy(
        update={
            "original": accepted.original.model_copy(
                update={
                    "revision": 3,
                    "state": PendingPhase.IDLE_PUBLISHED,
                    "updated_at_ms": 201,
                }
            )
        }
    )
    return published, accepted, idle


@pytest.mark.asyncio
async def test_idle_published_can_advance_once_to_quarantined_barrier_journal_first(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
    file_bridge_paths: FileBridgePaths,
) -> None:
    published, accepted, idle = _accepted_idle_journals(
        valid_journal,
        completed_artifact,
    )
    quarantined = idle.model_copy(
        update={
            "original": idle.original.model_copy(
                update={
                    "revision": 4,
                    "state": PendingPhase.QUARANTINED,
                    "updated_at_ms": 202,
                }
            )
        }
    )

    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        journal_store.save(
            published,
            expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
        )
        journal_store.save(
            accepted,
            expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
        )
        journal_store.save(
            idle,
            expected_revisions=JournalRevisions(original=2, reconcile_attempt=None),
        )
        assert journal_store.save(
            quarantined,
            expected_revisions=JournalRevisions(original=3, reconcile_attempt=None),
        ) == JournalRevisions(original=4, reconcile_attempt=None)

        loaded = journal_store.load()
        assert loaded is not None
        assert loaded.journal == quarantined
        assert loaded.journal.original.response_artifact == (idle.original.response_artifact)
        assert loaded.journal.original.settlement == idle.original.settlement
        assert loaded.journal.original.delivery_intents == idle.original.delivery_intents
        assert loaded.journal.original.request_id == idle.original.request_id
        assert loaded.journal.original.request_hash == idle.original.request_hash
        assert loaded.journal.original.runtime_snapshot == idle.original.runtime_snapshot
        barrier_bytes = file_bridge_paths.pending_file.read_bytes()
        with pytest.raises(BridgeError) as deletion:
            journal_store.delete(
                JournalDeleteExpectation(
                    root_key=quarantined.header.root_key,
                    required_release_id=quarantined.header.required_release_id,
                    original_request_id=quarantined.original.request_id,
                    reconcile_attempt_request_id=None,
                    revisions=JournalRevisions(original=4, reconcile_attempt=None),
                )
            )
        assert _error_code(deletion) is ErrorCode.STATE_CONFLICT
        assert file_bridge_paths.pending_file.read_bytes() == barrier_bytes


@pytest.mark.asyncio
async def test_idle_quarantine_rejects_other_phases_and_all_durable_evidence_drift(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
    file_bridge_paths: FileBridgePaths,
) -> None:
    published, accepted, idle = _accepted_idle_journals(
        valid_journal,
        completed_artifact,
    )
    base_changes = {
        "revision": 4,
        "state": PendingPhase.QUARANTINED,
        "updated_at_ms": 202,
    }
    request = idle.original.delivery_intents[0]
    drift_cases = (
        (
            "artifact",
            idle.model_copy(
                update={
                    "original": idle.original.model_copy(
                        update={
                            **base_changes,
                            "response_artifact": completed_artifact.model_copy(
                                update={"sha256": "0" * 64}
                            ),
                        }
                    )
                }
            ),
        ),
        (
            "settlement",
            idle.model_copy(
                update={
                    "original": idle.original.model_copy(
                        update={
                            **base_changes,
                            "settlement": CancelledSettlement(state="cancelled"),
                        }
                    )
                }
            ),
        ),
        (
            "delivery identity",
            idle.model_copy(
                update={
                    "original": idle.original.model_copy(
                        update={
                            **base_changes,
                            "delivery_intents": (
                                request.model_copy(
                                    update={
                                        "rendered_inbox_size_bytes": (
                                            request.rendered_inbox_size_bytes + 1
                                        )
                                    }
                                ),
                            ),
                        }
                    )
                }
            ),
        ),
        (
            "exchange identity",
            idle.model_copy(
                update={
                    "original": idle.original.model_copy(
                        update={**base_changes, "operation": "unit.delete"}
                    )
                }
            ),
        ),
    )
    other_phase_cases = tuple(
        (
            phase.value,
            idle.model_copy(
                update={
                    "original": idle.original.model_copy(
                        update={
                            "revision": 4,
                            "state": phase,
                            "updated_at_ms": 202,
                        }
                    )
                }
            ),
        )
        for phase in PendingPhase
        if phase is not PendingPhase.QUARANTINED
    )
    quarantined = idle.model_copy(
        update={"original": idle.original.model_copy(update=base_changes)}
    )

    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        journal_store.save(
            published,
            expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
        )
        journal_store.save(
            accepted,
            expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
        )
        journal_store.save(
            idle,
            expected_revisions=JournalRevisions(original=2, reconcile_attempt=None),
        )
        idle_bytes = file_bridge_paths.pending_file.read_bytes()
        for label, candidate in (*drift_cases, *other_phase_cases):
            with pytest.raises(BridgeError) as caught:
                journal_store.save(
                    candidate,
                    expected_revisions=JournalRevisions(
                        original=3,
                        reconcile_attempt=None,
                    ),
                )
            assert _error_code(caught) is ErrorCode.STATE_CONFLICT, label
            assert file_bridge_paths.pending_file.read_bytes() == idle_bytes, label

        journal_store.save(
            quarantined,
            expected_revisions=JournalRevisions(original=3, reconcile_attempt=None),
        )
        barrier_bytes = file_bridge_paths.pending_file.read_bytes()
        rollback = quarantined.model_copy(
            update={
                "original": quarantined.original.model_copy(
                    update={
                        "revision": 5,
                        "state": PendingPhase.IDLE_PUBLISHED,
                        "updated_at_ms": 203,
                    }
                )
            }
        )
        with pytest.raises(BridgeError) as caught:
            journal_store.save(
                rollback,
                expected_revisions=JournalRevisions(
                    original=4,
                    reconcile_attempt=None,
                ),
            )
        assert _error_code(caught) is ErrorCode.STATE_CONFLICT
        assert file_bridge_paths.pending_file.read_bytes() == barrier_bytes


@pytest.mark.asyncio
async def test_attempt_creation_freezes_original_and_only_attempt_revision_advances(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    reconcile_attempt: PendingExchange,
    file_bridge_paths: FileBridgePaths,
) -> None:
    attempt = reconcile_attempt
    published = _published_journal(valid_journal)
    quarantined = _quarantined_journal(valid_journal)
    with_attempt = quarantined.model_copy(update={"reconcile_attempt": attempt})
    published_attempt = with_attempt.model_copy(
        update={
            "reconcile_attempt": attempt.model_copy(
                update={
                    "delivery_intents": (
                        attempt.delivery_intents[0].model_copy(update={"published_at_ms": 301}),
                    ),
                    "revision": 1,
                    "state": PendingPhase.PUBLISHED,
                    "updated_at_ms": 301,
                }
            )
        }
    )
    original_bytes = _canonical(quarantined.original.model_dump(mode="json"))
    persisted_attempt = published_attempt.reconcile_attempt
    assert persisted_attempt is not None

    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        journal_store.save(
            published, expected_revisions=JournalRevisions(original=0, reconcile_attempt=None)
        )
        journal_store.save(
            quarantined, expected_revisions=JournalRevisions(original=1, reconcile_attempt=None)
        )
        assert journal_store.save(
            with_attempt,
            expected_revisions=JournalRevisions(original=2, reconcile_attempt=None),
        ) == JournalRevisions(original=2, reconcile_attempt=0)
        assert journal_store.save(
            published_attempt,
            expected_revisions=JournalRevisions(original=2, reconcile_attempt=0),
        ) == JournalRevisions(original=2, reconcile_attempt=1)
        loaded = journal_store.load()
        assert loaded is not None
        assert _canonical(loaded.journal.original.model_dump(mode="json")) == original_bytes
        old_bytes = file_bridge_paths.pending_file.read_bytes()
        for invalid in (
            published_attempt.model_copy(update={"reconcile_attempt": None}),
            published_attempt.model_copy(
                update={
                    "original": published_attempt.original.model_copy(
                        update={"updated_at_ms": 201}
                    ),
                    "reconcile_attempt": persisted_attempt.model_copy(
                        update={"revision": 2, "updated_at_ms": 302}
                    ),
                }
            ),
            published_attempt.model_copy(
                update={
                    "reconcile_attempt": attempt.model_copy(
                        update={"revision": 2, "updated_at_ms": 302}
                    )
                }
            ),
        ):
            with pytest.raises(BridgeError) as caught:
                journal_store.save(
                    invalid,
                    expected_revisions=JournalRevisions(original=2, reconcile_attempt=1),
                )
            assert _error_code(caught) is ErrorCode.STATE_CONFLICT
            assert file_bridge_paths.pending_file.read_bytes() == old_bytes


@pytest.mark.asyncio
async def test_model_construct_candidate_and_replace_failure_preserve_old_bytes(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    file_bridge_paths: FileBridgePaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = _published_journal(valid_journal)
    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        old_bytes = file_bridge_paths.pending_file.read_bytes()
        forged_original = PendingExchange.model_construct(operation="unit.set")
        forged = PendingJournal.model_construct(
            header=published.header,
            original=forged_original,
            reconcile_attempt=None,
        )
        with pytest.raises(BridgeError) as invalid:
            journal_store.save(
                forged,
                expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
            )
        assert _error_code(invalid) is ErrorCode.STATE_CONFLICT
        assert file_bridge_paths.pending_file.read_bytes() == old_bytes

        failure = OSError("injected replace failure")
        replace = Mock(side_effect=failure)
        monkeypatch.setattr(pending_journal_module, "atomic_replace_bytes", replace)
        with pytest.raises(OSError) as caught:
            journal_store.save(
                published,
                expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
            )
        assert caught.value is failure
        assert file_bridge_paths.pending_file.read_bytes() == old_bytes


@pytest.mark.asyncio
async def test_cancel_append_must_enter_cancel_phase_before_quarantine(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    file_bridge_paths: FileBridgePaths,
) -> None:
    published = _published_journal(valid_journal)
    request = published.original.delivery_intents[0]
    cancel = DeliveryIntent.model_validate(
        {
            **request.model_dump(mode="python"),
            "delivery_id": UUID("22222222-2222-4222-8222-222222222222"),
            "delivery_kind": "cancel",
            "original_request_delivery_id": request.delivery_id,
            "intended_at_ms": 150,
            "published_at_ms": None,
        }
    )
    invalid = published.model_copy(
        update={
            "original": published.original.model_copy(
                update={
                    "delivery_intents": (request, cancel),
                    "revision": 2,
                    "state": PendingPhase.QUARANTINED,
                    "updated_at_ms": 150,
                }
            )
        }
    )
    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        journal_store.save(
            published,
            expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
        )
        old_bytes = file_bridge_paths.pending_file.read_bytes()
        with pytest.raises(BridgeError) as caught:
            journal_store.save(
                invalid,
                expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
            )
    assert _error_code(caught) is ErrorCode.STATE_CONFLICT
    assert file_bridge_paths.pending_file.read_bytes() == old_bytes


@pytest.mark.asyncio
async def test_pre_attempt_identity_publication_and_phase_drift_preserve_old_bytes(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    file_bridge_paths: FileBridgePaths,
) -> None:
    published = _published_journal(valid_journal)
    request = published.original.delivery_intents[0]
    candidates = (
        published.model_copy(
            update={
                "original": published.original.model_copy(
                    update={
                        "delivery_intents": (
                            request.model_copy(update={"rendered_inbox_sha256": "0" * 64}),
                        ),
                        "revision": 2,
                        "state": PendingPhase.QUARANTINED,
                        "updated_at_ms": 150,
                    }
                )
            }
        ),
        published.model_copy(
            update={
                "original": published.original.model_copy(
                    update={
                        "delivery_intents": (request.model_copy(update={"published_at_ms": 102}),),
                        "revision": 2,
                        "state": PendingPhase.QUARANTINED,
                        "updated_at_ms": 150,
                    }
                )
            }
        ),
        published.model_copy(
            update={
                "original": published.original.model_copy(
                    update={
                        "delivery_intents": (request.model_copy(update={"published_at_ms": None}),),
                        "revision": 2,
                        "state": PendingPhase.PREPARED,
                        "updated_at_ms": 150,
                    }
                )
            }
        ),
        published.model_copy(
            update={
                "header": published.header.model_copy(update={"root_key": "f" * 64}),
                "original": published.original.model_copy(
                    update={
                        "revision": 2,
                        "state": PendingPhase.QUARANTINED,
                        "updated_at_ms": 150,
                    }
                ),
            }
        ),
    )
    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        journal_store.save(
            published,
            expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
        )
        old_bytes = file_bridge_paths.pending_file.read_bytes()
        for candidate in candidates:
            with pytest.raises(BridgeError) as caught:
                journal_store.save(
                    candidate,
                    expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
                )
            assert _error_code(caught) is ErrorCode.STATE_CONFLICT
            assert file_bridge_paths.pending_file.read_bytes() == old_bytes


@pytest.mark.asyncio
async def test_artifact_replacement_and_no_settlement_idle_are_state_conflicts(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
    no_settlement_artifact: ResponseArtifact,
    file_bridge_paths: FileBridgePaths,
) -> None:
    published = _published_journal(valid_journal)
    accepted = published.model_copy(
        update={
            "original": published.original.model_copy(
                update={
                    "response_artifact": completed_artifact,
                    "settlement": completed_artifact.accepted_response.settlement,
                    "revision": 2,
                    "state": PendingPhase.RESPONSE_ACCEPTED,
                    "updated_at_ms": 200,
                }
            )
        }
    )
    replaced = accepted.model_copy(
        update={
            "original": accepted.original.model_copy(
                update={
                    "response_artifact": completed_artifact.model_copy(update={"sha256": "0" * 64}),
                    "revision": 3,
                    "state": PendingPhase.QUARANTINED,
                    "updated_at_ms": 201,
                }
            )
        }
    )
    unsafe_response = published.model_copy(
        update={
            "original": published.original.model_copy(
                update={
                    "response_artifact": no_settlement_artifact,
                    "settlement": None,
                    "revision": 2,
                    "state": PendingPhase.RESPONSE_ACCEPTED,
                    "updated_at_ms": 201,
                }
            )
        }
    )
    unsafe_idle = unsafe_response.model_copy(
        update={
            "original": unsafe_response.original.model_copy(
                update={
                    "revision": 3,
                    "state": PendingPhase.IDLE_PUBLISHED,
                    "updated_at_ms": 202,
                }
            )
        }
    )
    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        journal_store.save(
            published,
            expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
        )
        journal_store.save(
            accepted,
            expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
        )
        old_bytes = file_bridge_paths.pending_file.read_bytes()
        with pytest.raises(BridgeError) as replacement:
            journal_store.save(
                replaced,
                expected_revisions=JournalRevisions(original=2, reconcile_attempt=None),
            )
        assert _error_code(replacement) is ErrorCode.STATE_CONFLICT
        assert file_bridge_paths.pending_file.read_bytes() == old_bytes

    file_bridge_paths.pending_file.unlink()
    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        journal_store.save(
            published,
            expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
        )
        journal_store.save(
            unsafe_response,
            expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
        )
        old_bytes = file_bridge_paths.pending_file.read_bytes()
        with pytest.raises(BridgeError) as idle:
            journal_store.save(
                unsafe_idle,
                expected_revisions=JournalRevisions(original=2, reconcile_attempt=None),
            )
        assert _error_code(idle) is ErrorCode.STATE_CONFLICT
        assert file_bridge_paths.pending_file.read_bytes() == old_bytes


def _remove_journal_not_started_evidence(tree: dict[str, object]) -> None:
    envelope = cast(dict[str, object], tree["envelope"])
    envelope_error = cast(dict[str, object], envelope["error"])
    envelope_error["mutation_not_started"] = None
    settlement = cast(dict[str, object], tree["settlement"])
    settlement_error = cast(dict[str, object], settlement["error"])
    settlement_error["mutation_not_started"] = None


@pytest.mark.asyncio
async def test_journal_revalidation_rejects_removed_eligible_not_started_evidence(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    valid_journal: PendingJournal,
    semantic_harness: SemanticHarness,
) -> None:
    exchange = valid_journal.original
    artifact = semantic_harness.artifact_from_parser(
        exchange,
        semantic_harness.response_envelope(
            exchange,
            delivery_id=exchange.delivery_intents[0].delivery_id,
            result=None,
            error=semantic_harness.not_started_error(exchange),
        ),
        digest_character="1",
        accepted_at_ms=200,
    )
    assert isinstance(artifact.accepted_response.settlement, RejectedSettlement)
    poisoned = semantic_harness.poisoned_artifact(
        artifact,
        _remove_journal_not_started_evidence,
    )
    await semantic_harness.assert_original_journal_positive_and_negative(
        store=journal_store,
        root_lock=root_lock,
        paths=file_bridge_paths,
        header_source=valid_journal,
        positive=semantic_harness.accepted_exchange(exchange, artifact),
        negative=semantic_harness.accepted_exchange(exchange, poisoned),
    )


@pytest.mark.asyncio
async def test_journal_revalidation_rebuilds_dynamic_operation_as_concrete_mutation(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    valid_journal: PendingJournal,
    semantic_harness: SemanticHarness,
) -> None:
    exchange = semantic_harness.make_exchange(
        valid_journal.original,
        request_id=semantic_harness.dynamic_request_id,
        delivery_id=semantic_harness.dynamic_delivery_id,
        operation="compat.probe.step",
        arguments=cast(dict[str, JsonValue], {"step": "dedupe"}),
        intended_at_ms=400,
    )
    assert exchange.effective_class is OperationClass.MUTATION
    artifact = semantic_harness.artifact_from_parser(
        exchange,
        semantic_harness.response_envelope(
            exchange,
            delivery_id=semantic_harness.dynamic_delivery_id,
            result=None,
            error=semantic_harness.not_started_error(exchange),
        ),
        digest_character="2",
        accepted_at_ms=500,
    )
    positive = semantic_harness.accepted_exchange(exchange, artifact)
    negative = positive.model_copy(update={"effective_class": OperationClass.DESTRUCTIVE})
    await semantic_harness.assert_original_journal_positive_and_negative(
        store=journal_store,
        root_lock=root_lock,
        paths=file_bridge_paths,
        header_source=valid_journal,
        positive=positive,
        negative=negative,
    )


def _replace_journal_completed_cancel_result(tree: dict[str, object]) -> None:
    invalid_result = cast(JsonValue, {"unit_guid": "UNIT-ONLY"})
    envelope = cast(dict[str, object], tree["envelope"])
    envelope_ack = cast(dict[str, object], envelope["result"])
    envelope_ack["result"] = invalid_result
    cancel_ack = cast(dict[str, object], tree["cancel_ack"])
    cancel_ack["result"] = invalid_result
    settlement = cast(dict[str, object], tree["settlement"])
    settlement["result"] = invalid_result


@pytest.mark.asyncio
async def test_journal_revalidation_rejects_completed_cancel_recovery_schema_drift(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    valid_journal: PendingJournal,
    semantic_harness: SemanticHarness,
) -> None:
    exchange = semantic_harness.cancel_wait_exchange(valid_journal.original)
    result = cast(
        JsonValue,
        {
            "unit_guid": "UNIT-1",
            "name": "Test unit",
            "side_guid": "SIDE-1",
            "dbid": 1,
            "latitude": 1.5,
            "longitude": 2.5,
        },
    )
    acknowledgement = cast(
        JsonValue,
        {
            "request_id": str(exchange.request_id),
            "request_hash": exchange.request_hash,
            "original_delivery_id": str(exchange.delivery_intents[0].delivery_id),
            "status": "completed",
            "result": result,
        },
    )
    artifact = semantic_harness.artifact_from_parser(
        exchange,
        semantic_harness.response_envelope(
            exchange,
            delivery_id=semantic_harness.cancel_delivery_id,
            result=acknowledgement,
            error=None,
        ),
        digest_character="3",
        accepted_at_ms=210,
    )
    assert isinstance(artifact.accepted_response.settlement, CompletedSettlement)
    poisoned = semantic_harness.poisoned_artifact(
        artifact,
        _replace_journal_completed_cancel_result,
    )
    await semantic_harness.assert_original_journal_positive_and_negative(
        store=journal_store,
        root_lock=root_lock,
        paths=file_bridge_paths,
        header_source=valid_journal,
        positive=semantic_harness.accepted_exchange(exchange, artifact),
        negative=semantic_harness.accepted_exchange(exchange, poisoned),
    )


@pytest.mark.asyncio
async def test_journal_revalidation_round_trips_cancelled_settlement(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    valid_journal: PendingJournal,
    semantic_harness: SemanticHarness,
) -> None:
    exchange = semantic_harness.cancel_wait_exchange(valid_journal.original)
    acknowledgement = cast(
        JsonValue,
        {
            "request_id": str(exchange.request_id),
            "request_hash": exchange.request_hash,
            "original_delivery_id": str(exchange.delivery_intents[0].delivery_id),
            "status": "cancelled",
            "result": None,
        },
    )
    artifact = semantic_harness.artifact_from_parser(
        exchange,
        semantic_harness.response_envelope(
            exchange,
            delivery_id=semantic_harness.cancel_delivery_id,
            result=acknowledgement,
            error=None,
        ),
        digest_character="9",
        accepted_at_ms=211,
    )
    assert isinstance(artifact.accepted_response.settlement, CancelledSettlement)
    accepted = semantic_harness.accepted_exchange(exchange, artifact)
    journal = PendingJournal(
        header=valid_journal.header,
        original=accepted,
        reconcile_attempt=None,
    )
    file_bridge_paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    async with root_lock:
        file_bridge_paths.pending_file.write_bytes(
            semantic_harness.canonical(journal.model_dump(mode="json"))
        )
        loaded = journal_store.load()
    assert loaded is not None
    assert isinstance(loaded.journal.original.settlement, CancelledSettlement)


def _replace_journal_reconcile_disposition(tree: dict[str, object]) -> None:
    envelope = cast(dict[str, object], tree["envelope"])
    envelope_result = cast(dict[str, object], envelope["result"])
    envelope_result["disposition"] = "not_applied"
    settlement = cast(dict[str, object], tree["settlement"])
    settlement_result = cast(dict[str, object], settlement["result"])
    settlement_result["disposition"] = "not_applied"


@pytest.mark.asyncio
async def test_journal_revalidation_rejects_reconcile_target_disposition_drift(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    valid_journal: PendingJournal,
    reconcile_attempt: PendingExchange,
    semantic_harness: SemanticHarness,
) -> None:
    target_id = reconcile_attempt.original_target_request_id
    target_hash = reconcile_attempt.original_target_request_hash
    assert target_id is not None
    assert target_hash is not None
    result = cast(
        JsonValue,
        {
            "request_id": str(target_id),
            "request_hash": target_hash,
            "disposition": "applied",
            "resolved": True,
        },
    )
    artifact = semantic_harness.artifact_from_parser(
        reconcile_attempt,
        semantic_harness.response_envelope(
            reconcile_attempt,
            delivery_id=reconcile_attempt.delivery_intents[0].delivery_id,
            result=result,
            error=None,
        ),
        digest_character="4",
        accepted_at_ms=410,
    )
    poisoned = semantic_harness.poisoned_artifact(
        artifact,
        _replace_journal_reconcile_disposition,
    )
    original_request = valid_journal.original.delivery_intents[0].model_copy(
        update={"published_at_ms": 101}
    )
    quarantined_original = PendingExchange.model_validate(
        {
            **valid_journal.original.model_dump(mode="python"),
            "delivery_intents": (original_request,),
            "revision": 2,
            "state": PendingPhase.QUARANTINED,
            "updated_at_ms": 200,
        }
    )
    positive_journal = PendingJournal(
        header=valid_journal.header,
        original=quarantined_original,
        reconcile_attempt=semantic_harness.accepted_exchange(reconcile_attempt, artifact),
    )
    negative_journal = PendingJournal(
        header=valid_journal.header,
        original=quarantined_original,
        reconcile_attempt=semantic_harness.accepted_exchange(reconcile_attempt, poisoned),
    )
    file_bridge_paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
    async with root_lock:
        file_bridge_paths.pending_file.write_bytes(
            semantic_harness.canonical(positive_journal.model_dump(mode="json"))
        )
        loaded = journal_store.load()
        assert loaded is not None
        assert loaded.journal == positive_journal

        file_bridge_paths.pending_file.write_bytes(
            semantic_harness.canonical(negative_journal.model_dump(mode="json"))
        )
        with pytest.raises(BridgeError) as caught:
            journal_store.load()
    assert caught.value.code is ErrorCode.JOURNAL_CORRUPT


def _add_journal_forbidden_manifest_mismatch_settlement(tree: dict[str, object]) -> None:
    envelope = cast(dict[str, object], tree["envelope"])
    tree["settlement"] = {"state": "rejected", "error": envelope["error"]}


@pytest.mark.asyncio
async def test_journal_revalidation_requires_no_manifest_mismatch_settlement(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    valid_journal: PendingJournal,
    semantic_harness: SemanticHarness,
) -> None:
    exchange = valid_journal.original
    reported = RuntimeSnapshot.create(
        runtime_version="0.2.0",
        runtime_asset_sha256="5" * 64,
        operation_manifest_sha256="6" * 64,
        host_contract_sha256="7" * 64,
        dependency_lock_sha256="8" * 64,
    )
    artifact = semantic_harness.artifact_from_parser(
        exchange,
        semantic_harness.response_envelope(
            exchange,
            delivery_id=exchange.delivery_intents[0].delivery_id,
            result=None,
            error=semantic_harness.not_started_error(
                exchange,
                code=ErrorCode.MANIFEST_MISMATCH,
            ),
            reported_snapshot=reported,
        ),
        digest_character="5",
        accepted_at_ms=220,
    )
    assert artifact.accepted_response.settlement is None
    poisoned = semantic_harness.poisoned_artifact(
        artifact,
        _add_journal_forbidden_manifest_mismatch_settlement,
    )
    await semantic_harness.assert_original_journal_positive_and_negative(
        store=journal_store,
        root_lock=root_lock,
        paths=file_bridge_paths,
        header_source=valid_journal,
        positive=semantic_harness.accepted_exchange(exchange, artifact),
        negative=semantic_harness.accepted_exchange(exchange, poisoned),
    )


def _journal_for_snapshot(
    source: PendingJournal,
    snapshot: RuntimeSnapshot,
) -> PendingJournal:
    original = source.original
    body_tree = cast(dict[str, object], json.loads(original.body_json))
    arguments = cast(dict[str, JsonValue], body_tree["arguments"])
    invocation = OPERATION_REGISTRY.resolve_wire_invocation(original.operation, arguments)
    body = RequestBody(
        protocol=snapshot.protocol,
        release_id=snapshot.release_id,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        expected_lineage_id=original.expected_lineage_id,
        expected_activation_id=original.expected_activation_id,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        operation=original.operation,
        arguments=arguments,
    )
    body_json = canonical_body_bytes(body).decode("utf-8")
    request_hash = hashlib.sha256(body_json.encode("utf-8")).hexdigest()
    recovery_schema_id = (
        None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
    )
    intent = original.delivery_intents[0].model_copy(
        update={
            "body_json": body_json,
            "request_hash": request_hash,
            "runtime_snapshot": snapshot,
            "result_schema_id": invocation.result_schema.schema_id,
            "recovery_schema_id": recovery_schema_id,
        }
    )
    exchange = original.model_copy(
        update={
            "request_hash": request_hash,
            "body_json": body_json,
            "runtime_snapshot": snapshot,
            "result_schema_id": invocation.result_schema.schema_id,
            "recovery_schema_id": recovery_schema_id,
            "delivery_intents": (intent,),
        }
    )
    return PendingJournal(
        header=source.header.model_copy(update={"required_release_id": snapshot.release_id}),
        original=exchange,
        reconcile_attempt=None,
    )


@pytest.mark.asyncio
async def test_create_candidate_matrix_fails_closed_before_first_write(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    valid_journal: PendingJournal,
    reconcile_attempt: PendingExchange,
    completed_artifact: ResponseArtifact,
    semantic_harness: SemanticHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = valid_journal.original
    request = original.delivery_intents[0]
    published_request = request.model_copy(update={"published_at_ms": 101})
    published_revision_zero = valid_journal.model_copy(
        update={
            "original": original.model_copy(
                update={
                    "delivery_intents": (published_request,),
                    "state": PendingPhase.PUBLISHED,
                    "updated_at_ms": 101,
                }
            )
        }
    )
    initial_artifact = valid_journal.model_copy(
        update={
            "original": original.model_copy(
                update={
                    "delivery_intents": (published_request,),
                    "response_artifact": completed_artifact,
                    "settlement": completed_artifact.accepted_response.settlement,
                    "state": PendingPhase.RESPONSE_ACCEPTED,
                    "updated_at_ms": 200,
                }
            )
        }
    )
    foreign_snapshot = RuntimeSnapshot.create(
        runtime_version="0.2.0",
        runtime_asset_sha256="0" * 64,
        operation_manifest_sha256=original.runtime_snapshot.operation_manifest_sha256,
        host_contract_sha256="1" * 64,
        dependency_lock_sha256="2" * 64,
    )
    wrong_schema_intent = request.model_copy(update={"result_schema_id": "0" * 64})
    wrong_hash_intent = request.model_copy(update={"request_hash": "0" * 64})
    rejected = semantic_harness.artifact_from_parser(
        original,
        semantic_harness.response_envelope(
            original,
            delivery_id=request.delivery_id,
            result=None,
            error=semantic_harness.not_started_error(original),
        ),
        digest_character="7",
        accepted_at_ms=220,
    )
    poisoned_evidence = semantic_harness.poisoned_artifact(
        rejected,
        _remove_journal_not_started_evidence,
    )
    accepted = semantic_harness.accepted_exchange(original, completed_artifact)
    cases = (
        (
            "nonzero revision",
            valid_journal.model_copy(
                update={"original": original.model_copy(update={"revision": 1})}
            ),
            "new pending journal is not at the revision-zero prepared endpoint",
        ),
        (
            "non-prepared phase",
            published_revision_zero,
            "new pending journal is not at the revision-zero prepared endpoint",
        ),
        (
            "initial reconcile attempt",
            valid_journal.model_copy(update={"reconcile_attempt": reconcile_attempt}),
            "new pending journal is not at the revision-zero prepared endpoint",
        ),
        (
            "initial response artifact",
            initial_artifact,
            "new pending journal is not at the revision-zero prepared endpoint",
        ),
        (
            "foreign release",
            _journal_for_snapshot(valid_journal, foreign_snapshot),
            "pending journal candidate failed semantic validation",
        ),
        (
            "wrong concrete class",
            valid_journal.model_copy(
                update={
                    "original": original.model_copy(
                        update={"effective_class": OperationClass.DESTRUCTIVE}
                    )
                }
            ),
            "pending journal candidate failed semantic validation",
        ),
        (
            "wrong registry schema",
            valid_journal.model_copy(
                update={
                    "original": original.model_copy(
                        update={
                            "result_schema_id": "0" * 64,
                            "delivery_intents": (wrong_schema_intent,),
                        }
                    )
                }
            ),
            "pending journal candidate failed semantic validation",
        ),
        (
            "full snapshot drift",
            valid_journal.model_copy(
                update={
                    "original": original.model_copy(update={"runtime_snapshot": foreign_snapshot})
                }
            ),
            "pending journal candidate failed semantic validation",
        ),
        (
            "request hash drift",
            valid_journal.model_copy(
                update={
                    "original": original.model_copy(
                        update={
                            "request_hash": "0" * 64,
                            "delivery_intents": (wrong_hash_intent,),
                        }
                    )
                }
            ),
            "pending journal candidate failed semantic validation",
        ),
        (
            "not-started evidence drift",
            valid_journal.model_copy(
                update={
                    "original": semantic_harness.accepted_exchange(
                        original,
                        poisoned_evidence,
                    )
                }
            ),
            "pending journal candidate failed semantic validation",
        ),
        (
            "settlement drift",
            valid_journal.model_copy(
                update={
                    "original": accepted.model_copy(
                        update={"settlement": CancelledSettlement(state="cancelled")}
                    )
                }
            ),
            "pending journal candidate failed semantic validation",
        ),
    )
    replace = Mock(side_effect=AssertionError("rejected create candidate must not be written"))
    monkeypatch.setattr(pending_journal_module, "atomic_replace_bytes", replace)

    async with root_lock:
        for label, candidate, message in cases:
            with pytest.raises(BridgeError) as caught:
                journal_store.save(candidate, expected_revisions=None)
            assert caught.value.to_payload() == {
                "code": ErrorCode.STATE_CONFLICT.value,
                "message": message,
                "details": {},
            }, label
            assert not file_bridge_paths.pending_file.exists(), label
    replace.assert_not_called()


@pytest.mark.asyncio
async def test_save_drift_matrix_is_one_way_and_preserves_old_bytes(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    valid_journal: PendingJournal,
) -> None:
    published = _published_journal(valid_journal)
    request = published.original.delivery_intents[0]
    quarantined_changes = {
        "revision": 2,
        "state": PendingPhase.QUARANTINED,
        "updated_at_ms": 150,
    }
    pre_cancel_cases = (
        (
            "rendered size drift",
            published.model_copy(
                update={
                    "original": published.original.model_copy(
                        update={
                            **quarantined_changes,
                            "delivery_intents": (
                                request.model_copy(update={"rendered_inbox_size_bytes": 513}),
                            ),
                        }
                    )
                }
            ),
            "existing delivery intent identity changed",
        ),
        (
            "response filename drift",
            published.model_copy(
                update={
                    "original": published.original.model_copy(
                        update={
                            **quarantined_changes,
                            "delivery_intents": (
                                request.model_copy(
                                    update={"response_filename": "CMOAgentBridge_Response_bad.inst"}
                                ),
                            ),
                        }
                    )
                }
            ),
            "pending journal candidate failed semantic validation",
        ),
        (
            "skipped revision",
            published.model_copy(
                update={
                    "original": published.original.model_copy(
                        update={**quarantined_changes, "revision": 3}
                    )
                }
            ),
            "pending exchange revision did not advance exactly once",
        ),
        (
            "same revision",
            published.model_copy(
                update={
                    "original": published.original.model_copy(
                        update={**quarantined_changes, "revision": 1}
                    )
                }
            ),
            "pending exchange revision did not advance exactly once",
        ),
    )
    cancel = DeliveryIntent.model_validate(
        {
            **request.model_dump(mode="python"),
            "delivery_id": UUID("22222222-2222-4222-8222-222222222222"),
            "delivery_kind": "cancel",
            "original_request_delivery_id": request.delivery_id,
            "intended_at_ms": 151,
            "published_at_ms": None,
            "rendered_inbox_sha256": "1" * 64,
        }
    )
    cancel_published = published.model_copy(
        update={
            "original": published.original.model_copy(
                update={
                    "delivery_intents": (request, cancel),
                    "revision": 2,
                    "state": PendingPhase.CANCEL_PUBLISHED,
                    "updated_at_ms": 151,
                }
            )
        }
    )

    async with root_lock:
        journal_store.save(valid_journal, expected_revisions=None)
        journal_store.save(
            published,
            expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
        )
        published_bytes = file_bridge_paths.pending_file.read_bytes()
        for label, candidate, message in pre_cancel_cases:
            with pytest.raises(BridgeError) as caught:
                journal_store.save(
                    candidate,
                    expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
                )
            assert caught.value.to_payload() == {
                "code": ErrorCode.STATE_CONFLICT.value,
                "message": message,
                "details": {},
            }, label
            assert file_bridge_paths.pending_file.read_bytes() == published_bytes, label

        journal_store.save(
            cancel_published,
            expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
        )
        cancel_bytes = file_bridge_paths.pending_file.read_bytes()
        published_cancel = cancel.model_copy(update={"published_at_ms": 152})
        reordered = cancel_published.model_copy(
            update={
                "original": cancel_published.original.model_copy(
                    update={
                        "delivery_intents": (published_cancel, request),
                        "revision": 3,
                        "updated_at_ms": 152,
                    }
                )
            }
        )
        second_cancel = cancel.model_copy(
            update={
                "delivery_id": UUID("99999999-9999-4999-8999-999999999999"),
                "intended_at_ms": 153,
                "rendered_inbox_sha256": "2" * 64,
            }
        )
        duplicate_cancel = cancel_published.model_copy(
            update={
                "original": cancel_published.original.model_copy(
                    update={
                        "delivery_intents": (request, cancel, second_cancel),
                        "revision": 3,
                        "updated_at_ms": 153,
                    }
                )
            }
        )
        post_cancel_cases = (
            ("delivery reorder", reordered, "existing delivery intent identity changed"),
            (
                "second cancel",
                duplicate_cancel,
                "pending journal candidate failed semantic validation",
            ),
        )
        for label, candidate, message in post_cancel_cases:
            with pytest.raises(BridgeError) as caught:
                journal_store.save(
                    candidate,
                    expected_revisions=JournalRevisions(original=2, reconcile_attempt=None),
                )
            assert caught.value.to_payload() == {
                "code": ErrorCode.STATE_CONFLICT.value,
                "message": message,
                "details": {},
            }, label
            assert file_bridge_paths.pending_file.read_bytes() == cancel_bytes, label


@pytest.mark.asyncio
async def test_reconcile_attempt_full_lifecycle_reaches_only_allowed_delete_endpoint(
    journal_store: PendingJournalStore,
    root_lock: RootLock,
    file_bridge_paths: FileBridgePaths,
    valid_journal: PendingJournal,
    reconcile_attempt: PendingExchange,
    semantic_harness: SemanticHarness,
) -> None:
    published = _published_journal(valid_journal)
    quarantined = _quarantined_journal(valid_journal)
    with_attempt = quarantined.model_copy(update={"reconcile_attempt": reconcile_attempt})
    attempt_request = reconcile_attempt.delivery_intents[0].model_copy(
        update={"published_at_ms": 301}
    )
    attempt_published_exchange = reconcile_attempt.model_copy(
        update={
            "delivery_intents": (attempt_request,),
            "revision": 1,
            "state": PendingPhase.PUBLISHED,
            "updated_at_ms": 301,
        }
    )
    attempt_published = with_attempt.model_copy(
        update={"reconcile_attempt": attempt_published_exchange}
    )
    target_id = reconcile_attempt.original_target_request_id
    target_hash = reconcile_attempt.original_target_request_hash
    assert target_id is not None
    assert target_hash is not None
    result = cast(
        JsonValue,
        {
            "request_id": str(target_id),
            "request_hash": target_hash,
            "disposition": "applied",
            "resolved": True,
        },
    )
    artifact = semantic_harness.artifact_from_parser(
        reconcile_attempt,
        semantic_harness.response_envelope(
            reconcile_attempt,
            delivery_id=reconcile_attempt.delivery_intents[0].delivery_id,
            result=result,
            error=None,
        ),
        digest_character="6",
        accepted_at_ms=410,
    )
    accepted_exchange = attempt_published_exchange.model_copy(
        update={
            "response_artifact": artifact,
            "settlement": artifact.accepted_response.settlement,
            "revision": 2,
            "state": PendingPhase.RESPONSE_ACCEPTED,
            "updated_at_ms": 410,
        }
    )
    accepted = attempt_published.model_copy(update={"reconcile_attempt": accepted_exchange})
    idle_exchange = accepted_exchange.model_copy(
        update={
            "revision": 3,
            "state": PendingPhase.IDLE_PUBLISHED,
            "updated_at_ms": 411,
        }
    )
    idle = accepted.model_copy(update={"reconcile_attempt": idle_exchange})

    async with root_lock:
        assert journal_store.save(valid_journal, expected_revisions=None) == JournalRevisions(
            original=0,
            reconcile_attempt=None,
        )
        assert journal_store.save(
            published,
            expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
        ) == JournalRevisions(original=1, reconcile_attempt=None)
        assert journal_store.save(
            quarantined,
            expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
        ) == JournalRevisions(original=2, reconcile_attempt=None)
        assert journal_store.save(
            with_attempt,
            expected_revisions=JournalRevisions(original=2, reconcile_attempt=None),
        ) == JournalRevisions(original=2, reconcile_attempt=0)
        assert journal_store.save(
            attempt_published,
            expected_revisions=JournalRevisions(original=2, reconcile_attempt=0),
        ) == JournalRevisions(original=2, reconcile_attempt=1)
        assert journal_store.save(
            accepted,
            expected_revisions=JournalRevisions(original=2, reconcile_attempt=1),
        ) == JournalRevisions(original=2, reconcile_attempt=2)
        assert journal_store.save(
            idle,
            expected_revisions=JournalRevisions(original=2, reconcile_attempt=2),
        ) == JournalRevisions(original=2, reconcile_attempt=3)
        loaded = journal_store.load()
        assert loaded is not None
        assert loaded.journal == idle
        journal_store.delete(
            JournalDeleteExpectation(
                root_key=idle.header.root_key,
                required_release_id=idle.header.required_release_id,
                original_request_id=idle.original.request_id,
                reconcile_attempt_request_id=idle_exchange.request_id,
                revisions=JournalRevisions(original=2, reconcile_attempt=3),
            )
        )
    assert not file_bridge_paths.pending_file.exists()
