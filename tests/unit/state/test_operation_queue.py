from __future__ import annotations

import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.models import RequestBody
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state import sqlite as sqlite_module
from cmo_agent_bridge.state.operation_queue import (
    OperationQueueRecord,
    OperationQueueState,
    OperationQueueStore,
)
from cmo_agent_bridge.state.sqlite import StateDatabase


ROOT_KEY = "a" * 64
SNAPSHOT = RuntimeSnapshot.create(
    runtime_version="0.2.0",
    runtime_asset_sha256="b" * 64,
    operation_manifest_sha256="c" * 64,
    host_contract_sha256="d" * 64,
    dependency_lock_sha256="e" * 64,
)


def _body(*, operation: str, request_id: UUID) -> bytes:
    return canonical_body_bytes(
        RequestBody(
            protocol=SNAPSHOT.protocol,
            release_id=SNAPSHOT.release_id,
            runtime_version=SNAPSHOT.runtime_version,
            runtime_tag=SNAPSHOT.runtime_tag,
            runtime_asset_sha256=SNAPSHOT.runtime_asset_sha256,
            expected_lineage_id=None,
            expected_activation_id=None,
            operation_manifest_sha256=SNAPSHOT.operation_manifest_sha256,
            operation=operation,
            arguments={"wire_request_id": str(request_id)},
        )
    )


def _queued_record(
    *, request_id: UUID, operation: str = "unit.assign", root_key: str = ROOT_KEY
) -> OperationQueueRecord:
    return OperationQueueRecord(
        queue_sequence=0,
        request_id=request_id,
        root_key=root_key,
        operation=operation,
        arguments_json=b'{"unit_guid":"public-unit"}',
        body_json=_body(operation=operation, request_id=request_id),
        runtime_snapshot=SNAPSHOT,
        result_schema_id="f" * 64,
        recovery_schema_id=None,
        expected_lineage_id=None,
        expected_activation_id=None,
        expected_process_pid=42,
        expected_process_create_time=100.5,
        state=OperationQueueState.QUEUED,
        result_json=None,
        error_json=None,
        created_at_ms=100,
        updated_at_ms=100,
        terminal_at_ms=None,
    )


def _store(path: Path) -> OperationQueueStore:
    database = StateDatabase(path)
    database.initialize()
    return OperationQueueStore(database)


def _create_v2_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        for statement in sqlite_module.MIGRATION_1_STATEMENTS:
            connection.execute(statement)
        connection.execute("INSERT INTO schema_migrations(version, applied_at_ms) VALUES(1, 10)")
        for statement in sqlite_module.MIGRATION_2_STATEMENTS:
            connection.execute(statement)
        connection.execute("INSERT INTO schema_migrations(version, applied_at_ms) VALUES(2, 20)")


def test_exact_v2_database_upgrades_to_operation_queue(tmp_path: Path) -> None:
    path = tmp_path / "legacy-v2.sqlite3"
    _create_v2_database(path)

    StateDatabase(path).initialize()

    with sqlite3.connect(path) as connection:
        history = connection.execute(
            "SELECT version,applied_at_ms FROM schema_migrations ORDER BY version"
        ).fetchall()
        assert [row[0] for row in history] == [1, 2, 3]
        assert history[:2] == [(1, 10), (2, 20)]
        assert type(history[2][1]) is int and history[2][1] >= 20
        assert connection.execute("SELECT COUNT(*) FROM operation_queue").fetchone() == (0,)


def test_enqueue_claim_fifo_and_cas(tmp_path: Path) -> None:
    store = _store(tmp_path / "state.sqlite3")
    first_id, second_id = uuid4(), uuid4()
    first = store.enqueue(_queued_record(request_id=first_id))
    second = store.enqueue(_queued_record(request_id=second_id))

    assert (first.queue_sequence, second.queue_sequence) == (1, 2)
    assert store.head() == first
    assert store.claim_next(root_key=ROOT_KEY, at_ms=110) == first.model_copy(
        update={"state": OperationQueueState.ACTIVE, "updated_at_ms": 110}
    )
    assert store.claim_next(root_key=ROOT_KEY, at_ms=111) is None
    assert store.reset_active_before_publication(first_id, at_ms=112) is True
    claimed = store.claim_next(root_key=ROOT_KEY, at_ms=113)
    assert claimed is not None and claimed.request_id == first_id
    completed = store.complete(first_id, b'{"ok":true}', at_ms=114)
    assert completed is not None and completed.state is OperationQueueState.COMPLETED
    assert store.claim_next(root_key=ROOT_KEY, at_ms=115) is not None
    assert store.claim_next(root_key=ROOT_KEY, at_ms=116) is None


def test_claim_is_isolated_per_game_root(tmp_path: Path) -> None:
    store = _store(tmp_path / "state.sqlite3")
    other_root = "b" * 64
    first = store.enqueue(_queued_record(request_id=uuid4()))
    other = store.enqueue(_queued_record(request_id=uuid4(), root_key=other_root))

    assert store.claim_next(root_key=ROOT_KEY, at_ms=110).request_id == first.request_id  # type: ignore[union-attr]
    assert store.claim_next(root_key=other_root, at_ms=111).request_id == other.request_id  # type: ignore[union-attr]
    assert store.claim_next(root_key=ROOT_KEY, at_ms=112) is None
    assert store.claim_next(root_key=other_root, at_ms=113) is None


def test_cancel_is_queued_only_and_terminal_rows_are_immutable(tmp_path: Path) -> None:
    store = _store(tmp_path / "state.sqlite3")
    request_id = uuid4()
    stored = store.enqueue(_queued_record(request_id=request_id))

    cancelled = store.cancel_queued(request_id, at_ms=101)

    assert cancelled is not None
    assert cancelled.state is OperationQueueState.CANCELLED
    assert cancelled.terminal_at_ms == 101
    assert store.cancel_queued(request_id, at_ms=102) is None
    assert store.claim_next(root_key=ROOT_KEY, at_ms=103) is None
    assert store.get(request_id) == cancelled
    assert store.counts().cancelled == 1
    assert store.counts().queued == 0
    assert stored.queue_sequence == cancelled.queue_sequence


def test_terminal_transitions_are_active_only_and_reject_preserves_error(tmp_path: Path) -> None:
    store = _store(tmp_path / "state.sqlite3")
    request_id = uuid4()
    store.enqueue(_queued_record(request_id=request_id))

    assert store.reject(request_id, b'{"code":"REJECTED"}', at_ms=101) is None
    claimed = store.claim_next(root_key=ROOT_KEY, at_ms=102)
    assert claimed is not None
    rejected = store.reject(request_id, b'{"code":"REJECTED"}', at_ms=103)
    assert rejected is not None
    assert rejected.state is OperationQueueState.REJECTED
    assert rejected.error_json == b'{"code":"REJECTED"}'
    assert rejected.result_json is None
    assert store.complete(request_id, b'{"ok":true}', at_ms=104) is None


def test_reject_queued_is_explicit_and_does_not_reject_active(tmp_path: Path) -> None:
    store = _store(tmp_path / "state.sqlite3")
    queued_id, active_id = uuid4(), uuid4()
    store.enqueue(_queued_record(request_id=active_id))
    store.enqueue(_queued_record(request_id=queued_id))
    assert store.claim_next(root_key=ROOT_KEY, at_ms=101).request_id == active_id  # type: ignore[union-attr]

    assert store.reject_queued(active_id, b'{"code":"REJECTED"}', at_ms=102) is None
    rejected = store.reject_queued(queued_id, b'{"code":"REJECTED"}', at_ms=103)

    assert rejected is not None
    assert rejected.state is OperationQueueState.REJECTED
    assert rejected.error_json == b'{"code":"REJECTED"}'


def test_enqueue_rejects_duplicate_request_id_and_noncanonical_public_arguments(tmp_path: Path) -> None:
    store = _store(tmp_path / "state.sqlite3")
    request_id = uuid4()
    store.enqueue(_queued_record(request_id=request_id))

    with pytest.raises(BridgeError) as duplicate:
        store.enqueue(_queued_record(request_id=request_id))
    assert duplicate.value.code is ErrorCode.REQUEST_ID_REUSED

    malformed = _queued_record(request_id=uuid4()).model_dump(mode="python")
    malformed["arguments_json"] = b'{"z":1,"a":2}'
    with pytest.raises(ValueError):
        OperationQueueRecord.model_validate(malformed)


def test_summary_snapshot_keeps_counts_and_quarantined_rows_on_one_read_view(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state.sqlite3"
    database = StateDatabase(path)
    database.initialize()
    store = OperationQueueStore(database)
    writer_store = OperationQueueStore(StateDatabase(path))
    request_id = uuid4()
    store.enqueue(_queued_record(request_id=request_id))
    assert store.claim_next(root_key=ROOT_KEY, at_ms=101) is not None
    original_transaction = database._transaction  # pyright: ignore[reportPrivateUsage]
    writer_committed = False

    class _InterleavingConnection:
        def __init__(self, connection: sqlite3.Connection) -> None:
            self._connection = connection

        def execute(
            self,
            sql: str,
            parameters: tuple[object, ...] = (),
        ) -> sqlite3.Cursor:
            nonlocal writer_committed
            cursor = self._connection.execute(sql, parameters)
            if "SELECT state,COUNT" in sql and not writer_committed:
                quarantined = writer_store.quarantine(
                    request_id,
                    b'{"code":"INDETERMINATE_OUTCOME"}',
                    at_ms=102,
                )
                assert quarantined is not None
                writer_committed = True
            return cursor

    @contextmanager
    def interleaved_transaction(*, write: bool) -> Generator[sqlite3.Connection]:
        with original_transaction(write=write) as connection:
            if write:
                yield connection
            else:
                proxy = _InterleavingConnection(connection)
                yield cast(sqlite3.Connection, cast(object, proxy))

    monkeypatch.setattr(database, "_transaction", interleaved_transaction)

    counts, quarantined = store.summary_snapshot(root_key=ROOT_KEY)

    assert writer_committed is True
    assert counts.active == 1
    assert counts.quarantined == 0
    assert quarantined == ()
    current = writer_store.counts(root_key=ROOT_KEY)
    assert current.active == 0
    assert current.quarantined == 1
