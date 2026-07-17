from __future__ import annotations

import math
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast
from unittest.mock import Mock
from uuid import UUID

import pytest
from pydantic import ValidationError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.session_store import SessionRecord, SessionStore
from cmo_agent_bridge.state.sqlite import StateDatabase


@pytest.fixture
def session_database(tmp_path: Path) -> StateDatabase:
    return StateDatabase(tmp_path / "state" / "bridge.sqlite3")


@pytest.fixture
def session_store(session_database: StateDatabase) -> SessionStore:
    return SessionStore(session_database)


@pytest.fixture
def session_record(runtime_snapshot: RuntimeSnapshot) -> SessionRecord:
    return SessionRecord(
        root_key="a" * 64,
        scenario_lineage_id=UUID("33333333-3333-4333-8333-333333333333"),
        activation_id=UUID("44444444-4444-4444-8444-444444444444"),
        build_number=1868,
        runtime_snapshot=runtime_snapshot,
        process_pid=1234,
        process_create_time=12345.5,
        validated_at_ms=100,
    )


def test_session_construction_performs_zero_sqlite_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = Mock(side_effect=AssertionError("constructors must not connect"))
    monkeypatch.setattr(sqlite3, "connect", connect)

    database = StateDatabase(tmp_path / "state.sqlite3")
    SessionStore(database)

    connect.assert_not_called()


def test_session_record_has_exact_fields_and_forbids_executable_path(
    session_record: SessionRecord,
) -> None:
    assert list(SessionRecord.model_fields) == [
        "root_key",
        "scenario_lineage_id",
        "activation_id",
        "build_number",
        "runtime_snapshot",
        "process_pid",
        "process_create_time",
        "validated_at_ms",
    ]
    with pytest.raises(ValidationError):
        SessionRecord.model_validate(
            {**session_record.model_dump(mode="python"), "executable": "Command.exe"}
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("build_number", True),
        ("build_number", 0),
        ("process_pid", False),
        ("process_pid", 0),
        ("process_create_time", True),
        ("process_create_time", 1),
        ("process_create_time", 0.0),
        ("process_create_time", math.nan),
        ("process_create_time", math.inf),
        ("validated_at_ms", True),
        ("validated_at_ms", -1),
    ],
)
def test_session_record_rejects_invalid_strict_numeric_values(
    session_record: SessionRecord,
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError):
        SessionRecord.model_validate({**session_record.model_dump(mode="python"), field: value})


def test_replace_load_full_upsert_root_isolation_and_activation_cas_clear(
    session_store: SessionStore,
    session_record: SessionRecord,
) -> None:
    session_store.replace(session_record)
    assert session_store.load(session_record.root_key) == session_record
    assert session_store.load("b" * 64) is None

    drifted_snapshot = RuntimeSnapshot.create(
        runtime_version="0.2.0",
        runtime_asset_sha256="1" * 64,
        operation_manifest_sha256="2" * 64,
        host_contract_sha256="3" * 64,
        dependency_lock_sha256="4" * 64,
    )
    replacement = session_record.model_copy(
        update={
            "scenario_lineage_id": UUID("66666666-6666-4666-8666-666666666666"),
            "activation_id": UUID("55555555-5555-4555-8555-555555555555"),
            "build_number": 1900,
            "runtime_snapshot": drifted_snapshot,
            "process_pid": 5678,
            "process_create_time": 22345.5,
            "validated_at_ms": 200,
        }
    )
    session_store.replace(replacement)
    assert session_store.load(session_record.root_key) == replacement
    assert (
        session_store.clear(
            session_record.root_key,
            expected_activation_id=session_record.activation_id,
        )
        is False
    )
    assert (
        session_store.clear(
            session_record.root_key,
            expected_activation_id=replacement.activation_id,
        )
        is True
    )
    assert session_store.clear(session_record.root_key) is False


def test_conditional_replace_inserts_absent_and_updates_only_matching_activation(
    session_store: SessionStore,
    session_record: SessionRecord,
) -> None:
    contender = session_record.model_copy(
        update={
            "scenario_lineage_id": UUID("66666666-6666-4666-8666-666666666666"),
            "activation_id": UUID("55555555-5555-4555-8555-555555555555"),
            "validated_at_ms": 200,
        }
    )

    assert (
        session_store.conditional_replace(
            session_record,
            expected_activation_id=None,
        )
        is True
    )
    assert (
        session_store.conditional_replace(
            contender,
            expected_activation_id=None,
        )
        is False
    )
    assert session_store.load(session_record.root_key) == session_record
    assert (
        session_store.conditional_replace(
            contender,
            expected_activation_id=contender.activation_id,
        )
        is False
    )
    assert session_store.load(session_record.root_key) == session_record
    assert (
        session_store.conditional_replace(
            contender,
            expected_activation_id=session_record.activation_id,
        )
        is True
    )
    assert session_store.load(session_record.root_key) == contender


def test_conditional_insert_has_one_winner_under_concurrency(
    session_store: SessionStore,
    session_record: SessionRecord,
) -> None:
    contenders = tuple(
        session_record.model_copy(
            update={
                "activation_id": UUID(f"{index:08x}-4444-4444-8444-444444444444"),
                "validated_at_ms": 100 + index,
            }
        )
        for index in range(1, 9)
    )

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(
                session_store.conditional_replace,
                contender,
                expected_activation_id=None,
            )
            for contender in contenders
        ]
        outcomes = [future.result(timeout=10) for future in futures]

    assert outcomes.count(True) == 1
    assert outcomes.count(False) == 7
    assert session_store.load(session_record.root_key) in contenders


def test_malformed_session_row_fails_closed(
    session_store: SessionStore,
    session_database: StateDatabase,
    session_record: SessionRecord,
) -> None:
    session_store.replace(session_record)
    with sqlite3.connect(session_database.path) as connection:
        connection.execute(
            "UPDATE sessions SET runtime_tag='forged' WHERE root_key=?",
            (session_record.root_key,),
        )
        connection.commit()
    with pytest.raises(BridgeError) as caught:
        session_store.load(session_record.root_key)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


@pytest.mark.parametrize("poison", [1, math.nan, math.inf])
def test_external_integer_nan_and_infinite_process_times_fail_closed_on_row_decode(
    session_store: SessionStore,
    session_database: StateDatabase,
    session_record: SessionRecord,
    poison: object,
) -> None:
    session_store.replace(session_record)
    with sqlite3.connect(session_database.path) as connection:
        connection.row_factory = sqlite3.Row
        durable = connection.execute(
            "SELECT * FROM sessions WHERE root_key=?", (session_record.root_key,)
        ).fetchone()
    assert durable is not None
    poisoned = dict(durable)
    poisoned["process_create_time"] = poison

    with pytest.raises(BridgeError) as caught:
        SessionStore._record_from_row(  # pyright: ignore[reportPrivateUsage]
            cast(sqlite3.Row, poisoned)
        )
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_concurrent_session_readers_use_independent_connections(
    session_store: SessionStore,
    session_record: SessionRecord,
) -> None:
    session_store.replace(session_record)
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(session_store.load, session_record.root_key) for _ in range(32)]
        assert all(future.result(timeout=10) == session_record for future in futures)


def test_migration_has_no_executable_column_and_h11_owns_fresh_processinfo_path_validation(
    session_database: StateDatabase,
) -> None:
    session_database.initialize()
    with sqlite3.connect(session_database.path) as connection:
        columns = [row[1] for row in connection.execute("PRAGMA table_info(sessions)")]
    assert "executable" not in columns
    assert "executable_path" not in columns
