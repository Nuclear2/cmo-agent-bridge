from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.state import sqlite as sqlite_module
from cmo_agent_bridge.state.sqlite import StateDatabase


ROOT_KEY = "a" * 64
REQUEST_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
LINEAGE_ID = "11111111-1111-4111-8111-111111111111"
ACTIVATION_ID = "22222222-2222-4222-8222-222222222222"


def _create_v1_database(path: Path, *, with_data: bool = False) -> None:
    with sqlite3.connect(path) as connection:
        for statement in sqlite_module.MIGRATION_1_STATEMENTS:
            connection.execute(statement)
        connection.execute("INSERT INTO schema_migrations(version, applied_at_ms) VALUES(1, 123)")
        if with_data:
            connection.execute(
                """
                INSERT INTO requests(
                    request_id,root_key,request_hash,operation,operation_class,state,
                    runtime_version,runtime_tag,runtime_asset_sha256,release_id,
                    host_contract_sha256,dependency_lock_sha256,manifest_sha256,
                    result_schema_id,recovery_schema_id,body_json,lineage_id,activation_id,
                    result_json,error_json,resolution_json,created_at_ms,updated_at_ms,terminal_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    REQUEST_ID,
                    ROOT_KEY,
                    "b" * 64,
                    "scenario.get",
                    "read",
                    "prepared",
                    "0.1.0",
                    "0_1_0-" + "c" * 64,
                    "c" * 64,
                    "d" * 64,
                    "e" * 64,
                    "f" * 64,
                    "1" * 64,
                    "2" * 64,
                    None,
                    sqlite3.Binary(b"{}"),
                    LINEAGE_ID,
                    ACTIVATION_ID,
                    None,
                    None,
                    None,
                    10,
                    10,
                    None,
                ),
            )
            connection.execute(
                """
                INSERT INTO sessions(
                    root_key,scenario_lineage_id,activation_id,build_number,
                    runtime_version,runtime_tag,runtime_asset_sha256,release_id,
                    host_contract_sha256,dependency_lock_sha256,manifest_sha256,
                    process_pid,process_create_time,validated_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    ROOT_KEY,
                    LINEAGE_ID,
                    ACTIVATION_ID,
                    1868,
                    "0.1.0",
                    "0_1_0-" + "c" * 64,
                    "c" * 64,
                    "d" * 64,
                    "e" * 64,
                    "f" * 64,
                    "1" * 64,
                    1234,
                    123.0,
                    10,
                ),
            )


def _create_exact_v2_table(connection: sqlite3.Connection) -> None:
    for statement in sqlite_module.MIGRATION_2_STATEMENTS:
        connection.execute(statement)


def _user_objects(connection: sqlite3.Connection) -> list[tuple[str, str]]:
    return connection.execute(
        "SELECT type,name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
    ).fetchall()


def test_exact_legacy_v1_upgrades_without_rewriting_existing_data(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    _create_v1_database(path, with_data=True)

    StateDatabase(path).initialize()

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT version,applied_at_ms FROM schema_migrations ORDER BY version"
        ).fetchall()[0] == (1, 123)
        assert connection.execute(
            "SELECT request_id,root_key,body_json FROM requests"
        ).fetchall() == [(REQUEST_ID, ROOT_KEY, b"{}")]
        assert connection.execute(
            "SELECT root_key,scenario_lineage_id,activation_id FROM sessions"
        ).fetchall() == [(ROOT_KEY, LINEAGE_ID, ACTIVATION_ID)]
        assert connection.execute("SELECT COUNT(*) FROM confirmations").fetchone() == (0,)


def test_repeated_v2_initialization_is_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    database = StateDatabase(path)

    database.initialize()
    database.initialize()

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT version,COUNT(*) FROM schema_migrations GROUP BY version ORDER BY version"
        ).fetchall() == [(1, 1), (2, 1), (3, 1)]


def test_malformed_v1_is_rejected_without_creating_confirmation_table(tmp_path: Path) -> None:
    path = tmp_path / "malformed-v1.sqlite3"
    _create_v1_database(path)
    with sqlite3.connect(path) as connection:
        connection.execute("ALTER TABLE sessions ADD COLUMN unexpected TEXT")

    with pytest.raises(BridgeError) as caught:
        StateDatabase(path).initialize()

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    with sqlite3.connect(path) as connection:
        assert "confirmations" not in {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert connection.execute(
            "SELECT version,applied_at_ms FROM schema_migrations"
        ).fetchall() == [(1, 123)]


@pytest.mark.parametrize("variant", ["partial-table", "missing-history", "wrong-history"])
def test_partial_or_wrong_history_v2_is_rejected(tmp_path: Path, variant: str) -> None:
    path = tmp_path / f"{variant}.sqlite3"
    _create_v1_database(path)
    with sqlite3.connect(path) as connection:
        if variant == "partial-table":
            connection.execute(
                "CREATE TABLE confirmations(token_sha256 TEXT PRIMARY KEY, root_key TEXT NOT NULL)"
            )
        else:
            _create_exact_v2_table(connection)
            if variant == "wrong-history":
                connection.execute(
                    "INSERT INTO schema_migrations(version,applied_at_ms) VALUES(3,456)"
                )

    with pytest.raises(BridgeError) as caught:
        StateDatabase(path).initialize()

    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_injected_migration_two_failure_rolls_back_fresh_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "fresh-failure.sqlite3"
    monkeypatch.setattr(sqlite_module, "MIGRATION_2_STATEMENTS", ("CREATE TABLE broken(",))

    with pytest.raises(sqlite3.OperationalError):
        StateDatabase(path).initialize()

    with sqlite3.connect(path) as connection:
        assert _user_objects(connection) == []


def test_injected_migration_two_failure_preserves_committed_v1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "legacy-failure.sqlite3"
    _create_v1_database(path, with_data=True)
    original_objects: list[tuple[str, str]]
    with sqlite3.connect(path) as connection:
        original_objects = _user_objects(connection)
    monkeypatch.setattr(sqlite_module, "MIGRATION_2_STATEMENTS", ("CREATE TABLE broken(",))

    with pytest.raises(sqlite3.OperationalError):
        StateDatabase(path).initialize()

    with sqlite3.connect(path) as connection:
        assert _user_objects(connection) == original_objects
        assert connection.execute(
            "SELECT version,applied_at_ms FROM schema_migrations"
        ).fetchall() == [(1, 123)]
        assert connection.execute("SELECT request_id FROM requests").fetchall() == [(REQUEST_ID,)]


def test_valid_but_wrong_migration_two_ddl_rolls_back_fresh_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "fresh-wrong-ddl.sqlite3"
    monkeypatch.setattr(
        sqlite_module,
        "MIGRATION_2_STATEMENTS",
        ("CREATE TABLE confirmations(token_sha256 TEXT PRIMARY KEY)",),
    )

    with pytest.raises(BridgeError) as caught:
        StateDatabase(path).initialize()

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    with sqlite3.connect(path) as connection:
        assert _user_objects(connection) == []


def test_valid_but_wrong_migration_two_ddl_rolls_back_to_committed_v1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "legacy-wrong-ddl.sqlite3"
    _create_v1_database(path, with_data=True)
    with sqlite3.connect(path) as connection:
        original_objects = _user_objects(connection)
    monkeypatch.setattr(
        sqlite_module,
        "MIGRATION_2_STATEMENTS",
        ("CREATE TABLE confirmations(token_sha256 TEXT PRIMARY KEY)",),
    )

    with pytest.raises(BridgeError) as caught:
        StateDatabase(path).initialize()

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    with sqlite3.connect(path) as connection:
        assert _user_objects(connection) == original_objects
        assert connection.execute(
            "SELECT version,applied_at_ms FROM schema_migrations"
        ).fetchall() == [(1, 123)]
        assert connection.execute("SELECT request_id FROM requests").fetchall() == [(REQUEST_ID,)]
