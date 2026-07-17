from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

from cmo_agent_bridge.state import sqlite as sqlite_module
from cmo_agent_bridge.state.sqlite import StateDatabase


def _initialize_after_barrier(owner: StateDatabase, barrier: Barrier) -> None:
    barrier.wait(timeout=5)
    owner.initialize()


def test_two_simultaneous_initializers_converge_on_one_migration(tmp_path: Path) -> None:
    path = tmp_path / "state" / "bridge.sqlite3"
    owners = (StateDatabase(path), StateDatabase(path))
    barrier = Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(_initialize_after_barrier, owner, barrier) for owner in owners]
        for future in futures:
            future.result(timeout=10)

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT version, COUNT(*) FROM schema_migrations GROUP BY version"
        ).fetchall() == [(1, 1), (2, 1)]
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)


def test_two_simultaneous_initializers_upgrade_exact_v1_once(tmp_path: Path) -> None:
    path = tmp_path / "state" / "legacy.sqlite3"
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as connection:
        for statement in sqlite_module.MIGRATION_1_STATEMENTS:
            connection.execute(statement)
        connection.execute("INSERT INTO schema_migrations(version,applied_at_ms) VALUES(1,123)")

    owners = (StateDatabase(path), StateDatabase(path))
    barrier = Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(_initialize_after_barrier, owner, barrier) for owner in owners]
        for future in futures:
            future.result(timeout=10)

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT version,COUNT(*) FROM schema_migrations GROUP BY version ORDER BY version"
        ).fetchall() == [(1, 1), (2, 1)]
        assert connection.execute(
            "SELECT applied_at_ms FROM schema_migrations WHERE version=1"
        ).fetchone() == (123,)
        assert connection.execute("SELECT COUNT(*) FROM confirmations").fetchone() == (0,)
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
