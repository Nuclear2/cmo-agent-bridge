from __future__ import annotations

import sqlite3
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import cast

from cmo_agent_bridge.errors import BridgeError, ErrorCode


MIGRATION_1_STATEMENTS = (
    """
CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    applied_at_ms INTEGER NOT NULL CHECK (applied_at_ms >= 0)
)
""",
    """
CREATE TABLE requests (
    request_id TEXT PRIMARY KEY CHECK (length(request_id) = 36),
    root_key TEXT NOT NULL CHECK (length(root_key) = 64),
    request_hash TEXT NOT NULL CHECK (length(request_hash) = 64),
    operation TEXT NOT NULL,
    operation_class TEXT NOT NULL CHECK (
        operation_class IN ('status','read','mutation','destructive','reconcile','dynamic')
    ),
    state TEXT NOT NULL CHECK (
        state IN ('prepared','published','cancel_published','response_accepted',
                  'idle_published','completed','cancelled','rejected','quarantined','resolved')
    ),
    runtime_version TEXT NOT NULL,
    runtime_tag TEXT NOT NULL,
    runtime_asset_sha256 TEXT NOT NULL CHECK (length(runtime_asset_sha256) = 64),
    release_id TEXT NOT NULL CHECK (length(release_id) = 64),
    host_contract_sha256 TEXT NOT NULL CHECK (length(host_contract_sha256) = 64),
    dependency_lock_sha256 TEXT NOT NULL CHECK (length(dependency_lock_sha256) = 64),
    manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
    result_schema_id TEXT NOT NULL,
    recovery_schema_id TEXT,
    body_json BLOB NOT NULL,
    lineage_id TEXT,
    activation_id TEXT,
    result_json TEXT,
    error_json TEXT,
    resolution_json TEXT,
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
    terminal_at_ms INTEGER
)
""",
    "CREATE INDEX requests_root_state_idx ON requests(root_key, state, updated_at_ms)",
    "CREATE INDEX requests_terminal_idx ON requests(terminal_at_ms) WHERE terminal_at_ms IS NOT NULL",
    """
CREATE TABLE deliveries (
    delivery_id TEXT PRIMARY KEY CHECK (length(delivery_id) = 36),
    request_id TEXT NOT NULL REFERENCES requests(request_id) ON DELETE RESTRICT,
    delivery_kind TEXT NOT NULL CHECK (delivery_kind IN ('request','cancel')),
    original_request_delivery_id TEXT NOT NULL CHECK (length(original_request_delivery_id) = 36),
    intended_at_ms INTEGER NOT NULL CHECK (intended_at_ms >= 0),
    published_at_ms INTEGER CHECK (published_at_ms IS NULL OR published_at_ms >= intended_at_ms),
    rendered_inbox_sha256 TEXT NOT NULL CHECK (length(rendered_inbox_sha256) = 64),
    rendered_inbox_size_bytes INTEGER NOT NULL CHECK (rendered_inbox_size_bytes >= 0),
    response_filename TEXT NOT NULL,
    response_at_ms INTEGER,
    response_sha256 TEXT,
    response_size_bytes INTEGER,
    accepted_response_json BLOB,
    settlement_json BLOB,
    CHECK (
        (
            response_at_ms IS NULL
            AND response_sha256 IS NULL
            AND response_size_bytes IS NULL
            AND accepted_response_json IS NULL
            AND settlement_json IS NULL
        )
        OR
        (
            response_at_ms IS NOT NULL
            AND typeof(response_at_ms) = 'integer'
            AND response_at_ms >= 0
            AND response_sha256 IS NOT NULL
            AND typeof(response_sha256) = 'text'
            AND length(response_sha256) = 64
            AND response_sha256 NOT GLOB '*[^0-9a-f]*'
            AND response_size_bytes IS NOT NULL
            AND typeof(response_size_bytes) = 'integer'
            AND response_size_bytes >= 0
            AND typeof(accepted_response_json) = 'blob'
            AND length(accepted_response_json) > 0
            AND typeof(settlement_json) = 'blob'
            AND length(settlement_json) > 0
        )
    )
)
""",
    "CREATE INDEX deliveries_request_idx ON deliveries(request_id, intended_at_ms)",
    """
CREATE TABLE sessions (
    root_key TEXT PRIMARY KEY CHECK (length(root_key) = 64),
    scenario_lineage_id TEXT NOT NULL CHECK (length(scenario_lineage_id) = 36),
    activation_id TEXT NOT NULL CHECK (length(activation_id) = 36),
    build_number INTEGER NOT NULL CHECK (build_number > 0),
    runtime_version TEXT NOT NULL,
    runtime_tag TEXT NOT NULL,
    runtime_asset_sha256 TEXT NOT NULL CHECK (length(runtime_asset_sha256) = 64),
    release_id TEXT NOT NULL CHECK (length(release_id) = 64),
    host_contract_sha256 TEXT NOT NULL CHECK (length(host_contract_sha256) = 64),
    dependency_lock_sha256 TEXT NOT NULL CHECK (length(dependency_lock_sha256) = 64),
    manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
    process_pid INTEGER NOT NULL CHECK (process_pid > 0),
    process_create_time REAL NOT NULL,
    validated_at_ms INTEGER NOT NULL CHECK (validated_at_ms >= 0)
)
    """,
)

MIGRATION_2_STATEMENTS = (
    """
CREATE TABLE confirmations (
    token_sha256 TEXT PRIMARY KEY,
    root_key TEXT NOT NULL,
    operation TEXT NOT NULL,
    binding_format TEXT NOT NULL,
    binding_sha256 TEXT NOT NULL CHECK (length(binding_sha256) = 64),
    lineage_id TEXT NOT NULL,
    activation_id TEXT NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    used_at_ms INTEGER
)
""",
)


def _normalized_schema_sql(value: object) -> str:
    if type(value) is not str:
        raise ValueError("SQLite schema SQL must be exact text")
    return " ".join(value.split())


_EXPECTED_MIGRATION_1_SCHEMA_SQL = {
    ("table", "schema_migrations"): _normalized_schema_sql(MIGRATION_1_STATEMENTS[0]),
    ("table", "requests"): _normalized_schema_sql(MIGRATION_1_STATEMENTS[1]),
    ("index", "requests_root_state_idx"): _normalized_schema_sql(MIGRATION_1_STATEMENTS[2]),
    ("index", "requests_terminal_idx"): _normalized_schema_sql(MIGRATION_1_STATEMENTS[3]),
    ("table", "deliveries"): _normalized_schema_sql(MIGRATION_1_STATEMENTS[4]),
    ("index", "deliveries_request_idx"): _normalized_schema_sql(MIGRATION_1_STATEMENTS[5]),
    ("table", "sessions"): _normalized_schema_sql(MIGRATION_1_STATEMENTS[6]),
}

_EXPECTED_MIGRATION_2_SCHEMA_SQL = {
    **_EXPECTED_MIGRATION_1_SCHEMA_SQL,
    ("table", "confirmations"): _normalized_schema_sql(MIGRATION_2_STATEMENTS[0]),
}

_EXPECTED_COLUMNS = {
    "schema_migrations": (
        ("version", "INTEGER", 0, 1),
        ("applied_at_ms", "INTEGER", 1, 0),
    ),
    "requests": (
        ("request_id", "TEXT", 0, 1),
        ("root_key", "TEXT", 1, 0),
        ("request_hash", "TEXT", 1, 0),
        ("operation", "TEXT", 1, 0),
        ("operation_class", "TEXT", 1, 0),
        ("state", "TEXT", 1, 0),
        ("runtime_version", "TEXT", 1, 0),
        ("runtime_tag", "TEXT", 1, 0),
        ("runtime_asset_sha256", "TEXT", 1, 0),
        ("release_id", "TEXT", 1, 0),
        ("host_contract_sha256", "TEXT", 1, 0),
        ("dependency_lock_sha256", "TEXT", 1, 0),
        ("manifest_sha256", "TEXT", 1, 0),
        ("result_schema_id", "TEXT", 1, 0),
        ("recovery_schema_id", "TEXT", 0, 0),
        ("body_json", "BLOB", 1, 0),
        ("lineage_id", "TEXT", 0, 0),
        ("activation_id", "TEXT", 0, 0),
        ("result_json", "TEXT", 0, 0),
        ("error_json", "TEXT", 0, 0),
        ("resolution_json", "TEXT", 0, 0),
        ("created_at_ms", "INTEGER", 1, 0),
        ("updated_at_ms", "INTEGER", 1, 0),
        ("terminal_at_ms", "INTEGER", 0, 0),
    ),
    "deliveries": (
        ("delivery_id", "TEXT", 0, 1),
        ("request_id", "TEXT", 1, 0),
        ("delivery_kind", "TEXT", 1, 0),
        ("original_request_delivery_id", "TEXT", 1, 0),
        ("intended_at_ms", "INTEGER", 1, 0),
        ("published_at_ms", "INTEGER", 0, 0),
        ("rendered_inbox_sha256", "TEXT", 1, 0),
        ("rendered_inbox_size_bytes", "INTEGER", 1, 0),
        ("response_filename", "TEXT", 1, 0),
        ("response_at_ms", "INTEGER", 0, 0),
        ("response_sha256", "TEXT", 0, 0),
        ("response_size_bytes", "INTEGER", 0, 0),
        ("accepted_response_json", "BLOB", 0, 0),
        ("settlement_json", "BLOB", 0, 0),
    ),
    "sessions": (
        ("root_key", "TEXT", 0, 1),
        ("scenario_lineage_id", "TEXT", 1, 0),
        ("activation_id", "TEXT", 1, 0),
        ("build_number", "INTEGER", 1, 0),
        ("runtime_version", "TEXT", 1, 0),
        ("runtime_tag", "TEXT", 1, 0),
        ("runtime_asset_sha256", "TEXT", 1, 0),
        ("release_id", "TEXT", 1, 0),
        ("host_contract_sha256", "TEXT", 1, 0),
        ("dependency_lock_sha256", "TEXT", 1, 0),
        ("manifest_sha256", "TEXT", 1, 0),
        ("process_pid", "INTEGER", 1, 0),
        ("process_create_time", "REAL", 1, 0),
        ("validated_at_ms", "INTEGER", 1, 0),
    ),
    "confirmations": (
        ("token_sha256", "TEXT", 0, 1),
        ("root_key", "TEXT", 1, 0),
        ("operation", "TEXT", 1, 0),
        ("binding_format", "TEXT", 1, 0),
        ("binding_sha256", "TEXT", 1, 0),
        ("lineage_id", "TEXT", 1, 0),
        ("activation_id", "TEXT", 1, 0),
        ("expires_at_ms", "INTEGER", 1, 0),
        ("used_at_ms", "INTEGER", 0, 0),
    ),
}

_EXPECTED_FOREIGN_KEYS = {
    "schema_migrations": (),
    "requests": (),
    "deliveries": (
        (0, 0, "requests", "request_id", "request_id", "NO ACTION", "RESTRICT", "NONE"),
    ),
    "sessions": (),
    "confirmations": (),
}

_EXPECTED_INDEX_PROPERTIES = {
    "schema_migrations": {},
    "requests": {
        "requests_root_state_idx": (0, "c", 0),
        "requests_terminal_idx": (0, "c", 1),
    },
    "deliveries": {"deliveries_request_idx": (0, "c", 0)},
    "sessions": {},
    "confirmations": {},
}

_EXPECTED_INDEX_COLUMNS = {
    "requests_root_state_idx": ("root_key", "state", "updated_at_ms"),
    "requests_terminal_idx": ("terminal_at_ms",),
    "deliveries_request_idx": ("request_id", "intended_at_ms"),
}


_EXPECTED_MIGRATION_1_TABLES = frozenset(
    {"schema_migrations", "requests", "deliveries", "sessions"}
)
_EXPECTED_MIGRATION_2_TABLES = _EXPECTED_MIGRATION_1_TABLES | {"confirmations"}
_EXPECTED_INDEXES = frozenset(
    {"requests_root_state_idx", "requests_terminal_idx", "deliveries_request_idx"}
)


def _state_conflict(message: str) -> BridgeError:
    return BridgeError(ErrorCode.STATE_CONFLICT, message)


class StateDatabase:
    def __init__(self, path: Path) -> None:
        raw_path = cast(object, path)
        if not isinstance(raw_path, Path):
            raise BridgeError(ErrorCode.INVALID_ARGUMENT, "state database path must be a Path")
        if "\0" in str(path) or not path.name or path == path.parent:
            raise BridgeError(ErrorCode.INVALID_ARGUMENT, "state database path must be concrete")
        self._path = Path(path)

    @property
    def path(self) -> Path:
        return self._path

    def initialize(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._open()
        try:
            self._migrate(connection)
        finally:
            connection.close()

    @contextmanager
    def _transaction(self, *, write: bool) -> Generator[sqlite3.Connection]:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._open()
        try:
            self._migrate(connection)
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            try:
                yield connection
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()
        finally:
            connection.close()

    def _open(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._path,
            timeout=5.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout=5000")
            mode = self._enable_wal(connection)
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            synchronous = connection.execute("PRAGMA synchronous").fetchone()[0]
            foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]
            busy_timeout = connection.execute("PRAGMA busy_timeout").fetchone()[0]
            if str(mode).lower() != "wal":
                raise _state_conflict("SQLite WAL mode could not be enabled")
            if synchronous != 2 or foreign_keys != 1 or busy_timeout != 5000:
                raise _state_conflict("SQLite durability pragmas are not effective")
            return connection
        except BaseException:
            connection.close()
            raise

    @staticmethod
    def _enable_wal(connection: sqlite3.Connection) -> object:
        deadline = time.monotonic() + 5.0
        delay = 0.01
        while True:
            try:
                return connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            except sqlite3.OperationalError as error:
                message = str(error).lower()
                if (
                    "locked" not in message and "busy" not in message
                ) or time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.1)

    def _migrate(self, connection: sqlite3.Connection) -> None:
        connection.execute("BEGIN IMMEDIATE")
        try:
            objects = connection.execute(
                "SELECT type,name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            ).fetchall()
            tables = self._schema_names(connection, "table")
            indexes = self._schema_names(connection, "index")
            if not objects:
                for statement in MIGRATION_1_STATEMENTS:
                    connection.execute(statement)
                self._verify_migration_1_schema(connection)
                applied_at_ms = int(time.time() * 1000)
                connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at_ms) VALUES(1, ?)",
                    (applied_at_ms,),
                )
                for statement in MIGRATION_2_STATEMENTS:
                    connection.execute(statement)
                self._verify_migration_2_schema(connection)
                connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at_ms) VALUES(2, ?)",
                    (applied_at_ms,),
                )
            elif tables == _EXPECTED_MIGRATION_1_TABLES and indexes == _EXPECTED_INDEXES:
                self._verify_migration_1_schema(connection)
                self._verify_migration_history(connection, (1,))
                for statement in MIGRATION_2_STATEMENTS:
                    connection.execute(statement)
                self._verify_migration_2_schema(connection)
                connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at_ms) VALUES(2, ?)",
                    (int(time.time() * 1000),),
                )
            elif tables == _EXPECTED_MIGRATION_2_TABLES and indexes == _EXPECTED_INDEXES:
                self._verify_migration_2_schema(connection)
                self._verify_migration_history(connection, (1, 2))
            else:
                raise _state_conflict("SQLite state schema is not an exact migration 1 or 2")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    @staticmethod
    def _verify_migration_history(
        connection: sqlite3.Connection, expected_versions: tuple[int, ...]
    ) -> None:
        rows = connection.execute(
            "SELECT version, applied_at_ms FROM schema_migrations ORDER BY version"
        ).fetchall()
        if len(rows) != len(expected_versions) or any(
            type(row["version"]) is not int
            or row["version"] != version
            or type(row["applied_at_ms"]) is not int
            or row["applied_at_ms"] < 0
            for row, version in zip(rows, expected_versions, strict=True)
        ):
            raise _state_conflict("SQLite migration history is invalid")

    @staticmethod
    def _schema_names(connection: sqlite3.Connection, kind: str) -> frozenset[str]:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type=? AND name NOT LIKE 'sqlite_%'",
            (kind,),
        ).fetchall()
        names = frozenset(str(row["name"]) for row in rows)
        return names

    @staticmethod
    def _verify_migration_1_schema(connection: sqlite3.Connection) -> None:
        StateDatabase._verify_schema(
            connection,
            expected_sql=_EXPECTED_MIGRATION_1_SCHEMA_SQL,
            expected_tables=_EXPECTED_MIGRATION_1_TABLES,
            migration=1,
        )

    @staticmethod
    def _verify_migration_2_schema(connection: sqlite3.Connection) -> None:
        StateDatabase._verify_schema(
            connection,
            expected_sql=_EXPECTED_MIGRATION_2_SCHEMA_SQL,
            expected_tables=_EXPECTED_MIGRATION_2_TABLES,
            migration=2,
        )

    @staticmethod
    def _verify_schema(
        connection: sqlite3.Connection,
        *,
        expected_sql: dict[tuple[str, str], str],
        expected_tables: frozenset[str],
        migration: int,
    ) -> None:
        try:
            master_rows = connection.execute(
                "SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            ).fetchall()
            master_sql = {
                (row["type"], row["name"]): _normalized_schema_sql(row["sql"])
                for row in master_rows
            }
            if master_sql != expected_sql:
                raise _state_conflict(f"SQLite state schema DDL differs from migration {migration}")

            for table in expected_tables:
                expected_columns = _EXPECTED_COLUMNS[table]
                rows = connection.execute(f"PRAGMA table_xinfo({table})").fetchall()
                columns = tuple(
                    (row["name"], row["type"], row["notnull"], row["pk"]) for row in rows
                )
                metadata_is_exact = all(
                    row["cid"] == index and row["dflt_value"] is None and row["hidden"] == 0
                    for index, row in enumerate(rows)
                )
                if columns != expected_columns or not metadata_is_exact:
                    raise _state_conflict(
                        f"SQLite {table} columns differ from migration {migration}"
                    )

                foreign_keys = tuple(
                    tuple(row)
                    for row in connection.execute(f"PRAGMA foreign_key_list({table})").fetchall()
                )
                if foreign_keys != _EXPECTED_FOREIGN_KEYS[table]:
                    raise _state_conflict(
                        f"SQLite {table} foreign keys differ from migration {migration}"
                    )

                indexes = {
                    row["name"]: (row["unique"], row["origin"], row["partial"])
                    for row in connection.execute(f"PRAGMA index_list({table})").fetchall()
                    if not str(row["name"]).startswith("sqlite_")
                }
                if indexes != _EXPECTED_INDEX_PROPERTIES[table]:
                    raise _state_conflict(
                        f"SQLite {table} indexes differ from migration {migration}"
                    )

            for index, expected_columns in _EXPECTED_INDEX_COLUMNS.items():
                rows = connection.execute(f"PRAGMA index_info({index})").fetchall()
                columns = tuple(row["name"] for row in rows)
                if columns != expected_columns or any(
                    row["seqno"] != position for position, row in enumerate(rows)
                ):
                    raise _state_conflict(f"SQLite {index} definition differs from migration 1")
        except BridgeError:
            raise
        except (sqlite3.DatabaseError, KeyError, TypeError, ValueError) as error:
            raise _state_conflict("SQLite state schema cannot be verified") from error
