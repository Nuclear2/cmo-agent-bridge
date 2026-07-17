from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast
from unittest.mock import Mock
from uuid import UUID

import pytest
from pydantic import JsonValue

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.operations.models import BridgeStatusWireArgs
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.models import RequestBody
from cmo_agent_bridge.protocol.response import parse_inst_response
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CancelledSettlement,
    CompletedSettlement,
    RejectedSettlement,
    ResponseArtifact,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state import request_ledger as request_ledger_module
from cmo_agent_bridge.state import sqlite as sqlite_module
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    PendingExchange,
    PendingJournal,
)
from cmo_agent_bridge.state.request_ledger import (
    DeliveryRecord,
    RequestLedger,
    RequestRecord,
)
from cmo_agent_bridge.state.sqlite import StateDatabase

if TYPE_CHECKING:
    from tests.unit.state.conftest import SemanticHarness


class _DeliveryLister(Protocol):
    def list_deliveries(self, request_id: UUID) -> tuple[DeliveryRecord, ...]: ...


def _list_deliveries(ledger: RequestLedger, request_id: UUID) -> tuple[DeliveryRecord, ...]:
    return cast(_DeliveryLister, ledger).list_deliveries(request_id)


def _cancel_intent(
    original: DeliveryIntent,
    *,
    delivery_id: UUID,
    intended_at_ms: int,
    digest_character: str,
) -> DeliveryIntent:
    return DeliveryIntent.model_validate(
        {
            **original.model_dump(mode="python"),
            "delivery_id": delivery_id,
            "delivery_kind": "cancel",
            "original_request_delivery_id": original.delivery_id,
            "intended_at_ms": intended_at_ms,
            "rendered_inbox_sha256": digest_character * 64,
        }
    )


@pytest.fixture
def state_database(tmp_path: Path) -> StateDatabase:
    return StateDatabase(tmp_path / "state" / "bridge.sqlite3")


@pytest.fixture
def request_ledger(
    state_database: StateDatabase,
    manifest_catalog: ManifestCatalog,
) -> RequestLedger:
    return RequestLedger(state_database, manifest_catalog)


@pytest.fixture
def prepared_record(valid_journal: PendingJournal) -> RequestRecord:
    exchange = valid_journal.original
    return RequestRecord(
        request_id=exchange.request_id,
        root_key=valid_journal.header.root_key,
        request_hash=exchange.request_hash,
        operation=exchange.operation,
        operation_class=exchange.effective_class,
        state=HostRequestState.PREPARED,
        runtime_snapshot=exchange.runtime_snapshot,
        result_schema_id=exchange.result_schema_id,
        recovery_schema_id=exchange.recovery_schema_id,
        body_json=exchange.body_json.encode("utf-8"),
        lineage_id=exchange.expected_lineage_id,
        activation_id=exchange.expected_activation_id,
        result_json=None,
        error_json=None,
        resolution_json=None,
        created_at_ms=exchange.created_at_ms,
        updated_at_ms=exchange.updated_at_ms,
        terminal_at_ms=None,
    )


def test_database_and_store_construction_performs_zero_sqlite_io(
    tmp_path: Path,
    manifest_catalog: ManifestCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = Mock(side_effect=AssertionError("constructors must not connect"))
    monkeypatch.setattr(sqlite3, "connect", connect)

    database = StateDatabase(tmp_path / "state.sqlite3")
    RequestLedger(database, manifest_catalog)

    connect.assert_not_called()
    assert not database.path.exists()


def test_migration_three_has_exact_tables_columns_indexes_and_history(
    state_database: StateDatabase,
) -> None:
    state_database.initialize()

    with sqlite3.connect(state_database.path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        }
        indexes = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            )
        }
        assert tables == {
            "schema_migrations",
            "requests",
            "deliveries",
            "sessions",
            "confirmations",
            "operation_queue",
            "sqlite_sequence",
        }
        assert indexes == {
            "requests_root_state_idx",
            "requests_terminal_idx",
            "deliveries_request_idx",
            "operation_queue_state_sequence_idx",
            "operation_queue_root_state_sequence_idx",
        }
        assert connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall() == [(1,), (2,), (3,)]
        delivery_columns = [
            row[1] for row in connection.execute("PRAGMA table_info(deliveries)").fetchall()
        ]
        assert delivery_columns == [
            "delivery_id",
            "request_id",
            "delivery_kind",
            "original_request_delivery_id",
            "intended_at_ms",
            "published_at_ms",
            "rendered_inbox_sha256",
            "rendered_inbox_size_bytes",
            "response_filename",
            "response_at_ms",
            "response_sha256",
            "response_size_bytes",
            "accepted_response_json",
            "settlement_json",
        ]
        request_columns = [
            row[1] for row in connection.execute("PRAGMA table_info(requests)").fetchall()
        ]
        assert request_columns == [
            "request_id",
            "root_key",
            "request_hash",
            "operation",
            "operation_class",
            "state",
            "runtime_version",
            "runtime_tag",
            "runtime_asset_sha256",
            "release_id",
            "host_contract_sha256",
            "dependency_lock_sha256",
            "manifest_sha256",
            "result_schema_id",
            "recovery_schema_id",
            "body_json",
            "lineage_id",
            "activation_id",
            "result_json",
            "error_json",
            "resolution_json",
            "created_at_ms",
            "updated_at_ms",
            "terminal_at_ms",
        ]
        session_columns = [
            row[1] for row in connection.execute("PRAGMA table_info(sessions)").fetchall()
        ]
        assert session_columns == [
            "root_key",
            "scenario_lineage_id",
            "activation_id",
            "build_number",
            "runtime_version",
            "runtime_tag",
            "runtime_asset_sha256",
            "release_id",
            "host_contract_sha256",
            "dependency_lock_sha256",
            "manifest_sha256",
            "process_pid",
            "process_create_time",
            "validated_at_ms",
        ]
        confirmation_columns = [
            row[1] for row in connection.execute("PRAGMA table_info(confirmations)").fetchall()
        ]
        assert confirmation_columns == [
            "token_sha256",
            "root_key",
            "operation",
            "binding_format",
            "binding_sha256",
            "lineage_id",
            "activation_id",
            "expires_at_ms",
            "used_at_ms",
        ]
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2

    with state_database._transaction(write=False) as configured:  # pyright: ignore[reportPrivateUsage]
        assert configured.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert configured.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert configured.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert configured.execute("PRAGMA busy_timeout").fetchone()[0] == 5000


def test_failed_migration_rolls_back_every_schema_object(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "rollback.sqlite3"
    database = StateDatabase(path)
    original = sqlite_module.MIGRATION_1_STATEMENTS
    monkeypatch.setattr(
        sqlite_module,
        "MIGRATION_1_STATEMENTS",
        (original[0], "CREATE TABLE broken("),
    )
    with pytest.raises(sqlite3.OperationalError):
        database.initialize()
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','index') "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
            == []
        )


def test_migration_enforces_foreign_key_for_missing_delivery_owner(
    state_database: StateDatabase,
) -> None:
    state_database.initialize()
    with sqlite3.connect(state_database.path) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        with pytest.raises(sqlite3.IntegrityError) as caught:
            connection.execute(
                """
                INSERT INTO deliveries(
                    delivery_id,request_id,delivery_kind,original_request_delivery_id,
                    intended_at_ms,published_at_ms,rendered_inbox_sha256,
                    rendered_inbox_size_bytes,response_filename
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    "11111111-1111-4111-8111-111111111111",
                    "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                    "request",
                    "11111111-1111-4111-8111-111111111111",
                    1,
                    None,
                    "e" * 64,
                    1,
                    "CMOAgentBridge_Response_aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.inst",
                ),
            )
    assert "FOREIGN KEY constraint failed" in str(caught.value)


def test_prepared_request_and_delivery_round_trip_are_idempotent(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(intent)
    request_ledger.insert_delivery(intent)

    assert request_ledger.get_request(prepared_record.request_id) == prepared_record
    assert request_ledger.get_delivery(intent.delivery_id) == DeliveryRecord(
        delivery_id=intent.delivery_id,
        request_id=intent.request_id,
        delivery_kind=intent.delivery_kind,
        original_request_delivery_id=intent.original_request_delivery_id,
        intended_at_ms=intent.intended_at_ms,
        published_at_ms=None,
        rendered_inbox_sha256=intent.rendered_inbox_sha256,
        rendered_inbox_size_bytes=intent.rendered_inbox_size_bytes,
        response_filename=intent.response_filename,
        response_artifact=None,
        settlement=None,
    )


def test_list_deliveries_requires_an_exact_request_uuid(
    request_ledger: RequestLedger,
) -> None:
    with pytest.raises(BridgeError) as caught:
        _list_deliveries(
            request_ledger,
            cast(UUID, "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"),
        )
    assert caught.value.to_payload() == {
        "code": ErrorCode.INVALID_ARGUMENT.value,
        "message": "request ID must be an exact UUID",
        "details": {},
    }


def test_list_deliveries_returns_empty_tuple_for_request_without_deliveries(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
) -> None:
    request_ledger.insert_prepared(prepared_record)

    assert _list_deliveries(request_ledger, prepared_record.request_id) == ()


def test_list_deliveries_returns_complete_records_in_stable_intent_and_id_order(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    original = valid_journal.original.delivery_intents[0]
    later_id = UUID("33333333-3333-4333-8333-333333333333")
    earlier_id = UUID("22222222-2222-4222-8222-222222222222")
    inserted_first = _cancel_intent(
        original,
        delivery_id=later_id,
        intended_at_ms=150,
        digest_character="3",
    )
    inserted_second = _cancel_intent(
        original,
        delivery_id=earlier_id,
        intended_at_ms=150,
        digest_character="2",
    )
    request_ledger.insert_prepared(prepared_record)
    for intent in (original, inserted_first, inserted_second):
        request_ledger.insert_delivery(intent)
    request_ledger.mark_delivery_published(original.delivery_id, published_at_ms=101)
    recorded_original = request_ledger.record_response(completed_artifact)

    assert _list_deliveries(request_ledger, prepared_record.request_id) == (
        recorded_original,
        DeliveryRecord(
            delivery_id=inserted_second.delivery_id,
            request_id=inserted_second.request_id,
            delivery_kind="cancel",
            original_request_delivery_id=original.delivery_id,
            intended_at_ms=150,
            published_at_ms=None,
            rendered_inbox_sha256="2" * 64,
            rendered_inbox_size_bytes=inserted_second.rendered_inbox_size_bytes,
            response_filename=inserted_second.response_filename,
            response_artifact=None,
            settlement=None,
        ),
        DeliveryRecord(
            delivery_id=inserted_first.delivery_id,
            request_id=inserted_first.request_id,
            delivery_kind="cancel",
            original_request_delivery_id=original.delivery_id,
            intended_at_ms=150,
            published_at_ms=None,
            rendered_inbox_sha256="3" * 64,
            rendered_inbox_size_bytes=inserted_first.rendered_inbox_size_bytes,
            response_filename=inserted_first.response_filename,
            response_artifact=None,
            settlement=None,
        ),
    )


def test_list_deliveries_fails_closed_when_any_selected_row_is_malformed(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
) -> None:
    original = valid_journal.original.delivery_intents[0]
    valid_cancel = _cancel_intent(
        original,
        delivery_id=UUID("22222222-2222-4222-8222-222222222222"),
        intended_at_ms=150,
        digest_character="2",
    )
    malformed_cancel = _cancel_intent(
        original,
        delivery_id=UUID("33333333-3333-4333-8333-333333333333"),
        intended_at_ms=151,
        digest_character="3",
    )
    request_ledger.insert_prepared(prepared_record)
    for intent in (original, valid_cancel, malformed_cancel):
        request_ledger.insert_delivery(intent)
    with sqlite3.connect(state_database.path) as connection:
        connection.execute(
            "UPDATE deliveries SET rendered_inbox_sha256=? WHERE delivery_id=?",
            ("z" * 64, str(malformed_cancel.delivery_id)),
        )
        connection.commit()

    with pytest.raises(BridgeError) as caught:
        _list_deliveries(request_ledger, prepared_record.request_id)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_list_deliveries_uses_a_read_transaction_without_row_mutation(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(intent)
    with sqlite3.connect(state_database.path) as connection:
        before = (
            connection.execute("SELECT * FROM requests ORDER BY request_id").fetchall(),
            connection.execute(
                "SELECT * FROM deliveries ORDER BY intended_at_ms,delivery_id"
            ).fetchall(),
        )
    transaction = Mock(wraps=state_database._transaction)  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(state_database, "_transaction", transaction)

    _list_deliveries(request_ledger, prepared_record.request_id)

    transaction.assert_called_once_with(write=False)
    with sqlite3.connect(state_database.path) as connection:
        after = (
            connection.execute("SELECT * FROM requests ORDER BY request_id").fetchall(),
            connection.execute(
                "SELECT * FROM deliveries ORDER BY intended_at_ms,delivery_id"
            ).fetchall(),
        )
    assert after == before


def test_insert_delivery_revalidates_owner_current_registry_binding(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    request_ledger.insert_prepared(prepared_record)
    with sqlite3.connect(state_database.path) as connection:
        connection.execute(
            "UPDATE requests SET operation_class='read' WHERE request_id=?",
            (str(prepared_record.request_id),),
        )
        connection.commit()

    with pytest.raises(BridgeError) as caught:
        request_ledger.insert_delivery(intent)
    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert request_ledger.get_delivery(intent.delivery_id) is None


def test_duplicate_request_id_distinguishes_hash_reuse_from_other_drift(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
) -> None:
    request_ledger.insert_prepared(prepared_record)
    with pytest.raises(BridgeError) as reused:
        request_ledger.insert_prepared(
            prepared_record.model_copy(update={"request_hash": "f" * 64})
        )
    assert reused.value.code is ErrorCode.REQUEST_ID_REUSED

    with pytest.raises(BridgeError) as drift:
        request_ledger.insert_prepared(
            prepared_record.model_copy(update={"updated_at_ms": prepared_record.updated_at_ms + 1})
        )
    assert drift.value.code is ErrorCode.STATE_CONFLICT


@pytest.mark.parametrize("stored_hash", ["z" * 64, "f" * 64])
def test_duplicate_request_validates_malformed_physical_row_before_reuse_classification(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    stored_hash: str,
) -> None:
    request_ledger.insert_prepared(prepared_record)
    with sqlite3.connect(state_database.path) as connection:
        connection.execute(
            "UPDATE requests SET request_hash=? WHERE request_id=?",
            (stored_hash, str(prepared_record.request_id)),
        )
        connection.commit()

    with pytest.raises(BridgeError) as caught:
        request_ledger.insert_prepared(prepared_record)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_delivery_publication_is_one_way_cas(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(intent)

    published = request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=101)
    assert published.published_at_ms == 101
    with pytest.raises(BridgeError) as repeated:
        request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=101)
    assert repeated.value.code is ErrorCode.STATE_CONFLICT


def test_record_response_fills_five_columns_together_and_semantically_reloads(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(intent)
    request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=101)

    recorded = request_ledger.record_response(completed_artifact)
    assert recorded.response_artifact == completed_artifact
    assert recorded.settlement == completed_artifact.accepted_response.settlement
    assert request_ledger.record_response(completed_artifact) == recorded
    with sqlite3.connect(state_database.path) as connection:
        row = connection.execute(
            "SELECT response_at_ms,response_sha256,response_size_bytes,"
            "accepted_response_json,settlement_json FROM deliveries WHERE delivery_id=?",
            (str(intent.delivery_id),),
        ).fetchone()
        assert all(value is not None for value in row)
        assert isinstance(row[3], bytes)
        assert isinstance(row[4], bytes)

    with pytest.raises(BridgeError) as drift:
        request_ledger.record_response(completed_artifact.model_copy(update={"sha256": "0" * 64}))
    assert drift.value.code is ErrorCode.STATE_CONFLICT


def test_record_response_preserves_state_conflict_for_malformed_durable_owner(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(intent)
    request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=101)
    with sqlite3.connect(state_database.path) as connection:
        connection.execute(
            "UPDATE requests SET runtime_tag='forged' WHERE request_id=?",
            (str(prepared_record.request_id),),
        )
        connection.commit()

    with pytest.raises(BridgeError) as caught:
        request_ledger.record_response(completed_artifact)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_record_response_maps_invalid_incoming_semantics_to_protocol_error(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(intent)
    request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=101)
    accepted = completed_artifact.accepted_response
    assert accepted.settlement is not None
    invalid_result = {"unit_guid": "UNIT-ONLY"}
    invalid_accepted = AcceptedResponse.model_validate(
        {
            **accepted.model_dump(mode="python"),
            "envelope": accepted.envelope.model_copy(update={"result": invalid_result}),
            "settlement": accepted.settlement.model_copy(update={"result": invalid_result}),
        }
    )
    invalid_artifact = completed_artifact.model_copy(update={"accepted_response": invalid_accepted})

    with pytest.raises(BridgeError) as caught:
        request_ledger.record_response(invalid_artifact)
    assert caught.value.code is ErrorCode.PROTOCOL_ERROR
    stored = request_ledger.get_delivery(intent.delivery_id)
    assert stored is not None
    assert stored.response_artifact is None


def test_exact_delivery_intent_retry_remains_idempotent_after_response_acceptance(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    published_intent = intent.model_copy(update={"published_at_ms": 101})
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(intent)
    request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=101)
    request_ledger.record_response(completed_artifact)

    request_ledger.insert_delivery(published_intent)
    with pytest.raises(BridgeError) as drift:
        request_ledger.insert_delivery(
            published_intent.model_copy(update={"rendered_inbox_sha256": "0" * 64})
        )
    assert drift.value.code is ErrorCode.STATE_CONFLICT


def test_delivery_with_accepted_response_requires_durable_publication_marker(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(intent)
    request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=101)
    request_ledger.record_response(completed_artifact)
    with sqlite3.connect(state_database.path) as connection:
        connection.execute(
            "UPDATE deliveries SET published_at_ms=NULL WHERE delivery_id=?",
            (str(intent.delivery_id),),
        )
        connection.commit()

    with pytest.raises(BridgeError) as caught:
        request_ledger.get_delivery(intent.delivery_id)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_no_settlement_is_literal_null_blob_not_sql_null(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
    no_settlement_artifact: ResponseArtifact,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(intent)
    request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=101)
    record = request_ledger.record_response(no_settlement_artifact)
    assert record.settlement is None
    with sqlite3.connect(state_database.path) as connection:
        value, kind = connection.execute(
            "SELECT settlement_json,typeof(settlement_json) FROM deliveries WHERE delivery_id=?",
            (str(intent.delivery_id),),
        ).fetchone()
    assert value == b"null"
    assert kind == "blob"


def test_delivery_response_check_rejects_partial_group(
    state_database: StateDatabase,
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
) -> None:
    request_ledger.insert_prepared(prepared_record)
    with sqlite3.connect(state_database.path) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        with pytest.raises(sqlite3.IntegrityError) as caught:
            connection.execute(
                "INSERT INTO deliveries(delivery_id,request_id,delivery_kind,"
                "original_request_delivery_id,intended_at_ms,published_at_ms,"
                "rendered_inbox_sha256,rendered_inbox_size_bytes,response_filename,response_at_ms) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    "11111111-1111-4111-8111-111111111111",
                    "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                    "request",
                    "11111111-1111-4111-8111-111111111111",
                    1,
                    None,
                    "e" * 64,
                    1,
                    "CMOAgentBridge_Response_aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.inst",
                    2,
                ),
            )
    assert "CHECK constraint failed" in str(caught.value)


def test_conditional_transition_enforces_terminal_json_and_time_invariants(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
) -> None:
    request_ledger.insert_prepared(prepared_record)
    completed = request_ledger.transition(
        prepared_record.request_id,
        expected_states=frozenset({HostRequestState.PREPARED}),
        new_state=HostRequestState.COMPLETED,
        updated_at_ms=200,
        terminal_at_ms=200,
        result_json='{"ok":true}',
    )
    assert completed.state is HostRequestState.COMPLETED
    assert completed.result_json == '{"ok":true}'
    with pytest.raises(BridgeError) as stale:
        request_ledger.transition(
            prepared_record.request_id,
            expected_states=frozenset({HostRequestState.PREPARED}),
            new_state=HostRequestState.REJECTED,
            updated_at_ms=201,
            terminal_at_ms=201,
            error_json='{"code":"bad"}',
        )
    assert stale.value.code is ErrorCode.STATE_CONFLICT


INVALID_TRANSITION_CHANGES: tuple[dict[str, object], ...] = (
    {"expected_states": frozenset[HostRequestState]()},
    {"updated_at_ms": 99},
    {"terminal_at_ms": None},
    {"result_json": '{"b":2, "a":1}'},
    {"result_json": '{"duplicate":1,"duplicate":2}'},
)


@pytest.mark.parametrize(
    "changes",
    INVALID_TRANSITION_CHANGES,
)
def test_transition_rejects_empty_state_time_terminal_and_noncanonical_json(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
    changes: dict[str, object],
) -> None:
    request_ledger.insert_prepared(prepared_record)
    arguments: dict[str, object] = {
        "expected_states": frozenset({HostRequestState.PREPARED}),
        "new_state": HostRequestState.COMPLETED,
        "updated_at_ms": 200,
        "terminal_at_ms": 200,
        "result_json": '{"ok":true}',
    }
    arguments.update(changes)
    with pytest.raises(BridgeError) as caught:
        request_ledger.transition(prepared_record.request_id, **arguments)  # type: ignore[arg-type]
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_unresolved_release_ids_include_quarantine_and_exclude_terminal(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
) -> None:
    request_ledger.insert_prepared(prepared_record)
    assert request_ledger.unresolved_release_ids() == frozenset(
        {prepared_record.runtime_snapshot.release_id}
    )
    request_ledger.transition(
        prepared_record.request_id,
        expected_states=frozenset({HostRequestState.PREPARED}),
        new_state=HostRequestState.CANCELLED,
        updated_at_ms=200,
        terminal_at_ms=200,
    )
    assert request_ledger.unresolved_release_ids() == frozenset()


def test_malformed_response_blob_fails_closed(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(intent)
    request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=101)
    request_ledger.record_response(completed_artifact)
    with sqlite3.connect(state_database.path) as connection:
        connection.execute(
            "UPDATE deliveries SET accepted_response_json=? WHERE delivery_id=?",
            (sqlite3.Binary(b'{"duplicate":1,"duplicate":2}'), str(intent.delivery_id)),
        )
        connection.commit()
    with pytest.raises(BridgeError) as corrupt:
        request_ledger.get_delivery(intent.delivery_id)
    assert corrupt.value.code is ErrorCode.STATE_CONFLICT


def test_request_reload_maps_recursive_durable_json_failure_to_state_conflict(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_ledger.insert_prepared(prepared_record)
    monkeypatch.setattr(
        request_ledger_module,
        "_parse_duplicate_free_json",
        Mock(side_effect=RecursionError("deep durable JSON")),
    )

    with pytest.raises(BridgeError) as caught:
        request_ledger.get_request(prepared_record.request_id)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_delivery_reload_rejects_noncanonical_typed_uuid_text_without_rewrite(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(intent)
    request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=101)
    request_ledger.record_response(completed_artifact)
    with sqlite3.connect(state_database.path) as connection:
        blob = connection.execute(
            "SELECT accepted_response_json FROM deliveries WHERE delivery_id=?",
            (str(intent.delivery_id),),
        ).fetchone()[0]
        poisoned = blob.replace(
            str(intent.request_id).encode(), str(intent.request_id).upper().encode()
        )
        connection.execute(
            "UPDATE deliveries SET accepted_response_json=? WHERE delivery_id=?",
            (sqlite3.Binary(poisoned), str(intent.delivery_id)),
        )
        connection.commit()
    with pytest.raises(BridgeError) as caught:
        request_ledger.get_delivery(intent.delivery_id)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_delivery_reload_binds_accepted_envelope_to_the_physical_row(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    first = valid_journal.original.delivery_intents[0]
    second_id = UUID("22222222-2222-4222-8222-222222222222")
    second = DeliveryIntent.model_validate(
        {
            **first.model_dump(mode="python"),
            "delivery_id": second_id,
            "original_request_delivery_id": second_id,
            "intended_at_ms": 102,
            "rendered_inbox_sha256": "1" * 64,
        }
    )
    second_envelope = completed_artifact.accepted_response.envelope.model_copy(
        update={"delivery_id": second_id}
    )
    second_accepted = completed_artifact.accepted_response.model_copy(
        update={"envelope": second_envelope}
    )
    second_artifact = completed_artifact.model_copy(
        update={"sha256": "2" * 64, "accepted_response": second_accepted}
    )
    request_ledger.insert_prepared(prepared_record)
    for intent, artifact, published_at in (
        (first, completed_artifact, 101),
        (second, second_artifact, 102),
    ):
        request_ledger.insert_delivery(intent)
        request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=published_at)
        request_ledger.record_response(artifact)

    with sqlite3.connect(state_database.path) as connection:
        second_blob = connection.execute(
            "SELECT accepted_response_json FROM deliveries WHERE delivery_id=?",
            (str(second_id),),
        ).fetchone()[0]
        connection.execute(
            "UPDATE deliveries SET accepted_response_json=? WHERE delivery_id=?",
            (second_blob, str(first.delivery_id)),
        )
        connection.commit()
    with pytest.raises(BridgeError) as caught:
        request_ledger.get_delivery(first.delivery_id)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_delivery_reload_rechecks_request_and_cancel_original_identity(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
) -> None:
    request = valid_journal.original.delivery_intents[0]
    cancel_id = UUID("22222222-2222-4222-8222-222222222222")
    cancel = DeliveryIntent.model_validate(
        {
            **request.model_dump(mode="python"),
            "delivery_id": cancel_id,
            "delivery_kind": "cancel",
            "original_request_delivery_id": request.delivery_id,
            "intended_at_ms": 102,
            "rendered_inbox_sha256": "1" * 64,
        }
    )
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(request)
    request_ledger.insert_delivery(cancel)

    with sqlite3.connect(state_database.path) as connection:
        connection.execute(
            "UPDATE deliveries SET original_request_delivery_id=? WHERE delivery_id=?",
            (str(cancel_id), str(request.delivery_id)),
        )
        connection.commit()
    with pytest.raises(BridgeError) as request_error:
        request_ledger.get_delivery(request.delivery_id)
    assert request_error.value.code is ErrorCode.STATE_CONFLICT

    with sqlite3.connect(state_database.path) as connection:
        connection.execute(
            "UPDATE deliveries SET original_request_delivery_id=? WHERE delivery_id=?",
            (str(request.delivery_id), str(request.delivery_id)),
        )
        connection.execute(
            "UPDATE deliveries SET original_request_delivery_id=? WHERE delivery_id=?",
            (str(cancel_id), str(cancel_id)),
        )
        connection.commit()
    with pytest.raises(BridgeError) as cancel_error:
        request_ledger.get_delivery(cancel_id)
    assert cancel_error.value.code is ErrorCode.STATE_CONFLICT


def test_cancel_reload_revalidates_referenced_request_delivery_self_identity(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
) -> None:
    request = valid_journal.original.delivery_intents[0]
    cancel_id = UUID("22222222-2222-4222-8222-222222222222")
    cancel = DeliveryIntent.model_validate(
        {
            **request.model_dump(mode="python"),
            "delivery_id": cancel_id,
            "delivery_kind": "cancel",
            "original_request_delivery_id": request.delivery_id,
            "intended_at_ms": 102,
            "rendered_inbox_sha256": "1" * 64,
        }
    )
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(request)
    request_ledger.insert_delivery(cancel)
    with sqlite3.connect(state_database.path) as connection:
        connection.execute(
            "UPDATE deliveries SET original_request_delivery_id=? WHERE delivery_id=?",
            (str(cancel_id), str(request.delivery_id)),
        )
        connection.commit()

    with pytest.raises(BridgeError) as caught:
        request_ledger.get_delivery(cancel_id)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


@pytest.mark.parametrize("malformed_time", [100.5, "not-an-integer"])
def test_transition_fails_closed_on_malformed_persisted_times(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    prepared_record: RequestRecord,
    malformed_time: object,
) -> None:
    request_ledger.insert_prepared(prepared_record)
    with sqlite3.connect(state_database.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints=ON")
        connection.execute(
            "UPDATE requests SET updated_at_ms=? WHERE request_id=?",
            (malformed_time, str(prepared_record.request_id)),
        )
        connection.commit()
    with pytest.raises(BridgeError) as caught:
        request_ledger.transition(
            prepared_record.request_id,
            expected_states=frozenset({HostRequestState.PREPARED}),
            new_state=HostRequestState.PUBLISHED,
            updated_at_ms=200,
        )
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_prune_small_epoch_is_still_a_lazy_initializing_operation(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
) -> None:
    assert not state_database.path.exists()
    assert request_ledger.prune_terminal(now_ms=1) == 0
    assert state_database.path.exists()
    with sqlite3.connect(state_database.path) as connection:
        assert connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall() == [(1,), (2,), (3,)]


def test_schema_drift_fails_closed_on_next_actual_operation(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
) -> None:
    state_database.initialize()
    with sqlite3.connect(state_database.path) as connection:
        connection.execute("CREATE TABLE unexpected(value INTEGER)")
        connection.commit()
    with pytest.raises(BridgeError) as caught:
        request_ledger.unresolved_release_ids()
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_same_named_schema_with_altered_column_and_partial_index_fails_closed(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
) -> None:
    state_database.initialize()
    with sqlite3.connect(state_database.path) as connection:
        connection.execute("ALTER TABLE sessions ADD COLUMN executable TEXT")
        connection.execute("DROP INDEX requests_terminal_idx")
        connection.execute("CREATE INDEX requests_terminal_idx ON requests(terminal_at_ms)")
        connection.commit()

    with pytest.raises(BridgeError) as caught:
        request_ledger.unresolved_release_ids()
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_extra_trigger_and_view_schema_objects_fail_closed(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
) -> None:
    state_database.initialize()
    with sqlite3.connect(state_database.path) as connection:
        connection.execute("CREATE VIEW request_ids AS SELECT request_id FROM requests")
        connection.execute(
            "CREATE TRIGGER session_insert_probe AFTER INSERT ON sessions BEGIN SELECT 1; END"
        )
        connection.commit()

    with pytest.raises(BridgeError) as caught:
        request_ledger.unresolved_release_ids()
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_reconciliation_updates_two_rows_atomically_or_rolls_both_back(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
    reconcile_attempt: PendingExchange,
) -> None:
    attempt_exchange = reconcile_attempt
    attempt = RequestRecord(
        request_id=attempt_exchange.request_id,
        root_key=prepared_record.root_key,
        request_hash=attempt_exchange.request_hash,
        operation=attempt_exchange.operation,
        operation_class=attempt_exchange.effective_class,
        state=HostRequestState.PREPARED,
        runtime_snapshot=attempt_exchange.runtime_snapshot,
        result_schema_id=attempt_exchange.result_schema_id,
        recovery_schema_id=attempt_exchange.recovery_schema_id,
        body_json=attempt_exchange.body_json.encode("utf-8"),
        lineage_id=attempt_exchange.expected_lineage_id,
        activation_id=attempt_exchange.expected_activation_id,
        result_json=None,
        error_json=None,
        resolution_json=None,
        created_at_ms=attempt_exchange.created_at_ms,
        updated_at_ms=attempt_exchange.updated_at_ms,
        terminal_at_ms=None,
    )
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_prepared(attempt)
    resolution = '{"disposition":"applied"}'
    with pytest.raises(BridgeError) as rollback:
        request_ledger.resolve_reconciliation(
            attempt_request_id=attempt.request_id,
            original_request_id=prepared_record.request_id,
            expected_attempt_states=frozenset({HostRequestState.PREPARED}),
            expected_original_states=frozenset({HostRequestState.PUBLISHED}),
            resolution_json=resolution,
            completed_at_ms=400,
        )
    assert rollback.value.code is ErrorCode.STATE_CONFLICT
    stored_attempt = request_ledger.get_request(attempt.request_id)
    stored_original = request_ledger.get_request(prepared_record.request_id)
    assert stored_attempt is not None
    assert stored_original is not None
    assert stored_attempt.state is HostRequestState.PREPARED
    assert stored_original.state is HostRequestState.PREPARED

    completed, resolved = request_ledger.resolve_reconciliation(
        attempt_request_id=attempt.request_id,
        original_request_id=prepared_record.request_id,
        expected_attempt_states=frozenset({HostRequestState.PREPARED}),
        expected_original_states=frozenset({HostRequestState.PREPARED}),
        resolution_json=resolution,
        completed_at_ms=400,
    )
    assert completed.state is HostRequestState.COMPLETED
    assert resolved.state is HostRequestState.RESOLVED
    assert resolved.resolution_json == resolution


def test_prune_retains_recent_unresolved_and_configured_newest_boundary(
    request_ledger: RequestLedger,
    prepared_record: RequestRecord,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert request_ledger_module._NEWEST_RETENTION_COUNT == 10_000  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(request_ledger_module, "_NEWEST_RETENTION_COUNT", 1)
    now = 4_000_000_000
    ids = [UUID(int=index + 10) for index in range(5)]
    for index, request_id in enumerate(ids):
        record = prepared_record.model_copy(
            update={
                "request_id": request_id,
                "created_at_ms": 100 + index,
                "updated_at_ms": 100 + index,
            }
        )
        request_ledger.insert_prepared(record)
    for request_id in ids[:3]:
        request_ledger.transition(
            request_id,
            expected_states=frozenset({HostRequestState.PREPARED}),
            new_state=HostRequestState.CANCELLED,
            updated_at_ms=200,
            terminal_at_ms=200,
        )
    request_ledger.transition(
        ids[3],
        expected_states=frozenset({HostRequestState.PREPARED}),
        new_state=HostRequestState.CANCELLED,
        updated_at_ms=now - 1,
        terminal_at_ms=now - 1,
    )

    assert request_ledger.prune_terminal(now_ms=now) == 3
    assert request_ledger.get_request(ids[0]) is None
    assert request_ledger.get_request(ids[1]) is None
    assert request_ledger.get_request(ids[2]) is None
    assert request_ledger.get_request(ids[3]) is not None
    assert request_ledger.get_request(ids[4]) is not None


def test_request_and_delivery_models_have_exact_fields() -> None:
    assert list(RequestRecord.model_fields) == [
        "request_id",
        "root_key",
        "request_hash",
        "operation",
        "operation_class",
        "state",
        "runtime_snapshot",
        "result_schema_id",
        "recovery_schema_id",
        "body_json",
        "lineage_id",
        "activation_id",
        "result_json",
        "error_json",
        "resolution_json",
        "created_at_ms",
        "updated_at_ms",
        "terminal_at_ms",
    ]
    assert list(DeliveryRecord.model_fields) == [
        "delivery_id",
        "request_id",
        "delivery_kind",
        "original_request_delivery_id",
        "intended_at_ms",
        "published_at_ms",
        "rendered_inbox_sha256",
        "rendered_inbox_size_bytes",
        "response_filename",
        "response_artifact",
        "settlement",
    ]


def _remove_not_started_evidence(tree: dict[str, object]) -> None:
    envelope = cast(dict[str, object], tree["envelope"])
    envelope_error = cast(dict[str, object], envelope["error"])
    envelope_error["mutation_not_started"] = None
    settlement = cast(dict[str, object], tree["settlement"])
    settlement_error = cast(dict[str, object], settlement["error"])
    settlement_error["mutation_not_started"] = None


def test_row_revalidation_rejects_removed_eligible_not_started_evidence(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    valid_journal: PendingJournal,
    semantic_harness: SemanticHarness,
) -> None:
    exchange = valid_journal.original
    envelope = semantic_harness.response_envelope(
        exchange,
        delivery_id=exchange.delivery_intents[0].delivery_id,
        result=None,
        error=semantic_harness.not_started_error(exchange),
    )
    artifact = semantic_harness.artifact_from_parser(
        exchange,
        envelope,
        digest_character="1",
        accepted_at_ms=200,
    )
    assert isinstance(artifact.accepted_response.settlement, RejectedSettlement)
    poisoned = semantic_harness.poisoned_artifact(artifact, _remove_not_started_evidence)

    semantic_harness.persist_and_reload(
        request_ledger,
        exchange,
        artifact,
        root_key=valid_journal.header.root_key,
    )
    delivery_id = artifact.accepted_response.envelope.delivery_id
    semantic_harness.replace_durable_response(state_database, delivery_id, poisoned)
    with pytest.raises(BridgeError) as caught:
        request_ledger.get_delivery(delivery_id)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_row_revalidation_rebuilds_dynamic_operation_as_concrete_mutation(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
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
    envelope = semantic_harness.response_envelope(
        exchange,
        delivery_id=semantic_harness.dynamic_delivery_id,
        result=None,
        error=semantic_harness.not_started_error(exchange),
    )
    artifact = semantic_harness.artifact_from_parser(
        exchange,
        envelope,
        digest_character="2",
        accepted_at_ms=500,
    )
    assert isinstance(artifact.accepted_response.settlement, RejectedSettlement)
    semantic_harness.persist_and_reload(
        request_ledger,
        exchange,
        artifact,
        root_key=valid_journal.header.root_key,
    )

    with sqlite3.connect(state_database.path) as connection:
        connection.execute(
            "UPDATE requests SET operation_class='destructive' WHERE request_id=?",
            (str(exchange.request_id),),
        )
        connection.commit()
    with pytest.raises(BridgeError) as caught:
        request_ledger.get_delivery(semantic_harness.dynamic_delivery_id)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def _replace_completed_cancel_result(tree: dict[str, object]) -> None:
    invalid_result = cast(JsonValue, {"unit_guid": "UNIT-ONLY"})
    envelope = cast(dict[str, object], tree["envelope"])
    envelope_ack = cast(dict[str, object], envelope["result"])
    envelope_ack["result"] = invalid_result
    cancel_ack = cast(dict[str, object], tree["cancel_ack"])
    cancel_ack["result"] = invalid_result
    settlement = cast(dict[str, object], tree["settlement"])
    settlement["result"] = invalid_result


def test_row_revalidation_rejects_completed_cancel_recovery_schema_drift(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
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
    envelope = semantic_harness.response_envelope(
        exchange,
        delivery_id=semantic_harness.cancel_delivery_id,
        result=acknowledgement,
        error=None,
    )
    artifact = semantic_harness.artifact_from_parser(
        exchange,
        envelope,
        digest_character="3",
        accepted_at_ms=210,
    )
    assert isinstance(artifact.accepted_response.settlement, CompletedSettlement)
    assert artifact.accepted_response.cancel_ack is not None
    poisoned = semantic_harness.poisoned_artifact(artifact, _replace_completed_cancel_result)

    semantic_harness.persist_and_reload(
        request_ledger,
        exchange,
        artifact,
        root_key=valid_journal.header.root_key,
    )
    semantic_harness.replace_durable_response(
        state_database,
        semantic_harness.cancel_delivery_id,
        poisoned,
    )
    with pytest.raises(BridgeError) as caught:
        request_ledger.get_delivery(semantic_harness.cancel_delivery_id)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def test_row_revalidation_round_trips_cancelled_settlement(
    request_ledger: RequestLedger,
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
    envelope = semantic_harness.response_envelope(
        exchange,
        delivery_id=semantic_harness.cancel_delivery_id,
        result=acknowledgement,
        error=None,
    )
    artifact = semantic_harness.artifact_from_parser(
        exchange,
        envelope,
        digest_character="9",
        accepted_at_ms=211,
    )
    assert isinstance(artifact.accepted_response.settlement, CancelledSettlement)
    semantic_harness.persist_and_reload(
        request_ledger,
        exchange,
        artifact,
        root_key=valid_journal.header.root_key,
    )


def _replace_reconcile_disposition(tree: dict[str, object]) -> None:
    envelope = cast(dict[str, object], tree["envelope"])
    envelope_result = cast(dict[str, object], envelope["result"])
    envelope_result["disposition"] = "not_applied"
    settlement = cast(dict[str, object], tree["settlement"])
    settlement_result = cast(dict[str, object], settlement["result"])
    settlement_result["disposition"] = "not_applied"


def test_row_revalidation_rejects_reconcile_target_disposition_drift(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
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
    envelope = semantic_harness.response_envelope(
        reconcile_attempt,
        delivery_id=reconcile_attempt.delivery_intents[0].delivery_id,
        result=result,
        error=None,
    )
    artifact = semantic_harness.artifact_from_parser(
        reconcile_attempt,
        envelope,
        digest_character="4",
        accepted_at_ms=410,
    )
    poisoned = semantic_harness.poisoned_artifact(artifact, _replace_reconcile_disposition)
    semantic_harness.persist_and_reload(
        request_ledger,
        reconcile_attempt,
        artifact,
        root_key=valid_journal.header.root_key,
    )
    delivery_id = reconcile_attempt.delivery_intents[0].delivery_id
    semantic_harness.replace_durable_response(state_database, delivery_id, poisoned)
    with pytest.raises(BridgeError) as caught:
        request_ledger.get_delivery(delivery_id)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


def _add_forbidden_manifest_mismatch_settlement(tree: dict[str, object]) -> None:
    envelope = cast(dict[str, object], tree["envelope"])
    tree["settlement"] = {"state": "rejected", "error": envelope["error"]}


def test_row_revalidation_requires_no_manifest_mismatch_settlement(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
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
    envelope = semantic_harness.response_envelope(
        exchange,
        delivery_id=exchange.delivery_intents[0].delivery_id,
        result=None,
        error=semantic_harness.not_started_error(exchange, code=ErrorCode.MANIFEST_MISMATCH),
        reported_snapshot=reported,
    )
    artifact = semantic_harness.artifact_from_parser(
        exchange,
        envelope,
        digest_character="5",
        accepted_at_ms=220,
    )
    assert artifact.accepted_response.settlement is None
    poisoned = semantic_harness.poisoned_artifact(
        artifact,
        _add_forbidden_manifest_mismatch_settlement,
    )
    semantic_harness.persist_and_reload(
        request_ledger,
        exchange,
        artifact,
        root_key=valid_journal.header.root_key,
    )
    delivery_id = exchange.delivery_intents[0].delivery_id
    semantic_harness.replace_durable_response(state_database, delivery_id, poisoned)
    with pytest.raises(BridgeError) as caught:
        request_ledger.get_delivery(delivery_id)
    assert caught.value.code is ErrorCode.STATE_CONFLICT


STATUS_REQUEST_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
STATUS_DELIVERY_ID = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
STATUS_CANDIDATE = UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
STATUS_LINEAGE = UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
STATUS_OLD_ACTIVATION = UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")


def _status_result(
    snapshot: RuntimeSnapshot,
    *,
    lineage_id: UUID,
    activation_id: UUID,
) -> dict[str, object]:
    return {
        "protocol": snapshot.protocol,
        "runtime_version": snapshot.runtime_version,
        "runtime_tag": snapshot.runtime_tag,
        "runtime_asset_sha256": snapshot.runtime_asset_sha256,
        "release_id": snapshot.release_id,
        "build": 1868,
        "manifest_sha256": snapshot.operation_manifest_sha256,
        "lineage_id": str(lineage_id),
        "activation_id": str(activation_id),
        "installed_event_names": ["Initialize", "Poll"],
        "installed_action_names": ["Initialize", "Poll"],
        "installed_trigger_names": ["Loaded", "Regular"],
        "pending_request_id": None,
        "quarantined": False,
        "paused_capability": None,
        "poll_interval_seconds": 5,
        "safe_payload_bytes": 4096,
        "verified_ledger_entries": 32,
        "effective_ledger_capacity": 32,
    }


def _status_inst(
    record: RequestRecord,
    intent: DeliveryIntent,
    *,
    lineage_id: UUID,
    activation_id: UUID,
) -> bytes:
    snapshot = record.runtime_snapshot
    envelope = {
        "protocol": snapshot.protocol,
        "request_id": str(record.request_id),
        "delivery_id": str(intent.delivery_id),
        "request_hash": record.request_hash,
        "ok": True,
        "result": _status_result(
            snapshot,
            lineage_id=lineage_id,
            activation_id=activation_id,
        ),
        "error": None,
        "scenario_time": "2026-07-10T13:00:00Z",
        "scenario_lineage_id": str(lineage_id),
        "activation_id": str(activation_id),
        "operation_manifest_sha256": snapshot.operation_manifest_sha256,
        "bridge_version": snapshot.runtime_version,
        "runtime_tag": snapshot.runtime_tag,
        "runtime_asset_sha256": snapshot.runtime_asset_sha256,
        "release_id": snapshot.release_id,
    }
    inner = json.dumps(
        envelope,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return json.dumps(
        {"Comments": inner},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


@pytest.mark.parametrize(
    (
        "expected_lineage_id",
        "expected_activation_id",
        "returned_lineage_id",
        "expected_bootstrap",
    ),
    (
        (None, None, STATUS_LINEAGE, True),
        (STATUS_LINEAGE, STATUS_OLD_ACTIVATION, STATUS_LINEAGE, False),
    ),
    ids=("bootstrap", "contextual"),
)
def test_status_response_rebuilds_typed_wire_and_context_expectation(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    runtime_snapshot: RuntimeSnapshot,
    valid_journal: PendingJournal,
    expected_lineage_id: UUID | None,
    expected_activation_id: UUID | None,
    returned_lineage_id: UUID,
    expected_bootstrap: bool,
) -> None:
    arguments = cast(
        dict[str, JsonValue],
        {
            "accept_lineage_id": (
                None if expected_lineage_id is None else str(expected_lineage_id)
            ),
            "activation_candidate": str(STATUS_CANDIDATE),
        },
    )
    invocation = OPERATION_REGISTRY.resolve_wire_invocation("bridge.status", arguments)
    assert type(invocation.wire_arguments) is BridgeStatusWireArgs
    assert invocation.wire_arguments.activation_candidate == STATUS_CANDIDATE
    body = RequestBody(
        protocol=runtime_snapshot.protocol,
        release_id=runtime_snapshot.release_id,
        runtime_version=runtime_snapshot.runtime_version,
        runtime_tag=runtime_snapshot.runtime_tag,
        runtime_asset_sha256=runtime_snapshot.runtime_asset_sha256,
        expected_lineage_id=expected_lineage_id,
        expected_activation_id=expected_activation_id,
        operation_manifest_sha256=runtime_snapshot.operation_manifest_sha256,
        operation="bridge.status",
        arguments=arguments,
    )
    body_bytes = canonical_body_bytes(body)
    request_hash = hashlib.sha256(body_bytes).hexdigest()
    recovery_schema_id = (
        None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
    )
    record = RequestRecord(
        request_id=STATUS_REQUEST_ID,
        root_key=valid_journal.header.root_key,
        request_hash=request_hash,
        operation="bridge.status",
        operation_class=invocation.effective_class,
        state=HostRequestState.PREPARED,
        runtime_snapshot=runtime_snapshot,
        result_schema_id=invocation.result_schema.schema_id,
        recovery_schema_id=recovery_schema_id,
        body_json=body_bytes,
        lineage_id=expected_lineage_id,
        activation_id=expected_activation_id,
        result_json=None,
        error_json=None,
        resolution_json=None,
        created_at_ms=100,
        updated_at_ms=100,
        terminal_at_ms=None,
    )
    intent = DeliveryIntent(
        request_id=record.request_id,
        delivery_id=STATUS_DELIVERY_ID,
        delivery_kind="request",
        original_request_delivery_id=STATUS_DELIVERY_ID,
        body_json=body_bytes.decode("utf-8"),
        request_hash=request_hash,
        runtime_snapshot=runtime_snapshot,
        result_schema_id=record.result_schema_id,
        recovery_schema_id=record.recovery_schema_id,
        intended_at_ms=100,
        published_at_ms=None,
        rendered_inbox_sha256="7" * 64,
        rendered_inbox_size_bytes=len(body_bytes),
        response_filename=f"CMOAgentBridge_Response_{record.request_id}.inst",
    )
    request_ledger.insert_prepared(record)
    request_ledger.insert_delivery(intent)
    request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=101)

    with state_database._transaction(write=False) as connection:  # pyright: ignore[reportPrivateUsage]
        rebuilt = request_ledger._revalidated_request(  # pyright: ignore[reportPrivateUsage]
            connection,
            record.request_id,
        )
    assert type(rebuilt.invocation.wire_arguments) is BridgeStatusWireArgs
    assert rebuilt.invocation.wire_arguments.activation_candidate == STATUS_CANDIDATE
    assert rebuilt.expectation.status_bootstrap is expected_bootstrap
    assert rebuilt.expectation.activation_candidate == STATUS_CANDIDATE
    assert rebuilt.expectation.expected_lineage_id == expected_lineage_id
    assert rebuilt.expectation.expected_activation_id == expected_activation_id

    raw = _status_inst(
        record,
        intent,
        lineage_id=returned_lineage_id,
        activation_id=STATUS_CANDIDATE,
    )
    accepted = parse_inst_response(raw, rebuilt.expectation)
    artifact = ResponseArtifact(
        filename=intent.response_filename,
        sha256=hashlib.sha256(raw).hexdigest(),
        size_bytes=len(raw),
        accepted_at_ms=102,
        accepted_response=accepted,
    )
    recorded = request_ledger.record_response(artifact)
    assert recorded.response_artifact == artifact
    assert request_ledger.get_delivery(intent.delivery_id) == recorded


def test_foreign_durable_response_reload_uses_only_running_catalog(
    request_ledger: RequestLedger,
    state_database: StateDatabase,
    manifest_catalog: ManifestCatalog,
    prepared_record: RequestRecord,
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    intent = valid_journal.original.delivery_intents[0]
    request_ledger.insert_prepared(prepared_record)
    request_ledger.insert_delivery(intent)
    request_ledger.mark_delivery_published(intent.delivery_id, published_at_ms=101)
    durable = request_ledger.record_response(completed_artifact)
    assert durable.response_artifact is not None

    replacement_snapshot = RuntimeSnapshot.create(
        runtime_version="0.2.0",
        runtime_asset_sha256="0" * 64,
        operation_manifest_sha256=prepared_record.runtime_snapshot.operation_manifest_sha256,
        host_contract_sha256="1" * 64,
        dependency_lock_sha256="2" * 64,
    )
    replacement_catalog = ManifestCatalog(
        ReleaseBinding(snapshot=replacement_snapshot, registry=OPERATION_REGISTRY)
    )
    running_resolve = Mock(wraps=replacement_catalog.resolve_running)
    historical_resolve = Mock(
        side_effect=AssertionError("durable reload must not use the historical catalog")
    )
    monkeypatch.setattr(replacement_catalog, "resolve_running", running_resolve)
    monkeypatch.setattr(manifest_catalog, "resolve_running", historical_resolve)
    current_ledger = RequestLedger(state_database, replacement_catalog)

    with pytest.raises(BridgeError) as caught:
        current_ledger.get_delivery(intent.delivery_id)
    assert caught.value.to_payload() == {
        "code": ErrorCode.STATE_CONFLICT.value,
        "message": "persisted delivery row is malformed",
        "details": {},
    }
    running_resolve.assert_called_once_with(prepared_record.runtime_snapshot.release_id)
    historical_resolve.assert_not_called()
    assert isinstance(caught.value.__cause__, BridgeError)
    assert caught.value.__cause__.code is ErrorCode.MANIFEST_MISMATCH
