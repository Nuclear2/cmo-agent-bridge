from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest
from pydantic import JsonValue

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.models import RequestBody
from cmo_agent_bridge.protocol.response import parse_inst_response
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CompletedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
    ResponseError,
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
from cmo_agent_bridge.state.request_ledger import RequestLedger, RequestRecord
from cmo_agent_bridge.state.revalidation import revalidate_pending_exchange
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths

if TYPE_CHECKING:
    from cmo_agent_bridge.state.pending_journal import PendingJournalStore


REQUEST_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
REQUEST_DELIVERY_ID = UUID("11111111-1111-4111-8111-111111111111")
CANCEL_DELIVERY_ID = UUID("22222222-2222-4222-8222-222222222222")
LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")
ROOT_KEY = "a" * 64


class SemanticHarness:
    """Shared real-registry/parser builders for the two authorized durable stores."""

    dynamic_request_id = UUID("77777777-7777-4777-8777-777777777777")
    dynamic_delivery_id = UUID("88888888-8888-4888-8888-888888888888")
    cancel_delivery_id = CANCEL_DELIVERY_ID

    def __init__(self, catalog: ManifestCatalog) -> None:
        self.catalog = catalog

    @staticmethod
    def canonical(value: object) -> bytes:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def inst(cls, envelope: dict[str, object]) -> bytes:
        return cls.canonical({"Comments": cls.canonical(envelope).decode("utf-8")})

    @staticmethod
    def make_exchange(
        template: PendingExchange,
        *,
        request_id: UUID,
        delivery_id: UUID,
        operation: str,
        arguments: dict[str, JsonValue],
        intended_at_ms: int,
    ) -> PendingExchange:
        invocation = OPERATION_REGISTRY.resolve_wire_invocation(operation, arguments)
        body = RequestBody(
            protocol=template.runtime_snapshot.protocol,
            release_id=template.runtime_snapshot.release_id,
            runtime_version=template.runtime_snapshot.runtime_version,
            runtime_tag=template.runtime_snapshot.runtime_tag,
            runtime_asset_sha256=template.runtime_snapshot.runtime_asset_sha256,
            expected_lineage_id=template.expected_lineage_id,
            expected_activation_id=template.expected_activation_id,
            operation_manifest_sha256=template.runtime_snapshot.operation_manifest_sha256,
            operation=operation,
            arguments=arguments,
        )
        body_json = canonical_body_bytes(body).decode("utf-8")
        request_hash = hashlib.sha256(body_json.encode("utf-8")).hexdigest()
        recovery_schema_id = (
            None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
        )
        intent = DeliveryIntent(
            request_id=request_id,
            delivery_id=delivery_id,
            delivery_kind="request",
            original_request_delivery_id=delivery_id,
            body_json=body_json,
            request_hash=request_hash,
            runtime_snapshot=template.runtime_snapshot,
            result_schema_id=invocation.result_schema.schema_id,
            recovery_schema_id=recovery_schema_id,
            intended_at_ms=intended_at_ms,
            published_at_ms=None,
            rendered_inbox_sha256="8" * 64,
            rendered_inbox_size_bytes=600,
            response_filename=f"CMOAgentBridge_Response_{request_id}.inst",
        )
        return PendingExchange(
            request_id=request_id,
            request_hash=request_hash,
            operation=operation,
            effective_class=invocation.effective_class,
            body_json=body_json,
            runtime_snapshot=template.runtime_snapshot,
            result_schema_id=invocation.result_schema.schema_id,
            recovery_schema_id=recovery_schema_id,
            expected_lineage_id=template.expected_lineage_id,
            expected_activation_id=template.expected_activation_id,
            delivery_intents=(intent,),
            response_artifact=None,
            settlement=None,
            original_target_request_id=None,
            original_target_request_hash=None,
            revision=0,
            state=PendingPhase.PREPARED,
            created_at_ms=intended_at_ms,
            updated_at_ms=intended_at_ms,
        )

    @classmethod
    def cancel_wait_exchange(cls, exchange: PendingExchange) -> PendingExchange:
        request = exchange.delivery_intents[0].model_copy(update={"published_at_ms": 101})
        cancel = DeliveryIntent.model_validate(
            {
                **request.model_dump(mode="python"),
                "delivery_id": cls.cancel_delivery_id,
                "delivery_kind": "cancel",
                "original_request_delivery_id": request.delivery_id,
                "intended_at_ms": 102,
                "published_at_ms": 103,
                "rendered_inbox_sha256": "1" * 64,
            }
        )
        return PendingExchange.model_validate(
            {
                **exchange.model_dump(mode="python"),
                "delivery_intents": (request, cancel),
                "revision": 2,
                "state": PendingPhase.CANCEL_PUBLISHED,
                "updated_at_ms": 103,
            }
        )

    @staticmethod
    def response_envelope(
        exchange: PendingExchange,
        *,
        delivery_id: UUID,
        result: JsonValue,
        error: dict[str, object] | None,
        reported_snapshot: RuntimeSnapshot | None = None,
    ) -> dict[str, object]:
        snapshot = exchange.runtime_snapshot if reported_snapshot is None else reported_snapshot
        return {
            "protocol": "cmo-agent-bridge/1",
            "request_id": str(exchange.request_id),
            "delivery_id": str(delivery_id),
            "request_hash": exchange.request_hash,
            "ok": error is None,
            "result": result,
            "error": error,
            "scenario_time": "2026-07-10T13:00:00Z",
            "scenario_lineage_id": str(exchange.expected_lineage_id),
            "activation_id": str(exchange.expected_activation_id),
            "operation_manifest_sha256": snapshot.operation_manifest_sha256,
            "bridge_version": snapshot.runtime_version,
            "runtime_tag": snapshot.runtime_tag,
            "runtime_asset_sha256": snapshot.runtime_asset_sha256,
            "release_id": snapshot.release_id,
        }

    @staticmethod
    def not_started_error(
        exchange: PendingExchange,
        *,
        code: ErrorCode = ErrorCode.CMO_LUA_ERROR,
    ) -> dict[str, object]:
        return {
            "code": code.value,
            "message": "runtime rejected request",
            "details": {},
            "mutation_not_started": {
                "schema_version": 1,
                "stage": "dispatch_validation",
                "request_id": str(exchange.request_id),
                "request_hash": exchange.request_hash,
                "operation": exchange.operation,
                "mutation_barrier_written": False,
                "execute_started": False,
            },
        }

    def artifact_from_parser(
        self,
        exchange: PendingExchange,
        envelope: dict[str, object],
        *,
        digest_character: str,
        accepted_at_ms: int,
    ) -> ResponseArtifact:
        binding = self.catalog.resolve_running(exchange.runtime_snapshot.release_id)
        validated = revalidate_pending_exchange(exchange, binding=binding)
        raw = self.inst(envelope)
        accepted = parse_inst_response(raw, validated.expectation)
        return ResponseArtifact(
            filename=f"CMOAgentBridge_Response_{exchange.request_id}.inst",
            sha256=digest_character * 64,
            size_bytes=len(raw),
            accepted_at_ms=accepted_at_ms,
            accepted_response=accepted,
        )

    @staticmethod
    def request_record(exchange: PendingExchange, root_key: str) -> RequestRecord:
        return RequestRecord(
            request_id=exchange.request_id,
            root_key=root_key,
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
            updated_at_ms=exchange.created_at_ms,
            terminal_at_ms=None,
        )

    @classmethod
    def persist_and_reload(
        cls,
        ledger: RequestLedger,
        exchange: PendingExchange,
        artifact: ResponseArtifact,
        *,
        root_key: str,
    ) -> None:
        ledger.insert_prepared(cls.request_record(exchange, root_key))
        for intent in exchange.delivery_intents:
            ledger.insert_delivery(intent)
            if intent.published_at_ms is None:
                ledger.mark_delivery_published(
                    intent.delivery_id,
                    published_at_ms=intent.intended_at_ms + 1,
                )
        recorded = ledger.record_response(artifact)
        assert recorded.response_artifact == artifact
        assert ledger.get_delivery(artifact.accepted_response.envelope.delivery_id) == recorded

    @staticmethod
    def poisoned_artifact(
        artifact: ResponseArtifact,
        mutate: Callable[[dict[str, object]], None],
    ) -> ResponseArtifact:
        tree = cast(dict[str, object], artifact.accepted_response.model_dump(mode="json"))
        mutate(tree)
        accepted = AcceptedResponse.model_validate(tree)
        return artifact.model_copy(update={"accepted_response": accepted})

    @classmethod
    def replace_durable_response(
        cls,
        database: StateDatabase,
        delivery_id: UUID,
        artifact: ResponseArtifact,
    ) -> None:
        accepted = artifact.accepted_response
        accepted_blob = cls.canonical(accepted.model_dump(mode="json"))
        settlement_blob = cls.canonical(
            None if accepted.settlement is None else accepted.settlement.model_dump(mode="json")
        )
        with sqlite3.connect(database.path) as connection:
            connection.execute(
                "UPDATE deliveries SET accepted_response_json=?,settlement_json=? "
                "WHERE delivery_id=?",
                (
                    sqlite3.Binary(accepted_blob),
                    sqlite3.Binary(settlement_blob),
                    str(delivery_id),
                ),
            )
            connection.commit()

    @staticmethod
    def accepted_exchange(
        exchange: PendingExchange,
        artifact: ResponseArtifact,
    ) -> PendingExchange:
        intents = tuple(
            intent
            if intent.published_at_ms is not None
            else intent.model_copy(update={"published_at_ms": intent.intended_at_ms + 1})
            for intent in exchange.delivery_intents
        )
        return PendingExchange.model_validate(
            {
                **exchange.model_dump(mode="python"),
                "delivery_intents": intents,
                "response_artifact": artifact,
                "settlement": artifact.accepted_response.settlement,
                "revision": exchange.revision + 1,
                "state": PendingPhase.RESPONSE_ACCEPTED,
                "updated_at_ms": max(exchange.updated_at_ms, artifact.accepted_at_ms),
            }
        )

    @classmethod
    async def assert_original_journal_positive_and_negative(
        cls,
        *,
        store: PendingJournalStore,
        root_lock: RootLock,
        paths: FileBridgePaths,
        header_source: PendingJournal,
        positive: PendingExchange,
        negative: PendingExchange,
    ) -> None:
        positive_journal = PendingJournal(
            header=header_source.header,
            original=positive,
            reconcile_attempt=None,
        )
        negative_journal = PendingJournal(
            header=header_source.header,
            original=negative,
            reconcile_attempt=None,
        )
        paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
        async with root_lock:
            paths.pending_file.write_bytes(cls.canonical(positive_journal.model_dump(mode="json")))
            loaded = store.load()
            assert loaded is not None
            assert loaded.journal == positive_journal

            paths.pending_file.write_bytes(cls.canonical(negative_journal.model_dump(mode="json")))
            with pytest.raises(BridgeError) as caught:
                store.load()
            assert caught.value.code is ErrorCode.JOURNAL_CORRUPT


@pytest.fixture
def runtime_snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=MANIFEST_SHA256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )


@pytest.fixture
def mutation_body(runtime_snapshot: RuntimeSnapshot) -> RequestBody:
    return RequestBody(
        protocol=runtime_snapshot.protocol,
        release_id=runtime_snapshot.release_id,
        runtime_version=runtime_snapshot.runtime_version,
        runtime_tag=runtime_snapshot.runtime_tag,
        runtime_asset_sha256=runtime_snapshot.runtime_asset_sha256,
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=ACTIVATION_ID,
        operation_manifest_sha256=runtime_snapshot.operation_manifest_sha256,
        operation="unit.add",
        arguments={
            "side_guid": "SIDE-1",
            "unit_type": "Aircraft",
            "dbid": 1,
            "name": "Test unit",
            "latitude": 1.5,
            "longitude": 2.5,
            "altitude": None,
            "loadout_dbid": None,
        },
    )


@pytest.fixture
def valid_exchange_factory(
    runtime_snapshot: RuntimeSnapshot,
    mutation_body: RequestBody,
) -> Callable[..., PendingExchange]:
    invocation = OPERATION_REGISTRY.resolve_wire_invocation(
        mutation_body.operation, mutation_body.arguments
    )
    body_json = canonical_body_bytes(mutation_body).decode("utf-8")
    import hashlib

    request_hash = hashlib.sha256(body_json.encode("utf-8")).hexdigest()

    def factory(
        *,
        state: PendingPhase = PendingPhase.PREPARED,
        published_at_ms: int | None = None,
        revision: int = 0,
        updated_at_ms: int = 100,
    ) -> PendingExchange:
        intent = DeliveryIntent(
            request_id=REQUEST_ID,
            delivery_id=REQUEST_DELIVERY_ID,
            delivery_kind="request",
            original_request_delivery_id=REQUEST_DELIVERY_ID,
            body_json=body_json,
            request_hash=request_hash,
            runtime_snapshot=runtime_snapshot,
            result_schema_id=invocation.result_schema.schema_id,
            recovery_schema_id=(
                None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
            ),
            intended_at_ms=100,
            published_at_ms=published_at_ms,
            rendered_inbox_sha256="e" * 64,
            rendered_inbox_size_bytes=512,
            response_filename=f"CMOAgentBridge_Response_{REQUEST_ID}.inst",
        )
        return PendingExchange(
            request_id=REQUEST_ID,
            request_hash=request_hash,
            operation=mutation_body.operation,
            effective_class=invocation.effective_class,
            body_json=body_json,
            runtime_snapshot=runtime_snapshot,
            result_schema_id=invocation.result_schema.schema_id,
            recovery_schema_id=(
                None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
            ),
            expected_lineage_id=LINEAGE_ID,
            expected_activation_id=ACTIVATION_ID,
            delivery_intents=(intent,),
            response_artifact=None,
            settlement=None,
            original_target_request_id=None,
            original_target_request_hash=None,
            revision=revision,
            state=state,
            created_at_ms=100,
            updated_at_ms=updated_at_ms,
        )

    return factory


@pytest.fixture
def valid_journal(
    runtime_snapshot: RuntimeSnapshot,
    valid_exchange_factory: Callable[..., PendingExchange],
    file_bridge_paths: FileBridgePaths,
) -> PendingJournal:
    return PendingJournal(
        header=PendingJournalHeader(
            format="cmo-agent-bridge/pending-journal",
            header_version=1,
            root_key=file_bridge_paths.root_key,
            required_release_id=runtime_snapshot.release_id,
        ),
        original=valid_exchange_factory(),
        reconcile_attempt=None,
    )


@pytest.fixture
def completed_artifact(valid_journal: PendingJournal) -> ResponseArtifact:
    exchange = valid_journal.original
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
    envelope = ResponseEnvelope(
        protocol="cmo-agent-bridge/1",
        request_id=exchange.request_id,
        delivery_id=exchange.delivery_intents[0].delivery_id,
        request_hash=exchange.request_hash,
        ok=True,
        result=result,
        error=None,
        scenario_time="2026-07-10T13:00:00Z",
        scenario_lineage_id=LINEAGE_ID,
        activation_id=ACTIVATION_ID,
        operation_manifest_sha256=exchange.runtime_snapshot.operation_manifest_sha256,
        bridge_version=exchange.runtime_snapshot.runtime_version,
        runtime_tag=exchange.runtime_snapshot.runtime_tag,
        runtime_asset_sha256=exchange.runtime_snapshot.runtime_asset_sha256,
        release_id=exchange.runtime_snapshot.release_id,
    )
    settlement = CompletedSettlement(state="completed", result=result)
    accepted = AcceptedResponse(
        envelope=envelope,
        delivery_kind="request",
        settlement=settlement,
        cancel_ack=None,
    )
    return ResponseArtifact(
        filename=exchange.delivery_intents[0].response_filename,
        sha256="f" * 64,
        size_bytes=256,
        accepted_at_ms=200,
        accepted_response=accepted,
    )


@pytest.fixture
def no_settlement_artifact(valid_journal: PendingJournal) -> ResponseArtifact:
    exchange = valid_journal.original
    envelope = ResponseEnvelope(
        protocol="cmo-agent-bridge/1",
        request_id=exchange.request_id,
        delivery_id=exchange.delivery_intents[0].delivery_id,
        request_hash=exchange.request_hash,
        ok=False,
        result=None,
        error=ResponseError(
            code=ErrorCode.CMO_LUA_ERROR,
            message="outcome is unsafe",
            details={},
        ),
        scenario_time="2026-07-10T13:00:00Z",
        scenario_lineage_id=LINEAGE_ID,
        activation_id=ACTIVATION_ID,
        operation_manifest_sha256=exchange.runtime_snapshot.operation_manifest_sha256,
        bridge_version=exchange.runtime_snapshot.runtime_version,
        runtime_tag=exchange.runtime_snapshot.runtime_tag,
        runtime_asset_sha256=exchange.runtime_snapshot.runtime_asset_sha256,
        release_id=exchange.runtime_snapshot.release_id,
    )
    accepted = AcceptedResponse(
        envelope=envelope,
        delivery_kind="request",
        settlement=None,
        cancel_ack=None,
    )
    return ResponseArtifact(
        filename=exchange.delivery_intents[0].response_filename,
        sha256="9" * 64,
        size_bytes=200,
        accepted_at_ms=201,
        accepted_response=accepted,
    )


@pytest.fixture
def reconcile_attempt(valid_journal: PendingJournal) -> PendingExchange:
    original = valid_journal.original
    request_id = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    delivery_id = UUID("66666666-6666-4666-8666-666666666666")
    arguments = cast(
        dict[str, JsonValue],
        {
            "request_id": str(original.request_id),
            "disposition": "applied",
            "confirmation_proof": "7" * 64,
        },
    )
    invocation = OPERATION_REGISTRY.resolve_wire_invocation("bridge.reconcile", arguments)
    body = RequestBody(
        protocol=original.runtime_snapshot.protocol,
        release_id=original.runtime_snapshot.release_id,
        runtime_version=original.runtime_snapshot.runtime_version,
        runtime_tag=original.runtime_snapshot.runtime_tag,
        runtime_asset_sha256=original.runtime_snapshot.runtime_asset_sha256,
        expected_lineage_id=original.expected_lineage_id,
        expected_activation_id=original.expected_activation_id,
        operation_manifest_sha256=original.runtime_snapshot.operation_manifest_sha256,
        operation="bridge.reconcile",
        arguments=arguments,
    )
    body_json = canonical_body_bytes(body).decode("utf-8")
    import hashlib

    request_hash = hashlib.sha256(body_json.encode("utf-8")).hexdigest()
    intent = DeliveryIntent(
        request_id=request_id,
        delivery_id=delivery_id,
        delivery_kind="request",
        original_request_delivery_id=delivery_id,
        body_json=body_json,
        request_hash=request_hash,
        runtime_snapshot=original.runtime_snapshot,
        result_schema_id=invocation.result_schema.schema_id,
        recovery_schema_id=(
            None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
        ),
        intended_at_ms=300,
        published_at_ms=None,
        rendered_inbox_sha256="8" * 64,
        rendered_inbox_size_bytes=600,
        response_filename=f"CMOAgentBridge_Response_{request_id}.inst",
    )
    return PendingExchange(
        request_id=request_id,
        request_hash=request_hash,
        operation="bridge.reconcile",
        effective_class=invocation.effective_class,
        body_json=body_json,
        runtime_snapshot=original.runtime_snapshot,
        result_schema_id=invocation.result_schema.schema_id,
        recovery_schema_id=(
            None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
        ),
        expected_lineage_id=original.expected_lineage_id,
        expected_activation_id=original.expected_activation_id,
        delivery_intents=(intent,),
        response_artifact=None,
        settlement=None,
        original_target_request_id=original.request_id,
        original_target_request_hash=original.request_hash,
        revision=0,
        state=PendingPhase.PREPARED,
        created_at_ms=300,
        updated_at_ms=300,
    )


@pytest.fixture
def file_bridge_paths(tmp_path: Path) -> FileBridgePaths:
    game_root = tmp_path / "game"
    game_root.mkdir()
    (game_root / "Command.exe").write_bytes(b"exe")
    (game_root / "Lua").mkdir()
    (game_root / "ImportExport").mkdir()
    local_app_data = tmp_path / "local"
    local_app_data.mkdir()
    return FileBridgePaths.build(game_root, local_app_data)


@pytest.fixture
def manifest_catalog(runtime_snapshot: RuntimeSnapshot) -> ManifestCatalog:
    return ManifestCatalog(ReleaseBinding(snapshot=runtime_snapshot, registry=OPERATION_REGISTRY))


@pytest.fixture
def semantic_harness(manifest_catalog: ManifestCatalog) -> SemanticHarness:
    return SemanticHarness(manifest_catalog)


@pytest.fixture
def root_lock(file_bridge_paths: FileBridgePaths) -> RootLock:
    return RootLock(file_bridge_paths.lock_file, timeout_seconds=0)


@pytest.fixture
def journal_store(
    file_bridge_paths: FileBridgePaths,
    root_lock: RootLock,
    manifest_catalog: ManifestCatalog,
) -> PendingJournalStore:
    from cmo_agent_bridge.state.pending_journal import PendingJournalStore

    return PendingJournalStore(
        file_bridge_paths,
        root_lock,
        manifest_catalog,
        max_journal_bytes=1_000_000,
        replace_retry_seconds=0,
    )
