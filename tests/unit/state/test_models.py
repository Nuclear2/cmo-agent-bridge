from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import cast
from uuid import UUID

import pytest
from pydantic import ValidationError

from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.protocol.response_models import (
    CancelledSettlement,
    CompletedSettlement,
    ResponseArtifact,
)
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    PendingExchange,
    PendingJournal,
    PendingJournalHeader,
    PendingPhase,
)


def test_exact_durable_model_field_order() -> None:
    assert list(PendingJournalHeader.model_fields) == [
        "format",
        "header_version",
        "root_key",
        "required_release_id",
    ]
    assert list(DeliveryIntent.model_fields) == [
        "request_id",
        "delivery_id",
        "delivery_kind",
        "original_request_delivery_id",
        "body_json",
        "request_hash",
        "runtime_snapshot",
        "result_schema_id",
        "recovery_schema_id",
        "intended_at_ms",
        "published_at_ms",
        "rendered_inbox_sha256",
        "rendered_inbox_size_bytes",
        "response_filename",
    ]
    assert list(PendingExchange.model_fields) == [
        "request_id",
        "request_hash",
        "operation",
        "effective_class",
        "body_json",
        "runtime_snapshot",
        "result_schema_id",
        "recovery_schema_id",
        "expected_lineage_id",
        "expected_activation_id",
        "delivery_intents",
        "response_artifact",
        "settlement",
        "original_target_request_id",
        "original_target_request_hash",
        "revision",
        "state",
        "created_at_ms",
        "updated_at_ms",
    ]
    assert list(PendingJournal.model_fields) == ["header", "original", "reconcile_attempt"]


def test_exact_state_enums() -> None:
    assert [phase.value for phase in PendingPhase] == [
        "prepared",
        "published",
        "cancel_published",
        "response_accepted",
        "idle_published",
        "quarantined",
    ]
    assert [state.value for state in HostRequestState] == [
        "prepared",
        "published",
        "cancel_published",
        "response_accepted",
        "idle_published",
        "completed",
        "cancelled",
        "rejected",
        "quarantined",
        "resolved",
    ]


def test_models_are_frozen_extra_forbidden_and_strict(valid_journal: PendingJournal) -> None:
    with pytest.raises(ValidationError):
        PendingJournal.model_validate({**valid_journal.model_dump(mode="python"), "extra": 1})
    with pytest.raises(ValidationError):
        PendingExchange.model_validate({**valid_journal.original.model_dump(mode="python"), "x": 1})
    with pytest.raises(ValidationError):
        DeliveryIntent.model_validate(
            {**valid_journal.original.delivery_intents[0].model_dump(mode="python"), "x": 1}
        )
    with pytest.raises(ValidationError):
        PendingJournalHeader.model_validate(
            {**valid_journal.header.model_dump(mode="python"), "x": 1}
        )
    with pytest.raises(ValidationError):
        valid_journal.original.revision = 1  # type: ignore[misc]


@pytest.mark.parametrize(
    ("model", "value"),
    [
        (PendingJournalHeader, "header"),
        (DeliveryIntent, "intent"),
        (PendingExchange, "exchange"),
        (PendingJournal, "journal"),
    ],
)
def test_every_durable_model_field_is_required(
    valid_journal: PendingJournal,
    model: type[PendingJournalHeader | DeliveryIntent | PendingExchange | PendingJournal],
    value: str,
) -> None:
    sources = {
        "header": valid_journal.header,
        "intent": valid_journal.original.delivery_intents[0],
        "exchange": valid_journal.original,
        "journal": valid_journal,
    }
    source = sources[value]
    dumped = source.model_dump(mode="python")
    for field in type(source).model_fields:
        candidate = dict(dumped)
        del candidate[field]
        with pytest.raises(ValidationError):
            model.model_validate(candidate)


@pytest.mark.parametrize(
    "field",
    [
        "revision",
        "created_at_ms",
        "updated_at_ms",
    ],
)
def test_exchange_rejects_bool_for_every_strict_integer(
    valid_journal: PendingJournal, field: str
) -> None:
    value = valid_journal.original.model_dump(mode="python")
    value[field] = True
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(value)


@pytest.mark.parametrize(
    "field",
    ["intended_at_ms", "published_at_ms", "rendered_inbox_size_bytes"],
)
def test_intent_rejects_bool_for_every_strict_integer(
    valid_journal: PendingJournal, field: str
) -> None:
    value = valid_journal.original.delivery_intents[0].model_dump(mode="python")
    value[field] = True
    with pytest.raises(ValidationError):
        DeliveryIntent.model_validate(value)


def test_header_requires_exact_version_and_sha256(valid_journal: PendingJournal) -> None:
    value = valid_journal.header.model_dump(mode="python")
    for wrong in (True, 1.0, 0, 2):
        with pytest.raises(ValidationError):
            PendingJournalHeader.model_validate({**value, "header_version": wrong})
    for field in ("root_key", "required_release_id"):
        for wrong in ("A" * 64, "a" * 63, b"a" * 64):
            with pytest.raises(ValidationError):
                PendingJournalHeader.model_validate({**value, field: wrong})


@pytest.mark.parametrize(
    "field",
    [
        "request_hash",
        "result_schema_id",
        "recovery_schema_id",
        "rendered_inbox_sha256",
    ],
)
def test_intent_rejects_invalid_sha_fields(valid_journal: PendingJournal, field: str) -> None:
    value = valid_journal.original.delivery_intents[0].model_dump(mode="python")
    if field == "recovery_schema_id" and value[field] is None:
        value[field] = "a" * 64
    for wrong in ("A" * 64, "a" * 63, b"a" * 64):
        with pytest.raises(ValidationError):
            DeliveryIntent.model_validate({**value, field: wrong})


@pytest.mark.parametrize(
    "field",
    ["request_hash", "result_schema_id", "recovery_schema_id", "original_target_request_hash"],
)
def test_exchange_rejects_invalid_sha_fields(valid_journal: PendingJournal, field: str) -> None:
    value = valid_journal.original.model_dump(mode="python")
    if value[field] is None:
        value[field] = "a" * 64
        if field == "original_target_request_hash":
            value["original_target_request_id"] = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
            value["effective_class"] = OperationClass.RECONCILE
    for wrong in ("A" * 64, "a" * 63, b"a" * 64):
        with pytest.raises(ValidationError):
            PendingExchange.model_validate({**value, field: wrong})


@pytest.mark.parametrize(
    ("model_name", "field"),
    [
        ("intent", "request_id"),
        ("intent", "delivery_id"),
        ("intent", "original_request_delivery_id"),
        ("exchange", "request_id"),
        ("exchange", "expected_lineage_id"),
        ("exchange", "expected_activation_id"),
    ],
)
def test_uuid_fields_reject_non_uuid_python_values(
    valid_journal: PendingJournal, model_name: str, field: str
) -> None:
    source = (
        valid_journal.original.delivery_intents[0]
        if model_name == "intent"
        else valid_journal.original
    )
    model = DeliveryIntent if model_name == "intent" else PendingExchange
    value = source.model_dump(mode="python")

    class UUIDSubclass(UUID):
        pass

    exact = cast(UUID, value[field])
    subclass = UUIDSubclass(str(exact))
    for wrong in (str(value[field]), "not-a-uuid", 1, subclass):
        with pytest.raises(ValidationError):
            model.model_validate({**value, field: wrong})


@pytest.mark.parametrize("field", ["operation", "body_json"])
def test_exchange_strict_strings_reject_bytes_and_subclasses(
    valid_journal: PendingJournal, field: str
) -> None:
    class StringSubclass(str):
        pass

    value = valid_journal.original.model_dump(mode="python")
    for wrong in (value[field].encode("utf-8"), StringSubclass(value[field])):
        with pytest.raises(ValidationError):
            PendingExchange.model_validate({**value, field: wrong})


@pytest.mark.parametrize("field", ["body_json", "response_filename"])
def test_intent_strict_strings_reject_bytes_and_subclasses(
    valid_journal: PendingJournal, field: str
) -> None:
    class StringSubclass(str):
        pass

    value = valid_journal.original.delivery_intents[0].model_dump(mode="python")
    for wrong in (value[field].encode("utf-8"), StringSubclass(value[field])):
        with pytest.raises(ValidationError):
            DeliveryIntent.model_validate({**value, field: wrong})


def test_every_required_strict_string_rejects_subclasses(valid_journal: PendingJournal) -> None:
    class StringSubclass(str):
        pass

    cases = (
        (
            PendingJournalHeader,
            valid_journal.header.model_dump(mode="python"),
            ("format", "root_key", "required_release_id"),
        ),
        (
            DeliveryIntent,
            valid_journal.original.delivery_intents[0].model_dump(mode="python"),
            (
                "delivery_kind",
                "body_json",
                "request_hash",
                "result_schema_id",
                "recovery_schema_id",
                "rendered_inbox_sha256",
                "response_filename",
            ),
        ),
        (
            PendingExchange,
            valid_journal.original.model_dump(mode="python"),
            ("request_hash", "operation", "body_json", "result_schema_id", "recovery_schema_id"),
        ),
    )
    for model, value, fields in cases:
        for field in fields:
            original = value[field]
            if original is None:
                continue
            with pytest.raises(ValidationError):
                model.model_validate({**value, field: StringSubclass(cast(str, original))})


def test_exchange_requires_immutable_nonempty_tuple(valid_journal: PendingJournal) -> None:
    value = valid_journal.original.model_dump(mode="python")
    with pytest.raises(ValidationError):
        PendingExchange.model_validate({**value, "delivery_intents": []})
    with pytest.raises(ValidationError):
        PendingExchange.model_validate({**value, "delivery_intents": ()})


def test_nested_runtime_snapshot_is_revalidated(valid_journal: PendingJournal) -> None:
    forged = valid_journal.original.runtime_snapshot.model_copy(update={"runtime_tag": "forged"})
    exchange = valid_journal.original.model_dump(mode="python")
    intent = valid_journal.original.delivery_intents[0].model_dump(mode="python")
    with pytest.raises(ValidationError):
        PendingExchange.model_validate({**exchange, "runtime_snapshot": forged})
    with pytest.raises(ValidationError):
        DeliveryIntent.model_validate({**intent, "runtime_snapshot": forged})


def test_forged_nested_durable_models_are_revalidated(
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    forged_intent = valid_journal.original.delivery_intents[0].model_copy(
        update={"request_id": UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")}
    )
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {
                **valid_journal.original.model_dump(mode="python"),
                "delivery_intents": (forged_intent,),
            }
        )

    forged_exchange = valid_journal.original.model_copy(update={"state": "bogus"})
    with pytest.raises(ValidationError):
        PendingJournal.model_validate(
            {**valid_journal.model_dump(mode="python"), "original": forged_exchange}
        )

    published = _published_exchange_tree(valid_journal)
    forged_artifact = completed_artifact.model_copy(update={"size_bytes": True})
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {
                **published,
                "state": PendingPhase.RESPONSE_ACCEPTED,
                "response_artifact": forged_artifact,
                "settlement": completed_artifact.accepted_response.settlement,
            }
        )
    settlement = cast(CompletedSettlement, completed_artifact.accepted_response.settlement)
    forged_settlement = settlement.model_copy(update={"state": "cancelled"})
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {
                **published,
                "state": PendingPhase.RESPONSE_ACCEPTED,
                "response_artifact": completed_artifact,
                "settlement": forged_settlement,
            }
        )


def test_exchange_rejects_noncanonical_duplicate_or_mismatched_body(
    valid_journal: PendingJournal,
) -> None:
    value = valid_journal.original.model_dump(mode="python")
    body = value["body_json"]
    for wrong in (
        body + "\n",
        body.replace('{"arguments":', '{"arguments":{},"arguments":', 1),
        body.replace('"operation":"unit.add"', '"operation":"unit.set"'),
    ):
        with pytest.raises(ValidationError):
            PendingExchange.model_validate({**value, "body_json": wrong})

    with pytest.raises(ValidationError):
        PendingExchange.model_validate({**value, "operation": "unit.set"})

    different_body = body.replace('"operation":"unit.add"', '"operation":"unit.set"')
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {
                **value,
                "body_json": different_body,
                "request_hash": hashlib.sha256(different_body.encode("utf-8")).hexdigest(),
            }
        )


def test_exchange_rejects_dynamic_or_safe_effective_class(valid_journal: PendingJournal) -> None:
    value = valid_journal.original.model_dump(mode="python")
    for wrong in (OperationClass.DYNAMIC, OperationClass.READ, OperationClass.STATUS):
        with pytest.raises(ValidationError):
            PendingExchange.model_validate({**value, "effective_class": wrong})


def test_prepared_phase_requires_sole_unpublished_request_intent(
    valid_journal: PendingJournal,
) -> None:
    value = valid_journal.original.model_dump(mode="python")
    intent = value["delivery_intents"][0]
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {**value, "delivery_intents": ({**intent, "published_at_ms": 101},)}
        )
    with pytest.raises(ValidationError):
        PendingExchange.model_validate({**value, "response_artifact": object()})


def test_published_phase_requires_request_publication(
    valid_exchange_factory: Callable[..., PendingExchange],
) -> None:
    with pytest.raises(ValidationError):
        valid_exchange_factory(state=PendingPhase.PUBLISHED)
    published = valid_exchange_factory(state=PendingPhase.PUBLISHED, published_at_ms=101)
    assert published.delivery_intents[0].published_at_ms == 101


def _published_exchange_tree(valid_journal: PendingJournal) -> dict[str, object]:
    value = valid_journal.original.model_dump(mode="python")
    intent = value["delivery_intents"][0]
    value["delivery_intents"] = ({**intent, "published_at_ms": 101},)
    return value


def test_cancel_published_phase_requires_exact_cancel_intent(
    valid_journal: PendingJournal,
) -> None:
    value = _published_exchange_tree(valid_journal)
    with pytest.raises(ValidationError):
        PendingExchange.model_validate({**value, "state": PendingPhase.CANCEL_PUBLISHED})

    request = cast(tuple[dict[str, object], ...], value["delivery_intents"])[0]
    cancel = {
        **request,
        "delivery_id": UUID("22222222-2222-4222-8222-222222222222"),
        "delivery_kind": "cancel",
        "original_request_delivery_id": request["delivery_id"],
        "published_at_ms": None,
    }
    exchange = PendingExchange.model_validate(
        {**value, "state": PendingPhase.CANCEL_PUBLISHED, "delivery_intents": (request, cancel)}
    )
    assert len(exchange.delivery_intents) == 2

    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {
                **value,
                "state": PendingPhase.CANCEL_PUBLISHED,
                "delivery_intents": (request, cancel, {**cancel, "delivery_id": UUID(int=5)}),
            }
        )


def test_response_accepted_and_idle_require_artifact_publication_and_settlement(
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    value = _published_exchange_tree(valid_journal)
    settlement = completed_artifact.accepted_response.settlement
    accepted = PendingExchange.model_validate(
        {
            **value,
            "state": PendingPhase.RESPONSE_ACCEPTED,
            "response_artifact": completed_artifact,
            "settlement": settlement,
        }
    )
    assert accepted.response_artifact == completed_artifact
    idle = PendingExchange.model_validate(
        {**accepted.model_dump(mode="python"), "state": PendingPhase.IDLE_PUBLISHED}
    )
    assert isinstance(idle.settlement, CompletedSettlement)

    for changes in (
        {"response_artifact": None, "settlement": None},
        {"delivery_intents": valid_journal.original.delivery_intents},
        {"settlement": CancelledSettlement(state="cancelled")},
    ):
        with pytest.raises(ValidationError):
            PendingExchange.model_validate(
                {
                    **value,
                    "state": PendingPhase.RESPONSE_ACCEPTED,
                    "response_artifact": completed_artifact,
                    "settlement": settlement,
                    **changes,
                }
            )


def test_quarantined_requires_published_request_and_preserves_optional_artifact(
    valid_journal: PendingJournal,
    completed_artifact: ResponseArtifact,
) -> None:
    published = _published_exchange_tree(valid_journal)
    without_artifact = PendingExchange.model_validate(
        {**published, "state": PendingPhase.QUARANTINED}
    )
    assert without_artifact.response_artifact is None
    with_artifact = PendingExchange.model_validate(
        {
            **published,
            "state": PendingPhase.QUARANTINED,
            "response_artifact": completed_artifact,
            "settlement": completed_artifact.accepted_response.settlement,
        }
    )
    assert with_artifact.response_artifact == completed_artifact
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {**valid_journal.original.model_dump(mode="python"), "state": PendingPhase.QUARANTINED}
        )


def test_derived_expectation_fields_are_not_persisted(valid_journal: PendingJournal) -> None:
    assert "status_bootstrap" not in PendingExchange.model_fields
    assert "activation_candidate" not in PendingExchange.model_fields
    for field, value in (("status_bootstrap", False), ("activation_candidate", None)):
        with pytest.raises(ValidationError):
            PendingExchange.model_validate(
                {**valid_journal.original.model_dump(mode="python"), field: value}
            )


def test_intent_identity_filename_and_times_are_exact(valid_journal: PendingJournal) -> None:
    value = valid_journal.original.delivery_intents[0].model_dump(mode="python")
    for changes in (
        {"original_request_delivery_id": UUID("22222222-2222-4222-8222-222222222222")},
        {"response_filename": "CMOAgentBridge_Response_bad.inst"},
        {"intended_at_ms": -1},
        {"rendered_inbox_size_bytes": -1},
        {"published_at_ms": 99},
    ):
        with pytest.raises(ValidationError):
            DeliveryIntent.model_validate({**value, **changes})


def test_cancel_identity_and_delivery_cardinality_are_exact(valid_journal: PendingJournal) -> None:
    request = valid_journal.original.delivery_intents[0]
    cancel_tree = {
        **request.model_dump(mode="python"),
        "delivery_id": UUID("22222222-2222-4222-8222-222222222222"),
        "delivery_kind": "cancel",
        "original_request_delivery_id": request.delivery_id,
    }
    cancel = DeliveryIntent.model_validate(cancel_tree)
    with pytest.raises(ValidationError):
        DeliveryIntent.model_validate(
            {**cancel_tree, "original_request_delivery_id": cancel_tree["delivery_id"]}
        )
    published = _published_exchange_tree(valid_journal)
    request_tree = cast(tuple[dict[str, object], ...], published["delivery_intents"])[0]
    wrong_original = DeliveryIntent.model_validate(
        {
            **cancel_tree,
            "original_request_delivery_id": UUID("55555555-5555-4555-8555-555555555555"),
        }
    )
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {
                **published,
                "state": PendingPhase.CANCEL_PUBLISHED,
                "delivery_intents": (request_tree, wrong_original),
            }
        )
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {
                **published,
                "state": PendingPhase.CANCEL_PUBLISHED,
                "delivery_intents": (request_tree, cancel, cancel),
            }
        )
    duplicate_id = cancel.model_copy(update={"delivery_id": request.delivery_id})
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {
                **published,
                "state": PendingPhase.CANCEL_PUBLISHED,
                "delivery_intents": (request_tree, duplicate_id),
            }
        )


def test_original_target_pair_is_all_null_or_all_present(valid_journal: PendingJournal) -> None:
    value = valid_journal.original.model_dump(mode="python")
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {**value, "original_target_request_id": valid_journal.original.request_id}
        )
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {**value, "original_target_request_hash": valid_journal.original.request_hash}
        )


def test_exchange_time_invariants_reject_negative_and_backwards(
    valid_journal: PendingJournal,
) -> None:
    value = valid_journal.original.model_dump(mode="python")
    for changes in (
        {"revision": -1},
        {"created_at_ms": -1},
        {"updated_at_ms": -1},
        {"created_at_ms": 101, "updated_at_ms": 100},
    ):
        with pytest.raises(ValidationError):
            PendingExchange.model_validate({**value, **changes})


def test_no_settlement_artifact_is_response_crash_window_and_never_idle(
    valid_journal: PendingJournal,
    no_settlement_artifact: ResponseArtifact,
) -> None:
    published = _published_exchange_tree(valid_journal)
    response = PendingExchange.model_validate(
        {
            **published,
            "state": PendingPhase.RESPONSE_ACCEPTED,
            "response_artifact": no_settlement_artifact,
            "settlement": None,
        }
    )
    assert response.settlement is None
    quarantined = PendingExchange.model_validate(
        {**response.model_dump(mode="python"), "state": PendingPhase.QUARANTINED}
    )
    assert quarantined.response_artifact == no_settlement_artifact
    with pytest.raises(ValidationError):
        PendingExchange.model_validate(
            {**response.model_dump(mode="python"), "state": PendingPhase.IDLE_PUBLISHED}
        )


def test_journal_original_must_match_header_release(
    valid_journal: PendingJournal,
) -> None:
    value = valid_journal.model_dump(mode="python")
    with pytest.raises(ValidationError):
        PendingJournal.model_validate(
            {
                **value,
                "header": {**value["header"], "required_release_id": "f" * 64},
            }
        )
