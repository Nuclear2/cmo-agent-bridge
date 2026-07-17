from __future__ import annotations

import base64
import hashlib
import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Final, Literal, NamedTuple, cast, get_type_hints
from uuid import UUID

import pytest
from pydantic import BaseModel, ValidationError

import cmo_agent_bridge.application as application_module
import cmo_agent_bridge.application.confirmation as confirmation_module

from cmo_agent_bridge.application import (
    ConfirmationBinding,
    ConfirmationTokenStore,
    IssuedConfirmation,
)
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.state.sqlite import StateDatabase


SQLITE_INT_MAX = 2**63 - 1
ROOT_KEY = "a" * 64
LINEAGE_ID = UUID("11111111-1111-4111-8111-111111111111")
ACTIVATION_ID = UUID("22222222-2222-4222-8222-222222222222")
BINDING_SHA256 = "b" * 64
DESTRUCTIVE_FORMAT = "cmo-agent-bridge/destructive-confirmation/1"
DESTRUCTIVE_LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
DESTRUCTIVE_ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")
RELEASE_ID = "f" * 64


def _token(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


VALID_TOKEN = _token(bytes(range(32)))
UNKNOWN_TOKEN = _token(bytes(range(32, 64)))
OVERLONG_TOKEN = _token(bytes(range(33)))
VERY_LARGE_TOKEN = "A" * 1_000_000


@pytest.fixture
def binding() -> ConfirmationBinding:
    return ConfirmationBinding(
        root_key=ROOT_KEY,
        operation="unit.delete",
        binding_format="cmo-agent-bridge/destructive-confirmation/1",
        binding_sha256=BINDING_SHA256,
        lineage_id=LINEAGE_ID,
        activation_id=ACTIVATION_ID,
    )


def _fixed_factory(token: object, calls: list[int] | None = None) -> Callable[[int], str]:
    def factory(size: int) -> str:
        if calls is not None:
            calls.append(size)
        return cast(str, token)

    return factory


def _denial(call: Callable[[], object]) -> tuple[ErrorCode, str, dict[str, object]]:
    with pytest.raises(BridgeError) as caught:
        call()
    return caught.value.code, caught.value.message, caught.value.details


def test_confirmation_models_have_strict_frozen_exact_shapes(
    binding: ConfirmationBinding,
) -> None:
    assert tuple(ConfirmationBinding.model_fields) == (
        "root_key",
        "operation",
        "binding_format",
        "binding_sha256",
        "lineage_id",
        "activation_id",
    )
    assert tuple(IssuedConfirmation.model_fields) == ("token", "expires_at_ms")

    with pytest.raises(ValidationError, match="frozen"):
        setattr(binding, "operation", "mission.delete")
    with pytest.raises(ValidationError):
        ConfirmationBinding.model_validate(
            {
                **binding.model_dump(mode="python"),
                "lineage_id": str(LINEAGE_ID),
            }
        )
    with pytest.raises(ValidationError):
        ConfirmationBinding.model_validate(
            {**binding.model_dump(mode="python"), "unexpected": True}
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("operation", ""),
        ("binding_format", ""),
        ("binding_format", "destructive-confirmation/1"),
        ("binding_format", "cmo-agent-bridge//1"),
        ("binding_format", "cmo-agent-bridge/destructive-confirmation"),
        ("binding_format", "cmo-agent-bridge/destructive-confirmation/0"),
        ("binding_format", "cmo-agent-bridge/destructive-confirmation/v1"),
    ],
)
def test_binding_rejects_empty_operation_and_unversioned_format(
    binding: ConfirmationBinding, field: str, value: str
) -> None:
    with pytest.raises(ValidationError):
        ConfirmationBinding.model_validate({**binding.model_dump(mode="python"), field: value})


def test_issue_expires_at_exact_boundary_and_persists_only_sha256(
    tmp_path: Path, binding: ConfirmationBinding
) -> None:
    path = tmp_path / "state.sqlite3"
    calls: list[int] = []
    store = ConfirmationTokenStore(
        StateDatabase(path), token_factory=_fixed_factory(VALID_TOKEN, calls)
    )

    issued = store.issue(binding, now_ms=1_234)

    assert issued == IssuedConfirmation(token=VALID_TOKEN, expires_at_ms=61_234)
    assert calls == [32]
    token_sha256 = hashlib.sha256(VALID_TOKEN.encode("utf-8")).hexdigest()
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            """
            SELECT token_sha256,root_key,operation,binding_format,binding_sha256,
                   lineage_id,activation_id,expires_at_ms,used_at_ms
            FROM confirmations
            """
        ).fetchall() == [
            (
                token_sha256,
                binding.root_key,
                binding.operation,
                binding.binding_format,
                binding.binding_sha256,
                str(binding.lineage_id),
                str(binding.activation_id),
                61_234,
                None,
            )
        ]
    for artifact in tmp_path.glob("state.sqlite3*"):
        assert VALID_TOKEN.encode("utf-8") not in artifact.read_bytes()


def test_exact_binding_consume_returns_stored_hash_and_marks_used(
    tmp_path: Path, binding: ConfirmationBinding
) -> None:
    path = tmp_path / "state.sqlite3"
    store = ConfirmationTokenStore(StateDatabase(path), token_factory=_fixed_factory(VALID_TOKEN))
    issued = store.issue(binding, now_ms=100)

    proof = store.consume(issued.token, binding, now_ms=200)

    expected = hashlib.sha256(VALID_TOKEN.encode("utf-8")).hexdigest()
    assert proof == expected
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT token_sha256,used_at_ms FROM confirmations"
        ).fetchall() == [(expected, 200)]


@pytest.mark.parametrize(
    ("field", "changed"),
    [
        ("root_key", "c" * 64),
        ("operation", "mission.delete"),
        ("binding_format", "cmo-agent-bridge/reconcile-confirmation/1"),
        ("binding_sha256", "d" * 64),
        ("lineage_id", UUID("33333333-3333-4333-8333-333333333333")),
        ("activation_id", UUID("44444444-4444-4444-8444-444444444444")),
    ],
)
def test_every_binding_drift_denies_without_consuming(
    tmp_path: Path,
    binding: ConfirmationBinding,
    field: str,
    changed: object,
) -> None:
    path = tmp_path / "state.sqlite3"
    store = ConfirmationTokenStore(StateDatabase(path), token_factory=_fixed_factory(VALID_TOKEN))
    issued = store.issue(binding, now_ms=100)
    drifted = binding.model_copy(update={field: changed})

    denial = _denial(lambda: store.consume(issued.token, drifted, now_ms=200))

    assert denial[0] is ErrorCode.POLICY_DENIED
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT used_at_ms FROM confirmations").fetchone() == (None,)


def test_unknown_malformed_expired_at_expiry_and_reuse_share_generic_denial(
    tmp_path: Path, binding: ConfirmationBinding
) -> None:
    malformed_path = tmp_path / "malformed.sqlite3"
    malformed = ConfirmationTokenStore(StateDatabase(malformed_path))
    malformed_denial = _denial(
        lambda: malformed.consume("not a url-safe token!", binding, now_ms=100)
    )
    assert not malformed_path.exists()

    unknown = ConfirmationTokenStore(StateDatabase(tmp_path / "unknown.sqlite3"))
    unknown_denial = _denial(lambda: unknown.consume(UNKNOWN_TOKEN, binding, now_ms=100))

    at_expiry_store = ConfirmationTokenStore(
        StateDatabase(tmp_path / "at-expiry.sqlite3"),
        token_factory=_fixed_factory(VALID_TOKEN),
    )
    at_expiry_token = at_expiry_store.issue(binding, now_ms=100)
    at_expiry_denial = _denial(
        lambda: at_expiry_store.consume(at_expiry_token.token, binding, now_ms=60_100)
    )

    expired_store = ConfirmationTokenStore(
        StateDatabase(tmp_path / "expired.sqlite3"),
        token_factory=_fixed_factory(VALID_TOKEN),
    )
    expired_token = expired_store.issue(binding, now_ms=100)
    expired_denial = _denial(
        lambda: expired_store.consume(expired_token.token, binding, now_ms=60_101)
    )

    reused_store = ConfirmationTokenStore(
        StateDatabase(tmp_path / "reused.sqlite3"),
        token_factory=_fixed_factory(VALID_TOKEN),
    )
    reused_token = reused_store.issue(binding, now_ms=100)
    reused_store.consume(reused_token.token, binding, now_ms=200)
    reused_denial = _denial(lambda: reused_store.consume(reused_token.token, binding, now_ms=201))

    assert malformed_denial == unknown_denial == at_expiry_denial == expired_denial == reused_denial
    assert malformed_denial[0] is ErrorCode.POLICY_DENIED


@pytest.mark.parametrize(
    "token", [OVERLONG_TOKEN, VERY_LARGE_TOKEN], ids=["overlong", "very-large"]
)
def test_nonexact_length_token_denies_without_database_io(
    tmp_path: Path, binding: ConfirmationBinding, token: str
) -> None:
    path = tmp_path / "state.sqlite3"
    store = ConfirmationTokenStore(StateDatabase(path))

    denial = _denial(lambda: store.consume(token, binding, now_ms=100))

    assert denial == (ErrorCode.POLICY_DENIED, "confirmation denied", {})
    assert not path.exists()


def test_one_millisecond_before_expiry_is_valid(
    tmp_path: Path, binding: ConfirmationBinding
) -> None:
    store = ConfirmationTokenStore(
        StateDatabase(tmp_path / "state.sqlite3"),
        token_factory=_fixed_factory(VALID_TOKEN),
    )
    issued = store.issue(binding, now_ms=100)

    assert (
        store.consume(issued.token, binding, now_ms=60_099)
        == hashlib.sha256(VALID_TOKEN.encode("utf-8")).hexdigest()
    )


def test_two_concurrent_consumers_have_exactly_one_success(
    tmp_path: Path, binding: ConfirmationBinding
) -> None:
    path = tmp_path / "state.sqlite3"
    issuer = ConfirmationTokenStore(StateDatabase(path), token_factory=_fixed_factory(VALID_TOKEN))
    issued = issuer.issue(binding, now_ms=100)
    stores = (
        ConfirmationTokenStore(StateDatabase(path)),
        ConfirmationTokenStore(StateDatabase(path)),
    )
    barrier = Barrier(2)

    def consume(store: ConfirmationTokenStore) -> tuple[str, object]:
        barrier.wait(timeout=5)
        try:
            return "success", store.consume(issued.token, binding, now_ms=200)
        except BridgeError as error:
            return "error", error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [
            future.result(timeout=10) for future in [executor.submit(consume, s) for s in stores]
        ]

    assert sorted(kind for kind, _ in results) == ["error", "success"]
    assert {value for kind, value in results if kind == "error"} == {ErrorCode.POLICY_DENIED}
    assert {value for kind, value in results if kind == "success"} == {
        hashlib.sha256(VALID_TOKEN.encode("utf-8")).hexdigest()
    }


@pytest.mark.parametrize(
    ("corrupt_expiry", "storage_type"),
    [
        ("not-an-integer", "text"),
        (60_100.5, "real"),
        (sqlite3.Binary(b"\xff"), "blob"),
    ],
)
def test_noninteger_persisted_expiry_denies_without_consuming(
    tmp_path: Path,
    binding: ConfirmationBinding,
    corrupt_expiry: object,
    storage_type: str,
) -> None:
    path = tmp_path / "state.sqlite3"
    store = ConfirmationTokenStore(StateDatabase(path), token_factory=_fixed_factory(VALID_TOKEN))
    issued = store.issue(binding, now_ms=100)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE confirmations SET expires_at_ms=?",
            (corrupt_expiry,),
        )
        assert connection.execute("SELECT typeof(expires_at_ms) FROM confirmations").fetchone() == (
            storage_type,
        )

    denial = _denial(lambda: store.consume(issued.token, binding, now_ms=200))

    assert denial == (ErrorCode.POLICY_DENIED, "confirmation denied", {})
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT used_at_ms FROM confirmations").fetchone() == (None,)


@pytest.mark.parametrize("now_ms", [-1, True, SQLITE_INT_MAX - 59_999])
def test_invalid_issue_epoch_is_rejected_before_database_io(
    tmp_path: Path,
    binding: ConfirmationBinding,
    now_ms: object,
) -> None:
    path = tmp_path / "state.sqlite3"
    store = ConfirmationTokenStore(StateDatabase(path), token_factory=_fixed_factory(VALID_TOKEN))

    with pytest.raises(BridgeError) as caught:
        store.issue(binding, now_ms=cast(int, now_ms))

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert not path.exists()


@pytest.mark.parametrize("now_ms", [-1, True, SQLITE_INT_MAX + 1])
def test_invalid_consume_epoch_is_rejected_before_database_io(
    tmp_path: Path,
    binding: ConfirmationBinding,
    now_ms: object,
) -> None:
    path = tmp_path / "state.sqlite3"
    store = ConfirmationTokenStore(StateDatabase(path))

    with pytest.raises(BridgeError) as caught:
        store.consume(VALID_TOKEN, binding, now_ms=cast(int, now_ms))

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert not path.exists()


def test_invalid_dependency_and_forged_binding_fail_before_database_io(
    tmp_path: Path, binding: ConfirmationBinding
) -> None:
    with pytest.raises(BridgeError) as database_error:
        ConfirmationTokenStore(cast(StateDatabase, object()))
    assert database_error.value.code is ErrorCode.INVALID_ARGUMENT

    database = StateDatabase(tmp_path / "state.sqlite3")
    with pytest.raises(BridgeError) as factory_error:
        ConfirmationTokenStore(database, token_factory=cast(Callable[[int], str], 7))
    assert factory_error.value.code is ErrorCode.INVALID_ARGUMENT

    forged = ConfirmationBinding.model_construct(
        root_key="not-a-hash",
        operation=binding.operation,
        binding_format=binding.binding_format,
        binding_sha256=binding.binding_sha256,
        lineage_id=binding.lineage_id,
        activation_id=binding.activation_id,
    )
    store = ConfirmationTokenStore(database, token_factory=_fixed_factory(VALID_TOKEN))
    with pytest.raises(BridgeError) as binding_error:
        store.issue(forged, now_ms=100)
    assert binding_error.value.code is ErrorCode.INVALID_ARGUMENT
    assert not database.path.exists()


@pytest.mark.parametrize(
    "factory_output",
    [object(), "", "short", "not+urlsafe", VALID_TOKEN + "=", OVERLONG_TOKEN],
)
def test_invalid_token_factory_output_fails_without_database_io(
    tmp_path: Path,
    binding: ConfirmationBinding,
    factory_output: object,
) -> None:
    path = tmp_path / "state.sqlite3"
    store = ConfirmationTokenStore(
        StateDatabase(path), token_factory=_fixed_factory(factory_output)
    )

    with pytest.raises(BridgeError) as caught:
        store.issue(binding, now_ms=100)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert not path.exists()


def test_repeated_token_hash_collision_retries_then_fails_without_overwrite(
    tmp_path: Path, binding: ConfirmationBinding
) -> None:
    path = tmp_path / "state.sqlite3"
    calls: list[int] = []
    store = ConfirmationTokenStore(
        StateDatabase(path), token_factory=_fixed_factory(VALID_TOKEN, calls)
    )
    first = store.issue(binding, now_ms=100)

    with pytest.raises(BridgeError) as caught:
        store.issue(binding, now_ms=200)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert first.token == VALID_TOKEN
    assert calls == [32, 32, 32, 32]
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT expires_at_ms,used_at_ms,COUNT(*) FROM confirmations"
        ).fetchone() == (60_100, None, 1)


def test_hash_collision_then_distinct_token_succeeds(
    tmp_path: Path, binding: ConfirmationBinding
) -> None:
    path = tmp_path / "state.sqlite3"
    calls: list[int] = []
    tokens = iter((VALID_TOKEN, VALID_TOKEN, UNKNOWN_TOKEN))

    def token_factory(size: int) -> str:
        calls.append(size)
        return next(tokens)

    store = ConfirmationTokenStore(StateDatabase(path), token_factory=token_factory)

    first = store.issue(binding, now_ms=100)
    second = store.issue(binding, now_ms=200)

    assert (first.token, second.token) == (VALID_TOKEN, UNKNOWN_TOKEN)
    assert calls == [32, 32, 32]
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT token_sha256 FROM confirmations ORDER BY token_sha256"
        ).fetchall() == sorted(
            [
                (hashlib.sha256(VALID_TOKEN.encode("utf-8")).hexdigest(),),
                (hashlib.sha256(UNKNOWN_TOKEN.encode("utf-8")).hexdigest(),),
            ]
        )


class _DestructiveApi(NamedTuple):
    target: type[BaseModel]
    descriptor: type[BaseModel]
    canonical_bytes: Callable[[BaseModel], bytes]
    binding: Callable[[BaseModel], ConfirmationBinding]
    constant: str


class _StringSubclass(str):
    pass


def _destructive_api() -> _DestructiveApi:
    names = (
        "DESTRUCTIVE_CONFIRMATION_FORMAT",
        "DestructiveTarget",
        "DestructiveConfirmationDescriptor",
        "canonical_destructive_confirmation_bytes",
        "destructive_confirmation_binding",
    )
    missing = [name for name in names if not hasattr(confirmation_module, name)]
    assert not missing, f"missing F1 confirmation surface: {', '.join(missing)}"
    target = getattr(confirmation_module, "DestructiveTarget")
    descriptor = getattr(confirmation_module, "DestructiveConfirmationDescriptor")
    canonical_bytes = getattr(confirmation_module, "canonical_destructive_confirmation_bytes")
    binding_function = getattr(confirmation_module, "destructive_confirmation_binding")
    constant = getattr(confirmation_module, "DESTRUCTIVE_CONFIRMATION_FORMAT")
    assert isinstance(target, type) and issubclass(target, BaseModel)
    assert isinstance(descriptor, type) and issubclass(descriptor, BaseModel)
    assert callable(canonical_bytes)
    assert callable(binding_function)
    assert type(constant) is str
    return _DestructiveApi(
        target=target,
        descriptor=descriptor,
        canonical_bytes=cast(Callable[[BaseModel], bytes], canonical_bytes),
        binding=cast(Callable[[BaseModel], ConfirmationBinding], binding_function),
        constant=constant,
    )


def _unit_descriptor(api: _DestructiveApi, **updates: object) -> BaseModel:
    values: dict[str, object] = {
        "format": DESTRUCTIVE_FORMAT,
        "root_key": ROOT_KEY,
        "operation": "unit.delete",
        "public_arguments": {"unit_guid": "UNIT-1"},
        "resolved_target": {"guid": "UNIT-1", "name": "Alpha", "type": "Aircraft"},
        "scenario_lineage_id": DESTRUCTIVE_LINEAGE_ID,
        "reserved_activation_id": DESTRUCTIVE_ACTIVATION_ID,
        "release_id": RELEASE_ID,
    }
    values.update(updates)
    return api.descriptor.model_validate(values)


def _mission_descriptor(api: _DestructiveApi, **updates: object) -> BaseModel:
    values: dict[str, object] = {
        "format": DESTRUCTIVE_FORMAT,
        "root_key": ROOT_KEY,
        "operation": "mission.delete",
        "public_arguments": {"mission_guid": "MISSION-1", "side_guid": "SIDE-1"},
        "resolved_target": {
            "guid": "MISSION-1",
            "name": "CAP North",
            "type": "patrol",
        },
        "scenario_lineage_id": DESTRUCTIVE_LINEAGE_ID,
        "reserved_activation_id": DESTRUCTIVE_ACTIVATION_ID,
        "release_id": RELEASE_ID,
    }
    values.update(updates)
    return api.descriptor.model_validate(values)


def _lookup_active(
    store: ConfirmationTokenStore, token: object, *, now_ms: object
) -> ConfirmationBinding:
    lookup = getattr(store, "lookup_active", None)
    assert callable(lookup), "missing F1 ConfirmationTokenStore.lookup_active"
    return cast(ConfirmationBinding, lookup(token, now_ms=now_ms))


UNIT_CANONICAL_JSON = (
    '{"format":"cmo-agent-bridge/destructive-confirmation/1",'
    '"operation":"unit.delete","public_arguments":{"unit_guid":"UNIT-1"},'
    '"release_id":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",'
    '"reserved_activation_id":"44444444-4444-4444-8444-444444444444",'
    '"resolved_target":{"guid":"UNIT-1","name":"Alpha","type":"Aircraft"},'
    '"root_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    '"scenario_lineage_id":"33333333-3333-4333-8333-333333333333"}'
)
MISSION_CANONICAL_JSON = (
    '{"format":"cmo-agent-bridge/destructive-confirmation/1",'
    '"operation":"mission.delete",'
    '"public_arguments":{"mission_guid":"MISSION-1","side_guid":"SIDE-1"},'
    '"release_id":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",'
    '"reserved_activation_id":"44444444-4444-4444-8444-444444444444",'
    '"resolved_target":{"guid":"MISSION-1","name":"CAP North","type":"patrol"},'
    '"root_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    '"scenario_lineage_id":"33333333-3333-4333-8333-333333333333"}'
)


def test_destructive_confirmation_surface_is_exported_with_exact_literal_constant() -> None:
    api = _destructive_api()
    public_names = (
        "DESTRUCTIVE_CONFIRMATION_FORMAT",
        "DestructiveTarget",
        "DestructiveConfirmationDescriptor",
        "canonical_destructive_confirmation_bytes",
        "destructive_confirmation_binding",
    )

    assert api.constant == DESTRUCTIVE_FORMAT
    assert (
        get_type_hints(confirmation_module)["DESTRUCTIVE_CONFIRMATION_FORMAT"]
        == Final[Literal[DESTRUCTIVE_FORMAT]]
    )
    assert api.descriptor.model_fields["format"].annotation == Literal[DESTRUCTIVE_FORMAT]
    for name in public_names:
        assert name in application_module.__all__
        assert getattr(application_module, name) is getattr(confirmation_module, name)


@pytest.mark.parametrize(
    ("model_name", "expected_fields"),
    [
        ("target", ("guid", "name", "type")),
        (
            "descriptor",
            (
                "format",
                "root_key",
                "operation",
                "public_arguments",
                "resolved_target",
                "scenario_lineage_id",
                "reserved_activation_id",
                "release_id",
            ),
        ),
    ],
)
def test_destructive_models_have_exact_strict_frozen_shapes(
    model_name: str, expected_fields: tuple[str, ...]
) -> None:
    api = _destructive_api()
    model = api.target if model_name == "target" else api.descriptor

    assert tuple(model.model_fields) == expected_fields
    assert model.model_config.get("extra") == "forbid"
    assert model.model_config.get("frozen") is True
    assert model.model_config.get("strict") is True
    assert model.model_config.get("revalidate_instances") == "always"

    value = (
        model.model_validate({"guid": "UNIT-1", "name": "Alpha", "type": "Aircraft"})
        if model_name == "target"
        else _unit_descriptor(api)
    )
    with pytest.raises(ValidationError, match="frozen"):
        setattr(value, next(iter(expected_fields)), "changed")


@pytest.mark.parametrize(
    "field", ["token", "expires_at_ms", "confirmation_proof", "impact", "request_id"]
)
def test_descriptor_forbids_confirmation_and_preview_only_fields(field: str) -> None:
    api = _destructive_api()
    values = _unit_descriptor(api).model_dump(mode="python")
    values[field] = "forbidden"

    with pytest.raises(ValidationError):
        api.descriptor.model_validate(values)


def test_target_forbids_mutable_fields_beyond_guid_name_and_type() -> None:
    api = _destructive_api()

    with pytest.raises(ValidationError):
        api.target.model_validate(
            {"guid": "UNIT-1", "name": "Alpha", "type": "Aircraft", "latitude": 1.0}
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("guid", ""),
        ("guid", 1),
        ("guid", _StringSubclass("UNIT-1")),
        ("name", 1),
        ("name", _StringSubclass("Alpha")),
        ("type", 1),
        ("type", _StringSubclass("Aircraft")),
    ],
)
def test_target_requires_exact_builtin_strings_and_nonempty_guid(field: str, value: object) -> None:
    api = _destructive_api()
    values: dict[str, object] = {"guid": "UNIT-1", "name": "Alpha", "type": "Aircraft"}
    values[field] = value

    with pytest.raises(ValidationError):
        api.target.model_validate(values)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("format", _StringSubclass(DESTRUCTIVE_FORMAT)),
        ("root_key", _StringSubclass(ROOT_KEY)),
        ("operation", _StringSubclass("unit.delete")),
        ("scenario_lineage_id", str(DESTRUCTIVE_LINEAGE_ID)),
        ("reserved_activation_id", str(DESTRUCTIVE_ACTIVATION_ID)),
        ("reserved_activation_id", UUID("11111111-1111-1111-8111-111111111111")),
        ("release_id", _StringSubclass(RELEASE_ID)),
    ],
)
def test_descriptor_requires_exact_strings_uuids_and_reserved_uuid4(
    field: str, value: object
) -> None:
    api = _destructive_api()
    values = _unit_descriptor(api).model_dump(mode="python")
    values[field] = value

    with pytest.raises(ValidationError):
        api.descriptor.model_validate(values)


@pytest.mark.parametrize(
    ("operation", "arguments", "target_guid"),
    [
        ("unit.delete", {}, "UNIT-1"),
        ("unit.delete", {"unit_guid": "UNIT-1", "extra": "x"}, "UNIT-1"),
        ("unit.delete", {"unit_guid": "UNIT-2"}, "UNIT-1"),
        ("unit.delete", {"mission_guid": "UNIT-1"}, "UNIT-1"),
        ("unit.delete", {"unit_guid": 1}, "UNIT-1"),
        ("mission.delete", {"mission_guid": "MISSION-1"}, "MISSION-1"),
        ("mission.delete", {"side_guid": "SIDE-1"}, "MISSION-1"),
        (
            "mission.delete",
            {"mission_guid": "MISSION-1", "side_guid": "SIDE-1", "extra": "x"},
            "MISSION-1",
        ),
        (
            "mission.delete",
            {"mission_guid": "MISSION-2", "side_guid": "SIDE-1"},
            "MISSION-1",
        ),
        ("mission.delete", {"mission_guid": "MISSION-1", "side_guid": ""}, "MISSION-1"),
        ("mission.delete", {"mission_guid": "", "side_guid": "SIDE-1"}, "MISSION-1"),
        ("mission.delete", {"mission_guid": "MISSION-1", "side_guid": 1}, "MISSION-1"),
        ("side.delete", {"side_guid": "SIDE-1"}, "SIDE-1"),
    ],
)
def test_descriptor_enforces_exact_operation_public_target_invariants(
    operation: str, arguments: dict[object, object], target_guid: str
) -> None:
    api = _destructive_api()
    values = _unit_descriptor(api).model_dump(mode="python")
    values.update(
        operation=operation,
        public_arguments=arguments,
        resolved_target={"guid": target_guid, "name": "Target", "type": "kind"},
    )

    with pytest.raises(ValidationError):
        api.descriptor.model_validate(values)


@pytest.mark.parametrize(
    "arguments",
    [
        {_StringSubclass("unit_guid"): "UNIT-1"},
        {"unit_guid": _StringSubclass("UNIT-1")},
    ],
)
def test_public_arguments_require_exact_builtin_string_keys_and_values(
    arguments: dict[object, object],
) -> None:
    api = _destructive_api()

    with pytest.raises(ValidationError):
        _unit_descriptor(api, public_arguments=arguments)


@pytest.mark.parametrize(
    ("factory", "expected_json", "expected_sha256"),
    [
        (
            _unit_descriptor,
            UNIT_CANONICAL_JSON,
            "fa9c27bf21f3cf141ee74205d35cece84177aa643f1b0e657950a9f91f9983ae",
        ),
        (
            _mission_descriptor,
            MISSION_CANONICAL_JSON,
            "a09f05c1f871b057e67aeb279a34065a23adc1bdacebb63719df32d3511058b7",
        ),
    ],
    ids=["unit-delete", "mission-delete"],
)
def test_destructive_confirmation_literal_canonical_vectors(
    factory: Callable[[_DestructiveApi], BaseModel],
    expected_json: str,
    expected_sha256: str,
) -> None:
    api = _destructive_api()
    descriptor = factory(api)

    canonical = api.canonical_bytes(descriptor)
    projected = api.binding(descriptor)

    assert canonical == expected_json.encode("utf-8")
    assert hashlib.sha256(canonical).hexdigest() == expected_sha256
    assert type(projected) is ConfirmationBinding
    assert projected == ConfirmationBinding(
        root_key=ROOT_KEY,
        operation=cast(str, getattr(descriptor, "operation")),
        binding_format=DESTRUCTIVE_FORMAT,
        binding_sha256=expected_sha256,
        lineage_id=DESTRUCTIVE_LINEAGE_ID,
        activation_id=DESTRUCTIVE_ACTIVATION_ID,
    )


@pytest.mark.parametrize(
    "field",
    [
        "root_key",
        "operation",
        "public_arguments",
        "resolved_target",
        "scenario_lineage_id",
        "reserved_activation_id",
        "release_id",
    ],
)
def test_every_permitted_descriptor_field_drift_changes_digest(field: str) -> None:
    api = _destructive_api()
    baseline = _mission_descriptor(api)
    if field == "root_key":
        drifted = _mission_descriptor(api, root_key="c" * 64)
    elif field == "operation":
        drifted = _unit_descriptor(
            api,
            public_arguments={"unit_guid": "MISSION-1"},
            resolved_target={"guid": "MISSION-1", "name": "CAP North", "type": "patrol"},
        )
    elif field == "public_arguments":
        drifted = _mission_descriptor(
            api, public_arguments={"mission_guid": "MISSION-1", "side_guid": "SIDE-2"}
        )
    elif field == "resolved_target":
        drifted = _mission_descriptor(
            api,
            resolved_target={"guid": "MISSION-1", "name": "CAP South", "type": "patrol"},
        )
    elif field == "scenario_lineage_id":
        drifted = _mission_descriptor(
            api, scenario_lineage_id=UUID("55555555-5555-4555-8555-555555555555")
        )
    elif field == "reserved_activation_id":
        drifted = _mission_descriptor(
            api, reserved_activation_id=UUID("66666666-6666-4666-8666-666666666666")
        )
    else:
        drifted = _mission_descriptor(api, release_id="e" * 64)

    assert api.binding(drifted).binding_sha256 != api.binding(baseline).binding_sha256


def test_unicode_case_and_whitespace_are_not_normalized() -> None:
    api = _destructive_api()
    composed = _unit_descriptor(
        api,
        public_arguments={"unit_guid": " UNIT-é "},
        resolved_target={"guid": " UNIT-é ", "name": "Café", "type": "Aircraft"},
    )
    decomposed = _unit_descriptor(
        api,
        public_arguments={"unit_guid": " unit-é "},
        resolved_target={"guid": " unit-é ", "name": "Café", "type": "Aircraft"},
    )

    composed_bytes = api.canonical_bytes(composed)
    decomposed_bytes = api.canonical_bytes(decomposed)

    assert b"\\u00e9" not in composed_bytes
    assert "é".encode() in composed_bytes
    assert composed_bytes != decomposed_bytes
    assert api.binding(composed).binding_sha256 != api.binding(decomposed).binding_sha256


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("format", "cmo-agent-bridge/destructive-confirmation/2"),
        ("root_key", "not-a-hash"),
        ("operation", "side.delete"),
        ("public_arguments", {"unit_guid": "UNIT-2"}),
        ("scenario_lineage_id", str(DESTRUCTIVE_LINEAGE_ID)),
        ("reserved_activation_id", UUID("11111111-1111-1111-8111-111111111111")),
        ("release_id", "F" * 64),
    ],
)
@pytest.mark.parametrize("function_name", ["canonical_bytes", "binding"])
def test_helpers_revalidate_model_construct_corruption(
    field: str, value: object, function_name: str
) -> None:
    api = _destructive_api()
    values = _unit_descriptor(api).model_dump(mode="python")
    values[field] = value
    forged = api.descriptor.model_construct(**values)
    function = api.canonical_bytes if function_name == "canonical_bytes" else api.binding

    with pytest.raises((BridgeError, TypeError, ValueError, ValidationError)):
        function(forged)


@pytest.mark.parametrize("function_name", ["canonical_bytes", "binding"])
def test_helpers_revalidate_nested_target_model_construct_corruption(
    function_name: str,
) -> None:
    api = _destructive_api()
    values = _unit_descriptor(api).model_dump(mode="python")
    values["resolved_target"] = api.target.model_construct(guid="", name="Alpha", type="Aircraft")
    forged = api.descriptor.model_construct(**values)
    function = api.canonical_bytes if function_name == "canonical_bytes" else api.binding

    with pytest.raises((BridgeError, TypeError, ValueError, ValidationError)):
        function(forged)


def test_lookup_active_returns_exact_binding_without_mutating_row(
    tmp_path: Path, binding: ConfirmationBinding
) -> None:
    path = tmp_path / "state.sqlite3"
    store = ConfirmationTokenStore(StateDatabase(path), token_factory=_fixed_factory(VALID_TOKEN))
    issued = store.issue(binding, now_ms=100)
    with sqlite3.connect(path) as connection:
        before = connection.execute("SELECT rowid,* FROM confirmations").fetchone()

    loaded = _lookup_active(store, issued.token, now_ms=60_099)

    assert type(loaded) is ConfirmationBinding
    assert loaded == binding
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT rowid,* FROM confirmations").fetchone() == before


def test_lookup_active_expiry_boundary_is_exclusive(
    tmp_path: Path, binding: ConfirmationBinding
) -> None:
    store = ConfirmationTokenStore(
        StateDatabase(tmp_path / "state.sqlite3"), token_factory=_fixed_factory(VALID_TOKEN)
    )
    issued = store.issue(binding, now_ms=100)

    assert _lookup_active(store, issued.token, now_ms=60_099) == binding
    assert _denial(lambda: _lookup_active(store, issued.token, now_ms=60_100)) == (
        ErrorCode.POLICY_DENIED,
        "confirmation denied",
        {},
    )


@pytest.mark.parametrize(
    "token",
    [
        pytest.param(None, id="none"),
        pytest.param(7, id="integer"),
        pytest.param(True, id="bool"),
        pytest.param("", id="empty"),
        pytest.param("short", id="short"),
        pytest.param("not+urlsafe", id="alphabet"),
        pytest.param(VALID_TOKEN + "=", id="padding"),
        pytest.param(OVERLONG_TOKEN, id="overlong"),
        pytest.param(VERY_LARGE_TOKEN, id="very-large"),
    ],
)
def test_lookup_malformed_token_denies_without_database_io(tmp_path: Path, token: object) -> None:
    path = tmp_path / "state.sqlite3"
    store = ConfirmationTokenStore(StateDatabase(path))

    assert _denial(lambda: _lookup_active(store, token, now_ms=100)) == (
        ErrorCode.POLICY_DENIED,
        "confirmation denied",
        {},
    )
    assert not path.exists()


@pytest.mark.parametrize("now_ms", [-1, True, 2**63])
def test_lookup_invalid_epoch_is_rejected_before_database_io(
    tmp_path: Path, now_ms: object
) -> None:
    path = tmp_path / "state.sqlite3"
    store = ConfirmationTokenStore(StateDatabase(path))

    with pytest.raises(BridgeError) as caught:
        _lookup_active(store, VALID_TOKEN, now_ms=now_ms)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert caught.value.details == {}
    assert not path.exists()


def test_lookup_unknown_expired_and_reused_share_generic_denial(
    tmp_path: Path, binding: ConfirmationBinding
) -> None:
    unknown_store = ConfirmationTokenStore(StateDatabase(tmp_path / "unknown.sqlite3"))
    unknown = _denial(lambda: _lookup_active(unknown_store, UNKNOWN_TOKEN, now_ms=100))

    expired_store = ConfirmationTokenStore(
        StateDatabase(tmp_path / "expired.sqlite3"), token_factory=_fixed_factory(VALID_TOKEN)
    )
    expired_token = expired_store.issue(binding, now_ms=100)
    expired = _denial(lambda: _lookup_active(expired_store, expired_token.token, now_ms=60_100))

    reused_store = ConfirmationTokenStore(
        StateDatabase(tmp_path / "reused.sqlite3"), token_factory=_fixed_factory(VALID_TOKEN)
    )
    reused_token = reused_store.issue(binding, now_ms=100)
    reused_store.consume(reused_token.token, binding, now_ms=200)
    reused = _denial(lambda: _lookup_active(reused_store, reused_token.token, now_ms=201))

    assert (
        unknown
        == expired
        == reused
        == (
            ErrorCode.POLICY_DENIED,
            "confirmation denied",
            {},
        )
    )


@pytest.mark.parametrize(
    ("column", "corrupt_value", "storage_type"),
    [
        pytest.param("token_sha256", "not-a-token-hash", "text", id="token-hash-text"),
        pytest.param("token_sha256", sqlite3.Binary(b"x" * 32), "blob", id="token-hash-blob"),
        pytest.param("root_key", "A" * 64, "text", id="root-key-uppercase"),
        pytest.param("root_key", sqlite3.Binary(b"a" * 64), "blob", id="root-key-blob"),
        pytest.param("operation", "", "text", id="operation-empty"),
        pytest.param("operation", sqlite3.Binary(b"unit.delete"), "blob", id="operation-blob"),
        pytest.param("binding_format", "destructive/1", "text", id="format-invalid"),
        pytest.param("binding_format", sqlite3.Binary(b"format"), "blob", id="format-blob"),
        pytest.param("binding_sha256", "D" * 64, "text", id="binding-hash-uppercase"),
        pytest.param("binding_sha256", sqlite3.Binary(b"d" * 64), "blob", id="binding-hash-blob"),
        pytest.param("lineage_id", "not-a-uuid", "text", id="lineage-malformed"),
        pytest.param(
            "lineage_id",
            "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
            "text",
            id="lineage-noncanonical",
        ),
        pytest.param("lineage_id", sqlite3.Binary(b"uuid"), "blob", id="lineage-blob"),
        pytest.param("activation_id", "not-a-uuid", "text", id="activation-malformed"),
        pytest.param(
            "activation_id",
            "BBBBBBBB-BBBB-4BBB-8BBB-BBBBBBBBBBBB",
            "text",
            id="activation-noncanonical",
        ),
        pytest.param("activation_id", sqlite3.Binary(b"uuid"), "blob", id="activation-blob"),
        pytest.param("expires_at_ms", "not-an-integer", "text", id="expiry-text"),
        pytest.param("expires_at_ms", 60_100.5, "real", id="expiry-real"),
        pytest.param("expires_at_ms", sqlite3.Binary(b"60100"), "blob", id="expiry-blob"),
        pytest.param("used_at_ms", "used", "text", id="used-text"),
        pytest.param("used_at_ms", 200.5, "real", id="used-real"),
        pytest.param("used_at_ms", sqlite3.Binary(b"used"), "blob", id="used-blob"),
    ],
)
def test_lookup_corrupt_stored_field_or_type_denies_without_mutation(
    tmp_path: Path,
    binding: ConfirmationBinding,
    column: str,
    corrupt_value: object,
    storage_type: str,
) -> None:
    path = tmp_path / "state.sqlite3"
    store = ConfirmationTokenStore(StateDatabase(path), token_factory=_fixed_factory(VALID_TOKEN))
    issued = store.issue(binding, now_ms=100)
    with sqlite3.connect(path) as connection:
        connection.execute(f"UPDATE confirmations SET {column}=?", (corrupt_value,))
        assert connection.execute(f"SELECT typeof({column}) FROM confirmations").fetchone() == (
            storage_type,
        )
        before = connection.execute("SELECT rowid,* FROM confirmations").fetchone()

    denial = _denial(lambda: _lookup_active(store, issued.token, now_ms=200))

    assert denial == (ErrorCode.POLICY_DENIED, "confirmation denied", {})
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT rowid,* FROM confirmations").fetchone() == before


def test_lookup_returns_only_confirmation_binding_without_proof_or_token_metadata(
    tmp_path: Path, binding: ConfirmationBinding
) -> None:
    store = ConfirmationTokenStore(
        StateDatabase(tmp_path / "state.sqlite3"), token_factory=_fixed_factory(VALID_TOKEN)
    )
    issued = store.issue(binding, now_ms=100)
    token_sha256 = hashlib.sha256(issued.token.encode()).hexdigest()

    loaded = _lookup_active(store, issued.token, now_ms=200)
    dumped = loaded.model_dump(mode="json")
    rendered = repr(loaded) + repr(dumped)

    assert type(loaded) is ConfirmationBinding
    assert tuple(dumped) == tuple(ConfirmationBinding.model_fields)
    for forbidden_attribute in (
        "token",
        "plaintext_token",
        "token_sha256",
        "confirmation_proof",
        "expires_at_ms",
        "rowid",
        "row_id",
    ):
        assert not hasattr(loaded, forbidden_attribute)
    assert issued.token not in rendered
    assert token_sha256 not in rendered
    assert str(issued.expires_at_ms) not in rendered
