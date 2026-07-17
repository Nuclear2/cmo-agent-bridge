from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Never, cast
from uuid import UUID

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.models import (
    AllowedDelivery,
    ExchangeCommand,
    RequestBody,
    ResponseExpectation,
)
from cmo_agent_bridge.protocol.response import parse_inst_response
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CompletedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot


REQUEST_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeea401")
DELIVERY_ID = UUID("11111111-1111-4111-8111-11111111a401")
LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")


@dataclass(frozen=True, slots=True)
class _BindingCase:
    path: Path
    raw: bytes
    artifact: ResponseArtifact
    expectation: ResponseExpectation


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _command() -> ExchangeCommand:
    snapshot = RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256=MANIFEST_SHA256,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "unit.set",
        {"unit_guid": "UNIT-1", "name": "Artifact binding"},
    )
    body = RequestBody(
        protocol=snapshot.protocol,
        release_id=snapshot.release_id,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=ACTIVATION_ID,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        operation="unit.set",
        arguments=invocation.wire_arguments.model_dump(mode="json"),
    )
    return ExchangeCommand(
        request_id=REQUEST_ID,
        body=body,
        invocation=invocation,
        runtime_snapshot=snapshot,
        timeout=30,
    )


def _case(tmp_path: Path) -> _BindingCase:
    command = _command()
    result_input = {
        "unit_guid": "UNIT-1",
        "name": "Artifact binding",
        "speed": None,
        "altitude": None,
        "heading": None,
        "course": None,
    }
    validated = command.invocation.result_adapter.validate_python(result_input)
    result = command.invocation.result_adapter.dump_python(validated, mode="json")
    request_hash = hashlib.sha256(canonical_body_bytes(command.body)).hexdigest()
    envelope = ResponseEnvelope(
        protocol=command.runtime_snapshot.protocol,
        request_id=command.request_id,
        delivery_id=DELIVERY_ID,
        request_hash=request_hash,
        ok=True,
        result=result,
        error=None,
        scenario_time="2026-07-12T13:00:00Z",
        scenario_lineage_id=LINEAGE_ID,
        activation_id=ACTIVATION_ID,
        operation_manifest_sha256=command.runtime_snapshot.operation_manifest_sha256,
        bridge_version=command.runtime_snapshot.runtime_version,
        runtime_tag=command.runtime_snapshot.runtime_tag,
        runtime_asset_sha256=command.runtime_snapshot.runtime_asset_sha256,
        release_id=command.runtime_snapshot.release_id,
    )
    settlement = CompletedSettlement(state="completed", result=result)
    accepted = AcceptedResponse(
        envelope=envelope,
        delivery_kind="request",
        settlement=settlement,
        cancel_ack=None,
    )
    envelope_json = _canonical_json_bytes(accepted.envelope.model_dump(mode="json"))
    raw = _canonical_json_bytes({"Comments": envelope_json.decode("utf-8")})
    artifact = ResponseArtifact(
        filename=f"CMOAgentBridge_Response_{command.request_id}.inst",
        sha256=hashlib.sha256(raw).hexdigest(),
        size_bytes=len(raw),
        accepted_at_ms=104,
        accepted_response=accepted,
    )
    expectation = ResponseExpectation(
        request_id=command.request_id,
        allowed_deliveries=(
            AllowedDelivery(
                delivery_id=DELIVERY_ID,
                delivery_kind="request",
            ),
        ),
        request_hash=artifact.accepted_response.envelope.request_hash,
        expected_lineage_id=command.body.expected_lineage_id,
        expected_activation_id=command.body.expected_activation_id,
        status_bootstrap=False,
        activation_candidate=None,
        runtime_snapshot=command.runtime_snapshot,
        invocation=command.invocation,
    )
    path = tmp_path / artifact.filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return _BindingCase(
        path=path,
        raw=raw,
        artifact=artifact,
        expectation=expectation,
    )


def _bind(
    path: Path,
    artifact: ResponseArtifact,
    expectation: ResponseExpectation,
    *,
    parser: Callable[[bytes, ResponseExpectation], AcceptedResponse],
) -> ResponseArtifact:
    try:
        module = importlib.import_module("cmo_agent_bridge.transports.file_bridge.artifact_binding")
    except ModuleNotFoundError:
        pytest.fail("artifact_binding module is missing")
    function = getattr(module, "bind_durable_response", None)
    assert callable(function), "bind_durable_response() is missing"
    typed = cast(Callable[..., ResponseArtifact], function)
    return typed(path, artifact, expectation, parser=parser)


def _reason(error: BridgeError) -> str:
    reason = error.details.get("reason")
    assert type(reason) is str
    return reason


def _forbidden_parser(_raw: bytes, _expectation: ResponseExpectation) -> Never:
    raise AssertionError("parser was called before durable metadata matched")


def _different_accepted_response(artifact: ResponseArtifact) -> AcceptedResponse:
    tree = artifact.accepted_response.model_dump(
        mode="python",
        round_trip=True,
        warnings=False,
    )
    envelope = cast(dict[str, Any], tree["envelope"])
    settlement = cast(dict[str, Any], tree["settlement"])
    result = dict(cast(dict[str, Any], envelope["result"]))
    result["name"] = "Different accepted response"
    envelope["result"] = result
    settlement["result"] = result
    return AcceptedResponse.model_validate(tree)


def test_binding_reads_once_and_passes_the_same_bytes_object_to_parser(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path)
    sentinel = bytes(bytearray(case.raw))
    reads: list[Path] = []
    parser_inputs: list[tuple[bytes, ResponseExpectation]] = []

    def read_once(path: Path) -> bytes:
        assert path == case.path
        reads.append(path)
        return sentinel

    def parser(raw: bytes, expectation: ResponseExpectation) -> AcceptedResponse:
        parser_inputs.append((raw, expectation))
        assert raw is sentinel
        assert expectation is case.expectation
        return case.artifact.accepted_response

    monkeypatch.setattr(Path, "read_bytes", read_once)

    bound = _bind(
        case.path,
        case.artifact,
        case.expectation,
        parser=parser,
    )

    assert bound == case.artifact
    assert reads == [case.path]
    assert parser_inputs == [(sentinel, case.expectation)]
    with case.path.open("rb") as stream:
        assert stream.read() == case.raw


def test_binding_accepts_the_real_protocol_parser_without_rewriting_response(
    tmp_path: Path,
) -> None:
    case = _case(tmp_path)
    before = case.path.read_bytes()

    bound = _bind(
        case.path,
        case.artifact,
        case.expectation,
        parser=parse_inst_response,
    )

    assert bound == case.artifact
    assert case.path.read_bytes() == before


@pytest.mark.parametrize(
    ("case_name", "expected_reason"),
    [
        ("filename", "response_filename_mismatch"),
        ("size", "response_size_mismatch"),
        ("sha256", "response_sha256_mismatch"),
    ],
)
def test_binding_rejects_durable_metadata_mismatch_before_parser(
    tmp_path: Path,
    case_name: str,
    expected_reason: str,
) -> None:
    case = _case(tmp_path)
    path = case.path
    artifact = case.artifact
    if case_name == "filename":
        path = case.path.with_name("different-response.inst")
        path.write_bytes(case.raw)
    elif case_name == "size":
        artifact = artifact.model_copy(update={"size_bytes": artifact.size_bytes + 1})
    else:
        artifact = artifact.model_copy(update={"sha256": "f" * 64})
    before = path.read_bytes()

    with pytest.raises(BridgeError) as caught:
        _bind(
            path,
            artifact,
            case.expectation,
            parser=_forbidden_parser,
        )

    assert caught.value.code is ErrorCode.INDETERMINATE_OUTCOME
    assert _reason(caught.value) == expected_reason
    assert caught.value.details["request_id"] == str(case.expectation.request_id)
    assert path.read_bytes() == before


def test_binding_rejects_missing_response_without_creating_it(
    tmp_path: Path,
) -> None:
    case = _case(tmp_path)
    case.path.unlink()

    with pytest.raises(BridgeError) as caught:
        _bind(
            case.path,
            case.artifact,
            case.expectation,
            parser=_forbidden_parser,
        )

    assert caught.value.code is ErrorCode.INDETERMINATE_OUTCOME
    assert _reason(caught.value) == "response_missing"
    assert caught.value.details["request_id"] == str(case.expectation.request_id)
    assert not case.path.exists()


def test_binding_wraps_response_read_oserror_without_modifying_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path)
    before = case.path.read_bytes()
    sentinel = OSError("response read sentinel")

    def fail_read(path: Path) -> Never:
        assert path == case.path
        raise sentinel

    monkeypatch.setattr(Path, "read_bytes", fail_read)

    with pytest.raises(BridgeError) as caught:
        _bind(
            case.path,
            case.artifact,
            case.expectation,
            parser=_forbidden_parser,
        )

    assert caught.value.code is ErrorCode.INDETERMINATE_OUTCOME
    assert _reason(caught.value) == "response_read_failed"
    assert caught.value.details["request_id"] == str(case.expectation.request_id)
    assert caught.value.__cause__ is sentinel
    with case.path.open("rb") as stream:
        assert stream.read() == before


def test_binding_wraps_parser_failure_and_preserves_response(
    tmp_path: Path,
) -> None:
    case = _case(tmp_path)
    before = case.path.read_bytes()
    sentinel = RuntimeError("parser sentinel")

    def parser(_raw: bytes, _expectation: ResponseExpectation) -> Never:
        raise sentinel

    with pytest.raises(BridgeError) as caught:
        _bind(
            case.path,
            case.artifact,
            case.expectation,
            parser=parser,
        )

    assert caught.value.code is ErrorCode.INDETERMINATE_OUTCOME
    assert _reason(caught.value) == "response_parse_failed"
    assert caught.value.details["request_id"] == str(case.expectation.request_id)
    assert caught.value.__cause__ is sentinel
    assert case.path.read_bytes() == before


def test_binding_rejects_canonically_different_accepted_response(
    tmp_path: Path,
) -> None:
    case = _case(tmp_path)
    before = case.path.read_bytes()
    different = _different_accepted_response(case.artifact)

    def parser(_raw: bytes, _expectation: ResponseExpectation) -> AcceptedResponse:
        return different

    with pytest.raises(BridgeError) as caught:
        _bind(
            case.path,
            case.artifact,
            case.expectation,
            parser=parser,
        )

    assert caught.value.code is ErrorCode.INDETERMINATE_OUTCOME
    assert _reason(caught.value) == "accepted_response_mismatch"
    assert caught.value.details["request_id"] == str(case.expectation.request_id)
    assert case.path.read_bytes() == before
