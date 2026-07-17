from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import cast

from pydantic import ValidationError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.models import ResponseExpectation
from cmo_agent_bridge.protocol.response_models import AcceptedResponse, ResponseArtifact


ResponseParser = Callable[[bytes, ResponseExpectation], AcceptedResponse]


def _invalid_argument(message: str) -> BridgeError:
    return BridgeError(ErrorCode.INVALID_ARGUMENT, message)


def _binding_error(
    *,
    reason: str,
    request_id: object,
    path: Path,
) -> BridgeError:
    return BridgeError(
        ErrorCode.INDETERMINATE_OUTCOME,
        "durable response artifact could not be bound to its raw file",
        {
            "reason": reason,
            "request_id": str(request_id),
            "response_path": str(path),
        },
    )


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def bind_durable_response(
    path: Path,
    artifact: ResponseArtifact,
    expectation: ResponseExpectation,
    *,
    parser: ResponseParser,
) -> ResponseArtifact:
    """Bind durable response metadata and semantics to one exact raw file read."""

    if not isinstance(cast(object, path), Path):
        raise _invalid_argument("durable response path must be a pathlib.Path")
    if type(expectation) is not ResponseExpectation:
        raise _invalid_argument("durable response expectation must be exact")
    if type(artifact) is not ResponseArtifact:
        raise _invalid_argument("durable response artifact must be exact")
    if not callable(parser):
        raise _invalid_argument("durable response parser must be callable")

    request_id = expectation.request_id
    try:
        durable = ResponseArtifact.model_validate(
            artifact.model_dump(mode="python", round_trip=True, warnings=False)
        )
    except (
        AttributeError,
        ValidationError,
        TypeError,
        ValueError,
        OverflowError,
        RecursionError,
    ) as error:
        raise _binding_error(
            reason="durable_artifact_invalid",
            request_id=request_id,
            path=path,
        ) from error

    if durable.accepted_response.envelope.request_id != request_id:
        raise _binding_error(
            reason="response_request_mismatch",
            request_id=request_id,
            path=path,
        )
    if path.name != durable.filename:
        raise _binding_error(
            reason="response_filename_mismatch",
            request_id=request_id,
            path=path,
        )

    try:
        raw = path.read_bytes()
    except FileNotFoundError as error:
        raise _binding_error(
            reason="response_missing",
            request_id=request_id,
            path=path,
        ) from error
    except OSError as error:
        raise _binding_error(
            reason="response_read_failed",
            request_id=request_id,
            path=path,
        ) from error

    if type(raw) is not bytes:
        raise _binding_error(
            reason="response_read_failed",
            request_id=request_id,
            path=path,
        )
    if len(raw) != durable.size_bytes:
        raise _binding_error(
            reason="response_size_mismatch",
            request_id=request_id,
            path=path,
        )
    if hashlib.sha256(raw).hexdigest() != durable.sha256:
        raise _binding_error(
            reason="response_sha256_mismatch",
            request_id=request_id,
            path=path,
        )

    try:
        parsed = parser(raw, expectation)
        if type(parsed) is not AcceptedResponse:
            raise TypeError("response parser did not return an exact AcceptedResponse")
        accepted = AcceptedResponse.model_validate(
            parsed.model_dump(mode="python", round_trip=True, warnings=False)
        )
    except Exception as error:
        raise _binding_error(
            reason="response_parse_failed",
            request_id=request_id,
            path=path,
        ) from error

    try:
        parsed_bytes = _canonical_json_bytes(accepted.model_dump(mode="json"))
        durable_bytes = _canonical_json_bytes(durable.accepted_response.model_dump(mode="json"))
    except (TypeError, ValueError, OverflowError, RecursionError) as error:
        raise _binding_error(
            reason="accepted_response_mismatch",
            request_id=request_id,
            path=path,
        ) from error
    if parsed_bytes != durable_bytes:
        raise _binding_error(
            reason="accepted_response_mismatch",
            request_id=request_id,
            path=path,
        )
    return durable
