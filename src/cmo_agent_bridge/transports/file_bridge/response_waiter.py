from __future__ import annotations

import asyncio
import hashlib
import math
import time
from collections.abc import Callable
from pathlib import Path
from typing import TypeGuard, cast

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.models import ResponseExpectation
from cmo_agent_bridge.protocol.response import (
    RetryableResponseJsonError,
    parse_inst_response,
)
from cmo_agent_bridge.protocol.response_models import ResponseArtifact
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo


def _invalid_argument(message: str) -> BridgeError:
    return BridgeError(ErrorCode.INVALID_ARGUMENT, message)


def _is_valid_process(value: object) -> TypeGuard[ProcessInfo]:
    if type(value) is not ProcessInfo:
        return False
    executable = cast(object, value.executable)
    return (
        type(value.pid) is int
        and value.pid > 0
        and type(value.create_time) is float
        and math.isfinite(value.create_time)
        and value.create_time > 0
        and isinstance(executable, Path)
    )


def _validated_process(value: object) -> ProcessInfo:
    if type(value) is not ProcessInfo:
        raise _invalid_argument("expected process must be an exact ProcessInfo")
    if type(value.pid) is not int or value.pid <= 0:
        raise _invalid_argument("expected process PID must be a positive exact integer")
    if (
        type(value.create_time) is not float
        or not math.isfinite(value.create_time)
        or value.create_time <= 0
    ):
        raise _invalid_argument(
            "expected process create time must be a positive finite exact float"
        )
    executable = cast(object, value.executable)
    if not isinstance(executable, Path):
        raise _invalid_argument("expected process executable must be a pathlib.Path")
    return value


def _process_details(process: ProcessInfo) -> dict[str, object]:
    return {
        "pid": process.pid,
        "create_time": process.create_time,
        "executable": str(process.executable),
    }


def _validated_number(
    value: object,
    *,
    description: str,
    allow_zero: bool,
) -> float:
    if type(value) not in {int, float}:
        raise _invalid_argument(f"{description} must be an exact int or float")
    try:
        validated = float(cast(int | float, value))
    except OverflowError as error:
        raise _invalid_argument(f"{description} must be finite") from error
    if not math.isfinite(validated) or validated < 0 or (validated == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "strictly positive"
        raise _invalid_argument(f"{description} must be finite and {qualifier}")
    return validated


class ResponseWaiter:
    def __init__(
        self,
        response_path: Path,
        expectation: ResponseExpectation,
        expected_process: ProcessInfo,
        process_check: Callable[[], ProcessInfo],
        poll_seconds: float = 0.05,
    ) -> None:
        if type(expectation) is not ResponseExpectation:
            raise _invalid_argument("expectation must be an exact ResponseExpectation")
        raw_response_path = cast(object, response_path)
        if not isinstance(raw_response_path, Path):
            raise _invalid_argument("response path must be a pathlib.Path")
        expected_filename = f"CMOAgentBridge_Response_{expectation.request_id}.inst"
        if response_path.name != expected_filename:
            raise _invalid_argument("response path filename does not match the expected request ID")
        validated_process = _validated_process(expected_process)
        if not callable(process_check):
            raise _invalid_argument("process check must be callable")

        self._response_path = response_path
        self._expectation = expectation
        self._expected_process = validated_process
        self._process_check = process_check
        self._poll_seconds = _validated_number(
            poll_seconds,
            description="poll interval",
            allow_zero=False,
        )

    async def wait(self, timeout_seconds: float) -> ResponseArtifact:
        timeout = _validated_number(
            timeout_seconds,
            description="timeout",
            allow_zero=True,
        )
        deadline = time.monotonic() + timeout
        first_poll = True
        last_json_error: RetryableResponseJsonError | None = None

        while True:
            if not first_poll and time.monotonic() >= deadline:
                if last_json_error is not None:
                    raise last_json_error
                raise self._timeout_error(timeout)
            first_poll = False

            self._check_process()
            try:
                raw = self._response_path.read_bytes()
            except (FileNotFoundError, PermissionError):
                pass
            except OSError as error:
                details: dict[str, object] = {
                    "response_path": str(self._response_path),
                    "type": type(error).__name__,
                }
                winerror = getattr(error, "winerror", None)
                if type(winerror) is int:
                    details["winerror"] = winerror
                raise BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "CMO response file could not be read",
                    details,
                ) from error
            else:
                try:
                    accepted = parse_inst_response(raw, self._expectation)
                except RetryableResponseJsonError as error:
                    last_json_error = error
                else:
                    raw_sha256 = hashlib.sha256(raw).hexdigest()
                    raw_size = len(raw)
                    accepted_at_ms = time.time_ns() // 1_000_000
                    filename = f"CMOAgentBridge_Response_{accepted.envelope.request_id}.inst"
                    return ResponseArtifact(
                        filename=filename,
                        sha256=raw_sha256,
                        size_bytes=raw_size,
                        accepted_at_ms=accepted_at_ms,
                        accepted_response=accepted,
                    )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if last_json_error is not None:
                    raise last_json_error
                raise self._timeout_error(timeout)
            await asyncio.sleep(min(self._poll_seconds, remaining))

    def _check_process(self) -> None:
        try:
            actual = self._process_check()
        except BridgeError:
            raise
        except Exception as error:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "CMO process check failed",
                {"type": type(error).__name__},
            ) from error

        if not _is_valid_process(actual):
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "CMO process check returned invalid process identity",
                {"type": type(actual).__name__},
            )
        if actual != self._expected_process:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "CMO process identity changed while waiting for response",
                {
                    "expected_process": _process_details(self._expected_process),
                    "actual_process": _process_details(actual),
                },
            )

    def _timeout_error(self, timeout_seconds: float) -> BridgeError:
        return BridgeError(
            ErrorCode.REQUEST_TIMEOUT,
            "timed out waiting for a correlated CMO response",
            {
                "request_id": str(self._expectation.request_id),
                "response_path": str(self._response_path),
                "timeout_seconds": timeout_seconds,
            },
        )
