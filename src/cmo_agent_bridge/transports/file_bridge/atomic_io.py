from __future__ import annotations

import math
import os
import tempfile
import time
from pathlib import Path
from typing import IO, Callable, cast

from cmo_agent_bridge.errors import BridgeError, ErrorCode


_SHARING_VIOLATIONS = frozenset({32, 33})
_INITIAL_RETRY_DELAY_SECONDS = 0.01
_MAX_RETRY_DELAY_SECONDS = 0.1


def _validate_retry_seconds(retry_seconds: float) -> float:
    value = cast(object, retry_seconds)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "replace retry seconds must be a finite non-negative number",
        )
    return float(value)


def _sharing_timeout(target: Path, retry_window: float, error: OSError) -> BridgeError:
    return BridgeError(
        ErrorCode.STATE_CONFLICT,
        "atomic replace timed out while the target was locked",
        {
            "target": str(target),
            "retry_seconds": retry_window,
            "winerror": error.winerror,
        },
    )


def _record_cleanup_failure(
    primary: BaseException,
    cleanup: BaseException,
    *,
    description: str,
    resource: str,
) -> None:
    note = f"{description} cleanup failed for {resource}: {type(cleanup).__name__}: {cleanup}"
    primary.add_note(note)
    if isinstance(primary, BridgeError):
        record = {
            "resource": resource,
            "description": description,
            "type": type(cleanup).__name__,
            "message": str(cleanup),
        }
        primary.details.setdefault("cleanup_error", record)
        cleanup_errors_value = primary.details.get("cleanup_errors")
        if isinstance(cleanup_errors_value, list):
            cleanup_errors = cast(list[dict[str, str]], cleanup_errors_value)
        else:
            cleanup_errors = []
            primary.details["cleanup_errors"] = cleanup_errors
        cleanup_errors.append(record)


def _attempt_cleanup(
    primary: BaseException | None,
    action: Callable[[], None],
    *,
    description: str,
    resource: str,
    ignore_missing: bool = False,
) -> BaseException | None:
    try:
        action()
    except FileNotFoundError as cleanup:
        if ignore_missing:
            return primary
        if primary is None:
            return cleanup
        _record_cleanup_failure(
            primary,
            cleanup,
            description=description,
            resource=resource,
        )
    except BaseException as cleanup:
        if primary is None:
            return cleanup
        _record_cleanup_failure(
            primary,
            cleanup,
            description=description,
            resource=resource,
        )
    return primary


def atomic_replace_bytes(target: Path, data: bytes, *, retry_seconds: float) -> None:
    retry_window = _validate_retry_seconds(retry_seconds)
    raw_data = cast(object, data)
    if not isinstance(raw_data, bytes):
        raise BridgeError(ErrorCode.INVALID_ARGUMENT, "atomic replacement data must be bytes")

    target_path = Path(target)
    file_descriptor, temp_name = tempfile.mkstemp(
        dir=target_path.parent,
        prefix=f".{target_path.name}.cmo-agent-bridge-",
        suffix=".tmp",
    )
    temp_path = Path(temp_name)
    descriptor_open = True
    stream: IO[bytes] | None = None
    primary_error: BaseException | None = None
    try:
        stream = cast(IO[bytes], os.fdopen(file_descriptor, "wb"))
        descriptor_open = False
        stream.write(raw_data)
        stream.flush()
        os.fsync(stream.fileno())
        stream.close()
        stream = None

        deadline = time.monotonic() + retry_window
        delay = _INITIAL_RETRY_DELAY_SECONDS
        retry_error: OSError | None = None
        replaced = False
        while not replaced:
            if retry_error is not None and time.monotonic() >= deadline:
                raise _sharing_timeout(target_path, retry_window, retry_error) from retry_error
            try:
                os.replace(temp_path, target_path)
                replaced = True
            except OSError as error:
                if getattr(error, "winerror", None) not in _SHARING_VIOLATIONS:
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise _sharing_timeout(target_path, retry_window, error) from error
                time.sleep(min(delay, remaining))
                retry_error = error
                delay = min(delay * 2, _MAX_RETRY_DELAY_SECONDS)
    except BaseException as error:
        primary_error = error
    finally:
        if stream is not None:
            primary_error = _attempt_cleanup(
                primary_error,
                stream.close,
                description="temporary file stream",
                resource=str(temp_path),
            )
        elif descriptor_open:

            def close_descriptor() -> None:
                os.close(file_descriptor)

            primary_error = _attempt_cleanup(
                primary_error,
                close_descriptor,
                description="file descriptor",
                resource=str(file_descriptor),
            )
        primary_error = _attempt_cleanup(
            primary_error,
            temp_path.unlink,
            description="temporary file",
            resource=str(temp_path),
            ignore_missing=True,
        )
    if primary_error is not None:
        raise primary_error
