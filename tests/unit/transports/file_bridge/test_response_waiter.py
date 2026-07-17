from __future__ import annotations

import asyncio
import hashlib
import inspect
import math
import os
from collections.abc import Callable
from pathlib import Path
from typing import Never, Protocol, cast
from uuid import UUID

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.models import AllowedDelivery, ResponseExpectation
from cmo_agent_bridge.protocol.response_models import AcceptedResponse
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.transports.file_bridge import response_waiter as response_waiter_module
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo
from cmo_agent_bridge.transports.file_bridge.response_waiter import ResponseWaiter


REQUEST_ID = UUID("11111111-1111-4111-8111-111111111111")
REQUEST_DELIVERY_ID = UUID("22222222-2222-4222-8222-222222222222")
LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")
REQUEST_HASH = "a" * 64
EXPECTED_FILENAME = f"CMOAgentBridge_Response_{REQUEST_ID}.inst"
RUNTIME_SNAPSHOT = RuntimeSnapshot.create(
    runtime_version="0.1.0",
    runtime_asset_sha256="b" * 64,
    operation_manifest_sha256=MANIFEST_SHA256,
    host_contract_sha256="c" * 64,
    dependency_lock_sha256="d" * 64,
)


class _HashLike(Protocol):
    def hexdigest(self) -> str: ...


class _CustomBaseException(BaseException):
    pass


class _SensitiveProcessError(RuntimeError):
    def __repr__(self) -> str:
        return "SENSITIVE_OBJECT_REPR_MUST_NOT_LEAK"


class _FakeClock:
    def __init__(
        self,
        now: float = 0.0,
        *,
        epoch_ns: int | None = None,
        sleep_failure: BaseException | None = None,
    ) -> None:
        self.now = now
        self.epoch_ns = epoch_ns
        self.sleep_failure = sleep_failure
        self.monotonic_observations: list[float] = []
        self.sleeps: list[float] = []
        self.time_calls = 0
        self.time_ns_calls = 0

    def monotonic(self) -> float:
        self.monotonic_observations.append(self.now)
        return self.now

    def time(self) -> Never:
        self.time_calls += 1
        raise AssertionError("wall clock must not drive response timeouts")

    def time_ns(self) -> int:
        self.time_ns_calls += 1
        if self.epoch_ns is None:
            raise AssertionError("epoch clock must be read only after a successful parse")
        return self.epoch_ns

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        if self.sleep_failure is not None:
            raise self.sleep_failure
        self.now += delay


class _FakeAsyncio:
    def __init__(self, clock: _FakeClock) -> None:
        self._clock = clock

    async def sleep(self, delay: float) -> None:
        await self._clock.sleep(delay)


@pytest.fixture
def valid_inst_bytes() -> bytes:
    return (Path(__file__).parents[3] / "fixtures" / "valid_response.inst").read_bytes()


@pytest.fixture
def expectation() -> ResponseExpectation:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "unit.assign_mission",
        {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1"},
    )
    return ResponseExpectation(
        request_id=REQUEST_ID,
        allowed_deliveries=(AllowedDelivery(REQUEST_DELIVERY_ID, "request"),),
        request_hash=REQUEST_HASH,
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=ACTIVATION_ID,
        status_bootstrap=False,
        activation_candidate=None,
        runtime_snapshot=RUNTIME_SNAPSHOT,
        invocation=invocation,
    )


@pytest.fixture
def expected_process() -> ProcessInfo:
    return ProcessInfo(
        pid=1234,
        create_time=1000.25,
        executable=Path("C:/CMO/Command.exe"),
    )


@pytest.fixture
def response_path(tmp_path: Path) -> Path:
    return tmp_path / EXPECTED_FILENAME


def _waiter(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    process_check: Callable[[], ProcessInfo] | None = None,
    *,
    poll_seconds: float = 0.05,
    ownership_check: Callable[[], bool] | None = None,
) -> ResponseWaiter:
    return ResponseWaiter(
        response_path=response_path,
        expectation=expectation,
        expected_process=expected_process,
        process_check=(lambda: expected_process) if process_check is None else process_check,
        poll_seconds=poll_seconds,
        ownership_check=ownership_check,
    )


def _assert_invalid_argument(call: Callable[[], object]) -> BridgeError:
    with pytest.raises(BridgeError) as caught:
        call()
    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    return caught.value


def test_public_signatures_require_explicit_expected_process() -> None:
    constructor = inspect.signature(ResponseWaiter.__init__)
    assert list(constructor.parameters) == [
        "self",
        "response_path",
        "expectation",
        "expected_process",
        "process_check",
        "poll_seconds",
        "ownership_check",
    ]
    assert constructor.parameters["poll_seconds"].default == 0.05
    assert constructor.parameters["ownership_check"].default is None
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for parameter in constructor.parameters.values()
    )

    wait = inspect.signature(ResponseWaiter.wait)
    assert list(wait.parameters) == ["self", "timeout_seconds"]
    assert wait.parameters["timeout_seconds"].default is inspect.Parameter.empty
    assert wait.parameters["timeout_seconds"].annotation == "float | None"
    assert inspect.iscoroutinefunction(ResponseWaiter.wait)


def test_constructor_rejects_non_path_and_wrong_exact_filename_before_io(
    tmp_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
) -> None:
    callback_calls = 0

    def process_check() -> ProcessInfo:
        nonlocal callback_calls
        callback_calls += 1
        return expected_process

    _assert_invalid_argument(
        lambda: ResponseWaiter(
            response_path=cast(Path, str(tmp_path / EXPECTED_FILENAME)),
            expectation=expectation,
            expected_process=expected_process,
            process_check=process_check,
        )
    )
    _assert_invalid_argument(
        lambda: ResponseWaiter(
            response_path=tmp_path / EXPECTED_FILENAME.upper(),
            expectation=expectation,
            expected_process=expected_process,
            process_check=process_check,
        )
    )
    _assert_invalid_argument(
        lambda: ResponseWaiter(
            response_path=tmp_path / f"CMOAgentBridge_Response_{REQUEST_DELIVERY_ID}.inst",
            expectation=expectation,
            expected_process=expected_process,
            process_check=process_check,
        )
    )

    assert callback_calls == 0


def test_constructor_validates_exact_expectation_type_before_request_id_access(
    response_path: Path,
    expected_process: ProcessInfo,
) -> None:
    class ExpectationLike:
        @property
        def request_id(self) -> Never:
            raise AssertionError("wrong-type expectation must not be inspected")

    _assert_invalid_argument(
        lambda: ResponseWaiter(
            response_path=response_path,
            expectation=cast(ResponseExpectation, ExpectationLike()),
            expected_process=expected_process,
            process_check=lambda: expected_process,
        )
    )


def test_constructor_rejects_wrong_process_and_noncallable_check(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
) -> None:
    _assert_invalid_argument(
        lambda: ResponseWaiter(
            response_path=response_path,
            expectation=expectation,
            expected_process=cast(ProcessInfo, object()),
            process_check=lambda: expected_process,
        )
    )
    _assert_invalid_argument(
        lambda: ResponseWaiter(
            response_path=response_path,
            expectation=expectation,
            expected_process=expected_process,
            process_check=cast(Callable[[], ProcessInfo], None),
        )
    )


@pytest.mark.parametrize(
    "invalid_process",
    [
        ProcessInfo(pid=cast(int, True), create_time=1.0, executable=Path("Command.exe")),
        ProcessInfo(pid=0, create_time=1.0, executable=Path("Command.exe")),
        ProcessInfo(pid=-1, create_time=1.0, executable=Path("Command.exe")),
        ProcessInfo(pid=cast(int, 1.0), create_time=1.0, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=cast(float, 1), executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=0.0, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=-1.0, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=math.inf, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=-math.inf, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=math.nan, executable=Path("Command.exe")),
        ProcessInfo(pid=1, create_time=1.0, executable=cast(Path, "Command.exe")),
    ],
)
def test_constructor_rejects_structurally_invalid_expected_process(
    response_path: Path,
    expectation: ResponseExpectation,
    invalid_process: ProcessInfo,
) -> None:
    _assert_invalid_argument(
        lambda: ResponseWaiter(
            response_path=response_path,
            expectation=expectation,
            expected_process=invalid_process,
            process_check=lambda: invalid_process,
        )
    )


@pytest.mark.parametrize(
    "invalid_poll",
    [True, False, "0.05", object(), 0, 0.0, -0.1, math.inf, -math.inf, math.nan, 10**400],
)
def test_constructor_rejects_invalid_poll_interval(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    invalid_poll: object,
) -> None:
    _assert_invalid_argument(
        lambda: ResponseWaiter(
            response_path=response_path,
            expectation=expectation,
            expected_process=expected_process,
            process_check=lambda: expected_process,
            poll_seconds=cast(float, invalid_poll),
        )
    )


@pytest.mark.parametrize(
    "invalid_timeout",
    [True, False, "0", object(), -0.1, math.inf, -math.inf, math.nan, 10**400],
)
@pytest.mark.asyncio
async def test_wait_rejects_invalid_timeout_before_process_or_filesystem_io(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    invalid_timeout: object,
) -> None:
    def forbidden_process_check() -> ProcessInfo:
        raise AssertionError("invalid timeout must fail before process I/O")

    waiter = _waiter(response_path, expectation, expected_process, forbidden_process_check)

    with pytest.raises(BridgeError) as caught:
        await waiter.wait(cast(float, invalid_timeout))

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert not response_path.exists()


@pytest.mark.asyncio
async def test_zero_timeout_accepts_preexisting_response_and_builds_exact_artifact(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response_path.write_bytes(valid_inst_bytes)
    checks = 0

    def process_check() -> ProcessInfo:
        nonlocal checks
        checks += 1
        return expected_process

    monkeypatch.setattr(
        response_waiter_module.time,
        "time_ns",
        lambda: 1_234_567_890_123_456_789,
    )
    artifact = await _waiter(response_path, expectation, expected_process, process_check).wait(
        timeout_seconds=0
    )

    assert checks == 1
    assert artifact.filename == EXPECTED_FILENAME
    assert artifact.sha256 == hashlib.sha256(valid_inst_bytes).hexdigest()
    assert artifact.size_bytes == len(valid_inst_bytes)
    assert artifact.accepted_at_ms == 1_234_567_890_123
    assert artifact.accepted_response.envelope.request_id == expectation.request_id
    assert artifact.accepted_response.ok is True


@pytest.mark.asyncio
async def test_zero_timeout_missing_response_times_out_after_one_immediate_poll(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
) -> None:
    checks = 0

    def process_check() -> ProcessInfo:
        nonlocal checks
        checks += 1
        return expected_process

    with pytest.raises(BridgeError) as caught:
        await _waiter(response_path, expectation, expected_process, process_check).wait(0)

    assert caught.value.code is ErrorCode.REQUEST_TIMEOUT
    assert caught.value.message == "timed out waiting for a correlated CMO response"
    assert caught.value.details == {
        "request_id": str(expectation.request_id),
        "response_path": str(response_path),
        "timeout_seconds": 0.0,
    }
    assert checks == 1


@pytest.mark.asyncio
async def test_unbounded_wait_polls_until_response_and_rechecks_process(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checks = 0

    def process_check() -> ProcessInfo:
        nonlocal checks
        checks += 1
        if checks == 2:
            response_path.write_bytes(valid_inst_bytes)
        return expected_process

    monkeypatch.setattr(response_waiter_module.time, "time_ns", lambda: 1_000_000)
    artifact = await _waiter(
        response_path,
        expectation,
        expected_process,
        process_check,
        poll_seconds=0.001,
    ).wait(None)

    assert checks == 2
    assert artifact.accepted_response.envelope.request_id == REQUEST_ID


@pytest.mark.asyncio
async def test_zero_timeout_json_decode_failure_returns_exact_protocol_error(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reads = _install_read_observations(monkeypatch, response_path, [b""])
    parser_errors = _record_parser_errors(monkeypatch)

    with pytest.raises(BridgeError) as caught:
        await _waiter(response_path, expectation, expected_process).wait(0)

    assert caught.value.code is ErrorCode.PROTOCOL_ERROR
    assert caught.value.message == "invalid outer .inst JSON"
    assert caught.value is parser_errors[0]
    assert len(parser_errors) == 1
    assert len(reads) == 1


@pytest.mark.asyncio
async def test_success_hashes_sizes_and_parses_one_identical_buffer_without_stat_or_reopen(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = valid_inst_bytes
    response_path.write_bytes(b"initial on-disk bytes are not the observed buffer")
    replacement_path = response_path.with_name("replacement.inst")
    replacement_bytes = b"replacement after parsing"
    replacement_path.write_bytes(replacement_bytes)
    reads: list[Path] = []
    hash_inputs: list[bytes] = []
    parser_inputs: list[bytes] = []
    real_sha256 = hashlib.sha256
    real_parser = response_waiter_module.parse_inst_response

    def read_bytes(path: Path) -> bytes:
        reads.append(path)
        return sentinel

    def sha256_spy(raw: bytes = b"") -> _HashLike:
        assert raw is sentinel
        hash_inputs.append(raw)
        return real_sha256(raw)

    class HashlibSpy:
        sha256 = staticmethod(sha256_spy)

    def parse(
        raw: bytes,
        observed_expectation: ResponseExpectation,
    ) -> AcceptedResponse:
        assert raw is sentinel
        parser_inputs.append(raw)
        accepted = real_parser(raw, observed_expectation)
        replacement_path.replace(response_path)
        return accepted

    real_stat = Path.stat

    def guarded_stat(path: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        if path == response_path:
            raise AssertionError("response waiter must not stat response files")
        return real_stat(path, follow_symlinks=follow_symlinks)

    waiter = _waiter(response_path, expectation, expected_process)
    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    monkeypatch.setattr(Path, "stat", guarded_stat)
    monkeypatch.setattr(response_waiter_module, "hashlib", HashlibSpy)
    monkeypatch.setattr(response_waiter_module, "parse_inst_response", parse)
    monkeypatch.setattr(
        response_waiter_module.time,
        "time_ns",
        lambda: 9_876_543_210_999_999_999,
    )

    artifact = await waiter.wait(0)

    assert reads == [response_path]
    assert hash_inputs == [sentinel]
    assert parser_inputs == [sentinel]
    assert hash_inputs[0] is sentinel
    assert parser_inputs[0] is sentinel
    assert artifact.sha256 == real_sha256(sentinel).hexdigest()
    assert artifact.size_bytes == len(sentinel)
    assert artifact.accepted_at_ms == 9_876_543_210_999
    assert artifact.filename == EXPECTED_FILENAME


def _install_read_observations(
    monkeypatch: pytest.MonkeyPatch,
    response_path: Path,
    observations: list[bytes | BaseException],
) -> list[Path]:
    remaining = list(observations)
    reads: list[Path] = []

    def read_bytes(path: Path) -> bytes:
        assert path == response_path
        reads.append(path)
        if not remaining:
            raise AssertionError("response waiter performed an unexpected extra read")
        observation = remaining.pop(0)
        if isinstance(observation, BaseException):
            raise observation
        return observation

    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    return reads


def _record_parser_errors(monkeypatch: pytest.MonkeyPatch) -> list[BridgeError]:
    real_parser = response_waiter_module.parse_inst_response
    errors: list[BridgeError] = []

    def parse(
        raw: bytes,
        expectation: ResponseExpectation,
    ) -> AcceptedResponse:
        try:
            return real_parser(raw, expectation)
        except BridgeError as error:
            errors.append(error)
            raise

    monkeypatch.setattr(response_waiter_module, "parse_inst_response", parse)
    return errors


@pytest.mark.asyncio
async def test_absent_file_then_valid_bytes_is_accepted(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [FileNotFoundError(response_path), valid_inst_bytes],
    )

    artifact = await _waiter(
        response_path,
        expectation,
        expected_process,
        poll_seconds=0.001,
    ).wait(1)

    assert artifact.sha256 == hashlib.sha256(valid_inst_bytes).hexdigest()
    assert reads == [response_path, response_path]


@pytest.mark.parametrize("permission_failures", [1, 2])
@pytest.mark.asyncio
async def test_permission_errors_are_transient_and_do_not_invoke_parser(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
    permission_failures: int,
) -> None:
    observations: list[bytes | BaseException] = [
        *(PermissionError("locked") for _index in range(permission_failures)),
        valid_inst_bytes,
    ]
    reads = _install_read_observations(monkeypatch, response_path, observations)
    real_parser = response_waiter_module.parse_inst_response
    parser_inputs: list[bytes] = []

    def parse(raw: bytes, observed_expectation: ResponseExpectation) -> AcceptedResponse:
        parser_inputs.append(raw)
        return real_parser(raw, observed_expectation)

    monkeypatch.setattr(response_waiter_module, "parse_inst_response", parse)

    artifact = await _waiter(
        response_path,
        expectation,
        expected_process,
        poll_seconds=0.001,
    ).wait(1)

    assert artifact.accepted_response.ok is True
    assert len(reads) == permission_failures + 1
    assert parser_inputs == [valid_inst_bytes]


@pytest.mark.asyncio
async def test_partial_outer_json_can_survive_more_than_three_polls_then_succeed(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock(epoch_ns=123_000_000)
    _install_clock(monkeypatch, clock)
    partial = valid_inst_bytes[:20]
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [partial, partial, partial, partial, partial, partial, valid_inst_bytes],
    )

    artifact = await _waiter(
        response_path,
        expectation,
        expected_process,
        poll_seconds=0.1,
    ).wait(1)

    assert artifact.accepted_response.ok is True
    assert artifact.accepted_at_ms == 123
    assert len(reads) == 7
    assert clock.sleeps == pytest.approx([0.1] * 6)


@pytest.mark.parametrize(
    ("needle", "replacement", "message"),
    [
        (
            str(REQUEST_ID).encode(),
            b"00000000-0000-4000-8000-000000000001",
            "response request ID does not match expectation",
        ),
        (
            REQUEST_HASH.encode(),
            b"e" * 64,
            "response request hash does not match expectation",
        ),
    ],
)
@pytest.mark.asyncio
async def test_correlation_mismatch_fails_on_first_read_without_waiting_for_rewrite(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
    needle: bytes,
    replacement: bytes,
    message: str,
) -> None:
    mismatched = valid_inst_bytes.replace(needle, replacement)
    assert mismatched != valid_inst_bytes
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [mismatched, valid_inst_bytes],
    )
    parser_errors = _record_parser_errors(monkeypatch)

    with pytest.raises(BridgeError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            poll_seconds=0.001,
        ).wait(1)

    assert caught.value.code is ErrorCode.PROTOCOL_ERROR
    assert caught.value.message == message
    assert caught.value is parser_errors[0]
    assert len(parser_errors) == 1
    assert len(reads) == 1


@pytest.mark.asyncio
async def test_changed_json_decode_failures_then_valid_response_succeeds(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    malformed_a = b'{"Comments":'
    malformed_b = b'{"Comments":"not json"}'
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [malformed_a, malformed_a, malformed_b, malformed_b, valid_inst_bytes],
    )

    artifact = await _waiter(
        response_path,
        expectation,
        expected_process,
        poll_seconds=0.001,
    ).wait(1)

    assert artifact.accepted_response.ok is True
    assert len(reads) == 5


@pytest.mark.parametrize(
    ("malformed", "message"),
    [
        (b'{"Comments":', "invalid outer .inst JSON"),
        (b'{"Comments":"not json"}', "invalid Comments envelope JSON"),
    ],
)
@pytest.mark.asyncio
async def test_permanent_json_decode_failure_raises_last_parser_error_at_deadline(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
    malformed: bytes,
    message: str,
) -> None:
    clock = _FakeClock()
    _install_clock(monkeypatch, clock)
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [malformed, malformed, malformed],
    )
    parser_errors = _record_parser_errors(monkeypatch)

    with pytest.raises(BridgeError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            poll_seconds=1,
        ).wait(3)

    assert caught.value.code is ErrorCode.PROTOCOL_ERROR
    assert caught.value.message == message
    assert len(parser_errors) == 3
    assert caught.value is parser_errors[-1]
    assert len(reads) == 3
    assert clock.sleeps == [1, 1, 1]


@pytest.mark.asyncio
async def test_unbounded_wait_fails_when_invalid_response_bytes_remain_stable(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    _install_clock(monkeypatch, clock)
    malformed = b'{"Comments":"not json"}'
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [malformed, malformed, malformed],
    )
    parser_errors = _record_parser_errors(monkeypatch)
    ownership_checks = 0

    def owns_inbox() -> bool:
        nonlocal ownership_checks
        ownership_checks += 1
        return True

    with pytest.raises(BridgeError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            poll_seconds=0.5,
            ownership_check=owns_inbox,
        ).wait(None)

    assert caught.value.code is ErrorCode.INDETERMINATE_OUTCOME
    assert caught.value.message == "CMO response remained invalid during durable wait"
    assert caught.value.details == {
        "request_id": str(REQUEST_ID),
        "response_path": str(response_path),
        "reason": "stable_invalid_response",
        "response_sha256": hashlib.sha256(malformed).hexdigest(),
        "response_size_bytes": len(malformed),
        "stable_seconds": 1.0,
        "protocol_error": "invalid Comments envelope JSON",
    }
    assert caught.value.__cause__ is parser_errors[-1]
    assert len(parser_errors) == 3
    assert len(reads) == 3
    assert ownership_checks == 2
    assert clock.sleeps == [0.5, 0.5]


@pytest.mark.asyncio
async def test_unbounded_wait_fails_when_published_inbox_ownership_is_lost(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    _install_clock(monkeypatch, clock)
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [FileNotFoundError(response_path)],
    )
    process_checks = 0
    ownership_checks = 0

    def process_check() -> ProcessInfo:
        nonlocal process_checks
        process_checks += 1
        return expected_process

    def owns_inbox() -> bool:
        nonlocal ownership_checks
        ownership_checks += 1
        return False

    with pytest.raises(BridgeError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            process_check,
            ownership_check=owns_inbox,
        ).wait(None)

    assert caught.value.code is ErrorCode.INDETERMINATE_OUTCOME
    assert caught.value.message == "published mutation no longer owns the CMO inbox"
    assert caught.value.details == {
        "request_id": str(REQUEST_ID),
        "reason": "published_inbox_evidence_missing",
    }
    assert len(reads) == 1
    assert process_checks == 1
    assert ownership_checks == 1
    assert clock.sleeps == []


@pytest.mark.asyncio
async def test_unbounded_wait_accepts_valid_response_before_rechecking_inbox_ownership(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock(epoch_ns=123_000_000)
    _install_clock(monkeypatch, clock)
    reads = _install_read_observations(monkeypatch, response_path, [valid_inst_bytes])
    ownership_checks = 0

    def lost_ownership() -> bool:
        nonlocal ownership_checks
        ownership_checks += 1
        return False

    artifact = await _waiter(
        response_path,
        expectation,
        expected_process,
        ownership_check=lost_ownership,
    ).wait(None)

    assert artifact.accepted_response.ok is True
    assert len(reads) == 1
    assert ownership_checks == 0
    assert clock.sleeps == []


@pytest.mark.parametrize(
    ("invalid", "message"),
    [
        (b"[]", "outer .inst JSON must be an object"),
        (b"{}", "outer .inst JSON requires string Comments"),
        (b'{"Comments":1}', "outer .inst JSON requires string Comments"),
        (b'{"Comments":"[]"}', "Comments envelope JSON must be an object"),
    ],
)
@pytest.mark.asyncio
async def test_structural_protocol_error_fails_on_first_read(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
    invalid: bytes,
    message: str,
) -> None:
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [invalid, valid_inst_bytes],
    )
    parser_errors = _record_parser_errors(monkeypatch)

    with pytest.raises(BridgeError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            poll_seconds=0.001,
        ).wait(1)

    assert caught.value.code is ErrorCode.PROTOCOL_ERROR
    assert caught.value.message == message
    assert len(parser_errors) == 1
    assert caught.value is parser_errors[0]
    assert len(reads) == 1


@pytest.mark.asyncio
async def test_missing_and_permission_between_json_decode_failures_still_allow_completion(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    malformed = b'{"Comments":"not json"}'
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [
            malformed,
            FileNotFoundError(response_path),
            malformed,
            PermissionError("locked"),
            valid_inst_bytes,
        ],
    )
    parser_errors = _record_parser_errors(monkeypatch)

    artifact = await _waiter(
        response_path,
        expectation,
        expected_process,
        poll_seconds=0.001,
    ).wait(1)

    assert artifact.accepted_response.ok is True
    assert len(reads) == 5
    assert len(parser_errors) == 2


@pytest.mark.asyncio
async def test_missing_file_after_json_decode_failure_does_not_replace_last_protocol_error(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    _install_clock(monkeypatch, clock)
    malformed = b'{"Comments":'
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [malformed, FileNotFoundError(response_path), PermissionError("locked")],
    )
    parser_errors = _record_parser_errors(monkeypatch)

    with pytest.raises(BridgeError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            poll_seconds=1,
        ).wait(3)

    assert caught.value.code is ErrorCode.PROTOCOL_ERROR
    assert caught.value.message == "invalid outer .inst JSON"
    assert caught.value is parser_errors[0]
    assert len(parser_errors) == 1
    assert len(reads) == 3


@pytest.mark.asyncio
async def test_non_protocol_parser_bridge_error_propagates_immediately_by_identity(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reads = _install_read_observations(monkeypatch, response_path, [b"parser input"])
    failure = BridgeError(ErrorCode.STATE_CONFLICT, "unexpected parser contract failure")

    def fail_parser(_raw: bytes, _expectation: ResponseExpectation) -> Never:
        raise failure

    monkeypatch.setattr(response_waiter_module, "parse_inst_response", fail_parser)

    with pytest.raises(BridgeError) as caught:
        await _waiter(response_path, expectation, expected_process).wait(1)

    assert caught.value is failure
    assert len(reads) == 1


@pytest.mark.asyncio
async def test_unexpected_response_read_oserror_maps_to_state_conflict_with_cause(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = OSError("device failure")
    failure.winerror = 123
    reads = _install_read_observations(monkeypatch, response_path, [failure])

    with pytest.raises(BridgeError) as caught:
        await _waiter(response_path, expectation, expected_process).wait(1)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.message == "CMO response file could not be read"
    assert caught.value.details == {
        "response_path": str(response_path),
        "type": "OSError",
        "winerror": 123,
    }
    assert caught.value.__cause__ is failure
    assert len(reads) == 1


def _install_clock(monkeypatch: pytest.MonkeyPatch, clock: _FakeClock) -> None:
    monkeypatch.setattr(response_waiter_module, "time", clock)
    monkeypatch.setattr(response_waiter_module, "asyncio", _FakeAsyncio(clock))


def _process_details(process: ProcessInfo) -> dict[str, object]:
    return {
        "pid": process.pid,
        "create_time": process.create_time,
        "executable": str(process.executable),
    }


@pytest.mark.asyncio
async def test_first_callback_replacement_cannot_self_pin_even_when_valid_file_exists(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replacement = ProcessInfo(
        pid=expected_process.pid + 1,
        create_time=expected_process.create_time,
        executable=expected_process.executable,
    )
    reads = _install_read_observations(monkeypatch, response_path, [valid_inst_bytes])

    with pytest.raises(BridgeError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            process_check=lambda: replacement,
        ).wait(0)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.message == "CMO process identity changed while waiting for response"
    assert caught.value.details == {
        "expected_process": _process_details(expected_process),
        "actual_process": _process_details(replacement),
    }
    assert reads == []


@pytest.mark.parametrize("changed_field", ["pid", "create_time", "executable"])
@pytest.mark.asyncio
async def test_each_exact_process_identity_field_is_binding(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
    changed_field: str,
) -> None:
    actual = {
        "pid": ProcessInfo(
            expected_process.pid + 1,
            expected_process.create_time,
            expected_process.executable,
        ),
        "create_time": ProcessInfo(
            expected_process.pid,
            expected_process.create_time + 0.000_000_1,
            expected_process.executable,
        ),
        "executable": ProcessInfo(
            expected_process.pid,
            expected_process.create_time,
            Path("C:/Other/Command.exe"),
        ),
    }[changed_field]
    reads = _install_read_observations(monkeypatch, response_path, [b"must not be read"])

    with pytest.raises(BridgeError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            process_check=lambda: actual,
        ).wait(0)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.details == {
        "expected_process": _process_details(expected_process),
        "actual_process": _process_details(actual),
    }
    assert reads == []


@pytest.mark.asyncio
async def test_exact_expected_process_is_checked_before_every_file_read(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock(epoch_ns=1_000_000)
    _install_clock(monkeypatch, clock)
    remaining: list[bytes | BaseException] = [
        FileNotFoundError(response_path),
        PermissionError("locked"),
        valid_inst_bytes,
    ]
    events: list[str] = []

    def process_check() -> ProcessInfo:
        events.append("process")
        return expected_process

    def read_bytes(path: Path) -> bytes:
        assert path == response_path
        events.append("read")
        observation = remaining.pop(0)
        if isinstance(observation, BaseException):
            raise observation
        return observation

    monkeypatch.setattr(Path, "read_bytes", read_bytes)

    artifact = await _waiter(
        response_path,
        expectation,
        expected_process,
        process_check,
        poll_seconds=1,
    ).wait(3)

    assert artifact.accepted_response.ok is True
    assert events == ["process", "read", "process", "read", "process", "read"]


@pytest.mark.parametrize(
    "code",
    [ErrorCode.CMO_NOT_RUNNING, ErrorCode.MULTIPLE_CMO_INSTANCES],
)
@pytest.mark.asyncio
async def test_process_callback_bridge_errors_propagate_unchanged(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
    code: ErrorCode,
) -> None:
    failure = BridgeError(code, "process guard failure", {"safe": True})
    reads = _install_read_observations(monkeypatch, response_path, [b"must not be read"])

    def fail() -> Never:
        raise failure

    with pytest.raises(BridgeError) as caught:
        await _waiter(response_path, expectation, expected_process, fail).wait(0)

    assert caught.value is failure
    assert reads == []


@pytest.mark.asyncio
async def test_raw_process_exception_maps_to_safe_state_conflict_with_exact_cause(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = _SensitiveProcessError("internal process detail")
    reads = _install_read_observations(monkeypatch, response_path, [b"must not be read"])

    def fail() -> Never:
        raise failure

    with pytest.raises(BridgeError) as caught:
        await _waiter(response_path, expectation, expected_process, fail).wait(0)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.message == "CMO process check failed"
    assert caught.value.details == {"type": "_SensitiveProcessError"}
    assert caught.value.__cause__ is failure
    serialized_details = str(caught.value.details)
    assert "SENSITIVE_OBJECT_REPR_MUST_NOT_LEAK" not in serialized_details
    assert "Traceback" not in serialized_details
    assert reads == []


@pytest.mark.parametrize(
    "invalid_result",
    [
        object(),
        ProcessInfo(0, 1.0, Path("Command.exe")),
        ProcessInfo(1, cast(float, 1), Path("Command.exe")),
        ProcessInfo(1, math.nan, Path("Command.exe")),
        ProcessInfo(1, 1.0, cast(Path, "Command.exe")),
    ],
)
@pytest.mark.asyncio
async def test_invalid_process_callback_return_maps_to_state_conflict(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
    invalid_result: object,
) -> None:
    reads = _install_read_observations(monkeypatch, response_path, [b"must not be read"])

    def invalid_check() -> ProcessInfo:
        return cast(ProcessInfo, invalid_result)

    with pytest.raises(BridgeError) as caught:
        await _waiter(response_path, expectation, expected_process, invalid_check).wait(0)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.message == "CMO process check returned invalid process identity"
    assert caught.value.details == {"type": type(invalid_result).__name__}
    assert caught.value.__cause__ is None
    assert reads == []


@pytest.mark.parametrize(
    "failure",
    [asyncio.CancelledError("cancel process check"), _CustomBaseException("stop")],
)
@pytest.mark.asyncio
async def test_process_callback_base_exceptions_propagate_by_identity_without_file_read(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
) -> None:
    reads = _install_read_observations(monkeypatch, response_path, [b"must not be read"])

    def fail() -> Never:
        raise failure

    with pytest.raises(type(failure)) as caught:
        await _waiter(response_path, expectation, expected_process, fail).wait(0)

    assert caught.value is failure
    assert reads == []


@pytest.mark.parametrize("raw", [b'{"Comments":', b"{valid response placeholder}"])
@pytest.mark.asyncio
async def test_process_failure_wins_before_valid_or_malformed_file_evidence(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
    raw: bytes,
) -> None:
    observed_raw = valid_inst_bytes if raw == b"{valid response placeholder}" else raw
    failure = BridgeError(ErrorCode.CMO_NOT_RUNNING, "CMO exited")
    reads = _install_read_observations(monkeypatch, response_path, [observed_raw])

    def fail() -> Never:
        raise failure

    with pytest.raises(BridgeError) as caught:
        await _waiter(response_path, expectation, expected_process, fail).wait(0)

    assert caught.value is failure
    assert reads == []


@pytest.mark.asyncio
async def test_timeout_uses_one_monotonic_deadline_and_caps_every_sleep_to_remaining(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock(now=10.0)
    _install_clock(monkeypatch, clock)
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [FileNotFoundError(response_path), FileNotFoundError(response_path)],
    )
    process_checks = 0

    def process_check() -> ProcessInfo:
        nonlocal process_checks
        process_checks += 1
        return expected_process

    with pytest.raises(BridgeError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            process_check,
            poll_seconds=0.5,
        ).wait(0.75)

    assert caught.value.code is ErrorCode.REQUEST_TIMEOUT
    assert clock.sleeps == [0.5, 0.25]
    assert clock.monotonic_observations == [10.0, 10.0, 10.5, 10.5, 10.75]
    assert clock.time_calls == 0
    assert clock.time_ns_calls == 0
    assert process_checks == 2
    assert len(reads) == 2


@pytest.mark.asyncio
async def test_reached_deadline_before_second_poll_prevents_new_process_and_file_observation(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    _install_clock(monkeypatch, clock)
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [FileNotFoundError(response_path)],
    )
    checks = 0

    def process_check() -> ProcessInfo:
        nonlocal checks
        checks += 1
        return expected_process

    with pytest.raises(BridgeError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            process_check,
            poll_seconds=0.5,
        ).wait(0.5)

    assert caught.value.code is ErrorCode.REQUEST_TIMEOUT
    assert clock.sleeps == [0.5]
    assert checks == 1
    assert len(reads) == 1


@pytest.mark.asyncio
async def test_json_decode_failure_in_active_poll_wins_if_deadline_crosses_during_parse(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    _install_clock(monkeypatch, clock)
    malformed = b'{"Comments":"not json"}'
    reads = _install_read_observations(monkeypatch, response_path, [malformed])
    real_parser = response_waiter_module.parse_inst_response
    parser_errors: list[BridgeError] = []

    def parse(raw: bytes, observed_expectation: ResponseExpectation) -> AcceptedResponse:
        clock.now = 4.0
        try:
            return real_parser(raw, observed_expectation)
        except BridgeError as error:
            parser_errors.append(error)
            raise

    monkeypatch.setattr(response_waiter_module, "parse_inst_response", parse)

    with pytest.raises(BridgeError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            poll_seconds=1,
        ).wait(3)

    assert caught.value.code is ErrorCode.PROTOCOL_ERROR
    assert caught.value.message == "invalid Comments envelope JSON"
    assert caught.value is parser_errors[0]
    assert len(reads) == 1


@pytest.mark.asyncio
async def test_valid_active_poll_succeeds_if_deadline_crosses_during_read_and_parse(
    response_path: Path,
    valid_inst_bytes: bytes,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock(epoch_ns=123_000_000)
    _install_clock(monkeypatch, clock)
    reads = 0

    def read_bytes(path: Path) -> bytes:
        nonlocal reads
        assert path == response_path
        reads += 1
        clock.now = 2.0
        return valid_inst_bytes

    monkeypatch.setattr(Path, "read_bytes", read_bytes)

    artifact = await _waiter(response_path, expectation, expected_process).wait(1)

    assert artifact.accepted_at_ms == 123
    assert reads == 1
    assert clock.now > 1


@pytest.mark.asyncio
async def test_active_poll_unexpected_oserror_wins_if_deadline_crosses_during_read(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    _install_clock(monkeypatch, clock)
    failure = OSError("read failed")

    def read_bytes(path: Path) -> Never:
        assert path == response_path
        clock.now = 2.0
        raise failure

    monkeypatch.setattr(Path, "read_bytes", read_bytes)

    with pytest.raises(BridgeError) as caught:
        await _waiter(response_path, expectation, expected_process).wait(1)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.__cause__ is failure


@pytest.mark.asyncio
async def test_deadline_after_repeated_json_decode_failures_returns_last_protocol_error(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    _install_clock(monkeypatch, clock)
    malformed = b'{"Comments":"not json"}'
    reads = _install_read_observations(monkeypatch, response_path, [malformed, malformed])
    parser_errors = _record_parser_errors(monkeypatch)

    with pytest.raises(BridgeError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            poll_seconds=1,
        ).wait(2)

    assert caught.value.code is ErrorCode.PROTOCOL_ERROR
    assert caught.value.message == "invalid Comments envelope JSON"
    assert caught.value is parser_errors[-1]
    assert len(parser_errors) == 2
    assert len(reads) == 2


@pytest.mark.asyncio
async def test_cancellation_while_sleeping_propagates_identity_without_additional_work(
    response_path: Path,
    expectation: ResponseExpectation,
    expected_process: ProcessInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancellation = asyncio.CancelledError("cancel wait")
    clock = _FakeClock(sleep_failure=cancellation)
    _install_clock(monkeypatch, clock)
    reads = _install_read_observations(
        monkeypatch,
        response_path,
        [FileNotFoundError(response_path)],
    )
    checks = 0

    def process_check() -> ProcessInfo:
        nonlocal checks
        checks += 1
        return expected_process

    with pytest.raises(asyncio.CancelledError) as caught:
        await _waiter(
            response_path,
            expectation,
            expected_process,
            process_check,
        ).wait(10)

    assert caught.value is cancellation
    assert checks == 1
    assert len(reads) == 1
    assert clock.time_ns_calls == 0
    assert not response_path.exists()
