from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Callable, Protocol, cast
from unittest.mock import Mock

import psutil
import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.transports.file_bridge import process_guard
from cmo_agent_bridge.transports.file_bridge.process_guard import (
    CmoProcessInspector,
    ProcessInfo,
    PsutilCmoProcessInspector,
    require_single_instance,
)


class _ProcessLike(Protocol):
    @property
    def pid(self) -> int: ...

    def exe(self) -> str: ...

    def create_time(self) -> float: ...

    def is_running(self) -> bool: ...


class _FakeProcess:
    def __init__(
        self,
        pid: int,
        executable: str,
        create_time: float,
        *,
        running: bool = True,
        failure_phase: str | None = None,
        failure: BaseException | None = None,
        events: list[tuple[int, str]] | None = None,
    ) -> None:
        self._pid = pid
        self._executable = executable
        self._create_time = create_time
        self._running = running
        self._failure_phase = failure_phase
        self._failure = failure
        self._events = events if events is not None else []

    def _event(self, phase: str) -> None:
        self._events.append((id(self), phase))
        if self._failure_phase == phase and self._failure is not None:
            raise self._failure

    @property
    def pid(self) -> int:
        self._event("pid")
        return self._pid

    def exe(self) -> str:
        self._event("exe")
        return self._executable

    def create_time(self) -> float:
        self._event("create_time")
        return self._create_time

    def is_running(self) -> bool:
        self._event("is_running")
        return self._running


class _StubInspector:
    def __init__(self, processes: tuple[ProcessInfo, ...]) -> None:
        self.processes = processes
        self.seen: list[Path] = []

    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        self.seen.append(command_exe)
        return self.processes


def _command_exe(tmp_path: Path, root_name: str = "CMO") -> Path:
    root = tmp_path / root_name
    root.mkdir()
    command = root / "Command.exe"
    command.write_bytes(b"command")
    return command


def _install_process_factory(
    monkeypatch: pytest.MonkeyPatch,
    pids: list[int],
    factory: Callable[[int], _ProcessLike],
) -> None:
    monkeypatch.setattr(process_guard.psutil, "pids", lambda: list(pids))
    monkeypatch.setattr(process_guard.psutil, "Process", factory)

    def forbidden_process_iter(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("process_iter cache must never be used")

    monkeypatch.setattr(process_guard.psutil, "process_iter", forbidden_process_iter)


def test_process_info_is_frozen_and_exact_identity_compares_all_fields(tmp_path: Path) -> None:
    executable = _command_exe(tmp_path).resolve(strict=True)
    baseline = ProcessInfo(pid=10, create_time=1000.0, executable=executable)

    assert baseline == ProcessInfo(pid=10, create_time=1000.0, executable=executable)
    assert baseline != ProcessInfo(pid=10, create_time=1000.0000001, executable=executable)
    assert baseline != ProcessInfo(pid=11, create_time=1000.0, executable=executable)
    assert baseline != ProcessInfo(
        pid=10,
        create_time=1000.0,
        executable=_command_exe(tmp_path, "Other").resolve(strict=True),
    )
    with pytest.raises(FrozenInstanceError):
        baseline.pid = 11  # type: ignore[misc]


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (r"D:\Games\CMO\Command.exe", r"d:/games/cmo/COMMAND.EXE"),
        (r"\\?\D:\Games\CMO\Command.exe", r"d:\games\cmo\command.exe"),
        (r"\\?\UNC\server\share\Command.exe", r"\\server\share\command.exe"),
    ],
)
def test_windows_paths_equal_exposes_the_process_selection_normalization(
    left: str,
    right: str,
) -> None:
    assert process_guard.windows_paths_equal(Path(left), Path(right)) is True
    assert process_guard.windows_paths_equal(Path(left), Path(r"D:\Other\Command.exe")) is False


def test_psutil_scan_builds_fresh_processes_and_reads_one_object_consistently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command_exe(tmp_path)
    events: list[tuple[int, str]] = []
    constructed: list[_FakeProcess] = []

    def factory(pid: int) -> _FakeProcess:
        process = _FakeProcess(pid, str(command), 1000.0 + pid, events=events)
        constructed.append(process)
        return process

    _install_process_factory(monkeypatch, [20, 10], factory)
    inspector = PsutilCmoProcessInspector()

    first = inspector.matching_processes(command)
    second = inspector.matching_processes(command)

    assert tuple(item.pid for item in first) == (10, 20)
    assert second == first
    assert len(constructed) == 4
    assert len({id(item) for item in constructed}) == 4
    for instance in constructed:
        assert [phase for token, phase in events if token == id(instance)] == [
            "pid",
            "exe",
            "create_time",
            "is_running",
        ]


def test_matching_uses_windows_case_semantics_and_exact_resolved_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command_exe(tmp_path, "MixedCaseRoot")
    same_name_elsewhere = _command_exe(tmp_path, "DifferentRoot")
    sibling = command.parent / "Command-copy.exe"
    sibling.write_bytes(b"copy")
    candidates = {
        10: _FakeProcess(10, str(command).swapcase(), 3.0),
        11: _FakeProcess(11, str(same_name_elsewhere), 2.0),
        12: _FakeProcess(12, str(sibling), 1.0),
    }
    _install_process_factory(monkeypatch, [12, 11, 10], candidates.__getitem__)

    matches = PsutilCmoProcessInspector().matching_processes(command)

    assert matches == (
        ProcessInfo(pid=10, create_time=3.0, executable=command.resolve(strict=True)),
    )


def test_target_command_must_strict_resolve_before_scanning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process_factory = Mock(side_effect=AssertionError("must not scan"))
    monkeypatch.setattr(process_guard.psutil, "pids", lambda: [10])
    monkeypatch.setattr(process_guard.psutil, "Process", process_factory)

    with pytest.raises(BridgeError) as caught:
        PsutilCmoProcessInspector().matching_processes(tmp_path / "missing" / "Command.exe")

    assert caught.value.code is ErrorCode.GAME_ROOT_INVALID
    process_factory.assert_not_called()


@pytest.mark.parametrize(
    ("executable", "running"),
    [("", True), ("missing.exe", True), ("{command}", False)],
)
def test_empty_disappeared_and_no_longer_running_candidates_are_ignored(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    executable: str,
    running: bool,
) -> None:
    command = _command_exe(tmp_path)
    value = str(command) if executable == "{command}" else executable
    candidate = _FakeProcess(10, value, 1000.0, running=running)
    _install_process_factory(monkeypatch, [10], lambda _pid: candidate)

    assert PsutilCmoProcessInspector().matching_processes(command) == ()


def test_empty_executable_is_skipped_before_create_time_is_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command_exe(tmp_path)
    events: list[tuple[int, str]] = []
    candidate = _FakeProcess(
        10,
        "",
        1000.0,
        failure_phase="create_time",
        failure=RuntimeError("create time must not be read"),
        events=events,
    )
    _install_process_factory(monkeypatch, [10], lambda _pid: candidate)

    assert PsutilCmoProcessInspector().matching_processes(command) == ()
    assert [phase for _token, phase in events] == ["pid", "exe"]


def test_non_file_executable_candidate_is_not_silently_ignored(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command_exe(tmp_path)
    directory = tmp_path / "not-an-executable"
    directory.mkdir()
    candidate = _FakeProcess(10, str(directory), 1000.0)
    _install_process_factory(monkeypatch, [10], lambda _pid: candidate)

    with pytest.raises(BridgeError) as caught:
        PsutilCmoProcessInspector().matching_processes(command)

    assert caught.value.code is ErrorCode.STATE_CONFLICT


@pytest.mark.parametrize("phase", ["construct", "pid", "exe", "create_time", "is_running"])
@pytest.mark.parametrize(
    "failure_factory",
    [
        lambda: psutil.AccessDenied(pid=10),
        lambda: psutil.NoSuchProcess(pid=10),
        lambda: psutil.ZombieProcess(pid=10),
    ],
)
def test_only_documented_psutil_races_are_ignored_at_every_scan_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    failure_factory: Callable[[], BaseException],
) -> None:
    command = _command_exe(tmp_path)
    failure = failure_factory()

    def factory(pid: int) -> _FakeProcess:
        if phase == "construct":
            raise failure
        return _FakeProcess(
            pid,
            str(command),
            1000.0,
            failure_phase=phase,
            failure=failure,
        )

    _install_process_factory(monkeypatch, [10], factory)

    assert PsutilCmoProcessInspector().matching_processes(command) == ()


def test_unexpected_process_error_is_not_silently_ignored(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command_exe(tmp_path)
    failure = RuntimeError("unexpected psutil failure")
    candidate = _FakeProcess(
        10,
        str(command),
        1000.0,
        failure_phase="exe",
        failure=failure,
    )
    _install_process_factory(monkeypatch, [10], lambda _pid: candidate)

    with pytest.raises(BridgeError) as caught:
        PsutilCmoProcessInspector().matching_processes(command)

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.__cause__ is failure


def test_scan_results_are_stably_sorted_by_pid_then_create_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command_exe(tmp_path)
    pids = [20, 10, 10]
    queue = [
        _FakeProcess(20, str(command), 5.0),
        _FakeProcess(10, str(command), 7.0),
        _FakeProcess(10, str(command), 3.0),
    ]

    def factory(_pid: int) -> _FakeProcess:
        return queue.pop(0)

    _install_process_factory(monkeypatch, pids, factory)

    matches = PsutilCmoProcessInspector().matching_processes(command)

    assert [(item.pid, item.create_time) for item in matches] == [
        (10, 3.0),
        (10, 7.0),
        (20, 5.0),
    ]


def test_same_pid_with_new_create_time_is_a_different_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command_exe(tmp_path)
    create_times = iter([1000.0, 1001.0])

    def factory(pid: int) -> _FakeProcess:
        return _FakeProcess(pid, str(command), next(create_times))

    _install_process_factory(monkeypatch, [10], factory)
    inspector = PsutilCmoProcessInspector()

    original = inspector.matching_processes(command)[0]
    replacement = inspector.matching_processes(command)[0]

    assert original.pid == replacement.pid
    assert original.executable == replacement.executable
    assert original != replacement


def test_same_path_with_new_pid_is_a_different_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = _command_exe(tmp_path)
    pids = iter([10, 11])
    monkeypatch.setattr(process_guard.psutil, "pids", lambda: [next(pids)])

    def factory(pid: int) -> _FakeProcess:
        return _FakeProcess(pid, str(command), 1000.0)

    monkeypatch.setattr(
        process_guard.psutil,
        "Process",
        factory,
    )

    inspector = PsutilCmoProcessInspector()
    original = inspector.matching_processes(command)[0]
    replacement = inspector.matching_processes(command)[0]

    assert original.create_time == replacement.create_time
    assert original.executable == replacement.executable
    assert original != replacement


def test_require_single_instance_returns_exactly_one_frozen_process(tmp_path: Path) -> None:
    command = _command_exe(tmp_path)
    expected = ProcessInfo(pid=10, create_time=1000.0, executable=command.resolve())
    inspector = _StubInspector((expected,))

    result = require_single_instance(inspector, command)

    assert result is expected
    assert inspector.seen == [command]


def test_zero_exact_command_processes_are_rejected(tmp_path: Path) -> None:
    command = _command_exe(tmp_path)
    inspector = _StubInspector(())

    with pytest.raises(BridgeError) as caught:
        require_single_instance(inspector, command)

    assert caught.value.code is ErrorCode.CMO_NOT_RUNNING


def test_two_exact_command_processes_are_rejected(tmp_path: Path) -> None:
    command = _command_exe(tmp_path)
    inspector = _StubInspector(
        (
            ProcessInfo(pid=10, create_time=1000.0, executable=command),
            ProcessInfo(pid=11, create_time=1001.0, executable=command),
        )
    )

    with pytest.raises(BridgeError) as caught:
        require_single_instance(inspector, command)

    assert caught.value.code is ErrorCode.MULTIPLE_CMO_INSTANCES
    assert caught.value.details["count"] == 2


def test_stub_conforms_to_inspector_protocol() -> None:
    inspector = cast(CmoProcessInspector, _StubInspector(()))

    assert inspector.matching_processes(Path("Command.exe")) == ()
