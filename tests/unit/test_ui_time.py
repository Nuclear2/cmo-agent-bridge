from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from pathlib import Path
import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo
from cmo_agent_bridge.ui_time import (
    SimulationRunState,
    TimeRate,
    UiTimeController,
    UiTimeProcessResult,
)


class _Inspector:
    def __init__(self, *results: tuple[ProcessInfo, ...]) -> None:
        self._results = list(results)
        self.calls: list[Path] = []

    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        self.calls.append(command_exe)
        if not self._results:
            raise AssertionError("unexpected process inspection")
        return self._results.pop(0)


class _Runner:
    def __init__(self, result: UiTimeProcessResult) -> None:
        self._result = result
        self.calls: list[tuple[tuple[str, ...], Path, float]] = []

    async def __call__(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
    ) -> UiTimeProcessResult:
        self.calls.append((tuple(command), cwd, timeout_seconds))
        return self._result


def _process(tmp_path: Path, *, pid: int = 42, create_time: float = 1000.125) -> ProcessInfo:
    root = tmp_path / "CMO"
    root.mkdir(exist_ok=True)
    executable = root / "Command.exe"
    executable.write_bytes(b"marker")
    return ProcessInfo(pid=pid, create_time=create_time, executable=executable.resolve(strict=True))


def _success(
    process: ProcessInfo,
    *,
    state: str = "paused",
    rate: TimeRate = TimeRate.X1,
) -> UiTimeProcessResult:
    return UiTimeProcessResult(
        returncode=0,
        stdout=json.dumps(
            {
                "ok": True,
                "pid": process.pid,
                "process_start_time_unix_ms": int(process.create_time * 1000),
                "executable": str(process.executable),
                "window_handle": 123456,
                "window_title": "Scenario - Command: Modern Operations",
                "state": state,
                "rate_code": rate.value,
                # The UI helper reports legacy labels for the two flame controls.
                # They identify controls, not guaranteed effective multipliers.
                "rate_multiplier": (1, 2, 5, 15, 30, 150)[rate.value],
            }
        ).encode(),
        stderr=b"",
    )


def _controller(
    process: ProcessInfo,
    runner: _Runner,
    inspector: _Inspector,
) -> UiTimeController:
    return UiTimeController(
        process.executable,
        process_inspector=inspector,
        runner=runner,
        powershell_executable="powershell.exe",
        timeout_seconds=2.5,
    )


@pytest.mark.parametrize(
    ("rate", "multiplier"),
    [
        (TimeRate.X1, 1),
        (TimeRate.X2, 2),
        (TimeRate.X5, 5),
        (TimeRate.X15, 15),
        (TimeRate.COARSE_1_SECOND, None),
        (TimeRate.COARSE_5_SECONDS, None),
    ],
)
def test_time_rate_exposes_only_guaranteed_multiplier(
    rate: TimeRate,
    multiplier: int | None,
) -> None:
    assert rate.value in range(6)
    assert rate.multiplier == multiplier


def test_coarse_rates_preserve_legacy_names_without_claiming_fixed_speed() -> None:
    assert TimeRate.X30 is TimeRate.COARSE_1_SECOND
    assert TimeRate.X150 is TimeRate.COARSE_5_SECONDS
    assert TimeRate.COARSE_1_SECOND.coarse_slice_seconds == 1
    assert TimeRate.COARSE_5_SECONDS.coarse_slice_seconds == 5


@pytest.mark.parametrize(
    ("timeout_override", "expected_timeout"),
    [(None, 15.0), (2.5, 2.5)],
)
async def test_helper_timeout_uses_default_and_honors_explicit_override(
    tmp_path: Path,
    timeout_override: float | None,
    expected_timeout: float,
) -> None:
    process = _process(tmp_path)
    inspector = _Inspector((process,), (process,))
    runner = _Runner(_success(process))
    if timeout_override is None:
        controller = UiTimeController(
            process.executable,
            process_inspector=inspector,
            runner=runner,
            powershell_executable="powershell.exe",
        )
    else:
        controller = UiTimeController(
            process.executable,
            process_inspector=inspector,
            runner=runner,
            powershell_executable="powershell.exe",
            timeout_seconds=timeout_override,
        )

    await controller.get_state()

    assert runner.calls[0][2] == expected_timeout


async def test_get_state_binds_exact_process_identity_and_parses_snapshot(tmp_path: Path) -> None:
    process = _process(tmp_path)
    inspector = _Inspector((process,), (process,))
    runner = _Runner(_success(process, state="running", rate=TimeRate.X15))
    controller = _controller(process, runner, inspector)

    state = await controller.get_state()

    assert state.process is process
    assert state.state is SimulationRunState.RUNNING
    assert state.rate is TimeRate.X15
    assert state.multiplier == 15
    assert state.window_handle == 123456
    assert state.window_title.startswith("Scenario")
    assert inspector.calls == [process.executable, process.executable]
    command, cwd, timeout = runner.calls[0]
    assert command[0] == "powershell.exe"
    assert command[command.index("-ProcessId") + 1] == "42"
    assert command[command.index("-ExpectedExecutable") + 1] == str(process.executable)
    assert command[command.index("-ExpectedCreateTimeUnixMs") + 1] == "1000125"
    assert command[command.index("-Action") + 1] == "get"
    assert "-RateCode" not in command
    assert command[command.index("-File") + 1].endswith("ui_time_controller.ps1")
    assert cwd == process.executable.parent
    assert timeout == 2.5


async def test_get_state_accepts_legacy_x150_label_as_cpu_driven_coarse_rate(
    tmp_path: Path,
) -> None:
    process = _process(tmp_path)
    inspector = _Inspector((process,), (process,))
    runner = _Runner(_success(process, state="running", rate=TimeRate.COARSE_5_SECONDS))
    controller = _controller(process, runner, inspector)

    state = await controller.get_state()

    assert state.rate is TimeRate.COARSE_5_SECONDS
    assert state.multiplier is None
    assert state.rate.coarse_slice_seconds == 5


@pytest.mark.parametrize(
    ("method", "rate", "expected_action", "expected_rate_code"),
    [
        ("pause", None, "pause", None),
        ("resume", None, "resume", None),
        ("resume", TimeRate.X15, "resume", "3"),
        ("resume", TimeRate.X1, "play-1x", None),
        ("set_rate", TimeRate.X30, "set-rate", "4"),
        ("play_1x", None, "play-1x", None),
    ],
)
async def test_mutation_methods_select_only_semantic_ui_actions(
    tmp_path: Path,
    method: str,
    rate: TimeRate | None,
    expected_action: str,
    expected_rate_code: str | None,
) -> None:
    process = _process(tmp_path)
    inspector = _Inspector((process,), (process,))
    result_rate = rate or TimeRate.X1
    runner = _Runner(_success(process, state="paused", rate=result_rate))
    controller = _controller(process, runner, inspector)

    if method == "pause":
        await controller.pause()
    elif method == "resume":
        await controller.resume(rate)
    elif method == "set_rate":
        assert rate is not None
        await controller.set_rate(rate)
    elif method == "play_1x":
        await controller.play_1x()
    else:
        raise AssertionError(f"unknown test method: {method}")

    command = runner.calls[0][0]
    assert command[command.index("-Action") + 1] == expected_action
    if expected_rate_code is None:
        assert "-RateCode" not in command
    else:
        assert command[command.index("-RateCode") + 1] == expected_rate_code


async def test_process_replacement_after_helper_call_fails_closed(tmp_path: Path) -> None:
    original = _process(tmp_path)
    replacement = ProcessInfo(
        pid=original.pid,
        create_time=original.create_time + 1,
        executable=original.executable,
    )
    inspector = _Inspector((original,), (replacement,))
    runner = _Runner(_success(original))
    controller = _controller(original, runner, inspector)

    with pytest.raises(BridgeError) as caught:
        await controller.pause()

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert "identity changed" in caught.value.message


async def test_helper_error_payload_is_structured_and_fails_closed(tmp_path: Path) -> None:
    process = _process(tmp_path)
    inspector = _Inspector((process,), (process,))
    runner = _Runner(
        UiTimeProcessResult(
            returncode=1,
            stdout=json.dumps(
                {
                    "ok": False,
                    "code": "CONTROL_AMBIGUOUS",
                    "message": "required control count was 2",
                    "details": {"automation_id": "PlayButton", "count": 2},
                }
            ).encode(),
            stderr=b"",
        )
    )
    controller = _controller(process, runner, inspector)

    with pytest.raises(BridgeError) as caught:
        await controller.pause()

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert caught.value.details["helper_code"] == "CONTROL_AMBIGUOUS"
    assert caught.value.details["automation_id"] == "PlayButton"


async def test_success_payload_must_match_process_and_rate_contract(tmp_path: Path) -> None:
    process = _process(tmp_path)
    inspector = _Inspector((process,), (process,))
    payload = json.loads(_success(process).stdout)
    payload["rate_multiplier"] = 2
    runner = _Runner(
        UiTimeProcessResult(returncode=0, stdout=json.dumps(payload).encode(), stderr=b"")
    )
    controller = _controller(process, runner, inspector)

    with pytest.raises(BridgeError) as caught:
        await controller.get_state()

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert "contradictory" in caught.value.message


async def test_controller_serializes_ui_actions(tmp_path: Path) -> None:
    process = _process(tmp_path)
    inspector = _Inspector((process,), (process,), (process,), (process,))
    active = 0
    maximum_active = 0

    async def runner(
        command: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
    ) -> UiTimeProcessResult:
        nonlocal active, maximum_active
        del command, cwd, timeout_seconds
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return _success(process)

    controller = UiTimeController(
        process.executable,
        process_inspector=inspector,
        runner=runner,
        powershell_executable="powershell.exe",
    )

    await asyncio.gather(controller.get_state(), controller.get_state())

    assert maximum_active == 1


async def test_timeout_is_reported_without_parsing_an_unknown_outcome(tmp_path: Path) -> None:
    process = _process(tmp_path)
    inspector = _Inspector((process,), (process,), (process,))
    actions: list[str] = []

    async def runner(
        command: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
    ) -> UiTimeProcessResult:
        del cwd, timeout_seconds
        action = command[command.index("-Action") + 1]
        actions.append(action)
        if action == "play-1x":
            raise TimeoutError
        return _success(process)

    controller = UiTimeController(
        process.executable,
        process_inspector=inspector,
        runner=runner,
        powershell_executable="powershell.exe",
    )

    with pytest.raises(BridgeError) as caught:
        await controller.play_1x()

    assert caught.value.code is ErrorCode.REQUEST_TIMEOUT
    assert caught.value.details["action_may_have_applied"] is True
    assert caught.value.details["safety_pause_attempted"] is True
    assert caught.value.details["safety_pause_verified"] is True
    assert actions == ["play-1x", "pause"]


async def test_failed_1x_release_and_failed_safety_pause_report_unknown_run_state(
    tmp_path: Path,
) -> None:
    process = _process(tmp_path)
    inspector = _Inspector((process,), (process,))
    actions: list[str] = []

    async def runner(
        command: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
    ) -> UiTimeProcessResult:
        del cwd, timeout_seconds
        actions.append(command[command.index("-Action") + 1])
        raise TimeoutError

    controller = UiTimeController(
        process.executable,
        process_inspector=inspector,
        runner=runner,
        powershell_executable="powershell.exe",
    )

    with pytest.raises(BridgeError) as caught:
        await controller.play_1x()

    assert caught.value.code is ErrorCode.STATE_CONFLICT
    assert "may still be running" in caught.value.message
    assert caught.value.details["action_may_have_applied"] is True
    assert caught.value.details["safety_pause_attempted"] is True
    assert caught.value.details["safety_pause_verified"] is False
    assert actions == ["play-1x", "pause"]


async def test_failed_high_rate_resume_uses_the_same_verified_safety_pause(
    tmp_path: Path,
) -> None:
    process = _process(tmp_path)
    inspector = _Inspector((process,), (process,), (process,))
    actions: list[str] = []

    async def runner(
        command: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
    ) -> UiTimeProcessResult:
        del cwd, timeout_seconds
        action = command[command.index("-Action") + 1]
        actions.append(action)
        if action == "resume":
            raise TimeoutError
        return _success(process)

    controller = UiTimeController(
        process.executable,
        process_inspector=inspector,
        runner=runner,
        powershell_executable="powershell.exe",
    )

    with pytest.raises(BridgeError) as caught:
        await controller.resume(TimeRate.X15)

    assert caught.value.code is ErrorCode.REQUEST_TIMEOUT
    assert caught.value.details["safety_pause_verified"] is True
    assert actions == ["resume", "pause"]


def test_packaged_helper_uses_semantic_controls_without_input_injection() -> None:
    script = (
        Path(__file__).parents[2]
        / "src"
        / "cmo_agent_bridge"
        / "host_assets"
        / "ui_time_controller.ps1"
    ).read_text(encoding="utf-8")

    for required in (
        "UIAutomationClient",
        "UIAutomationTypes",
        '"PlayButton"',
        '"PauseButton"',
        'AutomationId "PlayButtonAt1Time"',
        'AutomationId "TimeComboBox"',
        '"TimeItem1x"',
        '"TimeItem2x"',
        '"TimeItem5x"',
        '"TimeItem15x"',
        '"TimeItemTurbo"',
        '"TimeItemDoubleFlame"',
        "InvokePattern",
        "SelectionItemPattern",
        "ExpectedExecutable",
        "ExpectedCreateTimeUnixMs",
        'Code "MODAL_WINDOW"',
        "root.Current.IsEnabled",
        "GetForegroundWindow",
        "SetForegroundWindow",
        "TrySetForegroundWindowFromProcess",
    ):
        assert required in script
    for forbidden in (
        "SendInput",
        "SendKeys",
        "mouse_event",
        "keybd_event",
        "Cursor.Position",
        "SetFocus()",
    ):
        assert forbidden not in script


def test_packaged_helper_restores_only_from_cmo_with_detached_input_fallback() -> None:
    script = (
        Path(__file__).parents[2]
        / "src"
        / "cmo_agent_bridge"
        / "host_assets"
        / "ui_time_controller.ps1"
    ).read_text(encoding="utf-8")
    restore_start = script.index("function Restore-OriginalForeground")
    restore_end = script.index("$originalForeground = [IntPtr]::Zero")
    restore = script[restore_start:restore_end]

    for required in (
        "GetWindowThreadProcessId",
        "GetWindowProcessId",
        "GetCurrentThreadId",
        "IsWindowOwnedByProcess",
        "TrySetForegroundWindowFromProcess",
        "AttachThreadInput",
        "$currentProcessId -ne $ProcessId",
        "finally",
    ):
        assert required in script

    assert "$currentForeground -ne $CmoWindow" not in script
    assert restore.count("AttachThreadInput(") == 4
    assert restore.count("$false") >= 2
    assert restore.index("finally") < restore.rindex("$false")
    assert "AutomationElement]::FromHandle($OriginalForeground)" not in restore
