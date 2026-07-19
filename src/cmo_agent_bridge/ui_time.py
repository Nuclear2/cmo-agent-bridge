from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from importlib.resources import as_file, files
from pathlib import Path
from typing import Protocol, cast

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.transports.file_bridge.process_guard import (
    CmoProcessInspector,
    ProcessInfo,
    PsutilCmoProcessInspector,
    require_single_instance,
    windows_paths_equal,
)


class SimulationRunState(StrEnum):
    PAUSED = "paused"
    RUNNING = "running"


class TimeRate(IntEnum):
    X1 = 0
    X2 = 1
    X5 = 2
    X15 = 3
    COARSE_1_SECOND = 4
    COARSE_5_SECONDS = 5

    # Backward-compatible aliases for callers that persisted the old names.  CMO's
    # flame controls may be labelled 30x/150x in the UI, but their effective speed
    # is CPU-driven unless high-simulation-speed synchronisation is enabled.
    X30 = COARSE_1_SECOND
    X150 = COARSE_5_SECONDS

    @property
    def multiplier(self) -> int | None:
        return (1, 2, 5, 15, None, None)[self.value]

    @property
    def coarse_slice_seconds(self) -> int | None:
        return (None, None, None, None, 1, 5)[self.value]


@dataclass(frozen=True, slots=True)
class UiTimeState:
    process: ProcessInfo
    state: SimulationRunState
    rate: TimeRate
    window_handle: int
    window_title: str

    @property
    def multiplier(self) -> int | None:
        return self.rate.multiplier


@dataclass(frozen=True, slots=True)
class UiTimeProcessResult:
    returncode: int
    stdout: bytes
    stderr: bytes


class UiTimeProcessRunner(Protocol):
    async def __call__(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
    ) -> UiTimeProcessResult: ...


async def _run_process(
    command: Sequence[str],
    *,
    cwd: Path,
    timeout_seconds: float,
) -> UiTimeProcessResult:
    creationflags = cast(int, getattr(subprocess, "CREATE_NO_WINDOW", 0)) if os.name == "nt" else 0
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=str(cwd),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=creationflags,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout_seconds)
    except TimeoutError:
        process.kill()
        await process.communicate()
        raise
    except asyncio.CancelledError:
        process.kill()
        await process.communicate()
        raise
    return UiTimeProcessResult(
        returncode=process.returncode if process.returncode is not None else -1,
        stdout=stdout,
        stderr=stderr,
    )


def _bridge_failure(
    message: str,
    *,
    details: dict[str, object] | None = None,
    cause: BaseException | None = None,
) -> BridgeError:
    error = BridgeError(ErrorCode.STATE_CONFLICT, message, details)
    if cause is not None:
        error.__cause__ = cause
    return error


def _require_exact_keys(
    payload: dict[str, object],
    expected: set[str],
    *,
    description: str,
) -> None:
    actual = set(payload)
    if actual != expected:
        raise _bridge_failure(
            f"{description} has an invalid shape",
            details={
                "missing": sorted(expected - actual),
                "unknown": sorted(actual - expected),
            },
        )


def _parse_error_payload(
    payload: dict[str, object], *, returncode: int, stderr: str
) -> BridgeError:
    _require_exact_keys(
        payload, {"ok", "code", "message", "details"}, description="UI helper error"
    )
    if payload["ok"] is not False:
        raise _bridge_failure(
            "UI helper returned a contradictory failure payload",
            details={"returncode": returncode},
        )
    code = payload["code"]
    message = payload["message"]
    details = payload["details"]
    if type(code) is not str or type(message) is not str or type(details) is not dict:
        raise _bridge_failure(
            "UI helper returned an invalid failure payload",
            details={"returncode": returncode},
        )
    merged = cast(dict[str, object], details).copy()
    merged.update({"helper_code": code, "returncode": returncode})
    if stderr:
        merged["stderr"] = stderr
    return _bridge_failure(f"CMO UI time control failed: {message}", details=merged)


def _parse_success_payload(payload: dict[str, object], process: ProcessInfo) -> UiTimeState:
    _require_exact_keys(
        payload,
        {
            "ok",
            "pid",
            "process_start_time_unix_ms",
            "executable",
            "window_handle",
            "window_title",
            "state",
            "rate_code",
            "rate_multiplier",
        },
        description="UI helper result",
    )
    if payload["ok"] is not True:
        raise _bridge_failure("UI helper returned a contradictory success payload")

    pid = payload["pid"]
    start_ms = payload["process_start_time_unix_ms"]
    executable = payload["executable"]
    window_handle = payload["window_handle"]
    window_title = payload["window_title"]
    state_value = payload["state"]
    rate_code = payload["rate_code"]
    rate_multiplier = payload["rate_multiplier"]
    scalar_types_are_valid = (
        type(pid) is int
        and type(start_ms) is int
        and type(executable) is str
        and type(window_handle) is int
        and type(window_title) is str
        and type(state_value) is str
        and type(rate_code) is int
        and type(rate_multiplier) is int
    )
    if not scalar_types_are_valid:
        raise _bridge_failure("UI helper result contains invalid field types")

    expected_start_ms = int(process.create_time * 1000)
    if (
        pid != process.pid
        or start_ms != expected_start_ms
        or not windows_paths_equal(Path(cast(str, executable)), process.executable)
    ):
        raise _bridge_failure(
            "UI helper acted on a different CMO process identity",
            details={
                "expected_pid": process.pid,
                "observed_pid": pid,
                "expected_start_time_unix_ms": expected_start_ms,
                "observed_start_time_unix_ms": start_ms,
            },
        )
    if cast(int, window_handle) <= 0:
        raise _bridge_failure("UI helper returned an invalid CMO window handle")

    try:
        state = SimulationRunState(cast(str, state_value))
        rate = TimeRate(cast(int, rate_code))
    except ValueError as error:
        raise _bridge_failure("UI helper returned an unknown simulation state or rate", cause=error)
    # The helper preserves legacy UI label values (30/150) for the two flame
    # controls so it can identify older CMO builds.  Those labels are not a
    # guarantee of effective simulation speed and are deliberately not exposed
    # as multipliers by ``UiTimeState``.
    helper_multiplier = (1, 2, 5, 15, 30, 150)[rate.value]
    if helper_multiplier != rate_multiplier:
        raise _bridge_failure(
            "UI helper returned contradictory time-compression values",
            details={"rate_code": rate.value, "rate_multiplier": rate_multiplier},
        )
    return UiTimeState(
        process=process,
        state=state,
        rate=rate,
        window_handle=cast(int, window_handle),
        window_title=cast(str, window_title),
    )


class UiTimeController:
    """Control CMO's host UI through exact Windows UI Automation identities."""

    def __init__(
        self,
        command_exe: Path,
        *,
        process_inspector: CmoProcessInspector | None = None,
        runner: UiTimeProcessRunner | None = None,
        powershell_executable: str | None = None,
        timeout_seconds: float = 15.0,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._command_exe = Path(command_exe)
        self._process_inspector = process_inspector or PsutilCmoProcessInspector()
        self._runner = runner or _run_process
        self._powershell_executable = powershell_executable
        self._timeout_seconds = timeout_seconds
        self._lock = asyncio.Lock()

    async def get_state(self) -> UiTimeState:
        return await self._execute("get")

    async def pause(self) -> UiTimeState:
        return await self._execute("pause")

    async def resume(self, rate: TimeRate | None = None) -> UiTimeState:
        if rate is TimeRate.X1:
            return await self.play_1x()
        return await self._execute_running_transition("resume", rate=rate)

    async def set_rate(self, rate: TimeRate) -> UiTimeState:
        return await self._execute("set-rate", rate=rate)

    async def play_1x(self) -> UiTimeState:
        """Ensure the simulation is running at 1x through PlayButtonAt1Time."""

        return await self._execute_running_transition("play-1x")

    async def _execute_running_transition(
        self,
        action: str,
        *,
        rate: TimeRate | None = None,
    ) -> UiTimeState:
        async with self._lock:
            try:
                return await self._execute_unlocked(action, rate=rate)
            except asyncio.CancelledError:
                cleanup = asyncio.create_task(self._execute_unlocked("pause"))
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    try:
                        await cleanup
                    except Exception:
                        pass
                except Exception:
                    pass
                raise
            except Exception as error:
                try:
                    await self._execute_unlocked("pause")
                except Exception as pause_error:
                    raise _bridge_failure(
                        "CMO may still be running after a failed transition to running",
                        details={
                            "action_may_have_applied": True,
                            "safety_pause_attempted": True,
                            "safety_pause_verified": False,
                            "original_error": str(error),
                            "safety_pause_error": str(pause_error),
                        },
                        cause=error,
                    )
                if isinstance(error, BridgeError):
                    details = error.details.copy()
                    details.update(
                        {
                            "action_may_have_applied": True,
                            "safety_pause_attempted": True,
                            "safety_pause_verified": True,
                        }
                    )
                    raise BridgeError(error.code, error.message, details) from error
                raise

    async def _execute(self, action: str, *, rate: TimeRate | None = None) -> UiTimeState:
        async with self._lock:
            return await self._execute_unlocked(action, rate=rate)

    async def _execute_unlocked(
        self,
        action: str,
        *,
        rate: TimeRate | None = None,
    ) -> UiTimeState:
        process = require_single_instance(self._process_inspector, self._command_exe)
        powershell = self._powershell_executable or shutil.which("powershell.exe")
        powershell = powershell or shutil.which("pwsh.exe")
        powershell = powershell or shutil.which("powershell") or shutil.which("pwsh")
        if powershell is None:
            raise _bridge_failure("PowerShell is unavailable on this Windows host")

        script = files("cmo_agent_bridge.host_assets").joinpath("ui_time_controller.ps1")
        arguments = [
            powershell,
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
        ]
        try:
            with as_file(script) as script_path:
                command = [
                    *arguments,
                    str(script_path),
                    "-ProcessId",
                    str(process.pid),
                    "-ExpectedExecutable",
                    str(process.executable),
                    "-ExpectedCreateTimeUnixMs",
                    str(int(process.create_time * 1000)),
                    "-Action",
                    action,
                ]
                if rate is not None:
                    command.extend(("-RateCode", str(rate.value)))
                result = await self._runner(
                    command,
                    cwd=process.executable.parent,
                    timeout_seconds=self._timeout_seconds,
                )
        except TimeoutError as error:
            raise BridgeError(
                ErrorCode.REQUEST_TIMEOUT,
                "CMO UI time control timed out",
                {"action": action, "timeout_seconds": self._timeout_seconds},
            ) from error
        except OSError as error:
            raise _bridge_failure(
                "PowerShell could not run the CMO UI time helper",
                details={"action": action},
                cause=error,
            )

        replacement = require_single_instance(self._process_inspector, self._command_exe)
        if replacement != process:
            raise _bridge_failure(
                "CMO process identity changed during UI time control",
                details={
                    "original_pid": process.pid,
                    "replacement_pid": replacement.pid,
                    "original_create_time": process.create_time,
                    "replacement_create_time": replacement.create_time,
                },
            )

        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        try:
            decoded = cast(object, json.loads(result.stdout.decode("utf-8-sig")))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise _bridge_failure(
                "CMO UI time helper returned invalid JSON",
                details={"returncode": result.returncode, "stderr": stderr},
                cause=error,
            )
        if type(decoded) is not dict:
            raise _bridge_failure(
                "CMO UI time helper returned a non-object payload",
                details={"returncode": result.returncode},
            )
        payload = cast(dict[str, object], decoded)
        if result.returncode != 0 or payload.get("ok") is not True:
            raise _parse_error_payload(payload, returncode=result.returncode, stderr=stderr)
        if stderr:
            raise _bridge_failure(
                "CMO UI time helper wrote unexpected diagnostic output",
                details={"stderr": stderr},
            )
        return _parse_success_payload(payload, process)
