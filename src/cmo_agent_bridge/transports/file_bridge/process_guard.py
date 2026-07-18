from __future__ import annotations

import ntpath
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import psutil

from cmo_agent_bridge.errors import BridgeError, ErrorCode


_IGNORED_PROCESS_ERRORS = (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess)


@dataclass(frozen=True, slots=True)
class ProcessInfo:
    pid: int
    create_time: float
    executable: Path


class CmoProcessInspector(Protocol):
    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]: ...


def _windows_normalized(path: Path) -> str:
    value = str(path).replace("/", "\\")
    folded = value.casefold()
    if folded.startswith("\\\\?\\unc\\"):
        value = "\\\\" + value[len("\\\\?\\unc\\") :]
    elif folded.startswith("\\\\?\\"):
        value = value[len("\\\\?\\") :]
    return ntpath.normcase(ntpath.normpath(value))


def windows_paths_equal(left: Path, right: Path) -> bool:
    return _windows_normalized(left) == _windows_normalized(right)


def _target_executable(command_exe: Path) -> Path:
    path = Path(command_exe)
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise BridgeError(
            ErrorCode.GAME_ROOT_INVALID,
            "Command.exe does not exist or cannot be strictly resolved",
            {"path": str(path)},
        ) from error
    if not resolved.is_file():
        raise BridgeError(
            ErrorCode.GAME_ROOT_INVALID,
            "Command.exe must be a regular file",
            {"path": str(resolved)},
        )
    return resolved


def _scan_error(pid: int | None, error: BaseException) -> BridgeError:
    details: dict[str, object] = {"type": type(error).__name__}
    if pid is not None:
        details["pid"] = pid
    bridge_error = BridgeError(
        ErrorCode.STATE_CONFLICT,
        "CMO process inspection failed",
        details,
    )
    bridge_error.__cause__ = error
    return bridge_error


class PsutilCmoProcessInspector:
    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        target = _target_executable(command_exe)
        target_key = _windows_normalized(target)
        try:
            pids = psutil.pids()
        except _IGNORED_PROCESS_ERRORS as error:
            raise _scan_error(None, error) from error
        except Exception as error:
            raise _scan_error(None, error) from error

        matches: list[ProcessInfo] = []
        for requested_pid in pids:
            try:
                process = psutil.Process(requested_pid)
                pid = process.pid
                executable_text = process.exe()
                if not executable_text:
                    continue
                create_time = process.create_time()
                try:
                    executable = Path(executable_text).resolve(strict=True)
                    executable_status = executable.stat()
                except PermissionError:
                    raw_name = ntpath.basename(_windows_normalized(Path(executable_text)))
                    target_name = ntpath.basename(target_key)
                    if raw_name != target_name:
                        continue
                    raise
                except (FileNotFoundError, NotADirectoryError):
                    continue
                if not stat.S_ISREG(executable_status.st_mode):
                    raise ValueError("process executable path is not a regular file")
                if not process.is_running():
                    continue
            except _IGNORED_PROCESS_ERRORS:
                continue
            except Exception as error:
                raise _scan_error(requested_pid, error) from error
            if _windows_normalized(executable) == target_key:
                matches.append(
                    ProcessInfo(
                        pid=pid,
                        create_time=float(create_time),
                        executable=executable,
                    )
                )
        return tuple(sorted(matches, key=lambda process: (process.pid, process.create_time)))


def require_single_instance(
    inspector: CmoProcessInspector,
    command_exe: Path,
) -> ProcessInfo:
    matches = inspector.matching_processes(command_exe)
    if not matches:
        raise BridgeError(
            ErrorCode.CMO_NOT_RUNNING,
            "no running CMO process matches the configured Command.exe",
            {"command_exe": str(command_exe)},
        )
    if len(matches) > 1:
        raise BridgeError(
            ErrorCode.MULTIPLE_CMO_INSTANCES,
            "multiple CMO processes match the configured Command.exe",
            {
                "command_exe": str(command_exe),
                "count": len(matches),
                "pids": [process.pid for process in matches],
            },
        )
    return next(iter(matches))
