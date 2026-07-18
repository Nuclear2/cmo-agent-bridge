import json
from collections.abc import Mapping
from types import SimpleNamespace
from uuid import UUID

import pytest
from typer.testing import CliRunner

from cmo_agent_bridge import __version__
import cmo_agent_bridge.cli as cli_module
from cmo_agent_bridge.application.models import InvocationOutcome
from cmo_agent_bridge.application.queue_models import (
    CancelQueuedOperationResult,
    QueueSummary,
    QueueWaitResult,
    QueuedOperationReceipt,
    QueuedOperationStatus,
)
from cmo_agent_bridge.cli import app
from cmo_agent_bridge.state.operation_queue import OperationQueueState


_REQUEST_ID = UUID("00000000-0000-0000-0000-000000000123")


class _FakeQueueService:
    def __init__(self) -> None:
        self.submissions: list[tuple[str, dict[str, object]]] = []
        self.gets: list[UUID] = []
        self.waits: list[tuple[UUID, float]] = []
        self.cancels: list[UUID] = []

    @staticmethod
    def _status(state: OperationQueueState) -> QueuedOperationStatus:
        return QueuedOperationStatus(
            request_id=_REQUEST_ID,
            operation="mission.create",
            sequence=7,
            state=state,
            submitted_at_ms=100,
            completed_at_ms=(200 if state is OperationQueueState.CANCELLED else None),
        )

    def submit(
        self,
        *,
        operation: str,
        arguments: dict[str, object],
    ) -> QueuedOperationReceipt:
        self.submissions.append((operation, arguments))
        return QueuedOperationReceipt(
            request_id=_REQUEST_ID,
            operation=operation,
            sequence=7,
            state=OperationQueueState.QUEUED,
            submitted_at_ms=100,
        )

    def get(self, *, request_id: UUID) -> QueuedOperationStatus:
        self.gets.append(request_id)
        return self._status(OperationQueueState.QUEUED)

    async def wait(
        self,
        *,
        request_id: UUID,
        timeout_seconds: float,
    ) -> QueueWaitResult:
        self.waits.append((request_id, timeout_seconds))
        return QueueWaitResult(
            operation=self._status(OperationQueueState.QUEUED),
            timed_out=True,
        )

    def cancel(self, *, request_id: UUID) -> CancelQueuedOperationResult:
        self.cancels.append(request_id)
        return CancelQueuedOperationResult(
            operation=self._status(OperationQueueState.CANCELLED),
            cancelled=True,
        )

    def summary(self) -> QueueSummary:
        return QueueSummary(
            queued=1,
            active=2,
            completed=3,
            rejected=4,
            quarantined=5,
            cancelled=6,
        )


class _FakeUiCoordinationLock:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def __aenter__(self) -> None:
        self._events.append("enter")

    async def __aexit__(self, *_args: object) -> None:
        self._events.append("exit")


class _FakeQueueWorker:
    def __init__(self) -> None:
        self.start_count = 0
        self.stop_count = 0
        self.running = False

    def start(self) -> None:
        self.start_count += 1
        self.running = True

    async def stop(self) -> None:
        self.stop_count += 1
        self.running = False


class _ObservingQueueService(_FakeQueueService):
    def __init__(self, worker: _FakeQueueWorker) -> None:
        super().__init__()
        self._worker = worker
        self.worker_running_during_wait = False

    async def wait(
        self,
        *,
        request_id: UUID,
        timeout_seconds: float,
    ) -> QueueWaitResult:
        self.worker_running_during_wait = self._worker.running
        return await super().wait(
            request_id=request_id,
            timeout_seconds=timeout_seconds,
        )


class _FakeApplication:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object], str | None]] = []

    async def execute(
        self,
        operation: str,
        arguments: Mapping[str, object],
        *,
        confirmation_token: str | None = None,
    ) -> InvocationOutcome:
        self.calls.append((operation, dict(arguments), confirmation_token))
        return InvocationOutcome(
            protocol="cmo-agent-bridge/1",
            request_id=None,
            ok=True,
            result={"delegated": True},
            error=None,
        )


def _forbid_eager_build(**_kwargs: object) -> None:
    raise AssertionError("eager runtime build")


def test_version_command_reports_package_version() -> None:
    result = CliRunner().invoke(app, ["version"])

    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_serve_enters_stdio_without_eager_runtime_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[object] = []
    sentinel = object()

    def fake_manager(**_kwargs: object) -> object:
        return sentinel

    monkeypatch.setattr(cli_module, "McpRuntimeManager", fake_manager)
    monkeypatch.setattr(cli_module, "run_stdio", captured.append)
    monkeypatch.setattr(
        cli_module,
        "build_application_runtime",
        _forbid_eager_build,
    )

    result = CliRunner().invoke(app, ["serve"])

    assert result.exit_code == 0
    assert captured == [sentinel]


def test_submit_persists_typed_arguments_and_prints_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _FakeQueueService()
    build_calls: list[object] = []
    lock_events: list[str] = []

    def fake_build(**kwargs: object) -> object:
        build_calls.append(kwargs["game_root"])
        return SimpleNamespace(queue_service=queue)

    def fake_lock(_runtime: object) -> _FakeUiCoordinationLock:
        return _FakeUiCoordinationLock(lock_events)

    monkeypatch.setattr(cli_module, "build_application_runtime", fake_build)
    monkeypatch.setattr(
        cli_module,
        "new_ui_coordination_lock",
        fake_lock,
    )

    result = CliRunner().invoke(
        app,
        [
            "submit",
            "mission.create",
            "--args",
            '{"side":"Blue","name":"CAP"}',
        ],
    )

    assert result.exit_code == 0
    assert build_calls == [None]
    assert lock_events == ["enter", "exit"]
    assert queue.submissions == [("mission.create", {"side": "Blue", "name": "CAP"})]
    assert json.loads(result.stdout) == {
        "request_id": str(_REQUEST_ID),
        "operation": "mission.create",
        "sequence": 7,
        "state": "queued",
        "submitted_at_ms": 100,
    }


def test_submit_rejects_non_object_json_before_building_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli_module, "build_application_runtime", _forbid_eager_build)

    result = CliRunner().invoke(
        app,
        ["submit", "mission.create", "--args", "[]"],
    )

    assert result.exit_code == 2
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "INVALID_ARGUMENT"


def test_invoke_rejects_ordinary_cmo_mutation_before_runtime_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli_module, "build_application_runtime", _forbid_eager_build)

    result = CliRunner().invoke(
        app,
        [
            "invoke",
            "scenario.time_compression.set",
            "--args",
            '{"code":2}',
        ],
    )

    assert result.exit_code == 2
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "POLICY_DENIED"
    assert payload["error"]["details"]["operation"] == "scenario.time_compression.set"
    assert "submit" in payload["error"]["details"]["next_step"]


@pytest.mark.parametrize(
    ("operation", "arguments", "confirmation_token"),
    [
        ("bridge.status", {}, None),
        ("scenario.get", {}, None),
        ("unit.delete", {"unit_guid": "UNIT-1"}, None),
        ("unit.delete", {"unit_guid": "UNIT-1"}, "confirmation-token"),
    ],
)
def test_invoke_delegates_status_reads_and_destructive_workflows_without_wire_enrichment(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    arguments: dict[str, object],
    confirmation_token: str | None,
) -> None:
    application = _FakeApplication()

    def fake_build(**_kwargs: object) -> object:
        return SimpleNamespace(application=application)

    monkeypatch.setattr(
        cli_module,
        "build_application_runtime",
        fake_build,
    )
    command = ["invoke", operation, "--args", json.dumps(arguments)]
    if confirmation_token is not None:
        command.extend(["--confirmation-token", confirmation_token])

    result = CliRunner().invoke(app, command)

    assert result.exit_code == 0
    assert application.calls == [(operation, arguments, confirmation_token)]
    assert json.loads(result.stdout)["result"] == {"delegated": True}


def test_invoke_preserves_dynamic_read_and_mutation_routing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    application = _FakeApplication()

    def fake_build(**_kwargs: object) -> object:
        return SimpleNamespace(application=application)

    monkeypatch.setattr(
        cli_module,
        "build_application_runtime",
        fake_build,
    )
    runner = CliRunner()

    read_result = runner.invoke(
        app,
        [
            "invoke",
            "lua.call",
            "--args",
            '{"function":"ScenEdit_GetScore","arguments":{"side":"Blue"}}',
        ],
    )
    mutation_result = runner.invoke(
        app,
        [
            "invoke",
            "lua.call",
            "--args",
            '{"function":"SetScenarioTitle","arguments":{"title":"Queued"}}',
        ],
    )

    assert read_result.exit_code == 0
    assert mutation_result.exit_code == 2
    assert application.calls == [
        (
            "lua.call",
            {"function": "ScenEdit_GetScore", "arguments": {"side": "Blue"}},
            None,
        )
    ]
    assert json.loads(mutation_result.stdout)["error"]["code"] == "POLICY_DENIED"


def test_queue_cli_inspection_wait_cancel_and_summary_delegate_locally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _FakeQueueWorker()
    queue = _ObservingQueueService(worker)

    def fake_build(**_kwargs: object) -> object:
        return SimpleNamespace(queue_service=queue, queue_worker=worker)

    monkeypatch.setattr(cli_module, "build_application_runtime", fake_build)
    runner = CliRunner()

    get_result = runner.invoke(app, ["request-get", str(_REQUEST_ID)])
    wait_result = runner.invoke(
        app,
        ["request-wait", str(_REQUEST_ID), "--timeout", "0.25"],
    )
    cancel_result = runner.invoke(app, ["request-cancel", str(_REQUEST_ID)])
    summary_result = runner.invoke(app, ["queue-status"])

    assert [
        get_result.exit_code,
        wait_result.exit_code,
        cancel_result.exit_code,
        summary_result.exit_code,
    ] == [0, 0, 0, 0]
    assert queue.gets == [_REQUEST_ID]
    assert queue.waits == [(_REQUEST_ID, 0.25)]
    assert queue.cancels == [_REQUEST_ID]
    assert worker.start_count == 1
    assert worker.stop_count == 1
    assert queue.worker_running_during_wait is True
    assert worker.running is False
    assert json.loads(wait_result.stdout)["timed_out"] is True
    assert json.loads(cancel_result.stdout)["cancelled"] is True
    assert json.loads(summary_result.stdout) == {
        "queued": 1,
        "active": 2,
        "completed": 3,
        "rejected": 4,
        "quarantined": 5,
        "cancelled": 6,
    }
