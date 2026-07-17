from __future__ import annotations

import asyncio
import json
from enum import StrEnum
from pathlib import Path
from typing import cast
from uuid import UUID

import typer
from pydantic import JsonValue

from cmo_agent_bridge import __version__
from cmo_agent_bridge.application.queue_models import QueueWaitResult
from cmo_agent_bridge.bootstrap import (
    POLL_ACTION_SCRIPT,
    build_application_runtime,
    prepare_bridge,
)
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.mcp_runtime import McpRuntimeManager
from cmo_agent_bridge.mcp_server import run_stdio
from cmo_agent_bridge.operations.kinds import ExecutionTarget, OperationClass
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY


app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Local CLI for the CMO agent bridge.",
)


class QuarantineDisposition(StrEnum):
    APPLIED = "applied"
    NOT_APPLIED = "not_applied"


@app.command()
def version() -> None:
    """Print the installed bridge version."""
    typer.echo(__version__)


def _error_payload(error: BridgeError) -> dict[str, object]:
    return {
        "protocol": "cmo-agent-bridge/1",
        "request_id": None,
        "ok": False,
        "result": None,
        "error": error.to_payload(),
    }


def _emit(value: object) -> None:
    typer.echo(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def _fail(error: BridgeError, *, exit_code: int) -> None:
    _emit(_error_payload(error))
    raise typer.Exit(exit_code)


def _decode_arguments(arguments_json: str) -> dict[str, JsonValue]:
    try:
        decoded: object = json.loads(arguments_json)
    except json.JSONDecodeError as error:
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "--arguments-json must be valid JSON",
            {"position": error.pos},
        ) from error
    if not isinstance(decoded, dict):
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "--arguments-json must decode to a JSON object",
        )
    decoded_object = cast(dict[object, object], decoded)
    if not all(type(key) is str for key in decoded_object):
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "--arguments-json must contain string object keys",
        )
    return cast(dict[str, JsonValue], decoded_object)


def _requires_durable_queue(operation: str, arguments: dict[str, JsonValue]) -> bool:
    """Classify queue-only mutations without resolving trusted wire enrichment."""
    contract = OPERATION_REGISTRY.resolve(operation)
    if contract.target is not ExecutionTarget.CMO:
        return False
    if contract.base_class is OperationClass.MUTATION:
        return True
    if contract.base_class is not OperationClass.DYNAMIC:
        return False
    invocation = OPERATION_REGISTRY.resolve_invocation(operation, arguments)
    return invocation.effective_class is OperationClass.MUTATION


@app.command()
def prepare(
    game_root: Path = typer.Option(
        ...,
        "--game-root",
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        help="Command - Modern Operations installation root.",
    ),
    replace_saved_game_root: bool = typer.Option(
        False,
        "--replace-saved-game-root",
        help="Replace a previously saved CMO root.",
    ),
) -> None:
    """Deploy the CMO-side Lua runtime and save the local configuration."""
    try:
        prepared = prepare_bridge(
            game_root=game_root,
            replace_saved_game_root=replace_saved_game_root,
        )
    except BridgeError as error:
        _fail(error, exit_code=2)
        return
    _emit(
        {
            "ok": True,
            "game_root": str(prepared.paths.game_root),
            "runtime_tag": prepared.runtime_snapshot.runtime_tag,
            "dispatcher_path": str(prepared.dispatcher_path),
            "inbox_path": str(prepared.paths.inbox),
            "poll_path": str(prepared.poll_path),
            "lua_action": POLL_ACTION_SCRIPT,
            "next_step": (
                "Attach lua_action to a repeatable event with a Regular Time trigger "
                "in the CMO scenario editor."
            ),
        }
    )


@app.command()
def invoke(
    operation: str = typer.Argument(help="Operation name, for example scenario.get."),
    arguments_json: str = typer.Option(
        "{}",
        "--arguments-json",
        "--args",
        help="Operation arguments as a JSON object.",
    ),
    game_root: Path | None = typer.Option(
        None,
        "--game-root",
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        help="Override the saved CMO installation root.",
    ),
    confirmation_token: str | None = typer.Option(
        None,
        "--confirmation-token",
        help="Confirmation token for a supported destructive workflow.",
    ),
) -> None:
    """Invoke one bridge operation and print a single JSON outcome."""
    try:
        decoded = _decode_arguments(arguments_json)
        if _requires_durable_queue(operation, decoded):
            raise BridgeError(
                ErrorCode.POLICY_DENIED,
                "ordinary CMO mutations use the durable queue in bridge 0.2",
                {
                    "operation": operation,
                    "next_step": (
                        "run cmo-bridge submit, then cmo-bridge request-wait "
                        "with the returned request ID"
                    ),
                },
            )
        runtime = build_application_runtime(game_root=game_root)
        outcome = asyncio.run(
            runtime.application.execute(
                operation,
                decoded,
                confirmation_token=confirmation_token,
            )
        )
    except BridgeError as error:
        _fail(error, exit_code=2)
        return
    typer.echo(outcome.model_dump_json(indent=2))
    if not outcome.ok:
        raise typer.Exit(1)


@app.command("submit")
def submit_operation(
    operation: str = typer.Argument(help="Mutation operation name, for example mission.create."),
    arguments_json: str = typer.Option(
        "{}",
        "--arguments-json",
        "--args",
        help="Operation arguments as a JSON object.",
    ),
    game_root: Path | None = typer.Option(
        None,
        "--game-root",
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        help="Override the saved CMO installation root.",
    ),
) -> None:
    """Persist a mutation and return immediately with its queue request ID."""
    try:
        decoded = _decode_arguments(arguments_json)
        runtime = build_application_runtime(game_root=game_root)
        receipt = runtime.queue_service.submit(operation=operation, arguments=decoded)
    except BridgeError as error:
        _fail(error, exit_code=2)
        return
    _emit(receipt.model_dump(mode="json"))


@app.command("request-get")
def request_get(
    request_id: UUID = typer.Argument(help="Queue request ID returned by submit."),
    game_root: Path | None = typer.Option(None, "--game-root"),
) -> None:
    """Read one durable queue request without contacting CMO."""
    try:
        runtime = build_application_runtime(game_root=game_root)
        result = runtime.queue_service.get(request_id=request_id)
    except BridgeError as error:
        _fail(error, exit_code=2)
        return
    _emit(result.model_dump(mode="json"))


@app.command("request-wait")
def request_wait(
    request_id: UUID = typer.Argument(help="Queue request ID returned by submit."),
    timeout_seconds: float = typer.Option(30.0, "--timeout", min=0),
    game_root: Path | None = typer.Option(None, "--game-root"),
) -> None:
    """Wait for a queue result; timeout never cancels the underlying request."""
    try:
        runtime = build_application_runtime(game_root=game_root)

        async def wait_with_worker() -> QueueWaitResult:
            runtime.queue_worker.start()
            try:
                return await runtime.queue_service.wait(
                    request_id=request_id,
                    timeout_seconds=timeout_seconds,
                )
            finally:
                await runtime.queue_worker.stop()

        result = asyncio.run(wait_with_worker())
    except BridgeError as error:
        _fail(error, exit_code=2)
        return
    _emit(result.model_dump(mode="json"))


@app.command("request-cancel")
def request_cancel(
    request_id: UUID = typer.Argument(help="Queue request ID returned by submit."),
    game_root: Path | None = typer.Option(None, "--game-root"),
) -> None:
    """Cancel an operation only while it is still queued and unpublished."""
    try:
        runtime = build_application_runtime(game_root=game_root)
        result = runtime.queue_service.cancel(request_id=request_id)
    except BridgeError as error:
        _fail(error, exit_code=2)
        return
    _emit(result.model_dump(mode="json"))


@app.command("queue-status")
def queue_status(
    game_root: Path | None = typer.Option(None, "--game-root"),
) -> None:
    """Show durable queue counts without contacting CMO."""
    try:
        runtime = build_application_runtime(game_root=game_root)
        result = runtime.queue_service.summary()
    except BridgeError as error:
        _fail(error, exit_code=2)
        return
    _emit(result.model_dump(mode="json"))


@app.command("resolve-quarantine")
def resolve_quarantine(
    disposition: QuarantineDisposition = typer.Option(
        ...,
        "--disposition",
        help=(
            "Your independently verified outcome for the quarantined operation: "
            "applied or not_applied."
        ),
    ),
    confirmation_token: str | None = typer.Option(
        None,
        "--confirmation-token",
        help=(
            "Token returned by the preview call. Omit it to preview the exact "
            "Host-only resolution before committing."
        ),
    ),
    game_root: Path | None = typer.Option(
        None,
        "--game-root",
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        help="Override the saved CMO installation root.",
    ),
) -> None:
    """HOST-ONLY manual quarantine resolution; never replays the CMO operation."""
    selected = disposition.value
    try:
        runtime = build_application_runtime(game_root=game_root)
        if confirmation_token is None:
            result = asyncio.run(runtime.host_quarantine.preview(selected))
        else:
            result = asyncio.run(
                runtime.host_quarantine.confirm(
                    selected,
                    confirmation_token,
                )
            )
    except BridgeError as error:
        _fail(error, exit_code=1)
        return
    _emit(result.model_dump(mode="json", warnings="error"))


@app.command()
def serve(
    game_root: Path | None = typer.Option(
        None,
        "--game-root",
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        help="Override the saved CMO installation root.",
    ),
) -> None:
    """Serve CMO tools over stdio MCP, including host-side setup recovery."""
    run_stdio(McpRuntimeManager(game_root=game_root))


if __name__ == "__main__":
    app()
