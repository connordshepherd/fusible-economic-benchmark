from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from openai import OpenAI

from .config import AppConfig, ModelSettings
from .daytona_backend import (
    DaytonaPythonExecutor,
    LocalWorkspaceRuntime,
    build_local_workspace,
    build_tool_user_prompt,
)
from .pricing import openai_cost_usd, openai_price_book_id
from .prompting import tool_agent_system_prompt
from .types import ParsedAttachment, TaskRecord
from .utils import append_jsonl, ensure_dir, jsonable, utc_now_iso, write_json


def _supports_temperature(model_id: str) -> bool:
    lowered = model_id.lower()
    if lowered.startswith("gpt-5") or lowered.startswith("o"):
        return False
    return True


def _tool_schemas(*, enable_python_exec: bool) -> list[dict[str, Any]]:
    tools: list[dict[str, Any]] = [
        {
            "type": "function",
            "name": "list_files",
            "description": "List files and directories at a path inside the local workspace.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                },
                "required": ["path"],
                "additionalProperties": False,
            },
        },
        {
            "type": "function",
            "name": "read_file",
            "description": "Read a UTF-8 text file from the local workspace. Use start_line and max_lines for long files.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "start_line": {"type": "integer", "minimum": 1},
                    "max_lines": {"type": "integer", "minimum": 1, "maximum": 500},
                },
                "required": ["path"],
                "additionalProperties": False,
            },
        },
        {
            "type": "function",
            "name": "write_file",
            "description": "Write a UTF-8 text file into the local workspace.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"},
                },
                "required": ["path", "content"],
                "additionalProperties": False,
            },
        },
        {
            "type": "function",
            "name": "find_in_files",
            "description": "Search file contents recursively for a text pattern.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "pattern": {"type": "string"},
                },
                "required": ["path", "pattern"],
                "additionalProperties": False,
            },
        },
        {
            "type": "function",
            "name": "read_best_matches",
            "description": "Retrieve the best matching text windows for a natural-language query, especially useful on long legal or technical files.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "query": {"type": "string"},
                    "max_results": {"type": "integer", "minimum": 1, "maximum": 10},
                    "context_lines": {"type": "integer", "minimum": 1, "maximum": 80},
                },
                "required": ["path", "query"],
                "additionalProperties": False,
            },
        },
    ]
    if enable_python_exec:
        tools.append(
            {
            "type": "function",
            "name": "python_exec",
            "description": "Execute a short Python script in an isolated Daytona sandbox using the current workspace contents.",
            "parameters": {
                "type": "object",
                "properties": {
                    "code": {"type": "string"},
                    "cwd": {"type": "string"},
                    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 300},
                },
                "required": ["code"],
                "additionalProperties": False,
            },
            }
        )
    return tools


def _build_openai_request(model: ModelSettings, *, instructions: str, tools: list[dict[str, Any]]) -> dict[str, Any]:
    request: dict[str, Any] = {
        "model": model.model_id,
        "instructions": instructions,
        "max_output_tokens": model.max_tokens or 65535,
    }
    if tools:
        request["tools"] = tools
        request["tool_choice"] = "auto"
        request["parallel_tool_calls"] = False
    if model.temperature is not None and _supports_temperature(model.model_id):
        request["temperature"] = model.temperature

    reasoning_effort = model.model_configs.get("reasoning_effort") if model.model_configs else None
    if reasoning_effort:
        request["reasoning"] = {"effort": reasoning_effort}

    verbosity = model.model_configs.get("verbosity") if model.model_configs else None
    if verbosity:
        request["text"] = {"verbosity": verbosity}

    return request


def _response_text(response: Any) -> str:
    texts: list[str] = []
    for item in getattr(response, "output", []) or []:
        if getattr(item, "type", None) != "message":
            continue
        for content in getattr(item, "content", []) or []:
            if getattr(content, "type", None) == "output_text":
                texts.append(getattr(content, "text", ""))
    return "\n".join(texts).strip()


def _response_function_calls(response: Any) -> list[Any]:
    return [item for item in (getattr(response, "output", []) or []) if getattr(item, "type", None) == "function_call"]


def _usage_totals(response: Any) -> tuple[int, int, int, int]:
    usage = getattr(response, "usage", None)
    if not usage:
        return (0, 0, 0, 0)
    cached_tokens = 0
    details = getattr(usage, "input_tokens_details", None)
    if details:
        cached_tokens = int(getattr(details, "cached_tokens", 0) or 0)
    return (
        int(getattr(usage, "input_tokens", 0) or 0),
        int(getattr(usage, "output_tokens", 0) or 0),
        int(getattr(usage, "total_tokens", 0) or 0),
        cached_tokens,
    )


def _tool_result_to_output(result: dict[str, Any], *, max_chars: int) -> str:
    payload = json.dumps(jsonable(result), ensure_ascii=False)
    if len(payload) > max_chars:
        payload = payload[:max_chars]
    return payload


def _execute_tool(
    workspace: LocalWorkspaceRuntime,
    python_executor: DaytonaPythonExecutor,
    *,
    name: str,
    arguments: dict[str, Any],
    config: AppConfig,
) -> dict[str, Any]:
    if name == "list_files":
        return {"files": workspace.list_files(arguments["path"])}
    if name == "read_file":
        return workspace.read_text_file(
            arguments["path"],
            start_line=int(arguments.get("start_line", 1) or 1),
            max_lines=min(int(arguments.get("max_lines", config.agent.max_read_lines) or config.agent.max_read_lines), 500),
            max_chars=config.agent.max_tool_output_chars,
        )
    if name == "write_file":
        return workspace.write_text_file(arguments["path"], arguments["content"])
    if name == "find_in_files":
        return {
            "matches": workspace.find_in_files(
                arguments["path"],
                arguments["pattern"],
                max_results=config.agent.max_find_results,
            )
        }
    if name == "read_best_matches":
        return {
            "matches": workspace.read_best_matches(
                arguments["path"],
                arguments["query"],
                max_results=min(int(arguments.get("max_results", 5) or 5), 10),
                context_lines=min(int(arguments.get("context_lines", 12) or 12), 80),
                max_chars=max(1000, config.agent.max_tool_output_chars // 3),
            )
        }
    if name == "python_exec":
        return python_executor.python_exec(
            workspace,
            arguments["code"],
            cwd=str(arguments.get("cwd") or workspace.virtual_root_str()),
            timeout_seconds=int(arguments.get("timeout_seconds", config.agent.tool_timeout_seconds) or config.agent.tool_timeout_seconds),
            max_output_chars=config.agent.max_tool_output_chars,
        )
    raise ValueError(f"Unknown tool: {name}")


def _emit_runtime_trace(trace_path: Path, event: str, **payload: Any) -> None:
    append_jsonl(
        trace_path,
        {
            "event": event,
            "at": utc_now_iso(),
            **jsonable(payload),
        },
    )


def _run_tool_assisted_generation_sync(
    *,
    task: TaskRecord,
    parsed_attachments: list[ParsedAttachment],
    config: AppConfig,
    local_artifact_dir: Path,
) -> dict[str, Any]:
    ensure_dir(local_artifact_dir)
    client = OpenAI(timeout=600.0, max_retries=2)
    trace_path = local_artifact_dir / "tool_trace.jsonl"
    runtime_trace_path = local_artifact_dir / "runtime_trace.jsonl"
    workspace = LocalWorkspaceRuntime(local_artifact_dir / "workspace")
    python_executor = DaytonaPythonExecutor(
        config,
        trace=lambda event, payload: _emit_runtime_trace(runtime_trace_path, event, **payload),
    )
    accumulated_input_tokens = 0
    accumulated_output_tokens = 0
    accumulated_total_tokens = 0
    accumulated_cached_input_tokens = 0
    total_tool_calls = 0
    python_exec_calls = 0
    steps_used = 0
    tools_used: list[str] = []
    seen_tools: set[str] = set()
    response = None
    started = time.perf_counter()
    exhausted_step_budget = False

    try:
        _emit_runtime_trace(runtime_trace_path, "generation_start", task_id=task.task_id, local_artifact_dir=str(local_artifact_dir))
        workspace_manifest = build_local_workspace(
            workspace,
            task=task,
            dataset_dir=config.dataset_dir,
            parsed_attachments=parsed_attachments,
            local_artifact_dir=local_artifact_dir,
            trace=lambda event, payload: _emit_runtime_trace(runtime_trace_path, event, **payload),
        )
        instructions = tool_agent_system_prompt()
        user_prompt = build_tool_user_prompt(task, workspace_manifest)
        tool_schemas = _tool_schemas(enable_python_exec=config.agent.enable_python_exec)
        request_base = _build_openai_request(config.model, instructions=instructions, tools=tool_schemas)

        initial_started = time.perf_counter()
        _emit_runtime_trace(runtime_trace_path, "openai_initial_response_start", model=config.model.model_id)
        response = client.responses.create(
            input=user_prompt,
            **request_base,
        )
        _emit_runtime_trace(
            runtime_trace_path,
            "openai_initial_response_end",
            model=config.model.model_id,
            duration_seconds=time.perf_counter() - initial_started,
            response_id=getattr(response, "id", ""),
        )
        in_tokens, out_tokens, total_tokens, cached_tokens = _usage_totals(response)
        accumulated_input_tokens += in_tokens
        accumulated_output_tokens += out_tokens
        accumulated_total_tokens += total_tokens
        accumulated_cached_input_tokens += cached_tokens

        final_text = ""
        for step in range(1, config.agent.max_steps + 1):
            function_calls = _response_function_calls(response)
            if not function_calls:
                steps_used = max(steps_used, step)
                final_text = _response_text(response)
                break

            tool_outputs: list[dict[str, Any]] = []
            for call in function_calls:
                if total_tool_calls >= config.agent.max_tool_calls:
                    raise RuntimeError(
                        f"Exceeded max_tool_calls={config.agent.max_tool_calls} before finishing the response."
                    )
                tool_args = json.loads(call.arguments)
                _emit_runtime_trace(
                    runtime_trace_path,
                    "tool_call_start",
                    step=step,
                    tool_name=call.name,
                    arguments=tool_args,
                )
                tool_started = time.perf_counter()
                if call.name not in seen_tools:
                    seen_tools.add(call.name)
                    tools_used.append(call.name)
                tool_error: dict[str, Any] | None = None
                try:
                    tool_result = _execute_tool(
                        workspace,
                        python_executor,
                        name=call.name,
                        arguments=tool_args,
                        config=config,
                    )
                except Exception as exc:
                    tool_result = {
                        "ok": False,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    }
                    tool_error = {
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    }
                tool_elapsed = time.perf_counter() - tool_started
                total_tool_calls += 1
                if call.name == "python_exec":
                    python_exec_calls += 1
                if tool_error is None:
                    _emit_runtime_trace(
                        runtime_trace_path,
                        "tool_call_end",
                        step=step,
                        tool_name=call.name,
                        duration_seconds=tool_elapsed,
                    )
                else:
                    _emit_runtime_trace(
                        runtime_trace_path,
                        "tool_call_error",
                        step=step,
                        tool_name=call.name,
                        duration_seconds=tool_elapsed,
                        **tool_error,
                    )

                with trace_path.open("a", encoding="utf-8") as handle:
                    handle.write(
                        json.dumps(
                            {
                                "step": step,
                                "call_id": call.call_id,
                                "tool_name": call.name,
                                "arguments": tool_args,
                                "result": tool_result,
                                "duration_seconds": tool_elapsed,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

                tool_outputs.append(
                    {
                        "type": "function_call_output",
                        "call_id": call.call_id,
                        "output": _tool_result_to_output(tool_result, max_chars=config.agent.max_tool_output_chars),
                    }
                )

            followup_started = time.perf_counter()
            _emit_runtime_trace(runtime_trace_path, "openai_followup_response_start", step=step, model=config.model.model_id)
            response = client.responses.create(
                previous_response_id=response.id,
                input=tool_outputs,
                **request_base,
            )
            _emit_runtime_trace(
                runtime_trace_path,
                "openai_followup_response_end",
                step=step,
                model=config.model.model_id,
                duration_seconds=time.perf_counter() - followup_started,
                response_id=getattr(response, "id", ""),
            )
            in_tokens, out_tokens, total_tokens, cached_tokens = _usage_totals(response)
            accumulated_input_tokens += in_tokens
            accumulated_output_tokens += out_tokens
            accumulated_total_tokens += total_tokens
            accumulated_cached_input_tokens += cached_tokens
            steps_used = max(steps_used, step)
        else:
            exhausted_step_budget = True

        if not final_text:
            final_text = workspace.final_answer_text() or _response_text(response)

        if not final_text and exhausted_step_budget and response is not None:
            forced_final_started = time.perf_counter()
            _emit_runtime_trace(
                runtime_trace_path,
                "openai_forced_final_response_start",
                step=config.agent.max_steps,
                model=config.model.model_id,
            )
            forced_final_response = client.responses.create(
                previous_response_id=response.id,
                input=(
                    "You have exhausted your tool budget. Do not call any more tools. "
                    "Using only the information already gathered, provide the best final answer now."
                ),
                **_build_openai_request(config.model, instructions=instructions, tools=[]),
            )
            _emit_runtime_trace(
                runtime_trace_path,
                "openai_forced_final_response_end",
                step=config.agent.max_steps,
                model=config.model.model_id,
                duration_seconds=time.perf_counter() - forced_final_started,
                response_id=getattr(forced_final_response, "id", ""),
            )
            response = forced_final_response
            in_tokens, out_tokens, total_tokens, cached_tokens = _usage_totals(response)
            accumulated_input_tokens += in_tokens
            accumulated_output_tokens += out_tokens
            accumulated_total_tokens += total_tokens
            accumulated_cached_input_tokens += cached_tokens
            final_text = workspace.final_answer_text() or _response_text(response)

        if final_text:
            (local_artifact_dir / "final_answer.md").write_text(final_text, encoding="utf-8")

        usage_summary = {
            "input_tokens": accumulated_input_tokens,
            "cached_input_tokens": accumulated_cached_input_tokens,
            "output_tokens": accumulated_output_tokens,
            "total_tokens": accumulated_total_tokens,
            "tool_call_count": total_tool_calls,
            "python_exec_call_count": python_exec_calls,
            "steps_used": steps_used,
            "tools_used": tools_used,
        }
        write_json(local_artifact_dir / "usage_summary.json", usage_summary)

        total_cost = openai_cost_usd(
            config.pricing.openai_price_book,
            model_id=config.model.model_id,
            input_tokens=accumulated_input_tokens,
            cached_input_tokens=accumulated_cached_input_tokens,
            output_tokens=accumulated_output_tokens,
        ) or 0.0
        return {
            "success": bool(final_text),
            "response": final_text,
            "raw_response": json.dumps(jsonable(response), ensure_ascii=False),
            "input_tokens": accumulated_input_tokens,
            "cached_input_tokens": accumulated_cached_input_tokens,
            "output_tokens": accumulated_output_tokens,
            "total_tokens": accumulated_total_tokens,
            "total_cost": total_cost,
            "api_provider": "openai",
            "execution_time_seconds": time.perf_counter() - started,
            "error_message": "" if final_text else "Tool-assisted generation did not return a final answer.",
            "started_at": None,
            "completed_at": None,
            "generation_steps_used": steps_used,
            "tools_used": tools_used,
            "details": {
                "generation_mode": config.generation.mode,
                "workspace_provider": "local",
                "workspace_root": str(workspace.local_root),
                "sandbox_provider": "daytona" if python_executor.sandbox_id else "",
                "sandbox_id": python_executor.sandbox_id or "",
                "sandbox_used": bool(python_executor.sandbox_id),
                "steps_used": steps_used,
                "tool_call_count": total_tool_calls,
                "python_exec_call_count": python_exec_calls,
                "tools_used": tools_used,
                "tool_trace_path": str(trace_path),
                "runtime_trace_path": str(runtime_trace_path),
                "workspace_manifest_path": str(local_artifact_dir / "workspace_manifest.json"),
                "final_answer_path": str(local_artifact_dir / "final_answer.md"),
                "usage_summary_path": str(local_artifact_dir / "usage_summary.json"),
                "price_book_id": openai_price_book_id(config.pricing.openai_price_book),
            },
        }
    finally:
        _emit_runtime_trace(runtime_trace_path, "generation_end", task_id=task.task_id, duration_seconds=time.perf_counter() - started)
        python_executor.close()


async def run_tool_assisted_generation_once(
    *,
    task: TaskRecord,
    parsed_attachments: list[ParsedAttachment],
    config: AppConfig,
    local_artifact_dir: Path,
) -> dict[str, Any]:
    return _run_tool_assisted_generation_sync(
        task=task,
        parsed_attachments=parsed_attachments,
        config=config,
        local_artifact_dir=local_artifact_dir,
    )
