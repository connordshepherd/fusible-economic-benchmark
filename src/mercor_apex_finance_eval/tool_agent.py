from __future__ import annotations

import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Any

from openai import OpenAI

from .config import AppConfig, ModelSettings
from .daytona_backend import (
    OUTPUT_DIR,
    WORKSPACE_ROOT,
    DaytonaSandboxRuntime,
    build_daytona_workspace,
    build_tool_user_prompt,
)
from .pricing import openai_cost_usd, openai_price_book_id
from .prompting import tool_agent_system_prompt
from .types import ParsedAttachment, TaskRecord
from .utils import ensure_dir, jsonable, write_json


def _supports_temperature(model_id: str) -> bool:
    lowered = model_id.lower()
    if lowered.startswith("gpt-5") or lowered.startswith("o"):
        return False
    return True


def _tool_schemas() -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "name": "list_files",
            "description": "List files and directories at a path inside the sandbox workspace.",
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
            "description": "Read a UTF-8 text file from the sandbox. Use start_line and max_lines for long files.",
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
            "description": "Write a UTF-8 text file into the sandbox workspace.",
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
            "name": "python_exec",
            "description": "Execute a short Python script inside the sandbox workspace and return stdout.",
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
        },
    ]


def _build_openai_request(model: ModelSettings, *, instructions: str, tools: list[dict[str, Any]]) -> dict[str, Any]:
    request: dict[str, Any] = {
        "model": model.model_id,
        "instructions": instructions,
        "tools": tools,
        "tool_choice": "auto",
        "parallel_tool_calls": False,
        "max_output_tokens": model.max_tokens or 65535,
    }
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
    runtime: DaytonaSandboxRuntime,
    *,
    name: str,
    arguments: dict[str, Any],
    config: AppConfig,
) -> dict[str, Any]:
    if name == "list_files":
        return {"files": runtime.list_files(arguments["path"])}
    if name == "read_file":
        return runtime.read_text_file(
            arguments["path"],
            start_line=int(arguments.get("start_line", 1) or 1),
            max_lines=min(int(arguments.get("max_lines", config.agent.max_read_lines) or config.agent.max_read_lines), 500),
            max_chars=config.agent.max_tool_output_chars,
        )
    if name == "write_file":
        return runtime.write_text_file(arguments["path"], arguments["content"])
    if name == "find_in_files":
        return {
            "matches": runtime.find_in_files(
                arguments["path"],
                arguments["pattern"],
                max_results=config.agent.max_find_results,
            )
        }
    if name == "python_exec":
        return runtime.python_exec(
            arguments["code"],
            cwd=str(arguments.get("cwd") or WORKSPACE_ROOT),
            timeout_seconds=int(arguments.get("timeout_seconds", config.agent.tool_timeout_seconds) or config.agent.tool_timeout_seconds),
            max_output_chars=config.agent.max_tool_output_chars,
        )
    raise ValueError(f"Unknown tool: {name}")


def _run_tool_assisted_generation_sync(
    *,
    task: TaskRecord,
    parsed_attachments: list[ParsedAttachment],
    config: AppConfig,
    local_artifact_dir: Path,
) -> dict[str, Any]:
    ensure_dir(local_artifact_dir)
    client = OpenAI(timeout=600.0, max_retries=2)
    runtime = DaytonaSandboxRuntime(config, sandbox_name=f"apex-task-{task.task_id}-{uuid.uuid4().hex[:8]}")
    trace_path = local_artifact_dir / "tool_trace.jsonl"
    accumulated_input_tokens = 0
    accumulated_output_tokens = 0
    accumulated_total_tokens = 0
    accumulated_cached_input_tokens = 0
    total_tool_calls = 0
    response = None
    started = time.perf_counter()

    try:
        workspace_manifest = build_daytona_workspace(
            runtime,
            task=task,
            dataset_dir=config.dataset_dir,
            parsed_attachments=parsed_attachments,
            local_artifact_dir=local_artifact_dir,
        )
        instructions = tool_agent_system_prompt()
        user_prompt = build_tool_user_prompt(task, workspace_manifest)
        tool_schemas = _tool_schemas()
        request_base = _build_openai_request(config.model, instructions=instructions, tools=tool_schemas)

        response = client.responses.create(
            input=user_prompt,
            **request_base,
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
                final_text = _response_text(response)
                break

            tool_outputs: list[dict[str, Any]] = []
            for call in function_calls:
                if total_tool_calls >= config.agent.max_tool_calls:
                    raise RuntimeError(
                        f"Exceeded max_tool_calls={config.agent.max_tool_calls} before finishing the response."
                    )
                tool_args = json.loads(call.arguments)
                tool_started = time.perf_counter()
                tool_result = _execute_tool(
                    runtime,
                    name=call.name,
                    arguments=tool_args,
                    config=config,
                )
                tool_elapsed = time.perf_counter() - tool_started
                total_tool_calls += 1

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

            response = client.responses.create(
                previous_response_id=response.id,
                input=tool_outputs,
                **request_base,
            )
            in_tokens, out_tokens, total_tokens, cached_tokens = _usage_totals(response)
            accumulated_input_tokens += in_tokens
            accumulated_output_tokens += out_tokens
            accumulated_total_tokens += total_tokens
            accumulated_cached_input_tokens += cached_tokens

        if not final_text:
            final_text = runtime.download_text_if_exists(str(OUTPUT_DIR / "final_answer.md")) or _response_text(response)
        if final_text:
            (local_artifact_dir / "final_answer.md").write_text(final_text, encoding="utf-8")

        usage_summary = {
            "input_tokens": accumulated_input_tokens,
            "cached_input_tokens": accumulated_cached_input_tokens,
            "output_tokens": accumulated_output_tokens,
            "total_tokens": accumulated_total_tokens,
            "tool_call_count": total_tool_calls,
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
            "details": {
                "generation_mode": config.generation.mode,
                "sandbox_provider": "daytona",
                "sandbox_id": runtime.sandbox_id,
                "tool_call_count": total_tool_calls,
                "tool_trace_path": str(trace_path),
                "workspace_manifest_path": str(local_artifact_dir / "workspace_manifest.json"),
                "final_answer_path": str(local_artifact_dir / "final_answer.md"),
                "usage_summary_path": str(local_artifact_dir / "usage_summary.json"),
                "price_book_id": openai_price_book_id(config.pricing.openai_price_book),
            },
        }
    finally:
        runtime.close()


async def run_tool_assisted_generation_once(
    *,
    task: TaskRecord,
    parsed_attachments: list[ParsedAttachment],
    config: AppConfig,
    local_artifact_dir: Path,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        _run_tool_assisted_generation_sync,
        task=task,
        parsed_attachments=parsed_attachments,
        config=config,
        local_artifact_dir=local_artifact_dir,
    )
