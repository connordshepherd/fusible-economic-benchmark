from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _ordered_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        item = str(value).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _coerce_tools(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return _ordered_unique([str(item) for item in value])
    if isinstance(value, tuple):
        return _ordered_unique([str(item) for item in value])

    raw = str(value).strip()
    if not raw:
        return []
    if raw.startswith("[") and raw.endswith("]"):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return _ordered_unique([str(item) for item in parsed])
    if ";" in raw:
        return _ordered_unique([part.strip() for part in raw.split(";")])
    return [raw]


def infer_generation_steps_used(record: dict[str, Any]) -> int | None:
    direct = record.get("generation_steps_used")
    if direct not in (None, ""):
        return int(direct or 0)

    details = record.get("generation_details") or {}
    detail_value = details.get("steps_used")
    if detail_value not in (None, ""):
        return int(detail_value or 0)

    runtime_trace_path = details.get("runtime_trace_path")
    if not runtime_trace_path:
        return None

    path = Path(runtime_trace_path)
    if not path.exists():
        return None

    max_step = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        step = payload.get("step")
        if step in (None, ""):
            continue
        max_step = max(max_step, int(step))
    return max_step if max_step > 0 else None


def infer_tools_used(record: dict[str, Any]) -> list[str]:
    direct = _coerce_tools(record.get("tools_used"))
    if direct:
        return direct

    details = record.get("generation_details") or {}
    detail_tools = _coerce_tools(details.get("tools_used"))
    if detail_tools:
        return detail_tools

    tool_trace_path = details.get("tool_trace_path")
    if tool_trace_path:
        path = Path(tool_trace_path)
        if path.exists():
            tools: list[str] = []
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                name = payload.get("tool_name")
                if name not in (None, ""):
                    tools.append(str(name))
            tools = _ordered_unique(tools)
            if tools:
                return tools

    runtime_trace_path = details.get("runtime_trace_path")
    if runtime_trace_path:
        path = Path(runtime_trace_path)
        if path.exists():
            tools = []
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("event") not in {"tool_call_start", "tool_call_end", "tool_call_error"}:
                    continue
                name = payload.get("tool_name")
                if name not in (None, ""):
                    tools.append(str(name))
            return _ordered_unique(tools)

    return []


def union_tools(rows: list[dict[str, Any]]) -> list[str]:
    tools: list[str] = []
    for row in rows:
        tools.extend(infer_tools_used(row))
    return _ordered_unique(tools)


def tools_used_text(tools: list[str]) -> str:
    return "; ".join(_ordered_unique(tools))
