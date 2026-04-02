from __future__ import annotations

import asyncio
import csv
import json
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

from .config import AppConfig
from .tool_agent import run_tool_assisted_generation_once
from .types import ParsedAttachment, TaskRecord
from .utils import ensure_dir, read_jsonl, write_json


SMOKE_TASK_ID = 990001
SMOKE_DOMAIN = "Synthetic"
SMOKE_FILENAME = "python_exec_smoke_input.csv"

_DESKS = ("Rates", "Credit", "Macro", "SpecialSituations", "PrivateCapital")
_REGIONS = ("west", "central", "east", "south")
_CATEGORIES = ("priority-A", "priority-B", "review-C", "review-D", "archive-Z")


@dataclass(slots=True)
class PythonExecSmokeFixture:
    dataset_dir: Path
    csv_path: Path
    artifact_dir: Path
    task: TaskRecord
    parsed_attachments: list[ParsedAttachment]
    expected_result: dict[str, Any]


def _smoke_prompt() -> str:
    return (
        "This is a synthetic Python-execution smoke test.\n\n"
        "Your job is to read the raw CSV attachment and compute an exact JSON result. "
        "You must use python_exec at least once. The parsed attachment text is only a short preview and is not sufficient "
        "to solve the task. If you do not use python_exec, the task should be treated as failed.\n\n"
        "Use the raw CSV attachment named python_exec_smoke_input.csv. Select rows where all of the following are true:\n"
        "- is_active == 1\n"
        "- region is west or central\n"
        "- risk_score >= 73\n"
        "- category ends with -A or -C\n\n"
        "Then compute this exact JSON object with these keys:\n"
        '- "selected_row_count": integer\n'
        '- "selected_amount_cents": integer\n'
        '- "selected_fee_cents": integer, computed as sum(amount_cents * fee_bps // 10000) over selected rows\n'
        '- "selected_rows_sha256": lowercase hex SHA256 of the UTF-8 text formed by joining one line per selected row in source order using:\n'
        "  row_id|region|amount_cents|fee_bps|category\n"
        "  Join those lines with a single \\n character and do not add a trailing newline.\n\n"
        "Write the exact JSON object to /workspace/output/final_answer.md and also return the exact same JSON as your final answer. "
        "Return JSON only, with no Markdown fences or explanation."
    )


def _build_row(index: int) -> dict[str, Any]:
    return {
        "row_id": f"TX-{index:05d}",
        "desk": _DESKS[index % len(_DESKS)],
        "region": _REGIONS[(index * 3) % len(_REGIONS)],
        "amount_cents": 10_000 + ((index * 9_973) % 990_000),
        "risk_score": (index * 17) % 100,
        "fee_bps": 5 + ((index * 7) % 145),
        "is_active": 1 if (index % 4) != 0 else 0,
        "category": _CATEGORIES[(index * 5 + 2) % len(_CATEGORIES)],
    }


def _matches(row: dict[str, Any]) -> bool:
    return (
        int(row["is_active"]) == 1
        and str(row["region"]) in {"west", "central"}
        and int(row["risk_score"]) >= 73
        and (str(row["category"]).endswith("-A") or str(row["category"]).endswith("-C"))
    )


def _expected_result(rows: list[dict[str, Any]]) -> dict[str, Any]:
    selected = [row for row in rows if _matches(row)]
    digest_source = "\n".join(
        f"{row['row_id']}|{row['region']}|{row['amount_cents']}|{row['fee_bps']}|{row['category']}"
        for row in selected
    )
    return {
        "selected_row_count": len(selected),
        "selected_amount_cents": sum(int(row["amount_cents"]) for row in selected),
        "selected_fee_cents": sum((int(row["amount_cents"]) * int(row["fee_bps"])) // 10_000 for row in selected),
        "selected_rows_sha256": sha256(digest_source.encode("utf-8")).hexdigest(),
    }


def _preview_text(rows: list[dict[str, Any]], *, omitted_count: int) -> str:
    preview_rows = rows[:12]
    header = (
        "This parsed attachment is only a preview. Use the raw CSV attachment for the exact calculation.\n\n"
        "Preview rows:\n"
    )
    body = "\n".join(
        f"{row['row_id']},{row['desk']},{row['region']},{row['amount_cents']},{row['risk_score']},{row['fee_bps']},{row['is_active']},{row['category']}"
        for row in preview_rows
    )
    footer = f"\n\nRemaining rows omitted from preview: {omitted_count}\n"
    return header + body + footer


def create_python_exec_smoke_fixture(output_dir: Path, *, row_count: int) -> PythonExecSmokeFixture:
    output_dir = ensure_dir(output_dir)
    dataset_dir = ensure_dir(output_dir / "synthetic_dataset")
    artifact_dir = ensure_dir(output_dir / "generation_artifacts" / f"task_{SMOKE_TASK_ID}" / "run_1")
    docs_dir = ensure_dir(dataset_dir / "documents" / str(SMOKE_TASK_ID))
    csv_path = docs_dir / SMOKE_FILENAME

    rows = [_build_row(index) for index in range(1, row_count + 1)]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "row_id",
                "desk",
                "region",
                "amount_cents",
                "risk_score",
                "fee_bps",
                "is_active",
                "category",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    fixture = PythonExecSmokeFixture(
        dataset_dir=dataset_dir,
        csv_path=csv_path,
        artifact_dir=artifact_dir,
        task=TaskRecord(
            task_id=SMOKE_TASK_ID,
            domain=SMOKE_DOMAIN,
            prompt=_smoke_prompt(),
            rubric_json=json.dumps({"synthetic": True}),
            attachment_paths=[f"documents/{SMOKE_TASK_ID}/{SMOKE_FILENAME}"],
        ),
        parsed_attachments=[
            ParsedAttachment(
                filename=SMOKE_FILENAME,
                relative_path=str(csv_path.resolve()),
                content=_preview_text(rows, omitted_count=max(row_count - 12, 0)),
                cache_hit=True,
                num_pages=1,
                credits_incurred=0.0,
                cost_incurred_usd=0.0,
            )
        ],
        expected_result=_expected_result(rows),
    )
    write_json(
        output_dir / "smoke_fixture.json",
        {
            "task_id": fixture.task.task_id,
            "csv_path": fixture.csv_path,
            "row_count": row_count,
            "expected_result": fixture.expected_result,
            "artifact_dir": fixture.artifact_dir,
        },
    )
    return fixture


def _extract_json_object(text: str) -> dict[str, Any] | None:
    stripped = (text or "").strip()
    if not stripped:
        return None
    candidates: list[str] = [stripped]
    candidates.extend(re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", stripped, flags=re.DOTALL))

    first_brace = stripped.find("{")
    last_brace = stripped.rfind("}")
    if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
        candidates.append(stripped[first_brace : last_brace + 1])

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def summarize_python_exec_smoke(
    *,
    output_dir: Path,
    expected_result: dict[str, Any],
    generation_result: dict[str, Any],
) -> dict[str, Any]:
    details = generation_result.get("details", {}) if isinstance(generation_result, dict) else {}
    usage_path = Path(details["usage_summary_path"]) if details.get("usage_summary_path") else None
    tool_trace_path = Path(details["tool_trace_path"]) if details.get("tool_trace_path") else None
    runtime_trace_path = Path(details["runtime_trace_path"]) if details.get("runtime_trace_path") else None

    usage_summary = json.loads(usage_path.read_text(encoding="utf-8")) if usage_path and usage_path.exists() else {}
    tool_rows = read_jsonl(tool_trace_path) if tool_trace_path and tool_trace_path.exists() else []
    runtime_rows = read_jsonl(runtime_trace_path) if runtime_trace_path and runtime_trace_path.exists() else []

    final_answer = str(generation_result.get("response") or "").strip()
    parsed_final = _extract_json_object(final_answer)

    python_exec_rows = [row for row in tool_rows if row.get("tool_name") == "python_exec"]
    python_exec_successes = [
        row
        for row in python_exec_rows
        if isinstance(row.get("result"), dict) and int(
            row["result"]["exit_code"] if row["result"].get("exit_code") is not None else 1
        )
        == 0
    ]

    summary = {
        "task_id": SMOKE_TASK_ID,
        "smoke_passed": bool(parsed_final == expected_result and python_exec_successes),
        "generation_success": bool(generation_result.get("success")),
        "exact_match": parsed_final == expected_result,
        "python_exec_called": len(python_exec_rows) > 0,
        "python_exec_success": len(python_exec_successes) > 0,
        "python_exec_call_count": int(usage_summary.get("python_exec_call_count", 0) or 0),
        "tool_call_count": int(usage_summary.get("tool_call_count", 0) or 0),
        "steps_used": int(usage_summary.get("steps_used", 0) or 0),
        "tools_used": list(usage_summary.get("tools_used", []) or []),
        "expected_result": expected_result,
        "parsed_final_answer": parsed_final,
        "final_answer": final_answer,
        "generation_cost_usd": generation_result.get("total_cost"),
        "input_tokens": generation_result.get("input_tokens"),
        "cached_input_tokens": generation_result.get("cached_input_tokens"),
        "output_tokens": generation_result.get("output_tokens"),
        "total_tokens": generation_result.get("total_tokens"),
        "error_message": generation_result.get("error_message", ""),
        "sandbox_used": bool(details.get("sandbox_used")),
        "sandbox_id": details.get("sandbox_id", ""),
        "python_exec_tool_rows": python_exec_rows,
        "runtime_events": [row for row in runtime_rows if "daytona" in str(row.get("event", ""))],
    }
    write_json(output_dir / "smoke_summary.json", summary)
    return summary


def run_python_exec_smoke(config: AppConfig, *, output_dir: Path, row_count: int) -> dict[str, Any]:
    fixture = create_python_exec_smoke_fixture(output_dir, row_count=row_count)
    config.output_dir = output_dir.resolve()
    config.dataset_dir = fixture.dataset_dir.resolve()

    generation_result = asyncio.run(
        run_tool_assisted_generation_once(
            task=fixture.task,
            parsed_attachments=fixture.parsed_attachments,
            config=config,
            local_artifact_dir=fixture.artifact_dir,
        )
    )

    payload = {
        "generation_result": generation_result,
        "smoke_summary": summarize_python_exec_smoke(
            output_dir=output_dir,
            expected_result=fixture.expected_result,
            generation_result=generation_result,
        ),
    }
    write_json(output_dir / "smoke_result.json", payload)
    return payload
