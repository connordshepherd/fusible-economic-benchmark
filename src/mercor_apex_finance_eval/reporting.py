from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

from .utils import read_jsonl, write_json


TASK_SUMMARY_HEADERS = [
    "task_id",
    "domain",
    "attempts",
    "completed_runs",
    "business_passes",
    "pass_rate",
    "mean_score_pct",
    "mean_generation_cost_per_attempt_usd",
    "mean_grading_cost_per_attempt_usd",
    "total_parse_cost_usd",
    "mean_total_cost_per_attempt_usd",
    "mean_cost_of_successful_attempts_usd",
    "cost_per_success_usd",
    "hours_estimate",
    "value_low_usd",
    "value_base_usd",
    "value_high_usd",
    "expected_net_low_usd_per_attempt",
    "expected_net_base_usd_per_attempt",
    "expected_net_high_usd_per_attempt",
    "prompt_preview",
]


def _safe_mean(values: list[float]) -> float:
    return mean(values) if values else 0.0


def summarize_records(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    by_task: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in records:
        by_task[int(row["task_id"])] += [row]

    task_rows: list[dict[str, Any]] = []
    for task_id in sorted(by_task):
        rows = by_task[task_id]
        attempts = len(rows)
        completed = [row for row in rows if row.get("status") == "completed"]
        passes = [row for row in rows if row.get("business_pass") is True]

        gen_costs = [float(row.get("generation_cost_usd", 0.0) or 0.0) for row in rows]
        grade_costs = [float(row.get("grading_cost_usd", 0.0) or 0.0) for row in rows]
        parse_cost_total = sum(float(row.get("parse_cost_incurred_usd_this_run", 0.0) or 0.0) for row in rows)
        total_cost = sum(float(row.get("total_cost_usd_this_run", 0.0) or 0.0) for row in rows)
        score_values = [float(row.get("score_pct", 0.0) or 0.0) for row in completed]
        success_costs = [float(row.get("total_cost_usd_this_run", 0.0) or 0.0) for row in passes]

        example = rows[0]
        pass_rate = len(passes) / attempts if attempts else 0.0
        mean_cost = total_cost / attempts if attempts else 0.0

        row = {
            "task_id": task_id,
            "domain": example.get("domain"),
            "attempts": attempts,
            "completed_runs": len(completed),
            "business_passes": len(passes),
            "pass_rate": round(pass_rate, 4),
            "mean_score_pct": round(_safe_mean(score_values), 4),
            "mean_generation_cost_per_attempt_usd": round(_safe_mean(gen_costs), 6),
            "mean_grading_cost_per_attempt_usd": round(_safe_mean(grade_costs), 6),
            "total_parse_cost_usd": round(parse_cost_total, 6),
            "mean_total_cost_per_attempt_usd": round(mean_cost, 6),
            "mean_cost_of_successful_attempts_usd": round(_safe_mean(success_costs), 6) if success_costs else "",
            "cost_per_success_usd": round(total_cost / len(passes), 6) if passes else "",
            "hours_estimate": example.get("hours_estimate"),
            "value_low_usd": example.get("value_low_usd"),
            "value_base_usd": example.get("value_base_usd"),
            "value_high_usd": example.get("value_high_usd"),
            "expected_net_low_usd_per_attempt": round(pass_rate * float(example.get("value_low_usd", 0.0)) - mean_cost, 6),
            "expected_net_base_usd_per_attempt": round(pass_rate * float(example.get("value_base_usd", 0.0)) - mean_cost, 6),
            "expected_net_high_usd_per_attempt": round(pass_rate * float(example.get("value_high_usd", 0.0)) - mean_cost, 6),
            "prompt_preview": example.get("prompt_preview", ""),
        }
        task_rows.append(row)

    all_attempts = sum(int(row["attempts"]) for row in task_rows)
    all_passes = sum(int(row["business_passes"]) for row in task_rows)
    total_cost_all = sum(float(row["mean_total_cost_per_attempt_usd"]) * int(row["attempts"]) for row in task_rows)

    overall = {
        "tasks_evaluated": len(task_rows),
        "total_attempts": all_attempts,
        "total_business_passes": all_passes,
        "overall_pass_rate": round(all_passes / all_attempts, 6) if all_attempts else 0.0,
        "mean_task_pass_rate": round(_safe_mean([float(row["pass_rate"]) for row in task_rows]), 6) if task_rows else 0.0,
        "mean_task_value_base_usd": round(_safe_mean([float(row["value_base_usd"]) for row in task_rows]), 6) if task_rows else 0.0,
        "mean_total_cost_per_attempt_usd": round(total_cost_all / all_attempts, 6) if all_attempts else 0.0,
        "overall_cost_per_success_usd": round(total_cost_all / all_passes, 6) if all_passes else None,
        "mean_expected_net_base_usd_per_attempt": round(
            _safe_mean([float(row["expected_net_base_usd_per_attempt"]) for row in task_rows]), 6
        ) if task_rows else 0.0,
    }
    return task_rows, overall


def write_task_summary_csv(path: str | Path, rows: list[dict[str, Any]]) -> None:
    path = Path(path)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TASK_SUMMARY_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_report_md(path: str | Path, task_rows: list[dict[str, Any]], overall: dict[str, Any]) -> None:
    path = Path(path)
    lines = [
        "# APEX Finance Eval Report",
        "",
        "## Overall",
        "",
        f"- Tasks evaluated: **{overall['tasks_evaluated']}**",
        f"- Total attempts: **{overall['total_attempts']}**",
        f"- Total business passes: **{overall['total_business_passes']}**",
        f"- Overall pass rate: **{overall['overall_pass_rate']:.4f}**",
        f"- Mean total cost per attempt: **${overall['mean_total_cost_per_attempt_usd']:.6f}**",
        "",
        "## Task summary",
        "",
        "| Task ID | Pass rate | Mean score | Mean cost/attempt | Cost/success | Value base | Expected net/base |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in task_rows:
        cost_per_success = row["cost_per_success_usd"] if row["cost_per_success_usd"] != "" else "—"
        lines.append(
            f"| {row['task_id']} | {row['pass_rate']:.4f} | {row['mean_score_pct']:.2f} | "
            f"${float(row['mean_total_cost_per_attempt_usd']):.6f} | {cost_per_success} | "
            f"${float(row['value_base_usd']):.2f} | ${float(row['expected_net_base_usd_per_attempt']):.6f} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def rebuild_outputs(run_jsonl: str | Path, output_dir: str | Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    records = read_jsonl(Path(run_jsonl))
    task_rows, overall = summarize_records(records)
    write_task_summary_csv(output_dir / "task_summary.csv", task_rows)
    write_json(output_dir / "overall_summary.json", overall)
    write_report_md(output_dir / "report.md", task_rows, overall)
    return task_rows, overall
