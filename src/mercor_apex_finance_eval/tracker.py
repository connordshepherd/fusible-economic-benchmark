from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

from .pricing import openai_cost_usd, openai_price_book_id
from .utils import ensure_dir, read_jsonl, utc_now_iso, write_json

PROMOTION_HEADERS = [
    "attempt_key",
    "promoted_at",
    "run_jsonl_path",
    "output_dir",
    "task_id",
    "run_index",
    "label",
    "notes",
    "headline",
]

DISCOVERED_ATTEMPT_HEADERS = [
    "attempt_key",
    "promoted",
    "headline",
    "promotion_label",
    "promotion_notes",
    "promoted_at",
    "run_jsonl_path",
    "output_dir",
    "run_manifest_path",
    "task_id",
    "run_index",
    "domain",
    "status",
    "business_pass",
    "score_pct",
    "model_id",
    "generation_reasoning_effort",
    "generation_verbosity",
    "judge_model_id",
    "judge_reasoning_effort",
    "judge_verbosity",
    "generation_mode",
    "agent_budget",
    "generation_prompt_fingerprint",
    "grading_prompt_fingerprint",
    "setup_id",
    "price_book_id_current",
    "value_base_usd",
    "hours_estimate",
    "prompt_preview",
    "generation_input_tokens",
    "generation_cached_input_tokens",
    "generation_output_tokens",
    "grading_input_tokens",
    "grading_cached_input_tokens",
    "grading_output_tokens",
    "current_generation_cost_usd",
    "current_grading_cost_usd",
    "current_total_cost_usd",
    "recorded_total_cost_usd",
    "attempt_completed_at",
]

PROMOTED_ATTEMPT_HEADERS = list(DISCOVERED_ATTEMPT_HEADERS)

MASTER_TRACKER_HEADERS = [
    "task_id",
    "domain",
    "prompt_preview",
    "setup_id",
    "generation_mode",
    "model_id",
    "generation_reasoning_effort",
    "generation_verbosity",
    "judge_model_id",
    "judge_reasoning_effort",
    "judge_verbosity",
    "generation_prompt_fingerprint",
    "grading_prompt_fingerprint",
    "agent_budget",
    "price_book_id_current",
    "promoted_attempts",
    "completed_runs",
    "business_passes",
    "pass_rate",
    "mean_score_pct",
    "mean_generation_cost_per_attempt_usd",
    "mean_grading_cost_per_attempt_usd",
    "mean_total_cost_per_attempt_usd",
    "mean_cost_of_successful_attempts_usd",
    "cost_per_success_usd",
    "hours_estimate",
    "value_base_usd",
    "expected_net_base_usd_per_attempt",
    "latest_attempt_completed_at",
    "promotion_labels",
]


def _write_csv(path: Path, headers: list[str], rows: list[dict[str, Any]]) -> Path:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def _safe_mean(values: list[float]) -> float:
    return mean(values) if values else 0.0


def _attempt_key(run_jsonl_path: Path, task_id: int, run_index: int) -> str:
    return f"{run_jsonl_path.resolve()}::{task_id}::{run_index}"


def _read_manifest(run_jsonl_path: Path) -> dict[str, Any]:
    manifest_path = run_jsonl_path.parent / "run_manifest.json"
    if not manifest_path.exists():
        return {}
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _load_promotions(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    rows: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = str(row.get("attempt_key") or "").strip()
            if not key:
                continue
            rows[key] = row
    return rows


def _write_promotions(path: Path, rows: list[dict[str, Any]]) -> Path:
    ordered = sorted(rows, key=lambda row: (str(row.get("promoted_at", "")), str(row.get("attempt_key", ""))))
    return _write_csv(path, PROMOTION_HEADERS, ordered)


def _infer_generation_cached_input_tokens(record: dict[str, Any]) -> int:
    cached = record.get("generation_cached_input_tokens")
    if cached not in (None, ""):
        return int(cached or 0)

    details = record.get("generation_details") or {}
    usage_path = details.get("usage_summary_path")
    if usage_path:
        path = Path(usage_path)
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            return int(payload.get("cached_input_tokens", 0) or 0)
    return 0


def _infer_grading_token_fields(record: dict[str, Any]) -> tuple[int, int, int]:
    input_tokens = record.get("grading_input_tokens")
    cached_input_tokens = record.get("grading_cached_input_tokens")
    output_tokens = record.get("grading_output_tokens")
    if input_tokens not in (None, "") or cached_input_tokens not in (None, "") or output_tokens not in (None, ""):
        return (
            int(input_tokens or 0),
            int(cached_input_tokens or 0),
            int(output_tokens or 0),
        )

    criteria_results = record.get("criteria_results") or []
    return (
        sum(int(item.get("input_tokens", 0) or 0) for item in criteria_results),
        sum(int(item.get("cached_input_tokens", 0) or 0) for item in criteria_results),
        sum(int(item.get("output_tokens", 0) or 0) for item in criteria_results),
    )


def _current_cost_or_recorded(
    *,
    model_id: str,
    input_tokens: int,
    cached_input_tokens: int,
    output_tokens: int,
    recorded_cost: float,
    price_book_path: Path,
) -> float:
    rerated = openai_cost_usd(
        price_book_path,
        model_id=model_id,
        input_tokens=input_tokens,
        cached_input_tokens=cached_input_tokens,
        output_tokens=output_tokens,
    )
    if rerated is None:
        return float(recorded_cost or 0.0)
    return float(rerated)


def _setup_id(
    *,
    generation_mode: str,
    model_id: str,
    generation_reasoning_effort: str,
    judge_model_id: str,
    judge_reasoning_effort: str,
    generation_prompt_fingerprint: str,
    grading_prompt_fingerprint: str,
    agent_budget: str,
) -> str:
    return "|".join(
        [
            generation_mode or "unknown",
            model_id or "unknown",
            f"gen_reasoning={generation_reasoning_effort or 'default'}",
            f"judge={judge_model_id or 'unknown'}",
            f"judge_reasoning={judge_reasoning_effort or 'default'}",
            f"gen_prompt={generation_prompt_fingerprint[:12] if generation_prompt_fingerprint else 'unknown'}",
            f"grade_prompt={grading_prompt_fingerprint[:12] if grading_prompt_fingerprint else 'unknown'}",
            f"budget={agent_budget or 'default'}",
        ]
    )


def _normalize_attempt_record(
    record: dict[str, Any],
    *,
    run_jsonl_path: Path,
    price_book_path: Path,
    promotions: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    manifest = _read_manifest(run_jsonl_path)
    manifest_config = manifest.get("config", {})
    model_config = manifest_config.get("model", {})
    grader_config = manifest_config.get("grader", {})
    agent_config = manifest_config.get("agent", {})

    generation_mode = str(record.get("generation_mode") or manifest_config.get("generation", {}).get("mode") or "")
    if generation_mode != "tool_assisted_daytona":
        return None

    task_id = int(record["task_id"])
    run_index = int(record["run_index"])
    attempt_key = _attempt_key(run_jsonl_path, task_id, run_index)
    promotion = promotions.get(attempt_key, {})

    generation_input_tokens = int(record.get("generation_input_tokens", 0) or 0)
    generation_cached_input_tokens = _infer_generation_cached_input_tokens(record)
    generation_output_tokens = int(record.get("generation_output_tokens", 0) or 0)
    grading_input_tokens, grading_cached_input_tokens, grading_output_tokens = _infer_grading_token_fields(record)

    current_generation_cost = _current_cost_or_recorded(
        model_id=str(record.get("model_id", "")),
        input_tokens=generation_input_tokens,
        cached_input_tokens=generation_cached_input_tokens,
        output_tokens=generation_output_tokens,
        recorded_cost=float(record.get("generation_cost_usd", 0.0) or 0.0),
        price_book_path=price_book_path,
    )
    current_grading_cost = _current_cost_or_recorded(
        model_id=str(record.get("judge_model_id", "")),
        input_tokens=grading_input_tokens,
        cached_input_tokens=grading_cached_input_tokens,
        output_tokens=grading_output_tokens,
        recorded_cost=float(record.get("grading_cost_usd", 0.0) or 0.0),
        price_book_path=price_book_path,
    )

    generation_reasoning_effort = str(
        record.get("generation_reasoning_effort")
        or (model_config.get("model_configs", {}) or {}).get("reasoning_effort")
        or ""
    )
    generation_verbosity = str(
        record.get("generation_verbosity")
        or (model_config.get("model_configs", {}) or {}).get("verbosity")
        or ""
    )
    judge_reasoning_effort = str(
        record.get("judge_reasoning_effort")
        or (grader_config.get("model_configs", {}) or {}).get("reasoning_effort")
        or ""
    )
    judge_verbosity = str(
        record.get("judge_verbosity")
        or (grader_config.get("model_configs", {}) or {}).get("verbosity")
        or ""
    )

    generation_prompt_fingerprint = str(record.get("generation_prompt_fingerprint", "") or "")
    grading_prompt_fingerprint = str(record.get("grading_prompt_fingerprint", "") or "")
    agent_budget = f"steps={int(agent_config.get('max_steps', 0) or 0)},tools={int(agent_config.get('max_tool_calls', 0) or 0)}"

    return {
        "attempt_key": attempt_key,
        "promoted": bool(promotion),
        "headline": bool(str(promotion.get("headline", "")).lower() in {"1", "true", "yes"}),
        "promotion_label": promotion.get("label", ""),
        "promotion_notes": promotion.get("notes", ""),
        "promoted_at": promotion.get("promoted_at", ""),
        "run_jsonl_path": str(run_jsonl_path.resolve()),
        "output_dir": str(run_jsonl_path.parent.resolve()),
        "run_manifest_path": str((run_jsonl_path.parent / "run_manifest.json").resolve()),
        "task_id": task_id,
        "run_index": run_index,
        "domain": record.get("domain", ""),
        "status": record.get("status", ""),
        "business_pass": bool(record.get("business_pass", False)),
        "score_pct": float(record.get("score_pct", 0.0) or 0.0),
        "model_id": record.get("model_id", ""),
        "generation_reasoning_effort": generation_reasoning_effort,
        "generation_verbosity": generation_verbosity,
        "judge_model_id": record.get("judge_model_id", ""),
        "judge_reasoning_effort": judge_reasoning_effort,
        "judge_verbosity": judge_verbosity,
        "generation_mode": generation_mode,
        "agent_budget": agent_budget,
        "generation_prompt_fingerprint": generation_prompt_fingerprint,
        "grading_prompt_fingerprint": grading_prompt_fingerprint,
        "setup_id": _setup_id(
            generation_mode=generation_mode,
            model_id=str(record.get("model_id", "")),
            generation_reasoning_effort=generation_reasoning_effort,
            judge_model_id=str(record.get("judge_model_id", "")),
            judge_reasoning_effort=judge_reasoning_effort,
            generation_prompt_fingerprint=generation_prompt_fingerprint,
            grading_prompt_fingerprint=grading_prompt_fingerprint,
            agent_budget=agent_budget,
        ),
        "price_book_id_current": openai_price_book_id(price_book_path),
        "value_base_usd": float(record.get("value_base_usd", 0.0) or 0.0),
        "hours_estimate": float(record.get("hours_estimate", 0.0) or 0.0),
        "prompt_preview": record.get("prompt_preview", ""),
        "generation_input_tokens": generation_input_tokens,
        "generation_cached_input_tokens": generation_cached_input_tokens,
        "generation_output_tokens": generation_output_tokens,
        "grading_input_tokens": grading_input_tokens,
        "grading_cached_input_tokens": grading_cached_input_tokens,
        "grading_output_tokens": grading_output_tokens,
        "current_generation_cost_usd": round(current_generation_cost, 6),
        "current_grading_cost_usd": round(current_grading_cost, 6),
        "current_total_cost_usd": round(
            float(record.get("parse_cost_incurred_usd_this_run", 0.0) or 0.0) + current_generation_cost + current_grading_cost,
            6,
        ),
        "recorded_total_cost_usd": round(float(record.get("total_cost_usd_this_run", 0.0) or 0.0), 6),
        "attempt_completed_at": record.get("attempt_completed_at", ""),
    }


def discover_attempt_rows(outputs_root: str | Path, *, price_book_path: str | Path, promotions_path: str | Path) -> list[dict[str, Any]]:
    outputs_root = Path(outputs_root).resolve()
    price_book_path = Path(price_book_path).resolve()
    promotions = _load_promotions(Path(promotions_path).resolve())
    rows: list[dict[str, Any]] = []
    for run_jsonl_path in sorted(outputs_root.rglob("raw_runs.jsonl")):
        for record in read_jsonl(run_jsonl_path):
            normalized = _normalize_attempt_record(
                record,
                run_jsonl_path=run_jsonl_path,
                price_book_path=price_book_path,
                promotions=promotions,
            )
            if normalized is not None:
                rows.append(normalized)
    return rows


def summarize_promoted_attempts(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    grouped: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(int(row["task_id"]), str(row["setup_id"]))].append(row)

    summary_rows: list[dict[str, Any]] = []
    for _, group_rows in sorted(grouped.items(), key=lambda item: item[0]):
        attempts = len(group_rows)
        completed_rows = [row for row in group_rows if row.get("status") == "completed"]
        pass_rows = [row for row in group_rows if row.get("business_pass") is True]
        example = group_rows[0]

        total_cost = sum(float(row["current_total_cost_usd"]) for row in group_rows)
        success_costs = [float(row["current_total_cost_usd"]) for row in pass_rows]
        pass_rate = len(pass_rows) / attempts if attempts else 0.0
        mean_cost = total_cost / attempts if attempts else 0.0

        summary_rows.append(
            {
                "task_id": int(example["task_id"]),
                "domain": example["domain"],
                "prompt_preview": example["prompt_preview"],
                "setup_id": example["setup_id"],
                "generation_mode": example["generation_mode"],
                "model_id": example["model_id"],
                "generation_reasoning_effort": example["generation_reasoning_effort"],
                "generation_verbosity": example["generation_verbosity"],
                "judge_model_id": example["judge_model_id"],
                "judge_reasoning_effort": example["judge_reasoning_effort"],
                "judge_verbosity": example["judge_verbosity"],
                "generation_prompt_fingerprint": example["generation_prompt_fingerprint"],
                "grading_prompt_fingerprint": example["grading_prompt_fingerprint"],
                "agent_budget": example["agent_budget"],
                "price_book_id_current": example["price_book_id_current"],
                "promoted_attempts": attempts,
                "completed_runs": len(completed_rows),
                "business_passes": len(pass_rows),
                "pass_rate": round(pass_rate, 4),
                "mean_score_pct": round(_safe_mean([float(row["score_pct"]) for row in completed_rows]), 4),
                "mean_generation_cost_per_attempt_usd": round(
                    _safe_mean([float(row["current_generation_cost_usd"]) for row in group_rows]), 6
                ),
                "mean_grading_cost_per_attempt_usd": round(
                    _safe_mean([float(row["current_grading_cost_usd"]) for row in group_rows]), 6
                ),
                "mean_total_cost_per_attempt_usd": round(mean_cost, 6),
                "mean_cost_of_successful_attempts_usd": round(_safe_mean(success_costs), 6) if success_costs else "",
                "cost_per_success_usd": round(total_cost / len(pass_rows), 6) if pass_rows else "",
                "hours_estimate": float(example["hours_estimate"]),
                "value_base_usd": float(example["value_base_usd"]),
                "expected_net_base_usd_per_attempt": round(pass_rate * float(example["value_base_usd"]) - mean_cost, 6),
                "latest_attempt_completed_at": max(str(row.get("attempt_completed_at", "")) for row in group_rows),
                "promotion_labels": "; ".join(
                    sorted({str(row.get("promotion_label", "")).strip() for row in group_rows if str(row.get("promotion_label", "")).strip()})
                ),
            }
        )

    overall = {
        "promoted_attempts": sum(int(row["promoted_attempts"]) for row in summary_rows),
        "tracked_task_setups": len(summary_rows),
        "business_passes": sum(int(row["business_passes"]) for row in summary_rows),
        "overall_pass_rate": round(
            sum(int(row["business_passes"]) for row in summary_rows)
            / sum(int(row["promoted_attempts"]) for row in summary_rows),
            6,
        ) if summary_rows else 0.0,
        "mean_total_cost_per_attempt_usd": round(
            _safe_mean([float(row["mean_total_cost_per_attempt_usd"]) for row in summary_rows]),
            6,
        ) if summary_rows else 0.0,
        "mean_expected_net_base_usd_per_attempt": round(
            _safe_mean([float(row["expected_net_base_usd_per_attempt"]) for row in summary_rows]),
            6,
        ) if summary_rows else 0.0,
    }
    return summary_rows, overall


def _write_master_report(path: Path, rows: list[dict[str, Any]], overall: dict[str, Any]) -> Path:
    lines = [
        "# Master Tracker",
        "",
        "## Overall",
        "",
        f"- Tracked task setups: **{overall['tracked_task_setups']}**",
        f"- Promoted attempts: **{overall['promoted_attempts']}**",
        f"- Business passes: **{overall['business_passes']}**",
        f"- Overall pass rate: **{overall['overall_pass_rate']:.4f}**",
        f"- Mean total cost per attempt: **${overall['mean_total_cost_per_attempt_usd']:.6f}**",
        "",
        "## Task Setups",
        "",
        "| Task | Model | Reasoning | Runs | Pass rate | Cost/success | Value base | Expected net/base |",
        "|---:|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        cost_per_success = row["cost_per_success_usd"] if row["cost_per_success_usd"] != "" else "—"
        lines.append(
            f"| {row['task_id']} | {row['model_id']} | {row['generation_reasoning_effort'] or 'default'} | "
            f"{row['promoted_attempts']} | {float(row['pass_rate']):.4f} | {cost_per_success} | "
            f"${float(row['value_base_usd']):.2f} | ${float(row['expected_net_base_usd_per_attempt']):.6f} |"
        )
    ensure_dir(path.parent)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def rebuild_tracker(outputs_root: str | Path, tracker_dir: str | Path, *, price_book_path: str | Path) -> dict[str, Any]:
    outputs_root = Path(outputs_root).resolve()
    tracker_dir = ensure_dir(Path(tracker_dir).resolve())
    price_book_path = Path(price_book_path).resolve()

    promotions_path = tracker_dir / "promotions.csv"
    discovered_rows = discover_attempt_rows(outputs_root, price_book_path=price_book_path, promotions_path=promotions_path)
    discovered_rows.sort(key=lambda row: (int(row["task_id"]), str(row["output_dir"]), int(row["run_index"])))
    _write_csv(tracker_dir / "discovered_attempts.csv", DISCOVERED_ATTEMPT_HEADERS, discovered_rows)

    promoted_rows = [row for row in discovered_rows if row["promoted"]]
    _write_csv(tracker_dir / "promoted_attempts.csv", PROMOTED_ATTEMPT_HEADERS, promoted_rows)

    master_rows, overall = summarize_promoted_attempts(promoted_rows)
    _write_csv(tracker_dir / "master_tracker.csv", MASTER_TRACKER_HEADERS, master_rows)
    write_json(tracker_dir / "master_tracker_overall.json", overall)
    _write_master_report(tracker_dir / "master_tracker.md", master_rows, overall)

    return {
        "discovered_attempts": len(discovered_rows),
        "promoted_attempts": len(promoted_rows),
        "tracked_task_setups": len(master_rows),
        "tracker_dir": str(tracker_dir),
    }


def promote_run(
    *,
    tracker_dir: str | Path,
    output_dir: str | Path | None = None,
    run_jsonl_path: str | Path | None = None,
    task_id: int | None = None,
    run_index: int | None = None,
    promote_all: bool = False,
    label: str = "",
    notes: str = "",
    headline: bool = False,
    outputs_root: str | Path,
    price_book_path: str | Path,
) -> tuple[int, dict[str, Any]]:
    tracker_dir = ensure_dir(Path(tracker_dir).resolve())
    if run_jsonl_path is not None:
        resolved_run_jsonl_path = Path(run_jsonl_path).resolve()
    elif output_dir is not None:
        resolved_run_jsonl_path = (Path(output_dir).resolve() / "raw_runs.jsonl")
    else:
        raise ValueError("Provide either output_dir or run_jsonl_path.")

    if not resolved_run_jsonl_path.exists():
        raise FileNotFoundError(f"Could not find raw run log at {resolved_run_jsonl_path}")

    rows = read_jsonl(resolved_run_jsonl_path)
    if promote_all:
        selected = rows
    else:
        selected = rows
        if task_id is not None:
            selected = [row for row in selected if int(row["task_id"]) == int(task_id)]
        if run_index is not None:
            selected = [row for row in selected if int(row["run_index"]) == int(run_index)]
        if task_id is None and run_index is None:
            if len(selected) != 1:
                raise ValueError("Run log contains multiple attempts; pass --task-id/--run-index or use --all.")

    if not selected:
        raise ValueError("No matching attempts found to promote.")

    promotions_path = tracker_dir / "promotions.csv"
    promotions = _load_promotions(promotions_path)
    for row in selected:
        key = _attempt_key(resolved_run_jsonl_path, int(row["task_id"]), int(row["run_index"]))
        promotions[key] = {
            "attempt_key": key,
            "promoted_at": utc_now_iso(),
            "run_jsonl_path": str(resolved_run_jsonl_path),
            "output_dir": str(resolved_run_jsonl_path.parent),
            "task_id": int(row["task_id"]),
            "run_index": int(row["run_index"]),
            "label": label,
            "notes": notes,
            "headline": bool(headline),
        }

    _write_promotions(promotions_path, list(promotions.values()))
    summary = rebuild_tracker(outputs_root, tracker_dir, price_book_path=price_book_path)
    return len(selected), summary
