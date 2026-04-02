from __future__ import annotations

import asyncio
import csv
from pathlib import Path
from typing import Any

from .business_rules import business_pass
from .config import AppConfig
from .dataset import filter_tasks, load_tasks, resolve_attachment_paths
from .mercor_adapter import run_generation_once, run_grading_once
from .prompting import generation_system_prompt, generation_user_prompt, grading_prompt, tool_agent_system_prompt
from .reducto_parser import ReductoAttachmentParser
from .reporting import rebuild_outputs
from .task_map import build_task_map_rows
from .tool_agent import run_tool_assisted_generation_once
from .utils import append_jsonl, ensure_dir, read_jsonl, sha256_text, shorten, utc_now_iso, write_json
from .value_model import load_value_overrides, resolve_value_for_task


def _provider_env_var(model_id: str) -> str | None:
    lowered = model_id.lower()
    if lowered.startswith("gpt-") or lowered.startswith("o"):
        return "OPENAI_API_KEY"
    if lowered.startswith("gemini"):
        return "GOOGLE_API_KEY"
    if lowered.startswith("claude"):
        return "ANTHROPIC_API_KEY"
    if lowered.startswith("grok"):
        return "XAI_API_KEY"
    return None


def validate_environment(config: AppConfig) -> None:
    import os

    needed = set()
    for model_id in [config.model.model_id, config.grader.model_id]:
        env_var = _provider_env_var(model_id)
        if env_var:
            needed.add(env_var)
    if config.reducto.enabled:
        needed.add("REDUCTO_API_KEY")

    missing = [name for name in sorted(needed) if not os.getenv(name)]
    if missing:
        joined = ", ".join(missing)
        raise EnvironmentError(f"Missing required environment variables: {joined}")


def _selected_tasks_csv(output_path: Path, rows: list[dict[str, Any]]) -> None:
    headers = [
        "task_id",
        "domain",
        "job",
        "task_description",
        "success_criteria",
        "attachment_count",
        "attachment_total_bytes",
        "attachment_total_mb",
        "largest_attachment_bytes",
        "criterion_count",
        "primary_criteria_count",
        "secondary_criteria_count",
        "hours_estimate",
        "value_low_usd",
        "value_base_usd",
        "value_high_usd",
        "value_source",
        "notes",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class FinanceEvaluator:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.output_dir = ensure_dir(config.output_dir)
        self.raw_runs_path = self.output_dir / "raw_runs.jsonl"
        self.parser = ReductoAttachmentParser(settings=config.reducto, cache_dir=config.parse_cache_dir)

    def _existing_run_keys(self) -> set[tuple[int, int]]:
        if not self.config.evaluation.resume:
            return set()
        keys: set[tuple[int, int]] = set()
        for row in read_jsonl(self.raw_runs_path):
            try:
                keys.add((int(row["task_id"]), int(row["run_index"])))
            except (KeyError, TypeError, ValueError):
                continue
        return keys

    def _selection_rows(self, tasks) -> list[dict[str, Any]]:
        overrides = load_value_overrides(self.config.value_model.overrides_csv)
        task_map_rows = {
            int(row["task_id"]): row
            for row in build_task_map_rows(
                self.config.dataset_dir,
                tasks,
                task_metadata_path=self.config.tracking.task_metadata_csv,
            )
        }
        rows = []
        for task in tasks:
            value = resolve_value_for_task(
                task,
                overrides,
                default_hours=self.config.value_model.default_hours,
                low_rate=self.config.value_model.low_rate,
                base_rate=self.config.value_model.base_rate,
                high_rate=self.config.value_model.high_rate,
            )
            task_map_row = task_map_rows.get(task.task_id, {})
            rows.append(
                {
                    "task_id": task.task_id,
                    "domain": task.domain,
                    "job": str(task_map_row.get("job", "") or ""),
                    "task_description": str(task_map_row.get("task_description", task.task_description) or task.task_description),
                    "success_criteria": str(task_map_row.get("success_criteria", "") or ""),
                    "attachment_count": int(task_map_row.get("attachment_count", task.attachment_count) or 0),
                    "attachment_total_bytes": int(task_map_row.get("attachment_total_bytes", 0) or 0),
                    "attachment_total_mb": float(task_map_row.get("attachment_total_mb", 0.0) or 0.0),
                    "largest_attachment_bytes": int(task_map_row.get("largest_attachment_bytes", 0) or 0),
                    "criterion_count": int(task_map_row.get("criterion_count", 0) or 0),
                    "primary_criteria_count": int(task_map_row.get("primary_criteria_count", 0) or 0),
                    "secondary_criteria_count": int(task_map_row.get("secondary_criteria_count", 0) or 0),
                    "hours_estimate": value.hours_estimate,
                    "value_low_usd": value.value_low_usd,
                    "value_base_usd": value.value_base_usd,
                    "value_high_usd": value.value_high_usd,
                    "value_source": value.source,
                    "notes": value.notes,
                }
            )
        return rows

    async def run(self) -> None:
        validate_environment(self.config)

        tasks = load_tasks(self.config.dataset_dir)
        selected = filter_tasks(
            tasks,
            domain=self.config.selection.domain,
            task_ids=self.config.selection.task_ids,
            start_index=self.config.selection.start_index,
            limit=self.config.selection.limit,
        )
        task_metadata_by_id = {
            int(row["task_id"]): row
            for row in build_task_map_rows(
                self.config.dataset_dir,
                selected,
                task_metadata_path=self.config.tracking.task_metadata_csv,
            )
        }
        overrides = load_value_overrides(self.config.value_model.overrides_csv)
        existing = self._existing_run_keys()

        selection_rows = self._selection_rows(selected)
        _selected_tasks_csv(self.output_dir / "selected_tasks.csv", selection_rows)

        manifest = {
            "created_at": utc_now_iso(),
            "config": self.config.to_dict(),
            "selected_task_count": len(selected),
            "selected_task_ids": [task.task_id for task in selected],
        }
        write_json(self.output_dir / "run_manifest.json", manifest)

        grading_template = grading_prompt()
        generation_system_template = (
            tool_agent_system_prompt() if self.config.generation.mode == "tool_assisted_daytona" else generation_system_prompt()
        )
        generation_prompt_fingerprint = sha256_text(generation_system_template)
        grading_prompt_fingerprint = sha256_text(grading_template)

        for task in selected:
            resolved_attachments = resolve_attachment_paths(self.config.dataset_dir, task)
            parsed_attachments = self.parser.parse_many(resolved_attachments)
            task_metadata = task_metadata_by_id.get(task.task_id, {})
            parse_cost_for_task = sum(item.cost_incurred_usd for item in parsed_attachments)
            parse_pages_for_task = sum((item.num_pages or 0) for item in parsed_attachments if not item.cache_hit)
            parse_credits_for_task = sum(item.credits_incurred for item in parsed_attachments)

            value = resolve_value_for_task(
                task,
                overrides,
                default_hours=self.config.value_model.default_hours,
                low_rate=self.config.value_model.low_rate,
                base_rate=self.config.value_model.base_rate,
                high_rate=self.config.value_model.high_rate,
            )

            prompt = generation_user_prompt(task, parsed_attachments)

            for run_index in range(1, self.config.evaluation.runs_per_task + 1):
                key = (task.task_id, run_index)
                if key in existing:
                    continue

                local_artifact_dir = self.output_dir / "generation_artifacts" / f"task_{task.task_id}" / f"run_{run_index}"
                try:
                    if self.config.generation.mode == "tool_assisted_daytona":
                        generation = await run_tool_assisted_generation_once(
                            task=task,
                            parsed_attachments=parsed_attachments,
                            config=self.config,
                            local_artifact_dir=local_artifact_dir,
                        )
                    else:
                        generation = await run_generation_once(
                            prompt=prompt,
                            system_prompt=generation_system_prompt(),
                            model=self.config.model,
                            pricing=self.config.pricing,
                        )
                except Exception as exc:
                    generation = {
                        "success": False,
                        "response": "",
                        "raw_response": "",
                        "input_tokens": 0,
                        "cached_input_tokens": 0,
                        "output_tokens": 0,
                        "total_tokens": 0,
                        "total_cost": 0.0,
                        "api_provider": "unknown",
                        "execution_time_seconds": None,
                        "error_message": str(exc),
                        "started_at": None,
                        "completed_at": None,
                        "details": {
                            "generation_mode": self.config.generation.mode,
                            "exception_type": type(exc).__name__,
                        },
                    }

                record: dict[str, Any] = {
                    "task_id": task.task_id,
                    "domain": task.domain,
                    "run_index": run_index,
                    "attempt_started_at": utc_now_iso(),
                    "model_id": self.config.model.model_id,
                    "generation_reasoning_effort": self.config.model.model_configs.get("reasoning_effort", ""),
                    "generation_verbosity": self.config.model.model_configs.get("verbosity", ""),
                    "judge_model_id": self.config.grader.model_id,
                    "judge_reasoning_effort": self.config.grader.model_configs.get("reasoning_effort", ""),
                    "judge_verbosity": self.config.grader.model_configs.get("verbosity", ""),
                    "job": str(task_metadata.get("job", "") or ""),
                    "task_description": str(task_metadata.get("task_description", task.task_description) or task.task_description),
                    "success_criteria": str(task_metadata.get("success_criteria", "") or ""),
                    "prompt_preview": shorten(task.prompt, 160),
                    "attachment_count": int(task_metadata.get("attachment_count", task.attachment_count) or 0),
                    "attachment_total_bytes": int(task_metadata.get("attachment_total_bytes", 0) or 0),
                    "attachment_total_mb": float(task_metadata.get("attachment_total_mb", 0.0) or 0.0),
                    "largest_attachment_bytes": int(task_metadata.get("largest_attachment_bytes", 0) or 0),
                    "attachment_paths": task.attachment_paths,
                    "criterion_count": int(task_metadata.get("criterion_count", 0) or 0),
                    "primary_criteria_count": int(task_metadata.get("primary_criteria_count", 0) or 0),
                    "secondary_criteria_count": int(task_metadata.get("secondary_criteria_count", 0) or 0),
                    "hours_estimate": value.hours_estimate,
                    "value_low_usd": value.value_low_usd,
                    "value_base_usd": value.value_base_usd,
                    "value_high_usd": value.value_high_usd,
                    "value_source": value.source,
                    "value_notes": value.notes,
                    "parse_cache_hits": sum(1 for item in parsed_attachments if item.cache_hit),
                    "parse_cache_misses": sum(1 for item in parsed_attachments if not item.cache_hit),
                    "parse_pages_incurred_this_run": parse_pages_for_task if run_index == 1 else 0,
                    "parse_credits_incurred_this_run": parse_credits_for_task if run_index == 1 else 0.0,
                    "parse_cost_incurred_usd_this_run": parse_cost_for_task if run_index == 1 else 0.0,
                    "parse_attachments": [item.metadata for item in parsed_attachments],
                    "generation_input_tokens": generation["input_tokens"],
                    "generation_cached_input_tokens": generation.get("cached_input_tokens", 0),
                    "generation_output_tokens": generation["output_tokens"],
                    "generation_total_tokens": generation["total_tokens"],
                    "generation_cost_usd": generation["total_cost"],
                    "generation_provider": generation["api_provider"],
                    "generation_mode": self.config.generation.mode,
                    "generation_prompt_fingerprint": generation_prompt_fingerprint,
                    "generation_price_book_id": generation.get("details", {}).get("price_book_id", ""),
                    "generation_execution_time_seconds": generation["execution_time_seconds"],
                    "generation_error_message": generation["error_message"],
                    "generation_steps_used": int(generation.get("generation_steps_used", 0) or 0),
                    "tools_used": generation.get("tools_used", []),
                    "generation_details": generation.get("details", {}),
                    "status": "generation_failed",
                    "score_pct": 0.0,
                    "business_pass": False,
                    "primary_total": 0,
                    "primary_met": 0,
                    "secondary_total": 0,
                    "secondary_met": 0,
                    "all_primary_met": False,
                    "grading_cost_usd": 0.0,
                    "grading_input_tokens": 0,
                    "grading_cached_input_tokens": 0,
                    "grading_output_tokens": 0,
                    "grading_prompt_fingerprint": grading_prompt_fingerprint,
                    "grading_price_book_id": "",
                    "grading_tokens": 0,
                    "grading_execution_time_seconds": None,
                    "criteria_results": [],
                    "score_summary": {},
                    "response_text": generation["response"] if self.config.evaluation.save_response_text else None,
                }

                if generation["success"]:
                    grading = await run_grading_once(
                        solution=generation["response"],
                        rubric_json=task.rubric_json,
                        grader=self.config.grader,
                        grading_prompt_template=grading_template,
                        pricing=self.config.pricing,
                    )
                    passed, summary = business_pass(
                        score_pct=float(grading["percentage_score"] or 0.0),
                        criteria_results=grading["criteria_results"],
                        min_overall_score_pct=self.config.evaluation.min_overall_score_pct,
                        require_all_primary=self.config.evaluation.require_all_primary,
                    )
                    record.update(
                        {
                            "status": "completed" if not grading["grading_error"] else "grading_failed",
                            "score_pct": float(grading["percentage_score"] or 0.0),
                            "business_pass": bool(passed),
                            "primary_total": summary["primary_total"],
                            "primary_met": summary["primary_met"],
                            "secondary_total": summary["secondary_total"],
                            "secondary_met": summary["secondary_met"],
                            "all_primary_met": summary["all_primary_met"],
                            "grading_cost_usd": float(grading["total_grading_cost"] or 0.0),
                            "grading_input_tokens": int(grading.get("total_grading_input_tokens", 0) or 0),
                            "grading_cached_input_tokens": int(grading.get("total_grading_cached_input_tokens", 0) or 0),
                            "grading_output_tokens": int(grading.get("total_grading_output_tokens", 0) or 0),
                            "grading_price_book_id": grading.get("price_book_id", ""),
                            "grading_tokens": int(grading["total_grading_tokens"] or 0),
                            "grading_execution_time_seconds": grading["execution_time_seconds"],
                            "criteria_results": grading["criteria_results"],
                            "score_summary": grading["criteria_results"] if self.config.evaluation.save_score_summary else [],
                            "grading_error_message": grading["grading_error"],
                        }
                    )

                record["total_cost_usd_this_run"] = (
                    float(record["parse_cost_incurred_usd_this_run"])
                    + float(record["generation_cost_usd"])
                    + float(record["grading_cost_usd"])
                )
                record["attempt_completed_at"] = utc_now_iso()
                append_jsonl(self.raw_runs_path, record)

        rebuild_outputs(self.raw_runs_path, self.output_dir)


def run_sync(config: AppConfig) -> None:
    asyncio.run(FinanceEvaluator(config).run())
