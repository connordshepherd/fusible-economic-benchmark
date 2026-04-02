from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable

from .dataset import filter_tasks, load_tasks, resolve_attachment_paths
from .task_metadata import apply_task_metadata_override, load_task_metadata_overrides
from .types import TaskRecord
from .utils import jsonable, write_json


TASK_MAP_HEADERS = [
    "task_id",
    "domain",
    "job",
    "task_description",
    "success_criteria",
    "attachment_count",
    "attachment_total_bytes",
    "attachment_total_mb",
    "largest_attachment_bytes",
    "attachment_extensions",
    "attachment_paths",
    "prompt_char_count",
    "prompt_word_count",
    "criterion_count",
    "primary_criteria_count",
    "secondary_criteria_count",
    "criteria_with_sources_count",
    "criterion_types",
]

TASK_MAP_SORT_FIELDS = set(TASK_MAP_HEADERS) - {"task_description", "success_criteria", "attachment_paths", "criterion_types"}


def _normalize_rubric(rubric_json: str) -> dict[str, Any]:
    if not rubric_json:
        return {}

    parsed = json.loads(rubric_json)
    if isinstance(parsed, dict):
        return parsed
    if isinstance(parsed, list):
        merged: dict[str, Any] = {}
        for item in parsed:
            if isinstance(item, dict):
                merged.update(item)
        return merged
    return {}


def _attachment_extensions(paths: list[Path]) -> str:
    extensions = []
    for path in paths:
        suffix = path.suffix.lower() or "(none)"
        if suffix not in extensions:
            extensions.append(suffix)
    return ";".join(extensions)


def _criterion_types(rubric: dict[str, Any]) -> str:
    types: list[str] = []
    for criterion in rubric.values():
        if not isinstance(criterion, dict):
            continue
        for criterion_type in criterion.get("criterion_type", []) or []:
            text = str(criterion_type).strip()
            if text and text not in types:
                types.append(text)
    return ";".join(types)


def build_task_map_rows(
    dataset_dir: str | Path,
    tasks: Iterable[TaskRecord],
    *,
    task_metadata_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    dataset_dir = Path(dataset_dir)
    authored_metadata = load_task_metadata_overrides(task_metadata_path)
    rows: list[dict[str, Any]] = []

    for task in tasks:
        attachment_paths = resolve_attachment_paths(dataset_dir, task)
        attachment_sizes = [path.stat().st_size for path in attachment_paths]
        rubric = _normalize_rubric(task.rubric_json)

        primary_count = 0
        criteria_with_sources = 0
        for criterion in rubric.values():
            if not isinstance(criterion, dict):
                continue
            weight = str(criterion.get("weight", "") or "")
            if "Primary objective" in weight:
                primary_count += 1
            if str(criterion.get("sources", "") or "").strip():
                criteria_with_sources += 1

        prompt_word_count = len(task.prompt.split())
        criterion_count = len(rubric)

        row = {
            "task_id": task.task_id,
            "domain": task.domain,
            "job": "",
            "task_description": task.task_description,
            "success_criteria": "",
            "attachment_count": len(attachment_paths),
            "attachment_total_bytes": sum(attachment_sizes),
            "attachment_total_mb": round(sum(attachment_sizes) / (1024 * 1024), 6),
            "largest_attachment_bytes": max(attachment_sizes) if attachment_sizes else 0,
            "attachment_extensions": _attachment_extensions(attachment_paths),
            "attachment_paths": ";".join(str(path.relative_to(dataset_dir)) for path in attachment_paths),
            "prompt_char_count": len(task.prompt),
            "prompt_word_count": prompt_word_count,
            "criterion_count": criterion_count,
            "primary_criteria_count": primary_count,
            "secondary_criteria_count": max(criterion_count - primary_count, 0),
            "criteria_with_sources_count": criteria_with_sources,
            "criterion_types": _criterion_types(rubric),
        }
        rows.append(apply_task_metadata_override(row, authored_metadata.get(task.task_id)))

    return rows


def generate_task_map(
    dataset_dir: str | Path,
    *,
    task_metadata_path: str | Path | None = None,
    domain: str | None = None,
    task_ids: list[int] | None = None,
    start_index: int = 0,
    limit: int | None = None,
    sort_by: str = "task_id",
    descending: bool = False,
) -> list[dict[str, Any]]:
    if sort_by not in TASK_MAP_HEADERS:
        choices = ", ".join(TASK_MAP_HEADERS)
        raise ValueError(f"Unsupported sort field `{sort_by}`. Choose one of: {choices}")

    tasks = filter_tasks(
        load_tasks(dataset_dir),
        domain=domain,
        task_ids=task_ids,
        start_index=start_index,
        limit=limit,
    )
    rows = build_task_map_rows(dataset_dir, tasks, task_metadata_path=task_metadata_path)
    rows.sort(key=lambda row: row.get(sort_by), reverse=descending)
    return rows


def infer_task_map_format(output_path: str | Path, requested: str | None = None) -> str:
    if requested:
        return requested

    suffix = Path(output_path).suffix.lower()
    if suffix == ".json":
        return "json"
    if suffix == ".jsonl":
        return "jsonl"
    return "csv"


def write_task_map(output_path: str | Path, rows: list[dict[str, Any]], *, fmt: str | None = None) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    resolved_fmt = infer_task_map_format(output_path, fmt)
    if resolved_fmt == "csv":
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=TASK_MAP_HEADERS)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return output_path

    if resolved_fmt == "json":
        write_json(output_path, rows)
        return output_path

    if resolved_fmt == "jsonl":
        with output_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(jsonable(row), ensure_ascii=False) + "\n")
        return output_path

    raise ValueError(f"Unsupported output format `{resolved_fmt}`.")
