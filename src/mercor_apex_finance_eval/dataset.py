from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from .types import TaskRecord


def parse_attachment_field(raw: str) -> list[str]:
    if not raw:
        return []
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    return lines


def load_tasks(dataset_dir: str | Path) -> list[TaskRecord]:
    dataset_dir = Path(dataset_dir)
    csv_path = dataset_dir / "data" / "train.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find dataset CSV: {csv_path}")

    tasks: list[TaskRecord] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            task_id = int(row["Task ID"])
            tasks.append(
                TaskRecord(
                    task_id=task_id,
                    domain=row.get("Domain", "").strip(),
                    prompt=row.get("Prompt", "").strip(),
                    rubric_json=row.get("Rubric JSON", "").strip(),
                    attachment_paths=parse_attachment_field(row.get("File Attachments", "")),
                )
            )
    return tasks


def filter_tasks(
    tasks: Iterable[TaskRecord],
    *,
    domain: str | None = None,
    task_ids: list[int] | None = None,
    start_index: int = 0,
    limit: int | None = None,
) -> list[TaskRecord]:
    selected = list(tasks)
    if domain:
        selected = [task for task in selected if task.domain == domain]
    if task_ids:
        wanted = set(task_ids)
        selected = [task for task in selected if task.task_id in wanted]
        selected.sort(key=lambda task: task_ids.index(task.task_id) if task.task_id in wanted else 10**9)
    else:
        selected.sort(key=lambda task: task.task_id)

    if start_index:
        selected = selected[start_index:]
    if limit is not None:
        selected = selected[:limit]
    return selected


def resolve_attachment_paths(dataset_dir: str | Path, task: TaskRecord) -> list[Path]:
    dataset_dir = Path(dataset_dir)
    paths: list[Path] = []
    for rel in task.attachment_paths:
        candidate = dataset_dir / rel
        if not candidate.exists():
            raise FileNotFoundError(f"Attachment listed by task {task.task_id} does not exist: {candidate}")
        paths.append(candidate)
    return paths
