from __future__ import annotations

import csv
from pathlib import Path

from .dataset import filter_tasks, load_tasks
from .types import TaskRecord, ValueEstimate
from .utils import shorten


VALUE_FIELDS = [
    "task_id",
    "domain",
    "attachment_count",
    "prompt_preview",
    "hours_estimate",
    "value_low_usd",
    "value_base_usd",
    "value_high_usd",
    "notes",
]


def seed_value_file(
    *,
    dataset_dir: str | Path,
    output_csv: str | Path,
    domain: str,
    default_hours: float,
    low_rate: float,
    base_rate: float,
    high_rate: float,
    force: bool = False,
) -> Path:
    output_csv = Path(output_csv)
    if output_csv.exists() and not force:
        raise FileExistsError(f"{output_csv} already exists. Use --force to overwrite.")

    tasks = filter_tasks(load_tasks(dataset_dir), domain=domain)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=VALUE_FIELDS)
        writer.writeheader()
        for task in tasks:
            writer.writerow(
                {
                    "task_id": task.task_id,
                    "domain": task.domain,
                    "attachment_count": task.attachment_count,
                    "prompt_preview": shorten(task.prompt, 160),
                    "hours_estimate": f"{default_hours:.2f}",
                    "value_low_usd": f"{default_hours * low_rate:.2f}",
                    "value_base_usd": f"{default_hours * base_rate:.2f}",
                    "value_high_usd": f"{default_hours * high_rate:.2f}",
                    "notes": "",
                }
            )
    return output_csv


def load_value_overrides(path: str | Path) -> dict[int, ValueEstimate]:
    path = Path(path)
    if not path.exists():
        return {}

    overrides: dict[int, ValueEstimate] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                task_id = int(row["task_id"])
            except (TypeError, ValueError, KeyError):
                continue
            overrides[task_id] = ValueEstimate(
                hours_estimate=float(row.get("hours_estimate") or 0.0),
                value_low_usd=float(row.get("value_low_usd") or 0.0),
                value_base_usd=float(row.get("value_base_usd") or 0.0),
                value_high_usd=float(row.get("value_high_usd") or 0.0),
                source="override_csv",
                notes=row.get("notes", "").strip(),
            )
    return overrides


def resolve_value_for_task(
    task: TaskRecord,
    overrides: dict[int, ValueEstimate],
    *,
    default_hours: float,
    low_rate: float,
    base_rate: float,
    high_rate: float,
) -> ValueEstimate:
    if task.task_id in overrides:
        return overrides[task.task_id]

    return ValueEstimate(
        hours_estimate=default_hours,
        value_low_usd=default_hours * low_rate,
        value_base_usd=default_hours * base_rate,
        value_high_usd=default_hours * high_rate,
        source="default_rates",
        notes="",
    )
