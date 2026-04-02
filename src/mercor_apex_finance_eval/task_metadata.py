from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


TASK_METADATA_HEADERS = [
    "task_id",
    "domain",
    "job",
    "task_description",
    "success_criteria",
]


def load_task_metadata_overrides(path: str | Path | None) -> dict[int, dict[str, str]]:
    if not path:
        return {}

    csv_path = Path(path)
    if not csv_path.exists():
        return {}

    rows: dict[int, dict[str, str]] = {}
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                task_id = int(str(row.get("task_id", "")).strip())
            except (TypeError, ValueError):
                continue

            rows[task_id] = {
                "domain": str(row.get("domain", "") or "").strip(),
                "job": str(row.get("job", "") or "").strip(),
                "task_description": str(row.get("task_description", "") or "").strip(),
                "success_criteria": str(row.get("success_criteria", "") or "").strip(),
            }
    return rows


def apply_task_metadata_override(
    row: dict[str, Any],
    override: dict[str, str] | None,
) -> dict[str, Any]:
    merged = dict(row)
    payload = override or {}

    if payload.get("domain"):
        merged["domain"] = payload["domain"]
    if payload.get("job"):
        merged["job"] = payload["job"]
    merged["task_description"] = payload.get("task_description") or str(merged.get("task_description", "") or "")
    merged["success_criteria"] = payload.get("success_criteria") or str(merged.get("success_criteria", "") or "")
    return merged
