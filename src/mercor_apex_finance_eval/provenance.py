from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping


APEX_PUBLIC_V1_EXTENDED_PROVENANCE_ID = "apex_public_v1_extended_train"

TASK_PROVENANCE_HEADERS = [
    "provenance_id",
    "task_source",
    "source_type",
    "source_provider",
    "dataset_name",
    "dataset_version",
    "dataset_split",
    "access_level",
    "source_reference",
    "source_url",
    "notes",
]

_APEX_PUBLIC_V1_EXTENDED_ROW = {
    "provenance_id": APEX_PUBLIC_V1_EXTENDED_PROVENANCE_ID,
    "task_source": "Apex",
    "source_type": "dataset",
    "source_provider": "Mercor",
    "dataset_name": "APEX-v1-extended",
    "dataset_version": "v1-extended",
    "dataset_split": "train",
    "access_level": "public",
    "source_reference": "mercor/APEX-v1-extended",
    "source_url": "https://huggingface.co/datasets/mercor/APEX-v1-extended",
    "notes": "Current task provenance is the public APEX-v1-extended release. Additional eval sources may be added later.",
}


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return cleaned or "unknown"


def _fallback_row(*, provenance_id: str, task_source: str, dataset_name: str = "") -> dict[str, str]:
    source_type = "dataset" if dataset_name else "unknown"
    notes = "Task provenance metadata has not been fully authored for this source yet."
    return {
        "provenance_id": provenance_id,
        "task_source": task_source or dataset_name or "Unknown",
        "source_type": source_type,
        "source_provider": "",
        "dataset_name": dataset_name,
        "dataset_version": "",
        "dataset_split": "",
        "access_level": "",
        "source_reference": "",
        "source_url": "",
        "notes": notes,
    }


def infer_task_provenance(dataset_dir: str | Path | None) -> dict[str, str]:
    dataset_dir_text = str(dataset_dir or "").strip()
    dataset_path = Path(dataset_dir_text).resolve() if dataset_dir_text else None
    dataset_name = dataset_path.name if dataset_path else ""
    normalized = dataset_dir_text.lower()

    if "apex-v1-extended" in normalized or dataset_name.lower() == "apex-v1-extended":
        return dict(_APEX_PUBLIC_V1_EXTENDED_ROW)

    if dataset_name:
        return _fallback_row(
            provenance_id=f"dataset_{_slugify(dataset_name)}",
            task_source=dataset_name,
            dataset_name=dataset_name,
        )

    return _fallback_row(
        provenance_id="unknown",
        task_source="Unknown",
    )


def resolve_task_provenance_metadata(
    provenance_id: str | None,
    *,
    task_source: str | None = None,
) -> dict[str, str]:
    cleaned_id = str(provenance_id or "").strip()
    cleaned_source = str(task_source or "").strip()

    if not cleaned_id and cleaned_source.lower() == "apex":
        return dict(_APEX_PUBLIC_V1_EXTENDED_ROW)
    if cleaned_id == APEX_PUBLIC_V1_EXTENDED_PROVENANCE_ID:
        return dict(_APEX_PUBLIC_V1_EXTENDED_ROW)

    if cleaned_id.startswith("dataset_"):
        dataset_name = cleaned_source if cleaned_source and cleaned_source != "Unknown" else ""
        return _fallback_row(
            provenance_id=cleaned_id,
            task_source=cleaned_source or dataset_name or "Unknown",
            dataset_name=dataset_name,
        )

    return _fallback_row(
        provenance_id=cleaned_id or "unknown",
        task_source=cleaned_source or "Unknown",
    )


def build_task_provenance_rows(rows: list[Mapping[str, Any]]) -> list[dict[str, str]]:
    by_id: dict[str, dict[str, str]] = {}
    for row in rows:
        metadata = resolve_task_provenance_metadata(
            row.get("provenance_id"),
            task_source=row.get("task_source"),
        )
        by_id[metadata["provenance_id"]] = metadata
    return [by_id[key] for key in sorted(by_id)]
