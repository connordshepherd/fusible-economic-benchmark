from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .utils import compact_text


@dataclass(slots=True)
class TaskRecord:
    task_id: int
    domain: str
    prompt: str
    rubric_json: str
    attachment_paths: list[str] = field(default_factory=list)

    @property
    def attachment_count(self) -> int:
        return len(self.attachment_paths)

    @property
    def task_description(self) -> str:
        return compact_text(self.prompt)

    @property
    def prompt_preview(self) -> str:
        text = self.task_description
        return text[:160] + ("…" if len(text) > 160 else "")


@dataclass(slots=True)
class ParsedAttachment:
    filename: str
    relative_path: str
    content: str
    cache_hit: bool
    num_pages: int | None
    credits_incurred: float
    cost_incurred_usd: float
    duration_seconds: float | None = None
    studio_link: str | None = None
    job_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ValueEstimate:
    hours_estimate: float
    value_low_usd: float
    value_base_usd: float
    value_high_usd: float
    source: str
    notes: str = ""
