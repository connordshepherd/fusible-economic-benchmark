from __future__ import annotations

from importlib import resources
from string import Template

from .types import ParsedAttachment, TaskRecord


def _read_prompt(name: str) -> str:
    return resources.files("mercor_apex_finance_eval.prompts").joinpath(name).read_text(encoding="utf-8")


def generation_system_prompt() -> str:
    return _read_prompt("generation_system_prompt.txt")


def tool_agent_system_prompt() -> str:
    return _read_prompt("tool_agent_system_prompt.txt")


def grading_prompt() -> str:
    return _read_prompt("grading_prompt.txt")


def _attachment_block(attachments: list[ParsedAttachment]) -> str:
    if not attachments:
        return "(No attachments provided.)"

    parts = ["==== Attached files content ===="]
    for attachment in attachments:
        parts.append(f"=== {attachment.filename} ===")
        parts.append(attachment.content)
        parts.append("")
    return "\n".join(parts).strip()


def generation_user_prompt(task: TaskRecord, attachments: list[ParsedAttachment]) -> str:
    template = Template(_read_prompt("generation_user_prompt.txt"))
    return template.safe_substitute(
        domain=task.domain,
        task_prompt=task.prompt,
        attachments=_attachment_block(attachments),
    )
