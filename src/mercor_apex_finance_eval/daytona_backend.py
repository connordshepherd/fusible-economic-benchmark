from __future__ import annotations

import json
import shlex
from pathlib import Path, PurePosixPath
from typing import Any

from daytona_sdk import CreateSandboxFromImageParams, CreateSandboxFromSnapshotParams, Daytona
from daytona_sdk.common.image import Image

from .config import AppConfig
from .dataset import resolve_attachment_paths
from .types import ParsedAttachment, TaskRecord
from .utils import ensure_dir, jsonable


WORKSPACE_ROOT = PurePosixPath("/workspace")
INPUT_DIR = WORKSPACE_ROOT / "input"
RAW_DIR = INPUT_DIR / "raw_attachments"
PARSED_DIR = INPUT_DIR / "parsed_attachments"
OUTPUT_DIR = WORKSPACE_ROOT / "output"


class DaytonaSandboxRuntime:
    def __init__(self, config: AppConfig, *, sandbox_name: str) -> None:
        self.config = config
        self.client = Daytona()
        self.sandbox = self._create_sandbox(sandbox_name)

    def _create_sandbox(self, sandbox_name: str):
        labels = {
            "project": "economic-evals",
            "generation_mode": self.config.generation.mode,
            **self.config.daytona.labels,
        }
        if self.config.daytona.snapshot_name:
            params = CreateSandboxFromSnapshotParams(
                name=sandbox_name,
                language="python",
                os_user=self.config.daytona.os_user,
                env_vars=self.config.daytona.env_vars or None,
                labels=labels or None,
                auto_stop_interval=self.config.daytona.auto_stop_interval,
                auto_archive_interval=self.config.daytona.auto_archive_interval,
                auto_delete_interval=self.config.daytona.auto_delete_interval,
                network_block_all=self.config.daytona.network_block_all,
                network_allow_list=self.config.daytona.network_allow_list,
                ephemeral=self.config.daytona.ephemeral,
                snapshot=self.config.daytona.snapshot_name,
            )
            return self.client.create(params, timeout=1800)

        image = Image.base(self.config.daytona.base_image).workdir(str(WORKSPACE_ROOT))
        if self.config.daytona.preinstall_packages:
            image = image.pip_install(self.config.daytona.preinstall_packages)
        image = image.run_commands(
            f"mkdir -p {shlex.quote(str(RAW_DIR))}",
            f"mkdir -p {shlex.quote(str(PARSED_DIR))}",
            f"mkdir -p {shlex.quote(str(OUTPUT_DIR))}",
        )
        params = CreateSandboxFromImageParams(
            name=sandbox_name,
            language="python",
            os_user=self.config.daytona.os_user,
            env_vars=self.config.daytona.env_vars or None,
            labels=labels or None,
            auto_stop_interval=self.config.daytona.auto_stop_interval,
            auto_archive_interval=self.config.daytona.auto_archive_interval,
            auto_delete_interval=self.config.daytona.auto_delete_interval,
            network_block_all=self.config.daytona.network_block_all,
            network_allow_list=self.config.daytona.network_allow_list,
            ephemeral=self.config.daytona.ephemeral,
            image=image,
        )
        return self.client.create(params, timeout=1800)

    @property
    def sandbox_id(self) -> str:
        return str(self.sandbox.id)

    def close(self) -> None:
        try:
            self.sandbox.delete()
        except Exception:
            pass

    def ensure_dir(self, path: str) -> None:
        self.sandbox.process.exec(f"mkdir -p {shlex.quote(path)}", timeout=self.config.agent.tool_timeout_seconds)

    def upload_bytes(self, remote_path: str, content: bytes) -> None:
        parent = str(PurePosixPath(remote_path).parent)
        if parent and parent != ".":
            self.ensure_dir(parent)
        self.sandbox.fs.upload_file(content, remote_path)

    def upload_text(self, remote_path: str, content: str) -> None:
        self.upload_bytes(remote_path, content.encode("utf-8"))

    def upload_local_file(self, local_path: Path, remote_path: str) -> None:
        parent = str(PurePosixPath(remote_path).parent)
        if parent and parent != ".":
            self.ensure_dir(parent)
        self.sandbox.fs.upload_file(str(local_path), remote_path)

    def list_files(self, path: str) -> list[dict[str, Any]]:
        rows = []
        for item in self.sandbox.fs.list_files(path):
            row = jsonable(item)
            row.setdefault("path", str(PurePosixPath(path) / str(getattr(item, "name", row.get("name", "")))))
            rows.append(row)
        return rows

    def find_in_files(self, path: str, pattern: str, *, max_results: int) -> list[dict[str, Any]]:
        matches = [jsonable(match) for match in self.sandbox.fs.find_files(path, pattern)]
        return matches[:max_results]

    def read_text_file(self, path: str, *, start_line: int, max_lines: int, max_chars: int) -> dict[str, Any]:
        raw = self.sandbox.fs.download_file(path)
        text = raw.decode("utf-8", errors="replace")
        lines = text.splitlines()
        start_index = max(start_line - 1, 0)
        end_index = min(start_index + max_lines, len(lines))
        selected = lines[start_index:end_index]
        numbered = "\n".join(f"{idx}: {line}" for idx, line in enumerate(selected, start=start_index + 1))
        truncated = False
        if len(numbered) > max_chars:
            numbered = numbered[:max_chars]
            truncated = True
        return {
            "path": path,
            "start_line": start_index + 1,
            "end_line": end_index,
            "line_count": len(lines),
            "truncated": truncated,
            "content": numbered,
        }

    def write_text_file(self, path: str, content: str) -> dict[str, Any]:
        self.upload_text(path, content)
        return {
            "path": path,
            "bytes_written": len(content.encode("utf-8")),
        }

    def python_exec(self, code: str, *, cwd: str, timeout_seconds: int, max_output_chars: int) -> dict[str, Any]:
        self.ensure_dir(cwd)
        script_name = f".tool_python_{self.sandbox_id[:8]}.py"
        script_path = str(PurePosixPath(cwd) / script_name)
        self.upload_text(script_path, code)
        response = self.sandbox.process.exec(
            f"python {shlex.quote(script_path)}",
            cwd=cwd,
            timeout=timeout_seconds,
        )
        stdout = response.result or ""
        if len(stdout) > max_output_chars:
            stdout = stdout[:max_output_chars]
        return {
            "script_path": script_path,
            "exit_code": int(response.exit_code or 0),
            "stdout": stdout,
        }

    def download_text_if_exists(self, remote_path: str) -> str | None:
        try:
            return self.sandbox.fs.download_file(remote_path).decode("utf-8", errors="replace")
        except Exception:
            return None


def _safe_remote_name(raw_name: str) -> str:
    return raw_name.replace("/", "_")


def build_daytona_workspace(
    runtime: DaytonaSandboxRuntime,
    *,
    task: TaskRecord,
    dataset_dir: Path,
    parsed_attachments: list[ParsedAttachment],
    local_artifact_dir: Path,
) -> dict[str, Any]:
    ensure_dir(local_artifact_dir)
    manifest: dict[str, Any] = {
        "workspace_root": str(WORKSPACE_ROOT),
        "task_id": task.task_id,
        "domain": task.domain,
        "task_prompt_path": str(INPUT_DIR / "task_prompt.txt"),
        "attachment_manifest_path": str(INPUT_DIR / "attachment_manifest.json"),
        "raw_attachments_dir": str(RAW_DIR),
        "parsed_attachments_dir": str(PARSED_DIR),
        "output_dir": str(OUTPUT_DIR),
        "attachments": [],
    }

    runtime.upload_text(str(INPUT_DIR / "task_prompt.txt"), task.prompt)

    resolved = resolve_attachment_paths(dataset_dir, task)
    parsed_by_name = {str(Path(item.relative_path).resolve()): item for item in parsed_attachments}

    for source_path in resolved:
        relative = source_path.relative_to(dataset_dir).as_posix()
        parsed = parsed_by_name.get(str(source_path.resolve()))
        safe_name = _safe_remote_name(Path(relative).name)
        raw_remote_path = str(RAW_DIR / safe_name)
        runtime.upload_local_file(source_path, raw_remote_path)

        parsed_remote_path = None
        if parsed:
            parsed_remote_path = str(PARSED_DIR / f"{safe_name}.txt")
            runtime.upload_text(parsed_remote_path, parsed.content)

        manifest["attachments"].append(
            {
                "filename": Path(relative).name,
                "relative_path": relative,
                "raw_path": raw_remote_path,
                "parsed_text_path": parsed_remote_path,
                "num_pages": parsed.num_pages if parsed else None,
                "parse_cache_hit": parsed.cache_hit if parsed else None,
            }
        )

    runtime.upload_text(
        str(INPUT_DIR / "attachment_manifest.json"),
        json.dumps(manifest["attachments"], indent=2, ensure_ascii=False),
    )
    (local_artifact_dir / "workspace_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (local_artifact_dir / "task_prompt.txt").write_text(task.prompt, encoding="utf-8")
    return manifest


def build_tool_user_prompt(task: TaskRecord, workspace_manifest: dict[str, Any]) -> str:
    attachment_lines = []
    for item in workspace_manifest.get("attachments", []):
        summary = (
            f"- {item['filename']}: raw={item['raw_path']}"
            f", parsed={item.get('parsed_text_path') or '(none)'}"
            f", pages={item.get('num_pages')}"
        )
        attachment_lines.append(summary)
    attachment_block = "\n".join(attachment_lines) if attachment_lines else "(No attachments provided.)"

    return (
        f"Domain: {task.domain}\n\n"
        "Task prompt:\n"
        f"{task.prompt}\n\n"
        "Workspace:\n"
        f"- Task prompt file: {workspace_manifest['task_prompt_path']}\n"
        f"- Attachment manifest: {workspace_manifest['attachment_manifest_path']}\n"
        f"- Raw attachments directory: {workspace_manifest['raw_attachments_dir']}\n"
        f"- Parsed attachments directory: {workspace_manifest['parsed_attachments_dir']}\n"
        f"- Output directory: {workspace_manifest['output_dir']}\n\n"
        "Attachment paths:\n"
        f"{attachment_block}\n\n"
        "Use the tools when helpful, keep scratch work inside /workspace/output, and finish by writing the final deliverable to "
        "/workspace/output/final_answer.md and returning the same text as your final answer."
    )
