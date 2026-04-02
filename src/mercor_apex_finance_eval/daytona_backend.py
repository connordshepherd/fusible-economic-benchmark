from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import time
import uuid
from pathlib import Path, PurePosixPath
from typing import Any, Callable

from daytona_api_client import CreateBuildInfo, CreateSandbox, SandboxVolume
from daytona_sdk import CreateSandboxFromImageParams, CreateSandboxFromSnapshotParams, Daytona
from daytona_sdk._sync.sandbox import Sandbox
from daytona_sdk._sync.snapshot import SnapshotService
from daytona_sdk._utils.timeout import http_timeout
from daytona_sdk.common.errors import DaytonaError, DaytonaTimeoutError
from daytona_sdk.common.image import Image

from .config import AppConfig
from .dataset import resolve_attachment_paths
from .types import ParsedAttachment, TaskRecord
from .utils import ensure_dir, jsonable, write_json


WORKSPACE_ROOT = PurePosixPath("/workspace")
INPUT_DIR = WORKSPACE_ROOT / "input"
RAW_DIR = INPUT_DIR / "raw_attachments"
PARSED_DIR = INPUT_DIR / "parsed_attachments"
OUTPUT_DIR = WORKSPACE_ROOT / "output"

TraceCallback = Callable[[str, dict[str, Any]], None]


def _safe_remote_name(raw_name: str) -> str:
    return raw_name.replace("/", "_")


def _noop_trace(_event: str, _payload: dict[str, Any]) -> None:
    return


_SANDBOX_DEBUG_FIELDS = (
    "id",
    "name",
    "state",
    "desired_state",
    "error_reason",
    "created_at",
    "updated_at",
    "runner_id",
    "target",
    "cpu",
    "memory",
    "disk",
    "snapshot",
    "toolbox_proxy_url",
    "user",
    "network_block_all",
    "auto_stop_interval",
    "labels",
)


def _debug_value(value: Any) -> Any:
    if hasattr(value, "value") and isinstance(getattr(value, "value"), (str, int, float, bool)):
        return getattr(value, "value")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _debug_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_debug_value(item) for item in value]
    return str(value)


def _state_text(value: Any) -> str:
    if hasattr(value, "value"):
        raw_value = getattr(value, "value")
        if raw_value is not None:
            return str(raw_value).lower()
    normalized = _debug_value(value)
    if normalized is None:
        return ""
    return str(normalized).lower()


def _sandbox_debug_snapshot(sandbox: Any | None) -> dict[str, Any]:
    if sandbox is None:
        return {}
    payload: dict[str, Any] = {}
    for field in _SANDBOX_DEBUG_FIELDS:
        if not hasattr(sandbox, field):
            continue
        value = getattr(sandbox, field)
        if value is None:
            continue
        payload[field] = _debug_value(value)
    return payload


class LocalWorkspaceRuntime:
    def __init__(self, local_root: Path, *, trace: TraceCallback | None = None) -> None:
        self.local_root = ensure_dir(local_root)
        self.trace = trace or _noop_trace
        ensure_dir(self.local_path(OUTPUT_DIR))
        ensure_dir(self.local_path(RAW_DIR))
        ensure_dir(self.local_path(PARSED_DIR))

    def virtual_path(self, raw_path: str | PurePosixPath) -> PurePosixPath:
        path = PurePosixPath(raw_path)
        if not path.is_absolute():
            path = WORKSPACE_ROOT / path
        try:
            path.relative_to(WORKSPACE_ROOT)
        except ValueError as exc:
            raise ValueError(f"Path must stay inside {WORKSPACE_ROOT}: {raw_path}") from exc
        return path

    def local_path(self, virtual_path: str | PurePosixPath) -> Path:
        path = self.virtual_path(virtual_path)
        return self.local_root / path.relative_to(WORKSPACE_ROOT)

    def virtual_root_str(self) -> str:
        return str(WORKSPACE_ROOT)

    def list_files(self, path: str) -> list[dict[str, Any]]:
        local_path = self.local_path(path)
        if not local_path.exists():
            raise FileNotFoundError(f"No such path: {path}")
        rows: list[dict[str, Any]] = []
        for item in sorted(local_path.iterdir(), key=lambda entry: entry.name):
            relative = item.relative_to(self.local_root).as_posix()
            rows.append(
                {
                    "name": item.name,
                    "path": str(WORKSPACE_ROOT / relative),
                    "is_dir": item.is_dir(),
                    "size": item.stat().st_size,
                }
            )
        return rows

    def read_text_file(self, path: str, *, start_line: int, max_lines: int, max_chars: int) -> dict[str, Any]:
        local_path = self.local_path(path)
        text = local_path.read_text(encoding="utf-8", errors="replace")
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
            "path": str(self.virtual_path(path)),
            "start_line": start_index + 1,
            "end_line": end_index,
            "line_count": len(lines),
            "truncated": truncated,
            "content": numbered,
        }

    def write_text_file(self, path: str, content: str) -> dict[str, Any]:
        local_path = self.local_path(path)
        ensure_dir(local_path.parent)
        local_path.write_text(content, encoding="utf-8")
        return {
            "path": str(self.virtual_path(path)),
            "bytes_written": len(content.encode("utf-8")),
        }

    def find_in_files(self, path: str, pattern: str, *, max_results: int) -> list[dict[str, Any]]:
        local_path = self.local_path(path)
        targets = [local_path] if local_path.is_file() else sorted(p for p in local_path.rglob("*") if p.is_file())
        matches: list[dict[str, Any]] = []
        for target in targets:
            try:
                text = target.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            for line_number, line in enumerate(text.splitlines(), start=1):
                if pattern in line:
                    relative = target.relative_to(self.local_root).as_posix()
                    matches.append(
                        {
                            "path": str(WORKSPACE_ROOT / relative),
                            "line_number": line_number,
                            "line_text": line[:500],
                        }
                    )
                    if len(matches) >= max_results:
                        return matches
        return matches

    def read_best_matches(
        self,
        path: str,
        query: str,
        *,
        max_results: int,
        context_lines: int,
        max_chars: int,
    ) -> list[dict[str, Any]]:
        local_path = self.local_path(path)
        targets = [local_path] if local_path.is_file() else sorted(p for p in local_path.rglob("*") if p.is_file())
        query_lower = query.lower()
        query_tokens = _query_tokens(query)
        windows: list[dict[str, Any]] = []
        for target in targets:
            try:
                text = target.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            lines = text.splitlines()
            if not lines:
                continue
            file_windows = _best_match_windows(
                lines,
                query=query_lower,
                query_tokens=query_tokens,
                max_results=max_results,
                context_lines=context_lines,
                max_chars=max_chars,
            )
            relative = target.relative_to(self.local_root).as_posix()
            for window in file_windows:
                windows.append(
                    {
                        "path": str(WORKSPACE_ROOT / relative),
                        **window,
                    }
                )
        windows.sort(
            key=lambda row: (
                -float(row["score"]),
                str(row["path"]),
                int(row["start_line"]),
            )
        )
        return windows[:max_results]

    def copy_local_file(self, source_path: Path, target_path: str) -> None:
        local_target = self.local_path(target_path)
        ensure_dir(local_target.parent)
        shutil.copy2(source_path, local_target)

    def final_answer_text(self) -> str | None:
        final_path = self.local_path(OUTPUT_DIR / "final_answer.md")
        if not final_path.exists():
            return None
        return final_path.read_text(encoding="utf-8", errors="replace")


_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "how",
    "if",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "the",
    "to",
    "what",
    "when",
    "where",
    "which",
    "who",
    "will",
    "with",
}


def _query_tokens(query: str) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()
    for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", query.lower()):
        if len(token) < 3 or token in _STOPWORDS or token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _window_score(window_text: str, *, query: str, query_tokens: list[str]) -> tuple[float, list[str]]:
    matched_terms: list[str] = []
    score = 0.0
    if query and query in window_text:
        score += 8.0
    for token in query_tokens:
        if token in window_text:
            matched_terms.append(token)
            score += 2.0
            extra_hits = min(window_text.count(token) - 1, 3)
            if extra_hits > 0:
                score += extra_hits * 0.25
    return score, matched_terms


def _best_match_windows(
    lines: list[str],
    *,
    query: str,
    query_tokens: list[str],
    max_results: int,
    context_lines: int,
    max_chars: int,
) -> list[dict[str, Any]]:
    candidates: list[tuple[float, int, int, list[str], str]] = []
    for idx, line in enumerate(lines):
        line_lower = line.lower()
        if query and query in line_lower:
            line_seed_score = 4.0
        else:
            line_seed_score = 0.0
            for token in query_tokens:
                if token in line_lower:
                    line_seed_score += 1.0
        if line_seed_score <= 0:
            continue
        start = max(0, idx - context_lines)
        end = min(len(lines), idx + context_lines + 1)
        window_lines = lines[start:end]
        window_text = "\n".join(window_lines).lower()
        window_score, matched_terms = _window_score(window_text, query=query, query_tokens=query_tokens)
        score = line_seed_score + window_score
        if score <= 0:
            continue
        numbered = "\n".join(f"{line_no}: {text}" for line_no, text in enumerate(window_lines, start=start + 1))
        if len(numbered) > max_chars:
            numbered = numbered[:max_chars]
        candidates.append((score, start + 1, end, matched_terms, numbered))

    deduped: dict[tuple[int, int], tuple[float, list[str], str]] = {}
    for score, start_line, end_line, matched_terms, content in candidates:
        key = (start_line, end_line)
        existing = deduped.get(key)
        if existing is None or score > existing[0]:
            deduped[key] = (score, matched_terms, content)

    ranked = sorted(
        (
            {
                "score": round(score, 3),
                "matched_terms": matched_terms,
                "start_line": start_line,
                "end_line": end_line,
                "content": content,
            }
            for (start_line, end_line), (score, matched_terms, content) in deduped.items()
        ),
        key=lambda row: (-float(row["score"]), int(row["start_line"])),
    )
    return ranked[:max_results]


class DaytonaPythonExecutor:
    def __init__(self, config: AppConfig, *, trace: TraceCallback | None = None) -> None:
        self.config = config
        self.trace = trace or _noop_trace
        self.remote_workspace_root = PurePosixPath(config.daytona.workspace_root or str(WORKSPACE_ROOT))
        self.client: Daytona | None = None
        self.sandbox: Sandbox | None = None
        self.sandbox_name: str | None = None

    @property
    def sandbox_id(self) -> str | None:
        if self.sandbox is None:
            return None
        return str(self.sandbox.id)

    def _emit(self, event: str, **payload: Any) -> None:
        self.trace(event, payload)

    def _emit_sandbox_snapshot(self, event: str, sandbox: Any | None, **payload: Any) -> None:
        self._emit(event, **payload, **_sandbox_debug_snapshot(sandbox))

    def _emit_directory_snapshot(self, *, reason: str, sandbox_id: str | None, sandbox_name: str | None) -> None:
        assert self.client is not None
        try:
            listing = self.client.list()
        except Exception as exc:
            self._emit(
                "daytona_directory_snapshot_error",
                reason=reason,
                sandbox_id=sandbox_id or "",
                sandbox_name=sandbox_name or "",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            return

        match = None
        for item in listing.items:
            if sandbox_id and str(getattr(item, "id", "")) == sandbox_id:
                match = item
                break
            if sandbox_name and str(getattr(item, "name", "")) == sandbox_name:
                match = item
                break
        self._emit_sandbox_snapshot(
            "daytona_directory_snapshot",
            match,
            reason=reason,
            sandbox_id=sandbox_id or "",
            sandbox_name=sandbox_name or "",
            found=match is not None,
            total_items=len(listing.items),
        )

    def _probe_toolbox(self, sandbox: Sandbox, *, reason: str, timeout_seconds: int = 10) -> None:
        started = time.perf_counter()
        self._emit_sandbox_snapshot(
            "daytona_toolbox_probe_start",
            sandbox,
            reason=reason,
            timeout_seconds=timeout_seconds,
        )
        try:
            result = sandbox.process.exec("pwd", timeout=timeout_seconds)
        except Exception as exc:
            self._emit_sandbox_snapshot(
                "daytona_toolbox_probe_error",
                sandbox,
                reason=reason,
                timeout_seconds=timeout_seconds,
                duration_seconds=time.perf_counter() - started,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            return

        stdout = (result.result or "")[:500]
        self._emit_sandbox_snapshot(
            "daytona_toolbox_probe_end",
            sandbox,
            reason=reason,
            timeout_seconds=timeout_seconds,
            duration_seconds=time.perf_counter() - started,
            exit_code=int(result.exit_code or 0),
            stdout=stdout,
        )

    def _remote_path(self, virtual_path: str | PurePosixPath) -> PurePosixPath:
        path = PurePosixPath(virtual_path)
        if not path.is_absolute():
            path = WORKSPACE_ROOT / path
        relative = path.relative_to(WORKSPACE_ROOT)
        return self.remote_workspace_root / relative

    def _sandbox_from_response(self, response: Any, *, code_toolbox: Any) -> Sandbox:
        assert self.client is not None
        return Sandbox(
            response,
            self.client._toolbox_api_client,
            self.client._sandbox_api,
            code_toolbox,
        )

    def _create_params(self, sandbox_name: str) -> CreateSandboxFromSnapshotParams | CreateSandboxFromImageParams:
        labels = {
            "project": "economic-evals",
            "generation_mode": self.config.generation.mode,
            **self.config.daytona.labels,
        }
        if self.config.daytona.snapshot_name:
            return CreateSandboxFromSnapshotParams(
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

        image = Image.base(self.config.daytona.base_image).workdir(str(self.remote_workspace_root))
        if self.config.daytona.preinstall_packages:
            image = image.pip_install(self.config.daytona.preinstall_packages)
        image = image.run_commands(
            f"mkdir -p {shlex.quote(str(self._remote_path(RAW_DIR)))}",
            f"mkdir -p {shlex.quote(str(self._remote_path(PARSED_DIR)))}",
            f"mkdir -p {shlex.quote(str(self._remote_path(OUTPUT_DIR)))}",
        )
        return CreateSandboxFromImageParams(
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

    def _wait_for_started(self, sandbox: Sandbox, *, code_toolbox: Any) -> Sandbox:
        assert self.client is not None
        deadline = time.monotonic() + max(float(self.config.daytona.startup_timeout_seconds), 1.0)
        wait_started = time.perf_counter()
        last_signature: tuple[str, str, str, str] | None = None
        last_emit_at = 0.0
        poll_count = 0
        self._emit_sandbox_snapshot(
            "daytona_wait_started_begin",
            sandbox,
            timeout_seconds=float(self.config.daytona.startup_timeout_seconds),
        )
        while _state_text(getattr(sandbox, "state", None)) != "started":
            signature = (
                str(getattr(sandbox, "state", "")),
                str(getattr(sandbox, "desired_state", "")),
                str(getattr(sandbox, "error_reason", "")),
                str(getattr(sandbox, "updated_at", "")),
            )
            now = time.monotonic()
            if signature != last_signature or (now - last_emit_at) >= 5.0:
                self._emit_sandbox_snapshot(
                    "daytona_sandbox_state_poll",
                    sandbox,
                    poll_count=poll_count,
                    waited_seconds=time.perf_counter() - wait_started,
                )
                last_signature = signature
                last_emit_at = now
            if _state_text(getattr(sandbox, "state", None)) in {"error", "build_failed"}:
                self._emit_sandbox_snapshot(
                    "daytona_wait_started_failed_state",
                    sandbox,
                    poll_count=poll_count,
                    waited_seconds=time.perf_counter() - wait_started,
                )
                raise DaytonaError(
                    f"Sandbox {sandbox.id} failed to start with state={sandbox.state} error={sandbox.error_reason!r}"
                )
            if time.monotonic() >= deadline:
                self._emit_sandbox_snapshot(
                    "daytona_wait_started_timeout",
                    sandbox,
                    poll_count=poll_count,
                    waited_seconds=time.perf_counter() - wait_started,
                )
                self._probe_toolbox(sandbox, reason="wait_started_timeout", timeout_seconds=10)
                self._emit_directory_snapshot(
                    reason="wait_started_timeout",
                    sandbox_id=str(getattr(sandbox, "id", "")),
                    sandbox_name=str(getattr(sandbox, "name", "")),
                )
                raise DaytonaTimeoutError(
                    f"Sandbox {sandbox.id} did not reach started within "
                    f"{self.config.daytona.startup_timeout_seconds} seconds."
                )
            response = self.client._sandbox_api.get_sandbox(
                sandbox.id,
                _request_timeout=http_timeout(min(30.0, max(deadline - time.monotonic(), 1.0))),
            )
            sandbox = self._sandbox_from_response(response, code_toolbox=code_toolbox)
            poll_count += 1
            time.sleep(0.2)
        self._emit_sandbox_snapshot(
            "daytona_wait_started_end",
            sandbox,
            poll_count=poll_count,
            waited_seconds=time.perf_counter() - wait_started,
        )
        return sandbox

    def _wait_for_toolbox_ready(self) -> None:
        assert self.sandbox is not None
        deadline = time.monotonic() + max(min(float(self.config.daytona.startup_timeout_seconds), 120.0), 10.0)
        last_error: Exception | None = None
        poll_count = 0
        last_status: tuple[str, str] | None = None
        last_emit_at = 0.0
        self._emit_sandbox_snapshot(
            "daytona_toolbox_ready_start",
            self.sandbox,
            sandbox_id=self.sandbox_id,
            timeout_seconds=max(min(float(self.config.daytona.startup_timeout_seconds), 120.0), 10.0),
        )
        started = time.perf_counter()
        while time.monotonic() < deadline:
            poll_count += 1
            try:
                result = self.sandbox.process.exec("pwd", timeout=10)
                if int(result.exit_code or 0) == 0:
                    self._emit_sandbox_snapshot(
                        "daytona_toolbox_ready_end",
                        self.sandbox,
                        sandbox_id=self.sandbox_id,
                        poll_count=poll_count,
                        duration_seconds=time.perf_counter() - started,
                        stdout=(result.result or "")[:500],
                    )
                    return
                status = (str(result.exit_code), (result.result or "")[:120])
                now = time.monotonic()
                if status != last_status or (now - last_emit_at) >= 5.0:
                    self._emit_sandbox_snapshot(
                        "daytona_toolbox_ready_poll",
                        self.sandbox,
                        sandbox_id=self.sandbox_id,
                        poll_count=poll_count,
                        duration_seconds=time.perf_counter() - started,
                        exit_code=int(result.exit_code or 0),
                        stdout=(result.result or "")[:500],
                    )
                    last_status = status
                    last_emit_at = now
            except Exception as exc:
                last_error = exc
                error_signature = (type(exc).__name__, str(exc))
                now = time.monotonic()
                if error_signature != last_status or (now - last_emit_at) >= 5.0:
                    self._emit_sandbox_snapshot(
                        "daytona_toolbox_ready_poll_error",
                        self.sandbox,
                        sandbox_id=self.sandbox_id,
                        poll_count=poll_count,
                        duration_seconds=time.perf_counter() - started,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    last_status = error_signature
                    last_emit_at = now
            time.sleep(1.0)
        self._emit_sandbox_snapshot(
            "daytona_toolbox_ready_timeout",
            self.sandbox,
            sandbox_id=self.sandbox_id,
            poll_count=poll_count,
            duration_seconds=time.perf_counter() - started,
            error_type=type(last_error).__name__ if last_error else "",
            error_message=str(last_error) if last_error else "",
        )
        self._emit_directory_snapshot(
            reason="toolbox_ready_timeout",
            sandbox_id=self.sandbox_id,
            sandbox_name=self.sandbox_name,
        )
        raise RuntimeError(f"Daytona toolbox did not become ready before timeout: {last_error}")

    def _recover_existing_sandbox(
        self,
        sandbox_name: str,
        *,
        code_toolbox: Any,
        timeout_seconds: float,
    ) -> Sandbox | None:
        assert self.client is not None
        effective_timeout = min(timeout_seconds, 15.0)
        self._emit(
            "daytona_sandbox_recover_start",
            sandbox_name=sandbox_name,
            timeout_seconds=effective_timeout,
        )
        try:
            response = self.client._sandbox_api.get_sandbox(
                sandbox_name,
                _request_timeout=http_timeout(effective_timeout),
            )
        except Exception as exc:
            self._emit(
                "daytona_sandbox_recover_error",
                sandbox_name=sandbox_name,
                timeout_seconds=effective_timeout,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return None
        self._emit(
            "daytona_sandbox_recover_end",
            sandbox_name=sandbox_name,
            timeout_seconds=effective_timeout,
        )
        sandbox = self._sandbox_from_response(response, code_toolbox=code_toolbox)
        self._emit_sandbox_snapshot(
            "daytona_sandbox_recover_snapshot",
            sandbox,
            sandbox_name=sandbox_name,
            timeout_seconds=effective_timeout,
        )
        return sandbox

    def _ensure_sandbox(self) -> Sandbox:
        if self.sandbox is not None:
            self._emit_sandbox_snapshot(
                "daytona_sandbox_reuse",
                self.sandbox,
                sandbox_id=self.sandbox_id,
            )
            return self.sandbox
        if not os.getenv("DAYTONA_API_KEY"):
            raise EnvironmentError("python_exec requires DAYTONA_API_KEY.")

        self.client = Daytona()
        self.sandbox_name = f"apex-task-python-{uuid.uuid4().hex[:8]}"
        params = self._create_params(self.sandbox_name)
        code_toolbox = self.client._get_code_toolbox(params.language)
        timeout = float(self.config.daytona.create_timeout_seconds)
        attempts = max(self.config.daytona.create_retries, 1)
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            create_started = time.perf_counter()
            self._emit(
                "daytona_sandbox_create_start",
                sandbox_name=self.sandbox_name,
                attempt=attempt,
                snapshot_name=self.config.daytona.snapshot_name or "",
            )
            try:
                target = self.client._target
                volumes = []
                if getattr(params, "volumes", None):
                    volumes = [
                        SandboxVolume(volume_id=volume.volume_id, mount_path=volume.mount_path, subpath=volume.subpath)
                        for volume in params.volumes
                    ]
                sandbox_data = CreateSandbox(
                    name=params.name,
                    user=params.os_user,
                    env=params.env_vars if params.env_vars else {},
                    labels=params.labels,
                    public=params.public,
                    target=str(target) if target else None,
                    auto_stop_interval=params.auto_stop_interval,
                    auto_archive_interval=params.auto_archive_interval,
                    auto_delete_interval=params.auto_delete_interval,
                    volumes=volumes,
                    network_block_all=params.network_block_all,
                    network_allow_list=params.network_allow_list,
                )
                if isinstance(params, CreateSandboxFromSnapshotParams) and params.snapshot:
                    sandbox_data.snapshot = params.snapshot
                if isinstance(params, CreateSandboxFromImageParams) and params.image:
                    if isinstance(params.image, str):
                        sandbox_data.build_info = CreateBuildInfo(
                            dockerfile_content=Image.base(params.image).dockerfile(),
                        )
                    else:
                        context_hashes = SnapshotService.process_image_context(self.client._object_storage_api, params.image)
                        sandbox_data.build_info = CreateBuildInfo(
                            context_hashes=context_hashes,
                            dockerfile_content=params.image.dockerfile(),
                        )
                    if params.resources:
                        sandbox_data.cpu = params.resources.cpu
                        sandbox_data.memory = params.resources.memory
                        sandbox_data.disk = params.resources.disk
                        sandbox_data.gpu = params.resources.gpu

                response = self.client._sandbox_api.create_sandbox(
                    sandbox_data,
                    _request_timeout=http_timeout(timeout),
                )
                sandbox = self._sandbox_from_response(response, code_toolbox=code_toolbox)
                self._emit_sandbox_snapshot(
                    "daytona_sandbox_create_response",
                    sandbox,
                    sandbox_name=self.sandbox_name,
                    attempt=attempt,
                    duration_seconds=time.perf_counter() - create_started,
                )
                sandbox = self._wait_for_started(sandbox, code_toolbox=code_toolbox)
                self.sandbox = sandbox
                self._emit_sandbox_snapshot(
                    "daytona_sandbox_create_end",
                    self.sandbox,
                    sandbox_name=self.sandbox_name,
                    sandbox_id=self.sandbox_id,
                    attempt=attempt,
                    duration_seconds=time.perf_counter() - create_started,
                )
                self._wait_for_toolbox_ready()
                return self.sandbox
            except Exception as exc:
                last_error = exc
                self._emit(
                    "daytona_sandbox_create_error",
                    sandbox_name=self.sandbox_name,
                    attempt=attempt,
                    duration_seconds=time.perf_counter() - create_started,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                recovered = self._recover_existing_sandbox(
                    params.name,
                    code_toolbox=code_toolbox,
                    timeout_seconds=timeout,
                )
                if recovered is not None:
                    try:
                        self.sandbox = self._wait_for_started(recovered, code_toolbox=code_toolbox)
                        self._wait_for_toolbox_ready()
                        return self.sandbox
                    except Exception as recovered_exc:
                        last_error = recovered_exc
                        self._emit_sandbox_snapshot(
                            "daytona_sandbox_recover_failed",
                            recovered,
                            sandbox_name=self.sandbox_name,
                            attempt=attempt,
                            error_type=type(recovered_exc).__name__,
                            error_message=str(recovered_exc),
                        )
                if attempt >= attempts:
                    break
                time.sleep(max(self.config.daytona.retry_backoff_seconds, 0.0))

        raise RuntimeError(
            f"Failed to create Daytona sandbox after {attempts} attempt(s): {last_error}"
        ) from last_error

    def _ensure_remote_dir(self, remote_path: PurePosixPath) -> None:
        sandbox = self._ensure_sandbox()
        sandbox.process.exec(
            f"mkdir -p {shlex.quote(str(remote_path))}",
            timeout=self.config.agent.tool_timeout_seconds,
        )

    def _upload_bytes(self, remote_path: PurePosixPath, content: bytes) -> None:
        sandbox = self._ensure_sandbox()
        self._ensure_remote_dir(remote_path.parent)
        sandbox.fs.upload_file(content, str(remote_path))

    def _sync_workspace_to_remote(self, workspace: LocalWorkspaceRuntime) -> dict[str, Any]:
        sandbox = self._ensure_sandbox()
        started = time.perf_counter()
        self._emit(
            "daytona_sync_local_to_remote_start",
            sandbox_id=self.sandbox_id,
            local_root=str(workspace.local_root),
            remote_root=str(self.remote_workspace_root),
        )
        file_count = 0
        total_bytes = 0
        for local_path in sorted(path for path in workspace.local_root.rglob("*") if path.is_file()):
            relative = local_path.relative_to(workspace.local_root)
            remote_path = self.remote_workspace_root / relative.as_posix()
            self._ensure_remote_dir(remote_path.parent)
            sandbox.fs.upload_file(str(local_path), str(remote_path))
            file_count += 1
            total_bytes += local_path.stat().st_size
        payload = {
            "file_count": file_count,
            "total_bytes": total_bytes,
            "duration_seconds": time.perf_counter() - started,
        }
        self._emit("daytona_sync_local_to_remote_end", **payload)
        return payload

    def _sync_remote_output_to_local(self, workspace: LocalWorkspaceRuntime) -> dict[str, Any]:
        sandbox = self._ensure_sandbox()
        started = time.perf_counter()
        remote_output = self._remote_path(OUTPUT_DIR)
        local_output_root = workspace.local_path(OUTPUT_DIR)
        ensure_dir(local_output_root)
        self._emit(
            "daytona_sync_remote_output_start",
            sandbox_id=self.sandbox_id,
            remote_output_dir=str(remote_output),
            local_output_dir=str(local_output_root),
        )

        result = sandbox.process.exec(
            f"find {shlex.quote(str(remote_output))} -type f -printf '%P\\n'",
            timeout=self.config.agent.tool_timeout_seconds,
        )
        if int(result.exit_code or 0) != 0:
            payload = {
                "file_count": 0,
                "total_bytes": 0,
                "duration_seconds": time.perf_counter() - started,
            }
            self._emit("daytona_sync_remote_output_end", **payload)
            return payload

        file_count = 0
        total_bytes = 0
        for line in (result.result or "").splitlines():
            relative = line.strip()
            if not relative:
                continue
            remote_file = remote_output / relative
            local_file = local_output_root / relative
            ensure_dir(local_file.parent)
            content = sandbox.fs.download_file(str(remote_file))
            local_file.write_bytes(content)
            file_count += 1
            total_bytes += len(content)

        payload = {
            "file_count": file_count,
            "total_bytes": total_bytes,
            "duration_seconds": time.perf_counter() - started,
        }
        self._emit("daytona_sync_remote_output_end", **payload)
        return payload

    def python_exec(
        self,
        workspace: LocalWorkspaceRuntime,
        code: str,
        *,
        cwd: str,
        timeout_seconds: int,
        max_output_chars: int,
    ) -> dict[str, Any]:
        sandbox = self._ensure_sandbox()
        sync_up = self._sync_workspace_to_remote(workspace)
        virtual_cwd = workspace.virtual_path(cwd)
        remote_cwd = self._remote_path(virtual_cwd)
        self._ensure_remote_dir(remote_cwd)
        script_name = f".tool_python_{(self.sandbox_id or 'sandbox')[:8]}_{uuid.uuid4().hex[:8]}.py"
        script_path = remote_cwd / script_name
        self._upload_bytes(script_path, code.encode("utf-8"))

        exec_started = time.perf_counter()
        self._emit(
            "daytona_python_exec_start",
            sandbox_id=self.sandbox_id,
            cwd=str(remote_cwd),
            timeout_seconds=timeout_seconds,
            script_path=str(script_path),
        )
        response = sandbox.process.exec(
            f"python {shlex.quote(str(script_path))}",
            cwd=str(remote_cwd),
            timeout=timeout_seconds,
        )
        exec_payload = {
            "sandbox_id": self.sandbox_id,
            "cwd": str(remote_cwd),
            "script_path": str(script_path),
            "exit_code": int(response.exit_code or 0),
            "duration_seconds": time.perf_counter() - exec_started,
        }
        self._emit("daytona_python_exec_end", **exec_payload)
        sync_down = self._sync_remote_output_to_local(workspace)

        stdout = response.result or ""
        if len(stdout) > max_output_chars:
            stdout = stdout[:max_output_chars]
        return {
            "script_path": str(virtual_cwd / script_name),
            "exit_code": int(response.exit_code or 0),
            "stdout": stdout,
            "sandbox_id": self.sandbox_id,
            "sync_up": sync_up,
            "sync_down": sync_down,
        }

    def close(self) -> None:
        if self.sandbox is None:
            return
        try:
            self.sandbox.delete()
        except Exception:
            pass
        finally:
            self.sandbox = None


def build_local_workspace(
    runtime: LocalWorkspaceRuntime,
    *,
    task: TaskRecord,
    dataset_dir: Path,
    parsed_attachments: list[ParsedAttachment],
    local_artifact_dir: Path,
    trace: TraceCallback | None = None,
) -> dict[str, Any]:
    tracer = trace or _noop_trace
    started = time.perf_counter()
    tracer(
        "local_workspace_build_start",
        {
            "task_id": task.task_id,
            "workspace_root": str(WORKSPACE_ROOT),
            "local_workspace_root": str(runtime.local_root),
        },
    )
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

    runtime.write_text_file(str(INPUT_DIR / "task_prompt.txt"), task.prompt)

    resolved = resolve_attachment_paths(dataset_dir, task)
    parsed_by_name = {str(Path(item.relative_path).resolve()): item for item in parsed_attachments}
    raw_file_count = 0
    raw_total_bytes = 0

    for source_path in resolved:
        relative = source_path.relative_to(dataset_dir).as_posix()
        parsed = parsed_by_name.get(str(source_path.resolve()))
        safe_name = _safe_remote_name(Path(relative).name)
        raw_virtual_path = str(RAW_DIR / safe_name)
        runtime.copy_local_file(source_path, raw_virtual_path)
        raw_file_count += 1
        raw_total_bytes += source_path.stat().st_size

        parsed_virtual_path = None
        if parsed:
            parsed_virtual_path = str(PARSED_DIR / f"{safe_name}.txt")
            runtime.write_text_file(parsed_virtual_path, parsed.content)

        manifest["attachments"].append(
            {
                "filename": Path(relative).name,
                "relative_path": relative,
                "raw_path": raw_virtual_path,
                "parsed_text_path": parsed_virtual_path,
                "num_pages": parsed.num_pages if parsed else None,
                "parse_cache_hit": parsed.cache_hit if parsed else None,
            }
        )

    runtime.write_text_file(
        str(INPUT_DIR / "attachment_manifest.json"),
        json.dumps(manifest["attachments"], indent=2, ensure_ascii=False),
    )
    write_json(local_artifact_dir / "workspace_manifest.json", manifest)
    (local_artifact_dir / "task_prompt.txt").write_text(task.prompt, encoding="utf-8")
    tracer(
        "local_workspace_build_end",
        {
            "task_id": task.task_id,
            "raw_file_count": raw_file_count,
            "raw_total_bytes": raw_total_bytes,
            "duration_seconds": time.perf_counter() - started,
        },
    )
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
        "The file tools operate on the local workspace. Use python_exec only when you need isolated code execution. "
        "Keep scratch work inside the output directory, and finish by writing the final deliverable to "
        f"{workspace_manifest['output_dir']}/final_answer.md and returning the same text as your final answer."
    )
