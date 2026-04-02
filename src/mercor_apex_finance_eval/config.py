from __future__ import annotations

from dataclasses import dataclass, field, asdict
import json
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class ModelSettings:
    model_id: str = "gpt-5.4"
    temperature: float | None = None
    max_tokens: int | None = None
    max_input_tokens: int | None = None
    model_configs: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class GraderSettings:
    model_id: str = "gemini-2.5-pro"
    temperature: float = 0.01
    max_tokens: int = 65535
    model_configs: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ReductoSettings:
    enabled: bool = True
    credit_price_usd: float = 0.015
    table_output_format: str = "html"
    chunk_mode: str = "variable"
    summarize_figures: bool = True
    agentic_scopes: list[str] = field(default_factory=list)
    filter_blocks: list[str] = field(default_factory=list)
    page_start: int | None = None
    page_end: int | None = None


@dataclass(slots=True)
class SelectionSettings:
    domain: str = "Finance"
    task_ids: list[int] = field(default_factory=list)
    start_index: int = 0
    limit: int | None = None


@dataclass(slots=True)
class EvaluationSettings:
    runs_per_task: int = 2
    min_overall_score_pct: float = 80.0
    require_all_primary: bool = True
    resume: bool = True
    save_response_text: bool = True
    save_score_summary: bool = True


@dataclass(slots=True)
class GenerationSettings:
    mode: str = "tool_assisted_daytona"


@dataclass(slots=True)
class AgentSettings:
    max_steps: int = 12
    max_tool_calls: int = 24
    tool_timeout_seconds: int = 60
    max_read_lines: int = 250
    max_find_results: int = 80
    max_tool_output_chars: int = 20000
    enable_python_exec: bool = True


DEFAULT_DAYTONA_PACKAGES = [
    "pandas",
    "numpy",
    "openpyxl",
    "pyarrow",
    "duckdb",
    "pypdf",
    "pdfplumber",
    "python-docx",
    "beautifulsoup4",
    "lxml",
    "rapidfuzz",
]


@dataclass(slots=True)
class DaytonaSettings:
    snapshot_name: str | None = None
    workspace_root: str | None = None
    base_image: str = "python:3.10-slim"
    preinstall_packages: list[str] = field(default_factory=lambda: list(DEFAULT_DAYTONA_PACKAGES))
    network_block_all: bool = True
    network_allow_list: str | None = None
    auto_stop_interval: int = 15
    auto_archive_interval: int | None = None
    auto_delete_interval: int | None = None
    ephemeral: bool = True
    os_user: str | None = None
    create_timeout_seconds: int = 300
    startup_timeout_seconds: int = 300
    create_retries: int = 2
    retry_backoff_seconds: float = 5.0
    labels: dict[str, str] = field(default_factory=dict)
    env_vars: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ValueModelSettings:
    overrides_csv: Path
    default_hours: float = 3.5
    low_rate: float = 100.0
    base_rate: float = 150.0
    high_rate: float = 250.0


@dataclass(slots=True)
class PricingSettings:
    openai_price_book: Path


@dataclass(slots=True)
class TrackingSettings:
    outputs_root: Path
    tracker_dir: Path
    task_metadata_csv: Path
    auto_refresh_on_run: bool = True


@dataclass(slots=True)
class AppConfig:
    dataset_dir: Path
    output_dir: Path
    parse_cache_dir: Path
    model: ModelSettings
    grader: GraderSettings
    reducto: ReductoSettings
    selection: SelectionSettings
    evaluation: EvaluationSettings
    generation: GenerationSettings
    agent: AgentSettings
    daytona: DaytonaSettings
    value_model: ValueModelSettings
    pricing: PricingSettings
    tracking: TrackingSettings
    config_path: Path | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _resolve(base_dir: Path, raw: str | None, default: str) -> Path:
    value = raw or default
    path = Path(value)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).resolve()
    data = json.loads(config_path.read_text(encoding="utf-8"))
    base = config_path.parent

    model_data = data.get("model", {})
    grader_data = data.get("grader", {})
    reducto_data = data.get("reducto", {})
    selection_data = data.get("selection", {})
    evaluation_data = data.get("evaluation", {})
    generation_data = data.get("generation", {})
    agent_data = data.get("agent", {})
    daytona_data = data.get("daytona", {})
    value_data = data.get("value_model", {})
    pricing_data = data.get("pricing", {})
    tracking_data = data.get("tracking", {})

    cfg = AppConfig(
        dataset_dir=_resolve(base, data.get("dataset_dir"), "data/APEX-v1-extended"),
        output_dir=_resolve(base, data.get("output_dir"), "outputs/apex_finance_run"),
        parse_cache_dir=_resolve(base, data.get("parse_cache_dir"), ".cache/reducto"),
        model=ModelSettings(
            model_id=model_data.get("model_id", "gpt-5.4"),
            temperature=model_data.get("temperature"),
            max_tokens=model_data.get("max_tokens"),
            max_input_tokens=model_data.get("max_input_tokens"),
            model_configs=model_data.get("model_configs", {}),
        ),
        grader=GraderSettings(
            model_id=grader_data.get("model_id", "gemini-2.5-pro"),
            temperature=grader_data.get("temperature", 0.01),
            max_tokens=grader_data.get("max_tokens", 65535),
            model_configs=grader_data.get("model_configs", {}),
        ),
        reducto=ReductoSettings(
            enabled=reducto_data.get("enabled", True),
            credit_price_usd=reducto_data.get("credit_price_usd", 0.015),
            table_output_format=reducto_data.get("table_output_format", "html"),
            chunk_mode=reducto_data.get("chunk_mode", "variable"),
            summarize_figures=reducto_data.get("summarize_figures", True),
            agentic_scopes=reducto_data.get("agentic_scopes", []),
            filter_blocks=reducto_data.get("filter_blocks", []),
            page_start=reducto_data.get("page_start"),
            page_end=reducto_data.get("page_end"),
        ),
        selection=SelectionSettings(
            domain=selection_data.get("domain", "Finance"),
            task_ids=[int(v) for v in selection_data.get("task_ids", [])],
            start_index=int(selection_data.get("start_index", 0)),
            limit=selection_data.get("limit"),
        ),
        evaluation=EvaluationSettings(
            runs_per_task=int(evaluation_data.get("runs_per_task", 2)),
            min_overall_score_pct=float(evaluation_data.get("min_overall_score_pct", 80.0)),
            require_all_primary=bool(evaluation_data.get("require_all_primary", True)),
            resume=bool(evaluation_data.get("resume", True)),
            save_response_text=bool(evaluation_data.get("save_response_text", True)),
            save_score_summary=bool(evaluation_data.get("save_score_summary", True)),
        ),
        generation=GenerationSettings(
            mode=str(generation_data.get("mode", "tool_assisted_daytona")),
        ),
        agent=AgentSettings(
            max_steps=int(agent_data.get("max_steps", 12)),
            max_tool_calls=int(agent_data.get("max_tool_calls", 24)),
            tool_timeout_seconds=int(agent_data.get("tool_timeout_seconds", 60)),
            max_read_lines=int(agent_data.get("max_read_lines", 250)),
            max_find_results=int(agent_data.get("max_find_results", 80)),
            max_tool_output_chars=int(agent_data.get("max_tool_output_chars", 20000)),
            enable_python_exec=bool(agent_data.get("enable_python_exec", True)),
        ),
        daytona=DaytonaSettings(
            snapshot_name=daytona_data.get("snapshot_name"),
            workspace_root=daytona_data.get("workspace_root"),
            base_image=str(daytona_data.get("base_image", "python:3.10-slim")),
            preinstall_packages=[str(v) for v in daytona_data.get("preinstall_packages", DEFAULT_DAYTONA_PACKAGES)],
            network_block_all=bool(daytona_data.get("network_block_all", True)),
            network_allow_list=daytona_data.get("network_allow_list"),
            auto_stop_interval=int(daytona_data.get("auto_stop_interval", 15)),
            auto_archive_interval=daytona_data.get("auto_archive_interval"),
            auto_delete_interval=daytona_data.get("auto_delete_interval"),
            ephemeral=bool(daytona_data.get("ephemeral", True)),
            os_user=daytona_data.get("os_user"),
            create_timeout_seconds=int(daytona_data.get("create_timeout_seconds", 300)),
            startup_timeout_seconds=int(daytona_data.get("startup_timeout_seconds", 300)),
            create_retries=int(daytona_data.get("create_retries", 2)),
            retry_backoff_seconds=float(daytona_data.get("retry_backoff_seconds", 5.0)),
            labels={str(k): str(v) for k, v in daytona_data.get("labels", {}).items()},
            env_vars={str(k): str(v) for k, v in daytona_data.get("env_vars", {}).items()},
        ),
        value_model=ValueModelSettings(
            overrides_csv=_resolve(base, value_data.get("overrides_csv"), "configs/finance_values.csv"),
            default_hours=float(value_data.get("default_hours", 3.5)),
            low_rate=float(value_data.get("low_rate", 100.0)),
            base_rate=float(value_data.get("base_rate", 150.0)),
            high_rate=float(value_data.get("high_rate", 250.0)),
        ),
        pricing=PricingSettings(
            openai_price_book=_resolve(base, pricing_data.get("openai_price_book"), "../configs/openai_pricing.json"),
        ),
        tracking=TrackingSettings(
            outputs_root=_resolve(base, tracking_data.get("outputs_root"), "../outputs"),
            tracker_dir=_resolve(base, tracking_data.get("tracker_dir"), "../tracker"),
            task_metadata_csv=_resolve(base, tracking_data.get("task_metadata_csv"), "../configs/task_metadata.csv"),
            auto_refresh_on_run=bool(tracking_data.get("auto_refresh_on_run", True)),
        ),
        config_path=config_path,
    )
    return cfg


def apply_overrides(
    config: AppConfig,
    *,
    output_dir: str | None = None,
    limit: int | None = None,
    task_ids: list[int] | None = None,
) -> AppConfig:
    if output_dir:
        config.output_dir = Path(output_dir).resolve()
    if limit is not None:
        config.selection.limit = limit
    if task_ids is not None and len(task_ids) > 0:
        config.selection.task_ids = task_ids
    return config
