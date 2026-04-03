from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from .provenance import TASK_PROVENANCE_HEADERS, build_task_provenance_rows


DATABASE_URL_ENV_ORDER = [
    "DATABASE_URL_UNPOOLED",
    "POSTGRES_URL_NON_POOLING",
    "DATABASE_URL",
    "POSTGRES_URL",
    "POSTGRES_PRISMA_URL",
]

TASK_PROVENANCE_COLUMNS = list(TASK_PROVENANCE_HEADERS)

TASK_SETUP_COLUMNS = [
    "setup_id",
    "task_id",
    "domain",
    "task_source",
    "provenance_id",
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
    "generation_mode",
    "model_id",
    "generation_reasoning_effort",
    "generation_verbosity",
    "judge_model_id",
    "judge_reasoning_effort",
    "judge_verbosity",
    "generation_prompt_fingerprint",
    "grading_prompt_fingerprint",
    "agent_budget",
    "mean_generation_steps_used",
    "tools_used",
    "price_book_id_current",
    "promoted_attempts",
    "completed_runs",
    "business_passes",
    "pass_rate",
    "mean_score_pct",
    "mean_generation_cost_per_attempt_usd",
    "mean_grading_cost_per_attempt_usd",
    "mean_total_cost_per_attempt_usd",
    "mean_cost_of_successful_attempts_usd",
    "cost_per_success_usd",
    "hours_estimate",
    "value_base_usd",
    "expected_net_base_usd_per_attempt",
    "latest_attempt_completed_at",
    "promotion_labels",
]

PROMOTED_ATTEMPT_COLUMNS = [
    "attempt_key",
    "promoted",
    "headline",
    "promotion_label",
    "promotion_notes",
    "promoted_at",
    "run_jsonl_path",
    "output_dir",
    "run_manifest_path",
    "task_id",
    "run_index",
    "domain",
    "task_source",
    "provenance_id",
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
    "status",
    "business_pass",
    "score_pct",
    "model_id",
    "generation_reasoning_effort",
    "generation_verbosity",
    "judge_model_id",
    "judge_reasoning_effort",
    "judge_verbosity",
    "generation_mode",
    "agent_budget",
    "generation_steps_used",
    "tools_used",
    "generation_prompt_fingerprint",
    "grading_prompt_fingerprint",
    "setup_id",
    "price_book_id_current",
    "value_base_usd",
    "hours_estimate",
    "generation_input_tokens",
    "generation_cached_input_tokens",
    "generation_output_tokens",
    "grading_input_tokens",
    "grading_cached_input_tokens",
    "grading_output_tokens",
    "current_generation_cost_usd",
    "current_grading_cost_usd",
    "current_total_cost_usd",
    "recorded_total_cost_usd",
    "attempt_completed_at",
]


def resolve_database_url(explicit_url: str | None = None) -> str:
    if explicit_url:
        return explicit_url

    for key in DATABASE_URL_ENV_ORDER:
        value = os.getenv(key)
        if value:
            return value

    raise EnvironmentError(
        "Could not find a Postgres connection string. "
        f"Checked: {', '.join(DATABASE_URL_ENV_ORDER)}"
    )


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_overall(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped or None


def _int(value: str | None) -> int | None:
    parsed = _text(value)
    return int(parsed) if parsed is not None else None


def _float(value: str | None) -> float | None:
    parsed = _text(value)
    return float(parsed) if parsed is not None else None


def _bool(value: str | None) -> bool | None:
    parsed = _text(value)
    if parsed is None:
        return None
    lowered = parsed.lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    raise ValueError(f"Could not parse boolean value: {value!r}")


def _dt(value: str | None) -> datetime | None:
    parsed = _text(value)
    return datetime.fromisoformat(parsed) if parsed is not None else None


def _insert_rows(
    cur: psycopg.Cursor[Any],
    *,
    schema: str,
    table: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
) -> None:
    if not rows:
        return

    cur.executemany(
        sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        rows,
    )


def _ensure_schema(conn: psycopg.Connection[Any], *, schema: str) -> None:
    schema_ident = sql.Identifier(schema)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema_ident))

        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.task_provenances (
                    provenance_id text PRIMARY KEY,
                    task_source text NOT NULL,
                    source_type text NOT NULL,
                    source_provider text,
                    dataset_name text,
                    dataset_version text,
                    dataset_split text,
                    access_level text,
                    source_reference text,
                    source_url text,
                    notes text,
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            ).format(schema_ident)
        )

        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.task_setups (
                    task_id text NOT NULL,
                    setup_id text NOT NULL,
                    domain text NOT NULL,
                    task_source text NOT NULL,
                    provenance_id text NOT NULL REFERENCES {}.task_provenances (provenance_id),
                    job text,
                    task_description text NOT NULL,
                    success_criteria text,
                    attachment_count integer NOT NULL,
                    attachment_total_bytes bigint NOT NULL,
                    attachment_total_mb double precision NOT NULL,
                    largest_attachment_bytes bigint NOT NULL,
                    attachment_extensions text,
                    attachment_paths text,
                    prompt_char_count integer NOT NULL,
                    prompt_word_count integer NOT NULL,
                    criterion_count integer NOT NULL,
                    primary_criteria_count integer NOT NULL,
                    secondary_criteria_count integer NOT NULL,
                    criteria_with_sources_count integer NOT NULL,
                    criterion_types text,
                    generation_mode text NOT NULL,
                    model_id text NOT NULL,
                    generation_reasoning_effort text,
                    generation_verbosity text,
                    judge_model_id text NOT NULL,
                    judge_reasoning_effort text,
                    judge_verbosity text,
                    generation_prompt_fingerprint text,
                    grading_prompt_fingerprint text,
                    agent_budget text,
                    mean_generation_steps_used double precision,
                    tools_used text,
                    price_book_id_current text,
                    promoted_attempts integer NOT NULL,
                    completed_runs integer NOT NULL,
                    business_passes integer NOT NULL,
                    pass_rate double precision NOT NULL,
                    mean_score_pct double precision NOT NULL,
                    mean_generation_cost_per_attempt_usd double precision NOT NULL,
                    mean_grading_cost_per_attempt_usd double precision NOT NULL,
                    mean_total_cost_per_attempt_usd double precision NOT NULL,
                    mean_cost_of_successful_attempts_usd double precision,
                    cost_per_success_usd double precision,
                    hours_estimate double precision NOT NULL,
                    value_base_usd double precision NOT NULL,
                    expected_net_base_usd_per_attempt double precision NOT NULL,
                    latest_attempt_completed_at timestamptz,
                    promotion_labels text,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (task_id, setup_id)
                )
                """
            ).format(schema_ident, schema_ident)
        )

        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.promoted_attempts (
                    attempt_key text PRIMARY KEY,
                    promoted boolean NOT NULL,
                    headline boolean NOT NULL,
                    promotion_label text,
                    promotion_notes text,
                    promoted_at timestamptz,
                    run_jsonl_path text NOT NULL,
                    output_dir text NOT NULL,
                    run_manifest_path text NOT NULL,
                    task_id text NOT NULL,
                    run_index integer NOT NULL,
                    domain text NOT NULL,
                    task_source text NOT NULL,
                    provenance_id text NOT NULL REFERENCES {}.task_provenances (provenance_id),
                    job text,
                    task_description text NOT NULL,
                    success_criteria text,
                    attachment_count integer NOT NULL,
                    attachment_total_bytes bigint NOT NULL,
                    attachment_total_mb double precision NOT NULL,
                    largest_attachment_bytes bigint NOT NULL,
                    attachment_extensions text,
                    attachment_paths text,
                    prompt_char_count integer NOT NULL,
                    prompt_word_count integer NOT NULL,
                    criterion_count integer NOT NULL,
                    primary_criteria_count integer NOT NULL,
                    secondary_criteria_count integer NOT NULL,
                    criteria_with_sources_count integer NOT NULL,
                    criterion_types text,
                    status text NOT NULL,
                    business_pass boolean NOT NULL,
                    score_pct double precision NOT NULL,
                    model_id text NOT NULL,
                    generation_reasoning_effort text,
                    generation_verbosity text,
                    judge_model_id text NOT NULL,
                    judge_reasoning_effort text,
                    judge_verbosity text,
                    generation_mode text NOT NULL,
                    agent_budget text,
                    generation_steps_used integer,
                    tools_used text,
                    generation_prompt_fingerprint text,
                    grading_prompt_fingerprint text,
                    setup_id text NOT NULL,
                    price_book_id_current text,
                    value_base_usd double precision NOT NULL,
                    hours_estimate double precision NOT NULL,
                    generation_input_tokens integer NOT NULL,
                    generation_cached_input_tokens integer NOT NULL,
                    generation_output_tokens integer NOT NULL,
                    grading_input_tokens integer NOT NULL,
                    grading_cached_input_tokens integer NOT NULL,
                    grading_output_tokens integer NOT NULL,
                    current_generation_cost_usd double precision NOT NULL,
                    current_grading_cost_usd double precision NOT NULL,
                    current_total_cost_usd double precision NOT NULL,
                    recorded_total_cost_usd double precision NOT NULL,
                    attempt_completed_at timestamptz,
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            ).format(schema_ident, schema_ident)
        )

        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.tracker_overview (
                    singleton_key boolean PRIMARY KEY,
                    promoted_attempts integer NOT NULL,
                    tracked_task_setups integer NOT NULL,
                    business_passes integer NOT NULL,
                    overall_pass_rate double precision NOT NULL,
                    mean_total_cost_per_attempt_usd double precision NOT NULL,
                    mean_generation_steps_used double precision,
                    tools_used text,
                    mean_expected_net_base_usd_per_attempt double precision NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            ).format(schema_ident)
        )

        cur.execute(
            sql.SQL(
                "CREATE INDEX IF NOT EXISTS task_provenances_task_source_idx ON {}.task_provenances (task_source)"
            ).format(schema_ident)
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS task_setups_task_id_idx ON {}.task_setups (task_id)").format(schema_ident)
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS task_setups_provenance_id_idx ON {}.task_setups (provenance_id)").format(
                schema_ident
            )
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS promoted_attempts_task_id_idx ON {}.promoted_attempts (task_id)").format(
                schema_ident
            )
        )
        cur.execute(
            sql.SQL(
                "CREATE INDEX IF NOT EXISTS promoted_attempts_provenance_id_idx ON {}.promoted_attempts (provenance_id)"
            ).format(schema_ident)
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS promoted_attempts_setup_id_idx ON {}.promoted_attempts (setup_id)").format(
                schema_ident
            )
        )


def _load_provenance_rows(tracker_dir: Path, *, task_setup_rows: list[dict[str, str]], promoted_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    task_provenances_path = tracker_dir / "task_provenances.csv"
    if task_provenances_path.exists():
        return _read_csv(task_provenances_path)
    return build_task_provenance_rows(task_setup_rows + promoted_rows)


def _load_publish_payload(
    tracker_dir: Path,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], list[tuple[Any, ...]], tuple[Any, ...]]:
    tracker_dir = tracker_dir.resolve()
    task_setup_rows = _read_csv(tracker_dir / "master_tracker.csv")
    promoted_rows = _read_csv(tracker_dir / "promoted_attempts.csv")
    provenance_rows = _load_provenance_rows(tracker_dir, task_setup_rows=task_setup_rows, promoted_rows=promoted_rows)
    overall = _read_overall(tracker_dir / "master_tracker_overall.json")

    provenance_values = [
        (
            row["provenance_id"],
            row["task_source"],
            row["source_type"],
            _text(row.get("source_provider")),
            _text(row.get("dataset_name")),
            _text(row.get("dataset_version")),
            _text(row.get("dataset_split")),
            _text(row.get("access_level")),
            _text(row.get("source_reference")),
            _text(row.get("source_url")),
            _text(row.get("notes")),
        )
        for row in provenance_rows
    ]

    task_setup_values = [
        (
            row["setup_id"],
            row["task_id"],
            row["domain"],
            row["task_source"],
            row["provenance_id"],
            _text(row["job"]),
            row["task_description"],
            _text(row["success_criteria"]),
            _int(row["attachment_count"]) or 0,
            _int(row["attachment_total_bytes"]) or 0,
            _float(row["attachment_total_mb"]) or 0.0,
            _int(row["largest_attachment_bytes"]) or 0,
            _text(row["attachment_extensions"]),
            _text(row["attachment_paths"]),
            _int(row["prompt_char_count"]) or 0,
            _int(row["prompt_word_count"]) or 0,
            _int(row["criterion_count"]) or 0,
            _int(row["primary_criteria_count"]) or 0,
            _int(row["secondary_criteria_count"]) or 0,
            _int(row["criteria_with_sources_count"]) or 0,
            _text(row["criterion_types"]),
            row["generation_mode"],
            row["model_id"],
            _text(row["generation_reasoning_effort"]),
            _text(row["generation_verbosity"]),
            row["judge_model_id"],
            _text(row["judge_reasoning_effort"]),
            _text(row["judge_verbosity"]),
            _text(row["generation_prompt_fingerprint"]),
            _text(row["grading_prompt_fingerprint"]),
            _text(row["agent_budget"]),
            _float(row["mean_generation_steps_used"]) or 0.0,
            _text(row["tools_used"]),
            _text(row["price_book_id_current"]),
            _int(row["promoted_attempts"]) or 0,
            _int(row["completed_runs"]) or 0,
            _int(row["business_passes"]) or 0,
            _float(row["pass_rate"]) or 0.0,
            _float(row["mean_score_pct"]) or 0.0,
            _float(row["mean_generation_cost_per_attempt_usd"]) or 0.0,
            _float(row["mean_grading_cost_per_attempt_usd"]) or 0.0,
            _float(row["mean_total_cost_per_attempt_usd"]) or 0.0,
            _float(row["mean_cost_of_successful_attempts_usd"]),
            _float(row["cost_per_success_usd"]),
            _float(row["hours_estimate"]) or 0.0,
            _float(row["value_base_usd"]) or 0.0,
            _float(row["expected_net_base_usd_per_attempt"]) or 0.0,
            _dt(row["latest_attempt_completed_at"]),
            _text(row["promotion_labels"]),
        )
        for row in task_setup_rows
    ]

    promoted_values = [
        (
            row["attempt_key"],
            _bool(row["promoted"]) or False,
            _bool(row["headline"]) or False,
            _text(row["promotion_label"]),
            _text(row["promotion_notes"]),
            _dt(row["promoted_at"]),
            row["run_jsonl_path"],
            row["output_dir"],
            row["run_manifest_path"],
            row["task_id"],
            _int(row["run_index"]) or 0,
            row["domain"],
            row["task_source"],
            row["provenance_id"],
            _text(row["job"]),
            row["task_description"],
            _text(row["success_criteria"]),
            _int(row["attachment_count"]) or 0,
            _int(row["attachment_total_bytes"]) or 0,
            _float(row["attachment_total_mb"]) or 0.0,
            _int(row["largest_attachment_bytes"]) or 0,
            _text(row["attachment_extensions"]),
            _text(row["attachment_paths"]),
            _int(row["prompt_char_count"]) or 0,
            _int(row["prompt_word_count"]) or 0,
            _int(row["criterion_count"]) or 0,
            _int(row["primary_criteria_count"]) or 0,
            _int(row["secondary_criteria_count"]) or 0,
            _int(row["criteria_with_sources_count"]) or 0,
            _text(row["criterion_types"]),
            row["status"],
            _bool(row["business_pass"]) or False,
            _float(row["score_pct"]) or 0.0,
            row["model_id"],
            _text(row["generation_reasoning_effort"]),
            _text(row["generation_verbosity"]),
            row["judge_model_id"],
            _text(row["judge_reasoning_effort"]),
            _text(row["judge_verbosity"]),
            row["generation_mode"],
            _text(row["agent_budget"]),
            _int(row["generation_steps_used"]) or 0,
            _text(row["tools_used"]),
            _text(row["generation_prompt_fingerprint"]),
            _text(row["grading_prompt_fingerprint"]),
            row["setup_id"],
            _text(row["price_book_id_current"]),
            _float(row["value_base_usd"]) or 0.0,
            _float(row["hours_estimate"]) or 0.0,
            _int(row["generation_input_tokens"]) or 0,
            _int(row["generation_cached_input_tokens"]) or 0,
            _int(row["generation_output_tokens"]) or 0,
            _int(row["grading_input_tokens"]) or 0,
            _int(row["grading_cached_input_tokens"]) or 0,
            _int(row["grading_output_tokens"]) or 0,
            _float(row["current_generation_cost_usd"]) or 0.0,
            _float(row["current_grading_cost_usd"]) or 0.0,
            _float(row["current_total_cost_usd"]) or 0.0,
            _float(row["recorded_total_cost_usd"]) or 0.0,
            _dt(row["attempt_completed_at"]),
        )
        for row in promoted_rows
    ]

    overview_value = (
        True,
        int(overall.get("promoted_attempts", 0) or 0),
        int(overall.get("tracked_task_setups", 0) or 0),
        int(overall.get("business_passes", 0) or 0),
        float(overall.get("overall_pass_rate", 0.0) or 0.0),
        float(overall.get("mean_total_cost_per_attempt_usd", 0.0) or 0.0),
        float(overall.get("mean_generation_steps_used", 0.0) or 0.0),
        _text(overall.get("tools_used")),
        float(overall.get("mean_expected_net_base_usd_per_attempt", 0.0) or 0.0),
    )

    return provenance_values, task_setup_values, promoted_values, overview_value


def publish_tracker_to_postgres(
    *,
    tracker_dir: str | Path,
    database_url: str | None = None,
    schema: str = "evals",
) -> dict[str, Any]:
    tracker_dir = Path(tracker_dir).resolve()
    dsn = resolve_database_url(database_url)
    provenance_values, task_setup_values, promoted_values, overview_value = _load_publish_payload(tracker_dir)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
            )
            cur.execute(
                sql.SQL(
                    """
                    DROP TABLE IF EXISTS {}.promoted_attempts;
                    DROP TABLE IF EXISTS {}.task_setups;
                    DROP TABLE IF EXISTS {}.tracker_overview;
                    DROP TABLE IF EXISTS {}.task_provenances;
                    """
                ).format(
                    sql.Identifier(schema),
                    sql.Identifier(schema),
                    sql.Identifier(schema),
                    sql.Identifier(schema),
                )
            )
        _ensure_schema(conn, schema=schema)
        with conn.cursor() as cur:
            _insert_rows(
                cur,
                schema=schema,
                table="task_provenances",
                columns=TASK_PROVENANCE_COLUMNS,
                rows=provenance_values,
            )
            _insert_rows(
                cur,
                schema=schema,
                table="task_setups",
                columns=TASK_SETUP_COLUMNS,
                rows=task_setup_values,
            )
            _insert_rows(
                cur,
                schema=schema,
                table="promoted_attempts",
                columns=PROMOTED_ATTEMPT_COLUMNS,
                rows=promoted_values,
            )

            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {}.tracker_overview (
                        singleton_key, promoted_attempts, tracked_task_setups, business_passes,
                        overall_pass_rate, mean_total_cost_per_attempt_usd, mean_generation_steps_used,
                        tools_used, mean_expected_net_base_usd_per_attempt
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                ).format(sql.Identifier(schema)),
                overview_value,
            )

    return {
        "database_url_env_used": next((key for key in DATABASE_URL_ENV_ORDER if os.getenv(key) and os.getenv(key) == dsn), "explicit"),
        "schema": schema,
        "task_provenances": len(provenance_values),
        "task_setups": len(task_setup_values),
        "promoted_attempts": len(promoted_values),
        "tracker_dir": str(tracker_dir),
    }
