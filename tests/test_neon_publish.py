import csv
import json
import os

import pytest

from mercor_apex_finance_eval.neon_publish import (
    PROMOTED_ATTEMPT_COLUMNS,
    TASK_PROVENANCE_COLUMNS,
    TASK_SETUP_COLUMNS,
    _load_publish_payload,
    resolve_database_url,
)
from mercor_apex_finance_eval.provenance import APEX_PUBLIC_V1_EXTENDED_PROVENANCE_ID


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({"DATABASE_URL_UNPOOLED": "postgres://direct"}, "postgres://direct"),
        ({"POSTGRES_URL_NON_POOLING": "postgres://nonpool"}, "postgres://nonpool"),
        ({"DATABASE_URL": "postgres://db"}, "postgres://db"),
        ({"POSTGRES_URL": "postgres://pg"}, "postgres://pg"),
        ({"POSTGRES_PRISMA_URL": "postgres://prisma"}, "postgres://prisma"),
    ],
)
def test_resolve_database_url_prefers_expected_env_vars(monkeypatch: pytest.MonkeyPatch, env, expected):
    for key in [
        "DATABASE_URL_UNPOOLED",
        "POSTGRES_URL_NON_POOLING",
        "DATABASE_URL",
        "POSTGRES_URL",
        "POSTGRES_PRISMA_URL",
    ]:
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    assert resolve_database_url() == expected


def test_resolve_database_url_explicit_value_wins(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATABASE_URL", "postgres://env")
    assert resolve_database_url("postgres://explicit") == "postgres://explicit"


def test_resolve_database_url_errors_when_missing(monkeypatch: pytest.MonkeyPatch):
    for key in [
        "DATABASE_URL_UNPOOLED",
        "POSTGRES_URL_NON_POOLING",
        "DATABASE_URL",
        "POSTGRES_URL",
        "POSTGRES_PRISMA_URL",
    ]:
        monkeypatch.delenv(key, raising=False)

    with pytest.raises(EnvironmentError):
        resolve_database_url()


def test_load_publish_payload_derives_apex_provenance_when_tracker_file_is_missing(tmp_path):
    tracker_dir = tmp_path / "tracker"
    tracker_dir.mkdir()

    def write_csv(path, headers, rows):
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    write_csv(
        tracker_dir / "master_tracker.csv",
        TASK_SETUP_COLUMNS,
        [
            {
                "setup_id": "setup-1",
                "task_id": "13",
                "domain": "Legal",
                "task_source": "Apex",
                "provenance_id": APEX_PUBLIC_V1_EXTENDED_PROVENANCE_ID,
                "job": "Junior Associate",
                "task_description": "Draft the memo.",
                "success_criteria": "Apply the cited authorities.",
                "attachment_count": "1",
                "attachment_total_bytes": "14",
                "attachment_total_mb": "0.001",
                "largest_attachment_bytes": "14",
                "attachment_extensions": ".pdf",
                "attachment_paths": "documents/13/facts.pdf",
                "prompt_char_count": "120",
                "prompt_word_count": "20",
                "criterion_count": "2",
                "primary_criteria_count": "1",
                "secondary_criteria_count": "1",
                "criteria_with_sources_count": "1",
                "criterion_types": "Reasoning",
                "generation_mode": "tool_assisted_daytona",
                "model_id": "gpt-5.4",
                "generation_reasoning_effort": "medium",
                "generation_verbosity": "medium",
                "judge_model_id": "gpt-5.4",
                "judge_reasoning_effort": "low",
                "judge_verbosity": "low",
                "generation_prompt_fingerprint": "abc123",
                "grading_prompt_fingerprint": "def456",
                "agent_budget": "steps=24,tools=48",
                "mean_generation_steps_used": "17.0",
                "tools_used": "read_file; write_file",
                "price_book_id_current": "pricebook-v1",
                "promoted_attempts": "1",
                "completed_runs": "1",
                "business_passes": "1",
                "pass_rate": "1.0",
                "mean_score_pct": "100.0",
                "mean_generation_cost_per_attempt_usd": "0.5",
                "mean_grading_cost_per_attempt_usd": "0.1",
                "mean_total_cost_per_attempt_usd": "0.6",
                "mean_cost_of_successful_attempts_usd": "0.6",
                "cost_per_success_usd": "0.6",
                "hours_estimate": "2.0",
                "value_base_usd": "300.0",
                "expected_net_base_usd_per_attempt": "299.4",
                "latest_attempt_completed_at": "2026-03-30T07:01:10+00:00",
                "promotion_labels": "blog_candidate",
            }
        ],
    )
    write_csv(
        tracker_dir / "promoted_attempts.csv",
        PROMOTED_ATTEMPT_COLUMNS,
        [
            {
                "attempt_key": "attempt-1",
                "promoted": "True",
                "headline": "True",
                "promotion_label": "blog_candidate",
                "promotion_notes": "",
                "promoted_at": "2026-03-30T07:05:00+00:00",
                "run_jsonl_path": "/tmp/raw_runs.jsonl",
                "output_dir": "/tmp/output",
                "run_manifest_path": "/tmp/run_manifest.json",
                "task_id": "13",
                "run_index": "1",
                "domain": "Legal",
                "task_source": "Apex",
                "provenance_id": APEX_PUBLIC_V1_EXTENDED_PROVENANCE_ID,
                "job": "Junior Associate",
                "task_description": "Draft the memo.",
                "success_criteria": "Apply the cited authorities.",
                "attachment_count": "1",
                "attachment_total_bytes": "14",
                "attachment_total_mb": "0.001",
                "largest_attachment_bytes": "14",
                "attachment_extensions": ".pdf",
                "attachment_paths": "documents/13/facts.pdf",
                "prompt_char_count": "120",
                "prompt_word_count": "20",
                "criterion_count": "2",
                "primary_criteria_count": "1",
                "secondary_criteria_count": "1",
                "criteria_with_sources_count": "1",
                "criterion_types": "Reasoning",
                "status": "completed",
                "business_pass": "True",
                "score_pct": "100.0",
                "model_id": "gpt-5.4",
                "generation_reasoning_effort": "medium",
                "generation_verbosity": "medium",
                "judge_model_id": "gpt-5.4",
                "judge_reasoning_effort": "low",
                "judge_verbosity": "low",
                "generation_mode": "tool_assisted_daytona",
                "agent_budget": "steps=24,tools=48",
                "generation_steps_used": "17",
                "tools_used": "read_file; write_file",
                "generation_prompt_fingerprint": "abc123",
                "grading_prompt_fingerprint": "def456",
                "setup_id": "setup-1",
                "price_book_id_current": "pricebook-v1",
                "value_base_usd": "300.0",
                "hours_estimate": "2.0",
                "generation_input_tokens": "1000",
                "generation_cached_input_tokens": "100",
                "generation_output_tokens": "100",
                "grading_input_tokens": "500",
                "grading_cached_input_tokens": "50",
                "grading_output_tokens": "50",
                "current_generation_cost_usd": "0.5",
                "current_grading_cost_usd": "0.1",
                "current_total_cost_usd": "0.6",
                "recorded_total_cost_usd": "0.6",
                "attempt_completed_at": "2026-03-30T07:01:10+00:00",
            }
        ],
    )
    (tracker_dir / "master_tracker_overall.json").write_text(
        json.dumps(
            {
                "promoted_attempts": 1,
                "tracked_task_setups": 1,
                "business_passes": 1,
                "overall_pass_rate": 1.0,
                "mean_total_cost_per_attempt_usd": 0.6,
                "mean_generation_steps_used": 17.0,
                "tools_used": "read_file; write_file",
                "mean_expected_net_base_usd_per_attempt": 299.4,
            }
        ),
        encoding="utf-8",
    )

    provenance_values, task_setup_values, promoted_values, overview_value = _load_publish_payload(tracker_dir)

    provenance_row = dict(zip(TASK_PROVENANCE_COLUMNS, provenance_values[0]))
    task_setup_row = dict(zip(TASK_SETUP_COLUMNS, task_setup_values[0]))
    promoted_row = dict(zip(PROMOTED_ATTEMPT_COLUMNS, promoted_values[0]))

    assert provenance_row["provenance_id"] == APEX_PUBLIC_V1_EXTENDED_PROVENANCE_ID
    assert provenance_row["task_source"] == "Apex"
    assert provenance_row["dataset_name"] == "APEX-v1-extended"
    assert task_setup_row["task_source"] == "Apex"
    assert task_setup_row["provenance_id"] == APEX_PUBLIC_V1_EXTENDED_PROVENANCE_ID
    assert promoted_row["task_source"] == "Apex"
    assert promoted_row["provenance_id"] == APEX_PUBLIC_V1_EXTENDED_PROVENANCE_ID
    assert overview_value[1] == 1
