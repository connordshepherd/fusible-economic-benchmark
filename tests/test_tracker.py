import csv
import json

from mercor_apex_finance_eval.tracker import promote_run, rebuild_tracker


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def test_tracker_promotions_and_master_rollups_use_current_price_book(tmp_path):
    outputs_root = tmp_path / "outputs"
    run_dir = outputs_root / "legal_medium"
    tracker_dir = tmp_path / "tracker"
    price_book_path = tmp_path / "openai_pricing.json"

    _write_json(
        price_book_path,
        {
            "price_book_id": "test_prices_v1",
            "models": {
                "gpt-5.4": {
                    "input_per_1m_usd": 10.0,
                    "cached_input_per_1m_usd": 1.0,
                    "output_per_1m_usd": 100.0,
                }
            },
        },
    )

    _write_json(
        run_dir / "run_manifest.json",
        {
            "config": {
                "model": {
                    "model_id": "gpt-5.4",
                    "model_configs": {"reasoning_effort": "medium", "verbosity": "medium"},
                },
                "grader": {
                    "model_id": "gpt-5.4",
                    "model_configs": {"reasoning_effort": "low", "verbosity": "low"},
                },
                "generation": {"mode": "tool_assisted_daytona"},
                "agent": {"max_steps": 24, "max_tool_calls": 48},
            }
        },
    )
    _write_jsonl(
        run_dir / "raw_runs.jsonl",
        [
            {
                "task_id": 13,
                "domain": "Legal",
                "run_index": 1,
                "status": "completed",
                "business_pass": True,
                "score_pct": 100.0,
                "model_id": "gpt-5.4",
                "judge_model_id": "gpt-5.4",
                "generation_mode": "tool_assisted_daytona",
                "generation_reasoning_effort": "medium",
                "generation_verbosity": "medium",
                "judge_reasoning_effort": "low",
                "judge_verbosity": "low",
                "generation_prompt_fingerprint": "abc123",
                "grading_prompt_fingerprint": "def456",
                "prompt_preview": "Draft the legal opinion.",
                "hours_estimate": 2.0,
                "value_base_usd": 300.0,
                "parse_cost_incurred_usd_this_run": 0.0,
                "generation_input_tokens": 100000,
                "generation_cached_input_tokens": 50000,
                "generation_output_tokens": 1000,
                "generation_cost_usd": 999.0,
                "grading_input_tokens": 20000,
                "grading_cached_input_tokens": 5000,
                "grading_output_tokens": 200,
                "grading_cost_usd": 999.0,
                "total_cost_usd_this_run": 1998.0,
                "attempt_completed_at": "2026-03-30T07:01:10+00:00",
            }
        ],
    )

    summary = rebuild_tracker(outputs_root, tracker_dir, price_book_path=price_book_path)
    assert summary["discovered_attempts"] == 1
    assert summary["promoted_attempts"] == 0

    with (tracker_dir / "discovered_attempts.csv").open("r", encoding="utf-8", newline="") as handle:
        discovered_rows = list(csv.DictReader(handle))
    assert discovered_rows[0]["promoted"] == "False"
    assert discovered_rows[0]["generation_reasoning_effort"] == "medium"
    assert discovered_rows[0]["judge_reasoning_effort"] == "low"
    assert discovered_rows[0]["price_book_id_current"] == "test_prices_v1"
    assert discovered_rows[0]["current_total_cost_usd"] == "0.825"

    selected_count, summary = promote_run(
        tracker_dir=tracker_dir,
        output_dir=run_dir,
        outputs_root=outputs_root,
        price_book_path=price_book_path,
        label="blog_candidate",
        headline=True,
    )
    assert selected_count == 1
    assert summary["promoted_attempts"] == 1
    assert summary["tracked_task_setups"] == 1

    with (tracker_dir / "master_tracker.csv").open("r", encoding="utf-8", newline="") as handle:
        master_rows = list(csv.DictReader(handle))
    assert master_rows[0]["task_id"] == "13"
    assert master_rows[0]["generation_reasoning_effort"] == "medium"
    assert master_rows[0]["pass_rate"] == "1.0"
    assert master_rows[0]["mean_total_cost_per_attempt_usd"] == "0.825"
    assert master_rows[0]["promotion_labels"] == "blog_candidate"


def test_tracker_can_backfill_cached_generation_tokens_from_usage_summary(tmp_path):
    outputs_root = tmp_path / "outputs"
    run_dir = outputs_root / "legal_high"
    tracker_dir = tmp_path / "tracker"
    usage_summary_path = run_dir / "generation_artifacts" / "task_13" / "run_1" / "usage_summary.json"
    price_book_path = tmp_path / "openai_pricing.json"

    _write_json(
        price_book_path,
        {
            "price_book_id": "test_prices_v1",
            "models": {
                "gpt-5.4": {
                    "input_per_1m_usd": 1.0,
                    "cached_input_per_1m_usd": 0.1,
                    "output_per_1m_usd": 10.0,
                }
            },
        },
    )
    _write_json(usage_summary_path, {"cached_input_tokens": 1200})
    _write_json(
        run_dir / "run_manifest.json",
        {
            "config": {
                "model": {"model_id": "gpt-5.4", "model_configs": {"reasoning_effort": "high"}},
                "grader": {"model_id": "gpt-5.4"},
                "generation": {"mode": "tool_assisted_daytona"},
                "agent": {"max_steps": 12, "max_tool_calls": 24},
            }
        },
    )
    _write_jsonl(
        run_dir / "raw_runs.jsonl",
        [
            {
                "task_id": 13,
                "domain": "Legal",
                "run_index": 1,
                "status": "generation_failed",
                "business_pass": False,
                "score_pct": 0.0,
                "model_id": "gpt-5.4",
                "judge_model_id": "gpt-5.4",
                "generation_mode": "tool_assisted_daytona",
                "prompt_preview": "Draft the legal opinion.",
                "hours_estimate": 2.0,
                "value_base_usd": 300.0,
                "parse_cost_incurred_usd_this_run": 0.0,
                "generation_input_tokens": 5000,
                "generation_output_tokens": 100,
                "generation_cost_usd": 0.0,
                "generation_details": {"usage_summary_path": str(usage_summary_path.resolve())},
                "grading_cost_usd": 0.0,
                "total_cost_usd_this_run": 0.0,
                "attempt_completed_at": "2026-03-30T07:02:00+00:00",
            }
        ],
    )

    rebuild_tracker(outputs_root, tracker_dir, price_book_path=price_book_path)

    with (tracker_dir / "discovered_attempts.csv").open("r", encoding="utf-8", newline="") as handle:
        discovered_rows = list(csv.DictReader(handle))
    assert discovered_rows[0]["generation_cached_input_tokens"] == "1200"
