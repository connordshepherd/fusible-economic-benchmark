from mercor_apex_finance_eval.reporting import summarize_records


def test_reporting_includes_mean_steps_and_tools_used():
    records = [
        {
            "task_id": 13,
            "domain": "Legal",
            "status": "completed",
            "business_pass": False,
            "score_pct": 80.0,
            "job": "Associate",
            "task_description": "Describe the task.",
            "success_criteria": "Describe success.",
            "attachment_count": 1,
            "attachment_total_bytes": 100,
            "attachment_total_mb": 0.0001,
            "largest_attachment_bytes": 100,
            "criterion_count": 3,
            "primary_criteria_count": 2,
            "secondary_criteria_count": 1,
            "generation_cost_usd": 0.5,
            "grading_cost_usd": 0.1,
            "parse_cost_incurred_usd_this_run": 0.0,
            "total_cost_usd_this_run": 0.6,
            "hours_estimate": 2.0,
            "value_low_usd": 100.0,
            "value_base_usd": 200.0,
            "value_high_usd": 300.0,
            "generation_steps_used": 10,
            "tools_used": ["read_file", "write_file"],
        },
        {
            "task_id": 13,
            "domain": "Legal",
            "status": "completed",
            "business_pass": True,
            "score_pct": 100.0,
            "job": "Associate",
            "task_description": "Describe the task.",
            "success_criteria": "Describe success.",
            "attachment_count": 1,
            "attachment_total_bytes": 100,
            "attachment_total_mb": 0.0001,
            "largest_attachment_bytes": 100,
            "criterion_count": 3,
            "primary_criteria_count": 2,
            "secondary_criteria_count": 1,
            "generation_cost_usd": 0.4,
            "grading_cost_usd": 0.1,
            "parse_cost_incurred_usd_this_run": 0.0,
            "total_cost_usd_this_run": 0.5,
            "hours_estimate": 2.0,
            "value_low_usd": 100.0,
            "value_base_usd": 200.0,
            "value_high_usd": 300.0,
            "generation_steps_used": 14,
            "tools_used": ["read_file", "find_in_files"],
        },
    ]

    task_rows, overall = summarize_records(records)

    assert task_rows[0]["mean_generation_steps_used"] == 12.0
    assert task_rows[0]["tools_used"] == "read_file; write_file; find_in_files"
    assert overall["mean_generation_steps_used_per_attempt"] == 12.0
    assert overall["tools_used"] == "read_file; write_file; find_in_files"
