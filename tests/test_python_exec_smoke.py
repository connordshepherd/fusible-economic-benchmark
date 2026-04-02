from mercor_apex_finance_eval.python_exec_smoke import (
    create_python_exec_smoke_fixture,
    summarize_python_exec_smoke,
)
from mercor_apex_finance_eval.utils import write_json


def test_create_python_exec_smoke_fixture_writes_expected_inputs(tmp_path):
    fixture = create_python_exec_smoke_fixture(tmp_path / "smoke", row_count=50)

    assert fixture.csv_path.exists()
    assert fixture.task.attachment_paths == ["documents/990001/python_exec_smoke_input.csv"]
    assert fixture.expected_result["selected_row_count"] > 0
    assert "selected_rows_sha256" in fixture.expected_result


def test_summarize_python_exec_smoke_detects_exact_match_and_python_exec(tmp_path):
    output_dir = tmp_path / "smoke"
    artifact_dir = output_dir / "generation_artifacts" / "task_990001" / "run_1"
    artifact_dir.mkdir(parents=True)

    expected = {
        "selected_row_count": 3,
        "selected_amount_cents": 1200,
        "selected_fee_cents": 12,
        "selected_rows_sha256": "abc123",
    }
    write_json(
        artifact_dir / "usage_summary.json",
        {
            "python_exec_call_count": 1,
            "tool_call_count": 4,
            "steps_used": 3,
            "tools_used": ["list_files", "python_exec", "write_file"],
        },
    )
    (artifact_dir / "tool_trace.jsonl").write_text(
        '{"tool_name":"python_exec","result":{"exit_code":0,"stdout":"ok"}}\n',
        encoding="utf-8",
    )
    (artifact_dir / "runtime_trace.jsonl").write_text(
        '{"event":"daytona_python_exec_end","exit_code":0}\n',
        encoding="utf-8",
    )

    generation_result = {
        "success": True,
        "response": '{"selected_row_count":3,"selected_amount_cents":1200,"selected_fee_cents":12,"selected_rows_sha256":"abc123"}',
        "details": {
            "usage_summary_path": str((artifact_dir / "usage_summary.json").resolve()),
            "tool_trace_path": str((artifact_dir / "tool_trace.jsonl").resolve()),
            "runtime_trace_path": str((artifact_dir / "runtime_trace.jsonl").resolve()),
            "sandbox_used": True,
            "sandbox_id": "sandbox-123",
        },
        "total_cost": 0.12,
        "input_tokens": 100,
        "cached_input_tokens": 20,
        "output_tokens": 30,
        "total_tokens": 130,
        "error_message": "",
    }

    summary = summarize_python_exec_smoke(
        output_dir=output_dir,
        expected_result=expected,
        generation_result=generation_result,
    )

    assert summary["smoke_passed"] is True
    assert summary["python_exec_called"] is True
    assert summary["python_exec_success"] is True
    assert summary["exact_match"] is True
