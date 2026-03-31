import csv
import json

from mercor_apex_finance_eval.task_map import build_task_map_rows, write_task_map
from mercor_apex_finance_eval.types import TaskRecord


def test_build_task_map_rows_captures_prompt_attachment_and_rubric_metrics(tmp_path):
    dataset_dir = tmp_path / "dataset"
    docs_dir = dataset_dir / "documents" / "1"
    docs_dir.mkdir(parents=True)

    csv_path = docs_dir / "inputs.csv"
    csv_bytes = b"a,b\n1,2\n"
    csv_path.write_bytes(csv_bytes)

    pdf_path = docs_dir / "memo.pdf"
    pdf_bytes = b"%PDF-1.4 sample"
    pdf_path.write_bytes(pdf_bytes)

    task = TaskRecord(
        task_id=1,
        domain="Finance",
        prompt="Analyze the capital structure and summarize risks.",
        rubric_json=json.dumps(
            {
                "criterion 1": {
                    "weight": "Primary objective(s)",
                    "sources": "inputs.csv",
                    "criterion_type": ["Reasoning", "Extraction"],
                },
                "criterion 2": {
                    "weight": "Not primary objective",
                    "sources": "",
                    "criterion_type": ["Writing"],
                },
            }
        ),
        attachment_paths=["documents/1/inputs.csv", "documents/1/memo.pdf"],
    )

    rows = build_task_map_rows(dataset_dir, [task])

    assert len(rows) == 1
    row = rows[0]
    assert row["task_id"] == 1
    assert row["attachment_count"] == 2
    assert row["attachment_total_bytes"] == len(csv_bytes) + len(pdf_bytes)
    assert row["largest_attachment_bytes"] == max(len(csv_bytes), len(pdf_bytes))
    assert row["attachment_extensions"] == ".csv;.pdf"
    assert row["attachment_paths"] == "documents/1/inputs.csv;documents/1/memo.pdf"
    assert row["prompt_char_count"] == len(task.prompt)
    assert row["prompt_word_count"] == len(task.prompt.split())
    assert row["criterion_count"] == 2
    assert row["primary_criteria_count"] == 1
    assert row["secondary_criteria_count"] == 1
    assert row["criteria_with_sources_count"] == 1
    assert row["criterion_types"] == "Reasoning;Extraction;Writing"


def test_write_task_map_supports_csv_and_json(tmp_path):
    rows = [
        {
            "task_id": 1,
            "domain": "Finance",
            "attachment_count": 1,
            "attachment_total_bytes": 12,
            "attachment_total_mb": 0.000011,
            "largest_attachment_bytes": 12,
            "attachment_extensions": ".csv",
            "attachment_paths": "documents/1/inputs.csv",
            "prompt_char_count": 42,
            "prompt_word_count": 7,
            "criterion_count": 5,
            "primary_criteria_count": 2,
            "secondary_criteria_count": 3,
            "criteria_with_sources_count": 1,
            "criterion_types": "Reasoning",
            "prompt_preview": "Analyze this company.",
        }
    ]

    csv_path = write_task_map(tmp_path / "task_map.csv", rows)
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        parsed = list(csv.DictReader(handle))
    assert parsed[0]["task_id"] == "1"
    assert parsed[0]["attachment_total_bytes"] == "12"

    json_path = write_task_map(tmp_path / "task_map.json", rows)
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload[0]["domain"] == "Finance"
