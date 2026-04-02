import json

from mercor_apex_finance_eval.config import load_config
from mercor_apex_finance_eval.daytona_backend import (
    INPUT_DIR,
    OUTPUT_DIR,
    PARSED_DIR,
    RAW_DIR,
    LocalWorkspaceRuntime,
    build_local_workspace,
)
from mercor_apex_finance_eval.evaluation import validate_environment
from mercor_apex_finance_eval.types import ParsedAttachment, TaskRecord


def test_local_workspace_runtime_supports_basic_file_tools(tmp_path):
    runtime = LocalWorkspaceRuntime(tmp_path / "workspace")
    runtime.write_text_file("/workspace/output/notes.txt", "alpha\nbeta\nkeyword\n")

    listing = runtime.list_files("/workspace/output")
    assert listing[0]["name"] == "notes.txt"
    assert listing[0]["path"] == "/workspace/output/notes.txt"

    read = runtime.read_text_file("/workspace/output/notes.txt", start_line=2, max_lines=2, max_chars=200)
    assert read["content"] == "2: beta\n3: keyword"

    matches = runtime.find_in_files("/workspace", "keyword", max_results=10)
    assert matches == [
        {
            "path": "/workspace/output/notes.txt",
            "line_number": 3,
            "line_text": "keyword",
        }
    ]


def test_local_workspace_runtime_can_read_best_matches(tmp_path):
    runtime = LocalWorkspaceRuntime(tmp_path / "workspace")
    runtime.write_text_file(
        "/workspace/input/parsed_attachments/statute.txt",
        "\n".join(
            [
                "General code material.",
                "Miscellaneous definitions.",
                "Disposition of land use permit, personal property and improvements on death of assignee, see 3 N.N.C. sections 154 and 217.",
                "More notes.",
                "Unrelated material about contracts.",
            ]
        )
        + "\n",
    )
    runtime.write_text_file(
        "/workspace/input/parsed_attachments/other.txt",
        "A probate rule about oral wills and immediate family.\n",
    )

    matches = runtime.read_best_matches(
        "/workspace/input/parsed_attachments",
        "what happens to land use permit and improvements on death of assignee",
        max_results=3,
        context_lines=1,
        max_chars=500,
    )

    assert matches[0]["path"] == "/workspace/input/parsed_attachments/statute.txt"
    assert matches[0]["matched_terms"] == ["land", "use", "permit", "improvements", "death", "assignee"]
    assert "death of assignee" in matches[0]["content"].lower()


def test_build_local_workspace_copies_inputs_and_manifest(tmp_path):
    dataset_dir = tmp_path / "dataset"
    docs_dir = dataset_dir / "documents" / "1"
    docs_dir.mkdir(parents=True)
    csv_path = docs_dir / "inputs.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")

    task = TaskRecord(
        task_id=1,
        domain="Finance",
        prompt="Analyze the input file.",
        rubric_json=json.dumps({}),
        attachment_paths=["documents/1/inputs.csv"],
    )
    parsed = [
        ParsedAttachment(
            filename="inputs.csv",
            relative_path=str(csv_path),
            content="parsed text",
            cache_hit=True,
            num_pages=1,
            credits_incurred=0.0,
            cost_incurred_usd=0.0,
        )
    ]
    runtime = LocalWorkspaceRuntime(tmp_path / "artifact" / "workspace")
    manifest = build_local_workspace(
        runtime,
        task=task,
        dataset_dir=dataset_dir,
        parsed_attachments=parsed,
        local_artifact_dir=tmp_path / "artifact",
    )

    assert runtime.local_path(INPUT_DIR / "task_prompt.txt").read_text(encoding="utf-8") == task.prompt
    assert runtime.local_path(RAW_DIR / "inputs.csv").read_text(encoding="utf-8") == "a,b\n1,2\n"
    assert runtime.local_path(PARSED_DIR / "inputs.csv.txt").read_text(encoding="utf-8") == "parsed text"
    assert manifest["output_dir"] == str(OUTPUT_DIR)
    assert manifest["attachments"][0]["raw_path"] == "/workspace/input/raw_attachments/inputs.csv"


def test_validate_environment_does_not_require_daytona_until_python_exec(tmp_path, monkeypatch):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "dataset_dir": "../data/APEX-v1-extended",
                "output_dir": "../outputs/test",
                "parse_cache_dir": "../.cache/reducto",
                "model": {"model_id": "gpt-5.4"},
                "grader": {"model_id": "gpt-5.4"},
                "generation": {"mode": "tool_assisted_daytona"},
                "reducto": {"enabled": False},
            }
        ),
        encoding="utf-8",
    )
    config = load_config(config_path)

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.delenv("DAYTONA_API_KEY", raising=False)
    monkeypatch.delenv("REDUCTO_API_KEY", raising=False)

    validate_environment(config)
