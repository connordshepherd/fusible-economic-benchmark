# About Our Harness

## Plain-English Summary

Our agent does not get a full developer laptop.

It gets:

- a local workspace with the task prompt and attachments
- a small set of file tools
- a retrieval helper for long files
- an optional Python execution tool backed by Daytona

That is enough to support realistic document work, spreadsheet-style analysis, and light code-assisted reasoning without giving the model unrestricted system access.

## What The Agent Can See

Before generation starts, the harness builds a workspace that includes:

- `/workspace/input/task_prompt.txt`
- `/workspace/input/attachment_manifest.json`
- `/workspace/input/raw_attachments/...`
- `/workspace/input/parsed_attachments/...`
- `/workspace/output/`

The model is told about those paths and can inspect them with tools.

## Exact Tool Surface

The current tool set is:

- `list_files(path)`
  - list files and directories inside the local workspace
- `read_file(path, start_line?, max_lines?)`
  - read UTF-8 text from a workspace file
- `write_file(path, content)`
  - write UTF-8 text into the workspace
- `find_in_files(path, pattern)`
  - do recursive text search over workspace files
- `read_best_matches(path, query, max_results?, context_lines?)`
  - retrieve the best local text windows for a natural-language query
- `python_exec(code, cwd?, timeout_seconds?)`
  - run Python in a Daytona sandbox against the current workspace snapshot

## Which Tools Are Local Vs Remote

Local tools:

- `list_files`
- `read_file`
- `write_file`
- `find_in_files`
- `read_best_matches`

Remote tool:

- `python_exec`

This split is intentional. The agent loop and workspace are local. Daytona is only the execution backend for Python.

## What Python Access Really Means

When the model calls `python_exec`, it does not get arbitrary shell access to the host machine.

Instead, the harness:

1. syncs the local workspace into Daytona
2. uploads the generated script
3. runs Python there
4. captures stdout and exit code
5. syncs `/workspace/output` back to the local workspace

The Python environment is preinstalled and pinned. The model does not get a general-purpose `pip install` tool.

Current package set:

- `pandas`
- `numpy`
- `openpyxl`
- `pyarrow`
- `duckdb`
- `pypdf`
- `pdfplumber`
- `python-docx`
- `beautifulsoup4`
- `lxml`
- `rapidfuzz`

## What The Agent Does Not Have

The current harness does not expose:

- open internet browsing
- arbitrary shell commands
- arbitrary package installation
- direct access to files outside `/workspace`
- database access
- git access
- hidden background tools beyond the declared tool surface

That means the benchmark is still controlled and reproducible, even though the model can do meaningful computer work.

## Why `read_best_matches` Exists

Some tasks include very long legal or technical documents where repeated narrow grep-style searches waste steps.

`read_best_matches` is a local retrieval helper that returns the strongest nearby text windows for a natural-language query. It helps the agent navigate long files without needing to drop into Python or burn a large number of search calls.

## Scratch Work And Final Deliverables

The model may create scratch files in `/workspace/output`.

When it is done, it is instructed to:

- write the final deliverable to `/workspace/output/final_answer.md`
- return the same final text in the final response

This gives the harness one stable place to look for the final answer even if the last model response is interrupted or truncated.

## What We Record

For tool-assisted runs, the harness records:

- `tool_trace.jsonl`
  - every tool call, arguments, result, and duration
- `runtime_trace.jsonl`
  - runtime lifecycle events such as OpenAI responses, Daytona startup, and sync operations
- `usage_summary.json`
  - aggregate token usage, tool counts, step counts, and tools used
- `workspace_manifest.json`
  - the file layout presented to the model

These files make it possible to debug whether a failure came from:

- the model’s reasoning
- the tool strategy
- workspace construction
- Daytona startup
- grading

## Why This Tool Set

The current harness is designed to be minimal but realistic across Finance, Legal, Medicine, and Consulting.

It tries to answer:

- what can a model do with a bounded, auditable computer?
- how much do tool-assisted successes cost?
- which failures are reasoning failures versus harness failures?

That is a better fit for this project than either:

- a pure prompt-only benchmark
- or a fully unrestricted agent box
