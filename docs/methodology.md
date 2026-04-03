# Methodology

## Goal

This repo is meant to answer a practical question:

> For a professional task we think is worth `$X`, how often does the model actually succeed, what does each attempt cost, and what does that imply economically?

The public APEX dataset gives us realistic tasks, attachments, and rubrics. Our harness adds the missing operational layers:

- task-level value estimates
- explicit parsing cost capture
- tool-assisted generation
- consistent grading
- a curated promotion workflow for publishing results

## Scope

- The public dataset includes Finance, Legal, Medicine, and Consulting.
- The harness can run any domain in the public release.
- The published task rows currently come from Apex.
- The tracker and database now carry explicit provenance so more eval sources can be added later without pretending everything is Apex forever.
- The published tracker currently includes only `tool_assisted_daytona` attempts.
- Not every run is published. Only promoted attempts flow into the curated tracker and Neon database.

## Data Shape

### Source task data

Right now the source task data comes from the public APEX dataset. It gives one row per task in `train.csv` with:

- `Task ID`
- `Domain`
- `Prompt`
- `Rubric JSON`
- `File Attachments`

This repo turns that into a richer structural task map with fields such as:

- `job`
- `task_description`
- `success_criteria`
- `attachment_count`
- `attachment_total_bytes`
- `attachment_total_mb`
- `largest_attachment_bytes`
- `attachment_extensions`
- `attachment_paths`
- `prompt_char_count`
- `prompt_word_count`
- `criterion_count`
- `primary_criteria_count`
- `secondary_criteria_count`
- `criteria_with_sources_count`
- `criterion_types`

Those fields come from `map-tasks` plus hand-authored metadata in `configs/task_metadata.csv`.

We keep provenance separate from the task content itself. Today that provenance is Apex. Later, additional eval sources can publish into the same tracker and Neon schema with their own provenance records.

### Attempt-level data

`raw_runs.jsonl` is the source of truth for completed eval attempts. Each row is one attempt and includes:

- task identity and domain
- status, score, and `business_pass`
- model and judge model
- reasoning effort and verbosity
- token usage
- parse, generation, grading, and total cost
- prompt fingerprints
- runtime details such as tool traces and usage summaries

For tool-assisted runs, we also record:

- generation steps used
- tools used
- workspace artifact paths
- Daytona sandbox metadata when Python is invoked

### Curated tracker data

The tracker has five important layers:

- `tracker/discovered_attempts.csv`
  - every discovered `tool_assisted_daytona` attempt
- `tracker/promotions.csv`
  - the manual promotion registry
- `tracker/promoted_attempts.csv`
  - discovered attempts filtered to promoted rows only
- `tracker/master_tracker.csv`
  - one row per promoted `task_id x setup_id` rollup
- `tracker/task_provenances.csv`
  - one row per published task source / provenance record

`tracker/master_tracker_overall.json` contains the overall rollup across promoted setups.

### Published database shape

The Neon publisher writes curated data into the `evals` schema:

- `evals.task_provenances`
- `evals.task_setups`
- `evals.promoted_attempts`
- `evals.tracker_overview`

`task_setups` and `promoted_attempts` now carry both `task_source` and `provenance_id`. For the tasks we are publishing today, that means `task_source = Apex` and a linked provenance row for the public `APEX-v1-extended` release.

These tables are derived from promoted tracker rows, not from every experiment run in the repo.

## Harness Execution

Each attempt follows the same broad flow:

1. Load the task from its source dataset and resolve its attachments. Right now that source is Apex.
2. Parse attachments with Reducto or reuse the parse cache.
3. Build a local workspace containing:
   - task prompt
   - attachment manifest
   - raw attachments
   - parsed attachment text
   - output scratch area
4. Run the tool-assisted generation loop with OpenAI.
5. Let the model use local file tools, and use Daytona only if it explicitly calls `python_exec`.
6. Grade the final answer criterion by criterion with the judge model.
7. Write attempt artifacts, rerate costs from the current price book, and refresh tracker files if configured.

## How Daytona Works In This Setup

Daytona is not where the agent “lives.”

The agent loop runs locally inside this repo. The workspace is local. File reading, writing, searching, and excerpt retrieval are local. Daytona is only used for the `python_exec` tool.

That means:

- Legal or Medicine tasks may complete without using Daytona at all.
- Finance or Consulting tasks can use Daytona when Python is actually helpful.
- Sandbox startup cost only appears when the model chooses code execution.

When the model calls `python_exec`, the harness:

1. creates or reuses a Daytona sandbox
2. syncs the current local workspace into the sandbox
3. uploads the generated Python script
4. executes it in the sandbox
5. syncs `/workspace/output` back to the local workspace

Important constraints in the current setup:

- file tools are local only
- Python is stateless from the model’s point of view
- Daytona networking is blocked by default
- package installation is not exposed to the model
- the Python environment uses a pinned preinstalled package set

The current preinstalled package set is:

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

## Tooling Validation

We now keep a synthetic `python_exec` smoke test in the harness.

Purpose:

- force the model to generate Python
- prove that Daytona sandbox creation, workspace sync, execution, and result capture all work together

Command:

```bash
PYTHONPATH=src .venv/bin/python -m mercor_apex_finance_eval.cli smoke-python-exec \
  --config configs/openai_daytona_python_exec_smoke_20260401.json
```

The smoke test only passes if:

- the model actually calls `python_exec`
- the Python execution exits successfully
- the final JSON exactly matches the known expected checksum-style result

## Reasoning Modes And Pricing

Reasoning mode is a first-class part of the setup. We capture:

- generation reasoning effort
- generation verbosity
- judge reasoning effort
- judge verbosity

These fields flow through attempts, tracker rows, and Neon.

Pricing is also first-class. The tracker does not blindly trust old recorded costs. Instead, it rerates attempts from token counts against the active price book in `configs/openai_pricing.json`. That makes price changes easy to propagate without rerunning every task.

## Business Pass Rule

By default an attempt is a business pass only if:

- all primary criteria pass
- and overall score is at least `80%`

This matters because a near-miss rubric score may still be unacceptable in a client-style workflow.

## Promotion Process

Promotion is deliberately manual.

The intended workflow is:

1. run experiments
2. inspect the outputs and failure modes
3. promote only the attempts you want included in the published record
4. rebuild the tracker
5. publish the curated tracker to Neon

Key commands:

```bash
apex-finance-eval rebuild-tracker \
  --outputs-root outputs \
  --tracker-dir tracker \
  --openai-price-book configs/openai_pricing.json \
  --task-metadata-csv configs/task_metadata.csv
```

```bash
apex-finance-eval promote-run \
  --output-dir outputs/<run_dir> \
  --label <label> \
  --headline \
  --outputs-root outputs \
  --tracker-dir tracker \
  --openai-price-book configs/openai_pricing.json \
  --task-metadata-csv configs/task_metadata.csv
```

```bash
apex-finance-eval publish-neon \
  --tracker-dir tracker \
  --schema evals
```

The key idea is simple:

- `discovered_attempts` is the experiment log
- `promotions.csv` is the editorial layer
- `master_tracker`, `task_provenances`, and Neon are the published view

## What We Publish

The public-facing chart should be interpreted as:

- a curated set of promoted task/setup results
- using the current price book
- with explicit reasoning mode
- with explicit tool usage and step counts where available

It is not meant to imply that every internal experiment is equally publication-worthy.
