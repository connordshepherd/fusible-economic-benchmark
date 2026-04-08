# Economic Eval Harness

## Brief: what we are doing and why

This repo currently runs evals from the **public APEX-v1-extended tasks**. Apex is the source of the task rows we publish today, but the harness and publication model are being shaped so we can add more eval sources later. Finance is still the default path in a few commands and file names, but the harness now supports the public Finance, Legal, Medicine, and Consulting slices. The goal is not to reproduce Mercor’s hidden leaderboard exactly. The goal is to answer a more operational question:

> For a given professional task, how often does a model succeed, what did each attempt cost, and what is the implied cost per successful completion?

APEX is a strong place to start because the public release gives you realistic professional tasks, attached source files, and criterion-level rubrics. That makes it much closer to real knowledge work than a generic academic benchmark. The missing pieces, for an economics-oriented evaluation, are:

1. **A task value model**  
   APEX does not ship task-level dollar values, so this scaffold adds a CSV where you can assign `value_low`, `value_base`, and `value_high` per task.

2. **Attachment parsing with explicit cost capture**  
   Instead of letting parsing happen opaquely inside a benchmark harness, this repo parses attachments with **Reducto directly** and records pages, credits, cache hits, and dollar cost.

3. **Per-attempt generation and grading cost logging**  
   The scaffold calls provider SDKs directly for generation and rubric grading, then writes raw per-run records so you can see generation cost, grading cost, and total attempt cost.

4. **A business-pass rule on top of raw APEX score**  
   APEX natively gives criterion-level results and a percentage score. This repo adds an acceptance rule that is more useful for cost-benefit analysis:
   - default rule: **all primary criteria must pass**
   - and **overall score must be at least 80%**

That gives you outputs that are much closer to the economics questions you care about:

- pass rate by task
- mean cost per attempt
- mean cost of successful attempts
- cost per success
- expected value net of model cost

In other words, this repo is meant to turn the public APEX dataset into a small **economic utility lab**.

## What this scaffold is and is not

This repo **is**:

- a reproducible runner for the **public** APEX-v1-extended dataset today
- structured so more eval sources can be added later without changing the publication model
- focused on the **Finance** domain by default
- designed for **cost accounting**
- designed for **task-by-task valuation**
- resume-friendly
- suitable for a small pilot budget

This repo **is not**:

- an official Mercor leaderboard submission path
- an exact reproduction of Mercor’s hidden holdout evaluation
- a source of canonical task dollar values
- a billing system of record for OpenAI, Google, or Reducto invoices

## Design choices

A few choices are deliberate:

### Why finance first
Finance was the cleanest starting point for “economically valuable white-collar work,” but the current harness and tracker now support all four public APEX domains. Apex is the current source of the task data in the published tracker, not the long-term limit of the project.

### Why use criterion-level rubric grading
APEX ships task rubrics with criterion descriptions, weights, and sources. Grading against those criteria directly keeps the runner transparent and makes failures easier to inspect.

### Why parse with Reducto directly instead of relying on hidden internal parsing
You said Reducto was acceptable. Parsing directly has two advantages:
- you can see **exact Reducto credits**
- you can **cache parsed files** and avoid paying again across repeated runs

### Why add a value CSV
APEX gives realistic work but not dollar labels. The value CSV is the bridge from “benchmark score” to “cost-benefit analysis”.

### Why track both score and business pass
A model can score 70% and still fail a client-style acceptance test. For economics, the binary pass rule is often more decision-useful than the raw score alone.

## Important caveats

1. **Public set only**  
   This scaffold targets the public APEX-v1-extended release, not Mercor’s hidden leaderboard holdout.

2. **Prompt parity is approximate, not guaranteed**  
   The prompts in this repo are scaffold prompts over the public APEX tasks, not byte-for-byte copies of any private evaluation prompts.

3. **Cost numbers are runner-side measurements**  
   Generation and grading costs come from the provider SDK path. For finance-grade accounting, reconcile against provider billing exports after the run.

4. **Task values are yours**  
   This repo seeds a value file, but the final “this task is worth $X” judgment is your modeling decision.

## Canonical Docs

- [Methodology](docs/methodology.md)
- [About Our Harness](docs/about-our-harness.md)
- [Site Architecture Plan](docs/site-architecture-plan.md)
- [Methodology Sync Plan](docs/methodology-sync-plan.md)

The README is the quick operator guide. The two docs above are the best place to look for the current publication methodology and the exact tool surface the model sees.

## Repo layout

```text
economic-evals/
├── README.md
├── configs/
├── docs/
├── outputs/
├── tracker/
├── src/
│   └── mercor_apex_finance_eval/
│       ├── cli.py
│       ├── config.py
│       ├── dataset.py
│       ├── daytona_backend.py
│       ├── evaluation.py
│       ├── mercor_adapter.py
│       ├── neon_publish.py
│       ├── python_exec_smoke.py
│       ├── prompting.py
│       ├── reducto_parser.py
│       ├── reporting.py
│       ├── task_map.py
│       ├── task_metadata.py
│       ├── tool_agent.py
│       ├── tracker.py
│       ├── types.py
│       ├── utils.py
│       ├── value_model.py
│       └── prompts/
├── tests/
└── data/
```

## Quickstart

### 1. Create a virtualenv and install

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Set environment variables

Copy `.env.example` to `.env` and fill in the keys you need.

Typical minimum for the default example:

```bash
OPENAI_API_KEY=...
GOOGLE_API_KEY=...
REDUCTO_API_KEY=...
```

### 3. Download the public APEX dataset

```bash
apex-finance-eval download-dataset --output-dir data/APEX-v1-extended
```

### 4. Seed a finance value file

```bash
apex-finance-eval seed-values \
  --dataset-dir data/APEX-v1-extended \
  --domain Finance \
  --output configs/finance_values.csv
```

This creates a CSV you can edit by hand.

### 5. Inspect the tasks

```bash
apex-finance-eval list-tasks \
  --dataset-dir data/APEX-v1-extended \
  --domain Finance \
  --values-csv configs/finance_values.csv
```

Use this step to decide which tasks to run first.

If you want a fuller structural map of the dataset before choosing tasks, export one:

```bash
apex-finance-eval map-tasks \
  --dataset-dir data/APEX-v1-extended \
  --output outputs/task_map.csv
```

That writes one row per task with domain, attachment count, combined attachment size, prompt length, and rubric structure so you can sort for likely easy or hard cases.

### 6. Run a small pilot

```bash
apex-finance-eval run \
  --config configs/example_finance_public.json \
  --limit 3
```

That will:
- load the selected public APEX tasks
- parse attachments with Reducto
- build the local tool workspace
- call the configured model directly
- grade with the judge model
- write raw JSONL logs
- write summary CSV / JSON / Markdown reports

## About the current harness

The current published path is `tool_assisted_daytona`.

That means:

- the agent loop runs locally in this repo
- local tools handle file listing, reading, writing, searching, and excerpt retrieval
- Daytona is only used when the model explicitly calls `python_exec`

For the exact tool surface and workspace layout, see [About Our Harness](docs/about-our-harness.md).

## Recommended pilot workflow for a small budget

For a first pass, I would do this:

1. seed the finance values file
2. pick 3 tasks
3. run 2 repeats per task
4. inspect raw responses and grade details
5. adjust value estimates or prompts if needed
6. scale to 8 repeats only after the setup looks sane

The bundled example config uses a small repeat count on purpose. It is meant to be safe for a calibration run.

## How task value works

`configs/finance_values.csv` is where you assign per-task value estimates.

Columns:

- `task_id`
- `domain`
- `task_description`
- `attachment_count`
- `attachment_total_bytes`
- `attachment_total_mb`
- `largest_attachment_bytes`
- `criterion_count`
- `primary_criteria_count`
- `secondary_criteria_count`
- `hours_estimate`
- `value_low_usd`
- `value_base_usd`
- `value_high_usd`
- `notes`

The seed command fills the value columns from a simple default:

- low = hours × low rate
- base = hours × base rate
- high = hours × high rate

You should edit those numbers after reading the actual tasks.

## Hand-authored task metadata

`configs/task_metadata.csv` is where you curate the human-facing task metadata used by the tracker and site.

Columns:

- `task_id`
- `domain`
- `job`
- `task_description`
- `success_criteria`

`task_description` should be a short 2-3 sentence explanation of what the work is and what inputs matter. `success_criteria` should be a short 2-3 sentence explanation of what the grader is rewarding. `job` is a best-guess real-world role label such as `Junior Litigation Associate` or `Junior Credit Analyst`.

## Data Shape

There are three important layers of data in this repo.

### 1. Source task metadata

The public APEX source gives one task row with prompt, rubric JSON, and attachment paths. `map-tasks` enriches that with structural fields such as:

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
- hand-authored `job`, `task_description`, and `success_criteria`

### 2. Attempt-level data

`raw_runs.jsonl` stores one row per attempt. For tool-assisted runs that row is enriched with:

- reasoning effort and verbosity
- token usage
- current rerated costs
- prompt fingerprints
- generation steps used
- tools used
- runtime artifact paths

### 3. Published tracker data

The tracker separates experiment logging from publication:

- `tracker/discovered_attempts.csv`
- `tracker/promotions.csv`
- `tracker/promoted_attempts.csv`
- `tracker/master_tracker.csv`
- `tracker/master_tracker_overall.json`

The published Neon schema is derived from promoted tracker rows only.
Promotion decides which task/setup batches are included in the public tracker. For any setup we do publish, the default is to include all promoted attempts, including failures, so the published pass rate stays honest.

Key published field semantics:

- `task_description` is the human-readable summary of what the task is asking for.
- `success_criteria` is the human-readable summary of what the grader rewards.
- `business_pass` is the binary outcome used for published pass-rate accounting.
- detailed `score_summary` payloads and full `response_text` remain local run artifacts in `raw_runs.jsonl` when enabled; they are not currently published to Neon.

## How Daytona works here

Daytona is only the backend for `python_exec`.

The agent does not read files from Daytona or live there full time. Instead:

1. the harness builds a local workspace
2. the model uses local file tools by default
3. if it calls `python_exec`, the harness syncs the workspace into Daytona
4. the generated script runs there
5. `/workspace/output` is synced back locally

This setup keeps legal/document-heavy tasks lightweight while still allowing real Python execution for spreadsheet-style or data-heavy tasks.

The current Python package set is:

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

We also keep a synthetic `smoke-python-exec` command to validate the end-to-end Daytona path.

```bash
PYTHONPATH=src .venv/bin/python -m mercor_apex_finance_eval.cli smoke-python-exec \
  --config configs/openai_daytona_python_exec_smoke_20260401.json
```

## Promotion process

Promotion is manual by design.

We do not publish every experiment run. The intended workflow is:

1. run evals
2. inspect `raw_runs.jsonl`, summaries, and tool traces
3. promote the task/setup batches you want in the public tracker, usually including all attempts for those setups
4. rebuild the published tracker
5. publish the tracker to Neon

The main commands are:

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

`--label` should identify the published setup or batch, for example `legal_remaining9_xhigh_20260407`.
`--headline` is a presentation flag for spotlight rows; it should not be used to exclude failures from headline pass-rate reporting.

```bash
apex-finance-eval publish-neon \
  --tracker-dir tracker \
  --schema evals
```

`publish-neon` reads `DATABASE_URL_UNPOOLED` from `.env` or `.env.local` and snapshot-refreshes the managed `evals` tables from the current tracker state.

`discovered_attempts` is the experiment log. `promotions.csv` is the publication registry. `master_tracker` and Neon are the published view.

By default, promotion should preserve failures as well as successes for any published setup. Promoting only winning attempts is appropriate only for explicitly labeled spotlights, not for headline pass-rate reporting.

Published task/setup and attempt rows are currently tagged with `task_source = Apex` and linked to a provenance record for the public `APEX-v1-extended` release. That keeps today’s source explicit while leaving room for additional eval sources later.

The Neon publisher uses a lightweight snapshot refresh. It keeps the managed tables and indexes in place, clears their rows transactionally, and reloads the latest published snapshot instead of dropping and recreating the schema objects each time.

## What counts as success

By default a run is marked `business_pass = true` only if:

- every **primary** criterion passes
- and the overall rubric percentage is at least **80**

You can change this in the config.

## Output files

A run writes artifacts into the configured output directory.

Main files:

- `run_manifest.json`  
  The resolved config and selected task metadata.

- `selected_tasks.csv`  
  The actual task set used for the run.

- `raw_runs.jsonl`  
  One JSON record per completed attempt. In-progress work appears in `generation_artifacts` first and is appended here only after the attempt completes.

- `task_summary.csv`  
  One row per task with pass rate and cost metrics.

- `overall_summary.json`  
  Aggregate metrics for the full run.

- `report.md`  
  A quick human-readable report.

- `generation_artifacts/`  
  Per-attempt runtime traces, tool traces, prompt files, workspace manifests, and local workspace/output artifacts.

- `run.log`  
  Background launcher log for long-running local batches.

- `run.pid`  
  PID file for a detached local batch process.

## The metrics this repo computes

Per task:

- attempts
- completed runs
- business passes
- pass rate
- mean score
- mean generation cost per attempt
- mean grading cost per attempt
- total parse cost
- mean total cost per attempt
- mean cost of successful attempts
- cost per success
- expected net value using low / base / high value estimates

The most decision-useful number is usually:

```text
cost_per_success_usd = total_cost_across_attempts / number_of_business_passes
```

## Commands

### Download dataset

```bash
apex-finance-eval download-dataset --output-dir data/APEX-v1-extended
```

### Seed value CSV

```bash
apex-finance-eval seed-values \
  --dataset-dir data/APEX-v1-extended \
  --domain Finance \
  --task-metadata-csv configs/task_metadata.csv \
  --output configs/finance_values.csv
```

### List candidate tasks

```bash
apex-finance-eval list-tasks \
  --dataset-dir data/APEX-v1-extended \
  --domain Finance \
  --task-metadata-csv configs/task_metadata.csv \
  --values-csv configs/finance_values.csv
```

### Export a task map

```bash
apex-finance-eval map-tasks \
  --dataset-dir data/APEX-v1-extended \
  --task-metadata-csv configs/task_metadata.csv \
  --output outputs/task_map.csv
```

Useful columns include `job`, `task_description`, `success_criteria`, `attachment_count`, `attachment_total_bytes`, `attachment_total_mb`, `largest_attachment_bytes`, `prompt_char_count`, `criterion_count`, `primary_criteria_count`, and `secondary_criteria_count`.

### Run the Python-exec smoke test

```bash
apex-finance-eval smoke-python-exec \
  --config configs/openai_daytona_python_exec_smoke_20260401.json
```

This is the fastest way to verify that:

- the model actually calls `python_exec`
- Daytona sandbox startup works
- workspace sync works
- Python runs successfully
- the final answer exactly matches a known expected result

### Run an evaluation

```bash
apex-finance-eval run --config configs/example_finance_public.json
```

Optional overrides:

```bash
apex-finance-eval run \
  --config configs/example_finance_public.json \
  --task-ids 101 102 103 \
  --limit 3 \
  --output-dir outputs/pilot_three_tasks
```

### Rebuild summaries from raw JSONL

```bash
apex-finance-eval summarize \
  --run-jsonl outputs/pilot_three_tasks/raw_runs.jsonl \
  --output-dir outputs/pilot_three_tasks
```

## Notes on “what I paid”

This scaffold tracks three cost buckets:

1. **Parsing cost**  
   Reducto credits × configured dollar-per-credit rate.

2. **Generation cost**  
   Estimated from provider token usage returned by the SDK.

3. **Grading cost**  
   Estimated from provider token usage returned by the SDK.

That is enough for experiment accounting. If you need exact invoice reconciliation, export billing data from your provider(s) after the run and compare it to the runner totals.

## If you want to be closer to the public APEX setup

Change the config to something like:

- domain = Finance
- runs per task = 8
- judge model = `gemini-2.5-pro`

That still won’t make the run equivalent to any hidden leaderboard, but it gets closer to the public APEX setup.

## References

- Mercor public APEX dataset: https://huggingface.co/datasets/mercor/APEX-v1-extended
- Mercor public APEX leaderboard page: https://www.mercor.com/apex/apex-v1-leaderboard/
- Reducto Python SDK overview: https://docs.reducto.ai/sdk/python/overview
- Reducto parse docs: https://docs.reducto.ai/sdk/python/parse
- Reducto credit usage docs: https://docs.reducto.ai/reference/credit-usage
