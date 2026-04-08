# Site Architecture Plan

## Goal

Stand up a public site that does two jobs:

1. Publish the blog and project framing.
2. Show a live tracker of promoted eval runs and rollups.

Right now the task source is Apex. The data model should make that explicit without assuming Apex is the only source we will ever publish.

The recommended split is:

- `economic-evals` repo
  - canonical eval code
  - canonical methodology markdown
  - canonical harness explainer markdown
  - tracker generation
  - publish step for promoted results
- `economic-evals-site` repo
  - Vercel app
  - Neon-backed charts, tables, and task pages
  - blog content and project homepage
  - rendered methodology page

## Repo Roles

### `economic-evals`

Suggested canonical content:

- `docs/methodology.md`
- `docs/about-our-harness.md`
- `tracker/master_tracker.csv`
- `tracker/promoted_attempts.csv`
- `tracker/master_tracker_overall.json`

Suggested future publisher outputs:

- normalized JSON payloads for Neon upsert
- a small publish script that reads the promoted tracker rows and writes them to Postgres

### `economic-evals-site`

Suggested app responsibilities:

- homepage introducing the project
- methodology page rendered from synced markdown
- harness page rendered from synced markdown
- tracker overview page
- task detail pages
- run detail pages if you want drill-down later

Suggested content layout:

- `app/`
- `content/methodology.md`
- `content/about-our-harness.md`
- `lib/db.ts`
- `lib/content.ts`
- `lib/queries.ts`

## Data Flow

### Markdown

- Source of truth: `economic-evals/docs/methodology.md`
- Synced into: `economic-evals-site/content/methodology.md`
- Rendered by the site

### Structured eval data

- Source of truth: promoted tracker outputs in `economic-evals`
- Publish step pushes promoted rows into Neon
- Publish step also pushes explicit task provenance rows into Neon
- Site reads from Neon, not from repo files

## Neon Shape

I would start with a deliberately small schema, but include explicit provenance so today’s Apex tasks and future non-Apex tasks can coexist cleanly:

### `task_provenances`

One row per task source / provenance record.

Suggested columns:

- `provenance_id text primary key`
- `task_source text not null`
- `source_type text not null`
- `source_provider text`
- `dataset_name text`
- `dataset_version text`
- `dataset_split text`
- `access_level text`
- `source_reference text`
- `source_url text`
- `notes text`
- `updated_at timestamptz not null default now()`

### `task_setups`

One row per promoted task/setup rollup.

Suggested columns:

- `task_id text not null`
- `domain text not null`
- `task_source text not null`
- `provenance_id text not null`
- `setup_id text primary key`
- `job text`
- `task_description text not null`
- `success_criteria text`
- `attachment_count integer not null`
- `attachment_total_bytes bigint not null`
- `attachment_total_mb double precision not null`
- `largest_attachment_bytes bigint not null`
- `criterion_count integer not null`
- `primary_criteria_count integer not null`
- `secondary_criteria_count integer not null`
- `generation_mode text not null`
- `model_id text not null`
- `generation_reasoning_effort text`
- `generation_verbosity text`
- `judge_model_id text not null`
- `judge_reasoning_effort text`
- `judge_verbosity text`
- `generation_prompt_fingerprint text`
- `grading_prompt_fingerprint text`
- `agent_budget text`
- `price_book_id_current text`
- `promoted_attempts integer not null`
- `completed_runs integer not null`
- `business_passes integer not null`
- `pass_rate double precision not null`
- `mean_score_pct double precision not null`
- `mean_generation_cost_per_attempt_usd double precision not null`
- `mean_grading_cost_per_attempt_usd double precision not null`
- `mean_total_cost_per_attempt_usd double precision not null`
- `mean_cost_of_successful_attempts_usd double precision`
- `cost_per_success_usd double precision`
- `hours_estimate double precision not null`
- `value_base_usd double precision not null`
- `expected_net_base_usd_per_attempt double precision not null`
- `latest_attempt_completed_at timestamptz`
- `promotion_labels text`
- `updated_at timestamptz not null default now()`

### `promoted_attempts`

One row per promoted attempt.

Suggested columns:

- `attempt_key text primary key`
- `task_id text not null`
- `run_index integer not null`
- `domain text not null`
- `task_source text not null`
- `provenance_id text not null`
- `status text not null`
- `business_pass boolean not null`
- `score_pct double precision not null`
- `model_id text not null`
- `generation_reasoning_effort text`
- `generation_verbosity text`
- `judge_model_id text not null`
- `judge_reasoning_effort text`
- `judge_verbosity text`
- `generation_mode text not null`
- `agent_budget text`
- `setup_id text not null`
- `price_book_id_current text`
- `value_base_usd double precision not null`
- `hours_estimate double precision not null`
- `generation_input_tokens integer not null`
- `generation_cached_input_tokens integer not null`
- `generation_output_tokens integer not null`
- `grading_input_tokens integer not null`
- `grading_cached_input_tokens integer not null`
- `grading_output_tokens integer not null`
- `current_generation_cost_usd double precision not null`
- `current_grading_cost_usd double precision not null`
- `current_total_cost_usd double precision not null`
- `recorded_total_cost_usd double precision not null`
- `attempt_completed_at timestamptz`
- `promotion_label text`
- `promotion_notes text`
- `headline boolean not null default false`
- `run_jsonl_path text`
- `output_dir text`
- `run_manifest_path text`
- `updated_at timestamptz not null default now()`

### `tracker_overview`

Single-row table keyed by a boolean singleton.

Suggested columns:

- `singleton_key boolean primary key`
- `promoted_attempts integer not null`
- `tracked_task_setups integer not null`
- `business_passes integer not null`
- `overall_pass_rate double precision not null`
- `mean_total_cost_per_attempt_usd double precision not null`
- `mean_generation_steps_used double precision`
- `tools_used text`
- `mean_expected_net_base_usd_per_attempt double precision not null`
- `updated_at timestamptz not null default now()`

## Publish Flow

Recommended approach:

1. Run evals in `economic-evals`.
2. Rebuild tracker.
3. Promote the setup batches you want in the public tracker, typically including all attempts for those setups.
4. Run a publish command such as:

```bash
apex-finance-eval publish-neon \
  --tracker-dir tracker \
  --schema evals
```

5. Load database credentials from `DATABASE_URL_UNPOOLED` in `.env` or `.env.local`.
6. Snapshot-refresh:
   - `task_provenances`
   - `promoted_attempts`
   - `task_setups`
   - `tracker_overview`

`promotion_label` should identify the published batch or setup grouping. `headline` is a presentation flag for spotlight rows, not a way to exclude failures from pass-rate calculations.

## Vercel / Neon Runtime Notes

For the site:

- use Neon pooled connection string for normal app queries
- optionally keep a direct connection string for migrations
- keep all writes in the eval repo, not from the public site

That gives you a very clean boundary:

- eval repo writes data
- site repo reads data

## Recommendation

Build the first version with:

- two repos
- one synced markdown file
- one Neon database
- one writer: `economic-evals`
- one reader: `economic-evals-site`

That stays simple even as the tracker grows.
