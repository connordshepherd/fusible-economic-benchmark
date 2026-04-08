# Tracker UI Wireframe

This is the minimal version.

The goal is to publish a tracker that is easy to understand at a glance, while still letting a user inspect what each task was and how the attempts performed.

This spec intentionally avoids charts, advanced metrics, and internal-ops detail. We can add those back later if we miss them.

## V1 Goal

Answer four questions:

1. how many published setups do we have?
2. how often do they pass?
3. what do they cost?
4. what is each task actually asking for?

## Primary Objects

Use:

- `evals.tracker_overview`
- `evals.task_setups`
- `evals.promoted_attempts`

Primary browse object:

- `task_setup`

Drill-down object:

- `promoted_attempt`

## Primary Pages

Only build these first:

- `/tracker`
  - overview page
- `/tracker/setup/[setup_id]`
  - setup detail page

Do not build in v1:

- charts
- attempt detail pages
- cohort comparison views
- provenance pages
- token analytics

## Desktop V1

### Layout

```text
+------------------------------------------------------------------------------+
| Tracker                                                                      |
| Last publish time                                                            |
+------------------------------------------------------------------------------+
| Setups | Pass rate | Mean cost / attempt                                     |
+------------------------------------------------------------------------------+
| Search | Domain | Model | Reasoning                                          |
+------------------------------------------------------------------------------+
| Task | Job | Model | Reasoning | Pass rate | Mean score | Mean cost          |
| ...                                                                        > |
| ...                                                                        > |
+------------------------------------------------------------------------------+
```

### Top Summary

Only show:

- `tracked_task_setups`
- `overall_pass_rate`
- `mean_total_cost_per_attempt_usd`

### Filters

Keep filters small:

- search over `task_description` and `job`
- `domain`
- `model_id`
- `generation_reasoning_effort`

If we want to cut even further, `model_id` can go too.

### Main Table

One row per `task_setup`.

Show only:

- `task_id`
- `job`
- short `task_description`
- `model_id`
- `generation_reasoning_effort`
- `pass_rate`
- `mean_score_pct`
- `mean_total_cost_per_attempt_usd`

Do not show in the main table:

- `promoted_attempts`
- `business_passes`
- `mean_generation_steps_used`
- `cost_per_success_usd`
- `expected_net_base_usd_per_attempt`
- `latest_attempt_completed_at`
- provenance fields
- attachment stats

## Mobile V1

### Layout

```text
+----------------------------------+
| Tracker                          |
| Last publish time                |
+----------------------------------+
| Setups | Pass rate | Mean cost   |
+----------------------------------+
| Search                    Filter |
+----------------------------------+
| Card                            |
| task id + job                   |
| 2-line task summary             |
| model badge | reasoning badge   |
| pass rate | score | mean cost   |
+----------------------------------+
| Card ...                        |
+----------------------------------+
```

### Mobile Card

Show only:

- `task_id`
- `job`
- short `task_description`
- `model_id`
- `generation_reasoning_effort`
- `pass_rate`
- `mean_score_pct`
- `mean_total_cost_per_attempt_usd`

## Setup Detail Page

This is where the rest of the important context lives.

### Setup Header

Show:

- `task_id`
- `job`
- `model_id`
- `generation_reasoning_effort`
- `pass_rate`
- `mean_score_pct`
- `mean_total_cost_per_attempt_usd`

### Task Section

Show:

- full `task_description`
- full `success_criteria`

### Attempts Section

Show a simple attempts list with:

- `run_index`
- `business_pass`
- `score_pct`
- `current_total_cost_usd`
- `attempt_completed_at`

This is the main transparency mechanism for v1.

## Data We Intentionally Omit In V1

Keep these out unless we discover a strong need:

- `promoted_attempts`
- `business_passes`
- `mean_generation_steps_used`
- `tools_used`
- token counts
- prompt fingerprints
- attachment paths
- provenance metadata
- `promotion_label`
- `headline`
- `judge_*` fields
- `expected_net_base_usd_per_attempt`
- `cost_per_success_usd`

## Query Plan

### Overview

Use `evals.tracker_overview`.

Suggested fields:

- `tracked_task_setups`
- `overall_pass_rate`
- `mean_total_cost_per_attempt_usd`

### Setup List

Use `evals.task_setups`.

Suggested fields:

- `setup_id`
- `task_id`
- `domain`
- `job`
- `task_description`
- `model_id`
- `generation_reasoning_effort`
- `pass_rate`
- `mean_score_pct`
- `mean_total_cost_per_attempt_usd`

Suggested query:

```sql
select
  setup_id,
  task_id,
  domain,
  job,
  task_description,
  model_id,
  generation_reasoning_effort,
  pass_rate,
  mean_score_pct,
  mean_total_cost_per_attempt_usd
from evals.task_setups
where ($1::text is null or domain = $1)
  and ($2::text is null or model_id = $2)
  and ($3::text is null or generation_reasoning_effort = $3)
  and (
    $4::text is null
    or job ilike '%' || $4 || '%'
    or task_description ilike '%' || $4 || '%'
  )
order by task_id::int asc;
```

### Setup Detail

Use `evals.task_setups`.

Suggested fields:

- `setup_id`
- `task_id`
- `job`
- `task_description`
- `success_criteria`
- `model_id`
- `generation_reasoning_effort`
- `pass_rate`
- `mean_score_pct`
- `mean_total_cost_per_attempt_usd`

Suggested query:

```sql
select
  setup_id,
  task_id,
  job,
  task_description,
  success_criteria,
  model_id,
  generation_reasoning_effort,
  pass_rate,
  mean_score_pct,
  mean_total_cost_per_attempt_usd
from evals.task_setups
where setup_id = $1;
```

### Attempts For A Setup

Use `evals.promoted_attempts`.

Suggested fields:

- `run_index`
- `business_pass`
- `score_pct`
- `current_total_cost_usd`
- `attempt_completed_at`

Suggested query:

```sql
select
  run_index,
  business_pass,
  score_pct,
  current_total_cost_usd,
  attempt_completed_at
from evals.promoted_attempts
where setup_id = $1
order by run_index asc;
```

## Interaction Rules

- The list view should stay simple enough to scan without horizontal overload.
- Failures should remain visible through `pass_rate` and the attempts list.
- Task text should truncate in the list and expand on the detail page.
- Percentages should always render as percentages.
- Cost should always render as currency.
- Mobile should use cards, not a compressed table.

## Nice First Cut

If we want the cleanest possible first implementation, build exactly:

1. top summary with 3 metrics
2. searchable/filterable setup list
3. setup detail page
4. attempts list inside setup detail

That is enough to make the tracker public and understandable without over-designing it.
