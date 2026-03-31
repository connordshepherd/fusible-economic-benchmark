# Methodology Sync Plan

## Goal

Keep one canonical `methodology.md` file in this repo, and automatically sync it into the site repo so the Vercel app can render it.

## Recommendation

Use one-way sync with GitHub Actions.

- Canonical source:
  - `economic-evals/docs/methodology.md`
- Synced destination:
  - `economic-evals-site/content/methodology.md`

Do not make this bi-directional.

## Why Not Submodules

`git submodule` would work, but it is not ideal for a single markdown file:

- the site repo would track a pinned commit, not a normal file
- updating the site would require explicit submodule bump commits
- Vercel/content tooling is usually simpler when the markdown is just present in-repo

For this use case, GitHub Actions sync is easier to maintain.

## High-Level Workflow

1. You edit `docs/methodology.md` in `economic-evals`.
2. A GitHub Action runs on push to `main` when that file changes.
3. The workflow checks out the site repo.
4. It copies the file to `content/methodology.md`.
5. It opens or updates a PR in the site repo.

## Required Secret

In the `economic-evals` repo, add a secret:

- `SITE_REPO_SYNC_TOKEN`

That token should have permission to:

- clone the site repo
- create a branch
- push commits
- open or update PRs

If both repos are in the same org, a GitHub App token is ideal. A fine-scoped PAT also works for a first pass.

## Suggested Workflow File

Path:

- `.github/workflows/sync-methodology.yml`

Suggested shape:

```yaml
name: Sync Methodology

on:
  push:
    branches: [main]
    paths:
      - docs/methodology.md

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout source repo
        uses: actions/checkout@v4

      - name: Checkout site repo
        uses: actions/checkout@v4
        with:
          repository: YOUR_ORG/economic-evals-site
          token: ${{ secrets.SITE_REPO_SYNC_TOKEN }}
          path: site

      - name: Copy methodology
        run: |
          mkdir -p site/content
          cp docs/methodology.md site/content/methodology.md

      - name: Create PR
        uses: peter-evans/create-pull-request@v7
        with:
          token: ${{ secrets.SITE_REPO_SYNC_TOKEN }}
          path: site
          commit-message: sync methodology from economic-evals
          branch: chore/sync-methodology
          title: Sync methodology from economic-evals
          body: |
            Automated sync of `docs/methodology.md` from `economic-evals`.
```

## Review Model

I recommend PR-based sync, not direct push to `main`.

Why:

- site changes stay reviewable
- Vercel preview deploys can render the methodology before merge
- accidental edits are easier to catch

## Conflict Policy

The destination file in the site repo should be treated as generated.

That means:

- do not hand-edit `content/methodology.md` in the site repo
- if you need to change methodology, edit the source file here

If you want to make that obvious, add a short comment at the top of the destination file during sync, or a note in the site repo README.

## Optional Improvement

If you later want more shared docs, expand this into a `docs-sync/` directory:

- `economic-evals/docs/methodology.md`
- `economic-evals/docs/faq.md`
- `economic-evals/docs/limitations.md`

and sync all of them into:

- `economic-evals-site/content/docs/`

## Recommendation

Start with:

- one canonical markdown file
- one one-way sync workflow
- PR-based updates

That is the lowest-friction setup and will scale cleanly if you add more shared docs later.
