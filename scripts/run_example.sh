#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

source .venv/bin/activate

apex-finance-eval list-tasks \
  --dataset-dir data/APEX-v1-extended \
  --domain Finance \
  --values-csv configs/finance_values.csv

apex-finance-eval run \
  --config configs/example_finance_public.json \
  --limit 3
