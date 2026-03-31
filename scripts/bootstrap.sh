#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
pip install -r requirements.txt

apex-finance-eval download-dataset --output-dir data/APEX-v1-extended

apex-finance-eval seed-values \
  --dataset-dir data/APEX-v1-extended \
  --domain Finance \
  --output configs/finance_values.csv

echo ""
echo "Bootstrap complete."
echo "Next:"
echo "  1) edit .env"
echo "  2) inspect tasks with: apex-finance-eval list-tasks --dataset-dir data/APEX-v1-extended --domain Finance --values-csv configs/finance_values.csv"
echo "  3) run a pilot with: apex-finance-eval run --config configs/example_finance_public.json --limit 3"
