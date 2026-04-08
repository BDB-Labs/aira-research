#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIG_DIR="$ROOT/paper/figures"

cd "$FIG_DIR"

tectonic aira_check_taxonomy.tex
tectonic failure_suppression_model.tex

cp aira_check_taxonomy.pdf "$ROOT/arxiv_submission/figures/"
cp failure_suppression_model.pdf "$ROOT/arxiv_submission/figures/"

echo "Built figures in $FIG_DIR and copied PDFs to arxiv_submission/figures"
