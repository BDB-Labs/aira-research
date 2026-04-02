#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cp "$ROOT/paper/aira_paper.tex" "$ROOT/arxiv_submission/aira_paper.tex"
cp "$ROOT/paper/aira_paper.pdf" "$ROOT/arxiv_submission/aira_paper.pdf"

echo "Updated arXiv bundle in $ROOT/arxiv_submission"
