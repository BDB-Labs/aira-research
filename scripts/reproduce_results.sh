#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Generating release summary from included CSV artifacts..."
python3 "$ROOT/scripts/generate_tables.py"

echo "Rebuilding paper PDF..."
(cd "$ROOT/paper" && tectonic aira_paper.tex)

echo "Refreshing arXiv bundle..."
bash "$ROOT/scripts/build_arxiv_bundle.sh"

echo "Done."
