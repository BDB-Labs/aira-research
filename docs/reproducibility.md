# Reproducibility

## Canonical Tool Repository

The scanner is pinned externally rather than duplicated here:
- repository: `https://github.com/BDB-Labs/aira-scanner`
- commit: `fe4efc987c3934dea47aeb192a8a1b3b38a19084`

See [external/aira_scanner_version.txt](/Users/billp/Documents/GitHub/aira-research/external/aira_scanner_version.txt).

## What Can Be Reproduced From This Repo Alone

- paper build
- release summary tables
- result inspection
- manifest-level provenance review

## What Requires External Source Access

- full local reconstruction of the sampled code corpus
- rerunning the deterministic scanner from raw samples
- extending the corpus beyond the included metadata

## Commands

Build paper:

```bash
cd paper
tectonic aira_paper.tex
```

Refresh the arXiv bundle:

```bash
bash scripts/build_arxiv_bundle.sh
```

Generate release summary tables:

```bash
python3 scripts/generate_tables.py
```
