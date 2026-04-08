# AIRA Research Artifact

AIRA Research is the public research-artifact repository for **AIRA (AI-Induced Risk Audit)**, a deterministic inspection framework for detecting failure-concealing behavior in AI-assisted code.

This repository packages the study as a reusable artifact:
- paper source and compiled PDF
- aggregate empirical results behind the paper claims
- redacted manifests and dataset metadata
- conceptual figures
- reproducibility notes and helper scripts
- hardened corpus-build tooling for the next Arm B expansion

The scanner itself is maintained separately:
- [BDB-Labs/aira-scanner](https://github.com/BDB-Labs/aira-scanner)

Pinned scanner commit for this artifact:
- `fe4efc987c3934dea47aeb192a8a1b3b38a19084`

## Why This Repository Exists

The paper makes empirical claims about a specific class of AI-assisted coding failures: fail-soft patterns that preserve apparent continuity while concealing degraded guarantees. This repository exists so a reviewer or researcher can inspect:
- the manuscript
- the aggregate outputs behind the manuscript
- the provenance and composition of the rebuilt pilot
- the exact scanner version used
- the corpus-build tooling now being used to prepare the larger Arm B and 3,000-case study shape

## Quick Links

- Paper source: [paper/aira_paper.tex](paper/aira_paper.tex)
- Paper PDF: [paper/aira_paper.pdf](paper/aira_paper.pdf)
- Release summary: [results/release_summary.md](results/release_summary.md)
- Rebuilt pilot results: [results/study2_rebuilt_pilot](results/study2_rebuilt_pilot)
- Redacted dataset metadata: [data/dataset_metadata.csv](data/dataset_metadata.csv)
- Methodology note: [docs/methodology.md](docs/methodology.md)
- Reproducibility note: [docs/reproducibility.md](docs/reproducibility.md)
- Zenodo checklist: [docs/zenodo_release_checklist.md](docs/zenodo_release_checklist.md)
- Arm B build README: [docs/ARM_B_README.md](docs/ARM_B_README.md)
- 3,000-case protocol: [docs/CORPUS_3000_PROTOCOL.md](docs/CORPUS_3000_PROTOCOL.md)

## Release Snapshot

### Study 1: Governance-System Audit
- deterministic audit of a 1,120-file governance codebase
- `3,297` findings
- `13/15` checks failing

### Study 2: Rebuilt Balanced Pilot
- `300` agent-attributed files vs `300` matched human controls
- overall high-severity rate: `0.267` vs `0.203` HIGH/file
- strongest stable support: JavaScript (`0.54` vs `0.17`)
- strongest check-level support: `exception_handling` (`61` vs `43`)

## Ongoing Corpus Build

This repo now also tracks the hardened Arm B collection and matching pipeline used for the next corpus build:

- [scripts/arm_b_extract.py](scripts/arm_b_extract.py)
- [scripts/arm_b_match.py](scripts/arm_b_match.py)
- [tests/test_arm_b_extract.py](tests/test_arm_b_extract.py)
- [tests/test_arm_b_match.py](tests/test_arm_b_match.py)

The extractor and matcher currently default to the live AIRA workspace for data inputs and outputs:

- Arm A input: `/Users/billp/Documents/AIRA/data/aidev_arm_a_staged_1000_sample.jsonl`
- Arm B output: `/Users/billp/Documents/AIRA/data/arm_b`

Typical usage from this repo root:

```bash
python scripts/arm_b_extract.py --target 1500 --seed 42 --resume
python scripts/arm_b_match.py --arm-a /Users/billp/Documents/AIRA/data/aidev_arm_a_staged_1000_sample.jsonl --pool /Users/billp/Documents/AIRA/data/arm_b/index.jsonl --n 1000 --seed 42 --repo-cap 4
```

## Concept Figures

- [paper/figures/aira_check_taxonomy.pdf](paper/figures/aira_check_taxonomy.pdf)
- [paper/figures/failure_suppression_model.pdf](paper/figures/failure_suppression_model.pdf)

Build them with:

```bash
bash scripts/build_figures.sh
```

## What Is Included

- [paper/](paper/)
  LaTeX source, compiled paper, figure assets, and references.
- [results/](results/)
  Aggregate outputs for Study 1, the rebuilt balanced pilot, and follow-up analyses.
- [data/](data/)
  Redacted manifests and metadata for the released evidence snapshot.
- [docs/](docs/)
  Methodology, reproducibility, provenance, limitations, release guidance, and ongoing corpus-build documentation.
- [scripts/](scripts/)
  Helper scripts for rebuilding summaries, figures, the arXiv bundle, and the Arm B corpus build.
- [tests/](tests/)
  Regression coverage for the hardened Arm B extractor and matcher.

## What Is Not Included

This repository intentionally does **not** redistribute the raw GitHub/AIDev source corpus used to build the pilot samples.

Instead, it publishes:
- aggregate results
- redacted sample manifests
- dataset metadata
- provenance fields
- content hashes

That keeps the artifact reproducible without casually republishing third-party code.

## Reproducing The Artifact

Build the concept figures:

```bash
bash scripts/build_figures.sh
```

Build the paper PDF:

```bash
cd paper
tectonic aira_paper.tex
```

Generate the release summary:

```bash
python3 scripts/generate_tables.py
```

Refresh the arXiv bundle:

```bash
bash scripts/build_arxiv_bundle.sh
```

Or run the basic full refresh:

```bash
bash scripts/reproduce_results.sh
```

## Citation

Use [CITATION.cff](CITATION.cff). Once a DOI is minted through Zenodo, cite the tagged release and the paper together.
