# AIRA Research Artifact

AIRA Research is the paper-and-results companion repository for AIRA (AI-Induced Risk Audit), a deterministic inspection framework for detecting failure-concealing behavior in AI-assisted code.

This repository is the research artifact release:
- paper source and compiled PDF
- aggregate empirical results used in the paper
- redacted dataset manifests and metadata
- reproducibility and methodology notes
- scripts for rebuilding tables and the arXiv bundle

The scanner itself lives in a separate repository:
- [BDB-Labs/aira-scanner](https://github.com/BDB-Labs/aira-scanner)

Pinned scanner version for this release:
- `fe4efc987c3934dea47aeb192a8a1b3b38a19084`

## Repository Layout

```text
aira-research/
├── paper/
├── arxiv_submission/
├── data/
├── results/
├── docs/
├── scripts/
├── environment/
├── external/
├── LICENSE
├── CITATION.cff
└── README.md
```

## Included In This Release

Paper:
- [aira_paper.tex](/Users/billp/Documents/GitHub/aira-research/paper/aira_paper.tex)
- [aira_paper.pdf](/Users/billp/Documents/GitHub/aira-research/paper/aira_paper.pdf)

Study 1 aggregate exports:
- [results/study1_governance](/Users/billp/Documents/GitHub/aira-research/results/study1_governance)

Study 2 rebuilt pilot:
- [results/study2_rebuilt_pilot](/Users/billp/Documents/GitHub/aira-research/results/study2_rebuilt_pilot)

Follow-up analyses:
- [results/followups](/Users/billp/Documents/GitHub/aira-research/results/followups)

Redacted manifests:
- [data/manifests/aidev_manifest_redacted.jsonl](/Users/billp/Documents/GitHub/aira-research/data/manifests/aidev_manifest_redacted.jsonl)
- [data/manifests/human_control_manifest_redacted.jsonl](/Users/billp/Documents/GitHub/aira-research/data/manifests/human_control_manifest_redacted.jsonl)
- [data/dataset_metadata.csv](/Users/billp/Documents/GitHub/aira-research/data/dataset_metadata.csv)

## What Is Not Included

This repository intentionally does not redistribute the raw GitHub/AIDev source corpus used to build the pilot samples.

Instead, it provides:
- aggregate results
- redacted sample manifests
- dataset metadata
- source provenance fields
- content hashes for sample integrity

That keeps the artifact reproducible without casually republishing third-party code.

## Reproducing The Artifact

Rebuild the paper PDF:

```bash
cd paper
tectonic aira_paper.tex
```

Regenerate the simple release summary tables:

```bash
python3 scripts/generate_tables.py
```

Refresh the arXiv bundle from the current paper directory:

```bash
bash scripts/build_arxiv_bundle.sh
```

## Release Snapshot

Study 1:
- deterministic audit of a 1,120-file governance codebase
- `3,297` findings
- `13/15` checks failing

Study 2:
- rebuilt balanced pilot
- `300` agent-attributed files vs `300` matched human controls
- overall high-severity rate: `0.267` vs `0.203` HIGH/file
- strongest stable support: JavaScript (`0.54` vs `0.17`)
- strongest check-level support: `exception_handling` (`61` vs `43`)

## Citation

Use [CITATION.cff](/Users/billp/Documents/GitHub/aira-research/CITATION.cff) or cite the repository release once a DOI is minted through Zenodo.
