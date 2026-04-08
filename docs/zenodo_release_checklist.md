# Zenodo Release Checklist

Use this checklist when preparing the first DOI-backed release of `aira-research`.

## Before Creating The GitHub Release

- [ ] Confirm [README.md](../README.md) matches the current artifact contents
- [ ] Confirm [CITATION.cff](../CITATION.cff) has the right title, authors, version, and release date
- [ ] Confirm [LICENSE](../LICENSE) is correct
- [ ] Confirm [external/aira_scanner_version.txt](../external/aira_scanner_version.txt) points to the exact scanner commit used
- [ ] Confirm [paper/aira_paper.pdf](../paper/aira_paper.pdf) builds cleanly from [paper/aira_paper.tex](../paper/aira_paper.tex)
- [ ] Confirm [results/release_summary.md](../results/release_summary.md) reflects the current included CSV outputs
- [ ] Confirm redacted manifests do not contain raw `content` fields
- [ ] Confirm no private tokens, local absolute paths, or stray logs are present in the repo

## GitHub Release Preparation

- [ ] Create or update a tag, for example `v1.0.0`
- [ ] Title the release clearly, for example `v1.0.0 - Initial AIRA Research Artifact`
- [ ] In the release notes, summarize:
  - paper included
  - aggregate results included
  - redacted manifests included
  - pinned scanner commit
- [ ] Attach no extra binaries beyond what is already tracked unless necessary

## Zenodo Setup

- [ ] Sign in to Zenodo with GitHub
- [ ] Enable the `BDB-Labs/aira-research` repository in Zenodo
- [ ] Trigger the archive by publishing the GitHub release
- [ ] Verify the minted DOI
- [ ] Add the DOI back into:
  - `CITATION.cff`
  - GitHub release notes
  - README citation section

## After DOI Minting

- [ ] Update the recommended citation text
- [ ] Optionally add a Zenodo badge to the README
- [ ] Record the DOI in the paper and future arXiv versions

## Suggested Release Notes Skeleton

```text
Initial public research artifact release for AIRA.

Includes:
- paper source and compiled PDF
- rebuilt balanced pilot aggregate results
- governance-system aggregate exports
- repo-weighted and TypeScript sensitivity follow-ups
- redacted manifests and dataset metadata
- reproducibility scripts and documentation

Pinned scanner commit:
fe4efc987c3934dea47aeb192a8a1b3b38a19084
```
