# AIRA 3,000-Case Study Protocol

This document defines the next corpus-study phase after the rebuilt 600-file pilot.

The goal is not to reopen the current paper casually. The goal is to freeze a stronger,
more reproducible study shape that can scale to `3,000` file-level cases with a cleaner
control arm.

## 1. Target Study Shape

Default target:

- `1,000` Arm A files: agent-attributed
- `1,000` Arm B files: matched human controls
- `1,000` Arm C files: unattributed public baseline

This yields:

- `3,000` total file-level cases

If Arm C is not ready on day one, the study may begin as a two-arm expansion:

- `1,000` Arm A files
- `1,000` Arm B files

But the intended end state for the phase is `1,000 x 3`.

## 2. Why Arm B Must Change First

The rebuilt 600-file pilot established that Arm B is the main methodological bottleneck.
The problem is not scan speed. The problem is control-arm quality.

The next phase therefore uses a two-stage Arm B design:

1. Build a large, archived human-control candidate pool.
2. Match the final Arm B from that pool with explicit repo caps, language quotas,
   size-band quotas, and deterministic selection.

This separates acquisition from matching and makes the control arm reproducible.

## 3. Arm Definitions

### Arm A: Agent-Attributed

Primary source:

- `hao-li/AIDev`

Unit:

- file-level records reconstructed from agent-authored PR file patches

Target:

- `1,000` files total

### Arm B: Matched Human Controls

Primary source:

- `hao-li/AIDev` human PR metadata
- GitHub PR file materialization from those human PR records

Method:

- use AIDev human PR rows as the human-labeled control source
- fetch changed files from the linked human PRs
- build an oversized candidate pool first
- match to Arm A only after the pool is frozen

Fallback source if coverage remains poor:

- pre-2022 public-code human proxy pool

Target:

- `1,000` files total

### Arm C: Public Baseline

Primary source:

- The Stack v2

Role:

- unattributed public baseline only

Target:

- `1,000` files total

## 4. Inclusion Rules

Include only:

- Python, JavaScript, and TypeScript family source files
- files between `100` and `2,000` lines inclusive
- files with real source content available
- files with enough provenance to identify repo and source arm

Exclude:

- vendored code
- generated code
- bundled/minified output
- notebooks
- lockfiles
- binary files
- junk paths such as `node_modules`, `vendor`, `dist`, `build`, `.venv`, `coverage`

## 5. Required Controls

The 3,000-case study must record:

- file-weighted outputs
- repo-weighted outputs
- repo caps used during matching
- attribution tier
- full-file vs patch-derived provenance
- scanner version
- seed

## 6. Arm B v2 Rules

### 6.1 Candidate-Pool Build

The human pool builder should:

- derive Arm B language and size-band targets from the Arm A sample
- oversample each quota by a configurable factor
- cap per-repo contribution during pool build
- preserve repo, PR, and file provenance
- emit a JSONL candidate pool plus a detailed summary

Recommended defaults:

- oversample factor: `2.0`
- per-repo cap during pool build: `8`
- deterministic seed: `20260402`

### 6.2 Final Matching

The matcher should:

- require language equality
- require size-band equality
- prefer nearest line-count match
- prefer same-repo controls when available
- enforce a stricter final per-repo cap
- break ties deterministically using the seed

Recommended default:

- final per-repo cap: `4`

## 7. Weighting And Reporting

Primary reporting for the 3,000-case study should include:

- file-weighted high/medium/low findings
- repo-weighted high-severity rates
- per-check prevalence
- language-level cuts
- sensitivity runs with obvious outlier repos removed

Do not rely on file-weighted totals alone.

## 8. Manual Validation

Reserve at least:

- `10%` of the final sample

for manual adjudication across all three arms.

The manual subset should be stratified by:

- language
- severity
- check family
- patch-derived vs full-file provenance

## 9. Immediate Implementation Steps

1. Build Arm B v2 candidate-pool tooling.
2. Freeze matching rules and repo caps.
3. Build a first oversized human pool.
4. Materialize the `1,000`-file Arm B from that pool.
5. Expand Arm A to `1,000`.
6. Add Arm C.
7. Run deterministic scans.

## 10. Current Assumption

This protocol assumes that the next scaling step should be a true `3,000`-case study
(`1,000` files per arm), because that is large enough to be scientifically stronger than
the current rebuilt pilot while still small enough to keep acquisition, validation, and
repo-balance work tractable.
