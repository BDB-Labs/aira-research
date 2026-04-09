# AIRA Study 3 — Arm B Extraction + Matching

This workflow builds a human-control candidate pool for Arm B and then
draws the final matched Arm B sample aligned to Arm A.

The extractor is now quota-shaped, resumable, and fail-closed:

- quota source: Arm A language + size-band distribution
- collection schedule: round-robin across search languages
- resume: exact queue / page / repo-cursor state in `state.json`
- failure semantics: network / DNS / API failures checkpoint and stop; they do not masquerade as "no more repos"
- persistence: append-only accepted log plus checkpointed derived index
- provenance: every accepted file keeps repo / PR / file / patch identity and reconstructed content

## Prerequisites

```bash
pip install httpx scipy
export GITHUB_TOKEN=your_fine_grained_token
```

`GH_TOKEN` also works. If neither env var is set, the extractor falls back to
`gh auth token`.

For unattended runs, set `GITHUB_TOKEN` or `GH_TOKEN` explicitly. The `gh`
fallback only works if the GitHub CLI is installed and authenticated.

## Files

### `scripts/arm_b_extract.py`

Builds the oversized human candidate pool.

Outputs under `data/arm_b` by default:

- `accepted_log.jsonl`
- `index.jsonl`
- `patches/<patch_sha>.patch`
- `excluded.jsonl`
- `summary.json`
- `state.json`
- `extract.log`

### `scripts/arm_b_match.py`

Takes the pool and draws the final Arm B sample aligned to Arm A with strict
language equality, strict size-band equality, Arm A size-decile mirroring,
same-repo preference, nearest line-count tie-breaking, and a final repo cap.

### `docs/ARM_B_README.md`

Run sequence, defaults, and reproducibility notes.

## Step 1 — Build the candidate pool

Fresh run:

```bash
python scripts/arm_b_extract.py \
  --arm-a /Users/billp/Documents/AIRA/data/aidev_arm_a_staged_1000_sample.jsonl \
  --target 1500 \
  --seed 42 \
  --fresh
```

Resume the same run later:

```bash
python scripts/arm_b_extract.py \
  --arm-a /Users/billp/Documents/AIRA/data/aidev_arm_a_staged_1000_sample.jsonl \
  --target 1500 \
  --seed 42 \
  --resume
```

Notes:

- `--target` is the exact candidate-pool total.
- If you want the pool size derived from Arm A instead, set `--target 0` and use `--oversample-factor`.
- `--fresh` clears prior extractor artifacts in the output directory.
- `--resume` requires the same `arm_a`, `target`, `seed`, and `repo_cap` as the original run.
- If older patch-only rows are present, resume will try to hydrate full file content from GitHub using the recorded `commit_sha`.

### Collection behavior

The extractor:

- derives `language x size_band` quotas from Arm A
- scales those quotas to `--target`
- searches GitHub repos by search language
- accepts files by actual file language, with JavaScript and TypeScript treated as a compatible family
- rotates after a small accepted-file batch instead of filling all JavaScript first
- skips barren repos after a configurable zero-yield threshold
- caches repo-level AI screening across runs

### AI-exclusion filters

Arm B hard-excludes on either signal class:

1. Commit-message keywords
2. Repo-level AI config / workflow signals

Repo-level screening uses a recursive Git tree scan first, then a smaller fallback probe only when the tree response is truncated. Workflow files are checked by filename and YAML contents.

### Failure semantics

This is the main behavioral change from the earlier extractor:

- transport failures are fatal
- exhausted retries stop the run with a checkpoint
- `summary.json` and `state.json` are written before exit
- the run does not interpret DNS / rate-limit / API failure as "search exhausted"

Important exit codes:

- `0`: target completed
- `2`: clean stop with shortfalls still remaining
- `75`: fatal GitHub/network failure after checkpoint
- `130`: interrupted by user after checkpoint

## Step 2 — Draw the matched Arm B sample

```bash
python scripts/arm_b_match.py \
  --arm-a /Users/billp/Documents/AIRA/data/aidev_arm_a_staged_1000_sample.jsonl \
  --pool  /Users/billp/Documents/AIRA/data/arm_b/index.jsonl \
  --n     1000 \
  --seed  42 \
  --repo-cap 4
```

Outputs:

- `data/arm_b/matched_sample.jsonl`
- `data/arm_b/match_report.json`

## Reproducibility

Report these together:

- `--seed`
- `PR_DATE_START` / `PR_DATE_END`
- `--target` or `--oversample-factor`
- `--repo-cap`
- the Arm A input path

The seed controls deterministic ordering of:

- searched repos within each page
- PR order within each repo
- file order within each PR
- final index rewrite ordering
- match tie-breaks

`accepted_log.jsonl` is the append-only accepted-record log. `index.jsonl` is the
checkpointed derived manifest with refreshed `size_decile` values for matching.

GitHub search results can still drift over time, so the seed gives deterministic
selection from a fixed retrieved set, not a perfect historical replay of GitHub.

## Useful extractor flags

- `--stage-accept-limit 25`
  Number of accepted files before rotating to the next search language.

- `--repo-pr-page-budget-per-turn 3`
  How much of one repo to scan before rotating away.

- `--max-pr-pages-per-repo-total 25`
  Hard ceiling on total PR pages scanned from one repo.

- `--zero-yield-pr-threshold 60`
  Mark a repo as barren after this many scanned PRs with zero accepted files.

- `--checkpoint-every-accepts 10`
- `--checkpoint-every-requests 50`
- `--checkpoint-every-seconds 60`

These control how often `summary.json` and `state.json` are refreshed.

## Output schema

`index.jsonl` accepted records include:

- `repo`
- `repo_name`
- `repo_url`
- `repo_stars`
- `pr_number`
- `pr_identifier`
- `pr_merged_at`
- `commit_sha`
- `file_path`
- `path`
- `language`
- `content`
- `file_line_count`
- `size_band`
- `patch_lines`
- `size_decile`
- `patch_sha`
- `ai_excluded`
- `exclusion_signal`

`accepted_log.jsonl` carries the same accepted-record schema before checkpoint-time
decile rewrite.

`excluded.jsonl` records include:

- `timestamp`
- `repo`
- `pr_number`
- `file_path`
- `language`
- `reason`
- `detail`
- `stage`

## Recommended run sequence

1. Start a fresh candidate-pool build with `--fresh`.
2. Let it run until it exits or rate-limits.
3. Resume with `--resume` until `summary.json` shows a sufficiently large pool.
4. Inspect `summary.json` for remaining `language x size_band` shortfalls and repo concentration.
5. Run `arm_b_match.py` only after the pool is large enough to support the final draw.
