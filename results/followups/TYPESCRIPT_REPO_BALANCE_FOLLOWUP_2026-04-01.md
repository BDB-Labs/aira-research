# TypeScript Repo-Balance Follow-Up

Date: 2026-04-01

## Purpose

This note isolates the TypeScript question from the rebuilt pilot and tests whether the apparent human-control TypeScript reversal survives repo-balanced interpretation.

Artifacts:
- Follow-up bundle: `/Users/billp/Documents/AIRA/output/typescript_repo_balance_followup_v1`
- Repo-level CSV: `/Users/billp/Documents/AIRA/output/typescript_repo_balance_followup_v1/typescript_repo_overview.csv`
- Summary JSON: `/Users/billp/Documents/AIRA/output/typescript_repo_balance_followup_v1/typescript_repo_balance_summary.json`

## Starting Point

From the rebuilt file-balanced pilot:
- Agent TypeScript: `100` files, `15 HIGH`, `0.15 HIGH/file`
- Human TypeScript: `100` files, `32 HIGH`, `0.32 HIGH/file`

At face value, that looked like a TypeScript counter-signal against the hypothesis.

## Repo Concentration

The TypeScript repo distributions are not remotely symmetric.

- Agent TypeScript:
  - `100` files across `55` repos
- Human TypeScript:
  - `100` files across `6` repos

Human TypeScript is concentrated mainly in:
- `novuhq/novu`: `47` files
- `oven-sh/bun`: `41` files
- `getsentry/sentry`: `6` files
- `langfuse/langfuse`: `4` files
- `getsentry/sentry-javascript`: `1` file
- `onlook-dev/onlook`: `1` file

## Repo-Weighted Comparison

Repo-weighted TypeScript high-severity rate:
- Agent: `0.159 HIGH/file` averaged across repos
- Human: `3.706 HIGH/file` averaged across repos

That raw repo-weighted human value is dominated by a single extreme outlier:
- `onlook-dev/onlook`
  - `1` file
  - `22 HIGH`
  - `2 MEDIUM`
  - `1 LOW`
  - `22.0 HIGH/file`

## Outlier Sensitivity

Excluding only `onlook-dev/onlook`:
- Human TypeScript repo-weighted high rate drops from `3.706` to `0.048`

That is the critical result.

Interpretation:
- The TypeScript reversal is not stable as a repo-balanced claim.
- It depends heavily on one human-control hotspot.
- Without that hotspot, human TypeScript repo-weighted high severity is actually below the agent TypeScript repo-weighted mean.

## Bootstrap Sensitivity

To avoid comparing `55` agent repos against `6` human repos naively, a bootstrap was run:
- draw `6` agent repos at random from the `55` available
- compute the average repo-level TypeScript `HIGH/file`
- repeat `5000` times

Results:
- Agent bootstrap mean for 6-repo samples: `0.155`
- Agent bootstrap 95% interval: `0.0` to `0.667`
- Human 6-repo mean including `onlook-dev/onlook`: `3.706`
- Human 5-repo mean excluding `onlook-dev/onlook`: `0.048`

Empirical tail probabilities:
- `P(agent sample >= human mean including outlier) = 0.0`
- `P(agent sample >= human mean excluding outlier) = 0.5128`

This is strong evidence that the apparent TypeScript counter-signal is driven by the outlier, not by a stable arm-wide repo-balanced effect.

## Scientific Read

The TypeScript follow-up changes the interpretation materially:

- The rebuilt file-balanced pilot still shows a TypeScript reversal at the file level.
- But the repo-balanced follow-up shows that this reversal is highly outlier-sensitive.
- Once the single `onlook-dev/onlook` hotspot is removed, the repo-balanced TypeScript difference no longer clearly cuts against the hypothesis.

So the most defensible conclusion is:

> The rebuilt pilot’s TypeScript counter-signal is not robust under repo-balanced sensitivity analysis. It appears substantially driven by a single human-control outlier and should not be treated as a stable language-wide reversal without additional follow-up.

## What To Say

Safe:
- JavaScript remains the strongest stable support for the hypothesis.
- Python remains near parity.
- TypeScript requires cautious interpretation because its human-control reversal is highly sensitive to repo-level hotspot concentration.

Unsafe:
- “TypeScript falsifies the hypothesis.”
- “Human TypeScript is broadly worse.”
- “Repo weighting cleanly reverses the broader result.”
