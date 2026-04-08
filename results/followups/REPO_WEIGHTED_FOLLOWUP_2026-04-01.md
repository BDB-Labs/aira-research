# Repo-Weighted Follow-Up

Date: 2026-04-01

## Purpose

This note supplements the rebuilt pilot memo with repo-weighted interpretation so single-repo hotspots do not get mistaken for smooth population-wide effects.

Primary artifact:
- `/Users/billp/Documents/AIRA/output/comparison_bundle_full_pilot_rebuilt_v2`

## Key Structural Fact

The rebuilt balanced pilot is file-balanced, but it is not repo-balanced.

From `comparison_bundle_full_pilot_rebuilt_v2/bundle_metadata.json`:
- Agent-attributed arm: `300` files from `126` repos
- Human-control arm: `300` files from `21` repos

That asymmetry matters because file-weighted metrics can be strongly influenced by a small number of high-density repositories on the human side.

## File-Weighted Result

From `comparison_bundle_full_pilot_rebuilt_v2/arm_overview.csv`:
- agent-attributed: `0.267 HIGH/file`
- human-control: `0.203 HIGH/file`

This remains the main rebuilt-pilot headline and still supports a real overall high-severity gap in favor of the hypothesis.

## Repo-Weighted Result

From `comparison_bundle_full_pilot_rebuilt_v2/repo_weighted_overview.csv`:
- agent-attributed: `0.35 avg repo HIGH/file`
- human-control: `1.41 avg repo HIGH/file`

This should **not** be read as overturning the file-weighted result. It mostly reflects the fact that the human-control arm is concentrated in far fewer repositories, so a single extreme repository or even a single extreme file has much more leverage on repo-level averages.

## Repo-Weighted Language Result

From `comparison_bundle_full_pilot_rebuilt_v2/repo_weighted_language_overview.csv`:

- JavaScript
  - agent-attributed: `0.88 avg repo HIGH/file`
  - human-control: `0.635`
- Python
  - agent-attributed: `0.139`
  - human-control: `0.102`
- TypeScript
  - agent-attributed: `0.159`
  - human-control: `3.706`

The TypeScript repo-weighted human value is clearly dominated by hotspot concentration, not by a uniform spread across the control arm.

## TypeScript Sensitivity Check

The main TypeScript human hotspot is:
- `onlook-dev/onlook`
  - `1` file
  - `22 HIGH`
  - `2 MEDIUM`
  - `1 LOW`

Direct sensitivity check:
- Human TypeScript as scanned:
  - `100` files
  - `32 HIGH`
  - `0.32 HIGH/file`
- Human TypeScript excluding `onlook-dev/onlook`:
  - `99` files
  - `10 HIGH`
  - `0.101 HIGH/file`

This is the clearest sign so far that the TypeScript reversal is not stable as a general population claim. It is highly sensitive to one control-side outlier.

## Interpretation

What the repo-weighted follow-up means:

- The rebuilt file-weighted result still shows a modest overall agent-attributed high-severity excess.
- JavaScript remains the strongest support for the hypothesis.
- Python remains near parity.
- The apparent TypeScript reversal is not robust. It is heavily driven by concentrated control-side hotspots, especially `onlook-dev/onlook`.

The correct scientific posture is therefore:

> The rebuilt balanced pilot continues to support a real but conditional signal. The strongest support remains in JavaScript and exception-handling-related patterns. The TypeScript counter-signal appears substantially contaminated by control-side hotspot concentration and should not be treated as a stable language-wide reversal without additional repo-balanced follow-up.

## What To Say

Safe:
- The rebuilt balanced pilot shows a modest overall high-severity gap favoring the hypothesis.
- Repo-weighted inspection shows that the TypeScript counter-signal is highly hotspot-sensitive.
- The strongest stable support remains in JavaScript.

Unsafe:
- “The repo-weighted result disproves the hypothesis.”
- “Human code is generally worse once repo weighting is applied.”
- “TypeScript cleanly falsifies the framework.”

## Recommended Next Step

If we want the strongest next empirical move, it is no longer another generic rerun. It is one of:

1. Repo-balanced control construction for TypeScript specifically.
2. Reporting both file-weighted and repo-weighted figures side by side in the paper.
3. A hotspot-robust analysis that explicitly flags and sensitivity-tests extreme repos.
