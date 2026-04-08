# Rebuilt Pilot Data Memo

Date: 2026-04-01

## Scope

This memo summarizes the rebuilt large pilot after freezing a fully balanced human-control arm and rerunning the AIRA static scan.

Balanced corpus:
- Agent-attributed Arm A: `300` files
- Human-control Arm B: `300` files
- Language balance in both arms:
  - `100` Python
  - `100` JavaScript
  - `100` TypeScript

Primary artifacts:
- Agent scan: `/Users/billp/Documents/AIRA/output/aidev_full_pilot_arm_a_scan`
- Rebuilt human scan: `/Users/billp/Documents/AIRA/output/human_control_full_pilot_rebuilt_arm_b_scan`
- Rebuilt comparison bundle: `/Users/billp/Documents/AIRA/output/comparison_bundle_full_pilot_rebuilt_v1`
- Frozen rebuilt human sample: `/Users/billp/Documents/AIRA/data/human_control_full_pilot_rebuilt_sample.jsonl`

## Headline Result

The rebuilt control arm moves the larger pilot back toward a real agent-vs-human gap on high-severity findings, but the result remains materially weaker than the original 30-vs-30 dry run.

From `comparison_bundle_full_pilot_rebuilt_v1/arm_overview.csv`:

- Agent-attributed:
  - `300` files
  - `80 HIGH`
  - `148 MEDIUM`
  - `24 LOW`
  - `0.267 HIGH/file`
- Human control:
  - `300` files
  - `61 HIGH`
  - `195 MEDIUM`
  - `23 LOW`
  - `0.203 HIGH/file`

Interpretation:
- The rebuilt Arm B removes the earlier partial-pilot control skew.
- After that correction, agent-attributed code is again higher on high-severity findings.
- The difference is real enough to matter, but not strong enough to justify a universal claim.

## Language-Level Result

From `comparison_bundle_full_pilot_rebuilt_v1/language_overview.csv`:

- JavaScript strongly supports the hypothesis:
  - agent `0.54 HIGH/file`
  - human `0.17 HIGH/file`
- Python is effectively parity:
  - agent `0.11`
  - human `0.12`
- TypeScript cuts against the simple version of the hypothesis:
  - agent `0.15`
  - human `0.32`

This means the rebuilt large pilot does not support a single uniform “AI code is hotter everywhere” claim. The signal appears conditional by language.

## Check-Level Result

From `comparison_bundle_full_pilot_rebuilt_v1/check_failure_counts.csv`:

- Agent-attributed exceeds human on:
  - `exception_handling`: `61` vs `43`
  - `confidence_representation`: `11` vs `7`
  - `fallback_control`: `14` vs `12`
- Human control exceeds agent on:
  - `background_tasks`: `28` vs `15`
  - `environment_safety`: `8` vs `6`
  - `return_contracts`: `9` vs `7`

The strongest recurring support for the hypothesis remains concentrated around `exception_handling`.

## TypeScript Diagnostic

The TypeScript reversal is not fully diffuse.

Human-control TypeScript findings are concentrated in a small number of repositories, especially:
- `onlook-dev/onlook`: `1` file, `22 HIGH`, `2 MEDIUM`, `1 LOW`
- `oven-sh/bun`: `41` files, `8 HIGH`, `64 MEDIUM`
- `novuhq/novu`: `47` files, `2 HIGH`, `17 MEDIUM`, `5 LOW`

Important implication:
- Human TypeScript high severity is not evenly spread across the whole control arm.
- One human-control repository alone contributes `22` of the `32` TypeScript HIGH findings.

This does not invalidate the control arm, but it does mean the TypeScript result should be interpreted with hotspot sensitivity, not as a smooth population-wide difference.

## Scientific Read

Current state of the hypothesis:

- The rebuilt pilot is stronger than the earlier mixed partial pilot.
- The large-pilot result still does not justify a general law claim.
- The best-supported statement is:
  - there is a measurable signal in favor of the hypothesis overall,
  - it is strongest in JavaScript and exception-handling patterns,
  - and it is not stable enough across all languages to claim uniform prevalence.

Most defensible wording:

> The rebuilt balanced pilot supports a real but conditional signal rather than a uniform field-wide law. The strongest separation appears in JavaScript and in exception-handling-related failure patterns, while Python is near parity and TypeScript remains sensitive to control-arm hotspot composition.

## What This Means For The Paper

Safe claims:
- The dry pilot showed a strong early signal.
- The rebuilt balanced pilot preserves a weaker but still meaningful overall separation on high-severity findings.
- The result is conditional by language and failure class.
- `exception_handling` remains the most consistent check-level support.

Unsafe claims:
- “The corpus study proves the hypothesis generally.”
- “AI-attributed code is worse across all supported languages.”
- “The rebuilt pilot cleanly replicates the dry result.”

## Recommended Next Analytical Steps

1. Perform hotspot-sensitive reruns on the human TypeScript outliers, especially `onlook-dev/onlook`.
2. Add repo-weighted reporting in addition to file-weighted reporting so single-repo concentration cannot dominate the language conclusion.
3. Produce a language-by-check matrix for the rebuilt pilot, especially for JavaScript and TypeScript.
4. Keep the paper wording cautious: promising, conditional, and still under expansion.
