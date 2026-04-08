# Methodology

## Study 1

Study 1 is a deterministic AIRA audit of a 1,120-file governance-critical codebase used as an existence proof that the targeted failure class is real, measurable, and dense in practice.

## Study 2

Study 2 is a rebuilt balanced corpus pilot:
- `300` agent-attributed files
- `300` matched human-control files
- `100` Python, `100` JavaScript, `100` TypeScript files per arm

The agent-attributed arm was derived from AIDev-linked materialization. The human-control arm was built as a matched public-code comparison set.

## Primary Outcomes

- high-severity findings per file
- language-level differentials
- per-check failure counts
- repo-weighted follow-up analysis

## Interpretation Rule

The current artifact supports a real but conditional signal:
- not a universal law
- strongest stable support in JavaScript
- strongest check-level support in exception-handling-related patterns
- TypeScript remains sensitive to repo concentration
