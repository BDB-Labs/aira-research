# AIRA-RESEARCH

Versioned research tooling for the AIRA corpus build.

This repo currently tracks the hardened Arm B collection and matching pipeline:

- [scripts/arm_b_extract.py](/Users/billp/Documents/AIRA-RESEARCH/scripts/arm_b_extract.py)
- [scripts/arm_b_match.py](/Users/billp/Documents/AIRA-RESEARCH/scripts/arm_b_match.py)
- [docs/ARM_B_README.md](/Users/billp/Documents/AIRA-RESEARCH/docs/ARM_B_README.md)
- [docs/CORPUS_3000_PROTOCOL.md](/Users/billp/Documents/AIRA-RESEARCH/docs/CORPUS_3000_PROTOCOL.md)

The scripts still default to the live AIRA workspace for data inputs and outputs:

- Arm A input: `/Users/billp/Documents/AIRA/data/aidev_arm_a_staged_1000_sample.jsonl`
- Arm B output: `/Users/billp/Documents/AIRA/data/arm_b`

Typical usage from this repo root:

```bash
python scripts/arm_b_extract.py --target 1500 --seed 42 --resume
python scripts/arm_b_match.py --arm-a /Users/billp/Documents/AIRA/data/aidev_arm_a_staged_1000_sample.jsonl --pool /Users/billp/Documents/AIRA/data/arm_b/index.jsonl --n 1000 --seed 42
```
