#!/usr/bin/env python3
"""Generate a compact Markdown release summary from included CSV artifacts."""

from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results" / "study2_rebuilt_pilot"
OUT = ROOT / "results" / "release_summary.md"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def main() -> None:
    arm = read_csv(RESULTS / "arm_overview.csv")
    lang = read_csv(RESULTS / "language_overview.csv")

    lines = [
        "# Release Summary",
        "",
        "## Arm Overview",
        "",
        "| Arm | Files | HIGH | MED | LOW | HIGH/file |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in arm:
        lines.append(
            f"| {row['arm']} | {row['scanned_files']} | {row['high_total']} | "
            f"{row['medium_total']} | {row['low_total']} | {row['high_per_file']} |"
        )

    lines.extend(
        [
            "",
            "## Language Overview",
            "",
            "| Arm | Language | Files | HIGH/file | MED/file | LOW/file |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in lang:
        lines.append(
            f"| {row['arm']} | {row['language']} | {row['scanned_files']} | "
            f"{row['high_per_file']} | {row['medium_per_file']} | {row['low_per_file']} |"
        )

    OUT.write_text("\n".join(lines) + "\n")
    print(OUT)


if __name__ == "__main__":
    main()
