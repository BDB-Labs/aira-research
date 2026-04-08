"""
arm_b_match.py
--------------
Draw a final Arm B sample from the Arm B candidate pool produced by
arm_b_extract.py.

Matching invariants:

- language equality
- size-band equality
- Arm A size-decile distribution mirrored deterministically
- same-repo controls preferred when available
- nearest line-count match within the valid cell
- strict final repo cap
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


LANGUAGES = ["JavaScript", "Python", "TypeScript"]
SIZE_BANDS = ("100_299", "300_999", "1000_plus")
DEFAULT_FINAL_REPO_CAP = 4


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def write_jsonl(records: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, sort_keys=True) + "\n")


def record_key(seed: int, record: dict[str, Any]) -> str:
    return hashlib.sha256(
        (
            f"{seed}|{repo_name(record)}|{pr_identifier(record)}|"
            f"{file_path(record)}|{record.get('patch_sha', '')}"
        ).encode("utf-8")
    ).hexdigest()


def repo_name(record: dict[str, Any]) -> str:
    return str(record.get("repo") or record.get("repo_name") or "").strip()


def pr_identifier(record: dict[str, Any]) -> str:
    value = record.get("pr_number")
    if value is not None:
        return str(value)
    return str(record.get("pr_identifier") or "").strip()


def file_path(record: dict[str, Any]) -> str:
    return str(record.get("file_path") or record.get("path") or "").strip()


def patch_identity(record: dict[str, Any]) -> tuple[str, str, str]:
    patch_sha = str(record.get("patch_sha") or "").strip()
    if patch_sha:
        return ("patch_sha", patch_sha, "")
    return (repo_name(record), pr_identifier(record), file_path(record))


def size_value(record: dict[str, Any]) -> int:
    for key in ("file_line_count", "patch_lines"):
        value = record.get(key)
        if value not in (None, ""):
            return int(value)
    content = str(record.get("content") or "")
    return content.count("\n") + 1 if content else 0


def size_band(record: dict[str, Any]) -> str:
    band = str(record.get("size_band") or "").strip()
    if band in SIZE_BANDS:
        return band
    lines = size_value(record)
    if 100 <= lines <= 299:
        return "100_299"
    if 300 <= lines <= 999:
        return "300_999"
    if lines >= 1000:
        return "1000_plus"
    return "unknown"


def assign_deciles(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_lang: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        language = str(record.get("language") or "")
        if language in LANGUAGES:
            enriched = dict(record)
            enriched["file_line_count"] = size_value(enriched)
            enriched["size_band"] = size_band(enriched)
            by_lang[language].append(enriched)

    out: list[dict[str, Any]] = []
    for language in LANGUAGES:
        group = [record for record in by_lang.get(language, []) if record["size_band"] in SIZE_BANDS]
        if not group:
            continue
        sorted_group = sorted(
            group,
            key=lambda record: (
                record["file_line_count"],
                repo_name(record),
                pr_identifier(record),
                file_path(record),
            ),
        )
        n = len(sorted_group)
        for index, record in enumerate(sorted_group):
            record["size_decile"] = min(9, int((index / max(n - 1, 1)) * 10))
            out.append(record)
    return out


def dedupe_records(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    duplicates_removed = 0
    for record in records:
        key = patch_identity(record)
        if key in seen:
            duplicates_removed += 1
            continue
        seen.add(key)
        deduped.append(record)
    return deduped, duplicates_removed


def arm_a_cell_distribution(records: list[dict[str, Any]]) -> Counter[tuple[str, str, int]]:
    dist: Counter[tuple[str, str, int]] = Counter()
    for record in records:
        language = str(record.get("language") or "")
        band = str(record.get("size_band") or "")
        decile = int(record.get("size_decile", 0))
        if language in LANGUAGES and band in SIZE_BANDS:
            dist[(language, band, decile)] += 1
    return dist


def scale_cell_targets(
    cell_counts: Counter[tuple[str, str, int]],
    target_total: int,
) -> Counter[tuple[str, str, int]]:
    total = sum(cell_counts.values())
    if total <= 0:
        return Counter()
    exact: dict[tuple[str, str, int], float] = {
        key: (count / total) * target_total for key, count in cell_counts.items()
    }
    scaled = Counter({key: int(value) for key, value in exact.items()})
    remainder = target_total - sum(scaled.values())
    remainders = sorted(
        cell_counts.keys(),
        key=lambda key: (exact[key] - scaled[key], key[0], key[1], key[2]),
        reverse=True,
    )
    for key in remainders[:remainder]:
        scaled[key] += 1
    return scaled


def select_arm_a_anchors(
    arm_a: list[dict[str, Any]],
    *,
    cell_targets: Counter[tuple[str, str, int]],
    seed: int,
) -> list[dict[str, Any]]:
    by_cell: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for record in arm_a:
        cell = (record["language"], record["size_band"], int(record["size_decile"]))
        by_cell[cell].append(record)

    anchors: list[dict[str, Any]] = []
    for cell, target in sorted(cell_targets.items()):
        if target <= 0:
            continue
        ordered = sorted(by_cell.get(cell, []), key=lambda record: record_key(seed, record))
        anchors.extend(ordered[:target])
    return sorted(anchors, key=lambda record: record_key(seed, record))


def median(values: list[int]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    n = len(ordered)
    if n % 2:
        return float(ordered[n // 2])
    return (ordered[n // 2 - 1] + ordered[n // 2]) / 2


def matched_draw(
    pool: list[dict[str, Any]],
    arm_a: list[dict[str, Any]],
    n_total: int,
    seed: int,
    repo_cap: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    arm_a, arm_a_duplicates_removed = dedupe_records(arm_a)
    pool, pool_duplicates_removed = dedupe_records(pool)
    arm_a = assign_deciles(arm_a)
    pool = assign_deciles(pool)

    cell_targets = scale_cell_targets(arm_a_cell_distribution(arm_a), n_total)
    anchors = select_arm_a_anchors(arm_a, cell_targets=cell_targets, seed=seed)

    pool_index: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for record in pool:
        cell = (record["language"], record["size_band"], int(record["size_decile"]))
        pool_index[cell].append(record)

    selected: list[dict[str, Any]] = []
    selected_ids: set[tuple[str, str, str]] = set()
    repo_tally: Counter[str] = Counter()
    shortfalls_by_cell: Counter[str] = Counter()
    repo_cap_blocked_candidates = 0
    same_repo_matches = 0

    for anchor in anchors:
        cell = (anchor["language"], anchor["size_band"], int(anchor["size_decile"]))
        anchor_repo = repo_name(anchor)
        anchor_size = size_value(anchor)
        eligible: list[dict[str, Any]] = []
        same_repo_eligible: list[dict[str, Any]] = []

        for candidate in pool_index.get(cell, []):
            identity = patch_identity(candidate)
            repo = repo_name(candidate)
            if identity in selected_ids:
                continue
            if repo and repo_tally[repo] >= repo_cap:
                repo_cap_blocked_candidates += 1
                continue
            eligible.append(candidate)
            if repo and repo == anchor_repo:
                same_repo_eligible.append(candidate)

        search_space = same_repo_eligible or eligible
        if not search_space:
            shortfalls_by_cell[f"{cell[0]}:{cell[1]}:{cell[2]}"] += 1
            continue

        best = min(
            search_space,
            key=lambda candidate: (
                abs(size_value(candidate) - anchor_size),
                record_key(seed, candidate),
            ),
        )
        if repo_name(best) == anchor_repo:
            same_repo_matches += 1
        selected.append(best)
        selected_ids.add(patch_identity(best))
        if repo_name(best):
            repo_tally[repo_name(best)] += 1

    selected = sorted(selected, key=lambda record: record_key(seed, record))

    selected_cell_counts = Counter(
        (record["language"], record["size_band"], int(record["size_decile"])) for record in selected
    )
    arm_a_sizes_by_language = {
        language: [size_value(record) for record in anchors if record.get("language") == language]
        for language in LANGUAGES
    }
    arm_b_sizes_by_language = {
        language: [size_value(record) for record in selected if record.get("language") == language]
        for language in LANGUAGES
    }
    diagnostics: dict[str, Any] = {
        "seed": seed,
        "target_total": n_total,
        "total_selected": len(selected),
        "arm_a_input_records": len(arm_a),
        "arm_a_duplicates_removed": arm_a_duplicates_removed,
        "pool_records_after_dedupe": len(pool),
        "pool_duplicates_removed": pool_duplicates_removed,
        "repo_cap": repo_cap,
        "same_repo_matches": same_repo_matches,
        "repo_cap_blocked_candidates": repo_cap_blocked_candidates,
        "shortfall_total": sum(shortfalls_by_cell.values()),
        "shortfalls_by_cell": dict(sorted(shortfalls_by_cell.items())),
        "target_cell_counts": {
            f"{language}:{band}:{decile}": count
            for (language, band, decile), count in sorted(cell_targets.items())
        },
        "selected_cell_counts": {
            f"{language}:{band}:{decile}": count
            for (language, band, decile), count in sorted(selected_cell_counts.items())
        },
        "language_counts": dict(sorted(Counter(record["language"] for record in selected).items())),
        "size_band_counts": {
            language: dict(
                sorted(
                    Counter(
                        record["size_band"] for record in selected if record.get("language") == language
                    ).items()
                )
            )
            for language in LANGUAGES
        },
        "repos_used": len(repo_tally),
        "max_files_per_repo": max(repo_tally.values(), default=0),
        "line_balance": {},
    }

    for language in LANGUAGES:
        diagnostics["line_balance"][language] = {
            "arm_a_n": len(arm_a_sizes_by_language[language]),
            "arm_b_n": len(arm_b_sizes_by_language[language]),
            "arm_a_median": median(arm_a_sizes_by_language[language]),
            "arm_b_median": median(arm_b_sizes_by_language[language]),
        }

    try:
        from scipy.stats import ks_2samp

        for language in LANGUAGES:
            arm_a_sizes = arm_a_sizes_by_language[language]
            arm_b_sizes = arm_b_sizes_by_language[language]
            if arm_a_sizes and arm_b_sizes:
                stat, pvalue = ks_2samp(arm_a_sizes, arm_b_sizes)
                diagnostics["line_balance"][language]["ks_stat"] = round(float(stat), 4)
                diagnostics["line_balance"][language]["ks_pvalue"] = round(float(pvalue), 4)
    except ImportError:
        diagnostics["ks_test"] = "scipy not installed — skipped"

    return selected, diagnostics


def main() -> None:
    parser = argparse.ArgumentParser(description="Draw matched Arm B sample from candidate pool")
    parser.add_argument("--arm-a", default="data/arm_a/index.jsonl", help="Arm A index JSONL")
    parser.add_argument("--pool", default="data/arm_b/index.jsonl", help="Arm B candidate pool JSONL")
    parser.add_argument("--n", type=int, default=1_000, help="Number of files to draw")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic seed")
    parser.add_argument("--repo-cap", type=int, default=DEFAULT_FINAL_REPO_CAP, help="Final per-repo cap")
    parser.add_argument("--out", default="data/arm_b/matched_sample.jsonl")
    args = parser.parse_args()

    print(f"Loading Arm A: {args.arm_a}")
    arm_a = load_jsonl(args.arm_a)
    print(f"  {len(arm_a)} records")

    print(f"Loading Arm B pool: {args.pool}")
    pool = load_jsonl(args.pool)
    print(f"  {len(pool)} records")

    print(f"Drawing matched sample (n={args.n}, seed={args.seed}, repo_cap={args.repo_cap})...")
    selected, diagnostics = matched_draw(pool, arm_a, args.n, args.seed, args.repo_cap)

    out_path = Path(args.out)
    report_path = out_path.parent / "match_report.json"
    write_jsonl(selected, out_path)
    report_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"Wrote matched sample: {out_path} ({len(selected)} files)")
    print(f"Wrote match report:  {report_path}")
    print(json.dumps(diagnostics, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
