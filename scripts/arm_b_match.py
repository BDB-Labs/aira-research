"""
arm_b_match.py
--------------
Takes the Arm B candidate pool produced by arm_b_extract.py and draws
a final matched sample aligned to Arm A on:

  - Language distribution
  - File-size decile distribution within each language
  - Per-repo cap (enforced again at draw time: max REPO_CAP files/repo)

Produces:
  data/arm_b/matched_sample.jsonl   — final Arm B draw
  data/arm_b/match_report.json      — balance diagnostics for the paper

Usage
-----
    python arm_b_match.py --arm-a data/arm_a/index.jsonl \
                          --pool  data/arm_b/index.jsonl  \
                          --n     1000

Requirements
------------
    pip install scipy   (for KS test in balance report)
"""

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


LANGUAGES = ["JavaScript", "Python", "TypeScript"]
REPO_CAP = 8
OUTPUT_DIR = Path("data/arm_b")


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


def size_value(record: dict[str, Any]) -> int:
    for key in ("patch_lines", "file_line_count"):
        value = record.get(key)
        if value is not None:
            return int(value)
    content = str(record.get("content") or "")
    return content.count("\n") + 1 if content else 0


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


def stable_key(seed: int, record: dict[str, Any]) -> str:
    return hashlib.sha256(
        f"{seed}|{repo_name(record)}|{pr_identifier(record)}|{file_path(record)}|{record.get('patch_sha','')}".encode(
            "utf-8"
        )
    ).hexdigest()


def assign_deciles(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_lang: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        language = record.get("language")
        if language in LANGUAGES:
            by_lang[language].append(dict(record))

    out: list[dict[str, Any]] = []
    for language in LANGUAGES:
        group = by_lang.get(language, [])
        if not group:
            continue
        sorted_group = sorted(
            group,
            key=lambda record: (size_value(record), repo_name(record), pr_identifier(record), file_path(record)),
        )
        n = len(sorted_group)
        for index, record in enumerate(sorted_group):
            record["patch_lines"] = size_value(record)
            record["size_decile"] = min(9, int((index / max(n - 1, 1)) * 10))
            out.append(record)
    return out


def dedupe_pool(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
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


def arm_a_cell_distribution(records: list[dict[str, Any]]) -> Counter[tuple[str, int]]:
    dist: Counter[tuple[str, int]] = Counter()
    for record in records:
        language = record.get("language")
        decile = int(record.get("size_decile", 0))
        if language in LANGUAGES:
            dist[(language, decile)] += 1
    return dist


def scale_cell_targets(cell_counts: Counter[tuple[str, int]], target_total: int) -> Counter[tuple[str, int]]:
    total = sum(cell_counts.values())
    if total <= 0:
        return Counter()
    exact: dict[tuple[str, int], float] = {
        key: (count / total) * target_total for key, count in cell_counts.items()
    }
    scaled = Counter({key: int(value) for key, value in exact.items()})
    remainder = target_total - sum(scaled.values())
    remainders = sorted(
        cell_counts.keys(),
        key=lambda key: (exact[key] - scaled[key], key[0], key[1]),
        reverse=True,
    )
    for key in remainders[:remainder]:
        scaled[key] += 1
    return scaled


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
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    arm_a = assign_deciles(arm_a)
    pool = assign_deciles(pool)
    pool, duplicates_removed = dedupe_pool(pool)

    cell_targets = scale_cell_targets(arm_a_cell_distribution(arm_a), n_total)
    language_targets = Counter()
    for (language, _decile), count in cell_targets.items():
        language_targets[language] += count

    pool_index: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    all_by_language: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in pool:
        key = (record["language"], int(record["size_decile"]))
        pool_index[key].append(record)
        all_by_language[record["language"]].append(record)

    for key, records in pool_index.items():
        pool_index[key] = sorted(records, key=lambda record: stable_key(seed, record))
    for language, records in all_by_language.items():
        all_by_language[language] = sorted(records, key=lambda record: stable_key(seed, record))

    selected: list[dict[str, Any]] = []
    selected_ids: set[tuple[str, str, str]] = set()
    repo_tally: Counter[str] = Counter()
    repo_cap_rejections = 0
    unmatched_by_cell: Counter[str] = Counter()
    filled_by_language_only: Counter[str] = Counter()

    def try_take(record: dict[str, Any]) -> bool:
        nonlocal repo_cap_rejections
        identity = patch_identity(record)
        repo = repo_name(record)
        if identity in selected_ids:
            return False
        if repo and repo_tally[repo] >= REPO_CAP:
            repo_cap_rejections += 1
            return False
        selected.append(record)
        selected_ids.add(identity)
        if repo:
            repo_tally[repo] += 1
        return True

    for language in LANGUAGES:
        for decile in range(10):
            target = cell_targets.get((language, decile), 0)
            if target <= 0:
                continue
            taken = 0
            for record in pool_index.get((language, decile), []):
                if try_take(record):
                    taken += 1
                    if taken >= target:
                        break
            if taken < target:
                unmatched_by_cell[f"{language}:{decile}"] += target - taken

    for language in LANGUAGES:
        needed = language_targets.get(language, 0) - sum(1 for record in selected if record["language"] == language)
        if needed <= 0:
            continue
        for record in all_by_language.get(language, []):
            if try_take(record):
                filled_by_language_only[language] += 1
                needed -= 1
                if needed <= 0:
                    break

    if len(selected) < n_total:
        all_remaining = sorted(pool, key=lambda record: stable_key(seed, record))
        for record in all_remaining:
            if try_take(record) and len(selected) >= n_total:
                break

    selected = sorted(selected, key=lambda record: stable_key(seed, record))

    diagnostics: dict[str, Any] = {
        "seed": seed,
        "target_total": n_total,
        "total_selected": len(selected),
        "pool_records_in": len(pool) + duplicates_removed,
        "pool_records_after_dedupe": len(pool),
        "duplicates_removed": duplicates_removed,
        "repo_cap": REPO_CAP,
        "repo_cap_rejections": repo_cap_rejections,
        "language_targets": dict(sorted(language_targets.items())),
        "language_counts": dict(sorted(Counter(record["language"] for record in selected).items())),
        "unmatched_by_cell": dict(sorted(unmatched_by_cell.items())),
        "filled_by_language_only": dict(sorted(filled_by_language_only.items())),
        "repos_used": len(repo_tally),
        "max_files_per_repo": max(repo_tally.values(), default=0),
        "decile_balance": {},
    }

    for language in LANGUAGES:
        arm_a_sizes = [size_value(record) for record in arm_a if record.get("language") == language]
        arm_b_sizes = [size_value(record) for record in selected if record.get("language") == language]
        diagnostics["decile_balance"][language] = {
            "arm_a_n": len(arm_a_sizes),
            "arm_b_n": len(arm_b_sizes),
            "arm_a_median": median(arm_a_sizes),
            "arm_b_median": median(arm_b_sizes),
        }

    try:
        from scipy.stats import ks_2samp

        for language in LANGUAGES:
            arm_a_sizes = [size_value(record) for record in arm_a if record.get("language") == language]
            arm_b_sizes = [size_value(record) for record in selected if record.get("language") == language]
            if arm_a_sizes and arm_b_sizes:
                stat, pvalue = ks_2samp(arm_a_sizes, arm_b_sizes)
                diagnostics["decile_balance"][language]["ks_stat"] = round(float(stat), 4)
                diagnostics["decile_balance"][language]["ks_pvalue"] = round(float(pvalue), 4)
    except ImportError:
        diagnostics["ks_test"] = "scipy not installed — skipped"

    return selected[:n_total], diagnostics


def main() -> None:
    parser = argparse.ArgumentParser(description="Draw matched Arm B sample from candidate pool")
    parser.add_argument("--arm-a", default="data/arm_a/index.jsonl", help="Arm A index JSONL")
    parser.add_argument("--pool", default="data/arm_b/index.jsonl", help="Arm B candidate pool JSONL")
    parser.add_argument("--n", type=int, default=1_000, help="Number of files to draw")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--out", default="data/arm_b/matched_sample.jsonl")
    args = parser.parse_args()

    print(f"Loading Arm A: {args.arm_a}")
    arm_a = load_jsonl(args.arm_a)
    print(f"  {len(arm_a)} records")

    print(f"Loading Arm B pool: {args.pool}")
    pool = load_jsonl(args.pool)
    print(f"  {len(pool)} records")

    print(f"Drawing matched sample (n={args.n}, seed={args.seed})...")
    selected, diagnostics = matched_draw(pool, arm_a, args.n, args.seed)

    out_path = Path(args.out)
    report_path = out_path.parent / "match_report.json"
    write_jsonl(selected, out_path)
    report_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True), encoding="utf-8")

    print("\n── Match complete ──")
    print(f"  Selected          : {diagnostics['total_selected']} files")
    print(f"  Repos used        : {diagnostics['repos_used']}")
    print(f"  Max files/repo    : {diagnostics['max_files_per_repo']}")
    print(f"  Repo-cap rejections: {diagnostics['repo_cap_rejections']}")
    print("  Language counts:")
    for language, count in diagnostics["language_counts"].items():
        print(f"    {language:12s}: {count}")
    print("  Size balance:")
    for language, stats in diagnostics["decile_balance"].items():
        ks = f"  KS p={stats.get('ks_pvalue', 'n/a')}" if "ks_pvalue" in stats else ""
        print(
            f"    {language:12s}: Arm A median={stats['arm_a_median']:.0f}  Arm B median={stats['arm_b_median']:.0f}{ks}"
        )
    print(f"  Output            : {out_path}")
    print(f"  Report            : {report_path}")


if __name__ == "__main__":
    main()
