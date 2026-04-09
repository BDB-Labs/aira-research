"""
arm_b_extract.py
----------------
Build a resumable, quota-shaped Arm B (matched-human) candidate pool for AIRA.

This extractor is intentionally stateful:
- derives language + size-band + decile quotas from Arm A
- collects against those quotas in staged round-robin turns
- persists queue / page / repo cursor state for exact resume
- fails closed on transport / DNS / API exhaustion instead of treating errors as empty search
- writes detailed summary + state checkpoints throughout the run

Primary outputs
---------------
data/arm_b/accepted_log.jsonl
    append-only accepted records with full reconstructed content and provenance

data/arm_b/index.jsonl
    checkpointed derived manifest with refreshed size deciles for matching

data/arm_b/patches/<patch_sha>.patch
    raw unified diff for each accepted file

data/arm_b/excluded.jsonl
    audit trail for AI-exclusions, duplicate drops, and patch-content rejections

data/arm_b/summary.json
    current run summary with remaining quotas, request counts, and repo concentration

data/arm_b/state.json
    resumable cursor / cache / queue state
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import hashlib
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx


STATE_VERSION = 4

SEARCH_LANGUAGES = ("javascript", "python", "typescript")
DISPLAY_LANGUAGE = {
    "javascript": "JavaScript",
    "python": "Python",
    "typescript": "TypeScript",
}
FILE_EXTENSIONS = {
    ".js": "javascript",
    ".mjs": "javascript",
    ".cjs": "javascript",
    ".jsx": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".py": "python",
}
SIZE_BANDS = ("100_299", "300_999", "1000_plus")
SIZE_DECILES = tuple(range(10))
LANGUAGE_FAMILY = {
    "javascript": {"javascript", "typescript"},
    "typescript": {"javascript", "typescript"},
    "python": {"python"},
}

DEFAULT_ARM_A = Path("/Users/billp/Documents/AIRA/data/aidev_arm_a_staged_1000_sample.jsonl")
DEFAULT_OUTPUT_DIR = Path("/Users/billp/Documents/AIRA/data/arm_b")
DEFAULT_TARGET = 1500
DEFAULT_OVERSAMPLE_FACTOR = 2.0
DEFAULT_SEED = 42
DEFAULT_REPO_CAP = 4
DEFAULT_STAGE_ACCEPT_LIMIT = 25
DEFAULT_REPO_PR_PAGE_BUDGET = 3
DEFAULT_MAX_PR_PAGES_PER_REPO_TOTAL = 25
DEFAULT_ZERO_YIELD_PR_THRESHOLD = 60
DEFAULT_SEARCH_STARS_MIN = 50
DEFAULT_MAX_SEARCH_PAGES = 10  # GitHub search API practical ceiling with per_page=100
DEFAULT_CHECKPOINT_EVERY_ACCEPTS = 10
DEFAULT_CHECKPOINT_EVERY_REQUESTS = 50
DEFAULT_CHECKPOINT_EVERY_SECONDS = 60

PR_DATE_START = "2022-01-01"
PR_DATE_END = "2025-12-31"

HTTP_TIMEOUT_SECONDS = 30
HTTP_CONCURRENCY = 6
HTTP_RETRY_BACKOFF_SECONDS = (2, 5, 15, 30)
RATE_LIMIT_LOW_WATER = 10

ALLOWED_STATUS = {"added", "modified"}
JUNK_PATH_MARKERS = (
    "/node_modules/",
    "/vendor/",
    "/dist/",
    "/build/",
    "/__pycache__/",
    "/.venv/",
    "/.next/",
    "/coverage/",
    "/target/",
    "/out/",
    "/site-packages/",
)
EXCLUDED_FILENAMES = {
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "poetry.lock",
    "pipfile.lock",
}

COMMIT_AI_KEYWORDS = re.compile(
    r"\b(copilot|cursor|devin|claude|codeium|tabnine|ghostwriter|"
    r"aicommit|ai[\-_]commit|github[\-_]copilot|co[\-_]pilot)\b",
    re.IGNORECASE,
)
REPO_AI_WORKFLOW_PATTERN = re.compile(
    r"(copilot|cursor|codeium|devin|tabnine|ghostwriter)",
    re.IGNORECASE,
)

OUTPUT_INDEX = "index.jsonl"
OUTPUT_ACCEPTED_LOG = "accepted_log.jsonl"
OUTPUT_EXCLUDED = "excluded.jsonl"
OUTPUT_SUMMARY = "summary.json"
OUTPUT_STATE = "state.json"
OUTPUT_LOG = "extract.log"
OUTPUT_PATCH_DIR = "patches"


class GitHubRequestFailure(RuntimeError):
    def __init__(
        self,
        *,
        url: str,
        message: str,
        category: str,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.message = message
        self.category = category
        self.status_code = status_code


class LegacyHydrationRequired(RuntimeError):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(path)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def stable_key(seed: int, *parts: object) -> str:
    material = "|".join(str(part) for part in parts)
    return hashlib.sha256(f"{seed}|{material}".encode("utf-8")).hexdigest()


def normalize_language(value: Any) -> Optional[str]:
    raw = str(value or "").strip().lower()
    aliases = {
        "python": "python",
        "javascript": "javascript",
        "java script": "javascript",
        "typescript": "typescript",
        "type script": "typescript",
    }
    return aliases.get(raw)


def display_language(language: str) -> str:
    return DISPLAY_LANGUAGE[language]


def output_languages_for_search(search_language: str) -> set[str]:
    return set(LANGUAGE_FAMILY[search_language])


def language_from_path(path: str) -> Optional[str]:
    return FILE_EXTENSIONS.get(Path(path).suffix.lower())


def repo_name_from_url(repo_url: str) -> str:
    cleaned = repo_url.strip().rstrip("/")
    if not cleaned:
        return ""
    match = re.search(r"/repos/([^/]+/[^/]+)$", cleaned)
    if match:
        return match.group(1)
    match = re.search(r"github\.com/([^/]+/[^/]+)$", cleaned)
    if match:
        return match.group(1)
    return cleaned.rsplit("/", 1)[-1]


def line_count(text: str) -> int:
    return text.count("\n") + 1 if text else 0


def file_size_band(lines: int) -> str:
    if 100 <= lines <= 299:
        return "100_299"
    if 300 <= lines <= 999:
        return "300_999"
    if lines >= 1000:
        return "1000_plus"
    return "unknown"


def patch_line_count(patch: str) -> int:
    return sum(
        1
        for line in patch.splitlines()
        if line.startswith(("+", "-")) and not line.startswith(("+++", "---"))
    )


def patch_sha(patch: str) -> str:
    return hashlib.sha256(patch.encode("utf-8")).hexdigest()[:16]


def patch_to_content(patch: str) -> str:
    lines: list[str] = []
    for raw_line in patch.splitlines():
        if raw_line.startswith(("diff --git", "index ", "@@")):
            continue
        if raw_line.startswith(("---", "+++")):
            continue
        if raw_line.startswith("+"):
            lines.append(raw_line[1:])
            continue
        if raw_line.startswith(" "):
            lines.append(raw_line[1:])
            continue
        if raw_line.startswith("-"):
            continue
        lines.append(raw_line)
    return "\n".join(lines).strip()


def looks_like_bundled_artifact(content: str) -> bool:
    head = content[:4000].lower()
    webpack_hits = head.count("__webpack_require__")
    bundle_markers = (
        "webpackbootstrap",
        "sourceurl=webpack://",
        "the \"eval\" devtool has been used",
        "webpackuniversalmoduledefinition",
        "parcelrequire",
        "vite/client",
    )
    if any(marker in head for marker in bundle_markers):
        return True
    if webpack_hits >= 5:
        return True
    return False


def filter_content_candidate(filename: str, content: str) -> tuple[str | None, dict[str, Any] | None]:
    if not filename:
        return "missing_filename", None
    if not content or not str(content).strip():
        return "missing_content", None

    normalized_path = "/" + filename.replace("\\", "/").strip("/")
    basename = Path(filename).name.lower()
    if any(marker in normalized_path for marker in JUNK_PATH_MARKERS):
        return "junk_path", None
    if basename in EXCLUDED_FILENAMES:
        return "lockfile", None
    if normalized_path.endswith(".min.js"):
        return "minified_file", None
    if Path(filename).suffix.lower() == ".ipynb":
        return "notebook", None

    language = language_from_path(filename)
    if language is None:
        return "unknown_language", None

    lowered = str(content)[:1200].lower()
    if "@generated" in lowered or "auto-generated" in lowered or "autogenerated" in lowered:
        return "generated_code", None
    if looks_like_bundled_artifact(str(content)):
        return "bundled_artifact", None

    lines = line_count(str(content))
    band = file_size_band(lines)
    if band == "unknown":
        return "line_count_out_of_range", None

    return None, {
        "path": filename,
        "content": str(content),
        "language": language,
        "file_line_count": lines,
        "size_band": band,
    }


def pr_in_date_range(pr: dict[str, Any]) -> bool:
    merged_at = str(pr.get("merged_at") or "").strip()
    if not merged_at:
        return False
    merged = datetime.fromisoformat(merged_at.replace("Z", "+00:00"))
    start = datetime.fromisoformat(PR_DATE_START).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(PR_DATE_END).replace(tzinfo=timezone.utc)
    return start <= merged <= end


def filter_patch_candidate(filename: str, patch: str | None) -> tuple[str | None, dict[str, Any] | None]:
    if not filename:
        return "missing_filename", None
    if not patch or not str(patch).strip():
        return "missing_patch", None
    content = patch_to_content(str(patch))
    reason, candidate = filter_content_candidate(filename, content)
    if reason or candidate is None:
        return reason, None
    return None, {
        **candidate,
        "patch_lines": patch_line_count(patch),
        "patch_sha": patch_sha(patch),
        "raw_patch": patch,
    }


def scale_counter_to_total(
    observed: Counter[tuple[str, str, int]],
    *,
    observed_total: int,
    scaled_total: int,
) -> Counter[tuple[str, str, int]]:
    exact = {
        key: (count / observed_total) * scaled_total
        for key, count in observed.items()
    }
    scaled = Counter({key: int(value) for key, value in exact.items()})
    remainder = scaled_total - sum(scaled.values())
    ordering = sorted(
        observed.keys(),
        key=lambda key: (exact[key] - scaled[key], key[0], key[1], key[2]),
        reverse=True,
    )
    for key in ordering[:remainder]:
        scaled[key] += 1
    return scaled


def decile_upper_bounds_by_language(
    arm_a_records: list[dict[str, Any]],
) -> dict[str, list[int]]:
    bounds: dict[str, list[int]] = {}
    for language in SEARCH_LANGUAGES:
        display = display_language(language)
        upper_bounds: list[int] = []
        fallback = 0
        for decile in SIZE_DECILES:
            values = [
                parse_int(record.get("file_line_count"))
                for record in arm_a_records
                if record.get("language") == display and parse_int(record.get("size_decile"), -1) == decile
            ]
            if values:
                fallback = max(values)
            upper_bounds.append(fallback)
        bounds[language] = upper_bounds or [0] * len(SIZE_DECILES)
    return bounds


def target_decile_for_line_count(
    language: str,
    file_line_count: int,
    *,
    decile_upper_bounds: dict[str, list[int]],
) -> int:
    bounds = decile_upper_bounds.get(language) or [0] * len(SIZE_DECILES)
    for decile, upper_bound in enumerate(bounds):
        if file_line_count <= upper_bound:
            return decile
    return SIZE_DECILES[-1]


def load_arm_a_distribution(
    *,
    arm_a_path: Path,
    target_total: int | None,
    oversample_factor: float,
) -> tuple[Counter[tuple[str, str, int]], int, int, dict[str, list[int]]]:
    normalized_records: list[dict[str, Any]] = []
    arm_a_total = 0
    with arm_a_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            language = normalize_language(record.get("language"))
            if language is None:
                continue
            content = str(record.get("content") or "")
            lines = line_count(content)
            band = file_size_band(lines)
            if band == "unknown":
                continue
            normalized_records.append(
                {
                    "repo": str(record.get("repo") or record.get("repo_name") or ""),
                    "repo_name": str(record.get("repo_name") or record.get("repo") or ""),
                    "pr_number": parse_int(record.get("pr_number") or record.get("pr_identifier")),
                    "pr_identifier": str(record.get("pr_identifier") or record.get("pr_number") or ""),
                    "file_path": str(record.get("file_path") or record.get("path") or ""),
                    "path": str(record.get("path") or record.get("file_path") or ""),
                    "language": display_language(language),
                    "file_line_count": lines,
                    "size_band": band,
                }
            )
            arm_a_total += 1

    if arm_a_total <= 0:
        raise RuntimeError(f"No valid Arm A distribution could be derived from {arm_a_path}")

    if target_total is None or target_total <= 0:
        scaled_total = int(math.ceil(arm_a_total * oversample_factor))
    else:
        scaled_total = int(target_total)

    enriched_arm_a = assign_size_deciles(normalized_records, seed=0)
    observed: Counter[tuple[str, str, int]] = Counter()
    for record in enriched_arm_a:
        language = normalize_language(record.get("language"))
        band = str(record.get("size_band") or "")
        decile = parse_int(record.get("size_decile"), -1)
        if language is None or band not in SIZE_BANDS or decile not in SIZE_DECILES:
            continue
        observed[(language, band, decile)] += 1

    scaled = scale_counter_to_total(
        observed,
        observed_total=arm_a_total,
        scaled_total=scaled_total,
    )

    for language in SEARCH_LANGUAGES:
        for band in SIZE_BANDS:
            for decile in SIZE_DECILES:
                scaled.setdefault((language, band, decile), 0)

    return scaled, arm_a_total, scaled_total, decile_upper_bounds_by_language(enriched_arm_a)


def aggregate_counter_by_band(counter: Counter[tuple[str, str, int]]) -> Counter[tuple[str, str]]:
    aggregated: Counter[tuple[str, str]] = Counter()
    for (language, band, _decile), count in counter.items():
        aggregated[(language, band)] += count
    return aggregated


def serialize_counter_by_band(counter: Counter[tuple[str, str, int]]) -> dict[str, int]:
    aggregated = aggregate_counter_by_band(counter)
    return {
        f"{language}:{band}": aggregated[(language, band)]
        for language in SEARCH_LANGUAGES
        for band in SIZE_BANDS
        if aggregated.get((language, band), 0)
    }


def serialize_counter_by_strict_cell(counter: Counter[tuple[str, str, int]]) -> dict[str, int]:
    return {
        f"{language}:{band}:{decile}": counter[(language, band, decile)]
        for language in SEARCH_LANGUAGES
        for band in SIZE_BANDS
        for decile in SIZE_DECILES
        if counter.get((language, band, decile), 0)
    }


def deserialize_counter_by_strict_cell(payload: dict[str, Any]) -> Counter[tuple[str, str, int]]:
    counter: Counter[tuple[str, str, int]] = Counter()
    for key, value in payload.items():
        try:
            language, band, decile_text = key.split(":", 2)
        except ValueError:
            continue
        decile = parse_int(decile_text, -1)
        if language in SEARCH_LANGUAGES and band in SIZE_BANDS and decile in SIZE_DECILES:
            counter[(language, band, decile)] = parse_int(value)
    return counter


def total_remaining(counter: Counter[tuple[str, str, int]]) -> int:
    return sum(max(value, 0) for value in counter.values())


def remaining_for_search_language(counter: Counter[tuple[str, str, int]], search_language: str) -> int:
    return sum(
        count
        for (language, _band, _decile), count in counter.items()
        if language in output_languages_for_search(search_language)
    )


def top_repo_counts(records: list[dict[str, Any]], topn: int = 15) -> list[list[Any]]:
    counts: Counter[str] = Counter()
    for record in records:
        repo = str(record.get("repo") or record.get("repo_name") or "").strip()
        if repo:
            counts[repo] += 1
    return [[repo, count] for repo, count in counts.most_common(topn)]


def assign_size_deciles(records: list[dict[str, Any]], *, seed: int) -> list[dict[str, Any]]:
    by_language: dict[str, list[dict[str, Any]]] = {display_language(language): [] for language in SEARCH_LANGUAGES}
    for record in records:
        by_language.setdefault(str(record.get("language") or ""), []).append(dict(record))

    enriched: list[dict[str, Any]] = []
    for language in DISPLAY_LANGUAGE.values():
        group = by_language.get(language, [])
        if not group:
            continue
        ordered = sorted(
            group,
            key=lambda record: (
                parse_int(record.get("file_line_count")),
                str(record.get("repo") or record.get("repo_name") or ""),
                str(record.get("pr_number") or record.get("pr_identifier") or ""),
                str(record.get("file_path") or record.get("path") or ""),
            ),
        )
        total = len(ordered)
        for index, record in enumerate(ordered):
            rank = index / max(total - 1, 1)
            record["size_decile"] = min(9, int(rank * 10))
            enriched.append(record)
    return sorted(
        enriched,
        key=lambda record: stable_key(
            seed,
            record.get("repo") or record.get("repo_name") or "",
            record.get("pr_number") or record.get("pr_identifier") or "",
            record.get("file_path") or record.get("path") or "",
            record.get("patch_sha") or "",
        ),
    )


class GitHubClient:
    BASE = "https://api.github.com"

    def __init__(self, token: str):
        self._token = token
        self._sem = asyncio.Semaphore(HTTP_CONCURRENCY)
        self._client: Optional[httpx.AsyncClient] = None
        self.request_count = 0
        self.status_counts: Counter[str] = Counter()
        self.last_successful_request_at: str | None = None

    async def __aenter__(self) -> "GitHubClient":
        self._client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "AIRA-ArmB-Extractor",
            },
            timeout=HTTP_TIMEOUT_SECONDS,
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        assert self._client is not None
        await self._client.aclose()

    async def _get_json(self, url: str, *, params: dict[str, Any] | None = None, allow_404: bool = False) -> Any:
        assert self._client is not None
        last_failure: GitHubRequestFailure | None = None

        for attempt, wait_seconds in enumerate(HTTP_RETRY_BACKOFF_SECONDS, start=1):
            async with self._sem:
                try:
                    response = await self._client.get(url, params=params)
                except httpx.RequestError as exc:
                    message = f"{exc.__class__.__name__}: {exc}"
                    logging.warning("Request error %s (attempt %d/%d): %s", url, attempt, len(HTTP_RETRY_BACKOFF_SECONDS), message)
                    last_failure = GitHubRequestFailure(
                        url=url,
                        message=message,
                        category="network",
                    )
                    if attempt >= len(HTTP_RETRY_BACKOFF_SECONDS):
                        raise last_failure
                    await asyncio.sleep(wait_seconds)
                    continue

                self.request_count += 1
                self.status_counts[str(response.status_code)] += 1

                if response.status_code == 200:
                    self.last_successful_request_at = now_utc_iso()
                    remaining = parse_int(response.headers.get("x-ratelimit-remaining"), 999)
                    reset_at = parse_int(response.headers.get("x-ratelimit-reset"), 0)
                    if remaining < RATE_LIMIT_LOW_WATER and reset_at > 0:
                        pause_seconds = max(0, reset_at - int(time.time())) + 2
                        logging.info("Rate limit low (%d remaining) — sleeping %ds", remaining, pause_seconds)
                        await asyncio.sleep(pause_seconds)
                    return response.json()

                if response.status_code == 404 and allow_404:
                    return None

                if response.status_code in {403, 429}:
                    remaining = parse_int(response.headers.get("x-ratelimit-remaining"), -1)
                    reset_at = parse_int(response.headers.get("x-ratelimit-reset"), 0)
                    retry_after = parse_int(response.headers.get("retry-after"), wait_seconds)
                    if remaining == 0 and reset_at > 0:
                        retry_after = max(0, reset_at - int(time.time())) + 2
                    logging.warning(
                        "Rate/permission response %d for %s (attempt %d/%d) — waiting %ds",
                        response.status_code,
                        url,
                        attempt,
                        len(HTTP_RETRY_BACKOFF_SECONDS),
                        retry_after,
                    )
                    last_failure = GitHubRequestFailure(
                        url=url,
                        message=f"HTTP {response.status_code}",
                        category="rate_limit",
                        status_code=response.status_code,
                    )
                    if attempt >= len(HTTP_RETRY_BACKOFF_SECONDS):
                        raise last_failure
                    await asyncio.sleep(retry_after)
                    continue

                if response.status_code in {500, 502, 503, 504}:
                    logging.warning(
                        "Transient HTTP %d for %s (attempt %d/%d)",
                        response.status_code,
                        url,
                        attempt,
                        len(HTTP_RETRY_BACKOFF_SECONDS),
                    )
                    last_failure = GitHubRequestFailure(
                        url=url,
                        message=f"HTTP {response.status_code}",
                        category="transient_http",
                        status_code=response.status_code,
                    )
                    if attempt >= len(HTTP_RETRY_BACKOFF_SECONDS):
                        raise last_failure
                    await asyncio.sleep(wait_seconds)
                    continue

                message = f"Unhandled HTTP {response.status_code}: {response.text[:200]}"
                raise GitHubRequestFailure(
                    url=url,
                    message=message,
                    category="http",
                    status_code=response.status_code,
                )

        if last_failure is None:
            raise GitHubRequestFailure(url=url, message="Unknown request failure", category="unknown")
        raise last_failure

    async def search_repos(self, *, search_language: str, page: int, stars_min: int) -> list[dict[str, Any]]:
        payload = await self._get_json(
            f"{self.BASE}/search/repositories",
            params={
                "q": f"language:{display_language(search_language)} pushed:>={PR_DATE_START} stars:>={stars_min} fork:false archived:false",
                "sort": "updated",
                "order": "desc",
                "per_page": 100,
                "page": page,
            },
        )
        return list((payload or {}).get("items", []))

    async def get_repo_metadata(self, repo: str) -> dict[str, Any]:
        payload = await self._get_json(f"{self.BASE}/repos/{repo}")
        return dict(payload or {})

    async def _get_text_url(self, url: str, *, allow_404: bool = False) -> str | None:
        assert self._client is not None
        last_failure: GitHubRequestFailure | None = None

        for attempt, wait_seconds in enumerate(HTTP_RETRY_BACKOFF_SECONDS, start=1):
            async with self._sem:
                try:
                    response = await self._client.get(url)
                except httpx.RequestError as exc:
                    message = f"{exc.__class__.__name__}: {exc}"
                    logging.warning("Request error %s (attempt %d/%d): %s", url, attempt, len(HTTP_RETRY_BACKOFF_SECONDS), message)
                    last_failure = GitHubRequestFailure(
                        url=url,
                        message=message,
                        category="network",
                    )
                    if attempt >= len(HTTP_RETRY_BACKOFF_SECONDS):
                        raise last_failure
                    await asyncio.sleep(wait_seconds)
                    continue

                self.request_count += 1
                self.status_counts[str(response.status_code)] += 1

                if response.status_code == 200:
                    self.last_successful_request_at = now_utc_iso()
                    return response.text
                if response.status_code == 404 and allow_404:
                    return None
                if response.status_code in {403, 429, 500, 502, 503, 504}:
                    last_failure = GitHubRequestFailure(
                        url=url,
                        message=f"HTTP {response.status_code}",
                        category="http",
                        status_code=response.status_code,
                    )
                    if attempt >= len(HTTP_RETRY_BACKOFF_SECONDS):
                        raise last_failure
                    await asyncio.sleep(wait_seconds)
                    continue
                raise GitHubRequestFailure(
                    url=url,
                    message=f"Unhandled HTTP {response.status_code}: {response.text[:200]}",
                    category="http",
                    status_code=response.status_code,
                )

        if last_failure is None:
            raise GitHubRequestFailure(url=url, message="Unknown text request failure", category="unknown")
        raise last_failure

    async def get_repo_tree(self, repo: str, default_branch: str) -> dict[str, Any]:
        payload = await self._get_json(
            f"{self.BASE}/repos/{repo}/git/trees/{default_branch}",
            params={"recursive": 1},
        )
        return dict(payload or {})

    async def file_exists(self, repo: str, path: str) -> bool:
        payload = await self._get_json(
            f"{self.BASE}/repos/{repo}/contents/{path}",
            allow_404=True,
        )
        return payload is not None

    async def list_workflows(self, repo: str) -> list[dict[str, Any]]:
        payload = await self._get_json(
            f"{self.BASE}/repos/{repo}/actions/workflows",
            params={"per_page": 100},
        )
        return list((payload or {}).get("workflows", []))

    async def get_file_text(self, repo: str, path: str, *, ref: str | None = None, allow_404: bool = False) -> str | None:
        params: dict[str, Any] = {}
        if ref:
            params["ref"] = ref
        payload = await self._get_json(
            f"{self.BASE}/repos/{repo}/contents/{path}",
            params=params or None,
            allow_404=allow_404,
        )
        if payload is None:
            return None
        if isinstance(payload, list):
            return None
        encoding = str(payload.get("encoding") or "").lower()
        content = payload.get("content")
        if encoding == "base64" and content:
            try:
                return base64.b64decode(str(content), validate=False).decode("utf-8")
            except (binascii.Error, UnicodeDecodeError):
                return None
        if isinstance(content, str) and content:
            return content
        download_url = str(payload.get("download_url") or "").strip()
        if download_url:
            return await self._get_text_url(download_url, allow_404=allow_404)
        return None

    async def list_closed_prs(self, repo: str, page: int) -> list[dict[str, Any]]:
        payload = await self._get_json(
            f"{self.BASE}/repos/{repo}/pulls",
            params={
                "state": "closed",
                "sort": "updated",
                "direction": "desc",
                "per_page": 100,
                "page": page,
            },
            allow_404=True,
        )
        return list(payload or [])

    async def get_pr_commits(self, repo: str, pr_number: int) -> list[dict[str, Any]]:
        payload = await self._get_json(
            f"{self.BASE}/repos/{repo}/pulls/{pr_number}/commits",
            params={"per_page": 100},
            allow_404=True,
        )
        return list(payload or [])

    async def get_pr_files(self, repo: str, pr_number: int) -> list[dict[str, Any]]:
        payload = await self._get_json(
            f"{self.BASE}/repos/{repo}/pulls/{pr_number}/files",
            params={"per_page": 100},
            allow_404=True,
        )
        return list(payload or [])


class ArmBExtractor:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.arm_a_path = Path(args.arm_a).expanduser().resolve()
        self.output_dir = Path(args.output_dir).expanduser().resolve()
        self.index_path = self.output_dir / OUTPUT_INDEX
        self.accepted_log_path = self.output_dir / OUTPUT_ACCEPTED_LOG
        self.excluded_path = self.output_dir / OUTPUT_EXCLUDED
        self.summary_path = self.output_dir / OUTPUT_SUMMARY
        self.state_path = self.output_dir / OUTPUT_STATE
        self.log_path = self.output_dir / OUTPUT_LOG
        self.patch_dir = self.output_dir / OUTPUT_PATCH_DIR

        self.targets, self.arm_a_total, self.target_total, self.arm_a_decile_upper_bounds = load_arm_a_distribution(
            arm_a_path=self.arm_a_path,
            target_total=(args.target if args.target > 0 else None),
            oversample_factor=args.oversample_factor,
        )
        self.remaining_targets: Counter[tuple[str, str, int]] = Counter(self.targets)

        self.records: list[dict[str, Any]] = []
        self.excluded_count = 0
        self.excluded_by_reason: Counter[str] = Counter()
        self.included_by_language: Counter[str] = Counter()
        self.included_by_cell: Counter[tuple[str, str, int]] = Counter()
        self.repo_counts: Counter[str] = Counter()
        self.repo_band_counts: Counter[tuple[str, str, str]] = Counter()
        self.seen_patch_shas: set[str] = set()
        self.seen_file_keys: set[tuple[str, str, str]] = set()
        self.completed_prs: set[tuple[str, str]] = set()
        self.zero_yield_repos: set[str] = set()
        self.logged_repo_exclusions: set[tuple[str, str]] = set()
        self.pending_legacy_records: list[dict[str, Any]] = []

        self.search_pages = {language: 1 for language in SEARCH_LANGUAGES}
        self.search_exhausted = {language: False for language in SEARCH_LANGUAGES}
        self.search_stop_reason = {language: "" for language in SEARCH_LANGUAGES}
        self.repo_queue_by_search: dict[str, list[str]] = {language: [] for language in SEARCH_LANGUAGES}
        self.repo_cursors: dict[str, dict[str, Any]] = {}
        self.repo_ai_cache: dict[str, dict[str, Any]] = {}

        self.resume_used = False
        self.resume_loaded_from_state = False
        self.current_search_index = 0
        self.current_search_language = SEARCH_LANGUAGES[0]
        self.current_repo: str | None = None
        self.current_phase = "startup"

        self.migrated_existing_records = 0
        self.dropped_existing_records = 0
        self.overfilled_existing_cells: Counter[tuple[str, str, int]] = Counter()

        self.api_requests_offset = 0
        self.request_status_offset: Counter[str] = Counter()
        self.last_successful_request_at: str | None = None
        self.last_error: str | None = None

        self.accepted_since_checkpoint = 0
        self.requests_at_last_checkpoint = 0
        self.last_checkpoint_monotonic = time.monotonic()

    def output_languages_remaining(self) -> set[str]:
        return {
            language
            for (language, _band, _decile), count in self.remaining_targets.items()
            if count > 0
        }

    def prioritized_search_languages(self) -> list[str]:
        start = self.current_search_index % len(SEARCH_LANGUAGES)
        rotated = [SEARCH_LANGUAGES[(start + offset) % len(SEARCH_LANGUAGES)] for offset in range(len(SEARCH_LANGUAGES))]
        return sorted(
            rotated,
            key=lambda language: remaining_for_search_language(self.remaining_targets, language),
            reverse=True,
        )

    def arm_a_target_decile_for(self, language: str, file_line_count: int) -> int:
        return target_decile_for_line_count(
            language,
            file_line_count,
            decile_upper_bounds=self.arm_a_decile_upper_bounds,
        )

    def target_cell_for(self, language: str, size_band: str, file_line_count: int) -> tuple[str, str, int]:
        return (
            language,
            size_band,
            self.arm_a_target_decile_for(language, file_line_count),
        )

    def ensure_output_dir(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.patch_dir.mkdir(parents=True, exist_ok=True)

        existing = [
            path
            for path in (self.index_path, self.accepted_log_path, self.excluded_path, self.summary_path, self.state_path, self.log_path)
            if path.exists()
        ]
        if self.args.fresh:
            for path in existing:
                if path.is_file():
                    path.unlink()
            if self.patch_dir.exists():
                shutil.rmtree(self.patch_dir)
            self.patch_dir.mkdir(parents=True, exist_ok=True)
            return

        if existing and not self.args.resume:
            raise RuntimeError(
                f"Refusing to start with existing output in {self.output_dir}. "
                "Use --resume to continue or --fresh to reset."
            )

    def backup_legacy_outputs(self) -> None:
        index_backup = self.output_dir / "index.legacy.backup.jsonl"
        accepted_backup = self.output_dir / "accepted_log.legacy.backup.jsonl"
        excluded_backup = self.output_dir / "excluded.legacy.backup.jsonl"
        if self.index_path.exists() and not index_backup.exists():
            shutil.copy2(self.index_path, index_backup)
        if self.accepted_log_path.exists() and not accepted_backup.exists():
            shutil.copy2(self.accepted_log_path, accepted_backup)
        if self.excluded_path.exists() and not excluded_backup.exists():
            shutil.copy2(self.excluded_path, excluded_backup)

    def load_resume_artifacts(self) -> None:
        if not self.args.resume:
            return

        self.resume_used = True
        legacy_state_loaded = False

        if self.state_path.exists():
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if parse_int(payload.get("state_version")) in {2, 3, STATE_VERSION}:
                self.validate_resume_configuration(payload)
                self.load_state_payload(payload)
                legacy_state_loaded = True
                self.resume_loaded_from_state = True

        if self.excluded_path.exists():
            self.load_existing_exclusions()

        if self.accepted_log_path.exists():
            self.load_existing_records(self.accepted_log_path)
        elif self.index_path.exists():
            if not legacy_state_loaded:
                self.backup_legacy_outputs()
            self.load_existing_records(self.index_path)

        logging.info(
            "Resume state loaded: records=%d excluded=%d remaining=%d state_file=%s pending_legacy=%d",
            len(self.records),
            self.excluded_count,
            total_remaining(self.remaining_targets),
            self.resume_loaded_from_state,
            len(self.pending_legacy_records),
        )

    def validate_resume_configuration(self, payload: dict[str, Any]) -> None:
        expected = {
            "arm_a_path": str(self.arm_a_path),
            "target_total": self.target_total,
            "seed": self.args.seed,
        }
        for key, expected_value in expected.items():
            actual_value = payload.get(key)
            if actual_value != expected_value:
                raise RuntimeError(
                    f"Cannot resume with different {key}: existing={actual_value!r} requested={expected_value!r}"
                )
        existing_repo_cap = parse_int(payload.get("repo_cap"), self.args.repo_cap)
        if self.args.repo_cap > existing_repo_cap:
            raise RuntimeError(
                f"Cannot resume with looser repo_cap: existing={existing_repo_cap!r} requested={self.args.repo_cap!r}"
            )

    def load_state_payload(self, payload: dict[str, Any]) -> None:
        self.search_pages.update({key: parse_int(value, 1) for key, value in payload.get("search_pages", {}).items()})
        self.search_exhausted.update({key: bool(value) for key, value in payload.get("search_exhausted", {}).items()})
        self.search_stop_reason.update({key: str(value or "") for key, value in payload.get("search_stop_reason", {}).items()})
        self.repo_queue_by_search.update(
            {
                key: [str(repo) for repo in value]
                for key, value in payload.get("repo_queue_by_search", {}).items()
                if key in SEARCH_LANGUAGES
            }
        )
        self.repo_cursors.update({str(repo): dict(cursor) for repo, cursor in payload.get("repo_cursors", {}).items()})
        self.repo_ai_cache.update({str(repo): dict(entry) for repo, entry in payload.get("repo_ai_cache", {}).items()})
        self.zero_yield_repos.update(str(repo) for repo in payload.get("zero_yield_repos", []))
        completed_prs_payload = payload.get("completed_prs") or payload.get("seen_prs", [])
        self.completed_prs.update((str(repo), str(pr_identifier)) for repo, pr_identifier in completed_prs_payload)
        self.current_search_index = parse_int(payload.get("current_search_index"), 0)
        self.current_search_language = str(payload.get("current_search_language") or SEARCH_LANGUAGES[0])
        self.current_phase = str(payload.get("current_phase") or "resume")
        self.current_repo = str(payload.get("current_repo") or "") or None
        self.api_requests_offset = parse_int(payload.get("api_requests"), 0)
        self.request_status_offset.update({str(key): parse_int(value) for key, value in payload.get("request_status_counts", {}).items()})
        self.last_successful_request_at = str(payload.get("last_successful_request_at") or "") or None
        self.last_error = str(payload.get("last_error") or "") or None
        for cursor in self.repo_cursors.values():
            cursor.setdefault("pr_page", 1)
            cursor.setdefault("pr_index", 0)
            cursor.setdefault("scanned_prs", 0)
            cursor.setdefault("yielded_total", 0)
            cursor.setdefault("accepted_total", 0)
            cursor.setdefault("exhausted", False)
            cursor.setdefault("exhausted_reason", "")
            cursor.setdefault("active_pr_number", 0)
            cursor.setdefault("active_pr_commit_checked", False)
            cursor.setdefault("file_index", 0)

    def load_existing_exclusions(self) -> None:
        with self.excluded_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                reason = str(payload.get("reason") or payload.get("exclusion_signal") or "legacy_exclusion")
                self.excluded_by_reason[reason] += 1
                self.excluded_count += 1
                stage = str(payload.get("stage") or "")
                repo = str(payload.get("repo") or "").strip()
                if stage == "repo" and repo:
                    self.logged_repo_exclusions.add((repo, reason))

    def load_existing_records(self, source_path: Path) -> None:
        kept: list[dict[str, Any]] = []
        migration_exclusions: list[dict[str, Any]] = []

        with source_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                line = line.strip()
                if not line:
                    continue
                raw = json.loads(line)
                try:
                    record = self.normalize_existing_record(raw)
                except LegacyHydrationRequired as exc:
                    self.pending_legacy_records.append(
                        {
                            "line_number": line_number,
                            "raw": raw,
                            "detail": exc.message,
                        }
                    )
                    continue
                except RuntimeError as exc:
                    migration_exclusions.append(
                        {
                            "repo": str(raw.get("repo") or raw.get("repo_name") or ""),
                            "pr_number": parse_int(raw.get("pr_number") or raw.get("pr_identifier")),
                            "file_path": str(raw.get("file_path") or raw.get("path") or ""),
                            "language": display_language(normalize_language(raw.get("language")) or "javascript")
                            if normalize_language(raw.get("language")) in DISPLAY_LANGUAGE
                            else str(raw.get("language") or ""),
                            "reason": "migrated_existing_invalid",
                            "detail": f"line {line_number}: {exc}",
                            "stage": "migration",
                            "timestamp": now_utc_iso(),
                        }
                    )
                    self.dropped_existing_records += 1
                    self.excluded_by_reason["migrated_existing_invalid"] += 1
                    self.excluded_count += 1
                    continue

                kept.append(record)
                self.migrated_existing_records += 1

        self.records = kept
        for record in self.records:
            self.register_existing_record(record)

        for payload in migration_exclusions:
            append_jsonl(self.excluded_path, payload)

        if self.records:
            self.write_accepted_log()
        if migration_exclusions or self.records:
            self.write_index()

    def normalize_existing_record(self, raw: dict[str, Any]) -> dict[str, Any]:
        repo = str(raw.get("repo") or raw.get("repo_name") or "").strip()
        if not repo:
            raise RuntimeError("missing repo name")
        path = str(raw.get("file_path") or raw.get("path") or "").strip()
        if not path:
            raise RuntimeError("missing file path")
        pr_number = parse_int(raw.get("pr_number") or raw.get("pr_identifier"))
        if pr_number <= 0:
            raise RuntimeError("missing PR number")

        repo_url = str(raw.get("repo_url") or f"https://github.com/{repo}")
        merged_at = str(raw.get("pr_merged_at") or "")
        commit_sha = str(raw.get("commit_sha") or "")
        stars = parse_int(raw.get("repo_stars"), 0)

        reason: str | None = None
        candidate: dict[str, Any] | None = None
        patch_sha_value = str(raw.get("patch_sha") or "").strip()
        patch_text = ""
        content = str(raw.get("content") or "")

        if content:
            reason, candidate = filter_content_candidate(path, content)
        elif patch_sha_value:
            patch_path = self.patch_dir / f"{patch_sha_value}.patch"
            if patch_path.exists():
                patch_text = patch_path.read_text(encoding="utf-8")
                reason, candidate = filter_patch_candidate(path, patch_text)

        if (reason == "line_count_out_of_range" or candidate is None) and commit_sha and not content:
            raise LegacyHydrationRequired(
                f"legacy record requires remote content hydration: {reason or 'missing_candidate'}"
            )
        if reason or candidate is None:
            raise RuntimeError(f"legacy record failed normalization: {reason or 'missing_candidate'}")

        normalized_language = candidate["language"]
        content = candidate["content"]
        file_line_count = candidate["file_line_count"]
        size_band = candidate["size_band"]
        patch_lines = parse_int(raw.get("patch_lines"), candidate.get("patch_lines"),)
        if patch_lines <= 0:
            patch_lines = parse_int(candidate.get("patch_lines"), file_line_count)
        if not patch_sha_value:
            patch_sha_value = str(candidate.get("patch_sha") or "")
        if not patch_sha_value:
            patch_sha_value = stable_key(self.args.seed, repo, pr_number, path, file_line_count)[:16]

        return {
            "repo": repo,
            "repo_name": repo,
            "repo_url": repo_url,
            "repo_stars": stars,
            "pr_number": pr_number,
            "pr_identifier": str(pr_number),
            "pr_merged_at": merged_at,
            "commit_sha": commit_sha,
            "file_path": path,
            "path": path,
            "language": display_language(normalized_language),
            "content": content,
            "file_line_count": file_line_count,
            "size_band": size_band,
            "arm_a_target_decile": self.arm_a_target_decile_for(normalized_language, file_line_count),
            "patch_lines": patch_lines,
            "size_decile": parse_int(raw.get("size_decile"), 0),
            "patch_sha": patch_sha_value,
            "ai_excluded": False,
            "exclusion_signal": None,
        }

    def register_existing_record(self, record: dict[str, Any]) -> None:
        internal_language = normalize_language(record.get("language"))
        band = str(record.get("size_band") or "")
        file_line_count = parse_int(record.get("file_line_count"))
        repo = str(record.get("repo") or "")
        path = str(record.get("file_path") or record.get("path") or "")
        pr_identifier = str(record.get("pr_number") or record.get("pr_identifier") or "")
        patch_sha_value = str(record.get("patch_sha") or "")

        if internal_language is None or band not in SIZE_BANDS:
            raise RuntimeError(f"invalid existing record shape for {repo} {path}")

        self.included_by_language[internal_language] += 1
        target_decile = parse_int(record.get("arm_a_target_decile"), -1)
        if target_decile not in SIZE_DECILES:
            target_decile = self.arm_a_target_decile_for(internal_language, file_line_count)
            record["arm_a_target_decile"] = target_decile
        strict_cell = (internal_language, band, target_decile)
        self.included_by_cell[strict_cell] += 1
        if self.remaining_targets[strict_cell] > 0:
            self.remaining_targets[strict_cell] -= 1
        else:
            self.overfilled_existing_cells[strict_cell] += 1

        self.repo_counts[repo] += 1
        self.repo_band_counts[(repo, internal_language, band)] += 1
        self.seen_patch_shas.add(patch_sha_value)
        self.seen_file_keys.add((repo, path, pr_identifier))

    def total_api_requests(self, client: GitHubClient | None) -> int:
        return self.api_requests_offset + (client.request_count if client is not None else 0)

    def total_request_status_counts(self, client: GitHubClient | None) -> Counter[str]:
        counts = Counter(self.request_status_offset)
        if client is not None:
            counts.update(client.status_counts)
        return counts

    def effective_last_successful_request_at(self, client: GitHubClient | None) -> str | None:
        if client is not None and client.last_successful_request_at:
            return client.last_successful_request_at
        return self.last_successful_request_at

    def write_accepted_log(self) -> None:
        lines = [json.dumps(record, sort_keys=True) for record in self.records]
        atomic_write_text(self.accepted_log_path, "\n".join(lines) + ("\n" if lines else ""))

    def write_index(self) -> None:
        enriched = assign_size_deciles(self.records, seed=self.args.seed)
        lines = [json.dumps(record, sort_keys=True) for record in enriched]
        atomic_write_text(self.index_path, "\n".join(lines) + ("\n" if lines else ""))

    def maybe_checkpoint(
        self,
        *,
        client: GitHubClient | None,
        force: bool = False,
        complete: bool = False,
        interrupted: bool = False,
        fatal_error: str | None = None,
    ) -> None:
        elapsed = time.monotonic() - self.last_checkpoint_monotonic
        request_delta = self.total_api_requests(client) - self.requests_at_last_checkpoint
        if not force:
            if self.accepted_since_checkpoint < self.args.checkpoint_every_accepts:
                if request_delta < self.args.checkpoint_every_requests and elapsed < self.args.checkpoint_every_seconds:
                    return
        self.write_summary_and_state(
            client=client,
            complete=complete,
            interrupted=interrupted,
            fatal_error=fatal_error,
        )
        self.accepted_since_checkpoint = 0
        self.requests_at_last_checkpoint = self.total_api_requests(client)
        self.last_checkpoint_monotonic = time.monotonic()

    def write_summary_and_state(
        self,
        *,
        client: GitHubClient | None,
        complete: bool,
        interrupted: bool,
        fatal_error: str | None,
    ) -> None:
        self.write_index()
        summary = {
            "output_dir": str(self.output_dir),
            "index_path": str(self.index_path),
            "accepted_log_path": str(self.accepted_log_path),
            "excluded_path": str(self.excluded_path),
            "patch_dir": str(self.patch_dir),
            "state_path": str(self.state_path),
            "arm_a_path": str(self.arm_a_path),
            "arm_a_total": self.arm_a_total,
            "target_total": self.target_total,
            "oversample_factor": self.args.oversample_factor,
            "seed": self.args.seed,
            "repo_cap": self.args.repo_cap,
            "stage_accept_limit": self.args.stage_accept_limit,
            "repo_pr_page_budget_per_turn": self.args.repo_pr_page_budget_per_turn,
            "max_pr_pages_per_repo_total": self.args.max_pr_pages_per_repo_total,
            "zero_yield_pr_threshold": self.args.zero_yield_pr_threshold,
            "search_stars_min": self.args.search_stars_min,
            "max_search_pages": self.args.max_search_pages,
            "resume": self.args.resume,
            "resume_loaded_from_state": self.resume_loaded_from_state,
            "complete": complete,
            "interrupted": interrupted,
            "fatal_error": fatal_error,
            "current_phase": self.current_phase,
            "current_search_language": self.current_search_language,
            "current_repo": self.current_repo,
            "accepted_total": len(self.records),
            "included_by_language": dict(sorted(self.included_by_language.items())),
            "included_by_language_band": dict(sorted(serialize_counter_by_band(self.included_by_cell).items())),
            "target_by_language_band": dict(sorted(serialize_counter_by_band(self.targets).items())),
            "remaining_total": total_remaining(self.remaining_targets),
            "remaining_by_language_band": dict(sorted(serialize_counter_by_band(self.remaining_targets).items())),
            "overfilled_existing_by_language_band": dict(sorted(serialize_counter_by_band(self.overfilled_existing_cells).items())),
            "included_by_language_band_decile": dict(sorted(serialize_counter_by_strict_cell(self.included_by_cell).items())),
            "target_by_language_band_decile": dict(sorted(serialize_counter_by_strict_cell(self.targets).items())),
            "remaining_by_language_band_decile": dict(sorted(serialize_counter_by_strict_cell(self.remaining_targets).items())),
            "overfilled_existing_by_language_band_decile": dict(sorted(serialize_counter_by_strict_cell(self.overfilled_existing_cells).items())),
            "excluded_count": self.excluded_count,
            "excluded_by_reason": dict(sorted(self.excluded_by_reason.items())),
            "api_requests": self.total_api_requests(client),
            "request_status_counts": dict(sorted(self.total_request_status_counts(client).items())),
            "last_successful_request_at": self.effective_last_successful_request_at(client),
            "last_error": fatal_error or self.last_error,
            "search_pages": dict(self.search_pages),
            "search_exhausted": dict(self.search_exhausted),
            "search_stop_reason": dict(self.search_stop_reason),
            "queued_repos_by_search_language": {key: len(value) for key, value in self.repo_queue_by_search.items()},
            "zero_yield_repo_count": len(self.zero_yield_repos),
            "zero_yield_repos_sample": sorted(self.zero_yield_repos)[:25],
            "completed_pr_count": len(self.completed_prs),
            "pending_legacy_records": len(self.pending_legacy_records),
            "unique_repos": len(self.repo_counts),
            "repos_at_repo_cap": sum(1 for count in self.repo_counts.values() if count >= self.args.repo_cap),
            "repos_above_repo_cap": sum(1 for count in self.repo_counts.values() if count > self.args.repo_cap),
            "max_files_per_repo": max(self.repo_counts.values(), default=0),
            "top_repo_counts": top_repo_counts(self.records),
            "migrated_existing_records": self.migrated_existing_records,
            "dropped_existing_records": self.dropped_existing_records,
            "timestamp": now_utc_iso(),
        }
        atomic_write_json(self.summary_path, summary)

        state_payload = {
            "state_version": STATE_VERSION,
            "arm_a_path": str(self.arm_a_path),
            "target_total": self.target_total,
            "seed": self.args.seed,
            "repo_cap": self.args.repo_cap,
            "search_pages": self.search_pages,
            "search_exhausted": self.search_exhausted,
            "search_stop_reason": self.search_stop_reason,
            "repo_queue_by_search": self.repo_queue_by_search,
            "repo_cursors": self.repo_cursors,
            "repo_ai_cache": self.repo_ai_cache,
            "zero_yield_repos": sorted(self.zero_yield_repos),
            "current_search_index": self.current_search_index,
            "current_search_language": self.current_search_language,
            "current_repo": self.current_repo,
            "current_phase": self.current_phase,
            "targets_by_strict_cell": serialize_counter_by_strict_cell(self.targets),
            "remaining_targets_by_strict_cell": serialize_counter_by_strict_cell(self.remaining_targets),
            "included_by_strict_cell": serialize_counter_by_strict_cell(self.included_by_cell),
            "targets_by_band": serialize_counter_by_band(self.targets),
            "remaining_targets_by_band": serialize_counter_by_band(self.remaining_targets),
            "included_by_band": serialize_counter_by_band(self.included_by_cell),
            "included_by_language": dict(self.included_by_language),
            "completed_prs": sorted(list(self.completed_prs)),
            "api_requests": self.total_api_requests(client),
            "request_status_counts": dict(self.total_request_status_counts(client)),
            "last_successful_request_at": self.effective_last_successful_request_at(client),
            "last_error": fatal_error or self.last_error,
            "migrated_existing_records": self.migrated_existing_records,
            "dropped_existing_records": self.dropped_existing_records,
            "pending_legacy_records": len(self.pending_legacy_records),
            "timestamp": now_utc_iso(),
        }
        atomic_write_json(self.state_path, state_payload)

    def append_exclusion(
        self,
        *,
        repo: str,
        pr_number: int,
        file_path: str,
        language: str | None,
        reason: str,
        detail: str,
        stage: str,
    ) -> None:
        payload = {
            "timestamp": now_utc_iso(),
            "repo": repo,
            "pr_number": pr_number,
            "file_path": file_path,
            "language": display_language(language) if language in DISPLAY_LANGUAGE else (language or ""),
            "reason": reason,
            "detail": detail,
            "stage": stage,
        }
        append_jsonl(self.excluded_path, payload)
        self.excluded_by_reason[reason] += 1
        self.excluded_count += 1
        if stage == "repo" and repo:
            self.logged_repo_exclusions.add((repo, reason))

    def accept_candidate(
        self,
        *,
        repo: str,
        repo_url: str,
        repo_stars: int,
        pr_number: int,
        pr_merged_at: str,
        commit_sha: str,
        candidate: dict[str, Any],
    ) -> None:
        language = candidate["language"]
        band = candidate["size_band"]
        target_decile = self.arm_a_target_decile_for(language, candidate["file_line_count"])
        record = {
            "repo": repo,
            "repo_name": repo,
            "repo_url": repo_url,
            "repo_stars": repo_stars,
            "pr_number": pr_number,
            "pr_identifier": str(pr_number),
            "pr_merged_at": pr_merged_at,
            "commit_sha": commit_sha,
            "file_path": candidate["path"],
            "path": candidate["path"],
            "language": display_language(language),
            "content": candidate["content"],
            "file_line_count": candidate["file_line_count"],
            "size_band": band,
            "arm_a_target_decile": target_decile,
            "patch_lines": candidate["patch_lines"],
            "size_decile": 0,
            "patch_sha": candidate["patch_sha"],
            "ai_excluded": False,
            "exclusion_signal": None,
        }
        self.records.append(record)
        self.seen_patch_shas.add(candidate["patch_sha"])
        self.seen_file_keys.add((repo, candidate["path"], str(pr_number)))
        self.included_by_language[language] += 1
        self.included_by_cell[(language, band, target_decile)] += 1
        self.remaining_targets[(language, band, target_decile)] = max(0, self.remaining_targets[(language, band, target_decile)] - 1)
        self.repo_counts[repo] += 1
        self.repo_band_counts[(repo, language, band)] += 1
        self.accepted_since_checkpoint += 1

        patch_path = self.patch_dir / f"{candidate['patch_sha']}.patch"
        patch_path.write_text(candidate["raw_patch"], encoding="utf-8")
        append_jsonl(self.accepted_log_path, record)

    def compatible_output_language(self, search_language: str, output_language: str) -> bool:
        return output_language in output_languages_for_search(search_language)

    def repo_cursor(self, repo: str) -> dict[str, Any]:
        cursor = self.repo_cursors[repo]
        cursor.setdefault("pr_page", 1)
        cursor.setdefault("pr_index", 0)
        cursor.setdefault("scanned_prs", 0)
        cursor.setdefault("yielded_total", 0)
        cursor.setdefault("accepted_total", 0)
        cursor.setdefault("exhausted", False)
        cursor.setdefault("exhausted_reason", "")
        cursor.setdefault("active_pr_number", 0)
        cursor.setdefault("active_pr_commit_checked", False)
        cursor.setdefault("file_index", 0)
        return cursor

    def log_repo_stage_once(self, *, repo: str, language: str, reason: str, detail: str) -> None:
        if (repo, reason) in self.logged_repo_exclusions:
            return
        self.append_exclusion(
            repo=repo,
            pr_number=0,
            file_path="(repo-level)",
            language=language,
            reason=reason,
            detail=detail,
            stage="repo",
        )

    async def workflow_signal(
        self,
        client: GitHubClient,
        *,
        repo: str,
        default_branch: str,
        workflow_paths: list[str],
    ) -> str | None:
        for workflow_path in sorted(set(path for path in workflow_paths if path)):
            if REPO_AI_WORKFLOW_PATTERN.search(workflow_path):
                return f"repo_workflow:{workflow_path}"
            workflow_text = await client.get_file_text(repo, workflow_path, ref=default_branch, allow_404=True)
            if workflow_text and REPO_AI_WORKFLOW_PATTERN.search(workflow_text):
                return f"repo_workflow_content:{workflow_path}"
        return None

    async def detect_repo_ai_signal(self, client: GitHubClient, repo: str, default_branch_hint: str | None) -> tuple[str | None, str | None]:
        cached = self.repo_ai_cache.get(repo)
        if cached is not None and cached.get("checked"):
            return str(cached.get("signal") or "") or None, str(cached.get("default_branch") or "") or None

        default_branch = default_branch_hint
        if not default_branch:
            metadata = await client.get_repo_metadata(repo)
            default_branch = str(metadata.get("default_branch") or "").strip() or "HEAD"

        try:
            tree_payload = await client.get_repo_tree(repo, default_branch)
            paths = [str(item.get("path") or "") for item in tree_payload.get("tree", [])]
            truncated = bool(tree_payload.get("truncated"))
        except GitHubRequestFailure:
            raise

        signal: str | None = None
        workflow_paths: list[str] = []
        for path in paths:
            if path in {".github/copilot-instructions.md", ".github/copilot_instructions.md", ".cursorrules"}:
                signal = f"repo_config:{path}"
                break
            if path == ".devin" or path.startswith(".devin/"):
                signal = f"repo_config:{path}"
                break
            if path.startswith(".cursor/rules"):
                signal = f"repo_config:{path}"
                break
            if path.startswith(".github/workflows/"):
                workflow_paths.append(path)

        if signal is None and workflow_paths:
            signal = await self.workflow_signal(
                client,
                repo=repo,
                default_branch=default_branch,
                workflow_paths=workflow_paths,
            )

        if signal is None and truncated:
            for probe_path in (".github/copilot-instructions.md", ".github/copilot_instructions.md", ".cursorrules", ".devin"):
                if await client.file_exists(repo, probe_path):
                    signal = f"repo_config:{probe_path}"
                    break
            if signal is None:
                workflows = await client.list_workflows(repo)
                signal = await self.workflow_signal(
                    client,
                    repo=repo,
                    default_branch=default_branch,
                    workflow_paths=[str(workflow.get("path") or workflow.get("name") or "") for workflow in workflows],
                )

        self.repo_ai_cache[repo] = {
            "checked": True,
            "signal": signal,
            "default_branch": default_branch,
            "truncated": truncated if "truncated" in locals() else False,
        }
        return signal, default_branch

    async def pr_commit_signal(self, client: GitHubClient, repo: str, pr_number: int) -> str | None:
        commits = await client.get_pr_commits(repo, pr_number)
        for commit in commits:
            message = str((commit.get("commit") or {}).get("message") or "")
            if COMMIT_AI_KEYWORDS.search(message):
                snippet = message[:120].replace("\n", " ")
                return f"commit_keyword:{snippet}"
        return None

    async def hydrate_pending_legacy_records(self, client: GitHubClient) -> None:
        if not self.pending_legacy_records:
            return

        logging.info("Hydrating %d legacy records that lack full-file content", len(self.pending_legacy_records))
        pending = list(self.pending_legacy_records)
        self.pending_legacy_records = []

        for item in pending:
            raw = dict(item.get("raw") or {})
            repo = str(raw.get("repo") or raw.get("repo_name") or "").strip()
            path = str(raw.get("file_path") or raw.get("path") or "").strip()
            pr_number = parse_int(raw.get("pr_number") or raw.get("pr_identifier"))
            commit_sha = str(raw.get("commit_sha") or "").strip()
            language = normalize_language(raw.get("language")) or language_from_path(path)

            if not repo or not path or not commit_sha or pr_number <= 0:
                self.append_exclusion(
                    repo=repo,
                    pr_number=pr_number,
                    file_path=path or "(unknown)",
                    language=language,
                    reason="legacy_hydration_missing_provenance",
                    detail=item.get("detail") or "missing repo/path/commit_sha/pr_number",
                    stage="migration",
                )
                self.dropped_existing_records += 1
                continue

            self.current_phase = "legacy_hydration"
            self.current_repo = repo
            self.current_search_language = language or SEARCH_LANGUAGES[0]

            content = await client.get_file_text(repo, path, ref=commit_sha, allow_404=True)
            self.maybe_checkpoint(client=client, force=False)
            if not content:
                self.append_exclusion(
                    repo=repo,
                    pr_number=pr_number,
                    file_path=path,
                    language=language,
                    reason="legacy_hydration_missing_content",
                    detail=f"commit_sha={commit_sha}",
                    stage="migration",
                )
                self.dropped_existing_records += 1
                continue

            raw["content"] = content
            try:
                record = self.normalize_existing_record(raw)
            except (LegacyHydrationRequired, RuntimeError) as exc:
                self.append_exclusion(
                    repo=repo,
                    pr_number=pr_number,
                    file_path=path,
                    language=language,
                    reason="legacy_hydration_invalid",
                    detail=str(exc),
                    stage="migration",
                )
                self.dropped_existing_records += 1
                continue

            dedupe_key = (repo, path, str(pr_number))
            patch_sha_value = str(record.get("patch_sha") or "")
            if patch_sha_value and patch_sha_value in self.seen_patch_shas:
                self.append_exclusion(
                    repo=repo,
                    pr_number=pr_number,
                    file_path=path,
                    language=language,
                    reason="legacy_hydration_duplicate_patch",
                    detail=patch_sha_value,
                    stage="migration",
                )
                self.dropped_existing_records += 1
                continue
            if dedupe_key in self.seen_file_keys:
                self.append_exclusion(
                    repo=repo,
                    pr_number=pr_number,
                    file_path=path,
                    language=language,
                    reason="legacy_hydration_duplicate_file_identity",
                    detail=f"{repo}:{pr_number}:{path}",
                    stage="migration",
                )
                self.dropped_existing_records += 1
                continue

            self.records.append(record)
            self.register_existing_record(record)
            self.migrated_existing_records += 1
            append_jsonl(self.accepted_log_path, record)

        self.write_index()

    async def fetch_more_repos(self, client: GitHubClient, search_language: str) -> None:
        if self.search_exhausted[search_language]:
            return

        page = self.search_pages[search_language]
        if page > self.args.max_search_pages:
            self.search_exhausted[search_language] = True
            self.search_stop_reason[search_language] = "max_search_pages_reached"
            return

        self.current_phase = "search"
        self.current_search_language = search_language
        repos = await client.search_repos(
            search_language=search_language,
            page=page,
            stars_min=self.args.search_stars_min,
        )
        self.search_pages[search_language] = page + 1

        if not repos:
            self.search_exhausted[search_language] = True
            self.search_stop_reason[search_language] = "search_empty"
            return

        added = 0
        for repo_data in sorted(
            repos,
            key=lambda item: stable_key(self.args.seed, search_language, item.get("full_name", "")),
        ):
            repo = str(repo_data.get("full_name") or "").strip()
            if not repo:
                continue
            if repo in self.repo_cursors or repo in self.zero_yield_repos:
                continue
            cursor = {
                "repo": repo,
                "search_language": search_language,
                "repo_stars": parse_int(repo_data.get("stargazers_count"), 0),
                "repo_url": str(repo_data.get("html_url") or f"https://github.com/{repo}"),
                "default_branch": str(repo_data.get("default_branch") or "").strip(),
                "pr_page": 1,
                "pr_index": 0,
                "scanned_prs": 0,
                "yielded_total": 0,
                "accepted_total": 0,
                "exhausted": False,
                "exhausted_reason": "",
                "active_pr_number": 0,
                "active_pr_commit_checked": False,
                "file_index": 0,
            }
            self.repo_cursors[repo] = cursor
            self.repo_queue_by_search[search_language].append(repo)
            added += 1

        if added == 0 and len(repos) < 100:
            self.search_exhausted[search_language] = True
            self.search_stop_reason[search_language] = "search_only_duplicates"

    async def next_repo_for_search_language(self, client: GitHubClient, search_language: str) -> str | None:
        while True:
            queue = self.repo_queue_by_search[search_language]
            while queue:
                repo = queue.pop(0)
                cursor = self.repo_cursors.get(repo)
                if cursor is None or cursor.get("exhausted"):
                    continue
                return repo

            if self.search_exhausted[search_language]:
                return None
            await self.fetch_more_repos(client, search_language)
            if not self.repo_queue_by_search[search_language] and self.search_exhausted[search_language]:
                return None

    async def process_repo_turn(self, client: GitHubClient, repo: str, search_language: str, turn_budget: int) -> int:
        cursor = self.repo_cursor(repo)
        self.current_phase = "repo"
        self.current_repo = repo
        accepted = 0

        if self.repo_counts[repo] >= self.args.repo_cap:
            cursor["exhausted"] = True
            cursor["exhausted_reason"] = "repo_cap_reached"
            self.log_repo_stage_once(
                repo=repo,
                language=search_language,
                reason="repo_cap_reached",
                detail=f"accepted={self.repo_counts[repo]} cap={self.args.repo_cap}",
            )
            return 0

        repo_signal, default_branch = await self.detect_repo_ai_signal(client, repo, cursor.get("default_branch"))
        if default_branch and not cursor.get("default_branch"):
            cursor["default_branch"] = default_branch
        if repo_signal:
            reason, _, detail = repo_signal.partition(":")
            self.log_repo_stage_once(
                repo=repo,
                language=search_language,
                reason=reason,
                detail=detail,
            )
            cursor["exhausted"] = True
            cursor["exhausted_reason"] = reason
            return 0

        pages_processed = 0
        while pages_processed < self.args.repo_pr_page_budget_per_turn:
            if accepted >= turn_budget:
                break
            if self.repo_counts[repo] >= self.args.repo_cap:
                cursor["exhausted"] = True
                cursor["exhausted_reason"] = "repo_cap_reached"
                self.log_repo_stage_once(
                    repo=repo,
                    language=search_language,
                    reason="repo_cap_reached",
                    detail=f"accepted={self.repo_counts[repo]} cap={self.args.repo_cap}",
                )
                break
            if remaining_for_search_language(self.remaining_targets, search_language) <= 0:
                break
            if cursor["pr_page"] > self.args.max_pr_pages_per_repo_total:
                cursor["exhausted"] = True
                cursor["exhausted_reason"] = "repo_pr_page_limit"
                if cursor["yielded_total"] == 0:
                    self.zero_yield_repos.add(repo)
                self.log_repo_stage_once(
                    repo=repo,
                    language=search_language,
                    reason="repo_pr_page_limit",
                    detail=f"page={cursor['pr_page']} limit={self.args.max_pr_pages_per_repo_total}",
                )
                break

            self.current_phase = "pulls"
            page_number = parse_int(cursor.get("pr_page"), 1)
            prs = await client.list_closed_prs(repo, page_number)
            pages_processed += 1
            self.maybe_checkpoint(client=client, force=False)

            if not prs:
                cursor["exhausted"] = True
                cursor["exhausted_reason"] = "repo_pulls_exhausted"
                self.log_repo_stage_once(
                    repo=repo,
                    language=search_language,
                    reason="repo_pulls_exhausted",
                    detail=f"page={page_number}",
                )
                break

            ordered_prs = sorted(
                prs,
                key=lambda item: stable_key(self.args.seed, repo, item.get("number", 0)),
            )
            if cursor["pr_index"] >= len(ordered_prs):
                cursor["pr_page"] = page_number + 1
                cursor["pr_index"] = 0
                cursor["active_pr_number"] = 0
                cursor["active_pr_commit_checked"] = False
                cursor["file_index"] = 0
                continue

            for pr_position in range(parse_int(cursor.get("pr_index"), 0), len(ordered_prs)):
                pr = ordered_prs[pr_position]
                cursor["pr_index"] = pr_position
                if accepted >= turn_budget:
                    break
                if self.repo_counts[repo] >= self.args.repo_cap:
                    cursor["exhausted"] = True
                    cursor["exhausted_reason"] = "repo_cap_reached"
                    self.log_repo_stage_once(
                        repo=repo,
                        language=search_language,
                        reason="repo_cap_reached",
                        detail=f"accepted={self.repo_counts[repo]} cap={self.args.repo_cap}",
                    )
                    break
                if remaining_for_search_language(self.remaining_targets, search_language) <= 0:
                    break
                if not pr_in_date_range(pr):
                    cursor["pr_index"] = pr_position + 1
                    continue

                pr_number = parse_int(pr.get("number"))
                if pr_number <= 0:
                    cursor["pr_index"] = pr_position + 1
                    continue
                pr_identifier = str(pr_number)
                if (repo, pr_identifier) in self.completed_prs:
                    cursor["pr_index"] = pr_position + 1
                    continue

                if parse_int(cursor.get("active_pr_number")) != pr_number:
                    cursor["active_pr_number"] = pr_number
                    cursor["active_pr_commit_checked"] = False
                    cursor["file_index"] = 0
                    cursor["scanned_prs"] += 1

                if not cursor.get("active_pr_commit_checked"):
                    self.current_phase = "pr_commits"
                    commit_signal = await self.pr_commit_signal(client, repo, pr_number)
                    self.maybe_checkpoint(client=client, force=False)
                    if commit_signal:
                        reason, _, detail = commit_signal.partition(":")
                        self.append_exclusion(
                            repo=repo,
                            pr_number=pr_number,
                            file_path="(pr-level)",
                            language=search_language,
                            reason=reason,
                            detail=detail,
                            stage="pr",
                        )
                        self.completed_prs.add((repo, pr_identifier))
                        cursor["pr_index"] = pr_position + 1
                        cursor["active_pr_number"] = 0
                        cursor["active_pr_commit_checked"] = False
                        cursor["file_index"] = 0
                        continue
                    cursor["active_pr_commit_checked"] = True

                self.current_phase = "pr_files"
                files = await client.get_pr_files(repo, pr_number)
                self.maybe_checkpoint(client=client, force=False)
                merged_at = str(pr.get("merged_at") or "")
                pr_finished = True

                ordered_files = sorted(
                    files,
                    key=lambda item: stable_key(self.args.seed, repo, pr_number, item.get("filename", "")),
                )
                for file_index in range(parse_int(cursor.get("file_index"), 0), len(ordered_files)):
                    file_info = ordered_files[file_index]
                    cursor["file_index"] = file_index
                    if accepted >= turn_budget:
                        pr_finished = False
                        break
                    if self.repo_counts[repo] >= self.args.repo_cap:
                        cursor["exhausted"] = True
                        cursor["exhausted_reason"] = "repo_cap_reached"
                        self.log_repo_stage_once(
                            repo=repo,
                            language=search_language,
                            reason="repo_cap_reached",
                            detail=f"accepted={self.repo_counts[repo]} cap={self.args.repo_cap}",
                        )
                        pr_finished = False
                        break
                    if remaining_for_search_language(self.remaining_targets, search_language) <= 0:
                        pr_finished = False
                        break

                    filename = str(file_info.get("filename") or "")
                    status = str(file_info.get("status") or "")
                    if status not in ALLOWED_STATUS:
                        self.append_exclusion(
                            repo=repo,
                            pr_number=pr_number,
                            file_path=filename,
                            language=language_from_path(filename),
                            reason="file_status_not_allowed",
                            detail=status,
                            stage="file",
                        )
                        cursor["file_index"] = file_index + 1
                        continue

                    reason, candidate = filter_patch_candidate(filename, file_info.get("patch"))
                    if reason or candidate is None:
                        self.append_exclusion(
                            repo=repo,
                            pr_number=pr_number,
                            file_path=filename,
                            language=search_language,
                            reason=reason or "unknown_rejection",
                            detail="patch content filter",
                            stage="file",
                        )
                        cursor["file_index"] = file_index + 1
                        continue

                    output_language = candidate["language"]
                    if not self.compatible_output_language(search_language, output_language):
                        self.append_exclusion(
                            repo=repo,
                            pr_number=pr_number,
                            file_path=filename,
                            language=output_language,
                            reason="file_language_not_in_search_family",
                            detail=f"search_language={search_language}",
                            stage="file",
                        )
                        cursor["file_index"] = file_index + 1
                        continue

                    target_decile = self.arm_a_target_decile_for(
                        output_language,
                        candidate["file_line_count"],
                    )
                    cell_key = (output_language, candidate["size_band"], target_decile)
                    if self.remaining_targets[cell_key] <= 0:
                        self.append_exclusion(
                            repo=repo,
                            pr_number=pr_number,
                            file_path=filename,
                            language=output_language,
                            reason="target_already_satisfied",
                            detail=f"{output_language}:{candidate['size_band']}:{target_decile}",
                            stage="file",
                        )
                        cursor["file_index"] = file_index + 1
                        continue

                    dedupe_key = (repo, candidate["path"], pr_identifier)
                    if candidate["patch_sha"] in self.seen_patch_shas:
                        self.append_exclusion(
                            repo=repo,
                            pr_number=pr_number,
                            file_path=filename,
                            language=output_language,
                            reason="duplicate_patch",
                            detail=candidate["patch_sha"],
                            stage="file",
                        )
                        cursor["file_index"] = file_index + 1
                        continue
                    if dedupe_key in self.seen_file_keys:
                        self.append_exclusion(
                            repo=repo,
                            pr_number=pr_number,
                            file_path=filename,
                            language=output_language,
                            reason="duplicate_file_identity",
                            detail=f"{repo}:{pr_identifier}:{candidate['path']}",
                            stage="file",
                        )
                        cursor["file_index"] = file_index + 1
                        continue

                    self.accept_candidate(
                        repo=repo,
                        repo_url=str(cursor.get("repo_url") or f"https://github.com/{repo}"),
                        repo_stars=parse_int(cursor.get("repo_stars")),
                        pr_number=pr_number,
                        pr_merged_at=merged_at,
                        commit_sha=str(file_info.get("sha") or ""),
                        candidate=candidate,
                    )
                    cursor["yielded_total"] += 1
                    cursor["accepted_total"] += 1
                    accepted += 1
                    cursor["file_index"] = file_index + 1

                if cursor["yielded_total"] == 0 and cursor["scanned_prs"] >= self.args.zero_yield_pr_threshold:
                    cursor["exhausted"] = True
                    cursor["exhausted_reason"] = "zero_yield_repo"
                    self.zero_yield_repos.add(repo)
                    self.log_repo_stage_once(
                        repo=repo,
                        language=search_language,
                        reason="zero_yield_repo",
                        detail=f"scanned_prs={cursor['scanned_prs']}",
                    )
                    break

                if not pr_finished:
                    self.maybe_checkpoint(client=client, force=False)
                    break

                self.completed_prs.add((repo, pr_identifier))
                cursor["pr_index"] = pr_position + 1
                cursor["active_pr_number"] = 0
                cursor["active_pr_commit_checked"] = False
                cursor["file_index"] = 0
                self.maybe_checkpoint(client=client, force=False)

            if cursor.get("exhausted"):
                break
            if cursor.get("active_pr_number"):
                break
            if parse_int(cursor.get("pr_index"), 0) >= len(ordered_prs):
                cursor["pr_page"] = page_number + 1
                cursor["pr_index"] = 0
                cursor["active_pr_number"] = 0
                cursor["active_pr_commit_checked"] = False
                cursor["file_index"] = 0

            self.maybe_checkpoint(client=client, force=False)

        return accepted

    async def collect_search_language_turn(self, client: GitHubClient, search_language: str) -> int:
        if remaining_for_search_language(self.remaining_targets, search_language) <= 0:
            return 0

        accepted = 0
        self.current_search_language = search_language
        while accepted < self.args.stage_accept_limit:
            if remaining_for_search_language(self.remaining_targets, search_language) <= 0:
                break
            repo = await self.next_repo_for_search_language(client, search_language)
            if repo is None:
                break
            repo_accepted = await self.process_repo_turn(
                client,
                repo,
                search_language,
                self.args.stage_accept_limit - accepted,
            )
            cursor = self.repo_cursors.get(repo, {})
            if not cursor.get("exhausted"):
                self.repo_queue_by_search[search_language].append(repo)
            accepted += repo_accepted
            self.maybe_checkpoint(client=client, force=False)
        return accepted

    async def collect(self) -> int:
        token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if not token:
            try:
                result = subprocess.run(
                    ["gh", "auth", "token"],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                token = result.stdout.strip()
            except Exception as exc:  # pragma: no cover - runtime fallback
                raise RuntimeError(f"GITHUB_TOKEN/GH_TOKEN missing and gh auth token unavailable: {exc}") from exc
        if not token:
            raise RuntimeError("GITHUB_TOKEN/GH_TOKEN missing and gh auth token returned empty token")

        self.current_phase = "collect"
        interrupted = False
        fatal_error: str | None = None

        client: GitHubClient | None = None
        try:
            async with GitHubClient(token) as client:
                await self.hydrate_pending_legacy_records(client)
                self.maybe_checkpoint(
                    client=client,
                    force=bool(self.records or self.pending_legacy_records),
                    complete=False,
                    interrupted=False,
                    fatal_error=None,
                )
                while total_remaining(self.remaining_targets) > 0:
                    progress_this_cycle = 0
                    for search_language in self.prioritized_search_languages():
                        self.current_search_index = (SEARCH_LANGUAGES.index(search_language) + 1) % len(SEARCH_LANGUAGES)
                        if remaining_for_search_language(self.remaining_targets, search_language) <= 0:
                            continue
                        accepted = await self.collect_search_language_turn(client, search_language)
                        progress_this_cycle += accepted
                        if total_remaining(self.remaining_targets) <= 0:
                            break

                    if total_remaining(self.remaining_targets) <= 0:
                        break

                    blocked = [
                        search_language
                        for search_language in SEARCH_LANGUAGES
                        if remaining_for_search_language(self.remaining_targets, search_language) > 0
                        and self.search_exhausted[search_language]
                        and not self.repo_queue_by_search[search_language]
                    ]
                    if progress_this_cycle == 0 and blocked:
                        logging.warning(
                            "No progress in cycle and search exhausted for %s; stopping with remaining targets",
                            ",".join(blocked),
                        )
                        break

                complete = total_remaining(self.remaining_targets) == 0
                self.current_phase = "complete" if complete else "incomplete"
                self.last_successful_request_at = self.effective_last_successful_request_at(client)
                self.maybe_checkpoint(
                    client=client,
                    force=True,
                    complete=complete,
                    interrupted=False,
                    fatal_error=None,
                )
                if complete:
                    return 0
                return 2
        except KeyboardInterrupt:
            interrupted = True
            self.current_phase = "interrupted"
            fatal_error = "KeyboardInterrupt"
            logging.warning("Interrupted; writing checkpoint before exit")
            self.maybe_checkpoint(
                client=client,
                force=True,
                complete=False,
                interrupted=True,
                fatal_error=fatal_error,
            )
            return 130
        except GitHubRequestFailure as exc:
            fatal_error = f"{exc.category}: {exc.url}: {exc.message}"
            self.last_error = fatal_error
            self.current_phase = "fatal_network_error"
            logging.error("Fatal GitHub request failure: %s", fatal_error)
            self.maybe_checkpoint(
                client=client,
                force=True,
                complete=False,
                interrupted=False,
                fatal_error=fatal_error,
            )
            return 75
        finally:
            if interrupted and fatal_error is None:
                self.last_error = "KeyboardInterrupt"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a bulletproof Arm B candidate pool aligned to Arm A.")
    parser.add_argument("--arm-a", default=str(DEFAULT_ARM_A), help="Arm A JSONL used to derive language, size-band, and decile targets.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for index/excluded/summary/state/patch outputs.")
    parser.add_argument("--target", type=int, default=DEFAULT_TARGET, help="Exact candidate-pool target total. Use 0 to derive from Arm A * oversample factor.")
    parser.add_argument("--oversample-factor", type=float, default=DEFAULT_OVERSAMPLE_FACTOR, help="Used only when --target is 0.")
    parser.add_argument("--repo-cap", type=int, default=DEFAULT_REPO_CAP, help="Maximum accepted files per repo during pool build.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Deterministic seed for repo / PR / file ordering.")
    parser.add_argument("--resume", action="store_true", help="Resume from summary/state/index in the output directory.")
    parser.add_argument("--fresh", action="store_true", help="Delete existing output artifacts in the output directory before starting.")
    parser.add_argument("--stage-accept-limit", type=int, default=DEFAULT_STAGE_ACCEPT_LIMIT, help="Accepted files per search-language turn before rotating.")
    parser.add_argument("--repo-pr-page-budget-per-turn", type=int, default=DEFAULT_REPO_PR_PAGE_BUDGET, help="PR pages to scan from a repo before rotating away.")
    parser.add_argument("--max-pr-pages-per-repo-total", type=int, default=DEFAULT_MAX_PR_PAGES_PER_REPO_TOTAL, help="Hard ceiling on PR pages scanned from one repo.")
    parser.add_argument("--zero-yield-pr-threshold", type=int, default=DEFAULT_ZERO_YIELD_PR_THRESHOLD, help="Mark a repo as zero-yield after this many scanned PRs with no accepted files.")
    parser.add_argument("--search-stars-min", type=int, default=DEFAULT_SEARCH_STARS_MIN, help="Minimum star threshold for repo search.")
    parser.add_argument("--max-search-pages", type=int, default=DEFAULT_MAX_SEARCH_PAGES, help="Maximum GitHub search pages per search language.")
    parser.add_argument("--checkpoint-every-accepts", type=int, default=DEFAULT_CHECKPOINT_EVERY_ACCEPTS)
    parser.add_argument("--checkpoint-every-requests", type=int, default=DEFAULT_CHECKPOINT_EVERY_REQUESTS)
    parser.add_argument("--checkpoint-every-seconds", type=int, default=DEFAULT_CHECKPOINT_EVERY_SECONDS)
    parser.add_argument("--log", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def configure_logging(log_path: Path, level_name: str, resume: bool) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handlers: list[logging.Handler] = [
        logging.FileHandler(log_path, mode="a" if resume else "w"),
        logging.StreamHandler(sys.stdout),
    ]
    logging.basicConfig(
        level=getattr(logging, level_name),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    extractor = ArmBExtractor(args)
    extractor.ensure_output_dir()
    configure_logging(extractor.log_path, args.log, args.resume)

    logging.info(
        "Starting Arm B extraction: arm_a=%s target_total=%d resume=%s fresh=%s seed=%d",
        extractor.arm_a_path,
        extractor.target_total,
        args.resume,
        args.fresh,
        args.seed,
    )
    logging.info(
        "Quota summary: %s",
        dict(sorted(serialize_counter_by_band(extractor.targets).items())),
    )
    logging.info(
        "Round-robin settings: stage_accept_limit=%d repo_pr_page_budget_per_turn=%d max_pr_pages_per_repo_total=%d",
        args.stage_accept_limit,
        args.repo_pr_page_budget_per_turn,
        args.max_pr_pages_per_repo_total,
    )
    logging.info(
        "Dates: %s to %s | repo_cap=%d | stars_min=%d",
        PR_DATE_START,
        PR_DATE_END,
        args.repo_cap,
        args.search_stars_min,
    )

    extractor.load_resume_artifacts()
    return asyncio.run(extractor.collect())


if __name__ == "__main__":
    raise SystemExit(main())
