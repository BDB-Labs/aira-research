from __future__ import annotations

import argparse
import importlib.util
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
EXTRACT_PATH = REPO_ROOT / "scripts" / "arm_b_extract.py"
SPEC = importlib.util.spec_from_file_location("arm_b_extract", EXTRACT_PATH)
arm_b_extract = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(arm_b_extract)


def make_content(lines: int) -> str:
    return "\n".join(f"line {index}" for index in range(lines))


def make_patch(lines: int, *, prefix: str = "line") -> str:
    body = "\n".join(f"+{prefix} {index}" for index in range(lines))
    return f"@@\n{body}\n"


def write_arm_a(path: Path, *, lines: int = 140) -> None:
    record = {
        "repo_name": "arm-a/repo",
        "pr_identifier": "1",
        "path": "src/anchor.js",
        "language": "JavaScript",
        "content": make_content(lines),
    }
    path.write_text(json.dumps(record) + "\n", encoding="utf-8")


def write_arm_a_series(path: Path, lines_by_record: list[int], *, language: str = "JavaScript") -> None:
    rows = []
    for index, lines in enumerate(lines_by_record, 1):
        rows.append(
            {
                "repo_name": "arm-a/repo",
                "pr_identifier": str(index),
                "path": f"src/anchor-{index}.js",
                "language": language,
                "content": make_content(lines),
            }
        )
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def build_args(
    arm_a_path: Path,
    output_dir: Path,
    *,
    target: int = 2,
    resume: bool = False,
    repo_cap: int = 8,
) -> argparse.Namespace:
    return argparse.Namespace(
        arm_a=str(arm_a_path),
        output_dir=str(output_dir),
        target=target,
        oversample_factor=2.0,
        repo_cap=repo_cap,
        seed=42,
        resume=resume,
        fresh=False,
        stage_accept_limit=25,
        repo_pr_page_budget_per_turn=3,
        max_pr_pages_per_repo_total=25,
        zero_yield_pr_threshold=60,
        search_stars_min=50,
        max_search_pages=10,
        checkpoint_every_accepts=999,
        checkpoint_every_requests=999,
        checkpoint_every_seconds=999999,
        log="ERROR",
    )


class FakeClient:
    def __init__(self, prs_by_page: dict[int, list[dict]], files_by_pr: dict[int, list[dict]]):
        self.prs_by_page = prs_by_page
        self.files_by_pr = files_by_pr
        self.request_count = 0
        self.status_counts = Counter()
        self.last_successful_request_at = None

    async def list_closed_prs(self, repo: str, page: int) -> list[dict]:
        return list(self.prs_by_page.get(page, []))

    async def get_pr_files(self, repo: str, pr_number: int) -> list[dict]:
        return list(self.files_by_pr.get(pr_number, []))

    async def get_file_text(self, repo: str, path: str, *, ref: str | None = None, allow_404: bool = False) -> str | None:
        return None

    async def get_repo_metadata(self, repo: str) -> dict:
        return {"default_branch": "main"}

    async def get_repo_tree(self, repo: str, default_branch: str) -> dict:
        return {"tree": [], "truncated": False}

    async def list_workflows(self, repo: str) -> list[dict]:
        return []

    async def file_exists(self, repo: str, path: str) -> bool:
        return False


class FakeWorkflowClient(FakeClient):
    def __init__(self) -> None:
        super().__init__({}, {})

    async def get_repo_tree(self, repo: str, default_branch: str) -> dict:
        return {
            "tree": [
                {"path": ".github/workflows/ci.yml"},
            ],
            "truncated": False,
        }

    async def get_file_text(self, repo: str, path: str, *, ref: str | None = None, allow_404: bool = False) -> str | None:
        return "name: ci\nsteps:\n  - run: cursor --version\n"


class FakeHydrationClient(FakeClient):
    def __init__(self, hydrated_content: str) -> None:
        super().__init__({}, {})
        self.hydrated_content = hydrated_content

    async def get_file_text(self, repo: str, path: str, *, ref: str | None = None, allow_404: bool = False) -> str | None:
        return self.hydrated_content


class ArmBExtractTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.arm_a_path = self.root / "arm_a.jsonl"
        write_arm_a(self.arm_a_path)

    async def asyncTearDown(self) -> None:
        self.tempdir.cleanup()

    async def test_repo_turn_resumes_mid_page_without_skipping_remaining_prs(self) -> None:
        output_dir = self.root / "arm_b"
        args = build_args(self.arm_a_path, output_dir, target=2)
        extractor = arm_b_extract.ArmBExtractor(args)
        extractor.ensure_output_dir()
        repo = "owner/repo"
        extractor.repo_cursors[repo] = {
            "repo": repo,
            "search_language": "javascript",
            "repo_stars": 10,
            "repo_url": f"https://github.com/{repo}",
            "default_branch": "main",
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
        prs_by_page = {
            1: [
                {"number": 101, "merged_at": "2024-01-01T00:00:00Z"},
                {"number": 102, "merged_at": "2024-01-02T00:00:00Z"},
            ],
            2: [],
        }
        files_by_pr = {
            101: [{"filename": "src/one.js", "status": "modified", "patch": make_patch(140, prefix="one"), "sha": "sha-101"}],
            102: [{"filename": "src/two.js", "status": "modified", "patch": make_patch(140, prefix="two"), "sha": "sha-102"}],
        }
        client = FakeClient(prs_by_page, files_by_pr)

        async def no_repo_signal(_client, _repo, _default_branch):
            return None, "main"

        async def no_commit_signal(_client, _repo, _pr_number):
            return None

        extractor.detect_repo_ai_signal = no_repo_signal
        extractor.pr_commit_signal = no_commit_signal

        accepted_first = await extractor.process_repo_turn(client, repo, "javascript", 1)
        self.assertEqual(accepted_first, 1)
        self.assertEqual(len(extractor.records), 1)
        first_pr_number = extractor.records[0]["pr_number"]
        ordered_prs = sorted(
            prs_by_page[1],
            key=lambda item: arm_b_extract.stable_key(args.seed, repo, item.get("number", 0)),
        )
        if first_pr_number == ordered_prs[-1]["number"]:
            self.assertEqual(extractor.repo_cursors[repo]["pr_page"], 2)
            self.assertEqual(extractor.repo_cursors[repo]["pr_index"], 0)
        else:
            self.assertEqual(extractor.repo_cursors[repo]["pr_page"], 1)
            self.assertEqual(extractor.repo_cursors[repo]["pr_index"], 1)

        accepted_second = await extractor.process_repo_turn(client, repo, "javascript", 1)
        self.assertEqual(accepted_second, 1)
        self.assertEqual(len(extractor.records), 2)
        self.assertEqual(extractor.repo_cursors[repo]["pr_page"], 2)
        self.assertEqual(extractor.repo_cursors[repo]["pr_index"], 0)
        self.assertIn((repo, "101"), extractor.completed_prs)
        self.assertIn((repo, "102"), extractor.completed_prs)

    async def test_arm_a_target_decile_mapping_tracks_arm_a_quantiles(self) -> None:
        arm_a_path = self.root / "arm_a_deciles.jsonl"
        write_arm_a_series(arm_a_path, [100 + (index * 10) for index in range(10)])
        output_dir = self.root / "decile_arm_b"
        args = build_args(arm_a_path, output_dir, target=10)
        extractor = arm_b_extract.ArmBExtractor(args)

        self.assertEqual(extractor.arm_a_target_decile_for("javascript", 100), 0)
        self.assertEqual(extractor.arm_a_target_decile_for("javascript", 145), 5)
        self.assertEqual(extractor.arm_a_target_decile_for("javascript", 190), 9)

    async def test_validate_resume_configuration_allows_tighter_repo_cap(self) -> None:
        output_dir = self.root / "resume_cap_arm_b"
        args = build_args(self.arm_a_path, output_dir, target=2, repo_cap=4)
        extractor = arm_b_extract.ArmBExtractor(args)

        extractor.validate_resume_configuration(
            {
                "arm_a_path": str(extractor.arm_a_path),
                "target_total": extractor.target_total,
                "seed": extractor.args.seed,
                "repo_cap": 8,
            }
        )

    async def test_existing_record_counts_against_strict_target_cell(self) -> None:
        arm_a_path = self.root / "arm_a_strict_resume.jsonl"
        write_arm_a_series(arm_a_path, [100 + (index * 10) for index in range(10)])
        output_dir = self.root / "strict_resume_arm_b"
        output_dir.mkdir(parents=True, exist_ok=True)
        row = {
            "repo": "owner/repo",
            "pr_number": 10,
            "file_path": "src/existing.js",
            "language": "JavaScript",
            "content": make_content(145),
            "patch_sha": "existingpatch",
            "patch_lines": 145,
        }
        (output_dir / "index.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")

        args = build_args(arm_a_path, output_dir, target=10, resume=True)
        extractor = arm_b_extract.ArmBExtractor(args)
        extractor.ensure_output_dir()
        extractor.load_resume_artifacts()

        self.assertEqual(len(extractor.records), 1)
        self.assertEqual(extractor.records[0]["arm_a_target_decile"], 5)
        self.assertEqual(extractor.remaining_targets[("javascript", "100_299", 5)], 0)

    async def test_detect_repo_ai_signal_reads_workflow_contents(self) -> None:
        output_dir = self.root / "workflow_arm_b"
        args = build_args(self.arm_a_path, output_dir, target=1)
        extractor = arm_b_extract.ArmBExtractor(args)
        extractor.ensure_output_dir()
        client = FakeWorkflowClient()

        signal, default_branch = await extractor.detect_repo_ai_signal(client, "owner/repo", "main")
        self.assertEqual(default_branch, "main")
        self.assertEqual(signal, "repo_workflow_content:.github/workflows/ci.yml")

    async def test_legacy_patch_only_rows_queue_for_hydration_and_recover(self) -> None:
        output_dir = self.root / "legacy_arm_b"
        output_dir.mkdir(parents=True, exist_ok=True)
        patch_dir = output_dir / "patches"
        patch_dir.mkdir(parents=True, exist_ok=True)
        patch_sha = "deadbeefcafebabe"
        (patch_dir / f"{patch_sha}.patch").write_text(make_patch(20), encoding="utf-8")
        legacy_row = {
            "repo": "owner/repo",
            "pr_number": 55,
            "file_path": "src/legacy.js",
            "patch_sha": patch_sha,
            "patch_lines": 20,
            "commit_sha": "abc123",
            "language": "JavaScript",
            "size_decile": 0,
        }
        (output_dir / "index.jsonl").write_text(json.dumps(legacy_row) + "\n", encoding="utf-8")

        args = build_args(self.arm_a_path, output_dir, target=1, resume=True)
        extractor = arm_b_extract.ArmBExtractor(args)
        extractor.ensure_output_dir()
        extractor.load_resume_artifacts()

        self.assertEqual(len(extractor.records), 0)
        self.assertEqual(len(extractor.pending_legacy_records), 1)

        client = FakeHydrationClient(make_content(140))
        await extractor.hydrate_pending_legacy_records(client)

        self.assertEqual(len(extractor.pending_legacy_records), 0)
        self.assertEqual(len(extractor.records), 1)
        self.assertEqual(extractor.records[0]["file_line_count"], 140)
        self.assertEqual(extractor.records[0]["size_band"], "100_299")

    async def test_github_client_treats_pull_endpoint_404_as_empty(self) -> None:
        client = arm_b_extract.GitHubClient("token")

        async def fake_get_json(_url, **_kwargs):
            return None

        client._get_json = fake_get_json
        self.assertEqual(await client.list_closed_prs("owner/repo", 1), [])
        self.assertEqual(await client.get_pr_commits("owner/repo", 1), [])
        self.assertEqual(await client.get_pr_files("owner/repo", 1), [])

    async def test_github_client_treats_directory_payload_as_no_file_text(self) -> None:
        client = arm_b_extract.GitHubClient("token")

        async def fake_get_json(_url, **_kwargs):
            return []

        client._get_json = fake_get_json
        self.assertIsNone(await client.get_file_text("owner/repo", ".github/workflows/tests", ref="main"))

    async def test_resolve_gh_cli_falls_back_to_common_mac_path(self) -> None:
        original_which = arm_b_extract.shutil.which
        original_exists = arm_b_extract.Path.exists

        def fake_which(_name: str) -> None:
            return None

        def fake_exists(path_obj: Path) -> bool:
            return str(path_obj) == "/opt/homebrew/bin/gh"

        arm_b_extract.shutil.which = fake_which
        arm_b_extract.Path.exists = fake_exists
        try:
            self.assertEqual(arm_b_extract.resolve_gh_cli(), "/opt/homebrew/bin/gh")
        finally:
            arm_b_extract.shutil.which = original_which
            arm_b_extract.Path.exists = original_exists


if __name__ == "__main__":
    unittest.main()
