import importlib.util
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MATCH_PATH = REPO_ROOT / "scripts" / "arm_b_match.py"
SPEC = importlib.util.spec_from_file_location("arm_b_match", MATCH_PATH)
arm_b_match = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(arm_b_match)


def make_record(*, repo: str, pr: int, path: str, lines: int, language: str = "JavaScript") -> dict:
    return {
        "repo": repo,
        "pr_number": pr,
        "file_path": path,
        "language": language,
        "file_line_count": lines,
    }


class ArmBMatchTests(unittest.TestCase):
    def test_size_value_prefers_file_line_count_over_patch_lines(self) -> None:
        record = {
            "repo": "owner/repo",
            "pr_number": 1,
            "file_path": "src/example.js",
            "language": "JavaScript",
            "file_line_count": 240,
            "patch_lines": 12,
        }
        self.assertEqual(arm_b_match.size_value(record), 240)

    def test_matched_draw_prefers_same_repo_within_strict_cell(self) -> None:
        arm_a_lines = [100, 210, 320, 430, 540, 650, 760, 870, 980, 1090]
        arm_a = [
            make_record(repo=f"anchor/repo-{index}", pr=index, path=f"src/a{index}.js", lines=lines)
            for index, lines in enumerate(arm_a_lines)
        ]
        pool = [
            make_record(repo="anchor/repo-0", pr=1000, path="src/same-repo.js", lines=101),
            make_record(repo="other/repo", pr=1001, path="src/closer.js", lines=100),
            make_record(repo="pool/repo-1", pr=2001, path="src/p1.js", lines=210),
            make_record(repo="pool/repo-2", pr=2002, path="src/p2.js", lines=320),
            make_record(repo="pool/repo-3", pr=2003, path="src/p3.js", lines=430),
            make_record(repo="pool/repo-4", pr=2004, path="src/p4.js", lines=540),
            make_record(repo="pool/repo-5", pr=2005, path="src/p5.js", lines=650),
            make_record(repo="pool/repo-6", pr=2006, path="src/p6.js", lines=760),
            make_record(repo="pool/repo-7", pr=2007, path="src/p7.js", lines=870),
            make_record(repo="pool/repo-8", pr=2008, path="src/p8.js", lines=980),
            make_record(repo="pool/repo-9", pr=2009, path="src/p9.js", lines=1090),
            make_record(repo="pool/repo-10", pr=2010, path="src/p10.js", lines=1200),
        ]

        selected, diagnostics = arm_b_match.matched_draw(pool, arm_a, 10, 42, 4)

        self.assertEqual(len(selected), 10)
        selected_paths = {record["file_path"] for record in selected}
        self.assertIn("src/same-repo.js", selected_paths)
        self.assertNotIn("src/closer.js", selected_paths)
        self.assertGreaterEqual(diagnostics["same_repo_matches"], 1)
        self.assertEqual(diagnostics["shortfall_total"], 0)

    def test_matched_draw_enforces_repo_cap_and_strict_cells_without_backfill(self) -> None:
        arm_a = [make_record(repo=f"anchor/repo-{index}", pr=index, path=f"src/a{index}.js", lines=100 + index) for index in range(20)]
        pool = [
            make_record(repo="shared/repo", pr=1000 + index, path=f"src/p{index}.js", lines=100 + index)
            for index in range(20)
        ]

        selected, diagnostics = arm_b_match.matched_draw(pool, arm_a, 20, 42, 1)

        self.assertEqual(len(selected), 1)
        self.assertEqual(diagnostics["shortfall_total"], 19)
        self.assertEqual(diagnostics["max_files_per_repo"], 1)
        self.assertEqual(diagnostics["repo_cap"], 1)


if __name__ == "__main__":
    unittest.main()
