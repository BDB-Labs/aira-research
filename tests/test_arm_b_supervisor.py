from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SUPERVISOR_PATH = REPO_ROOT / "scripts" / "arm_b_supervisor.py"
SPEC = importlib.util.spec_from_file_location("arm_b_supervisor", SUPERVISOR_PATH)
arm_b_supervisor = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(arm_b_supervisor)


class ArmBSupervisorTests(unittest.TestCase):
    def test_parse_retry_exit_codes_rejects_empty(self) -> None:
        with self.assertRaises(Exception):
            arm_b_supervisor.parse_retry_exit_codes(" , ")

    def test_parse_retry_exit_codes_dedupes_and_preserves_order(self) -> None:
        self.assertEqual(arm_b_supervisor.parse_retry_exit_codes("75, 75, 130"), (75, 130))

    def test_infer_output_dir_supports_split_and_equals_flags(self) -> None:
        self.assertEqual(
            arm_b_supervisor.infer_output_dir(["--output-dir", "/tmp/a"]),
            Path("/tmp/a"),
        )
        self.assertEqual(
            arm_b_supervisor.infer_output_dir(["--output-dir=/tmp/b"]),
            Path("/tmp/b"),
        )

    def test_prepare_extractor_args_replaces_fresh_with_resume_on_restart(self) -> None:
        prepared = arm_b_supervisor.prepare_extractor_args(
            ["--arm-a", "/tmp/arm_a.jsonl", "--fresh", "--seed", "42"],
            restart=True,
        )
        self.assertEqual(prepared.count("--fresh"), 0)
        self.assertIn("--resume", prepared)

    def test_should_retry_only_for_retryable_noncomplete_runs(self) -> None:
        self.assertTrue(
            arm_b_supervisor.should_retry(
                75,
                retry_exit_codes=(75,),
                summary={"remaining_total": 13},
            )
        )
        self.assertFalse(
            arm_b_supervisor.should_retry(
                75,
                retry_exit_codes=(75,),
                summary={"remaining_total": 0},
            )
        )
        self.assertFalse(
            arm_b_supervisor.should_retry(
                2,
                retry_exit_codes=(75,),
                summary={"remaining_total": 13},
            )
        )

    def test_load_summary_returns_empty_dict_for_missing_or_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self.assertEqual(arm_b_supervisor.load_summary(root), {})
            (root / "summary.json").write_text("{not-json", encoding="utf-8")
            self.assertEqual(arm_b_supervisor.load_summary(root), {})
            (root / "summary.json").write_text(json.dumps({"remaining_total": 1}), encoding="utf-8")
            self.assertEqual(arm_b_supervisor.load_summary(root), {"remaining_total": 1})


if __name__ == "__main__":
    unittest.main()
