#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path


DEFAULT_EXTRACTOR_PATH = Path(__file__).with_name("arm_b_extract.py")
DEFAULT_OUTPUT_DIR = Path("/Users/billp/Documents/AIRA/data/arm_b")
DEFAULT_RESTART_DELAY_MINUTES = 15.0
DEFAULT_RETRY_EXIT_CODES = (75,)


def parse_retry_exit_codes(raw: str) -> tuple[int, ...]:
    codes: list[int] = []
    for item in raw.split(","):
        token = item.strip()
        if not token:
            continue
        try:
            code = int(token)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid exit code: {token!r}") from exc
        if code not in codes:
            codes.append(code)
    if not codes:
        raise argparse.ArgumentTypeError("at least one retry exit code is required")
    return tuple(codes)


def extract_flag_value(argv: list[str], flag: str) -> str | None:
    prefix = f"{flag}="
    for index, token in enumerate(argv):
        if token == flag and index + 1 < len(argv):
            return argv[index + 1]
        if token.startswith(prefix):
            return token.split("=", 1)[1]
    return None


def infer_output_dir(forwarded_args: list[str]) -> Path:
    raw = extract_flag_value(forwarded_args, "--output-dir")
    if raw:
        return Path(raw).expanduser()
    return DEFAULT_OUTPUT_DIR


def prepare_extractor_args(forwarded_args: list[str], *, restart: bool) -> list[str]:
    prepared = [token for token in forwarded_args if token != "--fresh"]
    if restart and "--resume" not in prepared:
        prepared.append("--resume")
    return prepared


def load_summary(output_dir: Path) -> dict[str, object]:
    summary_path = output_dir / "summary.json"
    if not summary_path.exists():
        return {}
    try:
        return json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def should_retry(exit_code: int, *, retry_exit_codes: tuple[int, ...], summary: dict[str, object]) -> bool:
    if exit_code not in retry_exit_codes:
        return False
    if summary.get("remaining_total") == 0:
        return False
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Supervise arm_b_extract.py and automatically resume after retryable failures."
    )
    parser.add_argument(
        "--extractor-path",
        default=str(DEFAULT_EXTRACTOR_PATH),
        help="Path to arm_b_extract.py.",
    )
    parser.add_argument(
        "--restart-delay-minutes",
        type=float,
        default=DEFAULT_RESTART_DELAY_MINUTES,
        help="Minutes to wait before retrying after a retryable extractor exit.",
    )
    parser.add_argument(
        "--max-restarts",
        type=int,
        default=0,
        help="Maximum retry attempts after the first failed run. 0 means unlimited.",
    )
    parser.add_argument(
        "--retry-exit-codes",
        type=parse_retry_exit_codes,
        default=DEFAULT_RETRY_EXIT_CODES,
        help="Comma-separated extractor exit codes that should trigger auto-resume. Default: 75.",
    )
    parser.add_argument(
        "--supervisor-log",
        default=None,
        help="Optional path for supervisor log output. Defaults to <output-dir>/supervisor.log.",
    )
    parser.add_argument(
        "--supervisor-log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Supervisor log verbosity.",
    )
    parser.add_argument(
        "--caffeinate",
        action="store_true",
        help="On macOS, keep the machine awake by attaching caffeinate to the supervisor PID.",
    )
    return parser


class ArmBSupervisor:
    def __init__(self, args: argparse.Namespace, forwarded_args: list[str]):
        self.args = args
        self.forwarded_args = list(forwarded_args)
        self.output_dir = infer_output_dir(self.forwarded_args)
        self.extractor_path = Path(args.extractor_path).expanduser()
        self.supervisor_log_path = (
            Path(args.supervisor_log).expanduser()
            if args.supervisor_log
            else self.output_dir / "supervisor.log"
        )
        self.stop_requested = False
        self.active_process: subprocess.Popen[bytes] | None = None

    def configure_logging(self) -> None:
        self.supervisor_log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers: list[logging.Handler] = [
            logging.FileHandler(self.supervisor_log_path, mode="a"),
            logging.StreamHandler(sys.stdout),
        ]
        logging.basicConfig(
            level=getattr(logging, self.args.supervisor_log_level),
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=handlers,
        )

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)

    def handle_signal(self, signum: int, _frame: object) -> None:
        self.stop_requested = True
        logging.warning("Received signal %s; forwarding to child and stopping supervisor", signum)
        if self.active_process and self.active_process.poll() is None:
            self.active_process.send_signal(signum)

    def maybe_start_caffeinate(self) -> subprocess.Popen[bytes] | None:
        if not self.args.caffeinate:
            return None
        try:
            proc = subprocess.Popen(
                ["caffeinate", "-ism", "-w", str(os.getpid())],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            logging.info("Attached caffeinate to supervisor PID %d", os.getpid())
            return proc
        except FileNotFoundError:
            logging.warning("caffeinate not available; continuing without a sleep assertion")
            return None

    def run_once(self, *, restart: bool) -> int:
        prepared_args = prepare_extractor_args(self.forwarded_args, restart=restart)
        command = [sys.executable, str(self.extractor_path), *prepared_args]
        logging.info("Launching extractor: %s", " ".join(shlex.quote(token) for token in command))
        self.active_process = subprocess.Popen(command)
        return_code = self.active_process.wait()
        self.active_process = None
        return return_code

    def sleep_before_restart(self) -> bool:
        delay_seconds = max(self.args.restart_delay_minutes, 0) * 60.0
        if delay_seconds == 0:
            return not self.stop_requested
        logging.info("Waiting %.2f minutes before retry", self.args.restart_delay_minutes)
        deadline = time.monotonic() + delay_seconds
        while time.monotonic() < deadline:
            if self.stop_requested:
                return False
            time.sleep(min(5.0, deadline - time.monotonic()))
        return not self.stop_requested

    def run(self) -> int:
        self.configure_logging()
        self.install_signal_handlers()
        caffeinate_proc = self.maybe_start_caffeinate()
        restart_count = 0
        restart = False
        logging.info(
            "Starting Arm B supervisor: output_dir=%s retry_exit_codes=%s restart_delay_minutes=%s max_restarts=%s",
            self.output_dir,
            self.args.retry_exit_codes,
            self.args.restart_delay_minutes,
            self.args.max_restarts,
        )
        try:
            while not self.stop_requested:
                exit_code = self.run_once(restart=restart)
                summary = load_summary(self.output_dir)
                logging.info(
                    "Extractor exited rc=%s phase=%s accepted=%s remaining=%s last_error=%s",
                    exit_code,
                    summary.get("current_phase"),
                    summary.get("accepted_total"),
                    summary.get("remaining_total"),
                    summary.get("last_error"),
                )
                if not should_retry(exit_code, retry_exit_codes=self.args.retry_exit_codes, summary=summary):
                    return exit_code
                if self.args.max_restarts and restart_count >= self.args.max_restarts:
                    logging.error("Restart limit reached (%d); leaving extractor stopped", self.args.max_restarts)
                    return exit_code
                restart_count += 1
                restart = True
                logging.warning(
                    "Retryable extractor failure detected (exit %d). Restart %d will use --resume.",
                    exit_code,
                    restart_count,
                )
                if not self.sleep_before_restart():
                    return 130
            return 130
        finally:
            if caffeinate_proc is not None and caffeinate_proc.poll() is None:
                caffeinate_proc.terminate()


def main() -> int:
    parser = build_parser()
    args, forwarded_args = parser.parse_known_args()
    supervisor = ArmBSupervisor(args, forwarded_args)
    return supervisor.run()


if __name__ == "__main__":
    raise SystemExit(main())
