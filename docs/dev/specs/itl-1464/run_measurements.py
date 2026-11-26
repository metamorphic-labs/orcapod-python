"""Run every step of ``measure.py`` in its own process, under a memory guard (ITL-1464).

    uv run python docs/dev/specs/itl-1464/run_measurements.py --store <dir> \
        --rows 2200000 --out <results.jsonl> [--steps as_table run ...]

A step whose resident memory passes ``--guard-gb`` (24 by default) is killed and
recorded as ``killed``; nothing here may starve the machine it runs on.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

MEASURE = Path(__file__).with_name("measure.py")

DEFAULT_STEPS = [
    "raw_copy",
    "as_table",
    "as_table_all_info",
    "run",
    "run_then_as_table",
    "as_polars_df",
    "iterate",
    "iterate_read_one_value",
    "flow",
    "map_data",
    "drop_nulls",
    "polars_filter",
    "join",
    "group_by",
    "group_by_chain",
    "function_pod",
    "job_run",
    "job_run_async",
]


def _rss_bytes(pid: int) -> int:
    found = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True
    ).stdout.strip()
    return int(found) * 1024 if found else 0


def _guarded(
    command: list[str], guard_bytes: int, *, capture: bool
) -> tuple[int | None, int, str, str]:
    """Run a command; kill it when its resident memory passes the guard.

    Returns:
        The resident memory at which it was killed (``None`` when it ran to its end),
        its return code, and its captured output and error text (empty when not
        captured).
    """
    pipe = subprocess.PIPE if capture else None
    process = subprocess.Popen(command, stdout=pipe, stderr=pipe, text=True)
    seen = 0
    while process.poll() is None:
        seen = max(seen, _rss_bytes(process.pid))
        if seen > guard_bytes:
            process.kill()
            process.wait()
            return seen, process.returncode, "", ""
        time.sleep(0.2)
    out, err = process.communicate()
    return None, process.returncode, out or "", err or ""


def run_setup(store: Path, rows: int, guard_bytes: int) -> None:
    """Write the synthetic tables, under the same guard as every step."""
    killed_at, code, _, _ = _guarded(
        [sys.executable, str(MEASURE), "setup", "--store", str(store)]
        + ["--rows", str(rows)],
        guard_bytes,
        capture=False,
    )
    if killed_at is not None:
        raise SystemExit(
            f"setup killed: resident memory {killed_at} bytes passed the guard"
        )
    if code != 0:
        raise SystemExit(f"setup failed with exit code {code}")


def run_step(
    step: str, store: Path, guard_bytes: int, extra: list[str]
) -> dict[str, Any]:
    """Run one step; kill it when its resident memory passes the guard."""
    killed_at, code, out, err = _guarded(
        [sys.executable, str(MEASURE), step, "--store", str(store), *extra],
        guard_bytes,
        capture=True,
    )
    if killed_at is not None:
        return {"step": step, "killed": True, "rss_when_killed_bytes": killed_at}
    if code != 0:
        return {"step": step, "failed": True, "error": err.strip()[-2000:]}
    record = json.loads(out.strip().splitlines()[-1])
    for line in err.splitlines():
        if line.startswith("{"):
            record.update(json.loads(line))
    return record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=2_200_000)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--guard-gb", type=float, default=24.0)
    parser.add_argument("--steps", nargs="*", default=DEFAULT_STEPS)
    parser.add_argument("--tracemalloc", action="store_true")
    parser.add_argument("--skip-setup", action="store_true")
    args = parser.parse_args()

    guard = int(args.guard_gb * 1024**3)
    if not args.skip_setup:
        run_setup(args.store, args.rows, guard)
    extra = ["--tracemalloc"] if args.tracemalloc else []
    with args.out.open("a") as out:
        for step in args.steps:
            record = run_step(step, args.store, guard, extra)
            print(json.dumps(record), flush=True)
            out.write(json.dumps(record) + "\n")
            out.flush()


if __name__ == "__main__":
    main()
