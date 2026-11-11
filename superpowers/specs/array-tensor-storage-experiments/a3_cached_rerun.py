"""A3: a cached re-run in a fresh process gives cache hits and equal arrays (ITL-874).

Claims tested, each run in a separate ``uv run python a3_cached_rerun_child.py`` process
against a ``DeltaTableDatabase`` directory:

1. sync then sync on one store: run 1 computes every row (``double`` 4 times, ``first``
   2 times); run 2 computes nothing.
2. async then async on a fresh store: the same expectation.
3. sync then async on one store.
4. Across the processes of a scenario, the arrays read back (dtype, shape, memory order,
   values) and the content hashes agree: the source, each source row (a ``Data`` holding an
   array), each output row and each node.

Every scenario also runs over plain ``int`` values as a control, so a failure that is not
specific to arrays shows up as such.

SQLite is not tried here: ``a3_pipeline_stores.py`` shows it stores no rows at all.

Run: ``uv run python superpowers/specs/array-tensor-storage-experiments/a3_cached_rerun.py``
"""

from __future__ import annotations

from typing import Any

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from a3_common import report  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory

HERE = Path(__file__).resolve().parent
CHILD = HERE / "a3_cached_rerun_child.py"


def run_child(store: Path, counter: Path, mode: str, values: str) -> dict[str, Any]:
    env = dict(os.environ, RUST_LOG="error", PYTHONHASHSEED="random")
    proc = subprocess.run(
        [sys.executable, str(CHILD), str(store), str(counter), mode, values],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip().splitlines()[-1])
    return json.loads(proc.stdout.strip().splitlines()[-1])


def scenario(name: str, modes: tuple[str, str], values: str) -> None:
    tmp = Path(tempfile.mkdtemp(prefix="itl874_a3_rerun_"))
    store, counter = tmp / "store", tmp / "calls.txt"
    label = f"{values} / {name}"
    runs: list[dict[str, Any]] = []
    for i, mode in enumerate(modes, start=1):
        try:
            runs.append(run_child(store, counter, mode, values))
        except Exception as exc:  # noqa: BLE001 - every failure is reported
            report(
                "DEFECT", f"{label}: run {i} ({mode})", f"{type(exc).__name__}: {exc}"
            )
            return

    want = [{"double": 4, "first": 2}, {"double": 0, "first": 0}]
    calls = [r["calls"] for r in runs]
    status = "OK" if calls == want else "DEFECT"
    report(status, f"{label}: pod calls per run (want {want})", calls)

    for key in sorted(runs[0]):
        if key == "calls":
            continue
        a, b = runs[0][key], runs[1][key]
        shown = a if not isinstance(a, dict) else f"{len(a)} rows"
        if a == b:
            report("OK", f"{label}: {key} equal across processes", shown)
        else:
            report("DEFECT", f"{label}: {key} differs across processes", f"{a} vs {b}")


def main() -> None:
    for values in ("array", "int"):
        scenario("sync then sync", ("sync", "sync"), values)
        scenario("async then async", ("async", "async"), values)
        scenario("sync then async", ("sync", "async"), values)


if __name__ == "__main__":
    main()
