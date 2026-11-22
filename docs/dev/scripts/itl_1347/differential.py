"""ITL-1347 differential driver: every fixture, mode, orchestrator and thread count, two Orcapods.

    python differential.py OUT --base <python> --branch <python>
        [--fixtures tutorial,spikes] [--threads 1,default]

``--base`` and ``--branch`` are the interpreters of two environments, each with one Orcapod
installed from a built wheel (pinned ``main`` and the stage branch). Every dump runs in its
own process, one after another. For each fixture:

1. two ``cold`` sync dumps with the base interpreter calibrate the volatile columns
   (``compare.py --calibrate``);
2. for each interpreter, orchestrator (``sync``, ``async``) and Polars thread count: a
   ``cold`` dump, then ``warm`` and ``load_only`` dumps over the store it left, and a
   ``held`` dump over a new store;
3. comparisons:
   - branch sync against base sync, ``exact``;
   - branch async against branch sync, ``set``;
   - branch async against base async, ``set``.

Writes ``OUT/report.json`` (every comparison with its differences; ``--compare-only``
writes ``OUT/report_compare_only.json`` and leaves ``report.json`` as it is) and prints a
summary. The exit status is the gate: 1 if a dump process failed or a comparison could not
complete (its stderr is kept in the report), 2 if sync on the branch differs from sync on
base, else 0. Completed async differences are printed for review against the spec's
named differences and do not set the status.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
MODES = ("cold", "warm", "load_only", "held")


def _dump(
    python: str,
    fixture: str,
    mode: str,
    orchestrator: str,
    threads: str,
    workdir: Path,
    out: Path,
) -> dict[str, Any]:
    env = dict(os.environ)
    if threads == "default":
        env.pop("POLARS_MAX_THREADS", None)
    else:
        env["POLARS_MAX_THREADS"] = threads
    start = time.perf_counter()
    done = subprocess.run(
        [
            python,
            str(HERE / "dump.py"),
            fixture,
            mode,
            orchestrator,
            str(workdir),
            str(out),
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "dump": str(out),
        "returncode": done.returncode,
        "stderr_tail": done.stderr[-2000:] if done.returncode else "",
        "seconds": round(time.perf_counter() - start, 2),
    }


def _compare(a: Path, b: Path, mode: str, volatile: Path) -> dict[str, Any]:
    done = subprocess.run(
        [
            sys.executable,
            str(HERE / "compare.py"),
            str(a),
            str(b),
            "--mode",
            mode,
            "--volatile",
            str(volatile),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    # compare.py exits 0 (equal) or 3 (completed, with differences); anything else means
    # the comparison could not complete, and its stderr is kept.
    completed = done.returncode in (0, 3)
    return {
        "a": str(a),
        "b": str(b),
        "mode": mode,
        "completed": completed,
        "stderr_tail": "" if completed else done.stderr[-2000:],
        "equal": done.returncode == 0,
        "output": done.stdout.splitlines()[-40:],
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("out", type=Path)
    parser.add_argument("--base", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--fixtures", default="tutorial,spikes")
    parser.add_argument("--threads", default="1,default")
    parser.add_argument(
        "--compare-only",
        action="store_true",
        help="redo the comparisons on the dumps already in OUT; run nothing",
    )
    args = parser.parse_args(argv)
    out: Path = args.out
    out.mkdir(parents=True, exist_ok=args.compare_only)
    pythons = {"base": args.base, "branch": args.branch}

    def dump_fn(*dump_args: Any) -> dict[str, Any]:
        if args.compare_only:
            return {"dump": str(dump_args[-1]), "returncode": 0, "skipped": True}
        return _dump(*dump_args)

    report: dict[str, Any] = {"runs": [], "comparisons": [], "load": []}
    for fixture in args.fixtures.split(","):
        calib = [out / fixture / f"calibrate_{i}" for i in (1, 2)]
        for i, dump in enumerate(calib, start=1):
            report["runs"].append(
                dump_fn(
                    pythons["base"],
                    fixture,
                    "cold",
                    "sync",
                    "default",
                    out / fixture / f"calibrate_work_{i}",
                    dump,
                )
            )
        volatile = out / fixture / "volatile.json"
        subprocess.run(
            [
                sys.executable,
                str(HERE / "compare.py"),
                str(calib[0]),
                str(calib[1]),
                "--calibrate",
                str(volatile),
            ],
            capture_output=True,
            check=True,
        )
        for threads in args.threads.split(","):
            for side, python in pythons.items():
                for orchestrator in ("sync", "async"):
                    tag = f"{side}_{orchestrator}_t{threads}"
                    work = out / fixture / f"work_{tag}"
                    held_work = out / fixture / f"work_{tag}_held"
                    for mode in MODES:
                        report["load"].append(os.getloadavg()[0])
                        report["runs"].append(
                            dump_fn(
                                python,
                                fixture,
                                mode,
                                orchestrator,
                                threads,
                                held_work if mode == "held" else work,
                                out / fixture / f"{tag}_{mode}",
                            )
                        )
            for mode in MODES:

                def d(side: str, orchestrator: str) -> Path:
                    return out / fixture / f"{side}_{orchestrator}_t{threads}_{mode}"

                report["comparisons"] += [
                    {
                        "fixture": fixture,
                        "threads": threads,
                        "check": f"sync branch vs base, {mode}",
                    }
                    | _compare(
                        d("base", "sync"), d("branch", "sync"), "exact", volatile
                    ),
                    {
                        "fixture": fixture,
                        "threads": threads,
                        "check": f"async vs sync, branch, {mode}",
                    }
                    | _compare(
                        d("branch", "sync"), d("branch", "async"), "set", volatile
                    ),
                    {
                        "fixture": fixture,
                        "threads": threads,
                        "check": f"async branch vs base, {mode}",
                    }
                    | _compare(
                        d("base", "async"), d("branch", "async"), "set", volatile
                    ),
                ]
    report_name = "report_compare_only.json" if args.compare_only else "report.json"
    (out / report_name).write_text(json.dumps(report, indent=1))
    failed_runs = [r for r in report["runs"] if r["returncode"] != 0]
    incomplete = [c for c in report["comparisons"] if not c["completed"]]
    for c in report["comparisons"]:
        status = (
            "EQUAL" if c["equal"] else "DIFFERENT" if c["completed"] else "INCOMPLETE"
        )
        print(f"{status:9} {c['fixture']:8} t={c['threads']:7} {c['check']}")
    print(f"{len(failed_runs)} dump processes failed")
    print(f"{len(incomplete)} comparisons could not complete")
    # The exit status is the gate. A failed dump or a comparison that could not complete
    # makes the run incomplete (1). A sync difference against main fails the governing
    # rule (2). Completed async differences are printed for review against the spec's
    # named differences, and do not set it.
    if failed_runs or incomplete:
        return 1
    if any(
        not c["equal"] and c["check"].startswith("sync") for c in report["comparisons"]
    ):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
