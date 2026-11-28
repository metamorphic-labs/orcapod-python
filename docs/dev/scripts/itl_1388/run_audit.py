"""Run every reader situation over the jobs ``writer.py`` saved (ITL-1388).

Usage: ``python run_audit.py <root>``; expects ``<root>/delta`` and ``<root>/sqlite``
written by ``writer.py``. Each reader is a fresh process; the pods module is importable
only in situations a, a_imported (imported before loading) and c (changed).
"""

from __future__ import annotations

import sys
from pathlib import Path

from common import run_fresh  # pyright: ignore[reportImplicitRelativeImport]  # a script folder, run as scripts

root = Path(sys.argv[1]).resolve()
JOBS = {
    "delta": ["main", "other", "separate", "downstream", "polling"],
    "sqlite": ["main", "other", "separate"],
}
for kind, jobs in JOBS.items():
    w = root / kind
    out = w / "out"
    out.mkdir(exist_ok=True)
    situations = {
        "a": ([w / "mods_v1"], []),
        "a_imported": ([w / "mods_v1"], ["--import-pods"]),
        "b": ([], []),
        "c": ([w / "mods_changed"], []),
    }
    if kind == "sqlite":
        # The saved SQLite database is not rebuilt by load (see the report): the
        # same reads with the database passed in.
        situations["b_database_given"] = ([], ["--sqlite", str(w / "store.sqlite")])
    for job in jobs:
        path = w / f"{job}_job.json"
        if not path.exists():
            print("missing", path)
            continue
        for sit, (pp, extra) in situations.items():
            res = run_fresh(
                "reader.py", [str(path), str(out / f"{job}.{sit}.json"), *extra], pp
            )
            status = (
                "ok"
                if res.returncode == 0
                else f"exit {res.returncode}: {res.stderr[-400:]}"
            )
            print(kind, job, sit, status)
