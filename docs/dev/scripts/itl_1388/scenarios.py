"""The audit's further situations (ITL-1388): questions 4 (vi), 5 and 7.

Usage: ``python scenarios.py <root>`` after ``writer.py <root>/delta delta``. Each
situation that loads a saved job runs it in a fresh process (``scenario_child.py``)
without the pods module, unless it is about an in-process database. Restores every file
it changes. Prints one block per situation.
"""

from __future__ import annotations

from typing import Any

import json
import shutil
import sys
from pathlib import Path

from common import READINGS, run_fresh, write_csv  # pyright: ignore[reportImplicitRelativeImport]  # a script folder, run as scripts

root = Path(sys.argv[1]).resolve()
w = root / "delta"
readings = w / "data" / "readings.csv"
HEADER = ["subject", "session", "value"]


def child(*args: str, pythonpath=None) -> dict[str, Any]:
    res = run_fresh("scenario_child.py", list(args), pythonpath)
    if res.returncode != 0:
        return {"exit": res.returncode, "stderr": res.stderr[-1500:]}
    return json.loads(res.stdout.strip().splitlines()[-1])


def show(title: str, result) -> None:
    print(f"\n### {title}")
    print(json.dumps(result, sort_keys=True, default=str)[:5000])


expected = json.loads((w / "expected.json").read_text())

# (vi-b) The source changed after the save: back to generation 1, then a new row.
try:
    write_csv(readings, HEADER, READINGS[1])
    r = child("load_only", str(w / "main_job.json"), "score")
    r["expected_gen1_rows"] = expected["generations"]["1"]["score"]
    show("source file back at generation 1, then load + run(load_only=True)", r)
    write_csv(readings, HEADER, READINGS[3] + [["s4", 1, 7.0]])
    show(
        "source file with one row the saved run never saw (s4)",
        child("load_only", str(w / "main_job.json"), "score"),
    )
finally:
    write_csv(readings, HEADER, READINGS[3])

# (5) The job moved to another machine: the whole folder copied elsewhere and the
# original location gone.
moved = root / "delta_moved"
away = root / "delta_away"
shutil.rmtree(moved, ignore_errors=True)
shutil.copytree(w, moved)
w.rename(away)
try:
    show(
        "moved folder: which step creates files at the saved (now missing) location",
        child("steps", str(moved / "main_job.json"), "score", str(w)),
    )
    if w.exists():
        shutil.rmtree(w)  # made by the load above; the original is at ``away``
    show(
        "moved folder, loaded as saved",
        child("load_only", str(moved / "main_job.json"), "score"),
    )
    if w.exists():
        print("NOTE: the reads above created", w)
        shutil.rmtree(w)
    show(
        "moved folder, database= given",
        child(
            "load_only",
            str(moved / "main_job.json"),
            "score",
            "--database-root",
            str(moved / "store"),
        ),
    )
    show(
        "moved folder, database= given and the CSV sources bound again",
        child(
            "load_only",
            str(moved / "main_job.json"),
            "score",
            "--database-root",
            str(moved / "store"),
            "--rebind-csv",
            str(moved / "data"),
        ),
    )
finally:
    if w.exists() and away.exists():
        shutil.rmtree(w)
    away.rename(w)

# (5) The database path is missing (the store deleted, the job file and sources kept).
store = w / "store"
hidden = w / "store_hidden"
store.rename(hidden)
try:
    show(
        "database folder missing: which step creates it",
        child("steps", str(w / "main_job.json"), "score", str(store)),
    )
    if store.exists():
        shutil.rmtree(store)
    show(
        "database folder missing", child("load_only", str(w / "main_job.json"), "score")
    )
finally:
    if store.exists() and hidden.exists():
        shutil.rmtree(store)
    hidden.rename(store)

# (5) Data context mismatch: the file names a data context this Orcapod does not have.
mismatch = w / "main_job.ctx_mismatch.json"
mismatch.write_text(
    (w / "main_job.json").read_text().replace("std:v1:default", "std:v9:default")
)
show(
    "file naming data context std:v9:default",
    child("load_only", str(mismatch), "score"),
)
mismatch.unlink()

# (5) The format version of the file is not this Orcapod's.
old = w / "main_job.old_format.json"
data = json.loads((w / "main_job.json").read_text())
data["orcapod_pipeline_job_version"] = "0.3.0"
old.write_text(json.dumps(data))
show("file of format 0.3.0", child("load_only", str(old), "score"))
old.unlink()

# (5) A file whose run content hashes were removed (malformed).
bad = w / "main_job.no_hashes.json"
data = json.loads((w / "main_job.json").read_text())
del data["run"]["node_job_content_hashes"]["score"]
bad.write_text(json.dumps(data))
show(
    "file without the run content hash of 'score'",
    child("load_only", str(bad), "score"),
)
bad.unlink()

# (2, 5) In-memory sources and an in-memory database, in one process.
show(
    "in-memory database and DictSource (one process)",
    child("in_memory", str(root / "inmem")),
)

# (7) The blueprint: Pipeline.save / load, then PipelineJob.from_pipeline.
show(
    "Pipeline.load + PipelineJob.from_pipeline (CSV sources, Delta database)",
    child("blueprint", str(w)),
)
show(
    "Pipeline.load + PipelineJob.from_pipeline with the pods module importable",
    child("blueprint", str(w), pythonpath=[w / "mods_v1"]),
)
