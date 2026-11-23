"""Scenario 7: "new data arrived: what is stale; what would a run recompute or reuse?"

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.11_new_data

Two units are appended to units.csv after a run. Proves, with public API:
- ``bound_source.check_for_new_data()`` takes the new rows in (it moves the source's
  modified time; it computes nothing and calls no user code), after which every job node
  downstream reports ``is_stale``; a node not downstream stays current;
- which rows are NEW at the first function node is known from the record ids before the
  run (the new units' record ids); which groups change is known from their keys;
- that the other 64 results are REUSED, and the changed groups RECOMPUTED, is what the run
  summary reports afterwards. Predicting it exactly before the run needs each input's data
  hash, which for a node behind another function node needs that function's results:
  a prediction can be exact only up to the first function node with new inputs.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ._common import workshop_job

root = Path(tempfile.mkdtemp())
job, data_dir = workshop_job(root)
job.run()
before = {
    r
    for r in job.nodes["units"]
    .as_table(columns={"system_tags": True})
    .column(
        [
            c
            for c in job.nodes["units"]
            .as_table(columns={"system_tags": True})
            .column_names
            if c.startswith("_tag_record_id")
        ][0]
    )
    .to_pylist()
}

csv = data_dir / "units.csv"
csv.write_text(
    csv.read_text() + "mouse_01,s1,imec0,4,V1,1500,333\n"
    "mouse_02,s2,imec1,4,CA1,900,444\n"
)


def stale(job):
    # A SourceJobNode has no is_stale; the function and operator job nodes do.
    return sorted(k for k in job.nodes if getattr(job.nodes[k], "is_stale", False))


print("stale before the check:", stale(job))
moved = job.nodes["units"].bound_source.check_for_new_data()
print("units took in new data:", moved)
print("stale after the check: ", stale(job))

units_now = job.nodes["units"].bound_source.as_table(columns={"system_tags": True})
rid = [c for c in units_now.column_names if c.startswith("_tag_record_id")][0]
new = [r for r in units_now.to_pylist() if r[rid] not in before]
print("new units records:", [(r[rid], r["subject_id"], r["session"]) for r in new])
print(
    "predicted: rates computes",
    len(new),
    "new input(s), reuses the rest; session_means recomputes groups",
    sorted({(r["subject_id"], r["session"]) for r in new}),
)
job.run()
print("run summary:", job.last_run)
