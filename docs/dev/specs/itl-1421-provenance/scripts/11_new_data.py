"""Scenario 7: "new data arrived: what is stale; what would a run recompute or reuse?"

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.11_new_data

Two units are appended to units.csv after a run. Proves, with public API and direct reads
of the PIPELINE store:
- node level: ``bound_source.check_for_new_data()`` takes the new rows in (it moves the
  source's modified time; computes nothing, calls no user code); every job node
  downstream then reports ``is_stale``;
- row level at the first function node, EXACT: a ``load_only`` run applies the
  operators (no user code, nothing written) and counts the inputs without a stored
  entry; each input's base entry id (hash of its system tags + input data hash,
  ``node.compute_base_entry_id``) is looked up in the pipeline store's
  ``__pipeline_base_entry_id``: the ones not there are exactly what will compute;
- row level past it, EXACT when membership changes: a group's entry is keyed by its
  folded system tags, which are a fold of its members' record ids, known WITHOUT the
  members' results (system tags pass through a function pod). A group whose refolded
  system tags have no entry in the next pipeline store will compute. (A group whose
  members are unchanged but one member's VALUE changed keeps its folded system tags; it
  computes only if the member's new result differs: an upper bound before the run.)
- the run summary confirms the prediction.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from orcapod.databases import DeltaTableDatabase
from orcapod.utils.arrow_utils import fold_system_tag_values

from ._common import workshop_job
from ._store import read_job, rows, system_tag_columns

root = Path(tempfile.mkdtemp())
db = DeltaTableDatabase(root / "store")
job, data_dir = workshop_job(root, database=db)
job.run()
S0 = read_job(job, db)

csv = data_dir / "units.csv"
csv.write_text(
    csv.read_text()
    + "mouse_01,s1,imec0,4,V1,1500,333\nmouse_02,s2,imec1,4,CA1,900,444\n"
)


def stale(job: Any) -> list[str]:
    # A SourceJobNode has no is_stale; the function and operator job nodes do.
    return sorted(k for k in job.nodes if getattr(job.nodes[k], "is_stale", False))


print("stale before the check:", stale(job))
print("units took in new data:", job.nodes["units"].bound_source.check_for_new_data())
print("stale after the check: ", stale(job))

print("\n== Prediction, before the run")
job.run(load_only=True)
print("load-only run (operators applied, no user code, nothing written):", job.last_run)
rates = job.nodes["rates"]
stored_ids = {
    r["__pipeline_base_entry_id"] for r in rows(S0["rates"].own(S0["rates"].pipeline))
}
inputs = list(job.nodes["Join"].iter_data())
new_inputs = [
    (t, d) for t, d in inputs if rates.compute_base_entry_id(t, d) not in stored_ids
]
print(
    f"rates: {len(inputs)} inputs; base entry id not in the PIPELINE store: {len(new_inputs)} -> {[(t.as_dict()['subject_id'], t.as_dict()['session'], t.as_dict()['probe'], t.as_dict()['unit']) for t, _ in new_inputs]}"
)

join_t = job.nodes["Join"].as_table(all_info=True)
jcols = system_tag_columns(join_t)
means = S0["session_means"]
stored_folds = {
    tuple(r[c] for c in system_tag_columns(means.pipeline))
    for r in rows(means.own(means.pipeline))
}
groups = sorted({(r["subject_id"], r["session"]) for r in rows(join_t)})
will_run = []
for key in groups:
    ms = sorted(
        (r for r in rows(join_t) if (r["subject_id"], r["session"]) == key),
        key=lambda r: (r["probe"], r["unit"]),
    )
    fold = tuple(
        fold_system_tag_values([m[c] for m in ms], label="group_v1") for c in jcols
    )
    if fold not in stored_folds:
        will_run.append(key)
print(
    f"session_means: {len(groups)} groups; refolded system tags with no entry in its PIPELINE store: {will_run}"
)

print("\n== The run")
job.run()
print(job.last_run)
