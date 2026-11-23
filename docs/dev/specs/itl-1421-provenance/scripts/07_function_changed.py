"""Scenario 3: "the function changed: what changed in the results?"

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.07_function_changed

Proves:
- a body edit WITHOUT a version bump keeps the function's uri, so a run over the same
  inputs reuses the stored results of the old body (nothing recomputed); the stored
  ``__pf_var_function_content_hash`` is the only trace that the body differs, and it can
  be compared with the current function's;
- with a major version bump (``v1.0``) the new results land beside the old ones (another
  uri); the two versions' records pair up per input by ``__input_data_hash`` (and the
  tags), so a per-input comparison is a read of two stored tables.
Public API only (``get_all_records``); one database shared by the two jobs.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ._common import workshop_job

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import CSVSource

root = Path(tempfile.mkdtemp())
store = DeltaTableDatabase(root / "store")
job1, data_dir = workshop_job(root, database=store, name="v0")
job1.run()


def rates_job(name: str, version: str, body):
    pod = function_pod(
        output_keys="rate_hz", function_name="firing_rate", version=version
    )(body)
    job = PipelineJob(name=name, database=store)
    with job:
        units = CSVSource(
            str(data_dir / "units.csv"),
            tag_columns=["subject_id", "session", "probe", "unit"],
            tag_only_columns=True,
            source_id="units",
        )
        sessions = CSVSource(
            str(data_dir / "sessions.csv"),
            tag_columns=["subject_id", "session"],
            tag_only_columns=True,
            source_id="sessions",
        )
        pod.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
            label="rates",
        )
    return job


def per_minute(n_spikes: int, duration_s: float) -> float:
    return 60 * n_spikes / duration_s  # the edit: spikes per minute


print("== Body edited, version unchanged (v0.0)")
same = rates_job("v0", "v0.0", per_minute)  # same job name: same pipeline tables
same.run()
recs = same.nodes["rates"].get_all_records(columns={"meta": True})
print(
    "  results computed by this run:",
    sum(
        1
        for r in recs.to_pylist()
        if r["__pod_ts"]
        > max(
            x["__pod_ts"]
            for x in job1.nodes["rates"]
            .get_all_records(columns={"meta": True})
            .to_pylist()
        )
    ),
)
print(
    "  a rate shown:",
    same.nodes["rates"].as_table().column("rate_hz")[0].as_py(),
    "(n/duration, the OLD body's value)",
)
stored = {r["__pf_var_function_content_hash"] for r in recs.to_pylist()}
print(
    "  stored function content hashes:",
    len(stored),
    "-> compare with the current function's to flag results of another body",
)

print("\n== Body edited, major version bumped (v1.0)")
new = rates_job("v1", "v1.0", per_minute)
new.run()
old_recs = {
    (r["subject_id"], r["session"], r["probe"], r["unit"], r["__input_data_hash"]): r
    for r in job1.nodes["rates"].get_all_records(columns={"meta": True}).to_pylist()
}
new_recs = new.nodes["rates"].get_all_records(columns={"meta": True}).to_pylist()
pairs = [
    (
        old_recs.get(
            (
                r["subject_id"],
                r["session"],
                r["probe"],
                r["unit"],
                r["__input_data_hash"],
            )
        ),
        r,
    )
    for r in new_recs
]
matched = [(o, n) for o, n in pairs if o is not None]
changed = [(o, n) for o, n in matched if o["rate_hz"] != n["rate_hz"]]
print(f"  paired per input: {len(matched)} of {len(new_recs)}; changed: {len(changed)}")
o, n = changed[0]
print(
    f"  e.g. {n['subject_id']} {n['session']} {n['probe']} unit {n['unit']}: "
    f"{o['rate_hz']:.4f} -> {n['rate_hz']:.4f}  ({o['__pf_var_function_version']} -> "
    f"{n['__pf_var_function_version']})"
)
