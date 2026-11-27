"""Scenario 3: "the function changed: what changed in the results?"

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.07_function_changed

Uses the two stores of a function node for what each is for:
- RESULT store (which function, which computation): every result row records the
  function's name, version, module, signature hash and CONTENT hash (its body), the git
  revision, the executor and Python version, and when it ran (``__pod_ts``). A body edit
  WITHOUT a version bump keeps the uri, so the old results are reused; the result rows'
  ``__pf_var_function_content_hash`` differs from the current function's, which flags
  every result of the old body.
- PIPELINE store (why, from what): a major version bump (``v1.0``) gives a new uri, so new
  pipeline and result tables. Each input row keeps its base entry id
  (``__pipeline_base_entry_id`` = hash of the input's SYSTEM TAGS and input data hash; no
  function in it), so the two versions' entries pair EXACTLY per input row by that id.
  Round one paired by tags + ``__input_data_hash``; the entry id is the same key, read
  from the store instead of rebuilt, and it also carries the system tags (two rows with
  equal tags but different source records stay apart).
Direct reads of the stored tables; one database shared by the jobs.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import CSVSource

from ._common import workshop_job
from ._store import KEY, hash_text, read_job, rows, short_hash

root = Path(tempfile.mkdtemp())
store = DeltaTableDatabase(root / "store")
job1, data_dir = workshop_job(root, database=store, name="v0")
job1.run()
S1 = read_job(job1, store)


def rates_job(name: str, version: str, body: Any) -> tuple[PipelineJob, Any]:
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
    return job, pod


def per_minute(n_spikes: int, duration_s: float) -> float:
    return 60 * n_spikes / duration_s  # the edit: spikes per minute


def body_hash(pod: Any) -> str | None:
    return hash_text(
        pod.pod.data_function.get_function_variation_data()["function_content_hash"]
    )


print("== Body edited, version unchanged (v0.0)")
same, pod = rates_job("v0", "v0.0", per_minute)  # same job name: same tables
same.run()
print("  run summary:", same.last_run)
S2 = read_job(same, store)
res = rows(S2["rates"].result)
current = body_hash(pod)
stale = [r for r in res if hash_text(r["__pf_var_function_content_hash"]) != current]
print(
    f"  RESULT store: {len(res)} results; function_content_hash differs from the current body's ({short_hash(current, 8)}): {len(stale)}"
)
print(
    f"  e.g. {short_hash(stale[0]['__pf_var_function_content_hash'], 8)} {stale[0]['__pf_var_function_version']} ran {stale[0]['__pod_ts']:%H:%M:%S} (git {stale[0]['__pf_var_git_hash'][:12]})"
)
print(
    "  a rate shown:",
    same.nodes["rates"].as_table().column("rate_hz")[0].as_py(),
    "(n/duration: the OLD body's value)",
)

print("\n== Body edited, major version bumped (v1.0)")
new, _ = rates_job("v0", "v1.0", per_minute)  # same job name, new uri: new tables
new.run()
S3 = read_job(new, store)
old_p = {
    r["__pipeline_base_entry_id"]: r
    for r in rows(S1["rates"].own(S1["rates"].pipeline))
}
new_p = {
    r["__pipeline_base_entry_id"]: r
    for r in rows(S3["rates"].own(S3["rates"].pipeline))
}
print(
    f"  PIPELINE store: v0 entries {len(old_p)}, v1.0 entries {len(new_p)}, paired by base entry id: {len(old_p.keys() & new_p.keys())}"
)
old_r = {r[KEY]: r for r in rows(S1["rates"].result)}
new_r = {r[KEY]: r for r in rows(S3["rates"].result)}
changed = [
    (old_p[k], new_p[k])
    for k in old_p.keys() & new_p.keys()
    if old_r[old_p[k]["__data_id"]]["rate_hz"]
    != new_r[new_p[k]["__data_id"]]["rate_hz"]
]
print(f"  RESULT store: per paired entry, the two results differ: {len(changed)}")
o, n = sorted(
    changed,
    key=lambda p: (p[0]["subject_id"], p[0]["session"], p[0]["probe"], p[0]["unit"]),
)[0]
ro, rn = old_r[o["__data_id"]], new_r[n["__data_id"]]
print(
    f"  e.g. {o['subject_id']} {o['session']} {o['probe']} unit {o['unit']}: {ro['rate_hz']:.4f} ({ro['__pf_var_function_version']}, body {short_hash(ro['__pf_var_function_content_hash'], 8)})"
    f" -> {rn['rate_hz']:.4f} ({rn['__pf_var_function_version']}, body {short_hash(rn['__pf_var_function_content_hash'], 8)})"
)
paths = [
    p
    for p in store.list_table_paths()
    if p[-1] in ("pdb_v1", "rdb_v1") and "firing_rate" in p
]
print(
    f"  firing_rate tables in the store: {sorted((p[4 if p[1] == '_result' else 3], p[-1]) for p in paths)}"
)
