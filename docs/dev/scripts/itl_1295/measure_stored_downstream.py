"""ITL-1295: does a changed tag get STORED by a node that computes after a reuse?

usage: uv run python docs/dev/scripts/itl_1295/measure_stored_downstream.py

Job 1 runs only node ``one`` over a source tagged ``k: date64`` off midnight. Job 2, a new
job over the same in-memory database, runs ``one`` (reused) and a NEW node ``two``
(``dbl``) over it, which computes. ``k`` in ``two``'s stored pipeline table (read from
the database directly) and as ``two`` presents it in that run are compared with the input.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import measure_read_path as m  # noqa: E402  # pyright: ignore[reportImplicitRelativeImport]  # a sibling script

from orcapod import PipelineJob  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from orcapod.pipeline import SyncPipelineOrchestrator  # noqa: E402

for name in ("int64 (control)", "date64 off midnight", "struct<a: date64>"):
    values = m.CASES[name]
    want = m.exact(values)
    db = InMemoryArrowDatabase()
    first = PipelineJob(name="itl1295", database=db)
    with first:
        m.inc.pod(m.source(values), label="one")
    first.run(orchestrator=SyncPipelineOrchestrator())
    second = PipelineJob(name="itl1295", database=db)
    with second:
        one = m.inc.pod(m.source(values), label="one")
        m.dbl.pod(one, label="two")
    summary = second.run(orchestrator=SyncPipelineOrchestrator())
    counts = {
        lbl: (summary.nodes[lbl].computed, summary.nodes[lbl].reused)
        for lbl in ("one", "two")
    }
    stored = [
        m.exact(table.column("k"))
        for p in db.list_table_paths()
        if p[1] == "dbl"
        and p[-1] == "pdb_v1"
        and (table := db.get_all_records(p)) is not None
    ]
    presented = m.exact(second.nodes["two"].as_table().column("k"))
    print(
        f"{name:22} (computed, reused)={counts} two stored k={stored} "
        + f"two presented k={presented} input={want}"
    )


# Columns that differ between any two runs: a random data id, the source-info tokens
# naming it, a time stamp.
PER_RUN = ("__data_id", "_source_y", "__pod_ts")


def stored_two(db: InMemoryArrowDatabase) -> dict[str, dict[str, str]]:
    """Node two's stored entries: per record id (the stable identity of an input row),
    every column's exact value; columns that differ between runs are left out."""
    [table] = [
        db.get_all_records(p)
        for p in db.list_table_paths()
        if p[1] == "dbl" and p[-1] == "pdb_v1"
    ]
    assert table is not None
    [record_id] = [c for c in table.column_names if c.startswith("_tag_record_id")]
    columns = {
        c: m.exact(table.column(c))
        for c in table.column_names
        if c not in PER_RUN and "timestamp" not in c
    }
    rows: dict[str, dict[str, str]] = {}
    for i, key in enumerate(table.column(record_id).to_pylist()):
        assert key not in rows, f"two entries for record {key}"
        rows[key] = {c: repr(values[i]) for c, values in columns.items()}
    return rows


print("Entries of 'two' stored after a reuse of 'one' vs. in one fresh job (date64):")
values = m.CASES["date64 off midnight"]
db_a = InMemoryArrowDatabase()
first = PipelineJob(name="itl1295", database=db_a)
with first:
    m.inc.pod(m.source(values), label="one")
first.run(orchestrator=SyncPipelineOrchestrator())
second = PipelineJob(name="itl1295", database=db_a)
with second:
    m.dbl.pod(m.inc.pod(m.source(values), label="one"), label="two")
second.run(orchestrator=SyncPipelineOrchestrator())
db_b = InMemoryArrowDatabase()
fresh = PipelineJob(name="itl1295", database=db_b)
with fresh:
    m.dbl.pod(m.inc.pod(m.source(values), label="one"), label="two")
fresh.run(orchestrator=SyncPipelineOrchestrator())
a, b = stored_two(db_a), stored_two(db_b)
print(f"   records: {sorted(a)} vs {sorted(b)}")
for record in sorted(a):
    for column, value in a[record].items():
        verdict = "same" if b.get(record, {}).get(column) == value else "DIFFER"
        print(f"   {record} {column[:50]:50} {verdict}")
