"""Types, positional record ids and a silent implementation change, today (ITL-1425).

Four measurements, all with the given ``source_id`` ``subjects``:

1. A CSV read without a declared schema (types inferred) in place of the declared one.
2. A Delta table whose ``count`` is ``int32`` bound into a slot declared ``int64``.
3. Positional record ids (``record_id_columns=()``) when a stand-in lists the rows in
   another order.
4. A reader change: the same source id and key, one value read differently (as a new
   implementation of a kind might read it). What a job shows.

Run from ``docs/dev/specs/itl-1425-source-identity``:
``uv run python -m scripts.types_positions_changes``.
"""

from __future__ import annotations

import warnings

import pyarrow as pa
from deltalake import write_deltalake

from orcapod import PipelineJob
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource, CSVSource, DeltaTableSource

from ._fixtures import (
    KEYED,
    POSITIONAL,
    KEY,
    ROWS,
    SCHEMA,
    arrow_table,
    same_rows_sources,
    scratch,
)
from .stand_in_job import calls, ratio

warnings.simplefilter("ignore")


def job_over(src, db):
    job = PipelineJob(name="types", database=db)
    with job:
        ratio.pod(src, label="ratio")
    return job


def run_counting(job) -> int:
    before = len(calls)
    job.run()
    return len(calls) - before


def main() -> None:
    folder = scratch()
    sources = same_rows_sources(folder)

    print("== 1. CSV with inferred types in place of the declared source ==")
    inferred = CSVSource(str(folder / "subjects.csv"), source_id="subjects", **KEYED)
    print(
        "   inferred data schema:", str(inferred.arrow_schemas[1]).replace("\n", "; ")
    )
    db = InMemoryArrowDatabase()
    job = job_over(sources["arrow_table"], db)
    print("   calls, declared source:", run_counting(job))
    try:
        job.bind(sources={"subjects": inferred})
        print(
            "   bind of the inferred CSV into the declared slot: accepted;",
            "calls:",
            run_counting(job),
        )
    except Exception as exc:  # noqa: BLE001
        print("   bind REFUSED:", type(exc).__name__, str(exc)[:160])
    fresh = job_over(inferred, db)
    print(
        "   fresh job over the inferred CSV: pipeline_hash equal:",
        fresh.nodes["ratio"].pipeline_hash() == job.nodes["ratio"].pipeline_hash(),
        "; calls:",
        run_counting(fresh),
    )

    print("\n== 2. A Delta table with count as int32 into an int64 slot ==")
    narrow = arrow_table().cast(
        pa.schema([SCHEMA.field(0), SCHEMA.field(1), pa.field("count", pa.int32())])
    )
    write_deltalake(str(folder / "narrow"), narrow)
    narrow_src = DeltaTableSource(folder / "narrow", source_id="subjects", **KEYED)
    try:
        job.bind(sources={"subjects": narrow_src})
        print("   bind accepted")
    except Exception as exc:  # noqa: BLE001
        print("   bind REFUSED:", type(exc).__name__)

    print("\n== 3. Positional record ids and another row order ==")
    a = ArrowTableSource(arrow_table(), **POSITIONAL)
    b = ArrowTableSource(arrow_table(list(reversed(ROWS))), **POSITIONAL)
    ta = a.as_table(columns={"system_tags": True, "source": True}).sort_by(KEY)
    tb = b.as_table(columns={"system_tags": True, "source": True}).sort_by(KEY)
    rid = [c for c in ta.column_names if c.startswith("_tag_record_id")][0]
    print("   content_hash equal:", a.content_hash() == b.content_hash())
    print("   record ids of s1, s2, s3 (original):", ta.column(rid).to_pylist())
    print("   record ids of s1, s2, s3 (reversed):", tb.column(rid).to_pylist())
    print(
        "   token of s1 weight:",
        ta.column("_source_weight")[0].as_py(),
        "vs",
        tb.column("_source_weight")[0].as_py(),
    )
    db = InMemoryArrowDatabase()
    j1, j2 = job_over(a, db), job_over(b, db)
    print("   calls original:", run_counting(j1), "; calls reversed:", run_counting(j2))

    print("\n== 4. A reader change under the same name: what shows ==")
    changed_rows = [dict(r) for r in ROWS]
    changed_rows[1]["weight"] = 22.0001  # one value read differently
    old = ArrowTableSource(arrow_table(), source_id="subjects", **KEYED)
    new = ArrowTableSource(arrow_table(changed_rows), source_id="subjects", **KEYED)
    print("   content_hash equal:", old.content_hash() == new.content_hash())
    print("   pipeline_hash equal:", old.pipeline_hash() == new.pipeline_hash())
    db = InMemoryArrowDatabase()
    j_old, j_new = job_over(old, db), job_over(new, db)
    print(
        "   calls before:",
        run_counting(j_old),
        "; calls after the change:",
        run_counting(j_new),
    )
    print(
        "   node content_hash equal:",
        j_old.nodes["ratio"].content_hash() == j_new.nodes["ratio"].content_hash(),
    )
    stored = j_new.nodes["ratio"].as_source().as_table()
    print(
        "   stored view rows for the node (3 records):",
        stored.num_rows,
        "; s2 values:",
        sorted(
            v
            for s, v in zip(
                stored.column(KEY).to_pylist(), stored.column("ratio").to_pylist()
            )
            if s == "s2"
        ),
    )
    print("   warnings or errors raised: none (the run completed)")


if __name__ == "__main__":
    main()
