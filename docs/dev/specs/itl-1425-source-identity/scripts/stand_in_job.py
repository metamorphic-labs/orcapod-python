"""Put each source kind in place of another in a job (ITL-1425, section 1).

The same rows as an ``ArrowTableSource``, a ``CSVSource`` (declared ``schema=``), a
``DeltaTableSource`` and a ``SQLiteTableSource``, all with the given ``source_id``
``subjects`` and the key column ``subject``. A function pod runs over them in two ways:

* Part A, bind: one job, built over the ``ArrowTableSource``; each other kind is bound
  into its slot in turn (``job.bind``) and the job runs again.
* Part B, a fresh job per kind over one shared database: what a user does when a table
  moved (a new script names the new source).

The script prints whether each bind is accepted, how many times the function ran, the
node hashes, whether the output (system tags and source-info tokens included) is equal,
and what the stored tables hold.

Run from ``docs/dev/specs/itl-1425-source-identity``:
``uv run python -m scripts.stand_in_job``. With ``--without-class-name`` the class name
is first removed from ``RootSource.identity_structure`` in this process (alternative C,
the patch of ``golden_moves``); nothing under ``src/`` changes.
"""

from __future__ import annotations

import sys
import warnings

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase

from ._fixtures import same_rows_sources, scratch

warnings.simplefilter("ignore")

calls: list[float] = []


@function_pod(output_keys="ratio")
def ratio(weight: float | None, count: int | None) -> float | None:
    calls.append(weight or 0.0)
    if weight is None or not count:
        return None
    return weight / count


def show_store(db: InMemoryArrowDatabase) -> None:
    for path in db.list_table_paths():
        table = db.get_all_records(path)
        assert table is not None
        name = "/".join(p if len(p) < 24 else p[:20] + "..." for p in path)
        print(f"   {name}: {table.num_rows} rows")
        if "__node_content_hash" in table.column_names:
            hashes = sorted(
                {h for h in table.column("__node_content_hash").to_pylist() if h}
            )
            print(f"      distinct __node_content_hash values: {len(hashes)}")


def row(kind: str, job: PipelineJob, before: int, previous) -> object:
    out = job.nodes["ratio"].as_table(columns={"source": True, "system_tags": True})
    print(
        f"   {kind:13s} calls: {len(calls) - before}; "
        f"node content_hash {job.nodes['ratio'].content_hash().to_hex()[:12]}; "
        f"slot content_hash {job.nodes['subjects'].content_hash().to_hex()[:12]}; "
        f"pipeline_hash {job.nodes['ratio'].pipeline_hash().to_hex()[:12]}; "
        f"output equal to previous: {previous.equals(out) if previous is not None else '-'}"
    )
    return out


def main() -> None:
    if "--without-class-name" in sys.argv:
        from .golden_moves import patch

        patch("C")
        print("** class name removed from the source identity (alternative C) **")
    sources = same_rows_sources(scratch())

    print("== Part A: one job, each kind bound in place of the previous ==")
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="stand_in", database=db)
    with job:
        ratio.pod(sources["arrow_table"], label="ratio")
    previous = None
    for kind, src in sources.items():
        if kind != "arrow_table":
            try:
                job.bind(sources={"subjects": src})
            except Exception as exc:  # noqa: BLE001 - the point is to show the refusal
                print(f"   {kind:13s} bind REFUSED: {type(exc).__name__}: {exc}")
                continue
        before = len(calls)
        job.run()
        previous = row(kind, job, before, previous)
    print("   stored:")
    show_store(db)
    print(
        "   note: the function node keeps the content hash it computed before the "
        "first bind; the slot's changes (see the column)."
    )

    print("\n== Part B: a fresh job per kind, one shared database ==")
    db = InMemoryArrowDatabase()
    previous = None
    for kind, src in sources.items():
        job = PipelineJob(name="stand_in", database=db)
        with job:
            ratio.pod(src, label="ratio")
        before = len(calls)
        job.run()
        previous = row(kind, job, before, previous)
        stored_view = job.nodes["ratio"].as_source().as_table()
        print(f"      node.as_source() rows: {stored_view.num_rows}")
    print("   stored:")
    show_store(db)


if __name__ == "__main__":
    main()
