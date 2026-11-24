"""The explicit-kind-in-id design, simulated (ITL-1425, the developer's direction).

In this process only (nothing under ``src/`` changes), ``golden_moves.patch("K_always")``
makes every source id ``<SOURCE_KIND>_v1:<name>`` (a given ``subjects`` read by a CSV
source is ``csv_v1:subjects``) and removes the class name from the source identity.

1. Without a stand-in: the same rows as two kinds under the given name ``subjects``.
2. With a simulated explicit stand-in: the CSV source swaps its kind part for the
   original's (``arrow_table_v1``), keeping its name; the ledger prototype checks that
   every record hashes equal first.

For each it prints the source ids, the tokens, the record ids, function calls in a fresh
job over one database, node content hashes and stored pipeline rows.

Run from ``docs/dev/specs/itl-1425-source-identity``:
``uv run python -m scripts.stand_in_explicit_kind``.
"""

from __future__ import annotations

import warnings

import orcapod.core.sources  # noqa: F401 - load the modules before patching
import orcapod.pipeline  # noqa: F401
from orcapod import PipelineJob
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource, CSVSource

from ._fixtures import KEYED, SCHEMA, arrow_table, scratch, write_files
from .golden_moves import patch
from .ledger_prototype import compare, ledger
from .stand_in_job import calls, ratio, show_store

warnings.simplefilter("ignore")


def run(db: InMemoryArrowDatabase, src) -> tuple[int, str]:
    job = PipelineJob(name="k", database=db)
    with job:
        ratio.pod(src, label="ratio")
    before = len(calls)
    job.run()
    return len(calls) - before, job.nodes["ratio"].content_hash().to_hex()[:12]


def describe(src) -> None:
    t = src.as_table(columns={"system_tags": True, "source": True})
    rid = next(c for c in t.column_names if c.startswith("_tag_record_id"))
    sid = next(c for c in t.column_names if c.startswith("_tag_source_id"))
    print(
        f"   {type(src).__name__:16s} source_id {src.source_id!r}; system tag "
        f"{t.column(sid)[0].as_py()!r}; record id {t.column(rid)[0].as_py()!r}; "
        f"token {t.column('_source_weight')[0].as_py()!r}"
    )


def main() -> None:
    patch("K_always")
    folder = scratch()
    paths = write_files(folder)
    original = ArrowTableSource(arrow_table(), source_id="subjects", **KEYED)
    csv = CSVSource(str(paths["csv"]), schema=SCHEMA, source_id="subjects", **KEYED)

    print("== 1. Two kinds, the same given name, no stand-in ==")
    describe(original)
    describe(csv)
    db = InMemoryArrowDatabase()
    for src in (original, csv):
        n, h = run(db, src)
        print(f"   {type(src).__name__:16s} calls {n}; node content hash {h}")
    show_store(db)

    print(
        "\n== 2. Explicit stand-in: the CSV swaps its kind part for the original's =="
    )
    check = compare(ledger(original), ledger(csv))
    print("   ledger check (row hashes per record id):", check)
    assert check["changed"] == 0
    kind = CSVSource.SOURCE_KIND
    CSVSource.SOURCE_KIND = "arrow_table"  # simulation of the swap only
    try:
        stand_in = CSVSource(
            str(paths["csv"]), schema=SCHEMA, source_id="subjects", **KEYED
        )
    finally:
        CSVSource.SOURCE_KIND = kind
    describe(stand_in)
    db = InMemoryArrowDatabase()
    for src in (original, stand_in):
        n, h = run(db, src)
        print(f"   {type(src).__name__:16s} calls {n}; node content hash {h}")
    show_store(db)
    print("   the ledger would record kind csv, version 1 and the file for these reads")


if __name__ == "__main__":
    main()
