"""Audit 1: what per-row lineage every exported operator leaves on a plain stream.

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.01_operator_lineage

Proves, per operator: the source-info tokens of every data column (scalar, list after
Batch/GroupBy/MergeJoin, nested after two aggregations), the system-tag column names after
joins, the folded digests after Batch/GroupBy, and that a dropping operator leaves nothing
behind about the rows it dropped.
"""

from __future__ import annotations

from ._common import sessions_source, short, show, units_source

import polars as pl
import pyarrow as pa

from orcapod.operators import (
    AddTags,
    Batch,
    DropColumns,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    Filter,
    GroupBy,
    Index,
    Join,
    MapColumns,
    MapData,
    MapTags,
    MergeJoin,
    Pick,
    PolarsFilter,
    Rejoin,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
    TagBy,
)
from orcapod.sources import ArrowTableSource, DictSource

units = units_source()
sessions = sessions_source()

print("== Sources: one source-info token per data cell, two system tags per row")
show("units", units)

print("\n== Join: name-extending system tags; data tokens pass through unchanged")
joined = Join()(units, sessions)
t = show("Join(units, sessions)", joined)
t_rev = Join()(sessions, units).as_table(all_info=True)
print(
    "  same system-tag names in both argument orders:",
    sorted(c for c in t.column_names if c.startswith("_tag"))
    == sorted(c for c in t_rev.column_names if c.startswith("_tag")),
)

print("\n== Name-preserving operators: tokens and system tags pass through")
for name, op in [
    ("SelectDataColumns", SelectDataColumns(["n_spikes"])),
    ("MapData", MapData({"n_spikes": "spikes"})),
    ("MapTags", MapTags({"session": "sess"})),
    ("AddTags", AddTags({"cohort": "A"})),
    ("TagBy", TagBy(["depth_um"])),
    ("SelectTagColumns", SelectTagColumns(["subject"])),
    ("DropTagColumns", DropTagColumns(["session"])),
    ("DropDataColumns", DropDataColumns(["depth_um"])),
    ("SelectColumns", SelectColumns(["subject", "n_spikes"])),
    ("DropColumns", DropColumns(["session", "depth_um"])),
    ("MapColumns", MapColumns({"session": "sess", "depth_um": "depth"})),
]:
    show(name, op(units), rows=1)

stats = DictSource(
    [
        {"subject": "m1", "session": "s1", "stats": {"a": 1, "b": 2}, "n": 5},
        {"subject": "m2", "session": "s1", "stats": {"a": 3, "b": 4}, "n": 6},
    ],
    tag_columns=["subject", "session"],
    tag_only_columns=True,
    source_id="stats",
)
show("Pick(stats, 'a', out='a')", Pick("stats", "a", out="a")(stats), rows=1)
show("Index(n_spikes, 0) over Batch(2)", Index("n_spikes", 0)(Batch(2)(units)), rows=1)

print("\n== Dropping operators: the dropped rows leave no trace in the result")
filtered = Filter({"subject": "m1"})(units)
show("Filter(subject == 'm1')", filtered, rows=4)
show(
    "PolarsFilter(n_spikes > 15)",
    PolarsFilter([pl.col("n_spikes") > 15])(units),
    rows=4,
)
nullable = ArrowTableSource(
    pa.table(
        {
            "subject": ["m1", "m2"],
            "session": ["s1", "s1"],
            "n_spikes": pa.array([1, None], pa.int64()),
            "depth_um": [5, 6],
        }
    ),
    tag_columns=["subject", "session"],
    tag_only_columns=True,
    source_id="nullable",
)
show("DropNulls(n_spikes)", DropNulls(["n_spikes"])(nullable), rows=4)
few_sessions = Filter({"subject": "m2"})(sessions)
show("SemiJoin(units, sessions where m2)", SemiJoin()(units, few_sessions), rows=4)
show("Join(units, sessions where m2)", Join()(units, few_sessions), rows=4)
print(
    "  -> each result is an ordinary table of the KEPT rows: no column, flag or count"
    " names the rows removed. Which rows were removed is only the difference of the"
    " record ids before and after the step."
)

print("\n== Reducing operators: list tokens; system tags folded into one digest")
show("Batch(2)", Batch(2)(units), rows=2)
grouped = GroupBy(["subject"])(Join()(units, sessions))
show("GroupBy(subject) over Join", grouped, rows=2)
show(
    "MergeJoin(units, units2)",
    MergeJoin()(units, units_source(source_id="units2")),
    rows=1,
)

print("\n== Two aggregations: nested list tokens")
twice = Batch(2)(GroupBy(["subject", "session"])(units))
show("Batch(2) over GroupBy(subject, session)", twice, rows=1)

print("\n== Rejoin: a diamond keeps the origin row's system tags unchanged")
left = SelectDataColumns(["n_spikes"])(units)
right = SelectDataColumns(["depth_um"])(units)
show("Rejoin(select n_spikes, select depth_um)", Rejoin()(left, right), rows=1)

print("\n== What the fold loses")
g = GroupBy(["subject"])(units).as_table(all_info=True)
rec = [c for c in g.column_names if c.startswith("_tag_record_id")][0]
print(f"  {short(rec)} = {g.column(rec).to_pylist()}")
print(
    "  The digest is one-way: the member record ids cannot be read from it."
    " The members are still named by the list-valued source-info tokens of every data"
    " column (one token per member, row-aligned):"
)
print("  _source_n_spikes =", g.column("_source_n_spikes").to_pylist())
