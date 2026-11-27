"""Audit 5: what each source kind writes as per-row lineage.

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.05_source_kinds

Proves, per source kind, with and without key columns: the source id (given and
default), the record id (``row_<n>`` or the canonical JSON of the key columns) and the
source-info token of each data column. Every kind follows one rule
(``<source id>::<record id>::<column>``), so a token resolves to a source row by the
source id and the record-id system tag. ``CachedSource`` keeps its source's tokens;
``DerivedSource(tags_as_data=...)`` starts afresh under the id the user gives.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import polars as pl
import pyarrow as pa
from ._common import sessions_source

from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import (
    ArrowTableSource,
    CachedSource,
    CSVSource,
    DataFrameSource,
    DeltaTableSource,
    DerivedSource,
    DictSource,
    ListSource,
    SQLiteTableSource,
)

tmp = Path(tempfile.mkdtemp())
rows = [
    {"subject": "m1", "session": "s1", "n_spikes": 10, "depth_um": 200},
    {"subject": "m2", "session": "s1", "n_spikes": 30, "depth_um": 600},
]
tags = ["subject", "session"]
(tmp / "u.csv").write_text(
    "subject,session,n_spikes,depth_um\nm1,s1,10,200\nm2,s1,30,600\n"
)
pl.DataFrame(rows).write_delta(str(tmp / "delta"))
with sqlite3.connect(tmp / "u.db") as db:
    db.execute(
        "create table units (subject text, session text, n_spikes integer,"
        " depth_um integer, primary key (subject, session))"
    )
    db.executemany(
        "insert into units values (?,?,?,?)", [tuple(r.values()) for r in rows]
    )


def report(name, source):
    t = source.as_table(all_info=True)
    rid = [c for c in t.column_names if c.startswith("_tag_record_id")]
    toks = sorted(c for c in t.column_names if c.startswith("_source_"))
    print(f"{name:28s} source_id={source.source_id!r}")
    print(
        f"{'':28s} record id={t.column(rid[0])[0].as_py()!r}  "
        f"token={t.column(toks[0])[0].as_py()!r}"
    )


report(
    "ArrowTableSource (given id)",
    ArrowTableSource(
        pa.Table.from_pylist(rows),
        tag_columns=tags,
        tag_only_columns=True,
        source_id="u",
    ),
)
report(
    "ArrowTableSource (default)",
    ArrowTableSource(
        pa.Table.from_pylist(rows), tag_columns=tags, tag_only_columns=True
    ),
)
report(
    "DictSource, key columns",
    DictSource(
        rows,
        tag_columns=tags,
        tag_only_columns=True,
        source_id="u",
        record_id_columns=["session", "subject"],
    ),
)
report(
    "ListSource",
    ListSource(
        "n", [10, 30], tag_function=lambda v, i: {"i": i}, expected_tag_keys=["i"]
    ),
)
report(
    "DataFrameSource",
    DataFrameSource(pl.DataFrame(rows), tag_columns=tags, tag_only_columns=True),
)
report(
    "CSVSource (default id)",
    CSVSource(str(tmp / "u.csv"), tag_columns=tags, tag_only_columns=True),
)
report(
    "DeltaTableSource",
    DeltaTableSource(
        tmp / "delta", tag_columns=tags, tag_only_columns=True, source_id="delta_units"
    ),
)
report(
    "SQLiteTableSource (pk)",
    SQLiteTableSource(tmp / "u.db", "units", tag_columns=tags, tag_only_columns=True),
)
inner = ArrowTableSource(
    pa.Table.from_pylist(rows), tag_columns=tags, tag_only_columns=True, source_id="u"
)
report("CachedSource over 'u'", CachedSource(inner, InMemoryArrowDatabase()))
report(
    "DerivedSource tags_as_data",
    DerivedSource(
        sessions_source(), tags_as_data={"session": "session_name"}, source_id="renamed"
    ),
)
print(
    "\nPollingSource: same rule (requires key columns; record id = key JSON);"
    " not run here, it needs a polling implementation."
)
