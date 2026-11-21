"""ITL-640 Q3 (ii): the same Python data through every source builder: which Arrow
types result, column by column, and which builders would give one table two
identities under an Arrow schema hash.

Run: ``uv run python docs/dev/scripts/itl_640/q3ii_builders.py [tmpdir]``

Builders: ``DictSource``, ``DataFrameSource`` over a pandas and over a polars frame,
``CSVSource`` (a CSV written from the same rows), ``ArrowTableSource`` over
``pa.Table.from_pylist`` and ``ListSource`` (one column per element value).
"""

from __future__ import annotations

import csv
import datetime as dt
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa

from orcapod.contexts import get_default_context
from orcapod.sources import (
    ArrowTableSource,
    CSVSource,
    DataFrameSource,
    DictSource,
    ListSource,
)

warnings.simplefilter("ignore")

ctx = get_default_context()

ROWS = [
    {
        "k": "a",
        "i": 1,
        "f": 1.5,
        "s": "x",
        "b": True,
        "ts": dt.datetime(2026, 1, 1, 12, 0),
        "tsz": dt.datetime(2026, 1, 1, 12, 0, tzinfo=dt.timezone.utc),
        "d": dt.date(2026, 1, 1),
        "li": [1, 2],
        "n": None,
    },
    {
        "k": "b",
        "i": 2,
        "f": 2.5,
        "s": "y",
        "b": False,
        "ts": dt.datetime(2026, 1, 2, 12, 0),
        "tsz": dt.datetime(2026, 1, 2, 12, 0, tzinfo=dt.timezone.utc),
        "d": dt.date(2026, 1, 2),
        "li": [3],
        "n": 5,
    },
]
CSV_COLUMNS = ["k", "i", "f", "s", "b", "n"]


def build(root: Path) -> dict[str, Any]:
    kw: dict[str, Any] = {
        "tag_columns": ["k"],
        "tag_only_columns": True,
        "source_id": "s",
    }
    out: dict[str, Any] = {}
    for label, make in (
        ("DictSource", lambda: DictSource(ROWS, **kw)),
        ("DataFrameSource(pandas)", lambda: DataFrameSource(pd.DataFrame(ROWS), **kw)),
        ("DataFrameSource(polars)", lambda: DataFrameSource(pl.DataFrame(ROWS), **kw)),
        (
            "ArrowTableSource(from_pylist)",
            lambda: ArrowTableSource(pa.Table.from_pylist(ROWS), **kw),
        ),
    ):
        try:
            out[label] = make()
        except Exception as exc:  # noqa: BLE001
            out[label] = f"REFUSED {type(exc).__name__}: {exc}"[:160]
    path = root / "rows.csv"
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in ROWS:
            writer.writerow(
                {c: ("" if row[c] is None else row[c]) for c in CSV_COLUMNS}
            )
    try:
        out["CSVSource"] = CSVSource(str(path), **kw)
    except Exception as exc:  # noqa: BLE001
        out["CSVSource"] = f"REFUSED {type(exc).__name__}: {exc}"[:160]
    return out


def field_text(f: "pa.Field[Any]") -> str:
    return f"{f.type}{'' if f.nullable else ' nn'}"


def main(root: Path) -> None:
    sources = build(root)
    columns = list(ROWS[0])
    tables = {}
    for label, src in sources.items():
        if isinstance(src, str):
            print(f"{label}: {src}")
            continue
        tables[label] = src.as_table()  # type: ignore[attr-defined]
    labels = list(tables)
    print(f"{'col':5}" + "".join(f"{lab[:30]:32}" for lab in labels))
    for c in columns:
        cells = []
        for lab in labels:
            t = tables[lab]
            cells.append(field_text(t.schema.field(c)) if c in t.column_names else "-")
        print(f"{c:5}" + "".join(f"{cell[:30]:32}" for cell in cells))
    print()
    print(f"Per column, the builders whose Arrow field hashes like {labels[0]}'s:")
    base = tables[labels[0]]
    for c in columns:
        h0 = ctx.arrow_hasher.hash_schema(pa.schema([base.schema.field(c)]))
        same, diff = [], []
        for lab in labels[1:]:
            t = tables[lab]
            if c not in t.column_names:
                continue
            h = ctx.arrow_hasher.hash_schema(pa.schema([t.schema.field(c)]))
            (same if h == h0 else diff).append(lab)
        print(f"  {c:5} same: {same}  DIFFERENT: {diff}")

    print()
    print("ListSource vs DictSource, one data column 'v' of Python ints / datetimes:")
    utc = dt.timezone.utc
    for values in (
        [1, 2],
        [dt.datetime(2026, 1, 1, tzinfo=utc), dt.datetime(2026, 1, 2, tzinfo=utc)],
    ):
        ls = ListSource("v", values)
        ds = DictSource(
            [{"element_index": i, "v": v} for i, v in enumerate(values)],
            tag_columns=["element_index"],
            tag_only_columns=True,
        )
        lf = ls.as_table().schema.field("v")
        df = ds.as_table().schema.field("v")
        print(
            f"  {type(values[0]).__name__}: ListSource {field_text(lf)} / DictSource {field_text(df)}"
            f" -> hash equal: {ctx.arrow_hasher.hash_schema(pa.schema([lf])) == ctx.arrow_hasher.hash_schema(pa.schema([df]))}"
        )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(Path(sys.argv[1]))
    else:
        with tempfile.TemporaryDirectory() as tmp:
            main(Path(tmp))
