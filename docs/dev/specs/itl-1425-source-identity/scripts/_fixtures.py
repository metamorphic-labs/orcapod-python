"""Shared synthetic fixtures: the same rows as four source kinds (ITL-1425).

Synthetic data only. Every source gets the same given ``source_id`` and the same key
column, so any difference the scripts print comes from the kind, not from the names.
"""

from __future__ import annotations

import csv
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, TypedDict

import pyarrow as pa

SOURCE_ID = "subjects"
KEY = "subject"

ROWS = [
    {"subject": "s1", "weight": 25.5, "count": 3},
    {"subject": "s2", "weight": 22.0, "count": 5},
    {"subject": "s3", "weight": 27.25, "count": 7},
]


class Keyed(TypedDict):
    """Keyword arguments of a source keyed by ``subject``."""

    tag_columns: list[str]
    record_id_columns: list[str]
    tag_only_columns: bool


class Positional(TypedDict):
    """Keyword arguments of a source whose record ids are positions."""

    tag_columns: list[str]
    record_id_columns: tuple[()]
    tag_only_columns: bool
    source_id: str


KEYED: Keyed = {
    "tag_columns": [KEY],
    "record_id_columns": [KEY],
    "tag_only_columns": True,
}
POSITIONAL: Positional = {
    "tag_columns": [KEY],
    "record_id_columns": (),
    "tag_only_columns": True,
    "source_id": SOURCE_ID,
}

SCHEMA = pa.schema(
    [
        pa.field("subject", pa.large_string(), nullable=False),
        pa.field("weight", pa.float64()),
        pa.field("count", pa.int64()),
    ]
)


def arrow_table(rows: list[dict[str, Any]] = ROWS) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def write_files(folder: Path, rows: list[dict[str, Any]] = ROWS) -> dict[str, Path]:
    """Write the rows as a CSV file, a Delta table and a SQLite table."""
    from deltalake import write_deltalake

    csv_path = folder / "subjects.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(SCHEMA.names))
        writer.writeheader()
        writer.writerows(rows)

    delta_path = folder / "subjects_delta"
    write_deltalake(str(delta_path), arrow_table(rows), mode="overwrite")

    sqlite_path = folder / "subjects.sqlite"
    con = sqlite3.connect(sqlite_path)
    con.execute("DROP TABLE IF EXISTS subjects")
    con.execute(
        "CREATE TABLE subjects (subject TEXT PRIMARY KEY NOT NULL, weight REAL, count INTEGER)"
    )
    con.executemany(
        "INSERT INTO subjects VALUES (?, ?, ?)",
        [(r["subject"], r["weight"], r["count"]) for r in rows],
    )
    con.commit()
    con.close()
    return {"csv": csv_path, "delta": delta_path, "sqlite": sqlite_path}


def same_rows_sources(
    folder: Path, *, source_id: str | None = SOURCE_ID, csv_schema: bool = True
):
    """The same rows as six source kinds, each with the same given id and key column."""
    import polars as pl

    from orcapod.sources import (
        ArrowTableSource,
        CSVSource,
        DataFrameSource,
        DeltaTableSource,
        DictSource,
        SQLiteTableSource,
    )

    paths = write_files(folder)
    return {
        "arrow_table": ArrowTableSource(arrow_table(), source_id=source_id, **KEYED),
        "csv": CSVSource(
            str(paths["csv"]),
            schema=SCHEMA if csv_schema else None,
            source_id=source_id,
            **KEYED,
        ),
        "delta_table": DeltaTableSource(paths["delta"], source_id=source_id, **KEYED),
        "sqlite_table": SQLiteTableSource(
            paths["sqlite"], "subjects", source_id=source_id, **KEYED
        ),
        "dict": DictSource(ROWS, data_schema=SCHEMA, source_id=source_id, **KEYED),
        "data_frame": DataFrameSource(
            pl.DataFrame(ROWS), schema=SCHEMA, source_id=source_id, **KEYED
        ),
    }


def scratch() -> Path:
    return Path(tempfile.mkdtemp(prefix="itl1425-"))
