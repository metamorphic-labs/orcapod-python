"""ITL-640 Q3 (i): does a column read back from each database have the Arrow schema it
was written with, as ``StarfixArrowHasher.hash_schema`` sees it?

Run: ``uv run python docs/dev/scripts/itl_640/q3i_storage_round_trips.py [tmpdir]``

For every database class (in-memory, Delta Lake, the connector database over SQLite)
and every column type below, one table ``(id, x)`` is written with ``add_records``,
flushed and read back with ``get_all_records``. The hash of the schema of ``x`` read
back is compared with the hash of the schema written. ``REFUSED`` means the database
refuses the type when it is written (``require_storable``), which is not a silent
change. Every ``DIFFERS`` is a place where something stored and read again (a node's
stored view, ``as_source``, a cached source, a loaded job) would get another identity
than the same rows in memory.
"""

from __future__ import annotations

import datetime as dt
import decimal
import sys
import tempfile
import uuid
import warnings
from pathlib import Path
from typing import Any

import pyarrow as pa

from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.databases.connector_arrow_database import ConnectorArrowDatabase
from orcapod.databases.sqlite_connector import SQLiteConnector

warnings.simplefilter("ignore")

ctx = get_default_context()
conv = ctx.type_converter
hasher = ctx.arrow_hasher
D = decimal.Decimal
ts = [dt.datetime(2026, 1, 1, 0, 0, 1), dt.datetime(2026, 1, 2)]
tsz = [t.replace(tzinfo=dt.timezone.utc) for t in ts]
dates = [dt.date(2026, 1, 1), dt.date(2026, 1, 2)]


def col(values, arrow_type, nullable=True):
    return pa.field("x", arrow_type, nullable=nullable), pa.array(values, arrow_type)


def logical_column(py_type, values):
    """A logical-type column built by the converter, as a source builds it."""
    ctx.semantic_hasher.hash_object(py_type)
    table = conv.python_dicts_to_arrow_table(
        [{"x": v} for v in values], python_schema={"x": py_type}
    )
    return table.schema.field("x"), table.column("x").combine_chunks()


COLUMNS = {
    "int8": lambda: col([1, 2], pa.int8()),
    "int16": lambda: col([1, 2], pa.int16()),
    "int32": lambda: col([1, 2], pa.int32()),
    "int64": lambda: col([1, 2], pa.int64()),
    "uint8": lambda: col([1, 200], pa.uint8()),
    "uint16": lambda: col([1, 2], pa.uint16()),
    "uint32": lambda: col([1, 2], pa.uint32()),
    "uint64": lambda: col([1, 2], pa.uint64()),
    "float32": lambda: col([1.5, 2.5], pa.float32()),
    "float64": lambda: col([1.5, 2.5], pa.float64()),
    "int64 not null": lambda: col([1, 2], pa.int64(), nullable=False),
    "bool": lambda: col([True, False], pa.bool_()),
    "string": lambda: col(["a", "b"], pa.string()),
    "binary": lambda: col([b"a", b"b"], pa.binary()),
    "ts[s]": lambda: col(ts, pa.timestamp("s")),
    "ts[ms]": lambda: col(ts, pa.timestamp("ms")),
    "ts[us]": lambda: col(ts, pa.timestamp("us")),
    "ts[ns]": lambda: col(ts, pa.timestamp("ns")),
    "ts[us,UTC]": lambda: col(tsz, pa.timestamp("us", tz="UTC")),
    "ts[ns,UTC]": lambda: col(tsz, pa.timestamp("ns", tz="UTC")),
    "ts[us,Europe/Berlin]": lambda: col(tsz, pa.timestamp("us", tz="Europe/Berlin")),
    "date32": lambda: col(dates, pa.date32()),
    "date64": lambda: col(dates, pa.date64()),
    "decimal(10,2)": lambda: col([D("1.25"), D("2.50")], pa.decimal128(10, 2)),
    "decimal(38,4)": lambda: col([D("1.25"), D("2.50")], pa.decimal128(38, 4)),
    "large_list<int64 nullable item>": lambda: col(
        [[1, 2], [3]], pa.large_list(pa.field("item", pa.int64()))
    ),
    "large_list<int64 non-null item>": lambda: col(
        [[1, 2], [3]], pa.large_list(pa.field("item", pa.int64(), nullable=False))
    ),
    "large_list<int8>": lambda: col([[1, 2], [3]], pa.large_list(pa.int8())),
    "large_list<large_list<int64>>": lambda: col(
        [[[1], [2]], [[3]]], pa.large_list(pa.large_list(pa.int64()))
    ),
    "struct<a:int32, b:string>": lambda: col(
        [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}],
        pa.struct([("a", pa.int32()), ("b", pa.string())]),
    ),
    "struct<a:int64 not null>": lambda: col(
        [{"a": 1}, {"a": 2}], pa.struct([pa.field("a", pa.int64(), nullable=False)])
    ),
    "fixed_size_list<int64>[2]": lambda: col([[1, 2], [3, 4]], pa.list_(pa.int64(), 2)),
    "dictionary<int32,string>": lambda: (
        pa.field("x", pa.dictionary(pa.int32(), pa.string())),
        pa.array(["a", "b"]).dictionary_encode(),
    ),
    "Path": lambda: logical_column(Path, [Path("/a"), Path("/b")]),
    "uuid.UUID": lambda: logical_column(uuid.UUID, [uuid.uuid4(), uuid.uuid4()]),
    "list[Path]": lambda: logical_column(list[Path], [[Path("/a")], [Path("/b")]]),
    "set[int]": lambda: logical_column(set[int], [{1, 2}, {3}]),
    "dict[str,int]": lambda: logical_column(dict[str, int], [{"a": 1}, {"b": 2}]),
}


def databases(root: Path):
    return {
        "InMemory": InMemoryArrowDatabase(),
        "Delta": DeltaTableDatabase(root / "delta"),
        "SQLite": ConnectorArrowDatabase(SQLiteConnector(str(root / "store.db"))),
    }


def describe(field: "pa.Field[Any]") -> str:
    return f"{field.type}{'' if field.nullable else ' not null'}"


def main(root: Path) -> None:
    dbs = databases(root)
    ids = pa.array([b"r1", b"r2"], pa.large_binary())
    print(f"{'column':34}" + "".join(f"{name:44}" for name in dbs))
    for i, (label, make) in enumerate(COLUMNS.items()):
        try:
            field, values = make()
        except Exception as exc:  # noqa: BLE001
            print(f"{label:34}cannot build: {type(exc).__name__}: {exc}")
            continue
        written = pa.Table.from_arrays(
            [ids, values],
            schema=pa.schema(
                [pa.field("id", pa.large_binary(), nullable=False), field]
            ),
        )
        h_written = hasher.hash_schema(pa.schema([written.schema.field("x")]))
        cells = []
        for name, db in dbs.items():
            path = ("t", f"c{i}")
            try:
                db.add_records(path, written, record_id_column="id", flush=True)
                back = db.get_all_records(path, record_id_column="id")
                assert back is not None
                read_field = back.schema.field("x")
                h_back = hasher.hash_schema(pa.schema([read_field]))
                same_values = (
                    back.column("x").to_pylist() == written.column("x").to_pylist()
                )
                if h_back == h_written:
                    cells.append("same" + ("" if same_values else " (VALUES DIFFER)"))
                else:
                    cells.append(f"DIFFERS -> {describe(read_field)}"[:42])
            except Exception as exc:  # noqa: BLE001
                cells.append(f"REFUSED {type(exc).__name__}: {exc}"[:42])
        print(f"{label:34}" + "".join(f"{c:44}" for c in cells))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(Path(sys.argv[1]))
    else:
        with tempfile.TemporaryDirectory() as tmp:
            main(Path(tmp))
