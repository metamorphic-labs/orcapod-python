"""ITL-1280: does ``node.as_source()`` keep its origin's Arrow types?

Run: ``uv run python docs/dev/scripts/itl_640/itl_1280_as_source_types.py``

For each narrow or unit-bearing Arrow type, as a tag and as a data column, over an
operator node (``map_data``, CacheMode.LOG) and a function node (tag only: a function
node's data are its declared outputs), on an in-memory and a Delta database, by the
sync and the async run: the type ``as_source()`` reports before the run (no rows) and
after it (stored rows), against the node's own type. Prints one line per case.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import decimal
import tempfile
import warnings
from pathlib import Path
from typing import Any

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource
from orcapod.types import CacheMode

warnings.simplefilter("ignore")

TS = [dt.datetime(2026, 1, 1, 0, 0, 1), dt.datetime(2026, 1, 2)]
TYPES: dict[str, pa.Array[Any]] = {
    "int8": pa.array([1, 2], pa.int8()),
    "int32": pa.array([1, 2], pa.int32()),
    "uint16": pa.array([1, 2], pa.uint16()),
    "float16": pa.array([1.5, 2.5], pa.float32()).cast(pa.float16()),
    "float32": pa.array([1.5, 2.5], pa.float32()),
    "ts[s]": pa.array(TS, pa.timestamp("s")),
    "ts[us] naive": pa.array(TS, pa.timestamp("us")),
    "ts[ms, Europe/Berlin]": pa.array(TS, pa.timestamp("ms", tz="Europe/Berlin")),
    "decimal128(10,2)": pa.array(
        [decimal.Decimal("1.25"), decimal.Decimal("2.50")], pa.decimal128(10, 2)
    ),
}


@function_pod(output_keys="y")
def _plus(w: float) -> float:
    return w + 1


def _table(column: pa.Array[Any], role: str) -> pa.Table:
    """Tags ``k`` and ``g``, data ``x`` and ``w``; the column under test is ``g``
    (tag) or ``x`` (data)."""
    g = column if role == "tag" else pa.array([1, 2], pa.int64())
    x = column if role == "data" else pa.array([1, 2], pa.int64())
    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("g", g.type),
            pa.field("x", x.type),
            pa.field("w", pa.float64(), nullable=False),
        ]
    )
    return pa.Table.from_arrays(
        [pa.array(["r1", "r2"], pa.large_string()), g, x, pa.array([0.5, 1.5])],
        schema=schema,
    )


def measure(type_name: str, role: str, node_kind: str, db_kind: str, path: str) -> str:
    column = TYPES[type_name]
    folder = Path(tempfile.mkdtemp())
    database = (
        InMemoryArrowDatabase() if db_kind == "memory" else DeltaTableDatabase(folder)
    )
    source = ArrowTableSource(
        _table(column, role),
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="s",
    )
    name = "g" if role == "tag" else "z"
    job = PipelineJob(name="j", database=database)
    with job:
        if node_kind == "operator":
            source.map_data({"x": "z"}, label="node")
        else:
            _plus.pod(source, label="node")
    node = job.nodes["node"]
    if node_kind == "operator":
        node.attach_databases(
            pipeline_database=database.at("j"), cache_mode=CacheMode.LOG
        )
    try:
        before = node.as_source().as_table().schema.field(name).type
        before_hash = node.as_source().pipeline_hash()
    except Exception as exc:  # noqa: BLE001
        return f"before: {type(exc).__name__}: {str(exc)[:120]}"
    try:
        job.run() if path == "sync" else asyncio.run(job.run_async())
    except Exception as exc:  # noqa: BLE001
        return f"run: {type(exc).__name__}: {str(exc)[:120]}"
    own = node.as_table().schema.field(name).type
    try:
        after = node.as_source().as_table().schema.field(name).type
        after_hash = node.as_source().pipeline_hash()
    except Exception as exc:  # noqa: BLE001
        return f"after: {type(exc).__name__}: {str(exc)[:140]}"
    ok = before == after == own == column.type and before_hash == after_hash
    return (
        f"{'same' if ok else 'DIFFERS'}  before {before}  after {after}  node {own}"
        f"{'' if before_hash == after_hash else '  (pipeline hash moved)'}"
    )


for type_name in TYPES:
    for role in ("tag", "data"):
        for node_kind in ("operator", "function"):
            if node_kind == "function" and role == "data":
                continue
            for db_kind in ("memory", "delta"):
                for path in ("sync", "async"):
                    result = measure(type_name, role, node_kind, db_kind, path)
                    print(
                        f"{type_name:22} {role:4} {node_kind:8} {db_kind:6} {path:5} {result}"
                    )
