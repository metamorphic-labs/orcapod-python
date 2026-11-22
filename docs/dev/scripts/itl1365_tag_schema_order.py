"""ITL-1365: what the tag-schema order fix changes, and what it does not.

Run on two checkouts (before and after the fix) with the same output folder name in
two places, then compare the two ``summary.json`` files:

    uv run python docs/dev/scripts/itl1365_tag_schema_order.py <out dir>
    uv run python docs/dev/scripts/itl1365_tag_schema_order.py <out dir> --load <before dir>

Every source kind is built over a table laid out ``a, b, c, x, y`` with the tags
declared ``c, b, a`` (``a`` and ``b`` data-tags), plus a raw ``ArrowTableStream``.
For each it records the keys, the ``output_schema`` order, the rows of
``as_table(all_info=True)`` (column names and values), the content and pipeline
hashes and the source id; then a job over it (``DropTagColumns``, ``MapTags`` and a
function pod, on a Delta database), run and saved: every node's hashes, every stored
table path with its column names, the saved job's text; and a standalone ``LOG``
operator node: its stored table path and columns. With ``--load``, the job saved by
the other checkout is loaded here, against its own database, and its node hashes and
stored rows are recorded too.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import warnings
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.csv as pacsv
from deltalake import write_deltalake

from orcapod import function_pod
from orcapod.core.nodes import OperatorJobNode
from orcapod.core.operators import DropTagColumns, MapTags
from orcapod.core.sources import (
    ArrowTableSource,
    CSVSource,
    DataFrameSource,
    DeltaTableSource,
    DictSource,
)
from orcapod.core.sources.sqlite_table_source import SQLiteTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import PipelineJob
from orcapod.types import CacheMode

warnings.simplefilter("ignore")

TABLE = pa.table(
    {
        "a": pa.array([1, 2, 3], pa.int64()),
        "b": pa.array(["p", "q", "r"], pa.large_string()),
        "c": pa.array([7, 8, 9], pa.int64()),
        "x": pa.array([0.5, 1.5, 2.5], pa.float64()),
        "y": pa.array(["u", "v", "w"], pa.large_string()),
    }
)
TAGS = ["c", "b", "a"]
TAG_ONLY = ["c"]


@function_pod(output_keys="z")
def double(x: float | None) -> float | None:
    return None if x is None else 2 * x


def _sources(root: Path) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    pacsv.write_csv(TABLE, root / "t.csv")
    write_deltalake(str(root / "delta"), TABLE)
    db = root / "t.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT, c INTEGER, x REAL, y TEXT)"
        )
        conn.executemany("INSERT INTO t VALUES (:a,:b,:c,:x,:y)", TABLE.to_pylist())
    common = {"tag_only_columns": TAG_ONLY}
    return {
        "raw_stream": ArrowTableStream(TABLE, tag_columns=TAGS, data_tags=["a", "b"]),
        "arrow_table": ArrowTableSource(
            TABLE, tag_columns=TAGS, source_id="s", **common
        ),
        "dict": DictSource(
            TABLE.to_pylist(), tag_columns=TAGS, source_id="s", **common
        ),
        "data_frame": DataFrameSource(
            pl.from_arrow(TABLE), tag_columns=TAGS, source_id="s", **common
        ),
        "csv": CSVSource(
            str(root / "t.csv"),
            tag_columns=TAGS,
            source_id="s",
            schema=TABLE.schema,
            **common,
        ),
        "delta": DeltaTableSource(
            root / "delta", tag_columns=TAGS, source_id="s", **common
        ),
        "sqlite": SQLiteTableSource(db, "t", tag_columns=TAGS, source_id="s", **common),
    }


def _rows(table: pa.Table) -> dict[str, Any]:
    return {"columns": table.column_names, "rows": [str(r) for r in table.to_pylist()]}


def _stored(database: DeltaTableDatabase) -> dict[str, Any]:
    out = {}
    for path in sorted(database.list_table_paths()):
        table = database.get_all_records(path)
        out["/".join(path)] = None if table is None else table.column_names
    return out


def _job_nodes(job: PipelineJob) -> dict[str, Any]:
    return {
        label: [str(node.content_hash()), str(node.pipeline_hash())]
        for label, node in sorted(job.nodes.items())
    }


def main(out: Path, load_from: Path | None) -> None:
    summary: dict[str, Any] = {}
    for kind, source in _sources(out / "inputs").items():
        entry: dict[str, Any] = {
            "keys": source.keys(all_info=True),
            "output_schema_order": [
                list(s) for s in source.output_schema(all_info=True)
            ],
            "table": _rows(source.as_table(all_info=True)),
            "content_hash": str(source.content_hash()),
            "pipeline_hash": str(source.pipeline_hash()),
            "source_id": getattr(source, "source_id", None),
        }
        if kind != "raw_stream":
            database = DeltaTableDatabase(out / "jobs" / kind / "db")
            job = PipelineJob(name="itl1365", database=database)
            with job:
                dropped = DropTagColumns(["a"])(source, label="dropped")
                mapped = MapTags({"b": "bb"})(dropped, label="mapped")
                double.pod(mapped, label="doubled")
            job.run()
            job.save(out / "jobs" / kind / "job.json")
            entry["job_nodes"] = _job_nodes(job)
            entry["job_node_tables"] = {
                label: _rows(node.as_table(all_info=True))
                for label, node in sorted(job.nodes.items())
            }
            entry["stored"] = _stored(database)
            entry["saved_job"] = json.loads(
                (out / "jobs" / kind / "job.json").read_text()
            )
            op_db = DeltaTableDatabase(out / "jobs" / kind / "opdb")
            node = OperatorJobNode(
                operator=DropTagColumns(["a"]),
                input_streams=[source],
                pipeline_database=op_db,
                cache_mode=CacheMode.LOG,
            )
            node.run()
            entry["operator_node"] = [
                str(node.content_hash()),
                str(node.pipeline_hash()),
            ]
            entry["operator_stored"] = _stored(op_db)
            if load_from is not None:
                other = load_from / "jobs" / kind
                loaded = PipelineJob.load(
                    other / "job.json", database=DeltaTableDatabase(other / "db")
                )
                entry["loaded_other_job_nodes"] = _job_nodes(loaded)
                entry["loaded_other_stored_rows"] = _rows(
                    loaded.nodes["doubled"].as_source().as_table(all_info=True)
                )
        summary[kind] = entry
    (out / "summary.json").write_text(json.dumps(summary, indent=1, default=str))


if __name__ == "__main__":
    load = Path(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[2] == "--load" else None
    main(Path(sys.argv[1]), load)
