"""ITL-1370: the Delta commits of a job, by table kind: operation kinds and counts.

One job writes every stored table kind (a function pipeline and result table, a
side-effect log, an operator cache, a cached source) on a new Delta store, three times:
the first run, a second run with new rows, and a run with ``commit_every_rows=2`` over
more rows. Each table's history is printed oldest first. Run it on main and on the
ITL-1370 branch: a job on new tables must commit the same kinds the same number of
times (condition (a) of the ITL-1370 review).

Run: ``PYTHONPATH=src uv run python docs/dev/scripts/itl_1370_delta_commits.py``
"""

from __future__ import annotations

import os
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any

import deltalake
import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.cached_source import CachedSource
from orcapod.databases import DeltaTableDatabase
from orcapod.side_effects import SideEffectPod
from orcapod.types import CacheMode

warnings.simplefilter("ignore")


def _source(name: str, data: list[str], rows: list[int]) -> ArrowTableSource:
    fields = [
        pa.field("z_tag", pa.large_string(), nullable=False),
        pa.field("a_tag", pa.int64(), nullable=False),
        *(pa.field(n, pa.float64(), nullable=False) for n in data),
    ]
    arrays = [
        pa.array([f"s{n}" for n in rows], pa.large_string()),
        pa.array(rows, pa.int64()),
        *(pa.array([float(n) for n in rows]) for _ in data),
    ]
    return ArrowTableSource(
        pa.Table.from_arrays(arrays, schema=pa.schema(fields)),
        tag_columns=["z_tag", "a_tag"],
        tag_only_columns=True,
        record_id_columns=["a_tag"],
        source_id=name,
    )


@function_pod(output_keys=["w_out", "c_out"])
def compute(y: float, b: float) -> tuple[float, float]:
    return y + b, y - b


def _notify(ctx: Any, y: float, b: float) -> None:
    return None


def _run(database: Any, rows: list[int], **run: Any) -> None:
    job = PipelineJob(name="commits", database=database)
    cached = CachedSource(_source("left", ["y", "b"], rows), cache_database=database)
    with job:
        compute.pod(cached, label="fn")
        Join()(cached, _source("right", ["x", "e"], rows), label="joined")
        SideEffectPod(_notify)(cached, label="notify")
    job.nodes["joined"].attach_databases(
        pipeline_database=database, cache_mode=CacheMode.LOG
    )
    job.run(**run)


def main() -> None:
    root = Path(tempfile.mkdtemp())
    database = DeltaTableDatabase(root)
    _run(database, [1, 2])
    _run(database, [1, 2, 3, 4])
    _run(database, list(range(1, 10)), commit_every_rows=2)
    for log in sorted(root.rglob("_delta_log")):
        table = log.parent
        history = deltalake.DeltaTable(str(table)).history()
        operations = [str(entry["operation"]) for entry in reversed(history)]
        print(table.name, operations)


if __name__ == "__main__":
    main()
    sys.stdout.flush()
    os._exit(0)
