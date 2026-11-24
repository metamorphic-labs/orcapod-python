"""ITL-1403: a Delta store written by one deltalake version, read and appended by another.

One job writes every stored table kind into a ``DeltaTableDatabase``: a function pod
(result and pipeline tables), an operator node that logs (a ``Join``: the operator cache),
a cached source and a side-effect pod (its log). ``run`` runs the job over the rows of a
generation (``first``: two rows per source; ``second``: two more), so a second run appends
to every table the first created. ``dump`` writes every stored table, read back through
``DeltaTableDatabase``, with its rows sorted, plus each table's Delta protocol, as JSON.

    <python A> interop.py run STORE first
    <python B> interop.py run STORE second   # B reads and appends to A's tables
    <python A or B> interop.py dump STORE OUT.json

``interop.sh`` runs both directions and a single-version control and compares the dumps.
"""

from __future__ import annotations

import json
import sys
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

ROWS = {
    "first": [("s1", 1), ("s2", 2)],
    "second": [("s1", 1), ("s2", 2), ("s3", 3), ("s4", 4)],
}


def _source(
    name: str, data: list[str], rows: list[tuple[str, int]]
) -> ArrowTableSource:
    fields = [
        pa.field("z_tag", pa.large_string(), nullable=False),
        pa.field("a_tag", pa.int64(), nullable=False),
        *(pa.field(n, pa.float64(), nullable=False) for n in data),
    ]
    arrays = [
        pa.array([z for z, _ in rows], pa.large_string()),
        pa.array([a for _, a in rows], pa.int64()),
        *(
            pa.array([float(a) * (k + 1) for _, a in rows], pa.float64())
            for k in range(len(data))
        ),
    ]
    return ArrowTableSource(
        pa.Table.from_arrays(arrays, schema=pa.schema(fields)),
        tag_columns=["z_tag", "a_tag"],
        tag_only_columns=True,
        source_id=name,
        record_id_columns=["z_tag"],
    )


@function_pod(output_keys=["w_out", "c_out"])
def compute(y: float, b: float) -> tuple[float, float]:
    return y + b, y - b


def notify(ctx: Any, y: float, b: float) -> None:
    return None


def run(store: Path, generation: str) -> None:
    rows = ROWS[generation]
    database = DeltaTableDatabase(store)
    job = PipelineJob(name="itl1403", database=database)
    cached = CachedSource(_source("left", ["y", "b"], rows), cache_database=database)
    with job:
        compute.pod(cached, label="fn")
        Join()(cached, _source("right", ["x", "e"], rows), label="joined")
        SideEffectPod(notify)(cached, label="notify")
    job.nodes["joined"].attach_databases(
        pipeline_database=database, cache_mode=CacheMode.LOG
    )
    job.run()
    database.flush()


def _canonical(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {k: _canonical(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_canonical(v) for v in value]
    return value


def dump(store: Path, out: Path) -> None:
    database = DeltaTableDatabase(store)
    tables: dict[str, Any] = {}
    for path in sorted(database.list_table_paths()):
        table = database.get_all_records(path, record_id_column="__record_id")
        assert table is not None, path
        rows = sorted(
            (
                json.dumps(_canonical(row), sort_keys=True, default=str)
                for row in table.to_pylist()
            )
        )
        delta = deltalake.DeltaTable(str(store.joinpath(*path)))
        protocol = delta.protocol()
        tables["/".join(path)] = {
            "schema": [f"{f.name}: {f.type}" for f in table.schema],
            "rows": rows,
            "protocol": {
                "min_reader_version": protocol.min_reader_version,
                "min_writer_version": protocol.min_writer_version,
                "reader_features": protocol.reader_features,
                "writer_features": protocol.writer_features,
            },
            "operations": [h["operation"] for h in delta.history()][::-1],
        }
    out.write_text(
        json.dumps({"deltalake": deltalake.__version__, "tables": tables}, indent=1)
    )
    print(f"deltalake {deltalake.__version__}: {len(tables)} tables dumped to {out}")


if __name__ == "__main__":
    if sys.argv[1] == "run":
        run(Path(sys.argv[2]), sys.argv[3])
        print(f"deltalake {deltalake.__version__}: ran {sys.argv[3]} on {sys.argv[2]}")
    else:
        dump(Path(sys.argv[2]), Path(sys.argv[3]))
