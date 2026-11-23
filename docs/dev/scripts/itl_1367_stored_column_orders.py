"""ITL-1367: the column order of each stored table kind as main writes it today.

A job over one source with two tags (``z_tag``, ``a_tag``, declared in that order) and two
data columns (``y``, ``b``) runs a function pod, an operator node that logs (a ``Join``),
a cached source and a side-effect pod against an in-memory database that records the column order of
every ``add_records`` call, by table path. Input for the step 2 proposal
(``docs/dev/specs/itl-1367-stored-column-order.md``).

Run: ``uv run python docs/dev/scripts/itl_1367_stored_column_orders.py``
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.cached_source import CachedSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.side_effects import SideEffectPod
from orcapod.types import CacheMode

SEEN: dict[tuple[str, ...], list[str]] = {}


_add_records = InMemoryArrowDatabase.add_records


def _recording_add_records(
    self: InMemoryArrowDatabase,
    record_path: tuple[str, ...],
    records: pa.Table,
    *args: Any,
    **kwargs: Any,
) -> None:
    """``add_records``, recording each table's column order as first written."""
    SEEN.setdefault(self.base_path + tuple(record_path), records.column_names)
    _add_records(self, record_path, records, *args, **kwargs)


# Every view (``at``) of the database records too: the method is replaced on the class.
setattr(InMemoryArrowDatabase, "add_records", _recording_add_records)  # noqa: B010


def _source(name: str, data: list[str]) -> ArrowTableSource:
    fields = [
        pa.field("z_tag", pa.large_string(), nullable=False),
        pa.field("a_tag", pa.int64(), nullable=False),
        *(pa.field(n, pa.float64(), nullable=False) for n in data),
    ]
    arrays = [
        pa.array(["s1", "s2"], pa.large_string()),
        pa.array([1, 2], pa.int64()),
        *(pa.array([1.0, 2.0], pa.float64()) for _ in data),
    ]
    return ArrowTableSource(
        pa.Table.from_arrays(arrays, schema=pa.schema(fields)),
        tag_columns=["z_tag", "a_tag"],
        tag_only_columns=True,
        source_id=name,
    )


@function_pod(output_keys=["w_out", "c_out"])
def compute(y: float, b: float) -> tuple[float, float]:
    return y + b, y - b


def notify(ctx: Any, y: float, b: float) -> None:
    return None


def main() -> None:
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="itl1367", database=database)
    cached = CachedSource(_source("left", ["y", "b"]), cache_database=database)
    with job:
        left = cached
        right = _source("right", ["x", "e"])
        compute.pod(left, label="fn")
        Join()(left, right, label="joined")
        SideEffectPod(notify)(left, label="notify")
    job.nodes["joined"].attach_databases(
        pipeline_database=database, cache_mode=CacheMode.LOG
    )
    job.run()
    for path, names in sorted(SEEN.items()):
        print("/".join(p[:40] for p in path))
        print("   ", names)


if __name__ == "__main__":
    main()
