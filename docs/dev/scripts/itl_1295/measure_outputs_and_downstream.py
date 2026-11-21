"""ITL-1295: function OUTPUT values, and what a changed tag does downstream after a reuse.

usage: uv run python docs/dev/scripts/itl_1295/measure_outputs_and_downstream.py

Part 1, outputs: functions returning values of the types a Python annotation gives (the
type converter decides the Arrow type); each node is run, then read again by a NEW job
over the same in-memory database (a reuse run). The output column is compared with what
the function returned, exactly (floats as bits).

Part 2, downstream: node ``one`` (``inc``) over a source tagged ``k: date64`` off
midnight, then ``Join`` of ``one`` with another source keyed on the same ``k`` values,
and a ``GroupBy("k")``. The rows of the join and of the grouping are counted on the
first run and on a reuse run of a new job object.
"""

from __future__ import annotations

import datetime
import struct
import uuid
from pathlib import Path
from typing import Any

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import SyncPipelineOrchestrator

SNAN64 = struct.unpack("<d", struct.pack("<Q", 0x7FF0000000000001))[0]
NEG_ZERO = -0.0


@function_pod(output_keys="y")
def as_float(x: int) -> float:
    return SNAN64 if x == 10 else NEG_ZERO


@function_pod(output_keys="y")
def as_path(x: int) -> Path:
    return Path(f"/data/{x}")


@function_pod(output_keys="y")
def as_uuid(x: int) -> uuid.UUID:
    return uuid.UUID(int=x)


@function_pod(output_keys="y")
def as_date(x: int) -> datetime.date:
    return datetime.date(2020, 1, x // 10)


@function_pod(output_keys="y")
def as_datetime(x: int) -> datetime.datetime:
    return datetime.datetime(2020, 1, 1, 0, 0, 0, x, tzinfo=datetime.timezone.utc)


@function_pod(output_keys="y")
def as_float_list(x: int) -> list[float]:
    return [SNAN64, NEG_ZERO, float(x)]


@function_pod(output_keys="y")
def as_int(x: int) -> int:
    return x + 1


@function_pod(output_keys="y")
def inc(x: int) -> int:
    return x + 1


def exact(array: Any) -> list[Any]:
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()
    if isinstance(array, pa.ExtensionArray):
        return exact(array.storage)
    t = array.type
    if pa.types.is_large_list(t) or pa.types.is_list(t):
        values = exact(array.values)
        offsets = array.offsets.to_pylist()
        return [values[offsets[i] : offsets[i + 1]] for i in range(len(array))]
    if pa.types.is_floating(t):
        unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[t.bit_width]
        return [hex(b) for b in array.view(unsigned).to_pylist()]
    if pa.types.is_temporal(t):
        return array.view(pa.int32() if t.bit_width == 32 else pa.int64()).to_pylist()
    if pa.types.is_decimal(t):
        return [str(x) for x in array.to_pylist()]
    return array.to_pylist()


def int_source(name: str = "s") -> ArrowTableSource:
    arrays = [pa.array([1, 2], pa.int64()), pa.array([10, 20], pa.int64())]
    schema = pa.schema(
        pa.field(n, a.type, nullable=False) for n, a in zip("kx", arrays)
    )
    return ArrowTableSource(
        pa.Table.from_arrays(arrays, schema=schema),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id=name,
    )


def outputs() -> None:
    print("== Part 1: function outputs (in-memory database)")
    for name, pod in (
        ("as_int", as_int),
        ("as_float", as_float),
        ("as_path", as_path),
        ("as_uuid", as_uuid),
        ("as_date", as_date),
        ("as_datetime", as_datetime),
        ("as_float_list", as_float_list),
    ):
        db = InMemoryArrowDatabase()
        reads: dict[str, Any] = {}
        for attempt in ("run", "reuse"):
            job = PipelineJob(name="itl1295out", database=db)
            with job:
                pod.pod(int_source(), label="node")
            try:
                job.run(orchestrator=SyncPipelineOrchestrator())
                column = job.nodes["node"].as_table().column("y")
                reads[attempt] = (str(column.type), exact(column))
            except BaseException as error:  # noqa: BLE001
                reads[attempt] = f"RAISES {type(error).__name__}: {str(error)[:80]}"
        memory = pod.pod(int_source()).as_table().column("y")
        reads["in_memory"] = (str(memory.type), exact(memory))
        same = reads["run"] == reads["reuse"] == reads["in_memory"]
        print(f"   {name:14} {'exact' if same else 'DIFFER'} {reads}")


def date_source(name: str, extra: str) -> ArrowTableSource:
    arrays = [
        pa.array([1, 86_400_001], pa.int64()).cast(pa.date64()),
        pa.array([10, 20], pa.int64()),
    ]
    schema = pa.schema(
        pa.field(n, a.type, nullable=False) for n, a in zip(["k", extra], arrays)
    )
    return ArrowTableSource(
        pa.Table.from_arrays(arrays, schema=schema),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id=name,
    )


def downstream() -> None:
    print("== Part 2: downstream of a changed tag (in-memory database)")
    db = InMemoryArrowDatabase()
    for attempt in ("run", "reuse"):
        job = PipelineJob(name="itl1295down", database=db)
        with job:
            one = inc.pod(date_source("s", "x"), label="one")
            Join()(one, date_source("other", "w"), label="joined")
            # Two rows on two days: two groups; truncated to midnight they stay two.
        try:
            job.run(orchestrator=SyncPipelineOrchestrator())
            joined = job.nodes["joined"].as_table()
            print(
                f"   {attempt:5} one.k={exact(job.nodes['one'].as_table().column('k'))} "
                + f"joined rows={joined.num_rows} joined.k={exact(joined.column('k'))}"
            )
        except BaseException as error:  # noqa: BLE001
            print(f"   {attempt:5} RAISES {type(error).__name__}: {str(error)[:120]}")


if __name__ == "__main__":
    outputs()
    downstream()
