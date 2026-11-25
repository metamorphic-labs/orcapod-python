"""Build and run the scalar storage workloads used by the ITL-1464 study."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from time import perf_counter
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import orcapod as op
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import SyncPipelineOrchestrator

TAG_NAMES = ("group_key", "order_key")
WIDTHS = ("row1", "row3", "row10", "group")


@op.function_pod(output_keys=("v0",))
def narrow(value: float) -> float:
    """Return one numeric result per row."""
    return value * 1.25


@op.function_pod(output_keys=("v0", "v1", "v2"))
def medium(value: float) -> tuple[float, float, float]:
    """Return three numeric results per row."""
    return value * 1.25, value**2, math.sin(value)


@op.function_pod(output_keys=tuple(f"v{i}" for i in range(10)))
def wide(
    value: float,
) -> tuple[float, float, float, float, float, float, float, float, float, float]:
    """Return ten numeric results per row."""
    return (
        value * 1.25,
        value**2,
        math.sin(value),
        math.cos(value),
        math.atan(value),
        math.sqrt(abs(value)),
        value * 2,
        value * 100,
        value * -0.5,
        value + 1,
    )


@op.function_pod(output_keys=("v0", "v1", "v2"))
def grouped(value: list[float]) -> tuple[int, float, float]:
    """Return count, mean and range in one record per group."""
    return len(value), sum(value) / len(value), max(value) - min(value)


def sample(path: Path, limit: int) -> pa.Table:
    """Read one bounded prefix, using export row order."""
    file = pq.ParquetFile(path)
    flat = [field for field in file.schema_arrow if not pa.types.is_nested(field.type)]
    batches = []
    remaining = limit
    for batch in file.iter_batches(
        batch_size=min(limit, 65_536), columns=[field.name for field in flat]
    ):
        batches.append(batch.slice(0, remaining))
        remaining -= batches[-1].num_rows
        if remaining == 0:
            break
    return pa.Table.from_batches(batches)


def adjacent_fraction(
    column: pa.ChunkedArray[Any] | pa.Array[Any], *, equal: bool = False
) -> float:
    """Measure the share of adjacent pairs satisfying equality or ascending order."""
    if len(column) < 2:
        return 1.0
    left, right = column.slice(0, len(column) - 1), column.slice(1)
    compare = pc.equal if equal else pc.less_equal
    matches = pc.mean(pc.cast(compare(left, right), pa.float64())).as_py()
    return float(matches or 0.0)


def project(table: pa.Table) -> tuple[pa.Table, dict[str, int | float]]:
    """Select scalar numeric data and a repeated key using only their cardinalities."""
    scalar = [
        column
        for column in table.columns
        if (
            pa.types.is_integer(column.type)
            or pa.types.is_floating(column.type)
            or pa.types.is_temporal(column.type)
            or pa.types.is_string(column.type)
            or pa.types.is_large_string(column.type)
        )
        and column.null_count == 0
    ]
    numeric = [
        column
        for column in scalar
        if pa.types.is_integer(column.type) or pa.types.is_floating(column.type)
    ]
    cardinalities = [
        (int(pc.count_distinct(column).as_py()), column) for column in scalar
    ]
    value = max(numeric, key=lambda col: int(pc.count_distinct(col).as_py()))
    value = pc.cast(value, pa.float64(), safe=False)
    value = pc.if_else(pc.is_finite(value), value, 0.0)
    key_candidates = [
        item for item in cardinalities if 1 < item[0] <= max(2, table.num_rows // 4)
    ]
    if key_candidates:
        # Prefer a meaningful repeated key with about 100 groups in a large sample,
        # instead of any constant or a per-row identity.
        key_count, key = min(
            key_candidates, key=lambda item: abs(math.log(item[0] / 100))
        )
        indexed = pc.index_in(key, value_set=pc.unique(key))
    else:
        key_count = min(10, table.num_rows)
        indexed = pa.array(np.arange(table.num_rows) % key_count, pa.int32())
    data = pa.table({"group_key": indexed, "order_key": value, "value": value})
    return data, {
        "rows": table.num_rows,
        "columns": table.num_columns,
        "group_count": key_count,
        "group_adjacent_equal": adjacent_fraction(indexed, equal=True),
        "value_distinct": int(pc.count_distinct(value).as_py()),
        "value_adjacent_ascending": adjacent_fraction(value),
    }


def read_profile(export: Path | None, profile: Path | None) -> dict[str, int | float]:
    """Load calibration statistics from an export prefix or an aggregate JSON file."""
    if export is not None:
        return project(sample(export, 250_000))[1]
    if profile is None:
        raise ValueError("A calibration export or profile is required")
    return cast("dict[str, int | float]", json.loads(profile.read_text()))


def synthetic(rows: int, profile: dict[str, int | float], seed: int) -> pa.Table:
    """Build anonymous keys and scalar values matched to aggregate export statistics."""
    rng = np.random.default_rng(seed)
    template_rows = int(profile["rows"])
    groups = max(2, int(profile["group_count"]))
    distinct = min(
        rows, max(2, round(rows * profile["value_distinct"] / template_rows))
    )
    order = np.linspace(0, distinct - 1, rows).astype(np.int64)
    # Shuffle a subset of ranks to introduce local reversals without changing cardinality.
    swap_rate = min(1.0, 2 * (1 - profile["value_adjacent_ascending"]))
    swaps = rng.random(rows) < swap_rate
    shuffled = order[swaps]
    rng.shuffle(shuffled)
    order[swaps] = shuffled
    # Geometrically distributed run lengths approximate adjacent key repetition.
    advance = rng.random(rows) >= profile["group_adjacent_equal"]
    key = np.cumsum(advance) % groups
    values = order / 1000.0
    return pa.table(
        {
            "group_key": pa.array(key, pa.int32()),
            "order_key": pa.array(values),
            "value": pa.array(values),
        }
    )


def run(table: pa.Table, root: Path, case: str, batch_size: int) -> float:
    """Compute one case with the public synchronous job and a Delta store."""
    started = perf_counter()
    source = op.sources.ArrowTableSource(
        table,
        tag_columns=TAG_NAMES,
        tag_only_columns=True,
        infer_nullable=True,
        source_id="storage-study",
    )
    job = op.PipelineJob(database=DeltaTableDatabase(root, batch_size=batch_size))
    with job:
        match case:
            case "row1":
                narrow.pod(source)
            case "row3":
                medium.pod(source)
            case "row10":
                wide.pod(source)
            case "group":
                grouped.pod(source.group_by("group_key", member_order=("order_key",)))
            case _:
                raise ValueError(case)
    job.run(orchestrator=SyncPipelineOrchestrator())
    return perf_counter() - started


def main() -> None:
    """Run a case on one export or on a seeded synthetic projection."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    parser.add_argument("case", choices=WIDTHS)
    parser.add_argument("--export", type=Path)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    if args.export:
        table, profile = project(sample(args.export, args.rows))
    else:
        profile = {
            "rows": args.rows,
            "group_count": 100,
            "group_adjacent_equal": 0.95,
            "value_distinct": args.rows,
            "value_adjacent_ascending": 0.98,
        }
        table = synthetic(args.rows, profile, 1464)
    elapsed = run(table, args.destination, args.case, args.batch_size)
    print(json.dumps({"profile": profile, "seconds": elapsed}))


if __name__ == "__main__":
    main()
