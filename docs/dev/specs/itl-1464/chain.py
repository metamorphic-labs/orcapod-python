"""Measure a job shaped like a whole analysis on one session (ITL-1464).

Each run is one process::

    uv run python docs/dev/specs/itl-1464/chain.py setup --scale 1.0 --store <dir>
    uv run python docs/dev/specs/itl-1464/chain.py <step> --store <dir> [--sync]

The tables are ``measure.py``'s: synthetic, read by the real ``SpiralDBTableSource`` over
the disk-backed fake of the SpiralDB client. At scale 1.0 the event table has about 8
million rows and the time series about 2 million. The steps build up one job:

- ``semi_join``: the events of the kept items;
- ``filter``: those in a time range;
- ``group_by``: one row per item, its event times a list;
- ``series``: the time series, filtered, its nulls dropped, one row per session, and a
  function over it;
- ``lookup_join``: the filtered events joined with a small table;
- ``chain``: all but ``lookup_join``, and a function over both branches.

Each prints one JSON line: seconds, peak physical footprint, peak resident memory and
the peak of the Arrow pool; ``--digest`` adds a digest of the last node's rows as a set,
to compare one version of the library with another.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parent))

import measure  # noqa: E402

ITEMS_TABLE = "item"
EVENT_ROWS = 8_100_000
SERIES_ROWS = 2_200_000
STEPS = ("semi_join", "filter", "group_by", "series", "lookup_join", "chain")
fake = measure.fake


def items_table() -> pa.Table:
    """One row per item, with a label that keeps seven of ten."""
    rows = measure.ITEMS
    table = measure.lookup_table().select(measure.ITEM_KEY)
    labels = ["kept" if i % 10 < 7 else "left" for i in range(rows)]
    return table.append_column("label", pa.array(labels, pa.string()))


def setup(scale: float) -> None:
    """Write the tables, in key order, and record their schemas."""
    from orcapod.utils import arrow_utils

    measure.BLOCKS = 6
    store = fake._store()
    for name, keys, table in (
        (
            measure.SERIES,
            measure.SERIES_KEYS,
            measure.time_series_table(int(SERIES_ROWS * scale)),
        ),
        (
            measure.EVENTS,
            measure.EVENT_KEYS,
            measure.event_table(int(EVENT_ROWS * scale)),
        ),
        (measure.LOCATIONS, measure.LOCATION_KEYS, measure.lookup_table()),
        (ITEMS_TABLE, measure.ITEM_KEY, items_table()),
    ):
        made = fake.ExternalProject(measure.PROJECT).create_table(
            f"default.{name}", key_schema=[table.schema.field(k) for k in keys]
        )
        ordered = fake._order([f for f in table.schema if f.name not in keys])
        table = table.select([*keys, *(f.name for f in ordered)])
        store.write(made.table_id, table)
        recorded = arrow_utils.normalize_table_to_large_types(table).schema
        recorded = pa.schema([f.with_nullable(f.name not in keys) for f in recorded])
        fake.migrate(measure.PROJECT, {name: recorded})


def build(step: str) -> None:
    """Build the named step's streams inside a job."""
    from orcapod import FunctionPod, col
    from orcapod.core.data_function import PythonDataFunction

    low = 1_700_000_000_000_000
    inside = col("timestamp").between(low, low + 10**15)

    def peaks(series_time: list[int], x_wide: list[float | None]) -> list[int]:
        """Every 100th time where ``x`` is positive."""
        x = np.asarray(x_wide, dtype=float)
        return np.asarray(series_time)[x > 0][::100].tolist()

    def count(event_time: list[int], peak_times: list[int]) -> list[int]:
        """How many events follow each of the first 50 peaks."""
        times = np.sort(np.asarray(event_time))
        first = np.asarray(peak_times[:50])
        low = np.searchsorted(times, first)
        return (np.searchsorted(times, first + 300_000) - low).tolist()

    def trains() -> Any:
        items = measure._source(ITEMS_TABLE)
        kept = items.filter(col("label").is_in(["kept"]))
        chosen = measure._source(measure.EVENTS).semi_join(kept)
        if step == "semi_join":
            return chosen
        chosen = chosen.filter(inside)
        if step == "filter":
            return chosen
        if step == "lookup_join":
            return chosen.join(measure._source(measure.LOCATIONS))
        grouped = chosen.group_by(measure.ITEM_KEY)
        return grouped.map_data({"timestamp": "event_time"})

    def series() -> Any:
        chosen = measure._source(measure.SERIES).filter(inside)
        present = chosen.drop_nulls(["x_small", "y_small"])
        grouped = present.group_by(["subject_id", "session_start"])
        renamed = grouped.map_data({"timestamp": "series_time"})
        pod = FunctionPod(PythonDataFunction(peaks, output_keys="peak_times"))
        return pod(renamed, label="peaks")

    if step == "series":
        series()
    elif step == "chain":
        pod = FunctionPod(PythonDataFunction(count, output_keys="counts"))
        pod(trains(), series(), label="counts")
    else:
        trains()


def digest(table: pa.Table) -> str:
    """A digest of the table's rows as a set, columns by name."""
    total = 0
    names = sorted(table.column_names)
    for batch in table.select(names).to_batches(max_chunksize=8_192):
        for row in batch.to_pylist():
            text = json.dumps(row, default=str, sort_keys=True).encode()
            total += int.from_bytes(hashlib.sha256(text).digest()[:16], "big")
    return f"{table.num_rows}:{total % (1 << 128):032x}"


def main() -> None:
    """Run one step and print its measurements."""
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("step", choices=["setup", *STEPS])
    _ = parser.add_argument("--store", required=True, type=Path)
    _ = parser.add_argument("--scale", type=float, default=0.25)
    _ = parser.add_argument("--sync", action="store_true")
    _ = parser.add_argument("--digest", action="store_true")
    args = parser.parse_args()
    warnings.simplefilter("ignore")
    with fake.install(args.store / "spiral"):
        fake.scan_batch_rows = 65_536
        if args.step == "setup":
            setup(args.scale)
            return
        import tempfile

        from orcapod import PipelineJob
        from orcapod.databases import DeltaTableDatabase
        from orcapod.pipeline import AsyncPipelineOrchestrator

        with tempfile.TemporaryDirectory(dir=args.store) as results:
            database = DeltaTableDatabase(results)
            with PipelineJob(name="analysis", database=database) as job:
                build(args.step)
            started = time.perf_counter()
            if args.sync:
                job.run()
            else:
                job.run(orchestrator=AsyncPipelineOrchestrator())
            record: dict[str, Any] = {
                "step": args.step,
                "path": "sync" if args.sync else "async",
                "events": measure._raw(measure.EVENTS).num_rows,
                "seconds": round(time.perf_counter() - started, 1),
                "footprint_mib": (measure._peak_footprint_bytes() or 0) >> 20,
                "rss_mib": measure._peak_rss_bytes() >> 20,
                "arrow_pool_mib": pa.default_memory_pool().max_memory() >> 20,
                "nodes": {
                    name: node.as_table().num_rows
                    for name, node in job.nodes.items()
                    if args.digest and node.node_type != "source"
                },
            }
            if args.digest:
                wanted = {"system_tags": True, "source": True}
                record["digests"] = {
                    name: digest(_without_run_ids(node.as_table(columns=wanted)))
                    for name, node in job.nodes.items()
                    if node.node_type == "operator"
                }
            print(json.dumps(record), flush=True)


def _without_run_ids(table: pa.Table) -> pa.Table:
    """``table`` without the tokens of function outputs, which hold a run's own ids."""
    names = [n for n in table.column_names if n not in ("_source_peak_times",)]
    return table.select(names)


if __name__ == "__main__":
    main()
