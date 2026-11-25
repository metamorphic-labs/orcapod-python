"""Measure the memory of one step of a workshop-shaped pipeline (ITL-1464).

One step per process, so that a step's peak is its own::

    uv run python docs/dev/specs/itl-1464/measure.py setup --rows 2177155 --store <dir>
    uv run python docs/dev/specs/itl-1464/measure.py <step> --store <dir>

``run_measurements.py`` runs every step under a memory guard and writes the table.

The tables are synthetic: the column names and Arrow types of three tables of a
SpiralDB sample dataset (a gaze time series, a spike event table and a unit location
table), with generated values. They are stored in the disk-backed fake of the SpiralDB
client that the test suite uses (``tests/test_databases/fake_spiral_sdk.py``), so the
real ``SpiralDBTableSource`` reads them: the code under measurement is the library's,
only the service is replaced. The fake memory-maps the stored file, so the rows it
hands over are not private memory of the process until they are copied.

Each step prints one JSON line: the rows and Arrow bytes that went in, the Arrow bytes
of the result, the peak resident memory of the process (``ru_maxrss``), the peak of
the Arrow memory pool, and the peak of Python's own allocations when ``--tracemalloc``
is given.
"""

from __future__ import annotations

import argparse
import gc
import json
import resource
import sys
import time
import tracemalloc
import warnings
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa

REPOSITORY = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPOSITORY))

from tests.test_databases import fake_spiral_sdk as fake  # noqa: E402

PROJECT = "synthetic-sample-000000"
GAZE = "experiment_gaze_online_timeseries"
SPIKES = "spike_event"
LOCATIONS = "unit_location"

SESSION_START = datetime(2026, 1, 1, 12, 0, 0)
SUBJECT = "subject"
HASH = "0123456789abcdef0123456789abcdef"
EXPERIMENTS = 6
UNITS = 300

GAZE_KEYS = [
    "experiment_preprocessing_hash",
    "subject_id",
    "session_start_datetime",
    "experiment_index",
    "timestamp",
]
EXPERIMENT_KEY = GAZE_KEYS[:4]
SPIKE_KEYS = [
    "spike_extraction_hash",
    "subject_id",
    "session_start_datetime",
    "recording_index",
    "probe_index",
    "timestamp",
    "unit_id",
]
UNIT_KEY = [
    "spike_extraction_hash",
    "subject_id",
    "session_start_datetime",
    "recording_index",
    "probe_index",
    "unit_id",
]
LOCATION_KEYS = [
    "spike_extraction_hash",
    "localization_hash",
    "subject_id",
    "session_start_datetime",
    "recording_index",
    "probe_index",
    "unit_id",
    "atlas_id",
    "atlas_label_region_short",
]


def _constant(value: Any, kind: pa.DataType, rows: int) -> pa.Array[Any]:
    return pa.array([value] * rows, kind)


def gaze_table(rows: int) -> pa.Table:
    """Return ``rows`` rows shaped like one session of the gaze time series."""
    rng = np.random.default_rng(0)
    per_experiment = -(-rows // EXPERIMENTS)
    experiment = np.repeat(np.arange(EXPERIMENTS, dtype=np.uint32), per_experiment)[
        :rows
    ]
    timestamp = np.arange(rows, dtype=np.uint64) * 5_000 + 1_700_000_000_000_000
    x = rng.normal(size=rows)
    y = rng.normal(size=rows)
    # A few nulls, as blinks leave: ``drop_nulls`` has rows to drop.
    missing = rng.random(rows) < 0.02
    return pa.table(
        {
            "experiment_preprocessing_hash": _constant(HASH, pa.string(), rows),
            "subject_id": _constant(SUBJECT, pa.string(), rows),
            "session_start_datetime": _constant(
                SESSION_START, pa.timestamp("us"), rows
            ),
            "experiment_index": pa.array(experiment),
            "timestamp": pa.array(timestamp),
            "eye_tracking_software": _constant("tracker", pa.string(), rows),
            "eye_tracking_software_version": _constant("1.2.3", pa.string(), rows),
            "eye_x_deg": pa.array(x.astype(np.float32), mask=missing),
            "eye_x_px": pa.array(x * 100, mask=missing),
            "eye_y_deg": pa.array(y.astype(np.float32), mask=missing),
            "eye_y_px": pa.array(y * 100, mask=missing),
            "gaze_unreliable": pa.array(missing),
            "pupil_size_norm": pa.array(rng.random(rows)),
        }
    )


def spike_table(rows: int) -> pa.Table:
    """Return ``rows`` rows shaped like one session of the spike event table."""
    rng = np.random.default_rng(1)
    timestamp = np.arange(rows, dtype=np.uint64) * 400 + 1_700_000_000_000_000
    return pa.table(
        {
            "spike_extraction_hash": _constant(HASH, pa.string(), rows),
            "subject_id": _constant(SUBJECT, pa.string(), rows),
            "session_start_datetime": _constant(
                SESSION_START, pa.timestamp("us"), rows
            ),
            "recording_index": _constant(0, pa.uint32(), rows),
            "probe_index": _constant(0, pa.uint32(), rows),
            "timestamp": pa.array(timestamp),
            "unit_id": pa.array(rng.integers(0, UNITS, rows).astype(np.uint32)),
            "spike_amplitude_uv": pa.array(rng.normal(size=rows).astype(np.float32)),
        }
    )


def location_table() -> pa.Table:
    """Return one row per unit, shaped like the unit location table."""
    rng = np.random.default_rng(2)
    rows = UNITS
    return pa.table(
        {
            "spike_extraction_hash": _constant(HASH, pa.string(), rows),
            "localization_hash": _constant(HASH[::-1], pa.string(), rows),
            "subject_id": _constant(SUBJECT, pa.string(), rows),
            "session_start_datetime": _constant(
                SESSION_START, pa.timestamp("us"), rows
            ),
            "recording_index": _constant(0, pa.uint32(), rows),
            "probe_index": _constant(0, pa.uint32(), rows),
            "unit_id": pa.array(np.arange(rows, dtype=np.uint32)),
            "atlas_id": _constant("atlas", pa.string(), rows),
            "atlas_label_region_short": _constant("V1", pa.string(), rows),
            "atlas_label_region_full": _constant("area", pa.string(), rows),
            "chamber_id": _constant("chamber", pa.string(), rows),
            "chamber_x_um": pa.array(rng.normal(size=rows)),
            "chamber_y_um": pa.array(rng.normal(size=rows)),
            "chamber_z_um": pa.array(rng.normal(size=rows)),
        }
    )


def setup(rows: int) -> None:
    """Write the three tables into the fake store, in key order, and record them."""
    from orcapod.utils import arrow_utils

    store = fake._store()
    for name, keys, table in (
        (GAZE, GAZE_KEYS, gaze_table(rows)),
        (SPIKES, SPIKE_KEYS, spike_table(rows)),
        (LOCATIONS, LOCATION_KEYS, location_table()),
    ):
        made = fake.ExternalProject(PROJECT).create_table(
            f"default.{name}", key_schema=[table.schema.field(k) for k in keys]
        )
        # Written as a stored version directly (the rows are generated in key order,
        # with unique keys), not through the fake's upsert, which builds a Python
        # tuple per key.
        ordered = fake._order([f for f in table.schema if f.name not in keys])
        table = table.select([*keys, *(f.name for f in ordered)])
        store.write(made.table_id, table)
        # Key columns are recorded as non-nullable, as the dataset declares them.
        recorded = arrow_utils.normalize_table_to_large_types(table).schema
        recorded = pa.schema([f.with_nullable(f.name not in keys) for f in recorded])
        fake.migrate(PROJECT, {name: recorded})


def _source(name: str) -> Any:
    from orcapod.core.sources import SpiralDBTableSource

    return SpiralDBTableSource(PROJECT, name, max_rows=None)


def _raw(name: str) -> pa.Table:
    store = fake._store()
    found = store.find(PROJECT, "default", name)
    assert found is not None
    return store.read(found["id"], 1)


def step_raw_copy() -> tuple[int, int, Any]:
    """The rows alone, copied into process memory: the floor every step stands on."""
    raw = _raw(GAZE)
    # ``take`` writes new buffers: the rows as private memory of this process.
    copy = raw.take(np.arange(raw.num_rows))
    return raw.nbytes, copy.nbytes, copy


def step_as_table() -> tuple[int, int, Any]:
    table = _source(GAZE).as_table()
    return _raw(GAZE).nbytes, table.nbytes, table


def step_as_table_all_info() -> tuple[int, int, Any]:
    table = _source(GAZE).as_table(all_info=True)
    groups: dict[str, int] = {"user": 0, "source_info": 0, "system_tags": 0, "other": 0}
    for name in table.column_names:
        size = table.column(name).nbytes
        if name.startswith("_source_"):
            groups["source_info"] += size
        elif name.startswith("_tag::"):
            groups["system_tags"] += size
        elif name.startswith("_"):
            groups["other"] += size
        else:
            groups["user"] += size
    print(json.dumps({"column_group_bytes": groups}), file=sys.stderr)
    return _raw(GAZE).nbytes, table.nbytes, table


def step_run() -> tuple[int, int, Any]:
    view = _source(GAZE).run()
    return _raw(GAZE).nbytes, 0, view


def step_run_then_as_table() -> tuple[int, int, Any]:
    source = _source(GAZE)
    view = source.run()
    table = source.as_table()
    return _raw(GAZE).nbytes, table.nbytes, (view, table)


def step_as_polars_df() -> tuple[int, int, Any]:
    frame = _source(GAZE).as_polars_df()
    return _raw(GAZE).nbytes, int(frame.estimated_size()), frame


def step_iterate() -> tuple[int, int, Any]:
    count = 0
    for _tag, _data in _source(GAZE):
        count += 1
    return _raw(GAZE).nbytes, 0, count


def step_iterate_read_one_value() -> tuple[int, int, Any]:
    total = 0.0
    for _tag, data in _source(GAZE):
        total += data["pupil_size_norm"]
    return _raw(GAZE).nbytes, 0, total


def step_flow() -> tuple[int, int, Any]:
    pairs = _source(GAZE).flow()
    return _raw(GAZE).nbytes, 0, pairs


def step_drop_nulls() -> tuple[int, int, Any]:
    table = _source(GAZE).drop_nulls(["eye_x_deg", "eye_y_deg"]).as_table()
    return _raw(GAZE).nbytes, table.nbytes, table


def step_polars_filter() -> tuple[int, int, Any]:
    import polars as pl

    table = _source(GAZE).polars_filter(pl.col("pupil_size_norm") > 0.5).as_table()
    return _raw(GAZE).nbytes, table.nbytes, table


def step_map_data() -> tuple[int, int, Any]:
    table = _source(GAZE).map_data({"pupil_size_norm": "pupil"}).as_table()
    return _raw(GAZE).nbytes, table.nbytes, table


def step_join() -> tuple[int, int, Any]:
    table = _source(SPIKES).join(_source(LOCATIONS)).as_table()
    return _raw(SPIKES).nbytes + _raw(LOCATIONS).nbytes, table.nbytes, table


def _grouped_gaze() -> Any:
    return (
        _source(GAZE)
        .drop_nulls(["eye_x_deg", "eye_y_deg"])
        .group_by(EXPERIMENT_KEY)
        .map_data({"timestamp": "gaze_time"})
    )


def step_group_by() -> tuple[int, int, Any]:
    table = _source(GAZE).group_by(EXPERIMENT_KEY).as_table()
    return _raw(GAZE).nbytes, table.nbytes, table


def step_group_by_chain() -> tuple[int, int, Any]:
    table = _grouped_gaze().as_table()
    return _raw(GAZE).nbytes, table.nbytes, table


def _onsets_pod() -> Any:
    from orcapod import function_pod

    @function_pod(output_keys="fixation_onset")
    def fixation_onsets(
        gaze_time: list[int], eye_x_deg: list[float], eye_y_deg: list[float]
    ) -> list[int]:
        t, x, y = np.asarray(gaze_time), np.asarray(eye_x_deg), np.asarray(eye_y_deg)
        speed = np.hypot(np.diff(x), np.diff(y)) / (np.diff(t) / 1_000_000)
        moving = speed > 40
        ends = np.flatnonzero(moving[:-1] & ~moving[1:]) + 1
        return [int(t[i]) for i in ends]

    return fixation_onsets


def step_function_pod() -> tuple[int, int, Any]:
    table = _onsets_pod().pod(_grouped_gaze()).as_table()
    return _raw(GAZE).nbytes, table.nbytes, table


def _job(store: Path) -> Any:
    from orcapod import PipelineJob
    from orcapod.databases import DeltaTableDatabase

    database = DeltaTableDatabase(str(store / "results"))
    with PipelineJob(name="fixation_response", database=database) as job:
        _onsets_pod().pod(_grouped_gaze(), label="onsets")
    return job


def step_job_run(store: Path) -> tuple[int, int, Any]:
    job = _job(store)
    job.run()
    return _raw(GAZE).nbytes, 0, job


def step_job_run_async(store: Path) -> tuple[int, int, Any]:
    from orcapod.pipeline import AsyncPipelineOrchestrator

    job = _job(store)
    job.run(orchestrator=AsyncPipelineOrchestrator())
    return _raw(GAZE).nbytes, 0, job


def _operator_chains() -> dict[str, Callable[[], Any]]:
    import polars as pl

    return {
        "map_data": lambda: _source(GAZE).map_data({"pupil_size_norm": "pupil"}),
        "drop_nulls": lambda: _source(GAZE).drop_nulls(["eye_x_deg", "eye_y_deg"]),
        "polars_filter": lambda: _source(GAZE).polars_filter(
            pl.col("pupil_size_norm") > 0.5
        ),
        "join": lambda: _source(SPIKES).join(_source(LOCATIONS)),
        "group_by": lambda: _source(GAZE).group_by(EXPERIMENT_KEY),
    }


def _operator_job(chain: str, orchestrator: str) -> Callable[[Path], Any]:
    """A job of one operator over its sources, run by the named orchestrator."""

    def step(store: Path) -> tuple[int, int, Any]:
        from orcapod import PipelineJob
        from orcapod.databases import DeltaTableDatabase
        from orcapod.pipeline import AsyncPipelineOrchestrator

        database = DeltaTableDatabase(str(store / f"results_{chain}_{orchestrator}"))
        with PipelineJob(name=f"only_{chain}", database=database) as job:
            _operator_chains()[chain]()
        if orchestrator == "async":
            job.run(orchestrator=AsyncPipelineOrchestrator())
        else:
            job.run()
        name = SPIKES if chain == "join" else GAZE
        return _raw(name).nbytes, 0, job

    return step


STEPS: dict[str, Callable[..., tuple[int, int, Any]]] = {
    **{
        f"job_{chain}_{orchestrator}": _operator_job(chain, orchestrator)
        for chain in ("map_data", "drop_nulls", "polars_filter", "join", "group_by")
        for orchestrator in ("sync", "async")
    },
    "raw_copy": step_raw_copy,
    "as_table": step_as_table,
    "as_table_all_info": step_as_table_all_info,
    "run": step_run,
    "run_then_as_table": step_run_then_as_table,
    "as_polars_df": step_as_polars_df,
    "iterate": step_iterate,
    "iterate_read_one_value": step_iterate_read_one_value,
    "flow": step_flow,
    "drop_nulls": step_drop_nulls,
    "polars_filter": step_polars_filter,
    "map_data": step_map_data,
    "join": step_join,
    "group_by": step_group_by,
    "group_by_chain": step_group_by_chain,
    "function_pod": step_function_pod,
    "job_run": step_job_run,
    "job_run_async": step_job_run_async,
}


def _peak_rss_bytes() -> int:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("step", choices=["setup", *STEPS])
    parser.add_argument("--store", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=2_177_155)
    parser.add_argument("--tracemalloc", action="store_true")
    args = parser.parse_args()
    warnings.simplefilter("ignore")

    with fake.install(args.store / "spiral"):
        fake.scan_batch_rows = 65_536
        if args.step == "setup":
            setup(args.rows)
            return
        import orcapod  # noqa: F401

        gc.collect()
        before = _peak_rss_bytes()
        pool = pa.default_memory_pool()
        if args.tracemalloc:
            tracemalloc.start(1)
        started = time.perf_counter()
        step = STEPS[args.step]
        took_store = args.step.startswith("job_")
        in_bytes, out_bytes, kept = step(args.store) if took_store else step()
        elapsed = time.perf_counter() - started
        record: dict[str, Any] = {
            "step": args.step,
            "rows": _raw(GAZE).num_rows,
            "input_arrow_bytes": in_bytes,
            "output_arrow_bytes": out_bytes,
            "rss_before_bytes": before,
            "peak_rss_bytes": _peak_rss_bytes(),
            "arrow_pool_peak_bytes": pool.max_memory(),
            "arrow_pool_held_bytes": pool.bytes_allocated(),
            "seconds": round(elapsed, 1),
        }
        if args.tracemalloc:
            held, peak = tracemalloc.get_traced_memory()
            top = tracemalloc.take_snapshot().statistics("lineno")[:8]
            record["python_held_bytes"] = held
            record["python_peak_bytes"] = peak
            record["python_top_lines"] = [
                f"{Path(s.traceback[0].filename).name}:{s.traceback[0].lineno} {s.size}"
                for s in top
            ]
        del kept
        print(json.dumps(record))


if __name__ == "__main__":
    main()
