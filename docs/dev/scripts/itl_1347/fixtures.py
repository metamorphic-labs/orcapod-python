"""ITL-1347 fixture pipelines, built on the tutorial dataset (``docs/tutorial/workshop_data.py``).

Every fixture is a function ``build(data_dir, database) -> PipelineJob`` that records a job
over the dataset written by ``make_dataset(data_dir)``. The fixtures use only the public
API, so the same file builds the same job on pinned ``main`` and on a stage branch: the
differential (``dump.py``, ``compare.py``) runs it under both checkouts.

- ``tutorial``: the pipeline of ``docs/tutorial/05-pipeline-job.md`` (two sources, a join,
  a function pod per unit, a group-by, a function pod per session).
- ``spikes``: the workshop's shape at a larger scale. Every spike of the dataset is one row
  (tens of thousands), joined to its unit, binned by a function pod per spike, grouped per
  unit and counted by a second function pod, with ``Batch``, ``MergeJoin``, ``SemiJoin``,
  ``PolarsFilter``, ``AddTags``, ``DropNulls``, ``MapData`` and ``Rejoin`` beside it, and a
  side effect at the end. Every operator of the stage-5 classification appears once.
"""

from __future__ import annotations

import csv
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa


def _tutorial_sources(data_dir: Path) -> tuple[Any, Any]:
    from orcapod.sources import CSVSource

    sessions = CSVSource(
        str(data_dir / "sessions.csv"),
        tag_columns=["subject_id", "session"],
        tag_only_columns=True,
        source_id="sessions",
    )
    units = CSVSource(
        str(data_dir / "units.csv"),
        tag_columns=["subject_id", "session", "probe", "unit"],
        tag_only_columns=True,
        source_id="units",
    )
    return sessions, units


def build_tutorial(data_dir: Path, database: Any) -> Any:
    """The tutorial's pipeline job (``05-pipeline-job.md``)."""
    from orcapod import PipelineJob, function_pod

    sessions, units = _tutorial_sources(data_dir)

    @function_pod(output_keys="rate_hz")
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        return n_spikes / duration_s

    @function_pod(output_keys="mean_rate_hz")
    def mean_rate(rate_hz: list[float]) -> float:
        return sum(rate_hz) / len(rate_hz)

    job = PipelineJob(name="itl1347_tutorial", database=database)
    with job:
        rates: Any = firing_rate.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
            label="rates",
        )
        mean_rate.pod(
            rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"]),
            label="session_means",
        )
    return job


def spikes_table(data_dir: Path) -> pa.Table:
    """Every spike of the dataset as one row, in a fixed order (file name, then line).

    ``ITL1347_SPIKES_MAX`` keeps only the first rows, for a quick check of the harness.
    """
    columns: dict[str, list[Any]] = {
        "subject_id": [],
        "session": [],
        "probe": [],
        "spike": [],
        "unit": [],
        "time_s": [],
    }
    for session_dir in sorted((data_dir / "sessions").iterdir()):
        subject_id, session = session_dir.name.rsplit("_", 1)
        for path in sorted(session_dir.glob("spikes_*.csv")):
            probe = path.stem.removeprefix("spikes_")
            with path.open(newline="", encoding="utf-8") as handle:
                for index, row in enumerate(csv.DictReader(handle)):
                    columns["subject_id"].append(subject_id)
                    columns["session"].append(session)
                    columns["probe"].append(probe)
                    columns["spike"].append(index)
                    columns["unit"].append(int(row["unit"]))
                    columns["time_s"].append(float(row["time_s"]))
    limit = os.environ.get("ITL1347_SPIKES_MAX")
    if limit is not None:
        columns = {name: values[: int(limit)] for name, values in columns.items()}
    types = {
        "subject_id": pa.large_string(),
        "session": pa.large_string(),
        "probe": pa.large_string(),
        "spike": pa.int64(),
        "unit": pa.int64(),
        "time_s": pa.float64(),
    }
    schema = pa.schema([pa.field(name, t, nullable=False) for name, t in types.items()])
    return pa.Table.from_arrays(
        [pa.array(columns[name], types[name]) for name in types], schema=schema
    )


def build_spikes(data_dir: Path, database: Any) -> Any:
    """The workshop's shape at scale: one row per spike, every operator kind once."""
    from orcapod import InvocationContext, PipelineJob, function_pod, tap_pod
    from orcapod.operators import MergeJoin, Rejoin
    from orcapod.sources import ArrowTableSource

    sessions, units = _tutorial_sources(data_dir)
    spikes = ArrowTableSource(
        spikes_table(data_dir),
        tag_columns=["subject_id", "session", "probe", "unit", "spike"],
        tag_only_columns=True,
        source_id="spikes",
    )

    @function_pod(output_keys="bin")
    def time_bin(time_s: float) -> int:
        return int(time_s // 10.0)

    @function_pod(output_keys="n_bins")
    def bins_hit(bin: list[int]) -> int:
        return len(set(bin))

    @tap_pod
    def report_fn(n_bins: int, ctx: InvocationContext) -> None:
        return None

    job = PipelineJob(name="itl1347_spikes", database=database)
    with job:
        located = spikes.join(units.select_data_columns(["region"]), label="located")
        binned: Any = time_bin.pod(
            located.select_data_columns(["time_s"]), label="binned"
        )
        per_unit = binned.group_by(["subject_id", "session", "probe", "unit"])
        counted: Any = bins_hit.pod(
            per_unit.select_data_columns(["bin"]), label="counted"
        )
        report: Any = report_fn
        report(counted, label="report")
        long_sessions = sessions.polars_filter(duration_s=120.0, label="long_sessions")
        units.semi_join(long_sessions, label="units_in_long_sessions")
        units.select_data_columns(["n_spikes"]).batch(batch_size=4, label="batched")
        merged = MergeJoin()(
            units.select_data_columns(["n_spikes"]),
            counted.map_data({"n_bins": "n_spikes"}),
        )
        merged.add_tags(study="itl1347", label="tagged").drop_nulls(label="no_nulls")
        Rejoin()(
            units.select_data_columns(["region"]),
            units.select_data_columns(["depth_um"]),
        )
    return job


FIXTURES: dict[str, Callable[[Path, Any], Any]] = {
    "tutorial": build_tutorial,
    "spikes": build_spikes,
}
