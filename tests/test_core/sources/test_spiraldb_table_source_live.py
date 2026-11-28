"""SpiralDBTableSource against a live SpiralDB project (ITL-186). Opt-in.

Skipped unless ``ORCAPOD_SPIRAL_LIVE_PROJECT`` names a project that spiral-sdk's sign-in
can read, with a ``default.spike_event`` table of more than 1,000 rows (the workshop
project is ``edi-tutorial-v1-841226``, 63,349 rows); the tests that need that many skip
when the table holds fewer. Every test is read-only.

    ORCAPOD_SPIRAL_LIVE_PROJECT=edi-tutorial-v1-841226 \\
        uv run --extra spiraldb pytest tests/test_core/sources/test_spiraldb_table_source_live.py
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod import PipelineJob, col, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import SourceTooLargeError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import SpiralDBTableSource

PROJECT = os.environ.get("ORCAPOD_SPIRAL_LIVE_PROJECT", "")

pytestmark = pytest.mark.skipif(
    not PROJECT, reason="Set ORCAPOD_SPIRAL_LIVE_PROJECT to run the live Spiral tests"
)

# The rows ``default.spike_event`` must record for the tests that read or refuse many.
_MIN_ROWS = 1_000


def _require_a_large_table() -> None:
    """Skip the calling test unless ``spike_event`` records more than ``_MIN_ROWS`` rows."""
    recorded = SpiralDBTableSource(
        PROJECT, "spike_event", tag_only_columns=True
    ).row_count_estimate
    if recorded <= _MIN_ROWS:
        pytest.skip(f"spike_event records {recorded} rows; these tests need more")


def test_building_a_source_is_quick_and_names_the_table():
    """Building a source takes under 10 seconds and names the table and its id."""
    start = time.monotonic()
    source = SpiralDBTableSource(PROJECT, "spike_event", tag_only_columns=True)
    elapsed = time.monotonic() - start

    assert elapsed < 10
    assert source.source_id == (
        f"spiraldb_table_v1:{PROJECT}.default.spike_event@{source.table_id}"
    )
    assert source.table_id.startswith("table_")
    assert "unit_id" in source.keys()[0]
    assert source.row_count_estimate > 0


def test_limit_reads_the_first_rows():
    """``limit=1000`` reads 1,000 rows with distinct record ids."""
    _require_a_large_table()
    source = SpiralDBTableSource(
        PROJECT, "spike_event", limit=1000, tag_only_columns=True
    )

    table = source.as_table(all_info=True)

    assert table.num_rows == 1000
    (record_ids,) = [
        table.column(name)
        for name in table.column_names
        if name.startswith("_tag_record_id::")
    ]
    assert len(set(record_ids.to_pylist())) == 1000


def test_where_reads_only_matching_rows_with_the_same_record_ids():
    """``where`` reads only matching rows, each with its unfiltered record id."""
    first = SpiralDBTableSource(
        PROJECT, "spike_event", limit=1, tag_only_columns=True
    ).as_table(all_info=True)
    unit_id = first.column("unit_id")[0].as_py()
    source = SpiralDBTableSource(
        PROJECT,
        "spike_event",
        where={"unit_id": unit_id},
        max_rows=None,
        tag_only_columns=True,
    )

    table = source.as_table(all_info=True)

    assert table.num_rows > 0
    assert set(table.column("unit_id").to_pylist()) == {unit_id}
    record_id = next(n for n in table.column_names if n.startswith("_tag_record_id::"))
    assert first.column(record_id)[0].as_py() in table.column(record_id).to_pylist()


def test_a_whole_table_over_the_cap_is_refused_without_reading():
    """A table over ``max_rows`` is refused within 10 seconds, without a read."""
    _require_a_large_table()
    source = SpiralDBTableSource(
        PROJECT, "spike_event", max_rows=10, tag_only_columns=True
    )

    start = time.monotonic()
    with pytest.raises(SourceTooLargeError, match="limit="):
        source.as_table()
    assert time.monotonic() - start < 10


def test_an_async_job_over_a_limited_read(tmp_path: Path):
    """An async job computes a 500-row read once; a rerun computes nothing."""
    calls: list[float | None] = []

    @function_pod(output_keys="doubled")
    def double(spike_amplitude_uv: float | None) -> float | None:
        """Return twice ``spike_amplitude_uv``, recording the call."""
        calls.append(spike_amplitude_uv)
        return None if spike_amplitude_uv is None else 2 * spike_amplitude_uv

    def run() -> PipelineJob:
        """Build the job over a 500-row read and run it asynchronously."""
        job = PipelineJob(name="live", database=DeltaTableDatabase(tmp_path / "store"))
        with job:
            double.pod(
                SpiralDBTableSource(
                    PROJECT, "spike_event", limit=500, tag_only_columns=True
                ),
                label="double",
            )
        job.run(orchestrator=AsyncPipelineOrchestrator())
        return job

    assert run().nodes["double"].as_table().num_rows == 500
    assert 0 < len(calls) <= 500
    calls.clear()
    run()
    assert calls == []


def test_tags_as_data_gives_each_unit_its_spike_times_in_time_order():
    """Grouped on the unit key, each unit's copied spike times are in time order (ITL-996).

    ``timestamp`` is the only key column outside the unit key, so it alone orders the
    members of a group.
    """
    _require_a_large_table()
    spikes = SpiralDBTableSource(
        PROJECT,
        "spike_event",
        tags_as_data={"timestamp": "spike_time_us"},
        limit=20_000,
        tag_only_columns=True,
    )
    unit_key = [c for c in spikes.keys()[0] if c != "timestamp"]

    # GroupBy keeps only its keys, so ``timestamp`` is dropped already (ITL-1070).
    per_unit = spikes.group_by(unit_key)

    assert "spike_time_us" in per_unit.keys()[1]
    rows = per_unit.as_table().to_pylist()
    assert len(rows) > 1
    assert sum(len(r["spike_time_us"]) for r in rows) == spikes.as_table().num_rows
    for row in rows:
        assert row["spike_time_us"] == sorted(row["spike_time_us"])


def _one_unit() -> tuple[SpiralDBTableSource, list[Any]]:
    """Return a source on the first unit's rows, and that unit's sorted timestamps.

    The unit is every key column of the first row but ``timestamp`` (CodeRabbit on
    PR 574): ``unit_id`` alone can name units of several sessions or probes, whose
    timestamps the scan reads one group after another, not in one sorted run.
    """
    first = SpiralDBTableSource(PROJECT, "spike_event", limit=1, tag_only_columns=True)
    row = first.as_table().to_pylist()[0]
    unit_key = {c: row[c] for c in first.keys()[0] if c != "timestamp"}
    unit = SpiralDBTableSource(
        PROJECT, "spike_event", max_rows=None, tag_only_columns=True
    ).where(unit_key)
    times: list[Any] = sorted(
        t for t in unit.as_table().column("timestamp").to_pylist() if t is not None
    )
    if len(times) < 8:
        pytest.skip("the first unit holds too few rows for a window")
    return unit, times


def test_a_window_reads_the_rows_a_local_filter_keeps():
    """A window of ``timestamp`` (with the unit's equality) reads exactly the rows of
    the whole unit that pyarrow keeps, with their record ids, in either argument order
    (ITL-998)."""
    unit, times = _one_unit()
    start, stop = times[len(times) // 4], times[len(times) // 2]
    whole = unit.as_table(all_info=True)
    kind = whole.schema.field("timestamp").type
    expected = whole.filter(
        pc.and_(
            pc.greater_equal(whole["timestamp"], pa.scalar(start, kind)),
            pc.less(whole["timestamp"], pa.scalar(stop, kind)),
        )
    )

    for bounds in (
        (col("timestamp") >= start, col("timestamp") < stop),
        (col("timestamp") < stop, col("timestamp") >= start),
    ):
        window = unit.where(*bounds)
        assert window.as_table(all_info=True).equals(expected)
        assert window.content_hash() == unit.content_hash()
    assert expected.num_rows > 0


def test_a_window_with_a_limit_and_head_reads_its_first_rows():
    unit, times = _one_unit()
    window = unit.where(col("timestamp") >= times[2])

    first = window.as_table()["timestamp"].to_pylist()
    assert first == [t for t in times if t >= times[2]]
    assert (
        unit.where(col("timestamp") >= times[2])
        .head(3)
        .as_table()["timestamp"]
        .to_pylist()
        == first[:3]
    )
    limited = SpiralDBTableSource(
        PROJECT, "spike_event", limit=2, max_rows=None, tag_only_columns=True
    ).where(unit.condition, col("timestamp") >= times[2])
    assert limited.as_table()["timestamp"].to_pylist() == first[:2]


def test_an_empty_window_reads_no_row_with_the_full_schema():
    unit, times = _one_unit()
    empty = unit.where(col("timestamp") >= times[3], col("timestamp") < times[3])

    table = empty.as_table(all_info=True)
    assert table.num_rows == 0
    assert table.schema == unit.as_table(all_info=True).schema
