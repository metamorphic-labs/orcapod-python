"""An EDI-shaped pipeline on data-tags, end to end (ITL-1023).

Specification "Orcapod v0.4 — Referential Tags" (ITL-1004), section 15.7: two time series
(spikes, gaze) share five key columns and each has a key column ``timestamp`` (as the real
EDI tables do); a third table is keyed on a subset of the keys. Each ``timestamp`` is
renamed in both roles with ``map_tags`` (developer, November 12, 2026) so the join does not
equate them, both time series are grouped by the shared keys, the three are joined, and a
function reads the timestamp lists and a key column by name. A source's declared tags are
data-tags by default (developer, November 12, 2026), so the keys and timestamps are data
as well as tags, with no ``tags_as_data`` copy and no hand-written drop before the join. The
third table keeps its tags plain (``tag_only_columns``), so the join meets plain with
data-tag (section 9.2) and data-tag with data-tag (section 9.3).

``GroupBy`` keeps only its ``by`` tags (ITL-1070); a data-tag timestamp, not a key,
loses its tag role only and stays as a data list (developer, November 13, 2026). The
members of a group are ordered by it, so each timestamp list is in time order and the
function needs no sort.

The pipeline runs through a function node on the in-memory store and on Delta, with the
sync and the async orchestrator; a second run on the same store computes nothing.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from typing import Any
from pathlib import Path

import pytest

import orcapod as op
from orcapod.core.operators import Join
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

KEYS = ["lab", "subject", "session", "probe", "unit"]

SPIKES = [
    {
        "lab": "a",
        "subject": "m1",
        "session": "s1",
        "probe": "p0",
        "unit": unit,
        "timestamp": t,
        "amplitude": 0.5 + unit,
    }
    for unit in (1, 2)
    for t in ((30, 10, 20) if unit == 1 else (15, 5))
]

GAZE = [
    {
        "lab": "a",
        "subject": "m1",
        "session": "s1",
        "probe": "p0",
        "unit": unit,
        "timestamp": t,
        "gaze_x": 0.1 * t,
    }
    for unit in (1, 2)
    for t in (12, 2)
]

TRIALS = [{"lab": "a", "subject": "m1", "condition": "dark"}]

CALLS: list[int] = []


@op.function_pod(output_keys=["summary"])
def align(spike_time: list[int], gaze_time: list[int], unit: int) -> str:
    """Summarize one unit's spike and gaze timestamps; reads the key ``unit`` as data."""
    CALLS.append(unit)
    return f"unit {unit}: spikes {spike_time} gaze {gaze_time}"


def _streams() -> tuple[Any, Any, Any]:
    """Return the grouped spikes, the grouped gaze and the trials table."""
    spikes = op.sources.DictSource(
        SPIKES, tag_columns=[*KEYS, "timestamp"], label="spikes", source_id="spikes"
    )
    gaze = op.sources.DictSource(
        GAZE, tag_columns=[*KEYS, "timestamp"], label="gaze", source_id="gaze"
    )
    trials = op.sources.DictSource(
        TRIALS,
        tag_columns=["lab", "subject"],
        tag_only_columns=["lab", "subject"],
        label="trials",
        source_id="trials",
    )
    # A data-tag renamed with map_tags is renamed in both roles; grouping then
    # keeps it as a time-ordered data list.
    spikes_by_unit = spikes.map_tags({"timestamp": "spike_time"}).group_by(KEYS)
    gaze_by_unit = gaze.map_tags({"timestamp": "gaze_time"}).group_by(KEYS)
    return spikes_by_unit, gaze_by_unit, trials


def _build(job: PipelineJob) -> None:
    """Record the EDI-shaped pipeline in ``job``."""
    with job:
        # Automatic join and automatic select of the pod call (ITL-807).
        align.pod(*_streams(), label="align")


def test_the_join_merges_the_shared_keys_into_one_data_column_each() -> None:
    joined = Join()(*_streams())
    tags, data = joined.keys()
    # Data-tag with data-tag (the five keys) and plain with data-tag
    # (``lab`` and ``subject`` of the trials table): one data column each.
    for key in KEYS:
        assert key in tags and data.count(key) == 1
    assert {"spike_time", "gaze_time"} <= set(data)
    assert not {"spike_time", "gaze_time"} & set(tags)
    assert joined.as_table().column_names.count("unit") == 1
    rows = {tag["unit"]: data for tag, data in joined.iter_data()}
    # The members are ordered by the timestamp, so the lists are in time order.
    assert rows[1]["spike_time"] == [10, 20, 30]
    assert rows[1]["gaze_time"] == [2, 12]
    # The other data lists follow the same member order.
    assert rows[1]["gaze_x"] == [0.1 * 2, 0.1 * 12]
    assert rows[2]["condition"] == "dark"


def _in_memory(_: Path) -> ArrowDatabaseProtocol:
    return InMemoryArrowDatabase()


def _delta(tmp_path: Path) -> ArrowDatabaseProtocol:
    return DeltaTableDatabase(tmp_path / "store")


@pytest.mark.parametrize("make_store", [_in_memory, _delta], ids=["memory", "delta"])
@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
def test_edi_shaped_pipeline_runs_and_second_run_computes_nothing(
    tmp_path: Path,
    make_store: Callable[[Path], ArrowDatabaseProtocol],
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    store = make_store(tmp_path)
    CALLS.clear()
    with warnings.catch_warnings():
        # The pod call's automatic select materializes the keys it drops (section 7).
        warnings.simplefilter("ignore")
        job = PipelineJob(database=store)
        _build(job)
        job.run(orchestrator=orchestrator())

    assert sorted(CALLS) == [1, 2]
    node = job.nodes["align"]
    tag_keys, data_keys = node.keys()
    # The pod call lets the input's data-tags fall through (developer decision,
    # November 12, 2026): every key stays a data-tag, beside the function's output.
    # Plain data (the timestamp lists, ``condition`` of the trials table) does not.
    for name in KEYS:
        assert name in tag_keys and name in data_keys
    assert "summary" in data_keys
    assert not {"spike_time", "gaze_time", "condition"} & set(data_keys)
    table = node.as_table()
    # A data-tag is stored and shown once.
    assert table.column_names.count("unit") == 1
    for tag, data in node.iter_data():
        assert tag["unit"] == data["unit"]
    rows = sorted(table.to_pylist(), key=lambda r: r["unit"])
    assert [r["summary"] for r in rows] == [
        "unit 1: spikes [10, 20, 30] gaze [2, 12]",
        "unit 2: spikes [5, 15] gaze [2, 12]",
    ]
    assert node.head().table.column_names.count("unit") == 1

    # Second run on the same store: everything is found, nothing is computed.
    CALLS.clear()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        job2 = PipelineJob(database=store)
        _build(job2)
        job2.run(orchestrator=orchestrator())
    assert CALLS == []
    rows2 = sorted(job2.nodes["align"].as_table().to_pylist(), key=lambda r: r["unit"])
    assert [r["summary"] for r in rows2] == [r["summary"] for r in rows]
