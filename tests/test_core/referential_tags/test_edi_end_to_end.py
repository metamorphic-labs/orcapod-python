"""An EDI-shaped pipeline on referential tags, end to end (ITL-1023).

Specification "Orcapod v0.4 — Referential Tags" (ITL-1004), section 15.7: two time series
(spikes, gaze) share five key columns and each has a column ``timestamp``; a third table is
keyed on a subset of the keys. Both time series are grouped by the shared keys, each
``timestamp`` list is renamed with ``map_data`` so the join does not collide on it, the
three are joined, and a function reads the timestamp lists and a key column by name. A
source's declared tags are referential by default (developer, November 12, 2026), so the
keys are data as well as tags, with no ``tags_as_data`` copy and no hand-written drop before
the join. The third table keeps its tags plain (``tag_only_columns``), so the join meets
plain with referential (section 9.2) and referential with referential (section 9.3).

``GroupBy`` keeps only its ``by`` tags (ITL-1070), so a ``timestamp`` declared as a tag
would be dropped by the grouping: it is declared as data at the source, and the grouping
makes it a list. Its members are then ordered by record id, not by time, so the function
sorts them.

The pipeline runs through a function node on the in-memory store and on Delta, with the
sync and the async orchestrator; a second run on the same store computes nothing.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from typing import Any, cast
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
    return f"unit {unit}: spikes {sorted(spike_time)} gaze {sorted(gaze_time)}"


def _streams() -> tuple[Any, Any, Any]:
    """Return the grouped spikes, the grouped gaze and the trials table."""
    spikes = op.sources.DictSource(SPIKES, tag_columns=KEYS, source_id="spikes")
    gaze = op.sources.DictSource(GAZE, tag_columns=KEYS, source_id="gaze")
    trials = op.sources.DictSource(
        TRIALS,
        tag_columns=["lab", "subject"],
        tag_only_columns=["lab", "subject"],
        source_id="trials",
    )
    # ``timestamp`` is data: renamed with map_data so the join does not collide on it.
    spikes_by_unit = spikes.group_by(KEYS).map_data({"timestamp": "spike_time"})
    gaze_by_unit = gaze.group_by(KEYS).map_data({"timestamp": "gaze_time"})
    return spikes_by_unit, gaze_by_unit, trials


def _build(job: PipelineJob) -> None:
    """Record the EDI-shaped pipeline in ``job``."""
    with job:
        # Automatic join and automatic select of the pod call (ITL-807).
        align.pod(*_streams(), label="align")


def test_the_join_merges_the_shared_keys_into_one_data_column_each() -> None:
    joined = Join()(*_streams())
    tags, data = joined.keys()
    # Referential with referential (the five keys) and plain with referential
    # (``lab`` and ``subject`` of the trials table): one data column each.
    for key in KEYS:
        assert key in tags and data.count(key) == 1
    assert {"spike_time", "gaze_time"} <= set(data)
    assert not {"spike_time", "gaze_time"} & set(tags)
    assert joined.as_table().column_names.count("unit") == 1
    rows = {tag["unit"]: data for tag, data in joined.iter_data()}
    # The timestamps are data, ordered by record id: compared as sets of values.
    assert set(cast("list[int]", rows[1]["spike_time"])) == {10, 20, 30}
    assert set(cast("list[int]", rows[1]["gaze_time"])) == {2, 12}
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
        job = PipelineJob(store=store)
        _build(job)
        job.run(orchestrator=orchestrator())

    assert sorted(CALLS) == [1, 2]
    node = job.nodes["align"]
    tag_keys, data_keys = node.keys()
    # The pod call lets the input's referential tags fall through (developer decision,
    # November 12, 2026): every key stays referential, beside the function's output.
    # Plain data (the timestamp lists, ``condition`` of the trials table) does not.
    for name in KEYS:
        assert name in tag_keys and name in data_keys
    assert "summary" in data_keys
    assert not {"spike_time", "gaze_time", "condition"} & set(data_keys)
    table = node.as_table()
    # A referential tag is stored and shown once.
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
        job2 = PipelineJob(store=store)
        _build(job2)
        job2.run(orchestrator=orchestrator())
    assert CALLS == []
    rows2 = sorted(job2.nodes["align"].as_table().to_pylist(), key=lambda r: r["unit"])
    assert [r["summary"] for r in rows2] == [r["summary"] for r in rows]
