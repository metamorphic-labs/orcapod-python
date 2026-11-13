"""An EDI-shaped pipeline on referential tags, end to end (ITL-1023).

Specification "Orcapod v0.4 — Referential Tags" (ITL-1004), section 15.7: two time series
(spikes, gaze) share five key columns and each has its own timestamp; a third table is keyed
on a subset of the keys. Both time series are grouped by the shared keys, the three are
joined, and a function reads the timestamp lists and a key column by name. A source's
declared tags are referential by default (developer, November 12, 2026), so the keys and
timestamps are data as well as tags, with no ``tags_as_data`` copy and no hand-written drop
before the join. The third table keeps its tags plain (``tag_only_columns``), so the join
meets plain with referential (section 9.2) and referential with referential (section 9.3).

The pipeline runs through a function node on the in-memory store and on Delta, with the
sync and the async orchestrator; a second run on the same store computes nothing.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from pathlib import Path

import pytest

import orcapod as op
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
        "spike_time": t,
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
        "gaze_time": t,
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


def _build(job: PipelineJob) -> None:
    """Record the EDI-shaped pipeline in ``job``."""
    spikes = op.sources.DictSource(
        SPIKES, tag_columns=[*KEYS, "spike_time"], source_id="spikes"
    )
    gaze = op.sources.DictSource(
        GAZE, tag_columns=[*KEYS, "gaze_time"], source_id="gaze"
    )
    trials = op.sources.DictSource(
        TRIALS,
        tag_columns=["lab", "subject"],
        tag_only_columns=["lab", "subject"],
        source_id="trials",
    )
    with job:
        spikes_by_unit = spikes.group_by(KEYS)
        gaze_by_unit = gaze.group_by(KEYS)
        # Automatic join and automatic select of the pod call (ITL-807).
        align.pod(spikes_by_unit, gaze_by_unit, trials, label="align")


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
    # A pod's output data is the function's outputs: at the pod boundary every
    # referential tag becomes a plain tag with the same value (developer, November 12,
    # 2026). ``unit`` was read by the function; ``session`` was dropped by the automatic
    # select (section 7). Both are plain tags of the output.
    assert set(KEYS) <= set(tag_keys)
    assert data_keys == ("summary",)
    table = node.as_table()
    assert table.column_names.count("unit") == 1
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
