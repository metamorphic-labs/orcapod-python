"""Sync and async runs store the same pipeline records after a select or drop (ITL-1040).

The async paths of ``SelectDataColumns`` and ``DropDataColumns`` drop columns datagram by
datagram. Before ITL-1040 the dropped columns' source info survived there, so an async run
stored an extra ``_source_<dropped column>`` in the function node's records that a sync run
did not: the stored table's schema depended on the orchestrator.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.function_node import (
    PIPELINE_RECORDED_AT_COL,
    PIPELINE_RUN_ID_COL,
)
from orcapod.core.operators import DropDataColumns, SelectDataColumns
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import ArrowTableSource
from tests.pipeline_stores import STORE_KINDS, function_store_cases, open_stores

# Columns that differ between two runs by design: the output datagram's uuid, the token
# that carries it, the result's timestamp, and the entry's recording run and time.
PER_RUN_COLUMNS = (
    "__data_id",
    "_source_y",
    "__pod_ts",
    PIPELINE_RUN_ID_COL,
    PIPELINE_RECORDED_AT_COL,
)


@function_pod(output_keys="y")
def times_ten(x: int) -> int:
    """Return ``10 * x``."""
    return 10 * x


def source(reordered: bool = False) -> ArrowTableSource:
    """Tags ``k`` and ``session``; data ``x`` (the pod's input) and ``s``, ``m`` (not its input)."""
    table = pa.table(
        {
            "k": ["a", "b", "c"],
            "session": [2, 1, 2],
            "x": [1, 2, 3],
            "s": ["p", "q", "r"],
            "m": [0.5, 1.5, 2.5],
        },
        schema=pa.schema(
            [
                pa.field("session", pa.int64(), nullable=False),
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
                pa.field("s", pa.large_string(), nullable=False),
                pa.field("m", pa.float64(), nullable=False),
            ]
        ),
    )
    if reordered:
        table = table.select(["m", "s", "k", "x", "session"])
    return ArrowTableSource(
        table,
        tag_columns=["session", "k"] if reordered else ["k", "session"],
        source_id="src",
        tag_only_columns=True,
    )


def automatic_select(src: ArrowTableSource) -> Any:
    """``pod(src)`` selects the pod's input ``x`` itself."""
    return times_ten.pod(src, label="node")


def explicit_select(src: ArrowTableSource) -> Any:
    """``SelectDataColumns(["x"])`` before the pod."""
    return times_ten.pod(SelectDataColumns(["x"])(src), label="node")


def explicit_drop(src: ArrowTableSource) -> Any:
    """``DropDataColumns(["s", "m"])`` before the pod."""
    return times_ten.pod(DropDataColumns(["s", "m"])(src), label="node")


PIPELINES = {
    "automatic_select": automatic_select,
    "explicit_select": explicit_select,
    "explicit_drop": explicit_drop,
}


def stored_records(
    pipeline: str, orchestrator: str, store: ArrowDatabaseProtocol, reordered: bool
) -> pa.Table:
    """The function node's stored records after a run, rows in tag order."""
    job = PipelineJob(name="itl1040", database=store)
    with job:
        PIPELINES[pipeline](source(reordered))
    job.run(
        orchestrator=SyncPipelineOrchestrator()
        if orchestrator == "sync"
        else AsyncPipelineOrchestrator()
    )
    records = job.nodes["node"].get_all_records(all_info=True)
    assert records is not None
    return records.sort_by("k")


@pytest.mark.parametrize("reordered", [False, True], ids=["mixed", "reordered"])
@pytest.mark.parametrize("pipeline", list(PIPELINES))
@function_store_cases
def test_async_stores_the_records_a_sync_run_stores(
    pipeline: str,
    pipeline_store_factory: Callable[[str], ArrowDatabaseProtocol],
    reordered: bool,
    tmp_path: Path,
) -> None:
    sync = stored_records(pipeline, "sync", pipeline_store_factory("sync"), reordered)
    async_ = stored_records(
        pipeline, "async", pipeline_store_factory("async"), reordered
    )

    assert async_.schema == sync.schema
    assert "_source_s" not in async_.column_names
    assert "_source_m" not in async_.column_names
    stable_sync = sync.drop_columns(list(PER_RUN_COLUMNS))
    stable_async = async_.drop_columns(list(PER_RUN_COLUMNS))
    assert {
        json.dumps(row, sort_keys=True, default=repr)
        for row in stable_async.to_pylist()
    } == {
        json.dumps(row, sort_keys=True, default=repr) for row in stable_sync.to_pylist()
    }

    with open_stores("delta", tmp_path / "reference") as reference:
        delta_sync = stored_records(pipeline, "sync", reference("sync"), reordered)
    assert stable_sync.equals(delta_sync.drop_columns(list(PER_RUN_COLUMNS)))


@pytest.mark.parametrize("pipeline_store_factory", STORE_KINDS, indirect=True)
@pytest.mark.parametrize("pipeline", ["explicit_select", "explicit_drop"])
def test_the_operator_node_table_is_the_same_on_both_orchestrators(
    pipeline: str, pipeline_store_factory: Callable[[str], ArrowDatabaseProtocol]
) -> None:
    """The select or drop node itself shows the same columns after either run."""
    tables = []
    for orchestrator in (SyncPipelineOrchestrator, AsyncPipelineOrchestrator):
        job = PipelineJob(
            name="itl1040", database=pipeline_store_factory(orchestrator.__name__)
        )
        with job:
            src = source()
            if pipeline == "explicit_select":
                SelectDataColumns(["x"])(src, label="op")
            else:
                DropDataColumns(["s", "m"])(src, label="op")
        job.run(orchestrator=orchestrator())
        tables.append(job.nodes["op"].as_table(all_info=True).sort_by("k"))
    assert tables[1].schema == tables[0].schema
    assert {tuple(row.values()) for row in tables[1].to_pylist()} == {
        tuple(row.values()) for row in tables[0].to_pylist()
    }
