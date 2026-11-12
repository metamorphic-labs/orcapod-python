"""A job node's ``head()`` shows its in-memory or stored rows and never runs it (ITL-991).

A node is associated with its storage (developer, November 12, 2026): its preview is the
first rows of what it holds after a run, or of its stored table, and showing it never
computes. These tests run jobs on both orchestrators and both stores, and load saved jobs.
"""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.nodes import OperatorJobNode
from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
from orcapod.core.operators import SelectDataColumns
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.streams import HeadStatus
from orcapod.types import CacheMode, Schema

CALLS: list[int] = []

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}


@function_pod(output_keys="y")
def triple(x: int) -> int:
    """Triple ``x``, counting every call."""
    CALLS.append(x)
    return 3 * x


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    CALLS.clear()


def _source(rows: int = 25) -> ArrowTableSource:
    table = pa.table(
        {"id": list(range(rows)), "x": list(range(rows))},
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id"], source_id="nums", label="nums")


def _store(kind: str, tmp_path: Path) -> ArrowDatabaseProtocol:
    if kind == "in_memory":
        return InMemoryArrowDatabase()
    return DeltaTableDatabase(base_path=tmp_path / "store")


def _job(store: ArrowDatabaseProtocol) -> PipelineJob:
    job = PipelineJob(name="itl991", store=store)
    with job:
        tripled = triple.pod(_source(), label="triple")
        _ = SelectDataColumns(["y"])(tripled, label="sel")
    return job


def test_nodes_before_a_run_show_no_rows_and_compute_nothing() -> None:
    job = _job(InMemoryArrowDatabase())
    fn_head = job.nodes["triple"].head()
    op_head = job.nodes["sel"].head()
    _ = job.nodes["triple"]._repr_html_()
    _ = job.nodes["sel"]._repr_html_()
    assert CALLS == []
    assert fn_head.status is HeadStatus.NOTHING_STORED
    assert fn_head.table.column_names == ["id", "y"]
    assert op_head.status is HeadStatus.NOT_COMPUTED
    assert "cache mode OFF" in op_head.header()


@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_nodes_after_a_run_show_their_first_rows(
    orchestrator: str, store_kind: str, tmp_path: Path
) -> None:
    store = _store(store_kind, tmp_path)
    job = _job(store)
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert len(CALLS) == 25
    for label in ("triple", "sel"):
        node = job.nodes[label]
        # Right after a run the rows are in memory: the store is not read at all.
        with mock.patch.object(
            type(store), "get_all_records", side_effect=AssertionError("store read")
        ):
            head = node.head()
        assert head.status is HeadStatus.ROWS, label
        assert head.total_rows == 25
        assert head.table.equals(node.as_table().slice(0, 10)), label
        assert "first 10 of 25" in node._repr_html_()
    assert len(CALLS) == 25


@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
def test_a_new_job_over_a_filled_store_reads_stored_rows_without_computing(
    store_kind: str, tmp_path: Path
) -> None:
    store = _store(store_kind, tmp_path)
    _ = _job(store).run()
    CALLS.clear()

    node = _job(store).nodes["triple"]
    head = node.head(4)
    assert CALLS == []
    assert head.status is HeadStatus.ROWS
    # The whole stored slice is read, so the header states the exact count and
    # makes no claim of a bounded read.
    assert head.header() == "FunctionJobNode[triple] — first 4 of 25 stored rows"
    assert head.table.equals(node.as_table().slice(0, 4))


def test_a_loaded_job_shows_stored_rows_and_never_raises(tmp_path: Path) -> None:
    store = DeltaTableDatabase(base_path=tmp_path / "store")
    job = _job(store)
    _ = job.run()
    saved = tmp_path / "job.json"
    job.save(saved)
    CALLS.clear()

    loaded = PipelineJob.load(str(saved), store=store)
    fn_head = loaded.nodes["triple"].head()
    op_head = loaded.nodes["sel"].head()
    assert CALLS == []
    assert fn_head.status is HeadStatus.ROWS
    assert fn_head.header().endswith("first 10 of 25 stored rows")
    assert op_head.status is HeadStatus.UNAVAILABLE
    assert "cache mode OFF" in op_head.header()


def test_blueprint_nodes_are_unavailable() -> None:
    job = _job(InMemoryArrowDatabase())
    for label in ("triple", "sel"):
        blueprint = job.nodes[label].as_node()
        head = blueprint.head()
        assert head.status is HeadStatus.UNAVAILABLE
        assert "pipeline blueprint" in blueprint._repr_html_()


def test_source_nodes() -> None:
    slot = SourceNode(
        name="nums", tag_schema=Schema({"id": int}), data_schema=Schema({"x": int})
    )
    assert slot.head().status is HeadStatus.UNAVAILABLE
    assert "input slot" in slot._repr_html_()
    unbound = SourceJobNode(
        name="nums", tag_schema=Schema({"id": int}), data_schema=Schema({"x": int})
    )
    assert unbound.head().status is HeadStatus.UNAVAILABLE
    assert "no source bound" in unbound.head().header()
    source = _source()
    bound = SourceJobNode(name="nums", bound_source=source)
    assert bound.head().table.equals(source.head().table)
    assert bound.head().header() == "SourceJobNode[nums] — first 10 of 25 rows"


def _operator_node(
    cache_mode: CacheMode, store: InMemoryArrowDatabase
) -> OperatorJobNode:
    return OperatorJobNode(
        operator=SelectDataColumns(["x"]),
        input_streams=[_source()],
        pipeline_database=store,
        cache_mode=cache_mode,
    )


def test_operator_node_cache_modes() -> None:
    store = InMemoryArrowDatabase()
    assert _operator_node(CacheMode.REPLAY, store).head().status is (
        HeadStatus.NOTHING_STORED
    )
    seed = _operator_node(CacheMode.LOG, store)
    _ = seed.execute(_source())

    replay = _operator_node(CacheMode.REPLAY, store)
    head = replay.head(3)
    assert head.status is HeadStatus.ROWS
    assert (
        head.header()
        == "OperatorJobNode[SelectDataColumns] — first 3 of 25 stored rows"
    )
    assert head.table.equals(replay.as_table().slice(0, 3))

    log = _operator_node(CacheMode.LOG, store)
    log_head = log.head()
    assert log_head.status is HeadStatus.NOT_COMPUTED
    assert "not replayed" in log_head.header()
