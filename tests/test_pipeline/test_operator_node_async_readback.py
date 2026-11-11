"""After a job run, an operator node reads back the same rows on both orchestrators (ITL-896).

``job.nodes[label]`` is a stream: ``as_table()`` and ``iter_data()`` must return the rows the
operator produced, whether the job ran on ``SyncPipelineOrchestrator`` or
``AsyncPipelineOrchestrator``, with either store and either ``materialize_results`` setting.
An operator node in a job stores nothing (``cache_mode`` is ``OFF``), so ``get_all_records()``
returns ``None`` on both.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.core.operators import (
    Batch,
    DropDataColumns,
    DropTagColumns,
    GroupBy,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    PolarsFilter,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod import function_pod
from orcapod.core.nodes import OperatorJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}


def _source_a() -> ArrowTableSource:
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int8()),
            "grp": pa.array(["g1", "g1", "g2"], type=pa.large_string()),
            "x": pa.array([10, 20, 30], type=pa.int8()),
            "s": pa.array(["p", None, "r"], type=pa.string()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int8(), nullable=False),
                pa.field("grp", pa.large_string(), nullable=False),
                pa.field("x", pa.int8(), nullable=False),
                pa.field("s", pa.string(), nullable=True),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id", "grp"], source_id="a")


def _source_b() -> ArrowTableSource:
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int8()),
            "z": pa.array([5, 6, 7], type=pa.int16()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int8(), nullable=False),
                pa.field("z", pa.int16(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id"], source_id="b")


def _source_c() -> ArrowTableSource:
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int8()),
            "grp": pa.array(["g1", "g1", "g2"], type=pa.large_string()),
            "x": pa.array([11, 21, 31], type=pa.int8()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int8(), nullable=False),
                pa.field("grp", pa.large_string(), nullable=False),
                pa.field("x", pa.int8(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id", "grp"], source_id="c")


@function_pod(output_keys="x2")
def double_x(x: int) -> int:
    return x * 2


@dataclass(frozen=True)
class Case:
    """An operator applied inside a job; the operator node under test is labelled ``op``."""

    name: str
    build: Callable[[StreamProtocol, StreamProtocol, StreamProtocol], object]
    expected_rows: int


CASES = [
    Case(
        "select_data_columns",
        lambda a, b, c: SelectDataColumns(["x"])(a, label="op"),
        3,
    ),
    Case("drop_data_columns", lambda a, b, c: DropDataColumns(["s"])(a, label="op"), 3),
    Case(
        "select_tag_columns",
        lambda a, b, c: SelectTagColumns(["id"])(a, label="op"),
        3,
    ),
    Case("drop_tag_columns", lambda a, b, c: DropTagColumns(["grp"])(a, label="op"), 3),
    Case("map_data", lambda a, b, c: MapData({"x": "w"})(a, label="op"), 3),
    Case("map_tags", lambda a, b, c: MapTags({"grp": "group"})(a, label="op"), 3),
    Case(
        "polars_filter",
        lambda a, b, c: PolarsFilter(constraints={"grp": "g1"})(a, label="op"),
        2,
    ),
    Case("join", lambda a, b, c: Join()(a, b, label="op"), 3),
    Case("merge_join", lambda a, b, c: MergeJoin()(a, c, label="op"), 3),
    Case("semi_join", lambda a, b, c: SemiJoin()(a, b, label="op"), 3),
    Case("batch", lambda a, b, c: Batch(batch_size=2)(a, label="op"), 2),
    Case("group_by", lambda a, b, c: GroupBy(by=["grp"])(a, label="op"), 2),
    Case(
        "operator_feeding_function_pod",
        lambda a, b, c: double_x.pod(
            SelectDataColumns(["x"])(a, label="op"), label="double"
        ),
        3,
    ),
]


def _make_store(kind: str, tmp_path: Path) -> ArrowDatabaseProtocol:
    if kind == "in_memory":
        return InMemoryArrowDatabase()
    return DeltaTableDatabase(base_path=tmp_path / "store")


def _run_job(
    case: Case,
    orchestrator: str,
    store_kind: str,
    tmp_path: Path,
    materialize_results: bool = True,
) -> PipelineJob:
    job = PipelineJob(
        name="itl896", store=_make_store(store_kind, tmp_path / orchestrator)
    )
    with job:
        _ = case.build(_source_a(), _source_b(), _source_c())
    _ = job.run(
        orchestrator=ORCHESTRATORS[orchestrator](),
        materialize_results=materialize_results,
    )
    return job


def _op_node(job: PipelineJob) -> OperatorJobNode:
    node: object = job.nodes["op"]
    assert isinstance(node, OperatorJobNode)
    return node


def _table_rows(table: pa.Table) -> list[dict[str, object]]:
    """Rows of ``table`` in a canonical order, so arrival order does not matter."""
    return sorted(table.to_pylist(), key=repr)


def _row_hashes(node: OperatorJobNode) -> list[tuple[str, str]]:
    return sorted(
        (tag.content_hash().to_string(), data.content_hash().to_string())
        for tag, data in node.iter_data()
    )


@pytest.mark.parametrize("materialize_results", [True, False])
@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
def test_operator_node_reads_back_same_rows_after_async_run(
    case: Case, store_kind: str, materialize_results: bool, tmp_path: Path
) -> None:
    sync_node = _op_node(
        _run_job(case, "sync", store_kind, tmp_path, materialize_results)
    )
    async_node = _op_node(
        _run_job(case, "async", store_kind, tmp_path, materialize_results)
    )

    sync_table = sync_node.as_table(all_info=True)
    async_table = async_node.as_table(all_info=True)

    assert sync_table.num_rows == case.expected_rows
    assert async_table.num_rows == case.expected_rows
    # Types, nullability and column order, including system tags and source info.
    assert async_table.schema == sync_table.schema
    assert _table_rows(async_table) == _table_rows(sync_table)
    assert _table_rows(async_node.as_table()) == _table_rows(sync_node.as_table())

    assert len(list(async_node.iter_data())) == case.expected_rows
    assert _row_hashes(async_node) == _row_hashes(sync_node)


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_operator_node_stores_nothing_so_get_all_records_is_none(
    orchestrator: str, tmp_path: Path
) -> None:
    node = _op_node(_run_job(CASES[0], orchestrator, "in_memory", tmp_path))

    assert node.as_table().num_rows == 3
    assert node.get_all_records() is None


NO_MATCH = Case(
    "polars_filter_no_match",
    lambda a, b, c: PolarsFilter(constraints={"grp": "none"})(a, label="op"),
    0,
)

MERGE_JOIN_AFTER_OPERATOR = Case(
    "merge_join_after_operator",
    lambda a, b, c: MergeJoin()(SelectDataColumns(["x"])(a), c, label="op"),
    3,
)


def _sync_and_async_nodes(
    case: Case, tmp_path: Path
) -> tuple[OperatorJobNode, OperatorJobNode]:
    sync_node = _op_node(_run_job(case, "sync", "in_memory", tmp_path))
    async_node = _op_node(_run_job(case, "async", "in_memory", tmp_path))
    return sync_node, async_node


def test_operator_node_with_no_rows_reads_back_empty_after_async_run(
    tmp_path: Path,
) -> None:
    sync_node, async_node = _sync_and_async_nodes(NO_MATCH, tmp_path)

    sync_table = sync_node.as_table()
    async_table = async_node.as_table()

    assert sync_table.num_rows == 0
    assert async_table.num_rows == 0
    assert async_table.column_names == sync_table.column_names
    assert list(async_node.iter_data()) == []


@pytest.mark.xfail(
    strict=True,
    reason=(
        "ITL-858/ITL-865: no row reaches the async node, so it reads back the table "
        "built from the Python output schema: int8 becomes int64, and the system-tag "
        "and source-info columns are missing"
    ),
)
def test_operator_node_with_no_rows_keeps_columns_and_types_after_async_run(
    tmp_path: Path,
) -> None:
    sync_node, async_node = _sync_and_async_nodes(NO_MATCH, tmp_path)

    assert async_node.as_table(all_info=True).schema == (
        sync_node.as_table(all_info=True).schema
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "ITL-906: the async MergeJoin names system-tag columns from the schema-only "
        "hash of its materialized inputs, not from its upstreams' pipeline hashes"
    ),
)
def test_merge_join_after_an_operator_reads_back_same_system_tags_after_async_run(
    tmp_path: Path,
) -> None:
    sync_node, async_node = _sync_and_async_nodes(MERGE_JOIN_AFTER_OPERATOR, tmp_path)

    assert async_node.as_table(all_info=True).column_names == (
        sync_node.as_table(all_info=True).column_names
    )


def _cleared_table(orchestrator: str, tmp_path: Path) -> tuple[pa.Table, pa.Table]:
    """The node's table after a run, and after ``clear_cache()``."""
    node = _op_node(_run_job(CASES[0], orchestrator, "in_memory", tmp_path))
    after_run = node.as_table()

    # An operator node keeps its output in memory only: clear_cache() discards it, and
    # the node reads as a zero-row table, as before the first run.
    node.clear_cache()

    assert list(node.iter_data()) == []
    return after_run, node.as_table()


def test_clear_cache_returns_operator_node_to_not_yet_run(tmp_path: Path) -> None:
    sync_after_run, sync_cleared = _cleared_table("sync", tmp_path / "sync")
    async_after_run, async_cleared = _cleared_table("async", tmp_path / "async")

    assert sync_after_run.num_rows == async_after_run.num_rows == 3
    assert sync_cleared.num_rows == async_cleared.num_rows == 0
    assert sync_cleared.column_names == sync_after_run.column_names
    assert async_cleared.schema == sync_cleared.schema


@pytest.mark.xfail(
    strict=True,
    reason=(
        "ITL-912: the zero-row table built from the Python output schema widens "
        "int8 to int64 and changes nullability"
    ),
)
@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_clear_cache_keeps_operator_node_arrow_types(
    orchestrator: str, tmp_path: Path
) -> None:
    after_run, cleared = _cleared_table(orchestrator, tmp_path)

    assert cleared.schema == after_run.schema
