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
from typing import Any

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
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import StreamProtocol

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


def _double_x(x: int) -> int:
    return x * 2


@dataclass(frozen=True)
class Case:
    """An operator applied inside a job; the operator node under test is labelled ``op``."""

    name: str
    build: Callable[[StreamProtocol, StreamProtocol, StreamProtocol], Any]
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
    Case(
        "polars_filter_no_match",
        lambda a, b, c: PolarsFilter(constraints={"grp": "none"})(a, label="op"),
        0,
    ),
    Case("join", lambda a, b, c: Join()(a, b, label="op"), 3),
    Case(
        "merge_join",
        lambda a, b, c: MergeJoin()(SelectDataColumns(["x"])(a), c, label="op"),
        3,
    ),
    Case("semi_join", lambda a, b, c: SemiJoin()(a, b, label="op"), 3),
    Case("batch", lambda a, b, c: Batch(batch_size=2)(a, label="op"), 2),
    Case("group_by", lambda a, b, c: GroupBy(by=["grp"])(a, label="op"), 2),
    Case(
        "operator_feeding_function_pod",
        lambda a, b, c: FunctionPod(
            PythonDataFunction(_double_x, output_keys="x2")
        )(SelectDataColumns(["x"])(a, label="op"), label="double"),
        3,
    ),
]


def _make_store(kind: str, tmp_path: Path) -> Any:
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
        case.build(_source_a(), _source_b(), _source_c())
    job.run(
        orchestrator=ORCHESTRATORS[orchestrator](),
        materialize_results=materialize_results,
    )
    return job


def _table_rows(table: pa.Table) -> list[dict[str, Any]]:
    """Rows of ``table`` in a canonical order, so arrival order does not matter."""
    return sorted(table.to_pylist(), key=repr)


def _row_hashes(node: Any) -> list[tuple[str, str]]:
    return sorted(
        (tag.content_hash().to_string(), data.content_hash().to_string())
        for tag, data in node.iter_data()
    )


@pytest.mark.parametrize("materialize_results", [True, False])
@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_operator_node_reads_back_same_rows_after_async_run(
    case: Case, store_kind: str, materialize_results: bool, tmp_path: Path
) -> None:
    sync_node = _run_job(
        case, "sync", store_kind, tmp_path, materialize_results
    ).nodes["op"]
    async_node = _run_job(
        case, "async", store_kind, tmp_path, materialize_results
    ).nodes["op"]

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
    node = _run_job(CASES[0], orchestrator, "in_memory", tmp_path).nodes["op"]

    assert node.as_table().num_rows == 3
    assert node.get_all_records() is None


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_clear_cache_returns_operator_node_to_not_yet_run(
    orchestrator: str, tmp_path: Path
) -> None:
    node = _run_job(CASES[0], orchestrator, "in_memory", tmp_path).nodes["op"]
    schema_after_run = node.as_table().schema

    # An operator node keeps its output in memory only: clear_cache() discards it, and
    # the node reads as a zero-row table with its schema, as before the first run.
    node.clear_cache()

    table = node.as_table()
    assert table.num_rows == 0
    assert table.schema == schema_after_run
    assert list(node.iter_data()) == []
