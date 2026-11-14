"""A zero-row table has exactly its populated table's schema, column order included (ITL-1131).

ITL-912 made a table with no rows carry the Arrow types and nullability of the rows. This
sweep checks the rest of that rule for every operator and for a function pod: the zero-row
table has the populated table's columns in the populated table's order. It compares
``as_table(all_info=True)`` with ``pa.Schema.equals`` (names, order, types, nullability):

- a plain stream, ``op(stream)``, over an input with no rows, and its ``head()`` before it
  computes, against the same stream over an input with rows;
- a job node before a run, and after a sync or async run whose input had no rows, against the
  same job node after a run with rows by the same orchestrator.

Inputs carry one or two system-tag pairs (a source, or a join of two sources).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod import contexts, function_pod
from orcapod.core.operators import (
    Batch,
    DropColumns,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    Pick,
    PolarsFilter,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import StreamProtocol

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}

WITH_ROWS = ("g1", "g1", "g2")
"""Groups of the sources' rows: the filter on ``g1`` keeps two rows."""
NO_ROWS = ("g2", "g2", "g3")
"""Groups of the sources' rows: the filter on ``g1`` keeps none."""


def _dict_column() -> "pa.Array[Any]":
    """A ``dict[str, int]`` column in the type the data context stores it with."""
    converter = contexts.resolve_context(None).type_converter
    table = converter.python_dicts_to_arrow_table(
        [{"m": {"k": i}} for i in (1, 2, 3)], python_schema={"m": dict[str, int]}
    )
    return table.column("m").combine_chunks()


def _table(groups: tuple[str, ...], data: dict[str, "pa.Array[Any]"]) -> pa.Table:
    """Tags ``g`` and ``id`` and ``data``; every column non-nullable except ``n``."""
    table = pa.table(
        {
            "g": pa.array(list(groups), pa.large_string()),
            "id": pa.array([1, 2, 3], pa.int8()),
            **data,
        }
    )
    return table.cast(
        pa.schema(
            [pa.field(f.name, f.type, nullable=f.name == "n") for f in table.schema]
        )
    )


def _source(
    name: str, groups: tuple[str, ...], data: dict[str, "pa.Array[Any]"]
) -> StreamProtocol:
    """A source of ``data`` behind a filter keeping group ``g1``."""
    source = ArrowTableSource(
        _table(groups, data), tag_columns=["g", "id"], source_id=name
    )
    return PolarsFilter(constraints={"g": "g1"})(source)


def _main(groups: tuple[str, ...]) -> StreamProtocol:
    item = pa.field("item", pa.int8(), nullable=False)
    return _source(
        "a",
        groups,
        {
            "x": pa.array([10, 20, 30], pa.int8()),
            "l": pa.array([[1], [2, 3], [4]], pa.large_list(item)),
            "n": pa.array([1, None, 3], pa.int8()),
            "m": _dict_column(),
        },
    )


def _other(groups: tuple[str, ...]) -> StreamProtocol:
    return _source("b", groups, {"z": pa.array([7, 8, 9], pa.int16())})


def _colliding(groups: tuple[str, ...]) -> StreamProtocol:
    return _source("c", groups, {"x": pa.array([1, 2, 3], pa.int8())})


def _input(groups: tuple[str, ...], pairs: int) -> StreamProtocol:
    """The operator's input: one source, or a join of two (two system-tag pairs)."""
    stream = _main(groups)
    if pairs == 2:
        extra = _source("d", groups, {"w": pa.array([5, 6, 7], pa.int32())})
        stream = Join()(stream, extra)
    return stream


@function_pod(output_keys="y")
def _plus_one(x: int) -> int:
    return x + 1


Step = Callable[[StreamProtocol, tuple[str, ...]], StreamProtocol]

STEPS: dict[str, Step] = {
    "join": lambda a, g: Join()(a, _other(g), label="op"),
    "merge_join": lambda a, g: MergeJoin()(
        SelectDataColumns(["x"])(a), _colliding(g), label="op"
    ),
    "semijoin": lambda a, g: SemiJoin()(a, _other(g), label="op"),
    "batch": lambda a, g: Batch(batch_size=2)(a, label="op"),
    "group_by": lambda a, g: GroupBy(by=["g"])(a, label="op"),
    "select_data": lambda a, g: SelectDataColumns(["x", "l"])(a, label="op"),
    "drop_data": lambda a, g: DropDataColumns(["l"])(a, label="op"),
    "select_tags": lambda a, g: SelectTagColumns(["g"])(a, label="op"),
    "drop_tags": lambda a, g: DropTagColumns(["id"])(a, label="op"),
    "select_columns": lambda a, g: SelectColumns(["g", "x"])(a, label="op"),
    "drop_columns": lambda a, g: DropColumns(["id", "l"])(a, label="op"),
    "map_tags": lambda a, g: MapTags({"id": "ident"})(a, label="op"),
    "map_data": lambda a, g: MapData({"x": "xx"})(a, label="op"),
    "filter": lambda a, g: PolarsFilter(constraints={"id": 1})(a, label="op"),
    "drop_nulls": lambda a, g: DropNulls(["n"])(a, label="op"),
    "drop_nulls_all": lambda a, g: DropNulls()(a, label="op"),
    "index": lambda a, g: Index("l", 0)(a, label="op"),
    "index_out": lambda a, g: Index("l", 0, out="first")(a, label="op"),
    "pick": lambda a, g: Pick("m", "k")(a, label="op"),
    "pick_out": lambda a, g: Pick("m", "k", out="k")(a, label="op"),
    "function_pod": lambda a, g: _plus_one.pod(a, label="op"),
}


def _stream(step: str, pairs: int, groups: tuple[str, ...]) -> StreamProtocol:
    return STEPS[step](_input(groups, pairs), groups)


def _job(step: str, pairs: int, groups: tuple[str, ...]) -> PipelineJob:
    job = PipelineJob(name="itl1131", database=InMemoryArrowDatabase())
    with job:
        _ = STEPS[step](_input(groups, pairs), groups)
    return job


def _populated(table: pa.Table) -> pa.Schema:
    """The reference schema: a table that has rows."""
    assert table.num_rows > 0
    return table.schema


def _assert_same_schema(table: pa.Table, expected: pa.Schema) -> None:
    assert table.num_rows == 0
    assert table.schema.equals(expected), (
        f"zero-row columns {table.schema.names}\n"
        f"populated columns {expected.names}\n"
        f"zero-row schema:\n{table.schema}\npopulated schema:\n{expected}"
    )


PAIRS = [1, 2]


@pytest.mark.parametrize("pairs", PAIRS)
@pytest.mark.parametrize("step", list(STEPS))
class TestPlainStream:
    """``op(stream)`` with no rows has the schema of ``op(stream)`` with rows."""

    def test_over_an_input_with_no_rows(self, step: str, pairs: int) -> None:
        expected = _populated(_stream(step, pairs, WITH_ROWS).as_table(all_info=True))
        table = _stream(step, pairs, NO_ROWS).as_table(all_info=True)
        _assert_same_schema(table, expected)

    def test_head_before_it_computes(self, step: str, pairs: int) -> None:
        expected = _populated(_stream(step, pairs, WITH_ROWS).as_table(all_info=True))
        view = _stream(step, pairs, WITH_ROWS).head(all_info=True)
        _assert_same_schema(view.as_table(), expected)


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("pairs", PAIRS)
@pytest.mark.parametrize("step", list(STEPS))
class TestJobNode:
    """A job node with no rows has the schema of the same node after a run with rows."""

    def _expected(self, step: str, pairs: int, orchestrator: str) -> pa.Schema:
        job = _job(step, pairs, WITH_ROWS)
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        return _populated(job.nodes["op"].as_table(all_info=True))

    def test_before_a_run(self, step: str, pairs: int, orchestrator: str) -> None:
        expected = self._expected(step, pairs, orchestrator)
        job = _job(step, pairs, WITH_ROWS)
        _assert_same_schema(job.nodes["op"].as_table(all_info=True), expected)

    def test_after_a_run_whose_input_had_no_rows(
        self, step: str, pairs: int, orchestrator: str
    ) -> None:
        expected = self._expected(step, pairs, orchestrator)
        job = _job(step, pairs, NO_ROWS)
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        _assert_same_schema(job.nodes["op"].as_table(all_info=True), expected)
