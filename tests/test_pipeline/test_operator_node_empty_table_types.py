"""An operator node's zero-row table has the Arrow types its rows have (ITL-912).

Before a run, after ``clear_cache()`` and after a run that produced no row, an operator node
reads as a zero-row table. That table must have exactly the columns, Arrow types and
nullability the node's table has after a run with rows: an ``int8`` stays ``int8``, a
non-nullable column stays non-nullable, a list keeps its item type and the system-tag
columns are present. The node gets those types without computing or reading a row: it
applies its operator to zero-row inputs, as the sync path does.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod import contexts, function_pod
from orcapod.core.nodes import OperatorJobNode
from orcapod.core.operators import Batch, PolarsFilter, SelectDataColumns
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
NO_MATCH = ("g2", "g2", "g3")


def _path_column() -> pa.ChunkedArray:
    """Three ``Path`` values in the logical type the data context stores them with."""
    converter = contexts.resolve_context(None).type_converter
    table = converter.python_dicts_to_arrow_table(
        [{"p": Path("/a")}, {"p": Path("/b")}, {"p": Path("/c")}],
        python_schema={"p": Path},
    )
    return table.column("p")


def _table(groups: tuple[str, ...]) -> pa.Table:
    """Non-nullable ``int8`` tag and data, a logical ``Path`` column and a list column."""
    path = _path_column()
    schema = pa.schema(
        [
            pa.field("id", pa.int8(), nullable=False),
            pa.field("grp", pa.large_string(), nullable=False),
            pa.field("x", pa.int8(), nullable=False),
            pa.field("p", path.type, nullable=False),
            pa.field(
                "l",
                pa.large_list(pa.field("item", pa.int8(), nullable=False)),
                nullable=False,
            ),
        ]
    )
    return pa.table(
        [
            pa.array([1, 2, 3], pa.int8()),
            pa.array(list(groups), pa.large_string()),
            pa.array([10, 20, 30], pa.int8()),
            path.combine_chunks(),
            pa.array([[1], [2, 3], []], schema.field("l").type),
        ],
        schema=schema,
    )


def _source(groups: tuple[str, ...] = WITH_ROWS) -> ArrowTableSource:
    """The source every pipeline here reads; same schema whatever ``groups`` is."""
    return ArrowTableSource(_table(groups), tag_columns=["id", "grp"], source_id="a")


def _g1(stream: StreamProtocol) -> StreamProtocol:
    """``stream`` behind a filter keeping group ``g1``."""
    return PolarsFilter(constraints={"grp": "g1"})(stream)


PIPELINES: dict[str, Callable[[StreamProtocol], object]] = {
    "filter": lambda a: PolarsFilter(constraints={"grp": "g1"})(a, label="op"),
    "select_after_filter": lambda a: SelectDataColumns(["x", "p", "l"])(
        _g1(a), label="op"
    ),
    "batch_after_filter": lambda a: Batch(batch_size=2)(_g1(a), label="op"),
}


def _job(pipeline: str, groups: tuple[str, ...] = WITH_ROWS) -> PipelineJob:
    """A job holding ``pipeline`` over a source with the given groups; not run."""
    job = PipelineJob(name="itl912", store=InMemoryArrowDatabase())
    with job:
        _ = PIPELINES[pipeline](_source(groups))
    return job


def _op(job: PipelineJob) -> OperatorJobNode:
    """The job's operator node labelled ``op``."""
    node = job.nodes["op"]
    assert isinstance(node, OperatorJobNode)
    return node


def _schema_after_run(pipeline: str, orchestrator: str) -> pa.Schema:
    """The node's full schema after a run with rows: the reference."""
    job = _job(pipeline)
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    table = _op(job).as_table(all_info=True)
    assert table.num_rows > 0
    return table.schema


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("pipeline", list(PIPELINES))
def test_not_yet_run_table_has_the_run_types(pipeline: str, orchestrator: str) -> None:
    """Before a run, the node's zero-row table has the types of its rows."""
    expected = _schema_after_run(pipeline, orchestrator)

    table = _op(_job(pipeline)).as_table(all_info=True)

    assert table.num_rows == 0
    assert table.schema == expected


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("pipeline", list(PIPELINES))
def test_cleared_table_has_the_run_types(pipeline: str, orchestrator: str) -> None:
    """After ``clear_cache()``, the node's zero-row table has the run's types."""
    job = _job(pipeline)
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    node = _op(job)
    expected = node.as_table(all_info=True).schema

    node.clear_cache()
    table = node.as_table(all_info=True)

    assert table.num_rows == 0
    assert table.schema == expected


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("pipeline", list(PIPELINES))
def test_zero_row_run_table_has_the_run_types(pipeline: str, orchestrator: str) -> None:
    """A run that produces no row reads with the types of a run with rows.

    ``select_after_filter`` and ``batch_after_filter`` take their input from a node that
    produced no row, so they check the empty input each orchestrator builds as well.
    """
    expected = _schema_after_run(pipeline, orchestrator)
    job = _job(pipeline, NO_MATCH)
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())

    table = _op(job).as_table(all_info=True)

    assert table.num_rows == 0
    assert table.schema == expected


class _RowsMustNotBeRead(ArrowTableSource):
    """A source whose rows must not be read: reading them raises."""

    def iter_data(self):  # pyright: ignore[reportImplicitOverride]  # test double
        raise AssertionError("the source's rows were read")

    def as_table(self, *, columns=None, all_info=False):  # pyright: ignore[reportImplicitOverride]  # test double
        raise AssertionError("the source's rows were read")


def test_not_yet_run_table_reads_no_source_row() -> None:
    """The node types its zero-row table from the source's schema, not its rows."""
    expected = _schema_after_run("select_after_filter", "sync")
    job = PipelineJob(name="itl912", store=InMemoryArrowDatabase())
    with job:
        _ = PIPELINES["select_after_filter"](
            _RowsMustNotBeRead(_table(WITH_ROWS), tag_columns=["id", "grp"], source_id="a")
        )

    table = _op(job).as_table(all_info=True)

    assert table.num_rows == 0
    assert table.schema == expected


def test_not_yet_run_table_calls_no_function() -> None:
    """Downstream of a function pod that has not run, the node calls no function.

    A function node that has not run cannot say its Arrow types without rows (ITL-985),
    so the operator node keeps its table built from the Python schema, with no row.
    """
    calls: list[int] = []

    @function_pod(output_keys="x2")
    def double_x(x: int) -> int:
        """Double ``x``, counting calls."""
        calls.append(x)
        return x * 2

    job = PipelineJob(name="itl912", store=InMemoryArrowDatabase())
    with job:
        _ = SelectDataColumns(["x2"])(double_x.pod(_g1(_source())), label="op")

    table = _op(job).as_table(all_info=True)

    assert calls == []
    assert table.num_rows == 0
    assert "x2" in table.column_names
