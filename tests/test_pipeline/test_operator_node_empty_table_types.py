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
from typing import Any
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod import contexts, function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.nodes import OperatorJobNode
from orcapod.core.operators import Batch, PolarsFilter, SelectDataColumns
from orcapod.core.sources import ArrowTableSource, DeltaTableSource
from orcapod.core.sources.cached_source import CachedSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.types import ColumnConfig

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}

WITH_ROWS = ("g1", "g1", "g2")
NO_MATCH = ("g2", "g2", "g3")


def _path_column() -> pa.ChunkedArray[Any]:
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

    def iter_data(self):
        raise AssertionError("the source's rows were read")

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        raise AssertionError("the source's rows were read")


def test_not_yet_run_table_reads_no_source_row() -> None:
    """The node types its zero-row table from the source's schema, not its rows."""
    expected = _schema_after_run("select_after_filter", "sync")
    job = PipelineJob(name="itl912", store=InMemoryArrowDatabase())
    with job:
        _ = PIPELINES["select_after_filter"](
            _RowsMustNotBeRead(
                _table(WITH_ROWS), tag_columns=["id", "grp"], source_id="a"
            )
        )

    table = _op(job).as_table(all_info=True)

    assert table.num_rows == 0
    assert table.schema == expected


@dataclass(frozen=True)
class Point:
    """A user class a function returns."""

    a: int
    b: str


CALLS: list[int] = []


@function_pod(output_keys=("n", "maybe", "where", "items", "point"))
def describe_x(x: int) -> tuple[int, int | None, Path, list[int], Point]:
    """Outputs of each kind: plain, optional, logical, list and user class."""
    CALLS.append(x)
    return x, None, Path(f"/{x}"), [x, x], Point(x, str(x))


def _function_stream(groups: tuple[str, ...]) -> StreamProtocol:
    """``describe_x`` over the ``g1`` rows of a source with the given groups."""
    return describe_x.pod(_g1(_source(groups)))


def _cached_function_stream(groups: tuple[str, ...]) -> StreamProtocol:
    """``describe_x`` through a ``CachedFunctionPod`` over the ``g1`` rows."""
    # The decorated function's pod, as the hashing tests pass it (typed loosely).
    inner: Any = describe_x.pod
    pod = CachedFunctionPod(inner, result_database=InMemoryArrowDatabase())
    return pod(_g1(_source(groups)))


@pytest.mark.parametrize(
    "make", [_function_stream, _cached_function_stream], ids=["plain", "cached"]
)
def test_function_pod_stream_zero_rows_has_the_computed_types(
    make: Callable[[tuple[str, ...]], StreamProtocol],
) -> None:
    """A function pod over zero rows gives the table of its computed rows, no row."""
    expected = make(WITH_ROWS).as_table(all_info=True)
    assert expected.num_rows > 0
    CALLS.clear()

    table = make(NO_MATCH).as_table(all_info=True)

    assert CALLS == []
    assert table.num_rows == 0
    assert table.schema == expected.schema


def _function_job(groups: tuple[str, ...] = WITH_ROWS) -> PipelineJob:
    """A job: ``describe_x`` over the ``g1`` rows, then an operator labelled ``op``."""
    job = PipelineJob(name="itl912", store=InMemoryArrowDatabase())
    with job:
        _ = SelectDataColumns(["n", "where", "items"])(
            describe_x.pod(_g1(_source(groups)), label="fn"), label="op"
        )
    return job


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_function_node_zero_row_tables_have_the_computed_types(
    orchestrator: str,
) -> None:
    """A function node, and an operator after it, type their zero-row tables.

    Before a run and after a run with no row, both tables equal the tables of a run
    with rows, and the function is never called for them.
    """
    ran = _function_job()
    _ = ran.run(orchestrator=ORCHESTRATORS[orchestrator]())
    expected_fn = ran.nodes["fn"].as_table(all_info=True).schema
    expected_op = _op(ran).as_table(all_info=True).schema
    CALLS.clear()

    not_run = _function_job()
    assert not_run.nodes["fn"].as_table(all_info=True).schema == expected_fn
    assert _op(not_run).as_table(all_info=True).schema == expected_op
    assert CALLS == []

    no_rows = _function_job(NO_MATCH)
    _ = no_rows.run(orchestrator=ORCHESTRATORS[orchestrator]())
    fn_table = no_rows.nodes["fn"].as_table(all_info=True)
    op_table = _op(no_rows).as_table(all_info=True)
    assert fn_table.num_rows == op_table.num_rows == 0
    assert fn_table.schema == expected_fn
    assert op_table.schema == expected_op
    assert CALLS == []


def _int8_table() -> pa.Table:
    """A small table of non-nullable ``int8`` tag and data columns."""
    return pa.table(
        [pa.array([1, 2], pa.int8()), pa.array([10, 20], pa.int8())],
        schema=pa.schema(
            [
                pa.field("id", pa.int8(), nullable=False),
                pa.field("x", pa.int8(), nullable=False),
            ]
        ),
    )


def _arrow_table_source(tmp_path: Path) -> ArrowTableSource:
    """An in-memory source; ``tmp_path`` is unused (every maker takes it)."""
    del tmp_path
    return ArrowTableSource(_int8_table(), tag_columns=["id"], source_id="kinds")


def _cached_source(tmp_path: Path) -> StreamProtocol:
    """A cached source over an in-memory source; its cache is never read here."""
    del tmp_path
    return CachedSource(
        ArrowTableSource(_int8_table(), tag_columns=["id"], source_id="kinds"),
        cache_database=InMemoryArrowDatabase(),
    )


def _delta_source(tmp_path: Path) -> StreamProtocol:
    """A Delta table source; its table is read when it is built."""
    from deltalake import write_deltalake

    write_deltalake(tmp_path / "delta", _int8_table(), mode="overwrite")
    return DeltaTableSource(tmp_path / "delta", tag_columns=["id"], source_id="kinds")


@pytest.mark.parametrize(
    "make_source",
    [_arrow_table_source, _cached_source, _delta_source],
    ids=["arrow_table", "cached", "delta"],
)
def test_operator_node_over_each_source_kind_types_its_zero_row_table(
    make_source: Callable[[Path], StreamProtocol], tmp_path: Path
) -> None:
    """Before a run, the node's zero-row table has the types of the run's rows."""

    def job() -> PipelineJob:
        """A job selecting ``x`` from a fresh source of this kind."""
        new = PipelineJob(name="itl912", store=InMemoryArrowDatabase())
        with new:
            _ = SelectDataColumns(["x"])(make_source(tmp_path), label="op")
        return new

    table = _op(job()).as_table(all_info=True)
    ran = job()
    _ = ran.run(orchestrator=SyncPipelineOrchestrator())
    expected = _op(ran).as_table(all_info=True)

    assert expected.num_rows == 2
    assert table.num_rows == 0
    assert table.schema == expected.schema
