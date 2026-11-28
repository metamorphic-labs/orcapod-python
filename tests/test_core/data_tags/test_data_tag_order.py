"""Data-tags come first among the data columns, in tag order (ITL-1205, fix B).

Developer decision, November 17, 2026: a stream lists its data-tags first among its data
keys, in tag order, then its other data columns in table order. A table built from rows
lays a data-tag out among the tags (``_in_row_order``), so this is the order every row
has, on every path: a plain stream, a source, an operator's result, and a job node's rows
on the sync and the async orchestrator, with and without a stored operator cache. Before,
a data-tag kept its table position in a stream's keys, so a source with data ``a, x, y``
(``x`` a data-tag) said ``a, x, y`` while the rows of a node over it, and the async
barrier path (``_materialize_input_stream``), gave ``x, a, y``.

Nothing stored or hashed changes: tables keep their physical column order, and schema,
row and table hashes do not depend on column order.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.data_tags import stream_data_tags
from orcapod.core.operators import AddTags, PolarsFilter, TagBy
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.types import CacheMode

# PolarsFilter is built here on purpose; the warning of every construction is tested in
# test_polars_filter_warning.py (ITL-1296).
pytestmark = pytest.mark.filterwarnings("ignore::orcapod.errors.PolarsFilterWarning")

JOB = "order"


def _table() -> pa.Table:
    """Data ``a``, the tag ``x`` and data ``y``: the tag is not the first column."""
    table = pa.table(
        {
            "a": pa.array([1.0, 2.0, 3.0], pa.float64()),
            "x": pa.array([1, 2, 3], pa.int64()),
            "y": pa.array(["p", "q", "r"], pa.large_string()),
        }
    )
    # Non-nullable, so ``y`` can become a tag.
    return table.cast(pa.schema([f.with_nullable(False) for f in table.schema]))


def _source(**kwargs: Any) -> op.sources.ArrowTableSource:  # pyright: ignore[reportExplicitAny]  # forwarded options
    return op.sources.ArrowTableSource(
        _table(), tag_columns=["x"], source_id="order", **kwargs
    )


def _assert_order_is_row_order(
    stream: StreamProtocol, data_keys: tuple[str, ...], *, row_built: bool = True
) -> None:
    """Keys, schema, every row and (for a table built from rows) the table agree."""
    assert stream.keys()[1] == data_keys
    assert tuple(stream.output_schema()[1]) == data_keys
    rows = list(stream.iter_data())
    assert rows
    for _, data in rows:
        assert tuple(data.keys()) == data_keys
    if not row_built:
        return
    # A table built from rows lays a data-tag out once, among the tags: in table
    # order, the data names are the data keys.
    table_order = tuple(c for c in stream.as_table().column_names if c in data_keys)
    assert table_order == data_keys


def test_a_raw_stream_lists_its_data_tags_first() -> None:
    stream = ArrowTableStream(_table(), tag_columns=["x"], data_tags=["x"])
    assert stream.keys() == (("x",), ("x", "a", "y"))
    for _, data in stream.iter_data():
        assert tuple(data.keys()) == ("x", "a", "y")
    # The table the stream holds is unchanged.
    assert stream.as_table().column_names == ["a", "x", "y"]


def test_data_tags_follow_the_tag_order() -> None:
    table = pa.table(
        {
            "a": pa.array([1.0], pa.float64()),
            "u": pa.array([1], pa.int64()),
            "v": pa.array([2], pa.int64()),
        }
    )
    stream = ArrowTableStream(table, tag_columns=["v", "u"], data_tags=["u", "v"])
    assert stream.keys() == (("v", "u"), ("v", "u", "a"))


def test_a_plain_stream_keeps_its_table_order() -> None:
    stream = ArrowTableStream(_table(), tag_columns=["x"])
    assert stream.keys() == (("x",), ("a", "y"))


def test_a_source_lists_its_data_tags_first() -> None:
    # A source's table is the user's, in its own column order.
    _assert_order_is_row_order(_source(), ("x", "a", "y"), row_built=False)
    assert _source(tag_only_columns=True).keys() == (("x",), ("a", "y"))


def test_the_order_moves_no_hash() -> None:
    """The stream's identity is its table, which keeps its physical order."""
    stream = ArrowTableStream(_table(), tag_columns=["x"], data_tags=["x"])
    reordered = ArrowTableStream(
        _table().select(["x", "a", "y"]), tag_columns=["x"], data_tags=["x"]
    )
    assert stream.keys() == reordered.keys()
    assert stream.content_hash() == reordered.content_hash()
    assert stream.pipeline_hash() == reordered.pipeline_hash()
    assert stream_data_tags(stream) == ("x",)


OPERATORS: list[tuple[str, Callable[[], Any]]] = [  # pyright: ignore[reportExplicitAny]  # operator factories
    ("polars_filter", lambda: PolarsFilter(constraints={"x": 2})),
    ("tag_by", lambda: TagBy("y")),
    ("add_tags", lambda: AddTags({"cohort": "A"})),
]


@pytest.mark.parametrize(("label", "make_op"), OPERATORS, ids=[o[0] for o in OPERATORS])
def test_an_operator_result_lists_data_tags_first(
    label: str,
    make_op: Callable[[], Any],  # pyright: ignore[reportExplicitAny]  # operator factory
) -> None:
    result = make_op()(_source())
    keys = result.keys()[1]
    assert keys[: len(stream_data_tags(result))] == stream_data_tags(result)
    _assert_order_is_row_order(result, keys, row_built=False)


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
@pytest.mark.parametrize("mode", [CacheMode.OFF, CacheMode.LOG], ids=["off", "log"])
@pytest.mark.parametrize(("label", "make_op"), OPERATORS, ids=[o[0] for o in OPERATORS])
def test_a_job_node_rows_follow_its_keys(
    label: str,
    make_op: Callable[[], Any],  # pyright: ignore[reportExplicitAny]  # operator factory
    mode: CacheMode,
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    store = InMemoryArrowDatabase()
    job = PipelineJob(name=JOB, database=store)
    with job:
        _ = make_op()(_source(), label=label)
    node = job.nodes[label]
    node.attach_databases(pipeline_database=store.at(JOB), cache_mode=mode)
    job.run(orchestrator=orchestrator())
    keys = node.keys()[1]
    assert keys[0] == "x"
    _assert_order_is_row_order(node, keys)


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
def test_a_fall_through_function_node_rows_follow_its_keys(
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    @op.function_pod(output_keys=["half"])
    def half(a: float) -> float:
        return a / 2

    job = PipelineJob(name=JOB, database=InMemoryArrowDatabase())
    with job:
        _ = half.pod(_source(), label="half")
    job.run(orchestrator=orchestrator())
    node = job.nodes["half"]
    assert node.keys() == (("x",), ("x", "half"))
    _assert_order_is_row_order(node, ("x", "half"))
