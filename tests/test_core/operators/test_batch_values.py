"""``Batch`` keeps every value exactly, in the input's row order (ITL-1281).

``Batch`` used to rebuild its lists from Python values: a ``time64[ns]`` value lost its
nanoseconds, a ``date64`` value off midnight came back at midnight and a float32
signaling NaN came back quiet. A batch is consecutive rows, so each list is now a slice
of the input's Arrow column.

The input has two tags (``k``, ``n``) in plain-tag, data-tag and mixed roles and in two
tag orders, and three data columns (two of the type under test, one ``large_string``).
Every batch size is checked (one row, exactly the row count, more than the row count, a
size that does not divide it, ``0``), with both drop modes, on the sync and the async
path, and through a job that stores and reads back.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeAlias, cast

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.core.operators import Batch
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.types import CacheMode
from orcapod.utils.arrow_utils import aggregated_list_type

from .several_items import async_rows, ordered_rows_fingerprint
from .test_join_arrow_values import (
    TYPES,
    _exact,  # pyright: ignore[reportPrivateUsage]  # the exact-value reader
)

CONTEXT = get_default_context()
Column: TypeAlias = "pa.Array[Any]"
Row: TypeAlias = "tuple[TagProtocol, DataProtocol]"

PLAIN, DATA_TAGS, MIXED = "plain", "data_tags", "mixed"
ROLES = (PLAIN, DATA_TAGS, MIXED)
TAG_ORDERS = (("k", "n"), ("n", "k"))
ROWS = 5

# (batch_size, drop_partial_batch) -> the batches as (start, end) row ranges.
SIZES: dict[tuple[int, bool], list[tuple[int, int]]] = {
    (1, False): [(i, i + 1) for i in range(ROWS)],
    (5, False): [(0, 5)],
    (5, True): [(0, 5)],
    (7, False): [(0, 5)],
    (7, True): [],
    (2, False): [(0, 2), (2, 4), (4, 5)],
    (2, True): [(0, 2), (2, 4)],
    (0, False): [(0, 5)],
    (0, True): [],
}
SIZE = pytest.mark.parametrize(("batch_size", "drop"), list(SIZES))


def _source(
    x: Column, y: Column, roles: str, tag_order: tuple[str, ...]
) -> ArrowTableSource:
    """Tags ``k`` (``int64``) and ``n`` (``large_string``) in ``tag_order`` and ``roles``;
    data ``x``, ``y`` and ``s``, interleaved with the tags. A column is nullable exactly
    when it holds a null."""
    tag_values = {
        "k": pa.array([3, 1, 4, 1, 5], pa.int64()),
        "n": pa.array(["e", "d", "c", "b", "a"], pa.large_string()),
    }
    columns = {
        "y": y,
        tag_order[0]: tag_values[tag_order[0]],
        "s": pa.array([f"s{i}" for i in range(ROWS)], pa.large_string()),
        tag_order[1]: tag_values[tag_order[1]],
        "x": x,
    }
    schema = pa.schema(
        pa.field(name, column.type, nullable=column.null_count > 0)
        for name, column in columns.items()
    )
    tag_only: bool | list[str]
    if roles == PLAIN:
        tag_only = True
    elif roles == DATA_TAGS:
        tag_only = []
    else:
        tag_only = [tag_order[-1]]
    return ArrowTableSource(
        pa.Table.from_arrays(list(columns.values()), schema=schema),
        tag_columns=list(tag_order),
        source_id="src",
        tag_only_columns=tag_only,
    )


def _batches(rows: Sequence[Row]) -> list[dict[str, Any]]:
    """Each output row's exact lists, read from its Arrow tables, in ``s`` order."""
    out: list[dict[str, Any]] = []
    for tag, data in rows:
        tag_table = tag.as_table()
        data_table = data.as_table()
        row = {
            name: _exact(table.column(name))[0]
            for table in (tag_table, data_table)
            for name in table.column_names
        }
        row["source_x"] = data.source_info()["x"]
        out.append(row)
    return sorted(out, key=lambda r: repr(r["s"]))


def _expected(
    x: Column, y: Column, ranges: list[tuple[int, int]]
) -> list[dict[str, Any]]:
    k = [3, 1, 4, 1, 5]
    n = ["e", "d", "c", "b", "a"]
    out = []
    for start, end in ranges:
        out.append(
            {
                "k": k[start:end],
                "n": n[start:end],
                "s": [f"s{i}" for i in range(start, end)],
                "x": _exact(x.slice(start, end - start)),
                "y": _exact(y.slice(start, end - start)),
                "source_x": [f"src::row_{i}::x" for i in range(start, end)],
            }
        )
    return sorted(out, key=lambda r: repr(r["s"]))


@SIZE
@pytest.mark.parametrize("roles", ROLES)
@pytest.mark.parametrize("tag_order", TAG_ORDERS)
def test_batches_are_consecutive_rows_in_order(
    batch_size: int, drop: bool, roles: str, tag_order: tuple[str, ...]
) -> None:
    """Every list of a batch is the same consecutive rows, in the input's order:
    tags, both data columns and source info parallel, sync and async."""
    x = pa.array([5, 4, 3, 2, 1], pa.int16())
    y = pa.array([0.5, None, 2.5, 3.5, 4.5], pa.float32())
    source = _source(x, y, roles, tag_order)
    stream = Batch(batch_size, drop_partial_batch=drop)(source)
    sync = list(stream.iter_data())
    assert _batches(sync) == _expected(x, y, SIZES[(batch_size, drop)])
    asynchronous = async_rows(Batch(batch_size, drop_partial_batch=drop), source)
    assert ordered_rows_fingerprint(asynchronous) == ordered_rows_fingerprint(sync)


TYPE = pytest.mark.parametrize("type_name", list(TYPES))


@TYPE
@pytest.mark.parametrize(("batch_size", "drop"), [(2, False), (2, True), (0, False)])
@pytest.mark.parametrize("roles", ROLES)
def test_every_value_is_kept_exactly(
    type_name: str, batch_size: int, drop: bool, roles: str
) -> None:
    """Values of every type, nulls included, come out bit for bit, sync and async."""
    build = TYPES[type_name]
    x, y = build([0, 1, None, 3, 2]), build([3, 2, 1, 0, None])
    source = _source(x, y, roles, ("k", "n"))
    expected = _expected(x, y, SIZES[(batch_size, drop)])
    stream = Batch(batch_size, drop_partial_batch=drop)(source)
    assert _batches(list(stream.iter_data())) == expected
    assert (
        _batches(async_rows(Batch(batch_size, drop_partial_batch=drop), source))
        == expected
    )
    table = stream.as_table()
    input_type = source.as_table().schema.field("x").type
    assert table.schema.field("x").type == aggregated_list_type(
        input_type, CONTEXT.type_converter, column="x"
    )


@TYPE
def test_zero_rows_keep_the_types(type_name: str) -> None:
    """A batch result with no rows (every row dropped) has the types of one with rows."""
    build = TYPES[type_name]
    source = _source(build([0, 1, 2, 3, 0]), build([3, 2, 1, 0, 1]), MIXED, ("k", "n"))
    with_rows = Batch(2)(source).as_table(columns={"source": True})
    without_rows = Batch(7, drop_partial_batch=True)(source).as_table(
        columns={"source": True}
    )
    assert without_rows.num_rows == 0
    assert without_rows.schema == with_rows.schema


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
@pytest.mark.parametrize("type_name", ["time64[ns]", "float32 signaling NaN"])
def test_a_job_stores_and_reads_back_every_value(
    orchestrator: Any, type_name: str
) -> None:
    """The node's rows and its stored records hold the exact values."""
    build = TYPES[type_name]
    x, y = build([0, 1, None, 3, 2]), build([3, 2, 1, 0, None])
    source = _source(x, y, MIXED, ("k", "n"))
    expected = _expected(x, y, SIZES[(2, False)])
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="batch_exact", database=database)
    with job:
        _ = Batch(2)(source, label="batched")
    job.nodes["batched"].attach_databases(
        pipeline_database=database.at("batch_exact"), cache_mode=CacheMode.LOG
    )
    job.run(orchestrator=orchestrator())
    assert _batches(list(job.nodes["batched"].iter_data())) == expected
    # The stored records are a new source: their source info names it, not ``src``.
    stored = job.nodes["batched"].as_source()
    assert [
        {k: v for k, v in row.items() if k != "source_x"}
        for row in _batches(list(stored.iter_data()))
    ] == [{k: v for k, v in row.items() if k != "source_x"} for row in expected]


# ── null-typed data on the row and async paths (ITL-1319) ─────────────────────

NULL_TYPED: dict[str, tuple[pa.DataType, list[Any]]] = {
    "null": (pa.null(), [None, None, None]),
    "large_list<null>": (pa.large_list(pa.null()), [[None], [], None]),
}
ITL_1319 = pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="ITL-1319: a datagram normalizes a null-typed column to large_string (Data's "
    + "null convention, Datagram._init_from_table without keep_null), so the row and "
    + "async results type a null-typed batched column as large_list<large_string>",
)


def _null_typed_stream(name: str) -> ArrowTableStream:
    """A raw stream (a source refuses a null-typed column): tags ``k`` and ``n``, data
    ``x`` and ``y`` of the null type."""
    arrow_type, values = NULL_TYPED[name]
    return ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2, 3], pa.int64()),
                "n": pa.array([1, 1, 1], pa.int64()),
                "x": pa.array(values, arrow_type),
                "y": pa.array(values, arrow_type),
            }
        ),
        tag_columns=["k", "n"],
    )


@pytest.mark.parametrize("name", list(NULL_TYPED))
def test_null_typed_batched_columns_keep_their_values_on_every_path(name: str) -> None:
    """The table path keeps the null type; every path keeps the values and nesting."""
    arrow_type, values = NULL_TYPED[name]
    stream = _null_typed_stream(name)
    table = Batch(2).unary_static_process(stream).as_table()
    for column in ("x", "y"):
        assert table.schema.field(column).type == pa.large_list(arrow_type)
        assert table.column(column).to_pylist() == [values[:2], values[2:]]
    for rows in (list(Batch(2)(stream).iter_data()), async_rows(Batch(2), stream)):
        # Batch keeps row order: the first batch holds rows 0 and 1, the second row 2.
        got = [
            [
                cast(list[Any], data.as_table().column(column).to_pylist()[0])
                for column in ("x", "y")
            ]
            for _, data in rows
        ]
        assert got == [[values[:2]] * 2, [values[2:]] * 2]


@ITL_1319
@pytest.mark.parametrize("path", ["datagram", "async"])
@pytest.mark.parametrize("name", list(NULL_TYPED))
def test_null_typed_batched_columns_keep_their_type_on_the_row_paths(
    name: str, path: str
) -> None:
    """The row (datagram) and async results type ``x`` and ``y`` as the table does."""
    stream = _null_typed_stream(name)
    table = Batch(2).unary_static_process(stream).as_table()
    rows = (
        list(Batch(2)(stream).iter_data())
        if path == "datagram"
        else async_rows(Batch(2), stream)
    )
    for _, data in rows:
        row = data.as_table()
        for column in ("x", "y"):
            assert row.schema.field(column).type == table.schema.field(column).type
