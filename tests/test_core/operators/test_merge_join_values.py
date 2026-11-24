"""``MergeJoin`` is ordered and keeps every value exactly (ITL-1281).

Developer decisions of November 19, 2026: ``MergeJoin(a, b)`` and ``MergeJoin(b, a)`` are
two operations with two identities; a merged list is ``[value of the first argument,
value of the second]``; nothing is sorted, so element ``i`` of every merged list of a row,
of its source-info list and the system tags ending in ``:i`` come from the same origin
row; the lists are built from the inputs' Arrow arrays, so every value is the input's,
bit for bit.

The inputs have two tags (``k``, ``n``) in plain-tag, data-tag and mixed roles and in two
tag orders, two colliding data columns whose values run in OPPOSITE orders across the two
inputs (a per-column value sort would break the row alignment), and one non-colliding
data column on each side. Keys repeat on both sides (many to many) and a null key matches
nothing. Every case runs on the sync and the async path.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeAlias

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.core.operators import MergeJoin
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
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

# Left rows (k, n) and right rows; keys repeat on both sides, a null key matches nothing.
LEFT_KEYS: list[int | None] = [0, 1, 1, 2, None]
LEFT_NUMBERS = [1, 1, 1, 2, 1]
RIGHT_KEYS: list[int | None] = [1, 3, 1, 2, None]
RIGHT_NUMBERS = [1, 1, 1, 2, 1]
# (left row, right row) pairs that match.
MATCHES = [(1, 0), (1, 2), (2, 0), (2, 2), (3, 3)]

# Two colliding columns whose values run in opposite orders across the inputs: on every
# matched row the left x is the smaller and the left y the larger.
LEFT_X = [10, 11, 12, 13, 14]
RIGHT_X = [20, 21, 22, 23, 24]
LEFT_Y = [9.5, 8.5, 7.5, 6.5, 5.5]
RIGHT_Y = [1.5, 2.5, 3.5, 4.5, 0.5]


def _source(
    columns: dict[str, Column],
    tags: Sequence[str],
    roles: str,
    source_id: str,
) -> ArrowTableSource:
    """A source of ``columns`` with ``tags`` (in this order) in ``roles``.

    A column is nullable exactly when it holds a null.
    """
    schema = pa.schema(
        pa.field(name, column.type, nullable=column.null_count > 0)
        for name, column in columns.items()
    )
    table = pa.Table.from_arrays(list(columns.values()), schema=schema)
    tag_only: bool | list[str]
    if roles == PLAIN:
        tag_only = True
    elif roles == DATA_TAGS:
        tag_only = []
    else:
        tag_only = [tags[-1]]
    return ArrowTableSource(
        table, tag_columns=list(tags), source_id=source_id, tag_only_columns=tag_only
    )


def _side(
    name: str,
    keys: list[int | None],
    numbers: list[int],
    x: Column,
    y: Column,
    roles: str,
    tag_order: tuple[str, ...],
    own: str | None = None,
) -> ArrowTableSource:
    """One input: tags ``k`` and ``n``, colliding ``x`` and ``y``, and its own data
    column ``own`` (default ``s_<name>``; a name both inputs give collides too).

    The table interleaves tags and data, the tags in ``tag_order``."""
    tag_values = {"k": pa.array(keys, pa.int64()), "n": pa.array(numbers, pa.int64())}
    columns = {
        "y": y,
        tag_order[0]: tag_values[tag_order[0]],
        own or f"s_{name}": pa.array(
            [f"{name}{i}" for i in range(len(keys))], pa.large_string()
        ),
        tag_order[1]: tag_values[tag_order[1]],
        "x": x,
    }
    return _source(columns, tag_order, roles, name)


def _inputs(roles: str, tag_order: tuple[str, ...]) -> tuple[ArrowTableSource, ...]:
    left = _side(
        "a",
        LEFT_KEYS,
        LEFT_NUMBERS,
        pa.array(LEFT_X, pa.int32()),
        pa.array(LEFT_Y, pa.float64()),
        roles,
        tag_order,
    )
    right = _side(
        "b",
        RIGHT_KEYS,
        RIGHT_NUMBERS,
        pa.array(RIGHT_X, pa.int32()),
        pa.array(RIGHT_Y, pa.float64()),
        roles,
        tag_order,
    )
    return left, right


def _system_tags_by_position(tag: TagProtocol) -> dict[str, tuple[str, str]]:
    """``{position: (source id, record id)}`` of one row's system tags."""
    found: dict[str, dict[str, str]] = {}
    for name, value in tag.system_tags().items():
        position = name.rsplit(":", 1)[1]
        kind = (
            "source"
            if name.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
            else "record"
        )
        found.setdefault(position, {})[kind] = str(value)
    return {p: (v["source"], v["record"]) for p, v in found.items()}


def _merged_rows(rows: Sequence[Row]) -> list[tuple[Any, ...]]:
    """Each row as (x, y, source info of x and y, system tags by position, s_a, s_b)."""
    out: list[tuple[Any, ...]] = []
    for tag, data in rows:
        values = data.as_dict()
        info = data.source_info()
        out.append(
            (
                values["x"],
                values["y"],
                info["x"],
                info["y"],
                _system_tags_by_position(tag),
                values["s_a"],
                values["s_b"],
            )
        )
    return sorted(out, key=repr)


def _expected(first: str, second: str) -> list[tuple[Any, ...]]:
    """The rows of ``MergeJoin()(first, second)``: every list in argument order."""
    data = {
        "a": (LEFT_X, LEFT_Y),
        "b": (RIGHT_X, RIGHT_Y),
    }
    rows = []
    for left_row, right_row in MATCHES:
        index = {"a": left_row, "b": right_row}
        i, j = index[first], index[second]
        # A source id is ``<kind part>:<name>`` (ITL-1425).
        one, two = f"arrow_table_v1:{first}", f"arrow_table_v1:{second}"
        rows.append(
            (
                [data[first][0][i], data[second][0][j]],
                [data[first][1][i], data[second][1][j]],
                [f"{one}::row_{i}::x", f"{two}::row_{j}::x"],
                [f"{one}::row_{i}::y", f"{two}::row_{j}::y"],
                {"0": (one, f"row_{i}"), "1": (two, f"row_{j}")},
                f"a{left_row}",
                f"b{right_row}",
            )
        )
    return sorted(rows, key=repr)


@pytest.mark.parametrize("roles", ROLES)
@pytest.mark.parametrize("tag_order", TAG_ORDERS)
def test_every_list_of_a_row_is_in_argument_order(
    roles: str, tag_order: tuple[str, ...]
) -> None:
    """Both colliding columns, their source info and the system tags follow one input
    order per row, the argument order, in both argument orders, sync and async."""
    left, right = _inputs(roles, tag_order)
    for (first, second), (one, other) in (
        (("a", "b"), (left, right)),
        (("b", "a"), (right, left)),
    ):
        stream = MergeJoin()(one, other)
        sync = list(stream.iter_data())
        assert _merged_rows(sync) == _expected(first, second)
        asynchronous = async_rows(MergeJoin(), one, other)
        assert ordered_rows_fingerprint(asynchronous) == ordered_rows_fingerprint(sync)


@pytest.mark.parametrize("roles", ROLES)
def test_the_two_argument_orders_are_two_operations(roles: str) -> None:
    """``MergeJoin(a, b)`` and ``MergeJoin(b, a)``: different hashes, reversed lists."""
    left, right = _inputs(roles, ("k", "n"))
    forward, backward = MergeJoin()(left, right), MergeJoin()(right, left)
    assert forward.content_hash() != backward.content_hash()
    assert forward.pipeline_hash() != backward.pipeline_hash()
    assert MergeJoin().argument_symmetry([left, right]) == (left, right)
    # The same operation twice has one identity.
    again = MergeJoin()(left, right)
    assert again.content_hash() == forward.content_hash()
    assert again.pipeline_hash() == forward.pipeline_hash()
    assert MergeJoin().argument_symmetry([right, left]) == (right, left)


def test_two_inputs_with_one_pipeline_hash_keep_argument_order() -> None:
    """Two sources of one schema: the positions are the argument indices, nothing is
    sorted by source id, so the first argument's values are always element 0."""
    one, two = (
        _side(
            name,
            keys,
            numbers,
            pa.array(x, pa.int32()),
            pa.array(y, pa.float64()),
            PLAIN,
            ("k", "n"),
            own="s",
        )
        for name, keys, numbers, x, y in (
            ("a", LEFT_KEYS, LEFT_NUMBERS, LEFT_X, LEFT_Y),
            ("b", RIGHT_KEYS, RIGHT_NUMBERS, RIGHT_X, RIGHT_Y),
        )
    )
    assert one.pipeline_hash() == two.pipeline_hash()
    for first, second, a, b in (("a", "b", one, two), ("b", "a", two, one)):
        want = [row[:5] for row in _expected(first, second)]
        for rows in (
            list(MergeJoin()(a, b).iter_data()),
            async_rows(MergeJoin(), a, b),
        ):
            got = []
            for tag, data in rows:
                info = data.source_info()
                got.append(
                    (
                        data["x"],
                        data["y"],
                        info["x"],
                        info["y"],
                        _system_tags_by_position(tag),
                    )
                )
            assert sorted(got, key=repr) == sorted(want, key=repr)


# ── every value type ─────────────────────────────────────────────────────────


TYPE = pytest.mark.parametrize("type_name", list(TYPES))


def _typed_inputs(type_name: str, roles: str) -> tuple[ArrowTableSource, ...]:
    """Inputs whose colliding ``x`` and ``y`` are of the type, with nulls."""
    build = TYPES[type_name]
    left = _side(
        "a",
        LEFT_KEYS,
        LEFT_NUMBERS,
        build([0, 1, None, 3, 0]),
        build([3, 2, 1, 0, None]),
        roles,
        ("k", "n"),
    )
    right = _side(
        "b",
        RIGHT_KEYS,
        RIGHT_NUMBERS,
        build([3, None, 1, 0, 1]),
        build([0, 1, 2, None, 3]),
        roles,
        ("k", "n"),
    )
    return left, right


def _exact_lists(rows: Sequence[Row]) -> list[tuple[str, str, str]]:
    """Each row's (s_a, s_b, exact x and y lists), read from its Arrow table; sorted."""
    out = []
    for _, data in rows:
        table = data.as_table()
        out.append(
            (
                str(table.column("s_a")[0].as_py()),
                str(table.column("s_b")[0].as_py()),
                repr((_exact(table.column("x")), _exact(table.column("y")))),
            )
        )
    return sorted(out)


@TYPE
@pytest.mark.parametrize("roles", ROLES)
def test_every_value_merges_exactly(type_name: str, roles: str) -> None:
    """Values of every type, nulls included, come out bit for bit in argument order,
    sync and async; the list keeps the element type (a list one level deeper)."""
    build = TYPES[type_name]
    left, right = _typed_inputs(type_name, roles)
    left_x, left_y = build([0, 1, None, 3, 0]), build([3, 2, 1, 0, None])
    right_x, right_y = build([3, None, 1, 0, 1]), build([0, 1, 2, None, 3])
    # ``_exact`` of a one-row slice is a one-element list; their sum is the pair.
    want = sorted(
        (
            f"a{i}",
            f"b{j}",
            repr(
                (
                    [_exact(left_x.slice(i, 1)) + _exact(right_x.slice(j, 1))],
                    [_exact(left_y.slice(i, 1)) + _exact(right_y.slice(j, 1))],
                )
            ),
        )
        for i, j in MATCHES
    )
    stream = MergeJoin()(left, right)
    sync = list(stream.iter_data())
    assert _exact_lists(sync) == want
    assert _exact_lists(async_rows(MergeJoin(), left, right)) == want
    # The merged column is the list of the input's type, never flattened (a list
    # column becomes a list of lists), and never null itself.
    table = stream.as_table()
    input_type = left.as_table().schema.field("x").type
    assert table.schema.field("x").type == aggregated_list_type(
        input_type, CONTEXT.type_converter, column="x"
    )
    assert not table.schema.field("x").nullable


@TYPE
def test_zero_rows_keep_the_types(type_name: str) -> None:
    """A MergeJoin with no matching row has the types of one with rows."""
    left, right = _typed_inputs(type_name, PLAIN)
    with_rows = MergeJoin()(left, right).as_table(columns={"source": True})
    # One row, a key nothing matches; nullable like the left input's columns.
    empty_right = _side(
        "b",
        [7],
        [7],
        TYPES[type_name]([None]),
        TYPES[type_name]([None]),
        PLAIN,
        ("k", "n"),
    )
    without_rows = MergeJoin()(left, empty_right).as_table(columns={"source": True})
    assert without_rows.num_rows == 0
    assert without_rows.schema.field("x").type == with_rows.schema.field("x").type
    assert without_rows.schema.field("y").type == with_rows.schema.field("y").type
    assert (
        without_rows.schema.field("_source_x").type
        == with_rows.schema.field("_source_x").type
    )


# ── nullability and refusals ─────────────────────────────────────────────────


def test_colliding_columns_differing_in_nullability_are_refused() -> None:
    """``T`` in one input and ``T | None`` in the other: a colliding DATA column must
    have one type in both inputs, nullability included, as before this change
    (ITL-829 reconciles tag keys only). Refused in both argument orders."""
    left = _side(
        "a",
        LEFT_KEYS,
        LEFT_NUMBERS,
        pa.array(LEFT_X, pa.int32()),
        pa.array(LEFT_Y, pa.float64()),
        PLAIN,
        ("k", "n"),
    )
    right = _side(
        "b",
        RIGHT_KEYS,
        RIGHT_NUMBERS,
        pa.array([20, None, 22, 23, 24], pa.int32()),
        pa.array(RIGHT_Y, pa.float64()),
        PLAIN,
        ("k", "n"),
    )
    for one, other in ((left, right), (right, left)):
        with pytest.raises(InputValidationError, match="'x'"):
            _ = MergeJoin()(one, other)


def test_nulls_in_both_inputs_merge_in_argument_order() -> None:
    """Nullable colliding columns holding nulls on both sides: each null stays at its
    input's position in the list (ITL-1243)."""
    x_left: list[int | None] = [10, None, 12, 13, 14]
    x_right: list[int | None] = [20, None, None, 23, 24]
    y_left: list[float | None] = [9.5, 8.5, None, 6.5, 5.5]
    y_right: list[float | None] = [None, 2.5, 3.5, None, 0.5]
    left = _side(
        "a",
        LEFT_KEYS,
        LEFT_NUMBERS,
        pa.array(x_left, pa.int32()),
        pa.array(y_left, pa.float64()),
        MIXED,
        ("k", "n"),
    )
    right = _side(
        "b",
        RIGHT_KEYS,
        RIGHT_NUMBERS,
        pa.array(x_right, pa.int32()),
        pa.array(y_right, pa.float64()),
        MIXED,
        ("k", "n"),
    )
    for one, other, first_is_left in ((left, right, True), (right, left, False)):
        for rows in (
            list(MergeJoin()(one, other).iter_data()),
            async_rows(MergeJoin(), one, other),
        ):
            got = {(d["s_a"], d["s_b"]): (d["x"], d["y"]) for _, d in rows}
            assert len(got) == len(MATCHES)
            for i, j in MATCHES:
                x, y = [x_left[i], x_right[j]], [y_left[i], y_right[j]]
                if not first_is_left:
                    x, y = x[::-1], y[::-1]
                assert got[(f"a{i}", f"b{j}")] == (x, y)


@pytest.mark.parametrize(
    ("left_type", "right_type"),
    [(pa.int64(), pa.float64()), (pa.large_string(), pa.int64())],
)
def test_colliding_columns_of_different_python_types_are_refused(
    left_type: pa.DataType, right_type: pa.DataType
) -> None:
    left = _side(
        "a",
        LEFT_KEYS,
        LEFT_NUMBERS,
        pa.array(LEFT_X).cast(left_type),
        pa.array(LEFT_Y, pa.float64()),
        PLAIN,
        ("k", "n"),
    )
    right = _side(
        "b",
        RIGHT_KEYS,
        RIGHT_NUMBERS,
        pa.array(RIGHT_X).cast(right_type),
        pa.array(RIGHT_Y, pa.float64()),
        PLAIN,
        ("k", "n"),
    )
    for one, other in ((left, right), (right, left)):
        with pytest.raises(InputValidationError, match="'x'"):
            _ = MergeJoin()(one, other)


def test_colliding_columns_of_different_arrow_widths_are_refused() -> None:
    """One Python type, two Arrow types: refused when the join runs, both orders."""
    left, _ = _inputs(PLAIN, ("k", "n"))
    right = _side(
        "b",
        RIGHT_KEYS,
        RIGHT_NUMBERS,
        pa.array(RIGHT_X, pa.int64()),
        pa.array(RIGHT_Y, pa.float64()),
        PLAIN,
        ("k", "n"),
    )
    for one, other in ((left, right), (right, left)):
        with pytest.raises(InputValidationError, match="'x'"):
            _ = MergeJoin()(one, other).as_table()


# ── a job stores and reads back ──────────────────────────────────────────────


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
@pytest.mark.parametrize("type_name", ["time64[ns]", "date64 off midnight"])
def test_a_job_stores_and_reads_back_every_value(
    orchestrator: Any, type_name: str
) -> None:
    """The node's rows and its stored records hold the exact values, in argument order."""
    left, right = _typed_inputs(type_name, MIXED)
    expected = _exact_lists(list(MergeJoin()(left, right).iter_data()))
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="merge_exact", database=database)
    with job:
        _ = MergeJoin()(left, right, label="merged")
    job.nodes["merged"].attach_databases(
        pipeline_database=database.at("merge_exact"), cache_mode=CacheMode.LOG
    )
    job.run(orchestrator=orchestrator())
    node: StreamProtocol = job.nodes["merged"]
    assert _exact_lists(list(node.iter_data())) == expected
    stored = job.nodes["merged"].as_source()
    assert _exact_lists(list(stored.iter_data())) == expected
