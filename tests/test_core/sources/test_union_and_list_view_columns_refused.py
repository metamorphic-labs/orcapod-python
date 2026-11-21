"""A union or list-view column is refused when a source or stream is built (ITL-640).

Developer decision, November 21, 2026: "Definitely refuse at construction". A stream's
identity is its Arrow schema hash, and the Arrow schema hash cannot hold a union (dense
or sparse) or a list view (``list_view``, ``large_list_view``), so a stream with such a
column could have no identity. Every column whose Arrow type is, or contains at any depth,
one of them is refused with ``InputValidationError`` naming each such column with its
role and type. The rule is ``schema_utils.union_or_list_view_columns`` and
``union_or_list_view_columns_error``, beside the dictionary refusal, applied by
``ArrowTableStream`` and first by the source builder: a raw stream and a source refuse the
same inputs with the same message.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.utils import schema_utils

NOT_SUPPORTED = schema_utils.UNION_OR_LIST_VIEW_COLUMNS_NOT_SUPPORTED


def _dense_union() -> pa.Array[Any]:
    return pa.UnionArray.from_dense(
        pa.array([0, 1, 0], pa.int8()),
        pa.array([0, 0, 1], pa.int32()),
        [pa.array([1, 2], pa.int64()), pa.array(["a"], pa.large_string())],
    )


def _sparse_union() -> pa.Array[Any]:
    return pa.UnionArray.from_sparse(
        pa.array([0, 1, 0], pa.int8()),
        [pa.array([1, 2, 3], pa.int64()), pa.array(["a", "b", "c"], pa.large_string())],
    )


def _list_view() -> pa.Array[Any]:
    return pa.array([[1], [2, 3], None], pa.list_view(pa.int64()))


def _large_list_view() -> pa.Array[Any]:
    return pa.array([[1], [2, 3], None], pa.large_list_view(pa.int64()))


KINDS = {
    "dense_union": _dense_union,
    "sparse_union": _sparse_union,
    "list_view": _list_view,
    "large_list_view": _large_list_view,
}


def _nest(array: pa.Array[Any], how: str) -> pa.Array[Any]:
    """``array`` (three rows) at the top of a column or inside a container."""
    if how == "top":
        return array
    if how == "struct":
        return pa.StructArray.from_arrays([array], names=["inner"])
    if how == "large_list":
        offsets = pa.array([0, 1, 2, 3], pa.int64())
        return pa.LargeListArray.from_arrays(offsets, array)
    if how == "fixed_size_list":
        return pa.FixedSizeListArray.from_arrays(array, 1)
    if how == "map item":
        keys = pa.array(["k1", "k2", "k3"], pa.large_string())
        offsets = pa.array([0, 1, 2, 3], pa.int32())
        return pa.MapArray.from_arrays(offsets, keys, array)  # pyright: ignore[reportArgumentType]  # the stubs type map offsets as Int64Array; a map takes int32
    raise ValueError(how)


NESTINGS = ["top", "struct", "large_list", "fixed_size_list", "map item"]


def _table(**special: pa.Array[Any]) -> pa.Table:
    """Tags ``a``, ``b``; data ``v``, ``w``; plus the special columns given."""
    columns: dict[str, pa.Array[Any]] = {
        "a": pa.array(["x", "y", "z"], pa.large_string()),
        "b": pa.array([1, 2, 3], pa.int64()),
        "v": pa.array([0.5, 1.5, 2.5], pa.float64()),
        "w": pa.array(["p", "q", "r"], pa.large_string()),
    }
    columns.update(special)
    return pa.Table.from_arrays(list(columns.values()), names=list(columns))


def _refusal(build: str, table: pa.Table, tags: list[str]) -> str:
    with pytest.raises(InputValidationError) as info:
        if build == "source":
            ArrowTableSource(table, tag_columns=tags, tag_only_columns=True)
        else:
            ArrowTableStream(table, tag_columns=tags)
    return str(info.value)


@pytest.mark.parametrize("how", NESTINGS)
@pytest.mark.parametrize("kind", list(KINDS))
@pytest.mark.parametrize("role", ["tag", "data"])
def test_a_source_and_a_raw_stream_refuse_it_alike(
    kind: str, how: str, role: str
) -> None:
    column = _nest(KINDS[kind](), how)
    table = _table(u=column)
    tags = ["a", "b", "u"] if role == "tag" else ["a", "b"]
    messages = {build: _refusal(build, table, tags) for build in ("source", "stream")}
    assert messages["source"] == messages["stream"]
    message = messages["stream"]
    assert NOT_SUPPORTED in message
    assert f"- {role} column 'u' has type {column.type}" in message
    for plain in ("a", "b", "v", "w"):
        assert f"column {plain!r}" not in message


@pytest.mark.parametrize("order", ["s l", "l s"])
def test_two_such_columns_are_both_named(order: str) -> None:
    """A union inside a struct and a list view inside a list, in either order."""
    columns = {
        "s": _nest(_sparse_union(), "struct"),
        "l": _nest(_large_list_view(), "large_list"),
    }
    table = _table(**{name: columns[name] for name in order.split()})
    messages = {
        build: _refusal(build, table, ["a", "l", "b"]) for build in ("source", "stream")
    }
    assert messages["source"] == messages["stream"]
    message = messages["stream"]
    # The tag first, then the data columns in table order.
    tag_line = f"- tag column 'l' has type {columns['l'].type}"
    data_line = f"- data column 's' has type {columns['s'].type}"
    assert message.index(tag_line) < message.index(data_line)


def test_a_table_without_them_builds() -> None:
    """Large lists of the same items (the message's remedy) are accepted."""
    remedied = _list_view().cast(pa.large_list(pa.int64()))
    table = _table(u=remedied)
    assert ArrowTableStream(table, tag_columns=["a", "b"]).as_table().num_rows == 3
    source = ArrowTableSource(table, tag_columns=["a", "b"], tag_only_columns=True)
    assert source.as_table().num_rows == 3


def test_it_is_refused_before_anything_is_hashed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The source builder refuses it first, before its schema or table hash."""
    from orcapod.contexts import get_default_context

    def fail(*args: object, **kwargs: object) -> object:
        raise AssertionError("hashed")

    hasher = get_default_context().arrow_hasher
    monkeypatch.setattr(type(hasher), "hash_schema", fail)
    monkeypatch.setattr(type(hasher), "hash_table", fail)
    message = _refusal("source", _table(u=_dense_union()), ["a", "b"])
    assert NOT_SUPPORTED in message
