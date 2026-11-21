"""A column of a type the schema hash rejects is refused when a source or stream is built.

Developer decision, November 21, 2026 (ITL-640): "Definitely refuse at construction",
for unions and list views; the same for run-end-encoded types (coordinator, same day:
one rule for every type the schema hash rejects). A stream's identity is its Arrow
schema hash, and the Arrow schema hash (starfix) cannot hold a union (dense or sparse),
a list view (``list_view``, ``large_list_view``) or a run-end-encoded type, so a stream
with such a column could have no identity. Every column whose Arrow type is, or
contains at any depth, one of them is refused with ``InputValidationError`` naming each
such column with its role and type. The rule is
``schema_utils.schema_hash_rejected_columns`` and
``schema_hash_rejected_columns_error``, beside the dictionary refusal, applied by
``ArrowTableStream`` and first by the source builder: a raw stream and a source refuse
the same inputs with the same message. The list of rejected kinds
(``SCHEMA_HASH_REJECTED_KINDS``) is checked here against starfix's public schema hash.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.utils import schema_utils

NOT_SUPPORTED = schema_utils.SCHEMA_HASH_REJECTED_COLUMNS_NOT_SUPPORTED


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


def _run_end_encoded() -> pa.Array[Any]:
    return pa.RunEndEncodedArray.from_arrays(
        pa.array([2, 3], pa.int32()), pa.array(["a", "b"], pa.large_string())
    )


KINDS = {
    "dense_union": _dense_union,
    "sparse_union": _sparse_union,
    "list_view": _list_view,
    "large_list_view": _large_list_view,
    "run_end_encoded": _run_end_encoded,
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


@pytest.mark.parametrize("order", ["s l r", "r l s"])
def test_three_such_columns_are_all_named(order: str) -> None:
    """A union inside a struct, a list view inside a list and a run-end-encoded column
    inside a map item, in either order."""
    columns = {
        "s": _nest(_sparse_union(), "struct"),
        "l": _nest(_large_list_view(), "large_list"),
        "r": _nest(_run_end_encoded(), "map item"),
    }
    table = _table(**{name: columns[name] for name in order.split()})
    messages = {
        build: _refusal(build, table, ["a", "l", "b"]) for build in ("source", "stream")
    }
    assert messages["source"] == messages["stream"]
    message = messages["stream"]
    # The tag first, then the data columns in table order.
    tag_line = f"- tag column 'l' has type {columns['l'].type}"
    data_lines = [
        f"- data column {name!r} has type {columns[name].type}"
        for name in order.split()
        if name != "l"
    ]
    assert message.index(tag_line) < min(message.index(line) for line in data_lines)
    assert message.index(data_lines[0]) < message.index(data_lines[1])


def _starfix_rejects(arrow_type: pa.DataType) -> bool:
    """Whether starfix's public schema hash refuses a column of ``arrow_type``."""
    from starfix import ArrowDigester

    try:
        ArrowDigester.hash_schema(pa.schema([pa.field("c", arrow_type)]))
    except ValueError as exc:
        if "Unsupported Arrow type for hashing" in str(exc):
            return True
        raise
    return False


_INT = pa.int64()
AGREEMENT: dict[str, pa.DataType] = {
    **{
        str(t): t
        for t in (
            pa.null(),
            pa.bool_(),
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.uint8(),
            pa.uint64(),
            pa.float16(),
            pa.float32(),
            pa.float64(),
            pa.string(),
            pa.large_string(),
            pa.binary(),
            pa.large_binary(),
            pa.binary(3),
            pa.string_view(),
            pa.binary_view(),
            pa.date32(),
            pa.date64(),
            pa.time32("ms"),
            pa.time64("ns"),
            pa.timestamp("us", tz="UTC"),
            pa.duration("s"),
            pa.decimal128(10, 2),
            pa.decimal256(40, 2),
            pa.month_day_nano_interval(),
            pa.list_(_INT),
            pa.large_list(_INT),
            pa.list_(_INT, 2),
            pa.struct([pa.field("a", _INT)]),
            pa.map_(pa.large_string(), _INT),
            pa.dictionary(pa.int32(), pa.string()),
        )
    },
    "dense_union": pa.dense_union([pa.field("a", _INT)]),
    "sparse_union": pa.sparse_union([pa.field("a", _INT)]),
    "list_view": pa.list_view(_INT),
    "large_list_view": pa.large_list_view(_INT),
    "run_end_encoded": pa.run_end_encoded(pa.int32(), _INT),
    "large_list<list_view>": pa.large_list(pa.list_view(_INT)),
    "struct<dense_union>": pa.struct(
        [pa.field("u", pa.dense_union([pa.field("a", _INT)]))]
    ),
    "map<string, run_end_encoded>": pa.map_(
        pa.large_string(),
        pa.run_end_encoded(pa.int32(), _INT),  # pyright: ignore[reportArgumentType]  # the stubs name only basic item types
    ),
    "fixed_size_list<large_list_view>": pa.list_(pa.large_list_view(_INT), 2),
}


@pytest.mark.parametrize("name", list(AGREEMENT))
def test_the_refused_types_are_exactly_those_starfix_rejects(name: str) -> None:
    """A type starfix starts (or stops) rejecting fails here, not in a stream."""
    arrow_type = AGREEMENT[name]
    refused = bool(
        schema_utils.schema_hash_rejected_columns(
            pa.schema([pa.field("c", arrow_type)])
        )
    )
    assert refused == _starfix_rejects(arrow_type)


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
