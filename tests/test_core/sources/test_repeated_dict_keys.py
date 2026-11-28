"""A stored dict with a repeated key is refused, at the source and when read (ITL-907).

Developer decision, November 28, 2026: both refusals. A ``dict[K, V]`` column is stored as
a list of key/value structs, and to Orcapod every list of ``struct<key, value>`` IS a dict
(the type converter's one definition, ``dict_entry_fields``). A table built outside Orcapod
can hold a cell with a repeated key; ``as_dict()`` used to return a dict that had silently
lost an entry while the table and its hash held both.

* A source refuses such a column when it is built (the source builder every source goes
  through), naming the column and the key.
* The conversion of a stored key/value list to a Python ``dict`` raises on a repeated key,
  which covers every route that passes no source (a raw ``ArrowTableStream``).

Two keys are repeated exactly when the Python ``dict`` would keep one of them: ``0.0`` and
``-0.0`` are, two nulls are, two NaN keys are not.
"""

from __future__ import annotations

import asyncio
import math
from typing import Any

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from orcapod import function_pod
from orcapod.channels import Channel
from orcapod.contexts import get_default_context
from orcapod.core.sources import (
    ArrowTableSource,
    DeltaTableSource,
    PollingSource,
)
from orcapod.core.sources import dict_keys
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError, RepeatedDictKeyError
from orcapod.types import Cursor, PollingConfig

KV = pa.struct([("key", pa.large_string()), ("value", pa.int64())])
DICT = pa.large_list(KV)


def _entries(*keys: Any) -> list[dict[str, Any]]:
    """One cell: the entries ``key -> position`` in the order given."""
    return [{"key": key, "value": index} for index, key in enumerate(keys)]


GOOD = [_entries("p", "q"), _entries("q"), _entries()]
# The repeat at the start and at the end of the cell, in the middle row.
REPEATS = {
    "start": [_entries("p"), _entries("a", "a", "b"), _entries("q", "r")],
    "end": [_entries("p"), _entries("b", "c", "a", "a"), _entries("q", "r")],
}
LAYOUTS = {
    "tags_first": ["a", "b", "d", "e", "v", "w"],
    "data_first": ["w", "e", "v", "d", "b", "a"],
}


def _table(
    d: Any, e: Any = None, layout: str = "tags_first", dict_type: pa.DataType = DICT
) -> pa.Table:
    """Tags ``a``, ``b``, data ``v``, ``w`` and the dict columns ``d`` and ``e``."""
    columns = {
        "a": pa.array(["x", "y", "z"], pa.large_string()),
        "b": pa.array([1, 2, 3], pa.int64()),
        "v": pa.array([1.5, 2.5, 3.5], pa.float64()),
        "w": pa.array(["m", "n", "o"], pa.large_string()),
        "d": d if isinstance(d, pa.Array) else pa.array(d, dict_type),
        "e": pa.array(GOOD if e is None else e, DICT),
    }
    names = LAYOUTS[layout]
    return pa.table([columns[name] for name in names], names=names)


def _source(table: pa.Table, role: str = "data", dicts: tuple[str, ...] = ("d",)):
    """An ``ArrowTableSource`` whose columns ``dicts`` have ``role``."""
    if role == "data":
        return ArrowTableSource(table, tag_columns=["b", "a"], tag_only_columns=True)
    return ArrowTableSource(
        table,
        tag_columns=["b", *dicts, "a"],
        tag_only_columns=True if role == "tag" else ["a", "b"],
    )


def _refused(info: pytest.ExceptionInfo[RepeatedDictKeyError], column: str, key: Any):
    error = info.value
    assert isinstance(error, InputValidationError)
    assert isinstance(error, ValueError)
    assert error.column == column
    assert error.key == key
    assert f"Column {column!r}" in str(error)
    assert f"the key {key!r} more than once" in str(error)


# ── at the door: a source ───────────────────────────────────────────────────


@pytest.mark.parametrize("layout", sorted(LAYOUTS))
@pytest.mark.parametrize("where", sorted(REPEATS))
@pytest.mark.parametrize("role", ["tag", "data-tag", "data"])
def test_a_source_refuses_a_dict_column_with_a_repeated_key(
    role: str, where: str, layout: str
) -> None:
    table = _table(REPEATS[where], layout=layout)
    with pytest.raises(RepeatedDictKeyError) as info:
        _source(table, role)
    _refused(info, "d", "a")


@pytest.mark.parametrize("layout", sorted(LAYOUTS))
@pytest.mark.parametrize("role", ["tag", "data-tag", "data"])
def test_a_source_with_two_dict_columns_names_the_one_with_the_repeat(
    role: str, layout: str
) -> None:
    only_e = _table(
        GOOD, [_entries("p"), _entries("q"), _entries("k", "j", "k")], layout
    )
    with pytest.raises(RepeatedDictKeyError) as info:
        _source(only_e, role, ("d", "e"))
    _refused(info, "e", "k")

    both = _table(
        REPEATS["start"], [_entries("k", "k"), _entries(), _entries()], layout
    )
    with pytest.raises(RepeatedDictKeyError) as info:
        _source(both, role, ("d", "e"))
    # The first in name order, whatever the table's layout and the columns' roles.
    _refused(info, "d", "a")


@pytest.mark.parametrize("layout", sorted(LAYOUTS))
@pytest.mark.parametrize("role", ["tag", "data-tag", "data"])
def test_a_dict_column_without_a_repeat_is_unchanged(role: str, layout: str) -> None:
    table = _table(GOOD, [_entries("k", "j"), _entries("j"), _entries("k")], layout)
    source = _source(table, role, ("d", "e"))
    out = source.as_table()
    assert out.column("d").to_pylist() == GOOD
    assert out.column("d").type == DICT
    rows = [{**tag.as_dict(), **data.as_dict()} for tag, data in source.iter_data()]
    assert [row["d"] for row in rows] == [{"p": 0, "q": 1}, {"q": 0}, {}]
    assert [row["e"] for row in rows] == [{"k": 0, "j": 1}, {"j": 0}, {"k": 0}]


VK = pa.struct([("value", pa.int64()), ("key", pa.large_string())])
SHAPES = {
    "large_list": pa.large_list(KV),
    "list": pa.list_(pa.struct([("key", pa.string()), ("value", pa.int64())])),
    "fixed_size_list": pa.list_(KV, 2),
    "value_before_key": pa.large_list(VK),
}


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_every_list_of_key_value_structs_is_a_dict(shape: str) -> None:
    """A list of pairs, in either field order and any list kind, is a dict to Orcapod."""
    cells = [_entries("p", "q"), _entries("a", "a"), _entries("q", "r")]
    table = _table(cells, dict_type=SHAPES[shape])
    with pytest.raises(RepeatedDictKeyError) as info:
        _source(table)
    _refused(info, "d", "a")
    good = [_entries("p", "q"), _entries("a", "b"), _entries("q", "r")]
    rows = [
        d.as_dict()["d"]
        for _, d in _source(_table(good, dict_type=SHAPES[shape])).iter_data()
    ]
    assert rows == [{"p": 0, "q": 1}, {"a": 0, "b": 1}, {"q": 0, "r": 1}]


def test_a_list_of_structs_with_other_field_names_is_not_a_dict() -> None:
    pairs = pa.large_list(
        pa.struct([("name", pa.large_string()), ("value", pa.int64())])
    )
    cells = [[{"name": "a", "value": 1}, {"name": "a", "value": 2}], [], []]
    source = _source(_table(cells, dict_type=pairs))
    assert source.as_table().column("d").to_pylist() == cells


NESTED = {
    # list[dict[str, int]]
    "in_a_list": (
        pa.large_list(DICT),
        [[_entries("p")], [_entries("p", "q"), _entries("b", "a", "a")], []],
        "a",
    ),
    # dict[str, dict[str, int]]
    "as_a_dict_value": (
        pa.large_list(pa.struct([("key", pa.large_string()), ("value", DICT)])),
        [
            [{"key": "o", "value": _entries("p")}],
            [
                {"key": "o", "value": _entries("p", "q")},
                {"key": "u", "value": _entries("a", "b", "a")},
            ],
            [],
        ],
        "a",
    ),
    # A struct holding a dict.
    "in_a_struct": (
        pa.struct([("n", pa.int64()), ("m", DICT)]),
        [
            {"n": 1, "m": _entries("p")},
            {"n": 2, "m": _entries("a", "a")},
            {"n": 3, "m": _entries()},
        ],
        "a",
    ),
    # The outer dict of a dict of dicts.
    "outer_of_nested": (
        pa.large_list(pa.struct([("key", pa.large_string()), ("value", DICT)])),
        [
            [{"key": "o", "value": _entries("p")}],
            [
                {"key": "o", "value": _entries("p", "q")},
                {"key": "o", "value": _entries("a", "b")},
            ],
            [],
        ],
        "o",
    ),
}


@pytest.mark.parametrize("kind", sorted(NESTED))
def test_a_source_refuses_a_repeated_key_at_every_depth(kind: str) -> None:
    arrow_type, cells, key = NESTED[kind]
    with pytest.raises(RepeatedDictKeyError) as info:
        _source(_table(cells, dict_type=arrow_type))
    _refused(info, "d", key)


def test_a_repeat_must_be_inside_one_cell() -> None:
    """The same key in two cells, and in two dicts of one list cell, is no repeat."""
    _source(_table([_entries("a"), _entries("a"), _entries("a")]))
    cells = [[_entries("a"), _entries("a")], [_entries("a", "b")], []]
    _source(_table(cells, dict_type=pa.large_list(DICT)))


def test_a_sliced_and_chunked_column_is_scanned_as_it_is() -> None:
    """Rows outside a slice do not count; a repeat in a later chunk does."""
    whole = _table(
        [_entries("a", "a"), _entries("p"), _entries("q"), _entries("r")][1:]
    )
    _source(whole)
    padded = pa.table(
        {
            "a": pa.array(["w", "x", "y", "z"], pa.large_string()),
            "b": pa.array([0, 1, 2, 3], pa.int64()),
            "v": pa.array([0.5, 1.5, 2.5, 3.5]),
            "d": pa.array(
                [_entries("a", "a"), _entries("p"), _entries("q"), _entries("r")], DICT
            ),
        }
    )
    ArrowTableSource(padded.slice(1), tag_columns=["a", "b"], tag_only_columns=True)
    chunked = pa.concat_tables(
        [padded.slice(1, 2), padded.slice(0, 1).set_column(1, "b", pa.array([9]))]
    )
    with pytest.raises(RepeatedDictKeyError) as info:
        ArrowTableSource(chunked, tag_columns=["a", "b"], tag_only_columns=True)
    _refused(info, "d", "a")


# ── which keys are repeated: what a Python dict would lose ──────────────────


def _keyed(key_type: pa.DataType, cells: list[list[Any]]) -> pa.Array[Any]:
    arrow_type = pa.large_list(pa.struct([("key", key_type), ("value", pa.int64())]))
    return pa.array([_entries(*cell) for cell in cells], arrow_type)


def test_a_repeated_null_key_is_a_repeated_key() -> None:
    with pytest.raises(RepeatedDictKeyError) as info:
        _source(_table(_keyed(pa.large_string(), [["p"], [None, "a", None], []])))
    _refused(info, "d", None)


def test_one_null_key_is_kept_as_before() -> None:
    source = _source(_table(_keyed(pa.large_string(), [["p"], [None, "a"], []])))
    rows = [data.as_dict()["d"] for _, data in source.iter_data()]
    assert rows == [{"p": 0}, {None: 0, "a": 1}, {}]


def test_positive_and_negative_zero_are_one_key() -> None:
    with pytest.raises(RepeatedDictKeyError) as info:
        _source(_table(_keyed(pa.float64(), [[1.0], [0.0, 2.0, -0.0], []])))
    # The key named is the one that repeats an earlier key of its cell.
    _refused(info, "d", -0.0)


@pytest.mark.parametrize("float_type", [pa.float64(), pa.float32(), pa.float16()])
def test_two_nan_keys_are_two_keys(float_type: pa.DataType) -> None:
    """A Python dict keeps both, so nothing is lost and nothing is refused."""
    nan = float("nan")
    source = _source(_table(_keyed(float_type, [[1.0], [nan, 2.0, nan], []])))
    read = [data.as_dict()["d"] for _, data in source.iter_data()][1]
    assert len(read) == 3
    assert sorted(read.values()) == [0, 1, 2]
    assert sum(math.isnan(key) for key in read) == 2


TUPLE = pa.struct([("f0", pa.float64()), ("f1", pa.large_string())])


def test_a_tuple_key_is_compared_field_by_field() -> None:
    def cell(*keys: tuple[float, str | None]) -> list[dict[str, Any]]:
        return [{"f0": f0, "f1": f1} for f0, f1 in keys]

    with pytest.raises(RepeatedDictKeyError) as info:
        _source(
            _table(_keyed(TUPLE, [[], cell((0.0, "a"), (1.0, "a"), (-0.0, "a")), []]))
        )
    _refused(info, "d", (-0.0, "a"))
    with pytest.raises(RepeatedDictKeyError) as info:
        _source(_table(_keyed(TUPLE, [[], cell((1.0, None), (1.0, None)), []])))
    _refused(info, "d", (1.0, None))
    # A NaN anywhere in a key makes it a key of its own, as in Python.
    nan = float("nan")
    source = _source(
        _table(_keyed(TUPLE, [[], cell((nan, "a"), (nan, "a"), (1.0, "b")), []]))
    )
    assert len([data.as_dict()["d"] for _, data in source.iter_data()][1]) == 3


def test_a_key_type_polars_has_no_rule_for_is_compared_in_python() -> None:
    """About the scan itself: no source holds such a key today (the Arrow hasher
    refuses an interval), so the scan is called directly."""
    converter = get_default_context().type_converter
    interval = pa.month_day_nano_interval()
    one = pa.scalar((1, 2, 3), interval).as_py()
    two = pa.scalar((1, 2, 4), interval).as_py()
    repeated = pa.chunked_array([_keyed(interval, [[one], [one, two, one], []])])
    assert dict_keys.holds_dict_type(repeated.type, converter)
    with pytest.raises(RepeatedDictKeyError) as info:
        dict_keys.refuse_repeated_dict_keys(repeated, converter)
    assert info.value.key == one
    assert info.value.column is None
    dict_keys.refuse_repeated_dict_keys(
        pa.chunked_array([_keyed(interval, [[one], [one, two], [one]])]), converter
    )


# ── other source kinds ──────────────────────────────────────────────────────


def test_a_delta_table_source_refuses_a_repeated_key(tmp_path: Any) -> None:
    table = pa.table(
        {
            "a": pa.array(["x", "y"], pa.large_string()),
            "b": pa.array([1, 2], pa.int64()),
            "v": pa.array([1.5, 2.5]),
            "d": pa.array([_entries("p", "q"), _entries("b", "a", "a")], DICT),
        }
    )
    path = str(tmp_path / "repeated")
    write_deltalake(path, table)
    with pytest.raises(RepeatedDictKeyError) as info:
        DeltaTableSource(
            delta_table_path=path, tag_columns=["a", "b"], tag_only_columns=True
        )
    _refused(info, "d", "a")


class _Batches:
    """Fetches a good batch, then one whose dict column repeats a key."""

    def identity(self) -> str:
        return "itl907-batches"

    def to_config(self) -> None:
        return None

    @classmethod
    def from_config(cls, config: Any) -> _Batches:
        return cls()

    def schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("a", pa.large_string()),
                pa.field("b", pa.int64()),
                pa.field("v", pa.float64()),
                pa.field("d", DICT),
            ]
        )

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        return cursor is None or cursor.value < 2

    async def fetch(self, cursor: Cursor[int] | None = None) -> tuple[Cursor[int], Any]:
        batch = 1 if cursor is None else cursor.value + 1
        cell = _entries("p", "q") if batch == 1 else _entries("a", "b", "a")
        table = pa.Table.from_pylist(
            [{"a": f"x{batch}", "b": batch, "v": 1.5, "d": cell}], schema=self.schema()
        )
        return Cursor(value=batch), table

    async def close(self) -> None:
        return None


def test_a_polling_source_refuses_a_repeated_key_in_a_later_batch() -> None:
    source = PollingSource(
        _Batches(),
        tag_columns=["a", "b"],
        record_id_columns="a",
        polling_config=PollingConfig(duration=0.01),
    )
    first = source.as_table()
    assert first.column("d").to_pylist() == [_entries("p", "q")]
    with pytest.raises(RepeatedDictKeyError) as info:
        source.as_table()
    _refused(info, "d", "a")


# ── the cost: only dict-typed columns are scanned ───────────────────────────


def test_only_columns_holding_a_dict_type_are_scanned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scanned: list[pa.DataType] = []
    scan = dict_keys.refuse_repeated_dict_keys

    def counted(column: Any, converter: Any) -> None:
        scanned.append(column.type)
        scan(column, converter)

    monkeypatch.setattr(dict_keys, "refuse_repeated_dict_keys", counted)
    plain = pa.table(
        {
            "a": pa.array(["x", "y"], pa.large_string()),
            "b": pa.array([1, 2], pa.int64()),
            "v": pa.array([[1, 2], [3]], pa.large_list(pa.int64())),
            "w": pa.array([{"key": "k", "n": 1}] * 2),
            "pairs": pa.array(
                [[{"name": "k", "value": 1}]] * 2,
                pa.large_list(
                    pa.struct([("name", pa.string()), ("value", pa.int64())])
                ),
            ),
        }
    )
    ArrowTableSource(plain, tag_columns=["a", "b"], tag_only_columns=True)
    assert scanned == []
    _source(_table(GOOD))
    assert scanned == [DICT, DICT]


# ── at the read: no source in the way ───────────────────────────────────────


def _stream(table: pa.Table, role: str = "data") -> ArrowTableStream:
    if role == "data":
        return ArrowTableStream(table, tag_columns=["b", "a"])
    return ArrowTableStream(
        table,
        tag_columns=["b", "d", "a"],
        data_tags=("d",) if role == "data-tag" else (),
    )


@pytest.mark.parametrize("layout", sorted(LAYOUTS))
@pytest.mark.parametrize("where", sorted(REPEATS))
@pytest.mark.parametrize("role", ["tag", "data-tag", "data"])
def test_a_raw_stream_keeps_the_stored_value_and_refuses_it_as_a_dict(
    role: str, where: str, layout: str
) -> None:
    stream = _stream(_table(REPEATS[where], layout=layout), role)
    # No value scan when a stream is built: the stored value is what was given.
    assert stream.as_table().column("d").to_pylist() == REPEATS[where]
    rows = list(stream.iter_data())
    for index, (tag, data) in enumerate(rows):
        holder = tag if role != "data" else data
        if index != 1:
            assert isinstance(holder.as_dict()["d"], dict)
            continue
        with pytest.raises(RepeatedDictKeyError) as info:
            holder.as_dict()
        _refused(info, "d", "a")


@pytest.mark.parametrize("kind", sorted(NESTED))
def test_the_read_refuses_a_repeated_key_at_every_depth(kind: str) -> None:
    arrow_type, cells, key = NESTED[kind]
    rows = list(_stream(_table(cells, dict_type=arrow_type)).iter_data())
    with pytest.raises(RepeatedDictKeyError) as info:
        rows[1][1].as_dict()
    _refused(info, "d", key)


def test_the_read_follows_what_a_python_dict_would_lose() -> None:
    nan = float("nan")

    def second(array: pa.Array[Any]) -> Any:
        return list(_stream(_table(array)).iter_data())[1][1]

    with pytest.raises(RepeatedDictKeyError) as info:
        second(_keyed(pa.large_string(), [["p"], [None, "a", None], []])).as_dict()
    _refused(info, "d", None)
    with pytest.raises(RepeatedDictKeyError) as info:
        second(_keyed(pa.float64(), [[1.0], [0.0, 2.0, -0.0], []])).as_dict()
    _refused(info, "d", -0.0)
    assert (
        len(second(_keyed(pa.float64(), [[1.0], [nan, nan], []])).as_dict()["d"]) == 2
    )
    assert second(_keyed(pa.large_string(), [["p"], [None, "a"], []])).as_dict()[
        "d"
    ] == {
        None: 0,
        "a": 1,
    }


@function_pod(output_keys="size")
def _size(d: dict[str, int]) -> int:
    return len(d)


def _pod_input(where: str) -> ArrowTableStream:
    table = _table(REPEATS[where]).select(["a", "b", "d"])
    # The function takes ``dict[str, int]``, not ``dict[str, int] | None``.
    table = table.cast(
        table.schema.set(2, table.schema.field("d").with_nullable(False))
    )
    return ArrowTableStream(table, tag_columns=["b", "a"])


@pytest.mark.parametrize("where", sorted(REPEATS))
def test_a_function_pod_reading_the_dict_raises_on_the_sync_path(where: str) -> None:
    with pytest.raises(RepeatedDictKeyError) as info:
        _size.pod(_pod_input(where)).as_table()
    _refused(info, "d", "a")


@pytest.mark.parametrize("where", sorted(REPEATS))
def test_a_function_pod_reading_the_dict_raises_on_the_async_path(where: str) -> None:
    async def run() -> None:
        stream = _pod_input(where)
        inputs: Channel[Any] = Channel(buffer_size=16)
        output: Channel[Any] = Channel(buffer_size=16)
        for row in stream.iter_data():
            await inputs.writer.send(row)
        await inputs.writer.close()
        await _size.pod.async_execute(
            [inputs.reader], output.writer, error_policy="fail_fast"
        )

    # The pod's task group reports the row's error in a group.
    with pytest.raises(ExceptionGroup) as group:
        asyncio.run(run())
    (error,) = group.value.exceptions
    assert isinstance(error, RepeatedDictKeyError)
    assert (error.column, error.key) == ("d", "a")
