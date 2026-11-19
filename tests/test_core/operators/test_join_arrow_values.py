"""``Join`` and ``SemiJoin`` emit every value of their input bit for bit (ITL-1284).

The sync path used to carry every column through Polars and back: a ``date64`` value
off midnight came back at midnight (as data, as a key, in a list, in a struct), and a
``decimal256`` column made Polars panic (an error no caller can catch). Now Polars only
decides which rows match, from the prepared keys (``arrow_keys``) and row numbers, and
every output column is taken from the input tables.

Each type below is a key and a data column at once, with a second key and a second
data column (an ``int64`` tag, a ``large_string`` data column), in plain-tag, data-tag
and mixed roles and in two tag orders, on the sync and the async path, for two and
three ``Join`` inputs and for ``SemiJoin``. Keys repeat on both sides (many to many),
and a null key matches nothing. Values are compared exactly: temporal values as their
integers, floats as their bits, decimals as text, logical types through their storage,
lists and structs element by element.
"""

from __future__ import annotations

import decimal
import uuid
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, TypeAlias, cast

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.core.operators import Join, SemiJoin
from orcapod.core.operators.arrow_keys import (
    part_without_rule,
    polars_key_frame,
    polars_ready,
)
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol

from .several_items import async_rows, ordered_rows_fingerprint

Column: TypeAlias = "pa.Array[Any]"
Row: TypeAlias = "tuple[TagProtocol, DataProtocol]"
D = decimal.Decimal
CONTEXT = get_default_context()

PLAIN, DATA_TAGS, MIXED = "plain", "data_tags", "mixed"
ROLES = (PLAIN, DATA_TAGS, MIXED)


# ── exact values ─────────────────────────────────────────────────────────────


def _exact(array: "Column | pa.ChunkedArray[Any]") -> list[Any]:
    """Return a column's values exactly, as plain Python values."""
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()
    if isinstance(array, pa.ExtensionArray):
        return _exact(array.storage)
    arrow_type = array.type
    if pa.types.is_large_list(arrow_type) or pa.types.is_list(arrow_type):
        lists = cast("pa.LargeListArray[Any]", array)
        values = _exact(lists.values)
        offsets = cast(list[int], lists.offsets.to_pylist())
        return [
            values[offsets[i] : offsets[i + 1]] if lists[i].is_valid else None
            for i in range(len(lists))
        ]
    if pa.types.is_fixed_size_list(arrow_type):
        return _exact(array.cast(pa.large_list(arrow_type.value_type)))
    if pa.types.is_struct(arrow_type):
        struct = cast("pa.StructArray", array)
        fields = [_exact(struct.field(i)) for i in range(arrow_type.num_fields)]
        return [
            tuple(f[j] for f in fields) if struct[j].is_valid else None
            for j in range(len(struct))
        ]
    if pa.types.is_floating(arrow_type):
        unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}
        return array.view(unsigned[arrow_type.bit_width]).to_pylist()
    if pa.types.is_map(arrow_type) or pa.types.is_interval(arrow_type):
        return [repr(value) for value in array.to_pylist()]
    if pa.types.is_temporal(arrow_type):
        width = cast(int, arrow_type.bit_width)
        return array.view(pa.int32() if width == 32 else pa.int64()).to_pylist()
    if pa.types.is_decimal(arrow_type):
        return [None if value is None else str(value) for value in array.to_pylist()]
    return array.to_pylist()


def _ints(values: list[int | None], arrow_type: pa.DataType) -> Column:
    return pa.array(values, pa.int64()).cast(arrow_type)


def _floats(bits: list[int | None], width: int) -> Column:
    unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[width]
    floating = {16: pa.float16(), 32: pa.float32(), 64: pa.float64()}[width]
    return pa.array(bits, unsigned).view(floating)


def _dec(units: int, tiny: int) -> D:
    """``units`` plus ``tiny`` at the 40th decimal place, built from text."""
    sign = "-" if units < 0 else ""
    return D(f"{sign}{abs(units)}.{tiny:040d}")


def _logical(python_type: type, storage: list[Any]) -> Column:
    arrow_type = cast(
        pa.BaseExtensionType,
        CONTEXT.type_converter.python_type_to_arrow_type(python_type),
    )
    return pa.ExtensionArray.from_storage(
        arrow_type, pa.array(storage, arrow_type.storage_type)
    )


def _list_of(values: Column) -> Column:
    """One one-element large list per value; a null value gives a null list."""
    offsets = pa.array(range(len(values) + 1), pa.int64())
    return pa.LargeListArray.from_arrays(offsets, values, mask=values.is_null())


DEC256 = pa.decimal256(76, 40)


def _date64(values: list[int | None]) -> Column:
    return _ints(values, pa.date64())


def _decimal256(values: list[tuple[int, int] | None]) -> Column:
    return pa.array([None if v is None else _dec(*v) for v in values], DEC256)


# Each builder gives four distinct values for a list of indices 0 to 3 (``None``: null).
Builder: TypeAlias = Callable[[list[int | None]], Column]


def _pick(values: list[Any]) -> Callable[[list[int | None]], list[Any]]:
    return lambda indices: [None if i is None else values[i] for i in indices]


DATE64 = _pick([1, 86_400_001, 2, 86_400_002])
DECIMALS = _pick([(1, 1), (1, 2), (-3, 1), (0, 7)])

AFFECTED: dict[str, Builder] = {
    "date64 off midnight": lambda ix: _date64(DATE64(ix)),
    "decimal256(76,40)": lambda ix: _decimal256(DECIMALS(ix)),
    "large_list<date64>": lambda ix: _list_of(_date64(DATE64(ix))),
    "large_list<decimal256>": lambda ix: _list_of(_decimal256(DECIMALS(ix))),
    "struct<a: date64, b: decimal256>": lambda ix: pa.StructArray.from_arrays(
        [_date64(DATE64(ix)), _decimal256(DECIMALS(ix))],
        names=["a", "b"],
        mask=pa.array([i is None for i in ix], pa.bool_()),
    ),
    "fixed_size_list<decimal256, 1>": lambda ix: pa.FixedSizeListArray.from_arrays(
        _decimal256([DECIMALS(ix)[k] or (0, 0) for k in range(len(ix))]),
        1,
        mask=pa.array([i is None for i in ix], pa.bool_()),
    ),
}

EXACT_ON_MAIN: dict[str, Builder] = {
    "time64[ns]": lambda ix: _ints(
        _pick([1, 1001, 2, 1_000_000_001])(ix), pa.time64("ns")
    ),
    "float32 signaling NaN": lambda ix: _floats(
        _pick([0x7F800001, 0x3F800000, 0x40000000, 0xC0000000])(ix), 32
    ),
    "float16 -0.0": lambda ix: _floats(_pick([0x8000, 0x3C00, 0x4000, 0x7E01])(ix), 16),
    "uint64 max": lambda ix: pa.array(_pick([2**64 - 1, 0, 1, 2**63])(ix), pa.uint64()),
    "fixed_size_binary(3)": lambda ix: pa.array(
        _pick([b"abc", b"\x00\x00\x00", b"abd", b"\xff\x00\x01"])(ix), pa.binary(3)
    ),
    "timestamp[ns, America/New_York]": lambda ix: _ints(
        _pick([1, 1001, -5, 1_700_000_000_000_000_001])(ix),
        pa.timestamp("ns", "America/New_York"),
    ),
    "duration[ns]": lambda ix: _ints(_pick([1, 1001, -1, 7])(ix), pa.duration("ns")),
    "decimal128(38,10)": lambda ix: pa.array(
        _pick([D("1.0000000001"), D("-2"), D("0.0000000002"), D("3")])(ix),
        pa.decimal128(38, 10),
    ),
    "logical Path": lambda ix: _logical(Path, _pick(["a", "b", "c", "é"])(ix)),
    "logical UUID": lambda ix: _logical(
        uuid.UUID, _pick([uuid.UUID(int=n).bytes for n in (1, 2, 3, 4)])(ix)
    ),
}

TYPES = {**AFFECTED, **EXACT_ON_MAIN}
TYPE = pytest.mark.parametrize("type_name", list(TYPES))


# ── inputs ───────────────────────────────────────────────────────────────────


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
        table,
        tag_columns=list(tags),
        source_id=source_id,
        tag_only_columns=tag_only,
    )


def _side(
    build: Builder,
    keys: list[int | None],
    numbers: list[int],
    name: str,
    roles: str,
    tag_order: tuple[str, ...],
    values: list[int | None],
) -> ArrowTableSource:
    """One input: tags ``k`` (the type) and ``n`` (``int64``); data of the type and
    a ``large_string``, named after the input so no two inputs share a data column.

    The table interleaves tags and data, the tags in ``tag_order``. (A table whose
    tags are laid out in another order than ``tag_columns`` gives the sync and the
    async path different tag column orders, on ``main`` too: not this issue.)"""
    tag_values = {"k": build(keys), "n": pa.array(numbers, pa.int64())}
    columns = {
        f"x_{name}": build(values),
        tag_order[0]: tag_values[tag_order[0]],
        f"s_{name}": pa.array(
            [f"{name}{i}" for i in range(len(keys))], pa.large_string()
        ),
        tag_order[1]: tag_values[tag_order[1]],
    }
    return _source(columns, tag_order, roles, name)


# Left rows: (k, n); the data of each row is its own index, so each output row shows
# which input rows it came from. Keys repeat on both sides: many to many.
LEFT_KEYS: list[int | None] = [0, 1, 1, 2, None]
LEFT_NUMBERS = [1, 1, 1, 2, 1]
RIGHT_KEYS: list[int | None] = [1, 3, 1, 2, None]
RIGHT_NUMBERS = [1, 1, 1, 2, 1]
# (left row, right row) pairs that match: k and n equal, never on a null key.
MATCHES = {(1, 0), (1, 2), (2, 0), (2, 2), (3, 3)}

TAG_ORDERS = (("k", "n"), ("n", "k"))


def _left(build: Builder, roles: str, tag_order: tuple[str, ...]) -> ArrowTableSource:
    values: list[int | None] = [0, 1, 2, 3, 0]
    return _side(build, LEFT_KEYS, LEFT_NUMBERS, "l", roles, tag_order, values)


def _right(build: Builder, roles: str, tag_order: tuple[str, ...]) -> ArrowTableSource:
    values: list[int | None] = [3, 2, 1, 0, 1]
    return _side(build, RIGHT_KEYS, RIGHT_NUMBERS, "r", roles, tag_order, values)


def _third(build: Builder, roles: str) -> ArrowTableSource:
    """A third ``Join`` input: tag ``k`` only, rows for key values 1 and 2."""
    columns = {
        "k": build([1, 2, 1]),
        "x_t": build([2, 3, 0]),
        "s_t": pa.array(["t0", "t1", "t2"], pa.large_string()),
    }
    return _source(columns, ("k",), roles, "t")


def _rows_of(stream: StreamProtocol) -> list[Row]:
    return list(stream.iter_data())


def _exact_rows(rows: Sequence[Row], names: Sequence[str]) -> list[tuple[str, ...]]:
    """Each row's values of ``names``, exactly, read from its Arrow tables; sorted."""
    out: list[tuple[str, ...]] = []
    for tag, data in rows:
        tag_table, data_table = tag.as_table(), data.as_table()
        out.append(
            tuple(
                repr(
                    _exact(
                        (
                            tag_table if name in tag_table.column_names else data_table
                        ).column(name)
                    )
                )
                for name in names
            )
        )
    return sorted(out)


# ── Join, two inputs ─────────────────────────────────────────────────────────


@TYPE
@pytest.mark.parametrize("roles", ROLES)
@pytest.mark.parametrize("tag_order", TAG_ORDERS)
def test_join_emits_every_value_exactly(
    type_name: str, roles: str, tag_order: tuple[str, ...]
) -> None:
    """Keys and data of the type, on both inputs, come out bit for bit, sync and
    async; the async rows equal the sync rows."""
    build = TYPES[type_name]
    left, right = _left(build, roles, tag_order), _right(build, roles, tag_order)
    names = ["k", "n", "x_l", "s_l", "x_r", "s_r"]
    left_values: list[int | None] = [0, 1, 2, 3, 0]
    right_values: list[int | None] = [3, 2, 1, 0, 1]
    want = sorted(
        (
            repr(_exact(build([LEFT_KEYS[li]]))),
            repr([LEFT_NUMBERS[li]]),
            repr(_exact(build([left_values[li]]))),
            repr([f"l{li}"]),
            repr(_exact(build([right_values[ri]]))),
            repr([f"r{ri}"]),
        )
        for li, ri in MATCHES
    )
    sync = _rows_of(Join()(left, right))
    asynchronous = async_rows(Join(), left, right)
    assert _exact_rows(sync, names) == want
    assert _exact_rows(asynchronous, names) == want
    assert ordered_rows_fingerprint(asynchronous) == ordered_rows_fingerprint(sync)


@TYPE
def test_join_table_columns_are_the_inputs_values(type_name: str) -> None:
    """The sync table itself (not only its rows) holds the input's values and types."""
    build = TYPES[type_name]
    left, right = _left(build, PLAIN, ("k", "n")), _right(build, PLAIN, ("k", "n"))
    table = Join()(left, right).as_table()
    assert table.schema.field("k").type == left.as_table().schema.field("k").type
    assert table.schema.field("x_l").type == left.as_table().schema.field("x_l").type
    assert table.schema.field("x_r").type == right.as_table().schema.field("x_r").type
    assert table.num_rows == len(MATCHES)
    assert sorted(map(repr, _exact(table.column("x_l")))) == sorted(
        repr(_exact(build([[0, 1, 2, 3, 0][li]]))[0]) for li, _ in MATCHES
    )


@TYPE
@pytest.mark.parametrize("roles", ROLES)
def test_three_inputs_join_exactly_in_every_order(type_name: str, roles: str) -> None:
    """Three inputs, in two argument orders: one result, every value exact, sync and
    async alike."""
    build = TYPES[type_name]
    left, right = _left(build, roles, ("k", "n")), _right(build, roles, ("n", "k"))
    third = _third(build, roles)
    names = ["k", "n", "x_l", "s_l", "x_r", "s_r", "x_t", "s_t"]
    results = []
    for order in ((left, right, third), (third, right, left)):
        sync = _rows_of(Join()(*order))
        asynchronous = async_rows(Join(), *order)
        assert _exact_rows(asynchronous, names) == _exact_rows(sync, names)
        assert ordered_rows_fingerprint(asynchronous) == ordered_rows_fingerprint(sync)
        results.append(_exact_rows(sync, names))
    assert results[0] == results[1]
    # Third rows: k=1 (twice: t0, t2) and k=2 (t1). Left/right pairs on k=1 are four,
    # on k=2 one: 4 * 2 + 1 * 1 rows.
    assert len(results[0]) == 9
    third_keys = {0: 1, 1: 2, 2: 1}
    want_x_t = sorted(
        repr(_exact(build([[2, 3, 0][t]])))
        for li, _ in MATCHES
        for t in third_keys
        if third_keys[t] == LEFT_KEYS[li]
    )
    assert sorted(row[6] for row in results[0]) == want_x_t


# ── SemiJoin ─────────────────────────────────────────────────────────────────


@TYPE
@pytest.mark.parametrize("roles", ROLES)
@pytest.mark.parametrize("tag_order", TAG_ORDERS)
def test_semi_join_emits_every_value_exactly(
    type_name: str, roles: str, tag_order: tuple[str, ...]
) -> None:
    """The kept left rows are the input's, bit for bit, in the left input's order;
    the async rows equal the sync rows."""
    build = TYPES[type_name]
    left, right = _left(build, roles, tag_order), _right(build, roles, tag_order)
    kept = sorted({li for li, _ in MATCHES})
    stream = SemiJoin()(left, right)
    table = stream.as_table()
    want = left.as_table().take(kept)
    for name in ("k", "n", "x_l", "s_l"):
        assert table.schema.field(name) == want.schema.field(name)
        assert _exact(table.column(name)) == _exact(want.column(name))
    names = ["k", "n", "x_l", "s_l"]
    sync = _rows_of(stream)
    asynchronous = async_rows(SemiJoin(), left, right)
    assert _exact_rows(asynchronous, names) == _exact_rows(sync, names)
    assert ordered_rows_fingerprint(asynchronous) == ordered_rows_fingerprint(sync)


# ── null keys, no match, zero rows ───────────────────────────────────────────


@TYPE
@pytest.mark.parametrize("operator", [Join, SemiJoin])
def test_a_null_key_matches_nothing(
    type_name: str, operator: Callable[[], Join | SemiJoin]
) -> None:
    """Row 4 of each side has a null ``k`` and equal ``n``: it matches nothing."""
    build = TYPES[type_name]
    left, right = _left(build, PLAIN, ("k", "n")), _right(build, PLAIN, ("k", "n"))
    for rows in (
        _rows_of(operator()(left, right)),
        async_rows(operator(), left, right),
    ):
        assert all(tag.as_table().column("k").null_count == 0 for tag, _ in rows)
        assert "['l4']" not in {
            repr(_exact(data.as_table().column("s_l"))) for _, data in rows
        }


@TYPE
@pytest.mark.parametrize("operator", [Join, SemiJoin])
def test_no_match_keeps_the_types(
    type_name: str, operator: Callable[[], Join | SemiJoin]
) -> None:
    """No key matches: zero rows, with the inputs' Arrow types."""
    build = TYPES[type_name]
    left = _side(build, [0, 1], [1, 1], "l", PLAIN, ("k", "n"), [0, 1])
    right = _side(build, [2, 3], [1, 1], "r", PLAIN, ("k", "n"), [2, 3])
    table = operator()(left, right).as_table()
    assert table.num_rows == 0
    for name in ("k", "x_l"):
        assert table.schema.field(name).type == left.as_table().schema.field(name).type
    assert async_rows(operator(), left, right) == []


@TYPE
@pytest.mark.parametrize("operator", [Join, SemiJoin])
def test_zero_row_inputs_keep_the_types(
    type_name: str, operator: Callable[[], Join | SemiJoin]
) -> None:
    """Inputs with no rows give zero rows with the inputs' Arrow types."""
    build = TYPES[type_name]

    def empty(name: str) -> ArrowTableStream:
        table = pa.Table.from_arrays(
            [
                build([]),
                pa.array([], pa.int64()),
                build([]),
                pa.array([], pa.large_string()),
            ],
            names=["k", "n", f"x_{name}", f"s_{name}"],
        )
        return ArrowTableStream(table, tag_columns=["k", "n"])

    left, right = empty("l"), empty("r")
    table = operator()(left, right).as_table()
    assert table.num_rows == 0
    for name in ("k", "x_l"):
        assert table.schema.field(name).type == left.as_table().schema.field(name).type


# ── key types with no rule ───────────────────────────────────────────────────

NO_RULE: dict[str, Column] = {
    "map<large_string, int64>": pa.array(
        [[("a", 1)], [("b", 2)]], pa.map_(pa.large_string(), pa.int64())
    ),
    "month_day_nano_interval": pa.array(
        [(1, 2, 3), (4, 5, 6)], pa.month_day_nano_interval()
    ),
    "dense_union": pa.UnionArray.from_dense(
        pa.array([0, 1], pa.int8()),
        pa.array([0, 0], pa.int32()),
        [pa.array([1], pa.int64()), pa.array(["a"], pa.large_string())],
    ),
    "large_list<map>": _list_of(
        pa.array([[("a", 1)], [("b", 2)]], pa.map_(pa.large_string(), pa.int64()))
    ),
    "struct<a: month_day_nano_interval>": pa.StructArray.from_arrays(
        [pa.array([(1, 2, 3), (4, 5, 6)], pa.month_day_nano_interval())], names=["a"]
    ),
}
NO_RULE_CASE = pytest.mark.parametrize("type_name", list(NO_RULE))


def _no_rule_streams(type_name: str) -> tuple[ArrowTableStream, ArrowTableStream]:
    """Two raw streams sharing tags ``n`` and ``k`` (the type with no rule)."""
    key = NO_RULE[type_name]
    numbers = pa.array([1, 2], pa.int64())

    def side(data: str) -> ArrowTableStream:
        table = pa.Table.from_arrays(
            [numbers, key, pa.array([0, 1], pa.int64()), pa.array(["a", "b"])],
            names=["n", "k", data, f"s_{data}"],
        )
        return ArrowTableStream(table, tag_columns=["n", "k"])

    return side("x"), side("y")


@NO_RULE_CASE
@pytest.mark.parametrize("operator", [Join, SemiJoin])
def test_a_key_type_with_no_rule_is_refused_naming_the_column(
    type_name: str, operator: Callable[[], Join | SemiJoin]
) -> None:
    """Refused by the sync path before rows are matched, with an
    ``InputValidationError`` naming the column and its type; also when the stream
    is read with no rows (``head()``'s zero-row tables go through the same path)."""
    left, right = _no_rule_streams(type_name)
    name = type(operator()).__name__
    with pytest.raises(
        InputValidationError, match=f"{name} cannot match rows on the tag 'k'"
    ):
        _ = operator()(left, right).as_table()
    no_rows = [
        ArrowTableStream(stream.as_table().slice(0, 0), tag_columns=["n", "k"])
        for stream in (left, right)
    ]
    with pytest.raises(InputValidationError, match="'k'"):
        _ = operator()(*no_rows).as_table()


@NO_RULE_CASE
@pytest.mark.parametrize("operator", [Join, SemiJoin])
def test_a_key_type_with_no_rule_is_refused_on_the_async_path(
    type_name: str, operator: Callable[[], Join | SemiJoin]
) -> None:
    """The async path, which validation does not precede here, refuses it too."""
    left, right = _no_rule_streams(type_name)
    with pytest.raises((InputValidationError, ExceptionGroup)) as caught:
        _ = async_rows(operator(), left, right)
    error: BaseException = caught.value
    while isinstance(error, BaseExceptionGroup) and len(error.exceptions) == 1:
        error = error.exceptions[0]
    assert isinstance(error, InputValidationError)
    assert "'k'" in str(error)


@NO_RULE_CASE
@pytest.mark.parametrize("operator", [Join, SemiJoin])
def test_a_data_column_of_a_type_with_no_rule_passes_through(
    type_name: str, operator: Callable[[], Join | SemiJoin]
) -> None:
    """Only keys reach Polars: a data column of any type is taken as it is."""
    column = NO_RULE[type_name]
    numbers = pa.array([1, 2], pa.int64())
    left = ArrowTableStream(
        pa.Table.from_arrays(
            [numbers, column, pa.array(["a", "b"])], names=["n", "x", "s"]
        ),
        tag_columns=["n"],
    )
    right = ArrowTableStream(
        pa.Table.from_arrays([numbers, pa.array([5, 6], pa.int64())], names=["n", "y"]),
        tag_columns=["n"],
    )
    table = operator()(left, right).as_table()
    assert table.schema.field("x").type == column.type
    assert _exact(table.column("x")) == _exact(column)
    rows = async_rows(operator(), left, right)
    assert [repr(_exact(d.as_table().column("x"))) for _, d in rows] == [
        repr(_exact(column.slice(i, 1))) for i in range(2)
    ]


# ── the preparation itself ───────────────────────────────────────────────────

PREPARED_TYPES: dict[str, Column] = {
    **{name: build([0, 1, None, 0]) for name, build in TYPES.items()},
    "dictionary<int32, string>": pa.array(["a", "b", None, "a"]).dictionary_encode(),
    "string_view": pa.array(["a", "b", None, "a"], pa.string_view()),
    "binary_view": pa.array([b"a", b"b", None, b"a"], pa.binary_view()),
    "null": pa.array([None] * 4, pa.null()),
    "large_list<large_list<time64[ns]>>": _list_of(
        _list_of(_ints([1, 2, None, 1], pa.time64("ns")))
    ),
}


@pytest.mark.parametrize("type_name", list(PREPARED_TYPES))
def test_a_prepared_key_matches_as_its_values_do(type_name: str) -> None:
    """Rows 0 and 3 of each column hold one value, row 1 another, row 2 a null.
    Polars, given the prepared column, matches exactly the rows whose values are
    equal (compared here independently, as exact values), so a preparation that
    merged or split values would fail. A sliced column is prepared as its slice,
    and the rule and the preparation agree."""
    column = PREPARED_TYPES[type_name]
    assert part_without_rule(column.type, structs=True) is None
    prepared = polars_ready(column)
    assert len(prepared) == len(column)
    sliced = polars_ready(column.slice(1, 3))
    assert _exact(sliced) == _exact(prepared.slice(1, 3))
    frame = polars_key_frame(pa.table({"k": column}), ["k"], "row")
    assert frame.get_column("row").to_list() == [0, 1, 2, 3]
    values = _exact(column)
    # A null key never matches in the operators (``Join`` keys on a constant column
    # too, ``SemiJoin`` drops null right keys); here only non-null rows are compared.
    present = [i for i, value in enumerate(values) if value is not None]
    pairs = frame.join(frame, on="k", how="inner", suffix="_other")
    matched = {
        (int(left), int(right))
        for left, right in zip(
            pairs.get_column("row").to_list(), pairs.get_column("row_other").to_list()
        )
        if left in present and right in present
    }
    assert matched == {
        (i, j) for i in present for j in present if repr(values[i]) == repr(values[j])
    }
    if present:
        assert matched >= {(0, 3), (3, 0)}
        assert (0, 1) not in matched


@pytest.mark.parametrize("type_name", list(NO_RULE))
def test_the_rule_and_the_preparation_refuse_alike(type_name: str) -> None:
    column = NO_RULE[type_name]
    assert part_without_rule(column.type, structs=True) is not None
    with pytest.raises(TypeError, match="no rule prepares"):
        _ = polars_ready(column)


def test_a_struct_has_a_rule_for_matching_only() -> None:
    """A struct matches field by field; it has no order (``structs=False``)."""
    struct_type = pa.struct([("a", pa.date64()), ("b", DEC256)])
    assert part_without_rule(struct_type, structs=True) is None
    assert part_without_rule(struct_type, structs=False) == struct_type
    assert part_without_rule(pa.large_list(struct_type), structs=False) == struct_type


@pytest.mark.parametrize("operator", [Join, SemiJoin])
def test_a_sliced_input_with_null_list_keys_joins(
    operator: Callable[[], Join | SemiJoin],
) -> None:
    """A table that is a slice of another, with a null list key: matched as the
    unsliced rows would be. (The stream rebuilds a sliced column; a sliced array
    given to ``polars_ready`` directly is tested above.)"""
    keys = _list_of(_date64([1, 86_400_001, None, 2, 86_400_001]))
    whole = pa.Table.from_arrays(
        [keys, pa.array([0, 1, 2, 3, 4], pa.int64())], names=["k", "x"]
    )
    left = ArrowTableStream(whole.slice(1), tag_columns=["k"])
    right = ArrowTableStream(
        pa.Table.from_arrays(
            [_list_of(_date64([86_400_001, None])), pa.array([7, 8], pa.int64())],
            names=["k", "y"],
        ),
        tag_columns=["k"],
    )
    table = operator()(left, right).as_table()
    assert sorted(_exact(table.column("x"))) == [1, 4]
    assert _exact(table.column("k")) == [[86_400_001], [86_400_001]]
