"""GroupBy groups, orders and emits by Arrow value, never by Python value (ITL-1250).

``GroupBy`` used to convert its input to Python rows: two keys that became one Python
value were one group (``time64[ns]`` 1 ns and 2 ns are both a ``datetime.time`` of 0 us),
and the data lists were rebuilt from Python, so a value with no exact Python form changed
(``time64[ns]`` truncated to microseconds, ``date64`` moved to midnight, a float32
signaling NaN quieted, ITL-1248). It now decides which rows group and in what order with
Polars, and builds its output from the input's Arrow arrays.

Every comparison here reads values exactly: temporal values as their integers, floats as
their bit patterns, decimals as text, recursively through lists.
"""

from __future__ import annotations

import decimal
import itertools
from typing import Any, TypeAlias, cast

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.operators import GroupBy
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.core.streams.base import StreamBase
from orcapod.protocols.core_protocols import StreamProtocol

from .several_items import async_rows

NS = pa.time64("ns")
Column: TypeAlias = "pa.Array[Any]"


def _table(columns: dict[str, Column]) -> pa.Table:
    return pa.Table.from_arrays(list(columns.values()), names=list(columns))


D = decimal.Decimal
# decimal256 values that differ in the 40th decimal place: no decimal128 holds them.
DEC = pa.decimal256(76, 40)


def _dec(units: int, tiny: int = 0) -> D:
    """``units`` with ``tiny`` in the 40 decimal places, the sign of ``units``, exactly.

    Built from text: Python's default decimal context keeps 28 digits, so arithmetic
    would round ``1 + 1e-40`` to ``1``.
    """
    return D(f"{'-' if units < 0 else ''}{abs(units)}.{tiny:040d}")


def _ints(values: list[int | None], arrow_type: pa.DataType) -> Column:
    """``values`` as integers cast to ``arrow_type`` (a temporal type)."""
    return pa.array(values, pa.int64()).cast(arrow_type)


def _floats(bits: list[int | None], width: int) -> Column:
    unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[width]
    floating = {16: pa.float16(), 32: pa.float32(), 64: pa.float64()}[width]
    return pa.array(bits, unsigned).view(floating)


def _exact(column: Column | pa.ChunkedArray[Any]) -> Any:
    """The values of ``array`` read exactly: what a Python round trip would lose is kept.

    Temporal values are their integers, floats their bit patterns, decimals their text,
    lists recursively, with nulls as ``None``.
    """
    array: Column = (
        column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
    )
    if isinstance(array, pa.ExtensionArray):
        array = array.storage
    arrow_type = array.type
    if pa.types.is_large_list(arrow_type) or pa.types.is_list(arrow_type):
        lists = cast("pa.LargeListArray[Any] | pa.ListArray[Any]", array)
        values = _exact(lists.values)
        offsets = lists.offsets.to_pylist()
        return [
            values[offsets[i] : offsets[i + 1]] if array[i].is_valid else None
            for i in range(len(array))
        ]
    if pa.types.is_floating(arrow_type):
        unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}
        return array.view(unsigned[arrow_type.bit_width]).to_pylist()
    if pa.types.is_temporal(arrow_type):
        return array.cast(pa.int64()).to_pylist()
    if pa.types.is_decimal(arrow_type):
        return [None if v is None else str(v) for v in array.to_pylist()]
    return array.to_pylist()


def _source(
    columns: dict[str, Column],
    tags: list[str],
    *,
    plain: list[str] | None = None,
) -> StreamBase:
    """A source over ``columns``; ``tags`` are tags, ``plain`` the plain ones (default all).

    Record ids are the ``rid`` column, so a reordered input keeps every record id.
    """
    return op.sources.ArrowTableSource(
        _table(columns),
        tag_columns=tags,
        tag_only_columns=True if plain is None else plain,
        source_id="s",
        record_id_columns="rid",
    )


def _permuted(columns: dict[str, Column], order: tuple[int, ...]) -> dict[str, Column]:
    indices = pa.array(order, pa.int64())
    return {name: column.take(indices) for name, column in columns.items()}


def _fingerprint(stream: StreamProtocol) -> dict[str, Any]:
    """Every column read exactly, the row hashes and the stream's hashes."""
    table = stream.as_table(all_info=True)
    return {
        "columns": {name: _exact(table.column(name)) for name in table.column_names},
        "row_hashes": [
            (tag.content_hash().to_string(), data.content_hash().to_string())
            for tag, data in stream.iter_data()
        ],
        "content_hash": stream.content_hash().to_string(),
        "pipeline_hash": stream.pipeline_hash().to_string(),
    }


# ── keys that differ below Python's precision ───────────────────────────────

# Five rows, four distinct keys: rows 1 and 4 share a key. On main the first two keys
# of each type were one group, and so were the last two.
KEY_CASES = {
    "time64_ns": _ints([1, 2, 1000, 1001, 2], NS),
    "date64_off_midnight": _ints([1, 2, 86_400_000, 86_400_001, 2], pa.date64()),
    "timestamp_ns": _ints([1, 2, 1000, 1001, 2], pa.timestamp("ns")),
    "duration_ns": _ints([1, 2, 1000, 1001, 2], pa.duration("ns")),
    "decimal256": pa.array(
        [_dec(1, 1), _dec(1, 2), _dec(2), _dec(2, 1), _dec(1, 2)], DEC
    ),
}


@pytest.mark.parametrize("plain", [True, False], ids=["plain_key", "data_tag_key"])
@pytest.mark.parametrize("name", list(KEY_CASES))
def test_keys_that_differ_below_python_precision_are_separate_groups(
    name: str, plain: bool
) -> None:
    """Four distinct Arrow keys are four groups, each emitting its own key, in every order."""
    key = KEY_CASES[name]
    columns: dict[str, Column] = {
        "k": key,
        "t": pa.array([5, 4, 3, 2, 1], pa.int64()),
        "rid": pa.array(["r0", "r1", "r2", "r3", "r4"], pa.large_string()),
        "v": pa.array([0, 1, 2, 3, 4], pa.int64()),
    }
    seen: dict[str, Any] = {}
    for order in itertools.permutations(range(5)):
        source = _source(
            _permuted(columns, order), ["k", "t"], plain=None if plain else ["t"]
        )
        grouped = source.group_by("k")
        table = grouped.as_table()
        assert _exact(table.column("k")) == _exact(key.take(pa.array([0, 1, 2, 3])))
        # Rows 1 and 4 share a key; they are ordered by ``t`` (4 before 1).
        assert table.column("v").to_pylist() == [[0], [4, 1], [2], [3]]
        fingerprint = _fingerprint(grouped)
        if not seen:
            seen = fingerprint
        assert fingerprint == seen


@pytest.mark.parametrize("by", [["a", "b"], ["b", "a"]], ids=["a_b", "b_a"])
@pytest.mark.parametrize("roles", ["plain", "data_tags", "mixed"])
def test_two_exact_keys_group_by_both_values_in_every_row_order(
    by: list[str], roles: str
) -> None:
    """Two keys, each differing only below a microsecond or below a day, in any by order."""
    columns: dict[str, Column] = {
        "a": _ints([1, 2, 1, 2, 1], NS),
        "b": _ints(
            [86_400_001, 86_400_001, 86_400_002, 86_400_001, 86_400_001], pa.date64()
        ),
        "t": pa.array([0, 1, 2, 3, 4], pa.int64()),
        "rid": pa.array(["r0", "r1", "r2", "r3", "r4"], pa.large_string()),
        "v": pa.array([0, 1, 2, 3, 4], pa.int64()),
        "w": _ints([10, 11, 12, 13, 14], NS),
    }
    plain = {"plain": None, "data_tags": ["t"], "mixed": ["a", "t"]}[roles]
    seen: dict[str, Any] = {}
    for order in itertools.permutations(range(5)):
        grouped = _source(
            _permuted(columns, order), ["a", "b", "t"], plain=plain
        ).group_by(by)
        table = grouped.as_table()
        # Groups in key order, keys in name order: (1, +1), (1, +2), (2, +1).
        assert _exact(table.column("a")) == [1, 1, 2]
        assert _exact(table.column("b")) == [86_400_001, 86_400_002, 86_400_001]
        assert table.column("v").to_pylist() == [[0, 4], [2], [1, 3]]
        assert _exact(table.column("w")) == [[10, 14], [12], [11, 13]]
        fingerprint = _fingerprint(grouped)
        if not seen:
            seen = fingerprint
        assert fingerprint == seen


# ── data values are kept bit for bit ────────────────────────────────────────

# Two groups (g = 1, 2); ``t`` orders the members. Every data column holds values that a
# Python round trip changes.
DATA_COLUMNS: dict[str, Column] = {
    "g": pa.array([1, 2, 1, 2, 1], pa.int64()),
    "t": pa.array([3, 1, 2, 0, 1], pa.int64()),
    "rid": pa.array(["r0", "r1", "r2", "r3", "r4"], pa.large_string()),
    "d_time": _ints([1, 1001, 2, None, 999_999_999], NS),
    "d_date": _ints([1, 86_400_001, None, 86_399_999, 2], pa.date64()),
    # signaling NaN, -NaN, payload NaN, -0.0, null
    "d_f32": _floats([0x7F800001, 0xFFC00000, 0x7FC00001, 0x80000000, None], 32),
    "d_f16": _floats([0x7C01, 0xFE00, 0x7E01, 0x8000, None], 16),
    "d_list": pa.array(
        [[1, None], [], None, [1001], [2, 3]], pa.large_list(pa.int64())
    ).cast(pa.large_list(NS)),
    "d_dec": pa.array([_dec(1, 1), None, _dec(-3), _dec(2, 1), _dec(0, 1)], DEC),
}
DATA_NAMES = ["d_time", "d_date", "d_f32", "d_f16", "d_list", "d_dec"]
# Members of g=1: rows 4 (t=1), 2 (t=2), 0 (t=3); of g=2: rows 3 (t=0), 1 (t=1).
MEMBERS = [[4, 2, 0], [3, 1]]


def _expected_lists(name: str) -> list[Any]:
    column = DATA_COLUMNS[name]
    return [_exact(column.take(pa.array(m, pa.int64()))) for m in MEMBERS]


@pytest.mark.parametrize("roles", ["plain", "data_tags", "mixed"])
def test_data_lists_hold_the_input_values_bit_for_bit(roles: str) -> None:
    """Every data list equals the input's values in member order, read exactly."""
    plain = {"plain": None, "data_tags": [], "mixed": ["t"]}[roles]
    for order in itertools.permutations(range(5)):
        source = _source(_permuted(DATA_COLUMNS, order), ["g", "t"], plain=plain)
        table = source.group_by("g").as_table()
        for name in DATA_NAMES:
            assert _exact(table.column(name)) == _expected_lists(name), name
        if roles != "plain" and roles != "mixed":
            # A data-tag non-key keeps its data column, as a list in member order.
            assert table.column("t").to_pylist() == [[1, 2, 3], [0, 1]]


def _group_rows_exact(rows: list[Any]) -> dict[int, dict[str, Any]]:
    """Each async or sync row's data, read exactly, by its key ``g``."""
    out: dict[int, dict[str, Any]] = {}
    for tag, data in rows:
        [g] = tag.as_table().column("g").to_pylist()
        table = data.as_table()
        out[g] = {name: _exact(table.column(name))[0] for name in DATA_NAMES}
    return out


def test_data_lists_are_bit_exact_on_the_async_path() -> None:
    source = _source(DATA_COLUMNS, ["g", "t"], plain=["t"])
    expected = {
        g: {name: _expected_lists(name)[i] for name in DATA_NAMES}
        for i, g in enumerate([1, 2])
    }
    assert _group_rows_exact(async_rows(GroupBy("g"), source)) == expected
    assert _group_rows_exact(list(GroupBy("g")(source).iter_data())) == expected


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_data_lists_are_bit_exact_in_a_job(orchestrator: Any) -> None:
    job = PipelineJob(name="exact_values", database=InMemoryArrowDatabase())
    with job:
        _ = _source(DATA_COLUMNS, ["g", "t"], plain=["t"]).group_by(
            "g", label="grouped"
        )
    job.run(orchestrator=orchestrator())
    table = job.nodes["grouped"].as_table()
    assert table.column("g").to_pylist() == [1, 2]
    for name in DATA_NAMES:
        assert _exact(table.column(name)) == _expected_lists(name), name


# ── member order by exact value ─────────────────────────────────────────────


@pytest.mark.parametrize("member_order", [None, ["t"], ["t", "u"]])
def test_members_are_ordered_by_the_exact_value_of_a_tag(
    member_order: list[str] | None,
) -> None:
    """A time64[ns] tag orders members by nanoseconds, not by the record id."""
    columns: dict[str, Column] = {
        "g": pa.array([1, 1, 1, 1], pa.int64()),
        "t": _ints([2, 1, 1001, 1000], NS),
        "u": pa.array([0, 0, 0, 0], pa.int64()),
        # Record ids against the order of t (v1, v0, v3, v2): a tie on t below a
        # microsecond would order v0 before v1 and v2 before v3.
        "rid": pa.array(["r2", "r3", "r0", "r1"], pa.large_string()),
        "v": pa.array([0, 1, 2, 3], pa.int64()),
    }
    for order in itertools.permutations(range(4)):
        source = _source(_permuted(columns, order), ["g", "t", "u"])
        grouped = source.group_by("g", member_order=member_order)
        assert grouped.as_table().column("v").to_pylist() == [[1, 0, 3, 2]]


def test_members_are_ordered_by_a_decimal256_tag() -> None:
    columns: dict[str, Column] = {
        "g": pa.array([1, 1, 1, 1], pa.int64()),
        "t": pa.array([_dec(-1, 1), _dec(1, 1), _dec(1), None], DEC),
        "rid": pa.array(["r0", "r1", "r2", "r3"], pa.large_string()),
        "v": pa.array([0, 1, 2, 3], pa.int64()),
    }
    for order in itertools.permutations(range(4)):
        grouped = _source(_permuted(columns, order), ["g", "t"]).group_by("g")
        assert grouped.as_table().column("v").to_pylist() == [[0, 2, 1, 3]]


def test_a_list_valued_tag_orders_by_exact_elements_with_nulls_last_at_every_depth() -> (
    None
):
    """Lists compare element by element; a null sorts last in a list and at the top."""
    tags = pa.array(
        [[1, None], [2], None, [None], [], [1], [1, 2]], pa.large_list(pa.int64())
    ).cast(pa.large_list(NS))
    columns: dict[str, Column] = {
        "g": pa.array([1] * 7, pa.int64()),
        "t": tags,
        "rid": pa.array([f"r{i}" for i in range(7)], pa.large_string()),
        "v": pa.array(range(7), pa.int64()),
    }
    # [] < [1] < [1, 2] < [1, null] < [2] < [null] < null; on main [1] and [2] tied.
    expected = [[4, 5, 6, 0, 1, 3, 2]]
    for order in itertools.islice(itertools.permutations(range(7)), 0, 5040, 97):
        grouped = _source(_permuted(columns, order), ["g", "t"]).group_by("g")
        assert grouped.as_table().column("v").to_pylist() == expected


# ── zero rows ───────────────────────────────────────────────────────────────


def test_zero_rows_give_the_types_of_the_rows() -> None:
    table = _table({name: DATA_COLUMNS[name] for name in ["g", "t", *DATA_NAMES]})
    full = GroupBy("g")(ArrowTableStream(table, tag_columns=["g", "t"])).as_table()
    empty = GroupBy("g")(
        ArrowTableStream(table.slice(0, 0), tag_columns=["g", "t"])
    ).as_table()
    assert empty.num_rows == 0
    assert empty.schema == full.schema


# ── struct-bearing tags are refused ─────────────────────────────────────────

STRUCT = pa.array(
    [{"x": 1, "y": "a"}, {"x": 1, "y": "a"}, {"x": 2, "y": None}],
    pa.struct([("x", pa.int64()), ("y", pa.large_string())]),
)
# A ``dict[str, int]`` tag is stored as a list of key/value structs.
DICT_SHAPED = pa.array(
    [[{"key": "a", "value": 1}], [{"key": "a", "value": 1}], []],
    pa.large_list(pa.struct([("key", pa.large_string()), ("value", pa.int64())])),
)
MAP = pa.array(
    [[("a", 1)], [("a", 1)], [("b", 2)]], pa.map_(pa.large_string(), pa.int64())
)
STRUCT_TAGS = {"struct": STRUCT, "dict_shaped": DICT_SHAPED, "map": MAP}


def _struct_stream(tag: Column) -> ArrowTableStream:
    table = _table(
        {
            "g": pa.array([1, 1, 2], pa.int64()),
            "s": tag,
            "v": pa.array([0, 1, 2], pa.int64()),
            "w": pa.array([3, 4, 5], pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["g", "s"])


@pytest.mark.parametrize("name", list(STRUCT_TAGS))
def test_a_struct_bearing_key_is_refused(name: str) -> None:
    stream = _struct_stream(STRUCT_TAGS[name])
    with pytest.raises(InputValidationError, match=r"'s'.*struct|struct.*'s'"):
        _ = GroupBy("s")(stream)
    with pytest.raises(InputValidationError, match=r"'s'"):
        _ = GroupBy(["g", "s"])(stream)


@pytest.mark.parametrize("name", list(STRUCT_TAGS))
def test_a_struct_bearing_ordering_tag_is_refused(name: str) -> None:
    """A non-key tag orders the members, and a struct has no decided order."""
    stream = _struct_stream(STRUCT_TAGS[name])
    with pytest.raises(InputValidationError, match="select_tag_columns"):
        _ = GroupBy("g")(stream)
    # The async path refuses it too: it runs the operator without ``process``.
    with pytest.raises(InputValidationError, match="select_tag_columns"):
        _ = async_rows(GroupBy("g"), stream)


def test_dropping_the_struct_tag_first_groups() -> None:
    stream = _struct_stream(STRUCT).select_tag_columns(["g"])
    assert GroupBy("g")(stream).as_table().column("v").to_pylist() == [[0, 1], [2]]
