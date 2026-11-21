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
import json
from pathlib import Path
from typing import Any, TypeAlias, cast

import polars as pl
import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.operators import GroupBy
from orcapod.core.operators.arrow_keys import part_without_rule, polars_ready
from orcapod.core.operators.group_by import (
    GROUP_BY_FOLD_LABEL,
    _sort_key,  # pyright: ignore[reportPrivateUsage]  # the test is about it
)
from orcapod.contexts import get_default_context
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
from orcapod.system_constants import constants
from orcapod.utils import arrow_utils

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


# ── every list kind orders element by element; nothing unprepared reaches Polars ──
#
# Polars panics on a decimal256 (an error no caller can catch), so every tag reaches
# its sort only through one preparation, through lists, large lists and fixed-size
# lists at every depth (CodeRabbit, PR 458). Each case has two ordering tags: ``t1``
# of the type under test and ``t2``, which breaks the tie between rows 0 and 4.


def _fixed(values: list[Any], element: pa.DataType, size: int) -> Column:
    return pa.array(values, pa.list_(element, size))


LIST_KIND_CASES: dict[str, tuple[Column, list[int], list[int]]] = {
    # [1, 5] < [1, null] < [1 + 1e-40, 2] (rows 0 and 4) < null
    "fixed_size_list<decimal256>": (
        _fixed(
            [[_dec(1, 1), _dec(2)], [_dec(1), None], None, [_dec(1), _dec(5)]]
            + [[_dec(1, 1), _dec(2)]],
            DEC,
            2,
        ),
        [3, 1, 4, 0, 2],
        [3, 1, 4, 2, 0],
    ),
    # [-0.5] < [2.0] (rows 0 and 4) < [null] < null
    "fixed_size_list<float16>": (
        pa.FixedSizeListArray.from_arrays(
            _floats([0x4000, None, 0xB800, 0, 0x4000], 16),
            1,
            mask=pa.array([False, False, False, True, False]),
        ),
        [2, 4, 0, 1, 3],
        [2, 4, 1, 3, 0],
    ),
    # nanoseconds two levels down: [[1], [1]] < [[1], [null]] < [[2], [1]] < [null, [0]]
    "fixed_size_list<fixed_size_list<time64[ns]>>": (
        _fixed(
            [[[2], [1]], [[1], [None]], [[1], [1]], [None, [0]], [[2], [1]]],
            pa.list_(pa.int64(), 1),
            2,
        ).cast(pa.list_(pa.list_(NS, 1), 2)),
        [2, 1, 4, 0, 3],
        [2, 1, 4, 3, 0],
    ),
    # [] < [[1], [1]] < [[2]] (rows 0 and 4) < [null]
    "large_list<fixed_size_list<decimal256>>": (
        pa.array(
            [[[_dec(2)]], [], [[_dec(1)], [_dec(1)]], [None], [[_dec(2)]]],
            pa.large_list(pa.list_(DEC, 1)),
        ),
        [1, 2, 4, 0, 3],
        [1, 2, 4, 3, 0],
    ),
}


def _list_kind_columns(t1: Column) -> dict[str, Column]:
    return {
        "g": pa.array([1] * 5, pa.int64()),
        "t1": t1,
        "t2": pa.array([1, 0, 0, 0, 0], pa.int64()),
        "rid": pa.array([f"r{i}" for i in range(5)], pa.large_string()),
        "v": pa.array(range(5), pa.int64()),
        "w": _ints([10, 11, 12, 13, 14], NS),
    }


@pytest.mark.parametrize("name", list(LIST_KIND_CASES))
def test_every_list_kind_orders_members_element_by_element(name: str) -> None:
    t1, by_name, t2_first = LIST_KIND_CASES[name]
    columns = _list_kind_columns(t1)
    for order in itertools.permutations(range(5)):
        source = _source(_permuted(columns, order), ["g", "t1", "t2"])
        for member_order, expected in ((None, by_name), (["t2"], t2_first)):
            grouped = source.group_by("g", member_order=member_order)
            table = grouped.as_table()
            assert table.column("v").to_pylist() == [expected], member_order
            assert _exact(table.column("w")) == [[10 + i for i in expected]]


@pytest.mark.parametrize("name", list(LIST_KIND_CASES))
def test_every_list_kind_orders_members_the_same_sync_and_async(name: str) -> None:
    t1, by_name, _ = LIST_KIND_CASES[name]
    source = _source(_list_kind_columns(t1), ["g", "t1", "t2"])
    for rows in (
        async_rows(GroupBy("g"), source),
        list(GroupBy("g")(source).iter_data()),
    ):
        [(_, data)] = rows
        assert data.as_table().column("v").to_pylist() == [by_name]


def test_a_fixed_size_list_key_is_refused_as_list_valued() -> None:
    columns = _list_kind_columns(LIST_KIND_CASES["fixed_size_list<decimal256>"][0])
    source = _source(columns, ["g", "t1", "t2"])
    with pytest.raises(InputValidationError, match="list-valued"):
        _ = source.group_by(["g", "t1"])
    with pytest.raises(InputValidationError, match="list-valued"):
        _ = async_rows(GroupBy(["t1", "g"]), source)


# Tag types GroupBy has no order for, beyond a struct or a map at the top level.
UNORDERABLE_TAGS: dict[str, Column] = {
    "month_day_nano_interval": pa.array(
        # (months, days, nanoseconds): one month against 31 days has no order.
        [(1, 0, 0), (0, 31, 0), None],
        pa.month_day_nano_interval(),
    ),
    "large_list<map>": pa.array(
        [[[("a", 1)]], [[("b", 2)]], []],
        pa.large_list(pa.map_(pa.large_string(), pa.int64())),
    ),
    "fixed_size_list<struct>": pa.FixedSizeListArray.from_arrays(
        pa.StructArray.from_arrays([pa.array([1, 2, 3], pa.int64())], names=["x"]),
        1,
    ),
}


# A union or a list view cannot be a stream's column at all (developer, November 21,
# 2026, ITL-640): the stream refuses it when it is built, before any operator.
DENSE_UNION: Column = pa.UnionArray.from_dense(
    pa.array([0, 1, 0], pa.int8()),
    pa.array([0, 0, 1], pa.int32()),
    [pa.array([1, 2], pa.int64()), pa.array(["a"], pa.large_string())],
)
REFUSED_BY_THE_STREAM: dict[str, Column] = {
    "dense_union": DENSE_UNION,
    "large_list_view": pa.array([[1], [2], None], pa.large_list_view(pa.int64())),
}


@pytest.mark.parametrize("name", list(REFUSED_BY_THE_STREAM))
def test_a_union_or_list_view_tag_is_refused_when_the_stream_is_built(
    name: str,
) -> None:
    with pytest.raises(
        InputValidationError,
        match=r"union and list-view columns are not supported in Orcapod(.|\n)*"
        r"tag column 's'",
    ):
        _ = _unorderable_stream(REFUSED_BY_THE_STREAM[name])


def _unorderable_stream(tag: Column) -> ArrowTableStream:
    table = _table(
        {
            "g": pa.array([1, 1, 2], pa.int64()),
            "u": pa.array([0, 1, 2], pa.int64()),
            "s": tag,
            "v": pa.array([0, 1, 2], pa.int64()),
            "w": pa.array([3, 4, 5], pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["g", "u", "s"])


@pytest.mark.parametrize("name", list(UNORDERABLE_TAGS))
def test_a_tag_without_an_order_is_refused_in_every_role(name: str) -> None:
    """Refused as an ordering tag and as a key, on the sync and the async path."""
    stream = _unorderable_stream(UNORDERABLE_TAGS[name])
    for by in (["g"], ["u", "g"]):
        with pytest.raises(InputValidationError, match="select_tag_columns"):
            _ = GroupBy(by)(stream)
        with pytest.raises(InputValidationError, match="select_tag_columns"):
            _ = async_rows(GroupBy(by), stream)
    for by in (["s"], ["g", "s"]):
        with pytest.raises(InputValidationError, match=r"'s'"):
            _ = GroupBy(by)(stream)
        with pytest.raises(InputValidationError, match=r"'s'"):
            _ = async_rows(GroupBy(by), stream)
    kept = stream.select_tag_columns(["g", "u"])
    assert GroupBy("g")(kept).as_table().column("v").to_pylist() == [[0, 1], [2]]


# ── every list follows one member order: the lists are parallel by origin row ──


@pytest.mark.parametrize("roles", ["plain", "data_tags", "mixed"])
def test_every_list_and_fold_follows_one_member_order(roles: str) -> None:
    """Element i of every data list, every source-info list and the folded record id
    belong to one origin row, in every row order (developer, November 19, 2026).

    The data columns are anti-correlated (``a`` rises where ``b`` falls), so a list
    sorted by its own values would come out in another order than its neighbours.
    """
    columns: dict[str, Column] = {
        "g": pa.array([1, 2, 1, 1, 2, 1], pa.int64()),
        "t": pa.array([3, 0, 1, 2, 1, 0], pa.int64()),
        "u": pa.array([0, 1, 1, 0, 0, 1], pa.int64()),
        "rid": pa.array([f"r{i}" for i in range(6)], pa.large_string()),
        "a": pa.array([10, 11, 12, 13, 14, 15], pa.int64()),
        "b": pa.array([5.0, 4.0, 3.0, 2.0, 1.0, 0.0], pa.float64()),
        "c": _ints([600, 500, 400, 300, 200, 100], NS),
    }
    row_of_a = {10 + i: f"r{i}" for i in range(6)}
    plain = {"plain": None, "data_tags": [], "mixed": ["u"]}[roles]
    for order in itertools.permutations(range(6)):
        source = _source(_permuted(columns, order), ["g", "t", "u"], plain=plain)
        for member_order in (None, ["u"]):
            table = source.group_by("g", member_order=member_order).as_table(
                all_info=True
            )
            [record_column] = [
                name
                for name in table.column_names
                if name.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
            ]
            exact_c = _exact(table.column("c"))
            for index, row in enumerate(table.to_pylist()):
                rows = [row_of_a[a] for a in row["a"]]
                # Every data list holds the values of those rows, in that order
                # (``c`` read from Arrow: as Python it would lose its nanoseconds).
                assert row["b"] == [5.0 - int(r[1:]) for r in rows]
                assert exact_c[index] == [600 - 100 * int(r[1:]) for r in rows]
                # Every source-info list names the same rows, in the same order.
                for name in ("a", "b", "c"):
                    tokens = row[f"{constants.SOURCE_PREFIX}{name}"]
                    assert [json.loads(t.split("::")[1])["rid"] for t in tokens] == rows
                # The folded record id is the fold of the same rows, in that order.
                assert row[record_column] == arrow_utils.fold_system_tag_values(
                    [json.dumps({"rid": r}, separators=(",", ":")) for r in rows],
                    label=GROUP_BY_FOLD_LABEL,
                )


# ── validation and preparation agree on every type ──────────────────────────
#
# CodeRabbit, PR 458: a dictionary over a logical type passed validation but its
# preparation unwrapped the logical type before decoding the dictionary, then met it
# again and raised. Since ITL-1264 no stream carries a dictionary-encoded column, so
# GroupBy reads only logical storage, and a dictionary is a type it has no order for:
# validation and preparation refuse it alike.

PATH_TYPE = cast(
    pa.BaseExtensionType,
    get_default_context().type_converter.python_type_to_arrow_type(Path),
)


def _paths(values: list[str | None]) -> Column:
    return pa.ExtensionArray.from_storage(
        PATH_TYPE, pa.array(values, PATH_TYPE.storage_type)
    )


def _dictionary(values: Column, indices: list[int | None]) -> Column:
    return pa.DictionaryArray.from_arrays(pa.array(indices, pa.int32()), values)


def test_a_dictionary_over_a_logical_type_never_reaches_group_by() -> None:
    """A dictionary-encoded Path tag is refused when the stream is built (ITL-1264),
    with the way to decode it, so GroupBy never sees it."""
    columns: dict[str, Column] = {
        "g": pa.array([1, 1, 1, 2], pa.int64()),
        "t1": _dictionary(_paths(["/b", "/a"]), [0, 1, None, 1]),
        "t2": pa.array([0, 0, 0, 1], pa.int64()),
        "rid": pa.array([f"r{i}" for i in range(4)], pa.large_string()),
        "v": pa.array(range(4), pa.int64()),
    }
    table = _table(columns)
    for tags in (["g", "t1", "t2"], ["t2", "t1", "g"]):
        with pytest.raises(InputValidationError) as raised:
            _ = ArrowTableStream(table, tag_columns=tags)
        message = str(raised.value)
        assert "dictionary-encoded columns are not currently supported" in message
        assert "tag column 't1'" in message
        assert "field.with_type(decoded)" in message


AGREEMENT_TYPES: dict[str, Column] = {
    "int8": pa.array([1, None, -1], pa.int8()),
    "uint64": pa.array([0, None, 2**64 - 1], pa.uint64()),
    "float16": _floats([0x3C00, None, 0x7C01], 16),
    "float64": pa.array([1.0, None, float("nan")], pa.float64()),
    "bool": pa.array([True, None, False]),
    "decimal128": pa.array([D(1), None, D(-1)], pa.decimal128(10, 2)),
    "decimal256": pa.array([_dec(1, 1), None, _dec(-1)], DEC),
    "large_string": pa.array(["b", None, "a"], pa.large_string()),
    "string_view": pa.array(["b", None, "a"], pa.string_view()),
    "binary_view": pa.array([b"b", None, b"a"], pa.binary_view()),
    "fixed_size_binary": pa.array([b"ab", None, b"ba"], pa.binary(2)),
    "date64": _ints([1, None, 86_400_001], pa.date64()),
    "time32[s]": pa.array([1, None, 2], pa.int32()).cast(pa.time32("s")),
    "time64[ns]": _ints([1, None, 2], NS),
    "timestamp[ns, tz]": _ints([1, None, 2], pa.timestamp("ns", "UTC")),
    "duration[s]": _ints([1, None, 2], pa.duration("s")),
    "null": pa.array([None, None, None], pa.null()),
    "logical Path": _paths(["/b", None, "/a"]),
    "fixed_size_list<decimal256>": _fixed([[_dec(1)], None, [_dec(2)]], DEC, 1),
    "large_list<fixed_size_list<float16>>": pa.LargeListArray.from_arrays(
        pa.array([0, 1, 1, 2], pa.int64()),
        pa.FixedSizeListArray.from_arrays(_floats([0x3C00, 0x4000], 16), 1),
    ),
    # Without an order (a dictionary among them: no stream carries one, ITL-1264):
    "dictionary<string>": _dictionary(
        pa.array(["b", "a"], pa.large_string()), [0, None, 1]
    ),
    "dictionary<Path>": _dictionary(_paths(["/b", "/a"]), [0, None, 1]),
    "large_list<dictionary<Path>>": pa.LargeListArray.from_arrays(
        pa.array([0, 1, 1, 2], pa.int64()),
        _dictionary(_paths(["/b", "/a"]), [0, 1]),
    ),
    "struct": pa.StructArray.from_arrays(
        [pa.array([1, 2, 3], pa.int64())], names=["x"]
    ),
    "map": pa.array([[("a", 1)], None, []], pa.map_(pa.large_string(), pa.int64())),
    "interval": pa.array([(1, 0, 0), None, (0, 31, 0)], pa.month_day_nano_interval()),
    "dense_union": DENSE_UNION,
    "large_list_view": pa.array([[1], None, [2]], pa.large_list_view(pa.int64())),
    "large_list<struct>": pa.array(
        [[{"x": 1}], None, []], pa.large_list(pa.struct([("x", pa.int64())]))
    ),
    "dictionary<struct>": _dictionary(
        pa.StructArray.from_arrays([pa.array([1, 2], pa.int64())], names=["x"]),
        [0, None, 1],
    ),
}
ORDERABLE = 20


@pytest.mark.parametrize("name", list(AGREEMENT_TYPES))
def test_validation_and_preparation_agree_on_every_type(name: str) -> None:
    """A type validation accepts is prepared and read by Polars; any other is refused
    by both, never passed through."""
    array = AGREEMENT_TYPES[name]
    # GroupBy's rule is the shared key preparation's with structs refused
    # (``arrow_keys``, ITL-1281).
    accepted = part_without_rule(array.type, structs=False) is None
    assert accepted == (list(AGREEMENT_TYPES).index(name) < ORDERABLE)
    if accepted:
        prepared = polars_ready(array)
        frame = pl.from_arrow(pa.Table.from_arrays([prepared], names=["x"]))
        assert isinstance(frame, pl.DataFrame)
        _ = frame.sort(_sort_key("x", frame.schema["x"]), nulls_last=True)
    elif part_without_rule(array.type, structs=True) is not None:
        with pytest.raises(TypeError, match="no rule"):
            _ = polars_ready(array)
    # A struct is prepared for matching (``Join``); GroupBy refuses it by its
    # rule above, before anything is prepared.
