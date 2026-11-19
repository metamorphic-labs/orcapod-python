"""The canonical row order of an identity table: the spec, pinned (ITL-1227).

Developer decision, November 19, 2026 ("option 2"): a raw stream's identity table is
hashed whole with its rows in a canonical order. The order is a value sort, with pyarrow as
the only sorter, over a closed list of column types (user tags in name order, then every
other column in name order; ascending; nulls last), and rows still tied are ordered by
their own row hash. ``docs/reference/hashing.md`` ("The canonical row order of an identity
table") states it so that a native implementation can reproduce it; these tests pin each
part of that statement, including that pyarrow's sort agrees with the stated comparison for
every participating type.
"""

from __future__ import annotations

import datetime
import decimal
import itertools
import pathlib
import uuid
from typing import Any

import numpy as np
import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.core.streams import ArrowTableStream
from orcapod.utils.arrow_utils import (
    canonical_identity_row_order,
    canonically_sortable,
)

HASHER = get_default_context().arrow_hasher
CONVERTER = get_default_context().type_converter


def _digest(table: pa.Table, row: int) -> bytes:
    return HASHER.hash_table(table.slice(row, 1)).to_prefixed_digest()


# ── the closed list of sort-key types ───────────────────────────────────────

PARTICIPATING = [
    *(getattr(pa, f"int{w}")() for w in (8, 16, 32, 64)),
    *(getattr(pa, f"uint{w}")() for w in (8, 16, 32, 64)),
    pa.float32(),
    pa.float64(),
    pa.bool_(),
    pa.string(),
    pa.large_string(),
    pa.binary(),
    pa.large_binary(),
    pa.binary(3),
    pa.date32(),
    pa.date64(),
    pa.time32("ms"),
    pa.time64("ns"),
    pa.timestamp("us"),
    pa.timestamp("ns", "Europe/Berlin"),
    pa.duration("s"),
    pa.decimal128(10, 2),
    pa.null(),
]


def _logical_type(python_type: Any) -> pa.DataType:
    return CONVERTER.python_type_to_arrow_type(python_type)


NOT_PARTICIPATING = {
    "float16": pa.float16(),
    "decimal256": pa.decimal256(50, 2),
    "large_list": pa.large_list(pa.int64()),
    "list": pa.list_(pa.int64()),
    "fixed_size_list": pa.list_(pa.int64(), 2),
    "struct": pa.struct([("a", pa.int64())]),
    "map": pa.map_(pa.string(), pa.int64()),
    "dictionary": pa.dictionary(pa.int32(), pa.string()),
    "string_view": pa.string_view(),
    "Path (logical)": _logical_type(pathlib.Path),
    "UUID (logical)": _logical_type(uuid.UUID),
    "ndarray (logical)": _logical_type(np.ndarray),
}


@pytest.mark.parametrize("arrow_type", PARTICIPATING, ids=str)
def test_these_types_are_sort_keys(arrow_type: pa.DataType) -> None:
    assert canonically_sortable(arrow_type)


@pytest.mark.parametrize("name", sorted(NOT_PARTICIPATING))
def test_these_types_are_never_sort_keys(name: str) -> None:
    """Decided by the type: pyarrow may learn to sort some of them; the order must not
    change when it does."""
    assert not canonically_sortable(NOT_PARTICIPATING[name])


# ── pyarrow agrees with the stated comparison ────────────────────────────────

NAN = float("nan")


def _spec_key(value: Any, kind: str) -> tuple[Any, ...]:
    """The spec's comparison, written in Python, for distinct non-null values."""
    if kind == "float":
        if np.isnan(value):
            return (1, 0.0)
        return (0, value)
    if kind == "string":
        return (value.encode("utf-8"),)
    return (value,)


# Distinct values per participating type, given in an order that is not sorted; the
# spec's order is checked against pyarrow's, nulls last. -0.0 and 0.0 tie and are
# tested apart.
AGREEMENT: list[tuple[str, pa.DataType, list[Any], str]] = [
    *(
        (
            f"int{w}",
            getattr(pa, f"int{w}")(),
            [3, -(2 ** (w - 1)), 2 ** (w - 1) - 1, 0, -1],
            "plain",
        )
        for w in (8, 16, 32, 64)
    ),
    *(
        (f"uint{w}", getattr(pa, f"uint{w}")(), [3, 0, 2**w - 1, 1], "plain")
        for w in (8, 16, 32, 64)
    ),
    (
        "float32",
        pa.float32(),
        [1.5, NAN, float("-inf"), -2.0, float("inf"), 0.0],
        "float",
    ),
    (
        "float64",
        pa.float64(),
        [1.5, NAN, float("-inf"), -2.0, float("inf"), -0.0],
        "float",
    ),
    ("bool", pa.bool_(), [True, False], "plain"),
    ("string", pa.string(), ["b", "é", "B", "", "a", "Z", "e", "\U0001f600"], "string"),
    ("large_string", pa.large_string(), ["b", "é", "B", "", "a"], "string"),
    ("binary", pa.binary(), [b"\xff", b"", b"a", b"\x00", b"ab"], "plain"),
    ("large_binary", pa.large_binary(), [b"\xff", b"", b"a", b"\x00"], "plain"),
    ("fixed_size_binary", pa.binary(2), [b"\xff\x00", b"\x00\xff", b"ab"], "plain"),
    (
        "date32",
        pa.date32(),
        [
            datetime.date(2020, 1, 2),
            datetime.date(1969, 12, 31),
            datetime.date(2020, 1, 1),
        ],
        "plain",
    ),
    (
        "date64",
        pa.date64(),
        [datetime.date(2020, 1, 2), datetime.date(1969, 12, 31)],
        "plain",
    ),
    ("time32[ms]", pa.time32("ms"), [5, 1, 86_399_999], "storage"),
    ("time64[ns]", pa.time64("ns"), [1001, 1, 5], "storage"),
    ("timestamp[us]", pa.timestamp("us"), [3, -1, 2], "storage"),
    (
        "timestamp[ns, Europe/Berlin]",
        pa.timestamp("ns", "Europe/Berlin"),
        [3, -1, 2],
        "storage",
    ),
    ("duration[s]", pa.duration("s"), [3, -5, 2], "storage"),
    (
        "decimal128(10,2)",
        pa.decimal128(10, 2),
        [decimal.Decimal("1.50"), decimal.Decimal("-2.25"), decimal.Decimal("0.01")],
        "plain",
    ),
]


@pytest.mark.parametrize(
    ("name", "arrow_type", "values", "kind"), AGREEMENT, ids=[a[0] for a in AGREEMENT]
)
def test_pyarrow_sorts_each_participating_type_as_the_spec_states(
    name: str, arrow_type: pa.DataType, values: list[Any], kind: str
) -> None:
    """Fails if pyarrow's sort ever disagrees with the stated comparison."""
    with_null = [*values, None]
    if kind == "storage":
        storage = pa.int32() if arrow_type.bit_width == 32 else pa.int64()
        column = pa.array(with_null, storage).cast(arrow_type)
        expected = sorted(values)
    else:
        column = pa.array(with_null, arrow_type)
        expected = sorted(values, key=lambda v: _spec_key(v, kind))
    for order in (list(range(len(with_null))), list(reversed(range(len(with_null))))):
        table = pa.table({"c": column.take(pa.array(order, pa.int64()))})
        result = canonical_identity_row_order(table, (), HASHER)
        got = result.column("c")
        if kind == "storage":
            got = got.cast(pa.int32() if arrow_type.bit_width == 32 else pa.int64())
        got_values = got.to_pylist()
        assert got_values[-1] is None  # nulls last
        if kind == "float":
            assert [("nan" if v != v else v) for v in got_values[:-1]] == [
                ("nan" if v != v else v) for v in expected
            ]
        else:
            assert got_values[:-1] == expected


def test_a_timestamp_zone_does_not_enter_the_comparison() -> None:
    """Values compare by their stored instant; the zone is the column's, one for all."""
    stored = [3_600_000_000_000, 0, 1_800_000_000_000]
    for zone in (None, "UTC", "Asia/Tokyo", "America/New_York"):
        arrow_type = pa.timestamp("ns", zone) if zone else pa.timestamp("ns")
        column = pa.array(stored, pa.int64()).cast(arrow_type)
        result = canonical_identity_row_order(pa.table({"c": column}), (), HASHER)
        assert result.column("c").cast(pa.int64()).to_pylist() == sorted(stored)


# ── tags first, then every other column, in name order ───────────────────────


def test_tag_columns_come_first_then_the_others_in_name_order() -> None:
    table = pa.table(
        {
            "a": [1, 2, 1, 2],
            "z": ["q", "q", "p", "p"],
            "m": [9, 8, 7, 6],
        }
    )
    tagged = canonical_identity_row_order(table, ("z",), HASHER)
    assert tagged.to_pylist() == [
        {"a": 1, "z": "p", "m": 7},
        {"a": 2, "z": "p", "m": 6},
        {"a": 1, "z": "q", "m": 9},
        {"a": 2, "z": "q", "m": 8},
    ]
    untagged = canonical_identity_row_order(table, (), HASHER)
    # a, then m (name order): 1-7, 1-9, 2-6, 2-8.
    assert [(r["a"], r["m"]) for r in untagged.to_pylist()] == [
        (1, 7),
        (1, 9),
        (2, 6),
        (2, 8),
    ]


def test_two_tags_are_taken_in_name_order_whatever_their_order() -> None:
    table = pa.table({"k": [2, 1, 2, 1], "j": ["b", "b", "a", "a"], "x": [1, 2, 3, 4]})
    for tags in (("k", "j"), ("j", "k")):
        result = canonical_identity_row_order(table, tags, HASHER)
        assert [(r["j"], r["k"]) for r in result.to_pylist()] == [
            ("a", 1),
            ("a", 2),
            ("b", 1),
            ("b", 2),
        ]


# ── ties go to the row hash ──────────────────────────────────────────────────


def _tie_values(name: str) -> pa.Array[Any] | None:
    arrow_type = NOT_PARTICIPATING[name]
    if name == "float16":
        return pa.array(np.array([2.0, 1.0, 3.0], np.float16))
    if name == "decimal256":
        return pa.array(
            [decimal.Decimal("2"), decimal.Decimal("1"), decimal.Decimal("3")],
            arrow_type,
        )
    if name in ("large_list", "list"):
        return pa.array([[2], [1], [3]], arrow_type)
    if name == "fixed_size_list":
        return pa.array([[2, 0], [1, 0], [3, 0]], arrow_type)
    if name == "struct":
        return pa.array([{"a": 2}, {"a": 1}, {"a": 3}], arrow_type)
    if name == "Path (logical)":
        return (
            CONVERTER.python_dicts_to_arrow_table(
                [{"c": pathlib.Path(p)} for p in ("/b", "/a", "/c")],
                python_schema={"c": pathlib.Path},
            )
            .column("c")
            .combine_chunks()
        )
    if name == "UUID (logical)":
        return (
            CONVERTER.python_dicts_to_arrow_table(
                [{"c": uuid.UUID(int=i)} for i in (2, 1, 3)],
                python_schema={"c": uuid.UUID},
            )
            .column("c")
            .combine_chunks()
        )
    if name == "ndarray (logical)":
        return (
            CONVERTER.python_dicts_to_arrow_table(
                [{"c": np.array([i])} for i in (2, 1, 3)],
                python_schema={"c": np.ndarray},
            )
            .column("c")
            .combine_chunks()
        )
    return None  # map, dictionary, string_view: not hashable or refused elsewhere


TIE_TYPES = [
    name
    for name in sorted(NOT_PARTICIPATING)
    if name not in ("map", "dictionary", "string_view")
]


@pytest.mark.parametrize("name", TIE_TYPES)
def test_rows_tied_in_every_sort_key_are_ordered_by_their_row_hash(name: str) -> None:
    """A column that is no sort key never orders rows by its value (or its stored bytes):
    rows equal in every sort key are ordered by their own row hash, in every input order."""
    values = _tie_values(name)
    assert values is not None
    base = pa.table({"k": pa.array([1, 1, 1], pa.int64()), "c": values})
    results = set()
    for order in itertools.permutations(range(3)):
        table = base.take(pa.array(order, pa.int64()))
        result = canonical_identity_row_order(table, ("k",), HASHER)
        digests = [_digest(result, i) for i in range(3)]
        assert digests == sorted(digests)
        results.add(HASHER.hash_table(result).to_hex())
    assert len(results) == 1


def test_a_list_column_is_ordered_by_row_hash_not_by_its_values() -> None:
    """Were lists a sort key (a newer pyarrow can sort them), this order would change."""
    table = pa.table({"c": pa.array([[1], [2], [3], [4]], pa.large_list(pa.int64()))})
    result = canonical_identity_row_order(table, (), HASHER)
    digests = [_digest(table, i) for i in range(4)]
    by_digest = [
        table.column("c")[i].as_py() for i in sorted(range(4), key=digests.__getitem__)
    ]
    assert result.column("c").to_pylist() == by_digest
    assert by_digest != [[1], [2], [3], [4]]  # the test would be empty otherwise


@pytest.mark.parametrize(
    "bits",
    [
        (0x8000000000000000, 0x0000000000000000),  # -0.0, 0.0
        (0x7FF8000000000000, 0x7FF8000000000001),  # NaN, NaN with a payload
        (0x7FF8000000000000, 0xFFF8000000000000),  # NaN, -NaN
    ],
    ids=["zero-signs", "nan-payloads", "nan-signs"],
)
def test_float_values_the_sort_counts_equal_are_a_tie(bits: tuple[int, int]) -> None:
    """The sort counts -0.0 equal to 0.0 and every NaN equal: such rows are a tie group,
    ordered by their row hashes (which still tell the bits apart before ITL-1179)."""
    floats = pa.array(np.array(bits, np.uint64).view(np.float64))
    base = pa.table({"k": [1, 1], "x": floats})
    results = []
    for order in ([0, 1], [1, 0]):
        table = base.take(pa.array(order, pa.int64()))
        result = canonical_identity_row_order(table, ("k",), HASHER)
        assert _digest(result, 0) <= _digest(result, 1)
        results.append(HASHER.hash_table(result).to_hex())
    assert results[0] == results[1]


@pytest.mark.xfail(
    strict=True,
    reason="ITL-1179: the float rule (starfix 0.6, PR 456) is not on this branch; once it "
    + "is, -0.0/0.0 and NaN bit patterns hash alike, so two raw streams differing only "
    + "in them are one stream. Rows the sort ties then also get equal row hashes, and "
    + "their order is arbitrary, which the final table hash ignores for the same reason.",
)
def test_raw_streams_differing_only_in_zero_signs_or_nan_bits_are_one_stream() -> None:
    def stream(bits: list[int]) -> ArrowTableStream:
        floats = pa.array(np.array(bits, np.uint64).view(np.float64))
        return ArrowTableStream(pa.table({"k": [1, 2], "x": floats}), tag_columns=["k"])

    assert (
        stream([0x8000000000000000, 0x7FF8000000000001]).content_hash()
        == stream([0x0000000000000000, 0x7FF8000000000000]).content_hash()
    )


def test_the_null_type_is_a_sort_key_that_ties_every_row() -> None:
    table = pa.table({"n": pa.array([None, None, None], pa.null()), "x": [3, 1, 2]})
    result = canonical_identity_row_order(table, (), HASHER)
    assert result.column("x").to_pylist() == [1, 2, 3]


# ── zero rows, one row, duplicates ───────────────────────────────────────────


def test_zero_and_one_row_are_returned_as_they_are() -> None:
    empty = pa.table({"k": pa.array([], pa.int64())})
    assert canonical_identity_row_order(empty, ("k",), HASHER).equals(empty)
    one = pa.table({"k": [1], "c": pa.array([[1]], pa.large_list(pa.int64()))})
    assert canonical_identity_row_order(one, ("k",), HASHER).equals(one)


def test_duplicate_rows_are_all_kept() -> None:
    table = pa.table({"k": [2, 1, 2, 1, 2], "x": [5, 6, 5, 6, 5]})
    result = canonical_identity_row_order(table, ("k",), HASHER)
    assert result.to_pylist() == [
        {"k": 1, "x": 6},
        {"k": 1, "x": 6},
        {"k": 2, "x": 5},
        {"k": 2, "x": 5},
        {"k": 2, "x": 5},
    ]
