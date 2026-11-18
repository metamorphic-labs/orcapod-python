"""A raw stream's content hash depends on neither its row order nor its column order (ITL-1227).

A raw ``ArrowTableStream`` (built over a table, with no producer) hashes its rows. Its
identity used to hold its tag columns and data-tags in tag order and to sort the rows by
the tags in tag order: two raw streams over one table that differed only in the order of
``tag_columns`` had different content hashes, rows tied on every tag or with a list tag
hashed in their given order, and a stream with no tag and no system tag could not be
hashed at all. Developer, November 18, 2026: "nothing should depend on tag order", and "I
agree with multiset of rows as the definitive way to sort". The identity now holds the rows
as a multiset of row hashes and the names as sorted tuples.
"""

from __future__ import annotations

import itertools
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.streams import ArrowTableStream

TAGS = ("k", "j", "m")
SYSTEM = ("s2", "s1")
DATA = ("x", "y")
TAG_ORDERS = tuple(itertools.permutations(TAGS))
SYSTEM_ORDERS = tuple(itertools.permutations(SYSTEM))

# Six rows; ``k`` and ``j`` repeat on their own, ``j`` holds a null, and rows 4 and 5 are
# tied on every user tag (they differ in their system tags and data).
COLUMNS: dict[str, "pa.Array[Any]"] = {
    "k": pa.array([2, 1, 2, 1, 1, 1], pa.int64()),
    "j": pa.array(["b", "a", "a", None, "b", "b"], pa.large_string()),
    "m": pa.array([1, 1, 2, 2, 3, 3], pa.int64()),
    "s1": pa.array(["p", "q", "r", "p", "q", "r"], pa.large_string()),
    "s2": pa.array(["u", "v", "u", "v", "u", "v"], pa.large_string()),
    "x": pa.array([10, 20, 30, 40, 50, 60], pa.int64()),
    "y": pa.array([0.5, 1.5, 2.5, 3.5, 4.5, 5.5], pa.float64()),
}

LAYOUTS = {
    "tags_first": lambda tags, system: [*tags, *system, *DATA],
    "data_first": lambda tags, system: [*reversed(DATA), *system, *reversed(tags)],
    "interleaved": lambda tags, system: [
        name
        for pair in itertools.zip_longest(["x", "y"], tags, system)
        for name in pair
        if name is not None
    ],
}

# The data-tags of each case, given in an order that is not name order.
DATA_TAG_SETS = pytest.mark.parametrize(
    "data_tags",
    [(), ("m", "j"), ("m", "k", "j")],
    ids=["plain", "two_data_tags", "all_data_tags"],
)
SYSTEM_TAGS = pytest.mark.parametrize(
    "system", [SYSTEM, ()], ids=["system_tags", "no_system_tags"]
)


def _stream(
    tag_order: tuple[str, ...],
    system_order: tuple[str, ...],
    layout: str,
    data_tags: tuple[str, ...],
    *,
    rows: list[int] | None = None,
) -> ArrowTableStream:
    names = LAYOUTS[layout](tag_order, system_order)
    table = pa.table([COLUMNS[name] for name in names], names=names)
    if rows is not None:
        table = table.take(pa.array(rows, pa.int64()))
    return ArrowTableStream(
        table,
        tag_columns=list(tag_order),
        system_tag_columns=list(system_order),
        data_tags=data_tags,
    )


@SYSTEM_TAGS
@DATA_TAG_SETS
def test_the_content_hash_is_one_for_every_tag_order_system_tag_order_and_layout(
    data_tags: tuple[str, ...], system: tuple[str, ...]
) -> None:
    """6 tag orders x the system-tag orders x 3 layouts: one content and pipeline hash."""
    streams = [
        _stream(tag_order, system_order, layout, data_tags)
        for tag_order in TAG_ORDERS
        for system_order in itertools.permutations(system)
        for layout in LAYOUTS
    ]
    assert len({s.content_hash() for s in streams}) == 1
    assert len({s.pipeline_hash() for s in streams}) == 1


@SYSTEM_TAGS
@DATA_TAG_SETS
def test_every_row_order_hashes_the_same(
    data_tags: tuple[str, ...], system: tuple[str, ...]
) -> None:
    """All 720 orders of the six rows (ties and a null among them), in two tag orders."""
    hashes = {
        _stream(
            tag_order, system, "tags_first", data_tags, rows=list(rows)
        ).content_hash()
        for rows in itertools.permutations(range(6))
        for tag_order in (TAGS, tuple(reversed(TAGS)))
    }
    assert len(hashes) == 1


def test_the_order_the_data_tags_are_given_in_does_not_count() -> None:
    hashes = {
        _stream(TAGS, SYSTEM, "tags_first", order).content_hash()
        for order in itertools.permutations(("k", "j", "m"))
    }
    assert len(hashes) == 1


def test_which_tags_are_data_tags_still_counts() -> None:
    """The data-tags are a set of names in the identity: each set is another stream."""
    hashes = {
        _stream(TAGS, SYSTEM, "tags_first", data_tags).content_hash()
        for data_tags in [(), ("j",), ("k",), ("j", "k"), ("j", "k", "m")]
    }
    assert len(hashes) == 5


def test_the_rows_still_count() -> None:
    """A different row, a missing row, or a row added twice: another stream."""
    reference = _stream(TAGS, SYSTEM, "tags_first", ()).content_hash()
    for rows in ([0, 1, 2, 3, 4], [0, 1, 2, 3, 4, 5, 5], [0, 0, 1, 2, 3, 4]):
        assert _stream(TAGS, SYSTEM, "tags_first", (), rows=rows).content_hash() != (
            reference
        )


def _two_column_stream(
    columns: dict[str, "pa.Array[Any]"], tags: list[str]
) -> ArrowTableStream:
    table = pa.table(list(columns.values()), names=list(columns))
    return ArrowTableStream(table, tag_columns=tags)


def test_a_stream_with_no_tag_and_no_system_tag_has_a_content_hash() -> None:
    """Main raised (nothing to sort by); a multiset needs no key."""

    def stream(xs: list[int], ys: list[str]) -> ArrowTableStream:
        return _two_column_stream(
            {
                "x": pa.array(xs, pa.int64()),
                "y": pa.array(ys, pa.large_string()),
            },
            [],
        )

    as_given = stream([1, 2, 3], ["a", "b", "c"])
    assert as_given.content_hash() == stream([3, 1, 2], ["c", "a", "b"]).content_hash()
    assert as_given.content_hash() != stream([1, 2, 4], ["a", "b", "c"]).content_hash()


def test_rows_tied_on_every_tag_hash_the_same_in_any_order() -> None:
    def tied(xs: list[int], tag_order: list[str]) -> ArrowTableStream:
        return _two_column_stream(
            {
                "k": pa.array([1, 1], pa.int64()),
                "j": pa.array(["a", "a"], pa.large_string()),
                "x": pa.array(xs, pa.int64()),
            },
            tag_order,
        )

    hashes = {
        tied(xs, order).content_hash()
        for xs in ([10, 20], [20, 10])
        for order in (["k", "j"], ["j", "k"])
    }
    assert len(hashes) == 1


def test_a_duplicate_row_is_kept_twice() -> None:
    """A multiset: two equal rows are not one row."""

    def stream(ks: list[int]) -> ArrowTableStream:
        return _two_column_stream(
            {
                "k": pa.array(ks, pa.int64()),
                "x": pa.array([7] * len(ks), pa.int64()),
            },
            ["k"],
        )

    assert stream([1, 1]).content_hash() != stream([1]).content_hash()
    assert stream([1, 1, 2]).content_hash() == stream([2, 1, 1]).content_hash()
    assert stream([1, 1, 2]).content_hash() != stream([1, 2, 2]).content_hash()


def test_rows_with_a_list_tag_hash_the_same_in_any_order() -> None:
    """Arrow cannot order a list tag; a multiset does not need to."""

    def stream(ks: list[list[int]], xs: list[int], tags: list[str]) -> ArrowTableStream:
        return _two_column_stream(
            {
                "k": pa.array(ks, pa.large_list(pa.int64())),
                "j": pa.array(["a"] * len(ks), pa.large_string()),
                "x": pa.array(xs, pa.int64()),
            },
            tags,
        )

    hashes = {
        stream(ks, xs, tags).content_hash()
        for ks, xs in (([[2], [1]], [20, 10]), ([[1], [2]], [10, 20]))
        for tags in (["k", "j"], ["j", "k"])
    }
    assert len(hashes) == 1


def test_a_stream_with_no_rows_has_one_content_hash_for_every_tag_order() -> None:
    hashes = {
        _stream(order, SYSTEM, "tags_first", ("m", "j"), rows=[]).content_hash()
        for order in TAG_ORDERS
    }
    assert len(hashes) == 1
    assert hashes != {_stream(TAGS, SYSTEM, "tags_first", ("m", "j")).content_hash()}
