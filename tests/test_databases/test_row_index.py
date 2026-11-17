"""Tests for the hash-index helpers shared by the Arrow databases (ITL-919, ITL-1173).

``matching_rows`` is tested directly: the cost of a lookup is the subject (ITL-1173), and
the databases' own lookup tests cover the results through their public methods.
"""

from __future__ import annotations

import random
from collections.abc import Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod.databases._row_index import KeyIndex, matching_rows


class _CountingPositions(list[int]):
    """A position list that counts how often it is walked."""

    walks = 0

    def __iter__(self) -> Iterator[int]:
        _CountingPositions.walks += 1
        return super().__iter__()


def _expression(pairs: list[tuple[str, object]]) -> pc.Expression:
    expression = pc.field(pairs[0][0]) == pairs[0][1]
    for column, value in pairs[1:]:
        expression = expression & (pc.field(column) == value)
    return expression


def _table(n: int) -> pa.Table:
    return pa.table(
        {
            "id": pa.array([f"id{i}".encode() for i in range(n)], pa.large_binary()),
            "context": pa.array(["ctx"] * n, pa.large_string()),
            "value": pa.array(range(n), pa.int64()),
        }
    )


@pytest.mark.parametrize("context_first", [False, True])
@pytest.mark.parametrize("n", [10, 1_000, 10_000])
def test_lookup_walks_only_the_shortest_position_list(
    n: int, context_first: bool
) -> None:
    """A lookup on (id, context) does not walk the context list, which holds every row.

    Before ITL-1173 the positions of every condition were intersected as sets, so a
    condition every row meets (the data context) made each lookup walk all N rows.
    """
    key_index = KeyIndex(_table(n))
    context_positions = _CountingPositions(key_index.column("context")["ctx"])
    key_index.columns["context"] = {"ctx": context_positions}
    _CountingPositions.walks = 0

    pairs: list[tuple[str, object]] = [("id", b"id3"), ("context", "ctx")]
    if context_first:
        pairs.reverse()
    result = matching_rows(key_index, pairs, _expression(pairs))

    assert result["value"].to_pylist() == [3]
    assert _CountingPositions.walks == 0


def test_lookup_with_no_match_in_one_condition_is_empty() -> None:
    key_index = KeyIndex(_table(5))
    pairs: list[tuple[str, object]] = [("id", b"id3"), ("context", "other")]
    result = matching_rows(key_index, pairs, _expression(pairs))
    assert result.num_rows == 0
    assert result.schema == key_index.table.schema


def test_lookup_equals_the_arrow_filter() -> None:
    """Indexed results equal ``table.filter`` for random conditions, rows in table order."""
    rng = random.Random(1173)
    n = 400
    table = pa.Table.from_batches(
        pa.table(
            {
                "a": pa.array(
                    [rng.choice([b"x", b"y", b"z", None]) for _ in range(n)],
                    pa.large_binary(),
                ),
                "b": pa.array(
                    [rng.choice(["p", "q", None]) for _ in range(n)], pa.large_string()
                ),
                "c": pa.array([rng.choice(["u", "v"]) for _ in range(n)], pa.string()),
                "n": pa.array([rng.randrange(3) for _ in range(n)], pa.int64()),
                "row": pa.array(range(n), pa.int64()),
            }
        ).to_batches(max_chunksize=37)
    )
    key_index = KeyIndex(table)
    for _ in range(200):
        columns = rng.sample(["a", "b", "c", "n"], rng.randint(1, 4))
        pairs: list[tuple[str, object]] = []
        for column in columns:
            if column == "a":
                pairs.append((column, rng.choice([b"x", b"y", b"z", b"w"])))
            elif column == "b":
                pairs.append((column, rng.choice(["p", "q", "r"])))
            elif column == "c":
                pairs.append((column, rng.choice(["u", "v"])))
            else:
                pairs.append((column, rng.randrange(3)))
        expression = _expression(pairs)
        got = matching_rows(key_index, pairs, expression)
        assert got["row"].to_pylist() == table.filter(expression)["row"].to_pylist()


def test_lookup_after_append_uses_the_extended_index() -> None:
    first = _table(4)
    key_index = KeyIndex(first)
    key_index.column("id")
    key_index.column("context")
    more = pa.table(
        {
            "id": pa.array([b"id9", b"id2"], pa.large_binary()),
            "context": pa.array(["ctx", "new"], pa.large_string()),
            "value": pa.array([90, 20], pa.int64()),
        }
    )
    key_index.append(more, pa.concat_tables([first, more]))

    cases: list[tuple[list[tuple[str, object]], list[int]]] = [
        ([("id", b"id2"), ("context", "ctx")], [2]),
        ([("id", b"id2"), ("context", "new")], [20]),
        ([("id", b"id9"), ("context", "ctx")], [90]),
    ]
    for pairs, expected in cases:
        got = matching_rows(key_index, pairs, _expression(pairs))
        assert got["value"].to_pylist() == expected
