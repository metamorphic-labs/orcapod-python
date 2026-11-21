"""The rows an ``ArrowTableStream`` hashes for its identity are in the canonical order (ITL-1227).

``identity_structure`` of a stream with no producer hashes its whole identity table. ITL-1053
kept its row order apart from the presentation sort of ``as_table``, ``head`` and the preview.
ITL-1227 (developer, November 19, 2026, "option 2") made that order canonical, before the
0.4.0 freeze: a value sort over a closed list of column types (user tags in name order,
then every other column in name order), ties settled by each tied row's own hash. This test
holds an independent copy of that rule, written with Python values instead of pyarrow's
sort, and compares it with the stream's identity table over a battery of shapes: no tag,
one tag, two tags in either order; system tags in either order or none; nulls; tied tags;
list tags (not a sort key).
"""

from __future__ import annotations

import functools
import itertools
import random
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.streams import ArrowTableStream
from orcapod.types import ColumnConfig


def _sort_key_columns(stream: ArrowTableStream, table: pa.Table) -> list[str]:
    """User tags in name order, then the other columns in name order; only the types
    this battery holds that the spec lists (integers and strings; never a list)."""
    tags = sorted(stream.keys()[0])
    rest = sorted(c for c in table.column_names if c not in tags)
    return [
        c
        for c in (*tags, *rest)
        if pa.types.is_integer(table.schema.field(c).type)
        or pa.types.is_string(table.schema.field(c).type)
        or pa.types.is_large_string(table.schema.field(c).type)
    ]


def _compare_values(a: Any, b: Any) -> int:
    """Ascending, nulls last; strings by their UTF-8 bytes."""
    if a is None or b is None:
        return (a is None) - (b is None)
    if isinstance(a, str):
        a, b = a.encode(), b.encode()
    return (a > b) - (a < b)


def canonical_rule(stream: ArrowTableStream) -> pa.Table:
    """The identity table in canonical row order, written out. Do not edit it."""
    table = stream.as_table(
        columns=ColumnConfig(meta=True, context=True, source=True, system_tags=True)
    )
    keys = _sort_key_columns(stream, table)
    rows = table.to_pylist()

    def compare(i: int, j: int) -> int:
        for c in keys:
            result = _compare_values(rows[i][c], rows[j][c])
            if result:
                return result
        return 0

    order = sorted(range(len(rows)), key=functools.cmp_to_key(compare))
    hasher = stream.data_context.arrow_hasher
    result: list[int] = []
    for _, group in itertools.groupby(
        order, key=lambda i: tuple(repr(rows[i][c]) for c in keys)
    ):
        members = list(group)
        members.sort(
            key=lambda i: hasher.hash_table(table.slice(i, 1)).to_prefixed_digest()
        )
        result.extend(members)
    return table.take(pa.array(result, pa.int64()))


def shapes() -> list[tuple[Any, ...]]:
    """Every shape of the battery: tag columns, system tag order, nulls, ties, list tags."""
    return list(
        itertools.product(
            [(), ("k",), ("k", "j"), ("j", "k")],
            [("s1", "s2"), ("s2", "s1"), ("s1",), ()],
            [False, True],
            [False, True],
            [False, True],
        )
    )


def build(shape: tuple[Any, ...], rows: list[int] | None = None) -> ArrowTableStream:
    """A six-row stream of the given shape; random but seeded values."""
    tags, system, nulls, ties, list_tag = shape
    rng = random.Random(7)
    n = 6
    k: list[Any] = [rng.choice([0, 1, 2]) if ties else i for i in range(n)]
    j: list[Any] = [rng.choice(["a", "b", "é"]) for _ in range(n)]
    if nulls:
        k[1] = None
        j[3] = None
    columns: dict[str, pa.Array[Any]] = {
        "k": pa.array(
            [None if v is None else [v] for v in k], pa.large_list(pa.int64())
        )
        if list_tag
        else pa.array(k, pa.int64()),
        "j": pa.array(j, pa.string()),
        "s1": pa.array([rng.choice(["p", "q", "r"]) for _ in range(n)], pa.string()),
        "s2": pa.array([rng.choice(["u", "v"]) for _ in range(n)], pa.string()),
        "x": pa.array([rng.choice([5, 6]) for _ in range(n)], pa.int64()),
    }
    names = [c for c in columns if c in tags or c in system or c == "x"]
    table = pa.table([columns[c] for c in names], names=names)
    if rows is not None:
        table = table.take(pa.array(rows, pa.int64()))
    return ArrowTableStream(table, tag_columns=tags, system_tag_columns=system)


@pytest.mark.parametrize("shape", shapes())
def test_the_identity_table_is_in_the_canonical_row_order(
    shape: tuple[Any, ...],
) -> None:
    """``identity_structure``'s table is the rule's, and every row order gives it."""
    stream = build(shape)
    name, table, tag_columns = stream.identity_structure()
    assert name == "ArrowTableStream"
    assert table.equals(canonical_rule(stream))
    assert table.num_rows == 6
    assert tag_columns == tuple(sorted(shape[0]))
    for rows in ([5, 4, 3, 2, 1, 0], [2, 0, 4, 1, 5, 3]):
        assert build(shape, rows).content_hash() == stream.content_hash()
