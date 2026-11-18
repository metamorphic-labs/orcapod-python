"""The rows an ``ArrowTableStream`` hashes for its identity are a multiset (ITL-1227).

``identity_structure`` of a stream with no producer hashes its rows. ITL-1053 kept that
apart from the presentation sort of ``as_table``, ``head`` and the preview. ITL-1227
(developer, November 18, 2026: "I agree with multiset of rows as the definitive way to
sort") replaced the sort by tags, before the 0.4.0 freeze: each row is hashed on its own
and the labelled digests are sorted. This test holds a literal copy of that rule and
compares it with the stream's identity over a battery of shapes: no tag, one tag, two tags
in either order; system tags in either order or none; nulls; tied tags; list tags.
"""

from __future__ import annotations

import itertools
import random
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.streams import ArrowTableStream
from orcapod.types import ColumnConfig


def multiset_rule(stream: ArrowTableStream) -> pa.Table:
    """The identity's rows (ITL-1227): the rule, written out. Do not edit it.

    Every column group but the per-row content hash; each row hashed alone by the Arrow
    hasher; the labelled digests (``b"<label>:" + digest``) sorted bytewise into one
    ``large_binary`` column named ``row_hash``.
    """
    table = stream.as_table(
        columns=ColumnConfig(meta=True, context=True, source=True, system_tags=True)
    )
    hasher = stream.data_context.arrow_hasher
    digests = sorted(
        hasher.hash_table(table.slice(i, 1)).to_prefixed_digest()
        for i in range(table.num_rows)
    )
    return pa.table({"row_hash": pa.array(digests, pa.large_binary())})


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
    j: list[Any] = [rng.choice(["a", "b"]) for _ in range(n)]
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
        "x": pa.array(range(n), pa.int64()),
    }
    names = [c for c in columns if c in tags or c in system or c == "x"]
    table = pa.table([columns[c] for c in names], names=names)
    if rows is not None:
        table = table.take(rows)
    return ArrowTableStream(table, tag_columns=tags, system_tag_columns=system)


@pytest.mark.parametrize("shape", shapes())
def test_the_identity_holds_the_rows_as_a_multiset_of_row_hashes(
    shape: tuple[Any, ...],
) -> None:
    """``identity_structure`` is the literal rule, and every row order gives it."""
    stream = build(shape)
    name, row_hashes, tag_columns = stream.identity_structure()
    assert name == "ArrowTableStream"
    assert row_hashes.equals(multiset_rule(stream))
    assert row_hashes.num_rows == 6
    assert tag_columns == tuple(sorted(shape[0]))
    reversed_rows = build(shape, rows=[5, 4, 3, 2, 1, 0])
    assert reversed_rows.content_hash() == stream.content_hash()
