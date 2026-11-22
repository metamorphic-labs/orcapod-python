"""Rows given as a slice of a larger array append and read back exactly (ITL-1334).

A slice's list offsets start where its first row's items do, and Arrow's list and map
constructors take no validity mask over such offsets. Every list or map Delta rebuilds
(written in its stored form, conformed to the stored struct layout, restored on read)
rebases the offsets and slices the child (``delta_arrow_types.rebased_offsets``), so a
null or non-null row at any slice offset, alone or among others, in one chunk or several,
reads back as written through every read path on a fresh database object.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.utils.arrow_utils import normalize_to_large_types
from tests.test_databases.test_delta_struct_evolution import PATH, READS

I64 = pa.int64()

# Each column type with four rows, the second null: [v0, None, v2, v3].
SOURCES: dict[str, tuple[Any, list[Any]]] = {
    "large_list": (pa.large_list(pa.int16()), [[1], None, [3, 4], []]),
    "list": (pa.list_(pa.int32()), [[1, 2], None, [], [5]]),
    "list, non-null items": (
        pa.large_list(pa.field("item", I64, nullable=False)),
        [[1], None, [3], [4, 5]],
    ),
    "map": (
        pa.map_(pa.large_string(), I64),
        [[("a", 1)], None, [("c", 3), ("d", 4)], []],
    ),
    "large_list<fixed_size_list>": (
        pa.large_list(pa.list_(I64, 2)),
        [[[1, 2]], None, [[3, 4], None], [[5, 6]]],
    ),
    "large_list<large_list>": (
        pa.large_list(pa.large_list(I64)),
        [[[1], None], None, [[2, 3]], [[]]],
    ),
    "struct<large_list>": (
        pa.struct([pa.field("l", pa.large_list(I64))]),
        [{"l": [1]}, None, {"l": None}, {"l": [4, 5]}],
    ),
    "map<large_list>": (
        pa.map_(pa.large_string(), pa.large_list(I64)),
        [[("a", [1])], None, [("c", None)], [("d", [4, 5])]],
    ),
    "struct (unrecorded)": (
        pa.struct([pa.field("a", I64)]),
        [{"a": 1}, None, {"a": None}, {"a": 4}],
    ),
}


def _batch(ids: list[bytes], x: Any, y: Any) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, pa.large_binary()),
            "k": pa.array([7] * len(ids)),
            "x": x,
            "y": y,
        }
    )


def _batches(arrow_type: Any, values: list[Any]) -> list[pa.Table]:
    """Four appends of slices: one row at offset 0, the null row alone at offset 1, two
    rows at offset 2, and three rows over two chunks (a sliced chunk, then another)."""
    source = pa.array(values, arrow_type)
    # ``y`` holds the rows in reverse, so its null is at another offset.
    reverse = pa.array(list(reversed(values)), arrow_type)
    return [
        _batch([b"1"], source.slice(0, 1), reverse.slice(0, 1)),
        _batch([b"2"], source.slice(1, 1), reverse.slice(1, 1)),
        _batch([b"3", b"4"], source.slice(2, 2), reverse.slice(2, 2)),
        _batch(
            [b"5", b"6", b"7"],
            pa.chunked_array([source.slice(1, 2), source.slice(3, 1)]),
            pa.chunked_array([reverse.slice(1, 2), reverse.slice(3, 1)]),
        ),
    ]


@pytest.mark.parametrize("read", sorted(READS))
@pytest.mark.parametrize("kind", sorted(SOURCES))
def test_sliced_rows_append_and_read_back(tmp_path: Path, kind: str, read: str) -> None:
    arrow_type, values = SOURCES[kind]
    batches = _batches(arrow_type, values)
    db = DeltaTableDatabase(tmp_path)
    for batch in batches:
        db.add_records(PATH, batch, record_id_column="id", flush=True)
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]  # nothing failed
    expected = pa.concat_tables(batches).sort_by("id")
    if read in ("get_record_by_id", "get_records_by_ids"):
        # These shared reads give the rows of ids 1 and 2.
        expected = expected.slice(0, 2)
    back = READS[read](DeltaTableDatabase(tmp_path))
    assert back is not None
    back = back.sort_by("id")
    for name in ("x", "y"):
        # Every Orcapod table is large: a ``list`` reads back as ``large_list``.
        want = normalize_to_large_types(expected.schema.field(name).type)
        got = back.schema.field(name).type
        assert got == want, name
        assert str(got) == str(want), name  # the item names too
        assert back.column(name).to_pylist() == expected.column(name).to_pylist(), name
