"""A struct appended with another field set keeps every value under its name (ITL-1334).

Schema evolution is on by default: an append may add struct fields, omit some, or give
them in another order. Delta's MERGE matches struct fields by position, so the rows are
first given the stored struct layout by name (``_conformed_to_stored``), and a column
whose Arrow type is recorded has its record describe the merged column (``merged_type``),
which a read restores by name. Every read path, on a fresh database object, gives each
value under the name it was written with: never swapped, never dropped.

A ``null``-typed part with nothing stored at its path (a new struct field, the items of a
new list) is refused naming the column and the path, before anything is buffered.
"""

from __future__ import annotations

import pathlib
import re
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.errors import InputValidationError, UnstorableColumnError

PATH = ("t",)


def _struct(*fields: tuple[str, Any]) -> pa.DataType:
    return pa.struct([pa.field(name, arrow_type) for name, arrow_type in fields])


def _rows(ids: list[bytes], **columns: tuple[list[Any], pa.DataType]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, pa.large_binary()),
            **{n: pa.array(v, t) for n, (v, t) in columns.items()},
        }
    )


# Each read path, on a fresh database object: the rows of ids 1 and 2, in id order.
READS: dict[str, Callable[[DeltaTableDatabase], pa.Table | None]] = {
    "get_all_records": lambda db: db.get_all_records(PATH, record_id_column="id"),
    "get_records_by_ids": lambda db: db.get_records_by_ids(
        PATH, [b"2", b"1"], record_id_column="id"
    ),
    "get_record_by_id": lambda db: pa.concat_tables(
        [
            t
            for t in (
                db.get_record_by_id(PATH, b"1", record_id_column="id"),
                db.get_record_by_id(PATH, b"2", record_id_column="id"),
            )
            if t is not None
        ]
    ),
    "get_records_with_column_value": lambda db: db.get_records_with_column_value(
        PATH, {"k": 7}, record_id_column="id"
    ),
}

F16, I64 = pa.float16(), pa.int64()

# (first type, first values, appended type, appended values, expected values):
# recorded (float16, a fixed-size list) and unrecorded (int64) structs, at the top
# level and inside a list; two columns each.
CASES: dict[str, tuple[Any, list[Any], Any, list[Any], list[Any]]] = {
    "reordered, recorded": (
        _struct(("a", F16), ("b", F16)),
        [{"a": 1.0, "b": 2.0}],
        _struct(("b", F16), ("a", F16)),
        [{"b": 4.0, "a": 3.0}],
        [{"a": 1.0, "b": 2.0}, {"a": 3.0, "b": 4.0}],
    ),
    "reordered, unrecorded": (
        _struct(("a", I64), ("b", I64)),
        [{"a": 1, "b": 2}],
        _struct(("b", I64), ("a", I64)),
        [{"b": 4, "a": 3}],
        [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
    ),
    "omitted, recorded": (
        _struct(("a", F16), ("b", I64)),
        [{"a": 1.0, "b": 2}],
        _struct(("a", F16)),
        [{"a": 3.0}],
        [{"a": 1.0, "b": 2}, {"a": 3.0, "b": None}],
    ),
    "added, recorded": (
        _struct(("a", F16)),
        [{"a": 1.0}],
        _struct(("a", F16), ("c", pa.list_(pa.int16(), 2))),
        [{"a": 3.0, "c": [5, 6]}],
        [{"a": 1.0, "c": None}, {"a": 3.0, "c": [5, 6]}],
    ),
    "reordered and added, in a list": (
        pa.large_list(_struct(("a", pa.uint8()), ("b", I64))),
        [[{"a": 1, "b": 2}]],
        pa.large_list(_struct(("c", I64), ("b", I64), ("a", pa.uint8()))),
        [[{"c": 9, "b": 4, "a": 3}]],
        [[{"a": 1, "b": 2, "c": None}], [{"a": 3, "b": 4, "c": 9}]],
    ),
}


@pytest.mark.parametrize("read", sorted(READS))
@pytest.mark.parametrize("case", sorted(CASES))
def test_an_evolved_struct_keeps_every_value_under_its_name(
    tmp_path: pathlib.Path, case: str, read: str
) -> None:
    first, first_values, appended, appended_values, expected = CASES[case]
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows(
            [b"1"],
            k=([7], pa.int64()),
            x=(first_values, first),
            y=(first_values, first),
        ),
        record_id_column="id",
        flush=True,
    )
    db.add_records(
        PATH,
        _rows(
            [b"2"],
            k=([7], pa.int64()),
            y=(appended_values, appended),
            x=(appended_values, appended),
        ),
        record_id_column="id",
        flush=True,
    )
    back = READS[read](DeltaTableDatabase(tmp_path))
    assert back is not None
    back = back.sort_by("id")
    for name in ("x", "y"):
        assert back.column(name).to_pylist() == expected, name


def test_omitting_a_non_nullable_stored_field_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    stored = pa.struct([pa.field("a", I64), pa.field("b", I64, nullable=False)])
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows([b"1"], x=([{"a": 1, "b": 2}], stored)),
        record_id_column="id",
        flush=True,
    )
    with pytest.raises(InputValidationError, match="'x'.*'b'.*non-nullable"):
        db.add_records(
            PATH,
            _rows([b"2"], x=([{"a": 3}], _struct(("a", I64)))),
            record_id_column="id",
            flush=True,
        )


@pytest.mark.parametrize(
    ("stored", "stored_values", "incoming", "incoming_values", "path"),
    [
        (
            _struct(("a", pa.int16())),
            [{"a": 1}],
            _struct(("a", pa.int16()), ("b", pa.null())),
            [{"a": 2, "b": None}],
            "x.b",
        ),
        (
            pa.large_list(_struct(("a", pa.int16()))),
            [[{"a": 1}]],
            pa.large_list(_struct(("a", pa.int16()), ("b", pa.null()))),
            [[{"a": 2, "b": None}]],
            "x[].b",
        ),
        (
            _struct(("a", pa.int16())),
            [{"a": 1}],
            _struct(("a", pa.int16()), ("l", pa.large_list(pa.null()))),
            [{"a": 2, "l": [None]}],
            "x.l[]",
        ),
    ],
    ids=["new struct field", "new field in a list of structs", "items of a new list"],
)
def test_an_untyped_new_nested_null_part_is_refused_before_buffering(
    tmp_path: pathlib.Path,
    stored: Any,
    stored_values: list[Any],
    incoming: Any,
    incoming_values: list[Any],
    path: str,
) -> None:
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows([b"1"], x=(stored_values, stored)),
        record_id_column="id",
        flush=True,
    )
    with pytest.raises(UnstorableColumnError, match=f"'x'.*'{re.escape(path)}'"):
        db.add_records(
            PATH, _rows([b"2"], x=(incoming_values, incoming)), record_id_column="id"
        )
    db.flush()  # nothing was buffered: the flush has nothing to fail on
    back = db.get_all_records(PATH, record_id_column="id")
    assert back is not None
    assert back.column("id").to_pylist() == [b"1"]


def test_a_null_value_at_a_typed_nested_path_appends(tmp_path: pathlib.Path) -> None:
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows([b"1"], x=([{"a": 1}], _struct(("a", pa.int16())))),
        record_id_column="id",
        flush=True,
    )
    db.add_records(
        PATH,
        _rows([b"2"], x=([{"a": None}], _struct(("a", pa.null())))),
        record_id_column="id",
        flush=True,
    )
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None
    assert back.sort_by("id").column("x").to_pylist() == [{"a": 1}, {"a": None}]


def test_a_map_with_renamed_fields_is_refused(tmp_path: pathlib.Path) -> None:
    """The recorded type goes through Arrow IPC, which names them key and value."""
    renamed = pa.map_(  # pyright: ignore[reportCallIssue]  # pyarrow takes fields
        pa.field("k", pa.string(), nullable=False),  # pyright: ignore[reportArgumentType]
        pa.field("v", I64),  # pyright: ignore[reportArgumentType]
    )
    with pytest.raises(UnstorableColumnError, match="'x'.*'key' and 'value'"):
        DeltaTableDatabase(tmp_path).add_records(
            PATH, _rows([b"1"], x=([[("a", 1)]], renamed)), record_id_column="id"
        )
