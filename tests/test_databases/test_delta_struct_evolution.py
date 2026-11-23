"""A struct appended with another field set keeps every value under its name (ITL-1334).

Schema evolution is on by default: an append may add struct fields, omit some, or give
them in another order. Delta's MERGE matches struct fields by position, so the rows are
first given the stored struct layout by name (``_conformed_to_stored``), and a column
whose Arrow type is recorded keeps its record, which a read restores by name. Every read
path, on a fresh database object, gives each value under the name it was written with:
never swapped, never dropped.

An append that would CHANGE a recorded column's record (a field added to a recorded
struct) is refused by Delta at the commit: deltalake 1.5 and later refuses a commit that
changes a column's field metadata, and the record is field metadata. The flush raises it
once and the batch leaves the queue (ITL-1403: the flush is the point of truth). A
struct with no record still gains fields, and a new column is still added.

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
    "added, unrecorded": (
        _struct(("a", I64)),
        [{"a": 1}],
        _struct(("a", I64), ("c", I64)),
        [{"a": 3, "c": 5}],
        [{"a": 1, "c": None}, {"a": 3, "c": 5}],
    ),
}

# Appends that would change a recorded column's record: refused (ITL-1403).
RECORD_CHANGES: dict[str, tuple[Any, list[Any], Any, list[Any]]] = {
    "added, recorded": (
        _struct(("a", F16)),
        [{"a": 1.0}],
        _struct(("a", F16), ("c", pa.list_(pa.int16(), 2))),
        [{"a": 3.0, "c": [5, 6]}],
    ),
    "reordered and added, in a list": (
        pa.large_list(_struct(("a", pa.uint8()), ("b", I64))),
        [[{"a": 1, "b": 2}]],
        pa.large_list(_struct(("c", I64), ("b", I64), ("a", pa.uint8()))),
        [[{"c": 9, "b": 4, "a": 3}]],
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


@pytest.mark.parametrize("flush", [True, False], ids=["flushed", "queued"])
@pytest.mark.parametrize("case", sorted(RECORD_CHANGES))
def test_an_append_changing_a_recorded_type_is_refused_once_at_flush(
    tmp_path: pathlib.Path, case: str, flush: bool
) -> None:
    """Accepted when added; Delta refuses the changed record at the commit; the flush
    raises once, the batch leaves the queue, and a second flush is a no-op (ITL-1403:
    the flush is the point of truth)."""
    first, first_values, appended, appended_values = RECORD_CHANGES[case]
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows([b"1"], k=([7], pa.int64()), x=(first_values, first)),
        record_id_column="id",
        flush=True,
    )
    with pytest.raises(InputValidationError, match="Delta refused the commit"):
        db.add_records(
            PATH,
            _rows([b"2"], k=([7], pa.int64()), x=(appended_values, appended)),
            record_id_column="id",
            flush=flush,
        )
        db.flush()
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    db.flush()  # a no-op
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None
    assert back.column("id").to_pylist() == [b"1"]
    assert back.column("x").to_pylist() == first_values


def test_a_new_column_is_still_added(tmp_path: pathlib.Path) -> None:
    """A column the table does not hold changes no stored record: it is added."""
    recorded = _struct(("a", F16))
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows([b"1"], x=([{"a": 1.0}], recorded)),
        record_id_column="id",
        flush=True,
    )
    db.add_records(
        PATH,
        _rows(
            [b"2"], x=([{"a": 2.0}], recorded), z=([[1, 2]], pa.list_(pa.int16(), 2))
        ),
        record_id_column="id",
        flush=True,
    )
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None
    back = back.sort_by("id")
    assert back.column("z").to_pylist() == [None, [1, 2]]
    assert back.schema.field("z").type == pa.list_(pa.int16(), 2)


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
    with pytest.raises(
        InputValidationError, match=r"'x': x\.b is stored as non-nullable"
    ):
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


# ---------------------------------------------------------------------------
# Appends the stored column cannot take: refused before buffering (ITL-1334)
# ---------------------------------------------------------------------------


def _new_non_null(inner: Any) -> Any:
    """``struct<a: float16, b: float16 not null>`` wrapped by ``inner``."""
    return inner(pa.struct([pa.field("a", F16), pa.field("b", F16, nullable=False)]))


def _old(inner: Any) -> Any:
    return inner(_struct(("a", F16)))


_WRAPPERS: dict[str, tuple[Any, Any, Any]] = {
    # name: (wrap a type, wrap an old value, wrap a new value)
    "top level": (lambda t: t, lambda v: v, lambda v: v),
    "in a large_list": (pa.large_list, lambda v: [v], lambda v: [v]),
    "in a fixed_size_list": (
        lambda t: pa.list_(t, 1),
        lambda v: [v],
        lambda v: [v],
    ),
    "in large_list<struct<fixed_size_list>>": (
        lambda t: pa.large_list(pa.struct([pa.field("s", pa.list_(t, 1))])),
        lambda v: [{"s": [v]}],
        lambda v: [{"s": [v]}],
    ),
}


def _assert_refused_and_nothing_pending(
    tmp_path: pathlib.Path, first: pa.Table, second: pa.Table, match: str
) -> None:
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, first, record_id_column="id", flush=True)
    with pytest.raises(InputValidationError, match=match):
        db.add_records(PATH, second, record_id_column="id")
    db.flush()  # nothing was buffered: the flush has nothing to fail on
    for reader in (db, DeltaTableDatabase(tmp_path)):
        back = reader.get_all_records(PATH, record_id_column="id")
        assert back is not None
        # Values, not types: a read gives text in its large variant.
        assert back.sort_by("id").to_pylist() == first.sort_by("id").to_pylist()


@pytest.mark.parametrize("where", sorted(_WRAPPERS))
def test_adding_a_non_nullable_struct_field_is_refused(
    tmp_path: pathlib.Path, where: str
) -> None:
    """The stored rows have no value for it, and Delta could not read the table back."""
    wrap_type, wrap_old, wrap_new = _WRAPPERS[where]
    first = _rows(
        [b"1", b"2"],
        x=([wrap_old({"a": 1.0}), wrap_old({"a": 2.0})], _old(wrap_type)),
        y=([wrap_old({"a": 1.0}), wrap_old({"a": 2.0})], _old(wrap_type)),
    )
    new = [wrap_new({"a": 3.0, "b": 4.0}), wrap_new({"a": 5.0, "b": 6.0})]
    second = _rows(
        [b"3", b"4"],
        x=(new, _new_non_null(wrap_type)),
        y=(new, _new_non_null(wrap_type)),
    )
    _assert_refused_and_nothing_pending(
        tmp_path, first, second, "'x'.*new non-nullable struct field"
    )


def test_adding_a_nullable_field_with_a_non_nullable_child_appends(
    tmp_path: pathlib.Path,
) -> None:
    """The stored rows read the new field as null, so its child needs no value.

    An unrecorded struct (a recorded one cannot gain a field on Delta: ITL-1403).
    """
    added = pa.struct(
        [pa.field("a", I64), pa.field("b", pa.struct([pa.field("c", I64, False)]))]
    )
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows([b"1"], x=([{"a": 1}], _struct(("a", I64)))),
        record_id_column="id",
        flush=True,
    )
    db.add_records(
        PATH,
        _rows([b"2"], x=([{"a": 2, "b": {"c": 3}}], added)),
        record_id_column="id",
        flush=True,
    )
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None
    assert back.sort_by("id").column("x").to_pylist() == [
        {"a": 1, "b": None},
        {"a": 2, "b": {"c": 3}},
    ]


def _named(name: str, unit: str = "m") -> Any:
    return pa.large_list(pa.field(name, I64, metadata={"unit": unit}))


def _child(unit: str) -> Any:
    return pa.struct([pa.field("a", I64, metadata={"unit": unit})])


# (first type, appended type, first value, appended value, reason): each change is
# tested in both orders.
_ATTRIBUTE_CHANGES: dict[str, tuple[Any, Any, Any, Any, str]] = {
    "list item name": (_named("itemA"), _named("itemB"), [1], [2], "is named"),
    "list item metadata": (
        _named("item", "m"),
        _named("item", "s"),
        [1],
        [2],
        "metadata",
    ),
    "struct child metadata": (_child("m"), _child("s"), {"a": 1}, {"a": 2}, "metadata"),
    "map keys_sorted": (
        pa.map_(pa.string(), I64, keys_sorted=True),  # pyright: ignore[reportCallIssue]  # the stubs lack keys_sorted
        pa.map_(pa.string(), I64, keys_sorted=False),  # pyright: ignore[reportCallIssue]  # the stubs lack keys_sorted
        [("a", 1), ("z", 2)],
        [("a", 3), ("b", 4)],
        "keys_sorted",
    ),
}


@pytest.mark.parametrize("order", ["forward", "reversed"])
@pytest.mark.parametrize("change", sorted(_ATTRIBUTE_CHANGES))
def test_an_append_changing_a_recorded_nested_attribute_is_refused(
    tmp_path: pathlib.Path, change: str, order: str
) -> None:
    """The record states these for every row: an append may not silently restate the
    appended rows' (a sorted-keys flag over unsorted keys would be a false type)."""
    first_type, second_type, first_value, second_value, reason = _ATTRIBUTE_CHANGES[
        change
    ]
    if order == "reversed":
        first_type, second_type = second_type, first_type
    first = _rows([b"1"], x=([first_value], first_type), y=([first_value], first_type))
    second = _rows(
        [b"2"], x=([second_value], second_type), y=([second_value], second_type)
    )
    _assert_refused_and_nothing_pending(tmp_path, first, second, f"'x'.*{reason}")


def test_adding_a_struct_field_inside_a_map_is_refused(tmp_path: pathlib.Path) -> None:
    """Delta's merge cannot add it: refused by column before buffering, not at flush."""
    old = pa.map_(pa.large_string(), _struct(("a", F16)))
    new = pa.map_(pa.large_string(), _struct(("a", F16), ("b", F16)))
    first = _rows(
        [b"1", b"2"],
        x=([[("k", {"a": 1.0})], [("k", {"a": 2.0})]], old),
        y=([[("k", {"a": 1.0})], [("k", {"a": 2.0})]], old),
    )
    second = _rows(
        [b"3", b"4"],
        x=([[("k", {"a": 3.0, "b": 4.0})], [("k", {"a": 5.0, "b": None})]], new),
        y=([[("k", {"a": 3.0, "b": 4.0})], [("k", {"a": 5.0, "b": None})]], new),
    )
    _assert_refused_and_nothing_pending(
        tmp_path, first, second, "'x'.*inside a map, which Delta cannot add"
    )


def _with_new_column_rows(ids: list[bytes], *, added: bool) -> pa.Table:
    """A recorded list, a recorded ``uint8`` and two logical lists; ``added`` adds a
    column the stored table does not have."""
    import pathlib as _pathlib

    from orcapod.contexts import get_default_context

    logical = get_default_context().type_converter.python_dicts_to_arrow_table(
        [{"s": {1, 2}, "p": [_pathlib.Path("/a")]} for _ in ids],
        python_schema={"s": set[int], "p": list[_pathlib.Path]},
    )
    table = _rows(
        ids,
        items=(
            [[1, 2]] * len(ids),
            pa.large_list(pa.field("item", I64, nullable=False)),
        ),
        u=([200] * len(ids), pa.uint8()),
    )
    table = table.append_column("k", pa.array([7] * len(ids), I64))
    table = table.append_column(logical.schema.field("s"), logical.column("s"))
    table = table.append_column(logical.schema.field("p"), logical.column("p"))
    if added:
        table = table.append_column("added", pa.array(["a"] * len(ids)))
    return table


@pytest.mark.parametrize("read", sorted(READS))
def test_a_new_column_keeps_every_other_columns_type(
    tmp_path: pathlib.Path, read: str
) -> None:
    """An append adding a column goes through a MERGE that writes each field's metadata
    from the rows: a list column handed to it keeps its record and a logical list its
    extension name, so neither reads back as Delta's plain list (ITL-1334)."""
    first = _with_new_column_rows([b"1"], added=False)
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, first, record_id_column="id", flush=True)
    db.add_records(
        PATH,
        _with_new_column_rows([b"2"], added=True),
        record_id_column="id",
        flush=True,
    )
    back = READS[read](DeltaTableDatabase(tmp_path))
    assert back is not None
    for name in ("items", "u", "s", "p"):
        assert str(back.schema.field(name).type) == str(first.schema.field(name).type)
    assert back.sort_by("id").column("added").to_pylist() == [None, "a"]


def test_struct_fields_differing_only_in_case_are_refused(
    tmp_path: pathlib.Path,
) -> None:
    """Delta field names are case-insensitive at every depth (ITL-1403): a struct
    holding ``x`` and ``X`` is refused, and so is an evolution that would add ``X``
    beside a stored ``x``, before anything is queued."""
    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(UnstorableColumnError, match="'x' and 'X' differ only in case"):
        db.add_records(
            PATH,
            _rows([b"1"], s=([{"x": 1, "X": 2}], _struct(("x", I64), ("X", I64)))),
            record_id_column="id",
        )
    db.add_records(
        PATH,
        _rows([b"1"], s=([{"x": 1}], _struct(("x", I64)))),
        record_id_column="id",
        flush=True,
    )
    with pytest.raises(InputValidationError, match="'x' and 'X' differ only in case"):
        db.add_records(
            PATH,
            _rows([b"2"], s=([{"X": 2}], _struct(("X", I64)))),
            record_id_column="id",
            flush=True,
        )
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None
    assert back.column("s").to_pylist() == [{"x": 1}]
