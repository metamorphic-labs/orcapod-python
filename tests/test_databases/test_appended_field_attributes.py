"""An append whose field attributes differ from the held column is refused (ITL-1334).

A table holds one type per column. Rows appended with other field metadata (the
column's own, or a nested field's: a struct child, a list item, a map key or item),
another nested field name or another ``keys_sorted`` would read back with the held
column's attributes, or the held rows with theirs. Every database that keeps such
attributes (in-memory, Delta) refuses the append before anything is buffered,
naming the column and the path, against the stored table and against a batch still
pending; the held rows read back unchanged. Nullability is left out: whether a
nullability change is refused belongs to the nullability audit (ITL-842).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.errors import InputValidationError, UnstorableColumnError
from tests.test_databases.type_matrix import (
    expected_field,
    field_difference,
    map_of_fields,
    open_database,
)

PATH = ("t",)
I64 = pa.int64()
DATABASES = ("in-memory", "Delta")


def _meta(unit: str | None) -> dict[str, str] | None:
    return None if unit is None else {"unit": unit}


def _column(unit: str | None) -> Any:
    """The column ``x`` itself carries the metadata."""
    return pa.field("x", I64, metadata=_meta(unit))


def _struct_child(unit: str | None) -> Any:
    return pa.field("x", pa.struct([pa.field("a", I64, metadata=_meta(unit))]))


def _list_item(unit: str | None) -> Any:
    return pa.field("x", pa.large_list(pa.field("item", I64, metadata=_meta(unit))))


def _map_key(unit: str | None) -> Any:
    key = pa.field("key", pa.large_string(), nullable=False, metadata=_meta(unit))
    return pa.field("x", map_of_fields(key, pa.field("value", I64)))


def _map_item(unit: str | None) -> Any:
    key = pa.field("key", pa.large_string(), nullable=False)
    return pa.field(
        "x", map_of_fields(key, pa.field("value", I64, metadata=_meta(unit)))
    )


# (field builder, the path the refusal names, values of the first and second batch)
WHERE: dict[str, tuple[Any, str, list[Any], list[Any]]] = {
    "column": (_column, "x has", [1, 2], [3, 4]),
    "struct child": (
        _struct_child,
        "x.a has",
        [{"a": 1}, {"a": 2}],
        [{"a": 3}, {"a": 4}],
    ),
    "list item": (_list_item, r"x\[\] has", [[1], [2, 3]], [[4], [5, 6]]),
    "map key": (
        _map_key,
        "x.key has",
        [[("k", 1)], [("l", 2)]],
        [[("m", 3)], [("n", 4)]],
    ),
    "map item": (
        _map_item,
        "x.item has",
        [[("k", 1)], [("l", 2)]],
        [[("m", 3)], [("n", 4)]],
    ),
}

# (first metadata, appended metadata): absent to present, present to absent, A to B and
# B to A.
CHANGES = {
    "absent -> present": (None, "m"),
    "present -> absent": ("m", None),
    "m -> s": ("m", "s"),
    "s -> m": ("s", "m"),
}


def _refuses_the_type(database: str, where: str, unit: str | None) -> bool:
    """Whether ``database`` cannot store the column at all: neither does."""
    return False


def _rows(ids: list[bytes], field: Any, values: list[Any]) -> pa.Table:
    """Two columns, ``x`` and ``y``, of ``field``'s type, attributes and values."""
    array = pa.array(values, field.type)
    return pa.Table.from_arrays(
        [pa.array(ids, pa.large_binary()), array, array],
        schema=pa.schema(
            [pa.field("id", pa.large_binary()), field, field.with_name("y")]
        ),
    )


def _values(table: pa.Table | None) -> list[dict[str, Any]]:
    assert table is not None
    return table.sort_by("id").select(["id", "x", "y"]).to_pylist()


def _queued_values(db: Any) -> list[dict[str, Any]]:
    """The rows queued for the table, as ``_values`` gives them (``[]`` for none)."""
    queued = db._pending_batches.get("t")  # the queue is the subject here
    if queued is None:
        return []
    return _values(
        queued.rename_columns(
            ["id" if n == db.RECORD_ID_COLUMN else n for n in queued.column_names]
        )
    )


@pytest.mark.parametrize("pending", [False, True], ids=["stored", "pending"])
@pytest.mark.parametrize("change", sorted(CHANGES))
@pytest.mark.parametrize("where", sorted(WHERE))
@pytest.mark.parametrize("database", DATABASES)
def test_an_append_with_other_field_attributes_is_refused(
    tmp_path: Path, database: str, where: str, change: str, pending: bool
) -> None:
    build, path, first_values, second_values = WHERE[where]
    first_unit, second_unit = CHANGES[change]
    first = _rows([b"1", b"2"], build(first_unit), first_values)
    second = _rows([b"3", b"4"], build(second_unit), second_values)
    with open_database(database, tmp_path) as db:
        if _refuses_the_type(database, where, first_unit):
            with pytest.raises(UnstorableColumnError, match="'x'"):
                db.add_records(PATH, first, record_id_column="id", flush=not pending)
            return
        db.add_records(PATH, first, record_id_column="id", flush=not pending)
        if _refuses_the_type(database, where, second_unit):
            with pytest.raises(UnstorableColumnError, match="'x'"):
                db.add_records(PATH, second, record_id_column="id")
        else:
            with pytest.raises(InputValidationError, match=f"'x': {path}"):
                db.add_records(PATH, second, record_id_column="id")
        # The queue holds exactly what it held before: the first rows when they
        # were not flushed, nothing otherwise.
        assert _queued_values(db) == (_values(first) if pending else [])
        db.flush()  # nothing of the refused rows was buffered
        back = db.get_all_records(PATH, record_id_column="id")
        assert _values(back) == _values(first)
        assert back is not None
        _assert_kept_field(first, back)
    if database == "Delta":
        with open_database(database, tmp_path) as fresh:
            again = fresh.get_all_records(PATH, record_id_column="id")
            assert _values(again) == _values(first)
            assert again is not None
            _assert_kept_field(first, again)


def _assert_kept_field(written: pa.Table, back: pa.Table) -> None:
    """``x`` reads back as written (every nested name, nullability and metadata, a
    map's ``keys_sorted``), or as its large variant, compared field by field."""
    exact = written.schema.field("x")
    got = back.schema.field("x")
    assert (
        field_difference(exact, got, "x") is None
        or field_difference(expected_field(exact), got, "x") is None
    ), field_difference(expected_field(exact), got, "x")


@pytest.mark.parametrize("database", DATABASES)
def test_an_append_with_the_same_field_attributes_is_kept(
    tmp_path: Path, database: str
) -> None:
    """The control: the same attributes append, and both batches read back."""
    for where in sorted(WHERE):
        build, _, first_values, second_values = WHERE[where]
        first = _rows([b"1", b"2"], build("m"), first_values)
        second = _rows([b"3", b"4"], build("m"), second_values)
        with open_database(database, tmp_path / where) as db:
            if _refuses_the_type(database, where, "m"):
                with pytest.raises(UnstorableColumnError, match="'x'"):
                    db.add_records(PATH, first, record_id_column="id", flush=True)
                continue
            db.add_records(PATH, first, record_id_column="id", flush=True)
            db.add_records(PATH, second, record_id_column="id", flush=True)
            back = db.get_all_records(PATH, record_id_column="id")
            assert _values(back) == _values(first) + _values(second), where


def _cast_leaf(field: Any, leaf: Any) -> Any:
    """``field`` with its int64 leaf (the column, or the struct child ``a``) of type
    ``leaf``, its attributes kept."""
    if pa.types.is_struct(field.type):
        return field.with_type(pa.struct([field.type.field(0).with_type(leaf)]))
    return field.with_type(leaf)


@pytest.mark.parametrize("stored", [False, True], ids=["pending", "stored and pending"])
@pytest.mark.parametrize("change", sorted(CHANGES))
@pytest.mark.parametrize("where", ["column", "struct child"])
@pytest.mark.parametrize("mode", ["coerce", "merge"])
def test_delta_compares_attributes_before_schema_handling_casts(
    tmp_path: Path, mode: str, where: str, change: str, stored: bool
) -> None:
    """A ``"coerce"`` or ``"merge"`` casts the rows to the pending batch's schema,
    which gave them its field metadata: ``unit=s`` rows were queued and stored as
    ``unit=m`` (CodeRabbit on PR 471). The rows' attributes are compared as given,
    so the append is refused and nothing queued changes. Each case casts: ``int32``
    rows coerced to ``int64``, ``null``-typed rows merged with ``int64``."""
    build, path, first_values, _ = WHERE[where]
    first_unit, second_unit = CHANGES[change]
    first = _rows([b"1", b"2"], build(first_unit), first_values)
    leaf = pa.int32() if mode == "coerce" else pa.null()
    values = (
        ([3, 4] if where == "column" else [{"a": 3}, {"a": 4}])
        if mode == "coerce"
        else ([None, None] if where == "column" else [{"a": None}, {"a": None}])
    )
    second = _rows([b"3", b"4"], _cast_leaf(build(second_unit), leaf), values)
    with open_database("Delta", tmp_path) as db:
        db.add_records(PATH, first, record_id_column="id", flush=stored)
        queued = first
        if stored:
            queued = _rows([b"5", b"6"], build(first_unit), first_values)
            db.add_records(PATH, queued, record_id_column="id")
        with pytest.raises(InputValidationError, match=f"'x': {path}"):
            db.add_records(PATH, second, record_id_column="id", schema_handling=mode)
        pending = db._pending_batches["t"]  # pyright: ignore[reportPrivateUsage]
        assert pending.schema.field("x").equals(
            queued.schema.field("x"), check_metadata=True
        )
        # The queued rows are the ones queued before the refusal, value for value.
        assert _queued_values(db) == _values(queued)
        db.flush()
    # Read back: exactly the rows written before the refusal (rows 5 and 6 with
    # the stored rows 1 and 2), with their attributes.
    written = pa.concat_tables([first, queued]) if stored else first
    with open_database("Delta", tmp_path) as fresh:
        back = fresh.get_all_records(PATH, record_id_column="id")
        assert back is not None
        assert _values(back) == _values(written)
        _assert_kept_field(first, back)
