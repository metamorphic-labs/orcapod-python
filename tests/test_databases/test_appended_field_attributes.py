"""An append whose field attributes differ from the held column is refused (ITL-1334).

A table holds one type per column. Rows appended with other field metadata (the
column's own, or a nested field's: a struct child, a list item, a map key or item),
another nested field name or another ``keys_sorted`` would read back with the held
column's attributes, or the held rows with theirs. Every database that keeps such
attributes (in-memory, Delta, SpiralDB) refuses the append before anything is buffered,
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
DATABASES = ("in-memory", "Delta", "SpiralDB")


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
    """SpiralDB refuses metadata on a map's key or item field: its connector would
    not keep it (``spiraldb_connector._holds_map_field_metadata``)."""
    return database == "SpiralDB" and where.startswith("map") and unit is not None


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
