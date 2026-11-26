"""Nullability of SpiralDB tables: the non-nullable paths of a schema, and their check.

SpiralDB reports every top-level column (key columns included) and every struct field as
nullable, whatever was written; only list items keep a non-null flag (ITL-936). Orcapod
records each table's schema in the dataset's schema table (``spiraldb_schema_table``) and
enforces its non-nullable paths on read.

A path starts with a top-level column name; each further segment is a struct field name, or
``"[]"`` for the item of a ``list``, ``large_list`` or ``fixed_size_list``. Map interiors have
no path.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pyarrow as pa

from orcapod.errors import NullInNonNullableColumnError

ITEM = "[]"
ColumnPath = tuple[str, ...]


def render_path(path: ColumnPath) -> str:
    """Return ``path`` as text: ``g.x`` for a struct field, ``l[]`` for a list item."""
    out = path[0]
    for segment in path[1:]:
        out += ITEM if segment == ITEM else f".{segment}"
    return out


# ------------------------------------------------------------------- schema -> paths


def _storage(t: pa.DataType) -> pa.DataType:
    """Return the storage type of an extension type (SpiralDB stores the storage type)."""
    return t.storage_type if isinstance(t, pa.ExtensionType) else t


def field_members(t: pa.DataType) -> list[pa.Field[Any]]:
    """Return the nested fields a path can address: struct fields, or a list's item."""
    t = _storage(t)
    if pa.types.is_struct(t):
        return [t.field(i) for i in range(t.num_fields)]
    if (
        pa.types.is_list(t)
        or pa.types.is_large_list(t)
        or pa.types.is_fixed_size_list(t)
    ):
        return [t.value_field]
    return []


def _collect(field: pa.Field[Any], path: ColumnPath, out: set[ColumnPath]) -> None:
    """Add the non-nullable paths at and below ``field`` to ``out``.

    Args:
        field: The field to walk.
        path: The field's path.
        out: The set collecting non-nullable paths.
    """
    if not field.nullable:
        out.add(path)
    is_struct = pa.types.is_struct(_storage(field.type))
    for member in field_members(field.type):
        _collect(member, (*path, member.name if is_struct else ITEM), out)


def schema_paths(schema: pa.Schema) -> frozenset[ColumnPath]:
    """Return the non-nullable paths of ``schema``, nested fields included
    (map interiors excepted)."""
    out: set[ColumnPath] = set()
    for field in schema:
        _collect(field, (field.name,), out)
    return frozenset(out)


# ------------------------------------------------------------------- enforce


def _descend(arrays: list[Any], segment: str) -> list[Any]:
    """Return the arrays one path segment below ``arrays`` (a struct field or list items)."""
    arrays = [a.storage if isinstance(a, pa.ExtensionArray) else a for a in arrays]
    if segment == ITEM:
        return [a.flatten() for a in arrays]
    return [a.field(segment) for a in arrays]


def check_no_nulls(
    data: pa.Table | pa.RecordBatch, paths: Iterable[ColumnPath]
) -> None:
    """Raise if ``data`` holds a null at any of ``paths``.

    A struct field is checked over its whole array (Arrow's meaning of non-nullable), a
    list item over the items of the list values. A top-level column costs one null-count
    lookup; a nested path one vectorized pass over its values.

    Raises:
        NullInNonNullableColumnError: Naming the first path, in sorted order, with a null.
    """
    for path in sorted(paths):
        column = data.column(path[0])
        arrays: list[Any] = (
            list(column.chunks) if isinstance(column, pa.ChunkedArray) else [column]
        )
        for segment in path[1:]:
            arrays = _descend(arrays, segment)
        if any(a.null_count for a in arrays):
            raise NullInNonNullableColumnError(render_path(path))


def cast_changed_columns(batch: pa.RecordBatch, target: pa.Schema) -> pa.RecordBatch:
    """Return ``batch`` under ``target``, casting only the columns whose type changes.

    A column whose type is already the target's keeps its array: pyarrow refuses to
    cast (view) a fixed-size list with non-null items to its own type when a null row
    hides null item slots (ITL-1334), and a field's nullability or metadata needs no
    cast.

    Args:
        batch: Rows as read.
        target: The schema to give them, column for column.

    Returns:
        The rows with ``target``'s schema.
    """
    arrays = [
        column if column.type == field.type else column.cast(field.type)
        for column, field in zip(batch.columns, target)
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=target)
