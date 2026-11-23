"""Name the columns delta-rs keeps file statistics for, when a Delta table is created (ITL-1187).

By default delta-rs collects minimum and maximum statistics for the first 32 columns of
every file it writes, and logs a warning (``deltalake_core::writer::stats``, "Skipping
column ... because it's a binary field") for each binary column on every write. Orcapod
stores hashes and record ids as binary columns, so every commit logged a few of these
lines.

A table that ``DeltaTableDatabase`` creates therefore names its statistics columns in a
table property, ``delta.dataSkippingStatsColumns``, set by ``statistics_configuration``:
the one rule for that list. A column is listed when it is a top-level column whose type,
or the storage type of its extension type, is one delta-rs makes statistics for: an
integer, ``float32`` or ``float64``, a boolean, a string, ``date32``, a timestamp or
``decimal128``. Every other column is left out: binary of any kind, lists, maps,
structs, ``null`` and the rest. A column whose name holds a comma or a backtick is left
out too: delta-rs splits the property on commas before it reads quoting, and it cannot
MERGE into a table with a backtick in a column name whether or not the column is listed
(a delta-rs limitation, seen with 1.0.2). Each listed name is put in backticks, so a
dot, a space or ``::`` in it is part of the name. delta-rs 1.0.2 matches each listed name
against the last name of every column path in the file, nested ones included: listing
``v`` also selects ``rec.v`` of a struct ``rec``, and warns when that field is binary. So a
top-level column is left out when its name is also the name of a nested field anywhere in
the schema, or of a part delta-rs gives a nested column in Parquet (``element`` and
``list`` when the schema has a list, ``key_value``, ``key`` and ``value`` when it has a
map). The list is
deliberately an allowlist: delta-rs 1.0.2 refuses the whole write when the property
names a list, a map, a struct with a binary field or a missing column, and still warns
for a named binary column.

When no column qualifies, the property would be empty, which delta-rs refuses; such a
table is created with ``delta.dataSkippingNumIndexedCols`` set to ``0`` (no statistics)
instead.

The property is set once, when the table is created, and Orcapod never changes it. A
column added later by schema evolution is not in the list: delta-rs makes no statistics
for it and logs nothing for it, unless it has a nested binary field named like a listed
column (then delta-rs warns for that field). A table created before this change has no property and is
read and appended to as before, unaltered; delta-rs keeps logging its warnings (see
``docs/troubleshooting.md``). The property is Delta table metadata only: it changes no
stored value, column, hash, path or layout.

Orcapod's own lookups do not use these statistics: they read the table into memory and
answer from an index, and the columns they filter on (record ids and hashes) are binary.
The statistics serve other Delta readers that filter a table on its listed columns.
"""

from __future__ import annotations

import pyarrow as pa

STATS_COLUMNS_PROPERTY = "delta.dataSkippingStatsColumns"
NUM_INDEXED_COLUMNS_PROPERTY = "delta.dataSkippingNumIndexedCols"


def _has_statistics(arrow_type: pa.DataType) -> bool:
    """Return whether delta-rs makes min/max statistics for a column of ``arrow_type``.

    Args:
        arrow_type: The column's type as it is written to Delta.

    Returns:
        True for an integer, ``float32``/``float64``, boolean, string, ``date32``,
        timestamp or ``decimal128`` column, or an extension type stored as one of
        those; False for every other type.
    """
    if isinstance(arrow_type, pa.ExtensionType):
        arrow_type = arrow_type.storage_type
    t = pa.types
    return (
        t.is_integer(arrow_type)
        or (t.is_floating(arrow_type) and arrow_type.bit_width in (32, 64))
        or t.is_boolean(arrow_type)
        or t.is_string(arrow_type)
        or t.is_large_string(arrow_type)
        or t.is_date32(arrow_type)
        or t.is_timestamp(arrow_type)
        or t.is_decimal128(arrow_type)
    )


def _nested_names(arrow_type: pa.DataType) -> set[str]:
    """Return every name a nested part of a column of ``arrow_type`` has, at any depth.

    Args:
        arrow_type: A column's type as it is written to Delta.

    Returns:
        The names of the fields inside the type (struct fields, list items, map keys
        and values) and the names delta-rs gives those parts in Parquet: ``list`` and
        ``element`` for a list, ``key_value``, ``key`` and ``value`` for a map. Empty
        for a type with no nested part.
    """
    if isinstance(arrow_type, pa.ExtensionType):
        arrow_type = arrow_type.storage_type
    names: set[str] = set()
    t = pa.types
    if (
        t.is_list(arrow_type)
        or t.is_large_list(arrow_type)
        or t.is_fixed_size_list(arrow_type)
    ):
        names |= {"list", "element"}
    if t.is_map(arrow_type):
        names |= {"key_value", "key", "value"}
    for i in range(arrow_type.num_fields):
        child = arrow_type.field(i)
        names.add(child.name)
        names |= _nested_names(child.type)
    return names


def statistics_columns(schema: pa.Schema) -> list[str]:
    """Return the names of the columns a new table keeps statistics for, in schema order.

    Args:
        schema: The schema of the rows that create the table, as written to Delta.

    Returns:
        The top-level columns whose type delta-rs makes statistics for, whose name holds
        no comma and no backtick, and whose name is no nested part's name
        (``_nested_names``) in any column of ``schema``.
    """
    nested: set[str] = set()
    for field in schema:
        nested |= _nested_names(field.type)
    return [
        field.name
        for field in schema
        if _has_statistics(field.type)
        and "," not in field.name
        and "`" not in field.name
        and field.name not in nested
    ]


def statistics_configuration(schema: pa.Schema) -> dict[str, str]:
    """Return the table properties that name a new Delta table's statistics columns.

    Args:
        schema: The schema of the rows that create the table, as written to Delta.

    Returns:
        ``{"delta.dataSkippingStatsColumns": <names in backticks, comma-separated>}``, or
        ``{"delta.dataSkippingNumIndexedCols": "0"}`` when no column qualifies.
    """
    names = statistics_columns(schema)
    if not names:
        return {NUM_INDEXED_COLUMNS_PROPERTY: "0"}
    return {STATS_COLUMNS_PROPERTY: ",".join(f"`{name}`" for name in names)}
