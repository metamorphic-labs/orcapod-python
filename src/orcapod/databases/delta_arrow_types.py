"""Keep a column's Arrow type through a Delta table (ITL-928, ITL-1334).

Delta Lake has no unsigned integer, no nanosecond, second or millisecond timestamp,
no zone other than UTC, no ``date64``, ``time``, ``duration``, ``float16`` or fixed-size
binary type, and no fixed-size list. The Delta writer converts the first kinds
(unsigned to the signed type of the same width, which fails for a value above the signed
maximum; every timestamp to microseconds, dropping nanosecond digits; a zone to UTC; a
fixed-size binary to ``large_binary``; a fixed-size list to a list, which reads back as
``large_list`` and, with a null row, cannot be read back as a fixed-size list at all),
renames the item field of a list whose items are non-null (``item`` to ``element``), and
cannot store the others.

A Delta database must give back the type it was given: a tag read back with another type
changes every record id computed from it. So before a write, a column whose type Delta
would not keep is recorded in its top-level field metadata (``ARROW_TYPE_KEY``: the
column's whole Arrow type, IPC-serialized; Delta keeps top-level field metadata but not
that of nested fields), and the values Delta cannot hold losslessly are written as
their integer form:

* an unsigned integer as the signed integer of the same width, bit for bit;
* ``timestamp[ns]``, ``date64``, ``time32``, ``time64`` and ``duration`` as their
  integer value (``int64``, or ``int32`` for ``time32``);
* ``float16`` as ``float32``, which holds every ``float16`` value exactly;
* a fixed-size list as a ``large_list`` of the same items (ITL-1275, ITL-1283);
* a fixed-size binary as ``large_binary``, and a list with non-null items as it is (its
  item field's name is restored from the record; ITL-1334).

What Delta cannot hold even so (``decimal256``, an interval) is refused before anything
is written: ``DeltaTableDatabase.require_storable`` asks the Delta writer's own
conversion about the column as it is written (``written_field``). A ``null``-typed value
is not recorded: it fits a column already stored with a type (the append check leaves it
out), and a column with no stored type yet is refused by ``add_records``.

``timestamp[s]``, ``timestamp[ms]`` and a zone other than UTC are left to Delta's own
conversion, which is lossless. A read casts each recorded column back to its recorded
type (large-normalized), at every depth, because the whole column type is recorded: a
fixed-size list is rebuilt from the list Delta gives back (a null row comes back with no
items, which a cast cannot fill), refusing a row of another length; a ``float16`` is
checked to hold exactly the ``float32`` value read; and a fixed-size binary to hold
values of its width. A column whose type has an
extension (logical) type anywhere is left as it is: its type is restored from its
extension metadata.

A table written before this existed has no recorded type and reads back with Delta's
types; such a database must be rebuilt.
"""

from __future__ import annotations

import base64
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any, cast

from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import numpy as np
    import pyarrow as pa
else:
    np = LazyModule("numpy")
    pa = LazyModule("pyarrow")

# Field metadata key holding a column's original Arrow type.
ARROW_TYPE_KEY = b"orcapod:arrow_type"


def _has_extension_type(arrow_type: pa.DataType) -> bool:
    """Return whether ``arrow_type`` is or contains an extension type."""
    if isinstance(arrow_type, pa.ExtensionType):
        return True
    return any(
        _has_extension_type(arrow_type.field(i).type)
        for i in range(arrow_type.num_fields)
    )


def _stored_form(arrow_type: pa.DataType) -> pa.DataType:
    """Return the type a column of ``arrow_type`` is written as, at every depth.

    Args:
        arrow_type: A type with no extension type in it.

    Returns:
        ``arrow_type`` with each unsigned integer replaced by the signed integer of
        the same width, each ``timestamp[ns]``, ``date64``, ``time`` and
        ``duration`` by its integer type, each ``float16`` by ``float32`` and each
        fixed-size list by a ``large_list`` of its items; every other part unchanged.
    """
    t = pa.types
    if t.is_float16(arrow_type):
        return pa.float32()
    if t.is_fixed_size_binary(arrow_type):
        return pa.large_binary()
    if t.is_unsigned_integer(arrow_type):
        return {8: pa.int8(), 16: pa.int16(), 32: pa.int32(), 64: pa.int64()}[
            arrow_type.bit_width
        ]
    if (t.is_timestamp(arrow_type) and arrow_type.unit == "ns") or (
        t.is_date64(arrow_type) or t.is_time64(arrow_type) or t.is_duration(arrow_type)
    ):
        return pa.int64()
    if t.is_time32(arrow_type):
        return pa.int32()
    if t.is_large_list(arrow_type):
        return pa.large_list(_stored_field(arrow_type.value_field))
    if t.is_list(arrow_type):
        return pa.list_(_stored_field(arrow_type.value_field))
    if t.is_fixed_size_list(arrow_type):
        return pa.large_list(_stored_field(arrow_type.value_field))
    if t.is_struct(arrow_type):
        return pa.struct([_stored_field(f) for f in arrow_type])
    if t.is_map(arrow_type):
        # pyarrow accepts key and item Fields here; its stubs only list types.
        return pa.map_(  # pyright: ignore[reportCallIssue]
            _stored_field(arrow_type.key_field),
            _stored_field(arrow_type.item_field),
            keys_sorted=arrow_type.keys_sorted,
        )
    return arrow_type


def _stored_field(field: "pa.Field[Any]") -> "pa.Field[Any]":
    """Return ``field`` with its type in the form it is written as."""
    return field.with_type(_stored_form(field.type))


def _serialize(arrow_type: pa.DataType) -> bytes:
    """Return ``arrow_type`` as field metadata text (base64 of an IPC schema)."""
    return base64.b64encode(
        pa.schema([pa.field("type", arrow_type)]).serialize().to_pybytes()
    )


def _deserialize(text: bytes) -> pa.DataType:
    """Return the Arrow type ``_serialize`` wrote as ``text``."""
    return pa.ipc.read_schema(pa.py_buffer(base64.b64decode(text))).field(0).type


def _is_recorded(arrow_type: pa.DataType) -> bool:
    """Return whether a column of ``arrow_type`` needs its type recorded.

    It does when it has, at any depth, a part Delta would not give back: an unsigned
    integer, a timestamp in another unit than microseconds or with a zone other than
    UTC, a ``date64``, ``time``, ``duration``, ``float16`` or fixed-size binary,
    a fixed-size list, or a list whose items are non-null (Delta renames its item field).
    """
    t = pa.types
    if (
        t.is_unsigned_integer(arrow_type)
        or t.is_date64(arrow_type)
        or t.is_time(arrow_type)
        or t.is_duration(arrow_type)
        or t.is_float16(arrow_type)
        or t.is_fixed_size_binary(arrow_type)
        or t.is_fixed_size_list(arrow_type)
    ):
        return True
    if (t.is_list(arrow_type) or t.is_large_list(arrow_type)) and not (
        arrow_type.value_field.nullable
    ):
        return True
    if t.is_timestamp(arrow_type):
        return arrow_type.unit != "us" or arrow_type.tz not in (None, "UTC")
    return any(
        _is_recorded(arrow_type.field(i).type) for i in range(arrow_type.num_fields)
    )


def to_delta_layout(table: pa.Table) -> pa.Table:
    """Return ``table`` as it is written to Delta, each changed column's type recorded.

    Args:
        table: The rows about to be written.

    Returns:
        The table with each recorded column (``_is_recorded``) converted to its
        stored form (``_stored_form``, values unchanged bit for bit) and its original
        type in its field metadata; every other column unchanged.
    """
    for index, field in enumerate(table.schema):
        if _has_extension_type(field.type) or not _is_recorded(field.type):
            continue
        metadata = dict(field.metadata or {})
        metadata[ARROW_TYPE_KEY] = _serialize(field.type)
        stored_type = _stored_form(field.type)
        # An integer view of the same values: a wrap-around cast keeps the bits.
        column = table.column(index).cast(stored_type, safe=False)
        table = table.set_column(
            index,
            pa.field(field.name, stored_type, field.nullable, metadata),
            column,
        )
    return table


def from_delta_layout(table: pa.Table) -> pa.Table:
    """Return rows read from Delta with each recorded column cast back to its type.

    Args:
        table: Rows as read from the Delta table.

    Returns:
        The rows with every column that carries ``ARROW_TYPE_KEY`` in its type
        (large-normalized) and without that key; other columns unchanged.
    """
    for index, field in enumerate(table.schema):
        metadata = field.metadata or {}
        if ARROW_TYPE_KEY not in metadata:
            continue
        # Any: pyarrow's stubs type ``chunked_array``'s ``type`` too narrowly.
        original: Any = arrow_utils.normalize_to_large_types(
            _deserialize(metadata[ARROW_TYPE_KEY])
        )
        rest = {k: v for k, v in metadata.items() if k != ARROW_TYPE_KEY}
        column = table.column(index)
        try:
            restored = pa.chunked_array(
                [_restore(chunk, original) for chunk in column.chunks], type=original
            )
        except ValueError as exc:
            raise ValueError(
                f"Column {field.name!r} cannot be read back as its recorded type "
                f"{original}: {exc}"
            ) from exc
        table = table.set_column(
            index,
            pa.field(field.name, original, field.nullable, rest or None),
            restored,
        )
    return table


def _needs_rebuild(arrow_type: pa.DataType) -> bool:
    """Whether ``arrow_type`` has, at any depth, a part a cast cannot restore exactly
    (a fixed-size list, which a null row breaks) or must check (a ``float16``)."""
    t = pa.types
    if t.is_fixed_size_list(arrow_type) or t.is_float16(arrow_type):
        return True
    return any(
        _needs_rebuild(arrow_type.field(i).type) for i in range(arrow_type.num_fields)
    )


def _restore(array: Any, target: Any) -> Any:
    """Return ``array``, as read from Delta, with the type ``target`` it was written as.

    Values were written from ``target``, so a cast restores them, except where
    ``_needs_rebuild``: a fixed-size list is rebuilt from the list read, and a
    ``float16`` is checked to hold exactly the value read. Containers holding one are
    rebuilt around their restored children.

    Raises:
        ValueError: A list row has another length than its fixed size, a value is not
            a ``float16`` value, or a fixed-size binary has another width: the stored
            rows are not what was written.
    """
    if array.type == target:
        return array
    t = pa.types
    if not _needs_rebuild(target):
        # Values were written from this type, so the cast back is exact.
        return array.cast(target, safe=False)
    mask = array.is_null() if array.null_count else None
    if t.is_float16(target):
        restored = array.cast(target, safe=False)
        if not _same_floats(restored.cast(array.type), array):
            raise ValueError("a value is not a float16 value")
        return restored
    if t.is_fixed_size_list(target):
        return _fixed_size_list(array, target)
    if t.is_struct(target):
        children = [
            _restore(array.field(i), target.field(i).type)
            for i in range(target.num_fields)
        ]
        return pa.StructArray.from_arrays(children, fields=list(target), mask=mask)
    if t.is_map(target):
        return pa.MapArray.from_arrays(
            array.offsets,
            _restore(array.keys, target.key_type),
            _restore(array.items, target.item_type),
            type=cast(Any, target),  # pyarrow's stubs type ``type`` too narrowly
            mask=mask,
        )
    if t.is_large_list(target) or t.is_list(target):
        array_class = pa.LargeListArray if t.is_large_list(target) else pa.ListArray
        return array_class.from_arrays(
            array.offsets,
            _restore(array.values, target.value_type),
            type=target,
            mask=mask,
        )
    raise ValueError(f"no restore for {target}")  # pragma: no cover  # every kind above


def _same_floats(a: Any, b: Any) -> bool:
    """Whether two float arrays hold the same values and nulls (NaN equal to NaN)."""
    if a.null_count != b.null_count or not a.is_null().equals(b.is_null()):
        return False
    left = a.to_numpy(zero_copy_only=False)
    right = b.to_numpy(zero_copy_only=False)
    return bool(np.array_equal(left, right, equal_nan=True))


def _fixed_size_list(array: Any, target: Any) -> Any:
    """Rebuild a fixed-size list column from the list Delta gives back.

    A valid row holds its items; a null row may hold none (Delta writes no items for a
    null list), so its slots are filled with nulls, which no reader sees.

    Raises:
        ValueError: A valid row holds another number of items than the fixed size.
    """
    size = target.list_size
    rows = len(array)
    offsets = array.offsets.to_numpy(zero_copy_only=False).astype(np.int64)
    lengths = np.diff(offsets)
    valid = ~array.is_null().to_numpy(zero_copy_only=False)
    if np.any(lengths[valid] != size):
        raise ValueError(f"a list row does not hold {size} items")
    indices = np.repeat(offsets[:-1], size) + np.tile(np.arange(size), rows)
    filled = np.repeat(valid | (lengths == size), size)
    values = array.values.take(pa.array(indices, pa.int64(), mask=~filled))
    return pa.FixedSizeListArray.from_arrays(
        _restore(values, target.value_type),
        type=target,
        mask=None if valid.all() else pa.array(~valid),
    )


def recorded_type(
    metadata: "Mapping[bytes, bytes] | Mapping[str, str] | None",
) -> pa.DataType | None:
    """Return the Arrow type recorded in a field's metadata, or ``None``.

    Args:
        metadata: The field metadata, as pyarrow gives it (bytes) or as the Delta
            log gives it (text).

    Returns:
        The recorded type, or ``None`` when the field has none.
    """
    for key, value in (metadata or {}).items():
        if (key.encode() if isinstance(key, str) else key) == ARROW_TYPE_KEY:
            return _deserialize(value.encode() if isinstance(value, str) else value)
    return None


def first_column_out_of_stored_range(
    table: pa.Table, names: "Collection[str]"
) -> str | None:
    """Return the first named column with a value its written form cannot hold.

    An unsigned integer is written as the signed integer of the same width, bit for
    bit; a column that records its type reads it back exactly. A column whose stored
    type is not recorded (written before types were recorded) would read such a value
    back as a negative number, so the caller refuses it.

    Args:
        table: The rows about to be written, with their own types.
        names: The columns to check: those stored without a recorded type.

    Returns:
        The first such column's name, or ``None``.
    """
    for name in names:
        index = table.schema.get_field_index(name)
        if index < 0:
            continue
        arrow_type = table.schema.field(index).type
        if _has_extension_type(arrow_type) or not _is_recorded(arrow_type):
            continue
        try:
            table.column(index).cast(_stored_form(arrow_type), safe=True)
        except pa.ArrowInvalid:
            return name
    return None


def written_field(field: "pa.Field[Any]") -> "pa.Field[Any]":
    """Return ``field`` as ``to_delta_layout`` writes it: in its stored form when its
    type is recorded, otherwise unchanged (metadata aside).

    Args:
        field: A column, as given to the database.

    Returns:
        The field Delta is handed for it.
    """
    if _has_extension_type(field.type) or not _is_recorded(field.type):
        return field
    return _stored_field(field)
