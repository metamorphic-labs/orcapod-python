"""Keep a column's Arrow type through a Delta table (ITL-928, ITL-1334).

Delta Lake has no unsigned integer, no nanosecond, second or millisecond timestamp,
no zone other than UTC, no ``date64``, ``time``, ``duration``, ``float16`` or fixed-size
binary type, and no fixed-size list. The Delta writer converts the first kinds
(unsigned to the signed type of the same width, which fails for a value above the signed
maximum; every timestamp to microseconds, dropping nanosecond digits; a zone to UTC; a
fixed-size binary to ``large_binary``; a fixed-size list to a list, which reads back as
``large_list`` and, with a null row, cannot be read back as a fixed-size list at all),
names every list item ``element`` and drops its field metadata, drops a map's
``keys_sorted``, and cannot store the others.

A Delta database must give back the type it was given: a tag read back with another type
changes every record id computed from it. So before a write, a column whose type Delta
would not keep, or holds a list or a map, is recorded in its top-level field metadata
(``ARROW_TYPE_KEY``: the column's whole Arrow type, IPC-serialized; Delta keeps
top-level field metadata), and the values Delta cannot hold losslessly are written in
another form:

* an unsigned integer as the signed integer of the same width, bit for bit;
* ``timestamp[ns]``, ``date64``, ``time32``, ``time64`` and ``duration`` as their
  integer value (``int64``, or ``int32`` for ``time32``);
* ``float16`` as ``float32``, which holds every ``float16`` value exactly;
* a fixed-size list as a ``large_list`` of the same items (ITL-1275, ITL-1283);
* a fixed-size binary as ``large_binary``; a list or a map as it is (its item names,
  item metadata and ``keys_sorted`` come back from the record; ITL-1334).

What Delta cannot hold even so (``decimal256``, an interval) is refused before anything
is written: ``DeltaTableDatabase.require_storable`` asks the Delta writer's own
conversion about the column as it is written (``written_field``). So is a map whose key
or item field is not named ``key`` / ``value``: the record goes through Arrow IPC, which
renames them. A ``null``-typed value is not recorded: it fits where the table already
stores a type (the append check leaves it out), and a ``null`` part with no stored type
at its path is refused by ``add_records``.

``timestamp[s]``, ``timestamp[ms]`` and a zone other than UTC are left to Delta's own
conversion, which is lossless. A read restores each recorded column to its recorded type
(large-normalized), at every depth, because the whole column type is recorded: every
nested type is rebuilt with the recorded field names, nullability and metadata, struct
fields matched by NAME (an append may evolve a struct, and the record then describes the
merged column: ``merged_type``); a fixed-size list is rebuilt from the list Delta gives
back (a null row comes back with no items, which a cast cannot fill), refusing a row of
another length; a ``float16`` is checked to hold exactly the ``float32`` value read; and a
fixed-size binary to hold values of its width. A column whose type has an extension
(logical) type anywhere is left as it is: its type is restored from its extension
metadata.

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
    UTC, a ``date64``, ``time``, ``duration``, ``float16`` or fixed-size binary, any
    list or map (Delta names every list item ``element`` and drops its metadata, and
    drops a map's ``keys_sorted``), or a nested field with metadata (ITL-1334).
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
        or t.is_list(arrow_type)
        or t.is_large_list(arrow_type)
        or t.is_map(arrow_type)
    ):
        return True
    if t.is_timestamp(arrow_type):
        return arrow_type.unit != "us" or arrow_type.tz not in (None, "UTC")
    # A nested field with metadata: the record states it for every row, so an append
    # cannot silently restate it (ITL-1334).
    return any(
        _is_recorded(arrow_type.field(i).type) or bool(arrow_type.field(i).metadata)
        for i in range(arrow_type.num_fields)
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
        # Any: pyarrow's stubs type ``chunked_array``'s ``type`` too narrowly.
        stored_type: Any = _stored_form(field.type)
        column = pa.chunked_array(
            [_to_stored(chunk, field.type) for chunk in table.column(index).chunks],
            type=stored_type,
        )
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


def _to_stored(array: Any, original: Any) -> Any:
    """Return ``array``, of type ``original``, in the form it is written (``_stored_form``).

    A leaf is cast: an integer view of the same values (a wrap-around cast keeps the
    bits), ``float16`` widened to ``float32``. A nested type is rebuilt around its
    converted children, so a fixed-size list's null row becomes an empty list: the
    items a null row hides (null under a non-nullable item field) never reach Delta,
    whose writer panics on them (ITL-1334).
    """
    t = pa.types
    stored: Any = _stored_form(original)  # dispatched on below, as pyarrow allows
    if not _is_nested(original):
        return array.cast(stored, safe=False)
    mask = array.is_null() if array.null_count else None
    if t.is_fixed_size_list(original):
        valid = ~array.is_null().to_numpy(zero_copy_only=False)
        lengths = np.where(valid, original.list_size, 0)
        offsets = np.concatenate([[0], np.cumsum(lengths)]).astype(np.int64)
        # flatten() gives the items of the non-null rows only, in row order.
        return pa.LargeListArray.from_arrays(
            pa.array(offsets, pa.int64()),
            _to_stored(array.flatten(), original.value_type),
            type=stored,
            mask=mask,
        )
    if t.is_struct(original):
        children = [
            _to_stored(array.field(i), original.field(i).type)
            for i in range(original.num_fields)
        ]
        return pa.StructArray.from_arrays(children, fields=list(stored), mask=mask)
    if t.is_map(original):
        return pa.MapArray.from_arrays(
            array.offsets,
            _to_stored(array.keys, original.key_type),
            _to_stored(array.items, original.item_type),
            type=cast(Any, stored),  # pyarrow's stubs type ``type`` too narrowly
            mask=mask,
        )
    array_class = pa.LargeListArray if t.is_large_list(original) else pa.ListArray
    return array_class.from_arrays(
        array.offsets,
        _to_stored(array.values, original.value_type),
        type=stored,
        mask=mask,
    )


def _is_nested(arrow_type: pa.DataType) -> bool:
    """Whether ``arrow_type`` has child fields (a struct, a list of any kind, a map)."""
    return arrow_type.num_fields > 0 or pa.types.is_fixed_size_list(arrow_type)


def _restore(array: Any, target: Any) -> Any:
    """Return ``array``, as read from Delta, with the type ``target`` it was written as.

    Every nested type is rebuilt around its restored children, so each child field
    gets the recorded name, nullability and metadata (Arrow's own type equality ignores
    them, so no nested type is taken as already restored): a struct's children are
    matched by NAME, never by position, since an append that evolves the struct may
    store them in another order; a fixed-size list is rebuilt from the list read; a map
    gets its recorded key and item fields and ``keys_sorted``. A leaf is cast (values
    were written from its type, so the cast back is exact), and a ``float16`` is checked
    to hold exactly the value read.

    Raises:
        ValueError: The stored rows are not what was written: a list row has another
            length than its fixed size, a value is not a ``float16`` value, a
            fixed-size binary has another width, or a struct has other fields than
            recorded.
    """
    t = pa.types
    if not _is_nested(target):
        if array.type == target:
            return array
        restored = array.cast(target, safe=False)
        if t.is_float16(target) and not _same_floats(restored.cast(array.type), array):
            raise ValueError("a value is not a float16 value")
        return restored
    mask = array.is_null() if array.null_count else None
    if t.is_fixed_size_list(target):
        return _fixed_size_list(array, target)
    if t.is_struct(target):
        if not t.is_struct(array.type):
            raise ValueError(f"a struct is stored as {array.type}")
        stored = {array.type.field(i).name: i for i in range(array.type.num_fields)}
        recorded = [field.name for field in target]
        if set(stored) != set(recorded):
            raise ValueError(
                f"the stored struct fields {sorted(stored)} are not the recorded "
                f"{sorted(recorded)}"
            )
        children = [
            _restore(array.field(stored[field.name]), field.type) for field in target
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
    large = t.is_large_list(target)
    array_class = pa.LargeListArray if large else pa.ListArray
    return array_class.from_arrays(
        array.offsets.cast(pa.int64() if large else pa.int32()),
        _restore(array.values, target.value_type),
        type=target,
        mask=mask,
    )


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
    values = _restore(
        array.values.take(pa.array(indices, pa.int64(), mask=~filled)),
        target.value_type,
    )
    # Built from buffers, so a zero-width list keeps its row count (from_arrays would
    # take the count from the items, of which it has none).
    validity = None if valid.all() else pa.array(valid, pa.bool_()).buffers()[1]
    return pa.Array.from_buffers(
        target,
        rows,
        cast(Any, [validity]),  # the stubs do not allow an absent (None) buffer
        null_count=int((~valid).sum()),
        children=[values],
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


def merged_type(stored: pa.DataType, incoming: pa.DataType) -> pa.DataType:
    """The type a column holds once rows of ``incoming`` are appended to ``stored``.

    An append may evolve a struct (schema evolution): Delta keeps the stored fields
    and adds the new ones, so the merged struct has the stored fields, in their order,
    each merged with the incoming field of the same name, then the incoming fields the
    stored struct does not have. Lists, fixed-size lists and maps merge their items.
    Any other type is the stored one: the append check (``_types_conflict``) has
    already refused an incoming type that changes it.

    Args:
        stored: The column's type as stored (its recorded type, else its stored type).
        incoming: The type of the rows appended.

    Returns:
        The column's type after the append, to be recorded (ITL-1334).
    """
    t = pa.types
    if t.is_struct(stored) and t.is_struct(incoming):
        incoming_fields = {field.name: field for field in incoming}
        stored_names = {field.name for field in stored}
        fields = [
            field.with_type(merged_type(field.type, incoming_fields[field.name].type))
            if field.name in incoming_fields
            else field
            for field in stored
        ] + [field for field in incoming if field.name not in stored_names]
        return pa.struct(fields)
    if t.is_map(stored) and t.is_map(incoming):
        return pa.map_(  # pyright: ignore[reportCallIssue]  # pyarrow takes key/item fields
            stored.key_field.with_type(merged_type(stored.key_type, incoming.key_type)),
            stored.item_field.with_type(
                merged_type(stored.item_type, incoming.item_type)
            ),
            keys_sorted=stored.keys_sorted,
        )
    if (
        (t.is_large_list(stored) and t.is_large_list(incoming))
        or (t.is_list(stored) and t.is_list(incoming))
        or (
            t.is_fixed_size_list(stored)
            and t.is_fixed_size_list(incoming)
            and stored.list_size == incoming.list_size
        )
    ):
        value_field = stored.value_field.with_type(
            merged_type(stored.value_type, incoming.value_type)
        )
        if t.is_large_list(stored):
            return pa.large_list(value_field)
        if t.is_list(stored):
            return pa.list_(value_field)
        return pa.list_(value_field, stored.list_size)
    return stored


def with_recorded_type(
    field: "pa.Field[Any]", arrow_type: pa.DataType
) -> "pa.Field[Any]":
    """Return ``field`` with ``arrow_type`` recorded in its metadata (``ARROW_TYPE_KEY``)."""
    metadata = dict(field.metadata or {})
    metadata[ARROW_TYPE_KEY] = _serialize(arrow_type)
    return field.with_metadata(cast(Any, metadata))  # the stubs type keys too narrowly
