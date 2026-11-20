"""Key columns prepared for Polars: the one place (ITL-1284, from ITL-1250).

An operator that matches, groups or orders rows with Polars gives Polars only its key
columns (and row numbers), prepared here, and takes every value of its output from its
input's Arrow tables. Polars changes some Arrow types on the way in and out (a
``date64`` value comes back at midnight) and panics on others (``decimal256``: a panic
is not an error a caller can catch), so no column reaches Polars unprepared, and none
that has no rule here.

Prepared for equality and order alike:

* a logical type is its storage;
* a date, time, timestamp or duration is its integer (the unit and the zone are the
  column's type, so the integers compare and order as the values do);
* a ``float16`` becomes ``float32`` (exact; the Polars this library allows may lack
  ``Float16``), a string or binary view its large variant;
* a ``decimal256`` becomes its four 64-bit limbs;
* a list, large list or fixed-size list becomes a large list over its prepared values;
* a struct (matching only: ``structs=True``) a struct of its prepared children.

Every other type (a map, an interval, a union, a list view, a dictionary, ...) has no
rule: ``part_without_rule`` names it, and the caller refuses the column, naming it. No
stream carries a dictionary-encoded column (ITL-1264), so none reaches an operator.

``Join``, ``SemiJoin``, ``MergeJoin`` (through ``Join``'s inner join) and ``GroupBy``
use this module; ``GroupBy``'s rule refuses structs (``structs=False``: a struct has
no decided order).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import numpy as np
    import polars as pl
    import pyarrow as pa
else:
    np = LazyModule("numpy")
    pl = LazyModule("polars")
    pa = LazyModule("pyarrow")


def is_list_kind(arrow_type: "pa.DataType") -> bool:
    """Return whether a type is a list, a large list or a fixed-size list."""
    return (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    )


def value_type(arrow_type: "pa.DataType") -> "pa.DataType":
    """Return the type a column's values are read as: a logical type's storage.

    At the top level; ``part_without_rule`` goes down.

    Args:
        arrow_type: The Arrow type of a column, or of a list's or struct's values.

    Returns:
        The type of the plain values.
    """
    while isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    return arrow_type


def _is_prepared_scalar(arrow_type: "pa.DataType") -> bool:
    """Return whether a scalar (storage) type has a rule here.

    Every integer, float, decimal, boolean, string, binary, date, time, timestamp
    and duration type, and the null type.
    """
    types = pa.types
    return (
        types.is_null(arrow_type)
        or types.is_boolean(arrow_type)
        or types.is_integer(arrow_type)
        or types.is_floating(arrow_type)
        or types.is_decimal(arrow_type)
        or types.is_string(arrow_type)
        or types.is_large_string(arrow_type)
        or types.is_string_view(arrow_type)
        or types.is_binary(arrow_type)
        or types.is_large_binary(arrow_type)
        or types.is_binary_view(arrow_type)
        or types.is_fixed_size_binary(arrow_type)
        or types.is_date(arrow_type)
        or types.is_time(arrow_type)
        or types.is_timestamp(arrow_type)
        or types.is_duration(arrow_type)
    )


def part_without_rule(
    arrow_type: "pa.DataType", *, structs: bool
) -> "pa.DataType | None":
    """Return the part of a key column's type this module has no rule for, or ``None``.

    The one rule for what may reach Polars, used from a schema (validation) and from
    a table (before ``polars_ready``), so nothing reaches Polars unprepared. A list
    of any kind goes down to its values, a struct (when ``structs``) to its
    children, through logical storage at every depth.

    Args:
        arrow_type: The Arrow type of a key column.
        structs: Whether a struct is prepared (matching, field by field) or has no
            rule (an order, which nobody has decided for a struct).

    Returns:
        The first type found that has no rule, or ``None``.
    """
    plain = value_type(arrow_type)
    if is_list_kind(plain):
        lists = cast(
            "pa.LargeListType[Any] | pa.ListType[Any] | pa.FixedSizeListType[Any, Any]",
            plain,
        )
        return part_without_rule(lists.value_type, structs=structs)
    if structs and pa.types.is_struct(plain):
        struct = cast("pa.StructType", plain)
        for index in range(struct.num_fields):
            part = part_without_rule(struct.field(index).type, structs=structs)
            if part is not None:
                return part
        return None
    return None if _is_prepared_scalar(plain) else plain


def values_of(array: "pa.Array[Any]") -> "pa.Array[Any]":
    """Return an array as its values are read: a logical type's storage.

    The array twin of ``value_type``.

    Args:
        array: A column, or the values of one.

    Returns:
        The array of plain values.
    """
    while isinstance(array, pa.ExtensionArray):
        array = array.storage
    return array


def _decimal256_limbs(array: "pa.Array[Any]") -> "pa.Array[Any]":
    """Return a decimal256 array as a struct of four 64-bit limbs.

    Polars has no 256-bit decimal. The limbs of the two's-complement value, most
    significant first (that one signed, the others unsigned), compare and order as
    the decimals of one column do: the scale is part of the column's type.

    Args:
        array: A ``decimal256`` array.

    Returns:
        A struct array with the limbs, null where ``array`` is null.
    """
    count = len(array)
    nulls = array.is_null().to_numpy(zero_copy_only=False)
    buffer = array.buffers()[1]
    # A pyarrow Buffer has the buffer protocol; its stub does not declare it.
    words = (
        np.frombuffer(buffer, dtype="<u8")  # pyright: ignore[reportCallIssue, reportArgumentType]
        if buffer is not None
        else np.zeros(0, dtype="<u8")
    )
    limbs = words[array.offset * 4 : (array.offset + count) * 4].reshape(count, 4)
    return pa.StructArray.from_arrays(
        [
            pa.array(limbs[:, 3].view("<i8"), pa.int64()),
            pa.array(limbs[:, 2], pa.uint64()),
            pa.array(limbs[:, 1], pa.uint64()),
            pa.array(limbs[:, 0], pa.uint64()),
        ],
        names=["l3", "l2", "l1", "l0"],
        mask=pa.array(nulls, pa.bool_()),
    )


def polars_ready(array: "pa.Array[Any]") -> "pa.Array[Any]":
    """Return a key column in a form Polars reads without changing it.

    Two prepared values are equal (and order) as the values are. Only the frame
    Polars matches, groups or orders sees it; the output takes every value from the
    input. Every type ``part_without_rule`` accepts is prepared (see the module).

    Args:
        array: A key column, or the values of one.

    Returns:
        The prepared array.

    Raises:
        TypeError: For a type with no rule. A caller checks ``part_without_rule``
            first and refuses the column, naming it.
    """
    array = values_of(array)
    arrow_type = array.type
    types = pa.types
    if types.is_large_list(arrow_type) or types.is_list(arrow_type):
        lists = cast("pa.LargeListArray[Any] | pa.ListArray[Any]", array)
        # The offsets of a sliced array start where the slice does; Arrow refuses
        # such offsets with a null mask, so they are rebased to the values used.
        offsets = lists.offsets.cast(pa.int64()).to_numpy()
        start = int(offsets[0])
        values = lists.values.slice(start, int(offsets[-1]) - start)
        return pa.LargeListArray.from_arrays(
            cast("pa.Int64Array", pa.array(offsets - start, pa.int64())),
            polars_ready(values),
            mask=array.is_null(),
        )
    if types.is_fixed_size_list(arrow_type):
        fixed = cast("pa.FixedSizeListArray[Any]", array)
        size = cast(int, arrow_type.list_size)
        values = fixed.values.slice(fixed.offset * size, len(fixed) * size)
        offsets = pa.array(np.arange(len(fixed) + 1, dtype=np.int64) * size, pa.int64())
        return pa.LargeListArray.from_arrays(
            cast("pa.Int64Array", offsets),
            polars_ready(values),
            mask=array.is_null(),
        )
    if types.is_struct(arrow_type):
        struct = cast("pa.StructArray", array)
        struct_type = cast("pa.StructType", arrow_type)
        return pa.StructArray.from_arrays(
            [polars_ready(struct.field(i)) for i in range(struct_type.num_fields)],
            names=[struct_type.field(i).name for i in range(struct_type.num_fields)],
            mask=array.is_null(),
        )
    if types.is_decimal256(arrow_type):
        return _decimal256_limbs(array)
    if types.is_float16(arrow_type):
        return array.cast(pa.float32())
    if (
        types.is_date(arrow_type)
        or types.is_time(arrow_type)
        or types.is_timestamp(arrow_type)
        or types.is_duration(arrow_type)
    ):
        # Not ``is_temporal``, which also takes an interval: that has no rule.
        width = cast(int, arrow_type.bit_width)
        return array.view(pa.int32() if width == 32 else pa.int64())
    if types.is_string_view(arrow_type):
        return array.cast(pa.large_string())
    if types.is_binary_view(arrow_type):
        return array.cast(pa.large_binary())
    if _is_prepared_scalar(arrow_type):
        return array
    raise TypeError(f"no rule prepares {arrow_type} for Polars")


def polars_key_frame(
    table: "pa.Table", keys: Sequence[str], row: str | None = None
) -> "pl.DataFrame":
    """Return a Polars frame of a table's key columns, prepared, and its row numbers.

    The frame an operator matches rows on: nothing but the prepared keys (in
    ``keys`` order) and, when ``row`` is given, each row's number in ``table``
    (``int64``), by which the operator then takes its output from ``table``.

    Args:
        table: The input table.
        keys: The key columns, each checked with ``part_without_rule`` first.
        row: The name of the row-number column, one no key has; ``None`` for none.

    Returns:
        The frame.
    """
    names = list(keys)
    arrays = [polars_ready(table.column(name).combine_chunks()) for name in names]
    if row is not None:
        names.append(row)
        arrays.append(pa.array(np.arange(table.num_rows, dtype=np.int64), pa.int64()))
    return cast(
        "pl.DataFrame", pl.from_arrow(pa.Table.from_arrays(arrays, names=names))
    )
