"""A source refuses a stored dict that repeats a key (ITL-907).

A ``dict[K, V]`` column is stored as a list of key/value structs, and to Orcapod every
such list is a dict: which Arrow type that is has one definition, the type converter's
``dict_entry_fields``. A table built outside Orcapod can hold a cell with one key twice;
read as a Python ``dict`` it would lose an entry. The source builder calls this module for
each column whose type holds a dict type, at any depth (a dict in a list, as a dict value,
in a struct or in a user class's storage); a table with no such column is not scanned.

Two keys are repeated exactly when the Python ``dict`` the cell is read as would keep one
of them: ``0.0`` and ``-0.0`` are one key, two nulls are one key, and a NaN (anywhere in a
key) equals nothing, so two NaN keys are two keys. Which rows hold a repeat is decided on
the Arrow data, by Polars over the prepared keys (``arrow_keys.polars_ready``, as the
joins and ``GroupBy`` compare keys); a key type with no preparation rule is compared in
Python, cell by cell. The conversion to a ``dict`` raises on a repeated key too
(``UniversalTypeConverter``), for the routes that pass no source.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

from orcapod.core.operators import arrow_keys
from orcapod.errors import RepeatedDictKeyError

if TYPE_CHECKING:
    from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol


def holds_dict_type(arrow_type: pa.DataType, converter: TypeConverterProtocol) -> bool:
    """Return whether a column's type is, or holds at any depth, a dict type.

    Decided from the type alone. A logical type is looked into only when the converter
    reads its storage as plain values (``reads_storage_as_values``: a user class, a
    native container).

    Args:
        arrow_type: The Arrow type of a column, or of a part of one.
        converter: The type converter, which defines the dict type.

    Returns:
        True when a value of the type can hold a dict.
    """
    if isinstance(arrow_type, pa.BaseExtensionType):
        if not converter.reads_storage_as_values(arrow_type):
            return False
        return holds_dict_type(arrow_type.storage_type, converter)
    if converter.dict_entry_fields(arrow_type) is not None:
        return True
    if arrow_keys.is_list_kind(arrow_type):
        lists = cast("pa.LargeListType[Any]", arrow_type)
        return holds_dict_type(lists.value_type, converter)
    if pa.types.is_struct(arrow_type):
        struct = cast("pa.StructType", arrow_type)
        return any(
            holds_dict_type(struct.field(index).type, converter)
            for index in range(struct.num_fields)
        )
    return False


def refuse_repeated_dict_keys(
    column: pa.ChunkedArray[Any], converter: TypeConverterProtocol
) -> None:
    """Refuse a column in which one cell's dict holds a key more than once.

    Called only for a column whose type ``holds_dict_type``. The caller knows the
    column's name and adds it (``RepeatedDictKeyError.in_column``).

    Args:
        column: The column, as the source was given it.
        converter: The type converter, which defines the dict type.

    Raises:
        RepeatedDictKeyError: If a dict, at any depth, repeats a key. It holds the key.
    """
    for chunk in column.chunks:
        _scan(chunk, converter)


def _scan(array: pa.Array[Any], converter: TypeConverterProtocol) -> None:
    """Check every dict in ``array``, at every depth, for a repeated key."""
    if len(array) == 0 or not holds_dict_type(array.type, converter):
        return
    if isinstance(array, pa.ExtensionArray):
        _scan(array.storage, converter)
        return
    arrow_type = array.type
    if converter.dict_entry_fields(arrow_type) is not None:
        _check_dicts(cast("pa.LargeListArray[Any]", array), converter)
        return
    if arrow_keys.is_list_kind(arrow_type):
        # ``flatten`` leaves out what a null list or a slice does not hold.
        _scan(cast("pa.LargeListArray[Any]", array).flatten(), converter)
        return
    # A struct: each child with the struct's own nulls, so a null struct holds nothing.
    for child in cast("pa.StructArray", array).flatten():
        _scan(child, converter)


def _check_dicts(
    dicts: pa.LargeListArray[Any], converter: TypeConverterProtocol
) -> None:
    """Check one array of dicts: each cell's keys, then the dicts its entries hold."""
    fields = converter.dict_entry_fields(dicts.type)
    assert fields is not None
    key_field, value_field = fields
    entries = cast("pa.StructArray", dicts.flatten())
    cells = _parent_indices(dicts)
    # A null entry is no key/value pair (the read refuses it on its own terms).
    if entries.null_count:
        valid = entries.is_valid()
        entries = cast("pa.StructArray", entries.filter(valid))
        cells = cells.filter(valid)
    flat = dict(zip((f.name for f in entries.type), entries.flatten(), strict=True))
    keys = flat[key_field.name]
    if len(keys) > 1:
        found = _first_repeat(cells, keys)
        if found is not None:
            to_python = converter.get_arrow_to_python_converter(key_field.type)
            stored = keys[found].as_py()
            raise RepeatedDictKeyError(None if stored is None else to_python(stored))
    _scan(flat[value_field.name], converter)


def _parent_indices(lists: pa.Array[Any]) -> pa.Array[Any]:
    """Return, for each element ``lists.flatten()`` gives, the row of ``lists`` it is in."""
    if pa.types.is_fixed_size_list(lists.type):
        size = cast(int, lists.type.list_size)
        rows = np.repeat(np.arange(len(lists), dtype=np.int64), size)
        if lists.null_count:
            rows = rows[
                np.repeat(lists.is_valid().to_numpy(zero_copy_only=False), size)
            ]
        return pa.array(rows, pa.int64())
    counted = pc.list_value_length(lists)
    lengths = counted.fill_null(0).to_numpy(zero_copy_only=False)
    return pa.array(
        np.repeat(np.arange(len(lists), dtype=np.int64), lengths), pa.int64()
    )


def _first_repeat(cells: pa.Array[Any], keys: pa.Array[Any]) -> int | None:
    """Return the position of a key its cell holds more than once, or ``None``.

    Args:
        cells: For each key, the cell (row of the dict array) it is in.
        keys: The keys of every cell, in cell order.

    Returns:
        The position in ``keys`` of the first key that repeats an earlier key of its
        cell, or ``None`` when no cell repeats a key.
    """
    if arrow_keys.part_without_rule(keys.type, structs=True) is not None:
        return _first_repeat_in_python(cells, keys)
    # A NaN equals nothing in Python, so a key holding one is never a repeat; Polars
    # would match two NaN. Such keys are left out before Polars compares.
    positions = np.arange(len(keys), dtype=np.int64)
    nan = _holds_nan(keys)
    if nan is not None and pc.any(nan).as_py():
        kept = pc.invert(nan)
        positions = positions[kept.to_numpy(zero_copy_only=False)]
        cells, keys = cells.filter(kept), keys.filter(kept)
        if len(keys) < 2:
            return None
    frame = cast(
        "pl.DataFrame",
        pl.from_arrow(
            pa.Table.from_arrays(
                [cells, arrow_keys.polars_ready(keys)], names=["cell", "key"]
            )
        ),
    )
    # The first key that an earlier key of its cell equals: the one the read raises on.
    first = frame.select(pl.struct("cell", "key").is_first_distinct().alias("first"))[
        "first"
    ].to_numpy()
    hits = np.flatnonzero(~first)
    return int(positions[hits[0]]) if len(hits) else None


def _first_repeat_in_python(cells: pa.Array[Any], keys: pa.Array[Any]) -> int | None:
    """``_first_repeat`` for a key type with no preparation rule: Python's own equality.

    A key Python cannot hash is in no ``dict`` at all (the read raises on it): it is
    not compared here.
    """
    seen: dict[Any, set[Any]] = {}
    for position, (cell, key) in enumerate(
        zip(cells.to_pylist(), keys.to_pylist(), strict=True)
    ):
        held = seen.setdefault(cell, set())
        try:
            if key in held:
                return position
            held.add(key)
        except TypeError:
            continue
    return None


def _holds_nan(array: pa.Array[Any]) -> pa.BooleanArray | None:
    """Return which values hold a NaN at any depth, or ``None`` when the type has no float."""
    array = arrow_keys.values_of(array)
    arrow_type = array.type
    if pa.types.is_floating(arrow_type):
        if pa.types.is_float16(arrow_type):
            array = array.cast(pa.float32())
        return cast("pa.BooleanArray", pc.is_nan(array).fill_null(False))
    if arrow_keys.is_list_kind(arrow_type):
        inner = _holds_nan(cast("pa.LargeListArray[Any]", array).flatten())
        if inner is None:
            return None
        rows = _parent_indices(array).filter(inner)
        return cast(
            "pa.BooleanArray",
            pc.is_in(pa.array(np.arange(len(array), dtype=np.int64)), value_set=rows),
        )
    if pa.types.is_struct(arrow_type):
        found: pa.BooleanArray | None = None
        for child in cast("pa.StructArray", array).flatten():
            inner = _holds_nan(child)
            if inner is not None:
                found = (
                    inner
                    if found is None
                    else cast("pa.BooleanArray", pc.or_(found, inner))
                )
        return found
    return None
