"""Canonical order for the unordered containers ``dict`` and ``set``.

A ``dict[K, V]`` value is stored as ``large_list<struct<key, value>>`` and a plain
``set[T]`` as ``large_list<T>``. A dict and a set are order independent, so the order
they are stored in must not come from insertion or iteration order (ITL-905). Orcapod
puts them in canonical order where a Python value becomes Arrow: in the converter, and
in a datagram built from Python values. An Arrow table is always hashed exactly as it
is stored; a ``list<struct<key, value>>`` that arrives already in Arrow form is an
ordered list at the Arrow level, and its order is part of its hash.

The order is defined on storage values (what the converter produces):

* ``None`` sorts first.
* ``bool``, ``int``, ``float``, ``Decimal``: numeric. A NaN of any numeric type has
  no order and is rejected. ``-0.0`` and ``0.0`` compare equal, so within one dict or
  set they collide (and raise, see below); a dict keyed by ``-0.0`` and one keyed by
  ``0.0`` store different bytes and hash differently, like any float value.
* ``str``: Unicode code point (never locale). ``bytes``: unsigned bytewise.
* ``date``, ``datetime`` (by instant), ``time``, ``timedelta``: natural order.
* Structs in sets: lexicographically by the declared storage fields, in field order.
* Lists in sets: lexicographically by their items, with a proper prefix sorting first.

Keys or elements that cannot be ordered against each other, and two keys or elements
with the same storage value, raise ``CanonicalOrderError``.
"""

from __future__ import annotations

import math
import numbers
import types
import typing
from collections.abc import Callable
from contextvars import ContextVar
from decimal import Decimal
from itertools import islice, pairwise
from typing import Any, TypeVar, cast, get_args, get_origin

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

from orcapod.errors import CanonicalOrderError, LogicalSetCanonicalOrderError

_T = TypeVar("_T")

# A sort key: ``(0,)`` for None, ``(1, value)`` otherwise.
_Rank = tuple[Any, ...]

# Field converters also serve standalone primitive containers. Only an enclosing
# user-class set requires canonicalizing those fields against their Arrow storage.
_in_logical_set: ContextVar[bool] = ContextVar("_in_logical_set", default=False)


def _key_rank(key: object) -> _Rank:
    """Return the sort key of one storage key or set element.

    Raises:
        CanonicalOrderError: If the key is a NaN of any numeric type (Python ``float``,
            NumPy floating scalars, ``Decimal``).
    """
    if key is None:
        return (0,)
    if _is_nan(key):
        raise CanonicalOrderError("a NaN key or set element has no canonical order")
    return (1, key)


def _is_nan(value: object) -> bool:
    """Return whether ``value`` is a NaN of any numeric type.

    Integers are never NaN and are not converted (a large ``int`` does not fit a float).
    """
    if isinstance(value, Decimal):
        return value.is_nan()
    if isinstance(value, numbers.Real) and not isinstance(value, numbers.Integral):
        return math.isnan(value)
    return False


def _sorted_by_rank(items: list[tuple[_Rank, _T]]) -> list[_T]:
    """Sort ``(rank, payload)`` pairs by rank and return the payloads.

    Raises:
        CanonicalOrderError: If two ranks cannot be compared or are equal.
    """
    try:
        items = sorted(items, key=lambda item: item[0])
    except TypeError as exc:
        raise CanonicalOrderError(f"values cannot be ordered: {exc}") from exc
    for (previous, _), (current, payload) in pairwise(items):
        if previous == current:
            raise CanonicalOrderError(f"duplicate key or set element: {payload!r}")
    return [payload for _, payload in items]


def sort_dict_entries(entries: list[dict[str, _T]]) -> list[dict[str, _T]]:
    """Return converted entries ``{"key": ..., "value": ...}`` in canonical key order.

    Args:
        entries: The entries of one dict value, as the converter produces them.

    Returns:
        A new list holding the same entries, sorted by key.

    Raises:
        CanonicalOrderError: For a NaN key, keys that cannot be ordered against each
            other, or two keys with the same storage value.
    """
    return _sorted_by_rank([(_key_rank(entry["key"]), entry) for entry in entries])


def sort_set_elements(elements: list[_T]) -> list[_T]:
    """Return the storage elements of one primitive set in canonical order.

    Args:
        elements: The converted elements of a set.

    Returns:
        A new list holding the same elements, sorted.

    Raises:
        CanonicalOrderError: For a NaN element, elements that cannot be ordered against
            each other, or two elements with the same storage value.
    """
    return _sorted_by_rank([(_key_rank(element), element) for element in elements])


_LIST_ARRAY_TYPES = (
    pa.ListArray,
    pa.LargeListArray,
    pa.FixedSizeListArray,
    pa.ListViewArray,
    pa.LargeListViewArray,
)


def _ordinal_column(keys: list[Any]) -> pa.Array[Any]:
    """Represent each distinct non-null Python comparison key by its sorted rank."""
    ranks = {key: i for i, key in enumerate(sorted(set(keys) - {None}))}
    return pa.array([ranks.get(key) for key in keys], type=pa.int64())


def _list_sort_column(
    array: pa.ListArray[Any]
    | pa.LargeListArray[Any]
    | pa.FixedSizeListArray[Any]
    | pa.ListViewArray[Any]
    | pa.LargeListViewArray[Any],
) -> pa.Array[Any]:
    """Use one lexicographic key per stored list, comparing only its own children."""
    children = _sort_columns(array.flatten())
    lengths = pc.list_value_length(array)
    if len(children) == 1 and (
        pa.types.is_integer(children[0].type)
        or pa.types.is_floating(children[0].type)
        or pa.types.is_boolean(children[0].type)
    ):
        # Dense rank compares list items lexicographically, with null items first
        # and equal lists sharing a rank. List validity stays separate from length.
        # Rebuild offsets over the flattened children, which omit null parent lists.
        offsets = pc.cumulative_sum(
            pa.concat_arrays(
                [pa.array([0], type=pa.int64()), lengths.cast(pa.int64()).fill_null(0)]
            )
        )
        lists = pa.LargeListArray.from_arrays(
            cast(pa.Int64Array, offsets), children[0], mask=pc.is_null(array)
        )
        return pl.Series(lists).rank("dense").to_arrow()
    # All child columns hold built-in scalars. Tag nulls to put them first, including
    # inside nested lists. The tuple comparison gives a proper prefix precedence.
    child_keys = zip(
        *(
            [(value is not None, value) for value in column.to_pylist()]
            for column in children
        ),
        strict=True,
    )
    keys = [
        None if length is None else tuple(islice(child_keys, length))
        for length in lengths.to_pylist()
    ]
    return _ordinal_column(keys)


def _sort_columns(array: pa.Array[Any]) -> list[pa.Array[Any]]:
    """Flatten struct comparison keys in field order, preserving parent nulls."""
    if array.null_count == len(array):
        # Arrow has no equality kernel for the null type. A constant key gives
        # the same ties and also covers unsupported storage whose values are null.
        return [pc.is_valid(array)]
    if isinstance(array, pa.StructArray):
        # flatten() masks the children where a struct is null. The validity key
        # distinguishes a null struct from a present struct with all null fields.
        columns: list[pa.Array[Any]] = (
            [pc.is_valid(array)]
            if array.null_count or not array.type.num_fields
            else []
        )
        for child in array.flatten():
            columns.extend(_sort_columns(child))
        return columns
    if isinstance(array, _LIST_ARRAY_TYPES):
        return [_list_sort_column(array)]
    if isinstance(array, pa.ExtensionArray):
        return _sort_columns(array.storage)
    if isinstance(array, pa.DictionaryArray):
        return _sort_columns(array.dictionary_decode())

    kind = array.type
    if pa.types.is_float16(kind):
        array = array.cast(pa.float32())
    if pa.types.is_floating(kind):
        if pc.any(pc.is_nan(array)).as_py():
            raise LogicalSetCanonicalOrderError(
                "a NaN key or set element has no canonical order"
            )
        return [array]
    if pa.types.is_string_view(kind):
        return [array.cast(pa.large_string())]
    if pa.types.is_binary_view(kind):
        return [array.cast(pa.large_binary())]
    if isinstance(kind, (pa.Decimal32Type, pa.Decimal64Type)):
        return [array.cast(pa.decimal128(kind.precision, kind.scale))]
    if pa.types.is_interval(kind):
        # Arrow has no sort/equality kernels for intervals. Its Python scalars
        # expose their stored months, days and nanoseconds as a tuple.
        return [_ordinal_column(array.to_pylist())]
    if pa.types.is_nested(kind):
        raise LogicalSetCanonicalOrderError(f"storage type {kind} cannot be ordered")

    # Use physical units for temporal values, including inside list keys, where
    # Python date/time objects could otherwise drop sub-day or sub-microsecond bits.
    if pa.types.is_date32(kind) or pa.types.is_time32(kind):
        return [array.view(pa.int32())]
    if pa.types.is_temporal(kind):
        return [array.view(pa.int64())]
    return [array]


def _adjacent_equal(array: pa.Array[Any]) -> pa.BooleanArray:
    """Compare neighboring sorted keys, with null equal to null."""
    left, right = array.slice(0, len(array) - 1), array.slice(1)
    equal = pc.equal(left, right)
    if array.null_count:
        both_null = pc.and_(pc.is_null(left), pc.is_null(right))
        equal = pc.or_kleene(equal, both_null)
        equal = pc.fill_null(equal, pa.scalar(False))
    return cast(pa.BooleanArray, equal)


def ordered_set_storage(
    elements: list[Any], storage_type: pa.DataType
) -> pa.Array[Any]:
    """Encode a logical set's elements once and return them in canonical order.

    Args:
        elements: The converted elements of one logical set.
        storage_type: The declared Arrow element storage type.

    Returns:
        The exact Arrow storage values, in canonical order.

    Raises:
        LogicalSetCanonicalOrderError: For an unorderable value (including NaN),
            or two elements with equal storage keys.
    """
    try:
        array = pa.array(elements, type=storage_type)
        columns = _sort_columns(array)
        return _sort_set_array(array, columns) if len(array) > 1 else array
    except (TypeError, pa.ArrowException) as exc:
        raise LogicalSetCanonicalOrderError("values cannot be ordered") from exc


def _ordered_dict_storage(
    entries: list[dict[str, Any]], entry_type: pa.DataType
) -> pa.Array[Any]:
    """Order typed dict entries by their keys and reject equal stored keys."""
    try:
        array = cast(pa.StructArray, pa.array(entries, type=entry_type))
        columns = _sort_columns(array.field("key"))
        return _sort_set_array(array, columns) if len(array) > 1 else array
    except (TypeError, pa.ArrowException) as exc:
        raise LogicalSetCanonicalOrderError("values cannot be ordered") from exc


def _ordered_set_column(
    rows: list[list[Any] | None], storage_type: pa.LargeListType[Any]
) -> pa.LargeListArray[Any]:
    """Encode and sort the elements of a column, keeping each set in its row."""
    try:
        storage = cast("pa.LargeListArray[Any]", pa.array(rows, type=storage_type))
        elements = storage.values
        if len(elements) == 0:
            return storage
        columns = _sort_columns(elements)
        if pc.max(pc.list_value_length(storage)).as_py() <= 1:
            return storage
        sorted_elements = _sort_set_array(
            elements, [pc.list_parent_indices(storage), *columns]
        )
        return pa.LargeListArray.from_arrays(
            storage.offsets,
            sorted_elements,
            type=storage_type,
            mask=pc.is_null(storage),
        )
    except (TypeError, pa.ArrowException) as exc:
        raise LogicalSetCanonicalOrderError("values cannot be ordered") from exc


def _sort_set_array(
    array: pa.Array[Any], columns: list[pa.Array[Any]]
) -> pa.Array[Any]:
    """Sort stored values by the comparison columns and refuse equal neighbors."""
    names = [str(i) for i in range(len(columns))]
    indices = pc.sort_indices(
        pa.Table.from_arrays(columns, names=names),
        sort_keys=[(name, "ascending") for name in names],
        null_placement="at_start",
    )
    # Intersect adjacent equality masks until no possible duplicate remains.
    duplicates = None
    for column in columns:
        equal = _adjacent_equal(column.take(indices))
        duplicates = equal if duplicates is None else pc.and_(duplicates, equal)
        if not pc.any(duplicates).as_py():
            try:
                return array.take(indices)
            except pa.ArrowNotImplementedError:
                # View types lack a take kernel, including inside structs.
                return pa.concat_arrays(
                    [array.slice(i, 1) for i in cast(list[int], indices.to_pylist())]
                )
    raise LogicalSetCanonicalOrderError("duplicate key or set element")


def canonical_python_value(
    value: _T,
    python_type: object,
    key_converter: Callable[[Any], Callable[[Any], object]],
) -> _T:
    """Return ``value`` with every dict in it in canonical key order.

    Walks ``python_type`` through ``dict``, ``list`` and ``Optional``. A dict whose order
    changes, or whose values change, is returned as a new dict; everything else is
    returned as given. The input is never mutated. Values of other types (including
    dataclass and pydantic objects) are returned unchanged.

    Args:
        value: A Python value of type ``python_type``.
        python_type: The value's declared Python type.
        key_converter: Returns the Python-to-storage converter for a key type, so keys
            are ordered by their storage value, as the converter stores them.

    Returns:
        ``value`` itself when nothing changes, otherwise a canonically ordered copy.

    Raises:
        CanonicalOrderError: For a key that has no canonical order (NaN), keys that
            cannot be ordered, or two keys that convert to the same storage key.
    """
    if value is None:
        return value
    origin = cast(object, get_origin(python_type))
    args = cast(tuple[object, ...], get_args(python_type))
    if origin is typing.Union or origin is types.UnionType:  # pyright: ignore[reportDeprecated]  -- detects Optional[T]
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) != 1:
            return value
        return canonical_python_value(value, non_none[0], key_converter)
    if origin is dict and len(args) == 2 and isinstance(value, dict):
        original = cast(dict[object, object], value)
        to_storage = key_converter(args[0])
        items = [
            (
                _key_rank(to_storage(k)),
                (k, canonical_python_value(v, args[1], key_converter)),
            )
            for k, v in original.items()
        ]
        ordered = _sorted_by_rank(items)
        unchanged = all(
            new_k is old_k and new_v is old_v
            for (new_k, new_v), (old_k, old_v) in zip(ordered, original.items())
        )
        return value if unchanged else cast(_T, dict(ordered))
    if origin is list and len(args) == 1 and isinstance(value, list):
        original_items = cast(list[object], value)
        new_items = [
            canonical_python_value(v, args[0], key_converter) for v in original_items
        ]
        if all(new is old for new, old in zip(new_items, original_items)):
            return value
        return cast(_T, new_items)
    return value
