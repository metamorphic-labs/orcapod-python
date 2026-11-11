"""Canonical entry order for dict values.

A ``dict[K, V]`` value is stored as ``large_list<struct<key, value>>``. A dict is order
independent, so two equal dicts must store and hash identically whatever order their
entries were inserted in (ITL-905). This module holds the one definition of that order.
It is applied when a Python dict is converted to Arrow, when a datagram is built from
Python values, and by the Arrow hasher to key/value lists that arrive already stored.

The order is defined on storage key values (what the converter produces and what
``to_pylist`` reads back), so the converter and the hasher cannot disagree:

* ``None`` sorts first.
* ``bool``, ``int``, ``float``, ``Decimal``: numeric. A NaN key has no order and is
  rejected. ``-0.0`` and ``0.0`` are one key.
* ``str``: Unicode code point (never locale). ``bytes``: unsigned bytewise.
* ``date``, ``datetime`` (by instant), ``time``, ``timedelta``: natural order.
* struct keys (a ``dict`` of field values): field by field, in field order.

Keys that cannot be ordered against each other, duplicate keys and null entries raise
``InvalidDictKeysError``.
"""

from __future__ import annotations

import math
import types
import typing
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, get_args, get_origin

from orcapod.errors import InvalidDictKeysError
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


def is_dict_arrow_type(arrow_type: pa.DataType) -> bool:
    """Return whether ``arrow_type`` is Orcapod's encoding of a ``dict``.

    A dict is a list (regular, large or fixed-size) whose element is a struct with
    exactly two fields named ``key`` and ``value``. Any other list of structs is an
    ordered list, whose order is part of its identity.

    Args:
        arrow_type: The Arrow type to test.

    Returns:
        True for a dict-encoded list type.
    """
    if not (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return False
    element = arrow_type.value_type
    return (
        pa.types.is_struct(element)
        and element.num_fields == 2
        and {element.field(0).name, element.field(1).name} == {"key", "value"}
    )


def _key_rank(key: Any) -> tuple[Any, ...]:
    """Return the sort key of one storage key value.

    Raises:
        InvalidDictKeysError: If the key is a NaN float.
    """
    if key is None:
        return (0,)
    if isinstance(key, float) and math.isnan(key):
        raise InvalidDictKeysError("a NaN dict key has no canonical order")
    if isinstance(key, Mapping):
        return (1, tuple(_key_rank(v) for v in key.values()))
    return (1, key)


def _sorted_by_rank(items: list[tuple[tuple[Any, ...], Any]]) -> list[Any]:
    """Sort ``(rank, payload)`` pairs by rank and return the payloads.

    Raises:
        InvalidDictKeysError: If two ranks cannot be compared or are equal.
    """
    try:
        items = sorted(items, key=lambda item: item[0])
    except TypeError as exc:
        raise InvalidDictKeysError(f"dict keys cannot be ordered: {exc}") from exc
    for (previous, _), (current, payload) in zip(items, items[1:]):
        if previous == current:
            raise InvalidDictKeysError(f"duplicate key in dict value: {payload!r}")
    return [payload for _, payload in items]


def sort_dict_entries(entries: list[Any]) -> list[Any]:
    """Return storage entries ``{"key": ..., "value": ...}`` in canonical key order.

    Args:
        entries: The entries of one dict value, as the converter produces them or as
            ``to_pylist`` reads them back.

    Returns:
        A new list holding the same entries, sorted by key.

    Raises:
        InvalidDictKeysError: For a null entry, a NaN key, keys that cannot be ordered
            against each other, or a duplicate key.
    """
    ranked = []
    for entry in entries:
        if entry is None:
            raise InvalidDictKeysError("a dict value holds a null entry")
        ranked.append((_key_rank(entry["key"]), entry))
    return _sorted_by_rank(ranked)


def canonical_python_value(
    value: Any,
    python_type: Any,
    key_converter: Callable[[Any], Callable[[Any], Any]],
) -> Any:
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
        InvalidDictKeysError: For a key that has no canonical order (NaN), keys that
            cannot be ordered, or two keys that convert to the same storage key.
    """
    if value is None:
        return None
    origin = get_origin(python_type)
    args = get_args(python_type)
    if origin is typing.Union or origin is types.UnionType:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) != 1:
            return value
        return canonical_python_value(value, non_none[0], key_converter)
    if origin is dict and len(args) == 2 and isinstance(value, dict):
        to_storage = key_converter(args[0])
        items = [
            (
                _key_rank(to_storage(k)),
                (k, canonical_python_value(v, args[1], key_converter)),
            )
            for k, v in value.items()
        ]
        ordered = _sorted_by_rank(items)
        unchanged = all(
            new_k is old_k and new_v is old_v
            for (new_k, new_v), (old_k, old_v) in zip(ordered, value.items())
        )
        return value if unchanged else dict(ordered)
    if origin is list and len(args) == 1 and isinstance(value, list):
        new_items = [canonical_python_value(v, args[0], key_converter) for v in value]
        if all(new is old for new, old in zip(new_items, value)):
            return value
        return new_items
    return value
