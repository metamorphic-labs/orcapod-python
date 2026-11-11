"""Canonical order for the unordered containers ``dict`` and ``set``.

A ``dict[K, V]`` value is stored as ``large_list<struct<key, value>>`` and a plain
``set[T]`` as ``large_list<T>``. Both are order independent, so two equal values must
store and hash identically whatever order their entries were inserted or iterated in
(ITL-905). This module holds the one definition of that order. Dict entries are sorted
by key when a Python dict is converted to Arrow, when a datagram is built from Python
values, and by the Arrow hasher for key/value lists that arrive already stored. Set
elements are sorted when a Python set is converted to Arrow (a stored set is a plain
list, so it cannot be recognised later).

The order is defined on storage values (what the converter produces and what
``to_pylist`` reads back), so the converter and the hasher cannot disagree:

* ``None`` sorts first.
* ``bool``, ``int``, ``float``, ``Decimal``: numeric. A NaN key has no order and is
  rejected. ``-0.0`` and ``0.0`` are one key.
* ``str``: Unicode code point (never locale). ``bytes``: unsigned bytewise.
* ``date``, ``datetime`` (by instant), ``time``, ``timedelta``: natural order.
* struct keys (a ``dict`` of field values): field by field, in field order.

Keys or elements that cannot be ordered against each other, duplicate dict keys and
null dict entries raise ``CanonicalOrderError``.
"""

from __future__ import annotations

import functools
import math
import types
import typing
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, get_args, get_origin

from orcapod.errors import CanonicalOrderError
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
    if not _is_list_type(arrow_type):
        return False
    element = arrow_type.value_type
    return (
        pa.types.is_struct(element)
        and element.num_fields == 2
        and {element.field(0).name, element.field(1).name} == {"key", "value"}
    )


def _is_list_type(arrow_type: pa.DataType) -> bool:
    return (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    )


def contains_dict_arrow_type(arrow_type: pa.DataType) -> bool:
    """Return whether a dict-encoded list occurs anywhere in ``arrow_type``.

    Looks through list elements, struct fields and extension storage.

    Args:
        arrow_type: The Arrow type to search.

    Returns:
        True if ``arrow_type`` is or contains a dict-encoded list.
    """
    try:
        return _contains_dict_cached(arrow_type)
    except TypeError:  # an extension type that does not implement __hash__
        return _contains_dict(arrow_type)


@functools.lru_cache(maxsize=1024)
def _contains_dict_cached(arrow_type: pa.DataType) -> bool:
    return _contains_dict(arrow_type)


def _contains_dict(arrow_type: pa.DataType) -> bool:
    if isinstance(arrow_type, pa.ExtensionType):
        return contains_dict_arrow_type(arrow_type.storage_type)
    if _is_list_type(arrow_type):
        return is_dict_arrow_type(arrow_type) or contains_dict_arrow_type(
            arrow_type.value_type
        )
    if pa.types.is_struct(arrow_type):
        return any(
            contains_dict_arrow_type(arrow_type.field(i).type)
            for i in range(arrow_type.num_fields)
        )
    return False


def canonical_storage_value(arrow_type: pa.DataType, value: Any) -> Any:
    """Return a storage value with the entries of every dict in canonical key order.

    ``value`` is one value of an Arrow column of type ``arrow_type`` as ``to_pylist``
    reads it. Dicts at any depth (inside lists, struct fields and extension storage)
    are sorted innermost first; every other list keeps its order. The input is not
    mutated.

    Args:
        arrow_type: The Arrow type of ``value``.
        value: The storage value.

    Returns:
        ``value`` itself when ``arrow_type`` holds no dict, otherwise a canonical copy.

    Raises:
        CanonicalOrderError: For a dict with a null entry, a NaN key, keys that cannot
            be ordered against each other, or a duplicate key.
    """
    if value is None or not contains_dict_arrow_type(arrow_type):
        return value
    if isinstance(arrow_type, pa.ExtensionType):
        return canonical_storage_value(arrow_type.storage_type, value)
    if _is_list_type(arrow_type):
        element_type = arrow_type.value_type
        elements = [canonical_storage_value(element_type, v) for v in value]
        if is_dict_arrow_type(arrow_type):
            return sort_dict_entries(elements)
        return elements
    # A struct holding a dict somewhere in its fields.
    return {
        field.name: canonical_storage_value(field.type, value.get(field.name))
        for field in arrow_type
    }


def _key_rank(key: Any) -> tuple[Any, ...]:
    """Return the sort key of one storage key value.

    Raises:
        CanonicalOrderError: If the key is a NaN float.
    """
    if key is None:
        return (0,)
    if isinstance(key, float) and math.isnan(key):
        raise CanonicalOrderError("a NaN key or set element has no canonical order")
    if isinstance(key, Mapping):
        return (1, tuple(_key_rank(v) for v in key.values()))
    return (1, key)


def _sorted_by_rank(items: list[tuple[tuple[Any, ...], Any]]) -> list[Any]:
    """Sort ``(rank, payload)`` pairs by rank and return the payloads.

    Raises:
        CanonicalOrderError: If two ranks cannot be compared or are equal.
    """
    try:
        items = sorted(items, key=lambda item: item[0])
    except TypeError as exc:
        raise CanonicalOrderError(f"values cannot be ordered: {exc}") from exc
    for (previous, _), (current, payload) in zip(items, items[1:]):
        if previous == current:
            raise CanonicalOrderError(f"duplicate key or set element: {payload!r}")
    return [payload for _, payload in items]


def sort_dict_entries(entries: list[Any]) -> list[Any]:
    """Return storage entries ``{"key": ..., "value": ...}`` in canonical key order.

    Args:
        entries: The entries of one dict value, as the converter produces them or as
            ``to_pylist`` reads them back.

    Returns:
        A new list holding the same entries, sorted by key.

    Raises:
        CanonicalOrderError: For a null entry, a NaN key, keys that cannot be ordered
            against each other, or a duplicate key.
    """
    ranked = []
    for entry in entries:
        if entry is None:
            raise CanonicalOrderError("a dict value holds a null entry")
        ranked.append((_key_rank(entry["key"]), entry))
    return _sorted_by_rank(ranked)


def sort_set_elements(elements: list[Any]) -> list[Any]:
    """Return the storage elements of one set value in canonical order.

    Args:
        elements: The converted elements of a set.

    Returns:
        A new list holding the same elements, sorted.

    Raises:
        CanonicalOrderError: For a NaN element, elements that cannot be ordered against
            each other, or two elements with the same storage value.
    """
    return _sorted_by_rank([(_key_rank(element), element) for element in elements])


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
        CanonicalOrderError: For a key that has no canonical order (NaN), keys that
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
