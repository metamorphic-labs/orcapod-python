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

Keys or elements that cannot be ordered against each other, and two keys or elements
with the same storage value, raise ``CanonicalOrderError``.

For a declared float32 input, a separate collision check compares rounded keys or
elements (ITL-924). The original values still determine the order and go to Arrow.
"""

from __future__ import annotations

import math
import numbers
import types
import typing
from collections.abc import Callable, Collection, Iterable
from decimal import Decimal
from itertools import pairwise
from typing import Any, TypeVar, cast, get_args, get_origin

from orcapod.errors import CanonicalOrderError
from orcapod.semantic_types.float_narrowing import (
    checking_input_floats,
    float32_key_check,
)

_T = TypeVar("_T")
_V = TypeVar("_V")

# A sort key: ``(0,)`` for None, ``(1, value)`` otherwise.
_Rank = tuple[Any, ...]
_KeyCheck = Callable[[object], float | None]


class _DuplicateRank(CanonicalOrderError):
    """Two entries have the same rank, at their original positions."""

    def __init__(
        self, first: int, second: int, stored: object, payload: object
    ) -> None:
        super().__init__(f"duplicate key or set element: {payload!r}")
        self.positions: tuple[int, int] = first, second
        self.stored: object = stored


def _collision_error(
    collision: _DuplicateRank, given: Iterable[object], kind: str
) -> CanonicalOrderError:
    """Describe the conflicting inputs in the caller's container."""
    first, second = collision.positions
    values = list(given)
    return CanonicalOrderError(
        f"duplicate {kind}: {values[first]!r} and {values[second]!r} are "
        + f"both stored as {collision.stored!r}, so one would be lost"
    )


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


def _sorted_by_rank(
    items: list[tuple[_Rank, _T]], key_check: _KeyCheck | None = None
) -> list[_T]:
    """Sort ``(rank, payload)`` pairs by rank and return the payloads.

    Args:
        items: The pairs to sort.
        key_check: An optional projection used only to detect stored collisions.

    Raises:
        CanonicalOrderError: If two ranks cannot be compared.
        _DuplicateRank: If two ranks are equal.
    """
    try:
        order = sorted(range(len(items)), key=lambda index: items[index][0])
    except TypeError as exc:
        raise CanonicalOrderError(f"values cannot be ordered: {exc}") from exc
    for first, second in pairwise(order):
        if items[first][0] == items[second][0]:
            stored: object = items[first][0][-1] if len(items[first][0]) > 1 else None
            raise _DuplicateRank(first, second, stored, items[second][1])
    if key_check is not None and checking_input_floats():
        seen: dict[float, int] = {}
        for index in order:
            rank, payload = items[index]
            rounded = key_check(rank[-1]) if len(rank) > 1 else None
            if rounded is not None:
                first = seen.setdefault(rounded, index)
                if first != index:
                    raise _DuplicateRank(first, index, rounded, payload)
    return [items[index][1] for index in order]


def sort_dict_entries(
    value: dict[_T, _V],
    convert_key: Callable[[_T], object],
    convert_value: Callable[[_V], object],
    key_check: _KeyCheck | None = None,
) -> list[dict[str, object]]:
    """Convert a dict into entries in canonical order of their stored keys.

    Args:
        value: The original dict.
        convert_key: Converts a key to its stored value.
        convert_value: Converts a value to its stored value.
        key_check: An optional projection for stored-key collision detection.

    Returns:
        A list of entries ``{"key": ..., "value": ...}``, sorted by key.

    Raises:
        CanonicalOrderError: For a NaN key, keys that cannot be ordered against each
            other, or two keys with the same storage value.
    """
    entries = [
        {"key": convert_key(key), "value": convert_value(item)}
        for key, item in value.items()
    ]
    try:
        return _sorted_by_rank(
            [(_key_rank(entry["key"]), entry) for entry in entries], key_check
        )
    except _DuplicateRank as exc:
        if key_check is None or not checking_input_floats():
            raise
        raise _collision_error(exc, value, "key") from exc


def sort_set_elements(
    elements: Collection[_T],
    convert: Callable[[_T], _T] | None = None,
    key_check: _KeyCheck | None = None,
) -> list[_T]:
    """Return the storage elements of one set value in canonical order.

    Args:
        elements: The elements of a set.
        convert: Converts an element to its stored value, if it is still a Python value.
        key_check: An optional projection for stored-element collision detection.

    Returns:
        A new list holding the same elements, sorted.

    Raises:
        CanonicalOrderError: For a NaN element, elements that cannot be ordered against
            each other, or two elements with the same storage value.
    """
    stored = elements if convert is None else [convert(element) for element in elements]
    try:
        return _sorted_by_rank(
            [(_key_rank(element), element) for element in stored], key_check
        )
    except _DuplicateRank as exc:
        if key_check is None or not checking_input_floats():
            raise
        raise _collision_error(exc, elements, "set element") from exc


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
        key_check = float32_key_check(args[0])
        items = [
            (
                _key_rank(to_storage(k)),
                (k, canonical_python_value(v, args[1], key_converter)),
            )
            for k, v in original.items()
        ]
        try:
            ordered = _sorted_by_rank(items, key_check)
        except _DuplicateRank as exc:
            if key_check is None or not checking_input_floats():
                raise
            raise _collision_error(exc, original, "key") from exc
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
