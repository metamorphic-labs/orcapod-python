"""Hashable match keys for the async join operators (ITL-1044).

The sync ``Join`` and ``SemiJoin`` match rows with a Polars join on the key
columns. The async paths index rows in a dictionary, so each key value must be
hashable and equal exactly when Polars treats two values as equal. Measured on
the Polars join:

* a list matches a list of the same elements in the same order; an empty list
  is not a null list; nulls inside a list or struct compare equal;
* a struct matches field by field; a null struct is not a struct of nulls;
* NaN equals NaN (also inside a list); ``0.0`` equals ``-0.0``;
* a null never matches, whatever its type (the sync join keys on the column
  together with a constant column, which makes a null list or struct unequal
  to another one too).

Values are read from the row's Arrow column (the storage of a logical column),
never from ``as_dict()``, and never through ``repr`` or ``str``.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any, cast

import pyarrow as pa

# The frozen form of a NaN value: equal to itself, as NaN is in Polars.
_NAN = object()


def freeze(value: object) -> object:
    """Return a Python value of an Arrow column in a hashable form.

    Structs (dicts) become tuples of their items in field order, lists become
    tuples, and NaN becomes one shared sentinel. Both inputs' key types are
    checked equal first, so frozen values of different types never meet.

    Args:
        value: A Python value (from ``as_py``) of an Arrow key column.

    Returns:
        The hashable form of ``value``.
    """
    if isinstance(value, float) and math.isnan(value):
        return _NAN
    if isinstance(value, dict):
        items = cast("dict[str, object]", value)
        return tuple((name, freeze(item)) for name, item in items.items())
    if isinstance(value, (list, tuple)):
        return tuple(freeze(item) for item in cast("Sequence[object]", value))
    return value


def frozen_scalar(scalar: "pa.Scalar[Any]") -> object | None:  # pyright: ignore[reportExplicitAny]
    """Return an Arrow scalar's match key, as the sync ``Join`` equality has it.

    Args:
        scalar: One value of a key column.

    Returns:
        ``None`` for a null (a null primitive, list or struct never matches,
        because the sync join also keys on a constant column), otherwise the
        frozen value.
    """
    storage = scalar.value if isinstance(scalar, pa.ExtensionScalar) else scalar
    if storage is None or not storage.is_valid:
        return None
    return freeze(storage.as_py())
