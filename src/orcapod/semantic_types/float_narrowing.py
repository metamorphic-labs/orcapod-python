"""Python floats entering a column declared as a narrower float type (ITL-924).

A column declared ``np.float32`` stores four-byte floats. A Python ``float`` given for
it is converted as Arrow converts it, with no check of the value: one that ``float32``
cannot hold exactly is stored changed (``0.1`` as ``0.10000000149011612``, ``1e-50`` as
``0.0``, ``1e300`` as ``inf``). Orcapod reports that with one
``orcapod.errors.ValueConversionWarning`` per process and declared type. Whether a
value is narrowed is decided from its type and the declared type, never from its value.

Dict keys and set elements are where narrowing can lose data: two that differ as
Python floats can be one ``float32`` value. ``stored_key_converter`` gives a key or
element as it is stored, so the duplicate check of ``canonical_order`` sees stored
values.

A leaf module: the function-output check (``output_conformance``) takes its
float-narrowing predicate from here.
"""

from __future__ import annotations

import os
import sys
import threading
import types
import typing
import warnings
from collections.abc import Callable
from typing import Any, cast, get_args, get_origin

import numpy as np

from orcapod.errors import ValueConversionWarning, type_text
from orcapod.types import DataType
from orcapod.utils.warning_utils import (
    error_filter_active,
    outside_orcapod_stacklevel,
    warn_at_frame,
)

# Byte width of a Python ``float``.
_PYTHON_FLOAT_WIDTH = 8

_lock = threading.RLock()
# The declared types whose narrowing warning was issued in this process.
_issued: set[type] = set()
_issuing: set[type] = set()
# Snapshot of the last filter list known to contain no applicable error action.
_non_escalating_filters = warnings.filters[:0]


def _after_fork() -> None:
    """Start fresh in the child, including a lock it can acquire."""
    global _lock
    _lock = threading.RLock()
    _issued.clear()
    _issuing.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


def narrows_float(value: object, declared_width: int) -> bool:
    """Whether ``value`` is a float wider than a declared float type.

    Decided from the type of ``value`` alone: its value is never read.

    Args:
        value: Any value.
        declared_width: Byte width of the declared float type.

    Returns:
        True for a Python ``float`` or NumPy floating scalar of more than
        ``declared_width`` bytes; False for a narrower or equal float and for a value
        that is not a float.
    """
    if isinstance(value, np.floating):
        return np.dtype(type(value)).itemsize > declared_width
    return isinstance(value, float) and _PYTHON_FLOAT_WIDTH > declared_width


def keep_float32_declaration(storage: DataType, declared: DataType) -> DataType:
    """Restore declared float32 leaves in an Arrow-derived conversion type.

    The Arrow-derived type controls every other conversion, including the
    container shape, nullability and conversion of booleans to integers.
    Registered classes already use their own field declarations.
    """
    if storage is float and declared is np.float32:
        return np.float32
    if storage == declared:
        return storage

    declared_origin = get_origin(declared)
    declared_args = cast(tuple[DataType, ...], get_args(declared))
    if declared_origin in (typing.Union, types.UnionType) and len(declared_args) == 2:  # pyright: ignore[reportDeprecated]  # detects Optional[T]
        if type(None) in declared_args:
            inner = next(arg for arg in declared_args if arg is not type(None))
            return keep_float32_declaration(storage, inner)

    storage_origin = get_origin(storage)
    storage_args = cast(tuple[DataType, ...], get_args(storage))
    if storage_origin in (typing.Union, types.UnionType) and len(storage_args) == 2:  # pyright: ignore[reportDeprecated]  # detects Optional[T]
        if type(None) in storage_args:
            inner = next(arg for arg in storage_args if arg is not type(None))
            return keep_float32_declaration(inner, declared) | None

    if (
        storage_origin is declared_origin
        and storage_origin in (dict, list, set, tuple)
        and len(storage_args) == len(declared_args)
    ):
        args = tuple(
            keep_float32_declaration(base, annotation)
            for base, annotation in zip(storage_args, declared_args)
        )
        if args != storage_args:
            return cast(
                DataType, cast(object, types.GenericAlias(storage_origin, args))
            )
    return storage


def narrower_float_width(python_type: object) -> int | None:
    """Return the byte width of a declared float type narrower than a Python float.

    Args:
        python_type: A declared Python type.

    Returns:
        The width (4 for ``np.float32``), or ``None`` when ``python_type`` is not a
        NumPy float type narrower than a Python ``float``.
    """
    if not isinstance(python_type, type) or not issubclass(python_type, np.floating):
        return None
    width = np.dtype(python_type).itemsize
    return width if width < _PYTHON_FLOAT_WIDTH else None


def _warn_narrowing(declared: type, message: str) -> None:
    """Issue the narrowing warning for ``declared``, once per process.

    An error filter still applies after the warning was issued.
    """
    global _non_escalating_filters
    with _lock:
        if declared in _issuing:
            return
        issued = declared in _issued
        if issued:
            filters = warnings.filters[:]
            if not any(
                action == "error" and issubclass(ValueConversionWarning, category)
                for action, _, category, _, _ in filters
            ):
                _non_escalating_filters = filters
                return
        frame = sys._getframe(outside_orcapod_stacklevel() - 1)  # pyright: ignore[reportPrivateUsage]  # locate the user's warning site
        if issued:
            if error_filter_active(message, ValueConversionWarning, frame):
                raise ValueConversionWarning(message)
            return
        _issuing.add(declared)
        try:
            # A fresh Python warning registry also keeps the parent's warning
            # site from suppressing the child's first warning.
            warn_at_frame(message, ValueConversionWarning, frame, fresh_registry=True)
            _issued.add(declared)
        finally:
            _issuing.discard(declared)


def noting_narrowed_floats(declared: type, width: int) -> Callable[[Any], Any]:
    """Return the Python-to-storage converter of a narrower float type.

    The converter returns every value as given (Arrow narrows it) and issues the
    narrowing warning the first time it is given a wider float.

    Args:
        declared: The declared type, e.g. ``np.float32``.
        width: Its byte width (``narrower_float_width``).

    Returns:
        The converter.
    """

    name = type_text(declared)
    message = (
        f"Python floats are being narrowed into {name}: a column declared {name} "
        "was given Python float values. They are converted as Arrow converts "
        f"them, without a check of each value, so a value {name} cannot hold "
        "exactly is stored changed (0.1 as 0.10000000149011612, 1e-50 as 0.0, "
        f"1e300 as inf). Pass {name} values, or declare the column float, to "
        "avoid this. This warning is issued once per process."
    )

    def convert(value: Any) -> Any:
        if (
            type(value) is not declared
            and (declared not in _issued or warnings.filters != _non_escalating_filters)
            and narrows_float(value, width)
        ):
            _warn_narrowing(declared, message)
        return value

    return convert


def stored_key_converter(
    key_type: object, convert: Callable[[Any], Any]
) -> Callable[[Any], Any]:
    """Return a converter giving a dict key or set element as it is stored.

    For a key type that is a narrower float, a wider float key is narrowed here as
    Arrow narrows it, so two keys that become one stored key are seen as duplicates.
    For every other key type ``convert`` is returned as it is.

    Args:
        key_type: The declared Python type of the keys or elements.
        convert: The Python-to-storage converter of ``key_type``.

    Returns:
        The converter of a key to its stored value.
    """
    if get_origin(key_type) in (typing.Union, types.UnionType):  # pyright: ignore[reportDeprecated]  # detects Optional[T]
        members = cast(tuple[object, ...], get_args(key_type))
        non_none = [member for member in members if member is not type(None)]
        if len(non_none) == 1 and len(members) == 2:
            key_type = non_none[0]
    width = narrower_float_width(key_type)
    if width is None:
        return convert
    declared: type[np.floating[Any]] = key_type  # pyright: ignore[reportAssignmentType]  # narrower_float_width checked it

    def to_stored_key(key: Any) -> Any:
        key = convert(key)
        if type(key) is not declared and narrows_float(key, width):
            # Arrow reads a Python float's payload directly, even for a
            # subclass overriding __float__.
            if isinstance(key, float):
                key = float.__float__(key)
            # A value too large becomes an infinity, as in Arrow; NumPy's overflow
            # warning would be a second report of the same narrowing.
            with np.errstate(over="ignore"):
                return declared(key)
        return key

    return to_stored_key


def reset_float_narrowing_warnings() -> None:
    """Forget which narrowing warnings were issued, so each is issued again.

    A test hook: a test that checks the warning starts as a new process would.
    """
    with _lock:
        _issued.clear()
