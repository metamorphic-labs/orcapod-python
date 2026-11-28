"""Python floats entering a column declared as a narrower float type (ITL-924).

A column declared ``np.float32`` stores four-byte floats. A Python ``float`` given for
it is converted as Arrow converts it, with no check of the value: one that ``float32``
cannot hold exactly is stored changed (``0.1`` as ``0.10000000149011612``, ``1e-50`` as
``0.0``, ``1e300`` as ``inf``). Orcapod reports that with one
``orcapod.errors.ValueConversionWarning`` per process and declared type. Whether a
value is narrowed is decided from its type and the declared type, never from its value.

Dict keys and set elements can collide when narrowing makes distinct inputs equal.
The collision check preserves the legacy key order and the values passed to Arrow.
"""

from __future__ import annotations

import os
import sys
import threading
import types
import typing
import warnings
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, cast, get_args, get_origin

import numpy as np

from orcapod.errors import ValueConversionWarning, type_text
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
# None excludes every column, while a set excludes only named columns.
_float_check_exclusions: ContextVar[frozenset[str] | None] = ContextVar(
    "float_check_exclusions", default=frozenset()
)


def checking_input_floats() -> bool:
    """Whether the current value is eligible for float input diagnostics."""
    return _float_check_exclusions.get() is not None


def checked_float_columns(names: Iterable[str]) -> frozenset[str]:
    """Find columns already validated by a function or stored as Arrow."""
    excluded = _float_check_exclusions.get()
    return frozenset(names) if excluded is None else excluded.intersection(names)


@contextmanager
def skip_float_checks(columns: frozenset[str] | None = None) -> Iterator[None]:
    """Suppress input diagnostics for checked columns in the current conversion."""
    prior = _float_check_exclusions.get()
    if prior is None or columns == frozenset():
        yield
        return
    token = _float_check_exclusions.set(None if columns is None else prior | columns)
    try:
        yield
    finally:
        _float_check_exclusions.reset(token)


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
            and checking_input_floats()
            and narrows_float(value, width)
        ):
            _warn_narrowing(declared, message)
        return value

    return convert


def float32_key_check(
    key_type: object,
) -> Callable[[object], float | None] | None:
    """Return a collision check for supported float32 keys and elements.

    The rounded value is only used to find collisions. Canonical ordering and
    Arrow still receive the original key. Skip NumPy subclasses whose conversion
    can invoke a user-defined method.
    """
    if get_origin(key_type) in (typing.Union, types.UnionType):  # pyright: ignore[reportDeprecated]  # detects Optional[T]
        members = cast(tuple[object, ...], get_args(key_type))
        non_none = [member for member in members if member is not type(None)]
        if len(non_none) == 1 and len(members) == 2:
            key_type = non_none[0]
    if key_type is not np.float32:
        return None
    return _float32_key


def _float32_key(key: object) -> float | None:
    if isinstance(key, float):
        # Arrow reads the payload of Python floats and their subclasses directly.
        value = float.__float__(key)
    elif type(key) in (np.float16, np.float32, np.longdouble):
        value = float(cast(np.floating[Any], key))
    else:
        return None
    with np.errstate(over="ignore"):
        return float(np.float32(value))


def reset_float_narrowing_warnings() -> None:
    """Forget which narrowing warnings were issued, so each is issued again.

    A test hook: a test that checks the warning starts as a new process would.
    """
    with _lock:
        _issued.clear()
