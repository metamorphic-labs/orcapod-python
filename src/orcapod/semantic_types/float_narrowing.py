"""Warn when a Python float enters a Python-declared float32 field."""

from __future__ import annotations

import os
import sys
import threading
import warnings
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from orcapod.errors import ValueConversionWarning
from orcapod.utils.warning_utils import (
    WarningRegistry,
    error_filter_active,
    outside_orcapod_stacklevel,
    warn_at_frame,
    warning_filter_action,
)

_MESSAGE = (
    "A column declared numpy.float32 was given Python floats. Values that "
    "numpy.float32 cannot hold exactly are stored changed. Pass numpy.float32 "
    "values to control the conversion, or declare the column float to keep "
    "Python float precision. This warning is issued once per process."
)

_lock = threading.RLock()
_issued = False
_issuing = False
# Snapshot of the last filter list known to contain no applicable error action.
_non_escalating_filters = warnings.filters[:0]
# None excludes every column, while a set excludes only named columns.
_float_check_exclusions: ContextVar[frozenset[str] | None] = ContextVar(
    "float_check_exclusions", default=frozenset()
)
# Keep provenance by row identity when stored and Python-origin rows share a schema.
stored_float_columns_by_row: ContextVar[dict[int, frozenset[str]] | None] = ContextVar(
    "stored_float_columns_by_row", default=None
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
    global _lock, _issued, _issuing
    _lock = threading.RLock()
    _issued = False
    _issuing = False


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


def warn_on_python_float(value: object) -> object:
    """Return the value unchanged and report Python-float input to float32."""
    if (
        issubclass(type(value), float)
        and (not _issued or warnings.filters != _non_escalating_filters)
        and checking_input_floats()
    ):
        _warn_narrowing()
    return value


def _warn_narrowing() -> None:
    """Issue the narrowing warning once per process.

    An error filter still applies after the warning was issued.
    """
    global _issued, _issuing, _non_escalating_filters
    with _lock:
        if _issuing:
            return
        if _issued:
            filters = warnings.filters[:]
            if not any(
                action == "error" and issubclass(ValueConversionWarning, category)
                for action, _, category, _, _ in filters
            ):
                _non_escalating_filters = filters
                return
        frame = sys._getframe(outside_orcapod_stacklevel() - 1)  # pyright: ignore[reportPrivateUsage]  # locate the user's warning site
        if _issued:
            if error_filter_active(_MESSAGE, ValueConversionWarning, frame):
                raise ValueConversionWarning(_MESSAGE)
            return
        _issuing = True
        try:
            # Python records default, once and module emissions in the supplied
            # registry. An always filter emits without writing a registry key.
            always = (
                warning_filter_action(_MESSAGE, ValueConversionWarning, frame)
                == "always"
            )
            registry: WarningRegistry = {}
            lineno = frame.f_lineno
            warn_at_frame(_MESSAGE, ValueConversionWarning, frame, registry=registry)
            _issued = always or (_MESSAGE, ValueConversionWarning, lineno) in registry
        finally:
            _issuing = False


def reset_float_narrowing_warnings() -> None:
    """Forget the previous narrowing warning.

    A test hook: a test that checks the warning starts as a new process would.
    """
    global _issued
    with _lock:
        _issued = False
