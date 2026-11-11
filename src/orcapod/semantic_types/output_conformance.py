"""Does a function's returned value conform to its declared output type? (ITL-895)

pyarrow's Python-to-Arrow conversion is not type strict. For an integer field it calls
``__int__`` on anything that is not an ``int``, so ``1.5`` (or ``numpy.float64(1.5)``,
``Decimal("1.5")``) is stored as ``1``. Numbers are read as epoch ticks for date and
timestamp fields, ``bytes`` and ``str`` are decoded/encoded into each other, and ``None``
is written into a field declared not null. Orcapod is type strict, so a function pod
checks every returned value against its declared output type before converting it.

``OUTPUT_CONFORMANCE`` is the one table of conversions that are accepted although the
value's type is not exactly the declared type. Each field is one decision (D1-D7 in
``superpowers/specs/itl-895-float-to-int/FINDINGS.md``, section 11).

Types this module does not know (logical types such as ``Path``, ``UUID`` or NumPy
arrays, pydantic models, ``Literal``, ``Any``) are left to their own converter.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import types
import typing
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from orcapod.errors import type_text


@dataclass(frozen=True)
class ConformanceRule:
    """Which near-miss conversions of a returned value are accepted.

    Anything not listed must be exactly the declared type (a subclass is accepted: it is
    the declared type and keeps its value). Every accepted conversion here keeps the value
    exactly, except D7, where storing a Python ``float`` as float32 rounds it.

    Attributes:
        int_for_float: D1 — an ``int`` for an output declared ``float``.
        numpy_scalar_same_kind: D2 — a NumPy scalar of the same kind (integer for an
            integer type, floating for a floating type, bool for ``bool``).
        bool_for_float: D3 — a ``bool`` for an output declared ``float``.
        none_for_non_optional: D4 — ``None`` for an output not declared ``T | None``.
        sequence_for_list: D5 — a ``tuple`` for ``list[T]`` or a ``list`` for
            ``tuple[...]``; other non-string iterables for ``list[T]`` pass unchecked.
        datetime_for_date: D6 — a ``datetime`` for an output declared ``date`` (the time
            is dropped).
        float_for_float32: D7 — a Python ``float`` (or a NumPy float of another width) for
            an output declared ``numpy.float32`` (the value is rounded to float32).
    """

    int_for_float: bool
    numpy_scalar_same_kind: bool
    bool_for_float: bool
    none_for_non_optional: bool
    sequence_for_list: bool
    datetime_for_date: bool
    float_for_float32: bool


# The table. Undecided entries keep the behaviour Orcapod had before ITL-895.
OUTPUT_CONFORMANCE = ConformanceRule(
    int_for_float=True,  # D1, undecided: accepted as before
    numpy_scalar_same_kind=True,  # D2, undecided: accepted as before
    bool_for_float=True,  # D3, undecided: accepted as before
    none_for_non_optional=False,  # D4, decided: rejected
    sequence_for_list=True,  # D5, undecided: accepted as before
    datetime_for_date=False,  # D6, decided: rejected (the time was dropped)
    float_for_float32=True,  # D7, undecided: accepted as before
)


@dataclass(frozen=True)
class Nonconformance:
    """The first place where a value does not conform to its declared type.

    Attributes:
        path: Where inside the returned value, such as ``[1]`` or ``.count``; empty for
            the value as a whole.
        expected: The declared type at that place.
        value: The offending value at that place.
    """

    path: str
    expected: Any
    value: Any

    def describe(self) -> str | None:
        """Describe a nested mismatch for an error message; ``None`` at the top level."""
        if not self.path:
            return None
        return (
            f"{self.path} is {type_text(type(self.value))} ({self.value!r}), "
            f"expected {type_text(self.expected)}"
        )


def _numpy_kind(value: Any) -> str | None:
    """Return the NumPy dtype kind of a NumPy scalar, or ``None`` for anything else.

    NumPy is only imported when the value's type comes from NumPy, so it is already loaded.
    """
    if not type(value).__module__.startswith("numpy"):
        return None
    import numpy as np

    if isinstance(value, np.generic):
        return value.dtype.kind
    return None


def _numpy_type_kind(declared: Any) -> str | None:
    """Return the dtype kind of a declared NumPy scalar type such as ``numpy.int8``."""
    if not isinstance(declared, type) or not declared.__module__.startswith("numpy"):
        return None
    import numpy as np

    if issubclass(declared, np.generic):
        return np.dtype(declared).kind
    return None


def _check_integer(value: Any, rule: ConformanceRule, exact: Any = None) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if exact is not None and type(value) is exact:
        return True
    return rule.numpy_scalar_same_kind and _numpy_kind(value) in ("i", "u")


def _check_float(value: Any, rule: ConformanceRule, float32: bool = False) -> bool:
    kind = _numpy_kind(value)
    if isinstance(value, bool) or kind == "b":
        return rule.bool_for_float and isinstance(value, bool)
    if float32:
        if kind == "f" and value.dtype.itemsize == 4:
            return True
        if isinstance(value, float) or kind == "f":
            return rule.float_for_float32 and (
                kind is None or rule.numpy_scalar_same_kind
            )
    else:
        if isinstance(value, float):  # numpy.float64 is a float subclass
            return True
        if kind == "f":
            return rule.numpy_scalar_same_kind
    if isinstance(value, int):
        return rule.int_for_float
    if kind in ("i", "u"):
        return rule.int_for_float and rule.numpy_scalar_same_kind
    return False


def _check_bool(value: Any, rule: ConformanceRule) -> bool:
    if isinstance(value, bool):
        return True
    return rule.numpy_scalar_same_kind and _numpy_kind(value) == "b"


def _check_elements(
    values: Iterable[Any], element_type: Any, rule: ConformanceRule, path: str
) -> Nonconformance | None:
    for i, item in enumerate(values):
        found = find_nonconformance(item, element_type, rule, f"{path}[{i}]")
        if found is not None:
            return found
    return None


def find_nonconformance(
    value: Any,
    declared: Any,
    rule: ConformanceRule = OUTPUT_CONFORMANCE,
    path: str = "",
) -> Nonconformance | None:
    """Return the first place where ``value`` does not conform to ``declared``.

    Args:
        value: A value returned by a function for one output.
        declared: The output's declared type.
        rule: The accepted near-miss conversions.
        path: Location of ``value`` inside the returned value (used when recursing).

    Returns:
        The first mismatch found, or ``None`` when the value conforms or its declared
        type is one this module leaves to the type's own converter.
    """
    mismatch = Nonconformance(path, declared, value)
    origin = typing.get_origin(declared)
    args = typing.get_args(declared)

    if origin is typing.Union or origin is types.UnionType:
        non_none = [a for a in args if a is not type(None)]
        if value is None and len(non_none) < len(args):
            return None
        if len(non_none) == 1:
            return find_nonconformance(value, non_none[0], rule, path)
        return None  # complex unions are rejected by the type converter itself

    if value is None:
        return None if rule.none_for_non_optional else mismatch

    if declared is bool:
        return None if _check_bool(value, rule) else mismatch
    if declared is int:
        return None if _check_integer(value, rule) else mismatch
    if declared is float:
        return None if _check_float(value, rule) else mismatch
    if declared is str:
        return None if isinstance(value, str) else mismatch
    if declared is bytes:
        return None if isinstance(value, bytes) else mismatch
    if declared is dt.datetime:
        return None if isinstance(value, dt.datetime) else mismatch
    if declared is dt.date:
        if isinstance(value, dt.datetime):
            return None if rule.datetime_for_date else mismatch
        return None if isinstance(value, dt.date) else mismatch
    if declared is dt.time:
        return None if isinstance(value, dt.time) else mismatch
    if declared is dt.timedelta:
        return None if isinstance(value, dt.timedelta) else mismatch

    numpy_kind = _numpy_type_kind(declared)
    if numpy_kind in ("i", "u"):
        return None if _check_integer(value, rule, exact=declared) else mismatch
    if numpy_kind == "f":
        import numpy as np

        is_float32 = np.dtype(declared).itemsize == 4
        return None if _check_float(value, rule, float32=is_float32) else mismatch
    if numpy_kind == "b":
        return None if _check_bool(value, rule) else mismatch

    if origin is list:
        element_type = args[0] if args else Any
        if isinstance(value, list) or (
            rule.sequence_for_list and isinstance(value, tuple)
        ):
            return _check_elements(value, element_type, rule, path)
        if (
            rule.sequence_for_list
            and isinstance(value, Iterable)
            and not isinstance(value, (str, bytes, Mapping))
        ):
            return (
                None  # other iterables pass unchecked (consuming them would empty them)
            )
        return mismatch

    if origin is tuple:
        if not (
            isinstance(value, tuple)
            or (rule.sequence_for_list and isinstance(value, list))
        ):
            return mismatch
        if len(args) == 2 and args[1] is Ellipsis:
            return _check_elements(value, args[0], rule, path)
        if len(value) != len(args):
            return mismatch
        for i, (item, item_type) in enumerate(zip(value, args)):
            found = find_nonconformance(item, item_type, rule, f"{path}[{i}]")
            if found is not None:
                return found
        return None

    if origin is dict:
        if not isinstance(value, Mapping):
            return mismatch
        key_type, value_type = args if len(args) == 2 else (Any, Any)
        for key, item in value.items():
            found = find_nonconformance(key, key_type, rule, f"{path}[{key!r}] key")
            if found is None:
                found = find_nonconformance(item, value_type, rule, f"{path}[{key!r}]")
            if found is not None:
                return found
        return None

    if isinstance(declared, type) and dataclasses.is_dataclass(declared):
        if not isinstance(value, declared):
            return None  # left to the dataclass logical type's converter, as before
        try:
            hints = typing.get_type_hints(declared)
        except Exception:  # noqa: BLE001 - unresolvable annotations: nothing to check against
            return None
        for field in dataclasses.fields(declared):
            found = find_nonconformance(
                getattr(value, field.name),
                hints.get(field.name, Any),
                rule,
                f"{path}.{field.name}",
            )
            if found is not None:
                return found
        return None

    return None
