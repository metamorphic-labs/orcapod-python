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
    expected: object
    value: object

    def describe(self) -> str | None:
        """Describe a nested mismatch for an error message; ``None`` at the top level."""
        if not self.path:
            return None
        return (
            f"{self.path} is {type_text(type(self.value))} ({self.value!r}), "
            f"expected {type_text(self.expected)}"
        )


def _numpy_scalar(value: object) -> tuple[str, int] | None:
    """Return ``(dtype kind, item size)`` of a NumPy scalar, or ``None`` for anything else.

    NumPy is only imported when the value's type comes from NumPy, so it is already loaded.
    """
    if not type(value).__module__.startswith("numpy"):
        return None
    import numpy as np

    if isinstance(value, np.generic):
        return value.dtype.kind, value.dtype.itemsize
    return None


def _numpy_kind(value: object) -> str | None:
    scalar = _numpy_scalar(value)
    return scalar[0] if scalar is not None else None


def _numpy_type(declared: object) -> tuple[str, int] | None:
    """Return ``(dtype kind, item size)`` of a declared NumPy scalar type (``numpy.int8``)."""
    if not isinstance(declared, type) or not declared.__module__.startswith("numpy"):
        return None
    import numpy as np

    if issubclass(declared, np.generic):
        dtype = np.dtype(declared)
        return dtype.kind, dtype.itemsize
    return None


def _check_integer(
    value: object, rule: ConformanceRule, exact: type | None = None
) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if exact is not None and type(value) is exact:
        return True
    return rule.numpy_scalar_same_kind and _numpy_kind(value) in ("i", "u")


def _check_float(value: object, rule: ConformanceRule, float32: bool = False) -> bool:
    scalar = _numpy_scalar(value)
    kind = scalar[0] if scalar is not None else None
    if isinstance(value, bool) or kind == "b":
        return rule.bool_for_float and isinstance(value, bool)
    if float32:
        if scalar == ("f", 4):
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


def _check_bool(value: object, rule: ConformanceRule) -> bool:
    if isinstance(value, bool):
        return True
    return rule.numpy_scalar_same_kind and _numpy_kind(value) == "b"


def _check_elements(
    values: Iterable[object], element_type: object, rule: ConformanceRule, path: str
) -> Nonconformance | None:
    for i, item in enumerate(values):
        found = find_nonconformance(item, element_type, rule, f"{path}[{i}]")
        if found is not None:
            return found
    return None


_PLAIN_TYPES: dict[object, type | tuple[type, ...]] = {
    str: str,
    bytes: bytes,
    dt.datetime: dt.datetime,
    dt.time: dt.time,
    dt.timedelta: dt.timedelta,
}


def find_nonconformance(
    value: object,
    declared: object,
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
    args: tuple[object, ...] = typing.get_args(declared)

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
    if declared is dt.date:
        if isinstance(value, dt.datetime):
            return None if rule.datetime_for_date else mismatch
        return None if isinstance(value, dt.date) else mismatch
    plain = _PLAIN_TYPES.get(declared)
    if plain is not None:
        return None if isinstance(value, plain) else mismatch

    numpy_type = _numpy_type(declared)
    if numpy_type is not None and isinstance(declared, type):
        kind, itemsize = numpy_type
        if kind in ("i", "u"):
            return None if _check_integer(value, rule, exact=declared) else mismatch
        if kind == "f":
            float32 = itemsize == 4
            return None if _check_float(value, rule, float32=float32) else mismatch
        if kind == "b":
            return None if _check_bool(value, rule) else mismatch
        return None

    if origin is list:
        element_type = args[0] if args else object
        if isinstance(value, list) or (
            rule.sequence_for_list and isinstance(value, tuple)
        ):
            return _check_elements(
                typing.cast("Iterable[object]", value), element_type, rule, path
            )
        if (
            rule.sequence_for_list
            and isinstance(value, Iterable)
            and not isinstance(value, (str, bytes, Mapping))
        ):
            # Other iterables pass unchecked: checking would consume a generator.
            return None
        return mismatch

    if origin is tuple:
        if not (
            isinstance(value, tuple)
            or (rule.sequence_for_list and isinstance(value, list))
        ):
            return mismatch
        items = list(typing.cast("Iterable[object]", value))
        if len(args) == 2 and args[1] is Ellipsis:
            return _check_elements(items, args[0], rule, path)
        if len(items) != len(args):
            return mismatch
        for i, (item, item_type) in enumerate(zip(items, args)):
            found = find_nonconformance(item, item_type, rule, f"{path}[{i}]")
            if found is not None:
                return found
        return None

    if origin is dict:
        if not isinstance(value, Mapping):
            return mismatch
        key_type, value_type = args if len(args) == 2 else (object, object)
        mapping = typing.cast("Mapping[object, object]", value)
        for key, item in mapping.items():
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
                typing.cast(object, getattr(value, field.name)),
                typing.cast(object, hints.get(field.name, object)),
                rule,
                f"{path}.{field.name}",
            )
            if found is not None:
                return found
        return None

    return None
