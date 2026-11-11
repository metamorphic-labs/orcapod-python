"""Does a function's returned value conform to its declared output type? (ITL-895)

pyarrow's Python-to-Arrow conversion is not type strict. For an integer field it calls
``__int__`` on anything that is not an ``int``, so ``1.5`` (or ``numpy.float64(1.5)``,
``Decimal("1.5")``) is stored as ``1``. Numbers are read as epoch ticks for date and
timestamp fields, ``bytes`` and ``str`` are decoded/encoded into each other, and ``None``
is written into a field declared not null. Orcapod is type strict, so a function pod
checks every returned value against its declared output type before converting it.

A value whose type is not exactly the declared type is a near miss (``NearMiss``).
``OUTPUT_CONFORMANCE`` is the one table that says how each near miss is treated: accepted
silently, accepted and converted with a warning, or rejected. The entries are the
developer's decisions of 2026-11-11 (D1-D7 in
``superpowers/specs/itl-895-float-to-int/FINDINGS.md``, section 11, and ITL-901).

Types this module does not know (logical types such as ``Path``, ``UUID`` or NumPy
arrays, pydantic models, ``Literal``, ``Any``) are left to their own converter.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import enum
import types
import typing
from collections.abc import Iterable, Iterator, ItemsView, KeysView, Mapping, Set
from dataclasses import dataclass

from orcapod.errors import type_text

if typing.TYPE_CHECKING:
    import numpy as np
    import numpy.typing as npt


class Treatment(enum.Enum):
    """How a near miss is treated."""

    ACCEPT = "accept"
    """Accepted silently: the conversion keeps the value exactly."""

    CONVERT_WITH_WARNING = "convert with warning"
    """Accepted and converted, with an ``ValueConversionWarning``."""

    REJECT = "reject"
    """Rejected with an ``OutputTypeError`` that says what to return instead."""


class NearMiss(enum.Enum):
    """A returned value whose type is close to, but not exactly, the declared type."""

    INT_FOR_FLOAT = "int_for_float"
    """D1: an ``int`` (or NumPy integer) for a floating output."""

    NUMPY_SCALAR_SAME_KIND = "numpy_scalar_same_kind"
    """D2: a NumPy scalar of the same kind, value-exact (``numpy.int64`` for ``int``)."""

    BOOL_FOR_FLOAT = "bool_for_float"
    """D3: a ``bool`` (or ``numpy.bool_``) for a floating output."""

    NONE_FOR_NON_OPTIONAL = "none_for_non_optional"
    """D4: ``None`` for an output not declared ``T | None``."""

    TUPLE_FOR_LIST = "tuple_for_list"
    """D5: a ``tuple`` for ``list[T]``."""

    DATETIME_FOR_DATE = "datetime_for_date"
    """D6: a ``datetime`` for ``date``."""

    FLOAT_FOR_FLOAT32 = "float_for_float32"
    """D7: a Python ``float`` (or a wider NumPy float) for ``numpy.float32``."""

    LONGDOUBLE_FOR_FLOAT = "longdouble_for_float"
    """A ``numpy.longdouble`` wider than 64 bits for ``float`` (the D7 analogue)."""

    SET_FOR_LIST = "set_for_list"
    """ITL-901: a ``set`` or ``frozenset`` for ``list[T]``."""

    ITERABLE_FOR_LIST = "iterable_for_list"
    """A re-iterable ordered container (NumPy array, ``deque``, ``range``, dict view)
    for ``list[T]``. Provisional: the developer decides between accepting it with a
    warning and rejecting it. One-shot iterators are always rejected: checking them
    would consume them."""

    @property
    def done(self) -> str:
        """What Orcapod does to the value when it converts it, for the warning."""
        return _NEAR_MISS_TEXT[self][0]

    @property
    def avoid(self) -> str:
        """How the function avoids the conversion, for the warning."""
        return _NEAR_MISS_TEXT[self][1]


_NEAR_MISS_TEXT: dict[NearMiss, tuple[str, str]] = {
    NearMiss.INT_FOR_FLOAT: ("converted it to a float", "Return float(value)"),
    NearMiss.NUMPY_SCALAR_SAME_KIND: (
        "converted it to a Python value",
        "Return value.item()",
    ),
    NearMiss.BOOL_FOR_FLOAT: ("converted it to a float", "Return float(value)"),
    NearMiss.NONE_FOR_NON_OPTIONAL: ("stored a null", "Return a value"),
    NearMiss.TUPLE_FOR_LIST: ("converted it to a list", "Return a list (list(value))"),
    NearMiss.DATETIME_FOR_DATE: ("dropped the time of day", "Return value.date()"),
    NearMiss.FLOAT_FOR_FLOAT32: (
        "rounded it to 32 bits",
        "Return numpy.float32(value)",
    ),
    NearMiss.LONGDOUBLE_FOR_FLOAT: ("rounded it to 64 bits", "Return float(value)"),
    NearMiss.SET_FOR_LIST: (
        "converted it to a list in iteration order, which differs between processes",
        "Return sorted(value)",
    ),
    NearMiss.ITERABLE_FOR_LIST: (
        "converted it to a list",
        "Return a list (list(value))",
    ),
}


# The table: one line per decision (developer, 2026-11-11).
OUTPUT_CONFORMANCE: Mapping[NearMiss, Treatment] = types.MappingProxyType(
    {
        NearMiss.INT_FOR_FLOAT: Treatment.REJECT,  # D1
        NearMiss.NUMPY_SCALAR_SAME_KIND: Treatment.ACCEPT,  # D2
        NearMiss.BOOL_FOR_FLOAT: Treatment.REJECT,  # D3
        NearMiss.NONE_FOR_NON_OPTIONAL: Treatment.REJECT,  # D4
        NearMiss.TUPLE_FOR_LIST: Treatment.CONVERT_WITH_WARNING,  # D5
        NearMiss.DATETIME_FOR_DATE: Treatment.CONVERT_WITH_WARNING,  # D6
        NearMiss.FLOAT_FOR_FLOAT32: Treatment.CONVERT_WITH_WARNING,  # D7
        NearMiss.LONGDOUBLE_FOR_FLOAT: Treatment.CONVERT_WITH_WARNING,  # D7 analogue
        NearMiss.SET_FOR_LIST: Treatment.REJECT,  # ITL-901
        NearMiss.ITERABLE_FOR_LIST: Treatment.CONVERT_WITH_WARNING,  # provisional
    }
)


@dataclass(frozen=True)
class Nonconformance:
    """The first place where a value does not conform to its declared type.

    Attributes:
        path: Where inside the returned value, such as ``[1]`` or ``.count``; empty for
            the value as a whole.
        expected: The declared type at that place.
        value: The offending value at that place.
        hint: What to return instead, when there is a specific fix.
    """

    path: str
    expected: object
    value: object
    hint: str | None = None

    def describe(self) -> str | None:
        """Describe a nested mismatch for an error message; ``None`` at the top level.

        Types only, never the value: the message is logged by pipelines.
        """
        if not self.path:
            return None
        return (
            f"{self.path} is {type_text(type(self.value))}, "
            + f"expected {type_text(self.expected)}"
        )


@dataclass(frozen=True)
class Conversion:
    """A near miss that is accepted and converted, with a warning.

    Attributes:
        location: Where inside the returned value, by structure only (``[*]`` for any
            list element or dict value, ``.keys()`` for dict keys, ``.field`` for a
            dataclass field), so it is the same on every row; empty for the value as a
            whole.
        expected: The declared type at that place.
        returned_type: The type of the value at that place.
        near_miss: Which near miss it is.
    """

    location: str
    expected: object
    returned_type: type
    near_miss: NearMiss


@dataclass(frozen=True)
class ConformanceReport:
    """The result of checking one returned value.

    Attributes:
        mismatch: The first place where the value does not conform, or ``None``.
        conversions: The conversions the value needs, each listed once, when it conforms.
    """

    mismatch: Nonconformance | None
    conversions: tuple[Conversion, ...]


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


def _numpy_type(declared: object) -> tuple[str, int] | None:
    """Return ``(dtype kind, item size)`` of a declared NumPy scalar type (``numpy.int8``)."""
    if not isinstance(declared, type) or not declared.__module__.startswith("numpy"):
        return None
    import numpy as np

    if issubclass(declared, np.generic):
        dtype = np.dtype(declared)
        return dtype.kind, dtype.itemsize
    return None


def _numpy_array(value: object) -> npt.NDArray[np.generic] | None:
    """Return ``value`` if it is a NumPy array, else ``None``."""
    if not type(value).__module__.startswith("numpy"):
        return None
    import numpy as np

    if isinstance(value, np.ndarray):
        return typing.cast("npt.NDArray[np.generic]", value)
    return None


def _float_hint(value: object, declared: object) -> str:
    """Say what to return instead of an integer or bool for a floating output.

    Args:
        value: The returned value; for ``float`` it gives the example (``0`` -> ``0.0``).
        declared: The declared floating type (``float`` or a NumPy float type).

    Returns:
        The hint, such as ``Return a float instead: 0.0 or float(value)``.
    """
    if declared is float:
        try:
            example = repr(float(typing.cast(typing.SupportsFloat, value)))
        except (TypeError, ValueError, OverflowError):
            return "Return a float instead: float(value)"
        return f"Return a float instead: {example} or float(value)"
    return f"Return a {type_text(declared)} instead: {type_text(declared)}(value)"


def _reject_hint(near_miss: NearMiss, value: object, declared: object) -> str:
    """What to return instead of a rejected near miss."""
    if near_miss in (NearMiss.INT_FOR_FLOAT, NearMiss.BOOL_FOR_FLOAT):
        return _float_hint(value, declared)
    if near_miss is NearMiss.NONE_FOR_NON_OPTIONAL:
        name = type_text(declared)
        return f"Return a {name}, or declare the output {name} | None"
    if near_miss is NearMiss.SET_FOR_LIST:
        return "Return a list in a fixed order instead, e.g. sorted(value)"
    if near_miss in (NearMiss.TUPLE_FOR_LIST, NearMiss.ITERABLE_FOR_LIST):
        return "Return a list instead: list(value)"
    return f"{near_miss.avoid} instead"


def _key_text(key: object) -> str:
    """Show a dict key in a mismatch path, shortened to at most 40 characters."""
    text = repr(key)
    return text if len(text) <= 40 else text[:37] + "..."


_FLOAT_FOR_INT_HINT = "Return an int instead: round(value), or int(value) to truncate"
_ITERATOR_HINT = "Return a list instead: list(value)"

_PLAIN_TYPES: dict[object, type] = {
    str: str,
    bytes: bytes,
    dt.datetime: dt.datetime,
    dt.time: dt.time,
    dt.timedelta: dt.timedelta,
}


_Result: typing.TypeAlias = tuple[Nonconformance | None, tuple[Conversion, ...]]
_CONFORMS: _Result = (None, ())


def _mismatch(value: object, declared: object, hint: str | None = None) -> _Result:
    """Return the result for a value that does not conform at this place.

    Args:
        value: The value at this place.
        declared: The declared type at this place.
        hint: What to return instead, when there is a specific fix.

    Returns:
        A result holding the mismatch (with an empty path) and no conversions.
    """
    return Nonconformance("", declared, value, hint), ()


def _at(result: _Result, path: str, location: str) -> _Result:
    """Place a nested result inside its container.

    Args:
        result: The result of checking one element, field, key or value.
        path: Where that part is in this value (``[3]``, ``['a']``, ``.count``).
        location: The same place by structure only (``[*]``, ``.count``).

    Returns:
        The result with ``path`` prefixed to the mismatch and ``location`` to every
        conversion.
    """
    mismatch, conversions = result
    if mismatch is not None:
        return dataclasses.replace(mismatch, path=path + mismatch.path), ()
    if not conversions:
        return _CONFORMS
    return None, tuple(
        dataclasses.replace(c, location=location + c.location) for c in conversions
    )


def _merge(results: Iterable[_Result]) -> _Result:
    """Combine the results of a value's parts: the first mismatch, or each conversion once."""
    merged: dict[tuple[NearMiss, str], Conversion] = {}
    for mismatch, conversions in results:
        if mismatch is not None:
            return mismatch, ()
        for conversion in conversions:
            _ = merged.setdefault(
                (conversion.near_miss, conversion.location), conversion
            )
    return (None, tuple(merged.values())) if merged else _CONFORMS


class _Checker:
    """Checks returned values against declared types under one conformance table."""

    def __init__(self, rule: Mapping[NearMiss, Treatment]) -> None:
        """Check under ``rule``, the treatment of each near miss."""
        self._rule: Mapping[NearMiss, Treatment] = rule

    def near(self, near_miss: NearMiss, value: object, declared: object) -> _Result:
        """Apply the table to a near miss: accept, convert with a warning, or reject."""
        treatment = self._rule[near_miss]
        if treatment is Treatment.REJECT:
            return _mismatch(value, declared, _reject_hint(near_miss, value, declared))
        if treatment is Treatment.CONVERT_WITH_WARNING:
            return None, (Conversion("", declared, type(value), near_miss),)
        return _CONFORMS

    def check(self, value: object, declared: object) -> _Result:
        """Check ``value`` against ``declared``; paths in the result are relative to it."""
        origin = typing.get_origin(declared)
        args: tuple[object, ...] = typing.get_args(declared)

        if origin is typing.Union or origin is types.UnionType:
            non_none = [a for a in args if a is not type(None)]
            if value is None and len(non_none) < len(args):
                return _CONFORMS
            if len(non_none) == 1:
                return self.check(value, non_none[0])
            return _CONFORMS  # complex unions are rejected by the type converter itself

        if value is None:
            return self.near(NearMiss.NONE_FOR_NON_OPTIONAL, value, declared)

        if declared is bool:
            return self.check_bool(value, declared)
        if declared is int:
            return self.check_integer(value, declared)
        if declared is float:
            return self.check_float(value, declared)
        if declared is dt.date:
            if isinstance(value, dt.datetime):
                return self.near(NearMiss.DATETIME_FOR_DATE, value, declared)
            return (
                _CONFORMS if isinstance(value, dt.date) else _mismatch(value, declared)
            )
        plain = _PLAIN_TYPES.get(declared)
        if plain is not None:
            return _CONFORMS if isinstance(value, plain) else _mismatch(value, declared)

        numpy_type = _numpy_type(declared)
        if numpy_type is not None:
            kind, itemsize = numpy_type
            if kind in ("i", "u"):
                return self.check_integer(value, declared)
            if kind == "f" and itemsize == 8:
                return self.check_float(value, declared)
            if kind == "f" and itemsize == 4:
                return self.check_float32(value, declared)
            if kind == "b":
                return self.check_bool(value, declared)
            return _CONFORMS

        if origin is list:
            element_type = args[0] if args else object
            return self.check_list(value, declared, element_type)

        if origin is dict:
            if not isinstance(value, Mapping):
                return _mismatch(value, declared)
            key_type, value_type = args if len(args) == 2 else (object, object)
            mapping = typing.cast("Mapping[object, object]", value)
            return _merge(self.check_items(mapping, key_type, value_type))

        if isinstance(declared, type) and dataclasses.is_dataclass(declared):
            if not isinstance(value, declared):
                return _CONFORMS  # left to the dataclass logical type's converter
            try:
                hints = typing.get_type_hints(declared)
            except Exception:  # noqa: BLE001 - unresolvable annotations: nothing to check against
                return _CONFORMS
            return _merge(
                _at(
                    self.check(
                        typing.cast(object, getattr(value, field.name)),
                        typing.cast(object, hints.get(field.name, object)),
                    ),
                    f".{field.name}",
                    f".{field.name}",
                )
                for field in dataclasses.fields(declared)
            )

        return _CONFORMS

    def check_bool(self, value: object, declared: object) -> _Result:
        """``bool`` or ``numpy.bool_``: a ``bool``, or a NumPy bool (D2)."""
        if isinstance(value, bool):
            return _CONFORMS
        scalar = _numpy_scalar(value)
        if scalar is not None and scalar[0] == "b":
            if isinstance(declared, type) and isinstance(value, declared):
                return _CONFORMS
            return self.near(NearMiss.NUMPY_SCALAR_SAME_KIND, value, declared)
        return _mismatch(value, declared)

    def check_integer(self, value: object, declared: object) -> _Result:
        """``int`` or a NumPy integer type; the range is checked by the conversion."""
        if isinstance(value, bool):
            return _mismatch(value, declared)
        if isinstance(value, int) or (
            isinstance(declared, type) and isinstance(value, declared)
        ):
            return _CONFORMS  # an int subclass such as IntEnum keeps its value
        scalar = _numpy_scalar(value)
        if scalar is not None and scalar[0] in ("i", "u"):
            return self.near(NearMiss.NUMPY_SCALAR_SAME_KIND, value, declared)
        if isinstance(value, float) or (scalar is not None and scalar[0] == "f"):
            return _mismatch(value, declared, _FLOAT_FOR_INT_HINT)
        from decimal import Decimal
        from fractions import Fraction

        if isinstance(value, (Decimal, Fraction)):
            return _mismatch(value, declared, _FLOAT_FOR_INT_HINT)
        return _mismatch(value, declared)

    def _check_not_float(self, value: object, declared: object) -> _Result:
        """Shared tail of the float checks: a bool, an integer or anything else."""
        scalar = _numpy_scalar(value)
        if isinstance(value, bool) or (scalar is not None and scalar[0] == "b"):
            return self.near(NearMiss.BOOL_FOR_FLOAT, value, declared)
        if isinstance(value, int) or (scalar is not None and scalar[0] in ("i", "u")):
            return self.near(NearMiss.INT_FOR_FLOAT, value, declared)
        return _mismatch(value, declared)

    def check_float(self, value: object, declared: object) -> _Result:
        """``float`` or ``numpy.float64``."""
        if isinstance(value, float):  # numpy.float64 is a float subclass
            return _CONFORMS
        scalar = _numpy_scalar(value)
        if scalar is not None and scalar[0] == "f":
            if scalar[1] > 8:  # numpy.longdouble where it is 80 or 128 bits
                return self.near(NearMiss.LONGDOUBLE_FOR_FLOAT, value, declared)
            # A NumPy float of at most 64 bits widens exactly.
            return self.near(NearMiss.NUMPY_SCALAR_SAME_KIND, value, declared)
        return self._check_not_float(value, declared)

    def check_float32(self, value: object, declared: object) -> _Result:
        """``numpy.float32``."""
        scalar = _numpy_scalar(value)
        if scalar == ("f", 4):
            return _CONFORMS
        if scalar is not None and scalar[0] == "f" and scalar[1] < 4:
            return self.near(NearMiss.NUMPY_SCALAR_SAME_KIND, value, declared)
        if isinstance(value, float) or (scalar is not None and scalar[0] == "f"):
            return self.near(NearMiss.FLOAT_FOR_FLOAT32, value, declared)
        return self._check_not_float(value, declared)

    def check_list(
        self, value: object, declared: object, element_type: object
    ) -> _Result:
        """``list[T]``: a list, or a near miss the table treats (tuple, set, ordered container).

        A one-shot iterator is always rejected, because checking it would consume it. A
        NumPy array is checked by one element, since all its elements share its dtype.

        Args:
            value: The returned value.
            declared: The declared ``list[T]``.
            element_type: ``T``.

        Returns:
            The result, with element paths relative to ``value``.
        """
        item: object = value  # the isinstance checks below must not narrow this one
        if isinstance(value, list):
            return self.check_elements(typing.cast("list[object]", value), element_type)
        if isinstance(value, (str, bytes, Mapping)):
            return _mismatch(item, declared)
        if isinstance(value, Set) and not isinstance(value, (KeysView, ItemsView)):
            near_miss = NearMiss.SET_FOR_LIST  # unordered: dict views keep their order
        elif isinstance(value, tuple):
            near_miss = NearMiss.TUPLE_FOR_LIST
        elif isinstance(value, Iterator):
            return _mismatch(item, declared, _ITERATOR_HINT)
        elif isinstance(value, Iterable):
            near_miss = NearMiss.ITERABLE_FOR_LIST
        else:
            return _mismatch(item, declared)
        container = self.near(near_miss, item, declared)
        if container[0] is not None:
            return container
        array = _numpy_array(item)
        if array is not None and array.ndim == 1 and array.dtype.kind != "O":
            # Every element has the array's dtype: checking one checks them all.
            elements: Iterable[object] = typing.cast("list[object]", list(array[:1]))
        else:
            elements = typing.cast(
                "Iterable[object]", item
            )  # re-iterable, not consumed
        return _merge((container, self.check_elements(elements, element_type)))

    def check_elements(self, values: Iterable[object], element_type: object) -> _Result:
        # The path is only built for an element that does not simply conform.
        """Check each element against ``element_type``.

        Args:
            values: The elements, in order (iterated once).
            element_type: The declared element type.

        Returns:
            The first mismatch at ``[i]``, or the elements' conversions at ``[*]``.
        """
        return _merge(
            _at(result, f"[{i}]", "[*]")
            for i, item in enumerate(values)
            if (result := self.check(item, element_type)) is not _CONFORMS
        )

    def check_items(
        self, mapping: Mapping[object, object], key_type: object, value_type: object
    ) -> Iterator[_Result]:
        """Check each key and value of a mapping.

        Args:
            mapping: The returned mapping.
            key_type: The declared key type.
            value_type: The declared value type.

        Yields:
            The result of each key or value that does not simply conform, placed at its key.
        """
        for key, item in mapping.items():
            result = self.check(key, key_type)
            if result is not _CONFORMS:
                yield _at(result, f"[{_key_text(key)}] key", ".keys()")
            result = self.check(item, value_type)
            if result is not _CONFORMS:
                yield _at(result, f"[{_key_text(key)}]", "[*]")


def check_conformance(
    value: object,
    declared: object,
    rule: Mapping[NearMiss, Treatment] = OUTPUT_CONFORMANCE,
) -> ConformanceReport:
    """Check a value returned for one output against the output's declared type.

    Args:
        value: A value returned by a function for one output.
        declared: The output's declared type.
        rule: How each near miss is treated.

    Returns:
        The first mismatch, or, when the value conforms, the conversions it needs, each
        once. A declared type this module leaves to its own converter conforms.
    """
    mismatch, conversions = _Checker(rule).check(value, declared)
    return ConformanceReport(mismatch, conversions)
