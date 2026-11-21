"""Row conditions: ``Condition``, what a restriction lets through (ITL-1218).

A ``Condition`` holds row conditions only (equality terms today), is part of no hash, and
is re-exported by ``orcapod.types``, where it is imported from.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date, datetime
from types import MappingProxyType
from typing import NamedTuple

from typing_extensions import override

from orcapod.utils.datetime_utils import instant_microseconds


# How a datetime, date or bytes value of a condition is written in a saved config, and a
# NaN or infinite float, which JSON has no number for (a finite float is written as is).
_FLOAT_KEY = "float"
_DATETIME_KEY = "datetime"
_DATE_KEY = "date"
_BYTES_KEY = "bytes"

_CONDITION_VALUE_TYPES: tuple[type, ...] = (
    bool,
    int,
    float,
    str,
    bytes,
    datetime,
    date,
)
"""The exact Python types a condition value may have (no subclass, no NumPy scalar)."""


def _same_condition_value(a: object, b: object) -> bool:
    """Return whether two condition values are one value.

    Values of different types differ (``1``, ``True`` and ``1.0`` are three values).
    Floats compare as Orcapod matches them everywhere (Polars' rule): ``0.0`` equals
    ``-0.0`` and NaN equals NaN. This decides whether two restrictions are one
    restriction, not which rows a source returns (the table system's own equality
    decides that). An aware datetime is its instant: two values of one instant are one value,
    whatever their zones, and the two 01:30 of a daylight-saving overlap (``fold``)
    are two (Python's ``==`` compares two values of one ``tzinfo`` by wall time and
    ignores ``fold``). A naive datetime is never equal to an aware one.
    """
    if type(a) is not type(b):
        return False
    if isinstance(a, float) and isinstance(b, float) and a != a and b != b:
        return True
    if isinstance(a, datetime) and isinstance(b, datetime):
        a_instant, b_instant = instant_microseconds(a), instant_microseconds(b)
        if (a_instant is None) != (b_instant is None):
            return False
        if a_instant is not None:
            return a_instant == b_instant
    return a == b


def _condition_hash_key(value: object) -> tuple[type, object]:
    """Return the key ``Condition.__hash__`` uses for a value, consistent with equality."""
    if isinstance(value, float) and value != value:
        return (float, "nan")
    if isinstance(value, datetime):
        instant = instant_microseconds(value)
        if instant is not None:
            return (datetime, ("instant", instant))
        return (datetime, ("wall", value))
    return (type(value), value)


def _condition_value_to_config(value: object) -> object:
    """Return a condition value in its JSON form (date, datetime, bytes and a NaN or
    infinite float tagged: ``{"float": "nan" | "inf" | "-inf"}``)."""
    if isinstance(value, float) and not math.isfinite(value):
        return {
            _FLOAT_KEY: "nan" if value != value else ("inf" if value > 0 else "-inf")
        }
    if isinstance(value, datetime):
        return {_DATETIME_KEY: value.isoformat()}
    if isinstance(value, date):
        return {_DATE_KEY: value.isoformat()}
    if isinstance(value, bytes):
        return {_BYTES_KEY: value.hex()}
    return value


def _condition_value_from_config(saved: object) -> object:
    """Return the condition value that ``_condition_value_to_config`` wrote."""
    if isinstance(saved, dict) and len(saved) == 1:
        ((key, text),) = saved.items()
        if key == _DATETIME_KEY:
            return datetime.fromisoformat(text)
        if key == _DATE_KEY:
            return date.fromisoformat(text)
        if key == _BYTES_KEY:
            return bytes.fromhex(text)
        if key == _FLOAT_KEY and text in ("nan", "inf", "-inf"):
            return float(text)
    return saved


def _require_condition_value(column: object, value: object) -> None:
    """Refuse a column name or value no condition holds.

    Raises:
        TypeError: If ``column`` is not a string, or ``value`` is not exactly of a
            supported type.
        ValueError: If ``value`` is ``None``.
    """
    if not isinstance(column, str):
        raise TypeError(
            f"where: a column name is a string, got {column!r} "
            f"({type(column).__name__})"
        )
    if value is None:
        raise ValueError(
            f"where: column {column!r}: None is refused: a null equals "
            "nothing, so the condition would match no row"
        )
    if type(value) not in _CONDITION_VALUE_TYPES:
        raise TypeError(
            f"where: column {column!r}: the value {value!r} is a "
            f"{type(value).__name__}, and a condition value is exactly a bool, "
            "int, float, str, bytes, datetime.datetime or datetime.date; "
            "convert it explicitly (a NumPy scalar with .item()): Orcapod "
            "converts no value"
        )


class _Term(NamedTuple):
    """One requirement of a ``Condition``: a column, an operator and a value."""

    column: str
    operator: str
    value: object


_EQUALS = "=="
"""The operator of an equality term, the only one today."""


class Condition:
    """Which rows hold: conditions on named columns, all of which must hold.

    Today a condition is a set of equalities: each named column must equal its value.
    It knows nothing of a source, a limit or an identity, and enters no Orcapod hash
    today. An operator that takes a condition (a future in-pipeline filter) would put
    it into a pipeline hash, and would first need a canonical, order-free identity
    form for it, with its own label (ITL-1265).
    A source uses one to choose which of its rows are read
    (``SpiralDBTableSource.where``, ITL-1218): a restriction, not an operator.

    Build one with ``Condition.of`` (the same arguments as ``where()``). A condition
    holds what can be checked without a table: column names and values of a supported
    type (``bool``, ``int``, ``float``, ``str``, ``bytes``, ``datetime.datetime``,
    ``datetime.date``, exactly, never converted). Whether the columns exist and each
    value fits its column's type is checked by whatever applies it.

    Two conditions are equal when they require the same columns to equal the same
    values, in any order. Values of different types differ (``1``, ``True``,
    ``1.0``); floats compare as Orcapod matches them (``0.0`` equals ``-0.0``, NaN
    equals NaN); an aware datetime is its instant (two zones of one instant are one
    value, the two 01:30 of a daylight-saving overlap two). That equality decides
    whether two restrictions are one; it does not decide which rows a source
    returns: a source applies a condition with its table system's own equality (for
    a float column, whether a NaN selects NaN rows and whether ``0.0`` selects
    ``-0.0`` rows is the table system's; under the usual floating-point rule a NaN
    equals nothing). To select rows by such values inside the pipeline, filter there. A condition is immutable and hashable, consistently with that
    equality; the hash is Python's own and in-process only, never an Orcapod hash.

    Example::

        condition = Condition.of(unit_id=106, session_index=2)
        str(condition)  # "unit_id=106, session_index=2"
        condition == Condition.of({"session_index": 2, "unit_id": 106})  # True
    """

    # Internally a tuple of terms (column, operator, value) in the order given, so
    # comparisons, membership and null checks can be added as new operators without
    # changing ``of``, ``combined``, equality, the hash or the saved form of an
    # equality-only condition.
    __slots__: tuple[str, ...] = ("_terms",)
    _terms: tuple[_Term, ...]  # pyright: ignore[reportUninitializedInstanceVariable]  # set with object.__setattr__: the class refuses setattr

    def __init__(self) -> None:
        """Build the condition with no requirement, which every row satisfies.

        A source starts from it; ``Condition.of`` builds a condition that names
        columns.
        """
        if hasattr(self, "_terms"):
            raise TypeError(
                "a Condition is immutable: __init__ runs once, on a new one"
            )
        object.__setattr__(self, "_terms", ())

    @classmethod
    def _of_terms(cls, terms: tuple[_Term, ...]) -> Condition:
        """Return a condition holding ``terms``, each already checked."""
        condition = cls()
        object.__setattr__(condition, "_terms", terms)
        return condition

    @classmethod
    def of(
        cls,
        mapping: Mapping[str, object] | Condition | None = None,
        /,
        **columns: object,
    ) -> Condition:
        """Build a condition from column values, as ``where()`` takes them.

        ``Condition.of(unit_id=106)``, ``Condition.of({"unit_id": 106})`` and both
        together are one condition: each named column must equal its value. Given a
        condition, it returns it unchanged (with keywords, the condition combined with
        them), so ``source.where(c)`` accepts a condition built beforehand.

        Args:
            mapping: Column name to value, or a condition. Positional only, so every
                column name can also be given as a keyword.
            **columns: Column name to value, added to ``mapping``.

        Returns:
            The condition; it names at least one column.

        Raises:
            ValueError: If no column is given, a value is ``None`` (a null equals
                nothing, so the condition would match no row), or a column is given
                in the mapping and as a keyword with different values.
            TypeError: If a column name is not a string, or a value is not exactly of
                a supported type.
        """
        if isinstance(mapping, Condition):
            condition = mapping.combined(cls._equal_to(columns)) if columns else mapping
        else:
            merged = dict(mapping or {})
            for column, value in [*merged.items(), *columns.items()]:
                _require_condition_value(column, value)
            for column, value in columns.items():
                if column in merged and not _same_condition_value(
                    merged[column], value
                ):
                    raise ValueError(
                        f"where: column {column!r} is given as {merged[column]!r} in "
                        f"the mapping and as {value!r} as a keyword"
                    )
                merged[column] = value
            condition = cls._equal_to(merged)
        if not condition:
            raise ValueError(
                "a condition needs at least one column: where(column=value, ...) or "
                "where({column: value, ...})"
            )
        return condition

    @classmethod
    def _equal_to(cls, values: Mapping[str, object]) -> Condition:
        """Return the condition that each column of ``values`` equals its value.

        Raises:
            TypeError: If a column name is not a string, or a value is not exactly of
                a supported type.
            ValueError: If a value is ``None``.
        """
        for column, value in values.items():
            _require_condition_value(column, value)
        return cls._of_terms(
            tuple(_Term(column, _EQUALS, value) for column, value in values.items())
        )

    @property
    def equalities(self) -> Mapping[str, object]:
        """Column name to the value it must equal, read-only, in the order given."""
        return MappingProxyType(
            {
                term.column: term.value
                for term in self._terms
                if term.operator == _EQUALS
            }
        )

    def combined(self, other: Condition) -> Condition:
        """Return the condition requiring both this one and ``other``.

        A requirement both state is kept once.

        Args:
            other: The condition to add.

        Returns:
            The condition requiring everything both require.

        Raises:
            ValueError: If a column is required to equal two different values.
        """
        terms = list(self._terms)
        for term in other._terms:
            if any(_same_term(term, kept) for kept in terms):
                continue
            for kept in terms:
                if (
                    kept.operator == term.operator == _EQUALS
                    and kept.column == term.column
                ):
                    raise ValueError(
                        f"where: column {term.column!r} is already restricted to "
                        f"{kept.value!r}; it cannot also equal {term.value!r}"
                    )
            terms.append(term)
        return Condition._of_terms(tuple(terms))

    def text(self) -> str:
        """Return the condition as words: ``a='x', b=1`` (``no condition`` if empty)."""
        if not self._terms:
            return "no condition"
        return ", ".join(
            f"{term.column}={term.value!r}"
            if term.operator == _EQUALS
            else f"{term.column} {term.operator} {term.value!r}"
            for term in self._terms
        )

    def to_config(self) -> dict[str, object]:
        """Return the condition as it is saved: column name to value, JSON-ready.

        Returns:
            Column name to value: a date, datetime or bytes value is written as
            ``{"date": iso}``, ``{"datetime": iso}`` or ``{"bytes": hex}``, a NaN or
            infinite float as ``{"float": "nan"}``, ``{"float": "inf"}`` or
            ``{"float": "-inf"}`` (JSON has no such number), any other value as it is.
        """
        return {
            column: _condition_value_to_config(value)
            for column, value in self.equalities.items()
        }

    @classmethod
    def from_config(cls, saved: Mapping[str, object]) -> Condition:
        """Return the condition that ``to_config`` wrote.

        Args:
            saved: Column name to saved value.

        Returns:
            The condition; with no column, the condition every row satisfies.
        """
        return cls._equal_to(
            {
                column: _condition_value_from_config(value)
                for column, value in saved.items()
            }
        )

    def __bool__(self) -> bool:
        """Return whether the condition requires anything."""
        return bool(self._terms)

    @override
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Condition):
            return NotImplemented
        return len(self._terms) == len(other._terms) and all(
            any(_same_term(term, theirs) for theirs in other._terms)
            for term in self._terms
        )

    @override
    def __hash__(self) -> int:
        return hash(
            frozenset(
                (term.column, term.operator, _condition_hash_key(term.value))
                for term in self._terms
            )
        )

    @override
    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"a Condition is immutable; cannot set {name!r}")

    @override
    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"a Condition is immutable; cannot delete {name!r}")

    @override
    def __str__(self) -> str:
        """Return ``text()``."""
        return self.text()

    @override
    def __repr__(self) -> str:
        return f"Condition.of({dict(self.equalities)!r})" if self else "Condition()"


def _same_term(a: _Term, b: _Term) -> bool:
    """Return whether two terms state one requirement (``_same_condition_value``)."""
    return (
        a.column == b.column
        and a.operator == b.operator
        and _same_condition_value(a.value, b.value)
    )
