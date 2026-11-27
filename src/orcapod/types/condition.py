"""Row conditions: ``Condition`` and ``col``, what a restriction or a filter lets through.

A ``Condition`` is a tree of row conditions on named columns (ITL-1218, ITL-1374):
equalities, membership, the comparisons, null tests and comparisons of two columns, on a
column or a field of a struct column, combined with ``&`` (all), ``|`` (any) and ``~``
(not). The top level of a condition is "all of". ``Condition.of`` builds the equality
conditions ``where()`` takes; ``col(name)`` builds the others.

A source uses a condition to choose which of its rows it reads (``where``: a restriction,
in no hash). The ``Filter`` operator takes one as a step of the pipeline: its identity is
the condition's ``canonical_form()``.

The tree is read through four node classes, ``Term``, ``AllOf``, ``AnyOf`` and
``Negation``, and a ``Term`` names its column through a ``FieldPath``. The checks of a
condition against a table's Arrow schema (``check_condition``) and against a stream's
Python schema (``check_condition_columns``) are here too, so a source's ``where`` and the
``Filter`` operator check values by one rule (``condition_value_type``).

The class is re-exported by ``orcapod.types``, where it is imported from; ``col`` by
``orcapod.types`` and ``orcapod``.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import date, datetime, timedelta, timezone
from types import MappingProxyType
from typing import Literal, NamedTuple, Never, TypeAlias, TypeVar

import pyarrow as pa
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


_T = TypeVar("_T")

# -----------------------------------------------------------------------------
# The tree
# -----------------------------------------------------------------------------

Operator: TypeAlias = Literal[
    "==", "!=", "<", "<=", ">", ">=", "is_in", "is_null", "is_not_null"
]
"""The operator of a ``Term``."""

EQUALS: Operator = "=="
"""The operator of an equality term."""

COMPARISONS: frozenset[str] = frozenset({"==", "!=", "<", "<=", ">", ">="})
"""The operators that compare a column with a value or with another column."""

ORDER_COMPARISONS: frozenset[str] = frozenset({"<", "<=", ">", ">="})
"""The comparisons that need an order on the column's type."""

NULL_TESTS: frozenset[str] = frozenset({"is_null", "is_not_null"})
"""The operators that read only whether a value is present; a ``Term`` of one has no value."""


class FieldPath(NamedTuple):
    """The column a term reads: a column of the stream, or a field of a struct column.

    ``FieldPath("info", ("kind",))`` is the child ``kind`` of the struct column ``info``;
    ``fields`` is empty for the column itself. A user class column is read through its
    stored struct, whose fields are its declared fields.

    Attributes:
        column: The name of a user tag or data column.
        fields: The struct field names from the column inward, outermost first.
    """

    column: str
    fields: tuple[str, ...] = ()

    def text(self) -> str:
        """Return the path as words: ``info`` or ``info.kind`` (display only)."""
        return ".".join((self.column, *self.fields))


class ColumnOperand(NamedTuple):
    """The value of a term that compares two columns: the other column's path.

    ``col("a") < col("b")`` is ``Term(FieldPath("a"), "<", ColumnOperand(FieldPath("b")))``.
    """

    path: FieldPath


class Term(NamedTuple):
    """One requirement: a column (or field), an operator and a value.

    The value is a condition value (``==``, ``!=`` and the order comparisons), a
    ``ColumnOperand`` (a comparison of two columns), a tuple of condition values, all of
    one type, unique, in the order first given (``is_in``), or ``None`` (the null tests).
    ``between(lo, hi)`` is the two terms ``>= lo`` and ``<= hi``.

    Compare terms with ``same_node``, never ``==``: ``0.0`` and ``-0.0`` are one value,
    and so is every NaN.
    """

    path: FieldPath
    operator: Operator
    value: object


class AllOf(NamedTuple):
    """Every child must hold (``&``; the top level of every condition)."""

    children: tuple[Node, ...]


class AnyOf(NamedTuple):
    """At least one child must hold (``|``)."""

    children: tuple[Node, ...]


class Negation(NamedTuple):
    """The child must not hold (``~``); unknown stays unknown (Kleene logic)."""

    child: Node


Node: TypeAlias = "Term | AllOf | AnyOf | Negation"
"""A node of a condition's tree."""


# -----------------------------------------------------------------------------
# Values
# -----------------------------------------------------------------------------


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
    infinite float tagged: ``{"float": "nan" | "inf" | "-inf"}``; ``-0.0`` as ``0.0``)."""
    if isinstance(value, float) and not math.isfinite(value):
        return {
            _FLOAT_KEY: "nan" if value != value else ("inf" if value > 0 else "-inf")
        }
    if isinstance(value, float) and value == 0.0:
        # -0.0 is 0.0 (the float rule), so two equal conditions save one form.
        return 0.0
    if isinstance(value, datetime):
        return {_DATETIME_KEY: value.isoformat()}
    if isinstance(value, date):
        return {_DATE_KEY: value.isoformat()}
    if isinstance(value, bytes):
        return {_BYTES_KEY: value.hex()}
    return value


_ISO_OFFSET = re.compile(r"([+-])(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?$")
"""The UTC offset at the end of an ISO datetime text, seconds and microseconds included."""


def _datetime_from_iso(text: str) -> datetime:
    """Return the datetime ``isoformat()`` wrote, with its offset exactly.

    ``datetime.fromisoformat`` reads an offset of zero whole seconds and a fraction
    (``+00:00:00.000001``) as zero, which would move the instant of a value kept under
    its own offset (``_frozen``'s fallback); the offset is read from the text instead.
    """
    value = datetime.fromisoformat(text)
    match = _ISO_OFFSET.search(text)
    if value.tzinfo is None or match is None:
        return value
    sign, hours, minutes, seconds, fraction = match.groups()
    offset = timedelta(
        hours=int(hours),
        minutes=int(minutes),
        seconds=int(seconds or 0),
        microseconds=int((fraction or "0").ljust(6, "0")),
    )
    return value.replace(tzinfo=timezone(-offset if sign == "-" else offset))


def _condition_value_from_config(saved: object) -> object:
    """Return the condition value that ``_condition_value_to_config`` wrote."""
    if isinstance(saved, dict) and len(saved) == 1:
        ((key, text),) = saved.items()
        if key == _DATETIME_KEY:
            return _datetime_from_iso(text)
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
            f"a column name is a string, got {column!r} ({type(column).__name__})"
        )
    if not column:
        # As col("") refuses it, so a saved condition always reads back.
        raise ValueError("a column name is not empty")
    if value is None:
        raise ValueError(
            f"column {column!r}: None is refused: a null equals nothing, so the "
            f"condition would match no row; to keep the null rows write "
            f"col({column!r}).is_null()"
        )
    if type(value) not in _CONDITION_VALUE_TYPES:
        membership = (
            f"; for membership write col({column!r}).is_in([...])"
            if isinstance(value, (list, tuple, set, frozenset))
            else ""
        )
        raise TypeError(
            f"column {column!r}: the value {value!r} is a "
            f"{type(value).__name__}, and a condition value is exactly a bool, "
            "int, float, str, bytes, datetime.datetime or datetime.date; "
            "convert it explicitly (a NumPy scalar with .item()): Orcapod "
            f"converts no value{membership}"
        )


_UTC_ORIGIN = datetime(1, 1, 1, tzinfo=timezone.utc)
"""0001-01-01 UTC, the origin of ``instant_microseconds``."""

_MAX_WALL_MICROSECONDS = (datetime.max - datetime.min) // timedelta(microseconds=1)
"""The wall time ``datetime.max``, in microseconds since 0001-01-01."""


def _frozen(value: object) -> object:
    """Return a condition value as a condition holds it: nothing in it calls back later.

    A ``datetime`` is frozen when the condition is built (ITL-1374): an aware value
    becomes its instant, in UTC (``timezone.utc``, immutable), so its own ``tzinfo`` is
    read once, here, and never again: not when the condition is compared, hashed,
    checked against a column, saved or applied. Its zone and the spelling of its offset
    are gone; its instant is kept exactly, a fraction of a second in the offset
    included, and ``fold`` is honoured. A naive value keeps its wall-time fields, without
    a ``tzinfo`` and with ``fold`` 0 (naive values compare by wall time, ``fold`` aside).
    Every other value is returned as it is.

    An instant that has no UTC ``datetime`` (year 1 at a positive offset, year 9999 at
    a negative one) is held at the nearest wall time (``datetime.min`` or
    ``datetime.max``) under the fixed offset that names the instant there, so its
    representation depends on the instant only.
    """
    if type(value) is not datetime:
        return value
    # The tzinfo is read once: every later step uses this one observation of the offset.
    offset = value.utcoffset()
    if offset is None:
        return datetime(
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
        )
    fixed = timezone(offset)
    instant = instant_microseconds(value.replace(tzinfo=fixed, fold=0))
    assert instant is not None  # a fixed timezone always has an offset
    try:
        return _UTC_ORIGIN + timedelta(microseconds=instant)
    except OverflowError:
        # An instant with no UTC datetime (year 1 at a positive offset, year 9999 at a
        # negative one) is held at the nearest wall time a datetime has, under the fixed
        # offset that names the instant there. Decided by the instant alone, so every
        # spelling of one instant is held, written and saved the same way.
        if instant < 0:
            wall = datetime.min
            shift = timedelta(microseconds=-instant)
        else:
            wall = datetime.max
            shift = timedelta(microseconds=_MAX_WALL_MICROSECONDS - instant)
        return wall.replace(tzinfo=timezone(shift))


def _checked_value(column: str, value: object) -> object:
    """Return ``value`` checked (``_require_condition_value``) and frozen (``_frozen``)."""
    _require_condition_value(column, value)
    return _frozen(value)


def condition_value_type(arrow_type: pa.DataType) -> type | None:
    """Return the Python type a condition value must have for a column of ``arrow_type``.

    ``bool``, ``int`` (every integer width, signed or not), ``float`` (every float
    width), ``str``, ``bytes`` (binary of every kind), ``datetime`` (timestamps) or
    ``date`` (dates). ``None`` for a column type no value condition supports (lists,
    structs, decimals, logical types and the rest); a null test supports every type.

    Args:
        arrow_type: The column's Arrow type.

    Returns:
        The exact Python type of a value for that column, or ``None``.
    """
    if pa.types.is_boolean(arrow_type):
        return bool
    if pa.types.is_integer(arrow_type):
        return int
    if pa.types.is_floating(arrow_type):
        return float
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return str
    if (
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_fixed_size_binary(arrow_type)
    ):
        return bytes
    if pa.types.is_timestamp(arrow_type):
        return datetime
    if pa.types.is_date(arrow_type):
        return date
    return None


def is_ordered_type(arrow_type: pa.DataType) -> bool:
    """Return whether an order comparison (``<``, ``<=``, ``>``, ``>=``) takes a column
    of ``arrow_type``: integer, float, string, binary, timestamp or date (not bool).

    Args:
        arrow_type: The column's Arrow type.

    Returns:
        Whether the column's values have an order a condition may compare by.
    """
    expected = condition_value_type(arrow_type)
    return expected is not None and expected is not bool


_EPOCH_MICROSECONDS = instant_microseconds(datetime(1970, 1, 1, tzinfo=timezone.utc))
_MICROSECONDS_PER_COUNT = {"s": 1_000_000, "ms": 1_000, "us": 1}


def timestamp_count(value: datetime, arrow_type: pa.TimestampType) -> int:
    """Return the count of ``arrow_type``'s unit since the epoch that is ``value``.

    Computed from the value's instant (an aware value) or wall time (a naive one) in
    integers, never through a ``datetime`` in the column's zone, so an instant with no
    Python ``datetime`` in that zone (year 1 east of UTC, year 9999 west of it) is held
    when the column's type holds it, and a fraction of a second in an offset counts.

    Args:
        value: The value; its kind (aware or naive) is already checked against the
            column's zone.
        arrow_type: The column's timestamp type.

    Raises:
        TypeError: If no count of the unit is the value exactly (a microsecond for
            ``timestamp[s]``), or the count is beyond the type's 64-bit range
            (``timestamp[ns]`` reaches years 1677 to 2262).
    """
    micros = instant_microseconds(value)
    if micros is None:
        micros = instant_microseconds(value.replace(tzinfo=timezone.utc))
    assert micros is not None and _EPOCH_MICROSECONDS is not None
    since_epoch = micros - _EPOCH_MICROSECONDS
    if arrow_type.unit == "ns":
        count, rest = since_epoch * 1_000, 0
    else:
        count, rest = divmod(since_epoch, _MICROSECONDS_PER_COUNT[arrow_type.unit])
    if rest:
        raise TypeError(f"holds {arrow_type}, which cannot hold {value!r} exactly")
    if not -(2**63) <= count < 2**63:
        raise TypeError(
            f"holds {arrow_type}, which cannot hold {value!r}: the instant is beyond "
            f"the range of a 64-bit count of {arrow_type.unit}"
        )
    return count


def check_value_for_column(value: object, arrow_type: pa.DataType) -> None:
    """Check one condition value against the Arrow type of its column; convert nothing.

    The literal rule (ITL-1374, section 2.6): the value must be exactly of
    ``condition_value_type(arrow_type)``, and the column's type must hold it exactly
    (``7`` for ``uint32``; not ``-1`` for ``uint32``, ``300`` for ``int8``, a
    microsecond for ``timestamp[s]``). A float is rounded to a narrower float column's
    precision, as the column stores any value, and refused only when finite and beyond
    its range. A zoned timestamp column takes an aware ``datetime``, any other a naive
    one.

    Args:
        value: The value.
        arrow_type: The column's Arrow type.

    Raises:
        TypeError: If the value is not of the column's type, does not fit it, or the
            column's type takes no value condition. The message names the type and
            the value, not the column: the caller names it.
    """
    expected = condition_value_type(arrow_type)
    if expected is None:
        raise TypeError(
            f"holds {arrow_type}, which a value condition does not support; only a "
            "null test (is_null, is_not_null) takes it"
        )
    if type(value) is not expected:
        raise TypeError(
            f"holds {arrow_type} ({expected.__name__}), but the value {value!r} is a "
            f"{type(value).__name__}; convert it explicitly to {expected.__name__} (a "
            "NumPy scalar with .item(), a date or time string with date.fromisoformat "
            "or datetime.fromisoformat): Orcapod converts no value"
        )
    if isinstance(value, datetime):
        zoned = getattr(arrow_type, "tz", None) is not None
        if zoned != (value.utcoffset() is not None):
            raise TypeError(
                f"holds {arrow_type}, so the value must be "
                f"{'an aware' if zoned else 'a naive'} datetime; got {value!r}"
            )
    if isinstance(value, datetime) and pa.types.is_timestamp(arrow_type):
        assert isinstance(arrow_type, pa.TimestampType)
        timestamp_count(value, arrow_type)
        return
    if isinstance(value, float):
        # Rounding to the column's precision is how a float column stores any
        # value, so it is not refused; a finite value beyond its range is.
        stored = pa.scalar(value, type=arrow_type).as_py()
        if math.isfinite(value) and not math.isfinite(stored):
            raise TypeError(f"holds {arrow_type}, whose range does not reach {value!r}")
        return
    try:
        exact = _same_stored_value(pa.scalar(value, type=arrow_type).as_py(), value)
    except (pa.ArrowInvalid, OverflowError) as exc:
        raise TypeError(
            f"holds {arrow_type}, which cannot hold {value!r}: {exc}"
        ) from exc
    if not exact:
        raise TypeError(f"holds {arrow_type}, which cannot hold {value!r} exactly")


def _same_stored_value(stored: object, value: object) -> bool:
    """Return whether the column's form of a condition value is the value itself.

    An aware datetime is compared by instant (its own offset, ``fold`` included):
    the column hands it back in its own zone object, and Python's ``==`` never
    equals two zones' values in a daylight-saving overlap.
    """
    if isinstance(stored, datetime) and isinstance(value, datetime):
        stored_instant = instant_microseconds(stored)
        value_instant = instant_microseconds(value)
        if stored_instant is not None and value_instant is not None:
            return stored_instant == value_instant
    return stored == value


def field_arrow_type(path: FieldPath, schema: pa.Schema) -> pa.DataType:
    """Return the Arrow type of the column or struct field ``path`` names.

    A user class column (an extension type over a struct) is read through its storage
    struct.

    Args:
        path: The column and the fields inward.
        schema: The table's columns.

    Returns:
        The field's Arrow type.

    Raises:
        ValueError: If the column is not in ``schema`` (the message lists the columns),
            a field is not in its struct (the message lists the struct's fields with
            their types), or a field is asked of a column that is not a struct.
    """
    if path.column not in schema.names:
        raise ValueError(
            f"no column {path.column!r}; the columns are {list(schema.names)}"
        )
    arrow_type = schema.field(path.column).type
    reached = path.column
    for name in path.fields:
        storage = (
            arrow_type.storage_type
            if isinstance(arrow_type, pa.ExtensionType)
            else arrow_type
        )
        if not pa.types.is_struct(storage):
            raise ValueError(
                f"{reached!r} holds {arrow_type}, which is not a struct, so it has no "
                f"field {name!r}"
            )
        index = storage.get_field_index(name)
        if index < 0:
            fields = ", ".join(
                f"{storage.field(i).name}: {storage.field(i).type}"
                for i in range(storage.num_fields)
            )
            raise ValueError(
                f"{reached!r} has no field {name!r}; its fields are {fields}"
            )
        arrow_type = storage.field(index).type
        reached = f"{reached}.{name}"
    return arrow_type


def _large(arrow_type: pa.DataType) -> pa.DataType:
    """Return a scalar Arrow type in its large variant (two columns compare by it).

    Only the scalar types a condition compares are concerned (``condition_value_type``
    is not ``None``): ``string`` and ``binary`` become ``large_string`` and
    ``large_binary``; every other type is returned unchanged.
    """
    if pa.types.is_string(arrow_type):
        return pa.large_string()
    if pa.types.is_binary(arrow_type):
        return pa.large_binary()
    return arrow_type


def check_condition(condition: Condition, schema: pa.Schema) -> None:
    """Check every term of ``condition`` against a table's Arrow schema; convert nothing.

    Each path must name a column (or field) of ``schema``; each value must pass
    ``check_value_for_column``; an order comparison needs a column type with an order
    (integer, float, string, binary, date, timestamp); equality and ``is_in`` a type
    ``condition_value_type`` supports; the two columns of a ``ColumnOperand`` term must
    have one Arrow type after large normalization. A null test takes every type.

    Args:
        condition: The condition.
        schema: The table's columns: the user tag and data columns only.

    Raises:
        ValueError: If a column or field is missing.
        TypeError: If a value or a column's type does not fit its term.
    """
    for term in condition.terms():
        arrow_type = field_arrow_type(term.path, schema)
        _check_term(term, arrow_type, schema)


def _check_term(term: Term, arrow_type: pa.DataType, schema: pa.Schema) -> None:
    """Check one term against the Arrow type of its column (``check_condition``)."""
    name = term.path.text()
    if term.operator in NULL_TESTS:
        return
    if term.operator in ORDER_COMPARISONS and not is_ordered_type(arrow_type):
        raise TypeError(
            f"column {name!r} holds {arrow_type}, which has no order a comparison "
            f"({term.operator}) may use; compare with == or is_in, or test for null"
        )
    value = term.value
    if isinstance(value, ColumnOperand):
        other_type = field_arrow_type(value.path, schema)
        if condition_value_type(arrow_type) is None:
            raise TypeError(
                f"column {name!r} holds {arrow_type}, which a comparison does not "
                "support"
            )
        if not _large(arrow_type).equals(_large(other_type)):
            raise TypeError(
                f"columns {name!r} ({arrow_type}) and {value.path.text()!r} "
                f"({other_type}) have different types; Orcapod converts no column"
            )
        return
    values = (
        value if term.operator == "is_in" and isinstance(value, tuple) else (value,)
    )
    for one in values:
        try:
            check_value_for_column(one, arrow_type)
        except TypeError as exc:
            raise TypeError(f"column {name!r} {exc}") from exc


def check_condition_columns(
    condition: Condition, python_schema: Mapping[str, object]
) -> None:
    """Check ``condition`` against a stream's Python schema, before any table exists.

    The check a graph build can make: every column a term names is in
    ``python_schema``, and a value term's value is of the column's Python type
    (``T`` or ``T | None``). Fields of a struct column, Arrow widths, ranges and time
    zones are checked by ``check_condition`` when the condition meets a table.

    Args:
        condition: The condition.
        python_schema: Column name to Python type: the user tag and data columns.

    Raises:
        ValueError: If a column is missing (the message lists the columns there are).
        TypeError: If a value is not of its column's Python type.
    """
    for term in condition.terms():
        paths = [term.path]
        if isinstance(term.value, ColumnOperand):
            paths.append(term.value.path)
        for path in paths:
            if path.column not in python_schema:
                raise ValueError(
                    f"no column {path.column!r}; the columns are {list(python_schema)}"
                )
        if term.path.fields or term.operator in NULL_TESTS:
            continue
        column_type = _without_none(python_schema[term.path.column])
        if column_type not in _CONDITION_VALUE_TYPES:
            continue  # a logical or nested type: check_condition decides
        value = term.value
        if isinstance(value, ColumnOperand):
            continue
        values = (
            value if term.operator == "is_in" and isinstance(value, tuple) else (value,)
        )
        for one in values:
            if type(one) is not column_type:
                raise TypeError(
                    f"column {term.path.column!r} holds {getattr(column_type, '__name__', column_type)}, "
                    f"but the value {one!r} is a {type(one).__name__}; convert it "
                    "explicitly: Orcapod converts no value"
                )


def _without_none(python_type: object) -> object:
    """Return ``python_type`` without ``None`` when it is ``T | None``; else unchanged."""
    import types
    import typing

    if typing.get_origin(python_type) not in (typing.Union, types.UnionType):
        return python_type
    members = tuple(m for m in typing.get_args(python_type) if m is not type(None))
    return members[0] if len(members) == 1 else python_type


def check_equality_conditions(
    conditions: Mapping[str, object], schema: pa.Schema
) -> dict[str, object]:
    """Check ``where`` equalities against a table's columns; convert nothing.

    ``check_value_for_column`` for each, after the column is found.

    Args:
        conditions: Column name to value.
        schema: The table's columns (system columns removed).

    Returns:
        The conditions, in the given order.

    Raises:
        ValueError: If a column is not in the table.
        TypeError: If a value is not of its column's type, does not fit it, or the
            column's type has no equality condition.
    """
    unknown = [c for c in conditions if c not in schema.names]
    if unknown:
        raise ValueError(
            f"where names columns that the table does not have: {unknown}. "
            f"Columns: {schema.names}"
        )
    for column, value in conditions.items():
        arrow_type = schema.field(column).type
        try:
            check_value_for_column(value, arrow_type)
        except TypeError as exc:
            raise TypeError(f"column {column!r} {exc}") from exc
    return dict(conditions)


def is_range_type(arrow_type: pa.DataType) -> bool:
    """Return whether a source's restriction takes a range of a column of ``arrow_type``.

    Integers of every width, timestamps of every unit and zone, and dates: the table
    system orders their values as Orcapod does. Float ranges are refused for now
    (ITL-1477) because SpiralDB uses IEEE 754 total order. Strings and binary follow
    the table system's collation. Use ``filter`` for these ranges.

    Args:
        arrow_type: The column's Arrow type.

    Returns:
        Whether a range of the column can be a restriction.
    """
    return (
        pa.types.is_integer(arrow_type)
        or pa.types.is_timestamp(arrow_type)
        or pa.types.is_date(arrow_type)
    )


_RESTRICTION_FORMS = (
    "where takes equalities (column=value) and ranges (col(c) >= a, col(c) < b, "
    "col(c).between(a, b)), all of which must hold"
)
_TO_FILTER = (
    "Filter the stream instead, as a step of the pipeline: source.filter(...) with the "
    "same condition"
)


def check_restriction(condition: Condition, schema: pa.Schema) -> None:
    """Check a source's restriction against its table's columns; convert nothing.

    A restriction is applied by the table system, so it takes only what the table
    system tests as Orcapod would: a column equal to a value, and a range (``<``,
    ``<=``, ``>``, ``>=``; ``between`` is two of them) of an integer, timestamp or date
    column (``is_range_type``). All must hold. A column equals one value; ranges add
    up and may select no row. Every value passes ``check_value_for_column``: exactly
    of its column's Python type, held exactly by its column's Arrow type.

    Args:
        condition: The condition.
        schema: The table's columns (system columns removed).

    Raises:
        ValueError: If a column is not in the table, or must equal two values.
        TypeError: If the condition holds anything else, a range names a column of
            another type, a value is not of its column's type or does not fit it,
            or a column is to equal ``-0.0`` or a NaN (``check_float_equalities``).
    """
    terms: list[Term] = []
    for node in condition.root.children:
        if not (
            isinstance(node, Term)
            and not node.path.fields
            and not isinstance(node.value, ColumnOperand)
            and (node.operator == EQUALS or node.operator in ORDER_COMPARISONS)
        ):
            raise TypeError(
                f"{_RESTRICTION_FORMS}; {condition} has others. {_TO_FILTER}"
            )
        terms.append(node)
    unknown = list(
        dict.fromkeys(t.path.column for t in terms if t.path.column not in schema.names)
    )
    if unknown:
        raise ValueError(
            f"where names columns that the table does not have: {unknown}. "
            f"Columns: {schema.names}"
        )
    equal_to: dict[str, object] = {}
    for term in terms:
        column, value = term.path.column, term.value
        arrow_type = schema.field(column).type
        if term.operator != EQUALS and not is_range_type(arrow_type):
            raise TypeError(
                f"column {column!r} holds {arrow_type}; where takes a range of an "
                f"integer, timestamp or date column only. {_TO_FILTER}"
            )
        try:
            check_value_for_column(value, arrow_type)
        except TypeError as exc:
            raise TypeError(f"column {column!r} {exc}") from exc
        if term.operator != EQUALS:
            continue
        check_float_equalities({column: value})
        held = equal_to.setdefault(column, value)
        if not _same_condition_value(held, value):
            raise ValueError(
                f"column {column!r} is already restricted to {held!r}; it cannot "
                f"also equal {value!r} (a source reads one value per column; filter "
                "the stream to state a condition that holds for no row)"
            )


def _matched_by_bits(value: object) -> bool:
    """Return whether ``value`` is a float the table system and Orcapod match apart."""
    return isinstance(value, float) and (
        math.isnan(value) or (value == 0.0 and math.copysign(1.0, value) < 0)
    )


def check_float_equalities(
    *conditions: Mapping[str, object] | Condition | None, **columns: object
) -> None:
    """For now, refuse equality to ``-0.0`` or NaN before merging (ITL-1480).

    Orcapod counts ``-0.0`` equal to ``0.0`` and NaN equal to NaN, so ``Condition.of``
    and ``combined`` keep one of two such values; SpiralDB matches a float by its bits
    (``-0.0`` only ``-0.0``, a NaN only its own bit pattern). The rows read would then
    depend on which of two equal values was kept, so neither is taken. Checked on the
    arguments as given (each mapping, each condition's top-level equalities, the
    keywords), since merging may already have dropped one; every other float
    equality is handled as before. Float ranges are refused for now by
    ``check_restriction`` (ITL-1477).

    Args:
        *conditions: The mappings and conditions given to ``where``.
        **columns: The keywords given to ``where``.

    Raises:
        TypeError: If a column is to equal ``-0.0`` or a NaN.
    """
    given: list[tuple[object, object]] = list(columns.items())
    for argument in conditions:
        if isinstance(argument, Condition):
            given.extend(
                (node.path.column, node.value)
                for node in argument.root.children
                if isinstance(node, Term) and node.operator == EQUALS
            )
        elif isinstance(argument, Mapping):
            given.extend(argument.items())
    for column, value in given:
        if _matched_by_bits(value):
            raise TypeError(
                f"column {column!r}: where takes no equality to {value!r}. SpiralDB "
                "matches -0.0 and NaN by their bits, while Orcapod counts -0.0 equal "
                "to 0.0 and NaN equal to NaN, so which rows are read would depend on "
                f"which of two equal values was kept (ITL-1480). {_TO_FILTER}"
            )


# -----------------------------------------------------------------------------
# Building conditions: col(name)
# -----------------------------------------------------------------------------


_TRUTH_VALUE_GUIDANCE = (
    "Pass separate arguments to filter() or where() (equality keywords in where()), "
    "or combine conditions with & (and), | (or), and ~ (not), with each comparison "
    "in parentheses. For a range, use .between(a, b) (both ends included)."
)


class ColumnRef:
    """A column, or a field of a struct column, to build conditions on: ``col(name)``.

    Its operators build a ``Condition``: ``col("t") >= 1.0``, ``col("unit").is_in([3, 7])``,
    ``col("t").between(0.5, 2.0)``, ``col("rate").is_null()``,
    ``col("info").field("kind") != "noise"``, ``col("a") < col("b")``. A value is checked
    when the condition is built (exactly a ``bool``, ``int``, ``float``, ``str``,
    ``bytes``, ``datetime.datetime`` or ``datetime.date``, never ``None``); whether it
    fits its column is checked when the condition meets a stream.

    A ``ColumnRef`` is unhashable because ``==`` and ``!=`` build conditions. Both
    ``ColumnRef`` and ``Condition`` raise ``TypeError`` in Python truth tests. To
    require both comparisons, write ``(col("t") > 1) & (col("t") < 2)``.
    """

    __slots__: tuple[str, ...] = ("_path",)
    _path: FieldPath  # pyright: ignore[reportUninitializedInstanceVariable]  # set with object.__setattr__: the class refuses setattr

    def __init__(self, column: str, fields: tuple[str, ...] = ()) -> None:
        """Build the reference to ``column`` (and ``fields`` inward).

        Args:
            column: The column's name.
            fields: Struct field names, outermost first.

        Raises:
            TypeError: If a name is not a string.
            ValueError: If a name is empty.
        """
        for name in (column, *fields):
            if not isinstance(name, str):
                raise TypeError(
                    f"a column or field name is a string, got {name!r} "
                    f"({type(name).__name__})"
                )
            if not name:
                raise ValueError("a column or field name is not empty")
        object.__setattr__(self, "_path", FieldPath(column, tuple(fields)))

    @property
    def path(self) -> FieldPath:
        """The column and the fields this reference names."""
        return self._path

    def field(self, name: str) -> ColumnRef:
        """Return the reference to the field ``name`` of this struct column (or field).

        Args:
            name: The struct field's name.

        Returns:
            The reference one level inward.
        """
        return ColumnRef(self._path.column, (*self._path.fields, name))

    def _term(self, operator: Operator, value: object) -> Condition:
        """Return the condition of one term on this reference, its value checked."""
        if isinstance(value, ColumnRef):
            operand: object = ColumnOperand(value.path)
        else:
            operand = _checked_value(self._path.text(), value)
        return Condition._of_children((Term(self._path, operator, operand),))  # pyright: ignore[reportPrivateUsage]  # one module

    def is_in(self, values: Iterable[object]) -> Condition:
        """Return the condition that the column's value is one of ``values``.

        ``values`` is a set: order and repeats do not count. A string or bytes value is
        not taken as a collection of characters.

        Args:
            values: One or more condition values, all of one type.

        Returns:
            The membership condition.

        Raises:
            ValueError: If ``values`` is empty or holds ``None``.
            TypeError: If ``values`` is a string or bytes, or a value is not exactly of
                a supported type, or the values are of several types.
        """
        name = self._path.text()
        if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
            raise TypeError(
                f"column {name!r}: is_in takes a collection of values, such as "
                f"[3, 7], not {values!r}"
            )
        unique: list[object] = []
        seen: set[object] = set()  # hash keys consistent with _same_condition_value
        for given in values:
            value = _checked_value(name, given)
            if unique and type(value) is not type(unique[0]):
                raise TypeError(
                    f"column {name!r}: is_in values are of one type; got "
                    f"{unique[0]!r} ({type(unique[0]).__name__}) and {value!r} "
                    f"({type(value).__name__})"
                )
            key = _condition_hash_key(value)
            if key not in seen:
                seen.add(key)
                # One zero whatever was given first: -0.0 and 0.0 are one member, and
                # the member kept (shown, saved) is 0.0, as the canonical form encodes it.
                unique.append(0.0 if type(value) is float and value == 0.0 else value)
        if not unique:
            raise ValueError(
                f"column {name!r}: is_in needs at least one value (no value matches "
                "no row)"
            )
        return Condition._of_children(  # pyright: ignore[reportPrivateUsage]  # one module
            (Term(self._path, "is_in", tuple(unique)),)
        )

    def between(self, low: object, high: object) -> Condition:
        """Return the condition ``low <= value <= high`` (both ends included).

        The condition is the two terms ``>= low`` and ``<= high``.

        Args:
            low: The smallest value kept.
            high: The largest value kept.

        Returns:
            The range condition.

        Raises:
            TypeError: If ``low`` and ``high`` are of different types, or not of a
                supported type.
            ValueError: If ``low`` is greater than ``high``.
        """
        name = self._path.text()
        # Frozen first, so two aware datetimes compare as instants (fold honoured),
        # never as wall times within one tzinfo.
        low, high = _checked_value(name, low), _checked_value(name, high)
        if type(low) is not type(high):
            raise TypeError(
                f"column {name!r}: between takes two values of one type; got "
                f"{low!r} ({type(low).__name__}) and {high!r} ({type(high).__name__})"
            )
        for end in (low, high):
            if isinstance(end, float) and end != end:
                raise ValueError(f"column {name!r}: a NaN is not an end of a range")
        try:
            reversed_range = bool(low > high)  # pyright: ignore[reportOperatorIssue]  # one checked type
        except TypeError as exc:
            raise TypeError(
                f"column {name!r}: between {low!r} and {high!r}: {exc}"
            ) from exc
        if reversed_range:
            raise ValueError(
                f"column {name!r}: between({low!r}, {high!r}) has its low end above "
                "its high end, so it matches no row"
            )
        return Condition._of_children(  # pyright: ignore[reportPrivateUsage]  # one module
            (Term(self._path, ">=", low), Term(self._path, "<=", high))
        )

    def is_null(self) -> Condition:
        """Return the condition that the value is null (a null struct makes its fields null)."""
        return Condition._of_children((Term(self._path, "is_null", None),))  # pyright: ignore[reportPrivateUsage]  # one module

    def is_not_null(self) -> Condition:
        """Return the condition that the value is present (it does not narrow the type)."""
        return Condition._of_children((Term(self._path, "is_not_null", None),))  # pyright: ignore[reportPrivateUsage]  # one module

    @override
    def __eq__(self, other: object) -> Condition:  # pyright: ignore[reportIncompatibleMethodOverride]  # builds a condition, as pl.col does
        return self._term("==", other)

    @override
    def __ne__(self, other: object) -> Condition:  # pyright: ignore[reportIncompatibleMethodOverride]  # builds a condition, as pl.col does
        return self._term("!=", other)

    def __lt__(self, other: object) -> Condition:
        return self._term("<", other)

    def __le__(self, other: object) -> Condition:
        return self._term("<=", other)

    def __gt__(self, other: object) -> Condition:
        return self._term(">", other)

    def __ge__(self, other: object) -> Condition:
        return self._term(">=", other)

    __hash__ = None  # pyright: ignore[reportAssignmentType]  # == builds a condition, so no hash

    def __bool__(self) -> Never:
        """Refuse a Python truth test.

        Raises:
            TypeError: Always.
        """
        raise TypeError(f"{self!r} has no truth value. {_TRUTH_VALUE_GUIDANCE}")

    @override
    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"a column reference is immutable; cannot set {name!r}")

    @override
    def __repr__(self) -> str:
        return f"col({self._path.column!r})" + "".join(
            f".field({name!r})" for name in self._path.fields
        )


def col(name: str) -> ColumnRef:
    """Return a reference to the column ``name``, to build conditions on.

    A comparison builds a ``Condition`` with no Python truth value. A chained
    comparison such as ``1 <= col("t") <= 5`` and the operators ``and``, ``or`` and
    ``not`` raise ``TypeError``. Pass separate conditions to ``filter()`` (or
    separate equality keywords to ``where()``), or combine conditions with ``&``,
    ``|`` and ``~``, putting each comparison in parentheses. For a range, use
    ``col("t").between(1, 5)``, which includes both ends.

    Example::

        units.filter(col("t") >= 1.0, col("unit").is_in([3, 7]))

    Args:
        name: A user tag or data column of the stream the condition is applied to.

    Returns:
        The reference.
    """
    return ColumnRef(name)


# -----------------------------------------------------------------------------
# The condition
# -----------------------------------------------------------------------------


def same_node(a: Node, b: Node) -> bool:
    """Return whether two nodes state one requirement.

    Values compare by ``_same_condition_value``; ``is_in`` values, and the children of
    ``AllOf`` and ``AnyOf``, as sets.
    """
    if isinstance(a, Term) and isinstance(b, Term):
        return _same_term(a, b)
    if isinstance(a, Negation) and isinstance(b, Negation):
        return same_node(a.child, b.child)
    if (isinstance(a, AllOf) and isinstance(b, AllOf)) or (
        isinstance(a, AnyOf) and isinstance(b, AnyOf)
    ):
        return _same_members(a.children, b.children, same_node)
    return False


def _same_members(
    first: tuple[_T, ...], second: tuple[_T, ...], same: Callable[[_T, _T], bool]
) -> bool:
    """Return whether two tuples hold the same members, as sets, under ``same``."""
    return all(any(same(x, y) for y in second) for x in first) and all(
        any(same(y, x) for x in first) for y in second
    )


class Condition:
    """Which rows hold: conditions on named columns, all of which must hold.

    A condition is a tree (``root``, an ``AllOf``) of terms combined with all, any and
    not. ``Condition.of`` builds the equalities ``where()`` takes; ``col(name)`` builds
    the rest; ``&``, ``|`` and ``~`` combine conditions. It knows nothing of a source, a
    limit or an identity. A source uses one to choose which of its rows are read
    (``SpiralDBTableSource.where``: equalities and ranges), a restriction in no hash;
    the ``Filter`` operator takes one as a step of the pipeline, its identity the
    condition's ``canonical_form()`` (ITL-1374).

    A condition holds what can be checked without a table: column names and values of
    a supported type (``bool``, ``int``, ``float``, ``str``, ``bytes``,
    ``datetime.datetime``, ``datetime.date``, exactly, never converted). Whether the
    columns exist and each value fits its column's type is checked by whatever applies
    it (``check_condition``).

    Two conditions are equal when they state the same requirements, in any order (the
    order of terms, of ``&`` and ``|`` operands and of ``is_in`` values never counts).
    Values of different types differ (``1``, ``True``, ``1.0``); floats compare as
    Orcapod matches them (``0.0`` equals ``-0.0``, NaN equals NaN); an aware datetime
    is its instant (two zones of one instant are one value, the two 01:30 of a
    daylight-saving overlap two). That equality decides whether two restrictions are
    one; it does not decide which rows a source returns: a source applies a condition
    with its table system's own equality (for a float column, whether a NaN selects NaN
    rows and whether ``0.0`` selects ``-0.0`` rows is the table system's; under the
    usual floating-point rule a NaN equals nothing). To select rows by such values
    inside the pipeline, filter there. A condition is immutable and hashable,
    consistently with that equality; the hash is Python's own and in-process only,
    never an Orcapod hash.

    A condition has no Python truth value. Use ``is_empty`` to check whether it
    requires anything, and ``&``, ``|`` and ``~`` to combine conditions.

    Example::

        condition = Condition.of(unit_id=106, session_index=2)
        str(condition)  # "unit_id=106, session_index=2"
        condition == Condition.of({"session_index": 2, "unit_id": 106})  # True
        late = Condition.of(session_index=2) & (col("t") >= 1.0)
    """

    __slots__: tuple[str, ...] = ("_root",)
    _root: AllOf  # pyright: ignore[reportUninitializedInstanceVariable]  # set with object.__setattr__: the class refuses setattr

    def __init__(self) -> None:
        """Build the condition with no requirement, which every row satisfies.

        A source starts from it; ``Condition.of`` and ``col`` build conditions that name
        columns.
        """
        if hasattr(self, "_root"):
            raise TypeError(
                "a Condition is immutable: __init__ runs once, on a new one"
            )
        object.__setattr__(self, "_root", AllOf(()))

    @classmethod
    def _of_children(cls, children: tuple[Node, ...]) -> Condition:
        """Return the condition requiring every node of ``children``, each checked."""
        condition = cls()
        object.__setattr__(condition, "_root", AllOf(children))
        return condition

    @classmethod
    def of(
        cls,
        /,
        *conditions: Mapping[str, object] | Condition | None,
        **columns: object,
    ) -> Condition:
        """Build a condition from conditions and column values, as ``where()`` and
        ``filter()`` take them.

        ``Condition.of(unit_id=106)``, ``Condition.of({"unit_id": 106})`` and both
        together are one condition: each named column must equal its value. A positional
        argument is a mapping (column name to value) or a condition (built beforehand,
        or with ``col``); all of them and every keyword must hold. Given one condition
        and nothing else, it returns it unchanged.

        Args:
            *conditions: Mappings of column name to value, and conditions. Positional
                only, so every column name can also be given as a keyword.
            **columns: Column name to value.

        Returns:
            The condition; it names at least one column.

        Raises:
            ValueError: If no column is given, a value is ``None`` (a null equals
                nothing, so the condition would match no row), or one column is
                named twice in this call with different values (in a mapping and as
                a keyword, or in two mappings). A contradiction between conditions
                (``col("x") == 1`` given with ``col("x") == 5``) is kept: it holds
                for no row.
            TypeError: If a column name is not a string, a value is not exactly of a
                supported type (a list given as a value: use ``col(c).is_in``), or a
                positional argument is neither a mapping nor a condition (a mask by
                position, a Polars or Arrow expression).
        """
        if (
            len(conditions) == 1
            and isinstance(conditions[0], Condition)
            and not columns
        ):
            condition = conditions[0]
        else:
            built: list[Condition] = []
            merged: dict[str, object] = {}
            for argument in conditions:
                if argument is None:
                    continue
                if isinstance(argument, Condition):
                    built.append(argument)
                elif isinstance(argument, Mapping):
                    for column, given in argument.items():
                        value = _checked_value(column, given)
                        if column in merged and not _same_condition_value(
                            merged[column], value
                        ):
                            raise ValueError(
                                f"column {column!r} is named twice in one call, as "
                                f"{merged[column]!r} and as {value!r} (in two "
                                "mappings); give it once"
                            )
                        merged[column] = value
                else:
                    raise TypeError(
                        f"a condition is a mapping of column to value, or a condition "
                        f"built with col(...); got {type(argument).__name__}. A mask "
                        "by position is not a condition (row order is not a result); "
                        "for a Polars expression use polars_filter"
                    )
            for column, given in columns.items():
                value = _checked_value(column, given)
                if column in merged and not _same_condition_value(
                    merged[column], value
                ):
                    raise ValueError(
                        f"column {column!r} is named twice in one call, as "
                        f"{merged[column]!r} in the mapping and as {value!r} as a "
                        "keyword; give it once"
                    )
                merged[column] = value
            condition = Condition()
            for part in built:
                condition = condition.combined(part)
            condition = condition.combined(cls._equal_to(merged))
        if condition.is_empty:
            raise ValueError(
                "a condition needs at least one column: column=value, a mapping "
                "{column: value, ...}, or a condition built with col(...)"
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
        return cls._of_children(
            tuple(
                Term(FieldPath(column), EQUALS, _checked_value(column, value))
                for column, value in values.items()
            )
        )

    @property
    def root(self) -> AllOf:
        """The condition's tree: every child of this ``AllOf`` must hold.

        Children are in the order given (display only; equality, hash and
        ``canonical_form`` never follow it).
        """
        return self._root

    @property
    def is_empty(self) -> bool:
        """Whether the condition requires nothing."""
        return not self._root.children

    @property
    def only_equalities(self) -> bool:
        """Whether every requirement is a column equal to a value.

        True for the empty condition. A field of a struct, or a comparison of two
        columns, is not an equality in this sense.
        """
        columns = [
            node.path.column
            for node in self._root.children
            if isinstance(node, Term) and _is_plain_equality(node)
        ]
        return len(columns) == len(self._root.children) and len(set(columns)) == len(
            columns
        )

    @property
    def equalities(self) -> Mapping[str, object]:
        """Column name to the value it must equal, read-only, in the order given.

        The top-level equalities of a plain column and a value; the other requirements
        are in ``root``.
        """
        return MappingProxyType(
            {
                node.path.column: node.value
                for node in self._root.children
                if isinstance(node, Term) and _is_plain_equality(node)
            }
        )

    @property
    def columns(self) -> frozenset[str]:
        """Every stream column a term reads (the column of a struct field path; both
        columns of a comparison of two columns)."""
        names: set[str] = set()
        for term in self.terms():
            names.add(term.path.column)
            if isinstance(term.value, ColumnOperand):
                names.add(term.value.path.column)
        return frozenset(names)

    def terms(self) -> Iterator[Term]:
        """Yield every term of the tree, depth first, in the order given."""
        return _terms_of(self._root)

    def _node(self) -> Node:
        """Return the condition as one node: its one child, or its ``AllOf``."""
        children = self._root.children
        return children[0] if len(children) == 1 else self._root

    def combined(self, other: Condition) -> Condition:
        """Return the condition requiring both this one and ``other``.

        A requirement both state is kept once. ``a & b`` is the same. A contradiction
        is kept as it is, never refused (developer, 2026-11-22): there is no general way
        to tell one (``x == 1`` with ``x > 5``, disjoint memberships, ranges, nulls), so
        ``(col("x") == 1) & (col("x") == 5)`` is a condition that holds for no row and
        a filter over it keeps zero rows.

        Args:
            other: The condition to add.

        Returns:
            The condition requiring everything both require.
        """
        children = list(self._root.children)
        # Kept nodes by their hash key (consistent with ``same_node``), so each new node
        # is compared only with the kept nodes of its key, not with every kept node.
        kept_by_key: dict[object, list[Node]] = {}
        for kept in children:
            kept_by_key.setdefault(_node_hash_key(kept), []).append(kept)
        for node in other._root.children:
            bucket = kept_by_key.setdefault(_node_hash_key(node), [])
            if any(same_node(node, kept) for kept in bucket):
                continue
            bucket.append(node)
            children.append(node)
        return Condition._of_children(tuple(children))

    def __and__(self, other: Condition) -> Condition:
        """Return ``self.combined(other)``: both must hold."""
        if not isinstance(other, Condition):
            return NotImplemented
        return self.combined(other)

    def __or__(self, other: Condition) -> Condition:
        """Return the condition that this one or ``other`` holds (Kleene: unknown or
        true is true)."""
        if not isinstance(other, Condition):
            return NotImplemented
        if self.is_empty or other.is_empty:
            raise ValueError(
                "an empty condition holds for every row, so 'or' with it holds for "
                "every row; leave it out"
            )
        options: list[Node] = []
        for node in (self._node(), other._node()):
            for option in node.children if isinstance(node, AnyOf) else (node,):
                if not any(same_node(option, kept) for kept in options):
                    options.append(option)
        if len(options) == 1:
            return self  # a | a is a: one alternative is no disjunction
        return Condition._of_children((AnyOf(tuple(options)),))

    def __invert__(self) -> Condition:
        """Return the condition that this one does not hold (unknown stays unknown, so
        a row with a null the condition reads is kept by neither)."""
        if self.is_empty:
            raise ValueError(
                "an empty condition holds for every row; 'not' of it holds for none"
            )
        return Condition._of_children((Negation(self._node()),))

    def canonical_form(self) -> tuple[object, ...]:
        """Return the condition's identity: plain tagged data, order-free, typed values.

        The form the ``Filter`` operator puts into its identity (ITL-1374, section
        3.2), hashed by the structure hasher there. Nested tuples of strings:

        - a term: ``("term", <path>, <operator>, <value>)``, no value for a null test;
        - a path: ``("col", name)``, a field ``("field", <path>, name)``;
        - a value: ``("int", "5")``, ``("float", float.hex())`` with ``-0.0`` as
          ``0.0`` and every NaN as ``"nan"``, ``("bool", "true")``, ``("str", s)``,
          ``("bytes", hex)``, ``("date", iso)``, ``("datetime_aware", <UTC instant in
          microseconds>)``, ``("datetime_naive", <ISO wall time, fold>)``; another
          column ``("ref", <path>)``; ``is_in`` values unique and sorted by their form;
        - ``("all", (...))`` and ``("any", (...))``: children flattened, unique and
          sorted by their form; ``("not", <child>)``.

        So ``1``, ``1.0`` and ``True`` differ, ``0.0`` and ``-0.0`` agree, and the order
        of keywords, arguments, operands and ``is_in`` values never counts. Equal
        conditions have equal forms.

        Returns:
            The canonical form, a tuple whose first element is ``"all"``.
        """
        return _canonical_node(self._root)

    def text(self) -> str:
        """Return the condition as words: ``a='x', b=1`` (``no condition`` if empty).

        Equalities read ``column=value``; other terms ``t >= 1.0``,
        ``unit in [3, 7]``, ``rate is null``; ``|`` and ``~`` read ``(a or b)`` and
        ``not (a)``. In the order given; display only.
        """
        if self.is_empty:
            return "no condition"
        return ", ".join(_node_text(child) for child in self._root.children)

    def canonical_text(self) -> str:
        """Return the condition as ``text()`` words, in no given order.

        The children of every ``all`` and ``any`` and the values of every ``is_in`` are
        in the order ``canonical_form`` sorts them, so ``a & b`` and ``b & a``, or
        ``is_in([3, 7])`` and ``is_in([7, 3])``, read identically. For a display that
        must not follow how the condition was written (``explain()``); ``text()``
        keeps the order given.
        """
        if self.is_empty:
            return "no condition"
        return ", ".join(
            _node_text(child, canonical=True)
            for child in _canonically_sorted(self._root.children)
        )

    def to_config(self) -> dict[str, object]:
        """Return the condition as it is saved, JSON-ready.

        In canonical order (the order ``canonical_form`` sorts the requirements), so
        equal conditions are saved alike, whatever order they were given in (ITL-998).
        Only equalities: column name to value (a date, datetime or bytes value
        written as ``{"date": iso}``, ``{"datetime": iso}`` or ``{"bytes": hex}``, a NaN
        or infinite float as ``{"float": "nan"}``, ``{"float": "inf"}`` or
        ``{"float": "-inf"}``, any other value as it is). Anything else: one key,
        ``":condition"`` (no column name starts with ``:``), holding the tree:
        ``{"all": [...]}``, ``{"any": [...]}``, ``{"not": node}``, a term
        ``{"column": c, "fields": [...], "op": o, "value": v}`` (``"values": [...]`` for
        ``is_in``, ``"other": {"column": c, "fields": [...]}`` for another column, no
        value for a null test).

        Returns:
            The saved form.
        """
        if not self.only_equalities:
            return {_TREE_KEY: _node_to_config(self._root)}
        return {
            node.path.column: _condition_value_to_config(node.value)
            for node in _canonically_sorted(self._root.children)
            if isinstance(node, Term)
        }

    @classmethod
    def from_config(cls, saved: Mapping[str, object]) -> Condition:
        """Return the condition that ``to_config`` wrote.

        Args:
            saved: The saved form.

        Returns:
            The condition; with no column, the condition every row satisfies.
        """
        tree = saved.get(_TREE_KEY)
        # A saved equality value is a scalar or a one-key tagged mapping (float, date,
        # datetime, bytes); any other mapping under the key is a tree. So an equality on
        # a column named like the key still reads as an equality.
        if isinstance(tree, Mapping) and not (
            len(tree) == 1 and set(tree) <= _TAGGED_VALUE_KEYS
        ):
            if len(saved) != 1 or set(tree) != {"all"}:
                raise ValueError(
                    f"a saved condition tree is the one key {_TREE_KEY!r} holding "
                    f"{{'all': [...]}}; got {dict(saved)!r}"
                )
            # Rebuilt through the public builders, so a saved tree meets every check a
            # built one does (two values required of one column are refused).
            return _condition_from_config(tree)
        return cls._equal_to(
            {
                column: _condition_value_from_config(value)
                for column, value in saved.items()
            }
        )

    def __bool__(self) -> Never:
        """Refuse a Python truth test.

        Raises:
            TypeError: Always, including for an empty condition.
        """
        raise TypeError(f"Condition has no truth value. {_TRUTH_VALUE_GUIDANCE}")

    @override
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Condition):
            return NotImplemented
        return same_node(self._root, other._root)

    @override
    def __hash__(self) -> int:
        return hash(_node_hash_key(self._root))

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
        if self.is_empty:
            return "Condition()"
        if self.only_equalities:
            return f"Condition.of({dict(self.equalities)!r})"
        return f"Condition.from_config({self.to_config()!r})"


_TREE_KEY = ":condition"

_TAGGED_VALUE_KEYS = frozenset({_FLOAT_KEY, _DATETIME_KEY, _DATE_KEY, _BYTES_KEY})
"""The keys of a tagged condition value in a saved config."""
"""The key of a saved condition that is not only equalities."""


def _is_plain_equality(node: Node) -> bool:
    """Return whether ``node`` is a column (not a field) equal to a value."""
    return (
        isinstance(node, Term)
        and node.operator == EQUALS
        and not node.path.fields
        and not isinstance(node.value, ColumnOperand)
    )


def _node_hash_key(node: Node) -> object:
    """Return a key for ``hash``, consistent with ``same_node``."""
    if isinstance(node, Term):
        value = node.value
        if node.operator == "is_in" and isinstance(value, tuple):
            key: object = frozenset(_condition_hash_key(v) for v in value)
        elif value is None or isinstance(value, ColumnOperand):
            key = value
        else:
            key = _condition_hash_key(value)
        return ("term", node.path, node.operator, key)
    if isinstance(node, Negation):
        return ("not", _node_hash_key(node.child))
    kind = "all" if isinstance(node, AllOf) else "any"
    return (kind, frozenset(_node_hash_key(child) for child in node.children))


def _same_term(a: Term, b: Term) -> bool:
    """Return whether two terms state one requirement (``_same_condition_value``)."""
    if a.path != b.path or a.operator != b.operator:
        return False
    if a.operator == "is_in":
        first, second = a.value, b.value
        # As sets of hash keys, consistent with _same_condition_value (typed values,
        # NaN, signed zero, aware datetimes by instant): linear, not quadratic.
        return (
            isinstance(first, tuple)
            and isinstance(second, tuple)
            and {_condition_hash_key(v) for v in first}
            == {_condition_hash_key(v) for v in second}
        )
    if isinstance(a.value, ColumnOperand) or isinstance(b.value, ColumnOperand):
        return a.value == b.value
    if a.value is None or b.value is None:
        return a.value is b.value
    return _same_condition_value(a.value, b.value)


def _terms_of(node: Node) -> Iterator[Term]:
    """Yield the terms of ``node``, depth first, in the order given."""
    if isinstance(node, Term):
        yield node
    elif isinstance(node, Negation):
        yield from _terms_of(node.child)
    else:
        for child in node.children:
            yield from _terms_of(child)


# -----------------------------------------------------------------------------
# Canonical form (the identity a Filter hashes; ITL-1374 section 3.2)
# -----------------------------------------------------------------------------


def _canonical_value(value: object) -> tuple[str, str]:
    """Return a condition value as ``(type token, text)``; see ``canonical_form``."""
    if isinstance(value, bool):
        return ("bool", "true" if value else "false")
    if isinstance(value, int):
        return ("int", str(value))
    if isinstance(value, float):
        if value != value:
            return ("float", "nan")
        return ("float", (0.0 if value == 0 else value).hex())
    if isinstance(value, str):
        return ("str", value)
    if isinstance(value, bytes):
        return ("bytes", value.hex())
    if isinstance(value, datetime):
        instant = instant_microseconds(value)
        if instant is not None:
            return ("datetime_aware", str(instant))
        return ("datetime_naive", value.replace(fold=0).isoformat())
    if isinstance(value, date):
        return ("date", value.isoformat())
    raise TypeError(
        f"no condition value is a {type(value).__name__}"
    )  # pragma: no cover


def _canonical_path(path: FieldPath) -> tuple[object, ...]:
    """Return ``("col", name)``, wrapped in ``("field", <path>, name)`` per field."""
    form: tuple[object, ...] = ("col", path.column)
    for name in path.fields:
        form = ("field", form, name)
    return form


def _sorted_unique(
    forms: Iterable[tuple[object, ...]],
) -> tuple[tuple[object, ...], ...]:
    """Return ``forms`` without repeats, sorted by their JSON text (any nesting)."""
    by_text = {json.dumps(form): form for form in forms}
    return tuple(by_text[text] for text in sorted(by_text))


def _canonical_node(node: Node) -> tuple[object, ...]:
    """Return the canonical form of ``node``; see ``Condition.canonical_form``."""
    if isinstance(node, Term):
        path = _canonical_path(node.path)
        value = node.value
        if node.operator in NULL_TESTS:
            return ("term", path, node.operator)
        if isinstance(value, ColumnOperand):
            return ("term", path, node.operator, ("ref", _canonical_path(value.path)))
        if node.operator == "is_in" and isinstance(value, tuple):
            return (
                "term",
                path,
                "is_in",
                _sorted_unique(_canonical_value(v) for v in value),
            )
        return ("term", path, node.operator, _canonical_value(value))
    if isinstance(node, Negation):
        return ("not", _canonical_node(node.child))
    kind = AllOf if isinstance(node, AllOf) else AnyOf
    flat: list[Node] = []
    pending = list(node.children)
    while pending:
        child = pending.pop(0)
        if isinstance(child, kind):
            pending[:0] = child.children
        else:
            flat.append(child)
    return (
        "all" if kind is AllOf else "any",
        _sorted_unique(_canonical_node(child) for child in flat),
    )


# -----------------------------------------------------------------------------
# Text and the saved form
# -----------------------------------------------------------------------------


def _canonically_sorted(nodes: Iterable[Node]) -> list[Node]:
    """Return ``nodes`` in the order of their canonical form."""
    return sorted(nodes, key=lambda node: json.dumps(_canonical_node(node)))


def _canonical_display(value: object) -> object:
    """Return ``value`` as ``canonical_text`` shows it: ``-0.0`` as ``0.0``, as the
    canonical form encodes it (every other value as it is)."""
    if type(value) is float and value == 0.0:
        return 0.0
    return value


def _node_text(node: Node, canonical: bool = False) -> str:
    """Return ``node`` as words (display only); ``canonical`` sorts children and
    ``is_in`` values as ``canonical_form`` does."""
    if isinstance(node, Term):
        path = node.path.text()
        value = node.value
        if node.operator in NULL_TESTS:
            return f"{path} is {'not ' if node.operator == 'is_not_null' else ''}null"
        if isinstance(value, ColumnOperand):
            return f"{path} {node.operator} {value.path.text()}"
        if node.operator == "is_in" and isinstance(value, tuple):
            if canonical:
                value = tuple(
                    _canonical_display(v)
                    for v in sorted(
                        value, key=lambda v: json.dumps(_canonical_value(v))
                    )
                )
            return f"{path} in [{', '.join(repr(v) for v in value)}]"
        if canonical:
            value = _canonical_display(value)
        if node.operator == EQUALS:
            return f"{path}={value!r}"
        return f"{path} {node.operator} {value!r}"
    if isinstance(node, Negation):
        return f"not ({_node_text(node.child, canonical)})"
    word = " and " if isinstance(node, AllOf) else " or "
    children = _canonically_sorted(node.children) if canonical else node.children
    return "(" + word.join(_node_text(child, canonical) for child in children) + ")"


def _path_to_config(path: FieldPath) -> dict[str, object]:
    return {"column": path.column, "fields": list(path.fields)}


def _node_to_config(node: Node) -> dict[str, object]:
    """Return ``node`` in its JSON form; see ``Condition.to_config``."""
    if isinstance(node, Negation):
        return {"not": _node_to_config(node.child)}
    if isinstance(node, (AllOf, AnyOf)):
        # Children in the order of their canonical form: what is saved never follows
        # the order they were given in (display keeps that order).
        children = sorted(node.children, key=lambda c: json.dumps(_canonical_node(c)))
        key = "all" if isinstance(node, AllOf) else "any"
        return {key: [_node_to_config(child) for child in children]}
    saved = {**_path_to_config(node.path), "op": node.operator}
    value = node.value
    if isinstance(value, ColumnOperand):
        saved["other"] = _path_to_config(value.path)
    elif node.operator == "is_in" and isinstance(value, tuple):
        ordered = sorted(value, key=lambda v: json.dumps(_canonical_value(v)))
        saved["values"] = [_condition_value_to_config(v) for v in ordered]
    elif node.operator not in NULL_TESTS:
        saved["value"] = _condition_value_to_config(value)
    return saved


_OPERATORS: frozenset[str] = COMPARISONS | NULL_TESTS | {"is_in"}


def _path_from_config(saved: object) -> FieldPath:
    if not isinstance(saved, Mapping):
        raise ValueError(f"a saved column path is a mapping; got {saved!r}")
    fields = saved.get("fields", [])
    if not isinstance(fields, list):
        raise ValueError(f"a saved column path's fields are a list; got {fields!r}")
    return ColumnRef(saved.get("column"), tuple(fields)).path  # pyright: ignore[reportArgumentType]  # checked by ColumnRef


def _condition_from_config(saved: object) -> Condition:
    """Return the condition of the node ``_node_to_config`` wrote, rebuilt through the
    builders (``col``, ``&``, ``|``, ``~``), so every construction check applies."""
    if not isinstance(saved, Mapping):
        raise ValueError(f"a saved condition node is a mapping; got {saved!r}")
    if set(saved) == {"not"}:
        return ~_condition_from_config(saved["not"])
    for key in ("all", "any"):
        if set(saved) == {key}:
            children = saved[key]
            if not isinstance(children, list) or not children:
                raise ValueError(
                    f"a saved {key!r} holds a non-empty list; got {children!r}"
                )
            parts = [_condition_from_config(child) for child in children]
            combined = parts[0]
            for part in parts[1:]:
                combined = combined & part if key == "all" else combined | part
            return combined
    path = _path_from_config(saved)
    ref = ColumnRef(path.column, path.fields)
    operator = saved.get("op")
    if operator not in _OPERATORS:
        raise ValueError(f"a saved condition term has an unknown operator {operator!r}")
    if operator == "is_null":
        return ref.is_null()
    if operator == "is_not_null":
        return ref.is_not_null()
    if operator == "is_in":
        values = saved.get("values")
        if not isinstance(values, list) or not values:
            raise ValueError(
                f"a saved is_in term holds a list of values; got {values!r}"
            )
        return ref.is_in([_condition_value_from_config(v) for v in values])
    if "other" in saved and "value" in saved:
        raise ValueError(
            "a saved comparison holds one operand, a 'value' or an 'other' column; got "
            f"both: {dict(saved)!r}"
        )
    if "other" in saved:
        other = _path_from_config(saved["other"])
        operand: object = ColumnRef(other.column, other.fields)
    else:
        operand = _condition_value_from_config(saved.get("value"))
    return ref._term(operator, operand)  # pyright: ignore[reportPrivateUsage, reportArgumentType]  # one module; checked against _OPERATORS
