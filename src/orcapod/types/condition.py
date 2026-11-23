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

import math
from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import date, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, NamedTuple, TypeAlias, TypeVar

from typing_extensions import override

from orcapod.utils.datetime_utils import instant_microseconds
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


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
"""The operator of an equality term (``where`` takes only these)."""

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
    raise NotImplementedError


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
    raise NotImplementedError


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
    raise NotImplementedError


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
    raise NotImplementedError


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
    raise NotImplementedError


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
    raise NotImplementedError


# -----------------------------------------------------------------------------
# Building conditions: col(name)
# -----------------------------------------------------------------------------


class ColumnRef:
    """A column, or a field of a struct column, to build conditions on: ``col(name)``.

    Its operators build a ``Condition``: ``col("t") >= 1.0``, ``col("unit").is_in([3, 7])``,
    ``col("t").between(0.5, 2.0)``, ``col("rate").is_null()``,
    ``col("info").field("kind") != "noise"``, ``col("a") < col("b")``. A value is checked
    when the condition is built (exactly a ``bool``, ``int``, ``float``, ``str``,
    ``bytes``, ``datetime.datetime`` or ``datetime.date``, never ``None``); whether it
    fits its column is checked when the condition meets a stream.

    ``==`` and ``!=`` build conditions, so a ``ColumnRef`` is not hashable and has no
    truth value: ``col("t") > 1 and col("t") < 2`` raises ``TypeError``; write
    ``(col("t") > 1) & (col("t") < 2)``.
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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    def is_null(self) -> Condition:
        """Return the condition that the value is null (a null struct makes its fields null)."""
        raise NotImplementedError

    def is_not_null(self) -> Condition:
        """Return the condition that the value is present (it does not narrow the type)."""
        raise NotImplementedError

    @override
    def __eq__(self, other: object) -> Condition:  # pyright: ignore[reportIncompatibleMethodOverride]  # builds a condition, as pl.col does
        raise NotImplementedError

    @override
    def __ne__(self, other: object) -> Condition:  # pyright: ignore[reportIncompatibleMethodOverride]  # builds a condition, as pl.col does
        raise NotImplementedError

    def __lt__(self, other: object) -> Condition:
        raise NotImplementedError

    def __le__(self, other: object) -> Condition:
        raise NotImplementedError

    def __gt__(self, other: object) -> Condition:
        raise NotImplementedError

    def __ge__(self, other: object) -> Condition:
        raise NotImplementedError

    __hash__ = None  # pyright: ignore[reportAssignmentType]  # == builds a condition, so no hash

    def __bool__(self) -> bool:
        """Refuse a truth value: ``and``/``or`` are not conditions; use ``&`` and ``|``.

        Raises:
            TypeError: Always.
        """
        raise NotImplementedError

    @override
    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"a column reference is immutable; cannot set {name!r}")

    @override
    def __repr__(self) -> str:
        raise NotImplementedError


def col(name: str) -> ColumnRef:
    """Return a reference to the column ``name``, to build conditions on.

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
    (``SpiralDBTableSource.where``, ITL-1218: equalities only), a restriction in no hash;
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

    The truth value of a condition is whether it requires anything (``Condition()``
    is false), so ``a and b`` is not a combination: use ``a & b``.

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
                nothing, so the condition would match no row), a column is given in
                a mapping and as a keyword with different values, or a column is
                required to equal two different values.
            TypeError: If a column name is not a string, a value is not exactly of a
                supported type (a list given as a value: use ``col(c).is_in``), or a
                positional argument is neither a mapping nor a condition (a mask by
                position, a Polars or Arrow expression).
        """
        if len(conditions) == 1 and isinstance(conditions[0], Condition):
            mapping = conditions[0]
            condition = mapping.combined(cls._equal_to(columns)) if columns else mapping
        elif len(conditions) <= 1:
            only = conditions[0] if conditions else None
            if only is not None and not isinstance(only, Mapping):
                raise NotImplementedError
            merged = dict(only or {})
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
        else:
            raise NotImplementedError
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
        return cls._of_children(
            tuple(
                Term(FieldPath(column), EQUALS, value)
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
    def only_equalities(self) -> bool:
        """Whether every requirement is a column equal to a value (``where`` takes only these).

        True for the empty condition. A field of a struct, or a comparison of two
        columns, is not an equality in this sense.
        """
        return all(_is_plain_equality(node) for node in self._root.children)

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
        raise NotImplementedError

    def terms(self) -> Iterator[Term]:
        """Yield every term of the tree, depth first, in the order given."""
        raise NotImplementedError

    def combined(self, other: Condition) -> Condition:
        """Return the condition requiring both this one and ``other``.

        A requirement both state is kept once. ``a & b`` is the same.

        Args:
            other: The condition to add.

        Returns:
            The condition requiring everything both require.

        Raises:
            ValueError: If a column is required to equal two different values.
        """
        children = list(self._root.children)
        for node in other._root.children:
            if any(same_node(node, kept) for kept in children):
                continue
            if isinstance(node, Term) and _is_plain_equality(node):
                for kept in children:
                    if (
                        isinstance(kept, Term)
                        and _is_plain_equality(kept)
                        and kept.path == node.path
                    ):
                        raise ValueError(
                            f"where: column {node.path.column!r} is already restricted "
                            f"to {kept.value!r}; it cannot also equal {node.value!r}"
                        )
            children.append(node)
        return Condition._of_children(tuple(children))

    def __and__(self, other: Condition) -> Condition:
        """Return ``self.combined(other)``: both must hold."""
        raise NotImplementedError

    def __or__(self, other: Condition) -> Condition:
        """Return the condition that this one or ``other`` holds (Kleene: unknown or
        true is true)."""
        raise NotImplementedError

    def __invert__(self) -> Condition:
        """Return the condition that this one does not hold (unknown stays unknown, so
        a row with a null the condition reads is kept by neither)."""
        raise NotImplementedError

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
        raise NotImplementedError

    def text(self) -> str:
        """Return the condition as words: ``a='x', b=1`` (``no condition`` if empty).

        Equalities read ``column=value``; other terms ``t >= 1.0``,
        ``unit in [3, 7]``, ``rate is null``; ``|`` and ``~`` read ``(a or b)`` and
        ``not (a)``. In the order given; display only.
        """
        if not self._root.children:
            return "no condition"
        if self.only_equalities:
            return ", ".join(
                f"{column}={value!r}" for column, value in self.equalities.items()
            )
        raise NotImplementedError

    def to_config(self) -> dict[str, object]:
        """Return the condition as it is saved, JSON-ready.

        Only equalities: column name to value, as before (a date, datetime or bytes value
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
            raise NotImplementedError
        return {
            column: _condition_value_to_config(value)
            for column, value in self.equalities.items()
        }

    @classmethod
    def from_config(cls, saved: Mapping[str, object]) -> Condition:
        """Return the condition that ``to_config`` wrote.

        Args:
            saved: The saved form.

        Returns:
            The condition; with no column, the condition every row satisfies.
        """
        if _TREE_KEY in saved:
            raise NotImplementedError
        return cls._equal_to(
            {
                column: _condition_value_from_config(value)
                for column, value in saved.items()
            }
        )

    def __bool__(self) -> bool:
        """Return whether the condition requires anything."""
        return bool(self._root.children)

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
        if not self:
            return "Condition()"
        if self.only_equalities:
            return f"Condition.of({dict(self.equalities)!r})"
        raise NotImplementedError


_TREE_KEY = ":condition"
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
        return (
            isinstance(first, tuple)
            and isinstance(second, tuple)
            and _same_members(first, second, _same_condition_value)
        )
    if isinstance(a.value, ColumnOperand) or isinstance(b.value, ColumnOperand):
        return a.value == b.value
    if a.value is None or b.value is None:
        return a.value is b.value
    return _same_condition_value(a.value, b.value)
