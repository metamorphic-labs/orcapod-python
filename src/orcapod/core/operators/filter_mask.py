"""The mask kernel of ``Filter`` (ITL-1378): a ``Condition`` to an Arrow boolean mask.

``check_filter_condition`` checks a condition against a table's Arrow schema (the one rule,
``orcapod.types.condition.check_condition``); ``condition_mask`` then evaluates it and
returns one boolean per row: ``True`` keeps the row, ``False`` or null drops it
(``table.filter(mask, null_selection_behavior="drop")``). Every path of the operator (sync,
async) uses them, so they keep the same rows.

The rules (docs/dev/specs/itl-1374-filter.md, sections 2.3 to 2.6):

- Nulls follow Kleene logic: a comparison with a null is null (unknown), ``&`` / ``|`` /
  ``~`` are ``and_kleene`` / ``or_kleene`` / ``invert``. Null tests are never null.
- Floats follow Orcapod's matching rule, not IEEE: ``0.0`` equals ``-0.0``, NaN equals NaN,
  and NaN is greater than every number (Polars' total order, the order ``GroupBy`` uses).
- A literal is compared in its column's type: an integer at the column's width, a float
  rounded to the column's width, a timestamp as a count of the column's unit.
- A struct field is read with ``pc.struct_field`` (a null struct gives a null field); a
  user class column through its storage struct.

Only the mask is computed here: no value of the table is converted or rewritten. A column
may be cast for the comparison only (``float16`` to ``float32``, a layout to its large
variant), which changes no row the filter keeps.
"""

from __future__ import annotations

import math
import struct
from collections.abc import Sequence
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

from orcapod.errors import InputValidationError
from orcapod.types.condition import (
    AllOf,
    ColumnOperand,
    Condition,
    FieldPath,
    Negation,
    Node,
    Term,
    check_condition,
)
from orcapod.utils import arrow_utils
from orcapod.utils.datetime_utils import instant_microseconds
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")


EQ, NE, LT, LE, GT, GE = "==", "!=", "<", "<=", ">", ">="
IS_IN, IS_NULL, IS_NOT_NULL = "is_in", "is_null", "is_not_null"

# The comparison ``a <op> b`` is ``b <swapped op> a``.
_SWAPPED = {LT: GT, LE: GE, GT: LT, GE: LE, EQ: EQ, NE: NE}

_EPOCH_MICROSECONDS = (date(1970, 1, 1).toordinal() - 1) * 86_400_000_000
_MICROSECONDS_PER_UNIT = {"s": 1_000_000, "ms": 1_000, "us": 1}


def check_filter_condition(condition: Condition, schema: pa.Schema) -> None:
    """Check ``condition`` against the Arrow types of a table's user columns.

    Args:
        condition: The condition.
        schema: The user tag and data columns (no system, source-info or meta column).

    Raises:
        InputValidationError: If a column or field is missing, a value does not fit its
            column, or an operator does not apply to a column's type
            (``check_condition``'s rule).
    """
    try:
        check_condition(condition, schema)
    except (TypeError, ValueError) as error:
        raise InputValidationError(f"Filter: {error}") from error


def _compute_form(values: pa.ChunkedArray) -> pa.ChunkedArray:
    """Return ``values`` as Arrow compute compares them: ``float16`` as ``float32``.

    An exact widening, for the mask only.
    """
    if pa.types.is_float16(values.type):
        return values.cast(pa.float32())
    return values


def _resolve(table: pa.Table, path: FieldPath) -> pa.ChunkedArray:
    """Return the values ``path`` names in ``table`` (checked by ``check_condition``)."""
    values = table.column(path.column)
    for name in path.fields:
        if isinstance(values.type, pa.ExtensionType):
            # A user class column: its storage struct holds its declared fields.
            storage_type = values.type.storage_type
            values = pa.chunked_array(
                [chunk.storage for chunk in values.chunks], type=storage_type
            )
        values = pc.struct_field(values, [values.type.get_field_index(name)])
    return values


def _literal_scalar(arrow_type: pa.DataType, value: object) -> pa.Scalar:
    """Return a checked literal as a scalar compared in its column's type.

    ``check_condition`` has checked that ``value`` is of the column's Python type and
    that the column holds it (a float: within its range). A float is rounded to the
    column's width (``float16`` columns are compared as ``float32``, an exact widening);
    a timestamp is the count of the column's unit since the epoch.
    """
    if pa.types.is_floating(arrow_type):
        assert isinstance(value, float)
        if pa.types.is_float64(arrow_type):
            return pa.scalar(value, type=pa.float64())
        if pa.types.is_float16(arrow_type) and math.isfinite(value):
            (value,) = struct.unpack("e", struct.pack("e", value))
        return pa.scalar(value, type=pa.float32())
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return pa.scalar(value, type=pa.large_string())
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return pa.scalar(value, type=pa.large_binary())
    if pa.types.is_timestamp(arrow_type):
        assert isinstance(value, datetime)
        instant = instant_microseconds(value)
        if instant is None:
            # A naive value against a naive column: its wall time, read as UTC.
            instant = instant_microseconds(value.replace(tzinfo=timezone.utc))
            assert instant is not None
        micros = instant - _EPOCH_MICROSECONDS
        unit = arrow_type.unit
        count = (
            micros * 1_000 if unit == "ns" else micros // _MICROSECONDS_PER_UNIT[unit]
        )
        return pa.scalar(count, type=arrow_type)
    return pa.scalar(value, type=arrow_type)


def _null_where_any_null(
    result: pa.ChunkedArray, *operands: pa.ChunkedArray
) -> pa.ChunkedArray:
    """Return ``result`` with a null wherever an operand is null (unknown)."""
    valid = pc.is_valid(operands[0])
    for operand in operands[1:]:
        valid = pc.and_(valid, pc.is_valid(operand))
    return pc.if_else(valid, result, pa.scalar(None, type=pa.bool_()))


def _float_compare(op: str, left: object, right: object) -> pa.ChunkedArray:
    """Compare two float operands (arrays or scalars) by Orcapod's float rule.

    ``0.0`` equals ``-0.0``, NaN equals NaN, NaN is greater than every number. The
    result is not yet null where an operand is null; the caller sets those.
    """
    left_nan = pc.is_nan(left)
    right_nan = pc.is_nan(right)
    if op in (EQ, NE):
        equal = pc.or_(pc.equal(left, right), pc.and_(left_nan, right_nan))
        return equal if op == EQ else pc.invert(equal)
    if op in (GT, GE):
        return _float_compare(_SWAPPED[op], right, left)
    if op == LT:
        # left < right: numbers by IEEE, or a number below a NaN.
        return pc.or_(pc.less(left, right), pc.and_(pc.invert(left_nan), right_nan))
    # LE: numbers by IEEE, or anything (NaN included) below a NaN.
    return pc.or_(pc.less_equal(left, right), right_nan)


_ARROW_COMPARE = {
    EQ: pc.equal,
    NE: pc.not_equal,
    LT: pc.less,
    LE: pc.less_equal,
    GT: pc.greater,
    GE: pc.greater_equal,
}


def _compare(
    op: str, left: pa.ChunkedArray, right: pa.ChunkedArray | pa.Scalar, floats: bool
) -> pa.ChunkedArray:
    """Compare ``left`` with ``right`` (an array or a scalar) by ``op``, Kleene nulls."""
    if floats:
        # Nulls in the result are set below, whatever the float rule gives them.
        result = _float_compare(op, left, right)
    else:
        result = _ARROW_COMPARE[op](left, right)
    if isinstance(right, pa.Scalar):
        return _null_where_any_null(result, left)
    return _null_where_any_null(result, left, right)


def _term_mask(table: pa.Table, term: Term) -> pa.ChunkedArray:
    """Return the mask of one checked term over ``table``."""
    values = _resolve(table, term.path)
    if term.operator == IS_NULL:
        return pc.is_null(values)
    if term.operator == IS_NOT_NULL:
        return pc.is_valid(values)
    arrow_type = values.type
    floats = pa.types.is_floating(arrow_type)
    computed = _compute_form(values)
    if term.operator == IS_IN:
        assert isinstance(term.value, tuple)
        scalars = [_literal_scalar(arrow_type, v) for v in term.value]
        if floats:
            # Arrow's is_in does not equate 0.0 with -0.0: one equality per value.
            result = _float_compare(EQ, computed, scalars[0])
            for scalar in scalars[1:]:
                result = pc.or_(result, _float_compare(EQ, computed, scalar))
        else:
            set_type = scalars[0].type
            value_set = pa.array([s.as_py() for s in scalars], type=set_type)
            result = pc.is_in(computed.cast(set_type), value_set=value_set)
        return _null_where_any_null(result, values)
    if isinstance(term.value, ColumnOperand):
        other = _compute_form(_resolve(table, term.value.path))
        # One type after large normalization (checked): compared in the large layout.
        large = arrow_utils.normalize_to_large_types(computed.type, keep_null=True)
        return _compare(term.operator, computed.cast(large), other.cast(large), floats)
    scalar = _literal_scalar(arrow_type, term.value)
    return _compare(term.operator, computed.cast(scalar.type), scalar, floats)


def _node_mask(table: pa.Table, node: Node) -> pa.ChunkedArray:
    if isinstance(node, Term):
        return _term_mask(table, node)
    if isinstance(node, Negation):
        return pc.invert(_node_mask(table, node.child))
    if not node.children:
        # An empty AND holds everywhere; an empty OR nowhere.
        return pa.chunked_array(
            [pa.array([isinstance(node, AllOf)] * table.num_rows, type=pa.bool_())]
        )
    combine = pc.and_kleene if isinstance(node, AllOf) else pc.or_kleene
    result = _node_mask(table, node.children[0])
    for child in node.children[1:]:
        result = combine(result, _node_mask(table, child))
    return result


def condition_mask(table: pa.Table, condition: Condition) -> pa.ChunkedArray:
    """Return the mask of ``condition`` over ``table``: ``True`` keeps a row, ``False``
    or null (unknown) drops it.

    The condition must have passed ``check_filter_condition`` against ``table``'s user
    columns; nothing is checked here.

    Args:
        table: The rows; the condition's columns are columns of it.
        condition: The checked condition.

    Returns:
        One boolean (or null) per row of ``table``.
    """
    return _node_mask(table, condition.root)


def filter_table(table: pa.Table, condition: Condition) -> pa.Table:
    """Return the rows of ``table`` whose checked condition holds, every value as it is."""
    return table.filter(
        condition_mask(table, condition), null_selection_behavior="drop"
    )


__all__: Sequence[str] = ("check_filter_condition", "condition_mask", "filter_table")
