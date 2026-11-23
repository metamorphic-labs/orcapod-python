"""The mask kernel of ``Filter`` (ITL-1378): a condition tree to an Arrow boolean mask.

One function, ``condition_mask``, evaluates a condition on an Arrow table and returns one
boolean per row: ``True`` keeps the row, ``False`` or null drops it
(``table.filter(mask, null_selection_behavior="drop")``). Every path of the operator (sync,
async) uses it, so they keep the same rows.

The rules (docs/dev/specs/itl-1374-filter.md, sections 2.3 to 2.6):

- Nulls follow Kleene logic: a comparison with a null is null (unknown), ``&`` / ``|`` /
  ``~`` are ``and_kleene`` / ``or_kleene`` / ``invert``. Null tests are never null.
- Floats follow Orcapod's matching rule, not IEEE: ``0.0`` equals ``-0.0``, NaN equals NaN,
  and NaN is greater than every number (Polars' total order, the order ``GroupBy`` uses).
- A literal must be exactly of the column's Python kind (``int`` for every integer width,
  ``float`` for every float width, ``str``, ``bytes``, ``bool``, ``date``, ``datetime``) and
  is accepted when the column's Arrow type holds it exactly; a float is rounded to the
  column's width; a timestamp literal with a part below the column's unit is refused.
- Two columns compared must have one Arrow type after large normalization.
- A struct field is read with ``pc.struct_field`` (a null struct gives a null field).

Only the mask is computed here: no value of the table is converted or rewritten. A column
may be cast for the comparison only (a view to its large variant, ``float16`` to
``float32``), which changes no row the filter keeps.
"""

from __future__ import annotations

import math
import struct
from collections.abc import Sequence
from datetime import date, datetime
from typing import TYPE_CHECKING

from orcapod.errors import InputValidationError
from orcapod.types.condition import (
    COMPARISONS,
    ORDER_COMPARISONS,
    AllOf,
    ColumnOperand,
    FieldPath,
    Negation,
    Node,
    Term,
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

# The comparison ``value <op> column`` is ``column <swapped op> value``.
_SWAPPED = {LT: GT, LE: GE, GT: LT, GE: LE, EQ: EQ, NE: NE}

_EPOCH_MICROSECONDS = (date(1970, 1, 1).toordinal() - 1) * 86_400_000_000
_MICROSECONDS_PER_UNIT = {"s": 1_000_000, "ms": 1_000, "us": 1}


def path_text(path: FieldPath) -> str:
    """Return how a path is written: ``col('info').field('kind')``."""
    return "".join(
        (f"col({path.column!r})", *(f".field({name!r})" for name in path.fields))
    )


# ---------------------------------------------------------------------------
# Kinds of Arrow types
# ---------------------------------------------------------------------------


def _python_kind(arrow_type: pa.DataType) -> type | None:
    """Return the Python kind a literal for a column of ``arrow_type`` must be.

    ``None`` for a type that takes no literal in version 1 (a logical type, a list, a
    struct, a map, a decimal, a time, a duration, ...).
    """
    if isinstance(arrow_type, pa.ExtensionType):
        return None
    if pa.types.is_boolean(arrow_type):
        return bool
    if pa.types.is_integer(arrow_type):
        return int
    if pa.types.is_floating(arrow_type):
        return float
    if (
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_string_view(arrow_type)
    ):
        return str
    if (
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_binary_view(arrow_type)
        or pa.types.is_fixed_size_binary(arrow_type)
    ):
        return bytes
    if pa.types.is_date(arrow_type):
        return date
    if pa.types.is_timestamp(arrow_type):
        return datetime
    return None


def _ordered(arrow_type: pa.DataType) -> bool:
    """Return whether version 1 compares values of ``arrow_type`` by ``<`` and ``>``."""
    kind = _python_kind(arrow_type)
    return kind is not None and kind is not bool


def _compute_form(values: pa.ChunkedArray | pa.Array) -> pa.ChunkedArray | pa.Array:
    """Return ``values`` as Arrow compute compares them: views as large, float16 as float32.

    The cast is a change of layout or an exact widening, for the mask only.
    """
    arrow_type = values.type
    if pa.types.is_string_view(arrow_type):
        return values.cast(pa.large_string())
    if pa.types.is_binary_view(arrow_type):
        return values.cast(pa.large_binary())
    if pa.types.is_float16(arrow_type):
        return values.cast(pa.float32())
    return values


# ---------------------------------------------------------------------------
# References
# ---------------------------------------------------------------------------


def _resolve(table: pa.Table, path: FieldPath) -> pa.ChunkedArray:
    """Return the values ``path`` names in ``table``.

    Raises:
        InputValidationError: If the column is missing, or a field is asked of a value
            that is not a struct, or of a struct without that field.
    """
    if path.column not in table.column_names:
        raise InputValidationError(
            f"Filter: {path_text(path)}: the stream has no column {path.column!r}"
        )
    values = table.column(path.column)
    for depth, name in enumerate(path.fields):
        parent_text = path_text(FieldPath(path.column, path.fields[:depth]))
        parent_type = values.type
        if isinstance(parent_type, pa.ExtensionType) and pa.types.is_struct(
            parent_type.storage_type
        ):
            # A user class column: its storage struct holds its declared fields.
            values = pa.chunked_array(
                [chunk.storage for chunk in values.chunks],
                type=parent_type.storage_type,
            )
            parent_type = values.type
        if not pa.types.is_struct(parent_type):
            raise InputValidationError(
                f"Filter: {path_text(path)}: {parent_text} is not a struct "
                f"(its type is {parent_type}), so it has no field {name!r}"
            )
        index = parent_type.get_field_index(name)
        if index < 0:
            fields = ", ".join(
                f"{parent_type.field(i).name}: {parent_type.field(i).type}"
                for i in range(parent_type.num_fields)
            )
            raise InputValidationError(
                f"Filter: {path_text(path)}: {parent_text} has no field "
                f"{name!r}; its fields are {fields}"
            )
        values = pc.struct_field(values, [index])
    return values


# ---------------------------------------------------------------------------
# Literals
# ---------------------------------------------------------------------------


def _literal_scalar(
    ref: FieldPath, arrow_type: pa.DataType, value: object
) -> pa.Scalar:
    """Return ``value`` as a scalar of ``arrow_type``, checked as section 2.6 says.

    Raises:
        InputValidationError: If ``value`` is not of the column's Python kind, or the
            column's Arrow type cannot hold it exactly.
    """
    kind = _python_kind(arrow_type)
    if kind is None:
        raise InputValidationError(
            f"Filter: {path_text(ref)} has type {arrow_type}, which takes no value "
            "condition (only is_null and is_not_null)"
        )
    if type(value) is not kind:
        raise InputValidationError(
            f"Filter: {path_text(ref)} has type {arrow_type}, whose values are "
            f"{kind.__name__}; the value {value!r} is a {type(value).__name__}. "
            f"Orcapod converts no value: give a {kind.__name__}"
        )

    def refuse(reason: str) -> InputValidationError:
        return InputValidationError(
            f"Filter: {path_text(ref)} has type {arrow_type}: the value {value!r} "
            f"{reason}"
        )

    if kind is int:
        assert isinstance(value, int)
        width = arrow_type.bit_width
        if pa.types.is_signed_integer(arrow_type):
            low, high = -(1 << (width - 1)), (1 << (width - 1)) - 1
        else:
            low, high = 0, (1 << width) - 1
        if not low <= value <= high:
            raise refuse(f"is outside its range [{low}, {high}]")
        return pa.scalar(value, type=arrow_type)
    if kind is float:
        assert isinstance(value, float)
        if pa.types.is_float64(arrow_type):
            return pa.scalar(value, type=pa.float64())
        if not math.isfinite(value):
            return pa.scalar(value, type=pa.float32())
        code = "f" if pa.types.is_float32(arrow_type) else "e"
        try:
            (rounded,) = struct.unpack(code, struct.pack(code, value))
        except OverflowError:
            rounded = math.inf
        if not math.isfinite(rounded):
            # A finite value the column cannot hold (struct packs some as infinity).
            raise refuse("is beyond its range")
        # float16 columns are compared as float32 (an exact widening).
        return pa.scalar(rounded, type=pa.float32())
    if kind is str:
        return pa.scalar(value, type=pa.large_string())
    if kind is bytes:
        assert isinstance(value, bytes)
        if pa.types.is_fixed_size_binary(arrow_type):
            if len(value) != arrow_type.byte_width:
                raise refuse(f"is not {arrow_type.byte_width} bytes long")
            return pa.scalar(value, type=arrow_type)
        return pa.scalar(value, type=pa.large_binary())
    if kind is bool:
        return pa.scalar(value, type=pa.bool_())
    if kind is date:
        return pa.scalar(value, type=arrow_type)
    # A timestamp.
    assert isinstance(value, datetime)
    instant = instant_microseconds(value)
    if (instant is not None) != (arrow_type.tz is not None):
        raise refuse(
            "is aware and the column is not zoned"
            if instant is not None
            else "is naive and the column is zoned: give an aware datetime"
        )
    if instant is not None:
        micros = instant - _EPOCH_MICROSECONDS
    else:
        micros = (
            (value.toordinal() - 1) * 86_400_000_000
            + value.hour * 3_600_000_000
            + value.minute * 60_000_000
            + value.second * 1_000_000
            + value.microsecond
            - _EPOCH_MICROSECONDS
        )
    unit = arrow_type.unit
    if unit == "ns":
        count = micros * 1_000
    else:
        per_unit = _MICROSECONDS_PER_UNIT[unit]
        count, rest = divmod(micros, per_unit)
        if rest:
            raise refuse(f"has a part below the column's unit ({unit})")
    return pa.scalar(count, type=arrow_type)


# ---------------------------------------------------------------------------
# Terms
# ---------------------------------------------------------------------------


def _null_where_any_null(
    result: pa.ChunkedArray | pa.Array, *operands: pa.ChunkedArray | pa.Array
) -> pa.ChunkedArray | pa.Array:
    """Return ``result`` with a null wherever an operand is null (unknown)."""
    valid = pc.is_valid(operands[0])
    for operand in operands[1:]:
        valid = pc.and_(valid, pc.is_valid(operand))
    return pc.if_else(valid, result, pa.scalar(None, type=pa.bool_()))


def _float_compare(op: str, left: object, right: object) -> object:
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
    op: str, left: pa.ChunkedArray, right: object, floats: bool
) -> pa.ChunkedArray | pa.Array:
    """Compare ``left`` with ``right`` (an array or a scalar) by ``op``, Kleene nulls."""
    if floats:
        # Nulls in the result are set below, whatever the float rule gives them.
        result = _float_compare(op, left, right)
    else:
        result = _ARROW_COMPARE[op](left, right)
    operands = [left]
    if not isinstance(right, pa.Scalar):
        operands.append(right)  # pyright: ignore[reportArgumentType]  # an array here
    return _null_where_any_null(result, *operands)  # pyright: ignore[reportArgumentType]  # compute results are arrays


def _require_op_allowed(ref: FieldPath, arrow_type: pa.DataType, op: str) -> None:
    """Refuse an ordering of an unordered type, or an equality of an untyped one."""
    if op in ORDER_COMPARISONS and not _ordered(arrow_type):
        raise InputValidationError(
            f"Filter: {path_text(ref)} has type {arrow_type}, which has no order "
            f"in a filter; {op} is refused"
        )


def _term_mask(table: pa.Table, term: Term) -> pa.ChunkedArray | pa.Array:
    """Return the mask of one term over ``table``."""
    values = _resolve(table, term.path)
    if term.operator == IS_NULL:
        return pc.is_null(values)
    if term.operator == IS_NOT_NULL:
        return pc.is_valid(values)
    arrow_type = values.type
    if term.operator == IS_IN:
        assert isinstance(term.value, tuple)
        scalars = [_literal_scalar(term.path, arrow_type, v) for v in term.value]
        computed = _compute_form(values)
        if pa.types.is_floating(arrow_type):
            result = None
            for scalar in scalars:
                equal = _float_compare(EQ, computed, scalar)
                result = equal if result is None else pc.or_(result, equal)
        else:
            set_type = scalars[0].type if scalars else computed.type
            value_set = pa.array([s.as_py() for s in scalars], type=set_type)
            if computed.type != set_type:
                computed = computed.cast(set_type)
            result = pc.is_in(computed, value_set=value_set)
        return _null_where_any_null(result, values)  # pyright: ignore[reportArgumentType]  # an array
    if term.operator not in COMPARISONS:
        raise ValueError(f"Filter: unknown operator {term.operator!r}")
    if isinstance(term.value, ColumnOperand):
        other = _resolve(table, term.value.path)
        left_type = arrow_utils.normalize_to_large_types(arrow_type, keep_null=True)
        right_type = arrow_utils.normalize_to_large_types(other.type, keep_null=True)
        if left_type != right_type:
            raise InputValidationError(
                f"Filter: {path_text(term.path)} {term.operator} {path_text(term.value.path)}: the "
                f"columns have types {arrow_type} and {other.type}; two columns "
                "compared must have one type (Orcapod converts no value)"
            )
        if _python_kind(arrow_type) is None:
            raise InputValidationError(
                f"Filter: {path_text(term.path)} has type {arrow_type}, which takes no "
                "value condition (only is_null and is_not_null)"
            )
        _require_op_allowed(term.path, arrow_type, term.operator)
        left = _compute_form(values)
        right = _compute_form(other)
        if left.type != right.type:
            left = left.cast(left_type)
            right = right.cast(right_type)
        return _compare(term.operator, left, right, pa.types.is_floating(arrow_type))
    scalar = _literal_scalar(term.path, arrow_type, term.value)
    _require_op_allowed(term.path, arrow_type, term.operator)
    computed = _compute_form(values)
    if computed.type != scalar.type:
        # A string, binary or float32-compared column against its literal's type: a
        # change of layout or an exact widening, for the mask only.
        computed = computed.cast(scalar.type)
    return _compare(term.operator, computed, scalar, pa.types.is_floating(arrow_type))


# ---------------------------------------------------------------------------
# The tree
# ---------------------------------------------------------------------------


def _node_mask(table: pa.Table, node: Node) -> pa.ChunkedArray | pa.Array:
    if isinstance(node, Term):
        return _term_mask(table, node)
    if isinstance(node, Negation):
        return pc.invert(_node_mask(table, node.child))
    if not node.children:
        # An empty AND holds everywhere; an empty OR nowhere.
        return pa.array([isinstance(node, AllOf)] * table.num_rows, type=pa.bool_())
    combine = pc.and_kleene if isinstance(node, AllOf) else pc.or_kleene
    result = _node_mask(table, node.children[0])
    for child in node.children[1:]:
        result = combine(result, _node_mask(table, child))
    return result


def condition_mask(table: pa.Table, node: Node) -> pa.ChunkedArray | pa.Array:
    """Return the mask of ``node`` over ``table``: ``True`` keeps a row, ``False`` or
    null (unknown) drops it.

    Args:
        table: The rows; the condition's columns are columns of it.
        node: The condition tree.

    Returns:
        One boolean (or null) per row of ``table``.

    Raises:
        InputValidationError: If a column is missing, a field is not there, a value does
            not fit its column, or an operator does not apply to a column's type.
    """
    return _node_mask(table, node)


def filter_table(table: pa.Table, node: Node) -> pa.Table:
    """Return the rows of ``table`` whose condition holds, every value as it is."""
    return table.filter(condition_mask(table, node), null_selection_behavior="drop")


def check_against_schema(schema: pa.Schema, node: Node) -> None:
    """Check ``node`` against an Arrow schema, as applying it would, on no row.

    Raises:
        InputValidationError: As ``condition_mask`` would.
    """
    condition_mask(schema.empty_table(), node)


__all__: Sequence[str] = ("condition_mask", "filter_table", "check_against_schema")
