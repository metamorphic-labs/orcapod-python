"""GroupBy operator — many->one reduction keyed on tag values."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence, Set
from typing import Any, ClassVar, TYPE_CHECKING, cast, get_origin

from orcapod.core.operators.base import UnaryOperator
from orcapod.core.data_tags import stream_data_tags
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams.base import typed_zero_row_table
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule
from orcapod.utils.schema_utils import column_names

if TYPE_CHECKING:
    import numpy as np
    import polars as pl
    import pyarrow as pa
else:
    np = LazyModule("numpy")
    pl = LazyModule("polars")
    pa = LazyModule("pyarrow")


# GroupBy decides which rows form a group, and in what order, with Polars, and builds
# its output from the input's Arrow arrays (ITL-1250): no value passes through Python,
# so two keys are one group only when Polars finds them equal (all NaN equal, 0.0 equal
# to -0.0, ITL-1234 and ITL-1251), and every emitted value is one a member holds, bit
# for bit. Only the row numbers and the group sizes come back from Polars.


def _is_list_kind(arrow_type: "pa.DataType") -> bool:
    """Return whether a type is a list, a large list or a fixed-size list."""
    return (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    )


def _value_type(arrow_type: "pa.DataType") -> "pa.DataType":
    """Return the type a column's values are read as: a logical type's storage,
    a dictionary's values, at the top level."""
    while True:
        if isinstance(arrow_type, pa.BaseExtensionType):
            arrow_type = arrow_type.storage_type
        elif pa.types.is_dictionary(arrow_type):
            arrow_type = arrow_type.value_type
        else:
            return arrow_type


def _is_orderable_scalar(arrow_type: "pa.DataType") -> bool:
    """Return whether ``GroupBy`` has an order for a scalar of this (storage) type.

    Every integer, float, decimal, boolean, string, binary, date, time, timestamp
    and duration type, and the null type: what ``_polars_ready`` prepares.
    """
    types = pa.types
    return (
        types.is_null(arrow_type)
        or types.is_boolean(arrow_type)
        or types.is_integer(arrow_type)
        or types.is_floating(arrow_type)
        or types.is_decimal(arrow_type)
        or types.is_string(arrow_type)
        or types.is_large_string(arrow_type)
        or types.is_string_view(arrow_type)
        or types.is_binary(arrow_type)
        or types.is_large_binary(arrow_type)
        or types.is_binary_view(arrow_type)
        or types.is_fixed_size_binary(arrow_type)
        or types.is_date(arrow_type)
        or types.is_time(arrow_type)
        or types.is_timestamp(arrow_type)
        or types.is_duration(arrow_type)
    )


def _unorderable_part(arrow_type: "pa.DataType") -> "pa.DataType | None":
    """Return the part of a tag's type ``GroupBy`` has no order for, or ``None``.

    The one rule for what may key a group or order its members, used by the
    validation (from the schema) and by the grouping itself (from the table), so
    nothing reaches Polars that ``_polars_ready`` does not prepare: Polars panics on
    some types (``decimal256``), and a panic is not an error a caller can catch. A
    list, large list or fixed-size list is ordered element by element, through
    logical storage and dictionary values at every depth. A struct or a map (a user
    class, a ``dict[K, V]``) has no decided order (developer, November 19, 2026), and
    neither has any type not named in ``_is_orderable_scalar`` (an interval, a
    union, a list view).

    Args:
        arrow_type: The Arrow type of a tag column.

    Returns:
        The first type found that has no order, or ``None``.
    """
    value_type = _value_type(arrow_type)
    if _is_list_kind(value_type):
        lists = cast(
            "pa.LargeListType[Any] | pa.ListType[Any] | pa.FixedSizeListType[Any, Any]",
            value_type,
        )
        return _unorderable_part(lists.value_type)
    return None if _is_orderable_scalar(value_type) else value_type


def _without_dictionary(array: "pa.Array[Any]") -> "pa.Array[Any]":
    """Decode a dictionary-encoded column to its values; any other column as it is.

    The one place ``GroupBy`` handles a dictionary-encoded tag, a float key's
    included: the values each member's index points at, bits as stored. (A stream may
    stop carrying dictionary-encoded tags, ITL-1264; then this goes.)

    Args:
        array: A column of the input table, or its storage.

    Returns:
        The decoded array.
    """
    if isinstance(array, pa.DictionaryArray):
        return array.dictionary_decode()
    return array


def _storage(column: "pa.ChunkedArray[Any]") -> "pa.Array[Any]":
    """Return a column as one array: a logical type as its storage, decoded.

    Args:
        column: A column of the input table.

    Returns:
        The column's values as stored.
    """
    array = column.combine_chunks()
    if isinstance(array, pa.ExtensionArray):
        array = array.storage
    return _without_dictionary(array)


def _decimal256_limbs(array: "pa.Array[Any]") -> "pa.Array[Any]":
    """Return a decimal256 column as a struct of four 64-bit limbs.

    Polars has no 256-bit decimal. The limbs of the two's-complement value, most
    significant first (that one signed, the others unsigned), compare and order as
    the decimals of one column do: the scale is part of the column's type.

    Args:
        array: A ``decimal256`` array.

    Returns:
        A struct array with the limbs, null where ``array`` is null.
    """
    count = len(array)
    nulls = array.is_null().to_numpy(zero_copy_only=False)
    buffer = array.buffers()[1]
    # A pyarrow Buffer has the buffer protocol; its stub does not declare it.
    words = (
        np.frombuffer(buffer, dtype="<u8")  # pyright: ignore[reportCallIssue, reportArgumentType]
        if buffer is not None
        else np.zeros(0, dtype="<u8")
    )
    limbs = words[array.offset * 4 : (array.offset + count) * 4].reshape(count, 4)
    return pa.StructArray.from_arrays(
        [
            pa.array(limbs[:, 3].view("<i8"), pa.int64()),
            pa.array(limbs[:, 2], pa.uint64()),
            pa.array(limbs[:, 1], pa.uint64()),
            pa.array(limbs[:, 0], pa.uint64()),
        ],
        names=["l3", "l2", "l1", "l0"],
        mask=pa.array(nulls, pa.bool_()),
    )


def _polars_ready(array: "pa.Array[Any]") -> "pa.Array[Any]":
    """Return a key or ordering column in a form Polars reads, ordered and equal alike.

    Only the sort frame sees it; the output takes every value from the input. Every
    type ``_unorderable_part`` accepts is prepared here, and no other type reaches
    it (the caller refuses it first):

    * a logical type is its storage and a dictionary its values;
    * a date, time, timestamp or duration is its integer (the column's unit and zone
      are its type, so the integers order and compare as the values do), so no
      conversion of Polars' own can change it;
    * a ``float16`` becomes ``float32`` (exact; the Polars this library allows may
      lack ``Float16``), a string or binary view its large variant;
    * a ``decimal256`` becomes its limbs (Polars panics on it);
    * a list, large list or fixed-size list becomes a large list over its prepared
      values (``_sort_key`` orders a list element by element).

    Args:
        array: A column of the input table, or the values of one.

    Returns:
        The prepared array.

    Raises:
        TypeError: For a type ``_unorderable_part`` refuses; never reached through
            ``GroupBy``, which refuses the column first.
    """
    if isinstance(array, pa.ExtensionArray):
        array = array.storage
    array = _without_dictionary(array)
    arrow_type = array.type
    types = pa.types
    if types.is_large_list(arrow_type) or types.is_list(arrow_type):
        lists = cast("pa.LargeListArray[Any] | pa.ListArray[Any]", array)
        return pa.LargeListArray.from_arrays(
            cast("pa.Int64Array", lists.offsets.cast(pa.int64())),
            _polars_ready(lists.values),
            mask=array.is_null(),
        )
    if types.is_fixed_size_list(arrow_type):
        fixed = cast("pa.FixedSizeListArray[Any]", array)
        size = cast(int, arrow_type.list_size)
        values = fixed.values.slice(fixed.offset * size, len(fixed) * size)
        offsets = pa.array(np.arange(len(fixed) + 1, dtype=np.int64) * size, pa.int64())
        return pa.LargeListArray.from_arrays(
            cast("pa.Int64Array", offsets),
            _polars_ready(values),
            mask=array.is_null(),
        )
    if types.is_decimal256(arrow_type):
        return _decimal256_limbs(array)
    if types.is_float16(arrow_type):
        return array.cast(pa.float32())
    if types.is_temporal(arrow_type):
        width = cast(int, arrow_type.bit_width)
        return array.view(pa.int32() if width == 32 else pa.int64())
    if types.is_string_view(arrow_type):
        return array.cast(pa.large_string())
    if types.is_binary_view(arrow_type):
        return array.cast(pa.large_binary())
    if _is_orderable_scalar(arrow_type):
        return array
    raise TypeError(f"GroupBy has no order for {arrow_type}")


def _sort_key(name: str, dtype: object) -> "pl.Expr":
    """Return the sort key of one frame column: nulls last at every depth.

    The sort puts a null scalar last (``nulls_last``), but Polars puts a null INSIDE
    a list first, so a list is sorted through ``(is_null, value)`` pairs at every
    depth, element by element: a shorter list first when it is a prefix, a NaN after
    every number and before null, as for a scalar (ITL-1226). Polars ties ``0.0`` with
    ``-0.0`` and every NaN with every NaN.

    Args:
        name: The column's name in the sort frame.
        dtype: Its Polars type.

    Returns:
        The expression to sort by.
    """

    # ``object``: a Polars data type is an instance or a class (``PolarsDataType``,
    # which Polars keeps private).
    def null_last(expr: "pl.Expr", inner: object) -> "pl.Expr":
        if isinstance(inner, pl.List):
            value = expr.list.eval(null_last(pl.element(), inner.inner))
        else:
            value = expr
        return pl.struct(expr.is_null().alias("null"), value.alias("value"))

    if isinstance(dtype, pl.List):
        return null_last(pl.col(name), dtype)
    return pl.col(name)


def _float_bits(array: "pa.Array[Any]") -> "pa.Array[Any] | None":
    """Return a float key's bit patterns, or ``None`` when the key is not a float.

    A logical type stored as a float, or a dictionary over floats, is a float key,
    read through its storage or its values.

    Args:
        array: A key column's values as stored (``_storage``).

    Returns:
        The unsigned integers of the column's own width, or ``None``.
    """
    if not pa.types.is_floating(array.type):
        return None
    unsigned = {16: pa.uint16, 32: pa.uint32, 64: pa.uint64}[array.type.bit_width]()
    return array.view(unsigned)


def _refuse_unorderable_tags(
    by: Collection[str], tag_types: Mapping[str, "pa.DataType"]
) -> None:
    """Refuse a user tag ``GroupBy`` cannot key a group or order members by.

    A key must be a scalar value groups are ordered by, and every non-key tag orders
    the members, so a type with no order (``_unorderable_part``: a struct or a map,
    such as a user class or a ``dict``, an interval, a union) can be neither
    (developer, November 19, 2026: refuse rather than decide an order). A key that
    is a list of any kind is refused too. The refusal depends on the schema only,
    never on whether two members happen to tie.

    Args:
        by: The group keys.
        tag_types: The Arrow type of each user tag of the input.

    Raises:
        InputValidationError: Naming the first such tag and its type.
    """
    for name, arrow_type in tag_types.items():
        if name in by and _is_list_kind(_value_type(arrow_type)):
            raise InputValidationError(
                f"GroupBy: {[name]} are list-valued tag columns and cannot be "
                + f"grouping keys. Column types: {{{name!r}: {arrow_type}}}. A "
                + "list-valued tag usually means the stream came from `Batch`; "
                + "group before batching rather than after."
            )
        part = _unorderable_part(arrow_type)
        if part is None:
            continue
        if name in by:
            raise InputValidationError(
                f"GroupBy: the key {name!r} has type {arrow_type}, and GroupBy has no "
                + f"order for {part} (a struct or a map, such as a user class or a "
                + "dict, or another type without an order): a group key must be a "
                + "scalar value with an order. Group by other tags."
            )
        raise InputValidationError(
            f"GroupBy: the tag {name!r} has type {arrow_type}, and GroupBy has no "
            + f"order for {part} (a struct or a map, such as a user class or a dict, "
            + "or another type without an order): GroupBy orders a group's members "
            + "by every tag that is not a key. GroupBy drops that tag anyway: drop it "
            + "before grouping, with stream.select_tag_columns([...]) naming the "
            + "tags to keep."
        )


GROUP_BY_FOLD_LABEL = "group_v1"
"""Label of the folds of ``GroupBy``: the folded source id and record id are
``"group_v1:<hex>"`` (ITL-1154), and so is the one source-info token of a data-tag
group key (ITL-1017). It names the operation; the procedure is
``arrow_utils.fold_system_tag_values``. Change it only when this operation's fold
would give an existing member list a different value.
"""


MEMBER_ORDER_NAME = "name"
"""The keyword for the default member order: the non-key tags in name order."""


def _member_order_names(
    member_order: str | Sequence[str] | None, by: tuple[str, ...]
) -> tuple[str, ...] | None:
    """Return the tags ``member_order`` names, or ``None`` for the default order.

    Args:
        member_order: ``None`` or ``"name"`` (the default order), or a sequence of tag
            names.
        by: The group keys.

    Returns:
        The names in the order given, or ``None`` for the default order.

    Raises:
        NotImplementedError: For a function: an ordering function is planned, not
            available yet.
        TypeError: For a set (unordered), or a name that is not a string.
        ValueError: For an unknown keyword, an empty sequence, a repeated name, or
            a group key.
    """
    if member_order is None:
        return None
    if isinstance(member_order, str):
        if member_order == MEMBER_ORDER_NAME:
            return None
        raise ValueError(
            f"GroupBy: member_order {member_order!r} is not a known order; the "
            + f"keyword is {MEMBER_ORDER_NAME!r} (the default: the non-key tags in "
            + "name order); to order by a tag, give a list of tag names: "
            + f"member_order=[{member_order!r}]"
        )
    if callable(member_order):
        raise NotImplementedError(
            "GroupBy: an ordering function for member_order is planned but not "
            + "available yet (ITL-1226); give a list of tag names"
        )
    if isinstance(member_order, Set):
        raise TypeError(
            "GroupBy: give member_order as a list or a tuple, not a set: its order "
            + "is the order the members are sorted by"
        )
    names = tuple(member_order)
    if not names:
        raise ValueError(
            "GroupBy: member_order needs at least one tag name; leave it out for "
            + "the default order"
        )
    if not_strings := [name for name in names if not isinstance(name, str)]:
        raise TypeError(
            f"GroupBy: member_order names must be strings, got {not_strings!r}"
        )
    if repeated := sorted({name for name in names if names.count(name) > 1}):
        raise ValueError(f"GroupBy: member_order names {repeated} more than once")
    if keys := [name for name in names if name in by]:
        raise ValueError(
            f"GroupBy: member_order names {keys}, a group key: a key has one value "
            + "in a group, so it cannot order the members"
        )
    return names


class GroupBy(UnaryOperator):
    """Reduce rows sharing a key into one row with list-valued data members.

    This is the only many->one operator.  Every other operator preserves one
    row per tag; ``GroupBy`` collapses N rows into one, which is what lets a
    downstream pod receive a whole group at once (for example, all of a
    recording session's per-probe result parquets).

    Given tags ``(subject, date, probe)`` and data ``(path)``, grouping by
    ``["subject", "date"]`` emits one row per distinct ``(subject, date)``:

    * ``subject`` and ``date`` stay scalar tag columns, in the input's tag
      order; they are the only tag columns of the output
    * ``probe`` is **dropped** (ITL-1070): a list of the members' values is not
      a key for the group, as SQL drops a non-key, non-aggregated column after
      ``GROUP BY``.  A data-tag ``probe`` loses its tag role only: its data
      column stays and becomes list-valued like ``path``
    * ``path`` becomes list-valued
    * the ``_source_*`` column of each data column becomes list-valued, one
      element per member, except that of a data-tag group key: its value is
      the merge of the members' equal values, so its source info is one token,
      the ``group_v1`` fold of the members' tokens in member order (ITL-1017)
    * system-tag columns fold to a scalar digest and gain a
      ``::{pipeline_hash}`` name suffix

    No data element ever comes from a tag value: ``GroupBy`` uses tags to form
    groups, but the data columns of the output are exactly the data columns of
    the input.  The members' values of a dropped plain tag are not carried
    anywhere.  If a downstream step needs to know which member each element
    came from, make that value a data column before grouping: declare the tag
    a data-tag at the source (the default), declare it as data, or use
    ``DerivedSource(origin, tags_as_data={...})``.

    Members are sorted by the input's non-group-key tags in name order, each
    ascending with nulls last, then by the ``record_id`` columns and then the
    source-id columns as tiebreakers, so the emitted lists are stable across
    runs (ITL-1226).  ``member_order`` names the tags to sort by first, in its
    order; the other non-key tags follow in name order, then the same
    tiebreakers.  A member is never ordered by a data value.  The order does
    not depend on how the input's columns are arranged: the order of its tags,
    the order of its data columns and the layout of its table change no value,
    id or hash of the output (developer, November 17, 2026).  The output lists
    its columns as the input does: the keys in the input's tag order, the data
    in the input's order; that is presentation, and no hash depends on it.

    The list of a data-tag non-key is in member order, and every other data
    list is aligned with it.  This matters because orcapod hashes those lists
    to build the cache key -- an unsorted list would make an identical member
    set hash differently and trigger a spurious recompute.  Groups themselves
    are emitted sorted by their keys, in name order, for the same reason: a
    reordered input or a reordered ``by`` must
    produce an identical output.

    Rows with equal keys form one group, as the joins match keys, except that all
    rows with a null key form one group too.  So do all rows with a NaN key, and
    ``0.0`` equals ``-0.0`` (ITL-1234).  A NaN key sorts after every number and
    before null.  Keys are compared and ordered as Arrow values, with Polars, never
    as Python values (ITL-1250): ``time64[ns]`` keys of 1 ns and 2 ns are two
    groups.  Every emitted key and every list element is taken from the input's
    Arrow columns, so the lists hold the members' values bit for bit.  A tag whose
    type has no order (a struct or a map, such as a user class or a ``dict``, an
    interval) can be neither a key nor a non-key tag of the input.  A float key that the members hold with different bits is
    emitted, per key column, as the one with the smallest bit pattern at the
    column's own width (``0.0`` over ``-0.0``, a positive NaN over a negative
    one, a signaling NaN over the canonical quiet NaN), taken from the input's
    Arrow column, so the emitted key is a value a member holds, bit for bit, and
    never depends on the row order or on ``member_order``.

    Tag tuples are expected to be unique within a stream, but nothing enforces
    that.  If the input does contain duplicate tag tuples, the members sharing
    a tuple are separated only by ``record_id``, which is fixed when the source
    is materialized -- so their order is stable for a given source table, but
    undefined if the source table itself is permuted.

    A grouped stream can be grouped again on a coarser key: its data lists nest
    one level deeper (``list[list[T]]``), as source info does.  The members of
    the second grouping are ordered by the first grouping's keys that are not
    in the coarser key (scalar tags of the grouped stream, in name order or as
    ``member_order`` gives), then by ``record_id``; a tag the first grouping
    dropped no longer takes part.  A
    list-valued tag of the input can order members but cannot be a grouping
    key.

    Contrast with ``Batch``, which partitions by row count for throughput.  It
    has no key, so it keeps every tag column, list-valued: those lists are
    what tells a batch's members apart.

    Args:
        by: Tag column names to group on, in any order (a set too), or one
            name as a string (a bare string is one name, never its characters).
            Must be non-empty, free of duplicates, and must all be scalar tag
            columns of the input stream.  Their order is not kept: it is not part
            of the identity, and the groups are emitted sorted by the keys in name
            order (developer, November 17, 2026, ITL-1226).
        member_order: The order of the members of a group.  ``None`` (or
            ``"name"``): by the non-key tags in name order.  A list or tuple of
            non-key tag names (plain tags or data-tags): by those tags in the
            order given, then by the other non-key tags in name order.  Part of
            the operator's identity only when given.  An ordering function is
            planned (ITL-1226) and refused for now.

    Raises:
        ValueError: If ``by`` is empty or repeats a name, or ``member_order`` is
            an unknown keyword, empty, repeats a name or names a group key.
        TypeError: If ``by`` is not a string or an iterable of strings, or
            ``member_order`` is a set or holds a name that is not a string.
        NotImplementedError: If ``member_order`` is a function.
    """

    # Data-tags (ITL-1023): handled.
    DATA_TAGS_ISSUE: ClassVar[str | None] = None

    def __init__(
        self,
        by: str | Collection[str],
        *,
        member_order: str | Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        by_tuple = column_names(by)
        if not by_tuple:
            raise ValueError("GroupBy requires at least one column in `by`.")
        duplicates = sorted({c for c in by_tuple if by_tuple.count(c) > 1})
        if duplicates:
            raise ValueError(
                f"GroupBy `by` contains duplicate column names: {duplicates}. "
                f"Got {list(by_tuple)}."
            )
        # The order of the keys only orders the groups (rows), never a value
        # within a row: it is not identity (developer, November 17, 2026).
        self.by = tuple(sorted(by_tuple))
        self.member_order = _member_order_names(member_order, by_tuple)
        super().__init__(**kwargs)

    def identity_structure(self) -> Any:
        # ``member_order`` enters only when given, so a GroupBy without it keeps the
        # identity it had before the option existed (ITL-1226).
        identity: tuple[Any, ...] = (self.__class__.__name__, self.by)
        if self.member_order is not None:
            identity += (("member_order", self.member_order),)
        return identity

    def to_config(self) -> dict[str, Any]:
        """Serialize this GroupBy operator to a config dict.

        ``by`` is emitted as a list rather than a tuple so the config stays
        JSON-serializable; ``__init__`` normalizes it back to a tuple.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``by``, and ``member_order`` when given.
        """
        config = super().to_config()
        config["config"] = {"by": list(self.by)}
        if self.member_order is not None:
            config["config"]["member_order"] = list(self.member_order)
        return config

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """Verify every grouping column is a scalar tag column of the input.

        Args:
            stream: The upstream stream to validate.

        Raises:
            InputValidationError: If any name in ``by`` is not a tag column, or
                names a list-valued tag column; or a name in ``member_order`` is
                not a tag column; or a key is a list of any kind, or a user tag's
                type has no order (``_unorderable_part``: a struct, a map, an
                interval...); checked here when the stream knows its Arrow types
                without rows, otherwise when it is grouped.
        """
        tag_columns, data_columns = stream.keys()
        missing = [c for c in self.by if c not in tag_columns]
        if missing:
            raise InputValidationError(
                f"GroupBy: {missing} are not tag columns of the input stream. "
                f"Available tag columns: {list(tag_columns)}. "
                f"(Data columns cannot be grouping keys: {list(data_columns)})"
            )

        # A list-valued tag -- what `Batch` produces -- cannot key a group.
        tag_types, _ = stream.output_schema()
        non_scalar = {
            c: tag_types.get(c) for c in self.by if get_origin(tag_types.get(c)) is list
        }
        if non_scalar:
            raise InputValidationError(
                f"GroupBy: {list(non_scalar)} are list-valued tag columns and "
                f"cannot be grouping keys. Column types: {non_scalar}. "
                "A list-valued tag usually means the stream came from `Batch`; "
                "group before batching rather than after."
            )

        # Every tag needs an order (a fixed-size list key is list-valued too), from
        # the Arrow types, when the stream knows them without rows; a stream that
        # does not (a function pod's) is checked when it is grouped.
        zero_rows = typed_zero_row_table(stream)
        if zero_rows is not None:
            _refuse_unorderable_tags(
                self.by,
                {c: zero_rows.schema.field(c).type for c in tag_columns},
            )

        # The members are ordered by tags only, never by a data value.
        for name in self.member_order or ():
            if name in tag_columns:
                continue
            if name in data_columns:
                raise InputValidationError(
                    f"GroupBy: member_order names {name!r}, which is a data column "
                    + "of the input, not a tag: members are ordered by tags only. "
                    + f"Make it a tag first: stream.tag_by({name!r})"
                )
            raise InputValidationError(
                f"GroupBy: member_order names {name!r}, which is not a tag column "
                + f"of the input; its tags are {list(tag_columns)}"
            )

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Partition rows by group key and emit one row per group.

        Args:
            stream: The upstream stream.

        Returns:
            A stream with one row per distinct group-key tuple, whose tag
            columns are the group keys, scalar, in the input's tag order.

        Raises:
            InputValidationError: If a key is a list of any kind, or a user tag's
                type has no order (``_unorderable_part``).
        """
        table = stream.as_table(columns={"source": True, "system_tags": True})
        tag_columns, _ = stream.keys()
        # The async path reaches here without ``validate_unary_input``.
        _refuse_unorderable_tags(
            self.by, {c: table.schema.field(c).type for c in tag_columns}
        )

        system_tag_columns = tuple(
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        # The source info of a data-tag key: the key's value is the merge of its
        # members' equal values, so its source info is one token, the fold of the
        # members' tokens in member order, as the system tags fold (ITL-1017).
        key_source_columns = tuple(
            name
            for name in (f"{constants.SOURCE_PREFIX}{c}" for c in self.by)
            if name in table.column_names
        )
        # A tag that is not a key loses its tag role (ITL-1070), but it still
        # orders the members below, so the data lists keep their order.  A plain
        # one is dropped; a data-tag keeps its data column, aggregated
        # like every other data column (developer, November 13, 2026).
        data_tags = stream_data_tags(stream)
        non_key_tags = tuple(c for c in tag_columns if c not in self.by)
        dropped_columns = tuple(c for c in non_key_tags if c not in data_tags)
        member_columns = tuple(
            c
            for c in table.column_names
            if c not in self.by
            and c not in system_tag_columns
            and c not in key_source_columns
            and c not in dropped_columns
        )
        # Non-key user tags order the members of a group: those `member_order`
        # names, in its order, then the others in name order, so the order never
        # depends on the input's tag order (ITL-1226).  The `record_id`
        # columns are appended as a final tiebreaker: tag tuples are supposed
        # to be unique within a stream, but nothing enforces that, and without
        # the tiebreaker duplicate tuples would fall back to emission order --
        # which Ray scheduling and DB fetch order make nondeterministic.
        # `record_id` is fixed when the source is materialized, so it is immune
        # to that shuffling.
        #
        # A joined stream carries one `record_id` per canonical input position,
        # and the broadcast side of a fan-out repeats its id on every row it
        # was joined into.  So *every* record_id column has to take part --
        # consulting only the first would tie on exactly the fan-out rows the
        # tiebreaker exists for.  They are sorted by name so the key order is
        # itself independent of column order in the input table. A record id
        # names a row within its source only (ITL-1154), so the source-id columns
        # come last: two members with equal record ids from different sources still
        # have one order.
        named = self.member_order or ()
        sort_columns = named + tuple(sorted(c for c in non_key_tags if c not in named))
        for prefix in (
            constants.SYSTEM_TAG_RECORD_ID_PREFIX,
            constants.SYSTEM_TAG_SOURCE_ID_PREFIX,
        ):
            sort_columns += tuple(
                sorted(c for c in system_tag_columns if c.startswith(prefix))
            )

        # Polars decides which rows form a group and in what order, in one stable
        # sort; the output is built from the input's Arrow arrays (ITL-1250). The
        # sort frame has synthetic names, so no user column name can collide: the
        # row number, the keys in name order (``by`` is sorted), then the columns
        # that order the members.
        frame_columns: dict[str, pa.Array[Any]] = {
            "row": pa.array(np.arange(table.num_rows, dtype=np.int64), pa.int64())
        }
        key_names = [f"key{i}" for i in range(len(self.by))]
        order_names = [f"order{i}" for i in range(len(sort_columns))]
        for name, c in zip(
            [*key_names, *order_names], [*self.by, *sort_columns], strict=True
        ):
            frame_columns[name] = _polars_ready(table.column(c).combine_chunks())
        # The bits of each float key, to emit the member value with the smallest.
        key_storage = {c: _storage(table.column(c)) for c in self.by}
        bit_names: dict[str, str] = {}
        for i, c in enumerate(self.by):
            bits = _float_bits(key_storage[c])
            if bits is not None:
                bit_names[c] = f"bits{i}"
                frame_columns[f"bits{i}"] = bits

        frame = cast(
            "pl.DataFrame",
            pl.from_arrow(
                pa.Table.from_arrays(
                    list(frame_columns.values()), names=list(frame_columns)
                )
            ),
        )
        # Keys first, so the groups come out in key order: a NaN key after every
        # number and before null, null last (``nulls_last``); rows that tie on every
        # column keep the input's order.
        frame = frame.sort(
            [_sort_key(name, frame.schema[name]) for name in key_names + order_names],
            nulls_last=True,
            maintain_order=True,
        )
        # Group boundaries by Polars' own equality: every NaN equals every NaN and
        # 0.0 equals -0.0, as the joins match keys (ITL-1234, ITL-1251).
        frame = frame.with_columns(pl.struct(key_names).rle_id().alias("group"))
        groups = frame.group_by("group", maintain_order=True).agg(
            pl.len().alias("size"),
            pl.col("row").first().alias("first"),
            # A float key: the member value with the smallest bit pattern at the
            # column's own width (ITL-1234), never one chosen by the row order or by
            # ``member_order``; a null key through a null index.
            *(
                pl.when(pl.col(bits).is_null().all())
                .then(None)
                .otherwise(pl.col("row").sort_by(bits).first())
                .alias(bits)
                for bits in bit_names.values()
            ),
        )
        order = frame["row"].to_arrow()
        offsets = cast(
            "pa.Int64Array",
            pa.array(
                np.concatenate([[0], np.cumsum(groups["size"].to_numpy())]),
                pa.int64(),
            ),
        )

        def emitted(c: str) -> pa.Array[Any]:
            # Every member of a group holds the same key value except a float's
            # bits, so a non-float key is the first member's.
            rows = groups[bit_names.get(c, "first")].to_arrow().cast(pa.int64())
            return table.column(c).take(rows).combine_chunks()

        def folded(c: str) -> pa.Array[Any]:
            # The members' text values in member order, folded per group; text loses
            # nothing in Python.
            values = cast(list[Any], table.column(c).take(order).to_pylist())
            bounds = cast(list[int], offsets.to_pylist())
            return pa.array(
                [
                    arrow_utils.fold_system_tag_values(
                        values[start:end], label=GROUP_BY_FOLD_LABEL
                    )
                    for start, end in zip(bounds[:-1], bounds[1:])
                ],
                table.schema.field(c).type,
            )

        # Every stream has a data context; StreamProtocol does not declare it.
        type_converter = stream.data_context.type_converter  # pyright: ignore[reportAttributeAccessIssue]
        fields: list[pa.Field[Any]] = []
        arrays: list[pa.Array[Any]] = []
        # The input's columns in its order, without the dropped tags: a key and a
        # folded column keep their field, a member column becomes a list.
        for field in table.drop_columns(list(dropped_columns)).schema:
            c = field.name
            if c in member_columns:
                list_type = arrow_utils.aggregated_list_type(
                    field.type, type_converter, column=c
                )
                fields.append(pa.field(c, list_type, nullable=False))
                arrays.append(
                    arrow_utils.aggregated_list_array(
                        table.column(c).take(order), offsets, list_type
                    )
                )
            else:
                fields.append(field)
                arrays.append(emitted(c) if c in self.by else folded(c))
        grouped_table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))

        n_char = self.orcapod_config.hashing.system_tag_n_char
        grouped_table = arrow_utils.append_to_system_tags(
            grouped_table, stream.pipeline_hash().to_hex(n_char)
        )

        # The keys, in the input's tag order rather than the order of `by`.  A
        # data-tag key stays a data-tag: scalar in both roles (specification
        # section 8.1); a data-tag non-key is now a plain (list) data column.
        # The keys lead the table, as in a table built from rows (ITL-1131): a
        # data-tag non-key declared before them keeps its data column, which
        # must not come first.
        keys = [c for c in tag_columns if c in self.by]
        grouped_table = grouped_table.select(
            [*keys, *(c for c in grouped_table.column_names if c not in keys)]
        )
        return ArrowTableStream(
            grouped_table,
            tag_columns=keys,
            data_context=stream.data_context,
            data_tags=[c for c in data_tags if c in self.by],
        )

    # ------------------------------------------------------------------
    # Schema prediction
    # ------------------------------------------------------------------

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Predict the grouped output schemas without grouping.

        Args:
            stream: The upstream stream.
            columns: Column inclusion config.
            all_info: Include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.  The tag schema holds the
            group keys, scalar, in the input's tag order, and the system tags,
            scalar under their suffixed names; every other user tag is
            dropped.  The data schema holds the input's data columns as
            ``list[T]`` (a data-tag non-key among them), except that a
            data-tag key stays scalar.
        """
        tag_types, data_types = stream.output_schema(columns=columns, all_info=all_info)
        n_char = self.orcapod_config.hashing.system_tag_n_char
        suffix = stream.pipeline_hash().to_hex(n_char)

        out_tag_types: dict[str, Any] = {}
        for name, col_type in tag_types.items():
            if name.startswith(constants.SYSTEM_TAG_PREFIX):
                out_tag_types[f"{name}{constants.BLOCK_SEPARATOR}{suffix}"] = col_type
            elif name in self.by:
                out_tag_types[name] = col_type
            # Any other user tag is dropped (ITL-1070).

        # A data-tag group key is scalar as data too (specification section 8.1);
        # a data-tag non-key keeps its data column, as a list (ITL-1070).
        out_data_types = {
            name: t if name in self.by else list[t] for name, t in data_types.items()
        }

        return Schema(out_tag_types), Schema(out_data_types)
