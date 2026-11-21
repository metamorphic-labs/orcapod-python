"""Rejoin: put the branches of one stream back together, row by origin row (ITL-1336).

A pipeline that splits one stream into branches and puts them back together (a diamond)
is expected to use ``Rejoin``, not ``Join`` (developer, November 21, 2026). ``Join``
combines different rows on their user tags and keeps every input's system tags, renamed
per input, so a diamond's system-tag columns double at every ``Join``; that is correct for
``Join``, which may pair rows of different origins. ``Rejoin`` pairs the rows of the
branches that come from the same origin row, by matching every system-tag column, and
keeps those columns unchanged: a result row has its origin row's provenance, and stacked
diamonds keep a constant number of system-tag columns.

Provisional (``PROVISIONAL_OPERATORS``): its identity is not frozen with data context v1.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.data_tags import data_tag_names, data_tags_first
from orcapod.core.operators.arrow_keys import polars_key_frame
from orcapod.core.operators.base import (
    require_matchable_key,
    require_shared_arrow_type,
)
from orcapod.core.operators.join import Join
from orcapod.core.operators.meta_columns import (
    without_input_meta,
    without_meta_columns,
)
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import (
    DataProtocol,
    StreamProtocol,
    TagProtocol,
)
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

# Row-number columns of the frames Polars matches; never a key (keys are system tags).
_LEFT_ROW = "__rejoin_left_row"
_RIGHT_ROW = "__rejoin_right_row"


def _system_tag_names(stream: StreamProtocol) -> list[str]:
    """The system-tag column names of a stream, sorted."""
    tag_schema, _ = stream.output_schema(columns={"system_tags": True})
    return sorted(
        name for name in tag_schema if name.startswith(constants.SYSTEM_TAG_PREFIX)
    )


_FLOAT_BITS = {16: "uint16", 32: "uint32", 64: "uint64"}


def _bits(array: "pa.Array[Any]") -> "pa.Array[Any]":
    """An array whose equality is bit for bit: each float as its integer bits.

    ``-0.0`` and ``0.0``, or two NaNs of different payloads, are then different values;
    every other type already compares exactly. Lists, fixed-size lists and structs are
    rebuilt over their converted children, with their own validity.
    """
    arrow_type = array.type
    if pa.types.is_floating(arrow_type):
        return array.view(getattr(pa, _FLOAT_BITS[arrow_type.bit_width])())
    if isinstance(array, pa.ExtensionArray):
        return _bits(array.storage)
    mask = array.is_null() if array.null_count else None
    if isinstance(array, pa.ListArray):
        return pa.ListArray.from_arrays(array.offsets, _bits(array.values), mask=mask)
    if isinstance(array, pa.LargeListArray):
        return pa.LargeListArray.from_arrays(
            array.offsets, _bits(array.values), mask=mask
        )
    if isinstance(array, pa.FixedSizeListArray):
        return pa.FixedSizeListArray.from_arrays(
            _bits(array.values), arrow_type.list_size, mask=mask
        )
    if isinstance(array, pa.StructArray):
        children = [_bits(array.field(i)) for i in range(arrow_type.num_fields)]
        names = [arrow_type.field(i).name for i in range(arrow_type.num_fields)]
        return pa.StructArray.from_arrays(children, names=names, mask=mask)
    return array


def _same_bits(left: "pa.Array[Any]", right: "pa.Array[Any]") -> bool:
    """Whether two arrays of one type hold the same value, bit for bit, on every row.

    The matched rows of a rejoin come from one origin row, so a column they share holds
    one value; equal values in different bits (``-0.0`` and ``0.0``) mean a branch
    changed it, and the rejoin would otherwise keep one input's bits by argument order.
    """
    return _bits(left).equals(_bits(right))


def _require_same_system_tags(names: Sequence[Sequence[str]]) -> None:
    """Raise unless every input has the same non-empty set of system-tag columns.

    Args:
        names: Each input's system-tag column names, sorted, in call order.

    Raises:
        InputValidationError: Naming each input's system-tag columns.
    """
    if all(list(name_list) == list(names[0]) for name_list in names) and names[0]:
        return
    listing = "; ".join(
        f"input {i + 1}: {list(name_list) or 'none'}"
        for i, name_list in enumerate(names)
    )
    stated = [list(name_list) for name_list in names if name_list]
    differ = any(name_list != stated[0] for name_list in stated)
    reason = (
        "the inputs' system-tag columns differ, so they are not branches of one "
        "stream (a branch through Join, GroupBy or Batch has other system-tag columns)"
        if differ
        else "an input has no system-tag columns, so no origin row to match on"
    )
    raise InputValidationError(
        f"Rejoin: {reason} ({listing}). Rejoin puts back together the branches of one "
        "stream, row by origin row; to combine different rows on their tags, use Join."
    )


class Rejoin(Join):
    """Put the branches of one stream back together, row by origin row.

    The inputs must share their system-tag columns: the same names, which they have when
    each branch went through operators that keep system tags (filters, column selection
    and renaming, ``AddTags``, function pods) and no ``Join``, ``GroupBy`` or ``Batch``.
    Rows are matched on every system-tag column, so each result row pairs the rows of the
    branches that come from one origin row, at most one per input: an input holding two
    rows of one origin row is refused. That is a deliberate first rule, which no operator
    that keeps system tags breaks today; it can be relaxed later if a branch may duplicate
    rows. A user tag in several inputs must have the same value on matched rows, bit for
    bit (so the result never depends on the order of the inputs); a tag of one input is
    kept. Provenance is never null: an input with a null system-tag value is refused;
    matched system-tag values are compared bit for bit too. The inputs share one data
    context, which the result keeps; inputs of different data contexts are refused.
    Data columns must be disjoint, as in ``Join``, and a data-tag in several inputs is
    one column. The result keeps the common system-tag columns unchanged.

    Inputs from two different sources with the same schema share their system-tag column
    names but no value: their rejoin is empty, not an error. To combine different rows
    on their tags, use ``Join``.

    Rejoin is provisional (``PROVISIONAL_OPERATORS``): its identity may change in a later
    release.
    """

    PROVISIONAL: ClassVar[bool] = True

    def validate_nonzero_inputs(self, *streams: StreamProtocol) -> None:
        """Check that the inputs can be rejoined: shared system tags, compatible columns.

        Raises:
            InputValidationError: If the inputs' system-tag columns differ or are absent,
                or their user columns cannot be combined (``Join``'s rules).
        """
        if len(streams) > 1:
            self._require_rejoinable(streams)
        super().validate_nonzero_inputs(*streams)

    @staticmethod
    def _require_rejoinable(streams: Sequence[StreamProtocol]) -> None:
        """Check, before computing, the system-tag columns the inputs state.

        Every input that states its system-tag columns must state the same ones. An input
        whose schema states none (a raw stream without system tags, or a node of a loaded
        job downstream of a read-only function node, whose saved schema holds none) is
        checked when computed, on its table (``_require_same_system_tags``).

        Args:
            streams: The input streams, in call order.

        Raises:
            InputValidationError: If two inputs state different system-tag columns.
        """
        names = [_system_tag_names(stream) for stream in streams]
        stated = [name_list for name_list in names if name_list]
        if len(stated) > 1 and any(name_list != stated[0] for name_list in stated):
            _require_same_system_tags(names)

    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """The result's schema: Join's user columns, the inputs' system tags unchanged.

        Args:
            *streams: The input streams.
            columns: The column groups to include.
            all_info: Include every column group.

        Returns:
            The ``(tag_schema, data_schema)`` of the result.
        """
        if len(streams) == 1:
            return super().output_schema(*streams, columns=columns, all_info=all_info)
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
        self._require_rejoinable(streams)
        tag_schema, data_schema = self._joined_user_schemas(*streams)
        data_schema = data_tags_first(tag_schema, data_schema)
        if columns_config.system_tags:
            # The common system-tag columns, from the first input that states them.
            for stream in self.order_input_streams(*streams):
                full_tags, _ = stream.output_schema(columns={"system_tags": True})
                system_tags = Schema(
                    {
                        name: full_tags[name]
                        for name in full_tags
                        if name.startswith(constants.SYSTEM_TAG_PREFIX)
                    }
                )
                if system_tags:
                    tag_schema = schema_utils.union_schemas(tag_schema, system_tags)
                    break
        return tag_schema, data_schema

    def static_process(self, *streams: StreamProtocol) -> StreamProtocol:
        """Rejoin the streams on their system-tag columns.

        Raises:
            InputValidationError: If the inputs cannot be rejoined or are of different
                data contexts, an input holds two rows of one origin row, a shared
                column (a system tag, a tag, a shared data-tag's source info, the data
                context) differs bit for bit on matched rows, or a shared tag's Arrow
                type differs.
        """
        if len(streams) == 1:
            return without_input_meta(streams[0], streams)
        ordered_streams = self.order_input_streams(*streams)
        tag_schema, data_schema = self._joined_user_schemas(*streams)

        tables: list[pa.Table] = []
        for stream in ordered_streams:
            tag_keys, _ = stream.keys()
            tables.append(
                without_meta_columns(
                    stream.as_table(
                        columns={"source": True, "system_tags": True, "context": True}
                    ),
                    tag_keys,
                )
            )
        # The rule on the tables themselves, in call order: an input whose schema
        # states no system tag (``_require_rejoinable``) is checked here.
        by_call = {id(stream): table for stream, table in zip(ordered_streams, tables)}
        _require_same_system_tags(
            [
                sorted(
                    name
                    for name in by_call[id(stream)].column_names
                    if name.startswith(constants.SYSTEM_TAG_PREFIX)
                )
                for stream in streams
            ]
        )
        keys = sorted(
            name
            for name in tables[0].column_names
            if name.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        # The rows of one origin row carry one data context: so must the inputs. It
        # is read from the rows, which carry it, never from what a stream reports (an
        # operator's lazy stream reports its pod's context, ITL-1339).
        context_keys = sorted(
            {
                # The context column is text and never null.
                str(key)
                for table in tables
                for key in table.column(constants.CONTEXT_KEY).unique().to_pylist()
            }
        )
        if len(context_keys) > 1:
            raise InputValidationError(
                f"Rejoin: the inputs' rows are of different data contexts "
                f"{context_keys}; branches of one source share its data context"
            )
        data_context = (
            context_keys[0] if context_keys else ordered_streams[0].data_context_key
        )
        for stream, table in zip(ordered_streams, tables):
            for name in keys:
                if table.column(name).null_count:
                    raise InputValidationError(
                        f"Rejoin: input {stream} has a null in system-tag column "
                        f"{name!r}; provenance is never null, so the input is refused"
                    )
            frame = polars_key_frame(table, keys)
            duplicated = frame.is_duplicated()
            if duplicated.any():
                # The system-tag values are text (a source id and a record id): name them.
                values = dict(zip(keys, frame.filter(duplicated).row(0)))
                raise InputValidationError(
                    f"Rejoin: input {stream} holds two rows of one origin row, the "
                    f"system-tag values {values}; a first rule pairs at most one row per "
                    "origin row in each input, so such rows are refused (to be relaxed if "
                    "a branch may duplicate rows)"
                )

        table = tables[0]
        user_tags = set(ordered_streams[0].keys()[0])
        for next_table, next_stream in zip(tables[1:], ordered_streams[1:]):
            next_tags = set(next_stream.keys()[0])
            table = self._rejoin_pair(table, next_table, keys, user_tags & next_tags)
            user_tags |= next_tags

        # Lay the columns out as Join does: tags, data, then the hidden columns.
        tag_order = list(tag_schema.keys())
        data_order = [name for name in data_schema if name not in tag_schema]
        listed = {*tag_order, *data_order}
        result_table = table.select(
            tag_order
            + data_order
            + [name for name in table.column_names if name not in listed]
        )
        return ArrowTableStream(
            result_table,
            tag_columns=tuple(tag_order),
            data_tags=data_tag_names(tag_schema, data_schema),
            data_context=data_context,
        )

    def _rejoin_pair(
        self,
        left: "pa.Table",
        right: "pa.Table",
        keys: Sequence[str],
        shared_tags: Collection[str],
    ) -> "pa.Table":
        """Pair the rows of two tables of one origin, by their system-tag columns.

        Every output value is taken from ``left`` or ``right`` by row number; Polars
        only decides which rows match (``arrow_keys``). A column both tables have (a
        system tag, a shared tag, a shared data-tag's source info, the data context) is
        kept once, from ``left``, after checking that the matched rows hold the same
        bits.

        Args:
            left: The rows rejoined so far.
            right: The next input's rows.
            keys: The system-tag columns, in both tables.
            shared_tags: The user tags of both tables.

        Returns:
            The paired rows.

        Raises:
            InputValidationError: If a shared tag or a shared column differs on matched
                rows, or a shared tag's Arrow type differs.
        """
        for name in sorted(shared_tags):
            tag_type = require_shared_arrow_type(
                "Rejoin",
                name,
                arrow_utils.column_arrow_type(left.schema, name),
                arrow_utils.column_arrow_type(right.schema, name),
            )
            require_matchable_key("Rejoin", name, tag_type)
        pairs = polars_key_frame(left, keys, _LEFT_ROW).join(
            polars_key_frame(right, keys, _RIGHT_ROW), on=list(keys), how="inner"
        )
        left_rows = pairs.get_column(_LEFT_ROW).to_arrow()
        right_rows = pairs.get_column(_RIGHT_ROW).to_arrow()
        names: list[str] = []
        arrays: list[Any] = []
        for name in left.column_names:
            names.append(name)
            arrays.append(left.column(name).combine_chunks().take(left_rows))
        for name in right.column_names:
            right_values = right.column(name).combine_chunks().take(right_rows)
            if name not in left.column_names:
                names.append(name)
                arrays.append(right_values)
                continue
            # Polars matched the system tags by equality; they too must be the same
            # bits (a float system tag of -0.0 matches 0.0).
            left_values = arrays[names.index(name)]
            if not _same_bits(left_values, right_values):
                if name in shared_tags:
                    what = f"tag {name!r}"
                elif name in keys:
                    what = f"system tag {name!r}"
                elif name == constants.CONTEXT_KEY:
                    what = f"the data context column {name!r}"
                else:
                    what = f"column {name!r} (a shared data-tag's source info)"
                raise InputValidationError(
                    f"Rejoin: {what} has different values on rows of one origin row in "
                    "two inputs (compared bit for bit: -0.0 is not 0.0, NaNs of "
                    "different payloads differ); a branch changed it, so the rows cannot "
                    "be put back together (rename it in one branch, or use Join)"
                )
        table = pa.Table.from_arrays(arrays, names=names)
        table = arrow_utils.restore_schema_nullability(table, left.schema, right.schema)
        return self._shared_key_nullability(
            table, left.schema, right.schema, shared_tags
        )

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Collect the inputs and rejoin them (the barrier of every operator).

        Rejoin is an inner match, so an input with no rows gives no rows; the rows equal
        the sync path's (``static_process``).
        """
        await StaticOutputOperatorPod.async_execute(
            self, inputs, output, input_pipeline_hashes=input_pipeline_hashes
        )

    def identity_structure(self) -> Any:
        self._note_provisional_identity_use()  # a direct read is a use (ITL-1279)
        return self.__class__.__name__

    def __repr__(self) -> str:
        return "Rejoin()"
