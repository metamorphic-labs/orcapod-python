"""Rejoin: put the branches of one stream back together, row by origin row (ITL-1336).

A pipeline that splits one stream into branches and puts them back together (a diamond)
is expected to use ``Rejoin``, not ``Join`` (developer, November 21, 2026). ``Join``
combines different rows on their user tags and keeps every input's system tags, renamed
per input, so a diamond's system-tag columns double at every ``Join``; that is correct for
``Join``, which may pair rows of different origins. ``Rejoin`` pairs the rows of the
branches that come from the same origin row, by matching every system-tag column, and
keeps those columns unchanged: a result row has its origin row's provenance, and stacked
diamonds keep a constant number of system-tag columns.

It is an inner join on every column its inputs share (developer, November 22, 2026,
ITL-1355): the system tags, a tag several inputs have, the source info of a shared
data-tag and the data context. Rows that match nothing are left out; nothing is refused
for what the rows hold.

Provisional (``PROVISIONAL_OPERATORS``): its identity is not frozen with data context v1.
"""

from __future__ import annotations

import warnings
from collections.abc import Collection, Sequence
from typing import Any, ClassVar

import polars as pl
import pyarrow as pa

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
from orcapod.errors import InputValidationError, RejoinDifferingColumnWarning
from orcapod.protocols.core_protocols import (
    DataProtocol,
    StreamProtocol,
    TagProtocol,
)
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils import arrow_utils, schema_utils, source_tokens
from orcapod.utils.warning_utils import outside_orcapod_stacklevel

# Row-number columns of the frames Polars matches, and the column of a row's occurrence
# number; never a key (keys are columns of the inputs, and no input column starts so).
_LEFT_ROW = "__rejoin_left_row"
_RIGHT_ROW = "__rejoin_right_row"
_OCCURRENCE = "__rejoin_occurrence"


def _system_tag_names(stream: StreamProtocol) -> list[str]:
    """The system-tag column names of a stream, sorted."""
    tag_schema, _ = stream.output_schema(columns={"system_tags": True})
    return sorted(
        name for name in tag_schema if name.startswith(constants.SYSTEM_TAG_PREFIX)
    )


def _with_comparable_tokens(tables: Sequence["pa.Table"]) -> "list[pa.Table]":
    """Give each shared source-info column one form: one recipe, or text (ITL-1464).

    Rejoin compares a shared column's values. When every input that has a token column
    derives it by one recipe, it is equal wherever the system tags are, which the rows
    already match on, so it stays derived and is not compared (``_agrees_by_recipe``).
    Otherwise its text is built in every input.

    Args:
        tables: The inputs' rows.

    Returns:
        The same tables; a derived column several of them have has one recipe.
    """
    types: dict[str, list[pa.DataType]] = {}
    for table in tables:
        for field in table.schema:
            types.setdefault(field.name, []).append(field.type)
    differing = {
        name
        for name, kinds in types.items()
        if any(source_tokens.derived_recipe(kind) is not None for kind in kinds)
        and any(kind != kinds[0] for kind in kinds)
    }
    return [source_tokens.materialize(table, differing) for table in tables]


def _agrees_by_recipe(table: "pa.Table", name: str) -> bool:
    """Whether column ``name`` is derived, so rows with equal system tags agree on it."""
    return source_tokens.derived_recipe(table.schema.field(name).type) is not None


def _agreeing(
    pairs: "pl.DataFrame",
    left: "pa.Table",
    right: "pa.Table",
    keys: Sequence[str],
    names: Sequence[str],
) -> "pl.DataFrame":
    """Keep the pairs of rows that also hold equal values in ``names``.

    Equality is that of ``Join``'s keys (the prepared columns of ``arrow_keys``, matched
    by Polars: NaN equals NaN, ``-0.0`` equals ``0.0``), except that here a null equals
    a null (in ``Join`` a null key matches nothing): two rows of one origin row that
    both hold no value in a column agree on it.

    Args:
        pairs: Row numbers of matched rows, ``_LEFT_ROW`` and ``_RIGHT_ROW``.
        left: The table ``_LEFT_ROW`` numbers.
        right: The table ``_RIGHT_ROW`` numbers.
        keys: The columns the pairs already match on (the system tags).
        names: The other shared columns to compare.

    Returns:
        The pairs that agree on ``names``, in the order of ``pairs``.
    """
    columns = [*keys, *names]
    agreeing = polars_key_frame(left, columns, _LEFT_ROW).join(
        polars_key_frame(right, columns, _RIGHT_ROW),
        on=columns,
        how="inner",
        nulls_equal=True,
    )
    return pairs.join(
        agreeing.select(_LEFT_ROW, _RIGHT_ROW),
        on=[_LEFT_ROW, _RIGHT_ROW],
        how="semi",
        maintain_order="left",
    )


def _repeats_an_origin(table: "pa.Table", keys: Sequence[str]) -> bool:
    """Whether two rows of ``table`` hold the same system tags."""
    return bool(polars_key_frame(table, keys).is_duplicated().any())


def _numbered(
    table: "pa.Table",
    keys: Sequence[str],
    others: Sequence[str],
    *,
    among: Sequence[str],
    row: str | None = None,
) -> "pl.DataFrame":
    """Return the key frame of the rows that have their system tags, each numbered.

    A row with a null system tag matches nothing and is left out. Every other row gets
    its occurrence number (``_OCCURRENCE``): how many rows before it, in row order, are
    equal to it in the columns ``among``. With it, rows that repeat an origin row are
    told apart, and two inputs' rows can be matched one to one in row order by a join
    on the key columns and that number, without ever pairing every repeat of one input
    with every repeat of the other.

    Args:
        table: An input's rows.
        keys: The system-tag columns.
        others: The other columns to prepare beside them.
        among: The columns whose equal rows are numbered 0, 1, 2, ...
        row: The name of the row-number column, or ``None`` for none.

    Returns:
        The prepared ``keys`` and ``others``, the row numbers when asked for, and the
        occurrence numbers.
    """
    frame = polars_key_frame(table, [*keys, *others], row)
    return frame.filter(
        pl.all_horizontal(pl.col(name).is_not_null() for name in keys)
    ).with_columns(pl.int_range(pl.len()).over(list(among)).alias(_OCCURRENCE))


def _pairs_of_repeated_origins(
    left: "pa.Table", right: "pa.Table", keys: Sequence[str], others: Sequence[str]
) -> "pl.DataFrame":
    """Pair rows one to one when an input repeats an origin row.

    System tags are assumed unique in each input (developer, November 22, 2026,
    ITL-1355), so this is not the usual path. Should an input repeat an origin row,
    each row still matches at most once: among the rows equal in every shared column,
    the first of one input pairs with the first of the other, the second with the
    second, and so on in row order (the order the rows arrived in, on the async path);
    a row left over matches nothing. The work is linear in the rows: no pair is ever
    made that is not kept. A row left over here is not considered again when a later
    input is rejoined, so with three or more inputs the repeats that remain can depend
    on the order in which the inputs are taken.

    Args:
        left: The rows rejoined so far.
        right: The next input's rows.
        keys: The system-tag columns, in both tables.
        others: The other columns both tables have.

    Returns:
        The row numbers of the pairs, ``_LEFT_ROW`` and ``_RIGHT_ROW``, in the order
        of ``left``'s rows.
    """
    shared = [*keys, *others]
    return (
        _numbered(left, keys, others, among=shared, row=_LEFT_ROW)
        .join(
            _numbered(right, keys, others, among=shared, row=_RIGHT_ROW),
            on=[*shared, _OCCURRENCE],
            how="inner",
            nulls_equal=True,
            maintain_order="left",
        )
        .select(_LEFT_ROW, _RIGHT_ROW)
    )


def _differing_columns(tables: Sequence["pa.Table"], keys: Sequence[str]) -> list[str]:
    """Return the shared columns in which rows of one origin row differ between inputs.

    Read from the inputs' own rows, not from the rows rejoined so far (which no longer
    hold an origin row an earlier input lacks or disagrees on): for every column
    several inputs have besides the system tags, the rows of those inputs are put
    under one another and counted per origin row. An origin row with two values in
    the column is one where two inputs differ. Rows that repeat an origin row are
    compared in row order, the first of each input with the first of the others
    (``_numbered``); a surplus repeat, one more than another input holds, has nothing
    to be compared with and names no column (developer decision, November 22, 2026:
    repeats break the assumption that system tags are unique, and a surplus one is
    left out whatever it holds). Equality is ``Join``'s, and a null equals a null.

    Args:
        tables: The inputs' rows.
        keys: The system-tag columns, in every table.

    Returns:
        The names of the differing columns, in the order first met.
    """
    holders: dict[str, list[int]] = {}
    for index, table in enumerate(tables):
        for name in table.column_names:
            if name not in keys and not _agrees_by_recipe(table, name):
                holders.setdefault(name, []).append(index)
    # The columns several inputs have, by the inputs that have them.
    shared: dict[tuple[int, ...], list[str]] = {}
    for name, indices in holders.items():
        if len(indices) > 1:
            shared.setdefault(tuple(indices), []).append(name)
    origin = [*keys, _OCCURRENCE]
    differing: list[str] = []
    for indices, names in shared.items():
        rows = pl.concat(
            [_numbered(tables[index], keys, names, among=keys) for index in indices]
        )
        origins = rows.select(origin).n_unique()
        if rows.n_unique() == origins:
            continue  # every origin row holds one value in each of these columns
        differing.extend(
            name for name in names if rows.select([*origin, name]).n_unique() > origins
        )
    return differing


def _described(name: str, shared_tags: Collection[str]) -> str:
    """How a warning names a shared column that is not a system tag."""
    if name in shared_tags:
        return f"tag {name!r}"
    if name == constants.CONTEXT_KEY:
        return "the data context"
    if name.startswith(constants.SOURCE_PREFIX):
        tag = name.removeprefix(constants.SOURCE_PREFIX)
        return f"the source info of data-tag {tag!r}"
    return f"column {name!r}"


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
    Data columns must be disjoint, as in ``Join``, and a data-tag in several inputs is
    one column. These are the only refusals: each is read from the inputs' columns,
    none from their rows.

    Rejoin is an inner join on every column its inputs share (developer, November 22,
    2026, ITL-1355): the system-tag columns, a user tag several inputs have, the source
    info of a shared data-tag and the data context. A result row puts together the rows
    of the branches that come from one origin row and are equal in all of them; a row
    that matches nothing is left out. So a tag two branches set to different values, or
    rows of different data contexts, give no row: a non-match, not an error. Equality is
    ``Join``'s: NaN equals NaN and ``-0.0`` equals ``0.0``. A null system tag matches
    nothing (provenance is never null). In any other shared column a null equals a
    null, on purpose unlike ``Join``, where a null tag key matches nothing: two rows of
    one origin row that both hold no value in a column agree on it. When rows of one
    origin row (their system tags match) differ between two inputs in another shared
    column, a ``RejoinDifferingColumnWarning`` names the columns, once per operator and
    set of columns. It is read from the inputs' own rows, so it names every such
    column whatever the order of the inputs, also when a third input lacks the row.

    System tags are assumed unique in each input, as every source makes them, so each
    row matches at most once; no input is refused for repeating one. Should an input
    repeat an origin row, each row still matches at most once: among the rows equal in
    every shared column, the first of one input pairs with the first of the other, the
    second with the second, in row order (the order the rows arrived in, on the async
    path), in time linear in the rows. The warning is narrower for such rows: it
    compares the repeats of an origin row by occurrence (the first of one input with
    the first of another, the second with the second), and a surplus repeat, one more
    than another input holds, matches nothing whatever its values and is never
    compared, so no warning names a column for it. With three or more inputs, the
    inputs are paired one after another in their canonical order, and a surplus repeat
    left out at one step is not considered again at a later one: which repeats remain
    can then depend on that order.

    A shared column is kept once, with the values of the first input that has it. The
    inputs are taken in a canonical order, by pipeline hash, so the result does not
    follow the order of the arguments; inputs with equal pipeline hashes (two branches
    of the same structure) stay in argument order. With unique system tags only the
    bits of values that are equal but not identical (``-0.0`` and ``0.0``, NaNs of
    different payloads) can follow it; no row, hash or identity does. A tag of one
    input is kept. The result keeps the common system-tag columns unchanged. Each row
    keeps its own data context in the data context column of the result's table; like
    any stream, the result reports one data context, and so do the rows it yields one
    by one (``iter_data``), also when its table holds rows of several.

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
        """Rejoin the streams on every column they share.

        Raises:
            InputValidationError: If the inputs' system-tag columns differ or are
                absent, or a shared column's Arrow type differs or cannot be matched.
                Never for what the rows hold.

        Warns:
            RejoinDifferingColumnWarning: Once per operator and set of columns, when
                rows of one origin row differ in another shared column (ITL-1355).
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
                    stream.as_carried_table(
                        columns={"source": True, "system_tags": True, "context": True}
                    ),
                    tag_keys,
                )
            )
        tables = _with_comparable_tokens(tables)
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

        table = tables[0]
        user_tags = set(ordered_streams[0].keys()[0])
        for next_table, next_stream in zip(tables[1:], ordered_streams[1:]):
            next_tags = set(next_stream.keys()[0])
            table = self._rejoin_pair(table, next_table, keys, user_tags & next_tags)
            user_tags |= next_tags
        # After the rows are paired: pairing checks that the shared columns' types agree.
        if differing := _differing_columns(tables, keys):
            self._warn_differing_once(
                tuple(sorted(_described(name, user_tags) for name in differing))
            )

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
            data_context=self._rows_data_context(result_table, tables)
            or ordered_streams[0].data_context_key,
        )

    @staticmethod
    def _rows_data_context(
        result: "pa.Table", inputs: Sequence["pa.Table"]
    ) -> str | None:
        """The data context the result's rows are of, read from the rows.

        It is read from the rows, which carry it, never from what a stream reports (an
        operator's lazy stream reports its pod's context, ITL-1339). Matched rows are
        of one data context each, so a result usually has one; with no result row it is
        the inputs' rows' own. Should rows of several remain (inputs that each hold rows
        of several data contexts), the stream takes the first key in text order. Every
        row still carries its own in the data context column of the table; the rows
        the stream yields one by one report the stream's, as those of any stream that
        holds rows of several data contexts do (``ArrowTableStream.iter_data``,
        ITL-1359).

        Args:
            result: The rejoined rows.
            inputs: The inputs' rows.

        Returns:
            The data context key, or ``None`` when no input has a row.
        """
        for tables in ([result], inputs):
            found = {
                # The context column is text and never null.
                str(key)
                for table in tables
                for key in table.column(constants.CONTEXT_KEY).unique().to_pylist()
            }
            if found:
                return min(found)
        return None

    def _warn_differing_once(self, described: tuple[str, ...]) -> None:
        """Issue ``RejoinDifferingColumnWarning`` once per set of differing columns.

        Args:
            described: The differing columns, each as the warning names it, sorted.
        """
        warned: set[tuple[str, ...]] | None = getattr(self, "_warned_differing", None)
        if warned is None:
            warned = set()
            self._warned_differing: set[tuple[str, ...]] = warned
        if described in warned:
            return
        # Recorded only once the warning is issued: under an error filter it raises,
        # and a retry must raise again.
        warnings.warn(
            "Rejoin left out rows that come from one origin row (their system tags "
            + f"match) and differ in {', '.join(described)}. Rejoin puts rows together "
            + "only when every column its inputs share is equal; a branch changed "
            + "this one. To keep the rows, drop or rename the column in one branch "
            + "before the rejoin (drop_tag_columns, map_tags); to silence this, filter "
            + "orcapod.errors.RejoinDifferingColumnWarning.",
            RejoinDifferingColumnWarning,
            stacklevel=outside_orcapod_stacklevel(),
        )
        warned.add(described)

    def _rejoin_pair(
        self,
        left: "pa.Table",
        right: "pa.Table",
        keys: Sequence[str],
        shared_tags: Collection[str],
    ) -> "pa.Table":
        """Pair the rows of two tables that are equal in every column both have.

        Every output value is taken from ``left`` or ``right`` by row number; Polars
        only decides which rows match (``arrow_keys``). The rows are first matched on
        the system tags, as they always were (so inputs whose shared columns all agree
        give the same rows in the same order as before ITL-1355), and of those pairs
        only the ones that agree on every other shared column are kept (``_agreeing``).
        With unique system tags each row is in one pair at most. When an input repeats
        an origin row, that first match would pair every repeat with every repeat, so
        the rows are paired one to one instead (``_pairs_of_repeated_origins``). A
        column both tables have is kept once, from ``left``.

        Args:
            left: The rows rejoined so far.
            right: The next input's rows.
            keys: The system-tag columns, in both tables.
            shared_tags: The user tags of both tables.

        Returns:
            The paired rows.

        Raises:
            InputValidationError: If a shared column's Arrow type differs, or has no
                rule for matching rows.
        """
        # Every column both tables have besides the system tags: the shared tags, the
        # source info of a shared data-tag, the data context.
        others = [
            name
            for name in right.column_names
            if name in left.column_names
            and name not in keys
            and not _agrees_by_recipe(left, name)
        ]
        for name in sorted(others):
            shared_type = require_shared_arrow_type(
                "Rejoin",
                name,
                arrow_utils.column_arrow_type(left.schema, name),
                arrow_utils.column_arrow_type(right.schema, name),
            )
            require_matchable_key("Rejoin", name, shared_type)
        if _repeats_an_origin(left, keys) or _repeats_an_origin(right, keys):
            pairs = _pairs_of_repeated_origins(left, right, keys, others)
        else:
            pairs = polars_key_frame(left, keys, _LEFT_ROW).join(
                polars_key_frame(right, keys, _RIGHT_ROW), on=list(keys), how="inner"
            )
            # Nothing matched, or nothing else is shared: there is nothing to compare.
            if others and pairs.height:
                pairs = _agreeing(pairs, left, right, keys, others)
        left_rows = pairs.get_column(_LEFT_ROW).to_arrow()
        right_rows = pairs.get_column(_RIGHT_ROW).to_arrow()
        names: list[str] = []
        arrays: list[Any] = []
        for name in left.column_names:
            names.append(name)
            arrays.append(left.column(name).combine_chunks().take(left_rows))
        for name in right.column_names:
            if name not in left.column_names:
                names.append(name)
                arrays.append(right.column(name).combine_chunks().take(right_rows))
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
