"""Streaming join kernels: symmetric hash joins on Arrow record batches (ITL-1362).

The async path of ``Join``, ``MergeJoin`` and ``SemiJoin`` (ITL-1347, stage 5) reads its
inputs as Arrow record batches, in whatever order they arrive, and must give the rows of
the operator's sync ``static_process`` over the same inputs, as a multiset (row order is
not a result on the async path; developer, November 22, 2026). A kernel is that
computation without the channels: the channel layer creates one per run from the input
streams (the descriptions) and hands it each arriving record batch (``push``) and each
input's end (``close``); every call returns the output rows it decided, as record batches.

How a kernel stays equal to the sync path:

- **Checks before the first batch.** The constructor applies the operator's own
  ``static_process`` to the inputs' zero-row streams
  (``StaticOutputOperatorPod._materialize_upstream`` with no rows: the description's
  Arrow types, or its Python schema when it cannot type itself), which processes no data
  and raises exactly the sync path's errors (ITL-856, e1 of the ITL-1347 spec). The
  zero-row result is also the output's description: its columns are what every emitted
  batch holds (``output_columns``).
- **Matching is the sync join's.** Each input keeps the rows it needs as Arrow tables and
  a Polars frame of their prepared keys and row numbers (``arrow_keys.polars_key_frame``,
  as ``_inner_join`` builds it). An arriving batch's key frame is joined with the other
  side's accumulated key frame in one vectorized join, never per row; every value is then
  taken from the Arrow tables by row number, so it is the input's, bit for bit. Each pair
  is emitted once, by whichever of its two rows arrives second.
- **The same column work.** A matched pair's table goes through the steps the sync path
  applies to its joined table (system-tag renaming by each input's pipeline hash and
  position, field flags, merged source info, system-tag value sorting, merged lists),
  with the sync path's helpers. Each of them works row by row, so applying them per
  batch of pairs gives the sync rows.
- **Field flags.** A batch's flags are the description's, except source info, which a
  complete table decides (``_flag_source_info_nullability``, step 5 of the spec's
  row-built form F): a kernel declares every source-info field nullable, and the stream
  made from its batches settles them.

A kernel holds only what can still match: a side's rows are dropped when the other side
closes (nothing can arrive to match them), and an arriving row is not kept once the
other side has closed.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from typing import Any, ClassVar

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

from orcapod.core.data_tags import stream_data_tags
from orcapod.core.operators.arrow_keys import polars_key_frame
from orcapod.core.operators.join import (
    _MERGED_SOURCE_PREFIX,  # pyright: ignore[reportPrivateUsage]  # Join's one rule
    Join,
)
from orcapod.core.operators.key_nullability import key_nullable
from orcapod.core.operators.merge_join import (
    MergeJoin,
    _merged_list,  # pyright: ignore[reportPrivateUsage]  # MergeJoin's one rule
    _output_types,  # pyright: ignore[reportPrivateUsage]  # MergeJoin's one rule
)
from orcapod.core.operators.semijoin import SemiJoin
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.utils import arrow_utils

# The column groups an operator input and result are read with: user tags, system tags,
# data and source info. No meta column and no context column: neither is transported.
TRANSPORT_COLUMNS: dict[str, bool] = {"source": True, "system_tags": True}

# The row-number columns of the key frames, one per side of a binary join; never a key
# (keys are tag columns, and a tag never starts with the meta prefix ``__``).
_ROW = ("__streaming_left_row", "__streaming_right_row")


def _key_frame(
    table: pa.Table, keys: Sequence[str], row: str, offset: int
) -> pl.DataFrame:
    """Return the prepared keys of ``table`` and its row numbers, counted from ``offset``.

    Args:
        table: The rows.
        keys: The key columns (``arrow_keys.polars_key_frame``).
        row: The row-number column.
        offset: The number of the first row.

    Returns:
        The key frame.
    """
    frame = polars_key_frame(table, keys, row)
    if offset:
        frame = frame.with_columns(pl.col(row) + offset)
    return frame


def _match(
    arriving: pl.DataFrame, held: pl.DataFrame, keys: Sequence[str]
) -> pl.DataFrame:
    """Join two key frames as the sync join does: inner on ``keys``, every pair if none.

    The sync path joins on its keys plus a constant column (``Join.static_process``), so
    inputs that share no key give every pair; that is a cross join.

    Args:
        arriving: The arriving batch's key frame.
        held: The other side's accumulated key frame.
        keys: The key columns.

    Returns:
        One row per matched pair, holding both row-number columns.
    """
    if not keys:
        return arriving.join(held, how="cross")
    return arriving.join(held, on=list(keys), how="inner")


def _joined_by_rows(
    left: pa.Table,
    right: pa.Table,
    keys: Sequence[str],
    left_rows: pa.Array[Any],
    right_rows: pa.Array[Any],
) -> pa.Table:
    """Take matched pairs from two tables, in the layout of ``join._inner_join``.

    The left columns (one of each key, the left row's value), then the right columns that
    are not keys, a name the left table also has taking the suffix ``_right``.

    Args:
        left: The left table.
        right: The right table.
        keys: The key columns.
        left_rows: The left row of each pair.
        right_rows: The right row of each pair.

    Returns:
        The joined table, fields without the inputs' flags (as ``_inner_join``).
    """
    names: list[str] = []
    arrays: list[Any] = []
    for name in left.column_names:
        names.append(name)
        arrays.append(left.column(name).take(left_rows))
    for name in right.column_names:
        if name in keys:
            continue
        names.append(f"{name}_right" if name in left.column_names else name)
        arrays.append(right.column(name).take(right_rows))
    return pa.Table.from_arrays(arrays, names=names)


class _Side:
    """One side of a binary symmetric hash join: the rows it holds and their key frame.

    Args:
        keys: The key columns.
        row: This side's row-number column.
    """

    def __init__(self, keys: Sequence[str], row: str) -> None:
        self.keys: list[str] = list(keys)
        self.row: str = row
        self.closed: bool = False
        self.schema: pa.Schema | None = None
        self._tables: list[pa.Table] = []
        self._frames: list[pl.DataFrame] = []
        self._table: pa.Table | None = None
        self._frame: pl.DataFrame | None = None
        self.rows: int = 0

    def key_frame(self, table: pa.Table) -> pl.DataFrame:
        """The key frame of ``table``, its rows numbered from 0."""
        return _key_frame(table, self.keys, self.row, 0)

    def append(self, table: pa.Table, frame: pl.DataFrame) -> None:
        """Hold ``table``'s rows, with ``frame`` from ``key_frame``."""
        self._tables.append(table)
        # Numbered after the rows already held, as ``table()`` holds them.
        self._frames.append(
            frame.with_columns(pl.col(self.row) + self.rows) if self.rows else frame
        )
        self._table = None
        self._frame = None
        self.rows += table.num_rows

    def table(self) -> pa.Table:
        """Every row held, numbered as in ``frame``."""
        if self._table is None:
            self._table = pa.concat_tables(self._tables)
        return self._table

    def frame(self) -> pl.DataFrame:
        """The key frame of every row held."""
        if self._frame is None:
            self._frame = pl.concat(self._frames, rechunk=False)
            self._frames = [self._frame]
        return self._frame

    def clear(self) -> None:
        """Drop every row held."""
        self._tables, self._frames = [], []
        self._table, self._frame = None, None
        self.rows = 0


class _BinaryLink:
    """A binary symmetric hash join of two sides, left and right.

    Args:
        keys: The key columns.
        finish: Turns the pairs' joined table (``_joined_by_rows`` layout) into the
            link's output rows; given the two sides' schemas.
    """

    def __init__(
        self,
        keys: Sequence[str],
        finish: Callable[[pa.Table, pa.Schema, pa.Schema], pa.Table],
    ) -> None:
        self.keys: list[str] = list(keys)
        self.sides: tuple[_Side, _Side] = (_Side(keys, _ROW[0]), _Side(keys, _ROW[1]))
        self._finish: Callable[[pa.Table, pa.Schema, pa.Schema], pa.Table] = finish

    @property
    def held_rows(self) -> int:
        """The rows both sides hold."""
        return self.sides[0].rows + self.sides[1].rows

    @property
    def closed(self) -> bool:
        """Whether both sides have closed."""
        return self.sides[0].closed and self.sides[1].closed

    def push(self, side: int, table: pa.Table) -> pa.Table | None:
        """Match ``table``'s rows with the other side's, then hold them if still needed.

        Args:
            side: 0 for the left side, 1 for the right.
            table: The arriving rows; every table of one side has one schema.

        Returns:
            The output rows of the new pairs, or ``None`` when there is none.
        """
        this, other = self.sides[side], self.sides[1 - side]
        if this.schema is None:
            this.schema = table.schema
        frame = this.key_frame(table)
        result = None
        if other.rows:
            pairs = _match(frame, other.frame(), self.keys)
            if pairs.height:
                rows = (
                    pairs.get_column(_ROW[0]).to_arrow(),
                    pairs.get_column(_ROW[1]).to_arrow(),
                )
                tables = (table, other.table()) if side == 0 else (other.table(), table)
                joined = _joined_by_rows(*tables, self.keys, *rows)
                assert self.sides[0].schema is not None
                assert self.sides[1].schema is not None
                result = self._finish(
                    joined, self.sides[0].schema, self.sides[1].schema
                )
        # A row is held only for rows still to arrive on the other side.
        if not other.closed:
            this.append(table, frame)
        return result

    def close(self, side: int) -> None:
        """Note that ``side`` ends: the other side's rows can never match again."""
        self.sides[side].closed = True
        self.sides[1 - side].clear()


class StreamingJoinKernel(ABC):
    """The streaming form of a join operator, over Arrow record batches.

    Created from the operator and its input streams, in call order; the streams are the
    descriptions (keys, Arrow types, pipeline hashes) and no row of them is read. The
    channel layer then calls ``push`` with each record batch an input delivers and
    ``close`` when an input ends, and sends on the record batches each call returns. A
    batch holds an input's transport columns (user tags, system tags, data and source
    info, by name, in any order); the batches emitted hold ``output_columns``.

    Args:
        operator: The operator.
        inputs: Its input streams, in call order.

    Raises:
        InputValidationError: Whatever the sync path raises for these inputs' columns
            and types (``static_process`` over the zero-row inputs).
    """

    # How many inputs the operator takes; ``None`` for one or more.
    ARITY: ClassVar[int | None] = None

    def __init__(
        self, operator: StaticOutputOperatorPod, inputs: Sequence[StreamProtocol]
    ) -> None:
        if not inputs or (self.ARITY is not None and len(inputs) != self.ARITY):
            raise ValueError(
                f"{type(self).__name__} takes "
                + (f"{self.ARITY}" if self.ARITY is not None else "one or more")
                + f" inputs, got {len(inputs)}"
            )
        self.operator: StaticOutputOperatorPod = operator
        self.inputs: tuple[StreamProtocol, ...] = tuple(inputs)
        # Each input as the sync path reads it with no rows: its description.
        self._zero_rows: tuple[StreamProtocol, ...] = tuple(
            StaticOutputOperatorPod._materialize_upstream([], stream)  # pyright: ignore[reportPrivateUsage]  # the operators' one form of an input
            for stream in self.inputs
        )
        self._layouts: tuple[pa.Schema, ...] = tuple(
            stream.as_table(columns=TRANSPORT_COLUMNS).schema
            for stream in self._zero_rows
        )
        self._schemas: list[pa.Schema | None] = [None] * len(self.inputs)
        self._closed: list[bool] = [False] * len(self.inputs)
        # The sync path over no rows: its checks, and the output's columns.
        self.zero_row_result: StreamProtocol = operator.static_process(*self._zero_rows)
        self.output_columns: tuple[str, ...] = tuple(
            self.zero_row_result.as_table(columns=TRANSPORT_COLUMNS).column_names
        )

    @property
    @abstractmethod
    def held_rows(self) -> int:
        """How many input rows the kernel holds now (rows that may still match)."""

    def push(self, input_index: int, batch: pa.RecordBatch) -> list[pa.RecordBatch]:
        """Take one record batch of an input; return the output rows it decides.

        Args:
            input_index: The input, in call order.
            batch: The batch, holding the input's transport columns.

        Returns:
            The output record batches, possibly none.

        Raises:
            ValueError: If the input has closed, or the batch lacks a column of the
                input's description or holds one of another Arrow type.
        """
        self._require_open(input_index)
        table = self._as_input_table(input_index, batch)
        if not table.num_rows:
            return []
        return self._emitted(self._push(input_index, table))

    def close(self, input_index: int) -> list[pa.RecordBatch]:
        """Note that an input has ended; return the output rows that decides.

        Args:
            input_index: The input, in call order.

        Returns:
            The output record batches, possibly none.

        Raises:
            ValueError: If the input has closed already.
        """
        self._require_open(input_index)
        self._closed[input_index] = True
        return self._emitted(self._close(input_index))

    @abstractmethod
    def _push(self, input_index: int, table: pa.Table) -> list[pa.Table]:
        """Take an input's rows (in its layout, never empty); return output tables."""

    @abstractmethod
    def _close(self, input_index: int) -> list[pa.Table]:
        """Note an input's end; return output tables."""

    def _require_open(self, input_index: int) -> None:
        if not 0 <= input_index < len(self.inputs):
            raise ValueError(
                f"{type(self).__name__}: no input {input_index} "
                + f"(it has {len(self.inputs)})"
            )
        if self._closed[input_index]:
            raise ValueError(
                f"{type(self).__name__}: input {input_index} has closed already"
            )

    def _as_input_table(self, input_index: int, batch: pa.RecordBatch) -> pa.Table:
        """Return a batch's columns in the input's layout, under one schema per input.

        The schema is the description's: its column order, its Arrow types and flags,
        except that every source-info field is nullable (a complete table decides
        those) and a column the description types ``null`` takes the batch's type
        (rows carry ``large_string`` there, ITL-1319).
        """
        layout = self._layouts[input_index]
        names = set(batch.schema.names)
        if missing := [name for name in layout.names if name not in names]:
            raise ValueError(
                f"{type(self).__name__}: a batch of input {input_index} lacks "
                + f"the column(s) {missing} its stream describes"
            )
        table = pa.Table.from_batches([batch]).select(layout.names)
        schema = self._schemas[input_index]
        if schema is None:
            fields: list[pa.Field[Any]] = []
            for field, actual in zip(layout, table.schema):
                arrow_type = field.type
                if pa.types.is_null(arrow_type):
                    arrow_type = actual.type
                nullable = field.nullable or field.name.startswith(
                    constants.SOURCE_PREFIX
                )
                fields.append(
                    pa.field(field.name, arrow_type, nullable, metadata=field.metadata)
                )
            schema = pa.schema(fields)
            self._schemas[input_index] = schema
        for field, actual in zip(schema, table.schema):
            if field.type != actual.type:
                raise ValueError(
                    f"{type(self).__name__}: column {field.name!r} of input "
                    + f"{input_index} is {actual.type} in a batch, but "
                    + f"{field.type} in its stream"
                )
        return pa.Table.from_arrays(table.columns, schema=schema)

    def _emitted(self, tables: list[pa.Table]) -> list[pa.RecordBatch]:
        """The record batches of the output tables, holding ``output_columns``."""
        batches: list[pa.RecordBatch] = []
        for table in tables:
            if table.num_rows:
                batches.extend(
                    table.select(list(self.output_columns))
                    .combine_chunks()
                    .to_batches()
                )
        return batches

    def _suffix(self, input_index: int, position: int) -> str:
        """What an input's system-tag column names gain: ``{pipeline hash}:{position}``."""
        n_char = self.operator.orcapod_config.hashing.system_tag_n_char
        pipeline_hash = self.inputs[input_index].pipeline_hash()
        return f"{pipeline_hash.to_hex(n_char)}:{position}"


class StreamingJoin(StreamingJoinKernel):
    """``Join`` on record batches: a staggered chain of binary symmetric hash joins.

    The inputs are taken in ``Join``'s canonical order (by pipeline hash, ties in call
    order), and joined as ``static_process`` joins them: the first with the second, the
    result with the third, and so on, each step on the tags its two sides share. Each
    step is a binary symmetric hash join whose left side receives the previous step's
    rows as they are decided. A data-tag several inputs join on keeps its members'
    source-info tokens pending through the chain, folded when the last step emits a row.

    Args:
        operator: The ``Join``.
        inputs: Its input streams, in call order.
    """

    def __init__(self, operator: Join, inputs: Sequence[StreamProtocol]) -> None:
        super().__init__(operator, inputs)
        self._join = operator
        order = sorted(
            range(len(inputs)), key=lambda i: inputs[i].pipeline_hash().to_hex()
        )
        self._position = {index: position for position, index in enumerate(order)}
        self._renames: dict[int, dict[str, str]] = {}
        self._links: list[_BinaryLink] = []
        self._merged_source: set[str] = set()
        if len(inputs) == 1:
            return
        first = inputs[order[0]]
        tag_keys = set(first.keys()[0])
        data_tags = set(stream_data_tags(first))
        for position, index in enumerate(order[1:], start=1):
            stream = inputs[index]
            next_tags = set(stream.keys()[0])
            next_data_tags = set(stream_data_tags(stream))
            merged = Join._merged_data_tags(  # pyright: ignore[reportPrivateUsage]  # Join's one rule
                tag_keys, data_tags, next_tags, next_data_tags
            )
            rename = {
                f"{constants.SOURCE_PREFIX}{name}": f"{_MERGED_SOURCE_PREFIX}{name}"
                for name in sorted(merged)
            }
            self._renames[index] = rename
            self._merged_source |= set(rename)
            keys = sorted(tag_keys & next_tags)
            last = position == len(inputs) - 1
            self._links.append(
                _BinaryLink(keys, self._link_finish(keys, rename, final=last))
            )
            tag_keys |= next_tags
            data_tags |= next_data_tags

    @property
    def held_rows(self) -> int:
        return sum(link.held_rows for link in self._links)

    def _link_finish(
        self, keys: Sequence[str], rename: dict[str, str], *, final: bool
    ) -> Callable[[pa.Table, pa.Schema, pa.Schema], pa.Table]:
        """What one step does to its joined pairs, as ``Join.static_process`` does."""

        def finish(
            joined: pa.Table, left_schema: pa.Schema, right_schema: pa.Schema
        ) -> pa.Table:
            table = arrow_utils.restore_schema_nullability(
                joined, left_schema, right_schema
            )
            table = Join._shared_key_nullability(  # pyright: ignore[reportPrivateUsage]  # Join's one rule
                table, left_schema, right_schema, keys
            )
            for left_name, right_name in rename.items():
                table = Join._merge_source_columns(table, left_name, right_name)  # pyright: ignore[reportPrivateUsage]  # Join's one rule
            if not final:
                return table
            for name in sorted(self._merged_source):
                table = Join._fold_source_column(table, name)  # pyright: ignore[reportPrivateUsage]  # Join's one rule
            return arrow_utils.sort_system_tag_values(table)

        return finish

    def _push(self, input_index: int, table: pa.Table) -> list[pa.Table]:
        if not self._links:
            return [table]
        position = self._position[input_index]
        table = arrow_utils.append_to_system_tags(
            table, self._suffix(input_index, position)
        )
        if rename := self._renames.get(input_index):
            table = table.rename_columns([rename.get(c, c) for c in table.column_names])
        step, side = (0, 0) if position == 0 else (position - 1, 1)
        result = self._links[step].push(side, table)
        while result is not None and step < len(self._links) - 1:
            step += 1
            result = self._links[step].push(0, result)
        return [] if result is None else [result]

    def _close(self, input_index: int) -> list[pa.Table]:
        if not self._links:
            return []
        position = self._position[input_index]
        step, side = (0, 0) if position == 0 else (position - 1, 1)
        self._links[step].close(side)
        # A step whose two sides have closed ends the next step's left side.
        while step < len(self._links) - 1 and self._links[step].closed:
            step += 1
            self._links[step].close(0)
        return []


class StreamingMergeJoin(StreamingJoinKernel):
    """``MergeJoin`` on record batches: one binary symmetric hash join, argument order.

    Each matched pair's merged lists are built from the pair's two rows, the first
    argument's value first, as ``MergeJoin.binary_static_process`` builds them.

    Args:
        operator: The ``MergeJoin``.
        inputs: Its two input streams, in argument order.
    """

    ARITY: ClassVar[int | None] = 2

    def __init__(self, operator: MergeJoin, inputs: Sequence[StreamProtocol]) -> None:
        super().__init__(operator, inputs)
        left, right = inputs
        left_tags, left_data = left.keys()
        right_tags, right_data = right.keys()
        self._shared_tags = set(left_tags) & set(right_tags)
        merged = Join._merged_data_tags(  # pyright: ignore[reportPrivateUsage]  # Join's one rule
            left_tags, stream_data_tags(left), right_tags, stream_data_tags(right)
        )
        right_names = set(self._layouts[1].names)
        self._rename = {
            f"{constants.SOURCE_PREFIX}{name}": f"{_MERGED_SOURCE_PREFIX}{name}"
            for name in sorted(merged)
            if f"{constants.SOURCE_PREFIX}{name}" in right_names
        }
        self._colliding = (set(left_data) & set(right_data)) - merged
        self._captured = (
            set(left_tags) | set(left_data) | set(right_tags) | set(right_data)
        )
        # The left input's converter, as the sync path takes it.
        self._type_converter = StaticOutputOperatorPod._upstream_context(  # pyright: ignore[reportPrivateUsage]  # the operators' one rule
            left
        ).type_converter
        self._link = _BinaryLink(sorted(self._shared_tags), self._finish)

    @property
    def held_rows(self) -> int:
        return self._link.held_rows

    def _finish(
        self, joined: pa.Table, left_schema: pa.Schema, right_schema: pa.Schema
    ) -> pa.Table:
        """``MergeJoin.binary_static_process`` from its joined table on."""
        output_types = _output_types(
            {f.name: f.type for f in left_schema},
            {f.name: f.type for f in right_schema},
        )
        left_nullable = {
            f.name: f.nullable for f in left_schema if f.name in self._captured
        }
        right_nullable = {
            f.name: f.nullable for f in right_schema if f.name in self._captured
        }
        output_nullable = {**right_nullable, **left_nullable}
        for name in self._shared_tags:
            output_nullable[name] = key_nullable(
                left_nullable[name], right_nullable[name]
            )
        for name in self._colliding:
            output_nullable[name] = False
            source_name = f"{constants.SOURCE_PREFIX}{name}"
            if source_name in output_nullable:
                output_nullable[source_name] = False
        # The sync path flags the columns it does not capture (system tags and source
        # info) by their null count over the whole result; a batch of it cannot, so a
        # system tag keeps its input's flag (a provenance value is never null) and
        # source info is nullable, settled on the complete table (module docstring).
        for schema in (right_schema, left_schema):
            for field in schema:
                if field.name not in self._captured:
                    output_nullable.setdefault(
                        field.name,
                        True
                        if field.name.startswith(constants.SOURCE_PREFIX)
                        else field.nullable,
                    )
        left_names = set(left_schema.names)
        suffixed = (set(right_schema.names) & left_names) - self._shared_tags
        for left_name, right_name in self._rename.items():
            joined = Join._merge_source_columns(joined, left_name, right_name)  # pyright: ignore[reportPrivateUsage]  # Join's one rule
            joined = Join._fold_source_column(joined, left_name)  # pyright: ignore[reportPrivateUsage]  # Join's one rule
        for name in sorted(self._colliding):
            source_name = f"{constants.SOURCE_PREFIX}{name}"
            pairs = [(name, output_types[name])]
            if (
                source_name in joined.column_names
                and f"{source_name}_right" in joined.column_names
            ):
                pairs.append((source_name, output_types[source_name]))
            for column, element_type in pairs:
                list_type = arrow_utils.aggregated_list_type(
                    element_type, self._type_converter, column=column
                )
                merged_list = _merged_list(
                    joined.column(column), joined.column(f"{column}_right"), list_type
                )
                index = joined.column_names.index(column)
                joined = joined.drop_columns([column, f"{column}_right"])
                joined = joined.add_column(index, column, merged_list)
                output_types[column] = list_type
        for base_name in suffixed:
            suffixed_name = f"{base_name}_right"
            if suffixed_name not in joined.column_names:
                continue
            if base_name not in joined.column_names:
                index = joined.column_names.index(suffixed_name)
                column_data = joined.column(suffixed_name)
                joined = joined.drop_columns([suffixed_name])
                joined = joined.add_column(index, base_name, column_data)
            else:
                joined = joined.drop_columns([suffixed_name])
        schema = pa.schema(
            [
                pa.field(name, output_types[name], nullable=output_nullable[name])
                for name in joined.column_names
            ]
        )
        return pa.Table.from_arrays(
            [
                column if column.type == field.type else column.cast(field.type)
                for column, field in zip(joined.columns, schema)
            ],
            schema=schema,
        )

    def _push(self, input_index: int, table: pa.Table) -> list[pa.Table]:
        table = arrow_utils.append_to_system_tags(
            table, self._suffix(input_index, input_index)
        )
        if input_index == 1 and self._rename:
            table = table.rename_columns(
                [self._rename.get(c, c) for c in table.column_names]
            )
        result = self._link.push(input_index, table)
        return [] if result is None else [result]

    def _close(self, input_index: int) -> list[pa.Table]:
        self._link.close(input_index)
        return []


class StreamingSemiJoin(StreamingJoinKernel):
    """``SemiJoin`` on record batches: the left input streams.

    A left row is emitted as soon as its key is among the right rows seen so far; a left
    row with no such match waits until a right row matches it or the right input closes,
    when it is dropped. The right input's rows are held as keys only, while the left
    input is open. With no tag shared by the inputs, every left row passes, as
    ``static_process`` returns the left stream.

    Args:
        operator: The ``SemiJoin``.
        inputs: Its two input streams, left then right.
    """

    ARITY: ClassVar[int | None] = 2

    def __init__(self, operator: SemiJoin, inputs: Sequence[StreamProtocol]) -> None:
        super().__init__(operator, inputs)
        left, right = inputs
        self._keys = list(SemiJoin._match_keys(left.keys()[0], right.keys()[0]))  # pyright: ignore[reportPrivateUsage]  # SemiJoin's one rule
        # The left rows not decided yet, and the right rows' keys.
        self._waiting: pa.Table | None = None
        self._right_frames: list[pl.DataFrame] = []
        self._right_frame: pl.DataFrame | None = None

    @property
    def held_rows(self) -> int:
        waiting = self._waiting.num_rows if self._waiting is not None else 0
        return waiting + sum(frame.height for frame in self._right_frames)

    def _right_keys(self) -> pl.DataFrame | None:
        """The key frame of every right row held, or ``None`` with none."""
        if not self._right_frames:
            return None
        if self._right_frame is None:
            self._right_frame = pl.concat(self._right_frames, rechunk=False)
            self._right_frames = [self._right_frame]
        return self._right_frame

    def _kept(self, left: pa.Table, right_keys: pl.DataFrame) -> pa.Array[Any]:
        """Whether each left row's key is among ``right_keys``, as a boolean mask."""
        kept = polars_key_frame(left, self._keys, _ROW[0]).join(
            right_keys, on=self._keys, how="semi"
        )
        mask = np.zeros(left.num_rows, dtype=np.bool_)
        mask[kept.get_column(_ROW[0]).to_numpy()] = True
        return pa.array(mask)

    def _push(self, input_index: int, table: pa.Table) -> list[pa.Table]:
        if not self._keys:
            return [table] if input_index == 0 else []
        if input_index == 0:
            right_keys = self._right_keys()
            if right_keys is None:
                kept = pa.array(np.zeros(table.num_rows, dtype=np.bool_))
            else:
                kept = self._kept(table, right_keys)
            if not self._closed[1]:
                undecided = table.filter(pc.invert(kept))
                if undecided.num_rows:
                    self._waiting = (
                        undecided
                        if self._waiting is None
                        else pa.concat_tables([self._waiting, undecided])
                    )
            return [table.filter(kept)]
        # A right batch: only its keys, and only rows whose keys hold no null (the
        # sync path drops those: a null key never matches).
        keys = table.select(self._keys)
        valid = keys.column(0).is_valid()
        for index in range(1, keys.num_columns):
            valid = pc.and_(valid, keys.column(index).is_valid())
        keys = keys.filter(valid)
        if not keys.num_rows:
            return []
        frame = polars_key_frame(keys, self._keys)
        if not self._closed[0]:
            self._right_frames.append(frame)
            self._right_frame = None
        if self._waiting is None:
            return []
        kept = self._kept(self._waiting, frame)
        decided = self._waiting.filter(kept)
        remaining = self._waiting.filter(pc.invert(kept))
        self._waiting = remaining if remaining.num_rows else None
        return [decided]

    def _close(self, input_index: int) -> list[pa.Table]:
        if input_index == 0:
            # No left row will arrive: the right keys can match nothing new.
            self._right_frames, self._right_frame = [], None
        else:
            # No right row will arrive: the undecided left rows never match.
            self._waiting = None
        return []
