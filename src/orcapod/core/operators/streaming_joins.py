"""Streaming join kernels: symmetric hash joins on Arrow record batches (ITL-1362).

The async path of ``Join``, ``MergeJoin`` and ``SemiJoin`` (ITL-1347, stage 5) reads its
inputs as Arrow record batches, in whatever order they arrive, and must give the rows of
the operator's sync ``static_process`` over the same inputs, as a multiset (row order is
not a result on the async path; developer, November 22, 2026). A kernel is that
computation without the channels: the channel layer creates one per run from the input
streams (the descriptions) and hands it each arriving record batch (``push``) and each
input's end (``close``); every call returns the output rows it decided, as record batches.

The oracle is the sync operator over its inputs as a job's edges carry them: each input
rebuilt from its rows (``StaticOutputOperatorPod._materialize_upstream``), which is the
row-built form F of the ITL-1347 spec. Batches arrive in that form (``null`` columns as
``large_string``, ITL-1319), and every type a kernel holds is that form's.

How a kernel stays equal to the sync path:

- **Schema-decided checks before the first batch.** The constructor applies the
  operator's own ``static_process`` to the inputs' zero-row streams in the row-built form
  (the description's Arrow types, or its Python schema when it cannot type itself, put
  through F), which processes no data and raises exactly the sync path's errors for
  those columns and types (ITL-856, e1 of the ITL-1347 spec). Its result,
  ``zero_row_result``, is the output's description: its columns are what every emitted
  batch holds (``output_columns``).
- **Value-decided refusals when the batch arrives.** A refusal that depends on values,
  not on the description, cannot be raised before them: merging the source-info token
  lists of a data-tag several inputs join on refuses lists of different lengths
  (``ValueError``, from the sync path's own helper and with its message). A kernel raises
  it when the offending batch arrives; rows already emitted belong to a run that then
  fails.
- **An input that ends empty.** The sync path reads an input with no rows in its zero-row
  form, where a ``null`` column stays ``null``, not ``large_string``. When the last input
  closes, a kernel whose empty inputs have such a column applies ``static_process`` again
  over that mix, and raises what it raises. An input with no rows makes every join's
  result empty, so no row was emitted before.
- **Matching is the sync join's.** Each input keeps the rows it needs as Arrow tables and
  a Polars frame of their prepared keys and row numbers (``arrow_keys.polars_key_frame``),
  with the constant key every join adds (``join._with_common_key``). An arriving batch's
  key frame is joined with the other side's accumulated key frame by the sync path's own
  rule (``join._matched_pairs``), in one vectorized join, never per row: a top-level
  null key matches nothing. Every value is then taken from the Arrow tables by row
  number, so it is the input's, bit for bit. Each pair is emitted once, by whichever of
  its two rows arrives second.
- **The same column work.** A matched pair's table goes through the steps the sync path
  applies to its joined table (system-tag renaming by each input's pipeline hash and
  position, field flags, merged source info, system-tag value sorting, merged lists),
  with the sync path's helpers. Each of them works row by row, so applying them per
  batch of pairs gives the sync rows.
- **Field flags.** A batch's flags are the sync result's, except the system-tag and
  source-info fields: a complete table decides those by its values (``_flag_by_values``,
  the last step of F), so a kernel declares every such field nullable and the stream made
  from its batches settles them.

A kernel holds only what can still match: a side's rows are dropped when the other side
closes (nothing can arrive to match them), and an arriving row is not kept once the
other side has closed. A step of a ``Join`` chain that can never emit again (a side
closed holding no row) releases its successor's other side and every earlier step, and
``SemiJoin`` never keeps a left row whose key holds a top-level null.
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
    _COMMON_JOIN_KEY,  # pyright: ignore[reportPrivateUsage]  # Join's one rule
    _MERGED_SOURCE_PREFIX,  # pyright: ignore[reportPrivateUsage]  # Join's one rule
    Join,
    _matched_pairs,  # pyright: ignore[reportPrivateUsage]  # Join's one rule
    _with_common_key,  # pyright: ignore[reportPrivateUsage]  # Join's one rule
)
from orcapod.core.operators.key_nullability import key_nullable
from orcapod.core.operators.merge_join import (
    MergeJoin,
    _merged_list,  # pyright: ignore[reportPrivateUsage]  # MergeJoin's one rule
    _output_types,  # pyright: ignore[reportPrivateUsage]  # MergeJoin's one rule
)
from orcapod.core.operators.semijoin import (
    SemiJoin,
    _has_whole_key,  # pyright: ignore[reportPrivateUsage]  # SemiJoin's one rule
)
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.utils import arrow_utils

# The column groups an operator input and result are read with: user tags, system tags,
# data and source info. No meta column and no context column: neither is transported.
TRANSPORT_COLUMNS: dict[str, bool] = {"source": True, "system_tags": True}

# The row-number columns of the key frames, one per side of a binary join; never a key
# (keys are tag columns, and a tag never starts with the meta prefix ``__``).
_ROW = ("__streaming_left_row", "__streaming_right_row")


# TODO(ITL-1360): ``_row_built_columns`` and ``_flag_by_values`` are interim copies of
# stage 2's ``StaticOutputOperatorPod._row_built_columns`` and ``_flag_by_values`` (the
# row-built form F, same names and behaviour); call those once stage 2 is on job-speed.
def _row_built_columns(
    table: pa.Table, keys: tuple[Sequence[str], Sequence[str]]
) -> pa.Table:
    """Return ``table``'s columns as ``_rows_to_table`` lays out the table's rows.

    The steps of F that each row decides on its own: user tags in tag order, system tags
    in table order, the data columns that are not data-tags and the source info of every
    data column, both in data-key order, normalized to the large types without keeping
    ``null`` (a row's datagram stores a ``null`` column as ``large_string``).

    Args:
        table: A table of user tags, system tags, data and source info.
        keys: The tag keys and the data keys of its stream (``stream.keys()``).

    Returns:
        The same arrays, selected and ordered, in their large types.
    """
    tag_keys, data_keys = keys
    names = set(table.column_names)
    tags = set(tag_keys)
    system_tags = [
        n for n in table.column_names if n.startswith(constants.SYSTEM_TAG_PREFIX)
    ]
    data = [n for n in data_keys if n not in tags]
    source_info = [
        f"{constants.SOURCE_PREFIX}{n}"
        for n in data_keys
        if f"{constants.SOURCE_PREFIX}{n}" in names
    ]
    return arrow_utils.normalize_table_to_large_types(
        table.select([*tag_keys, *system_tags, *data, *source_info])
    )


def _flag_by_values(table: pa.Table) -> pa.Table:
    """Flag each system-tag and source-info field nullable only when it holds a null.

    The last step of F, on a complete table only. No schema metadata is kept.

    Args:
        table: A table of user tags, system tags, data and source info.

    Returns:
        The same arrays under the new field flags.
    """
    prefixes = (constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX)
    fields = [
        field.with_nullable(table.column(i).null_count > 0)
        if field.name.startswith(prefixes)
        else field
        for i, field in enumerate(table.schema)
    ]
    return pa.Table.from_arrays(table.columns, schema=pa.schema(fields))


def _settled_by_values(schema: pa.Schema) -> pa.Schema:
    """``schema`` with every system-tag and source-info field nullable.

    What a kernel declares for the fields a complete table decides by its values
    (``_flag_by_values``): one record batch of many cannot.
    """
    prefixes = (constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX)
    return pa.schema(
        [
            field.with_nullable(True) if field.name.startswith(prefixes) else field
            for field in schema
        ]
    )


def _row_built_zero_rows(stream: StreamProtocol) -> tuple[StreamProtocol, bool]:
    """``stream`` with no rows, as an input rebuilt from rows of it reads.

    The zero-row input (``_materialize_upstream`` with no rows) put through F: what
    ``_materialize_upstream(rows, stream, zero_rows=True)`` gives, without rows.

    Args:
        stream: An input stream (its description).

    Returns:
        The zero-row stream in the row-built form, and whether its Arrow types differ
        from the zero-row input's (a ``null`` column, which rows carry as
        ``large_string``).
    """
    pod = StaticOutputOperatorPod
    zero = pod._materialize_upstream([], stream)  # pyright: ignore[reportPrivateUsage]  # the operators' one form of an input
    table = zero.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
    row_built = _flag_by_values(_row_built_columns(table, stream.keys()))
    differs = any(
        field.type != table.schema.field(field.name).type for field in row_built.schema
    )
    built = pod._as_upstream_stream(  # pyright: ignore[reportPrivateUsage]  # the operators' one form of an input
        row_built, stream.keys()[0], stream_data_tags(stream), stream
    )
    return built, differs


def _key_frame(table: pa.Table, keys: Sequence[str], row: str) -> pl.DataFrame:
    """The frame a join matches ``table``'s rows on, rows numbered from 0.

    The prepared keys and the constant join key, as the sync join builds its frames
    (``join._with_common_key``, ``arrow_keys.polars_key_frame``).

    Args:
        table: The rows.
        keys: The shared tag columns.
        row: The row-number column.

    Returns:
        The key frame.
    """
    return polars_key_frame(
        _with_common_key(table.select(list(keys))), [*keys, _COMMON_JOIN_KEY], row
    )


def _match(
    arriving: pl.DataFrame, held: pl.DataFrame, keys: Sequence[str]
) -> pl.DataFrame:
    """Join two key frames by the sync join's rule (``join._matched_pairs``).

    Both frames hold the constant join key, so inputs that share no tag give every pair
    and a top-level null key matches nothing, as on the sync path.

    Args:
        arriving: The arriving batch's key frame.
        held: The other side's accumulated key frame.
        keys: The shared tag columns.

    Returns:
        One row per matched pair, holding both row-number columns.
    """
    return _matched_pairs(arriving, held, [*keys, _COMMON_JOIN_KEY])


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
        return _key_frame(table, self.keys, self.row)

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
    def exhausted(self) -> bool:
        """Whether the link can never emit again: a side closed holding no row.

        A closed side's rows are held while the other side is open; with none, no row
        still to arrive can pair. Two closed sides hold no row (each close drops the
        other side's rows).
        """
        return any(side.closed and not side.rows for side in self.sides)

    def release(self) -> None:
        """End both sides: the link's output is never needed again."""
        for side in self.sides:
            side.closed = True
            side.clear()

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
        # Each input as the sync path reads rows of it, with no rows (the row-built
        # form), and whether that differs from how it reads the input with none.
        built = [_row_built_zero_rows(stream) for stream in self.inputs]
        self._zero_rows: tuple[StreamProtocol, ...] = tuple(z for z, _ in built)
        self._null_typed: tuple[bool, ...] = tuple(d for _, d in built)
        # The schema of every batch of an input, in its row-built layout; the fields a
        # complete table decides by values declared nullable.
        self._layouts: tuple[pa.Schema, ...] = tuple(
            _settled_by_values(stream.as_table(columns=TRANSPORT_COLUMNS).schema)
            for stream in self._zero_rows
        )
        self._closed: list[bool] = [False] * len(self.inputs)
        self._received: list[bool] = [False] * len(self.inputs)
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
        self._received[input_index] = True
        return self._emitted(self._push(input_index, table))

    def close(self, input_index: int) -> list[pa.RecordBatch]:
        """Note that an input has ended; return the output rows that decides.

        Args:
            input_index: The input, in call order.

        Returns:
            The output record batches, possibly none.

        Raises:
            ValueError: If the input has closed already.
            InputValidationError: When the last input closes, what the sync path raises
                over inputs some of which had no rows, read in their zero-row form
                (module docstring).
        """
        self._require_open(input_index)
        self._closed[input_index] = True
        emitted = self._emitted(self._close(input_index))
        if all(self._closed):
            self._check_empty_inputs()
        return emitted

    def _check_empty_inputs(self) -> None:
        """Apply the sync path's checks to the inputs as they ended.

        An input with no rows is read in its zero-row form, where a ``null`` column stays
        ``null``; only such an input can make the checks differ from the constructor's.
        """
        empty = [
            not received and null_typed
            for received, null_typed in zip(self._received, self._null_typed)
        ]
        if not any(empty):
            return
        pod = StaticOutputOperatorPod
        self.operator.static_process(
            *(
                pod._materialize_upstream([], stream)  # pyright: ignore[reportPrivateUsage]  # the operators' one form of an input
                if is_empty
                else zero_rows
                for stream, zero_rows, is_empty in zip(
                    self.inputs, self._zero_rows, empty
                )
            )
        )

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

        The schema is the input's row-built form (``null`` columns as ``large_string``,
        ITL-1319), every system-tag and source-info field nullable (a complete table
        decides those). A batch with no rows may also carry a column in the type of the
        input's zero-row form (``null``): it holds no value.
        """
        layout = self._layouts[input_index]
        names = set(batch.schema.names)
        if missing := [name for name in layout.names if name not in names]:
            raise ValueError(
                f"{type(self).__name__}: a batch of input {input_index} lacks "
                + f"the column(s) {missing} its stream describes"
            )
        table = pa.Table.from_batches([batch]).select(layout.names)
        for field, actual in zip(layout, table.schema):
            if field.type != actual.type and (
                table.num_rows or not pa.types.is_null(actual.type)
            ):
                raise ValueError(
                    f"{type(self).__name__}: column {field.name!r} of input "
                    + f"{input_index} is {actual.type} in a batch, but "
                    + f"{field.type} in its stream"
                )
        if not table.num_rows:
            return layout.empty_table()
        return pa.Table.from_arrays(table.columns, schema=layout)

    def _emitted(self, tables: list[pa.Table]) -> list[pa.RecordBatch]:
        """The record batches of the output tables, holding ``output_columns``.

        Every system-tag and source-info field is declared nullable: the complete
        table's values decide those flags (``_flag_by_values``).
        """
        batches: list[pa.RecordBatch] = []
        for table in tables:
            if table.num_rows:
                table = table.select(list(self.output_columns)).combine_chunks()
                table = pa.Table.from_arrays(
                    table.columns, schema=_settled_by_values(table.schema)
                )
                batches.extend(table.to_batches())
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
        # A step that can never emit again ends the next step's left side, and no
        # earlier step's rows are needed any more (they could only reach this one).
        for step, link in enumerate(self._links):
            if not link.exhausted:
                continue
            for earlier in self._links[:step]:
                earlier.release()
            if (
                step + 1 < len(self._links)
                and not self._links[step + 1].sides[0].closed
            ):
                self._links[step + 1].close(0)
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
        # The columns the sync path does not capture (system tags and source info) are
        # flagged by their values over the complete table; a batch declares them
        # nullable (module docstring).
        for schema in (right_schema, left_schema):
            for field in schema:
                if field.name not in self._captured:
                    output_nullable.setdefault(field.name, True)
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
                # A left row whose key holds a top-level null never matches.
                undecided = table.filter(
                    pc.and_(pc.invert(kept), _has_whole_key(table, self._keys))
                )
                if undecided.num_rows:
                    self._waiting = (
                        undecided
                        if self._waiting is None
                        else pa.concat_tables([self._waiting, undecided])
                    )
            return [table.filter(kept)]
        # A right batch: only its keys, and only rows whose keys hold no top-level null
        # (the sync path drops those: such a key never matches).
        keys = table.select(self._keys)
        keys = keys.filter(_has_whole_key(keys, self._keys))
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
