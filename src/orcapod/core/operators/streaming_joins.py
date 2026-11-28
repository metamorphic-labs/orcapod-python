"""Streaming join kernels: symmetric hash joins on Arrow record batches (ITL-1362).

The async path of ``Join``, ``MergeJoin``, ``SemiJoin`` and ``Rejoin`` (ITL-1347, stage
5) is to read its inputs as Arrow record batches, in whatever order they arrive; these
kernels are that computation, not yet wired into ``async_execute``. A kernel gives the
rows of the operator's sync ``static_process`` over the same inputs as a multiset (row
order is not a result on the async path; developer, November 22, 2026), within the
limits each kernel's docstring states (``StreamingRejoin``: repeated origins with three
or more inputs). A kernel is that computation without the channels: the channel layer creates one per run from the input
streams (the descriptions) and hands it each arriving record batch (``push``) and each
input's end (``close``); every call returns the output rows it decided, as record batches.

The oracle is the sync operator over its inputs as a job's edges carry them: each input
rebuilt from its rows (``StaticOutputOperatorPod._materialize_upstream``), which is the
row-built form F of the ITL-1347 spec. Batches arrive in that form (``null`` columns as
``large_string``, ITL-1319), and every type a kernel holds is that form's.

The kernels serve a job's edges, so their domain is the inputs a job can carry. A
``null``-typed column is outside it: no source accepts one (the Arrow hasher refuses the
type, also with no rows), the type converter maps no Python type to it, and a bound
stream must match its slot. A kernel still accepts a raw stream with such a column and
gives the sync rows for it, but a refusal it raises for such a column before the first
batch is decided on the row-built form and may name ``large_string`` where the sync path
over a raw empty stream names ``null``.

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
  it for the first offending pair it observes, when that pair's batch arrives; rows
  already emitted belong to a run that then fails. With several offending pairs, which
  one is reported may depend on arrival (the sync path reports the first in its own
  order): the error type is the sync path's and the message has its form.
- **An input that ends empty.** The sync path reads an input with no rows in its zero-row
  form, where a ``null`` column stays ``null``, not ``large_string``. When the last input
  closes, a kernel whose empty inputs have such a column applies ``static_process`` again
  over that mix, and raises what it raises; when it emitted no row, that result is its
  ``zero_row_result`` from then on (the output's types are the sync result's). The
  re-check refuses only shared columns (keys, colliding data columns), and an input with
  no rows that shares a tag with the others empties the result (an inner join), so no
  row was emitted before it can refuse. ``SemiJoin`` with no shared tag passes every
  left row whatever its right input holds, and has no shared column to refuse.
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
  source-info fields: a complete table decides those by its values (``StaticOutputOperatorPod._flag_by_values``,
  the last step of F), so a kernel declares every such field nullable and the stream made
  from its batches settles them.

A kernel holds only what can still match: a side's rows are dropped when the other side
closes (nothing can arrive to match them), and an arriving row is not kept once the
other side has closed. A row whose key holds a top-level null is never kept (it matches
nothing). A step of a ``Join`` chain that can never emit again (a side closed holding no
row that can match) closes its successor's left side and releases the earlier steps,
except those the sync path still merges before it (``StreamingJoin._close``).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
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
    NullTokenListError,
    _matched_pairs,  # pyright: ignore[reportPrivateUsage]  # Join's one rule
    _with_common_key,  # pyright: ignore[reportPrivateUsage]  # Join's one rule
    null_token_list_error,
)
from orcapod.core.operators.base import require_shared_arrow_type
from orcapod.core.operators.key_nullability import key_nullable
from orcapod.core.operators.merge_join import (
    MergeJoin,
    _merged_list,  # pyright: ignore[reportPrivateUsage]  # MergeJoin's one rule
    _output_types,  # pyright: ignore[reportPrivateUsage]  # MergeJoin's one rule
)
from orcapod.core.operators.rejoin import (
    Rejoin,
    _described,  # pyright: ignore[reportPrivateUsage]  # Rejoin's one rule
)
from orcapod.core.operators.semijoin import (
    SemiJoin,
    _has_whole_key,  # pyright: ignore[reportPrivateUsage]  # SemiJoin's one rule
)
from orcapod.core.operators.static_output_pod import (
    StaticOutputOperatorPod,
    settle_token_types,
)
from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS, row_built_columns
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.utils import arrow_utils

# The column groups an operator input and result are read with: user tags, system tags,
# data and source info. No meta column and no context column: neither is transported.
TRANSPORT_COLUMNS: dict[str, bool] = {"source": True, "system_tags": True}

# The row-number columns of the key frames, one per side of a binary join; never a key
# (keys are tag columns, and a tag never starts with the meta prefix ``__``).
_ROW = ("__streaming_left_row", "__streaming_right_row")


def _settled_by_values(schema: pa.Schema) -> pa.Schema:
    """``schema`` with every system-tag and source-info field nullable.

    What a kernel declares for the fields a complete table decides by its values
    (``StaticOutputOperatorPod._flag_by_values``): one record batch of many cannot.
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
    row_built = pod._flag_by_values(row_built_columns(table, stream.keys()))  # pyright: ignore[reportPrivateUsage]  # F's last step
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

        A side holds only rows that can match (``push`` drops a row with a null key).

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
        if other.closed and not other.rows:
            # Nothing can pair with these rows, and none is held: no key to prepare.
            return None
        if self.keys:
            # A row whose key holds a top-level null matches nothing: never held, so a
            # closed side holding only such rows is exhausted.
            table = table.filter(_has_whole_key(table, self.keys))
            if not table.num_rows:
                return None
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
        # Each input's source-info token types, as its batches so far settle them
        # (``settle_token_types``): the input's complete table must still be buildable,
        # as the sync path builds it before matching.
        self._token_types: list[dict[str, pa.DataType]] = [{} for _ in self.inputs]
        self._emitted_any: bool = False
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
                input's description or holds one of another Arrow type, or its
                source-info tokens cannot make one table with the input's earlier
                batches (``SourceInfoTypesDisagree``, a ``RowBuiltFormUnavailable``).
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
            self._check_settled_against_empty_inputs()
            result = self._check_empty_inputs()
            if result is not None and not self._emitted_any and not emitted:
                # No row: the output is the sync path's result over the inputs as
                # they ended, whose types an empty input decides.
                self.zero_row_result = result
        return emitted

    def _check_settled_against_empty_inputs(self) -> None:
        """Compare the settled token types with the inputs that delivered no rows.

        Called once every input has closed. The sync path reads an input with no
        rows in its zero-row form, whose source-info token types are the declared
        ones, and the others with the types their rows settle to. The kernels whose
        sync path accepts any token types accept every such run.
        """

    def _check_empty_inputs(self) -> StreamProtocol | None:
        """Apply the sync path's checks to the inputs as they ended.

        An input with no rows is read in its zero-row form, where a ``null`` column stays
        ``null``; only such an input can make the checks differ from the constructor's.

        Returns:
            The sync path's result over the inputs as they ended, with no rows, or
            ``None`` when it is the constructor's.
        """
        empty = [
            not received and null_typed
            for received, null_typed in zip(self._received, self._null_typed)
        ]
        if not any(empty):
            return None
        pod = StaticOutputOperatorPod
        return self.operator.static_process(
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
        """Take an input's rows; return the output tables they decide.

        Args:
            input_index: The input, in call order.
            table: Its rows in its layout (``_as_input_table``), never empty.

        Returns:
            The output tables, possibly none.
        """

    @abstractmethod
    def _close(self, input_index: int) -> list[pa.Table]:
        """Note an input's end; return the output tables that decides.

        Args:
            input_index: The input, in call order; open until this call.

        Returns:
            The output tables, possibly none.
        """

    def _require_open(self, input_index: int) -> None:
        """Refuse an input that does not exist or has closed already.

        Args:
            input_index: The input, in call order.

        Raises:
            ValueError: If the kernel has no such input, or it has closed.
        """
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
        # Tokens that cannot make one table with the input's earlier batches are
        # refused here, as the sync path refuses the input before any match: the
        # batch then changes nothing.
        token_types = settle_token_types(batch, self._token_types[input_index])
        names = set(batch.schema.names)
        if missing := [name for name in layout.names if name not in names]:
            raise ValueError(
                f"{type(self).__name__}: a batch of input {input_index} lacks "
                + f"the column(s) {missing} its stream describes"
            )
        table = pa.Table.from_batches([batch]).select(layout.names)
        columns = list(table.columns)
        for index, (field, actual) in enumerate(zip(layout, table.schema)):
            # The pipeline declares token types including nested item fields.
            if field.type.equals(
                actual.type,
                check_metadata=field.name.startswith(constants.SOURCE_PREFIX),
            ):
                continue
            if not table.num_rows and pa.types.is_null(actual.type):
                continue
            # A source-info column is held to its stream's type like any other
            # column: the pipeline determines it, and a batch never brings its own
            # (ITL-1491).
            raise ValueError(
                f"{type(self).__name__}: column {field.name!r} of input "
                + f"{input_index} is {actual.type} in a batch, but "
                + f"{field.type} in its stream"
            )
        self._check_settled_token_types(input_index, token_types)
        self._token_types[input_index] = token_types
        if not table.num_rows:
            return layout.empty_table()
        return pa.Table.from_arrays(columns, schema=layout)

    def _check_settled_token_types(
        self, input_index: int, token_types: Mapping[str, pa.DataType]
    ) -> None:
        """Refuse a batch whose settled source-info token types this operator refuses.

        Called before the batch changes anything, with the token types its input
        settles to with it (``settle_token_types``). The kernels whose sync path
        accepts any token types (the plain join, the semi-join) accept every batch.

        Args:
            input_index: The batch's input, in call order.
            token_types: The input's source-info token types, this batch included.
        """
        del input_index, token_types

    def _emitted(self, tables: list[pa.Table]) -> list[pa.RecordBatch]:
        """The record batches of the output tables, holding ``output_columns``.

        Every system-tag and source-info field is declared nullable: the complete
        table's values decide those flags (``StaticOutputOperatorPod._flag_by_values``).
        """
        batches: list[pa.RecordBatch] = []
        for table in tables:
            if table.num_rows:
                self._emitted_any = True
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
        # Whether each step merges source info (a data-tag both its sides hold), which
        # refuses token lists of different lengths: a refusal decided by values.
        self._merges: list[bool] = []
        # The steps whose output is dropped: no later step it reaches can emit, but the
        # step still matches, so a value-decided refusal is raised as on sync.
        self._sinks: set[int] = set()
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
            self._merges.append(bool(rename))
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
                try:
                    table = Join._merge_source_columns(table, left_name, right_name)  # pyright: ignore[reportPrivateUsage]  # Join's one rule
                except NullTokenListError as error:
                    raise null_token_list_error(
                        "Join", left_name, table.schema.field(left_name).type
                    ) from error
            if not final:
                return table
            for name in sorted(self._merged_source):
                table = Join._fold_source_column(table, name)  # pyright: ignore[reportPrivateUsage]  # Join's one rule
            return arrow_utils.sort_system_tag_values(table)

        return finish

    def _push(self, input_index: int, table: pa.Table) -> list[pa.Table]:
        """Feed an input's rows into its step and forward what each step emits.

        The input at canonical position 0 is the first step's left side, the input at
        position ``p`` the right side of step ``p - 1``. The rows a step emits are its
        successor's left side, up to the last step, whose rows are the output.

        Args:
            input_index: The input, in call order.
            table: Its rows, never empty.

        Returns:
            The last step's output, possibly none.
        """
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
            if step in self._sinks:
                return []
            step += 1
            result = self._links[step].push(0, result)
        return [] if result is None else [result]

    def _close(self, input_index: int) -> list[pa.Table]:
        """End an input's side of its step, and every side that can no longer receive.

        A step that can never emit again (``_BinaryLink.exhausted``) closes its
        successor's left side. The earlier steps' rows could only reach it, so they are
        released, except the steps up to the last earlier one that merges source info:
        the sync path joins those steps before it reaches the empty one, and a merge
        refuses token lists of different lengths (a value-decided refusal, module
        docstring). Those steps keep matching, and the last of them drops its output
        (``_sinks``). Closing never emits a row.

        Args:
            input_index: The input, in call order.

        Returns:
            No table.
        """
        if not self._links:
            return []
        position = self._position[input_index]
        step, side = (0, 0) if position == 0 else (position - 1, 1)
        self._links[step].close(side)
        for step, link in enumerate(self._links):
            if not link.exhausted:
                continue
            # The last earlier step that merges source info, and every step before it,
            # keep matching; the steps after it, up to this one, are released.
            merging = [j for j in range(step) if self._merges[j]]
            kept = merging[-1] if merging else -1
            if kept >= 0:
                self._sinks.add(kept)
            for earlier in self._links[kept + 1 : step]:
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
        # The data columns the batches carry: a raw stream lists its meta columns
        # as data, and no operator carries them (they are not in the layouts).
        left_data = [n for n in left_data if n in self._layouts[0].names]
        right_data = [n for n in right_data if n in self._layouts[1].names]
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

    def _check_settled_against_empty_inputs(self) -> None:
        """Refuse, when every input has closed, a run whose input with rows settled a
        shared source-info column to another token type than an input with no rows
        declares, as ``MergeJoin`` refuses the two Arrow types of one column.

        The sync path reads the input with no rows in its zero-row form (its declared
        token types) and the other one by its rows (their settled types); the two
        inputs' settled types are compared as each batch arrives
        (``_check_settled_token_types``).

        Raises:
            InputValidationError: If such a column's types differ.
        """
        pod = StaticOutputOperatorPod
        for empty in (0, 1):
            other = 1 - empty
            if self._received[empty] or not self._received[other]:
                continue
            declared = (
                pod._materialize_upstream([], self.inputs[empty])  # pyright: ignore[reportPrivateUsage]  # the operators' one form of an input
                .as_table(columns=TRANSPORT_COLUMNS)
                .schema
            )
            settled = self._token_types[other]
            for name in sorted(settled.keys() & set(declared.names)):
                types = {empty: declared.field(name).type, other: settled[name]}
                _ = require_shared_arrow_type("MergeJoin", name, types[0], types[1])

    def _check_settled_token_types(
        self, input_index: int, token_types: Mapping[str, pa.DataType]
    ) -> None:
        """Refuse a batch when a source-info column both inputs carry settles to two
        token types, as ``MergeJoin`` refuses inputs whose Arrow types for one column
        disagree (``require_shared_arrow_type``).

        The sync path types each input's tokens over its complete table before it
        joins, so it refuses the run before any match, whether keys match or not.
        A kernel learns an input's token type from its first batch with a token and
        refuses at the batch that makes the two inputs disagree.

        Raises:
            InputValidationError: If the two inputs' token types for a shared
                source-info column differ.
        """
        other = self._token_types[1 - input_index]
        for name in sorted(token_types.keys() & other.keys()):
            mine, theirs = token_types[name], other[name]
            left, right = (mine, theirs) if input_index == 0 else (theirs, mine)
            _ = require_shared_arrow_type("MergeJoin", name, left, right)

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
            try:
                joined = Join._merge_source_columns(joined, left_name, right_name)  # pyright: ignore[reportPrivateUsage]  # Join's one rule
            except NullTokenListError as error:
                raise null_token_list_error(
                    "MergeJoin", left_name, joined.schema.field(left_name).type
                ) from error
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


# The occurrence-number column of a Rejoin key frame, and the prefix of the other side's
# columns in the warning's pairs (never an input column: those never start so).
_OCCURRENCE = "__streaming_occurrence"
_OTHER = "__streaming_other_"


def _with_context(table: pa.Table, context_key: str) -> pa.Table:
    """``table`` with the data context column every row of it carries on the sync path.

    An input a job rebuilds from rows carries its producer's data context on every row
    (``StaticOutputOperatorPod._as_upstream_stream``), and ``Rejoin`` matches on it.
    Transport batches carry no context column, so the kernel adds the input's own.
    """
    return table.append_column(
        pa.field(constants.CONTEXT_KEY, pa.large_string(), nullable=False),
        pa.array([context_key] * table.num_rows, pa.large_string()),
    )


def _with_system_tags(table: pa.Table, keys: Sequence[str]) -> pa.Table:
    """The rows of ``table`` whose system tags are all present: a null matches nothing."""
    if not table.num_rows:
        return table
    return table.filter(_has_whole_key(table, keys))


def _take_rows(table: pa.Table, rows: np.ndarray[Any, np.dtype[np.int64]]) -> pa.Table:
    """The rows of ``table`` with these numbers, in this order, without combining chunks.

    Arrow's ``take`` on a chunked column concatenates its chunks first, a copy of the
    whole column. Here only the chunks that hold a wanted row are read: each one's rows
    are taken from it alone, and the small result is put in the order asked for.

    Args:
        table: A table, of any number of chunks.
        rows: Row numbers in ``table``.

    Returns:
        The rows, as one chunk.
    """
    batches = table.to_batches()
    if len(batches) <= 1:
        return table.take(pa.array(rows, pa.int64()))
    lengths = np.fromiter((b.num_rows for b in batches), np.int64, len(batches))
    starts = np.concatenate([np.zeros(1, np.int64), np.cumsum(lengths)[:-1]])
    chunks = np.searchsorted(starts, rows, side="right") - 1
    order = np.argsort(chunks, kind="stable")
    sorted_rows, sorted_chunks = rows[order], chunks[order]
    edges = np.flatnonzero(np.diff(sorted_chunks)) + 1
    parts: list[pa.Table] = []
    for group in np.split(np.arange(len(sorted_rows)), edges):
        chunk = int(sorted_chunks[group[0]])
        local = sorted_rows[group] - starts[chunk]
        parts.append(
            pa.Table.from_batches([batches[chunk]]).take(pa.array(local, pa.int64()))
        )
    taken = pa.concat_tables(parts).combine_chunks()
    return taken.take(pa.array(np.argsort(order, kind="stable"), pa.int64()))


def _occurrences(frame: pl.DataFrame, among: Sequence[str]) -> pl.DataFrame:
    """Number each row among the rows before it equal to it in ``among`` (0, 1, ...).

    With it, rows that repeat an origin row are matched one to one in the order they
    arrived (``Rejoin``'s occurrence rule), and no join ever pairs every repeat of one
    side with every repeat of the other: the pairs made are only the pairs kept.
    """
    return frame.with_columns(
        pl.int_range(pl.len()).over(list(among)).alias(_OCCURRENCE)
    )


class _Remaining:
    """An input's rows not matched yet, with a key frame and a mask of removed rows.

    Removing a row only clears its mask bit; the rows are compacted when the removed
    ones are the majority, so a batch never copies the whole table.

    Args:
        columns: The shared columns of the key frame (system tags first).
        row: The row-number column.
    """

    def __init__(self, columns: Sequence[str], row: str) -> None:
        self.columns: list[str] = list(columns)
        self.row: str = row
        self.closed: bool = False
        self.schema: pa.Schema | None = None
        self._tables: list[pa.Table] = []
        self._frames: list[pl.DataFrame] = []
        self._alive: list[np.ndarray[Any, np.dtype[np.bool_]]] = []
        self._table: pa.Table | None = None
        self._frame: pl.DataFrame | None = None
        self.size: int = 0
        self.rows: int = 0

    def append(self, table: pa.Table) -> None:
        """Hold ``table``'s rows."""
        if not table.num_rows:
            return
        frame = polars_key_frame(table, self.columns, self.row)
        self._frames.append(frame.with_columns(pl.col(self.row) + self.size))
        self._tables.append(table)
        self._alive.append(np.ones(table.num_rows, dtype=np.bool_))
        self._table, self._frame = None, None
        self.size += table.num_rows
        self.rows += table.num_rows

    def table(self) -> pa.Table:
        """Every row appended and not compacted away, numbered as in ``frame``."""
        if self._table is None:
            self._table = pa.concat_tables(self._tables)
            self._tables = [self._table]
        return self._table

    def frame(self) -> pl.DataFrame:
        """The key frame of the rows not removed, in the order they arrived."""
        if self._frame is None:
            frame = pl.concat(self._frames, rechunk=False)
            alive = np.concatenate(self._alive)
            self._frames, self._alive = [frame], [alive]
            self._frame = frame.filter(pl.Series(alive))
        return self._frame

    def remove(self, rows: np.ndarray[Any, np.dtype[np.int64]]) -> None:
        """Remove the rows with these numbers (each held and not removed yet)."""
        if not len(rows):
            return
        self.frame()
        alive = self._alive[0]
        alive[rows] = False
        self.rows -= len(rows)
        self._frame = None
        if self.rows * 2 < self.size:
            self._compact()

    def _compact(self) -> None:
        """Drop the removed rows and number the rest from 0."""
        alive = np.concatenate(self._alive)
        table = self.table().filter(pa.array(alive))
        self.clear()
        self.append(table)

    def clear(self) -> None:
        """Drop every row held."""
        self._tables, self._frames, self._alive = [], [], []
        self._table, self._frame = None, None
        self.size = 0
        self.rows = 0


class _RejoinLink:
    """One step of ``Rejoin``: a symmetric match of two sides on every shared column.

    Args:
        system: The system-tag columns, sorted.
        others: The other columns both sides have (shared user tags, a shared data-tag's
            source info, the data context), sorted.
        finish: Turns the pairs' table into the link's output; given both schemas.
    """

    def __init__(
        self,
        system: Sequence[str],
        others: Sequence[str],
        finish: Callable[[pa.Table, pa.Schema, pa.Schema], pa.Table],
    ) -> None:
        self.system: list[str] = list(system)
        self.others: list[str] = list(others)
        self.shared: list[str] = [*system, *others]
        self.sides: tuple[_Remaining, _Remaining] = (
            _Remaining(self.shared, _ROW[0]),
            _Remaining(self.shared, _ROW[1]),
        )
        self._finish: Callable[[pa.Table, pa.Schema, pa.Schema], pa.Table] = finish
        # The shared columns other than system tags in which rows of one origin row
        # (their system tags match) were seen to differ.
        self.differing: set[str] = set()

    @property
    def held_rows(self) -> int:
        """The unmatched rows both sides hold."""
        return self.sides[0].rows + self.sides[1].rows

    @property
    def exhausted(self) -> bool:
        """Whether the link can never emit again: a side closed holding no row."""
        return any(side.closed and not side.rows for side in self.sides)

    def release(self) -> None:
        """End both sides: the link's output is never needed again."""
        for side in self.sides:
            side.closed = True
            side.clear()

    def close(self, side: int) -> None:
        """Note that ``side`` ends: the other side's rows can never match again."""
        self.sides[side].closed = True
        self.sides[1 - side].clear()

    def push(self, side: int, table: pa.Table) -> pa.Table | None:
        """Match ``table``'s rows with the other side's remaining rows.

        Args:
            side: 0 for the left side, 1 for the right.
            table: The arriving rows (with the data context column).

        Returns:
            The output rows of the matched pairs, or ``None`` when there is none.
        """
        this, other = self.sides[side], self.sides[1 - side]
        if this.schema is None:
            this.schema = table.schema
        table = _with_system_tags(table, self.system)
        if not table.num_rows:
            return None
        row, other_row = _ROW[side], _ROW[1 - side]
        frame = _occurrences(polars_key_frame(table, self.shared, row), self.shared)
        result = None
        unmatched = np.ones(table.num_rows, dtype=np.bool_)
        if other.rows:
            held = _occurrences(other.frame(), self.shared)
            pairs = frame.join(
                held, on=[*self.shared, _OCCURRENCE], how="inner", nulls_equal=True
            )
            if pairs.height:
                mine = pairs.get_column(row).to_numpy()
                theirs = pairs.get_column(other_row).to_numpy()
                unmatched[mine] = False
                tables = (table, other.table()) if side == 0 else (other.table(), table)
                rows = (mine, theirs) if side == 0 else (theirs, mine)
                result = self._pairs(*tables, *rows)
                other.remove(theirs)
        rest = table.filter(pa.array(unmatched)) if not unmatched.all() else table
        if rest.num_rows and other.rows and self.others:
            self._note_differing(rest, other, side)
        if not other.closed:
            this.append(rest)
        return result

    def _pairs(
        self,
        left: pa.Table,
        right: pa.Table,
        left_rows: np.ndarray[Any, np.dtype[np.int64]],
        right_rows: np.ndarray[Any, np.dtype[np.int64]],
    ) -> pa.Table:
        """The matched pairs as ``Rejoin._rejoin_pair`` builds them, by row number.

        Values are taken from the chunks that hold them (``_take_rows``): no held
        table is copied.
        """
        left_taken = _take_rows(left, left_rows)
        right_taken = _take_rows(right, right_rows)
        names: list[str] = []
        arrays: list[Any] = []
        for name in left.column_names:
            names.append(name)
            arrays.append(left_taken.column(name))
        for name in right.column_names:
            if name not in left.column_names:
                names.append(name)
                arrays.append(right_taken.column(name))
        assert self.sides[0].schema is not None
        assert self.sides[1].schema is not None
        return self._finish(
            pa.Table.from_arrays(arrays, names=names),
            self.sides[0].schema,
            self.sides[1].schema,
        )

    def _note_differing(self, rest: pa.Table, other: _Remaining, side: int) -> None:
        """Note the shared columns in which unmatched rows of one origin row differ.

        One more join, of the arriving rows that matched nothing on the system tags only
        against the other side's remaining rows, pairs the rows of one origin row (their
        repeats in arrival order), with both sides' prepared values of the other shared
        columns. Each such column is then compared over those pairs in one vectorized
        expression, ``eq_missing`` on the prepared values: ``Join``'s equality (NaN
        equals NaN, ``-0.0`` equals ``0.0``, at any depth) with a null equal to a null,
        as the sync path compares them.
        """
        row = _ROW[side]
        names = [name for name in self.others if name not in self.differing]
        if not names:
            return
        origin = [*self.system, _OCCURRENCE]
        mine = _occurrences(polars_key_frame(rest, self.shared, row), self.system)
        theirs = _occurrences(other.frame(), self.system).select(
            [*origin, *(pl.col(name).alias(_OTHER + name) for name in names)]
        )
        pairs = mine.select([*origin, *names]).join(theirs, on=origin, how="inner")
        if not pairs.height:
            return
        differ = pairs.select(
            (~pl.col(name).eq_missing(pl.col(_OTHER + name))).any().alias(name)
            for name in names
        ).row(0, named=True)
        self.differing.update(name for name, differs in differ.items() if differs)


class StreamingRejoin(StreamingJoinKernel):
    """``Rejoin`` on record batches: symmetric matches on every shared column.

    ITL-1355's rule, as ``Rejoin.static_process`` applies it: the inputs are taken in
    ``Join``'s canonical order (by pipeline hash, ties in call order) and rejoined pair by
    pair, the rows rejoined so far with the next input, each step an inner join on every
    column both sides have (system tags, shared user tags, a shared data-tag's source
    info, the data context) under ``Join``'s equality, a null equal to a null outside the
    system tags and a null system tag matching nothing. Each step is a ``_RejoinLink``:

    - each side keeps only its rows not matched yet (a matched row leaves at once), so
      the memory held is the unmatched rows;
    - an arriving batch is matched, in one vectorized join, with the other side's
      remaining rows; rows that repeat an origin row pair one to one in arrival order;
    - once one side closes, the other side's remaining rows are dropped, and an arriving
      row either matches at once or is dropped.

    Cost, per arriving batch and per step it reaches: at most two Polars joins against
    the other side's remaining rows, the match and, when some arriving rows matched
    nothing and the other side holds rows, one more on the system tags for the warning
    (each other shared column then compared in one vectorized expression); and the
    occurrence numbers of the remaining rows. That is O(held rows) per batch, as in the
    ``Join`` kernels, plus O(m log m) to take the m pairs it makes from the held chunks
    (``_take_rows``: one vectorized chunk lookup and one sort for all of them), never by
    copying a whole held table; a compaction copies the remaining rows when half of
    those held were removed. A long run of one-row batches against a large buffer is
    quadratic in the rows.

    Rows: they equal the sync path's as a multiset, for every arrival schedule, when
    origins are unique (system tags unique in each input, as every source makes them),
    and also with repeated origins for two inputs (and at the first step of a chain):
    the k-th repeat of one input pairs with the k-th of the other, in each input's row
    order, as on the sync path. With three or more inputs and repeated origins, the
    pairing of repeats at a later step can depend on arrival and differ from the sync
    path, which ranks the rows rejoined so far in its table order among the rows that
    survived the earlier step. The approved spec names this risk ("Duplicates ... 'match
    once' pairs duplicates in arrival order, which can differ between sync and async
    (risks; ITL-1356 removes the case)"); the developer's rule for ``Rejoin`` is that
    system tags are unique by construction. The exact alternative (option X) is
    survival-aware ranking: per-step counters, and rows of a repeated origin held until
    every earlier row of that origin has a final fate at the step before.

    The data context: a job's rows carry their producer's data context, which ``Rejoin``
    matches on. Transport batches carry none, so each input's rows are given the data
    context of its stream (``StaticOutputOperatorPod._upstream_context``): inputs of two
    data contexts match nothing, and a link holds their rows to the end.

    The warning: rows of one origin row (system tags equal) that differ in another
    shared column are noted per batch, between an arriving row that matched nothing and
    the other side's remaining rows; when the last input closes, the kernel issues
    ``RejoinDifferingColumnWarning`` once for the columns noted, through the operator
    (``Rejoin._warn_differing_once``), as the sync path issues it once for its run.

    - Guaranteed: with two inputs and unique origins, the warning is the sync path's
      (the same columns, once) for every arrival schedule.
    - Best effort otherwise (provisional, November 22, 2026; the rows are never
      affected): the kernel reports only differences between rows the chain actually
      compares, so a difference can be missed, and with repeated origins it can depend
      on arrival. With three or more inputs a step compares the rows rejoined so far
      with the next input: a difference between two inputs for an origin row that an
      input before the later of them lacks is not seen (whether or not the two are
      adjacent in the chain). With repeated origins a repeat already matched is no
      longer compared, while the sync path compares repeats by occurrence whatever
      their matches. The sync path compares every pair of inputs over all their rows;
      the alternative (option B) is pairwise detection, which would keep every input's
      shared-column keys for every row until all inputs close.

    ``Rejoin`` refuses nothing for what rows hold: every refusal it raises for its inputs'
    columns and types comes before the first batch, except the base class's check of an
    input that ends empty, which can refuse at the last close (``StreamingJoinKernel``).

    Args:
        operator: The ``Rejoin``.
        inputs: Its input streams, in call order.
    """

    def __init__(self, operator: Rejoin, inputs: Sequence[StreamProtocol]) -> None:
        super().__init__(operator, inputs)
        self._rejoin = operator
        order = sorted(
            range(len(inputs)), key=lambda i: inputs[i].pipeline_hash().to_hex()
        )
        self._position = {index: position for position, index in enumerate(order)}
        self._contexts = [
            StaticOutputOperatorPod._upstream_context(stream).context_key  # pyright: ignore[reportPrivateUsage]  # the operators' one rule
            for stream in inputs
        ]
        self._user_tags: set[str] = {
            name for stream in inputs for name in stream.keys()[0]
        }
        self._links: list[_RejoinLink] = []
        if len(inputs) == 1:
            return
        layouts = [self._layouts[i] for i in order]
        system = sorted(
            name
            for name in layouts[0].names
            if name.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        left_names = [*layouts[0].names, constants.CONTEXT_KEY]
        user_tags = set(inputs[order[0]].keys()[0])
        for index in order[1:]:
            right_names = [*self._layouts[index].names, constants.CONTEXT_KEY]
            others = sorted(
                name
                for name in right_names
                if name in left_names and name not in system
            )
            next_tags = set(inputs[index].keys()[0])
            self._links.append(
                _RejoinLink(system, others, self._link_finish(user_tags & next_tags))
            )
            left_names = [
                *left_names,
                *(name for name in right_names if name not in left_names),
            ]
            user_tags |= next_tags

    @property
    def held_rows(self) -> int:
        return sum(link.held_rows for link in self._links)

    def _link_finish(
        self, shared_tags: set[str]
    ) -> Callable[[pa.Table, pa.Schema, pa.Schema], pa.Table]:
        """What one step does to its pairs, as ``Rejoin._rejoin_pair`` does."""

        def finish(
            table: pa.Table, left_schema: pa.Schema, right_schema: pa.Schema
        ) -> pa.Table:
            table = arrow_utils.restore_schema_nullability(
                table, left_schema, right_schema
            )
            return Join._shared_key_nullability(  # pyright: ignore[reportPrivateUsage]  # Join's one rule
                table, left_schema, right_schema, shared_tags
            )

        return finish

    def _push(self, input_index: int, table: pa.Table) -> list[pa.Table]:
        """Feed an input's rows into its step and forward what each step emits.

        Args:
            input_index: The input, in call order.
            table: Its rows, never empty.

        Returns:
            The last step's output, possibly none.
        """
        if not self._links:
            return [table]
        table = _with_context(table, self._contexts[input_index])
        position = self._position[input_index]
        step, side = (0, 0) if position == 0 else (position - 1, 1)
        result = self._links[step].push(side, table)
        while result is not None and step < len(self._links) - 1:
            step += 1
            result = self._links[step].push(0, result)
        # A match can take the last row a closed side held: that step is exhausted.
        self._release_exhausted()
        return [] if result is None else [result]

    def _release_exhausted(self) -> None:
        """Release what a step that can never emit again makes useless.

        Such a step (a side closed holding no row, ``_RejoinLink.exhausted``) closes its
        successor's left side, and every earlier step is released: its rows could only
        reach this one. A step is exhausted by a close, or by a match that takes the
        last row a closed side held.
        """
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

    def _close(self, input_index: int) -> list[pa.Table]:
        """End an input's side, every side that can no longer receive, and warn last.

        A step that can never emit again closes its successor's left side and releases
        every earlier step, whose rows could only reach it. When the last input closes,
        the differing columns noted are warned of once.

        Args:
            input_index: The input, in call order.

        Returns:
            No table.
        """
        if not self._links:
            return []
        position = self._position[input_index]
        step, side = (0, 0) if position == 0 else (position - 1, 1)
        self._links[step].close(side)
        self._release_exhausted()
        if all(self._closed):
            differing = {name for link in self._links for name in link.differing}
            if differing:
                self._rejoin._warn_differing_once(  # pyright: ignore[reportPrivateUsage]  # Rejoin's one rule
                    tuple(
                        sorted(_described(name, self._user_tags) for name in differing)
                    )
                )
        return []
