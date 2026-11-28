from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, cast

import numpy as np
import pyarrow as pa

from orcapod.core.operators.meta_columns import WithoutMetaWriter
from orcapod.protocols.core_protocols.streams import (
    RecordBatchInput,
    RecordBatchOutput,
)
from orcapod.channels import (
    ReadableChannel,
    WritableChannel,
)
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.operators.static_output_pod import (
    close_inputs,
    concatenate_batches,
    finish_operator,
)
from orcapod.core.data_tags import stream_data_tags
from orcapod.core.streams import ArrowTableStream
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash
from orcapod.utils import arrow_utils

from orcapod.types import Schema


BATCH_FOLD_LABEL = "batch_v1"
"""Label of the folds of ``Batch``: the folded source id and record id are
``"batch_v1:<hex>"`` (ITL-1154). It names the operation; the procedure is
``arrow_utils.fold_system_tag_values``. Change it only when this operation's fold
would give an existing member list a different value.
"""


class Batch(UnaryOperator):
    """Group consecutive rows into batches of ``batch_size`` rows, list-wrapping them.

    ``Batch`` has no key, so it keeps every tag, list-valued: those lists are what
    tells a batch's members apart.  Every user tag, data and source-info column
    becomes a list with one element per member; system-tag columns fold to a scalar
    digest and gain a ``::{pipeline_hash}`` name suffix.

    A data-tag (ITL-1017, developer decision of November 16, 2026: "Batch
    always simply takes anything and puts into list") is one column, so it becomes
    the same list in both roles and stays a data-tag.  Its source info is one token
    per member, as for any data column: nothing is merged, so no token is folded.

    Args:
        batch_size: Rows per batch; ``0`` puts every row in one batch.
        drop_partial_batch: Drop a last batch with fewer than ``batch_size`` rows.
    """

    # Provisional: its identity is not frozen with data context v1 (ITL-1279).
    PROVISIONAL: ClassVar[bool] = True

    # Data-tags (ITL-1023): handled (ITL-1017).
    DATA_TAGS_ISSUE: ClassVar[str | None] = None

    def __init__(self, batch_size: int = 0, drop_partial_batch: bool = False, **kwargs):
        if batch_size < 0:
            raise ValueError("Batch size must be non-negative.")

        super().__init__(**kwargs)

        self.batch_size = batch_size
        self.drop_partial_batch = drop_partial_batch

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """
        Batch works on any input stream, so no validation is needed.
        """
        return None

    def _batch_sizes(self, row_count: int) -> list[int]:
        """Return the number of members of each batch, in row order.

        Batches are consecutive rows: ``batch_size`` rows each (``0``: every row
        in one batch), a last batch with fewer rows kept unless
        ``drop_partial_batch``.

        Args:
            row_count: The number of input rows.

        Returns:
            One size per batch; their sum is the number of rows kept.
        """
        if row_count == 0:
            return []
        if self.batch_size == 0:
            return [] if self.drop_partial_batch else [row_count]
        full, rest = divmod(row_count, self.batch_size)
        sizes = [self.batch_size] * full
        if rest and not self.drop_partial_batch:
            sizes.append(rest)
        return sizes

    fold_label: ClassVar[str] = BATCH_FOLD_LABEL
    """The label of this operator's folds of the system tags."""

    def fold_members(
        self, table: pa.Table, tag_columns: Sequence[str]
    ) -> list[list[int]]:
        """Return the members of each batch of ``table``, as row numbers in member order.

        Reads only the given table, with the rule batching uses: consecutive rows,
        ``batch_size`` each (``_batch_sizes``). The caller checks a batch by folding
        its members' system tags (``fold_system_tag_values`` with ``fold_label``),
        which must give the batch's folded values: the rows must be in the order the
        batch read them.

        Args:
            table: The input rows, in the order the batch read them.
            tag_columns: The input's user tag columns (unused: a batch has no key).

        Returns:
            One list of row numbers per batch.
        """
        del tag_columns  # a batch is decided by position alone
        out: list[list[int]] = []
        start = 0
        for size in self._batch_sizes(table.num_rows):
            out.append(list(range(start, start + size)))
            start += size
        return out

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Group rows into fixed-size batches, list-wrapping their values.

        Tag and data columns become list-valued; a data-tag, one column,
        becomes the same list in both roles and stays a data-tag.  Source-info
        columns become list-valued too, one element per batch member.  System-tag
        columns are folded to a scalar instead -- record identity hashes them
        directly, so they must not become lists.

        The lists are built from the input's Arrow arrays (the members are
        consecutive rows, so each list is a slice; ITL-1281): every value is the
        input's, bit for bit, for every Arrow type, and every list of a row is
        parallel by member, in the input's row order.

        Args:
            stream: The upstream stream.

        Returns:
            A stream with one row per batch.
        """
        table = stream.as_table(columns={"source": True, "system_tags": True})

        tag_columns, _ = stream.keys()

        sizes = self._batch_sizes(table.num_rows)
        offsets = cast(
            "pa.Int64Array",
            pa.array(
                np.concatenate([[0], np.cumsum(sizes, dtype=np.int64)]), pa.int64()
            ),
        )
        kept = table.slice(0, int(offsets[-1].as_py()))
        bounds = cast(list[int], offsets.to_pylist())
        # Every stream has a data context; StreamProtocol does not declare it.
        type_converter = stream.data_context.type_converter  # pyright: ignore[reportAttributeAccessIssue]

        fields: list[pa.Field[Any]] = []
        arrays: list[pa.Array[Any]] = []
        for field in table.schema:
            column = kept.column(field.name)
            if field.name.startswith(constants.SYSTEM_TAG_PREFIX):
                # Text values, folded per batch; text loses nothing in Python.
                values = cast(list[Any], column.to_pylist())
                fields.append(field)
                arrays.append(
                    pa.array(
                        [
                            arrow_utils.fold_system_tag_values(
                                values[start:end], label=BATCH_FOLD_LABEL
                            )
                            for start, end in zip(bounds[:-1], bounds[1:])
                        ],
                        field.type,
                    )
                )
                continue
            list_type = arrow_utils.aggregated_list_type(
                field.type, type_converter, column=field.name
            )
            fields.append(
                arrow_utils.source_info_field(field.name, list_type)
                if field.name.startswith(constants.SOURCE_PREFIX)
                else pa.field(field.name, list_type, nullable=False)
            )
            arrays.append(arrow_utils.aggregated_list_array(column, offsets, list_type))
        batched_table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))

        n_char = self.orcapod_config.hashing.system_tag_n_char
        batched_table = arrow_utils.append_to_system_tags(
            batched_table, stream.pipeline_hash().to_hex(n_char)
        )

        # A data-tag stays a data-tag: the same list in both roles.
        return ArrowTableStream(
            batched_table,
            tag_columns=tag_columns,
            data_context=stream.data_context,
            data_tags=stream_data_tags(stream),
        )

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Predict the batched output schemas without batching.

        Every user tag, data, and source column becomes ``list[T]``, so a
        data-tag is ``list[T]`` in both halves.  System tag columns keep
        their scalar type and gain a ``::{pipeline_hash}`` name suffix.

        Args:
            stream: The upstream stream.
            columns: Column inclusion config.
            all_info: Include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.
        """
        tag_types, data_types = stream.output_schema(columns=columns, all_info=all_info)
        n_char = self.orcapod_config.hashing.system_tag_n_char
        suffix = stream.pipeline_hash().to_hex(n_char)

        batched_tag_types: dict[str, Any] = {}
        for name, col_type in tag_types.items():
            if name.startswith(constants.SYSTEM_TAG_PREFIX):
                batched_tag_types[f"{name}{constants.BLOCK_SEPARATOR}{suffix}"] = (
                    col_type
                )
            else:
                batched_tag_types[name] = list[col_type]

        batched_data_types = {k: list[v] for k, v in data_types.items()}

        return Schema(batched_tag_types), Schema(batched_data_types)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Streaming batch: emit full batches as they accumulate.

        When ``batch_size > 0``, each group of ``batch_size`` rows is
        materialized and emitted immediately, allowing downstream consumers
        to start processing before all input is consumed.  When
        ``batch_size == 0`` (batch everything), falls back to barrier mode.

        Args:
            inputs: The one readable input channel.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: The input stream's pipeline hash, as a
                one-element sequence. Every rebuilt batch reports it, so the
                folded system-tag names end in it as on the sync path (ITL-906).

        Raises:
            ValueError: If ``input_pipeline_hashes`` does not have one hash per input.
        """
        try:
            output = WithoutMetaWriter(output)
            if len(input_pipeline_hashes) != len(inputs):
                raise ValueError(
                    f"input_pipeline_hashes length ({len(input_pipeline_hashes)}) "
                    f"must match inputs length ({len(inputs)})"
                )
            pipeline_hash = input_pipeline_hashes[0]
            if self.batch_size == 0:
                # Must collect all rows — barrier fallback
                rows = await inputs[0].collect()
                if rows:
                    stream = self._materialize_input_stream(rows, pipeline_hash)
                    result = self.unary_static_process(stream)
                    for tag, data in result.iter_data():
                        await output.send((tag, data))
                return

            batch: list[tuple[TagProtocol, DataProtocol]] = []
            async for tag, data in inputs[0]:
                batch.append((tag, data))
                if len(batch) >= self.batch_size:
                    stream = self._materialize_input_stream(batch, pipeline_hash)
                    result = self.unary_static_process(stream)
                    for out_tag, out_data in result.iter_data():
                        await output.send((out_tag, out_data))
                    batch = []

            # Flush partial batch
            if batch and not self.drop_partial_batch:
                stream = self._materialize_input_stream(batch, pipeline_hash)
                result = self.unary_static_process(stream)
                for out_tag, out_data in result.iter_data():
                    await output.send((out_tag, out_data))
        finally:
            await output.close()

    async def async_execute_batches(
        self, inputs: Sequence[RecordBatchInput], output: RecordBatchOutput
    ) -> None:
        """Batch the rows of an input channel as they arrive.

        The Arrow-record-batch form of ``async_execute`` (ITL-1347). With a
        ``batch_size``, every ``batch_size`` rows that have arrived are batched and
        sent at once, whatever record batches they came in, so a batch holds
        consecutive arrivals, as on the row path; the rows left at the end make the
        last, smaller batch unless ``drop_partial_batch``. With ``batch_size == 0``
        (every row in one batch) all of the input is read first.

        Args:
            inputs: The one input channel.
            output: The channel the batched rows are sent into.
        """
        if self.batch_size == 0:
            await super().async_execute_batches(inputs, output)
            return
        (channel,) = inputs
        try:
            waiting: list[pa.RecordBatch] = []
            rows = 0
            async for batch in channel:
                if not batch.num_rows:
                    continue  # nothing to batch, nothing to hold
                waiting.append(batch)
                rows += batch.num_rows
                full = rows - rows % self.batch_size
                if not full:
                    continue
                # Arrived batches may differ in their provenance fields: put them
                # under one another as a complete table settles them.
                arrived = concatenate_batches(waiting)
                assert arrived is not None  # they hold rows
                await self._send_processed(
                    arrived.slice(0, full).to_batches(), channel.stream, output
                )
                waiting = arrived.slice(full).to_batches()
                rows -= full
            # The rows left over are fewer than one batch: the sync rule decides
            # whether they make a last one.
            if rows:
                await self._send_processed(waiting, channel.stream, output)
        except BaseException as error:
            output.fail(error)
            await close_inputs(inputs, error)
            raise
        await finish_operator(inputs, output)

    def to_config(self) -> dict[str, Any]:
        """Serialize this Batch operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``batch_size`` and ``drop_partial_batch``.
        """
        config = super().to_config()
        config["config"] = {
            "batch_size": self.batch_size,
            "drop_partial_batch": self.drop_partial_batch,
        }
        return config

    def rows_dropped_text(
        self,
        position: int,
        others: Sequence[str],
        inputs: Sequence[StreamProtocol],
    ) -> str | None:
        """What the step drops, for ``explain()``: a last partial batch, if asked."""
        return "a last partial batch is dropped" if self.drop_partial_batch else None

    def identity_structure(self) -> Any:
        self._note_provisional_identity_use()  # a direct read is a use (ITL-1279)
        return (self.__class__.__name__, self.batch_size, self.drop_partial_batch)
