"""The edges of an async job: rows, or Arrow record batches of rows (ITL-1464).

An edge is a ``Channel`` or ``BroadcastChannel`` (``orcapod.channels``), unchanged. What it
carries is ``(Tag, Data)`` pairs, or, from a node that has its rows as Arrow data,
``RowBatch`` items: record batches of the producing node's rows in the row-built form
(``streams.base.row_built_columns``). ``EdgeWriter`` and ``EdgeReader`` are the two ends as
nodes see them. A reader gives whichever form its node asks for, so a node that knows only
pairs is unchanged, and pairs are made only for such a node.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Generator, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

import pyarrow as pa

from orcapod.channels import ChannelClosed
from orcapod.core.data_tags import data_tag_names
from orcapod.core.operators import static_output_pod
from orcapod.core.operators.static_output_pod import (
    BatchSpool,
    StaticOutputOperatorPod,
    keys_in_delivered_order,
    rows_once,
    stream_from_batches,
)
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import row_built_columns
from orcapod.system_constants import constants
from orcapod.utils import source_tokens

if TYPE_CHECKING:
    from orcapod.channels import ReadableChannel, WritableChannel
    from orcapod.protocols.core_protocols import (
        DataProtocol,
        StreamProtocol,
        TagProtocol,
    )

_PROVENANCE = (constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX)

EDGE_BATCHES = 4
"""How many items wait on an edge whose producer sends record batches."""

REFILL_TURNS = 3
"""Turns a reader gives its producer before it takes fewer rows than a full batch."""


@dataclass(frozen=True)
class RowBatch:
    """Some rows of ``stream`` as one Arrow record batch, in order.

    Attributes:
        batch: The rows (``row_built_columns``).
        stream: The producing node: the description.
    """

    batch: pa.RecordBatch
    stream: StreamProtocol

    def rows(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """The rows as pairs, in order."""
        return rows_once(stream_from_batches(self.stream, [self.batch]))


@runtime_checkable
class BatchWriter(Protocol):
    """A writer that takes record batches too."""

    async def send_batch(self, batch: pa.RecordBatch, stream: StreamProtocol) -> None:
        """Send record batches of ``stream``'s rows."""
        ...


class EdgeWriter:
    """The producing end of an edge.

    Args:
        writer: The channel's writer.
    """

    def __init__(self, writer: WritableChannel[Any]) -> None:
        self._writer: WritableChannel[Any] = writer

    async def send(self, item: tuple[TagProtocol, DataProtocol]) -> None:
        """Send one row."""
        await self._writer.send(item)

    async def send_batch(self, batch: pa.RecordBatch, stream: StreamProtocol) -> None:
        """Send a record batch of ``stream``'s rows, cut to ``ROW_CHUNK_ROWS`` rows each."""
        for start in range(0, batch.num_rows, static_output_pod.ROW_CHUNK_ROWS):
            await self._writer.send(
                RowBatch(batch.slice(start, static_output_pod.ROW_CHUNK_ROWS), stream)
            )

    async def close(self) -> None:
        """Signal that no more items will be sent."""
        await self._writer.close()


async def send_batch(
    output: WritableChannel[tuple[TagProtocol, DataProtocol]],
    batch: pa.RecordBatch,
    stream: StreamProtocol,
) -> None:
    """Send a record batch of ``stream``'s rows into ``output``, as pairs if it must.

    Args:
        output: A node's output: an ``EdgeWriter``, or any writer of pairs.
        batch: The rows (``row_built_columns``).
        stream: The producing node.
    """
    if isinstance(output, BatchWriter):
        await output.send_batch(batch, stream)
        return
    for start in range(0, batch.num_rows, static_output_pod.ROW_CHUNK_ROWS):
        for row in RowBatch(
            batch.slice(start, static_output_pod.ROW_CHUNK_ROWS), stream
        ).rows():
            await output.send(row)


class EdgeReader:
    """The consuming end of an edge, read as pairs or as record batches.

    Args:
        reader: The channel's reader.
    """

    def __init__(self, reader: ReadableChannel[Any]) -> None:
        self._reader: ReadableChannel[Any] = reader
        self._rows: Iterator[tuple[TagProtocol, DataProtocol]] | None = None

    async def receive(self) -> tuple[TagProtocol, DataProtocol]:
        """The next row as a pair."""
        while True:
            if self._rows is not None:
                row = next(self._rows, None)
                if row is not None:
                    return row
                self._rows = None
            item = await self._reader.receive()
            if not isinstance(item, RowBatch):
                return cast("tuple[TagProtocol, DataProtocol]", item)
            self._rows = item.rows()

    def __aiter__(self) -> AsyncIterator[tuple[TagProtocol, DataProtocol]]:
        return self

    async def __anext__(self) -> tuple[TagProtocol, DataProtocol]:
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration from None

    async def collect(self) -> list[tuple[TagProtocol, DataProtocol]]:
        """Every remaining row as a pair."""
        return [row async for row in self]

    async def discard(self) -> None:
        """Read to the end and let every row go, making no pair."""
        self._rows = None
        async for _ in self._reader:
            pass


class EdgeFeed:
    """The record batches of an edge, for an input channel (``BatchFeed``).

    Pairs are gathered into record batches of at most ``ROW_CHUNK_ROWS`` rows, built as
    the table of those rows, without waiting for rows a producer has not sent yet.
    A failed producer ends the edge as it ends a channel of pairs: the run's task group
    cancels this reader.

    Args:
        reader: The edge, or any reader of pairs.
        stream: The node the rows are of.
    """

    def __init__(
        self,
        reader: ReadableChannel[tuple[TagProtocol, DataProtocol]],
        stream: StreamProtocol,
    ) -> None:
        self._source: ReadableChannel[Any] = (
            reader._reader if isinstance(reader, EdgeReader) else reader  # pyright: ignore[reportPrivateUsage]  # the edge's own reader
        )
        self._stream: StreamProtocol = stream
        self._held: RowBatch | None = None
        self._ended: bool = False

    async def next(self) -> pa.RecordBatch | None:
        """The next record batch, or ``None`` at the end."""
        if self._held is not None:
            held, self._held = self._held, None
            return held.batch
        if self._ended:
            return None
        rows: list[tuple[TagProtocol, DataProtocol]] = []
        while len(rows) < static_output_pod.ROW_CHUNK_ROWS:
            try:
                item = await (self._soon() if rows else self._source.receive())
            except ChannelClosed:
                self._ended = True
                break
            if item is None:
                break
            if isinstance(item, RowBatch):
                if not rows:
                    return item.batch
                self._held = item
                break
            rows.append(cast("tuple[TagProtocol, DataProtocol]", item))
        if not rows:
            return None
        table, _, _ = StaticOutputOperatorPod._rows_to_table(rows)  # pyright: ignore[reportPrivateUsage]  # the table of rows
        (batch,) = table.combine_chunks().to_batches()
        return batch

    async def _soon(self) -> Any:  # pyright: ignore[reportExplicitAny]  # a pair or a RowBatch
        """The next item if it is there within a few turns of the loop, else ``None``."""
        queue: asyncio.Queue[Any] | None = getattr(self._source, "_queue", None)
        if queue is None:
            queue = getattr(getattr(self._source, "_channel", None), "_queue", None)
        if queue is None:
            # Not a library channel: what it holds cannot be seen without waiting.
            return None
        for _ in range(REFILL_TURNS):
            if not queue.empty():
                return await self._source.receive()
            await asyncio.sleep(0)
        return None

    def close(self) -> None:
        """Stop reading."""
        self._ended = True
        self._held = None

    @property
    def opened_on(self) -> StreamProtocol | None:
        """``None``: the rows are what a producer sends."""
        return None


class KeptBatches:
    """An operator job node's output, received as record batches (ITL-1464).

    Holds them in a ``BatchSpool`` and gives, at the end, the table a run that handed on
    rows keeps (``StaticOutputOperatorPod._row_built_form``: the steps decided over every
    row). Every batch must have passed the node's output channel.

    Args:
        node: The node the rows are of.
    """

    def __init__(self, node: StreamProtocol) -> None:
        self._node: StreamProtocol = node
        # The node's keys, in the order its rows have them; asked once.
        self._keys: tuple[list[str], list[str]] | None = None
        self._spool: BatchSpool = BatchSpool()

    @property
    def num_rows(self) -> int:
        """How many rows were kept."""
        return self._spool.num_rows

    def close(self) -> None:
        """Let the kept rows go; an output stream already given stays valid."""
        self._spool.close()

    def add(self, batch: pa.RecordBatch) -> None:
        """Keep a batch's rows after every row kept before them."""
        if not batch.num_rows:
            return
        if self._keys is None:
            self._keys = keys_in_delivered_order(self._node.keys(), batch.schema.names)
        self._spool.add(batch)

    def output_stream(self) -> StreamProtocol:
        """The stream over the rows that the node keeps and stores.

        Raises:
            ValueError: If no row was kept.
        """
        if self._keys is None:
            raise ValueError("Cannot make an output stream of no rows")
        # The columns in the order of a table built from rows (a join lists its data
        # before its system tags), and F's last step.
        rows = self._spool.table()
        assert rows is not None  # a row was kept
        table = StaticOutputOperatorPod._flag_by_values(  # pyright: ignore[reportPrivateUsage]  # F's last step
            row_built_columns(rows, self._keys)
        )
        tag_keys, data_keys = self._keys
        return ArrowTableStream(
            table,
            tag_columns=tag_keys,
            data_tags=data_tag_names(tag_keys, data_keys),
        )


class TappedFeed:
    """A feed that remembers the columns of the rows it gave, and no row.

    For a run with no output row: the node rebuilds each input with no rows, as the sync
    path rebuilds it (``_materialize_upstream(zero_rows=True)``); what depends on the rows
    there is the columns, and whether a system-tag or source-info value was missing.

    Args:
        feed: The feed.
    """

    def __init__(self, feed: EdgeFeed) -> None:
        self._feed: EdgeFeed = feed
        self._first: pa.Table | None = None
        self._nulls: set[str] = set()

    async def next(self) -> pa.RecordBatch | None:
        """The feed's next record batch."""
        batch = await self._feed.next()
        if batch is not None and batch.num_rows:
            if self._first is None:
                self._first = pa.Table.from_batches([batch.slice(0, 0)])
            self._nulls.update(
                field.name
                for field, column in zip(batch.schema, batch.columns, strict=True)
                if field.name.startswith(_PROVENANCE)
                and column.null_count
                and not source_tokens.is_derived(field.type)
            )
        return batch

    def close(self) -> None:
        """Stop reading."""
        self._feed.close()

    @property
    def opened_on(self) -> StreamProtocol | None:
        """``None``, as the feed's."""
        return None

    def zero_rows(self, upstream: StreamProtocol) -> StreamProtocol:
        """The input with no rows, as ``upstream``'s stream."""
        pod = StaticOutputOperatorPod
        if self._first is None:
            return pod._materialize_upstream([], upstream, zero_rows=True)  # pyright: ignore[reportPrivateUsage]  # the zero-row input
        table = source_tokens.materialize(self._first)
        fields = [
            f.with_nullable(f.name in self._nulls)
            if f.name.startswith(_PROVENANCE)
            else f
            for f in table.schema
        ]
        table = pa.Table.from_arrays(table.columns, schema=pa.schema(fields))
        tag_keys, data_keys = keys_in_delivered_order(
            upstream.keys(), table.column_names
        )
        return pod._as_upstream_stream(  # pyright: ignore[reportPrivateUsage]  # the zero-row input
            table, tag_keys, data_tag_names(tag_keys, data_keys), upstream
        )


async def off_the_loop(
    batches: Generator[pa.RecordBatch, None, None],
) -> AsyncIterator[pa.RecordBatch]:
    """Iterate ``batches``, each made on one thread of its own, never on the event loop.

    The thread owns the generator for its whole life: when the consumer stops or is
    cancelled, it closes the generator after the batch in progress.

    Args:
        batches: What makes the record batches.
    """
    owner = ThreadPoolExecutor(max_workers=1, thread_name_prefix="source-read")
    loop = asyncio.get_running_loop()
    try:
        while True:
            batch = await loop.run_in_executor(owner, next, batches, None)
            if batch is None:
                break
            yield batch
    finally:
        _ = owner.submit(batches.close)
        owner.shutdown(wait=False)
