"""Async channel primitives for push-based pipeline execution.

Provides bounded async channels with close/done signaling, backpressure,
and fan-out (broadcast) support.

Channels of Arrow record batches (ITL-1347): ``InputChannel`` and ``OutputChannel`` are
the forms of ``ReadableChannel`` and ``WritableChannel`` that carry a stream's rows as
Arrow record batches. The stream is primary and owns the description (keys, schemas,
data context, pipeline hash); a channel refers to its stream and restates nothing. An
input channel is opened from a stream (``channel_from_stream``, ``stream.open_channel()``)
or from an output channel (``OutputChannel.open_reader``); an output channel, once
closed, gives its rows back as a stream (``OutputChannel.result``). Every conversion
between streams and channels is in this module.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, runtime_checkable

import pyarrow as pa

from orcapod.errors import UpstreamFailedError

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import StreamProtocol

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


# ---------------------------------------------------------------------------
# Sentinel & exception
# ---------------------------------------------------------------------------


class _Sentinel:
    """Internal marker signaling channel closure."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<channel-closed>"


_CLOSED = _Sentinel()


class ChannelClosed(Exception):
    """Raised when ``receive()`` is called on a closed, drained channel."""


# ---------------------------------------------------------------------------
# Protocol types
# ---------------------------------------------------------------------------


@runtime_checkable
class ReadableChannel(Protocol[T_co]):
    """Consumer side of a channel."""

    async def receive(self) -> T_co:
        """Receive next item. Raises ``ChannelClosed`` when done."""
        ...

    def __aiter__(self) -> AsyncIterator[T_co]: ...

    async def __anext__(self) -> T_co: ...

    async def collect(self) -> list[T_co]:
        """Drain all remaining items into a list."""
        ...


@runtime_checkable
class WritableChannel(Protocol[T_contra]):
    """Producer side of a channel."""

    async def send(self, item: T_contra) -> None:
        """Send an item. Blocks if channel buffer is full (backpressure)."""
        ...

    async def close(self) -> None:
        """Signal that no more items will be sent."""
        ...


# ---------------------------------------------------------------------------
# Concrete reader / writer views
# ---------------------------------------------------------------------------


class _ChannelReader(Generic[T]):
    """Concrete ReadableChannel backed by a Channel.

    ``Channel`` is intentionally single-consumer.  Fan-out (multiple readers)
    is handled by ``BroadcastChannel``, which gives every reader its own
    independent queue.  To avoid deadlocks with small queue buffers we avoid
    *awaiting* any sentinel re-enqueue: once the closure sentinel has been
    consumed the channel-level ``_drained`` flag is set, and a best-effort
    non-blocking re-enqueue of the sentinel is used only to wake any other
    coroutines already suspended in ``queue.get()``.  After ``_drained`` is
    set, any subsequent ``receive()`` call (even from a freshly created
    reader) raises ``ChannelClosed`` immediately without touching the queue.
    """

    __slots__ = ("_channel",)

    def __init__(self, channel: Channel[T]) -> None:
        self._channel = channel

    async def receive(self) -> T:
        if self._channel._drained:
            raise ChannelClosed()
        if self._channel._close_pending and self._channel._queue.empty():
            # Closed while the queue was full: every item is received; the end now.
            self._channel._drained = True
            raise ChannelClosed()
        item = await self._channel._queue.get()
        if isinstance(item, _Sentinel):
            self._channel._drained = True
            # Wake any other coroutine already suspended in queue.get().
            # put_nowait succeeds immediately because the queue just lost an
            # item; if the queue is somehow full (shouldn't happen once closed)
            # we silently skip — the _drained flag will cover those callers.
            try:
                self._channel._queue.put_nowait(_CLOSED)
            except asyncio.QueueFull:
                pass
            raise ChannelClosed()
        return item  # type: ignore[return-value]

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration

    async def collect(self) -> list[T]:
        items: list[T] = []
        async for item in self:
            items.append(item)
        return items


class _ChannelWriter(Generic[T]):
    """Concrete WritableChannel backed by a Channel."""

    __slots__ = ("_channel",)

    def __init__(self, channel: Channel[T]) -> None:
        self._channel = channel

    async def send(self, item: T) -> None:
        if self._channel._closed.is_set():
            raise ChannelClosed("Cannot send to a closed channel")
        await self._channel._queue.put(item)

    async def close(self) -> None:
        """Signal that no more items will be sent, without waiting.

        Never blocks (ITL-932): on a failed run every reader is cancelled while
        writers close their outputs in ``finally`` blocks, and a close that waited
        for room in a full queue would hang the run. When the queue is full, the
        close is recorded instead of queued; readers receive every queued item and
        then ``ChannelClosed``, exactly as with a queued close.
        """
        if not self._channel._closed.is_set():
            self._channel._closed.set()
            try:
                self._channel._queue.put_nowait(_CLOSED)
            except asyncio.QueueFull:
                self._channel._close_pending = True


# ---------------------------------------------------------------------------
# Channel
# ---------------------------------------------------------------------------


@dataclass
class Channel(Generic[T]):
    """Bounded async channel with close/done signaling.

    Args:
        buffer_size: Maximum number of items that can be buffered.
            Defaults to 64.
    """

    buffer_size: int = 64
    _queue: asyncio.Queue[T | _Sentinel] = field(init=False)
    _closed: asyncio.Event = field(init=False, default_factory=asyncio.Event)
    _drained: bool = field(init=False, default=False)
    # Closed while the queue was full: the close is not queued (ITL-932).
    _close_pending: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self._queue = asyncio.Queue(maxsize=self.buffer_size)

    @property
    def reader(self) -> _ChannelReader[T]:
        """Return a readable view of this channel."""
        return _ChannelReader(self)

    @property
    def writer(self) -> _ChannelWriter[T]:
        """Return a writable view of this channel."""
        return _ChannelWriter(self)


# ---------------------------------------------------------------------------
# Broadcast channel (fan-out)
# ---------------------------------------------------------------------------


class _BroadcastReader(Generic[T]):
    """A reader that receives items broadcast from a shared source.

    Each broadcast reader maintains its own independent queue so that
    multiple downstream consumers can read at their own pace.
    """

    __slots__ = ("_closed", "_queue")

    def __init__(self, buffer_size: int) -> None:
        self._queue: asyncio.Queue[T | _Sentinel] = asyncio.Queue(maxsize=buffer_size)
        # Set once this reader's stream has ended, or by a close that found its
        # queue full (ITL-932): then the end comes once the queue is drained.
        self._closed = False

    async def receive(self) -> T:
        if self._closed and self._queue.empty():
            raise ChannelClosed()
        item = await self._queue.get()
        if isinstance(item, _Sentinel):
            # Remember the end so repeated receive() calls also raise.
            self._closed = True
            raise ChannelClosed()
        return item  # type: ignore[return-value]

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration

    async def collect(self) -> list[T]:
        items: list[T] = []
        async for item in self:
            items.append(item)
        return items


class BroadcastChannel(Generic[T]):
    """A channel whose output is broadcast to multiple readers.

    Each call to ``add_reader()`` creates an independent reader queue.
    Items sent via the writer are copied to every reader's queue.

    Args:
        buffer_size: Per-reader buffer size. Defaults to 64.
    """

    def __init__(self, buffer_size: int = 64) -> None:
        self._buffer_size = buffer_size
        self._readers: list[_BroadcastReader[T]] = []
        self._closed = False

    def add_reader(self) -> _BroadcastReader[T]:
        """Create and return a new reader for this broadcast channel."""
        reader = _BroadcastReader[T](self._buffer_size)
        self._readers.append(reader)
        return reader

    @property
    def writer(self) -> _BroadcastWriter[T]:
        """Return a writable view of this broadcast channel."""
        return _BroadcastWriter(self)


class _BroadcastWriter(Generic[T]):
    """Writer that fans out items to all broadcast readers."""

    __slots__ = ("_broadcast",)

    def __init__(self, broadcast: BroadcastChannel[T]) -> None:
        self._broadcast = broadcast

    async def send(self, item: T) -> None:
        if self._broadcast._closed:
            raise ChannelClosed("Cannot send to a closed channel")
        for reader in self._broadcast._readers:
            await reader._queue.put(item)

    async def close(self) -> None:
        """Signal every reader that no more items will be sent, without waiting.

        Never blocks on a slow or cancelled reader (ITL-932); see
        ``_ChannelWriter.close``.
        """
        if not self._broadcast._closed:
            self._broadcast._closed = True
            for reader in self._broadcast._readers:
                try:
                    reader._queue.put_nowait(_CLOSED)
                except asyncio.QueueFull:
                    reader._closed = True


# ---------------------------------------------------------------------------
# Channels of Arrow record batches (ITL-1347)
# ---------------------------------------------------------------------------

SOURCE_BATCH_ROWS = 65_536
"""The most rows of one Arrow record batch a complete stream is read in.

Developer decision, November 22, 2026 (ITL-1347, question 6): a source sends record
batches of at most 65,536 rows; an operator sends its result's own record batches.
"""

BATCH_BUFFER_SIZE = 8
"""How many Arrow record batches an input channel queues before its producer waits."""


def transport_columns(
    table: pa.Table, keys: tuple[Sequence[str], Sequence[str]]
) -> pa.Table:
    """Return ``table`` as the columns passed between nodes, for a channel.

    The transport columns of a stream are what a table built from its rows holds: user
    tags, system tags, data and source info, in that order, with no data-context and no
    meta (``__``) column, and ``null`` as ``large_string`` (a row's datagram stores a
    ``null`` column so). The source info follows the stream's data keys, a data-tag's
    first, as each row lists it. No value is changed.

    One property is left for the complete table: whether a source-info field is
    nullable follows its tokens, so it cannot be decided on one record batch of many.
    Here every source-info field is nullable, so all record batches of one channel have
    one schema; ``stream_from_batches`` sets the flags over all of them.

    Args:
        table: A stream's table with its system tags and source info
            (``ZERO_ROW_INPUT_COLUMNS``), of any number of rows.
        keys: The stream's tag keys and data keys (``stream.keys()``).

    Returns:
        The transport columns, in row order.
    """
    # TODO(ITL-1360): this is steps 1 to 4 of the row-built form of the design
    # (docs/dev/specs/itl-1347-job-table-transport.md); stage 2 defines that form once,
    # and this function then calls it.
    from orcapod.system_constants import constants
    from orcapod.utils import arrow_utils

    tag_keys, data_keys = keys
    names = table.column_names
    system_tags = [n for n in names if n.startswith(constants.SYSTEM_TAG_PREFIX)]
    source_info = [n for n in names if n.startswith(constants.SOURCE_PREFIX)]
    # A row lists its source info in the order of its data columns.
    by_data_key = [f"{constants.SOURCE_PREFIX}{name}" for name in data_keys]
    source_info = [n for n in by_data_key if n in source_info] + [
        n
        for n in source_info
        if n not in by_data_key
        and not n.removeprefix(constants.SOURCE_PREFIX).startswith(
            constants.META_PREFIX
        )
    ]
    placed = {*tag_keys, *system_tags, *source_info, constants.CONTEXT_KEY}
    data = [
        n
        for n in names
        if n not in placed
        and not n.startswith((constants.META_PREFIX, constants.SOURCE_PREFIX))
    ]
    kept = arrow_utils.normalize_table_to_large_types(
        table.select([*tag_keys, *system_tags, *data, *source_info])
    )
    schema = pa.schema(
        [
            column.with_nullable(True)
            if column.name.startswith(constants.SOURCE_PREFIX)
            else column
            for column in kept.schema
        ]
    )
    return pa.Table.from_arrays(kept.columns, schema=schema)


def _declared_transport_schema(stream: StreamProtocol) -> pa.Schema | None:
    """Return the schema ``stream``'s record batches have, from its description.

    Never computes, reads or polls a row: it is the schema of the stream's zero-row
    input, as its record batches carry it (``transport_columns``).

    Args:
        stream: The describing stream.

    Returns:
        The schema, or ``None`` for a stream that cannot say its Arrow types without
        rows (an unbound input slot, a source that has not polled yet).
    """
    # TODO(ITL-1360): ``stream.arrow_schema()``, once every stream has it.
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
    from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS

    typed = StaticOutputOperatorPod._zero_row_input(stream)  # pyright: ignore[reportPrivateUsage]  # the zero-row form of an input
    if typed is None:
        return None
    return transport_columns(
        typed.as_table(columns=ZERO_ROW_INPUT_COLUMNS), stream.keys()
    ).schema


def stream_from_batches(
    stream: StreamProtocol, batches: Sequence[pa.RecordBatch]
) -> StreamProtocol:
    """Return the Arrow record batches of ``stream``'s rows as a stream.

    The one place a stream is made from Arrow record batches: what an output channel
    gives once closed (``OutputChannel.result``) and what an input channel delivered
    (``InputChannel.to_stream``). The batches are put under one another in the order
    given, each system-tag and source-info field is flagged nullable only when one of
    its values is null (decided over the complete table, never per batch), and the
    table is wrapped with ``stream``'s identity: its producer, upstreams, pipeline hash and data
    context. With no batch it is ``stream``'s zero-row input, typed by its description
    and never by rows.

    Args:
        stream: The describing stream: the node or stream the rows are of.
        batches: Its rows as transport columns (``transport_columns``), in order.

    Returns:
        A complete, re-readable stream over the rows.
    """
    from orcapod.core.data_tags import stream_data_tags
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod

    if not batches:
        return StaticOutputOperatorPod._materialize_upstream([], stream)  # pyright: ignore[reportPrivateUsage]  # the zero-row input of an operator
    table = _flagged_by_values(pa.Table.from_batches(list(batches)))
    return StaticOutputOperatorPod._as_upstream_stream(  # pyright: ignore[reportPrivateUsage]  # the identity of a successor's input
        table, stream.keys()[0], stream_data_tags(stream), stream
    )


def _flagged_by_values(table: pa.Table) -> pa.Table:
    """Flag each system-tag and source-info field nullable only when it holds a null.

    What a table built from rows has: a row states its system tags and its source-info
    tokens as values, so those fields are non-nullable unless a value is missing. It is
    decided over the complete table, never per record batch (two batches of one
    channel would then have two schemas). The same arrays under the new flags.

    Args:
        table: The complete table of a stream's transport columns.

    Returns:
        The table with those fields' flags set by their values.
    """
    from orcapod.system_constants import constants

    by_values = (constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX)
    schema = pa.schema(
        [
            column.with_nullable(table.column(i).null_count > 0)
            if column.name.startswith(by_values)
            else column
            for i, column in enumerate(table.schema)
        ]
    )
    return pa.Table.from_arrays(table.columns, schema=schema)


class _BatchFeed(Protocol):
    """Where an input channel's record batches come from."""

    async def next(self) -> pa.RecordBatch | None:
        """Return the next record batch, or ``None`` at the end."""
        ...

    def close(self) -> None:
        """Stop reading: nothing waits for this reader any more."""
        ...


class _StreamFeed:
    """The record batches of a complete stream, read at this reader's own position."""

    def __init__(self, stream: StreamProtocol) -> None:
        self._stream = stream
        self._batches: deque[pa.RecordBatch] | None = None

    async def next(self) -> pa.RecordBatch | None:
        if self._batches is None:
            # On the first read, not when the channel is opened: opening computes
            # nothing.
            from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS

            table = transport_columns(
                self._stream.as_table(columns=ZERO_ROW_INPUT_COLUMNS),
                self._stream.keys(),
            )
            self._batches = deque(
                batch
                for batch in table.to_batches(max_chunksize=SOURCE_BATCH_ROWS)
                if batch.num_rows
            )
        return self._batches.popleft() if self._batches else None

    def close(self) -> None:
        self._batches = deque()


class _OutputFeed:
    """One reader of an output channel, with its own position."""

    def __init__(self, output: OutputChannel) -> None:
        self._output = output
        self.open = True
        # With retained batches: this reader's position among them. Without: the
        # batches sent and not yet read by this reader.
        self.position = 0
        self.queue: deque[pa.RecordBatch] = deque()

    async def next(self) -> pa.RecordBatch | None:
        output = self._output
        while self.open:
            if output._error is not None:  # pyright: ignore[reportPrivateUsage]
                raise output._upstream_failed()  # pyright: ignore[reportPrivateUsage]
            if output._retain:  # pyright: ignore[reportPrivateUsage]
                if self.position < len(output._batches):  # pyright: ignore[reportPrivateUsage]
                    self.position += 1
                    return output._batches[self.position - 1]  # pyright: ignore[reportPrivateUsage]
            elif self.queue:
                batch = self.queue.popleft()
                output._notify()  # pyright: ignore[reportPrivateUsage]  # room for the producer
                return batch
            if output._closed:  # pyright: ignore[reportPrivateUsage]
                return None
            await output._changed.wait()  # pyright: ignore[reportPrivateUsage]
        return None

    def close(self) -> None:
        self.open = False
        self.queue.clear()
        self._output._notify()  # pyright: ignore[reportPrivateUsage]  # the producer stops waiting


class InputChannel:
    """A single-pass, back-pressured reader of a stream's Arrow record batches.

    The Arrow-record-batch form of ``ReadableChannel``. The description of what it
    delivers is its ``stream``, known before the first batch: the producing node, or
    the stream it was opened from. A channel has no identity of its own and is read
    once; to read again, go back to a stream (the one it was opened from, or the one
    ``to_stream`` gives).

    Made by ``channel_from_stream`` (``stream.open_channel()``) or by
    ``OutputChannel.open_reader``, never directly.
    """

    def __init__(self, stream: StreamProtocol, feed: _BatchFeed) -> None:
        self._stream = stream
        self._feed = feed
        self._received = 0
        self._ended = False

    @property
    def stream(self) -> StreamProtocol:
        """The stream whose rows this channel delivers: the description."""
        return self._stream

    async def receive(self) -> pa.RecordBatch:
        """Return the next Arrow record batch.

        Raises:
            ChannelClosed: At the end, and after ``close``.
            UpstreamFailedError: If the producer failed or was cancelled.
        """
        if self._ended:
            raise ChannelClosed()
        batch = await self._feed.next()
        if batch is None:
            self._ended = True
            raise ChannelClosed()
        self._received += 1
        return batch

    def __aiter__(self) -> AsyncIterator[pa.RecordBatch]:
        return self

    async def __anext__(self) -> pa.RecordBatch:
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration

    async def collect(self) -> list[pa.RecordBatch]:
        """Read to the end and return every remaining record batch, in arrival order."""
        return [batch async for batch in self]

    async def to_stream(self) -> StreamProtocol:
        """Read to the end and return what the channel delivered, as a stream.

        The stream holds the record batches in arrival order, with the identity of
        ``stream`` (``stream_from_batches``). It is how an operator that needs all of
        an input reads it. Called on a channel not read yet: a stream of part of an
        input is never made.

        Raises:
            RuntimeError: If a record batch has already been received.
            UpstreamFailedError: If the producer failed or was cancelled.
        """
        if self._received:
            raise RuntimeError(
                "to_stream() gives everything an input channel delivers, as a stream; "
                + f"{self._received} record batch(es) of this one have been received "
                + "already."
            )
        return stream_from_batches(self._stream, await self.collect())

    async def close(self) -> None:
        """Stop reading. The producer no longer waits for this reader."""
        self._ended = True
        self._feed.close()


class OutputChannel:
    """The producer side of a node's async output, in Arrow record batches.

    The Arrow-record-batch form of ``WritableChannel``. Its ``stream`` is the producing
    node, or the stream the output is of: the description of every record batch sent.
    Each consumer reads through its own input channel (``open_reader``), which is the
    fan-out. Once closed, ``result`` gives the output as an ordinary stream.

    Args:
        stream: The describing stream.
        retain: Keep every record batch sent, so that ``result`` can give the output
            stream. Readers then read the kept batches at their own position, a reader
            may be opened at any time, and ``send`` never waits. Without it, ``send``
            waits until every reader has room, so the slowest reader sets the pace,
            and ``result`` raises.
        buffer_size: Without ``retain``, how many record batches each reader queues
            before ``send`` waits.
    """

    def __init__(
        self,
        stream: StreamProtocol,
        *,
        retain: bool = True,
        buffer_size: int = BATCH_BUFFER_SIZE,
    ) -> None:
        if buffer_size < 1:
            raise ValueError(f"buffer_size must be at least 1, got {buffer_size}")
        self._stream = stream
        self._retain = retain
        self._buffer_size = buffer_size
        self._batches: list[pa.RecordBatch] = []
        self._readers: list[_OutputFeed] = []
        self._sent = 0
        self._closed = False
        self._error: BaseException | None = None
        self._result: StreamProtocol | None = None
        # The schema every record batch must have: the description's, or the first
        # batch's when the stream cannot say its Arrow types without rows.
        self._schema: pa.Schema | None = None
        self._schema_known = False
        self._changed = asyncio.Event()

    @property
    def stream(self) -> StreamProtocol:
        """The stream whose rows are written here: the description."""
        return self._stream

    def _notify(self) -> None:
        """Wake everything waiting on this channel: readers and a waiting ``send``."""
        changed, self._changed = self._changed, asyncio.Event()
        changed.set()

    def _upstream_failed(self) -> UpstreamFailedError:
        error = self._error
        assert error is not None
        failure = UpstreamFailedError(
            f"The input from {_named(self._stream)} did not finish: its producer "
            + f"failed with {type(error).__name__}: {error}"
        )
        failure.__cause__ = error
        return failure

    def _check_schema(self, batch: pa.RecordBatch) -> None:
        """Raise unless ``batch`` has the schema of this channel's record batches."""
        if not self._schema_known:
            self._schema = _declared_transport_schema(self._stream)
            self._schema_known = True
        if self._schema is None:
            self._schema = batch.schema
        mismatch = _schema_mismatch(self._schema, batch.schema)
        if mismatch is not None:
            raise ValueError(
                f"A record batch sent as {_named(self._stream)} does not have that "
                + f"stream's columns: {mismatch}."
            )

    async def send(self, batch: pa.RecordBatch) -> None:
        """Send one Arrow record batch of the stream's transport columns.

        Raises:
            ChannelClosed: If the channel is closed or failed.
            ValueError: If the batch's schema is not the stream's.
        """
        if self._closed or self._error is not None:
            raise ChannelClosed("Cannot send to a closed channel")
        self._check_schema(batch)
        self._sent += 1
        if self._retain:
            self._batches.append(batch)
            self._notify()
            return
        for reader in list(self._readers):
            # The slowest reader sets the pace; one that closed is not waited for.
            while reader.open and len(reader.queue) >= self._buffer_size:
                await self._changed.wait()
            if reader.open:
                reader.queue.append(batch)
                # At once, not after the last reader: this reader may be waiting for
                # the batch while the send waits for another reader's room.
                self._notify()

    async def close(self) -> None:
        """Complete the output. A second close does nothing; never waits."""
        if not self._closed and self._error is None:
            self._closed = True
            self._notify()

    def fail(self, error: BaseException) -> None:
        """End the output with the producer's failure or cancellation.

        Every reader's next read raises ``UpstreamFailedError`` (its cause is
        ``error``), never a normal end. The failure is seen at once: record batches a
        reader has not yet read are not delivered first, so no consumer acts on part of
        a failed output. Does nothing on a channel already closed or failed.

        Args:
            error: The producer's error.
        """
        if not self._closed and self._error is None:
            self._error = error
            self._notify()

    def open_reader(self) -> InputChannel:
        """Open an input channel for one consumer, with its own position.

        Raises:
            ValueError: Without ``retain``, once a record batch has been sent: the
                reader would miss it.
        """
        if not self._retain and self._sent:
            raise ValueError(
                f"A reader of {_named(self._stream)} must be opened before the first "
                + "record batch is sent: this output channel does not retain what it "
                + "sent (retain=False)."
            )
        feed = _OutputFeed(self)
        if not self._retain:
            self._readers.append(feed)
        return InputChannel(self._stream, feed)

    def result(self) -> StreamProtocol:
        """Return the output as a stream, once the channel is closed.

        The concatenation of the record batches sent, in the order sent, with the
        description and identity of ``stream`` (``stream_from_batches``); the same
        object on every call.

        Raises:
            RuntimeError: If the channel is not closed, failed, or does not retain.
        """
        if self._error is not None:
            raise RuntimeError(
                f"The output of {_named(self._stream)} has no result: its producer "
                + f"failed with {type(self._error).__name__}: {self._error}"
            ) from self._error
        if not self._retain:
            raise RuntimeError(
                f"The output of {_named(self._stream)} was not retained "
                + "(retain=False), so it has no result stream."
            )
        if not self._closed:
            raise RuntimeError(
                f"The output of {_named(self._stream)} is not closed yet; its result "
                + "is a stream only once it is complete."
            )
        if self._result is None:
            self._result = stream_from_batches(self._stream, self._batches)
        return self._result


def channel_from_stream(stream: StreamProtocol) -> InputChannel:
    """Open an input channel on a complete stream.

    The channel delivers the stream's transport columns (``transport_columns``) as
    Arrow record batches of at most ``SOURCE_BATCH_ROWS`` rows. Several channels may be
    opened on one stream, each with its own position; nothing is computed until the
    first read, and nothing is queued, since the stream is complete.

    Args:
        stream: The stream to read; it is the channel's description.

    Returns:
        The input channel.
    """
    return InputChannel(stream, _StreamFeed(stream))


def _named(stream: Any) -> str:  # pyright: ignore[reportExplicitAny]  # any stream or node
    """How an error names a stream: its label, or what it prints as."""
    label = getattr(stream, "label", None)
    return repr(label) if label else repr(stream)


def _schema_mismatch(declared: pa.Schema, sent: pa.Schema) -> str | None:
    """Say how a record batch's schema differs from a channel's, or ``None``.

    The two schemas agree when they have the same columns in the same order, each with
    the same Arrow type and nullability. What a description does not say of a row is
    not compared: a ``null`` column, which rows carry as ``large_string``, and whether
    a system-tag or source-info field is nullable, which follows the values of the
    complete table (``_flagged_by_values``).
    """
    from orcapod.system_constants import constants
    from orcapod.utils import arrow_utils

    if declared.names != sent.names:
        return f"it has {sent.names}, the stream has {declared.names}"
    for expected, column in zip(declared, sent):
        expected_type = arrow_utils.normalize_to_large_types(expected.type)
        if expected_type != arrow_utils.normalize_to_large_types(column.type):
            return f"column {column.name!r} is {column.type}, the stream's is {expected.type}"
        if expected.nullable != column.nullable and not column.name.startswith(
            (constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX)
        ):
            flags = {True: "nullable", False: "not nullable"}
            return (
                f"column {column.name!r} is {flags[column.nullable]}, the stream's is "
                + flags[expected.nullable]
            )
    return None
