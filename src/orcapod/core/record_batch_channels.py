"""Channels of Arrow record batches (ITL-1347).

``InputChannel`` and ``OutputChannel`` carry a stream's rows as Arrow record batches; they
implement ``RecordBatchInput`` and ``RecordBatchOutput`` of ``orcapod.channels``. The
stream is primary and owns the description (keys, schemas, data context, pipeline hash); a
channel refers to its stream and restates nothing. An input channel is opened from a
stream (``channel_from_stream``) or from an output channel (``OutputChannel.open_reader``);
an output channel, once closed, gives its rows back as a stream (``OutputChannel.result``).
A stream is made from record batches by one function, ``stream_from_batches`` of
``orcapod.core.operators.static_output_pod``.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, Protocol

import pyarrow as pa

from orcapod.channels import ChannelClosed
from orcapod.core.operators.static_output_pod import (
    stream_from_batches,
    transport_columns,
    transport_schema,
)
from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS
from orcapod.errors import UpstreamFailedError
from orcapod.system_constants import constants
from orcapod.utils import arrow_utils

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import StreamProtocol

SOURCE_BATCH_ROWS = 65_536
"""The most rows of one Arrow record batch a complete stream is read in.

Developer decision, November 22, 2026 (ITL-1347, question 6): a source sends record
batches of at most 65,536 rows; an operator sends its result's own record batches.
"""

BATCH_BUFFER_SIZE = 8
"""How many Arrow record batches an input channel queues before its producer waits."""


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

    Made by ``channel_from_stream`` or by ``OutputChannel.open_reader``, never
    directly.
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
            self._schema = transport_schema(self._stream)
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
