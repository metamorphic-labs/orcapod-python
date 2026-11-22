"""The input channel of Arrow record batches (ITL-1347).

``InputChannel`` implements ``RecordBatchInput``
(``orcapod.protocols.core_protocols.record_batch_channels``): a single-pass,
back-pressured reader of a stream's Arrow record batches. The stream is primary and owns
the description (keys, schemas, data context, pipeline hash); a channel refers to its
stream and restates nothing.

A channel reads through a feed (``BatchFeed``), which says where its record batches come
from: a complete stream (``StreamBase.open_channel()``), or an output channel
(``OutputChannel.open_reader()`` in ``orcapod.core.record_batch_channels``). This module
imports no concrete stream, node or operator, so a stream can open a channel without an
import cycle.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, Protocol

from orcapod.channels import ChannelClosed

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.core_protocols.record_batch_channels import (
        RecordBatchInput,
    )
    from orcapod.protocols.core_protocols.streams import StreamProtocol

SOURCE_BATCH_ROWS = 65_536
"""The most rows of one Arrow record batch a complete stream is read in.

Developer decision, November 22, 2026 (ITL-1347, question 6): a source sends record
batches of at most 65,536 rows; an operator sends its result's own record batches.
"""


class BatchFeed(Protocol):
    """Where an input channel's record batches come from."""

    async def next(self) -> pa.RecordBatch | None:
        """Return the next record batch, or ``None`` at the end."""
        ...

    def close(self) -> None:
        """Stop reading: nothing waits for this reader any more."""
        ...

    async def to_stream(self, channel: RecordBatchInput) -> StreamProtocol:
        """Return all of ``channel``'s input as a stream; ``channel`` is not read yet.

        Args:
            channel: The input channel this feed serves.
        """
        ...


class InputChannel:
    """A single-pass, back-pressured reader of a stream's Arrow record batches.

    The Arrow-record-batch form of ``ReadableChannel``. The description of what it
    delivers is its ``stream``, known before the first batch: the producing node, or
    the stream it was opened from. A channel has no identity of its own and is read
    once; to read again, go back to a stream (the one it was opened from, or the one
    ``to_stream`` gives).

    Made by ``stream.open_channel()`` or by ``OutputChannel.open_reader()``, never
    directly.
    """

    def __init__(self, stream: StreamProtocol, feed: BatchFeed) -> None:
        """Make an input channel; use ``open_channel`` or ``open_reader``.

        Args:
            stream: The stream whose rows the channel delivers: its description.
            feed: Where the record batches come from.
        """
        self._stream = stream
        self._feed = feed
        self._received = 0
        self._ended = False
        # Closed by its consumer before the producer completed.
        self._closed = False

    @property
    def stream(self) -> StreamProtocol:
        """The stream whose rows this channel delivers: the description."""
        return self._stream

    async def receive(self) -> pa.RecordBatch:
        """Return the next Arrow record batch.

        A read that is cancelled while it waits closes the channel, as ``close`` does:
        a consumer that stops reading never holds its producer.

        Raises:
            ChannelClosed: At the end, and after ``close``. After a ``close`` that came
                before the producer completed, the error says so, and iteration
                (``async for``) raises it instead of ending: no consumer loop takes
                part of an input for all of it.
            UpstreamFailedError: If the producer failed or was cancelled.
        """
        if self._closed:
            raise self._closed_early()
        if self._ended:
            raise ChannelClosed()
        try:
            batch = await self._feed.next()
        except asyncio.CancelledError:
            await self.close()
            raise
        if batch is None:
            if self._closed:  # woken by its own early close
                raise self._closed_early()
            self._ended = True
            raise ChannelClosed()
        self._received += 1
        return batch

    def __aiter__(self) -> AsyncIterator[pa.RecordBatch]:
        """Iterate over the remaining record batches, in arrival order."""
        return self

    async def __anext__(self) -> pa.RecordBatch:
        """Return the next record batch; end the iteration at the producer's end.

        A channel closed before its producer completed does not end the iteration:
        its ``ChannelClosed`` is raised, so a loop never takes part of an input for
        all of it.
        """
        try:
            return await self.receive()
        except ChannelClosed:
            if self._closed:
                raise  # closed before its producer completed: not the end
            raise StopAsyncIteration

    def _closed_early(self) -> ChannelClosed:
        """The error of a read after a close that came before the producer completed."""
        return ChannelClosed(
            f"The input channel of {_named(self._stream)} was closed before its "
            + "producer completed; it has no complete input to give."
        )

    async def collect(self) -> list[pa.RecordBatch]:
        """Read to the end and return every remaining record batch, in arrival order.

        Raises:
            ChannelClosed: If the channel was closed before its producer completed:
                what was read is part of the input, never returned as all of it.
            UpstreamFailedError: If the producer failed or was cancelled.
        """
        return [batch async for batch in self]

    async def to_stream(self) -> StreamProtocol:
        """Read to the end and return all of the channel's input, as a stream.

        It is how an operator that needs all of an input reads it, and it needs a
        channel not read yet: a stream of part of an input is never made. A channel
        opened on a complete stream gives that stream; a reader of an output channel
        gives the record batches it delivered, in arrival order, with the identity of
        ``stream``.

        Raises:
            RuntimeError: If a record batch has already been received.
            ChannelClosed: If the channel was closed before its producer completed.
            UpstreamFailedError: If the producer failed or was cancelled.
        """
        if self._received:
            raise RuntimeError(
                "to_stream() gives everything an input channel delivers, as a stream; "
                + f"{self._received} record batch(es) of this one have been received "
                + "already."
            )
        if self._closed:
            raise self._closed_early()
        stream = await self._feed.to_stream(self)
        self._ended = True
        return stream

    async def close(self) -> None:
        """Stop reading. The producer no longer waits for this reader."""
        if not self._ended:
            self._closed = True
        self._ended = True
        self._feed.close()


def _named(stream: Any) -> str:  # pyright: ignore[reportExplicitAny]  # any stream or node
    """How an error names a stream: its label, or what it prints as."""
    label = getattr(stream, "label", None)
    return repr(label) if label else repr(stream)
