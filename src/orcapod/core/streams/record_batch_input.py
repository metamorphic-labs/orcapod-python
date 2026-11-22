"""The input channel of Arrow record batches (ITL-1347).

``InputChannel`` implements ``RecordBatchInput``
(``orcapod.protocols.core_protocols.streams``): a single-pass,
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

    @property
    def opened_on(self) -> StreamProtocol | None:
        """The complete stream the feed reads, or ``None`` (a reader of an output)."""
        ...


class InputChannel:
    """A single-pass, back-pressured reader of a stream's Arrow record batches.

    The Arrow-record-batch form of ``ReadableChannel``. The description of what it
    delivers is its ``stream``, known before the first batch: the producing node, or
    the stream it was opened from. A channel has no identity of its own and is read
    once; to read again, go back to a stream (the one it was opened from, or the one
    ``channel_to_stream`` makes of what it delivered).

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
        # The producer's end was reached (the feed returned ``None``).
        self._ended = False
        # Closed by its consumer before the producer completed.
        self._closed = False
        # A read was called: the channel has a consumer (reserved at call time).
        self._started = False
        # A read is in progress.
        self._reading = False
        # The consumer: the asyncio task the channel belongs to, for its whole life,
        # and the claim a conversion holds while it moves to the task that runs it.
        self._owner: asyncio.Task[Any] | None = None
        self._claim: object | None = None

    @property
    def stream(self) -> StreamProtocol:
        """The stream whose rows this channel delivers: the description."""
        return self._stream

    @property
    def unread(self) -> bool:
        """Whether no read was called and the channel is not closed."""
        return not self._started and not self._ended and not self._closed

    @property
    def opened_on(self) -> StreamProtocol | None:
        """The complete stream this channel reads, when it was opened on one.

        ``None`` for a reader of an output channel, whatever its description: only a
        complete stream's own feed delivers exactly that stream.
        """
        return self._feed.opened_on

    def reserve(self, claim: object | None = None) -> object:
        """Take the channel for one consumer that will read all of it.

        Without ``claim``: the channel, not read yet, is held for the returned claim:
        no read is taken, from any task, until a task takes it over with the claim
        (the task that runs a conversion). With the claim it returned: the calling
        task takes the channel over, and any other task's read is then refused.

        Args:
            claim: The claim an earlier ``reserve()`` returned, to take it over.

        Returns:
            The claim.

        Raises:
            RuntimeError: If a read was called on it already (by any consumer, also
                one still waiting), so all of the input can no longer be given; or
                if ``claim`` is not this channel's claim.
            ChannelClosed: If it was closed.
        """
        if self._closed:
            raise self._closed_early()
        if claim is not None:
            if claim is not self._claim:
                raise RuntimeError(
                    f"The input channel of {_named(self._stream)} is claimed by "
                    + "another consumer."
                )
            self._owner = _current_task()
            return claim
        if self._started or self._ended or self._owner is not None:
            raise RuntimeError(
                f"The input channel of {_named(self._stream)} has been read already, "
                + "or is being read by another consumer; all of its input can no "
                + "longer be given."
            )
        self._started = True
        self._owner = None  # held for the claim until a task takes it over
        self._claim = object()
        return self._claim

    def _belong_to_caller(self) -> None:
        """Bind the channel to the calling task on its first read; refuse any other.

        A channel has one consumer for its whole life: between batches as well as
        while a read is pending, a read from another task is refused.
        """
        task = _current_task()
        if self._owner is None and self._claim is not None:
            raise RuntimeError(
                f"The input channel of {_named(self._stream)} is held for a "
                + "conversion that has not started; no other read is taken."
            )
        if self._owner is None:
            self._owner = task
        elif self._owner is not task:
            raise RuntimeError(
                f"The input channel of {_named(self._stream)} belongs to another "
                + "consumer (another task); a channel has one consumer."
            )

    async def receive(self) -> pa.RecordBatch:
        """Return the next Arrow record batch.

        A read that is cancelled while it waits closes the channel, as ``close`` does:
        a consumer that stops reading never holds its producer.

        Raises:
            ChannelClosed: At the end, and after ``close``. After a ``close`` that came
                before the producer completed, the error says so, and iteration
                (``async for``) raises it instead of ending: no consumer loop takes
                part of an input for all of it. A ``ChannelClosed`` raised by the
                feed's own read is a read error too: only the feed's end ends it.
            RuntimeError: If the channel belongs to another consumer (another task;
                the first read binds it), or a read of it is in progress.
            UpstreamFailedError: If the producer failed or was cancelled.
        """
        if self._closed:
            raise self._closed_early()
        if self._ended:
            raise ChannelClosed()  # after the end there is nothing to take, for anyone
        self._belong_to_caller()
        if self._reading:
            raise RuntimeError(
                f"The input channel of {_named(self._stream)} is being read by another "
                + "consumer; a channel has one consumer."
            )
        self._started = True
        self._reading = True
        try:
            batch = await self._feed.next()
        except asyncio.CancelledError:
            await self.close()
            raise
        finally:
            self._reading = False
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
            # Only the producer's end ends the iteration: an early close, or a
            # ChannelClosed raised by the feed's own read, is a read error.
            if self._ended and not self._closed:
                raise StopAsyncIteration
            raise

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

    async def close(self) -> None:
        """Stop reading. The producer no longer waits for this reader."""
        if not self._ended:
            self._closed = True
        self._ended = True
        self._feed.close()


def _current_task() -> asyncio.Task[Any] | None:
    """The running asyncio task, or ``None`` outside one."""
    try:
        return asyncio.current_task()
    except RuntimeError:  # no running event loop
        return None


def _named(stream: Any) -> str:  # pyright: ignore[reportExplicitAny]  # any stream or node
    """How an error names a stream: its label, or what it prints as."""
    label = getattr(stream, "label", None)
    return repr(label) if label else repr(stream)
