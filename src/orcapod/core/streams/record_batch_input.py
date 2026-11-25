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
import functools
import weakref
from collections.abc import AsyncIterator, Coroutine
from typing import TYPE_CHECKING, Any, Protocol

import pyarrow as pa

from orcapod.channels import ChannelClosed
from orcapod.utils import source_tokens

if TYPE_CHECKING:
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
    ``to_stream`` makes of what it delivered).

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
        # The claim is held weakly: a claim freed before a task took it over (its
        # conversion cancelled before it started, or never awaited) closes the
        # channel, so no producer waits on a reader nobody will read.
        self._owner: asyncio.Task[Any] | None = None
        self._claim: weakref.ref[_Claim] | None = None
        self._claim_release: weakref.finalize[Any, Any] | None = None
        # A reader dropped before anyone reads it (no owner task, no claim): when it
        # is freed, its feed is closed, so a bounded producer never waits for it. A
        # feed never refers to its channel, so this holds nothing alive; a live
        # unread reader stays registered and gets the first batch.
        self._release_when_freed = weakref.finalize(self, feed.close)
        self._release_when_freed.atexit = False

    @property
    def stream(self) -> StreamProtocol:
        """The stream whose rows this channel delivers: the description."""
        return self._stream

    @property
    def unread(self) -> bool:
        """Whether no read was called and the channel is not closed.

        A reserved channel (``reserve()``) is not unread, even before its first read.
        """
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
            held = self._claim() if self._claim is not None else None
            if held is None or claim is not held:
                raise RuntimeError(
                    f"The input channel of {_named(self._stream)} is claimed by "
                    + "another consumer."
                )
            self._bind_owner(_current_task())
            self._drop_claim()  # redeemed: a later reserve(claim) is refused
            return claim
        if self._started or self._ended or self._owner is not None:
            raise RuntimeError(
                f"The input channel of {_named(self._stream)} has been read already, "
                + "or is being read by another consumer; all of its input can no "
                + "longer be given."
            )
        self._started = True
        self._owner = None  # held for the claim until a task takes it over
        new_claim = _Claim()
        self._claim = weakref.ref(new_claim)
        # The release holds the feed (the reader's place in its producer, which never
        # refers to the channel), so it is released even if the channel object has
        # been freed first.
        self._claim_release = weakref.finalize(
            new_claim, _release_claim, self._feed, weakref.ref(self)
        )
        return new_claim

    def _drop_claim(self) -> None:
        """Forget the claim: taken over, or the channel closed."""
        if self._claim_release is not None:
            _ = self._claim_release.detach()
        self._claim_release = None
        self._claim = None

    def _abandon(self) -> None:
        """Mark the channel closed: its claim was freed before any task took it over.

        Synchronous (it runs when the claim is freed); its feed is closed by the
        claim's release itself (``_release_claim``).
        """
        self._claim_release = None
        self._claim = None
        if not self._ended:
            self._closed = True
        self._ended = True
        self._owner = None

    def _bind_owner(self, task: asyncio.Task[Any] | None) -> None:  # pyright: ignore[reportExplicitAny]  # any task
        """Make ``task`` the channel's one consumer.

        When the task finishes (or is cancelled) while it still owns the channel,
        before the end and without closing it, the channel is closed: its consumer is
        gone, and no producer may wait for it.
        """
        self._owner = task
        if task is not None:
            task.add_done_callback(
                functools.partial(_owner_finished, weakref.ref(self), self._feed)
            )

    def _owner_gone(self, task: asyncio.Task[Any]) -> None:  # pyright: ignore[reportExplicitAny]  # any task
        """Close the channel if ``task`` still owns it: the consumer finished."""
        if self._owner is not task or self._ended:
            return
        self._closed = True
        self._ended = True
        self._owner = None
        self._drop_claim()
        self._feed.close()

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
            self._bind_owner(task)
        elif self._owner is not task:
            raise RuntimeError(
                f"The input channel of {_named(self._stream)} belongs to another "
                + "consumer (another task); a channel has one consumer."
            )

    async def receive(self) -> pa.RecordBatch:
        """Return the next Arrow record batch, its source-info tokens as text.

        The text of a derived token column (``utils/source_tokens.py``) is built here: a
        recipe leaves the library in no batch. The rest is as ``_receive_carried``.
        """
        batch = await self._receive_carried()
        if not source_tokens.derived_columns(batch.schema):
            return batch
        table = source_tokens.materialize(pa.Table.from_batches([batch]))
        return table.combine_chunks().to_batches()[0]

    async def carried(self) -> AsyncIterator[pa.RecordBatch]:
        """Iterate over the remaining record batches as they travel, for an operator.

        Their source-info columns may be derived; the end and the errors are those of
        iterating the channel.
        """
        while True:
            try:
                batch = await self._receive_carried()
            except ChannelClosed:
                if self._ended and not self._closed:
                    return
                raise
            yield batch

    async def _receive_carried(self) -> pa.RecordBatch:
        """Return the next Arrow record batch, as it travels.

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
        except StopAsyncIteration as error:
            # Only ``None`` is the feed's end: a raised StopAsyncIteration would end
            # an ``async for`` over part of the input.
            raise RuntimeError(
                f"The read of the input channel of {_named(self._stream)} raised "
                + "StopAsyncIteration; a feed ends by returning None."
            ) from error
        finally:
            self._reading = False
        if batch is None:
            if self._closed:  # woken by its own early close
                raise self._closed_early()
            self._ended = True
            self._owner = None  # nothing left to read: the consumer is released
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

    def to_stream(self) -> Coroutine[Any, Any, StreamProtocol]:
        """Return what the channel delivers, as a stream: the one such conversion.

        It reads the channel to its end (one pass; every error of the read, such as
        an unbound source's or a failed producer's, is raised here, before any
        operator runs) and builds a fixed snapshot of what was delivered through the
        one settlement path (``stream_from_batches``). The only shortcut: a channel
        opened on a complete stream that ``_output_stream`` built
        (``ROW_BUILT_OUTPUT``), not read yet, gives that stream, which is exactly what
        its batches would rebuild; the channel is then closed. A reader of an output
        channel always collects, whatever its description.

        A plain method that returns the conversion to await: the channel is reserved
        when it is called, before the conversion is awaited, so nothing can read it
        in between; the task that runs the conversion takes the channel over, and
        any other task's read is refused. If the conversion never starts (its task
        is cancelled first, or it is never awaited), the reservation's claim is freed
        with it and the channel is closed, so no producer waits on it.

        Returns:
            The conversion to await: a complete, re-readable stream of the channel's
            input.

        Raises:
            RuntimeError: If a read of the channel was already called (a batch
                received, its end reached, or another consumer still waiting): a
                stream of part of an input is never made, and a channel is never read
                again.
            ChannelClosed: If the channel was closed, before or during the read.
            UpstreamFailedError: If the producer failed or was cancelled.
        """
        claim = self.reserve()  # at call time: nothing can read it before the await
        return self._to_stream(claim)

    async def _to_stream(self, claim: object) -> StreamProtocol:
        """Read the reserved channel and return its input as a stream (``to_stream``).

        Args:
            claim: The reservation's claim, which the task running this conversion
                takes over.
        """
        # Imported here: the operators sit above the streams, and this module is
        # imported by core/streams/base.py (a module-level import would be circular).
        from orcapod.core.operators.static_output_pod import (
            ROW_BUILT_OUTPUT,
            stream_from_batches,
        )

        self.reserve(claim)
        opened_on = self.opened_on
        if opened_on is not None and getattr(opened_on, ROW_BUILT_OUTPUT, False):
            await self.close()
            return opened_on
        return stream_from_batches(
            self._stream, [batch async for batch in self.carried()]
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
        """Stop reading. The producer no longer waits for this reader.

        Callable from any task. The consumer task is released: no read is taken
        after a close, from any task.
        """
        if not self._ended:
            self._closed = True
        self._ended = True
        self._owner = None
        self._drop_claim()
        self._feed.close()


class _Claim:
    """The claim ``reserve()`` returns; the channel is held for it while it lives.

    A plain class, so that the channel can hold it weakly.
    """


def _owner_finished(
    channel_ref: weakref.ref[InputChannel],
    feed: BatchFeed,
    task: asyncio.Task[Any],  # pyright: ignore[reportExplicitAny]  # any task
) -> None:
    """Release a reader whose consumer task finished between reads.

    A done callback of the owning task: the channel, if it still exists and the task
    still owns it before its end, is closed (``InputChannel._owner_gone``). A channel
    freed before the callback runs (a reader opened in the task's own frame) can no
    longer be asked; its feed, held here and never referring to the channel, is
    closed, so its producer does not wait for a reader nobody holds. Closing a feed
    that ended already does nothing.

    Args:
        channel_ref: The channel, held weakly.
        feed: The channel's feed (its place in its producer).
        task: The finished task.
    """
    channel = channel_ref()
    if channel is None:
        feed.close()
        return
    channel._owner_gone(task)  # pyright: ignore[reportPrivateUsage]  # the task's release


def _release_claim(feed: BatchFeed, channel_ref: weakref.ref[InputChannel]) -> None:
    """Release a reader whose claim was freed before any task took it over.

    The reader's feed (its place in its producer) is closed in every case, so no
    producer waits on it, also when the channel object was freed first; the channel,
    if it still exists, is marked closed.

    Args:
        feed: The reader's feed.
        channel_ref: The channel, held weakly.
    """
    feed.close()
    channel = channel_ref()
    if channel is not None:
        channel._abandon()  # pyright: ignore[reportPrivateUsage]  # the claim's release


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
