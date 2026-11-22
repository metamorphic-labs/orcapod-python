"""Channels of Arrow record batches (ITL-1347).

``InputChannel`` and ``OutputChannel`` carry a stream's rows as Arrow record batches; they
implement ``RecordBatchInput`` and ``RecordBatchOutput``
(``orcapod.protocols.core_protocols.record_batch_channels``). The
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
from typing import TYPE_CHECKING, Any, Protocol, cast

import pyarrow as pa
import pyarrow.compute as pc

from orcapod.channels import ChannelClosed
from orcapod.core.operators.static_output_pod import (
    stream_from_batches,
    transport_schema,
)
from orcapod.core.streams.base import (
    ZERO_ROW_INPUT_COLUMNS,
    row_built_columns,
    row_built_names,
)
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
        """Make a feed of ``stream``'s rows; nothing is read until the first ``next``.

        Args:
            stream: The complete stream to read.
        """
        self._stream = stream
        self._batches: deque[pa.RecordBatch] | None = None

    async def next(self) -> pa.RecordBatch | None:
        """Return the stream's next record batch, or ``None`` at the end.

        The stream's table is read on the first call, not when the feed is made, in
        the row-built form (``row_built_columns``) and cut into batches of at most
        ``SOURCE_BATCH_ROWS`` rows; batches with no rows are skipped.
        """
        if self._batches is None:
            # On the first read, not when the channel is opened: opening computes
            # nothing.
            table = row_built_columns(
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
        """Stop reading: the batches not read are dropped. No producer waits on it."""
        self._batches = deque()


class _OutputFeed:
    """One reader of an output channel, with its own position."""

    def __init__(self, output: OutputChannel) -> None:
        """Make a reader of ``output`` at the start of what it sends.

        Args:
            output: The output channel read.
        """
        self._output = output
        self.open = True
        # With retained batches: this reader's position among them. Without: the
        # batches sent and not yet read by this reader.
        self.position = 0
        self.queue: deque[pa.RecordBatch] = deque()

    async def next(self) -> pa.RecordBatch | None:
        """Return this reader's next record batch, or ``None`` at the end.

        Waits while nothing is there for it. A failed output raises at once, with
        batches still unread. Without ``retain``, taking a batch frees room in this
        reader's queue and wakes a producer waiting for it.

        Raises:
            UpstreamFailedError: If the output failed.
        """
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
        """Stop reading and unregister from the output.

        The queued batches are dropped and a producer waiting for room in this
        reader's queue is woken: it no longer waits for this reader.
        """
        self.open = False
        self.queue.clear()
        output = self._output
        readers = output._readers  # pyright: ignore[reportPrivateUsage]
        if self in readers:
            readers.remove(self)  # a send no longer walks it
        output._notify()  # pyright: ignore[reportPrivateUsage]  # the producer stops waiting


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
        """Make an input channel; use ``channel_from_stream`` or ``open_reader``.

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
        """Read to the end and return what the channel delivered, as a stream.

        The stream holds the record batches in arrival order, with the identity of
        ``stream`` (``stream_from_batches``). It is how an operator that needs all of
        an input reads it. Called on a channel not read yet: a stream of part of an
        input is never made.

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
        return stream_from_batches(self._stream, await self.collect())

    async def close(self) -> None:
        """Stop reading. The producer no longer waits for this reader."""
        if not self._ended:
            self._closed = True
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
        """Make an output channel for ``stream``'s rows (the arguments: see the class).

        Raises:
            ValueError: If ``buffer_size`` is less than 1.
        """
        if buffer_size < 1:
            raise ValueError(f"buffer_size must be at least 1, got {buffer_size}")
        self._stream = stream
        self._retain = retain
        self._buffer_size = buffer_size
        self._batches: list[pa.RecordBatch] = []
        self._readers: list[_OutputFeed] = []
        self._sent = 0
        # Sends waiting for a reader's room (``retain=False`` only).
        self._sending = 0
        self._send_lock = asyncio.Lock()
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
        """The error a reader or a send meets on a failed output, caused by its cause."""
        error = self._error
        assert error is not None
        failure = UpstreamFailedError(
            f"The input from {_named(self._stream)} did not finish: its producer "
            + f"failed with {type(error).__name__}: {error}"
        )
        failure.__cause__ = error
        return failure

    def _raise_if_ended(self) -> None:
        """Raise what a send meets on an ended channel: closed, or failed."""
        if isinstance(self._error, _ClosedDuringSend):
            raise ChannelClosed(str(self._error))
        if self._error is not None:
            raise self._upstream_failed()
        if self._closed:
            raise ChannelClosed("Cannot send to a closed channel")

    def _check_schema(self, batch: pa.RecordBatch) -> None:
        """Raise unless ``batch`` has the schema of this channel's record batches."""
        if not self._schema_known:
            self._schema = transport_schema(self._stream)
            self._schema_known = True
        schema = self._schema
        if schema is None:
            # No Arrow types in the description: the first batch that passes sets them
            # (they must be in the large variants), but the names are still the
            # stream's. A refused batch sets nothing.
            names = batch.schema.names
            if names != row_built_names(names, self._stream.keys()):
                raise ValueError(
                    f"A record batch sent as {_named(self._stream)} does not have that "
                    + f"stream's columns: it has {names}, the stream's keys are "
                    + f"{self._stream.keys()}."
                )
            schema = batch.schema
        mismatch = _schema_mismatch(schema, batch)
        if mismatch is not None:
            raise ValueError(
                f"A record batch sent as {_named(self._stream)} does not have that "
                + f"stream's columns: {mismatch}."
            )
        self._schema = schema

    async def send(self, batch: pa.RecordBatch) -> None:
        """Send one Arrow record batch of the stream's transport columns.

        A batch with no rows is checked and not kept. Without ``retain``, sends are
        taken one at a time, so every reader gets the batches in one order. A send
        waiting for a reader's room when the channel is closed or failed raises at
        once and publishes nothing more.

        Any abort of a send that has reached one reader and not yet all of them (a
        close, a failure, a cancellation, any error) fails the channel: readers never
        complete normally with different inputs. Every reader's next read then raises
        ``UpstreamFailedError``, caused by the abort (a ``ChannelClosed`` for a close).
        A send aborted before it reached any reader leaves the channel as it was.

        Raises:
            ChannelClosed: If the channel is closed.
            UpstreamFailedError: If the channel failed (``fail``); its cause is the
                producer's error.
            ValueError: If the batch's schema is not the stream's.
        """
        self._raise_if_ended()
        self._check_schema(batch)
        if not batch.num_rows:
            return
        self._sent += 1
        if self._retain:
            self._batches.append(batch)
            self._notify()
            return
        # One send at a time, so every reader gets the batches in one order.
        async with self._send_lock:
            self._raise_if_ended()
            self._sending += 1
            delivered = False
            try:
                for reader in list(self._readers):
                    # The slowest reader sets the pace; one that closed is not waited
                    # for.
                    while reader.open and len(reader.queue) >= self._buffer_size:
                        await self._changed.wait()
                        self._raise_if_ended()
                    if reader.open:
                        reader.queue.append(batch)
                        delivered = True
                        # At once, not after the last reader: this reader may be
                        # waiting for the batch while the send waits for another
                        # reader's room.
                        self._notify()
            except BaseException as error:
                # A send that reached a reader and stops before the others (cancelled,
                # or any error) fails the channel, as a close during it does.
                if delivered and self._error is None:
                    self._error = error
                    self._notify()
                raise
            finally:
                self._sending -= 1

    async def close(self) -> None:
        """Complete the output. A second close does nothing; never waits.

        A close that comes while a send is waiting for a reader's room does not
        complete the output: that send may have reached some readers and not others,
        and readers never complete normally with different inputs. The channel fails
        instead: the waiting send raises ``ChannelClosed``, and every reader's next
        read raises ``UpstreamFailedError`` whose cause is a ``ChannelClosed``.
        """
        if self._closed or self._error is not None:
            return
        if self._sending:
            self._error = _ClosedDuringSend(
                f"The output of {_named(self._stream)} was closed while a record "
                + "batch was being sent to its readers."
            )
        else:
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

    The channel delivers the stream's rows in the row-built form (``row_built_columns``) as
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


def _schema_mismatch(declared: pa.Schema, batch: pa.RecordBatch) -> str | None:
    """Say how a record batch's columns differ from a channel's, or ``None``.

    The batch must have the description's columns in the same order, each with exactly
    the description's type in its large variant (no other representation is taken and
    converted: type strictness), the same nullability and the same field metadata. A
    ``null`` type of the description (anywhere in a column's type) is carried as
    ``large_string``, as a row's datagram stores it, and only null values are taken
    there. Whether a system-tag or source-info field is nullable is not compared: it
    follows the values of the complete table (``_flag_by_values``).
    """
    sent = batch.schema
    if declared.names != sent.names:
        return f"it has {sent.names}, the stream has {declared.names}"
    provenance = (constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX)
    for index, (expected, column) in enumerate(zip(declared, sent)):
        carried = arrow_utils.normalize_to_large_types(expected.type)
        if column.type != carried:
            return f"column {column.name!r} is {column.type}, the stream's is {carried}"
        if carried != expected.type and not _holds_only_nulls(
            batch.column(index), expected.type
        ):
            return (
                f"column {column.name!r} holds values where the stream's type "
                + f"{expected.type} holds only nulls"
            )
        if column.name.startswith(provenance):
            # A row builds these from its values: no field metadata at any depth.
            if column.metadata or not column.type.equals(
                _without_metadata(column.type), check_metadata=True
            ):
                return f"column {column.name!r} carries field metadata"
            continue
        if expected.nullable != column.nullable:
            flags = {True: "nullable", False: "not nullable"}
            return (
                f"column {column.name!r} is {flags[column.nullable]}, the stream's is "
                + flags[expected.nullable]
            )
        if (expected.metadata or None) != (column.metadata or None) or not (
            column.type.equals(carried, check_metadata=True)
        ):
            return (
                f"column {column.name!r} has field metadata {column.metadata} (or in "
                + f"its type {column.type}), the stream's is {expected.metadata}"
            )
    return None


def _without_metadata(kind: pa.DataType) -> pa.DataType:
    """``kind`` with the field metadata of every nested field removed."""
    if isinstance(kind, pa.LargeListType):
        return pa.large_list(_field_without_metadata(kind.value_field))  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
    if isinstance(kind, pa.ListType):
        return pa.list_(_field_without_metadata(kind.value_field))  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
    if isinstance(kind, pa.FixedSizeListType):
        return pa.list_(_field_without_metadata(kind.value_field), kind.list_size)  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
    if isinstance(kind, pa.StructType):
        return pa.struct(
            [_field_without_metadata(kind.field(i)) for i in range(kind.num_fields)]
        )  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
    return kind


def _field_without_metadata(field: pa.Field[Any]) -> pa.Field[Any]:  # pyright: ignore[reportExplicitAny]  # any field
    """``field`` with no field metadata, at its own level and below."""
    return pa.field(field.name, _without_metadata(field.type), nullable=field.nullable)


def _holds_only_nulls(array: pa.Array[Any], declared: pa.DataType) -> bool:  # pyright: ignore[reportExplicitAny]  # any column
    """Whether ``array``'s values are null wherever ``declared`` has the ``null`` type.

    Judged on the logical values of ``array`` as sent: a slice's own rows only, and
    nothing under a null list or struct (its children's buffers are not read).

    Args:
        array: A column as carried (``null`` as ``large_string``).
        declared: The column's type in the description.
    """
    if pa.types.is_null(declared):
        return array.null_count == len(array)
    if isinstance(
        declared, (pa.ListType, pa.LargeListType, pa.FixedSizeListType, pa.MapType)
    ):
        if isinstance(array.type, pa.MapType):
            # A map's entries as a list of structs, which list_flatten reads.
            entries = pa.struct([array.type.key_field, array.type.item_field])  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownArgumentType]
            array = array.cast(pa.large_list(entries))  # pyright: ignore[reportUnknownMemberType]
        # The values of the non-null lists of the slice, in order.
        values = pc.list_flatten(array)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if isinstance(declared, pa.MapType):
            keys, items = cast(
                "pa.StructArray", cast(object, values)
            ).flatten()  # list_flatten of entries is a struct array
            return _holds_only_nulls(keys, declared.key_type) and _holds_only_nulls(  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
                items,
                declared.item_type,  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            )
        return _holds_only_nulls(values, declared.value_type)  # pyright: ignore[reportUnknownArgumentType]
    if isinstance(declared, pa.StructType):
        # Each child with the struct's validity applied, for the slice's rows.
        children = array.flatten()  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
        return all(
            _holds_only_nulls(child, declared.field(i).type)  # pyright: ignore[reportUnknownArgumentType]
            for i, child in enumerate(children)  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
        )
    return True


class _ClosedDuringSend(ChannelClosed):
    """The failure of an output channel closed while a send was in flight."""
