"""Protocols of the channels of Arrow record batches (ITL-1347).

``RecordBatchInput`` and ``RecordBatchOutput`` are what an operator's record-batch form
reads and writes. The stream is primary and owns the description (keys, schemas, data
context, pipeline hash); a channel refers to its stream and restates nothing. The
implementations are in ``orcapod.core.record_batch_channels``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.core_protocols.streams import StreamProtocol


class RecordBatchInput(Protocol):
    """A single-pass reader of a stream's Arrow record batches.

    The Arrow-record-batch form of ``ReadableChannel`` (``orcapod.channels``); ``InputChannel`` of
    ``orcapod.core.record_batch_channels`` is the implementation. Its description is
    its ``stream``, known before the first batch.
    """

    @property
    def stream(self) -> StreamProtocol:
        """The stream whose rows this channel delivers: the description."""
        ...

    async def receive(self) -> pa.RecordBatch:
        """Return the next Arrow record batch.

        Raises:
            ChannelClosed: At the end, and after ``close``.
            UpstreamFailedError: If the producer failed or was cancelled.
        """
        ...

    def __aiter__(self) -> AsyncIterator[pa.RecordBatch]:
        """Iterate over the remaining record batches, in arrival order."""
        ...

    async def collect(self) -> list[pa.RecordBatch]:
        """Read to the end and return every remaining record batch, in arrival order."""
        ...

    async def to_stream(self) -> StreamProtocol:
        """Read to the end and return what the channel delivered, as a stream."""
        ...

    async def close(self) -> None:
        """Stop reading. The producer no longer waits for this reader."""
        ...


class RecordBatchOutput(Protocol):
    """The producer side of a stream's Arrow record batches.

    The Arrow-record-batch form of ``WritableChannel`` (``orcapod.channels``); ``OutputChannel`` of
    ``orcapod.core.record_batch_channels`` is the implementation.
    """

    async def send(self, batch: pa.RecordBatch) -> None:
        """Send one Arrow record batch of the stream's transport columns."""
        ...

    async def close(self) -> None:
        """Complete the output."""
        ...

    def fail(self, error: BaseException) -> None:
        """End the output with the producer's failure or cancellation."""
        ...
