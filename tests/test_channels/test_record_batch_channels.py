"""Input and output channels of Arrow record batches (ITL-1347, stage 5: L8).

A stream is primary and owns the description; a channel is a separate, single-pass
carrier of the stream's rows as Arrow record batches. An input channel is opened from a
complete stream (``stream.open_channel()``) or from an output channel
(``OutputChannel.open_reader``); an output channel, once closed, gives its rows back as
an ordinary stream (``result()``), and so does an input channel that is read to its end
(``to_stream()``). One test per rule of the design's table "The hard semantics".

The oracle of every table is ``R``: the table today's code builds from a stream's rows
(``StaticOutputOperatorPod._rows_to_table``).
"""

from __future__ import annotations

import asyncio
import gc
import weakref
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar
from unittest import mock

import pyarrow as pa
import pytest

from orcapod.protocols.core_protocols.streams import (
    RecordBatchInput,
    RecordBatchOutput,
)
from orcapod.channels import (
    ChannelClosed,
    ReadableChannel,
    WritableChannel,
)
from orcapod.core.operators import (
    Batch,
    MapData,
    PolarsFilter,
    SelectDataColumns,
    SemiJoin,
)
from orcapod.core.operators.static_output_pod import (
    ROW_BUILT_OUTPUT,
    StaticOutputOperatorPod,
    stream_from_batches,
)
from orcapod.core.record_batch_channels import (
    BATCH_BUFFER_SIZE,
    OutputChannel,
    _schema_mismatch,  # pyright: ignore[reportPrivateUsage]  # the check itself is tested
)
from orcapod.core.streams.record_batch_input import SOURCE_BATCH_ROWS, InputChannel
from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import UnboundSourceError, UpstreamFailedError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.core.sources import PollingSource
from orcapod.core.sources.source_proxy import SourceProxy
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import SyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.sources import ArrowTableSource
from orcapod.types import Schema
from tests.test_channels.test_polling_source import FakeDynamicSource, _batch

# PolarsFilter is built here on purpose; the warning of every construction is tested in
# test_polars_filter_warning.py (ITL-1296).
pytestmark = pytest.mark.filterwarnings("ignore::orcapod.errors.PolarsFilterWarning")

T = TypeVar("T")
ALL = {"system_tags": True, "source": True}
TIMEOUT = 5.0


def test_raw_system_tag_fields_survive_a_record_batch_channel() -> None:
    """A channel carries a stream's system-tag Arrow fields through to its rows."""
    system = pa.schema(
        [
            pa.field("_tag_rank", pa.int8(), False, metadata={b"purpose": b"rank"}),
            pa.field("_tag_label", pa.large_string(), True),
        ]
    )
    schema = pa.schema(
        [
            pa.field("subject", pa.large_string(), False),
            pa.field("trial", pa.int16(), False),
            pa.field("x", pa.int8(), False),
            pa.field("y", pa.int8(), False),
            *system,
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array(values, field.type)
            for values, field in zip(
                (["a", "b"], [1, 2], [3, 4], [5, 6], [7, 8], ["a", None]),
                schema,
                strict=True,
            )
        ],
        schema=schema,
    )
    stream = ArrowTableStream(table, tag_columns=("subject", "trial"))

    async def forward() -> StreamProtocol:
        output = OutputChannel(stream)
        async for batch in stream.open_channel():
            assert batch.select(system.names).schema.equals(system, check_metadata=True)
            await output.send(batch)
        await output.close()
        return output.result()

    assert pa.schema([stream.arrow_schema().field(n) for n in system.names]).equals(
        system, check_metadata=True
    )
    rebuilt = run(forward)
    assert (
        rebuilt.as_table(columns=ALL)
        .select(system.names)
        .schema.equals(system, check_metadata=True)
    )
    for tag, _ in rebuilt.iter_data():
        assert (
            tag.as_table(columns=ALL)
            .select(system.names)
            .schema.equals(system, check_metadata=True)
        )


def run(make: Callable[[], Awaitable[T]]) -> T:
    """Run a coroutine to its end; a channel that never wakes fails the test."""

    async def bounded() -> T:
        return await asyncio.wait_for(make(), TIMEOUT)

    return asyncio.run(bounded())


def source(n: int = 7, source_id: str = "events") -> ArrowTableSource:
    """``n`` rows: tags ``subject`` and ``trial``, both data-tags too (stated: no
    tag-only column), three data columns."""
    return ArrowTableSource(
        pa.table(
            {
                "subject": pa.array([f"m{i % 3}" for i in range(n)], pa.string()),
                "trial": pa.array(range(n), pa.int16()),
                "amplitude": pa.array([0.5 * i for i in range(n)], pa.float32()),
                "note": pa.array(
                    [None if i % 4 == 1 else f"n{i}" for i in range(n)], pa.string()
                ),
                "trace": pa.array([[i, i + 1] for i in range(n)], pa.list_(pa.int8())),
            }
        ),
        tag_columns=["subject", "trial"],
        tag_only_columns=(),
        source_id=source_id,
    )


def shaped_source(kind: pa.DataType, n: int = 2) -> ArrowTableStream:
    """Return a stream whose amplitude and token have the declared list layers."""
    from tests.token_shapes import nested_value, with_token_column

    table = with_token_column(
        source(n).as_table(columns=ALL),
        "amplitude",
        [nested_value("seed", kind)] * n,
        kind,
    )
    return ArrowTableStream(
        table, tag_columns=["subject", "trial"], data_tags=["trial"]
    )


def R(stream: Any) -> pa.Table:
    """The table today's code builds from ``stream``'s rows."""
    return StaticOutputOperatorPod._rows_to_table(list(stream.iter_data()))[0]


def same(table: pa.Table, expected: pa.Table) -> bool:
    """Whether two tables have one schema (metadata included) and the same rows."""
    return table.schema.equals(expected.schema, check_metadata=True) and bool(
        table.combine_chunks().equals(expected.combine_chunks())
    )


def batches_of(stream: Any, rows: int = SOURCE_BATCH_ROWS) -> list[pa.RecordBatch]:
    """``stream``'s record batches, cut every ``rows`` rows."""

    async def read() -> list[pa.RecordBatch]:
        return await stream.open_channel().collect()

    return pa.Table.from_batches(run(read)).to_batches(max_chunksize=rows)


# --- Opening an input channel on a stream ----------------------------------------------


def test_a_channel_opened_on_a_stream_delivers_its_rows_and_refers_to_it() -> None:
    """The record batches are the stream's rows in the row-built form; the description
    is the stream, and ``to_stream()`` rebuilds what the channel delivered, with
    the stream's identity."""
    stream = source()
    channel = stream.open_channel()

    assert isinstance(channel, InputChannel)
    assert isinstance(channel, ReadableChannel)
    as_protocol: RecordBatchInput = channel  # what an operator reads (type-checked)
    assert as_protocol.stream is stream
    batches = run(stream.open_channel().collect)
    assert same(pa.Table.from_batches(batches), R(stream))
    delivered = run(lambda: channel.to_stream())
    assert same(delivered.as_table(columns=ALL), R(stream))
    assert delivered.keys() == stream.keys()
    assert delivered.data_context_key == stream.data_context_key
    assert delivered.pipeline_hash() == stream.pipeline_hash()
    with pytest.raises(ChannelClosed):
        _ = run(channel.receive)  # read to its end


def test_a_source_node_opens_a_channel_on_its_rows() -> None:
    """A source node is a stream too. A job's source node delivers its source's rows in
    the row-built form, which ``to_stream()`` rebuilds. An unbound slot opens a
    channel (opening reads nothing), and its first read raises."""
    stream = source()
    node = SourceJobNode.from_stream(stream, name="events")
    assert isinstance(node, StreamProtocol)
    batches = run(node.open_channel().collect)
    assert same(pa.Table.from_batches(batches), R(node))
    rebuilt = run(lambda: node.open_channel().to_stream())
    assert same(rebuilt.as_table(columns=ALL), R(node))

    slot = SourceNode.from_stream(stream, name="events")
    assert isinstance(slot, StreamProtocol)
    channel = slot.open_channel()
    with pytest.raises(UnboundSourceError):
        _ = run(channel.receive)


def test_a_bound_source_node_sends_its_rows_in_their_own_column_order() -> None:
    """A source whose physical columns are x, j, k, y with tags declared k, j: the
    node's batches have the column order of its own rows (``execute()``: k, j, ...).
    Since ``main`` 4d192bfc the slot's schema lists the tags in tag order too (#486)."""
    stream = ArrowTableSource(
        pa.table(
            {
                "x": pa.array([1, 2], pa.int64()),
                "j": pa.array([3, 4], pa.int64()),
                "k": pa.array([5, 6], pa.int64()),
                "y": pa.array([7.5, 8.5], pa.float64()),
            }
        ),
        tag_columns=["k", "j"],
        tag_only_columns=True,
        source_id="ordered",
    )
    node = SourceJobNode.from_stream(stream, name="ordered")
    assert node.keys()[0] == stream.keys()[0] == ("k", "j")  # tag order (#486)
    rows = list(node.iter_data())  # the rows execute() returned before ITL-1347 stage 2
    expected = StaticOutputOperatorPod._rows_to_table(rows)[0]
    batches = run(node.open_channel().collect)
    assert pa.Table.from_batches(batches).schema.names == expected.schema.names
    assert same(pa.Table.from_batches(batches), expected)


def _keyed_source(
    tag_only: tuple[str, ...], *, source_id: str = "keyed", offset: int = 0
) -> ArrowTableSource:
    """Tags ``k, j`` (data-tags unless named in ``tag_only``), data ``x, y``; every
    field non-nullable, so a slot may declare ``x: int | None`` (a narrower bind)."""
    table = pa.table(
        {
            "x": pa.array([1 + offset, 2 + offset], pa.int64()),
            "k": pa.array([5 + offset, 6 + offset], pa.int64()),
            "j": pa.array([3 + offset, 4 + offset], pa.int64()),
            "y": pa.array([7.5, 8.5], pa.float64()),
        }
    )
    table = table.cast(pa.schema([f.with_nullable(False) for f in table.schema]))
    return ArrowTableSource(
        table, tag_columns=["k", "j"], tag_only_columns=tag_only, source_id=source_id
    )


def _declared_node(source: ArrowTableSource) -> SourceJobNode:
    """A slot that declares tags ``j, k`` and data ``y, x: int | None`` (the source's
    data-tags first), bound to ``source``: the declared form."""
    data_tags = [t for t in ("j", "k") if t in source.keys()[1]]
    return SourceJobNode(
        "keyed",
        tag_schema=Schema({"j": int, "k": int}),
        data_schema=Schema(
            {**{t: int for t in data_tags}, "y": float, "x": int | None}
        ),
        bound_source=source,
    )


@pytest.mark.parametrize(
    ("bind", "tag_only"),
    [
        ("transparent", ()),
        ("transparent", ("j",)),
        ("transparent", ("k",)),
        # A declared slot with one of two data-tags fails on main already, in its
        # own execute() rows (duplicate column); reported separately.
        ("declared", ()),
    ],
    ids=["transparent-both", "transparent-k", "transparent-j", "declared-both"],
)
def test_a_source_nodes_channel_carries_its_execute_output(
    bind: str, tag_only: tuple[str, ...]
) -> None:
    """A source node's batches, and the stream rebuilt from them, are its own
    ``execute()`` output (schema, order, values, content hash), with data-tags in each
    tag position (the declared slot orders the tags j, k against the source's k, j);
    the node's own output channel takes them."""
    source = _keyed_source(tag_only)
    node = (
        SourceJobNode.from_stream(source, name="keyed")
        if bind == "transparent"
        else _declared_node(source)
    )
    expected = StaticOutputOperatorPod._materialize_upstream(
        list(node.iter_data()), node
    )  # pyright: ignore[reportPrivateUsage]  # the job's rebuild of the rows
    wanted = expected.as_table(columns=ALL)
    assert any(
        name.startswith("_source_") and name.removeprefix("_source_") in ("j", "k")
        for name in wanted.column_names
    )  # data-tag tokens are there

    async def read() -> tuple[list[pa.RecordBatch], Any]:
        batches = await node.open_channel().collect()
        output = OutputChannel(node)
        for batch in batches:
            await output.send(batch)  # the node's description takes them
        await output.close()
        return batches, await node.open_channel().to_stream()

    batches, rebuilt = run(read)
    assert same(pa.Table.from_batches(batches), R(expected))
    assert same(rebuilt.as_table(columns=ALL), wanted)
    assert rebuilt.content_hash() == expected.content_hash()


def test_a_rebind_before_the_first_read_is_read_whole() -> None:
    """A channel opened on a source node, then the node rebound: the first read reads
    the new binding only (rows, order and source info from one binding)."""
    node = SourceJobNode.from_stream(_keyed_source(()), name="keyed")
    channel = node.open_channel()
    node.bound_source = _keyed_source((), source_id="other", offset=10)
    expected = StaticOutputOperatorPod._materialize_upstream(
        list(node.iter_data()), node
    )  # pyright: ignore[reportPrivateUsage]  # the job's rebuild of the rows
    batches = run(channel.collect)
    assert same(pa.Table.from_batches(batches), R(expected))


def test_a_source_nodes_conversion_keeps_the_delivered_column_order() -> None:
    """Plain tags declared ``k, j`` over physical columns ``x, j, k, y``: the stream
    ``to_stream()`` makes keeps the columns in the order they were delivered (the
    node's rows), which since ``main`` 4d192bfc is also the slot's (tag order, #486)."""
    stream = ArrowTableSource(
        pa.table(
            {
                "x": pa.array([1, 2], pa.int64()),
                "j": pa.array([3, 4], pa.int64()),
                "k": pa.array([5, 6], pa.int64()),
                "y": pa.array([7.5, 8.5], pa.float64()),
            }
        ),
        tag_columns=["k", "j"],
        tag_only_columns=True,
        source_id="ordered",
    )
    node = SourceJobNode.from_stream(stream, name="ordered")
    assert node.keys()[0] == ("k", "j")  # the slot's order: tag order (#486)

    async def read() -> tuple[list[str], list[str]]:
        batches = await node.open_channel().collect()
        rebuilt = await node.open_channel().to_stream()
        return batches[0].schema.names, rebuilt.as_table(columns=ALL).column_names

    delivered, rebuilt = run(read)
    assert delivered[:2] == ["k", "j"]
    assert rebuilt == delivered


def test_a_source_nodes_description_takes_its_rows_in_their_own_order() -> None:
    """Plain tags declared ``k, j`` over physical columns ``x, j, k, y``: the node's
    own output channel takes the batches its channel delivers."""
    stream = ArrowTableSource(
        pa.table(
            {
                "x": pa.array([1, 2], pa.int64()),
                "j": pa.array([3, 4], pa.int64()),
                "k": pa.array([5, 6], pa.int64()),
                "y": pa.array([7.5, 8.5], pa.float64()),
            }
        ),
        tag_columns=["k", "j"],
        tag_only_columns=True,
        source_id="ordered",
    )
    node = SourceJobNode.from_stream(stream, name="ordered")

    async def read() -> None:
        output = OutputChannel(node)
        for batch in await node.open_channel().collect():
            await output.send(batch)

    run(read)


def test_a_flagged_stream_is_given_as_it_is_only_by_its_own_channel() -> None:
    """The shortcut belongs to a channel opened on a stream ``_output_stream`` built:
    such a channel gives that stream. A reader of an output channel whose description
    is that same stream reads its producer: other rows, or its failure."""
    stream = source(2)
    batches = batches_of(stream, 1)

    async def flagged_stream() -> Any:
        output = OutputChannel(stream)
        for batch in batches:
            await output.send(batch)
        await output.close()
        return await output.open_reader().to_stream()

    flagged = run(flagged_stream)
    assert getattr(flagged, ROW_BUILT_OUTPUT, False)

    async def read() -> None:
        direct = flagged.open_channel()
        assert await direct.to_stream() is flagged
        other = OutputChannel(flagged)
        reader = other.open_reader()
        await other.send(batches[0])
        await other.close()
        rebuilt = await reader.to_stream()
        assert rebuilt is not flagged
        assert rebuilt.as_table().num_rows == 1
        failed = OutputChannel(flagged)
        failing = failed.open_reader()
        failed.fail(OSError("disk gone"))
        with pytest.raises(UpstreamFailedError):
            _ = await failing.to_stream()
        op = SemiJoin()
        semi_output = OutputChannel(op(stream, flagged))
        broken = OutputChannel(flagged)
        broken_reader = broken.open_reader()
        broken.fail(OSError("disk gone"))
        with pytest.raises(UpstreamFailedError):
            await op.async_execute_batches(
                [stream.open_channel(), broken_reader], semi_output
            )

    run(read)


@pytest.mark.parametrize("retain", [True, False], ids=["retained", "bounded"])
def test_a_reader_has_one_consumer(retain: bool) -> None:
    """A receive waiting on a reader holds it: a second consumer of the same reader
    (a conversion, or another receive) is refused, naming it, and the waiting
    receive gets the batch."""
    stream = source(1)
    (batch,) = batches_of(stream)

    async def read() -> int:
        output = OutputChannel(stream, retain=retain)
        reader = output.open_reader()
        waiting = asyncio.ensure_future(reader.receive())
        await asyncio.sleep(0)
        assert not reader.unread
        with pytest.raises(RuntimeError, match="read already|another consumer"):
            _ = await reader.to_stream()
        with pytest.raises(RuntimeError, match="another consumer"):
            _ = await reader.receive()
        await output.send(batch)
        await output.close()
        return (await waiting).num_rows

    assert run(read) == 1


@pytest.mark.parametrize("retain", [True, False], ids=["retained", "bounded"])
def test_a_reader_belongs_to_its_first_consumer_between_batches(retain: bool) -> None:
    """Task A iterates a reader and pauses after its first batch; task B's read of the
    same reader, between A's batches, is refused, and A reads all three batches."""
    stream = source(3)
    batches = batches_of(stream, 1)

    async def read() -> int:
        output = OutputChannel(stream, retain=retain, buffer_size=4)
        reader = output.open_reader()
        for batch in batches:
            await output.send(batch)
        await output.close()
        paused, resume = asyncio.Event(), asyncio.Event()

        async def consumer_a() -> int:
            seen = 0
            async for _ in reader:
                seen += 1
                if seen == 1:
                    paused.set()
                    await resume.wait()
            return seen

        async def consumer_b() -> None:
            _ = await reader.receive()

        a = asyncio.ensure_future(consumer_a())
        await paused.wait()
        with pytest.raises(RuntimeError, match="belongs to another consumer"):
            await asyncio.ensure_future(consumer_b())
        resume.set()
        return await a

    assert run(read) == 3


@pytest.mark.parametrize("retain", [True, False], ids=["retained", "bounded"])
def test_a_reserved_reader_is_read_only_through_its_claim(retain: bool) -> None:
    """``reserve()`` holds the reader for its claim: no read is taken (from another
    task, nor from the reserving task) until a task takes it over with the claim;
    then only that task reads."""
    stream = source(2)
    batches = batches_of(stream, 1)

    async def read() -> int:
        output = OutputChannel(stream, retain=retain, buffer_size=4)
        reader = output.open_reader()
        for batch in batches:
            await output.send(batch)
        await output.close()
        claim = reader.reserve()
        with pytest.raises(RuntimeError, match="held for a conversion"):
            _ = await reader.receive()

        async def other() -> None:
            _ = await reader.receive()

        with pytest.raises(RuntimeError, match="held for a conversion"):
            await asyncio.ensure_future(other())
        with pytest.raises(RuntimeError, match="claimed by another consumer"):
            _ = reader.reserve(object())

        async def owner() -> int:
            _ = reader.reserve(claim)
            return len(await reader.collect())

        taken = await asyncio.ensure_future(owner())
        with pytest.raises(ChannelClosed):
            _ = await reader.receive()  # read to its end: nothing left, for anyone
        return taken

    assert run(read) == 2


@pytest.mark.parametrize("retain", [True, False], ids=["retained", "bounded"])
def test_a_conversion_reserves_the_reader_when_it_is_created(retain: bool) -> None:
    """``pending = reader.to_stream()`` reserves at once: a receive before
    ``await pending`` is refused, and the conversion gets every batch."""
    stream = source(2)
    batches = batches_of(stream, 1)

    async def read() -> int:
        output = OutputChannel(stream, retain=retain, buffer_size=4)
        reader = output.open_reader()
        for batch in batches:
            await output.send(batch)
        await output.close()
        pending = reader.to_stream()
        with pytest.raises(RuntimeError, match="held for a conversion"):
            _ = await reader.receive()
        return (await pending).as_table().num_rows

    assert run(read) == 2


def test_a_claim_is_taken_over_once() -> None:
    """A redeemed claim cannot be redeemed again: the channel stays with the task that
    took it over."""
    stream = source(2)
    batches = batches_of(stream, 1)

    async def read() -> int:
        output = OutputChannel(stream)
        reader = output.open_reader()
        for batch in batches:
            await output.send(batch)
        await output.close()
        claim = reader.reserve()
        _ = reader.reserve(claim)

        async def thief() -> None:
            _ = reader.reserve(claim)

        with pytest.raises(RuntimeError, match="claimed by another consumer"):
            await asyncio.ensure_future(thief())
        return len(await reader.collect())

    assert run(read) == 2


def test_a_reader_releases_its_consumer_task_at_its_end_and_on_close() -> None:
    """The consumer task is not kept by the reader once it has nothing left to read:
    after the end, or after ``close()``."""
    stream = source(2)
    batches = batches_of(stream, 1)

    async def read() -> tuple[bool, bool]:
        released: list[bool] = []
        for ending in ("end", "close"):
            output = OutputChannel(stream)
            reader = output.open_reader()
            for batch in batches:
                await output.send(batch)
            await output.close()

            async def consume() -> None:
                if ending == "end":
                    _ = await reader.collect()
                else:
                    _ = await reader.receive()
                    await reader.close()

            task = asyncio.ensure_future(consume())
            await task
            reference = weakref.ref(task)
            del task
            await asyncio.sleep(0)  # the loop drops its own references to the task
            gc.collect()
            released.append(reference() is None)
        return released[0], released[1]

    assert run(read) == (True, True)


class _StoppingFeed:
    """A feed whose second read raises ``StopAsyncIteration`` instead of returning
    ``None``: a broken feed, not the end of its input."""

    def __init__(self, batch: pa.RecordBatch) -> None:
        self._batch: pa.RecordBatch | None = batch

    async def next(self) -> pa.RecordBatch | None:
        """Return the one batch, then raise ``StopAsyncIteration``."""
        if self._batch is None:
            raise StopAsyncIteration
        batch, self._batch = self._batch, None
        return batch

    def close(self) -> None:
        """Nothing to release."""

    @property
    def opened_on(self) -> None:
        """Not a complete stream's feed."""
        return None


def test_a_feed_raising_stop_async_iteration_is_a_read_error() -> None:
    """Only a feed returning ``None`` ends a channel: a raised ``StopAsyncIteration``
    is a read error, so ``collect()`` and a barrier never take one batch for all."""
    stream = source(2)
    (first, _) = batches_of(stream, 1)

    async def read() -> None:
        with pytest.raises(RuntimeError, match="StopAsyncIteration"):
            _ = await InputChannel(stream, _StoppingFeed(first)).collect()
        with pytest.raises(RuntimeError, match="StopAsyncIteration"):
            _ = await InputChannel(stream, _StoppingFeed(first)).to_stream()

    run(read)


def test_a_barrier_leaves_no_conversion_behind_when_an_input_was_read() -> None:
    """A barrier whose second input was read already: the second input's setup fails
    synchronously, before the first input's conversion (already scheduled) has begun.
    That conversion is cancelled and awaited before the barrier raises, its reader is
    closed and the producer released, so no task fails later with nobody to hear it.
    A failure after a barrier has taken a batch is
    ``test_a_barrier_that_read_part_of_a_failed_input_never_computes``."""
    left = source(2)
    right = source(2, source_id="other")
    left_batches = batches_of(left, 1)
    unheard: list[Any] = []

    async def read() -> None:
        asyncio.get_running_loop().set_exception_handler(
            lambda _loop, context: unheard.append(context)
        )
        producer = OutputChannel(left, retain=False, buffer_size=1)
        first = producer.open_reader()
        second = right.open_channel()
        _ = await second.receive()
        await producer.send(left_batches[0])  # the conversion takes it and waits
        op = SemiJoin()
        output = OutputChannel(op(left, right))
        with pytest.raises(RuntimeError, match="read already"):
            await op.async_execute_batches([first, second], output)
        # The conversion is done: its reader is closed and its producer no longer
        # waits for it.
        async with asyncio.timeout(TIMEOUT):
            await producer.send(left_batches[1])
            await producer.send(left_batches[1])
        with pytest.raises(ChannelClosed):
            _ = await first.receive()
        gc.collect()
        await asyncio.sleep(0.01)

    run(read)
    gc.collect()
    assert unheard == []


@pytest.mark.parametrize("never", ["cancelled", "dropped"])
def test_a_conversion_that_never_starts_releases_its_reader(never: str) -> None:
    """``to_stream()`` reserves at call time. If its conversion never starts (its
    task is cancelled before the first step, or it is dropped unawaited), the
    reservation is released: the reader is closed and a bounded producer no longer
    waits on it."""
    stream = source(4)
    batches = batches_of(stream, 1)

    async def read() -> None:
        output = OutputChannel(stream, retain=False, buffer_size=1)
        reader = output.open_reader()
        if never == "cancelled":
            task = asyncio.ensure_future(reader.to_stream())
            _ = task.cancel()
            _ = await asyncio.gather(task, return_exceptions=True)
            del task
        else:
            pending = reader.to_stream()
            pending.close()  # what dropping an unawaited coroutine does
            del pending
        gc.collect()
        for batch in batches:  # would wait forever on a held reader of one slot
            await asyncio.wait_for(output.send(batch), 1)
        await output.close()
        with pytest.raises(ChannelClosed):
            _ = await reader.receive()

    run(read)


def test_a_dropped_reader_whose_claim_is_freed_later_releases_its_producer() -> None:
    """A reader of a bounded output is reserved, then the reader object is dropped
    before its claim: when the claim is freed, the reader's place in the output is
    released all the same, so sends past its queue complete."""
    stream = source(4)
    batches = batches_of(stream, 1)

    async def read() -> None:
        output = OutputChannel(stream, retain=False, buffer_size=1)
        reader = output.open_reader()
        claim = reader.reserve()
        del reader
        gc.collect()  # the channel is gone; its claim is not
        del claim
        gc.collect()
        for batch in batches:  # would wait forever on a reader still registered
            await asyncio.wait_for(output.send(batch), 1)
        await output.close()

    run(read)


def test_a_closed_reader_cannot_be_converted() -> None:
    """A reader closed before any read: ``to_stream()`` raises ``ChannelClosed``,
    as documented, not the error of a channel already read."""
    stream = source(1)

    async def read() -> None:
        reader = OutputChannel(stream).open_reader()
        await reader.close()
        with pytest.raises(ChannelClosed, match="closed before its producer"):
            _ = await reader.to_stream()

    run(read)


class _ClosingFetch(FakeDynamicSource):
    """A dynamic source whose fetch raises ``ChannelClosed`` (its own input closed
    early); its schema is declared, so nothing before the read fetches."""

    async def fetch(self, cursor: Any = None) -> Any:
        """Record the fetch, then fail it as a source whose own input closed."""
        self.fetch_cursors.append(cursor)
        raise ChannelClosed("underlying input closed early")


def test_a_channel_closed_inside_the_read_is_a_read_error() -> None:
    """A ``ChannelClosed`` raised by the feed's own read (the source's) is not the end
    of the input: iteration, ``collect`` and ``to_stream()`` raise it, and a
    barrier fails before its body."""
    declared = Schema({"id": int, "val": int})

    def polling() -> PollingSource[Any]:
        fake = _ClosingFetch(
            batches=[_batch(1, 10), _batch(2, 20)], schema_override=declared
        )
        return PollingSource(
            fake, tag_columns="id", record_id_columns="id", source_id="closing"
        )

    async def read() -> None:
        with pytest.raises(ChannelClosed, match="underlying input"):
            _ = await polling().open_channel().collect()
        channel = polling().open_channel()
        with pytest.raises(ChannelClosed, match="underlying input"):
            _ = await channel.to_stream()
        assert not channel.unread
        left = source()
        right = polling()
        op = SemiJoin()
        output = OutputChannel(op(left, right))
        with pytest.raises(ChannelClosed, match="underlying input"):
            await op.async_execute_batches(
                [left.open_channel(), right.open_channel()], output
            )

    run(read)


def test_a_delivered_batch_is_not_kept_by_the_feed() -> None:
    """Once delivered, a batch is not held by the channel: a consumer that drops it
    frees it, also while the channel is still open."""
    n = SOURCE_BATCH_ROWS + 3
    stream = ArrowTableSource(
        pa.table(
            {"k": pa.array(range(n), pa.int64()), "v": pa.array(range(n), pa.int64())}
        ),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="large",
    )

    async def read() -> bool:
        channel = stream.open_channel()
        first = await channel.receive()
        reference = weakref.ref(first)
        del first
        gc.collect()
        released_while_open = reference() is None  # before the channel is drained
        rest = await channel.collect()
        assert sum(batch.num_rows for batch in rest) == 3
        return released_while_open

    assert run(read)


def test_an_unbound_input_fails_a_barrier_before_it_runs() -> None:
    """``SemiJoin`` with no shared tag never reads its right input on the sync path,
    but ``to_stream()`` reads every input first: an unbound right input (a slot,
    or a ``SourceProxy``) raises what reading it raises, before the operator runs."""
    left = source()
    other = ArrowTableSource(
        pa.table({"other": pa.array([1], pa.int64()), "v": pa.array([0.5])}),
        tag_columns=["other"],
        tag_only_columns=True,
        source_id="other",
    )
    slot = SourceNode.from_stream(other, name="other")
    proxy = SourceProxy(
        source_id="other",
        expected_class_name="ArrowTableSource",
        tag_schema=Schema({"other": int}),
        data_schema=Schema({"v": float}),
        content_hash_str=other.content_hash().to_string(),
        pipeline_hash_str=other.pipeline_hash().to_string(),
    )
    for right, raised in ((slot, UnboundSourceError), (proxy, NotImplementedError)):
        op = SemiJoin()

        async def go(right: Any = right) -> None:
            output = OutputChannel(op(left, right))
            await op.async_execute_batches(
                [left.open_channel(), right.open_channel()], output
            )

        with pytest.raises(raised):
            run(go)


def test_an_exhausted_channel_never_yields_rows_later() -> None:
    """A job node's channel read to its end before the node's run delivered nothing;
    after the run, the same channel neither gives the run's rows nor reads again."""
    job = PipelineJob(name="exhausted", database=InMemoryArrowDatabase())
    with job:
        _ = SelectDataColumns(["subject", "trial", "note"])(source(), label="selected")
    node = job.nodes["selected"]

    async def before_run() -> Any:
        channel = node.open_channel()
        assert await channel.collect() == []
        return channel

    channel = run(before_run)
    _ = job.run(orchestrator=SyncPipelineOrchestrator())
    assert node.as_table().num_rows == 7

    async def after_run() -> None:
        assert await channel.collect() == []  # single pass: nothing more
        with pytest.raises(RuntimeError, match="read already"):
            _ = await channel.to_stream()

    run(after_run)


def test_a_polling_source_is_read_once_through_a_channel() -> None:
    """A fresh ``PollingSource``: opening a channel fetches nothing; reading it fetches
    once and the stream ``to_stream()`` gives is a fixed snapshot, read again
    with no further fetch, also through ``collect``."""
    fake = FakeDynamicSource(batches=[_batch(i, 10 * i) for i in range(1, 6)])
    polling = PollingSource(
        fake, tag_columns="id", record_id_columns="id", source_id="polled"
    )

    async def read() -> None:
        channel = polling.open_channel()
        other = polling.open_channel()
        assert len(fake.fetch_cursors) == 0  # opening reads nothing
        snapshot = await channel.to_stream()
        fetched = len(fake.fetch_cursors)
        assert fetched == 1  # exactly one fetch for the channel's first read
        assert len(await other.collect()) >= 1
        assert len(fake.fetch_cursors) == 2  # and one for the other channel's
        fetched = len(fake.fetch_cursors)
        rows = snapshot.as_table().num_rows
        assert snapshot.as_table().num_rows == rows  # a fixed snapshot
        assert len(fake.fetch_cursors) == fetched  # read again: no fetch
        second = polling.open_channel()
        assert len(fake.fetch_cursors) == fetched
        batches = await second.collect()
        read_once = len(fake.fetch_cursors)
        assert await second.collect() == []  # one pass
        assert len(fake.fetch_cursors) == read_once  # never read again
        assert batches

    run(read)


def test_opening_a_channel_computes_nothing() -> None:
    """The stream's table is read at the first ``receive``, not when the channel opens."""
    stream = source()
    with mock.patch.object(
        ArrowTableSource,
        "as_table",
        autospec=True,
        side_effect=ArrowTableSource.as_table,
    ) as read:
        channel = stream.open_channel()
        assert read.call_count == 0
        _ = run(channel.receive)
        assert read.call_count == 1


def test_two_channels_on_one_stream_have_their_own_positions() -> None:
    """Two channels opened on one stream: each reader gets every row, and the stream stays."""
    stream = source()

    async def read() -> tuple[int, int, int]:
        first, second = stream.open_channel(), stream.open_channel()
        one = await first.receive()
        rest = await second.collect()
        return one.num_rows, sum(b.num_rows for b in rest), len(await first.collect())

    assert run(read) == (7, 7, 0)
    assert stream.as_table().num_rows == 7


def test_a_stream_is_read_in_record_batches_of_bounded_size() -> None:
    """A complete stream sends record batches of at most ``SOURCE_BATCH_ROWS`` rows."""
    n = SOURCE_BATCH_ROWS + 10
    stream = ArrowTableSource(
        pa.table({"k": pa.array(range(n), pa.int32()), "x": pa.array(range(n))}),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="many",
    )
    sizes = [batch.num_rows for batch in run(stream.open_channel().collect)]
    assert sizes == [SOURCE_BATCH_ROWS, 10]


def test_a_channel_is_read_once_and_the_stream_again() -> None:
    """A channel is single-pass; to read again, go back to a stream."""

    async def read() -> tuple[int, int]:
        channel = source().open_channel()
        first = await channel.collect()
        with pytest.raises(ChannelClosed):
            _ = await channel.receive()
        return len(first), len(await channel.collect())

    assert run(read) == (1, 0)


def test_to_stream_is_everything_or_nothing() -> None:
    """``to_stream()`` on a channel already read is refused: never part of an
    input, and a channel is never read again."""

    batches = batches_of(source(), 2)

    async def read() -> None:
        output = OutputChannel(source())
        for batch in batches:
            await output.send(batch)
        await output.close()
        channel = output.open_reader()
        _ = await channel.receive()
        with pytest.raises(RuntimeError, match="read already"):
            _ = await channel.to_stream()

    run(read)


# --- An output channel closes into a stream ------------------------------------------


@pytest.mark.parametrize(
    "rows", [1, 2, 3, SOURCE_BATCH_ROWS], ids=lambda n: f"{n}_rows"
)
def test_result_is_the_batches_sent_in_order_as_the_nodes_stream(rows: int) -> None:
    """After close, ``result()`` is one re-readable stream with the node's identity."""
    stream = source()
    batches = batches_of(stream, rows)

    async def write() -> OutputChannel:
        output = OutputChannel(stream)
        assert isinstance(output, WritableChannel)
        as_protocol: RecordBatchOutput = (
            output  # what an operator writes (type-checked)
        )
        assert as_protocol is output
        assert output.stream is stream
        for batch in batches:
            await output.send(batch)
        await output.close()
        return output

    output = run(write)
    result = output.result()
    assert result is output.result()  # one object: the node's kept output
    assert same(result.as_table(columns=ALL), R(stream))
    assert same(result.as_table(columns=ALL), R(result))  # re-readable, as rows too
    assert result.pipeline_hash() == stream.pipeline_hash()
    assert result.data_context_key == stream.data_context_key
    assert result.upstreams == stream.upstreams


def test_the_identity_of_a_result_with_and_without_a_producer() -> None:
    """``result()`` and ``to_stream()`` take the describing stream's identity: its
    producer and upstreams when it has a producer, its pipeline hash when it has none."""
    root = source()
    produced = SelectDataColumns(["subject", "trial", "amplitude"])(root)

    async def both(stream: Any, batches: list[pa.RecordBatch]) -> tuple[Any, Any]:
        output = OutputChannel(stream)
        for batch in batches:
            await output.send(batch)
        await output.close()
        return output.result(), await output.open_reader().to_stream()

    for stream in (root, produced):
        batches = batches_of(stream, 3)
        for made in run(lambda stream=stream, batches=batches: both(stream, batches)):
            assert made.producer is stream.producer
            assert made.upstreams == stream.upstreams
            assert made.pipeline_hash() == stream.pipeline_hash()
            assert same(made.as_table(columns=ALL), R(stream))
    assert produced.producer is not None and root.producer is None


def test_a_close_with_no_batch_is_the_typed_zero_row_stream() -> None:
    """Zero rows: ``result()`` and ``to_stream()`` are typed by the description, never
    by rows, with the columns of a run with rows."""
    stream = SelectDataColumns(["subject", "trial", "amplitude", "trace"])(source())
    with_rows = R(stream)

    async def empty() -> tuple[Any, Any]:
        output = OutputChannel(stream)
        reader = output.open_reader()
        await output.close()
        await output.close()  # a second close does nothing
        return output.result(), await reader.to_stream()

    for made in run(empty):
        table = made.as_table(columns=ALL)
        assert table.num_rows == 0
        assert table.column_names == with_rows.column_names
        assert [f.type for f in table.schema] == [f.type for f in with_rows.schema]
        assert made.pipeline_hash() == stream.pipeline_hash()


def test_send_after_close_is_refused_and_result_needs_a_closed_channel() -> None:
    """A send after ``close()`` raises ``ChannelClosed``, and ``result()`` exists only
    once the channel is closed."""
    stream = source()
    (batch,) = batches_of(stream)

    async def write() -> None:
        output = OutputChannel(stream)
        await output.send(batch)
        with pytest.raises(RuntimeError, match="not closed"):
            _ = output.result()
        await output.close()
        with pytest.raises(ChannelClosed):
            await output.send(batch)
        assert output.result().as_table().num_rows == 7

    run(write)


def test_a_batch_of_other_columns_is_refused() -> None:
    """``send`` checks each record batch against the stream's description."""
    stream = source()
    (batch,) = batches_of(stream)
    renamed = batch.rename_columns(
        ["other" if name == "note" else name for name in batch.schema.names]
    )
    retyped = batch.set_column(
        batch.schema.get_field_index("amplitude"),
        "amplitude",
        batch.column("amplitude").cast(pa.float64()),
    )
    reflagged = pa.RecordBatch.from_arrays(
        batch.columns,
        schema=pa.schema(
            [
                f.with_nullable(not f.nullable) if f.name == "trial" else f
                for f in batch.schema
            ]
        ),
    )

    async def write() -> None:
        output = OutputChannel(stream)
        for wrong, said in (
            (renamed, "'other'"),
            (retyped, "column 'amplitude' is double"),
            (reflagged, "column 'trial' is (not )?nullable"),
        ):
            with pytest.raises(ValueError, match=said):
                await output.send(wrong)
        await output.send(batch)

    run(write)


def _in_order(batch: pa.RecordBatch, names: Sequence[str]) -> pa.RecordBatch:
    return batch.select(list(names))


@pytest.mark.parametrize("retain", [True, False], ids=["retained", "bounded"])
@pytest.mark.parametrize("declared", [True, False], ids=["declared", "undeclared"])
def test_the_first_batch_fixes_the_column_order(retain: bool, declared: bool) -> None:
    """Columns are compared by name: a batch whose columns are the stream's in another
    order is taken (a column order is not a result). The first batch with rows fixes
    the channel's order, and every later batch is put into it, never the other way
    round."""
    table = source().as_table(columns=ALL)
    stream = (
        source()
        if declared
        else _UntypedStream(
            table, tag_columns=["subject", "trial"], data_tags=["trial"]
        )
    )
    assert stream.arrow_types_declared is declared
    first, second, third = batches_of(stream, 3)
    names = first.schema.names
    reversed_names = list(reversed(names))
    shuffled = names[1::2] + names[::2]
    assert len({tuple(names), tuple(reversed_names), tuple(shuffled)}) == 3
    sent = [
        _in_order(first, reversed_names),
        second,  # the description's order
        _in_order(third, shuffled),
    ]

    def names_of(batch: pa.RecordBatch) -> list[str]:
        return batch.schema.names

    async def through_reader() -> tuple[list[list[str]], Any]:
        output = OutputChannel(stream, retain=retain, buffer_size=4)
        reader = output.open_reader()
        received: list[list[str]] = []
        for batch in sent:
            await output.send(batch)
            received.append(names_of(await reader.receive()))
        await output.close()
        return received, output.result() if retain else None

    received, result = run(through_reader)
    assert received == [reversed_names] * 3
    if result is not None:
        rows = result.as_table(columns=ALL)
        assert rows.num_rows == 7
        assert sorted(rows.column_names) == sorted(names)

        async def in_description_order() -> Any:
            output = OutputChannel(stream)
            for batch in (first, second, third):
                await output.send(batch)
            await output.close()
            return output.result()

        expected = run(in_description_order).as_table(columns=ALL)
        assert same(rows.select(expected.column_names), expected)


@pytest.mark.parametrize("declared", [True, False], ids=["declared", "undeclared"])
def test_a_reordered_batch_must_still_have_the_streams_columns(declared: bool) -> None:
    """Order aside, the check is unchanged: names match exactly (case counts), and each
    column's type and nullability are the description's (or the first batch's, without
    Arrow types). A refused batch fixes no order."""
    table = source().as_table(columns=ALL)
    stream = (
        source()
        if declared
        else _UntypedStream(
            table, tag_columns=["subject", "trial"], data_tags=["trial"]
        )
    )
    first, second = batches_of(stream, 4)
    reversed_names = list(reversed(first.schema.names))

    def reordered(batch: pa.RecordBatch) -> pa.RecordBatch:
        return _in_order(batch, reversed_names)

    recased = reordered(second).rename_columns(
        ["Note" if name == "note" else name for name in reversed_names]
    )
    retyped = reordered(second)
    retyped = retyped.set_column(
        retyped.schema.get_field_index("amplitude"),
        "amplitude",
        retyped.column("amplitude").cast(pa.float64()),
    )
    reflagged = reordered(second)
    reflagged = pa.RecordBatch.from_arrays(
        reflagged.columns,
        schema=pa.schema(
            [
                f.with_nullable(not f.nullable) if f.name == "trial" else f
                for f in reflagged.schema
            ]
        ),
    )

    async def write() -> list[str]:
        output = OutputChannel(stream)
        reader = output.open_reader()
        await output.send(first)  # fixes the description's order
        for wrong, said in (
            (recased, "'Note'"),
            (retyped, "column 'amplitude' is double"),
            (reflagged, "column 'trial' is (not )?nullable"),
        ):
            with pytest.raises(ValueError, match=said):
                await output.send(wrong)
        await output.send(reordered(second))
        _ = await reader.receive()
        return (await reader.receive()).schema.names

    assert run(write) == first.schema.names


def test_what_a_description_does_not_say_is_accepted() -> None:
    """A ``null`` column travels as ``large_string``, and a source-info field's
    nullability is set over the complete table: neither is a mismatch."""
    stream = ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2, 3], pa.int64()),
                "x": pa.array([1.5, 2.5, 3.5], pa.float64()),
                "unknown": pa.array([None, None, None], pa.null()),
                "_source_x": pa.array(
                    ["s::r0::x", None, "s::r2::x"], pa.large_string()
                ),
            }
        ),
        tag_columns=["k"],
    )
    assert stream.as_table().schema.field("unknown").type == pa.null()

    batches = batches_of(stream, 1)
    assert len({batch.schema for batch in batches}) == 1  # one schema in transit

    async def through() -> Any:
        output = OutputChannel(stream)
        for batch in batches:
            await output.send(batch)
        await output.close()
        return output.result()

    result = run(through)
    table = result.as_table(columns=ALL)
    assert same(table, R(stream))
    assert table.schema.field("unknown").type == pa.large_string()
    assert table.schema.field("_source_x").nullable  # one token is null


def test_a_batch_in_another_representation_is_refused() -> None:
    """Type strictness at ``send``: exactly the description's types in their large
    variants (``string`` for a ``large_string`` column is refused, never converted),
    the description's field metadata, and only nulls where the description has
    ``null``."""
    stream = source()
    (batch,) = batches_of(stream)
    note = batch.schema.get_field_index("note")
    narrow = batch.set_column(note, "note", batch.column("note").cast(pa.string()))
    tagged = pa.RecordBatch.from_arrays(
        batch.columns,
        schema=batch.schema.set(
            note, batch.schema.field(note).with_metadata({"unit": "mV"})
        ),
    )
    nulls = ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "x": pa.array([1.5, 2.5], pa.float64()),
                "unknown": pa.array([None, None], pa.null()),
            }
        ),
        tag_columns=["k"],
    )
    (null_batch,) = batches_of(nulls)
    unknown = null_batch.schema.get_field_index("unknown")
    filled = null_batch.set_column(
        unknown,
        null_batch.schema.field(unknown),
        pa.array(["a value", None], pa.large_string()),
    )

    async def write() -> None:
        output = OutputChannel(stream)
        with pytest.raises(ValueError, match="column 'note' is string"):
            await output.send(narrow)
        with pytest.raises(ValueError, match="column 'note' has field metadata"):
            await output.send(tagged)
        await output.send(batch)
        null_output = OutputChannel(nulls)
        with pytest.raises(ValueError, match="'unknown' holds values"):
            await null_output.send(filled)
        await null_output.send(null_batch)

    run(write)


def test_a_batch_with_no_rows_is_checked_and_not_kept() -> None:
    """A zero-row batch is not held, retained or queued, and no reader gets it."""
    stream = source(3)
    (batch,) = batches_of(stream)
    empty = batch.slice(0, 0)

    async def write() -> tuple[list[int], list[int], int]:
        kept = OutputChannel(stream)
        queued = OutputChannel(stream, retain=False, buffer_size=1)
        reader = queued.open_reader()
        for output in (kept, queued):
            for _ in range(3):
                await output.send(empty)  # never waits, also with one slot
            with pytest.raises(ValueError):
                await output.send(empty.rename_columns(["x"] * empty.num_columns))
        await kept.send(batch)
        await kept.close()
        await queued.close()
        late = kept.open_reader()
        return (
            [b.num_rows for b in await late.collect()],
            [b.num_rows for b in await reader.collect()],
            kept.result().as_table().num_rows,
        )

    assert run(write) == ([3], [], 3)


@pytest.mark.parametrize("ending", ["close", "fail"])
def test_a_waiting_send_ends_when_the_channel_ends(ending: str) -> None:
    """A send waiting for a reader's room when the channel is closed or failed raises
    at once (``ChannelClosed``, or the failure) and its batch is never delivered. A
    close during a send does not complete the channel: it fails, so the reader's next
    read raises, with a ``ChannelClosed`` as the cause."""
    stream = source(2)
    first, second = batches_of(stream, 1)
    boom = OSError("disk gone")

    async def write() -> tuple[type[object], list[int]]:
        output = OutputChannel(stream, retain=False, buffer_size=1)
        reader = output.open_reader()
        await output.send(first)  # fills the one slot
        waiting = asyncio.ensure_future(output.send(second))
        await asyncio.sleep(0)
        assert not waiting.done()
        if ending == "close":
            await output.close()
        else:
            output.fail(boom)
        raised = await asyncio.wait_for(
            asyncio.gather(waiting, return_exceptions=True), 1
        )
        delivered: list[int] = []
        with pytest.raises(UpstreamFailedError) as failed:
            async for batch in reader:
                delivered.append(batch.num_rows)
        cause = failed.value.__cause__
        assert isinstance(cause, ChannelClosed if ending == "close" else OSError)
        return type(raised[0]), delivered

    raised, delivered = run(write)
    assert raised is (ChannelClosed if ending == "close" else UpstreamFailedError)
    assert delivered == []  # a failure is seen at once


@pytest.mark.parametrize("ending", ["close", "fail"])
def test_a_send_scheduled_before_the_end_of_a_retained_output_publishes_nothing(
    ending: str,
) -> None:
    """Retained: a send that has not run when the channel ends raises, and the result
    and every reader hold only what was sent before."""
    stream = source(2)
    first, second = batches_of(stream, 1)

    async def write() -> tuple[type[object], list[int]]:
        output = OutputChannel(stream)
        await output.send(first)
        scheduled = asyncio.ensure_future(output.send(second))
        if ending == "close":
            await output.close()
        else:
            output.fail(OSError("disk gone"))
        (raised,) = await asyncio.gather(scheduled, return_exceptions=True)
        rows = [first.num_rows]
        if ending == "close":
            rows = [b.num_rows for b in await output.open_reader().collect()]
            assert output.result().as_table().num_rows == 1
        return type(raised), rows

    raised, rows = run(write)
    assert raised is (ChannelClosed if ending == "close" else UpstreamFailedError)
    assert rows == [1]


def test_to_stream_of_a_channel_closed_before_its_end_raises() -> None:
    """A reader closed while ``to_stream()`` (or ``collect()``) reads never makes part
    of an input into a stream."""
    stream = source(2)
    first, second = batches_of(stream, 1)

    async def write() -> None:
        for read in ("to_stream", "collect"):
            output = OutputChannel(stream, retain=False, buffer_size=4)
            reader = output.open_reader()
            reading = asyncio.ensure_future(
                reader.to_stream() if read == "to_stream" else reader.collect()
            )
            await output.send(first)
            await asyncio.sleep(0)
            await reader.close()
            await output.send(second)
            await output.close()
            with pytest.raises(ChannelClosed, match="closed before its producer"):
                _ = await reading

    run(write)


def test_a_cancelled_read_closes_the_reader_and_frees_the_producer() -> None:
    """Cancelling a waiting ``receive()`` closes that reader: the producer is not held
    by a consumer that stopped reading."""
    stream = source(3)
    batches = batches_of(stream, 1)

    async def write() -> None:
        output = OutputChannel(stream, retain=False, buffer_size=1)
        reader = output.open_reader()
        waiting = asyncio.ensure_future(reader.receive())
        await asyncio.sleep(0)
        _ = waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            _ = await waiting
        for batch in batches:  # would wait forever on the abandoned reader
            await asyncio.wait_for(output.send(batch), 1)
        with pytest.raises(ChannelClosed):
            _ = await reader.receive()

    run(write)


def test_a_closed_reader_is_unregistered() -> None:
    """``close()`` removes the reader from its output channel: readers opened and
    closed leave nothing for a send to walk."""
    stream = source(2)
    batches = batches_of(stream, 1)

    async def write() -> int:
        output = OutputChannel(stream, retain=False)
        for _ in range(100):
            await output.open_reader().close()
        staying = output.open_reader()
        for batch in batches:
            await output.send(batch)
        await output.close()
        assert len(await staying.collect()) == 2
        return len(output._readers)  # pyright: ignore[reportPrivateUsage]  # registration is what is tested

    assert run(write) == 1


def test_a_close_during_a_send_that_reached_one_reader_fails_both() -> None:
    """Two readers with one slot each: a send has queued its batch for the fast reader
    and waits for the slow one when the channel is closed. The send raises
    ``ChannelClosed``; neither reader completes normally (their inputs would differ):
    each next read raises ``UpstreamFailedError`` caused by a ``ChannelClosed``."""
    stream = source(2)
    first, second = batches_of(stream, 1)

    async def write() -> list[BaseException]:
        output = OutputChannel(stream, retain=False, buffer_size=1)
        fast, slow = output.open_reader(), output.open_reader()
        await output.send(first)
        _ = await fast.receive()
        sending = asyncio.ensure_future(output.send(second))
        await asyncio.sleep(0)
        assert not sending.done()  # queued for fast, waiting for slow
        await output.close()
        with pytest.raises(ChannelClosed):
            await sending
        failures: list[BaseException] = []
        for reader in (fast, slow):
            with pytest.raises(UpstreamFailedError) as failed:
                _ = await reader.collect()
            failures.append(failed.value)
        with pytest.raises(RuntimeError, match="has no result"):
            _ = output.result()
        return failures

    for failure in run(write):
        assert isinstance(failure.__cause__, ChannelClosed)


def test_metadata_is_compared_at_every_depth() -> None:
    """A user field's nested field metadata must be the description's; a provenance
    field (system tag, source info) carries none, as a row builds it."""
    stream = source()
    (batch,) = batches_of(stream)
    trace = batch.schema.get_field_index("trace")
    trace_type = batch.schema.field(trace).type
    tagged_item = pa.large_list(trace_type.value_field.with_metadata({"units": "b"}))
    nested = batch.set_column(
        trace,
        batch.schema.field(trace).with_type(tagged_item),
        batch.column(trace).cast(tagged_item),
    )
    system_tag = next(
        i for i, name in enumerate(batch.schema.names) if name.startswith("_tag")
    )
    provenance = pa.RecordBatch.from_arrays(
        batch.columns,
        schema=batch.schema.set(
            system_tag, batch.schema.field(system_tag).with_metadata({"m": "1"})
        ),
    )

    async def write() -> None:
        output = OutputChannel(stream)
        with pytest.raises(ValueError, match="column 'trace' has field metadata"):
            await output.send(nested)
        with pytest.raises(ValueError, match="carries field metadata"):
            await output.send(provenance)
        await output.send(batch)

    run(write)


def test_nulls_are_judged_on_the_values_sent() -> None:
    """A struct row that is null may hold a value in its child's buffer, and a slice of
    a map shares its backing array with rows outside it: neither is a value sent."""
    stream = ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "s": pa.array(
                    [None, {"a": None}], pa.struct([pa.field("a", pa.null())])
                ),
            }
        ),
        tag_columns=["k"],
    )
    (batch,) = batches_of(stream)
    index = batch.schema.get_field_index("s")
    carried = batch.schema.field(index).type
    masked = pa.StructArray.from_arrays(
        [pa.array(["in the buffer", None], pa.large_string())],
        fields=[carried.field(0)],
        mask=pa.array([True, False]),
    )
    assert masked.to_pylist() == [None, {"a": None}]  # pyright: ignore[reportAttributeAccessIssue]  # the stubs omit it
    buffered = batch.set_column(index, batch.schema.field(index), masked)

    async def write() -> None:
        output = OutputChannel(stream)
        await output.send(buffered)
        await output.close()

    run(write)

    # A map column declared map<large_string, null>, sent as one row of a longer map
    # array whose other row holds a value.
    declared = pa.schema([pa.field("m", pa.map_(pa.large_string(), pa.null()))])
    whole = pa.array(
        [[("a", None)], [("b", "a value")]],
        pa.map_(pa.large_string(), pa.large_string()),
    )
    sent = pa.RecordBatch.from_arrays([whole.slice(0, 1)], names=["m"])
    assert _schema_mismatch(declared, sent) is None
    other = pa.RecordBatch.from_arrays([whole.slice(1, 1)], names=["m"])
    assert "holds values" in str(_schema_mismatch(declared, other))


class _UntypedStream(ArrowTableStream):
    """A stream that cannot say its Arrow types without rows."""

    def _typed_zero_row_table(self) -> pa.Table | None:  # pyright: ignore[reportIncompatibleMethodOverride]  # a kind that cannot type itself without rows
        return None


def test_without_arrow_types_the_names_are_still_the_streams() -> None:
    """A stream with no typed zero-row table: the first record batch gives the Arrow
    types, but a batch whose columns contradict the stream's keys is refused."""
    table = source().as_table(columns=ALL)
    stream = _UntypedStream(
        table, tag_columns=["subject", "trial"], data_tags=["trial"]
    )
    assert not stream.arrow_types_declared  # no Arrow types without rows
    (batch,) = batches_of(stream)
    renamed = batch.rename_columns(
        ["other" if name == "note" else name for name in batch.schema.names]
    )

    async def write() -> int:
        output = OutputChannel(stream)
        with pytest.raises(ValueError, match="the stream's keys are"):
            await output.send(renamed)
        await output.send(batch)
        await output.close()
        return output.result().as_table().num_rows

    assert run(write) == 7


def test_a_refused_first_batch_sets_no_schema() -> None:
    """Without Arrow types in the description, a refused first batch sets nothing: a
    correct batch sent next is taken."""
    stream = _UntypedStream(
        source().as_table(columns=ALL),
        tag_columns=["subject", "trial"],
        data_tags=["trial"],
    )
    (batch,) = batches_of(stream)
    note = batch.schema.get_field_index("note")
    narrow = pa.RecordBatch.from_arrays(
        [
            *batch.columns[:note],
            batch.column(note).cast(pa.string()),
            *batch.columns[note + 1 :],
        ],
        schema=batch.schema.set(
            note, pa.field("note", pa.string(), metadata={"unit": "mV"})
        ),
    )

    async def write() -> int:
        output = OutputChannel(stream)
        with pytest.raises(ValueError, match="column 'note' is string"):
            await output.send(narrow)
        await output.send(batch)
        await output.close()
        return output.result().as_table().num_rows

    assert run(write) == 7


def test_overlapping_sends_reach_every_reader_in_one_order() -> None:
    """Two sends started while a reader is full: they are taken one at a time, so both
    readers get the batches in one order. The first send queues its batch for the fast
    reader and waits for room in the slow one; the second waits for the first; the
    readers draining releases both."""
    stream = source(3)
    first, second, third = batches_of(stream, 1)

    async def write() -> tuple[list[Any], list[Any]]:
        output = OutputChannel(stream, retain=False, buffer_size=1)
        fast, slow = output.open_reader(), output.open_reader()
        await output.send(first)
        _ = await fast.receive()
        sending = [
            asyncio.ensure_future(output.send(second)),
            asyncio.ensure_future(output.send(third)),
        ]
        await asyncio.sleep(0)

        async def trials(reader: InputChannel, start: list[Any]) -> list[Any]:
            got: list[Any] = list(start)
            async for batch in reader:
                got.extend(batch.column("trial").to_pylist())
            return got

        async def close_after_sends() -> None:
            """Close the output once both sends are done (they wait for the slow
            reader's room, which its own consumer task frees)."""
            _ = await asyncio.gather(*sending)
            await output.close()

        # Each reader has one consumer: this task keeps reading the fast one.
        slow_order = asyncio.ensure_future(trials(slow, []))
        closing = asyncio.ensure_future(close_after_sends())
        fast_order = await trials(fast, [0])
        await closing
        return fast_order, await slow_order

    fast_order, slow_order = run(write)
    assert fast_order == slow_order == [0, 1, 2]


def test_a_cancelled_send_that_reached_one_reader_fails_both() -> None:
    """A send cancelled after it queued its batch for the fast reader, while it waits
    for the slow one, fails the channel: neither reader completes normally, each next
    read raising ``UpstreamFailedError`` caused by the cancellation."""
    stream = source(2)
    first, second = batches_of(stream, 1)

    async def write() -> list[BaseException | None]:
        output = OutputChannel(stream, retain=False, buffer_size=1)
        fast, slow = output.open_reader(), output.open_reader()
        await output.send(first)
        _ = await fast.receive()
        sending = asyncio.ensure_future(output.send(second))
        await asyncio.sleep(0)  # queued for fast, waiting for room in slow
        _ = sending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sending
        await output.close()
        causes: list[BaseException | None] = []
        for reader in (fast, slow):
            with pytest.raises(UpstreamFailedError) as failed:
                _ = await reader.collect()
            causes.append(failed.value.__cause__)
        return causes

    for cause in run(write):
        assert isinstance(cause, asyncio.CancelledError)


def test_a_send_cancelled_before_it_reached_a_reader_leaves_the_channel_usable() -> (
    None
):
    """A send cancelled while it waits for the only reader's room delivered nothing: the
    channel goes on and completes normally."""
    stream = source(3)
    first, second, third = batches_of(stream, 1)

    async def write() -> list[Any]:
        output = OutputChannel(stream, retain=False, buffer_size=1)
        reader = output.open_reader()
        await output.send(first)  # fills the one slot
        sending = asyncio.ensure_future(output.send(second))
        await asyncio.sleep(0)
        _ = sending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sending
        got = (await reader.receive()).column("trial").to_pylist()
        await output.send(third)
        await output.close()
        for batch in await reader.collect():
            got.extend(batch.column("trial").to_pylist())
        return got

    assert run(write) == [0, 2]


# --- Retained output: readers at their own position, send never waits -------------------


def test_a_retained_output_never_makes_send_wait() -> None:
    """With ``retain=True`` (the default) the producer runs ahead of every reader."""
    stream = source(40)
    batches = batches_of(stream, 1)
    assert len(batches) > 3 * BATCH_BUFFER_SIZE

    async def write() -> tuple[int, int]:
        output = OutputChannel(stream)
        slow = output.open_reader()
        for batch in batches:  # nobody reads: no send waits
            await output.send(batch)
        await output.close()
        return len(await slow.collect()), output.result().as_table().num_rows

    assert run(write) == (40, 40)


def test_a_late_reader_of_a_retained_output_replays_and_continues() -> None:
    """``open_reader()`` after the first send, and after close: every batch, in order."""
    stream = source()
    batches = batches_of(stream, 2)

    async def write() -> list[list[Any]]:
        output = OutputChannel(stream)
        early = output.open_reader()
        await output.send(batches[0])
        late = output.open_reader()
        first = (await late.receive()).column("trial").to_pylist()
        for batch in batches[1:]:
            await output.send(batch)
        await output.close()
        after = output.open_reader()
        trials = [
            [t for b in await reader.collect() for t in b.column("trial").to_pylist()]
            for reader in (early, late, after)
        ]
        return [first, *trials]

    first, early, late, after = run(write)
    assert first == [0, 1]
    assert early == after == list(range(7))
    assert late == list(range(2, 7))


# --- Output not retained: back-pressure, and the slowest reader sets the pace ------------


def test_without_retain_send_waits_for_room_in_every_reader() -> None:
    """Each reader queues ``buffer_size`` record batches; then the producer waits, and
    the slowest reader sets the pace, whatever the others have read."""
    stream = source(12)
    batches = batches_of(stream, 1)

    async def write() -> tuple[int, bool, int, int]:
        output = OutputChannel(stream, retain=False, buffer_size=2)
        fast, slow = output.open_reader(), output.open_reader()
        sent: list[int] = []

        async def produce() -> None:
            for i, batch in enumerate(batches):
                await output.send(batch)
                sent.append(i)
            await output.close()

        producer = asyncio.ensure_future(produce())
        # Two batches fit each queue. The fast reader (one consumer, its own task)
        # makes room for a third, which then waits for the slow reader: that send
        # does not return, and the fast reader waits for its fourth batch.
        three_read = asyncio.Event()

        async def fast_consumer() -> int:
            """The fast reader's one consumer: read three batches, signal, then wait
            for the rest, which come only as the slow reader makes room."""
            got = [await fast.receive() for _ in range(3)]
            three_read.set()
            return len(got) + len(await fast.collect())

        fast_reading = asyncio.ensure_future(fast_consumer())
        await three_read.wait()
        await asyncio.sleep(0.01)
        sent_while_slow_is_full = len(sent)
        fast_waits_for_the_slow_reader = not fast_reading.done()
        slow_batches = await slow.collect()
        fast_total = await fast_reading
        await producer
        return (
            sent_while_slow_is_full,
            fast_waits_for_the_slow_reader,
            fast_total,
            len(slow_batches),
        )

    assert run(write) == (2, True, 12, 12)


def test_a_reader_that_closes_early_no_longer_holds_the_producer() -> None:
    """``close()`` on an input channel unregisters it: the producer stops waiting."""
    stream = source(12)
    batches = batches_of(stream, 1)

    async def write() -> tuple[int, int]:
        output = OutputChannel(stream, retain=False, buffer_size=1)
        leaving, staying = output.open_reader(), output.open_reader()

        async def produce() -> None:
            for batch in batches:
                await output.send(batch)
            await output.close()

        producer = asyncio.ensure_future(produce())
        _ = await leaving.receive()
        await leaving.close()
        with pytest.raises(ChannelClosed):
            _ = await leaving.receive()
        rows = len(await staying.collect())
        await producer
        return rows, len(batches)

    assert run(write) == (12, 12)


def test_without_retain_a_late_reader_is_refused_and_there_is_no_result() -> None:
    """Without ``retain``, a reader opened after the first send would miss it, so it is
    refused, and the output has no result stream."""
    stream = source()
    (batch,) = batches_of(stream)

    async def write() -> None:
        output = OutputChannel(stream, retain=False)
        reader = output.open_reader()
        await output.send(batch)
        with pytest.raises(ValueError, match="before the first record batch"):
            _ = output.open_reader()
        await output.close()
        assert len(await reader.collect()) == 1
        with pytest.raises(RuntimeError, match="not retained"):
            _ = output.result()

    run(write)


def test_a_buffer_must_hold_one_batch() -> None:
    """``buffer_size`` below 1 is refused when the output channel is made."""
    with pytest.raises(ValueError, match="buffer_size"):
        _ = OutputChannel(source(), retain=False, buffer_size=0)


# --- Producer failure and cancellation ---------------------------------------------------


@pytest.mark.parametrize("retain", [True, False], ids=["retained", "not_retained"])
def test_a_failed_producer_is_never_a_normal_end(retain: bool) -> None:
    """After ``fail(error)`` every reader's next read raises ``UpstreamFailedError``
    naming the stream, with the producer's error as its cause: also a reader with
    batches still unread, and one blocked waiting for a batch."""
    stream = source()
    batches = batches_of(stream, 2)
    boom = OSError("disk gone")

    async def write() -> list[BaseException]:
        output = OutputChannel(stream, retain=retain)
        unread, waiting, barrier = (output.open_reader() for _ in range(3))
        await output.send(batches[0])
        first_read = asyncio.Event()

        async def waiting_consumer() -> Any:
            """The reader's one consumer: read a batch, signal, then wait for the next,
            which the producer's failure ends."""
            _ = await waiting.receive()
            first_read.set()
            return await waiting.receive()

        blocked = asyncio.ensure_future(waiting_consumer())
        await first_read.wait()
        whole = asyncio.ensure_future(barrier.to_stream())
        await asyncio.sleep(0)
        output.fail(boom)
        output.fail(RuntimeError("a second failure changes nothing"))
        await output.close()  # nor does a close
        with pytest.raises(UpstreamFailedError) as refused:
            await output.send(batches[1])
        assert refused.value.__cause__ is boom
        with pytest.raises(RuntimeError, match="has no result"):
            _ = output.result()
        errors: list[BaseException] = []
        for read in (unread.receive(), blocked, whole, unread.collect()):
            with pytest.raises(UpstreamFailedError) as caught:
                _ = await read
            errors.append(caught.value)
        return errors

    for error in run(write):
        assert error.__cause__ is boom
        assert repr(stream.label) in str(error)
        assert "OSError: disk gone" in str(error)


def test_fail_after_close_changes_nothing() -> None:
    """``fail()`` on a closed channel does nothing: its readers end normally and the
    result is the batches sent."""
    stream = source()
    batches = batches_of(stream, 3)

    async def write() -> int:
        output = OutputChannel(stream)
        for batch in batches:
            await output.send(batch)
        await output.close()
        output.fail(OSError("too late"))
        return (await output.open_reader().to_stream()).as_table().num_rows

    assert run(write) == 7


# --- Operators over channels: failure, cancellation, reading every input to its end ------


def execute(
    op: StaticOutputOperatorPod,
    streams: Sequence[Any],
    inputs: Callable[[], Awaitable[list[RecordBatchInput]]],
) -> OutputChannel:
    """Run ``op`` over input channels; return its output channel."""

    async def go() -> OutputChannel:
        output = OutputChannel(op(*streams))
        try:
            await op.async_execute_batches(await inputs(), output)
        except UpstreamFailedError:
            pass
        return output

    return run(go)


@pytest.mark.parametrize(
    "make_op",
    [lambda: MapData({"amplitude": "volts"}), lambda: PolarsFilter(constraints={})],
    ids=["row_by_row", "barrier"],
)
def test_an_operator_fails_its_output_when_its_input_failed(
    make_op: Callable[[], StaticOutputOperatorPod],
) -> None:
    """A producer that fails after some batches: the operator's own read raises, it
    fails its output with that error, and a barrier operator never computes over the
    part it received."""
    stream = source()
    batches = batches_of(stream, 2)
    op = make_op()

    async def inputs() -> list[RecordBatchInput]:
        feed = OutputChannel(stream)
        await feed.send(batches[0])
        feed.fail(OSError("disk gone"))
        return [feed.open_reader()]

    with mock.patch.object(
        type(op), "static_process", autospec=True, side_effect=type(op).static_process
    ) as computed:
        output = execute(op, [stream], inputs)
    assert computed.call_count == 0

    async def downstream() -> None:
        with pytest.raises(UpstreamFailedError) as caught:
            _ = await output.open_reader().receive()
        assert isinstance(caught.value.__cause__, UpstreamFailedError)
        assert isinstance(caught.value.__cause__.__cause__, OSError)

    run(downstream)
    with pytest.raises(RuntimeError, match="has no result"):
        _ = output.result()


def test_a_barrier_that_read_part_of_a_failed_input_never_computes() -> None:
    """CodeRabbit on PR 510: the barrier has started and taken the first batch when
    its producer fails; it never computes over the part it received, and its output
    fails."""
    stream = source()
    batches = batches_of(stream, 2)
    op = PolarsFilter(constraints={})

    async def go() -> None:
        feed = OutputChannel(stream, retain=False, buffer_size=1)
        reader = feed.open_reader()
        output = OutputChannel(op(stream))
        task = asyncio.ensure_future(op.async_execute_batches([reader], output))
        await feed.send(batches[0])
        # With one slot, this send returns once the barrier has taken the first
        # batch: the barrier is running and holds part of its input.
        await feed.send(batches[1])
        feed.fail(OSError("disk gone"))
        with pytest.raises(UpstreamFailedError):
            async with asyncio.timeout(TIMEOUT):
                await task
        with pytest.raises(RuntimeError, match="has no result"):
            _ = output.result()

    with mock.patch.object(
        type(op), "static_process", autospec=True, side_effect=type(op).static_process
    ) as computed:
        run(go)
    assert computed.call_count == 0


def test_a_cancelled_operator_fails_its_output_and_closes_its_input() -> None:
    """Cancellation is a failure for the readers downstream, and the operator's input
    channel is closed, so its producer no longer waits for it."""
    stream = source(12)
    batches = batches_of(stream, 1)
    op = MapData({"amplitude": "volts"})

    async def go() -> tuple[BaseException, int]:
        feed = OutputChannel(stream, retain=False, buffer_size=1)
        reader = feed.open_reader()
        output = OutputChannel(op(stream))
        downstream = output.open_reader()
        task = asyncio.ensure_future(op.async_execute_batches([reader], output))
        await feed.send(batches[0])
        _ = await downstream.receive()
        _ = task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # The operator closed its input: the producer is never held again.
        for batch in batches[1:]:
            await feed.send(batch)
        with pytest.raises(UpstreamFailedError) as caught:
            _ = await downstream.receive()
        cause = caught.value.__cause__
        assert cause is not None
        return cause, len(batches)

    cause, sent = run(go)
    assert isinstance(cause, asyncio.CancelledError) and sent == 12


def test_an_operator_reads_its_input_to_the_end_and_closes_its_output() -> None:
    """The normal end: every input batch is read, the output is closed, and its result
    is the operator's sync result, built from rows."""
    stream = source()
    op = SelectDataColumns(["subject", "trial", "note"])

    async def inputs() -> list[RecordBatchInput]:
        return [stream.open_channel()]

    output = execute(op, [stream], inputs)
    assert same(output.result().as_table(columns=ALL), R(op(stream)))


def test_stream_from_batches_is_the_one_conversion() -> None:
    """``result()`` and ``to_stream()`` are ``stream_from_batches``: its table equals the
    rows' table for every way the batches are cut, and no batch gives zero rows."""
    stream = source()
    for rows in (1, 2, 5, 7):
        made = stream_from_batches(stream, batches_of(stream, rows))
        assert same(made.as_table(columns=ALL), R(stream))
    assert stream_from_batches(stream, []).as_table().num_rows == 0


def test_a_batch_with_a_duplicate_column_is_refused() -> None:
    """Without Arrow types the names are compared as a multiset: a duplicate is refused
    (Codex on PR #485, round 5)."""
    table = source().as_table(columns=ALL)
    stream = _UntypedStream(
        table, tag_columns=["subject", "trial"], data_tags=["trial"]
    )
    (batch,) = batches_of(stream)
    name = next(n for n in batch.schema.names if not n.startswith(("_", "subject")))
    doubled = batch.append_column(batch.schema.field(name), batch.column(name))

    async def write() -> None:
        output = OutputChannel(stream)
        await output.send(doubled)

    with pytest.raises(ValueError, match="a column more than once"):
        run(write)


def test_a_batch_whose_tokens_cannot_settle_with_accepted_ones_is_refused() -> None:
    """A second batch with list tokens where the first had string tokens is refused at
    send: two batches of one stream declare one token type (Codex on PR #485, round 5)."""
    from orcapod.core.operators.static_output_pod import SourceInfoTypesDisagree

    stream = source()
    (batch,) = batches_of(stream)
    column = next(n for n in batch.schema.names if n.startswith("_source_"))
    position = batch.schema.get_field_index(column)
    tokens = pa.large_list(pa.large_string())
    listed = pa.RecordBatch.from_arrays(
        [
            *batch.columns[:position],
            pa.array([["t"]] * batch.num_rows, tokens),
            *batch.columns[position + 1 :],
        ],
        schema=batch.schema.set(position, pa.field(column, tokens, nullable=True)),
    )

    async def write() -> None:
        output = OutputChannel(stream)
        await output.send(batch)
        await output.send(listed)

    with pytest.raises(SourceInfoTypesDisagree, match=column):
        run(write)


# --- Batches that settle together (Codex on PR #485, final check, findings 3-5) ------------

TOKEN = pa.large_string()
TOKENS = pa.large_list(TOKEN)
TOKEN_LISTS = pa.large_list(TOKENS)


def _with_amplitude_tokens(
    batch: pa.RecordBatch, start: int, values: list[Any], kind: pa.DataType
) -> pa.RecordBatch:
    """Rows ``start ...`` of ``batch`` with ``_source_amplitude`` carried as ``kind``
    holding ``values`` (tokens of one row each)."""
    part = batch.slice(start, len(values))
    index = part.schema.get_field_index("_source_amplitude")
    return pa.RecordBatch.from_arrays(
        [
            *part.columns[:index],
            pa.array(values, type=kind),
            *part.columns[index + 1 :],
        ],
        schema=part.schema.set(index, pa.field("_source_amplitude", kind)),
    )


NON_NULL_TOKENS = pa.large_list(pa.field("item", TOKEN, nullable=False))
FIXED_TOKENS = pa.list_(TOKEN, 1)

# Batches that declare one token type are assembled with that type, whatever their
# tokens show (ITL-1491: the pipeline determines a token column's type).
ONE_TYPE_CASES: dict[
    str, tuple[list[tuple[int, list[Any], pa.DataType]], pa.DataType, list[Any]]
] = {
    # An empty token list (which shows one list level), then a token list.
    "empty_list_two_levels": (
        [(0, [[]], TOKEN_LISTS), (1, [[["b"]]], TOKEN_LISTS)],
        TOKEN_LISTS,
        [[], [["b"]]],
    ),
    # A null token inside a list, then a list token.
    "null_element_then_list": (
        [(0, [[None]], TOKENS), (1, [["b"]], TOKENS)],
        TOKENS,
        [[None], ["b"]],
    ),
    # Lists whose item field is declared non-nullable keep it.
    "non_null_items": (
        [(0, [["a"]], NON_NULL_TOKENS), (1, [["b"]], NON_NULL_TOKENS)],
        NON_NULL_TOKENS,
        [["a"], ["b"]],
    ),
    # A hand-built fixed-size list stays one.
    "fixed_size_list": (
        [(0, [["a"]], FIXED_TOKENS), (1, [["b"]], FIXED_TOKENS)],
        FIXED_TOKENS,
        [["a"], ["b"]],
    ),
    # A token no value could type ([[], [[]]] three levels deep) is taken as declared.
    "three_levels": (
        [
            (0, [[[], [[]]]], pa.large_list(TOKEN_LISTS)),
            (1, [[[["b"]]]], pa.large_list(TOKEN_LISTS)),
        ],
        pa.large_list(TOKEN_LISTS),
        [[[], [[]]], [[["b"]]]],
    ),
    # Without a declaration, an empty batch does not settle the token type.
    "zero_rows_first": (
        [(0, [], TOKEN), (0, [["a"]], TOKENS), (1, [["b"]], TOKENS)],
        TOKENS,
        [["a"], ["b"]],
    ),
}


@pytest.mark.parametrize("case", list(ONE_TYPE_CASES))
def test_batches_send_accepts_are_assembled(case: str) -> None:
    """What ``send`` accepts can be assembled: ``result()`` and ``to_stream()`` build
    the complete table, each source-info column of the type its batches declare."""
    parts, declared, tokens = ONE_TYPE_CASES[case]
    stream = shaped_source(declared)
    if case == "zero_rows_first":
        stream = _UntypedStream(
            stream.as_table(columns=ALL),
            tag_columns=["subject", "trial"],
            data_tags=["trial"],
        )
    (batch,) = batches_of(stream)

    async def write() -> tuple[Any, Any]:
        output = OutputChannel(stream)
        reader = output.open_reader()
        for start, values, kind in parts:
            await output.send(_with_amplitude_tokens(batch, start, values, kind))
        await output.close()
        return output.result(), await reader.to_stream()

    result, converted = run(write)
    for assembled in (result, converted):
        table = assembled.as_table(columns=ALL)
        assert table.schema.field("_source_amplitude").type == declared
        assert table.column("_source_amplitude").to_pylist() == tokens


# Batches that declare two token types: before ITL-1491 the first three "settled" to
# one type worked out from the tokens; a token column's type is now the declared one.
TWO_TYPE_CASES: dict[str, list[tuple[int, list[Any], pa.DataType]]] = {
    "list_depth": [(0, [[]], TOKEN_LISTS), (1, [["b"]], TOKENS)],
    "list_then_string": [(0, [["a"]], TOKENS), (1, ["b"], TOKEN)],
    "item_nullability": [(0, [["a"]], NON_NULL_TOKENS), (1, [["b"]], TOKENS)],
    "fixed_then_large": [(0, [["a"]], FIXED_TOKENS), (1, [["b"]], TOKENS)],
}


@pytest.mark.parametrize("order", ["as_listed", "reversed"])
@pytest.mark.parametrize("case", list(TWO_TYPE_CASES))
def test_a_batch_that_declares_another_token_type_is_refused_at_send(
    case: str, order: str
) -> None:
    """A batch whose source-info column declares another type than the batches
    accepted so far is refused at send, whatever its tokens are; the channel takes a
    batch of the accepted type after it, and assembles what it accepted."""
    from orcapod.core.operators.static_output_pod import SourceInfoTypesDisagree

    (_, first_values, first), (_, other_values, other) = (
        TWO_TYPE_CASES[case]
        if order == "as_listed"
        else list(reversed(TWO_TYPE_CASES[case]))
    )
    stream = _UntypedStream(
        shaped_source(first).as_table(columns=ALL),
        tag_columns=["subject", "trial"],
        data_tags=["trial"],
    )
    (batch,) = batches_of(stream)

    async def write() -> Any:
        output = OutputChannel(stream)
        await output.send(_with_amplitude_tokens(batch, 0, first_values, first))
        with pytest.raises(SourceInfoTypesDisagree, match="_source_amplitude"):
            await output.send(_with_amplitude_tokens(batch, 1, other_values, other))
        await output.send(_with_amplitude_tokens(batch, 1, first_values, first))
        await output.close()
        return output.result().as_table(columns=ALL)

    table = run(write)
    assert table.schema.field("_source_amplitude").type == first
    assert table.column("_source_amplitude").to_pylist() == [*first_values] * 2


def test_a_duplicate_system_tag_column_is_refused_on_a_declared_stream() -> None:
    """A batch with a system-tag column twice is refused, whatever the description."""
    stream = source(2)
    (batch,) = batches_of(stream)
    name = next(n for n in batch.schema.names if n.startswith("_tag"))
    doubled = batch.append_column(batch.schema.field(name), batch.column(name))

    async def write() -> None:
        await OutputChannel(stream).send(doubled)

    with pytest.raises(ValueError, match="a column more than once"):
        run(write)


def test_batch_puts_arrivals_whose_tokens_show_two_depths_together() -> None:
    """``Batch(2)`` over arrivals whose source-info tokens show two depths (an empty
    list, then a list of token lists), both declared two levels deep: it groups them
    under one more list level of the declared type (ITL-1491)."""
    stream = shaped_source(TOKEN_LISTS)
    (batch,) = batches_of(stream)
    op = Batch(batch_size=2)

    async def go() -> Any:
        feed = OutputChannel(stream)
        reader = feed.open_reader()
        await feed.send(_with_amplitude_tokens(batch, 0, [[]], TOKEN_LISTS))
        await feed.send(_with_amplitude_tokens(batch, 1, [[["b"]]], TOKEN_LISTS))
        await feed.close()
        output = OutputChannel(op(stream))
        await op.async_execute_batches([reader], output)
        return output.result()

    table = run(go).as_table(columns=ALL)
    assert table.num_rows == 1
    assert table.schema.field("_source_amplitude").type == pa.large_list(TOKEN_LISTS)
    assert table.column("_source_amplitude").to_pylist() == [[[], [["b"]]]]


def test_a_source_nodes_channel_gives_its_output_stream_as_it_is() -> None:
    """A source node's channel reads its ``execute()`` output stream, a complete output
    in the row-built form, which ``to_stream()`` gives as it is."""
    node = SourceJobNode.from_stream(source(2), name="events")

    async def read() -> Any:
        return await node.open_channel().to_stream()

    output = run(read)
    assert getattr(output, ROW_BUILT_OUTPUT, False)
    assert same(output.as_table(columns=ALL), R(node))


def test_channel_keeps_first_system_tag_declaration_and_entry_id() -> None:
    """A later batch containing a null cannot change an earlier row's entry ID."""
    from orcapod.core import row_ids

    name = "_tag_custom"
    field = pa.field(name, pa.large_string(), metadata={b"owner": b"test"})
    table = pa.Table.from_arrays(
        [pa.array([1, 2]), pa.array([7, 8]), pa.array(["kept", None], field.type)],
        schema=pa.schema([pa.field("k", pa.int64()), pa.field("x", pa.int64()), field]),
    )
    stream = _UntypedStream(table, tag_columns=["k"])
    first, second = batches_of(ArrowTableStream(table, tag_columns=["k"]), rows=1)

    async def read(pieces: Sequence[pa.RecordBatch]):
        channel = OutputChannel(stream)
        for batch in pieces:
            await channel.send(batch)
        await channel.close()
        outputs = [channel.result(), await channel.open_reader().to_stream()]
        ids = []
        for result in outputs:
            assert isinstance(result, ArrowTableStream)
            tag, data = next(result.iter_data())
            declared = result.as_table(columns=ALL).schema.field(name)
            assert declared.equals(field, check_metadata=True)
            ids.append(row_ids.entry_id(result.data_context.arrow_hasher, tag, data))
        return ids

    assert run(lambda: read([first])) == run(lambda: read([first, second]))


@pytest.mark.parametrize("num_rows", [0, 1])
@pytest.mark.parametrize("wrong_type", [TOKEN, TOKENS])
def test_concrete_channel_refuses_first_batch_with_a_different_token_type(
    num_rows: int, wrong_type: pa.DataType
) -> None:
    """A concrete stream's declared token type also applies to its first batch."""
    from orcapod.core.operators.static_output_pod import SourceInfoTypesDisagree

    declared = pa.large_list(pa.field("leaf", TOKEN, metadata={b"kind": b"token"}))
    stream = ArrowTableStream(
        pa.table(
            {
                "amplitude": pa.array([[1]], pa.large_list(pa.int64())),
                "_source_amplitude": pa.array([["old"]], declared),
            }
        ),
        tag_columns=[],
    )
    (batch,) = batches_of(stream)
    values = ["wrong"] if wrong_type == TOKEN else [["wrong"]]
    wrong = _with_amplitude_tokens(batch, 0, values[:num_rows], wrong_type)
    with pytest.raises(SourceInfoTypesDisagree, match="_source_amplitude"):
        _ = stream_from_batches(stream, [wrong])

    async def write() -> None:
        output = OutputChannel(stream)
        reader = output.open_reader()
        with pytest.raises(SourceInfoTypesDisagree, match="_source_amplitude"):
            await output.send(wrong)
        await output.send(batch)
        await output.close()
        delivered = await reader.receive()
        assert delivered is not None
        for schema in (
            delivered.schema,
            output.result().arrow_schema(),
        ):
            assert schema.field("_source_amplitude").equals(
                stream.arrow_schema().field("_source_amplitude"), check_metadata=True
            )

    run(write)


def test_unbound_slot_settles_only_its_provisional_token_type() -> None:
    """An unbound slot's scalar token description can settle to a list declaration."""
    from orcapod.core.operators.static_output_pod import SourceInfoTypesDisagree

    declared = pa.large_list(pa.field("leaf", TOKEN, metadata={b"kind": b"token"}))
    tags, data = shaped_source(declared).output_schema()
    slot = SourceJobNode(name="events", tag_schema=tags, data_schema=data)
    assert slot.bound_source is None and not slot.arrow_types_declared
    provisional = slot.arrow_schema()
    assert provisional.field("_source_amplitude").type == TOKEN
    (batch,) = batches_of(shaped_source(declared))
    system_columns = iter(
        batch.column(index)
        for index, field in enumerate(batch.schema)
        if field.name.startswith("_tag_")
    )
    index = provisional.get_field_index("_source_amplitude")
    declared_field = batch.schema.field("_source_amplitude")
    schema = provisional.set(index, declared_field)
    settled = pa.RecordBatch.from_arrays(
        [
            next(system_columns)
            if field.name.startswith("_tag_")
            else batch.column(batch.schema.get_field_index(field.name))
            for field in schema
        ],
        schema=schema,
    )

    async def write() -> None:
        output = OutputChannel(slot)
        reader = output.open_reader()
        await output.send(settled)
        with pytest.raises(SourceInfoTypesDisagree, match="_source_amplitude"):
            await output.send(_with_amplitude_tokens(settled, 0, ["wrong"], TOKEN))
        await output.close()
        for result in (output.result(), await reader.to_stream()):
            assert (
                result.arrow_schema()
                .field("_source_amplitude")
                .equals(declared_field, check_metadata=True)
            )

    run(write)
