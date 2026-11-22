"""Input and output channels of Arrow record batches (ITL-1347, stage 5: L8).

A stream is primary and owns the description; a channel is a separate, single-pass
carrier of the stream's rows as Arrow record batches. An input channel is opened from a
complete stream (``channel_from_stream``) or from an output channel
(``OutputChannel.open_reader``); an output channel, once closed, gives its rows back as
an ordinary stream (``result()``), and so does an input channel that is read to its end
(``to_stream()``). One test per rule of the design's table "The hard semantics".

The oracle of every table is ``R``: the table today's code builds from a stream's rows
(``StaticOutputOperatorPod._rows_to_table``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar
from unittest import mock

import pyarrow as pa
import pytest

from orcapod.channels import (
    ChannelClosed,
    ReadableChannel,
    RecordBatchInput,
    RecordBatchOutput,
    WritableChannel,
)
from orcapod.core.operators import MapData, PolarsFilter, SelectDataColumns
from orcapod.core.operators.static_output_pod import (
    StaticOutputOperatorPod,
    stream_from_batches,
)
from orcapod.core.record_batch_channels import (
    BATCH_BUFFER_SIZE,
    SOURCE_BATCH_ROWS,
    InputChannel,
    OutputChannel,
    channel_from_stream,
)
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import UpstreamFailedError
from orcapod.sources import ArrowTableSource

T = TypeVar("T")
ALL = {"system_tags": True, "source": True}
TIMEOUT = 5.0


def run(make: Callable[[], Awaitable[T]]) -> T:
    """Run a coroutine to its end; a channel that never wakes fails the test."""

    async def bounded() -> T:
        return await asyncio.wait_for(make(), TIMEOUT)

    return asyncio.run(bounded())


def source(n: int = 7, source_id: str = "events") -> ArrowTableSource:
    """``n`` rows: tags ``subject`` and ``trial`` (a data-tag too), three data columns."""
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
        source_id=source_id,
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
        return await channel_from_stream(stream).collect()

    return pa.Table.from_batches(run(read)).to_batches(max_chunksize=rows)


# --- Opening an input channel on a stream ----------------------------------------------


def test_a_channel_opened_on_a_stream_delivers_its_rows_and_refers_to_it() -> None:
    """The record batches are the stream's rows; the description is the stream."""
    stream = source()
    channel = channel_from_stream(stream)

    assert isinstance(channel, InputChannel)
    assert isinstance(channel, ReadableChannel)
    as_protocol: RecordBatchInput = channel  # what an operator reads (type-checked)
    assert as_protocol.stream is stream
    delivered = run(channel.to_stream)
    assert same(delivered.as_table(columns=ALL), R(stream))
    assert delivered.keys() == stream.keys()
    assert delivered.output_schema() == stream.output_schema()
    assert delivered.data_context_key == stream.data_context_key
    assert delivered.pipeline_hash() == stream.pipeline_hash()


def test_opening_a_channel_computes_nothing() -> None:
    """The stream's table is read at the first ``receive``, not when the channel opens."""
    stream = source()
    with mock.patch.object(
        ArrowTableSource,
        "as_table",
        autospec=True,
        side_effect=ArrowTableSource.as_table,
    ) as read:
        channel = channel_from_stream(stream)
        assert read.call_count == 0
        _ = run(channel.receive)
        assert read.call_count == 1


def test_two_channels_on_one_stream_have_their_own_positions() -> None:
    """Two channels opened on one stream: each reader gets every row, and the stream stays."""
    stream = source()

    async def read() -> tuple[int, int, int]:
        first, second = channel_from_stream(stream), channel_from_stream(stream)
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
    sizes = [batch.num_rows for batch in run(channel_from_stream(stream).collect)]
    assert sizes == [SOURCE_BATCH_ROWS, 10]


def test_a_channel_is_read_once_and_the_stream_again() -> None:
    """A channel is single-pass; to read again, go back to a stream."""

    async def read() -> tuple[int, int]:
        channel = channel_from_stream(source())
        first = await channel.collect()
        with pytest.raises(ChannelClosed):
            _ = await channel.receive()
        return len(first), len(await channel.collect())

    assert run(read) == (1, 0)


def test_to_stream_is_everything_or_nothing() -> None:
    """``to_stream()`` on a channel already read is refused: never part of an input."""

    batches = batches_of(source(), 2)

    async def read() -> None:
        output = OutputChannel(source())
        for batch in batches:
            await output.send(batch)
        await output.close()
        channel = output.open_reader()
        _ = await channel.receive()
        with pytest.raises(RuntimeError, match="1 record batch"):
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
        # Two batches fit each queue. The fast reader makes room for a third, which
        # then waits for the slow reader: that send does not return.
        got_fast = [await fast.receive() for _ in range(3)]
        await asyncio.sleep(0.01)
        sent_while_slow_is_full = len(sent)
        starved = asyncio.ensure_future(fast.receive())
        await asyncio.sleep(0.01)
        fast_waits_for_the_slow_reader = not starved.done()

        async def rest_of_fast() -> int:
            return 1 + len(await fast.collect()) if await starved else 0

        rest, slow_batches = await asyncio.gather(rest_of_fast(), slow.collect())
        await producer
        return (
            sent_while_slow_is_full,
            fast_waits_for_the_slow_reader,
            len(got_fast) + rest,
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
        _ = await waiting.receive()
        blocked = asyncio.ensure_future(waiting.receive())
        whole = asyncio.ensure_future(barrier.to_stream())
        await asyncio.sleep(0)
        output.fail(boom)
        output.fail(RuntimeError("a second failure changes nothing"))
        await output.close()  # nor does a close
        with pytest.raises(ChannelClosed):
            await output.send(batches[1])
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
    inputs: Callable[[], Awaitable[list[InputChannel]]],
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

    async def inputs() -> list[InputChannel]:
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

    async def inputs() -> list[InputChannel]:
        return [channel_from_stream(stream)]

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
