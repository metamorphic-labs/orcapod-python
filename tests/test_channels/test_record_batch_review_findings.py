"""Regression tests for review findings on the record-batch channels (ITL-1362).

Each test captures a finding a reviewer reported on the channel PRs (#483, #488, #495)
whose fix had no test that fails without it, or a test that covered only the reported
case of a wider class. The finding is named in each test's docstring.
"""

from __future__ import annotations

import asyncio
import gc
import weakref
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.operators import SemiJoin
from orcapod.core.operators.static_output_pod import close_inputs
from orcapod.core.record_batch_channels import OutputChannel
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import UpstreamFailedError
from tests.test_channels.test_record_batch_channels import ALL, R, batches_of, source

TIMEOUT = 5.0


def _trial(batch: pa.RecordBatch) -> int:
    return batch.column("trial")[0].as_py()


# --- Overlapping sends on a bounded output (CodeRabbit, PR 483) ----------------------


def test_a_send_started_while_another_waits_reaches_every_reader_after_it() -> None:
    """CodeRabbit on PR 483 (thread 4657988055): overlapping sends on an output that
    does not retain must reach every reader in one order.

    The reviewer's schedule, one task reading both readers of a one-slot output: the
    fast reader takes A; the send of X queues X for the fast reader and waits for the
    slow reader's room; the send of Y is started; the slow reader takes A and the fast
    reader takes X, waking the send of X after Y's has been scheduled. Without one
    send at a time, Y reaches the slow reader before X does."""
    stream = source(3)
    a, x, y = batches_of(stream, 1)

    async def schedule() -> tuple[list[int], list[int]]:
        output = OutputChannel(stream, retain=False, buffer_size=1)
        fast, slow = output.open_reader(), output.open_reader()
        await output.send(a)
        got_fast = [_trial(await fast.receive())]
        sending_x = asyncio.ensure_future(output.send(x))
        await asyncio.sleep(0)  # X queued for the fast reader, waiting for the slow one
        # Y's send is scheduled now, so it runs before X's send wakes up.
        sending_y = asyncio.ensure_future(output.send(y))
        got_slow = [_trial(await slow.receive())]  # A, without suspending
        got_fast.append(_trial(await fast.receive()))  # X, without suspending
        async with asyncio.timeout(TIMEOUT):
            got_slow.append(_trial(await slow.receive()))
            got_slow.append(_trial(await slow.receive()))
            got_fast.append(_trial(await fast.receive()))
            await sending_x
            await sending_y
        return got_fast, got_slow

    got_fast, got_slow = asyncio.run(schedule())
    assert got_fast == got_slow == [0, 1, 2]


# --- Closing every input (CodeRabbit, PR 483) -----------------------------------------


class _FailingClose:
    """An input channel whose close raises; it delivers nothing."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True
        raise OSError("cannot close")


@pytest.mark.parametrize("failing", [False, True], ids=["succeeding", "failing"])
def test_every_input_is_closed_when_one_close_fails_with_another_still_open(
    failing: bool,
) -> None:
    """CodeRabbit on PR 483 (review 6012081355): an operator closes every input even
    when one close raises, so no producer is left waiting on a reader nobody reads.

    The second input is an open reader of a one-slot output whose producer waits for
    its room. After ``close_inputs``, that send no longer waits. When the operator
    succeeded, the first close error is raised once every input is closed; when it
    failed, the close error is a note on the operator's error, which is not replaced."""
    stream = source(3)
    first_batch, second_batch, _ = batches_of(stream, 1)

    async def go() -> BaseException | None:
        producer = OutputChannel(stream, retain=False, buffer_size=1)
        reader = producer.open_reader()
        await producer.send(first_batch)
        waiting = asyncio.ensure_future(producer.send(second_batch))
        await asyncio.sleep(0)
        assert not waiting.done()  # the reader's slot is full
        broken = _FailingClose()
        inputs: list[Any] = [broken, reader]
        error = RuntimeError("the operator failed") if failing else None
        raised: BaseException | None = None
        try:
            await close_inputs(inputs, error)
        except OSError as close_error:
            raised = close_error
        assert broken.closed
        async with asyncio.timeout(TIMEOUT):
            await waiting  # the closed reader is no longer waited for
        if error is not None:
            assert raised is None
            assert any("cannot close" in note for note in error.__notes__)
            return None
        return raised

    raised = asyncio.run(go())
    if not failing:
        assert isinstance(raised, OSError)


def test_a_failing_operator_releases_an_input_it_never_read() -> None:
    """CodeRabbit on PR 483 (review 6012081355), through an operator: a barrier whose
    first input fails while its second input's producer waits for room closes the
    second input, so that producer is released, and the output fails."""
    left, right = source(3), source(3, source_id="other")
    right_batches = batches_of(right, 1)
    op = SemiJoin()

    async def go() -> None:
        left_out = OutputChannel(left, retain=False, buffer_size=1)
        right_out = OutputChannel(right, retain=False, buffer_size=1)
        inputs = [left_out.open_reader(), right_out.open_reader()]
        output = OutputChannel(op(left, right))
        for batch in right_batches[:1]:
            await right_out.send(batch)
        waiting = asyncio.ensure_future(right_out.send(right_batches[1]))
        left_out.fail(OSError("left producer failed"))
        with pytest.raises(UpstreamFailedError, match="left producer failed"):
            async with asyncio.timeout(TIMEOUT):
                await op.async_execute_batches(inputs, output)
        async with asyncio.timeout(TIMEOUT):
            await waiting  # released: the operator closed the right input
        with pytest.raises(RuntimeError, match="no result"):
            _ = output.result()

    asyncio.run(go())


# --- Batch holds no batch without rows (Codex on PR 483, round 1, finding 2) ---------


class _PausingFeed:
    """A feed that delivers some record batches, waits for ``resume``, then the rest."""

    def __init__(
        self,
        before: list[pa.RecordBatch],
        after: list[pa.RecordBatch],
        resume: asyncio.Event,
        paused: asyncio.Event,
    ) -> None:
        self._before, self._after = list(before), list(after)
        self._resume, self._paused = resume, paused

    async def next(self) -> pa.RecordBatch | None:
        if self._before:
            return self._before.pop(0)
        if not self._resume.is_set():
            self._paused.set()
            await self._resume.wait()
        return self._after.pop(0) if self._after else None

    def close(self) -> None:
        self._before.clear()
        self._after.clear()

    @property
    def opened_on(self) -> Any:  # pyright: ignore[reportExplicitAny]
        return None


@pytest.mark.parametrize("size", [2, 3])
def test_batch_holds_no_arrival_without_rows(size: int) -> None:
    """Codex on PR 483, round 1, finding 2: ``Batch`` with a size keeps the rows that
    have arrived until a batch is full, and must not keep the record batches with no
    row among them (a producer could grow that list without limit). Many zero-row
    batches arrive while a row waits; none is held."""
    from orcapod.core.operators import Batch
    from orcapod.core.streams.record_batch_input import InputChannel

    stream = source(4)
    rows = batches_of(stream, 1)
    empties = [rows[0].slice(0, 0) for _ in range(50)]
    held = [weakref.ref(empty) for empty in empties]
    resume, paused = asyncio.Event(), asyncio.Event()
    feed = _PausingFeed([rows[0], *empties], rows[1:], resume, paused)
    del empties
    op = Batch(batch_size=size)

    async def go() -> Any:  # pyright: ignore[reportExplicitAny]
        output = OutputChannel(op(stream))
        task = asyncio.ensure_future(
            op.async_execute_batches([InputChannel(stream, feed)], output)
        )
        async with asyncio.timeout(TIMEOUT):
            await paused.wait()
        gc.collect()
        # The last one may still be the loop's current batch; no other is held.
        alive = sum(ref() is not None for ref in held[:-1])
        resume.set()
        async with asyncio.timeout(TIMEOUT):
            await task
        return alive, output.result()

    alive, result = asyncio.run(go())
    assert alive == 0
    expected = R(op(stream))
    assert result.as_table(columns=ALL).num_rows == expected.num_rows


# --- Arrivals whose provenance flags differ (Codex on PR 483, round 1, finding 2) ----


@pytest.mark.parametrize("kind", ["system_tag", "source_info"])
def test_batch_takes_arrivals_whose_provenance_field_flags_differ(kind: str) -> None:
    """Codex on PR 483, round 1, finding 2: arrivals built from their own rows may
    flag a provenance field nullable in one and not in the next (the send check does
    not compare those flags). ``Batch(2)`` puts them together and gives the sync
    result, for a system-tag field as for a source-info field."""
    from orcapod.core.operators import Batch

    stream = source(2)
    first, second = batches_of(stream, 1)
    prefix = "_tag" if kind == "system_tag" else "_source_"
    index = next(i for i, n in enumerate(first.schema.names) if n.startswith(prefix))
    field = first.schema.field(index)
    flagged = pa.RecordBatch.from_arrays(
        first.columns,
        schema=first.schema.set(index, field.with_nullable(not field.nullable)),
    )
    assert flagged.schema.field(index).nullable != second.schema.field(index).nullable
    op = Batch(batch_size=2)

    async def go() -> Any:  # pyright: ignore[reportExplicitAny]
        middle = OutputChannel(stream)
        output = OutputChannel(op(stream))
        reader = middle.open_reader()
        await middle.send(flagged)
        await middle.send(second)
        await middle.close()
        async with asyncio.timeout(TIMEOUT):
            await op.async_execute_batches([reader], output)
        return output.result()

    result = asyncio.run(go())
    expected = R(op(stream))
    table = result.as_table(columns=ALL)
    assert table.num_rows == expected.num_rows == 1
    assert table.select(expected.column_names).to_pylist() == expected.to_pylist()


# --- Representations other than the large variant (Codex on PR 483, round 1, 4) ------


def _typed_stream() -> ArrowTableStream:
    return ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "b": pa.array([b"x", b"y"], pa.large_binary()),
                "l": pa.array([[1], [2, 3]], pa.large_list(pa.int64())),
                "s": pa.array(
                    [{"a": "p"}, {"a": "q"}], pa.struct([("a", pa.large_string())])
                ),
                "ls": pa.array([["p"], ["q"]], pa.large_list(pa.large_string())),
            }
        ),
        tag_columns=["k"],
    )


@pytest.mark.parametrize(
    ("column", "other"),
    [
        ("b", pa.binary()),
        ("l", pa.list_(pa.int64())),
        ("s", pa.struct([("a", pa.string())])),
        ("ls", pa.large_list(pa.string())),
    ],
    ids=["binary", "list", "string_in_struct", "string_in_list"],
)
def test_a_column_in_another_representation_is_refused(
    column: str, other: pa.DataType
) -> None:
    """Codex on PR 483, round 1, finding 4: a batch passes only with the description's
    types in their large variants, at every depth; another representation that the
    result could not be assembled from is refused at send, never converted."""
    stream = _typed_stream()
    (batch,) = batches_of(stream)
    index = batch.schema.get_field_index(column)
    narrow = batch.set_column(
        index,
        batch.schema.field(index).with_type(other),
        batch.column(index).cast(other),
    )

    async def write() -> None:
        output = OutputChannel(stream)
        with pytest.raises(ValueError, match=f"column '{column}' is"):
            await output.send(narrow)
        await output.send(batch)

    asyncio.run(write())


# --- Field metadata at every depth (Codex on PR 483, round 2, finding 3) -------------


def _with_metadata_inside(kind: pa.DataType) -> pa.DataType:
    """``kind`` with metadata on its first nested field."""
    if isinstance(kind, pa.StructType):
        child = kind.field(0)
        return pa.struct(
            [
                child.with_metadata({"units": "b"}),
                *(kind.field(i) for i in range(1, kind.num_fields)),
            ]
        )
    if isinstance(kind, pa.MapType):
        item = kind.item_field.with_metadata({"units": "b"})
        return pa.map_(kind.key_type, item)  # pyright: ignore[reportArgumentType]  # pyarrow takes a field for the item; the stubs say a type
    assert isinstance(kind, pa.LargeListType)
    return pa.large_list(kind.value_field.with_metadata({"units": "b"}))


@pytest.mark.parametrize(
    "column", ["s", "m", "ls"], ids=["struct_child", "map_item", "list_item"]
)
def test_nested_field_metadata_must_be_the_descriptions(column: str) -> None:
    """Codex on PR 483, round 2, finding 3: a user field's nested field metadata
    (a struct child, a map item, a list item) must be the description's."""
    stream = ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "s": pa.array(
                    [{"a": "p"}, {"a": "q"}], pa.struct([("a", pa.large_string())])
                ),
                "m": pa.array(
                    [[("a", 1)], [("b", 2)]], pa.map_(pa.large_string(), pa.int64())
                ),
                "ls": pa.array([["p"], ["q"]], pa.large_list(pa.large_string())),
            }
        ),
        tag_columns=["k"],
    )
    (batch,) = batches_of(stream)
    index = batch.schema.get_field_index(column)
    tagged_type = _with_metadata_inside(batch.schema.field(index).type)
    tagged = batch.set_column(
        index,
        batch.schema.field(index).with_type(tagged_type),
        batch.column(index).cast(tagged_type),
    )

    async def write() -> None:
        output = OutputChannel(stream)
        with pytest.raises(ValueError, match=f"column '{column}' has field metadata"):
            await output.send(tagged)
        await output.send(batch)

    asyncio.run(write())


def _list_token_stream() -> ArrowTableStream:
    """A raw stream whose source-info column holds list tokens."""
    return ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "x": pa.array([1.5, 2.5], pa.float64()),
                "_source_x": pa.array(
                    [["s::r0::x"], ["s::r1::x"]], pa.large_list(pa.large_string())
                ),
            }
        ),
        tag_columns=["k"],
    )


@pytest.mark.parametrize("where", ["top_level", "list_item"])
def test_a_source_info_field_carries_no_metadata(where: str) -> None:
    """Codex on PR 483, round 2, finding 3: a source-info field carries no field
    metadata, at its own level or on a list item, as a row builds it."""
    stream = source() if where == "top_level" else _list_token_stream()
    (batch,) = batches_of(stream)
    index = next(
        i for i, n in enumerate(batch.schema.names) if n.startswith("_source_")
    )
    field = batch.schema.field(index)
    if where == "top_level":
        tagged = pa.RecordBatch.from_arrays(
            batch.columns,
            schema=batch.schema.set(index, field.with_metadata({"m": "1"})),
        )
    else:
        tagged_type = _with_metadata_inside(field.type)
        tagged = batch.set_column(
            index, field.with_type(tagged_type), batch.column(index).cast(tagged_type)
        )

    async def write() -> None:
        output = OutputChannel(stream)
        with pytest.raises(ValueError, match="carries field metadata"):
            await output.send(tagged)
        await output.send(batch)

    asyncio.run(write())


# --- A null-typed source-info column (Codex on PR 483, round 2, finding 5: declined) --


def test_a_token_sent_into_a_null_typed_source_info_column_is_taken() -> None:
    """Codex on PR 483, round 2, finding 5 (declined): a source-info column whose raw
    type is ``null`` is described by its tokens' type, as a row types a token, so a
    batch with a real token is accepted at send, and the result's column is
    ``large_string``. This pins the declined behaviour."""
    stream = ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "x": pa.array([1.5, 2.5], pa.float64()),
                "_source_x": pa.array([None, None], pa.null()),
            }
        ),
        tag_columns=["k"],
    )
    (batch,) = batches_of(stream)
    index = batch.schema.get_field_index("_source_x")
    tokened = batch.set_column(
        index,
        pa.field("_source_x", pa.large_string()),
        pa.array(["s::r0::x", None], pa.large_string()),
    )

    async def write() -> Any:  # pyright: ignore[reportExplicitAny]
        output = OutputChannel(stream)
        await output.send(tokened)
        await output.close()
        return output.result()

    table = asyncio.run(write()).as_table(columns=ALL)
    assert table.schema.field("_source_x").type == pa.large_string()
    assert table.column("_source_x").to_pylist() == ["s::r0::x", None]


# --- Duplicate columns (Codex on PR 485, final round, finding 3) ---------------------


@pytest.mark.parametrize("kind", ["data", "system_tag", "source_info"])
@pytest.mark.parametrize("declared", [True, False], ids=["declared", "undeclared"])
def test_a_batch_with_a_column_twice_is_refused(declared: bool, kind: str) -> None:
    """Codex on PR 485's final round, finding 3: a batch holding a column twice is
    refused whatever the description says; the undeclared system-tag case is the one
    only the duplicate check refuses."""
    from tests.test_channels.test_record_batch_channels import _UntypedStream  # pyright: ignore[reportPrivateUsage]

    table = source().as_table(columns=ALL)
    stream = (
        source()
        if declared
        else _UntypedStream(
            table, tag_columns=["subject", "trial"], data_tags=["trial"]
        )
    )
    (batch,) = batches_of(stream)
    prefix = {"data": None, "system_tag": "_tag", "source_info": "_source_"}[kind]
    name = next(
        n
        for n in batch.schema.names
        if (n.startswith(prefix) if prefix else n == "amplitude")
    )
    doubled = batch.append_column(batch.schema.field(name), batch.column(name))

    async def write() -> None:
        await OutputChannel(stream).send(doubled)

    with pytest.raises(ValueError, match="a column more than once"):
        asyncio.run(write())


# --- A read error after a successful read (Codex on PR 488, round 2, finding 5) ------


@pytest.mark.parametrize("read", ["iteration", "collect", "to_stream"])
def test_a_channel_closed_raised_by_a_later_fetch_is_a_read_error(read: str) -> None:
    """Codex on PR 488, round 2, finding 5: a source whose first fetch succeeds and a
    later fetch raises ``ChannelClosed`` (its own input closed early): the channel read
    that meets it raises it, through iteration, ``collect`` and ``to_stream``; it is
    never taken for the end of the input."""
    from orcapod.channels import ChannelClosed
    from orcapod.core.sources import PollingSource
    from orcapod.types import Schema
    from tests.test_channels.test_polling_source import FakeDynamicSource, _batch

    class _ClosingLater(FakeDynamicSource):
        async def fetch(self, cursor: Any = None) -> Any:  # pyright: ignore[reportExplicitAny]
            if self.fetch_cursors:
                self.fetch_cursors.append(cursor)
                raise ChannelClosed("underlying input closed early")
            return await super().fetch(cursor)

    fake = _ClosingLater(
        batches=[_batch(1, 10), _batch(2, 20)],
        schema_override=Schema({"id": int, "val": int}),
    )
    polling = PollingSource(
        fake, tag_columns="id", record_id_columns="id", source_id="later"
    )

    async def go() -> None:
        first = await polling.open_channel().collect()  # the first fetch succeeds
        assert sum(batch.num_rows for batch in first) == 1
        channel = polling.open_channel()
        with pytest.raises(ChannelClosed, match="underlying input"):
            if read == "iteration":
                async for _ in channel:
                    pass
            elif read == "collect":
                _ = await channel.collect()
            else:
                _ = await channel.to_stream()

    asyncio.run(go())
