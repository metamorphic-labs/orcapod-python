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


def _list_token_stream(token_type: pa.DataType | None = None) -> ArrowTableStream:
    """A raw stream whose source-info column holds list tokens.

    Args:
        token_type: The token column's declared type; a plain large list of
            strings when not given.
    """
    return ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                # A list token needs list data (the token-shape rule, ITL-1491).
                "x": pa.array([[1.5], [2.5]], pa.large_list(pa.float64())),
                "_source_x": pa.array(
                    [["s::r0::x"], ["s::r1::x"]],
                    token_type or pa.large_list(pa.large_string()),
                ),
            }
        ),
        tag_columns=["k"],
    )


def test_a_source_info_field_carries_no_metadata_of_its_own() -> None:
    """Codex on PR 483, round 2, finding 3: a source-info field carries no field
    metadata at its own level, as a row builds it."""
    stream = source()
    (batch,) = batches_of(stream)
    index = next(
        i for i, n in enumerate(batch.schema.names) if n.startswith("_source_")
    )
    field = batch.schema.field(index)
    tagged = pa.RecordBatch.from_arrays(
        batch.columns,
        schema=batch.schema.set(index, field.with_metadata({"m": "1"})),
    )

    async def write() -> None:
        output = OutputChannel(stream)
        with pytest.raises(ValueError, match="carries field metadata"):
            await output.send(tagged)
        await output.send(batch)

    asyncio.run(write())


def test_a_token_columns_item_metadata_is_part_of_its_declared_type() -> None:
    """Restated for ITL-1557 (fifth review of PR #607, finding 1; first restated
    for ITL-1491, review of PR #601, P2-4): metadata on a token list's item field
    is part of the column's declared type. A stream that declares it keeps it, in
    its table and in its record batches, and the stream an output channel produces
    declares it too. The stream's declaration binds every batch, the first one
    included: a batch without the metadata is refused whenever it is sent, and a
    batch with it is taken."""
    from orcapod.core.operators.static_output_pod import SourceInfoTypesDisagree

    plain_type = pa.large_list(pa.large_string())
    tagged_type = _with_metadata_inside(plain_type)
    stream = _list_token_stream(tagged_type)
    (tagged,) = batches_of(stream)
    index = next(
        i for i, n in enumerate(tagged.schema.names) if n.startswith("_source_")
    )
    field = tagged.schema.field(index)
    # The declared stream and its batches hold the item metadata.
    declared = stream.as_table(columns={"source": True}).schema.field(field.name)
    assert declared.type.equals(tagged_type, check_metadata=True)
    assert field.type.equals(tagged_type, check_metadata=True)
    plain = tagged.set_column(
        index, field.with_type(plain_type), tagged.column(index).cast(plain_type)
    )

    async def write() -> pa.Table:
        output = OutputChannel(stream)
        with pytest.raises(SourceInfoTypesDisagree, match=field.name):
            await output.send(plain.slice(0, 1))
        await output.send(tagged.slice(0, 1))
        with pytest.raises(SourceInfoTypesDisagree, match=field.name):
            await output.send(plain.slice(1, 1))
        await output.send(tagged.slice(1, 1))
        await output.close()
        return output.result().as_table(columns={"source": True})

    result = asyncio.run(write())
    assert result.schema.field(field.name).type.equals(tagged_type, check_metadata=True)
    assert result.num_rows == 2


def test_a_batch_may_not_add_item_metadata_to_a_declared_token_type() -> None:
    """Fifth review of PR #607, finding 1: a concrete stream's declared token type
    binds its first batch too, item metadata included. A first batch whose token
    item field carries metadata the stream does not declare is refused (it used to
    be taken, and replaced the stream's declared type), and the stream's own batch
    is then taken with the declared type."""
    from orcapod.core.operators.static_output_pod import SourceInfoTypesDisagree

    stream = _list_token_stream()
    (batch,) = batches_of(stream)
    index = next(
        i for i, n in enumerate(batch.schema.names) if n.startswith("_source_")
    )
    field = batch.schema.field(index)
    tagged_type = _with_metadata_inside(field.type)
    tagged = batch.set_column(
        index, field.with_type(tagged_type), batch.column(index).cast(tagged_type)
    )

    async def write() -> pa.Table:
        output = OutputChannel(stream)
        with pytest.raises(SourceInfoTypesDisagree, match=field.name):
            await output.send(tagged)
        await output.send(batch)
        await output.close()
        return output.result().as_table(columns={"source": True})

    result = asyncio.run(write())
    assert result.schema.field(field.name).type.equals(field.type, check_metadata=True)
    assert result.num_rows == 2


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


# --- CodeRabbit on PR 510 -------------------------------------------------------------


def _non_nullable_stream() -> ArrowTableStream:
    """Tags ``k`` and ``g``, data ``x`` and ``y``, every field non-nullable."""
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("g", pa.large_string(), nullable=False),
            pa.field("x", pa.float64(), nullable=False),
            pa.field("y", pa.int64(), nullable=False),
        ]
    )
    table = pa.table(
        {
            "k": pa.array([1, 2], pa.int64()),
            "g": pa.array(["a", "b"], pa.large_string()),
            "x": pa.array([1.5, 2.5], pa.float64()),
            "y": pa.array([3, 4], pa.int64()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["k", "g"])


@pytest.mark.parametrize("column", ["k", "g", "x", "y"])
def test_a_null_in_a_column_declared_non_nullable_is_refused(column: str) -> None:
    """CodeRabbit on PR 510 (major): the send check compared the nullability flag
    only, so a batch whose field is also flagged non-nullable but holds a null passed
    and reached readers of a stream that declares no null there. Tags and data."""
    stream = _non_nullable_stream()
    (batch,) = batches_of(stream)
    assert not stream.arrow_schema().field(column).nullable
    index = batch.schema.get_field_index(column)
    values = batch.column(index).to_pylist()
    values[0] = None
    nulled = pa.RecordBatch.from_arrays(
        [
            *batch.columns[:index],
            pa.array(values, batch.schema.field(index).type),
            *batch.columns[index + 1 :],
        ],
        schema=batch.schema,  # the field still says non-nullable
    )
    assert not nulled.schema.field(column).nullable

    async def write() -> None:
        output = OutputChannel(stream)
        with pytest.raises(ValueError, match=f"column '{column}' holds nulls"):
            await output.send(nulled)
        await output.send(batch)

    asyncio.run(write())


@pytest.mark.parametrize("where", ["key", "item"])
def test_a_map_typed_system_tag_column_carries_no_nested_metadata(where: str) -> None:
    """CodeRabbit on PR 510: the provenance check strips field metadata at every depth
    to compare, and missed the key and item fields of a map: a system-tag column of
    map type with metadata on its key or item passed. A map-typed source-info column
    is not a token type at all and is refused."""
    from orcapod.core.record_batch_channels import _schema_mismatch  # pyright: ignore[reportPrivateUsage]

    plain = pa.map_(pa.large_string(), pa.large_string())
    key = plain.key_field
    item = plain.item_field
    if where == "key":
        tagged = pa.map_(key.with_metadata({"m": "1"}), item)  # pyright: ignore[reportArgumentType]  # a field for the key
    else:
        tagged = pa.map_(key, item.with_metadata({"m": "1"}))  # pyright: ignore[reportArgumentType]  # a field for the item
    values = [[("a", "b")]]
    declared = pa.schema([pa.field("k", pa.int64()), pa.field("_tag_m", plain)])
    good = pa.RecordBatch.from_arrays(
        [pa.array([1], pa.int64()), pa.array(values, plain)], schema=declared
    )
    assert _schema_mismatch(declared, good) is None
    bad = pa.RecordBatch.from_arrays(
        [pa.array([1], pa.int64()), pa.array(values, plain).cast(tagged)],
        names=["k", "_tag_m"],
    )
    assert "carries field metadata" in str(_schema_mismatch(declared, bad))
    source_declared = pa.schema(
        [pa.field("k", pa.int64()), pa.field("_source_x", plain)]
    )
    source_batch = pa.RecordBatch.from_arrays(
        [pa.array([1], pa.int64()), pa.array(values, plain)], schema=source_declared
    )
    assert "not a token type" in str(_schema_mismatch(source_declared, source_batch))


@pytest.mark.parametrize("ending", ["returns", "raises", "cancelled"])
def test_a_reader_whose_task_finishes_between_reads_releases_its_producer(
    ending: str,
) -> None:
    """CodeRabbit on PR 510: a reader belongs to the task that first reads it. When
    that task finishes between reads (returns, raises or is cancelled) without
    reaching the end or closing it, the reader is closed, so the producer of a
    bounded output does not wait for it; another task cannot read it either."""
    from orcapod.channels import ChannelClosed

    stream = source(3)
    first, second, third = batches_of(stream, 1)

    async def go() -> None:
        output = OutputChannel(stream, retain=False, buffer_size=1)
        reader = output.open_reader()
        await output.send(first)

        async def consume_one() -> None:
            _ = await reader.receive()
            if ending == "raises":
                raise RuntimeError("the consumer failed")
            if ending == "cancelled":
                await asyncio.Event().wait()  # cancelled below

        task = asyncio.ensure_future(consume_one())
        await output.send(second)  # fills the reader's slot after the first is taken
        if ending == "cancelled":
            await asyncio.sleep(0)
            _ = task.cancel()
        _ = await asyncio.gather(task, return_exceptions=True)
        async with asyncio.timeout(TIMEOUT):
            await output.send(third)  # the reader is gone: nothing waits for it
        with pytest.raises(ChannelClosed):
            _ = await reader.receive()

    asyncio.run(go())


# --- Codex on PR 510 ---------------------------------------------------------------


@pytest.mark.parametrize("ending", ["returns", "raises", "cancelled"])
def test_a_reader_opened_in_a_finished_task_releases_its_producer(ending: str) -> None:
    """Codex on PR 510: a task opens a reader in its own frame, takes one batch and
    ends; the reader is freed before the task's done callback can ask it, so the
    callback closes its feed directly and the producer of a one-slot output does
    not wait for a reader nobody holds."""
    stream = source(3)
    first, second, third = batches_of(stream, 1)

    async def go() -> None:
        output = OutputChannel(stream, retain=False, buffer_size=1)

        async def consume_one() -> None:
            reader = output.open_reader()  # held by this frame only
            _ = await reader.receive()
            if ending == "raises":
                raise RuntimeError("the consumer failed")
            if ending == "cancelled":
                await asyncio.Event().wait()  # cancelled below

        task = asyncio.ensure_future(consume_one())
        await asyncio.sleep(0)  # the reader is registered
        await output.send(first)
        await output.send(second)  # fills the reader's slot after it took the first
        if ending == "cancelled":
            await asyncio.sleep(0)
            _ = task.cancel()
        _ = await asyncio.gather(task, return_exceptions=True)
        gc.collect()
        async with asyncio.timeout(TIMEOUT):
            await output.send(third)  # nothing waits for the gone reader

    asyncio.run(go())


def _token_batch(
    stream: ArrowTableStream, tokens: list[Any], kind: pa.DataType
) -> pa.RecordBatch:  # pyright: ignore[reportExplicitAny]  # tokens of any shape
    """``stream``'s rows as sent, with ``_source_x``'s tokens replaced."""
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
    from orcapod.core.operators.streaming_joins import TRANSPORT_COLUMNS

    rebuilt = StaticOutputOperatorPod._materialize_upstream(  # pyright: ignore[reportPrivateUsage]  # the rows as a job hands them on
        list(stream.iter_data()), stream
    )
    sent = rebuilt.as_table(columns=TRANSPORT_COLUMNS).combine_chunks()
    index = sent.schema.get_field_index("_source_x")
    return sent.set_column(
        index, pa.field("_source_x", kind), pa.array(tokens, kind)
    ).to_batches()[0]


DEEP_TOKENS = pa.large_list(pa.large_list(pa.large_string()))
SHALLOW_TOKENS = pa.large_list(pa.large_string())


@pytest.mark.parametrize(
    "shallow_first", [True, False], ids=["shallow_first", "deep_first"]
)
@pytest.mark.parametrize("kernel", ["join", "merge_join", "semi_join"])
def test_batches_of_one_input_that_declare_two_token_types_are_refused(
    kernel: str, shallow_first: bool
) -> None:
    """Codex on PR 510, restated for ITL-1491: the pipeline determines a token
    column's type, so every batch of one input declares the same one. A batch that
    declares another (one list level where the other declares two) is refused, also
    when no row would match, instead of finishing with no row. The sync path reads
    the same rows with the type their stream declares, whatever the tokens show."""
    from orcapod.core.operators import Join, MergeJoin, SemiJoin
    from orcapod.core.operators.streaming_joins import (
        StreamingJoin,
        StreamingMergeJoin,
        StreamingSemiJoin,
    )

    left = ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                # ``x`` has its token's two list layers (the token-shape rule).
                "x": pa.array(
                    [[[10]], [[20]]], pa.large_list(pa.large_list(pa.int64()))
                ),
                "_source_x": pa.array([[["a"]], [["b"]]], DEEP_TOKENS),
            }
        ),
        tag_columns=["k"],
    )
    right = ArrowTableStream(
        pa.table(
            {"k": pa.array([7, 8], pa.int64()), "y": pa.array([5, 6], pa.int64())}
        ),
        tag_columns=["k"],
    )
    make = {
        "join": lambda: StreamingJoin(Join(), (left, right)),
        "merge_join": lambda: StreamingMergeJoin(MergeJoin(), (left, right)),
        "semi_join": lambda: StreamingSemiJoin(SemiJoin(), (left, right)),
    }[kernel]
    shallow = _token_batch(left, [[], []], SHALLOW_TOKENS).slice(0, 1)
    deep = _token_batch(left, [[["a"]], [["b"]]], DEEP_TOKENS).slice(1, 1)
    batches = [shallow, deep] if shallow_first else [deep, shallow]
    # The sync path: the same rows keep the type their stream declares.
    tokens = [[], [["b"]]] if shallow_first else [[["a"]], []]
    mixed = ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "x": pa.array(
                    [[[10]], [[20]]], pa.large_list(pa.large_list(pa.int64()))
                ),
                "_source_x": pa.array(tokens, DEEP_TOKENS),
            }
        ),
        tag_columns=["k"],
    )
    rebuilt = _token_batch(mixed, tokens, DEEP_TOKENS)
    assert rebuilt.schema.field("_source_x").type == DEEP_TOKENS
    assert rebuilt.column("_source_x").to_pylist() == tokens
    # The batch that declares another type than its stream is refused when it
    # arrives, first or second (a ``SourceInfoTypesDisagree`` is a ``ValueError``);
    # the stream's own batch is taken.
    instance = make()
    for batch in batches:
        if batch is shallow:
            with pytest.raises(ValueError, match="_source_x"):
                _ = instance.push(0, batch)
        else:
            _ = instance.push(0, batch)
    # The refused batch changed nothing: the input still closes, with no row.
    assert instance.close(0) == []


NON_NULLABLE = pa.field("required", pa.int64(), nullable=False)
NESTED_CASES: dict[str, tuple[pa.DataType, list[Any], list[Any]]] = {  # pyright: ignore[reportExplicitAny]  # values of any shape
    # type, a refused value list, an accepted value list (nulls only where allowed)
    "struct_child": (
        pa.struct([NON_NULLABLE]),
        [{"required": None}, {"required": 1}],
        [None, {"required": 1}],
    ),
    "list_item": (
        pa.large_list(pa.field("item", pa.int64(), nullable=False)),
        [[1, None], [2]],
        [None, [2]],
    ),
    "map_item": (
        pa.map_(pa.large_string(), pa.field("value", pa.int64(), nullable=False)),  # pyright: ignore[reportArgumentType]  # pyarrow takes a field for the item
        [[("a", None)], [("b", 2)]],
        [None, [("b", 2)]],
    ),
    "map_key": (
        pa.map_(
            pa.field("key", pa.struct([NON_NULLABLE]), nullable=False),  # pyright: ignore[reportArgumentType]  # pyarrow takes a field for the key
            pa.int64(),
        ),
        [[({"required": None}, 1)], [({"required": 2}, 3)]],
        [None, [({"required": 2}, 3)]],
    ),
    "struct_in_struct": (
        pa.struct([pa.field("s", pa.struct([NON_NULLABLE]))]),
        [{"s": {"required": None}}, {"s": None}],
        [{"s": None}, None],
    ),
}


@pytest.mark.parametrize("case", list(NESTED_CASES))
def test_a_nested_null_under_a_non_nullable_field_is_refused(case: str) -> None:
    """Codex on PR 510: a null nested under a field the stream's type declares not
    nullable (a struct child, a list item, a map item, at any depth) is refused at
    send; nulls the type allows (a null struct, list or map row, a nullable parent)
    are taken."""
    kind, refused, accepted = NESTED_CASES[case]
    stream = ArrowTableStream(
        pa.table(
            {"k": pa.array([1, 2], pa.int64()), "v": pa.array(accepted, kind)},
            schema=pa.schema([pa.field("k", pa.int64()), pa.field("v", kind)]),
        ),
        tag_columns=["k"],
    )
    (batch,) = batches_of(stream)
    index = batch.schema.get_field_index("v")
    bad = batch.set_column(index, batch.schema.field(index), pa.array(refused, kind))

    async def write() -> None:
        output = OutputChannel(stream)
        with pytest.raises(ValueError, match="which the stream's type declares not"):
            await output.send(bad)
        await output.send(batch)

    asyncio.run(write())


def test_a_system_tag_cannot_hold_a_null_in_a_required_struct_child() -> None:
    name = "_tag_extra"
    kind = pa.struct(
        [pa.field("required", pa.int64(), nullable=False, metadata={b"unit": b"m"})]
    )
    stream = ArrowTableStream(
        pa.table(
            {
                "k": [1, 2],
                "x": [3, 4],
                name: pa.array([{"required": 1}, None], type=kind),
            }
        ),
        tag_columns=["k"],
    )
    (good,) = pa.Table.from_batches(batches_of(stream)).combine_chunks().to_batches()
    index = good.schema.get_field_index(name)
    bad = good.set_column(
        index,
        good.schema.field(index),
        pa.array([{"required": None}, None], type=kind),
    )

    async def write() -> None:
        output = OutputChannel(stream)
        with pytest.raises(ValueError, match=r"_tag_extra.*required.*not nullable"):
            await output.send(bad)
        await output.send(good)
        await output.close()
        result = output.result().as_table(columns={"system_tags": True})
        assert result.schema.field(name).equals(
            good.schema.field(name), check_metadata=True
        )
        assert result[name].to_pylist() == [{"required": 1}, None]

    asyncio.run(write())


def test_empty_channel_result_keeps_accepted_arrow_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    field = pa.field("x", pa.int8(), nullable=True, metadata={b"unit": b"m"})
    table = pa.schema([field]).empty_table()
    stream = ArrowTableStream(table, tag_columns=[])
    monkeypatch.setattr(stream, "_typed_zero_row_table", lambda: None)
    batch = pa.RecordBatch.from_arrays([pa.array([], pa.int8())], schema=table.schema)

    async def run() -> None:
        output = OutputChannel(stream)
        await output.send(batch)
        await output.close()
        assert (
            output.result()
            .as_table()
            .schema.field("x")
            .equals(field, check_metadata=True)
        )

    asyncio.run(run())


# --- CodeRabbit on PR 510, final pass (365f2a08) ------------------------------------


def _deep_token_stream(tokens: list[Any], keys: list[int]) -> ArrowTableStream:  # pyright: ignore[reportExplicitAny]  # tokens of any shape
    """Tag ``k``, data ``x`` and ``y``, ``x``'s source info declared two list levels
    deep."""
    return ArrowTableStream(
        pa.table(
            {
                "k": pa.array(keys, pa.int64()),
                # ``x`` has its token's two list layers (the token-shape rule).
                "x": pa.array(
                    [[[10 * k]] for k in keys], pa.large_list(pa.large_list(pa.int64()))
                ),
                "y": pa.array([k + 0.5 for k in keys], pa.float64()),
                "_source_x": pa.array(tokens, DEEP_TOKENS),
            }
        ),
        tag_columns=["k"],
    )


@pytest.mark.parametrize(
    "matching", [True, False], ids=["keys_match", "no_key_matches"]
)
@pytest.mark.parametrize("shallow", ["left", "right"])
def test_merge_join_inputs_whose_tokens_show_two_depths_are_joined(
    shallow: str, matching: bool
) -> None:
    """CodeRabbit on PR 510, restated for ITL-1491: both inputs declare ``_source_x``
    two list levels deep; one delivers ``[]`` (which shows one level) and the other
    ``[["b"]]``. A token column's type is the one its stream declares, never taken
    from the tokens, so the two inputs have one type: the sync path joins them, and
    the kernel gives the same rows, whether or not a key matches."""
    from orcapod.core.operators import MergeJoin
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
    from orcapod.core.operators.streaming_joins import (
        TRANSPORT_COLUMNS,
        StreamingMergeJoin,
    )

    right_keys = [1, 2] if matching else [7, 8]
    empty = _deep_token_stream([[], []], [1, 2])
    deep = _deep_token_stream([[["b"]], [["c"]]], right_keys)
    left, right = (empty, deep) if shallow == "left" else (deep, empty)

    def rebuilt(stream: ArrowTableStream) -> Any:  # pyright: ignore[reportExplicitAny]
        return StaticOutputOperatorPod._materialize_upstream(  # pyright: ignore[reportPrivateUsage]  # the rows as a job hands them on
            list(stream.iter_data()), stream
        )

    sync = MergeJoin().static_process(rebuilt(left), rebuilt(right))
    expected = sync.as_table(columns=TRANSPORT_COLUMNS)
    assert expected.num_rows == (2 if matching else 0)
    kernel = StreamingMergeJoin(MergeJoin(), (left, right))
    batches = [
        rebuilt(s).as_table(columns=TRANSPORT_COLUMNS).combine_chunks().to_batches()[0]
        for s in (left, right)
    ]
    for batch in batches:
        assert batch.schema.field("_source_x").type == DEEP_TOKENS
    out = kernel.push(0, batches[0]) + kernel.push(1, batches[1])
    out += kernel.close(0) + kernel.close(1)
    assert sum(batch.num_rows for batch in out) == expected.num_rows
    if matching:
        got = pa.Table.from_batches(out).select(expected.column_names)
        assert (
            got.schema.field("_source_x").type
            == expected.schema.field("_source_x").type
        )
        assert sorted(got.to_pylist(), key=lambda row: row["k"]) == sorted(
            expected.to_pylist(), key=lambda row: row["k"]
        )


def test_merge_join_inputs_whose_tokens_settle_to_one_type_are_joined() -> None:
    """CodeRabbit on PR 510: both inputs hold only empty token lists: they join."""
    from orcapod.core.operators import MergeJoin
    from orcapod.core.operators.streaming_joins import (
        TRANSPORT_COLUMNS,
        StreamingMergeJoin,
    )
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod

    left = _deep_token_stream([[], []], [1, 2])
    right = _deep_token_stream([[], []], [1, 2])
    kernel = StreamingMergeJoin(MergeJoin(), (left, right))
    out: list[pa.RecordBatch] = []
    for index, stream in enumerate((left, right)):
        table = StaticOutputOperatorPod._materialize_upstream(  # pyright: ignore[reportPrivateUsage]  # the rows as a job hands them on
            list(stream.iter_data()), stream
        ).as_table(columns=TRANSPORT_COLUMNS)
        out += kernel.push(index, table.combine_chunks().to_batches()[0])
    out += kernel.close(0) + kernel.close(1)
    assert sum(batch.num_rows for batch in out) == 2


@pytest.mark.parametrize(
    "queued", [False, True], ids=["nothing_queued", "a_batch_queued"]
)
def test_a_bounded_reader_dropped_before_its_first_read_releases_its_producer(
    queued: bool,
) -> None:
    """CodeRabbit on PR 510 (major): a reader opened on a bounded output and dropped
    before any read (no owner task, no claim) is closed when it is freed, so the
    producer does not wait for it; a live reader still gets every batch."""
    stream = source(3)
    batches = batches_of(stream, 1)

    async def go() -> list[int]:
        output = OutputChannel(stream, retain=False, buffer_size=1)
        live = output.open_reader()
        dropped = output.open_reader()
        if queued:
            await output.send(batches[0])  # queued for both readers, never read
            got = [_trial(await live.receive())]
        else:
            got = []
        del dropped
        gc.collect()
        for batch in batches[1:] if queued else batches:
            async with asyncio.timeout(TIMEOUT):
                await output.send(batch)  # nothing waits for the dropped reader
            got.append(_trial(await live.receive()))
        return got

    assert asyncio.run(go()) == [0, 1, 2]


class _CancelledClose:
    """An input channel whose close is cancelled."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True
        raise asyncio.CancelledError()


@pytest.mark.parametrize("failing", [False, True], ids=["succeeding", "failing"])
def test_a_cancelled_close_still_closes_every_other_input(failing: bool) -> None:
    """CodeRabbit on PR 510 (outside the diff): a close cancelled while an operator
    closes its inputs does not stop the others; the cancellation is raised once every
    input is closed, and an operator error in flight keeps precedence (the
    cancellation is noted on it)."""
    stream = source(3)
    first, second, _ = batches_of(stream, 1)

    async def go() -> BaseException | None:
        producer = OutputChannel(stream, retain=False, buffer_size=1)
        reader = producer.open_reader()
        await producer.send(first)
        waiting = asyncio.ensure_future(producer.send(second))
        await asyncio.sleep(0)
        assert not waiting.done()
        cancelled = _CancelledClose()
        error = RuntimeError("the operator failed") if failing else None
        raised: BaseException | None = None
        try:
            await close_inputs([cancelled, reader], error)  # pyright: ignore[reportArgumentType]  # a stand-in input
        except asyncio.CancelledError as cancellation:
            raised = cancellation
        assert cancelled.closed
        async with asyncio.timeout(TIMEOUT):
            await waiting  # the second input was closed all the same
        if error is not None:
            assert any("cancelled" in note for note in error.__notes__)
        return raised

    raised = asyncio.run(go())
    assert (raised is None) == failing


class _ListFeed:
    """A feed that delivers the given record batches, then ends."""

    def __init__(self, batches: list[pa.RecordBatch]) -> None:
        self._batches = list(batches)

    async def next(self) -> pa.RecordBatch | None:
        return self._batches.pop(0) if self._batches else None

    def close(self) -> None:
        self._batches.clear()

    @property
    def opened_on(self) -> Any:  # pyright: ignore[reportExplicitAny]
        return None


def test_batches_that_declare_two_token_types_fail_the_operator() -> None:
    """CodeRabbit on PR 510, restated for ITL-1491: a row-by-row operator processes
    batches as they arrive; a later batch that declares another source-info token
    type than the earlier ones (two list levels, then one) is refused, and the output
    fails instead of completing with part of the rows. The operator drops the
    column, so its own output (whose send check compares token types too) would not
    see the conflict. The sync path reads such rows with their stream's one type."""
    from orcapod.core.operators import SelectDataColumns
    from orcapod.core.operators.static_output_pod import SourceInfoTypesDisagree
    from orcapod.core.streams.record_batch_input import InputChannel

    stream = _deep_token_stream([[], [["b"]]], [1, 2])  # its rows: one declared type
    clean = _deep_token_stream([[["a"]], [["b"]]], [1, 2])  # one schema, buildable
    deep = _token_batch(clean, [[["a"]], [["b"]]], DEEP_TOKENS).slice(0, 1)
    shallow = _token_batch(clean, [[], []], SHALLOW_TOKENS).slice(1, 1)
    op = SelectDataColumns(["y"])
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod

    # A job's sync path rebuilds the input from its rows, with the stream's type.
    rebuilt = StaticOutputOperatorPod._materialize_upstream(  # pyright: ignore[reportPrivateUsage]  # the job's sync form
        list(stream.iter_data()), stream
    ).as_table(columns={"source": True})
    assert rebuilt.schema.field("_source_x").type == DEEP_TOKENS
    assert rebuilt.column("_source_x").to_pylist() == [[], [["b"]]]

    sent: list[pa.RecordBatch] = []

    class ObservedOutput(OutputChannel):
        async def send(self, batch: pa.RecordBatch) -> None:
            await super().send(batch)
            sent.append(batch)

    async def go() -> OutputChannel:
        output = ObservedOutput(op(clean))
        with pytest.raises(SourceInfoTypesDisagree):
            await op.async_execute_batches(
                [InputChannel(clean, _ListFeed([deep, shallow]))], output
            )
        assert [batch.column("k").to_pylist() for batch in sent] == [[1]]
        return output

    output = asyncio.run(go())
    with pytest.raises(RuntimeError, match="has no result"):
        _ = output.result()


# --- CodeRabbit on PR 520: an input with no rows -------------------------------------


@pytest.mark.parametrize("tokens", ["one_level", "two_levels"])
@pytest.mark.parametrize("empty", [0, 1], ids=["left_empty", "right_empty"])
@pytest.mark.parametrize("kernel", ["join", "merge_join", "semi_join"])
def test_settled_tokens_against_an_input_with_no_rows_decide_as_the_sync_path(
    kernel: str, empty: int, tokens: str
) -> None:
    """CodeRabbit on PR 520: one input delivers no rows, the other's tokens are empty
    lists (``[]``, which show one list level) or fill the two levels its type
    declares. The sync path reads the empty input in its zero-row form and the other
    by its rows, both with the declared two levels (ITL-1491: a token column's type
    is never taken from the tokens), so no kernel refuses. The kernel decides as the
    sync path does when the last input closes: the same rows."""
    from orcapod.core.operators import Join, MergeJoin, SemiJoin
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
    from orcapod.core.operators.streaming_joins import (
        TRANSPORT_COLUMNS,
        StreamingJoin,
        StreamingMergeJoin,
        StreamingSemiJoin,
    )

    filled = _deep_token_stream(
        [[], []] if tokens == "one_level" else [[["a"]], [["b"]]], [1, 2]
    )
    nothing = _deep_token_stream([], [])
    if kernel == "join":
        # Join needs disjoint data: the other input under other data names.
        nothing = ArrowTableStream(
            pa.table(
                {
                    "k": pa.array([], pa.int64()),
                    "w": pa.array([], pa.large_list(pa.large_list(pa.int64()))),
                    "_source_w": pa.array([], DEEP_TOKENS),
                }
            ),
            tag_columns=["k"],
        )
    inputs = (nothing, filled) if empty == 0 else (filled, nothing)
    operator, make = {
        "join": (Join(), StreamingJoin),
        "merge_join": (MergeJoin(), StreamingMergeJoin),
        "semi_join": (SemiJoin(), StreamingSemiJoin),
    }[kernel]

    def rebuilt(stream: ArrowTableStream) -> Any:  # pyright: ignore[reportExplicitAny]
        return StaticOutputOperatorPod._materialize_upstream(  # pyright: ignore[reportPrivateUsage]  # the job's sync form
            list(stream.iter_data()), stream
        )

    try:
        sync: Any = operator.static_process(*map(rebuilt, inputs)).as_table().num_rows  # pyright: ignore[reportExplicitAny]
    except Exception as error:  # noqa: BLE001  # the refusal is the outcome compared
        sync = (type(error).__name__, str(error))

    instance = make(operator, inputs)  # pyright: ignore[reportArgumentType]  # each kernel with its own operator, from one table
    other = 1 - empty
    sent = rebuilt(inputs[other]).as_table(columns=TRANSPORT_COLUMNS)
    try:
        out = instance.push(other, sent.combine_chunks().to_batches()[0])
        out += instance.close(empty) + instance.close(other)
        result: Any = sum(batch.num_rows for batch in out)  # pyright: ignore[reportExplicitAny]
    except Exception as error:  # noqa: BLE001  # the refusal is the outcome compared
        result = (type(error).__name__, str(error))
    assert result == sync
    assert isinstance(sync, int)


# --- Review of PR 601: genuinely different declared token types (ITL-1491) ------------


def _typed_token_stream(
    tokens: list[Any],  # pyright: ignore[reportExplicitAny]  # tokens of any shape
    keys: list[int],
    kind: pa.DataType,
) -> ArrowTableStream:
    """Tags ``k`` and ``g``, data ``x`` (a list) and ``y``; ``x``'s token declared ``kind``.

    ``x`` is a list in every stream, so a list token and a plain string token are
    both valid over it (the token-shape rule, ITL-1491).
    """
    return ArrowTableStream(
        pa.table(
            {
                "k": pa.array(keys, pa.int64()),
                "g": pa.array([f"g{k}" for k in keys], pa.large_string()),
                "x": pa.array([[10 * k] for k in keys], pa.large_list(pa.int64())),
                "y": pa.array([k + 0.5 for k in keys], pa.float64()),
                "_source_x": pa.array(tokens, kind),
            }
        ),
        tag_columns=["k", "g"],
    )


@pytest.mark.parametrize(
    "layout", ["keys_match", "no_key_matches", "left_empty", "right_empty"]
)
@pytest.mark.parametrize("listed", ["left", "right"])
def test_merge_join_inputs_that_declare_two_token_types_are_refused(
    listed: str, layout: str
) -> None:
    """Review of PR 601: the two inputs DECLARE ``_source_x`` with two types (a list
    of tokens in one, a plain string token in the other; both fit the list-valued
    ``x``), on matching keys, on keys that never match and with an input that has no
    rows. The sync path refuses the two types of one column; the kernel refuses
    too, with the same error, when it is built over the inputs, before any batch."""
    from orcapod.core.operators import MergeJoin
    from orcapod.core.operators.streaming_joins import StreamingMergeJoin
    from orcapod.errors import InputValidationError

    keys = {
        "keys_match": ([1, 2], [1, 2]),
        "no_key_matches": ([1, 2], [7, 8]),
        "left_empty": ([], [1, 2]),
        "right_empty": ([1, 2], []),
    }[layout]
    string = pa.large_string()
    kinds = (SHALLOW_TOKENS, string) if listed == "left" else (string, SHALLOW_TOKENS)
    left, right = (
        _typed_token_stream(
            [["a"] if kind == SHALLOW_TOKENS else "a" for _ in side], side, kind
        )
        for side, kind in zip(keys, kinds)
    )
    with pytest.raises(InputValidationError) as sync_error:
        _ = MergeJoin().static_process(left, right)
    assert "'_source_x'" in str(sync_error.value)
    with pytest.raises(InputValidationError) as kernel_error:
        _ = StreamingMergeJoin(MergeJoin(), (left, right))
    assert str(kernel_error.value) == str(sync_error.value)
