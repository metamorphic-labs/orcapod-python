"""Operators over channels of Arrow record batches equal the sync path (ITL-1347, L6).

Every operator has one body, ``static_process``. Its Arrow-record-batch form
(``async_execute_batches``) reads input channels opened from streams and writes an
output channel, which closes into a stream. An operator whose output rows each come
from one input row is applied to every record batch as it arrives; every other
operator reads all of its inputs first (``to_stream()``) and then runs the
sync call; ``Batch`` batches consecutive arrivals.

For every case of the async/sync matrix (``test_async_sync_equivalence.CASES``) the
output channel's result must be the table today's code builds from the sync rows
(``R``: same columns, order, Arrow types, nullability, field metadata, values and row
order), however the input arrives: in one record batch, one row per batch, or batches
of mixed sizes. The same holds for inputs that come from an operator instead of a
source, and for inputs with no rows, where the result is the typed zero-row stream.
"""

from __future__ import annotations

import asyncio
import itertools
import math
from collections.abc import Callable, Sequence
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

import orcapod.contexts as contexts
import orcapod.core.operators as operators
from orcapod.core.operators import AddTags, Batch, PolarsFilter, SelectDataColumns
from orcapod.core.operators.static_output_pod import (
    StaticOutputOperatorPod,
    stream_from_batches,
)
from orcapod.core.record_batch_channels import OutputChannel
from orcapod.protocols.core_protocols.streams import RecordBatchInput
from orcapod.channels import ChannelClosed
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import SyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.system_constants import constants
from tests.test_core.operators.test_rejoin import other_context  # noqa: F401  # the fixture
from orcapod.errors import UpstreamFailedError
from orcapod.protocols.core_protocols import StreamProtocol
from tests.test_core.operators.test_async_sync_equivalence import (
    AFTER_OPERATOR_CASES,
    CASES,
    Case,
    _zero_row_cases,
    source_a,
)

pytestmark = [
    pytest.mark.filterwarnings("ignore::orcapod.errors.SemiJoinIgnoredColumnWarning"),
    pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning"),
]

ALL = {"system_tags": True, "source": True}
TIMEOUT = 20.0

# How an input's rows arrive: the sizes of its record batches, repeated to the end.
LAYOUTS: dict[str, tuple[int, ...] | None] = {
    "one_batch": None,
    "one_row_per_batch": (1,),
    "mixed_sizes": (2, 1, 3),
}


def _cut(table: pa.Table, sizes: tuple[int, ...] | None) -> list[pa.RecordBatch]:
    """``table`` as record batches of the given sizes, in order."""
    whole = table.combine_chunks()
    if sizes is None:
        return whole.to_batches()
    batches: list[pa.RecordBatch] = []
    start = 0
    for size in itertools.cycle(sizes):
        if start >= whole.num_rows:
            break
        batches.extend(whole.slice(start, size).to_batches())
        start += size
    return batches


async def _channel(
    stream: StreamProtocol, sizes: tuple[int, ...] | None
) -> RecordBatchInput:
    """An input channel delivering ``stream``'s rows in record batches of ``sizes``:
    the stream's own channel for ``None``, else one read of it, cut and re-sent
    through an output channel (never a second read of the stream)."""
    if sizes is None:
        return stream.open_channel()
    batches = await stream.open_channel().collect()
    feed = OutputChannel(stream)
    if batches:
        for batch in _cut(pa.Table.from_batches(batches), sizes):
            await feed.send(batch)
    await feed.close()
    return feed.open_reader()


def run_on_channels(
    op: StaticOutputOperatorPod,
    sources: Sequence[StreamProtocol],
    sizes: tuple[int, ...] | None,
) -> StreamProtocol:
    """Run ``op`` over input channels of ``sources``; return its output as a stream."""

    async def go() -> StreamProtocol:
        output = OutputChannel(op(*sources))
        inputs = [await _channel(source, sizes) for source in sources]
        await asyncio.wait_for(op.async_execute_batches(inputs, output), TIMEOUT)
        return output.result()

    return asyncio.run(go())


def R(stream: StreamProtocol) -> pa.Table | None:
    """The table today's code builds from ``stream``'s rows; ``None`` with no row."""
    rows = list(stream.iter_data())
    return StaticOutputOperatorPod._rows_to_table(rows)[0] if rows else None


def _same_value(value: Any, expected: Any) -> bool:
    """Equality of two Python values of a cell, a NaN equal to a NaN."""
    if isinstance(value, float) and isinstance(expected, float):
        return value == expected or (math.isnan(value) and math.isnan(expected))
    if isinstance(value, dict) and isinstance(expected, dict):
        return value.keys() == expected.keys() and all(
            _same_value(value[key], expected[key]) for key in value
        )
    if isinstance(value, (list, tuple)) and isinstance(expected, (list, tuple)):
        return len(value) == len(expected) and all(
            _same_value(a, b) for a, b in zip(value, expected)
        )
    return type(value) is type(expected) and value == expected


def assert_same_table(table: pa.Table, expected: pa.Table) -> None:
    """Assert one schema, field for field with metadata, and the same rows in order."""
    assert table.schema.names == expected.schema.names
    for field, wanted in zip(table.schema, expected.schema):
        assert field.equals(wanted, check_metadata=True), (field, wanted)
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert table.num_rows == expected.num_rows
    for name in table.column_names:
        # The storage of a logical type: its values as stored, not as decoded.
        column, wanted = (t.column(name).combine_chunks() for t in (table, expected))
        assert _same_value(column.to_pylist(), wanted.to_pylist()), name


def assert_equals_sync(case: Case, sizes: tuple[int, ...] | None) -> None:
    """Assert the channel result of ``case`` is the sync result built from rows."""
    sources = case.make_sources()
    expected = R(case.make_op()(*sources))
    result = run_on_channels(case.make_op(), sources, sizes)
    table = result.as_table(columns=ALL)
    if expected is None:
        assert table.num_rows == 0
        return
    assert_same_table(table, expected)
    # The result is an ordinary stream: its own rows build the same table again.
    again = R(result)
    assert again is not None
    assert_same_table(again, expected)


@pytest.mark.parametrize("layout", list(LAYOUTS))
@pytest.mark.parametrize("case", CASES, ids=[case.label for case in CASES])
def test_the_channel_result_is_the_sync_result(case: Case, layout: str) -> None:
    """Every operator, every way its input's record batches are cut."""
    assert_equals_sync(case, LAYOUTS[layout])


@pytest.mark.parametrize("layout", ["one_row_per_batch", "mixed_sizes"])
@pytest.mark.parametrize("case", AFTER_OPERATOR_CASES)
def test_the_channel_result_is_the_sync_result_after_an_operator(
    case: Case, layout: str
) -> None:
    """Inputs that are an operator's output, not a source: the description (pipeline
    hash, system-tag names) is the input's stream on both paths."""
    assert_equals_sync(case, LAYOUTS[layout])


@pytest.mark.parametrize("case", _zero_row_cases())
def test_inputs_with_no_rows_give_the_typed_zero_row_result(case: Case) -> None:
    """All inputs empty, or one of several: the result has no row and the columns and
    Arrow types of a run with rows, typed by the description and never by rows."""
    sources = case.make_sources()
    sync = case.make_op()(*sources)
    expected = R(sync)
    result = run_on_channels(case.make_op(), sources, (1,))
    table = result.as_table(columns=ALL)
    if expected is not None:
        assert_same_table(table, expected)
        return
    assert table.num_rows == 0
    assert result.keys() == sync.keys()
    # Arrow types are compared, not the Python schemas: the sync stream's Python schema
    # is its operator's prediction, which can differ from the Python reading of the
    # same Arrow types (``large_list<item: int16>`` predicted ``list[int | None]``, read
    # ``list[int]``), with rows as without them; a stream rebuilt from Arrow reads them.
    assert result.pipeline_hash() == sync.pipeline_hash()
    # The Arrow types are those of the sync result with no rows, column for column.
    sync_table = sync.as_table(columns=ALL)
    assert sorted(table.column_names) == sorted(sync_table.column_names)
    for name in table.column_names:
        assert table.schema.field(name).type == sync_table.schema.field(name).type, name


def test_every_operator_is_in_the_matrix() -> None:
    """A new operator fails here until the matrix has a case for it."""
    covered = {type(case.make_op()).__name__ for case in CASES}
    exported = {
        name
        for name in operators.__all__
        if isinstance(getattr(operators, name), type)
        and issubclass(getattr(operators, name), StaticOutputOperatorPod)
    }
    assert exported - covered == set()


ROW_BY_ROW = {
    "SelectTagColumns",
    "SelectDataColumns",
    "DropTagColumns",
    "DropDataColumns",
    "SelectColumns",
    "DropColumns",
    "MapTags",
    "MapData",
    "MapColumns",
    "AddTags",
    "DropNulls",
    "Index",
    "Pick",
}


def test_the_row_by_row_operators_are_exactly_these() -> None:
    """The operators applied per record batch, as the design lists them; every other
    operator reads all of its input first (``Batch`` batches arrivals)."""
    declared = {
        name
        for name in operators.__all__
        if isinstance(getattr(operators, name), type)
        and getattr(getattr(operators, name), "ROW_BY_ROW", False)
    }
    assert declared == ROW_BY_ROW


@pytest.mark.parametrize(
    "case",
    [case for case in CASES if type(case.make_op()).__name__ in ROW_BY_ROW],
    ids=lambda case: case.label,
)
def test_a_row_by_row_operator_sends_each_batch_as_it_arrives(case: Case) -> None:
    """One result per input record batch, sent before the next input is read. Each
    one-row input is sent and its result (the sync body over that row alone) is
    received before the next row is sent or the input is closed; a row whose result
    is empty sends nothing, which the end of the output shows. A barrier operator
    sends nothing until its input ends, so it fails here."""
    (source,) = case.make_sources()
    op = case.make_op()
    expected = R(case.make_op()(source))
    assert expected is not None

    async def go() -> list[pa.RecordBatch]:
        rows = pa.Table.from_batches(await source.open_channel().collect())
        inputs = _cut(rows, (1,))
        wanted = [
            case.make_op()(stream_from_batches(source, [batch])).as_table().num_rows
            for batch in inputs
        ]
        feed = OutputChannel(source, retain=False, buffer_size=1)
        reader = feed.open_reader()
        output = OutputChannel(op(source), retain=False, buffer_size=1)
        results = output.open_reader()
        task = asyncio.ensure_future(op.async_execute_batches([reader], output))
        received: list[pa.RecordBatch] = []
        for batch, rows_wanted in zip(inputs, wanted):
            await asyncio.wait_for(feed.send(batch), TIMEOUT)
            if rows_wanted:
                # This row's result arrives before the next row is sent.
                # asyncio.timeout, not wait_for: on Python 3.11 wait_for reads in
                # another task, and a reader has one consumer.
                async with asyncio.timeout(TIMEOUT):
                    result = await results.receive()
                assert result.num_rows == rows_wanted
                received.append(result)
        await feed.close()
        async with asyncio.timeout(TIMEOUT):
            assert await results.collect() == []
        await asyncio.wait_for(task, TIMEOUT)
        return received

    received = asyncio.run(go())
    table = pa.Table.from_batches(received) if received else None
    assert (table.num_rows if table is not None else 0) == expected.num_rows
    if table is not None:
        assert table.to_pylist() == expected.to_pylist()


@pytest.mark.parametrize("batch_size", [2, 3, 5])
@pytest.mark.parametrize("drop_partial_batch", [False, True])
@pytest.mark.parametrize("layout", list(LAYOUTS))
def test_batch_groups_consecutive_arrivals_whatever_the_record_batches(
    batch_size: int, drop_partial_batch: bool, layout: str
) -> None:
    """``Batch(n)`` batches every ``n`` rows that arrive, across record batch
    boundaries; the rows left at the end make the last batch unless dropped."""
    case = Case(
        "batch",
        lambda: Batch(batch_size=batch_size, drop_partial_batch=drop_partial_batch),
        lambda: (source_a(with_data_tags=True),),
    )
    assert_equals_sync(case, LAYOUTS[layout])


def test_batch_sends_each_full_batch_before_the_input_ends() -> None:
    """Streaming: the first batch is out while the input is still open."""
    source = source_a()
    op = Batch(batch_size=2)

    async def go() -> tuple[int, int]:
        feed = OutputChannel(source, retain=False, buffer_size=1)
        reader = feed.open_reader()
        output = OutputChannel(op(source))
        results = output.open_reader()
        task = asyncio.ensure_future(op.async_execute_batches([reader], output))
        rows = pa.Table.from_batches(await source.open_channel().collect())
        one_row = _cut(rows, (1,))
        await feed.send(one_row[0])
        await feed.send(one_row[1])
        async with asyncio.timeout(TIMEOUT):
            first = await results.receive()
        assert not task.done()
        await feed.send(one_row[2])
        await feed.close()
        async with asyncio.timeout(TIMEOUT):
            rest = await results.collect()
        await task
        return first.num_rows, sum(batch.num_rows for batch in rest)

    assert asyncio.run(go()) == (1, 1)


# --- The oracle of a barrier operator: what a job's sync path gives it ------------------


def _outcome(make: Callable[[], StreamProtocol]) -> tuple[str, Any]:
    """The rows a stream gives, or the kind of error that refuses it."""
    try:
        return ("rows", make().as_table().to_pylist())
    except Exception as error:  # noqa: BLE001  # the refusal is the outcome compared
        return ("refused", type(error).__name__)


BARRIER_ORACLE_CASES: dict[
    str, tuple[Callable[[], StreamProtocol], Callable[[], StaticOutputOperatorPod]]
] = {
    # A null column reaches an operator as large_string (a row's datagram stores it so).
    "null_column": (
        lambda: ArrowTableStream(
            pa.table(
                {
                    "k": pa.array([1, 2, 3], pa.int64()),
                    "x": pa.array([None, None, None], pa.null()),
                    "y": pa.array([1, 2, 3], pa.int64()),
                }
            ),
            tag_columns=["k"],
        ),
        lambda: PolarsFilter([(pl.col("x") + 1).is_null()]),
    ),
    # A predicate that selects by position reads the columns in row order.
    "position_predicate": (
        lambda: ArrowTableStream(
            pa.table(
                {
                    "x": pa.array([2, -4], pa.int64()),
                    "k": pa.array([-1, 2], pa.int64()),
                    "z": pa.array([10, 20], pa.int64()),
                    "j": pa.array([5, 6], pa.int64()),
                }
            ),
            tag_columns=["k", "j"],
        ),
        lambda: PolarsFilter([pl.first() > 0]),
    ),
}


@pytest.mark.parametrize("label", list(BARRIER_ORACLE_CASES))
def test_a_barrier_operator_sees_its_input_as_a_job_gives_it(label: str) -> None:
    """The record-batch form is held to what a job's sync path gives an operator: the
    upstream rebuilt from its rows (``_materialize_upstream``: ``null`` as
    ``large_string``, the columns in row order), not the raw stream it came from, in
    every layout: ``to_stream()`` rebuilds what the channel delivered. A
    predicate that reads a ``null`` column's type or the column order follows that
    form (documented behaviour, e5); in a job neither case reaches an operator: a
    source refuses a ``null`` column, and a job refuses an expression that selects
    columns by position."""
    make_stream, make_op = BARRIER_ORACLE_CASES[label]
    stream = make_stream()
    job_sync = _outcome(
        lambda: make_op()(
            StaticOutputOperatorPod._materialize_upstream(  # pyright: ignore[reportPrivateUsage]  # the job's sync input
                list(stream.iter_data()), stream
            )
        )
    )
    # The raw stream gives another outcome: the case tells the two oracles apart.
    assert _outcome(lambda: make_op()(stream)) != job_sync
    for layout in LAYOUTS:
        assert (
            _outcome(lambda: run_on_channels(make_op(), [stream], LAYOUTS[layout]))
            == job_sync
        )


# --- Arrivals from another producer -------------------------------------------------------


def test_batch_takes_arrivals_whose_provenance_flags_differ() -> None:
    """``AddTags`` feeding ``Batch(2)`` row by row: each arriving batch is built from its
    own rows, so a source-info field is nullable in one (a null token) and not in the
    next. ``Batch`` puts them together and gives the sync result."""
    raw = ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2, 3, 4], pa.int64()),
                "x": pa.array([1.5, 2.5, 3.5, 4.5], pa.float64()),
                "_source_x": pa.array(
                    [None, "s::r1::x", "s::r2::x", None], pa.large_string()
                ),
            }
        ),
        tag_columns=["k"],
    )
    expected = R(Batch(batch_size=2)(AddTags({"cohort": "A"})(raw)))
    assert expected is not None
    add, batch = AddTags({"cohort": "A"}), Batch(batch_size=2)

    async def go() -> tuple[StreamProtocol, set[bool]]:
        tagged = add(raw)
        middle = OutputChannel(tagged)
        output = OutputChannel(batch(tagged))
        reader = middle.open_reader()
        _ = await asyncio.wait_for(
            asyncio.gather(
                add.async_execute_batches([await _channel(raw, (1,))], middle),
                batch.async_execute_batches([reader], output),
            ),
            TIMEOUT,
        )
        flags = {
            b.schema.field("_source_x").nullable
            for b in await middle.open_reader().collect()
        }
        return output.result(), flags

    result, flags = asyncio.run(go())
    assert flags == {True, False}  # the arrivals did differ
    assert_same_table(result.as_table(columns=ALL), expected)
    assert result.as_table(columns=ALL).to_pylist() == expected.to_pylist()


# --- The row-built form, F (stage 2, ITL-1360) ---------------------------------------------

SYSTEM_TAG = "_tag_source_id::abc"


def test_the_provenance_columns_have_the_row_built_schema() -> None:
    """A raw stream whose system-tag field carries metadata and whose list-valued
    source info has a non-nullable item named ``member``: the channel result's
    provenance fields are those of the table built from the sync rows."""
    schema = pa.schema(
        [
            pa.field("k", pa.int64()),
            pa.field(SYSTEM_TAG, pa.large_string(), metadata={"m": "1"}),
            pa.field("x", pa.int64()),
            pa.field("z", pa.int64()),
            pa.field("y", pa.int64()),
            pa.field(
                "_source_x",
                pa.large_list(pa.field("member", pa.large_string(), nullable=False)),
            ),
        ]
    )
    raw = ArrowTableStream(
        pa.table(
            [
                pa.array([1, 2], pa.int64()),
                pa.array(["s", "s"], pa.large_string()),
                pa.array([1, 2], pa.int64()),
                pa.array([3, 4], pa.int64()),
                pa.array([5, 6], pa.int64()),
                pa.array([["a"], ["b"]], schema.field("_source_x").type),
            ],
            schema=schema,
        ),
        tag_columns=["k"],
    )
    expected = R(SelectDataColumns(["x", "z"])(raw))
    assert expected is not None
    for layout in ("one_batch", "one_row_per_batch"):
        result = run_on_channels(SelectDataColumns(["x", "z"]), [raw], LAYOUTS[layout])
        assert_same_table(result.as_table(columns=ALL), expected)


def test_a_meta_column_does_not_travel() -> None:
    """A raw stream with a meta column: neither it nor a source-info column of it is
    in the record batches or in the stream rebuilt from them, as in the table built
    from the stream's rows."""
    raw = ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "x": pa.array([1, 2], pa.int64()),
                "z": pa.array([3, 4], pa.int64()),
                "__note": pa.array(["a", "b"], pa.large_string()),
            }
        ),
        tag_columns=["k"],
    )
    expected = R(raw)
    assert expected is not None

    async def go() -> tuple[list[str], StreamProtocol]:
        batches = await raw.open_channel().collect()
        output = OutputChannel(raw)
        for batch in batches:
            await output.send(batch)
        await output.close()
        return batches[0].schema.names, output.result()

    wire, rebuilt = asyncio.run(go())
    assert wire == expected.schema.names
    assert rebuilt.as_table(columns=ALL).schema.names == expected.schema.names


# --- An input closed before its producer completed --------------------------------------


@pytest.mark.parametrize(
    "make_op",
    [lambda: SelectDataColumns(["i8", "s"]), lambda: Batch(batch_size=1)],
    ids=["row_by_row", "batch_by_arrival"],
)
def test_an_input_closed_early_fails_a_streaming_operator(
    make_op: Callable[[], StaticOutputOperatorPod],
) -> None:
    """A streaming loop takes an input reader closed before its producer completed for
    a failure, never for the end of its input: the operator fails its output instead
    of completing it with part of the rows."""
    source = source_a()
    op = make_op()

    async def go() -> None:
        rows = pa.Table.from_batches(await source.open_channel().collect())
        first, second = _cut(rows, (1,))[:2]
        feed = OutputChannel(source, retain=False, buffer_size=4)
        reader = feed.open_reader()
        output = OutputChannel(op(source))
        task = asyncio.ensure_future(op.async_execute_batches([reader], output))
        await feed.send(first)
        await asyncio.sleep(0.01)
        await reader.close()
        await feed.send(second)
        await feed.close()
        with pytest.raises(ChannelClosed, match="closed before its producer"):
            await asyncio.wait_for(task, TIMEOUT)
        with pytest.raises(UpstreamFailedError):
            _ = await output.open_reader().collect()
        with pytest.raises(RuntimeError, match="has no result"):
            _ = output.result()

    asyncio.run(go())


# --- F on the edges of a raw stream's provenance (stage 2, ITL-1360) ----------------------


def _job_sync(op: StaticOutputOperatorPod, stream: StreamProtocol) -> StreamProtocol:
    """``op`` over ``stream`` as a job's sync path gives it its input."""
    return op(
        StaticOutputOperatorPod._materialize_upstream(  # pyright: ignore[reportPrivateUsage]  # the job's sync input
            list(stream.iter_data()), stream
        )
    )


def _schema_outcome(make: Callable[[], StreamProtocol]) -> tuple[str, Any]:
    """The schema and rows of a stream's table, or the kind of error that refuses it."""
    try:
        table = make().as_table(columns=ALL)
        return ("table", (table.schema, table.to_pylist()))
    except Exception as error:  # noqa: BLE001  # the refusal is the outcome compared
        return ("refused", type(error).__name__)


@pytest.mark.parametrize("layout", ["one_batch", "one_row_per_batch"])
def test_empty_source_info_lists_are_typed_from_their_tokens(layout: str) -> None:
    """A raw stream whose source-info column is a nested list type holding only empty
    lists: the channel's result equals the job's sync path, both typing the tokens from
    their values over the complete table (``_type_source_info_by_values``), as the rows
    do: one list level, not the column's two."""
    raw = ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "x": pa.array([1, 2], pa.int64()),
                "z": pa.array([3, 4], pa.int64()),
                "_source_x": pa.array(
                    [[], []], pa.large_list(pa.large_list(pa.large_string()))
                ),
            }
        ),
        tag_columns=["k"],
    )
    channel = run_on_channels(SelectDataColumns(["x"]), [raw], LAYOUTS[layout])
    job_sync = _job_sync(SelectDataColumns(["x"]), raw)
    got = channel.as_table(columns=ALL)
    rebuilt = job_sync.as_table(columns=ALL)
    assert got.to_pylist() == rebuilt.to_pylist()
    assert got.schema.equals(rebuilt.schema)
    assert got.schema.field("_source_x").type == pa.large_list(pa.large_string())


def test_a_system_tag_with_a_null_is_refused_by_the_job_sync_path() -> None:
    """Pins a stream no job can run: a raw stream whose system-tag column holds a string
    and a null. The job's sync path refuses it (the rows' system-tag fields differ in
    their flags), while the channel gives both rows; a job never sees such a stream,
    because a source with a nullable system tag cannot be bound to a slot
    (``SourceSpecMismatchError``)."""
    raw = ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                SYSTEM_TAG: pa.array(["s", None], pa.large_string()),
                "x": pa.array([1, 2], pa.int64()),
                "z": pa.array([3, 4], pa.int64()),
            }
        ),
        tag_columns=["k"],
    )
    assert _schema_outcome(lambda: _job_sync(SelectDataColumns(["x"]), raw)) == (
        "refused",
        "ArrowInvalid",
    )
    channel = run_on_channels(SelectDataColumns(["x"]), [raw], None)
    assert channel.as_table(columns=ALL).column(SYSTEM_TAG).to_pylist() == ["s", None]


# --- Ending the output and closing the inputs ------------------------------------------------


class _FailingClose(OutputChannel):
    """An output channel whose ``close`` fails (an implementation that flushes)."""

    async def close(self) -> None:
        raise OSError("flush failed")


def test_a_failing_close_of_the_output_fails_it() -> None:
    """An operator whose ``output.close()`` raises fails the output, so its readers
    never wait for an end that does not come."""
    source = source_a()
    op = SelectDataColumns(["i8", "s"])

    async def go() -> None:
        output = _FailingClose(op(source))
        results = output.open_reader()
        with pytest.raises(OSError, match="flush failed"):
            await op.async_execute_batches([source.open_channel()], output)
        with pytest.raises(UpstreamFailedError):
            async with asyncio.timeout(TIMEOUT):
                _ = await results.collect()

    asyncio.run(go())


def test_every_input_is_closed_when_one_close_fails() -> None:
    """A barrier operator over two inputs whose first close raises: the second input
    is still closed, and the close error is raised once both are closed."""
    left, right = source_a(), source_a()
    op = operators.SemiJoin()

    async def go() -> None:
        first, second = left.open_channel(), right.open_channel()

        async def failing_close() -> None:
            raise OSError("cannot close")

        first.close = failing_close  # a failing input
        output = OutputChannel(op(left, right))
        with pytest.raises(OSError, match="cannot close"):
            await op.async_execute_batches([first, second], output)
        with pytest.raises(ChannelClosed):
            _ = await second.receive()  # closed
        assert output.result().as_table().num_rows > 0  # the output completed first

    asyncio.run(go())


# --- A prediction that once refused a valid result (ITL-1365, fixed on main) --------------------


def test_drop_tag_columns_over_three_tags_sends_its_result() -> None:
    """Tags c, b, a with data-tags a and b; ``DropTagColumns(["a"])``: the channel
    result is the table built from the sync rows. The operator predicts its tags in
    another order than its result has them (ITL-1365); the send check compares
    columns by name, so the order-only difference does not refuse the result."""
    stream = ArrowTableStream(
        pa.table(
            {
                "a": pa.array([1, 2], pa.int64()),
                "b": pa.array([3, 4], pa.int64()),
                "c": pa.array([7, 8], pa.int64()),
                "x": pa.array([5.0, 6.0], pa.float64()),
            }
        ),
        tag_columns=["c", "b", "a"],
        data_tags=["a", "b"],
    )
    expected = R(operators.DropTagColumns(["a"])(stream))
    assert expected is not None
    result = run_on_channels(operators.DropTagColumns(["a"]), [stream], None)
    assert_same_table(result.as_table(columns=ALL), expected)


# --- A barrier over a job's own nodes ------------------------------------------------------


def test_rejoin_over_job_nodes_with_rows_of_two_data_contexts(
    other_context: contexts.DataContext,  # noqa: F811  # the fixture, by name
) -> None:
    """``Rejoin`` of a job's source node with a renaming branch held from the run, rows
    in two data contexts: reading the source node's channel rebuilds its rows in the
    row-built form (never its raw table with its own context column), so the result
    has both rows, as the sync run does."""
    table = pa.table(
        {
            "k": pa.array([1, 2], pa.int64()),
            "x": pa.array([1.0, 2.0], pa.float64()),
            "y": pa.array([3, 4], pa.int64()),
            "source_id::a": pa.array(["s", "s"], pa.large_string()),
            "record_id::a": pa.array(["r0", "r1"], pa.large_string()),
            constants.CONTEXT_KEY: pa.array(
                [contexts.get_default_context().context_key, other_context.context_key],
                pa.large_string(),
            ),
        }
    )
    table = table.cast(
        pa.schema(
            [
                f.with_nullable(False) if f.name.endswith("::a") else f
                for f in table.schema
            ]
        )
    )
    raw = ArrowTableStream(
        table, tag_columns=["k"], system_tag_columns=["source_id::a", "record_id::a"]
    )
    job = PipelineJob(name="rejoin-contexts", database=InMemoryArrowDatabase())
    with job:
        branch = operators.MapData({"x": "u", "y": "v"})(raw, label="branch")
        _ = operators.Rejoin()(raw, branch, label="rejoin")
    _ = job.run(orchestrator=SyncPipelineOrchestrator())
    rejoin = job.nodes["rejoin"]
    assert rejoin.as_table().num_rows == 2
    upstreams = rejoin.upstreams

    async def go() -> StreamProtocol:
        op = operators.Rejoin()
        output = OutputChannel(op(*upstreams))
        await op.async_execute_batches([u.open_channel() for u in upstreams], output)
        return output.result()

    result = asyncio.run(go())
    assert result.as_table().num_rows == 2
    assert sorted(map(str, result.as_table().to_pylist())) == sorted(
        map(str, rejoin.as_table().to_pylist())
    )
