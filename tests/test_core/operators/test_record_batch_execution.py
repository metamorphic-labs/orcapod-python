"""Operators over channels of Arrow record batches equal the sync path (ITL-1347, L6).

Every operator has one body, ``static_process``. Its Arrow-record-batch form
(``async_execute_batches``) reads input channels opened from streams and writes an
output channel, which closes into a stream. An operator whose output rows each come
from one input row is applied to every record batch as it arrives; every other
operator reads all of its inputs first (``InputChannel.to_stream``) and then runs the
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
from collections.abc import Sequence
from typing import Any

import pyarrow as pa
import pytest

import orcapod.core.operators as operators
from orcapod.channels import InputChannel, OutputChannel
from orcapod.core.operators import Batch
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
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
) -> InputChannel:
    """An input channel delivering ``stream``'s rows in record batches of ``sizes``."""
    batches = await stream.open_channel().collect()
    if sizes is None or not batches:
        return stream.open_channel()
    rows = pa.Table.from_batches(batches)
    feed = OutputChannel(stream)
    for batch in _cut(rows, sizes):
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
    assert result.output_schema(columns=ALL) == sync.output_schema(columns=ALL)
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
    """One result record batch per input record batch, before the next is read: with a
    queue of one batch and no retained output, the operator can only go on when its
    reader has taken the batch before."""
    (source,) = case.make_sources()
    op = case.make_op()
    expected = R(case.make_op()(source))
    assert expected is not None

    async def go() -> tuple[list[int], pa.Table]:
        feed = OutputChannel(source, retain=False, buffer_size=1)
        reader = feed.open_reader()
        output = OutputChannel(op(source), retain=False, buffer_size=1)
        results = output.open_reader()
        task = asyncio.ensure_future(op.async_execute_batches([reader], output))
        rows = pa.Table.from_batches(await source.open_channel().collect())
        received: list[pa.RecordBatch] = []
        sizes: list[int] = []
        for batch in _cut(rows, (1,)):
            await asyncio.wait_for(feed.send(batch), TIMEOUT)
            await asyncio.sleep(0.001)
            sizes.append(len(received))
            # Whatever this input row gave is there before the next row is sent.
            while output._sent > len(received):  # pyright: ignore[reportPrivateUsage]
                received.append(await asyncio.wait_for(results.receive(), TIMEOUT))
        await feed.close()
        received.extend(await asyncio.wait_for(results.collect(), TIMEOUT))
        await task
        return sizes, pa.Table.from_batches(received) if received else rows.slice(0, 0)

    _, table = asyncio.run(go())
    assert table.num_rows == expected.num_rows
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
        first = await asyncio.wait_for(results.receive(), TIMEOUT)
        assert not task.done()
        await feed.send(one_row[2])
        await feed.close()
        rest = await asyncio.wait_for(results.collect(), TIMEOUT)
        await task
        return first.num_rows, sum(batch.num_rows for batch in rest)

    assert asyncio.run(go()) == (1, 1)
