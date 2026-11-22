"""Streaming join kernels give the sync join's rows, whatever the batches and arrivals.

ITL-1362 (ITL-1347, stage 5): ``Join``, ``MergeJoin`` and ``SemiJoin`` run on the async
path as symmetric hash joins over Arrow record batches (``core/operators/streaming_joins``).
Row order is not a result on the async path (developer, November 22, 2026), so a kernel
must give the rows of the operator's sync ``static_process`` over the same inputs as a
multiset, with the same columns and Arrow types. These tests feed each kernel its inputs'
transport tables cut into batches of random sizes (empty and one-row batches included),
in random interleavings of the inputs and of their ends, and compare.

Field flags are compared as the stream built from the batches settles them: every flag
is the sync result's, except source info, which a complete table decides (step 5 of the
spec's row-built form F, ``_flag_source_info_nullability``) and is compared after it.
"""

from __future__ import annotations

import itertools
import random
import struct
import warnings
from collections import Counter
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.operators import Join, MergeJoin, SemiJoin
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.operators.streaming_joins import (
    TRANSPORT_COLUMNS,
    StreamingJoin,
    StreamingJoinKernel,
    StreamingMergeJoin,
    StreamingSemiJoin,
)
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol

from .several_items import MAIN, MIXED, P, PLAIN, Q, R, DATA_TAGS, Arrangement
from .several_items import TAGS_FIRST, make_source

SEEDS = range(12)
# Batch sizes drawn for the cuts: empty and one-row batches included.
SIZES = (0, 1, 1, 2, 3, 5, 64)


@pytest.fixture(autouse=True)
def _quiet_provisional() -> Any:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


def _transport(stream: StreamProtocol) -> pa.Table:
    """An input's rows as the channel layer sends them: its transport columns."""
    return stream.as_table(columns=TRANSPORT_COLUMNS)


def _cut(table: pa.Table, rng: random.Random) -> list[pa.RecordBatch]:
    """``table`` as record batches of random sizes, in order."""
    batches: list[pa.RecordBatch] = []
    start = 0
    while start < table.num_rows:
        size = rng.choice(SIZES)
        if size == 0:
            batches.append(
                pa.record_batch(
                    [pa.array([], field.type) for field in table.schema],
                    schema=table.schema,
                )
            )
        else:
            batches.extend(table.slice(start, size).combine_chunks().to_batches())
        start += size
    return batches


def _arrivals(
    tables: Sequence[pa.Table], rng: random.Random
) -> list[tuple[int, pa.RecordBatch | None]]:
    """A random interleaving of every input's batches, each input closing after its last.

    ``None`` stands for an input's end.
    """
    queues = [[*_cut(table, rng), None] for table in tables]
    events: list[tuple[int, pa.RecordBatch | None]] = []
    while any(queues):
        index = rng.choice([i for i, queue in enumerate(queues) if queue])
        events.append((index, queues[index].pop(0)))
    return events


def _run(
    kernel: StreamingJoinKernel, events: Sequence[tuple[int, pa.RecordBatch | None]]
) -> list[pa.RecordBatch]:
    out: list[pa.RecordBatch] = []
    for index, batch in events:
        out.extend(kernel.close(index) if batch is None else kernel.push(index, batch))
    return out


def _value(value: Any) -> Any:
    """A value in a form that compares bit for bit (floats by their bits)."""
    if isinstance(value, float):
        return ("float", struct.pack("<d", value).hex())
    if isinstance(value, list):
        return tuple(_value(item) for item in value)
    if isinstance(value, dict):
        return tuple((key, _value(item)) for key, item in value.items())
    return value


def _rows(table: pa.Table) -> Counter[tuple[Any, ...]]:
    names = sorted(table.column_names)
    return Counter(
        tuple(_value(row[name]) for name in names) for row in table.to_pylist()
    )


def _settled(table: pa.Table) -> pa.Table:
    return StaticOutputOperatorPod._flag_source_info_nullability(table)  # pyright: ignore[reportPrivateUsage]


def assert_same_rows(
    batches: Sequence[pa.RecordBatch], kernel: StreamingJoinKernel, expected: pa.Table
) -> None:
    """The emitted batches equal ``expected`` as a multiset, with its columns and types."""
    for batch in batches:
        assert batch.schema.names == list(kernel.output_columns)
    got = (
        pa.Table.from_batches(batches)
        if batches
        else expected.slice(0, 0).select(list(kernel.output_columns))
    )
    assert sorted(got.column_names) == sorted(expected.column_names)
    got = _settled(got.select(expected.column_names))
    want = _settled(expected)
    if got.num_rows or not want.num_rows:
        for field in want.schema:
            other = got.schema.field(field.name)
            assert other.type == field.type, field.name
            assert other.metadata == field.metadata, field.name
            if got.num_rows:
                assert other.nullable == field.nullable, field.name
    assert _rows(got) == _rows(want)


def _check(
    make_kernel: Callable[[Sequence[StreamProtocol]], StreamingJoinKernel],
    sync: Callable[..., StreamProtocol],
    inputs: Sequence[StreamProtocol],
    seeds: Sequence[int] = SEEDS,
) -> None:
    expected = _transport(sync(*inputs))
    tables = [_transport(stream) for stream in inputs]
    for seed in seeds:
        kernel = make_kernel(inputs)
        batches = _run(kernel, _arrivals(tables, random.Random(seed)))
        assert_same_rows(batches, kernel, expected)
        assert kernel.held_rows == 0


# ---------------------------------------------------------------------------
# Inputs: keys of several types, with repeats, nulls, NaN and signed zeros
# ---------------------------------------------------------------------------

NAN = float("nan")
KEY_POOLS: dict[str, tuple[pa.DataType, list[Any]]] = {
    "int": (pa.int64(), [1, 2, 3, 2]),
    "nullable_int": (pa.int64(), [1, None, 2, None]),
    "string": (pa.large_string(), ["a", "b", "a", "c"]),
    "float": (pa.float64(), [0.0, -0.0, NAN, 1.5]),
    "decimal": (pa.decimal128(10, 2), [Decimal("1.10"), Decimal("2.00"), None]),
    "timestamp_tz": (
        pa.timestamp("us", tz="UTC"),
        [
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        ],
    ),
    "struct": (
        pa.struct([("a", pa.int64()), ("b", pa.large_string())]),
        [{"a": 1, "b": "x"}, {"a": 1, "b": "y"}, {"a": 2, "b": "x"}],
    ),
    "list": (pa.large_list(pa.int64()), [[1, 2], [2], [], [1, 2]]),
}


def _keyed_source(
    source_id: str,
    pool: str,
    n: int,
    data: dict[str, pa.DataType],
    seed: int,
    *,
    extra_tag: str | None = None,
) -> ArrowTableSource:
    """A source with tag ``k`` drawn from ``KEY_POOLS[pool]`` and the given data columns."""
    rng = random.Random(seed)
    arrow_type, values = KEY_POOLS[pool]
    columns: dict[str, pa.Array[Any]] = {
        "k": pa.array([rng.choice(values) for _ in range(n)], arrow_type)
    }
    tags = ["k"]
    if extra_tag is not None:
        columns[extra_tag] = pa.array(
            [rng.choice(["u", "v"]) for _ in range(n)], pa.large_string()
        )
        tags.append(extra_tag)
    for name, data_type in data.items():
        if pa.types.is_floating(data_type):
            column = pa.array([rng.random() for _ in range(n)], data_type)
        elif pa.types.is_integer(data_type):
            column = pa.array(list(range(n)), data_type)
        else:
            column = pa.array([f"{name}{i}" for i in range(n)], data_type)
        columns[name] = column
    schema = pa.schema(
        pa.field(name, column.type, nullable=column.null_count > 0)
        for name, column in columns.items()
    )
    return ArrowTableSource(
        pa.table(list(columns.values()), schema=schema),
        tag_columns=tags,
        tag_only_columns=True,
        source_id=source_id,
    )


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------


def _join_kernel(inputs: Sequence[StreamProtocol]) -> StreamingJoinKernel:
    return StreamingJoin(Join(), inputs)


@pytest.mark.parametrize("pool", sorted(KEY_POOLS))
def test_join_two_inputs_every_key_type(pool: str) -> None:
    left = _keyed_source(
        "a", pool, 9, {"x": pa.int64(), "x2": pa.float64()}, 1, extra_tag="g"
    )
    right = _keyed_source("b", pool, 7, {"y": pa.int64(), "y2": pa.large_string()}, 2)
    for inputs in ((left, right), (right, left)):
        _check(_join_kernel, Join().static_process, inputs)


@pytest.mark.parametrize("order", list(itertools.permutations(range(3))))
def test_join_three_inputs_every_argument_order(order: tuple[int, ...]) -> None:
    sources = [
        make_source(MAIN),
        make_source(P),
        make_source(Q),
    ]
    inputs = [sources[i] for i in order]
    _check(_join_kernel, Join().static_process, inputs, seeds=range(6))


@pytest.mark.parametrize("roles", [PLAIN, DATA_TAGS, MIXED])
def test_join_four_inputs_in_each_tag_role(roles: str) -> None:
    # Data-tags shared by several inputs keep their members' tokens pending through
    # the chain and fold them when the last step emits (``join_v1``).
    sources = [
        make_source(spec, Arrangement(spec.tags, TAGS_FIRST, roles))
        for spec in (MAIN, P, Q, R)
    ]
    for inputs in (sources, list(reversed(sources))):
        _check(_join_kernel, Join().static_process, inputs, seeds=range(6))


def test_join_inputs_sharing_no_tag_give_every_pair() -> None:
    left = _keyed_source("a", "int", 5, {"x": pa.int64()}, 3)
    right = ArrowTableSource(
        pa.table({"h": ["p", "q", "r"], "y": [1.0, 2.0, 3.0]}),
        tag_columns=["h"],
        tag_only_columns=True,
        source_id="h",
    )
    _check(_join_kernel, Join().static_process, (left, right))


def test_join_inputs_of_one_pipeline_hash_sort_their_system_tags() -> None:
    # One schema, so one pipeline hash: the system-tag values are sorted per row. Two
    # inputs of one schema share their data columns, so only data-tags (merged by the
    # join, their tokens folded) can be joined.
    def keys_only(source_id: str, values: list[str]) -> ArrowTableSource:
        return ArrowTableSource(
            pa.table({"k": pa.array(values, pa.large_string())}),
            tag_columns=["k"],
            source_id=source_id,
        )

    first = keys_only("a", ["a", "b", "a", "c", "b"])
    second = keys_only("b", ["b", "a", "b", "d"])
    assert first.pipeline_hash() == second.pipeline_hash()
    for inputs in ((first, second), (second, first)):
        _check(_join_kernel, Join().static_process, inputs)


def test_join_of_one_input_passes_its_rows() -> None:
    _check(_join_kernel, Join().static_process, (make_source(MAIN),))


# ---------------------------------------------------------------------------
# MergeJoin
# ---------------------------------------------------------------------------


def _merge_kernel(inputs: Sequence[StreamProtocol]) -> StreamingJoinKernel:
    return StreamingMergeJoin(MergeJoin(), inputs)


@pytest.mark.parametrize("pool", ["int", "nullable_int", "float", "struct", "list"])
def test_merge_join_colliding_columns_in_both_argument_orders(pool: str) -> None:
    left = _keyed_source(
        "a", pool, 8, {"x": pa.int64(), "c": pa.float64()}, 6, extra_tag="g"
    )
    right = _keyed_source("b", pool, 6, {"c": pa.float64(), "y": pa.int64()}, 7)
    for inputs in ((left, right), (right, left)):
        _check(_merge_kernel, MergeJoin().static_process, inputs)


@pytest.mark.parametrize("roles", [PLAIN, DATA_TAGS, MIXED])
def test_merge_join_in_each_tag_role(roles: str) -> None:
    main = make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, roles))
    partner = make_source(P, Arrangement(P.tags, TAGS_FIRST, roles))
    for inputs in ((main, partner), (partner, main)):
        _check(_merge_kernel, MergeJoin().static_process, inputs)


# ---------------------------------------------------------------------------
# SemiJoin
# ---------------------------------------------------------------------------


def _semi_kernel(inputs: Sequence[StreamProtocol]) -> StreamingJoinKernel:
    return StreamingSemiJoin(SemiJoin(), inputs)


@pytest.mark.parametrize("pool", sorted(KEY_POOLS))
def test_semi_join_every_key_type(pool: str) -> None:
    left = _keyed_source("a", pool, 9, {"x": pa.int64()}, 8, extra_tag="g")
    right = _keyed_source("b", pool, 4, {"y": pa.int64()}, 9)
    _check(_semi_kernel, SemiJoin().static_process, (left, right))


@pytest.mark.parametrize("roles", [PLAIN, DATA_TAGS, MIXED])
def test_semi_join_in_each_tag_role(roles: str) -> None:
    main = make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, roles))
    for partner_spec in (P, Q):
        partner = make_source(partner_spec, rows=[0, 2])
        _check(_semi_kernel, SemiJoin().static_process, (main, partner))


def test_semi_join_without_a_shared_tag_passes_every_left_row() -> None:
    left = make_source(MAIN)
    right = ArrowTableSource(
        pa.table({"h": ["p"], "y": [1.0]}),
        tag_columns=["h"],
        tag_only_columns=True,
        source_id="h",
    )
    _check(_semi_kernel, SemiJoin().static_process, (left, right))


# ---------------------------------------------------------------------------
# Checks before the first batch (e1, ITL-856)
# ---------------------------------------------------------------------------


def _typed(source_id: str, key_type: pa.DataType, n: int) -> ArrowTableStream:
    table = pa.table(
        {"k": pa.array(list(range(n)), key_type), source_id: pa.array(list(range(n)))}
    )
    return ArrowTableStream(table, tag_columns=["k"])


@pytest.mark.parametrize(
    ("operator", "kernel"),
    [
        (Join(), StreamingJoin),
        (MergeJoin(), StreamingMergeJoin),
        (SemiJoin(), StreamingSemiJoin),
    ],
)
def test_disagreeing_key_types_raise_before_the_first_batch(
    operator: Any, kernel: Any
) -> None:
    # The sync path's error over rows ...
    with pytest.raises(InputValidationError) as sync_error:
        operator.static_process(_typed("x", pa.int16(), 2), _typed("y", pa.int32(), 2))
    # ... is the kernel's, from the descriptions, also when an input has no row.
    with pytest.raises(InputValidationError) as kernel_error:
        kernel(operator, (_typed("x", pa.int16(), 0), _typed("y", pa.int32(), 2)))
    assert str(kernel_error.value) == str(sync_error.value)


def test_a_batch_must_hold_its_inputs_columns_and_types() -> None:
    left, right = make_source(MAIN), make_source(P)
    kernel = StreamingJoin(Join(), (left, right))
    table = _transport(left)
    with pytest.raises(ValueError, match="lacks"):
        kernel.push(0, table.drop_columns(["d1"]).to_batches()[0])
    with pytest.raises(ValueError, match="'d1'"):
        kernel.push(
            0,
            table.set_column(
                table.column_names.index("d1"),
                "d1",
                table.column("d1").cast(pa.int32()),
            ).to_batches()[0],
        )


def test_an_input_closes_once_and_takes_no_batch_after() -> None:
    left, right = make_source(MAIN), make_source(P)
    kernel = StreamingJoin(Join(), (left, right))
    assert kernel.close(0) == []
    with pytest.raises(ValueError, match="closed already"):
        kernel.close(0)
    with pytest.raises(ValueError, match="closed already"):
        kernel.push(0, _transport(left).to_batches()[0])
    with pytest.raises(ValueError, match="no input 2"):
        kernel.push(2, _transport(left).to_batches()[0])


# ---------------------------------------------------------------------------
# What a kernel holds
# ---------------------------------------------------------------------------


def test_join_drops_a_sides_rows_when_the_other_side_closes() -> None:
    left, right = make_source(MAIN), make_source(P)
    kernel = StreamingJoin(Join(), (left, right))
    left_table, right_table = _transport(left), _transport(right)
    kernel.push(0, left_table.to_batches()[0])
    kernel.push(1, right_table.slice(0, 1).to_batches()[0])
    assert kernel.held_rows == left_table.num_rows + 1
    # The right input ends: no left row can match anything new, the right rows stay.
    kernel.close(1)
    assert kernel.held_rows == 1
    # A left row arriving now is matched against the right rows and not kept.
    kernel.push(0, left_table.slice(0, 2).to_batches()[0])
    assert kernel.held_rows == 1
    kernel.close(0)
    assert kernel.held_rows == 0


def test_semi_join_holds_undecided_left_rows_only_until_the_right_input_ends() -> None:
    left, right = make_source(MAIN), make_source(P, rows=[0])
    kernel = StreamingSemiJoin(SemiJoin(), (left, right))
    left_table = _transport(left)
    assert kernel.push(0, left_table.to_batches()[0]) == []
    assert kernel.held_rows == left_table.num_rows
    emitted = kernel.push(1, _transport(right).to_batches()[0])
    matched = sum(batch.num_rows for batch in emitted)
    assert matched > 0
    assert kernel.held_rows == left_table.num_rows - matched + 1
    # The right input ends: the undecided left rows are dropped; the right keys stay
    # for left rows still to arrive.
    kernel.close(1)
    assert kernel.held_rows == 1
    kernel.close(0)
    assert kernel.held_rows == 0


def test_kernels_change_no_sync_result() -> None:
    # A kernel applies the operator only to zero-row inputs; its sync result over rows
    # is the same before and after kernels were made from it.
    inputs = (make_source(MAIN), make_source(P))
    before = _rows(_transport(Join().static_process(*inputs)))
    operator = Join()
    StreamingJoin(operator, inputs)
    assert _rows(_transport(operator.static_process(*inputs))) == before
