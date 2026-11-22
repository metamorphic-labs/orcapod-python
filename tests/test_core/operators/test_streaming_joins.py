"""Streaming join kernels give the sync join's rows, whatever the batches and arrivals.

ITL-1362 (ITL-1347, stage 5): ``Join``, ``MergeJoin`` and ``SemiJoin`` run on the async
path as symmetric hash joins over Arrow record batches (``core/operators/streaming_joins``).
Row order is not a result on the async path (developer, November 22, 2026), so a kernel
must give the rows of the operator's sync ``static_process`` over the same inputs as a
multiset, with the same columns and Arrow types.

The oracle is the sync operator over its inputs as a job's edges carry them: each input
rebuilt from its rows (``StaticOutputOperatorPod._materialize_upstream``, the row-built
form F of the ITL-1347 spec, where a ``null`` column is ``large_string``). These tests
feed each kernel those inputs' transport tables cut into batches of random sizes (empty
and one-row batches included), in random interleavings of the inputs and of their ends,
and compare.

Field flags are compared as the stream built from the batches settles them: the
system-tag and source-info fields by their values over the complete table (the last step
of F, ``_flag_by_values``), on both sides, rows or none.
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
    _flag_by_values,  # pyright: ignore[reportPrivateUsage]  # F's last step
    StreamingJoin,
    StreamingJoinKernel,
    StreamingMergeJoin,
    StreamingSemiJoin,
)
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants

from .several_items import MAIN, MIXED, P, PLAIN, Q, R, DATA_TAGS, Arrangement
from .several_items import TAGS_FIRST, make_source

SEEDS = range(12)
# The fields whose flags a complete table decides by its values.
SETTLED_PREFIXES = (constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX)
# Batch sizes drawn for the cuts: empty and one-row batches included.
SIZES = (0, 1, 1, 2, 3, 5, 64)


@pytest.fixture(autouse=True)
def _quiet_provisional() -> Any:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


def _transport(stream: StreamProtocol) -> pa.Table:
    """A stream's transport columns."""
    return stream.as_table(columns=TRANSPORT_COLUMNS)


def _row_built(stream: StreamProtocol, *, zero_rows: bool = False) -> StreamProtocol:
    """``stream`` as an operator in a job reads it: rebuilt from its rows (form F)."""
    return StaticOutputOperatorPod._materialize_upstream(  # pyright: ignore[reportPrivateUsage]  # the oracle
        list(stream.iter_data()), stream, zero_rows=zero_rows
    )


def _sent(stream: StreamProtocol) -> pa.Table:
    """An input's rows as the channel layer sends them: the row-built transport table."""
    return _transport(_row_built(stream))


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


def assert_same_fields(got: pa.Table, want: pa.Table) -> None:
    """Same columns, types, field metadata and flags, after settling both (F's last step)."""
    assert sorted(got.column_names) == sorted(want.column_names)
    got = _flag_by_values(got.select(want.column_names))
    want = _flag_by_values(want)
    for field in want.schema:
        other = got.schema.field(field.name)
        assert other.type == field.type, field.name
        assert other.metadata == field.metadata, field.name
        assert other.nullable == field.nullable, field.name


def assert_same_rows(
    batches: Sequence[pa.RecordBatch], kernel: StreamingJoinKernel, expected: pa.Table
) -> None:
    """The emitted batches equal ``expected`` as a multiset, with its columns and types.

    With no batch, the kernel's own zero-row result stands for its output.
    """
    for batch in batches:
        assert batch.schema.names == list(kernel.output_columns)
        # One schema for every batch of the run; the fields a complete table decides
        # by its values are declared nullable (``_flag_by_values`` settles them).
        assert batch.schema == batches[0].schema
        for field in batch.schema:
            if field.name.startswith(SETTLED_PREFIXES):
                assert field.nullable, field.name
    got = (
        pa.Table.from_batches(batches)
        if batches
        else _transport(kernel.zero_row_result).select(list(kernel.output_columns))
    )
    assert_same_fields(got, expected)
    assert _rows(got.select(expected.column_names)) == _rows(expected)


def _check(
    make_kernel: Callable[[Sequence[StreamProtocol]], StreamingJoinKernel],
    sync: Callable[..., StreamProtocol],
    inputs: Sequence[StreamProtocol],
    seeds: Sequence[int] = SEEDS,
) -> None:
    rebuilt = [_row_built(stream) for stream in inputs]
    expected = _transport(sync(*rebuilt))
    # The kernel's zero-row result is the sync operator's over the inputs rebuilt from
    # their rows with none kept.
    zero_rows = _transport(
        sync(*(_row_built(stream, zero_rows=True) for stream in inputs))
    )
    tables = [_transport(stream) for stream in rebuilt]
    for seed in seeds:
        kernel = make_kernel(inputs)
        assert_same_fields(_transport(kernel.zero_row_result), zero_rows)
        assert (
            list(kernel.output_columns)
            == _transport(kernel.zero_row_result).column_names
        )
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
    "decimal256": (
        pa.decimal256(40, 2),
        [Decimal("1.10"), Decimal("-2.00"), None, Decimal("1.10")],
    ),
    # Top-level nulls: a null list, struct or decimal256 key matches nothing, as on the
    # sync path (a single-key Polars join would match two of them).
    "struct": (
        pa.struct([("a", pa.int64()), ("b", pa.large_string())]),
        [{"a": 1, "b": "x"}, {"a": 1, "b": "y"}, {"a": 2, "b": "x"}, None],
    ),
    "list": (pa.large_list(pa.int64()), [[1, 2], [2], [], [1, 2], None]),
}
# The pools whose keys may hold a top-level null.
NULL_POOLS = ("decimal", "decimal256", "list", "nullable_int", "struct")


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


@pytest.mark.parametrize("shared", ["one key", "two keys"])
@pytest.mark.parametrize("pool", sorted(KEY_POOLS))
def test_join_two_inputs_every_key_type(pool: str, shared: str) -> None:
    left = _keyed_source(
        "a", pool, 12, {"x": pa.int64(), "x2": pa.float64()}, 1, extra_tag="g"
    )
    right = _keyed_source(
        "b",
        pool,
        10,
        {"y": pa.int64(), "y2": pa.large_string()},
        2,
        extra_tag="g" if shared == "two keys" else None,
    )
    for inputs in ((left, right), (right, left)):
        _check(_join_kernel, Join().static_process, inputs)


@pytest.mark.parametrize("pool", NULL_POOLS)
def test_join_three_inputs_with_null_keys(pool: str) -> None:
    sources = [
        _keyed_source(name, pool, 10, {f"x{name}": pa.int64()}, seed)
        for seed, name in enumerate("abc")
    ]
    for order in itertools.permutations(range(3)):
        _check(
            _join_kernel,
            Join().static_process,
            [sources[i] for i in order],
            seeds=range(3),
        )


@pytest.mark.parametrize(
    ("arrow_type", "values"),
    [
        (pa.large_list(pa.int64()), [None, [1]]),
        (pa.struct([("a", pa.int64())]), [None, {"a": 1}]),
        (pa.decimal256(40, 2), [None, Decimal("1.00")]),
    ],
)
@pytest.mark.parametrize(
    ("operator", "kernel"),
    [
        (Join(), StreamingJoin),
        (MergeJoin(), StreamingMergeJoin),
        (SemiJoin(), StreamingSemiJoin),
    ],
)
def test_a_null_key_matches_nothing(
    arrow_type: pa.DataType, values: list[Any], operator: Any, kernel: Any
) -> None:
    # The reviewer's minimal case: two null keys, one shared key, one-row batches.
    def keyed(source_id: str, data: str) -> ArrowTableSource:
        return ArrowTableSource(
            pa.table({"k": pa.array(values, arrow_type), data: [1, 2]}),
            tag_columns=["k"],
            tag_only_columns=True,
            source_id=source_id,
        )

    inputs = (keyed("a", "v"), keyed("b", "w"))
    expected = _transport(operator.static_process(*map(_row_built, inputs)))
    assert expected.num_rows == 1
    _check(lambda i: kernel(operator, i), operator.static_process, inputs)


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


@pytest.mark.parametrize("shared", ["one key", "two keys"])
@pytest.mark.parametrize(
    "pool", ["int", "nullable_int", "float", "struct", "list", "decimal256"]
)
def test_merge_join_colliding_columns_in_both_argument_orders(
    pool: str, shared: str
) -> None:
    left = _keyed_source(
        "a", pool, 10, {"x": pa.int64(), "c": pa.float64()}, 6, extra_tag="g"
    )
    right = _keyed_source(
        "b",
        pool,
        8,
        {"c": pa.float64(), "y": pa.int64()},
        7,
        extra_tag="g" if shared == "two keys" else None,
    )
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


@pytest.mark.parametrize("shared", ["one key", "two keys"])
@pytest.mark.parametrize("pool", sorted(KEY_POOLS))
def test_semi_join_every_key_type(pool: str, shared: str) -> None:
    left = _keyed_source("a", pool, 12, {"x": pa.int64()}, 8, extra_tag="g")
    right = _keyed_source(
        "b",
        pool,
        5,
        {"y": pa.int64()},
        9,
        extra_tag="g" if shared == "two keys" else None,
    )
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


# ---------------------------------------------------------------------------
# Inputs rebuilt from rows: ``null`` columns are ``large_string`` (form F, ITL-1319)
# ---------------------------------------------------------------------------


def _with_null_column(
    name: str, null_type: bool, data: str, *, tag: bool, n: int = 2
) -> ArrowTableStream:
    """A raw stream with column ``name`` all null, typed ``null`` or ``large_string``."""
    column_type = pa.null() if null_type else pa.large_string()
    columns: dict[str, pa.Array[Any]] = {
        "k": pa.array(list(range(n)), pa.int64()),
        name: pa.array([None] * n, column_type),
        data: pa.array(list(range(n)), pa.int64()),
    }
    return ArrowTableStream(
        pa.table(list(columns.values()), names=list(columns)),
        tag_columns=["k", name] if tag else ["k"],
    )


ALL_KERNELS: list[tuple[Any, Any]] = [
    (Join(), StreamingJoin),
    (MergeJoin(), StreamingMergeJoin),
    (SemiJoin(), StreamingSemiJoin),
]


@pytest.mark.parametrize(("operator", "kernel"), ALL_KERNELS)
def test_a_null_typed_tag_meets_a_large_string_tag_as_rows_carry_it(
    operator: Any, kernel: Any
) -> None:
    # Rows carry the ``null`` tag as ``large_string``: the sync path over the inputs
    # rebuilt from rows accepts the pair (all null, so nothing matches), and so does
    # the kernel, built from the descriptions.
    left = _with_null_column("n", True, "v", tag=True)
    right = _with_null_column("n", False, "w", tag=True)
    for inputs in ((left, right), (right, left)):
        _check(lambda i: kernel(operator, i), operator.static_process, inputs)


def test_merge_join_merges_a_null_typed_and_a_large_string_column() -> None:
    left = _with_null_column("x", True, "v", tag=False)
    right = _with_null_column("x", False, "w", tag=False)
    for inputs in ((left, right), (right, left)):
        expected = _transport(MergeJoin().static_process(*map(_row_built, inputs)))
        assert expected.column("x").to_pylist() == [[None, None]] * 2
        _check(_merge_kernel, MergeJoin().static_process, inputs)


@pytest.mark.parametrize(("operator", "kernel"), ALL_KERNELS)
def test_a_zero_row_batch_of_the_zero_row_form_does_not_fix_a_type(
    operator: Any, kernel: Any
) -> None:
    # Both inputs describe ``x`` as ``null`` (a colliding data column for MergeJoin, a
    # shared tag otherwise). An empty batch typed as the zero-row form (``null``) holds
    # no value and is taken; the populated batches after it are in the row-built form
    # (``large_string``) and are taken too.
    tag = not isinstance(operator, MergeJoin)
    left = _with_null_column("x", True, "v", tag=tag)
    right = _with_null_column("x", True, "w", tag=tag)
    for inputs in ((left, right), (right, left)):
        instance = kernel(operator, inputs)
        out: list[pa.RecordBatch] = []
        for index, stream in enumerate(inputs):
            schema = _transport(stream).schema
            empty = pa.record_batch(
                [pa.array([], field.type) for field in schema], schema=schema
            )
            assert pa.types.is_null(empty.schema.field("x").type)
            out += instance.push(index, empty)
            out += instance.push(index, _sent(stream).combine_chunks().to_batches()[0])
        for index in range(2):
            out += instance.close(index)
        expected = _transport(operator.static_process(*map(_row_built, inputs)))
        assert_same_rows(out, instance, expected)


def test_a_null_typed_input_that_ends_empty_is_read_in_its_zero_row_form() -> None:
    # With no rows, the sync path reads an input in its zero-row form, where ``null``
    # stays ``null``: a ``null`` key against a ``large_string`` one is refused, while
    # the kernel, which cannot know the input will be empty, refuses at the last close.
    for operator, kernel in ALL_KERNELS:
        left = _with_null_column("n", True, "v", tag=True, n=0)
        right = _with_null_column("n", False, "w", tag=True)
        with pytest.raises(Exception) as sync_error:
            operator.static_process(_row_built(left), _row_built(right))
        instance = kernel(operator, (left, right))
        assert instance.push(1, _sent(right).combine_chunks().to_batches()[0]) == []
        assert instance.close(0) == []
        with pytest.raises(type(sync_error.value)) as kernel_error:
            instance.close(1)
        assert str(kernel_error.value) == str(sync_error.value)
        # Two ``null`` inputs that both end empty are accepted, as on the sync path.
        both = (left, _with_null_column("n", True, "w", tag=True, n=0))
        operator.static_process(*map(_row_built, both))
        instance = kernel(operator, both)
        assert instance.close(0) == []
        assert instance.close(1) == []


# ---------------------------------------------------------------------------
# System-tag flags are settled on the complete table
# ---------------------------------------------------------------------------


def test_merge_join_over_a_join_of_inputs_of_one_pipeline_hash() -> None:
    # A Join of inputs tied on pipeline hash declares its system tags nullable; a table
    # built from its rows flags them by their values. MergeJoin over it, in the kernel,
    # declares them nullable and the complete table settles them as the sync path's.
    def tied(source_id: str, keys: list[int], groups: list[str]) -> ArrowTableSource:
        return ArrowTableSource(
            pa.table(
                {
                    "k": pa.array(keys, pa.int64()),
                    "g": pa.array(groups, pa.large_string()),
                }
            ),
            tag_columns=["k", "g"],
            source_id=source_id,
        )

    joined = Join()(
        tied("a", [1, 2, 3], ["x", "y", "x"]),
        tied("b", [2, 1, 3], ["y", "x", "x"]),
        tied("c", [3, 2, 1], ["x", "y", "x"]),
    )
    other = _keyed_source("o", "int", 6, {"z": pa.int64()}, 4)
    for inputs in ((joined, other), (other, joined)):
        _check(_merge_kernel, MergeJoin().static_process, inputs)
        _check(_join_kernel, Join().static_process, inputs, seeds=range(4))


# ---------------------------------------------------------------------------
# A refusal that depends on values is raised when its batch arrives
# ---------------------------------------------------------------------------


def _data_tag_stream(data: str, tokens: list[list[str]]) -> ArrowTableStream:
    """A stream with data-tag ``k`` whose source info is a token list per row."""
    token_list = pa.large_list(pa.large_string())
    return ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                data: pa.array([1, 2], pa.int64()),
                "_source_k": pa.array(tokens, token_list),
                f"_source_{data}": pa.array([["s"], ["t"]], token_list),
            }
        ),
        tag_columns=["k"],
        data_tags=["k"],
    )


@pytest.mark.parametrize(
    ("operator", "kernel"), [(Join(), StreamingJoin), (MergeJoin(), StreamingMergeJoin)]
)
def test_source_info_lists_of_different_lengths_raise_when_their_batch_arrives(
    operator: Any, kernel: Any
) -> None:
    # The descriptions agree (one type of token list); the lengths are values. The
    # sync path refuses the run; the kernel raises the same error when the offending
    # pair arrives, after the rows already decided (module docstring).
    left = _data_tag_stream("v", [["a"], ["b"]])
    right = _data_tag_stream("w", [["c"], ["d", "e"]])
    with pytest.raises(ValueError, match="different lengths") as sync_error:
        operator.static_process(_row_built(left), _row_built(right))
    instance = kernel(operator, (left, right))
    instance.push(0, _sent(left).combine_chunks().to_batches()[0])
    sent = _sent(right).combine_chunks()
    emitted = instance.push(1, sent.slice(0, 1).to_batches()[0])
    assert sum(batch.num_rows for batch in emitted) == 1
    with pytest.raises(ValueError, match="different lengths") as kernel_error:
        instance.push(1, sent.slice(1, 1).to_batches()[0])
    assert str(kernel_error.value) == str(sync_error.value)


def test_join_releases_every_step_once_an_input_ends_empty() -> None:
    # Three inputs: when the first in canonical order ends with no row, no step can
    # emit again, so the rows of the other inputs are not held while they are open.
    sources = [make_source(MAIN), make_source(P), make_source(Q)]
    order = sorted(range(3), key=lambda i: sources[i].pipeline_hash().to_hex())
    for first in (order[0], order[1]):
        kernel = StreamingJoin(Join(), sources)
        assert kernel.close(first) == []
        for index in range(3):
            if index != first:
                kernel.push(
                    index, _sent(sources[index]).combine_chunks().to_batches()[0]
                )
        assert kernel.held_rows == 0
        rest = [index for index in range(3) if index != first]
        assert kernel.close(rest[0]) == []
        assert kernel.close(rest[1]) == []


def test_join_releases_the_steps_before_one_that_cannot_emit() -> None:
    # The last input in canonical order ends empty: the earlier steps' rows could
    # only reach that step, so none is held.
    sources = [make_source(MAIN), make_source(P), make_source(Q)]
    order = sorted(range(3), key=lambda i: sources[i].pipeline_hash().to_hex())
    kernel = StreamingJoin(Join(), sources)
    assert kernel.close(order[2]) == []
    for index in order[:2]:
        kernel.push(index, _sent(sources[index]).combine_chunks().to_batches()[0])
    assert kernel.held_rows == 0


@pytest.mark.parametrize("pool", NULL_POOLS)
def test_semi_join_never_holds_a_left_row_with_a_null_key(pool: str) -> None:
    left = _keyed_source("a", pool, 30, {"x": pa.int64()}, 11)
    # The same seed: a right row for every left key, the null ones included.
    right = _keyed_source("b", pool, 30, {"y": pa.int64()}, 11)
    left_rows = _sent(left).combine_chunks()
    null_keys = left_rows.column("k").null_count
    assert null_keys > 0
    kernel = StreamingSemiJoin(SemiJoin(), (left, right))
    assert kernel.push(0, left_rows.to_batches()[0]) == []
    assert kernel.held_rows == left_rows.num_rows - null_keys
    # The left input ends and the right rows arrive: every left row with a key is
    # emitted, and nothing is held.
    kernel.close(0)
    emitted = kernel.push(1, _sent(right).combine_chunks().to_batches()[0])
    assert sum(batch.num_rows for batch in emitted) == left_rows.num_rows - null_keys
    assert kernel.held_rows == 0
    assert kernel.close(1) == []
