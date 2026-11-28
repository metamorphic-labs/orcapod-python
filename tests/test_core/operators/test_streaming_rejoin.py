"""The streaming Rejoin kernel gives the sync Rejoin's rows, within its stated limits.

ITL-1362 (ITL-1347, stage 5): ``StreamingRejoin`` is the streaming form of ``Rejoin`` on
Arrow record batches, not yet wired into ``async_execute`` (which is still the barrier).
It applies ITL-1355's rule: an inner join on every column the inputs share (system
tags, shared user tags, a shared data-tag's source info, the data context), ``Join``'s
equality, a null equal to a null outside the system tags, a null system tag matching
nothing, each row matching at most once. Its rows equal the sync rows as a multiset for
every schedule with unique origins, and with repeated origins for two inputs; with
three or more inputs and repeated origins, arrival order can change the pairing
(``test_repeats_with_three_inputs_can_pair_by_arrival``), and the warning is guaranteed
only for two inputs with unique origins; those documented differences are pinned below.
These are direct tests of the kernel, not of an async execution path. The oracle and the arrivals are those of
``test_streaming_joins``: the sync operator over the inputs rebuilt from rows, compared
as a multiset, after random batch cuts and interleavings.
"""

from __future__ import annotations

import dataclasses
import itertools
import math
import random
import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

from orcapod import contexts
from orcapod.core.operators import Rejoin
from orcapod.core.operators.streaming_joins import (
    StreamingJoinKernel,
    StreamingRejoin,
)
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError, RejoinDifferingColumnWarning
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants

from .test_streaming_joins import (
    _arrivals,  # pyright: ignore[reportPrivateUsage]  # the shared harness
    _rows,  # pyright: ignore[reportPrivateUsage]  # the shared harness
    _check,  # pyright: ignore[reportPrivateUsage]  # the shared harness
    _row_built,  # pyright: ignore[reportPrivateUsage]  # the shared harness
    _run,  # pyright: ignore[reportPrivateUsage]  # the shared harness
    _sent,  # pyright: ignore[reportPrivateUsage]  # the shared harness
    _transport,  # pyright: ignore[reportPrivateUsage]  # the shared harness
    assert_same_rows,
)

# PolarsFilter is built here on purpose; the warning of every construction is tested in
# test_polars_filter_warning.py (ITL-1296).
pytestmark = pytest.mark.filterwarnings("ignore::orcapod.errors.PolarsFilterWarning")


@pytest.fixture(autouse=True)
def _quiet_provisional() -> Any:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


def _kernel(inputs: Sequence[StreamProtocol]) -> StreamingJoinKernel:
    return StreamingRejoin(Rejoin(), inputs)


def _origin(n: int = 12, *, data_tags: bool = False) -> ArrowTableSource:
    """``n`` rows: tags ``subject`` and ``unit``; data ``rate``, ``depth``, ``area``."""
    return ArrowTableSource(
        pa.table(
            {
                "subject": pa.array([f"s{i % 3}" for i in range(n)], pa.large_string()),
                "unit": pa.array(list(range(n)), pa.int64()),
                "rate": pa.array([float(i) for i in range(n)], pa.float64()),
                "depth": pa.array([10 * i for i in range(n)], pa.int64()),
                "area": pa.array(["V1"] * n, pa.large_string()),
            }
        ),
        tag_columns=["subject", "unit"],
        tag_only_columns=False if data_tags else True,
        source_id="units",
    )


def _branches(origin: Any) -> list[Any]:
    """Three branches of one stream, each with its own data column and rows."""
    return [
        origin.select_data_columns(["rate"]),
        origin.polars_filter(pl.col("depth") >= 30).select_data_columns(["depth"]),
        origin.polars_filter(pl.col("rate") < 9.0).select_data_columns(["area"]),
    ]


# ---------------------------------------------------------------------------
# The rows, against the sync path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("data_tags", [False, True])
def test_two_branches_in_both_orders(data_tags: bool) -> None:
    first, second, _ = _branches(_origin(data_tags=data_tags))
    for inputs in ((first, second), (second, first)):
        _check(_kernel, Rejoin().static_process, inputs)


@pytest.mark.parametrize("order", list(itertools.permutations(range(3))))
def test_three_branches_in_every_order(order: tuple[int, ...]) -> None:
    branches = _branches(_origin(data_tags=True))
    _check(_kernel, Rejoin().static_process, [branches[i] for i in order], range(6))


def test_a_single_input_passes_its_rows() -> None:
    _check(_kernel, Rejoin().static_process, (_branches(_origin())[0],))


def test_a_shared_tag_that_differs_on_some_rows_matches_only_where_equal() -> None:
    origin = _origin()
    left = origin.select_data_columns(["rate"]).add_tags(run=1)
    right = origin.polars_filter(pl.col("unit") < 6).select_data_columns(["depth"])
    right = right.add_tags(run=1)
    other = origin.polars_filter(pl.col("unit") >= 6).select_data_columns(["depth"])
    other = other.add_tags(run=2)
    for inputs in ((left, right), (right, left), (left, other), (other, left)):
        _check(_kernel, Rejoin().static_process, inputs)


def _hand_branch(
    data: str, *, origin: list[str | None], tag: list[int | None]
) -> ArrowTableStream:
    """A hand-built branch: tag ``k``, one system tag, data column ``data``."""
    n = len(origin)
    return ArrowTableStream(
        pa.table(
            {
                "k": pa.array(tag, pa.int64()),
                data: pa.array([float(i) for i in range(n)], pa.float64()),
                "origin": pa.array(origin, pa.large_string()),
            }
        ),
        tag_columns=["k"],
        system_tag_columns=["origin"],
    )


def test_a_null_system_tag_matches_nothing_and_a_null_tag_equals_a_null() -> None:
    # Rows rebuilt from datagrams cannot hold a null system tag (provenance is never
    # null), so the oracle here is the sync path over the raw streams, and the
    # kernel is fed their tables.
    left = _hand_branch("a", origin=["r1", None, "r3", "r4"], tag=[1, 2, None, 4])
    right = _hand_branch("b", origin=["r1", None, "r3", "r4"], tag=[1, 2, None, 5])
    for inputs in ((left, right), (right, left)):
        expected = _transport(Rejoin().static_process(*inputs))
        assert sorted(expected.column("k").to_pylist(), key=str) == [1, None]
        tables = [_transport(stream) for stream in inputs]
        for seed in range(6):
            kernel = StreamingRejoin(Rejoin(), inputs)
            batches = _run(kernel, _arrivals(tables, random.Random(seed)))
            assert_same_rows(batches, kernel, expected)
            assert kernel.held_rows == 0


def _repeated(n: int, data: str, k: list[int] | None = None) -> ArrowTableStream:
    """``n`` rows of ONE origin row: tag ``k``, one system tag, data ``data`` = 0..n-1."""
    return ArrowTableStream(
        pa.table(
            {
                "k": pa.array(k or [1] * n, pa.int64()),
                data: pa.array(range(n), pa.int64()),
                "origin": pa.array(["x"] * n, pa.large_string()),
            }
        ),
        tag_columns=["k"],
        system_tag_columns=["origin"],
    )


def test_repeats_of_an_origin_row_pair_one_to_one_in_arrival_order() -> None:
    # Each input's batches arrive in its row order, so the arrival-order pairing of
    # the occurrence rule is the sync path's row-order pairing.
    for left, right in (
        (_repeated(9, "a"), _repeated(6, "b")),
        (_repeated(8, "a", [1, 2] * 4), _repeated(7, "b", [2, 1, 2, 2, 1, 1, 2])),
    ):
        for inputs in ((left, right), (right, left)):
            _check(_kernel, Rejoin().static_process, inputs)


def test_alternating_repeats_keep_only_unmatched_rows() -> None:
    # 3,000 rows of one origin row on each side, in one-row batches: each arriving row
    # pairs with one remaining row (never with every repeat), and the remaining set
    # holds at most the rows not paired yet.
    n = 3000
    left, right = _repeated(n, "a"), _repeated(n - 7, "b")
    kernel = StreamingRejoin(Rejoin(), (left, right))
    out: list[pa.RecordBatch] = []
    largest = 0
    lefts = _sent(left).combine_chunks().to_batches(max_chunksize=1)
    rights = _sent(right).combine_chunks().to_batches(max_chunksize=1)
    for index in range(n):
        out += kernel.push(0, lefts[index])
        largest = max(largest, kernel.held_rows)  # sampled after every push
        if index < len(rights):
            out += kernel.push(1, rights[index])
            largest = max(largest, kernel.held_rows)
    # The peak: the seven left rows the right input never pairs, held as they wait.
    assert largest == 7
    assert sum(batch.num_rows for batch in out) == n - 7
    assert kernel.held_rows == 7
    kernel.close(1)
    assert kernel.held_rows == 0
    kernel.close(0)


# ---------------------------------------------------------------------------
# What the kernel holds
# ---------------------------------------------------------------------------


def test_only_unmatched_rows_are_held() -> None:
    first, second, _ = _branches(_origin())
    kernel = StreamingRejoin(Rejoin(), (first, second))
    first_rows, second_rows = _sent(first), _sent(second)
    kernel.push(0, first_rows.combine_chunks().to_batches()[0])
    assert kernel.held_rows == first_rows.num_rows
    emitted = kernel.push(1, second_rows.combine_chunks().to_batches()[0])
    matched = sum(batch.num_rows for batch in emitted)
    assert matched == second_rows.num_rows
    # Every matched row left both sides: only the first input's unmatched rows stay.
    assert kernel.held_rows == first_rows.num_rows - matched
    kernel.close(1)
    assert kernel.held_rows == 0
    kernel.close(0)


def test_after_one_input_closes_the_other_inputs_rows_are_not_held() -> None:
    first, second, _ = _branches(_origin())
    second_rows = _sent(second).combine_chunks()
    kernel = StreamingRejoin(Rejoin(), (first, second))
    kernel.push(1, second_rows.slice(0, 3).to_batches()[0])
    assert kernel.held_rows == 3
    # The first input ends with no row: the second's rows can never match.
    kernel.close(0)
    assert kernel.held_rows == 0
    kernel.push(1, second_rows.slice(3).to_batches()[0])
    assert kernel.held_rows == 0
    assert kernel.close(1) == []


def test_three_inputs_release_every_step_once_an_input_ends_empty() -> None:
    branches = _branches(_origin(data_tags=True))
    for empty in range(3):
        kernel = StreamingRejoin(Rejoin(), branches)
        kernel.close(empty)
        for index in range(3):
            if index != empty:
                kernel.push(
                    index, _sent(branches[index]).combine_chunks().to_batches()[0]
                )
        assert kernel.held_rows == 0, empty


# ---------------------------------------------------------------------------
# The warning and the data context
# ---------------------------------------------------------------------------


def _warnings_of(run: Any) -> list[str]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        run()
    return sorted(
        str(w.message)
        for w in caught
        if issubclass(w.category, RejoinDifferingColumnWarning)
    )


def _sync_warnings(inputs: Sequence[StreamProtocol]) -> list[str]:
    rebuilt = [_row_built(stream) for stream in inputs]
    return _warnings_of(lambda: Rejoin().static_process(*rebuilt))


def _kernel_warnings(inputs: Sequence[StreamProtocol], seed: int) -> list[str]:
    tables = [_sent(stream) for stream in inputs]

    def run() -> None:
        kernel = StreamingRejoin(Rejoin(), inputs)
        _run(kernel, _arrivals(tables, random.Random(seed)))

    return _warnings_of(run)


def test_two_inputs_that_differ_in_shared_columns_warn_as_the_sync_path() -> None:
    # Rows of one origin row whose shared tag ``k`` differs: a non-match, and one
    # warning for the run naming the column, as on the sync path.
    left = _hand_branch("a", origin=["r1", "r2", "r3"], tag=[1, 2, 3])
    for right in (
        _hand_branch("b", origin=["r1", "r2", "r3"], tag=[1, 9, 3]),
        _hand_branch("b", origin=["r1", "r2", "r3"], tag=[1, 2, 3]),
        _hand_branch("b", origin=["r4", "r2", "r3"], tag=[1, None, 3]),
    ):
        for inputs in ((left, right), (right, left)):
            want = _sync_warnings(inputs)
            for seed in range(6):
                assert _kernel_warnings(inputs, seed) == want
            _check(_kernel, Rejoin().static_process, inputs, range(4))


OTHER_CONTEXT = "std:v1:test-other"


@pytest.fixture
def other_context(monkeypatch: pytest.MonkeyPatch) -> contexts.DataContext:
    """A copy of the default data context under another key (as ``test_rejoin``)."""
    other = dataclasses.replace(
        contexts.get_default_context(), context_key=OTHER_CONTEXT
    )
    registry = contexts._get_registry()  # pyright: ignore[reportPrivateUsage]  # the test registers a second context
    original = registry.get_context

    def get_context(key: Any = None) -> contexts.DataContext:  # pyright: ignore[reportExplicitAny]  # the registry's own argument
        return other if key == OTHER_CONTEXT else original(key)

    monkeypatch.setattr(registry, "get_context", get_context)
    return other


def _in_context(context: contexts.DataContext, data: str) -> ArrowTableStream:
    """A raw stream of the given data context: tag ``g``, one system tag, ``data``."""
    return ArrowTableStream(
        pa.table(
            {
                "g": pa.array([1, 2, 3], pa.int64()),
                data: pa.array([0.5, 0.25, 0.125], pa.float64()),
                "origin": pa.array(["r1", "r2", "r3"], pa.large_string()),
                constants.CONTEXT_KEY: pa.array(
                    [context.context_key] * 3, pa.large_string()
                ),
            }
        ),
        tag_columns=["g"],
        system_tag_columns=["origin"],
        data_context=context,
    )


def test_inputs_of_two_data_contexts_match_nothing_and_warn_as_the_sync_path(
    other_context: contexts.DataContext,
) -> None:
    default = contexts.get_default_context()
    for inputs in (
        (_in_context(default, "a"), _in_context(other_context, "b")),
        (_in_context(other_context, "b"), _in_context(default, "a")),
    ):
        want = _sync_warnings(inputs)
        assert want
        for seed in range(4):
            assert _kernel_warnings(inputs, seed) == want
        _check(_kernel, Rejoin().static_process, inputs, range(4))


# ---------------------------------------------------------------------------
# Refusals before the first batch (e1, ITL-856)
# ---------------------------------------------------------------------------


def test_refusals_are_the_sync_paths_before_the_first_batch() -> None:
    origin = _origin()
    joined = origin.select_data_columns(["rate"]).join(
        ArrowTableSource(
            pa.table({"subject": pa.array(["s0"], pa.large_string()), "x": [1]}),
            tag_columns=["subject"],
            tag_only_columns=True,
            source_id="x",
        )
    )
    left = _hand_branch("a", origin=["r1"], tag=[1])
    retyped = ArrowTableStream(
        pa.table(
            {
                "k": pa.array([1], pa.int32()),
                "b": pa.array([0.0]),
                "origin": pa.array(["r1"], pa.large_string()),
            }
        ),
        tag_columns=["k"],
        system_tag_columns=["origin"],
    )
    for inputs in (
        (origin.select_data_columns(["depth"]), joined),
        (left, retyped),
        (left, _hand_branch("a", origin=["r1"], tag=[1])),
    ):
        with pytest.raises(InputValidationError) as sync_error:
            Rejoin().static_process(*map(_row_built, inputs))
        with pytest.raises(InputValidationError) as kernel_error:
            StreamingRejoin(Rejoin(), inputs)
        assert str(kernel_error.value) == str(sync_error.value)


def test_every_batch_names_the_output_columns_in_order() -> None:
    branches = _branches(_origin(data_tags=True))
    kernel = StreamingRejoin(Rejoin(), branches)
    batches = _run(
        kernel, _arrivals([_sent(stream) for stream in branches], random.Random(1))
    )
    assert batches
    expected = _transport(Rejoin().static_process(*map(_row_built, branches)))
    assert_same_rows(batches, kernel, expected)


def test_three_inputs_miss_a_difference_the_middle_input_lacks() -> None:
    # A documented difference from the sync path (StreamingRejoin's docstring): with
    # three inputs the warning is detected per link, so a difference between the first
    # and the last input (canonical order) for an origin row the middle input lacks is
    # warned about by the sync path only. The rows are identical. The alternative,
    # pairwise detection with every input's keys kept to the end, was not chosen
    # (option B, November 22, 2026).
    names = ["a", "b", "c"]
    order = sorted(
        names,
        key=lambda n: (
            _hand_branch(n, origin=["r1", "r2"], tag=[1, 2]).pipeline_hash().to_hex()
        ),
    )
    first, middle, last = order
    inputs = [
        _hand_branch(first, origin=["r1", "r2"], tag=[1, 2]),
        _hand_branch(middle, origin=["r1"], tag=[1]),
        _hand_branch(last, origin=["r1", "r2"], tag=[1, 9]),
    ]
    assert [
        inputs[i]
        for i in sorted(range(3), key=lambda i: inputs[i].pipeline_hash().to_hex())
    ] == inputs
    assert len(_sync_warnings(inputs)) == 1
    for seed in range(4):
        assert _kernel_warnings(inputs, seed) == []
    _check(_kernel, Rejoin().static_process, inputs, range(4))


def test_a_match_that_takes_a_closed_inputs_last_row_releases_the_chain() -> None:
    # Canonical order first, middle, last: the last input sends its only row (x) and
    # closes; the first sends x and y; the middle sends x, which matches through to
    # the last input's x. The last step then holds nothing on a closed side and can
    # never emit again, so the first step's y, and any row the first input sends
    # after, are not held.
    names = sorted(
        "abc",
        key=lambda n: _hand_branch(n, origin=["x"], tag=[1]).pipeline_hash().to_hex(),
    )
    first = _hand_branch(names[0], origin=["x", "y"], tag=[1, 2])
    middle = _hand_branch(names[1], origin=["x"], tag=[1])
    last = _hand_branch(names[2], origin=["x"], tag=[1])
    inputs = [first, middle, last]
    assert sorted(inputs, key=lambda s: s.pipeline_hash().to_hex()) == inputs
    kernel = StreamingRejoin(Rejoin(), inputs)
    assert kernel.push(2, _sent(last).combine_chunks().to_batches()[0]) == []
    assert kernel.close(2) == []
    assert kernel.push(0, _sent(first).combine_chunks().to_batches()[0]) == []
    emitted = kernel.push(1, _sent(middle).combine_chunks().to_batches()[0])
    assert sum(batch.num_rows for batch in emitted) == 1
    assert kernel.held_rows == 0
    more = _hand_branch(names[0], origin=["z"], tag=[3])
    assert kernel.push(0, _sent(more).combine_chunks().to_batches()[0]) == []
    assert kernel.held_rows == 0


# ---------------------------------------------------------------------------
# Documented differences from the sync path (StreamingRejoin's docstring)
# ---------------------------------------------------------------------------


def _in_canonical_order(make: Any, candidates: Sequence[tuple[str, ...]]) -> list[Any]:
    """The first ``make(names)`` whose streams are already in canonical order."""
    for names in candidates:
        inputs = make(names)
        if sorted(inputs, key=lambda s: s.pipeline_hash().to_hex()) == inputs:
            return inputs
    raise AssertionError("no arrangement in canonical order")


def _pushed(
    inputs: Sequence[StreamProtocol], schedule: Sequence[tuple[int, int, int]]
) -> tuple[pa.Table, list[str]]:
    """The kernel's rows and warnings for ``schedule``: (input, first row, row count),
    each input's rows in its own order, then every input closed."""
    rows = [_sent(stream).combine_chunks() for stream in inputs]
    out: list[pa.RecordBatch] = []

    def run() -> None:
        kernel = StreamingRejoin(Rejoin(), inputs)
        for index, start, count in schedule:
            out.extend(
                kernel.push(index, rows[index].slice(start, count).to_batches()[0])
            )
        for index in range(len(inputs)):
            out.extend(kernel.close(index))

    caught = _warnings_of(run)
    return pa.Table.from_batches(out), caught


def test_repeats_with_three_inputs_can_pair_by_arrival() -> None:
    # The spec's named risk ("Duplicates ... 'match once' pairs duplicates in arrival
    # order, which can differ between sync and async (risks; ITL-1356 removes the
    # case)"). One origin row repeated: the first two inputs pair their repeats on
    # tag k (orders [1, 2] and [2, 1]); the third, without k, pairs by occurrence
    # with the rows rejoined so far. The sync path ranks those in its table order,
    # among the rows that survived the first step; the kernel ranks them as they
    # arrive. Two schedules that keep each input's row order: one gives the sync
    # rows, one does not. The exact alternative is option X: survival-aware ranking
    # with per-step counters, rows of a repeated origin held until every earlier row
    # of that origin has a final fate at the step before.
    def keyed(data: str, k: list[int]) -> ArrowTableStream:
        return _repeated(len(k), data, k)

    def plain(data: str) -> ArrowTableStream:
        return ArrowTableStream(
            pa.table(
                {
                    data: pa.array([10, 20], pa.int64()),
                    "origin": pa.array(["x", "x"], pa.large_string()),
                }
            ),
            tag_columns=[],
            system_tag_columns=["origin"],
        )

    inputs = _in_canonical_order(
        lambda n: [keyed(n[0], [1, 2]), keyed(n[1], [2, 1]), plain(n[2])],
        list(itertools.permutations("abcdefgh", 3)),
    )
    want = _rows(_transport(Rejoin().static_process(*map(_row_built, inputs))))
    whole, _ = _pushed(inputs, [(0, 0, 2), (1, 0, 2), (2, 0, 2)])
    one_by_one, _ = _pushed(inputs, [(0, 0, 2), (1, 0, 1), (1, 1, 1), (2, 0, 2)])
    assert _rows(whole) == want
    assert whole.num_rows == one_by_one.num_rows == 2
    # One row at a time, k=2 arrives first and pairs with the third branch's first
    # value: the alternate pairing, every other value as it was.
    (third,) = [stream for stream in inputs if not stream.keys()[0]]
    value = third.keys()[1][0]

    def pairs(table: pa.Table) -> set[tuple[Any, Any]]:
        return set(zip(table.column("k").to_pylist(), table.column(value).to_pylist()))

    assert pairs(whole) == {(1, 10), (2, 20)}
    assert pairs(one_by_one) == {(2, 10), (1, 20)}
    # Every other column with its key: a swap of the first two branches' values
    # between k=1 and k=2 fails here.
    others = [name for name in whole.column_names if name != value]
    assert _rows(one_by_one.select(others)) == _rows(whole.select(others))


def test_the_warning_for_repeats_of_two_inputs_can_depend_on_arrival() -> None:
    # Best effort with repeated origins: the sync path compares repeats by occurrence
    # whatever their matches, the kernel only rows that matched nothing. One origin,
    # k = [1, 2] against [2, 1]: the rows are the sync rows in both schedules; the
    # warning is the sync path's when the rows alternate, and missed when the first
    # input's rows are all held first. The alternative is option B: pairwise
    # detection with every input's keys kept to the end.
    inputs = [
        _hand_branch("a", origin=["o", "o"], tag=[1, 2]),
        _hand_branch("b", origin=["o", "o"], tag=[2, 1]),
    ]
    want_rows = _rows(_transport(Rejoin().static_process(*map(_row_built, inputs))))
    assert len(_sync_warnings(inputs)) == 1
    buffered, buffered_warnings = _pushed(inputs, [(0, 0, 2), (1, 0, 1), (1, 1, 1)])
    alternating, alternating_warnings = _pushed(
        inputs, [(0, 0, 1), (1, 0, 1), (0, 1, 1), (1, 1, 1)]
    )
    assert _rows(buffered) == _rows(alternating) == want_rows
    assert buffered_warnings == []
    assert alternating_warnings == _sync_warnings(inputs)


def test_the_warning_misses_adjacent_inputs_when_an_earlier_input_lacks_the_row() -> (
    None
):
    # Best effort with three inputs: the first input (canonical order) has only origin
    # y; the next two have origin x with different k. They are adjacent, but their x
    # rows are never compared: the step that would compare them sees the rows
    # rejoined so far, which lack x. The sync path warns; the rows are identical. The
    # alternative is option B: pairwise detection with every input's keys kept.
    inputs = _in_canonical_order(
        lambda n: [
            _hand_branch(n[0], origin=["y"], tag=[1]),
            _hand_branch(n[1], origin=["x"], tag=[1]),
            _hand_branch(n[2], origin=["x"], tag=[2]),
        ],
        list(itertools.permutations("abcdef", 3)),
    )
    assert len(_sync_warnings(inputs)) == 1
    for seed in range(4):
        assert _kernel_warnings(inputs, seed) == []
    _check(_kernel, Rejoin().static_process, inputs, range(4))


def test_two_inputs_with_unique_origins_warn_as_the_sync_path_for_every_schedule() -> (
    None
):
    # The guarantee: two inputs, unique origins, every arrival schedule.
    left = _hand_branch("a", origin=["r1", "r2", "r3", "r4"], tag=[1, 2, 3, None])
    right = _hand_branch("b", origin=["r4", "r3", "r2", "r5"], tag=[None, 7, 2, 1])
    for inputs in ((left, right), (right, left)):
        want = _sync_warnings(inputs)
        assert len(want) == 1
        for seed in range(24):
            assert _kernel_warnings(inputs, seed) == want


# ---------------------------------------------------------------------------
# Cost: one join per batch, no whole-table copy
# ---------------------------------------------------------------------------


_POOLS: list[Any] = []


def test_each_batch_costs_one_join_and_copies_no_held_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # N repeats held in N one-row chunks, then their N partners one row at a time:
    # each partner batch runs one Polars join against the held rows (O(held) per
    # batch, as in the Join kernels) and takes its values from the held chunks by row
    # number; no held table is copied, so a batch allocates far less Arrow memory
    # than the held rows occupy.
    n = 400
    left, right = _repeated(n, "a"), _repeated(n, "b")
    kernel = StreamingRejoin(Rejoin(), (left, right))
    for batch in _sent(left).combine_chunks().to_batches(max_chunksize=1):
        kernel.push(0, batch)
    held_bytes = _sent(left).nbytes
    joins = 0
    original = pl.DataFrame.join

    def counted(self: pl.DataFrame, *args: Any, **kwargs: Any) -> pl.DataFrame:
        nonlocal joins
        joins += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pl.DataFrame, "join", counted)
    previous = pa.default_memory_pool()
    large: list[int] = []
    for index, batch in enumerate(
        _sent(right).combine_chunks().to_batches(max_chunksize=1)
    ):
        before_joins = joins
        # A pool of its own per batch: its peak counts every Arrow allocation of the
        # batch, also one freed before the batch returns. Pools are kept for the
        # process: arrays allocated in one may outlive the test.
        pool = pa.proxy_memory_pool(previous)
        _POOLS.append(pool)
        pa.set_memory_pool(pool)
        try:
            emitted = kernel.push(1, batch)
        finally:
            pa.set_memory_pool(previous)
        assert sum(b.num_rows for b in emitted) == 1
        assert joins - before_joins == 1
        if (pool.max_memory() or 0) >= held_bytes / 4:
            large.append(index)
    # Only the compactions copy the remaining rows: when half the rows held were
    # removed since the last one, so at most log2(n) times in the run.
    assert len(large) <= math.ceil(math.log2(n)), large
    assert kernel.held_rows == 0


def test_a_partly_matching_batch_costs_two_joins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Held: origins x and z with k = [1, 1]. Arriving in one batch: x and z with
    # k = [2, 1]. z matches (the match join); x matched nothing but shares its
    # origin with a held row, so one more join on the system tags finds the pair and
    # a vectorized comparison names k. Two joins, and the warning is the sync path's.
    left = _hand_branch("a", origin=["x", "z"], tag=[1, 1])
    right = _hand_branch("b", origin=["x", "z"], tag=[2, 1])
    joins = 0
    original = pl.DataFrame.join

    def counted(self: pl.DataFrame, *args: Any, **kwargs: Any) -> pl.DataFrame:
        nonlocal joins
        joins += 1
        return original(self, *args, **kwargs)

    kernel = StreamingRejoin(Rejoin(), (left, right))
    kernel.push(0, _sent(left).combine_chunks().to_batches()[0])
    monkeypatch.setattr(pl.DataFrame, "join", counted)
    caught: list[pa.RecordBatch] = []

    def run() -> None:
        caught.extend(kernel.push(1, _sent(right).combine_chunks().to_batches()[0]))
        caught.extend(kernel.close(0) + kernel.close(1))

    found = _warnings_of(run)
    monkeypatch.undo()
    assert joins == 2
    assert sum(batch.num_rows for batch in caught) == 1
    assert found == _sync_warnings((left, right))
    assert len(found) == 1


def test_four_inputs_keep_buffered_matches_when_earlier_steps_release() -> None:
    # Canonical inputs I0..I3. x and y match through the first two steps and wait at
    # the last step for I3. Then I1 closes: the steps before the last can never emit
    # again and are released, but the last step keeps its two buffered rows, which
    # I3 then matches. A late row of I0 (z) goes to a released step and is not held.
    # Every call order of the four inputs gives the sync rows.
    names = sorted(
        "abcd",
        key=lambda n: _hand_branch(n, origin=["x"], tag=[1]).pipeline_hash().to_hex(),
    )
    canonical = [
        _hand_branch(names[0], origin=["x", "y", "z"], tag=[1, 2, 3]),
        *(_hand_branch(n, origin=["x", "y"], tag=[1, 2]) for n in names[1:]),
    ]
    assert sorted(canonical, key=lambda s: s.pipeline_hash().to_hex()) == canonical
    expected = _transport(Rejoin().static_process(*map(_row_built, canonical)))
    assert expected.num_rows == 2
    sent = [_sent(stream).combine_chunks() for stream in canonical]
    for call_order in itertools.permutations(range(4)):
        inputs = [canonical[i] for i in call_order]
        at = {position: call_order.index(position) for position in range(4)}
        kernel = StreamingRejoin(Rejoin(), inputs)
        out: list[pa.RecordBatch] = []
        out += kernel.push(at[0], sent[0].slice(0, 2).to_batches()[0])
        out += kernel.push(at[1], sent[1].to_batches()[0])
        out += kernel.push(at[2], sent[2].to_batches()[0])
        assert kernel.held_rows == 2
        out += kernel.close(at[1])
        assert kernel.held_rows == 2
        out += kernel.push(at[0], sent[0].slice(2, 1).to_batches()[0])
        assert kernel.held_rows == 2
        out += kernel.push(at[3], sent[3].to_batches()[0])
        assert kernel.held_rows == 0
        for position in (0, 2, 3):
            out += kernel.close(at[position])
        assert_same_rows(out, kernel, expected)


def test_extension_and_nested_payloads_cross_held_chunks_and_compaction() -> None:
    # A logical (extension) payload and a nested payload, held in one-row chunks and
    # matched in another order, across the compactions: every value is taken from
    # its chunk, bit for bit, and the rows are the sync rows.
    n = 40
    path_type = contexts.get_default_context().type_converter.python_type_to_arrow_type(
        Path
    )
    assert isinstance(path_type, pa.BaseExtensionType)
    nested_type = pa.large_list(
        pa.struct([("a", pa.int64()), ("b", pa.large_list(pa.int64()))])
    )
    origin = ArrowTableSource(
        pa.table(
            {
                "unit": pa.array(range(n), pa.int64()),
                "path": pa.ExtensionArray.from_storage(
                    path_type,
                    pa.array(
                        [f"/data/f{i}.bin" for i in range(n)], path_type.storage_type
                    ),
                ),
                "nested": pa.array(
                    [[{"a": i, "b": [i, i + 1]}] for i in range(n)], nested_type
                ),
                "other": pa.array([float(i) for i in range(n)], pa.float64()),
            }
        ),
        tag_columns=["unit"],
        tag_only_columns=True,
        source_id="payloads",
    )
    held = origin.select_data_columns(["path", "nested"])
    arriving = origin.select_data_columns(["other"])
    expected = _transport(
        Rejoin().static_process(_row_built(held), _row_built(arriving))
    )
    assert expected.num_rows == n
    for inputs, first in (((held, arriving), 0), ((arriving, held), 1)):
        kernel = StreamingRejoin(Rejoin(), inputs)
        out: list[pa.RecordBatch] = []
        for batch in _sent(held).combine_chunks().to_batches(max_chunksize=1):
            out += kernel.push(first, batch)
        rows = _sent(arriving).combine_chunks()
        for start in reversed(range(0, n, 3)):
            out += kernel.push(1 - first, rows.slice(start, 3).to_batches()[0])
        out += kernel.close(0) + kernel.close(1)
        assert kernel.held_rows == 0
        assert_same_rows(out, kernel, expected)
        got = pa.Table.from_batches(out)
        assert got.schema.field("path").type == path_type
