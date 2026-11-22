"""The streaming Rejoin kernel gives the sync Rejoin's rows, whatever the arrivals.

ITL-1362 (ITL-1347, stage 5): ``Rejoin`` runs on the async path as symmetric matches on
every shared column over Arrow record batches (``StreamingRejoin``), with ITL-1355's rule:
an inner join on every column the inputs share (system tags, shared user tags, a shared
data-tag's source info, the data context), ``Join``'s equality, a null equal to a null
outside the system tags, a null system tag matching nothing, each row matching at most
once. The oracle and the arrivals are those of ``test_streaming_joins``: the sync
operator over the inputs rebuilt from rows, compared as a multiset, after random batch
cuts and interleavings.
"""

from __future__ import annotations

import dataclasses
import itertools
import random
import warnings
from collections.abc import Sequence
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
    _check,  # pyright: ignore[reportPrivateUsage]  # the shared harness
    _row_built,  # pyright: ignore[reportPrivateUsage]  # the shared harness
    _run,  # pyright: ignore[reportPrivateUsage]  # the shared harness
    _sent,  # pyright: ignore[reportPrivateUsage]  # the shared harness
    _transport,  # pyright: ignore[reportPrivateUsage]  # the shared harness
    assert_same_rows,
)


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


def test_thousands_of_repeats_stay_linear() -> None:
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
        if index < len(rights):
            out += kernel.push(1, rights[index])
        largest = max(largest, kernel.held_rows)
    assert largest <= 8
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
