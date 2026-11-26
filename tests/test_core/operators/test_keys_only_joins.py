"""Joins on the async path hold keys and references, whatever their inputs certify.

ITL-1464: the streaming kernels keep their inputs' join keys in memory and the rows in a
spool (``core/row_spool``), let a row go once it has matched when the other side is
certified unique on the keys (``core/unique_keys``), and send their output in bounded
pieces. The rows are those of the sync path, as a multiset, with every spool in memory or
on disk and for every kind of inputs: one to one, a lookup on either side, many to many.
"""

from __future__ import annotations

import asyncio
import functools
import itertools
import random
import warnings
from collections import Counter
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

import orcapod.contexts as contexts

from orcapod.core.operators import Join, MergeJoin, Rejoin, SemiJoin
from orcapod.core.operators import streaming_joins
from orcapod.core.operators.streaming_joins import (
    StreamingJoin,
    StreamingJoinKernel,
    StreamingMergeJoin,
    StreamingRejoin,
    StreamingSemiJoin,
)
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.record_batch_channels import OutputChannel
from orcapod.core.row_spool import RowSpool
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams.carried import carried_batches
from orcapod.core.streams.record_batch_input import InputChannel
from orcapod.errors import UpstreamFailedError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.utils import source_tokens
from tests.test_core.operators.test_rejoin import other_context  # noqa: F401  # the fixture

from .test_streaming_joins import _arrivals, _check, _rows, _run, _sent, _transport  # pyright: ignore[reportPrivateUsage]  # the kernels' one oracle

TIMEOUT = 30.0


@pytest.fixture(autouse=True)
def _quiet() -> Any:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


@pytest.fixture(params=["memory", "disk"])
def spooled(request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: Any) -> Path:
    """Every spool in memory, or every row on disk; output in pieces of 3 rows, and an
    ended side of more than 2 rows indexed only once it is known to be the smaller."""
    limit = 0 if request.param == "disk" else 1 << 40
    monkeypatch.setattr(
        streaming_joins, "RowSpool", functools.partial(RowSpool, limit, str(tmp_path))
    )
    monkeypatch.setattr(streaming_joins, "PAIRS_PER_BATCH", 3)
    monkeypatch.setattr(streaming_joins, "PUSH_ROWS", 2)
    monkeypatch.setattr(streaming_joins, "_COMPACT_ROWS", 4)
    return tmp_path


def _files(root: Path) -> list[Path]:
    return list(root.rglob("*.arrow"))


def _source(
    name: str,
    tags: dict[str, list[Any]],
    *,
    key: Sequence[str] | None = None,
    types: dict[str, pa.DataType] | None = None,
    layout: str = "tags_first",
    colliding: bool = False,
) -> ArrowTableSource:
    """A source with these tags and two data columns, shuffled rows."""
    n = len(next(iter(tags.values())))
    order = random.Random(name).sample(range(n), n)
    columns: dict[str, pa.Array[Any]] = {
        t: pa.array([v[i] for i in order], (types or {}).get(t))
        for t, v in tags.items()
    }
    first = "value" if colliding else f"{name}_x"
    data: dict[str, pa.Array[Any]] = {
        first: pa.array([f"{name}{i}" for i in order], pa.large_string()),
        f"{name}_y": pa.array([None if i % 4 == 1 else i / 2 for i in order]),
    }
    merged = {**columns, **data} if layout == "tags_first" else {**data, **columns}
    names = list(reversed(merged)) if layout == "reversed" else list(merged)
    return ArrowTableSource(
        pa.table({c: merged[c] for c in names}),
        tag_columns=list(tags) if layout == "tags_first" else list(reversed(tags)),
        record_id_columns=key,
        tag_only_columns=True,
        source_id=name,
    )


def _pairs(n: int, m: int) -> dict[str, list[Any]]:
    return {
        "session": [f"s{i}" for i in range(n) for _ in range(m)],
        "unit": [j for _ in range(n) for j in range(m)],
    }


def _one_to_one(layout: str) -> list[ArrowTableSource]:
    key = ("session", "unit")
    return [
        _source("a", _pairs(4, 5), key=key, layout=layout),
        _source("b", _pairs(5, 4), key=key[::-1], layout="reversed"),
    ]


def _lookup(layout: str) -> list[ArrowTableSource]:
    spikes = {**_pairs(6, 6), "n": list(range(36))}
    spikes["session"] = [f"s{i % 5}" for i in range(36)]
    return [
        _source("spikes", spikes, key=("n", "session"), layout=layout),
        _source("units", _pairs(4, 5), key=("session", "unit"), layout="reversed"),
    ]


def _many_to_many(layout: str) -> list[ArrowTableSource]:
    left = {**_pairs(2, 3), "n": list(range(6))}
    right = {"unit": [0, 1, 1, 2, 2, 2, 9], "m": list(range(7))}
    return [
        _source("a", left, key=("n",), layout=layout),
        _source("b", right, key=("m",), layout="reversed"),
    ]


def _null_keys(layout: str) -> list[ArrowTableSource]:
    """A unique side (a key is never null) against keys with nulls and repeats."""
    left = {"k": [1, None, 2, 2, None, 7], "n": list(range(6))}
    return [
        _source("a", left, key=("n",), layout=layout),
        _source("b", {"k": [1, 2, 3], "g": [1, 1, 2]}, key=("k",), layout="reversed"),
    ]


def _cross(layout: str) -> list[ArrowTableSource]:
    return [
        _source("a", {"i": [1, 2, 3], "i2": [1, 1, 1]}, key=("i",), layout=layout),
        _source("b", {"j": [1, 2, 3, 4], "j2": [1, 1, 1, 1]}, key=("j",)),
    ]


KINDS: dict[str, Callable[[str], list[ArrowTableSource]]] = {
    "one_to_one": _one_to_one,
    "lookup": _lookup,
    "many_to_many": _many_to_many,
    "null_keys": _null_keys,
    "no_shared_tag": _cross,
}
LAYOUTS = ("tags_first", "data_first", "reversed")


def _join(inputs: Sequence[StreamProtocol]) -> StreamingJoinKernel:
    return StreamingJoin(Join(), inputs)


def _semi(inputs: Sequence[StreamProtocol]) -> StreamingJoinKernel:
    return StreamingSemiJoin(SemiJoin(), inputs)


def _merge(inputs: Sequence[StreamProtocol]) -> StreamingJoinKernel:
    return StreamingMergeJoin(MergeJoin(), inputs)


@pytest.mark.parametrize("swapped", [False, True])
@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("kind", sorted(KINDS))
def test_two_inputs_give_the_sync_rows(
    spooled: Path, kind: str, layout: str, swapped: bool
) -> None:
    inputs = KINDS[kind](layout)
    if swapped:
        inputs.reverse()
    _check(_join, Join().static_process, inputs)
    _check(_semi, SemiJoin().static_process, inputs)
    _check(_merge, MergeJoin().static_process, inputs)
    assert _files(spooled) == []


@pytest.mark.parametrize("kind", ["one_to_one", "lookup", "many_to_many"])
def test_merge_join_of_a_colliding_column(spooled: Path, kind: str) -> None:
    (a, b) = KINDS[kind]("tags_first")
    inputs = [
        stream.map_data({f"{name}_x": "value"})
        if hasattr(stream, "map_data")
        else stream
        for stream, name in (
            (a, a.source_id.split(":")[1]),
            (b, b.source_id.split(":")[1]),
        )
    ]
    _check(_merge, MergeJoin().static_process, inputs)
    _check(_merge, MergeJoin().static_process, inputs[::-1])
    assert _files(spooled) == []


@pytest.mark.parametrize("order", [(0, 1, 2), (2, 0, 1), (1, 2, 0)])
@pytest.mark.parametrize("keyed", ["all", "some", "none"])
def test_three_inputs_give_the_sync_rows(
    spooled: Path, keyed: str, order: tuple[int, ...]
) -> None:
    spikes = {**_pairs(4, 5), "n": list(range(20))}
    inputs = [
        _source("spikes", spikes, key=None if keyed == "none" else ("n",)),
        _source(
            "units",
            _pairs(5, 4),
            key=("session", "unit") if keyed == "all" else None,
            layout="reversed",
        ),
        _source(
            "sessions",
            {"session": ["s0", "s1", "s3", "s9"], "day": [1, 2, 2, 3]},
            key=("session",) if keyed != "none" else None,
            layout="data_first",
        ),
    ]
    _check(_join, Join().static_process, [inputs[i] for i in order])
    assert _files(spooled) == []


def test_float_keys_with_a_certified_side(spooled: Path) -> None:
    """``GroupBy`` certifies float keys under the joins' equality (NaN, signed zero)."""
    nan = float("nan")
    values = [0.0, -0.0, nan, 1.5, float("-nan"), 1.5]
    grouped = _source("g", {"f": values, "n": list(range(6))}).group_by("f")
    assert grouped.unique_keys == frozenset({frozenset({"f"})})
    other = _source("o", {"f": [-0.0, nan, 1.5, 2.0, 0.0], "m": [1, 2, 3, 4, 5]})
    for inputs in ([grouped, other], [other, grouped]):
        _check(_join, Join().static_process, inputs)
        _check(_semi, SemiJoin().static_process, inputs)


# ---------------------------------------------------------------------------
# What is held
# ---------------------------------------------------------------------------


def _batches(stream: StreamProtocol, size: int) -> list[pa.RecordBatch]:
    table = _sent(stream).combine_chunks()
    return [
        batch
        for start in range(0, table.num_rows, size)
        for batch in table.slice(start, size).combine_chunks().to_batches()
    ]


def _sorted_source(name: str, n: int, *, key: bool = True) -> ArrowTableSource:
    return ArrowTableSource(
        pa.table(
            {
                "t": list(range(n)),
                "u": ["u"] * n,
                f"{name}1": [1] * n,
                f"{name}2": [2] * n,
            }
        ),
        tag_columns=["u", "t"],
        record_id_columns=["t"] if key else None,
        tag_only_columns=True,
        source_id=name,
    )


def test_a_one_to_one_join_in_step_holds_only_the_rows_between_the_inputs(
    spooled: Path,
) -> None:
    left, right = _sorted_source("a", 60), _sorted_source("b", 60)
    kernel = _join([left, right])
    emitted = 0
    for mine, theirs in zip(_batches(left, 5), _batches(right, 5)):
        kernel.push(0, mine)
        assert kernel.held_rows == 5
        emitted += sum(b.num_rows for b in kernel.push(1, theirs))
        assert kernel.held_rows == 0
        assert _files(spooled) == []  # and the files went with their rows
    assert emitted == 60


def _keys_held(kernel: StreamingJoinKernel) -> list[int]:
    """How many rows' keys each side of a kernel's links holds."""
    return [
        sum(frame.height for frame in side._frames)  # pyright: ignore[reportPrivateUsage]  # what it keeps
        for link in kernel._links  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]  # what it keeps
        for side in link.sides
    ]


@pytest.mark.parametrize("first", [0, 1])
def test_a_join_holds_the_keys_of_its_smaller_side_whichever_ends_first(
    spooled: Path, first: int
) -> None:
    spikes = _source(
        "spikes", {"unit": [0, 1, 2, 3] * 10, "n": list(range(40))}, key=("n",)
    )
    units = _source("units", {"unit": [0, 1, 2], "area": [1, 1, 2]}, key=("unit",))
    inputs = [spikes, units]
    batches = [_batches(spikes, 8), _batches(units, 2)]
    kernel = StreamingJoin(Join(), inputs)
    small = kernel._position[1]  # pyright: ignore[reportPrivateUsage]  # its side
    emitted = 0

    def step(out: list[pa.RecordBatch]) -> None:
        nonlocal emitted
        emitted += sum(b.num_rows for b in out)
        held = _keys_held(kernel)
        assert held[1 - small] == 0  # the larger side never has keys
        assert held[small] <= 3

    for batch in batches[first]:
        step(kernel.push(first, batch))
    step(kernel.close(first))
    assert emitted == 0
    for batch in batches[1 - first]:
        step(kernel.push(1 - first, batch))
    if first == 0:
        assert emitted == 0  # the units may still be more than the spikes
    step(kernel.close(1 - first))
    assert emitted == 30
    assert kernel.held_rows == 0
    assert _files(spooled) == []


def test_an_ended_side_is_indexed_once_the_other_has_as_many_rows(
    spooled: Path,
) -> None:
    """The wait is bounded by the ended side's rows: nothing waits for an end."""
    left = _source("a", {"k": [1, 2] * 5, "n": list(range(10))})
    right = _source("b", {"k": [1, 2] * 50, "m": list(range(100))})
    kernel = _join([left, right])
    for batch in _batches(left, 5):
        assert kernel.push(0, batch) == []
    assert kernel.close(0) == []
    emitted = [
        sum(b.num_rows for b in kernel.push(1, batch)) for batch in _batches(right, 5)
    ]
    assert emitted[:2] == [0, 50]  # ten rows arrived: the left ten are indexed
    assert emitted[2:] == [25] * 18
    assert kernel.held_rows == 10


def test_a_small_ended_side_is_indexed_at_once(spooled: Path) -> None:
    left = _source("a", {"k": [1, 2], "n": [1, 2]})
    right = _source("b", {"k": [1, 2] * 5, "m": list(range(10))})
    kernel = _join([left, right])
    for batch in _batches(left, 2):
        kernel.push(0, batch)
    kernel.close(0)
    assert sum(b.num_rows for b in kernel.push(1, _batches(right, 1)[0])) == 1


def test_a_row_of_a_lookup_indexed_first_is_let_go_when_it_matches(
    spooled: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other side is unique, so an indexed row can match only once."""
    spikes = _source("spikes", {"unit": [0, 0, 1, 2], "n": [1, 2, 3, 4]}, key=("n",))
    units = _source("units", {"unit": [0, 1, 2], "area": [1, 1, 2]}, key=("unit",))
    monkeypatch.setattr(streaming_joins, "PUSH_ROWS", 64)  # indexed at once
    kernel = _join([spikes, units])
    (all_spikes,), all_units = _batches(spikes, 4), _batches(units, 1)
    kernel.push(0, all_spikes)
    kernel.close(0)
    held = 4
    for batch in all_units:
        held -= sum(b.num_rows for b in kernel.push(1, batch))
        assert kernel.held_rows == held
    assert held == 0


def test_without_a_certificate_the_indexed_rows_stay(spooled: Path) -> None:
    left, right = (
        _sorted_source("a", 6, key=False),
        _sorted_source("b", 6, key=False),
    )
    kernel = _join([left, right])
    (one,), (two,) = _batches(left, 6), _batches(right, 6)
    kernel.push(0, one)
    kernel.push(1, two)
    assert kernel.held_rows == 12
    assert sum(b.num_rows for b in kernel.close(0)) == 6
    assert kernel.held_rows == 6  # the left ones: a right row may still match them
    assert sum(b.num_rows for b in kernel.push(1, two)) == 6
    kernel.release()
    assert _files(spooled) == []


def test_a_many_to_many_match_is_emitted_in_bounded_pieces(
    spooled: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(streaming_joins, "PUSH_ROWS", 64)  # indexed at once
    left = _source("a", {"k": [1] * 20 + [2], "n": list(range(21))})
    right = _source("b", {"k": [1] * 15 + [2], "m": list(range(16))})
    (mine,), (theirs,) = _batches(left, 21), _batches(right, 16)
    # Replayed when a side ends, and probed after it: both in pieces.
    replayed = _join([left, right])
    replayed.push(0, mine)
    replayed.push(1, theirs)
    probed = _join([left, right])
    probed.push(0, mine)
    probed.close(0)
    for batches in (replayed.close(0), probed.push(1, theirs)):
        sizes = [batch.num_rows for batch in batches]
        assert sum(sizes) == 20 * 15 + 1
        assert max(sizes) <= 3
    # One arriving row's matches are cut too.
    many = _join([left, right])
    many.push(0, mine)
    many.close(0)
    assert all(b.num_rows <= 3 for b in many.push(1, theirs.slice(0, 1)))


def test_replayed_rows_leave_the_disk_as_they_are_matched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each input's rows are on disk at most once, and go while they are replayed."""
    monkeypatch.setattr(
        streaming_joins, "RowSpool", functools.partial(RowSpool, 0, str(tmp_path))
    )
    monkeypatch.setattr(streaming_joins, "PUSH_ROWS", 4)
    left = _source("a", {"k": [1, 2] * 20, "n": list(range(40))})
    right = _source("b", {"k": [1, 2], "m": [1, 2]})
    kernel = _join([left, right])
    for batch in _batches(left, 4):
        kernel.push(0, batch)
    (small,) = _batches(right, 2)
    kernel.push(1, small)
    assert len(_files(tmp_path)) == 11
    seen = [len(_files(tmp_path)) for _ in kernel.iter_close(1)]
    assert seen == sorted(seen, reverse=True)
    assert seen[-1] <= 2 and len(_files(tmp_path)) == 1  # the indexed side's rows
    kernel.release()
    assert _files(tmp_path) == []


def test_a_semi_join_holds_each_right_key_once(spooled: Path) -> None:
    left = _source("a", {"k": [1, 2], "n": [1, 2]})
    right = _source("b", {"k": [1, 1, 3, 3, 3] * 4, "m": list(range(20))})
    kernel = _semi([left, right])
    for batch in _batches(right, 5):
        kernel.push(1, batch)
    assert kernel.held_rows == 2


# ---------------------------------------------------------------------------
# The channels
# ---------------------------------------------------------------------------


def _bounded(coroutine: Any) -> Any:
    async def go() -> Any:
        return await asyncio.wait_for(coroutine, TIMEOUT)

    return asyncio.run(go())


@pytest.mark.parametrize("kind", sorted(KINDS))
@pytest.mark.parametrize("op", [Join(), SemiJoin(), MergeJoin()], ids=lambda o: repr(o))
def test_the_channel_result_is_the_sync_result_as_rows(
    spooled: Path, op: Any, kind: str
) -> None:
    inputs = KINDS[kind]("tags_first")
    output = OutputChannel(op(*inputs))
    _bounded(op.async_execute_batches([s.open_channel() for s in inputs], output))
    assert _rows(_transport(output.result())) == _rows(_transport(op(*inputs)))
    assert _files(spooled) == []


def test_a_diamond_through_bounded_channels_does_not_wait_for_itself(
    spooled: Path,
) -> None:
    """Both inputs read one upstream, which sends to both and waits for the slower."""
    source = _sorted_source("a", 50)
    left = source.select_data_columns(["a1"])
    right = source.select_data_columns(["a2"])

    async def go() -> StreamProtocol:
        one = OutputChannel(left, retain=False, buffer_size=1)
        two = OutputChannel(right, retain=False, buffer_size=1)
        op = Join()
        output = OutputChannel(op(left, right))
        readers = [one.open_reader(), two.open_reader()]

        async def produce() -> None:
            for channel, stream in ((two, right), (one, left)):
                for batch in _batches(stream, 1):
                    await channel.send(batch)
            await one.close()
            await two.close()

        await asyncio.gather(produce(), op.async_execute_batches(readers, output))
        return output.result()

    result = _bounded(go())
    assert _rows(_transport(result)) == _rows(_transport(left.join(right)))


def test_a_failed_input_fails_the_output_and_leaves_no_file(
    spooled: Path, request: pytest.FixtureRequest
) -> None:
    left, right = _sorted_source("a", 20), _sorted_source("b", 20)

    async def go() -> None:
        failing = OutputChannel(left, retain=False, buffer_size=4)
        op = Join()
        output = OutputChannel(op(left, right))
        reader = failing.open_reader()
        run = asyncio.create_task(
            op.async_execute_batches([reader, right.open_channel()], output)
        )
        await failing.send(_batches(left, 5)[0])
        await asyncio.sleep(0.05)
        if "disk" in request.node.name:
            assert _files(spooled)
        failing.fail(RuntimeError("the read broke"))
        with pytest.raises(UpstreamFailedError, match="the read broke"):
            await run
        with pytest.raises(RuntimeError, match="the read broke"):
            output.result()

    _bounded(go())
    assert _files(spooled) == []


def test_a_cancelled_join_stops_its_readers_and_leaves_no_file(spooled: Path) -> None:
    left, right = _sorted_source("a", 20), _sorted_source("b", 20)

    async def go() -> None:
        waiting = OutputChannel(left, retain=False, buffer_size=1)
        op = Join()
        output = OutputChannel(op(left, right))
        run = asyncio.create_task(
            op.async_execute_batches(
                [waiting.open_reader(), right.open_channel()], output
            )
        )
        await waiting.send(_batches(left, 5)[0])
        await asyncio.sleep(0.05)
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run
        # Nothing reads the producer's output any more, and it waits for nobody.
        for batch in _batches(left, 5)[1:]:
            await waiting.send(batch)

    _bounded(go())
    assert _files(spooled) == []


def test_an_output_that_is_not_read_holds_the_join_back(spooled: Path) -> None:
    left = _source("a", {"k": [1] * 30, "n": list(range(30))})
    right = _source("b", {"k": [1] * 30, "m": list(range(30))})

    async def go() -> None:
        op = Join()
        result = op(left, right)
        output = OutputChannel(result, retain=False, buffer_size=1)
        reader = output.open_reader()
        run = asyncio.create_task(
            op.async_execute_batches(
                [left.open_channel(), right.open_channel()], output
            )
        )
        await asyncio.sleep(0.1)
        assert not run.done()  # 900 rows in pieces of 3: it waits for the reader
        rows = sum([batch.num_rows async for batch in reader])
        await run
        assert rows == 900

    _bounded(go())


def test_a_subclass_keeps_its_own_computation() -> None:
    class Emptied(Join):
        def static_process(self, *streams: StreamProtocol) -> StreamProtocol:
            joined = super().static_process(*streams)
            table = joined.as_table(columns={"source": True, "system_tags": True})
            return ArrowTableStream(table.slice(0, 1), tag_columns=joined.keys()[0])

    inputs = _one_to_one("tags_first")
    op = Emptied()
    output = OutputChannel(op(*inputs))
    _bounded(op.async_execute_batches([s.open_channel() for s in inputs], output))
    assert output.result().as_table().num_rows == 1


def test_a_random_arrival_of_everything_once_more(spooled: Path) -> None:
    """Larger inputs, every kind of certificate, many interleavings."""
    rng = random.Random(0)
    for n in (1, 40):
        tags = {
            "k": [rng.randrange(8) for _ in range(n)],
            "h": [rng.choice("xy") for _ in range(n)],
            "n": list(range(n)),
        }
        left = _source("a", tags, key=("n", "k"))
        right = _source("b", {"k": list(range(8)), "q": [1] * 8}, key=("k",))
        tables = [_sent(left), _sent(right)]
        expected = _rows(_transport(Join().static_process(left, right)))
        for seed in range(30):
            kernel = _join([left, right])
            batches = _run(kernel, _arrivals(tables, random.Random(seed)))
            got = pa.Table.from_batches(batches) if batches else None
            assert (_rows(got) if got is not None else {}) == expected


# ---------------------------------------------------------------------------
# Batches whose source-info tokens are derived (``utils/source_tokens``)
# ---------------------------------------------------------------------------


def _carried(stream: StreamProtocol) -> pa.Table:
    """An input's rows as a channel sends them: its tokens derived where they can be."""
    feed = stream.open_channel()

    async def read() -> list[pa.RecordBatch]:
        return [batch async for batch in carried_batches(feed)]

    return pa.Table.from_batches(asyncio.run(read()))


def _check_derived(
    make_kernel: Callable[[Sequence[StreamProtocol]], StreamingJoinKernel],
    sync: Callable[..., StreamProtocol],
    inputs: Sequence[StreamProtocol],
    texts: Sequence[bool] = (False, False, False),
) -> list[pa.RecordBatch]:
    """Check the rows of a kernel fed derived batches (text for ``texts``)."""
    tables = [
        source_tokens.materialize(_carried(s)) if text else _carried(s)
        for s, text in zip(inputs, texts)
    ]
    for table, text in zip(tables, texts):
        assert bool(source_tokens.derived_columns(table.schema)) != text
    expected = _rows(_transport(sync(*inputs)))
    batches: list[pa.RecordBatch] = []
    for seed in range(6):
        kernel = make_kernel(inputs)
        batches = _run(kernel, _arrivals(tables, random.Random(seed)))
        rows: Any = Counter()
        for batch in batches:
            rows += _rows(source_tokens.materialize(pa.Table.from_batches([batch])))
        assert rows == expected
        assert kernel.held_rows == 0
    return batches


@pytest.mark.parametrize("texts", [(False, False), (True, False), (False, True)])
@pytest.mark.parametrize("kind", sorted(KINDS))
def test_derived_tokens_give_the_sync_rows(
    spooled: Path, kind: str, texts: tuple[bool, bool]
) -> None:
    inputs = KINDS[kind]("reversed")
    for make, op in ((_join, Join()), (_semi, SemiJoin()), (_merge, MergeJoin())):
        _ = _check_derived(make, op.static_process, inputs, texts)
    assert _files(spooled) == []


def test_a_join_carries_derived_tokens_through(spooled: Path) -> None:
    """They cost nothing held: no text is built for a join of different pipelines."""
    inputs = KINDS["lookup"]("tags_first")
    batches = _check_derived(_join, Join().static_process, inputs)
    derived = source_tokens.derived_columns(batches[0].schema)
    assert len(derived) == 4  # two data columns of each input


def _data_tagged(name: str, n: int) -> ArrowTableSource:
    return ArrowTableSource(
        pa.table(
            {
                "k": [i % 3 for i in range(n)],
                "g": ["ab"[i % 2] for i in range(n)],
                f"{name}1": list(range(n)),
                f"{name}2": [f"{name}{i}" for i in range(n)],
            }
        ),
        tag_columns=["g", "k"],
        source_id=name,
    )


@pytest.mark.parametrize(
    "texts", [(False, False, False), (True, False, False), (False, True, True)]
)
def test_merged_data_tags_of_derived_tokens(
    spooled: Path, texts: tuple[bool, bool, bool]
) -> None:
    inputs = [_data_tagged(name, 5 + i) for i, name in enumerate("abc")]
    _ = _check_derived(_join, Join().static_process, inputs, texts)
    _ = _check_derived(_merge, MergeJoin().static_process, inputs[:2], texts[:2])
    _ = _check_derived(_semi, SemiJoin().static_process, inputs[:2], texts[:2])


def test_inputs_of_one_pipeline_hash_sort_their_system_tags(spooled: Path) -> None:
    """Their system-tag values move, so a token derived from them is built first."""
    inputs = [
        ArrowTableSource(
            pa.table(
                {"k": [1, 2, 1], "j": [1, 1, 2], "v": ["x", "y", "z"], "w": [1, 2, 3]}
            ),
            tag_columns=["j", "k"],
            tag_only_columns=True,
            source_id=name,
        )
        for name in ("second", "first")
    ]
    _ = _check_derived(_merge, MergeJoin().static_process, inputs)
    renamed = [s.map_data({"v": f"v{i}", "w": f"w{i}"}) for i, s in enumerate(inputs)]
    _ = _check_derived(_join, Join().static_process, renamed)


@pytest.mark.parametrize("first", ["text", "derived"])
def test_a_producer_that_changes_a_tokens_form(spooled: Path, first: str) -> None:
    left, right = KINDS["one_to_one"]("tags_first")
    (derived,) = _carried(left).combine_chunks().to_batches()
    (text,) = source_tokens.materialize(_carried(left)).combine_chunks().to_batches()
    one, two = (text, derived) if first == "text" else (derived, text)
    kernel = _join([left, right])
    _ = kernel.push(0, one.slice(0, 3))
    _ = kernel.push(0, two.slice(3))
    rows: Any = Counter()
    for batch in _carried(right).to_batches():
        for out in kernel.push(1, batch):
            rows += _rows(source_tokens.materialize(pa.Table.from_batches([out])))
    assert rows == _rows(_transport(left.join(right)))


@pytest.mark.parametrize("staged", [False, True])
@pytest.mark.parametrize("kind", ["one_to_one", "lookup", "many_to_many"])
@pytest.mark.parametrize("op", [Join(), SemiJoin(), MergeJoin()], ids=lambda o: repr(o))
def test_batches_of_both_forms_however_they_arrive(
    spooled: Path, op: Any, kind: str, staged: bool
) -> None:
    """Review of #567: grouped arrivals succeeded where staged ones were refused."""
    inputs = KINDS[kind]("tags_first")
    expected = _rows(_transport(op(*inputs)))
    tables = [_carried(s) for s in inputs]

    async def go() -> StreamProtocol:
        channels = [OutputChannel(s, retain=False, buffer_size=8) for s in inputs]
        output = OutputChannel(op(*inputs))
        readers = [channel.open_reader() for channel in channels]
        run = asyncio.create_task(op.async_execute_batches(readers, output))
        for channel, table in zip(channels, tables):
            size = max(1, table.num_rows // 3)
            for start in range(0, table.num_rows, size):
                part = table.slice(start, size)
                if (start // size) % 2:
                    part = source_tokens.materialize(part)
                for batch in part.combine_chunks().to_batches():
                    await channel.send(batch)
                if staged:
                    await asyncio.sleep(0.01)
        for channel in channels:
            await channel.close()
        await run
        return output.result()

    assert _rows(_transport(_bounded(go()))) == expected
    assert _files(spooled) == []


@pytest.mark.parametrize("op", [Join(), SemiJoin(), MergeJoin()], ids=lambda o: repr(o))
def test_an_operator_after_a_join_gets_derived_batches(spooled: Path, op: Any) -> None:
    """A chain over channels: the join's output is another operator's input."""
    inputs = KINDS["lookup"]("reversed")
    joined = op(*inputs)
    then = joined.drop_nulls("spikes_y")

    async def go() -> StreamProtocol:
        middle = OutputChannel(joined)
        last = OutputChannel(then)
        reader = middle.open_reader()
        await asyncio.gather(
            op.async_execute_batches([s.open_channel() for s in inputs], middle),
            then.producer.async_execute_batches([reader], last),
        )
        return last.result()

    assert _rows(_transport(_bounded(go()))) == _rows(_transport(then))


@pytest.mark.parametrize("spare", ["source", "operator"])
def test_a_customized_input_that_repeats_a_key_loses_no_row(
    spooled: Path, spare: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review of #565: the instance gives two rows of a key its class would refuse."""
    left = _source("a", {"k": [1], "n": [7]}, key=("k",))
    twice = _source("b", {"k": [1, 1], "m": [1, 2]})
    once = _source("b", {"k": [1], "m": [1]}, key=("k",))
    if spare == "source":
        right: Any = once
        right.as_table = twice.as_table
        right.iter_data = twice.iter_data
    else:
        op: Any = SemiJoin()
        op.binary_static_process = lambda *_: twice
        right = op(once, once)
    monkeypatch.setattr(streaming_joins, "PUSH_ROWS", 1)
    join = Join()
    output = OutputChannel(join(left, right))
    inputs = [left.open_channel(), right.open_channel()]
    _bounded(join.async_execute_batches(inputs, output))
    assert output.result().as_table().num_rows == 2


@pytest.mark.parametrize("op", [Join(), SemiJoin(), MergeJoin()], ids=lambda o: repr(o))
def test_a_customized_join_keeps_its_own_computation(op: Any) -> None:
    inputs = _one_to_one("tags_first")
    original = op.static_process

    def first_row(*streams: StreamProtocol) -> StreamProtocol:
        joined = original(*streams)
        table = joined.as_table(columns={"source": True, "system_tags": True})
        return ArrowTableStream(table.slice(0, 1), tag_columns=joined.keys()[0])

    op.static_process = first_row
    output = OutputChannel(op(*inputs))
    _bounded(op.async_execute_batches([s.open_channel() for s in inputs], output))
    assert output.result().as_table().num_rows == 1


def test_what_an_in_step_join_keeps_does_not_grow_with_the_rows_that_passed(
    spooled: Path,
) -> None:
    """Review of #565: the held-row mask was numbered from the first row ever held."""
    left, right = _sorted_source("a", 400), _sorted_source("b", 400)
    kernel = StreamingJoin(Join(), [left, right])
    (link,) = kernel._links  # pyright: ignore[reportPrivateUsage]  # what it keeps
    for mine, theirs in zip(_batches(left, 4), _batches(right, 4)):
        _ = kernel.push(0, mine)
        _ = kernel.push(1, theirs)
        for side in link.sides:
            assert len(side._alive) <= 8  # pyright: ignore[reportPrivateUsage]  # what it keeps
            assert sum(f.height for f in side._frames) <= 4  # pyright: ignore[reportPrivateUsage]  # what it keeps


def test_rows_held_for_long_among_rows_that_pass_are_matched_at_the_end(
    spooled: Path,
) -> None:
    """The numbering is moved past removed rows while earlier and later ones are held."""
    n = 200
    left = _sorted_source("a", n)
    right = _sorted_source("b", n)
    kernel = _join([left, right])
    mine = _sent(left).sort_by("t")
    theirs = _sent(right).sort_by("t")
    waiting = [5, 6, 90, 150]
    passing = [i for i in range(n) if i not in waiting]
    got = 0
    for batch in mine.combine_chunks().to_batches(max_chunksize=10):
        got += sum(b.num_rows for b in kernel.push(0, batch))
        low = batch.column("t")[0].as_py()
        rows = [i for i in passing if low <= i < low + 10]
        (part,) = theirs.take(pa.array(rows)).combine_chunks().to_batches()
        got += sum(b.num_rows for b in kernel.push(1, part))
    assert got == len(passing)
    assert kernel.held_rows == len(waiting)
    (last,) = theirs.take(pa.array(waiting)).combine_chunks().to_batches()
    out = pa.Table.from_batches(kernel.push(1, last))
    assert sorted(str(t) for t in out.column("t").to_pylist()) == sorted(
        map(str, waiting)
    )
    assert kernel.held_rows == 0


# ---------------------------------------------------------------------------
# Long second inputs, and an end in the middle of a replay
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("op", [Join(), SemiJoin(), MergeJoin()], ids=lambda o: repr(o))
def test_an_endless_input_is_joined_as_it_arrives_once_the_other_has_ended(
    spooled: Path, op: Any
) -> None:
    """It never ends: every row after the first input's end is sent, and none held."""
    endless = _source("a", {"k": [1, 2, 3, 4], "n": [1, 2, 3, 4]})
    small = _source("b", {"k": [1, 2], "m": [1, 2]}, key=("k",))
    result = op(endless, small)

    async def go() -> int:
        channel = OutputChannel(endless, retain=False, buffer_size=1)
        output = OutputChannel(result, retain=False, buffer_size=1)
        reader, results = channel.open_reader(), output.open_reader()
        run = asyncio.create_task(
            op.async_execute_batches([reader, small.open_channel()], output)
        )

        batches = _batches(endless, 2)

        async def produce() -> None:
            while True:
                for batch in batches:
                    await channel.send(batch)
                    await asyncio.sleep(0)  # also when nobody reads any more

        producing = asyncio.create_task(produce())
        rows = 0
        async for batch in results:
            rows += batch.num_rows
            if rows > 300:
                break
        # What was spooled before the small input ended has been replayed and let go.
        assert len(_files(spooled)) <= 2
        for task in (run, producing):
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        return rows

    assert _bounded(go()) > 300
    assert _files(spooled) == []


@pytest.mark.parametrize("end", ["cancel", "close_reader"])
@pytest.mark.parametrize("op", [Join(), SemiJoin(), MergeJoin()], ids=lambda o: repr(o))
def test_a_join_that_ends_while_replaying_leaves_nothing(
    spooled: Path, op: Any, end: str
) -> None:
    left = _source("a", {"k": [1, 2] * 100, "n": list(range(200))})
    right = _source("b", {"k": [1, 2], "m": [1, 2]})

    async def go() -> None:
        late = OutputChannel(right, retain=False, buffer_size=4)
        output = OutputChannel(op(left, right), retain=False, buffer_size=1)
        results = output.open_reader()
        run = asyncio.create_task(
            op.async_execute_batches([left.open_channel(), late.open_reader()], output)
        )
        await asyncio.sleep(0.05)  # the left rows are spooled, the left input ended
        for batch in _batches(right, 1):
            await late.send(batch)
        await late.close()
        _ = await results.receive()  # the replay has begun, and waits for this reader
        await asyncio.sleep(0.05)
        assert not run.done()
        if end == "cancel":
            run.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run
            with pytest.raises(UpstreamFailedError):
                _ = await results.receive()
        else:
            await results.close()
            await run  # no reader: it ends normally

    _bounded(go())
    assert _files(spooled) == []


def test_an_input_that_fails_after_the_other_ended_fails_the_join(
    spooled: Path,
) -> None:
    left = _source("a", {"k": [1, 2] * 10, "n": list(range(20))})
    right = _source("b", {"k": [1, 2], "m": [1, 2]})

    async def go() -> None:
        failing = OutputChannel(left, retain=False, buffer_size=8)
        op = Join()
        output = OutputChannel(op(left, right))
        run = asyncio.create_task(
            op.async_execute_batches(
                [failing.open_reader(), right.open_channel()], output
            )
        )
        await failing.send(_batches(left, 5)[0])
        await asyncio.sleep(0.05)
        failing.fail(RuntimeError("the read broke"))
        with pytest.raises(UpstreamFailedError, match="the read broke"):
            await run

    _bounded(go())
    assert _files(spooled) == []


# ---------------------------------------------------------------------------
# Rejoin of two inputs
# ---------------------------------------------------------------------------


def _branches(kind: str) -> list[Any]:
    source = _source(
        "a",
        {**_pairs(4, 5), "n": list(range(20))},
        key=("n", "unit"),
        layout="reversed",
    )
    if kind == "data_tags":
        source = _data_tagged("a", 20)
        return [
            source.select_data_columns(["a1", "k"]),
            source.select_data_columns(["a2"]),
        ]
    first = source.select_data_columns(["a_x"])
    second = source.select_data_columns(["a_y"])
    if kind == "filtered":
        return [first, second.drop_nulls("a_y")]
    if kind == "differing_tag":
        return [first, second.map_data(a_y="z")]
    if kind == "empty":
        return [first, second.filter(session="none")]
    return [first, second]


@pytest.mark.parametrize("swapped", [False, True])
@pytest.mark.parametrize(
    "kind", ["plain", "filtered", "differing_tag", "empty", "data_tags"]
)
def test_rejoin_of_two_branches_over_channels(
    spooled: Path, kind: str, swapped: bool
) -> None:
    inputs = _branches(kind)[:: -1 if swapped else 1]
    op = Rejoin()
    output = OutputChannel(op(*inputs))
    _bounded(op.async_execute_batches([s.open_channel() for s in inputs], output))
    assert _rows(_transport(output.result())) == _rows(_transport(op(*inputs)))
    assert _files(spooled) == []


def test_rejoin_holds_only_rows_not_matched_yet(spooled: Path) -> None:
    inputs = _branches("plain")
    kernel = StreamingRejoin(Rejoin(), inputs)
    for mine, theirs in zip(_batches(inputs[0], 5), _batches(inputs[1], 5)):
        assert kernel.push(0, mine) == []
        assert kernel.held_rows == 5
        assert sum(b.num_rows for b in kernel.push(1, theirs)) == 5
        assert kernel.held_rows == 0
        assert _files(spooled) == []


def test_rejoin_of_three_inputs_is_a_barrier_and_equal(spooled: Path) -> None:
    source = _source("a", {**_pairs(4, 5), "n": list(range(20))}, key=("n",))
    inputs = [
        source.select_data_columns(["a_x"]),
        source.select_data_columns(["a_y"]),
        source.map_data(a_x="x2", a_y="y2"),
    ]
    op = Rejoin()
    output = OutputChannel(op(*inputs))
    _bounded(op.async_execute_batches([s.open_channel() for s in inputs], output))
    assert _rows(_transport(output.result())) == _rows(_transport(op(*inputs)))


def test_one_old_row_does_not_pin_what_later_rows_cost(spooled: Path) -> None:
    """Review of #565: a mask numbered from an old row grew with every later one."""
    left, right = _sorted_source("a", 400), _sorted_source("b", 400)
    kernel = StreamingJoin(Join(), [left, right])
    (link,) = kernel._links  # pyright: ignore[reportPrivateUsage]  # what it keeps
    lefts, rights = _batches(left, 4), _batches(right, 4)
    _ = kernel.push(0, lefts[0])  # never matched until the end
    for mine, theirs in zip(lefts[1:], rights[1:]):
        _ = kernel.push(0, mine)
        _ = kernel.push(1, theirs)
        for side in link.sides:
            assert len(side._alive) <= 32  # pyright: ignore[reportPrivateUsage]  # what it keeps
            assert len(side._numbers) <= 32  # pyright: ignore[reportPrivateUsage]  # what it keeps
    assert kernel.held_rows == 4
    assert sum(b.num_rows for b in kernel.push(1, rights[0])) == 4


def test_a_public_read_of_a_channel_gives_text() -> None:
    """Review of #567: a recipe reached a consumer, which could write it to a file."""
    source = _data_tagged("a", 5)

    async def read() -> list[list[pa.RecordBatch]]:
        sent = OutputChannel(source)
        for batch in _carried_batches:
            await sent.send(batch)
        await sent.close()
        feeds = [source.open_channel, sent.open_reader]
        return [
            [await feed().receive() for feed in feeds],
            [batch for feed in feeds async for batch in feed()],
            [batch for feed in feeds for batch in await feed().collect()],
        ]

    _carried_batches = _carried(source).combine_chunks().to_batches()
    assert source_tokens.derived_columns(_carried_batches[0].schema)
    for batches in _bounded(read()):
        assert len(batches) == 2
        for batch in batches:
            assert source_tokens.derived_columns(batch.schema) == ()
            assert pa.Table.from_batches([batch]).to_pylist() == (
                _transport(source).select(batch.schema.names).to_pylist()
            )


def test_a_channel_reads_a_stream_through_its_own_reader() -> None:
    """Review of #567: a subclass's ``as_table`` was bypassed."""

    class OneRow(ArrowTableStream):
        def as_table(self, **kwargs: Any) -> pa.Table:  # pyright: ignore[reportIncompatibleMethodOverride]  # a user's reader
            return super().as_table(**kwargs).slice(0, 1)

    stream = OneRow(pa.table({"k": [1, 2], "x": [1, 2]}), tag_columns=["k"])
    batches = _bounded(stream.open_channel().collect())
    assert sum(batch.num_rows for batch in batches) == 1


def test_rows_with_logical_types_are_replayed(spooled: Path) -> None:
    """A logical type (a ``Path``) among the columns of rows held and replayed."""
    rows = [
        {"k": i % 2, "n": i, "p": Path(f"/x/{i}"), "q": [Path("/y")]} for i in range(9)
    ]
    left = DictSource(
        rows, tag_columns=["n", "k"], tag_only_columns=True, source_id="left"
    )
    right = _source("b", {"k": [0, 1], "m": [1, 2]})
    for inputs in ([left, right], [right, left]):
        _check(_join, Join().static_process, inputs, range(4))
        _check(_semi, SemiJoin().static_process, inputs, range(4))
        _check(_merge, MergeJoin().static_process, inputs, range(4))


@pytest.mark.parametrize("first", [0, 1])
def test_alternating_token_forms_keep_one_memory_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, first: int
) -> None:
    """Review of #567: a spool per change of form kept every one of them in memory."""
    spools: list[RowSpool] = []

    def make() -> RowSpool:
        spools.append(RowSpool(20_000, str(tmp_path)))
        return spools[-1]

    monkeypatch.setattr(streaming_joins, "RowSpool", make)
    left = ArrowTableSource(
        pa.table(
            {
                "k": [i % 2 for i in range(64)],
                "n": list(range(64)),
                "x": [f"{i}" + "x" * 1_000 for i in range(64)],
                "y": list(range(64)),
            }
        ),
        tag_columns=["n", "k"],
        tag_only_columns=True,
        source_id="left",
    )
    right = _source("b", {"k": [0, 1], "m": [1, 2]})
    derived = _carried(left).combine_chunks()
    forms = [derived, source_tokens.materialize(derived)]
    kernel = _join([left, right])
    for i in range(64):
        (batch,) = forms[(i + first) % 2].slice(i, 1).combine_chunks().to_batches()
        assert kernel.push(0, batch) == []
        assert sum(spool.memory_bytes for spool in spools) <= 20_000
        # One spool per side to start with, and at most one more.
        assert len([spool for spool in spools if spool.size]) <= 2
    assert len(_files(tmp_path)) < 10
    for batch in _carried(right).to_batches():
        kernel.push(1, batch)
    rows: Any = Counter()
    for out in [*kernel.close(1), *kernel.close(0)]:
        rows += _rows(source_tokens.materialize(pa.Table.from_batches([out])))
    assert rows == _rows(_transport(left.join(right)))
    assert _files(tmp_path) == []


def test_no_read_of_a_channel_gives_a_recipe() -> None:
    """Review of #567: the channel's own protocol offered the internal form."""
    channel = _data_tagged("a", 3).open_channel()
    assert [n for n in dir(channel) if "carried" in n and not n.startswith("_")] == []


def test_a_channel_of_a_users_class_is_read_through_its_own_reads() -> None:
    source = _source("a", {"k": [1, 2, 3], "n": [1, 2, 3]})
    inner = source.open_channel()

    class OneRow(InputChannel):
        async def receive(self) -> pa.RecordBatch:
            return (await super().receive()).slice(0, 1)

    channel = OneRow(source, cast(InputChannel, inner)._feed)  # pyright: ignore[reportPrivateUsage]  # a user's channel over a feed

    async def read() -> int:
        return sum([b.num_rows async for b in carried_batches(channel)])

    assert _bounded(read()) == 1


@pytest.mark.parametrize("op", [Join(), SemiJoin(), MergeJoin()], ids=lambda o: repr(o))
def test_a_replay_whose_sends_never_wait_can_be_cancelled(
    spooled: Path, op: Any
) -> None:
    """Review of #567: the whole replay ran before any other task had a turn."""
    left = _source("a", {"k": [1, 2] * 100, "n": list(range(200))})
    right = _source("b", {"k": [1, 2], "m": [1, 2]})
    sent = 0
    began = asyncio.Event()

    class Sink(OutputChannel):
        async def send(self, batch: pa.RecordBatch) -> None:
            nonlocal sent
            sent += batch.num_rows
            began.set()
            await super().send(batch)

    async def go() -> None:
        sink = Sink(op(left, right), retain=False)  # no reader: a send never waits
        run = asyncio.create_task(
            op.async_execute_batches([left.open_channel(), right.open_channel()], sink)
        )
        await began.wait()
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run

    _bounded(go())
    assert 0 < sent < 100
    assert _files(spooled) == []


def test_rejoin_over_channels_equals_its_barrier_for_rows_of_several_contexts(
    spooled: Path,
    other_context: contexts.DataContext,  # noqa: F811  # the fixture, by name
) -> None:
    """Channels carry no per-row data context, so the two forms see one (ITL-1359)."""
    default = contexts.get_default_context()

    def branch(name: str, second: str) -> ArrowTableStream:
        table = pa.table(
            {
                "k": [1, 2],
                "_tag_source_id::t": pa.array(["s", "s"], pa.large_string()),
                "_tag_record_id::t": pa.array(["r1", "r2"], pa.large_string()),
                name: [10, 20],
                constants.CONTEXT_KEY: pa.array(
                    [default.context_key, second], pa.large_string()
                ),
            }
        )
        return ArrowTableStream(table, tag_columns=["k"], data_context=default)

    inputs = [branch("a", other_context.context_key), branch("b", default.context_key)]
    op = Rejoin()

    async def go(*, barrier: bool) -> StreamProtocol:
        output = OutputChannel(op(*inputs))
        channels = [s.open_channel() for s in inputs]
        if barrier:
            await StaticOutputOperatorPod.async_execute_batches(op, channels, output)
        else:
            await op.async_execute_batches(channels, output)
        return output.result()

    assert _rows(_transport(_bounded(go(barrier=False)))) == _rows(
        _transport(_bounded(go(barrier=True)))
    )


def _rejoin(inputs: Sequence[StreamProtocol]) -> StreamingJoinKernel:
    return StreamingRejoin(Rejoin(), inputs)


def _data_tagged_inputs(kernel: str) -> list[Any]:
    """Inputs that share data-tags, with every token of them derived."""
    if kernel == "rejoin":
        source = _data_tagged("a", 7)
        return [
            source.select_data_columns(["a1", "k", "g"]),
            source.select_data_columns(["a2", "k", "g"]),
        ]
    names = "abc" if kernel == "join3" else "ab"
    return [_data_tagged(name, 5 + i) for i, name in enumerate(names)]


DATA_TAG_KERNELS: dict[str, tuple[Any, Any]] = {
    "join": (_join, Join()),
    "join3": (_join, Join()),
    "merge_join": (_merge, MergeJoin()),
    "semi_join": (_semi, SemiJoin()),
    "rejoin": (_rejoin, Rejoin()),
}


@pytest.mark.parametrize("blocks", [2, 64])
@pytest.mark.parametrize("changes", ["one", "every"])
@pytest.mark.parametrize("first", ["derived", "text"])
@pytest.mark.parametrize("kernel", sorted(DATA_TAG_KERNELS))
def test_shared_data_tags_when_a_tokens_form_changes(
    spooled: Path,
    kernel: str,
    first: str,
    changes: str,
    blocks: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review of #567: a merged data-tag's tokens are held under a name of the join's
    own, and rows of both forms could not be put together. Every input changes its form
    half way (or one does, each in turn), before and after the others arrive and end."""
    # With blocks of 64 rows, rows of both forms are fetched together.
    monkeypatch.setattr(streaming_joins, "PUSH_ROWS", blocks)
    monkeypatch.setattr(streaming_joins, "PAIRS_PER_BATCH", blocks)
    make, op = DATA_TAG_KERNELS[kernel]
    inputs = _data_tagged_inputs(kernel)
    expected = _rows(_transport(op.static_process(*inputs)))
    assert expected
    carried = [_carried(s).combine_chunks() for s in inputs]
    assert all(source_tokens.derived_columns(t.schema) for t in carried)
    changing = [{i} for i in range(len(inputs))]
    if changes == "every":
        changing = [set(range(len(inputs)))]
    for changed in changing:
        parts: list[list[pa.RecordBatch]] = []
        for index, table in enumerate(carried):
            half = table.num_rows // 2
            pieces = [table.slice(0, half), table.slice(half)]
            if index in changed:
                as_text = 1 if first == "derived" else 0
                pieces[as_text] = source_tokens.materialize(pieces[as_text])
            parts.append([b for t in pieces for b in t.combine_chunks().to_batches()])
        for order in itertools.permutations(range(len(inputs))):
            # Each input whole in turn, its end now or only after the last one.
            for ends_at_once in (True, False):
                events: list[tuple[int, pa.RecordBatch | None]] = []
                for index in order:
                    events += [(index, batch) for batch in parts[index]]
                    if ends_at_once:
                        events.append((index, None))
                if not ends_at_once:
                    events += [(index, None) for index in order]
                _check_events(make(inputs), events, expected)
        # And the inputs' halves in turn.
        events = [
            (index, parts[index][half])
            for half in (0, 1)
            for index in range(len(inputs))
        ]
        events += [(index, None) for index in range(len(inputs))]
        _check_events(make(inputs), events, expected)
    assert _files(spooled) == []


def _check_events(
    kernel: StreamingJoinKernel,
    events: Sequence[tuple[int, pa.RecordBatch | None]],
    expected: Any,
) -> None:
    """Check the rows a kernel gives for these arrivals, their tokens as text."""
    rows: Any = Counter()
    for batch in _run(kernel, events):
        rows += _rows(source_tokens.materialize(pa.Table.from_batches([batch])))
    assert rows == expected
    assert kernel.held_rows == 0


@pytest.mark.parametrize("first", ["derived", "text"])
@pytest.mark.parametrize("kernel", sorted(DATA_TAG_KERNELS))
def test_shared_data_tags_whose_tokens_change_form_over_staged_channels(
    spooled: Path, kernel: str, first: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same over channels, the join taking each batch before the next is sent."""
    monkeypatch.setattr(streaming_joins, "PUSH_ROWS", 64)
    monkeypatch.setattr(streaming_joins, "PAIRS_PER_BATCH", 64)
    _, op = DATA_TAG_KERNELS[kernel]
    inputs = _data_tagged_inputs(kernel)
    expected = _rows(_transport(op.static_process(*inputs)))
    tables = [_carried(s).combine_chunks() for s in inputs]

    async def go() -> StreamProtocol:
        channels = [OutputChannel(s, retain=False, buffer_size=8) for s in inputs]
        output = OutputChannel(op(*inputs))
        readers = [channel.open_reader() for channel in channels]
        run = asyncio.create_task(op.async_execute_batches(readers, output))
        for half in (0, 1):
            for channel, table in zip(channels, tables):
                middle = table.num_rows // 2
                part = table.slice(0, middle) if half == 0 else table.slice(middle)
                if (half == 0) == (first == "text"):
                    part = source_tokens.materialize(part)
                for batch in part.combine_chunks().to_batches():
                    await channel.send(batch)
                await asyncio.sleep(0.01)
        for channel in channels:
            await channel.close()
        await run
        return output.result()

    assert _rows(_transport(_bounded(go()))) == expected
    assert _files(spooled) == []
