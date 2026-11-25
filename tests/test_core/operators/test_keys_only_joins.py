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
import random
import warnings
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.operators import Join, MergeJoin, SemiJoin
from orcapod.core.operators import streaming_joins
from orcapod.core.operators.streaming_joins import (
    StreamingJoin,
    StreamingJoinKernel,
    StreamingMergeJoin,
    StreamingSemiJoin,
)
from orcapod.core.record_batch_channels import OutputChannel
from orcapod.core.row_spool import RowSpool
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import UpstreamFailedError
from orcapod.protocols.core_protocols import StreamProtocol

from .test_streaming_joins import _arrivals, _check, _rows, _run, _sent, _transport  # pyright: ignore[reportPrivateUsage]  # the kernels' one oracle

TIMEOUT = 30.0


@pytest.fixture(autouse=True)
def _quiet() -> Any:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


@pytest.fixture(params=["memory", "disk"])
def spooled(request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: Any) -> Path:
    """Every spool in memory, or every row on disk; output in pieces of 3 rows."""
    limit = 0 if request.param == "disk" else 1 << 40
    monkeypatch.setattr(
        streaming_joins, "RowSpool", functools.partial(RowSpool, limit, str(tmp_path))
    )
    monkeypatch.setattr(streaming_joins, "PAIRS_PER_BATCH", 3)
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


def test_a_lookup_holds_the_unique_side_and_no_row_that_matched(spooled: Path) -> None:
    spikes = _source(
        "spikes", {"unit": [0, 1, 2, 3] * 10, "n": list(range(40))}, key=("n",)
    )
    units = _source("units", {"unit": [0, 1, 2], "area": [1, 1, 2]}, key=("unit",))
    kernel = _join([spikes, units])
    (first, *rest) = _batches(spikes, 8)
    kernel.push(0, first)
    assert kernel.held_rows == 8  # arrived before any unit
    (all_units,) = _batches(units, 3)
    kernel.push(1, all_units)
    assert kernel.held_rows == 2 + 3  # the spikes of unit 3, and the units
    for batch in rest:
        kernel.push(0, batch)
    assert kernel.held_rows == 10 + 3  # unit 3 may still arrive
    kernel.close(1)
    assert kernel.held_rows == 3  # it did not
    kernel.close(0)
    assert kernel.held_rows == 0


def test_without_a_certificate_nothing_that_may_match_again_is_let_go(
    spooled: Path,
) -> None:
    left, right = (
        _sorted_source("a", 6, key=False),
        _sorted_source("b", 6, key=False),
    )
    kernel = _join([left, right])
    (one,), (two,) = _batches(left, 6), _batches(right, 6)
    kernel.push(0, one)
    assert sum(b.num_rows for b in kernel.push(1, two)) == 6
    assert kernel.held_rows == 12
    kernel.release()
    assert _files(spooled) == []


def test_a_many_to_many_match_is_emitted_in_bounded_pieces(spooled: Path) -> None:
    left = _source("a", {"k": [1] * 20 + [2], "n": list(range(21))})
    right = _source("b", {"k": [1] * 15 + [2], "m": list(range(16))})
    kernel = _join([left, right])
    (mine,), (theirs,) = _batches(left, 21), _batches(right, 16)
    kernel.push(0, mine)
    sizes = [batch.num_rows for batch in kernel.push(1, theirs)]
    assert sum(sizes) == 20 * 15 + 1
    assert max(sizes) <= 3
    # One arriving row's matches are cut too.
    many = _join([left, right])
    many.push(0, mine)
    assert all(b.num_rows <= 3 for b in many.push(1, theirs.slice(0, 1)))


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
