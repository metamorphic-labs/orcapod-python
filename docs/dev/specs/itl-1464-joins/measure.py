"""One async join over record-batch channels, its time and peak memory (ITL-1464).

Usage: ``measure.py <case> <rows> [barrier]``. Synthetic rows; the inputs are sent batch by
batch through bounded channels (nothing else is held) and the output is read and dropped.
``barrier`` runs the join as before this change (every input read whole, then the sync
call). Cases:

- ``lookup``: ``rows`` spikes against 300 units, the units arriving first;
- ``lookup_late``: the same, the units arriving after every spike;
- ``one_to_one``: two inputs of ``rows`` rows, unique on every shared tag, in step;
- ``one_to_one_late``: the same, one input arriving entirely before the other;
- ``many``: ``rows`` spikes against 1,000 events, neither unique on the session and
  shank they share: five output rows per spike.
- ``many_open``: two inputs of ``rows`` rows in step, neither unique on the session and
  burst they share: five output rows per row, and both sides' keys held to the end.
- ``semi``: ``rows`` spikes kept by the units that exist (300).
"""

from __future__ import annotations

import asyncio
import resource
import sys
import time
import warnings
from collections.abc import Callable, Iterator
from typing import Any

import numpy as np
import pyarrow as pa

from orcapod.core.operators import Join, SemiJoin
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.record_batch_channels import OutputChannel
from orcapod.core.sources import ArrowTableSource
from orcapod.protocols.core_protocols import StreamProtocol

BATCH = 8_192
SESSION = "session-0123456789abcdef"


def _text(values: np.ndarray[Any, Any], pattern: str) -> pa.Array[Any]:
    return pa.array([pattern % v for v in values.tolist()], pa.large_string())


def spikes(start: int, n: int) -> pa.Table:
    """Synthetic event rows: tags and data of several types."""
    i = np.arange(start, start + n)
    return pa.table(
        {
            "session": pa.array([SESSION] * n, pa.large_string()),
            "unit": _text(i % 300, "unit-%05d"),
            "spike": pa.array(i, pa.int64()),
            "shank": pa.array(i % 200, pa.int64()),
            "burst": pa.array(i // 5, pa.int64()),
            "time": pa.array(i / 30_000.0),
            "amplitude": pa.array((i % 97) * 1.5),
        }
    )


def second(start: int, n: int) -> pa.Table:
    """A second value per spike, for the one-to-one cases."""
    i = np.arange(start, start + n)
    return pa.table(
        {
            "session": pa.array([SESSION] * n, pa.large_string()),
            "unit": _text(i % 300, "unit-%05d"),
            "spike": pa.array(i, pa.int64()),
            "depth": pa.array(i % 13, pa.int64()),
            "width": pa.array((i % 7) * 0.5),
        }
    )


def units(start: int, n: int) -> pa.Table:
    i = np.arange(start, start + n)
    return pa.table(
        {
            "session": pa.array([SESSION] * n, pa.large_string()),
            "unit": _text(i, "unit-%05d"),
            "area": _text(i % 9, "area-%d"),
            "quality": pa.array(i * 1.0),
        }
    )


def marks(start: int, n: int) -> pa.Table:
    """Rows five of which share a burst, as five spikes do."""
    i = np.arange(start, start + n)
    return pa.table(
        {
            "session": pa.array([SESSION] * n, pa.large_string()),
            "burst": pa.array(i // 5, pa.int64()),
            "mark": pa.array(i, pa.int64()),
            "level": pa.array(i * 0.1),
            "kind": _text(i % 4, "kind-%d"),
        }
    )


def events(start: int, n: int) -> pa.Table:
    i = np.arange(start, start + n)
    return pa.table(
        {
            "session": pa.array([SESSION] * n, pa.large_string()),
            "shank": pa.array(i % 200, pa.int64()),
            "event": pa.array(i, pa.int64()),
            "onset": pa.array(i * 0.1),
            "kind": _text(i % 4, "kind-%d"),
        }
    )


class Input:
    """An input: a small source of its kind describes it, its rows are made as sent."""

    def __init__(
        self,
        name: str,
        make: Callable[[int, int], pa.Table],
        rows: int,
        tags: list[str],
        key: list[str],
    ):
        self.make, self.rows, self.key, self.name = make, rows, key, name
        self.tags = tags
        self.stream = self.source(0, 4)

    def source(self, start: int, n: int) -> ArrowTableSource:
        return ArrowTableSource(
            self.make(start, n),
            tag_columns=self.tags,
            record_id_columns=self.key,
            tag_only_columns=True,
            source_id=self.name,
        )

    def batches(self) -> Iterator[pa.RecordBatch]:
        for start in range(0, self.rows, BATCH):
            stream: StreamProtocol = self.source(start, min(BATCH, self.rows - start))
            zero = StaticOutputOperatorPod._materialize_upstream([], self.stream)
            names = zero.as_table(columns={"source": True, "system_tags": True})
            table = getattr(stream, "as_carried_table", stream.as_table)(
                columns={"source": True, "system_tags": True}
            )
            yield from table.select(names.column_names).combine_chunks().to_batches()


async def run(case: str, rows: int, barrier: bool) -> tuple[int, float]:
    tags = {"many": "shank", "many_open": "burst"}.get(case, "unit")
    big = Input(
        "spikes",
        spikes,
        rows,
        ["session", tags, "spike"],
        ["session", "spike"]
        if case.startswith("many")
        else ["session", "unit", "spike"],
    )
    if case.startswith("one_to_one"):
        other = Input(
            "second",
            second,
            rows,
            ["spike", "unit", "session"],
            ["session", "unit", "spike"],
        )
    elif case == "many_open":
        other = Input(
            "marks", marks, rows, ["session", "burst", "mark"], ["session", "mark"]
        )
    elif case == "many":
        other = Input(
            "events", events, 1_000, ["session", "shank", "event"], ["session", "event"]
        )
    else:
        other = Input("units", units, 300, ["unit", "session"], ["session", "unit"])
    op = SemiJoin() if case == "semi" else Join()
    result = op(big.stream, other.stream)
    output = OutputChannel(result, retain=False, buffer_size=2)
    reader = output.open_reader()
    channels = [
        OutputChannel(i.stream, retain=False, buffer_size=2) for i in (big, other)
    ]
    readers = [channel.open_reader() for channel in channels]

    async def produce(index: int, source: Input, after: asyncio.Event | None) -> None:
        if after is not None:
            await after.wait()
        for batch in source.batches():
            await channels[index].send(batch)
            await asyncio.sleep(0)
        await channels[index].close()
        done[index].set()

    done = [asyncio.Event(), asyncio.Event()]
    first = {"lookup": 1, "many": 1, "semi": 1}.get(case)
    waits: list[asyncio.Event | None] = [None, None]
    if first is not None:
        waits[1 - first] = done[first]
    if case.endswith("late"):
        waits[1] = done[0]
    count = 0

    async def consume() -> None:
        nonlocal count
        async for batch in reader:
            count += batch.num_rows

    if barrier:
        execute = StaticOutputOperatorPod.async_execute_batches(op, readers, output)
    else:
        execute = op.async_execute_batches(readers, output)
    began = time.perf_counter()
    await asyncio.gather(
        produce(0, big, waits[0]), produce(1, other, waits[1]), consume(), execute
    )
    return count, time.perf_counter() - began


if __name__ == "__main__":
    warnings.simplefilter("ignore")
    case, rows = sys.argv[1], int(sys.argv[2])
    barrier = "barrier" in sys.argv
    base = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    count, seconds = asyncio.run(run(case, rows, barrier))
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    unit = 2**20 if sys.platform == "darwin" else 2**10
    print(
        f"{case:16} rows={rows:>9,} {'barrier' if barrier else 'keys':8}"
        + f" out={count:>11,} {seconds:7.1f} s"
        + f" peak={peak / unit:8.0f} MiB (import {base / unit:.0f})"
        + f" arrow_peak={(pa.default_memory_pool().max_memory() or 0) / 2**20:8.0f} MiB"
    )
