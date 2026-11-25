"""Random graphs: a certificate never changes a result, and every certificate holds.

ITL-1464: a join on the async path lets a row go when the other side is certified unique on
the keys (``core/unique_keys``), so a wrong certificate would lose rows. Random graphs of
sources (with and without unique tags, with and without nulls), operators and joins, with
shared upstreams, are run three ways and compared as sets of rows: on the sync path; on
record-batch channels, every operator in its own task, the batches cut and the spools
filled at random; and the same with certificates ignored. Every stream's certificate is also
checked on its rows directly, with the joins' equality.

``python -m tests.test_core.operators.test_certificate_graphs FIRST COUNT`` runs more seeds.
"""

from __future__ import annotations

import asyncio
import functools
import random
import sys
import warnings
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import numpy as np
import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPodStream, function_pod
from orcapod.core.operators import MergeJoin, Rejoin
from orcapod.core.operators import streaming_joins
from orcapod.core.operators.arrow_keys import polars_key_frame
from orcapod.core.operators.static_output_pod import DynamicPodStream
from orcapod.core.record_batch_channels import OutputChannel
from orcapod.core.row_spool import RowSpool
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams.carried import carried_batches
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol

from .test_streaming_joins import _rows, _transport  # pyright: ignore[reportPrivateUsage]  # the one oracle

SEEDS = range(80)
TIMEOUT = 60.0
MAX_ROWS = 400

TAGS = {
    "a": pa.int64(),
    "b": pa.large_string(),
    "c": pa.int64(),
    "f": pa.float64(),
}
POOLS: dict[str, list[Any]] = {
    "a": [1, 2, 3],
    "b": ["x", "y", "a longer string than a view holds"],
    "c": [0, 1, None],
    "f": [0.0, -0.0, float("nan"), float("-nan"), 1.5],
}


def _source(rng: random.Random, name: str) -> Any:
    """A source with two to four tags, two data columns, and perhaps checked keys."""
    tags = rng.sample(sorted(TAGS), rng.randint(2, 4))
    n = rng.randint(1, 12)
    rows = [tuple(rng.choice(POOLS[t]) for t in tags) for _ in range(n)]
    key: list[str] | None = None
    candidates = [t for t in tags if t in ("a", "b")]
    if candidates and rng.random() < 0.6:
        # Key columns hold each value combination once.
        key = rng.sample(candidates, rng.randint(1, len(candidates)))
        seen: set[Any] = set()
        kept: list[Any] = []
        for row in rows:
            value = tuple(row[tags.index(t)] for t in key)
            if value not in seen:
                seen.add(value)
                kept.append(row)
        rows = kept
    columns: dict[str, Any] = {
        t: pa.array([row[i] for row in rows], TAGS[t]) for i, t in enumerate(tags)
    }
    columns["x"] = pa.array([f"{name}{i}" for i in range(len(rows))], pa.large_string())
    columns["y"] = pa.array([None if i % 3 == 1 else i for i in range(len(rows))])
    names = rng.sample(sorted(columns), len(columns))
    return ArrowTableSource(
        pa.table({c: columns[c] for c in names}),
        tag_columns=rng.sample(tags, len(tags)),
        record_id_columns=key,
        tag_only_columns=rng.random() < 0.8,
        source_id=name,
    )


@function_pod(output_keys=["x", "y"])
def relabel(x: str | None, y: int | None) -> tuple[str | None, int | None]:
    """A function pod over the two data columns."""
    return (x or "") + "!", y


class _Graphs:
    """Streams made at random from earlier ones.

    Args:
        rng: What decides.
    """

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.made = 0

    def fresh(self, stream: Any) -> Any:
        """``stream`` with its plain data columns renamed, so that no join collides."""
        self.made += 1
        tags, data = stream.keys()
        renames = {d: f"{d[:1]}{self.made}" for d in data if d not in tags}
        return stream.map_data(renames) if renames else stream

    def unary(self, stream: Any) -> Any:
        rng = self.rng
        tags, data = stream.keys()
        plain = [d for d in data if d not in tags]
        some_tags = rng.sample(tags, rng.randint(1, len(tags)))
        steps = [
            lambda: stream.select_tag_columns(some_tags),
            lambda: stream.drop_tag_columns([rng.choice(tags)]),
            lambda: stream.map_tags(
                {t: {"a": "c", "c": "a"}.get(t, t) for t in some_tags if t in "ac"}
            ),
            lambda: stream.map_tags({rng.choice(tags): "z"}),
            lambda: stream.map_tags({some_tags[0]: "z"}, drop_unmapped=True),
            lambda: stream.add_tags(a=1) if "a" not in tags else stream.add_tags(k=2),
            lambda: stream.drop_nulls(),
            lambda: stream.drop_nulls(rng.choice([*tags, *plain])),
            lambda: stream.filter({rng.choice(tags): 1}),
            *[lambda: stream.group_by(some_tags)] * 3,
            lambda: stream.drop_nulls().tag_by(rng.choice(plain)),
            lambda: stream.select_columns([*some_tags, *plain]),
            lambda: stream.drop_columns([some_tags[0]]),
            lambda: stream.map_columns({some_tags[0]: "w"}),
            lambda: relabel.pod(stream) if set(plain) == {"x", "y"} else stream,
        ]
        return rng.choice(steps)()

    def binary(self, left: Any, right: Any, third: Any) -> Any:
        rng = self.rng
        kind = rng.choice(["join", "join", "join3", "semi", "merge", "rejoin"])
        if kind == "semi":
            return left.semi_join(right)
        if kind == "rejoin":
            plain = [d for d in left.keys()[1] if d not in left.keys()[0]]
            half = max(1, len(plain) // 2)
            branch = left.select_data_columns(plain[half:])
            if rng.random() < 0.5:
                branch = branch.drop_nulls()
            return Rejoin()(left.select_data_columns(plain[:half]), branch)
        if kind == "merge":
            return MergeJoin()(left, right)
        if kind == "join3":
            return self.fresh(left).join(self.fresh(right), self.fresh(third))
        return self.fresh(left).join(self.fresh(right))

    def graph(self) -> Any:
        """The last stream of a random graph."""
        rng = self.rng
        streams: list[Any] = [_source(rng, f"s{i}") for i in range(rng.randint(2, 4))]
        if rng.random() < 0.4:
            streams.append(relabel.pod(rng.choice(streams)))
        for _ in range(rng.randint(3, 8)):
            try:
                if rng.random() < 0.5:
                    made = self.unary(rng.choice(streams[-3:]))
                else:
                    made = self.binary(*(rng.choice(streams) for _ in range(3)))
                if not 0 < made.as_table().num_rows <= MAX_ROWS:
                    continue
            except (InputValidationError, ValueError, KeyError, NotImplementedError):
                continue  # not a valid step here
            streams.append(made)
        return streams[-1]


def _is_step(stream: Any) -> bool:
    """Whether a stream is an operator's result (run over channels here)."""
    return isinstance(stream, DynamicPodStream)


def _reached(stream: Any, found: dict[int, Any]) -> Iterator[Any]:
    """Every stream ``stream`` is made of, each once, its inputs first."""
    if id(stream) in found:
        return
    found[id(stream)] = stream
    if _is_step(stream) or isinstance(stream, FunctionPodStream):
        for upstream in stream.upstreams:
            yield from _reached(upstream, found)
    yield stream


async def _run(stream: Any, rng: random.Random) -> StreamProtocol:
    """Run a graph over bounded channels, one task per stream, every batch cut at random."""
    streams = list(_reached(stream, {}))
    channels = {
        id(s): OutputChannel(s, retain=s is stream, buffer_size=rng.randint(1, 3))
        for s in streams
    }
    readers = {
        id(s): [channels[id(u)].open_reader() for u in s.upstreams]
        for s in streams
        if _is_step(s)
    }
    fed = {id(u) for s in streams if _is_step(s) for u in s.upstreams}

    async def give(source: Any) -> None:
        output = channels[id(source)]
        async for batch in carried_batches(source.open_channel()):
            start = 0
            while start < batch.num_rows:
                size = rng.randint(1, 5)
                await output.send(batch.slice(start, size))
                start += size
                if rng.random() < 0.3:
                    await asyncio.sleep(0)
        await output.close()

    tasks = [
        s.producer.async_execute_batches(readers[id(s)], channels[id(s)])
        if _is_step(s)
        else give(s)
        for s in streams
        if _is_step(s) or id(s) in fed
    ]
    rng.shuffle(tasks)
    _ = await asyncio.wait_for(asyncio.gather(*tasks), TIMEOUT)
    return channels[id(stream)].result()


def _async_rows(stream: Any, seed: int, root: Path, *, certified: bool) -> Counter[Any]:
    """The rows of a graph run over channels."""
    rng = random.Random(seed)
    patches: dict[str, Any] = {
        "RowSpool": functools.partial(
            RowSpool, rng.choice([0, 2_000, 10**9]), str(root)
        ),
        "PUSH_ROWS": rng.choice([1, 4, 64]),
        "PAIRS_PER_BATCH": rng.choice([1, 7, 64]),
        "_COMPACT_ROWS": rng.choice([1, 8]),
    }
    if not certified:
        patches["unique_on"] = lambda keys, columns: False
    with mock.patch.multiple(streaming_joins, **patches):
        return _rows(_transport(asyncio.run(_run(stream, rng))))


def _repeats(stream: Any, key: frozenset[str]) -> bool:
    """Whether two rows agree on ``key`` under the joins' equality (a null is none's)."""
    table = stream.as_table().select(sorted(key))
    whole = np.ones(table.num_rows, dtype=np.bool_)
    for column in table.columns:
        whole &= np.asarray(column.is_valid())
    frame = polars_key_frame(table.filter(pa.array(whole)), sorted(key))
    return frame.height != frame.unique().height


def check_seed(seed: int, root: Path) -> tuple[int, bool]:
    """Check one random graph.

    Returns:
        How many joins of it had a certified side, and whether it is a graph to run.
    """
    stream = _Graphs(random.Random(seed)).graph()
    reached = list(_reached(stream, {}))
    for part in reached:
        for key in part.unique_keys:
            assert not _repeats(part, key), (seed, part, key)
    if not _is_step(stream):
        return 0, False
    expected = _rows(_transport(stream))
    used: list[bool] = []
    original = streaming_joins.unique_on

    def counted(keys: Any, columns: Any) -> bool:
        used.append(original(keys, columns))
        return used[-1]

    with mock.patch.object(streaming_joins, "unique_on", counted):
        assert _async_rows(stream, seed, root, certified=True) == expected, seed
    assert _async_rows(stream, seed + 1, root, certified=True) == expected, seed
    assert _async_rows(stream, seed, root, certified=False) == expected, seed
    assert list(root.rglob("*.arrow")) == [], seed
    return sum(used), True


@pytest.mark.filterwarnings("ignore")
@pytest.mark.parametrize("seed", SEEDS)
def test_a_certificate_changes_no_result(seed: int, tmp_path: Path) -> None:
    _ = check_seed(seed, tmp_path)


@pytest.mark.filterwarnings("ignore")
def test_the_graphs_use_certificates_and_every_kind_of_step(tmp_path: Path) -> None:
    """The seeds above reach what they are for."""
    steps: Counter[str] = Counter()
    certified = 0
    for seed in SEEDS:
        if seed < 30:
            used, _ = check_seed(seed, tmp_path)
            certified += used
        for part in _reached(_Graphs(random.Random(seed)).graph(), {}):
            kind = part.producer if part.producer is not None else part
            steps[type(kind).__name__] += 1
    assert certified >= 8
    for name in (
        "Join",
        "SemiJoin",
        "MergeJoin",
        "Rejoin",
        "GroupBy",
        "MapTags",
        "FunctionPod",
    ):
        assert steps[name] >= 2, (name, steps)


def test_a_wrong_certificate_is_seen(tmp_path: Path) -> None:
    """The check itself: with every stream certified unique, some graph loses rows."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for seed in SEEDS:
            stream = _Graphs(random.Random(seed)).graph()
            if not _is_step(stream):
                continue
            expected = _rows(_transport(stream))
            with mock.patch.object(streaming_joins, "unique_on", lambda *_: True):
                if _async_rows(stream, seed, tmp_path, certified=True) != expected:
                    return
    raise AssertionError("no graph tells a wrong certificate")


if __name__ == "__main__":
    import tempfile

    warnings.simplefilter("ignore")
    first, count = int(sys.argv[1]), int(sys.argv[2])
    ran = certified = failed = 0
    with tempfile.TemporaryDirectory() as directory:
        for seed in range(first, first + count):
            try:
                used, is_run = check_seed(seed, Path(directory))
            except Exception as error:  # noqa: BLE001  # report and go on
                failed += 1
                print("seed", seed, type(error).__name__, str(error)[:300], flush=True)
                continue
            ran, certified = ran + is_run, certified + (used > 0)
    print(
        f"{count} seeds, {ran} graphs run, {certified} used a certificate, {failed} failed"
    )
