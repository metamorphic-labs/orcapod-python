"""Interleaved and resumed reads of a function pod stream compute each row once (ITL-1065).

Two iterators open on one pod stream share its results: a row one of them computed is the
row the other yields, and a later full read yields the same datagrams. An upstream that
completes (or, for an operator stream, computes) for the first time while the stream is
being read is not a change of the stream's input, whatever the upstream's kind: a source, an
operator stream (the automatic column selection of ``pod(stream)``), a pod stream, or a pod
stream over an operator stream. A real reset of the input between two reads is still a
change: the stream's next read starts again, and every later read is of one state.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from typing import cast

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol

from .test_pod_stream_staleness import _ConcurrentExecutor

# Rows of the source, not in tag order.
IDS = [3, 1, 4, 0, 2, 5]

CALLS: Counter[str] = Counter()
EPOCH = [0]

UPSTREAMS = ["source", "operator", "pod_stream", "pod_over_operator"]


def f(x: int) -> int:
    """Return ``x + 1``; counted."""
    CALLS["f"] += 1
    return x + 1


def g(y: int) -> int:
    """Return ``2 * y``; counted."""
    CALLS["g"] += 1
    return 2 * y


def stamped(x: int) -> int:
    """Return ``x`` tagged with the current epoch: a function whose values change."""
    CALLS["stamped"] += 1
    return 1000 * EPOCH[0] + x


@pytest.fixture(autouse=True)
def _reset() -> Iterator[None]:
    """Clear the call record and the epoch around each test."""
    CALLS.clear()
    EPOCH[0] = 0
    yield
    CALLS.clear()
    EPOCH[0] = 0


def _source(extra: bool) -> ArrowTableSource:
    """A source with tag ``id`` and data ``x``; with ``extra``, a data column ``e`` too.

    A pod taking only ``x`` called on the source with ``e`` puts an automatic
    ``SelectDataColumns`` operator stream in front of itself.
    """
    fields = [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
    ]
    arrays = [pa.array(IDS, pa.int64()), pa.array([10 * i for i in IDS], pa.int64())]
    if extra:
        fields.append(pa.field("e", pa.int64(), nullable=False))
        arrays.append(pa.array([-i for i in IDS], pa.int64()))
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    return ArrowTableSource(table, tag_columns=["id"], source_id="src")


def _other() -> ArrowTableSource:
    """A second source with tag ``id`` and data ``q``, in another row order."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("q", pa.int64(), nullable=False),
        ]
    )
    ids = sorted(IDS)
    table = pa.Table.from_arrays(
        [pa.array(ids, pa.int64()), pa.array([7 * i for i in ids], pa.int64())],
        schema=schema,
    )
    return ArrowTableSource(table, tag_columns=["id"], source_id="other")


def _pod(
    function: object, output: str, executor: str, cached: bool
) -> FunctionPod | CachedFunctionPod:
    """The pod of ``function``; when ``cached``, storing its results in a fresh store."""
    pod = function_pod(
        output_keys=output,
        executor=_ConcurrentExecutor() if executor == "concurrent" else None,
    )(function).pod
    assert isinstance(pod, FunctionPod)
    if not cached:
        return pod
    return CachedFunctionPod(pod, result_database=InMemoryArrowDatabase())


def _stream(upstream: str, executor: str, cached: bool) -> StreamProtocol:
    """The pod stream read by the tests, over an input of kind ``upstream``."""
    if upstream == "source":
        return _pod(f, "y", executor, cached)(_source(extra=False))
    if upstream == "operator":
        return _pod(f, "y", executor, cached)(_source(extra=True))
    first = _pod(f, "y", executor, cached)(
        _source(extra=upstream == "pod_over_operator")
    )
    return _pod(g, "z", executor, cached)(first)


def _expected_calls(upstream: str) -> Counter[str]:
    """Each function of the stream called once per input row."""
    if upstream in ("source", "operator"):
        return Counter({"f": len(IDS)})
    return Counter({"f": len(IDS), "g": len(IDS)})


Row = tuple[TagProtocol, DataProtocol]


def _same_datagrams(left: list[Row], right: list[Row]) -> bool:
    """Whether two reads yielded the very same data datagrams, in the same order."""
    return len(left) == len(right) and all(
        a is b for (_, a), (_, b) in zip(left, right, strict=True)
    )


def _stale(stream: StreamProtocol) -> bool:
    """Whether ``stream`` is stale (``is_stale`` is not on ``StreamProtocol``)."""
    return bool(getattr(stream, "is_stale"))


@pytest.mark.parametrize("cached", [False, True], ids=["plain", "cached"])
@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
@pytest.mark.parametrize("upstream", UPSTREAMS)
def test_interleaved_reads_compute_each_row_once(
    upstream: str, executor: str, cached: bool
) -> None:
    """Two interleaved reads and a later full read share one computation of every row."""
    stream = _stream(upstream, executor, cached)
    first = iter(stream.iter_data())
    second = iter(stream.iter_data())
    first_rows = [next(first)]
    second_rows = [next(second) for _ in range(3)]
    first_rows += list(first)
    second_rows += list(second)
    assert CALLS == _expected_calls(upstream)
    assert not _stale(stream)

    third_rows = list(stream.iter_data())
    assert CALLS == _expected_calls(upstream)
    assert len(third_rows) == len(IDS)
    assert _same_datagrams(first_rows, third_rows)
    assert _same_datagrams(second_rows, third_rows)


@pytest.mark.parametrize("cached", [False, True], ids=["plain", "cached"])
@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
@pytest.mark.parametrize("upstream", UPSTREAMS)
def test_a_read_after_an_abandoned_partial_read_computes_each_row_once(
    upstream: str, executor: str, cached: bool
) -> None:
    """A read left after one row is resumed by the next read, not started again."""
    stream = _stream(upstream, executor, cached)
    partial = iter(stream.iter_data())
    first_row = next(partial)
    del partial
    rows = list(stream.iter_data())
    assert CALLS == _expected_calls(upstream)
    assert len(rows) == len(IDS)
    assert rows[0][1] is first_row[1]
    assert not _stale(stream)


def _values(stream: StreamProtocol, column: str) -> dict[int, int]:
    """Each row's ``column`` value, keyed by its ``id`` tag, read with ``iter_data``."""
    return {
        cast(int, tag.as_dict()["id"]): cast(int, data.as_dict()[column])
        for tag, data in stream.iter_data()
    }


@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
@pytest.mark.parametrize("through", ["pod_stream", "operator"])
def test_a_reset_between_interleaved_reads_gives_one_state(
    through: str, executor: str
) -> None:
    """A reset of the input between two interleaved reads is a change of content.

    The stamped stream is reset after the first read took one row. The second read then
    starts again from the new state, and once both reads are drained the stream is current
    and every read of it is of the new state only.
    """
    stamped_stream = _pod(stamped, "y", executor, cached=False)(_source(extra=False))
    if through == "pod_stream":
        input_stream: StreamProtocol = stamped_stream
    else:
        # A join adds data column ``q``, so the pod call adds an automatic selection.
        input_stream = Join()(stamped_stream, _other())
    last = _pod(g, "z", executor, cached=False)(input_stream)

    first = iter(last.iter_data())
    second = iter(last.iter_data())
    _ = next(first)
    EPOCH[0] = 1
    clear_cache = getattr(stamped_stream, "clear_cache")
    clear_cache()
    second_rows = [next(second) for _ in range(3)]
    _ = list(first)
    second_rows += list(second)
    assert not _stale(last)

    CALLS.clear()
    third_rows = list(last.iter_data())
    assert CALLS == Counter()
    assert _same_datagrams(second_rows, third_rows)
    zs = _values(last, "z")
    ys = _values(stamped_stream, "y")
    assert CALLS == Counter()
    assert sorted(zs) == sorted(ys) == sorted(IDS)
    assert {y // 1000 for y in ys.values()} == {1}
    for key, y in ys.items():
        assert zs[key] == 2 * y, key
