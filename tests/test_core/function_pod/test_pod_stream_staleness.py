"""A chain of function pod streams is not stale after a full read (ITL-1052).

A stream stamps its modified time when its results are complete (its input read to the end),
before it ends its own iterator, the same rule as a job node (ITL-1042). An upstream in a
chain finishes before its downstream sees the end, so after a full read every stream is at
least as new as its upstreams: a second read (``iter_data``, ``as_table`` or ``head``) calls
no function and reads no store. A real change upstream (``clear_cache()``) still makes the
downstream stale, and its next read recomputes.
"""

from __future__ import annotations

import contextlib
from collections import Counter
from collections.abc import Callable, Iterator
from typing import Any
from unittest import mock

import pyarrow as pa
import pytest

from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.executors.base import PythonFunctionExecutorBase
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import FunctionPodProtocol, StreamProtocol
from orcapod.streams import HeadStatus

# Rows of the source, not in tag order.
IDS = [3, 1, 4, 0, 2]

# The read methods of the store protocol, counted on every view of an in-memory store.
STORE_READS = (
    "get_all_records",
    "get_records_with_column_value",
    "get_records_by_ids",
    "get_record_by_id",
)

CALLS: Counter[str] = Counter()


def plus_one(x: int) -> int:
    """Return ``x + 1``; counted."""
    CALLS["plus_one"] += 1
    return x + 1


def double(y: int) -> int:
    """Return ``2 * y``; counted."""
    CALLS["double"] += 1
    return 2 * y


def minus_three(z: int) -> int:
    """Return ``z - 3``; counted."""
    CALLS["minus_three"] += 1
    return z - 3


# (function, input name, output name) of each link of a chain, in order.
LINKS: list[tuple[Callable[[int], int], str, str]] = [
    (plus_one, "x", "y"),
    (double, "y", "z"),
    (minus_three, "z", "w"),
]


class _ConcurrentExecutor(PythonFunctionExecutorBase):
    """An executor that asks for the concurrent read path; it runs functions inline."""

    @property
    def executor_type_id(self) -> str:
        """The executor's id."""
        return "test-concurrent"

    def supported_function_type_ids(self) -> frozenset[str]:
        """Every function type."""
        return frozenset()

    @property
    def supports_concurrent_execution(self) -> bool:
        """Take the concurrent read path."""
        return True

    def execute_callable(
        self, fn: Callable[..., Any], kwargs: dict[str, Any], *args: Any, **kw: Any
    ) -> Any:
        """Call ``fn``."""
        return fn(**kwargs)

    async def async_execute_callable(
        self, fn: Callable[..., Any], kwargs: dict[str, Any], *args: Any, **kw: Any
    ) -> Any:
        """Call ``fn``."""
        return fn(**kwargs)


@pytest.fixture(autouse=True)
def _reset_calls() -> Iterator[None]:
    """Clear the call record around each test."""
    CALLS.clear()
    yield
    CALLS.clear()


def _source() -> ArrowTableSource:
    """A source with tag ``id`` and data ``x``, rows not in tag order."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    )
    table = pa.Table.from_arrays(
        [pa.array(IDS, type=pa.int64()), pa.array([10 * i for i in IDS], pa.int64())],
        schema=schema,
    )
    return ArrowTableSource(table, tag_columns=["id"], source_id="src")


def _pod(
    link: int, executor: str, store: InMemoryArrowDatabase | None
) -> FunctionPodProtocol:
    """The pod of link ``link``; with ``store``, a cached pod storing its results there."""
    function, _, output = LINKS[link]
    data_function = PythonDataFunction(
        function,
        output_keys=output,
        executor=_ConcurrentExecutor() if executor == "concurrent" else None,
    )
    pod = FunctionPod(data_function)
    if store is None:
        return pod
    return CachedFunctionPod(pod, result_database=store)


def _chain(
    length: int, executor: str, store: InMemoryArrowDatabase | None = None
) -> list[StreamProtocol]:
    """The streams of a chain of ``length`` pods over the source, upstream first."""
    streams: list[StreamProtocol] = []
    stream: StreamProtocol = _source()
    for link in range(length):
        stream = _pod(link, executor, store).process(stream)
        streams.append(stream)
    return streams


def _read(stream: StreamProtocol, how: str) -> int:
    """Read ``stream`` with ``how`` and return the number of rows read."""
    if how == "iter_data":
        return len(list(stream.iter_data()))
    if how == "as_table":
        return stream.as_table().num_rows
    head = stream.head(100)
    assert head.status is HeadStatus.ROWS, head.header()
    return head.table.num_rows


@contextlib.contextmanager
def _counting_store_reads() -> Iterator[list[str]]:
    """Count reads of every in-memory store while the block runs."""
    reads: list[str] = []

    def counting(name: str) -> Callable[..., Any]:
        original = getattr(InMemoryArrowDatabase, name)

        def read(self: Any, *args: Any, **kwargs: Any) -> Any:
            reads.append(name)
            return original(self, *args, **kwargs)

        return read

    with contextlib.ExitStack() as patches:
        for name in STORE_READS:
            patches.enter_context(
                mock.patch.object(InMemoryArrowDatabase, name, counting(name))
            )
        yield reads


def _stale(streams: list[StreamProtocol]) -> list[bool]:
    """Whether each stream is stale."""
    return [bool(getattr(stream, "is_stale")) for stream in streams]


@pytest.mark.parametrize("how", ["iter_data", "as_table", "head"])
@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
@pytest.mark.parametrize("length", [2, 3])
def test_a_second_read_of_a_chain_calls_no_function(
    length: int, executor: str, how: str
) -> None:
    """After a full read no stream of the chain is stale; reading again computes nothing."""
    streams = _chain(length, executor)
    assert len(list(streams[-1].iter_data())) == len(IDS)
    assert sum(CALLS.values()) == length * len(IDS)
    assert _stale(streams) == [False] * length
    CALLS.clear()
    assert _read(streams[-1], how) == len(IDS)
    assert CALLS == Counter()
    assert _stale(streams) == [False] * length


@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
def test_a_chain_first_read_as_a_table_is_not_read_again(executor: str) -> None:
    """A first read through ``as_table`` leaves the chain current too."""
    streams = _chain(3, executor)
    first = streams[-1].as_table()
    CALLS.clear()
    assert streams[-1].as_table().equals(first)
    assert len(list(streams[-1].iter_data())) == len(IDS)
    assert streams[-1].head(100).table.num_rows == len(IDS)
    assert CALLS == Counter()


@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
def test_a_chain_through_an_operator_is_not_read_again(executor: str) -> None:
    """A pod after a join of a pod stream reads its input once."""
    first = _pod(0, executor, None).process(_source())
    joined = Join()(first, _source())
    last = _pod(1, executor, None).process(joined)
    assert len(list(last.iter_data())) == len(IDS)
    CALLS.clear()
    for how in ("iter_data", "as_table", "head"):
        assert _read(last, how) == len(IDS)
    assert CALLS == Counter()
    assert _stale([first, joined, last]) == [False, False, False]


@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
@pytest.mark.parametrize("length", [2, 3])
def test_a_second_read_of_a_cached_chain_reads_no_store(
    length: int, executor: str
) -> None:
    """A chain of cached pods is read again from memory, without store lookups."""
    streams = _chain(length, executor, InMemoryArrowDatabase())
    assert len(list(streams[-1].iter_data())) == len(IDS)
    assert sum(CALLS.values()) == length * len(IDS)
    CALLS.clear()
    with _counting_store_reads() as reads:
        for how in ("iter_data", "as_table", "head"):
            assert _read(streams[-1], how) == len(IDS)
    assert reads == []
    assert CALLS == Counter()
    assert _stale(streams) == [False] * length


@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
@pytest.mark.parametrize("length", [2, 3])
def test_a_reset_upstream_still_recomputes_the_chain(
    length: int, executor: str
) -> None:
    """``clear_cache()`` on the first stream makes the rest stale; the next read recomputes.

    The read is ``iter_data``: ``as_table`` of a stale pod stream still returns its kept
    table, which is ITL-1045.
    """
    streams = _chain(length, executor)
    expected = [data.as_dict() for _, data in streams[-1].iter_data()]
    clear_cache = getattr(streams[0], "clear_cache")
    clear_cache()
    assert _stale(streams[1:]) == [True] * (length - 1)
    CALLS.clear()
    assert [data.as_dict() for _, data in streams[-1].iter_data()] == expected
    assert sum(CALLS.values()) == length * len(IDS)
    assert _stale(streams) == [False] * length
