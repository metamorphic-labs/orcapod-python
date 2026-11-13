"""Streams outside a job keep each derived column with its own row (ITL-1034).

Paths: a source's ``ArrowTableStream`` (record-id tokens and positional tokens), and a
function pod's ``FunctionPodStream``, computed row by row or concurrently, with the gate
forcing every completion order on the concurrent path. Each is read in four patterns (a
fresh read; a partial ``iter_data`` then a full read; a repeated read; two interleaved
iterations) and its ``as_table`` and ``head`` are checked under every column configuration,
for every tie scenario and input order. Every read must also give every row exactly once.

Known defects are marked ``xfail(strict=True)`` on exactly the cases they break:

* ITL-1045: ``FunctionPodStream.as_table`` builds the table and its content hashes in two
  passes; an upstream reset in between attaches the new rows' hashes to the old table.
* ITL-1046: ``Index`` after ``Batch`` or ``GroupBy`` gives the element the whole member
  list's token instead of its own.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable
from typing import Any

import pytest

from orcapod import function_pod
from orcapod.core.executors import LocalPythonFunctionExecutor
from orcapod.core.operators import Batch, GroupBy, Index
from orcapod.system_constants import constants

from .checker import Layout, assert_rows_aligned
from .fixtures import (
    COLUMN_CONFIGS,
    INPUT_ORDERS,
    SCENARIOS,
    Sources,
    content_hash_column,
    keys_a,
    make_sources,
)
from .gate import GATE, ORDERS, new_gated_scale_pod
from .jobs import derive_y

ITL_1045 = pytest.mark.xfail(
    strict=True,
    reason="ITL-1045: as_table attaches a recomputed upstream's hashes to the old table",
)
ITL_1046 = pytest.mark.xfail(
    strict=True,
    reason="ITL-1046: Index after Batch/GroupBy gives the element the list's token",
)


class ConcurrentLocalExecutor(LocalPythonFunctionExecutor):
    """The local executor, declared concurrent: the stream computes rows together."""

    @property
    def supports_concurrent_execution(self) -> bool:
        """Whether rows may be computed concurrently: yes."""
        return True


# A stream kind: builds the stream from the sources.
StreamBuilder = Callable[[Sources], Any]


def source_a(sources: Sources) -> Any:
    """Source A: record ids from ``record_id_column``."""
    return sources.a


def source_c(sources: Sources) -> Any:
    """Source C: positional record ids."""
    return sources.c


def pod_row_by_row(sources: Sources) -> Any:
    """``gated_scale`` over A, computed one row at a time (the gate is disarmed)."""
    derive_y(sources)
    return new_gated_scale_pod()(sources.a)


def pod_concurrent(sources: Sources) -> Any:
    """``gated_scale`` over A, computed concurrently; the gate forces the order."""
    derive_y(sources)
    stream = new_gated_scale_pod()(sources.a)
    stream.executor = ConcurrentLocalExecutor()
    return stream


KINDS: dict[str, StreamBuilder] = {
    "source_record_id": source_a,
    "source_positional": source_c,
    "pod_row_by_row": pod_row_by_row,
    "pod_concurrent": pod_concurrent,
}
PATTERNS = ("fresh", "partial_then_full", "repeated", "interleaved")


def stream_params() -> list[Any]:
    """Every stream kind, completion order and read pattern."""
    params = []
    for kind in KINDS:
        orders = list(ORDERS) if kind == "pod_concurrent" else [None]
        for order in orders:
            for pattern in PATTERNS:
                name = "-".join(p for p in (kind, order, pattern) if p)
                params.append(pytest.param(kind, order, pattern, id=name))
    return params


def read(stream: Any, pattern: str) -> list[Any]:
    """Read ``stream`` in ``pattern``; return the rows of the last full read."""
    if pattern == "partial_then_full":
        partial = iter(stream.iter_data())
        next(partial)
        next(partial)
        del partial
        return list(stream.iter_data())
    if pattern == "repeated":
        list(stream.iter_data())
        stream.as_table(all_info=True)
        return list(stream.iter_data())
    if pattern == "interleaved":
        first = iter(stream.iter_data())
        second = iter(stream.iter_data())
        seen = [next(first)]
        list(itertools.islice(second, 3))
        seen += list(first)
        list(second)
        return seen
    return list(stream.iter_data())


@pytest.mark.parametrize(("kind", "order", "pattern"), stream_params())
@pytest.mark.parametrize("input_order", INPUT_ORDERS)
@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_every_row_of_a_stream_is_aligned(
    scenario: str, input_order: str, kind: str, order: str | None, pattern: str
) -> None:
    sources = make_sources(scenario, input_order)
    stream = KINDS[kind](sources)
    keys = keys_a(scenario, input_order)
    if order is None:
        GATE.disarm()
    else:
        GATE.arm(ORDERS[order](keys))
    try:
        rows = read(stream, pattern)
        realised = list(GATE.completed)
    finally:
        GATE.disarm()

    assert len(rows) == len(keys), "a read lost or repeated rows"
    assert stream.as_table().num_rows == len(keys)
    if order is not None:
        assert realised == ORDERS[order](keys), "the gate did not realise the order"

    tags, data = stream.keys()
    failures = []
    for name, config in COLUMN_CONFIGS.items():
        layout = Layout(
            tag_keys=tuple(tags),
            data_keys=tuple(data),
            content_hash_column=content_hash_column(config),
        )
        tables = [("as_table", stream.as_table(**config))]
        if not config.get("columns", {}).get("sort_by_tags"):
            tables.append(("head", stream.head(100, **config).as_table()))
        for call, table in tables:
            try:
                assert_rows_aligned(table, layout, sources.oracle, stream.data_context)
            except AssertionError as exc:
                failures.append(f"{call} [{name}] {exc}")
    assert not failures, "\n".join(failures)


# ---------------------------------------------------------------------------
# Filed defects, each with its exact case
# ---------------------------------------------------------------------------


@ITL_1045
def test_as_table_after_an_upstream_reset_keeps_each_hash_with_its_row() -> None:
    """An upstream reset between two ``as_table`` calls must not mix two computations."""
    counter = itertools.count()

    @function_pod(output_keys="y")
    def stamp(x: int) -> int:
        """Not deterministic: a call counter (a timestamp or a random draw alike)."""
        return 100 * next(counter) + x

    @function_pod(output_keys="z")
    def plus_one(y: int) -> int:
        """Return ``y + 1``."""
        return y + 1

    sources = make_sources("unique")
    upstream: Any = stamp.pod(sources.a.select_data_columns(["x"]))
    stream: Any = plus_one.pod(upstream)
    stream.as_table()
    upstream.clear_cache()
    table = stream.as_table(columns={"content_hash": True})
    hasher = stream.data_context.arrow_hasher
    own = [
        hasher.hash_table(table.slice(i, 1).select(["z"])).to_string()
        for i in range(table.num_rows)
    ]
    assert table.column("_content_hash").to_pylist() == own


@ITL_1046
@pytest.mark.parametrize("aggregate", ["batch", "group_by"])
def test_index_after_aggregation_gives_each_element_its_own_token(
    aggregate: str,
) -> None:
    """The element ``Index`` takes from a member list carries that member's token."""
    sources = make_sources("duplicate")
    members = (
        Batch(2)(sources.a) if aggregate == "batch" else GroupBy(by=["g"])(sources.a)
    )
    tokens = members.as_table(columns={"source": True}).column("_source_x").to_pylist()
    first = Index("x", 0, out="first")(members)
    table = first.as_table(columns={"source": True})
    expected = [member_tokens[0] if member_tokens else None for member_tokens in tokens]
    assert table.column(f"{constants.SOURCE_PREFIX}first").to_pylist() == expected
