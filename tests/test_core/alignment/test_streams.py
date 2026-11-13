"""Streams outside a job keep each derived column with its own row (ITL-1034).

Paths: a source's ``ArrowTableStream`` (record-id tokens and positional tokens), a
function pod's ``FunctionPodStream`` computed row by row or concurrently (the gate forcing
every completion order on the concurrent path), a ``SideEffectPod``'s pass-through stream,
and a ``DerivedSource`` with ``tags_as_data`` (a new root with fresh provenance). Every
table must also hold exactly the expected rows (``expectations.py``). Each is read in four patterns (a
fresh read; a partial ``iter_data`` then a full read; a repeated read; two interleaved
iterations) and its ``as_table`` and ``head`` are checked under every column configuration,
for every tie scenario and input order. Every read must also give every row exactly once.

Known defects are marked ``xfail(strict=True)`` on exactly the cases they break:

* ITL-1066: ``SideEffectPodStream.as_table`` raises when asked for the context key (and so
  under ``all_info``), and ignores ``content_hash`` and ``sort_by_tags`` (only those
  configurations of the side-effect stream).
* ITL-1065: two interleaved iterations of a function pod stream behind an operator stream
  (the automatic select) recompute a row the first iteration already yielded (the
  row-by-row path, interleaved reads).

* ITL-1045: ``FunctionPodStream.as_table`` builds the table and its content hashes in two
  passes; an upstream reset in between attaches the new rows' hashes to the old table.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.executors import LocalPythonFunctionExecutor
from orcapod.core.operators import Batch, GroupBy, Index
from orcapod.core.sources import DerivedSource
from orcapod.side_effects import SideEffectPod, SideEffectPodStream
from orcapod.system_constants import constants

from .checker import (
    Layout,
    assert_requested_columns,
    assert_rows_aligned,
    rows_table,
)
from .fixtures import (
    COLUMN_CONFIGS,
    INPUT_ORDERS,
    SCENARIOS,
    TAG_COLUMNS,
    Sources,
    content_hash_column,
    keys_a,
    make_sources,
)
from .expectations import member as member_of
from .expectations import rows
from .gate import GATE, ORDERS, new_gated_scale_pod
from .oracle import Oracle
from .jobs import derive_y, stream_function_tokens

ITL_1065 = pytest.mark.xfail(
    strict=True,
    reason="ITL-1065: interleaved reads behind an operator stream recompute a row",
)
ITL_1066 = pytest.mark.xfail(
    strict=True,
    reason="ITL-1066: SideEffectPodStream.as_table raises on context, ignores "
    "content_hash and sort_by_tags",
)
# The configurations ITL-1066 breaks: context (and so all_info and every config with
# context) raises; content_hash is left out; sort_by_tags does not sort.
ITL_1066_CONFIGS = {
    "context",
    "all_groups_unsorted",
    "all_info",
    "content_hash",
    "content_hash_named",
    "content_hash_sorted",
    "sort_by_tags",
}
ITL_1045 = pytest.mark.xfail(
    strict=True,
    reason="ITL-1045: as_table attaches a recomputed upstream's hashes to the old table",
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


def side_effect_stream(sources: Sources) -> Any:
    """A ``SideEffectPod`` over A: a pass-through stream that delivers each row."""

    def deliver(x: int, s: str, m: int | None, ctx: Any) -> None:
        """Deliver nothing: the stream's rows are what is checked."""

    return SideEffectPod(deliver)(sources.a)


def derived_tags_as_data(sources: Sources) -> Any:
    """``DerivedSource(A, tags_as_data={"g": "g_data"})``: a new root with fresh provenance.

    Its rows are registered in an oracle of their own (its ``x`` values repeat A's),
    from the test's own table of A with the copied column added: the copy of each row's
    ``g`` must be that row's ``g``.
    """
    derived = DerivedSource(
        sources.a, tags_as_data={"g": "g_data"}, record_id_column="x"
    )
    table = sources.oracle_tables["A"]
    copied = table.append_column(
        pa.field("g_data", table.schema.field("g").type), table.column("g")
    )
    oracle = Oracle()
    oracle.add_source(
        derived.source_id, copied, tag_columns=TAG_COLUMNS, record_id_column="x"
    )
    sources.oracle = oracle
    sources.expected_source = derived.source_id
    return derived


KINDS: dict[str, StreamBuilder] = {
    "source_record_id": source_a,
    "source_positional": source_c,
    "pod_row_by_row": pod_row_by_row,
    "pod_concurrent": pod_concurrent,
    "side_effect": side_effect_stream,
    "derived_tags_as_data": derived_tags_as_data,
}
# The source whose rows each kind's stream holds (the derived kind sets its own).
EXPECTED_SOURCE = {"source_positional": "C"}
PATTERNS = ("fresh", "partial_then_full", "repeated", "interleaved")


def stream_params() -> list[Any]:
    """Every stream kind, completion order and read pattern, marked where a defect hits."""
    params = []
    for kind in KINDS:
        orders = list(ORDERS) if kind == "pod_concurrent" else [None]
        for order in orders:
            for pattern in PATTERNS:
                name = "-".join(p for p in (kind, order, pattern) if p)
                marks = (
                    [ITL_1065]
                    if kind == "pod_row_by_row" and pattern == "interleaved"
                    else []
                )
                params.append(pytest.param(kind, order, pattern, id=name, marks=marks))
    return params


def read(stream: Any, pattern: str) -> list[list[Any]]:
    """Read ``stream`` in ``pattern``; return the rows of every full read it made."""
    if pattern == "partial_then_full":
        partial = iter(stream.iter_data())
        next(partial)
        next(partial)
        del partial
        return [list(stream.iter_data())]
    if pattern == "repeated":
        first = list(stream.iter_data())
        # A full table between the reads; a side-effect stream's raises under all_info
        # (ITL-1066, checked config by config in test_side_effect_stream_tables).
        if isinstance(stream, SideEffectPodStream):
            stream.as_table()
        else:
            stream.as_table(all_info=True)
        return [first, list(stream.iter_data())]
    if pattern == "interleaved":
        first = iter(stream.iter_data())
        second = iter(stream.iter_data())
        seen_first = [next(first)]
        seen_second = list(itertools.islice(second, 3))
        seen_first += list(first)
        seen_second += list(second)
        return [seen_first, seen_second]
    return [list(stream.iter_data())]


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
        reads = read(stream, pattern)
        realised = list(GATE.completed)
    finally:
        GATE.disarm()

    if order is not None:
        assert realised == ORDERS[order](keys), "the gate did not realise the order"
    tags, data = stream.keys()
    expected = rows(
        sources.oracle,
        sources.expected_source or EXPECTED_SOURCE.get(kind, "A"),
    )
    # A function output's tokens, matched by record id with the run's own rows.
    tokens = stream_function_tokens(stream) if "y" in data else None
    for read_rows in reads:
        assert len(read_rows) == len(keys), "a read lost or repeated rows"
        # The rows the iteration yielded, not only the tables built afterwards.
        assert_rows_aligned(
            rows_table(read_rows),
            Layout(tag_keys=tuple(tags), data_keys=tuple(data)),
            sources.oracle,
            stream.data_context,
            function_tokens=tokens,
            expected_rows=expected,
        )

    if kind == "side_effect":
        # Showing a side-effect stream never delivers: its head has no rows. Its tables
        # are checked config by config in test_side_effect_stream_tables (ITL-1066).
        assert stream.head(100).as_table().num_rows == 0
        return
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
                assert table.num_rows == len(keys), f"{table.num_rows} rows"
                assert_requested_columns(
                    table, config, layout, require_sorted=call != "head"
                )
                assert_rows_aligned(
                    table,
                    layout,
                    sources.oracle,
                    stream.data_context,
                    function_tokens=tokens,
                    expected_rows=expected,
                )
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


@pytest.mark.parametrize(
    ("input_order", "config_name"),
    [
        pytest.param(
            input_order,
            name,
            id=f"{input_order}-{name}",
            # An input already in tag order looks sorted unsorted: there the ignored
            # sort_by_tags cannot be seen.
            marks=[ITL_1066]
            if name in ITL_1066_CONFIGS
            and not (name == "sort_by_tags" and input_order == "sorted")
            else [],
        )
        for input_order in INPUT_ORDERS
        for name in COLUMN_CONFIGS
    ],
)
@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_side_effect_stream_tables(
    scenario: str, input_order: str, config_name: str
) -> None:
    """A side-effect stream's table under each configuration: aligned, every row once."""
    sources = make_sources(scenario, input_order)
    stream = side_effect_stream(sources)
    config = COLUMN_CONFIGS[config_name]
    tags, data = stream.keys()
    layout = Layout(
        tag_keys=tuple(tags),
        data_keys=tuple(data),
        content_hash_column=content_hash_column(config),
    )
    table = stream.as_table(**config)
    assert_requested_columns(table, config, layout)
    assert_rows_aligned(
        table,
        layout,
        sources.oracle,
        stream.data_context,
        expected_rows=rows(sources.oracle, "A"),
    )


# ---------------------------------------------------------------------------
# Previews before a full read
# ---------------------------------------------------------------------------

# The configurations a preview is checked under. head() shows rows in stream order and,
# on main, refuses sort_by_tags and does not sort under all_info; PR #354 (ITL-1053)
# makes it sort among the rows it has, so the sorted configurations are left out here
# and EXPECTED_HEAD below is the one place to follow it when it merges.
HEAD_CONFIGS = {
    name: config
    for name, config in COLUMN_CONFIGS.items()
    if not config.get("all_info") and not config.get("columns", {}).get("sort_by_tags")
}
HEAD_SIZES = (0, 2, 4, 100)


def expected_head(origin_order: list[Any], available: int, n: int) -> list[Any]:
    """The rows a head of ``n`` shows: the first ``min(n, available)`` in stream order.

    ``origin_order`` is the stream's rows in input order; ``available`` how many of them
    the stream holds (a source all of them; a pod those computed so far).
    """
    return [(member,) for member in origin_order[: min(n, available)]]


PREVIEW_STATES = ("fresh_source", "fresh_pod", "partial_pod", "complete_pod")


@pytest.mark.parametrize("state", PREVIEW_STATES)
@pytest.mark.parametrize("input_order", INPUT_ORDERS)
@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_a_preview_before_a_full_read_shows_the_first_rows_and_computes_nothing(
    scenario: str, input_order: str, state: str
) -> None:
    """``head(n)`` of a fresh source, a fresh, partly or fully computed pod stream."""
    sources = make_sources(scenario, input_order)
    if state == "fresh_source":
        stream: Any = sources.a
        available = len(keys_a(scenario, input_order))
    else:
        derive_y(sources)
        stream = new_gated_scale_pod()(sources.a)
    GATE.disarm()
    if state == "fresh_pod":
        available = 0
    elif state == "partial_pod":
        partial = iter(stream.iter_data())
        next(partial), next(partial), next(partial)
        del partial
        available = 3
    elif state == "complete_pod":
        stream.as_table()
        available = len(keys_a(scenario, input_order))
    computed_before = len(GATE.completed)

    origin_order = [member_of(o) for o in sources.oracle.origins if o.source_id == "A"]
    tags, data = stream.keys()
    failures = []
    for name, config in HEAD_CONFIGS.items():
        layout = Layout(
            tag_keys=tuple(tags),
            data_keys=tuple(data),
            content_hash_column=content_hash_column(config),
        )
        for n in HEAD_SIZES:
            table = stream.head(n, **config).as_table()
            want = expected_head(origin_order, available, n)
            try:
                assert table.num_rows == len(want), f"{table.num_rows} rows"
                if not want:
                    continue
                assert_requested_columns(table, config, layout, require_sorted=False)
                assert_rows_aligned(
                    table,
                    layout,
                    sources.oracle,
                    stream.data_context,
                    expected_rows=want,
                )
            except AssertionError as exc:
                failures.append(f"head({n}) [{name}] {exc}")
    assert len(GATE.completed) == computed_before, "a preview computed rows"
    assert not failures, "\n".join(failures)
