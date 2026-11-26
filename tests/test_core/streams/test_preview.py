"""``stream.preview(n)``: the first rows of the sync result, and no more work (ITL-1464)."""

from __future__ import annotations

import asyncio
import gc
import weakref
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

from orcapod import PipelineJob, col, function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import CachedDataFunction, PythonDataFunction
from orcapod.core.operators import Join, MapData, MergeJoin, PolarsFilter, SemiJoin
from orcapod.core.sources import ArrowTableSource, CachedSource, DerivedSource
from orcapod.core.sources.polling_source import PollingSource
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams import preview as preview_module
from orcapod.core.streams.base import reads_in_batches
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import PreviewRefusedError, RowIdentifierError
from orcapod.side_effects import SideEffectPod
from orcapod.sources import SpiralDBTableSource
from orcapod.streams import StreamView, ViewStatus
from orcapod.types import Schema
from tests.test_core.streams.test_head_and_preview import _FakeDynamicSource
from tests.test_databases import fake_spiral_sdk

ROWS = 40
_PROJECT = "sample"
_KEYS = pa.schema(
    [pa.field("subject", pa.string()), pa.field("timestamp", pa.uint64())]
)


def _rows(n: int = ROWS) -> pa.Table:
    """The test table: ``n`` rows over three subjects, ``x`` null on every seventh."""
    return pa.table(
        {
            "subject": [f"m{i * 3 // n}" for i in range(n)],
            "timestamp": pa.array(range(n), pa.uint64()),
            "x": pa.array([None if i % 7 == 3 else i for i in range(n)], pa.int64()),
            "note": [f"n{i}" for i in range(n)],
        }
    )


@pytest.fixture(params=["spiral", "held"])
def source(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Make a source of 40 rows, read on demand (6 per SDK batch) or held."""
    monkeypatch.setattr(fake_spiral_sdk, "scan_batch_rows", 6)
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        fake_spiral_sdk.ExternalProject(_PROJECT).create_table(
            "default.event", key_schema=_KEYS
        ).write(_rows())

        def make(**kwargs: Any) -> Any:
            kwargs.setdefault("tag_only_columns", True)
            if request.param == "spiral":
                kwargs.setdefault("batch_rows", 4)
                return SpiralDBTableSource(_PROJECT, "event", **kwargs)
            kwargs.pop("batch_rows", None)
            return ArrowTableSource(
                _rows(), tag_columns=["subject", "timestamp"], source_id="e", **kwargs
            )

        yield make


@pytest.fixture()
def spiral(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A SpiralDB source of 40 rows, scanned 6 rows per SDK batch, and its scan log."""
    monkeypatch.setattr(fake_spiral_sdk, "scan_batch_rows", 6)
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        fake_spiral_sdk.ExternalProject(_PROJECT).create_table(
            "default.event", key_schema=_KEYS
        ).write(_rows())

        def make(**kwargs: Any) -> SpiralDBTableSource:
            options: dict[str, Any] = {"tag_only_columns": True, "batch_rows": 4}
            options.update(kwargs)
            made = SpiralDBTableSource(
                _PROJECT,
                "event",
                **options,
            )
            fake_spiral_sdk.scan_log.clear()
            return made

        yield make


class _Calls:
    """A function pod that records the rows it was called on."""

    def __init__(self, fail_at: int | None = None) -> None:
        self.seen: list[int] = []
        seen = self.seen

        @function_pod(output_keys="y")
        def triple(timestamp: int | None, x: int | None) -> int:
            seen.append(timestamp or 0)
            if timestamp == fail_at:
                raise RuntimeError(f"failed at {timestamp}")
            return 3 * (x or 0)

        self.function: Any = triple

    @property
    def pod(self) -> FunctionPod:
        return self.function.pod


def _with_time(source: Any) -> Any:
    """``source`` with its ``timestamp`` as data: a pod takes it."""
    return source(tag_only_columns=["subject"])


def _same(left: pa.Table, right: pa.Table) -> None:
    """Assert two tables are equal in values and in schema, Arrow metadata included
    (field metadata carries logical types and data-tags)."""
    assert left.schema.equals(right.schema, check_metadata=True)
    assert left.equals(right)


# ---------------------------------------------------------------------------
# The first n rows of the sync order
# ---------------------------------------------------------------------------


CHAINS: dict[str, Callable[[Any], Any]] = {
    "source": lambda s: s(),
    "data_tags": lambda s: s(tag_only_columns=False),
    "drop_nulls": lambda s: s().drop_nulls(),
    "filter": lambda s: s().filter(col("x") >= 9, subject="m2"),
    "nothing_left": lambda s: s().filter(col("x") >= 1000),
    "columns": lambda s: (
        s()
        .map_data({"note": "text"})
        .map_tags({"timestamp": "t"})
        .add_tags(cohort="A")
        .drop_tag_columns("subject")
    ),
    "pass_through": lambda s: s().select_data_columns(["x", "note"]),
    "function": lambda s: _Calls().pod(_with_time(s)),
    "function_then_filter": lambda s: (
        _Calls().pod(_with_time(s)).filter(col("y") >= 30).map_data({"y": "z"})
    ),
    "filter_then_function": lambda s: _Calls().pod(_with_time(s).filter(subject="m1")),
    "nothing_to_compute": lambda s: _Calls().pod(
        _with_time(s).filter(subject="absent")
    ),
    "two_functions": lambda s: _on_y(_Calls().pod(_with_time(s)).drop_nulls()),
}


def _on_y(stream: Any) -> Any:
    @function_pod(output_keys=("half", "label"))
    def second(y: int) -> tuple[float, str]:
        return y / 2, str(y)

    return second.pod(stream)


@pytest.mark.parametrize("n", [1, 3, 12, 39, 40, 41, 500])
@pytest.mark.parametrize("name", list(CHAINS))
def test_a_preview_is_the_first_rows_of_the_sync_result(
    source: Any, name: str, n: int
) -> None:
    """Values, types, nullability, columns and order; and what the view says."""
    stream = CHAINS[name](source)
    view = stream.preview(n)
    whole = CHAINS[name](source).as_table()
    _same(view.as_table(), whole.slice(0, n))
    assert type(view) is StreamView
    if whole.num_rows < n:
        assert view.status is ViewStatus.ROWS
        assert view.total_rows == whole.num_rows
        assert f"preview: all {whole.num_rows} row" in view.header()
    else:
        assert view.status is ViewStatus.PARTIAL
        assert view.total_rows is None
        assert f"preview: first {n} row" in view.header()
        assert "run() computes every row" in str(view)
    # Nothing was kept: head() shows what it did before.
    assert stream.head(3).header() == CHAINS[name](source).head(3).header()
    _ = view._repr_html_()


@pytest.mark.parametrize("first", [1, 2, 1024])
def test_the_batch_sizes_do_not_show(
    source: Any, monkeypatch: pytest.MonkeyPatch, first: int
) -> None:
    """The size of the first slice is not part of the result."""
    monkeypatch.setattr(preview_module, "FIRST_BATCH_ROWS", first)
    stream = source().drop_nulls().filter(subject="m2")
    _same(stream.preview(7).as_table(), stream.as_table().slice(0, 7))


WHOLE: dict[str, Callable[[Any], Any]] = {
    "join": lambda s: s().join(s().select_data_columns(["x"]).map_data({"x": "x2"})),
    "semi_join": lambda s: s().semi_join(s().filter(subject="m1")),
    "group_by": lambda s: s().group_by("subject"),
    "batch": lambda s: s().batch(3),
    "polars_filter": lambda s: s().polars_filter(pl.col("x") > 5),
    "tag_by": lambda s: s().drop_nulls().tag_by("note"),
    "over_a_group": lambda s: s().group_by("subject").map_data({"x": "xs"}),
    "rejoin": lambda s: (
        lambda t: t.select_data_columns(["x"]).rejoin(
            t.select_data_columns(["note"]).filter(subject="m2")
        )
    )(s()),
    "join_after_function": lambda s: (
        lambda t: t.join(s().select_data_columns(["note"]))
    )(_Calls().pod(_with_time(s))),
    "several": lambda s: (
        s()
        .drop_nulls()
        .join(s().filter(col("x") >= 9).map_data({"x": "a", "note": "b"}))
        .group_by("subject")
    ),
}


@pytest.mark.parametrize("n", [1, 2, 30, 500])
@pytest.mark.parametrize("name", list(WHOLE))
def test_a_step_over_its_whole_input(source: Any, name: str, n: int) -> None:
    """Refused; with ``compute_all`` the first rows of the sync result, nothing kept."""
    stream = WHOLE[name](source)
    with pytest.raises(PreviewRefusedError, match="compute_all=True"):
        stream.preview(n)
    view = stream.preview(n, compute_all=True)
    whole = WHOLE[name](source).as_table()
    _same(view.as_table(), whole.slice(0, n))
    assert stream.head(3).header() == WHOLE[name](source).head(3).header()


# ---------------------------------------------------------------------------
# How often a function is called
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("n", [1, 5, 40, 100])
def test_a_function_is_called_once_per_row_shown(source: Any, n: int) -> None:
    """Never on a row past the last one needed; in order; again on the next preview."""
    calls = _Calls()
    stream = calls.pod(_with_time(source))
    _ = stream.preview(n)
    expected = list(range(min(n, ROWS)))
    assert calls.seen == expected
    _ = stream.preview(n)
    assert calls.seen == expected * 2


def test_a_filter_after_a_function_calls_it_until_enough_rows_pass(source: Any) -> None:
    """Rows 10, 11 and 12 pass (``x`` of row 10 is null); the next is not asked for."""
    calls = _Calls()
    stream = calls.pod(_with_time(source)).filter(col("y") >= 30).map_data({"y": "z"})
    view = stream.preview(2)
    assert view.table.column("z").to_pylist() == [33, 36]
    assert calls.seen == list(range(13))


def test_a_filter_before_a_function_spares_the_calls(source: Any) -> None:
    """Only rows that pass it reach the function."""
    calls = _Calls()
    stream = calls.pod(_with_time(source).filter(subject="m1"))
    _ = stream.preview(3)
    assert calls.seen == [14, 15, 16]


def test_each_function_of_a_chain_is_called_once_per_row(source: Any) -> None:
    """The inner one no more often than the outer one needs."""
    inner = _Calls()
    outer: list[int] = []

    @function_pod(output_keys="z")
    def second(y: int) -> int:
        outer.append(y)
        return y + 1

    _ = second.pod(inner.pod(_with_time(source))).preview(4)
    assert inner.seen == [0, 1, 2, 3]
    assert len(outer) == 4


def test_a_shared_stream_is_computed_once_by_compute_all(source: Any) -> None:
    """Two branches of one function joined: one call per row, not two."""
    calls = _Calls()
    shared = calls.pod(_with_time(source))
    joined = shared.join(shared.map_data({"y": "z"}))
    view = joined.preview(3, compute_all=True)
    assert len(view) == 3
    assert calls.seen == list(range(ROWS))


def _branches(s: Any, count: int) -> list[Any]:
    """Branches over one source whose whole-input results differ in every value."""
    return [
        s()
        .select_data_columns(["x"])
        .polars_filter(pl.col("x") >= k)
        .map_data({"x": f"x{k}"})
        for k in range(count)
    ]


def _joined(streams: list[Any]) -> Any:
    first, *rest = streams
    for other in rest:
        first = first.join(other)
    return first


# Each gives the memo several temporary results of one size, freed one after another.
TEMPORARIES: dict[str, Callable[[Any], Any]] = {
    "joined_filters": lambda s: Join()(*_branches(s, 6)),
    "nested_joins": lambda s: _joined(_branches(s, 6)),
    "join_of_two_joins": lambda s: _joined(_branches(s, 4)[:2]).join(
        _joined(_branches(s, 4)[2:])
    ),
    "merge_of_two_filters": lambda s: MergeJoin()(
        *(
            s().select_data_columns(["x"]).map_data({"x": "a"}).polars_filter(p)
            for p in (pl.col("a") >= 0, pl.col("a") >= 20)
        )
    ),
    "filters_of_tag_bys": lambda s: Join()(
        *(
            s()
            .drop_nulls()
            .tag_by("note")
            .map_data({"x": f"x{k}"})
            .polars_filter(pl.col(f"x{k}") > k)
            for k in range(4)
        )
    ),
}


@pytest.mark.parametrize("name", list(TEMPORARIES))
def test_a_temporary_result_is_never_taken_for_another(source: Any, name: str) -> None:
    """Review of PR 571 (blocker): the memo was keyed by the identity of a freed stream,
    so a branch was given another's rows. Repeated: the reuse depends on the allocator."""
    whole = TEMPORARIES[name](source).as_table()
    assert whole.num_rows
    for attempt in range(12):
        stream = TEMPORARIES[name](source)
        view = stream.preview(ROWS * ROWS, compute_all=True)
        _same(view.as_table(), whole)
        if attempt % 3 == 0:
            gc.collect()


def test_a_stream_read_in_full_is_held_until_the_preview_ends() -> None:
    """The rule itself, whatever the allocator does: the memo holds the stream."""
    stream: Any = ArrowTableStream(_rows(), tag_columns=["subject"])
    request = preview_module.PreviewRequest(limit=None, whole=True)
    first = list(preview_module.preview_batches(stream, request))
    held = weakref.ref(stream)
    del stream
    _ = gc.collect()
    assert held() is not None
    assert list(preview_module.preview_batches(held(), request)) == first
    del request, first
    _ = gc.collect()
    assert held() is None


def test_an_operator_changed_on_the_instance_is_not_applied_by_batch(
    source: Any,
) -> None:
    """Review of PR 571: what its class declares says nothing of another body."""
    pod = MapData({"note": "text"})
    original: Any = pod.static_process
    pod.static_process = lambda *streams: original(  # pyright: ignore[reportAttributeAccessIssue]  # replaced on the instance: the test
        *(
            PolarsFilter(predicates=(pl.col("x") == pl.col("x").max(),))(s)
            for s in streams
        )
    )
    stream = pod(source())
    assert not reads_in_batches(stream)
    with pytest.raises(PreviewRefusedError, match="compute_all=True"):
        stream.preview(1)
    assert stream.preview(1, compute_all=True).table.column("x").to_pylist() == [39]
    assert stream.run().table.column("x").to_pylist() == [39]


def test_a_function_over_a_whole_step_is_called_for_the_rows_shown(source: Any) -> None:
    """``compute_all`` computes the step beneath in full, not the function above it."""
    seen: list[int] = []

    @function_pod(output_keys="size")
    def count(x: list[int | None]) -> int:
        seen.append(len(x))
        return len(x)

    stream = count.pod(source().group_by("subject"))
    assert len(stream.preview(2, compute_all=True)) == 2
    assert len(seen) == 2


def test_results_a_stream_holds_are_used(source: Any) -> None:
    """After a run nothing is called; after a partial read only the rest is."""
    calls = _Calls()
    stream = calls.pod(_with_time(source))
    rows = iter(stream)
    _ = [next(rows) for _ in range(3)]
    assert calls.seen == [0, 1, 2]
    _ = stream.preview(5)
    assert calls.seen == [0, 1, 2, 3, 4]
    _ = stream.run()
    calls.seen.clear()
    _same(stream.preview(5).as_table(), stream.as_table().slice(0, 5))
    _ = stream.map_data({"y": "z"}).preview(5)
    assert calls.seen == []


def test_no_hook_fires(source: Any) -> None:
    """A post-run hook is an effect."""
    fired: list[Any] = []
    calls = _Calls()
    pod = calls.pod
    pod.add_post_run_hook(fired.append)
    _ = pod(_with_time(source)).preview(3)
    assert calls.seen == [0, 1, 2]
    assert fired == []


# ---------------------------------------------------------------------------
# Nothing stored
# ---------------------------------------------------------------------------


def _cached_pod(calls: _Calls, store: InMemoryArrowDatabase) -> Any:
    return CachedFunctionPod(calls.pod, result_database=store)


def _cached_function(calls: _Calls, store: InMemoryArrowDatabase) -> Any:
    inner = calls.pod.data_function
    return FunctionPod(CachedDataFunction(inner, result_database=store))  # pyright: ignore[reportArgumentType]  # conforms at runtime


@pytest.mark.parametrize("kind", [_cached_pod, _cached_function])
def test_a_cached_pod_reads_its_results_and_stores_none(
    source: Any, kind: Callable[..., Any]
) -> None:
    """Stored results are used, missing ones computed and left out of the store."""
    calls = _Calls()
    store = InMemoryArrowDatabase()
    stream = kind(calls, store).process(
        _with_time(source).select_data_columns(["timestamp", "x"])
    )
    _ = stream.preview(3)
    assert calls.seen == [0, 1, 2]
    assert store.list_table_paths() == []
    # Rows 0 and 1 stored by an iteration of another stream over the same pod.
    other = kind(calls, store).process(
        _with_time(source).select_data_columns(["timestamp", "x"])
    )
    rows = iter(other)
    _ = [next(rows) for _ in range(2)]
    calls.seen.clear()
    view = stream.preview(4)
    assert calls.seen == [2, 3]
    assert view.table.column("y").to_pylist() == [0, 3, 6, 0]
    (path,) = store.list_table_paths()
    table = store.get_all_records(path)
    assert table is not None and table.num_rows == 2


# ---------------------------------------------------------------------------
# The read of a source
# ---------------------------------------------------------------------------


def test_the_limit_reaches_the_scan(spiral: Any) -> None:
    """Pushed down when every row read is shown; else the scan is closed early."""
    cases: list[tuple[Callable[[Any], Any], int | None, int]] = [
        (lambda s: s, 5, 1),
        (lambda s: s.map_data({"note": "text"}).add_tags(a=1), 5, 1),
        (lambda s: s.drop_nulls(["x"]), None, 2),
        (lambda s: s.filter(subject="m0"), None, 1),
        (
            lambda s: DerivedSource(
                s, tags_as_data={"timestamp": "time"}, source_id="derived"
            ),
            5,
            1,
        ),
    ]
    for chain, limit, batches in cases:
        stream = chain(spiral())
        assert len(stream.preview(5)) == 5
        (scan,) = fake_spiral_sdk.scan_log
        assert scan.limit == limit
        assert scan.batches_read <= batches + 1
        assert scan.closed
    stream = _Calls().pod(spiral(tag_only_columns=["subject"]))
    _ = stream.preview(5)
    assert fake_spiral_sdk.scan_log[0].limit == 5


DERIVED_CHAINS: dict[
    str, tuple[dict[str, Any], dict[str, str], Callable[[Any], Any]]
] = {
    "filter": ({}, {"timestamp": "time"}, lambda d: d.filter(col("time") >= 0)),
    "drop_nulls": ({}, {"timestamp": "time"}, lambda d: d.drop_nulls(["x"])),
    "function": (
        {"tag_only_columns": ["subject"]},
        {"subject": "who"},
        lambda d: _Calls().pod(d.drop_nulls(["x"])),
    ),
}


@pytest.mark.parametrize("keys", [(), ("subject",)])
@pytest.mark.parametrize("name", list(DERIVED_CHAINS))
def test_a_derived_source_below_a_step_that_drops_rows_reads_what_it_needs(
    spiral: Any, name: str, keys: tuple[str, ...]
) -> None:
    """The origin is read batch by batch, and its scan closed once enough rows passed.

    Review of fdf70e94: with no limit (a step above may drop rows), the derived
    source read every batch of its origin before giving its first row.
    """
    options, tags_as_data, chain = DERIVED_CHAINS[name]
    key_columns = (*keys, "timestamp") if keys else ()

    def derived() -> Any:
        return DerivedSource(
            spiral(**options),
            tags_as_data=tags_as_data,
            source_id="derived",
            record_id_columns=key_columns or None,
        )

    stream = chain(derived())
    view = stream.preview(5)
    (scan,) = fake_spiral_sdk.scan_log
    assert scan.limit is None
    assert scan.batches_read <= 2
    assert scan.closed
    _same(view.as_table(), chain(derived()).as_table().slice(0, 5))


@pytest.mark.parametrize("keys", [(), ("timestamp", "subject")])
def test_a_derived_source_read_in_batches_names_its_rows_as_read_whole(
    spiral: Any, keys: tuple[str, ...]
) -> None:
    """Record ids, system tags and source info of every batch: as in one read.

    Positional record ids continue from one batch to the next.
    """

    def derived() -> Any:
        return DerivedSource(
            spiral(),
            tags_as_data={"timestamp": "time"},
            source_id="derived",
            record_id_columns=keys or None,
        )

    request = preview_module.PreviewRequest(limit=None)
    batches = list(preview_module.preview_batches(derived(), request))
    assert len(batches) > 1
    read = pa.concat_tables(
        b.as_table(columns=preview_module.PREVIEW_COLUMNS) for b in batches
    )
    _same(read, derived().as_table(columns=preview_module.PREVIEW_COLUMNS))
    rows = asyncio.run(_async_rows(derived()))
    assert _row_set(rows) == _row_set(
        read.select(derived().keys()[0] + derived().keys()[1]).to_pylist()
    )


def test_a_repeated_key_across_batches_is_refused_when_reached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A key repeated in a later batch raises when that row is reached, not before.

    Each batch holds unique keys; only the third repeats one of the first: preview
    succeeds through the first two batches (CodeRabbit, PR 571).
    """
    monkeypatch.setattr(preview_module, "FIRST_BATCH_ROWS", 2)
    keys = ["a", "b", "c", "d", "e", "f", "a", "g"]
    origin = ArrowTableSource(
        pa.table(
            {
                "key": keys,
                "time": pa.array(range(len(keys)), pa.int64()),
                "x": pa.array(range(len(keys)), pa.int64()),
            }
        ),
        tag_columns=["key", "time"],
        source_id="keys",
        tag_only_columns=True,
    )

    def derived() -> Any:
        """Read in batches of 2, 4, 8: a filter above takes the limit away."""
        return DerivedSource(
            origin,
            tags_as_data={"time": "t"},
            source_id="derived",
            record_id_columns="key",
        ).filter(col("x") >= 0)

    assert derived().preview(6).table.column("key").to_pylist() == keys[:6]
    with pytest.raises(RowIdentifierError, match="unique"):
        derived().preview(7)
    with pytest.raises(RowIdentifierError, match="unique"):
        derived().as_table()


def test_a_source_limit_and_where_hold(spiral: Any) -> None:
    """The smaller of the two limits; ``where`` in the scan."""
    view = spiral(limit=3, where={"subject": "m1"}).preview(10)
    assert view.status is ViewStatus.ROWS
    assert view.table.column("timestamp").to_pylist() == [14, 15, 16]


def test_max_rows_holds_only_when_every_row_is_read(spiral: Any) -> None:
    """As for ``head``: a table of any size can be looked at."""
    assert len(spiral(max_rows=10).preview(20)) == 20
    with pytest.raises(ValueError, match="max_rows"):
        spiral(max_rows=10).group_by("subject").preview(1, compute_all=True)


def test_a_refreshed_source_is_read_as_it_is_now(spiral: Any) -> None:
    """A result kept before a change is not shown after it."""
    root = spiral(tag_only_columns=["subject"])
    calls = _Calls()
    stream = calls.pod(root.filter(subject="m0")).map_data({"y": "z"})
    _ = stream.run()
    calls.seen.clear()
    changed = _rows().slice(1, 1).set_column(2, "x", pa.array([100], pa.int64()))
    fake_spiral_sdk.ExternalProject(_PROJECT).table("default.event").write(changed)
    assert stream.preview(2).table.column("z").to_pylist() == [0, 3]  # still pinned
    assert calls.seen == []
    assert root.check_for_new_data()
    assert stream.preview(2).table.column("z").to_pylist() == [0, 300]
    assert calls.seen == [0, 1]


# ---------------------------------------------------------------------------
# Errors and refusals
# ---------------------------------------------------------------------------


def test_an_error_is_raised_only_for_a_row_that_is_needed(source: Any) -> None:
    """The function's own error, and nothing is kept; a later row is never reached."""
    calls = _Calls(fail_at=5)
    stream = calls.pod(_with_time(source)).map_data({"y": "z"})
    assert len(stream.preview(5)) == 5
    with pytest.raises(RuntimeError, match="failed at 5"):
        stream.preview(6)
    assert calls.seen == [*range(5), *range(6)]
    assert stream.head().status is ViewStatus.NOT_COMPUTED


def test_a_source_error_is_raised_only_when_reached(
    spiral: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The scan is closed after an error, too."""
    original = SpiralDBTableSource._key_tokens  # pyright: ignore[reportPrivateUsage]  # a failing row

    def failing(self: Any, table: pa.Table) -> list[str]:
        if 20 in table.column("timestamp").to_pylist():
            raise RowIdentifierError("bad key")
        return original(self, table)

    monkeypatch.setattr(SpiralDBTableSource, "_key_tokens", failing)
    stream = spiral().drop_nulls(["x"])
    assert len(stream.preview(5)) == 5
    with pytest.raises(RowIdentifierError, match="bad key"):
        stream.preview(30)
    assert all(scan.closed for scan in fake_spiral_sdk.scan_log)


def test_what_would_act_is_refused_before_any_call(source: Any) -> None:
    """Side effects, the invocation context, polls and cache writes; compute_all or not."""
    calls = _Calls()
    delivered: list[int] = []

    def deliver(y: int, ctx: Any) -> None:
        delivered.append(y)

    @function_pod(output_keys="w", ctx_arg="ctx")
    def act(y: int, ctx: Any) -> int:
        delivered.append(y)
        return y

    computed = calls.pod(_with_time(source))
    store = InMemoryArrowDatabase()
    fake = _FakeDynamicSource()
    refused = [
        SideEffectPod(deliver)(computed),
        act.pod(computed),
        MapData({"w": "v"})(act.pod(computed)),
        CachedSource(source(), cache_database=store),
        PollingSource(fake, tag_columns="id", record_id_columns="id"),
    ]
    for stream in refused:
        for compute_all in (False, True):
            with pytest.raises(
                PreviewRefusedError, match=r"run\(\) computes every row"
            ):
                stream.preview(2, compute_all=compute_all)
    assert (calls.seen, delivered) == ([], [])
    assert store.list_table_paths() == []
    assert (fake.polls, fake.fetches) == (0, 0)


def test_a_derived_source_reads_only_the_rows_shown(source: Any) -> None:
    """Its origin's first rows, as its origin previews them; nothing is kept."""
    derived = DerivedSource(
        source(), tags_as_data={"timestamp": "time"}, source_id="derived"
    )
    shown = [derived.preview(3).as_table(), derived.preview(3, compute_all=True)]
    assert derived._held_rows() is None  # pyright: ignore[reportPrivateUsage]
    whole = derived.as_table()
    for view in shown:
        _same(view if isinstance(view, pa.Table) else view.as_table(), whole[:3])
    _same(derived.preview(500).as_table(), whole)


@pytest.mark.parametrize("origin", ["cached", "polling"])
def test_a_derived_source_never_writes_or_polls_its_origin(
    source: Any, origin: str
) -> None:
    """Refused by the origin, compute_all or not: no cache table, no poll, nothing kept.

    Review of 2dda2d82: the derived source read its origin whole through
    ``as_table()``, which wrote a cache table, polled, and kept the rows.
    """
    store = InMemoryArrowDatabase()
    fake = _FakeDynamicSource(declared=Schema({"id": int, "val": int}))
    made: Any = (
        CachedSource(source(), cache_database=store)
        if origin == "cached"
        else PollingSource(fake, tag_columns="id", record_id_columns="id")
    )
    tag = "subject" if origin == "cached" else "id"
    derived = DerivedSource(made, tags_as_data={tag: "copy"}, source_id="derived")
    before = (fake.polls, fake.fetches)
    for compute_all in (False, True):
        with pytest.raises(PreviewRefusedError, match=r"run\(\) computes every row"):
            derived.preview(1, compute_all=compute_all)
    assert store.list_table_paths() == []
    assert (fake.polls, fake.fetches) == before
    assert derived._held_rows() is None  # pyright: ignore[reportPrivateUsage]
    if origin == "cached":
        assert made.head().status is ViewStatus.NOT_COMPUTED


@pytest.mark.parametrize("tags_as_data", [None, {"subject": "who"}])
def test_a_derived_source_over_a_node_keeps_nothing(
    source: Any, tags_as_data: dict[str, str] | None
) -> None:
    """The node's stored rows, read whole only with compute_all and not kept."""
    job = PipelineJob(name="itl1464_derived", database=InMemoryArrowDatabase())
    with job:
        _ = _Calls().pod(_with_time(source), label="triple")
    _ = job.run()
    node = job.nodes["triple"]
    derived = (
        DerivedSource(node)
        if tags_as_data is None
        else DerivedSource(node, tags_as_data=tags_as_data, source_id="derived")
    )
    with pytest.raises(PreviewRefusedError, match="compute_all"):
        derived.preview(2)
    shown = derived.preview(2, compute_all=True).as_table()
    assert derived._held_rows() is None  # pyright: ignore[reportPrivateUsage]
    _same(shown, derived.as_table()[:2])


def _unrelated() -> Any:
    """A source tagged ``unrelated`` and ``batch``, whose data a ``_Calls`` pod takes."""
    return ArrowTableSource(
        pa.table(
            {
                "unrelated": ["u0", "u1", "u2"],
                "batch": ["b0", "b1", "b2"],
                "timestamp": pa.array([0, 1, 2], pa.uint64()),
                "x": pa.array([1, 2, 3], pa.int64()),
            }
        ),
        tag_columns=["unrelated", "batch"],
        source_id="other",
        tag_only_columns=True,
    )


@pytest.mark.parametrize("n", [1, 3, 40, 41])
@pytest.mark.parametrize("form", ["method", "operator", "renamed_tag"])
def test_a_semi_join_without_a_shared_tag_computes_no_right_input(
    source: Any, n: int, form: str
) -> None:
    """The left input's first rows, with no call of the right input's function.

    Review of 2dda2d82: preview computed both inputs before SemiJoin gave back its
    left input, so it called (and raised in) a function the full read never calls.
    """
    calls = _Calls(fail_at=0)

    def joined() -> Any:
        """A fresh stream each time: a read keeps its result, which a preview uses."""
        right = calls.pod(_unrelated())
        left = source()
        if form == "method":
            return left.semi_join(right)
        if form == "renamed_tag":
            left = left.map_tags({"subject": "who"})
        return SemiJoin()(left, right)

    whole = joined().as_table()
    assert calls.seen == []
    assert _row_set(asyncio.run(_async_rows(joined()))) == _row_set(whole.to_pylist())
    for compute_all in (False, True):
        _same(joined().preview(n, compute_all=compute_all).as_table(), whole[:n])
    assert calls.seen == []


def _row_set(rows: list[dict[str, Any]]) -> set[tuple[tuple[str, Any], ...]]:
    """The rows as a set, for an order the async path does not promise."""
    return {tuple(sorted(row.items())) for row in rows}


async def _async_rows(stream: Any) -> list[dict[str, Any]]:
    """Every row of the stream's async read, as dicts of its tags and data."""
    return [
        {**tag.as_dict(), **data.as_dict()}
        async for tag, data in stream.async_iter_data()
    ]


def test_a_semi_join_with_a_shared_tag_still_reads_both(source: Any) -> None:
    """With a shared tag both inputs are needed, as in the full read."""
    left = source()
    right = source().filter(subject="m1")
    joined = left.semi_join(right)
    with pytest.raises(PreviewRefusedError, match="compute_all"):
        joined.preview(2)
    _same(joined.preview(5, compute_all=True).as_table(), joined.as_table()[:5])


def test_nodes_are_refused(source: Any) -> None:
    """Every node of a job, run or not, and a stream over one."""
    calls = _Calls()
    job = PipelineJob(name="itl1464", database=InMemoryArrowDatabase())
    with job:
        _ = calls.pod(_with_time(source), label="triple").map_data(
            {"y": "z"}, label="renamed"
        )
    nodes = list(job.nodes.values())
    for node in nodes:
        with pytest.raises(PreviewRefusedError):
            node.preview(1)
    _ = job.run()
    calls.seen.clear()
    for node in nodes:
        with pytest.raises(PreviewRefusedError):
            node.preview(1, compute_all=True)
    with pytest.raises(PreviewRefusedError):
        job.nodes["triple"].map_data({"y": "q"}).preview(1)
    assert calls.seen == []


@pytest.mark.parametrize(
    ("n", "error"),
    [(0, ValueError), (-1, ValueError), (2.0, TypeError), (True, TypeError)],
)
def test_the_number_of_rows_is_checked(source: Any, n: Any, error: Any) -> None:
    """Before anything is read."""
    with pytest.raises(error):
        source().preview(n)


def test_an_inactive_function_gives_no_row(source: Any) -> None:
    """No limit is pushed down, every row is looked at, and the view says so."""
    calls = _Calls()
    function = calls.pod.data_function
    assert isinstance(function, PythonDataFunction)
    function.set_active(False)
    view = calls.pod(_with_time(source)).preview(3)
    assert (len(view), view.status, calls.seen) == (0, ViewStatus.ROWS, [])


# ---------------------------------------------------------------------------
# Review of 342d8bb0: read-only at every layer; shortcuts only for their own class
# ---------------------------------------------------------------------------


def _nested_cached(calls: _Calls, stores: list[InMemoryArrowDatabase]) -> Any:
    """``calls``' function wrapped in one ``CachedDataFunction`` per store, the first
    store innermost."""
    function: Any = calls.pod.data_function
    for store in stores:
        function = CachedDataFunction(function, result_database=store)
    return FunctionPod(function)


def _untouched(store: InMemoryArrowDatabase) -> None:
    """Assert nothing was written to ``store``: it holds no table."""
    assert store.list_table_paths() == []


@pytest.mark.parametrize("depth", [2, 3])
def test_nested_cached_functions_write_at_no_layer(source: Any, depth: int) -> None:
    """Review of 342d8bb0 (blocker): with two cached layers, preview created and wrote
    the inner result table. Every layer reads; none writes or creates a table."""
    calls = _Calls()
    stores = [InMemoryArrowDatabase() for _ in range(depth)]
    stream = _nested_cached(calls, stores).process(
        _with_time(source).select_data_columns(["timestamp", "x"])
    )
    shown = stream.preview(3).as_table()
    assert calls.seen == [0, 1, 2]
    for store in stores:
        _untouched(store)
    expected = (
        _nested_cached(_Calls(), [InMemoryArrowDatabase() for _ in range(depth)])
        .process(_with_time(source).select_data_columns(["timestamp", "x"]))
        .as_table()
    )
    _same(shown, expected.select(shown.column_names).slice(0, 3))


@pytest.mark.parametrize("depth", [2, 3])
def test_nested_cached_functions_read_every_layer(source: Any, depth: int) -> None:
    """A result stored only by the innermost layer is read through the outer ones,
    which add nothing."""
    calls = _Calls()
    stores = [InMemoryArrowDatabase() for _ in range(depth)]
    rows = iter(
        _nested_cached(calls, stores[:1]).process(
            _with_time(source).select_data_columns(["timestamp", "x"])
        )
    )
    _ = [next(rows) for _ in range(2)]
    (inner_path,) = stores[0].list_table_paths()
    calls.seen.clear()
    view = (
        _nested_cached(calls, stores)
        .process(_with_time(source).select_data_columns(["timestamp", "x"]))
        .preview(4)
    )
    assert calls.seen == [2, 3]
    assert view.table.column("y").to_pylist() == [0, 3, 6, 0]
    assert stores[0].list_table_paths() == [inner_path]
    inner = stores[0].get_all_records(inner_path)
    assert inner is not None and inner.num_rows == 2
    for store in stores[1:]:
        _untouched(store)


def _python_function_calling_twice() -> Any:
    class _CalledTwice(PythonDataFunction):
        """A subclass whose ``call`` does more than its class's: it calls twice."""

        def call(self, data: Any, **kwargs: Any) -> Any:
            _ = super().call(data, **kwargs)
            return super().call(data, **kwargs)

    calls = _Calls()
    inner: Any = calls.pod.data_function
    return calls, _CalledTwice(inner._function, output_keys="y")


def _wrapper_writing() -> Any:
    from orcapod.core.data_function import DataFunctionWrapper

    written: list[Any] = []

    class _Writing(DataFunctionWrapper[Any]):
        """A wrapper whose ``call`` records every output: an effect."""

        def call(self, data: Any, **kwargs: Any) -> Any:
            output = super().call(data, **kwargs)
            written.append(output)
            return output

    calls = _Calls()
    return calls, _Writing(calls.pod.data_function)


@pytest.mark.parametrize(
    "make", [_python_function_calling_twice, _wrapper_writing], ids=["leaf", "wrapper"]
)
@pytest.mark.parametrize("cached", [False, True])
def test_a_data_function_overriding_call_is_refused_before_any_call(
    source: Any, make: Callable[[], Any], cached: bool
) -> None:
    """A layer that cannot be made read-only makes preview refuse, never write: a
    subclass whose ``call`` is its own, also beneath a cached layer."""
    calls, function = make()
    store = InMemoryArrowDatabase()
    if cached:
        function = CachedDataFunction(function, result_database=store)
    stream = FunctionPod(function).process(
        _with_time(source).select_data_columns(["timestamp", "x"])
    )
    for compute_all in (False, True):
        with pytest.raises(PreviewRefusedError, match="no read-only call"):
            stream.preview(2, compute_all=compute_all)
    assert calls.seen == []
    _untouched(store)


def test_a_plain_wrapper_is_previewed_through(source: Any) -> None:
    """A ``DataFunctionWrapper`` is read-only when what it wraps is."""
    from orcapod.core.data_function import DataFunctionWrapper

    calls = _Calls()
    store = InMemoryArrowDatabase()
    function = DataFunctionWrapper[Any](
        CachedDataFunction(calls.pod.data_function, result_database=store)
    )
    stream = FunctionPod(function).process(
        _with_time(source).select_data_columns(["timestamp", "x"])
    )
    assert stream.preview(2).table.column("y").to_pylist() == [0, 3]
    assert calls.seen == [0, 1]
    _untouched(store)


class _DiscardingSemiJoin(SemiJoin):
    """A SemiJoin that drops the first left row: its own ``binary_static_process``."""

    def binary_static_process(self, left_stream: Any, right_stream: Any) -> Any:
        result = super().binary_static_process(left_stream, right_stream)
        table = result.as_table(columns={"source": True, "system_tags": True})
        return ArrowTableStream(
            table.slice(1),
            tag_columns=result.keys()[0],
            data_context=result.data_context_key,
        )


def test_a_semi_join_subclass_is_not_given_back_its_left_input(source: Any) -> None:
    """Review of 342d8bb0 (high): the shortcut gave back the left input past a
    subclass's own processing. A subclass runs the full operation."""

    def joined() -> Any:
        return _DiscardingSemiJoin()(source(), _unrelated())

    whole = joined().as_table()
    with pytest.raises(PreviewRefusedError, match="compute_all"):
        joined().preview(1)
    for n in (1, 3):
        _same(joined().preview(n, compute_all=True).as_table(), whole[:n])


class _DiscardingMapData(MapData):
    """A MapData that drops the first row: its own ``unary_static_process``."""

    def unary_static_process(self, stream: Any) -> Any:
        result = super().unary_static_process(stream)
        table = result.as_table(columns={"source": True, "system_tags": True})
        return ArrowTableStream(
            table.slice(1),
            tag_columns=result.keys()[0],
            data_context=result.data_context_key,
        )


def test_a_batch_operator_subclass_is_not_applied_by_batch(source: Any) -> None:
    """The batch shortcut is its class's: a subclass with its own processing is
    applied to its whole input."""
    stream = _DiscardingMapData({"note": "text"})(source())
    assert not reads_in_batches(stream)
    with pytest.raises(PreviewRefusedError, match="compute_all"):
        stream.preview(1)
    whole = _DiscardingMapData({"note": "text"})(source()).as_table()
    for n in (1, 3):
        _same(stream.preview(n, compute_all=True).as_table(), whole[:n])


class _FilteringPod(FunctionPod):
    """A pod whose ``process_data`` is its own: it gives no row."""

    def process_data(self, tag: Any, data: Any, **kwargs: Any) -> Any:
        tag, _ = super().process_data(tag, data, **kwargs)
        return tag, None


class _FilteringCachedPod(CachedFunctionPod):
    """A cached pod whose ``process_data`` is its own: it gives no row."""

    def process_data(self, tag: Any, data: Any, **kwargs: Any) -> Any:
        tag, _ = super().process_data(tag, data, **kwargs)
        return tag, None


@pytest.mark.parametrize("kind", ["plain", "cached"])
def test_a_pod_subclass_with_its_own_processing_is_refused(
    source: Any, kind: str
) -> None:
    """A pod's preview stands for its class's ``process_data``; a subclass's own is
    not known, so the preview is refused before any call."""
    calls = _Calls()
    store = InMemoryArrowDatabase()
    if kind == "plain":
        pod: Any = _FilteringPod(calls.pod.data_function)
    else:
        pod = _FilteringCachedPod(calls.pod, result_database=store)
    stream = pod.process(_with_time(source).select_data_columns(["timestamp", "x"]))
    for compute_all in (False, True):
        with pytest.raises(PreviewRefusedError, match="not a pod of the library"):
            stream.preview(2, compute_all=compute_all)
    assert calls.seen == []
    _untouched(store)


# ---------------------------------------------------------------------------
# Review of 82a3a6e0: shortcuts for exactly their class (N55)
# ---------------------------------------------------------------------------


def test_a_data_function_with_its_own_output_builder_is_refused(source: Any) -> None:
    """Review of 82a3a6e0, finding 1: a subclass's ``_build_output_data`` ran (an
    effect) beneath a cached layer. A data function subclass is refused before any
    call."""
    built: list[Any] = []

    class _Recording(PythonDataFunction):
        def _build_output_data(self, values: Any) -> Any:
            output = super()._build_output_data(values)
            built.append(output)
            return output

    calls = _Calls()
    inner: Any = calls.pod.data_function
    store = InMemoryArrowDatabase()
    leaf: Any = _Recording(inner._function, output_keys="y")
    function = CachedDataFunction(leaf, result_database=store)
    stream = FunctionPod(function).process(
        _with_time(source).select_data_columns(["timestamp", "x"])
    )
    for compute_all in (False, True):
        with pytest.raises(PreviewRefusedError, match="no read-only call"):
            stream.preview(2, compute_all=compute_all)
    assert calls.seen == []
    assert built == []
    _untouched(store)


def test_a_cached_pod_with_its_own_lookup_is_refused(source: Any) -> None:
    """Review of 82a3a6e0, finding 2: a subclass's ``lookup_cached_data`` gave rows the
    full read does not. A cached pod subclass is refused before any call."""

    class _Synthetic(CachedFunctionPod):
        def lookup_cached_data(self, data: Any) -> Any:
            return None

    calls = _Calls()
    store = InMemoryArrowDatabase()
    stream = _Synthetic(calls.pod, result_database=store).process(
        _with_time(source).select_data_columns(["timestamp", "x"])
    )
    with pytest.raises(PreviewRefusedError, match="not a pod of the library"):
        stream.preview(2)
    assert calls.seen == []
    _untouched(store)


class _WrappedSemiJoin(SemiJoin):
    """A SemiJoin whose ``static_process`` is wrapped by ``__getattribute__`` to drop
    the first row: no member of the class is overridden."""

    def __getattribute__(self, name: str) -> Any:
        member = super().__getattribute__(name)
        if name != "static_process":
            return member

        def dropping(*streams: Any) -> Any:
            result = member(*streams)
            table = result.as_table(columns={"source": True, "system_tags": True})
            return ArrowTableStream(
                table.slice(1),
                tag_columns=result.keys()[0],
                data_context=result.data_context_key,
            )

        return dropping


def test_a_semi_join_with_dynamic_dispatch_runs_the_full_operation(
    source: Any,
) -> None:
    """Review of 82a3a6e0, finding 3: the shortcut held for a subclass that wraps its
    processing dynamically. A subclass runs the full operation."""

    def joined() -> Any:
        return _WrappedSemiJoin()(source(), _unrelated())

    whole = joined().as_table()
    assert whole.column("timestamp").to_pylist()[:2] == [1, 2]
    with pytest.raises(PreviewRefusedError, match="compute_all"):
        joined().preview(2)
    _same(joined().preview(2, compute_all=True).as_table(), whole[:2])


_PROXIED_CALLS: list[int] = []


def _shifted(timestamp: int) -> int:
    """A plain function, saved by name in a proxy's config."""
    _PROXIED_CALLS.append(timestamp)
    return timestamp + 100


def test_a_bound_proxy_previews_its_function(source: Any) -> None:
    """Review of 82a3a6e0, finding 4 (a regression): a ``DataFunctionProxy`` bound to
    a plain function previews as at 342d8bb0, through the bound function."""
    from orcapod.core.data_function_proxy import DataFunctionProxy

    function = PythonDataFunction(_shifted, output_keys="y", version="v1.0")
    proxy = DataFunctionProxy(
        config=function.to_config(),
        content_hash_str=function.content_hash().to_string(),
        pipeline_hash_str=function.pipeline_hash().to_string(),
    )
    proxy.bind(function)  # pyright: ignore[reportArgumentType]  # conforms at runtime

    def stream() -> Any:
        return FunctionPod(proxy).process(  # pyright: ignore[reportArgumentType]  # conforms at runtime
            _with_time(source).select_data_columns(["timestamp"]).drop_nulls()
        )

    _PROXIED_CALLS.clear()
    shown = stream().preview(2).as_table()
    assert _PROXIED_CALLS == [0, 1]
    whole = stream().as_table()
    _same(shown, whole.select(shown.column_names).slice(0, 2))


class _DroppingDerived(DerivedSource):
    """A DerivedSource whose ``as_table`` drops the first row."""

    def as_table(self, *args: Any, **kwargs: Any) -> Any:
        return super().as_table(*args, **kwargs).slice(1)


class _DescendingDerived(DerivedSource):
    """A DerivedSource that derives its rows in descending timestamp order."""

    def _derive_from(self, table: Any, *args: Any, **kwargs: Any) -> Any:
        return super()._derive_from(
            table.sort_by([("timestamp", "descending")]), *args, **kwargs
        )


@pytest.mark.parametrize("kind", [_DroppingDerived, _DescendingDerived])
def test_a_derived_source_subclass_previews_its_own_read(
    source: Any, kind: type[DerivedSource]
) -> None:
    """Review of 82a3a6e0, finding 5: a subclass's preview diverged from its read.
    A subclass is read in full, only with ``compute_all``, and keeps nothing."""

    def derived() -> Any:
        return kind(source(), tags_as_data={"timestamp": "t"}, source_id="d")

    whole = derived().as_table()
    previewed = derived()
    with pytest.raises(PreviewRefusedError, match="compute_all"):
        previewed.preview(2)
    for n in (1, 2, 41):
        _same(previewed.preview(n, compute_all=True).as_table(), whole[:n])
    assert previewed._held_rows() is None  # pyright: ignore[reportPrivateUsage]


# ---------------------------------------------------------------------------
# Review of bae54b3c: a DerivedSource subclass's full read keeps nothing anywhere
# ---------------------------------------------------------------------------


def test_a_derived_source_subclass_over_stored_rows_is_previewed_with_compute_all(
    source: Any,
) -> None:
    """Review of bae54b3c, finding 1: over an exact DerivedSource of a job node's
    stored rows, the origin was probed without ``compute_all`` and refused although
    the caller gave it."""
    job = PipelineJob(name="itl1464_derived_subclass", database=InMemoryArrowDatabase())
    with job:
        _ = _Calls().pod(_with_time(source), label="triple")
    _ = job.run()

    def derived() -> Any:
        """Over a cold origin each time: a read keeps the origin's rows on it."""
        inner = DerivedSource(job.nodes["triple"])
        return _DroppingDerived(inner, tags_as_data={"subject": "who"}, source_id="d")

    whole = derived().as_table()
    previewed = derived()
    with pytest.raises(PreviewRefusedError, match="compute_all"):
        previewed.preview(2)
    for n in (1, 2):
        _same(previewed.preview(n, compute_all=True).as_table(), whole[:n])
    assert previewed._held_rows() is None  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("depth", [1, 2])
def test_a_derived_source_subclass_leaves_its_origins_cold(
    source: Any, depth: int
) -> None:
    """Review of bae54b3c, finding 2: the full read of a copy kept every row on the
    shared, cold origin. Each origin is read through a copy of its own."""
    origins: list[Any] = [source()]
    for level in range(depth):
        origins.append(
            DerivedSource(
                origins[-1],
                tags_as_data={"subject": f"s{level}"},
                source_id=f"o{level}",
            )
        )
    previewed = _DroppingDerived(
        origins[-1], tags_as_data={"timestamp": "t"}, source_id="d"
    )
    shown = previewed.preview(2, compute_all=True).as_table()
    for origin in [previewed, *origins[1:]]:
        assert origin._held_rows() is None  # pyright: ignore[reportPrivateUsage]
    _same(shown, previewed.as_table()[:2])


@pytest.mark.parametrize("origin", ["cached", "subclass_over_node"])
def test_a_derived_source_subclass_refuses_an_origin_that_could_keep_rows(
    source: Any, origin: str
) -> None:
    """When in doubt, refuse: an origin whose full read could write or keep rows (a
    ``CachedSource``; a job node a subclass reads directly) is refused, compute_all or
    not, and nothing is written or kept."""
    store = InMemoryArrowDatabase()
    if origin == "cached":
        made: Any = CachedSource(source(), cache_database=store)
    else:
        job = PipelineJob(name="itl1464_direct_node", database=InMemoryArrowDatabase())
        with job:
            _ = _Calls().pod(_with_time(source), label="triple")
        _ = job.run()
        made = job.nodes["triple"]
    derived = _DroppingDerived(made, tags_as_data={"subject": "who"}, source_id="d")
    for compute_all in (False, True):
        with pytest.raises(PreviewRefusedError):
            derived.preview(1, compute_all=compute_all)
    assert store.list_table_paths() == []
    assert derived._held_rows() is None  # pyright: ignore[reportPrivateUsage]


class _KeepingDerived(DerivedSource):
    """A DerivedSource of your own that keeps every table it reads in a list."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.kept: list[Any] = []

    def as_table(self, *args: Any, **kwargs: Any) -> Any:
        table = super().as_table(*args, **kwargs)
        self.kept.append(table)
        return table


def test_a_derived_source_subclass_refuses_an_origin_of_your_own_class(
    source: Any,
) -> None:
    """Review of 963d4929: a ``DerivedSource`` subclass as the origin was read through
    a shallow copy sharing its list, so it kept all 40 rows. Only an origin of exactly
    the library class is copied and read; a subclass origin is refused before it is
    read, compute_all or not."""
    origin = _KeepingDerived(source(), tags_as_data={"subject": "who"}, source_id="o")
    derived = _DroppingDerived(origin, tags_as_data={"timestamp": "t"}, source_id="d")
    for compute_all in (False, True):
        with pytest.raises(PreviewRefusedError):
            derived.preview(2, compute_all=compute_all)
    assert origin.kept == []
    assert origin._held_rows() is None  # pyright: ignore[reportPrivateUsage]
    assert derived._held_rows() is None  # pyright: ignore[reportPrivateUsage]


# ---------------------------------------------------------------------------
# CodeRabbit review of 8987839d
# ---------------------------------------------------------------------------


def test_a_cached_pod_over_a_pod_without_a_preview_is_refused(source: Any) -> None:
    """An inner pod that is not a pod of the library is refused with
    ``PreviewRefusedError``, not an ``AttributeError``."""
    calls = _Calls()

    class _Bare:
        """A function pod by protocol only: it delegates all but the preview."""

        def __init__(self, pod: Any) -> None:
            self._pod = pod

        def __getattr__(self, name: str) -> Any:
            if name in ("require_preview", "preview_data"):
                raise AttributeError(name)
            return getattr(self._pod, name)

    store = InMemoryArrowDatabase()
    pod = CachedFunctionPod(_Bare(calls.pod), result_database=store)  # pyright: ignore[reportArgumentType]  # conforms at runtime
    stream = pod.process(_with_time(source).select_data_columns(["timestamp", "x"]))
    with pytest.raises(PreviewRefusedError, match="inner pod _Bare"):
        stream.preview(2)
    assert calls.seen == []
    _untouched(store)


def test_a_data_function_writing_its_own_preview_call_is_refused(source: Any) -> None:
    """A subclass that writes ``preview_call`` again is not the library's class."""
    effects: list[Any] = []

    class _OwnPreview(PythonDataFunction):
        def preview_call(self, data: Any) -> Any:
            self.require_preview()
            effects.append(data)
            return self.call(data)

    calls = _Calls()
    inner: Any = calls.pod.data_function
    function: Any = _OwnPreview(inner._function, output_keys="y")
    stream = FunctionPod(function).process(
        _with_time(source).select_data_columns(["timestamp", "x"])
    )
    with pytest.raises(PreviewRefusedError, match="no read-only call"):
        stream.preview(2)
    assert calls.seen == []
    assert effects == []


def test_a_pod_writing_its_own_preview_data_is_refused(source: Any) -> None:
    """A pod subclass that writes ``preview_data`` again is not the library's."""

    class _OwnPreview(FunctionPod):
        def preview_data(self, data: Any) -> Any:
            return None

    calls = _Calls()
    stream = _OwnPreview(calls.pod.data_function).process(
        _with_time(source).select_data_columns(["timestamp", "x"])
    )
    with pytest.raises(PreviewRefusedError, match="not a pod of the library"):
        stream.preview(2)
    assert calls.seen == []


def test_a_held_result_shows_no_meta_columns(source: Any) -> None:
    """A held result of a cached function shows the columns a cold preview shows."""
    store = InMemoryArrowDatabase()
    calls = _Calls()

    def stream() -> Any:
        return _cached_function(calls, store).process(
            _with_time(source).select_data_columns(["timestamp", "x"])
        )

    cold = stream().preview(3).table
    held = stream()
    _ = held.as_table()
    shown = held.preview(3).table
    assert not any(name.startswith("__") for name in shown.column_names)
    assert shown.column_names == cold.column_names
    assert shown.column("y").to_pylist() == cold.column("y").to_pylist()


def test_a_held_empty_result_does_not_limit_the_input(source: Any) -> None:
    """A held input that gave no row (the function was inactive then) uses a row of
    the input without giving one: the limit is not pushed down."""
    calls = _Calls()
    function: Any = calls.pod.data_function
    function.set_active(False)
    pod = calls.pod
    pod.add_post_run_hook(lambda *_: function.set_active(True))
    stream = pod(_with_time(source).select_data_columns(["timestamp", "x"]))
    rows = iter(stream)
    _ = next(rows)
    shown = stream.preview(2).as_table()
    _same(shown, stream.as_table().select(shown.column_names)[:2])


class _BatchDeclaringMapData(MapData):
    """A MapData of your own that declares batches and drops each batch's first row."""

    BATCHES_GIVE_THE_WHOLE = True

    def unary_static_process(self, stream: Any) -> Any:
        result = super().unary_static_process(stream)
        table = result.as_table(columns={"source": True, "system_tags": True})
        return ArrowTableStream(
            table.slice(1),
            tag_columns=result.keys()[0],
            data_context=result.data_context_key,
        )


def test_an_operator_of_your_own_declaring_batches_is_computed_in_full(
    source: Any,
) -> None:
    """The batch shortcut is the library's: an operator of your own that declares it
    is computed in full by preview, only with compute_all."""

    def stream() -> Any:
        return _BatchDeclaringMapData({"note": "text"})(source())

    whole = stream().as_table()
    with pytest.raises(PreviewRefusedError, match="compute_all"):
        stream().preview(2)
    _same(stream().preview(2, compute_all=True).as_table(), whole[:2])


def test_a_semi_join_given_back_input_shows_no_meta_columns() -> None:
    """A left input with a meta column, given back by a SemiJoin with no shared tag:
    the preview has the columns of the full read."""
    left = ArrowTableStream(
        pa.table(
            {
                "k": ["a", "b", "c"],
                "x": pa.array([1, 2, 3], pa.int64()),
                "__debug": ["d1", "d2", "d3"],
            }
        ),
        tag_columns=["k"],
    )
    joined = SemiJoin()(left, _unrelated())
    shown = joined.preview(2).as_table()
    _same(shown, joined.as_table()[:2])
    assert "__debug" not in shown.column_names


def test_a_source_subclass_is_read_in_full_only_with_compute_all(
    spiral: Any,
) -> None:
    """A SpiralDB source of your own reads its whole table: only with compute_all."""

    class _Mine(SpiralDBTableSource):
        pass

    mine = _Mine(_PROJECT, "event", tag_only_columns=True)
    with pytest.raises(PreviewRefusedError, match="compute_all"):
        mine.preview(1)
    _same(mine.preview(2, compute_all=True).as_table(), mine.as_table()[:2])


def test_a_derived_source_subclass_refuses_an_origin_read_replaced_on_it(
    source: Any,
) -> None:
    """An origin of a library class whose ``as_table`` is replaced on the instance
    (it keeps what it reads) is refused, not read."""
    origin = source()
    kept: list[Any] = []
    read = origin.as_table

    def keeping(*args: Any, **kwargs: Any) -> Any:
        table = read(*args, **kwargs)
        kept.append(table)
        return table

    origin.as_table = keeping
    derived = _DroppingDerived(origin, tags_as_data={"timestamp": "t"}, source_id="d")
    with pytest.raises(PreviewRefusedError):
        derived.preview(2, compute_all=True)
    assert kept == []


def test_a_stream_subclass_is_read_in_full_only_with_compute_all() -> None:
    """A raw stream of your own may compute its rows in ``as_table``."""

    class _Dropping(ArrowTableStream):
        def as_table(self, *args: Any, **kwargs: Any) -> Any:
            return super().as_table(*args, **kwargs).slice(1)

    stream = _Dropping(_rows(), tag_columns=["subject", "timestamp"])
    with pytest.raises(PreviewRefusedError, match="compute_all"):
        stream.preview(2)
    _same(stream.preview(2, compute_all=True).as_table(), stream.as_table()[:2])


@pytest.mark.parametrize(
    "mapping",
    [
        {"note": "text", "x": "value"},
        {"x": "value", "note": "text"},
    ],
    ids=["note_first", "x_first"],
)
def test_a_mapping_of_several_entries_is_previewed_as_read(
    source: Any, mapping: dict[str, str]
) -> None:
    """Several mapping entries, in two orders, on data and on tags (with data-tags):
    the preview is the first rows of the sync read. ``preview`` is sync only."""
    tags = {"subject": "who", "timestamp": "t"}
    for stream in (
        source().map_data(mapping),
        source(tag_only_columns=False).map_tags(tags),
        source().map_tags(dict(reversed(list(tags.items())))),
    ):
        for n in (1, 5, 41):
            _same(stream.preview(n).as_table(), stream.as_table()[:n])


# ---------------------------------------------------------------------------
# Verification of 48f8533e: one exact-set gate (N55)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("wrapped", [False, True])
def test_a_data_function_overriding_both_preview_methods_is_refused(
    source: Any, wrapped: bool
) -> None:
    """A subclass writing ``require_preview`` and ``preview_call`` both: neither runs,
    plain or beneath a cached layer."""
    effects: list[Any] = []

    class _Both(PythonDataFunction):
        def require_preview(self) -> None:
            effects.append("checked")

        def preview_call(self, data: Any) -> Any:
            effects.append(data)
            return self.call(data)

    calls = _Calls()
    inner: Any = calls.pod.data_function
    function: Any = _Both(inner._function, output_keys="y")
    store = InMemoryArrowDatabase()
    if wrapped:
        function = CachedDataFunction(function, result_database=store)
    stream = FunctionPod(function).process(
        _with_time(source).select_data_columns(["timestamp", "x"])
    )
    with pytest.raises(PreviewRefusedError, match="no read-only call"):
        stream.preview(2)
    assert effects == []
    assert calls.seen == []
    _untouched(store)


@pytest.mark.parametrize("wrapped", [False, True])
def test_a_pod_overriding_both_preview_methods_is_refused(
    source: Any, wrapped: bool
) -> None:
    """A pod subclass writing ``require_preview`` and ``preview_data`` both: neither
    runs, plain or beneath a cached pod."""
    effects: list[Any] = []

    class _Both(FunctionPod):
        def require_preview(self) -> None:
            effects.append("checked")

        def preview_data(self, data: Any) -> Any:
            effects.append(data)
            return self.data_function.call(data)

    calls = _Calls()
    pod: Any = _Both(calls.pod.data_function)
    store = InMemoryArrowDatabase()
    if wrapped:
        pod = CachedFunctionPod(pod, result_database=store)
    stream = pod.process(_with_time(source).select_data_columns(["timestamp", "x"]))
    with pytest.raises(PreviewRefusedError):
        stream.preview(2)
    assert effects == []
    assert calls.seen == []
    _untouched(store)


class _DropsAfterAProducer(MapData):
    """An operator of your own that drops the first row when its input has a
    producer: it reads its input as the full read gives it."""

    def unary_static_process(self, stream: Any) -> Any:
        result = super().unary_static_process(stream)
        if stream.producer is None:
            return result
        table = result.as_table(columns={"source": True, "system_tags": True})
        return ArrowTableStream(
            table.slice(1),
            tag_columns=result.keys()[0],
            data_context=result.data_context_key,
        )


def test_a_whole_input_keeps_its_producer(source: Any) -> None:
    """The whole input a preview computes is given with its producer, so the rows are
    the first of the full read."""

    def stream() -> Any:
        return _DropsAfterAProducer({"y": "z"})(_Calls().pod(_with_time(source)))

    whole = stream().as_table()
    _same(stream().preview(2, compute_all=True).as_table(), whole[:2])


def test_a_data_function_without_preview_methods_is_refused(source: Any) -> None:
    """A data function written to the protocol before ``preview_call``: refused with
    ``PreviewRefusedError``, not ``AttributeError``."""
    calls = _Calls()

    class _Legacy:
        """Delegates every method but the preview ones."""

        def __init__(self, function: Any) -> None:
            self._function = function

        def __getattr__(self, name: str) -> Any:
            if name in ("require_preview", "preview_call"):
                raise AttributeError(name)
            return getattr(self._function, name)

    function: Any = _Legacy(calls.pod.data_function)
    stream = FunctionPod(function).process(
        _with_time(source).select_data_columns(["timestamp", "x"])
    )
    with pytest.raises(PreviewRefusedError, match="no read-only call"):
        stream.preview(2)
    assert calls.seen == []


def _all_columns_pod(seen: list[int], store: InMemoryArrowDatabase | None) -> Any:
    """A pod that takes every data column of the SpiralDB source, so it reads the
    source directly (no selection step between them), cached or not."""

    @function_pod(output_keys="y")
    def triple_all(timestamp: int | None, x: int | None, note: str | None) -> int:
        seen.append(timestamp or 0)
        return 3 * (x or 0)

    pod: Any = triple_all.pod
    if store is None:
        return pod
    return FunctionPod(CachedDataFunction(pod.data_function, result_database=store))


@pytest.mark.parametrize("cached", [False, True])
def test_held_results_are_shown_without_reading_the_input(
    spiral: Any, cached: bool
) -> None:
    """Rows held by an iteration are shown first; the source is scanned only when
    more rows are asked for than they give, with or without a limit pushed down."""
    seen: list[int] = []
    store = InMemoryArrowDatabase()
    stream = _all_columns_pod(seen, store if cached else None).process(
        spiral(tag_only_columns=["subject"])
    )
    rows = iter(stream)
    _ = [next(rows) for _ in range(2)]
    fake_spiral_sdk.scan_log.clear()
    seen.clear()
    shown = stream.preview(2).table
    assert fake_spiral_sdk.scan_log == []
    assert seen == []
    assert shown.column("timestamp").to_pylist() == [0, 1]
    more = stream.preview(3).table
    assert more.column("timestamp").to_pylist() == [0, 1, 2]
    assert seen == [2]
    _same(more, stream.as_table().select(more.column_names)[:3])


def test_held_results_of_every_input_read_no_input(spiral: Any) -> None:
    """After an iteration to the end, a preview past the last row reads nothing."""
    seen: list[int] = []
    stream = _all_columns_pod(seen, None).process(spiral(tag_only_columns=["subject"]))
    _ = list(stream.iter_data())
    fake_spiral_sdk.scan_log.clear()
    seen.clear()
    view = stream.preview(41)
    assert len(view) == 40
    assert fake_spiral_sdk.scan_log == []
    assert seen == []


def test_an_operator_in_the_library_namespace_is_not_the_library(
    source: Any,
) -> None:
    """A batch-declaring operator of your own named into the ``orcapod`` namespace is
    still not one of the library's classes."""

    class _Named(_BatchDeclaringMapData):
        BATCHES_GIVE_THE_WHOLE = True

    _Named.__module__ = "orcapod.local_extension"

    def stream() -> Any:
        return _Named({"note": "text"})(source())

    whole = stream().as_table()
    with pytest.raises(PreviewRefusedError, match="compute_all"):
        stream().preview(2)
    _same(stream().preview(2, compute_all=True).as_table(), whole[:2])
