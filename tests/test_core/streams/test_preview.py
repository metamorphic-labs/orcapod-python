"""``stream.preview(n)``: the first rows of the sync result, and no more work (ITL-1464)."""

from __future__ import annotations

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
from orcapod.core.operators import MapData
from orcapod.core.sources import ArrowTableSource, CachedSource, DerivedSource
from orcapod.core.sources.polling_source import PollingSource
from orcapod.core.streams import preview as preview_module
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import PreviewRefusedError, RowIdentifierError
from orcapod.side_effects import SideEffectPod
from orcapod.sources import SpiralDBTableSource
from orcapod.streams import StreamView, ViewStatus
from tests.test_core.streams.test_head_and_preview import _FakeDynamicSource
from tests.test_databases import fake_spiral_sdk

ROWS = 40
_PROJECT = "sample"
_KEYS = pa.schema(
    [pa.field("subject", pa.string()), pa.field("timestamp", pa.uint64())]
)


def _rows(n: int = ROWS) -> pa.Table:
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
    pod._post_run_hooks.append(fired.append)  # pyright: ignore[reportPrivateUsage]  # a registered hook
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


def test_a_derived_source_is_read_whole(source: Any) -> None:
    """Refused, since its origin may compute; allowed by ``compute_all``."""
    derived = DerivedSource(
        source(), tags_as_data={"timestamp": "time"}, source_id="derived"
    )
    with pytest.raises(PreviewRefusedError, match="compute_all"):
        derived.preview(3)
    _same(
        derived.preview(3, compute_all=True).as_table(), derived.as_table().slice(0, 3)
    )


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
