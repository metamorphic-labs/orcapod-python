"""``run()`` over a source read on demand: batches, and a bounded view (ITL-1464).

The differential compares the batch-wise result with the result of the operators over the
whole input (``as_table()`` of a stream that was never run), for every column group. Runs
against the disk-backed fake of the SpiralDB client.
"""

from __future__ import annotations

import pickle
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

from orcapod import col, function_pod
from orcapod.core.operators import DropNulls, MapData
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import base, view as view_module
from orcapod.core.streams.view import BoundedStreamView, StreamView, WindowOfRows
from orcapod.errors import BoundedViewError, RowIdentifierError, SourceTooLargeError
from orcapod.sources import SpiralDBTableSource
from orcapod.streams import ViewStatus
from tests.test_databases import fake_spiral_sdk

_PROJECT = "sample"
_KEYS = pa.schema(
    [pa.field("subject", pa.string()), pa.field("timestamp", pa.uint64())]
)


def _rows(n: int) -> pa.Table:
    """``n`` rows: two keys, data with nulls, strings longer than the display shows."""
    return pa.table(
        {
            "subject": [f"m{i * 3 // max(n, 1)}" for i in range(n)],
            "timestamp": pa.array(range(n), pa.uint64()),
            "amplitude": pa.array(
                [None if i % 7 == 3 else i + 0.5 for i in range(n)], pa.float32()
            ),
            "note": [f"{i:04d}" + "a" * (200 if i % 5 == 0 else 3) for i in range(n)],
            "trace": pa.array(
                [[float(i)] * (i % 4) for i in range(n)], pa.list_(pa.float64())
            ),
            "flag": [i % 2 == 0 for i in range(n)],
        }
    )


@pytest.fixture()
def source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Make a source of ``n`` rows (default 40), scanned 6 rows per batch."""
    monkeypatch.setattr(fake_spiral_sdk, "scan_batch_rows", 6)
    with fake_spiral_sdk.install(tmp_path / "spiral"):

        def make(n: int = 40, **kwargs: Any) -> SpiralDBTableSource:
            name = f"event_{n}"
            project = fake_spiral_sdk.ExternalProject(_PROJECT)
            try:
                return SpiralDBTableSource(_PROJECT, name, **kwargs)
            except Exception:  # noqa: BLE001 - not made yet
                project.create_table(f"default.{name}", key_schema=_KEYS).write(
                    _rows(n)
                )
            return SpiralDBTableSource(_PROJECT, name, **kwargs)

        yield make


@pytest.fixture()
def small_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    """A result of more than 20 rows is bounded, with a window of 12 rows."""
    monkeypatch.setattr(view_module, "RUN_VIEW_COMPLETE_ROWS", 20)
    monkeypatch.setattr(view_module, "RUN_VIEW_WINDOW_ROWS", 12)
    import orcapod.core.operators.static_output_pod as pods

    monkeypatch.setattr(pods, "RUN_VIEW_COMPLETE_ROWS", 20)


CHAINS: dict[str, Callable[[Any], Any]] = {
    "source": lambda s: s,
    "drop_nulls": lambda s: s.drop_nulls(["amplitude", "note"]),
    "drop_nulls_all": lambda s: s.drop_nulls(),
    "filter": lambda s: s.filter(col("amplitude") >= 5.0, flag=True),
    "filter_none_left": lambda s: s.filter(col("amplitude") >= 1e6),
    "map_data": lambda s: s.map_data({"note": "text", "amplitude": "uv"}),
    "map_tags": lambda s: s.map_tags({"timestamp": "t", "subject": "who"}),
    "map_columns": lambda s: s.map_columns({"timestamp": "t", "note": "text"}),
    "add_tags": lambda s: s.add_tags(cohort="A", run=3),
    "select_tags": lambda s: s.select_tag_columns(["timestamp"]),
    "select_data": lambda s: s.select_data_columns(["note", "amplitude"]),
    "select_data_pass_through": lambda s: s.select_data_columns(list(s.keys()[1])),
    "drop_tags": lambda s: s.drop_tag_columns(["subject", "timestamp"]),
    "drop_data": lambda s: s.drop_data_columns(["trace", "note"]),
    "select": lambda s: s.select_columns("timestamp", "note", "flag"),
    "drop": lambda s: s.drop_columns("subject", "trace"),
    "several": lambda s: (
        s.drop_nulls(["amplitude"])
        .filter(flag=True)
        .map_data({"note": "text"})
        .add_tags(cohort="A")
        .drop_columns("trace")
    ),
}

CONFIGS: list[dict[str, Any]] = [
    {},
    {"source": True, "system_tags": True},
    {"context": True},
    {"content_hash": True},
    {"sort_by_tags": True},
    {"all_info": True},
]


def _same(left: pa.Table, right: pa.Table) -> None:
    assert left.schema.equals(right.schema, check_metadata=True)
    assert left.equals(right)


@pytest.mark.parametrize("tag_only", [True, False, ["timestamp"]])
@pytest.mark.parametrize("batch_rows", [1, 5, 8192])
@pytest.mark.parametrize("name", list(CHAINS))
def test_the_batches_give_the_whole(
    source: Any, name: str, batch_rows: int, tag_only: Any
) -> None:
    """Rows, order, types, nullability, tokens, record ids and row hashes are those of
    the operators over the whole input; the kept result is the one a read keeps."""

    def stream() -> Any:
        if name == "map_columns" and tag_only is not True:
            pytest.skip("MapColumns takes no data-tags yet (ITL-1022)")
        return CHAINS[name](source(batch_rows=batch_rows, tag_only_columns=tag_only))

    whole = stream()
    ran = stream()
    assert base.reads_in_batches(ran)
    view = ran.run()
    assert type(view) is StreamView
    _same(view.as_table(), whole.as_table())
    for config in CONFIGS:
        _same(ran.as_table(columns=config), whole.as_table(columns=config))
    assert ran.keys() == whole.keys()
    assert ran.content_hash() == whole.content_hash()
    assert [(t.as_dict(all_info=True), d.content_hash()) for t, d in ran] == [
        (t.as_dict(all_info=True), d.content_hash()) for t, d in whole
    ]


@pytest.mark.parametrize("name", list(CHAINS))
def test_a_small_run_shows_what_the_whole_shows(source: Any, name: str) -> None:
    """Header, text and HTML of a small result are those of a view of the whole."""
    ran = CHAINS[name](source(tag_only_columns=True))
    view = ran.run()
    whole = CHAINS[name](source(tag_only_columns=True))
    expected = whole._view_of_rows(whole.as_table(), whole._RUN_ORIGIN)  # pyright: ignore[reportPrivateUsage]  # the view as it was built before
    assert view.header() == expected.header()
    assert str(view) == str(expected)
    assert view._repr_html_() == expected._repr_html_()
    assert ran.head(3).header() == whole.head(3).header()
    assert ran.head(3).table.equals(whole.head(3).table)
    assert ran._repr_html_() == whole._repr_html_()


def test_a_run_of_at_most_the_limit_is_kept_and_reads_once(source: Any) -> None:
    """A small result is kept at every step of the chain: no second scan."""
    first = source().drop_nulls(["amplitude"])
    out = first.map_data({"note": "text"})
    fake_spiral_sdk.scan_log.clear()
    view = out.run()
    scans = len(fake_spiral_sdk.scan_log)
    assert out.as_table().equals(view.as_table())
    assert out.run().as_table().equals(view.as_table())
    assert first.head(2).status is ViewStatus.ROWS
    assert len(first.as_table()) == len(view)
    assert len(fake_spiral_sdk.scan_log) == scans


# ---------------------------------------------------------------------------
# The bounded view
# ---------------------------------------------------------------------------


def _bounded_and_complete(stream: Any) -> tuple[BoundedStreamView, StreamView]:
    view = stream.run()
    assert isinstance(view, BoundedStreamView)
    table = stream.as_table()
    return view, stream._view_of_rows(table, stream._RUN_ORIGIN)  # the complete view


@pytest.mark.usefixtures("small_limits")
@pytest.mark.parametrize("n", [33, 40, 1200])
@pytest.mark.parametrize("name", ["source", "map_data", "drop_nulls", "several"])
def test_a_bounded_view_is_displayed_as_the_complete_one(
    source: Any, name: str, n: int
) -> None:
    """Header, legend, shape line and rows, in text and HTML."""
    chain = CHAINS[name]
    if name == "several" and n < 100:
        pytest.skip("leaves fewer rows than the limit")
    view, complete = _bounded_and_complete(chain(source(n)))
    assert view.header() == complete.header()
    assert str(view) == str(complete)
    assert view._repr_html_() == complete._repr_html_()
    assert repr(view) == repr(complete)
    assert view.total_rows == complete.total_rows == len(view) == len(complete)
    assert view.status is complete.status
    assert view.tag_columns == complete.tag_columns
    assert view.data_tag_columns == complete.data_tag_columns


@pytest.mark.usefixtures("small_limits")
def test_head_and_tail_within_the_window_are_the_complete_views(source: Any) -> None:
    """Rows and header; and the cuts and sorts of those."""
    view, complete = _bounded_and_complete(source().map_data({"note": "text"}))
    for n in (0, 1, 5, 12):
        for cut in ("head", "tail"):
            mine, theirs = getattr(view, cut)(n), getattr(complete, cut)(n)
            assert type(mine) is StreamView
            _same(mine.as_table(), theirs.as_table())
            assert mine.header() == theirs.header()
            assert str(mine) == str(theirs)
            assert mine.sort_by_tags().header() == theirs.sort_by_tags().header()
    assert view.head(8).tail(3).header() == complete.head(8).tail(3).header()
    assert view.tail(8).head(3).header() == complete.tail(8).head(3).header()


@pytest.mark.usefixtures("small_limits")
@pytest.mark.parametrize(
    "ask",
    [
        lambda v: v.as_table(),
        lambda v: v.as_polars_df(),
        lambda v: v.table,
        lambda v: v.sort_by_tags(),
        lambda v: v.head(13),
        lambda v: v.tail(13),
    ],
    ids=["as_table", "as_polars_df", "table", "sort_by_tags", "head", "tail"],
)
def test_what_needs_rows_not_held_is_refused(
    source: Any, ask: Callable[[Any], Any]
) -> None:
    """A part is never given as the whole; the error names the stream's method."""
    view = source().run()
    with pytest.raises(BoundedViewError, match=r"stream\.as_table\(\)"):
        ask(view)


@pytest.mark.usefixtures("small_limits")
def test_a_bounded_view_checks_the_number_of_rows(source: Any) -> None:
    """As a complete view does, before the window."""
    view = source().run()
    with pytest.raises(TypeError):
        view.head(True)
    with pytest.raises(ValueError, match="negative"):
        view.tail(-1)
    with pytest.raises(AttributeError):
        _ = view.no_such_member
    assert len(pickle.loads(pickle.dumps(view))) == len(view)


@pytest.mark.usefixtures("small_limits")
@pytest.mark.parametrize("n", [19, 20, 21])
def test_the_limit_is_counted_in_rows_whatever_the_batches(source: Any, n: int) -> None:
    """Equal data gives the same kind of view: however cut, run again, or already read."""
    for batch_rows in (1, 7, 8192):
        stream = source(n, batch_rows=batch_rows).map_data({"note": "text"})
        bounded = n > 20
        assert isinstance(stream.run(), BoundedStreamView) is bounded
        assert isinstance(stream.run(), BoundedStreamView) is bounded
        _ = stream.as_table()  # the stream now holds every row
        assert isinstance(stream.run(), BoundedStreamView) is bounded
        assert (
            isinstance(source(n, batch_rows=batch_rows).run(), BoundedStreamView)
            is bounded
        )


@pytest.mark.usefixtures("small_limits")
def test_head_after_a_large_run_shows_the_window(source: Any) -> None:
    """Default columns, within the window, while the inputs are unchanged."""
    stream = source().drop_nulls(["amplitude"])
    view = stream.run()
    fake_spiral_sdk.scan_log.clear()
    assert stream.head(3).table.equals(view.head(3).table)
    assert stream.head(3).header() == view.head(3).header()
    assert "first 10 of 34 computed rows, then sorted by tags" in stream._repr_html_()
    assert stream.head(3, columns={"source": True}).status is ViewStatus.NOT_COMPUTED
    assert stream.head(13).status is ViewStatus.NOT_COMPUTED
    assert fake_spiral_sdk.scan_log == []
    stream.clear_cache()
    assert stream.head(3).status is ViewStatus.NOT_COMPUTED


@pytest.mark.usefixtures("small_limits")
def test_head_after_the_source_moved_shows_no_old_rows(source: Any) -> None:
    """The window is not shown once an input changed."""
    root = source()
    stream = root.map_data({"note": "text"})
    _ = stream.run()
    fake_spiral_sdk.ExternalProject(_PROJECT).table("default.event_40").write(
        _rows(41).slice(40)
    )
    assert root.check_for_new_data()
    assert stream.head(3).status is ViewStatus.NOT_COMPUTED
    assert len(stream.run()) == 41


@pytest.mark.usefixtures("small_limits")
def test_reads_after_a_large_run_give_every_row(source: Any) -> None:
    """``as_table()`` and iteration read again; a bounded run keeps no rows."""
    stream = CHAINS["several"](source(200))
    assert isinstance(stream.run(), BoundedStreamView)
    whole = CHAINS["several"](source(200))
    _same(stream.as_table(), whole.as_table())
    assert len(list(stream)) == whole.as_table().num_rows


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class _Refusing(DropNulls):
    """Reads its input, then raises when there are rows."""

    BATCHES_GIVE_THE_WHOLE = True

    def unary_static_process(self, stream: Any) -> Any:
        if stream.as_table().num_rows:
            raise RuntimeError("the operator refused")
        return super().unary_static_process(stream)


def test_max_rows_holds_for_the_whole_read(source: Any) -> None:
    """The read is refused as ``as_table()`` refuses it; no view is returned."""
    for make in (
        lambda: source(max_rows=30),
        lambda: source(max_rows=30, where={"flag": True}, limit=25),
    ):
        stream = make().map_data({"note": "text"})
        try:
            expected: object = len(make().as_table())
        except SourceTooLargeError as error:
            expected = str(error)
        try:
            found: object = len(stream.run())
        except SourceTooLargeError as error:
            found = str(error)
        assert found == expected
    late = source(max_rows=10, where={"flag": True})
    with pytest.raises(SourceTooLargeError):
        _ = late.drop_nulls().run()
    assert len(late.drop_nulls().head()) == 0


def test_a_late_source_error_is_raised_as_a_whole_read_raises_it(
    source: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Also before an operator's own error on an earlier batch."""
    root = source(batch_rows=6)
    original = SpiralDBTableSource._key_tokens  # pyright: ignore[reportPrivateUsage]  # a failing row, late

    def failing(self: Any, table: pa.Table) -> list[str]:
        if 30 in table.column("timestamp").to_pylist():
            raise RowIdentifierError("a null key, late")
        return original(self, table)

    monkeypatch.setattr(SpiralDBTableSource, "_key_tokens", failing)
    for stream in (root.map_data({"note": "text"}), _Refusing(["amplitude"])(root)):
        with pytest.raises(RowIdentifierError, match="late"):
            _ = stream.as_table()
        with pytest.raises(RowIdentifierError, match="late"):
            _ = stream.run()
        assert stream.head().status is ViewStatus.NOT_COMPUTED


def test_an_operator_error_keeps_nothing(source: Any) -> None:
    """The operator's error is raised, and nothing is shown after it."""
    stream = _Refusing(["amplitude"])(source())
    with pytest.raises(RuntimeError, match="refused"):
        _ = stream.run()
    assert stream.head().status is ViewStatus.NOT_COMPUTED


# ---------------------------------------------------------------------------
# What is not read in batches
# ---------------------------------------------------------------------------


class _Subclass(MapData):
    """A subclass may change what an operator does: it declares batches itself."""


def test_only_declared_operators_over_an_on_demand_source_are_batched(
    source: Any,
) -> None:
    """Everything else computes as before, and its view holds every row."""

    @function_pod(output_keys="double")
    def double(amplitude: float) -> float:
        return 2 * amplitude

    root = source()
    held = ArrowTableSource(_rows(40), tag_columns=["subject"])
    clean = root.drop_nulls(["amplitude"])
    for stream in (
        held,
        held.drop_nulls(),
        _Subclass({"note": "text"})(root),
        clean.polars_filter(pl.col("flag")),
        clean.group_by("subject"),
        clean.batch(4),
        clean.join(held.select_data_columns(["flag"]).map_data({"flag": "f"})),
        double.pod(clean),
        MapData({"double": "d"})(double.pod(clean)),
    ):
        assert not base.reads_in_batches(stream)
        assert not isinstance(stream, SpiralDBTableSource)
        assert "BATCHES_GIVE_THE_WHOLE" not in vars(_Subclass)
    assert StaticOutputOperatorPod.BATCHES_GIVE_THE_WHOLE is False


# ---------------------------------------------------------------------------
# The window
# ---------------------------------------------------------------------------


def _column(kind: pa.DataType, rows: int) -> pa.Array[Any]:
    if kind == pa.uuid():
        storage = pa.array([uuid.UUID(int=i).bytes for i in range(rows)], pa.binary(16))
        return pa.ExtensionArray.from_storage(pa.uuid(), storage)
    return pa.array([None if i % 5 == 4 else i for i in range(rows)], pa.int64()).cast(
        kind
    )


@pytest.mark.parametrize("cut", [1, 3, 8, 50])
def test_the_window_is_the_first_and_last_rows(
    monkeypatch: pytest.MonkeyPatch, cut: int
) -> None:
    """Whatever the batches, for every kind of column; and it holds no batch."""
    monkeypatch.setattr(view_module, "RUN_VIEW_COMPLETE_ROWS", 20)
    monkeypatch.setattr(view_module, "RUN_VIEW_WINDOW_ROWS", 8)
    table = pa.table(
        {
            "i": _column(pa.int64(), 50),
            "half": _column(pa.float32(), 50).cast(pa.float16()),
            "text": _column(pa.large_string(), 50),
            "uuid": _column(pa.uuid(), 50),
            "decimal": _column(pa.decimal256(30, 2), 50),
            "lists": pa.array([[i] * (i % 3) for i in range(50)]),
            "structs": pa.array([{"a": i, "b": [str(i)]} for i in range(50)]),
            "maps": pa.array(
                [[(i, "x")] for i in range(50)], pa.map_(pa.int64(), pa.string())
            ),
        }
    )
    window = WindowOfRows()
    for start in range(0, 50, cut):
        window.add(table.slice(start, cut))
    assert window.total == 50
    assert window.complete is None
    _same(window.first, table.slice(0, 8))
    _same(window.last, table.slice(42))
