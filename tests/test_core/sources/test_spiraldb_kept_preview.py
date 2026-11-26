"""A SpiralDB source keeps its preview (ITL-1485), counted in SDK calls on the fake.

A source reads one pinned version, moved only by its source check (a run's), so the rows
of its last preview are its preview for as long as the version, the condition and the
limit are those it read under. A second preview makes no SDK call; a preview that asks for
more rows than are kept, or one after the version moved, reads again.
"""

from __future__ import annotations

import collections
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.core.sources import spiraldb_table_source as spiral_module
from orcapod.core.streams.view import StreamView
from orcapod.databases import InMemoryArrowDatabase
from orcapod.databases.spiraldb_schema_table import SCHEMA_TABLE
from orcapod.operators import SelectColumns
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_spiral_sdk

_PROJECT = "kept-preview"
# Two key columns (tags) and two data columns.
_SCHEMA = pa.schema(
    [
        pa.field("subject", pa.large_string(), nullable=False),
        pa.field("k", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
        pa.field("y", pa.large_string(), nullable=True),
    ]
)
_KEYS = pa.schema([_SCHEMA.field("subject"), _SCHEMA.field("k")])


def _rows(ks: list[int]) -> pa.Table:
    return pa.table(
        {
            "subject": [f"s{k % 2}" for k in ks],
            "k": ks,
            "x": [10 * k for k in ks],
            "y": [f"v{k}" for k in ks],
        },
        schema=_SCHEMA,
    )


def _table() -> Any:  # pyright: ignore[reportExplicitAny]  # the fake's table
    return fake_spiral_sdk.ExternalProject(_PROJECT).table("default.t")


@pytest.fixture
def calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[collections.Counter[str]]:
    """A fake project with table ``default.t`` of 30 rows; counts every SDK call on it.

    Counted: "lookup" (open by name), "by_id" (open a version by id), "open_schemas"
    (open the dataset's schema table), "schema" (a snapshot's schema), "scan" (rows)
    and "count" (keys), and every read of the schema table's rows ("read_schemas").
    """
    counts: collections.Counter[str] = collections.Counter()

    def name_label(args: tuple[Any, ...], kwargs: Any) -> str:  # pyright: ignore[reportExplicitAny]
        return "open_schemas" if args[3] == SCHEMA_TABLE else "lookup"

    def handle_label(user: str) -> Callable[..., str]:
        def label(args: tuple[Any, ...], kwargs: Any) -> str:  # pyright: ignore[reportExplicitAny]
            if args[0]._table["name"] == SCHEMA_TABLE:
                return f"{user}_schemas"
            columns = args[1] if len(args) > 1 else kwargs.get("columns")
            return "count" if user == "scan" and columns else user

        return label

    counted: dict[tuple[type, str], Callable[..., str]] = {
        (fake_spiral_sdk.FakeClient, "keyed_table"): name_label,
        (fake_spiral_sdk.FakeClient, "keyed_table_by_id"): lambda a, k: "by_id",
        (fake_spiral_sdk.FakeKeyedTable, "schema"): handle_label("schema"),
        (fake_spiral_sdk.FakeKeyedTable, "scan"): handle_label("scan"),
    }
    for (cls, name), label_of in counted.items():
        original: Callable[..., Any] = getattr(cls, name)  # pyright: ignore[reportExplicitAny]

        def wrapper(
            *args: Any,  # pyright: ignore[reportExplicitAny]  # passed through
            _original: Callable[..., Any] = original,  # pyright: ignore[reportExplicitAny]
            _label_of: Callable[..., str] = label_of,
            **kwargs: Any,  # pyright: ignore[reportExplicitAny]
        ) -> Any:  # pyright: ignore[reportExplicitAny]
            counts[_label_of(args, kwargs)] += 1
            return _original(*args, **kwargs)

        monkeypatch.setattr(cls, name, wrapper)
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        table = fake_spiral_sdk.ExternalProject(_PROJECT).create_table(
            "default.t", key_schema=_KEYS
        )
        table.write(_rows(list(range(30))))
        table.record(_SCHEMA)
        counts.clear()
        yield counts


def _source(**kwargs: Any) -> SpiralDBTableSource:  # pyright: ignore[reportExplicitAny]  # options
    return SpiralDBTableSource(project_id=_PROJECT, table_name="t", **kwargs)


def _scans() -> list[int | None]:
    """The limits of the row scans made since the log was last cleared."""
    return [record.limit for record in fake_spiral_sdk.scan_log]


def _same_view(kept: StreamView, fresh: StreamView) -> None:
    """``kept`` is the view a source keeping nothing gives for the same arguments."""
    assert kept.table.equals(fresh.table, check_metadata=True)
    assert kept.description == fresh.description
    assert kept.total_rows == fresh.total_rows
    assert kept.total_is_estimate == fresh.total_is_estimate
    assert kept.tag_columns == fresh.tag_columns
    assert kept.status == fresh.status


def _fresh(**kwargs: Any) -> SpiralDBTableSource:  # pyright: ignore[reportExplicitAny]  # options
    """A new source: its first preview reads as ``main`` does, keeping nothing yet."""
    return _source(**kwargs)


# Every argument form a preview takes: the columns asked for and the order shown.
_COLUMN_FORMS: list[dict[str, Any]] = [  # pyright: ignore[reportExplicitAny]
    {},
    {"all_info": True},
    {"columns": {"sort_by_tags": True}},
    {"columns": {"source": True, "system_tags": True}},
    {"columns": {"context": True, "sort_by_tags": True}},
]


@pytest.mark.parametrize("tag_only", [True, ()], ids=["plain_tags", "data_tags"])
def test_a_second_preview_makes_no_call(
    calls: collections.Counter[str], tag_only: bool | tuple[str, ...]
) -> None:
    source = _source(tag_only_columns=tag_only)
    calls.clear()
    first = source.head()
    assert calls["scan"] == 1
    calls.clear()
    second = source.head()
    assert calls == {}
    _same_view(second, first)
    # The notebook preview is the same head: no call either.
    html = source._repr_html_()
    assert calls == {}
    assert html == _fresh(tag_only_columns=tag_only)._repr_html_()


@pytest.mark.parametrize("form", range(len(_COLUMN_FORMS)))
def test_other_columns_and_fewer_rows_are_answered_from_the_kept_rows(
    calls: collections.Counter[str], form: int
) -> None:
    source = _source(tag_only_columns=("subject",))
    _ = source.head(12)
    calls.clear()
    for n in (12, 5, 1, 0):
        kept = source.head(n, **_COLUMN_FORMS[form])
        assert calls == {}, n
        fresh = _fresh(tag_only_columns=("subject",)).head(n, **_COLUMN_FORMS[form])
        calls.clear()
        _same_view(kept, fresh)


def test_more_rows_than_kept_reads_again_once(calls: collections.Counter[str]) -> None:
    source = _source(tag_only_columns=True)
    _ = source.head(10)
    fake_spiral_sdk.scan_log.clear()
    calls.clear()
    more = source.head(20)
    # One read of the 20 rows from the start (a SpiralDB scan has no offset).
    assert _scans() == [20]
    assert calls["scan"] == 1
    _same_view(more, _fresh(tag_only_columns=True).head(20))
    calls.clear()
    _ = source.head(15)
    _ = source.head(20)
    assert calls == {}


def test_a_source_whose_rows_are_all_kept_answers_any_size(
    calls: collections.Counter[str],
) -> None:
    source = _source(tag_only_columns=True)
    _ = source.head(50)  # all 30 rows
    calls.clear()
    for n in (100, 30, 31, 10_000):
        _same_view(source.head(n), _fresh(tag_only_columns=True).head(n))
        calls.clear()
        _ = source.head(n)
        assert calls == {}


def test_a_changed_table_is_shown_after_the_source_check(
    calls: collections.Counter[str],
) -> None:
    source = _source(tag_only_columns=True)
    before = source.head(40)
    assert before.total_rows == 30
    _table().write(_rows([100, 101]))
    calls.clear()
    # The source reads its pinned version until it is checked: the kept rows are it.
    unchecked = source.head(40)
    assert calls == {}
    _same_view(unchecked, before)
    assert source.check_for_new_data()
    calls.clear()
    after = source.head(40)
    assert calls["scan"] == 1
    assert after.total_rows == 32
    assert 100 in after.table["k"].to_pylist()
    calls.clear()
    _ = source.head(40)
    assert calls == {}


@pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
def test_a_job_run_that_takes_in_new_data_makes_the_next_preview_read(
    calls: collections.Counter[str],
    orchestrator: type[AsyncPipelineOrchestrator] | None,
) -> None:
    source = _source(tag_only_columns=True)
    job = PipelineJob(name="kept-preview", database=InMemoryArrowDatabase())
    with job:
        SelectColumns(["subject", "k", "x"])(source, label="read")
    runner = orchestrator() if orchestrator is not None else None
    _ = source.head()
    # A run that finds nothing new leaves the kept preview in use.
    _ = job.run(orchestrator=runner)
    calls.clear()
    _ = source.head()
    assert calls == {}
    # A run that takes in a new version: the next preview reads it.
    _table().write(_rows([-2]))
    _ = job.run(orchestrator=runner)
    calls.clear()
    after = source.head()
    assert calls["scan"] == 1
    assert -2 in after.table["k"].to_pylist()
    calls.clear()
    _ = source.head()
    assert calls == {}


def test_a_restricted_copy_keeps_its_own_preview(
    calls: collections.Counter[str],
) -> None:
    source = _source(tag_only_columns=True)
    whole = source.head()
    restricted = source.where(subject="s1")
    calls.clear()
    seen = restricted.head()
    assert calls["scan"] == 1
    assert set(seen.table["subject"].to_pylist()) == {"s1"}
    _same_view(seen, _fresh(tag_only_columns=True).where(subject="s1").head())
    calls.clear()
    _ = restricted.head(3)
    _same_view(source.head(), whole)
    assert calls == {}


@pytest.mark.parametrize("limit", [3, 12])
def test_a_limited_source_keeps_its_preview(
    calls: collections.Counter[str], limit: int
) -> None:
    source = _source(tag_only_columns=True, limit=limit)
    _ = source.head()
    calls.clear()
    # Up to the limit, or fewer rows than were read.
    for n in (10, 2, 20) if limit == 3 else (10, 2):
        kept = source.head(n)
        assert calls == {}, n
        fresh = _fresh(tag_only_columns=True, limit=limit).head(n)
        calls.clear()
        _same_view(kept, fresh)


def test_a_given_version_keeps_its_preview(calls: collections.Counter[str]) -> None:
    source = _source(tag_only_columns=True, asof=_table().last_modified_at())
    first = source.head()
    _table().write(_rows([300]))
    assert not source.check_for_new_data()
    calls.clear()
    _same_view(source.head(), first)
    assert calls == {}


def test_a_closed_source_keeps_nothing(calls: collections.Counter[str]) -> None:
    source = _source(tag_only_columns=True)
    _ = source.head()
    source.close()
    with pytest.raises(RuntimeError, match="closed"):
        _ = source.head()


def test_a_preview_over_the_row_bound_is_not_kept(
    calls: collections.Counter[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(spiral_module, "PREVIEW_KEPT_ROWS", 5)
    source = _source(tag_only_columns=True)
    _ = source.head(6)
    calls.clear()
    _ = source.head(6)
    assert calls["scan"] == 1
    _ = source.head(5)  # kept: 5 rows
    calls.clear()
    _ = source.head(5)
    _ = source.head(2)
    assert calls == {}


def test_a_preview_over_the_byte_bound_is_not_kept(
    calls: collections.Counter[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(spiral_module, "PREVIEW_KEPT_BYTES", 1)
    source = _source(tag_only_columns=True)
    _ = source.head()
    calls.clear()
    _ = source.head()
    assert calls["scan"] == 1


def test_full_reads_neither_use_nor_fill_the_kept_preview(
    calls: collections.Counter[str],
) -> None:
    source = _source(tag_only_columns=True)
    _ = source.as_table()
    calls.clear()
    _ = source.head()
    assert calls["scan"] == 1
    calls.clear()
    assert source.as_table().num_rows == 30
    assert calls["scan"] == 1


def test_an_empty_restriction_keeps_its_preview(
    calls: collections.Counter[str],
) -> None:
    source = _source(tag_only_columns=True).where(subject="nobody")
    first = source.head()
    assert first.table.num_rows == 0
    calls.clear()
    _same_view(source.head(), first)
    assert calls == {}


# Option sets whose header total is worked out differently: a cap below the rows read,
# a limit above and below them, a condition.
_OPTIONS: dict[str, dict[str, Any]] = {  # pyright: ignore[reportExplicitAny]
    "small_cap": {"max_rows": 5},
    "small_cap_limit": {"max_rows": 5, "limit": 8},
    "limit_above_rows": {"limit": 40},
    "where": {"where": {"subject": "s1"}},
    "where_limit": {"where": {"subject": "s1"}, "limit": 4},
    # Two conditions, a tag and a data column, in both orders (CodeRabbit, 62db6a1a).
    "where_two": {"where": {"subject": "s1", "x": 30}},
    "where_two_reversed": {"where": {"x": 30, "subject": "s1"}},
}


@pytest.mark.parametrize("options", sorted(_OPTIONS))
@pytest.mark.parametrize("first", [10, 50])
def test_every_kept_view_is_the_view_a_fresh_read_gives(
    calls: collections.Counter[str], options: str, first: int
) -> None:
    source = _source(tag_only_columns=True, **_OPTIONS[options])
    _ = source.head(first)
    counts = 0
    for n in (first, 7, 3, 0):
        calls.clear()
        kept = source.head(n)
        # Every preview the kept record answers reads no row. The one call: after a
        # read of every row a condition chose (its total exact, so not counted then),
        # the first smaller preview counts the table, once (round 3 of the review).
        assert "scan" not in calls, n
        assert set(calls) <= {"lookup", "open_schemas", "schema", "count"}, n
        counts += calls["count"]
        _same_view(kept, _fresh(tag_only_columns=True, **_OPTIONS[options]).head(n))
        calls.clear()
        _ = source.head(n)
        assert calls == {}, n
    assert counts <= (1 if "where" in _OPTIONS[options] else 0)


@pytest.mark.parametrize(
    ("options", "first", "then"),
    [
        # The cap below both previews: each counts to its own bound on main.
        ({"max_rows": 5}, {"n": 10}, {"n": 7, "columns": {"sort_by_tags": True}}),
        # A condition read whole: the table's count is shown by the smaller preview.
        ({"where": {"subject": "s1"}}, {"n": 40}, {"n": 10}),
    ],
    ids=["small_cap_sorted", "where_whole_then_fewer"],
)
def test_a_smaller_preview_makes_no_call(
    calls: collections.Counter[str],
    options: dict[str, Any],  # pyright: ignore[reportExplicitAny]
    first: dict[str, Any],  # pyright: ignore[reportExplicitAny]
    then: dict[str, Any],  # pyright: ignore[reportExplicitAny]
) -> None:
    """Codex review of af670090, finding 5: the total is kept with the rows."""
    source = _source(tag_only_columns=True, **options)
    _ = source.head(**first)
    calls.clear()
    kept = source.head(**then)
    # A read of every matching row counted nothing (round 3): the first smaller
    # preview counts once; no row is read.
    assert calls["scan"] == 0
    assert calls["count"] == (1 if "where" in options else 0)
    _same_view(kept, _fresh(tag_only_columns=True, **options).head(**then))
    calls.clear()
    _ = source.head(**then)
    assert calls == {}


@pytest.fixture
def after_the_read(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[Callable[[], None]], None]:
    """Run an action once, when the next scan of rows has handed out its last batch:
    a preview paused after reading, before it keeps anything."""
    action: list[Callable[[], None]] = []
    original = fake_spiral_sdk.FakeReader.read_next_batch

    def read_next_batch(self: fake_spiral_sdk.FakeReader) -> pa.RecordBatch:
        try:
            return original(self)
        except StopIteration:
            record = self._record  # pyright: ignore[reportPrivateUsage]  # the fake's
            if (
                action
                and record.columns is None
                and SCHEMA_TABLE not in (record.identifier)
            ):
                action.pop()()
            raise

    monkeypatch.setattr(fake_spiral_sdk.FakeReader, "read_next_batch", read_next_batch)
    return action.append


def test_a_source_closed_during_a_preview_keeps_and_serves_nothing(
    calls: collections.Counter[str],
    after_the_read: Callable[[Callable[[], None]], None],
) -> None:
    """Codex review of af670090, finding 3."""
    source = _source(tag_only_columns=True)
    after_the_read(source.close)
    _ = source.head(40)
    with pytest.raises(RuntimeError, match="closed"):
        _ = source.head(40)
    with pytest.raises(RuntimeError, match="closed"):
        _ = source.head(5)


def test_a_check_during_a_preview_does_not_mix_versions(
    calls: collections.Counter[str],
    after_the_read: Callable[[Callable[[], None]], None],
) -> None:
    """Codex review of af670090, finding 4: rows, total and key of one version."""
    source = _source(tag_only_columns=True)

    def new_first_key_and_check() -> None:
        _table().write(_rows([-2]))
        assert source.check_for_new_data()

    after_the_read(new_first_key_and_check)
    during = source.head(10, columns={"sort_by_tags": True})
    # The rows and the total are those of the version the preview read.
    assert -2 not in during.table["k"].to_pylist()
    assert during.total_rows == 30
    # Nothing of the old version answers for the new one.
    calls.clear()
    after = source.head(10, columns={"sort_by_tags": True})
    assert calls["scan"] == 1
    assert -2 in after.table["k"].to_pylist()
    assert after.total_rows == 31


def test_the_memory_bound_counts_what_the_kept_rows_hold(
    calls: collections.Counter[str],
) -> None:
    """Codex review of af670090, finding 2: a small row sliced out of a batch with a
    65 MiB row beside it keeps only its own bytes."""
    huge = "z" * (65 * 2**20)
    project = fake_spiral_sdk.ExternalProject(_PROJECT)
    wide = project.create_table("default.wide", key_schema=_KEYS)
    rows = _rows([0, 1])
    rows = rows.set_column(3, "y", pa.array(["small", huge], pa.large_string()))
    wide.write(rows)
    wide.record(_SCHEMA)
    source = SpiralDBTableSource(_PROJECT, "wide", tag_only_columns=True)
    first = source.head(1)
    assert first.table["y"].to_pylist() == ["small"]
    kept = source._kept_preview  # pyright: ignore[reportPrivateUsage]  # the bound's subject
    assert kept is not None and kept.rows is not None
    assert kept.rows.get_total_buffer_size() < 2**20
    calls.clear()
    _same_view(source.head(1), first)
    assert calls == {}
    # A preview that holds the huge row itself is shown, not kept.
    both = source.head(2)
    assert both.table.num_rows == 2
    calls.clear()
    _ = source.head(2)
    assert calls["scan"] == 1


# --------------------------------------------------------------------------------------
# Round 2 of the review (854bbf94)


@pytest.fixture
def failing_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every count of a table's keys fails, as a scan that times out does."""
    original = fake_spiral_sdk.FakeKeyedTable.scan

    def scan(self: fake_spiral_sdk.FakeKeyedTable, *args: Any, **kwargs: Any) -> Any:  # pyright: ignore[reportExplicitAny]
        columns = args[0] if args else kwargs.get("columns")
        if columns and SCHEMA_TABLE not in self._table["name"]:  # pyright: ignore[reportPrivateUsage]  # the fake's
            raise fake_spiral_sdk.SpiralError("the count timed out")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(fake_spiral_sdk.FakeKeyedTable, "scan", scan)


def test_a_failing_count_never_fails_a_preview_that_read_every_row(
    calls: collections.Counter[str], failing_counts: None
) -> None:
    """Round 2, finding 1: the count is for later previews only."""
    source = _source(tag_only_columns=True, where={"subject": "s1"})
    seen = source.head(40)
    assert seen.table.num_rows == 15
    assert seen.total_rows == 15
    assert not seen.total_is_estimate
    # Its rows are kept without a count (round 3): the same preview makes no call.
    calls.clear()
    _same_view(source.head(40), seen)
    assert calls == {}
    # A smaller preview needs the table's count, as on main, and its failure is its
    # own; the kept rows stay.
    with pytest.raises(fake_spiral_sdk.SpiralError, match="timed out"):
        _ = source.head(10)
    calls.clear()
    _same_view(source.head(40), seen)
    assert calls == {}


def test_a_source_closed_before_the_count_still_answers_the_preview(
    calls: collections.Counter[str],
    after_the_read: Callable[[Callable[[], None]], None],
) -> None:
    """Round 2, finding 1: closed after the row scan, before the count."""
    source = _source(tag_only_columns=True, where={"subject": "s1"})
    after_the_read(source.close)
    seen = source.head(40)
    assert seen.table.num_rows == 15
    with pytest.raises(RuntimeError, match="closed"):
        _ = source.head(40)


def test_rows_over_the_keep_bound_are_not_counted(
    calls: collections.Counter[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 2, finding 1: never count for a record that will not be kept."""
    monkeypatch.setattr(spiral_module, "PREVIEW_KEPT_ROWS", 10)
    source = _source(tag_only_columns=True, where={"subject": "s1"})
    calls.clear()
    seen = source.head(40)
    assert seen.table.num_rows == 15
    assert "count" not in calls


def test_a_late_preview_of_an_old_version_does_not_replace_a_newer_one(
    calls: collections.Counter[str],
    after_the_read: Callable[[Callable[[], None]], None],
) -> None:
    """Round 2, finding 2."""
    source = _source(tag_only_columns=True)
    newer: list[StreamView] = []

    def move_and_preview() -> None:
        _table().write(_rows([-2]))
        assert source.check_for_new_data()
        newer.append(source.head(10))

    after_the_read(move_and_preview)
    older = source.head(10)
    assert -2 not in older.table["k"].to_pylist()
    assert -2 in newer[0].table["k"].to_pylist()
    calls.clear()
    _same_view(source.head(10), newer[0])
    assert calls == {}


def test_rows_over_the_row_bound_are_never_copied(
    calls: collections.Counter[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 2, finding 3: the row bound is checked before any copy."""
    monkeypatch.setattr(spiral_module, "PREVIEW_KEPT_ROWS", 5)
    copied: list[int] = []
    original = spiral_module._compact  # pyright: ignore[reportPrivateUsage]  # the subject

    def compact(rows: pa.Table) -> pa.Table:
        copied.append(rows.num_rows)
        return original(rows)

    monkeypatch.setattr(spiral_module, "_compact", compact)
    _ = _source(tag_only_columns=True).head(6)
    assert copied == []


def test_rows_that_are_not_a_slice_are_not_copied() -> None:
    """Round 2, finding 3: a whole large-string column (whose last offset ``nbytes``
    leaves out) is kept as it is; a slice of a larger batch is copied out."""
    whole = pa.table({"s": pa.array(["a" * 100] * 1000, pa.large_string())})
    assert whole.get_total_buffer_size() > whole.nbytes
    assert spiral_module._compact(whole) is whole  # pyright: ignore[reportPrivateUsage]  # the subject
    sliced = pa.table(
        {"s": pa.array(["a", "z" * (2 * 2**20)], pa.large_string())}
    ).slice(0, 1)
    compacted = spiral_module._compact(sliced)  # pyright: ignore[reportPrivateUsage]  # the subject
    assert compacted is not sliced
    assert compacted.equals(sliced)
    assert compacted.get_total_buffer_size() < 2**10


# --------------------------------------------------------------------------------------
# Round 3 of the review (0a9ccfeb)


def test_a_preview_that_read_every_matching_row_counts_nothing(
    calls: collections.Counter[str],
) -> None:
    """Round 3, finding 1: its total is exact, so it never waits for a count. The
    first smaller preview answered from its rows counts once, as main does, and keeps
    the count."""
    options: dict[str, Any] = {"where": {"subject": "s1"}}  # pyright: ignore[reportExplicitAny]
    source = _source(tag_only_columns=True, **options)
    calls.clear()
    seen = source.head(40)
    assert seen.table.num_rows == 15
    assert "count" not in calls
    calls.clear()
    fewer = source.head(10)
    assert "scan" not in calls
    _same_view(fewer, _fresh(tag_only_columns=True, **options).head(10))
    calls.clear()
    _same_view(source.head(10), fewer)
    _ = source.head(3)
    assert calls == {}


def test_unsliced_chunks_are_never_copied() -> None:
    """Round 3, finding 2: many whole one-row chunks over many large-binary columns
    hold a few bytes each beyond their values; together they are not a slice."""
    columns = {
        f"c{i}": pa.chunked_array(
            [pa.array([b"v"], pa.large_binary()) for _ in range(8192)]
        )
        for i in range(17)
    }
    whole = pa.table(list(columns.values()), names=list(columns))
    assert whole.get_total_buffer_size() > whole.nbytes + 2**20
    assert spiral_module._compact(whole) is whole  # pyright: ignore[reportPrivateUsage]  # the subject


def test_only_a_sliced_column_is_copied() -> None:
    """Round 3, finding 2: a real slice in one column copies that column only."""
    big = pa.array([b"a" * (3 * 2**20)], pa.large_binary())
    sliced = pa.array([b"s", b"z" * (2 * 2**20)], pa.large_binary()).slice(0, 1)
    rows = pa.table({"whole": big, "sliced": sliced})
    compacted = spiral_module._compact(rows)  # pyright: ignore[reportPrivateUsage]  # the subject
    assert compacted.equals(rows)
    assert compacted.schema.equals(rows.schema, check_metadata=True)
    # The whole column keeps its buffers; the slice holds only its own bytes.
    kept_whole = compacted.column("whole").chunks[0].buffers()[2]
    given_whole = rows.column("whole").chunks[0].buffers()[2]
    assert kept_whole is not None
    assert given_whole is not None
    assert kept_whole.address == given_whole.address
    assert compacted.column("sliced").chunk(0).get_total_buffer_size() < 2**10


def test_a_late_store_of_the_same_key_keeps_what_is_known(
    calls: collections.Counter[str],
    after_the_read: Callable[[Callable[[], None]], None],
) -> None:
    """Verification of b18ce0e7: a cold filtered head(40) paused before it keeps its
    rows; head(50) keeps the same 15 rows; head(10) counts and keeps the total; the
    late head(40) keeps the same key and must not erase the count."""
    options: dict[str, Any] = {"where": {"subject": "s1"}}  # pyright: ignore[reportExplicitAny]
    source = _source(tag_only_columns=True, **options)

    def others_first() -> None:
        assert source.head(50).table.num_rows == 15
        _ = source.head(10)

    after_the_read(others_first)
    late = source.head(40)
    assert late.table.num_rows == 15
    calls.clear()
    fewer = source.head(5)
    assert calls == {}
    _same_view(fewer, _fresh(tag_only_columns=True, **options).head(5))
    calls.clear()
    _ = source.head(50)
    assert calls == {}


# --------------------------------------------------------------------------------------
# CodeRabbit review of 62db6a1a: a restriction of several conditions


# Each restriction holds two conditions; its forms give them in both orders, as a
# mapping, as keywords and as two chained ``where()`` calls.
_TWO_CONDITIONS: dict[str, dict[str, Any]] = {  # pyright: ignore[reportExplicitAny]
    "tag_and_data": {"subject": "s1", "x": 30},
    "two_data": {"x": 50, "y": "v5"},
    "two_tags": {"subject": "s0", "k": 4},
}


def _restricted(
    source: SpiralDBTableSource,
    conditions: dict[str, Any],  # pyright: ignore[reportExplicitAny]
    form: str,
) -> SpiralDBTableSource:
    items = list(conditions.items())
    if form == "mapping":
        return source.where(dict(items))
    if form == "mapping_reversed":
        return source.where(dict(reversed(items)))
    if form == "keywords_reversed":
        return source.where(**dict(reversed(items)))
    first, second = items if form == "chained" else list(reversed(items))
    return source.where(dict([first])).where(dict([second]))


_FORMS = [
    "mapping",
    "mapping_reversed",
    "keywords_reversed",
    "chained",
    "chained_reversed",
]


@pytest.mark.parametrize("form", _FORMS)
@pytest.mark.parametrize("conditions", sorted(_TWO_CONDITIONS))
@pytest.mark.parametrize("tag_only", [True, ()], ids=["plain_tags", "data_tags"])
def test_a_restriction_of_two_conditions_keeps_its_preview_in_any_order(
    calls: collections.Counter[str],
    tag_only: bool | tuple[str, ...],
    conditions: str,
    form: str,
) -> None:
    """Two conditions, given in any order and form, are one restriction: its kept
    preview is the view a fresh source of the same form gives (the header names the
    conditions in the order given, as on ``main``), its rows are those of the first
    order, and a second preview makes no call."""
    given = _TWO_CONDITIONS[conditions]

    def fresh() -> SpiralDBTableSource:
        return _restricted(_source(tag_only_columns=tag_only), given, form)

    source = fresh()
    reference = _source(tag_only_columns=tag_only).where(**given)
    assert source.condition == reference.condition
    calls.clear()
    seen = source.head(10)
    assert calls["scan"] == 1
    assert seen.table.num_rows == 1
    assert seen.table.equals(reference.head(10).table, check_metadata=True)
    calls.clear()
    for n in (10, 1, 0):
        kept = source.head(n, columns={"source": True, "system_tags": True})
        assert "scan" not in calls, n
        _same_view(kept, fresh().head(n, columns={"source": True, "system_tags": True}))
        calls.clear()
        _ = source.head(n)
        assert calls == {}, n


@pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
@pytest.mark.parametrize("reversed_order", [False, True], ids=["given", "reversed"])
def test_a_job_over_a_source_of_two_conditions_keeps_and_renews_its_preview(
    calls: collections.Counter[str],
    orchestrator: type[AsyncPipelineOrchestrator] | None,
    reversed_order: bool,
) -> None:
    """A run that finds nothing new leaves the kept preview of a two-condition
    restriction in use; a run that takes in a new version makes the next preview read
    it, on the sync and the async path."""
    conditions = {"subject": "s1", "y": "v7"}
    if reversed_order:
        conditions = dict(reversed(list(conditions.items())))
    source = _source(tag_only_columns=True, where=conditions)
    job = PipelineJob(name="kept-preview-two", database=InMemoryArrowDatabase())
    with job:
        SelectColumns(["subject", "k", "x"])(source, label="read")
    runner = orchestrator() if orchestrator is not None else None
    assert source.head().table["k"].to_pylist() == [7]
    _ = job.run(orchestrator=runner)
    calls.clear()
    _ = source.head()
    assert calls == {}
    # A new row that matches both conditions (k = 107 is odd: subject "s1").
    _table().write(
        pa.table(
            {"subject": ["s1"], "k": [107], "x": [1070], "y": ["v7"]}, schema=_SCHEMA
        )
    )
    _ = job.run(orchestrator=runner)
    calls.clear()
    after = source.head()
    assert calls["scan"] == 1
    assert set(after.table["k"].to_pylist()) == {7, 107}
    calls.clear()
    _ = source.head()
    assert calls == {}
