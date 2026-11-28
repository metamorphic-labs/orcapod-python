"""A cached source refuses a changed row under a record id it holds (ITL-1396).

A ``CachedSource`` keeps every row its wrapped source ever delivered. A row whose record
id (its key columns, or its position) is already in the cache with other content is
refused before anything is written; the same row delivered again writes nothing; a row
under a new record id is added. Every cached row records when the cache wrote it, in a
column no read presents and no hash covers.
"""

from __future__ import annotations

import asyncio
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.sources.cached_source import SOURCE_RECORDED_AT_COL
from orcapod.core.sources.source_proxy import SourceProxy
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.databases.connector_arrow_database import ConnectorArrowDatabase
from orcapod.databases.sqlite_connector import SQLiteConnector
from orcapod.errors import (
    CachedSourceChangedRowError,
    InputValidationError,
    SourceCheckFailedWarning,
    UnstorableColumnError,
)
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource, CachedSource, CSVSource, DerivedSource
from orcapod.system_constants import SOURCE_DB_SCHEMA_VERSION, constants

ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}

Row = tuple[str, int, float, int]
"""One reading: subject, session, value, count."""

ROWS: list[Row] = [("s1", 1, 1.0, 10), ("s1", 2, 2.0, 20), ("s2", 1, 3.0, 30)]

# Two arrangements of one dataset: the order of the file's columns, of the declared
# tags and of the key columns.
ARRANGEMENTS: dict[str, dict[str, list[str]]] = {
    "as_declared": {
        "file": ["subject", "session", "value", "count"],
        "tags": ["subject", "session"],
        "keys": ["subject", "session"],
    },
    "rearranged": {
        "file": ["count", "session", "value", "subject"],
        "tags": ["session", "subject"],
        "keys": ["subject", "session"],
    },
}

CALLS: list[tuple[float, int]] = []


@function_pod(output_keys=("total", "double"))
def summarize(value: float, count: int) -> tuple[float, int]:
    """Return ``value + count`` and twice ``count``."""
    CALLS.append((value, count))
    return value + count, 2 * count


@pytest.fixture(autouse=True)
def _reset() -> None:
    CALLS.clear()


def _write(path: Path, rows: list[Row], arrangement: str = "as_declared") -> None:
    names = ARRANGEMENTS[arrangement]["file"]
    lines = [",".join(names)]
    for subject, session, value, count in rows:
        cells = {"subject": subject, "session": session, "value": value, "count": count}
        lines.append(",".join(str(cells[name]) for name in names))
    path.write_text("\n".join(lines) + "\n")


def _csv(path: Path, arrangement: str = "as_declared", *, keyed: bool = True) -> Any:
    kwargs: dict[str, Any] = {}
    if keyed:
        kwargs["record_id_columns"] = ARRANGEMENTS[arrangement]["keys"]
    return CSVSource(
        str(path),
        tag_columns=ARRANGEMENTS[arrangement]["tags"],
        tag_only_columns=True,
        source_id="readings",
        **kwargs,
    )


def _cached(
    path: Path, cache: Any, arrangement: str = "as_declared", *, keyed: bool = True
) -> CachedSource:
    return CachedSource(_csv(path, arrangement, keyed=keyed), cache_database=cache)


def _job(source: Any, database: Any) -> PipelineJob:
    job = PipelineJob(name="readings_job", database=database)
    with job:
        summarize.pod(source, label="summary")
    return job


def _run(job: PipelineJob, orchestrator: str) -> Any:
    return job.run(orchestrator=ORCHESTRATORS[orchestrator]())


def _presented(source: Any) -> list[Row]:
    return sorted(
        (r["subject"], r["session"], r["value"], r["count"])
        for r in source.as_table().to_pylist()
    )


def _stored_cache(source: CachedSource, cache: Any) -> pa.Table:
    """The cache table as the database holds it, every stored column."""
    table = cache.get_all_records(
        source.cache_path, record_id_column=CachedSource.HASH_COLUMN_NAME
    )
    assert table is not None
    return table


def _cache_rows(source: CachedSource, cache: Any) -> list[tuple[Any, ...]]:
    """Every stored cell of the cache, rows sorted: equal when nothing was written."""
    table = _stored_cache(source, cache)
    names = sorted(table.column_names)
    return sorted(tuple(row[name] for name in names) for row in table.to_pylist())


def _stored_tables(database: InMemoryArrowDatabase) -> dict[tuple[str, ...], int]:
    """Every table of a job database with its number of rows."""
    counts: dict[tuple[str, ...], int] = {}
    for path in database.list_table_paths(()):
        table = database.get_all_records(tuple(path))
        counts[tuple(path)] = 0 if table is None else table.num_rows
    return counts


def _changed(rows: list[Row], *keys: tuple[str, int]) -> list[Row]:
    """``rows`` with the value of each named key moved by a half."""
    return [
        (s, n, v + 0.5, c) if (s, n) in keys else (s, n, v, c) for s, n, v, c in rows
    ]


# ---------------------------------------------------------------------------
# A changed row under an existing key is refused
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
def test_a_changed_value_under_an_existing_key_is_refused_in_a_new_job(
    tmp_path: Path, orchestrator: str, arrangement: str
) -> None:
    path = tmp_path / "readings.csv"
    cache, database = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    _write(path, ROWS, arrangement)
    first = _cached(path, cache, arrangement)
    _ = _run(_job(first, database), orchestrator)
    assert len(CALLS) == 3
    cache_before = _cache_rows(first, cache)
    stored_before = _stored_tables(database)
    CALLS.clear()

    _write(path, _changed(ROWS, ("s1", 2)), arrangement)
    second = _cached(path, cache, arrangement)
    with pytest.raises(CachedSourceChangedRowError) as refused:
        _ = _run(_job(second, database), orchestrator)

    message = str(refused.value)
    assert "csv_v1:readings" in message
    assert '{"subject":"s1","session":2}' in message
    assert "1 row" in message
    assert "source id" in message and "cache" in message  # the remedy
    assert isinstance(refused.value, InputValidationError)
    assert CALLS == []
    assert _cache_rows(second, cache) == cache_before
    assert _stored_tables(database) == stored_before


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_changed_value_is_refused_in_the_job_that_cached_it(
    tmp_path: Path, orchestrator: str
) -> None:
    """The file changes under a live job: its next run is refused, nothing written."""
    path = tmp_path / "readings.csv"
    cache, database = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    _write(path, ROWS)
    source = _cached(path, cache)
    job = _job(source, database)
    _ = _run(job, orchestrator)
    cache_before = _cache_rows(source, cache)
    stored_before = _stored_tables(database)
    CALLS.clear()

    _write(path, _changed(ROWS, ("s2", 1)))
    with pytest.raises(CachedSourceChangedRowError, match="csv_v1:readings"):
        _ = _run(job, orchestrator)
    assert CALLS == []
    assert _cache_rows(source, cache) == cache_before
    assert _stored_tables(database) == stored_before
    # It stays refused: a later read does not present the cache as if nothing changed.
    with pytest.raises(CachedSourceChangedRowError):
        _ = source.as_table()


@pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
def test_several_changed_keys_are_named_up_to_five_with_their_count(
    tmp_path: Path, arrangement: str
) -> None:
    path = tmp_path / "readings.csv"
    cache = InMemoryArrowDatabase()
    rows: list[Row] = [(f"s{i}", i, float(i), i) for i in range(8)]
    _write(path, rows, arrangement)
    first = _cached(path, cache, arrangement)
    assert len(_presented(first)) == 8
    before = _cache_rows(first, cache)

    changed_keys = [(f"s{i}", i) for i in range(7)]
    _write(path, _changed(rows, *changed_keys), arrangement)
    second = _cached(path, cache, arrangement)
    with pytest.raises(CachedSourceChangedRowError) as refused:
        _ = second.as_table()
    message = str(refused.value)
    assert "7 rows" in message
    named = [
        key
        for key in changed_keys
        if f'{{"subject":"{key[0]}","session":{key[1]}}}' in message
    ]
    assert named == changed_keys[:5]  # the first five, in record-id order
    assert _cache_rows(second, cache) == before


def test_a_changed_tag_only_or_data_only_cell_is_a_changed_row(tmp_path: Path) -> None:
    """Every stored column counts: a second data column, not only the first."""
    path = tmp_path / "readings.csv"
    cache = InMemoryArrowDatabase()
    _write(path, ROWS)
    assert len(_presented(_cached(path, cache))) == 3
    _write(path, [("s1", 1, 1.0, 10), ("s1", 2, 2.0, 21), ("s2", 1, 3.0, 30)])
    with pytest.raises(CachedSourceChangedRowError, match='"session":2'):
        _ = _cached(path, cache).as_table()


# ---------------------------------------------------------------------------
# What stays accepted
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
def test_the_same_rows_delivered_again_write_nothing(
    tmp_path: Path, orchestrator: str, arrangement: str
) -> None:
    path = tmp_path / "readings.csv"
    cache, database = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    _write(path, ROWS, arrangement)
    first = _cached(path, cache, arrangement)
    _ = _run(_job(first, database), orchestrator)
    before = _cache_rows(first, cache)  # every cell, the write times included
    CALLS.clear()

    time.sleep(0.002)  # a later write would record a later time
    _write(path, list(reversed(ROWS)), arrangement)  # keyed: order is not content
    second = _cached(path, cache, arrangement)
    _ = _run(_job(second, database), orchestrator)
    assert CALLS == []
    assert _cache_rows(second, cache) == before
    assert _presented(second) == sorted(ROWS)


def test_plain_derived_source_redelivery_writes_nothing(tmp_path: Path) -> None:
    path = tmp_path / "readings.csv"
    _write(path, ROWS)
    job = _job(_csv(path), InMemoryArrowDatabase())
    _ = job.run()
    cache = InMemoryArrowDatabase()

    first = CachedSource(DerivedSource(job.nodes["summary"]), cache)
    presented = first.as_table(all_info=True)
    assert presented.num_rows == len(ROWS)
    assert all(
        not name.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
        for name in presented.column_names
    )
    before = _cache_rows(first, cache)

    second = CachedSource(DerivedSource(job.nodes["summary"]), cache)
    assert second.as_table(all_info=True).equals(presented)
    assert _cache_rows(second, cache) == before


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
def test_a_row_under_a_new_key_is_added(
    tmp_path: Path, orchestrator: str, arrangement: str
) -> None:
    path = tmp_path / "readings.csv"
    cache, database = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    _write(path, ROWS, arrangement)
    _ = _run(_job(_cached(path, cache, arrangement), database), orchestrator)
    CALLS.clear()

    new: Row = ("s2", 2, 4.0, 40)
    _write(path, [new, *ROWS], arrangement)
    second = _cached(path, cache, arrangement)
    _ = _run(_job(second, database), orchestrator)
    assert CALLS == [(4.0, 40)]
    assert _presented(second) == sorted([*ROWS, new])
    assert _stored_cache(second, cache).num_rows == 4


def test_a_row_removed_from_the_source_stays_and_may_come_back(tmp_path: Path) -> None:
    path = tmp_path / "readings.csv"
    cache = InMemoryArrowDatabase()
    _write(path, ROWS)
    assert _presented(_cached(path, cache)) == sorted(ROWS)
    _write(path, ROWS[:2])
    assert _presented(_cached(path, cache)) == sorted(ROWS)  # cumulative
    _write(path, ROWS)
    assert _presented(_cached(path, cache)) == sorted(ROWS)
    # A removed key that comes back with other content is a changed row.
    _write(path, ROWS[:2])
    assert _presented(_cached(path, cache)) == sorted(ROWS)
    _write(path, _changed(ROWS, ("s2", 1)))
    with pytest.raises(CachedSourceChangedRowError, match='"subject":"s2"'):
        _ = _cached(path, cache).as_table()


# ---------------------------------------------------------------------------
# Another store, another session, a reloaded job
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_cache_kept_on_disk_refuses_in_a_later_session(
    tmp_path: Path, orchestrator: str
) -> None:
    """New database, source and job objects over one stored cache, as a new process."""
    path = tmp_path / "readings.csv"
    _write(path, ROWS)

    def session() -> tuple[CachedSource, DeltaTableDatabase, PipelineJob]:
        cache = DeltaTableDatabase(str(tmp_path / "cache"))
        source = _cached(path, cache)
        job = _job(source, DeltaTableDatabase(str(tmp_path / "store")))
        return source, cache, job

    source, cache, job = session()
    _ = _run(job, orchestrator)
    before = _cache_rows(source, cache)
    CALLS.clear()

    source, cache, job = session()
    _ = _run(job, orchestrator)  # the same rows: accepted
    assert CALLS == []
    assert _cache_rows(source, cache) == before

    _write(path, _changed(ROWS, ("s1", 1), ("s1", 2)))
    source, cache, job = session()
    with pytest.raises(CachedSourceChangedRowError, match="2 rows"):
        _ = _run(job, orchestrator)
    assert CALLS == []
    source, cache, _ = session()
    assert _cache_rows(source, cache) == before


def test_a_reloaded_job_refuses_a_changed_row(tmp_path: Path) -> None:
    path = tmp_path / "readings.csv"
    _write(path, ROWS)
    cache = DeltaTableDatabase(str(tmp_path / "cache"))
    database = DeltaTableDatabase(str(tmp_path / "store"))
    source = _cached(path, cache)
    job = _job(source, database)
    _ = job.run()
    before = _cache_rows(source, cache)
    job.save(str(tmp_path / "job.json"))
    CALLS.clear()

    _write(path, _changed(ROWS, ("s1", 2)))
    loaded = PipelineJob.load(str(tmp_path / "job.json"))
    with pytest.raises(CachedSourceChangedRowError, match='"session":2'):
        _ = loaded.run(load_only=True)
    assert CALLS == []
    assert _cache_rows(_cached(path, cache), cache) == before


@pytest.mark.parametrize("warm_table", [True, False], ids=["present", "absent"])
def test_a_delta_writer_checks_a_delivery_against_the_latest_snapshot(
    tmp_path: Path, warm_table: bool
) -> None:
    path = tmp_path / "readings.csv"
    cache_dir = str(tmp_path / "cache")
    first_db = DeltaTableDatabase(cache_dir)
    _write(path, ROWS[:1])
    first = _cached(path, first_db)
    if warm_table:
        assert _presented(first) == ROWS[:1]
    else:
        assert first.get_all_records() is None

    _write(path, ROWS[:2])
    second_db = DeltaTableDatabase(cache_dir)
    second = _cached(path, second_db)
    assert _presented(second) == ROWS[:2]
    before = _cache_rows(second, second_db)

    _write(path, _changed(ROWS[:2], ("s1", 2)))
    delivery = _cached(path, first_db)
    with pytest.raises(CachedSourceChangedRowError, match='"session":2'):
        _ = delivery.as_table()
    fresh_db = DeltaTableDatabase(cache_dir)
    assert _cache_rows(delivery, fresh_db) == before


# ---------------------------------------------------------------------------
# Record ids by position: one rule for every source
# ---------------------------------------------------------------------------


def test_a_positional_source_refuses_a_changed_row_at_a_position(
    tmp_path: Path,
) -> None:
    path = tmp_path / "readings.csv"
    cache = InMemoryArrowDatabase()
    _write(path, ROWS)
    first = _cached(path, cache, keyed=False)
    assert _presented(first) == sorted(ROWS)
    before = _cache_rows(first, cache)

    _write(path, _changed(ROWS, ("s1", 2)))
    second = _cached(path, cache, keyed=False)
    with pytest.raises(CachedSourceChangedRowError) as refused:
        _ = second.as_table()
    assert "row_1" in str(refused.value)
    assert _cache_rows(second, cache) == before


def test_a_positional_source_accepts_an_append_and_refuses_a_reorder(
    tmp_path: Path,
) -> None:
    path = tmp_path / "readings.csv"
    cache = InMemoryArrowDatabase()
    _write(path, ROWS)
    assert _presented(_cached(path, cache, keyed=False)) == sorted(ROWS)

    new: Row = ("s2", 2, 4.0, 40)
    _write(path, [*ROWS, new])
    appended = _cached(path, cache, keyed=False)
    assert _presented(appended) == sorted([*ROWS, new])
    before = _cache_rows(appended, cache)

    # A reorder, and an insert in the middle, give positions other rows.
    for rows in (
        [ROWS[1], ROWS[0], ROWS[2], new],
        [ROWS[0], ("s9", 9, 9.0, 9), *ROWS[1:]],
    ):
        _write(path, rows)
        moved = _cached(path, cache, keyed=False)
        with pytest.raises(CachedSourceChangedRowError, match="row_"):
            _ = moved.as_table()
        assert _cache_rows(moved, cache) == before


def test_an_in_memory_source_is_held_to_the_same_rule() -> None:
    """Not a CSV matter: any wrapped source kind, here an Arrow table with keys."""

    def source(value: float) -> ArrowTableSource:
        return ArrowTableSource(
            pa.table(
                {
                    "subject": pa.array(["s1", "s1"], pa.large_string()),
                    "session": pa.array([1, 2], pa.int64()),
                    "value": pa.array([1.0, value], pa.float64()),
                    "count": pa.array([10, 20], pa.int64()),
                }
            ),
            tag_columns=["subject", "session"],
            record_id_columns=["session", "subject"],
            tag_only_columns=True,
            source_id="readings",
        )

    cache = InMemoryArrowDatabase()
    assert CachedSource(source(2.0), cache_database=cache).as_table().num_rows == 2
    assert CachedSource(source(2.0), cache_database=cache).as_table().num_rows == 2
    with pytest.raises(
        CachedSourceChangedRowError, match="arrow_table_v1:readings"
    ) as refused:
        _ = CachedSource(source(2.5), cache_database=cache).as_table()
    assert '{"session":2,"subject":"s1"}' in str(refused.value)


class _ReorderedReadDatabase(InMemoryArrowDatabase):
    """Return the same stored columns in a different order."""

    def get_all_records(
        self, record_path: tuple[str, ...], record_id_column: str | None = None
    ) -> pa.Table | None:
        records = super().get_all_records(record_path, record_id_column)
        if records is None:
            return None
        return records.select(sorted(records.column_names))


class _ReorderedDeliverySource(ArrowTableSource):
    """Return the same delivered columns in a different order."""

    def as_table(self, **kwargs: Any) -> pa.Table:
        table = super().as_table(**kwargs)
        return table.select(sorted(table.column_names))


@pytest.mark.parametrize("reordered", ["stored", "delivered", "both"])
def test_a_changed_row_is_refused_with_reordered_fields(reordered: str) -> None:
    database = (
        InMemoryArrowDatabase()
        if reordered == "delivered"
        else _ReorderedReadDatabase()
    )

    def cached(value: float, *, first: bool = False) -> CachedSource:
        source = (
            ArrowTableSource
            if first or reordered == "stored"
            else _ReorderedDeliverySource
        )
        return source(
            pa.table({"id": [1], "value": [value]}),
            record_id_columns="id",
            source_id="readings",
        ).cached(database)

    first = cached(1.0, first=True)
    assert first.as_table().num_rows == 1
    before = _stored_cache(first, database)
    assert cached(1.0).as_table().num_rows == 1
    assert _stored_cache(first, database).equals(before)
    with pytest.raises(CachedSourceChangedRowError, match='"id":1'):
        _ = cached(2.0).as_table()
    assert _stored_cache(first, database).equals(before)


@pytest.mark.parametrize("entry", ["table", "rows", "proxy"])
def test_a_refused_read_leaves_every_nested_cache_unchanged(
    tmp_path: Path, entry: str
) -> None:
    path = tmp_path / "readings.csv"
    inner_db, middle_db, outer_db = (InMemoryArrowDatabase() for _ in range(3))

    def outer_source(source: CachedSource) -> CachedSource | SourceProxy:
        if entry != "proxy":
            return source
        tag_schema, data_schema = source.output_schema()
        proxy = SourceProxy(
            source.source_id,
            source.content_hash().to_string(),
            source.pipeline_hash().to_string(),
            tag_schema,
            data_schema,
        )
        proxy.bind(source)
        return proxy

    _write(path, ROWS[:1])
    first = outer_source(_cached(path, InMemoryArrowDatabase())).cached(outer_db)
    assert _presented(first) == ROWS[:1]
    before = _stored_cache(first, outer_db)
    new: Row = ("s9", 1, 9.0, 90)
    _write(path, [new])
    seed_middle = _cached(path, middle_db)
    assert _presented(seed_middle) == [new]
    middle_before = _stored_cache(seed_middle, middle_db)

    changed = _changed(ROWS, ("s1", 1))
    _write(path, changed)
    inner = _cached(path, inner_db)
    middle = inner.cached(middle_db)
    wrapped = outer_source(middle)
    outer = wrapped.cached(outer_db)
    # Only the outer cache holds a conflicting row. The middle cache may accept
    # this delivery, but must wait for the outer check before adding any new keys.
    with pytest.raises(CachedSourceChangedRowError):
        if entry == "rows":
            _ = list(outer.iter_data())
        else:
            _ = outer.as_table()
    assert _stored_cache(outer, outer_db).equals(before)
    assert _stored_cache(middle, middle_db).equals(middle_before)
    assert list(inner_db.list_table_paths(())) == []

    accepted = wrapped.cached(InMemoryArrowDatabase())
    assert _presented(accepted) == sorted([*changed, new])
    assert _stored_cache(middle, middle_db).num_rows == 4
    assert _stored_cache(inner, inner_db).num_rows == 3


def test_checking_for_new_data_leaves_a_nested_delivery_for_the_read(
    tmp_path: Path,
) -> None:
    path = tmp_path / "readings.csv"
    inner_db, outer_db = InMemoryArrowDatabase(), InMemoryArrowDatabase()

    _write(path, ROWS[:1])
    seed = _cached(path, outer_db)
    assert _presented(seed) == ROWS[:1]
    outer_before = _stored_cache(seed, outer_db)

    _write(path, ROWS[1:])
    inner = _cached(path, inner_db)
    assert _presented(inner) == ROWS[1:]
    inner_before = _stored_cache(inner, inner_db)
    outer = inner.cached(outer_db)

    _write(path, _changed(ROWS, ("s1", 1)))
    assert outer.check_for_new_data()
    assert _stored_cache(inner, inner_db).equals(inner_before)
    with pytest.raises(CachedSourceChangedRowError):
        _ = outer.as_table()
    assert _stored_cache(inner, inner_db).equals(inner_before)
    assert _stored_cache(outer, outer_db).equals(outer_before)


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("depth", [2, 3])
def test_a_nested_conflict_refuses_every_job_rerun(
    tmp_path: Path, orchestrator: str, depth: int
) -> None:
    path = tmp_path / "readings.csv"
    _write(path, ROWS)
    caches: list[tuple[CachedSource, InMemoryArrowDatabase]] = []
    source = _csv(path)
    for _ in range(depth):
        cache = InMemoryArrowDatabase()
        source = source.cached(cache)
        caches.append((source, cache))

    database = InMemoryArrowDatabase()
    job = _job(source, database)

    def run() -> None:
        if orchestrator == "async":
            _ = asyncio.run(job.run_async())
        else:
            _ = job.run()

    run()
    before = [_stored_cache(source, cache) for source, cache in caches]
    stored_before = _stored_tables(database)
    CALLS.clear()
    _write(path, _changed(ROWS, ("s1", 2)))

    for _ in range(2):
        with warnings.catch_warnings():
            warnings.simplefilter("error", SourceCheckFailedWarning)
            with pytest.raises(CachedSourceChangedRowError, match='"session":2'):
                run()
        assert CALLS == []
        assert _stored_tables(database) == stored_before
        for (source, cache), snapshot in zip(caches, before, strict=True):
            assert _stored_cache(source, cache).equals(snapshot)

    _write(path, ROWS)
    run()
    assert CALLS == []


def test_a_refused_keyed_derivation_leaves_its_origin_cache_unchanged() -> None:
    inner_db, outer_db = InMemoryArrowDatabase(), InMemoryArrowDatabase()

    def nested(value: int, database: InMemoryArrowDatabase) -> DerivedSource:
        origin = ArrowTableSource(
            pa.table({"id": [1], "site": ["lab"], "value": [value]}),
            source_id="origin",
            record_id_columns="id",
            tag_columns=["id", "site"],
            tag_only_columns=True,
        ).cached(database)
        return DerivedSource(
            origin,
            source_id="derived",
            tags_as_data={"site": "copied_site"},
            record_id_columns="id",
        )

    seed = nested(10, InMemoryArrowDatabase()).cached(outer_db)
    assert seed.as_table()["value"].to_pylist() == [10]
    before = _stored_cache(seed, outer_db)
    incoming = nested(20, inner_db)
    outer = incoming.cached(outer_db)

    for _ in range(2):
        with pytest.raises(CachedSourceChangedRowError, match='"id":1'):
            _ = outer.as_table()
        assert _stored_cache(outer, outer_db).equals(before)
        assert list(inner_db.list_table_paths(())) == []

    accepted = incoming.cached(InMemoryArrowDatabase())
    assert accepted.as_table()["value"].to_pylist() == [20]
    assert len(list(inner_db.list_table_paths(()))) == 1


@function_pod(output_keys=("total", "extra"))
def _with_extra(value: int | None) -> tuple[int, int | None]:
    return value or 0, 42


@function_pod(output_keys="total")
def _without_extra(value: int | None) -> int:
    return value or 0


@pytest.mark.parametrize("omit_on_append", [True, False], ids=["omitted", "added"])
def test_a_nested_delta_cache_preserves_schema_evolution(
    tmp_path: Path, omit_on_append: bool
) -> None:
    def source(key: int, *, extra: bool) -> DerivedSource:
        origin = ArrowTableSource(
            pa.table({"id": [key], "value": [10]}),
            source_id="origin",
            record_id_columns="id",
            tag_columns="id",
            tag_only_columns=True,
        )
        job = PipelineJob(name="origin", database=InMemoryArrowDatabase())
        with job:
            (_with_extra if extra else _without_extra).pod(origin, label="values")
        _ = job.run()
        return DerivedSource(job.nodes["values"], source_id="plain")

    database = DeltaTableDatabase(tmp_path)
    seed = source(1, extra=omit_on_append).cached(database, cache_path=("fixed",))
    assert seed.as_table()["id"].to_pylist() == [1]
    inner = source(2, extra=not omit_on_append).cached(database, cache_path=("fixed",))
    outer = inner.cached(InMemoryArrowDatabase())

    result = outer.as_table().sort_by("id")
    assert result["id"].to_pylist() == [1, 2]
    assert result["extra"].to_pylist() == ([42, None] if omit_on_append else [None, 42])
    assert inner.as_table().sort_by("id").equals(result)


def _derived(table: pa.Table, pod: Any, tags: list[str]) -> DerivedSource:
    """A plain derived source over a job node computed from ``table``."""
    origin = ArrowTableSource(
        table,
        source_id="origin",
        record_id_columns="id",
        tag_columns=tags,
        tag_only_columns=True,
    )
    job = PipelineJob(name="origin", database=InMemoryArrowDatabase())
    with job:
        pod.pod(origin, label="values")
    _ = job.run()
    return DerivedSource(job.nodes["values"], source_id="plain")


@pytest.mark.parametrize("children", [("x", "y"), ("y", "x")])
def test_a_nested_delta_cache_accepts_an_added_nullable_struct_child(
    tmp_path: Path, children: tuple[str, str]
) -> None:
    """Independent review of 76912324: the stored struct has one child, the appended
    rows two. Delta evolves the struct; the check of the delivery, made before the
    write, must not hold the stored rows to the new declaration."""

    def source(key: int, names: tuple[str, ...]) -> DerivedSource:
        values = {"x": key * 10, "y": key * 20}
        payload = pa.StructArray.from_arrays(
            [pa.array([values[name]]) for name in names],
            fields=[pa.field(name, pa.int64()) for name in names],
        )
        table = pa.table(
            {"id": [key], "site": ["lab"], "payload": payload, "value": [key * 3]}
        )
        return _derived(table, _without_extra, ["id", "site", "payload"])

    database = DeltaTableDatabase(tmp_path)
    seed = source(1, ("x",)).cached(database, cache_path=("fixed",))
    assert seed.as_table()["id"].to_pylist() == [1]
    inner = source(2, children).cached(database, cache_path=("fixed",))
    outer = inner.cached(InMemoryArrowDatabase())

    result = outer.as_table().sort_by("id")
    assert result["payload"].to_pylist() == [{"x": 10, "y": None}, {"x": 20, "y": 40}]
    assert result["total"].to_pylist() == [3, 6]
    assert inner.as_table().sort_by("id").equals(result)


@pytest.mark.parametrize("omit_on_append", [True, False], ids=["omitted", "added"])
def test_a_nested_cache_over_a_wrapped_delta_store_preserves_schema_evolution(
    tmp_path: Path, omit_on_append: bool
) -> None:
    """Independent review of 76912324: a Delta store behind the extension-aware
    wrapper appends as Delta does; the store's kind is never read from its class."""
    from orcapod.databases.extension_aware_database import ExtensionAwareDatabase
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter

    def source(key: int, *, extra: bool) -> DerivedSource:
        table = pa.table({"id": [key], "site": ["lab"], "value": [10], "count": [3]})
        return _derived(
            table.drop(["count"]),
            _with_extra if extra else _without_extra,
            ["id", "site"],
        )

    database = ExtensionAwareDatabase(
        DeltaTableDatabase(tmp_path), UniversalTypeConverter()
    )
    seed = source(1, extra=omit_on_append).cached(database, cache_path=("fixed",))
    assert seed.as_table()["id"].to_pylist() == [1]
    inner = source(2, extra=not omit_on_append).cached(database, cache_path=("fixed",))
    outer = inner.cached(InMemoryArrowDatabase())

    result = outer.as_table().sort_by("id")
    assert result["id"].to_pylist() == [1, 2]
    assert result["extra"].to_pylist() == ([42, None] if omit_on_append else [None, 42])
    assert inner.as_table().sort_by("id").equals(result)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="A positional DerivedSource reads the actual Delta order after its origin writes.",
)
def test_a_refused_positional_derivation_leaves_its_delta_origin_unchanged(
    tmp_path: Path,
) -> None:
    inner_db = DeltaTableDatabase(tmp_path / "inner")
    outer_db = InMemoryArrowDatabase()

    def nested(ids: list[int]) -> tuple[CachedSource, CachedSource]:
        inner = ArrowTableSource(
            pa.table({"id": ids, "value": ids}),
            source_id="readings",
            record_id_columns="id",
            tag_columns="id",
            tag_only_columns=True,
        ).cached(inner_db)
        derived = DerivedSource(
            inner, source_id="derived", tags_as_data={"id": "copied_id"}
        )
        return inner, derived.cached(outer_db)

    initial_inner, initial_outer = nested([1])
    _ = initial_outer.as_table()
    before = _stored_cache(initial_inner, inner_db)

    # Delta reads the appended row first, so the derivation now gives row_0 to ID 2.
    updated_inner, updated_outer = nested([1, 2])
    with pytest.raises(CachedSourceChangedRowError, match="row_0"):
        _ = updated_outer.as_table()
    assert _stored_cache(updated_inner, inner_db).equals(before)


@pytest.mark.parametrize(
    ("name", "source_tag"),
    [
        ("record_id_extra", None),
        ("record_id", None),
        ("record_id::custom", None),
        pytest.param("record_id::custom", "source_id::custom", id="matched-pair"),
        pytest.param(
            "record_id::" + "a1" * 32, "source_id::" + "a1" * 32, id="hex-pair"
        ),
    ],
)
def test_a_custom_record_id_like_tag_cannot_mask_a_changed_row(
    name: str, source_tag: str | None
) -> None:
    cache = InMemoryArrowDatabase()

    def cached(value: float, tag: str) -> CachedSource:
        schema = pa.schema(
            [
                pa.field("subject", pa.large_string()),
                pa.field("session", pa.int64()),
                pa.field("value", pa.float64()),
                pa.field("count", pa.int64()),
                pa.field(name, pa.large_string(), nullable=False),
            ]
        )
        row = {"subject": "s1", "session": 1, "value": value, "count": 10, name: tag}
        system_tags = [name]
        if source_tag is not None:
            schema = schema.append(
                pa.field(source_tag, pa.large_string(), nullable=False)
            )
            row[source_tag] = "arrow_table_v1:readings"
            system_tags.append(source_tag)
        table = pa.Table.from_pylist([row], schema=schema)
        return ArrowTableSource(
            table,
            tag_columns=["subject", "session"],
            tag_only_columns=True,
            record_id_columns=["subject", "session"],
            system_tag_columns=system_tags,
            source_id="readings",
        ).cached(cache)

    first = cached(1.0, "before")
    assert _presented(first) == ROWS[:1]
    before = _cache_rows(first, cache)

    changed = cached(1.5, "after")
    with pytest.raises(CachedSourceChangedRowError, match='"session":1'):
        _ = changed.as_table()
    assert _cache_rows(changed, cache) == before


# ---------------------------------------------------------------------------
# The recorded-at column
# ---------------------------------------------------------------------------


def test_sqlite_refuses_the_source_cache_timestamp_at_construction(
    tmp_path: Path,
) -> None:
    path = tmp_path / "readings.csv"
    _write(path, ROWS)
    cache = ConnectorArrowDatabase(SQLiteConnector())
    with pytest.raises(UnstorableColumnError, match=SOURCE_RECORDED_AT_COL):
        _ = _cached(path, cache)
    assert list(cache.list_table_paths(())) == []


@pytest.mark.parametrize("ingest", [False, True], ids=["read", "ingest"])
def test_a_prerelease_source_cache_without_recorded_at_requires_rebuild(
    tmp_path: Path, ingest: bool
) -> None:
    path = tmp_path / "readings.csv"
    _write(path, ROWS)
    current_db = InMemoryArrowDatabase()
    current = _cached(path, current_db)
    assert _presented(current) == sorted(ROWS)
    prerelease = _stored_cache(current, current_db).drop([SOURCE_RECORDED_AT_COL])
    _write(path, [*ROWS, ("s2", 2, 4.0, 40)])

    old_db = InMemoryArrowDatabase()
    old = _cached(path, old_db)
    old_db.add_records(
        old.cache_path, prerelease, record_id_column=CachedSource.HASH_COLUMN_NAME
    )
    with pytest.raises(
        InputValidationError, match=r"sdb_v2.*__recorded_at.*[Rr]ebuild"
    ):
        if ingest:
            _ = old.as_table()
        else:
            _ = old.get_all_records()
    assert _stored_cache(old, old_db).equals(prerelease)


@pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
def test_every_cached_row_records_when_it_was_written(
    tmp_path: Path, arrangement: str
) -> None:
    path = tmp_path / "readings.csv"
    cache = InMemoryArrowDatabase()
    _write(path, ROWS, arrangement)
    started = datetime.now(timezone.utc)
    first = _cached(path, cache, arrangement)
    _ = first.as_table()
    written = datetime.now(timezone.utc)

    assert SOURCE_RECORDED_AT_COL == "__recorded_at"
    assert first.cache_path[-1] == SOURCE_DB_SCHEMA_VERSION == "sdb_v2"
    stored = _stored_cache(first, cache)
    field = stored.schema.field(SOURCE_RECORDED_AT_COL)
    assert field.type == pa.timestamp("us", tz="UTC")
    assert not field.nullable
    times = cast("list[datetime]", stored.column(SOURCE_RECORDED_AT_COL).to_pylist())
    assert len(times) == 3 and len(set(times)) == 1  # one time per write
    assert started <= times[0] <= written

    # A later write: the rows already cached keep their time, the new row has its own.
    time.sleep(0.002)
    new: Row = ("s2", 2, 4.0, 40)
    _write(path, [*ROWS, new], arrangement)
    second = _cached(path, cache, arrangement)
    _ = second.as_table()
    by_key = {
        (r["subject"], r["session"]): r[SOURCE_RECORDED_AT_COL]
        for r in _stored_cache(second, cache).to_pylist()
    }
    assert {by_key[(s, n)] for s, n, _, _ in ROWS} == {times[0]}
    assert by_key[("s2", 2)] > written


def test_the_recorded_at_column_is_in_nothing_a_read_presents(tmp_path: Path) -> None:
    path = tmp_path / "readings.csv"
    cache = InMemoryArrowDatabase()
    _write(path, ROWS)
    cached = _cached(path, cache)
    plain = _csv(path)

    assert cached.keys(all_info=True) == plain.keys(all_info=True)
    assert cached.output_schema(all_info=True) == plain.output_schema(all_info=True)
    table = cached.as_table(all_info=True)
    assert table.schema.names == plain.as_table(all_info=True).schema.names
    assert SOURCE_RECORDED_AT_COL not in table.schema.names
    records = cached.get_all_records()
    assert records is not None
    assert SOURCE_RECORDED_AT_COL not in records.schema.names
    assert not any(
        name.startswith(constants.META_PREFIX) for name in records.schema.names
    )
    for tag, data in cached.iter_data():
        assert SOURCE_RECORDED_AT_COL not in tag.as_dict(all_info=True)
        assert SOURCE_RECORDED_AT_COL not in data.as_dict(all_info=True)
    assert (
        SOURCE_RECORDED_AT_COL not in cached.head(3, all_info=True).table.schema.names
    )


def test_the_time_of_writing_is_in_no_hash(tmp_path: Path) -> None:
    """Two caches written at two times hold and present the same hashes."""
    path = tmp_path / "readings.csv"
    _write(path, ROWS)
    plain = _csv(path)

    def cached_now() -> tuple[CachedSource, pa.Table, pa.Table]:
        cache = InMemoryArrowDatabase()
        source = _cached(path, cache)
        presented = source.as_table(all_info=True).sort_by(
            [("subject", "ascending"), ("session", "ascending")]
        )
        stored = _stored_cache(source, cache).sort_by(
            [("subject", "ascending"), ("session", "ascending")]
        )
        return source, presented, stored

    early, early_rows, early_stored = cached_now()
    time.sleep(0.002)
    late, late_rows, late_stored = cached_now()
    assert (
        early_stored.column(SOURCE_RECORDED_AT_COL).to_pylist()
        != late_stored.column(SOURCE_RECORDED_AT_COL).to_pylist()
    )

    assert early.content_hash() == late.content_hash() == plain.content_hash()
    assert early.pipeline_hash() == late.pipeline_hash() == plain.pipeline_hash()
    assert early.source_id == late.source_id == plain.source_id
    assert early.cache_path == late.cache_path
    # Every presented cell: values, record ids, tokens, row content hashes.
    assert early_rows.equals(late_rows)
    assert early_rows.equals(
        plain.as_table(all_info=True).sort_by(
            [("subject", "ascending"), ("session", "ascending")]
        )
    )
    # The stored record hashes, and every stored column but the time.
    assert early_stored.drop([SOURCE_RECORDED_AT_COL]).equals(
        late_stored.drop([SOURCE_RECORDED_AT_COL])
    )
    for (early_tag, early_data), (late_tag, late_data) in zip(
        sorted(early.iter_data(), key=lambda pair: str(pair[0].as_dict())),
        sorted(late.iter_data(), key=lambda pair: str(pair[0].as_dict())),
        strict=True,
    ):
        assert early_tag.content_hash() == late_tag.content_hash()
        assert early_data.content_hash() == late_data.content_hash()


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_downstream_ids_do_not_follow_the_time_of_writing(
    tmp_path: Path, orchestrator: str
) -> None:
    """A job over a cache written earlier stores what a job over a fresh cache does."""
    path = tmp_path / "readings.csv"
    _write(path, ROWS)

    def node_rows() -> tuple[Any, list[dict[str, Any]], list[bytes]]:
        database = InMemoryArrowDatabase()
        job = _job(_cached(path, InMemoryArrowDatabase()), database)
        _ = _run(job, orchestrator)
        node = job.nodes["summary"]
        rows = node.as_table(columns={"system_tags": True}).to_pylist()
        # The id the node's pipeline table keeps each row under: the instance id
        # covers the input's tags, system tags, values and source-info tokens.
        (pipeline_path,) = [
            path for path in database.list_table_paths(()) if path[-1] == "pdb_v2"
        ]
        stored = database.get_all_records(tuple(pipeline_path))
        assert stored is not None
        ids = sorted(
            cast("list[bytes]", stored.column("__pipeline_instance_id").to_pylist())
        )
        return (
            node.content_hash(),
            sorted(rows, key=lambda r: (r["subject"], r["session"])),
            ids,
        )

    early = node_rows()
    assert len(early[2]) == 3
    time.sleep(0.002)
    assert node_rows() == early
