"""A cached source refuses a cache holding two contents for one record id (ITL-1396).

A delivery that changes a cached row is refused before it is written, so one cached
source never writes two contents for one record id. A cache table can still come to hold
them: two writers that both passed the check before either wrote, or a table written by
hand. Such a cache is refused whenever it is read (developer, November 28, 2026), on every
read path, so a cached source never presents two rows for one record id. The same row
stored twice is not a conflict.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.config import HashingConfig, OrcapodConfig
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import CachedSourceChangedRowError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource, CachedSource
from tests.test_core.sources.cache_versions import write_outside_the_rule

ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}

Row = tuple[str, int, float, int]
"""One reading: subject, session, value, count."""

ROWS: list[Row] = [("s1", 1, 1.0, 10), ("s1", 2, 2.0, 20), ("s2", 1, 3.0, 30)]

# Two arrangements of one dataset: the table's column order and the declared tag order.
ARRANGEMENTS: dict[str, dict[str, list[str]]] = {
    "as_declared": {
        "table": ["subject", "session", "value", "count"],
        "tags": ["subject", "session"],
    },
    "rearranged": {
        "table": ["count", "session", "value", "subject"],
        "tags": ["session", "subject"],
    },
}
KEYS = ["subject", "session"]

CALLS: list[tuple[float, int]] = []


@function_pod(output_keys=("total", "double"))
def summarize(value: float, count: int) -> tuple[float, int]:
    """Return ``value + count`` and twice ``count``."""
    CALLS.append((value, count))
    return value + count, 2 * count


@pytest.fixture(autouse=True)
def _reset() -> None:
    CALLS.clear()


def _source(
    rows: list[Row],
    arrangement: str = "as_declared",
    *,
    keyed: bool = True,
    extra: bool = False,
    config: OrcapodConfig | None = None,
) -> Any:
    cells = {
        "subject": pa.array([r[0] for r in rows], pa.large_string()),
        "session": pa.array([r[1] for r in rows], pa.int64()),
        "value": pa.array([r[2] for r in rows], pa.float64()),
        "count": pa.array([r[3] for r in rows], pa.int64()),
    }
    names = ARRANGEMENTS[arrangement]["table"]
    kwargs: dict[str, Any] = {"record_id_columns": KEYS} if keyed else {}
    table = pa.table({name: cells[name] for name in names})
    if extra:
        table = table.append_column("extra", pa.nulls(len(rows), pa.int32()))
    return ArrowTableSource(
        table.cast(
            pa.schema([f.with_nullable(f.name == "extra") for f in table.schema])
        ),
        tag_columns=ARRANGEMENTS[arrangement]["tags"],
        tag_only_columns=True,
        source_id="readings",
        config=config,
        **kwargs,
    )


def _database(store: str, tmp_path: Path, name: str = "cache") -> Any:
    if store == "delta":
        return DeltaTableDatabase(base_path=str(tmp_path / name))
    return InMemoryArrowDatabase()


def _changed(rows: list[Row], *keys: tuple[str, int]) -> list[Row]:
    """``rows`` with another value and count under each of ``keys``."""
    return [
        (s, n, v + 0.5, c + 1) if (s, n) in keys else (s, n, v, c)
        for s, n, v, c in rows
    ]


def _conflicting_cache(
    cache: Any,
    arrangement: str = "as_declared",
    *,
    keyed: bool = True,
    rows: list[Row] = ROWS,
    keys: tuple[tuple[str, int], ...] = (("s1", 2), ("s2", 1)),
) -> None:
    """Fill ``cache`` with ``rows``, then with other content under ``keys``."""
    first = CachedSource(_source(rows, arrangement, keyed=keyed), cache_database=cache)
    _ = first.as_table()
    write_outside_the_rule(
        _source(_changed(rows, *keys), arrangement, keyed=keyed), cache
    )


def _stored(source: CachedSource, cache: Any) -> list[tuple[Any, ...]]:
    """Every stored cell of the cache, rows sorted: equal when nothing was written."""
    table = cache.get_all_records(
        source.cache_path, record_id_column=CachedSource.HASH_COLUMN_NAME
    )
    assert table is not None
    names = sorted(table.column_names)
    return sorted(tuple(row[name] for name in names) for row in table.to_pylist())


READS: dict[str, Any] = {
    "as_table": lambda source: source.as_table(),
    "as_table_all_info": lambda source: source.as_table(all_info=True),
    "iter_data": lambda source: list(source.iter_data()),
    "run": lambda source: source.run(),
    "head": lambda source: source.head(),
    "head_all_info": lambda source: source.head(2, all_info=True),
    "get_all_records": lambda source: source.get_all_records(),
}


@pytest.mark.parametrize("read", list(READS))
@pytest.mark.parametrize("store", ["memory", "delta"])
@pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
def test_every_read_of_a_cache_with_two_contents_for_a_record_id_is_refused(
    tmp_path: Path, arrangement: str, store: str, read: str
) -> None:
    cache = _database(store, tmp_path)
    _conflicting_cache(cache, arrangement)
    source = CachedSource(_source(ROWS, arrangement), cache_database=cache)
    before = _stored(source, cache)

    with pytest.raises(CachedSourceChangedRowError) as refusal:
        READS[read](source)

    message = str(refusal.value)
    assert "the cache" in message and "holds" in message
    assert "cached_v" not in message and "'arrow_table_v1:readings'" in message
    assert "2 record ids" in message
    assert '{"subject":"s1","session":2}, {"subject":"s2","session":1}' in message
    assert _stored(source, cache) == before


@pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
@pytest.mark.parametrize("read", ["head", "get_all_records"])
@pytest.mark.parametrize("change", ["hash_length", "declared_schema"])
def test_a_conflicting_cache_uses_its_stored_record_id_column(
    arrangement: str, read: str, change: str
) -> None:
    cache = InMemoryArrowDatabase()
    path = ("fixed",) if change == "declared_schema" else None
    seed = _source(ROWS, arrangement).cached(cache, cache_path=path)
    _ = seed.as_table()
    write_outside_the_rule(_source(_changed(ROWS, ("s1", 2)), arrangement), cache, path)

    config = (
        OrcapodConfig(hashing=HashingConfig(schema_n_char=16))
        if change == "hash_length"
        else None
    )
    source = _source(
        ROWS, arrangement, extra=change == "declared_schema", config=config
    ).cached(cache, cache_path=path)
    assert source.cache_path == seed.cache_path
    before = _stored(source, cache)

    with pytest.raises(CachedSourceChangedRowError, match="1 record id"):
        READS[read](source)

    assert _stored(source, cache) == before


@pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
def test_a_changed_delivery_compares_record_ids_across_declared_schemas(
    tmp_path: Path, arrangement: str
) -> None:
    cache = DeltaTableDatabase(tmp_path)
    path = ("fixed",)
    seed = _source(ROWS[:1], arrangement).cached(cache, cache_path=path)
    _ = seed.as_table()
    before = _stored(seed, cache)
    source = _source(_changed(ROWS, ("s1", 1)), arrangement, extra=True).cached(
        cache, cache_path=path
    )

    with pytest.raises(
        CachedSourceChangedRowError, match=r'\{"subject":"s1","session":1\}'
    ):
        source.as_table()

    assert _stored(seed, cache) == before


@pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
def test_the_notebook_preview_shows_the_refusal_and_no_row(arrangement: str) -> None:
    """A display hook never raises: the preview shows the error ``head()`` raises."""
    cache = InMemoryArrowDatabase()
    _conflicting_cache(cache, arrangement)
    source = CachedSource(_source(ROWS, arrangement), cache_database=cache)

    html = source._repr_html_()

    assert "could not read" in html
    assert "2 record ids with more than one content" in html
    assert "<td" not in html


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("store", ["memory", "delta"])
@pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
def test_a_job_over_such_a_cache_is_refused_and_computes_nothing(
    tmp_path: Path, arrangement: str, store: str, orchestrator: str
) -> None:
    cache = _database(store, tmp_path)
    _conflicting_cache(cache, arrangement)
    source = CachedSource(_source(ROWS, arrangement), cache_database=cache)
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="readings_job", database=database)
    with job:
        summarize.pod(source, label="summary")

    for _ in range(2):
        with pytest.raises(CachedSourceChangedRowError, match="the cache"):
            job.run(orchestrator=ORCHESTRATORS[orchestrator]())

    assert CALLS == []
    assert list(database.list_table_paths(())) == []


@pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
def test_a_positional_cache_with_two_contents_for_a_position_is_refused(
    arrangement: str,
) -> None:
    cache = InMemoryArrowDatabase()
    _conflicting_cache(cache, arrangement, keyed=False, keys=(("s1", 2),))
    source = CachedSource(_source(ROWS, arrangement, keyed=False), cache_database=cache)

    with pytest.raises(CachedSourceChangedRowError, match="1 record id .*: row_1\\."):
        source.as_table()


def test_more_than_five_record_ids_are_named_up_to_five_with_the_count() -> None:
    rows: list[Row] = [("s", n, float(n), n) for n in range(8)]
    cache = InMemoryArrowDatabase()
    _conflicting_cache(cache, rows=rows, keys=tuple(("s", n) for n in range(7)))
    source = CachedSource(_source(rows), cache_database=cache)

    with pytest.raises(CachedSourceChangedRowError) as refusal:
        source.as_table()

    message = str(refusal.value)
    assert "7 record ids" in message and "and 2 more" in message
    named = [n for n in range(8) if f'{{"subject":"s","session":{n}}}' in message]
    assert named == [0, 1, 2, 3, 4]


class _RepeatingDatabase(InMemoryArrowDatabase):
    """A store that gives every cached row back twice: identical duplicates."""

    def get_all_records(self, *args: Any, **kwargs: Any) -> pa.Table | None:
        records = super().get_all_records(*args, **kwargs)
        return None if records is None else pa.concat_tables([records, records])


def test_the_same_row_stored_twice_is_not_a_conflict() -> None:
    cache = _RepeatingDatabase()
    source = CachedSource(_source(ROWS), cache_database=cache)

    rows = source.as_table().to_pylist()

    assert len(rows) == 2 * len(ROWS)
    assert {(r["subject"], r["session"], r["value"], r["count"]) for r in rows} == set(
        ROWS
    )


@pytest.mark.parametrize("store", ["memory", "delta"])
def test_a_delivery_to_such_a_cache_writes_nothing(tmp_path: Path, store: str) -> None:
    """The delivery agrees with one of the two contents and brings a new row."""
    cache = _database(store, tmp_path)
    _conflicting_cache(cache)
    source = CachedSource(_source([*ROWS, ("s3", 1, 4.0, 40)]), cache_database=cache)
    before = _stored(source, cache)

    with pytest.raises(CachedSourceChangedRowError, match="the cache"):
        source.as_table()

    assert _stored(source, cache) == before


@pytest.mark.parametrize("conflict", ["inner", "outer"])
@pytest.mark.parametrize("store", ["memory", "delta"])
@pytest.mark.parametrize("read", ["as_table", "iter_data", "head"])
def test_a_conflict_in_any_nested_cache_refuses_the_read_and_writes_nothing(
    tmp_path: Path, read: str, store: str, conflict: str
) -> None:
    inner_cache = _database(store, tmp_path, "inner")
    outer_cache = _database(store, tmp_path, "outer")
    _conflicting_cache(inner_cache if conflict == "inner" else outer_cache)
    inner = CachedSource(_source(ROWS), cache_database=inner_cache)
    outer = CachedSource(inner, cache_database=outer_cache)
    other = outer_cache if conflict == "inner" else inner_cache

    # A head asks the wrapped source for its head, so an inner cache refuses it too.
    with pytest.raises(CachedSourceChangedRowError, match="the cache"):
        READS[read](outer)

    assert (
        other.get_all_records(
            inner.cache_path, record_id_column=CachedSource.HASH_COLUMN_NAME
        )
        is None
    )


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("conflict", ["inner", "outer"])
def test_a_job_over_nested_caches_is_refused_for_a_conflict_in_either(
    conflict: str, orchestrator: str
) -> None:
    inner_cache, outer_cache = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    _conflicting_cache(inner_cache if conflict == "inner" else outer_cache)
    outer = CachedSource(
        CachedSource(_source(ROWS), cache_database=inner_cache),
        cache_database=outer_cache,
    )
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="readings_job", database=database)
    with job:
        summarize.pod(outer, label="summary")

    with pytest.raises(CachedSourceChangedRowError, match="the cache"):
        job.run(orchestrator=ORCHESTRATORS[orchestrator]())

    assert CALLS == []
    assert list(database.list_table_paths(())) == []


@pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
def test_a_source_holding_its_rows_shows_them_until_it_reads_the_cache_again(
    arrangement: str,
) -> None:
    """A full read and a head agree: both show the held rows, then both refuse."""
    cache = InMemoryArrowDatabase()
    source = CachedSource(_source(ROWS, arrangement), cache_database=cache)
    held = source.as_table()
    write_outside_the_rule(_source(_changed(ROWS, ("s1", 2)), arrangement), cache)

    # The held rows were checked when they were read; the cache is not read again.
    assert source.as_table().equals(held)
    assert source.head().table.num_rows == len(ROWS)

    source.clear_cache()
    with pytest.raises(CachedSourceChangedRowError, match="the cache"):
        source.head()
    with pytest.raises(CachedSourceChangedRowError, match="the cache"):
        source.as_table()


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_another_source_of_the_job_may_write_before_a_refusal_fails_the_run(
    orchestrator: str,
) -> None:
    """A stated limit (independent review of 76912324): the cached sources of one job
    are checked one by one. The refused cache is unchanged and no result of the run is
    stored for it; a sibling source that accepted its own delivery has written it."""

    def other(rows: list[Row]) -> Any:
        table = (
            _source(rows)
            .as_table()
            .rename_columns({"value": "other_value", "count": "other_count"})
        )
        return ArrowTableSource(
            table,
            tag_columns=["subject", "session"],
            tag_only_columns=True,
            record_id_columns=KEYS,
            source_id="others",
        )

    accepted_cache, refused_cache = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    _ = CachedSource(other(ROWS[:2]), cache_database=accepted_cache).as_table()
    _ = CachedSource(_source(ROWS), cache_database=refused_cache).as_table()
    accepted = CachedSource(other(ROWS), cache_database=accepted_cache)
    refused = CachedSource(
        _source(_changed(ROWS, ("s1", 2))), cache_database=refused_cache
    )
    before = _stored(refused, refused_cache)
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="two_sources", database=database)
    with job:
        summarize.pod(refused, label="summary")
        accepted.select_tag_columns(["subject", "session"], label="others")

    with pytest.raises(CachedSourceChangedRowError, match="the source delivered"):
        job.run(orchestrator=ORCHESTRATORS[orchestrator]())

    assert _stored(refused, refused_cache) == before
    assert CALLS == []
    assert len(_stored(accepted, accepted_cache)) == len(ROWS)


@pytest.mark.parametrize("read", ["head", "get_all_records", "as_table"])
def test_an_unbound_proxy_refuses_conflicts_with_a_custom_provenance_pair(
    tmp_path: Path, read: str
) -> None:
    def with_custom_pair(value: int) -> ArrowTableSource:
        table = pa.table(
            {
                "id": [1],
                "value": [value],
                "record_id::custom": ["constant"],
                "source_id::custom": ["arrow_table_v1:proxy"],
            }
        )
        return ArrowTableSource(
            table.cast(pa.schema([f.with_nullable(False) for f in table.schema])),
            source_id="proxy",
            record_id_columns="id",
            tag_columns="id",
            system_tag_columns=["record_id::custom", "source_id::custom"],
        )

    db = DeltaTableDatabase(tmp_path)
    source = with_custom_pair(10).cached(db)
    assert source.as_table()["value"].to_pylist() == [10]
    config = source.to_config()
    write_outside_the_rule(with_custom_pair(20), db)

    loaded = CachedSource.from_config(config)
    with pytest.raises(
        CachedSourceChangedRowError, match=r'1 record id .*: \{"id":1\}'
    ):
        READS[read](loaded)


def test_freshness_refuses_a_conflicted_cache_over_an_unchanged_source() -> None:
    db = InMemoryArrowDatabase()
    _conflicting_cache(db)
    source = CachedSource(_source(ROWS), cache_database=db)

    with pytest.raises(CachedSourceChangedRowError, match="2 record ids"):
        source.check_for_new_data()


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_an_unchanged_job_rerun_refuses_a_newly_conflicted_cache(
    orchestrator: str,
) -> None:
    db = InMemoryArrowDatabase()
    source = CachedSource(_source(ROWS), cache_database=db)
    job = PipelineJob(name="unchanged_readings", database=InMemoryArrowDatabase())
    with job:
        summarize.pod(source, label="summary")
    job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert len(CALLS) == len(ROWS)
    write_outside_the_rule(_source(_changed(ROWS, ("s1", 2))), db)

    with pytest.raises(CachedSourceChangedRowError, match="1 record id"):
        job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert len(CALLS) == len(ROWS)


@pytest.mark.parametrize("read", ["head", "get_all_records"])
def test_a_warm_delta_reader_refuses_conflicts_committed_by_another_handle(
    tmp_path: Path, read: str
) -> None:
    observer = DeltaTableDatabase(tmp_path)
    writer = DeltaTableDatabase(tmp_path)
    _ = CachedSource(_source(ROWS), cache_database=observer).as_table()
    source = CachedSource(_source(ROWS), cache_database=observer)
    assert source.get_all_records() is not None
    write_outside_the_rule(_source(_changed(ROWS, ("s1", 2))), writer)

    with pytest.raises(CachedSourceChangedRowError, match="1 record id"):
        READS[read](source)


@pytest.mark.parametrize("preview", [False, True], ids=["head", "notebook"])
def test_an_unheld_delta_head_scans_only_record_ids_and_hashes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, preview: bool
) -> None:
    from orcapod.databases import delta_lake_databases

    db = DeltaTableDatabase(tmp_path)
    _ = CachedSource(_source(ROWS), cache_database=db).as_table()
    source = CachedSource(_source(ROWS), cache_database=DeltaTableDatabase(tmp_path))
    scans: list[list[str]] = []
    original = delta_lake_databases.delta_to_pyarrow_dataset

    class ScanProbe:
        def __init__(self, dataset: Any) -> None:
            self.dataset = dataset

        def to_table(self, *args: Any, **kwargs: Any) -> pa.Table:
            table = self.dataset.to_table(*args, **kwargs)
            scans.append(table.column_names)
            return table

        def to_batches(self, *, columns: list[str]) -> Any:
            scans.append(columns)
            return self.dataset.to_batches(columns=columns)

    def tracked(*args: Any, **kwargs: Any) -> ScanProbe:
        return ScanProbe(original(*args, **kwargs))

    monkeypatch.setattr(delta_lake_databases, "delta_to_pyarrow_dataset", tracked)
    if preview:
        assert "could not read" not in source._repr_html_()
    else:
        assert source.head(0).table.num_rows == 0
    (columns,) = scans
    (record_id,) = [name for name in columns if name.startswith("_tag_record_id::")]
    assert set(columns) == {
        record_id,
        DeltaTableDatabase.RECORD_ID_COLUMN,
        "__data_context",
    }
