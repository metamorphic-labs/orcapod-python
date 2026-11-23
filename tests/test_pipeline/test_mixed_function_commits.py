"""Function batches keep their commit cadence across database kinds."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, Literal, Self, TypedDict, cast

import deltalake
import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    ExtensionAwareDatabase,
    InMemoryArrowDatabase,
    NoOpArrowDatabase,
)
from orcapod.databases._pending_commits import PendingCommit
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import DictSource
from orcapod.system_constants import (
    PIPELINE_DB_SCHEMA_VERSION,
    RESULT_DB_SCHEMA_VERSION,
)
from tests.test_databases.test_connector_arrow_database import MockDBConnector

Layout = Literal["memory", "delta", "noop", "connector"]
PIPELINE = PIPELINE_DB_SCHEMA_VERSION
RESULT = RESULT_DB_SCHEMA_VERSION


@function_pod(output_keys="y")
async def double(x: int, offset: int) -> int:
    """Return a stable result with two bound input fields."""
    return 2 * x + offset


class CommitConnector(MockDBConnector):
    """Store function tables as Arrow and count physical connector upserts."""

    def __init__(self) -> None:
        """Start with no tables or writes."""
        super().__init__()
        self.upserts: dict[str, list[int]] = defaultdict(list)

    def upsert_records(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool = False,
    ) -> None:
        """Record each real upsert's table and row count."""
        super().upsert_records(table_name, records, id_column, skip_existing)
        self.upserts[table_name].append(records.num_rows)


def make_job(
    pipeline: ArrowDatabaseProtocol,
    results: ArrowDatabaseProtocol | None = None,
    *,
    reverse_order: bool = False,
) -> PipelineJob:
    """Build a batch over five rows, with optionally separate result storage."""
    job = PipelineJob(name="mixed", database=pipeline)
    rows = [{"tag": i, "group": "all", "x": i, "offset": i + 10} for i in range(5)]
    tags = ["tag", "group"]
    if reverse_order:
        rows = [dict(reversed(tuple(row.items()))) for row in rows]
        tags.reverse()
    with job:
        _ = double.pod(
            DictSource(
                data=rows, tag_columns=tags, tag_only_columns=True, source_id="src"
            ),
            label="out",
        )
    if results is not None:
        job.nodes["out"].attach_databases(
            pipeline_database=pipeline.at("mixed"), result_database=results
        )
    return job


class RunOptions(TypedDict, total=False):
    """The cadence option, omitted when exercising the default."""

    commit_every_rows: int


def run(job: PipelineJob, asynchronous: bool, every: int | None) -> None:
    """Exercise either public entry point and omit the option for the default."""
    options: RunOptions = {} if every is None else {"commit_every_rows": every}
    if asynchronous:
        _ = asyncio.run(job.run_async(**options))
    else:
        _ = job.run(**options)


def delta_history(root: Path, kind: str) -> list[tuple[str, int]]:
    """Read the operation and number of rows added by each Delta commit."""
    (path,) = root.rglob(f"{kind}/_delta_log")
    counts = deltalake.DeltaTable(str(path.parent)).history()
    return [
        (item["operation"], int(item["operationMetrics"]["num_added_rows"]))
        for item in reversed(counts)
    ]


@pytest.mark.parametrize("pipeline_kind", ["memory", "delta", "noop", "connector"])
@pytest.mark.parametrize("result_kind", ["memory", "delta"])
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("every", [1, 3, None], ids=["one", "three", "default"])
def test_mixed_database_batches_keep_main_cadence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pipeline_kind: Layout,
    result_kind: Layout,
    asynchronous: bool,
    every: int | None,
) -> None:
    """Keep pipeline writes and whole-store flushes at the measured main cadence."""
    connector = CommitConnector()
    pipeline_root = tmp_path / "pipeline"
    result_root = tmp_path / "result"
    pipeline = {
        "connector": ConnectorArrowDatabase(connector),
        "delta": DeltaTableDatabase(pipeline_root),
        "memory": InMemoryArrowDatabase(),
        "noop": NoOpArrowDatabase(),
    }[pipeline_kind]
    results = (
        DeltaTableDatabase(result_root)
        if result_kind == "delta"
        else InMemoryArrowDatabase()
    )
    whole_flushes: dict[type, int] = defaultdict(int)
    promotions: dict[str, list[int]] = defaultdict(list)
    flush_memory = InMemoryArrowDatabase.flush
    flush_noop = NoOpArrowDatabase.flush
    promote = getattr(InMemoryArrowDatabase, "flush_pending_table", None)

    def heap_rows(db: InMemoryArrowDatabase) -> dict[str, int]:
        tables = db._tables  # pyright: ignore[reportPrivateUsage]  # distinguish promoted and pending rows
        return {key: table.num_rows for key, table in tables.items()}

    def record_promotions(db: InMemoryArrowDatabase, before: dict[str, int]) -> None:
        for key, after in heap_rows(db).items():
            inserted = after - before.get(key, 0)
            if inserted:
                promotions[key.split("/")[-1]].append(inserted)

    def observe_memory(db: InMemoryArrowDatabase) -> None:
        whole_flushes[InMemoryArrowDatabase] += 1
        before = heap_rows(db)
        flush_memory(db)
        record_promotions(db, before)

    def observe_noop(db: NoOpArrowDatabase) -> None:
        whole_flushes[NoOpArrowDatabase] += 1
        flush_noop(db)

    def observe_promotion(db: InMemoryArrowDatabase, path: tuple[str, ...]) -> None:
        assert promote is not None
        before = heap_rows(db)
        promote(db, path)
        record_promotions(db, before)

    monkeypatch.setattr(InMemoryArrowDatabase, "flush", observe_memory)
    monkeypatch.setattr(NoOpArrowDatabase, "flush", observe_noop)
    if promote is not None:
        monkeypatch.setattr(
            InMemoryArrowDatabase, "flush_pending_table", observe_promotion
        )
    job = make_job(pipeline, results, reverse_order=asynchronous)
    run(job, asynchronous, every)
    pipeline_batches = {1: [1, 1, 1, 1, 1], 3: [3, 2], None: [5]}[every]
    result_batches = [5] if asynchronous else pipeline_batches
    # Main flushes once per full batch, once at node end and twice at job end
    # for in-memory and no-op pipeline stores, even when the last batch is empty.
    expected_whole = 3 + (5 // every if every is not None else 0)
    if pipeline_kind == "memory":
        assert whole_flushes[InMemoryArrowDatabase] == expected_whole
        assert promotions[PIPELINE] == pipeline_batches
    elif pipeline_kind == "noop":
        assert whole_flushes[NoOpArrowDatabase] == expected_whole
        assert not pipeline.list_table_paths()
    elif pipeline_kind == "connector":
        assert list(connector.upserts.values()) == [pipeline_batches]
    else:
        assert delta_history(pipeline_root, PIPELINE) == [
            ("WRITE", rows) for rows in pipeline_batches
        ]
    if result_kind == "delta":
        assert delta_history(result_root, RESULT) == [
            ("WRITE", rows) for rows in result_batches
        ]
    elif pipeline_kind == "noop":
        assert not promotions[RESULT], (
            "main leaves separate in-memory results pending in a dry run"
        )
    else:
        # The one promotion exception: main left these rows pending, even after
        # publishing their references. The fix promotes them at the batch boundary.
        assert promotions[RESULT] == result_batches
    assert job.nodes["out"].as_table().num_rows == 5
    if pipeline_kind != "noop":
        assert job.nodes["out"].as_source().as_table().num_rows == 5


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
def test_shared_in_memory_store_keeps_whole_flush_count(
    asynchronous: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A default all-in-memory job has one whole flush per batch as on main."""
    store = InMemoryArrowDatabase()
    original = InMemoryArrowDatabase.flush
    count = 0

    def observe(db: InMemoryArrowDatabase) -> None:
        nonlocal count
        count += 1
        original(db)

    monkeypatch.setattr(InMemoryArrowDatabase, "flush", observe)
    run(make_job(store), asynchronous, 3)
    assert count == 4


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
def test_noop_result_store_keeps_pipeline_records(
    tmp_path: Path, asynchronous: bool
) -> None:
    """A declared discard keeps main's pipeline bookkeeping and current rows."""
    db = DeltaTableDatabase(tmp_path)
    results = NoOpArrowDatabase()
    job = make_job(db, results)
    run(job, asynchronous, 3)
    assert delta_history(tmp_path, PIPELINE) == [("WRITE", 3), ("WRITE", 2)]
    assert not results.list_table_paths()
    assert job.nodes["out"].as_table().num_rows == 5
    assert job.nodes["out"].as_source().as_table().num_rows == 0


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
def test_promoting_memory_results_preserves_public_read_views(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, asynchronous: bool
) -> None:
    """Pending and promoted result rows are identical through all row readers."""
    memory = InMemoryArrowDatabase()
    original = getattr(InMemoryArrowDatabase, "flush_pending_table", None)
    observed: list[int] = []

    def promote(db: InMemoryArrowDatabase, path: tuple[str, ...]) -> None:
        def read() -> list[pa.Table | None]:
            record_id_name = "rid"
            rows = db.get_all_records(path, record_id_column=record_id_name)
            assert rows is not None
            ids = cast(list[bytes], rows[record_id_name].to_pylist())
            return [
                rows.combine_chunks(),
                db.get_record_by_id(path, ids[0], record_id_column=record_id_name),
                db.get_records_by_ids(path, ids, record_id_column=record_id_name),
                db.get_records_with_column_value(
                    path, {"y": 10}, record_id_column=record_id_name
                ),
            ]

        before = read()
        assert original is not None
        original(db, path)
        assert before == read()
        full = before[0]
        assert full is not None
        observed.append(full.num_rows)

    if original is not None:
        monkeypatch.setattr(InMemoryArrowDatabase, "flush_pending_table", promote)
    job = make_job(DeltaTableDatabase(tmp_path), memory)
    run(job, asynchronous, 3)
    assert observed == ([5, 5] if asynchronous else [3, 5])
    joined = job.nodes["out"].get_all_records()
    assert joined is not None and joined.num_rows == 5


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
def test_connector_job_keeps_only_ready_records_on_result_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, asynchronous: bool
) -> None:
    """A connector can publish independently queued work while a result is pending."""
    from orcapod.errors import OrcapodDatabaseWriteError

    connector = CommitConnector()
    pipeline = ConnectorArrowDatabase(connector)
    results = DeltaTableDatabase(tmp_path)
    job = make_job(pipeline, results)
    original_flush = DeltaTableDatabase.flush_batch
    original_write = deltalake.write_deltalake
    checked = False

    def flush_result(db: DeltaTableDatabase, path: tuple[str, ...]) -> None:
        nonlocal checked
        if path[-1] == RESULT:
            independent = ("independent",)
            pipeline.add_record(
                independent, b"ok", pa.table({"value": [1]}), skip_duplicates=True
            )
            pipeline.flush()
            fresh = ConnectorArrowDatabase(connector)
            assert fresh.list_table_paths() == [independent]
            checked = True
        original_flush(db, path)

    def fail_result(table_or_uri, *args, **kwargs):
        uri = (
            table_or_uri.table_uri
            if isinstance(table_or_uri, deltalake.DeltaTable)
            else table_or_uri
        )
        if Path(uri).name == RESULT:
            raise OSError("result store unavailable")
        original_write(table_or_uri, *args, **kwargs)

    monkeypatch.setattr(DeltaTableDatabase, "flush_batch", flush_result)
    monkeypatch.setattr(deltalake, "write_deltalake", fail_result)
    with pytest.raises(OrcapodDatabaseWriteError) as raised:
        run(job, asynchronous, 3)
    assert isinstance(raised.value.__cause__, OSError)
    assert checked
    pipeline.flush()
    assert pipeline.list_table_paths() == [("independent",)]


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
def test_result_commit_error_names_the_actual_store_and_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, asynchronous: bool
) -> None:
    """Check error identity independently of whether pipeline records were written."""
    from orcapod.errors import OrcapodDatabaseWriteError

    pipeline = DeltaTableDatabase(tmp_path / "pipeline")
    results = DeltaTableDatabase(tmp_path / "results").at("external")
    job = make_job(pipeline, results)
    attempted: list[Path] = []

    def fail_write(table_or_uri, *args, **kwargs):
        attempted.append(Path(table_or_uri))
        raise OSError("injected result failure")

    monkeypatch.setattr(deltalake, "write_deltalake", fail_write)
    with pytest.raises(OrcapodDatabaseWriteError) as raised:
        run(job, asynchronous, 3)
    assert attempted and attempted[0].name == RESULT
    assert str(attempted[0]) in str(raised.value)
    assert isinstance(raised.value.__cause__, OSError)


def test_connector_result_failure_withdraws_its_pipeline_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed connector upsert withdraws the pending Delta references."""
    from orcapod.errors import OrcapodDatabaseWriteError

    pipeline = DeltaTableDatabase(tmp_path)
    connector = CommitConnector()
    results = ConnectorArrowDatabase(connector)
    job = make_job(pipeline, results)

    def fail_result(*args: object, **kwargs: object) -> None:
        raise OSError("connector result write failed")

    monkeypatch.setattr(connector, "upsert_records", fail_result)
    with pytest.raises(OrcapodDatabaseWriteError) as raised:
        run(job, asynchronous=False, every=3)
    assert isinstance(raised.value.__cause__, OSError)
    assert RESULT in str(raised.value)
    pipeline.flush()
    assert not pipeline.list_table_paths()


class DatabaseWithoutCommitGuards:
    """Stand-in for the database interface without the required commit methods."""

    def __init__(self, inner: InMemoryArrowDatabase) -> None:
        self._inner = inner

    def add_record(self, *args: Any, **kwargs: Any) -> None:
        self._inner.add_record(*args, **kwargs)

    def add_records(self, *args: Any, **kwargs: Any) -> None:
        self._inner.add_records(*args, **kwargs)

    def get_record_by_id(self, *args: Any, **kwargs: Any) -> pa.Table | None:
        return self._inner.get_record_by_id(*args, **kwargs)

    def get_all_records(self, *args: Any, **kwargs: Any) -> pa.Table | None:
        return self._inner.get_all_records(*args, **kwargs)

    def get_records_by_ids(self, *args: Any, **kwargs: Any) -> pa.Table | None:
        return self._inner.get_records_by_ids(*args, **kwargs)

    def get_records_with_column_value(
        self, *args: Any, **kwargs: Any
    ) -> pa.Table | None:
        return self._inner.get_records_with_column_value(*args, **kwargs)

    def flush(self) -> None:
        self._inner.flush()

    @property
    def base_path(self) -> tuple[str, ...]:
        return self._inner.base_path

    def at(self, *path_components: str) -> Self:
        return type(self)(self._inner.at(*path_components))

    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        return self._inner.table_exists(record_path)

    def list_table_paths(self, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
        return self._inner.list_table_paths(prefix)

    def to_config(self) -> dict[str, Any]:
        return self._inner.to_config()

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Self:
        return cls(InMemoryArrowDatabase.from_config(config))


class ProtocolDatabase(DatabaseWithoutCommitGuards):
    """Forward the complete protocol without exposing unrelated backend methods."""

    @property
    def stores_records(self) -> bool:
        return self._inner.stores_records

    def pending_result(
        self, record_path: tuple[str, ...], record_id: bytes
    ) -> PendingCommit | None:
        return self._inner.pending_result(record_path, record_id)

    def guard_record(
        self, record_path: tuple[str, ...], record_id: bytes, dependency: PendingCommit
    ) -> AbstractContextManager[None]:
        return self._inner.guard_record(record_path, record_id, dependency)

    def fail_pending_result(self, record_path: tuple[str, ...]) -> None:
        self._inner.fail_pending_result(record_path)

    def flush_result_batch(
        self, record_path: tuple[str, ...], *, publish_records: bool
    ) -> None:
        self._inner.flush_result_batch(record_path, publish_records=publish_records)

    def flush_batch(self, record_path: tuple[str, ...]) -> None:
        self._inner.flush_batch(record_path)


def test_library_databases_conform_to_the_commit_protocol(tmp_path: Path) -> None:
    """Built-in stores conform, and an incomplete store fails the runtime check."""
    stores: list[ArrowDatabaseProtocol] = [
        InMemoryArrowDatabase(),
        DeltaTableDatabase(tmp_path),
        ConnectorArrowDatabase(CommitConnector()),
        NoOpArrowDatabase(),
        ExtensionAwareDatabase.from_config(InMemoryArrowDatabase().to_config()),
    ]
    assert all(isinstance(store, ArrowDatabaseProtocol) for store in stores)
    assert not isinstance(
        DatabaseWithoutCommitGuards(InMemoryArrowDatabase()), ArrowDatabaseProtocol
    )


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("placement", ["pipeline", "result", "both", "shared"])
def test_complete_protocol_database_preserves_main_rows_and_flushes(
    monkeypatch: pytest.MonkeyPatch, asynchronous: bool, placement: str
) -> None:
    """The complete internal protocol is sufficient to run a job."""
    pipeline = InMemoryArrowDatabase().at("pipeline")
    result = InMemoryArrowDatabase().at("result")
    pipeline_store = ProtocolDatabase(pipeline) if placement != "result" else pipeline
    result_store = (
        ProtocolDatabase(result) if placement in ("result", "both") else result
    )
    assert isinstance(pipeline_store, ArrowDatabaseProtocol)
    assert isinstance(result_store, ArrowDatabaseProtocol)
    calls: list[tuple[str, ...]] = []
    original_flush = InMemoryArrowDatabase.flush

    def observe_flush(database: InMemoryArrowDatabase) -> None:
        calls.append(database.base_path)
        original_flush(database)

    monkeypatch.setattr(InMemoryArrowDatabase, "flush", observe_flush)
    job = make_job(
        pipeline_store,
        None if placement == "shared" else result_store,
    )
    run(job, asynchronous, every=2)
    node = job.nodes["out"]
    assert sorted(cast(list[int], node.as_table()["y"].to_pylist())) == [
        10,
        13,
        16,
        19,
        22,
    ]
    assert sorted(cast(list[int], node.as_source().as_table()["y"].to_pylist())) == [
        10,
        13,
        16,
        19,
        22,
    ]
    assert calls == [("pipeline", "mixed")] * 4 + [("pipeline", "mixed", "_result")]
