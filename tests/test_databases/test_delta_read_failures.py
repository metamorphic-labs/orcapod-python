"""Regression tests for ITL-884: a Delta table that cannot be read is never "no rows".

Covers:
- A table that exists but cannot be read raises ``OrcapodDatabaseReadError`` on every read
  path, naming the table; a table that does not exist still reads as ``None``.
- A table whose log cannot be loaded is never overwritten by a flush.
- ``flush`` raises ``OrcapodDatabaseReadError`` and the append refusal's
  ``InputValidationError`` themselves, not wrapped in ``RuntimeError``, and
  notes every other batch that failed in the same flush.
- An append whose column types differ from the stored table is refused before
  anything is written, naming the column and both types; legitimate appends
  (same types in another layout, new or missing columns, unchanged extension
  storage) still succeed.
- A readable table whose stored user-class shape differs from the registered class
  raises ``TypeIdentityError``, not ``OrcapodDatabaseReadError`` (ITL-881).
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import json
import os
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import deltalake
import pyarrow as pa
import pytest
from deltalake.exceptions import DeltaProtocolError

from orcapod import function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.databases import delta_lake_databases
from orcapod.errors import (
    InputValidationError,
    OrcapodDatabaseReadError,
    TypeIdentityError,
)
from orcapod.pipeline import PipelineJob
from orcapod.sources import DictSource

PATH = ("results", "t")


def rows(ids: list[bytes], x: pa.Array[Any], **extra: pa.Array[Any]) -> pa.Table:
    """Build a table keyed by ``rid`` with an ``x`` column and optional extras."""
    return pa.table({"rid": pa.array(ids, pa.large_binary()), "x": x, **extra})


def table_dir(base: Path) -> Path:
    """Return the directory of the table at ``PATH`` under ``base``."""
    return base.joinpath(*PATH)


def snapshot(directory: Path) -> dict[str, bytes]:
    """Every file under ``directory`` with its bytes, keyed by relative path."""
    return {
        str(p.relative_to(directory)): p.read_bytes()
        for p in sorted(directory.rglob("*"))
        if p.is_file()
    }


class _Ext(pa.ExtensionType):
    """A minimal extension type standing in for a logical type's storage."""

    def __init__(self, storage: pa.DataType) -> None:
        """Wrap ``storage`` in the test extension type."""
        super().__init__(storage, "orcapod.test.itl884")

    def __arrow_ext_serialize__(self) -> bytes:
        """Return the (empty) serialized parameters of the type."""
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "_Ext":
        """Rebuild the type over ``storage_type``."""
        return cls(storage_type)


def ext_array(values: list[Any], storage: pa.DataType) -> pa.ExtensionArray[Any]:
    """Build an extension array of ``values`` over the given storage type."""
    return pa.ExtensionArray.from_storage(_Ext(storage), pa.array(values, storage))


@pytest.fixture
def base(tmp_path: Path) -> Path:
    """Return the database root, inside the test's temporary directory."""
    return tmp_path / "store"


@pytest.fixture
def stored(base: Path) -> DeltaTableDatabase:
    """A store holding two rows (``a``, ``b``) of ``x: int64`` at ``PATH``."""
    db = DeltaTableDatabase(base_path=base)
    db.add_records(
        PATH,
        rows([b"a", b"b"], pa.array([1, 2], pa.int64())),
        record_id_column="rid",
        flush=True,
    )
    return db


@pytest.fixture
def unreadable(base: Path, stored: DeltaTableDatabase) -> DeltaTableDatabase:
    """A fresh view of a table whose log is intact but whose data files are gone."""
    data_files = list(table_dir(base).glob("*.parquet"))
    assert data_files
    for f in data_files:
        f.unlink()
    return DeltaTableDatabase(base_path=base)


def unsupported_reader_feature(base: Path) -> None:
    """Make the table's log require a reader feature deltalake does not know."""
    log = table_dir(base) / "_delta_log" / "00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if "protocol" in action:
            action["protocol"] = {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["orcapodMadeUpFeature"],
                "writerFeatures": ["orcapodMadeUpFeature"],
            }
    log.write_text("\n".join(json.dumps(a) for a in actions))


def assert_names_table(exc: OrcapodDatabaseReadError, base: Path) -> None:
    """Assert that ``exc`` names the table at ``PATH`` and chains its cause."""
    assert Path(exc.table) == table_dir(base)
    assert exc.table in str(exc)
    assert exc.__cause__ is not None


# ---------------------------------------------------------------------------
# Reads: unreadable table raises, absent table is None
# ---------------------------------------------------------------------------

READS: dict[str, Callable[[DeltaTableDatabase], object]] = {
    "get_all_records": lambda db: db.get_all_records(PATH),
    "get_records_by_ids": lambda db: db.get_records_by_ids(PATH, [b"a"]),
    "get_record_by_id": lambda db: db.get_record_by_id(PATH, b"a"),
    "get_records_with_column_value": lambda db: db.get_records_with_column_value(
        PATH, {"x": 1}
    ),
}


@pytest.fixture(params=[("table",), PATH], ids=["top-level", "nested"])
def damaged_path(request: pytest.FixtureRequest) -> tuple[str, ...]:
    """Return the record path of the table whose log will be removed."""
    return request.param


@pytest.fixture(
    params=[None, ["tag1", "tag2"], ["tag2", "tag1"]],
    ids=["flat", "partitioned", "reordered-partitions"],
)
def damaged_store(
    base: Path,
    damaged_path: tuple[str, ...],
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> DeltaTableDatabase:
    """Return a fresh handle after dropping the log from a populated table."""
    monkeypatch.setattr(delta_lake_databases, "sleep", lambda _delay: None)
    directory = base.joinpath(*damaged_path)
    data = pa.table(
        {
            DeltaTableDatabase.RECORD_ID_COLUMN: [b"a", b"b"],
            "tag1": ["a", "c"],
            "tag2": ["b", "d"],
            "x": [1, 3],
            "y": [2, 4],
        }
    )
    partitions = request.param
    if partitions == ["tag2", "tag1"]:
        data = data.select(
            ["y", "tag2", DeltaTableDatabase.RECORD_ID_COLUMN, "x", "tag1"]
        )
    deltalake.write_deltalake(directory, data, partition_by=partitions)
    warm = DeltaTableDatabase(base_path=base)
    assert warm.get_all_records(damaged_path) is not None
    assert any(directory.rglob("*.parquet"))
    shutil.rmtree(directory / "_delta_log")
    return DeltaTableDatabase(base_path=base)


MISSING_LOG_READS: dict[
    str, Callable[[DeltaTableDatabase, tuple[str, ...]], object]
] = {
    "get_all_records": lambda db, path: db.get_all_records(path),
    "get_records_by_ids_forward": lambda db, path: db.get_records_by_ids(
        path, [b"a", b"b"]
    ),
    "get_records_by_ids_reverse": lambda db, path: db.get_records_by_ids(
        path, [b"b", b"a"]
    ),
    "get_record_by_id": lambda db, path: db.get_record_by_id(path, b"a"),
    "get_records_with_column_value_reverse": lambda db, path: (
        db.get_records_with_column_value(path, {"y": 2, "x": 1})
    ),
    "get_records_with_column_value_forward": lambda db, path: (
        db.get_records_with_column_value(path, {"x": 1, "y": 2})
    ),
    "get_table_schema": lambda db, path: db.get_table_schema(path),
    "table_exists": lambda db, path: db.table_exists(path),
}


@pytest.mark.parametrize(
    "read", MISSING_LOG_READS.values(), ids=list(MISSING_LOG_READS)
)
def test_missing_log_is_a_read_error(
    damaged_path: tuple[str, ...],
    damaged_store: DeltaTableDatabase,
    read: Callable[[DeltaTableDatabase, tuple[str, ...]], object],
) -> None:
    """Reads and table metadata queries name the table with orphaned data."""
    with pytest.raises(OrcapodDatabaseReadError, match="_delta_log") as info:
        _ = read(damaged_store, damaged_path)
    assert info.value.record_path == damaged_path


def test_table_exists_uses_the_loaded_snapshot(
    stored: DeltaTableDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A warm existence check performs no new log opens or data-directory walks."""
    assert stored.get_all_records(PATH) is not None

    def unexpected_io(*_args: object, **_kwargs: object) -> None:
        pytest.fail("A warm existence check should use the loaded snapshot")

    with monkeypatch.context() as probes:
        probes.setattr(deltalake.DeltaTable, "__init__", unexpected_io)
        probes.setattr(os, "scandir", unexpected_io)
        assert stored.table_exists(PATH)


def test_flush_refuses_a_missing_log(
    base: Path,
    damaged_path: tuple[str, ...],
    damaged_store: DeltaTableDatabase,
) -> None:
    """Flushing to a folder with orphaned data writes no files or log."""
    directory = base.joinpath(*damaged_path)
    before = snapshot(directory)
    damaged_store.add_records(
        damaged_path, rows([b"c"], pa.array([3], pa.int64())), record_id_column="rid"
    )

    with pytest.raises(OrcapodDatabaseReadError, match="_delta_log") as info:
        damaged_store.flush()

    assert info.value.record_path == damaged_path
    assert snapshot(directory) == before


@pytest.mark.parametrize("name", ["table", "tag=child"])
def test_partition_paths_do_not_count_as_separate_tables(base: Path, name: str) -> None:
    """A healthy table owns its partitions, even when listing starts in one."""
    path = ("group", name)
    data = pa.table({"tag1": ["a"], "tag2": ["b"], "x": [1], "y": [2]})
    deltalake.write_deltalake(base.joinpath(*path), data, partition_by=["tag1", "tag2"])
    db = DeltaTableDatabase(base_path=base)
    assert not db.table_exists(("group",))
    assert db.list_table_paths() == [path]
    assert db.list_table_paths((*path, "tag1=a")) == []


@pytest.mark.parametrize("operation", ["exists", "read", "schema", "create"])
def test_partition_path_accepts_independent_child(
    base: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    """A parent's active data files do not make its partition a damaged child."""
    monkeypatch.setattr(delta_lake_databases, "sleep", lambda _delay: None)
    parent = ("group", "table")
    partition = (*parent, "tag1=a", "tag2=b")
    data = pa.table(
        {
            DeltaTableDatabase.RECORD_ID_COLUMN: [b"a"],
            "tag1": ["a"],
            "tag2": ["b"],
            "x": [1],
        }
    )
    deltalake.write_deltalake(
        base.joinpath(*parent), data, partition_by=["tag1", "tag2"]
    )
    db = DeltaTableDatabase(base).at(*partition[:-1])
    child = partition[-1:]
    if operation == "exists":
        assert not db.table_exists(child)
    elif operation == "read":
        assert db.get_all_records(child) is None
    elif operation == "schema":
        assert db.get_table_schema(child) is None
    else:
        db.add_record(child, b"b", pa.table({"x": [2]}), flush=True)
        child_rows = DeltaTableDatabase(base).get_all_records(partition)
        assert child_rows is not None and child_rows.column("x").to_pylist() == [2]
        parent_rows = DeltaTableDatabase(base).get_all_records(parent)
        assert parent_rows is not None and parent_rows.column("x").to_pylist() == [1]


def test_missing_child_log_is_refused_inside_occupied_partition(
    base: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A parent's files and a damaged child's files can share a partition."""
    monkeypatch.setattr(delta_lake_databases, "sleep", lambda _delay: None)
    parent = ("parent",)
    child = (*parent, "tag1=a", "tag2=b")
    record_id = DeltaTableDatabase.RECORD_ID_COLUMN
    data = pa.table({record_id: [b"a"], "tag1": ["a"], "tag2": ["b"], "x": [1]})
    deltalake.write_deltalake(
        base.joinpath(*parent), data, partition_by=["tag1", "tag2"]
    )
    directory = base.joinpath(*child)
    deltalake.write_deltalake(directory, pa.table({record_id: [b"b"], "x": [2]}))
    shutil.rmtree(directory / "_delta_log")
    before = snapshot(directory)

    with pytest.raises(OrcapodDatabaseReadError, match="_delta_log"):
        DeltaTableDatabase(base).get_all_records(child)
    with pytest.raises(OrcapodDatabaseReadError, match="_delta_log"):
        DeltaTableDatabase(base).add_record(
            child, b"c", pa.table({"x": [3]}), flush=True
        )
    assert snapshot(directory) == before


def move_delta_data_file(directory: Path) -> Path:
    """Move a Delta file into an arbitrary directory while retaining its AddFile."""
    log = next((directory / "_delta_log").glob("*.json"))
    original = next(directory.glob("*.parquet"))
    moved = directory / "files" / "bucket" / original.name
    moved.parent.mkdir(parents=True)
    original.rename(moved)
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if "add" in action:
            action["add"]["path"] = moved.relative_to(directory).as_posix()
    log.write_text("".join(json.dumps(action) + "\n" for action in actions))

    return moved


@pytest.mark.parametrize("write", [False, True], ids=["read", "flush"])
def test_missing_log_with_non_hive_data_path(
    base: Path, monkeypatch: pytest.MonkeyPatch, write: bool
) -> None:
    """A valid AddFile path can contain arbitrary directory components."""
    monkeypatch.setattr(delta_lake_databases, "sleep", lambda _delay: None)
    directory = table_dir(base)
    data = pa.table({DeltaTableDatabase.RECORD_ID_COLUMN: [b"a"], "x": [1]})
    deltalake.write_deltalake(directory, data)
    _ = move_delta_data_file(directory)

    db = DeltaTableDatabase(base_path=base)
    original_rows = db.get_all_records(PATH)
    assert original_rows is not None and original_rows.column("x").to_pylist() == [1]
    shutil.rmtree(directory / "_delta_log")
    before = snapshot(directory)
    fresh = DeltaTableDatabase(base_path=base)

    with pytest.raises(OrcapodDatabaseReadError, match="_delta_log") as info:
        if write:
            fresh.add_record(PATH, b"b", pa.table({"x": [2]}), flush=True)
        else:
            fresh.get_all_records(PATH)
    assert info.value.record_path == PATH
    assert snapshot(directory) == before


@pytest.mark.parametrize("listing", ["list_table_paths", "list_sources"])
def test_discovery_keeps_a_table_with_retained_non_hive_data(
    base: Path, listing: str
) -> None:
    """A retired data file in a subdirectory does not hide a healthy table."""
    directory = table_dir(base)
    deltalake.write_deltalake(directory, pa.table({"x": [1]}))
    retired = move_delta_data_file(directory)
    deltalake.write_deltalake(directory, pa.table({"x": [2]}), mode="overwrite")
    assert retired.is_file()

    db = DeltaTableDatabase(base)
    assert getattr(db, listing)() == [PATH]


def test_list_sources_keeps_directory_symlink_aliases(base: Path) -> None:
    """Source listing follows a directory alias as it does a regular table path."""
    target = base / "table"
    deltalake.write_deltalake(target, pa.table({"x": [1]}))
    try:
        (base / "alias").symlink_to(target, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"Directory symlinks unavailable: {exc}")

    db = DeltaTableDatabase(base)
    assert sorted(db.list_sources()) == [("alias",), ("table",)]
    assert db.list_table_paths() == [("table",)]


@pytest.mark.parametrize("listing", ["list_table_paths", "list_sources"])
def test_healthy_partitioned_discovery_keeps_legacy_io(
    base: Path, monkeypatch: pytest.MonkeyPatch, listing: str
) -> None:
    """Path listing opens no log, and source listing stops at the first table."""
    deltalake.write_deltalake(
        table_dir(base),
        pa.table({"tag1": ["a", "b"], "tag2": ["c", "d"], "x": [1, 2]}),
        partition_by=["tag1", "tag2"],
    )
    db = DeltaTableDatabase(base)
    opens: list[Path] = []
    directories: list[Path] = []
    original_open = deltalake.DeltaTable.__init__
    original_scandir = os.scandir

    def track_open(
        table: deltalake.DeltaTable,
        uri: str,
        *,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        opens.append(Path(uri))
        original_open(table, uri, storage_options=storage_options)

    def track_scandir(directory: str):
        directories.append(Path(directory))
        return original_scandir(directory)

    monkeypatch.setattr(deltalake.DeltaTable, "__init__", track_open)
    monkeypatch.setattr(os, "scandir", track_scandir)
    assert getattr(db, listing)() == [PATH]
    if listing == "list_table_paths":
        assert not opens
    else:
        assert opens == [base / PATH[0], table_dir(base)]
        assert all(not path.is_relative_to(table_dir(base)) for path in directories)


def test_scoped_partition_listing_only_walks_its_subtree(
    base: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scoping a handle inside a partition does not turn it into a table."""
    scope = (*PATH, "tag1=a")
    data = pa.table({"tag1": ["a"], "tag2": ["b"], "x": [1]})
    deltalake.write_deltalake(table_dir(base), data, partition_by=["tag1", "tag2"])
    db = DeltaTableDatabase(base_path=base).at(*scope)
    original = delta_lake_databases._list_local_directory  # pyright: ignore[reportPrivateUsage]

    def list_scoped(directory: str):
        assert Path(directory).is_relative_to(base.joinpath(*scope))
        return original(directory)

    monkeypatch.setattr(delta_lake_databases, "_list_local_directory", list_scoped)
    assert db.list_table_paths() == []


def test_empty_initial_log_is_refused_on_read(
    base: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty log with existing data cannot be mistaken for an absent table."""
    monkeypatch.setattr(delta_lake_databases, "sleep", lambda _delay: None)
    deltalake.write_deltalake(table_dir(base), pa.table({"x": [1]}))
    log = table_dir(base) / "_delta_log"
    shutil.rmtree(log)
    log.mkdir()

    with pytest.raises(OrcapodDatabaseReadError, match="_delta_log") as info:
        DeltaTableDatabase(base).get_all_records(PATH)
    assert info.value.record_path == PATH


@function_pod(output_keys=["sum", "difference"])
def _combine(x: int, y: int) -> tuple[int, int]:
    """Compute two columns for the persistent job result."""
    return x + y, x - y


@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
def test_job_refuses_a_missing_result_log(
    base: Path, monkeypatch: pytest.MonkeyPatch, use_async: bool, reverse: bool
) -> None:
    """A rebuilt job refuses orphaned results without writing to the store."""
    monkeypatch.setattr(
        delta_lake_databases, "sleep", lambda _delay: None, raising=False
    )
    records = [
        {"tag1": "a", "tag2": 1, "x": 2, "y": 7},
        {"tag1": "b", "tag2": 2, "x": 4, "y": 8},
    ]
    tags = ["tag1", "tag2"]
    if reverse:
        records = [dict(reversed(list(record.items()))) for record in records]
        tags.reverse()

    def run() -> None:
        source = DictSource(
            records, tag_columns=tags, tag_only_columns=True, source_id="input"
        )
        job = PipelineJob(name="missing_log", database=DeltaTableDatabase(base))
        with job:
            _ = _combine.pod(source, label="combine")
        if use_async:
            _ = asyncio.run(job.run_async())
        else:
            _ = job.run()

    run()
    (result_log,) = [
        log
        for log in base.rglob("_delta_log")
        if "_result" in log.relative_to(base).parts
        and any(log.parent.glob("*.parquet"))
    ]
    shutil.rmtree(result_log)
    before = snapshot(base)

    with pytest.raises(OrcapodDatabaseReadError, match="_delta_log") as info:
        run()

    assert Path(info.value.table) == result_log.parent
    assert snapshot(base) == before


def test_flush_can_load_a_log_published_during_the_retry(
    base: Path, stored: DeltaTableDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A concurrent first commit can publish its log after its data is visible."""
    log = table_dir(base) / "_delta_log"
    unpublished = base / "unpublished"
    db = DeltaTableDatabase(base_path=base)
    db.add_records(
        PATH, rows([b"c"], pa.array([3], pa.int64())), record_id_column="rid"
    )
    log.rename(unpublished)
    monkeypatch.setattr(
        delta_lake_databases, "sleep", lambda _delay: unpublished.rename(log)
    )

    db.flush()

    result = db.get_all_records(PATH)
    assert result is not None
    assert result.sort_by("x").column("x").to_pylist() == [1, 2, 3]


def test_empty_table_folder_can_be_created(base: Path) -> None:
    """An empty folder still accepts its first commit."""
    table_dir(base).mkdir(parents=True)
    db = DeltaTableDatabase(base_path=base)
    assert db.get_all_records(PATH) is None
    db.add_records(
        PATH,
        rows([b"a"], pa.array([1], pa.int64())),
        record_id_column="rid",
        flush=True,
    )
    result = db.get_all_records(PATH)
    assert result is not None
    assert result.column("x").to_pylist() == [1]


class TestUnreadableTable:
    """A table that exists but cannot be read raises on every read path."""

    @pytest.mark.parametrize("read", READS.values(), ids=list(READS))
    def test_read_raises_store_read_error(self, base, unreadable, read):
        """Each read of the unreadable table raises ``OrcapodDatabaseReadError`` naming it."""
        with pytest.raises(OrcapodDatabaseReadError) as info:
            read(unreadable)
        assert_names_table(info.value, base)

    def test_skip_duplicates_append_raises_store_read_error(self, base, unreadable):
        """The existing-ids read for ``skip_duplicates`` raises, never assumes none."""
        # skip_duplicates reads the stored record ids before buffering the rows.
        with pytest.raises(OrcapodDatabaseReadError) as info:
            unreadable.add_records(
                PATH,
                rows([b"c"], pa.array([3], pa.int64())),
                record_id_column="rid",
                skip_duplicates=True,
            )
        assert_names_table(info.value, base)

    def test_table_still_exists(self, unreadable):
        """The unreadable table is still reported as existing."""
        assert unreadable.table_exists(PATH)


def test_bad_filter_is_not_reported_as_unreadable_table(stored):
    """A bad filter column is the caller's error, not a ``OrcapodDatabaseReadError``."""
    # The table reads fine; the filter names a column it does not have.
    with pytest.raises(pa.ArrowInvalid):
        stored.get_records_with_column_value(PATH, {"no_such_column": 1})


class TestAbsentTable:
    """A table that does not exist still reads as ``None``."""

    @pytest.mark.parametrize("read", READS.values(), ids=list(READS))
    def test_read_returns_none(self, base, read):
        """Each read of a table that was never written returns ``None``."""
        db = DeltaTableDatabase(base_path=base)
        assert read(db) is None
        assert not db.table_exists(PATH)


# ---------------------------------------------------------------------------
# Flush never overwrites a table it could not load
# ---------------------------------------------------------------------------


class TestUnloadableTableIsNeverOverwritten:
    """A table whose log cannot be loaded is never replaced by a flush."""

    def test_unsupported_reader_feature(self, base, stored):
        """An unloadable log makes reads and the flush raise; nothing is written."""
        unsupported_reader_feature(base)
        before = snapshot(table_dir(base))
        db = DeltaTableDatabase(base_path=base)

        with pytest.raises(OrcapodDatabaseReadError):
            db.get_all_records(PATH)
        with pytest.raises(OrcapodDatabaseReadError) as info:
            db.add_records(
                PATH,
                rows([b"c"], pa.array([3], pa.int64())),
                record_id_column="rid",
                flush=True,
            )
        assert_names_table(info.value, base)
        assert snapshot(table_dir(base)) == before

    def test_protocol_error_on_load_is_not_treated_as_absent(
        self, base, stored, monkeypatch
    ):
        """A ``DeltaProtocolError`` on load makes the flush raise, not overwrite."""
        # deltalake 1.0.2 does not raise DeltaProtocolError for a local table we can
        # build here, so the load is replaced by one that raises it for this table.
        # Before ITL-884 the flush treated this error as "no table" and wrote the
        # batch with mode="overwrite", replacing the stored rows.
        real = deltalake.DeltaTable
        target = table_dir(base)

        def failing_load(table_uri, *args, **kwargs):
            """Raise ``DeltaProtocolError`` for the target table only."""
            if Path(str(table_uri)) == target:
                raise DeltaProtocolError("simulated")
            return real(table_uri, *args, **kwargs)

        before = snapshot(target)
        db = DeltaTableDatabase(base_path=base)
        monkeypatch.setattr(deltalake, "DeltaTable", failing_load)
        with pytest.raises(OrcapodDatabaseReadError) as info:
            db.add_records(
                PATH,
                rows([b"c"], pa.array([3], pa.int64())),
                record_id_column="rid",
                flush=True,
            )
        monkeypatch.undo()

        assert isinstance(info.value.__cause__, DeltaProtocolError)
        assert snapshot(target) == before
        stored_rows = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert stored_rows is not None
        assert stored_rows.num_rows == 2


class TestFlushRaisesStoreErrorsThemselves:
    """``flush`` raises database errors as themselves, with the other failures noted."""

    def test_store_read_error_is_raised_with_a_note_per_other_failed_batch(
        self, base, stored, caplog
    ):
        """The ``OrcapodDatabaseReadError`` is raised and the refused batch is noted on
        it, with its discard notice: how many rows of which table were not written and
        are no longer queued, in the raised error and in the log (ITL-1334)."""
        other = ("results", "u")
        db = DeltaTableDatabase(base_path=base)
        # Queued first, while nothing is stored at ``other``; then another writer
        # stores x as int64 there, so the flush refuses these rows (ITL-1334).
        db.add_records(
            other, rows([b"b", b"d"], pa.array([2.0, 4.0])), record_id_column="rid"
        )
        stored.add_records(
            other,
            rows([b"a"], pa.array([1], pa.int64())),
            record_id_column="rid",
            flush=True,
        )
        unsupported_reader_feature(base)
        # Queued second: its table's log cannot be loaded.
        db.add_records(
            PATH, rows([b"c"], pa.array([3], pa.int64())), record_id_column="rid"
        )

        with caplog.at_level("ERROR"), pytest.raises(OrcapodDatabaseReadError) as info:
            db.flush()

        # The OrcapodDatabaseReadError wins over the earlier refusal, because it is the one
        # that fails a run whatever the error policy; the refusal is noted on it.
        assert_names_table(info.value, base)
        notes = getattr(info.value, "__notes__", [])
        assert len(notes) == 1
        assert "results/u" in notes[0]
        assert "InputValidationError" in notes[0]
        discarded = (
            "The 2 rows queued for table 'results/u' were not written and are no "
            "longer queued"
        )
        assert discarded in notes[0]
        assert any(discarded in record.getMessage() for record in caplog.records)
        # The batch the stored table refuses leaves the queue (every later flush
        # would refuse it too); the one whose table cannot be read stays queued, so
        # the next flush fails the same way (ITL-1334).
        assert db._pending_batches.keys() == {"results/t"}  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(OrcapodDatabaseReadError):
            db.flush()

    def test_other_flush_failures_still_raise_runtime_error(self, base, monkeypatch):
        """A flush that fails for another reason still raises ``RuntimeError``."""

        def failing_write(*args, **kwargs):
            """Raise ``OSError`` for every write."""
            raise OSError("simulated disk failure")

        db = DeltaTableDatabase(base_path=base)
        db.add_records(
            PATH, rows([b"a"], pa.array([1], pa.int64())), record_id_column="rid"
        )
        monkeypatch.setattr(deltalake, "write_deltalake", failing_write)

        with pytest.raises(RuntimeError, match="Failed to flush 1 batch") as info:
            db.flush()
        assert isinstance(info.value.__cause__, OSError)


# ---------------------------------------------------------------------------
# Append refusal: a column's type differs from the stored table
# ---------------------------------------------------------------------------


def append(db: DeltaTableDatabase, table: pa.Table) -> None:
    """Add ``table`` at ``PATH`` and flush it."""
    db.add_records(PATH, table, record_id_column="rid", flush=True)


class TestChangedColumnTypeIsRefused:
    """An append whose column type differs from the stored table is refused."""

    @pytest.mark.parametrize(
        ("new_type", "value", "named"),
        [
            (pa.float64(), 2.0, "double"),
            (pa.int32(), 2, "int32"),
            (pa.large_string(), "2", "string"),
            (pa.timestamp("ns"), datetime.datetime(2020, 1, 2), "timestamp"),
        ],
        ids=[
            "int64->float64",
            "int64->int32",
            "int64->large_string",
            "int64->timestamp[ns]",
        ],
    )
    def test_plain_type_change(self, base, stored, new_type, value, named):
        """A changed column type is refused, naming the column and both types."""
        with pytest.raises(InputValidationError) as info:
            append(stored, rows([b"c"], pa.array([value], new_type)))
        message = str(info.value)
        assert "'x'" in message
        assert "int64" in message
        assert named in message
        assert str(table_dir(base)) in message

        back = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert back is not None
        assert back.num_rows == 2
        assert back.schema.field("x").type == pa.int64()

    def test_extension_storage_change(self, base):
        """A changed extension storage type is refused and the table stays readable."""
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], ext_array([b"x"], pa.large_binary())))

        with pytest.raises(InputValidationError) as info:
            append(
                db,
                rows([b"b"], ext_array([[1.0]], pa.large_list(pa.float32()))),
            )
        message = str(info.value)
        assert "'x'" in message
        assert "binary" in message
        assert "list" in message

        back = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert back is not None
        assert back.num_rows == 1

    def test_nested_field_type_change(self, base: Path) -> None:
        """A changed type of an existing struct field is refused."""
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], pa.array([{"a": 1}], pa.struct([("a", pa.int64())]))))

        with pytest.raises(InputValidationError) as info:
            append(
                db,
                rows([b"b"], pa.array([{"a": 2.0}], pa.struct([("a", pa.float64())]))),
            )
        message = str(info.value)
        assert "'x'" in message
        assert "struct<a: int64>" in message
        assert "struct<a: double>" in message

        back = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert back is not None
        assert back.num_rows == 1
        assert back.schema.field("x").type == pa.struct([("a", pa.int64())])


class TestLegitimateAppendsStillPass:
    """Appends whose types match the stored table, as Delta stores them, succeed."""

    def read_back(self, base: Path) -> pa.Table:
        """Read every row at ``PATH`` with a fresh database view."""
        back = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert back is not None
        return back

    def test_identical_schema(self, base, stored):
        """An append with the stored schema succeeds."""
        append(stored, rows([b"c"], pa.array([3], pa.int64())))
        assert self.read_back(base).num_rows == 3

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            (pa.array(["a"], pa.string()), pa.array(["b"], pa.large_string())),
            (pa.array(["a"], pa.large_string()), pa.array(["b"], pa.string())),
            (
                pa.array([[1]], pa.list_(pa.int64())),
                pa.array([[2]], pa.large_list(pa.int64())),
            ),
            (pa.array([b"a"], pa.binary()), pa.array([b"b"], pa.large_binary())),
        ],
        ids=["string->large", "large->string", "list->large", "binary->large"],
    )
    def test_large_and_non_large_layouts(self, base, first, second):
        """Large and non-large layouts of one type count as the same type."""
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], first))
        append(db, rows([b"b"], second))
        assert self.read_back(base).num_rows == 2

    def test_new_column(self, base, stored):
        """An append that adds a column succeeds."""
        append(
            stored,
            rows([b"c"], pa.array([3], pa.int64()), y=pa.array(["new"])),
        )
        back = self.read_back(base)
        assert back.num_rows == 3
        assert "y" in back.column_names

    def test_new_struct_field(self, base: Path) -> None:
        """An append that adds a field to a stored struct column succeeds."""
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], pa.array([{"a": 1}], pa.struct([("a", pa.int64())]))))
        evolved = pa.struct([("a", pa.int64()), ("b", pa.int64())])
        append(db, rows([b"b"], pa.array([{"a": 2, "b": 3}], evolved)))

        back = DeltaTableDatabase(base_path=base).get_all_records(
            PATH, record_id_column="rid"
        )
        assert back is not None
        back = back.sort_by("rid")
        assert back.schema.field("x").type == evolved
        assert back.column("x").to_pylist() == [
            {"a": 1, "b": None},
            {"a": 2, "b": 3},
        ]

    def test_new_struct_field_inside_a_list_is_refused(self, base: Path) -> None:
        """An append that adds a field to a struct inside a list column would change
        the list column's recorded type: Delta refuses the commit, the flush raises
        once and the batch leaves the queue, the stored rows unchanged (ITL-1403)."""
        db = DeltaTableDatabase(base_path=base)
        stored_type = pa.large_list(pa.struct([("a", pa.int64())]))
        append(db, rows([b"a"], pa.array([[{"a": 1}]], stored_type)))
        evolved = pa.large_list(pa.struct([("a", pa.int64()), ("b", pa.int64())]))
        with pytest.raises(InputValidationError, match="Delta refused the commit"):
            append(db, rows([b"b"], pa.array([[{"a": 2, "b": 3}]], evolved)))
        assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]

        back = DeltaTableDatabase(base_path=base).get_all_records(
            PATH, record_id_column="rid"
        )
        assert back is not None
        assert back.schema.field("x").type == stored_type
        assert back.column("x").to_pylist() == [[{"a": 1}]]

    def test_missing_column(self, base):
        """An append that omits a stored column succeeds."""
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], pa.array([1], pa.int64()), y=pa.array(["old"])))
        append(db, rows([b"b"], pa.array([2], pa.int64())))
        assert self.read_back(base).num_rows == 2

    def test_extension_type_with_unchanged_storage(self, base):
        """An extension type with unchanged storage appends."""
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], ext_array([b"x"], pa.large_binary())))
        append(db, rows([b"b"], ext_array([b"y"], pa.large_binary())))
        assert self.read_back(base).num_rows == 2

    def test_nullability_only_difference_is_accepted(self, base):
        """A nullability-only difference is accepted (pinned, ITL-842)."""
        # Pins today's behaviour: the append check ignores nullability (ITL-884,
        # decision D3). Whether a nullability change should be refused belongs to
        # the nullability audit, ITL-842. The read-back assertion records what
        # Delta does on merge: the stored non-null child becomes nullable, so the
        # rows written first now read back with a different (looser) type.
        strict = pa.struct([pa.field("a", pa.int64(), nullable=False)])
        loose = pa.struct([pa.field("a", pa.int64(), nullable=True)])
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], pa.array([{"a": 1}], strict)))
        append(db, rows([b"b"], pa.array([{"a": 2}], loose)))

        back = self.read_back(base)
        assert back.num_rows == 2
        assert back.schema.field("x").type == loose


# ---------------------------------------------------------------------------
# A readable table whose class differs is not an unreadable table (ITL-881)
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _StoredShape:
    """A registered user class; the table below stores another shape under its name."""

    x: int
    y: int


@pytest.fixture
def other_shape(base: Path) -> DeltaTableDatabase:
    """A readable table storing ``_StoredShape``'s type name over another struct."""
    value: Any = _StoredShape(1, 2)
    source = DictSource(
        data=[{"k": "a", "p": value}], tag_columns=["k"], tag_only_columns=True
    )
    ext = source.as_table().schema.field("p").type
    wrong = pa.struct([pa.field("x", pa.float64()), pa.field("y", pa.int64())])
    field = pa.field("x", wrong).with_metadata(
        {
            b"ARROW:extension:name": ext.extension_name.encode(),
            b"ARROW:extension:metadata": ext.__arrow_ext_serialize__(),
        }
    )
    table = pa.table(
        {
            "__record_id": pa.array([b"a"], pa.large_binary()),
            "x": pa.array([{"x": 1.5, "y": 2}], type=wrong),
        },
        schema=pa.schema([pa.field("__record_id", pa.large_binary()), field]),
    )
    deltalake.write_deltalake(str(table_dir(base)), table)
    return DeltaTableDatabase(base_path=base)


READS_WITHOUT_VALUE_FILTER = {
    name: read
    for name, read in READS.items()
    if name != "get_records_with_column_value"
}


@pytest.mark.parametrize(
    "read",
    READS_WITHOUT_VALUE_FILTER.values(),
    ids=list(READS_WITHOUT_VALUE_FILTER),
)
def test_class_mismatch_is_type_identity_error_not_store_read_error(other_shape, read):
    """A stored shape that differs from the registered class raises ``TypeIdentityError``.

    ``OrcapodDatabaseReadError`` means the table cannot be read. Here the table is intact and the
    class differs, so the identity error is raised as is, never wrapped.
    """
    with pytest.raises(TypeIdentityError) as info:
        read(other_shape)
    assert not isinstance(info.value, OrcapodDatabaseReadError)
