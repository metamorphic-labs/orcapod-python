"""Integration tests for SQLiteConnector with ConnectorArrowDatabase."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.databases import ConnectorArrowDatabase, SQLiteConnector
from orcapod.databases._sql_store import STORE_PATH_CATALOG
from orcapod.errors import OrcapodDatabaseError
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
from orcapod.types import ColumnInfo


@pytest.fixture
def db() -> Iterator[ConnectorArrowDatabase]:
    connector = SQLiteConnector(":memory:")
    database = ConnectorArrowDatabase(connector)
    yield database
    connector.close()


class TestConnectorArrowDatabaseWithSQLite:
    def test_long_paths_with_the_same_sanitized_name_store_separate_rows(
        self, db: ConnectorArrowDatabase
    ) -> None:
        common = "shared_prefix_" * 12
        first = ("job", common + ":a", "pdb_v2")
        second = ("job", common + "-a", "pdb_v2")
        db.add_record(first, b"one", pa.table({"value": [1]}), flush=True)
        db.add_record(second, b"one", pa.table({"value": [2]}), flush=True)

        first_rows = db.get_all_records(first)
        second_rows = db.get_all_records(second)
        assert first_rows is not None and first_rows.to_pylist() == [{"value": 1}]
        assert second_rows is not None and second_rows.to_pylist() == [{"value": 2}]

    def test_paths_with_a_forced_digest_collision_cannot_share_a_table(
        self, db: ConnectorArrowDatabase, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from orcapod.databases import store_table_names

        monkeypatch.setattr(
            store_table_names, "store_table_digest", lambda path: "a" * 64
        )
        first = ("name:part",)
        second = ("name-part",)
        db.add_record(first, b"one", pa.table({"value": [1]}))
        with pytest.raises(OrcapodDatabaseError, match="already belongs"):
            db.add_record(second, b"one", pa.table({"value": [2]}))
        db.flush_batch(first)
        with pytest.raises(OrcapodDatabaseError, match="already belongs"):
            db.get_all_records(second)
        with pytest.raises(OrcapodDatabaseError, match="already belongs"):
            db.add_record(second, b"one", pa.table({"value": [2]}))
        assert db.list_table_paths() == [first]
        records = db.get_all_records(first)
        assert records is not None and records.to_pylist() == [{"value": 1}]

    def test_pending_path_is_refused_if_another_writer_claims_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from orcapod.databases import store_table_names

        monkeypatch.setattr(
            store_table_names, "store_table_digest", lambda path: "a" * 64
        )
        location = tmp_path / "store.db"
        with SQLiteConnector(location) as first, SQLiteConnector(location) as second:
            _check_pending_collision(first, second)

    @pytest.mark.parametrize(
        "operation", ["list", "read", "pending_point", "queue", "flush"]
    )
    def test_unsupported_catalog_layout_is_refused(self, operation: str) -> None:
        path = ("versioned",)
        one, two = pa.table({"value": [1]}), pa.table({"value": [2]})
        with SQLiteConnector() as connector:
            db = ConnectorArrowDatabase(connector)
            db.add_record(path, b"id", one, flush=True)
            if operation in ("flush", "pending_point"):
                db.add_record(path, b"id", two)
            conn = connector._require_open()
            _ = conn.execute("PRAGMA ignore_check_constraints=ON")
            _ = conn.execute(
                f'UPDATE main."{STORE_PATH_CATALOG}" SET layout_version = 2'
            )
            with pytest.raises(
                OrcapodDatabaseError, match="unsupported catalog layout version 2"
            ):
                _check_catalog_operation(db, path, two, operation)
            name = next(n for n in connector.get_table_names() if n.startswith("o1_"))
            assert tuple(
                conn.execute(f'SELECT value FROM main."{name}"').fetchone()
            ) == (1,)

    @pytest.mark.parametrize("write", [False, True])
    def test_store_uses_main_when_a_temp_table_shadows_its_name(
        self, write: bool
    ) -> None:
        path = ("shadow",)
        one, two = pa.table({"value": [1]}), pa.table({"value": [2]})
        with SQLiteConnector() as connector:
            store = ConnectorArrowDatabase(connector)
            store.add_record(path, b"id", one, flush=True)
            name = next(n for n in connector.get_table_names() if n.startswith("o1_"))
            conn = connector._require_open()
            _ = conn.execute(
                f'CREATE TEMP TABLE "{name}" ("__record_id" BLOB PRIMARY KEY, value INTEGER)'
            )
            _ = conn.execute(f'INSERT INTO temp."{name}" VALUES (?, ?)', (b"id", 99))
            if write:
                store.add_record(path, b"id", two, flush=True)
                assert tuple(
                    conn.execute(f'SELECT value FROM main."{name}"').fetchone()
                ) == (2,)
                assert tuple(
                    conn.execute(f'SELECT value FROM temp."{name}"').fetchone()
                ) == (99,)
            else:
                records = store.get_all_records(path)
                assert records is not None and records.equals(one)

    def test_legacy_table_is_ignored_and_preserved(self) -> None:
        with SQLiteConnector(":memory:") as connector:
            columns = [
                ColumnInfo("__record_id", pa.large_binary(), False),
                ColumnInfo("value", pa.int64(), False),
            ]
            connector.create_table_if_not_exists("job__pdb_v2", columns, "__record_id")
            connector.upsert_records(
                "job__pdb_v2",
                pa.table({"__record_id": [b"one"], "value": [1]}),
                "__record_id",
            )
            db = ConnectorArrowDatabase(connector)
            path = ("job", "pdb_v2")
            assert not db.table_exists(path)
            assert db.get_table_schema(path) is None
            assert db.get_all_records(path) is None
            assert db.list_table_paths(("job",)) == []

            db.add_record(path, b"one", pa.table({"value": [2]}), flush=True)
            assert db.list_table_paths(("job",)) == [path]
            old = pa.Table.from_batches(
                list(connector.iter_batches('SELECT "value" FROM "job__pdb_v2"'))
            )
            assert old.to_pylist() == [{"value": 1}]

    def test_unregistered_physical_name_is_not_claimed(self) -> None:
        from orcapod.databases.store_table_names import store_table_name

        with SQLiteConnector(":memory:") as connector:
            path = ("unregistered",)
            name = store_table_name(path, hint_length=27, digest_length=32)
            connector.create_table_if_not_exists(
                name,
                [ColumnInfo("__record_id", pa.large_binary(), False)],
                "__record_id",
            )
            db = ConnectorArrowDatabase(connector)
            assert not db.table_exists(path)
            assert db.list_table_paths() == []
            with pytest.raises(OrcapodDatabaseError, match="no stored path"):
                db.add_record(path, b"one", pa.table({"value": [1]}), flush=True)
            assert connector.get_table_names() == [name]

    def test_registration_and_table_creation_roll_back_on_write_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with SQLiteConnector(":memory:") as connector:
            db = ConnectorArrowDatabase(connector)

            def fail_write(*args: object, **kwargs: object) -> None:
                raise RuntimeError("simulated write failure")

            with monkeypatch.context() as patch:
                patch.setattr(connector, "upsert_records", fail_write)
                with pytest.raises(RuntimeError, match="simulated write failure"):
                    db.add_record(
                        ("new",), b"one", pa.table({"value": [1]}), flush=True
                    )
            assert connector.get_table_names() == []
            db.add_record(("new",), b"one", pa.table({"value": [1]}), flush=True)
            assert db.list_table_paths() == [("new",)]

    def test_unknown_catalog_entries_are_ignored(self, tmp_path: Path) -> None:
        location = tmp_path / "store.db"
        path = ("job", "pdb_v2")
        with SQLiteConnector(location) as connector:
            ConnectorArrowDatabase(connector).add_record(
                path, b"one", pa.table({"value": [1]}), flush=True
            )
        with sqlite3.connect(location) as conn:
            row = conn.execute(
                f'SELECT layout_version, table_name FROM "{STORE_PATH_CATALOG}"'
            ).fetchone()
            assert row is not None
            version, name = row
            assert version == 1
            _ = conn.execute(
                f'INSERT INTO "{STORE_PATH_CATALOG}" '
                "(layout_version, table_name, entry_kind, entry_name, entry_value) "
                "VALUES (?, ?, 'future', 'short_name', 'an opaque value')",
                (version, name),
            )
        with SQLiteConnector(location) as connector:
            db = ConnectorArrowDatabase(connector)
            assert db.list_table_paths(("job",)) == [path]
            rows = db.get_all_records(path)
            assert rows is not None and rows.to_pylist() == [{"value": 1}]
            db.add_record(path, b"two", pa.table({"value": [2]}), flush=True)

    def test_add_and_get_record(self, db: ConnectorArrowDatabase) -> None:
        # add_record stamps all rows with the same __record_id, so within-batch
        # deduplication keeps only the last row (same behaviour as InMemoryArrowDatabase).
        record = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=record, flush=True)
        result = db.get_record_by_id(("fn", "test"), "r1")
        assert result is not None
        assert result.num_rows == 1
        assert result.column("x")[0].as_py() == 3  # last row kept

    def test_add_multiple_records(self, db: ConnectorArrowDatabase) -> None:
        r1 = pa.table({"x": pa.array([1], type=pa.int64())})
        r2 = pa.table({"x": pa.array([2], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=r1)
        db.add_record(("fn", "test"), record_id="r2", record=r2)
        db.flush()
        all_records = db.get_all_records(("fn", "test"))
        assert all_records is not None
        assert all_records.num_rows == 2

    def test_skip_duplicates(self, db: ConnectorArrowDatabase) -> None:
        record_v1 = pa.table({"x": pa.array([1], type=pa.int64())})
        record_v2 = pa.table({"x": pa.array([99], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=record_v1, flush=True)

        # skip_duplicates=True: must not raise, original value must be preserved
        db.add_record(
            ("fn", "test"),
            record_id="r1",
            record=record_v2,
            skip_duplicates=True,
            flush=True,
        )
        result = db.get_record_by_id(("fn", "test"), "r1")
        assert result is not None
        assert result.column("x")[0].as_py() == 1  # original preserved, not overwritten

        # skip_duplicates=False (default): must raise ValueError on duplicate pending batch
        connector2 = SQLiteConnector(":memory:")
        db2 = ConnectorArrowDatabase(connector2)
        db2.add_record(("fn", "test"), record_id="r1", record=record_v1)
        with pytest.raises(ValueError):
            db2.add_record(("fn", "test"), record_id="r1", record=record_v2)
        connector2.close()

    def test_second_flush_schema_consistency(self, db: ConnectorArrowDatabase) -> None:
        """ConnectorArrowDatabase validates schema on second flush — must match exactly."""
        r1 = pa.table({"x": pa.array([1], type=pa.int64())})
        r2 = pa.table({"x": pa.array([2], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=r1, flush=True)
        db.add_record(
            ("fn", "test"), record_id="r2", record=r2, flush=True
        )  # second flush
        all_records = db.get_all_records(("fn", "test"))
        assert all_records is not None
        assert all_records.num_rows == 2

    def test_bool_column_roundtrip(self, db: ConnectorArrowDatabase) -> None:
        """BOOLEAN columns must survive write-flush-read without becoming int.

        add_record deduplicates all rows to the last one, so the 3-row table
        collapses to a single row with flag=True (the last value).
        """
        record = pa.table(
            {
                "flag": pa.array([False, False, True], type=pa.bool_()),
            }
        )
        db.add_record(("fn", "test"), record_id="r1", record=record, flush=True)
        result = db.get_record_by_id(("fn", "test"), "r1")
        assert result is not None
        flag_col = result.column("flag")
        assert flag_col.type == pa.bool_()
        assert result.num_rows == 1
        assert flag_col[0].as_py() is True  # last row kept

    def test_second_flush_schema_mismatch_raises(
        self, db: ConnectorArrowDatabase
    ) -> None:
        """ConnectorArrowDatabase.flush() raises ValueError on schema mismatch."""
        r1 = pa.table({"x": pa.array([1], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=r1, flush=True)

        # Incompatible schema: different column type for 'x'
        r2 = pa.table({"x": pa.array([2.0], type=pa.float64())})
        db.add_record(("fn", "test"), record_id="r2", record=r2)
        with pytest.raises(ValueError, match="Schema mismatch"):
            db.flush()

    def test_get_records_by_ids(self, db: ConnectorArrowDatabase) -> None:
        for i in range(5):
            r = pa.table({"x": pa.array([i], type=pa.int64())})
            db.add_record(("fn", "test"), record_id=f"r{i}", record=r)
        db.flush()
        result = db.get_records_by_ids(("fn", "test"), ["r1", "r3"])
        assert result is not None
        assert result.num_rows == 2


def _check_pending_collision(
    first: DBConnectorProtocol, second: DBConnectorProtocol
) -> None:
    queued = ConnectorArrowDatabase(first)
    winner = ConnectorArrowDatabase(second)
    pending, committed = ("name:part",), ("name-part",)
    one, two = pa.table({"value": [1]}), pa.table({"value": [2]})
    queued.add_record(pending, b"id", two)
    winner.add_record(committed, b"id", one, flush=True)
    for check in (
        queued.list_table_paths,
        lambda: queued.get_record_by_id(pending, b"id"),
        lambda: queued.flush_batch(pending),
    ):
        with pytest.raises(OrcapodDatabaseError, match="name-part.*name:part"):
            check()
    rows = winner.get_all_records(committed)
    assert rows is not None and rows.equals(one)


def _check_catalog_operation(
    db: ConnectorArrowDatabase, path: tuple[str, ...], table: pa.Table, operation: str
) -> None:
    if operation == "list":
        db.list_table_paths()
    elif operation == "read":
        db.get_all_records(path)
    elif operation == "pending_point":
        db.get_record_by_id(path, b"id")
    elif operation == "queue":
        db.add_record(path, b"id", table)
    else:
        db.flush_batch(path)
