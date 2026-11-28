# tests/test_databases/test_postgresql_connector_integration.py
"""Integration tests for PostgreSQLConnector — requires a live PostgreSQL instance.

Run with: pytest -m postgres tests/test_databases/test_postgresql_connector_integration.py

Connects to a running PostgreSQL instance via standard PG* environment variables:

    PGHOST      (default: localhost)
    PGPORT      (default: 5432)
    PGDATABASE  (default: testdb)
    PGUSER      (default: postgres)
    PGPASSWORD  (default: postgres)

In CI, a PostgreSQL service container is started by the GitHub Actions workflow
and these variables are set automatically. Locally, point them at any running
PostgreSQL instance (e.g. `docker run -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16`).

Each test gets an isolated schema (created fresh, dropped after), so tests can
share a single database without interfering with each other.
"""

from __future__ import annotations

import datetime
import os
import uuid
from collections.abc import Iterator
from typing import cast

import psycopg
import pyarrow as pa
import pytest
from psycopg import sql

from orcapod.databases import ConnectorArrowDatabase, PostgreSQLConnector
from orcapod.databases._sql_store import STORE_PATH_CATALOG
from orcapod.errors import OrcapodDatabaseError
from orcapod.types import ColumnInfo
from tests.test_databases.test_sqlite_connector_integration import (
    _check_catalog_operation,
    _check_pending_collision,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _base_dsn() -> str:
    """Build a DSN from standard PG* environment variables."""
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    dbname = os.environ.get("PGDATABASE", "testdb")
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "postgres")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


@pytest.fixture
def connector() -> Iterator[PostgreSQLConnector]:
    """Function-scoped connector with per-test schema isolation.

    Creates a fresh schema for each test and drops it on teardown, so every
    test starts with an empty namespace without needing a dedicated database.
    """
    base_dsn = _base_dsn()
    schema = f"test_{uuid.uuid4().hex[:12]}"
    dsn = f"{base_dsn} options=-csearch_path={schema}"

    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")

    c = PostgreSQLConnector(dsn)
    yield c
    c.close()

    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _make_columns() -> list[ColumnInfo]:
    return [
        ColumnInfo("__record_id", pa.large_string(), nullable=False),
        ColumnInfo("value", pa.float64(), nullable=True),
    ]


def _make_table(ids: list[str], values: list[float | None]) -> pa.Table:
    return pa.table(
        {
            "__record_id": pa.array(ids, type=pa.large_string()),
            "value": pa.array(values, type=pa.float64()),
        }
    )


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestLifecycle:
    def test_open_and_close(self, connector: PostgreSQLConnector) -> None:
        connector._require_open()  # must not raise
        connector.close()
        with pytest.raises(RuntimeError, match="closed"):
            connector._require_open()

    def test_close_is_idempotent(self, connector: PostgreSQLConnector) -> None:
        connector.close()
        connector.close()  # must not raise

    def test_context_manager(self) -> None:
        dsn = _base_dsn()
        with PostgreSQLConnector(dsn) as c:
            c._require_open()
        with pytest.raises(RuntimeError, match="closed"):
            c._require_open()


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestGetTableNames:
    def test_empty_database(self, connector: PostgreSQLConnector) -> None:
        assert connector.get_table_names() == []

    def test_returns_table_names(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "foo" (id INTEGER PRIMARY KEY)')
            cur.execute('CREATE TABLE "bar" (id INTEGER PRIMARY KEY)')
        connector._conn.commit()
        assert connector.get_table_names() == ["bar", "foo"]

    def test_excludes_views(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "base_tbl" (id INTEGER PRIMARY KEY)')
            cur.execute('CREATE VIEW "v_base" AS SELECT * FROM "base_tbl"')
        connector._conn.commit()
        assert "v_base" not in connector.get_table_names()
        assert "base_tbl" in connector.get_table_names()


@pytest.mark.postgres
class TestGetPkColumns:
    def test_single_pk(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, val REAL)')
        connector._conn.commit()
        assert connector.get_pk_columns("t") == ["id"]

    def test_composite_pk_order_preserved(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute(
                'CREATE TABLE "t" (a TEXT, b INTEGER, c REAL, PRIMARY KEY (a, b))'
            )
        connector._conn.commit()
        assert connector.get_pk_columns("t") == ["a", "b"]

    def test_no_pk_returns_empty(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (val TEXT)')
        connector._conn.commit()
        assert connector.get_pk_columns("t") == []

    def test_nonexistent_table_returns_empty(
        self, connector: PostgreSQLConnector
    ) -> None:
        assert connector.get_pk_columns("no_such_table") == []

    def test_raises_on_invalid_table_name(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.get_pk_columns('table"name')


@pytest.mark.postgres
class TestGetColumnInfo:
    def test_all_pg_types(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE "t" (
                    a BOOLEAN,
                    b SMALLINT,
                    c INTEGER,
                    d BIGINT,
                    e REAL,
                    f DOUBLE PRECISION,
                    g TEXT,
                    h BYTEA,
                    i UUID,
                    j JSONB,
                    k DATE,
                    l TIMESTAMP,
                    m TIMESTAMPTZ
                )
            """)
        connector._conn.commit()
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["a"].arrow_type == pa.bool_()
        assert infos["b"].arrow_type == pa.int32()
        assert infos["c"].arrow_type == pa.int32()
        assert infos["d"].arrow_type == pa.int64()
        assert infos["e"].arrow_type == pa.float32()
        assert infos["f"].arrow_type == pa.float64()
        assert infos["g"].arrow_type == pa.large_string()
        assert infos["h"].arrow_type == pa.large_binary()
        assert infos["i"].arrow_type == pa.binary(16)
        assert infos["j"].arrow_type == pa.large_string()
        assert infos["k"].arrow_type == pa.date32()
        assert infos["l"].arrow_type == pa.timestamp("us")
        assert infos["m"].arrow_type == pa.timestamp("us", tz="UTC")

    def test_nullable_flag(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (a TEXT NOT NULL, b TEXT)')
        connector._conn.commit()
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["a"].nullable is False
        assert infos["b"].nullable is True

    def test_nonexistent_table_returns_empty(
        self, connector: PostgreSQLConnector
    ) -> None:
        assert connector.get_column_info("no_such_table") == []

    def test_raises_on_invalid_table_name(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.get_column_info('bad"name')

    def test_array_column(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (ids INTEGER[])')
        connector._conn.commit()
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["ids"].arrow_type == pa.large_list(pa.int32())


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestCreateTableIfNotExists:
    def test_creates_table(self, connector: PostgreSQLConnector) -> None:
        connector.create_table_if_not_exists("tbl", _make_columns(), "__record_id")
        assert "tbl" in connector.get_table_names()

    def test_idempotent(self, connector: PostgreSQLConnector) -> None:
        cols = _make_columns()
        connector.create_table_if_not_exists("tbl", cols, "__record_id")
        connector.create_table_if_not_exists(
            "tbl", cols, "__record_id"
        )  # must not raise

    def test_pk_column_set(self, connector: PostgreSQLConnector) -> None:
        connector.create_table_if_not_exists("tbl", _make_columns(), "__record_id")
        assert connector.get_pk_columns("tbl") == ["__record_id"]

    def test_not_null_respected(self, connector: PostgreSQLConnector) -> None:
        connector.create_table_if_not_exists("tbl", _make_columns(), "__record_id")
        infos = {ci.name: ci for ci in connector.get_column_info("tbl")}
        assert infos["__record_id"].nullable is False
        assert infos["value"].nullable is True

    def test_raises_when_pk_column_not_in_columns(
        self, connector: PostgreSQLConnector
    ) -> None:
        with pytest.raises(ValueError, match="not found in columns"):
            connector.create_table_if_not_exists("tbl", _make_columns(), "missing_pk")

    def test_raises_on_invalid_table_name(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.create_table_if_not_exists(
                'bad"name', _make_columns(), "__record_id"
            )


@pytest.mark.postgres
class TestUpsertRecords:
    def setup_table(self, connector: PostgreSQLConnector) -> None:
        connector.create_table_if_not_exists("tbl", _make_columns(), "__record_id")

    def test_insert_new_records(self, connector: PostgreSQLConnector) -> None:
        self.setup_table(connector)
        connector.upsert_records(
            "tbl", _make_table(["a", "b"], [1.0, 2.0]), "__record_id"
        )
        batches = list(connector.iter_batches('SELECT * FROM "tbl"'))
        assert sum(b.num_rows for b in batches) == 2

    def test_replace_existing(self, connector: PostgreSQLConnector) -> None:
        self.setup_table(connector)
        connector.upsert_records("tbl", _make_table(["a"], [1.0]), "__record_id")
        connector.upsert_records(
            "tbl", _make_table(["a"], [99.0]), "__record_id", skip_existing=False
        )
        batches = list(
            connector.iter_batches('SELECT * FROM "tbl" WHERE "__record_id" = \'a\'')
        )
        table = pa.Table.from_batches(batches)
        assert table.column("value")[0].as_py() == 99.0

    def test_skip_existing_keeps_original(self, connector: PostgreSQLConnector) -> None:
        self.setup_table(connector)
        connector.upsert_records("tbl", _make_table(["a"], [1.0]), "__record_id")
        connector.upsert_records(
            "tbl", _make_table(["a"], [99.0]), "__record_id", skip_existing=True
        )
        batches = list(
            connector.iter_batches('SELECT * FROM "tbl" WHERE "__record_id" = \'a\'')
        )
        table = pa.Table.from_batches(batches)
        assert table.column("value")[0].as_py() == 1.0

    def test_raises_on_invalid_table_name(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.upsert_records(
                'bad"name', _make_table(["a"], [1.0]), "__record_id"
            )


# ---------------------------------------------------------------------------
# iter_batches
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestIterBatches:
    def setup_data(self, connector: PostgreSQLConnector) -> None:
        cols = [
            ColumnInfo("id", pa.large_string(), nullable=False),
            ColumnInfo("score", pa.float64(), nullable=True),
            ColumnInfo("count", pa.int64(), nullable=True),
            ColumnInfo("active", pa.bool_(), nullable=True),
        ]
        connector.create_table_if_not_exists("data", cols, "id")
        records = pa.table(
            {
                "id": pa.array(["a", "b", "c"], type=pa.large_string()),
                "score": pa.array([1.5, 2.5, 3.5], type=pa.float64()),
                "count": pa.array([10, 20, 30], type=pa.int64()),
                "active": pa.array([True, False, True], type=pa.bool_()),
            }
        )
        connector.upsert_records("data", records, "id")

    def test_returns_all_rows(self, connector: PostgreSQLConnector) -> None:
        self.setup_data(connector)
        batches = list(connector.iter_batches('SELECT * FROM "data"'))
        assert sum(b.num_rows for b in batches) == 3

    def test_correct_types_roundtrip(self, connector: PostgreSQLConnector) -> None:
        self.setup_data(connector)
        table = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "data"'))
        schema = {f.name: f.type for f in table.schema}
        assert schema["score"] == pa.float64()
        assert schema["count"] == pa.int64()
        assert schema["active"] == pa.bool_()

    def test_batch_size(self, connector: PostgreSQLConnector) -> None:
        self.setup_data(connector)
        batches = list(connector.iter_batches('SELECT * FROM "data"', batch_size=2))
        assert len(batches) == 2
        assert batches[0].num_rows == 2
        assert batches[1].num_rows == 1

    def test_empty_result(self, connector: PostgreSQLConnector) -> None:
        self.setup_data(connector)
        batches = list(connector.iter_batches('SELECT * FROM "data" WHERE 1=0'))
        assert batches == []

    def test_early_termination_closes_cursor(
        self, connector: PostgreSQLConnector
    ) -> None:
        """Abandoning the generator mid-iteration must not leak server-side cursors."""
        self.setup_data(connector)
        gen = connector.iter_batches('SELECT * FROM "data"', batch_size=1)
        next(gen)  # consume one batch
        gen.close()  # abandon — must send CLOSE to PG without raising
        # Subsequent queries must still work
        batches = list(connector.iter_batches('SELECT * FROM "data"'))
        assert sum(b.num_rows for b in batches) == 3


# ---------------------------------------------------------------------------
# Type roundtrip
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestTypeRoundtrip:
    """Write each PG type and read it back; verify Arrow types are correct."""

    def test_bool_roundtrip(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, v BOOLEAN)')
            cur.execute('INSERT INTO "t" VALUES (%s, %s)', ("r1", True))
        connector._conn.commit()
        table = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "t"'))
        assert table.schema.field("v").type == pa.bool_()
        assert table.column("v")[0].as_py() is True

    def test_int_types_roundtrip(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (i2 SMALLINT, i4 INTEGER, i8 BIGINT)')
            cur.execute('INSERT INTO "t" VALUES (%s, %s, %s)', (1, 2, 3))
        connector._conn.commit()
        table = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "t"'))
        assert table.schema.field("i2").type == pa.int32()
        assert table.schema.field("i4").type == pa.int32()
        assert table.schema.field("i8").type == pa.int64()

    def test_timestamptz_roundtrip(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, ts TIMESTAMPTZ)')
            cur.execute("INSERT INTO \"t\" VALUES ('r1', '2024-01-15 12:00:00+00')")
        connector._conn.commit()
        table = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "t"'))
        assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")

    def test_int_array_roundtrip(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, vals INTEGER[])')
            cur.execute("INSERT INTO \"t\" VALUES ('r1', ARRAY[1,2,3])")
        connector._conn.commit()
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["vals"].arrow_type == pa.large_list(pa.int32())


# ---------------------------------------------------------------------------
# ConnectorArrowDatabase integration
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestConnectorArrowDatabaseWithPostgreSQL:
    @pytest.fixture
    def db(self, connector: PostgreSQLConnector) -> ConnectorArrowDatabase:
        return ConnectorArrowDatabase(connector)

    def test_add_and_get_record(self, db: ConnectorArrowDatabase) -> None:
        record = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=record, flush=True)
        result = db.get_record_by_id(("fn", "test"), "r1")
        assert result is not None
        assert result.num_rows == 1
        assert result.column("x")[0].as_py() == 3  # last row kept (dedup)

    def test_add_multiple_records(self, db: ConnectorArrowDatabase) -> None:
        r1 = pa.table({"x": pa.array([1], type=pa.int64())})
        r2 = pa.table({"x": pa.array([2], type=pa.int64())})
        db.add_record(("fn", "multi"), record_id="r1", record=r1)
        db.add_record(("fn", "multi"), record_id="r2", record=r2)
        db.flush()
        result = db.get_all_records(("fn", "multi"))
        assert result is not None
        assert result.num_rows == 2

    def test_skip_duplicates(self, db: ConnectorArrowDatabase) -> None:
        record_v1 = pa.table({"x": pa.array([1], type=pa.int64())})
        record_v2 = pa.table({"x": pa.array([99], type=pa.int64())})
        db.add_record(("fn", "dup"), record_id="r1", record=record_v1, flush=True)
        db.add_record(
            ("fn", "dup"),
            record_id="r1",
            record=record_v2,
            skip_duplicates=True,
            flush=True,
        )
        result = db.get_record_by_id(("fn", "dup"), "r1")
        assert result is not None
        assert result.column("x")[0].as_py() == 1  # original preserved

    def test_schema_mismatch_raises(self, db: ConnectorArrowDatabase) -> None:
        r1 = pa.table({"x": pa.array([1], type=pa.int64())})
        db.add_record(("fn", "mismatch"), record_id="r1", record=r1, flush=True)
        r2 = pa.table({"x": pa.array([2.0], type=pa.float64())})
        db.add_record(("fn", "mismatch"), record_id="r2", record=r2)
        with pytest.raises(ValueError, match="Schema mismatch"):
            db.flush()


# ---------------------------------------------------------------------------
# What the connector states it stores reads back as written (ITL-1162)
# ---------------------------------------------------------------------------


@pytest.mark.postgres
@pytest.mark.parametrize(
    "arrow_type, values",
    [
        pytest.param(pa.bool_(), [True, None], id="bool"),
        pytest.param(pa.int32(), [1, None], id="int32"),
        pytest.param(pa.int64(), [1, None], id="int64"),
        pytest.param(pa.float32(), [1.5, None], id="float32"),
        pytest.param(pa.float64(), [1.5, None], id="float64"),
        pytest.param(pa.large_string(), ["a", None], id="large_string"),
        pytest.param(pa.large_binary(), [b"a", None], id="large_binary"),
        pytest.param(pa.date32(), [datetime.date(2026, 1, 1), None], id="date32"),
        pytest.param(
            pa.timestamp("us"),
            [datetime.datetime(2026, 1, 1, 12), None],
            id="timestamp",
        ),
        pytest.param(
            pa.timestamp("us", tz="UTC"),
            [datetime.datetime(2026, 1, 1, 12, tzinfo=datetime.timezone.utc), None],
            id="timestamp_utc",
        ),
    ],
)
def test_storable_types_read_back_as_written(
    connector: PostgreSQLConnector, arrow_type: pa.DataType, values: list[object]
) -> None:
    """Each type ``unstorable_column_reason`` accepts round-trips, twice (ITL-1162)."""
    assert (
        connector.unstorable_column_reason("v", arrow_type, primary_key=False) is None
    )
    db = ConnectorArrowDatabase(connector)
    table = pa.table({"v": pa.array(values, type=arrow_type)})
    db.add_record(("t",), b"1", table.slice(0, 1), flush=True)
    db.add_record(("t",), b"2", table.slice(1, 1), flush=True)

    stored = db.get_all_records(("t",))
    assert stored is not None
    assert stored.schema.field("v").type == arrow_type
    assert sorted(stored["v"].to_pylist(), key=lambda v: v is None) == values


@pytest.mark.postgres
def test_function_node_job_refused(connector: PostgreSQLConnector) -> None:
    """A function node's result table cannot be stored; the job refuses it (ITL-1162)."""
    from orcapod import PipelineJob, function_pod
    from orcapod.errors import UnstorableColumnError
    from orcapod.sources import DictSource

    @function_pod(output_keys="double")
    def double(x: int) -> int:
        return 2 * x

    job = PipelineJob(name="p", database=ConnectorArrowDatabase(connector))
    source = DictSource(
        data=[{"k": "a", "x": 1}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="src",
    )
    with pytest.raises(UnstorableColumnError) as exc_info:
        with job:
            _ = double.pod(source, label="dbl")
    assert exc_info.value.column == "__pf_exec_executor_info"
    assert connector.get_table_names() == []


@pytest.mark.postgres
def test_long_column_name_refused_and_nothing_written(
    connector: PostgreSQLConnector,
) -> None:
    """A name PostgreSQL would truncate is refused before the table exists (ITL-1162)."""
    from orcapod.errors import UnstorableColumnError

    db = ConnectorArrowDatabase(connector)
    name = "_tag_source_id::" + "0" * 64
    table = pa.table({name: pa.array(["s"], type=pa.large_string())})
    with pytest.raises(UnstorableColumnError):
        db.add_record(("t",), b"1", table)
    db.flush()
    assert connector.get_table_names() == []


@pytest.mark.postgres
def test_operator_cache_refused_at_write_and_nothing_written(
    connector: PostgreSQLConnector,
) -> None:
    """An operator cache's system-tag column names exceed 63 bytes (ITL-1162)."""
    from orcapod import PipelineJob
    from orcapod.core.operators import SelectDataColumns
    from orcapod.errors import UnstorableColumnError
    from orcapod.sources import DictSource
    from orcapod.types import CacheMode

    db = ConnectorArrowDatabase(connector)
    job = PipelineJob(name="p", database=db)
    source = DictSource(
        data=[{"k": "a", "x": 1, "y": 2}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="src",
    )
    with job:
        _ = SelectDataColumns(["x"])(source, label="sel")
    job.nodes["sel"].attach_databases(
        pipeline_database=db.at("p"), cache_mode=CacheMode.LOG
    )
    with pytest.raises(Exception) as exc_info:
        _ = job.run()
    # The system-tag column names are refused before any write.
    chain: list[BaseException] = []
    err: BaseException | None = exc_info.value
    while err is not None:
        chain.append(err)
        err = err.__cause__ or err.__context__
    assert any(isinstance(e, UnstorableColumnError) for e in chain), chain
    assert connector.get_table_names() == []


@pytest.mark.postgres
def test_long_non_ascii_paths_fit_postgresql_table_names(
    connector: PostgreSQLConnector,
) -> None:
    """Keep separate tables even when PostgreSQL would truncate the old names."""
    db = ConnectorArrowDatabase(connector)
    first = ("job", "é" * 70 + "a", "pdb_v2")
    second = ("job", "é" * 70 + "b", "pdb_v2")
    db.add_record(first, b"one", pa.table({"value": [1]}), flush=True)
    db.add_record(second, b"one", pa.table({"value": [2]}), flush=True)

    names = [name for name in connector.get_table_names() if name.startswith("o1_")]
    assert len(names) == 2
    assert all(len(name.encode("utf-8")) == 63 for name in names)
    with PostgreSQLConnector(connector._dsn) as reopened:
        store = ConnectorArrowDatabase(reopened)
        assert store.list_table_paths(("job", first[1])) == [first]
        assert store.at("job").list_table_paths() == [first[1:], second[1:]]
        first_rows = store.get_all_records(first)
        second_rows = store.get_all_records(second)
    assert first_rows is not None and first_rows.to_pylist() == [{"value": 1}]
    assert second_rows is not None and second_rows.to_pylist() == [{"value": 2}]


@pytest.mark.postgres
def test_record_write_failure_rolls_back_the_table_and_registration(
    connector: PostgreSQLConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The catalog, record table and rows are committed together."""
    store = ConnectorArrowDatabase(connector)
    path = ("job", "new")
    record = pa.table({"value": [1]})

    def fail_write(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated write failure")

    with monkeypatch.context() as patch:
        patch.setattr(connector, "upsert_records", fail_write)
        with pytest.raises(RuntimeError, match="simulated write failure"):
            store.add_record(path, b"one", record, flush=True)

    with PostgreSQLConnector(connector._dsn) as reopened:
        assert reopened.get_table_names() == []
        other = ConnectorArrowDatabase(reopened)
        assert other.list_table_paths() == []
        other.add_record(path, b"one", record, flush=True)
        assert other.list_table_paths() == [path]


@pytest.mark.postgres
def test_unknown_catalog_entries_are_ignored(connector: PostgreSQLConnector) -> None:
    path = ("job", "pdb_v2")
    store = ConnectorArrowDatabase(connector)
    store.add_record(path, b"one", pa.table({"value": [1]}), flush=True)
    catalog = sql.Identifier(STORE_PATH_CATALOG)
    with psycopg.connect(connector._dsn) as conn:
        row = conn.execute(
            sql.SQL("SELECT layout_version, table_name FROM {}").format(catalog)
        ).fetchone()
        assert row is not None
        version, name = row
        assert version == 1
        _ = conn.execute(
            sql.SQL("""
                INSERT INTO {} (layout_version, table_name, entry_kind, entry_name, entry_value)
                VALUES (%s, %s, 'future', 'short_name', 'an opaque value')
                """).format(catalog),
            (version, name),
        )
    assert store.list_table_paths(("job",)) == [path]
    rows = store.get_all_records(path)
    assert rows is not None and rows.to_pylist() == [{"value": 1}]
    store.add_record(path, b"two", pa.table({"value": [2]}), flush=True)


@pytest.mark.postgres
def test_catalog_uses_the_first_schema_on_the_search_path(
    connector: PostgreSQLConnector,
) -> None:
    _, _, _, primary = connector.current_location()
    fallback = f'JOIN."%{uuid.uuid4().hex[:12]}'
    base_dsn = _base_dsn()
    path = ("job", "pdb_v2")
    with psycopg.connect(base_dsn, autocommit=True) as admin:
        _ = admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(fallback)))
        try:
            with (
                PostgreSQLConnector(base_dsn) as other,
                PostgreSQLConnector(base_dsn) as first,
            ):
                _ = other._require_open().execute(
                    sql.SQL("SET search_path TO {}").format(sql.Identifier(fallback))
                )
                _ = first._require_open().execute(
                    sql.SQL("SET search_path TO {}, {}").format(
                        sql.Identifier(primary), sql.Identifier(fallback)
                    )
                )
                other_store = ConnectorArrowDatabase(other)
                store = ConnectorArrowDatabase(first)
                other_store.add_record(
                    path, b"one", pa.table({"value": [1]}), flush=True
                )

                assert store.list_table_paths() == []
                assert store.get_all_records(path) is None
                store.add_record(path, b"one", pa.table({"value": [2]}), flush=True)

                assert len(STORE_PATH_CATALOG.encode("utf-8")) <= 63
                assert STORE_PATH_CATALOG in first.get_table_names()
                assert STORE_PATH_CATALOG in other.get_table_names()
                assert (
                    store.list_table_paths() == other_store.list_table_paths() == [path]
                )
                primary_rows = store.get_all_records(path)
                fallback_rows = other_store.get_all_records(path)
                assert primary_rows is not None and primary_rows.to_pylist() == [
                    {"value": 2}
                ]
                assert fallback_rows is not None and fallback_rows.to_pylist() == [
                    {"value": 1}
                ]
                _ = first._require_open().execute(
                    sql.SQL("SET search_path TO {}").format(sql.Identifier(fallback))
                )
                switched_rows = store.get_all_records(path)
                assert switched_rows is not None and switched_rows.equals(fallback_rows)
        finally:
            _ = admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(fallback))
            )


@pytest.mark.postgres
@pytest.mark.parametrize(
    "operation", ["list", "read", "pending_point", "queue", "flush"]
)
def test_unsupported_catalog_layout_is_refused(
    connector: PostgreSQLConnector, operation: str
) -> None:
    db = ConnectorArrowDatabase(connector)
    path = ("versioned",)
    one, two = pa.table({"value": [1]}), pa.table({"value": [2]})
    db.add_record(path, b"id", one, flush=True)
    if operation in ("flush", "pending_point"):
        db.add_record(path, b"id", two)
        connector._require_open().commit()
    catalog = sql.Identifier(STORE_PATH_CATALOG)
    with psycopg.connect(connector._dsn) as conn:
        _ = conn.execute(
            sql.SQL("ALTER TABLE {} DROP CONSTRAINT {}").format(
                catalog, sql.Identifier(f"{STORE_PATH_CATALOG}_layout_version_check")
            )
        )
        _ = conn.execute(sql.SQL("UPDATE {} SET layout_version = 2").format(catalog))
    with pytest.raises(
        OrcapodDatabaseError, match="unsupported catalog layout version 2"
    ):
        _check_catalog_operation(db, path, two, operation)
    name = next(n for n in connector.get_table_names() if n.startswith("o1_"))
    with psycopg.connect(connector._dsn) as conn:
        assert conn.execute(
            sql.SQL("SELECT value FROM {}").format(sql.Identifier(name))
        ).fetchone() == (1,)


@pytest.mark.postgres
def test_store_writes_to_the_verified_schema_when_a_temp_table_shadows_its_name(
    connector: PostgreSQLConnector,
) -> None:
    db = ConnectorArrowDatabase(connector)
    path = ("shadow",)
    db.add_record(path, b"id", pa.table({"value": [1]}), flush=True)
    name = next(n for n in connector.get_table_names() if n.startswith("o1_"))
    conn = connector._require_open()
    _ = conn.execute(
        sql.SQL(
            'CREATE TEMP TABLE {} ("__record_id" BYTEA PRIMARY KEY, value BIGINT)'
        ).format(sql.Identifier(name))
    )
    temp = sql.Identifier("pg_temp", name)
    _ = conn.execute(
        sql.SQL("INSERT INTO {} VALUES (%s, %s)").format(temp), (b"id", 99)
    )
    db.add_record(path, b"id", pa.table({"value": [2]}), flush=True)
    assert conn.execute(sql.SQL("SELECT value FROM {}").format(temp)).fetchone() == (
        99,
    )
    rows = db.get_all_records(path)
    assert rows is not None and rows.to_pylist() == [{"value": 2}]


@pytest.mark.postgres
@pytest.mark.parametrize("operation", ["list", "queue", "flush"])
@pytest.mark.parametrize("toast", [False, True], ids=["pg_temp", "temporary_toast"])
def test_store_refuses_a_temporary_schema(
    connector: PostgreSQLConnector, operation: str, toast: bool
) -> None:
    db = ConnectorArrowDatabase(connector)
    path = ("temporary",)
    data = pa.table({"value": [1]})
    if operation == "flush":
        db.add_record(path, b"id", data)
    conn = connector._require_open()
    _ = conn.execute("CREATE TEMP TABLE seed (value INTEGER)")
    schema = "pg_temp"
    if toast:
        (temporary_schema,) = conn.execute(
            "SELECT nspname FROM pg_catalog.pg_namespace WHERE oid = pg_my_temp_schema()"
        ).fetchone()
        schema = cast(str, temporary_schema).replace("pg_temp_", "pg_toast_temp_", 1)
    _ = conn.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
    )
    conn.commit()

    with pytest.raises(OrcapodDatabaseError, match="temporary PostgreSQL schema"):
        _check_catalog_operation(db, path, data, operation)

    assert conn.execute(
        """
        SELECT relname FROM pg_catalog.pg_class
        WHERE relnamespace = pg_my_temp_schema() AND relkind = 'r'
        """
    ).fetchall() == [("seed",)]


@pytest.mark.postgres
def test_pending_path_is_refused_if_another_writer_claims_name(
    connector: PostgreSQLConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    from orcapod.databases import store_table_names

    monkeypatch.setattr(store_table_names, "store_table_digest", lambda path: "a" * 64)
    with PostgreSQLConnector(connector._dsn) as other:
        _check_pending_collision(connector, other)


@pytest.mark.postgres
def test_paths_with_a_forced_digest_collision_cannot_share_a_table(
    connector: PostgreSQLConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    from orcapod.databases import store_table_names

    monkeypatch.setattr(store_table_names, "store_table_digest", lambda path: "a" * 64)
    db = ConnectorArrowDatabase(connector)
    committed, colliding = ("name-part",), ("name:part",)
    one, two = pa.table({"value": [1]}), pa.table({"value": [2]})
    db.add_record(committed, b"id", one)
    with pytest.raises(OrcapodDatabaseError, match="name-part.*name:part"):
        db.add_record(colliding, b"id", two)
    db.flush_batch(committed)
    with pytest.raises(OrcapodDatabaseError, match="name-part.*name:part"):
        db.add_record(colliding, b"id", two)
    with pytest.raises(OrcapodDatabaseError, match="name-part.*name:part"):
        db.get_all_records(colliding)
    rows = db.get_all_records(committed)
    assert rows is not None and rows.equals(one)
