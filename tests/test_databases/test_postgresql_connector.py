# tests/test_databases/test_postgresql_connector.py
"""Unit tests for PostgreSQLConnector — no live database required."""

from __future__ import annotations

import asyncio
import pathlib
import socket
from collections.abc import Iterable, Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pyarrow as pa
import pytest

from orcapod.databases import ConnectorArrowDatabase
from orcapod.databases.postgresql_connector import (
    PostgreSQLConnector,
    _arrow_type_to_pg_sql,
    _coerce_pg_value,
    _pg_type_to_arrow,
)
from orcapod.errors import InputValidationError
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
from orcapod.types import ColumnInfo


class _PostgreSQLTransaction:
    """Track PostgreSQL's aborted transaction and the effects of savepoint commands."""

    def __init__(self) -> None:
        self.committed = {b"seed": 1}
        self.pending = self.committed.copy()
        self.savepoint: dict[bytes, int] | None = None
        self.aborted = False
        self.commands: list[str] = []

    def execute(self, sql: str, row: tuple[bytes, int] | None = None) -> None:
        if sql.startswith("ROLLBACK TO SAVEPOINT "):
            assert self.savepoint is not None
            self.pending = self.savepoint.copy()
            self.aborted = False
        assert not self.aborted
        if sql.startswith("SAVEPOINT "):
            assert self.savepoint is None
            self.savepoint = self.pending.copy()
        elif sql.startswith("RELEASE SAVEPOINT "):
            assert self.savepoint is not None
            self.savepoint = None
        elif sql.startswith("INSERT "):
            assert row is not None
            key, value = row
            if value < 0:
                self.aborted = True
                raise psycopg.errors.CheckViolation("value must be positive")
            self.pending[key] = value
        elif not sql.startswith("ROLLBACK TO SAVEPOINT "):
            assert sql.startswith("CREATE TABLE IF NOT EXISTS") or sql == (
                "SET CONSTRAINTS ALL IMMEDIATE"
            )
        self.commands.append(sql)

    def executemany(self, sql: str, rows: Iterable[tuple[bytes, int]]) -> None:
        for row in rows:
            self.execute(sql, row)

    def commit(self) -> None:
        assert not self.aborted
        assert self.savepoint is None
        self.committed = self.pending.copy()
        self.commands.append("COMMIT")

    def rollback(self) -> None:
        self.aborted = False
        self.pending = self.committed.copy()
        self.savepoint = None
        self.commands.append("ROLLBACK")

    def read(self) -> Iterator[pa.RecordBatch]:
        yield pa.RecordBatch.from_arrays(
            [
                pa.array(list(self.committed), pa.large_binary()),
                pa.array(list(self.committed.values()), pa.int64()),
            ],
            names=["__record_id", "v"],
        )


@pytest.mark.parametrize("bad_first", [False, True])
@pytest.mark.parametrize("on_event_loop", [False, True])
def test_postgresql_check_refusal_uses_savepoints_to_commit_valid_rows(
    bad_first: bool, on_event_loop: bool
) -> None:
    transaction = _PostgreSQLTransaction()
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.execute.side_effect = transaction.execute
    cursor.executemany.side_effect = transaction.executemany
    connection.commit.side_effect = transaction.commit
    connection.rollback.side_effect = transaction.rollback
    with patch("psycopg.connect", return_value=connection):
        connector = PostgreSQLConnector("dbname=test")

    with (
        connector,
        patch.object(connector, "get_table_names", return_value=["t"]),
        patch.object(
            connector,
            "get_column_info",
            return_value=[
                ColumnInfo("__record_id", pa.large_binary(), nullable=False),
                ColumnInfo("v", pa.int64(), nullable=True),
            ],
        ),
        patch.object(
            connector, "iter_batches", side_effect=lambda *args: transaction.read()
        ),
    ):
        writer = ConnectorArrowDatabase(connector)
        reader = ConnectorArrowDatabase(connector)

        def check() -> None:
            left = pa.table({"id": [b"bad-1", b"good-1"], "v": [-1, 11]})
            right = pa.table({"id": [b"good-2", b"bad-2"], "v": [12, -2]})
            for batch in (left, right) if bad_first else (right, left):
                writer.add_records(("t",), batch, record_id_column="id")
            with pytest.raises(
                InputValidationError, match="must be positive"
            ) as refused:
                writer.flush()
            for handle in (reader, writer):
                rows = handle.get_all_records(("t",), record_id_column="id")
                assert rows is not None
                assert rows.sort_by("id").to_pylist() == [
                    {"id": b"good-1", "v": 11},
                    {"id": b"good-2", "v": 12},
                    {"id": b"seed", "v": 1},
                ]
            assert "bad-1" in str(refused.value) and "bad-2" in str(refused.value)
            assert (
                sum(
                    command.startswith("SAVEPOINT ") for command in transaction.commands
                )
                == 4
            )
            assert (
                sum(
                    command.startswith("ROLLBACK TO SAVEPOINT ")
                    for command in transaction.commands
                )
                == 2
            )
            assert transaction.commands.count("ROLLBACK") == 1
            assert transaction.commands[-1] == "COMMIT"
            before = transaction.commands.copy()
            writer.flush()
            assert transaction.commands == before

        async def on_loop() -> None:
            check()

        if on_event_loop:
            asyncio.run(on_loop())
        else:
            check()


class TestPgTypeToArrow:
    def test_bool(self):
        assert _pg_type_to_arrow("bool", "bool") == pa.bool_()

    def test_int2(self):
        assert _pg_type_to_arrow("int2", "int2") == pa.int32()

    def test_int4(self):
        assert _pg_type_to_arrow("int4", "int4") == pa.int32()

    def test_int8(self):
        assert _pg_type_to_arrow("int8", "int8") == pa.int64()

    def test_float4(self):
        assert _pg_type_to_arrow("float4", "float4") == pa.float32()

    def test_float8(self):
        assert _pg_type_to_arrow("float8", "float8") == pa.float64()

    def test_numeric(self):
        assert _pg_type_to_arrow("numeric", "numeric") == pa.float64()

    def test_text(self):
        assert _pg_type_to_arrow("text", "text") == pa.large_string()

    def test_varchar(self):
        assert _pg_type_to_arrow("character varying", "varchar") == pa.large_string()

    def test_char(self):
        assert _pg_type_to_arrow("character", "bpchar") == pa.large_string()

    def test_name(self):
        assert _pg_type_to_arrow("name", "name") == pa.large_string()

    def test_bytea(self):
        assert _pg_type_to_arrow("bytea", "bytea") == pa.large_binary()

    def test_uuid(self):
        result = _pg_type_to_arrow("uuid", "uuid")
        assert result == pa.binary(16)

    def test_json(self):
        assert _pg_type_to_arrow("json", "json") == pa.large_string()

    def test_jsonb(self):
        assert _pg_type_to_arrow("jsonb", "jsonb") == pa.large_string()

    def test_date(self):
        assert _pg_type_to_arrow("date", "date") == pa.date32()

    def test_timestamp(self):
        assert _pg_type_to_arrow(
            "timestamp without time zone", "timestamp"
        ) == pa.timestamp("us")

    def test_timestamptz(self):
        assert _pg_type_to_arrow(
            "timestamp with time zone", "timestamptz"
        ) == pa.timestamp("us", tz="UTC")

    def test_time_is_string(self):
        result = _pg_type_to_arrow("time without time zone", "time")
        assert result == pa.large_string()

    def test_timetz_is_string(self):
        result = _pg_type_to_arrow("time with time zone", "timetz")
        assert result == pa.large_string()

    def test_int4_array(self):
        result = _pg_type_to_arrow("ARRAY", "_int4")
        assert result == pa.large_list(pa.int32())

    def test_text_array(self):
        result = _pg_type_to_arrow("ARRAY", "_text")
        assert result == pa.large_list(pa.large_string())

    def test_unknown_falls_back_to_large_string(self):
        result = _pg_type_to_arrow("weird_type", "weird_type")
        assert result == pa.large_string()


class TestArrowTypeToPgSql:
    def test_bool(self):
        assert _arrow_type_to_pg_sql(pa.bool_()) == "BOOLEAN"

    def test_int32(self):
        assert _arrow_type_to_pg_sql(pa.int32()) == "INTEGER"

    def test_int64(self):
        assert _arrow_type_to_pg_sql(pa.int64()) == "BIGINT"

    def test_float32(self):
        assert _arrow_type_to_pg_sql(pa.float32()) == "REAL"

    def test_float64(self):
        assert _arrow_type_to_pg_sql(pa.float64()) == "DOUBLE PRECISION"

    def test_large_string(self):
        assert _arrow_type_to_pg_sql(pa.large_string()) == "TEXT"

    def test_string(self):
        assert _arrow_type_to_pg_sql(pa.utf8()) == "TEXT"

    def test_large_binary(self):
        assert _arrow_type_to_pg_sql(pa.large_binary()) == "BYTEA"

    def test_binary(self):
        assert _arrow_type_to_pg_sql(pa.binary()) == "BYTEA"

    def test_date32(self):
        assert _arrow_type_to_pg_sql(pa.date32()) == "DATE"

    def test_timestamp_no_tz(self):
        assert _arrow_type_to_pg_sql(pa.timestamp("us")) == "TIMESTAMP"

    def test_timestamp_with_tz(self):
        assert _arrow_type_to_pg_sql(pa.timestamp("us", tz="UTC")) == "TIMESTAMPTZ"

    def test_large_list_raises(self):
        with pytest.raises(ValueError, match="not supported"):
            _arrow_type_to_pg_sql(pa.large_list(pa.int32()))

    def test_unknown_raises_instead_of_text(self):
        """No silent TEXT fallback (ITL-1162)."""
        for arrow_type in (pa.null(), pa.struct([("a", pa.int64())])):
            with pytest.raises(ValueError, match="not supported"):
                _arrow_type_to_pg_sql(arrow_type)

    def test_boolean_information_schema_name(self):
        """``information_schema`` names a bool column ``boolean``."""
        assert _pg_type_to_arrow("boolean", "boolean") == pa.bool_()


class TestCoercePgValue:
    """Unit tests for _coerce_pg_value."""

    def test_none_passthrough(self):
        assert _coerce_pg_value(None, pa.binary(16)) is None

    def test_uuid_coerced_to_bytes_for_binary16(self):
        import uuid as _uuid

        u = _uuid.UUID("12345678-1234-5678-1234-567812345678")
        result = _coerce_pg_value(u, pa.binary(16))
        assert result == u.bytes
        assert isinstance(result, bytes)
        assert len(result) == 16

    def test_uuid_not_coerced_for_other_types(self):
        import uuid as _uuid

        u = _uuid.UUID("12345678-1234-5678-1234-567812345678")
        # Non-binary(16) target — UUID object passed through unchanged
        result = _coerce_pg_value(u, pa.large_string())
        assert result is u

    def test_plain_bytes_passthrough_for_binary16(self):
        raw = b"\x00" * 16
        result = _coerce_pg_value(raw, pa.binary(16))
        assert result is raw

    def test_non_uuid_value_passthrough(self):
        assert _coerce_pg_value(42, pa.int32()) == 42
        assert _coerce_pg_value("hello", pa.large_string()) == "hello"


class TestPostgreSQLConnectorScaffold:
    def test_isinstance_dbconnector_protocol(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
            assert isinstance(connector, DBConnectorProtocol)
            connector._conn = None  # skip actual close


class TestConfig:
    def test_to_config(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector(
                "postgresql://user:pass@localhost:5432/mydb"
            )
            config = connector.to_config()
            assert config["connector_type"] == "postgresql"
            assert config["dsn"] == "postgresql://user:pass@localhost:5432/mydb"
            connector._conn = None

    def test_from_config_roundtrip(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            config = {
                "connector_type": "postgresql",
                "dsn": "postgresql://localhost/test",
            }
            connector = PostgreSQLConnector.from_config(config)
            assert isinstance(connector, PostgreSQLConnector)
            connector._conn = None

    def test_from_config_wrong_type_raises(self) -> None:
        with pytest.raises(ValueError, match="postgresql"):
            PostgreSQLConnector.from_config({"connector_type": "sqlite", "dsn": "x"})

    def test_from_config_missing_type_raises(self) -> None:
        with pytest.raises(ValueError, match="postgresql"):
            PostgreSQLConnector.from_config({"dsn": "postgresql://localhost/test"})


class TestLifecycleUnit:
    def test_close_is_idempotent(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            connector = PostgreSQLConnector("postgresql://localhost/test")
            connector.close()
            connector.close()  # must not raise
            assert mock_conn.close.call_count == 1

    def test_require_open_raises_after_close(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
            connector.close()
            with pytest.raises(RuntimeError, match="closed"):
                connector._require_open()

    def test_context_manager(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            with PostgreSQLConnector("postgresql://localhost/test") as c:
                c._require_open()  # must not raise
            with pytest.raises(RuntimeError, match="closed"):
                c._require_open()


class TestValidateTableName:
    def test_raises_on_double_quote(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
            with pytest.raises(ValueError, match="double-quote"):
                connector._validate_table_name('bad"name')
            connector._conn = None

    def test_valid_name_does_not_raise(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
            connector._validate_table_name("valid_table_name")  # must not raise
            connector._conn = None


class TestSchemaIntrospectionUnit:
    """Unit tests for get_table_names and get_pk_columns using mocked cursor."""

    def _make_connector(self) -> PostgreSQLConnector:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            return PostgreSQLConnector("postgresql://localhost/test")

    def test_get_table_names_returns_sorted_list(self) -> None:
        connector = self._make_connector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = [("alpha",), ("beta",)]  # ORDER BY in SQL
        connector._conn.cursor.return_value = mock_cursor
        result = connector.get_table_names()
        assert result == ["alpha", "beta"]
        connector._conn = None

    def test_get_pk_columns_single_pk(self) -> None:
        connector = self._make_connector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = [("id",)]
        connector._conn.cursor.return_value = mock_cursor
        result = connector.get_pk_columns("my_table")
        assert result == ["id"]
        connector._conn = None

    def test_get_pk_columns_no_pk(self) -> None:
        connector = self._make_connector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        connector._conn.cursor.return_value = mock_cursor
        result = connector.get_pk_columns("my_table")
        assert result == []
        connector._conn = None


class TestServerLocationUnit:
    """``current_location`` and ``given_server`` (ITL-1267), mocked connection.

    The values a live server reports are asserted by the postgres-marked tests in
    ``tests/test_core/sources/test_postgresql_table_source_integration.py``.
    """

    @staticmethod
    def _connector(
        dsn: str = "postgresql://localhost/test",
        *,
        host: str = "localhost",
        port: int = 5432,
        parameters: dict[str, str] | None = None,
    ) -> PostgreSQLConnector:
        with patch("psycopg.connect") as mock_connect:
            conn = MagicMock()
            conn.info.host = host
            conn.info.port = port
            conn.info.get_parameters.return_value = (
                {"host": host} if parameters is None else parameters
            )
            cursor = MagicMock()
            cursor.__enter__ = lambda s: s
            cursor.__exit__ = MagicMock(return_value=False)
            cursor.fetchone.return_value = ("testdb", "public")
            conn.cursor.return_value = cursor
            mock_connect.return_value = conn
            return PostgreSQLConnector(dsn)

    @pytest.mark.parametrize(
        ("host", "port"),
        [("db.example.org", 5433), ("/var/run/postgresql", 5432), ("::1", 6543)],
    )
    def test_current_location_reports_the_connection(
        self, host: str, port: int
    ) -> None:
        connector = self._connector(host=host, port=port)
        assert connector.current_location() == (host, port, "testdb", "public")
        connector._conn = None

    # ── given_server: what libpq takes, from the DSN, a service file, the environment
    #
    # Each case hands psycopg's own first connection attempt for the DSN (its host
    # resolution included, every host name resolving to PSYCOPG_ADDRESS) to libpq's
    # PQconnectStart, which applies the DSN, then the service file, then the
    # environment, before it connects; nothing listens at any address used, and the
    # attempt is abandoned once its parameters are read. So the precedence tested is
    # libpq's own, not a model of it.

    PSYCOPG_ADDRESS = "203.0.113.9"
    SOCKET = "/var/run/postgresql"
    SERVER_ENV = ("PGHOST", "PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE", "PGSYSCONFDIR")

    @pytest.fixture
    def isolated_env(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> pathlib.Path:
        """No server variable of the caller's; no service file but the test's."""
        for envvar in self.SERVER_ENV:
            monkeypatch.delenv(envvar, raising=False)
        monkeypatch.setenv("PGSYSCONFDIR", str(tmp_path))
        monkeypatch.setenv("PGSERVICEFILE", str(tmp_path / "pg_service.conf"))
        return tmp_path

    def _as_libpq_takes(self, dsn: str) -> PostgreSQLConnector:
        """A connector whose connection reports the parameters libpq takes for dsn."""
        from psycopg import pq
        from psycopg.conninfo import conninfo_attempts, conninfo_to_dict, make_conninfo

        resolved = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", (self.PSYCOPG_ADDRESS, 5432))
        ]
        with patch("socket.getaddrinfo", return_value=resolved):
            attempt = conninfo_attempts(conninfo_to_dict(dsn))[0]
        pgconn = pq.PGconn.connect_start(make_conninfo("", **attempt).encode())
        try:
            reported = {
                option.keyword.decode(): option.val.decode()
                for option in pgconn.info
                if option.val and option.keyword in (b"host", b"hostaddr")
            }
        finally:
            pgconn.finish()
        return self._connector(dsn, parameters=reported)

    def _given(
        self,
        isolated_env: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        dsn: list[str],
        env: dict[str, str],
        service: list[str] | None,
        reverse: bool,
    ) -> tuple[str, str]:
        """``given_server`` for the DSN keywords, environment and service file lines.

        ``reverse`` writes the DSN keywords, the variables and the service file lines
        in the other order.
        """

        def ordered(items: list[str]) -> list[str]:
            return items[::-1] if reverse else items

        if service is not None:
            lines = ordered(service)
            (isolated_env / "pg_service.conf").write_text(
                "\n".join(["[one]", *lines]) + "\n"
            )
            dsn = [*dsn, "service=one"]
        for envvar in ordered(list(env)):
            monkeypatch.setenv(envvar, env[envvar])
        connector = self._as_libpq_takes(" ".join(ordered([*dsn, "dbname=x"])))
        try:
            return connector.given_server()
        finally:
            connector._conn = None

    @pytest.mark.parametrize("reverse", [False, True], ids=["in-order", "reversed"])
    @pytest.mark.parametrize(
        ("dsn", "expected"),
        [
            (["host=db.example.org"], ("db.example.org", "")),
            ([f"host={SOCKET}"], (SOCKET, "")),
            (["host=192.0.2.1"], ("192.0.2.1", "")),
            (["hostaddr=192.0.2.1"], ("", "192.0.2.1")),
            (["host=192.0.2.1", "hostaddr=192.0.2.1"], ("192.0.2.1", "192.0.2.1")),
            (
                ["host=db.example.org", "hostaddr=192.0.2.1"],
                ("db.example.org", "192.0.2.1"),
            ),
            ([f"host={SOCKET}", "hostaddr=192.0.2.1"], (SOCKET, "192.0.2.1")),
            (
                ["host=a.example.org,b.example.org", "port=5432"],
                ("a.example.org,b.example.org", ""),
            ),
            (["hostaddr=192.0.2.1,192.0.2.2"], ("", "192.0.2.1,192.0.2.2")),
        ],
    )
    def test_given_in_the_dsn(
        self,
        isolated_env: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        dsn: list[str],
        expected: tuple[str, str],
        reverse: bool,
    ) -> None:
        given = self._given(isolated_env, monkeypatch, dsn, {}, None, reverse)
        assert given == expected

    @pytest.mark.parametrize(
        ("dsn", "expected"),
        [
            ("postgresql://u:p@db.example.org:5432/x", ("db.example.org", "")),
            (
                "postgresql://u@db.example.org/x?hostaddr=192.0.2.1",
                ("db.example.org", "192.0.2.1"),
            ),
            (
                "postgresql:///x?hostaddr=192.0.2.1&host=db.example.org",
                ("db.example.org", "192.0.2.1"),
            ),
            (
                "postgresql://u:p@b.example.org:5433,a.example.org:5432/x",
                ("b.example.org,a.example.org", ""),
            ),
            ("postgresql:///x?hostaddr=192.0.2.1", ("", "192.0.2.1")),
        ],
    )
    def test_given_in_a_uri(
        self, isolated_env: pathlib.Path, dsn: str, expected: tuple[str, str]
    ) -> None:
        connector = self._as_libpq_takes(dsn)
        assert connector.given_server() == expected
        connector._conn = None

    @pytest.mark.parametrize("reverse", [False, True], ids=["in-order", "reversed"])
    @pytest.mark.parametrize(
        ("dsn", "env", "expected"),
        [
            ([], {"PGHOST": "db.example.org"}, ("db.example.org", "")),
            ([], {"PGHOSTADDR": "192.0.2.1"}, ("", "192.0.2.1")),
            (
                [],
                {"PGHOST": "db.example.org", "PGHOSTADDR": "192.0.2.1"},
                ("db.example.org", "192.0.2.1"),
            ),
            (
                ["host=db.example.org"],
                {"PGHOSTADDR": "192.0.2.1"},
                ("db.example.org", "192.0.2.1"),
            ),
            (
                ["hostaddr=192.0.2.1"],
                {"PGHOST": "db.example.org"},
                ("db.example.org", "192.0.2.1"),
            ),
            ([], {"PGHOST": SOCKET, "PGHOSTADDR": "192.0.2.1"}, (SOCKET, "192.0.2.1")),
            # The DSN overrides the environment, key by key.
            (
                ["host=db.example.org"],
                {"PGHOST": "a.example.org,b.example.org"},
                ("db.example.org", ""),
            ),
            (
                ["host=db.example.org", "hostaddr=192.0.2.1"],
                {"PGHOSTADDR": "192.0.2.7,192.0.2.8"},
                ("db.example.org", "192.0.2.1"),
            ),
            (
                [],
                {"PGHOST": "a.example.org,b.example.org"},
                ("a.example.org,b.example.org", ""),
            ),
            ([], {"PGHOSTADDR": "192.0.2.1,192.0.2.2"}, ("", "192.0.2.1,192.0.2.2")),
        ],
    )
    def test_given_in_the_environment(
        self,
        isolated_env: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        dsn: list[str],
        env: dict[str, str],
        expected: tuple[str, str],
        reverse: bool,
    ) -> None:
        given = self._given(isolated_env, monkeypatch, dsn, env, None, reverse)
        assert given == expected

    @pytest.mark.parametrize("reverse", [False, True], ids=["in-order", "reversed"])
    @pytest.mark.parametrize(
        ("service", "dsn", "env", "expected"),
        [
            # Host and address from the service file alone.
            (["host=svc.invalid"], [], {}, ("svc.invalid", "")),
            (
                ["host=svc.invalid", "hostaddr=192.0.2.50"],
                [],
                {},
                ("svc.invalid", "192.0.2.50"),
            ),
            (
                ["host=192.0.2.50", "hostaddr=192.0.2.50"],
                [],
                {},
                ("192.0.2.50", "192.0.2.50"),
            ),
            (["hostaddr=192.0.2.50"], [], {}, ("", "192.0.2.50")),
            (
                ["host=a.invalid,b.invalid"],
                [],
                {},
                ("a.invalid,b.invalid", ""),
            ),
            # The service file's address beside a host of the DSN or the environment:
            # psycopg resolves a host name and passes its address, which overrides the
            # service file's; it does not resolve a socket directory, so the service
            # file's address is the one used.
            (
                ["hostaddr=192.0.2.50"],
                ["host=db.example.org"],
                {},
                ("db.example.org", ""),
            ),
            (["hostaddr=192.0.2.50"], [f"host={SOCKET}"], {}, (SOCKET, "192.0.2.50")),
            (
                ["hostaddr=192.0.2.50"],
                [],
                {"PGHOST": "db.example.org"},
                ("db.example.org", ""),
            ),
            (["hostaddr=192.0.2.50"], [], {"PGHOST": SOCKET}, (SOCKET, "192.0.2.50")),
            # The service file overrides the environment, key by key.
            (
                ["hostaddr=192.0.2.50"],
                [],
                {"PGHOSTADDR": "192.0.2.60"},
                ("", "192.0.2.50"),
            ),
            (
                ["host=svc.invalid"],
                [],
                {"PGHOST": "db.example.org"},
                ("svc.invalid", PSYCOPG_ADDRESS),
            ),
            (["host=svc.invalid"], [], {"PGHOST": SOCKET}, ("svc.invalid", "")),
            (
                ["host=svc.invalid"],
                [],
                {"PGHOST": "db.example.org", "PGHOSTADDR": "192.0.2.60"},
                ("svc.invalid", "192.0.2.60"),
            ),
            # The service file's host beside an address of the DSN or the environment.
            (
                ["host=svc.invalid"],
                ["hostaddr=192.0.2.1"],
                {},
                ("svc.invalid", "192.0.2.1"),
            ),
            (
                ["host=svc.invalid"],
                [],
                {"PGHOSTADDR": "192.0.2.60"},
                ("svc.invalid", "192.0.2.60"),
            ),
            # The DSN overrides the service file.
            (
                ["host=svc.invalid", "hostaddr=192.0.2.50"],
                ["host=db.example.org"],
                {},
                ("db.example.org", ""),
            ),
            (
                ["host=svc.invalid", "hostaddr=192.0.2.50"],
                ["hostaddr=192.0.2.1"],
                {},
                ("svc.invalid", "192.0.2.1"),
            ),
        ],
    )
    def test_given_in_a_service_file(
        self,
        isolated_env: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        service: list[str],
        dsn: list[str],
        env: dict[str, str],
        expected: tuple[str, str],
        reverse: bool,
    ) -> None:
        given = self._given(isolated_env, monkeypatch, dsn, env, service, reverse)
        assert given == expected

    @pytest.mark.parametrize(
        ("at_connect", "later", "expected"),
        [
            # psycopg resolved PGHOST: a PGHOSTADDR set later does not make its
            # address the user's.
            (
                {"PGHOST": "db.example.org"},
                {"PGHOSTADDR": "192.0.2.7"},
                ("db.example.org", ""),
            ),
            # Another host later, in place of the one connected to.
            (
                {"PGHOST": "db.example.org"},
                {"PGHOST": "other.example.org"},
                ("db.example.org", ""),
            ),
            # Several servers at connect time stay several when PGHOST is unset.
            (
                {"PGHOST": "a.example.org,b.example.org"},
                {"PGHOST": None},
                ("a.example.org,b.example.org", ""),
            ),
            # One server at connect time stays one when PGHOST becomes a list.
            (
                {"PGHOST": "db.example.org"},
                {"PGHOST": "a.example.org,b.example.org"},
                ("db.example.org", ""),
            ),
        ],
    )
    def test_the_environment_is_read_when_the_connector_connects(
        self,
        isolated_env: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        at_connect: dict[str, str],
        later: dict[str, str | None],
        expected: tuple[str, str],
    ) -> None:
        for envvar, value in at_connect.items():
            monkeypatch.setenv(envvar, value)
        connector = self._as_libpq_takes("dbname=x")
        for envvar, value in later.items():
            if value is None:
                monkeypatch.delenv(envvar)
            else:
                monkeypatch.setenv(envvar, value)
        try:
            assert connector.given_server() == expected
        finally:
            connector._conn = None


class TestPsycopgResolvesTheHost:
    """The psycopg behaviour ``given_server`` relies on, pinned (ITL-1267).

    psycopg's ``Connection.connect`` resolves a single host name of the DSN or
    ``PGHOST`` itself, when neither gives a ``hostaddr``, and passes the address to
    libpq as ``hostaddr`` (psycopg 3.1.17 and later; earlier releases passed the DSN
    unchanged). Real name resolution of ``localhost`` and psycopg's own connect
    loop: only the step that would open the connection is replaced, to record the
    connection string psycopg hands to libpq. If a future psycopg stops doing this
    (or renames that step), these tests fail.
    """

    @staticmethod
    def _passed_to_libpq(dsn: str) -> list[dict[str, Any]]:
        from psycopg.conninfo import conninfo_to_dict

        passed: list[str] = []

        def refuse(conninfo: str, **_: Any) -> Any:
            passed.append(conninfo)
            raise psycopg.OperationalError("not connecting in a unit test")

        with patch.object(psycopg.Connection, "_connect_gen", side_effect=refuse):
            with pytest.raises(psycopg.OperationalError):
                psycopg.connect(dsn)
        assert passed, "psycopg did not reach the step that opens the connection"
        return [conninfo_to_dict(conninfo) for conninfo in passed]

    @staticmethod
    def _addresses(name: str) -> set[str]:
        return {
            str(info[4][0])
            for info in socket.getaddrinfo(
                name, 5432, proto=socket.IPPROTO_TCP, type=socket.SOCK_STREAM
            )
        }

    @pytest.fixture(autouse=True)
    def _no_server_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for envvar in ("PGHOST", "PGHOSTADDR", "PGSERVICE"):
            monkeypatch.delenv(envvar, raising=False)

    @pytest.mark.parametrize("via", ["dsn", "PGHOST"])
    def test_a_host_name_is_passed_with_the_address_psycopg_resolved(
        self, monkeypatch: pytest.MonkeyPatch, via: str
    ) -> None:
        if via == "PGHOST":
            monkeypatch.setenv("PGHOST", "localhost")
        dsn = "host=localhost dbname=x" if via == "dsn" else "dbname=x"
        attempts = self._passed_to_libpq(dsn)
        addresses = self._addresses("localhost")
        assert {attempt["hostaddr"] for attempt in attempts} <= addresses
        assert all(
            attempt.get("host", "localhost") == "localhost" for attempt in attempts
        )

    @pytest.mark.parametrize(
        ("dsn", "hostaddr"),
        [
            ("host=127.0.0.1 dbname=x", "127.0.0.1"),
            ("host=localhost hostaddr=192.0.2.1 dbname=x", "192.0.2.1"),
            ("hostaddr=192.0.2.1 host=localhost dbname=x", "192.0.2.1"),
        ],
    )
    def test_an_address_is_passed_as_given(self, dsn: str, hostaddr: str) -> None:
        assert [attempt["hostaddr"] for attempt in self._passed_to_libpq(dsn)] == [
            hostaddr
        ]

    def test_a_socket_directory_is_not_resolved(self) -> None:
        attempts = self._passed_to_libpq("host=/var/run/postgresql dbname=x")
        assert [attempt.get("hostaddr") for attempt in attempts] == [None]


def test_iter_batches_reads_a_not_null_column_back_non_null() -> None:
    """The batch schema takes each column's nullability from ``get_column_info``
    (``is_nullable``), as its type, so a column created NOT NULL reads back non-null
    (ITL-1276); a nullable one stays nullable. Unit level: no server."""
    from orcapod.types import ColumnInfo

    with patch("psycopg.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        connector = PostgreSQLConnector("postgresql://localhost/test")
        cursor = MagicMock()
        cursor.description = [MagicMock(), MagicMock()]
        cursor.description[0].name = "required"
        cursor.description[1].name = "optional"
        cursor.fetchmany.side_effect = [[(1, None), (2, 5)], []]
        mock_connect.return_value.cursor.return_value = cursor
        infos = [
            ColumnInfo(name="required", arrow_type=pa.int64(), nullable=False),
            ColumnInfo(name="optional", arrow_type=pa.int64(), nullable=True),
        ]
        with patch.object(PostgreSQLConnector, "get_column_info", return_value=infos):
            batches = list(connector.iter_batches('SELECT * FROM "t"'))
        connector._conn = None  # skip actual close
    assert len(batches) == 1
    schema = batches[0].schema
    assert schema.field("required").nullable is False
    assert schema.field("optional").nullable is True
    assert batches[0].column("required").to_pylist() == [1, 2]


def test_iter_batches_reads_a_non_projected_column_nullable() -> None:
    """Only a direct projection keeps the stored NOT NULL: an aggregate over one table
    (whose column types are still looked up) reads nullable, since it can give nulls
    (Codex review of PR 471). Unit level: no server."""
    from orcapod.types import ColumnInfo

    def batches_of(connector: PostgreSQLConnector, cursor: Any, query: str) -> Any:
        cursor.fetchmany.side_effect = [[(None,)], []]
        return list(connector.iter_batches(query))

    with patch("psycopg.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        connector = PostgreSQLConnector("postgresql://localhost/test")
        cursor = MagicMock()
        cursor.description = [MagicMock()]
        cursor.description[0].name = "required"
        mock_connect.return_value.cursor.return_value = cursor
        infos = [ColumnInfo(name="required", arrow_type=pa.int64(), nullable=False)]
        with patch.object(PostgreSQLConnector, "get_column_info", return_value=infos):
            direct = batches_of(
                connector, cursor, 'SELECT "required" FROM "t" WHERE "required" > 0'
            )
            aggregate = batches_of(
                connector, cursor, 'SELECT max("required") AS "required" FROM "t"'
            )
        connector._conn = None  # skip actual close
    assert direct[0].schema.field("required").nullable is False
    assert aggregate[0].schema.field("required").type == pa.int64()
    assert aggregate[0].schema.field("required").nullable is True
