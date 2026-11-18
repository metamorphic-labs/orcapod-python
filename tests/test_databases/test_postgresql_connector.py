# tests/test_databases/test_postgresql_connector.py
"""Unit tests for PostgreSQLConnector — no live database required."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from orcapod.databases.postgresql_connector import (
    PostgreSQLConnector,
    _arrow_type_to_pg_sql,
    _coerce_pg_value,
    _pg_type_to_arrow,
)
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol


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
    """``current_location`` and ``names_several_servers`` (ITL-1267), mocked connection.

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

    @pytest.mark.parametrize(
        "dsn",
        [
            "host=a.example.org,b.example.org port=5432 dbname=x",
            "host=a.example.org,b.example.org port=5432,5433 dbname=x",
            "postgresql://u:p@a.example.org:5432,b.example.org:5433/x",
            "postgresql://u@b.example.org,a.example.org/x",
            "hostaddr=192.0.2.1,192.0.2.2 dbname=x",
        ],
    )
    def test_several_hosts_in_the_dsn(
        self, dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PGHOST", raising=False)
        monkeypatch.delenv("PGHOSTADDR", raising=False)
        connector = self._connector(dsn)
        assert connector.names_several_servers()
        connector._conn = None

    @pytest.mark.parametrize(
        ("envvar", "value"),
        [
            ("PGHOST", "a.example.org,b.example.org"),
            ("PGHOSTADDR", "192.0.2.1,192.0.2.2"),
        ],
    )
    def test_several_hosts_from_the_environment(
        self, envvar: str, value: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PGHOST", raising=False)
        monkeypatch.delenv("PGHOSTADDR", raising=False)
        monkeypatch.setenv(envvar, value)
        connector = self._connector("dbname=x user=u")
        assert connector.names_several_servers()
        connector._conn = None

    def test_the_dsn_host_overrides_the_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PGHOST", "a.example.org,b.example.org")
        monkeypatch.delenv("PGHOSTADDR", raising=False)
        connector = self._connector("host=db.example.org dbname=x")
        assert not connector.names_several_servers()
        connector._conn = None

    @pytest.mark.parametrize("key", ["host", "hostaddr"])
    def test_several_hosts_the_connection_reports(
        self, key: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A service file can name several hosts; the DSN then names none, and only
        # the connection's own parameters show them.
        monkeypatch.delenv("PGHOST", raising=False)
        monkeypatch.delenv("PGHOSTADDR", raising=False)
        connector = self._connector(
            "service=pool dbname=x",
            parameters={key: "a.example.org,b.example.org"},
        )
        assert connector.names_several_servers()
        connector._conn = None

    @pytest.mark.parametrize(
        "dsn",
        [
            "postgresql://u:p@db.example.org:5432/x",
            "host=db.example.org port=5432 dbname=x",
            "host=db.example.org hostaddr=192.0.2.1 dbname=x",
            "host=/var/run/postgresql dbname=x",
            "dbname=x",
        ],
    )
    def test_one_server(self, dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("PGHOST", raising=False)
        monkeypatch.delenv("PGHOSTADDR", raising=False)
        connector = self._connector(dsn, parameters={"host": "db.example.org"})
        assert not connector.names_several_servers()
        connector._conn = None
