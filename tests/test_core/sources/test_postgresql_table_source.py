"""Unit tests for PostgreSQLTableSource."""

from __future__ import annotations
from typing import ClassVar
from unittest.mock import MagicMock, patch
import pyarrow as pa
import pytest


# ===========================================================================
# 1. Import / export sanity
# ===========================================================================


def test_import_from_core_sources():
    from orcapod.core.sources import PostgreSQLTableSource

    assert PostgreSQLTableSource is not None


def test_import_from_orcapod_sources():
    from orcapod.sources import PostgreSQLTableSource

    assert PostgreSQLTableSource is not None


def test_in_core_sources_all():
    import orcapod.core.sources as m

    assert "PostgreSQLTableSource" in m.__all__


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

DSN = "postgresql://user:pass@localhost:5432/testdb"
_PATCH = "orcapod.core.sources.postgresql_table_source.PostgreSQLConnector"


def _make_mock_connector(
    table_names: list[str] | None = None,
    pk_columns: list[str] | None = None,
    batches: list[pa.RecordBatch] | None = None,
) -> MagicMock:
    """Build a MagicMock shaped like a PostgreSQLConnector.

    Defaults: table 'measurements' with PK 'session_id' and 3 rows.
    """
    mock = MagicMock()

    if table_names is None:
        table_names = ["measurements"]
    if pk_columns is None:
        pk_columns = ["session_id"]
    if batches is None:
        schema = pa.schema(
            [
                pa.field("session_id", pa.large_string()),
                pa.field("trial", pa.int64()),
                pa.field("response", pa.float64()),
            ]
        )
        batches = [
            pa.RecordBatch.from_arrays(
                [
                    pa.array(["s1", "s2", "s3"], type=pa.large_string()),
                    pa.array([1, 2, 3], type=pa.int64()),
                    pa.array([0.1, 0.2, 0.3], type=pa.float64()),
                ],
                schema=schema,
            )
        ]

    mock.get_table_names.return_value = table_names
    mock.get_pk_columns.return_value = pk_columns
    mock.current_location.return_value = ("localhost", 5432, "orcapod", "public")
    mock.given_server.return_value = ("localhost", "")
    mock.get_column_info.return_value = []
    mock.iter_batches.return_value = iter(batches)
    return mock


# ===========================================================================
# 2. Protocol conformance
# ===========================================================================


class TestProtocolConformance:
    def test_is_source_protocol(self):
        from orcapod.core.sources import PostgreSQLTableSource
        from orcapod.protocols.core_protocols import SourceProtocol

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        assert isinstance(src, SourceProtocol)

    def test_is_stream_protocol(self):
        from orcapod.core.sources import PostgreSQLTableSource
        from orcapod.protocols.core_protocols import StreamProtocol

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        assert isinstance(src, StreamProtocol)

    def test_is_pipeline_element_protocol(self):
        from orcapod.core.sources import PostgreSQLTableSource
        from orcapod.protocols.hashing_protocols import PipelineElementProtocol

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        assert isinstance(src, PipelineElementProtocol)


# ===========================================================================
# 3. PK as default tag columns
# ===========================================================================


class TestPKAsDefaultTags:
    def test_single_pk_is_tag_column(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema

    def test_pk_not_in_data_schema(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        _, data_schema = src.output_schema()
        assert "session_id" not in data_schema

    def test_non_pk_columns_in_data_schema(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        _, data_schema = src.output_schema()
        assert "trial" in data_schema
        assert "response" in data_schema

    def test_composite_pk_all_columns_are_tags(self):
        from orcapod.core.sources import PostgreSQLTableSource

        schema = pa.schema(
            [
                pa.field("user_id", pa.large_string()),
                pa.field("event_id", pa.int64()),
                pa.field("payload", pa.large_string()),
            ]
        )
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(["u1", "u1"], type=pa.large_string()),
                pa.array([1, 2], type=pa.int64()),
                pa.array(["click", "scroll"], type=pa.large_string()),
            ],
            schema=schema,
        )
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector(
                table_names=["events"],
                pk_columns=["user_id", "event_id"],
                batches=[batch],
            )
            src = PostgreSQLTableSource(DSN, "events", tag_only_columns=True)
        tag_schema, _ = src.output_schema()
        assert "user_id" in tag_schema
        assert "event_id" in tag_schema

    def test_default_source_id_names_server_and_table(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        assert (
            src.source_id
            == "postgresql_table:localhost:5432/orcapod.public.measurements"
        )

    def test_explicit_source_id_overrides_default(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(
                DSN, "measurements", source_id="meas", tag_only_columns=True
            )
        assert src.source_id == "meas"


# ===========================================================================
# 4. Explicit tag_columns override
# ===========================================================================


class TestExplicitTagOverride:
    def test_explicit_tag_columns_override_pk(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(
                DSN, "measurements", tag_columns=["trial"], tag_only_columns=True
            )
        tag_schema, _ = src.output_schema()
        assert "trial" in tag_schema
        assert "session_id" not in tag_schema

    def test_multiple_explicit_tag_columns(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(
                DSN,
                "measurements",
                tag_columns=["session_id", "trial"],
                tag_only_columns=True,
            )
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema
        assert "trial" in tag_schema


# ===========================================================================
# 5. No-PK error
# ===========================================================================


class TestNoPKError:
    def test_no_pk_and_no_tag_columns_raises(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector(pk_columns=[])
            with pytest.raises(ValueError, match="has no primary key columns"):
                PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)


# ===========================================================================
# 6. Missing / empty table errors
# ===========================================================================


class TestTableErrors:
    def test_missing_table_raises_value_error(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector(table_names=[])
            with pytest.raises(ValueError, match="not found in database"):
                PostgreSQLTableSource(DSN, "nonexistent", tag_only_columns=True)

    def test_empty_table_raises_value_error(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector(batches=[])
            with pytest.raises(ValueError, match="is empty"):
                PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)


# ===========================================================================
# 7. Stream behaviour
# ===========================================================================


class TestStreamBehaviour:
    def test_producer_is_none(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        assert src.producer is None

    def test_upstreams_is_empty(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        assert src.upstreams == ()

    def test_iter_data_yields_one_per_row(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        assert len(list(src.iter_data())) == 3

    def test_iter_data_tags_contain_pk(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        for tags, _ in src.iter_data():
            assert "session_id" in tags

    def test_output_schema_returns_two_schemas(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        assert len(src.output_schema()) == 2

    def test_as_table_returns_pyarrow_table(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        assert isinstance(src.as_table(), pa.Table)

    def test_as_table_row_count_matches_source(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        assert src.as_table().num_rows == 3

    def test_connector_is_closed_after_construction(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_connector = _make_mock_connector()
            mock_cls.return_value = mock_connector
            PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        mock_connector.close.assert_called_once()

    def test_connector_is_closed_even_when_super_raises(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_connector = _make_mock_connector(table_names=[])
            mock_cls.return_value = mock_connector
            with pytest.raises(ValueError):
                PostgreSQLTableSource(DSN, "nonexistent", tag_only_columns=True)
        mock_connector.close.assert_called_once()

    def test_close_exception_is_suppressed_on_success_path(self):
        """close() errors are swallowed — construction still succeeds."""
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_connector = _make_mock_connector()
            mock_connector.close.side_effect = RuntimeError("close failed")
            mock_cls.return_value = mock_connector
            src = PostgreSQLTableSource(
                DSN, "measurements", tag_only_columns=True
            )  # must not raise
        assert src is not None

    def test_close_exception_does_not_mask_super_init_failure(self):
        """close() errors are swallowed — original super().__init__ error still propagates."""
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_connector = _make_mock_connector(table_names=[])
            mock_connector.close.side_effect = RuntimeError("close failed")
            mock_cls.return_value = mock_connector
            with pytest.raises(ValueError, match="not found in database"):
                PostgreSQLTableSource(DSN, "nonexistent", tag_only_columns=True)


# ===========================================================================
# 8. Deterministic hashing
# ===========================================================================


class TestDeterministicHashing:
    def test_pipeline_hash_is_deterministic(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src1 = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        assert src1.pipeline_hash() == src2.pipeline_hash()

    def test_content_hash_is_deterministic(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src1 = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        assert src1.content_hash() == src2.content_hash()

    def test_different_tag_columns_yields_different_pipeline_hash(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src1 = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource(
                DSN, "measurements", tag_columns=["trial"], tag_only_columns=True
            )
        assert src1.pipeline_hash() != src2.pipeline_hash()


# ===========================================================================
# 9. to_config shape
# ===========================================================================


class TestToConfig:
    def _make_src(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            return PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)

    def test_has_source_type_postgresql_table(self):
        assert self._make_src().to_config()["source_type"] == "postgresql_table"

    def test_has_dsn(self):
        assert self._make_src().to_config()["dsn"] == DSN

    def test_has_table_name(self):
        assert self._make_src().to_config()["table_name"] == "measurements"

    def test_has_tag_columns(self):
        assert "session_id" in self._make_src().to_config()["tag_columns"]

    def test_has_source_id(self):
        assert (
            self._make_src().to_config()["source_id"]
            == "postgresql_table:localhost:5432/orcapod.public.measurements"
        )

    def test_has_content_hash(self):
        assert "content_hash" in self._make_src().to_config()

    def test_has_pipeline_hash(self):
        assert "pipeline_hash" in self._make_src().to_config()

    def test_no_connector_key(self):
        assert "connector" not in self._make_src().to_config()

    def test_no_label_key(self):
        # label is not serialised (consistent with SQLiteTableSource)
        assert "label" not in self._make_src().to_config()


# ===========================================================================
# 10. from_config round-trip
# ===========================================================================


class TestFromConfig:
    def _make_src(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            return PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)

    def test_from_config_reconstructs_source_id(self):
        from orcapod.core.sources import PostgreSQLTableSource

        src = self._make_src()
        config = src.to_config()
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource.from_config(config)
        assert src2.source_id == src.source_id

    def test_from_config_hashes_match(self):
        from orcapod.core.sources import PostgreSQLTableSource

        src = self._make_src()
        config = src.to_config()
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource.from_config(config)
        assert src2.content_hash() == src.content_hash()
        assert src2.pipeline_hash() == src.pipeline_hash()

    def test_from_config_with_explicit_tag_columns(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(
                DSN, "measurements", tag_columns=["trial"], tag_only_columns=True
            )
        config = src.to_config()
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource.from_config(config)
        tag_schema, _ = src2.output_schema()
        assert "trial" in tag_schema

    def test_from_config_missing_dsn_raises(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with pytest.raises(KeyError):
            PostgreSQLTableSource.from_config({"table_name": "measurements"})


# ===========================================================================
# 11. resolve_source_from_config dispatch
# ===========================================================================


def test_resolve_source_from_config_dispatches_to_postgresql_table_source():
    from orcapod.core.sources import PostgreSQLTableSource
    from orcapod.pipeline.serialization import resolve_source_from_config

    with patch(_PATCH) as mock_cls:
        mock_cls.return_value = _make_mock_connector()
        src = PostgreSQLTableSource(DSN, "measurements", tag_only_columns=True)
    config = src.to_config()

    with patch(_PATCH) as mock_cls:
        mock_cls.return_value = _make_mock_connector()
        src2 = resolve_source_from_config(config)

    assert isinstance(src2, PostgreSQLTableSource)


# ===========================================================================
# 12. The default source id names the server (ITL-1267)
# ===========================================================================


def _server_connector(
    host: str,
    port: int,
    database: str = "orcapod",
    schema: str = "public",
    *,
    given: tuple[str, str] | None = None,
) -> MagicMock:
    """A mocked connector whose connection reports ``host``, ``port`` and location.

    ``given`` is the ``host`` and ``hostaddr`` the user gave (``given_server``);
    by default the host alone.
    """
    mock = _make_mock_connector()
    mock.current_location.return_value = (host, port, database, schema)
    mock.given_server.return_value = (host, "") if given is None else given
    return mock


class TestTableAddress:
    """``postgresql_table_address``: the location of the default source id."""

    @pytest.mark.parametrize(
        ("host", "port", "expected"),
        [
            ("db.example.org", 5432, "db.example.org:5432/orcapod.public.t"),
            ("db.example.org", 5433, "db.example.org:5433/orcapod.public.t"),
            # A DNS name is case-insensitive: one spelling.
            ("DB.Example.ORG", 5432, "db.example.org:5432/orcapod.public.t"),
            ("LOCALHOST", 5432, "localhost:5432/orcapod.public.t"),
            # An IPv4 address as reported.
            ("127.0.0.1", 5432, "127.0.0.1:5432/orcapod.public.t"),
            # An IPv6 address: bracketed, in the exploded form (never '::').
            (
                "::1",
                5432,
                "[0000:0000:0000:0000:0000:0000:0000:0001]:5432/orcapod.public.t",
            ),
            (
                "2001:DB8::1",
                5432,
                "[2001:0db8:0000:0000:0000:0000:0000:0001]:5432/orcapod.public.t",
            ),
            (
                "2001:db8:0:0:0:0:0:1",
                5432,
                "[2001:0db8:0000:0000:0000:0000:0000:0001]:5432/orcapod.public.t",
            ),
            (
                "fe80::1%eth0",
                5432,
                "[fe80:0000:0000:0000:0000:0000:0000:0001%eth0]:5432/orcapod.public.t",
            ),
            # A Unix-domain socket directory, as reported, case kept.
            (
                "/var/run/postgresql",
                5432,
                "/var/run/postgresql:5432/orcapod.public.t",
            ),
            ("/Tmp/PG", 5433, "/Tmp/PG:5433/orcapod.public.t"),
            # A Linux abstract-namespace socket, as reported.
            ("@pgsock", 5432, "@pgsock:5432/orcapod.public.t"),
        ],
    )
    def test_address(self, host: str, port: int, expected: str) -> None:
        from orcapod.core.sources.postgresql_table_source import (
            postgresql_table_address,
        )

        assert postgresql_table_address(host, port, "orcapod", "public", "t") == (
            expected
        )

    def test_spellings_of_one_ipv6_address_give_one_address(self) -> None:
        from orcapod.core.sources.postgresql_table_source import (
            postgresql_table_address,
        )

        addresses = {
            postgresql_table_address(host, 5432, "d", "s", "t")
            for host in ("::1", "0::1", "0:0:0:0:0:0:0:1")
        }
        assert len(addresses) == 1

    @pytest.mark.parametrize(
        ("database", "schema", "table"),
        [
            ("a/b", "public", "t"),
            ("orcapod", "a/b", "t"),
            ("orcapod", "public", "a/b"),
            ("a.b", "public", "t"),
            ("orcapod", "a.b", "t"),
            ("orcapod", "public", "a.b"),
        ],
    )
    def test_separator_in_a_name_has_no_address(
        self, database: str, schema: str, table: str
    ) -> None:
        from orcapod.core.sources.postgresql_table_source import (
            postgresql_table_address,
        )

        with pytest.raises(ValueError, match="source_id"):
            postgresql_table_address("localhost", 5432, database, schema, table)

    @pytest.mark.parametrize(
        "host",
        ["/tmp/pg::sock", "/a::b/c", "@pg::sock", "/tmp/pg:", "/tmp/a:b:"],
        ids=[
            "socket directory holding ::",
            ":: in the middle",
            "abstract socket holding ::",
            "socket directory ending with ':'",
            "':' inside and at the end",
        ],
    )
    def test_a_host_that_would_put_double_colon_in_the_address_has_none(
        self, host: str
    ) -> None:
        """``::`` separates a source-info token: refused, asking for a source_id."""
        from orcapod.core.sources.postgresql_table_source import (
            postgresql_table_address,
        )

        with pytest.raises(
            ValueError, match="contains '::'.*give the source a source_id"
        ):
            postgresql_table_address(host, 5432, "orcapod", "public", "t")

    def test_a_single_colon_in_a_socket_directory_is_kept(self) -> None:
        from orcapod.core.sources.postgresql_table_source import (
            postgresql_table_address,
        )

        assert postgresql_table_address("/tmp/a:b", 5432, "d", "s", "t") == (
            "/tmp/a:b:5432/d.s.t"
        )

    def test_empty_host_has_no_address(self) -> None:
        from orcapod.core.sources.postgresql_table_source import (
            postgresql_table_address,
        )

        with pytest.raises(ValueError, match="source_id"):
            postgresql_table_address("", 5432, "orcapod", "public", "t")


class TestDefaultSourceIdNamesServer:
    def _source(
        self, connector: MagicMock, dsn: str = DSN, source_id: str | None = None
    ):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = connector
            return PostgreSQLTableSource(
                dsn, "measurements", source_id=source_id, tag_only_columns=True
            )

    def test_one_table_name_on_several_servers_gives_several_ids(self) -> None:
        servers = [
            ("db.example.org", 5432),
            ("db.example.org", 5433),
            ("db2.example.org", 5432),
            ("/var/run/postgresql", 5432),
        ]
        ids = [self._source(_server_connector(h, p)).source_id for h, p in servers]
        assert ids == [
            "postgresql_table:db.example.org:5432/orcapod.public.measurements",
            "postgresql_table:db.example.org:5433/orcapod.public.measurements",
            "postgresql_table:db2.example.org:5432/orcapod.public.measurements",
            "postgresql_table:/var/run/postgresql:5432/orcapod.public.measurements",
        ]
        # In the reverse order each source keeps its own id.
        reversed_ids = [
            self._source(_server_connector(h, p)).source_id
            for h, p in reversed(servers)
        ]
        assert reversed_ids == list(reversed(ids))

    def test_one_server_gives_one_id_whatever_the_dsn(self) -> None:
        dsns = [
            "postgresql://alice:s3cret@db.example.org:5432/orcapod",
            "host=db.example.org port=5432 dbname=orcapod user=alice password=s3cret",
            "host=DB.EXAMPLE.ORG dbname=orcapod user=bob",
        ]
        ids = {
            self._source(_server_connector("db.example.org", 5432), dsn).source_id
            for dsn in dsns
        }
        assert ids == {
            "postgresql_table:db.example.org:5432/orcapod.public.measurements"
        }

    @pytest.mark.parametrize(
        "dsn",
        [
            "postgresql://alice:s3cret@db.example.org:5432/orcapod",
            "host=db.example.org port=5432 dbname=orcapod user=alice password=s3cret",
        ],
    )
    def test_id_never_holds_user_or_password(self, dsn: str) -> None:
        source_id = self._source(
            _server_connector("db.example.org", 5432), dsn
        ).source_id
        assert "alice" not in source_id
        assert "s3cret" not in source_id

    SEVERAL_SERVERS: ClassVar[list[tuple[str, str]]] = [
        ("a.example.org,b.example.org", ""),
        ("b.example.org,a.example.org", ""),
        ("", "192.0.2.1,192.0.2.2"),
        ("a.example.org,b.example.org", "192.0.2.1,192.0.2.2"),
    ]
    # A host beside a hostaddr that is not that address: the connection is sent to
    # the address, so the host does not name the server.
    HOST_BESIDE_HOSTADDR: ClassVar[list[tuple[str, str]]] = [
        ("db.example.org", "192.0.2.1"),
        ("db.example.org", "192.0.2.2"),
        ("localhost", "127.0.0.1"),
        ("/var/run/postgresql", "192.0.2.1"),
        ("192.0.2.1", "192.0.2.2"),
    ]

    @pytest.mark.parametrize("given", SEVERAL_SERVERS)
    def test_several_servers_have_no_default(self, given: tuple[str, str]) -> None:
        connector = _server_connector("db.example.org", 5432, given=given)
        with pytest.raises(ValueError, match="several servers.*source_id"):
            self._source(connector)
        connector.current_location.assert_not_called()

    @pytest.mark.parametrize("given", HOST_BESIDE_HOSTADDR)
    def test_host_beside_hostaddr_has_no_default(self, given: tuple[str, str]) -> None:
        connector = _server_connector(given[0], 5432, given=given)
        with pytest.raises(
            ValueError, match="host name alone does not identify.*source_id"
        ):
            self._source(connector)
        connector.current_location.assert_not_called()

    @pytest.mark.parametrize(
        ("host", "hostaddr"),
        [
            ("0:0:0:0:0:0:0:1", "::1"),
            ("::1", "0:0:0:0:0:0:0:1"),
            ("2001:DB8::1", "2001:db8:0:0:0:0:0:1"),
            ("2001:db8:0:0:0:0:0:1", "2001:DB8::1"),
            ("fe80::1%eth0", "fe80:0:0:0:0:0:0:1%eth0"),
        ],
        ids=[
            "exploded host, short address",
            "short host, exploded address",
            "upper case host",
            "upper case address",
            "one zone",
        ],
    )
    def test_two_spellings_of_one_ip_address_name_it(
        self, host: str, hostaddr: str
    ) -> None:
        connector = _server_connector(host, 5432, given=(host, hostaddr))
        source_id = self._source(connector).source_id
        assert source_id.startswith("postgresql_table:[")
        assert source_id.endswith("]:5432/orcapod.public.measurements")

    @pytest.mark.parametrize(
        ("host", "hostaddr"),
        [
            ("fe80::1%eth0", "fe80::1"),
            ("fe80::1", "fe80::1%eth0"),
            ("fe80::1%eth0", "fe80::1%eth1"),
            ("::ffff:127.0.0.1", "127.0.0.1"),
            ("::1", "::2"),
            ("localhost", "::1"),
        ],
        ids=[
            "zone only on the host",
            "zone only on the address",
            "two zones",
            "IPv4-mapped against IPv4",
            "two IPv6 addresses",
            "a name beside an IPv6 address",
        ],
    )
    def test_different_addresses_or_a_name_still_have_no_default(
        self, host: str, hostaddr: str
    ) -> None:
        connector = _server_connector(host, 5432, given=(host, hostaddr))
        with pytest.raises(ValueError, match="host name alone does not identify"):
            self._source(connector)

    def test_hostaddr_alone_or_equal_to_the_host_names_the_address(self) -> None:
        # libpq reports the address as the host when no host is given; a host that
        # is the address itself names it too.
        for given in [("", "192.0.2.1"), ("192.0.2.1", "192.0.2.1")]:
            connector = _server_connector("192.0.2.1", 5432, given=given)
            assert self._source(connector).source_id == (
                "postgresql_table:192.0.2.1:5432/orcapod.public.measurements"
            )

    @pytest.mark.parametrize(
        "given", [("db.example.org", ""), *SEVERAL_SERVERS, *HOST_BESIDE_HOSTADDR]
    )
    def test_given_source_id_is_used_as_given(self, given: tuple[str, str]) -> None:
        connector = _server_connector("db.example.org", 5432, given=given)
        src = self._source(connector, source_id="measurements_v1")
        assert src.source_id == "measurements_v1"
        connector.current_location.assert_not_called()
        connector.given_server.assert_not_called()
