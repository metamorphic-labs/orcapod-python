"""Integration tests for PostgreSQLTableSource — requires a live PostgreSQL instance.

Run with:
    pytest -m postgres tests/test_core/sources/test_postgresql_table_source_integration.py

Connects via standard PG* environment variables:

    PGHOST      (default: localhost)
    PGPORT      (default: 5432)
    PGDATABASE  (default: testdb)
    PGUSER      (default: postgres)
    PGPASSWORD  (default: postgres)

Each test gets an isolated schema created fresh and dropped on teardown.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator, Mapping
from types import MappingProxyType

import pytest

psycopg = pytest.importorskip("psycopg")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _base_dsn() -> str:
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    dbname = os.environ.get("PGDATABASE", "testdb")
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "postgres")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


@pytest.fixture
def pg_schema() -> Iterator[str]:
    """Create a fresh per-test schema; drop it on teardown."""
    base_dsn = _base_dsn()
    schema = f"test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
    yield schema
    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.fixture
def schema_dsn(pg_schema: str) -> str:
    """DSN that pins the search_path to the test schema."""
    return f"{_base_dsn()} options=-csearch_path={pg_schema}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestSinglePKTable:
    """Source backed by a table with a single-column PK."""

    def test_pk_column_is_tag(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    "(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)"
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "measurements", tag_only_columns=True)
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema

    def test_non_pk_columns_in_data_schema(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    "(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)"
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1)],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "measurements", tag_only_columns=True)
        _, data_schema = src.output_schema()
        assert "trial" in data_schema
        assert "response" in data_schema

    def test_iter_data_count_matches_rows(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    "(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)"
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "measurements", tag_only_columns=True)
        assert len(list(src.iter_data())) == 3

    def test_tag_values_are_correct(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    "(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)"
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "measurements", tag_only_columns=True)
        tag_values = sorted([tags["session_id"] for tags, _ in src.iter_data()])
        assert tag_values == ["s1", "s2", "s3"]


@pytest.mark.postgres
class TestCompositePKTable:
    """Source backed by a table with a composite PK."""

    def test_both_pk_columns_are_tags(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "events" '
                    "(user_id TEXT, event_id INTEGER, payload TEXT, "
                    "PRIMARY KEY (user_id, event_id))"
                )
                cur.executemany(
                    'INSERT INTO "events" VALUES (%s, %s, %s)',
                    [("u1", 1, "click"), ("u1", 2, "scroll")],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "events", tag_only_columns=True)
        tag_schema, _ = src.output_schema()
        assert "user_id" in tag_schema
        assert "event_id" in tag_schema


@pytest.mark.postgres
class TestExplicitTagOverride:
    """tag_columns override overrides the PK."""

    def test_explicit_tag_columns_override_pk(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    "(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)"
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1)],
                )
            conn.commit()

        src = PostgreSQLTableSource(
            schema_dsn, "measurements", tag_columns=["trial"], tag_only_columns=True
        )
        tag_schema, _ = src.output_schema()
        assert "trial" in tag_schema
        assert "session_id" not in tag_schema


@pytest.mark.postgres
@pytest.mark.integration
class TestPipelineIntegration:
    """PostgreSQLTableSource drives a full pipeline end-to-end."""

    def test_postgresql_source_in_pipeline(self, schema_dsn: str) -> None:
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.data_function import PythonDataFunction
        from orcapod.core.sources import PostgreSQLTableSource
        from orcapod.databases import InMemoryArrowDatabase
        from orcapod.pipeline import PipelineJob
        from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    "(session_id TEXT PRIMARY KEY, trial INTEGER NOT NULL, response REAL NOT NULL)"
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
                )
            conn.commit()

        def double_response(trial: int, response: float) -> float:
            return response * 2.0

        src = PostgreSQLTableSource(schema_dsn, "measurements", tag_only_columns=True)
        pf = PythonDataFunction(double_response, output_keys="doubled")
        pod = FunctionPod(pf)

        job = PipelineJob(name="pg_integration", database=InMemoryArrowDatabase())
        with job:
            pod(src, label="doubler")

        from orcapod.pipeline.dag import OrcaDAG

        exec_dag: OrcaDAG = OrcaDAG()
        for node in job._persistent_node_map.values():
            exec_dag.add_node(node)
        for u_hash, v_hash in job._graph_edges:
            if (
                u_hash in job._persistent_node_map
                and v_hash in job._persistent_node_map
            ):
                exec_dag.add_edge(
                    job._persistent_node_map[u_hash],
                    job._persistent_node_map[v_hash],
                )
        orch = SyncPipelineOrchestrator()
        result = orch.run(exec_dag)

        fn_outputs = [
            v for k, v in result.node_outputs.items() if k.node_type == "function"
        ]
        assert len(fn_outputs) == 1
        assert len(fn_outputs[0]) == 3

        doubled_values = sorted([pkt.as_dict()["doubled"] for _, pkt in fn_outputs[0]])
        assert doubled_values == pytest.approx([0.2, 0.4, 0.6])

        tag_values = sorted([tags["session_id"] for tags, _ in fn_outputs[0]])
        assert tag_values == ["s1", "s2", "s3"]


# ---------------------------------------------------------------------------
# The default source id names the server (ITL-1267)
# ---------------------------------------------------------------------------


# The test server's connection parameters, read from the PG* environment once, when
# the module is imported: before any fixture (``no_server_env``) or test changes it.
_SERVER: Mapping[str, str] = MappingProxyType(
    {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": os.environ.get("PGPORT", "5432"),
        "dbname": os.environ.get("PGDATABASE", "testdb"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", "postgres"),
    }
)


def _keyword_dsn(schema: str, **params: str | None) -> str:
    """A keyword DSN for the test server; a parameter given as None is left out.

    The parameters given come after the server's others, in the order given.
    """
    merged: dict[str, str | None] = {
        **{key: value for key, value in _SERVER.items() if key not in params},
        **params,
    }
    parts = [f"{key}={value}" for key, value in merged.items() if value is not None]
    return " ".join([*parts, f"options=-csearch_path={schema}"])


@pytest.fixture
def server_table(pg_schema: str) -> str:
    """A one-row table ``t`` in the test schema; returns the schema."""
    with psycopg.connect(_keyword_dsn(pg_schema)) as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (k TEXT PRIMARY KEY, x INTEGER)')
            cur.execute("INSERT INTO \"t\" VALUES ('a', 1)")
        conn.commit()
    return pg_schema


@pytest.fixture
def no_server_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the PG* host and port variables out of the DSNs a test builds."""
    for envvar in ("PGHOST", "PGHOSTADDR", "PGPORT"):
        monkeypatch.delenv(envvar, raising=False)


def _source_id(dsn: str) -> str:
    from orcapod.core.sources import PostgreSQLTableSource

    return PostgreSQLTableSource(dsn, "t", tag_only_columns=True).source_id


def _expected(host: str, schema: str, port: str | None = None) -> str:
    return (
        f"postgresql_table:{host}:{port or _SERVER['port']}/"
        f"{_SERVER['dbname']}.{schema}.t"
    )


def _requires_localhost() -> None:
    if _SERVER["host"] != "localhost":
        pytest.skip("needs the test server at PGHOST=localhost, as in CI")


@pytest.mark.postgres
class TestDefaultSourceIdOnTheServer:
    """What the connection reports, measured on the test server.

    The CI server is reached as ``localhost:5432`` over TCP; Unix sockets are covered
    by unit tests only (the CI server runs in a container and has no local socket).
    """

    def test_uri_and_keyword_dsn_give_one_id(self, server_table: str) -> None:
        uri = (
            f"postgresql://{_SERVER['user']}:{_SERVER['password']}@{_SERVER['host']}:"
            f"{_SERVER['port']}/{_SERVER['dbname']}"
            f"?options=-csearch_path%3D{server_table}"
        )
        ids = [_source_id(uri), _source_id(_keyword_dsn(server_table))]
        assert ids == [_expected(_SERVER["host"].lower(), server_table)] * 2

    def test_id_never_holds_user_or_password(self, server_table: str) -> None:
        # Both DSNs carry the user and the password; the id is exactly the host, the
        # port and the three-part name, with room for nothing else. (A substring
        # check would fail a correct id whenever the user's name is also the
        # database's, as with PGUSER=postgres PGDATABASE=postgres.)
        uri = (
            f"postgresql://{_SERVER['user']}:{_SERVER['password']}@{_SERVER['host']}:"
            f"{_SERVER['port']}/{_SERVER['dbname']}"
            f"?options=-csearch_path%3D{server_table}"
        )
        for dsn in (uri, _keyword_dsn(server_table)):
            assert _source_id(dsn) == _expected(_SERVER["host"].lower(), server_table)

    def test_upper_case_host_name_gives_the_lower_case_id(
        self, server_table: str
    ) -> None:
        _requires_localhost()
        ids = [
            _source_id(_keyword_dsn(server_table, host=host))
            for host in ("LOCALHOST", "localhost", "LocalHost")
        ]
        assert ids == [_expected("localhost", server_table)] * 3

    def test_omitted_port_is_written_as_the_port_used(
        self, server_table: str, no_server_env: None
    ) -> None:
        _requires_localhost()
        if _SERVER["port"] != "5432":
            pytest.skip("needs the test server on the default port 5432")
        omitted = _source_id(_keyword_dsn(server_table, host="localhost", port=None))
        given = _source_id(_keyword_dsn(server_table, host="localhost", port="5432"))
        assert omitted == given == _expected("localhost", server_table, "5432")

    def test_two_spellings_of_one_server_give_two_ids(self, server_table: str) -> None:
        _requires_localhost()
        ids = [
            _source_id(_keyword_dsn(server_table, host=host))
            for host in ("localhost", "127.0.0.1")
        ]
        assert ids == [
            _expected("localhost", server_table),
            _expected("127.0.0.1", server_table),
        ]

    def test_hostaddr_alone_names_the_address(
        self, server_table: str, no_server_env: None
    ) -> None:
        _requires_localhost()
        dsn = _keyword_dsn(server_table, host=None, hostaddr="127.0.0.1")
        assert _source_id(dsn) == _expected("127.0.0.1", server_table)

    def test_host_equal_to_hostaddr_names_the_address(
        self, server_table: str, no_server_env: None
    ) -> None:
        _requires_localhost()
        dsn = _keyword_dsn(server_table, host="127.0.0.1", hostaddr="127.0.0.1")
        assert _source_id(dsn) == _expected("127.0.0.1", server_table)

    @pytest.mark.parametrize(
        "order",
        [("host", "hostaddr"), ("hostaddr", "host")],
        ids=["host-first", "hostaddr-first"],
    )
    @pytest.mark.parametrize("form", ["keyword", "uri"])
    def test_host_beside_hostaddr_in_the_dsn_has_no_default(
        self,
        server_table: str,
        no_server_env: None,
        form: str,
        order: tuple[str, str],
    ) -> None:
        _requires_localhost()
        from orcapod.core.sources import PostgreSQLTableSource

        server = {"host": "LocalHost", "hostaddr": "127.0.0.1"}
        if form == "keyword":
            dsn = _keyword_dsn(server_table, **{key: server[key] for key in order})
        else:
            query = "&".join(f"{key}={server[key]}" for key in order)
            dsn = (
                f"postgresql://{_SERVER['user']}:{_SERVER['password']}@:"
                f"{_SERVER['port']}/{_SERVER['dbname']}?{query}"
                f"&options=-csearch_path%3D{server_table}"
            )
        with pytest.raises(ValueError, match="host name alone does not identify"):
            _source_id(dsn)
        given = PostgreSQLTableSource(
            dsn, "t", source_id="t_by_address", tag_only_columns=True
        )
        assert given.source_id == "t_by_address"

    @pytest.mark.parametrize(
        ("dsn_params", "env"),
        [
            ({"host": "localhost"}, {"PGHOSTADDR": "127.0.0.1"}),
            ({"hostaddr": "127.0.0.1"}, {"PGHOST": "localhost"}),
            ({}, {"PGHOST": "localhost", "PGHOSTADDR": "127.0.0.1"}),
            ({}, {"PGHOSTADDR": "127.0.0.1", "PGHOST": "localhost"}),
        ],
        ids=["dsn-host-env-addr", "env-host-dsn-addr", "env-both", "env-both-reversed"],
    )
    def test_host_beside_hostaddr_from_the_environment_has_no_default(
        self,
        server_table: str,
        no_server_env: None,
        monkeypatch: pytest.MonkeyPatch,
        dsn_params: dict[str, str],
        env: dict[str, str],
    ) -> None:
        _requires_localhost()
        for envvar, value in env.items():
            monkeypatch.setenv(envvar, value)
        dsn = _keyword_dsn(server_table, **{"host": None, **dsn_params})
        with pytest.raises(ValueError, match="host name alone does not identify"):
            _source_id(dsn)

    @pytest.mark.parametrize(
        ("service", "dsn_params"),
        [
            ("host=localhost\nhostaddr=127.0.0.1", {}),
            ("hostaddr=127.0.0.1\nhost=localhost", {}),
            ("host=localhost", {"hostaddr": "127.0.0.1"}),
        ],
        ids=["service-both", "service-both-reversed", "service-host-dsn-addr"],
    )
    def test_host_beside_hostaddr_from_a_service_file_has_no_default(
        self,
        server_table: str,
        no_server_env: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        service: str,
        dsn_params: dict[str, str],
    ) -> None:
        _requires_localhost()
        service_file = tmp_path / "pg_service.conf"
        service_file.write_text(f"[one]\n{service}\nport={_SERVER['port']}\n")
        monkeypatch.setenv("PGSERVICEFILE", str(service_file))
        dsn = _keyword_dsn(
            server_table, **{"host": None, "port": None, "service": "one", **dsn_params}
        )
        with pytest.raises(ValueError, match="host name alone does not identify"):
            _source_id(dsn)

    @pytest.mark.parametrize(
        ("dsn_host", "env_host"),
        [("localhost", None), (None, "localhost")],
        ids=["dsn-host", "pghost"],
    )
    def test_host_name_beside_a_service_file_hostaddr_keeps_the_default(
        self,
        server_table: str,
        no_server_env: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        dsn_host: str | None,
        env_host: str | None,
    ) -> None:
        # psycopg resolves a host name of the DSN or PGHOST and passes the address it
        # reached as hostaddr, which overrides the service file's: the connection goes
        # to the named host, not to the service file's unreachable address (a wrong
        # claim would fail at connect_timeout).
        _requires_localhost()
        service_file = tmp_path / "pg_service.conf"
        service_file.write_text(f"[one]\nhostaddr=192.0.2.1\nport={_SERVER['port']}\n")
        monkeypatch.setenv("PGSERVICEFILE", str(service_file))
        if env_host is not None:
            monkeypatch.setenv("PGHOST", env_host)
        dsn = _keyword_dsn(
            server_table, host=dsn_host, port=None, service="one", connect_timeout="5"
        )
        assert _source_id(dsn) == _expected("localhost", server_table)

    @pytest.mark.parametrize(
        ("service_host", "env_host"),
        [("localhost", "127.0.0.1"), ("127.0.0.1", "localhost")],
        ids=["service-name-env-address", "service-address-env-name"],
    )
    def test_service_file_host_beside_a_resolved_pghost_has_no_default(
        self,
        server_table: str,
        no_server_env: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        service_host: str,
        env_host: str,
    ) -> None:
        # libpq takes the service file's host over PGHOST, while psycopg resolved
        # PGHOST and sent the connection to that address: the reported host does not
        # name the server reached.
        _requires_localhost()
        service_file = tmp_path / "pg_service.conf"
        service_file.write_text(f"[one]\nhost={service_host}\nport={_SERVER['port']}\n")
        monkeypatch.setenv("PGSERVICEFILE", str(service_file))
        monkeypatch.setenv("PGHOST", env_host)
        dsn = _keyword_dsn(server_table, host=None, port=None, service="one")
        with pytest.raises(ValueError, match="host name alone does not identify"):
            _source_id(dsn)

    def test_host_from_a_service_file_names_the_host(
        self,
        server_table: str,
        no_server_env: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        _requires_localhost()
        service_file = tmp_path / "pg_service.conf"
        service_file.write_text(f"[one]\nhost=LocalHost\nport={_SERVER['port']}\n")
        monkeypatch.setenv("PGSERVICEFILE", str(service_file))
        dsn = _keyword_dsn(server_table, host=None, port=None, service="one")
        assert _source_id(dsn) == _expected("localhost", server_table)

    def test_ipv6_host_is_bracketed_and_exploded(self, server_table: str) -> None:
        _requires_localhost()
        dsn = _keyword_dsn(server_table, host="::1")
        try:
            psycopg.connect(dsn).close()
        except psycopg.OperationalError:
            pytest.skip("the test server is not reachable over IPv6")
        assert _source_id(dsn) == _expected(
            "[0000:0000:0000:0000:0000:0000:0000:0001]", server_table
        )

    @pytest.mark.parametrize("hosts", ["localhost,127.0.0.1", "127.0.0.1,localhost"])
    def test_several_hosts_in_the_dsn_have_no_default(
        self, server_table: str, hosts: str
    ) -> None:
        _requires_localhost()
        from orcapod.core.sources import PostgreSQLTableSource

        dsn = _keyword_dsn(server_table, host=hosts)
        with pytest.raises(ValueError, match="several servers"):
            _source_id(dsn)
        given = PostgreSQLTableSource(
            dsn, "t", source_id="t_on_the_pool", tag_only_columns=True
        )
        assert given.source_id == "t_on_the_pool"

    def test_several_hosts_from_pghost_have_no_default(
        self, server_table: str, no_server_env: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _requires_localhost()
        monkeypatch.setenv("PGHOST", "localhost,127.0.0.1")
        with pytest.raises(ValueError, match="several servers"):
            _source_id(_keyword_dsn(server_table, host=None))

    def test_several_hosts_from_a_service_file_have_no_default(
        self,
        server_table: str,
        no_server_env: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        _requires_localhost()
        service_file = tmp_path / "pg_service.conf"
        service_file.write_text(
            f"[pool]\nhost=localhost,127.0.0.1\nport={_SERVER['port']}\n"
        )
        monkeypatch.setenv("PGSERVICEFILE", str(service_file))
        dsn = _keyword_dsn(server_table, host=None, port=None, service="pool")
        with pytest.raises(ValueError, match="several servers"):
            _source_id(dsn)
