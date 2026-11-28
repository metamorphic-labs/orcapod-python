"""Tests for PostgresHashCacher against a PostgreSQL server.

These tests are marked ``@pytest.mark.postgres`` and are **skipped by default**.
To run them, pass ``--postgres``::

    uv run pytest tests/test_hashing/test_postgres_hash_cacher.py --postgres

When ``PGHOST`` is set, the fixture uses the configured PostgreSQL server and
needs permission to create schemas. Otherwise it uses Docker to start a
``postgres:16`` container. Each test puts its own schema first in the search
path, followed by ``pg_catalog``.
Tests without the postgres marker run without a server.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from typing import cast
from unittest.mock import Mock
from uuid import uuid4

import pytest
from typing_extensions import override
from upath import UPath

psycopg = pytest.importorskip("psycopg")

from psycopg import Connection, connect, errors, sql  # noqa: E402
from psycopg.conninfo import make_conninfo  # noqa: E402
from testcontainers.postgres import PostgresContainer  # noqa: E402

from orcapod.hashing.file_hashers import FileHashKey  # noqa: E402
from orcapod.hashing.postgres_hash_cacher import PostgresHashCacher  # noqa: E402
from orcapod.types import ContentHash  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_key(
    path: str = "/a/b.txt", mtime_ns: int = 1000, size: int = 100
) -> FileHashKey:
    return FileHashKey(
        path=UPath(path), mtime_ns=mtime_ns, size=size, algorithm="sha256"
    )


def make_hash(digest: bytes = b"\xab" * 32) -> ContentHash:
    return ContentHash(method="sha256", digest=digest)


def test_identical_put_accepts_bytes_from_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Accept a stored TEXT label returned as bytes by the driver."""
    value = make_hash()
    insert = Mock(rowcount=0)
    select = Mock(fetchone=Mock(return_value=(b"sha256", value.to_prefixed_digest())))
    execute = Mock(side_effect=[insert, select])
    conn = Mock(execute=execute, transaction=Mock(return_value=nullcontext()))
    monkeypatch.setattr(
        "orcapod.hashing.postgres_hash_cacher.psycopg.connect",
        Mock(side_effect=[nullcontext(Mock()), conn]),
    )

    with PostgresHashCacher("") as cacher:
        cacher.put(make_key(), value)

    assert execute.call_count == 2


class _VersionedContentHash(ContentHash):
    @override
    def to_prefixed_digest(self) -> bytes:
        return b"v2:" + super().to_prefixed_digest()


class _CustomEncodingLabel(str):
    @override
    def encode(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        return b"custom"


class _ChangingAlgorithmKey(FileHashKey):
    _algorithm_reads: int = 0

    @override
    def __getattribute__(self, name: str) -> object:
        if name == "algorithm":
            reads = self._algorithm_reads
            object.__setattr__(self, "_algorithm_reads", reads + 1)
            return "sha256" if reads < 2 else "sha3_256"
        return super().__getattribute__(name)


def _insert_raw(pg_conninfo: str, key: FileHashKey, blob: bytes) -> None:
    with psycopg.connect(pg_conninfo) as conn:
        conn.execute(
            "INSERT INTO file_hash_cache_v2 (path, mtime_ns, size, algorithm, hash, cached_at) "
            "VALUES (%s, %s, %s, %s, %s, 1)",
            (str(key.path), key.mtime_ns, key.size, key.algorithm, blob),
        )


def _assert_resolved_table_schema(
    conn: Connection[tuple[object, ...]], *tables: str
) -> None:
    for table in tables:
        assert conn.execute(
            "SELECT relnamespace = to_regnamespace(current_schema()) "
            "FROM pg_catalog.pg_class WHERE oid = to_regclass(%s)",
            (table,),
        ).fetchone() == (True,), f"{table} must resolve to the test schema"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pg_server_conninfo() -> Iterator[str]:
    """Use the CI service when configured or start one container for the module."""
    if os.environ.get("PGHOST"):
        yield ""  # libpq reads the PG* environment variables.
        return
    with PostgresContainer("postgres:16") as pg:
        yield (
            f"host={pg.get_container_host_ip()} "
            f"port={pg.get_exposed_port(5432)} "
            f"dbname={pg.dbname} "
            f"user={pg.username} "
            f"password={pg.password}"
        )


@pytest.fixture()
def pg_schema_conninfo(pg_server_conninfo: str) -> Iterator[str]:
    """Give each test an empty schema ahead of the PostgreSQL catalog."""
    schema = f"orcapod_hash_cache_test_{uuid4().hex}"
    identifier = sql.Identifier(schema)
    conninfo = psycopg.conninfo.make_conninfo(
        pg_server_conninfo, options=f"-c search_path={schema},pg_catalog"
    )
    with psycopg.connect(pg_server_conninfo) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(identifier))
    try:
        with psycopg.connect(conninfo) as conn:
            assert conn.execute("SELECT current_schemas(true)").fetchone() == (
                [schema, "pg_catalog"],
            )
        yield conninfo
    finally:
        with psycopg.connect(pg_server_conninfo) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(identifier))


@pytest.fixture()
def pg_conninfo(pg_schema_conninfo: str, request: pytest.FixtureRequest) -> str:
    """Initialize and verify the cache tables before a test can write to them."""
    conninfo = pg_schema_conninfo
    if hasattr(request, "param"):
        conninfo = make_conninfo(conninfo, client_encoding=cast(str, request.param))
    PostgresHashCacher(conninfo).close()
    with connect(conninfo) as conn:
        _assert_resolved_table_schema(
            conn, "file_hash_cache_v2", "file_hash_cache_v2_meta"
        )
    return conninfo


@pytest.fixture()
def cacher(pg_conninfo: str) -> Iterator[PostgresHashCacher]:
    """Open the cache in the test's own schema."""
    with PostgresHashCacher(pg_conninfo) as c:
        yield c


@pytest.fixture()
def public_cache_sentinels(pg_server_conninfo: str) -> Iterator[None]:
    """Seed public tables owned by this test and check them after schema teardown."""
    with psycopg.connect(pg_server_conninfo) as conn:
        available = conn.execute(
            """
            SELECT has_schema_privilege(to_regnamespace('public'), 'CREATE')
                AND to_regclass('public.file_hash_cache') IS NULL
                AND to_regclass('public.file_hash_cache_v2') IS NULL
            """
        ).fetchone()
        if available != (True,):
            pytest.skip("The public schema cannot be used for test-owned cache tables")
        conn.execute("CREATE TABLE public.file_hash_cache (sentinel integer)")
        conn.execute("CREATE TABLE public.file_hash_cache_v2 (sentinel integer)")
        conn.execute("INSERT INTO public.file_hash_cache VALUES (1)")
        conn.execute("INSERT INTO public.file_hash_cache_v2 VALUES (2)")
    try:
        yield
        with psycopg.connect(pg_server_conninfo) as conn:
            assert conn.execute("SELECT * FROM public.file_hash_cache").fetchall() == [
                (1,)
            ]
            assert conn.execute(
                "SELECT * FROM public.file_hash_cache_v2"
            ).fetchall() == [(2,)]
    finally:
        with psycopg.connect(pg_server_conninfo) as conn:
            conn.execute("DROP TABLE public.file_hash_cache, public.file_hash_cache_v2")


# ---------------------------------------------------------------------------
# Basic operations
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherBasic:
    def test_miss_returns_none(self, cacher):
        assert cacher.get(make_key()) is None

    def test_put_then_get_returns_hit(self, cacher):
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        result = cacher.get(key)
        assert result is not None
        assert result.method == value.method
        assert result.digest == value.digest

    def test_different_path_is_miss(self, cacher):
        cacher.put(make_key("/a/b.txt"), make_hash(b"\xaa" * 32))
        assert cacher.get(make_key("/a/c.txt")) is None

    def test_different_mtime_ns_is_miss(self, cacher):
        cacher.put(make_key(mtime_ns=1000), make_hash(b"\xaa" * 32))
        assert cacher.get(make_key(mtime_ns=2000)) is None

    def test_different_size_is_miss(self, cacher):
        cacher.put(make_key(size=100), make_hash(b"\xaa" * 32))
        assert cacher.get(make_key(size=200)) is None

    def test_clear_empties_table(self, cacher):
        key = make_key()
        cacher.put(key, make_hash())
        cacher.clear()
        assert cacher.get(key) is None

    def test_persistence_across_instances(self, pg_conninfo):
        key = make_key("/persist.txt", mtime_ns=9999, size=512)
        value = make_hash(b"\xcc" * 32)

        cacher1 = PostgresHashCacher(pg_conninfo)
        cacher1.put(key, value)
        cacher1.close()

        cacher2 = PostgresHashCacher(pg_conninfo)
        result = cacher2.get(key)
        cacher2.close()

        assert result is not None
        assert result.method == value.method
        assert result.digest == value.digest

    def test_context_manager_closes_connection(self, pg_conninfo):
        key = make_key("/ctx.txt", mtime_ns=7777, size=64)
        value = make_hash(b"\xdd" * 32)

        with PostgresHashCacher(pg_conninfo) as c:
            c.put(key, value)

        # Verify persistence (connection was closed but data committed)
        cacher2 = PostgresHashCacher(pg_conninfo)
        result = cacher2.get(key)
        cacher2.close()
        assert result is not None
        assert result.digest == value.digest


# ---------------------------------------------------------------------------
# Write guards
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherReadOnly:
    def test_put_is_noop_when_read_only(self, pg_conninfo):
        key = make_key("/ro.txt")
        value = make_hash()

        cacher = PostgresHashCacher(pg_conninfo, read_only=True)
        cacher.put(key, value)
        assert cacher.get(key) is None
        cacher.close()

    def test_read_only_can_read_preexisting(self, pg_conninfo):
        key = make_key("/ro_pre.txt", mtime_ns=5555, size=256)
        value = make_hash(b"\xee" * 32)

        writable = PostgresHashCacher(pg_conninfo)
        writable.put(key, value)
        writable.close()

        read_only = PostgresHashCacher(pg_conninfo, read_only=True)
        result = read_only.get(key)
        read_only.close()

        assert result is not None
        assert result.digest == value.digest

    def test_default_is_not_read_only(self, cacher):
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None


@pytest.mark.postgres
class TestPostgresHashCacherThreshold:
    def test_small_file_not_cached(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=100)
        key = make_key(size=50)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None
        cacher.close()

    def test_file_at_threshold_is_cached(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=100)
        key = make_key(size=100)
        value = make_hash()
        cacher.put(key, value)
        result = cacher.get(key)
        cacher.close()
        assert result is not None
        assert result.digest == value.digest

    def test_large_file_is_cached(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=100)
        key = make_key(size=200)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None
        cacher.close()

    def test_none_threshold_caches_all(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=None)
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None
        cacher.close()

    def test_zero_threshold_caches_all(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=0)
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None
        cacher.close()


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherConcurrency:
    @pytest.mark.parametrize("pg_conninfo", ["UTF8", "SQL_ASCII"], indirect=True)
    @pytest.mark.parametrize("same_hash", [True, False])
    @pytest.mark.parametrize("malformed_existing", [False, True])
    def test_concurrent_puts(
        self,
        pg_conninfo: str,
        cacher: PostgresHashCacher,
        same_hash: bool,
        malformed_existing: bool,
        caplog: pytest.LogCaptureFixture,
    ):
        key = make_key("/concurrent.txt", mtime_ns=12345, size=999)
        if malformed_existing:
            _insert_raw(pg_conninfo, key, b"corrupt")
        values = (
            make_hash(b"\xff" * 32),
            make_hash(bytes([255 if same_hash else 127]) * 32),
        )
        barrier = threading.Barrier(2)

        def put(value: ContentHash) -> bool:
            try:
                barrier.wait(timeout=10)
                cacher.put(key, value)
            except ValueError as exc:
                assert "Different hash" in str(exc)
                return False
            finally:
                cacher.close()
            return True

        with ThreadPoolExecutor(max_workers=2) as pool:
            accepted = list(pool.map(put, values))
        assert sum(accepted) == (2 if same_hash else 1)
        assert cacher.get(key) == next(
            value for value, success in zip(values, accepted, strict=True) if success
        )
        with psycopg.connect(pg_conninfo) as conn:
            assert conn.execute(
                "SELECT COUNT(*) FROM file_hash_cache_v2 "
                "WHERE path=%s AND mtime_ns=%s AND size=%s",
                (str(key.path), key.mtime_ns, key.size),
            ).fetchone() == (1,)
        assert len(caplog.records) == int(malformed_existing)

    @pytest.mark.parametrize("pg_conninfo", ["UTF8", "SQL_ASCII"], indirect=True)
    def test_conflicting_hash_is_refused_and_identical_hash_is_a_noop(
        self, pg_conninfo: str, cacher: PostgresHashCacher
    ):
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        with psycopg.connect(pg_conninfo) as conn:
            conn.execute("UPDATE file_hash_cache_v2 SET cached_at=1")
        cacher.put(key, value)
        with pytest.raises(ValueError, match="Different hash"):
            cacher.put(key, make_hash(b"\x01" * 32))
        assert cacher.get(key) == value
        with psycopg.connect(pg_conninfo) as conn:
            assert conn.execute(
                "SELECT cached_at FROM file_hash_cache_v2"
            ).fetchone() == (1,)


# ---------------------------------------------------------------------------
# Parity with SqliteHashCacher
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherParity:
    def test_parity_with_sqlite(self, pg_conninfo, tmp_path):
        """Same key/value → equivalent ContentHash from both backends."""
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        key = make_key("/parity.txt", mtime_ns=111, size=222)
        value = make_hash(b"\x12" * 32)

        pg_cacher = PostgresHashCacher(pg_conninfo)
        pg_cacher.put(key, value)
        pg_result = pg_cacher.get(key)
        pg_cacher.close()

        sq_cacher = SqliteHashCacher(tmp_path / "parity.db")
        sq_cacher.put(key, value)
        sq_result = sq_cacher.get(key)

        assert pg_result is not None
        assert sq_result is not None
        assert pg_result.method == sq_result.method
        assert pg_result.digest == sq_result.digest


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherValidation:
    def test_negative_min_cache_size_bytes_raises(self, pg_conninfo):
        with pytest.raises(ValueError, match="non-negative"):
            PostgresHashCacher(pg_conninfo, min_cache_size_bytes=-1)

    def test_repr_includes_conninfo_and_guards(self, pg_conninfo):
        cacher = PostgresHashCacher(
            pg_conninfo, read_only=True, min_cache_size_bytes=1024
        )
        r = repr(cacher)
        assert "PostgresHashCacher" in r
        assert "read_only=True" in r
        assert "min_cache_size_bytes=1024" in r
        cacher.close()


# ---------------------------------------------------------------------------
# enable_file_hash_caching(conninfo=...) integration
# ---------------------------------------------------------------------------


@pytest.fixture()
def restore_default_file_handler():
    """Restore the default FileHandler and DirectoryHandler after each test."""
    from orcapod.contexts import get_default_context
    from orcapod.logical_types.directory_type import Directory
    from orcapod.logical_types.file_type import File

    context = get_default_context()
    registry = context.semantic_hasher.type_handler_registry
    original_file_handler = registry.get_handler_for_type(File)
    original_dir_handler = registry.get_handler_for_type(Directory)
    yield
    registry.register(File, original_file_handler)
    registry.register(Directory, original_dir_handler)


@pytest.mark.postgres
class TestEnableFileHashCachingPostgres:
    def test_conninfo_activates_postgres_cacher(
        self, restore_default_file_handler, pg_conninfo
    ):
        """enable_file_hash_caching(conninfo=...) wires up PostgresHashCacher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.logical_types.file_type import File

        enable_file_hash_caching(conninfo=pg_conninfo)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        assert isinstance(handler.file_hasher.cacher, PostgresHashCacher)

    def test_conninfo_passes_read_only(self, restore_default_file_handler, pg_conninfo):
        """read_only=True is forwarded to PostgresHashCacher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.logical_types.file_type import File

        enable_file_hash_caching(conninfo=pg_conninfo, read_only=True)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        assert handler.file_hasher.cacher._read_only is True

    def test_conninfo_passes_min_cache_size_bytes(
        self, restore_default_file_handler, pg_conninfo
    ):
        """min_cache_size_bytes is forwarded to PostgresHashCacher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.logical_types.file_type import File

        enable_file_hash_caching(conninfo=pg_conninfo, min_cache_size_bytes=2048)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        assert handler.file_hasher.cacher._min_cache_size_bytes == 2048


# ---------------------------------------------------------------------------
# Schema version and repr
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresSchemaVersion:
    def test_fixture_resolves_its_test_schema_before_pg_catalog(self, pg_conninfo: str):
        with psycopg.connect(pg_conninfo) as conn:
            row = conn.execute(
                "SELECT current_schema(), current_schemas(true)"
            ).fetchone()
        assert row is not None
        schema, search_path = row
        assert isinstance(schema, str) and schema.startswith("orcapod_hash_cache_test_")
        assert search_path == [schema, "pg_catalog"]
        with psycopg.connect(pg_conninfo) as conn:
            assert conn.execute(
                "SELECT relnamespace = to_regnamespace(%s) FROM pg_class "
                "WHERE oid = to_regclass('file_hash_cache_v2')",
                (schema,),
            ).fetchone() == (True,)

    @pytest.mark.usefixtures("public_cache_sentinels")
    def test_fixture_isolates_public_cache_tables(self, request: pytest.FixtureRequest):
        # Create the isolated schema after the public tables, so its cleanup runs first.
        conninfo = cast(str, request.getfixturevalue("pg_conninfo"))
        with psycopg.connect(conninfo) as conn:
            assert conn.execute(
                "SELECT to_regclass('file_hash_cache') IS DISTINCT FROM "
                "to_regclass('public.file_hash_cache')"
            ).fetchone() == (True,)
            _assert_resolved_table_schema(conn, "file_hash_cache_v2")
        with PostgresHashCacher(conninfo) as cacher:
            key = make_key()
            value = make_hash()
            assert cacher.get(key) is None
            cacher.put(key, value)
            assert cacher.get(key) == value
            cacher.clear()
            assert cacher.get(key) is None

    def test_meta_table_contains_schema_version(self, pg_conninfo: str):
        PostgresHashCacher(pg_conninfo).close()

        with psycopg.connect(pg_conninfo) as conn:
            row = conn.execute(
                "SELECT value FROM file_hash_cache_v2_meta WHERE key = 'schema_version'"
            ).fetchone()
        assert row == ("2",)

    def test_old_cache_is_preserved_beside_new_table(self, pg_schema_conninfo: str):
        pg_conninfo = pg_schema_conninfo
        key = make_key()
        old_hash = make_hash()
        new_hash = make_hash(b"\x01" * 32)
        with psycopg.connect(pg_conninfo) as conn:
            conn.execute(
                """
                CREATE TABLE file_hash_cache (
                    path TEXT NOT NULL, mtime_ns BIGINT NOT NULL, size BIGINT NOT NULL,
                    hash BYTEA NOT NULL,
                    cached_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
                    PRIMARY KEY (path, mtime_ns, size)
                )
                """
            )
            _assert_resolved_table_schema(conn, "file_hash_cache")
            conn.execute(
                "INSERT INTO file_hash_cache VALUES (%s, %s, %s, %s, %s)",
                (
                    str(key.path),
                    key.mtime_ns,
                    key.size,
                    old_hash.to_prefixed_digest(),
                    42,
                ),
            )
            conn.execute(
                "CREATE INDEX idx_file_hash_cache_path_size_mtime "
                "ON file_hash_cache (path, size, mtime_ns DESC)"
            )
            conn.execute(
                "CREATE TABLE file_hash_cache_meta (key TEXT PRIMARY KEY, value TEXT)"
            )
            _assert_resolved_table_schema(conn, "file_hash_cache_meta")
            conn.execute(
                "INSERT INTO file_hash_cache_meta VALUES ('schema_version', '1')"
            )

        def old_snapshot() -> tuple[object, ...]:
            with psycopg.connect(pg_conninfo) as conn:
                return (
                    conn.execute(
                        "SELECT *, ctid::text, xmin::text FROM file_hash_cache ORDER BY path"
                    ).fetchall(),
                    conn.execute(
                        "SELECT *, ctid::text, xmin::text FROM file_hash_cache_meta ORDER BY key"
                    ).fetchall(),
                    conn.execute(
                        "SELECT a.attname, a.atttypid, a.attnotnull, pg_get_expr(d.adbin, d.adrelid) "
                        "FROM pg_attribute AS a LEFT JOIN pg_attrdef AS d "
                        "ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
                        "WHERE a.attrelid='file_hash_cache'::regclass AND a.attnum > 0 "
                        "ORDER BY a.attnum"
                    ).fetchall(),
                    conn.execute(
                        "SELECT indexdef FROM pg_indexes "
                        "WHERE schemaname=current_schema() AND tablename='file_hash_cache' "
                        "ORDER BY indexname",
                    ).fetchall(),
                )

        original = old_snapshot()
        with PostgresHashCacher(pg_conninfo) as c:
            with psycopg.connect(pg_conninfo) as conn:
                _assert_resolved_table_schema(
                    conn, "file_hash_cache_v2", "file_hash_cache_v2_meta"
                )
            assert c.get(key) is None
            c.put(key, new_hash)
            assert c.get(key) == new_hash
            c.clear()
            assert c.get(key) is None
        assert old_snapshot() == original

        with psycopg.connect(pg_conninfo) as conn:
            assert conn.execute(
                "SELECT count(*) FROM file_hash_cache_v2"
            ).fetchone() == (0,)


# ---------------------------------------------------------------------------
# match_mtime flag
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherMatchMtime:
    def test_default_true_mtime_change_causes_miss(self, pg_conninfo):
        """Default (match_mtime=True): different mtime → cache miss."""
        cacher = PostgresHashCacher(pg_conninfo)
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash(b"\xaa" * 32))
        assert cacher.get(make_key(mtime_ns=2000, size=100)) is None
        cacher.close()

    def test_false_mtime_change_is_hit(self, pg_conninfo):
        """match_mtime=False: different mtime, same path+size → cache hit."""
        cacher = PostgresHashCacher(pg_conninfo, match_mtime=False)
        value = make_hash(b"\xbb" * 32)
        cacher.put(make_key(mtime_ns=1000, size=100), value)
        result = cacher.get(make_key(mtime_ns=2000, size=100))
        assert result is not None
        assert result.digest == value.digest
        cacher.close()

    def test_false_size_change_is_miss(self, pg_conninfo):
        """match_mtime=False: different size → cache miss."""
        cacher = PostgresHashCacher(pg_conninfo, match_mtime=False)
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash())
        assert cacher.get(make_key(mtime_ns=2000, size=200)) is None
        cacher.close()

    def test_false_returns_latest_mtime_entry(self, pg_conninfo):
        """match_mtime=False: multiple entries for same path+size → returns highest mtime_ns."""
        cacher = PostgresHashCacher(pg_conninfo, match_mtime=False)
        hash_old = make_hash(b"\xaa" * 32)
        hash_new = make_hash(b"\xbb" * 32)
        cacher.put(make_key(mtime_ns=1000, size=100), hash_old)
        cacher.put(make_key(mtime_ns=2000, size=100), hash_new)
        result = cacher.get(make_key(mtime_ns=3000, size=100))
        assert result is not None
        assert result.digest == hash_new.digest
        cacher.close()

    def test_repr_shows_match_mtime_true(self, pg_conninfo):
        """__repr__ includes match_mtime=True by default."""
        cacher = PostgresHashCacher(pg_conninfo)
        assert "match_mtime=True" in repr(cacher)
        cacher.close()

    def test_repr_shows_match_mtime_false(self, pg_conninfo):
        """__repr__ includes match_mtime=False."""
        cacher = PostgresHashCacher(pg_conninfo, match_mtime=False)
        assert "match_mtime=False" in repr(cacher)
        cacher.close()


class TestPostgresReprRedaction:
    def test_repr_redacts_url_password(self):
        """__repr__ replaces the password in a URL-form conninfo with ***."""
        from orcapod.hashing.postgres_hash_cacher import _redact_conninfo

        raw = "postgresql://alice:s3cr3t@db.example.com:5432/orcapod"
        redacted = _redact_conninfo(raw)
        assert "s3cr3t" not in redacted
        assert "alice" in redacted
        assert "***" in redacted

    def test_repr_redacts_keyword_password(self):
        """__repr__ replaces the password in a keyword-form conninfo with ***."""
        from orcapod.hashing.postgres_hash_cacher import _redact_conninfo

        raw = "host=db.example.com dbname=orcapod user=alice password=s3cr3t"
        redacted = _redact_conninfo(raw)
        assert "s3cr3t" not in redacted
        assert "***" in redacted

    def test_repr_no_password_unchanged(self):
        """__repr__ does not alter a conninfo string that has no password."""
        from orcapod.hashing.postgres_hash_cacher import _redact_conninfo

        raw = "host=localhost dbname=orcapod user=alice"
        assert _redact_conninfo(raw) == raw


# ---------------------------------------------------------------------------
# sha256 is the only file hash algorithm (ITL-1488, ITL-1155)
# ---------------------------------------------------------------------------


_SIZES = {"sha256": 32, "md5": 16}


def _key_for_algorithm(algorithm: str, mtime_ns: int = 1000) -> FileHashKey:
    return FileHashKey(
        path=UPath("/a/b.txt"), mtime_ns=mtime_ns, size=100, algorithm=algorithm
    )


def _all_rows(pg_conninfo: str) -> list[tuple[object, ...]]:
    with connect(pg_conninfo) as conn:
        return conn.execute(
            "SELECT path, mtime_ns, size, algorithm, hash, cached_at "
            "FROM file_hash_cache_v2 ORDER BY algorithm, mtime_ns"
        ).fetchall()


@pytest.mark.postgres
class TestPostgresHashCacherAlgorithmLabel:
    @pytest.mark.parametrize("match_mtime", [True, False])
    def test_row_of_another_algorithm_stays_and_is_never_read(
        self, pg_conninfo: str, match_mtime: bool
    ):
        """A row an earlier version stored under md5: untouched, never returned."""
        old_key = _key_for_algorithm("md5", mtime_ns=2000)
        _insert_raw(pg_conninfo, old_key, b"md5:" + b"\x07" * 16)
        before = _all_rows(pg_conninfo)
        value = ContentHash("sha256", b"\x01" * 32)
        with PostgresHashCacher(pg_conninfo, match_mtime=match_mtime) as c:
            assert c.get(old_key) is None
            assert c.get(_key_for_algorithm("sha256", mtime_ns=2000)) is None
            c.put(_key_for_algorithm("sha256"), value)
            assert c.get(_key_for_algorithm("sha256")) == value
            assert c.get(old_key) is None
            # Without the mtime match, the newer md5 row is skipped.
            got = c.get(_key_for_algorithm("sha256", mtime_ns=3000))
            assert got == (None if match_mtime else value)
        after = _all_rows(pg_conninfo)
        assert [row for row in after if row[3] != "sha256"] == before
        assert len(after) == 2

    @pytest.mark.parametrize("match_mtime", [True, False])
    def test_put_under_another_algorithm_is_refused(
        self, pg_conninfo: str, match_mtime: bool
    ):
        """A well-formed md5 hash under an md5 key is refused, naming md5."""
        with PostgresHashCacher(pg_conninfo, match_mtime=match_mtime) as c:
            with pytest.raises(
                ValueError,
                match="Unsupported file hash algorithm 'md5': sha256 is the only",
            ):
                c.put(_key_for_algorithm("md5"), ContentHash("md5", b"\x01" * 16))
            assert c.get(_key_for_algorithm("md5")) is None
            assert c.get(_key_for_algorithm("sha256")) is None
        assert _all_rows(pg_conninfo) == []


_CRAFTED_BLOBS = [
    pytest.param(b"sha256:" + b"\x07" * 25, id="raw-32-byte-blob-starting-with-prefix"),
    pytest.param(b"sha256:" + b"\x07" * 33, id="digest-too-long"),
    pytest.param(b"sha256:", id="empty-digest"),
    pytest.param(b"sha256" + b"\x07" * 32, id="no-separator"),
    pytest.param(b"md5:" + b"\x07" * 32, id="other-label"),
]


@pytest.mark.postgres
class TestPostgresHashCacherStrictStore:
    @pytest.mark.parametrize("pg_conninfo", ["UTF8", "SQL_ASCII"], indirect=True)
    @pytest.mark.parametrize("match_mtime", [True, False])
    def test_rejects_a_conflict_from_another_label(
        self, pg_conninfo: str, match_mtime: bool, caplog: pytest.LogCaptureFixture
    ):
        with connect(pg_conninfo) as conn:
            try:
                _ = conn.execute(
                    "CREATE COLLATION algorithm_ci "
                    "(provider=icu, locale='und-u-ks-level2', deterministic=false)"
                )
            except errors.FeatureNotSupported as exc:
                pytest.skip(f"ICU collation unavailable: {exc}")
            conn.execute(
                "ALTER TABLE file_hash_cache_v2 "
                "ALTER COLUMN algorithm TYPE TEXT COLLATE algorithm_ci"
            )
        # A stray row whose label the collation equates with the key's label.
        key = make_key()
        stray = FileHashKey(key.path, key.mtime_ns, key.size, "SHA256")
        blob = b"SHA256:" + b"\x07" * 32
        _insert_raw(pg_conninfo, stray, blob)
        with PostgresHashCacher(pg_conninfo, match_mtime=match_mtime) as cache:
            assert cache.get(key) is None
            with pytest.raises(ValueError, match="matched a row with algorithm label"):
                cache.put(key, make_hash())
            # The stray label itself is not a file hash algorithm.
            with pytest.raises(ValueError, match="'SHA256'.*sha256 is the only"):
                cache.put(stray, ContentHash("SHA256", b"\x02" * 32))
            assert cache.get(stray) is None
        with connect(pg_conninfo) as conn:
            assert conn.execute(
                "SELECT convert_to(algorithm, 'UTF8'), hash, cached_at FROM file_hash_cache_v2"
            ).fetchall() == [(b"SHA256", blob, 1)]
        assert not caplog.records

    @pytest.mark.parametrize("match_mtime", [True, False])
    @pytest.mark.parametrize(
        ("key_type", "label"),
        [
            pytest.param(
                FileHashKey, _CustomEncodingLabel("sha256"), id="custom-encoding"
            ),
            pytest.param(_ChangingAlgorithmKey, "sha256", id="changing-algorithm"),
        ],
    )
    def test_reads_with_a_canonical_algorithm_snapshot(
        self,
        pg_conninfo: str,
        match_mtime: bool,
        key_type: type[FileHashKey],
        label: str,
    ):
        key = make_key()
        lookup_key = key_type(key.path, key.mtime_ns, key.size, label)
        with PostgresHashCacher(pg_conninfo, match_mtime=match_mtime) as cache:
            original = make_hash()
            cache.put(key, original)
            assert cache.get(lookup_key) == original

    @pytest.mark.parametrize("match_mtime", [True, False])
    @pytest.mark.parametrize("same_digest", [True, False])
    def test_preserves_valid_rows_with_custom_serialization(
        self,
        pg_conninfo: str,
        match_mtime: bool,
        same_digest: bool,
        caplog: pytest.LogCaptureFixture,
    ):
        key = make_key()
        original = make_hash()
        incoming = _VersionedContentHash(
            "sha256", original.digest if same_digest else b"\x01" * 32
        )
        blob = original.to_prefixed_digest()
        with PostgresHashCacher(pg_conninfo, match_mtime=match_mtime) as cache:
            _insert_raw(pg_conninfo, key, blob)
            if same_digest:
                cache.put(key, incoming)
            else:
                with pytest.raises(ValueError, match="Different hash"):
                    cache.put(key, incoming)
            assert cache.get(key) == original
        with connect(pg_conninfo) as conn:
            assert conn.execute(
                "SELECT hash, cached_at FROM file_hash_cache_v2"
            ).fetchall() == [(blob, 1)]
        assert not caplog.records

    @pytest.mark.parametrize("match_mtime", [True, False])
    @pytest.mark.parametrize(
        ("key_label", "value_label"), [("sha256", "md5"), ("md5", "sha256")]
    )
    def test_put_of_another_algorithms_hash_is_refused(
        self, pg_conninfo: str, match_mtime: bool, key_label: str, value_label: str
    ):
        c = PostgresHashCacher(pg_conninfo, match_mtime=match_mtime)
        try:
            value = ContentHash(value_label, b"\x01" * _SIZES[value_label])
            with pytest.raises(ValueError, match="label"):
                c.put(_key_for_algorithm(key_label), value)
            assert c.get(_key_for_algorithm(key_label)) is None
            assert c.get(_key_for_algorithm(value_label)) is None
        finally:
            c.close()

    def test_put_of_a_digest_of_the_wrong_length_is_refused(
        self, cacher: PostgresHashCacher
    ):
        with pytest.raises(ValueError, match="digest"):
            cacher.put(
                _key_for_algorithm("sha256"), ContentHash("sha256", b"\x01" * 31)
            )
        assert cacher.get(_key_for_algorithm("sha256")) is None

    @pytest.mark.parametrize("pg_conninfo", ["UTF8", "SQL_ASCII"], indirect=True)
    @pytest.mark.parametrize("match_mtime", [True, False])
    @pytest.mark.parametrize("blob", _CRAFTED_BLOBS)
    def test_repairs_a_malformed_stored_hash(
        self,
        pg_conninfo: str,
        match_mtime: bool,
        blob: bytes,
        caplog: pytest.LogCaptureFixture,
    ):
        with (
            psycopg.connect(pg_conninfo) as conn,
            PostgresHashCacher(pg_conninfo, match_mtime=match_mtime) as cacher,
        ):
            key = make_key()
            _insert_raw(pg_conninfo, key, blob)
            assert cacher.get(key) is None
            value = make_hash()
            before_row = conn.execute(
                "SELECT EXTRACT(EPOCH FROM clock_timestamp())::BIGINT"
            ).fetchone()
            cacher.put(key, _VersionedContentHash(value.method, value.digest))
            assert cacher.get(key) == value
            with pytest.raises(ValueError, match="Different hash"):
                cacher.put(key, make_hash(b"\x01" * 32))
            assert cacher.get(key) == value
            row = conn.execute(
                "SELECT cached_at, EXTRACT(EPOCH FROM clock_timestamp())::BIGINT "
                "FROM file_hash_cache_v2"
            ).fetchone()
        assert before_row is not None and row is not None
        (before,) = before_row
        cached_at, after = row
        assert isinstance(before, int) and isinstance(after, int)
        assert isinstance(cached_at, int)
        assert before <= cached_at <= after
        (warning,) = caplog.records
        assert warning.levelname == "WARNING"
        assert str(key.path) in warning.getMessage()
        assert key.algorithm in warning.getMessage()

    def test_well_formed_stored_hash_is_read(
        self, pg_conninfo: str, cacher: PostgresHashCacher
    ):
        with psycopg.connect(pg_conninfo) as conn:
            conn.execute(
                "INSERT INTO file_hash_cache_v2 (path, mtime_ns, size, algorithm, hash) "
                "VALUES (%s, %s, %s, %s, %s)",
                ("/a/b.txt", 1000, 100, "sha256", b"sha256:" + b"\x07" * 32),
            )
        assert cacher.get(_key_for_algorithm("sha256")) == ContentHash(
            "sha256", b"\x07" * 32
        )

    def test_concurrent_puts_with_refused_labels_never_reach_the_store(
        self, pg_conninfo: str
    ):
        """Concurrent sha256 puts succeed while md5 puts are refused; no md5 row."""
        PostgresHashCacher(pg_conninfo).close()
        values = {
            label: ContentHash(label, bytes([n]) * _SIZES[label])
            for n, label in ((1, "sha256"), (2, "md5"))
        }
        errors: list[Exception] = []
        refused: list[str] = []
        barrier = threading.Barrier(8)

        def write(label: str) -> None:
            # A worker that fails, also while connecting, aborts the barrier so
            # the others stop waiting and the error is reported (CodeRabbit, #594).
            c: PostgresHashCacher | None = None
            try:
                c = PostgresHashCacher(pg_conninfo)
                barrier.wait()
                for _ in range(20):
                    if label == "sha256":
                        c.put(_key_for_algorithm(label), values[label])
                        assert c.get(_key_for_algorithm(label)) == values[label]
                    else:
                        with pytest.raises(ValueError, match="sha256 is the only"):
                            c.put(_key_for_algorithm(label), values[label])
                        assert c.get(_key_for_algorithm(label)) is None
                        refused.append(label)
            except Exception as exc:
                barrier.abort()
                errors.append(exc)
            finally:
                if c is not None:
                    c.close()

        threads = [
            threading.Thread(target=write, args=(label,))
            for label in ("sha256", "md5") * 4
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert len(refused) == 80

        reader = PostgresHashCacher(pg_conninfo)
        try:
            found = {label: reader.get(_key_for_algorithm(label)) for label in values}
        finally:
            reader.close()
        assert found == {"sha256": values["sha256"], "md5": None}
        assert [row[3] for row in _all_rows(pg_conninfo)] == ["sha256"]


_INVALID_LABELS = [
    pytest.param("sha256\x00", id="embedded-nul"),
    pytest.param("shä256", id="non-ascii"),
    pytest.param("", id="empty"),
]


@pytest.mark.postgres
class TestPostgresHashCacherInvalidLabel:
    @pytest.mark.parametrize("match_mtime", [True, False])
    @pytest.mark.parametrize("label", _INVALID_LABELS)
    def test_put_raises_value_error_and_read_is_a_miss(
        self, pg_conninfo: str, match_mtime: bool, label: str
    ):
        c = PostgresHashCacher(pg_conninfo, match_mtime=match_mtime)
        try:
            with pytest.raises(ValueError):
                c.put(_key_for_algorithm(label), ContentHash(label, b"\x01" * 32))
            with psycopg.connect(pg_conninfo) as conn:
                conn.execute(
                    "INSERT INTO file_hash_cache_v2 (path, mtime_ns, size, algorithm, hash) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (
                        "/a/b.txt",
                        1000,
                        100,
                        label if "\x00" not in label else "sha256",
                        label.encode("utf-8") + b":" + b"\x07" * 32,
                    ),
                )
            assert c.get(_key_for_algorithm(label)) is None
        finally:
            c.close()
