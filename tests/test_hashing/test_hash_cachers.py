"""Tests for InMemoryHashCacher and SqliteHashCacher."""

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from threading import Barrier

import pytest
from upath import UPath

from orcapod.hashing.file_hashers import FileHashKey
from orcapod.types import ContentHash

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_key(path_str: str, mtime_ns: int = 1000, size: int = 100) -> FileHashKey:
    return FileHashKey(
        path=UPath(path_str), mtime_ns=mtime_ns, size=size, algorithm="sha256"
    )


def make_hash(method: str = "sha256", digest: bytes = b"\xab" * 32) -> ContentHash:
    return ContentHash(method=method, digest=digest)


# ---------------------------------------------------------------------------
# InMemoryHashCacher
# ---------------------------------------------------------------------------


class TestInMemoryHashCacher:
    def test_miss_returns_none(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher

        cacher = InMemoryHashCacher()
        assert cacher.get(make_key("/a/b.txt")) is None

    def test_put_then_get_returns_hit(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher

        cacher = InMemoryHashCacher()
        key = make_key("/a/b.txt")
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_different_mtime_is_different_key(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher

        cacher = InMemoryHashCacher()
        key1 = make_key("/a/b.txt", mtime_ns=1000)
        key2 = make_key("/a/b.txt", mtime_ns=2000)
        cacher.put(key1, make_hash(digest=b"\xaa" * 32))
        assert cacher.get(key2) is None

    def test_different_size_is_different_key(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher

        cacher = InMemoryHashCacher()
        key1 = make_key("/a/b.txt", size=100)
        key2 = make_key("/a/b.txt", size=200)
        cacher.put(key1, make_hash(digest=b"\xaa" * 32))
        assert cacher.get(key2) is None

    def test_clear_removes_all_entries(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher

        cacher = InMemoryHashCacher()
        key = make_key("/a/b.txt")
        cacher.put(key, make_hash())
        cacher.clear()
        assert cacher.get(key) is None


# ---------------------------------------------------------------------------
# SqliteHashCacher
# ---------------------------------------------------------------------------


class TestSqliteHashCacher:
    def test_miss_returns_none(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        cacher = SqliteHashCacher(tmp_path / "cache.db")
        assert cacher.get(make_key("/a/b.txt")) is None

    def test_put_then_get_returns_hit(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key = make_key("/a/b.txt")
        value = make_hash()
        cacher.put(key, value)
        result = cacher.get(key)
        assert result is not None
        assert result.method == value.method
        assert result.digest == value.digest

    def test_different_mtime_is_different_key(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key1 = make_key("/a/b.txt", mtime_ns=1000)
        key2 = make_key("/a/b.txt", mtime_ns=2000)
        cacher.put(key1, make_hash(digest=b"\xaa" * 32))
        assert cacher.get(key2) is None

    def test_different_size_is_different_key(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key1 = make_key("/a/b.txt", size=100)
        key2 = make_key("/a/b.txt", size=200)
        cacher.put(key1, make_hash(digest=b"\xaa" * 32))
        assert cacher.get(key2) is None

    def test_persistence_across_instances(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        key = make_key("/a/b.txt")
        value = make_hash()

        cacher1 = SqliteHashCacher(db)
        cacher1.put(key, value)
        cacher1.close()

        cacher2 = SqliteHashCacher(db)
        result = cacher2.get(key)
        assert result is not None
        assert result.method == value.method
        assert result.digest == value.digest

    def test_wal_mode_enabled(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        SqliteHashCacher(db)
        with sqlite3.connect(db) as conn:
            cursor = conn.execute("PRAGMA journal_mode")
            mode = cursor.fetchone()[0]
        assert mode == "wal"

    def test_clear_empties_table(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key = make_key("/a/b.txt")
        cacher.put(key, make_hash())
        cacher.clear()
        assert cacher.get(key) is None

    def test_env_var_path_override(self, tmp_path, monkeypatch):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "env_cache.db"
        monkeypatch.setenv("ORCAPOD_HASH_CACHE_DB", str(db))
        cacher = SqliteHashCacher()
        assert cacher.db_path == db

    def test_context_manager_closes_connection(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        with SqliteHashCacher(db) as cacher:
            cacher.put(make_key("/a/b.txt"), make_hash())
        # After __exit__, thread-local conn should be None — verify by
        # reopening and checking persistence still works
        cacher2 = SqliteHashCacher(db)
        assert cacher2.get(make_key("/a/b.txt")) is not None


# ---------------------------------------------------------------------------
# enable_file_hash_caching()
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


class TestEnableFileHashCaching:
    def test_registers_cached_file_hasher(self, restore_default_file_handler, tmp_path):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.logical_types.file_type import File
        from orcapod.hashing.file_hashers import CachedFileHasher

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        assert isinstance(handler.file_hasher, CachedFileHasher)

    def test_double_call_logs_warning_and_does_not_double_wrap(
        self, restore_default_file_handler, tmp_path, caplog
    ):
        import logging
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.logical_types.file_type import File
        from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        with caplog.at_level(logging.WARNING):
            enable_file_hash_caching(db_path=tmp_path / "cache2.db")

        assert "already has a CachedFileHasher" in caplog.text

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        # Outer is CachedFileHasher
        assert isinstance(handler.file_hasher, CachedFileHasher)
        # Inner (the base hasher) must NOT be a CachedFileHasher
        assert isinstance(handler.file_hasher.file_hasher, FileHasher)

    def test_preserves_base_hasher_algorithm(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.logical_types.file_type import File

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        # The base FileHasher should still use sha256 (from the default data context)
        assert handler.file_hasher.file_hasher.algorithm == "sha256"

    def test_directory_handler_uses_cached_file_hasher(
        self, restore_default_file_handler, tmp_path
    ):
        """After enable_file_hash_caching(), DirectoryHandler uses CachedFileHasher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.logical_types.directory_type import Directory
        from orcapod.hashing.file_hashers import CachedFileHasher

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        dir_handler = registry.get_handler_for_type(Directory)
        assert isinstance(dir_handler.directory_hasher.file_hasher, CachedFileHasher)

    def test_shared_cache_between_file_and_directory_handlers(
        self, restore_default_file_handler, tmp_path
    ):
        """FileHandler and DirectoryHandler share the exact same CachedFileHasher instance."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.logical_types.directory_type import Directory
        from orcapod.logical_types.file_type import File

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry

        file_hasher = registry.get_handler_for_type(File).file_hasher
        dir_file_hasher = registry.get_handler_for_type(
            Directory
        ).directory_hasher.file_hasher

        assert file_hasher is dir_file_hasher

    def test_read_only_kwarg_passes_through_to_cacher(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.logical_types.file_type import File
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        enable_file_hash_caching(db_path=tmp_path / "cache.db", read_only=True)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        cacher = handler.file_hasher.cacher

        assert isinstance(cacher, SqliteHashCacher)
        assert cacher._read_only is True

    def test_min_cache_size_bytes_kwarg_passes_through_to_cacher(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.logical_types.file_type import File
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        enable_file_hash_caching(
            db_path=tmp_path / "cache.db", min_cache_size_bytes=1024
        )

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        cacher = handler.file_hasher.cacher

        assert isinstance(cacher, SqliteHashCacher)
        assert cacher._min_cache_size_bytes == 1024

    def test_match_mtime_kwarg_passes_through_to_cacher(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.logical_types.file_type import File
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        enable_file_hash_caching(db_path=tmp_path / "cache.db", match_mtime=False)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        cacher = handler.file_hasher.cacher

        assert isinstance(cacher, SqliteHashCacher)
        assert cacher._match_mtime is False


class TestEnableFileHashCachingConninfo:
    def test_conninfo_and_db_path_raises(self, restore_default_file_handler, tmp_path):
        """Providing both conninfo and db_path raises ValueError."""
        from orcapod.contexts import enable_file_hash_caching

        with pytest.raises(ValueError, match="not both"):
            enable_file_hash_caching(
                db_path=tmp_path / "x.db",
                conninfo="postgresql://unused",
            )


# ---------------------------------------------------------------------------
# SqliteHashCacher schema version detection
# ---------------------------------------------------------------------------


class TestSqliteSchemaVersion:
    def test_concurrent_openers_can_enable_wal_on_a_legacy_db(self, tmp_path: Path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        key = make_key("/a/b.txt")
        value = make_hash()

        with ThreadPoolExecutor(max_workers=2) as pool:
            # Exercise multiple fresh databases because the lock race is timing-dependent.
            for attempt in range(35):
                db = tmp_path / f"legacy-{attempt}.db"
                with closing(sqlite3.connect(db)) as conn, conn:
                    assert conn.execute("PRAGMA journal_mode").fetchone() == ("delete",)
                    conn.execute("CREATE TABLE file_hash_cache (size INTEGER)")
                barrier = Barrier(2)

                def open_and_put() -> None:
                    barrier.wait(timeout=10)
                    with SqliteHashCacher(db) as cacher:
                        cacher.put(key, value)
                        assert cacher.get(key) == value

                first = pool.submit(open_and_put)
                second = pool.submit(open_and_put)
                first.result()
                second.result()

    def test_fresh_db_is_stamped_v2(self, tmp_path: Path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        SqliteHashCacher(db).close()

        with sqlite3.connect(db) as conn:
            assert conn.execute("PRAGMA user_version").fetchone() == (2,)

    @pytest.mark.parametrize("version", [0, 1])
    def test_old_cache_is_preserved_beside_new_table(
        self, tmp_path: Path, version: int
    ):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "old_cache.db"
        key = make_key("/old.txt")
        old_hash = make_hash(digest=b"\x01" * 32)
        new_hash = make_hash(digest=b"\x02" * 32)
        with sqlite3.connect(db) as conn:
            conn.execute(
                """
                CREATE TABLE file_hash_cache (
                    path      TEXT    NOT NULL,
                    mtime_ns  INTEGER NOT NULL,
                    size      INTEGER NOT NULL,
                    hash      BLOB    NOT NULL,
                    PRIMARY KEY (path, mtime_ns, size)
                ) WITHOUT ROWID
                """
            )
            if version == 1:
                conn.execute(
                    "ALTER TABLE file_hash_cache ADD COLUMN cached_at INTEGER NOT NULL DEFAULT 0"
                )
                conn.execute("PRAGMA user_version = 1")
            conn.execute(
                "INSERT INTO file_hash_cache (path, mtime_ns, size, hash) VALUES (?, ?, ?, ?)",
                (str(key.path), key.mtime_ns, key.size, old_hash.to_prefixed_digest()),
            )
            old_dump = tuple(conn.iterdump())

        with SqliteHashCacher(db) as cacher:
            assert cacher.get(key) is None
            cacher.put(key, new_hash)
            assert cacher.get(key) == new_hash
            cacher.clear()
            assert cacher.get(key) is None

        with sqlite3.connect(db) as conn:
            assert conn.execute("PRAGMA user_version").fetchone() == (2,)
            dump = tuple(conn.iterdump())
        assert any("CREATE TABLE file_hash_cache_v2 " in line for line in dump)
        assert (
            tuple(line for line in dump if "file_hash_cache_v2" not in line) == old_dump
        )

    def test_existing_v2_db_reopens_without_error(self, tmp_path: Path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        SqliteHashCacher(db).close()
        SqliteHashCacher(db).close()

    def test_put_sets_nonzero_cached_at(self, tmp_path: Path):
        """Fresh entries get the current epoch in the new table."""
        import time

        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        before = int(time.time()) - 1
        with SqliteHashCacher(db) as cacher:
            cacher.put(make_key("/new.txt"), make_hash())

        with sqlite3.connect(db) as conn:
            row = conn.execute(
                "SELECT cached_at FROM file_hash_cache_v2 WHERE path='/new.txt'"
            ).fetchone()
        assert row is not None
        assert row[0] > before

    def test_identical_put_preserves_cached_at(self, tmp_path: Path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        key = make_key("/a/b.txt")
        with SqliteHashCacher(db) as cacher:
            cacher.put(key, make_hash())
            with sqlite3.connect(db) as conn:
                conn.execute("UPDATE file_hash_cache_v2 SET cached_at=1")
            cacher.put(key, make_hash())
        with sqlite3.connect(db) as conn:
            assert conn.execute(
                "SELECT cached_at FROM file_hash_cache_v2"
            ).fetchone() == (1,)
