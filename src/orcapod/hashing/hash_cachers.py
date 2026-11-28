"""File hash cacher implementations.

Provides ``InMemoryHashCacher`` (testing/ephemeral use) and
``SqliteHashCacher`` (persistent, production-grade) — both implementing
``CacherProtocol[FileHashKey, ContentHash]``.
"""

import logging
import os
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path
from typing import cast

from orcapod.hashing.file_hashers import (
    FILE_HASH_CACHE_SCHEMA_VERSION,
    FileHashKey,
    canonical_file_hash,
    matches_stored_file_hash,
    parse_stored_file_hash,
    serialize_storable_file_hash,
    stored_hash_format,
)
from orcapod.hashing.hash_utils import SHA256_ALGORITHM, require_file_hash_algorithm
from orcapod.types import ContentHash

logger = logging.getLogger(__name__)

_SQLITE_BUSY_TIMEOUT_SECONDS = 5.0


class InMemoryHashCacher:
    """Dict-backed file hash cacher for testing and ephemeral in-process use.

    No persistence, no thread-safety guarantees beyond the GIL, no eviction.
    Use ``SqliteHashCacher`` for production workloads.

    Args:
        read_only: When ``True``, all ``put()`` calls are silent no-ops.
            ``get()`` still works normally. Defaults to ``False``.
        min_cache_size_bytes: When set to a positive integer, files whose
            ``key.size`` is strictly below this threshold are not inserted.
            ``None`` and ``0`` disable the threshold (default behaviour).
            Negative values raise ``ValueError``. Defaults to ``None``.
        match_mtime: When ``True`` (default), cache hits require
            ``path``, ``mtime_ns``, ``size``, and ``algorithm`` to all match. When
            ``False``, ``path``, ``size``, and ``algorithm`` are compared; among
            multiple matching entries the one with the highest ``mtime_ns``
            is returned. The write path is unaffected — ``mtime_ns`` is
            always stored.
    """

    def __init__(
        self,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
        match_mtime: bool = True,
    ) -> None:
        if min_cache_size_bytes is not None and min_cache_size_bytes < 0:
            raise ValueError(
                f"min_cache_size_bytes must be None or a non-negative integer, "
                f"got {min_cache_size_bytes!r}"
            )
        # Nested dict: (path, size, algorithm) → {mtime_ns: ContentHash}.
        # Separating the mtime_ns level from (path, size, algorithm) makes both
        # lookup modes O(1)/O(k) instead of O(n) over all cached entries. The
        # algorithm is part of the key: a hash of another algorithm is never
        # returned (ITL-1488).
        self._cache: dict[tuple, dict[int, ContentHash]] = {}
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes
        self._match_mtime = match_mtime

    def get(self, key: FileHashKey) -> ContentHash | None:
        """Return the cached ``ContentHash`` for ``key``, or ``None`` on miss.

        When ``match_mtime=True`` (default), all four key fields must match.
        When ``match_mtime=False``, ``path``, ``size``, and ``algorithm`` are compared;
        among all matching entries the one with the highest ``mtime_ns`` is
        returned.

        Args:
            key: File hash cache key.

        Returns:
            Cached ``ContentHash``, or ``None`` if not found.
        """
        try:
            require_file_hash_algorithm(key.algorithm)
        except ValueError:
            return None
        inner = self._cache.get((key.path, key.size, SHA256_ALGORITHM))
        if inner is None:
            return None
        if self._match_mtime:
            return inner.get(key.mtime_ns)
        # match_mtime=False: return the hash from the entry with the
        # highest mtime_ns.  max() over the inner dict's keys is O(k)
        # where k is the number of distinct mtimes for this (path, size)
        # pair — almost always 1 in practice.
        return inner[max(inner)]

    def put(self, key: FileHashKey, value: ContentHash) -> None:
        """Store ``value`` under ``key``.

        No-ops silently when ``read_only=True`` or when ``key.size`` is below
        ``min_cache_size_bytes``. Putting the existing hash is also a no-op.

        Args:
            key: File hash cache key.
            value: ``ContentHash`` to store.

        Raises:
            ValueError: If the hash label or digest length is invalid for the key
                (checked before the write guards), or if a write finds a different
                hash under the same key.
        """
        value = canonical_file_hash(value, stored_label=key.algorithm)
        if self._read_only:
            return
        if (
            self._min_cache_size_bytes is not None
            and self._min_cache_size_bytes > 0
            and key.size < self._min_cache_size_bytes
        ):
            return
        entries = self._cache.setdefault((key.path, key.size, SHA256_ALGORITHM), {})
        if entries.setdefault(key.mtime_ns, value) != value:
            raise ValueError(f"Different hash already cached for {key!r}")

    def clear(self) -> None:
        """Remove all entries from the cache."""
        self._cache.clear()

    def __repr__(self) -> str:
        return (
            f"InMemoryHashCacher("
            f"read_only={self._read_only!r}, "
            f"min_cache_size_bytes={self._min_cache_size_bytes!r}, "
            f"match_mtime={self._match_mtime!r})"
        )


class SqliteHashCacher:
    """SQLite-backed file hash cacher.

    Stores file hashes keyed on ``(path, mtime_ns, size, algorithm)`` in a local
    SQLite database. Uses WAL mode for single-writer/multi-reader
    concurrency and thread-local connections for thread safety.

    Writes store a BLOB containing ``b"sha256:"`` followed by the validated digest
    bytes. Historical rows for other algorithms remain in ``file_hash_cache_v2``
    and are ignored by lookups. Older cache tables are left untouched.

    Args:
        db_path: Path to the SQLite database file. Defaults to
            ``~/.orcapod/file_hash_cache.db`` or the
            ``ORCAPOD_HASH_CACHE_DB`` environment variable.
        read_only: When ``True``, all ``put()`` calls are silent no-ops.
            ``get()`` still works normally. Defaults to ``False``.
        min_cache_size_bytes: When set to a positive integer, files whose
            ``key.size`` is strictly below this threshold are not inserted.
            ``None`` and ``0`` disable the threshold (default behaviour).
            Negative values raise ``ValueError``. Defaults to ``None``.
        match_mtime: When ``True`` (default), cache hits require
            ``path``, ``mtime_ns``, ``size``, and ``algorithm`` to all match. When
            ``False``, ``path``, ``size``, and ``algorithm`` are compared; among
            multiple matching entries the one with the highest ``mtime_ns``
            is returned. The write path is unaffected — ``mtime_ns`` is
            always stored.

    Note:
        Heavy multi-writer scenarios are a known SQLite limitation. A Turso
        / libSQL migration is planned as a follow-up issue.
    """

    DEFAULT_DB_PATH = Path.home() / ".orcapod" / "file_hash_cache.db"

    def __init__(
        self,
        db_path: Path | None = None,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
        match_mtime: bool = True,
    ) -> None:
        if min_cache_size_bytes is not None and min_cache_size_bytes < 0:
            raise ValueError(
                f"min_cache_size_bytes must be None or a non-negative integer, "
                f"got {min_cache_size_bytes!r}"
            )
        self.db_path = Path(
            db_path or os.environ.get("ORCAPOD_HASH_CACHE_DB") or self.DEFAULT_DB_PATH
        )
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes
        self._match_mtime = match_mtime
        self._local = threading.local()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Enable WAL mode and create the version 2 cache alongside older tables."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with (
            closing(
                sqlite3.connect(self.db_path, timeout=_SQLITE_BUSY_TIMEOUT_SECONDS)
            ) as conn,
            conn,
        ):
            # SQLite can skip the busy handler on a lock upgrade during a
            # journal-mode change. Retry the pragma after it releases its locks.
            # The deadline limits which failures trigger another retry. Each
            # pragma attempt and BEGIN IMMEDIATE use SQLite's own busy timeout.
            deadline = time.monotonic() + _SQLITE_BUSY_TIMEOUT_SECONDS
            while True:
                try:
                    conn.execute("PRAGMA journal_mode=WAL")
                except sqlite3.OperationalError as exc:
                    if (
                        exc.sqlite_errorcode != sqlite3.SQLITE_BUSY
                        or time.monotonic() >= deadline
                    ):
                        raise
                    time.sleep(0.01)
                else:
                    break
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS file_hash_cache_v2 (
                    path      TEXT    NOT NULL,
                    mtime_ns  INTEGER NOT NULL,
                    size      INTEGER NOT NULL,
                    algorithm TEXT    NOT NULL,
                    hash      BLOB    NOT NULL,
                    cached_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                    PRIMARY KEY (path, mtime_ns, size, algorithm)
                ) WITHOUT ROWID
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_file_hash_cache_v2_path_size_algorithm_mtime
                ON file_hash_cache_v2 (path, size, algorithm, mtime_ns DESC)
                """
            )

            # user_version tracks the layout of the current table.
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version < FILE_HASH_CACHE_SCHEMA_VERSION:
                conn.execute(f"PRAGMA user_version = {FILE_HASH_CACHE_SCHEMA_VERSION}")

    def _connection(self) -> sqlite3.Connection:
        """Return this thread's connection, opening it on first use."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return conn

    def get(self, key: FileHashKey) -> ContentHash | None:
        """Return the cached ``ContentHash`` for ``key``, or ``None`` on miss.

        When ``match_mtime=True`` (default), all four key fields must match.
        When ``match_mtime=False``, the newest entry for the path, size and
        algorithm is returned.

        Args:
            key: File hash cache key.

        Returns:
            Cached ``ContentHash``, or ``None`` if not found.
        """
        algorithm = str.__str__(key.algorithm)
        hash_format = stored_hash_format(algorithm)
        if hash_format is None:
            return None
        conn = self._connection()
        if self._match_mtime:
            row = conn.execute(
                "SELECT CASE WHEN typeof(hash)='blob' THEN hash END FROM file_hash_cache_v2 "
                "WHERE path=? AND mtime_ns=? AND size=? AND algorithm COLLATE BINARY=?",
                (str(key.path), key.mtime_ns, key.size, algorithm),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT CASE WHEN typeof(hash)='blob' THEN hash END FROM file_hash_cache_v2 "
                "WHERE path=? AND size=? AND algorithm COLLATE BINARY=? "
                "ORDER BY mtime_ns DESC LIMIT 1",
                (str(key.path), key.size, algorithm),
            ).fetchone()
        if row is None or not isinstance(row[0], bytes):
            return None
        return parse_stored_file_hash(row[0], algorithm, *hash_format)

    def put(self, key: FileHashKey, value: ContentHash) -> None:
        """Store ``value`` under ``key``.

        No-ops silently when ``read_only=True`` or when ``key.size`` is below
        ``min_cache_size_bytes``. Putting the existing hash is also a no-op.
        Replacing a malformed stored value refreshes its timestamp and logs a warning.

        Args:
            key: File hash cache key.
            value: ``ContentHash`` to store.

        Raises:
            ValueError: If the hash label or digest length is invalid for the key
                (checked before the write guards), if the store matches a row
                with another label, or if a different well-formed hash exists
                under the same key.
        """
        algorithm = str.__str__(key.algorithm)
        prefix, digest_size, stored_value = serialize_storable_file_hash(
            algorithm, value
        )
        if self._read_only:
            return
        if (
            self._min_cache_size_bytes is not None
            and self._min_cache_size_bytes > 0
            and key.size < self._min_cache_size_bytes
        ):
            return
        conn = self._connection()
        fields = (str(key.path), key.mtime_ns, key.size, algorithm)
        # The INSERT starts a write transaction, even when it encounters a
        # conflict. Keep the writer lock through the comparison.
        with conn:
            inserted = conn.execute(
                """
                INSERT INTO file_hash_cache_v2 (path, mtime_ns, size, algorithm, hash)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (path, mtime_ns, size, algorithm) DO NOTHING
                """,
                (*fields, stored_value),
            )
            if inserted.rowcount == 1:
                return
            existing = cast(
                "tuple[object, object] | None",
                conn.execute(
                    """
                    SELECT algorithm, CASE WHEN typeof(hash)='blob' THEN hash END
                    FROM file_hash_cache_v2
                    WHERE path=? AND mtime_ns=? AND size=? AND algorithm=?
                    """,
                    fields,
                ).fetchone(),
            )
            if existing is None:
                raise ValueError(f"Different hash already cached for {key!r}")
            stored_algorithm, blob = existing
            if str.__eq__(algorithm, stored_algorithm) is not True:
                raise ValueError(
                    f"Cache matched a row with algorithm label {stored_algorithm!r} "
                    f"for key label {algorithm!r}"
                )
            if blob == stored_value:
                return
            if matches_stored_file_hash(blob, prefix, digest_size):
                raise ValueError(f"Different hash already cached for {key!r}")
            _ = conn.execute(
                """
                UPDATE file_hash_cache_v2 SET hash=?, cached_at=strftime('%s', 'now')
                WHERE path=? AND mtime_ns=? AND size=? AND algorithm=?
                """,
                (stored_value, *fields),
            )
        logger.warning(
            "Replaced a malformed cached hash for %s (%s)", key.path, algorithm
        )

    def clear(self) -> None:
        """Delete SHA-256 rows, preserving rows from historical algorithms."""
        conn = self._connection()
        conn.execute(
            "DELETE FROM file_hash_cache_v2 WHERE algorithm COLLATE BINARY=?",
            (SHA256_ALGORITHM,),
        )
        conn.commit()

    def close(self) -> None:
        """Close this thread's database connection."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def __repr__(self) -> str:
        return (
            f"SqliteHashCacher("
            f"db_path={str(self.db_path)!r}, "
            f"read_only={self._read_only!r}, "
            f"min_cache_size_bytes={self._min_cache_size_bytes!r}, "
            f"match_mtime={self._match_mtime!r})"
        )

    def __enter__(self) -> "SqliteHashCacher":
        """Return self for use as a context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the thread-local connection on exit."""
        self.close()
