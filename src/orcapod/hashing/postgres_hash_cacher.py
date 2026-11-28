"""PostgreSQL-backed file hash cacher.

Provides ``PostgresHashCacher`` — a network-accessible, concurrent-safe
implementation of ``CacherProtocol[FileHashKey, ContentHash]``. Requires
the optional ``psycopg`` driver (``pip install 'orcapod[postgresql]'``).
"""

from __future__ import annotations

import logging
import re
import threading

from orcapod.hashing.file_hashers import (
    FILE_HASH_CACHE_SCHEMA_VERSION,
    FileHashKey,
    matches_stored_file_hash,
    parse_stored_file_hash,
    serialize_storable_file_hash,
    stored_hash_format,
)
from orcapod.types import ContentHash

logger = logging.getLogger(__name__)

try:
    import psycopg
except ImportError as _exc:  # pragma: no cover
    raise ImportError(
        "PostgresHashCacher requires psycopg. "
        "Install it with: pip install 'orcapod[postgresql]'"
    ) from _exc


def _redact_conninfo(conninfo: str) -> str:
    """Return ``conninfo`` with any password value replaced by ``***``.

    Handles both URL form (``postgresql://user:pass@host/db``) and
    keyword DSN form (``host=... password=secret ...``).

    Args:
        conninfo: Raw psycopg3 connection string.

    Returns:
        Connection string safe for logging, with the password redacted.
    """
    # URL form: postgresql://user:pass@host → postgresql://user:***@host
    # Capture group 1: "://user:", group 2: "@" — replace the password between them.
    redacted = re.sub(r"(://[^:@]*:)[^@]+(@)", r"\1***\2", conninfo)
    # Keyword form: password=value or password='value with spaces'
    redacted = re.sub(r"(?i)\bpassword\s*=\s*(?:'[^']*'|\S+)", "password=***", redacted)
    return redacted


class PostgresHashCacher:
    """PostgreSQL-backed file hash cacher.

    Stores file hashes keyed on ``(path, mtime_ns, size, algorithm)`` in a shared
    PostgreSQL database. Uses thread-local connections for thread safety.

    The hash is stored as a BYTEA in ``{method}:{raw_digest}`` format via
    ``ContentHash.to_prefixed_digest()``. Each algorithm has its own row in
    ``file_hash_cache_v2``. The companion ``file_hash_cache_v2_meta`` table records
    schema version 2. Older cache tables and their metadata are left untouched.

    Requires ``psycopg[binary]>=3.0`` (install with
    ``pip install 'orcapod[postgresql]'``).
    Minimum supported PostgreSQL version: **14**.

    Args:
        conninfo: psycopg3 connection string, e.g.
            ``"postgresql://user:pass@host:5432/dbname"`` or keyword DSN
            ``"host=myhost dbname=mydb user=myuser password=mypass"``.
            Any password in the conninfo is redacted in ``__repr__``.
        read_only: When ``True``, all ``put()`` calls are silent no-ops.
            ``get()`` still works normally. Defaults to ``False``.
        min_cache_size_bytes: When set to a positive integer, files whose
            ``key.size`` is strictly below this threshold are not inserted.
            ``None`` and ``0`` disable the threshold (default behaviour).
            Negative values raise ``ValueError``. Defaults to ``None``.
        match_mtime: When ``True`` (default), cache hits require
            all four key fields to match. When ``False``, ``path``, ``size``, and
            ``algorithm`` are compared; among
            multiple matching entries the one with the highest ``mtime_ns``
            is returned. The write path is unaffected — ``mtime_ns`` is
            always stored.

    Raises:
        ValueError: If ``min_cache_size_bytes`` is negative.
    """

    def __init__(
        self,
        conninfo: str,
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
        self._conninfo = conninfo
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes
        self._match_mtime = match_mtime
        self._local = threading.local()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create the version 2 cache and its metadata alongside older tables."""
        with psycopg.connect(self._conninfo) as conn:
            # Concurrent first-time setup in one schema must serialize its DDL.
            conn.execute(
                "SELECT pg_advisory_xact_lock("
                "hashtext(current_schema()), hashtext('file_hash_cache_v2'))"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS file_hash_cache_v2 (
                    path      TEXT   NOT NULL,
                    mtime_ns  BIGINT NOT NULL,
                    size      BIGINT NOT NULL,
                    algorithm TEXT   NOT NULL,
                    hash      BYTEA  NOT NULL,
                    cached_at BIGINT NOT NULL
                        DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
                    PRIMARY KEY (path, mtime_ns, size, algorithm)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_file_hash_cache_v2_path_size_algorithm_mtime
                ON file_hash_cache_v2 (path, size, algorithm, mtime_ns DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS file_hash_cache_v2_meta (
                    key   TEXT NOT NULL PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO file_hash_cache_v2_meta (key, value)
                VALUES ('schema_version', %s)
                ON CONFLICT (key) DO NOTHING
                """,
                (str(FILE_HASH_CACHE_SCHEMA_VERSION),),
            )

    def _connection(self) -> "psycopg.Connection[tuple[object, ...]]":
        """Return this thread's connection, opening it on first use."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = psycopg.connect(self._conninfo, autocommit=True)
            conn.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
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
            return None  # Invalid labels can include NULs, which PostgreSQL rejects.
        conn = self._connection()
        if self._match_mtime:
            row = conn.execute(
                "SELECT hash FROM file_hash_cache_v2 "
                "WHERE path=%s AND mtime_ns=%s AND size=%s AND algorithm=%s",
                (str(key.path), key.mtime_ns, key.size, algorithm),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT hash FROM file_hash_cache_v2 "
                "WHERE path=%s AND size=%s AND algorithm=%s "
                "ORDER BY mtime_ns DESC LIMIT 1",
                (str(key.path), key.size, algorithm),
            ).fetchone()
        if row is None:
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
        with conn.transaction():
            while True:
                inserted = conn.execute(
                    """
                    INSERT INTO file_hash_cache_v2 (path, mtime_ns, size, algorithm, hash)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (path, mtime_ns, size, algorithm) DO NOTHING
                    """,
                    (*fields, stored_value),
                )
                if inserted.rowcount == 1:
                    return
                # A separate statement sees a competing insert after it commits.
                # Lock the existing row through comparison and repair. If clear()
                # deleted it in the meantime, retry against the current table.
                existing = conn.execute(
                    """
                    SELECT algorithm, hash FROM file_hash_cache_v2
                    WHERE path=%s AND mtime_ns=%s AND size=%s AND algorithm=%s
                    FOR NO KEY UPDATE
                    """,
                    fields,
                ).fetchone()
                if existing is None:
                    continue
                stored_algorithm, blob = existing
                # psycopg returns TEXT as bytes under SQL_ASCII.
                stored_label = (
                    str.encode(stored_algorithm)
                    if isinstance(stored_algorithm, str)
                    else stored_algorithm
                )
                if bytes.__eq__(prefix[:-1], stored_label) is not True:
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
                    UPDATE file_hash_cache_v2
                    SET hash=%s, cached_at=EXTRACT(EPOCH FROM statement_timestamp())::BIGINT
                    WHERE path=%s AND mtime_ns=%s AND size=%s AND algorithm=%s
                    """,
                    (stored_value, *fields),
                )
                break
        logger.warning(
            "Replaced a malformed cached hash for %s (%s)", key.path, algorithm
        )

    def clear(self) -> None:
        """Delete all rows from the cache table."""
        self._connection().execute("DELETE FROM file_hash_cache_v2")

    def close(self) -> None:
        """Close this thread's database connection."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def __enter__(self) -> "PostgresHashCacher":
        """Return self for use as a context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the thread-local connection on exit."""
        self.close()

    def __repr__(self) -> str:
        return (
            f"PostgresHashCacher("
            f"conninfo={_redact_conninfo(self._conninfo)!r}, "
            f"read_only={self._read_only!r}, "
            f"min_cache_size_bytes={self._min_cache_size_bytes!r}, "
            f"match_mtime={self._match_mtime!r})"
        )
