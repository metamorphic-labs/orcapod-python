"""A cached file hash is used only for the algorithm that made it (ITL-1488).

The cache accepts only SHA-256. Historical rows from other algorithms must
stay untouched and must never be returned by the SHA-256 lookup.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from typing import cast

import pytest
from typing_extensions import override
from upath import UPath

from orcapod.hashing.directory_hashers import BasicDirectoryHasher
from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher, FileHashKey
from orcapod.hashing.hash_cachers import InMemoryHashCacher, SqliteHashCacher
from orcapod.hashing.semantic_hashing.builtin_handlers import FileHandler
from orcapod.types import ContentHash, PathLike

Cacher = InMemoryHashCacher | SqliteHashCacher


@pytest.fixture
def data_file(tmp_path: Path) -> Path:
    path = tmp_path / "data.bin"
    path.write_bytes(b"orcapod file hash cache" * 100)
    return path


@pytest.fixture(params=["in_memory", "sqlite"])
def cacher(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Cacher]:
    if request.param == "in_memory":
        yield InMemoryHashCacher()
    else:
        with SqliteHashCacher(tmp_path / "cache.db") as sqlite_cacher:
            yield sqlite_cacher


def _key(algorithm: str, mtime_ns: int = 1000) -> FileHashKey:
    return FileHashKey(UPath("/a/b.txt"), mtime_ns, 100, algorithm)


@pytest.mark.parametrize("label", ["md5", "xxh64"])
def test_cacher_refuses_another_label_beside_a_sha256_entry(cacher: Cacher, label: str):
    value = _hash_of("sha256")
    cacher.put(_key("sha256"), value)

    assert cacher.get(_key(label)) is None
    with pytest.raises(ValueError, match=f"Unsupported file hash algorithm {label!r}"):
        cacher.put(_key(label), _hash_of(label))

    assert cacher.get(_key(label)) is None
    assert cacher.get(_key("sha256")) == value


@pytest.mark.parametrize("label", ["md5", "xxh64"])
def test_sqlite_without_mtime_skips_a_newer_row_of_another_label(
    tmp_path: Path, label: str
):
    db_path = tmp_path / "cache.db"
    old_key = _key(label, mtime_ns=2000)
    old_blob = _hash_of(label).to_prefixed_digest()
    with SqliteHashCacher(db_path, match_mtime=False) as cache:
        _insert_raw(db_path, old_blob, key=old_key)
        assert cache.get(_key("sha256", mtime_ns=3000)) is None
        value = _hash_of("sha256")
        cache.put(_key("sha256"), value)
        assert cache.get(_key("sha256", mtime_ns=3000)) == value
        assert cache.get(old_key) is None
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT mtime_ns, hash, cached_at FROM file_hash_cache_v2 WHERE algorithm=?",
            (label,),
        ).fetchall() == [(old_key.mtime_ns, old_blob, 1)]


def test_sqlite_clear_keeps_historical_rows(tmp_path: Path) -> None:
    """Clearing the active cache leaves the old algorithm's row intact."""
    db_path = tmp_path / "cache.db"
    old_key = _key("md5")
    old_blob = _hash_of("md5").to_prefixed_digest()
    with SqliteHashCacher(db_path) as cache:
        _insert_raw(db_path, old_blob, key=old_key)
        cache.put(_key("sha256"), _hash_of("sha256"))
        cache.clear()
        assert cache.get(_key("sha256")) is None
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT algorithm, hash, cached_at FROM file_hash_cache_v2"
        ).fetchall() == [("md5", old_blob, 1)]


@pytest.mark.parametrize("match_mtime", [True, False])
def test_sqlite_read_and_clear_match_the_exact_label(
    tmp_path: Path, match_mtime: bool
) -> None:
    """A case-insensitive column cannot turn a historical row into an active one."""
    db_path = tmp_path / "cache.db"
    with sqlite3.connect(db_path) as conn:
        _ = conn.execute(
            """CREATE TABLE file_hash_cache_v2 (
                path TEXT NOT NULL, mtime_ns INTEGER NOT NULL, size INTEGER NOT NULL,
                algorithm TEXT COLLATE NOCASE NOT NULL, hash BLOB NOT NULL,
                cached_at INTEGER NOT NULL,
                PRIMARY KEY (path, mtime_ns, size, algorithm)
            ) WITHOUT ROWID"""
        )
    blob = _hash_of("sha256").to_prefixed_digest()
    _insert_raw(db_path, blob, key=_key("SHA256"))
    with SqliteHashCacher(db_path, match_mtime=match_mtime) as cache:
        assert cache.get(_key("sha256")) is None
        cache.clear()
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT algorithm, hash FROM file_hash_cache_v2"
        ).fetchall() == [("SHA256", blob)]


class _LookalikeLabel(str):
    """A label that compares and hashes like SHA-256 in a dictionary."""

    @override
    def __eq__(self, other: object) -> bool:
        return other == "sha256"

    @override
    def __hash__(self) -> int:
        return hash("sha256")


@pytest.mark.parametrize("match_mtime", [True, False])
def test_memory_read_rejects_a_lookalike_label(match_mtime: bool) -> None:
    """A string's underlying label determines whether a cache lookup is valid."""
    cache = InMemoryHashCacher(match_mtime=match_mtime)
    value = _hash_of("sha256")
    cache.put(_key("sha256"), value)
    assert cache.get(_key(_LookalikeLabel("md5"))) is None
    assert cache.get(_key("sha256")) == value


@pytest.fixture
def restore_file_handlers() -> Iterator[None]:
    """Put the default context's File and Directory handlers back afterwards."""
    from orcapod.contexts import get_default_context
    from orcapod.logical_types.directory_type import Directory
    from orcapod.logical_types.file_type import File

    registry = get_default_context().semantic_hasher.type_handler_registry
    file_handler = registry.get_handler_for_type(File)
    directory_handler = registry.get_handler_for_type(Directory)
    assert file_handler is not None and directory_handler is not None
    yield
    registry.register(File, file_handler)
    registry.register(Directory, directory_handler)


@pytest.mark.parametrize("label", ["md5", "xxh64"])
def test_default_file_hash_ignores_a_legacy_row_of_another_algorithm(
    restore_file_handlers, tmp_path: Path, data_file: Path, label: str
):
    """Enabling caching over a historical cache preserves the file value hash."""
    from orcapod.contexts import enable_file_hash_caching, get_default_context
    from orcapod.logical_types.file_type import File

    hasher = get_default_context().semantic_hasher
    uncached = hasher.hash_object(File(data_file))

    db_path = tmp_path / "cache.db"
    path = UPath(data_file).resolve()
    stat = path.stat()
    key = FileHashKey(path, stat.st_mtime_ns, stat.st_size, label)
    with SqliteHashCacher(db_path):
        _insert_raw(db_path, _hash_of(label).to_prefixed_digest(), key=key)
    enable_file_hash_caching(db_path=db_path)

    assert hasher.hash_object(File(data_file)) == uncached  # filled by this call
    assert hasher.hash_object(File(data_file)) == uncached  # a cache hit


class _CountingHasher:
    """A ``FileHasher`` that counts the files it actually reads."""

    def __init__(self) -> None:
        self.inner: FileHasher = FileHasher()
        self.reads: int = 0

    @property
    def hash_algorithm(self) -> str:
        return self.inner.hash_algorithm

    def hash_file(self, file_path: PathLike) -> ContentHash:
        self.reads += 1
        return self.inner.hash_file(file_path)


class _LegacyCacher:
    """A custom cacher keyed by file metadata alone."""

    def __init__(self) -> None:
        self.entries: dict[tuple[UPath, int, int], object] = {}

    def get(self, key: FileHashKey) -> ContentHash | None:
        # Model a custom backend whose deserialization may violate the protocol.
        return cast(
            "ContentHash | None", self.entries.get((key.path, key.mtime_ns, key.size))
        )

    def put(self, key: FileHashKey, value: ContentHash) -> None:
        self.entries[(key.path, key.mtime_ns, key.size)] = value


class _WrongResultHasher:
    """A hasher whose returned label disagrees with its advertised label."""

    hash_algorithm: str = "sha256"

    def hash_file(self, file_path: PathLike) -> ContentHash:
        """Return an incompatible hash."""
        _ = file_path
        return _hash_of("md5")


class _MisencodedFileHash(ContentHash):
    @override
    def to_prefixed_digest(self) -> bytes:
        return b"md5:" + b"\x07" * 16


class _MisencodingFileHasher:
    hash_algorithm: str = "sha256"

    def hash_file(self, file_path: PathLike) -> ContentHash:
        digest = FileHasher().hash_file(file_path).digest
        return _MisencodedFileHash(self.hash_algorithm, digest)


def test_cached_file_hasher_canonicalizes_custom_results(data_file: Path) -> None:
    cache = _LegacyCacher()
    hasher = CachedFileHasher(_MisencodingFileHasher(), cache)
    expected = FileHasher().hash_file(data_file).to_prefixed_digest()

    assert hasher.hash_file(data_file).to_prefixed_digest() == expected
    (stored,) = cache.entries.values()
    assert isinstance(stored, ContentHash)
    assert stored.to_prefixed_digest() == expected


def test_cached_file_hasher_canonicalizes_custom_hits(data_file: Path) -> None:
    cache = _LegacyCacher()
    hasher = _CountingHasher()
    path = UPath(data_file.resolve())
    stat = path.stat()
    value = _MisencodingFileHasher().hash_file(data_file)
    cache.put(FileHashKey(path, stat.st_mtime_ns, stat.st_size, "sha256"), value)

    result = CachedFileHasher(hasher, cache).hash_file(data_file)

    assert (
        result.to_prefixed_digest()
        == FileHasher().hash_file(data_file).to_prefixed_digest()
    )
    assert hasher.reads == 0


def test_file_value_canonicalizes_custom_results(data_file: Path) -> None:
    from orcapod.contexts import get_default_semantic_hasher
    from orcapod.logical_types.file_type import File

    handler = FileHandler(_MisencodingFileHasher())
    result = handler.handle(File(data_file), get_default_semantic_hasher())
    assert (
        result.to_prefixed_digest()
        == FileHasher().hash_file(data_file).to_prefixed_digest()
    )


def test_directory_canonicalizes_custom_file_leaves(data_file: Path) -> None:
    plain = BasicDirectoryHasher(FileHasher())
    custom = BasicDirectoryHasher(_MisencodingFileHasher())
    assert custom.hash_directory(data_file.parent) == plain.hash_directory(
        data_file.parent
    )


def test_memory_cacher_canonicalizes_custom_values() -> None:
    key = _key("sha256")
    expected = _hash_of("sha256")
    cache = InMemoryHashCacher()
    cache.put(key, _MisencodedFileHash(expected.method, expected.digest))

    result = cache.get(key)
    assert result is not None
    assert result.to_prefixed_digest() == expected.to_prefixed_digest()


@pytest.mark.parametrize("consumer", ["file", "directory"])
def test_uncached_consumers_refuse_a_changed_label_before_hashing(
    data_file: Path, consumer: str
) -> None:
    from orcapod.contexts import get_default_semantic_hasher
    from orcapod.logical_types.file_type import File

    inner = _CountingHasher()
    if consumer == "file":
        file_handler = FileHandler(inner)
        inner.inner.algorithm = "md5"
        with pytest.raises(ValueError, match="Unsupported file hash algorithm 'md5'"):
            _ = file_handler.handle(File(data_file), get_default_semantic_hasher())
    else:
        directory_hasher = BasicDirectoryHasher(inner)
        inner.inner.algorithm = "md5"
        with pytest.raises(ValueError, match="Unsupported file hash algorithm 'md5'"):
            _ = directory_hasher.hash_directory(data_file.parent)
    assert inner.reads == 0


def test_cached_file_hasher_rejects_an_invalid_computed_hash(data_file: Path) -> None:
    """The wrapper validates the result even if a custom cacher accepts anything."""
    cache = _LegacyCacher()
    hasher = CachedFileHasher(_WrongResultHasher(), cache)

    with pytest.raises(ValueError, match="'md5'.*'sha256'"):
        _ = hasher.hash_file(data_file)
    assert not cache.entries


def test_cached_file_hasher_rechecks_the_file_algorithm_after_miss(
    data_file: Path,
) -> None:
    """A cache lookup can change the advertised algorithm before file hashing."""
    inner = _CountingHasher()

    class ChangingCacher(_LegacyCacher):
        @override
        def get(self, key: FileHashKey) -> ContentHash | None:
            inner.inner.algorithm = "md5"
            return None

    cache = ChangingCacher()
    hasher = CachedFileHasher(inner, cache)
    with pytest.raises(ValueError, match="Unsupported file hash algorithm 'md5'"):
        _ = hasher.hash_file(data_file)
    assert inner.reads == 0
    assert not cache.entries


def test_file_value_rejects_an_invalid_computed_hash(data_file: Path) -> None:
    """The uncached File handler also checks hashes returned by custom hashers."""
    from orcapod.contexts import get_default_semantic_hasher
    from orcapod.logical_types.file_type import File

    handler = FileHandler(_WrongResultHasher())
    with pytest.raises(ValueError, match="'md5'.*'sha256'"):
        _ = handler.handle(File(data_file), get_default_semantic_hasher())


def test_directory_rejects_an_invalid_file_leaf_hash(data_file: Path) -> None:
    """A custom file hasher must produce SHA-256 before a leaf is folded in."""
    hasher = BasicDirectoryHasher(_WrongResultHasher())

    with pytest.raises(ValueError, match="'md5'.*'sha256'"):
        _ = hasher.hash_directory(data_file.parent)


def test_directory_rechecks_the_file_algorithm_after_ignore_filter(
    data_file: Path,
) -> None:
    """A filter can change the hasher's advertised algorithm during traversal."""
    inner = _CountingHasher()
    hasher = BasicDirectoryHasher(inner)

    def change_algorithm(path: UPath) -> bool:
        assert path.name == data_file.name
        inner.inner.algorithm = "md5"
        return False

    with pytest.raises(ValueError, match="Unsupported file hash algorithm 'md5'"):
        _ = hasher.hash_directory(data_file.parent, ignore=change_algorithm)
    assert inner.reads == 0


class _LabelledCacher(_LegacyCacher):
    """A custom cacher that advertises an algorithm."""

    hash_algorithm: str = "md5"


def test_cached_file_hasher_refuses_a_custom_cachers_unsupported_label() -> None:
    """An explicit cacher algorithm follows the same rule as the inner hasher."""
    with pytest.raises(ValueError, match="Unsupported file hash algorithm 'md5'"):
        _ = CachedFileHasher(FileHasher(), _LabelledCacher())


@pytest.mark.parametrize("changed_component", ["hasher", "cacher"])
def test_cached_file_hasher_refuses_changed_labels_before_cache_lookup(
    data_file: Path, changed_component: str
) -> None:
    class TrackingCacher(_LabelledCacher):
        hash_algorithm: str = "sha256"

        def __init__(self) -> None:
            super().__init__()
            self.lookups: list[str] = []

        @override
        def get(self, key: FileHashKey) -> ContentHash | None:
            self.lookups.append(key.algorithm)
            return _hash_of("md5")

    inner = _WrongResultHasher()
    cache = TrackingCacher()
    wrapped = CachedFileHasher(inner, cache)
    changed = inner if changed_component == "hasher" else cache
    changed.hash_algorithm = "md5"

    with pytest.raises(ValueError) as raised:
        _ = wrapped.hash_file(data_file)
    assert not cache.lookups
    assert not cache.entries
    assert str(raised.value) == (
        "Unsupported file hash algorithm 'md5': sha256 is the only file hash algorithm"
    )


@pytest.mark.parametrize(
    "cached_value",
    [
        pytest.param(ContentHash("md5", b"\x07" * 16), id="wrong-algorithm"),
        pytest.param(ContentHash("sha256", b"\x07" * 31), id="short-digest"),
        pytest.param(ContentHash("sha256", b"\x07" * 33), id="long-digest"),
        pytest.param(b"\x07" * 32, id="raw-bytes"),
        pytest.param(
            SimpleNamespace(method="sha256", digest=b"\x07" * 32), id="look-alike"
        ),
        pytest.param(None, id="none"),
    ],
)
def test_cached_file_hasher_treats_invalid_custom_hits_as_misses(
    data_file: Path, cached_value: object
):
    legacy_cache = _LegacyCacher()
    file_hasher = _CountingHasher()
    cached_hasher = CachedFileHasher(file_hasher, legacy_cache)
    path = UPath(data_file.resolve())
    stat = path.stat()
    legacy_cache.entries[(path, stat.st_mtime_ns, stat.st_size)] = cached_value

    expected = file_hasher.inner.hash_file(data_file)
    assert cached_hasher.hash_file(data_file) == expected
    assert file_hasher.reads == 1
    assert cached_hasher.hash_file(data_file) == expected
    assert file_hasher.reads == 1


def test_cached_file_hasher_uses_a_valid_custom_hit_without_reading(data_file: Path):
    legacy_cache = _LegacyCacher()
    file_hasher = _CountingHasher()
    cached_hasher = CachedFileHasher(file_hasher, legacy_cache)
    path = UPath(data_file.resolve())
    stat = path.stat()
    expected = file_hasher.inner.hash_file(data_file)
    legacy_cache.put(
        FileHashKey(path, stat.st_mtime_ns, stat.st_size, expected.method), expected
    )

    assert cached_hasher.hash_file(data_file) is expected
    assert file_hasher.reads == 0


def test_conflicting_hash_is_refused_and_identical_hash_is_a_noop(cacher: Cacher):
    key = _key("sha256")
    original = _hash_of("sha256", 1)
    cacher.put(key, original)
    cacher.put(key, original)
    with pytest.raises(ValueError, match="Different hash"):
        cacher.put(key, _hash_of("sha256", 2))
    assert cacher.get(key) == original


@pytest.mark.parametrize("same_hash", [True, False])
@pytest.mark.parametrize("malformed_existing", [False, True])
def test_sqlite_concurrent_puts(
    tmp_path: Path,
    same_hash: bool,
    malformed_existing: bool,
    caplog: pytest.LogCaptureFixture,
):
    key = _key("sha256")
    values = (_hash_of("sha256", 1), _hash_of("sha256", 1 if same_hash else 2))
    barrier = Barrier(2)
    db = tmp_path / "cache.db"
    with SqliteHashCacher(db) as cacher:
        if malformed_existing:
            _insert_raw(db, b"corrupt")

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
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM file_hash_cache_v2").fetchone() == (
            1,
        )
    assert len(caplog.records) == int(malformed_existing)


# ---------------------------------------------------------------------------
# The store never holds a value under another algorithm's key, and reads strictly
# ---------------------------------------------------------------------------

DIGEST_SIZES = {
    "sha256": 32,
    "md5": 16,
    "xxh64": 8,
}


def _hash_of(label: str, fill: int = 1) -> ContentHash:
    """A well-formed hash of ``label``: its algorithm's exact digest length."""
    return ContentHash(label, bytes([fill]) * DIGEST_SIZES[label])


@pytest.fixture(params=["in_memory", "sqlite"])
def any_mtime_cacher(
    request: pytest.FixtureRequest, tmp_path: Path
) -> Iterator[Cacher]:
    """In-memory and SQLite cachers, with and without the mtime match."""
    match_mtime = request.node.callspec.params.get("match_mtime", True)
    if request.param == "in_memory":
        yield InMemoryHashCacher(match_mtime=match_mtime)
    else:
        with SqliteHashCacher(tmp_path / "c.db", match_mtime=match_mtime) as c:
            yield c


@pytest.mark.parametrize("match_mtime", [True, False])
@pytest.mark.parametrize(
    ("key_label", "value_label"), [("sha256", "md5"), ("md5", "sha256")]
)
def test_put_of_another_algorithms_hash_is_refused(
    any_mtime_cacher: Cacher, match_mtime: bool, key_label: str, value_label: str
):
    """Putting an md5 hash under a sha256 key raises and stores nothing."""
    with pytest.raises(ValueError, match="label"):
        any_mtime_cacher.put(_key(key_label), _hash_of(value_label))
    assert any_mtime_cacher.get(_key(key_label)) is None
    assert any_mtime_cacher.get(_key(value_label)) is None


def test_mislabelled_put_cannot_poison_a_cached_file_hasher(
    cacher: Cacher, data_file: Path
):
    """A refused put leaves CachedFileHasher computing the true hash."""
    stat = data_file.resolve().stat()
    key = FileHashKey(
        UPath(data_file).resolve(), stat.st_mtime_ns, stat.st_size, "sha256"
    )
    with pytest.raises(ValueError):
        cacher.put(key, _hash_of("md5"))
    hashed = CachedFileHasher(FileHasher(), cacher).hash_file(data_file)
    assert hashed == FileHasher().hash_file(data_file)


@pytest.mark.parametrize("length", [31, 33])
def test_put_of_a_digest_of_the_wrong_length_is_refused(cacher: Cacher, length: int):
    key = _key("sha256")
    with pytest.raises(ValueError, match="digest"):
        cacher.put(key, ContentHash(key.algorithm, b"\x01" * length))
    assert cacher.get(key) is None


def _insert_raw(
    db_path: Path, blob: bytes | str, *, key: FileHashKey | None = None
) -> None:
    key = key if key is not None else _key("sha256")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO file_hash_cache_v2 (path, mtime_ns, size, algorithm, hash, cached_at) "
            "VALUES (?, ?, ?, ?, ?, 1)",
            (str(key.path), key.mtime_ns, key.size, key.algorithm, blob),
        )


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


@pytest.mark.parametrize("match_mtime", [True, False])
def test_sqlite_rejects_a_conflict_from_another_label(
    tmp_path: Path,
    match_mtime: bool,
    caplog: pytest.LogCaptureFixture,
):
    db_path = tmp_path / "cache.db"
    stray = _key("SHA256")
    blob = ContentHash(stray.algorithm, b"\x07" * 32).to_prefixed_digest()
    with SqliteHashCacher(db_path, match_mtime=match_mtime) as cache:
        _insert_raw(db_path, blob, key=stray)
        adapter_key = (str, sqlite3.PrepareProtocol)
        previous_adapter = sqlite3.adapters.get(adapter_key)
        # Make SQLite bind the SHA-256 key to the stray label for this transaction.
        sqlite3.register_adapter(
            str, lambda value: "SHA256" if value == "sha256" else value
        )
        try:
            with pytest.raises(ValueError, match="matched a row with algorithm label"):
                cache.put(_key("sha256"), _hash_of("sha256"))
        finally:
            if previous_adapter is None:
                del sqlite3.adapters[adapter_key]
            else:
                sqlite3.register_adapter(str, previous_adapter)
        assert cache.get(_key("sha256")) is None
        assert cache.get(stray) is None
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT algorithm, hash, cached_at FROM file_hash_cache_v2"
        ).fetchall() == [("SHA256", blob, 1)]
    assert not caplog.records


@pytest.mark.parametrize("match_mtime", [True, False])
@pytest.mark.parametrize(
    ("key_type", "label"),
    [
        pytest.param(FileHashKey, _CustomEncodingLabel("sha256"), id="custom-encoding"),
        pytest.param(_ChangingAlgorithmKey, "sha256", id="changing-algorithm"),
    ],
)
def test_sqlite_reads_with_a_canonical_algorithm_snapshot(
    tmp_path: Path, match_mtime: bool, key_type: type[FileHashKey], label: str
):
    key = _key("sha256")
    lookup_key = key_type(key.path, key.mtime_ns, key.size, label)
    with SqliteHashCacher(tmp_path / "cache.db", match_mtime=match_mtime) as cache:
        original = _hash_of("sha256")
        cache.put(key, original)
        assert cache.get(lookup_key) == original


@pytest.mark.parametrize("match_mtime", [True, False])
@pytest.mark.parametrize("same_digest", [True, False])
def test_sqlite_preserves_valid_rows_with_custom_serialization(
    tmp_path: Path,
    match_mtime: bool,
    same_digest: bool,
    caplog: pytest.LogCaptureFixture,
):
    db_path = tmp_path / "cache.db"
    key = _key("sha256")
    original = _hash_of("sha256")
    incoming = _VersionedContentHash("sha256", bytes([1 if same_digest else 2]) * 32)
    blob = original.to_prefixed_digest()
    with SqliteHashCacher(db_path, match_mtime=match_mtime) as cache:
        _insert_raw(db_path, blob)
        if same_digest:
            cache.put(key, incoming)
        else:
            with pytest.raises(ValueError, match="Different hash"):
                cache.put(key, incoming)
        assert cache.get(key) == original
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT hash, cached_at FROM file_hash_cache_v2"
        ).fetchall() == [(blob, 1)]
    assert not caplog.records


CRAFTED_BLOBS = [
    pytest.param(b"sha256:" + b"\x07" * 25, id="raw-32-byte-blob-starting-with-prefix"),
    pytest.param(b"sha256:" + b"\x07" * 33, id="digest-too-long"),
    pytest.param(b"sha256:", id="empty-digest"),
    pytest.param(b"sha256" + b"\x07" * 32, id="no-separator"),
    pytest.param(b"SHA256:" + b"\x07" * 32, id="other-case-label"),
    pytest.param(b"md5:" + b"\x07" * 32, id="other-label"),
    pytest.param("sha256:" + "a" * 32, id="text-instead-of-blob"),
]


@pytest.mark.parametrize("match_mtime", [True, False])
@pytest.mark.parametrize("blob", CRAFTED_BLOBS)
def test_sqlite_repairs_a_malformed_stored_hash(
    tmp_path: Path,
    match_mtime: bool,
    blob: bytes | str,
    caplog: pytest.LogCaptureFixture,
):
    db_path = tmp_path / "c.db"
    with SqliteHashCacher(db_path, match_mtime=match_mtime) as c:
        _insert_raw(db_path, blob)
        key = _key("sha256")
        assert c.get(key) is None
        value = _hash_of("sha256")
        before = int(time.time())
        c.put(key, _VersionedContentHash(value.method, value.digest))
        assert c.get(key) == value
        with pytest.raises(ValueError, match="Different hash"):
            c.put(key, _hash_of("sha256", fill=2))
        assert c.get(key) == value
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT cached_at FROM file_hash_cache_v2").fetchone()
    assert row is not None and before <= row[0] <= int(time.time())
    (warning,) = caplog.records
    assert warning.levelname == "WARNING"
    assert str(key.path) in warning.getMessage()
    assert key.algorithm in warning.getMessage()


def test_sqlite_reads_a_well_formed_stored_hash(tmp_path: Path):
    db_path = tmp_path / "c.db"
    with SqliteHashCacher(db_path) as c:
        _insert_raw(db_path, b"sha256:" + b"\x07" * 32)
        assert c.get(_key("sha256")) == ContentHash("sha256", b"\x07" * 32)


@pytest.mark.parametrize("operation", ["get", "put"])
@pytest.mark.parametrize("match_mtime", [True, False])
def test_sqlite_invalid_utf8_text_is_a_miss_or_repaired(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, operation: str, match_mtime: bool
):
    db_path = tmp_path / "cache.db"
    key = _key("sha256")
    value = _hash_of("sha256")
    with SqliteHashCacher(db_path, match_mtime=match_mtime) as cache:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO file_hash_cache_v2 (path, mtime_ns, size, algorithm, hash, cached_at) "
                "VALUES (?, ?, ?, ?, CAST(X'ff' AS TEXT), 1)",
                (str(key.path), key.mtime_ns, key.size, key.algorithm),
            )
        if operation == "get":
            assert cache.get(key) is None
            assert not caplog.records
            return
        cache.put(key, value)
        assert cache.get(key) == value
        with sqlite3.connect(db_path) as conn:
            assert conn.execute(
                "SELECT typeof(hash), cached_at > 1 FROM file_hash_cache_v2"
            ).fetchone() == ("blob", 1)
    (warning,) = caplog.records
    assert warning.levelname == "WARNING"
    assert str(key.path) in warning.getMessage()
    assert key.algorithm in warning.getMessage()


# ---------------------------------------------------------------------------
# An invalid label is refused on put and a miss on read, on every cacher
# ---------------------------------------------------------------------------

INVALID_LABELS = [
    pytest.param("sha256\x00", id="embedded-nul"),
    pytest.param("shä256", id="non-ascii"),
    pytest.param("", id="empty"),
]


@pytest.mark.parametrize("match_mtime", [True, False])
@pytest.mark.parametrize("label", INVALID_LABELS)
def test_put_with_an_invalid_label_raises_value_error(
    any_mtime_cacher: Cacher, match_mtime: bool, label: str
):
    with pytest.raises(ValueError):
        any_mtime_cacher.put(_key(label), ContentHash(label, b"\x01" * 32))
    assert any_mtime_cacher.get(_key(label)) is None


@pytest.mark.parametrize("match_mtime", [True, False])
@pytest.mark.parametrize("label", INVALID_LABELS)
def test_sqlite_read_with_an_invalid_label_is_a_miss(
    tmp_path: Path, match_mtime: bool, label: str
):
    """A persisted row whose bytes begin with the invalid label is still a miss."""
    db_path = tmp_path / "c.db"
    with SqliteHashCacher(db_path, match_mtime=match_mtime) as c:
        _insert_raw(
            db_path, label.encode("utf-8") + b":" + b"\x07" * 32, key=_key(label)
        )
        assert c.get(_key(label)) is None


class _CustomLabelHasher:
    """A hasher whose label names no ``hashlib`` algorithm."""

    def __init__(self) -> None:
        self.calls: int = 0

    @property
    def hash_algorithm(self) -> str:
        return "app-sha256-v1"

    def hash_file(self, file_path: PathLike) -> ContentHash:
        _ = file_path
        self.calls += 1
        return ContentHash("app-sha256-v1", b"\x00" * 32)


def test_cached_file_hasher_refuses_a_label_no_cache_can_store(cacher: Cacher):
    """CodeRabbit on #594: a label a cache cannot store is refused when the
    ``CachedFileHasher`` is built, not after every file has been hashed."""
    inner = _CustomLabelHasher()
    with pytest.raises(ValueError, match="app-sha256-v1"):
        _ = CachedFileHasher(inner, cacher)
    assert inner.calls == 0


@pytest.mark.parametrize("consumer", [FileHandler, BasicDirectoryHasher])
def test_uncached_consumers_refuse_an_unsupported_file_hasher(
    consumer: type[FileHandler] | type[BasicDirectoryHasher],
) -> None:
    """Direct injection into File and Directory follows the context's SHA-256 rule."""
    inner = _CustomLabelHasher()
    with pytest.raises(ValueError, match="Unsupported file hash algorithm"):
        _ = consumer(inner)
    assert inner.calls == 0
