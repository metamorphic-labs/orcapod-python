"""A cached file hash is used only for the algorithm that made it (ITL-1488).

The file hash cache keys an entry by path, modified time, size and algorithm. A
hash made with one algorithm must never be returned for another: the file's hash
enters identities and stored values, so a cache filled with ``md5`` must not
change what the default ``sha256`` hasher returns.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from typing import cast

import pytest
from upath import UPath

from orcapod.hashing.cache_population import populate_hash_cache
from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher, FileHashKey
from orcapod.hashing.hash_cachers import InMemoryHashCacher, SqliteHashCacher
from orcapod.types import ContentHash, PathLike

Cacher = InMemoryHashCacher | SqliteHashCacher

ALGORITHM_ORDERS = [("sha256", "md5"), ("md5", "sha256"), ("sha256", "xxh64")]


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


@pytest.mark.parametrize(("first", "second"), ALGORITHM_ORDERS)
def test_shared_cache_returns_each_algorithms_own_hash(
    cacher: Cacher, data_file: Path, first: str, second: str
):
    """Two algorithms over one cache, one file: each gets its own hash, in any order."""
    expected = {
        algorithm: FileHasher(algorithm=algorithm).hash_file(data_file)
        for algorithm in (first, second)
    }
    hashers = {
        algorithm: CachedFileHasher(FileHasher(algorithm=algorithm), cacher)
        for algorithm in (first, second)
    }

    # Fill with the first, then ask for the second, then both again (a hit each).
    for algorithm in (first, second, first, second):
        result = hashers[algorithm].hash_file(data_file)
        assert result == expected[algorithm]
        assert result.method == algorithm


@pytest.mark.parametrize(("first", "second"), ALGORITHM_ORDERS)
def test_warm_cache_with_another_algorithm_does_not_change_the_hash(
    tmp_path: Path, data_file: Path, first: str, second: str
):
    """The issue's case: warm-cache with one algorithm, then hash with another."""
    db_path = tmp_path / "cache.db"
    populate_hash_cache(
        data_file.parent, min_size_bytes=0, db_path=db_path, algorithm=first
    )

    with SqliteHashCacher(db_path) as sqlite_cacher:
        result = CachedFileHasher(
            FileHasher(algorithm=second), sqlite_cacher
        ).hash_file(data_file)
    assert result == FileHasher(algorithm=second).hash_file(data_file)


@pytest.mark.parametrize(("first", "second"), ALGORITHM_ORDERS)
def test_warm_cache_counts_another_algorithms_entry_as_not_cached(
    tmp_path: Path, data_file: Path, first: str, second: str
):
    """An entry of another algorithm is not 'already cached' for warm-cache."""
    db_path = tmp_path / "cache.db"
    populate_hash_cache(
        data_file.parent, min_size_bytes=0, db_path=db_path, algorithm=first
    )

    dry = populate_hash_cache(
        data_file.parent,
        min_size_bytes=0,
        db_path=db_path,
        algorithm=second,
        dry_run=True,
    )
    assert (dry.hashed, dry.already_cached) == (1, 0)

    stats = populate_hash_cache(
        data_file.parent, min_size_bytes=0, db_path=db_path, algorithm=second
    )
    assert (stats.hashed, stats.already_cached) == (1, 0)

    again = populate_hash_cache(
        data_file.parent, min_size_bytes=0, db_path=db_path, algorithm=second
    )
    assert (again.hashed, again.already_cached) == (0, 1)


# ---------------------------------------------------------------------------
# The cachers: a key's label selects its own algorithm's entry
# ---------------------------------------------------------------------------


def _key(algorithm: str, mtime_ns: int = 1000) -> FileHashKey:
    return FileHashKey(UPath("/a/b.txt"), mtime_ns, 100, algorithm)


@pytest.mark.parametrize(("first", "second"), [("sha256", "md5"), ("md5", "sha256")])
def test_cacher_entry_of_another_label_is_miss(cacher: Cacher, first: str, second: str):
    cacher.put(_key(first), _hash_of(first, 1))
    assert cacher.get(_key(second)) is None
    assert cacher.get(_key(first)) == _hash_of(first, 1)


@pytest.mark.parametrize("kind", ["in_memory", "sqlite"])
@pytest.mark.parametrize(("first", "second"), [("sha256", "md5"), ("md5", "sha256")])
def test_match_mtime_false_skips_a_newer_entry_of_another_label(
    tmp_path: Path, kind: str, first: str, second: str
):
    """Without the mtime match, the newest entry OF THE KEY'S LABEL is returned."""
    if kind == "in_memory":
        c = InMemoryHashCacher(match_mtime=False)
    else:
        c = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
    c.put(_key(first, 1000), _hash_of(first, 1))
    c.put(_key(second, 2000), _hash_of(second, 2))
    assert c.get(_key(first, 3000)) == _hash_of(first, 1)
    assert c.get(_key(second, 3000)) == _hash_of(second, 2)


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


@pytest.mark.parametrize("warmed_with", ["md5", "xxh64"])
def test_default_file_hash_ignores_a_cache_warmed_with_another_algorithm(
    restore_file_handlers, tmp_path: Path, data_file: Path, warmed_with: str
):
    """The issue's report: 'warm-cache --force --algorithm md5', then caching on."""
    from orcapod.contexts import enable_file_hash_caching, get_default_context
    from orcapod.logical_types.file_type import File

    hasher = get_default_context().semantic_hasher
    uncached = hasher.hash_object(File(data_file))

    db_path = tmp_path / "cache.db"
    populate_hash_cache(
        data_file.parent,
        min_size_bytes=0,
        db_path=db_path,
        algorithm=warmed_with,
        force=True,
    )
    enable_file_hash_caching(db_path=db_path)

    assert hasher.hash_object(File(data_file)) == uncached  # filled by this call
    assert hasher.hash_object(File(data_file)) == uncached  # a cache hit


class _CountingHasher:
    """A ``FileHasher`` that counts the files it actually reads."""

    def __init__(self, algorithm: str) -> None:
        self.inner: FileHasher = FileHasher(algorithm=algorithm)
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
    file_hasher = _CountingHasher(FileHasher().hash_algorithm)
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
    file_hasher = _CountingHasher(FileHasher().hash_algorithm)
    cached_hasher = CachedFileHasher(file_hasher, legacy_cache)
    path = UPath(data_file.resolve())
    stat = path.stat()
    expected = file_hasher.inner.hash_file(data_file)
    legacy_cache.put(
        FileHashKey(path, stat.st_mtime_ns, stat.st_size, expected.method), expected
    )

    assert cached_hasher.hash_file(data_file) is expected
    assert file_hasher.reads == 0


@pytest.mark.parametrize(("first", "second"), ALGORITHM_ORDERS)
def test_alternating_algorithms_keep_both_cached(
    cacher: Cacher, data_file: Path, first: str, second: str
):
    """Read the file once per algorithm, including when the cache is shared."""
    inner = {algorithm: _CountingHasher(algorithm) for algorithm in (first, second)}
    cached = {
        algorithm: CachedFileHasher(inner[algorithm], cacher)
        for algorithm in (first, second)
    }
    for algorithm in (first, second, first, second):
        assert cached[algorithm].hash_file(data_file) == FileHasher(
            algorithm=algorithm
        ).hash_file(data_file)
    assert (inner[first].reads, inner[second].reads) == (1, 1)


def test_conflicting_hash_is_refused_and_identical_hash_is_a_noop(cacher: Cacher):
    key = _key("sha256")
    original = _hash_of("sha256", 1)
    cacher.put(key, original)
    cacher.put(key, original)
    with pytest.raises(ValueError, match="Different hash"):
        cacher.put(key, _hash_of("sha256", 2))
    assert cacher.get(key) == original


@pytest.mark.parametrize("same_hash", [True, False])
def test_sqlite_concurrent_puts(tmp_path: Path, same_hash: bool):
    key = _key("sha256")
    values = (_hash_of("sha256", 1), _hash_of("sha256", 1 if same_hash else 2))
    barrier = Barrier(2)
    with SqliteHashCacher(tmp_path / "cache.db") as cacher:

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


# ---------------------------------------------------------------------------
# The store never holds a value under another algorithm's key, and reads strictly
# ---------------------------------------------------------------------------

DIGEST_SIZES = {
    "sha256": 32,
    "md5": 16,
    "sha1": 20,
    "sha512": 64,
    "xxh64": 8,
    "crc32": 4,
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
        cacher.put(key, FileHasher(algorithm="md5").hash_file(data_file))
    hashed = CachedFileHasher(FileHasher(), cacher).hash_file(data_file)
    assert hashed == FileHasher().hash_file(data_file)


@pytest.mark.parametrize(
    ("label", "length"), [("sha256", 31), ("sha256", 33), ("md5", 32)]
)
def test_put_of_a_digest_of_the_wrong_length_is_refused(
    cacher: Cacher, label: str, length: int
):
    with pytest.raises(ValueError, match="digest"):
        cacher.put(_key(label), ContentHash(label, b"\x01" * length))
    assert cacher.get(_key(label)) is None


def test_put_of_an_unknown_algorithm_is_refused(cacher: Cacher):
    with pytest.raises(ValueError):
        cacher.put(_key("no_such_algo"), ContentHash("no_such_algo", b"\x01" * 32))


@pytest.mark.parametrize("label", sorted(DIGEST_SIZES))
def test_every_supported_algorithm_round_trips(
    cacher: Cacher, data_file: Path, label: str
):
    value = FileHasher(algorithm=label).hash_file(data_file)
    assert len(value.digest) == DIGEST_SIZES[label]
    cacher.put(_key(label), value)
    assert cacher.get(_key(label)) == value


def _insert_raw(db_path: Path, blob: bytes | str, algorithm: str = "sha256") -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO file_hash_cache_v2 (path, mtime_ns, size, algorithm, hash) "
            "VALUES (?, ?, ?, ?, ?)",
            ("/a/b.txt", 1000, 100, algorithm, blob),
        )


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
def test_sqlite_reads_a_malformed_stored_hash_as_a_miss(
    tmp_path: Path, match_mtime: bool, blob: bytes | str
):
    db_path = tmp_path / "c.db"
    with SqliteHashCacher(db_path, match_mtime=match_mtime) as c:
        _insert_raw(db_path, blob)
        assert c.get(_key("sha256")) is None


def test_sqlite_reads_a_well_formed_stored_hash(tmp_path: Path):
    db_path = tmp_path / "c.db"
    with SqliteHashCacher(db_path) as c:
        _insert_raw(db_path, b"sha256:" + b"\x07" * 32)
        assert c.get(_key("sha256")) == ContentHash("sha256", b"\x07" * 32)


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
            db_path, label.encode("utf-8") + b":" + b"\x07" * 32, algorithm=label
        )
        assert c.get(_key(label)) is None


@pytest.mark.parametrize("kind", ["in_memory", "sqlite"])
def test_any_refusal_of_hashlib_is_value_error_on_put_and_a_miss_on_read(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, kind: str
):
    """A hashlib backend may refuse a name with another exception (RuntimeError)."""
    import hashlib

    real_new = hashlib.new

    def refusing_new(name: str, *args: object, **kwargs: object) -> object:
        if name == "refused_algo":
            raise RuntimeError("backend refuses this algorithm")
        return real_new(name, *args, **kwargs)  # pyright: ignore[reportArgumentType] -- passes the caller's arguments through

    db_path = tmp_path / "c.db"
    c: Cacher = (
        InMemoryHashCacher() if kind == "in_memory" else SqliteHashCacher(db_path)
    )
    if isinstance(c, SqliteHashCacher):
        _insert_raw(db_path, b"refused_algo:" + b"\x07" * 32, algorithm="refused_algo")
    monkeypatch.setattr(hashlib, "new", refusing_new)

    with pytest.raises(ValueError):
        c.put(_key("refused_algo"), ContentHash("refused_algo", b"\x01" * 32))
    assert c.get(_key("refused_algo")) is None


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
