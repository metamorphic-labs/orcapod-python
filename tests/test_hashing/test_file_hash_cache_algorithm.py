"""A cached file hash is used only for the algorithm that made it (ITL-1488).

The file hash cache keys an entry by the file's path, modified time and size. A
hash made with one algorithm must never be returned for another: the file's hash
enters identities and stored values, so a cache filled with ``md5`` must not
change what the default ``sha256`` hasher returns.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from upath import UPath

from orcapod.hashing.cache_population import populate_hash_cache
from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher, FileHashKey
from orcapod.hashing.hash_cachers import InMemoryHashCacher, SqliteHashCacher
from orcapod.types import ContentHash

ALGORITHM_ORDERS = [("sha256", "md5"), ("md5", "sha256"), ("sha256", "xxh64")]


@pytest.fixture
def data_file(tmp_path: Path) -> Path:
    path = tmp_path / "data.bin"
    path.write_bytes(b"orcapod file hash cache" * 100)
    return path


@pytest.fixture(params=["in_memory", "sqlite"])
def cacher(request, tmp_path: Path):
    if request.param == "in_memory":
        yield InMemoryHashCacher()
    else:
        with SqliteHashCacher(tmp_path / "cache.db") as sqlite_cacher:
            yield sqlite_cacher


@pytest.mark.parametrize(("first", "second"), ALGORITHM_ORDERS)
def test_shared_cache_returns_each_algorithms_own_hash(
    cacher, data_file: Path, first: str, second: str
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


def _key(label: str, mtime_ns: int = 1000) -> FileHashKey:
    return FileHashKey(UPath("/a/b.txt"), mtime_ns, 100, label)


@pytest.mark.parametrize(("first", "second"), [("sha256", "md5"), ("md5", "sha256")])
def test_cacher_entry_of_another_label_is_miss(cacher, first: str, second: str):
    cacher.put(_key(first), ContentHash(first, b"\x01" * 16))
    assert cacher.get(_key(second)) is None
    assert cacher.get(_key(first)) == ContentHash(first, b"\x01" * 16)


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
    c.put(_key(first, 1000), ContentHash(first, b"\x01" * 16))
    c.put(_key(second, 2000), ContentHash(second, b"\x02" * 16))
    assert c.get(_key(first, 3000)) == ContentHash(first, b"\x01" * 16)
    assert c.get(_key(second, 3000)) == ContentHash(second, b"\x02" * 16)


@pytest.fixture
def restore_file_handlers():
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
