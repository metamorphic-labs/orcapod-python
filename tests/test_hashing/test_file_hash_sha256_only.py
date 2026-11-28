"""``sha256`` is the only file hash algorithm (ITL-1155).

A hash label names the whole algorithm that produced a digest. The file content
hash keeps the label ``sha256``; no other file hash algorithm has a defined label,
and ``hash_path`` hashed the path text, not the content. So ``hash_file``, the
file hasher classes, the file hash cache and a data context file all refuse every
other algorithm, with an error that names the algorithm given.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import shutil
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from typer.testing import CliRunner
from upath import UPath

import orcapod as op
from orcapod.cli import app
from orcapod.contexts import get_default_context
from orcapod.contexts.core import ContextValidationError
from orcapod.contexts.registry import JSONDataContextRegistry
from orcapod.hashing.cache_population import populate_hash_cache
from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher, FileHashKey
from orcapod.hashing.hash_cachers import InMemoryHashCacher, SqliteHashCacher
from orcapod.hashing.hash_utils import hash_file
from orcapod.types import ContentHash, PathLike

Cacher = InMemoryHashCacher | SqliteHashCacher

CONTEXTS_DIR = Path(op.__file__).parent / "contexts"

# Every algorithm the file hasher used to accept besides sha256, a hashlib name it
# never listed, another spelling of sha256, and a name of nothing.
REFUSED_ALGORITHMS = [
    "md5",
    "sha1",
    "sha512",
    "xxh64",
    "crc32",
    "hash_path",
    "sha3_256",
    "blake2b",
    "SHA256",
    "sha-256",
    "",
    "no_such_algorithm",
]

# Digest lengths of well-formed hashes of some refused labels.
OTHER_DIGEST_SIZES = {"md5": 16, "sha1": 20, "sha512": 64, "xxh64": 8, "crc32": 4}


def _refusal(algorithm: str) -> str:
    """The pattern of the error for ``algorithm``: it names it, and sha256."""
    return (
        rf"Unsupported file hash algorithm {algorithm!r}: "
        r"sha256 is the only file hash algorithm"
    ).replace("\\'", "'")


@pytest.fixture
def data_file(tmp_path: Path) -> Path:
    path = tmp_path / "data.bin"
    path.write_bytes(b"orcapod file hash" * 100)
    return path


@pytest.fixture(params=["in_memory", "sqlite"])
def cacher(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Cacher]:
    if request.param == "in_memory":
        yield InMemoryHashCacher()
    else:
        with SqliteHashCacher(tmp_path / "cache.db") as sqlite_cacher:
            yield sqlite_cacher


# ---------------------------------------------------------------------------
# hash_file and FileHasher
# ---------------------------------------------------------------------------


def test_hash_file_is_the_sha256_of_the_content(data_file: Path) -> None:
    expected = ContentHash("sha256", hashlib.sha256(data_file.read_bytes()).digest())
    assert hash_file(data_file) == expected
    assert FileHasher().hash_file(data_file) == expected
    assert FileHasher(algorithm="sha256").hash_file(data_file) == expected
    assert FileHasher().hash_algorithm == "sha256"


def test_hash_file_has_no_algorithm_argument() -> None:
    """The standalone hasher always uses SHA-256."""
    assert "algorithm" not in inspect.signature(hash_file).parameters


@pytest.mark.parametrize("algorithm", REFUSED_ALGORITHMS)
def test_file_hasher_refuses_every_other_algorithm_when_built(algorithm: str) -> None:
    with pytest.raises(ValueError) as raised:
        FileHasher(algorithm=algorithm)
    assert repr(algorithm) in str(raised.value)
    assert "sha256 is the only file hash algorithm" in str(raised.value)


@pytest.mark.parametrize("attribute", ["algorithm", "hash_algorithm"])
def test_file_hasher_algorithm_is_read_only(data_file: Path, attribute: str) -> None:
    """Both public SHA-256 properties refuse reassignment."""
    hasher = FileHasher()
    with pytest.raises(AttributeError):
        object.__setattr__(hasher, attribute, "md5")
    assert hasher.hash_file(data_file) == hash_file(data_file)


def test_path_text_never_enters_a_file_hash(tmp_path: Path) -> None:
    """Same content at two paths: one hash. Other content at one path: another."""
    first, second = tmp_path / "a.bin", tmp_path / "b.bin"
    first.write_bytes(b"same")
    second.write_bytes(b"same")
    assert hash_file(first) == hash_file(second)
    before = hash_file(first)
    first.write_bytes(b"other")
    assert hash_file(first) != before


# ---------------------------------------------------------------------------
# The file hash cache
# ---------------------------------------------------------------------------


class _OtherLabelHasher:
    """A custom hasher object that reports another label."""

    def __init__(self, label: str) -> None:
        self.label: str = label
        self.calls: int = 0

    @property
    def hash_algorithm(self) -> str:
        return self.label

    def hash_file(self, file_path: PathLike) -> ContentHash:
        _ = file_path
        self.calls += 1
        return ContentHash(self.label, b"\x00" * OTHER_DIGEST_SIZES[self.label])


@pytest.mark.parametrize("label", sorted(OTHER_DIGEST_SIZES))
def test_cached_file_hasher_refuses_an_inner_hasher_of_another_label(
    cacher: Cacher, label: str
) -> None:
    inner = _OtherLabelHasher(label)
    with pytest.raises(ValueError, match=_refusal(label)):
        _ = CachedFileHasher(inner, cacher)
    assert inner.calls == 0


def _key(algorithm: str, mtime_ns: int = 1000) -> FileHashKey:
    return FileHashKey(UPath("/a/b.txt"), mtime_ns, 100, algorithm)


@pytest.mark.parametrize("label", sorted(OTHER_DIGEST_SIZES))
def test_cacher_refuses_a_put_under_another_label(cacher: Cacher, label: str) -> None:
    """A well-formed hash of another algorithm, under its own key: refused."""
    value = ContentHash(label, b"\x01" * OTHER_DIGEST_SIZES[label])
    with pytest.raises(ValueError, match=_refusal(label)):
        cacher.put(_key(label), value)
    assert cacher.get(_key(label)) is None
    assert cacher.get(_key("sha256")) is None


def _insert_row(db_path: Path, key: FileHashKey, blob: bytes) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO file_hash_cache_v2 "
            "(path, mtime_ns, size, algorithm, hash, cached_at) "
            "VALUES (?, ?, ?, ?, ?, 1)",
            (str(key.path), key.mtime_ns, key.size, key.algorithm, blob),
        )


def _rows(db_path: Path) -> list[tuple[Any, ...]]:
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            "SELECT path, mtime_ns, size, algorithm, hash, cached_at "
            "FROM file_hash_cache_v2 ORDER BY algorithm"
        ).fetchall()


@pytest.mark.parametrize("match_mtime", [True, False])
def test_rows_of_other_algorithms_in_a_cache_file_stay_and_are_never_read(
    tmp_path: Path, data_file: Path, match_mtime: bool
) -> None:
    """A cache file filled before the restriction: its md5 and xxh64 rows are
    untouched, never returned, and the sha256 entry is written beside them."""
    db_path = tmp_path / "cache.db"
    path = UPath(data_file).resolve()
    stat = path.stat()
    old = {
        label: FileHashKey(path, stat.st_mtime_ns, stat.st_size, label)
        for label in ("md5", "xxh64")
    }
    with SqliteHashCacher(db_path, match_mtime=match_mtime) as sqlite_cacher:
        for label, key in old.items():
            digest = b"\x07" * OTHER_DIGEST_SIZES[label]
            _insert_row(db_path, key, label.encode() + b":" + digest)
        before = _rows(db_path)
        assert len(before) == 2

        for key in old.values():
            assert sqlite_cacher.get(key) is None

        cached = CachedFileHasher(FileHasher(), sqlite_cacher)
        expected = FileHasher().hash_file(data_file)
        assert cached.hash_file(data_file) == expected
        assert cached.hash_file(data_file) == expected

    after = _rows(db_path)
    assert [row for row in after if row[3] != "sha256"] == before
    (sha_row,) = [row for row in after if row[3] == "sha256"]
    assert sha_row[4] == expected.to_prefixed_digest()


def test_warming_a_cache_holding_another_algorithms_rows(
    tmp_path: Path, data_file: Path
) -> None:
    """An md5 row does not count as cached, and warming leaves it in place."""
    db_path = tmp_path / "cache.db"
    path = UPath(data_file).resolve()
    stat = path.stat()
    with SqliteHashCacher(db_path):
        _insert_row(
            db_path,
            FileHashKey(path, stat.st_mtime_ns, stat.st_size, "md5"),
            b"md5:" + b"\x07" * 16,
        )
    before = _rows(db_path)

    dry = populate_hash_cache(
        data_file.parent, min_size_bytes=0, db_path=db_path, dry_run=True
    )
    assert (dry.hashed, dry.already_cached) == (1, 0)
    stats = populate_hash_cache(data_file.parent, min_size_bytes=0, db_path=db_path)
    assert (stats.hashed, stats.already_cached, stats.errors) == (1, 0, 0)
    again = populate_hash_cache(data_file.parent, min_size_bytes=0, db_path=db_path)
    assert (again.hashed, again.already_cached) == (0, 1)

    after = _rows(db_path)
    assert [row for row in after if row[3] != "sha256"] == before
    assert [row[3] for row in after] == ["md5", "sha256"]


# ---------------------------------------------------------------------------
# No argument chooses an algorithm any more
# ---------------------------------------------------------------------------


def test_populate_hash_cache_takes_no_algorithm() -> None:
    assert "algorithm" not in inspect.signature(populate_hash_cache).parameters


def test_warm_cache_has_no_algorithm_option(tmp_path: Path, data_file: Path) -> None:
    runner = CliRunner()
    db_path = tmp_path / "cache.db"
    refused = runner.invoke(
        app,
        ["warm-cache", str(data_file.parent), "--db-path", str(db_path)]
        + ["--min-size", "0", "--algorithm", "md5"],
    )
    assert refused.exit_code == 2
    assert not db_path.exists()
    assert "--algorithm" not in runner.invoke(app, ["warm-cache", "--help"]).output

    done = runner.invoke(
        app,
        ["warm-cache", str(data_file.parent), "--db-path", str(db_path)]
        + ["--min-size", "0"],
    )
    assert done.exit_code == 0, done.output
    assert [row[3] for row in _rows(db_path)] == ["sha256"]


# ---------------------------------------------------------------------------
# A data context file naming another algorithm
# ---------------------------------------------------------------------------


def _manifests_naming(tmp_path: Path, config: dict[str, Any]) -> Path:
    """Copy the data context files, giving the file hasher ``config``."""
    manifests = tmp_path / "data"
    manifests.mkdir()
    shutil.copytree(CONTEXTS_DIR / "data" / "schemas", manifests / "schemas")
    spec = json.loads((CONTEXTS_DIR / "data" / "v1.json").read_text())
    spec["file_hasher"]["_config"] = config
    (manifests / "v1.json").write_text(json.dumps(spec))
    return manifests


@pytest.mark.parametrize("algorithm", ["md5", "xxh64", "crc32", "hash_path", "sha512"])
def test_data_context_file_naming_another_algorithm_is_refused(
    tmp_path: Path, algorithm: str
) -> None:
    registry = JSONDataContextRegistry(
        contexts_dir=_manifests_naming(tmp_path, {"algorithm": algorithm})
    )
    with pytest.raises(ContextValidationError) as raised:
        registry.get_context()
    assert (
        f"Unsupported file hash algorithm {algorithm!r}: "
        "sha256 is the only file hash algorithm"
    ) in str(raised.value)


def test_data_context_file_naming_sha256_loads(tmp_path: Path) -> None:
    registry = JSONDataContextRegistry(
        contexts_dir=_manifests_naming(tmp_path, {"algorithm": "sha256"})
    )
    assert registry.get_context().context_key == "std:v1:default"


# ---------------------------------------------------------------------------
# File columns in a pipeline: sha256 on every path, in every arrangement
# ---------------------------------------------------------------------------

TAG_ORDERS = [("subject", "session"), ("session", "subject")]
LAYOUTS = [
    ("subject", "session", "recording", "notes"),
    ("notes", "recording", "session", "subject"),
    ("recording", "subject", "notes", "session"),
]


def _file_source(folder: Path, tag_order: tuple[str, ...], layout: tuple[str, ...]):
    """Two tag columns and two file columns, in the given arrangement."""
    files: dict[str, list[op.File]] = {"recording": [], "notes": []}
    for row in range(3):
        for column in files:
            path = folder / f"{column}_{row}.bin"
            path.write_bytes(f"{column} {row}".encode() * 50)
            files[column].append(op.File(path))
    columns: dict[str, list[Any]] = {
        "subject": ["s1", "s1", "s2"],
        "session": [1, 2, 1],
        **files,
    }
    return op.sources.DictSource(
        [dict(zip(layout, values)) for values in zip(*(columns[c] for c in layout))],
        tag_columns=list(tag_order),
        tag_only_columns=True,
        source_id="recordings",
    )


@op.function_pod(output_keys=("recording_size", "notes_size"))
def file_sizes(recording: op.File, notes: op.File) -> tuple[int, int]:
    """The sizes of both files."""
    return recording.stat().st_size, notes.stat().st_size


def _file_hashes(folder: Path) -> dict[str, ContentHash]:
    return {
        path.name: ContentHash("sha256", hashlib.sha256(path.read_bytes()).digest())
        for path in sorted(folder.glob("*.bin"))
    }


def _rows_of(table: pa.Table) -> list[tuple[Any, ...]]:
    names = ("subject", "session", "recording_size", "notes_size")
    return sorted(zip(*(table.column(name).to_pylist() for name in names)))


@pytest.mark.parametrize("tag_order", TAG_ORDERS)
@pytest.mark.parametrize("layout", LAYOUTS)
def test_file_columns_hash_as_sha256_in_every_arrangement(
    tmp_path: Path, tag_order: tuple[str, ...], layout: tuple[str, ...]
) -> None:
    """Every file cell's value hash is the sha256 of its content, and the rows a
    function pod computes over both file columns are the same, sync and async."""
    source = _file_source(tmp_path, tag_order, layout)
    expected = _file_hashes(tmp_path)
    hasher = get_default_context().semantic_hasher
    seen = 0
    for _, data in source.iter_data():
        for column in ("recording", "notes"):
            value = data[column]
            assert isinstance(value, op.File)
            assert hasher.hash_object(value) == expected[value.name]
            seen += 1
    assert seen == 6

    reference = _file_source(tmp_path, TAG_ORDERS[0], LAYOUTS[0])
    assert source.content_hash() == reference.content_hash()
    assert [data.content_hash() for _, data in source.iter_data()] == [
        data.content_hash() for _, data in reference.iter_data()
    ]

    want = [("s1", 1, 550, 350), ("s1", 2, 550, 350), ("s2", 1, 550, 350)]
    assert _rows_of(file_sizes.pod(source).as_table()) == want

    for run_async in (False, True):
        job = op.PipelineJob(
            name="sizes", database=op.databases.InMemoryArrowDatabase()
        )
        with job:
            file_sizes.pod(source, label="sizes")
        if run_async:
            asyncio.run(job.run_async())
        else:
            job.run()
        assert _rows_of(job.nodes["sizes"].as_table()) == want
