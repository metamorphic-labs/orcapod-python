from dataclasses import dataclass

from upath import UPath

from orcapod.hashing.hash_utils import (
    SHA256_ALGORITHM,
    file_hash_digest_size,
    hash_file,
)
from orcapod.protocols.hashing_protocols import (
    CacherProtocol,
    FileContentHasherProtocol,
)
from orcapod.types import ContentHash, PathLike

FILE_HASH_CACHE_SCHEMA_VERSION = 2


@dataclass(frozen=True)
class FileHashKey:
    """Cache lookup key for a file hash.

    The key captures the three file attributes that together identify a
    file's content without reading it: its absolute path, last-modified
    time in nanoseconds, and byte size; and the label of the algorithm whose
    hash is looked up. A cacher returns an entry only for the same label: a
    hash made with another algorithm is a miss (ITL-1488).

    Attributes:
        path: Absolute, resolved ``UPath``. Any ``PathLike`` input is
            normalised to ``UPath`` before constructing this key.
        mtime_ns: Last-modified time in nanoseconds (``path.stat().st_mtime_ns``).
        size: File size in bytes (``path.stat().st_size``).
        algorithm: PROVISIONAL. Hash label of the algorithm (``ContentHash.method`` of the
            hash, e.g. ``"sha256"``).
    """

    path: UPath
    mtime_ns: int
    size: int
    algorithm: str


def stored_hash_prefix(label: str) -> bytes | None:
    """The leading bytes of a stored hash of ``label`` (``<label>:``), if it is valid.

    A persistent cacher stores ``ContentHash.to_prefixed_digest()``, which starts
    with these bytes.

    Args:
        label: The label of a key.

    Returns:
        ``<label>:`` as ASCII bytes, or ``None`` when ``label`` is not an ASCII
        name of an algorithm with a fixed digest length: no hash of it can be
        stored, so every lookup with it is a miss.
    """
    if not label.isascii():
        return None
    try:
        _ = file_hash_digest_size(label)
    except ValueError:
        return None
    return f"{label}:".encode("ascii")


def require_storable(key: FileHashKey, value: ContentHash) -> None:
    """Refuse to store ``value`` under ``key`` unless it is a hash of the key's algorithm.

    Every file hash cacher calls this first in ``put``, so a store never holds a
    value under another algorithm's key (ITL-1488): the value's label must be the
    key's, and its digest exactly that algorithm's length.

    Args:
        key: The key the value would be stored under.
        value: The hash to store.

    Raises:
        ValueError: If the labels differ, the label names no algorithm with a fixed
            digest length, or the digest has another length.
    """
    if value.method != key.algorithm:
        raise ValueError(
            f"A {value.method!r} hash cannot be stored under a key whose label is "
            f"{key.algorithm!r}"
        )
    size = file_hash_digest_size(key.algorithm)
    if len(value.digest) != size:
        raise ValueError(
            f"A {key.algorithm!r} digest is {size} bytes, not {len(value.digest)}"
        )


def parse_stored_file_hash(blob: object, label: str) -> ContentHash | None:
    """Read a stored ``<label>:<digest>`` value strictly, for one algorithm.

    A persistent cacher stores ``ContentHash.to_prefixed_digest()``. The value is
    read only when it is exactly ``label``, a ``:`` and a digest of that
    algorithm's length; anything else (another label, a raw digest whose bytes
    happen to begin with ``<label>:``, a digest of another length) is a miss.

    Args:
        blob: The stored value, expected to contain binary data.
        label: The label of the key that was looked up.

    Returns:
        The stored hash, or ``None`` when the value is not a well-formed hash of
        ``label``.
    """
    if isinstance(blob, memoryview):
        blob = blob.tobytes()
    elif isinstance(blob, bytearray):
        blob = bytes(blob)
    if not isinstance(blob, bytes):
        return None
    prefix = stored_hash_prefix(label)
    if prefix is None or not blob.startswith(prefix):
        return None
    digest = blob[len(prefix) :]
    if len(digest) != file_hash_digest_size(label):
        return None
    return ContentHash(method=label, digest=digest)


class FileHasher:
    """Hash file content using a configurable algorithm.

    Args:
        algorithm: Hashing algorithm to use. Defaults to ``"sha256"``.
        buffer_size: Read buffer size in bytes. Defaults to 65536.
    """

    def __init__(
        self,
        algorithm: str = SHA256_ALGORITHM,
        buffer_size: int = 65536,
    ):
        self.algorithm = algorithm
        self.buffer_size = buffer_size

    @property
    def hash_algorithm(self) -> str:
        """The label of every hash this hasher returns: the algorithm's name."""
        return self.algorithm

    def hash_file(self, file_path: PathLike) -> ContentHash:
        """Hash the file at ``file_path`` and return its ``ContentHash``.

        Args:
            file_path: Path to the file to hash.

        Returns:
            ContentHash of the file's content.
        """
        return hash_file(
            file_path, algorithm=self.algorithm, buffer_size=self.buffer_size
        )


class CachedFileHasher:
    """File hasher that caches results to avoid redundant I/O.

    Wraps any ``FileContentHasherProtocol`` with a
    ``CacherProtocol[FileHashKey, ContentHash]``. On each call to
    ``hash_file``:

    1. Resolve the path to an absolute ``UPath`` and stat the file.
    2. Look up ``(path, mtime_ns, size, algorithm)`` in the cacher, using
       the inner hasher's ``hash_algorithm``. Each algorithm has its own entry.
    3. Accept a hit only if it is a ``ContentHash`` with the expected label and
       digest length.
    4. On miss: delegate to the inner hasher, store the result, return it.

    Both ``FileHasher`` and ``CachedFileHasher`` satisfy
    ``FileContentHasherProtocol`` — callers do not need to know which they
    have.

    The inner hasher's label must name an algorithm a cache can store: a
    ``hashlib`` algorithm with a fixed digest length, or ``hash_path``, ``xxh64``
    or ``crc32`` (every algorithm ``FileHasher`` can hash with qualifies). Any
    other label is refused here, before a file is hashed.

    Args:
        file_hasher: Inner hasher that performs the actual content hashing.
        cacher: Cache backend implementing
            ``CacherProtocol[FileHashKey, ContentHash]``.

    Raises:
        ValueError: If ``file_hasher.hash_algorithm`` names no algorithm with a fixed
            digest length.
    """

    def __init__(
        self,
        file_hasher: FileContentHasherProtocol,
        cacher: "CacherProtocol[FileHashKey, ContentHash]",
    ) -> None:
        _ = file_hash_digest_size(file_hasher.hash_algorithm)
        self.file_hasher = file_hasher
        self.cacher = cacher

    @property
    def hash_algorithm(self) -> str:
        """The inner hasher's label: the label of every hash this hasher returns."""
        return self.file_hasher.hash_algorithm

    def hash_file(self, file_path: PathLike) -> ContentHash:
        """Return the ``ContentHash`` for ``file_path``, using the cache.

        Args:
            file_path: Path to the file to hash.

        Returns:
            ContentHash of the file's content (from cache or computed).
        """
        path = file_path if isinstance(file_path, UPath) else UPath(file_path)
        path = path.resolve()
        stat = path.stat()
        key = FileHashKey(path, stat.st_mtime_ns, stat.st_size, self.hash_algorithm)

        hit = self.cacher.get(key)
        if isinstance(hit, ContentHash):
            try:
                require_storable(key, hit)
            except ValueError:
                pass  # Custom backends can return an incompatible cached hash.
            else:
                return hit

        result = self.file_hasher.hash_file(file_path)
        self.cacher.put(key, result)
        return result
