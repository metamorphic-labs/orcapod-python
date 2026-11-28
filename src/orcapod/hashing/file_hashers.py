from dataclasses import dataclass

from upath import UPath

from orcapod.hashing.hash_utils import (
    SHA256_ALGORITHM,
    file_hash_digest_size,
    hash_file,
    require_file_hash_algorithm,
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
            hash): ``"sha256"``, the only file hash algorithm (ITL-1155). A key
            with any other label is refused on ``put`` and a miss on ``get``.
    """

    path: UPath
    mtime_ns: int
    size: int
    algorithm: str


def stored_hash_format(label: str) -> tuple[bytes, int] | None:
    """Return the ASCII prefix and digest size for a supported cache label."""
    if not label.isascii():
        return None
    try:
        digest_size = file_hash_digest_size(label)
    except ValueError:
        return None
    return label.encode("ascii") + b":", digest_size


def _validated_file_digest(algorithm: str, value: ContentHash) -> bytes:
    """Snapshot the raw digest after checking the hash's label and digest size."""
    method = value.method
    if str.__eq__(algorithm, method) is not True:
        raise ValueError(f"File hash has label {method!r}, expected {algorithm!r}")
    size = file_hash_digest_size(algorithm)
    # Validate the same bytes that will be returned, even for a custom hash.
    raw_digest = value.digest
    digest = (
        raw_digest if type(raw_digest) is bytes else memoryview(raw_digest).tobytes()
    )
    if len(digest) != size:
        raise ValueError(f"A {algorithm!r} digest is {size} bytes, not {len(digest)}")
    return digest


def canonical_file_hash(algorithm: str, value: ContentHash) -> ContentHash:
    """Validate the file hash and ensure it uses canonical SHA-256 serialization."""
    digest = _validated_file_digest(algorithm, value)
    if (
        type(value) is ContentHash
        and type(value.method) is str
        and value.digest is digest
    ):
        return value
    return ContentHash(SHA256_ALGORITHM, digest)


def serialize_storable_file_hash(
    algorithm: str, value: ContentHash
) -> tuple[bytes, int, bytes]:
    """Return the key's prefix and digest size, and the canonical bytes to store."""
    digest = _validated_file_digest(algorithm, value)
    prefix = SHA256_ALGORITHM.encode("ascii") + b":"
    return prefix, len(digest), prefix + digest


def matches_stored_file_hash(blob: object, prefix: bytes, digest_size: int) -> bool:
    """Check the stored binary format using an already validated algorithm."""
    if isinstance(blob, memoryview):
        blob = blob.tobytes()
    return (
        isinstance(blob, (bytes, bytearray))
        and len(blob) == len(prefix) + digest_size
        and blob.startswith(prefix)
    )


def parse_stored_file_hash(
    blob: object, label: str, prefix: bytes, digest_size: int
) -> ContentHash | None:
    """Read a stored ``<label>:<digest>`` value strictly, for one algorithm.

    A persistent cacher stores the canonical prefix and raw digest. The value is
    read only when it is exactly ``label``, a ``:`` and a digest of that
    algorithm's length; anything else (another label, a raw digest whose bytes
    happen to begin with ``<label>:``, a digest of another length) is a miss.

    Args:
        blob: The stored value, expected to contain binary data.
        label: The label of the key that was looked up.
        prefix: The validated ASCII prefix of that label.
        digest_size: The validated digest size for that label.

    Returns:
        The stored hash, or ``None`` when the value is not a well-formed hash of
        ``label``.
    """
    if isinstance(blob, memoryview):
        blob = blob.tobytes()
    elif isinstance(blob, bytearray):
        blob = bytes(blob)
    if not isinstance(blob, bytes) or not matches_stored_file_hash(
        blob, prefix, digest_size
    ):
        return None
    return ContentHash(method=label, digest=blob[len(prefix) :])


class FileHasher:
    """Hash file content with SHA-256.

    ``sha256`` is the only file hash algorithm (ITL-1155): the file content hash
    carries the label ``sha256``, and no other algorithm has a defined label.

    Args:
        algorithm: The hash algorithm. Only ``"sha256"``, the default, is
            accepted.
        buffer_size: Read buffer size in bytes. Defaults to 65536.

    Raises:
        ValueError: If ``algorithm`` is not ``"sha256"``; the message names the
            algorithm given.
    """

    def __init__(
        self,
        algorithm: str = SHA256_ALGORITHM,
        buffer_size: int = 65536,
    ):
        require_file_hash_algorithm(algorithm)
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

    1. Validate the advertised algorithms, resolve the path to an absolute
       ``UPath``, and stat the file.
    2. Look up ``(path, mtime_ns, size, algorithm)`` in the cacher using the
       canonical SHA-256 label.
    3. Accept a hit only if it is a ``ContentHash`` with the expected label and
       digest length.
    4. On miss: delegate to the inner hasher, validate the result, and cache it.
       Both hits and newly computed results return a canonical ``ContentHash``.

    Both ``FileHasher`` and ``CachedFileHasher`` satisfy
    ``FileContentHasherProtocol`` — callers do not need to know which they
    have.

    The inner hasher's label must be ``sha256``, the only file hash algorithm
    (ITL-1155). Any other label is refused here, before a file is hashed.

    Args:
        file_hasher: Inner hasher that performs the actual content hashing.
        cacher: Cache backend implementing
            ``CacherProtocol[FileHashKey, ContentHash]``. A backend that advertises
            an algorithm must report ``"sha256"``.

    Raises:
        ValueError: If the file hasher or cacher advertises an unsupported
            algorithm.
    """

    def __init__(
        self,
        file_hasher: FileContentHasherProtocol,
        cacher: "CacherProtocol[FileHashKey, ContentHash]",
    ) -> None:
        self.file_hasher = file_hasher
        self.cacher = cacher
        self._validate_algorithms()

    def _validate_algorithms(self) -> None:
        """Refuse unsupported labels before hashing or consulting the cache."""
        require_file_hash_algorithm(self.hash_algorithm)
        for attribute in ("hash_algorithm", "algorithm"):
            require_file_hash_algorithm(
                getattr(self.cacher, attribute, SHA256_ALGORITHM)
            )

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
        self._validate_algorithms()
        path = file_path if isinstance(file_path, UPath) else UPath(file_path)
        path = path.resolve()
        stat = path.stat()
        key = FileHashKey(path, stat.st_mtime_ns, stat.st_size, SHA256_ALGORITHM)

        hit = self.cacher.get(key)
        if isinstance(hit, ContentHash):
            try:
                return canonical_file_hash(key.algorithm, hit)
            except (TypeError, ValueError):
                pass  # Custom backends can return an incompatible cached hash.

        result = canonical_file_hash(
            key.algorithm, self.file_hasher.hash_file(file_path)
        )
        self.cacher.put(key, result)
        return result
