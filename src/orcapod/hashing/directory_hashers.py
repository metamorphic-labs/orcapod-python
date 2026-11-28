"""BasicDirectoryHasher — recursive Merkle tree hashing for directory trees."""

from __future__ import annotations

import hashlib
import logging
import os
from collections.abc import Callable, Iterable
from pathlib import PurePosixPath

from upath import UPath

from orcapod.hashing.file_hashers import FileHasher, verified_file_hash
from orcapod.hashing.hash_utils import (
    SHA256_ALGORITHM,
    SHA256_DIGEST_SIZE,
    require_file_hash_algorithm,
)
from orcapod.protocols.hashing_protocols import (
    DirectoryHasherProtocol,
    FileContentHasherProtocol,
)
from orcapod.types import ContentHash, PathLike

logger = logging.getLogger(__name__)

DIRECTORY_HASH_LABEL = "directory_v1"
"""``ContentHash.method`` of a directory hash: the single definition of the label.

It names the whole procedure of this module: a Merkle tree over the directory's
entries with SHA-256 entry and node digests, file leaves taken from the file hasher's
hash with its label (``b"<label>:" + digest``: a hash fed into another hash carries its
label, ITL-1154), symlinks recorded by target. A change that gives an existing directory a
different digest takes a new label (``directory_v2``).
"""


def require_directory_hash_algorithm(algorithm: object) -> None:
    """Require SHA-256 for the entry and node digests of ``directory_v1``."""
    if str.__eq__(SHA256_ALGORITHM, algorithm) is not True:
        raise ValueError(
            f"{DIRECTORY_HASH_LABEL} is defined over {SHA256_ALGORITHM} "
            f"entry and node digests; got algorithm {algorithm!r}"
        )


def canonical_directory_hash(value: ContentHash) -> ContentHash:
    """Return a validated directory hash with canonical serialization."""
    method = value.method
    if str.__eq__(DIRECTORY_HASH_LABEL, method) is not True:
        raise ValueError(
            f"Directory hash has label {method!r}, expected {DIRECTORY_HASH_LABEL!r}"
        )
    digest = memoryview(value.digest).tobytes()
    if len(digest) != SHA256_DIGEST_SIZE:
        raise ValueError(
            f"Directory hash must have {SHA256_DIGEST_SIZE} bytes, got {len(digest)}"
        )
    return ContentHash(DIRECTORY_HASH_LABEL, digest)


def _materialize_ignore_patterns(ignore: Iterable[str]) -> tuple[str, ...]:
    """Validate an iterable of glob patterns and consume it once."""
    if isinstance(ignore, str):
        raise TypeError(
            f"ignore= expects an iterable of pattern strings, not a bare string. "
            f"Pass a list instead: ignore=[{ignore!r}]"
        )
    return tuple(ignore)


def _compile_ignore(
    ignore: Callable[[UPath], bool] | Iterable[str] | None,
) -> Callable[[UPath], bool] | None:
    """Convert an ignore spec to a single callable filter.

    Args:
        ignore: ``None`` (no filtering), an iterable of glob patterns matched against
            the POSIX relative path from the root via ``pathlib.PurePosixPath.match()``
            (right-anchored: ``"*.pyc"`` matches any ``.pyc`` at any depth;
            ``"sub/*.pyc"`` matches any ``.pyc`` in any directory named ``sub/`` at
            any depth, not just at the root level), or a callable
            ``(UPath) -> bool`` receiving the relative path from the root.

    Returns:
        A callable ``(UPath) -> bool`` returning ``True`` to exclude an entry,
        or ``None`` if no filtering is needed.
    """
    if ignore is None:
        return None
    if callable(ignore):
        return ignore
    patterns = _materialize_ignore_patterns(ignore)

    def _glob_filter(relative: UPath) -> bool:
        return any(PurePosixPath(relative.as_posix()).match(pat) for pat in patterns)

    return _glob_filter


def _hash_dir(
    path: UPath,
    root: UPath,
    filter_fn: Callable[[UPath], bool] | None,
    file_hasher: FileContentHasherProtocol,
) -> bytes:
    """Recursively compute the Merkle hash of a directory.

    Args:
        path: The directory to hash (current recursion node).
        root: The top-level directory passed to ``BasicDirectoryHasher.hash_directory``.
            Used to compute relative paths for filter evaluation.
        filter_fn: Optional filter callable; receives the relative ``UPath`` from
            ``root`` and returns ``True`` to exclude the entry. Excluding a directory
            entry also implicitly excludes its entire subtree (the recursive call is
            skipped for excluded directories).
        file_hasher: Hasher used to compute ``ContentHash`` for each file leaf.

    Returns:
        The raw digest bytes for this directory node.
    """
    entries: list[tuple[bytes, bytes]] = []

    for child in path.iterdir():
        try:
            relative = child.relative_to(root)
        except ValueError as exc:
            raise RuntimeError(
                f"BasicDirectoryHasher: child path {child!r} is not relative to root {root!r}"
            ) from exc
        if filter_fn is not None and filter_fn(relative):
            continue

        name_bytes = child.name.encode("utf-8")

        if child.is_symlink():
            # Hash-as-symlink: record the link target, never dereference.
            target = os.readlink(child)
            entry_bytes = b"symlink\x00" + name_bytes + b"\x00" + target.encode("utf-8")
        elif child.is_file():
            # File hashers accept UPath directly, including remote paths.
            file_hash = verified_file_hash(file_hasher, child)  # pyright: ignore[reportArgumentType]
            # The file hash comes from another hasher: it enters with its label.
            entry_bytes = (
                b"file\x00" + name_bytes + b"\x00" + file_hash.to_prefixed_digest()
            )
        elif child.is_dir():
            subdir_digest = _hash_dir(child, root, filter_fn, file_hasher)
            entry_bytes = b"dir\x00" + name_bytes + b"\x00" + subdir_digest
        else:
            # Special file (socket, device node, named pipe) — skip silently.
            logger.debug("BasicDirectoryHasher: skipping special file %s", child)
            continue

        entry_digest = hashlib.sha256(entry_bytes).digest()
        entries.append((name_bytes, entry_digest))

    # Sort byte-wise by name — locale-independent, deterministic.
    entries.sort(key=lambda x: x[0])

    h = hashlib.sha256()
    h.update(b"directory\x00")
    for _, entry_digest in entries:
        h.update(entry_digest)
    return h.digest()


class BasicDirectoryHasher:
    """Recursive Merkle tree hasher for directory trees.

    Computes a stable content hash of a directory tree using a recursive Merkle scheme:
    file leaves hash their content via the injected ``file_hasher``, subdirectory nodes
    hash their sorted children, and the root hash propagates the entire tree. Symlinks
    are recorded as ``(symlink, target)`` without dereferencing — cycle-safe and
    deterministic.

    Pass a ``CachedFileHasher`` as ``file_hasher`` to avoid re-reading unchanged files
    on repeated calls (e.g. across pipeline runs).

    Args:
        file_hasher: File content hasher reporting the ``sha256`` algorithm,
            used to compute the ``ContentHash`` for each file leaf.
        algorithm: Hash algorithm for structural (Merkle entry and node) hashing.
            Only ``"sha256"`` (the default) is accepted: the label ``directory_v1``
            names the SHA-256 procedure and nothing else.
        buffer_size: Retained for context round-trip use. File content buffer sizing
            is owned by ``file_hasher``. Defaults to 65536.

    Raises:
        ValueError: If the requested or file hasher algorithm is unsupported.

    Example:
        >>> from orcapod.hashing.file_hashers import FileHasher
        >>> hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        >>> result = hasher.hash_directory("/tmp/mydir")
        >>> result.method
        'directory_v1'
    """

    def __init__(
        self,
        file_hasher: FileContentHasherProtocol,
        algorithm: str = SHA256_ALGORITHM,
        buffer_size: int = 65536,
    ) -> None:
        require_directory_hash_algorithm(algorithm)
        require_file_hash_algorithm(file_hasher.hash_algorithm)
        self.file_hasher = file_hasher
        self.buffer_size = buffer_size

    @property
    def algorithm(self) -> str:
        """The directory hasher's structural algorithm, SHA-256."""
        return SHA256_ALGORITHM

    def hash_directory(
        self,
        directory_path: PathLike,
        ignore: Callable[[UPath], bool] | Iterable[str] | None = None,
    ) -> ContentHash:
        """Compute the recursive Merkle hash of a directory tree.

        Args:
            directory_path: Path to the directory to hash.
            ignore: Optional filter. An iterable of glob patterns matched against the
                **POSIX relative path from the root** via ``pathlib.PurePosixPath.match()``
                (right-anchored: ``"*.pyc"`` matches any ``.pyc`` at any depth;
                ``"sub/*.pyc"`` matches any ``.pyc`` in any directory named ``sub/``
                at any depth, not just at the root level),
                or a callable ``(UPath) -> bool`` receiving the relative path from the
                root and returning ``True`` to exclude an entry. Applied at every level
                of recursion.

                Hash invariant: excluded entries are invisible to the hash. The result
                is identical to those entries never existing. The pattern string itself
                is not input to the hash. Excluding a directory entry also excludes its
                entire subtree.

        Returns:
            A ``ContentHash`` with ``method="directory_v1"`` (``DIRECTORY_HASH_LABEL``).

        Raises:
            ValueError: If a hasher advertises an unsupported algorithm or a custom
                file hash does not match SHA-256 of the file bytes.
            FileNotFoundError: If ``directory_path`` does not exist.
            NotADirectoryError: If ``directory_path`` is not a directory.
            PermissionError: If the directory is not traversable.

        Example:
            >>> from orcapod.hashing.file_hashers import FileHasher
            >>> hasher = BasicDirectoryHasher(file_hasher=FileHasher())
            >>> result = hasher.hash_directory("/tmp/mydir")
            >>> result.method
            'directory_v1'
            >>> # Exclude all .pyc files at any depth
            >>> result = hasher.hash_directory("/tmp/mydir", ignore=["*.pyc"])
            >>> # Exclude .pyc only inside build/
            >>> result = hasher.hash_directory("/tmp/mydir", ignore=["build/*.pyc"])
            >>> # Custom callable filter (receives relative UPath from root)
            >>> result = hasher.hash_directory("/tmp/mydir", ignore=lambda p: p.name.startswith("."))
        """
        require_directory_hash_algorithm(self.algorithm)
        file_hasher = self.file_hasher
        require_file_hash_algorithm(file_hasher.hash_algorithm)
        path = UPath(directory_path)
        filter_fn = _compile_ignore(ignore)
        digest = _hash_dir(path, path, filter_fn, file_hasher)
        return ContentHash(method=DIRECTORY_HASH_LABEL, digest=digest)


def verified_directory_hash(
    directory_hasher: DirectoryHasherProtocol,
    path: PathLike,
    ignore: Callable[[UPath], bool] | Iterable[str] | None,
) -> ContentHash:
    """Hash a directory and verify custom results against the library's Merkle tree."""
    require_directory_hash_algorithm(getattr(directory_hasher, "algorithm", None))
    if type(directory_hasher) is BasicDirectoryHasher:
        return directory_hasher.hash_directory(path, ignore=ignore)

    if callable(ignore):
        # Reuse the custom traversal's decisions. The caller's filter can have
        # side effects, so it should run only once per relative path.
        filter_fn = ignore
        decisions: dict[str, bool] = {}

        def once(relative: UPath) -> bool:
            key = relative.as_posix()
            if key not in decisions:
                decisions[key] = filter_fn(relative)
            return decisions[key]

        ignore = once
        custom_ignore = once
    elif ignore is not None:
        ignore = _materialize_ignore_patterns(ignore)
        custom_ignore = list(ignore)
    else:
        custom_ignore = None

    reported = directory_hasher.hash_directory(path, ignore=custom_ignore)
    require_directory_hash_algorithm(getattr(directory_hasher, "algorithm", None))
    canonical = canonical_directory_hash(reported)
    computed = BasicDirectoryHasher(FileHasher()).hash_directory(path, ignore=ignore)
    if canonical.digest != computed.digest:
        raise ValueError(f"Directory hash does not match {DIRECTORY_HASH_LABEL}")
    return computed
