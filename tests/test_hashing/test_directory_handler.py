"""Tests for BasicDirectoryHasher and DirectoryHandler."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

import pytest
from typing_extensions import override
from upath import UPath

from orcapod.contexts import get_default_semantic_hasher
from orcapod.logical_types.directory_type import Directory
from orcapod.hashing.directory_hashers import BasicDirectoryHasher
from orcapod.hashing.file_hashers import FileHasher
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    DirectoryHandler,
    register_builtin_python_type_handlers,
)
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    PythonTypeHandlerRegistry,
)
from orcapod.types import ContentHash, PathLike


class TestBasicDirectoryHasher:
    def test_empty_directory_returns_content_hash(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        result = hasher.hash_directory(empty)
        assert isinstance(result, ContentHash)
        assert result.method == "directory_v1"

    def test_empty_directory_hash_is_stable(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2)

    def test_identical_content_same_hash(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "file.txt").write_bytes(b"hello")
        (d2 / "file.txt").write_bytes(b"hello")
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2)

    def test_different_content_different_hash(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "file.txt").write_bytes(b"hello")
        (d2 / "file.txt").write_bytes(b"world")
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        assert hasher.hash_directory(d1) != hasher.hash_directory(d2)

    def test_single_byte_change_in_nested_file_changes_hash(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        for base in (d1, d2):
            (base / "sub" / "deep").mkdir(parents=True)
            (base / "sub" / "deep" / "unchanged.txt").write_bytes(b"same content")
        (d1 / "sub" / "deep" / "target.txt").write_bytes(b"hello world")
        (d2 / "sub" / "deep" / "target.txt").write_bytes(b"hello World")  # capital W
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        assert hasher.hash_directory(d1) != hasher.hash_directory(d2)

    def test_adding_file_changes_hash(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        (d / "a.txt").write_bytes(b"content")
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        h1 = hasher.hash_directory(d)
        (d / "b.txt").write_bytes(b"new file")
        h2 = hasher.hash_directory(d)
        assert h1 != h2

    def test_removing_file_changes_hash(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        (d / "a.txt").write_bytes(b"content")
        (d / "b.txt").write_bytes(b"to remove")
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        h1 = hasher.hash_directory(d)
        (d / "b.txt").unlink()
        h2 = hasher.hash_directory(d)
        assert h1 != h2

    def test_hidden_files_included_by_default(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "file.txt").write_bytes(b"same")
        (d2 / "file.txt").write_bytes(b"same")
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        h1 = hasher.hash_directory(d1)
        (d2 / ".hidden").write_bytes(b"dotfile")
        h2 = hasher.hash_directory(d2)
        assert h1 != h2

    def test_ignore_glob_excludes_matching_files(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "app.py").write_bytes(b"code")
        (d2 / "app.py").write_bytes(b"code")
        (d2 / "app.pyc").write_bytes(b"compiled")  # extra .pyc, excluded
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2, ignore=["*.pyc"])

    def test_ignore_callable_excludes_entries(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "app.py").write_bytes(b"code")
        (d2 / "app.py").write_bytes(b"code")
        (d2 / "excluded.txt").write_bytes(b"extra")
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        h1 = hasher.hash_directory(d1)
        h2 = hasher.hash_directory(d2, ignore=lambda p: p.name == "excluded.txt")
        assert h1 == h2

    def test_symlink_recorded_not_followed(self, tmp_path):
        """Two dirs with identical symlinks to the same target → same hash."""
        real_file = tmp_path / "real.txt"
        real_file.write_bytes(b"content")
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "link").symlink_to(real_file)
        (d2 / "link").symlink_to(real_file)
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2)

    def test_symlink_cycle_safe(self, tmp_path):
        """A symlink pointing to an ancestor directory must not cause infinite recursion."""
        d = tmp_path / "d"
        d.mkdir()
        (d / "self_link").symlink_to(d)  # points to its own parent
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        result = hasher.hash_directory(d)  # must complete without error
        assert isinstance(result, ContentHash)

    def test_large_tree_smoke_test(self, tmp_path):
        """Hashing 200 files across 10 subdirectories must complete without error."""
        for i in range(10):
            sub = tmp_path / f"sub_{i:02d}"
            sub.mkdir()
            for j in range(20):
                (sub / f"file_{j:03d}.txt").write_bytes(f"content {i} {j}".encode())
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        result = hasher.hash_directory(tmp_path)
        assert isinstance(result, ContentHash)

    def test_rename_changes_hash(self, tmp_path):
        """Same content, different name → different hash."""
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "original.txt").write_bytes(b"content")
        (d2 / "renamed.txt").write_bytes(b"content")  # same content, different name
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        assert hasher.hash_directory(d1) != hasher.hash_directory(d2)

    def test_ignore_applied_recursively(self, tmp_path):
        """ignore filter is applied to entries inside nested subdirectories."""
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "sub").mkdir()
        (d2 / "sub").mkdir()
        (d1 / "sub" / "app.py").write_bytes(b"code")
        (d2 / "sub" / "app.py").write_bytes(b"code")
        (d2 / "sub" / "app.pyc").write_bytes(
            b"compiled"
        )  # nested .pyc, should be excluded
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2, ignore=["*.pyc"])

    def test_relative_path_pattern_scopes_to_subdirectory(self, tmp_path):
        """Pattern 'sub/*.pyc' must exclude .pyc files in sub/ but not in other/."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "sub").mkdir()
        (base / "other").mkdir()
        (base / "sub" / "app.py").write_bytes(b"code")
        (base / "sub" / "app.pyc").write_bytes(b"compiled in sub")
        (base / "other" / "app.py").write_bytes(b"code")

        # reference: same tree but with sub/app.pyc physically absent
        ref = tmp_path / "ref"
        ref.mkdir()
        (ref / "sub").mkdir()
        (ref / "other").mkdir()
        (ref / "sub" / "app.py").write_bytes(b"code")
        (ref / "other" / "app.py").write_bytes(b"code")

        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        # "sub/*.pyc" should exclude sub/app.pyc → same hash as ref
        assert hasher.hash_directory(
            base, ignore=["sub/*.pyc"]
        ) == hasher.hash_directory(ref)
        # "other/*.pyc" should not match sub/app.pyc → different hash from ref
        assert hasher.hash_directory(
            base, ignore=["other/*.pyc"]
        ) != hasher.hash_directory(ref)

    def test_ignore_none_is_equivalent_to_no_ignore(self, tmp_path):
        """hash(dir) == hash(dir, ignore=None) — the new arg in absent form must not shift the hash."""
        d = tmp_path / "d"
        d.mkdir()
        (d / "file.txt").write_bytes(b"content")
        (d / "sub").mkdir()
        (d / "sub" / "nested.txt").write_bytes(b"nested")
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        assert hasher.hash_directory(d) == hasher.hash_directory(d, ignore=None)

    def test_non_matching_filter_is_equivalent_to_no_filter(self, tmp_path):
        """A filter that matches nothing must produce the same hash as no filter."""
        d = tmp_path / "d"
        d.mkdir()
        (d / "file.txt").write_bytes(b"content")
        (d / "sub").mkdir()
        (d / "sub" / "nested.txt").write_bytes(b"nested")
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        assert hasher.hash_directory(d) == hasher.hash_directory(
            d, ignore=["*.nonexistent_xyz"]
        )

    def test_filter_identity_irrelevance(self, tmp_path):
        """Two patterns selecting the same effective file set must produce the same hash."""
        d = tmp_path / "d"
        d.mkdir()
        (d / "app.py").write_bytes(b"code")
        (d / "app.pyc").write_bytes(b"compiled")
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        # Both patterns exclude only app.pyc — results must be identical
        h1 = hasher.hash_directory(d, ignore=["*.pyc"])
        h2 = hasher.hash_directory(d, ignore=["app.pyc"])
        assert h1 == h2

    @pytest.mark.skipif(
        not hasattr(os, "mkfifo"), reason="mkfifo not available on this platform"
    )
    def test_special_files_skipped(self, tmp_path):
        """Named pipes (FIFOs) and other special files are silently skipped."""
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "file.txt").write_bytes(b"content")
        (d2 / "file.txt").write_bytes(b"content")
        os.mkfifo(str(d2 / "myfifo"))  # special file — should be silently skipped
        hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        # The FIFO is excluded from the hash, so both directories are identical
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2)


class TestDirectoryHandler:
    @pytest.fixture
    def handler(self):
        return DirectoryHandler(BasicDirectoryHasher(file_hasher=FileHasher()))

    @pytest.fixture
    def hasher(self):
        registry = PythonTypeHandlerRegistry()
        register_builtin_python_type_handlers(registry)
        return SemanticAwarePythonHasher(
            hasher_id="test_directory_v0",
            type_handler_registry=registry,
        )

    def test_returns_content_hash(self, handler, hasher, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        (d / "file.txt").write_bytes(b"content")
        result = handler.handle(Directory(d), hasher)
        assert isinstance(result, ContentHash)

    def test_rejects_non_directory_object(self, handler, hasher):
        with pytest.raises(TypeError, match="DirectoryHandler"):
            handler.handle("not_a_directory", hasher)

    def test_same_content_same_hash(self, handler, hasher, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "a.txt").write_bytes(b"identical")
        (d2 / "a.txt").write_bytes(b"identical")
        h1 = handler.handle(Directory(d1), hasher)
        h2 = handler.handle(Directory(d2), hasher)
        assert h1 == h2

    def test_different_content_different_hash(self, handler, hasher, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "a.txt").write_bytes(b"content A")
        (d2 / "a.txt").write_bytes(b"content B")
        h1 = handler.handle(Directory(d1), hasher)
        h2 = handler.handle(Directory(d2), hasher)
        assert h1 != h2

    def test_passes_ignore_to_hasher(self, handler, hasher, tmp_path):
        """ignore on the Directory instance is forwarded to the hasher."""
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "app.py").write_bytes(b"code")
        (d2 / "app.py").write_bytes(b"code")
        (d2 / "app.pyc").write_bytes(b"compiled")  # extra .pyc, should be excluded
        h1 = handler.handle(Directory(d1), hasher)
        h2 = handler.handle(Directory(d2, ignore=["*.pyc"]), hasher)
        assert h1 == h2


class _CustomDirectoryHasher:
    def __init__(self, result: ContentHash) -> None:
        self.algorithm: str = "sha256"
        self.result: ContentHash = result
        self.calls: int = 0

    def hash_directory(
        self,
        directory_path: PathLike,
        ignore: Callable[[UPath], bool] | list[str] | None = None,
    ) -> ContentHash:
        del directory_path, ignore
        self.calls += 1
        return self.result


def test_directory_handler_refuses_a_reassigned_custom_algorithm(
    tmp_path: Path,
) -> None:
    """Check the advertised algorithm again before invoking the custom hasher."""
    custom = _CustomDirectoryHasher(ContentHash("md5", b"\x00" * 16))
    handler = DirectoryHandler(custom)
    custom.algorithm = "md5"

    with pytest.raises(ValueError, match="directory_v1.*sha256.*'md5'"):
        _ = handler.handle(Directory(tmp_path), get_default_semantic_hasher())
    assert custom.calls == 0


@pytest.mark.parametrize(
    "result",
    [
        pytest.param(ContentHash("md5", b"\x00" * 32), id="wrong-label"),
        pytest.param(ContentHash("directory_v1", b"\x00" * 16), id="wrong-length"),
    ],
)
def test_directory_handler_rejects_an_invalid_custom_result(
    tmp_path: Path, result: ContentHash
) -> None:
    """A SHA-256 declaration cannot make an invalid result acceptable."""
    handler = DirectoryHandler(_CustomDirectoryHasher(result))

    with pytest.raises(ValueError, match="Directory hash"):
        _ = handler.handle(Directory(tmp_path), get_default_semantic_hasher())


def test_directory_handler_canonicalizes_custom_results(tmp_path: Path) -> None:
    """The returned directory hash serializes from its validated label and digest."""

    class MisencodedDirectoryHash(ContentHash):
        @override
        def to_prefixed_digest(self) -> bytes:
            return b"md5:" + self.digest

    digest = BasicDirectoryHasher(FileHasher()).hash_directory(tmp_path).digest
    handler = DirectoryHandler(
        _CustomDirectoryHasher(MisencodedDirectoryHash("directory_v1", digest))
    )

    result = handler.handle(Directory(tmp_path), get_default_semantic_hasher())

    assert result.to_prefixed_digest() == b"directory_v1:" + digest


def test_direct_directory_hasher_refuses_a_subclass_advertising_md5(
    tmp_path: Path,
) -> None:
    """Inherited directory hashing refuses a subclass with a mismatched declaration."""

    class ReconfiguredDirectory(BasicDirectoryHasher):
        _declared_algorithm: str = "sha256"

        @property
        @override
        def algorithm(self) -> str:
            return self._declared_algorithm

        @algorithm.setter
        def algorithm(self, value: str) -> None:
            self._declared_algorithm = value

    custom = ReconfiguredDirectory(FileHasher())
    custom.algorithm = "md5"
    with pytest.raises(ValueError, match="directory_v1.*sha256.*'md5'"):
        _ = custom.hash_directory(tmp_path)


def test_directory_handler_refuses_a_declaration_changed_during_traversal(
    tmp_path: Path,
) -> None:
    """A callback can change the declaration before the custom digest is returned."""
    _ = (tmp_path / "data").write_bytes(b"abc")

    class SwitchingDirectoryHasher:
        algorithm = "sha256"

        def hash_directory(
            self,
            directory_path: PathLike,
            ignore: Callable[[UPath], bool] | list[str] | None = None,
        ) -> ContentHash:
            assert callable(ignore)
            assert not ignore(UPath("data"))
            return ContentHash("directory_v1", b"\x00" * 32)

    custom = SwitchingDirectoryHasher()

    def ignore(path: UPath) -> bool:
        custom.algorithm = "md5"
        return False

    handler = DirectoryHandler(custom)
    with pytest.raises(ValueError, match="directory_v1.*sha256.*'md5'"):
        _ = handler.handle(
            Directory(tmp_path, ignore=ignore), get_default_semantic_hasher()
        )


def test_directory_handler_rejects_a_sha256_shaped_custom_directory_digest(
    tmp_path: Path,
) -> None:
    """The declared algorithm and digest length cannot certify a Merkle hash."""
    handler = DirectoryHandler(
        _CustomDirectoryHasher(ContentHash("directory_v1", bytes(32)))
    )
    with pytest.raises(ValueError, match="does not match.*directory_v1"):
        _ = handler.handle(Directory(tmp_path), get_default_semantic_hasher())


def test_verified_custom_directory_traversal_reuses_ignore_decisions(
    tmp_path: Path,
) -> None:
    """A conforming custom backend and its verifier share one callback per path."""
    _ = (tmp_path / "data").write_bytes(b"abc")
    _ = (tmp_path / "ignored").write_bytes(b"ignore me")
    basic = BasicDirectoryHasher(FileHasher())

    class CustomDirectoryHasher(BasicDirectoryHasher):
        pass

    seen: list[str] = []

    def ignore(path: UPath) -> bool:
        seen.append(path.name)
        return path.name == "ignored"

    handler = DirectoryHandler(CustomDirectoryHasher(FileHasher()))
    result = handler.handle(
        Directory(tmp_path, ignore=ignore), get_default_semantic_hasher()
    )
    assert result == basic.hash_directory(tmp_path, ignore=["ignored"])
    assert sorted(seen) == ["data", "ignored"]
