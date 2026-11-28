"""CLI tests for ``orcapod warm-cache``."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from typer.testing import CliRunner
from upath import UPath


@pytest.fixture
def runner():
    return CliRunner()


class TestWarmCacheCLI:
    def test_help_exits_zero(self, runner):
        from orcapod.cli import app

        result = runner.invoke(app, ["warm-cache", "--help"])
        assert result.exit_code == 0
        assert "PATH" in result.output

    def test_basic_run(self, runner, tmp_path):
        db = tmp_path / "cache.db"
        f = tmp_path / "f.bin"
        f.write_bytes(b"x" * 20)

        from orcapod.cli import app

        result = runner.invoke(
            app,
            [
                "warm-cache",
                str(tmp_path),
                "--min-size",
                "0.00002",  # int(0.00002 * 1024 * 1024) = 20 bytes threshold; 20-byte file qualifies
                "--db-path",
                str(db),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "1 hashed" in result.output
        assert "0 errors." in result.output

    def test_forced_hash_conflict_exits_one(
        self, runner: CliRunner, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ):
        from orcapod.cli import app
        from orcapod.hashing.file_hashers import FileHasher, FileHashKey
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        files = tmp_path / "files"
        files.mkdir()
        changed = (files / "changed.bin").resolve()
        changed.write_bytes(b"AAA")
        db = tmp_path / "cache.db"
        args = ["warm-cache", str(files), "--min-size", "0", "--db-path", str(db)]
        original = FileHasher().hash_file(changed)
        warmed = runner.invoke(app, args)
        assert warmed.exit_code == 0, warmed.output

        stat = changed.stat()
        changed.write_bytes(b"BBB")
        os.utime(changed, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        (files / "clean.bin").write_bytes(b"CCC")

        result = runner.invoke(app, [*args, "--force"])

        assert "1 hashed" in result.output
        assert "1 errors." in result.output
        assert "Average hashing speed:" in result.output
        assert f"Failed to process file {changed}" in caplog.messages
        assert "Different hash already cached" in caplog.text
        key = FileHashKey(
            UPath(changed), stat.st_mtime_ns, stat.st_size, original.method
        )
        with SqliteHashCacher(db) as cacher:
            assert cacher.get(key) == original
        assert result.exit_code == 1, result.output

    def test_unreadable_file_exits_one(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ):
        from orcapod.cli import app

        files = tmp_path / "files"
        files.mkdir()
        unreadable = UPath((files / "unreadable.bin").resolve())
        unreadable.write_bytes(b"AAA")
        (files / "readable.bin").write_bytes(b"BBB")
        local_path_type = type(unreadable)
        open_file = local_path_type.open

        def deny_open(path: UPath, mode: str):
            if path == unreadable:
                raise PermissionError("permission denied")
            return open_file(path, mode)

        monkeypatch.setattr(local_path_type, "open", deny_open)
        db = tmp_path / "cache.db"
        result = runner.invoke(
            app, ["warm-cache", str(files), "--min-size", "0", "--db-path", str(db)]
        )

        assert "1 hashed" in result.output
        assert "1 errors." in result.output
        assert f"Failed to process file {unreadable}" in caplog.messages
        assert "PermissionError: permission denied" in caplog.text
        assert result.exit_code == 1, result.output

    def test_dry_run_with_a_file_error_still_exits_zero(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ):
        from orcapod.cli import app
        from orcapod.hashing.file_hashers import FileHashKey
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        files = tmp_path / "files"
        files.mkdir()
        blocked = UPath((files / "blocked.bin").resolve())
        blocked.write_bytes(b"AAA")
        (files / "clean.bin").write_bytes(b"BBB")
        get = SqliteHashCacher.get

        def fail_cache_check(cacher: SqliteHashCacher, key: FileHashKey):
            if key.path == blocked:
                raise OSError("cache lookup failed")
            return get(cacher, key)

        monkeypatch.setattr(SqliteHashCacher, "get", fail_cache_check)
        db = tmp_path / "cache.db"
        args = ["warm-cache", str(files), "--min-size", "0", "--db-path", str(db)]
        result = runner.invoke(app, [*args, "--dry-run"])

        assert result.exit_code == 0, result.output
        assert "Dry run complete" in result.output
        assert "1 would be hashed" in result.output
        assert "1 errors." in result.output
        assert f"Failed to check file {blocked}" in caplog.messages

    def test_already_cached_on_second_run(self, runner, tmp_path):
        db = tmp_path / "cache.db"
        f = tmp_path / "f.bin"
        f.write_bytes(b"x" * 20)

        from orcapod.cli import app

        args = [
            "warm-cache",
            str(tmp_path),
            "--min-size",
            "0.00002",
            "--db-path",
            str(db),
        ]
        runner.invoke(app, args)  # first run — populates cache
        result = runner.invoke(app, args)  # second run — all cached

        assert result.exit_code == 0
        assert "1 already cached" in result.output

    def test_nonexistent_path_exits_nonzero(self, runner):
        from orcapod.cli import app

        result = runner.invoke(app, ["warm-cache", "/nonexistent_xyz_abc_987"])
        assert result.exit_code != 0

    def test_min_size_default_shown_in_help(self, runner):
        from orcapod.cli import app

        result = runner.invoke(app, ["warm-cache", "--help"])
        assert "500 MB" in result.output  # default 500 MB should appear

    def test_workers_option_accepted(self, runner, tmp_path):
        db = tmp_path / "cache.db"
        f = tmp_path / "f.bin"
        f.write_bytes(b"x" * 20)

        from orcapod.cli import app

        result = runner.invoke(
            app,
            [
                "warm-cache",
                str(tmp_path),
                "--min-size",
                "0.00002",
                "--db-path",
                str(db),
                "--workers",
                "2",
            ],
        )
        assert result.exit_code == 0, result.output

    def test_file_path_exits_nonzero(self, runner, tmp_path):
        """Passing a file (not a directory) as PATH exits with a non-zero code."""
        f = tmp_path / "notadir.bin"
        f.write_bytes(b"x" * 10)

        from orcapod.cli import app

        result = runner.invoke(app, ["warm-cache", str(f)])
        assert result.exit_code != 0
        assert "not a directory" in result.output

    def test_speed_line_shown_when_files_hashed(self, runner, tmp_path):
        """'Average hashing speed:' line appears when at least one file was hashed."""
        db = tmp_path / "cache.db"
        f = tmp_path / "f.bin"
        f.write_bytes(b"x" * 20)

        from orcapod.cli import app

        result = runner.invoke(
            app,
            [
                "warm-cache",
                str(tmp_path),
                "--min-size",
                "0.00002",
                "--db-path",
                str(db),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Average hashing speed:" in result.output

    def test_speed_line_not_shown_when_nothing_hashed(self, runner, tmp_path):
        """'Average hashing speed:' line is suppressed when no files were hashed."""
        db = tmp_path / "cache.db"
        # File below threshold — will be skipped, not hashed.
        f = tmp_path / "tiny.bin"
        f.write_bytes(b"x" * 5)

        from orcapod.cli import app

        result = runner.invoke(
            app,
            [
                "warm-cache",
                str(tmp_path),
                "--min-size",
                "100",  # 100 MB threshold — tiny.bin won't qualify
                "--db-path",
                str(db),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Average hashing speed:" not in result.output

    def test_summary_output_contains_counts(self, runner, tmp_path):
        """Output summary line contains hashed, already cached, skipped, and errors counts."""
        db = tmp_path / "cache.db"
        f = tmp_path / "f.bin"
        f.write_bytes(b"x" * 20)

        from orcapod.cli import app

        result = runner.invoke(
            app,
            [
                "warm-cache",
                str(tmp_path),
                "--min-size",
                "0.00002",
                "--db-path",
                str(db),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "hashed" in result.output
        assert "already cached" in result.output
        assert "skipped" in result.output
        assert "errors" in result.output

    def test_workers_zero_exits_nonzero(self, runner, tmp_path):
        """--workers 0 exits with a non-zero code and prints a clear error."""
        from orcapod.cli import app

        result = runner.invoke(app, ["warm-cache", str(tmp_path), "--workers", "0"])
        assert result.exit_code != 0
        assert "workers" in result.output.lower()
