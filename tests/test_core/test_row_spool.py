"""Rows held by number, in memory and on local disk (``core/row_spool``, ITL-1464)."""

from __future__ import annotations

import asyncio
import errno
import gc
import os
import signal
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pytest

from orcapod.core.row_spool import RowSpool


def _table(start: int, rows: int) -> pa.Table:
    """Rows with two tags and two data columns, a list and an extension among them."""
    numbers = list(range(start, start + rows))
    return pa.table(
        {
            "session": pa.array([f"s{n % 3}" for n in numbers], pa.large_string()),
            "at": pa.array(
                [datetime.fromtimestamp(n, timezone.utc) for n in numbers],
                pa.timestamp("us", "UTC"),
            ),
            "values": pa.array(
                [None if n % 5 == 0 else [n, None] for n in numbers],
                pa.large_list(pa.int64()),
            ),
            "id": pa.ExtensionArray.from_storage(
                pa.uuid(),
                pa.array([n.to_bytes(16, "big") for n in numbers], pa.binary(16)),
            ),
        }
    )


def _filled(
    root: Path, sizes: list[int], memory_bytes: int
) -> tuple[RowSpool, pa.Table]:
    spool = RowSpool(memory_bytes, str(root))
    tables, start = [], 0
    for size in sizes:
        tables.append(_table(start, size))
        spool.append(tables[-1])
        start += size
    return spool, pa.concat_tables(tables)


def _files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.arrow"))


@pytest.mark.parametrize("memory_bytes", [0, 3_000, 10**9])
def test_rows_come_back_by_number_in_the_order_asked(
    tmp_path: Path, memory_bytes: int
) -> None:
    spool, rows = _filled(tmp_path, [1, 50, 0, 7, 120, 3], memory_bytes)
    assert spool.size == rows.num_rows
    assert bool(_files(tmp_path)) == (memory_bytes < 10**9)
    assert spool.memory_bytes <= max(memory_bytes, 0)
    rng = np.random.default_rng(0)
    for count in (1, 5, 400):  # 400: every row several times
        wanted = rng.integers(0, rows.num_rows, count)
        taken = spool.take(wanted)
        assert taken.schema.equals(rows.schema, check_metadata=True)
        assert taken.equals(rows.take(pa.array(wanted, pa.int64())))
    spool.close()


def test_a_field_flagged_non_nullable_keeps_the_null_it_holds(tmp_path: Path) -> None:
    schema = pa.schema([pa.field("a", pa.int64(), nullable=False, metadata={"k": "v"})])
    table = pa.Table.from_arrays([pa.array([1, None])], schema=schema)
    with RowSpool(0, str(tmp_path)) as spool:
        spool.append(table)
        taken = spool.take(np.array([1, 0]))
    assert taken.schema.equals(schema, check_metadata=True)
    assert taken.column("a").to_pylist() == [None, 1]


def test_a_slice_does_not_keep_its_parent(tmp_path: Path) -> None:
    spool = RowSpool(10**9, str(tmp_path))
    spool.append(_table(0, 10_000).slice(5, 2))
    held = spool._segments[0].table  # pyright: ignore[reportPrivateUsage]  # what it keeps alive
    assert held is not None and held.get_total_buffer_size() < 2_000
    assert spool.memory_bytes < 1_000
    assert spool.take(np.array([1])).equals(_table(6, 1))


@pytest.mark.parametrize("memory_bytes", [0, 3_000, 10**9])
def test_discarded_rows_free_their_segment_and_the_rest_stay(
    tmp_path: Path, memory_bytes: int
) -> None:
    spool, rows = _filled(tmp_path, [40, 40, 40], memory_bytes)
    spool.discard(np.arange(0, 120, 2))
    spool.discard(np.array([], dtype=np.int64))
    odd = np.arange(1, 120, 2)
    assert spool.take(odd).equals(rows.take(pa.array(odd, pa.int64())))
    spool.append(_table(120, 40))  # numbered after the discarded rows
    assert spool.take(np.array([159, 1])).equals(
        pa.concat_tables([_table(159, 1), _table(1, 1)]).combine_chunks()
    )
    spool.discard(odd)
    spool.discard(np.arange(120, 160))
    assert spool.memory_bytes == 0
    assert _files(tmp_path) == []


def test_rows_spilled_after_a_segment_between_them_was_freed(tmp_path: Path) -> None:
    spool = RowSpool(1 << 16, str(tmp_path))
    for start in (0, 10, 20):
        spool.append(_table(start, 10))
    spool.discard(np.arange(10, 20))
    spool.append(_table(30, 5_000))  # past the limit: what is in memory is written
    assert spool.memory_bytes == 0
    wanted = np.array([0, 25, 30, 5_029, 9])
    expected = pa.concat_tables([_table(0, 30), _table(30, 5_000)])
    assert spool.take(wanted).equals(expected.take(pa.array(wanted, pa.int64())))
    spool.close()


@pytest.mark.parametrize("memory_bytes", [0, 3_000, 10**9])
def test_the_whole_table_and_slices_of_it(tmp_path: Path, memory_bytes: int) -> None:
    spool, rows = _filled(tmp_path, [1, 50, 7, 120, 3], memory_bytes)
    assert spool.table().equals(rows, check_metadata=True)
    for start, length in ((0, 1), (1, 50), (40, 100), (170, 50), (181, 1), (500, 2)):
        part = spool.slice(start, length)
        assert part.schema.equals(rows.schema, check_metadata=True)
        assert part.equals(rows.slice(start, length))
    with pytest.raises(ValueError, match="slice"):
        _ = spool.slice(-1, 2)
    with pytest.raises(ValueError, match="slice"):
        _ = spool.slice(1, -2)
    spool.discard(np.array([3]))
    with pytest.raises(ValueError, match="discarded"):
        _ = spool.table()
    spool.close()
    with pytest.raises(ValueError, match="closed"):
        _ = spool.table()


def test_a_spool_nothing_was_appended_to_has_no_table(tmp_path: Path) -> None:
    spool = RowSpool(0, str(tmp_path))
    with pytest.raises(ValueError, match="nothing was appended"):
        _ = spool.table()
    spool.append(_table(0, 2))
    assert spool.slice(0, 0).equals(_table(0, 0))


def test_the_table_is_mapped_and_not_copied(tmp_path: Path) -> None:
    spool = RowSpool(0, str(tmp_path))
    for start in range(0, 40_000, 10_000):
        spool.append(_table(start, 10_000))
    before = pa.total_allocated_bytes()
    table = spool.table()
    size = table.nbytes
    assert size > 1_000_000
    assert pa.total_allocated_bytes() - before < size / 100
    spool.close()


@pytest.mark.skipif(sys.platform == "win32", reason="a mapped file cannot be deleted")
@pytest.mark.parametrize("memory_bytes", [0, 3_000, 10**9])
@pytest.mark.parametrize("end", ["close", "collect"])
def test_a_table_outlives_its_spool(
    tmp_path: Path, memory_bytes: int, end: str
) -> None:
    spool, rows = _filled(tmp_path, [40, 5_000, 40], memory_bytes)
    table, part = spool.table(), spool.slice(30, 100)
    if end == "close":
        spool.close()
    del spool
    gc.collect()
    assert _files(tmp_path) == []
    assert table.equals(rows, check_metadata=True)
    table.validate(full=True)
    del table
    gc.collect()
    assert part.equals(rows.slice(30, 100))


def test_nothing_is_written_below_the_limit(tmp_path: Path) -> None:
    spool, _ = _filled(tmp_path, [10, 10], 10**9)
    assert list(tmp_path.iterdir()) == []
    spool.close()


def _spool_directories(root: Path) -> list[Path]:
    return [p for d in root.glob("orcapod-spool-*") for p in d.iterdir() if p.is_dir()]


def test_close_removes_the_files_and_refuses_more_rows(tmp_path: Path) -> None:
    spool, _ = _filled(tmp_path, [10], 0)
    assert _files(tmp_path)
    spool.close()
    spool.close()
    assert _spool_directories(tmp_path) == []
    with pytest.raises(ValueError, match="closed"):
        spool.append(_table(0, 1))


def test_an_exception_removes_the_files(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="stop"), RowSpool(0, str(tmp_path)) as spool:
        spool.append(_table(0, 10))
        raise RuntimeError("stop")
    assert _spool_directories(tmp_path) == []


@pytest.mark.asyncio
async def test_a_cancellation_removes_the_files(tmp_path: Path) -> None:
    async def hold() -> None:
        with RowSpool(0, str(tmp_path)) as spool:
            spool.append(_table(0, 10))
            await asyncio.Event().wait()

    task = asyncio.create_task(hold())
    await asyncio.sleep(0)
    assert _files(tmp_path)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert _spool_directories(tmp_path) == []


def test_a_collected_spool_removes_its_files(tmp_path: Path) -> None:
    spool, _ = _filled(tmp_path, [10], 0)
    assert _files(tmp_path)
    del spool
    gc.collect()
    assert _spool_directories(tmp_path) == []


_CHILD = """
import os, sys
import pyarrow as pa
from orcapod.core.row_spool import RowSpool
spool = RowSpool(0, sys.argv[1])
spool.append(pa.table({"a": [1]}))
print("ready", flush=True)
if sys.argv[2] == "kill":
    os.kill(os.getpid(), 9)
if sys.argv[2] == "wait":
    sys.stdin.read()
"""


def _child(root: Path, end: str) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-c", _CHILD, str(root), end],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )


def test_a_process_that_ends_normally_leaves_nothing(tmp_path: Path) -> None:
    child = _child(tmp_path, "exit")
    _ = child.communicate()
    assert child.returncode == 0
    assert list(tmp_path.glob("orcapod-spool-*")) == []


@pytest.mark.skipif(sys.platform == "win32", reason="swept through fcntl locks")
def test_a_killed_process_is_swept_and_a_live_one_is_not(tmp_path: Path) -> None:
    killed = _child(tmp_path, "kill")
    _ = killed.communicate()
    (left,) = tmp_path.glob("orcapod-spool-*")
    assert _files(left)
    live = _child(tmp_path, "wait")
    assert live.stdout is not None
    assert live.stdout.readline() == "ready\n"
    try:
        assert not left.exists()  # the second process swept it
        (held,) = tmp_path.glob("orcapod-spool-*")
        other = tmp_path / "orcapod-spool-not-ours"  # no lock file: left alone
        other.mkdir()
        (other / "file").touch()
        spool, _ = _filled(tmp_path, [10], 0)
        assert held.exists() and _files(held)
        assert other.exists()
        spool.close()
    finally:
        _ = live.communicate()
    assert not held.exists()


_FORK = """
import os, sys, signal, threading
import numpy as np, pyarrow as pa
from orcapod.core import row_spool
from orcapod.core.row_spool import RowSpool
case, root = sys.argv[1:]
table = pa.table({"x": [1, 2], "y": [3, 4]})

def in_child(function):
    child = os.fork()
    if child == 0:
        signal.alarm(10)
        try:
            function()
        except BaseException as error:
            print(repr(error), file=sys.stderr, flush=True)
            os._exit(1)
        os._exit(0)
    assert os.waitstatus_to_exitcode(os.waitpid(child, 0)[1]) == 0

if case == "guard_held":
    # Another thread makes a directory while this one forks.
    entered, release = threading.Event(), threading.Event()
    def hold():
        with row_spool._directories._guard:
            entered.set()
            release.wait()
    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    assert entered.wait(10)
    def spill():
        with RowSpool(0, root) as spool:
            spool.append(table)
            assert spool.take(np.array([1])).equals(table.slice(1, 1))
    try:
        in_child(spill)
    finally:
        release.set()
        thread.join()
elif case == "inherited":
    spool = RowSpool(0, root)
    spool.append(table)
    def use():
        for call in (
            lambda: spool.discard(np.array([0, 1])),
            lambda: spool.take(np.array([0])),
            lambda: spool.append(table),
        ):
            try:
                call()
            except RuntimeError as error:
                assert "the process that made it" in str(error)
            else:
                raise AssertionError("a child used its parent's spool")
        spool.close()
        row_spool._directories.remove()
        with RowSpool(0, root) as own:  # sweeps, and leaves the parent's
            own.append(table)
    in_child(use)
    assert spool.take(np.array([1, 0])).equals(table.take(pa.array([1, 0])))
    spool.close()
elif case in ("root_lock", "unpublished"):
    # A fork while another thread has just opened the root's lock file, or has made a
    # directory and not yet recorded it; then the parent is killed holding the lock.
    opened, resume = threading.Event(), threading.Event()
    name = "fdopen" if case == "root_lock" else "_make_process_directory"
    where = os if case == "root_lock" else row_spool
    original = getattr(where, name)
    def paused(*args, **kwargs):
        result = original(*args, **kwargs)
        opened.set()
        resume.wait(30)
        return result
    setattr(where, name, paused)
    if case == "root_lock":
        import fcntl
        locked, take_lock = threading.Event(), fcntl.lockf
        def lockf(*args):
            take_lock(*args)
            locked.set()
            threading.Event().wait(30)
        fcntl.lockf = lockf
    spool = RowSpool(0, root)
    threading.Thread(target=spool.append, args=(table,), daemon=True).start()
    assert opened.wait(10)
    ready, wait = os.pipe()
    child = os.fork()
    if child == 0:
        signal.alarm(120)  # never left behind
        os.write(wait, b"x")
        os.close(1)
        os.close(2)
        signal.pause()
    os.read(ready, 1)
    if case == "root_lock":
        resume.set()
        assert locked.wait(10)
    print(child, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)
elif case == "outlived":
    # A child that outlives its parent does not keep the parent's directory locked.
    spool = RowSpool(0, root)
    spool.append(table)
    ready, wait = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(0)
        os.write(wait, b"x")
        os.close(1)
        os.close(2)
        signal.alarm(120)  # never left behind
        signal.pause()
    os.read(ready, 1)
    print(child, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)
"""


def _fork(case: str, root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", _FORK, case, str(root)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
@pytest.mark.parametrize("case", ["guard_held", "inherited"])
def test_a_forked_child_neither_waits_for_nor_touches_its_parents_spools(
    tmp_path: Path, case: str
) -> None:
    done = _fork(case, tmp_path)
    assert done.returncode == 0, done.stderr
    # The child ended without its exit handlers (``os._exit``): its own directory is
    # left, with no owner, for the next process to sweep.
    (left,) = tmp_path.glob("orcapod-spool-*")
    spool, _ = _filled(tmp_path, [10], 0)
    assert not left.exists()
    spool.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
@pytest.mark.parametrize("case", ["outlived", "root_lock", "unpublished"])
def test_a_child_that_outlives_its_killed_parent_holds_none_of_its_locks(
    tmp_path: Path, case: str
) -> None:
    done = _fork(case, tmp_path)
    left = list(tmp_path.glob("orcapod-spool-*"))
    assert bool(left) == (case != "root_lock")
    child = int(done.stdout)
    try:
        # Another process spills at once, and sweeps what the killed one left.
        other = _child(tmp_path, "exit")
        _ = other.communicate(timeout=30)
        assert other.returncode == 0
        assert not any(directory.exists() for directory in left)
        spool, _ = _filled(tmp_path, [10], 0)
        spool.close()
    finally:
        os.kill(child, signal.SIGKILL)


@pytest.mark.parametrize("stage", ["write", "close"])
def test_a_failed_spill_leaves_the_spool_as_it_was(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    spool, rows = _filled(tmp_path, [40, 10], 1 << 14)
    held, size = spool.memory_bytes, spool.size
    assert held and _files(tmp_path) == []

    class Full:
        """A file writer that writes some bytes, then finds the disk full."""

        def __init__(self, sink: Any, schema: pa.Schema) -> None:
            self.sink = sink

        def __enter__(self) -> Full:
            return self

        def __exit__(self, *exc: object) -> None:
            if stage == "close":
                raise OSError(errno.ENOSPC, "no space")

        def write_table(self, table: pa.Table) -> None:
            self.sink.write(b"part of a file")
            if stage == "write":
                raise OSError(errno.ENOSPC, "no space")

    with monkeypatch.context() as patched:
        patched.setattr(pa.ipc, "new_file", Full)
        for _ in range(2):
            with pytest.raises(OSError, match="no space"):
                spool.append(_table(50, 5_000))
            assert (spool.memory_bytes, spool.size) == (held, size)
            assert _files(tmp_path) == []
    everything = np.arange(50)
    assert spool.take(everything).equals(rows.combine_chunks())
    spool.append(_table(50, 5_000))  # the retry: the rows once
    assert spool.size == 5_050
    assert spool.take(np.array([5_049, 0, 50])).equals(
        _table(0, 5_050).take(pa.array([5_049, 0, 50], pa.int64()))
    )
    assert len(_files(tmp_path)) == 1
    spool.close()


def test_a_first_append_that_fails_decides_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def full(*args: object) -> None:
        raise OSError(errno.ENOSPC, "no space")

    spool = RowSpool(0, str(tmp_path))
    with monkeypatch.context() as patched:
        patched.setattr(pa.ipc, "new_file", full)
        with pytest.raises(OSError, match="no space"):
            spool.append(_table(0, 3))
    other = pa.table({"a": [1, 2], "b": [3, 4]})  # another schema: nothing was held
    spool.append(other)
    assert spool.take(np.array([1, 0])).equals(other.take(pa.array([1, 0], pa.int64())))
    spool.close()


def test_a_closed_spool_refuses_every_use(tmp_path: Path) -> None:
    spool, _ = _filled(tmp_path, [10], 0)
    spool.close()
    with pytest.raises(ValueError, match="closed"):
        _ = spool.take(np.array([0]))
    with pytest.raises(ValueError, match="closed"):
        spool.discard(np.array([0]))


@pytest.mark.skipif(sys.platform == "win32", reason="swept through fcntl locks")
def test_a_process_killed_before_it_locked_its_directory_is_swept(
    tmp_path: Path,
) -> None:
    left = tmp_path / "orcapod-spool-unlocked"
    left.mkdir()
    spool, _ = _filled(tmp_path, [10], 0)
    assert not left.exists()
    spool.close()


@pytest.mark.skipif(sys.platform == "win32", reason="swept through fcntl locks")
def test_a_removal_that_stopped_part_way_is_finished(tmp_path: Path) -> None:
    left = tmp_path / "orcapod-spool-partly"
    (left / "some-spool").mkdir(parents=True)
    (left / "some-spool" / "0.arrow").touch()
    (left / "lock").touch()
    spool, _ = _filled(tmp_path, [10], 0)
    assert not left.exists()
    spool.close()


def test_a_process_never_sweeps_its_own_directory(tmp_path: Path) -> None:
    """Its own record lock would not stop it: also when a root has two names."""
    root, alias = tmp_path / "root", tmp_path / "alias"
    root.mkdir()
    alias.symlink_to(root, target_is_directory=True)
    first, rows = _filled(root, [10], 0)
    second, _ = _filled(alias, [10], 0)
    assert len(list(root.glob("orcapod-spool-*"))) == 2
    # And the first directory is still locked for every other process.
    other = _child(root, "exit")
    _ = other.communicate()
    assert other.returncode == 0
    assert first.take(np.array([9])).equals(rows.slice(9, 1))
    first.close()
    second.close()


@pytest.mark.parametrize("read", ["take", "table", "slice"])
def test_a_close_waits_for_a_read_of_another_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, read: str
) -> None:
    spool, rows = _filled(tmp_path, [10, 10], 0)
    about_to_map, go_on = threading.Event(), threading.Event()
    original = pa.memory_map

    def parked(path: str) -> Any:
        about_to_map.set()
        assert go_on.wait(10)
        return original(path)

    monkeypatch.setattr(pa, "memory_map", parked)
    reads: dict[str, Any] = {
        "take": lambda: spool.take(np.arange(20)),
        "table": spool.table,
        "slice": lambda: spool.slice(0, 20),
    }
    with ThreadPoolExecutor(2) as pool:
        result = pool.submit(reads[read])
        assert about_to_map.wait(10)
        closed = pool.submit(spool.close)
        with pytest.raises(FutureTimeout):
            closed.result(timeout=0.1)
        go_on.set()
        assert result.result(10).equals(rows)
        closed.result(10)
    assert _files(tmp_path) == []
    with pytest.raises(ValueError, match="closed"):
        reads[read]()


def test_threads_append_and_read_one_spool(tmp_path: Path) -> None:
    spool = RowSpool(1 << 12, str(tmp_path))

    def work(number: int) -> None:
        for i in range(20):
            spool.append(pa.table({"a": [number] * 5, "b": [i] * 5}))
            rows = np.arange(0, spool.size, 7)
            assert spool.take(rows).num_rows == len(rows)
            _ = spool.table()

    with ThreadPoolExecutor(4) as pool:
        for done in [pool.submit(work, n) for n in range(4)]:
            done.result(60)
    assert spool.size == spool.table().num_rows == 400
    counts = spool.table().group_by(["a", "b"]).aggregate([([], "count_all")])
    assert set(counts.column("count_all").to_pylist()) == {5}
    spool.close()
