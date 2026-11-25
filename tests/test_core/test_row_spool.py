"""Rows held by number, in memory and on local disk (``core/row_spool``, ITL-1464)."""

from __future__ import annotations

import asyncio
import gc
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from orcapod.core import row_spool
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
        spool, _ = _filled(tmp_path, [10], 0)
        assert held.exists() and _files(held)
        assert other.exists()
        spool.close()
    finally:
        _ = live.communicate()
    assert not held.exists()


def test_a_forked_child_never_removes_its_parents_files(tmp_path: Path) -> None:
    # A child inherits its parent's finalizers; the owner check is what stops them.
    directory = tmp_path / "spool"
    directory.mkdir()
    row_spool._remove_directory(str(directory), os.getpid() + 1)  # pyright: ignore[reportPrivateUsage]  # the owner check itself
    assert directory.exists()
    row_spool._remove_directory(str(directory), os.getpid())  # pyright: ignore[reportPrivateUsage]  # the owner check itself
    assert not directory.exists()
