"""Rows held for a later fetch by row number: some in memory, the rest on local disk.

A join on the async path keeps its inputs' join keys in memory and the rows behind them
here (ITL-1464). ``RowSpool.append`` numbers rows in the order they come; ``take`` fetches
rows by those numbers; ``discard`` says that rows are never fetched again.

Format: rows wait in memory until they exceed a byte limit, then become one segment: an
Arrow IPC file of one record batch, uncompressed, read memory-mapped (a mapped page is
the operating system's to drop, not memory this process holds). An extension column is
written as its storage and wrapped again when read, so reading needs no registered type.

Lifetime: a segment is deleted once every row of it is discarded; a spool's files are
removed by ``close`` (a spool is a context manager: normal end, exception and
cancellation alike) and when it is collected. A process keeps its spools in one directory
of its own, removed at interpreter exit. A killed process leaves its directory behind; the
next process to spill there removes it: every directory holds a lock file its process
keeps locked, so a directory whose lock is free has no owner. (Without ``fcntl``, as on
Windows, nothing is swept.) A spool is not a store: nothing in it outlives the run, is
found by a later process as rows, or enters a hash.
"""

from __future__ import annotations

import atexit
import os
import shutil
import tempfile
import threading
import weakref
from typing import IO, Any, Self

import numpy as np
import numpy.typing as npt
import pyarrow as pa

try:
    import fcntl
except ImportError:  # no such module on Windows
    fcntl = None

SPOOL_MEMORY_BYTES = 64 * 1024 * 1024
"""How many bytes of rows a spool keeps in memory before they become a segment."""

_PREFIX = "orcapod-spool-"
_LOCK_NAME = "lock"


class _ProcessDirectories:
    """The directory of this process under each root, made and swept on first use."""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._pid: int = os.getpid()
        self._held: dict[str, tuple[str, IO[str]]] = {}

    def get(self, root: str) -> str:
        """This process's directory under ``root``."""
        with self._guard:
            if self._pid != os.getpid():
                # A forked child: the directories held are its parent's.
                self._pid, self._held = os.getpid(), {}
            if root not in self._held or not os.path.isdir(self._held[root][0]):
                self._held[root] = _make_process_directory(root)
            return self._held[root][0]

    def remove(self) -> None:
        """Remove this process's directories (at exit)."""
        if self._pid != os.getpid():
            return  # a forked child holds none of its own
        for directory, lock in self._held.values():
            shutil.rmtree(directory, ignore_errors=True)
            lock.close()
        self._held = {}


def _make_process_directory(root: str) -> tuple[str, IO[str]]:
    """Make this process's directory under ``root``, locked, and sweep abandoned ones.

    Both happen under one lock of the root, so a sweep never sees a directory that is
    made but not yet locked.
    """
    os.makedirs(root, exist_ok=True)
    if fcntl is None:
        directory = tempfile.mkdtemp(prefix=_PREFIX, dir=root)
        return directory, open(os.path.join(directory, _LOCK_NAME), "w")
    name = os.path.join(root, f".{_PREFIX}{os.getuid()}.lock")
    flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC
    with os.fdopen(os.open(name, flags, 0o600), "w") as root_lock:
        fcntl.flock(root_lock, fcntl.LOCK_EX)
        _sweep(root)
        directory = tempfile.mkdtemp(prefix=_PREFIX, dir=root)
        lock = open(os.path.join(directory, _LOCK_NAME), "w")
        fcntl.flock(lock, fcntl.LOCK_EX)
    return directory, lock


def _sweep(root: str) -> None:
    """Remove this user's spool directories under ``root`` that no process holds."""
    assert fcntl is not None
    with os.scandir(root) as entries:
        candidates = [
            entry.path
            for entry in entries
            if entry.name.startswith(_PREFIX) and entry.is_dir(follow_symlinks=False)
        ]
    for directory in candidates:
        try:
            if os.stat(directory).st_uid != os.getuid():
                continue
            with open(os.path.join(directory, _LOCK_NAME)) as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                shutil.rmtree(directory, ignore_errors=True)
        except OSError:
            continue  # held by a live process, being removed, or not a spool's


_directories = _ProcessDirectories()
_ = atexit.register(_directories.remove)


def _remove_directory(directory: str, owner: int) -> None:
    """Remove a spool's directory, in the process that made it only."""
    if os.getpid() == owner:
        shutil.rmtree(directory, ignore_errors=True)


class _Segment:
    """Rows of one run of row numbers, in memory (``table``) or in a file (``path``)."""

    def __init__(self, start: int, table: pa.Table) -> None:
        self.start: int = start
        self.rows: int = table.num_rows
        self.live: int = table.num_rows
        self.table: pa.Table | None = table
        self.path: str | None = None

    def drop(self) -> None:
        """Let the rows go, and delete their file."""
        self.table = None
        if self.path is not None:
            os.unlink(self.path)
            self.path = None


class RowSpool:
    """Rows numbered from 0 in the order appended, fetched by number.

    Args:
        memory_bytes: How many bytes of rows stay in memory; beyond that the rows in
            memory are written to a file.
        root: The directory to keep files under; by default the system's temporary
            directory. Nothing is made there until rows are written.
    """

    def __init__(
        self, memory_bytes: int = SPOOL_MEMORY_BYTES, root: str | None = None
    ) -> None:
        self._memory_bytes: int = memory_bytes
        self._root: str | None = root
        self._closed: bool = False
        self._schema: pa.Schema | None = None
        self._segments: list[_Segment] = []
        self._starts: npt.NDArray[np.int64] | None = None
        self._in_memory: int = 0
        self._directory: str | None = None
        self._cleanup: weakref.finalize[..., Any] | None = None  # pyright: ignore[reportExplicitAny]  # any finalizer
        self.size: int = 0
        """How many rows were appended: the next row's number."""

    @property
    def memory_bytes(self) -> int:
        """How many bytes of rows are in memory now."""
        return self._in_memory

    def append(self, table: pa.Table) -> None:
        """Hold ``table``'s rows, numbered from ``size`` on.

        Args:
            table: The rows; every table appended has one schema.

        Raises:
            ValueError: If the spool is closed.
            OSError: If the rows in memory cannot be written to the local disk.
        """
        if self._closed:
            raise ValueError("the row spool is closed")
        if not table.num_rows:
            return
        if self._schema is None:
            self._schema = table.schema
        # One chunk that owns its rows: a slice would keep its whole parent alive.
        self._add(_Segment(self.size, _owned(table)))
        self.size += table.num_rows
        if self._in_memory > self._memory_bytes:
            self._spill()

    def _add(self, segment: _Segment) -> None:
        self._segments.append(segment)
        self._starts = None
        if segment.table is not None:
            self._in_memory += segment.table.nbytes

    def take(self, rows: npt.NDArray[np.int64]) -> pa.Table:
        """The rows with these numbers, in this order, as one chunk per column.

        Only the segments that hold a wanted row are read, each once.

        Args:
            rows: Row numbers, at least one, none of them discarded.
        """
        if self._starts is None:
            self._starts = np.fromiter(
                (s.start for s in self._segments), np.int64, len(self._segments)
            )
        held_in = np.searchsorted(self._starts, rows, side="right") - 1
        order = np.argsort(held_in, kind="stable")
        sorted_rows, sorted_in = rows[order], held_in[order]
        edges = np.flatnonzero(np.diff(sorted_in)) + 1
        parts: list[pa.Table] = []
        for group in np.split(np.arange(len(rows)), edges):
            segment = self._segments[int(sorted_in[group[0]])]
            local = pa.array(sorted_rows[group] - segment.start, pa.int64())
            parts.append(self._take_from(segment, local))
        if len(parts) == 1:
            # One segment: the stable sort changed nothing.
            return parts[0].combine_chunks()
        taken = pa.concat_tables(parts).combine_chunks()
        back = np.empty(len(order), np.int64)
        back[order] = np.arange(len(order))
        return taken.take(pa.array(back, pa.int64())).combine_chunks()

    def _take_from(self, segment: _Segment, local: pa.Array[Any]) -> pa.Table:  # pyright: ignore[reportExplicitAny]  # row numbers
        """Rows of one segment, by their numbers in it."""
        if segment.table is not None:
            return segment.table.take(local)
        assert segment.path is not None and self._schema is not None
        with pa.memory_map(segment.path) as mapped:
            stored = pa.ipc.open_file(mapped).read_all()
            return _wrapped(stored.take(local), self._schema)

    def discard(self, rows: npt.NDArray[np.int64]) -> None:
        """Say that these rows are never fetched again.

        A segment whose rows are all discarded is let go, and its file deleted.

        Args:
            rows: Row numbers, each discarded once only.
        """
        if not len(rows):
            return
        starts = np.fromiter(
            (s.start for s in self._segments), np.int64, len(self._segments)
        )
        held_in = np.searchsorted(starts, rows, side="right") - 1
        counts = np.bincount(held_in, minlength=len(self._segments))
        emptied = False
        for index in np.flatnonzero(counts):
            segment = self._segments[int(index)]
            segment.live -= int(counts[index])
            if not segment.live:
                if segment.table is not None:
                    self._in_memory -= segment.table.nbytes
                segment.drop()
                emptied = True
        if emptied:
            self._segments = [s for s in self._segments if s.live]
            self._starts = None

    def _spill(self) -> None:
        """Write the segments in memory to one file, as one segment."""
        waiting = [s for s in self._segments if s.table is not None]
        assert self._schema is not None
        # Neighbours only: a segment's rows have consecutive numbers.
        runs: list[list[_Segment]] = []
        for segment in waiting:
            if runs and runs[-1][-1].start + runs[-1][-1].rows == segment.start:
                runs[-1].append(segment)
            else:
                runs.append([segment])
        for run in runs:
            tables = [s.table for s in run if s.table is not None]
            table = pa.concat_tables(tables).combine_chunks()
            merged = _Segment(run[0].start, table)
            merged.live = sum(s.live for s in run)
            merged.path = self._write(merged)
            merged.table = None
            replaced = set(map(id, run))
            position = self._segments.index(run[0])
            self._segments = [s for s in self._segments if id(s) not in replaced]
            self._segments.insert(position, merged)
            self._in_memory -= sum(t.nbytes for t in tables)
        self._starts = None

    def _write(self, segment: _Segment) -> str:
        """Write a segment's rows to a file of their own; return its path."""
        assert segment.table is not None
        if self._directory is None:
            process_directory = _directories.get(self._root or tempfile.gettempdir())
            self._directory = tempfile.mkdtemp(dir=process_directory)
            self._cleanup = weakref.finalize(
                self, _remove_directory, self._directory, os.getpid()
            )
        path = os.path.join(self._directory, f"{segment.start}.arrow")
        table = _as_storage(segment.table)
        with (
            pa.OSFile(path, "wb") as sink,
            pa.ipc.new_file(sink, table.schema) as writer,
        ):
            writer.write_table(table)
        return path

    def close(self) -> None:
        """Let every row go and remove the files. Calling it again does nothing."""
        self._closed = True
        self._segments = []
        self._starts = None
        self._in_memory = 0
        if self._cleanup is not None:
            _ = self._cleanup()
        self._directory = self._cleanup = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _owned(table: pa.Table) -> pa.Table:
    """``table`` as one chunk over buffers of its own size, copied only if needed."""
    table = table.combine_chunks()
    if table.get_total_buffer_size() > 2 * table.nbytes:
        table = table.take(pa.array(np.arange(table.num_rows), pa.int64()))
    return table


def _as_storage(table: pa.Table) -> pa.Table:
    """``table`` with each extension column as its storage, no field metadata.

    Flags are dropped too: a field declared non-nullable may hold a null (a column not
    settled yet), and ``_wrapped`` puts the schema back.
    """
    columns: list[Any] = []  # pyright: ignore[reportExplicitAny]  # any column
    for column in table.combine_chunks().columns:
        array: Any = column.chunk(0)  # pyright: ignore[reportExplicitAny]  # any column
        is_extension = isinstance(column.type, pa.BaseExtensionType)
        columns.append(array.storage if is_extension else array)
    return pa.Table.from_arrays(columns, names=table.column_names)


def _wrapped(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Undo ``_as_storage``: the columns of ``table``, one chunk each, under ``schema``."""
    columns: list[Any] = []  # pyright: ignore[reportExplicitAny]  # any column
    for column, field in zip(table.combine_chunks().columns, schema):
        array = column.chunk(0)
        if isinstance(field.type, pa.BaseExtensionType):
            array = pa.ExtensionArray.from_storage(field.type, array)
        columns.append(array)
    return pa.Table.from_arrays(columns, schema=schema)
