"""Track the database objects that accept a job's node writes."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from orcapod.databases._pending_commits import PendingCommits


class WriteDatabase(Protocol):
    """The part of the database interface used to track a node's writes."""

    @property
    def stores_records(self) -> bool:
        """Whether the database stores rows when asked to insert them."""
        ...


WriteKey = tuple[int, tuple[str, ...]]
DatabaseWrites = dict[WriteKey, WriteDatabase]

_run_writes: ContextVar[DatabaseWrites | None] = ContextVar("_run_writes", default=None)
_current_write: ContextVar[tuple[object, WriteKey, WriteDatabase] | None] = ContextVar(
    "_current_write", default=None
)


def database_queue(database: WriteDatabase) -> object:
    """Identify the pending queue that a whole flush covers, shared by scoped views."""
    queue = getattr(database, "_pending_commits", None)
    return database if queue is None else queue


@contextmanager
def collect_database_writes(
    writes: DatabaseWrites,
) -> Iterator[None]:
    """Collect node writes for one job execution, including its async tasks."""
    run_token = _run_writes.set(writes)
    write_token = _current_write.set(None)
    try:
        yield
    finally:
        _current_write.reset(write_token)
        _run_writes.reset(run_token)


@contextmanager
def track_database_write(
    database: WriteDatabase, path: tuple[str, ...], *, has_rows: bool = True
) -> Iterator[None]:
    """Track an insert only if it queued rows, before any automatic flush."""
    writes = _run_writes.get()
    key = (id(database), path)
    if writes is None or key in writes or not has_rows or not database.stores_records:
        yield
        return

    queue = database_queue(database)
    if queue is database:
        # The protocol's add methods give no receipt. For a custom database
        # without a queue, treat a successfully completed add as a write.
        yield
        writes[key] = database
        return

    token = _current_write.set((queue, key, database))
    try:
        yield
    finally:
        _current_write.reset(token)


def note_queued_write(queue: PendingCommits) -> None:
    """Record a node write after deduplication and before automatic publication."""
    active = _current_write.get()
    if active is not None and active[0] is queue:
        writes = _run_writes.get()
        if writes is not None:
            _, key, database = active
            writes[key] = database


def finish_database_table_write(database: WriteDatabase, path: tuple[str, ...]) -> None:
    """Retire a table handled by the node's own commit, including dry-run results."""
    writes = _run_writes.get()
    if writes is not None:
        _ = writes.pop((id(database), path), None)
