"""Dependencies between buffered rows, kept outside their stored Arrow schemas."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Set
from contextlib import contextmanager
from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True)
class QueuedRows:
    """One table's queued rows and their ids, replaced as a whole (ITL-1381).

    The queue never changes the rows and the ids in two steps: an interruption
    (``KeyboardInterrupt``) between two statements leaves both as they were or
    both as they become.
    """

    table: pa.Table
    ids: frozenset[bytes]


class QueuedTables(Mapping[str, pa.Table]):
    """Read-only view of each key's queued rows."""

    def __init__(self, queue: dict[str, QueuedRows]) -> None:
        self._queue = queue

    def __getitem__(self, key: str) -> pa.Table:
        return self._queue[key].table

    def __iter__(self) -> Iterator[str]:
        return iter(self._queue)

    def __len__(self) -> int:
        return len(self._queue)


class QueuedIds(Mapping[str, frozenset[bytes]]):
    """Read-only view of each key's queued ids."""

    def __init__(self, queue: dict[str, QueuedRows]) -> None:
        self._queue = queue

    def __getitem__(self, key: str) -> frozenset[bytes]:
        return self._queue[key].ids

    def __iter__(self) -> Iterator[str]:
        return iter(self._queue)

    def __len__(self) -> int:
        return len(self._queue)


class PendingCommit:
    """The outcome of an attempt to commit pending results."""

    def __init__(self) -> None:
        self.committed = False
        self._dependents: dict[tuple[PendingCommits, str], set[bytes]] = {}

    def add_dependent(self, queue: PendingCommits, key: str, record_id: bytes) -> None:
        """Withdraw this dependent record if the attempt fails."""
        self._dependents.setdefault((queue, key), set()).add(record_id)

    def finish(
        self, *, committed: bool, discarded: dict[str, set[bytes]] | None = None
    ) -> None:
        """Release the guarded rows on success or withdraw them on failure."""
        self.committed = committed
        if not committed:
            for (queue, key), ids in self._dependents.items():
                queue.discard(key, ids, self, discarded=discarded)
        self._dependents.clear()


class PendingCommits:
    """Track table commit attempts and dependencies for a database's shared queue."""

    def __init__(
        self,
        record_id_column: str,
        *,
        on_empty: Callable[[str], None] | None = None,
    ) -> None:
        self._record_id_column = record_id_column
        # The queue itself: every change replaces a key's entry by one assignment.
        self._queue: dict[str, QueuedRows] = {}
        self._batches = QueuedTables(self._queue)
        self._ids = QueuedIds(self._queue)
        self._on_empty = on_empty
        self._attempts: dict[str, dict[bytes | None, PendingCommit]] = {}
        self._guards: dict[str, dict[bytes, PendingCommit]] = {}
        self._schema_checks: dict[str, Callable[[pa.Schema], None]] = {}

    @property
    def batches(self) -> Mapping[str, pa.Table]:
        """Each key's queued rows (read-only)."""
        return self._batches

    @property
    def ids(self) -> Mapping[str, frozenset[bytes]]:
        """Each key's queued ids (read-only)."""
        return self._ids

    def queue(self, key: str, table: pa.Table, ids: Set[bytes]) -> None:
        """Make ``table`` (with exactly ``ids``) the key's queued rows, in one step."""
        self._queue[key] = QueuedRows(table, frozenset(ids))

    def clear(self) -> None:
        """Drop every queued row."""
        self._queue.clear()

    def _replace(self, key: str, table: pa.Table, ids: frozenset[bytes]) -> None:
        """Keep ``ids`` of the key's rows (``table``), or drop the key when none."""
        if ids:
            self._queue[key] = QueuedRows(table, ids)
        else:
            self._queue.pop(key, None)

    @contextmanager
    def guard_schema(
        self, key: str, check: Callable[[pa.Schema], None]
    ) -> Iterator[None]:
        """Keep a schema check with the batch through buffering and retries."""
        self._schema_checks.setdefault(key, check)
        try:
            yield
        finally:
            if key not in self._batches:
                self._schema_checks.pop(key, None)

    def schema_check(self, key: str) -> Callable[[pa.Schema], None] | None:
        """Get the check for a batch being committed."""
        return self._schema_checks.get(key)

    def attempt(self, key: str, record_id: bytes | None = None) -> PendingCommit:
        """Get a commit dependency for a row or for a table committed as a unit."""
        attempts = self._attempts.setdefault(key, {})
        attempt = attempts.get(record_id)
        if attempt is None:
            attempt = attempts[record_id] = PendingCommit()
        return attempt

    def finish(self, key: str, *, committed: bool) -> None:
        """Resolve all dependencies on this table's current commit attempt."""
        for attempt in self._attempts.pop(key, {}).values():
            attempt.finish(committed=committed)

    def finish_rows(
        self,
        key: str,
        attempted: set[bytes],
        *,
        committed: bool,
        discarded: dict[str, set[bytes]] | None = None,
    ) -> None:
        """Resolve dependents of attempted rows, leaving other guards in place."""
        attempts = self._attempts.get(key)
        if attempts is None:
            return
        resolved = [
            attempts.pop(record_id) for record_id in attempts.keys() & attempted
        ]
        if not attempts:
            self._attempts.pop(key)
        for attempt in resolved:
            attempt.finish(committed=committed, discarded=discarded)

    @contextmanager
    def guard(
        self, key: str, record_id: bytes, attempt: PendingCommit
    ) -> Iterator[None]:
        """Guard a record before enqueueing it, which can trigger auto-flush."""
        if record_id in self._ids.get(key, ()):
            yield  # The queued duplicate already has its original dependency.
            return
        self._guards.setdefault(key, {})[record_id] = attempt
        attempt.add_dependent(self, key, record_id)
        try:
            yield
        finally:
            if record_id not in self._ids.get(key, ()):
                self.forget(key, {record_id})

    def discard(
        self,
        key: str,
        ids: set[bytes],
        attempt: PendingCommit,
        *,
        discarded: dict[str, set[bytes]] | None = None,
    ) -> None:
        """Withdraw the records still guarded by the failed attempt."""
        guards = self._guards.get(key, {})
        ids = {record_id for record_id in ids if guards.get(record_id) is attempt}
        self.forget(key, ids)
        held = self._queue.get(key)
        if held is None:
            return
        removed = ids & held.ids
        if not removed:
            return
        remaining = held.ids - removed
        if remaining:
            self._replace(key, self._select(held.table, removed, keep=False), remaining)
        else:
            self._queue.pop(key)
            self._schema_checks.pop(key, None)
            if self._on_empty is not None:
                self._on_empty(key)
        if discarded is not None:
            discarded.setdefault(key, set()).update(removed)
        self.finish_rows(key, removed, committed=False, discarded=discarded)

    def peek_ready(
        self, key: str, among: Set[bytes] | None = None
    ) -> tuple[pa.Table, set[bytes]] | None:
        """The records ready to commit, left in the queue (blocked ones excluded).

        Args:
            key: The table's key.
            among: Only these ids (the rows of one write policy). ``None``: every
                ready row.

        Returns:
            The ready rows and their ids, or ``None`` when none is ready. Nothing
            leaves the queue: ``remove`` takes them out once their write was tried.
        """
        held = self._queue.get(key)
        if held is None or not held.table.num_rows:
            return None
        blocked = {
            record_id
            for record_id, attempt in self._guards.get(key, {}).items()
            if not attempt.committed
        }
        ready = set(held.ids if among is None else held.ids & among) - blocked
        if not ready:
            return None
        if ready == held.ids:
            return held.table, ready
        return self._select(held.table, ready, keep=True), ready

    def remove(self, key: str, ids: Set[bytes]) -> None:
        """Take rows out of the queue, in one step."""
        held = self._queue.get(key)
        if held is None or not ids & held.ids:
            return
        remaining = held.ids - ids
        table = (
            self._select(held.table, set(ids), keep=False) if remaining else held.table
        )
        self._replace(key, table, remaining)

    def take_ready(
        self, key: str, among: Set[bytes] | None = None
    ) -> tuple[pa.Table, set[bytes]] | None:
        """Take the records ready to commit, leaving blocked records readable.

        Args:
            key: The table's key.
            among: Take only these ids; the other queued rows stay queued.
                ``None`` takes every ready row.

        The taken and the remaining rows are built before the queue changes, and
        the queue changes in one step (``remove``).
        """
        ready = self.peek_ready(key, among)
        if ready is not None:
            self.remove(key, ready[1])
        return ready

    def restore(self, key: str, batch: pa.Table, ids: Set[bytes]) -> None:
        """Restore a retryable write ahead of rows that were held back."""
        held = self._queue.get(key)
        if held is None:
            self.queue(key, batch, ids)
        else:
            self.queue(key, pa.concat_tables([batch, held.table]), held.ids | ids)

    def forget(self, key: str, ids: set[bytes]) -> None:
        """Forget dependencies for records that are leaving the queue."""
        if key not in self._batches:
            self._schema_checks.pop(key, None)
        guards = self._guards.get(key)
        if guards is not None:
            for record_id in ids:
                guards.pop(record_id, None)
            if not guards:
                self._guards.pop(key)

    def _select(self, batch: pa.Table, ids: Set[bytes], *, keep: bool) -> pa.Table:
        """Keep or remove the given rows, preserving their order and schema."""
        mask = [
            (record_id in ids) == keep
            for record_id in batch[self._record_id_column].to_pylist()
        ]
        return batch.filter(pa.array(mask, type=pa.bool_()))
