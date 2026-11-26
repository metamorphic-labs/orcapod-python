"""Dependencies between buffered rows, kept outside their stored Arrow schemas."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager

import pyarrow as pa


class RejectedWriteRow:
    """Attach the rejected row's input position to an atomic connector error."""

    row_index: int | None = None


class PendingCommit:
    """The outcome of the current attempt to commit a table's pending rows."""

    def __init__(self) -> None:
        self.committed = False
        self._dependents: dict[tuple[PendingCommits, str], set[bytes]] = {}

    def add_dependent(self, queue: PendingCommits, key: str, record_id: bytes) -> None:
        """Withdraw this dependent record if the attempt fails."""
        self._dependents.setdefault((queue, key), set()).add(record_id)

    def finish(self, *, committed: bool) -> None:
        """Release the guarded rows on success or withdraw them on failure."""
        self.committed = committed
        if not committed:
            for (queue, key), ids in self._dependents.items():
                queue.discard(key, ids, self)
        self._dependents.clear()


class PendingCommits:
    """Track table commit attempts and dependencies for a database's shared queue."""

    def __init__(
        self,
        record_id_column: str,
        batches: dict[str, pa.Table],
        ids: dict[str, set[bytes]],
        *,
        on_empty: Callable[[str], None] | None = None,
    ) -> None:
        self._record_id_column = record_id_column
        self._batches = batches
        self._ids = ids
        self._on_empty = on_empty
        self._attempts: dict[str, PendingCommit] = {}
        self._guards: dict[str, dict[bytes, PendingCommit]] = {}
        self._schema_checks: dict[str, Callable[[pa.Schema], None]] = {}

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

    def attempt(self, key: str) -> PendingCommit:
        """Get the attempt shared by all rows currently pending for a table."""
        attempt = self._attempts.get(key)
        if attempt is None:
            attempt = self._attempts[key] = PendingCommit()
        return attempt

    def finish(self, key: str, *, committed: bool) -> None:
        """Resolve an attempt before another flush can consider its dependents."""
        attempt = self._attempts.pop(key, None)
        if attempt is not None:
            attempt.finish(committed=committed)

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

    def discard(self, key: str, ids: set[bytes], attempt: PendingCommit) -> None:
        """Withdraw the records still guarded by the failed attempt."""
        guards = self._guards.get(key, {})
        ids = {record_id for record_id in ids if guards.get(record_id) is attempt}
        self.forget(key, ids)
        held_ids = self._ids.get(key)
        if held_ids is None or not held_ids.intersection(ids):
            return
        held_ids.difference_update(ids)
        if held_ids:
            self._batches[key] = self._select(self._batches[key], ids, keep=False)
        else:
            self._batches.pop(key)
            self._ids.pop(key)
            self._schema_checks.pop(key, None)
            if self._on_empty is not None:
                self._on_empty(key)

    def take_ready(self, key: str) -> tuple[pa.Table, set[bytes]] | None:
        """Take the records ready to commit, leaving blocked records readable."""
        batch = self._batches.get(key)
        if batch is None or not batch.num_rows:
            return None
        blocked = {
            record_id
            for record_id, attempt in self._guards.get(key, {}).items()
            if not attempt.committed
        }
        if not blocked:
            return self._batches.pop(key), self._ids.pop(key, set())
        ids = self._ids[key] - blocked
        if not ids:
            return None
        self._batches[key] = self._select(batch, blocked, keep=True)
        self._ids[key] = blocked
        return self._select(batch, blocked, keep=False), ids

    def restore(self, key: str, batch: pa.Table, ids: set[bytes]) -> None:
        """Restore a retryable write ahead of rows that were held back."""
        held = self._batches.get(key)
        self._batches[key] = batch if held is None else pa.concat_tables([batch, held])
        self._ids.setdefault(key, set()).update(ids)

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

    def _select(self, batch: pa.Table, ids: set[bytes], *, keep: bool) -> pa.Table:
        """Keep or remove the given rows, preserving their order and schema."""
        mask = [
            (record_id in ids) == keep
            for record_id in batch[self._record_id_column].to_pylist()
        ]
        return batch.filter(pa.array(mask, type=pa.bool_()))
