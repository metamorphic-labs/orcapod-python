"""Observer events a run could not record (ITL-1556).

The status and logging observers record run events as rows in a database. A write that
fails does not stop the run: its results do not depend on those rows. The loss is
reported instead. A run opens a record under its run id (``collecting_lost_events``), an
observer that fails to write an event adds to the record of its run
(``record_lost_event``), and the run puts the record in its ``RunSummary`` and warns once
at its end.

The record is found by run id, which every observer of a run is given
(``on_run_start``), so it holds for each contextualized copy of an observer and for an
observer inside a ``CompositeObserver`` alike. An event lost outside an open record (an
observer handed to an orchestrator or a node directly, with no job run to report it) is
only logged by its observer.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

STATUS_EVENTS = "status"
"""The kind of the events a ``StatusObserver`` records."""

LOGGING_EVENTS = "logging"
"""The kind of the events a ``LoggingObserver`` records."""


@dataclass(frozen=True)
class LostObserverEvents:
    """The events of one observer kind that a run could not record.

    Attributes:
        kind: The observer kind: ``"status"`` (a ``StatusObserver``'s state
            transitions) or ``"logging"`` (a ``LoggingObserver``'s log rows).
        count: How many events of that kind were not recorded.
        first_error: The first failed write's error, as ``Type: message``.
    """

    kind: str
    count: int
    first_error: str


class RunLosses:
    """The lost observer events of one run, counted per observer kind.

    Observer hooks may be called from several threads, so the counts are updated under
    a lock.
    """

    def __init__(self) -> None:
        """Start with no lost event."""
        self._lock: threading.Lock = threading.Lock()
        self._counts: dict[str, int] = {}
        self._first_errors: dict[str, str] = {}

    def record(self, kind: str, error: BaseException) -> None:
        """Count one lost event of ``kind``; keep the kind's first error.

        Args:
            kind: The observer kind.
            error: What the failed write raised.
        """
        text = f"{type(error).__name__}: {error}"
        with self._lock:
            self._counts[kind] = self._counts.get(kind, 0) + 1
            _ = self._first_errors.setdefault(kind, text)

    def by_kind(self) -> dict[str, LostObserverEvents]:
        """The lost events so far, by observer kind, in kind order.

        Returns:
            One entry per kind that lost an event; empty when none was lost.
        """
        with self._lock:
            return {
                kind: LostObserverEvents(kind, count, self._first_errors[kind])
                for kind, count in sorted(self._counts.items())
            }


_open_lock = threading.Lock()
_open_records: dict[str, RunLosses] = {}


@contextmanager
def collecting_lost_events(run_id: str) -> Iterator[RunLosses]:
    """Keep a record of the observer events lost under ``run_id`` while the block runs.

    Args:
        run_id: The run's id, as the run gives it to its observers.

    Yields:
        The run's record; read it (``by_kind``) once the run's nodes have finished.
    """
    losses = RunLosses()
    with _open_lock:
        _open_records[run_id] = losses
    try:
        yield losses
    finally:
        with _open_lock:
            if _open_records.get(run_id) is losses:
                del _open_records[run_id]


def record_lost_event(run_id: str, kind: str, error: BaseException) -> None:
    """Add one lost event to the record of run ``run_id``, if a run keeps one.

    Args:
        run_id: The run the observer was started for.
        kind: The observer kind (``STATUS_EVENTS`` or ``LOGGING_EVENTS``).
        error: What the failed write raised.
    """
    with _open_lock:
        losses = _open_records.get(run_id)
    if losses is not None:
        losses.record(kind, error)
