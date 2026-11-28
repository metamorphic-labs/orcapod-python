"""Observer events a run could not record (ITL-1556).

Each job execution gives its built-in observers a shared counter. Their contextualized
copies and data loggers carry the counter with them, including onto worker threads.
An observer used directly by an orchestrator or node only logs failed writes.
"""

from __future__ import annotations

import threading
from copy import copy
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import logging
    from typing import Self

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
        try:
            message = str(error)
        except Exception:
            message = "<message unavailable>"
        text = f"{type(error).__name__}: {message}"
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


class RunLossObserver:
    """A built-in observer whose copy can collect losses for one job execution."""

    _run_losses: RunLosses | None = None

    def with_run_losses(self, losses: RunLosses) -> Self:
        """Copy the observer with a counter for this job's execution."""
        observer = copy(self)
        observer._run_losses = losses
        return observer


def report_lost_event(
    losses: RunLosses | None,
    kind: str,
    error: Exception,
    logger: logging.Logger,
    message: str,
    *args: str,
) -> None:
    """Count a failed observer write and log the exception from its handler."""
    if losses is not None:
        losses.record(kind, error)
    try:
        logger.exception(message, *args, stacklevel=2)
    except Exception:
        # An application log handler can raise. The observer write remains nonfatal.
        pass
