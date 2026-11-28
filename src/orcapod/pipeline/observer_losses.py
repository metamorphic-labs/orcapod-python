"""Observer events a run could not record (ITL-1556).

Each job execution gives its node-scoped built-in observers a shared counter.
Those observers and their data loggers carry it onto worker threads.
An observer used directly by an orchestrator or node only logs failed writes.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import logging

STATUS_EVENTS = "status"
"""The kind of the events a ``StatusObserver`` records."""

LOGGING_EVENTS = "logging"
"""The kind of the events a ``LoggingObserver`` records."""


def ascii_loss_text(text: str) -> str:
    """Escape observer-loss text for strict output encodings, including surrogates."""
    return str.encode(text, "ascii", "backslashreplace").decode("ascii")


@dataclass(frozen=True)
class LostObserverEvents:
    """The events of one observer kind that a run could not record.

    Attributes:
        kind: The observer kind: ``"status"`` (a ``StatusObserver``'s state
            transitions) or ``"logging"`` (a ``LoggingObserver``'s log rows).
        count: How many events of that kind were not recorded.
        first_error: The first reported error as ``Type: message``, or its type
            name if its message could not be rendered. Non-ASCII text is escaped.
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

    def count(self, kind: str) -> bool:
        """Count one lost event of ``kind`` and claim the first error if needed.

        Args:
            kind: The observer kind.

        Returns:
            Whether this event is the first loss of its kind.
        """
        with self._lock:
            count = self._counts.get(kind, 0) + 1
            self._counts[kind] = count
            return count == 1

    def keep_first_error(self, kind: str, error: Exception) -> None:
        """Format and keep the error from the event that first claimed ``kind``.

        Formatting can invoke user code, so do it outside the counter's lock.

        Args:
            kind: The observer kind.
            error: What the first failed write raised.
        """
        try:
            name = str.__str__(type(error).__name__)
        except Exception:
            name = "Exception"
        try:
            # An exception's __str__ can return a subclass with a custom __format__.
            message = str.__str__(str(error))
            text = f"{name}: {message}"
        except Exception:
            text = name
        safe_text = ascii_loss_text(text)
        with self._lock:
            self._first_errors[kind] = safe_text

    def by_kind(self) -> dict[str, LostObserverEvents]:
        """The lost events so far, by observer kind, in kind order.

        Returns:
            One entry per kind that lost an event; empty when none was lost. A
            report still formatting its first error shows ``Exception`` until ready.
        """
        with self._lock:
            return {
                kind: LostObserverEvents(
                    kind, count, self._first_errors.get(kind, "Exception")
                )
                for kind, count in sorted(self._counts.items())
            }


def report_lost_event(
    losses: RunLosses | None,
    kind: str,
    error: Exception,
    logger: logging.Logger,
    message: str,
    *args: str,
) -> None:
    """Count a failed observer write and log the exception from its handler."""
    first_loss = losses is not None and losses.count(kind)
    try:
        logger.exception(message, *args, stacklevel=2)
    except Exception:
        # An application log handler can raise. The observer write remains nonfatal.
        pass
    # Give the existing traceback the first opportunity to render the exception.
    if first_loss and losses is not None:
        losses.keep_first_error(kind, error)
