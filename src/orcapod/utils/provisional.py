"""Uses of provisional features: the record of which ones a block of code used (ITL-1266).

A provisional feature is one whose identity is not frozen with the current data context: a
provisional operator (``core/operators/provisional.py``). Its hashes may change in a later
release, so the golden values that depend on it are kept outside the frozen golden files.
Each provisional feature calls ``note_provisional_use`` with its name whenever it is used,
before its once-per-process warning; ``recording_provisional_uses`` collects those names
for a block of code. The golden hash tool runs each golden case inside one, so a case that
uses a provisional feature it does not declare is refused. Recording changes no warning:
the warnings keep their own once-per-process bookkeeping.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager

_lock = threading.RLock()
_recorders: list[set[str]] = []


def note_provisional_use(name: str) -> None:
    """Record that the provisional feature ``name`` was used.

    Every block recording at the time (``recording_provisional_uses``) receives the name,
    whichever thread used the feature.

    Args:
        name: The feature's name, e.g. ``PolarsFilter``.
    """
    with _lock:
        for recorder in _recorders:
            recorder.add(name)


@contextmanager
def recording_provisional_uses() -> Iterator[set[str]]:
    """Collect the names of the provisional features used inside the block.

    Blocks may nest; each receives every name noted while it is open.

    Yields:
        The set of names noted so far; it keeps growing until the block exits.
    """
    recorder: set[str] = set()
    with _lock:
        _recorders.append(recorder)
    try:
        yield recorder
    finally:
        with _lock:
            # By identity: two recorders holding the same names are equal sets.
            index = next(i for i, r in enumerate(_recorders) if r is recorder)
            del _recorders[index]
