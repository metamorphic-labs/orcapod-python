"""Uses of provisional features: the record of which ones a block of code used (ITL-1266).

A provisional feature is one whose identity is not frozen with the current data context: a
provisional operator (``core/operators/provisional.py``) or a provisional source
(``core/sources/provisional.py``). Its hashes may change in a later
release, so the golden values that depend on it are kept outside the frozen golden files.
Each provisional feature calls ``note_provisional_use`` with its name whenever it is used,
before its once-per-process warning; ``recording_provisional_uses`` collects those names
for a block of code. The golden hash tool runs each golden case inside one, so a case that
uses a provisional feature it does not declare is refused. Recording changes no warning:
the warnings keep their own once-per-process bookkeeping.
"""

from __future__ import annotations

import threading
import warnings
from collections.abc import Iterator
from contextlib import contextmanager

from orcapod.errors import ProvisionalHashWarning
from orcapod.utils.warning_utils import outside_orcapod_stacklevel

_lock = threading.RLock()
_recorders: list[set[str]] = []
_issued: set[str] = set()


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


def provisional_class_name(cls: type) -> str | None:
    """The provisional class ``cls`` is, or derives from.

    Args:
        cls: A class that may set ``PROVISIONAL = True`` (an operator or a source).

    Returns:
        The name of the nearest class in ``cls``'s method resolution order that sets
        ``PROVISIONAL = True`` itself, or ``None`` when ``cls`` is not provisional.
    """
    if not getattr(cls, "PROVISIONAL", False):
        return None
    for klass in cls.__mro__:
        if klass.__dict__.get("PROVISIONAL") is True:
            return klass.__name__
    return None


def warn_provisional_use(name: str, message: str) -> None:
    """Note the use of a provisional feature and warn, once per feature per process.

    The use is noted every time (``note_provisional_use``). ``ProvisionalHashWarning``
    is issued the first time ``name`` is used in the process, and recorded as issued only
    after ``warnings.warn`` returns, so under an error filter every use raises. It is
    attributed to the first frame outside Orcapod, the code that used the feature.

    Args:
        name: The feature's name, e.g. ``PolarsFilter`` or ``PollingSource``.
        message: The warning text.
    """
    note_provisional_use(name)
    if name in _issued:
        return
    with _lock:
        if name in _issued:
            return
        warnings.warn(
            message, ProvisionalHashWarning, stacklevel=outside_orcapod_stacklevel()
        )
        _issued.add(name)


def reset_provisional_use_warnings() -> None:
    """Forget which provisional features have warned, so each warns again.

    A test hook: a test that checks a warning starts as a new process would.
    """
    with _lock:
        _issued.clear()
