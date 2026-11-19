"""The provisional operators: identities not frozen with data context v1 (ITL-1266).

Data context v1 freezes every hash at the 0.4.0 release. A provisional operator is outside
that freeze: its identity (and so the content and pipeline hashes, stored paths and record
ids of its results and of every step downstream of it) may still change in a later release.
Its golden values live in the golden store's provisional files, which are computed and
checked on every run but never frozen by a release.

``PROVISIONAL_OPERATORS`` is the one list. Each listed operator also sets the class
attribute ``PROVISIONAL = True`` (declared on ``StaticOutputOperatorPod``); a test checks
that the list and the markers agree. Constructing a provisional operator issues
``ProvisionalHashWarning`` once per operator per process and notes the use
(``utils/provisional.note_provisional_use``). An operator leaves the list at a release:
its golden values then move into a frozen golden file, and its identity is frozen from that
release on.
"""

from __future__ import annotations

import threading
import warnings
from collections.abc import Mapping
from types import MappingProxyType

from orcapod.errors import ProvisionalHashWarning
from orcapod.utils.provisional import note_provisional_use
from orcapod.utils.warning_utils import outside_orcapod_stacklevel

PROVISIONAL_OPERATORS: Mapping[str, str] = MappingProxyType(
    {
        # Whether the order of its predicates is part of its identity (ITL-1247), and
        # whether a general Filter over a Condition supersedes it (ITL-1265).
        "PolarsFilter": "ITL-1247",
    }
)
"""The provisional operators: class name to the issue that decides its identity."""

_lock = threading.RLock()
_issued: set[str] = set()


def provisional_operator_name(cls: type) -> str | None:
    """The provisional operator a class is, or derives from.

    Args:
        cls: An operator class.

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


def warn_provisional_operator(name: str) -> None:
    """Note the use of a provisional operator and warn, once per operator per process.

    The use is noted every time. The warning is recorded as issued only after
    ``warnings.warn`` returns, so under an error filter every construction raises. It is
    attributed to the first frame outside Orcapod, the code that built the operator.

    Args:
        name: The provisional operator's class name.
    """
    note_provisional_use(name)
    if name in _issued:
        return
    with _lock:
        if name in _issued:
            return
        warnings.warn(
            f"{name} is provisional: its identity is not frozen with data context v1 "
            "and may change in a later release. Then the stored paths and record ids "
            f"of its results, and of every step downstream of {name}, change, and "
            "those results are computed again instead of reused (nothing stored is "
            "lost or wrong). See 'Provisional operators' in docs/concepts/operators.md. "
            "To silence: warnings.filterwarnings('ignore', "
            "category=orcapod.errors.ProvisionalHashWarning).",
            ProvisionalHashWarning,
            stacklevel=outside_orcapod_stacklevel(),
        )
        _issued.add(name)


def reset_provisional_operator_warnings() -> None:
    """Forget which provisional operators have warned, so each warns again.

    A test hook: a test that checks the warning starts as a new process would.
    """
    with _lock:
        _issued.clear()
