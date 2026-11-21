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
(``utils/provisional.warn_provisional_use``). An operator leaves the list at a release:
its golden values then move into a frozen golden file, and its identity is frozen from that
release on.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from orcapod.utils.provisional import (
    provisional_class_name,
    reset_provisional_use_warnings,
    warn_provisional_use,
)

PROVISIONAL_OPERATORS: Mapping[str, str] = MappingProxyType(
    {
        # Whether the order of its predicates is part of its identity (ITL-1247), and
        # whether a general Filter over a Condition supersedes it (ITL-1265).
        "PolarsFilter": "ITL-1247",
    }
)
"""The provisional operators: class name to the issue that decides its identity."""


def provisional_operator_name(cls: type) -> str | None:
    """The provisional operator a class is, or derives from (``provisional_class_name``).

    Args:
        cls: An operator class.

    Returns:
        The provisional class's name, or ``None`` when ``cls`` is not provisional.
    """
    return provisional_class_name(cls)


def warn_provisional_operator(name: str) -> None:
    """Note the use of a provisional operator and warn, once per operator per process.

    Args:
        name: The provisional operator's class name.
    """
    warn_provisional_use(
        name,
        f"{name} is provisional: its identity is not frozen with data context v1 "
        "and may change in a later release. Then the stored paths and record ids "
        f"of its results, and of every step downstream of {name}, change, and "
        "those results are computed again instead of reused (nothing stored is "
        "lost or wrong). See 'Provisional operators' in docs/concepts/operators.md. "
        "To silence: warnings.filterwarnings('ignore', "
        "category=orcapod.errors.ProvisionalHashWarning).",
    )


def reset_provisional_operator_warnings() -> None:
    """Forget which provisional features have warned, so each warns again.

    A test hook: a test that checks the warning starts as a new process would.
    """
    reset_provisional_use_warnings()
