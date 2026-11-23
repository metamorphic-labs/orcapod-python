"""Which observers read the input rows a node passes them (ITL-1361).

A leaf module: the nodes and the observers both read it, and it imports neither.
"""

from __future__ import annotations


def reads_no_rows(observer: object) -> bool:
    """Whether ``observer`` provably never reads an input row's tag or data (ITL-1361).

    True only for a library observer whose own class declares ``READS_NO_ROWS = True``
    (``NoOpObserver``, ``RunCountingObserver``; never inherited by a subclass), and for
    a ``CompositeObserver`` all of whose observers read none. A function job node then
    hands the observer row views made only when read; any other observer receives the
    rows themselves, as before.

    Args:
        observer: The observer a node passes rows to.

    Returns:
        Whether it reads no input row.
    """
    declared = vars(type(observer)).get("READS_NO_ROWS")
    if isinstance(declared, property):
        return bool(declared.__get__(observer))
    return declared is True
