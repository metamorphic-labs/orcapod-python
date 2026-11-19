"""Aware datetimes compared by instant (ITL-1218).

One place decides whether a ``datetime`` is aware and which instant it names, for the
conditions of a restricted source (``orcapod.types.Condition``) and the check of a
condition value against its column.
"""

from __future__ import annotations

from datetime import datetime

_MICROSECONDS_PER_DAY = 86_400_000_000


def is_aware(value: datetime) -> bool:
    """Return whether ``value`` names an instant: its ``utcoffset()`` is not ``None``.

    A ``tzinfo`` whose ``utcoffset`` returns ``None`` makes a naive value, so
    ``tzinfo is not None`` is not the test.
    """
    return value.utcoffset() is not None


def instant_microseconds(value: datetime) -> int | None:
    """Return the instant an aware ``value`` names, in microseconds since 0001-01-01 UTC.

    Computed from the wall time and the offset the value's own ``fold`` selects,
    without building a UTC ``datetime`` (which leaves the supported range near year 1
    and year 9999), so the two times of a daylight-saving overlap are two instants.
    ``None`` for a naive value.
    """
    offset = value.utcoffset()
    if offset is None:
        return None
    wall = value.replace(tzinfo=None) - datetime.min
    shift = offset.days * _MICROSECONDS_PER_DAY + offset.seconds * 1_000_000
    return (
        wall.days * _MICROSECONDS_PER_DAY
        + wall.seconds * 1_000_000
        + wall.microseconds
        - (shift + offset.microseconds)
    )
