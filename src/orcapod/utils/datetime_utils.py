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

    Computed from the calendar fields of the wall time (day ordinal, hour, minute,
    second, microsecond) and the offset the value's own ``fold`` selects. No
    ``datetime`` arithmetic is done, so neither the supported range near year 1 and
    year 9999 nor a ``datetime`` subclass of narrower range (Arrow hands back a
    nanosecond timestamp as a ``pandas.Timestamp``, which cannot reach year 1) can
    make it fail, and the two times of a daylight-saving overlap are two instants.
    ``None`` for a naive value.
    """
    offset = value.utcoffset()
    if offset is None:
        return None
    wall_seconds = (
        (value.toordinal() - 1) * 86_400
        + value.hour * 3_600
        + value.minute * 60
        + value.second
    )
    shift = offset.days * _MICROSECONDS_PER_DAY + offset.seconds * 1_000_000
    return wall_seconds * 1_000_000 + value.microsecond - (shift + offset.microseconds)
