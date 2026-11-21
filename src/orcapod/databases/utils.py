"""Utilities shared across database implementations."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import TableCommittingDatabaseProtocol


def coerce_record_id(record_id: str | bytes) -> bytes:
    """Encode ``record_id`` to bytes if it is a ``str``.

    All database implementations store record IDs as ``bytes``
    (``pa.large_binary()``).  This helper lets callers pass plain strings
    without the caller needing to know the encoding — UTF-8 is used, which
    is a lossless round-trip for any ASCII or text-based ID.

    Args:
        record_id: A record identifier as either ``bytes`` or a ``str``.

    Returns:
        The record identifier as ``bytes``.
    """
    if isinstance(record_id, str):
        return record_id.encode()
    return record_id


def as_table_committing(database: object) -> TableCommittingDatabaseProtocol | None:
    """Return ``database`` if it commits table by table, else ``None`` (ITL-919).

    The one check for ``TableCommittingDatabaseProtocol``, used by ``PipelineJob.run``
    (the ``batch_size`` rule) and by function nodes (results committed before their
    pipeline records). It looks the attributes up on the object, so a wrapper that
    forwards them only when the database it wraps has them (``ExtensionAwareDatabase``)
    qualifies exactly when that database does; ``isinstance`` on the protocol would
    not, because it inspects the class.

    Args:
        database: Any database object.

    Returns:
        ``database``, typed as the protocol, or ``None``.
    """
    try:
        batch_size = getattr(database, "batch_size")  # noqa: B009  # AttributeError means "no"
        flush_batch = getattr(database, "flush_batch")  # noqa: B009
    except AttributeError:
        return None
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        return None
    if not callable(flush_batch):
        return None
    return cast("TableCommittingDatabaseProtocol", database)


_IDENTIFIER = r'(?:"[^"]+"|\w+)'
_DIRECT_PROJECTION = re.compile(
    rf"^\s*SELECT\s+(?:\*|{_IDENTIFIER}(?:\s*,\s*{_IDENTIFIER})*)\s+"
    rf"FROM\s+{_IDENTIFIER}\s*(?P<rest>.*?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
# After the table: nothing, or a clause that only chooses or orders rows.
_ROW_CLAUSE = re.compile(r"^(?:$|(?:WHERE|ORDER\s+BY|LIMIT|OFFSET)\b)", re.IGNORECASE)
_NOT_DIRECT = re.compile(
    r"\b(?:JOIN|UNION|INTERSECT|EXCEPT|GROUP\s+BY|HAVING|SELECT)\b", re.IGNORECASE
)


def is_direct_projection(query: str) -> bool:
    """Whether ``query`` gives rows of one table's columns unchanged (ITL-1334).

    That is ``SELECT *`` or a list of plain column names ``FROM`` one table, followed
    by nothing but a ``WHERE``, ``ORDER BY``, ``LIMIT`` or ``OFFSET``. Only then does a
    result column have the stored column's nullability: an outer join, an aggregate,
    an expression or a union can give nulls in a column stored ``NOT NULL``. The test
    is conservative: a query it does not recognize reads every column nullable.

    Args:
        query: An SQL query.

    Returns:
        Whether every result column is a stored column as it is stored.
    """
    match = _DIRECT_PROJECTION.match(query)
    if match is None:
        return False
    rest = match.group("rest")
    return _ROW_CLAUSE.match(rest) is not None and _NOT_DIRECT.search(rest) is None
