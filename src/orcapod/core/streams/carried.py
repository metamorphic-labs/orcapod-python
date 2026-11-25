"""Reading a stream's table with its derived token columns as they are (ITL-1464).

``carried_table`` is how the library's own operators read their inputs: the table of
``as_table``, where a source-info column may be a recipe in place of its text
(``utils/source_tokens.py``). It is no part of a stream's interface. Only a stream whose
class is a library class that declares ``_carried_table`` itself, read as that class
(``reads_as_its_class``: no subclass, nothing replaced on the object), is read this way;
every other stream, a subclass with its own ``as_table`` or a user's stream, is read
through ``as_table`` and gives text.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pyarrow as pa

from orcapod.core.exact_class import reads_as_its_class

CARRIED_TABLE = "_carried_table"
"""The method a library class declares when its table may hold derived columns."""


def carried_table(
    stream: Any,  # pyright: ignore[reportExplicitAny]  # a stream (the protocol would make an import cycle)
    columns: Any,  # pyright: ignore[reportExplicitAny]  # as as_table
) -> pa.Table:
    """``stream.as_table(columns=columns)``, derived token columns as they are.

    Args:
        stream: The stream.
        columns: The column groups, as for ``as_table``.

    Returns:
        The table, for library code that passes it on; never for a user, a user's
        operator or function, a store or a hash.
    """
    kind = type(stream)
    read: Any = vars(kind).get(CARRIED_TABLE)  # pyright: ignore[reportExplicitAny]  # a method
    if read is None or not reads_as_its_class(stream, kind):
        return stream.as_table(columns=columns)
    return read(stream, columns)


CARRIED_BATCHES = "_carried_batches"
"""The method of the library's channel class whose batches may hold derived columns."""


def carried_batches(channel: Any) -> AsyncIterator[pa.RecordBatch]:  # pyright: ignore[reportExplicitAny]  # a channel (its protocol would make an import cycle)
    """The remaining record batches of a channel, derived token columns as they are.

    For library code that passes them on, as ``carried_table`` is for tables, and by the
    same rule: only the library's own channel class, read as that class, is read this
    way; any other channel is iterated and gives what it gives.

    Args:
        channel: An input channel (``RecordBatchInput``).
    """
    kind = type(channel)
    read: Any = vars(kind).get(CARRIED_BATCHES)  # pyright: ignore[reportExplicitAny]  # a method
    if read is None or not reads_as_its_class(channel, kind):
        return aiter(channel)
    return read(channel)
