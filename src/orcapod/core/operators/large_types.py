"""Every operator's output takes the large Arrow types (ITL-1122).

An operator's output is a table Orcapod creates, so each of its columns takes the
large-normalized form of its input type (``arrow_utils.normalize_to_large_types`` with
``keep_null=True``: ``string`` to ``large_string``, ``binary`` to ``large_binary``,
``list`` to ``large_list``, recursively). That holds also when the operator passes its
rows through unchanged (a select, a rename, a filter with nothing to filter, a join), so
an operator gives the same types on a plain stream as inside a job, whichever input a
column came from. Only layouts change: width, nullability, field metadata and logical
(extension) types are kept, and large and non-large variants hash identically.
"""

from __future__ import annotations

from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.utils import arrow_utils


def with_large_types(stream: StreamProtocol) -> StreamProtocol:
    """Return an operator's sync result with every column in its large variant.

    Args:
        stream: What the operator built (or the input it returned unaltered).

    Returns:
        ``stream`` itself when every column is already large; otherwise the same rows
        (tags, system tags, data, source info, data context) with the non-large
        columns re-laid out as their large variants.
    """
    table = stream.as_table(
        columns={"source": True, "system_tags": True, "context": True}
    )
    normalized = arrow_utils.normalize_table_to_large_types(table, keep_null=True)
    if normalized is table:
        return stream
    return ArrowTableStream(
        normalized,
        tag_columns=stream.keys()[0],
        data_context=stream.data_context_key,
    )
