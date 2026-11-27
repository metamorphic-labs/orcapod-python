"""The data context of an entry an operator gathers from several rows (ITL-1359).

Developer decision, November 24, 2026: a datagram has one data context. An operator
that gathers several rows into one (``GroupBy``, ``Batch``, the merged lists of
``MergeJoin``) accepts them only if all of them have the same data context, and the
gathered entry takes that context. Members of different data contexts are refused;
nothing is converted from one context to another.

This module is the one place of that rule: the data context of each row
(``read_members``), and the context of each gathered entry, or the refusal
(``gathered_context_keys``).
"""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import pyarrow as pa

from orcapod import contexts
from orcapod.core.operators.static_output_pod import produced_stream
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import SourceProtocol, StreamProtocol
from orcapod.system_constants import constants


def read_members(
    stream: StreamProtocol,
) -> tuple[pa.Table, pa.Array[Any], contexts.DataContext]:
    """Read an input's rows, with the data context each is of.

    A row's data context is the one its table names. An ``ArrowTableStream`` not given
    the column fills it with the default context's key, whatever its own data context,
    so a column that names only the default context states nothing: those rows are of
    the context their datagrams report (``_unstated_context``). An operator's lazy stream reports its pod's data context, not its rows'
    (ITL-1339), so the stream that holds the rows is read and asked (once: asking again
    could run the operator again).

    Args:
        stream: An operator's input.

    Returns:
        The rows (tags, system tags, data and source info), the data context key of
        each, never null, and the data context the rows' datagrams report.
    """
    held = produced_stream(stream)
    table = held.as_table(
        columns={"source": True, "system_tags": True, "context": True}
    )
    default = contexts.get_default_context_key()
    # Every stream has a data context; StreamProtocol does not declare it.
    context: contexts.DataContext = held.data_context  # pyright: ignore[reportAttributeAccessIssue]
    if context.context_key != default:
        context = _unstated_context(held, context)
    own = context.context_key
    unstated = pa.array([own] * table.num_rows, pa.large_string())
    if constants.CONTEXT_KEY not in table.column_names:
        return table, unstated, context
    keys = cast(
        "pa.Array[Any]",
        table.column(constants.CONTEXT_KEY).cast(pa.large_string()).combine_chunks(),
    )
    table = table.drop_columns([constants.CONTEXT_KEY])
    if set(keys.unique().to_pylist()) <= {None, default}:
        return table, unstated, context
    return table, keys.fill_null(own), context


def _unstated_context(
    held: StreamProtocol, reported: contexts.DataContext
) -> contexts.DataContext:
    """Return the data context of rows whose table does not state one.

    Args:
        held: The stream that holds the rows.
        reported: The data context it reports.

    Returns:
        What its datagrams report: the stream's own context for an
        ``ArrowTableStream``; for a source, which reports its own context whatever its
        rows', what its first row says; the default one for any other kind, whose
        table is made from its rows and names their context.
    """
    if isinstance(held, ArrowTableStream):
        return reported
    if isinstance(held, SourceProtocol):
        for _, data in held.iter_data():
            if data.data_context_key == reported.context_key:
                return reported
            return contexts.resolve_context(data.data_context_key)
    return contexts.get_default_context()


def gathered_context_keys(
    operator: str,
    keys: pa.Array[Any],
    offsets: pa.Int64Array,
    tags: pa.Table,
    tag_rows: pa.Array[Any],
) -> pa.Array[Any]:
    """Return the data context of each gathered entry, or refuse a mixed entry.

    Args:
        operator: The operator's name, for the refusal this function reports.
        keys: The members' data context keys, entry by entry.
        offsets: The offsets of each entry in ``keys``: one more than the entries,
            each entry with at least one member.
        tags: The user tags of the rows the members come from; the refusal shows
            one member's.
        tag_rows: The row of ``tags`` each member is, in the order of ``keys``.

    Returns:
        One data context key per entry.

    Raises:
        InputValidationError: If the members of an entry are of different data
            contexts. The message names the operator, the contexts of that entry and
            the tags of one of its rows.
    """
    bounds = offsets.to_numpy()
    starts = bounds[:-1]
    gathered = keys.take(pa.array(starts, pa.int64()))
    if len(keys.unique()) < 2:
        return gathered
    codes = keys.dictionary_encode().indices.to_numpy(zero_copy_only=False)
    # Each member against the first member of its own entry.
    differs = np.flatnonzero(
        codes[: bounds[-1]] != np.repeat(codes[starts], np.diff(bounds))
    )
    if not differs.size:
        return gathered
    member = int(differs[0])
    entry = int(np.searchsorted(bounds, member, side="right")) - 1
    members = keys.slice(bounds[entry], bounds[entry + 1] - bounds[entry])
    found = sorted(cast("set[str]", set(members.to_pylist())))
    shown = tags.slice(tag_rows[member].as_py(), 1).to_pylist()[0]
    raise InputValidationError(
        f"{operator} gathers several rows into one, so they must all have the same data "
        + f"context; it would gather rows of the data contexts {found}, one of them the "
        + f"row with the tags {shown} (data context {keys[member].as_py()!r}). Nothing is "
        + "converted between data contexts: give the operator rows of one data context."
    )


def with_context_keys(table: pa.Table, keys: pa.Array[Any]) -> pa.Table:
    """Append the data context column to an operator's result table.

    Args:
        table: The result, without the column.
        keys: The data context key of each of its rows.

    Returns:
        ``table`` with the column.
    """
    return table.append_column(
        pa.field(constants.CONTEXT_KEY, pa.large_string(), nullable=False), keys
    )


def gathered_stream_context(
    keys: pa.Array[Any], inputs: contexts.DataContext | None
) -> contexts.DataContext | None:
    """Return the data context of the stream that holds the gathered entries.

    Args:
        keys: The entries' data context keys.
        inputs: The data context the operator's input rows report.

    Returns:
        The entries' one data context; ``inputs`` when there is no entry, or when the
        entries are of several data contexts (each then names its own in the table, and
        the stream reports one, as any stream does).
    """
    found = keys.unique().to_pylist()
    if len(found) != 1 or (inputs is not None and found[0] == inputs.context_key):
        return inputs
    return contexts.resolve_context(cast(str, found[0]))
