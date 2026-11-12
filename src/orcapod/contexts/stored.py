"""The data context recorded on stored rows (ITL-944).

Every row Orcapod stores carries the key of the data context that wrote it, in the
column ``__data_context`` (``constants.DATA_CONTEXT_COL``): function pipeline and
result tables, operator-cache tables, cached-source tables and side-effect invocation
logs. The key names one generation of hashing (``std:v1:default`` since Orcapod v0.3),
so two rows with the same key were hashed with exactly the same definitions.

Rows written under the current data context are the current entries: every read and
lookup keeps those rows only. Rows of another data context stay in the table,
unchanged, until they are migrated.

Tables written before v0.3 have no ``__data_context`` column and live at an older
table layout (another last path component). They are the generation
``std:v0.1:default``; Orcapod counts and reports them and does not read them as
results.

To list the data contexts a stored table holds::

    from orcapod.contexts import stored_data_contexts

    stored_data_contexts(store, record_path)  # {"std:v1:default": 3}
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, NamedTuple, Protocol, cast

from orcapod.errors import InputValidationError, SideEffectLogDataContextError
from orcapod.system_constants import PRE_V1_DATA_CONTEXT_KEY, constants
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")

logger = logging.getLogger(__name__)


def with_data_context(table: "pa.Table", context_key: str) -> "pa.Table":
    """Return ``table`` with the ``__data_context`` column appended.

    Args:
        table: Rows about to be stored.
        context_key: Key of the data context that produced the rows.

    Returns:
        A new table whose last column is ``__data_context``, a non-null
        ``large_string`` holding ``context_key`` on every row.
    """
    return table.append_column(
        pa.field(constants.DATA_CONTEXT_COL, pa.large_string(), nullable=False),
        pa.array([context_key] * table.num_rows, type=pa.large_string()),
    )


def require_same_data_context(input_context_key: str, own_context_key: str) -> None:
    """Refuse an input that was hashed under another data context than the writer's.

    A stored row names one data context, the one whose hashers produced its keys. An
    input of another data context would give a row whose keys mix two generations of
    hashing, so the writer refuses it before anything is written. What should
    eventually happen instead (the node's data context is meant to win) is tracked
    in ITL-951.

    Args:
        input_context_key: Data context key of the input (a row or a stream).
        own_context_key: Data context key of the node, cache or source that writes.

    Raises:
        InputValidationError: If the two keys differ.
    """
    if input_context_key != own_context_key:
        raise InputValidationError(
            f"The input was produced under the data context {input_context_key!r}, "
            f"but this node stores its rows under {own_context_key!r}. A stored row "
            "carries one data context; convert the input to the node's data context "
            "first, or run the node under the input's data context."
        )


def keep_rows_of_data_context(table: "pa.Table", context_key: str) -> "pa.Table":
    """Drop the rows of ``table`` written under another data context.

    Every read of a whole stored table goes through this function, so rows of another
    data context are never returned as results.

    Args:
        table: Rows read from a stored table at the current layout.
        context_key: Key of the current data context.

    Returns:
        ``table`` itself when no row is dropped, otherwise ``table`` without the rows
        whose ``__data_context`` is not ``context_key``.

    Raises:
        KeyError: If ``table`` has no ``__data_context`` column.
    """
    mine = cast(
        "pa.BooleanArray",
        pc.equal(
            table.column(constants.DATA_CONTEXT_COL),
            pa.scalar(context_key, pa.large_string()),
        ).fill_null(False),
    )
    if pc.all(mine).as_py() is not False:
        return table
    # The single place where stored rows of another data context are left out of a read.
    return table.filter(mine)


def report_rows_of_other_data_contexts(
    record_path: tuple[str, ...], skipped: int
) -> None:
    """Warn that a read left out stored rows written under another data context.

    Args:
        record_path: Path of the stored table from the root of the store.
        skipped: Number of rows of the table left out of the read.
    """
    logger.warning(
        "Skipped %d %s of stored table %r: written under another data context than "
        "the current one. They are not used and are left unchanged; they can be "
        "migrated later.",
        skipped,
        "row" if skipped == 1 else "rows",
        "/".join(record_path),
    )


def report_old_layout_rows(record_path: tuple[str, ...], rows: int) -> None:
    """Warn that a table written at an older table layout was found and is not used.

    Args:
        record_path: Path of the old-layout table from the root of the store.
        rows: Number of rows the table holds.
    """
    logger.warning(
        "Found %d %s in stored table %r written by an earlier version of Orcapod "
        "(older table layout, data context %s). They are not used and are left "
        "unchanged; they can be migrated later.",
        rows,
        "row" if rows == 1 else "rows",
        "/".join(record_path),
        PRE_V1_DATA_CONTEXT_KEY,
    )


class _RecordStore(Protocol):
    """The part of ``ArrowDatabaseProtocol`` this module needs (no import cycle)."""

    @property
    def base_path(self) -> tuple[str, ...]: ...

    def table_exists(self, record_path: tuple[str, ...]) -> bool: ...

    def get_all_records(
        self, record_path: tuple[str, ...], record_id_column: str | None = None
    ) -> "pa.Table | None": ...


class StaleTable(NamedTuple):
    """Stored rows a node will not use: an old-layout table, or rows of another data context.

    Attributes:
        record_path: Path of the table from the root of the store.
        data_context: Key of the data context that wrote the rows.
        rows: Number of such rows in the table.
        old_layout: True for a table at an older layout, False for rows of another
            data context in a table at the current layout.
    """

    record_path: tuple[str, ...]
    data_context: str
    rows: int
    old_layout: bool


def find_stale_tables(
    database: _RecordStore,
    old_paths: tuple[tuple[str, ...], ...],
    current: tuple[tuple[str, ...], str] | None = None,
) -> list[StaleTable]:
    """Find the stored rows of other generations that belong to one table of a node.

    The tables are only read: nothing is written to them.

    Args:
        database: The store the paths belong to.
        old_paths: Paths at which an earlier version of Orcapod stored this table.
        current: The table's current path and the current data context key, to also
            look for rows of another data context in the current table (this reads
            the whole table). ``None`` looks at the old layouts only.

    Returns:
        One entry per old-layout table that exists, then one per other data context
        found in the current table (in key order). Empty when there is none.
    """
    found: list[StaleTable] = []
    for path in old_paths:
        if not database.table_exists(path):
            continue
        records = database.get_all_records(path)
        rows = 0 if records is None else records.num_rows
        found.append(
            StaleTable(database.base_path + path, PRE_V1_DATA_CONTEXT_KEY, rows, True)
        )
    if current is not None:
        current_path, current_context = current
        for key, rows in sorted(stored_data_contexts(database, current_path).items()):
            if key != current_context:
                found.append(
                    StaleTable(database.base_path + current_path, key, rows, False)
                )
    return found


def account_for_stale_tables(
    found: list[StaleTable], *, delivers_side_effects: bool, current_context: str
) -> None:
    """Decide what a node does about stored rows of other generations it will not use.

    This is the one place where that is decided, for every kind of node:

    - A node whose results can be computed again (function, operator and source
      nodes) reports each table with its number of rows and moves on.
    - A node whose run is an external action (a side-effect pod, or a function pod
      that takes the invocation context) must not act again for inputs it already
      handled, and those completions cannot be matched under the current data
      context. It fails before doing anything.

    Args:
        found: What ``find_stale_tables`` found for the node's tables.
        delivers_side_effects: Whether running the node is an external action.
        current_context: Key of the node's data context.

    Raises:
        SideEffectLogDataContextError: If ``delivers_side_effects`` and ``found`` is
            not empty. The caller adds the node label.
    """
    if not found:
        return
    if delivers_side_effects:
        raise SideEffectLogDataContextError(
            tuple(table.record_path for table in found),
            found[0].data_context,
            current_context,
        )
    for table in found:
        if table.old_layout:
            report_old_layout_rows(table.record_path, table.rows)
        else:
            report_rows_of_other_data_contexts(table.record_path, table.rows)


def stored_data_contexts(
    database: _RecordStore, record_path: tuple[str, ...]
) -> dict[str, int]:
    """Count the rows of a stored table by the data context that wrote them.

    Reads the whole table. A table without a ``__data_context`` column was written
    before Orcapod v0.3; all its rows are counted under ``std:v0.1:default``.

    Args:
        database: The store, positioned so that ``record_path`` names the table.
        record_path: Path of the stored table, including its layout component.

    Returns:
        A mapping from data context key to number of rows, for example
        ``{"std:v1:default": 3}``. A missing or empty table gives ``{}``.
    """
    table = database.get_all_records(record_path)
    if table is None or table.num_rows == 0:
        return {}
    if constants.DATA_CONTEXT_COL not in table.column_names:
        return {PRE_V1_DATA_CONTEXT_KEY: table.num_rows}
    per_value = pc.value_counts(table.column(constants.DATA_CONTEXT_COL))
    keys = cast("list[str]", per_value.field(0).to_pylist())
    counts = cast("list[int]", per_value.field(1).to_pylist())
    return dict(zip(keys, counts))
