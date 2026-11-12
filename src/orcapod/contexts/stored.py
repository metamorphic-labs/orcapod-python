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
from typing import TYPE_CHECKING, Protocol, cast

from orcapod.system_constants import constants
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")

logger = logging.getLogger(__name__)

# The data context of everything stored before Orcapod v0.3: rows that carry this key
# in a pre-v0.3 column, and every table without a ``__data_context`` column.
PRE_V1_DATA_CONTEXT_KEY = "std:v0.1:default"


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


def report_old_layout_tables(
    database: _RecordStore, old_paths: tuple[tuple[str, ...], ...]
) -> dict[tuple[str, ...], int]:
    """Count and report the old-layout tables that exist among ``old_paths``.

    The tables are only read: nothing is written to them. One warning is logged per
    table found.

    Args:
        database: The store the paths belong to.
        old_paths: Paths at which an earlier version of Orcapod stored this table.

    Returns:
        The number of rows of each path that holds a table, keyed by the path from
        the root of the store. Empty when no old-layout table exists.
    """
    found: dict[tuple[str, ...], int] = {}
    for path in old_paths:
        if not database.table_exists(path):
            continue
        records = database.get_all_records(path)
        rows = 0 if records is None else records.num_rows
        found[database.base_path + path] = rows
        report_old_layout_rows(database.base_path + path, rows)
    return found


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
