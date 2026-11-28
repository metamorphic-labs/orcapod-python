"""The data context recorded on stored rows (ITL-944).

Every row Orcapod stores carries the key of the data context that wrote it, in the
column ``__data_context`` (``constants.DATA_CONTEXT_COL``): function pipeline and
result tables, operator-cache tables, cached-source tables and side-effect invocation
logs. The key names one generation of hashing (``std:v1:default`` since Orcapod v0.3),
so two rows with the same key were hashed with exactly the same definitions.

Rows written under the current data context are the current entries: every read and
lookup keeps those rows only. Rows of another data context stay in the table,
unchanged, until they are migrated.

A table at another layout (another last path component than ``CURRENT_LAYOUTS``) was
written by another build of Orcapod; Orcapod reports it and does not read it as results.

The path of a table holds hashes, and those can differ between builds, so such a table is
not looked for at a computed path. The beginning of the path, up to the name of the
function or pod, holds no hash: the tables under it are listed (``list_table_paths``), and
a table whose last path component is not one of the current layouts is of another layout
(ITL-954). This listing also finds current-layout completion tables that the
current job does not address, which can happen when a pipeline hash changes (ITL-1559).

Example:
    To list the data contexts a stored table holds, call
    ``stored_data_contexts(database, record_path)`` (from ``orcapod.contexts``); it
    returns, for example, ``{"std:v1:default": 3}``.
"""

from __future__ import annotations

import logging
import warnings
from collections.abc import Collection
from dataclasses import dataclass, field
from typing import Any, NamedTuple, Protocol, cast

import pyarrow as pa
import pyarrow.compute as pc

from orcapod.errors import (
    InputValidationError,
    SideEffectLogDataContextError,
    OrcapodDatabaseContentWarning,
    TableListingNotSupportedError,
)
from orcapod.system_constants import (
    OPERATOR_DB_SCHEMA_VERSION,
    PIPELINE_DB_SCHEMA_VERSION,
    PRE_V1_DATA_CONTEXT_KEY,
    RESULT_DB_SCHEMA_VERSION,
    SOURCE_DB_SCHEMA_VERSION,
    TRACKING_DB_SCHEMA_VERSION,
    constants,
)

logger = logging.getLogger(__name__)

# The last path component of every table this version of Orcapod writes.
CURRENT_LAYOUTS: frozenset[str] = frozenset(
    {
        PIPELINE_DB_SCHEMA_VERSION,
        RESULT_DB_SCHEMA_VERSION,
        TRACKING_DB_SCHEMA_VERSION,
        OPERATOR_DB_SCHEMA_VERSION,
        SOURCE_DB_SCHEMA_VERSION,
    }
)

# How many table paths a report spells out before it only counts the rest.
_PATHS_SPELLED_OUT = 5


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
        data_context_field(),
        pa.array([context_key] * table.num_rows, type=pa.large_string()),
    )


def data_context_field() -> "pa.Field[pa.DataType]":
    """The ``__data_context`` field every stored table ends with (``with_data_context``)."""
    return pa.field(constants.DATA_CONTEXT_COL, pa.large_string(), nullable=False)


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
        record_path: Path of the stored table from the root of the database.
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


class _DescribedDatabase(Protocol):
    """The part of ``ArrowDatabaseProtocol`` that names a database (no import cycle)."""

    @property
    def base_path(self) -> tuple[str, ...]:
        """Path of this database's view from the root of the underlying database."""
        ...

    def to_config(self) -> dict[str, Any]:
        """Return the database's configuration, with its location when it has one."""
        ...


def describe_database_table(
    database: _DescribedDatabase, record_path: tuple[str, ...]
) -> str:
    """Name a stored table for a message: the database's location and the table's path.

    Args:
        database: The database, or a view of it.
        record_path: Path of the table relative to ``database``.

    Returns:
        For example ``"database at '/data/store' (table 'job/node/...')"``.
    """
    store_config = database.to_config()
    location = store_config.get("root_uri")
    description = (
        f"database at {location!r}"
        if location
        else f"{store_config.get('type', 'attached')} database"
    )
    table = "/".join(database.base_path + record_path)
    return f"{description} (table {table!r})"


def report_unexpected_row_count(
    node: str, location: str, expected: int, found: int
) -> None:
    """Warn that a loaded node's database holds another number of rows than its run's.

    The database is the source of truth: the read returns what it holds. The warning says
    that it differs from what the job file recorded (ITL-1043).

    Args:
        node: The node, as messages name it (``"FunctionJobNode 'dbl'"``).
        location: The database and table, from ``describe_database_table``.
        expected: The rows the job file records for the node's run.
        found: The rows the database holds under the node's run key.
    """
    warnings.warn(
        OrcapodDatabaseContentWarning(
            f"{node}: the job file records {expected} "
            f"{'row' if expected == 1 else 'rows'} for its run, but the {location} "
            f"holds {found} under the run's key. The read returns what the database "
            "holds; the database may not be the one the run wrote, or it may have "
            "changed since."
        ),
        stacklevel=3,
    )


def report_old_layout_rows(record_path: tuple[str, ...], rows: int) -> None:
    """Warn that a table written at an older table layout was found and is not used.

    Args:
        record_path: Path of the old-layout table from the root of the database.
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


def report_tables_under_name(
    name_path: tuple[str, ...], record_paths: Collection[tuple[str, ...]]
) -> None:
    """Warn that tables written before v0.3 were found under a function's name.

    Args:
        name_path: Path, from the root of the database, up to the function's name.
        record_paths: Paths of the tables found, from the root of the database.
    """
    logger.warning(
        "Found %d %s written by an earlier version of Orcapod under %r (older table "
        "layout, data context %s): %s. They are not used and are left unchanged; "
        "they can be migrated later. Tables of another function or node with the "
        "same name are included.",
        len(record_paths),
        "table" if len(record_paths) == 1 else "tables",
        "/".join(name_path),
        PRE_V1_DATA_CONTEXT_KEY,
        _spelled_out(record_paths),
    )


def report_unlisted_side_effect_tables(
    name_paths: Collection[tuple[str, ...]], reason: str
) -> None:
    """Warn that a side-effecting node could not look for completions logged before v0.3.

    Args:
        name_paths: Paths, from the root of the database, up to the node's function or
            pod name: where the tables would have been looked for.
        reason: Why the database could not list its tables.
    """
    logger.warning(
        "The database cannot list its tables (%s), so the node that performs side "
        "effects could not check for completions logged before Orcapod v0.3 under "
        "%s. If the database holds such tables, every side effect is delivered again.",
        reason,
        " and ".join(repr("/".join(path)) for path in name_paths),
    )


def _spelled_out(record_paths: Collection[tuple[str, ...]]) -> str:
    """Return the first few of ``record_paths`` as text, and how many more there are."""
    ordered = sorted(record_paths)
    shown = ", ".join(repr("/".join(path)) for path in ordered[:_PATHS_SPELLED_OUT])
    more = len(ordered) - _PATHS_SPELLED_OUT
    return f"{shown} (and {more} more)" if more > 0 else shown


class _RecordStore(Protocol):
    """The part of ``ArrowDatabaseProtocol`` this module needs (no import cycle)."""

    @property
    def base_path(self) -> tuple[str, ...]:
        """Path of this database's view from the root of the underlying database."""
        ...

    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Return whether a table is stored at ``record_path``.

        Args:
            record_path: Path of the table, relative to ``base_path``.

        Returns:
            True if the database holds a table at that path.
        """
        ...

    def get_all_records(
        self, record_path: tuple[str, ...], record_id_column: str | None = None
    ) -> "pa.Table | None":
        """Return every row of the table at ``record_path``.

        Args:
            record_path: Path of the table, relative to ``base_path``.
            record_id_column: Name of a column to add holding each row's record
                id; no such column when ``None``.

        Returns:
            The rows as an Arrow table, or ``None`` if there are none.
        """
        ...


class StaleTable(NamedTuple):
    """Stored rows a node will not use: an old-layout table, or rows of another data context.

    Attributes:
        record_path: Path of the table from the root of the database.
        data_context: Key of the data context that wrote the rows.
        rows: Number of such rows in the table; ``None`` for a table found by
            listing the database, which is not read.
        old_layout: True for a table at an older layout, False for rows of another
            data context in a table at the current layout.
        found_under: For a table found by listing the database, the path (from the
            root of the database) under which the tables were listed; ``None`` for a
            table found at a computed path.
    """

    record_path: tuple[str, ...]
    data_context: str
    rows: int | None
    old_layout: bool
    found_under: tuple[str, ...] | None = None


class UnlistedTables(NamedTuple):
    """A place where tables written before v0.3 could not be looked for.

    Attributes:
        name_path: Path, from the root of the database, under which the tables would
            have been listed.
        reason: Why the database could not list its tables.
    """

    name_path: tuple[str, ...]
    reason: str


@dataclass
class StaleTables:
    """What a search for stored rows of other generations found.

    Attributes:
        tables: The tables found, each once.
        unlisted: The places that could not be searched because the database cannot
            list its tables.
        other_completion_paths: Current-layout tables recording completions at
            the caller's previous pipeline or result cache path.
    """

    tables: list[StaleTable] = field(default_factory=list)
    unlisted: list[UnlistedTables] = field(default_factory=list)
    other_completion_paths: list[tuple[str, ...]] = field(default_factory=list)

    def include(self, other: "StaleTables") -> None:
        """Add what another search found, leaving out tables already known.

        Args:
            other: The result of a search for another table of the same node.
        """
        known = {(table.record_path, table.old_layout) for table in self.tables}
        self.tables.extend(
            table
            for table in other.tables
            if (table.record_path, table.old_layout) not in known
        )
        self.unlisted.extend(other.unlisted)
        for path in other.other_completion_paths:
            if path not in self.other_completion_paths:
                self.other_completion_paths.append(path)


def tables_of_another_layout(
    paths: list[tuple[str, ...]],
    result_revision: str | None = None,
) -> list[tuple[str, ...]]:
    """Select the listed tables with another layout than this build's.

    Another layout and the current ones are told apart by path components, never by a
    hash: every table this version writes ends in one of ``CURRENT_LAYOUTS``, and a
    function result table has its data function type's revision just before its layout
    (``.../rev1/rdb_v1``, ITL-1259).

    Args:
        paths: The database's table paths under the function or pod name.
        result_revision: The revision component of the result tables the caller reads
            (``rev1``). A listed result table with another revision is of another
            layout too. ``None`` checks the last component only.

    Returns:
        The paths of tables that are not at a current layout, in listing order.
    """
    return [
        path
        for path in paths
        if path[-1] not in CURRENT_LAYOUTS
        or (
            result_revision is not None
            and path[-1] == RESULT_DB_SCHEMA_VERSION
            and (len(path) < 2 or path[-2] != result_revision)
        )
    ]


def _list_table_paths(
    database: object, prefix: tuple[str, ...]
) -> list[tuple[str, ...]]:
    """Call ``list_table_paths`` of a database that may not have the method.

    Raises:
        TableListingNotSupportedError: If the database has no such method or cannot
            list its tables.
    """
    list_table_paths = getattr(database, "list_table_paths", None)
    if list_table_paths is None:
        raise TableListingNotSupportedError(
            f"{type(database).__name__} has no list_table_paths method"
        )
    return cast("list[tuple[str, ...]]", list_table_paths(prefix))


def find_stale_tables(
    database: _RecordStore,
    current: tuple[tuple[str, ...], str] | None = None,
    name_path: tuple[str, ...] | None = None,
    result_revision: str | None = None,
    previous_completion_path: tuple[str, ...] | None = None,
    previous_result: tuple[tuple[str, ...], tuple[str, bytes] | None] | None = None,
) -> StaleTables:
    """Find the stored rows of other generations that belong to one table of a node.

    - under ``name_path``, by listing the database, for tables of another layout
      wherever their hashes put them (ITL-954), and for the completion table of
      this node's previous pipeline identity (ITL-1559);
    - in the current table, for rows of another data context;
    - at the exact previous result path, for evidence of completed calls (ITL-1559).

    Args:
        database: The database the paths belong to.
        current: The table's current path and the current data context key, to also
            look for rows of another data context in the current table (this reads
            the whole table). ``None`` skips that search.
        name_path: The beginning of the table's path up to the name of its function
            or pod, the part that holds no hash. ``None`` skips the listing.
        result_revision: For a result cache, the revision of its data function type's
            record: a result table under ``name_path`` at another revision is listed too
            (``tables_of_another_layout``).
        previous_completion_path: The exact path of this node's completion table
            before its pipeline identity changed, relative to this database.
        previous_result: The exact previous result table path and optional
            column/value pair identifying a result in a shared legacy namespace.

    Returns:
        The tables of another layout found, then one entry per other data context
        found in the current table (in key order), the alternate completion paths,
        and ``name_path`` under ``unlisted`` if the database could not list its tables.
    """
    found = StaleTables()
    if name_path is not None:
        under = database.base_path + name_path
        try:
            paths = _list_table_paths(database, name_path)
        except TableListingNotSupportedError as exc:
            found.unlisted.append(UnlistedTables(under, str(exc)))
        else:
            other_layouts = tables_of_another_layout(paths, result_revision)
            found.include(
                StaleTables(
                    [
                        StaleTable(
                            database.base_path + path,
                            PRE_V1_DATA_CONTEXT_KEY,
                            None,
                            True,
                            under,
                        )
                        for path in other_layouts
                    ]
                )
            )
            if (
                previous_completion_path is not None
                and previous_completion_path in paths
            ):
                found.other_completion_paths.append(
                    database.base_path + previous_completion_path
                )
    if previous_result is not None:
        path, match = previous_result
        if match is None:
            completed = database.table_exists(path)
        else:
            column, value = match
            rows = database.get_all_records(path)
            completed = (
                rows is not None
                and column in rows.column_names
                and pc.any(
                    pc.equal(rows[column], pa.scalar(value, type=pa.large_binary()))
                ).as_py()
                is True
            )
        if completed:
            found.other_completion_paths.append(database.base_path + path)
    if current is not None:
        current_path, current_context = current
        for key, rows in sorted(stored_data_contexts(database, current_path).items()):
            if key != current_context:
                found.tables.append(
                    StaleTable(database.base_path + current_path, key, rows, False)
                )
    return found


def account_for_stale_tables(
    found: StaleTables, *, delivers_side_effects: bool, current_context: str
) -> None:
    """Decide what a node does about stored rows of other generations it will not use.

    This is the one place where that is decided, for every kind of node:

    - A node whose results can be computed again (function, operator and source
      nodes) reports each table with its number of rows and moves on.
    - A node whose run is an external action (a side-effect pod, or a function pod
      that takes the invocation context) must not act again for inputs it already
      handled when those completions cannot be matched at the current path or under
      the current data context. It fails before doing anything. Every old-layout table
      and a completion table at the node's previous pipeline or result path count.
      The error names the tables. Where the database cannot list its tables, the
      node warns and proceeds (developer, November 11, 2026).

    Args:
        found: What ``find_stale_tables`` found for the node's tables.
        delivers_side_effects: Whether running the node is an external action.
        current_context: Key of the node's data context.

    Raises:
        SideEffectLogDataContextError: If ``delivers_side_effects`` and a table was
            found. The caller adds the node label.
    """
    if delivers_side_effects:
        if found.tables or found.other_completion_paths:
            raise SideEffectLogDataContextError(
                tuple(table.record_path for table in found.tables)
                + tuple(found.other_completion_paths),
                found.tables[0].data_context if found.tables else None,
                current_context,
            )
        if found.unlisted:
            report_unlisted_side_effect_tables(
                [unlisted.name_path for unlisted in found.unlisted],
                found.unlisted[0].reason,
            )
        return
    listed: dict[tuple[str, ...], list[tuple[str, ...]]] = {}
    for table in found.tables:
        if table.found_under is not None:
            listed.setdefault(table.found_under, []).append(table.record_path)
        elif table.rows is None:
            continue
        elif table.old_layout:
            report_old_layout_rows(table.record_path, table.rows)
        else:
            report_rows_of_other_data_contexts(table.record_path, table.rows)
    for name_path, record_paths in listed.items():
        report_tables_under_name(name_path, record_paths)


def stored_data_contexts(
    database: _RecordStore, record_path: tuple[str, ...]
) -> dict[str, int]:
    """Count the rows of a stored table by the data context that wrote them.

    Reads the whole table. A table without a ``__data_context`` column was written
    before Orcapod v0.3; all its rows are counted under ``std:v0.1:default``.

    Args:
        database: The database, positioned so that ``record_path`` names the table.
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


def require_storable(database: object, schema: pa.Schema) -> None:
    """Refuse ``schema`` if ``database`` states it cannot store one of its columns.

    The one check for ``ColumnCheckingDatabaseProtocol`` (ITL-1162): a job node asks
    it about each table it stores when a database is attached, before anything runs.
    A database that states no rule (in-memory) stores every column, and nothing is
    checked. Delta states one: the types it cannot hold even through a recorded type
    (ITL-1334).

    Args:
        database: Any database object.
        schema: The Arrow schema of a table the database is to hold.

    Raises:
        UnstorableColumnError: If the database cannot store a column of ``schema``.
    """
    check = getattr(database, "require_storable", None)
    if callable(check):
        _ = check(schema)
