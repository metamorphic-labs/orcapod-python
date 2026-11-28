"""ConnectorArrowDatabase — generic ArrowDatabaseProtocol backed by any DBConnectorProtocol.

Implements the full ArrowDatabaseProtocol on top of any DBConnectorProtocol,
owning all record-management logic: record_path → table name mapping,
``__record_id`` column convention, in-memory pending-batch management,
deduplication, upsert, and flush.

Connector implementations (SQLiteConnector, PostgreSQLConnector, SpiralDBConnector)
need only satisfy DBConnectorProtocol; they do not implement ArrowDatabaseProtocol.

Example::

    connector = SQLiteConnector(":memory:")   # PLT-1076
    db = ConnectorArrowDatabase(connector)
    db.add_record(("results", "my_fn"), record_id=b"sha256:abc", record=table)
    db.flush()
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Callable, Collection, Mapping
from contextlib import AbstractContextManager
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc

from orcapod.databases._pending_commits import PendingCommit, PendingCommits
from orcapod.databases.utils import (
    coerce_record_id,
    first_changed_column,
    first_logical_identity_change,
    in_held_column_order,
)
from orcapod.errors import (
    InputValidationError,
    OrcapodDatabaseError,
    OrcapodDatabaseReadError,
    UnstorableColumnError,
    UnstorableTableNameError,
)
from orcapod.protocols.db_connector_protocol import ColumnInfo, DBConnectorProtocol
from orcapod.utils.arrow_utils import (
    normalize_schema_to_large_types,
    normalize_table_to_large_types,
    normalize_to_large_types,
)

#: The ``type`` of a connector database's saved configuration.
CONNECTOR_DATABASE_TYPE = "connector_arrow_database"


def _arrow_schema_to_column_infos(schema: pa.Schema) -> list[ColumnInfo]:
    """Convert a PyArrow schema to a list of ColumnInfo."""
    return [
        ColumnInfo(name=field.name, arrow_type=field.type, nullable=field.nullable)
        for field in schema
    ]


def _alike(parts: list[pa.Table]) -> list[pa.Table]:
    """Stored and queued rows under one schema when they differ only in a column's
    nullability: a store may read a column back ``NOT NULL`` (PostgreSQL's primary
    key) that the queued rows declare nullable. Each such field is nullable when
    either side's is, and the arrays are re-wrapped under it, never cast. Any other
    difference is left for ``pa.concat_tables`` to refuse, as before."""
    first = parts[0].schema
    if any(part.schema.names != first.names for part in parts):
        return parts
    fields = []
    for i, field in enumerate(first):
        others = [part.schema.field(i) for part in parts[1:]]
        loose = field.with_nullable(True)
        if not all(
            other.with_nullable(True).equals(loose, check_metadata=True)
            for other in others
        ):
            return parts
        fields.append(
            field.with_nullable(field.nullable or any(o.nullable for o in others))
        )
    schema = pa.schema(fields, metadata=cast(Any, first.metadata))
    return [
        part
        if part.schema.equals(schema, check_metadata=True)
        else pa.Table.from_arrays(part.columns, schema=schema)
        for part in parts
    ]


class ConnectorArrowDatabase:
    """Generic ``ArrowDatabaseProtocol`` implementation backed by a ``DBConnectorProtocol``.

    Records are buffered in memory (pending batch) and written to the connector
    on ``flush()``. The ``record_path`` tuple is mapped to a sanitized SQL table
    name using ``"__".join(sanitized_parts)``.

    Args:
        connector: A ``DBConnectorProtocol`` implementation providing the
            underlying DB access (connection, type mapping, queries, writes).
        max_hierarchy_depth: Maximum allowed length for ``record_path`` tuples.
            Defaults to 10, matching ``InMemoryArrowDatabase``.
    """

    stores_records = True

    RECORD_ID_COLUMN = "__record_id"
    _ROW_INDEX_COLUMN = "__row_index"

    def preview_appended_rows(self, stored: pa.Table, incoming: pa.Table) -> pa.Table:
        """Return ``stored`` followed by ``incoming``, as a read after the append gives them.

        This store keeps a table's columns as they are: ``incoming`` holds every
        stored column, taken by name.
        """
        return pa.concat_tables([stored, incoming.select(stored.column_names)])

    def __init__(
        self,
        connector: DBConnectorProtocol,
        max_hierarchy_depth: int = 10,
        _path_prefix: tuple[str, ...] = (),
        _shared_pending_batches: dict[str, pa.Table] | None = None,
        _shared_pending_record_ids: dict[str, set[bytes]] | None = None,
        _shared_pending_skip_existing: dict[str, bool] | None = None,
        _root: ConnectorArrowDatabase | None = None,
        _scoped_path: tuple[str, ...] = (),
    ) -> None:
        self._connector = connector
        self.max_hierarchy_depth = max_hierarchy_depth
        self._path_prefix = _path_prefix
        self._root = _root
        self._scoped_path = _scoped_path
        self._pending_batches: dict[str, pa.Table] = (
            _shared_pending_batches if _shared_pending_batches is not None else {}
        )
        self._pending_record_ids: dict[str, set[bytes]] = (
            _shared_pending_record_ids
            if _shared_pending_record_ids is not None
            else defaultdict(set)
        )
        # The schema each stored table was read with, for the append check
        # (``_refuse_changed_columns``).
        self._stored_schemas: dict[str, pa.Schema] = {}
        # Per-batch flag: True when the batch was added with skip_duplicates=True,
        # so flush() can pass skip_existing=True to the connector and let it use
        # native INSERT-OR-IGNORE semantics rather than Python-side prefiltering.
        self._pending_skip_existing: dict[str, bool] = (
            _shared_pending_skip_existing
            if _shared_pending_skip_existing is not None
            else {}
        )

        self._pending_commits = (
            _root._pending_commits
            if _root is not None
            else PendingCommits(
                self.RECORD_ID_COLUMN,
                self._pending_batches,
                self._pending_record_ids,
                on_empty=self._discard_pending_duplicate_policy,
            )
        )

    def _discard_pending_duplicate_policy(self, key: str) -> None:
        """Forget the duplicate policy when an entire queued batch is discarded."""
        self._pending_skip_existing.pop(key, None)

    # ── Path helpers ──────────────────────────────────────────────────────────

    def _get_record_key(self, record_path: tuple[str, ...]) -> str:
        return "/".join(self._path_prefix + record_path)

    def _path_to_table_name(self, record_path: tuple[str, ...]) -> str:
        """Map a record_path to a safe SQL table name.

        Each component is sanitized (non-alphanumeric chars → ``_``), then
        joined with ``__`` as separator. A ``t_`` prefix is added if the result
        starts with a digit to ensure a valid SQL identifier.
        """
        parts = [re.sub(r"[^a-zA-Z0-9_]", "_", part) for part in record_path]
        name = "__".join(parts)
        if name and name[0].isdigit():
            name = "t_" + name
        return name

    def _validate_record_path(self, record_path: tuple[str, ...]) -> None:
        if not record_path:
            raise ValueError("record_path cannot be empty")
        if len(self._path_prefix) + len(record_path) > self.max_hierarchy_depth:
            raise ValueError(
                f"record_path depth {len(record_path)} exceeds maximum "
                f"{self.max_hierarchy_depth - len(self._path_prefix)} "
                f"(base_path uses {len(self._path_prefix)} components)"
            )
        for i, component in enumerate(record_path):
            if not component or not isinstance(component, str):
                raise ValueError(
                    f"record_path component {i} is invalid: {repr(component)}"
                )
            # "/" is the separator used by _get_record_key; "\0" is a common
            # string-boundary sentinel. Both would corrupt key round-tripping
            # in flush() where record_path is reconstructed via split("/").
            if "/" in component or "\0" in component:
                raise ValueError(
                    f"record_path component {repr(component)} contains an "
                    "invalid character ('/' or '\\0')"
                )

    # ── Record-ID column helpers ──────────────────────────────────────────────

    def _ensure_record_id_column(
        self, arrow_data: pa.Table, record_id: str | bytes
    ) -> pa.Table:
        record_id = coerce_record_id(record_id)
        if self.RECORD_ID_COLUMN not in arrow_data.column_names:
            key_array = pa.array([record_id] * len(arrow_data), type=pa.large_binary())
            arrow_data = arrow_data.add_column(0, self.RECORD_ID_COLUMN, key_array)
        return arrow_data

    def _remove_record_id_column(self, arrow_data: pa.Table) -> pa.Table:
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            arrow_data = arrow_data.drop([self.RECORD_ID_COLUMN])
        return arrow_data

    def _handle_record_id_column(
        self, arrow_data: pa.Table, record_id_column: str | None
    ) -> pa.Table:
        if not record_id_column:
            return self._remove_record_id_column(arrow_data)
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            new_names = [
                record_id_column if name == self.RECORD_ID_COLUMN else name
                for name in arrow_data.schema.names
            ]
            return arrow_data.rename_columns(new_names)
        raise ValueError(
            f"Record ID column '{self.RECORD_ID_COLUMN}' not found in table."
        )

    # ── Deduplication ─────────────────────────────────────────────────────────

    def _deduplicate_within_table(self, table: pa.Table) -> pa.Table:
        """Keep the last occurrence of each record ID within a single table."""
        if table.num_rows <= 1:
            return table
        indices = pa.array(range(table.num_rows))
        table_with_idx = table.add_column(0, self._ROW_INDEX_COLUMN, indices)
        grouped = table_with_idx.group_by([self.RECORD_ID_COLUMN]).aggregate(
            [(self._ROW_INDEX_COLUMN, "max")]
        )
        max_indices = grouped[f"{self._ROW_INDEX_COLUMN}_max"].to_pylist()
        mask = pc.is_in(indices, pa.array(max_indices))
        return table.filter(mask)

    # ── Committed data access ─────────────────────────────────────────────────

    def _get_committed_table(self, record_path: tuple[str, ...]) -> pa.Table | None:
        """Fetch all committed records for a path from the connector."""
        table_name = self._path_to_table_name(self._path_prefix + record_path)
        self._require_table_name(table_name)
        if table_name not in self._connector.get_table_names():
            return None
        batches = list(self._connector.iter_batches(f'SELECT * FROM "{table_name}"'))
        if not batches:
            return None
        return pa.Table.from_batches(batches)

    def list_table_paths(self, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
        """List the paths of the tables stored under ``prefix``.

        A table name in the backend does not keep the path it was made from (see
        ``_path_to_table_name``), so this lists by name: the tables, committed or
        pending, whose name starts with the name ``prefix`` maps to. The components
        after ``prefix`` are the rest of the name split at ``__``. They are in the
        backend's form: every character other than letters, digits and ``_`` is
        ``_``, and a component that holds ``__`` comes back as two. Such a path
        still names the same table. A table of another path that maps to a name
        with the same beginning is listed as well. With an empty prefix at the
        root, a name that got the ``t_`` prefix keeps it.

        Costs one listing of the backend's table names; no table is read.

        Args:
            prefix: Leading path components, relative to ``self.base_path``.

        Returns:
            The table paths, relative to ``self.base_path``, sorted.
        """
        full_prefix = self._path_prefix + prefix
        names = set(self._connector.get_table_names())
        names.update(
            self._path_to_table_name(tuple(key.split("/")))
            for key in self._pending_batches
        )
        stem = self._path_to_table_name(full_prefix) if full_prefix else ""
        found: set[tuple[str, ...]] = set()
        for name in names:
            if not stem:
                found.add(tuple(name.split("__")))
            elif name == stem:
                found.add(prefix)
            elif name.startswith(stem + "__"):
                found.add(prefix + tuple(name[len(stem) + 2 :].split("__")))
        return sorted(found)

    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Return ``True`` if a table exists at ``record_path``.

        Checks both the in-memory pending batch and the connector's committed
        tables. This is a cheap existence check that does NOT load any records.

        Args:
            record_path: Path components identifying the table, relative to
                ``self.base_path``.

        Returns:
            ``True`` if a table exists at the given path, ``False`` otherwise.
        """
        table_name = self._path_to_table_name(self._path_prefix + record_path)
        self._require_table_name(table_name)
        record_key = self._get_record_key(record_path)
        if record_key in self._pending_batches:
            return True
        return table_name in self._connector.get_table_names()

    # ── Storable columns ──────────────────────────────────────────────────────

    def get_table_schema(self, record_path: tuple[str, ...]) -> pa.Schema | None:
        """Read the committed or queued schema without fetching rows.

        Raises:
            OrcapodDatabaseReadError: If the connector cannot inspect the schema.
        """
        full_path = self._path_prefix + record_path
        table_name = self._path_to_table_name(full_path)
        self._require_table_name(table_name)
        try:
            if table_name not in self._connector.get_table_names():
                pending = self._pending_batches.get(self._get_record_key(record_path))
                return None if pending is None else pending.schema
            return pa.schema(
                pa.field(column.name, column.arrow_type, nullable=column.nullable)
                for column in self._connector.get_column_info(table_name)
            )
        except OrcapodDatabaseError:
            raise
        except Exception as exc:
            raise OrcapodDatabaseReadError(table_name, full_path) from exc

    def require_storable(self, schema: pa.Schema) -> None:
        """Refuse a table schema with a column the connector cannot store (ITL-1162).

        The connector states which columns it stores
        (``DBConnectorProtocol.unstorable_column_reason``): a column it would read
        back as another type, or could not write at all, is refused. Called by
        ``add_records`` before anything of the table is buffered, and by a job node
        when the database is attached to it, before anything runs.

        Args:
            schema: The Arrow schema of a table this database is to hold. The
                ``__record_id`` column, when present, is checked as the key.

        Raises:
            UnstorableColumnError: For the first column the connector cannot
                store, naming the connector, the column, its Arrow type and why.
        """
        for field in schema:
            reason = self._connector.unstorable_column_reason(
                field.name,
                field.type,
                primary_key=field.name == self.RECORD_ID_COLUMN,
            )
            if reason is not None:
                raise UnstorableColumnError(
                    database_kind=type(self._connector).__name__,
                    column=field.name,
                    arrow_type=field.type,
                    reason=reason,
                )

    def _require_table_name(self, table_name: str) -> None:
        """Refuse a table name the connector would not keep (ITL-1162).

        Called at every entry that creates, writes, reads or checks a table, so a
        name the backend would change (truncate) is never sent to it.

        Raises:
            UnstorableTableNameError: Naming the connector, the name and why.
        """
        reason = self._connector.unstorable_table_name_reason(table_name)
        if reason is not None:
            raise UnstorableTableNameError(
                database_kind=type(self._connector).__name__,
                table_name=table_name,
                reason=reason,
            )

    # ── Write methods ─────────────────────────────────────────────────────────

    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str | bytes,
        record: pa.Table,
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        """Add a single record identified by ``record_id``."""
        data_with_id = self._ensure_record_id_column(record, record_id)
        self.add_records(
            record_path=record_path,
            records=data_with_id,
            record_id_column=self.RECORD_ID_COLUMN,
            skip_duplicates=skip_duplicates,
            flush=flush,
        )

    @property
    def keeps_field_metadata(self) -> bool:
        """Whether this database keeps Arrow field metadata: as its connector states.

        A connector without the ``keeps_field_metadata`` member does not keep it
        (ITL-1019).
        """
        from orcapod.protocols.database_protocols import keeps_field_metadata

        return keeps_field_metadata(self._connector)

    def validate_records(self, records: pa.Table) -> None:
        """Raise ``ValueError`` if the connector could not store ``records`` as given.

        The check ``add_records`` applies, without writing anything.

        Args:
            records: The rows (any number, none included) to check.
        """
        self._connector.validate_records(records)

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: pa.Table,
        record_id_column: str | None = None,
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        """Add multiple records to the pending batch."""
        self._validate_record_path(record_path)
        if records.num_rows == 0:
            return

        if record_id_column is None:
            record_id_column = records.column_names[0]
        if record_id_column not in records.column_names:
            raise ValueError(
                f"record_id_column '{record_id_column}' not found in table columns: "
                f"{records.column_names}"
            )

        # Normalise to internal column name
        if record_id_column != self.RECORD_ID_COLUMN:
            rename_map = {record_id_column: self.RECORD_ID_COLUMN}
            records = records.rename_columns(
                [rename_map.get(c, c) for c in records.column_names]
            )

        # Enforce binary record-id type — string columns are rejected to prevent
        # silent deduplication failures (set[str] & set[bytes] is always empty).
        rid_type = records[self.RECORD_ID_COLUMN].type
        if not pa.types.is_large_binary(rid_type) and not pa.types.is_binary(rid_type):
            raise TypeError(
                f"Record-id column must be pa.large_binary() or pa.binary(), "
                f"got {rid_type}. Encode the column to bytes before calling add_records()."
            )

        # Checked and buffered in the large variants every connector reads back
        # (view types included), so a read that joins buffered and committed rows
        # sees one layout (ITL-1122, ITL-1162).
        self.require_storable(
            normalize_schema_to_large_types(records.schema, keep_null=True)
        )
        self._require_table_name(
            self._path_to_table_name(self._path_prefix + record_path)
        )
        records = normalize_table_to_large_types(records, keep_null=True)
        self._connector.validate_records(records)

        record_key = self._get_record_key(record_path)
        # Columns match by name: rows with the held columns in another order are
        # put in the held order, so the connector always receives the stored order
        # (ITL-1367).
        # A column of another logical type is refused first, on the raw identifier
        # bytes, before the reorder compares types (ITL-1371).
        # The held schemas are gathered once for both checks (one table listing).
        held = self._held_schemas(record_path)
        self._refuse_changed_columns(record_path, records, held, identity_only=True)
        records = in_held_column_order(records, self._held_column_order(record_path))
        self._refuse_changed_columns(record_path, records, held)

        records = self._deduplicate_within_table(records)
        input_ids = set(cast(list[bytes], records[self.RECORD_ID_COLUMN].to_pylist()))

        if skip_duplicates:
            # Only filter records that conflict with the in-flight pending batch.
            # Committed duplicates are handled at flush time via
            # upsert_records(skip_existing=True), which lets the connector use
            # native INSERT-OR-IGNORE semantics — no full-table read needed here.
            pending_conflicts = input_ids & self._pending_record_ids[record_key]
            if pending_conflicts:
                mask = pc.invert(
                    pc.is_in(
                        records[self.RECORD_ID_COLUMN],
                        pa.array(list(pending_conflicts)),
                    )
                )
                records = records.filter(mask)
            if records.num_rows == 0:
                return
            # Mark this pending slot so flush() uses skip_existing=True.
            self._pending_skip_existing[record_key] = True
        else:
            conflicts = input_ids & self._pending_record_ids[record_key]
            if conflicts:
                raise ValueError(
                    f"Records with IDs {conflicts} already exist in the pending batch. "
                    "Use skip_duplicates=True to skip them."
                )

        # Buffer in pending batch
        existing_pending = self._pending_batches.get(record_key)
        if existing_pending is None:
            self._pending_batches[record_key] = records
        else:
            self._pending_batches[record_key] = pa.concat_tables(
                [existing_pending, records]
            )
        self._pending_record_ids[record_key].update(
            cast(list[bytes], records[self.RECORD_ID_COLUMN].to_pylist())
        )

        if flush:
            self.flush()

    def _held_column_order(
        self, record_path: tuple[str, ...]
    ) -> "list[pa.Field[Any]] | None":
        """The columns of the batch pending for the table, in its order, or ``None``.

        An append is aligned when it is queued only to rows this handle queued
        (ITL-1367): nothing about a stored table is trusted across calls, since
        another connection may rename, add, drop, retype or create again a column
        between two calls. The stored table is looked up fresh where queued and
        stored rows meet, a flush (``_stored_columns``, main's own lookup) and a read
        that combines them (the rows it reads), and the queued rows are matched to it
        there, by name, after the flush's own check. A point lookup of a queued row
        answers from memory, as main does.
        """
        pending = self._pending_batches.get(self._get_record_key(record_path))
        if pending is not None:
            return list(pending.schema)
        return None

    def _stored_columns(self, table_name: str) -> "list[pa.Field[Any]] | None":
        """The stored table's columns as the connector reports them now, in its order
        (``get_column_info``, large-normalized types), or ``None`` when it does not
        exist. Never remembered: each call asks the connector."""
        if table_name not in self._connector.get_table_names():
            return None
        return [
            pa.field(
                c.name,
                normalize_to_large_types(c.arrow_type, keep_null=True),
                nullable=c.nullable,
            )
            for c in self._connector.get_column_info(table_name)
        ]

    def _held_schemas(self, record_path: tuple[str, ...]) -> list[pa.Schema] | None:
        """The schemas held for the table, the pending batch's and the stored one's,
        for ``_refuse_changed_columns``; ``None`` when the connector keeps no field
        metadata. The stored table's schema is read once per table
        (``_read_stored_schema``): an append the check accepts does not change it."""
        if not self.keeps_field_metadata:
            return None
        held: list[pa.Schema] = []
        pending = self._pending_batches.get(self._get_record_key(record_path))
        if pending is not None:
            held.append(pending.schema)
        table_name = self._path_to_table_name(self._path_prefix + record_path)
        stored = self._stored_schemas.get(table_name)
        if stored is None and table_name in self._connector.get_table_names():
            stored = self._read_stored_schema(table_name)
            if stored is not None:
                self._stored_schemas[table_name] = stored
        if stored is not None:
            held.append(stored)
        return held

    def _refuse_changed_columns(
        self,
        record_path: tuple[str, ...],
        records: pa.Table,
        held: list[pa.Schema] | None,
        *,
        identity_only: bool = False,
    ) -> None:
        """Refuse rows whose columns would not read back as written (ITL-1334).

        The table holds one type per column, so an appended column must have the held
        column's field metadata and nested field attributes (``appended_field_change``),
        held by the stored table and by a batch pending for it: otherwise one side's
        rows would read back with the other's. A connector that keeps no field
        metadata refuses metadata and nested types itself, so only one that keeps it
        is checked (``held`` is ``None`` for one that keeps none).

        A column of another logical type identifier is refused first
        (``first_logical_identity_change``, ITL-1371); ``identity_only`` checks only
        that, so it can run before the rows are reordered by name.

        Raises:
            InputValidationError: For the first such column, naming it and the path.
        """
        if held is None:
            return
        # A column of another logical type would read back as the held one (ITL-1371).
        changed = first_logical_identity_change(held, records)
        if changed is None and not identity_only:
            changed = first_changed_column(held, records.schema)
        if changed is not None:
            raise InputValidationError(
                f"Cannot append to the table at {record_path!r}: column "
                f"{changed[0]!r}: {changed[1]}. Nothing was written."
            )

    def _read_stored_schema(self, table_name: str) -> pa.Schema | None:
        """The schema a read of the stored table gives, its field metadata included.

        Read without rows from a connector that offers ``get_arrow_schema``
        (``SpiralDBConnector``); from any other, the schema of its first batch.
        """
        get_arrow_schema = getattr(self._connector, "get_arrow_schema", None)
        if callable(get_arrow_schema):
            return cast("pa.Schema", get_arrow_schema(table_name))
        first = next(
            iter(self._connector.iter_batches(f'SELECT * FROM "{table_name}"')), None
        )
        return None if first is None else first.schema

    # ── base_path / at ────────────────────────────────────────────────────────

    @property
    def base_path(self) -> tuple[str, ...]:
        """The current relative root of this database view (always () for root instances)."""
        return self._path_prefix

    def at(self, *path_components: str) -> "ConnectorArrowDatabase":
        """Return a new ConnectorArrowDatabase scoped to the given sub-path.

        The returned instance shares the connector and all three pending dicts
        (_pending_batches, _pending_record_ids, _pending_skip_existing) by reference.
        Calling flush() on any view drains the entire shared pending queue.

        Raises:
            ValueError: If any path component is empty, not a str, or contains
                ``'/'`` or ``'\\0'`` (which would corrupt the ``'/'``-separated
                record key scheme and break ``flush()``'s key reconstruction).
        """
        for i, component in enumerate(path_components):
            if not component or not isinstance(component, str):
                raise ValueError(
                    f"at() path component {i} is invalid: {repr(component)}"
                )
            if "/" in component or "\0" in component:
                raise ValueError(
                    f"at() path component {repr(component)} contains an invalid character "
                    "('/' or '\\0')"
                )
        new_root = self._root if self._root is not None else self
        new_scoped_path = self._scoped_path + path_components
        return ConnectorArrowDatabase(
            connector=self._connector,
            max_hierarchy_depth=self.max_hierarchy_depth,
            _path_prefix=self._path_prefix + path_components,
            _shared_pending_batches=self._pending_batches,
            _shared_pending_record_ids=self._pending_record_ids,
            _shared_pending_skip_existing=self._pending_skip_existing,
            _root=new_root,
            _scoped_path=new_scoped_path,
        )

    # ── Flush ─────────────────────────────────────────────────────────────────

    def pending_result(
        self, record_path: tuple[str, ...], record_id: bytes
    ) -> PendingCommit | None:
        """Return a commit dependency for a buffered result, or verify it is stored."""
        key = self._get_record_key(record_path)
        if record_id in self._pending_record_ids.get(key, ()):
            return self._pending_commits.attempt(key)
        if self.get_record_by_id(record_path, record_id) is None:
            raise ValueError("The referenced result is neither stored nor pending.")
        return None

    def fail_pending_result(self, record_path: tuple[str, ...]) -> None:
        """Withdraw records depending on this table's current commit attempt."""
        self._pending_commits.finish(self._get_record_key(record_path), committed=False)

    def guard_record(
        self,
        record_path: tuple[str, ...],
        record_id: bytes,
        dependency: PendingCommit,
    ) -> AbstractContextManager[None]:
        """Prevent this record's publication until its result commits."""
        return self._pending_commits.guard(
            self._get_record_key(record_path), record_id, dependency
        )

    def guard_table_schema(
        self, record_path: tuple[str, ...], check: Callable[[pa.Schema], None]
    ) -> AbstractContextManager[None]:
        """Check the schema at the connector's commit."""
        return self._pending_commits.guard_schema(
            self._get_record_key(record_path), check
        )

    def flush(self) -> None:
        """Commit all pending batches to the connector via upsert."""
        for record_key in list(self._pending_batches):
            self._flush_by_key(record_key)

    def refresh_table(self, record_path: tuple[str, ...]) -> None:
        """Nothing to refresh: every read queries the connector's backend (ITL-1408)."""

    def flush_result_batch(
        self, record_path: tuple[str, ...], *, publish_records: bool
    ) -> None:
        """Commit a durable result table even if no pipeline records are stored."""
        self.flush_batch(record_path)

    def flush_batch(self, record_path: tuple[str, ...]) -> None:
        """Commit one table's ready records."""
        self._flush_by_key(self._get_record_key(record_path))

    def _flush_by_key(self, record_key: str) -> None:
        """Commit the ready records at a key shared by all views."""
        ready = self._pending_commits.take_ready(record_key)
        if ready is None:
            return
        pending, ids = ready
        record_path = tuple(record_key.split("/"))
        table_name = self._path_to_table_name(record_path)
        skip_existing = self._pending_skip_existing.get(record_key, False)
        if record_key not in self._pending_batches:
            self._pending_skip_existing.pop(record_key, None)

        try:
            self._require_table_name(table_name)
            columns = _arrow_schema_to_column_infos(pending.schema)

            # Schema validation: if the table already exists, confirm the column
            # names and Arrow types match before writing.  Schema evolution is
            # intentionally out of scope; a clear ValueError is preferable to a
            # cryptic DB-level error or a silent partial write.
            # Looked up now, as the check needs: nothing about the stored table is
            # trusted from an earlier call (ITL-1367).
            existing = self._stored_columns(table_name)
            check = self._pending_commits.schema_check(record_key)
            if check is not None:
                check(pending.schema if existing is None else pa.schema(existing))
            if existing is not None:
                # A connector reads text and binary back in their large variants,
                # which are the same types (ITL-1162).
                existing_cols = {f.name: f.type for f in existing}
                pending_cols = {
                    c.name: normalize_to_large_types(c.arrow_type, keep_null=True)
                    for c in columns
                }
                if existing_cols != pending_cols:

                    def _fmt(cols: dict) -> str:
                        return (
                            "{"
                            + ", ".join(f"{k}: {v}" for k, v in sorted(cols.items()))
                            + "}"
                        )

                    raise ValueError(
                        f"Schema mismatch for table {table_name!r}: "
                        f"existing={_fmt(existing_cols)}, "
                        f"pending={_fmt(pending_cols)}. "
                        "Schema evolution is not supported."
                    )

                # Checked by exact name and type above: only then are the rows put
                # in the stored order, by name (ITL-1367).
                pending = in_held_column_order(pending, existing)

            self._connector.create_table_if_not_exists(
                table_name, columns, pk_column=self.RECORD_ID_COLUMN
            )
            self._connector.upsert_records(
                table_name,
                pending,
                id_column=self.RECORD_ID_COLUMN,
                skip_existing=skip_existing,
            )
        except BaseException:
            self._pending_commits.finish(record_key, committed=False)
            raise
        else:
            self._pending_commits.finish(record_key, committed=True)
        finally:
            self._pending_commits.forget(record_key, ids)

    # ── Read methods ──────────────────────────────────────────────────────────

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: str | bytes,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> pa.Table | None:
        if flush:
            self.flush()
        record_id = coerce_record_id(record_id)
        record_key = self._get_record_key(record_path)

        # Check pending first
        if record_id in self._pending_record_ids.get(record_key, set()):
            pending = self._pending_batches[record_key]
            filtered = pending.filter(pc.field(self.RECORD_ID_COLUMN) == record_id)
            if filtered.num_rows > 0:
                return self._handle_record_id_column(filtered, record_id_column)

        # Check committed
        committed = self._get_committed_table(record_path)
        if committed is None:
            return None
        filtered = committed.filter(pc.field(self.RECORD_ID_COLUMN) == record_id)
        if filtered.num_rows == 0:
            return None
        return self._handle_record_id_column(filtered, record_id_column)

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
    ) -> pa.Table | None:
        record_key = self._get_record_key(record_path)
        parts: list[pa.Table] = []

        committed = self._get_committed_table(record_path)
        if committed is not None and committed.num_rows > 0:
            parts.append(committed)
        pending = self._pending_batches.get(record_key)
        if pending is not None and pending.num_rows > 0:
            # Matched to the stored rows just read, by name (ITL-1367).
            if committed is not None:
                pending = in_held_column_order(
                    pending,
                    [
                        f.with_type(normalize_to_large_types(f.type, keep_null=True))
                        for f in committed.schema
                    ],
                )
            parts.append(pending)

        if not parts:
            return None
        table = parts[0] if len(parts) == 1 else pa.concat_tables(_alike(parts))
        return self._handle_record_id_column(table, record_id_column)

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: Collection[str | bytes],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> pa.Table | None:
        if flush:
            self.flush()
        ids_list = [coerce_record_id(r) for r in record_ids]
        if not ids_list:
            return None
        all_records = self.get_all_records(
            record_path, record_id_column=self.RECORD_ID_COLUMN
        )
        if all_records is None:
            return None
        filtered = all_records.filter(
            pc.is_in(all_records[self.RECORD_ID_COLUMN], pa.array(ids_list))
        )
        if filtered.num_rows == 0:
            return None
        return self._handle_record_id_column(filtered, record_id_column)

    def get_records_with_column_value(
        self,
        record_path: tuple[str, ...],
        column_values: Collection[tuple[str, Any]] | Mapping[str, Any],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> pa.Table | None:
        if flush:
            self.flush()
        all_records = self.get_all_records(
            record_path, record_id_column=self.RECORD_ID_COLUMN
        )
        if all_records is None:
            return None

        if isinstance(column_values, Mapping):
            pairs = list(column_values.items())
        else:
            pairs = cast(list[tuple[str, Any]], list(column_values))

        expr = None
        for col, val in pairs:
            e = pc.field(col) == val
            expr = e if expr is None else expr & e  # type: ignore[assignment]

        filtered = all_records.filter(expr)
        if filtered.num_rows == 0:
            return None
        return self._handle_record_id_column(filtered, record_id_column)

    # ── Config ────────────────────────────────────────────────────────────────

    def to_config(self, db_registry: Any = None) -> dict[str, Any]:
        """Serialize configuration to a JSON-compatible dict."""
        return {
            "type": CONNECTOR_DATABASE_TYPE,
            "connector": self._connector.to_config(),
            "base_path": list(self._path_prefix),
            "max_hierarchy_depth": self.max_hierarchy_depth,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> ConnectorArrowDatabase:
        """Refuse: a connector is not rebuilt from a saved configuration (ITL-1391).

        The connection is the user's to supply: a loaded job is given its database,
        ``PipelineJob.load(path, database=ConnectorArrowDatabase(<connector>))``.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "A connector database is not rebuilt from its saved configuration: build "
            "ConnectorArrowDatabase(<connector>) and pass it."
        )
