"""The tables of a SpiralDB pipeline store, one dataset's (ITL-1498).

What a ``SpiralDBConnector`` in store mode does with its tables; the design is
``docs/dev/specs/itl-1446-round2-spiral-sdk.md`` (PR 588), section 2.

- A table's name is computed from its full record path (``spiral_store_names``); a
  table is found by opening that name, never by listing.
- Its metadata holds its identity record and its manifest (``spiral_store``): the record
  path, layout, data context, and the logical schema with the stored names. They are
  written in one upsert before its first rows.
- A table under the name that records another path or format, or holds rows without a
  record, is refused (``SpiralStoreTableCollisionError``). A table with no record and no
  rows (a creation that stopped before its first rows) is adopted.
- Rows are encoded on write and decoded on read by the codec; a read gives the logical
  columns, logical types live.
- The store's tables are listed by one listing of the dataset, narrowed by name, and the
  metadata of each candidate (cached by table id): that is how stale layouts are found
  (ITL-954).

Private until the store is switched on (stage 5): there is no lease yet (stage 4).
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa
import pyarrow.compute as pc

import orcapod.databases.spiral_store as codec
import orcapod.databases.spiral_store_names as names
from orcapod.errors import SpiralStoreTableCollisionError
from orcapod.logical_types.canonical_form import declaration_form_schema
from orcapod.types import ColumnInfo

if TYPE_CHECKING:
    import spiral as sp
else:
    from orcapod.utils.lazy_module import LazyModule

    sp = LazyModule("spiral")

DATA_CONTEXT_COLUMN = "__data_context"
"""The column every stored row's data context is in (``constants.DATA_CONTEXT_COL``)."""


class _Scan(Protocol):
    """The part of spiral-sdk's scan the store uses."""

    def to_table(self) -> pa.Table: ...


class _KeyedTable(Protocol):
    """The part of spiral-sdk's ``KeyedTable`` the store uses."""

    @property
    def resource_id(self) -> str: ...

    @property
    def key_columns(self) -> tuple[str, ...]: ...

    def get_metadata(self) -> dict[str, bytes]: ...

    def set_metadata(self, metadata: dict[str, bytes]) -> None: ...

    def scan(
        self, columns: list[str] | None = None, *, limit: int | None = None
    ) -> _Scan: ...

    def append(self, data: pa.Table) -> _KeyedTable: ...


class _Project(Protocol):
    """The part of spiral-sdk's ``Project`` the store uses."""

    def create_keyed_table(
        self, dataset: str, name: str, key_schema: pa.Schema
    ) -> _KeyedTable: ...


class _Client(Protocol):
    """The part of spiral-sdk's ``Client`` the store uses."""

    def project(self, id: str) -> _Project: ...

    def keyed_table_by_id(self, table_id: str) -> _KeyedTable: ...


@dataclass(frozen=True)
class _Found:
    """A store table that exists, with what its metadata records."""

    table_id: str
    record: names.StoreTableRecord
    manifest: codec.StoreManifest


class StoreTables:
    """The store tables of one SpiralDB dataset.

    Args:
        client: A spiral-sdk client.
        project_id: The project.
        dataset: The dataset.
        open_table: Opens a keyed table by name; ``None`` when there is none.
        list_tables: Lists the dataset's keyed tables: names by table id.
        service_call: A context that notes on a failing SDK call what it was doing.
    """

    def __init__(
        self,
        client: _Client,
        project_id: str,
        dataset: str,
        open_table: Callable[[str], _KeyedTable | None],
        list_tables: Callable[[], dict[str, str]],
        service_call: Callable[[str], AbstractContextManager[None]],
    ) -> None:
        self._client: _Client = client
        self._project_id: str = project_id
        self._dataset: str = dataset
        self._open_table: Callable[[str], _KeyedTable | None] = open_table
        self._list_tables: Callable[[], dict[str, str]] = list_tables
        self._service_call: Callable[[str], AbstractContextManager[None]] = service_call
        self._paths: dict[str, tuple[str, ...]] = {}
        self._found: dict[str, _Found] = {}
        self._records_by_id: dict[str, names.StoreTableRecord | None] = {}

    key_column: str = codec.KEY_COLUMN
    """The key column of every store table."""

    def require_key_column(self, name: str) -> None:
        """Refuse a key column other than the store's."""
        if name != codec.KEY_COLUMN:
            raise ValueError(
                f"a SpiralDB store table's key is {codec.KEY_COLUMN!r}, not {name!r}"
            )

    def unstorable_column_reason(
        self, name: str, arrow_type: pa.DataType, primary_key: bool
    ) -> str | None:
        """Why the codec cannot keep a column, or ``None``."""
        if primary_key:
            if name == codec.KEY_COLUMN and pa.types.is_large_binary(arrow_type):
                return None
            return f"a store table's key is {codec.KEY_COLUMN!r}, large_binary"
        key = pa.field(codec.KEY_COLUMN, pa.large_binary(), nullable=False)
        try:
            _ = codec.logical_schema(pa.schema([pa.field(name, arrow_type), key]))
        except codec.UnstorableInSpiralError as exc:
            return str(exc)
        return None

    def validate(self, records: pa.Table) -> None:
        """Run the codec on rows to buffer: the last row of each key, as kept."""
        if records.num_rows == 0:
            return
        _ = _one_data_context(records)
        _ = codec.encode_rows(_last_of_each_key(records))

    # ── names ────────────────────────────────────────────────────────────────

    def table_name(self, path: tuple[str, ...]) -> str:
        """Return the table name of a full record path, and remember the path."""
        name = names.store_table_name(path)
        self._paths[name] = tuple(path)
        return name

    def _path_of(self, table_name: str) -> tuple[str, ...]:
        """The path a name was computed from (``table_name``)."""
        path = self._paths.get(table_name)
        if path is None:
            raise KeyError(
                f"{table_name!r} is not a name this store computed for a path"
            )
        return path

    # ── finding a table ──────────────────────────────────────────────────────

    def _judge(self, table_name: str) -> _Found | None:
        """Return the table at a name if it holds rows of this path, ``None`` if it is
        absent or a table to adopt (no record and no rows, or a record and no rows).

        Raises:
            SpiralStoreTableCollisionError: If the table is another path's, of another
                format, or holds rows without a manifest.
        """
        found = self._found.get(table_name)
        if found is not None:
            return found
        path = self._path_of(table_name)
        table = self._open_table(table_name)
        if table is None:
            return None
        with self._service_call(f"read the metadata of {table_name}"):
            metadata = table.get_metadata()
        try:
            record = names.read_store_table_metadata(metadata)
            if record is not None:
                names.require_store_table_path(record, path)
            manifest = None if record is None else codec.read_manifest(metadata, path)
        except (SpiralStoreTableCollisionError, codec.ManifestError) as exc:
            raise SpiralStoreTableCollisionError(
                str(exc), table=self._qualified(table_name)
            ) from exc
        if manifest is None:
            self._require_key(table, table_name)
            if self._holds_rows(table):
                raise SpiralStoreTableCollisionError(
                    "it holds rows but no store manifest",
                    table=self._qualified(table_name),
                )
            return None
        assert record is not None
        found = _Found(str(table.resource_id), record, manifest)
        self._found[table_name] = found
        return found

    def has_table(self, table_name: str) -> bool:
        """Whether the store table at a computed name exists (holds a manifest)."""
        return self._judge(table_name) is not None

    def _holds_rows(self, table: _KeyedTable) -> bool:
        """Whether a keyed table has a row."""
        with self._service_call("read a key of a store table"):
            keys = table.scan([codec.KEY_COLUMN], limit=1).to_table()
        return keys.num_rows > 0

    def _require_key(self, table: _KeyedTable, table_name: str) -> None:
        """Refuse a table to adopt whose key is not the store's."""
        if tuple(table.key_columns) != (codec.KEY_COLUMN,):
            raise SpiralStoreTableCollisionError(
                f"its key columns are {list(table.key_columns)}, "
                + f"not [{codec.KEY_COLUMN!r}]",
                table=self._qualified(table_name),
            )

    def _qualified(self, table_name: str) -> str:
        return f"{self._project_id}/{self._dataset}/{table_name}"

    # ── listing ──────────────────────────────────────────────────────────────

    def table_paths(
        self, prefix: tuple[str, ...], pending: Collection[tuple[str, ...]]
    ) -> list[tuple[str, ...]]:
        """Return the full paths of the store tables under ``prefix``, and of the
        pending ones.

        One listing of the dataset; the names that start as a table under ``prefix``
        must; then the record of each candidate, read once per table id. A listed table
        whose record is of a path with another name is refused.
        """
        start = names.store_table_name_prefix(prefix)
        found = {p for p in pending if p[: len(prefix)] == prefix}
        with self._service_call("list the store tables"):
            listed = self._list_tables()
        for table_id, table_name in listed.items():
            if not table_name.startswith(start):
                continue
            record = self._record_of(table_id, table_name)
            if record is not None and record.path[: len(prefix)] == prefix:
                found.add(record.path)
        return sorted(found)

    def _record_of(
        self, table_id: str, table_name: str
    ) -> names.StoreTableRecord | None:
        """The record of a listed table, read once per id; ``None`` without one."""
        if table_id not in self._records_by_id:
            with self._service_call(f"read the metadata of {table_name}"):
                metadata = self._client.keyed_table_by_id(table_id).get_metadata()
            try:
                record = names.read_store_table_metadata(metadata)
            except SpiralStoreTableCollisionError as exc:
                raise SpiralStoreTableCollisionError(
                    str(exc), table=self._qualified(table_name)
                ) from exc
            if record is not None and names.store_table_name(record.path) != table_name:
                raise SpiralStoreTableCollisionError(
                    f"it records path {list(record.path)}, whose table has another name",
                    table=self._qualified(table_name),
                )
            self._records_by_id[table_id] = record
        return self._records_by_id[table_id]

    def table_names(self) -> list[str]:
        """The names of the dataset's tables that start as store tables do, sorted."""
        with self._service_call("list the store tables"):
            listed = self._list_tables()
        prefix = names.STORE_TABLE_NAME_PREFIX
        return sorted(n for n in listed.values() if n.startswith(prefix))

    # ── reading ──────────────────────────────────────────────────────────────

    def _require(self, table_name: str) -> _Found:
        found = self._judge(table_name)
        if found is None:
            raise KeyError(f"no store table {self._qualified(table_name)}")
        return found

    def arrow_schema(self, table_name: str) -> pa.Schema:
        """The schema rows of the table are read with: logical types live."""
        return _live(self._require(table_name).manifest.schema)

    def column_info(self, table_name: str) -> list[ColumnInfo]:
        """The table's columns, as ``arrow_schema`` gives them."""
        return [
            ColumnInfo(name=f.name, arrow_type=f.type, nullable=f.nullable)
            for f in self.arrow_schema(table_name)
        ]

    def scan(self, table_name: str) -> Iterator[pa.RecordBatch]:
        """Every row of the table, decoded; nothing for a table with no rows."""
        found = self._require(table_name)
        schema = _live(found.manifest.schema)
        with self._service_call(f"read {table_name}"):
            stored = self._client.keyed_table_by_id(found.table_id).scan().to_table()
        if stored.num_rows == 0:
            return
        rows = codec.decode_rows(stored, found.manifest, schema)
        yield from rows.to_batches()

    # ── writing ──────────────────────────────────────────────────────────────

    def create(self, table_name: str) -> None:
        """Create the table at a computed name, unless it exists or is to be adopted."""
        if self._judge(table_name) is not None:
            return
        if self._open_table(table_name) is not None:
            return  # to adopt: its first write records it
        key = pa.schema([pa.field(codec.KEY_COLUMN, pa.large_binary(), nullable=False)])
        project = self._client.project(self._project_id)
        try:
            with self._service_call(f"create {table_name}"):
                _ = project.create_keyed_table(self._dataset, table_name, key)
        except sp.SpiralError as exc:
            if "status 409" not in str(exc):
                raise
            # Made meanwhile: judged as any table found under the name.
            _ = self._judge(table_name)

    def write(self, table_name: str, records: pa.Table, skip_existing: bool) -> None:
        """Upsert rows; the first write records the table's identity and manifest.

        Raises:
            ValueError: If the rows hold no ``__data_context`` column, or several data
                contexts, or columns other than the manifest's.
            UnstorableInSpiralError: If the codec refuses the rows.
        """
        path = self._path_of(table_name)
        context = _one_data_context(records)
        found = self._judge(table_name)
        table = self._open_table(table_name)
        if table is None:
            raise KeyError(f"no store table {self._qualified(table_name)}")
        if found is None:
            manifest = codec.new_manifest(path, path[-1], records.schema)
            encoded = _encoded(records)
            metadata = {
                **names.store_table_metadata(path, context),
                **codec.manifest_entries(manifest),
            }
            with self._service_call(f"record {table_name}"):
                table.set_metadata(metadata)
            found = _Found(
                str(table.resource_id),
                names.StoreTableRecord(path, path[-1], context),
                manifest,
            )
            self._found[table_name] = found
        else:
            difference = codec.schema_difference(found.manifest.schema, records.schema)
            if difference is not None:
                raise ValueError(
                    f"the rows are not of the table's schema ({difference}): "
                    + "schema evolution is not supported"
                )
            if skip_existing:
                records = self._without_stored_keys(found, records)
            encoded = _encoded(records)
        if encoded.num_rows == 0:
            return  # a write of no rows is never sent
        with self._service_call(f"write {table_name}"):
            _ = table.append(encoded)

    def _without_stored_keys(self, found: _Found, records: pa.Table) -> pa.Table:
        """The rows whose key the table does not hold yet."""
        with self._service_call("read the keys of a store table"):
            stored = (
                self._client.keyed_table_by_id(found.table_id)
                .scan([codec.KEY_COLUMN])
                .to_table()
            )
        if stored.num_rows == 0:
            return records
        held = stored.column(codec.KEY_COLUMN).cast(pa.large_binary())
        return records.filter(pc.invert(pc.is_in(records[codec.KEY_COLUMN], held)))


def _last_of_each_key(records: pa.Table) -> pa.Table:
    """The last row of each key, in row order (what ``ConnectorArrowDatabase`` keeps)."""
    if codec.KEY_COLUMN not in records.column_names:
        return records  # refused by the codec, naming the key
    keys = records.column(codec.KEY_COLUMN).to_pylist()
    last = {key: i for i, key in enumerate(keys)}
    if len(last) == len(keys):
        return records
    return records.take(pa.array(sorted(last.values()), pa.int64()))


def _encoded(records: pa.Table) -> pa.Table:
    """The rows as written, or an empty table for no rows (never sent)."""
    if records.num_rows == 0:
        return records
    return codec.encode_rows(records)


def _one_data_context(records: pa.Table) -> str:
    """The one data context of rows to store (ITL-1359: one per datagram).

    Raises:
        ValueError: If there is no data context column, a null in it, or two values.
    """
    if DATA_CONTEXT_COLUMN not in records.column_names:
        raise ValueError(
            f"a SpiralDB store keeps rows with a {DATA_CONTEXT_COLUMN!r} column only"
        )
    values = pc.unique(records.column(DATA_CONTEXT_COLUMN)).to_pylist()
    if len(values) != 1 or not isinstance(values[0], str) or not values[0]:
        raise ValueError(
            f"rows of one write to a SpiralDB store hold one data context, not {values}"
        )
    return values[0]


def _live(schema: pa.Schema) -> pa.Schema:
    """A manifest's schema with its logical types live, as the rows were given."""
    from orcapod.contexts import get_default_context

    return declaration_form_schema(schema, get_default_context().type_converter)
