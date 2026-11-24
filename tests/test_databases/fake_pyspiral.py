"""A disk-backed fake of the pyspiral surface that ``SpiralDBConnector`` uses.

The unit tests for ``SpiralDBConnector`` and ``SpiralDBTableSource`` replace pyspiral (or the
whole connector) with ``MagicMock`` objects, so they never push a real Arrow table through the
connector's read path. This fake does: it stores real Arrow tables and returns them the way
SpiralDB does, so a source built on it goes through the same code as one built on a live
project.

Only the calls the connector makes are implemented:

* ``Spiral(overrides=...)``, ``Spiral.project(id)``,
  ``Spiral.scan(expr, where=..., limit=..., asof=...)``, ``Spiral.core.manifest_stats``
  (each call is recorded in ``scan_log``; batches are read on demand)
* ``Project.list_tables()`` (``TableResource`` with ``.dataset`` / ``.table``),
  ``Project.table(id)``, ``Project.create_table(id, key_schema=..., exist_ok=...)``,
  ``Project.drop_table(id)``, ``Spiral.core.drop_table(table_id=...)``
* ``Table.key_schema``, ``Table.table_id``, ``Table.last_modified_at()``, ``Table[name] == v``,
  ``Table.core.get_snapshot()``, ``Table.schema().to_arrow()``, ``Table.select()``, ``Table.write(t)``,
  ``Table.get_metadata()``, ``Table.set_metadata(kv)``, ``Table.drop_metadata(keys)``
* ``Scan.to_record_batches()``, ``Scan.to_table()``

Tables live under a directory (``FAKE_SPIRAL_ROOT`` or the ``root`` given to ``install``), one
Arrow IPC file per table, so a second Python process sees the tables the first one wrote.

Storage behaviour. Each way SpiralDB changes data on the way in or out is a switch in
``StorageBehaviour``; ``observed`` says whether it was seen on a live project (ITL-40, the
connector's docstrings, pyspiral's own documentation) or is an assumption to confirm live:

* ``string_as_utf8`` (observed, ITL-40): strings come back as ``string`` even when written as
  ``large_string``. Applied at every nesting level (assumed for nested).
* ``binary_as``: ``"binary"`` (observed on pyspiral 0.11-0.14, ITL-40) or ``"large_binary"``
  (what pyspiral 0.14.9's ``Scan.to_record_batches(disable_view_types=True)`` documents).
* ``drop_timezone`` (observed): ``timestamp[us, tz=UTC]`` comes back as ``timestamp[us]``.
* ``drop_arrow_metadata`` (observed, ITL-471): schema and field metadata are dropped; the
  table's key-value store (``set_metadata``) round-trips.
* ``decode_dictionaries`` (assumed): dictionary columns come back as their value type.
* ``server_nullability`` (observed, pyspiral 0.16.33, ITL-936): SpiralDB reports every
  top-level column nullable, key columns included, and every struct field nullable, whatever
  was written; only list and fixed-size-list items keep a non-null flag. A null in a
  top-level column declared non-null is refused on write (Arrow validates the outgoing batch
  on the client). A null in a struct field declared non-null is accepted and stored as the
  type's zero value (a null written to an ``int32`` field read back as ``0``). Switch it off
  only to model a database that keeps nullability.
* ``reject_struct_nulls`` (documented by pyspiral ``Table.write``): a null struct value is
  refused unless written with ``push_down_nulls=True``, which turns it into a struct of null
  fields (observed live, ITL-936; top-level struct columns only in the fake).
* Creating a table name is atomic across processes and threads; with ``exist_ok=False`` a taken
  name is a plain ``Exception`` starting ``409 Conflict`` (observed live, ITL-1446: 25 rounds
  of 5 processes, one winner each).
* A table is dropped by its id; an unknown or already dropped id is a plain ``Exception``
  starting ``404 Not Found``, also when a newer table has the name (observed live, ITL-1446: 10
  takeover rounds). Dropping by name looks the id up first (pyspiral 0.16.33's source).
* Rows come back sorted by key and writes upsert by key (SpiralDB is a key-sorted store).
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import types
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pyarrow as pa
import pyarrow.compute as pc

_ROOT_ENV = "FAKE_SPIRAL_ROOT"
_BEHAVIOUR_FILE = "_behaviour.json"
# pyspiral 0.16.33 ``Table.set_metadata`` limits.
_MAX_PAIR_BYTES = 8192
_MAX_KEY_CHARS = 255
_MAX_ENTRIES = 1024


@dataclass
class ScanRecord:
    """One ``Spiral.scan(...)`` call: the table, its keyword arguments, batches handed out."""

    identifier: str
    kwargs: dict[str, Any]
    batches_read: int = 0


# Every scan opened in this process, oldest first. Tests clear it and read it.
scan_log: list[ScanRecord] = []
# Rows per batch a scan hands out (SpiralDB's batches follow its stored fragments).
scan_batch_rows = 2


@dataclass(frozen=True)
class StorageBehaviour:
    """How the fake transforms data, mirroring SpiralDB's storage behaviour."""

    string_as_utf8: bool = True
    binary_as: str = "binary"
    drop_timezone: bool = True
    drop_arrow_metadata: bool = True
    decode_dictionaries: bool = True
    server_nullability: bool = True
    reject_struct_nulls: bool = True


def _check_arrow_type(arrow_type: pa.DataType) -> None:
    """Mirror the unsupported inputs to pyspiral 0.16.33's Schema.from_arrow."""
    if (
        pa.types.is_fixed_size_binary(arrow_type)
        or pa.types.is_duration(arrow_type)
        or pa.types.is_union(arrow_type)
        or pa.types.is_interval(arrow_type)
    ):
        raise ValueError(f"Arrow data type not supported: {arrow_type}")
    if pa.types.is_dictionary(arrow_type):
        _check_arrow_type(arrow_type.value_type)
    for index in range(arrow_type.num_fields):
        _check_arrow_type(arrow_type.field(index).type)


def _stored_type(t: pa.DataType, b: StorageBehaviour) -> pa.DataType:
    """Return the Arrow type SpiralDB hands back for a column written as ``t``.

    Args:
        t: The type that was written.
        b: The storage behaviour to apply.

    Returns:
        The stored type, with the same nesting and field names. Struct fields become
        nullable under ``server_nullability``; list items keep their nullability.
    """
    if isinstance(t, pa.ExtensionType):
        # Vortex stores the storage type; the extension name lives in field metadata,
        # which is dropped.
        return _stored_type(t.storage_type, b)
    if pa.types.is_dictionary(t) and b.decode_dictionaries:
        return _stored_type(t.value_type, b)
    if (pa.types.is_string(t) or pa.types.is_large_string(t)) and b.string_as_utf8:
        return pa.string()
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return pa.binary() if b.binary_as == "binary" else pa.large_binary()
    if pa.types.is_timestamp(t) and t.tz is not None and b.drop_timezone:
        return pa.timestamp(t.unit)
    if pa.types.is_struct(t):
        return pa.struct(
            [
                _stored_field(t.field(i), b, nullable=b.server_nullability or None)
                for i in range(t.num_fields)
            ]
        )
    if pa.types.is_map(t):
        # pyarrow accepts key/item fields here; its stubs list only types.
        return pa.map_(  # pyright: ignore[reportCallIssue]
            _stored_field(t.key_field, b),
            _stored_field(t.item_field, b),
            keys_sorted=t.keys_sorted,
        )
    if pa.types.is_large_list(t):
        return pa.large_list(_stored_field(t.value_field, b))
    if pa.types.is_list(t):
        return pa.list_(_stored_field(t.value_field, b))
    if pa.types.is_fixed_size_list(t):
        return pa.list_(_stored_field(t.value_field, b), t.list_size)
    return t


def _stored_field(
    f: pa.Field[Any], b: StorageBehaviour, nullable: bool | None = None
) -> pa.Field[Any]:
    """Return the field SpiralDB hands back for a field written as ``f``.

    Args:
        f: The field that was written.
        b: The storage behaviour to apply.
        nullable: ``True`` to report the field nullable whatever was written; ``None``
            keeps the written flag.
    """
    metadata = None if b.drop_arrow_metadata else f.metadata
    flag = f.nullable if nullable is None else nullable
    return pa.field(f.name, _stored_type(f.type, b), flag, metadata)


def _zero_struct_field_nulls(
    col: pa.ChunkedArray[Any], written: pa.DataType
) -> pa.ChunkedArray[Any]:
    """Store a null in a struct field declared non-null as the type's zero (seen live).

    Only top-level struct columns with primitive fields are modelled; other nulls are kept.
    """
    if not pa.types.is_struct(written):
        return col
    chunks = []
    for chunk in col.chunks:
        struct = cast("pa.StructArray", chunk)
        children = []
        for i in range(written.num_fields):
            child = struct.field(i)
            field = written.field(i)
            if not field.nullable and child.null_count:
                try:
                    child = pc.fill_null(child, pa.scalar(0).cast(field.type))
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    pass
            children.append(child)
        chunks.append(
            pa.StructArray.from_arrays(
                children,
                fields=[
                    pa.field(written.field(i).name, children[i].type, True)
                    for i in range(written.num_fields)
                ],
                mask=struct.is_null(),
            )
        )
    return pa.chunked_array(chunks)


def _push_down_struct_nulls(col: pa.ChunkedArray[Any]) -> pa.ChunkedArray[Any]:
    """Turn each null struct of a top-level struct column into a struct of null fields.

    What ``push_down_nulls=True`` was seen to do live (pyspiral 0.16.33, ITL-936): the
    struct-level null and any values under it are lost.
    """
    if not pa.types.is_struct(col.type) or not col.null_count:
        return col
    chunks = []
    for chunk in col.chunks:
        struct = cast("pa.StructArray", chunk)
        valid = struct.is_valid()
        children = [
            pc.if_else(
                valid, struct.field(i), pa.scalar(None, struct.type.field(i).type)
            )
            for i in range(struct.type.num_fields)
        ]
        chunks.append(
            pa.StructArray.from_arrays(
                children,
                fields=[
                    struct.type.field(i).with_nullable(True)
                    for i in range(struct.type.num_fields)
                ],
            )
        )
    return pa.chunked_array(chunks)


def _has_struct_nulls(arr: pa.ChunkedArray[Any]) -> bool:
    """Return whether ``arr`` is a struct column with a null struct value."""
    return pa.types.is_struct(arr.type) and arr.null_count > 0


class _Store:
    """Directory-backed table store shared by every fake object."""

    def __init__(self, root: Path) -> None:
        """Open the database under ``root`` and load its storage behaviour."""
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        bfile = self.root / _BEHAVIOUR_FILE
        self.behaviour = (
            StorageBehaviour(**json.loads(bfile.read_text()))
            if bfile.exists()
            else StorageBehaviour()
        )

    def _path(self, project_id: str, identifier: str, suffix: str) -> Path:
        # identifiers contain a dot (``dataset.table``), so never use ``with_suffix``
        """Return the file holding ``identifier``'s part named by ``suffix``."""
        return self.root / project_id / f"{identifier}{suffix}"

    @contextmanager
    def locked(self) -> Iterator[None]:
        """Make the body one step of the service, for every process and thread."""
        with open(self.root / ".lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def exists(self, project_id: str, identifier: str) -> bool:
        """Return whether table ``identifier`` exists in ``project_id``."""
        return self._path(project_id, identifier, ".keys.json").exists()

    def identifiers(self, project_id: str) -> list[str]:
        """Return every table identifier of ``project_id``, sorted."""
        pdir = self.root / project_id
        if not pdir.exists():
            return []
        return sorted(p.name[: -len(".keys.json")] for p in pdir.glob("*.keys.json"))

    def create(self, project_id: str, identifier: str, key_schema: pa.Schema) -> bool:
        """Create table ``identifier`` with ``key_schema`` and empty metadata, atomically.

        Returns:
            Whether the name was free. If not, nothing is changed.
        """
        with self.locked():
            if self.exists(project_id, identifier):
                return False
            self._create(project_id, identifier, key_schema)
            return True

    def _create(self, project_id: str, identifier: str, key_schema: pa.Schema) -> None:
        """Write the files of the new table ``identifier``."""
        (self.root / project_id).mkdir(parents=True, exist_ok=True)
        sink = pa.BufferOutputStream()
        pa.ipc.new_stream(sink, key_schema).close()
        self._path(project_id, identifier, ".keys.arrow").write_bytes(
            sink.getvalue().to_pybytes()
        )
        self._path(project_id, identifier, ".kv.json").write_text("{}")
        # SpiralDB gives every table its own id; a recreated table gets a new one.
        self._path(project_id, identifier, ".id.txt").write_text(
            f"table_{uuid.uuid4().hex[:12]}"
        )
        self._path(project_id, identifier, ".version.txt").write_text("0")
        # Last: the table exists once this file does.
        self._path(project_id, identifier, ".keys.json").write_text(
            json.dumps(key_schema.names)
        )

    def drop_id(self, table_id: str) -> None:
        """Remove the table with the id ``table_id``, whatever its name.

        Raises:
            Exception: If no table has that id (a 404, as pyspiral raises it).
        """
        with self.locked():
            for project in self.root.iterdir():
                for identifier in (
                    self.identifiers(project.name) if project.is_dir() else ()
                ):
                    if self.table_id(project.name, identifier) == table_id:
                        self.drop(project.name, identifier)
                        return
        raise Exception(f"404 Not Found: Resource `{table_id}` not found")

    def drop(self, project_id: str, identifier: str) -> None:
        """Remove every file of table ``identifier``."""
        for suffix in (
            ".keys.json",
            ".keys.arrow",
            ".kv.json",
            ".data.arrow",
            ".id.txt",
            ".version.txt",
        ):
            p = self._path(project_id, identifier, suffix)
            if p.exists():
                p.unlink()

    def key_schema(self, project_id: str, identifier: str) -> pa.Schema:
        """Return table ``identifier``'s key schema."""
        raw = self._path(project_id, identifier, ".keys.arrow").read_bytes()
        return pa.ipc.open_stream(raw).schema

    def read(self, project_id: str, identifier: str) -> pa.Table | None:
        """Return table ``identifier``'s rows, or ``None`` before the first write."""
        p = self._path(project_id, identifier, ".data.arrow")
        if not p.exists():
            return None
        with pa.ipc.open_file(pa.memory_map(str(p))) as r:
            return r.read_all()

    def write(self, project_id: str, identifier: str, table: pa.Table) -> None:
        """Replace table ``identifier``'s rows with ``table`` and move its version on."""
        p = self._path(project_id, identifier, ".data.arrow")
        part = p.with_name(f"{p.name}.{uuid.uuid4().hex}")
        with pa.ipc.new_file(str(part), table.schema) as w:
            w.write_table(table)
        os.replace(part, p)  # a reader sees the old rows or the new ones
        self._path(project_id, identifier, ".version.txt").write_text(
            str(self.version(project_id, identifier) + 1)
        )

    def table_id(self, project_id: str, identifier: str) -> str:
        """Return table ``identifier``'s own id."""
        return self._path(project_id, identifier, ".id.txt").read_text()

    def version(self, project_id: str, identifier: str) -> int:
        """Return table ``identifier``'s version: ``0`` before the first write."""
        return int(self._path(project_id, identifier, ".version.txt").read_text())

    def kv(self, project_id: str, identifier: str) -> dict[str, bytes]:
        """Return table ``identifier``'s key-value metadata."""
        raw = json.loads(self._path(project_id, identifier, ".kv.json").read_text())
        return {k: bytes.fromhex(v) for k, v in raw.items()}

    def set_kv(self, project_id: str, identifier: str, kv: dict[str, bytes]) -> None:
        """Upsert ``kv`` into table ``identifier``'s key-value metadata."""
        current = self.kv(project_id, identifier)
        current.update(kv)
        self._path(project_id, identifier, ".kv.json").write_text(
            json.dumps({k: v.hex() for k, v in current.items()})
        )

    def drop_kv(self, project_id: str, identifier: str, keys: list[str]) -> None:
        """Remove ``keys`` from table ``identifier``'s key-value metadata."""
        current = self.kv(project_id, identifier)
        for key in keys:
            current.pop(key, None)
        self._path(project_id, identifier, ".kv.json").write_text(
            json.dumps({k: v.hex() for k, v in current.items()})
        )


@dataclass(frozen=True)
class TableResource:
    """What ``Project.list_tables()`` returns per table."""

    id: str
    project_id: str
    dataset: str
    table: str


class FakeSchema:
    """What ``Table.schema()`` returns: a wrapper whose ``to_arrow()`` gives the schema."""

    def __init__(self, schema: pa.Schema) -> None:
        """Wrap ``schema``."""
        self._schema = schema

    def to_arrow(self) -> pa.Schema:
        """Return the stored Arrow schema."""
        return self._schema


class FakeExpr:
    """The result of ``Table.select()``: names the table to scan."""

    def __init__(self, table: FakeTable) -> None:
        """Name ``table`` as the table to scan."""
        self.table = table


class FakePredicate:
    """A ``where`` expression: every named column equals its value."""

    def __init__(self, equalities: list[tuple[str, Any]]) -> None:
        """Hold the ``(column, value)`` pairs a row must all match."""
        self.equalities = equalities

    def __and__(self, other: FakePredicate) -> FakePredicate:
        """Return a predicate requiring this one's equalities and ``other``'s."""
        return FakePredicate(self.equalities + other.equalities)

    def __repr__(self) -> str:
        """Return the equalities as ``name == value`` terms joined by ``&``."""
        return " & ".join(f"{name} == {value!r}" for name, value in self.equalities)


class FakeColumn:
    """What ``table[name]`` returns: compare it with ``==`` to get a predicate."""

    def __init__(self, name: str) -> None:
        """Name the column ``name``."""
        self._name = name

    def __eq__(self, value: object) -> FakePredicate:  # pyright: ignore[reportIncompatibleMethodOverride]  # as pyspiral's Expr does
        """Return the predicate that this column equals ``value``."""
        return FakePredicate([(self._name, value)])

    __hash__ = None  # pyright: ignore[reportAssignmentType]  # an expression, not a value


class FakeScan:
    """What ``Spiral.scan(table.select(), where=..., limit=...)`` returns."""

    def __init__(
        self,
        table: FakeTable,
        record: ScanRecord,
        where: FakePredicate | None,
        limit: int | None,
    ) -> None:
        """Prepare a scan of ``table``; no row is read until rows are asked for.

        Args:
            table: The table to scan.
            record: The ``scan_log`` entry that counts the batches handed out.
            where: The equalities a row must match, or ``None`` for every row.
            limit: The most rows to return, or ``None`` for no limit.
        """
        self._table = table
        self._record = record
        self._where = where
        self._limit = limit

    def _rows(self) -> pa.Table:
        """Return the rows matching ``where``, cut to ``limit``, in key order."""
        rows = self._table._read_all()
        for name, value in self._where.equalities if self._where else []:
            rows = rows.filter(
                pc.equal(
                    rows.column(name), pa.scalar(value, rows.schema.field(name).type)
                )
            )
        return rows if self._limit is None else rows.slice(0, self._limit)

    def to_table(self) -> pa.Table:
        """Return the scanned rows, in key order."""
        return self._rows()

    def to_record_batches(self, **kwargs: Any) -> pa.RecordBatchReader:
        """Return the rows in key order, ``scan_batch_rows`` per batch, read on demand."""
        rows = self._rows()

        def batches() -> Iterator[pa.RecordBatch]:
            # SpiralDB yields more than one batch for larger tables; split to exercise that.
            """Yield ``rows`` in batches, counting each in the scan record."""
            for batch in rows.to_batches(max_chunksize=scan_batch_rows):
                self._record.batches_read += 1
                yield batch

        return pa.RecordBatchReader.from_batches(rows.schema, batches())


class FakeTable:
    """A stored table: key schema, Arrow schema, key-value metadata and upserting writes.

    Args:
        store: The directory-backed store.
        project_id: The project the table belongs to.
        identifier: ``"<dataset>.<table>"``.

    Raises:
        ValueError: If the table does not exist.
    """

    def __init__(self, store: _Store, project_id: str, identifier: str) -> None:
        """Open table ``identifier`` of ``project_id`` in ``store``."""
        if not store.exists(project_id, identifier):
            raise ValueError(f"Table not found: {project_id}.{identifier}")
        self._store = store
        self._project_id = project_id
        self._identifier = identifier
        self._table_id = store.table_id(project_id, identifier)

    @property
    def key_schema(self) -> pa.Schema:
        """The key columns, in key order."""
        return self._store.key_schema(self._project_id, self._identifier)

    @property
    def table_id(self) -> str:
        """The table's own id: kept on rename, new when the table is recreated."""
        return self._table_id

    @property
    def core(self) -> Any:
        """The table's low-level handle; only ``get_snapshot`` is modelled."""
        return types.SimpleNamespace(get_snapshot=lambda asof=None: self)

    def last_modified_at(self) -> int:
        """Return the table's version: ``0`` before the first write."""
        return self._store.version(self._project_id, self._identifier)

    def __getitem__(self, name: str) -> FakeColumn:
        """Return the column expression ``table[name]``."""
        return FakeColumn(name)

    def schema(self) -> FakeSchema:
        """Return the stored schema (only the key columns before the first write)."""
        stored = self._store.read(self._project_id, self._identifier)
        keys = self.key_schema
        if stored is None:
            return FakeSchema(keys)
        return FakeSchema(stored.schema)

    def select(self, *paths: str, exclude: list[str] | None = None) -> FakeExpr:
        """Return an expression selecting the whole table (projections are not modelled)."""
        if paths or exclude:
            raise NotImplementedError("fake Table.select supports only select()")
        return FakeExpr(self)

    def get_metadata(self) -> dict[str, bytes]:
        """Return the table's key-value metadata."""
        return self._store.kv(self._project_id, self._identifier)

    def set_metadata(self, metadata: dict[str, bytes]) -> None:
        """Upsert ``metadata`` into the table's key-value metadata.

        Enforces the limits pyspiral 0.16.33 documents for ``Table.set_metadata``: str keys
        and bytes values, a key + value pair of at most 8 KiB, keys of at most 255
        characters, and at most 1024 entries per table.

        Raises:
            TypeError: If a key is not ``str`` or a value not ``bytes``.
            ValueError: If a limit is exceeded.
        """
        for k, v in metadata.items():
            if not isinstance(k, str) or not isinstance(v, bytes):
                raise TypeError("metadata keys must be str and values bytes")
            if len(k) > _MAX_KEY_CHARS:
                raise ValueError(f"metadata key {k[:40]!r}... exceeds 255 characters")
            if len(k.encode()) + len(v) > _MAX_PAIR_BYTES:
                raise ValueError(f"metadata entry {k!r} exceeds 8 KiB")
        current = self._store.kv(self._project_id, self._identifier)
        if len({*current, *metadata}) > _MAX_ENTRIES:
            raise ValueError("a table can have at most 1024 metadata entries")
        self._store.set_kv(self._project_id, self._identifier, metadata)

    def drop_metadata(self, keys: list[str]) -> None:
        """Remove ``keys`` from the table's key-value metadata; absent keys are ignored."""
        self._store.drop_kv(self._project_id, self._identifier, keys)

    def write(
        self, table: pa.Table, push_down_nulls: bool = False, **kwargs: Any
    ) -> None:
        """Store ``table`` the way SpiralDB does: transform types, then upsert by key.

        Args:
            table: The rows to write; every key column must be present.
            push_down_nulls: Accept null struct values (SpiralDB's own flag).
            **kwargs: Accepted for signature compatibility; unused.

        Raises:
            ValueError: If key columns are missing, the write has no value columns
                or a batch repeats a key, a column uses an unsupported name or type,
                a struct value is null without ``push_down_nulls``, or the column set
                differs from the stored table (live SpiralDB adds a new column: the
                fake does not model it).
        """
        b = self._store.behaviour
        keys = self.key_schema
        missing = [k for k in keys.names if k not in table.column_names]
        if missing:
            raise ValueError(f"write is missing key columns {missing}")
        values = sorted(name for name in table.column_names if name not in keys.names)
        if not values:
            # pyspiral 0.16.33 native writer diagnostic.
            raise ValueError(
                "Write array contains only key columns, no data columns to write"
            )
        if keys.names:
            for batch in table.select(keys.names).to_batches():
                # pyspiral 0.16.33 native writer diagnostic names the input batch.
                batch_keys = set(zip(*batch.columns))
                if len(batch_keys) != batch.num_rows:
                    raise ValueError("Duplicate keys found in input batch")
        for name in table.column_names:
            # SpiralDB's own refusals, with its messages as observed live (ITL-1164).
            if "." in name:
                raise ValueError(f"Column '{name}' contains a reserved dot-separator")
            if name.startswith("__") and name not in keys.names:
                raise ValueError(
                    f"Column name '{name}' has reserved prefix: column group columns "
                    "must not start with '__'"
                )
            if name.startswith("$") and name not in keys.names:
                # pyspiral 0.16.33 native writer diagnostic in writer/extract.rs.
                raise ValueError(
                    f"Field name {name!r} starts with '$', which is reserved for key columns."
                )
        for field in table.schema:
            _check_arrow_type(field.type)
        if b.reject_struct_nulls and not push_down_nulls:
            for name in table.column_names:
                if _has_struct_nulls(table.column(name)):
                    raise ValueError(
                        f"SpiralDB does not allow struct-level nulls (column {name!r}); "
                        "write with push_down_nulls=True"
                    )
        fields = []
        arrays = []
        for f in table.schema:
            col = table.column(f.name)
            if push_down_nulls:
                col = _push_down_struct_nulls(col)
            if b.server_nullability:
                # Arrow validates the outgoing batch on the client (live, ITL-936).
                if not f.nullable and col.null_count:
                    raise ValueError(
                        f"Column {f.name!r} is declared as non-nullable but contains "
                        "null values"
                    )
                col = _zero_struct_field_nulls(col, f.type)
            sf = _stored_field(f, b, nullable=b.server_nullability or None)
            if isinstance(f.type, pa.ExtensionType):
                col = pa.chunked_array(
                    [cast("pa.ExtensionArray[Any]", c).storage for c in col.chunks]
                )
            if pa.types.is_dictionary(col.type) and b.decode_dictionaries:
                col = col.cast(col.type.value_type)
            fields.append(sf)
            arrays.append(_cast_storage(col, sf.type))
        incoming = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
        # SpiralDB matches a write's columns by name and stores them in its own
        # order, whatever the order written: the key columns in key-schema order,
        # then the value columns sorted by name, by code point (observed live,
        # ITL-1367). Struct children are sorted live too; the fake keeps them as
        # written (struct field order: ITL-1368).
        incoming = incoming.select([*keys.names, *values])
        with self._store.locked():
            self._commit(incoming)

    def _commit(self, incoming: pa.Table) -> None:
        """Upsert ``incoming`` by key: one atomic step (observed live, ITL-1446)."""
        keys = self.key_schema
        existing = self._store.read(self._project_id, self._identifier)
        if existing is not None:
            if existing.schema.names != incoming.schema.names:
                raise ValueError(
                    "fake SpiralDB: column set differs from existing table"
                )
            # Column by column, only where the type changes: pyarrow refuses to cast a
            # fixed-size list with non-null items to its own type when a null row
            # hides null item slots (ITL-1334).
            incoming = pa.Table.from_arrays(
                [
                    column if column.type == field.type else column.cast(field.type)
                    for column, field in zip(incoming.columns, existing.schema)
                ],
                schema=existing.schema,
            )
            if not keys.names:
                incoming = pa.concat_tables([existing, incoming])
            else:
                incoming = _upsert(existing, incoming, keys.names)
        if keys.names:
            incoming = incoming.sort_by([(k, "ascending") for k in keys.names])
        self._store.write(self._project_id, self._identifier, incoming.combine_chunks())

    def _read_all(self) -> pa.Table:
        """Return every stored row (an empty key-only table before the first write)."""
        stored = self._store.read(self._project_id, self._identifier)
        if stored is None:
            keys = self.key_schema
            return keys.empty_table()
        return stored


def _upsert(existing: pa.Table, incoming: pa.Table, key_names: list[str]) -> pa.Table:
    """Replace existing rows whose key appears in ``incoming`` and append the rest."""
    new_keys = set(zip(*(incoming.column(k).to_pylist() for k in key_names)))
    keep = pa.array(
        [
            k not in new_keys
            for k in zip(*(existing.column(k).to_pylist() for k in key_names))
        ],
        type=pa.bool_(),
    )
    return pa.concat_tables([existing.filter(keep), incoming])


def _cast_storage(
    col: pa.ChunkedArray[Any], target: pa.DataType
) -> pa.ChunkedArray[Any]:
    """Cast ``col`` to the stored type ``target``, keeping the UTC instant of a timestamp."""
    if col.type == target:
        return col
    if pa.types.is_timestamp(col.type) and pa.types.is_timestamp(target):
        # dropping the tz keeps the UTC instant (what SpiralDB was seen to do)
        return pc.cast(col, pa.timestamp(col.type.unit, tz="UTC")).cast(target)
    if pa.types.is_map(col.type) and pa.types.is_map(target):
        # pyarrow 23 aborts the process (not an exception) casting some map arrays
        # ("Map array keys array should have no nulls"), so a map is rebuilt from its
        # parts: offsets, keys and items cast on their own, validity kept (ITL-1334).
        # (cast: pyarrow's stubs type ``chunked_array``'s ``type`` too narrowly.)
        return pa.chunked_array(
            [_cast_map_chunk(chunk, target) for chunk in col.chunks],
            type=cast(Any, target),
        )
    return col.cast(target)


def _cast_map_chunk(chunk: Any, target: Any) -> Any:
    """Return the map array ``chunk`` with its keys and items cast to ``target``'s."""
    keys = _cast_storage(pa.chunked_array([chunk.keys]), target.key_type)
    items = _cast_storage(pa.chunked_array([chunk.items]), target.item_type)
    return pa.MapArray.from_arrays(
        chunk.offsets,
        keys.combine_chunks(),
        items.combine_chunks(),
        type=target,
        mask=chunk.is_null() if chunk.null_count else None,
    )


class FakeProject:
    """What ``Spiral.project(id)`` returns: list, open, create and drop tables."""

    def __init__(self, store: _Store, project_id: str) -> None:
        """Bind project ``project_id`` in ``store``."""
        self._store = store
        self._project_id = project_id

    def list_tables(self) -> list[TableResource]:
        """Return every table of the project, in every dataset."""
        return self._list_tables()

    def _list_tables(
        self, dataset: str | None = None, table: str | None = None
    ) -> list[TableResource]:
        """Apply the SDK's project API filters, which omit empty values."""
        with self._store.locked():
            return self._listed(dataset, table)

    def _listed(self, dataset: str | None, table: str | None) -> list[TableResource]:
        """Return the tables the filters leave."""
        out = []
        for ident in self._store.identifiers(self._project_id):
            found_dataset, _, found_table = ident.partition(".")
            if (dataset and dataset != found_dataset) or (
                table and table != found_table
            ):
                continue
            table_id = self._store.table_id(self._project_id, ident)
            out.append(
                TableResource(table_id, self._project_id, found_dataset, found_table)
            )
        return out

    def table(self, identifier: str) -> FakeTable:
        """Parse and resolve a name as pyspiral does, including omitted filters."""
        parts = identifier.split(".")
        if len(parts) == 1:
            dataset, table = "default", parts[0]
        elif len(parts) == 2:
            dataset, table = parts
        else:
            raise ValueError(
                f"Invalid table identifier: {self._project_id}.{identifier}"
            )
        resources = self._list_tables(dataset, table)
        if not resources:
            raise ValueError(f"Table not found: {self._project_id}.{dataset}.{table}")
        resolved = resources[0]
        return FakeTable(
            self._store, self._project_id, f"{resolved.dataset}.{resolved.table}"
        )

    def create_table(
        self,
        identifier: str,
        *,
        key_schema: Any,
        root_uri: Any = None,
        exist_ok: bool = False,
    ) -> FakeTable:
        """Create a table with ``key_schema`` and open it.

        Key columns are stored the way SpiralDB stores them, nullable under
        ``server_nullability`` however they were declared.

        Args:
            identifier: ``"<dataset>.<table>"`` or a name in the default dataset.
            key_schema: An Arrow schema, or ``(name, type)`` pairs as pyspiral accepts.
            root_uri: Accepted for signature compatibility; unused.
            exist_ok: Open an existing table instead of raising.

        Returns:
            The new table, or with ``exist_ok`` the existing one.

        Raises:
            ValueError: If the name or key schema is invalid.
            Exception: If the table exists and ``exist_ok`` is false (a 409).
        """
        parts = identifier.split(".")
        if len(parts) == 1:
            identifier = f"default.{identifier}"
        # The pyspiral 0.16.33 spiral.api.types.TableName annotation on create_table.
        name = parts[-1]
        if (
            len(parts) > 2
            or len(name) > 128
            or re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_-]*", name) is None
        ):
            raise ValueError(f"Invalid table name: {identifier!r}")
        if isinstance(key_schema, pa.Schema):
            schema = key_schema
        else:
            schema = pa.schema([pa.field(n, t, True) for n, t in key_schema])
        for field in schema:
            _check_arrow_type(field.type)
            if pa.types.is_null(field.type):
                # pyspiral 0.16.33 native key schema diagnostic.
                raise ValueError(
                    f"Key schema cannot contain Null type field: {field.name}"
                )
        b = self._store.behaviour
        # SpiralDB reports key columns nullable however they were declared (ITL-936).
        schema = pa.schema(
            [
                pa.field(
                    f.name,
                    _stored_type(f.type, b),
                    True if b.server_nullability else f.nullable,
                )
                for f in schema
            ]
        )
        created = self._store.create(self._project_id, identifier, schema)
        if not created and not exist_ok:
            raise Exception(
                f"409 Conflict: Table {self._project_id}.{identifier} already exists."
            )
        return self.table(identifier)

    def drop_table(self, identifier: str) -> None:
        """Look the table ``"<dataset>.<table>"`` up and drop its id."""
        self._store.drop_id(self.table(identifier).table_id)


class FakeSpiral:
    """Stands in for ``spiral.Spiral``; tables live under ``FAKE_SPIRAL_ROOT``.

    Args:
        config: Accepted for signature compatibility; unused.
        overrides: Recorded; unused.

    Raises:
        RuntimeError: If ``FAKE_SPIRAL_ROOT`` is not set (use ``install``).
    """

    def __init__(
        self, *, config: Any = None, overrides: dict[str, str] | None = None
    ) -> None:
        """Open the database under ``FAKE_SPIRAL_ROOT`` and record ``overrides``."""
        root = os.environ.get(_ROOT_ENV)
        if not root:
            raise RuntimeError(f"{_ROOT_ENV} is not set; call fake_pyspiral.install()")
        self._store = _Store(Path(root))
        self.overrides = overrides

    def project(self, project_id: str) -> FakeProject:
        """Return project ``project_id`` (created on first use)."""
        return FakeProject(self._store, project_id)

    def scan(self, *projections: Any, **kwargs: Any) -> FakeScan:
        """Return a scan of one ``table.select()``, restricted by ``where`` and ``limit``.

        Every call is appended to ``scan_log``. ``asof`` is accepted and recorded; the
        fake keeps one version of a table. Other projections and ``key_ranges`` are
        not modelled.
        """
        if len(projections) != 1 or not isinstance(projections[0], FakeExpr):
            raise NotImplementedError(
                "fake Spiral.scan supports only scan(table.select())"
            )
        unsupported = {k for k, v in kwargs.items() if v is not None} - {
            "where",
            "limit",
            "asof",
        }
        if unsupported:
            raise NotImplementedError(f"fake Spiral.scan: {sorted(unsupported)}")
        table = projections[0].table
        record = ScanRecord(table._identifier, dict(kwargs))
        scan_log.append(record)
        return FakeScan(table, record, kwargs.get("where"), kwargs.get("limit"))

    @property
    def core(self) -> Any:
        """The client's low-level handle; ``manifest_stats`` and ``drop_table`` only."""

        return types.SimpleNamespace(
            manifest_stats=self.manifest_stats, drop_table=self.drop_table
        )

    def drop_table(self, table_id: str) -> None:
        """Drop the table with exactly this id; a 404 if there is none."""
        self._store.drop_id(table_id)

    def manifest_stats(self, snapshot: FakeTable) -> Any:
        """Return stats whose ``key_space_stats.total_rows`` counts rows."""
        rows = snapshot._read_all().num_rows
        return types.SimpleNamespace(
            key_space_stats=types.SimpleNamespace(total_rows=rows)
        )


fake_spiral_module = types.SimpleNamespace(Spiral=FakeSpiral)


def write_behaviour(root: Path, behaviour: StorageBehaviour) -> None:
    """Persist ``behaviour`` so every process using ``root`` applies it."""
    root.mkdir(parents=True, exist_ok=True)
    (root / _BEHAVIOUR_FILE).write_text(json.dumps(asdict(behaviour)))


@contextmanager
def install(root: Path | str):
    """Route ``SpiralDBConnector`` to the fake, with tables stored under ``root``."""
    with (
        patch.dict(os.environ, {_ROOT_ENV: str(root)}),
        patch("orcapod.databases.spiraldb_connector.sp", fake_spiral_module),
    ):
        yield
