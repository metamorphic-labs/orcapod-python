"""An in-memory fake of the pyspiral surface ``edi_tutorial_dataset.py`` uses.

Modelled on pyspiral 0.16.33 and on what was seen live (ITL-926, ITL-933):

* ``Spiral.project``, ``list_projects``, ``create_project(id_prefix=, description=,
  volume_id=)``, ``scan(table_or_select, where=)``.
* ``Project.list_tables`` (``.dataset``, ``.table``), ``table`` (``ValueError`` when
  missing), ``create_table(identifier, key_schema=)``, ``drop_table``.
* ``Table[...]`` and ``Table.select(...)`` build expressions; comparisons and ``&``
  combine them. A comparison literal must carry the column's exact Arrow type, as live
  SpiralDB refuses ``u64`` against ``i64`` ("Cannot compare different DTypes").
* ``Table.key_schema.to_arrow()``, ``Table.schema().to_arrow()`` (the key schema until
  data is written), ``get_metadata`` / ``set_metadata`` (upsert), ``txn()`` with
  ``write``, ``commit``, ``abort`` and the context-manager protocol.
* Writes upsert by key and keep rows sorted by key. The first write fixes the table's
  schema; a later write with a different schema raises. A struct-level null raises,
  as SpiralDB refuses one. ``enforce_non_null`` makes a null in a non-nullable column
  raise (what SpiralDB does is checked live by the tool's ``probe`` command).

Every mutating call is appended to ``Spiral.log`` as ``(project_id, operation, name)``.
"""

from __future__ import annotations

import dataclasses
import itertools
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc


class Expr:
    """A filter expression over one table's columns."""

    def evaluate(self, table: pa.Table) -> Any:  # pragma: no cover
        raise NotImplementedError

    def _compare(self, other: Any, op: str) -> Expr:
        return Compare(self, other, op)

    def __ge__(self, other: Any) -> Expr:
        return self._compare(other, "greater_equal")

    def __gt__(self, other: Any) -> Expr:
        return self._compare(other, "greater")

    def __le__(self, other: Any) -> Expr:
        return self._compare(other, "less_equal")

    def __lt__(self, other: Any) -> Expr:
        return self._compare(other, "less")

    def __and__(self, other: Expr) -> Expr:
        return And(self, other)


@dataclasses.dataclass(eq=False)
class Column(Expr):
    name: str

    def evaluate(self, table: pa.Table) -> Any:
        return table.column(self.name)


@dataclasses.dataclass(eq=False)
class Compare(Expr):
    left: Expr
    right: Any
    op: str

    def evaluate(self, table: pa.Table) -> Any:
        col = self.left.evaluate(table)
        if not isinstance(self.right, pa.Scalar) or self.right.type != col.type:
            got = (
                self.right.type
                if isinstance(self.right, pa.Scalar)
                else type(self.right)
            )
            raise ValueError(f"Cannot compare different DTypes {col.type} and {got}")
        return getattr(pc, self.op)(col, self.right)


@dataclasses.dataclass(eq=False)
class And(Expr):
    left: Expr
    right: Expr

    def evaluate(self, table: pa.Table) -> Any:
        return pc.and_(self.left.evaluate(table), self.right.evaluate(table))


@dataclasses.dataclass(eq=False)
class Select:
    table: FakeTable
    columns: tuple[str, ...]


class _Schema:
    def __init__(self, schema: pa.Schema) -> None:
        self._schema = schema

    def to_arrow(self) -> pa.Schema:
        return self._schema

    @property
    def names(self) -> list[str]:
        return self._schema.names


@dataclasses.dataclass(frozen=True)
class TableResource:
    project_id: str
    dataset: str
    table: str


@dataclasses.dataclass
class _Stored:
    key_schema: pa.Schema
    data: pa.Table | None = None
    metadata: dict[str, bytes] = dataclasses.field(default_factory=dict)


class FakeTxn:
    def __init__(self, table: FakeTable) -> None:
        self._table = table
        self._batches: list[pa.RecordBatch] = []
        self._done = False

    def __enter__(self) -> FakeTxn:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.abort()

    def write(self, batch: pa.RecordBatch | pa.Table) -> None:
        self._table._check_write(batch)
        if isinstance(batch, pa.Table):
            self._batches.extend(batch.to_batches())
        else:
            self._batches.append(batch)

    def commit(self) -> None:
        if not self._done:
            self._done = True
            if self._batches:
                self._table._commit(pa.Table.from_batches(self._batches))

    def abort(self) -> None:
        self._done = True
        self._batches = []


class FakeTable:
    def __init__(self, spiral: FakeSpiral, project_id: str, ident: str) -> None:
        self._spiral = spiral
        self._project_id = project_id
        self._ident = ident

    @property
    def _stored(self) -> _Stored:
        return self._spiral._projects[self._project_id][self._ident]

    def __getitem__(self, name: str) -> Column:
        return Column(name)

    def select(self, *columns: str) -> Select:
        return Select(self, columns)

    @property
    def key_schema(self) -> _Schema:
        return _Schema(self._stored.key_schema)

    def schema(self) -> _Schema:
        data = self._stored.data
        return _Schema(self._stored.key_schema if data is None else data.schema)

    def get_metadata(self) -> dict[str, bytes]:
        return dict(self._stored.metadata)

    def set_metadata(self, metadata: dict[str, bytes]) -> None:
        self._spiral.log.append((self._project_id, "set_metadata", self._ident))
        self._stored.metadata.update(metadata)

    def txn(self) -> FakeTxn:
        return FakeTxn(self)

    def _check_write(self, batch: pa.RecordBatch | pa.Table) -> None:
        self._spiral.log.append((self._project_id, "write", self._ident))
        keys = self._stored.key_schema
        for k in keys:
            if k.name not in batch.schema.names:
                raise ValueError(f"write is missing key column {k.name!r}")
            if batch.schema.field(k.name) != k:
                raise ValueError(
                    f"key column {batch.schema.field(k.name)} differs from key schema {k}"
                )
        data = self._stored.data
        if data is not None and batch.schema != data.schema:
            raise ValueError(f"write schema {batch.schema} differs from {data.schema}")
        for i, field in enumerate(batch.schema):
            col = batch.column(i)
            if pa.types.is_struct(field.type) and col.null_count:
                raise ValueError("SpiralDB does not allow struct-level nulls")
            if self._spiral.enforce_non_null and not field.nullable and col.null_count:
                raise ValueError(f"null in non-nullable column {field.name!r}")

    def _commit(self, incoming: pa.Table) -> None:
        stored = self._stored
        keys = stored.key_schema.names
        if stored.data is not None:
            new_keys = set(
                zip(*(incoming.column(k).to_pylist() for k in keys), strict=True)
            )
            keep = pa.array(
                [
                    k not in new_keys
                    for k in zip(
                        *(stored.data.column(k).to_pylist() for k in keys), strict=True
                    )
                ],
                pa.bool_(),
            )
            incoming = pa.concat_tables([stored.data.filter(keep), incoming])
        stored.data = incoming.sort_by(
            [(k, "ascending") for k in keys]
        ).combine_chunks()


class FakeScan:
    def __init__(self, table: pa.Table, batch_rows: int) -> None:
        self._table = table
        self._batch_rows = batch_rows

    def to_record_batches(self) -> pa.RecordBatchReader:
        batches = self._table.to_batches(max_chunksize=self._batch_rows)
        return pa.RecordBatchReader.from_batches(self._table.schema, batches)

    def to_table(self) -> pa.Table:
        return self._table


@dataclasses.dataclass
class ProjectInfo:
    id: str
    description: str = ""


class FakeProject:
    def __init__(self, spiral: FakeSpiral, project_id: str) -> None:
        self._spiral = spiral
        self.id = project_id

    @property
    def _tables(self) -> dict[str, _Stored]:
        return self._spiral._projects.setdefault(self.id, {})

    def list_tables(self) -> list[TableResource]:
        return [
            TableResource(self.id, *ident.split(".", 1))
            for ident in sorted(self._tables)
        ]

    def table(self, identifier: str) -> FakeTable:
        if identifier not in self._tables:
            raise ValueError(f"Table not found: {self.id}.{identifier}")
        return FakeTable(self._spiral, self.id, identifier)

    def create_table(self, identifier: str, *, key_schema: pa.Schema) -> FakeTable:
        self._spiral.log.append((self.id, "create_table", identifier))
        if identifier in self._tables:
            raise ValueError(f"Table already exists: {identifier}")
        self._tables[identifier] = _Stored(key_schema)
        return FakeTable(self._spiral, self.id, identifier)

    def drop_table(self, identifier: str) -> None:
        self._spiral.log.append((self.id, "drop_table", identifier))
        del self._tables[identifier]


class FakeSpiral:
    """The client. ``batch_rows`` sets how many rows each scanned batch holds."""

    def __init__(self, *, batch_rows: int = 3, enforce_non_null: bool = False) -> None:
        self._projects: dict[str, dict[str, _Stored]] = {}
        self._descriptions: dict[str, str] = {}
        self._suffix = itertools.count(100001)
        self.batch_rows = batch_rows
        self.enforce_non_null = enforce_non_null
        self.log: list[tuple[str, str, str]] = []

    # -- setup helpers for tests (not part of pyspiral) --------------------------
    def add_table(
        self, project_id: str, identifier: str, table: pa.Table, keys: list[str]
    ) -> None:
        """Store ``table`` as-is (setup only; not logged)."""
        projects = self._projects.setdefault(project_id, {})
        key_schema = pa.schema([table.schema.field(k) for k in keys])
        projects[identifier] = _Stored(
            key_schema, table.sort_by([(k, "ascending") for k in keys])
        )
        self._descriptions.setdefault(project_id, "")

    def rows(self, project_id: str, identifier: str) -> pa.Table:
        data = self._projects[project_id][identifier].data
        assert data is not None
        return data

    # -- pyspiral surface ---------------------------------------------------------
    def project(self, project_id: str) -> FakeProject:
        return FakeProject(self, project_id)

    def list_projects(self) -> list[ProjectInfo]:
        return [
            ProjectInfo(p, self._descriptions.get(p, ""))
            for p in sorted(self._projects)
        ]

    def create_project(
        self, *, id_prefix: str, description: str, volume_id: str | None = None
    ) -> ProjectInfo:
        pid = f"{id_prefix}-{next(self._suffix)}"
        self.log.append((pid, "create_project", volume_id or ""))
        self._projects[pid] = {}
        self._descriptions[pid] = description
        return ProjectInfo(pid, description)

    def scan(
        self, projection: FakeTable | Select, *, where: Expr | None = None
    ) -> FakeScan:
        table = projection.table if isinstance(projection, Select) else projection
        data = table._stored.data
        if data is None:
            data = table._stored.key_schema.empty_table()
        if isinstance(projection, Select):
            keys = set(table._stored.key_schema.names)
            if set(projection.columns) <= keys:
                raise ValueError(
                    "Reading key columns without including at least one column from "
                    "any column group is currently not supported"
                )
            data = data.select(list(projection.columns))
        if where is not None:
            data = data.filter(where.evaluate(data))
        return FakeScan(data, self.batch_rows)


def iter_rows(table: pa.Table) -> Iterator[dict[str, Any]]:
    """Yield the rows of ``table`` as dictionaries."""
    yield from table.to_pylist()
