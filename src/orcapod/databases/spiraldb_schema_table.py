"""The schema table of a SpiralDB dataset (ITL-1186).

SpiralDB reports every top-level column and every struct field nullable and keeps no
Arrow metadata, and spiral-sdk reads no key-value metadata of a table. So the schemas of a
dataset's tables are kept in one keyed table beside them, ``__orcapod_schemas__``, written
by ``scripts/migrate_spiral_schemas.py``. It is the assumed behaviour: a table that
disagrees with its row is refused, and nothing else is consulted.

One row per table, keyed by SpiralDB's immutable id of the table; the name is checked. A
row's ``schema`` is the Arrow schema the table is read with (IPC bytes), and ``nullability``
says where its flags come from: a nullability record (``record``), or the service, for a
table that had none (``service``). One schema per table: a snapshot whose columns differ
from it, also an older version, is refused.

The last row written is the completion marker, under the key ``__complete__``: how many
table rows there are and a digest of them. Rows without a marker that fits them are an
interrupted migration, and every read of the dataset is refused until it is finished. A
dataset with no schema table, or a table that has no row in a complete one, is read as
SpiralDB reports it, with a warning.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from functools import cached_property
from threading import Lock
from typing import Any

import pyarrow as pa

from orcapod.databases.spiraldb_nullability import ITEM, ColumnPath, field_members
from orcapod.errors import TableSchemaError
from orcapod.logical_types.canonical_form import storage_form_schema
from orcapod.utils import arrow_utils

SCHEMA_TABLE = "__orcapod_schemas__"
"""The reserved name of a dataset's schema table."""
FORMAT = "orcapod.table_schema"
FORMAT_VERSION = 1
MARKER_KEY = "__complete__"
FROM_RECORD = "record"
FROM_SERVICE = "service"

KEY_SCHEMA = pa.schema([pa.field("table_id", pa.large_string(), nullable=False)])
ROW_SCHEMA = pa.schema(
    [
        *KEY_SCHEMA,
        pa.field("table_name", pa.large_string()),
        pa.field("format", pa.large_string()),
        pa.field("format_version", pa.int64()),
        pa.field("nullability", pa.large_string()),
        pa.field("schema", pa.large_binary()),
        pa.field("key_columns", pa.large_string()),
        pa.field("nullable_columns", pa.large_string()),
        pa.field("captured_version", pa.int64()),
        pa.field("project", pa.large_string()),
        pa.field("dataset", pa.large_string()),
        pa.field("plan_digest", pa.large_string()),
        pa.field("table_count", pa.int64()),
        pa.field("rows_digest", pa.large_string()),
        pa.field("written_at", pa.large_string()),
        pa.field("written_by", pa.large_string()),
    ]
)


# ---------------------------------------------------------------- comparing schemas


def _compared_type(t: pa.DataType) -> Any:
    """Return ``t`` as it is compared: every nested name and their order count; layout
    (large or not) and nullability do not."""
    if pa.types.is_struct(t):
        fields = [t.field(i) for i in range(t.num_fields)]
        return ["struct", *([f.name, _compared_type(f.type)] for f in fields)]
    if pa.types.is_fixed_size_list(t):
        return ["fixed_size_list", t.list_size, _compared_type(t.value_type)]
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return ["list", _compared_type(t.value_type)]
    if pa.types.is_map(t):
        return ["map", _compared_type(t.key_type), _compared_type(t.item_type)]
    return str(arrow_utils.normalize_to_large_types(t, keep_null=True))


def compared_columns(schema: pa.Schema) -> list[Any]:
    """Return the columns of ``schema`` as they are compared, in order."""
    return [[f.name, _compared_type(f.type)] for f in schema]


def describe_difference(table: pa.Schema, recorded: pa.Schema) -> str:
    """Return how a table's columns differ from a recorded schema's, naming both."""
    have = {f.name: _compared_type(f.type) for f in table}
    want = {f.name: _compared_type(f.type) for f in recorded}
    parts = []
    if missing := sorted(want.keys() - have.keys()):
        parts.append(f"recorded but not in the table: {missing}")
    if extra := sorted(have.keys() - want.keys()):
        parts.append(f"in the table but not recorded: {extra}")
    parts.extend(
        f"column {name!r} is {table.field(name).type} in the table and "
        f"{recorded.field(name).type} in the row"
        for name in sorted(have.keys() & want.keys())
        if have[name] != want[name]
    )
    if not parts and table.names != recorded.names:
        parts.append(
            f"the table has columns in order {table.names}, the row {recorded.names}"
        )
    return "; ".join(parts) or "no difference"


def nullable_paths(schema: pa.Schema) -> list[ColumnPath]:
    """Return the nullable paths of ``schema`` (map interiors excepted), sorted."""
    out: list[ColumnPath] = []

    def walk(field: pa.Field[Any], path: ColumnPath) -> None:
        if field.nullable:
            out.append(path)
        is_struct = pa.types.is_struct(field.type)
        for member in field_members(field.type):
            walk(member, (*path, member.name if is_struct else ITEM))

    for field in schema:
        walk(field, (field.name,))
    return sorted(out)


# ------------------------------------------------------------------------------- a row


@dataclass(frozen=True)
class SchemaRow:
    """One table's row.

    Attributes:
        table_id: SpiralDB's id of the table.
        table_name: The table's plain name.
        key_columns: The table's key columns, in key order.
        schema_bytes: The schema the table is read with, as Arrow IPC bytes. They are
            what is stored and digested, never bytes encoded again from ``schema``:
            which extension types a process has registered changes those.
        nullability: ``FROM_RECORD`` or ``FROM_SERVICE``.
        captured_version: The table's version when its schema was recorded. Kept for
            later; no read uses it.
    """

    table_id: str
    table_name: str
    key_columns: tuple[str, ...]
    schema_bytes: bytes
    nullability: str
    captured_version: int

    @cached_property
    def schema(self) -> pa.Schema:
        """The schema in ``schema_bytes``."""
        return read_schema(self.schema_bytes)

    def content(self) -> dict[str, Any]:
        """Return the row's canonical content, as JSON values (what is digested)."""
        return {
            "table_id": self.table_id,
            "table_name": self.table_name,
            "format": FORMAT,
            "format_version": FORMAT_VERSION,
            "nullability": self.nullability,
            "captured_version": self.captured_version,
            "schema": base64.b64encode(self.schema_bytes).decode(),
            "key_columns": list(self.key_columns),
            "nullable_columns": [list(p) for p in nullable_paths(self.schema)],
        }

    def stored(self) -> dict[str, Any]:
        """Return the row's stored values, without when and by whom."""
        content = self.content()
        return {
            **content,
            "schema": self.schema_bytes,
            "key_columns": canonical_json(content["key_columns"]),
            "nullable_columns": canonical_json(content["nullable_columns"]),
        }

    @classmethod
    def from_stored(cls, row: Mapping[str, Any]) -> SchemaRow:
        """Read a stored row in full.

        Args:
            row: The row's values.

        Raises:
            TableSchemaError: If it cannot be read: an unknown format or version,
                unreadable schema bytes, or readable columns that disagree with them.
        """
        _require_format(row)
        if row["nullability"] not in (FROM_RECORD, FROM_SERVICE):
            raise TableSchemaError(
                f"a row has the unknown nullability {row['nullability']!r}"
            )
        try:
            schema_bytes = bytes(row["schema"])
            schema = read_schema(schema_bytes)
            keys = json.loads(row["key_columns"])
        except (pa.ArrowInvalid, OSError, TypeError, ValueError) as exc:
            raise TableSchemaError(f"a row cannot be decoded ({exc})") from exc
        if (
            not isinstance(keys, list)
            or not keys
            or not all(isinstance(k, str) for k in keys)
        ):
            raise TableSchemaError(f"a row's key_columns are {row['key_columns']!r}")
        readable = canonical_json([list(p) for p in nullable_paths(schema)])
        captured = row["captured_version"]
        if not isinstance(captured, int) or captured < 0:
            raise TableSchemaError(f"a row's captured_version is {captured!r}")
        if row["nullable_columns"] != readable:
            raise TableSchemaError(
                f"a row's nullable_columns {row['nullable_columns']!r} disagree with its "
                f"schema's {readable!r}"
            )
        if not set(keys) <= set(schema.names):
            raise TableSchemaError(
                f"a row's key_columns {keys} are not all columns of its schema "
                f"{schema.names}"
            )
        return cls(
            row["table_id"],
            row["table_name"],
            tuple(keys),
            schema_bytes,
            row["nullability"],
            captured,
        )


def canonical_json(value: Any) -> str:
    """Return ``value`` as JSON in one form: sorted keys, no spaces."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class Scope:
    """What a schema table is complete for, as its marker states it.

    Attributes:
        project: The project.
        dataset: The dataset.
        plan_digest: The digest of the migration plan it was written from.
    """

    project: str
    dataset: str
    plan_digest: str


def rows_digest(scope: Scope, rows: Iterable[SchemaRow]) -> str:
    """Return the digest the completion marker holds: the SHA-256 (hex) of the scope and
    of the rows' canonical content, in table id order.

    Args:
        scope: The marker's scope.
        rows: Every table's row.
    """
    content = {
        "project": scope.project,
        "dataset": scope.dataset,
        "plan_digest": scope.plan_digest,
        "rows": [r.content() for r in sorted(rows, key=lambda r: r.table_id)],
    }
    return hashlib.sha256(canonical_json(content).encode()).hexdigest()


def marker_values(scope: Scope, rows: Iterable[SchemaRow]) -> dict[str, Any]:
    """Return the stored values of the completion marker of ``rows``, without when and
    by whom."""
    listed = list(rows)
    return {
        **dict.fromkeys(f.name for f in ROW_SCHEMA),
        "table_id": MARKER_KEY,
        "format": FORMAT,
        "format_version": FORMAT_VERSION,
        "project": scope.project,
        "dataset": scope.dataset,
        "plan_digest": scope.plan_digest,
        "table_count": len(listed),
        "rows_digest": rows_digest(scope, listed),
    }


def read_schema(data: bytes) -> pa.Schema:
    """Return the schema in IPC bytes, its extension types (pyarrow brings a registered
    one, named in a field's metadata, to life) back in their storage form."""
    return storage_form_schema(pa.ipc.read_schema(pa.py_buffer(data)))


def _require_format(row: Mapping[str, Any]) -> None:
    """Refuse a row of another format or version."""
    if row["format"] != FORMAT:
        raise TableSchemaError(f"a row has the unknown format {row['format']!r}")
    if row["format_version"] != FORMAT_VERSION:
        raise TableSchemaError(
            f"a row has format version {row['format_version']!r} (this Orcapod reads "
            f"version {FORMAT_VERSION})"
        )


def read_rows(
    stored: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, SchemaRow], Mapping[str, Any] | None]:
    """Interpret stored rows.

    Args:
        stored: Every row of a schema table.

    Returns:
        The table rows by table id, and the marker's values (``None`` without one).

    Raises:
        TableSchemaError: If a row cannot be read in full.
    """
    rows: dict[str, SchemaRow] = {}
    marker: Mapping[str, Any] | None = None
    for values in stored:
        if values["table_id"] == MARKER_KEY:
            _require_format(values)
            marker = values
        else:
            rows[values["table_id"]] = SchemaRow.from_stored(values)
    return rows, marker


def require_complete(
    project: str,
    dataset: str,
    rows: Mapping[str, SchemaRow],
    marker: Mapping[str, Any] | None,
) -> None:
    """Refuse rows whose completion marker is missing or does not fit them.

    Args:
        project: The project the schema table was read from.
        dataset: Its dataset.
        rows: The table rows, by table id.
        marker: The marker's values, or ``None``.

    Raises:
        TableSchemaError: If the migration that wrote them was interrupted or withdrawn,
            or the rows were changed or copied from another dataset since.
    """
    if marker is None:
        raise TableSchemaError(
            f"the dataset's {SCHEMA_TABLE} has no completion marker: its migration was "
            "interrupted. Run the migration's write step again"
        )
    if (marker["project"], marker["dataset"]) != (project, dataset):
        raise TableSchemaError(
            f"the dataset's {SCHEMA_TABLE} was written for dataset "
            f"{marker['project']}/{marker['dataset']}"
        )
    scope = Scope(project, dataset, str(marker["plan_digest"]))
    if marker["table_count"] != len(rows) or marker["rows_digest"] != rows_digest(
        scope, rows.values()
    ):
        raise TableSchemaError(
            f"the dataset's {SCHEMA_TABLE} does not fit its completion marker "
            f"({marker['table_count']} tables recorded, {len(rows)} found, or their "
            "content differs): its migration was interrupted or withdrawn, or rows were "
            "changed. Run the migration again"
        )


def require_schema_table(table: Any) -> bool:
    """Check that an opened table is a schema table of this format.

    Args:
        table: The SDK's handle.

    Returns:
        Whether rows were ever written to it.

    Raises:
        TableSchemaError: If it has another key or other columns: the reserved name was
            taken by another program, or by another version.
    """
    if list(table.key_columns) != KEY_SCHEMA.names:
        raise TableSchemaError(
            f"the dataset's {SCHEMA_TABLE} has key columns {list(table.key_columns)}, "
            f"not {KEY_SCHEMA.names}"
        )
    found = table.schema()
    if found.names == KEY_SCHEMA.names:
        return False
    if sorted(compared_columns(found)) != sorted(compared_columns(ROW_SCHEMA)):
        raise TableSchemaError(
            f"the dataset's {SCHEMA_TABLE} is not a schema table this Orcapod reads: "
            f"{describe_difference(found, ROW_SCHEMA)}"
        )
    return True


# ---------------------------------------------------------------------------- the table


class SchemaTable:
    """The schema table of one dataset, read through one client.

    The rows are kept for the snapshot (id and version) they were read from, so a lookup
    opens the table and reads it again only when it changed. No lock is held during a
    call to the service.

    Args:
        project: The project.
        dataset: The dataset.
        open_table: Opens the schema table's latest snapshot; ``None`` when the dataset
            has none. Any other failure is raised.
        service_call: A context that says what a failing service call was doing.
    """

    def __init__(
        self,
        project: str,
        dataset: str,
        open_table: Callable[[], Any | None],
        service_call: Callable[[str], AbstractContextManager[None]],
    ) -> None:
        self._project = project
        self._dataset = dataset
        self._open = open_table
        self._service_call = service_call
        self._lock = Lock()
        self._kept: tuple[tuple[str, int], dict[str, SchemaRow]] | None = None

    def row(self, table_id: str) -> SchemaRow | None:
        """Return the row of ``table_id`` as stored now; ``None`` when the dataset has no
        schema table, or a complete one without that table.

        Raises:
            TableSchemaError: If the table cannot be read in full or is not complete.
        """
        rows = self.rows()
        return None if rows is None else rows.get(table_id)

    def rows(self) -> Mapping[str, SchemaRow] | None:
        """Return every row by table id, as stored now; ``None`` when the dataset has no
        schema table.

        Raises:
            TableSchemaError: If the table cannot be read in full or is not complete.
        """
        table = self._open()
        if table is None:
            return None
        snapshot = (str(table.resource_id), int(table.version_id))
        with self._lock:
            kept = self._kept
        if kept is None or kept[0] != snapshot:
            with self._service_call("read the schema table"):
                written = require_schema_table(table)
                stored = table.scan().to_table().to_pylist() if written else []
            rows, marker = read_rows(stored)
            require_complete(self._project, self._dataset, rows, marker)
            kept = (snapshot, rows)
            with self._lock:
                # An older read that ends late does not replace a newer one.
                if (
                    self._kept is None
                    or self._kept[0][0] != snapshot[0]
                    or self._kept[0][1] < snapshot[1]
                ):
                    self._kept = kept
        return kept[1]
