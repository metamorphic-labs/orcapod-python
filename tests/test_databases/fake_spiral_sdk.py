"""A disk-backed fake of the spiral-sdk surface that Orcapod uses (ITL-1186).

It stores real Arrow tables, every version of each, and returns them the way the service
does, so a connector or source on it goes through the code a live project uses. The same
conformance tests run on it and, opt-in, on the service
(``test_spiraldb_conformance.py``).

Each rule of ``RULES`` was seen on the service with spiral-sdk 0.16.74 and 0.16.75 on
November 24, 2026, on throwaway tables, unless it says otherwise; its name is cited where
the rule is applied. The conformance tests passed live with spiral-sdk 0.16.78 on November
26, 2026 (ITL-1495).
"""

from __future__ import annotations

import json
import math
import os
import threading
import types
import urllib.parse
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pyarrow as pa
import pyarrow.compute as pc

from orcapod.databases.spiraldb_schema_table import SCHEMA_TABLE

RULES = {
    "missing": "a missing table or dataset: SpiralError 'resource not found: "
    "<project>.<dataset>.<name>' and nothing more",
    "id": "every table has its own id; a recreated one gets a new id",
    "version-0": "a table never written has version '0' and its key columns only",
    "asof": "asof gives the newest version not after it; before the first write the key "
    "columns only, with version_id the asof itself",
    "create-409": "create with exist_ok of a table with another key, or without exist_ok "
    "of an existing one: SpiralError with 'status 409' only",
    "create-key-types": "a key of a timestamp, date, decimal or fixed-size binary type "
    "cannot be created (ValueError); a table pyspiral created with one is read",
    "names": "an empty name is a ValueError; more than 255 characters 'status 400'",
    "key-flag": "key columns read nullable however they were declared",
    "types": "strings, binaries and lists read in their small variants; a dictionary and "
    "a run-end encoding as their values; decimal32/64 as decimal128; a map loses "
    "keys_sorted and has fields 'key' and 'value'; a time zone is kept",
    "flags": "top-level columns and struct fields read nullable; a list item keeps the "
    "flag of the first write",
    "metadata": "schema and field metadata is dropped",
    "order": "key columns in key order, then value columns: non-structs by name, then "
    "structs by name; the same for the fields of a struct outside a list",
    "stage": "a write is validated when staged; nothing shows before finish",
    "zero-rows": "staging no rows and finishing publishes nothing",
    "upsert": "rows replace those of their key column by column: a column the write "
    "lacks keeps its value; a new column is added, null in older rows",
    "dup": "a repeated key in one batch is refused; across batches the last wins",
    "null-key": "a null key is refused",
    "key-only": "a write without a value column is refused",
    "no-key": "a write without a key column is refused",
    "reserved": "a value column or struct field starting with '__' or '$', any with '.', "
    "and one named '' or '*' (spiral-sdk 0.16.78, November 26, 2026) is refused",
    "time-of-day": "spiral-sdk 0.16.78 (November 26, 2026): a time32 or time64 value outside "
    "one day, at any depth, refuses the whole write",
    "nul": "a column name is cut at a NUL character; so is a struct field name, at every "
    "depth (spiral-sdk 0.16.78, November 26, 2026)",
    "type-conflict": "a column of another type than stored is refused",
    "unsupported": "duration, interval, fixed-size binary, union, an empty struct and a "
    "null struct outside a list are refused",
    "non-null": "a null in a column, struct field or list item declared or stored "
    "non-null is refused",
    "literal": "a literal must have the column's exact type (large_string is none)",
    "float-eq": "0.0 does not equal -0.0, and NaN equals NaN",
    "rows": "rows come in key order; filter before limit",
    "list": "the documented listing filters a dataset and pages; a page can be empty "
    "(documented, not seen)",
    "table-metadata": "spiral-sdk 0.16.78 (November 26, 2026): get_metadata and "
    "set_metadata hold bytes by text key, an upsert that never deletes (an empty value is "
    "stored); not versioned (every snapshot, asof included, sees the latest; a write makes "
    "no snapshot); readable before any row; a key over 255 UTF-8 bytes, or an entry whose "
    "key and value together are over 8,192 bytes: 'status 400' and nothing written",
}

_MAX_METADATA_KEY_BYTES = 255
# Upserts of metadata are atomic on the service (two clients' keys are all kept); the
# fake's stores are in one process.
_metadata_lock = threading.Lock()
_MAX_METADATA_ENTRY_BYTES = 8192

_ROOT_ENV = "FAKE_SPIRAL_SDK_ROOT"
_API_URL = "https://api.fake.invalid"


class SpiralError(Exception):
    """Stands in for ``spiral.SpiralError``."""


def _refuse(message: str) -> SpiralError:
    """Return the error the service gives for an invalid argument."""
    return SpiralError(f"Scandal:InvalidArgument (message: Runtime Error: {message})")


def _status(code: int) -> SpiralError:
    """Return the error the service gives when it hides the response's body."""
    return SpiralError(f"remote service returned an unknown error (status {code})")


@dataclass
class ScanRecord:
    """One scan: its table and version, its arguments, and the batches handed out."""

    identifier: str
    version: int
    columns: list[str] | None
    filter: Any
    limit: int | None
    batches_read: int = 0
    closed: bool = False

    @property
    def kwargs(self) -> dict[str, Any]:
        """The scan's restriction: ``where``, ``limit`` and the version (``asof``)."""
        return {"where": self.filter, "limit": self.limit, "asof": self.version}


# Every scan of a user's table read in this process, oldest first. Tests clear and read it.
scan_log: list[ScanRecord] = []
# Every count: a scan of some columns only.
count_log: list[ScanRecord] = []
# Every read of a schema table's rows.
schema_scan_log: list[ScanRecord] = []
# Rows per batch a scan hands out (the service's batches follow its stored fragments).
scan_batch_rows = 2
# Tables per page of the listing.
list_page_size = 3


# --------------------------------------------------------------------------- what is stored


def _order(fields: list[pa.Field[Any]]) -> list[pa.Field[Any]]:
    """Order fields as the service does (rule ``order``)."""
    return sorted(fields, key=lambda f: (pa.types.is_struct(f.type), f.name))


def _stored_type(t: pa.DataType, *, in_list: bool = False) -> pa.DataType:
    """Return the type a column written as ``t`` reads back with (``types``, ``flags``)."""
    if isinstance(t, pa.BaseExtensionType):
        return _stored_type(t.storage_type, in_list=in_list)
    if pa.types.is_dictionary(t) or pa.types.is_run_end_encoded(t):
        return _stored_type(t.value_type, in_list=in_list)
    if pa.types.is_decimal32(t) or pa.types.is_decimal64(t):
        return pa.decimal128(t.precision, t.scale)
    if pa.types.is_large_string(t) or pa.types.is_string_view(t):
        return pa.string()
    if pa.types.is_large_binary(t) or pa.types.is_binary_view(t):
        return pa.binary()
    if pa.types.is_struct(t):
        fields = [
            pa.field(f.name, _stored_type(f.type, in_list=in_list))
            for f in (t.field(i) for i in range(t.num_fields))
        ]
        return pa.struct(fields if in_list else _order(fields))
    if pa.types.is_map(t):
        item = t.item_field
        make: Any = pa.map_  # pyarrow takes an item field; its stubs list only types
        return make(
            _stored_type(t.key_type, in_list=True),
            pa.field("value", _stored_type(item.type, in_list=True), item.nullable),
        )
    if pa.types.is_fixed_size_list(t):
        return pa.list_(_stored_item(t), t.list_size)
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return pa.list_(_stored_item(t))
    return t


def _stored_item(t: Any) -> pa.Field[Any]:
    """Return the stored item field of a list type."""
    item = t.value_field
    return pa.field(item.name, _stored_type(item.type, in_list=True), item.nullable)


def _refused_type(t: pa.DataType) -> str | None:
    """Return why a column of type ``t`` cannot be written (``unsupported``)."""
    if isinstance(t, pa.BaseExtensionType):
        return _refused_type(t.storage_type)
    if (
        pa.types.is_duration(t)
        or pa.types.is_interval(t)
        or pa.types.is_fixed_size_binary(t)
        or pa.types.is_union(t)
    ):
        return f"Array encoding not implemented for Arrow data type {t}"
    if pa.types.is_struct(t):
        if t.num_fields == 0:
            return "column group is empty"
        for i in range(t.num_fields):
            if (why := _reserved(t.field(i).name)) is not None:
                return why
    if pa.types.is_dictionary(t) or pa.types.is_run_end_encoded(t):
        return None
    for i in range(t.num_fields):
        if (why := _refused_type(t.field(i).type)) is not None:
            return why
    return None


def _reserved(name: str) -> str | None:
    """Return the refusal of a reserved column name (``reserved``)."""
    if name.startswith(("__", "$")) or "." in name or name in ("", "*"):
        return f'Column name "{name}" is reserved'
    return None


_DAY = {"s": 86_400, "ms": 86_400_000, "us": 86_400_000_000, "ns": 86_400_000_000_000}


def _decoded(array: Any) -> Any:
    """``array`` with every dictionary and run-end encoding at its top undone, as the
    service stores it (``types``); a run-end-encoded struct included, which pyarrow
    cannot decode itself."""
    if isinstance(array, pa.ExtensionArray):
        array = array.storage
    if pa.types.is_dictionary(array.type):
        return _decoded(array.dictionary_decode())
    if pa.types.is_run_end_encoded(array.type):
        return _decoded(array.values.take(pa.array(_run_indices(array), pa.int64())))
    return array


def _run_indices(array: Any) -> list[int]:
    """The run of each row of a run-end-encoded array (a slice of one included): only
    the runs that overlap the slice are expanded, and only where they overlap it."""
    start, stop = array.offset, array.offset + len(array)
    indices: list[int] = []
    begin = 0
    for run, end in enumerate(array.run_ends.to_pylist()):
        low, high = max(begin, start), min(end, stop)
        if low < high:
            indices.extend([run] * (high - low))
        if end >= stop:
            break
        begin = end
    return indices


def _needs_decoding(t: Any, *, in_extension: bool = False) -> bool:
    """Whether a column must be rebuilt as the service stores it before its names are
    cut (``types``, ``nul``): it holds a dictionary or run-end encoding at any depth, or
    an extension type whose storage holds a struct field name with NUL."""
    if pa.types.is_dictionary(t) or pa.types.is_run_end_encoded(t):
        return True
    if isinstance(t, pa.BaseExtensionType):
        return _needs_decoding(t.storage_type, in_extension=True)
    if in_extension and pa.types.is_struct(t):
        if any("\0" in t.field(i).name for i in range(t.num_fields)):
            return True
    return any(
        _needs_decoding(t.field(i).type, in_extension=in_extension)
        for i in range(t.num_fields)
    )


def _deep_decoded(array: Any) -> Any:
    """``array`` with every dictionary and run-end encoding undone, at any depth, as the
    service stores it (``types``), so nested names are cut as at the top (``nul``)."""
    a = _decoded(array)
    if a.offset:
        a = pa.concat_arrays([a])  # rebuilt from offset zero: pyarrow builds no slice
    t = a.type
    mask = a.is_null() if a.null_count else None
    if pa.types.is_struct(t):
        children = [_deep_decoded(a.field(i)) for i in range(t.num_fields)]
        fields = [t.field(i).with_type(c.type) for i, c in enumerate(children)]
        return pa.StructArray.from_arrays(children, fields=fields, mask=mask)
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        values = _deep_decoded(a.values)
        kind: Any = pa.list_ if pa.types.is_list(t) else pa.large_list
        cls: Any = pa.ListArray if pa.types.is_list(t) else pa.LargeListArray
        target = kind(t.value_field.with_type(values.type))
        return cls.from_arrays(a.offsets, values, type=target, mask=mask)
    if pa.types.is_fixed_size_list(t):
        size = t.list_size
        values = _deep_decoded(a.values.slice(a.offset * size, len(a) * size))
        target = pa.list_(t.value_field.with_type(values.type), size)
        if size == 0:  # no values to count the rows by: from buffers
            valid = pc.is_valid(a).buffers()[1] if a.null_count else None
            return pa.Array.from_buffers(
                target,
                len(a),
                cast(Any, [valid]),  # the stubs take no None for an absent buffer
                null_count=a.null_count,
                children=[values],
            )
        return cast(Any, pa.FixedSizeListArray).from_arrays(
            values, type=target, mask=mask
        )
    if pa.types.is_map(t):
        keys, items = _deep_decoded(a.keys), _deep_decoded(a.items)
        target = cast(Any, pa.map_)(
            t.key_field.with_type(keys.type),
            t.item_field.with_type(items.type),
            keys_sorted=t.keys_sorted,
        )
        return cast(Any, pa.MapArray).from_arrays(
            a.offsets, keys, items, type=target, mask=mask
        )
    return a


def _time_outside_a_day(array: Any) -> bool:
    """Whether a time value outside one day is in ``array``, at any depth (``time-of-day``)."""
    array = _decoded(array)
    t = array.type
    if pa.types.is_time(t):
        ticks = array.view(pa.int32() if pa.types.is_time32(t) else pa.int64())
        day = _DAY[t.unit]
        return any(v is not None and not 0 <= v < day for v in ticks.to_pylist())
    if pa.types.is_struct(t):
        return any(_time_outside_a_day(array.field(i)) for i in range(t.num_fields))
    if pa.types.is_map(t):
        return _time_outside_a_day(array.keys) or _time_outside_a_day(array.items)
    if (
        pa.types.is_list(t)
        or pa.types.is_large_list(t)
        or pa.types.is_fixed_size_list(t)
    ):
        return _time_outside_a_day(array.flatten())
    return False


def _stored_array(array: pa.Array[Any], target: pa.DataType) -> pa.Array[Any]:
    """Return ``array`` as it is stored, of type ``target``.

    Rebuilt from its parts where a cast would not do (structs whose fields are reordered,
    maps), so every value and every null is kept.

    Args:
        array: The array written.
        target: ``_stored_type`` of its type, or the type already stored.
    """
    a: Any = array
    if isinstance(a, pa.ExtensionArray):
        a = a.storage
    a = _decoded(a)
    if a.type == target:
        return a
    t: Any = target
    if pa.types.is_struct(t):
        children = [
            _stored_array(a.field(t.field(i).name), t.field(i).type)
            for i in range(t.num_fields)
        ]
        return pa.StructArray.from_arrays(
            children, fields=list(t), mask=a.is_null() if a.null_count else None
        )
    if pa.types.is_map(t):
        return cast(Any, pa.MapArray).from_arrays(
            a.offsets,
            _stored_array(a.keys, t.key_type),
            _stored_array(a.items, t.item_type),
            type=t,
            mask=a.is_null() if a.null_count else None,
        )
    if pa.types.is_list(t) and (
        pa.types.is_list(a.type) or pa.types.is_large_list(a.type)
    ):
        offsets = a.offsets.cast(pa.int32())
        return pa.ListArray.from_arrays(
            offsets,
            _stored_array(a.values, t.value_type),
            type=t,
            mask=a.is_null() if a.null_count else None,
        )
    if pa.types.is_fixed_size_list(t):
        values = _stored_array(
            a.values.slice(a.offset * t.list_size, len(a) * t.list_size), t.value_type
        )
        if t.list_size == 0:  # no values to count the rows by: from buffers
            valid = pc.is_valid(a).buffers()[1] if a.null_count else None
            return pa.Array.from_buffers(
                t,
                len(a),
                cast(Any, [valid]),  # the stubs take no None for an absent buffer
                null_count=a.null_count,
                children=[values],
            )
        return cast(Any, pa.FixedSizeListArray).from_arrays(
            values, type=t, mask=a.is_null() if a.null_count else None
        )
    return a.cast(target)


def _names_cut_at_nul(t: Any) -> Any:
    """Return ``t`` with every struct field name cut at its first NUL (``nul``)."""
    if isinstance(t, pa.BaseExtensionType):
        return t
    if pa.types.is_struct(t):
        return pa.struct(
            [
                pa.field(
                    f.name.split("\0")[0],
                    _names_cut_at_nul(f.type),
                    f.nullable,
                    f.metadata,
                )
                for f in t
            ]
        )
    if pa.types.is_large_list(t) or pa.types.is_list(t):
        item = t.value_field
        kind: Any = pa.large_list if pa.types.is_large_list(t) else pa.list_
        return kind(item.with_type(_names_cut_at_nul(item.type)))
    if pa.types.is_fixed_size_list(t):
        item = t.value_field
        return pa.list_(item.with_type(_names_cut_at_nul(item.type)), t.list_size)
    if pa.types.is_map(t):
        map_of: Any = pa.map_  # the stubs take no fields and keys_sorted together
        return map_of(
            t.key_field.with_type(_names_cut_at_nul(t.key_type)),
            t.item_field.with_type(_names_cut_at_nul(t.item_type)),
            keys_sorted=t.keys_sorted,
        )
    return t


def _check_nulls(array: Any, field: pa.Field[Any], *, in_list: bool) -> None:
    """Refuse a null where the written type has none, or a null struct (``non-null``,
    ``unsupported``).

    Args:
        array: The array.
        field: The field it was written under.
        in_list: Whether it is inside a list, where a null struct is kept.
    """
    if isinstance(array, pa.ExtensionArray):
        array = array.storage
    if not field.nullable and array.null_count:
        raise SpiralError(
            f"Column '{field.name}' is declared as non-nullable but contains null values"
        )
    t: Any = array.type
    if pa.types.is_struct(t):
        if array.null_count and not in_list:
            raise _refuse(f"Column group {field.name} contains null rows")
        for i in range(t.num_fields):
            _check_nulls(array.field(i), t.field(i), in_list=in_list)
    elif pa.types.is_list(t) or pa.types.is_large_list(t):
        _check_nulls(array.flatten(), t.value_field, in_list=True)
    elif pa.types.is_fixed_size_list(t):
        _check_nulls(array.flatten(), t.value_field, in_list=True)


def _bits(value: Any) -> Any:
    """Return ``value`` so that ``==`` compares it as the service does (``float-eq``)."""
    if isinstance(value, float):
        return "nan" if math.isnan(value) else (value, math.copysign(1.0, value))
    return value


# ------------------------------------------------------------------------------- the store


class _Store:
    """Directory-backed tables, shared by every fake object."""

    def __init__(self, root: Path) -> None:
        """Open the store under ``root``."""
        self.root = root
        root.mkdir(parents=True, exist_ok=True)

    def tables(self) -> list[dict[str, Any]]:
        """Return every table's description."""
        return [
            json.loads(p.read_text())
            for p in sorted(self.root.glob("table_*/table.json"))
        ]

    def find(self, project: str, dataset: str, name: str) -> dict[str, Any] | None:
        """Return the table of that name, or ``None``."""
        for table in self.tables():
            if (table["project"], table["dataset"], table["name"]) == (
                project,
                dataset,
                name,
            ):
                return table
        return None

    def by_id(self, table_id: str) -> dict[str, Any]:
        """Return the table with ``table_id`` (``id``)."""
        path = self.root / table_id / "table.json"
        if not path.exists():
            raise _status(404)
        return json.loads(path.read_text())

    def create(
        self, project: str, dataset: str, name: str, key_schema: pa.Schema
    ) -> dict[str, Any]:
        """Create a table (``id``, ``key-flag``)."""
        table_id = f"table_{uuid.uuid4().hex[:12]}"
        folder = self.root / table_id
        folder.mkdir()
        keys = pa.schema([pa.field(f.name, _stored_type(f.type)) for f in key_schema])
        (folder / "keys.arrow").write_bytes(keys.serialize().to_pybytes())
        table = {
            "id": table_id,
            "project": project,
            "dataset": dataset,
            "name": name,
            "keys": keys.names,
        }
        (folder / "table.json").write_text(json.dumps(table))
        return table

    def drop(self, table_id: str) -> None:
        """Remove a table and every version of it."""
        folder = self.root / table_id
        for path in folder.iterdir():
            path.unlink()
        folder.rmdir()

    def metadata(self, table_id: str) -> dict[str, bytes]:
        """Return a table's metadata (``table-metadata``)."""
        _ = self.by_id(table_id)
        path = self.root / table_id / "metadata.json"
        if not path.exists():
            return {}
        stored = cast("dict[str, str]", json.loads(path.read_text()))
        return {k: bytes.fromhex(v) for k, v in stored.items()}

    def set_metadata(self, table_id: str, entries: dict[str, bytes]) -> None:
        """Upsert ``entries`` into a table's metadata (``table-metadata``)."""
        for key, value in entries.items():
            size = len(key.encode())
            if (
                size > _MAX_METADATA_KEY_BYTES
                or size + len(value) > _MAX_METADATA_ENTRY_BYTES
            ):
                raise _status(400)
        with _metadata_lock:  # one upsert at a time: concurrent keys are all kept
            stored = self.metadata(table_id)
            stored.update(entries)
            # Written beside, then put in place: a reader sees a whole mapping.
            target = self.root / table_id / "metadata.json"
            written = target.with_suffix(".json.tmp")
            _ = written.write_text(json.dumps({k: v.hex() for k, v in stored.items()}))
            _ = written.replace(target)

    def keys(self, table_id: str) -> pa.Schema:
        """Return a table's key schema."""
        raw = (self.root / table_id / "keys.arrow").read_bytes()
        return pa.ipc.read_schema(pa.py_buffer(raw))

    def versions(self, table_id: str) -> list[int]:
        """Return a table's versions, oldest first."""
        return sorted(int(p.stem[1:]) for p in (self.root / table_id).glob("v*.arrow"))

    def read(self, table_id: str, version: int) -> pa.Table:
        """Return a table's rows at ``version`` (``version-0``, ``asof``)."""
        existing = [v for v in self.versions(table_id) if v <= version]
        if not existing:
            return self.keys(table_id).empty_table()
        path = self.root / table_id / f"v{existing[-1]}.arrow"
        with pa.ipc.open_file(pa.memory_map(str(path))) as reader:
            return reader.read_all()

    def write(self, table_id: str, rows: pa.Table) -> int:
        """Store ``rows`` as a table's next version."""
        if not (self.root / table_id).exists():
            raise _status(404)
        version = max([0, *self.versions(table_id)]) + 1
        with pa.ipc.new_file(
            str(self.root / table_id / f"v{version}.arrow"), rows.schema
        ) as writer:
            writer.write_table(rows)
        return version


def _staged(rows: pa.Table, keys: list[str], stored: pa.Schema) -> pa.Table:
    """Validate ``rows`` and return them as they are stored (``stage``)."""
    names = [name.split("\0")[0] for name in rows.column_names]  # nul
    rows = rows.rename_columns(names)
    for i, field in enumerate(rows.schema):  # nul, at every depth
        if _needs_decoding(field.type):
            # Stored as its values (``types``): decoded first, so its names are cut.
            decoded = pa.chunked_array(
                [_deep_decoded(c) for c in rows.column(i).chunks]
            )
            rows = rows.set_column(i, field.with_type(decoded.type), decoded)
            field = rows.schema.field(i)
        cut = _names_cut_at_nul(field.type)
        if cut != field.type:
            column = rows.column(i)
            rows = rows.set_column(
                i,
                field.with_type(cut),  # the field, flag and metadata kept
                pa.chunked_array([c.view(cut) for c in column.chunks], type=cut),
            )
    for key in keys:
        if key not in names:
            raise _refuse(f"Input batch is missing key field '{key}'")
        if rows.column(key).null_count:
            raise _refuse(f"Key field '{key}' contains null values")
    if set(names) <= set(keys):
        raise _refuse("Write contains no data columns")
    tuples = list(zip(*(rows.column(k).to_pylist() for k in keys)))
    if len(set(tuples)) != len(tuples):
        raise _refuse("Duplicate keys found in input batch")
    fields, arrays = [], []
    for field in rows.schema:
        why = None if field.name in keys else _reserved(field.name)
        why = why or _refused_type(field.type)
        if why is not None:
            raise _refuse(why)
        array: Any = pa.concat_arrays(
            rows.column(field.name).chunks or [pa.array([], field.type)]
        )
        _check_nulls(array, field, in_list=False)
        if _time_outside_a_day(array):
            raise SpiralError(
                "Scandal:Internal (message: Vortex error: Invalid time scalar: adding "
                "duration to time overflowed)"
            )
        target = _stored_type(field.type)
        if field.name in stored.names:
            kept = stored.field(field.name).type
            if _without_flags(kept) != _without_flags(target):
                raise _refuse(
                    f"cannot unify incompatible data types: {kept} and {target}"
                )
            _check_nulls(array, pa.field(field.name, kept), in_list=False)
            _check_stored_items(array, kept)
            target = kept
        fields.append(pa.field(field.name, target))
        arrays.append(_stored_array(array, target))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _check_stored_items(array: Any, stored: Any) -> None:
    """Refuse a null list item where the stored item is non-null (``non-null``)."""
    if isinstance(array, pa.ExtensionArray):
        array = array.storage
    if pa.types.is_list(stored) or pa.types.is_fixed_size_list(stored):
        items = array.flatten()
        if not stored.value_field.nullable and items.null_count:
            raise SpiralError("Vortex error: Cannot cast AllInvalid to NonNullable")
        _check_stored_items(items, stored.value_type)
    elif pa.types.is_struct(stored):
        for i in range(stored.num_fields):
            _check_stored_items(array.field(stored.field(i).name), stored.field(i).type)


def _without_flags(t: pa.DataType) -> pa.DataType:
    """Return ``t`` with every nested field nullable."""
    if pa.types.is_struct(t):
        return pa.struct(
            [
                (t.field(i).name, _without_flags(t.field(i).type))
                for i in range(t.num_fields)
            ]
        )
    if pa.types.is_fixed_size_list(t):
        return pa.list_(_without_flags(t.value_type), t.list_size)
    if pa.types.is_list(t):
        return pa.list_(_without_flags(t.value_type))
    if pa.types.is_map(t):
        return pa.map_(_without_flags(t.key_type), _without_flags(t.item_type))
    return t


def _upserted(stored: pa.Table, rows: pa.Table, keys: list[str]) -> pa.Table:
    """Return ``stored`` with ``rows`` upserted, in key order (``upsert``, ``order``)."""
    fields = {f.name: f for f in (*stored.schema, *rows.schema)}
    fields.update({f.name: f for f in stored.schema})
    values = _order([f for name, f in fields.items() if name not in keys])
    schema = pa.schema([*(fields[k] for k in keys), *values])

    def key_of(table: pa.Table) -> list[tuple[Any, ...]]:
        return list(zip(*(table.column(k).to_pylist() for k in keys)))

    old = {key: i for i, key in enumerate(key_of(stored))}
    new = {key: i for i, key in enumerate(key_of(rows))}
    # Python's order of these key values is the service's: text by code point.
    ordered = sorted({**old, **new})
    columns = []
    for field in schema:
        parts: list[pa.Array[Any]] = []
        for table in (stored, rows):
            column = (
                table.column(field.name).combine_chunks()
                if field.name in table.column_names
                else pa.nulls(table.num_rows, field.type)
            )
            parts.append(cast("pa.Array[Any]", column))
        whole: Any = pa.concat_arrays(
            [p if p.type == field.type else _stored_array(p, field.type) for p in parts]
        )
        indices = [
            stored.num_rows + new[key]
            if key in new and field.name in rows.column_names
            else old.get(key)
            for key in ordered
        ]
        columns.append(whole.take(pa.array(indices, pa.int64())))
    return pa.Table.from_arrays(columns, schema=schema)


# --------------------------------------------------------------------------- expressions


@dataclass(frozen=True)
class _Column:
    """A column named in an expression."""

    name: str


@dataclass(frozen=True)
class _Literal:
    """A value with the exact Arrow type ``FakeKeyedTable.scan`` checks."""

    value: Any
    type: pa.DataType


@dataclass(frozen=True)
class FakePredicate:
    """Equalities a row must all match."""

    terms: tuple[tuple[str, _Literal], ...]

    @property
    def equalities(self) -> list[tuple[str, Any]]:
        """The ``(column, value)`` pairs, in the order given."""
        return [(name, wanted.value) for name, wanted in self.terms]

    def __repr__(self) -> str:
        """Return the equalities as ``name == value`` terms joined by ``&``."""
        return " & ".join(f"{name} == {value!r}" for name, value in self.equalities)


def column(name: str) -> _Column:
    """Stand in for ``spiral.column``."""
    return _Column(name)


def literal(value: Any, type: Any = None) -> _Literal:
    """Stand in for ``spiral.literal`` (``literal``): an explicit scalar type."""
    if type is None or pa.types.is_large_string(type) or pa.types.is_large_binary(type):
        raise TypeError(f"unsupported literal Arrow type {type}")
    return _Literal(pa.scalar(value, type).as_py(), type)


def eq(left: _Column, right: _Literal) -> FakePredicate:
    """Stand in for ``spiral.eq``."""
    return FakePredicate(((left.name, right),))


def and_(left: FakePredicate, right: FakePredicate) -> FakePredicate:
    """Stand in for ``spiral.and_``."""
    return FakePredicate(left.terms + right.terms)


# -------------------------------------------------------------------------------- the client


class FakeReader:
    """Stands in for the reader of ``Scan.to_reader()``; a closed one gives no row."""

    def __init__(self, rows: pa.Table, record: ScanRecord, batch_rows: int) -> None:
        """Hold the parts it is made of."""
        self._batches = iter(rows.to_batches(max_chunksize=batch_rows))
        self._record = record

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        """Yield the batches until there is none."""
        while True:
            try:
                yield self.read_next_batch()
            except StopIteration:
                return

    def __next__(self) -> pa.RecordBatch:
        """Return the next batch."""
        return self.read_next_batch()

    def read_next_batch(self) -> pa.RecordBatch:
        """Return the next batch, or raise ``StopIteration``."""
        if self._record.closed:
            raise SpiralError("the reader is closed")
        batch = next(self._batches)
        self._record.batches_read += 1
        return batch

    def close(self) -> None:
        """Close the reader."""
        self._record.closed = True


class FakeScan:
    """Stands in for ``spiral.tables.Scan``: rows are read when asked for."""

    def __init__(self, rows: pa.Table, record: ScanRecord, batch_rows: int) -> None:
        """Hold the parts it is made of."""
        self._rows, self._record, self._batch_rows = rows, record, batch_rows

    def to_reader(self) -> FakeReader:
        """Return a reader of the rows, ``scan_batch_rows`` at most per batch."""
        log = (
            schema_scan_log
            if self._record.identifier.endswith(f".{SCHEMA_TABLE}")
            else (count_log if self._record.columns else scan_log)
        )
        log.append(self._record)
        return FakeReader(self._rows, self._record, self._batch_rows)

    def to_table(self) -> pa.Table:
        """Return the rows."""
        batches = list(self.to_reader())
        return pa.Table.from_batches(batches, schema=self._rows.schema)


class FakeKeyedTable:
    """Stands in for ``spiral.keyed_tables.KeyedTable``: one immutable snapshot."""

    def __init__(self, store: _Store, table: dict[str, Any], version: int) -> None:
        """Hold the parts it is made of."""
        self._store, self._table, self._version = store, table, version

    @property
    def resource_id(self) -> str:
        """The table's id (``id``)."""
        return self._table["id"]

    @property
    def version_id(self) -> str:
        """The snapshot's version (``version-0``, ``asof``)."""
        return str(self._version)

    @property
    def key_columns(self) -> tuple[str, ...]:
        """The key columns, in key order."""
        return tuple(self._table["keys"])

    def get_metadata(self) -> dict[str, bytes]:
        """Return the table's metadata, the latest whatever the snapshot (``table-metadata``)."""
        return self._store.metadata(self.resource_id)

    def set_metadata(self, metadata: dict[str, bytes]) -> None:
        """Upsert entries of the table's metadata (``table-metadata``)."""
        self._store.set_metadata(self.resource_id, metadata)

    def schema(self) -> pa.Schema:
        """Return the snapshot's schema."""
        return self._store.read(self.resource_id, self._version).schema

    def scan(
        self,
        columns: list[str] | None = None,
        *,
        filter: FakePredicate | None = None,
        limit: int | None = None,
        batch_rows: int | None = None,
    ) -> FakeScan:
        """Plan a scan (``rows``, ``literal``, ``float-eq``)."""
        rows = self._store.read(self.resource_id, self._version)
        for name, wanted in filter.terms if filter else ():
            if rows.schema.field(name).type != wanted.type:
                raise SpiralError(
                    "scan filter is unsupported: Comparison requires equal dtypes, got "
                    f"{rows.schema.field(name).type} and {wanted.type}"
                )
            # Temporal values as their counts: a date64 may be off midnight.
            stored: Any = rows.column(name)
            given: Any = pa.array([wanted.value], wanted.type)
            if pa.types.is_temporal(wanted.type):
                width = pa.int32() if pa.types.is_date32(wanted.type) else pa.int64()
                stored, given = stored.cast(width), given.cast(width)
            values, (value,) = stored.to_pylist(), given.to_pylist()
            rows = rows.filter(
                pa.array(
                    [v is not None and _bits(v) == _bits(value) for v in values],
                    pa.bool_(),
                )
            )
        if columns is not None:
            rows = rows.select(columns)
        if limit is not None:
            rows = rows.slice(0, limit)
        record = ScanRecord(
            f"{self._table['dataset']}.{self._table['name']}",
            self._version,
            columns,
            filter,
            limit,
        )
        size = (
            scan_batch_rows if batch_rows is None else min(batch_rows, scan_batch_rows)
        )
        return FakeScan(rows, record, size)

    def append(self, data: pa.Table) -> FakeKeyedTable:
        """Upsert rows and return the new snapshot."""
        writer = self.writer()
        writer.append(data)
        return writer.finish()

    def writer(self) -> FakeWriter:
        """Begin a write."""
        return FakeWriter(self._store, self._table)


class FakeWriter:
    """Stands in for ``spiral.keyed_tables.KeyedTableWrite``."""

    def __init__(self, store: _Store, table: dict[str, Any]) -> None:
        """Hold the parts it is made of."""
        self._store, self._table = store, table
        self._parts: list[pa.Table] = []
        self._keys_to_delete: list[pa.Table] = []
        self._drop: list[str] = []
        self.aborted = False

    def append(self, data: pa.Table) -> None:
        """Validate and stage rows (``stage``)."""
        latest = self._latest()
        self._parts.append(_staged(data, self._table["keys"], latest.schema))

    def delete_keys(self, data: pa.Table) -> None:
        """Stage the deletion of rows."""
        self._keys_to_delete.append(data)

    def alter_schema(self, changes: list[str]) -> None:
        """Stage column drops (``drop_column`` gives the name here)."""
        self._drop.extend(changes)

    def finish(self) -> FakeKeyedTable:
        """Publish what was staged (``zero-rows``)."""
        self._store.by_id(self._table["id"])
        keys = self._table["keys"]
        rows = self._latest()
        before = rows
        for part in self._parts:
            if part.num_rows:
                rows = _upserted(rows, part, keys)
        for dropped in self._keys_to_delete:
            gone = set(zip(*(dropped.column(k).to_pylist() for k in keys)))
            kept = [
                key not in gone
                for key in zip(*(rows.column(k).to_pylist() for k in keys))
            ]
            rows = rows.filter(pa.array(kept, pa.bool_()))
        if self._drop:
            rows = rows.drop_columns(self._drop)
        if rows is not before:
            self._store.write(self._table["id"], rows)
        return _open(self._store, self._table, None)

    def abort(self) -> None:
        """Close without publishing."""
        self.aborted = True

    def _latest(self) -> pa.Table:
        """Return the table's latest rows."""
        self._store.by_id(self._table["id"])
        versions = self._store.versions(self._table["id"])
        return self._store.read(self._table["id"], max([0, *versions]))


def drop_column(name: str) -> str:
    """Stand in for ``spiral.drop_column``."""
    return name


def _open(store: _Store, table: dict[str, Any], asof: int | None) -> FakeKeyedTable:
    """Open a table at ``asof`` or its latest version (``asof``, ``version-0``)."""
    versions = store.versions(table["id"])
    if asof is None:
        version = max([0, *versions])
    else:
        existing = [v for v in versions if v <= asof]
        version = existing[-1] if existing else asof
    return FakeKeyedTable(store, table, version)


class FakeProject:
    """Stands in for ``spiral.projects.Project``."""

    def __init__(self, store: _Store, project: str) -> None:
        """Hold the parts it is made of."""
        self._store, self._project = store, project

    def create_keyed_table(
        self, dataset: str, name: str, key_schema: pa.Schema, *, exist_ok: bool = False
    ) -> FakeKeyedTable:
        """Create a table (``create-409``, ``create-key-types``, ``names``)."""
        if not dataset or not name:
            raise ValueError("dataset and table names must be nonempty")
        if len(name) > 255:
            raise _status(400)
        for field in key_schema:
            t = field.type
            if (
                pa.types.is_timestamp(t)
                or pa.types.is_date(t)
                or pa.types.is_decimal(t)
                or pa.types.is_fixed_size_binary(t)
            ):
                raise ValueError(
                    f"Keyed creation does not support type {t} for key column "
                    f"'{field.name}'"
                )
        table = self._store.find(self._project, dataset, name)
        if table is not None:
            asked = [(f.name, _stored_type(f.type)) for f in key_schema]
            found = [(f.name, f.type) for f in self._store.keys(table["id"])]
            if not exist_ok or asked != found:
                raise _status(409)
            return _open(self._store, table, None)
        created = self._store.create(self._project, dataset, name, key_schema)
        return _open(self._store, created, None)


class FakeClient:
    """Stands in for ``spiral.Client``; tables live under the installed root."""

    def __init__(self, *, api_url: str | None = None, token: str | None = None) -> None:
        """Hold the parts it is made of."""
        self._store = _store()
        self.api_url = api_url
        self._native = types.SimpleNamespace(
            api_url=api_url or _API_URL, access_token=lambda: "fake-token"
        )

    def project(self, id: str) -> FakeProject:
        """Return a project, without a request."""
        return FakeProject(self._store, id)

    def keyed_table(
        self, project: str, dataset: str, name: str, *, asof: int | None = None
    ) -> FakeKeyedTable:
        """Open a table by name (``missing``)."""
        table = self._store.find(project, dataset, name)
        if table is None:
            raise SpiralError(f"resource not found: {project}.{dataset}.{name}")
        return _open(self._store, table, asof)

    def keyed_table_by_id(
        self, table_id: str, *, asof: int | None = None
    ) -> FakeKeyedTable:
        """Open a table by its id."""
        return _open(self._store, self._store.by_id(table_id), asof)


def _store() -> _Store:
    """Return the installed store."""
    root = os.environ.get(_ROOT_ENV)
    if not root:
        raise RuntimeError(f"{_ROOT_ENV} is not set; call fake_spiral_sdk.install()")
    return _Store(Path(root))


def fake_http_get_json(url: str, token: str | None) -> dict[str, Any]:
    """Stand in for the documented listing endpoint (``list``): pages, the last but one
    of them empty."""
    parts = urllib.parse.urlsplit(url)
    query = dict(urllib.parse.parse_qsl(parts.query))
    assert token == "fake-token"
    segments = [urllib.parse.unquote(s) for s in parts.path.split("/")]
    assert (
        segments[1:4] == ["api", "keyed-tables", "projects"] and segments[5] == "tables"
    )
    items = [
        {"table": t["name"], "dataset": t["dataset"], "id": t["id"]}
        for t in _store().tables()
        if t["project"] == segments[4]
        and query.get("dataset", t["dataset"]) == t["dataset"]
    ]
    pages = [
        items[i : i + list_page_size] for i in range(0, len(items), list_page_size)
    ] or [[]]
    if len(pages) > 1:
        pages.insert(-1, [])
    index = int(query.get("pageToken", "0"))
    page: dict[str, Any] = {"items": pages[index]}
    if index + 1 < len(pages):
        page["nextPageToken"] = str(index + 1)
    return page


fake_module = types.SimpleNamespace(
    Client=FakeClient,
    SpiralError=SpiralError,
    column=column,
    literal=literal,
    eq=eq,
    and_=and_,
    drop_column=drop_column,
)


# ------------------------------------------------------------------------ another program


class ExternalTable:
    """A table as another program writes it: no row in the schema table."""

    def __init__(self, store: _Store, table: dict[str, Any]) -> None:
        """Hold the parts it is made of."""
        self._store, self._table = store, table

    @property
    def table_id(self) -> str:
        """The table's id."""
        return self._table["id"]

    def last_modified_at(self) -> int:
        """Return the table's version."""
        return int(_open(self._store, self._table, None).version_id)

    def write(self, rows: pa.Table) -> None:
        """Upsert rows."""
        _open(self._store, self._table, None).append(rows)

    def record(self, schema: pa.Schema) -> None:
        """Migrate the dataset, with ``schema`` as what this table was written with."""
        migrate(
            self._table["project"],
            {self._table["name"]: schema},
            self._table["dataset"],
        )


class ExternalProject:
    """A project as another program uses it: keys of any type (as pyspiral creates
    them), and drops."""

    def __init__(self, project: str) -> None:
        """Hold the parts it is made of."""
        self._store, self._project = _store(), project

    def create_table(self, identifier: str, *, key_schema: Any) -> ExternalTable:
        """Create ``"<dataset>.<table>"`` with a key schema or ``(name, type)`` pairs."""
        dataset, _, name = identifier.partition(".")
        table = self._store.create(self._project, dataset, name, pa.schema(key_schema))
        return ExternalTable(self._store, table)

    def table(self, identifier: str) -> ExternalTable:
        """Open ``"<dataset>.<table>"``."""
        dataset, _, name = identifier.partition(".")
        table = self._store.find(self._project, dataset, name)
        if table is None:
            raise KeyError(identifier)
        return ExternalTable(self._store, table)

    def drop_table(self, identifier: str) -> None:
        """Drop ``"<dataset>.<table>"``; nothing if there is none."""
        dataset, _, name = identifier.partition(".")
        table = self._store.find(self._project, dataset, name)
        if table is not None:
            self._store.drop(table["id"])


@contextmanager
def install(root: Path | str):
    """Route Orcapod's SpiralDB calls to the fake, with tables stored under ``root``."""
    with (
        patch.dict(os.environ, {_ROOT_ENV: str(root)}),
        patch("orcapod.databases.spiraldb_connector.sp", fake_module),
        patch("orcapod.databases.spiraldb_store_tables.sp", fake_module),
        patch("orcapod.databases.spiraldb_connector.require_sign_in", lambda: None),
        patch(
            "orcapod.databases.spiraldb_connector._http_get_json", fake_http_get_json
        ),
    ):
        yield


def migrate(
    project: str,
    schemas: dict[str, pa.Schema],
    dataset: str = "default",
    *,
    complete: bool = True,
) -> None:
    """Write a dataset's schema table as the migration tool does: a row per table, with
    ``schemas`` (by table name) where given, then the completion marker. Rows that are
    there for other tables are kept.

    Args:
        project: The project.
        schemas: Table name to the schema it was written with. A column SpiralDB
            reorders is recorded in SpiralDB's order.
        dataset: The dataset.
        complete: Whether to write the marker.
    """
    from orcapod.databases import spiraldb_schema_table as st
    from orcapod.utils import arrow_utils

    store = _store()
    handle = FakeProject(store, project).create_keyed_table(
        dataset, SCHEMA_TABLE, st.KEY_SCHEMA, exist_ok=True
    )
    stored = store.read(handle.resource_id, int(handle.version_id))
    by_id, _ = st.read_rows(stored.to_pylist() if stored.num_columns > 1 else [])
    for table in store.tables():
        if (table["project"], table["dataset"]) != (project, dataset):
            continue
        if table["name"] == SCHEMA_TABLE:
            continue
        found = _open(store, table, None).schema()
        given = schemas.get(table["name"])
        if given is None and table["id"] in by_id:
            continue
        fields = [
            _recorded_field(f, None if given is None else given.field(f.name))
            for f in found
        ]
        schema = pa.schema(
            [
                f.with_type(
                    arrow_utils.normalize_to_large_types(f.type, keep_null=True)
                )
                for f in fields
            ],
            metadata=cast(Any, None if given is None else given.metadata),
        )
        by_id[table["id"]] = st.SchemaRow(
            table["id"],
            table["name"],
            tuple(table["keys"]),
            schema.serialize().to_pybytes(),
            st.FROM_SERVICE if given is None else st.FROM_RECORD,
            max([0, *store.versions(table["id"])]),
        )
    rows = list(by_id.values())
    values = [row.stored() for row in rows]
    if complete:
        values.append(st.marker_values(st.Scope(project, dataset, "0" * 64), rows))
    written = [{"written_at": "", "written_by": "", **v} for v in values]
    handle.append(pa.Table.from_pylist(written, schema=st.ROW_SCHEMA))


def _recorded_field(found: pa.Field[Any], given: Any) -> pa.Field[Any]:
    """Return SpiralDB's field with the flags and metadata of the field as written."""
    if given is None:
        return found
    t: Any = found.type
    g: Any = given.type
    if isinstance(g, pa.BaseExtensionType):
        g = g.storage_type
    new = t
    if pa.types.is_struct(t) and pa.types.is_struct(g):
        new = pa.struct([_recorded_field(f, g.field(f.name)) for f in t])
    elif pa.types.is_fixed_size_list(t):
        new = pa.list_(_recorded_field(t.value_field, g.value_field), t.list_size)
    elif pa.types.is_list(t):
        new = pa.list_(_recorded_field(t.value_field, g.value_field))
    return pa.field(found.name, new, given.nullable, given.metadata)
