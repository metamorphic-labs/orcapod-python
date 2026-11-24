"""The codec of an Orcapod store table in SpiralDB (ITL-1446).

Pure functions, with no pyspiral: they say how a table Orcapod stores is written to
SpiralDB and how it is read back. Nothing calls them yet. The design is
``docs/dev/specs/itl-1446-spiral-columns.md`` (sections 3.1 to 3.3).

* **Names.** ``encode_name`` gives a value column, or a struct field at any depth, a
  name SpiralDB accepts; ``decode_name`` is its inverse. The key column ``__record_id``
  keeps its name.
* **Types.** Rows are written in a *physical* form (``physical_schema``, ``encode_rows``)
  and restored to the *logical* one (``decode_rows``). A type either comes back exactly
  or is refused before anything is written. What only a live probe can decide is
  refused, and listed in ``AWAITING_LIVE_PROBE``.
* **The manifest.** The logical schema of the table, kept in the table's key-value
  metadata (``manifest_entries``, ``read_manifest``): entries are only ever added, never
  replaced or removed, and two manifests that disagree are refused.

Logical types are handled by their identifier (extension name and metadata) alone, as
``ARROW:extension:*`` field metadata over the storage type: neither writing nor reading
a manifest consults the types registered in the process.
``canonical_form.declaration_form_schema`` makes them live.

A SpiralDB store is a set of rows: the order of rows is not kept. An order-sensitive
step (``Batch``) over a stored source may therefore group differently than over a Delta
store.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from orcapod.databases.utils import identity_node
from orcapod.logical_types.canonical_form import (
    EXTENSION_METADATA_KEY,
    EXTENSION_NAME_KEY,
)
from orcapod.utils import arrow_utils

KEY_COLUMN = "__record_id"

# ------------------------------------------------------------------------ errors


class SpiralStoreError(ValueError):
    """A column, value, name or manifest the codec refuses.

    Attributes:
        reason: What is wrong.
        path: Where, from the column inwards: column name, struct field names and
            ``"[]"`` for a list's items. Empty when the error is not about a column.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        self.path: tuple[str, ...] = ()
        super().__init__(reason)

    def __str__(self) -> str:
        if not self.path:
            return self.reason
        where = repr(self.path[0])
        if len(self.path) > 1:
            where += f" (at {list(self.path[1:])})"
        return f"column {where}: {self.reason}"


class UnstorableInSpiralError(SpiralStoreError):
    """Rows or a schema SpiralDB would not give back exactly. Nothing was written."""


class StoredRowsError(SpiralStoreError):
    """Rows read from SpiralDB are not what the manifest says was written."""


class ManifestError(SpiralStoreError):
    """A manifest cannot be read in full, or cannot be written within the limits."""


class ManifestConflictError(ManifestError):
    """One table holds two manifests that disagree."""


@contextmanager
def _at(segment: str) -> Iterator[None]:
    """Add ``segment`` in front of the path of an error raised inside."""
    try:
        yield
    except SpiralStoreError as exc:
        exc.path = (segment, *exc.path)
        raise


# ------------------------------------------------------------------------- names

_MARK = "ox_"
_CONTROL = "\x00-\x1f\x7f"
_NOT_PLAIN = re.compile(f"[.$*{_CONTROL}]")
_ESCAPED = re.compile(f"[.$*;{_CONTROL}]")
_CODE = re.compile(";([0-9A-F]{2})")


def encode_name(name: str) -> str:
    """Return the stored name of a value column or a struct field.

    A plain name is stored as it is. Any other one (empty, starting with ``__`` or
    ``ox_``, or holding ``.``, ``$``, ``*`` or a control character) is ``ox_`` followed
    by the name with each of those characters, and ``;``, as ``;`` and two upper-case
    hex digits. Injective: ``decode_name`` is its inverse.

    Args:
        name: The Orcapod name.

    Returns:
        The stored name. It is never empty, never starts with ``__`` and holds none of
        the characters above.
    """
    if name and not name.startswith(("__", _MARK)) and not _NOT_PLAIN.search(name):
        return name
    return _MARK + _ESCAPED.sub(lambda m: f";{ord(m[0]):02X}", name)


def decode_name(stored: str) -> str:
    """Return the Orcapod name of a stored value column or struct field.

    Args:
        stored: A name ``encode_name`` gives.

    Returns:
        The Orcapod name.

    Raises:
        StoredRowsError: If ``encode_name`` never gives this name.
    """
    name = stored
    if stored.startswith(_MARK):
        name = _CODE.sub(lambda m: chr(int(m[1], 16)), stored[len(_MARK) :])
    if encode_name(name) != stored:
        raise StoredRowsError(f"{stored!r} is not a name Orcapod writes")
    return name


# -------------------------------------------------------------- types, as declared

AWAITING_LIVE_PROBE: Mapping[str, str] = {
    "float16": "a float16",
    "decimal256": "a decimal256",
    "date64": "a date64",
    "time": "a time of day",
    "map": "a map",
    "-0.0": "a negative zero",
    "NaN bits": "a NaN other than the canonical quiet one (its bits)",
}
"""What the design leaves to the live type matrix (probe P3), and so is refused for now."""


def _not_probed(what: str) -> UnstorableInSpiralError:
    """The refusal of an entry of ``AWAITING_LIVE_PROBE``."""
    return UnstorableInSpiralError(
        f"it is not known yet whether SpiralDB keeps {AWAITING_LIVE_PROBE[what]} exactly"
    )


def _without_identifier(field: pa.Field[Any]) -> dict[bytes, bytes]:
    """The field's metadata without the two keys that hold a logical type identifier."""
    metadata = cast("dict[bytes, bytes]", dict(field.metadata or {}))
    metadata.pop(EXTENSION_NAME_KEY, None)
    metadata.pop(EXTENSION_METADATA_KEY, None)
    return metadata


def _logical_field(field: pa.Field[Any]) -> pa.Field[Any]:
    """Return a column in the form the manifest records.

    Types are in their large variants. A logical type, live or annotated, is its
    storage type with its identifier as field metadata, the metadata key always
    present. A logical type below the top of the column is refused.
    """
    node = identity_node(field)
    if not node.known:
        raise UnstorableInSpiralError("its extension type cannot be identified")
    arrow_type = field.type
    if node.identifier is not None and EXTENSION_NAME_KEY not in (field.metadata or {}):
        arrow_type = cast(Any, arrow_type).storage_type
    _refuse_nested_identifier(arrow_type)
    metadata = _without_identifier(field)
    if node.identifier is not None:
        metadata[EXTENSION_NAME_KEY], metadata[EXTENSION_METADATA_KEY] = node.identifier
    return pa.field(
        field.name,
        arrow_utils.normalize_to_large_types(arrow_type, keep_null=True),
        field.nullable,
        cast(Any, metadata or None),
    )


def _refuse_nested_identifier(arrow_type: Any) -> None:
    """Refuse a logical type at or below ``arrow_type``, live or annotated."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        raise UnstorableInSpiralError("a logical type nested in another type")
    for child in _children(arrow_type):
        with _at(child.name if pa.types.is_struct(arrow_type) else "[]"):
            if EXTENSION_NAME_KEY in (child.metadata or {}):
                raise UnstorableInSpiralError("a logical type nested in another type")
            _refuse_nested_identifier(child.type)


def _children(arrow_type: Any) -> list[pa.Field[Any]]:
    """The child fields of a struct or a list; of other nested types, what they have."""
    return [arrow_type.field(i) for i in range(arrow_type.num_fields)]


def _unique(names: list[str]) -> None:
    """Refuse a repeated name among columns or struct fields."""
    if len(set(names)) != len(names):
        repeated = sorted({n for n in names if names.count(n) > 1})
        raise UnstorableInSpiralError(f"names used more than once: {repeated}")


def logical_schema(schema: pa.Schema) -> pa.Schema:
    """Return ``schema`` in the form the manifest records, or refuse it.

    Args:
        schema: The schema of rows to be stored, ``__record_id`` included.

    Returns:
        The schema with every type in its large variant and every logical type as its
        storage type with its identifier as field metadata. Names, column order,
        nullability and metadata are kept at every depth.

    Raises:
        UnstorableInSpiralError: If a column cannot be stored exactly, naming it, or if
            the table has no key column, no value column or a repeated name.
    """
    fields: list[pa.Field[Any]] = []
    for field in schema:
        with _at(field.name):
            logical = _logical_field(field)
            _ = _physical_type(logical.type)
            if field.name == KEY_COLUMN and (
                logical.type != pa.large_binary()
                or EXTENSION_NAME_KEY in (logical.metadata or {})
            ):
                raise UnstorableInSpiralError("the key column must be binary")
            fields.append(logical)
    _unique(schema.names)
    if KEY_COLUMN not in schema.names:
        raise UnstorableInSpiralError(f"no key column {KEY_COLUMN!r}")
    if len(fields) < 2:
        raise UnstorableInSpiralError("no column beside the key column")
    return pa.schema(fields, metadata=cast(Any, schema.metadata))


def _is_list(arrow_type: pa.DataType) -> bool:
    """Whether ``arrow_type`` is a variable-size list of either width."""
    return pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type)


def _physical_type(arrow_type: pa.DataType) -> pa.DataType:
    """Return the type a logical type (``_logical_field``) is written as, or refuse it."""
    t = pa.types
    logical: Any = arrow_type
    if t.is_timestamp(arrow_type):
        return pa.timestamp(logical.unit)
    if t.is_fixed_size_binary(arrow_type):
        return pa.large_binary()
    if t.is_duration(arrow_type):
        return pa.int64()
    if t.is_null(arrow_type):
        return pa.int8()
    if t.is_struct(arrow_type):
        fields = _children(arrow_type)
        if not fields:
            raise UnstorableInSpiralError("a struct with no fields")
        _unique([f.name for f in fields])
        physical: list[pa.Field[Any]] = []
        for field in fields:
            with _at(field.name):
                physical.append(
                    pa.field(encode_name(field.name), _physical_type(field.type))
                )
        return pa.struct(physical)
    if _is_list(arrow_type) or t.is_fixed_size_list(arrow_type):
        with _at("[]"):
            item = pa.field("item", _physical_type(logical.value_type))
        if t.is_fixed_size_list(arrow_type):
            return pa.list_(item, logical.list_size)
        return pa.large_list(item)
    if (
        t.is_boolean(arrow_type)
        or t.is_integer(arrow_type)
        or t.is_float32(arrow_type)
        or t.is_float64(arrow_type)
        or t.is_decimal128(arrow_type)
        or t.is_date32(arrow_type)
        or t.is_large_string(arrow_type)
        or t.is_large_binary(arrow_type)
    ):
        return arrow_type
    for what, matches in (
        ("float16", t.is_float16),
        ("decimal256", t.is_decimal256),
        ("date64", t.is_date64),
        ("time", t.is_time),
        ("map", t.is_map),
    ):
        if matches(arrow_type):
            raise _not_probed(what)
    raise UnstorableInSpiralError(f"SpiralDB stores no column of type {arrow_type}")


def physical_schema(schema: pa.Schema) -> pa.Schema:
    """Return the schema rows of ``schema`` are written with.

    Args:
        schema: The schema of rows to be stored.

    Returns:
        Stored names and physical types, in the order given. Every field is nullable
        and has no metadata.

    Raises:
        UnstorableInSpiralError: As ``logical_schema``.
    """
    return pa.schema(
        [
            pa.field(_stored_column_name(f.name), _physical_type(f.type))
            for f in logical_schema(schema)
        ]
    )


def _stored_column_name(name: str) -> str:
    """The stored name of a top-level column: the key column keeps its own."""
    return name if name == KEY_COLUMN else encode_name(name)


# ------------------------------------------------------------------ rows, to write


def encode_rows(rows: pa.Table) -> pa.Table:
    """Return ``rows`` as they are written to SpiralDB.

    Args:
        rows: The rows, ``__record_id`` included.

    Returns:
        The same rows under ``physical_schema(rows.schema)``.

    Raises:
        UnstorableInSpiralError: If the schema is refused (``logical_schema``), or a
            column holds a null under a non-null field, a null struct, or a value
            whose bits are awaiting a live probe. The column is named.
    """
    schema = logical_schema(rows.schema)
    physical = physical_schema(rows.schema)
    columns: list[Any] = []
    for field, stored, column in zip(schema, physical, rows.columns):
        with _at(field.name):
            chunks = [_encode(_storage(c), field) for c in column.chunks]
        columns.append(cast(Any, pa.chunked_array)(chunks, type=stored.type))
    return pa.Table.from_arrays(columns, schema=physical)


def _storage(array: Any) -> Any:
    """The storage of an extension array; any other array as it is."""
    return array.storage if isinstance(array, pa.ExtensionArray) else array


def _require_flag(array: Any, field: pa.Field[Any]) -> None:
    """Refuse a null under a non-null field."""
    if not field.nullable and array.null_count:
        raise UnstorableInSpiralError("a null in a non-nullable field")


def _encode(array: Any, field: pa.Field[Any]) -> Any:
    """Return ``array``, the values of ``field``, in its physical form.

    Nested types are rebuilt around their converted children, from the rows a reader
    sees: what a null list hides is not written.
    """
    _require_flag(array, field)
    logical: Any = field.type
    t = pa.types
    if t.is_struct(logical):
        if array.null_count:
            raise UnstorableInSpiralError("SpiralDB cannot store a null struct")
        children: list[Any] = []
        for i, child in enumerate(logical):
            with _at(child.name):
                children.append(_encode(array.field(i), child))
        return pa.StructArray.from_arrays(children, fields=list(_struct_of(logical)))
    if _is_list(logical):
        with _at("[]"):
            items = _encode(array.flatten(), logical.value_field)
        return _large_list(array, items, _physical_type(logical))
    if t.is_fixed_size_list(logical):
        if array.null_count and _expands_to_null_struct(logical.value_type):
            raise UnstorableInSpiralError(
                "a null fixed-size list of structs, which holds null structs"
            )
        with _at("[]"):
            items = _encode(array.flatten(), logical.value_field)
        return _fixed_size_list(array, items, _physical_type(logical))
    if t.is_floating(logical):
        _refuse_unprobed_floats(array)
    if t.is_null(logical):
        return pa.nulls(len(array), pa.int8())
    if t.is_timestamp(logical) or t.is_duration(logical):
        return array.view(_physical_type(logical))  # the same integers
    return array.cast(_physical_type(logical))


def _struct_of(logical: pa.DataType) -> list[pa.Field[Any]]:
    """The physical fields of a logical struct type."""
    return _children(_physical_type(logical))


def _expands_to_null_struct(item_type: Any) -> bool:
    """Whether a null fixed-size list of these items would need a null struct in it."""
    if pa.types.is_fixed_size_list(item_type):
        return _expands_to_null_struct(item_type.value_type)
    return pa.types.is_struct(item_type)


def _refuse_unprobed_floats(array: Any) -> None:
    """Refuse a negative zero, and a NaN that is not the canonical quiet one."""
    values = array.drop_null()
    if pa.types.is_float32(array.type):
        bits, zero, nan = values.view(pa.uint32()), 0x80000000, 0x7FC00000
    else:
        bits, zero, nan = values.view(pa.uint64()), 1 << 63, 0x7FF8000000000000
    raw = bits.to_numpy(zero_copy_only=False)
    if np.any(raw == zero):
        raise _not_probed("-0.0")
    if np.any(raw[np.isnan(values.to_numpy(zero_copy_only=False))] != nan):
        raise _not_probed("NaN bits")


# ------------------------------------------------------------- nested array builders


def _valid(array: Any) -> Any:
    """The rows of ``array`` that are not null, as a NumPy mask."""
    return array.is_valid().to_numpy(zero_copy_only=False)


def _large_list(array: Any, items: Any, target: Any) -> Any:
    """Rebuild a list array around new items, one per item of its non-null rows."""
    lengths = np.diff(array.offsets.to_numpy(zero_copy_only=False)) * _valid(array)
    offsets = np.concatenate([[0], np.cumsum(lengths)]).astype(np.int64)
    return pa.LargeListArray.from_arrays(
        pa.array(offsets, pa.int64()),
        items,
        type=target,
        mask=array.is_null() if array.null_count else None,
    )


def _fixed_size_list(array: Any, items: Any, target: Any) -> Any:
    """Rebuild a fixed-size list array around new items, one per item of its non-null
    rows; a null row's slots are filled with nulls, which no reader sees."""
    valid = _valid(array)
    if not valid.all():
        slots = np.repeat(valid, target.list_size)
        positions = np.cumsum(slots, dtype=np.int64) - 1
        items = items.take(pa.array(positions, pa.int64(), mask=~slots))
    # From buffers: a zero-width list keeps its row count, and a non-null item field
    # may have the null slots of a null row.
    return pa.Array.from_buffers(
        target,
        len(array),
        cast(Any, [None if valid.all() else pa.array(valid).buffers()[1]]),
        null_count=int((~valid).sum()),
        children=[items],
    )


# ------------------------------------------------------------------- rows, as read


def decode_rows(rows: pa.Table, schema: pa.Schema) -> pa.Table:
    """Return rows read from SpiralDB as the logical rows they were written from.

    Args:
        rows: Rows as read: stored names, columns and struct fields in any order.
        schema: The logical schema to give them: a manifest's, or the same schema with
            some of its logical types live.

    Returns:
        The rows under ``schema``, columns and struct fields in its order.

    Raises:
        StoredRowsError: If a column or struct field is missing, extra or of another
            type, or a value is not one that was written; the column is named.
    """
    recorded = logical_schema(schema)
    stored = [_stored_column_name(f.name) for f in recorded]
    _require_names(rows.schema.names, stored)
    columns: list[Any] = []
    for target, field, name in zip(schema, recorded, stored):
        with _at(target.name):
            chunks = [_decode(c, field) for c in rows.column(name).chunks]
            live = isinstance(target.type, pa.BaseExtensionType)
            storage: Any = target.type.storage_type if live else target.type
            if storage != field.type:
                chunks = [c.cast(storage) for c in chunks]
            if live:
                chunks = [
                    pa.ExtensionArray.from_storage(cast(Any, target.type), c)
                    for c in chunks
                ]
            columns.append(cast(Any, pa.chunked_array)(chunks, type=target.type))
    return pa.Table.from_arrays(columns, schema=schema)


def _require_names(found: list[str], expected: list[str]) -> None:
    """Refuse a set of stored names other than the expected one."""
    if sorted(found) != sorted(expected):
        missing = sorted(set(expected) - set(found))
        extra = sorted(set(found) - set(expected))
        repeated = sorted({n for n in found if found.count(n) > 1})
        raise StoredRowsError(
            f"stored names missing {missing}, not expected {extra}, repeated {repeated}"
        )


def _decode(array: Any, field: pa.Field[Any]) -> Any:
    """Return stored values as the logical values of ``field``."""
    logical: Any = field.type
    t = pa.types
    restored: Any
    if t.is_struct(logical):
        if not t.is_struct(array.type):
            raise _retyped(array, logical)
        if array.null_count:
            raise StoredRowsError("a null struct, which Orcapod never writes")
        names = [f.name for f in _children(array.type)]
        children_of = list(logical)
        _require_names(names, [encode_name(c.name) for c in children_of])
        children: list[Any] = []
        for child in children_of:
            with _at(child.name):
                stored = array.field(names.index(encode_name(child.name)))
                children.append(_decode(stored, child))
        restored = pa.StructArray.from_arrays(children, fields=children_of)
    elif _is_list(logical):
        if not _is_list(array.type):
            raise _retyped(array, logical)
        with _at("[]"):
            items = _decode(array.flatten(), logical.value_field)
        restored = _large_list(array, items, logical)
    elif t.is_fixed_size_list(logical):
        if (
            not t.is_fixed_size_list(array.type)
            or array.type.list_size != logical.list_size
        ):
            raise _retyped(array, logical)
        with _at("[]"):
            items = _decode(array.flatten(), logical.value_field)
        restored = _fixed_size_list(array, items, logical)
    else:
        restored = _decode_leaf(array, logical)
    if not field.nullable and restored.null_count:
        raise StoredRowsError("a null in a non-nullable field")
    return restored


def _decode_leaf(array: Any, logical: Any) -> Any:
    """Return stored values of a type with no children as ``logical`` values."""
    t = pa.types
    physical = _physical_type(logical)
    found = arrow_utils.normalize_to_large_types(array.type, keep_null=True)
    if found != physical:
        raise _retyped(array, logical)
    array = array.cast(physical)
    if t.is_null(logical):
        if array.null_count != len(array):
            raise StoredRowsError("a value in a column of nulls")
        return pa.nulls(len(array))
    if t.is_timestamp(logical) or t.is_duration(logical):
        return array.view(logical)  # the same integers
    if t.is_fixed_size_binary(logical):
        widths: Any = pc.binary_length(array.drop_null())
        if np.any(widths.to_numpy(zero_copy_only=False) != logical.byte_width):
            raise StoredRowsError(f"a value that is not {logical.byte_width} bytes")
        # Through Python bytes: pyarrow's cast checks the slots of null rows too.
        return pa.array(array.to_pylist(), logical)
    return array


def _retyped(array: Any, logical: pa.DataType) -> StoredRowsError:
    """The error of a column whose stored type is not the one written."""
    return StoredRowsError(
        f"stored as {array.type}, not as {_physical_type(logical)} ({logical})"
    )


# ---------------------------------------------------------------------- the manifest

MANIFEST_PREFIX = "__orcapod_store__/"
_FORMAT = "orcapod.spiral_store"
_VERSION = 1
# SpiralDB: 1,024 entries a table, 8 KiB a key and value together.
_MAX_ENTRIES = 1024
_MAX_PAIR_BYTES = 8192
# Part of format 1: two writers must cut one payload at the same places.
_CHUNK_BYTES = _MAX_PAIR_BYTES - 128
_KEY = re.compile(re.escape(MANIFEST_PREFIX) + "([0-9a-f]{64})(?:/(0|[1-9][0-9]*))?")
# In the recorded Arrow schema an ``ARROW:extension:*`` key is written behind this
# mark (and a key that starts with the mark too, so the marking is reversible), so
# reading it back never builds an extension type that happens to be registered.
_MARKED = b"orcapod:key:"
_EXTENSION_KEYS = b"ARROW:extension:"


@dataclass(frozen=True)
class StoreManifest:
    """What a store table's manifest says.

    Attributes:
        record_path: The table's full record path, each component exact.
        schema: The table's logical schema (``logical_schema``).
    """

    record_path: tuple[str, ...]
    schema: pa.Schema


def manifest_entries(
    manifest: StoreManifest,
) -> tuple[dict[str, bytes], dict[str, bytes]]:
    """Return the metadata entries that record ``manifest``.

    The two dictionaries are written in this order, one call each, so that a reader
    never sees a header whose chunks are missing. Chunks without a header are no
    manifest. A payload's keys are named by its digest, so writing never replaces
    another manifest's entry.

    Args:
        manifest: The manifest. Its schema may be in any form ``logical_schema`` takes.

    Returns:
        The chunk entries, and the header entry.

    Raises:
        UnstorableInSpiralError: If the schema cannot be stored.
        ManifestError: If the manifest does not fit in a table's metadata.
    """
    schema = logical_schema(manifest.schema)
    payload = json.dumps(
        {
            "record_path": list(manifest.record_path),
            "schema": base64.b64encode(
                _map_metadata(schema, _mark).serialize().to_pybytes()
            ).decode("ascii"),
            "version": _VERSION,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    digest = _digest(payload)
    pieces = [
        payload[i : i + _CHUNK_BYTES] for i in range(0, len(payload), _CHUNK_BYTES)
    ]
    if len(pieces) + 1 > _MAX_ENTRIES:
        raise ManifestError(
            f"the schema needs {len(pieces) + 1} metadata entries; SpiralDB holds "
            f"{_MAX_ENTRIES} a table"
        )
    header = {
        "chunks": len(pieces),
        "format": _FORMAT,
        "sha256": digest,
        "version": _VERSION,
    }
    return (
        {f"{MANIFEST_PREFIX}{digest}/{i}": piece for i, piece in enumerate(pieces)},
        {
            f"{MANIFEST_PREFIX}{digest}": json.dumps(
                header, separators=(",", ":"), sort_keys=True
            ).encode("ascii")
        },
    )


def read_manifest(metadata: Mapping[str, bytes]) -> StoreManifest | None:
    """Return the manifest in a table's key-value metadata.

    Args:
        metadata: All of the table's key-value metadata.

    Returns:
        The manifest, or ``None`` when the table holds none (it is not a store table).
        When it holds several that agree (``manifest_difference``), the one of the
        smallest digest, so every reader takes the same one.

    Raises:
        ManifestConflictError: If two manifests disagree.
        ManifestError: If a manifest cannot be read in full, is of an unknown format or
            version, or an entry under ``__orcapod_store__/`` is not one of a manifest.
    """
    digests: list[str] = []
    for key in metadata:
        if not key.startswith(MANIFEST_PREFIX):
            continue
        match = _KEY.fullmatch(key)
        if match is None:
            raise ManifestError(f"unknown manifest entry {key!r}")
        if match[2] is None:
            digests.append(match[1])
    digests.sort()
    manifests = [_read_one(metadata, digest) for digest in digests]
    for digest, other in zip(digests[1:], manifests[1:]):
        difference = manifest_difference(manifests[0], other)
        if difference is not None:
            raise ManifestConflictError(
                f"the table holds two manifests, {digests[0]} and {digest}, that "
                f"disagree: {difference}"
            )
    return manifests[0] if manifests else None


def _digest(payload: bytes) -> str:
    """The digest that names a manifest (bare hex, in a name)."""
    return hashlib.sha256(payload).hexdigest()


def _json_object(text: bytes) -> dict[str, Any]:
    """Parse a JSON object."""
    try:
        value = json.loads(text.decode("ascii"))
    except (ValueError, RecursionError) as exc:
        raise ManifestError(f"a manifest part is not JSON ({exc})") from exc
    if not isinstance(value, dict):
        raise ManifestError("a manifest part is not a JSON object")
    return cast("dict[str, Any]", value)


def _require_version(value: object) -> None:
    """Refuse a format version this Orcapod does not read."""
    if type(value) is not int or value != _VERSION:
        raise ManifestError(
            f"unsupported manifest version {value!r} (this Orcapod reads {_VERSION})"
        )


def _read_one(metadata: Mapping[str, bytes], digest: str) -> StoreManifest:
    """Read the manifest named by ``digest``."""
    header = _json_object(metadata[MANIFEST_PREFIX + digest])
    if header.get("format") != _FORMAT:
        raise ManifestError(f"unknown manifest format {header.get('format')!r}")
    _require_version(header.get("version"))
    count = header.get("chunks")
    if type(count) is not int or not 1 <= count < _MAX_ENTRIES:
        raise ManifestError(f"invalid chunk count {count!r}")
    if header.get("sha256") != digest:
        raise ManifestError(f"the header of {digest} names another digest")
    pieces: list[bytes] = []
    for i in range(count):
        piece = metadata.get(f"{MANIFEST_PREFIX}{digest}/{i}")
        if piece is None:
            raise ManifestError(f"chunk {i} of {count} of {digest} is missing")
        pieces.append(piece)
    payload = b"".join(pieces)
    if _digest(payload) != digest:
        raise ManifestError(f"the chunks of {digest} do not match it")
    body = _json_object(payload)
    _require_version(body.get("version"))
    path, text = body.get("record_path"), body.get("schema")
    if not isinstance(path, list) or not all(
        isinstance(part, str) for part in cast("list[object]", path)
    ):
        raise ManifestError("the record path is not a list of strings")
    if not isinstance(text, str):
        raise ManifestError("the schema is not text")
    try:
        schema = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(text, validate=True)))
    except (binascii.Error, ValueError, pa.ArrowException, OSError) as exc:
        raise ManifestError(f"the schema cannot be read ({exc})") from exc
    recorded = _map_metadata(schema, _unmark)
    try:
        usable = logical_schema(recorded)
    except UnstorableInSpiralError as exc:
        raise ManifestError(
            f"the recorded schema is not a store schema: {exc}"
        ) from exc
    return StoreManifest(tuple(cast("list[str]", path)), usable)


def _mark(key: bytes) -> bytes:
    """The form a field metadata key is recorded in."""
    return _MARKED + key if key.startswith((_MARKED, _EXTENSION_KEYS)) else key


def _unmark(key: bytes) -> bytes:
    """The inverse of ``_mark``."""
    return key.removeprefix(_MARKED)


def _map_metadata(schema: pa.Schema, change: Any) -> pa.Schema:
    """Return ``schema`` with every field metadata key, at every depth, changed."""
    return pa.schema(
        [_map_field(field, change) for field in schema],
        metadata=cast(Any, schema.metadata),
    )


def _map_field(field: pa.Field[Any], change: Any) -> pa.Field[Any]:
    """``_map_metadata`` for one field. Store types have struct and list children only."""
    arrow_type: Any = field.type
    if pa.types.is_struct(arrow_type):
        arrow_type = pa.struct([_map_field(f, change) for f in _children(arrow_type)])
    elif pa.types.is_large_list(arrow_type):
        arrow_type = pa.large_list(_map_field(arrow_type.value_field, change))
    elif pa.types.is_fixed_size_list(arrow_type):
        arrow_type = pa.list_(
            _map_field(arrow_type.value_field, change), arrow_type.list_size
        )
    metadata = {change(k): v for k, v in (field.metadata or {}).items()}
    return pa.field(field.name, arrow_type, field.nullable, cast(Any, metadata or None))


# ----------------------------------------------------------- comparing two manifests


def manifest_difference(a: StoreManifest, b: StoreManifest) -> str | None:
    """Return how two manifests disagree, or ``None`` when they agree.

    Compared as decoded, never as bytes: the record path, the schema metadata, and
    each column by name, with its type, nullability and metadata, field by field at
    every depth. The order of columns and of metadata entries does not count: one
    pipeline's table may be written with its columns in another order.

    Args:
        a: A manifest.
        b: Another one. Either schema may be in any form ``logical_schema`` takes.

    Returns:
        The first difference, in words.
    """
    if a.record_path != b.record_path:
        return f"record path {list(a.record_path)} and {list(b.record_path)}"
    left, right = logical_schema(a.schema), logical_schema(b.schema)
    if dict(left.metadata or {}) != dict(right.metadata or {}):
        return "the schema metadata differs"
    if set(left.names) != set(right.names):
        return f"columns {sorted(left.names)} and {sorted(right.names)}"
    for field in left:
        difference = _field_difference(field, right.field(field.name), repr(field.name))
        if difference is not None:
            return difference
    return None


def _field_difference(a: pa.Field[Any], b: pa.Field[Any], where: str) -> str | None:
    """How two fields differ. Arrow's own equality ignores nested names."""
    for what, x, y in (
        ("name", a.name, b.name),
        ("nullable", a.nullable, b.nullable),
        ("metadata", dict(a.metadata or {}), dict(b.metadata or {})),
        ("type", a.type.id, b.type.id),
        ("number of fields", a.type.num_fields, b.type.num_fields),
    ):
        if x != y:
            shown = (a.type, b.type) if what == "type" else (x, y)
            return f"{where}: {what} {shown[0]} and {shown[1]}"
    if not a.type.num_fields:
        return None if a.type == b.type else f"{where}: type {a.type} and {b.type}"
    if pa.types.is_fixed_size_list(a.type):
        sizes = (cast(Any, a.type).list_size, cast(Any, b.type).list_size)
        if sizes[0] != sizes[1]:
            return f"{where}: list size {sizes[0]} and {sizes[1]}"
    struct = pa.types.is_struct(a.type)
    for x, y in zip(_children(a.type), _children(b.type)):
        inner = f"{where}.{x.name}" if struct else f"{where}[]"
        difference = _field_difference(x, y, inner)
        if difference is not None:
            return difference
    return None
