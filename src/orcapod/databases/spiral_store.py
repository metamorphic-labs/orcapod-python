"""The codec of an Orcapod store table in SpiralDB (ITL-1446, ITL-1497).

Pure functions, with no SpiralDB client: they say how a table Orcapod stores is written to
SpiralDB and how it is read back. Nothing calls them yet. The design is
``docs/dev/specs/itl-1446-spiral-columns.md`` (PR 542, sections 3.1 to 3.3), as updated
for spiral-sdk by ``docs/dev/specs/itl-1446-round2-spiral-sdk.md`` (PR 588, section 3);
what SpiralDB keeps was measured live with spiral-sdk 0.16.78
(``docs/dev/plans/2026-11-26-itl-1497-spiral-store-codec.md``).

* **Names.** ``encode_name`` gives a value column, or a struct field at any depth, a
  name SpiralDB accepts. The key column ``__record_id`` keeps its name. Nothing decodes
  a name: a read looks for exactly the stored names the manifest lists.
* **Types.** Rows are written in a *physical* form (``physical_schema``, ``encode_rows``)
  and restored to the *logical* one (``decode_rows``). A type either comes back exactly
  or is refused. Every refusal of a type, a name or a value that may be lifted is an
  entry of one table, ``REFUSALS``.
* **The manifest.** One a table, written before its first rows; a table that still has
  no rows may be recorded again (stage 3) (``manifest_entries``, ``read_manifest``): the
  record path, the kind, the logical schema, and the stored names at every depth. It is
  kept in the table's own metadata, in chunks of at most 8 KiB, beside the table's identity
  record (``spiral_store_names``); never in the dataset's schema table, which is all or
  nothing under one completion marker (decision of 2026-11-26, ITL-1498).

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
from collections.abc import Callable, Iterator, Mapping
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
    """A manifest cannot be read, is of an unknown format or version, or is not the one
    asked for."""


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


def encode_name(name: str) -> str:
    """Return the stored name of a value column or a struct field.

    A plain name is stored as it is. Any other one (empty, starting with ``__`` or
    ``ox_``, or holding ``.``, ``$``, ``*`` or a control character) is ``ox_`` followed
    by the name with each of those characters, and ``;``, as ``;`` and two upper-case
    hex digits. Injective.

    Args:
        name: The Orcapod name.

    Returns:
        The stored name. It is never empty, never starts with ``__`` and holds none of
        the characters above.

    Raises:
        UnstorableInSpiralError: If the name is not UTF-8 text (a lone surrogate).
    """
    _require_utf8(name)
    if name and not name.startswith(("__", _MARK)) and not _NOT_PLAIN.search(name):
        return name
    return _MARK + _ESCAPED.sub(lambda m: f";{ord(m[0]):02X}", name)


def _require_utf8(text: str) -> None:
    """Refuse text with no UTF-8 form: a lone surrogate."""
    try:
        _ = text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise UnstorableInSpiralError(f"{text!r} is not UTF-8 text") from exc


# ------------------------------------------------------------- what is refused, and why


@dataclass(frozen=True)
class Refusal:
    """Something the store does not keep.

    Attributes:
        what: What it is.
        why: The evidence: what the service or its client was measured to do, or that
            it is not measured.
        matches: Whether an Arrow type is it; ``None`` when it is not about a type.
    """

    what: str
    why: str
    matches: Callable[[pa.DataType], bool] | None = None

    def error(self) -> UnstorableInSpiralError:
        """The refusal as an error."""
        return UnstorableInSpiralError(
            f"a SpiralDB store refuses {self.what} ({self.why})"
        )


_types = pa.types
_REFUSED_LIVE = "the service or its client refuses it"
NULL_STRUCT = Refusal("a null struct", _REFUSED_LIVE)
EMPTY_STRUCT = Refusal("a struct with no fields", _REFUSED_LIVE)
NO_ROWS = Refusal("a write of no rows", "it publishes nothing; the store sends none")
RENAMED_MAP = Refusal(
    "a map whose fields are not named key and value", "Arrow IPC renames them"
)
MAP_METADATA = Refusal("field metadata inside a map", "decided in the design")
VIEW = Refusal(
    "a string view or binary view", "Orcapod's tables hold the large variants"
)
REFUSALS: tuple[Refusal, ...] = (
    NULL_STRUCT,
    EMPTY_STRUCT,
    NO_ROWS,
    RENAMED_MAP,
    MAP_METADATA,
    VIEW,
    # Substitutes that held live, not used yet: large_binary, int64.
    Refusal("a fixed-size binary", _REFUSED_LIVE, _types.is_fixed_size_binary),
    Refusal("a duration", _REFUSED_LIVE, _types.is_duration),
    Refusal("an interval", _REFUSED_LIVE, _types.is_interval),
)
"""Every refusal that new evidence or a decision may lift. A type that is in neither this
table nor ``_NATIVE``, and that ``_physical_type`` does not map, is refused too."""

_NATIVE: tuple[Callable[[pa.DataType], bool], ...] = (
    _types.is_boolean,
    _types.is_integer,
    _types.is_float16,
    _types.is_float32,
    _types.is_float64,
    _types.is_decimal128,
    _types.is_decimal256,
    _types.is_date32,
    _types.is_date64,
    _types.is_null,
    _types.is_large_string,
    _types.is_large_binary,
    _types.is_timestamp,
)
"""Types written as they are (measured live, bit for bit: floats with their NaN payloads
and signed zeros; timestamps of every unit with their zone, ``datetime.max`` and
``datetime.min`` included; ``date64`` off midnight)."""


# -------------------------------------------------------------- types, as declared


def _without_identifier(field: pa.Field[Any]) -> dict[bytes, bytes]:
    """The field's metadata without the two keys that hold a logical type identifier."""
    metadata = dict(field.metadata or {})
    _ = metadata.pop(EXTENSION_NAME_KEY, None)
    _ = metadata.pop(EXTENSION_METADATA_KEY, None)
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
    if pa.types.is_dictionary(arrow_type):
        # An encoding, not a type: refused here, before a dictionary of a logical
        # type's values is taken for that type (``identity_node`` looks through it).
        raise UnstorableInSpiralError(f"a SpiralDB store keeps no type {arrow_type}")
    if node.identifier is not None and EXTENSION_NAME_KEY not in (field.metadata or {}):
        arrow_type = arrow_type.storage_type
    _refuse_nested_identifier(arrow_type)
    if _holds(arrow_type, _types.is_string_view) or _holds(
        arrow_type, _types.is_binary_view
    ):
        raise VIEW.error()
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
        name = child.name  # read also for a list's item, whose name must be text too
        with _at(name if pa.types.is_struct(arrow_type) else "[]"):
            if EXTENSION_NAME_KEY in (child.metadata or {}):
                raise UnstorableInSpiralError("a logical type nested in another type")
            _refuse_nested_identifier(child.type)


def _children(arrow_type: Any) -> list[pa.Field[Any]]:
    """The child fields of a struct or a list; of other nested types, what they have."""
    return [arrow_type.field(i) for i in range(arrow_type.num_fields)]


def _holds(arrow_type: Any, matches: Callable[[pa.DataType], bool]) -> bool:
    """Whether ``arrow_type`` is, or holds at any depth, a type that matches."""
    return matches(arrow_type) or any(
        _holds(child.type, matches) for child in _children(arrow_type)
    )


def _holds_metadata(arrow_type: Any) -> bool:
    """Whether a field below ``arrow_type``, at any depth, has metadata."""
    return any(f.metadata or _holds_metadata(f.type) for f in _children(arrow_type))


def _stored_names(names: list[str]) -> list[str]:
    """Return the stored names of siblings: columns, or the fields of one struct.

    SpiralDB folds neither case nor Unicode normal forms (measured live), so names that
    differ are different.
    """
    stored: list[str] = []
    for name in names:
        with _at(name):
            stored.append(encode_name(name))
    if len(set(stored)) != len(stored):
        repeated = sorted({n for n in names if names.count(n) > 1})
        raise UnstorableInSpiralError(f"names used more than once: {repeated}")
    return stored


def _stored_columns(names: list[str]) -> list[str]:
    """The stored names of a table's columns: the key column keeps its own.

    No encoded name starts with ``__``, so none is the key column's.
    """
    stored = iter(_stored_names([n for n in names if n != KEY_COLUMN]))
    return [n if n == KEY_COLUMN else next(stored) for n in names]


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
            the table has no single key column, no value column, a repeated name or a
            name that is not UTF-8.
    """
    try:
        return _logical_schema(schema)
    except UnicodeDecodeError as exc:
        # pyarrow holds a name of any bytes, and fails where it is read as text.
        raise UnstorableInSpiralError(f"a name is not UTF-8 text ({exc})") from exc


def _logical_schema(schema: pa.Schema) -> pa.Schema:
    """``logical_schema``, for names that can be read."""
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
    _ = _stored_columns(schema.names)
    if schema.names.count(KEY_COLUMN) != 1:
        raise UnstorableInSpiralError(f"not exactly one key column {KEY_COLUMN!r}")
    if len(fields) < 2:
        raise UnstorableInSpiralError("no column beside the key column")
    return pa.schema(fields, metadata=cast(Any, schema.metadata))


def _is_list(arrow_type: pa.DataType) -> bool:
    """Whether ``arrow_type`` is a variable-size list of either width."""
    return pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type)


def _physical_type(arrow_type: pa.DataType) -> pa.DataType:
    """Return the type a logical type (``_logical_field``) is written as, or refuse it.

    The one traversal of a type: struct fields and the items of every list kind, to any
    depth. A logical type's storage is traversed as any other type. Struct fields get
    encoded names; sibling names must differ. A struct is never null (``_encode``).
    """
    t = pa.types
    logical: Any = arrow_type
    if t.is_time64(arrow_type):
        # As integers: the service refuses a time of day outside one day, as a whole
        # write; Arrow holds such values.
        return pa.int64()
    if t.is_time32(arrow_type):
        return pa.int32()
    if t.is_decimal32(arrow_type) or t.is_decimal64(arrow_type):
        # What the service stores them as.
        return pa.decimal128(logical.precision, logical.scale)
    if t.is_struct(arrow_type):
        fields = _children(arrow_type)
        if not fields:
            raise EMPTY_STRUCT.error()
        physical: list[pa.Field[Any]] = []
        for field, stored in zip(fields, _stored_names([f.name for f in fields])):
            with _at(field.name):
                physical.append(pa.field(stored, _physical_type(field.type)))
        return pa.struct(physical)
    if _is_list(arrow_type):
        with _at("[]"):
            return pa.large_list(pa.field("item", _physical_type(logical.value_type)))
    if t.is_fixed_size_list(arrow_type):
        with _at("[]"):
            item = pa.field("item", _physical_type(logical.value_type))
        return pa.list_(item, logical.list_size)
    if t.is_map(arrow_type):
        key, item = logical.key_field, logical.item_field
        if (key.name, item.name) != ("key", "value"):
            raise RENAMED_MAP.error()
        if _holds_metadata(arrow_type):
            raise MAP_METADATA.error()
        with _at("[]"):
            # SpiralDB drops ``keys_sorted``: the manifest restores it.
            return pa.map_(_physical_type(key.type), _physical_type(item.type))
    for refusal in REFUSALS:
        if refusal.matches is not None and refusal.matches(arrow_type):
            raise refusal.error()
    if any(matches(arrow_type) for matches in _NATIVE):
        return arrow_type
    raise UnstorableInSpiralError(f"a SpiralDB store keeps no type {arrow_type}")


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
    logical = logical_schema(schema)
    return pa.schema(
        [
            pa.field(stored, _physical_type(f.type))
            for f, stored in zip(logical, _stored_columns(logical.names))
        ]
    )


# ------------------------------------------------------------------ rows, to write


def encode_rows(rows: pa.Table) -> pa.Table:
    """Return ``rows`` as they are written to SpiralDB.

    Args:
        rows: The rows, ``__record_id`` included.

    Returns:
        The same rows under ``physical_schema(rows.schema)``.

    Raises:
        UnstorableInSpiralError: If the schema is refused (``logical_schema``), or a
            column holds a null under a non-null field, a null struct, a refused value,
            or a null key, or there are no rows. The column is named. This is per batch: rows of earlier
            batches stay stored.
    """
    schema = logical_schema(rows.schema)
    physical = physical_schema(rows.schema)
    if not rows.num_rows:
        raise NO_ROWS.error()
    columns: list[Any] = []
    for field, stored, column in zip(schema, physical, rows.columns):
        with _at(field.name):
            if field.name == KEY_COLUMN and column.null_count:
                raise UnstorableInSpiralError("a null key")
            if field.name == KEY_COLUMN and pc.count_distinct(column).as_py() < len(
                column
            ):
                # The service refuses the whole write: refused before any table is made.
                raise UnstorableInSpiralError("a record id repeated in one write")
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
            raise NULL_STRUCT.error()
        children: list[Any] = []
        for i, child in enumerate(logical):
            with _at(child.name):
                children.append(_encode(array.field(i), child))
        fields = _children(_physical_type(logical))
        return pa.StructArray.from_arrays(children, fields=fields)
    if _is_list(logical):
        with _at("[]"):
            items = _encode(array.flatten(), logical.value_field)
        return _large_list(array, items, _physical_type(logical))
    if t.is_fixed_size_list(logical):
        with _at("[]"):
            items = _encode(array.flatten(), logical.value_field)
        return _fixed_size_list(array, items, _physical_type(logical))
    if t.is_map(logical):
        with _at("[]"):
            rows = _map_rows(array)
            keys = _encode(array.keys.take(rows), logical.key_field)
            items = _encode(array.items.take(rows), logical.item_field)
        return _map(array, keys, items, _physical_type(logical))
    if t.is_time(logical):
        return array.view(_physical_type(logical))  # the same integers
    return array.cast(_physical_type(logical))


# ------------------------------------------------------------- nested array builders


def _valid(array: Any) -> Any:
    """The rows of ``array`` that are not null, as a NumPy mask."""
    return array.is_valid().to_numpy(zero_copy_only=False)


def _lengths(array: Any) -> Any:
    """The number of items of each row of a list or map array; 0 of a null row."""
    return np.diff(array.offsets.to_numpy(zero_copy_only=False)) * _valid(array)


def _offsets(array: Any) -> Any:
    """Offsets from zero for the items of the non-null rows of a list or map array."""
    return np.concatenate([[0], np.cumsum(_lengths(array))])


def _map_rows(array: Any) -> Any:
    """Where the items of a map array's non-null rows are, in row order."""
    starts = array.offsets.to_numpy(zero_copy_only=False)[:-1]
    lengths, offsets = _lengths(array), _offsets(array)
    first = np.repeat(starts - offsets[:-1], lengths)
    return pa.array((first + np.arange(offsets[-1])).astype(np.int64))


def _map(array: Any, keys: Any, items: Any, target: Any) -> Any:
    """Rebuild a map array around new keys and items, of its non-null rows."""
    return pa.MapArray.from_arrays(
        pa.array(_offsets(array).astype(np.int32)),
        keys,
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


def _large_list(array: Any, items: Any, target: Any) -> Any:
    """Rebuild a list array around new items, one per item of its non-null rows."""
    offsets = _offsets(array).astype(np.int64)
    return pa.LargeListArray.from_arrays(
        pa.array(offsets, pa.int64()),
        items,
        type=target,
        mask=array.is_null() if array.null_count else None,
    )


# ------------------------------------------------------------------- rows, as read


def require_stored_schema(found: pa.Schema, manifest: StoreManifest) -> None:
    """Refuse a table whose stored columns are not exactly the ones the manifest lists.

    For rows as read, and for the first commit of a table read back.

    Args:
        found: The stored schema; columns and struct fields in any order, in small or
            large variants.
        manifest: The table's manifest.

    Raises:
        StoredRowsError: If a column or a struct field, at any depth, is missing, extra,
            repeated or of another type.
    """
    _require_names(found.names, manifest.physical.names)
    for field, physical in zip(manifest.schema, manifest.physical):
        with _at(field.name):
            _require_type(found.field(physical.name).type, field.type, physical.type)


def _require_names(found: list[str], expected: list[str]) -> None:
    """Refuse stored sibling names other than the expected ones."""
    if sorted(found) != sorted(expected):
        missing = sorted(set(expected) - set(found))
        extra = sorted(set(found) - set(expected))
        repeated = sorted({n for n in found if found.count(n) > 1})
        raise StoredRowsError(
            f"stored names missing {missing}, not expected {extra}, repeated {repeated}"
        )


def _require_type(found: Any, logical: Any, stored: Any) -> None:
    """Refuse a stored type other than ``stored``, under which ``logical`` is kept."""
    if pa.types.is_struct(stored) and pa.types.is_struct(found):
        _require_names(
            [f.name for f in _children(found)], [f.name for f in _children(stored)]
        )
        for field, kept in zip(_children(logical), _children(stored)):
            with _at(field.name):
                _require_type(found.field(kept.name).type, field.type, kept.type)
    elif _is_list(stored) and _is_list(found):
        with _at("[]"):
            _require_type(*(cast(Any, x).value_type for x in (found, logical, stored)))
    elif pa.types.is_fixed_size_list(stored) and pa.types.is_fixed_size_list(found):
        sizes = [cast(Any, x).list_size for x in (found, stored)]
        if sizes[0] != sizes[1]:
            raise StoredRowsError(f"stored as {found}, not as {stored} ({logical})")
        with _at("[]"):
            _require_type(*(cast(Any, x).value_type for x in (found, logical, stored)))
    elif pa.types.is_map(stored) and pa.types.is_map(found):
        with _at("[]"):
            for part in ("key_type", "item_type"):
                _require_type(*(getattr(x, part) for x in (found, logical, stored)))
    elif arrow_utils.normalize_to_large_types(found, keep_null=True) != stored:
        raise StoredRowsError(f"stored as {found}, not as {stored} ({logical})")


def decode_rows(
    rows: pa.Table, manifest: StoreManifest, schema: pa.Schema | None = None
) -> pa.Table:
    """Return rows read from SpiralDB as the logical rows they were written from.

    Args:
        rows: Rows as read: stored names, columns and struct fields in any order.
        manifest: The table's manifest. Columns are found by the stored names it lists.
        schema: The manifest's schema with some of its logical types live, to give the
            rows; the manifest's own when ``None``.

    Returns:
        The rows under the schema, columns and struct fields in its order.

    Raises:
        StoredRowsError: If the stored columns are not the manifest's
            (``require_stored_schema``), or a value is not one that was written; the
            column is named.
        ManifestError: If ``schema`` is not the manifest's.
    """
    target = manifest.schema if schema is None else schema
    try:
        difference = schema_difference(manifest.schema, target)
    except UnstorableInSpiralError as exc:
        raise ManifestError(f"not the recorded schema: {exc}") from exc
    if difference is not None or target.names != manifest.schema.names:
        raise ManifestError(f"not the recorded schema: {difference or 'column order'}")
    require_stored_schema(rows.schema, manifest)
    columns: list[Any] = []
    for out, field, physical in zip(target, manifest.schema, manifest.physical):
        with _at(field.name):
            column = rows.column(physical.name)
            if field.name == KEY_COLUMN and column.null_count:
                raise StoredRowsError("a null key")
            chunks = [_decode(c, field, physical) for c in column.chunks]
            live = isinstance(out.type, pa.BaseExtensionType)
            storage: Any = out.type.storage_type if live else out.type
            if storage != field.type:
                chunks = [_retyped(c, storage) for c in chunks]
            if live:
                chunks = [
                    pa.ExtensionArray.from_storage(cast(Any, out.type), c)
                    for c in chunks
                ]
            columns.append(cast(Any, pa.chunked_array)(chunks, type=out.type))
    return pa.Table.from_arrays(columns, schema=target)


def empty_rows(manifest: StoreManifest) -> pa.Table:
    """Return zero rows of the recorded schema.

    What a table with a manifest and no value columns reads as (a crash before its first
    rows). Such a table is never scanned: the service refuses a key-only scan.

    Args:
        manifest: The table's manifest.

    Returns:
        A table with no rows.
    """
    return manifest.schema.empty_table()


def _retyped(array: Any, target: Any) -> Any:
    """Return decoded values in another layout of their type (small or large variants).

    A type holding a map is rebuilt from its parts: pyarrow aborts the process when it
    casts a map between layouts. Any other type is cast.
    """
    t = pa.types
    if not _holds(target, t.is_map):
        return array.cast(target)
    mask = array.is_null() if array.null_count else None
    if t.is_struct(target):
        children = [
            _retyped(array.field(i), target.field(i).type)
            for i in range(target.num_fields)
        ]
        return pa.StructArray.from_arrays(children, fields=_children(target), mask=mask)
    if t.is_map(target):
        rows = _map_rows(array)
        keys = _retyped(array.keys.take(rows), target.key_type)
        items = _retyped(array.items.take(rows), target.item_type)
        return _map(array, keys, items, target)
    items = _retyped(array.flatten(), target.value_type)
    if t.is_fixed_size_list(target):
        return _fixed_size_list(array, items, target)
    if t.is_list(target):
        small_offsets = pa.array(_offsets(array).astype(np.int32), pa.int32())
        return pa.ListArray.from_arrays(small_offsets, items, type=target, mask=mask)
    return _large_list(array, items, target)


def _decode(array: Any, field: pa.Field[Any], physical: pa.Field[Any]) -> Any:
    """Return stored values of ``physical`` as the logical values of ``field``."""
    logical: Any = field.type
    stored: Any = physical.type
    t = pa.types
    restored: Any
    if t.is_struct(logical):
        if array.null_count:
            raise StoredRowsError("a null struct, which Orcapod never writes")
        names = [f.name for f in _children(array.type)]
        children: list[Any] = []
        for child, child_stored in zip(_children(logical), _children(stored)):
            with _at(child.name):
                values = array.field(names.index(child_stored.name))
                children.append(_decode(values, child, child_stored))
        restored = pa.StructArray.from_arrays(children, fields=_children(logical))
    elif _is_list(logical):
        with _at("[]"):
            items = _decode(array.flatten(), logical.value_field, stored.value_field)
        restored = _large_list(array, items, logical)
    elif t.is_fixed_size_list(logical):
        with _at("[]"):
            items = _decode(array.flatten(), logical.value_field, stored.value_field)
        restored = _fixed_size_list(array, items, logical)
    elif t.is_map(logical):
        with _at("[]"):
            rows = _map_rows(array)
            keys = array.keys.take(rows)
            keys = _decode(keys, logical.key_field, stored.key_field)
            items = array.items.take(rows)
            items = _decode(items, logical.item_field, stored.item_field)
        restored = _map(array, keys, items, logical)
    else:
        restored = array.cast(stored)
        if t.is_time(logical):
            restored = restored.view(logical)  # the same integers
        elif restored.type != logical:
            restored = restored.cast(logical)  # a decimal32 or decimal64
    if not field.nullable and restored.null_count:
        raise StoredRowsError("a null in a non-nullable field")
    return restored


# ---------------------------------------------------------------------- the manifest

# A draft: no table may be written with it outside tests. The persisted format is fixed
# with the creation rules (stage 3 of the design), as version 1.
MANIFEST_FORMAT = "orcapod.spiral_store.draft"
MANIFEST_VERSION = 0
# In the recorded Arrow schema an ``ARROW:extension:*`` key is written behind this
# mark (and a key that starts with the mark too, so the marking is reversible), so
# reading it back never builds an extension type that happens to be registered.
_MARKED = b"orcapod:key:"
_EXTENSION_KEYS = b"ARROW:extension:"


@dataclass(frozen=True)
class StoreManifest:
    """What a store table's manifest says. Make one with ``new_manifest``.

    Attributes:
        record_path: The table's full record path, each component exact.
        kind: What the table is, as text; the codec does not interpret it.
        schema: The table's logical schema (``logical_schema``).
        physical: The stored schema (``physical_schema``), field for field in the order
            of ``schema`` at every depth: it lists the stored names.
    """

    record_path: tuple[str, ...]
    kind: str
    schema: pa.Schema
    physical: pa.Schema


def new_manifest(
    record_path: tuple[str, ...], kind: str, schema: pa.Schema
) -> StoreManifest:
    """Return the manifest of a table to be created.

    Args:
        record_path: The table's full record path.
        kind: What the table is.
        schema: The schema of its rows.

    Returns:
        The manifest.

    Raises:
        UnstorableInSpiralError: If the schema cannot be stored, or a path component is
            not UTF-8 or holds ``/`` or NUL.
    """
    for part in record_path:
        _require_utf8(part)
        if "/" in part or "\0" in part:
            raise UnstorableInSpiralError(f"the path component {part!r}")
    _require_utf8(kind)
    return StoreManifest(
        record_path, kind, logical_schema(schema), physical_schema(schema)
    )


def manifest_bytes(manifest: StoreManifest) -> bytes:
    """Return the payload that records ``manifest``.

    ``manifest_entries`` cuts it into a table's metadata entries. Writing the same
    manifest again gives the same bytes. Two manifests whose schemas are equivalent but
    represented differently (field metadata in another insertion order) can give
    different bytes, because the Arrow IPC inside is not canonical: compare schemas with
    ``schema_difference``, never by these bytes.

    Args:
        manifest: The manifest.

    Returns:
        Canonical JSON: the format and its version, the kind, the record path, and both
        schemas as base64 of their Arrow IPC form.
    """
    return json.dumps(
        {
            "format": MANIFEST_FORMAT,
            "kind": manifest.kind,
            "physical": _schema_text(manifest.physical),
            "record_path": list(manifest.record_path),
            "schema": _schema_text(_map_metadata(manifest.schema, _mark)),
            "version": MANIFEST_VERSION,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _schema_text(schema: pa.Schema) -> str:
    """An Arrow schema as base64 of its IPC form."""
    return base64.b64encode(schema.serialize().to_pybytes()).decode("ascii")


def _schema_of(text: object) -> pa.Schema:
    """The inverse of ``_schema_text``."""
    if not isinstance(text, str):
        raise ManifestError("a schema is not text")
    try:
        raw = base64.b64decode(text, validate=True)
        return pa.ipc.read_schema(pa.py_buffer(raw))
    except (binascii.Error, ValueError, pa.ArrowException, OSError) as exc:
        raise ManifestError(f"a schema cannot be read ({exc})") from exc


def read_manifest_bytes(recorded: bytes, record_path: tuple[str, ...]) -> StoreManifest:
    """Return the manifest ``manifest_bytes`` recorded.

    Args:
        recorded: The payload, as reassembled from the chunks in the table's metadata
            (stage 3).
        record_path: The record path the table was looked up for.

    Returns:
        The manifest.

    Raises:
        ManifestError: If the bytes are not a manifest, of an unknown format or version,
            or of another path; or the recorded schemas are not a store table's.
    """
    try:
        body = json.loads(recorded.decode("ascii"))
    except (ValueError, RecursionError) as exc:
        raise ManifestError(f"a manifest is not JSON ({exc})") from exc
    if not isinstance(body, dict):
        raise ManifestError("a manifest is not a JSON object")
    fields = cast("dict[str, Any]", body)
    if fields.get("format") != MANIFEST_FORMAT:
        raise ManifestError(f"unknown manifest format {fields.get('format')!r}")
    version = fields.get("version")
    if type(version) is not int or version != MANIFEST_VERSION:
        raise ManifestError(
            f"unsupported manifest version {version!r} "
            f"(this Orcapod reads {MANIFEST_VERSION})"
        )
    path, kind = fields.get("record_path"), fields.get("kind")
    if not isinstance(path, list) or not all(
        isinstance(part, str) for part in cast("list[object]", path)
    ):
        raise ManifestError("the record path is not a list of strings")
    if not isinstance(kind, str):
        raise ManifestError("the kind is not text")
    if tuple(cast("list[str]", path)) != record_path:
        raise ManifestError(
            f"the table is recorded as {path}, not as {list(record_path)}"
        )
    try:
        schema = _map_metadata(_schema_of(fields.get("schema")), _unmark)
        physical = _schema_of(fields.get("physical"))
        usable = logical_schema(schema)
        expected = physical_schema(schema)
    except UnstorableInSpiralError as exc:
        raise ManifestError(
            f"the recorded schema is not a store schema: {exc}"
        ) from exc
    except UnicodeDecodeError as exc:
        # pyarrow holds a name of any bytes, and fails where it is read as text.
        raise ManifestError(f"a recorded name is not UTF-8 text ({exc})") from exc
    # The stored names are read from the manifest, never recomputed; its shape and types
    # must be the ones the logical schema is kept in.
    if _without_names(physical) != _without_names(expected) or (
        usable.names.index(KEY_COLUMN) != physical.get_field_index(KEY_COLUMN)
    ):
        raise ManifestError("the recorded stored schema is not the logical one's")
    try:
        _require_distinct(physical)
    except UnicodeDecodeError as exc:
        raise ManifestError(f"a recorded name is not UTF-8 text ({exc})") from exc
    return StoreManifest(record_path, kind, usable, physical)


MANIFEST_KEY = "orcapod.manifest"
"""The metadata key of a manifest's header; its chunks are ``<MANIFEST_KEY>/<i>``."""
# SpiralDB holds a key and its value in 8,192 bytes together (measured with spiral-sdk
# 0.16.78); a chunk leaves room for its key. Part of the format: one payload is always
# cut at the same places.
_CHUNK_BYTES = 8_000


def manifest_entries(manifest: StoreManifest) -> dict[str, bytes]:
    """Return the metadata entries that record ``manifest`` in its table.

    The chunks of the payload, then the header: the format, the version, the number of
    chunks and the SHA-256 of the payload, a checksum of what the chunks hold. Written in
    one upsert; a header always names exactly its chunks, so chunks of an earlier,
    interrupted manifest are never read.

    Args:
        manifest: The manifest.

    Returns:
        The entries, chunks first.
    """
    payload = manifest_bytes(manifest)
    pieces = [
        payload[i : i + _CHUNK_BYTES] for i in range(0, len(payload), _CHUNK_BYTES)
    ]
    header = {
        "chunks": len(pieces),
        "format": MANIFEST_FORMAT,
        "sha256": _checksum(payload),
        "version": MANIFEST_VERSION,
    }
    entries = {f"{MANIFEST_KEY}/{i}": piece for i, piece in enumerate(pieces)}
    entries[MANIFEST_KEY] = json.dumps(
        header, separators=(",", ":"), sort_keys=True
    ).encode("ascii")
    return entries


def read_manifest(
    metadata: Mapping[str, bytes], record_path: tuple[str, ...]
) -> StoreManifest | None:
    """Return the manifest in a table's metadata.

    Args:
        metadata: All of the table's metadata.
        record_path: The record path the table was looked up for.

    Returns:
        The manifest, or ``None`` when the table holds no header (a creation that
        stopped before its manifest).

    Raises:
        ManifestError: If the header or its chunks cannot be read in full, the header
            is of another format or version, the chunks do not match their checksum,
            or the manifest is not one of ``read_manifest_bytes``.
    """
    raw = metadata.get(MANIFEST_KEY)
    if raw is None:
        return None
    try:
        header = json.loads(raw.decode("ascii"))
    except (ValueError, RecursionError) as exc:
        raise ManifestError(f"a manifest header is not JSON ({exc})") from exc
    if not isinstance(header, dict):
        raise ManifestError("a manifest header is not a JSON object")
    fields = cast("dict[str, Any]", header)
    if fields.get("format") != MANIFEST_FORMAT:
        raise ManifestError(f"unknown manifest format {fields.get('format')!r}")
    version = fields.get("version")
    if type(version) is not int or version != MANIFEST_VERSION:
        raise ManifestError(
            f"unsupported manifest header version {version!r} "
            f"(this Orcapod reads {MANIFEST_VERSION})"
        )
    count = fields.get("chunks")
    if type(count) is not int or count < 1:
        raise ManifestError(f"invalid chunk count {count!r}")
    if count > len(metadata):
        # More chunks than the metadata has entries: some must be missing. Refused
        # before a key is listed for each, so a corrupt count allocates nothing.
        raise ManifestError(
            f"the manifest names {count} chunks; the metadata holds "
            f"{len(metadata)} entries"
        )
    keys = [f"{MANIFEST_KEY}/{i}" for i in range(count)]
    missing = [k for k in keys if k not in metadata]
    if missing:
        raise ManifestError(f"the manifest's chunks {missing} are missing")
    payload = b"".join(metadata[k] for k in keys)
    if fields.get("sha256") != _checksum(payload):
        raise ManifestError("the manifest's chunks do not match its checksum")
    return read_manifest_bytes(payload, record_path)


def _checksum(payload: bytes) -> str:
    """The SHA-256 of a manifest payload, as hex: the header's check of its chunks."""
    return hashlib.sha256(payload).hexdigest()


def _without_names(fields: Any) -> list[object]:
    """The types of fields at every depth, without names."""
    shape: list[object] = []
    for field in fields:
        arrow_type = field.type
        inner = _children(arrow_type)
        size = getattr(arrow_type, "list_size", None)
        shape.append(
            (arrow_type.id, size, _without_names(inner)) if inner else arrow_type
        )
    return shape


def _require_distinct(fields: Any) -> None:
    """Refuse a recorded stored schema with two siblings of one name."""
    names = [field.name for field in fields]
    if len(set(names)) != len(names):
        raise ManifestError("the recorded stored schema repeats a name")
    for field in fields:
        _require_distinct(_children(field.type))


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
    """``_map_metadata`` for one field. A map has no metadata inside."""
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


# ------------------------------------------------- comparing a schema with a manifest's


def schema_difference(recorded: pa.Schema, other: pa.Schema) -> str | None:
    """Return how a schema differs from a recorded one, or ``None`` when it is the same.

    Compared as decoded, never as bytes: the schema metadata, and each column by name,
    with its type, nullability and metadata, field by field at every depth. The order of
    columns and of metadata entries does not count: one pipeline's table may be written
    with its columns in another order.

    Args:
        recorded: A manifest's schema.
        other: Another one, in any form ``logical_schema`` takes.

    Returns:
        The first difference, in words.

    Raises:
        UnstorableInSpiralError: If a schema cannot be stored.
    """
    left, right = logical_schema(recorded), logical_schema(other)
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
    for what in ("list_size", "keys_sorted"):
        x, y = (getattr(f.type, what, None) for f in (a, b))
        if x != y:
            return f"{where}: {what} {x} and {y}"
    struct = pa.types.is_struct(a.type)
    for x, y in zip(_children(a.type), _children(b.type)):
        inner = f"{where}.{x.name}" if struct else f"{where}[]"
        difference = _field_difference(x, y, inner)
        if difference is not None:
            return difference
    return None
