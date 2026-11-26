"""Names and metadata records of SpiralDB store tables (ITL-1495).

A store table's full path (the store's base path plus the record path) is too long for a
SpiralDB table name, so a table gets a short name computed from the path alone,
``o1_<hint>_<digest>``, and the full path is written into the table's metadata. A store
computes the name to find a table and never lists to find one; the metadata says which path
a table holds, and a table recording another path is refused.

- ``<digest>``: the SHA-256 of the UTF-8 canonical JSON array of the path components, as 64
  lower-case hex digits. It is a hash in a name, so it is bare: the label below names the
  algorithm and is never written into the name, whose ``o1_`` grammar fixes it.
- ``<hint>``: the components joined with ``/``, each character mapped to one character (a
  lower-case ASCII letter or digit to itself, an upper-case ASCII letter to its lower case,
  anything else to ``_``), cut to 96 characters. It is for people, and narrows a listing:
  the hint of a path starts with the hint of each of its prefixes. Nothing parses a name.

The specification is ``docs/dev/specs/itl-1446-round2-spiral-sdk.md`` (section 2). The module
is pure and imports no SpiralDB client.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from orcapod.errors import SpiralStoreTableCollisionError

SPIRAL_TABLE_NAME_LABEL = "spiral_table_name_v1"
"""The algorithm of ``store_table_digest``. A new algorithm is a new name grammar (``o2_``)."""

STORE_TABLE_NAME_PREFIX = "o1_"
"""The start of every store table's name; it versions the name grammar."""

HINT_LENGTH = 96
"""The most characters of a name's hint."""

STORE_FORMAT = "orcapod.spiral_store.v1"
"""The format of a store table, recorded in its metadata."""

MAX_PATH_JSON_BYTES = 8000
"""The longest recorded path, in UTF-8 bytes of its JSON. SpiralDB refuses an entry whose key
and value together are over 8,192 bytes; this leaves room for the key."""

MAX_METADATA_ENTRY_BYTES = 8192
"""The most UTF-8 bytes of a metadata entry's key and value together that SpiralDB accepts
(measured live with spiral-sdk 0.16.78)."""

STORE_KEY = "orcapod.store"
PATH_KEY = "orcapod.path"
LAYOUT_KEY = "orcapod.layout"
DATA_CONTEXT_KEY = "orcapod.data_context"
_RECORD_KEYS = (STORE_KEY, PATH_KEY, LAYOUT_KEY, DATA_CONTEXT_KEY)


def _path_json(path: Sequence[str]) -> bytes:
    """Return the UTF-8 canonical JSON array of ``path``'s components.

    Raises:
        ValueError: If the path is empty, a component is not a string, or a component
            cannot be written as UTF-8.
    """
    components = list(path)
    if not components:
        raise ValueError("a store table path has at least one component")
    if not all(type(c) is str for c in components):
        raise ValueError(f"a store table path holds strings only: {components!r}")
    try:
        return json.dumps(
            components, ensure_ascii=False, separators=(",", ":")
        ).encode()
    except UnicodeEncodeError as exc:
        raise ValueError(f"a store table path must be valid UTF-8: {exc}") from exc


def store_table_digest(path: Sequence[str]) -> str:
    """Return the bare hex digest of ``path`` that ends its table's name.

    Algorithm ``SPIRAL_TABLE_NAME_LABEL``: SHA-256 of the UTF-8 canonical JSON array of the
    components (no spaces, non-ASCII characters as they are).

    Args:
        path: The full path components.

    Raises:
        ValueError: If the path cannot be named (``_path_json``).
    """
    return hashlib.sha256(_path_json(path)).hexdigest()


def _hint_of(text: str) -> str:
    """Map each character of ``text`` to one name character, cut to ``HINT_LENGTH``."""
    return "".join(
        c.lower() if c.isascii() and c.isalnum() else "_" for c in text[:HINT_LENGTH]
    )


def table_name_hint(path: Sequence[str]) -> str:
    """Return the readable part of ``path``'s table name.

    Args:
        path: The full path components.
    """
    return _hint_of("/".join(path))


def store_table_name(path: Sequence[str]) -> str:
    """Return the SpiralDB name of the store table at ``path``.

    Args:
        path: The full path components: the store's base path, then the record path.

    Returns:
        ``o1_<hint>_<digest>``, at most 164 characters, all in ``[a-z0-9_]``.

    Raises:
        ValueError: If the path cannot be named (``_path_json``).
    """
    digest = store_table_digest(path)
    return f"{STORE_TABLE_NAME_PREFIX}{table_name_hint(path)}_{digest}"


def store_table_name_prefix(prefix: Sequence[str]) -> str:
    """Return the start of the name of every store table whose path starts with ``prefix``.

    Tables of other paths can share it: a listing narrowed by it is a superset, and each
    table's recorded path decides.

    Args:
        prefix: Leading full path components; empty for every store table.
    """
    if not prefix:
        return STORE_TABLE_NAME_PREFIX
    return STORE_TABLE_NAME_PREFIX + table_name_hint(prefix)


@dataclass(frozen=True)
class StoreTableRecord:
    """What a store table's metadata records.

    Attributes:
        path: The full path components.
        layout: The table's layout, the path's last component.
        data_context_key: The data context of the writer that created the table.
    """

    path: tuple[str, ...]
    layout: str
    data_context_key: str


def store_table_metadata(
    path: Sequence[str], data_context_key: str
) -> dict[str, bytes]:
    """Return the metadata a new store table at ``path`` is given.

    Args:
        path: The full path components.
        data_context_key: The data context of the writer creating the table.

    Raises:
        ValueError: If the path cannot be named or recorded (``MAX_PATH_JSON_BYTES``), or
            the data context key is empty.
    """
    path_json = _path_json(path)
    if len(path_json) > MAX_PATH_JSON_BYTES:
        size = len(path_json)
        raise ValueError(
            f"a store table path is at most {MAX_PATH_JSON_BYTES} bytes as JSON, not {size}"
        )
    if not data_context_key:
        raise ValueError("a store table records the data context that created it")
    record = {
        STORE_KEY: STORE_FORMAT.encode(),
        PATH_KEY: path_json,
        LAYOUT_KEY: path[-1].encode(),
        DATA_CONTEXT_KEY: data_context_key.encode(),
    }
    for key, value in record.items():
        size = len(key.encode()) + len(value)
        if size > MAX_METADATA_ENTRY_BYTES:
            limit = MAX_METADATA_ENTRY_BYTES
            raise ValueError(
                f"the metadata entry {key!r} is {size} bytes, over SpiralDB's {limit}"
            )
    return record


def read_store_table_metadata(metadata: Mapping[str, bytes]) -> StoreTableRecord | None:
    """Return the store table record in a table's metadata.

    Args:
        metadata: The table's metadata, as SpiralDB returns it.

    Returns:
        The record, or ``None`` when the table holds none of its entries: a table whose
        creation stopped before its metadata, or another program's.

    Raises:
        SpiralStoreTableCollisionError: If the record is partial, of another format, or
            unreadable.
    """
    present = [k for k in _RECORD_KEYS if k in metadata]
    if not present:
        return None
    missing = [k for k in _RECORD_KEYS if k not in metadata]
    if missing:
        raise SpiralStoreTableCollisionError(f"its record lacks {missing}")
    if metadata[STORE_KEY] != STORE_FORMAT.encode():
        raise SpiralStoreTableCollisionError(
            f"its format is {metadata[STORE_KEY]!r}, not {STORE_FORMAT!r}"
        )
    try:
        path: object = json.loads(metadata[PATH_KEY].decode())  # pyright: ignore[reportAny]  # checked below
        layout = metadata[LAYOUT_KEY].decode()
        data_context_key = metadata[DATA_CONTEXT_KEY].decode()
    except (ValueError, RecursionError) as exc:  # JSON, UTF-8, the integer limit, depth
        raise SpiralStoreTableCollisionError(
            f"its record cannot be read: {exc}"
        ) from exc
    items = cast("list[object]", path) if isinstance(path, list) else []
    if not items or not all(isinstance(c, str) for c in items):
        raise SpiralStoreTableCollisionError(
            f"its path is not a list of names: {path!r}"
        )
    components = tuple(cast("list[str]", items))
    try:
        _ = _path_json(components)  # what a writer refuses is refused on read too
    except ValueError as exc:
        raise SpiralStoreTableCollisionError(
            f"its path cannot be named: {exc}"
        ) from exc
    if layout != components[-1]:
        raise SpiralStoreTableCollisionError(
            f"its layout {layout!r} is not its path's last component {components[-1]!r}"
        )
    if not data_context_key:
        raise SpiralStoreTableCollisionError("it records no data context")
    return StoreTableRecord(components, layout, data_context_key)


def require_store_table_path(record: StoreTableRecord, path: Sequence[str]) -> None:
    """Refuse a store table whose record is of another path than ``path``.

    Args:
        record: The table's record (``read_store_table_metadata``).
        path: The full path the table was looked up for.

    Raises:
        SpiralStoreTableCollisionError: If the record is of another path.
    """
    if record.path != tuple(path):
        raise SpiralStoreTableCollisionError(
            f"it records path {list(record.path)}, not {list(path)}"
        )
