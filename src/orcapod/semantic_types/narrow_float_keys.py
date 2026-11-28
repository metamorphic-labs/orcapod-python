"""No dict key and no set element is declared ``float32`` or ``float16`` (ITL-1591).

Developer decisions D16 and D25, November 28, 2026. A Python float stored under a
``float32`` or ``float16`` type is narrowed, and two distinct values can narrow to one
stored value (``1e-50`` and ``2e-50`` both become ``0.0``). For a plain value that is a
loss of precision; for the keys of a dict or the elements of a set it loses an entry,
because the container read back holds one of them. So a declared type whose dict key or
set element is ``float32`` or ``float16`` is refused where it is declared, in a Python
schema or annotation and in an Arrow schema or table alike. The refusal reads types
only, never a value.

What counts, at the top of a type and at any depth inside it (a list item, a struct
child, a dict or map key or value, a logical type's storage):

- the key of a dict: the type converter's dict shape (``nested_storage.dict_entry_fields``,
  a list of ``{key, value}`` structs) or an Arrow ``map``;
- the element of a set: the native ``set[T]`` container (a ``ListLogicalType`` of the
  set category), as a live extension type or as its storage with the identifier in
  ``ARROW:extension:*`` field metadata.

A ``float32`` or ``float16`` plain column, list element, struct field or dict VALUE is
accepted, and so is a ``float64`` key or element.

The rule is here once. The type converter applies it to every dict and set type it
builds from a Python declaration (``require_no_narrow_float_keys``); the source builder,
``ArrowTableStream`` and a datagram over an Arrow table apply it to an Arrow schema
(``require_schema_without_narrow_float_keys``).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING

from orcapod.errors import InputValidationError
from orcapod.semantic_types.nested_storage import dict_entry_fields

if TYPE_CHECKING:
    import pyarrow as pa

NARROW_FLOAT_KEYS_NOT_SUPPORTED = (
    "float32 and float16 are not supported as a dict key type or a set element type"
)
"""The start of every refusal of this rule."""

_REMEDY = (
    "A float stored under such a type is narrowed, so two distinct keys or elements can "
    "become one stored value and an entry is lost. Declare the key or element as float64 "
    "(float in Python, pa.float64() in Arrow), or use another type (an integer or a "
    "string)."
)

_EXTENSION_METADATA_KEY = b"ARROW:extension:metadata"


class NarrowFloatKeyError(InputValidationError, ValueError):
    """A declared type has a ``float32`` or ``float16`` dict key or set element."""


def _is_narrow_float(arrow_type: pa.DataType) -> bool:
    import pyarrow as pa

    if isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    return pa.types.is_float32(arrow_type) or pa.types.is_float16(arrow_type)


def _is_set_metadata(raw: bytes | None) -> bool:
    """Whether extension metadata ``raw`` is that of a native set container."""
    from orcapod.logical_types.list_logical_type_factory import SET_CATEGORY

    if not raw:
        return False
    try:
        metadata = json.loads(raw)
    except ValueError:
        return False
    return isinstance(metadata, dict) and metadata.get("category") == SET_CATEGORY


def _dict_key(
    arrow_type: pa.DataType, field_metadata: Mapping[bytes, bytes] | None
) -> str | None:
    """Say so when ``arrow_type`` stores a dict whose key is a narrow float."""
    import pyarrow as pa

    if pa.types.is_map(arrow_type):
        key_type = arrow_type.key_type
    elif (entry_fields := dict_entry_fields(arrow_type)) is not None:
        key_type = entry_fields[0].type
    else:
        return None
    return f"dict key type {key_type}" if _is_narrow_float(key_type) else None


def _set_element(
    arrow_type: pa.DataType, field_metadata: Mapping[bytes, bytes] | None
) -> str | None:
    """Say so when ``arrow_type`` stores a set whose element is a narrow float."""
    import pyarrow as pa

    if isinstance(arrow_type, pa.ExtensionType):
        raw = arrow_type.__arrow_ext_serialize__()
        storage = arrow_type.storage_type
    else:
        raw = (field_metadata or {}).get(_EXTENSION_METADATA_KEY)
        storage = arrow_type
    if not _is_set_metadata(raw):
        return None
    if not (pa.types.is_list(storage) or pa.types.is_large_list(storage)):
        return None
    element_type = storage.value_type
    if _is_narrow_float(element_type):
        return f"set element type {element_type}"
    return None


_REFUSED_PARTS = (_dict_key, _set_element)
"""The parts of a declared type that may not be a narrow float.

Each takes one Arrow type with the metadata of the field that holds it, at any depth of
a declared type, and returns the description of what it refuses there, or ``None``.
"""


def narrow_float_keys(
    arrow_type: pa.DataType, field_metadata: Mapping[bytes, bytes] | None = None
) -> list[str]:
    """Return what the rule refuses in ``arrow_type``, at any depth.

    Args:
        arrow_type: A declared Arrow type, or the Arrow type a declared Python type
            maps to.
        field_metadata: The metadata of the field that holds the type, when it has
            one: a set in its storage form is known by it.

    Returns:
        A description of each refused part (``"dict key type float"``,
        ``"set element type halffloat"``), sorted, each once; empty when the type is
        accepted.
    """
    import pyarrow as pa

    found = {
        description
        for part in _REFUSED_PARTS
        if (description := part(arrow_type, field_metadata)) is not None
    }
    children: list[tuple[pa.DataType, Mapping[bytes, bytes] | None]]
    if isinstance(arrow_type, pa.BaseExtensionType):
        children = [(arrow_type.storage_type, None)]
    elif (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        item = arrow_type.value_field
        children = [(item.type, item.metadata)]
    elif pa.types.is_struct(arrow_type):
        children = [(child.type, child.metadata) for child in arrow_type]
    elif pa.types.is_map(arrow_type):
        children = [
            (arrow_type.key_field.type, arrow_type.key_field.metadata),
            (arrow_type.item_field.type, arrow_type.item_field.metadata),
        ]
    else:
        children = []
    for child_type, child_metadata in children:
        found.update(narrow_float_keys(child_type, child_metadata))
    return sorted(found)


def require_no_narrow_float_keys(arrow_type: pa.DataType) -> None:
    """Refuse a declared type with a ``float32`` or ``float16`` dict key or set element.

    Args:
        arrow_type: A declared Arrow type, or the Arrow type a declared Python type
            maps to.

    Raises:
        NarrowFloatKeyError: If ``arrow_type`` is, or contains at any depth, a dict
            keyed by, or a set of, ``float32`` or ``float16``. The message names the
            type and each such part, and the remedy.
    """
    if found := narrow_float_keys(arrow_type):
        raise NarrowFloatKeyError(
            f"{NARROW_FLOAT_KEYS_NOT_SUPPORTED}: Arrow type {arrow_type} has "
            f"{', '.join(found)}. {_REMEDY}"
        )


def require_schema_without_narrow_float_keys(arrow_schema: pa.Schema) -> None:
    """Refuse an Arrow schema with a ``float32`` or ``float16`` dict key or set element.

    Applied to the table a source is built from, to the table of every stream and to
    the table a datagram is given, so a type declared in Arrow is refused as one
    declared in Python is. No value is read.

    Args:
        arrow_schema: The Arrow schema of the table.

    Raises:
        NarrowFloatKeyError: If a column's type is, or contains at any depth, a dict
            keyed by, or a set of, ``float32`` or ``float16``. The message names each
            such column, in name order, with its type and each such part, and the
            remedy.
    """
    lines = [
        f"  - column {field.name!r} has type {field.type} ({', '.join(found)})"
        for field in sorted(arrow_schema, key=lambda field: field.name)
        if (found := narrow_float_keys(field.type, field.metadata))
    ]
    if lines:
        raise NarrowFloatKeyError(
            f"{NARROW_FLOAT_KEYS_NOT_SUPPORTED}:\n" + "\n".join(lines) + f"\n{_REMEDY}"
        )
