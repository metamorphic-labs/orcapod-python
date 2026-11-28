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
builds from a Python declaration (``require_no_narrow_float_keys``); source ingestion and explicit Arrow declarations apply it to an Arrow schema
(``require_schema_without_narrow_float_keys``).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from types import UnionType
from typing import TYPE_CHECKING, Literal, Union, cast, get_args, get_origin

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

_EXTENSION_NAME_KEY = b"ARROW:extension:name"
_EXTENSION_METADATA_KEY = b"ARROW:extension:metadata"


class NarrowFloatKeyError(InputValidationError, ValueError):
    """A declared type has a ``float32`` or ``float16`` dict key or set element."""


def require_supported_python_float_key(annotation: object, *, is_set: bool) -> None:
    """Report an unsupported NumPy half float in a key or set element declaration."""
    import numpy as np

    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in (Union, UnionType):
        non_none = [arg for arg in args if arg is not type(None)]
        if len(non_none) == 1:
            require_supported_python_float_key(non_none[0], is_set=is_set)
    if annotation is np.float16 or (
        origin is Literal
        and {type(value) for value in args if value is not None} == {np.float16}
    ):
        part = "set element" if is_set else "dict key"
        raise NarrowFloatKeyError(
            f"{NARROW_FLOAT_KEYS_NOT_SUPPORTED}: {part} type {annotation}. {_REMEDY}"
        )


def _is_narrow_float(arrow_type: pa.DataType) -> bool:
    import pyarrow as pa

    if isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    return pa.types.is_float32(arrow_type) or pa.types.is_float16(arrow_type)


def _native_container_metadata(
    name: bytes | None, raw: bytes | None
) -> dict[str, object]:
    """Read metadata only when the extension identifier names a native container."""
    if not name or not raw:
        return {}

    from orcapod.logical_types.list_logical_type_factory import (
        LIST_CATEGORY,
        SET_CATEGORY,
    )

    try:
        metadata: object = json.loads(raw)
    except ValueError:
        return {}
    if not isinstance(metadata, dict):
        return {}
    fields = cast("dict[str, object]", metadata)
    category = fields.get("category")
    if category not in (LIST_CATEGORY, SET_CATEGORY):
        return {}
    if not name.startswith(f"{category}[".encode()) or not name.endswith(b"]"):
        return {}
    return fields


def _dict_key(arrow_type: pa.DataType) -> str | None:
    """Describe a narrow float key in a plain dict's storage type."""
    import pyarrow as pa

    if pa.types.is_map(arrow_type):
        key_type = arrow_type.key_type
    elif (entry_fields := dict_entry_fields(arrow_type)) is not None:
        key_type = entry_fields[0].type
    else:
        return None
    return f"dict key type {key_type}" if _is_narrow_float(key_type) else None


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

    metadata = field_metadata or {}
    if isinstance(arrow_type, pa.BaseExtensionType):
        if isinstance(arrow_type, pa.ExtensionType):
            metadata = {
                _EXTENSION_NAME_KEY: arrow_type.extension_name.encode(),
                _EXTENSION_METADATA_KEY: arrow_type.__arrow_ext_serialize__(),
            }
        else:
            metadata = {}
        arrow_type = arrow_type.storage_type

    native = _native_container_metadata(
        metadata.get(_EXTENSION_NAME_KEY), metadata.get(_EXTENSION_METADATA_KEY)
    )
    if native and (pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type)):
        from orcapod.logical_types.list_logical_type_factory import SET_CATEGORY

        item = arrow_type.value_field
        found: set[str] = set()
        if native["category"] == SET_CATEGORY and _is_narrow_float(item.type):
            found.add(f"set element type {item.type}")
        element_name = native.get("element_ext_name")
        element_raw = native.get("element_ext_metadata")
        item_metadata = dict(item.metadata or {})
        if isinstance(element_name, str):
            item_metadata[_EXTENSION_NAME_KEY] = element_name.encode()
            item_metadata[_EXTENSION_METADATA_KEY] = (
                element_raw.encode() if isinstance(element_raw, str) else b""
            )
        found.update(narrow_float_keys(item.type, item_metadata))
        return sorted(found)

    if pa.types.is_dictionary(arrow_type):
        return narrow_float_keys(arrow_type.value_type)

    found: set[str] = set()
    if (description := _dict_key(arrow_type)) is not None:
        found.add(description)
    for index in range(arrow_type.num_fields):
        child = arrow_type.field(index)
        found.update(narrow_float_keys(child.type, child.metadata))
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

    Applied to a source's Arrow declaration and to explicitly declared mutations.
    Operator results and their caches can have the same Arrow shape as a dict,
    so their already-built schemas do not go through this check. No value is read.

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
