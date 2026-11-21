"""One form for the logical types of an Arrow schema that comes from outside (ITL-640).

A logical type's identifier is its extension name plus its extension metadata, however a
field carries it (CLAUDE.md, "Logical type identifiers"). Arrow carries it in two ways:
as a live ``pa.ExtensionType``, or as the storage type with the ``ARROW:extension:name``
and ``ARROW:extension:metadata`` field metadata (the IPC form: what a table read back
from a store, or passed through ``normalize_extension_columns``, holds; also what a
process has when the extension type is not registered with pyarrow). Every place where
an Arrow schema enters a schema identity, a saved schema or a declaration first brings
it to one form here, so the same identifier always means the same thing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from orcapod.logical_types.identifier import LogicalTypeIdentifierError
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

EXTENSION_NAME_KEY = b"ARROW:extension:name"
EXTENSION_METADATA_KEY = b"ARROW:extension:metadata"


def canonical_logical_schema(schema: "pa.Schema", type_converter: Any) -> "pa.Schema":
    """Return ``schema`` with every logical type in its live form.

    A field (at any depth: list items, fixed-size list items, struct children, map
    keys and items) whose identifier is carried in ``ARROW:extension:*`` field
    metadata becomes the live extension type the data context's converter registers
    for that identifier (``register_logical_type_from_arrow_metadata``: a registered
    type, or one a factory rebuilds from the identifier, such as a native container
    ``list[T]`` / ``set[T]`` or a structural class), and the two keys leave its
    metadata. The check is the converter's own: a known name with metadata it does not
    read, or with another storage, raises ``LogicalTypeIdentifierError``. An
    identifier no registered type or factory resolves (an exact user class this
    process has not imported) stays in the metadata form, which is how the live form
    of such a type is hashed and stored anyway. Fields with no identifier, and live
    extension types, are returned unchanged. Schema metadata is kept.

    Args:
        schema: The Arrow schema.
        type_converter: The data context's type converter.

    Returns:
        The schema in the canonical form; ``schema`` itself when nothing changes.

    Raises:
        LogicalTypeIdentifierError: If a field names a known logical type with
            metadata or storage that type does not read.
    """
    fields = [canonical_logical_field(field, type_converter) for field in schema]
    if all(new is old for new, old in zip(fields, schema)):
        return schema
    metadata = cast("dict[bytes | str, bytes | str] | None", schema.metadata)
    return pa.schema(fields, metadata=metadata)


def canonical_logical_field(
    field: "pa.Field[Any]", type_converter: Any
) -> "pa.Field[Any]":
    """Return ``field`` with its logical type, and its members', in the live form.

    See ``canonical_logical_schema``.

    Args:
        field: An Arrow field.
        type_converter: The data context's type converter.

    Returns:
        The field in the canonical form; ``field`` itself when nothing changes.
    """
    arrow_type = field.type
    if isinstance(arrow_type, pa.ExtensionType):
        return field
    storage = _canonical_type(arrow_type, type_converter)
    metadata = field.metadata or {}
    name = metadata.get(EXTENSION_NAME_KEY)
    if name is None:
        return field if storage is arrow_type else field.with_type(storage)
    try:
        live = type_converter.register_logical_type_from_arrow_metadata(
            name.decode("utf-8"), metadata.get(EXTENSION_METADATA_KEY, b""), storage
        )
    except LogicalTypeIdentifierError as exc:
        raise exc.for_column(field.name) from exc
    except ValueError:
        # No registered type and no factory for this identifier: it stays as stored.
        return field if storage is arrow_type else field.with_type(storage)
    rest: dict[bytes | str, bytes | str] = {
        key: value
        for key, value in metadata.items()
        if key not in (EXTENSION_NAME_KEY, EXTENSION_METADATA_KEY)
    }
    canonical = field.with_type(live).remove_metadata()
    return canonical.with_metadata(rest) if rest else canonical


def _canonical_type(arrow_type: "pa.DataType", type_converter: Any) -> "pa.DataType":
    """Return ``arrow_type`` with its members' logical types in the live form."""
    if pa.types.is_struct(arrow_type):
        children = [arrow_type.field(i) for i in range(arrow_type.num_fields)]
        new = [canonical_logical_field(child, type_converter) for child in children]
        if all(a is b for a, b in zip(new, children)):
            return arrow_type
        return pa.struct(new)
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        item = arrow_type.value_field
        new_item = canonical_logical_field(item, type_converter)
        if new_item is item:
            return arrow_type
        if pa.types.is_large_list(arrow_type):
            return pa.large_list(new_item)
        if pa.types.is_fixed_size_list(arrow_type):
            return pa.list_(new_item, arrow_type.list_size)
        return pa.list_(new_item)
    if pa.types.is_map(arrow_type):
        key = canonical_logical_field(arrow_type.key_field, type_converter)
        item = canonical_logical_field(arrow_type.item_field, type_converter)
        if key is arrow_type.key_field and item is arrow_type.item_field:
            return arrow_type
        return pa.map_(key, item, keys_sorted=arrow_type.keys_sorted)  # pyright: ignore[reportCallIssue, reportArgumentType]  # pyarrow takes fields for both; the stubs only types
    return arrow_type
