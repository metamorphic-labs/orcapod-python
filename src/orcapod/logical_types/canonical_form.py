"""The two forms of the logical types of an Arrow schema (ITL-640).

A logical type's identifier is its extension name plus its extension metadata, however a
field carries it (CLAUDE.md, "Logical type identifiers"). Arrow carries it in two ways:
as a live ``pa.ExtensionType``, or as the storage type with the ``ARROW:extension:name``
and ``ARROW:extension:metadata`` field metadata (the storage, or IPC, form: what a table
read back from a store, or passed through ``normalize_extension_columns``, holds; also
what a process has when the extension type is not registered with pyarrow).

One walker, ``storage_form_schema``, brings every extension type, at any depth, to the
storage form. It needs no registry, and it is the form of every identity:

- ``StarfixArrowHasher.hash_schema`` applies it first, so every schema hash (a source's
  identity, system-tag column names, a function's output schema, and the per-column
  comparisons of a bind, a polling batch and a derived source's stored read, which
  compare ``hash_schema`` of one field) sees one form;
- the saved-schema writer (``pipeline.serialization.arrow_type_texts``) writes the
  storage form, so a saved type never depends on the form it came in.

A declaration (an Arrow schema given to a ``SourceNode`` or by a ``PollingSource``
implementation, and a saved schema read back, ``arrow_schema_from_texts``) goes through
``declaration_form_schema``: the storage form everywhere, except that a logical type at
the top of a column becomes the live type, because that is what a source holds and what
gives the column its Python type. Arrow cannot build an empty table with an extension
type inside a list, struct or map, so a nested identifier is never made live.
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


def declaration_form_schema(schema: "pa.Schema", type_converter: Any) -> "pa.Schema":
    """Return ``schema`` in the form a declared schema takes (ITL-640).

    Every field goes through ``declaration_form_field``. Schema metadata is kept.

    Args:
        schema: The declared Arrow schema.
        type_converter: The data context's type converter.

    Returns:
        The schema in the declaration form; ``schema`` itself when nothing changes.

    Raises:
        LogicalTypeIdentifierError: If a column names a known logical type with
            metadata or storage that type does not read, naming the column.
    """
    fields = [declaration_form_field(field, type_converter) for field in schema]
    if all(new is old for new, old in zip(fields, schema)):
        return schema
    metadata = cast("dict[bytes | str, bytes | str] | None", schema.metadata)
    return pa.schema(fields, metadata=metadata)


def declaration_form_field(
    field: "pa.Field[Any]", type_converter: Any
) -> "pa.Field[Any]":
    """Return a declared column with its logical types in the declaration form.

    A live extension type at the top of the column is kept as it is. Anything else
    is first brought to the storage form (``storage_form_field``): a logical type
    inside a list, large list, fixed-size list, struct or map, live or not, becomes
    its storage type with its identifier as field metadata, which Arrow can build
    and which keeps the identifier. Then an identifier at the top of the column
    becomes the live type the data context's converter registers for it
    (``register_logical_type_from_arrow_metadata``: a registered type, or one a
    factory rebuilds from the identifier, such as a native container or a structural
    class), and the two keys leave the field's metadata. The check is the
    converter's own: a known name with metadata it does not read, or with another
    storage, raises. An identifier no registered type or factory resolves (an exact
    user class this process has not imported) stays in the storage form, which is
    how such a type is hashed and stored anyway.

    Args:
        field: A declared Arrow field.
        type_converter: The data context's type converter.

    Returns:
        The field in the declaration form; ``field`` itself when nothing changes.

    Raises:
        LogicalTypeIdentifierError: If the column names a known logical type with
            metadata or storage that type does not read, naming the column.
    """
    if isinstance(field.type, pa.ExtensionType):
        return field
    stored = storage_form_field(field)
    metadata = stored.metadata or {}
    name = metadata.get(EXTENSION_NAME_KEY)
    if name is None:
        return stored
    try:
        live = type_converter.register_logical_type_from_arrow_metadata(
            name.decode("utf-8"), metadata.get(EXTENSION_METADATA_KEY, b""), stored.type
        )
    except LogicalTypeIdentifierError as exc:
        raise exc.for_column(field.name) from exc
    except ValueError:
        # No registered type and no factory for this identifier: it stays as stored.
        return stored
    rest: dict[bytes | str, bytes | str] = {
        key: value
        for key, value in metadata.items()
        if key not in (EXTENSION_NAME_KEY, EXTENSION_METADATA_KEY)
    }
    declared = stored.with_type(live).remove_metadata()
    return declared.with_metadata(rest) if rest else declared


def storage_form_schema(schema: "pa.Schema") -> "pa.Schema":
    """Return ``schema`` with every logical type in its storage form (ITL-640).

    The form of every identity (developer, November 21, 2026, option (a)): the form
    ``hash_schema`` hashes and the saved-schema writer writes:
    every extension type, at any depth (list, large list and fixed-size list items,
    struct children, map keys and items), becomes its storage type with its identifier
    as the ``ARROW:extension:name`` and ``ARROW:extension:metadata`` field metadata. A
    field already in that form (a table read back from a store, a type pyarrow has not
    registered) is kept as it is, with its members brought to the same form. It needs
    no registry: the identifier is read from the type or the field, never resolved.

    Args:
        schema: The Arrow schema.

    Returns:
        The schema in the storage form; ``schema`` itself when nothing changes.
    """
    fields = [storage_form_field(field) for field in schema]
    if all(new is old for new, old in zip(fields, schema)):
        return schema
    metadata = cast("dict[bytes | str, bytes | str] | None", schema.metadata)
    return pa.schema(fields, metadata=metadata)


def storage_form_field(field: "pa.Field[Any]") -> "pa.Field[Any]":
    """Return ``field`` with its logical type, and its members', in the storage form.

    See ``storage_form_schema``.

    Args:
        field: An Arrow field.

    Returns:
        The field in the storage form; ``field`` itself when nothing changes.
    """
    arrow_type = field.type
    if not isinstance(arrow_type, pa.ExtensionType):
        storage = _storage_form_type(arrow_type)
        return field if storage is arrow_type else field.with_type(storage)
    from orcapod.logical_types.identifier import arrow_type_identifier

    identifier = arrow_type_identifier(arrow_type)
    metadata = cast("dict[bytes | str, bytes | str]", dict(field.metadata or {}))
    metadata[EXTENSION_NAME_KEY] = identifier.name.encode("utf-8")
    metadata[EXTENSION_METADATA_KEY] = identifier.metadata
    return field.with_type(_storage_form_type(arrow_type.storage_type)).with_metadata(
        metadata
    )


def _storage_form_type(arrow_type: "pa.DataType") -> "pa.DataType":
    """Return ``arrow_type`` with its members' logical types in the storage form."""
    if isinstance(arrow_type, pa.ExtensionType):
        return _storage_form_type(arrow_type.storage_type)
    if pa.types.is_struct(arrow_type):
        children = [arrow_type.field(i) for i in range(arrow_type.num_fields)]
        new = [storage_form_field(child) for child in children]
        if all(a is b for a, b in zip(new, children)):
            return arrow_type
        return pa.struct(new)
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        item = arrow_type.value_field
        new_item = storage_form_field(item)
        if new_item is item:
            return arrow_type
        if pa.types.is_large_list(arrow_type):
            return pa.large_list(new_item)
        if pa.types.is_fixed_size_list(arrow_type):
            return pa.list_(new_item, arrow_type.list_size)
        return pa.list_(new_item)
    if pa.types.is_map(arrow_type):
        key = storage_form_field(arrow_type.key_field)
        item = storage_form_field(arrow_type.item_field)
        if key is arrow_type.key_field and item is arrow_type.item_field:
            return arrow_type
        return pa.map_(key, item, keys_sorted=arrow_type.keys_sorted)  # pyright: ignore[reportCallIssue, reportArgumentType]  # pyarrow takes fields for both; the stubs only types
    return arrow_type
