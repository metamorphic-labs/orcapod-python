"""Utilities shared across database implementations."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
import dataclasses
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.database_protocols import TableCommittingDatabaseProtocol
else:
    pa = LazyModule("pyarrow")


def coerce_record_id(record_id: str | bytes) -> bytes:
    """Encode ``record_id`` to bytes if it is a ``str``.

    All database implementations store record IDs as ``bytes``
    (``pa.large_binary()``).  This helper lets callers pass plain strings
    without the caller needing to know the encoding — UTF-8 is used, which
    is a lossless round-trip for any ASCII or text-based ID.

    Args:
        record_id: A record identifier as either ``bytes`` or a ``str``.

    Returns:
        The record identifier as ``bytes``.
    """
    if isinstance(record_id, str):
        return record_id.encode()
    return record_id


def as_table_committing(database: object) -> TableCommittingDatabaseProtocol | None:
    """Return ``database`` if it commits table by table, else ``None`` (ITL-919).

    The one check for ``TableCommittingDatabaseProtocol``, used by ``PipelineJob.run``
    (the ``batch_size`` rule) and by function nodes (results committed before their
    pipeline records). It looks the attributes up on the object, so a wrapper that
    forwards them only when the database it wraps has them (``ExtensionAwareDatabase``)
    qualifies exactly when that database does; ``isinstance`` on the protocol would
    not, because it inspects the class.

    Args:
        database: Any database object.

    Returns:
        ``database``, typed as the protocol, or ``None``.
    """
    try:
        batch_size = getattr(database, "batch_size")  # noqa: B009  # AttributeError means "no"
        flush_batch = getattr(database, "flush_batch")  # noqa: B009
    except AttributeError:
        return None
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        return None
    if not callable(flush_batch):
        return None
    return cast("TableCommittingDatabaseProtocol", database)


_IDENTIFIER = r'(?:"[^"]+"|\w+)'
_DIRECT_PROJECTION = re.compile(
    rf"^\s*SELECT\s+(?:\*|{_IDENTIFIER}(?:\s*,\s*{_IDENTIFIER})*)\s+"
    rf"FROM\s+{_IDENTIFIER}\s*(?P<rest>.*?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
# After the table: nothing, or a clause that only chooses or orders rows.
_ROW_CLAUSE = re.compile(r"^(?:$|(?:WHERE|ORDER\s+BY|LIMIT|OFFSET)\b)", re.IGNORECASE)
_NOT_DIRECT = re.compile(
    r"\b(?:JOIN|UNION|INTERSECT|EXCEPT|GROUP\s+BY|HAVING|SELECT)\b", re.IGNORECASE
)


def is_direct_projection(query: str) -> bool:
    """Whether ``query`` gives rows of one table's columns unchanged (ITL-1334).

    That is ``SELECT *`` or a list of plain column names ``FROM`` one table, followed
    by nothing but a ``WHERE``, ``ORDER BY``, ``LIMIT`` or ``OFFSET``. Only then does a
    result column have the stored column's nullability: an outer join, an aggregate,
    an expression or a union can give nulls in a column stored ``NOT NULL``. The test
    is conservative: a query it does not recognize reads every column nullable.

    Args:
        query: An SQL query.

    Returns:
        Whether every result column is a stored column as it is stored.
    """
    match = _DIRECT_PROJECTION.match(query)
    if match is None:
        return False
    rest = match.group("rest")
    return _ROW_CLAUSE.match(rest) is not None and _NOT_DIRECT.search(rest) is None


def metadata_bytes(metadata: Mapping[Any, Any] | None) -> dict[bytes, bytes]:
    """Field metadata as bytes keys and values (pyarrow gives bytes, a Delta log text)."""
    return {
        (k.encode() if isinstance(k, str) else k): (
            v.encode() if isinstance(v, str) else v
        )
        for k, v in (metadata or {}).items()
    }


def appended_field_change(
    held: "pa.Field[Any]", incoming: "pa.Field[Any]"
) -> str | None:
    """Say why rows of ``incoming`` would not read back as written, appended to a
    column a database holds as ``held`` (ITL-1334).

    A table holds one type per column, so an appended column must have the held
    column's field metadata and, at every depth, each nested field's name and metadata
    (a struct's children, matched by name; a list's item; a map's key and item) and a
    map's ``keys_sorted``: otherwise one side's rows read back with the other side's
    attributes. A struct field on one side only, and a difference in a type's kind or
    parameters, are left to the database's own append rule (struct evolution, a type
    conflict). Nullability, at any depth, is too: whether a nullability change is
    refused belongs to the nullability audit (ITL-842).

    Args:
        held: The column as the database holds it (stored or pending).
        incoming: The column of the rows to append.

    Returns:
        The reason, naming the path (``x``, ``x.a``, ``x[]``, ``x.key``), or ``None``.
    """
    if metadata_bytes(held.metadata) != metadata_bytes(incoming.metadata):
        return f"{incoming.name} has other field metadata than the stored column"
    return _nested_change(
        arrow_utils.normalize_to_large_types(held.type, keep_null=True),
        arrow_utils.normalize_to_large_types(incoming.type, keep_null=True),
        incoming.name,
    )


def _child_change(
    held: "pa.Field[Any]", incoming: "pa.Field[Any]", path: str
) -> str | None:
    if held.name != incoming.name:
        return f"{path} is named {incoming.name!r}, the stored column's {held.name!r}"
    if metadata_bytes(held.metadata) != metadata_bytes(incoming.metadata):
        return f"{path} has other field metadata than the stored column"
    return _nested_change(held.type, incoming.type, path)


def _nested_change(held: Any, incoming: Any, path: str) -> str | None:
    t = pa.types
    if t.is_struct(held) and t.is_struct(incoming):
        known = {f.name: f for f in held}
        for field in incoming:
            if field.name in known:
                found = _child_change(known[field.name], field, f"{path}.{field.name}")
                if found is not None:
                    return found
        return None
    if t.is_map(held) and t.is_map(incoming):
        if held.keys_sorted != incoming.keys_sorted:
            return (
                f"{path} has keys_sorted={incoming.keys_sorted}, the stored column "
                f"{held.keys_sorted}"
            )
        return _child_change(
            held.key_field, incoming.key_field, f"{path}.key"
        ) or _child_change(held.item_field, incoming.item_field, f"{path}.item")
    lists = (t.is_list, t.is_large_list, t.is_fixed_size_list)
    if any(k(held) for k in lists) and any(k(incoming) for k in lists):
        # cast: the type guards above narrow the types in the stubs' terms.
        return _child_change(
            cast(Any, held).value_field, cast(Any, incoming).value_field, f"{path}[]"
        )
    return None


def first_changed_column(
    held: "Iterable[pa.Schema]", schema: "pa.Schema"
) -> tuple[str, str] | None:
    """The first column of ``schema`` whose rows would not read back as written,
    appended where a database holds the tables ``held`` (``appended_field_change``).

    Args:
        held: The schemas the database holds for the table (stored, pending).
        schema: The schema of the rows to append.

    Returns:
        The column's name and the reason, or ``None``.
    """
    held = list(held)
    for field in schema:
        for given in held:
            index = given.get_field_index(field.name)
            if index < 0:
                continue
            reason = appended_field_change(given.field(index), field)
            if reason is not None:
                return field.name, reason
    return None


_EXTENSION_NAME = b"ARROW:extension:name"
_EXTENSION_METADATA = b"ARROW:extension:metadata"

# A logical type identifier as stored: the extension name and metadata, as bytes.
# Compared as bytes only; decoded only to word an error.
RawIdentifier = tuple[bytes, bytes]


@dataclass(frozen=True)
class IdentityNode:
    """What a database knows of the logical type identifier at one place in a column
    (ITL-1371): the column itself, or a nested field.

    Attributes:
        known: Whether ``identifier`` is reliably known. A held column read back
            through something that may rewrite or drop an identifier (a Delta list
            item, map key or item; see ``delta_identity_node``) is not known, and the
            rule abstains there.
        identifier: The raw identifier, or ``None`` for no logical type.
        claims: ``False`` for an untyped null (``pa.null()``) part: it holds no value
            and claims no type, so it is never another identifier.
        children: The nested places, by path label (``.name`` for a struct child,
            ``[]`` for a list item, ``.key`` and ``.item`` for a map, ``|name`` for a
            union member, ``{values}`` for dictionary values, ``<storage>`` for an
            extension type held directly as another's storage, ``.name`` for the
            other children of a nested type).
    """

    known: bool
    identifier: RawIdentifier | None
    claims: bool = True
    children: "dict[str, IdentityNode]" = dataclasses.field(default_factory=dict)


UNKNOWN = IdentityNode(known=False, identifier=None)


def _annotation(metadata: Mapping[Any, Any] | None) -> RawIdentifier | None:
    as_bytes = metadata_bytes(metadata)
    name = as_bytes.get(_EXTENSION_NAME)
    if name is None:
        return None
    return name, as_bytes.get(_EXTENSION_METADATA, b"")


def _extension_identifier(arrow_type: Any) -> RawIdentifier:
    """The raw identifier of an Arrow extension type."""
    if isinstance(arrow_type, pa.ExtensionType):
        return (
            arrow_type.extension_name.encode(),
            arrow_type.__arrow_ext_serialize__(),
        )
    # An extension type defined in Arrow's C++ library (``arrow.uuid``,
    # ``arrow.fixed_shape_tensor``) has no ``__arrow_ext_serialize__``: its name and
    # metadata are read from its C data interface export, as field metadata (arro3,
    # which ``deltalake`` depends on).
    from arro3.core import Field as Arro3Field

    exported = _annotation(Arro3Field.from_arrow(pa.field("x", arrow_type)).metadata)
    assert exported is not None  # an extension type exports its name
    return exported


def identity_node(arrow_field: "pa.Field[Any]") -> IdentityNode:
    """The identifiers an Arrow field carries, at every depth, in either form.

    A logical type reaches a database as an Arrow extension type, or as its storage
    type annotated with ``ARROW:extension:name`` / ``ARROW:extension:metadata`` (the
    form a stored column reads back in, and the one ``ExtensionAwareDatabase``
    passes on). Each level is one identifier: an annotation names the field's own
    type as storage; an extension type is unwrapped one level, and an extension
    type found as storage is a level of its own. The children of every nested Arrow
    type are enumerated from the type itself, so no container is a leaf.

    Args:
        arrow_field: A column, or a nested field, as given (or as a database holds
            it, when what it holds is the field itself).

    Returns:
        The field's ``IdentityNode``; every part known.
    """
    arrow_type = arrow_field.type
    if pa.types.is_null(arrow_type):
        return IdentityNode(known=True, identifier=None, claims=False)
    identifier = _annotation(arrow_field.metadata)
    if identifier is not None:
        storage = arrow_type
    elif isinstance(arrow_type, pa.BaseExtensionType):
        identifier = _extension_identifier(arrow_type)
        storage = arrow_type.storage_type
    else:
        storage = arrow_type
    return IdentityNode(
        known=True, identifier=identifier, children=_children_of(storage)
    )


def _children_of(arrow_type: Any) -> dict[str, IdentityNode]:
    t = pa.types
    if isinstance(arrow_type, pa.BaseExtensionType):
        return {"<storage>": identity_node(pa.field("storage", arrow_type))}
    if t.is_dictionary(arrow_type):
        return {"{values}": identity_node(pa.field("values", arrow_type.value_type))}
    if t.is_map(arrow_type):
        return {
            ".key": identity_node(arrow_type.key_field),
            ".item": identity_node(arrow_type.item_field),
        }
    if t.is_struct(arrow_type):
        return {f".{child.name}": identity_node(child) for child in arrow_type}
    if (
        t.is_list(arrow_type)
        or t.is_large_list(arrow_type)
        or t.is_fixed_size_list(arrow_type)
        or t.is_list_view(arrow_type)
        or t.is_large_list_view(arrow_type)
    ):
        return {"[]": identity_node(arrow_type.value_field)}
    prefix = "|" if t.is_union(arrow_type) else "."
    children = (arrow_type.field(i) for i in range(arrow_type.num_fields))
    return {f"{prefix}{child.name}": identity_node(child) for child in children}


def _identifier_text(identifier: RawIdentifier | None) -> str:
    if identifier is None:
        return "no logical type"
    name, metadata = identifier
    return repr(f"{name.decode('utf-8', 'replace')};{metadata.hex()}")


def logical_identity_change(
    held: IdentityNode, incoming: IdentityNode, path: str
) -> str | None:
    """Say why rows of ``incoming`` would be read back as another logical type than
    they were written as, appended where a database holds ``held`` (ITL-1371).

    A table holds one logical type per column and per nested field, so at every place
    both sides have, the appended identifier must be the held one; no logical type is
    an identifier too. The rule refuses only where it knows: wherever the held
    identifier is not reliably known, or a side is an untyped null, it abstains at
    that place, and a place only one side has (struct evolution, another kind of
    type) is left to the database's other append checks. It compares raw bytes,
    never Arrow type equality: pyarrow's equality of two instances of one Python
    extension class ignores their serialized metadata.

    Args:
        held: What the database holds (stored or pending).
        incoming: The rows to append, as given (``identity_node``).
        path: The column name, or the path of the nested place.

    Returns:
        The reason, naming the path and both identifiers' text, or ``None``.
    """
    if not (held.claims and incoming.claims):
        return None
    if held.known and incoming.known and held.identifier != incoming.identifier:
        return (
            f"{path} is of logical type {_identifier_text(incoming.identifier)}, the "
            f"stored column's {_identifier_text(held.identifier)}"
        )
    for label, child in incoming.children.items():
        held_child = held.children.get(label)
        if held_child is not None:
            found = logical_identity_change(held_child, child, f"{path}{label}")
            if found is not None:
                return found
    return None


def first_logical_identity_change(
    held: "Iterable[pa.Schema]", schema: "pa.Schema"
) -> tuple[str, str] | None:
    """The first column of ``schema`` that carries another logical type identifier
    than a column of that name the database holds, as given (``identity_node``).

    For a database that holds what it was given (in-memory, a pending batch, a
    connector's stored schema). Columns are matched by name; the order of either
    schema plays no part.

    Args:
        held: The schemas the database holds for the table (stored, pending).
        schema: The schema of the rows to append, as given.

    Returns:
        The column's name and the reason, or ``None``.
    """
    held = list(held)
    for given_field in schema:
        incoming = identity_node(given_field)
        for given in held:
            index = given.get_field_index(given_field.name)
            if index < 0:
                continue
            reason = logical_identity_change(
                identity_node(given.field(index)), incoming, given_field.name
            )
            if reason is not None:
                return given_field.name, reason
    return None
