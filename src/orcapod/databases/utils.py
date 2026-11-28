"""Utilities shared across database implementations."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, cast

import pyarrow as pa

from orcapod.utils import arrow_utils

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import TableCommittingDatabaseProtocol

H = TypeVar("H")
G = TypeVar("G")


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


_IDENTIFIER = r'(?:"(?:[^"]|"")+"|\w+)'
_RELATION = rf"(?:(?P<schema>{_IDENTIFIER})\s*\.\s*)?(?P<table>{_IDENTIFIER})"
SQL_FROM_TABLE = re.compile(rf"\bFROM\s+{_RELATION}", re.IGNORECASE)
_DIRECT_PROJECTION = re.compile(
    rf"^\s*SELECT\s+(?:\*|{_IDENTIFIER}(?:\s*,\s*{_IDENTIFIER})*)\s+"
    rf"FROM\s+{_RELATION}\s*(?P<rest>.*?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
# After the table: nothing, or a clause that only chooses or orders rows.
_ROW_CLAUSE = re.compile(r"^(?:$|(?:WHERE|ORDER\s+BY|LIMIT|OFFSET)\b)", re.IGNORECASE)
_NOT_DIRECT = re.compile(
    r"\b(?:JOIN|UNION|INTERSECT|EXCEPT|GROUP\s+BY|HAVING|SELECT)\b", re.IGNORECASE
)


def sql_table_identifier(table: str, schema: str | None = None) -> str:
    """Quote a table identifier and, if provided, its separate schema identifier."""
    parts = (table,) if schema is None else (schema, table)
    return ".".join('"' + part.replace('"', '""') + '"' for part in parts)


def unquote_sql_identifier(identifier: str) -> str:
    """Decode one identifier read by the SQL relation pattern."""
    if identifier.startswith('"'):
        return identifier[1:-1].replace('""', '"')
    return identifier


def match_direct_projection(query: str) -> re.Match[str] | None:
    """Match a direct projection and identify its source relation."""
    match = _DIRECT_PROJECTION.match(query)
    if match is not None:
        rest = match["rest"]
        if _ROW_CLAUSE.match(rest) is not None and _NOT_DIRECT.search(rest) is None:
            return match
    return None


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
    return match_direct_projection(query) is not None


def metadata_bytes(metadata: Mapping[Any, Any] | None) -> dict[bytes, bytes]:
    """Field metadata as bytes keys and values (pyarrow gives bytes, a Delta log text)."""
    return {
        (k.encode() if isinstance(k, str) else k): (
            v.encode() if isinstance(v, str) else v
        )
        for k, v in (metadata or {}).items()
    }


def appended_field_change(
    held: "pa.Field[Any]",
    incoming: "pa.Field[Any]",
    *,
    delta_log_storage: bool = False,
) -> str | None:
    """Say why rows of ``incoming`` would not read back as written, appended to a
    column a database holds as ``held`` (ITL-1334).

    A table holds one type per column, so an appended column must have the held
    column's field metadata and, at every depth, each nested field's name and metadata
    (a struct's children, matched by name; a list's item; a map's key and item;
    fields of extension storage) and a map's ``keys_sorted``: otherwise one side's
    rows read back with the other side's attributes. A struct field on one side only, and a difference in a type's kind or
    parameters, are left to the database's own append rule (struct evolution, a type
    conflict). Nullability, at any depth, is too: whether a nullability change is
    refused belongs to the nullability audit (ITL-842).

    Args:
        held: The column as the database holds it (stored or pending).
        incoming: The column of the rows to append.
        delta_log_storage: The held type is the physical Delta log type. Within
            extension storage the log cannot describe metadata on list items or
            map keys and values. Struct field metadata is still known, apart
            from the logical identifiers checked separately against the log.

    Returns:
        The reason, naming the path (``x``, ``x.a``, ``x[]``, ``x.key``), or ``None``.
    """
    if metadata_bytes(held.metadata) != metadata_bytes(incoming.metadata):
        return f"{incoming.name} has other field metadata than the stored column"
    return _nested_change(
        arrow_utils.normalize_to_large_types(held.type, keep_null=True),
        arrow_utils.normalize_to_large_types(incoming.type, keep_null=True),
        incoming.name,
        delta_log_storage=delta_log_storage,
    )


def _child_change(
    held: "pa.Field[Any]",
    incoming: "pa.Field[Any]",
    path: str,
    *,
    metadata_only: bool,
    delta_log_storage: bool,
    check_metadata: bool = True,
) -> str | None:
    if not metadata_only and held.name != incoming.name:
        return f"{path} is named {incoming.name!r}, the stored column's {held.name!r}"
    if check_metadata:
        incoming_metadata = metadata_bytes(incoming.metadata)
        if delta_log_storage and metadata_only and _EXTENSION_NAME in incoming_metadata:
            # The physical Delta schema omits these keys to prevent Arrow from
            # decoding the extension. The identity check uses the raw log instead.
            del incoming_metadata[_EXTENSION_NAME]
            incoming_metadata.pop(_EXTENSION_METADATA, None)
        if metadata_bytes(held.metadata) != incoming_metadata:
            return f"{path} has other field metadata than the stored column"
    return _nested_change(
        held.type,
        incoming.type,
        path,
        metadata_only=metadata_only,
        delta_log_storage=delta_log_storage,
    )


def _nested_change(
    held: Any,
    incoming: Any,
    path: str,
    *,
    delta_log_storage: bool,
    metadata_only: bool = False,
) -> str | None:
    # Field metadata matters inside extension storage. Its layout is checked
    # separately, and may use physical field names (e.g. Delta's list "element").
    while isinstance(held, pa.BaseExtensionType):
        held = held.storage_type
        metadata_only = True
    while isinstance(incoming, pa.BaseExtensionType):
        incoming = incoming.storage_type
        metadata_only = True
    t = pa.types
    if t.is_struct(held) and t.is_struct(incoming):
        known = {f.name: f for f in held}
        for field in incoming:
            if field.name in known:
                found = _child_change(
                    known[field.name],
                    field,
                    f"{path}.{field.name}",
                    metadata_only=metadata_only,
                    delta_log_storage=delta_log_storage,
                )
                if found is not None:
                    return found
        return None
    # Delta has no field annotation for these positions. Continue into their
    # types because a struct child further down still has its own metadata.
    collection_metadata_known = not (metadata_only and delta_log_storage)
    if t.is_map(held) and t.is_map(incoming):
        if not metadata_only and held.keys_sorted != incoming.keys_sorted:
            return (
                f"{path} has keys_sorted={incoming.keys_sorted}, the stored column "
                f"{held.keys_sorted}"
            )
        return _child_change(
            held.key_field,
            incoming.key_field,
            f"{path}.key",
            metadata_only=metadata_only,
            delta_log_storage=delta_log_storage,
            check_metadata=collection_metadata_known,
        ) or _child_change(
            held.item_field,
            incoming.item_field,
            f"{path}.item",
            metadata_only=metadata_only,
            delta_log_storage=delta_log_storage,
            check_metadata=collection_metadata_known,
        )
    lists = (t.is_list, t.is_large_list, t.is_fixed_size_list)
    if any(k(held) for k in lists) and any(k(incoming) for k in lists):
        # cast: the type guards above narrow the types in the stubs' terms.
        return _child_change(
            cast(Any, held).value_field,
            cast(Any, incoming).value_field,
            f"{path}[]",
            metadata_only=metadata_only,
            delta_log_storage=delta_log_storage,
            check_metadata=collection_metadata_known,
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
            item, map key or item; see ``_delta_identity_node``) is not known, and
            the rule abstains there.
        identifier: The raw identifier, or ``None`` for no logical type.
        claims: ``False`` for a bare untyped null (``pa.null()`` with no
            annotation): it holds no value and claims no type, so it is never
            another identifier.
        children: The nested places, in order, each with its label (``.name`` for a
            struct child, ``[]`` for a list item, ``.key`` and ``.item`` for a map,
            ``|name`` for a union member, ``<storage>`` for an extension type held
            directly as another's storage, ``.name`` for the other children of a
            nested type; every label but a struct child's starts with ``_ROLE``, so
            places pair only within one role). Labels may repeat: Arrow allows two
            struct children or union members of one name.
    """

    known: bool
    identifier: RawIdentifier | None
    claims: bool = True
    children: "tuple[tuple[str, IdentityNode], ...]" = ()


UNKNOWN = IdentityNode(known=False, identifier=None)

# Child labels pair places of one role: a struct field is labelled ``.<name>``; a
# place of any other role carries ``_ROLE`` first, so a map's key, say, never pairs
# with a struct field named ``key``. Messages show labels without it.
_ROLE = "\x00"
MAP_KEY = f"{_ROLE}.key"
MAP_ITEM = f"{_ROLE}.item"
LIST_ITEM = f"{_ROLE}[]"


def _annotation(metadata: Mapping[Any, Any] | None) -> RawIdentifier | None:
    as_bytes = metadata_bytes(metadata)
    name = as_bytes.get(_EXTENSION_NAME)
    if name is None:
        return None
    return name, as_bytes.get(_EXTENSION_METADATA, b"")


def _extension_identifier(arrow_type: Any) -> RawIdentifier | None:
    """The raw identifier an Arrow extension type holds.

    Read from the type's C data interface export (``__arrow_c_schema__``), where
    Arrow writes the extension name and the serialized metadata it holds, as bytes
    (``_exported_metadata``), never by calling ``__arrow_ext_serialize__`` again: a
    Python extension type's serializer may give other bytes than the ones Arrow took
    when the type was made, and the export is also the only way to read the bytes of
    an extension type defined in Arrow's C++ library (``arrow.uuid``).

    Returns:
        The identifier, or ``None`` when Arrow cannot export the type (an extension
        type whose storage is itself an extension type): the bytes Arrow holds are
        then out of reach, and the serializer is not asked in their place.
    """
    try:
        capsule = pa.field("x", arrow_type).__arrow_c_schema__()
    except pa.ArrowNotImplementedError:
        return None
    identifier = _annotation(_exported_metadata(capsule))
    assert identifier is not None  # an extension type exports its name
    return identifier


def _exported_metadata(capsule: Any) -> dict[bytes, bytes]:
    """The metadata of an exported ``ArrowSchema``, as raw bytes.

    Read as the Arrow C data interface lays it out (a stable ABI): the struct's
    third member is the metadata, a 32-bit count, then for each pair a 32-bit
    length and the key bytes, a 32-bit length and the value bytes, native byte
    order. Keys and values are bytes: no text decoding, so metadata that is not
    UTF-8 is read as it is. The capsule releases the schema when it is collected.
    """
    import ctypes
    import struct

    get_pointer = ctypes.pythonapi.PyCapsule_GetPointer
    get_pointer.restype = ctypes.c_void_p
    get_pointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    schema = get_pointer(capsule, b"arrow_schema")
    # struct ArrowSchema { const char* format; const char* name;
    #                      const char* metadata; ... }
    metadata_pointer = ctypes.c_void_p.from_address(
        schema + 2 * ctypes.sizeof(ctypes.c_void_p)
    ).value
    if not metadata_pointer:
        return {}

    def read(offset: int, size: int) -> bytes:
        return ctypes.string_at(metadata_pointer + offset, size)

    count = struct.unpack("=i", read(0, 4))[0]
    offset = 4
    pairs: dict[bytes, bytes] = {}
    for _ in range(count):
        key_length = struct.unpack("=i", read(offset, 4))[0]
        key = read(offset + 4, key_length)
        offset += 4 + key_length
        value_length = struct.unpack("=i", read(offset, 4))[0]
        pairs[key] = read(offset + 4, value_length)
        offset += 4 + value_length
    return pairs


def identity_node(arrow_field: "pa.Field[Any]") -> IdentityNode:
    """The identifiers an Arrow field carries, at every depth, in either form.

    A logical type reaches a database as an Arrow extension type, or as its storage
    type annotated with ``ARROW:extension:name`` / ``ARROW:extension:metadata`` (the
    form a stored column reads back in, and the one ``ExtensionAwareDatabase``
    passes on). Each level is one identifier: an annotation names the field's own
    type as storage; an extension type is unwrapped one level, and an extension
    type found as storage is a level of its own. A dictionary is an encoding, not a
    type: its values stand where it stands. The children of every nested Arrow type
    are enumerated from the type itself, so no container is a leaf.

    Args:
        arrow_field: A column, or a nested field, as given (or as a database holds
            it, when what it holds is the field itself).

    Returns:
        The field's ``IdentityNode``; every part known.
    """
    arrow_type = arrow_field.type
    if pa.types.is_dictionary(arrow_type):
        return identity_node(arrow_field.with_type(arrow_type.value_type))
    identifier = _annotation(arrow_field.metadata)
    if pa.types.is_null(arrow_type):
        # A bare null claims nothing; an annotated one claims its annotation.
        return IdentityNode(
            known=True, identifier=identifier, claims=identifier is not None
        )
    if identifier is not None:
        storage = arrow_type
    elif isinstance(arrow_type, pa.BaseExtensionType):
        identifier = _extension_identifier(arrow_type)
        storage = arrow_type.storage_type
        if identifier is None:
            # Not exportable: this level's bytes are not known; its storage is.
            return IdentityNode(
                known=False, identifier=None, children=_children_of(storage)
            )
    else:
        storage = arrow_type
    return IdentityNode(
        known=True, identifier=identifier, children=_children_of(storage)
    )


def _children_of(arrow_type: Any) -> "tuple[tuple[str, IdentityNode], ...]":
    t = pa.types
    if t.is_dictionary(arrow_type):
        return _children_of(arrow_type.value_type)
    if isinstance(arrow_type, pa.BaseExtensionType):
        return ((f"{_ROLE}<storage>", identity_node(pa.field("storage", arrow_type))),)
    if t.is_map(arrow_type):
        return (
            (MAP_KEY, identity_node(arrow_type.key_field)),
            (MAP_ITEM, identity_node(arrow_type.item_field)),
        )
    if (
        t.is_list(arrow_type)
        or t.is_large_list(arrow_type)
        or t.is_fixed_size_list(arrow_type)
        or t.is_list_view(arrow_type)
        or t.is_large_list_view(arrow_type)
    ):
        return ((LIST_ITEM, identity_node(arrow_type.value_field)),)
    if t.is_struct(arrow_type):
        return tuple((f".{child.name}", identity_node(child)) for child in arrow_type)
    prefix = f"{_ROLE}|" if t.is_union(arrow_type) else f"{_ROLE}."
    children = (arrow_type.field(i) for i in range(arrow_type.num_fields))
    return tuple((f"{prefix}{child.name}", identity_node(child)) for child in children)


def _identifier_text(identifier: RawIdentifier | None) -> str:
    """An identifier as the error shows it: ``'<name>;<metadata hex>'`` (a quoted
    text) when the name is text; a name that is not UTF-8 as a bytes literal,
    ``b'\\xff';<metadata hex>``, which no text can be shown as, so two names never
    read alike."""
    if identifier is None:
        return "no logical type"
    name, metadata = identifier
    try:
        return repr(f"{name.decode()};{metadata.hex()}")
    except UnicodeDecodeError:
        return f"{name!r};{metadata.hex()}"


def matched_pairs(
    held: "Sequence[tuple[str, H]]", incoming: "Sequence[tuple[str, G]]"
) -> "list[tuple[str, H, G]]":
    """Pair the labelled places of two sides (ITL-1371).

    A label that occurs once on each side pairs by label. Where a label repeats on
    either side (Arrow allows two struct children, union members or columns of one
    name), its places pair by occurrence: the k-th with the k-th. Never by label
    alone, so a repeated place is never overwritten by another of its name. A place
    without a partner is left out.

    Args:
        held: The held side's places, in order, with their labels.
        incoming: The given side's places, in order, with their labels.

    Returns:
        ``(label, held place, given place)`` for every pair, in the given order.
    """
    by_label: dict[str, list[H]] = {}
    for label, place in held:
        by_label.setdefault(label, []).append(place)
    seen: dict[str, int] = {}
    pairs: list[tuple[str, H, G]] = []
    for label, place in incoming:
        k = seen.get(label, 0)
        seen[label] = k + 1
        candidates = by_label.get(label, [])
        if k < len(candidates):
            pairs.append((label, candidates[k], place))
    return pairs


def logical_identity_change(
    held: IdentityNode, incoming: IdentityNode, path: str
) -> str | None:
    """Say why rows of ``incoming`` would be read back as another logical type than
    they were written as, appended where a database holds ``held`` (ITL-1371).

    A table holds one logical type per column and per nested field, so at every place
    both sides have, the appended identifier must be the held one; no logical type is
    an identifier too. The rule refuses only where it knows: wherever the held
    identifier is not reliably known (``IdentityNode.known``), or a side is a bare
    untyped null, it abstains at that place, and a place only one side has (struct evolution, another kind of
    type) is left to the database's other append checks. It compares raw bytes,
    never Arrow type equality: pyarrow's equality of two instances of one Python
    extension class ignores their serialized metadata. Places pair as
    ``matched_pairs`` says.

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
    for label, held_child, child in matched_pairs(held.children, incoming.children):
        found = logical_identity_change(
            held_child, child, f"{path}{label.replace(_ROLE, '')}"
        )
        if found is not None:
            return found
    return None


def given_identities(
    given: "pa.Table | pa.Schema",
) -> "list[tuple[str, list[IdentityNode]]]":
    """The identifiers the rows to append carry, column by column (ITL-1371).

    A column's field, and, for a table, each of its chunks: pyarrow lets the chunks of
    one column hold instances of one Python extension class with other serialized
    metadata (it compares them by class), and nested fields with other field
    metadata, an annotation included (it compares types without it), so every
    chunk's own type is read too, in both forms, and every one of them must be the
    held one.

    Args:
        given: The rows to append, or only their schema.

    Returns:
        ``(column name, identity nodes)`` in column order; the field's node first.
    """
    if isinstance(given, pa.Schema):
        return [(f.name, [identity_node(f)]) for f in given]
    out: list[tuple[str, list[IdentityNode]]] = []
    for field, column in zip(given.schema, given.columns):
        nodes = [identity_node(field)]
        seen = set(nodes)
        for chunk in column.chunks:
            node = identity_node(field.with_type(chunk.type))
            if node not in seen:
                seen.add(node)
                nodes.append(node)
        out.append((field.name, nodes))
    return out


def identity_refusal(
    held: "Sequence[tuple[str, IdentityNode]]", given: "pa.Table | pa.Schema"
) -> tuple[str, str] | None:
    """The first column of ``given`` known to carry another logical type identifier
    than the one held in its place (``logical_identity_change``).

    Columns pair as ``matched_pairs`` says: by name, or by occurrence where a name
    repeats; the order of columns of distinct names plays no part.

    Args:
        held: The held columns, in order, with what is known of their identifiers.
        given: The rows to append (``given_identities``), or their schema.

    Returns:
        The column's name and the reason, or ``None``.
    """
    for name, held_node, nodes in matched_pairs(held, given_identities(given)):
        for node in nodes:
            reason = logical_identity_change(held_node, node, name)
            if reason is not None:
                return name, reason
    return None


def first_logical_identity_change(
    held: "Iterable[pa.Schema]", given: "pa.Table | pa.Schema"
) -> tuple[str, str] | None:
    """The first column of ``given`` that carries another logical type identifier
    than the column the database holds in its place (``identity_refusal``), for a
    database that holds what it was given (in-memory, a pending batch, a connector's
    stored schema): every held identifier is known.

    Args:
        held: The schemas the database holds for the table (stored, pending).
        given: The rows to append, or their schema.

    Returns:
        The column's name and the reason, or ``None``.
    """
    for schema in held:
        found = identity_refusal([(f.name, identity_node(f)) for f in schema], given)
        if found is not None:
            return found
    return None


def in_held_column_order(
    records: "pa.Table", held: "Sequence[pa.Field[Any]] | None"
) -> "pa.Table":
    """``records`` with its columns in the order the database holds them (ITL-1367).

    An append matches columns by name, never by position: when ``records`` has
    exactly the held columns in another order, and each of its fields equals the held
    field of its name, its columns are put in the held order. Equal is one rule
    (``_same_field``): the same type at every depth (in the large variants, a logical
    type with its own name and metadata, at any depth) and no change the append
    check reports (``appended_field_change``: field metadata and nested field
    attributes at every depth). Names are matched exactly, as Arrow matches them:
    ``a`` and ``A`` are two columns.

    Anything else is returned exactly as given, so the database's own rule applies
    as before: no held table, a column added or missing, a field that differs in any
    way (a struct whose children are in another order included: ITL-1368), a name that
    repeats.

    Args:
        records: The rows to append, the record id column already named as held.
        held: The held columns, in the held order, or ``None`` when nothing is held.

    Returns:
        ``records``, reordered or as given.
    """
    if held is None:
        return records
    names = records.column_names
    held_names = [field.name for field in held]
    if names == held_names:
        return records
    if len(set(names)) != len(names) or len(set(held_names)) != len(held_names):
        return records
    if set(names) != set(held_names):
        return records
    held_fields = {field.name: field for field in held}
    if not all(_same_field(field, held_fields[field.name]) for field in records.schema):
        return records
    return records.select(held_names)


def _same_field(given: "pa.Field[Any]", held: "pa.Field[Any]") -> bool:
    """Whether rows of ``given`` would be held as ``held`` holds them: the same type at
    every depth, in the large variants (a logical type is equal only to itself: its
    name, metadata and storage), and no change ``appended_field_change`` reports."""
    # Eligibility is decided on the types as given, before normalization turns a
    # view type into its large variant.
    if not (_handled(given.type) and _handled(held.type)):
        return False
    large = arrow_utils.normalize_to_large_types
    if not _same_type(
        large(given.type, keep_null=True), large(held.type, keep_null=True)
    ):
        return False
    return appended_field_change(held, given) is None


def _handled(arrow_type: Any) -> bool:
    """Whether every type in ``arrow_type``'s tree is one the comparison handles
    (``_same_type``): a leaf kind of ``_LEAF_KINDS``, a struct, a map, a list, large
    list or fixed-size list, or an extension type over those."""
    t = pa.types
    if isinstance(arrow_type, pa.ExtensionType):
        return _handled(arrow_type.storage_type)
    if t.is_struct(arrow_type):
        return all(
            _handled(arrow_type.field(i).type) for i in range(arrow_type.num_fields)
        )
    if t.is_map(arrow_type):
        return _handled(arrow_type.key_type) and _handled(arrow_type.item_type)
    if (
        t.is_list(arrow_type)
        or t.is_large_list(arrow_type)
        or t.is_fixed_size_list(arrow_type)
    ):
        return _handled(cast(Any, arrow_type).value_type)
    return any(getattr(t, kind)(arrow_type) for kind in _LEAF_KINDS)


_LEAF_KINDS = (
    "is_null",
    "is_boolean",
    "is_integer",
    "is_floating",
    "is_decimal",
    "is_string",
    "is_large_string",
    "is_binary",
    "is_large_binary",
    "is_fixed_size_binary",
    "is_date",
    "is_time",
    "is_timestamp",
    "is_duration",
)
"""The leaf types the comparison handles. Eligibility is positive: a type of any
other kind anywhere in a column's tree (a list view, a union, a dictionary, a
run-end-encoded type, an interval, a view type, anything pyarrow adds later) makes
the column not eligible, and the table is left as given for the database's own
rule (codex review of PR 491, round 4)."""


def _same_type(given: Any, held: Any) -> bool:
    """Whether two Arrow types are equal at every depth, for a type tree the
    comparison handles entirely: the leaf kinds of ``_LEAF_KINDS``, a struct, a map,
    a list, large list or fixed-size list, and an extension type over those, equal
    only to one of its own name and serialized metadata (pyarrow compares a Python
    extension type by its class and storage, not by its metadata). Any other type
    anywhere in either tree gives ``False``."""
    t = pa.types
    if isinstance(given, pa.ExtensionType) or isinstance(held, pa.ExtensionType):
        return (
            isinstance(given, pa.ExtensionType)
            and isinstance(held, pa.ExtensionType)
            and given.extension_name == held.extension_name
            and given.__arrow_ext_serialize__() == held.__arrow_ext_serialize__()
            and _same_type(given.storage_type, held.storage_type)
        )
    if given != held:
        return False
    if t.is_struct(given):
        return all(
            _same_type(given.field(i).type, held.field(i).type)
            for i in range(given.num_fields)
        )
    if t.is_map(given):
        return _same_type(given.key_type, held.key_type) and _same_type(
            given.item_type, held.item_type
        )
    if t.is_list(given) or t.is_large_list(given) or t.is_fixed_size_list(given):
        return _same_type(cast(Any, given).value_type, cast(Any, held).value_type)
    return any(getattr(t, kind)(given) for kind in _LEAF_KINDS)
