"""Utilities shared across database implementations."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
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


def canonical_column_order(records: "pa.Table", record_id_column: str) -> "pa.Table":
    """``records`` with its columns in the canonical order of a new table (ITL-1370).

    The record id column, then every other column sorted by name, by code point
    (Python's ``sorted`` on ``str``): the order SpiralDB stores every table in, so all
    five databases store and read one order. Nothing else depends on it: no hash, path
    or record id reads a stored table's column order, and every reader presents its
    rows in its own layout whatever the stored order
    (``utils.arrow_utils.in_presented_order``).
    Names that repeat are returned as given, for the database's own rule.

    Args:
        records: The rows that create the table.
        record_id_column: The record id column, as the database names it.

    Returns:
        ``records``, reordered or as given.
    """
    names = records.column_names
    if len(set(names)) != len(names) or record_id_column not in names:
        return records
    order = [record_id_column, *sorted(n for n in names if n != record_id_column)]
    if order == names:
        return records
    return records.select(order)


def in_held_column_order(
    records: "pa.Table",
    held: "Sequence[tuple[str, pa.DataType]] | None",
    record_id_column: str,
) -> "pa.Table":
    """``records`` with its columns in the order the database stores them (ITL-1367,
    ITL-1370).

    A new table (``held`` is ``None``) is written in the canonical order
    (``canonical_column_order``): the record id, then every other column by name. A
    table the database already holds keeps its order: an append matches columns by
    name, never by position, so when ``records`` has exactly the held columns in
    another order, each with the held type, its columns are put in the held order (a
    table written before ITL-1370, in another order, keeps it). The fields are kept as
    given (metadata, nullability): the append checks that follow compare them by name.

    Types are compared as the database compares them: a logical (extension) type as
    its storage type, in the large variants. Anything else is returned as given, for
    the database's own append rule: a column added or missing, a type
    that differs (a struct whose children are in another order included: its type
    differs; ITL-1368), and a name that repeats exactly. Names are matched exactly, as
    Arrow matches them: ``a`` and ``A`` are two columns.

    Args:
        records: The rows to append, the record id column already named as held.
        held: Each held column's name and type, in the held order (the pending
            batch, else the stored table), or ``None`` when the database holds no
            such table.
        record_id_column: The record id column, as the database names it.

    Returns:
        ``records``, reordered or as given.
    """
    if held is None:
        return canonical_column_order(records, record_id_column)
    names = records.column_names
    held_names = [name for name, _ in held]
    if names == held_names:
        return records
    # Arrow matches names exactly: ``a`` and ``A`` are two columns. Only a name that
    # repeats exactly cannot be matched.
    if len(set(names)) != len(names) or len(set(held_names)) != len(held_names):
        return records
    if set(names) != set(held_names):
        return records
    held_types = dict(held)
    for field in records.schema:
        if _stored_form(field.type) != _stored_form(held_types[field.name]):
            return records
    return records.select(held_names)


def _stored_form(arrow_type: "pa.DataType") -> "pa.DataType":
    """``arrow_type`` as a database compares it: a logical (extension) type as its
    storage type, a dictionary as its value type, in the large variants. A database
    that holds such a column reports that type, so the comparison sees what the
    database sees."""
    if isinstance(arrow_type, pa.ExtensionType):
        arrow_type = arrow_type.storage_type
    if pa.types.is_dictionary(arrow_type):
        arrow_type = cast(Any, arrow_type).value_type
    return arrow_utils.normalize_to_large_types(arrow_type, keep_null=True)
