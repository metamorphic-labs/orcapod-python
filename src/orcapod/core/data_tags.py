# pyright: reportImportCycles=false
# The existing cycle data_tags -> types -> core_protocols -> side_effect_pod -> side_effects
# -> function_node, which the rename from referential_tags.py brought into pyright's view
# (a file-level diagnostic: no line-level ignore reaches it). Breaking it is ITL-1206.
"""Data-tags: the rules every stream and operator shares (ITL-1023).

Specification "Orcapod v0.4 — Referential Tags" (attached to ITL-1004). A tag is either
plain (it carries its own value) or a data-tag (a handle to the data column of the
same name). In a stream a data-tag ``x`` is one physical column ``x`` whose name is
both a tag key and a data key, so the binding is the name intersection of the two halves of
``output_schema()`` / ``keys()`` (section 4.3: "the presence of a tag and data column with
the same name must therefore be sufficient to identify the tag as referential", the specification's word for a data-tag). No other
schema field records it.

This module holds what more than one operator needs: finding the data-tags of a stream,
the namespace validator (sections 4 and 13.4), and the diagnostic emitted when a structural
operator turns a data-tag into a plain tag (section 7).

The identities of streams with data-tags are settled (developer, November 18, 2026) and
freeze with data context v1 at the 0.4.0 release. No identity depends on the order of a
stream's tag columns: where a list of data-tags enters an identity, it enters as a set of
sorted names (ITL-1246). Plain (``tag_only_columns``) streams keep their v0.3 identities
exactly.
"""

from __future__ import annotations

import datetime
import types
import typing
import warnings
import weakref
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Literal

from orcapod.errors import InputValidationError
from orcapod.types import Schema, SourceInfoValue

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
else:
    pa = LazyModule("pyarrow")

# Diagnostic emitted when a structural operator materializes a data-tag (section 7.4).
# ``"warn"`` emits a ``DataTagMaterializationWarning``; ``"silent"`` emits nothing. The
# setting never changes a result, a schema or a hash. A module-level default for now.
data_tag_materialization: Literal["warn", "silent"] = "warn"


class DataTagMaterializationWarning(UserWarning):
    """A structural operator turned a data-tag into a plain tag (section 7)."""


# Set while Orcapod itself adds a structural step the user did not write (the select
# of a function pod call): its materializations are not reported.
_diagnostic_off: ContextVar[bool] = ContextVar(
    "data_tag_materialization_off", default=False
)


# The operations that already reported, by id, holding the object so the id is not
# reused while the entry exists.
_reported: "weakref.WeakValueDictionary[int, object]" = weakref.WeakValueDictionary()


@contextmanager
def materialization_diagnostic_off() -> Iterator[None]:
    """Build the steps in this block without the materialization diagnostic.

    Used for steps Orcapod adds by itself, such as the automatic select of a function
    pod call, which would otherwise report on every call. The steps and their results
    are exactly the same.

    Yields:
        Nothing.
    """
    token = _diagnostic_off.set(True)
    try:
        yield
    finally:
        _diagnostic_off.reset(token)


def with_data_tags_first(tag: TagProtocol, data: DataProtocol) -> DataProtocol:
    """Return a row's ``data`` with its data-tags first, in tag order (ITL-1205).

    The row form of ``data_tags_first``: an operator's async path that passes rows
    on gives each row the order its sync path gives. A row without data-tags, or one
    already in that order, is returned as it is.

    Args:
        tag: The row's tag; only its names, in order, are read.
        data: The row's data.

    Returns:
        ``data`` itself, or a copy with the same columns, values and source info, its
        data-tags first.
    """
    names = tuple(data.keys())
    tags = tuple(name for name in tag.keys() if name in names)
    if names[: len(tags)] == tags:
        return data
    return data.select(*tags, *(name for name in names if name not in tags))


def data_tags_first(tag_schema: Mapping[str, Any], data_schema: Schema) -> Schema:  # pyright: ignore[reportExplicitAny]  # only the names of the tag schema are read
    """Return ``data_schema`` with its data-tags first, in tag order (ITL-1205).

    Developer decision, November 17, 2026: a stream lists its data-tags first among
    its data columns, in tag order, then the other data columns in their own order.
    A table built from rows lays a data-tag out among the tags, so this is the order
    of every row; ``ArrowTableStream`` keys and every operator's prediction follow it.
    No schema, row or table hash depends on the order of a stream's columns.

    Args:
        tag_schema: The tag schema; only its names, in order, are read.
        data_schema: The data schema to order.

    Returns:
        ``data_schema`` itself when it is already in that order, else a reordered copy
        with the same types and optional fields.
    """
    names = tuple(data_schema)
    tags = tuple(name for name in tag_schema if name in data_schema)
    if names[: len(tags)] == tags:
        return data_schema
    ordered = {name: data_schema[name] for name in tags}
    ordered.update((name, data_schema[name]) for name in names if name not in ordered)
    return Schema(ordered, optional_fields=data_schema.optional_fields)


def data_tag_names(
    tag_names: Iterable[str], data_names: Iterable[str]
) -> tuple[str, ...]:
    """Return the data-tags: the tag names that are also data names, in tag order.

    Args:
        tag_names: The tag column names (system tags may be included; they never
            match a data name).
        data_names: The data column names.

    Returns:
        The names in both, in the order of ``tag_names``.
    """
    data = set(data_names)
    return tuple(name for name in tag_names if name in data)


def stream_data_tags(stream: Any) -> tuple[str, ...]:  # pyright: ignore[reportExplicitAny]  # any object with keys(): a stream, source or node
    """Return the data-tags of ``stream``, known from its keys at graph-build time.

    Args:
        stream: A stream (anything with ``keys()`` returning tag and data names).

    Returns:
        The names that are both tag keys and data keys, in tag order.
    """
    tag_keys, data_keys = stream.keys()
    return data_tag_names(tag_keys, data_keys)


def check_namespaces(
    tag_schema: Mapping[str, Any],  # pyright: ignore[reportExplicitAny]  # Python types of any kind
    data_schema: Mapping[str, Any],  # pyright: ignore[reportExplicitAny]  # Python types of any kind
    data_tags: Iterable[str],
) -> None:
    """Check the column namespaces of an output schema (sections 4.1 to 4.4).

    A name in both halves must be one of ``data_tags`` (a materialized tag and a data
    column never share a name), and a data-tag has one type in both halves.

    Args:
        tag_schema: The output tag schema.
        data_schema: The output data schema.
        data_tags: The names the producer keeps as data-tags.

    Raises:
        InputValidationError: If a tag and a data column share a name that is not a
            data-tag, or a data-tag's tag and data types differ.
    """
    refs = set(data_tags)
    for name in data_tag_names(tag_schema, data_schema):
        if name not in refs:
            raise InputValidationError(
                f"tag {name!r} and data column {name!r} would share a name without "
                "being one data-tag column: a plain tag and a data column cannot "
                "share a name"
            )
        if tag_schema[name] != data_schema[name]:
            raise InputValidationError(
                f"data-tag {name!r} has type {tag_schema[name]} as a tag and "
                f"{data_schema[name]} as data"
            )


def warn_materialized(operation: object, names: Iterable[str]) -> None:
    """Report that ``operation`` turned the data-tags ``names`` into plain tags.

    Its job is the diagnostic of section 7.4, so it takes the operation. It reports
    once per operation object: a job validates its operators again when it builds and
    runs its nodes, and that is not a new structural step. An operation built while
    ``materialization_diagnostic_off`` is active (Orcapod's own step) never reports.
    Emits nothing when ``names`` is empty or ``data_tag_materialization`` is
    ``"silent"``.

    Args:
        operation: The structural operation (an operator instance).
        names: The tags that became plain.
    """
    names = tuple(names)
    if not names or id(operation) in _reported:
        return
    _reported[id(operation)] = operation
    if data_tag_materialization == "silent" or _diagnostic_off.get():
        return
    warnings.warn(
        f"{type(operation).__name__}: the data column of data-tag(s) {list(names)} was removed; "
        "they are now plain tags with the same values (set "
        "orcapod.core.data_tags.data_tag_materialization = 'silent' to hide this)",
        DataTagMaterializationWarning,
        stacklevel=3,
    )


# The marker of a data-tag in a stored table (developer, November 16, 2026,
# ITL-1019): Arrow field metadata on the stored tag column, at the level of a logical
# type's extension metadata. Only a data-tag carries it; a plain tag carries no
# marker. It is not ``ARROW:extension:*`` metadata, so no schema or table hash reads it.
TAG_KIND_METADATA_KEY = b"orcapod:tag_kind"
DATA_TAG_KIND = b"data_tag"


def mark_data_tags(table: "pa.Table", data_tags: Iterable[str]) -> "pa.Table":
    """Return ``table`` with exactly the columns ``data_tags`` marked as data-tags.

    A stored table states its own bindings: the writer marks the columns it knows to be
    data-tags and removes a marker any other column carries (one that came in from
    upstream metadata is not this writer's binding). Values and every other field
    property (name, type, nullability, other metadata) are unchanged.

    Args:
        table: The rows about to be stored.
        data_tags: The data-tags of the rows; each must be a column of
            ``table``.

    Returns:
        The table with the marker on exactly the ``data_tags`` columns.

    Raises:
        ValueError: If a data-tag is not a column of ``table``.
    """
    refs = set(data_tags)
    if missing := refs - set(table.column_names):
        raise ValueError(
            f"data-tags {sorted(missing)} are not columns of the stored table"
        )
    schema = table.schema
    changed = False
    for index, field in enumerate(schema):
        metadata: dict[bytes | str, bytes | str] = {
            key: value for key, value in (field.metadata or {}).items()
        }
        marked = metadata.get(TAG_KIND_METADATA_KEY) is not None
        if field.name in refs:
            if metadata.get(TAG_KIND_METADATA_KEY) == (DATA_TAG_KIND):
                continue
            metadata[TAG_KIND_METADATA_KEY] = DATA_TAG_KIND
        elif marked:
            del metadata[TAG_KIND_METADATA_KEY]
        else:
            continue
        schema = schema.set(
            index,
            field.with_metadata(metadata) if metadata else field.remove_metadata(),
        )
        changed = True
    if not changed:
        return table
    # The same arrays under the new field metadata: no cast, no copy.
    return pa.Table.from_arrays(table.columns, schema=schema)


def stored_data_tags(schema: "pa.Schema") -> tuple[str, ...]:
    """Return the columns a stored table marks as data-tags, in column order.

    Args:
        schema: The Arrow schema of a stored table.

    Returns:
        The names of the columns that carry the data-tag marker.

    Raises:
        ValueError: If a column carries the marker key with another value.
    """
    names: list[str] = []
    for field in schema:
        value = (field.metadata or {}).get(TAG_KIND_METADATA_KEY)
        if value is None:
            continue
        if value != DATA_TAG_KIND:
            raise ValueError(
                f"stored column {field.name!r} has {TAG_KIND_METADATA_KEY!r} "
                f"metadata {value!r}; the only valid value is "
                f"{DATA_TAG_KIND!r}"
            )
        names.append(field.name)
    return tuple(names)


def require_database_keeps_marker(database: object, data_tags: Iterable[str]) -> None:
    """Refuse, before anything is written, a database that cannot store the marker.

    A stored table must state what it stores (developer, November 16, 2026, ITL-1019),
    so a data-tag is never stored unmarked. A database is accepted only when it
    states that it keeps Arrow field metadata
    (``FieldMetadataKeepingDatabaseProtocol.keeps_field_metadata``); one that does not
    have the member is refused, so an unknown database never passes by default. The
    statement decides: a database that states it is not probed. One that does not,
    and can check rows before writing them (``RecordValidatingDatabaseProtocol``), is
    asked why, and the reason it gives is part of the error.

    Args:
        database: The database a node will store its data-tags in.
        data_tags: The data-tags it will store; nothing is checked when empty.

    Raises:
        InputValidationError: If the database does not state that it keeps field
            metadata, naming the database class and the two ways out.
    """
    from orcapod.protocols.database_protocols import (
        RecordValidatingDatabaseProtocol,
        keeps_field_metadata,
    )

    data_tags = tuple(data_tags)
    if not data_tags or keeps_field_metadata(database):
        return
    reason = "it does not state that it keeps field metadata (keeps_field_metadata)"
    if isinstance(database, RecordValidatingDatabaseProtocol):
        # Only for the error: the reason the database gives for refusing marked rows.
        probe = mark_data_tags(
            pa.table({name: pa.array([], type=pa.null()) for name in data_tags}),
            data_tags,
        )
        try:
            database.validate_records(probe)
        except ValueError as exc:
            reason = str(exc)
    raise InputValidationError(
        f"{type(database).__name__} cannot store the data-tags "
        f"{list(data_tags)}: a stored data-tag carries Arrow field "
        f"metadata, and this database keeps none ({reason}). Use a database that "
        "keeps field metadata (DeltaTableDatabase, InMemoryArrowDatabase, the "
        "SpiralDB database), or keep these tags plain with tag_only_columns on "
        "the source. Nothing was written."
    )


def without_tag_kind_marks(table: "pa.Table") -> "pa.Table":
    """Return ``table`` with no data-tag marker on any column.

    A stored table read into a stream drops the marker: in a stream the binding is the
    name intersection of the tag and data keys, and a marker carried further would be
    written by a downstream store whose binding it is not.

    Args:
        table: Rows read from a stored table.

    Returns:
        The same rows; no field carries the marker.
    """
    return mark_data_tags(table, ())


# The Python types a column may have to become a data-tag by ``tag_by``
# (ITL-1175): one value per row, of a type with plain equality and order. A column
# whose Arrow type the type converter reads as no Python type (``Any``: decimal, time
# of day, duration) is refused. Sources do not apply this rule yet: a source accepts
# any type for a declared tag.
TAGGABLE_TYPES: tuple[type, ...] = (
    int,
    float,
    bool,
    str,
    bytes,
    datetime.datetime,
    datetime.date,
)


def require_taggable_type(python_type: object) -> None:
    """Refuse a column type that cannot become a data-tag (ITL-1175).

    The rule ``tag_by`` applies: the type must be one of ``TAGGABLE_TYPES``. A
    nullable type (``T | None``) is refused, with the way out: ``drop_nulls`` narrows
    the column to ``T``. Lists, sets, dicts, tuples, structs, user classes and every
    logical type (``Path``, ``UUID``, arrays) are refused, and so is a column the type
    converter gives no Python type (``Any``).

    Args:
        python_type: The column's Python type, as ``output_schema()`` gives it.

    Raises:
        InputValidationError: If the type cannot be a tag. The caller names the
            column.
    """
    if python_type in TAGGABLE_TYPES:
        return
    members = typing.get_args(python_type)
    if typing.get_origin(python_type) in (typing.Union, types.UnionType) and (
        type(None) in members
    ):
        rest = [member for member in members if member is not type(None)]
        if len(rest) == 1 and rest[0] in TAGGABLE_TYPES:
            raise InputValidationError(
                f"its type {python_type} is nullable, and a tag needs a value in "
                + "every row: drop the rows with a null first with drop_nulls, which "
                + f"narrows the column to {rest[0].__name__}"
            )
    names = ", ".join(t.__name__ for t in TAGGABLE_TYPES)
    raise InputValidationError(
        f"its type {python_type} cannot be a tag: a tag is one value of one of "
        + f"these types: {names}"
    )


def not_supported_yet(operation: str, issue: str) -> NotImplementedError:
    """Return the error for an operation that does not handle data-tags yet.

    Args:
        operation: The operation's name.
        issue: The Linear issue that will add the support.

    Returns:
        The error to raise.
    """
    return NotImplementedError(
        f"{operation}: not supported with data-tags yet ({issue}). Build the "
        "source with tag_only_columns=True to keep every tag plain, as in v0.3, or "
        "tag_only_columns=[...] to keep the named tags plain."
    )


TagOnlyColumns = str | Iterable[str] | bool
"""The ``tag_only_columns`` option of a source: names, or ``True`` for every tag."""


def source_data_tags(
    tag_columns: Iterable[str], tag_only_columns: TagOnlyColumns
) -> tuple[str, ...]:
    """Return a source's data-tags: its declared tags minus the tag-only ones.

    Every declared tag of a source is a data-tag by default (developer, November 12,
    2026): it is also a data column of the same name. ``tag_only_columns`` names the tags
    kept plain, exactly as in v0.3 (tag only, not data); ``True`` keeps all of them plain.

    Args:
        tag_columns: The source's declared tag columns.
        tag_only_columns: The tags kept plain: one name, several, ``True`` for all of
            them, or ``False`` / empty for none.

    Returns:
        The data-tags, in ``tag_columns`` order.

    Raises:
        ValueError: If ``tag_only_columns`` names a column that is not in
            ``tag_columns``.
    """
    tags = tuple(tag_columns)
    if tag_only_columns is True:
        return ()
    if tag_only_columns is False:
        plain: set[str] = set()
    elif isinstance(tag_only_columns, str):
        plain = {tag_only_columns}
    else:
        plain = set(tag_only_columns)
    if unknown := plain - set(tags):
        raise ValueError(
            f"tag_only_columns {sorted(unknown)} are not in tag_columns {list(tags)}"
        )
    return tuple(name for name in tags if name not in plain)


def merge_source_info(
    values: Sequence[SourceInfoValue],
    combine: Callable[[Sequence[str | None]], str],
) -> SourceInfoValue:
    """Merge the source info of several equal values into the source info of one.

    The values merged are equal, so their source info has one shape: a token, or a
    list of them nested as deep as the data column (provenance keeps the shape of its
    data column; developer decision, November 13, 2026, ITL-1017). The result has
    that shape, and each of its tokens is ``combine`` of the tokens at that position;
    the operation that merges passes its own fold (``Join``: the ``join_v1`` fold).
    This is what a ``Join`` does with the column of a tag that is a data-tag on
    several of its inputs, and does not assume the column is a tag.

    Args:
        values: The source info of each merged value, all of one shape.
        combine: Folds the tokens at one position into one token.

    Returns:
        The merged source info, of the values' shape.

    Raises:
        ValueError: If the values do not have one shape (a token beside a list, or
            lists of different lengths).
    """
    if all(isinstance(value, list) for value in values):
        lists = typing.cast("list[list[SourceInfoValue]]", list(values))
        if len({len(value) for value in lists}) > 1:
            raise ValueError(
                f"cannot merge source info of lists of different lengths: "
                f"{[len(value) for value in lists]}"
            )
        return [merge_source_info(column, combine) for column in zip(*lists)]
    if any(isinstance(value, list) for value in values):
        raise ValueError("cannot merge the source info of a list with that of a scalar")
    return combine(typing.cast("list[str | None]", list(values)))
