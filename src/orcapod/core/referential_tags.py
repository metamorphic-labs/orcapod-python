"""Referential tags: the rules every stream and operator shares (ITL-1023).

Specification "Orcapod v0.4 — Referential Tags" (attached to ITL-1004). A tag is either
materialized (it carries its own value) or referential (a handle to the data column of the
same name). In a stream a referential tag ``x`` is one physical column ``x`` whose name is
both a tag key and a data key, so the binding is the name intersection of the two halves of
``output_schema()`` / ``keys()`` (section 4.3: "the presence of a tag and data column with
the same name must therefore be sufficient to identify the tag as referential"). No other
schema field records it.

This module holds what more than one operator needs: finding the references of a stream,
the namespace validator (sections 4 and 13.4), and the diagnostic emitted when a structural
operator turns a reference into a plain tag (section 7).

The identities of referential streams are not frozen yet: on the integration branch of
ITL-1023 they may still change. Plain (``tag_only_columns``) streams keep their v0.3
identities exactly.
"""

from __future__ import annotations

import typing
import warnings
import weakref
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Literal

from orcapod.errors import InputValidationError
from orcapod.types import SourceInfoValue
from orcapod.utils import arrow_utils

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

# Diagnostic emitted when a structural operator materializes a reference (section 7.4).
# ``"warn"`` emits a ``ReferenceMaterializationWarning``; ``"silent"`` emits nothing. The
# setting never changes a result, a schema or a hash. A module-level default for now.
reference_materialization: Literal["warn", "silent"] = "warn"


class ReferenceMaterializationWarning(UserWarning):
    """A structural operator turned a referential tag into a plain tag (section 7)."""


# Set while Orcapod itself adds a structural step the user did not write (the select
# of a function pod call): its materializations are not reported.
_diagnostic_off: ContextVar[bool] = ContextVar(
    "reference_materialization_off", default=False
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


def reference_names(
    tag_names: Iterable[str], data_names: Iterable[str]
) -> tuple[str, ...]:
    """Return the referential tags: the tag names that are also data names, in tag order.

    Args:
        tag_names: The tag column names (system tags may be included; they never
            match a data name).
        data_names: The data column names.

    Returns:
        The names in both, in the order of ``tag_names``.
    """
    data = set(data_names)
    return tuple(name for name in tag_names if name in data)


def stream_references(stream: Any) -> tuple[str, ...]:  # pyright: ignore[reportExplicitAny]  # any object with keys(): a stream, source or node
    """Return the referential tags of ``stream``, known from its keys at graph-build time.

    Args:
        stream: A stream (anything with ``keys()`` returning tag and data names).

    Returns:
        The names that are both tag keys and data keys, in tag order.
    """
    tag_keys, data_keys = stream.keys()
    return reference_names(tag_keys, data_keys)


def check_namespaces(
    tag_schema: Mapping[str, Any],  # pyright: ignore[reportExplicitAny]  # Python types of any kind
    data_schema: Mapping[str, Any],  # pyright: ignore[reportExplicitAny]  # Python types of any kind
    references: Iterable[str],
) -> None:
    """Check the column namespaces of an output schema (sections 4.1 to 4.4).

    A name in both halves must be one of ``references`` (a materialized tag and a data
    column never share a name), and a reference has one type in both halves.

    Args:
        tag_schema: The output tag schema.
        data_schema: The output data schema.
        references: The names the producer keeps as referential tags.

    Raises:
        InputValidationError: If a tag and a data column share a name that is not a
            reference, or a reference's tag and data types differ.
    """
    refs = set(references)
    for name in reference_names(tag_schema, data_schema):
        if name not in refs:
            raise InputValidationError(
                f"tag {name!r} and data column {name!r} would share a name without "
                "being one referential column: a plain tag and a data column cannot "
                "share a name"
            )
        if tag_schema[name] != data_schema[name]:
            raise InputValidationError(
                f"referential tag {name!r} has type {tag_schema[name]} as a tag and "
                f"{data_schema[name]} as data"
            )


def warn_materialized(operation: object, names: Iterable[str]) -> None:
    """Report that ``operation`` turned the referential tags ``names`` into plain tags.

    Its job is the diagnostic of section 7.4, so it takes the operation. It reports
    once per operation object: a job validates its operators again when it builds and
    runs its nodes, and that is not a new structural step. An operation built while
    ``materialization_diagnostic_off`` is active (Orcapod's own step) never reports.
    Emits nothing when ``names`` is empty or ``reference_materialization`` is
    ``"silent"``.

    Args:
        operation: The structural operation (an operator instance).
        names: The tags that became plain.
    """
    names = tuple(names)
    if not names or id(operation) in _reported:
        return
    _reported[id(operation)] = operation
    if reference_materialization == "silent" or _diagnostic_off.get():
        return
    warnings.warn(
        f"{type(operation).__name__}: the data column of referential tag(s) {list(names)} was removed; "
        "they are now plain tags with the same values (set "
        "orcapod.core.referential_tags.reference_materialization = 'silent' to hide this)",
        ReferenceMaterializationWarning,
        stacklevel=3,
    )


# The marker of a referential tag in a stored table (developer, November 16, 2026,
# ITL-1019): Arrow field metadata on the stored tag column, at the level of a logical
# type's extension metadata. Only a referential tag carries it; a plain tag carries no
# marker. It is not ``ARROW:extension:*`` metadata, so no schema or table hash reads it.
REFERENTIAL_TAG_METADATA_KEY = b"orcapod:tag_kind"
REFERENTIAL_TAG_METADATA_VALUE = b"referential"


def mark_references(table: "pa.Table", references: Iterable[str]) -> "pa.Table":
    """Return ``table`` with exactly the columns ``references`` marked referential.

    A stored table states its own bindings: the writer marks the columns it knows to be
    referential and removes a marker any other column carries (one that came in from
    upstream metadata is not this writer's binding). Values and every other field
    property (name, type, nullability, other metadata) are unchanged.

    Args:
        table: The rows about to be stored.
        references: The referential tags of the rows; each must be a column of
            ``table``.

    Returns:
        The table with the marker on exactly the ``references`` columns.

    Raises:
        ValueError: If a reference is not a column of ``table``.
    """
    refs = set(references)
    if missing := refs - set(table.column_names):
        raise ValueError(
            f"referential tags {sorted(missing)} are not columns of the stored table"
        )
    schema = table.schema
    changed = False
    for index, field in enumerate(schema):
        metadata: dict[bytes | str, bytes | str] = {
            key: value for key, value in (field.metadata or {}).items()
        }
        marked = metadata.get(REFERENTIAL_TAG_METADATA_KEY) is not None
        if field.name in refs:
            if metadata.get(REFERENTIAL_TAG_METADATA_KEY) == (
                REFERENTIAL_TAG_METADATA_VALUE
            ):
                continue
            metadata[REFERENTIAL_TAG_METADATA_KEY] = REFERENTIAL_TAG_METADATA_VALUE
        elif marked:
            del metadata[REFERENTIAL_TAG_METADATA_KEY]
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


def stored_references(schema: "pa.Schema") -> tuple[str, ...]:
    """Return the columns a stored table marks as referential tags, in column order.

    Args:
        schema: The Arrow schema of a stored table.

    Returns:
        The names of the columns that carry the referential tag marker.

    Raises:
        ValueError: If a column carries the marker key with another value.
    """
    names: list[str] = []
    for field in schema:
        value = (field.metadata or {}).get(REFERENTIAL_TAG_METADATA_KEY)
        if value is None:
            continue
        if value != REFERENTIAL_TAG_METADATA_VALUE:
            raise ValueError(
                f"stored column {field.name!r} has {REFERENTIAL_TAG_METADATA_KEY!r} "
                f"metadata {value!r}; the only valid value is "
                f"{REFERENTIAL_TAG_METADATA_VALUE!r}"
            )
        names.append(field.name)
    return tuple(names)


def require_database_keeps_marker(database: object, references: Iterable[str]) -> None:
    """Refuse, before anything is written, a database that cannot store the marker.

    A stored table must state what it stores (developer, November 16, 2026, ITL-1019),
    so a referential tag is never stored unmarked. A database that checks rows before
    writing them (``RecordValidatingDatabaseProtocol``, the connector-backed databases)
    is asked whether it keeps the marker and refused if it does not. Any other database
    is accepted without a check: the in-memory and Delta databases keep Arrow field
    metadata.

    Args:
        database: The database a node will store its referential tags in.
        references: The referential tags it will store; nothing is checked when empty.

    Raises:
        InputValidationError: If the database cannot keep the marker, naming the
            database kind and the two ways out.
    """
    from orcapod.protocols.database_protocols import RecordValidatingDatabaseProtocol

    references = tuple(references)
    if not references or not isinstance(database, RecordValidatingDatabaseProtocol):
        return
    probe = mark_references(
        pa.table({name: pa.array([], type=pa.null()) for name in references}),
        references,
    )
    try:
        database.validate_records(probe)
    except ValueError as exc:
        raise InputValidationError(
            f"{type(database).__name__} cannot store the referential tags "
            f"{list(references)}: a stored referential tag carries Arrow field "
            f"metadata, and this database keeps none ({exc}). Use a database that "
            "keeps field metadata (DeltaTableDatabase, InMemoryArrowDatabase, the "
            "SpiralDB database), or keep these tags plain with tag_only_columns on "
            "the source. Nothing was written."
        ) from exc


def without_reference_marks(table: "pa.Table") -> "pa.Table":
    """Return ``table`` with no referential tag marker on any column.

    A stored table read into a stream drops the marker: in a stream the binding is the
    name intersection of the tag and data keys, and a marker carried further would be
    written by a downstream store whose binding it is not.

    Args:
        table: Rows read from a stored table.

    Returns:
        The same rows; no field carries the marker.
    """
    return mark_references(table, ())


def not_supported_yet(operation: str, issue: str) -> NotImplementedError:
    """Return the error for an operation that does not handle referential tags yet.

    Args:
        operation: The operation's name.
        issue: The Linear issue that will add the support.

    Returns:
        The error to raise.
    """
    return NotImplementedError(
        f"{operation}: not supported with referential tags yet ({issue}). Build the "
        "source with tag_only_columns=True to keep every tag plain, as in v0.3, or "
        "tag_only_columns=[...] to keep the named tags plain."
    )


TagOnlyColumns = str | Iterable[str] | bool
"""The ``tag_only_columns`` option of a source: names, or ``True`` for every tag."""


def source_reference_tags(
    tag_columns: Iterable[str], tag_only_columns: TagOnlyColumns
) -> tuple[str, ...]:
    """Return a source's referential tags: its declared tags minus the tag-only ones.

    Every declared tag of a source is referential by default (developer, November 12,
    2026): it is also a data column of the same name. ``tag_only_columns`` names the tags
    kept plain, exactly as in v0.3 (tag only, not data); ``True`` keeps all of them plain.

    Args:
        tag_columns: The source's declared tag columns.
        tag_only_columns: The tags kept plain: one name, several, ``True`` for all of
            them, or ``False`` / empty for none.

    Returns:
        The referential tags, in ``tag_columns`` order.

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
    combine: Callable[
        [Sequence[str | None]], str
    ] = arrow_utils.fold_source_info_tokens,
) -> SourceInfoValue:
    """Merge the source info of several equal values into the source info of one.

    The values merged are equal, so their source info has one shape: a token, or a
    list of them nested as deep as the data column (provenance keeps the shape of its
    data column; developer decision, November 13, 2026, ITL-1017). The result has
    that shape, and each of its tokens is ``combine`` of the tokens at that position:
    by default one merged token (``arrow_utils.fold_source_info_tokens``). This is
    what a ``Join`` does with the column of a tag that is referential on several of
    its inputs, and does not assume the column is a tag.

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
