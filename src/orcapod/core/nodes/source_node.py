"""Source node hierarchy for Pipeline and PipelineJob.

SourceNode — schema-only input-slot declaration.
SourceJobNode — execution variant that wraps a concrete StreamProtocol.
Both share SourceNodeBase which provides schema-based identity.
"""

from __future__ import annotations


import contextlib
import dataclasses
import functools
import json
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Collection, Iterator, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from orcapod import contexts
from orcapod.core.nodes.provenance_methods import ProvenanceMethods
from orcapod.config import OrcapodConfig
from orcapod.core.base import TraceableBase
from orcapod.core.datagrams import Data, Tag
from orcapod.core.operators.drop_nulls import _without_none  # pyright: ignore[reportPrivateUsage]  # DropNulls' rule for removing None from a type, shared, never copied
from orcapod.core.sources.base import table_is_rows
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import (
    ZERO_ROW_INPUT_COLUMNS,
    CompleteStreamFeed,
    keys_of_rows,
    reads_as_its_class,
    system_tags_as_rows_give,
    stream_arrow_schema,
    schema_requires_io,
    typed_arrow_schemas,
    typed_zero_row_input,
    typed_zero_row_table,
)
from orcapod.core.streams.record_batch_input import InputChannel
from orcapod.core.streams.view import (
    ViewStatus,
    StreamView,
    head_of,
    preview_html,
)
from orcapod.core.streams.base import (
    check_head_size,
)
from orcapod.errors import SourceSpecMismatchError, UnboundSourceError
from orcapod.logical_types.canonical_form import declaration_form_schema
from orcapod.logical_types.user_class_identity import equivalent_hints
from orcapod.system_constants import constants
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils import arrow_utils
from orcapod.utils.arrow_utils import system_tag_column_names
from orcapod.utils.schema_utils import (
    arrow_schema_identity,
    compute_source_schema_hash,
    declared_arrow_schema,
)

if TYPE_CHECKING:
    from orcapod.types.explanation import Explanation
    from orcapod.channels import WritableChannel
    from orcapod.protocols.core_protocols import StreamProtocol
    from orcapod.protocols.core_protocols.streams import (
        RecordBatchInput,
    )
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol

logger = logging.getLogger(__name__)


def _type_text(data_type: Any) -> str:
    """Return *data_type* as it is written in an annotation (``int``, ``int | None``)."""
    return data_type.__name__ if isinstance(data_type, type) else str(data_type)


def _saved_type_description(text: str) -> str:
    """Name the type a saved type text stands for, for a message.

    A user class is named by its identity record (``user class 'mod.A' (dataclass)``),
    any other extension type by its extension name; other text is quoted as it is.

    Args:
        text: A saved type text, e.g. ``extension<mod.A@<sha>;<hex of metadata>>``.
    """
    start = text.find("extension<")
    if start < 0:
        return f"type {text!r}"
    name, _, rest = text[start + len("extension<") :].partition(";")
    try:
        metadata = json.loads(bytes.fromhex(rest.split(">", 1)[0]))
    except ValueError:  # not hex, or not JSON (UnicodeDecodeError is a ValueError)
        metadata = None
    identity = metadata.get("identity") if isinstance(metadata, dict) else None
    if isinstance(identity, dict) and isinstance(identity.get("name"), str):
        return f"user class {identity['name']!r} ({identity.get('kind', 'class')})"
    return f"extension type {name!r}"


def _is_narrower(declared: Any, given: Any) -> bool:
    """Return whether *given* is *declared* without its ``None`` member.

    ``int`` is narrower than ``int | None`` and ``int | str`` than ``int | str | None``;
    nothing else is (developer, November 14, 2026, ITL-1112). Structural classes of one
    identity are one type here as everywhere (``equivalent_hints``).

    Args:
        declared: The slot's declared Python type of a column.
        given: The bound source's Python type of that column.
    """
    return declared != given and equivalent_hints(_without_none(declared), given)


def _arrow_field_text(field: "pa.Field[Any]") -> str:
    """The Arrow type of ``field`` with its top-level nullability, for a message."""
    return f"{field.type}{'' if field.nullable else ' not null'}"


@dataclasses.dataclass(frozen=True)
class _DeclaredForm:
    """How the rows of a bound source become rows of the declared schema.

    The pipeline determines every node's schema (ITL-1112). A slot declaring
    ``x: int | None`` accepts a source of ``x: int`` and gives its rows as rows of
    ``x: int | None``: the same arrays under nullable fields. The source's system-tag
    columns take the slot's names (their values stay the source's own). As a stable
    convenience, not a guarantee (column order is not part of a node's schema), the
    columns come in the declared order: tags, data and source info as declared, the
    system tags source id first. Nothing else
    differs: values, types, source-info tokens and meta columns pass unchanged, so the
    rows equal those of a source of the declared schema with the same source id.

    Attributes:
        nullable: The columns declared ``T | None`` that the source gives as ``T``.
        renames: The source's system-tag column names, mapped to the declared ones.
        tag_order: The declared tag columns, in declared order.
        data_order: The declared data columns, in declared order.
        system_tag_order: The declared system-tag columns, source id then record id.
    """

    nullable: frozenset[str]
    renames: Mapping[str, str]
    tag_order: tuple[str, ...]
    data_order: tuple[str, ...]
    system_tag_order: tuple[str, str]

    def apply(self, table: pa.Table) -> pa.Table:
        """Return *table*'s arrays under the declared fields and system-tag names.

        Serves a whole stream table and a single row's tag or data table: columns that
        are not in the table are skipped.

        Args:
            table: Rows read from the bound source.

        Returns:
            The same arrays, with the columns in ``nullable`` flagged nullable, the
            columns in ``renames`` renamed, and the tag, system-tag, data and
            source-info columns in declared order within the positions each group
            holds.
        """
        schema = table.schema
        for position, field in enumerate(table.schema):
            if field.name in self.nullable:
                field = field.with_nullable(True)
            if field.name in self.renames:
                field = field.with_name(self.renames[field.name])
            schema = schema.set(position, field)  # pyright: ignore[reportUnknownMemberType]  # stubs leave Field unparametrized
        # Same arrays under the declared fields: no cast, so no value or type changes.
        table = pa.Table.from_arrays(table.columns, schema=schema)
        return table.select(self._declared_positions(table.column_names))

    def _declared_positions(self, names: list[str]) -> list[int]:
        """Return the column positions of *names* with each group in declared order.

        The tag columns, the system-tag pair, the data columns and the data columns'
        source-info columns each fill the positions their group holds, in declared
        order; every other column keeps its position.
        """
        position = {name: index for index, name in enumerate(names)}
        order = list(range(len(names)))
        source_info = tuple(f"{constants.SOURCE_PREFIX}{n}" for n in self.data_order)
        groups = (self.tag_order, self.system_tag_order, self.data_order, source_info)
        placed = _placed(order, position, groups)
        if len(set(placed)) == len(placed):
            return placed
        # A data-tag is in the tag group and the data group, and placing it twice
        # filled one position twice and lost another column (ITL-1420): place it with
        # the tags only. Only for this case, so no order that was a placement before
        # changes.
        tags = set(self.tag_order)
        data = tuple(n for n in self.data_order if n not in tags)
        return _placed(
            order, position, (self.tag_order, self.system_tag_order, data, source_info)
        )


def _placed(
    order: list[int], position: dict[str, int], groups: Sequence[Sequence[str]]
) -> list[int]:
    """``order`` with each group's present names filling its positions in group order.

    Args:
        order: The column positions to start from.
        position: Each column name's position.
        groups: Groups of names, each in the order to place them.

    Returns:
        A new list of positions.
    """
    placed = list(order)
    for group in groups:
        present = [name for name in group if name in position]
        for slot, name in zip(
            sorted(position[n] for n in present), present, strict=True
        ):
            placed[slot] = position[name]
    return placed


def _system_tag_columns(
    source: StreamProtocol, tag_names: Collection[str]
) -> tuple[list[str], list[str], list[str]]:
    """Return *source*'s source-id, record-id and any other system-tag columns.

    A root source, or a stream that keeps its system tags (a filter, a rename), has one
    of each; a stream that joins or merges inputs has one pair per input; a stream over a
    table without them (a raw ``ArrowTableStream``) has none. A slot accepts only the
    first, also as a pipeline's input (ITL-1126).

    Args:
        source: The stream being bound.
        tag_names: Its user tag columns.

    Returns:
        The source-id columns, the record-id columns, and every other system-tag
        column, which no slot declares.
    """
    names = [
        name
        for name in source.keys(columns={"system_tags": True})[0]
        if name not in tag_names
    ]
    source_id = f"{constants.SYSTEM_TAG_SOURCE_ID_PREFIX}{constants.BLOCK_SEPARATOR}"
    record_id = f"{constants.SYSTEM_TAG_RECORD_ID_PREFIX}{constants.BLOCK_SEPARATOR}"
    source_ids = [name for name in names if name.startswith(source_id)]
    record_ids = [name for name in names if name.startswith(record_id)]
    others = [name for name in names if name not in {*source_ids, *record_ids}]
    return source_ids, record_ids, others


def _one_system_tag_pair_issue(
    source_ids: list[str], record_ids: list[str], others: list[str]
) -> str:
    """Return why a source without exactly one system-tag pair is refused, and the remedy.

    Args:
        source_ids: The source's source-id system-tag columns.
        record_ids: The source's record-id system-tag columns.
        others: The source's other system-tag columns.
    """
    found = sorted(source_ids + record_ids + others)
    issue = (
        "a source must carry exactly one source-id and one record-id system-tag "
        f"column and no other, not {found}"
    )
    if not found:
        remedy = (
            "a stream without system-tag columns (a raw ArrowTableStream) cannot be "
            "bound or used as a pipeline's input; wrap its table as a source, e.g. "
            "ArrowTableSource(stream.as_table(), tag_columns=[...])"
        )
    elif len(source_ids) > 1 or len(record_ids) > 1:
        remedy = (
            "a joined or merged stream cannot be bound; make the join part of the "
            "pipeline, or store the joined stream and bind the stored source"
        )
    else:
        return issue
    return f"{issue}: {remedy} (binding such a stream is planned, ITL-1127)"


_TAG_ROLE = frozenset({"tag"})
_BOTH_ROLES = frozenset({"tag", "data"})


def _roles(
    name: str, tag_schema: Mapping[str, Any], data_schema: Mapping[str, Any]
) -> frozenset[str]:
    """Return the roles ``name`` has in a pair of schemas: ``"tag"``, ``"data"``, both, none.

    A name in both is a data-tag (ITL-1023): one column in both roles.
    """
    return frozenset(
        role
        for role, schema in (("tag", tag_schema), ("data", data_schema))
        if name in schema
    )


def _data_tag_role_hint(
    name: str,
    *,
    source_roles: frozenset[str],
    slot_roles: frozenset[str],
    source_plain_tags: list[str],
) -> str | None:
    """Explain a role difference that a data-tag on one side causes, with the remedy.

    The bind refuses every role difference (ITL-1142: strict for now). The missing and
    unexpected columns it lists do not say that one side holds the column as a
    data-tag, a tag and a data column of one name; this does, and names how to
    make the two agree (ITL-1022).

    Args:
        name: The column name.
        source_roles: The roles the column has in the source (see ``_roles``).
        slot_roles: The roles the column has in the slot.
        source_plain_tags: The source's plain tags, in tag order. ``tag_only_columns``
            lists every plain tag, so the remedy names the exact list that changes
            ``name`` only.

    Returns:
        The explanation, or ``None`` when the roles agree, when neither side holds the
        column as a data-tag, or when one side lacks the column altogether.
    """
    if source_roles == slot_roles or not source_roles or not slot_roles:
        return None
    if source_roles == _BOTH_ROLES:
        if slot_roles == _TAG_ROLE:
            remedy = (
                f"build the source with tag_only_columns={[*source_plain_tags, name]} "
                "to keep "
                f"it a plain tag, or declare the slot with {name!r} as both a tag and "
                "a data column"
            )
            slot_role = "a plain tag"
        else:
            remedy = (
                f"build the source without {name!r} in tag_columns, or declare the "
                f"slot with {name!r} as both a tag and a data column"
            )
            slot_role = "a data column only"
        return (
            f"{name!r} is a data-tag of the source (a tag and a data column of "
            f"one name) but {slot_role} of the slot: {remedy}"
        )
    if slot_roles == _BOTH_ROLES:
        # A declared tag is a data-tag unless ``tag_only_columns`` names it.
        others = [tag for tag in source_plain_tags if tag != name]
        tag_only = f"tag_only_columns={others}" if others else "no tag_only_columns"
        if source_roles == _TAG_ROLE:
            source_role = "a plain tag"
            remedy = (
                f"build the source with {tag_only} (a declared tag is a data-tag "
                f"unless named there), or declare the slot with {name!r} as a plain tag"
            )
        else:
            source_role = "a data column only"
            remedy = (
                f"build the source with {name!r} in tag_columns and {tag_only}, or "
                f"declare the slot with {name!r} as a data column only"
            )
        return (
            f"{name!r} is {source_role} of the source but a data-tag of the "
            f"slot (a tag and a data column of one name): {remedy}"
        )
    return None


def _columns_without_arrow_type(
    tag_schema: Mapping[str, Any], data_schema: Mapping[str, Any], converter: Any
) -> list[tuple[str, Any]]:
    """The declared columns whose Python type has no Arrow type (ITL-640).

    A slot's identity is its Arrow schema, so every column a slot declares in Python
    needs the Arrow type the data context's converter gives it. A column whose saved
    type is still text (a class this process has not registered, ITL-1138) is left out.

    Args:
        tag_schema: The declared tag schema.
        data_schema: The declared data schema.
        converter: The data context's type converter.

    Returns:
        ``(column, declared type)`` for every column the converter cannot map, tag
        columns first, each group in declared order.
    """
    missing: list[tuple[str, Any]] = []
    seen: set[str] = set()
    for schema in (tag_schema, data_schema):
        for name, python_type in schema.items():
            if name in seen or isinstance(python_type, str):
                continue
            seen.add(name)
            try:
                declared_arrow_schema({name: python_type}, converter)
            except (ValueError, TypeError):
                missing.append((name, python_type))
    return missing


def _no_arrow_type_error(slot: str, missing: list[tuple[str, Any]]) -> ValueError:
    """The error for a slot declaring columns that have no Arrow type (ITL-640).

    Args:
        slot: The slot's name, which the error reports.
        missing: ``(column, declared type)`` of each such column
            (``_columns_without_arrow_type``).

    Returns:
        The ``ValueError`` naming the slot, each column and its declared type.
    """
    described = ", ".join(f"{column!r} is declared {kind}" for column, kind in missing)
    verb = "has" if len(missing) == 1 else "have"
    return ValueError(
        f"slot {slot!r}: {described}, which {verb} no Arrow type. A slot's types "
        "need an Arrow type: its identity is its Arrow schema (ITL-640). Declare "
        "the column with a type Orcapod stores, or declare the slot with a "
        "pa.Schema or from a stream."
    )


def _require_stream_arrow_types(stream: Any, slot: str) -> None:
    """Refuse a slot made from a stream whose declared columns have no Arrow type.

    A stream that types its zero-row table gives the slot its Arrow types; one that
    cannot (``typed_zero_row_table`` is ``None``) gives the converter's types of its
    Python schema, which must exist, as for a slot declared in Python (ITL-640).

    Args:
        stream: The stream the slot is made from.
        slot: The slot's name, which the error reports.

    Raises:
        ValueError: If a declared column of ``stream`` has no Arrow type.
    """
    if isinstance(getattr(stream, "arrow_schemas", None), tuple):
        return
    if typed_zero_row_table(stream) is not None:
        return
    tag_schema, data_schema = stream.output_schema()
    missing = _columns_without_arrow_type(
        tag_schema, data_schema, stream.data_context.type_converter
    )
    if missing:
        raise _no_arrow_type_error(slot, missing)


class _ExecuteOutputFeed:
    """The record batches of a source node's own ``execute()`` output.

    The channel of the output stream ``execute()`` gives (ITL-1347, stage 2: the table
    a job hands on, already in the row-built form), built at the first read: one
    binding is read, at that moment. A batch is dropped once delivered.
    """

    def __init__(self, node: SourceNodeBase) -> None:
        """Make the feed; nothing is read until the first read.

        Args:
            node: The source node whose output it delivers.
        """
        self._node = node
        self._output: StreamProtocol | None = None
        self._feed: CompleteStreamFeed | None = None

    def _output_feed(self) -> CompleteStreamFeed:
        """The feed of the node's output stream, made on the first read."""
        if self._feed is None:
            self._output = self._node.execute()
            self._feed = CompleteStreamFeed(self._output)
        return self._feed

    async def next(self) -> pa.RecordBatch | None:
        """Return the next record batch of the node's output, or ``None`` at the end."""
        return await self._output_feed().next()

    def close(self) -> None:
        """Stop reading: the batches not read are dropped."""
        if self._feed is not None:
            self._feed.close()
        else:
            self._feed = _closed_feed(self._node)

    @property
    def opened_on(self) -> StreamProtocol | None:
        """The node's output stream once the channel's first read made it, else
        ``None``.

        Inspecting it never runs the node or takes its binding: the first read does
        (``open_channel``'s guarantee). An unread source-node channel so answers
        ``None``, and a conversion (``to_stream``) reads it like any other.
        """
        return self._feed.opened_on if self._feed is not None else None


def _closed_feed(node: SourceNodeBase) -> CompleteStreamFeed:
    """A feed closed before its first read: it reads nothing and delivers nothing."""
    feed = CompleteStreamFeed(node)
    feed.close()
    return feed


class SourceNodeBase(TraceableBase, ABC):
    """Abstract base for SourceNode and SourceJobNode.

    Provides schema-based identity (content_hash, pipeline_hash) and
    shared properties.  Both sub-types carry identical schemas so their
    pipeline_hash() values always match; content_hash() diverges only
    when SourceJobNode has a concrete source bound.

    Args:
        name: The input-slot name used as the key in
            ``PipelineJob.bind(sources={name: source})``.
        tag_schema: Mapping of tag column names to Python types, or a ``pa.Schema``
            (then ``data_schema`` is one too): the slot holds exactly those Arrow
            types.
        data_schema: Mapping of data column names to Python types, or a ``pa.Schema``.
        data_context: Optional data context override.
        label: Optional display label override.
        config: Optional config override.
        arrow_schemas: The Arrow tag and data schemas of the slot's columns, when
            known (a slot made from a stream takes its stream's). ``None`` converts
            the declared Python types with the data context's type converter.
    """

    node_type = "source"

    def __init__(
        self,
        name: str,
        tag_schema: "Schema | pa.Schema",
        data_schema: "Schema | pa.Schema",
        data_context: str | contexts.DataContext | None = None,
        label: str | None = None,
        config: OrcapodConfig | None = None,
        arrow_schemas: "tuple[pa.Schema, pa.Schema] | None" = None,
    ) -> None:
        super().__init__(label=label, data_context=data_context, config=config)
        if isinstance(tag_schema, pa.Schema) or isinstance(data_schema, pa.Schema):
            # A slot declared in Arrow holds exactly those types (ITL-640).
            if not (
                isinstance(tag_schema, pa.Schema) and isinstance(data_schema, pa.Schema)
            ):
                raise TypeError(
                    "declare both tag_schema and data_schema as pa.Schema, or both "
                    "as Python schemas"
                )
            if arrow_schemas is not None:
                raise ValueError(
                    "arrow_schemas is given by tag_schema and data_schema already"
                )
            converter = self.data_context.type_converter
            # Logical types in one form first: a field declared with its identifier
            # in ARROW:extension:* metadata is that logical type (ITL-640).
            tag_schema = declaration_form_schema(tag_schema, converter)
            data_schema = declaration_form_schema(data_schema, converter)
            arrow_schemas = (tag_schema, data_schema)
            tag_schema = Schema(converter.arrow_schema_to_python_schema(tag_schema))
            data_schema = Schema(converter.arrow_schema_to_python_schema(data_schema))
        self._name = name
        self._tag_schema = tag_schema
        self._data_schema = data_schema
        # The Arrow types of the slot's columns, which its identity is built from
        # (ITL-640): those of the stream it was made from, or the conversion of the
        # declared Python types (``arrow_schemas``).
        self._given_arrow_schemas = arrow_schemas
        # A slot rebuilt from a saved job keeps the saved hashes (ITL-1138); see
        # ``_keep_saved``.
        self._saved_pipeline_hash: ContentHash | None = None
        self._saved_content_hash: ContentHash | None = None
        # The saved Arrow type texts of an unresolved loaded slot (a column's class not
        # registered yet): while set, the one source of truth for the slot's column
        # types, from which ``_resolve`` derives the Arrow pair and the Python types
        # together (ITL-640).
        self._saved_arrow_texts: dict[str, Any] | None = None
        if arrow_schemas is None:
            missing = _columns_without_arrow_type(
                tag_schema, data_schema, self.data_context.type_converter
            )
            if missing:
                raise _no_arrow_type_error(name, missing)

    # ------------------------------------------------------------------
    # Rebuilt from a saved job
    # ------------------------------------------------------------------

    def _keep_saved(
        self,
        *,
        pipeline_hash: str | None,
        content_hash: str | None,
        saved_types: Mapping[str, Mapping[str, str]],
        nullable: Mapping[str, Collection[str]],
    ) -> None:
        """Keep what a saved job recorded for this slot (ITL-1138, ITL-640).

        The slot returns the saved pipeline hash, and the saved content hash while no
        source is bound, as loaded function and operator nodes return theirs, so its
        stored address does not depend on what the loading process had registered.

        A slot whose saved types the load could not read whole (a user class nothing
        in this process has registered yet) is unresolved: its saved Arrow type texts
        become the one source of truth for its column types, every column (an
        ``int32`` stays ``int32``), until ``_resolve`` can read them all.

        Args:
            pipeline_hash: The saved pipeline hash text, or ``None`` if not saved.
            content_hash: The saved content hash text, or ``None`` if not saved.
            saved_types: The saved type texts, ``{"tag": {...}, "data": {...}}``.
            nullable: The saved nullable column names per role.
        """
        if pipeline_hash is not None:
            self._saved_pipeline_hash = ContentHash.from_string(pipeline_hash)
        if content_hash is not None:
            self._saved_content_hash = ContentHash.from_string(content_hash)
        if self._given_arrow_schemas is None or self._pending_columns():
            # One state: the texts hold the types, and the Arrow pair and the Python
            # types are both derived from them by ``_resolve``, together.
            self._given_arrow_schemas = None
            self._saved_arrow_texts = {
                "tag": dict(saved_types.get("tag", {})),
                "data": dict(saved_types.get("data", {})),
                "nullable": {role: list(names) for role, names in nullable.items()},
            }

    def _take_saved_state_of(self, other: "SourceNodeBase") -> None:
        """Keep the saved state of *other*, a slot of the same schema.

        The one way a copy of a slot takes it (``from_stream`` over a slot,
        ``from_slot``, ``as_node``, ...), so no copy drops a piece: the given Arrow
        schemas, the saved Arrow type texts of an unresolved slot (ITL-640) and the
        saved pipeline hash, which does not depend on the slot's name, are always
        kept. The label and saved content hash are kept when this slot has *other*'s
        name.
        """
        self._saved_pipeline_hash = other._saved_pipeline_hash
        self._given_arrow_schemas = other._given_arrow_schemas
        if self._name == other._name:
            self._label = other._label
            self._saved_content_hash = other._saved_content_hash
        self._saved_arrow_texts = other._saved_arrow_texts

    def _pending_columns(self) -> dict[str, str]:
        """The columns whose Python type is still their saved text, with their role.

        Derived, never kept: a column of an unresolved slot whose saved type this
        process cannot read as a Python type yet (ITL-1138).
        """
        return {
            name: role
            for role, schema in (("tag", self._tag_schema), ("data", self._data_schema))
            for name, value in schema.items()
            if isinstance(value, str)
        }

    def _resolve(self) -> bool:
        """Resolve an unresolved loaded slot, if its saved types can be read now.

        The one transition from unresolved to resolved (ITL-640; CodeRabbit on PR
        454): the Arrow pair is rebuilt from the saved Arrow type texts, the Python
        types are derived from the same texts, and both replace the slot's types
        together with the texts dropped, or nothing changes. A column saved as
        ``int32`` stays ``int32``: the Arrow types never come from Python types or
        converter defaults. Every reader of the slot's types (``arrow_schemas``, its
        identity, its system-tag names, ``_fit``, the saver) calls this first.

        Returns:
            Whether the slot is resolved (a slot that never was unresolved is).
        """
        texts = self._saved_arrow_texts
        if texts is None:
            return True
        # Local import: orcapod.pipeline imports the node modules.
        from orcapod.pipeline.serialization import (
            arrow_schema_from_texts,
            deserialize_schema,
        )

        converter = self.data_context.type_converter
        python: dict[str, dict[str, Any]] = {}
        arrow: list[pa.Schema] = []
        for role in ("tag", "data"):
            names = texts["nullable"][role]
            python[role] = deserialize_schema(texts[role], converter, nullable=names)
            if any(isinstance(value, str) for value in python[role].values()):
                return False
            try:
                arrow.append(arrow_schema_from_texts(texts[role], converter, names))
            except (ValueError, TypeError, KeyError):
                return False
        self._given_arrow_schemas = (arrow[0], arrow[1])
        self._tag_schema = Schema(
            python["tag"], optional_fields=self._tag_schema.optional_fields
        )
        self._data_schema = Schema(
            python["data"], optional_fields=self._data_schema.optional_fields
        )
        self._saved_arrow_texts = None
        # The system-tag names come from the schema hash, now of the resolved types.
        with contextlib.suppress(AttributeError):  # not computed yet
            del self._schema_hash_str
        self._invalidate_content_hash_cache()
        # A slot without a saved pipeline hash computed one from the type text.
        self._invalidate_pipeline_hash_cache()
        return True

    def _is_unresolved(self) -> bool:
        """Whether this loaded slot's saved types cannot be read yet (ITL-640)."""
        return not self._resolve()

    def saved_type_texts(self) -> dict[str, Any]:
        """The slot's column types as a saved file holds them: the one writer.

        ``{"tag": {...}, "data": {...}, "nullable": {...}}``, written by
        ``Pipeline.save`` as the slot's ``output_schema`` and its ``source_config``
        (and so by ``PipelineJob.save``, which embeds that pipeline). A resolved slot
        writes its own Arrow pair (``serialize_arrow_schemas``); an unresolved one its
        saved Arrow type texts and nullability as loaded, every column (an ``int32``
        stays ``int32``), never a conversion of its Python types (ITL-640; CodeRabbit
        on PR 454).
        """
        if self._resolve():
            from orcapod.pipeline.serialization import serialize_arrow_schemas

            return serialize_arrow_schemas(
                *self.arrow_schemas, self.data_context.type_converter
            )
        texts = self._saved_arrow_texts
        assert texts is not None  # unresolved
        return {
            "tag": dict(texts["tag"]),
            "data": dict(texts["data"]),
            "nullable": {
                role: list(names) for role, names in texts["nullable"].items()
            },
        }

    def _bound_stream(self) -> "StreamProtocol | None":
        """The concrete stream bound to this slot; a declaration has none."""
        return None

    def content_hash(self, hasher=None) -> ContentHash:
        """Return the content hash; a slot rebuilt from a saved job returns the saved one.

        The saved value stands while no source is bound (ITL-1138); a bound source gives
        its own identity, as for any slot.
        """
        if self._saved_content_hash is not None and self._bound_stream() is None:
            return self._saved_content_hash
        return super().content_hash(hasher)

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """Return the pipeline hash; a slot rebuilt from a saved job returns the saved one.

        A slot's saved type of a user class this process has not registered is text
        until a source is bound, so recomputing would give another value than the run's
        (ITL-1138), as for loaded function and operator nodes.
        """
        if self._saved_pipeline_hash is not None:
            return self._saved_pipeline_hash
        return super().pipeline_hash(hasher)

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def arrow_schemas(self) -> "tuple[pa.Schema, pa.Schema]":
        """The Arrow tag and data schemas of the slot's columns (ITL-640).

        A slot made from a stream holds the Arrow types of that stream's rows; a slot
        declared with Python types holds the data context's conversion of them. The
        slot's identity, and so every pipeline hash and stored path below it, is built
        from these schemas, and a bound source must have them.
        """
        if not self._resolve():
            # A loaded slot whose saved types cannot be read yet is unresolved: its
            # types are the saved ones, never converter defaults.
            raise ValueError(self._unresolved_message())
        if self._given_arrow_schemas is not None:
            return self._given_arrow_schemas
        converter = self.data_context.type_converter
        return (
            declared_arrow_schema(self._tag_schema, converter),
            declared_arrow_schema(self._data_schema, converter),
        )

    def _unresolved_message(self) -> str:
        """The error of a loaded slot whose saved Arrow types cannot be rebuilt yet."""
        pending = sorted(self._pending_columns()) or sorted(
            {
                **(self._saved_arrow_texts or {}).get("tag", {}),
                **(self._saved_arrow_texts or {}).get("data", {}),
            }
        )
        return (
            f"SourceNode {self._name!r} was loaded with columns {pending} whose saved "
            "types this process cannot read yet (a user class not registered): its "
            "Arrow types are the saved ones and cannot be rebuilt until the class is "
            "registered; bind a source of that class or define a function pod that "
            "uses it first."
        )

    def _schema_identity(self) -> Any:
        """The hashes of the slot's Arrow schemas: ``schema_utils.arrow_schema_identity``.

        A slot rebuilt from a saved job with a column whose class this process has not
        registered (ITL-1138) has no Arrow type for it yet: until the class is
        registered, the saved type text stands in, as the saved hashes do.
        """
        if not self._resolve():
            return (self._tag_schema, self._data_schema)
        return arrow_schema_identity(*self.arrow_schemas, self.data_context)

    def identity_structure(self) -> Any:
        """Return the content identity: ``("source_node", name, schema identity)``."""
        return ("source_node", self._name, self._schema_identity())

    def pipeline_identity_structure(self) -> Any:
        """Return the pipeline identity: the slot's Arrow schema identity (name-independent).

        Sources with equal Arrow schemas share the same DB table paths regardless
        of name; sources whose Arrow types differ do not (ITL-640).
        """
        return self._schema_identity()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """The input-slot name used as the key in ``PipelineJob.bind(sources={...})``."""
        return self._name

    def computed_label(self) -> str | None:
        """Resolve the node label to the slot name.

        Implements ``LabelableMixin.computed_label()`` so that ``self.label``
        resolves to the slot name without an explicit label assignment.

        Returns:
            The slot name.
        """
        return self.name

    @property
    def tag_schema(self) -> Schema:
        """Tag schema for this input slot (resolved first, ``_resolve``)."""
        self._resolve()
        return self._tag_schema

    @property
    def data_schema(self) -> Schema:
        """Data schema for this input slot (resolved first, ``_resolve``)."""
        self._resolve()
        return self._data_schema

    @functools.cached_property
    def _schema_hash_str(self) -> str:
        """Cached schema hash used for system-tag column naming.

        Produces the same hash that ``SourceStreamBuilder`` embeds in system-tag
        column names, making it possible to predict those names from the declared
        schemas alone without requiring a live source. Computed once and cached
        since ``_tag_schema`` and ``_data_schema`` are immutable after construction.

        Returns:
            Hex string schema hash.
        """
        if not self._resolve():
            # The saved type text stands in until the class is registered (ITL-1138).
            return self.data_context.semantic_hasher.hash_object(
                (self._tag_schema, self._data_schema)
            ).to_hex(char_count=self.orcapod_config.hashing.schema_n_char)
        tag_schema, data_schema = self.arrow_schemas
        return compute_source_schema_hash(
            tag_schema,
            data_schema,
            self.data_context,
            self.orcapod_config,
        )

    @property
    def node_uri(self) -> tuple[str, ...]:
        """Canonical URI tuple for this source node.

        Returns a tuple identifying this node as a named schema-only source slot.
        """
        return ("source_node", self._name)

    def explain(self) -> Explanation:
        """Say what the source bound here applies itself (``StreamBase.explain``).

        Returns:
            An ``Explanation``; for a slot with no source bound, that it has none.
        """
        from orcapod.core.streams.explain import explain

        return explain(self)

    @property
    def producer(self) -> None:
        """Source nodes have no producer pod — they are root nodes.

        Returns:
            Always ``None``.
        """
        return None

    @property
    def upstreams(self) -> "tuple[StreamProtocol, ...]":
        """Source nodes have no upstream streams — they are root nodes.

        Returns:
            Always an empty tuple.
        """
        return ()

    @upstreams.setter
    def upstreams(self, value: "tuple[StreamProtocol, ...]") -> None:
        if len(value) != 0:
            raise ValueError("SourceNode upstreams must be empty")

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return ``(tag_schema, data_schema)``.

        When ``columns.system_tags`` is ``True`` (or ``all_info=True``), the
        returned tag schema is extended with the two system-tag entries for this
        node's schema: the source id and the record id, both ``str``, the types every
        source's rows have (Arrow ``large_string``).
        Their names are deterministic from the declared schemas and match what a
        concrete source with the same schema would produce.

        Other ``ColumnConfig`` flags (``meta``, ``context``, ``source``,
        ``content_hash``, ``sort_by_tags``) are no-ops at the node level —
        consistent with ``ArrowTableStream.output_schema()`` which also ignores them.

        Args:
            columns: Column selection config.
            all_info: If ``True``, equivalent to ``ColumnConfig(system_tags=True)``
                for this method.

        Returns:
            Tuple of ``(tag_schema, data_schema)``.
        """
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
        # A loaded slot's Python types are derived from its saved texts (ITL-640).
        self._resolve()
        tag_schema = self._tag_schema
        if columns_config.system_tags:
            source_id_col, record_id_col = system_tag_column_names(
                self._schema_hash_str
            )
            tag_schema = Schema(
                {**dict(tag_schema), source_id_col: str, record_id_col: str}
            )
        return tag_schema, self._data_schema

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return ``(tag_keys, data_keys)``.

        Derived from ``output_schema()`` to ensure the two methods are always
        consistent. See ``output_schema()`` for the full description of which
        ``ColumnConfig`` flags are honoured at the node level.

        Args:
            columns: Column selection config.
            all_info: If ``True``, include all available column groups.

        Returns:
            Tuple of ``(tag_column_names, data_column_names)``.
        """
        tag_schema, data_schema = self.output_schema(columns=columns, all_info=all_info)
        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self, source: "StreamProtocol") -> None:
        """Check that *source* fits this slot's declared schema.

        The source's tag and data columns must have the declared names and roles, and
        each the declared Python type, or, where the slot declares ``T | None``, ``T``
        (ITL-1112). Those are what every downstream pipeline hash, and so every stored
        path, is built from, so a source that fits never moves one. Column order and
        ``optional_fields`` are not compared; neither enters the pipeline hash. The
        source must also carry exactly one source-id and one record-id system-tag
        column, which take the slot's names; a joined or merged stream, or one with no
        system tags, is refused (ITL-1126).

        Args:
            source: A concrete stream to validate.

        Raises:
            SourceSpecMismatchError: If a column is missing, unexpected, or of a type
                the declared one does not accept, or the source's system-tag columns
                are not exactly one source-id and one record-id column.
        """
        _ = self._fit(source)

    def _fit(self, source: "StreamProtocol") -> tuple[Schema, Schema, frozenset[str]]:
        """Check *source* against the declared schema, as ``validate`` describes.

        Args:
            source: A concrete stream to check.

        Returns:
            The source's tag and data schemas, and the names of the columns it gives as
            ``T`` where the slot declares ``T | None``.

        Raises:
            SourceSpecMismatchError: If *source* does not fit.
        """
        source_tag, source_data = source.output_schema()

        issues: list[str] = []
        # The one transition (ITL-640): a source being bound has registered the class
        # it holds, so a slot saved with that class resolves now, whole, or not at all.
        resolved = self._resolve()
        unrestored = self._pending_columns()
        texts = self._saved_arrow_texts or {"tag": {}, "data": {}}
        for name, role in unrestored.items():
            saved = _saved_type_description(texts[role][name])
            issues.append(
                f"{role} column {name!r} was saved as {saved}, which is not registered "
                "in this process, so the source cannot be checked against it: bind a "
                "source of that class (building one registers it) or define a function "
                "pod that uses it before binding"
            )
        if not resolved and not unrestored:
            # Every column has a Python type, but the saved Arrow types still cannot be
            # rebuilt: the slot is unresolved, never checked against substituted types.
            issues.append(self._unresolved_message())
        narrower: set[str] = set()
        # The Arrow types of the slot's and the source's columns, read once (ITL-640);
        # an unresolved slot has no Arrow schema to compare.
        compare_arrow = resolved
        source_arrow: dict[str, pa.Schema] = (
            dict(zip(("tag", "data"), typed_arrow_schemas(source)))
            if compare_arrow
            else {}
        )
        slot_arrow: dict[str, pa.Schema] = (
            dict(zip(("tag", "data"), self.arrow_schemas)) if compare_arrow else {}
        )
        for role, declared, given in (
            ("tag", self._tag_schema, source_tag),
            ("data", self._data_schema, source_data),
        ):
            missing = set(declared.keys()) - set(given.keys())
            extra = set(given.keys()) - set(declared.keys())
            if missing:
                issues.append(f"missing {role} columns: {sorted(missing)}")
            if extra:
                issues.append(f"unexpected {role} columns: {sorted(extra)}")
            for name in declared.keys():
                if name in unrestored:
                    continue
                if name not in given:
                    continue
                if equivalent_hints(given[name], declared[name]):
                    pass
                elif _is_narrower(declared[name], given[name]):
                    narrower.add(name)
                else:
                    issues.append(
                        f"{role} column {name!r} is declared "
                        f"{_type_text(declared[name])} but the source has "
                        f"{_type_text(given[name])}"
                    )
                    continue
                if not compare_arrow:
                    continue
                arrow_issue = self._arrow_type_issue(
                    role, name, slot_arrow[role], source_arrow[role]
                )
                if arrow_issue is not None:
                    issues.append(arrow_issue)

        # A name whose roles differ only because one side holds it as a data-tag
        # (a tag and a data column of one name) is refused like any role difference
        # (ITL-1142: strict for now); say so, and how to make the two agree (ITL-1022).
        for name in sorted(
            {*self._tag_schema, *self._data_schema, *source_tag, *source_data}
        ):
            hint = _data_tag_role_hint(
                name,
                source_roles=_roles(name, source_tag, source_data),
                slot_roles=_roles(name, self._tag_schema, self._data_schema),
                # ``tag_only_columns`` lists every plain tag, so a remedy keeps the
                # source's other plain tags plain.
                source_plain_tags=[tag for tag in source_tag if tag not in source_data],
            )
            if hint is not None:
                issues.append(hint)

        # The slot's rows carry one system-tag pair under the slot's names, which only
        # a source with exactly one pair can take. A joined or merged stream (one pair
        # per input) or a stream with none is refused (developer, November 14, 2026,
        # ITL-1126); binding one properly is ITL-1127.
        source_ids, record_ids, others = _system_tag_columns(source, source_tag.keys())
        if len(source_ids) != 1 or len(record_ids) != 1 or others:
            issues.append(_one_system_tag_pair_issue(source_ids, record_ids, others))
        else:
            # The pair must have the types the slot predicts for it (str, str).
            given_tags = source.output_schema(columns={"system_tags": True})[0]
            for name, declared_type in ((source_ids[0], str), (record_ids[0], str)):
                if given_tags[name] != declared_type:
                    issues.append(
                        f"system-tag column {name!r} is declared "
                        f"{_type_text(declared_type)} but the source has "
                        f"{_type_text(given_tags[name])}"
                    )

        if issues:
            raise SourceSpecMismatchError(
                f"SourceNode '{self._name}' is not compatible with the provided source. "
                + "; ".join(issues)
            )
        return source_tag, source_data, frozenset(narrower)

    def _arrow_type_issue(
        self,
        role: str,
        name: str,
        slot_schema: "pa.Schema",
        source_schema: "pa.Schema",
    ) -> str | None:
        """Compare one column's Arrow type in the slot and in *source* (ITL-640).

        The slot's identity is built from its Arrow schemas, so a source fits only
        with the same Arrow type, as ``hash_schema`` compares it (column order, large
        and non-large variants, list item names, dictionary encoding and field
        metadata aside), at every depth; the one difference accepted is a non-null
        column where the slot's column is nullable (``T`` into ``T | None``). Nested
        nullability must match exactly.

        Args:
            role: ``"tag"`` or ``"data"``.
            name: The column name.
            slot_schema: The slot's Arrow schema of that role.
            source_schema: The source's Arrow schema of that role.

        Returns:
            The issue text, or ``None`` when the types agree.
        """
        if name not in slot_schema.names or name not in source_schema.names:
            return None
        declared = slot_schema.field(name)
        given = source_schema.field(name)
        if declared.nullable and not given.nullable:
            given = given.with_nullable(True)
        hasher = self.data_context.arrow_hasher
        if hasher.hash_schema(pa.schema([declared])) == hasher.hash_schema(
            pa.schema([given])
        ):
            return None
        issue = (
            f"{role} column {name!r} has the Arrow type {_arrow_field_text(declared)} "
            f"in the slot but {_arrow_field_text(source_schema.field(name))} in the "
            "source"
        )
        if self._given_arrow_schemas is None:
            python_type = (self._tag_schema if role == "tag" else self._data_schema)[
                name
            ]
            issue += (
                f" (declared {_type_text(python_type)} means "
                f"{_arrow_field_text(declared)}; declare the slot from the source or "
                "with an Arrow schema)"
            )
        return issue

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        """Materialize stream as a PyArrow Table.

        Delegates to the concrete source (SourceJobNode), or raises for
        schema-only SourceNode.

        Args:
            columns: Column selection config.
            all_info: If True, include all metadata columns.

        Raises:
            UnboundSourceError: When no concrete data is available.
        """
        # Calling iter_data() will raise UnboundSourceError for SourceNode,
        # or delegate to concrete for SourceJobNode.
        # For SourceJobNode with a concrete source, delegate directly.
        raise UnboundSourceError(
            f"SourceNode '{self._name}' is not bound to a concrete source. "
            "Use PipelineJob.bind() to attach data before calling as_table()."
        )

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return no rows: an input slot holds no data until a source is bound.

        See ``StreamBase.head``.
        """
        check_head_size(n)
        return self._head_without_rows(
            ColumnConfig.handle_config(columns, all_info=all_info),
            ViewStatus.UNAVAILABLE,
            note="input slot; bind a source",
        )

    def _head_without_rows(
        self, config: ColumnConfig, status: ViewStatus, note: str | None = None
    ) -> StreamView:
        """Return a head with no rows: the slot's columns from its declared schema."""
        tag_schema, data_schema = self.output_schema(columns=config)
        table = arrow_utils.make_empty_table(
            {**tag_schema, **data_schema}, self.data_context.type_converter
        )
        return StreamView.without_rows(
            self, status, table, tag_columns=tuple(tag_schema.keys()), note=note
        )

    def _repr_html_(self) -> str:
        """Render ``head()`` in tag order for a notebook; never computes."""
        return preview_html(self)

    def run(self) -> StreamView:
        """Read the bound source's rows and return a ``StreamView`` of all of them.

        Does the work ``as_table()`` does (``StreamBase.run``): a ``SourceJobNode`` reads
        its bound source (polling or reading it as that source's own ``run()`` does), and
        an input slot with no source bound raises.

        Raises:
            UnboundSourceError: When no concrete source is bound.
        """
        table = self.as_table()
        return StreamView.of_rows(
            self,
            table,
            tag_columns=self.keys()[0],
            total_rows=table.num_rows,
            display_all_rows=False,
        )

    def open_channel(self) -> RecordBatchInput:
        """Open an input channel on this node's rows, for async computation (ITL-1347).

        The channel carries exactly this node's own ``execute()`` output: its rows,
        as the table a job builds from them, in Arrow record batches. Everything
        (the binding, the declared form, the column order, the source info) is read
        at the first read, from one binding; opening reads nothing. The description
        is this node. An unbound node's first read raises what reading it raises.

        Returns:
            The input channel.
        """
        return InputChannel(self, _ExecuteOutputFeed(self))

    async def async_iter_data(self):
        """Asynchronous iterator over (tag, data) pairs.

        Yields:
            tuple[TagProtocol, DataProtocol]: A ``(tag, data)`` pair from the
            concrete source.  Raises before yielding anything when unbound.

        Raises:
            UnboundSourceError: When no concrete data is available.
        """
        for pair in self.iter_data():
            yield pair

    # ------------------------------------------------------------------
    # Abstract
    # ------------------------------------------------------------------

    @abstractmethod
    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Yield ``(tag, data)`` pairs, or raise if data is unavailable."""
        ...

    def _rows_table(
        self,
    ) -> tuple[pa.Table, tuple[tuple[str, ...], tuple[str, ...]]] | None:
        """The table of this node's rows (``iter_data``) and the rows' keys, or ``None``.

        ``None`` here: a node that carries no data of its own hands on its rows
        (``RowsOnRead``), which raise what reading it raises. A bound job node gives
        its bound source's table when F of it is provably the rows' own table.
        """
        return None

    def arrow_schema(self) -> pa.Schema:
        """The Arrow schema of the columns this stream passes between job nodes.

        See ``stream_arrow_schema`` (ITL-1347): never reads, computes or polls a row.
        The columns are in the order of the node's rows (``_row_keys``).
        """
        return stream_arrow_schema(self, self._row_keys())

    def _row_keys(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """The tag and data keys of this node's rows: its own keys."""
        return self.keys()

    @property
    def arrow_types_declared(self) -> bool:
        """Whether ``arrow_schema()`` holds the rows' own Arrow types (ITL-1089, ITL-1347)."""
        return typed_zero_row_table(self) is not None

    def execute(
        self,
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> StreamProtocol:
        """Execute this source node: read its source once and return its output stream.

        The source's table, as the table built from its rows would be
        (``StaticOutputOperatorPod._output_stream``), with this node's identity, built
        without making rows (ITL-1347, stage 2). The sync orchestrator hands it to every
        successor.

        Args:
            observer: Optional execution observer.

        Returns:
            The node's output stream.

        Raises:
            UnboundSourceError: When no concrete data is available.
        """
        from orcapod.core.operators.static_output_pod import (
            RowBuiltFormUnavailable,
            StaticOutputOperatorPod,
            rows_on_read,
        )

        node_label = self.label
        node_hash = self.content_hash().to_string()
        if observer is not None:
            observer.on_node_start(node_label, node_hash)
        # The table and keys of this node's rows, when F of the table is provably the
        # rows' own table; otherwise (a narrower bind) the rows, built when read.
        rows_table = self._rows_table()
        result: StreamProtocol
        if rows_table is None:
            result = rows_on_read(self.iter_data(), self)
        else:
            table, keys = rows_table
            try:
                result = StaticOutputOperatorPod._output_stream(table, keys, self)
            except RowBuiltFormUnavailable:
                # The rows decide (read once, here), built when read, as on main.
                result = rows_on_read(self.iter_data(), self)
        if observer is not None:
            observer.on_node_end(node_label, node_hash)
        return result

    async def async_execute(
        self,
        output: "WritableChannel[tuple[TagProtocol, DataProtocol]]",
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> None:
        """Push all (tag, data) pairs to the output channel.

        Delegates to ``async_iter_data`` so that dynamic sources
        (e.g. ``PollingSource``) stream continuously without modification
        to this node.

        Args:
            output: Channel to write results to.
            observer: Optional execution observer.

        Raises:
            UnboundSourceError: When no concrete data is available.
        """
        node_label = self.label
        node_hash = self.content_hash().to_string()
        try:
            if observer is not None:
                observer.on_node_start(node_label, node_hash)
            async for tag, data in self.async_iter_data():
                await output.send((tag, data))
            if observer is not None:
                observer.on_node_end(node_label, node_hash)
        finally:
            await output.close()

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(name={self._name!r}, "
            f"tag_schema={dict(self._tag_schema)!r}, "
            f"data_schema={dict(self._data_schema)!r})"
        )

    def _declared_zero_row_table(self) -> "pa.Table | None":
        """A slot's zero-row table, typed by the slot's own Arrow schemas.

        A slot made from a stream, or rebuilt from a saved job, holds the Arrow types
        of its columns (ITL-640): its rows will have them, since a bind accepts only a
        source of those types. A slot declared with Python types only, or one with a
        column whose class is not registered, answers ``None`` (the declared Python
        schema stands in, ITL-1089).
        """
        if not self._resolve() or self._given_arrow_schemas is None:
            return None
        tag_schema, data_schema = self.output_schema(columns=ZERO_ROW_INPUT_COLUMNS)
        own = {field.name: field for schema in self.arrow_schemas for field in schema}
        fields = []
        for name, python_type in {**tag_schema, **data_schema}.items():
            if name in own:
                fields.append(own[name])
            else:
                fields.append(
                    declared_arrow_schema(
                        {name: python_type}, self.data_context.type_converter
                    ).field(name)
                )
        return pa.Table.from_batches([], schema=pa.schema(fields))


class SourceNode(SourceNodeBase):
    """Schema-only input-slot declaration for ``Pipeline`` recording.

    Replaces ``SourceSpec`` as the user-facing way to declare typed pipeline
    inputs.  Pass a ``SourceNode`` inside a ``with pipeline:`` block as the
    upstream for any pod invocation.

    Example::

        slot = SourceNode(name="data", tag_schema={"id": int}, data_schema={"v": float})
        with pipeline:
            result = my_pod(slot)

        job = PipelineJob.from_pipeline(pipeline, database=db, sources={"data": my_source})
        job.run()

    Hash-stability note:
        ``identity_structure()`` returns ``("SourceSpec", name, tag_schema, data_schema)``
        — identical to the old ``SourceSpec`` — so existing DB paths remain valid.
    """

    def _typed_zero_row_table(self) -> "pa.Table | None":
        """The slot's zero-row table from its own Arrow schemas (ITL-640).

        A slot made from a stream or rebuilt from a saved job types its zero-row table
        from the Arrow types it holds, so the nodes over it type theirs too (a saved
        job writes those types); a slot declared with Python types only answers
        ``None``. See ``_declared_zero_row_table``.
        """
        return self._declared_zero_row_table()

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Raise ``UnboundSourceError`` — ``SourceNode`` carries no data.

        Raises:
            UnboundSourceError: Always.
        """
        raise UnboundSourceError(
            f"SourceNode '{self._name}' is not bound to a concrete source. "
            "Use PipelineJob.from_pipeline(..., sources={'<name>': source}) "
            "or job.bind(sources={'<name>': source}) to attach data."
        )

    @classmethod
    def from_stream(
        cls,
        stream: StreamProtocol,
        name: str | None = None,
    ) -> SourceNode:
        """Wrap *stream* in a ``SourceNode``, or return it unchanged if already one.

        Derives tag and data schemas from ``stream.output_schema()``.  The slot
        name is resolved in priority order:

        1. *name* parameter (explicit override — highest priority).
        2. ``stream.source_name`` when *stream* is a ``RootSource``: its source id
           without the kind part (ITL-1425), so a slot keeps its name whichever kind
           or implementation version reads the data, stable across pipeline
           serialisation / deserialisation.
        3. The existing slot name when *stream* is a ``SourceJobNode``.
        4. ``"{stream.label}:{hash}"`` for any other concrete stream type without
           a producer. A stream made by a pod requires an explicit *name*.

        Args:
            stream: The upstream stream to wrap.
            name: Optional explicit slot name.

        Returns:
            The original *stream* if it is already a ``SourceNode``; otherwise
            a new ``SourceNode`` with schemas derived from *stream*.
        """
        if isinstance(stream, SourceNode):
            return stream
        tag_schema, data_schema = stream.output_schema()
        if name is not None:
            slot_name = name
        else:
            # Local import avoids a module-level circular dependency between
            # core.nodes and core.sources.
            from orcapod.core.sources.base import RootSource

            if isinstance(stream, RootSource):
                slot_name = stream.source_name
            elif isinstance(stream, SourceNodeBase):
                # A job's slot keeps its name in a blueprint (ITL-1387).
                slot_name = stream.name
            else:
                _require_name_for_pod_stream(stream)
                slot_name = f"{stream.label}:{stream.content_hash().to_hex()}"
        if not isinstance(stream, SourceNodeBase):
            _require_stream_arrow_types(stream, slot_name)
        node = cls(
            name=slot_name,
            tag_schema=tag_schema,
            data_schema=data_schema,
            arrow_schemas=(
                stream._given_arrow_schemas  # pyright: ignore[reportPrivateUsage]  # one slot copied into another
                if isinstance(stream, SourceNodeBase)
                else typed_arrow_schemas(stream)
            ),
        )
        if isinstance(stream, SourceNodeBase):
            # A loaded job's slot keeps all its saved state (ITL-1138, ITL-640),
            # through the one method every copy of a slot uses.
            node._take_saved_state_of(stream)
        return node


class SourceJobNode(ProvenanceMethods, SourceNodeBase):
    """Execution-ready source node wrapping an optional concrete stream.

    Used inside ``PipelineJob._persistent_node_map``.  The ``_concrete``
    field is **mutable** — ``PipelineJob.bind(sources={...})`` updates it
    in-place so that downstream ``FunctionJobNode`` objects (which hold a
    reference to this same object) automatically see the new concrete source
    without cascading reference updates.

    Hash behaviour:

    * ``content_hash()`` — delegates to the bound source's
      ``identity_structure()`` when bound (via ``identity_structure()``
      override); falls back to schema-based identity when unbound.
    * ``pipeline_hash()`` — always schema-based (inherited); never
      data-inclusive.  This invariant keeps DB paths stable across different
      data sources bound to the same slot.

    Args:
        name: Slot name.
        tag_schema: Tag schema.  Optional when *bound_source* is provided;
            inferred from ``bound_source.output_schema()`` when omitted.
            Stored for future rebind validation.
        data_schema: Data schema.  Optional when *bound_source* is provided;
            inferred from ``bound_source.output_schema()`` when omitted.
            Stored for future rebind validation.
        bound_source: Optional concrete stream.  Can be set or replaced later
            via ``job_node.bound_source = source``.
        data_context: Optional data context override.

    Raises:
        ValueError: If both *tag_schema* and *data_schema* are omitted and
            *bound_source* is ``None``.
    """

    def __init__(
        self,
        name: str,
        tag_schema: Schema | None = None,
        data_schema: Schema | None = None,
        bound_source: StreamProtocol | None = None,
        data_context: str | contexts.DataContext | None = None,
        arrow_schemas: "tuple[pa.Schema, pa.Schema] | None" = None,
    ) -> None:
        if tag_schema is None or data_schema is None:
            if bound_source is None:
                raise ValueError(
                    "SourceJobNode requires either explicit tag_schema/data_schema "
                    "or a bound_source to derive schemas from."
                )
            derived_tag, derived_data = bound_source.output_schema()
            if arrow_schemas is None:
                # Each role's Arrow schema from where that role comes from: an omitted
                # role takes the source's, a declared one its declaration's (one Arrow
                # type per Python type), so a declared role is never replaced by the
                # source's types (CodeRabbit on PR 454).
                source_tag, source_data = typed_arrow_schemas(bound_source)
                converter = contexts.resolve_context(data_context).type_converter
                arrow_schemas = (
                    source_tag
                    if tag_schema is None
                    else declared_arrow_schema(tag_schema, converter),
                    source_data
                    if data_schema is None
                    else declared_arrow_schema(data_schema, converter),
                )
            if tag_schema is None:
                tag_schema = derived_tag
            if data_schema is None:
                data_schema = derived_data
        super().__init__(
            name=name,
            tag_schema=tag_schema,
            data_schema=data_schema,
            data_context=data_context,
            arrow_schemas=arrow_schemas,
        )
        self._bound_source: StreamProtocol | None = None
        self._declared_form: _DeclaredForm | None = None
        if bound_source is not None:
            # Checks the source and, for a narrower one, records how its rows become
            # rows of the declared schema (ITL-1112), as the setter does, but does not
            # stamp the slot as modified (ITL-1095). super().__init__() has
            # initialised the hash caches it invalidates.
            self._bind(bound_source)

    # ------------------------------------------------------------------
    # bound_source property — explicit binding with cache invalidation
    # ------------------------------------------------------------------

    def run(self, *, load_only: bool = False) -> StreamView:
        """Read the bound source's rows and return a ``StreamView`` of all of them.

        As every job node's ``run()``, it first checks the bound source for new data
        (``check_sources``, ITL-1102): a check that raises warns
        (``SourceCheckFailedWarning``) and the source's held rows are read. A source is
        read, never computed, so ``load_only`` changes nothing, except that a load-only
        run refuses a bound stream that computes or delivers when read (ITL-1121); a job
        node's ``run()`` takes it (ITL-1095).

        Args:
            load_only: Accepted for every job node; a source reads its rows either way.

        Raises:
            UnboundSourceError: When no concrete source is bound.
            ValueError: If ``load_only`` and the bound stream computes or delivers
                when read (``node_run.refuse_unloadable_inputs``).
        """
        from orcapod.pipeline.node_run import check_sources, refuse_unloadable_inputs

        _ = check_sources([self])
        if load_only:
            refuse_unloadable_inputs(self)
        return super().run()

    @property
    def bound_source(self) -> StreamProtocol | None:
        """The concrete stream currently bound to this slot, or ``None``."""
        return self._bound_source

    @bound_source.setter
    def bound_source(self, value: StreamProtocol | None) -> None:
        """Bind *value* as the concrete source and invalidate both hash caches.

        A source is checked against the slot's declared schema first (``validate``), so
        one that does not fit is refused before anything changes. A narrower source
        (``x: int`` for a slot declaring ``x: int | None``) is accepted, and the node
        gives its rows as rows of the declared schema (ITL-1112). ``None`` unbinds.
        Binding stamps the slot as modified, so every job node fed by it is stale and
        reads empty until it runs again (ITL-1095).

        Raises:
            SourceSpecMismatchError: If *value* does not fit the declared schema.
        """
        self._bind(value)
        self._update_modified_time()

    def _bound_stream(self) -> StreamProtocol | None:
        """The concrete stream bound to this slot, or ``None``."""
        return self._bound_source

    def _bind(self, value: StreamProtocol | None) -> None:
        """Check *value* and bind it, invalidating both hash caches.

        The binding the setter and ``__init__`` share. Only the setter stamps the slot
        as modified (ITL-1095): a node built with its source is not a rebinding.

        Raises:
            SourceSpecMismatchError: If *value* does not fit the declared schema.
        """
        form = None if value is None else self._declared_form_for(value)
        self._bound_source = value
        self._declared_form = form
        self._invalidate_content_hash_cache()
        self._invalidate_pipeline_hash_cache()

    @property
    def last_modified(self) -> datetime | None:
        """When the slot's content last changed: its binding or its bound source.

        The later of the time the slot was bound and the bound source's own
        ``last_modified`` (a ``CachedSource`` or ``PollingSource`` moves it when it
        reads new rows). A bound source that does not know its modification time makes
        the slot's unknown too (``None``), so every node below it is stale. Every run
        first checks the bound source for new data (``check_for_new_data``, ITL-1102),
        which moves its modified time when it found some. A source that cannot see its
        data change (one built over data held in memory) is not re-read: build a new
        source object and bind it, or clear the dependent nodes.
        """
        own = self._modified_time
        if self._bound_source is None:
            return own
        bound = self._bound_source.last_modified
        if bound is None or own is None:
            return bound
        return max(own, bound)

    def _declared_form_for(self, source: StreamProtocol) -> _DeclaredForm | None:
        """Check *source* and return how its rows become rows of the declared schema.

        Args:
            source: The stream being bound.

        Returns:
            ``None`` when the source's rows already are rows of the declared schema
            (the same type objects, column order and system-tag names and order), so
            they pass unchanged;
            otherwise the ``_DeclaredForm`` that makes them so.

        Raises:
            SourceSpecMismatchError: If *source* does not fit the declared schema.
        """
        source_tag, source_data, narrower = self._fit(source)
        # _fit has checked that the source has exactly one pair and nothing else.
        source_ids, record_ids, _ = _system_tag_columns(source, source_tag.keys())
        declared_source_id, declared_record_id = system_tag_column_names(
            self._schema_hash_str
        )
        renames = {
            name: declared
            for name, declared in (
                (source_ids[0], declared_source_id),
                (record_ids[0], declared_record_id),
            )
            if name != declared
        }
        tag_order = tuple(self._tag_schema.keys())
        data_order = tuple(self._data_schema.keys())
        source_system_tags = [
            name
            for name in source.keys(columns={"system_tags": True})[0]
            if name not in source_tag
        ]
        # Exactly the declared types: a structural class of the declared identity is
        # another class object, and the slot shows the declared one.
        if (
            dict(source_tag) == dict(self._tag_schema)
            and dict(source_data) == dict(self._data_schema)
            and not renames
            and tuple(source_tag.keys()) == tag_order
            and tuple(source_data.keys()) == data_order
            and source_system_tags == [declared_source_id, declared_record_id]
        ):
            return None
        return _DeclaredForm(
            nullable=narrower,
            renames=renames,
            tag_order=tag_order,
            data_order=data_order,
            system_tag_order=(declared_source_id, declared_record_id),
        )

    def _declared_rows(
        self, rows: Iterator[tuple[TagProtocol, DataProtocol]], form: _DeclaredForm
    ) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Yield each row of a narrower source as a row of the declared schema."""
        for tag, data in rows:
            yield self._declared_row(tag, data, form)

    @staticmethod
    def _declared_row(
        tag: TagProtocol, data: DataProtocol, form: _DeclaredForm
    ) -> tuple[TagProtocol, DataProtocol]:
        """Return one row of a narrower source as a row of the declared schema.

        The row's tag (with its system tags) and data (with its source info) Arrow
        tables go through ``form``, and the row is rebuilt from them with its own
        data context.
        """
        tag_table = form.apply(tag.as_table(columns={"system_tags": True}))
        data_table = form.apply(data.as_table(columns={"source": True, "meta": True}))
        new_tag = (
            Tag(tag_table, data_context=tag.data_context_key)
            if tag_table.num_columns
            else tag
        )
        return new_tag, Data(data_table, data_context=data.data_context_key)

    def _declared_stream(
        self, source: StreamProtocol, form: _DeclaredForm, table: pa.Table
    ) -> ArrowTableStream:
        """Return a stream over *table*, read from *source*, as the declared schema.

        *table* must hold the system-tag, source-info and context columns, so the
        stream selects, sorts and hashes the declared rows as any stream does.
        """
        return ArrowTableStream(
            form.apply(table),
            tag_columns=tuple(self._tag_schema.keys()),
            data_context=source.data_context,  # pyright: ignore[reportAttributeAccessIssue]  # StreamProtocol omits data_context, which every concrete stream provides (ITL-844)
        )

    # ------------------------------------------------------------------
    # Identity — delegate to bound source when set
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Delegate to ``bound_source.identity_structure()`` when bound.

        This is the correct extension point: content_hash() flows from
        identity_structure(), so overriding here avoids bypassing the
        caching and resolver logic in ContentIdentifiableBase.content_hash().

        Returns:
            Bound source's identity structure when bound; schema-based
            identity (inherited from SourceNodeBase) when unbound.
        """
        if self._bound_source is not None:
            return self._bound_source.identity_structure()
        return super().identity_structure()

    def _row_keys(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """The tag and data keys of this node's rows, known without reading a row.

        The bound source's rows have the keys of its table (``keys_of_rows``, over its
        typed zero-row table, which reads no row). A narrower bind (``_DeclaredForm``)
        and an unbound slot answer with the slot's own keys: their description reports
        its types undeclared (``arrow_types_declared``).
        """
        if self._bound_source is None or self._declared_form is not None:
            return self.keys()
        table = typed_zero_row_input(self._bound_source)
        if table is None:
            return self.keys()
        return keys_of_rows(table, self._bound_source.keys())

    def arrow_schema(self) -> pa.Schema:
        """The Arrow schema of the columns this stream passes between job nodes.

        See ``stream_arrow_schema``. A narrower bind hands on its rows as ``main`` gives
        them (``RowsOnRead``), so its description is the declared Python schema's
        (``arrow_types_declared`` is false): no second placement rule.
        """
        if self._declared_form is not None:
            return stream_arrow_schema(self, self.keys(), from_table=False)
        return stream_arrow_schema(self, self._row_keys())

    @property
    def arrow_types_declared(self) -> bool:
        """False for a narrower bind (see ``arrow_schema``), else as every source node."""
        if self._declared_form is not None:
            return False
        return super().arrow_types_declared

    def _rows_table(
        self,
    ) -> tuple[pa.Table, tuple[tuple[str, ...], tuple[str, ...]]] | None:
        """The bound source's table and the keys of its rows (``keys_of_rows``).

        ``None`` for a narrower bind (its rows are given as the declared schema one by
        one, ``_declared_rows``), for a node that is not exactly a ``SourceJobNode``
        (a subclass may read its rows its own way), and for a bound stream that is not
        a library source kind whose table is its rows (``TABLE_IS_ROWS``): the node then
        hands on those rows (``RowsOnRead``).

        Raises:
            UnboundSourceError: When no concrete source is attached.
        """
        if not reads_as_its_class(self, SourceJobNode):
            # A subclass, or a node with a member replaced on the instance, may read
            # its rows its own way (an ``iter_data`` that filters, or rows of its own
            # without a bound source): its rows, as main iterated them (ITL-1347, the
            # one rule).
            return None
        if self._bound_source is None:
            raise UnboundSourceError(
                f"SourceJobNode '{self._name}' has no concrete source bound. "
                "Call job.bind(sources={'<name>': source}) before running."
            )
        if self._declared_form is not None:
            return None
        if (
            not table_is_rows(self._bound_source)
            or self._bound_source.data_context_key != self.data_context_key
        ):
            # Not a library source kind whose table is its rows (a side-effect or
            # function pod stream, an operator's result, a raw stream, a poller), or
            # one whose rows report another data context than the node's: its rows,
            # as main iterated them (the edge table is built in the node's context).
            return None
        table = self._bound_source.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
        if not system_tags_as_rows_give(table):
            # System tags the rows would type otherwise (a narrow integer, a
            # nullable field): the rows decide, as on main.
            return None
        return table, keys_of_rows(table, self._bound_source.keys())

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Delegate to concrete source, or raise if unbound.

        Raises:
            UnboundSourceError: When no concrete source is attached.
        """
        if self._bound_source is None:
            raise UnboundSourceError(
                f"SourceJobNode '{self._name}' has no concrete source bound. "
                "Call job.bind(sources={'<name>': source}) before running."
            )
        if self._declared_form is None:
            return self._bound_source.iter_data()
        return self._declared_rows(self._bound_source.iter_data(), self._declared_form)

    def _typed_zero_row_table(self) -> "pa.Table | None":
        """The bound source's zero-row table in the declared form; ``None`` if unknown.

        A source given in the declared form (``_DeclaredForm``) has its zero-row table
        given in that form too, so it has exactly the schema of the node's populated
        table (ITL-1112, ITL-912). ``None`` when unbound or when the source cannot type
        its rows without rows. See ``typed_zero_row_table``.
        """
        if self._bound_source is None:
            return self._declared_zero_row_table()
        table = typed_zero_row_table(self._bound_source)
        if table is None or self._declared_form is None:
            return table
        return self._declared_form.apply(table)

    def _schema_requires_io(self, config: ColumnConfig) -> bool:
        """Whether reading the schema with ``config`` would poll: as for the bound source."""
        return self._bound_source is not None and schema_requires_io(
            self._bound_source, config
        )

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the bound source's head, or no rows when no source is bound.

        A narrower bound source's rows are shown as rows of the declared schema.
        """
        if self._bound_source is None:
            check_head_size(n)
            return self._head_without_rows(
                ColumnConfig.handle_config(columns, all_info=all_info),
                ViewStatus.UNAVAILABLE,
                note="no source bound",
            )
        if self._declared_form is None:
            return head_of(
                self._bound_source.head(n, columns=columns, all_info=all_info), self
            )
        # The source's own head, read with every column group the declared stream
        # needs; the declared stream then selects the asked-for groups and computes
        # the content hashes of the declared rows. Nothing is computed.
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        view = head_of(
            self._bound_source.head(
                n,
                columns=ColumnConfig(
                    meta=True,
                    source=True,
                    system_tags=True,
                    context=True,
                    sort_by_tags=config.sort_by_tags,
                ),
            ),
            self,
        )
        table = self._declared_stream(
            self._bound_source, self._declared_form, view.table
        ).as_table(columns=config)
        renames = self._declared_form.renames
        tag_columns = {renames.get(c, c) for c in view.tag_columns}
        return dataclasses.replace(
            view,
            table=table,
            tag_columns=tuple(c for c in table.column_names if c in tag_columns),
        )

    async def async_iter_data(
        self,
    ) -> AsyncIterator[tuple[TagProtocol, DataProtocol]]:
        """Delegate to ``bound_source.async_iter_data()`` when bound.

        Overrides ``SourceNodeBase.async_iter_data()`` to route through the
        bound source's own async generator instead of wrapping ``iter_data()``
        synchronously. This ensures that dynamic sources such as
        ``PollingSource`` run their async polling loop rather than returning
        a static snapshot.

        Raises:
            UnboundSourceError: When no concrete source is attached.
        """
        if self._bound_source is None:
            raise UnboundSourceError(
                f"SourceJobNode '{self._name}' has no concrete source bound. "
                "Call job.bind(sources={'<name>': source}) before running."
            )
        form = self._declared_form
        async for tag, data in self._bound_source.async_iter_data():
            yield (tag, data) if form is None else self._declared_row(tag, data, form)

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return ``(tag_schema, data_schema)``, delegating to ``bound_source`` when set.

        When ``bound_source`` has the declared schema, this is a transparent
        pass-through to ``bound_source.output_schema(columns=columns, all_info=all_info)``
        so callers get the same result as querying the source directly. A narrower
        bound source (``x: int`` for a declared ``x: int | None``) is given as the
        declared schema (ITL-1112).

        When unbound, delegates to ``SourceNodeBase.output_schema()`` which includes
        system-tag schema entries derived from the declared schemas.

        Args:
            columns: Column selection config.
            all_info: If ``True``, include all available column groups.

        Returns:
            Tuple of ``(tag_schema, data_schema)``.
        """
        if self._bound_source is None or self._declared_form is not None:
            # Unbound, or bound to a narrower source given as the declared schema.
            return super().output_schema(columns=columns, all_info=all_info)
        return self._bound_source.output_schema(columns=columns, all_info=all_info)

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        """Materialize the concrete source as a PyArrow Table.

        A narrower bound source's rows are given as rows of the declared schema
        (ITL-1112).

        Args:
            columns: Column selection config.
            all_info: If True, include all metadata columns.

        Raises:
            UnboundSourceError: When no concrete source is attached.
        """
        if self._bound_source is None:
            raise UnboundSourceError(
                f"SourceJobNode '{self._name}' has no concrete source bound. "
                "Call job.bind(sources={'<name>': source}) before calling as_table()."
            )
        if self._declared_form is None:
            return self._bound_source.as_table(columns=columns, all_info=all_info)
        table = self._bound_source.as_table(
            columns={
                "meta": True,
                "source": True,
                "system_tags": True,
                "context": True,
                "sort_by_tags": False,
            }
        )
        return self._declared_stream(
            self._bound_source, self._declared_form, table
        ).as_table(columns=columns, all_info=all_info)

    @classmethod
    def from_stream(
        cls,
        stream: StreamProtocol,
        name: str | None = None,
    ) -> "SourceJobNode":
        """Create a ``SourceJobNode`` from *stream* using three-way logic.

        The three cases are:

        1. *stream* is already a ``SourceJobNode`` — copy it, preserving the
           existing ``bound_source`` (the SJN itself is **not** used as the
           bound source).
        2. *stream* is a ``SourceNode`` (schema-only, no data) — create an
           **unbound** ``SourceJobNode`` with the same name and schemas. The
           resulting SJN has ``bound_source=None`` and the same
           ``content_hash()`` as *stream* (schema-based).
        3. Any other concrete stream (``ArrowTableSource``, etc.) — create a
           **bound** ``SourceJobNode`` whose ``content_hash()`` delegates to
           the concrete stream.

        Used by ``Pipeline.compile`` for each leaf of the pipeline. The stream is
        bound as any source is, so a leaf without exactly one source-id and one
        record-id system-tag column (a raw ``ArrowTableStream``) is refused
        (ITL-1126).

        Args:
            stream: The stream to wrap.
            name: Optional explicit slot name. In all three cases, when *name*
                is provided it takes precedence over the stream's own name.
                For concrete streams (Case 3), when *name* is ``None`` the slot
                name is resolved as: ``stream.source_name`` (the source id
                without its kind part) for ``RootSource`` instances, or ``"{stream.label}:{hash_prefix}"`` for any other
                concrete stream type.

        Returns:
            A ``SourceJobNode`` configured according to the case above.
        """
        match stream:
            case SourceJobNode():
                # Case 1: copy — preserve bound_source, do NOT wrap the SJN itself.
                # The copy states the slot's Arrow schemas from the start: its bound
                # source is validated against them as it is constructed (ITL-640).
                node = cls(
                    name=name if name is not None else stream.name,
                    tag_schema=stream.tag_schema,
                    data_schema=stream.data_schema,
                    arrow_schemas=stream._given_arrow_schemas,  # pyright: ignore[reportPrivateUsage]  # one slot copied into another
                    bound_source=stream.bound_source,
                )
                node._take_saved_state_of(stream)
                return node
            case SourceNode():
                # Case 2: unbound — schema placeholder; SJN hash = SourceNode hash.
                node = cls(
                    name=name if name is not None else stream.name,
                    tag_schema=stream.tag_schema,
                    data_schema=stream.data_schema,
                    arrow_schemas=stream._given_arrow_schemas,  # pyright: ignore[reportPrivateUsage]  # one slot copied into another
                    bound_source=None,
                )
                node._take_saved_state_of(stream)
                return node
            case _:
                # Case 3: concrete stream — bound SJN.
                # Slot name priority:
                #   1. explicit *name* parameter
                #   2. stream.source_name for RootSource (its id without the kind part)
                #   3. label + short hash prefix for any other concrete stream type
                tag_schema, data_schema = stream.output_schema()
                if name is not None:
                    slot_name = name
                else:
                    # Local import avoids a module-level circular dependency between
                    # core.nodes and core.sources.
                    from orcapod.core.sources.base import RootSource

                    if isinstance(stream, RootSource):
                        slot_name = stream.source_name
                    else:
                        _require_name_for_pod_stream(stream)
                        slot_name = f"{stream.label}:{stream.content_hash().to_hex()}"
                _require_stream_arrow_types(stream, slot_name)
                return cls(
                    name=slot_name,
                    tag_schema=tag_schema,
                    data_schema=data_schema,
                    arrow_schemas=typed_arrow_schemas(stream),
                    bound_source=stream,
                )

    @classmethod
    def _from_loaded_slot(
        cls, slot: SourceNode, bound_source: StreamProtocol | None
    ) -> "SourceJobNode":
        """Build the job node of a slot rebuilt from a saved job (``PipelineJob.load``).

        The node takes the slot's saved hashes and saved type texts before *bound_source*
        is checked, so a column saved with a class this process registers only now
        (by building the source) is restored for the check (ITL-1138).

        Args:
            slot: The slot ``Pipeline.load`` rebuilt from the job file.
            bound_source: The source rebuilt from the saved config, if any.

        Raises:
            SourceSpecMismatchError: If *bound_source* does not fit the slot.
        """
        node = cls(
            name=slot.name,
            tag_schema=slot.tag_schema,
            data_schema=slot.data_schema,
            arrow_schemas=slot._given_arrow_schemas,  # pyright: ignore[reportPrivateUsage]  # one slot copied into another
        )
        node._take_saved_state_of(slot)
        if bound_source is not None:
            node._bind(bound_source)
        return node

    def as_node(self) -> SourceNode:
        """Return the lightweight ``SourceNode`` equivalent of this job node.

        Returns:
            A new ``SourceNode`` with the same name and schemas.
        """
        node = SourceNode(
            name=self._name,
            tag_schema=self._tag_schema,
            data_schema=self._data_schema,
            arrow_schemas=self._given_arrow_schemas,
        )
        node._take_saved_state_of(self)
        return node


def _require_name_for_pod_stream(stream: StreamProtocol) -> None:
    """Refuse to make a slot from a stream made by a pod without a name (ITL-1387).

    A stream becomes a source only by the user's explicit act: naming the slot made
    from it is that act. A pipeline never makes one implicitly: a stream given to it
    comes in with its pod and every upstream step.

    Raises:
        ValueError: If *stream* has a producer.
    """
    if stream.producer is not None:
        raise ValueError(
            f"{stream.label!r} is made by a pod, so it becomes a source slot only when "
            "you name the slot (name=...). In a pipeline it comes in with its steps; to "
            "cut a pipeline there, use a node's as_source() or a source object."
        )
