"""Source node hierarchy for Pipeline and PipelineJob.

SourceNode — schema-only input-slot declaration.
SourceJobNode — execution variant that wraps a concrete StreamProtocol.
Both share SourceNodeBase which provides schema-based identity.
"""

from __future__ import annotations

import dataclasses
import functools
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator, Mapping
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.config import OrcapodConfig
from orcapod.core.base import TraceableBase
from orcapod.core.datagrams import Data, Tag
from orcapod.core.operators.drop_nulls import _without_none  # pyright: ignore[reportPrivateUsage]  # DropNulls' rule for removing None from a type, shared, never copied
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import schema_requires_io
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
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_utils
from orcapod.utils.arrow_utils import system_tag_column_names
from orcapod.utils.lazy_module import LazyModule
from orcapod.utils.schema_utils import compute_source_schema_hash

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.channels import WritableChannel
    from orcapod.protocols.core_protocols import StreamProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


def _type_text(data_type: Any) -> str:
    """Return *data_type* as it is written in an annotation (``int``, ``int | None``)."""
    return data_type.__name__ if isinstance(data_type, type) else str(data_type)


def _is_narrower(declared: Any, given: Any) -> bool:
    """Return whether *given* is *declared* without its ``None`` member.

    ``int`` is narrower than ``int | None`` and ``int | str`` than ``int | str | None``;
    nothing else is (developer, November 14, 2026, ITL-1112).

    Args:
        declared: The slot's declared Python type of a column.
        given: The bound source's Python type of that column.
    """
    return declared != given and _without_none(declared) == given


@dataclasses.dataclass(frozen=True)
class _DeclaredForm:
    """How the rows of a narrower bound source become rows of the declared schema.

    A slot declaring ``x: int | None`` accepts a source of ``x: int`` and gives its rows
    as rows of ``x: int | None`` (ITL-1112): the same arrays under nullable fields, and
    the two system-tag columns, named after the source's schema hash, renamed after the
    declared schema's. Nothing else differs: values, types, source-info columns and
    their tokens pass unchanged, so the rows equal those of a source of the declared
    schema with the same source id.

    Attributes:
        nullable: The columns declared ``T | None`` that the source gives as ``T``.
        renames: The source's system-tag column names, mapped to the declared ones.
    """

    nullable: frozenset[str]
    renames: Mapping[str, str]

    def apply(self, table: pa.Table) -> pa.Table:
        """Return *table*'s arrays under the declared fields and system-tag names.

        Serves a whole stream table and a single row's tag or data table: columns that
        are not in the table are skipped.

        Args:
            table: Rows read from the bound source.

        Returns:
            The same arrays, with the columns in ``nullable`` flagged nullable and the
            columns in ``renames`` renamed.
        """
        fields = []
        for field in table.schema:
            if field.name in self.nullable:
                field = field.with_nullable(True)
            if field.name in self.renames:
                field = field.with_name(self.renames[field.name])
            fields.append(field)
        schema = pa.schema(fields, metadata=table.schema.metadata)
        # Same arrays under the declared fields: no cast, so no value or type changes.
        return pa.Table.from_arrays(table.columns, schema=schema)


class SourceNodeBase(TraceableBase, ABC):
    """Abstract base for SourceNode and SourceJobNode.

    Provides schema-based identity (content_hash, pipeline_hash) and
    shared properties.  Both sub-types carry identical schemas so their
    pipeline_hash() values always match; content_hash() diverges only
    when SourceJobNode has a concrete source bound.

    Args:
        name: The input-slot name used as the key in
            ``PipelineJob.bind(sources={name: source})``.
        tag_schema: Mapping of tag column names to Python types.
        data_schema: Mapping of data column names to Python types.
        data_context: Optional data context override.
        label: Optional display label override.
        config: Optional config override.
    """

    node_type = "source"

    def __init__(
        self,
        name: str,
        tag_schema: Schema,
        data_schema: Schema,
        data_context: str | contexts.DataContext | None = None,
        label: str | None = None,
        config: OrcapodConfig | None = None,
    ) -> None:
        super().__init__(label=label, data_context=data_context, config=config)
        self._name = name
        self._tag_schema = tag_schema
        self._data_schema = data_schema

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Return the content identity: ``("source_node", name, tag_schema, data_schema)``."""
        return ("source_node", self._name, self._tag_schema, self._data_schema)

    def pipeline_identity_structure(self) -> Any:
        """Return the pipeline identity: ``(tag_schema, data_schema)`` (name-independent).

        Sources with identical schemas share the same DB table paths regardless
        of name.
        """
        return (self._tag_schema, self._data_schema)

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
        """Tag schema for this input slot."""
        return self._tag_schema

    @property
    def data_schema(self) -> Schema:
        """Data schema for this input slot."""
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
        return compute_source_schema_hash(
            self._tag_schema,
            self._data_schema,
            self.data_context,
            self.orcapod_config,
        )

    @property
    def node_uri(self) -> tuple[str, ...]:
        """Canonical URI tuple for this source node.

        Returns a tuple identifying this node as a named schema-only source slot.
        """
        return ("source_node", self._name)

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
        node's schema (both typed as ``str``). Their names are deterministic from
        the declared schemas and match what a concrete source with the same schema
        would produce.

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
        ``optional_fields`` are not compared; neither enters the pipeline hash.

        Args:
            source: A concrete stream to validate.

        Raises:
            SourceSpecMismatchError: If a column is missing, unexpected, or of a type
                the declared one does not accept.
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
        narrower: set[str] = set()
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
                if name not in given or given[name] == declared[name]:
                    continue
                if _is_narrower(declared[name], given[name]):
                    narrower.add(name)
                else:
                    issues.append(
                        f"{role} column {name!r} is declared "
                        f"{_type_text(declared[name])} but the source has "
                        f"{_type_text(given[name])}"
                    )

        if issues:
            raise SourceSpecMismatchError(
                f"SourceNode '{self._name}' is not compatible with the provided source. "
                + "; ".join(issues)
            )
        return source_tag, source_data, frozenset(narrower)

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

    def execute(
        self,
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Execute this source node: materialize and return data.

        Args:
            observer: Optional execution observer.

        Returns:
            List of (tag, data) tuples.

        Raises:
            UnboundSourceError: When no concrete data is available.
        """
        node_label = self.label
        node_hash = self.content_hash().to_string()
        if observer is not None:
            observer.on_node_start(node_label, node_hash)
        result = list(self.iter_data())
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
        2. ``stream.source_id`` when *stream* is a ``RootSource`` — the canonical
           source identity, stable across pipeline serialisation / deserialisation.
        3. ``"{stream.label}:{hash_prefix}"`` for any other concrete stream type,
           combining the cosmetic label with a short content-hash suffix to ensure
           uniqueness.

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
                slot_name = stream.source_id
            else:
                slot_name = f"{stream.label}:{stream.content_hash().to_hex()}"
        return cls(
            name=slot_name,
            tag_schema=tag_schema,
            data_schema=data_schema,
        )


class SourceJobNode(SourceNodeBase):
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
    ) -> None:
        if tag_schema is None or data_schema is None:
            if bound_source is None:
                raise ValueError(
                    "SourceJobNode requires either explicit tag_schema/data_schema "
                    "or a bound_source to derive schemas from."
                )
            derived_tag, derived_data = bound_source.output_schema()
            if tag_schema is None:
                tag_schema = derived_tag
            if data_schema is None:
                data_schema = derived_data
        super().__init__(
            name=name,
            tag_schema=tag_schema,
            data_schema=data_schema,
            data_context=data_context,
        )
        self._bound_source: StreamProtocol | None = None
        self._declared_form: _DeclaredForm | None = None
        if bound_source is not None:
            # Through the setter, which checks the source and, for a narrower one,
            # records how its rows become rows of the declared schema (ITL-1112).
            # super().__init__() has initialised the hash caches it invalidates.
            self.bound_source = bound_source

    # ------------------------------------------------------------------
    # bound_source property — explicit binding with cache invalidation
    # ------------------------------------------------------------------

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

        Raises:
            SourceSpecMismatchError: If *value* does not fit the declared schema.
        """
        form = None if value is None else self._declared_form_for(value)
        self._bound_source = value
        self._declared_form = form
        self._invalidate_content_hash_cache()
        self._invalidate_pipeline_hash_cache()

    def _declared_form_for(self, source: StreamProtocol) -> _DeclaredForm | None:
        """Check *source* and return how its rows become rows of the declared schema.

        Args:
            source: The stream being bound.

        Returns:
            ``None`` when the source's schema equals the declared one, so its rows pass
            unchanged; otherwise the ``_DeclaredForm`` of its narrower columns.

        Raises:
            SourceSpecMismatchError: If *source* does not fit the declared schema.
        """
        source_tag, source_data, narrower = self._fit(source)
        if not narrower:
            return None
        source_schema_hash = compute_source_schema_hash(
            source_tag, source_data, self.data_context, self.orcapod_config
        )
        return _DeclaredForm(
            nullable=narrower,
            renames=dict(
                zip(
                    system_tag_column_names(source_schema_hash),
                    system_tag_column_names(self._schema_hash_str),
                    strict=True,
                )
            ),
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
        """Return the bound source's head, or no rows when no source is bound."""
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
        return dataclasses.replace(
            view,
            table=table,
            tag_columns=tuple(c for c in view.tag_columns if c in table.column_names),
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

        When ``bound_source`` is present, this is a transparent pass-through to
        ``bound_source.output_schema(columns=columns, all_info=all_info)`` so callers
        get the same result as querying the source directly.

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

        Args:
            stream: The stream to wrap.
            name: Optional explicit slot name. In all three cases, when *name*
                is provided it takes precedence over the stream's own name.
                For concrete streams (Case 3), when *name* is ``None`` the slot
                name is resolved as: ``stream.source_id`` for ``RootSource``
                instances, or ``"{stream.label}:{hash_prefix}"`` for any other
                concrete stream type.

        Returns:
            A ``SourceJobNode`` configured according to the case above.
        """
        match stream:
            case SourceJobNode():
                # Case 1: copy — preserve bound_source, do NOT wrap the SJN itself.
                return cls(
                    name=name if name is not None else stream.name,
                    tag_schema=stream.tag_schema,
                    data_schema=stream.data_schema,
                    bound_source=stream.bound_source,
                )
            case SourceNode():
                # Case 2: unbound — schema placeholder; SJN hash = SourceNode hash.
                return cls(
                    name=name if name is not None else stream.name,
                    tag_schema=stream.tag_schema,
                    data_schema=stream.data_schema,
                    bound_source=None,
                )
            case _:
                # Case 3: concrete stream — bound SJN.
                # Slot name priority:
                #   1. explicit *name* parameter
                #   2. stream.source_id for RootSource (canonical, stable identity)
                #   3. label + short hash prefix for any other concrete stream type
                tag_schema, data_schema = stream.output_schema()
                if name is not None:
                    slot_name = name
                else:
                    # Local import avoids a module-level circular dependency between
                    # core.nodes and core.sources.
                    from orcapod.core.sources.base import RootSource

                    if isinstance(stream, RootSource):
                        slot_name = stream.source_id
                    else:
                        slot_name = f"{stream.label}:{stream.content_hash().to_hex()}"
                return cls(
                    name=slot_name,
                    tag_schema=tag_schema,
                    data_schema=data_schema,
                    bound_source=stream,
                )

    def as_node(self) -> SourceNode:
        """Return the lightweight ``SourceNode`` equivalent of this job node.

        Returns:
            A new ``SourceNode`` with the same name and schemas.
        """
        return SourceNode(
            name=self._name,
            tag_schema=self._tag_schema,
            data_schema=self._data_schema,
        )
