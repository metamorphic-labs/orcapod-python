"""OperatorNode hierarchy — pure blueprint + DB-backed execution node.

Three classes:

* ``OperatorNodeBase`` — shared base; no DB state.  Holds identity,
  schema, and all non-DB properties.
* ``OperatorNode`` — thin blueprint descriptor.  Raises
  ``PipelineJobRequiredError`` on ``iter_data()``.  This is the node
  recorded in a ``Pipeline`` and serialized to disk.
* ``OperatorJobNode`` — DB-backed execution node with full persistence
  logic.  Created by ``PipelineJob`` at run time.

``OperatorNode`` and ``OperatorJobNode`` are *siblings*: both inherit
directly from ``OperatorNodeBase``, neither from the other.
"""

from __future__ import annotations

import asyncio
import dataclasses
import sys
import logging
from abc import abstractmethod
from collections.abc import Collection, Iterator, Mapping, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal, cast

import pyarrow as pa
import pyarrow.compute as pc

from orcapod import contexts
from orcapod.core.nodes.provenance_methods import ProvenanceMethods
from orcapod.core.data_tags import (
    mark_data_tags,
    data_tag_names,
    require_database_keeps_marker,
    stored_data_tags,
    stream_data_tags,
    without_tag_kind_marks,
)
from orcapod.channels import Channel, ChannelClosed, ReadableChannel, WritableChannel
from orcapod.config import OrcapodConfig
from orcapod.core.operators.static_output_pod import (
    RowBuiltFormUnavailable,
    StaticOutputOperatorPod,
    produced_stream,
    rows_on_read,
)
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import (
    ZERO_ROW_INPUT_COLUMNS,
    StreamBase,
    check_head_size,
    reads_as_its_class,
    short_stream_repr,
    system_tags_as_rows_give,
)
from orcapod.core.streams.view import (
    JOB_NODE_NOT_RUN_NOTE,
    LOADED_ONLY_OPERATOR_NOTE,
    STALE_NOTE,
    BLUEPRINT_NOTE,
    ViewStatus,
    StreamView,
    head_of,
)
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.core.nodes.staleness import RunHoldingNode
from orcapod.errors import (
    InputValidationError,
    PipelineJobRequiredError,
    OrcapodDatabaseReadError,
    OrcapodDatabaseWriteError,
    UnstorableColumnError,
)
from orcapod.contexts.stored import (
    account_for_stale_tables,
    data_context_field,
    find_stale_tables,
    require_storable,
    keep_rows_of_data_context,
    require_same_data_context,
    describe_database_table,
    report_rows_of_other_data_contexts,
    report_unexpected_row_count,
    with_data_context,
)
from orcapod.protocols.core_protocols import (
    DataProtocol,
    StreamProtocol,
    TagProtocol,
    TrackerManagerProtocol,
)
from orcapod.protocols.core_protocols.operator_pod import OperatorPodProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import OPERATOR_DB_SCHEMA_VERSION, constants
import orcapod.core.row_ids as row_ids
from orcapod.types import CacheMode, ColumnConfig, ContentHash, Schema
from orcapod.utils import arrow_utils

logger = logging.getLogger(__name__)

# The insertion time of an operator cache row (``timestamp[us, tz=UTC]``, not null).
OPERATOR_RECORDED_AT_COL = "__recorded_at"

# What an operator cache row is stored with beside its key and is never served: its
# entry id and its insertion time. Neither is part of the row key (ITL-1471).
_STORED_ONLY_COLUMNS = (row_ids.ENTRY_ID_COL, OPERATOR_RECORDED_AT_COL)


def _stored_only_fields() -> "tuple[pa.Field[Any], pa.Field[Any]]":
    """The fields of ``_STORED_ONLY_COLUMNS``, in that order: one definition for the
    writer and for the check of the database when it is attached."""
    return (
        pa.field(row_ids.ENTRY_ID_COL, pa.large_binary(), nullable=False),
        pa.field(
            OPERATOR_RECORDED_AT_COL, pa.timestamp("us", tz="UTC"), nullable=False
        ),
    )


if TYPE_CHECKING:
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol


async def _discard(channel: ReadableChannel[Any]) -> None:
    """Read ``channel`` to its end, keeping nothing.

    Args:
        channel: The channel to drain.
    """
    async for _ in channel:
        pass


class _InputTap:
    """An input channel reader that keeps the rows needed to rebuild its columns.

    An async operator node hands its input channels to the operator, so it sees
    no input row itself. When the run produces no output row, the node rebuilds
    each input with no rows and applies the operator to those, as the sync path
    applies it to the rebuilt upstream rows (ITL-1080). Those columns and types
    are what ``StaticOutputOperatorPod._rows_to_table`` makes of the input's
    rows. Keep the first row and the first occurrence of an absent token in
    each source-info column. These sampled rows still go through the existing
    row-built token type check before they are sliced to zero rows.

    Attributes:
        rows: The kept rows, in arrival order.
    """

    def __init__(self, reader: ReadableChannel[tuple[TagProtocol, DataProtocol]]):
        self._reader = reader
        self.rows: list[tuple[TagProtocol, DataProtocol]] = []
        self._null_sources: set[str] = set()

    def _keep(self, item: tuple[TagProtocol, DataProtocol]) -> None:
        """Keep ``item`` if it is the first row or has a new ``None`` token."""
        nulls = {name for name, token in item[1].source_info().items() if token is None}
        if not self.rows or not nulls <= self._null_sources:
            self.rows.append(item)
            self._null_sources |= nulls

    async def receive(self) -> tuple[TagProtocol, DataProtocol]:
        """Receive the next row from the wrapped reader, keeping it if needed."""
        item = await self._reader.receive()
        self._keep(item)
        return item

    def __aiter__(self) -> "_InputTap":
        return self

    async def __anext__(self) -> tuple[TagProtocol, DataProtocol]:
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration

    async def collect(self) -> list[tuple[TagProtocol, DataProtocol]]:
        """Drain every remaining row."""
        return [item async for item in self]


# ---------------------------------------------------------------------------
# OperatorNodeBase — shared base (no DB)
# ---------------------------------------------------------------------------


class OperatorNodeBase(StreamBase):
    """Shared base for ``OperatorNode`` and ``OperatorJobNode``.

    Carries all non-DB state: identity, schema, upstreams, and properties
    shared by both the blueprint and the execution variant.
    """

    node_type = "operator"
    HASH_COLUMN_NAME = "_record_hash"
    # The content hash a saved job recorded for this node's run (ITL-1043); set only
    # on a node rebuilt by ``OperatorJobNode.from_descriptor``. A class default, so
    # nodes built without ``__init__`` have it too.
    _saved_run_content_hash: str | None = None

    def __init__(
        self,
        operator: OperatorPodProtocol,
        input_streams: Collection[StreamProtocol],
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        config: OrcapodConfig | None = None,
        table_scope: Literal["pipeline_hash", "content_hash"] = "pipeline_hash",
    ) -> None:
        """Initialize the shared operator-node state.

        Args:
            operator: The operator pod that defines the transformation logic
                and output schema.
            input_streams: One or more upstream streams consumed by this
                operator.  Passed through as a tuple.
            tracker_manager: Optional tracker manager override.  Defaults to
                ``DEFAULT_TRACKER_MANAGER``.
            label: Optional display label for this node.
            config: Optional config override; defaults to the global config.
            table_scope: Determines how the database table path is scoped.
                ``"pipeline_hash"`` (default) shares a table across all runs
                with the same topology and schemas regardless of which concrete
                data is bound.  ``"content_hash"`` creates a separate table per
                unique data combination.
        """
        if tracker_manager is None:
            tracker_manager = DEFAULT_TRACKER_MANAGER
        self.tracker_manager = tracker_manager

        self._operator = operator
        self._input_streams = tuple(input_streams)

        super().__init__(label=label, config=config)

        # Validate inputs eagerly
        self._operator.validate_inputs(*self._input_streams)

        self._set_modified_time(None)

        # Descriptor fields — populated by from_descriptor() for read-only/UNAVAILABLE
        # nodes. Initialized here so they are always present on the concrete class
        # (avoids getattr access for possibly-absent attributes).
        from orcapod.pipeline.serialization import LoadStatus

        self._load_status: LoadStatus = LoadStatus.FULL
        self._stored_content_hash: str | None = None
        self._stored_pipeline_hash: str | None = None
        self._stored_schema: dict = {}
        self._stored_node_uri: tuple[str, ...] = ()
        self._stored_pipeline_path: tuple[str, ...] = ()
        self._descriptor: dict = {}

        if table_scope not in ("pipeline_hash", "content_hash"):
            raise ValueError(
                f"Unknown table_scope {table_scope!r}. "
                "Expected one of: 'pipeline_hash', 'content_hash'."
            )
        self._table_scope = table_scope
        self._node_identity_path_cache: tuple[str, ...] | None = None

    # ------------------------------------------------------------------
    # load_status
    # ------------------------------------------------------------------

    @property
    def load_status(self) -> Any:
        """Return the load status of this node.

        Returns:
            The ``LoadStatus`` enum value indicating how this node was
            loaded.  Defaults to ``FULL`` for nodes created via
            ``__init__``.
        """
        return self._load_status

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        return self.pipeline_identity_structure()

    def pipeline_identity_structure(self) -> Any:
        return (self._operator, self._operator.argument_symmetry(self._input_streams))

    # ------------------------------------------------------------------
    # Read-only overrides (for deserialized nodes without live operator)
    # ------------------------------------------------------------------

    def content_hash(self, hasher=None) -> ContentHash:
        """Return the content hash: the run's when a loaded job recorded it.

        A node of a job loaded from a file that records its run's content hash returns
        that: the rows the run stored carry it, and inputs rebuilt from the file (an
        in-memory source is not saved) cannot reproduce it (ITL-1043). Otherwise a
        read-only node returns its descriptor's value, and any other node computes it.
        """
        if self._saved_run_content_hash is not None:
            return ContentHash.from_string(self._saved_run_content_hash)
        if self._operator is None and self._stored_content_hash is not None:
            return ContentHash.from_string(self._stored_content_hash)
        return super().content_hash(hasher)

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """Return the pipeline hash; a node rebuilt from a descriptor returns the saved one.

        A node rebuilt with its operator still returns the saved value: its inputs are
        rebuilt from the saved schemas, which do not keep every property that the run's
        inputs hashed (ITL-1043: a column's nullability), so recomputing could give
        another value and another table path than the run's.
        """
        if self._stored_pipeline_hash is not None:
            return ContentHash.from_string(self._stored_pipeline_hash)
        return super().pipeline_hash(hasher)

    # ------------------------------------------------------------------
    # Stream interface
    # ------------------------------------------------------------------

    @property
    def producer(self) -> OperatorPodProtocol:
        return self._operator

    @property
    def data_context(self) -> contexts.DataContext:
        return contexts.resolve_context(self.data_context_key)

    @property
    def data_context_key(self) -> str:
        """Key of the node's data context.

        A node loaded from a saved job without its operator (read-only) has the data
        context saved in its descriptor.
        """
        if self._operator is None:
            return self._data_context.context_key
        return self._operator.data_context_key

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return self._input_streams

    @property
    def cache_mode(self) -> CacheMode:
        """How the node caches its output: ``CacheMode.OFF`` for a node outside a
        job, which keeps no cache."""
        return CacheMode.OFF

    @upstreams.setter
    def upstreams(self, value: tuple[StreamProtocol, ...]) -> None:
        """Replace the node's inputs.

        Raises:
            ValueError: If the node caches (``LOG`` or ``REPLAY``) and a source
                feeding ``value`` is restricted (ITL-1218); nothing changes.
        """
        from orcapod.pipeline.restrictions import refuse_restricted_operator_cache

        refuse_restricted_operator_cache(self, self.cache_mode, streams=value)
        self._input_streams = value
        self._forget_output_schema()

    @property
    def arrow_schemas(self) -> "tuple[pa.Schema, pa.Schema] | None":
        """A read-only loaded node's tag and data Arrow schemas, as saved (ITL-640).

        A node whose operator cannot be rebuilt (``Index``, ``Pick``, ITL-1326) cannot
        type its zero-row table, but its saved ``output_schema`` text holds the Arrow
        types its rows have; ``typed_arrow_schemas`` takes them from here, as a loaded
        function node takes its saved output types. ``None`` for a node with an
        operator, which derives its types, and for a saved text that cannot be
        restored.
        """
        if self._operator is not None or not self._stored_schema:
            return None
        from orcapod.pipeline.serialization import arrow_schema_from_texts

        saved = self._stored_schema
        nullable = saved.get("nullable")
        converter = self.data_context.type_converter
        try:
            return (
                arrow_schema_from_texts(
                    saved.get("tag", {}),
                    converter,
                    None if nullable is None else nullable.get("tag", []),
                ),
                arrow_schema_from_texts(
                    saved.get("data", {}),
                    converter,
                    None if nullable is None else nullable.get("data", []),
                ),
            )
        except (ValueError, TypeError):
            return None

    def _saved_zero_row_table(self) -> "pa.Table | None":
        """A read-only loaded node's table with no rows, typed by its saved Arrow pair.

        The user tag and data columns, each once (a data-tag among the tags), with the
        Arrow types and nullability its saved ``output_schema`` records: the Arrow
        schema is never computed (ITL-912), so a saved ``int32`` stays ``int32``
        (CodeRabbit on PR 454). No system-tag or source-info column: the saved schema
        does not name them. ``None`` when the node has an operator, or when
        ``arrow_schemas`` cannot restore the saved text (a logical type this process
        has not registered).
        """
        schemas = self.arrow_schemas
        if schemas is None:
            return None
        tag_schema, data_schema = schemas
        fields = [
            *tag_schema,
            *(f for f in data_schema if f.name not in tag_schema.names),
        ]
        return pa.schema(fields).empty_table()

    def _zero_row_table(self, config: ColumnConfig) -> "pa.Table | None":
        """This node's table with no rows; a read-only node's from its saved types."""
        if self._operator is not None:
            return super()._zero_row_table(config)
        # ``head`` and the preview: the same saved types ``_make_empty_table`` uses.
        # ``_typed_zero_row_table`` stays ``None`` for such a node: what it promises a
        # downstream consumer (system-tag and source-info columns) the saved schema
        # does not name.
        saved = self._saved_zero_row_table()
        if saved is None:
            return None
        return ArrowTableStream(
            saved,
            tag_columns=self.keys()[0],
            data_tags=stream_data_tags(self),
            data_context=self.data_context,
        ).as_table(columns=config)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        if self._operator is None:
            tag_keys = tuple(self._stored_schema.get("tag", {}).keys())
            data_keys = tuple(self._stored_schema.get("data", {}).keys())
            return tag_keys, data_keys
        tag_schema, data_schema = self.output_schema(columns=columns, all_info=all_info)
        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return output schema, using stored value in read-only mode.

        Raises:
            ValueError: If the node was loaded and a column's saved type cannot be
                restored to a Python type (an unregistered logical type).
        """
        if self._operator is None:
            from orcapod.pipeline.serialization import (
                deserialize_output_schema,
                SavedSchemaVersionError,
            )
            from orcapod.logical_types.identifier import LogicalTypeIdentifierError

            try:
                return deserialize_output_schema(
                    self._stored_schema,
                    self.data_context.type_converter,
                )
            except (LogicalTypeIdentifierError, SavedSchemaVersionError) as exc:
                exc.args = (
                    f"Cannot restore the output schema of loaded node {self.label!r}: {exc}",
                )
                raise
            except ValueError as exc:
                raise ValueError(
                    f"Cannot restore the output schema of loaded node {self.label!r}: "
                    f"{exc}"
                ) from exc
        # Memoized (ITL-1111). The prediction depends on the operator's configuration
        # and on the input streams' schemas and pipeline hashes; the ``upstreams``
        # setter and a job's ``bind`` clear it when an input changes. Never on rows.
        operator = self._operator
        return self._memoized_output_schema(
            columns,
            all_info,
            lambda: operator.output_schema(
                *self._input_streams, columns=columns, all_info=all_info
            ),
        )

    # ------------------------------------------------------------------
    # Node identity path
    # ------------------------------------------------------------------

    @property
    def node_identity_path(self) -> tuple[str, ...]:
        """Return the node identity path for observer contextualization.

        When ``table_scope="pipeline_hash"`` (default) the path is
        ``operator.uri + (schema:{pipeline_hash},)`` — all runs that share
        the same pipeline structure use one shared table, with per-run
        disambiguation via the ``_node_content_hash`` row-level column.

        When ``table_scope="content_hash"`` the legacy path is returned:
        ``operator.uri + (schema:{pipeline_hash}, instance:{content_hash})``.

        In read-only/UNAVAILABLE mode (no operator) the path stored from the
        deserialized descriptor is returned (empty tuple when absent).
        """
        if self._operator is None:
            return self._stored_pipeline_path
        if self._node_identity_path_cache is not None:
            return self._node_identity_path_cache
        # Hashes inside a path are bare hex digests: a label never enters a name.
        path = self.node_uri + (f"schema:{self.pipeline_hash().to_hex()}",)
        if self._table_scope != "pipeline_hash":
            path += (f"instance:{self.content_hash().to_hex()}",)
        self._node_identity_path_cache = path
        return path

    @property
    def node_uri(self) -> tuple[str, ...]:
        """Canonical URI tuple identifying this computation.

        Identical to ``operator.uri`` at runtime.
        Returns stored value in read-only (deserialized) mode.
        """
        if self._operator is None:
            return self._stored_node_uri
        return self._operator.uri

    # ------------------------------------------------------------------
    # Caching
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear the node identity path cache."""
        self._node_identity_path_cache = None
        self._update_modified_time()

    def __repr__(self) -> str:
        # Upstreams by class and label only: their own repr would nest every path (ITL-1111).
        upstreams = ", ".join(short_stream_repr(s) for s in self._input_streams)
        return (
            f"{type(self).__name__}(label={self.label!r}, operator={self._operator!r}, "
            f"upstreams=({upstreams}))"
        )

    @abstractmethod
    def set_ephemeral_database(self, database: "ArrowDatabaseProtocol | None") -> None:
        """Assign or remove the ephemeral result database for this node.

        No-op for operator nodes — full ephemeral support for operators
        is deferred to ITL-509.

        Args:
            database: An ``ArrowDatabaseProtocol`` instance to attach, or ``None``
                to detach. Ignored by operator nodes in v1.
        """

    # ---------------------------------------------------------------------------
    # OperatorNode — thin blueprint (no DB)
    # ---------------------------------------------------------------------------

    def _typed_zero_row_table(self) -> "pa.Table | None":
        """This node's zero-row table: its operator applied to zero-row inputs.

        The sync path's rule (ITL-912): each input is the upstream's zero-row table
        (``StaticOutputOperatorPod._zero_row_input``), and the operator's result over
        those is this node's table with no rows, with the Arrow types, nullability,
        system-tag and source-info columns its rows have. No row is computed or read,
        no function is called and nothing is stored, and a result the node holds is
        not consulted: the types follow from the inputs' types alone.

        Returns:
            The zero-row table with system tags and source info, or ``None`` when the
            node has no operator or an input cannot say its Arrow types without rows
            (see ``typed_zero_row_table``).
        """
        if self._operator is None:
            return None
        inputs: list[StreamProtocol] = []
        for upstream in self._input_streams:
            zero_rows = StaticOutputOperatorPod._zero_row_input(upstream)
            if zero_rows is None:
                return None
            inputs.append(zero_rows)
        # zero_row_result calls static_process, not process: an invocation recorded by
        # a tracker would add a step to whatever pipeline is being built while this
        # table is read.
        operator = cast(StaticOutputOperatorPod, self._operator)
        result = operator.zero_row_result(*inputs)
        # In the column order of the node's rows; an operator's own table may order
        # them otherwise (a join lists its data before its system tags).
        return StaticOutputOperatorPod._in_row_order(
            result.as_table(columns=ZERO_ROW_INPUT_COLUMNS), result.keys()[0]
        )


class OperatorNode(OperatorNodeBase):
    """Thin blueprint descriptor for an operator pod invocation.

    Carries no database references.  Calling ``iter_data()`` raises
    ``PipelineJobRequiredError`` — wrap the containing ``Pipeline`` in a
    ``PipelineJob`` to obtain an executable ``OperatorJobNode``.

    This is the node type recorded inside a ``Pipeline`` context manager
    and serialized to disk via ``Pipeline.save()``.
    """

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Raise ``PipelineJobRequiredError`` — blueprint nodes cannot produce data.

        Raises:
            PipelineJobRequiredError: Always.
        """
        raise PipelineJobRequiredError(
            f"OperatorNode '{self.label}' is a blueprint — it carries no database "
            "references and cannot produce data directly.  "
            "Wrap the containing Pipeline in a PipelineJob to obtain an executable "
            "OperatorJobNode."
        )
        # Python classifies a function as a generator (returning an Iterator
        # lazily) based purely on the *syntactic* presence of a ``yield``
        # statement anywhere in the function body — this is determined at
        # compile time, not at runtime.  Without ``yield``, this would be a
        # plain function: calling it would execute the body immediately and
        # raise ``PipelineJobRequiredError`` before the caller ever gets an
        # iterator object.  With ``yield`` present (even though it is
        # unreachable due to the ``return`` above), Python compiles the
        # function as a generator function.  Calling it therefore returns a
        # generator object instantly without executing any body code; the
        # ``raise`` is deferred until the caller first calls ``next()`` on
        # the iterator (i.e., when iteration actually begins).  This matches
        # the expected ``Iterator`` return-type contract.
        return  # pragma: no cover
        yield  # pragma: no cover

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return no rows: a blueprint node holds no data."""
        check_head_size(n)
        return self._head_without_rows(
            ColumnConfig.handle_config(columns, all_info=all_info),
            ViewStatus.UNAVAILABLE,
            note=BLUEPRINT_NOTE,
        )

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[Any, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        """Raise ``PipelineJobRequiredError`` — blueprint nodes cannot produce data.

        Raises:
            PipelineJobRequiredError: Always.
        """
        raise PipelineJobRequiredError(
            f"OperatorNode '{self.label}' is a blueprint — it carries no database "
            "references and cannot produce data directly.  "
            "Wrap the containing Pipeline in a PipelineJob to obtain an executable "
            "OperatorJobNode."
        )

    @classmethod
    def from_descriptor(
        cls,
        descriptor: dict[str, Any],
        operator: OperatorPodProtocol | None,
        input_streams: tuple[StreamProtocol, ...] | list[StreamProtocol],
        databases: dict[str, Any],
    ) -> "OperatorNode":
        """Construct an OperatorNode from a serialized descriptor.

        When *operator* and *input_streams* are provided the node operates
        in full mode — constructed normally via ``__init__``.  When
        *operator* is ``None`` the node is created in read-only mode with
        metadata from the descriptor; computation methods will raise
        ``PipelineJobRequiredError``.

        Args:
            descriptor: The serialized node descriptor dict.
            operator: An optional live operator instance.  ``None`` for
                read-only mode.
            input_streams: Input streams for the operator.  Empty tuple
                for read-only mode.
            databases: Unused — kept for API parity with
                ``OperatorJobNode.from_descriptor``.

        Returns:
            A new ``OperatorNode`` instance.

        Raises:
            ValueError: If ``table_scope`` is missing or invalid.
        """
        from orcapod.pipeline.serialization import LoadStatus

        if "table_scope" not in descriptor:
            raise ValueError(
                f"OperatorNode descriptor is missing required 'table_scope' field: "
                f"{descriptor.get('label', '<unlabeled>')}"
            )
        raw_table_scope = descriptor["table_scope"]
        if raw_table_scope not in ("pipeline_hash", "content_hash"):
            raise ValueError(
                f"OperatorNode descriptor has invalid 'table_scope' value "
                f"{raw_table_scope!r} for {descriptor.get('label', '<unlabeled>')}; "
                "expected one of ('pipeline_hash', 'content_hash')"
            )
        table_scope: Literal["pipeline_hash", "content_hash"] = raw_table_scope

        if operator is not None and input_streams:
            # Full mode: construct normally.
            node = cls(
                operator=operator,
                input_streams=input_streams,
                label=descriptor.get("label"),
                table_scope=table_scope,
            )
            node._descriptor = descriptor
            # The run's pipeline hash, not one recomputed from the rebuilt inputs.
            node._stored_pipeline_hash = descriptor.get("pipeline_hash")
            node._load_status = LoadStatus.FULL
            return node

        # Read-only mode: bypass __init__ (which calls validate_inputs) and
        # manually set the minimum required state.
        node = cls.__new__(cls)

        # From LabelableMixin
        node._label = descriptor.get("label")

        # From DataContextMixin
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig

        node._data_context = contexts.resolve_saved_context(
            descriptor.get("data_context_key")
        )
        config_dict = descriptor.get("config")
        node._orcapod_config = (
            OrcapodConfig.from_dict(config_dict)
            if config_dict is not None
            else DEFAULT_CONFIG
        )

        # From ContentIdentifiableBase
        node._content_hash_cache = {}
        node._cached_int_hash = None

        # From PipelineElementBase
        node._pipeline_hash_cache = {}

        # From TemporalMixin
        node._modified_time = None

        # From OperatorNodeBase
        node._operator = None
        node._input_streams = ()
        node.tracker_manager = DEFAULT_TRACKER_MANAGER

        # Descriptor metadata for read-only access
        node._descriptor = descriptor
        node._stored_schema = descriptor.get("output_schema", {})
        node._stored_content_hash = descriptor.get("content_hash")
        node._stored_pipeline_hash = descriptor.get("pipeline_hash")
        node._stored_pipeline_path = tuple(descriptor.get("pipeline_path", ()))
        node._stored_node_uri = tuple(descriptor.get("node_uri") or [])
        node._table_scope = table_scope
        node._node_identity_path_cache = None

        # Blueprint nodes loaded read-only are always UNAVAILABLE (no DB)
        node._load_status = LoadStatus.UNAVAILABLE

        return node

    def set_ephemeral_database(self, database: "ArrowDatabaseProtocol | None") -> None:
        """No-op — blueprint operator nodes carry no database references."""

    def as_node(self) -> OperatorNode:
        """Return the lightweight blueprint equivalent of this node.

        Returns a fresh ``OperatorNode`` with the same operator, input streams,
        label, table scope, and tracker manager.  Its ``content_hash()`` /
        ``pipeline_hash()`` are identical to those of this node.

        Returns:
            A new equivalent ``OperatorNode``.

        Raises:
            RuntimeError: If this node is in UNAVAILABLE state (no live
                operator).  UNAVAILABLE nodes cannot be cloned into a usable
                blueprint — callers must handle this status before invoking
                ``as_node()``.
        """
        if self._operator is None:
            raise RuntimeError(
                f"Cannot clone OperatorNode {self._label!r} into a blueprint: "
                "the node is UNAVAILABLE (no live operator was provided when "
                "it was loaded). Only FULL or READ_ONLY nodes support as_node()."
            )
        return OperatorNode(
            operator=self._operator,
            input_streams=self._input_streams,
            label=self._label,
            table_scope=self._table_scope,
            tracker_manager=self.tracker_manager,
        )


# ---------------------------------------------------------------------------
# OperatorJobNode — DB-backed execution node
# ---------------------------------------------------------------------------


class OperatorJobNode(ProvenanceMethods, RunHoldingNode, OperatorNodeBase):
    """DB-backed execution node for operator pod invocations.

    Stream node representing an operator invocation with optional DB persistence.

    When constructed without database parameters, provides the core stream
    interface (identity, schema, iteration) without any persistence.  When
    databases are provided (either at construction or via ``attach_databases``),
    adds pipeline record storage with per-row deduplication, ``get_all_records()``
    for retrieving stored results, ``as_source()`` for creating a
    ``DerivedSource`` from DB records, and three-tier cache mode
    (OFF / LOG / REPLAY).

    Node identity path structure::

        operator.uri / schema:{pipeline_hash} / instance:{content_hash}

    Where ``pipeline_hash`` encodes the pipeline structure (operator +
    upstream topology) and ``instance:{content_hash}`` is the
    data-inclusive hash that encodes upstream source identities, ensuring
    each unique source combination gets its own cache table.

    Cache modes:
        - **OFF** (default): compute, don't write to DB.
        - **LOG**: compute AND write to DB (append-only historical record).
        - **REPLAY**: skip computation, flow cached results downstream.
    """

    # Set once a read has reported the stored rows it skipped (rows of another data
    # context); a class default so nodes rebuilt from a saved descriptor start unset.
    _skipped_rows_reported: bool = False
    _stale_tables_checked: bool = False
    # The job file's expected row count for a loaded node, and whether it was checked
    # against the attached database (ITL-1043). Advisory: a mismatch only warns.
    _expected_row_count: int | None = None
    _row_count_checked: bool = False

    @property
    def _stored_table_path(self) -> tuple[str, ...]:
        """Path of this node's stored table: the identity path plus the layout version.

        Every read and write of the operator-cache table uses this path.
        """
        return self.node_identity_path + (OPERATOR_DB_SCHEMA_VERSION,)

    def __init__(
        self,
        operator: OperatorPodProtocol,
        input_streams: tuple[StreamProtocol, ...] | list[StreamProtocol],
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        config: OrcapodConfig | None = None,
        # Optional DB params for persistent mode:
        pipeline_database: ArrowDatabaseProtocol | None = None,
        cache_mode: CacheMode = CacheMode.OFF,
        table_scope: Literal["pipeline_hash", "content_hash"] = "pipeline_hash",
    ):
        super().__init__(
            operator=operator,
            input_streams=input_streams,
            tracker_manager=tracker_manager,
            label=label,
            config=config,
            table_scope=table_scope,
        )

        # Stream-level caching state
        self._cached_output_stream: StreamProtocol | None = None
        self._cached_output_table: pa.Table | None = None

        # DB persistence state (initially None; set via __init__ params or attach_databases)
        self._pipeline_database: ArrowDatabaseProtocol | None = None
        self._cache_mode: CacheMode = CacheMode.OFF

        # True for a node rebuilt by ``from_descriptor`` (a job loaded by
        # ``PipelineJob.load``): its output was computed by an earlier run.
        self._loaded = False

        if pipeline_database is not None:
            self.attach_databases(
                pipeline_database=pipeline_database,
                cache_mode=cache_mode,
            )
        # A node that has not run in this process holds nothing and is stale (ITL-1095).
        self._set_modified_time(None)

    @property
    def cache_mode(self) -> CacheMode:
        """How the node caches its output (``OFF``, ``LOG`` or ``REPLAY``): set by
        ``attach_databases``, ``CacheMode.OFF`` until then."""
        return self._cache_mode

    # ------------------------------------------------------------------
    # as_node — return the lightweight OperatorNode equivalent
    # ------------------------------------------------------------------

    def as_node(self) -> OperatorNode:
        """Return the lightweight ``OperatorNode`` equivalent of this job node.

        Returns:
            A new ``OperatorNode`` with the same operator, input streams,
            label, table scope, and tracker manager.  Its ``content_hash()`` /
            ``pipeline_hash()`` are identical to those of this ``OperatorJobNode``.
        """
        return OperatorNode(
            operator=self._operator,
            input_streams=self._input_streams,
            label=self._label,
            table_scope=self._table_scope,
            tracker_manager=self.tracker_manager,
        )

    def set_ephemeral_database(self, database: "ArrowDatabaseProtocol | None") -> None:
        """No-op — full ephemeral support for operators is deferred to ITL-509."""

    # ------------------------------------------------------------------
    # Override clear_cache to also clear output stream caches
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear output caches and node identity path cache.

        The node then holds no run: it is stale and reads empty, as before its first run
        in this process (ITL-1095). Rows it stored stay stored (``as_source()``).
        """
        super().clear_cache()  # clears _node_identity_path_cache + _update_modified_time
        self._cached_output_stream = None
        self._cached_output_table = None
        self._run_load_only = False
        # No content established: the node, and every node downstream, is stale.
        self._set_modified_time(None)

    # ------------------------------------------------------------------
    # attach_databases
    # ------------------------------------------------------------------

    def require_storable_tables(
        self,
        pipeline_database: ArrowDatabaseProtocol,
        cache_mode: CacheMode = CacheMode.OFF,
    ) -> None:
        """Refuse a database that cannot store this node's cache (ITL-1162).

        With caching on, checks the cache's own columns (``__node_content_hash``,
        ``__data_context``, and the entry id and insertion time every row is stored
        with, ``_stored_only_fields``); the operator's output columns are checked
        when they are written. Nothing is computed and nothing changes.

        Args:
            pipeline_database: The database for the cache.
            cache_mode: The caching behaviour; ``OFF`` stores nothing.

        Raises:
            UnstorableColumnError: If the database cannot store a column, naming
                this node.
        """
        if cache_mode == CacheMode.OFF:
            return
        try:
            require_storable(
                pipeline_database,
                pa.schema(
                    [
                        pa.field(constants.NODE_CONTENT_HASH_COL, pa.large_binary()),
                        data_context_field(),
                        *_stored_only_fields(),
                    ]
                ),
            )
        except UnstorableColumnError as exc:
            exc.add_context(node_label=self.label)
            raise

    def attach_databases(
        self,
        pipeline_database: ArrowDatabaseProtocol,
        cache_mode: CacheMode = CacheMode.OFF,
    ) -> None:
        """Attach a database for persistent caching and pipeline records.

        Args:
            pipeline_database: Database for pipeline records.
            cache_mode: Caching behaviour (OFF, LOG, or REPLAY).

        Raises:
            InputValidationError: If the node's output has data-tags, the cache
                mode stores or replays them (``LOG`` or ``REPLAY``), and the database
                cannot keep the marker that states them (ITL-1019). Nothing is
                attached or written.
            ValueError: If the cache mode is ``LOG`` or ``REPLAY`` and a source
                feeding the node is restricted (a condition or a limit, ITL-1218):
                the cache is kept under the node's identity, which a restriction is
                not part of. Nothing is attached.
        """
        from orcapod.pipeline.restrictions import refuse_restricted_operator_cache

        refuse_restricted_operator_cache(self, cache_mode)
        if cache_mode != CacheMode.OFF:
            # A stored table states its data-tags; refuse a database that
            # cannot, before anything runs or is written (ITL-1019).
            require_database_keeps_marker(pipeline_database, stream_data_tags(self))

        # A database that cannot store the cache refuses it now, before anything
        # runs or changes (ITL-1162).
        self.require_storable_tables(pipeline_database, cache_mode)
        self._skipped_rows_reported = False
        self._stale_tables_checked = False
        self._row_count_checked = False
        self._pipeline_database = pipeline_database
        self._cache_mode = cache_mode

        # Clear caches
        self._node_identity_path_cache = None
        self.clear_cache()
        self._invalidate_content_hash_cache()
        self._invalidate_pipeline_hash_cache()

    # ------------------------------------------------------------------
    # from_descriptor — reconstruct from a serialized pipeline descriptor
    # ------------------------------------------------------------------

    @classmethod
    def from_descriptor(
        cls,
        descriptor: dict[str, Any],
        operator: OperatorPodProtocol | None,
        input_streams: tuple[StreamProtocol, ...] | list[StreamProtocol],
        databases: dict[str, Any],
        job_content_hash: str | None = None,
        expected_row_count: int | None = None,
    ) -> OperatorJobNode:
        """Construct an OperatorJobNode from a serialized descriptor.

        When *operator* and *input_streams* are provided the node operates
        in full mode — constructed normally via ``__init__``.  When
        *operator* is ``None`` the node is created in read-only mode with
        metadata from the descriptor; computation methods will raise
        ``RuntimeError``.

        Args:
            descriptor: The serialized node descriptor dict.
            operator: An optional live operator instance.  ``None`` for
                read-only mode.
            input_streams: Input streams for the operator.  Empty tuple
                for read-only mode.
            databases: Mapping of database role names (``"pipeline"``)
                to database instances.
            job_content_hash: The content hash of the node in the saved job's run,
                which its stored rows carry (``PipelineJob.load`` refuses a file that
                does not record it).
            expected_row_count: How many rows the database held under the run's key when
                the job was saved, from the job file's optional ``run.expected_rows``;
                ``None`` when not recorded. Advisory: a read returns what the database
                holds and only warns (``OrcapodDatabaseContentWarning``) when it differs.

        Returns:
            A new ``OperatorJobNode`` instance.
        """
        from orcapod.pipeline.serialization import LoadStatus

        if "table_scope" not in descriptor:
            raise ValueError(
                f"OperatorJobNode descriptor is missing required 'table_scope' field: "
                f"{descriptor.get('label', '<unlabeled>')}"
            )
        raw_table_scope = descriptor["table_scope"]
        if raw_table_scope not in ("pipeline_hash", "content_hash"):
            raise ValueError(
                f"OperatorJobNode descriptor has invalid 'table_scope' value "
                f"{raw_table_scope!r} for {descriptor.get('label', '<unlabeled>')}; "
                "expected one of ('pipeline_hash', 'content_hash')"
            )
        table_scope: Literal["pipeline_hash", "content_hash"] = raw_table_scope

        pipeline_db = databases.get("pipeline")
        cache_mode_str = descriptor.get("cache_mode", "off")
        try:
            cache_mode = CacheMode(cache_mode_str)
        except ValueError:
            cache_mode = CacheMode.OFF

        if operator is not None and input_streams:
            # Full mode: construct normally.
            node = cls(
                operator=operator,
                input_streams=input_streams,
                label=descriptor.get("label"),
                table_scope=table_scope,
            )
            if pipeline_db is not None:
                node.attach_databases(
                    pipeline_database=pipeline_db,
                    cache_mode=cache_mode,
                )
            node._descriptor = descriptor
            # The run's pipeline hash, not one recomputed from the rebuilt inputs: it
            # names the table the run's rows are stored in.
            node._stored_pipeline_hash = descriptor.get("pipeline_hash")
            node._saved_run_content_hash = job_content_hash
            node._expected_row_count = expected_row_count
            node._load_status = LoadStatus.FULL
            node._loaded = True
            return node

        # Read-only mode: bypass __init__, set minimum required state
        node = cls.__new__(cls)

        # From LabelableMixin
        node._label = descriptor.get("label")

        # From DataContextMixin
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig

        node._data_context = contexts.resolve_saved_context(
            descriptor.get("data_context_key")
        )
        config_dict = descriptor.get("config")
        node._orcapod_config = (
            OrcapodConfig.from_dict(config_dict)
            if config_dict is not None
            else DEFAULT_CONFIG
        )

        # From ContentIdentifiableBase
        node._content_hash_cache = {}
        node._cached_int_hash = None

        # From PipelineElementBase
        node._pipeline_hash_cache = {}

        # From TemporalMixin
        node._modified_time = None

        # From OperatorNodeBase
        node._operator = None
        node._input_streams = tuple(input_streams)
        node.tracker_manager = DEFAULT_TRACKER_MANAGER

        # From OperatorJobNode
        node._cached_output_stream = None
        node._cached_output_table = None
        node._loaded = True

        # DB persistence state
        node._pipeline_database = pipeline_db
        node._cache_mode = cache_mode

        # Descriptor metadata for read-only access
        node._descriptor = descriptor
        node._stored_schema = descriptor.get("output_schema", {})
        node._stored_content_hash = descriptor.get("content_hash")
        node._saved_run_content_hash = job_content_hash
        node._expected_row_count = expected_row_count
        node._stored_pipeline_hash = descriptor.get("pipeline_hash")
        node._stored_pipeline_path = tuple(descriptor.get("pipeline_path", ()))
        node._stored_node_uri = tuple(descriptor.get("node_uri") or [])
        node._table_scope = table_scope
        node._node_identity_path_cache = None

        # Determine load status based on DB availability and cache mode.
        # An uncached operator (cache_mode=OFF) never writes records to the
        # database, so even when a pipeline_db exists there is nothing to
        # read back.  Only operators that actively persist results
        # (LOG or REPLAY mode) can legitimately serve data in read-only mode.
        node._load_status = LoadStatus.UNAVAILABLE
        if pipeline_db is not None and cache_mode != CacheMode.OFF:
            node._load_status = LoadStatus.READ_ONLY

        return node

    # ------------------------------------------------------------------
    # Data-tags in the stored table (ITL-1019)
    # ------------------------------------------------------------------

    def declared_data_tags(self) -> tuple[str, ...]:
        """Return the data-tags of this node's output, as the node declares them.

        A node built in this process declares its operator's predicted output: the
        names in both halves of ``keys()``. A node loaded by ``PipelineJob.load``
        declares its saved descriptor's output schema, the saved job being an
        independent point of verification of the stored table (developer, November 16,
        2026). These are the tags the node's stored table must mark.

        Returns:
            The data-tags, in tag order; empty for a plain output.
        """
        saved = self._descriptor.get("output_schema") if self._loaded else None
        if saved is not None:
            return data_tag_names(saved.get("tag", {}), saved.get("data", {}))
        return stream_data_tags(self)

    def _check_stored_data_tags(self, schema: "pa.Schema") -> None:
        """Refuse a stored table whose data-tag marker disagrees with the node.

        The stored table states its bindings (developer, November 16, 2026, ITL-1019):
        a data-tag column carries the marker of ``data_tags``. The node
        reads the table only when it agrees with what the node declares
        (``declared_data_tags``).

        Args:
            schema: The schema of the node's stored table.

        Raises:
            InputValidationError: If the table's marker is invalid, or the table and
                the node disagree on which tags are data-tags.
        """
        assert self._pipeline_database is not None
        table_path = self._pipeline_database.base_path + self._stored_table_path
        try:
            stored = stored_data_tags(schema)
        except ValueError as exc:
            raise InputValidationError(
                f"Operator node {self.label!r}, stored table {table_path}: {exc}"
            ) from exc
        expected = self.declared_data_tags()
        if set(stored) != set(expected):
            declarer = "saved job" if self._loaded else "operator"
            raise InputValidationError(
                f"Operator node {self.label!r}: the stored table {table_path} marks "
                f"the data-tags {sorted(stored)}, but the node's {declarer} "
                f"declares {sorted(expected)}. The table was not written by this "
                "node's pipeline; nothing was read."
            )

    def _stream_over_stored(self, records: "pa.Table") -> ArrowTableStream:
        """Return a stream over rows read from the stored table, without the marker.

        In a stream the binding is the shared name of a tag and a data column, so the
        marker stays with the stored table: carried on, it would be written by a
        downstream store whose binding it is not.

        The binding is the node's (``declared_data_tags``), which every read checked
        against the table's marker (``_check_stored_data_tags``).

        Args:
            records: The stored rows, without the bookkeeping columns.

        Returns:
            The stream, in this node's data context.
        """
        # Stored rows are this node's: give them its data context, not the default.
        # ``data_context`` types the Python datagrams only; the rows' Arrow context
        # column comes from the table, and a stream fills a missing one with the
        # default key.
        rows = without_tag_kind_marks(records)
        if constants.CONTEXT_KEY not in rows.column_names:
            rows = rows.append_column(
                pa.field(constants.CONTEXT_KEY, pa.large_string(), nullable=False),
                pa.array([self.data_context_key] * rows.num_rows, pa.large_string()),
            )
        return ArrowTableStream(
            rows,
            tag_columns=self.keys()[0],
            data_tags=self.declared_data_tags(),
            data_context=self.data_context,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_content_hash(self) -> ContentHash:
        """Return the content hash this node's run stores its rows under.

        A node that runs computes it. A loaded node has the one its saved job records
        (ITL-1043; ``PipelineJob.load`` refuses a file without it): the hash it would
        recompute from inputs rebuilt from the file is not the run's.
        """
        if not self._loaded or self._saved_run_content_hash is None:
            return self.content_hash()
        return ContentHash.from_string(self._saved_run_content_hash)

    def _expected_row_count_to_save(self, run_finished: bool) -> int | None:
        """Return the count ``PipelineJob.save`` records for this node, if certain.

        A loaded node keeps the count its file recorded, or none. A node of a job whose
        last run finished (its writes committed) records the rows the database holds under
        its run's content hash; otherwise nothing is certain.

        Args:
            run_finished: Whether the job's last run finished and committed.
        """
        if self._loaded:
            return self._expected_row_count
        if not run_finished or self._pipeline_database is None:
            return None
        records = self._read_stored_records()
        return 0 if records is None else records.num_rows

    def _check_expected_rows(self, found: int) -> None:
        """Warn, once per database, if a loaded node finds another number of rows than recorded.

        The database is the source of truth: the read returns what it holds (ITL-1043).

        Args:
            found: The rows the database holds under the node's run key.
        """
        if self._expected_row_count is None or self._row_count_checked:
            return
        self._row_count_checked = True
        if found != self._expected_row_count and self._pipeline_database is not None:
            report_unexpected_row_count(
                f"OperatorJobNode {self.label!r}",
                describe_database_table(
                    self._pipeline_database, self._stored_table_path
                ),
                self._expected_row_count,
                found,
            )

    def _no_stored_results_error(self) -> RuntimeError:
        """Return the error for reading a loaded operator node with nothing to serve.

        Operator outputs are stored only when caching is enabled. A loaded node
        needs a readable cache; returning an empty table when none is available
        would look like the operator produced nothing.
        """
        inputs = ", ".join(repr(stream.label) for stream in self._input_streams)
        return RuntimeError(
            f"OperatorJobNode {self.label!r} has no stored results available for this read. "
            "Operator outputs require a stored cache and replay configuration. Read the "
            f"stored results of its input nodes ({inputs}) instead; an input source "
            "whose data was not saved must be bound first with job.bind(sources=...)."
        )

    def _cannot_compute_error(self) -> RuntimeError:
        """Describe why this loaded node cannot execute an operator."""
        return RuntimeError(
            f"OperatorJobNode {self.label!r} was loaded by PipelineJob.load and "
            "cannot compute; fill it from the database with run(load_only=True), read "
            "its stored results with as_source(), or create a fresh PipelineJob to "
            "run the pipeline again."
        )

    def _filter_by_content_hash(self, table: pa.Table) -> pa.Table:
        """Filter *table* to rows whose ``NODE_CONTENT_HASH_COL`` matches this node.

        Only applied when ``table_scope="pipeline_hash"`` because in that mode
        multiple runs share the same DB table and must be disambiguated at read
        time.  In ``"content_hash"`` mode every run has its own table.
        """
        if self._table_scope != "pipeline_hash":
            return table
        col_name = constants.NODE_CONTENT_HASH_COL
        if col_name not in table.column_names:
            raise ValueError(
                f"Cannot isolate records for table_scope='pipeline_hash': "
                f"required column {col_name!r} is missing from the stored table. "
                "This may indicate records written by an older version of the code."
            )
        col_type = table.schema.field(col_name).type
        if col_type != pa.large_binary():
            raise ValueError(
                f"Cannot isolate records for table_scope='pipeline_hash': "
                f"column {col_name!r} has type {col_type!r}, expected large_binary. "
                "This table was written by an older version of the code that stored "
                "this column as large_string. Drop and recreate the affected pipeline "
                "DB tables (see docs/dev/design_issues/ ON1)."
            )
        own_hash = self.content_hash().to_prefixed_digest()
        mask = pc.equal(table.column(col_name), own_hash)
        return table.filter(mask)

    # ------------------------------------------------------------------
    # Computation and caching
    # ------------------------------------------------------------------

    def _store_output_stream(self, stream: StreamProtocol) -> None:
        """Materialize stream and store in the pipeline database with per-row dedup.

        The stored table states its bindings (developer, November 16, 2026, ITL-1019):
        the columns of the node's data-tags carry the marker of
        ``data_tags``, written from the operator's own output binding and
        never from metadata that came in from upstream. The marker is put on after the
        per-row record hashes are computed, so it never enters them.
        """
        pipeline_database = self._pipeline_database
        if pipeline_database is None:
            raise RuntimeError(
                f"OperatorJobNode {self.label!r} has no pipeline database attached."
            )
        self._account_for_stale_tables()
        output_table = stream.as_table(
            columns={"source": True, "system_tags": True, "context": True},
        )
        # The inputs and the rows name their data context; all must be this node's.
        for input_stream in self._input_streams:
            require_same_data_context(
                input_stream.data_context_key, self.data_context_key
            )
        row_context_keys = cast(
            "list[str]",
            pc.unique(output_table.column(constants.CONTEXT_KEY)).to_pylist(),
        )
        for row_context_key in row_context_keys:
            require_same_data_context(row_context_key, self.data_context_key)
        output_table = output_table.drop([constants.CONTEXT_KEY])

        # The entry id of each row (ITL-1471) is computed from the row as the stream
        # gives it, before the bookkeeping columns are added.
        rows = list(stream.iter_data())
        if len(rows) != output_table.num_rows:
            raise RuntimeError(
                f"OperatorJobNode {self.label!r}: the output gave {len(rows)} rows "
                f"and a table of {output_table.num_rows}"
            )

        # Always append the node content hash column first so that it is included
        # in the per-row HASH_COLUMN_NAME computation below.  This makes record IDs
        # run-scoped: identical rows from different runs carry a different
        # NODE_CONTENT_HASH_COL value and therefore hash to different record IDs,
        # preventing skip_duplicates=True from silently dropping a later run's rows.
        # When table_scope="pipeline_hash" this column is also used at read time by
        # _filter_by_content_hash to isolate rows belonging to the current run.
        # In content_hash scope each run has its own isolated table so the column
        # is present in storage but filtering is never applied on reads.
        n_rows = output_table.num_rows
        node_content_hash = self.content_hash()
        # The node content hash enters the hashed row as it is stored, with its label:
        # a hash fed into another hash carries its label (ITL-1154).
        output_table = output_table.append_column(
            constants.NODE_CONTENT_HASH_COL,
            pa.array(
                [node_content_hash.to_prefixed_digest()] * n_rows,
                type=pa.large_binary(),
            ),
        )

        # Per-row record hashes for dedup: hash(tag + data + system_tags + node_content_hash).
        # The same pass gives each row its entry id: its tags, system tags and the
        # hash of its data (``row_ids.entry_id``), the id a side effect keys the
        # row by. It is no part of the row key.
        arrow_hasher = self.data_context.arrow_hasher
        record_hashes = []
        entry_ids = []
        row = 0
        for batch in output_table.to_batches():
            for i in range(len(batch)):
                # A hash stored as a value carries its label: b"<label>:<digest>".
                record_hashes.append(
                    arrow_hasher.hash_table(batch.slice(i, 1)).to_prefixed_digest()
                )
                entry_ids.append(
                    row_ids.entry_id(arrow_hasher, *rows[row]).to_prefixed_digest()
                )
                row += 1

        output_table = output_table.add_column(
            0,
            self.HASH_COLUMN_NAME,
            pa.array(record_hashes, type=pa.large_binary()),
        )
        # Appended after the row key is computed, so neither enters it: the entry
        # id, and the time of insertion. A row already stored is skipped below and
        # keeps its first time.
        entry_id_field, recorded_at_field = _stored_only_fields()
        stored_table = output_table.append_column(
            entry_id_field, pa.array(entry_ids, type=entry_id_field.type)
        ).append_column(
            recorded_at_field,
            pa.array([datetime.now(timezone.utc)] * n_rows, recorded_at_field.type),
        )

        # Store — record IDs are run-scoped (NODE_CONTENT_HASH_COL is included in
        # the hash), so rows from different runs with identical output will have
        # distinct record IDs and both be stored.  skip_duplicates=True still
        # deduplicates exact re-runs of the same node within a single run.
        # The data context column is appended after the record hashes are computed:
        # it names the generation of hashing that produced them and is not hashed.
        try:
            pipeline_database.add_records(
                self._stored_table_path,
                with_data_context(
                    mark_data_tags(stored_table, stream_data_tags(self)),
                    self.data_context_key,
                ),
                record_id_column=self.HASH_COLUMN_NAME,
                skip_duplicates=True,
            )
        except OrcapodDatabaseReadError:
            # The stored table could not be read: report the read failure itself.
            raise
        except Exception as exc:
            raise OrcapodDatabaseWriteError(
                self._stored_table_path, node_label=self.label
            ) from exc

        self._cached_output_table = output_table.drop(
            [self.HASH_COLUMN_NAME, constants.NODE_CONTENT_HASH_COL]
        )

    def _account_for_stale_tables(self) -> None:
        """Report operator caches at other layouts once per attached database."""
        if self._stale_tables_checked or self._pipeline_database is None:
            return
        account_for_stale_tables(
            find_stale_tables(
                self._pipeline_database, name_path=self.node_uri[:1] or None
            ),
            delivers_side_effects=False,
            current_context=self.data_context_key,
        )
        self._stale_tables_checked = True

    def _read_stored_records(self) -> "pa.Table | None":
        """Read this node's stored rows written under the current data context.

        Every read of the whole stored table goes through here. Rows are narrowed to
        the current data context (``__data_context``) and then to this node's content
        hash (``_filter_by_content_hash``). Rows of another data context stay in the
        table untouched, and a table at the older layout is not read (ITL-944).

        Returns:
            The stored rows, with ``HASH_COLUMN_NAME`` and ``NODE_CONTENT_HASH_COL``
            still present and without the ``__data_context`` column, or ``None``
            when the table does not exist.
        """
        records = self._read_current_context_records()
        if records is None:
            return None
        return self._filter_by_content_hash(records)

    def _read_current_context_records(self) -> "pa.Table | None":
        """Read every row of this node's stored table written under the current data context.

        The rows of every run that shares the table, before ``_filter_by_content_hash``
        keeps this node's. Rows of another data context are left out and reported once.

        Every read of the stored table goes through here, so every read checks the
        table's data-tag marker against the node (``_check_stored_data_tags``).
        The rows keep the marker; a read into a stream drops it
        (``_stream_over_stored``).

        The columns a row is stored with beside its key, its entry id and its
        insertion time (``_STORED_ONLY_COLUMNS``, ITL-1471), are dropped here, so no
        read that serves rows shows them.

        Returns:
            The rows, with ``HASH_COLUMN_NAME`` and ``NODE_CONTENT_HASH_COL`` and without
            the ``__data_context`` column, or ``None`` when the table does not exist.

        Raises:
            InputValidationError: If the table and the node disagree on which tags are
                data-tags (ITL-1019).
        """
        assert self._pipeline_database is not None
        self._account_for_stale_tables()
        records = self._pipeline_database.get_all_records(
            self._stored_table_path, record_id_column=self.HASH_COLUMN_NAME
        )
        if records is None:
            return None
        self._check_stored_data_tags(records.schema)
        stored_rows = records.num_rows
        records = keep_rows_of_data_context(records, self.data_context_key)
        if records.num_rows < stored_rows and not self._skipped_rows_reported:
            self._skipped_rows_reported = True
            report_rows_of_other_data_contexts(
                self._pipeline_database.base_path + self._stored_table_path,
                stored_rows - records.num_rows,
            )
        return self._in_row_order_on_read(
            records.drop([constants.DATA_CONTEXT_COL, *_STORED_ONLY_COLUMNS])
        )

    def _in_row_order_on_read(self, records: "pa.Table") -> "pa.Table":
        """Put a stored table's user columns in row order; bookkeeping stays in place.

        A node run on its own stored its rows in its operator's column order before
        ITL-1133 (a join lists its data before its system tags); every writer now
        stores the row order (``StaticOutputOperatorPod._in_row_order``). Read in row
        order, such a table gives the columns of every other path, and a table in row
        order is returned unchanged. Only the order changes: no column is added,
        dropped, renamed or retyped, and the bookkeeping columns (``__``, and
        ``HASH_COLUMN_NAME``) keep their positions.
        """
        names = records.column_names

        def bookkeeping(name: str) -> bool:
            return (
                name.startswith(constants.META_PREFIX) or name == self.HASH_COLUMN_NAME
            )

        user = [n for n in names if not bookkeeping(n)]
        ordered = iter(
            StaticOutputOperatorPod._in_row_order(
                records.select(user), self.keys()[0]
            ).column_names
        )
        order = [n if bookkeeping(n) else next(ordered) for n in names]
        if order == names:
            return records
        return records.select(order)

    def _make_empty_table(self) -> "pa.Table":
        """Build the zero-row table this node reads as when it holds no rows.

        The one place a node's zero-row table is built, for every path that reads
        it (``as_table``, ``_load_cached_stream_from_db`` in REPLAY, the stored
        view): the node's operator applied to its inputs' zero-row tables
        (``_typed_zero_row_table``), so the columns, Arrow types and nullability are
        those of the node's rows. A loaded node without its operator (``Index``,
        ``Pick``) takes its saved Arrow pair (``_saved_zero_row_table``): a saved
        ``int32`` stays ``int32`` (CodeRabbit on PR 454). Only when neither can say
        the Arrow types (an input that cannot type itself without rows, a known
        limitation, see ``as_table``; a saved text naming an unregistered logical
        type) is the table built from the node's Python output schema with
        ``arrow_utils.make_empty_table``: plain types give ``nullable=False`` fields
        and ``T | None`` types ``nullable=True`` fields.
        """
        typed = self._typed_zero_row_table()
        if typed is None:
            typed = self._saved_zero_row_table()
        if typed is not None:
            return typed
        tag_schema, data_schema = self.output_schema()
        return arrow_utils.make_empty_table(
            {**tag_schema, **data_schema},
            self.data_context.type_converter,
        )

    def _read_run_records(self) -> "pa.Table | None":
        """Read the stored rows of this node's run: its content hash, current context.

        A loaded node names its run by the content hash its job file records (every
        saved job records it, ``PipelineJob.load``; ITL-1043).

        Returns:
            The run's stored rows (``_read_stored_records``), or ``None`` when the table
            does not exist.
        """
        stored = self._read_current_context_records()
        records = None if stored is None else self._filter_by_content_hash(stored)
        return records

    def _load_cached_stream_from_db(self) -> "ArrowTableStream | None":
        """Read from DB in CacheMode.REPLAY only, without modifying node state.

        Returns an ``ArrowTableStream`` (possibly wrapping zero rows) when
        ``CacheMode.REPLAY`` is active and a database is attached; ``None``
        otherwise.

        This method is intentionally **state-free**: it never assigns to
        ``_cached_output_stream`` and never calls ``_update_modified_time()``.
        Repeated calls re-query the DB each time — in-memory caching is the
        responsibility of the computation paths (``run()``, ``execute()``).

        Guards:
            - Returns ``None`` if ``_pipeline_database is None``.
            - Returns ``None`` if ``_cache_mode != CacheMode.REPLAY``
              (LOG/OFF modes may have stale historical records in the DB).

        The database is the source of truth: a loaded node reads under its run's
        content hash, which its job file records, exactly as a node that runs does, so
        an absent table or a table with no rows under that hash is an empty result.
        """
        if self._pipeline_database is None:
            return None
        if self._cache_mode != CacheMode.REPLAY:
            return None
        records = self._read_run_records()
        if self._loaded:
            self._check_expected_rows(0 if records is None else records.num_rows)
        if records is None:
            if self._operator is None and not self._loaded:
                # Read-only (deserialized) node with no operator: cannot derive
                # schema without a live operator. Return None so callers fall
                # back to iter([]) / _make_empty_table() guarded paths.
                return None
            # A loaded node without its operator gets its saved Arrow types here.
            records_table = self._make_empty_table()
        else:
            records_table = records
            # Drop internal columns that must not surface to stream consumers.
            cols_to_drop = [
                c
                for c in [constants.NODE_CONTENT_HASH_COL, self.HASH_COLUMN_NAME]
                if c in records_table.column_names
            ]
            if cols_to_drop:
                records_table = records_table.drop(cols_to_drop)
        return self._stream_over_stored(records_table)

    def get_cached_output(self) -> StreamProtocol | None:
        """Return cached output stream in REPLAY mode, else None.

        Returns:
            The cached stream if REPLAY mode and DB records exist,
            otherwise None.
        """
        if self._pipeline_database is None:
            return None
        if self._cache_mode != CacheMode.REPLAY:
            return None
        self._replay_from_cache()
        return self._cached_output_stream

    def execute(
        self,
        *input_streams: StreamProtocol,
        observer: ExecutionObserverProtocol | None = None,
        load_only: bool = False,
    ) -> StreamProtocol:
        """Execute input streams: compute, persist, and cache.

        Args:
            *input_streams: Input streams to execute.
            observer: Optional execution observer for hooks.
            load_only: A load-only run (ITL-1095): the operator still runs on the rows
                that reach it (it is not a stored computation), but nothing is
                written: a ``LOG`` node does not store its output. ``REPLAY`` replays
                as in any run.

        Returns:
            The node's output stream: its rows as the table ``_rows_to_table`` builds
            from them (``StaticOutputOperatorPod._output_stream``), with this node's
            identity, built from the operator's result table without making rows
            (ITL-1347, stage 2). The sync orchestrator hands it to every successor.

        Raises:
            ValueError: If the node caches (``LOG`` or ``REPLAY``) and a source
                feeding ``input_streams`` is restricted (ITL-1218); nothing is
                read, computed or written.
        """
        from orcapod.pipeline.restrictions import refuse_restricted_operator_cache
        from orcapod.pipeline.observer import NoOpObserver

        # A loaded node runs only in a load-only run, with its rebuilt operator.
        if self._loaded and (not load_only or self._operator is None):
            raise self._cannot_compute_error()
        refuse_restricted_operator_cache(self, self._cache_mode, streams=input_streams)

        node_label = self.label
        node_hash = self.content_hash().to_string()

        obs = observer if observer is not None else NoOpObserver()
        ctx_obs = obs.contextualize(*self.node_identity_path)

        try:
            ctx_obs.on_node_start(node_label, node_hash)

            self._run_load_only = load_only
            # Check REPLAY cache first
            cached_output = self.get_cached_output()
            if cached_output is not None:
                try:
                    output = StaticOutputOperatorPod._output_stream(
                        cached_output.as_table(columns=ZERO_ROW_INPUT_COLUMNS),
                        cached_output.keys(),
                        self,
                    )
                except RowBuiltFormUnavailable:
                    # A stored table F cannot build from: its rows, built when read,
                    # as main handed them on (ITL-1347).
                    output = rows_on_read(cached_output.iter_data(), self)
                ctx_obs.on_node_end(node_label, node_hash)
                return output

            # Compute
            result_stream = self._operator.process(*input_streams)
            produced = produced_stream(result_stream)

            output: StreamProtocol
            # The result's table, as the table built from its rows would be, without
            # making the rows (ITL-1347, stage 2), when it provably is its rows: the
            # stream holding them is exactly an ``ArrowTableStream`` in the node's
            # data context, whose system tags are what the rows give.
            result_table: pa.Table | None = None
            if (
                reads_as_its_class(produced, ArrowTableStream)
                and produced.data_context_key == self.data_context_key
            ):
                # From the captured stream: reading the wrapper again could run the
                # operator again (an upstream with no modified time), so the table,
                # the keys and the gate describe one computation.
                result_table = produced.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
                if not system_tags_as_rows_give(result_table):
                    result_table = None
            if result_table is None:
                # Otherwise (a custom stream kind, a replaced reader, rows of another
                # data context, system tags the rows type otherwise): main's own row
                # path, its rows read once and handed on unchanged, its table built
                # where a successor reads it (ITL-1347).
                # The rows of the computation already made (``produced``), read once.
                rows = list(produced.iter_data())
                self._cached_output_stream = StaticOutputOperatorPod._kept_output(
                    result_stream, rows
                )
                output = rows_on_read(rows, self)
            else:
                # An exact ``ArrowTableStream``'s ``keys()`` are the keys its rows are
                # read with (``iter_data``), so they are the order of the result's
                # rows, to any depth of composition, never the prediction.
                keys = produced.keys()
                tag_keys = keys[0]
                data_tags = data_tag_names(*keys)

                # Cache, in row order with or without rows (ITL-1131, ITL-1133).
                if result_table.num_rows:
                    self._cached_output_stream = (
                        StaticOutputOperatorPod._keep_output_table(
                            result_table, produced
                        )
                    )
                    kept_table = self._cached_output_stream.as_table(
                        columns=ZERO_ROW_INPUT_COLUMNS
                    )
                    # The output stream, built once the node holds its run (as the
                    # sync orchestrator built a successor's input after ``execute``
                    # returned).
                    output = StaticOutputOperatorPod._as_upstream_stream(
                        kept_table, tag_keys, data_tags, self
                    )
                else:
                    self._cached_output_stream = (
                        StaticOutputOperatorPod._zero_rows_in_row_order(result_stream)
                    )
                    # Its zero-row form is derived where a successor reads it, as on
                    # main: the operator is not called again here (``RowsOnRead``).
                    output = rows_on_read([], self)

            self._update_modified_time()

            # Persist to DB only in LOG mode, and never in a load-only run.
            if (
                not load_only
                and self._pipeline_database is not None
                and self._cache_mode == CacheMode.LOG
                and self._cached_output_stream is not None
            ):
                self._store_output_stream(self._cached_output_stream)

            ctx_obs.on_node_end(node_label, node_hash)
            return output
        except BaseException:
            # A run that raised (its database write or an observer included) defines no rows.
            self.clear_cache()
            raise

    def _compute_output(self) -> StreamProtocol:
        """Run the operator over the node's inputs and return what the node keeps.

        The rows in row order, as a job run keeps them (``_kept_output``), so a node
        run on its own has the columns, in memory and stored, of the same node in a
        job (ITL-1133). The rows are computed here, not when first read.
        """
        result = produced_stream(self._operator.process(*self._input_streams))
        if reads_as_its_class(result, ArrowTableStream):
            return StaticOutputOperatorPod._keep_output_table(
                result.as_table(columns=ZERO_ROW_INPUT_COLUMNS), result
            )
        return StaticOutputOperatorPod._kept_output(result, list(result.iter_data()))

    def _compute_and_store(self, *, store: bool = True) -> None:
        """Compute operator output, optionally store in DB.

        Args:
            store: Whether a ``LOG`` node stores the output (not in a load-only run).
        """
        self._cached_output_stream = self._compute_output()

        if self._cache_mode == CacheMode.OFF or not store:
            self._update_modified_time()
            return

        self._store_output_stream(self._cached_output_stream)
        self._update_modified_time()

    def _replay_from_cache(self) -> None:
        """Load cached results from DB, skip computation.

        If no cached records exist yet, produces an empty stream with
        the correct schema (zero rows, correct columns). A loaded node reads through
        ``_load_cached_stream_from_db``, which raises when its run's rows cannot be
        read, so an unreadable cache is never kept as an empty stream.
        """
        if self._loaded:
            stream = self._load_cached_stream_from_db()
            if stream is None:
                raise self._no_stored_results_error()
            self._cached_output_stream = stream
            self._update_modified_time()
            return
        records = self._read_stored_records()
        if records is None:
            records = self._make_empty_table()
        else:
            # Drop internal columns that must not surface to stream consumers.
            cols_to_drop = [
                c
                for c in [constants.NODE_CONTENT_HASH_COL, self.HASH_COLUMN_NAME]
                if c in records.column_names
            ]
            if cols_to_drop:
                records = records.drop(cols_to_drop)

        self._cached_output_stream = self._stream_over_stored(records)
        self._update_modified_time()

    def run(self, *, load_only: bool = False) -> StreamView:
        """Run the node as a job run would, and return a ``StreamView`` of its rows.

        State-driven (developer, November 13, 2026; ITL-1095): a node that holds the
        output of a completed run and is not stale is not run again; the view shows it.
        Otherwise the node runs, after every job node it depends on that holds no
        current run (each once, in dependency order, as ``job.run()`` would run them;
        ``last_run`` reports the counts), and the run's writes are committed. A node
        built over streams outside a job runs its operator on them directly.

        Without a database:
            Always compute via the operator's ``process()`` method.

        With a database:
            - **OFF**: always compute, no DB writes.
            - **LOG**: always compute, write results to DB.
            - **REPLAY**: skip computation, load from DB.

        Args:
            load_only: As ``job.run(load_only=True)``: the operator runs on the rows
                that reach it, a ``LOG`` node writes nothing, and the nodes run first
                load their stored results only.

        Returns:
            A ``StreamView`` of every row ``as_table()`` then returns, naming its rows
            as ``head()`` then names them.

        Raises:
            RuntimeError: If the node was loaded by ``PipelineJob.load`` and
                ``load_only`` is false.
            ValueError: If this node or one it depends on caches (``LOG`` or
                ``REPLAY``) over a restricted source (ITL-1218); nothing has run.
        """
        from orcapod.pipeline.node_run import (
            check_sources,
            depends_on_job_nodes,
            refuse_unloadable_inputs,
            run_with_dependencies,
        )
        from orcapod.pipeline.restrictions import refuse_restriction_conflicts
        from orcapod.pipeline.run_summary import warn_row_failures

        if self._loaded and not load_only:
            raise self._cannot_compute_error()
        # A source read under two restrictions, or a caching operator over a restricted
        # source, is refused before any source is checked and before anything is
        # computed or written (ITL-1218).
        refuse_restriction_conflicts([self])
        # Every source feeding the run first takes in any new data (ITL-1102).
        unchecked = check_sources([self])
        if self.holds_current_run(load_only=load_only):
            return self._run_view(computed=False)
        if depends_on_job_nodes(self):
            summary = run_with_dependencies(
                self, load_only=load_only, unchecked_sources=unchecked
            )
            self._last_run = summary
            if not summary.ok:
                warn_row_failures(summary, sys._getframe(1))  # pyright: ignore[reportPrivateUsage]  # the caller's line is where the warning is shown
            return self._run_view(computed=False)
        if self._loaded:
            raise self._cannot_compute_error()
        if load_only:
            refuse_unloadable_inputs(self)
        if self.is_stale or not self._holds_run(load_only=load_only):
            # Stale, or a load-only run's output asked for by a full run: run again.
            self.clear_cache()

        computed = False
        if self._cached_output_stream is None:
            try:
                if self._pipeline_database is not None:
                    if self._cache_mode == CacheMode.REPLAY:
                        self._replay_from_cache()
                    else:
                        self._compute_and_store(store=not load_only)
                        computed = True
                        if self._cache_mode == CacheMode.LOG and not load_only:
                            # Committed before run() returns, as at the end of a job
                            # run.
                            from orcapod.pipeline.node_run import commit_run

                            commit_run([self._pipeline_database], [self])
                    self._run_load_only = load_only
                else:
                    self._cached_output_stream = self._compute_output()
                    # As in the branches with a database (ITL-1121).
                    self._run_load_only = load_only
                    self._update_modified_time()
                    computed = True
                if computed:
                    # Building the view is part of the run: a failure there clears the
                    # node (ITL-1121).
                    return self._run_view(computed=True)
            except BaseException:
                # A run that raised defines no rows.
                self.clear_cache()
                raise
        return self._run_view(computed=computed)

    def _run_view(self, *, computed: bool) -> StreamView:
        """The ``run()`` view of the node's output.

        Args:
            computed: Whether this ``run()`` computed the output lazily just now.
        """
        # Read the kept output itself: a lazy input that finishes computing during this
        # read stamps itself after the node, which would make as_table() judge it stale.
        assert self._cached_output_stream is not None
        table = self._cached_output_stream.as_table()
        if computed:
            # Without a database and in OFF mode the output stream is lazy: as_table()
            # computes it, and an uncomputed input finishes only then. Stamp the node
            # after that, or it would be older than its input and judged stale. A run
            # that reused the kept result computed nothing and is not stamped.
            self._update_modified_time()
        # The view names its rows as head() names them afterwards: computed by a live
        # computation, plain rows when replayed from the database or kept by execute().
        kept = self._cached_output_stream
        origin = "computed" if kept is None else head_of(kept.head(1), self).origin
        view = self._view_of_rows(table, origin)
        if self._run_load_only:
            return dataclasses.replace(
                view, status=ViewStatus.LOADED_ONLY, note=LOADED_ONLY_OPERATOR_NOTE
            )
        return view

    # Whether the output the node holds came from a load-only run (ITL-1095): its
    # input may lack rows, so a full run does not take it as complete.
    _run_load_only: bool = False

    def _holds_run(self, *, load_only: bool) -> bool:
        """Whether the node holds a run's output; a load-only run's only for ``load_only``."""
        return self._cached_output_stream is not None and (
            load_only or not self._run_load_only
        )

    def _current_output(self) -> StreamProtocol | None:
        """The output of the node's current run, or ``None`` when it holds none.

        A node holds a run's output from the end of ``run()``, ``execute()`` or
        ``async_execute()`` until it is cleared or becomes stale (an input or the
        operator changed since). Before a run in this process it holds none, whatever
        its cache mode and database hold (ITL-1095).
        """
        if self._cached_output_stream is None or self.is_stale:
            return None
        return self._cached_output_stream

    def held_row_count(self) -> int:
        """The number of rows of the node's current run, from its output table.

        The same count as ``iter_data`` (0 when the node holds no run), without building
        a ``(Tag, Data)`` pair per row (ITL-1351).

        Returns:
            The row count.
        """
        current = self._current_output()
        return 0 if current is None else current.as_table().num_rows

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Return an iterator over the (tag, data) rows of the node's current run.

        Read-only: never computes and never reads the database. Yields the rows of the
        latest ``run()``, ``execute()`` or ``async_execute()``; nothing before a run in
        this process, after ``clear_cache()`` or once the node is stale, in every cache
        mode (ITL-1095). ``as_source()`` reads what the node stored (``LOG``).
        """
        current = self._current_output()
        if current is None:
            return iter([])
        return current.iter_data()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Return the output of the node's current run as a PyArrow table.

        Read-only: never computes and never reads the database. Returns the rows of the
        latest ``run()``, ``execute()`` or ``async_execute()``; before a run in this
        process, after ``clear_cache()`` or once the node is stale, a zero-row table
        with the node's columns, in every cache mode (ITL-1095).

        That zero-row table has the columns, Arrow types and nullability of the
        node's rows, system tags and source info included: it is the operator's
        result over its inputs' zero-row tables, got without computing or reading
        a row (ITL-912). A loaded node without its operator (``Index``, ``Pick``)
        has the Arrow types its saved ``output_schema`` records
        (``_saved_zero_row_table``), without system-tag columns. Known limitation:
        when an input cannot say its Arrow types without rows (a ``PollingSource``
        before its first poll, an unbound input slot or ``SourceProxy``, a
        ``DerivedSource``, or a loaded node with no live upstream), the table is
        built from the node's Python output schema: an ``int8`` column reads as
        ``int64`` and the system-tag columns are absent (ITL-1089, ITL-985).
        """
        current = self._current_output()
        if current is not None:
            return current.as_table(columns=columns, all_info=all_info)
        # Every node, one with an operator or one rebuilt without it, from the one
        # builder of its zero-row table (``_make_empty_table``).
        empty_stream = ArrowTableStream(
            self._make_empty_table(),
            tag_columns=self.keys()[0],
            data_tags=stream_data_tags(self),
            data_context=self.data_context,
        )
        return empty_stream.as_table(columns=columns, all_info=all_info)

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first rows of the node's current run; never runs the operator.

        After a run (or ``execute``) the rows are the node's output in memory. Before a
        run in this process the node shows no rows and says so, pointing to ``run()``
        and to ``as_source()``, in every cache mode; once stale it says its inputs
        changed (ITL-1095). The database is never read. See ``StreamBase.head``.
        """
        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        if self._cached_output_stream is None:
            return self._head_without_rows(
                config, ViewStatus.NOT_COMPUTED, note=JOB_NODE_NOT_RUN_NOTE
            )
        if self.is_stale:
            # run() would recompute; the kept rows are not the current result.
            return self._head_without_rows(
                config, ViewStatus.NOT_COMPUTED, note=STALE_NOTE
            )
        head = head_of(self._cached_output_stream.head(n, columns=config), self)
        if self._run_load_only:
            # Output of a load-only run: its input may lack rows (ITL-1095).
            return dataclasses.replace(
                head, status=ViewStatus.LOADED_ONLY, note=LOADED_ONLY_OPERATOR_NOTE
            )
        return head

    # ------------------------------------------------------------------
    # DB retrieval
    # ------------------------------------------------------------------

    def get_all_records(
        self,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table | None":
        """Retrieve all stored records from the pipeline database.

        Returns the stored output table with column filtering applied
        per ``ColumnConfig`` conventions.  Returns ``None`` when no
        database is attached or no matching records exist. Switching to OFF
        does not remove records stored by an earlier LOG run.

        This is the raw records view: a data-tag column keeps the marker the
        stored table states it with (ITL-1019), checked against the node on read.
        ``as_source()`` reads the binding from it.

        Raises:
            InputValidationError: If the stored table and the node disagree on which
                tags are data-tags.
        """
        if self._pipeline_database is None:
            return None

        # A loaded node that cannot name its run's rows raises rather than show the
        # stored rows as none (ITL-1043): this is the read behind node.as_source().
        results = self._read_run_records()
        if self._loaded:
            # The advisory check of every read of a loaded node (ITL-1043).
            self._check_expected_rows(0 if results is None else results.num_rows)
        if results is None:
            return None

        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        # Always drop internal row-level columns.
        drop_columns = [
            c
            for c in [constants.NODE_CONTENT_HASH_COL, self.HASH_COLUMN_NAME]
            if c in results.column_names
        ]
        if not column_config.meta and not column_config.all_info:
            drop_columns.extend(
                c for c in results.column_names if c.startswith(constants.META_PREFIX)
            )
        if not column_config.source and not column_config.all_info:
            drop_columns.extend(
                c for c in results.column_names if c.startswith(constants.SOURCE_PREFIX)
            )
        if not column_config.system_tags and not column_config.all_info:
            drop_columns.extend(
                c
                for c in results.column_names
                if c.startswith(constants.SYSTEM_TAG_PREFIX)
            )
        if drop_columns:
            # Deduplicate: NODE_CONTENT_HASH_COL now starts with META_PREFIX so it
            # can appear in both the explicit list and the meta-prefix sweep above.
            unique_drop = list(
                dict.fromkeys(c for c in drop_columns if c in results.column_names)
            )
            if unique_drop:
                results = results.drop(unique_drop)

        return results if results.num_rows > 0 else None

    # ------------------------------------------------------------------
    # DerivedSource
    # ------------------------------------------------------------------

    def as_source(
        self,
        tags_as_data: "Mapping[str, str] | None" = None,
        *,
        source_id: str | None = None,
    ):
        """Return a DerivedSource backed by the DB records of this node.

        Without ``tags_as_data`` and ``source_id``, its ``source_id`` is the
        ``DerivedSource`` default, ``derived_v1:<algorithm label>:<hex>`` of this
        node's content hash.

        Args:
            tags_as_data: Maps a tag column of this node to the name of a data
                column that receives a copy of it. When given, the result is a
                new root source with its own provenance, which needs a
                ``source_id``; see ``DerivedSource``.
            source_id: The source's name. Required with ``tags_as_data``, which
                has no default (ITL-1313).

        Raises:
            ValueError: If ``tags_as_data`` is given without a ``source_id``.
            RuntimeError: If no database is attached.
            InputValidationError: If ``tags_as_data`` is not valid for this
                node's columns.
        """
        if self._pipeline_database is None:
            raise RuntimeError("Cannot create a DerivedSource without a database")

        from orcapod.core.sources.derived_source import DerivedSource

        # Without a source_id the plain form takes the DerivedSource default
        # (``derived_v1:<algorithm label>:<hex>`` of this node's content hash); the
        # ``tags_as_data`` form refuses to be built without one (ITL-1313).
        return DerivedSource(
            origin=self,
            source_id=source_id,
            tags_as_data=tags_as_data,
            data_context=self.data_context_key,
            config=self.orcapod_config,
        )

    # ------------------------------------------------------------------
    # Async channel execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        observer: "ExecutionObserverProtocol | None" = None,
        load_only: bool = False,
    ) -> None:
        """Async execution with cache mode handling when DB is attached.

        Every mode except REPLAY runs the wrapped operator's ``async_execute``,
        forwards its rows downstream and keeps them on the node, exactly as
        ``execute()`` does, so ``iter_data()`` and ``as_table()`` return them
        after the run whichever orchestrator ran the node.

        Without a database, or with one in OFF mode, nothing is stored.

        With a database:
            - REPLAY: emit from DB while reading and discarding every input, so
              the upstream nodes run as with the sync orchestrator; close output.
            - OFF: delegate to operator, forward and keep results.
            - LOG: delegate to operator, forward and keep results, then store
              them in the DB.

        Args:
            inputs: Sequence of readable channels from upstream nodes.
            output: Writable channel for output (tag, data) pairs.
            observer: Optional execution observer for hooks.
            load_only: As for ``execute``: a ``LOG`` node stores nothing (ITL-1095).

        Raises:
            ValueError: If the node caches (``LOG`` or ``REPLAY``) and a source
                feeding its ``upstreams`` (which the channels carry) is restricted
                (ITL-1218); the output is closed, nothing read or written.
        """
        from orcapod.pipeline.restrictions import refuse_restricted_operator_cache
        from orcapod.pipeline.observer import NoOpObserver

        # A loaded node runs only in a load-only run, with its rebuilt operator.
        if self._loaded and (not load_only or self._operator is None):
            await output.close()
            raise self._cannot_compute_error()
        try:
            refuse_restricted_operator_cache(self, self._cache_mode)
        except ValueError:
            await output.close()
            raise

        obs = observer if observer is not None else NoOpObserver()
        node_label = self.label
        node_hash = self.content_hash().to_string()
        ctx_obs = obs.contextualize(*self.node_identity_path)

        try:
            ctx_obs.on_node_start(node_label, node_hash)
            self._run_load_only = load_only

            if (
                self._pipeline_database is not None
                and self._cache_mode == CacheMode.REPLAY
            ):
                # Read and discard every input while the stored rows are sent: the
                # upstream nodes run over every row, as with the sync orchestrator,
                # and an input no one reads blocks its producer, and the run, once
                # its channel is full (ITL-1075).
                discards = [asyncio.create_task(_discard(ch)) for ch in inputs]
                try:
                    self._replay_from_cache()
                    assert self._cached_output_stream is not None
                    for tag, data in self._cached_output_stream.iter_data():
                        await output.send((tag, data))
                    _ = await asyncio.gather(*discards)
                    # The inputs are closed, so every upstream node has stamped itself:
                    # stamp after them, or the replayed rows would read as stale.
                    self._update_modified_time()
                finally:
                    for discard in discards:
                        _ = discard.cancel()
                    # Wait for the readers to end; a failure of the replay stays the
                    # error raised.
                    _ = await asyncio.gather(*discards, return_exceptions=True)
                ctx_obs.on_node_end(node_label, node_hash)
                return  # finally block closes output

            # Delegate to the operator, forward its rows downstream and keep them.
            intermediate: Channel[tuple[TagProtocol, DataProtocol]] = Channel()
            should_store = (
                not load_only
                and self._pipeline_database is not None
                and self._cache_mode == CacheMode.LOG
            )
            if should_store:
                # Rows are forwarded while the operator runs, so an input this node
                # will refuse to store must be refused before the first row leaves.
                for input_stream in self._input_streams:
                    require_same_data_context(
                        input_stream.data_context_key, self.data_context_key
                    )
            collected: list[tuple[TagProtocol, DataProtocol]] = []

            async def forward() -> None:
                async for item in intermediate.reader:
                    collected.append(item)
                    await output.send(item)

            hashes = [s.pipeline_hash() for s in self._input_streams]
            operator = cast(StaticOutputOperatorPod, self._operator)
            taps = [_InputTap(reader) for reader in inputs]
            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(
                        operator.async_execute(
                            taps,
                            intermediate.writer,
                            input_pipeline_hashes=hashes,
                        )
                    )
                    tg.create_task(forward())
            except ExceptionGroup as exc:
                # Preserve direct operator errors; concurrent failures stay grouped.
                if len(exc.exceptions) == 1:
                    raise exc.exceptions[0] from exc
                raise

            # TaskGroup has completed. Keep the output on the node, as execute()
            # does, and store it in LOG mode (sync DB write, post-hoc). A run
            # with no rows replaces an earlier run's output too: the node then
            # reads as a zero-row table.
            if collected:
                stream = StaticOutputOperatorPod._stream_over_row_table(
                    StaticOutputOperatorPod._rows_to_table(
                        collected, keep_token_fields=True
                    )
                )
                self._cached_output_stream = stream
                if should_store:
                    self._store_output_stream(stream)
            else:
                # A completed run with no rows keeps the operator's result over its
                # inputs rebuilt with no rows, exactly as the sync orchestrator
                # rebuilds them, so the node reads as computed and empty with the
                # sync path's columns and Arrow types (ITL-1080). An input that
                # delivered no row is its upstream's zero-row table, as on the sync
                # path (ITL-912). Nothing is stored for it.
                empty_result = operator.process(
                    *(
                        StaticOutputOperatorPod._materialize_upstream(
                            tap.rows, upstream, zero_rows=True
                        )
                        for tap, upstream in zip(taps, self._input_streams, strict=True)
                    )
                )
                # Computed, as execute() computes it, so the node reads as run, and
                # kept in the column order of a run with rows, as execute() keeps it
                # (ITL-1131).
                _ = list(empty_result.iter_data())
                self._cached_output_stream = (
                    StaticOutputOperatorPod._zero_rows_in_row_order(empty_result)
                )

            self._update_modified_time()

            ctx_obs.on_node_end(node_label, node_hash)
        except BaseException:
            # A run that raised or was cancelled defines no rows.
            self.clear_cache()
            raise
        finally:
            try:
                await output.close()
            except BaseException:
                # A close that raises or is cancelled fails the run: no rows.
                self.clear_cache()
                raise
