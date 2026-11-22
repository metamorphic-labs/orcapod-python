from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from collections.abc import Collection, Coroutine, Iterable, Iterator, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar, NoReturn, cast

import pyarrow as pa

from orcapod.protocols.core_protocols.streams import (
    RecordBatchInput,
    RecordBatchOutput,
)
from orcapod.channels import (
    ReadableChannel,
    WritableChannel,
)
from orcapod import contexts
from orcapod.config import OrcapodConfig
from orcapod.contexts import DataContext
from orcapod.core.base import TraceableBase
from orcapod.core.data_tags import (
    not_supported_yet,
    data_tag_names,
    stream_data_tags,
)
from orcapod.core.function_pod import FunctionPodStream
from orcapod.core.operators.provisional import (
    provisional_operator_name,
    warn_provisional_operator,
)
from orcapod.utils.provisional import note_provisional_use, provisional_class_name
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import (
    ZERO_ROW_INPUT_COLUMNS,
    StreamBase,
    check_head_size,
    short_stream_repr,
    typed_zero_row_input,
    typed_zero_row_table,
    row_built_columns,
)
from orcapod.core.streams.view import (
    STALE_NOTE,
    ViewStatus,
    StreamView,
    head_of,
)
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.protocols.core_protocols import (
    ArgumentGroup,
    DataProtocol,
    PodProtocol,
    StreamProtocol,
    TagProtocol,
    TrackerManagerProtocol,
)
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from orcapod.protocols.hashing_protocols import SemanticHasherProtocol


class RowBuiltFormUnavailable(ValueError):
    """F cannot be built from the whole table (``_row_built_form``).

    The one signal that a node's output must come from its rows instead: the caller
    builds the rows (``StaticOutputOperatorPod._rows_to_table``, through
    ``_raise_as_rows_do``), which raises the error a table built from rows raises, so
    the run fails as it does when rows are handed on, with the same observer events.
    """


class SourceInfoTypesDisagree(RowBuiltFormUnavailable):
    """Two rows give one source-info column different types (``_row_built_form``).

    A table built from such rows cannot be concatenated.
    """


class StaticOutputOperatorPod(TraceableBase):
    """Abstract base class for pods whose core logic yields a static output stream.

    The static output stream is wrapped in ``DynamicPodStream`` which re-executes
    the pod as necessary to keep the output up-to-date.  Pod invocations are
    tracked by the tracker manager.
    """

    # The Linear issue that will make this operator handle data-tags, or
    # ``None`` when it handles them (ITL-1023). An operator that declares nothing
    # does not handle them yet: it raises instead of giving a wrong answer.
    DATA_TAGS_ISSUE: ClassVar[str | None] = "ITL-1022"

    ROW_BY_ROW: ClassVar[bool] = False
    """Whether each output row comes from one input row, whatever the other rows are.

    Declared by the operators that select, rename or add columns, narrow a column or
    leave rows out by their own values (the column selections and mappers,
    ``AddTags``, ``DropNulls``, ``Index``, ``Pick``). Such an operator can be applied
    to each Arrow record batch of its input as it arrives
    (``async_execute_batches``); every other operator needs all of an input first.
    """

    OUTPUT_SCHEMA_IS_INPUT_SCHEMA: ClassVar[bool] = False
    """Whether the output has exactly its single input's columns and Arrow types.

    Declared by an operator that only chooses which rows exist (``PolarsFilter``). Its
    zero-row table is then its input's (``zero_row_result``), got without evaluating a
    predicate, so no user code runs to learn the column types (ITL-912).
    """

    PROVISIONAL: ClassVar[bool] = False
    """Whether the operator is provisional: its identity is not frozen (ITL-1266).

    Set to ``True`` exactly by the operators listed in ``PROVISIONAL_OPERATORS``
    (``core/operators/provisional.py``); constructing one issues
    ``ProvisionalHashWarning`` once per operator per process.
    """

    def __init__(
        self, tracker_manager: TrackerManagerProtocol | None = None, **kwargs
    ) -> None:
        provisional = provisional_operator_name(type(self))
        if provisional is not None:
            warn_provisional_operator(provisional)
        self.tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        super().__init__(**kwargs)

    def _note_provisional_identity_use(self) -> None:
        """Note a use of this operator when it is provisional; never warn.

        Called where the operator's identity is read (``content_hash``,
        ``pipeline_hash``, ``identity_structure`` of a provisional operator) and where it
        is applied to streams (``process``, ``static_process``, a ``DynamicPodStream``'s
        hashes, identity structures, reads and ``head``), so an operator
        built before a recording (a module-level ``Batch(2)``) is still seen by it
        (ITL-1279). The same note as ``RootSource._note_provisional_identity_use`` for a
        provisional source (ITL-1314). The warning stays at construction. The note
        enters no hash, config or path.
        """
        provisional = provisional_class_name(type(self))
        if provisional is not None:
            note_provisional_use(provisional)

    def content_hash(self, hasher=None) -> ContentHash:
        """The operator's content hash; notes the use of a provisional operator.

        Args:
            hasher: Optional semantic hasher, as for ``ContentIdentifiableBase``.

        Returns:
            The content hash, unchanged by the note.
        """
        self._note_provisional_identity_use()
        return super().content_hash(hasher)

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """The operator's pipeline hash; notes the use of a provisional operator.

        Args:
            hasher: Optional semantic hasher, as for ``PipelineElementBase``.

        Returns:
            The pipeline hash, unchanged by the note.
        """
        self._note_provisional_identity_use()
        return super().pipeline_hash(hasher)

    def check_data_tag_support(self, *streams: StreamProtocol) -> None:
        """Raise if a data-tag reaches an operator that does not handle it yet.

        Args:
            *streams: The input streams.

        Raises:
            NotImplementedError: If ``DATA_TAGS_ISSUE`` is set and an input
                has a data-tag.
        """
        if self.DATA_TAGS_ISSUE is None:
            return
        if any(stream_data_tags(stream) for stream in streams):
            raise not_supported_yet(type(self).__name__, self.DATA_TAGS_ISSUE)

    def pipeline_identity_structure(self) -> Any:
        """Return the pipeline identity, which defaults to content identity for operators."""
        return self.identity_structure()

    @property
    def uri(self) -> tuple[str, ...]:
        """Return a unique resource identifier for the pod."""
        return (
            f"{self.__class__.__name__}",
            self.content_hash().to_hex(),
        )

    @abstractmethod
    def validate_inputs(self, *streams: StreamProtocol) -> None:
        """Validate input streams, raising exceptions if invalid.

        Args:
            *streams: Input streams to validate.

        Raises:
            PodInputValidationError: If inputs are invalid.
        """
        ...

    @abstractmethod
    def argument_symmetry(self, streams: Collection[StreamProtocol]) -> ArgumentGroup:
        """Describe symmetry/ordering constraints on input arguments.

        Returns a structure encoding which arguments can be reordered:
        - ``frozenset``: Arguments commute (order doesn't matter).
        - ``tuple``: Arguments have fixed positions.
        - Nesting expresses partial symmetry.

        Examples:
            Full symmetry (Join)::

                return frozenset([a, b, c])

            No symmetry (Concatenate)::

                return (a, b, c)

            Partial symmetry::

                return (frozenset([a, b]), c)
        """
        ...

    @abstractmethod
    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Determine output (tag, data) schemas without triggering computation.

        Args:
            *streams: Input streams to analyze.
            columns: Column configuration for included column groups.
            all_info: If True, include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.

        Raises:
            ValidationError: If input types are incompatible.
        """
        ...

    @abstractmethod
    def static_process(self, *streams: StreamProtocol) -> StreamProtocol:
        """Execute the pod on the input streams and return a static output stream.

        Args:
            *streams: Input streams to process.

        Returns:
            The resulting output stream.
        """
        ...

    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> DynamicPodStream:
        """Invoke the pod on input streams and return a ``DynamicPodStream``.

        Args:
            *streams: Input streams to process.
            label: Optional label for tracking.

        Returns:
            A ``DynamicPodStream`` wrapping the computation.
        """
        logger.debug("Invoking kernel %s on streams: %s", self, streams)
        self._note_provisional_identity_use()

        # perform input stream validation
        self.check_data_tag_support(*streams)
        self.validate_inputs(*streams)
        self.tracker_manager.record_operator_pod_invocation(
            self, upstreams=streams, label=label
        )
        output_stream = DynamicPodStream(
            pod=self,
            upstreams=streams,
            label=label,
        )
        return output_stream

    def to_config(self) -> dict[str, Any]:
        """Serialize this operator to a JSON-compatible config dict.

        Subclasses with constructor parameters should override this to include
        their specific config in the ``"config"`` key.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys.
        """
        return {
            "class_name": self.__class__.__name__,
            "module_path": self.__class__.__module__,
            "config": {},
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "StaticOutputOperatorPod":
        """Reconstruct an operator from a config dict.

        Args:
            config: Dict as returned by ``to_config()``.

        Returns:
            A new instance of this operator class.
        """
        return cls(**config.get("config", {}))

    def __call__(self, *streams: StreamProtocol, **kwargs) -> DynamicPodStream:
        """Convenience alias for ``process``."""
        logger.debug("Invoking pod %s on streams through __call__: %s", self, streams)
        # perform input stream validation
        return self.process(*streams, **kwargs)

    # ------------------------------------------------------------------
    # Async channel execution (default barrier mode)
    # ------------------------------------------------------------------

    @staticmethod
    def _rows_to_table(
        rows: Sequence[tuple[TagProtocol, DataProtocol]],
    ) -> tuple[pa.Table, tuple[str, ...], tuple[str, ...]]:
        """Concatenate (Tag, Data) pairs into one table, keeping per-row source info.

        Each row contributes its own system-tag and ``_source_*`` columns, so
        every row keeps its own provenance tokens (a token embeds a per-row
        identifier, so rows never share one). A ``_source_*`` field is declared
        non-nullable unless some row's token is ``None``; ``Data.as_table``
        declares every source-info field nullable, and this keeps the field
        flags (which the operator node's per-row record hash covers) what they
        were when a broadcast token made them non-nullable.

        Args:
            rows: Non-empty sequence of (Tag, Data) pairs.

        A data-tag (a name in both the tag and the data of a row) is one
        column: it is taken from the tags, and its source info from the data.

        Returns:
            The concatenated table, the user tag column names and the data-tags.

        Raises:
            ValueError: If ``rows`` is empty.
        """
        from orcapod.utils import arrow_utils

        if not rows:
            raise ValueError("Cannot materialize an empty list of rows into a stream")

        first_tag, first_data = rows[0]
        data_tags = data_tag_names(first_tag.keys(), first_data.keys())
        tag_tables = [tag.as_table(columns={"system_tags": True}) for tag, _ in rows]
        data_tables = [data.as_table(columns={"source": True}) for _, data in rows]
        if data_tags:
            data_tables = [t.drop_columns(list(data_tags)) for t in data_tables]

        # Rows may list the same columns in a different order (cached vs
        # freshly computed), so match columns by name.
        combined_tags = arrow_utils.concat_tables_by_name(tag_tables)
        combined_data = arrow_utils.concat_tables_by_name(data_tables)

        combined_data = StaticOutputOperatorPod._flag_source_info_nullability(
            combined_data
        )

        full_table = arrow_utils.hstack_tables(combined_tags, combined_data)
        return full_table, tuple(first_tag.keys()), data_tags

    @staticmethod
    def _in_row_order(table: pa.Table, tag_keys: Sequence[str]) -> pa.Table:
        """Return ``table`` with its columns in the order of a table built from rows.

        The one statement of that order (ITL-1131): user tags, system tags, data,
        source info, as ``_rows_to_table`` builds it (each row's tag table with its
        system tags, then its data table with its source info). Data columns keep their
        order in ``table``. Every table a node or an operator input holds with no rows
        takes this order, so it equals the table a run with rows gives.

        Args:
            table: A table of user tags, system tags, data and source info.
            tag_keys: The user tag columns, in order.

        Returns:
            The same columns, reordered; no column is added, dropped or changed.
        """
        from orcapod.system_constants import constants

        names = table.column_names
        system_tags = [n for n in names if n.startswith(constants.SYSTEM_TAG_PREFIX)]
        source_info = [n for n in names if n.startswith(constants.SOURCE_PREFIX)]
        placed = {*tag_keys, *system_tags, *source_info}
        data = [n for n in names if n not in placed]
        return table.select([*tag_keys, *system_tags, *data, *source_info])

    @staticmethod
    def _as_row_built_table(table: pa.Table, tag_keys: Sequence[str]) -> pa.Table:
        """Return ``table`` as ``_rows_to_table`` would build it from the same rows.

        Its columns in row order (``_in_row_order``) and each source-info field flagged
        nullable only when some row's token is ``None``
        (``_flag_source_info_nullability``). For a table with no rows: what a result
        built from rows has, with no row (ITL-1131).

        Args:
            table: A table of user tags, system tags, data and source info.
            tag_keys: The user tag columns, in order.

        Returns:
            The same arrays, reordered, under the new source-info field flags.
        """
        return StaticOutputOperatorPod._flag_source_info_nullability(
            StaticOutputOperatorPod._in_row_order(table, tag_keys)
        )

    @staticmethod
    def _zero_rows_in_row_order(result: StreamProtocol) -> StreamProtocol:
        """Return an operator's result with no rows as a run with rows would keep it.

        The zero-row counterpart of ``_materialize_to_stream``: ``result``'s zero-row
        table (user tags, system tags, data and source info, with its Arrow types and
        nullability) in the order of a table built from rows (``_in_row_order``). An
        operator's own table may order its columns otherwise (a join lists its data
        before its system tags), so a node that keeps the result of a run with no rows
        keeps this, and reads with the columns of a run with rows (ITL-1131).

        Args:
            result: An operator's result that has no rows.

        Returns:
            A stream over the reordered zero-row table, in ``result``'s data context.
        """
        tag_keys = result.keys()[0]
        table = StaticOutputOperatorPod._in_row_order(
            result.as_table(columns=ZERO_ROW_INPUT_COLUMNS), tag_keys
        )
        return ArrowTableStream(
            table,
            tag_columns=tag_keys,
            data_tags=stream_data_tags(result),
            data_context=result.data_context_key,
        )

    @staticmethod
    def _kept_output(
        result: StreamProtocol,
        rows: Sequence[tuple[TagProtocol, DataProtocol]],
    ) -> StreamProtocol:
        """Return what an operator node keeps, and stores, of a run of its operator.

        The rows of the run as ``_materialize_to_stream`` builds them, or, with no row,
        the result with no rows in the same order (``_zero_rows_in_row_order``). Every
        way a node runs its operator (a job run, sync or async, and ``node.run()`` on
        its own) keeps this, so a node has one column order, the row order, in memory
        and in its stored table (ITL-1133).

        Args:
            result: The operator's result of the run.
            rows: ``result``'s rows, in order.

        Returns:
            A stream over the rows, in row order.
        """
        if rows:
            return StaticOutputOperatorPod._materialize_to_stream(rows)
        return StaticOutputOperatorPod._zero_rows_in_row_order(result)

    @staticmethod
    def _flag_by_values(table: pa.Table) -> pa.Table:
        """Flag each system-tag and source-info field nullable only when it holds a null.

        The last step of F (``_row_built_form``), which needs the complete table. A table
        built from rows takes its system tags and source-info tokens from each row's
        values, so those fields are non-nullable unless a value is missing, whatever
        flags the table the rows came from had (a join over inputs tied on pipeline hash
        declares its system tags nullable). The table built from rows also carries no
        schema metadata (``arrow_utils.hstack_tables``), so none is kept.

        Args:
            table: A table of user tags, system tags, data and source info.

        Returns:
            The same arrays under the new field flags, with no schema metadata.
        """
        from orcapod.system_constants import constants

        prefixes = (constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX)
        fields = [
            field.with_nullable(table.column(i).null_count > 0)
            if field.name.startswith(prefixes)
            else field
            for i, field in enumerate(table.schema)
        ]
        # Same arrays, new field flags: no cast, so no value or type can change.
        return pa.Table.from_arrays(table.columns, schema=pa.schema(fields))

    @staticmethod
    def _type_source_info_by_values(table: pa.Table) -> pa.Table:
        """Type each source-info column from its tokens, as the rows of a table built from rows do.

        A step of F (``_row_built_form``) on the complete table, beside
        ``_flag_by_values``. A row's ``Data`` types each source-info token from its
        value (``arrow_utils.source_info_arrow_type``, the one rule): a string or
        ``None`` as ``large_string``, a list as ``large_list`` of its first informative
        element's type, so a list of ``None`` or of empty lists shows fewer levels than
        its column may declare. The rows' tables are then concatenated, which needs
        every row to give a column one type. This step gives each column that type,
        built from the same values. A ``large_string`` column is every row's type
        already and is left as it is.

        Args:
            table: A table of user tags, system tags, data and source info, after the
                per-slice steps (``streams.base.row_built_columns``).

        Returns:
            The table with each source-info column of the type its tokens give.

        Raises:
            SourceInfoTypesDisagree: If two rows give one source-info column different
                types, where the rows' tables cannot be concatenated. The caller builds
                the rows (``_rows_to_table``), which raises the error a table built from
                rows raises.
        """
        from orcapod.system_constants import constants
        from orcapod.utils import arrow_utils

        columns = list(table.columns)
        fields = list(table.schema)
        changed = False
        for index, field in enumerate(fields):
            if not field.name.startswith(constants.SOURCE_PREFIX):
                continue
            if pa.types.is_large_string(field.type):
                continue
            values = table.column(index).to_pylist()
            kinds = {arrow_utils.source_info_arrow_type(value) for value in values}
            if len(kinds) != 1:
                raise SourceInfoTypesDisagree(field.name)
            (kind,) = kinds
            if kind == field.type:
                continue
            columns[index] = pa.chunked_array([pa.array(values, type=kind)])
            fields[index] = pa.field(field.name, kind, nullable=field.nullable)
            changed = True
        if not changed:
            return table
        return pa.Table.from_arrays(
            columns,
            schema=pa.schema(
                fields,
                cast("dict[bytes | str, bytes | str] | None", table.schema.metadata),
            ),
        )

    @staticmethod
    def _row_built_form(
        table: pa.Table, keys: tuple[Sequence[str], Sequence[str]]
    ) -> pa.Table:
        """Return ``table`` as ``_rows_to_table`` builds it from the table's own rows.

        F in the ITL-1347 spec: the same table, without making the rows. Two steps:

        - per slice (``streams.base.row_built_columns``), what each row decides on its
          own: the columns, their order, their large types, and the provenance fields'
          form (no system-tag field metadata, source-info lists of nullable items);
        - on the complete table, what depends on the values of every row: each
          source-info column's type, from its tokens (``_type_source_info_by_values``),
          then the system-tag and source-info field flags (``_flag_by_values``).

        The equality with ``_rows_to_table`` is tested over every operator, source kind,
        column role and raw-stream provenance shape (ITL-1347, L0-L2).

        Args:
            table: A table of user tags, system tags, data and source info, with rows.
            keys: The tag keys and the data keys of the stream ``table`` belongs to
                (``stream.keys()``).

        Returns:
            The table ``_rows_to_table`` would build from its rows.

        Raises:
            RowBuiltFormUnavailable: If the whole table cannot be built: the rows'
                tokens give a source-info column two types
                (``SourceInfoTypesDisagree``), or an Arrow conversion fails. The node
                then builds its rows (``_raise_as_rows_do``).
        """
        try:
            return StaticOutputOperatorPod._flag_by_values(
                StaticOutputOperatorPod._type_source_info_by_values(
                    row_built_columns(table, keys)
                )
            )
        except RowBuiltFormUnavailable:
            raise
        except pa.ArrowException as error:
            # Values the whole table cannot be built from (a token the rows' own
            # conversion refuses too): the rows decide, as on ``main``.
            raise RowBuiltFormUnavailable(str(error)) from error

    @staticmethod
    def _raise_as_rows_do(
        rows: Iterable[tuple[TagProtocol, DataProtocol]],
        error: RowBuiltFormUnavailable,
    ) -> NoReturn:
        """Build ``rows`` into a table to raise the error a table built from them raises.

        Called when ``_row_built_form`` finds rows whose source-info tokens cannot make
        one table (``RowBuiltFormUnavailable``): the rows' own concatenation
        (``_rows_to_table``) raises exactly what a run that hands rows on raises.

        Args:
            rows: The rows of the table F was given, in order.
            error: The disagreement F found, raised if the rows do concatenate.

        Raises:
            pyarrow.ArrowInvalid: As ``_rows_to_table`` raises it.
            RowBuiltFormUnavailable: ``error``, if the rows build after all.
        """
        StaticOutputOperatorPod._rows_to_table(list(rows))
        raise error

    @staticmethod
    def _output_stream(
        table: pa.Table,
        keys: tuple[Sequence[str], Sequence[str]],
        node: StreamProtocol,
    ) -> StreamProtocol:
        """Return the stream a node hands its successors, given the table of its rows.

        Exactly what ``_materialize_upstream`` builds from the node's rows: with rows,
        ``_row_built_form`` of ``table`` as ``node``'s stream (``_as_upstream_stream``);
        without, ``node``'s zero-row input. A node's ``execute`` returns it, and the sync
        orchestrator gives it to every successor as it is (ITL-1347, stage 2).

        Args:
            table: The node's rows: user tags, system tags, data and source info.
            keys: The tag keys and the data keys of the stream the rows come from.
            node: The node (or stream) whose output this is.

        Returns:
            The output stream, with ``node``'s identity.
        """
        if table.num_rows == 0:
            return StaticOutputOperatorPod._materialize_upstream([], node)
        tag_keys, data_keys = keys
        return StaticOutputOperatorPod._as_upstream_stream(
            StaticOutputOperatorPod._row_built_form(table, keys),
            tag_keys,
            data_tag_names(tag_keys, data_keys),
            node,
        )

    @staticmethod
    def _flag_source_info_nullability(table: pa.Table) -> pa.Table:
        """Declare each ``_source_*`` field nullable only if some row's token is ``None``.

        The rule ``_rows_to_table`` applies to the rows it concatenates, and that a
        zero-row input (``_zero_row_input``) applies to no row: there, no token is
        ``None``, so every source-info field is non-nullable.

        Args:
            table: A table whose ``_source_*`` columns hold source-info tokens.

        Returns:
            The same arrays under the new field flags.
        """
        from orcapod.system_constants import constants

        schema = table.schema
        for i, field in enumerate(schema):
            if field.name.startswith(constants.SOURCE_PREFIX):
                nullable = table.column(i).null_count > 0
                schema = schema.set(i, field.with_nullable(nullable))  # pyright: ignore[reportUnknownMemberType]  # stubs leave Field unparametrized
        # Same arrays, new field flags: no cast, so no value or type can change.
        return pa.Table.from_arrays(table.columns, schema=schema)

    @staticmethod
    def _zero_row_input(upstream: StreamProtocol) -> StreamProtocol | None:
        """Return ``upstream`` as an operator input with no rows, typed as its rows are.

        The zero-row form of what ``_materialize_upstream`` builds from an upstream's
        rows: the upstream's zero-row table (``typed_zero_row_table``: user tags,
        system tags, data and source info, with the Arrow types its rows have) in the
        column order and with the source-info flags ``_rows_to_table`` gives, as the
        upstream's stream (``_as_upstream_stream``), so the stream's identity,
        pipeline hash and system-tag column names are the upstream's (ITL-912).

        Args:
            upstream: The node or stream an operator reads.

        Returns:
            The zero-row stream, or ``None`` when ``upstream`` cannot say its Arrow
            types without rows; the caller then builds its input from the Python
            output schema.
        """
        table = typed_zero_row_input(upstream)
        if table is None:
            return None
        tag_keys = upstream.keys()[0]
        return StaticOutputOperatorPod._as_upstream_stream(
            StaticOutputOperatorPod._as_row_built_table(table, tag_keys),
            tag_keys,
            stream_data_tags(upstream),
            upstream,
        )

    def zero_row_result(self, *inputs: StreamProtocol) -> StreamProtocol:
        """This operator's result over zero-row inputs, for its zero-row table (ITL-912).

        ``static_process`` over the inputs, or, for an operator whose output schema is
        its input's (``OUTPUT_SCHEMA_IS_INPUT_SCHEMA``), the input's columns in the
        large-normalized form a run gives them, so no predicate or other user code is
        evaluated. A run never comes here: it calls
        ``static_process`` (or ``process``) whether its inputs have rows or not.

        Args:
            inputs: The inputs as zero-row streams.

        Returns:
            A stream with no rows and the columns and Arrow types of the result.
        """
        if self.OUTPUT_SCHEMA_IS_INPUT_SCHEMA:
            from orcapod.utils import arrow_utils

            (only,) = inputs
            # The input's columns, each in the large-normalized form a run gives it.
            schema = only.as_table(columns=ZERO_ROW_INPUT_COLUMNS).schema
            normalized = pa.schema(
                [
                    field.with_type(
                        arrow_utils.normalize_to_large_types(field.type, keep_null=True)
                    )
                    for field in schema
                ]
            )
            return ArrowTableStream(
                normalized.empty_table(),
                tag_columns=only.keys()[0],
                data_tags=stream_data_tags(only),
                data_context=only.data_context_key,
            )
        return self.static_process(*inputs)

    @staticmethod
    def _materialize_to_stream(
        rows: Sequence[tuple[TagProtocol, DataProtocol]],
    ) -> StreamProtocol:
        """Materialize a list of (Tag, Data) pairs into an ArrowTableStream.

        Holds an operator's or a node's OUTPUT rows, whose stream identity is
        given by whoever holds them (the node, the ``DynamicPodStream``), so the
        stream has no producer and its own pipeline hash is never read. Rows
        that stand for an operator's INPUT go through
        ``_materialize_input_stream``. Every row keeps its own source info (see
        ``_rows_to_table``).

        Args:
            rows: Non-empty sequence of (Tag, Data) pairs.

        Returns:
            A stream over the rows, in order.

        Raises:
            ValueError: If ``rows`` is empty.
        """
        return StaticOutputOperatorPod._stream_over_row_table(
            StaticOutputOperatorPod._rows_to_table(rows)
        )

    @staticmethod
    def _stream_over_row_table(
        row_table: tuple[pa.Table, tuple[str, ...], tuple[str, ...]],
    ) -> StreamProtocol:
        """The stream ``_materialize_to_stream`` builds, over ``_rows_to_table``'s result.

        Args:
            row_table: The table, user tag keys and data-tags ``_rows_to_table`` returns.

        Returns:
            A stream over the table's rows, in order.
        """
        table, tag_keys, data_tags = row_table
        # No ``source_info`` argument: the table's own ``_source_*`` columns
        # pass through, one value per row.
        return ArrowTableStream(table, tag_columns=tag_keys, data_tags=data_tags)

    @staticmethod
    def _materialize_upstream(
        rows: Sequence[tuple[TagProtocol, DataProtocol]],
        upstream: StreamProtocol,
        *,
        zero_rows: bool = False,
    ) -> StreamProtocol:
        """Rebuild the rows an upstream node delivered as that node's stream.

        This is how an operator node's input is built in a job: the sync
        orchestrator rebuilds every upstream's buffered rows with it, and an
        async operator node rebuilds its inputs with it when its run produced
        no row (``zero_rows``). The stream takes the upstream's producer and
        upstreams, so its identity and pipeline hash are the upstream's and
        system-tag column names come out as they would on the upstream itself.

        With rows, the table is ``_rows_to_table(rows)``: every row keeps its
        own system tags and source info. Without rows, it is the upstream's
        zero-row table (``_zero_row_input``), with the Arrow types its rows have
        (ITL-912). Only an upstream that cannot say its Arrow types without rows
        (see ``typed_zero_row_table``) is built from its Python output schema
        (with system tags and source info), a known limitation (ITL-1089).

        Args:
            rows: The rows the upstream delivered; may be empty.
            upstream: The upstream node or stream the rows came from.
            zero_rows: Keep the table's columns and types but none of its rows.

        Returns:
            A stream over the rows (none when ``zero_rows``), in order.
        """
        from orcapod.utils import arrow_utils

        if rows:
            table, tag_keys, data_tags = StaticOutputOperatorPod._rows_to_table(rows)
            if zero_rows:
                table = table.slice(0, 0)
            return StaticOutputOperatorPod._as_upstream_stream(
                table, tag_keys, data_tags, upstream
            )
        typed = StaticOutputOperatorPod._zero_row_input(upstream)
        if typed is not None:
            # A complete output with no row, in the row-built form (``ROW_BUILT_OUTPUT``).
            setattr(typed, ROW_BUILT_OUTPUT, True)
            return typed
        # Build an empty table preserving declared field nullability.
        tag_schema, data_schema = upstream.output_schema(
            columns={"system_tags": True, "source": True}
        )
        table = arrow_utils.make_empty_table(
            {**tag_schema, **data_schema},
            StaticOutputOperatorPod._upstream_context(upstream).type_converter,
        )
        return StaticOutputOperatorPod._as_upstream_stream(
            table, upstream.keys()[0], stream_data_tags(upstream), upstream
        )

    @staticmethod
    def _upstream_context(upstream: StreamProtocol) -> DataContext:
        """The data context of ``upstream``, which its rebuilt stream is in."""
        from orcapod.core.base import DataContextMixin

        if isinstance(upstream, DataContextMixin):
            return upstream.data_context
        return contexts.resolve_context(upstream.data_context_key)

    @staticmethod
    def _as_upstream_stream(
        table: pa.Table,
        tag_keys: Sequence[str],
        data_tags: Sequence[str],
        upstream: StreamProtocol,
    ) -> StreamProtocol:
        """Wrap a table of ``upstream``'s rows (or none) as ``upstream``'s stream.

        Args:
            table: User tags, system tags, data and source info, without the
                context column.
            tag_keys: The user tag columns.
            data_tags: The data-tags among ``tag_keys`` (ITL-1023).
            upstream: The node or stream the table stands for.

        Returns:
            A stream with ``upstream``'s producer, upstreams and data context.
        """
        from orcapod.system_constants import constants

        # The rebuilt stream is in the upstream's data context, as its rows are.
        context = StaticOutputOperatorPod._upstream_context(upstream)
        # The context column names the upstream's context on every row; without
        # it, the stream would fill in the default context's key.
        table = table.append_column(
            pa.field(constants.CONTEXT_KEY, pa.large_string(), nullable=False),
            pa.array([context.context_key] * table.num_rows, pa.large_string()),
        )
        if upstream.producer is None:
            # A node with no pod (a loaded function node, a restored side-effect
            # node, ITL-1094) cannot give the stream a producer to hash, so the
            # stream reports the node's pipeline hash, as the async path does;
            # otherwise it would fall back to a root's schema-only hash and an
            # operator would name its system tags after that. A source's pipeline
            # hash is that schema-only hash already.
            stream: StreamProtocol = _RowsOfStream(
                table,
                tag_columns=tag_keys,
                pipeline_hash=upstream.pipeline_hash(),
                data_tags=data_tags,
                upstreams=upstream.upstreams,
                data_context=context,
            )
        else:
            # No ``source_info`` argument: the table's own ``_source_*`` columns
            # pass through, one value per row.
            stream = ArrowTableStream(
                table,
                tag_columns=tag_keys,
                data_tags=data_tags,
                producer=upstream.producer,
                upstreams=upstream.upstreams,
                data_context=context,
            )
        # Every complete node output in the row-built form ends here (the rows'
        # rebuild, F's output, a stored-table output) or, with no row, in
        # ``_materialize_upstream``'s zero-row input: the places that mark it
        # (ITL-1347): ``channel_to_stream`` may give it as it is.
        setattr(stream, ROW_BUILT_OUTPUT, True)
        return stream

    @staticmethod
    def _materialize_input_stream(
        rows: Sequence[tuple[TagProtocol, DataProtocol]],
        pipeline_hash: ContentHash,
    ) -> StreamProtocol:
        """Rebuild an input stream from the rows a barrier ``async_execute`` collected.

        The rebuilt stream reports ``pipeline_hash`` as its own, so an operator
        that names system-tag columns after its input's pipeline hash
        (``MergeJoin``, ``Batch``, ``GroupBy``) names them as the sync path
        does (ITL-906). Every row keeps its own source info (see
        ``_rows_to_table``) and its own data context, taken from its datagrams; the
        stream is in the first row's data context (without them, the stream would
        fill in the default context's key).

        Args:
            rows: Non-empty sequence of (Tag, Data) pairs.
            pipeline_hash: The pipeline hash of the stream the rows came from.

        Returns:
            A stream over the rows, in order.

        Raises:
            ValueError: If ``rows`` is empty.
        """
        from orcapod.system_constants import constants

        table, tag_keys, data_tags = StaticOutputOperatorPod._rows_to_table(rows)
        table = table.append_column(
            pa.field(constants.CONTEXT_KEY, pa.large_string(), nullable=False),
            pa.array([data.data_context_key for _, data in rows], pa.large_string()),
        )
        return _RowsOfStream(
            table,
            tag_columns=tag_keys,
            pipeline_hash=pipeline_hash,
            data_tags=data_tags,
            data_context=contexts.resolve_context(rows[0][1].data_context_key),
        )

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Default barrier-mode async execution.

        Collects all inputs, runs ``static_process``, emits results.
        Subclasses override for streaming or incremental strategies. When an
        input has no rows, nothing is emitted (ITL-865): an operator whose
        result is not empty then must override this method.

        Every operator's ``async_execute`` takes ``input_pipeline_hashes``, with
        no default: the operator node passes it for every operator alike.

        Args:
            inputs: Readable channels, one per upstream node.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: Pipeline hash for each input stream,
                positionally matching ``inputs``. Each rebuilt input stream
                reports its hash, so system-tag column names equal the sync
                path's (ITL-906).

        Raises:
            ValueError: If ``input_pipeline_hashes`` does not have one hash per input.
        """
        try:
            if len(input_pipeline_hashes) != len(inputs):
                raise ValueError(
                    f"input_pipeline_hashes length ({len(input_pipeline_hashes)}) "
                    f"must match inputs length ({len(inputs)})"
                )
            all_rows = await asyncio.gather(*(ch.collect() for ch in inputs))
            if not all(all_rows):
                # Every barrier operator that exists gives no rows when an input has
                # none. The typed empty stream the sync path returns needs a schema
                # the channels do not carry (ITL-858), so nothing is emitted (ITL-865).
                return
            streams = [
                self._materialize_input_stream(rows, pipeline_hash)
                for rows, pipeline_hash in zip(all_rows, input_pipeline_hashes)
            ]
            result = self.static_process(*streams)
            for tag, data in result.iter_data():
                await output.send((tag, data))
        finally:
            await output.close()

    # ------------------------------------------------------------------
    # Async execution on Arrow record batches (ITL-1347)
    # ------------------------------------------------------------------

    async def async_execute_batches(
        self, inputs: Sequence[RecordBatchInput], output: RecordBatchOutput
    ) -> None:
        """Run this operator over input channels of Arrow record batches.

        The Arrow-record-batch form of ``async_execute`` (ITL-1347, stage 5); the
        nodes and the orchestrator still call the row form until they are wired to
        this one, which then takes its name. The operator has one body for both
        paths, ``static_process``: each input's description is its channel's stream
        (identity, pipeline hash, data context, keys), as on the sync path.

        A row-by-row operator (``ROW_BY_ROW``) is applied to each record batch as it
        arrives and sends that batch's result at once. Every other operator reads
        all of its inputs first (``channel_to_stream``) and then runs exactly
        the sync call, so an input with no rows gives the typed zero-row result of
        the sync path, with no row to send.

        The result is held to what a job's sync path gives: the operator over each
        upstream rebuilt from its rows (``_materialize_upstream``: ``null`` as
        ``large_string``, the columns in row order), not over the raw stream the rows
        came from. A predicate that reads a ``null`` column's type or the column order
        follows that form (documented behaviour, e5).

        The output is closed when the operator is done. If it fails or is cancelled,
        the output fails instead, so no reader takes it for complete; every input is
        closed in either case.

        Args:
            inputs: One input channel per input stream, in the operator's order.
            output: The channel the result's record batches are sent into.
        """
        try:
            if self.ROW_BY_ROW:
                (channel,) = inputs
                async for batch in channel:
                    await self._send_processed([batch], channel.stream, output)
            else:
                streams = await _streams_of(inputs)
                await self._send_result(self.static_process(*streams), output)
            await output.close()
        except BaseException as error:
            output.fail(error)
            await close_inputs(inputs, error)
            raise
        await close_inputs(inputs)

    async def _send_processed(
        self,
        batches: Sequence[pa.RecordBatch],
        stream: StreamProtocol,
        output: RecordBatchOutput,
    ) -> None:
        """Apply this operator to some record batches of one input and send the result.

        The batches are wrapped as a stream with the input's identity and given to
        ``static_process``, the sync body.

        Args:
            batches: Record batches of the input's rows, in order.
            stream: The input's describing stream.
            output: The channel to send the result into.
        """
        await self._send_result(
            self.static_process(stream_from_batches(stream, batches)), output
        )

    @staticmethod
    async def _send_result(result: StreamProtocol, output: RecordBatchOutput) -> None:
        """Send a result stream's rows into ``output`` as its own record batches.

        Args:
            result: An operator's result.
            output: The channel to send into; a result with no rows sends nothing.
        """
        table = row_built_columns(
            result.as_table(columns=ZERO_ROW_INPUT_COLUMNS), result.keys()
        )
        for batch in table.to_batches():
            if batch.num_rows:
                await output.send(batch)


async def close_inputs(
    inputs: Sequence[RecordBatchInput], error: BaseException | None = None
) -> None:
    """Close every input channel of an operator, also when one close fails.

    Each input is closed on its own, so a failing close never leaves another input's
    producer waiting. When the operator already failed (``error``), a failing close is
    added to that error as a note and the operator's error stays the one raised;
    otherwise the first failing close is raised once every input is closed.

    Args:
        inputs: The operator's input channels.
        error: The error the operator is failing with, if any.

    Raises:
        Exception: The first error of a close, when ``error`` is ``None``.
    """
    first: Exception | None = None
    for channel in inputs:
        try:
            await channel.close()
        except Exception as failure:  # noqa: BLE001  # every input is closed first
            if error is not None:
                error.add_note(f"Closing an input channel also failed: {failure!r}")
            elif first is None:
                first = failure
    if first is not None:
        raise first


async def _streams_of(inputs: Sequence[RecordBatchInput]) -> list[StreamProtocol]:
    """Read every input channel to its end, each as a stream, at the same time.

    Args:
        inputs: The input channels.

    Returns:
        One complete stream per input, in order.

    Raises:
        UpstreamFailedError: If an input's producer failed; the other inputs are no
            longer read.
    """
    tasks: list[asyncio.Future[StreamProtocol]] = []
    try:
        for channel in inputs:
            tasks.append(asyncio.ensure_future(channel_to_stream(channel)))
        return list(await asyncio.gather(*tasks))
    except BaseException:
        # Every conversion already started is cancelled and awaited: none is left
        # running, or raising with nobody to hear it.
        for task in tasks:
            _ = task.cancel()
        _ = await asyncio.gather(*tasks, return_exceptions=True)
        raise


# ---------------------------------------------------------------------------
# Conversions between streams and Arrow record batches (ITL-1347)
# ---------------------------------------------------------------------------


def transport_schema(stream: StreamProtocol) -> pa.Schema | None:
    """Return the schema ``stream``'s record batches have, from its description.

    Never computes, reads or polls a row: the stream's zero-row input with the columns
    of F in F's order (``row_built_names``) and its provenance fields as a row builds
    them (``row_built_provenance``), as ``stream_arrow_schema`` of stage 2 gives it. A
    ``null`` type is kept, as the description declares it (record batches carry it as
    ``large_string``, holding only nulls). The flags of system-tag and source-info
    fields are not part of it: they follow the values of the complete table.

    Args:
        stream: The describing stream.

    Returns:
        The schema, or ``None`` for a stream that cannot say its Arrow types without
        rows (an unbound input slot, a source that has not polled yet).
    """
    # TODO(ITL-1360): ``stream.arrow_schema()``, once every stream has it.
    from orcapod.core.streams.base import row_built_names, row_built_provenance
    from orcapod.utils import arrow_utils

    typed = StaticOutputOperatorPod._zero_row_input(stream)  # pyright: ignore[reportPrivateUsage]  # the zero-row form of an input
    if typed is None:
        return None
    zero_rows = typed.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
    zero_rows = zero_rows.cast(
        pa.schema(
            [
                field.with_type(
                    arrow_utils.normalize_to_large_types(field.type, keep_null=True)
                )
                for field in zero_rows.schema
            ]
        )
    )
    names = row_built_names(zero_rows.column_names, stream.keys())
    return row_built_provenance(zero_rows.select(names)).schema


ROW_BUILT_OUTPUT = "_orcapod_row_built_output"
"""The attribute that marks a complete node output in the row-built form (ITL-1347).

Set where every such output is made (``_as_upstream_stream``, and the zero-row input
of ``_materialize_upstream``), never inferred from a class: such a stream is a
complete, immutable output already in the row-built form, so a consumer of its record
batches would rebuild exactly it, and ``channel_to_stream`` may give it without
reading.
"""


def channel_to_stream(
    channel: RecordBatchInput,
) -> Coroutine[Any, Any, StreamProtocol]:
    """Return what an input channel delivers, as a stream: the one such conversion.

    It reads the channel to its end (one pass; every error of the read, such as an
    unbound source's or a failed producer's, is raised here, before any operator runs)
    and builds a fixed snapshot of what was delivered through the one settlement path
    (``stream_from_batches``). The only shortcut: a channel opened on a complete
    complete node output in the row-built form (``ROW_BUILT_OUTPUT``), not read yet, gives
    that stream, which is exactly what its batches would rebuild; the channel is then
    closed. A reader of an output channel always collects, whatever its description.

    The channel is reserved when this function is called, before the returned
    coroutine is awaited, so nothing can read it in between; the task that runs the
    coroutine takes the channel over, and any other task's read is refused.

    Args:
        channel: The input channel, not read yet.

    Returns:
        The conversion to await: a complete, re-readable stream of the channel's
        input.

    Raises:
        RuntimeError: If a read of the channel was already called (a batch received,
            its end reached, or another consumer still waiting): a stream of part of
            an input is never made, and a channel is never read again.
        ChannelClosed: If the channel was closed, before or during the read.
        UpstreamFailedError: If the producer failed or was cancelled.
    """
    claim = channel.reserve()  # at call time: nothing can read it before the await
    return _channel_to_stream(channel, claim)


async def _channel_to_stream(
    channel: RecordBatchInput, claim: object
) -> StreamProtocol:
    """Read a reserved channel and return its input as a stream (``channel_to_stream``).

    Args:
        channel: The channel ``channel_to_stream`` reserved.
        claim: Its claim, which the task running this conversion takes over.
    """
    channel.reserve(claim)
    opened_on = channel.opened_on
    if opened_on is not None and getattr(opened_on, ROW_BUILT_OUTPUT, False):
        await channel.close()
        return opened_on
    return stream_from_batches(channel.stream, await channel.collect())


def stream_from_batches(
    stream: StreamProtocol, batches: Sequence[pa.RecordBatch]
) -> StreamProtocol:
    """Return the Arrow record batches of ``stream``'s rows as a stream.

    The one place a stream is made from Arrow record batches: what an output channel
    gives once closed (``OutputChannel.result``), what an input channel delivered
    (``channel_to_stream``), and the record batches a row-by-row operator is
    applied to. The batches are put under one another in the order given, and the
    table is handed on as a node hands its rows to its successors
    (``_output_stream``): in the row-built form, each system-tag and source-info field
    flagged nullable only when one of its values is null (decided over the complete
    table, never per batch), with ``stream``'s identity: its producer, upstreams,
    pipeline hash and data context. With no batch it is ``stream``'s zero-row input,
    typed by its description and never by rows.

    Args:
        stream: The describing stream: the node or stream the rows are of.
        batches: Its rows (``row_built_columns``), in order.

    Returns:
        A complete, re-readable stream over the rows.
    """
    if not batches:
        return StaticOutputOperatorPod._materialize_upstream([], stream)  # pyright: ignore[reportPrivateUsage]  # the zero-row input of an operator
    table = pa.Table.from_batches([with_open_provenance_flags(b) for b in batches])
    return StaticOutputOperatorPod._output_stream(  # pyright: ignore[reportPrivateUsage]  # F and the identity, stage 2's
        table, _keys_in_delivered_order(stream.keys(), table.column_names), stream
    )


def _keys_in_delivered_order(
    keys: tuple[Sequence[str], Sequence[str]], names: Sequence[str]
) -> tuple[list[str], list[str]]:
    """The description's tag and data keys, in the order the delivered rows have them.

    The roles come from the description; the order from the batches, so a stream
    made from them keeps the columns as they were delivered. They agree for every
    stream whose description follows its rows (an operator's or a function's);
    a source node whose plain tags are declared in another order than its columns
    describes itself in the slot's order (ITL-1365) while its rows are not.

    Args:
        keys: The describing stream's tag keys and data keys.
        names: The delivered table's column names.

    Returns:
        The tag keys and the data keys, data-tags first in tag order.
    """
    tag_keys, data_keys = keys
    tags, data = set(tag_keys), set(data_keys)
    position = {name: i for i, name in enumerate(names)}
    ordered_tags = sorted(tag_keys, key=lambda n: position.get(n, len(names)))
    data_tags = [n for n in ordered_tags if n in data]
    plain = sorted(
        (n for n in data_keys if n not in tags),
        key=lambda n: position.get(n, len(names)),
    )
    return ordered_tags, data_tags + plain


def with_open_provenance_flags(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Flag every system-tag and source-info field of ``batch`` nullable.

    Record batches of one channel differ at most in these flags: each is built from its
    own rows (``row_built_columns`` leaves them, ``_flag_by_values`` sets them over a
    complete table). Opened, they have one schema and can be put under one another;
    the complete table's flags are then set by its values. The same arrays.

    Args:
        batch: A record batch of a stream's rows.

    Returns:
        The batch with those fields nullable.
    """
    prefixes = (constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX)
    schema = pa.schema(
        [
            field.with_nullable(True) if field.name.startswith(prefixes) else field
            for field in batch.schema
        ]
    )
    return pa.RecordBatch.from_arrays(batch.columns, schema=schema)


class _RowsOfStream(ArrowTableStream):
    """Rows standing in for the stream they came from, with its pipeline hash.

    The barrier-mode ``async_execute`` rebuilds each input from its rows, and the
    sync path rebuilds the rows of an upstream with no producer
    (``_as_upstream_stream``). A rebuilt ``ArrowTableStream`` has no producer, so
    its own pipeline hash is the schema-only hash of a root; this stream reports
    the pipeline hash of the stream the rows came from instead (the operator
    node's ``input_pipeline_hashes``, or the upstream's own). Its content hash is
    that of its table, as for any ``ArrowTableStream`` without a producer.

    Args:
        table: The rows.
        tag_columns: The user tag columns.
        pipeline_hash: The pipeline hash of the stream the rows came from.
        upstreams: The upstreams of the stream the rows came from, if any.
        data_context: The rows' data context; ``None`` for the default.
    """

    def __init__(
        self,
        table: pa.Table,
        *,
        tag_columns: Collection[str],
        pipeline_hash: ContentHash,
        data_tags: Collection[str] = (),
        upstreams: tuple[StreamProtocol, ...] = (),
        data_context: DataContext | None = None,
    ) -> None:
        super().__init__(
            table,
            tag_columns=tag_columns,
            data_tags=data_tags,
            upstreams=upstreams,
            data_context=data_context,
        )
        self._input_pipeline_hash: ContentHash = pipeline_hash

    def pipeline_hash(
        self, hasher: SemanticHasherProtocol | None = None
    ) -> ContentHash:
        """The pipeline hash of the stream the rows came from.

        It was computed by the operator node with the hasher of its data
        context, so it is returned whatever ``hasher`` is given.

        Args:
            hasher: Not used.

        Returns:
            The input pipeline hash the operator node passed.
        """
        return self._input_pipeline_hash


class DynamicPodStream(StreamBase):
    """Recomputable stream wrapping a ``StaticOutputOperatorPod`` invocation."""

    def __init__(
        self,
        pod: StaticOutputOperatorPod,
        upstreams: tuple[StreamProtocol, ...] = (),
        label: str | None = None,
        data_context: DataContext | None = None,
        config: OrcapodConfig | None = None,
    ) -> None:
        self._pod = pod
        self._upstreams = upstreams

        super().__init__(label=label, data_context=data_context, config=config)
        self._set_modified_time(None)
        self._cached_time: datetime | None = None
        self._cached_stream: StreamProtocol | None = None
        # Staleness bookkeeping, in memory only (never hashed, saved or stored): the
        # upstreams' states when the kept result was made (``_upstream_state``), and a
        # count of the kept results discarded, which tells a downstream operator stream
        # that this one changed (ITL-1083).
        self._kept_states: tuple[tuple[str, object], ...] | None = None
        self._reset_generation: int = 0

    def content_hash(self, hasher=None) -> ContentHash:
        """The stream's content hash; notes the use of a provisional operator.

        The hash is cached, so the note is made here rather than where the operator's
        hash is read (ITL-1279).

        Args:
            hasher: Optional semantic hasher, as for ``ContentIdentifiableBase``.

        Returns:
            The content hash, unchanged by the note.
        """
        self._pod._note_provisional_identity_use()
        return super().content_hash(hasher)

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """The stream's pipeline hash; notes the use of a provisional operator.

        Args:
            hasher: Optional semantic hasher, as for ``PipelineElementBase``.

        Returns:
            The pipeline hash, unchanged by the note.
        """
        self._pod._note_provisional_identity_use()
        return super().pipeline_hash(hasher)

    def identity_structure(self) -> Any:
        self._pod._note_provisional_identity_use()  # a direct read is a use (ITL-1279)
        structure = (self._pod,)
        if self._upstreams:
            structure += (self._pod.argument_symmetry(self._upstreams),)
        return structure

    def pipeline_identity_structure(self) -> Any:
        self._pod._note_provisional_identity_use()  # a direct read is a use (ITL-1279)
        structure = (self._pod,)
        if self._upstreams:
            structure += (self._pod.argument_symmetry(self._upstreams),)
        return structure

    @property
    def producer(self) -> PodProtocol:
        return self._pod

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return self._upstreams

    def clear_cache(self) -> None:
        """Clear the cached stream, forcing recomputation on next access."""
        self._cached_stream = None
        self._cached_time = None
        self._kept_states = None
        self._reset_generation += 1

    def _typed_zero_row_table(self) -> pa.Table | None:
        """The pod's result over its upstreams' zero-row tables (ITL-912).

        The rule an operator node follows (``OperatorJobNode._typed_zero_row_table``),
        with each upstream given as the stream it is, as ``static_process`` reads
        it: no row is computed or read, and nothing this stream holds is consulted.
        """
        inputs: list[StreamProtocol] = []
        for upstream in self._upstreams:
            # The upstream itself with no row: the pod reads the upstream stream,
            # not rows rebuilt from it, so its columns stay in their own order.
            table = typed_zero_row_table(upstream)
            if table is None:
                return None
            inputs.append(
                ArrowTableStream(
                    table,
                    tag_columns=upstream.keys()[0],
                    data_tags=stream_data_tags(upstream),
                    producer=upstream.producer,
                    upstreams=upstream.upstreams,
                    data_context=StaticOutputOperatorPod._upstream_context(upstream),
                )
            )
        pod = cast(StaticOutputOperatorPod, self._pod)
        return pod.zero_row_result(*inputs).as_table(columns=ZERO_ROW_INPUT_COLUMNS)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return the (tag_keys, data_keys) column names for this stream."""
        tag_schema, data_schema = self.output_schema(columns=columns, all_info=all_info)
        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the (tag_schema, data_schema) for this stream."""
        # Memoized (ITL-1111). The prediction depends on the operator's configuration
        # and on the upstreams' schemas and pipeline hashes, all fixed for the life of
        # this stream; never on rows, so recomputing the rows when stale leaves it valid.
        return self._memoized_output_schema(
            columns,
            all_info,
            lambda: self._pod.output_schema(
                *self.upstreams, columns=columns, all_info=all_info
            ),
        )

    @property
    def last_modified(self) -> datetime | None:
        """Returns the last modified time of the stream."""
        self._update_cache_status()
        return self._cached_time

    def _update_cache_status(self) -> None:
        """Discard the kept result if the operator or an upstream changed since it was made.

        A pod stream or an operator stream upstream is changed when its state
        (``_upstream_state``) differs from the one noted with the kept result: its modified
        time moves when it completes its first computation, which happens after the result
        was kept when the operator returned that stream unread (a pass-through), and that is
        not a change (ITL-1083, the rule ITL-1065 set for pod streams). Any other upstream,
        and the operator, are changed when their modified time is unknown or newer.
        """
        if self._cached_time is None:
            return

        states = _upstream_states(self.upstreams)
        times = [value for kind, value in states if kind == "time"]
        times.append(self._pod.last_modified)

        if any(t is None for t in times):
            self._discard_result()
            self._cached_time = None
            return

        newest = max(cast(list[datetime], times))
        if states != self._kept_states or newest > self._cached_time:
            self._discard_result()
            # Noted so that the next check finds a change only if there is a new one.
            self._kept_states = states
            self._cached_time = datetime.now(timezone.utc)

    @property
    def is_stale(self) -> bool:
        """Whether the next read must run the operator: no kept result, or an outdated one.

        Judged as ``_update_cache_status`` judges the kept result (which it discards if
        outdated), not by comparing modified times: an upstream that completed its first
        computation after the result was kept is newer, but not changed (ITL-1083).
        """
        self._update_cache_status()
        return self._cached_stream is None

    def _discard_result(self) -> None:
        """Drop the kept result; a downstream operator stream sees the change."""
        self._cached_stream = None
        self._reset_generation += 1

    def _refresh(self) -> None:
        """Run the operator unless its kept result is current: the one computation
        path of ``as_table``, ``iter_data`` and so ``run``.

        Notes the use of a provisional operator on every read, also of a kept result
        (ITL-1279)."""
        self._pod._note_provisional_identity_use()
        self._update_cache_status()

        # recompute if cache is invalid
        if self._cached_time is None or self._cached_stream is None:
            # Noted before the operator reads its inputs, so that a change made while it
            # reads them is seen by the next check.
            kept_states = _upstream_states(self.upstreams)
            self._cached_stream = self._pod.static_process(
                *self.upstreams,
            )
            self._cached_time = datetime.now(timezone.utc)
            self._kept_states = kept_states

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        self._refresh()
        assert self._cached_stream is not None, (
            "StreamProtocol has not been updated or is empty."
        )
        return self._cached_stream.as_table(columns=columns, all_info=all_info)

    def iter_data(
        self,
    ) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        self._refresh()
        assert self._cached_stream is not None, (
            "StreamProtocol has not been updated or is empty."
        )
        return self._cached_stream.iter_data()

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first rows of the operator's kept result; never runs it over rows.

        The head of an operator is not the operator over the heads of its inputs (a
        group over ten rows has the wrong members), so before the result is computed,
        or after an input changed, there are no rows. See ``StreamBase.head``.
        """
        check_head_size(n)
        # A preview of a kept result is a read: it notes a provisional use (ITL-1279).
        self._pod._note_provisional_identity_use()
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        had_result = self._cached_stream is not None
        self._update_cache_status()
        if self._cached_stream is not None and self._cached_time is not None:
            return head_of(
                self._cached_stream.head(n, columns=config), self, origin="computed"
            )
        return self._head_without_rows(
            config,
            ViewStatus.NOT_COMPUTED,
            note=STALE_NOTE if had_result else None,
        )

    def __repr__(self) -> str:
        # Upstreams by class and label only: their own repr would nest every path (ITL-1111).
        upstreams = ", ".join(
            short_stream_repr(upstream) for upstream in self.upstreams
        )
        return (
            f"{self.__class__.__name__}(label={self.label!r}, kernel={self.producer}, "
            f"upstreams=({upstreams}))"
        )


def _upstream_state(stream: StreamProtocol) -> tuple[str, object]:
    """What tells a change of an upstream of an operator stream.

    A pod stream or an operator stream computes lazily, so its modified time moves when it
    completes its first computation, which is not a change of its content: its reset
    generation tells a change instead (ITL-1083, as ``FunctionPodStream`` judges a pod-stream
    input, ITL-1065). A stale pod stream counts the reset its next read makes. An operator
    stream is first asked for its modified time, which discards its own kept result if that
    is stale. Any other upstream (source, table stream, node) is told by its modified time.

    Args:
        stream: The upstream.

    Returns:
        ``("generation", n)`` or ``("time", modified time or None)``.
    """
    if isinstance(stream, FunctionPodStream):
        generation = stream._reset_generation  # pyright: ignore[reportPrivateUsage]  # staleness bookkeeping shared by the stream kinds, never public
        return ("generation", generation + int(stream.is_stale))
    if isinstance(stream, DynamicPodStream):
        _ = stream.last_modified
        return ("generation", stream._reset_generation)  # pyright: ignore[reportPrivateUsage]  # same class
    return ("time", stream.last_modified)


def _upstream_states(
    upstreams: tuple[StreamProtocol, ...],
) -> tuple[tuple[str, object], ...]:
    """The state of each upstream (``_upstream_state``), in order."""
    return tuple(_upstream_state(stream) for stream in upstreams)
