"""Filter: keep the rows that satisfy a ``Condition`` (ITL-1374, ITL-1378).

A step of the pipeline: its identity is the condition's canonical form, so it enters
every hash downstream. The rows are chosen by an Arrow mask (``filter_mask``) and taken
with ``Table.filter``, so every value passes through bit for bit; no Polars.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.data_tags import stream_data_tags
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.operators.filter_mask import (
    check_filter_condition,
    condition_mask,
    filter_table,
)
from orcapod.core.operators.meta_columns import WithoutMetaWriter
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.types.condition import Condition, check_condition_columns
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


def _user_columns(
    tag_columns: Sequence[str], data_columns: Sequence[str]
) -> tuple[str, ...]:
    """Return the columns a condition may name: user tags and data, data-tags once.

    Meta columns are left out: a stream built straight from a table lists them among
    its data columns (ITL-846), and they are not user columns.
    """
    return tuple(
        dict.fromkeys(
            name
            for name in (*tag_columns, *data_columns)
            if not name.startswith(constants.META_PREFIX)
        )
    )


class Filter(UnaryOperator):
    """Keep the rows that satisfy a condition; every value passes through as it is.

    The condition is a ``Condition`` (``orcapod.types``): column equalities and the
    terms ``col(name)`` builds (comparisons, ``is_in``, ``between``, null tests,
    struct fields, column against column), combined with ``&``, ``|`` and ``~``.
    Tag columns, data columns and data-tags are named alike. Nulls follow Kleene
    logic (a row whose condition is unknown is dropped) and floats Orcapod's matching
    rule (``0.0`` equals ``-0.0``, NaN equals NaN and is greater than every number).

    The output has the input's columns, types and nullability (``is_not_null`` does
    not narrow ``T | None``: ``DropNulls`` does), system tags (names and values),
    source info and row order. Only the rows change.

    The operator's identity is ``("Filter", condition.canonical_form())``: the order
    of keywords, arguments, operands and ``is_in`` values never changes it.

    Filter is provisional (``PROVISIONAL_OPERATORS``): its identity may change in a
    later release.

    Args:
        condition: The condition, or a mapping of column equalities.

    Raises:
        ValueError: If the condition names no column.
        TypeError: If ``condition`` is neither a condition nor a mapping.
    """

    # Data-tags (ITL-1023): a data-tag is one column, named alike as tag or data.
    DATA_TAGS_ISSUE: ClassVar[str | None] = None

    # A filter chooses rows and never changes a column (ITL-912).
    OUTPUT_SCHEMA_IS_INPUT_SCHEMA: ClassVar[bool] = True

    # Its head shows the matching rows of its input's head (decision 13, ITL-1374).
    HEAD_FROM_INPUT_HEAD: ClassVar[bool] = True

    # Its identity is not frozen with data context v1 (ITL-1374).
    PROVISIONAL: ClassVar[bool] = True

    def __init__(
        self, condition: Condition | Mapping[str, object], **kwargs: Any
    ) -> None:
        self._condition: Condition = Condition.of(condition)
        super().__init__(**kwargs)

    @property
    def condition(self) -> Condition:
        """The condition every kept row satisfies."""
        return self._condition

    def rows_dropped_text(
        self,
        position: int,
        others: Sequence[str],
        inputs: Sequence[StreamProtocol],
    ) -> str | None:
        """What the filter keeps, for ``explain()``: its condition's text."""
        return self._condition.canonical_text()

    def identity_structure(self) -> Any:
        """Return the operator's identity: its class name and the canonical condition.

        Returns:
            ``("Filter", condition.canonical_form())``.
        """
        self._note_provisional_identity_use()  # a direct read is a use (ITL-1279)
        return (self.__class__.__name__, self._condition.canonical_form())

    def to_config(self) -> dict[str, Any]:
        """Serialize this operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path`` and ``config`` keys, where
            ``config`` holds ``condition``, the condition's saved form
            (``Condition.to_config``).
        """
        config = super().to_config()
        config["config"] = {"condition": self._condition.to_config()}
        return config

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Filter:
        """Reconstruct a Filter from a config dict.

        Args:
            config: Dict as returned by ``to_config()``.

        Returns:
            A new ``Filter`` with the saved condition.
        """
        return cls(Condition.from_config(config["config"]["condition"]))

    # ------------------------------------------------------------------
    # Validation and schema prediction
    # ------------------------------------------------------------------

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """Check the condition against the stream's Python schema (graph build).

        Every column a term names must be a user tag or data column, and a value
        term's value of the column's Python type. Struct fields, Arrow widths,
        ranges and time zones are checked when the filter is applied.

        Args:
            stream: The upstream stream.

        Raises:
            InputValidationError: If a column is missing or a value does not fit it.
        """
        tag_schema, data_schema = stream.output_schema()
        names = _user_columns(tuple(tag_schema), tuple(data_schema))
        python_schema = {
            name: tag_schema[name] if name in tag_schema else data_schema[name]
            for name in names
        }
        try:
            check_condition_columns(self._condition, python_schema)
        except (TypeError, ValueError) as error:
            raise InputValidationError(f"Filter: {error}") from error

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the input's schemas: a filter changes no column.

        Args:
            stream: The upstream stream.
            columns: Column inclusion config passed through to ``output_schema``.
            all_info: Whether to include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.
        """
        return stream.output_schema(columns=columns, all_info=all_info)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def _check_arrow(self, schema: pa.Schema, names: Sequence[str]) -> None:
        """Check the condition against the Arrow types of the user columns ``names``.

        Raises:
            InputValidationError: If a field is missing, or a value or a column's
                type does not fit its term.
        """
        check_filter_condition(
            self._condition, pa.schema([schema.field(name) for name in names])
        )

    def head_rows(self, stream: StreamProtocol, table: pa.Table) -> pa.Table | None:
        """The rows of ``table`` (what ``stream``'s ``head`` shows) the condition keeps.

        Decision 13 of ITL-1374: a filter holds no user code, has no effect and keeps a
        row by that row alone, so its head may evaluate the condition over the rows its
        input's head shows. This is the plain stream's head: a filter captured
        in a job is a job node, which shows its current run (ITL-1095) and so reads not
        computed before its first run. Nothing is computed, polled or written. A condition that does
        not fit the input's Arrow types shows nothing here; its error appears at the run.

        Args:
            stream: The input.
            table: The rows its head shows.

        Returns:
            The matching rows, without meta columns, or ``None`` when the condition does
            not fit ``table``.
        """
        names = _user_columns(*stream.keys())
        try:
            self._check_arrow(table.schema, names)
        except InputValidationError:
            return None
        meta = [n for n in table.column_names if n.startswith(constants.META_PREFIX)]
        return filter_table(table.drop_columns(meta), self._condition)

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Return the stream's rows that satisfy the condition, in input order.

        Args:
            stream: The upstream stream.

        Returns:
            A stream over the kept rows, every column as it is. A result with no
            rows has the same columns and types.

        Raises:
            InputValidationError: If the condition does not fit the input's Arrow
                types.
        """
        # Every system column passes through: source info, system tags and each row's
        # data context (a raw stream may hold several).
        table = stream.as_table(
            columns={
                "source": True,
                "system_tags": True,
                "context": True,
                "sort_by_tags": False,
            }
        )
        tag_columns, data_columns = stream.keys()
        self._check_arrow(table.schema, _user_columns(tag_columns, data_columns))
        return ArrowTableStream(
            filter_table(table, self._condition),
            tag_columns=tag_columns,
            data_tags=stream_data_tags(stream),
            data_context=stream.data_context_key,
        )

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Streaming: test each row as it arrives and pass the kept ones on.

        Each row's user columns are read from its tag and data Arrow tables into one
        one-row table, and the kernel of the sync path decides whether it is kept. A
        kept row is passed on as it arrived (without its meta columns), so its values,
        types and system tags are those of the sync path.

        On this channel path the condition is checked against each row's Arrow types,
        the only schema the channel carries: an input with no rows is never checked, and
        a raw stream's Arrow ``null`` column arrives as ``large_string`` (``Data``'s null
        convention, ITL-1319), so a condition its sync path refuses on either can pass
        here. A job run (either orchestrator) checks its input's Arrow schema before any
        row, as the sync path does, and refuses both; no source holds a ``null`` column.

        Args:
            inputs: Single-element sequence of readable channels.
            output: Channel to send kept rows to.
            input_pipeline_hashes: Not used: a filter keeps system-tag names as they
                are.
        """
        try:
            output = WithoutMetaWriter(output)
            # Schemas already checked; a list, since a schema holding a logical type
            # is not hashable.
            checked: list[pa.Schema] = []
            async for tag, data in inputs[0]:
                row = self._row_table(tag, data)
                if not any(row.schema.equals(seen) for seen in checked):
                    self._check_arrow(row.schema, row.column_names)
                    checked.append(row.schema)
                mask = condition_mask(row, self._condition)
                if mask.to_pylist() == [True]:
                    await output.send((tag, data))
        finally:
            await output.close()

    @staticmethod
    def _row_table(tag: TagProtocol, data: DataProtocol) -> pa.Table:
        """Return one row's user tag and data columns as a one-row table.

        A data-tag is one column, read from the tag.
        """
        tag_table = tag.as_table()
        data_table = data.as_table()
        names = _user_columns(tag_table.column_names, data_table.column_names)
        columns = [
            tag_table.column(name)
            if name in tag_table.column_names
            else data_table.column(name)
            for name in names
        ]
        fields = [
            tag_table.schema.field(name)
            if name in tag_table.column_names
            else data_table.schema.field(name)
            for name in names
        ]
        return pa.Table.from_arrays(columns, schema=pa.schema(fields))
