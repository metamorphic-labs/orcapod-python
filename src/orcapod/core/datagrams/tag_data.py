"""
Tag and Data — datagram subclasses with system-tags and source-info support.

``Tag``
    Extends ``Datagram`` with *system tags*: metadata fields whose names start with
    ``constants.SYSTEM_TAG_PREFIX``.  System tags travel alongside the primary data
    but are excluded from content hashing and structural operations unless explicitly
    requested via ``ColumnConfig(system_tags=True)``.

``Data``
    Extends ``Datagram`` with *source information*: provenance tokens (strings,
    None, or lists of tokens) keyed by data-column name.  Source-info keys are
    stored without the ``constants.SOURCE_PREFIX`` internally and added back when
    serialising via ``as_dict()`` / ``as_table()``.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator, Mapping, Sequence
from typing import cast, TYPE_CHECKING, Any, Self

import pyarrow as pa

from orcapod import contexts
from orcapod.core.datagrams.datagram import Datagram, context_key_field
from orcapod.semantic_types import infer_python_schema_from_pylist_data
from orcapod.system_constants import constants
from orcapod.types import (
    ColumnConfig,
    ContentHash,
    DataType,
    DataValue,
    Schema,
    SchemaLike,
    SourceInfoValue,
    TokenLayout,
)
from orcapod.utils import arrow_utils

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import DataProtocol

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tag
# ---------------------------------------------------------------------------


# The Arrow type of a null source-info token, whatever type its column declares
# (``arrow_utils.source_info_arrow_type(None)``).
_NULL_TOKEN_TYPE = pa.large_string()


def _source_info_field(name: str, token: SourceInfoValue) -> pa.Field[Any]:
    """The field of source-info column ``name`` holding ``token``, as a row's table has it."""
    import pyarrow as _pa

    return _pa.field(name, arrow_utils.source_info_arrow_type(token))


class Tag(Datagram):
    """
    Datagram with system-tags support.

    System tags are metadata fields whose names begin with
    ``constants.SYSTEM_TAG_PREFIX``.  They are excluded from the primary data
    representation (and therefore from content hashing) unless the caller requests
    them via ``ColumnConfig(system_tags=True)``.

    Accepts the same inputs as ``Datagram`` (dict or Arrow table/batch).
    System-tag fields found in the input are automatically extracted.
    """

    def __init__(
        self,
        data: "Mapping[str, DataValue] | pa.Table | pa.RecordBatch",
        system_tags: "Mapping[str, DataValue] | None" = None,
        meta_info: "Mapping[str, DataValue] | None" = None,
        python_schema: "SchemaLike | None" = None,
        data_context: "str | contexts.DataContext | None" = None,
        record_uuid: "uuid.UUID | None" = None,
        **kwargs,
    ) -> None:
        import pyarrow as _pa

        if isinstance(data, _pa.RecordBatch):
            data = _pa.Table.from_batches([data])

        extracted_sys_tags: dict[str, DataValue]

        if isinstance(data, _pa.Table):
            # Arrow path: call super() first, then extract system-tag columns from
            # self._data_table (same pattern as the legacy ArrowTag).
            super().__init__(
                data,
                meta_info=meta_info,
                data_context=data_context,
                record_uuid=record_uuid,
                **kwargs,
            )
            sys_tag_cols = [
                c
                for c in self._data_table.column_names  # type: ignore[union-attr]
                if c.startswith(constants.SYSTEM_TAG_PREFIX)
            ]
            if sys_tag_cols:
                extracted_sys_tags = (
                    self._data_context.type_converter.arrow_table_to_python_dicts(
                        self._data_table.select(sys_tag_cols)  # type: ignore[union-attr]
                    )[0]
                )
                self._data_table = self._data_table.drop_columns(sys_tag_cols)  # type: ignore[union-attr]
                # Invalidate derived caches
                self._data_arrow_schema = None
            else:
                extracted_sys_tags = {}
        else:
            # Dict path: extract system-tag keys before calling super()
            data_only = {
                k: v
                for k, v in data.items()
                if not k.startswith(constants.SYSTEM_TAG_PREFIX)
            }
            extracted_sys_tags = {
                k: v
                for k, v in data.items()
                if k.startswith(constants.SYSTEM_TAG_PREFIX)
            }
            super().__init__(
                data_only,
                python_schema=python_schema,
                meta_info=meta_info,
                data_context=data_context,
                record_uuid=record_uuid,
                **kwargs,
            )

        self._set_system_tags({**extracted_sys_tags, **(system_tags or {})})

    def _set_system_tags(self, system_tags: dict[str, DataValue]) -> None:
        """Hold ``system_tags`` as this tag's system tags."""
        self._system_tags: dict[str, DataValue] = system_tags
        self._system_tags_python_schema: Schema = infer_python_schema_from_pylist_data(
            [self._system_tags], default_type=str
        )
        self._system_tags_table: "pa.Table | None" = None

    @classmethod
    def rows_of(
        cls,
        table: "pa.Table",
        data_context: "str | contexts.DataContext | None" = None,
    ) -> Iterator[Self]:
        """Yield the tag of each row of ``table``, in row order.

        Row ``i`` gives the tag ``cls(table.slice(i, 1), data_context=data_context)``
        gives: the same user tags, system tags, meta values, Arrow types and hashes.
        What the constructor does to every row alike is done once for the whole table
        (ITL-1348): the normalization to the large types of a datagram and the split
        of the columns into groups. Building the tags of a table row by row through the
        constructor normalizes every row again. The table may be chunked in any way.

        Args:
            table: The rows: user-tag and system-tag columns.
            data_context: The data context of every tag; ``None`` reads each row's own
                from the table's data-context column, or takes the default.

        Yields:
            One tag per row.

        Raises:
            ValueError: If the table has rows and no tag column.
        """
        rows = cls._rows_of_table(
            table, data_context, apart_prefix=constants.SYSTEM_TAG_PREFIX
        )
        for tag, system_tag_row, _ in rows:
            tag._set_system_tags(
                tag._data_context.type_converter.arrow_table_to_python_dicts(
                    system_tag_row
                )[0]
                if system_tag_row is not None
                else {}
            )
            yield tag

    # ------------------------------------------------------------------
    # Internal helper
    # ------------------------------------------------------------------

    def _ensure_system_tags_table(self) -> "pa.Table":
        if self._system_tags_table is None:
            self._system_tags_table = (
                self._data_context.type_converter.python_dicts_to_arrow_table(
                    [self._system_tags],
                    python_schema=self._system_tags_python_schema,
                )
            )
        return self._system_tags_table

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

    def keys(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        keys = super().keys(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags:
            keys += tuple(self._system_tags.keys())
        return keys

    def schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> Schema:
        schema = super().schema(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags:
            return Schema({**schema, **self._system_tags_python_schema})
        return schema

    def arrow_schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Schema":
        schema = super().arrow_schema(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags and self._system_tags:
            return arrow_utils.join_arrow_schemas(
                schema, self._ensure_system_tags_table().schema
            )
        return schema

    def as_dict(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "dict[str, DataValue]":
        result = super().as_dict(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags:
            result.update(self._system_tags)
        return result

    def as_table(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Table":
        table = super().as_table(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags and self._system_tags:
            table = arrow_utils.hstack_tables(table, self._ensure_system_tags_table())
        return table

    def system_tags(self) -> "dict[str, DataValue]":
        """Return a copy of the system-tags dict."""
        return dict(self._system_tags)

    def as_datagram(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> Datagram:
        data = self.as_dict(columns=columns, all_info=all_info)
        python_schema = self.schema(columns=columns, all_info=all_info)
        return Datagram(
            data, python_schema=python_schema, data_context=self._data_context
        )

    def copy(self, include_cache: bool = True, preserve_id: bool = False) -> Self:
        new_tag = super().copy(include_cache=include_cache, preserve_id=preserve_id)
        new_tag._system_tags = dict(self._system_tags)
        new_tag._system_tags_python_schema = self._system_tags_python_schema
        new_tag._system_tags_table = self._system_tags_table if include_cache else None
        return new_tag


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


def _source_info_python_type(value: "SourceInfoValue") -> DataType:
    """Derive the Python type for a single source-info value.

    Read off ``arrow_utils.source_info_arrow_type``, the one source-info type
    rule, so the ``Schema`` and the Arrow table always agree on a token's depth.

    Args:
        value: The stored provenance token.

    Returns:
        ``str`` for scalars and unknowns, ``list[...]`` nested to the token's
        depth for lists.
    """
    return _token_python_type(arrow_utils.source_info_arrow_type(value))


def _token_python_type(arrow_type: "pa.DataType") -> DataType:
    """The Python type of a source-info column of ``arrow_type``.

    Args:
        arrow_type: A token column's Arrow type: a string, or a list of any kind
            (large, plain or fixed-size) of a token type.

    Returns:
        ``str``, under one ``list[...]`` per list level of ``arrow_type``.
    """
    depth = 0
    while (
        pa.types.is_large_list(arrow_type)
        or pa.types.is_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        arrow_type = arrow_type.value_type  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]  # a list type has a value type
        depth += 1
    python_type: DataType = str
    for _ in range(depth):
        python_type = list[python_type]  # type: ignore[valid-type]
    return python_type


class Data(Datagram):
    """
    Datagram with source-information tracking.

    Source info maps each data-column name to a provenance token
    (``SourceInfoValue``: a string, ``None``, or -- for many->one operators -- a
    list of tokens, one per aggregated member).
    Keys in ``_source_info`` are stored **without** the ``SOURCE_PREFIX``; the
    prefix is added transparently when serialising to dict or Arrow table.

    Accepts the same inputs as ``Datagram`` (dict or Arrow table/batch).
    Source-info fields (columns beginning with ``SOURCE_PREFIX``) found in the
    input are automatically extracted.
    """

    def __init__(
        self,
        data: "Mapping[str, DataValue] | pa.Table | pa.RecordBatch",
        meta_info: "Mapping[str, DataValue] | None" = None,
        source_info: "Mapping[str, SourceInfoValue] | None" = None,
        python_schema: "SchemaLike | None" = None,
        data_context: "str | contexts.DataContext | None" = None,
        record_uuid: "uuid.UUID | None" = None,
        **kwargs,
    ) -> None:
        import pyarrow as _pa

        if isinstance(data, _pa.RecordBatch):
            data = _pa.Table.from_batches([data])

        if isinstance(data, _pa.Table):
            # Arrow path: use prepare_prefixed_columns to split source-info from data
            if source_info is None:
                source_info = {}
            else:
                # Normalise: remove existing prefix from provided keys
                source_info = {
                    k.removeprefix(constants.SOURCE_PREFIX)
                    if k.startswith(constants.SOURCE_PREFIX)
                    else k: v
                    for k, v in source_info.items()
                }

            data_table, si_table = self._data_and_source_info_tables(data, source_info)
            super().__init__(
                data_table,
                meta_info=meta_info,
                data_context=data_context,
                record_uuid=record_uuid,
                **kwargs,
            )
            self._source_info: dict[str, SourceInfoValue] = self._source_info_of_row(
                si_table
            )
            # The row's source-info columns: the table's own, with their types; a
            # token given as a value (``source_info``) is made here, so its column
            # is typed from that value (ITL-1491).
            typed = si_table if si_table.num_rows == 1 else None
        else:
            typed = None
            # Dict path: extract source-info keys before calling super()
            data_only = {
                k: v
                for k, v in data.items()
                if not k.startswith(constants.SOURCE_PREFIX)
            }
            contained_source_info: dict[str, SourceInfoValue] = {
                k.removeprefix(constants.SOURCE_PREFIX): v  # type: ignore[misc]
                for k, v in data.items()
                if k.startswith(constants.SOURCE_PREFIX)
            }
            super().__init__(
                data_only,
                python_schema=python_schema,
                meta_info=meta_info,
                data_context=data_context,
                record_uuid=record_uuid,
                **kwargs,
            )
            self._source_info = {**contained_source_info, **(source_info or {})}

        self._source_info_table: "pa.Table | None" = None
        # The row's source-info columns as the Arrow table it was read from gave
        # them, when it was read from one (here, or ``rows_of``); ``None``
        # otherwise, and after any change of the columns or tokens.
        self._typed_source_info: "pa.Table | None" = typed
        # What ``token_layout`` answered; a copy starts without one.
        self._token_layout: "TokenLayout | None" = None

    @staticmethod
    def _data_and_source_info_tables(
        table: "pa.Table", source_info: "Mapping[str, SourceInfoValue]"
    ) -> "tuple[pa.Table, pa.Table]":
        """Split ``table`` into its data columns and the source info of each.

        Args:
            table: Data, meta and source-info columns, of any number of rows.
            source_info: Tokens to use instead of the table's, by data column name.

        Returns:
            The table without its source-info columns, and a table with one
            source-info column per data column (null where there is no token).
        """
        data_table, prefixed_tables = arrow_utils.prepare_prefixed_columns(
            table,
            {constants.SOURCE_PREFIX: source_info},
            exclude_columns=[constants.CONTEXT_KEY],
            exclude_prefixes=[constants.META_PREFIX],
        )
        return data_table, prefixed_tables[constants.SOURCE_PREFIX]

    @staticmethod
    def _source_info_of_row(si_table: "pa.Table") -> "dict[str, SourceInfoValue]":
        """Return the tokens of a one-row source-info table, by data column name."""
        if si_table.num_columns > 0 and si_table.num_rows > 0:
            return {
                k.removeprefix(constants.SOURCE_PREFIX): v
                for k, v in si_table.to_pylist()[0].items()
            }
        return {}

    @classmethod
    def rows_of(
        cls,
        table: "pa.Table",
        data_context: "str | contexts.DataContext | None" = None,
    ) -> Iterator[Self]:
        """Yield the data of each row of ``table``, in row order.

        Row ``i`` gives the data ``cls(table.slice(i, 1), data_context=data_context)``
        gives: the same values, source info, meta values, Arrow types and hashes. What
        the constructor does to every row alike is done once for the whole table
        (ITL-1348): the split into data and source-info columns and the normalization
        to the large types of a datagram. Building the data of a table row by row
        through the constructor splits and normalizes every row again. The table may
        be chunked in any way.

        Args:
            table: The rows: data, meta and source-info columns.
            data_context: The data context of every row; ``None`` reads each row's own
                from the table's data-context column, or takes the default.

        Yields:
            One ``Data`` per row.

        Raises:
            ValueError: If the table has rows and no data column.
        """
        if table.num_rows == 0:
            return
        data_table, si_table = cls._data_and_source_info_tables(table, {})
        if data_table.num_columns == 0:
            raise ValueError("Data table must contain at least one data column.")
        rows = cls._rows_of_table(data_table, data_context, beside=si_table)
        # The rows of one table hold one layout of token and data types: the first
        # row reads it and the others are given the same object (ITL-1491).
        layout: "TokenLayout | None" = None
        for data, _, source_info_row in rows:
            assert source_info_row is not None
            data._source_info = cls._source_info_of_row(source_info_row)
            data._source_info_table = None
            data._typed_source_info = source_info_row
            data._token_layout = layout
            if layout is None:
                layout = data.token_layout()
            yield data

    # ------------------------------------------------------------------
    # Internal helper
    # ------------------------------------------------------------------

    def _ensure_source_info_table(self) -> "pa.Table":
        """Return the row's source-info columns as ``as_table`` shows them.

        The type of a token column is determined by the pipeline, never taken
        from values (developer, November 27, 2026, ITL-1491): a row read from an
        Arrow table gives the columns that table has, with their types (a null
        token's included) and every field nullable; the arrays are wrapped again,
        never cast. Only a row built from Python values, for which no Arrow type
        exists, types each token from its value
        (``arrow_utils.source_info_arrow_type``).
        """
        typed = self._typed_source_info
        if self._source_info_table is None and typed is not None:
            self._source_info_table = pa.Table.from_arrays(
                typed.columns,
                schema=pa.schema([pa.field(f.name, f.type) for f in typed.schema]),
            )
        if self._source_info_table is None:
            import pyarrow as _pa

            # Only the source info of the data columns this Data holds, in the order of
            # those columns, as every other view gives it (ITL-1040).
            current = {
                k: self._source_info[k]
                for k in super().keys()
                if k in self._source_info
            }
            if current:
                prefixed = {
                    f"{constants.SOURCE_PREFIX}{k}": v for k, v in current.items()
                }
                schema = _pa.schema(
                    [_source_info_field(k, v) for k, v in prefixed.items()]
                )
                self._source_info_table = _pa.Table.from_pylist(
                    [prefixed], schema=schema
                )
            else:
                self._source_info_table = _pa.table({})
        return self._source_info_table

    @staticmethod
    def function_output_zero_row_table(
        python_schema: Schema,
        data_context: contexts.DataContext,
        meta_python_schema: Schema | None = None,
        output_arrow_schema: "pa.Schema | None" = None,
    ) -> pa.Table:
        """The table a function's output rows give, with no row.

        The zero-row form of ``Data.as_table(columns={"context": True, "source":
        True})`` for the ``Data`` a data function returns: built with
        ``python_schema=`` its declared output schema and a string source-info
        token for every column. Each part comes from the code that types a
        computed row: the data fields from the converter's
        ``python_schema_to_arrow_schema`` (``Datagram._ensure_arrow_schema``), the
        context field from ``context_key_field``, the meta fields from the same
        conversion of their Python types (``Datagram._ensure_meta_table``) and the
        source-info fields from ``_source_info_field`` with a string token
        (ITL-912). The columns are in the order ``Data.arrow_schema`` gives them.

        Args:
            python_schema: The function's declared output schema.
            data_context: The data context the rows are typed in.
            meta_python_schema: The meta columns every output row carries (a
                cached pod's ``__computed`` flag), if any.
            output_arrow_schema: The Arrow types of the output columns when they are
                known apart from ``python_schema`` (a loaded node's saved types,
                ITL-640); ``None`` converts ``python_schema``.

        Returns:
            A zero-row table: data columns, ``_context_key``, meta columns,
            source-info columns.
        """
        import pyarrow as _pa

        fields = list(
            output_arrow_schema
            if output_arrow_schema is not None
            else data_context.type_converter.python_schema_to_arrow_schema(
                python_schema
            )
        )
        fields.append(context_key_field())
        if meta_python_schema:
            fields.extend(
                data_context.type_converter.python_schema_to_arrow_schema(
                    meta_python_schema
                )
            )
        # A data function's source-info token is always a string (see
        # ``PythonDataFunction``): the type is the one a string token gives.
        fields.extend(
            _source_info_field(f"{constants.SOURCE_PREFIX}{name}", "")
            for name in python_schema
        )
        return _pa.schema(fields).empty_table()

    # ------------------------------------------------------------------
    # Source-info API
    # ------------------------------------------------------------------

    def source_info(self) -> "dict[str, SourceInfoValue]":
        """Return source info for all data-column keys (None for unknown)."""
        return {k: self._source_info.get(k) for k in self.keys()}

    def source_info_table(self) -> "pa.Table":
        """Return the source-info tokens as a one-row table.

        One ``_source_<column>`` per data column, in their order, every field
        nullable; a data column with no token has a null token. Each column has
        one canonical type, a function of the token's value alone (ITL-1471): a
        null token is a ``large_string`` null whatever type its column declares
        (a null says nothing a type could tell apart), and a non-null token of a
        row read from an Arrow table (a stream's row) keeps the type that table
        gives it: its type is never worked out again from its value, which a
        nested token column would not survive. A change of the row keeps the
        typed column of every token it keeps (a selection, a drop, a rename,
        added columns, a column taken from another row with its token:
        ``with_columns_of``). Only a token created from a Python value, where no
        Arrow type exists (a row built from values, ``with_source_info``), is
        typed from its value. So a row built from values and the same row read
        from a table give the same columns when the values' own types are the
        table's (a hand-built list token that shows fewer list levels than the
        table declares is typed from its value, and differs).

        ``as_table(columns={"source": True})`` gives the same typed columns
        (ITL-1491) and differs in two ways: a null token keeps the type its
        column declares, so that the rows of one stream make one table; and a row
        built from values has no column for a data column whose token was never
        given.

        Returns:
            The one-row table; a table with no column when there is no data
            column (a placeholder that carries no token).
        """
        typed = self._typed_source_info
        if typed is None:
            names = Datagram.keys(self)
            if all(name in self._source_info for name in names):
                return self._ensure_source_info_table()
            return self._token_columns({}, every_column=True)
        # The fields of a row's own table: the name and the type, nullable, as
        # ``_source_info_field`` makes them (a stream's may be non-null). The
        # arrays are the stream's, wrapped again, never cast. A null token is a
        # ``large_string`` null whatever its column declares, and so is the token
        # of a data column the row holds no token column for.
        null = pa.chunked_array([pa.nulls(1, _NULL_TOKEN_TYPE)])
        columns: "list[pa.ChunkedArray[Any]]" = []
        column_names: "list[str]" = []
        for name in Datagram.keys(self):
            column_name = f"{constants.SOURCE_PREFIX}{name}"
            column = (
                typed.column(column_name) if column_name in typed.column_names else null
            )
            columns.append(null if column.null_count == len(column) else column)
            column_names.append(column_name)
        return pa.Table.from_arrays(
            columns,
            schema=pa.schema(
                [
                    pa.field(name, column.type)
                    for name, column in zip(column_names, columns)
                ]
            ),
        )

    def token_layout(self) -> "TokenLayout":
        """Return the token columns of this row that are not plain strings.

        What the token-shape rule reads of a row that is handed on without being
        built into a stream (``schema_utils.require_token_shapes_of_rows``). No
        table is built and no data value is read: a row read from an Arrow table
        gives the types of the token columns it holds and of its data columns; a
        row built from Python values has no Arrow type for a token, so a token
        that is a list is typed from that list
        (``arrow_utils.source_info_arrow_type``, as ``as_table`` types it), a
        string or ``None`` token is a plain string, and the data types come from
        the row's schema. The rows read from one table share one layout object.

        Returns:
            One entry per token column whose type is not ``large_string``, in the
            order of the data columns: the token column, the data column, the
            token type and the data type, as the row holds them. Empty when every
            token is a plain string.
        """
        layout = self._token_layout
        if layout is None:
            layout = self._token_layout = self._read_token_layout()
        return layout

    def _read_token_layout(self) -> "TokenLayout":
        typed = self._typed_source_info
        token_types: "dict[str, pa.DataType]" = {}
        if typed is not None:
            token_types = {
                field.name.removeprefix(constants.SOURCE_PREFIX): field.type
                for field in typed.schema
                if not pa.types.is_large_string(field.type)
            }
        else:
            # Built from values: only a list is not a plain string token.
            token_types = {
                name: arrow_utils.source_info_arrow_type(token)
                for name, token in self._source_info.items()
                if token is not None and not isinstance(token, str)
            }
        if not token_types:
            return ()
        # The schema of the table the row holds, or of its Python schema; no
        # table is built for it.
        table = self._data_table
        data_schema = table.schema if table is not None else self._ensure_arrow_schema()
        return tuple(
            (
                f"{constants.SOURCE_PREFIX}{name}",
                name,
                token_types[name],
                data_schema.field(name).type,
            )
            for name in Datagram.keys(self)
            if name in token_types
        )

    def token_is_null(self, name: str) -> bool:
        """Whether the token of data column ``name`` is null in this row.

        Read from the token the row already holds (a null of a row's token column
        is held as ``None``); no Arrow is read or built. Asked only of a column of
        ``token_layout()``: there a row built from Python values never holds a
        ``None``, which is a plain string token.

        Args:
            name: A data column of ``token_layout()``.

        Returns:
            Whether the token is null.
        """
        return self._source_info.get(name) is None

    def _token_columns(
        self, kept: "Mapping[str, pa.ChunkedArray[Any]]", *, every_column: bool = False
    ) -> "pa.Table":
        """Return this row's token columns, in the order of its data columns.

        A row has a token column for each data column it holds a token for (a
        null token included), as ``as_table(columns={"source": True})`` shows
        them: a row read from a table for every data column, a row built from
        values only for the tokens it was given.

        Args:
            kept: The typed column of each token that is kept as it was read,
                by the name of the data column it now belongs to.
            every_column: Give every data column a column, null when the row holds
                no token for it (the form of ``source_info_table``).

        Returns:
            A one-row table: for a data column in ``kept`` that column, for any
            other the row's token typed from its value.
        """
        arrays: "list[pa.ChunkedArray[Any]]" = []
        fields: "list[pa.Field[Any]]" = []
        for name in Datagram.keys(self):
            column_name = f"{constants.SOURCE_PREFIX}{name}"
            array = kept.get(name)
            if array is None:
                if not every_column and name not in self._source_info:
                    continue
                token = self._source_info.get(name)
                token_type = _source_info_field(column_name, token).type
                array = pa.chunked_array([pa.array([token], type=token_type)])
            arrays.append(array)
            fields.append(pa.field(column_name, array.type))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def _keep_typed_tokens(self, changed: Self, origin: "Mapping[str, str]") -> None:
        """Give ``changed`` the typed columns of the tokens it keeps from this row.

        Does nothing when this row holds no typed columns (it was built from
        Python values): its tokens are then typed from their values anyway.

        Args:
            changed: The row made from this one; its columns and tokens are set.
            origin: For each data column of ``changed`` whose token is this
                row's, unchanged: the name the column has in this row.
        """
        typed = self._typed_source_info
        if typed is None:
            changed._typed_source_info = None
            return
        kept = {
            new: typed.column(f"{constants.SOURCE_PREFIX}{old}")
            for new, old in origin.items()
            if f"{constants.SOURCE_PREFIX}{old}" in typed.column_names
        }
        changed._typed_source_info = changed._token_columns(kept)

    def with_source_info(
        self, **source_info: "SourceInfoValue | pa.Scalar[Any]"
    ) -> Self:
        """Create a copy with updated source-information entries.

        A token given as a Python value is typed from that value: no Arrow type
        exists for it. A token whose column type is known (an operator that
        derives a token from its input's, as ``Index`` does) is given as an
        Arrow scalar of that type, and keeps it (ITL-1491), in its large variant
        and with a ``null`` leaf read as ``large_string``, as a token column read
        from a table is held.

        Args:
            **source_info: The token of each named data column: a Python value,
                or an Arrow scalar that carries the token column's type.

        Returns:
            The copy.
        """
        current = dict(self._source_info)
        typed_given: "dict[str, pa.ChunkedArray[Any]]" = {}
        for key, value in source_info.items():
            if key.startswith(constants.SOURCE_PREFIX):
                key = key.removeprefix(constants.SOURCE_PREFIX)
            if isinstance(value, pa.Scalar):
                # In the large types of a datagram, as a token column read from a
                # table is: a ``null`` leaf is a string's (a stream reads it so,
                # and the instance id cannot hash a ``null`` type).
                typed_given[key] = pa.chunked_array(
                    [
                        pa.array(
                            [value.as_py()],
                            type=arrow_utils.normalize_to_large_types(value.type),
                        )
                    ]
                )
                value = cast("SourceInfoValue", value.as_py())
            current[key] = value
        new_p = self.copy(include_cache=False)
        new_p._source_info = current
        replaced = {key.removeprefix(constants.SOURCE_PREFIX) for key in source_info}
        self._keep_typed_tokens(
            new_p, {k: k for k in Datagram.keys(new_p) if k not in replaced}
        )
        if typed_given:
            kept = dict(typed_given)
            carried = new_p._typed_source_info
            if carried is not None:
                kept |= {
                    name.removeprefix(constants.SOURCE_PREFIX): carried.column(name)
                    for name in carried.column_names
                    if name.removeprefix(constants.SOURCE_PREFIX) not in typed_given
                }
            new_p._typed_source_info = new_p._token_columns(kept)
        return new_p

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

    def keys(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        keys = super().keys(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            keys += tuple(f"{constants.SOURCE_PREFIX}{k}" for k in super().keys())
        return keys

    def schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> Schema:
        schema = dict(super().schema(columns=columns, all_info=all_info))
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            # A token column's Python type follows its Arrow type when the row
            # carries one (ITL-1491), so it agrees with ``arrow_schema``; only a
            # token given as a value is typed from that value.
            typed = self._typed_source_info
            for key in super().keys():
                name = f"{constants.SOURCE_PREFIX}{key}"
                if typed is not None and name in typed.column_names:
                    schema[name] = _token_python_type(typed.schema.field(name).type)
                else:
                    schema[name] = _source_info_python_type(self._source_info.get(key))
        return Schema(schema)

    def arrow_schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Schema":
        schema = super().arrow_schema(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            si_table = self._ensure_source_info_table()
            if si_table.num_columns > 0:
                return arrow_utils.join_arrow_schemas(schema, si_table.schema)
        return schema

    def as_dict(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "dict[str, DataValue]":
        result = super().as_dict(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            for key, value in self.source_info().items():
                result[f"{constants.SOURCE_PREFIX}{key}"] = value
        return result

    def as_table(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Table":
        table = super().as_table(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            si_table = self._ensure_source_info_table()
            if si_table.num_columns > 0 and si_table.num_rows > 0:
                table = arrow_utils.hstack_tables(table, si_table)
        return table

    def select(self, *column_names: str) -> Self:
        """Return a copy holding only the given data columns and their source info.

        The source info is in the order of the columns the copy holds.
        """
        new_p = super().select(*column_names)
        # Datagram.keys: the data columns, also for an EmptyData, whose keys() raises.
        new_p._source_info = {
            k: self._source_info[k]
            for k in Datagram.keys(new_p)
            if k in self._source_info
        }
        new_p._source_info_table = None
        self._keep_typed_tokens(new_p, {k: k for k in Datagram.keys(new_p)})
        return new_p

    def drop(self, *column_names: str, ignore_missing: bool = False) -> Self:
        """Return a copy without the given data columns and their source info."""
        new_p = super().drop(*column_names, ignore_missing=ignore_missing)
        new_p._source_info = {
            k: v for k, v in self._source_info.items() if k not in column_names
        }
        new_p._source_info_table = None
        self._keep_typed_tokens(new_p, {k: k for k in Datagram.keys(new_p)})
        return new_p

    def rename(self, column_mapping: "Mapping[str, str]") -> Self:
        """Return a copy with data columns renamed, each token moving with its column.

        Only the tokens of the columns this row holds are carried over: a token
        held for a name the row had no column of (given earlier through
        ``with_source_info``) is dropped, so it never stands in for the token of
        the column renamed to that name. An empty mapping changes nothing and
        returns this row itself.
        """
        if not column_mapping:
            return self
        new_p = super().rename(column_mapping)
        held = self._names_with_tokens()
        new_p._source_info = {
            column_mapping.get(k, k): self._source_info[k]
            for k in held
            if k in self._source_info
        }
        new_p._source_info_table = None
        self._keep_typed_tokens(new_p, {column_mapping.get(k, k): k for k in held})
        return new_p

    def _names_with_tokens(self) -> tuple[str, ...]:
        """The names whose tokens are this row's own: its data columns, in order."""
        return Datagram.keys(self)

    def with_columns(
        self,
        column_types: Mapping[str, DataType | pa.DataType | pa.Field[Any]]  # pyright: ignore[reportExplicitAny]  # Field is invariant; Any admits every Arrow type
        | None = None,
        **updates: DataValue,
    ) -> Self:
        """Return a copy with new data columns, each with empty source info.

        Args:
            column_types: Optional types for some or all new columns; see
                ``Datagram.with_columns``.
            **updates: New column names mapped to their values.

        Returns:
            A new ``Data`` with the added columns and their source info set
            to ``None``.
        """
        new_p = super().with_columns(column_types=column_types, **updates)
        new_source_info = dict(self._source_info)
        for col in updates:
            new_source_info[col] = None  # new columns get empty source info
        new_p._source_info = new_source_info
        new_p._source_info_table = None
        self._keep_typed_tokens(
            new_p, {k: k for k in Datagram.keys(new_p) if k not in updates}
        )
        return new_p

    def as_datagram(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> Datagram:
        data = self.as_dict(columns=columns, all_info=all_info)
        python_schema = self.schema(columns=columns, all_info=all_info)
        return Datagram(
            data=data, python_schema=python_schema, data_context=self._data_context
        )

    def with_columns_of(self, other: "DataProtocol", names: "Sequence[str]") -> Self:
        """Return a copy with ``other``'s columns ``names`` placed first (ITL-1023).

        The added columns keep ``other``'s Arrow fields, values and source info. This
        data's own columns are kept as they are: their Arrow fields, and the Python
        values a function returned, are not converted again. Used for the columns of
        data-tags that fall through a function pod.

        A data-tag that falls through a function pod keeps its source-info token
        with its type unchanged (developer, November 27, 2026, ITL-1491): the added
        columns' token columns are ``other``'s, never typed again from their
        values. A column a function returns is never a pass-through, even with an
        input's name and value: its token points to the function.

        Args:
            other: The data to take the columns from.
            names: The columns to take, in order.

        Returns:
            The new data, or this data when ``names`` is empty.

        Raises:
            ValueError: If this data already has one of ``names``.
        """
        if not names:
            return self
        if clash := sorted(set(names) & set(self.keys())):
            raise ValueError(f"Columns already exist: {clash}")
        columns = other.as_table().select(list(names))
        other_schema = other.schema()
        other_info = other.source_info()
        new_p = self.copy(include_cache=False)
        new_p._data_table = arrow_utils.hstack_tables(
            columns, self._ensure_data_table()
        )
        if self._data_dict is not None:
            new_p._data_dict = {
                **{name: other[name] for name in names},
                **self._data_dict,
            }
        new_p._data_python_schema = Schema(
            {
                **{name: other_schema[name] for name in names},
                **self._ensure_python_schema(),
            }
        )
        new_p._data_arrow_schema = None
        # The added columns' source info is ``other``'s; an entry this data holds for
        # one of them (it does not hold the column) does not override it.
        # A taken column has a token only when ``other`` shows a token column for
        # it, so the row has the token columns of the same row built by hand.
        theirs = other.as_table(columns={"source": True})
        new_p._source_info = {
            **{
                name: other_info.get(name)
                for name in names
                if f"{constants.SOURCE_PREFIX}{name}" in theirs.column_names
            },
            **{k: v for k, v in self._source_info.items() if k not in names},
        }
        new_p._source_info_table = None
        # The added columns keep the token columns ``other`` carries, with their
        # types (the pipeline determines a token column's type; ITL-1491); this
        # row's own are its typed columns when it has them, and typed from their
        # values otherwise (a function's output, whose tokens are created here).
        kept = {
            name: theirs.column(f"{constants.SOURCE_PREFIX}{name}")
            for name in names
            if f"{constants.SOURCE_PREFIX}{name}" in theirs.column_names
        }
        own = self._typed_source_info
        if own is not None:
            kept |= {
                name.removeprefix(constants.SOURCE_PREFIX): own.column(name)
                for name in own.column_names
                if name.removeprefix(constants.SOURCE_PREFIX) not in names
            }
        new_p._typed_source_info = new_p._token_columns(kept)
        return new_p

    def copy(self, include_cache: bool = True, preserve_id: bool = True) -> Self:
        new_p = super().copy(include_cache=include_cache, preserve_id=preserve_id)
        new_p._source_info = dict(self._source_info)
        new_p._source_info_table = self._source_info_table if include_cache else None
        # Not a cache: it is what the row was read with, and stays true for a copy
        # (every method that changes the columns or the tokens clears it).
        new_p._typed_source_info = self._typed_source_info
        # Read again when asked: the methods that copy go on to change the copy.
        new_p._token_layout = None
        return new_p


# ---------------------------------------------------------------------------
# EmptyData
# ---------------------------------------------------------------------------


class EmptyData(Data):
    """A ``Data``-shaped token representing missing data.

    ``EmptyData`` is produced when an upstream pod's ephemeral result has
    expired or been pruned. It carries all normal datagram metadata (data
    context, record UUID) but has no data payload. Every payload-access
    method raises ``EmptyDataAccessError`` — callers must
    ``isinstance(data, EmptyData)`` before touching columns.

    ``content_hash()`` is overridden to return ``cached_content_hash`` (the
    hash of the **upstream output** — i.e., the data payload this token stands
    in for). Because the downstream node's result cache is keyed by its own
    input (= the upstream output), returning this hash lets
    ``ResultCache.lookup(empty_data)`` work transparently without any changes
    to the cache infrastructure. If ``cached_content_hash`` is ``None`` (a
    pipeline DB row lacking the stored hash column), ``content_hash()`` raises
    ``EmptyDataHashMissingError`` loudly.

    A placeholder carries source-info tokens (ITL-1471): one for each data column
    of the result that is gone, built as that result's would be, with the sentinel
    ``EPHEMERAL_RESULT_GONE`` (``core/row_ids.py``) where the result id would
    stand. A node that receives the placeholder computes the row's instance id
    from them. ``source_info()`` and ``source_info_table()`` return them; they
    are the only part of the placeholder that reads like a row.

    Args:
        cached_content_hash: The content hash of the missing data payload this
            token represents — typically the upstream node's output hash, which
            equals the downstream node's input hash. ``None`` for old-format
            rows lacking the stored hash column.
        source_info: The source-info tokens of the missing result, by data column
            name. ``None`` or empty when none is known.
        python_schema: Optional schema hint (passed to parent).
        data_context: Data context key or instance (passed to parent).
        record_uuid: Optional explicit UUID for this token.
    """

    def __init__(
        self,
        cached_content_hash: ContentHash | None = None,
        source_info: "Mapping[str, SourceInfoValue] | None" = None,
        python_schema: SchemaLike | None = None,
        data_context: str | contexts.DataContext | None = None,
        record_uuid: uuid.UUID | None = None,
    ) -> None:
        # Initialise the parent with an empty dict — no payload columns.
        super().__init__(
            {},
            python_schema=python_schema,
            data_context=data_context,
            record_uuid=record_uuid,
        )
        self._cached_content_hash: ContentHash | None = cached_content_hash
        self._source_info = dict(source_info or {})

    # ------------------------------------------------------------------
    # Content identity
    # ------------------------------------------------------------------

    def content_hash(self, hasher: Any = None) -> ContentHash:
        """Return the cached content hash or raise ``EmptyDataHashMissingError``.

        The returned hash is the hash of the **missing payload** this token
        stands in for — typically the upstream node's output hash (stored as
        ``OUTPUT_DATA_HASH_COL`` in the pipeline DB). This equals the
        downstream node's input hash, so ``ResultCache.lookup(empty_data)``
        finds the correct cached result transparently without touching the
        missing data.

        Args:
            hasher: Ignored — ``EmptyData`` uses the stored hash directly.

        Returns:
            The ``cached_content_hash`` set at construction.

        Raises:
            EmptyDataHashMissingError: If ``cached_content_hash`` is ``None``.
        """
        from orcapod.errors import EmptyDataHashMissingError

        if self._cached_content_hash is None:
            raise EmptyDataHashMissingError(self)
        return self._cached_content_hash

    def identity_structure(self) -> Any:
        """Always raises ``EmptyDataAccessError`` — no payload to hash."""
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "identity_structure")

    # ------------------------------------------------------------------
    # Payload-access overrides — all raise EmptyDataAccessError
    # ------------------------------------------------------------------

    def as_dict(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> dict[str, DataValue]:
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "as_dict")

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "as_table")

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "keys")

    def schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> Schema:
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "schema")

    def arrow_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Schema:
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "arrow_schema")

    def _names_with_tokens(self) -> tuple[str, ...]:
        """The names of the tokens: a placeholder holds no column, only tokens."""
        return tuple(self._source_info)

    # ------------------------------------------------------------------
    # Copy — preserves EmptyData-specific fields
    # ------------------------------------------------------------------

    def copy(self, include_cache: bool = True, preserve_id: bool = True) -> Self:
        """Return a shallow copy of this token, preserving EmptyData-specific fields.

        Overrides ``Datagram.copy()`` so that ``_cached_content_hash`` is carried
        across (the base implementation uses ``object.__new__`` and only copies
        ``Datagram`` fields, which would silently drop the extra attribute). The
        tokens are copied by ``Data.copy``.

        Args:
            include_cache: Forwarded to ``super().copy()``.
            preserve_id: Forwarded to ``super().copy()``.

        Returns:
            A new ``EmptyData`` instance with all fields copied.
        """
        new_p = super().copy(include_cache=include_cache, preserve_id=preserve_id)
        new_p._cached_content_hash = self._cached_content_hash
        return new_p

    # ------------------------------------------------------------------
    # Read-only accessors
    # ------------------------------------------------------------------

    @property
    def cached_content_hash(self) -> ContentHash | None:
        """The stored content hash, or ``None`` if absent (old-format row)."""
        return self._cached_content_hash

    def token_layout(self) -> "TokenLayout":
        """Return no entry: a placeholder holds no data column for a token to fit."""
        return ()

    def source_info(self) -> "dict[str, SourceInfoValue]":
        """Return the tokens the placeholder carries, by data column name."""
        return dict(self._source_info)

    def source_info_table(self) -> pa.Table:
        """Return the tokens as a one-row table of source-info columns.

        The columns are typed as a row's own source-info columns are
        (``_source_info_field``), in the order the tokens were given.

        Returns:
            The one-row table, or a table with no column when the placeholder
            carries no token.
        """
        if not self._source_info:
            return pa.table({})
        prefixed = {
            f"{constants.SOURCE_PREFIX}{k}": v for k, v in self._source_info.items()
        }
        return pa.Table.from_pylist(
            [prefixed],
            schema=pa.schema([_source_info_field(k, v) for k, v in prefixed.items()]),
        )
