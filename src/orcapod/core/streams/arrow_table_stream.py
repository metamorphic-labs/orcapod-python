from __future__ import annotations

import itertools
import logging
from collections.abc import Collection, Iterator
from typing import Any

import pyarrow as pa

from orcapod.hashing.orcapod_table import OrcapodTable
from orcapod import contexts
from orcapod.core.datagrams import Data, Tag
from orcapod.core.streams.base import (
    ZERO_ROW_INPUT_COLUMNS,
    StreamBase,
    check_head_size,
)
from orcapod.core.streams.view import StreamView
from orcapod.protocols.core_protocols import PodProtocol, StreamProtocol, TagProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema, SourceInfoValue
from orcapod.semantic_types.narrow_float_keys import (
    require_schema_without_narrow_float_keys,
)
from orcapod.utils import arrow_utils, schema_utils

logger = logging.getLogger(__name__)


class ArrowTableStream(StreamBase):
    """
    An immutable stream based on a PyArrow Table.
    This stream is designed to be used with data that is already in a tabular format,
    such as data loaded from a file or database. The columns to be treated as tags are
    specified at initialization, and the rest of the columns are treated as data.
    The stream is immutable, meaning that once it is created, it cannot be modified.
    This is useful for ensuring that the data in the stream remains consistent and unchanging.

    The types of the tag and data columns are inferred from the PyArrow Table schema.

    Every column takes the large variant of its Arrow type (developer, November 14,
    2026, ITL-1122): ``string`` is ``large_string``, ``binary`` is ``large_binary``,
    ``list`` is ``large_list``, recursively. This is the one place a table becomes a
    stream, so every source (each builds its stream here, from a user's Arrow table
    too) and every operator result presents large types. Width, nullability, field
    metadata and logical types are kept; large and non-large variants hash identically.
    """

    # The table's rows are held, not computed: a run() view says "all N rows", as
    # head() does.
    _RUN_ORIGIN: str = "held"

    def __init__(
        self,
        table: "pa.Table",
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        source_info: dict[str, SourceInfoValue] | None = None,
        producer: PodProtocol | None = None,
        upstreams: tuple[StreamProtocol, ...] = (),
        data_tags: Collection[str] = (),
        **kwargs: Any,
    ) -> None:
        """Wrap ``table`` as a stream.

        Args:
            table: The rows: tag, data, system-tag and source-info columns.
            tag_columns: The user tag columns.
            system_tag_columns: Columns to rename into system tags.
            source_info: Source-info tokens to add, by column.
            producer: The pod that produced the stream, if any.
            upstreams: The streams the producer read.
            data_tags: The tag columns that are data-tags: each is also a data
                column of the same name, stored once (specification "Referential
                Tags", section 4). Must be a subset of ``tag_columns``.
            **kwargs: Passed to ``StreamBase``.

        Raises:
            ValueError: If a tag column or a data-tag is not a tag column of
                the table, or the stream has no data column.
            InputValidationError: If a column's type is, or contains, a
                dictionary: dictionary-encoded columns are not currently supported
                (ITL-1264, ITL-1277).
        """
        super().__init__(**kwargs)

        self._producer = producer
        self._upstreams = upstreams

        # Every table Orcapod presents is in its large variants (ITL-1122).
        table = arrow_utils.normalize_table_to_large_types(table, keep_null=True)
        # Dictionary-encoded columns are not currently supported (ITL-1264,
        # ITL-1277): every source and operator result becomes a stream here, so this
        # refuses them all, tags and data alike.
        if found := schema_utils.dictionary_columns(table.schema):
            raise schema_utils.dictionary_columns_error(
                found, tag_columns, data_tags, system_tag_columns
            )
        # Nor are columns of a type the schema hash rejects (a union, a list view, a
        # run-end-encoded type; developer, November 21, 2026, ITL-640): a stream's
        # identity is its Arrow schema hash, which cannot hold them.
        if found := schema_utils.schema_hash_rejected_columns(table.schema):
            raise schema_utils.schema_hash_rejected_columns_error(
                found, tag_columns, data_tags, system_tag_columns
            )

        # Nor is a dict keyed by, or a set of, float32 or float16 (developer, November
        # 28, 2026, ITL-1591): two distinct floats can narrow to one stored value, so
        # an entry would be lost. A check of types, as the two above.
        require_schema_without_narrow_float_keys(table.schema)

        data_table, data_context_table = arrow_utils.split_by_column_groups(
            table, [constants.CONTEXT_KEY]
        )
        if data_table is None:
            # TODO: provide better error message
            raise ValueError(
                "Table must contain at least one column to be used as a stream."
            )
        table = data_table

        if data_context_table is None:
            data_context_table = pa.table(
                {
                    constants.CONTEXT_KEY: pa.array(
                        [contexts.get_default_context_key()] * len(table),
                        pa.large_string(),
                    )
                },
                schema=pa.schema(
                    [pa.field(constants.CONTEXT_KEY, pa.large_string(), nullable=False)]
                ),
            )

        prefix_info = {constants.SOURCE_PREFIX: source_info}

        # A tag never has a meta column's name (ITL-1080).
        schema_utils.require_names_not_meta(tag_columns)
        # determine tag columns first and then exclude any source info
        self._tag_columns = tuple(c for c in tag_columns if c in table.column_names)
        self._system_tag_columns = tuple(
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        if len(system_tag_columns) > 0:
            # rename system_tag_columns
            column_name_map = {
                c: f"{constants.SYSTEM_TAG_PREFIX}{c}" for c in system_tag_columns
            }
            table = table.rename_columns(
                [column_name_map.get(c, c) for c in table.column_names]
            )

            self._system_tag_columns += tuple(
                f"{constants.SYSTEM_TAG_PREFIX}{c}" for c in system_tag_columns
            )

        self._all_tag_columns = self._tag_columns + self._system_tag_columns
        if delta := set(tag_columns) - set(self._tag_columns):
            raise ValueError(
                f"Specified tag columns {delta} are not present in the table."
            )
        if unknown := set(data_tags) - set(self._tag_columns):
            raise ValueError(
                f"Data-tags {sorted(unknown)} are not tag columns of the stream."
            )
        # A data-tag is one column in both roles: a tag, and a data column of
        # the same name (tag order).
        self._data_tags = tuple(c for c in self._tag_columns if c in set(data_tags))
        tag_only = tuple(c for c in self._all_tag_columns if c not in self._data_tags)
        table, prefix_tables = arrow_utils.prepare_prefixed_columns(
            table,
            prefix_info,
            exclude_columns=tag_only,
        )
        # now table should only contain tag columns and data columns
        # Data-tags come first among the data columns, in tag order, then the other
        # data columns in table order (developer decision, November 17, 2026,
        # ITL-1205): a table built from rows lays a data-tag out among the tags, so
        # this is the order of every row, on every path. Only the keys follow it: the
        # table keeps its column order, and no stream, schema, row or table hash
        # depends on the order of a stream's columns.
        self._data_columns = self._data_tags + tuple(
            c
            for c in table.column_names
            if c not in tag_only and c not in self._data_tags
        )
        # No user column name holds the source-info separator (ITL-1154).
        schema_utils.require_names_without_separator(
            (*self._tag_columns, *self._data_columns)
        )
        self._table = table
        self._source_info_table = prefix_tables[constants.SOURCE_PREFIX]
        self._data_context_table = data_context_table

        if len(self._data_columns) == 0:
            raise ValueError(
                "No data columns found in the table. At least one data column is required."
            )

        # Respect Arrow nullable flags as-is (after optional inference above):
        # nullable=True → T | None, nullable=False → T.
        # The tag schema lists the tags in tag order, as ``keys()`` does and as the
        # data schema follows ``_data_columns``; never in table layout (ITL-1365).
        # A declared name the table does not hold (renamed into a system tag, or a
        # system tag that is not there) is left out, as before.
        names = set(self._table.column_names)
        tag_schema = pa.schema(
            self._table.schema.field(name)
            for name in dict.fromkeys(self._tag_columns)
            if name in names
        )
        system_tag_schema = pa.schema(
            self._table.schema.field(name)
            for name in dict.fromkeys(self._system_tag_columns)
            if name in names
        )
        all_tag_schema = arrow_utils.join_arrow_schemas(tag_schema, system_tag_schema)
        data_schema = pa.schema(
            self._table.schema.field(name) for name in self._data_columns
        )

        self._tag_schema = tag_schema
        self._system_tag_schema = system_tag_schema
        self._all_tag_schema = all_tag_schema
        self._data_schema = data_schema

        self._cached_elements: list[tuple[TagProtocol, Data]] | None = None
        self._update_modified_time()  # set modified time to now

    def identity_structure(self) -> Any:
        if self._producer is not None:
            return super().identity_structure()
        # A raw stream (no producer) is its table, its tag columns and its data-tags,
        # none of them in an order the input chose (developer, November 18 and 19,
        # 2026, ITL-1227): the table in its canonical row order (``_identity_table``),
        # hashed whole by the Arrow table handler, and the names as sorted tuples.
        return (
            self.__class__.__name__,
            # A table Orcapod built: hashed by ``hash_table`` (Orcapod's float rule),
            # never as a pyarrow value (ITL-1179).
            OrcapodTable(self._identity_table()),
            tuple(sorted(self._tag_columns)),
        ) + (
            # Only when there is one: a plain stream keeps its identity, and a stream
            # with data-tags and a plain one over one table differ (ITL-1023).
            (("data_tags", tuple(sorted(self._data_tags))),) if self._data_tags else ()
        )

    @property
    def data_tags(self) -> tuple[str, ...]:
        """The data-tags: tag columns that are also data columns (tag order)."""
        return self._data_tags

    def _identity_table(self) -> "pa.Table":
        """Return the table ``identity_structure`` hashes, its rows in canonical order.

        Developer decisions, November 18 and 19, 2026 (ITL-1227): a raw stream's identity
        depends on neither the order of its rows nor that of its columns: "take the
        data, order it as we had agreed and then hash the entirety of the arrow table".
        The table holds every column group but the per-row content hash: user tags,
        system tags, data, source info, data context and meta columns. Its rows are put
        in the canonical row order of an identity table
        (``arrow_utils.canonical_identity_row_order``: a value sort over a closed list
        of column types, ties settled by each tied row's own hash), and the table is
        hashed whole, once, by the Arrow table handler (``hash_table``, ``arrow_v1``),
        which covers its schema and ignores column order: a stream with no rows still
        commits to its columns. A row that occurs twice is there twice. There is no
        special case for no rows, no tag column, tied tags, list tags or library values.

        THIS RULE IS A HASH INPUT AND MUST NOT CHANGE.

        Returns:
            The identity table in canonical row order.
        """
        table = self._assemble_table(
            ColumnConfig(meta=True, context=True, source=True, system_tags=True),
            limit=None,
        )
        return arrow_utils.canonical_identity_row_order(
            table, self._tag_columns, self.data_context.arrow_hasher
        )

    def pipeline_identity_structure(self) -> Any:
        if self._producer is None or not isinstance(
            self._producer, PipelineElementProtocol
        ):
            # A stream with no pipeline producer is a root: the Arrow schemas of its
            # Python output schema's columns, from the table it holds (ITL-640).
            from orcapod.utils.schema_utils import arrow_schema_identity

            tag_schema, data_schema = self.output_schema()
            held = self._table.schema
            return arrow_schema_identity(
                pa.schema([held.field(name) for name in tag_schema]),
                pa.schema([held.field(name) for name in data_schema]),
                self.data_context,
            )
        return super().pipeline_identity_structure()

    @property
    def producer(self) -> PodProtocol | None:
        return self._producer

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return self._upstreams

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """
        Returns the keys of the tag and data columns in the stream.
        This is useful for accessing the columns in the stream.
        """
        tag_columns = self._tag_columns
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
        # TODO: add standard parsing of columns
        if columns_config.system_tags:
            tag_columns += self._system_tag_columns
        return tag_columns, self._data_columns

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        Returns the types of the tag and data columns in the stream.
        This is useful for accessing the types of the columns in the stream.
        """
        # normalize column config
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
        # TODO: consider using MappingProxyType to avoid copying the dicts
        converter = self.data_context.type_converter
        if columns_config.system_tags:
            tag_schema = self._all_tag_schema
        else:
            tag_schema = self._tag_schema
        return (
            converter.arrow_schema_to_python_schema(tag_schema),
            converter.arrow_schema_to_python_schema(self._data_schema),
        )

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """
        Returns the underlying table representation of the stream.
        This is useful for converting the stream to a table format.
        """
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
        return self._assemble_table(columns_config, limit=None)

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first ``n`` rows of the table this stream holds.

        Without sorting only those rows are assembled: content hashes, when asked for,
        are computed for them alone. With ``sort_by_tags`` (or ``all_info``) the rows are
        sorted first, as ``as_table`` sorts them, and the first ``n`` of those are
        returned, so the head is always ``as_table(...)`` cut to ``n`` rows. See
        ``StreamBase.head``.
        """
        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        if config.sort_by_tags:
            table = self._assemble_table(config, limit=None).slice(0, n)
        else:
            table = self._assemble_table(config, limit=n)
        return StreamView.of_rows(
            self,
            table,
            tag_columns=self.keys(columns=config)[0],
            total_rows=self._table.num_rows,
        )

    def _typed_zero_row_table(self) -> "pa.Table":
        """This stream's table cut to no rows: its own Arrow types, no row assembled."""
        return self._assemble_table(ZERO_ROW_INPUT_COLUMNS, limit=0)

    def _assemble_table(
        self, columns_config: ColumnConfig, limit: int | None
    ) -> "pa.Table":
        """Return the stream's rows with the column groups of ``columns_config``.

        Args:
            columns_config: The column groups to include, and whether to sort by tags.
            limit: Assemble only the first ``limit`` rows; ``None`` for all of them.
        """
        output_table = self._table
        data_context_table = self._data_context_table
        source_info_table = self._source_info_table
        if limit is not None:
            output_table = output_table.slice(0, limit)
            data_context_table = data_context_table.slice(0, limit)
            source_info_table = source_info_table.slice(0, limit)

        def hashes() -> pa.Array[Any]:
            rows = self.iter_data()
            if limit is not None:
                rows = itertools.islice(rows, limit)
            return pa.array(
                [str(data.content_hash()) for _, data in rows], type=pa.large_string()
            )

        def shape(carried: "pa.Table") -> "pa.Table":
            table_stack = (carried,)
            if columns_config.context:
                table_stack += (data_context_table,)
            if columns_config.source:
                table_stack += (source_info_table,)
            table = arrow_utils.hstack_tables(*table_stack)
            if columns_config.sort_by_tags and (
                self._tag_columns or columns_config.system_tags
            ):
                # The presentation sort (the identity has no row order, see
                # ``_identity_table``). A stream with no tag column is sorted only when
                # the system tags are asked for. The system tags stay in the table
                # through the sort: they break ties between equal tags.
                table = arrow_utils.sort_by_tag_columns(table, tuple(self._tag_columns))
            if not columns_config.system_tags:
                table = table.drop_columns(list(self._system_tag_columns))
            return table

        return arrow_utils.with_content_hash_column(
            output_table,
            columns_config.content_hash,
            hashes,
            shape,
            nullable=False,
        )

    def clear_cache(self) -> None:
        """
        Resets the cached elements of the stream.
        This is useful for re-iterating over the stream.
        """
        self._cached_elements = None

    def iter_data(self) -> Iterator[tuple[TagProtocol, Data]]:
        """Iterates over the rows of the stream as ``(Tag, Data)`` pairs.

        Every row of the backing table is yielded exactly once, in table order,
        regardless of how the tag, data and source-info columns are chunked.

        Yields:
            A ``(Tag, Data)`` tuple for each row.
        """
        # TODO: make it work with table batch stream
        if self._cached_elements is None:
            cached_elements = []
            tag_columns = list(self._all_tag_columns)
            # Each row's source info goes to ``Data`` as Arrow columns, so it
            # keeps the type it has here: re-typing it from Python values
            # (``to_pylist``) would infer a type the stream already knows.
            source_columns = list(self._source_info_table.column_names)
            data_columns = list(self._data_columns) + source_columns

            # The source-info columns go beside the data columns they describe: the
            # rows of this one table are read together, however each column is
            # chunked (``rows_of`` reads batch by batch, cut at every column's chunk
            # boundaries, so no row is dropped or paired with another's).
            rows = self._table
            for name in source_columns:
                rows = rows.append_column(
                    self._source_info_table.schema.field(name),
                    self._source_info_table.column(name),
                )
            # The table is prepared once for all of its rows (ITL-1348): building
            # ``Tag(row)`` and ``Data(row)`` from one-row slices normalized every row
            # three times and split its columns again.
            tags = (
                Tag.rows_of(rows.select(tag_columns), self.data_context)
                if tag_columns
                else None
            )
            for data in Data.rows_of(rows.select(data_columns), self.data_context):
                # A fresh empty tag per row, so rows never share one object.
                tag = next(tags) if tags is not None else Tag({})

                yield tag, data

                cached_elements.append((tag, data))
            self._cached_elements = cached_elements
        else:
            yield from self._cached_elements

    def __repr__(self) -> str:
        refs = f", data_tags={self._data_tags}" if self._data_tags else ""
        return (
            f"{self.__class__.__name__}(table={self._table.column_names}, "
            f"tag_columns={self._tag_columns}{refs})"
        )
