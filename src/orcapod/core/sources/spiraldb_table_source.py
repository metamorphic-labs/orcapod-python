"""SpiralDBTableSource — a read-only RootSource that streams a SpiralDB table.

The source is built from the table's metadata alone (schema, key columns, SpiralDB's
table id): no row is read until one is asked for. Rows then arrive in batches, so a
read holds one of SpiralDB's batches at a time whatever the table's size. The table's
primary-key (key-schema) columns are the tag columns by default.

Requires the ``spiraldb`` optional extra: ``pip install orcapod[spiraldb]``.
Authentication is spiral-sdk's: ``SPIRAL_*`` environment variables, or the credentials
``spiral login`` saved.
The SDK client is reused for reads and freshness checks, including on restricted copies.
Use a new source after changing the SDK endpoint, credentials or options. Use the source
as a context manager or call ``close()`` after its reads and jobs have finished.

Example::

    # Default dataset, PK columns become tag columns automatically
    source = SpiralDBTableSource("my-project-123456", "spike_event", limit=1000)

    # Only the rows of one unit
    source = SpiralDBTableSource(
        "my-project-123456", "spike_event", where={"unit_id": 106}
    )

    # The same restriction added to a built source: a new source, same identity
    source = SpiralDBTableSource("my-project-123456", "spike_event").where(unit_id=106)

    # Only a window of a column, half-open: col("timestamp") >= start and < stop
    source = SpiralDBTableSource("my-project-123456", "spike_event").where(
        col("timestamp") >= start, col("timestamp") < stop
    )

    # The spike time is a key column (a tag); also offer it as data
    source = SpiralDBTableSource(
        "my-project-123456", "spike_event", tags_as_data={"timestamp": "spike_time_us"}
    )

    # Explicit dataset, tag columns overridden
    source = SpiralDBTableSource(
        "my-project-123456", "spike_event", dataset="prod", tag_columns=["unit_id"]
    )
"""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import os
from collections.abc import AsyncIterator, Collection, Generator, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from types import TracebackType
from typing import Any, ClassVar, NamedTuple, TYPE_CHECKING

import pyarrow as pa
from typing_extensions import override

from orcapod.core.sources.base import RootSource, saved_tag_only_columns
from orcapod.core.data_tags import TagOnlyColumns, source_data_tags
from orcapod.core.sources.source_ids import joined_location, location_source_name
from orcapod.core.sources.derived_source import validate_tags_as_data
from orcapod.core.sources.stream_builder import (
    require_custom_system_tags,
    require_supported_nullable_types,
    SourceStreamBuilder,
    key_column_type_allowed,
    normalize_record_id_columns,
    row_identifiers,
)
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import check_head_size
from orcapod.core.streams.view import ViewStatus, StreamView
from orcapod.databases.spiraldb_connector import (
    SpiralDBConnector,
    SpiralDBTable,
    SpiralDBTableNotFoundError,
    require_same_process,
)
from orcapod.errors import InputValidationError, RowIdentifierError, SourceTooLargeError
from orcapod.types import ColumnConfig, Condition
from orcapod.types.condition import check_float_equalities, check_restriction
from orcapod.utils import arrow_utils
from orcapod.utils.schema_utils import _normalize_column_list

if TYPE_CHECKING:
    from orcapod import contexts
    from orcapod.config import OrcapodConfig
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol

DEFAULT_MAX_ROWS = 100_000
DEFAULT_BATCH_ROWS = 8_192
# The most rows a notebook display of the source reads.
PREVIEW_ROWS = 10


def _is_positive_int(value: object) -> bool:
    """Return whether ``value`` is an integer of at least 1 (``True`` is not)."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


class _Head(NamedTuple):
    """The first rows of a source, and what is known of how many rows it reads.

    Attributes:
        rows: The rows read; zero rows (the source's columns) when ``rows_read`` is
            false.
        rows_read: Whether rows were read at all.
        total_rows: The rows the source reads or, as an estimate, the table's row count;
            ``None`` when it is above the bound that was counted.
        total_is_estimate: Whether ``total_rows`` is the table's row count rather than
            that of the rows the source reads.
        counted_to: With no ``total_rows``, the rows the table has at least.
    """

    rows: "pa.Table"
    rows_read: bool
    total_rows: int | None
    total_is_estimate: bool
    counted_to: int = 0


def _reads_source_info(config: ColumnConfig) -> bool:
    """Whether a table with the column groups of ``config`` needs source-info tokens.

    The tokens are one string per row and data column, many times the size of the
    rows they describe (ITL-1464), so a read builds them only when its result shows
    them (``source``) or hashes each row (``content_hash``: a row's hash is computed
    from the row as the stream holds it).

    Args:
        config: The normalized column configuration of the read.

    Returns:
        True when the tokens must be built.
    """
    return bool(config.source or config.content_hash)


def _schema_difference(found: "pa.Schema", expected: "pa.Schema") -> str:
    """Describe how the table's columns as found differ from the ones the source expects."""
    if found.names != expected.names:
        return (
            f"the table has columns {found.names} but the source was built with "
            f"{expected.names}"
        )
    for got, want in zip(found, expected, strict=True):
        if not got.equals(want, check_metadata=True):
            return (
                f"the table has column {got.name!r} as {got} with metadata "
                f"{got.metadata} but the source was built with {want} with metadata "
                f"{want.metadata}"
            )
    return (
        f"the table has schema metadata {found.metadata} but the source was built "
        f"with {expected.metadata}"
    )


SPIRALDB_SOURCE_ID_HAS_TABLE_ID = True
"""Whether a SpiralDB default source id ends in ``@<table id>`` (ITL-1154).

The developer decides whether the table id stays part of the default; it is one
constant so the change is one line.
"""


def spiraldb_table_address(
    project_id: str, dataset: str, table_name: str, table_id: str
) -> str:
    """Return a SpiralDB table's address: ``<project>.<dataset>.<table>[@<table id>]``.

    ``<project>.<dataset>.<table>`` is Spiral's own full table identifier
    (``Table.identifier``); the table id follows when
    ``SPIRALDB_SOURCE_ID_HAS_TABLE_ID`` is set.

    Args:
        project_id: The Spiral project.
        dataset: The dataset.
        table_name: The table's name.
        table_id: SpiralDB's id of the table.

    Returns:
        The address, the location part of the default source id.

    Raises:
        ValueError: If a part contains its separator (``.`` in a name, ``@`` in
            the address or the table id), so the parts could not be told apart;
            give the source a ``source_id``.
    """
    address = joined_location(".", project_id, dataset, table_name)
    if SPIRALDB_SOURCE_ID_HAS_TABLE_ID:
        address = joined_location("@", address, table_id)
    return address


class SpiralDBTableSource(RootSource):
    """A read-only Source over a table in a SpiralDB dataset, read in batches.

    Building the source reads the table's metadata only: that the table exists, its
    key columns, its schema (with the table's row in the dataset's schema table applied) and
    SpiralDB's own id of the table. No row is read. ``output_schema``, ``keys``,
    ``content_hash``, ``pipeline_hash`` and ``to_config`` never read a row either.

    Rows are read when asked for (``iter_data``, ``async_iter_data``, ``as_table`` and
    the dataframe methods). Each read is one scan of the table in key order. SpiralDB
    sends it in batches whose sizes follow its stored fragments (they cannot be
    chosen); the source hands each on in slices of at most ``batch_rows`` rows, so
    iterating holds one SpiralDB batch, and the slice being processed, at a time.
    ``as_table`` and the dataframe methods hold every row they return. Nothing is
    cached: every read scans again.

    Display. ``head(n)`` and the notebook display (its ``head()``) read only the first
    ``n`` rows (``PREVIEW_ROWS``, 10, by default), in one scan to which ``max_rows`` does
    not apply, and the header says how many rows the table has.

    Identity. The source is identified by where the table is, never by its rows. The
    default ``source_id`` is ``spiraldb_table_v1:<project_id>.<dataset>.<table_name>@<table
    id>`` (the name from ``spiraldb_table_address`` after the kind part, ITL-1425): Spiral's own table identifier, then the table id,
    SpiralDB's own, so a table dropped and created again under the same name is a
    different source, and a renamed table gets a new source id too. A row is
    identified by its primary key (or ``record_id_columns``) in the JSON key form.
    ``where``, ``limit`` and ``asof`` choose which rows are read and are not part of
    the source's identity. ``tags_as_data`` is: it decides what a data column holds,
    so its sorted items enter the content hash (when it is given), and its copies
    are columns of the schema, so they enter both the content and the pipeline hash.
    The source id and every row's record ID do not depend on it.

    Key columns as data. A function pod never sees tags, so a key it needs (a
    spike's time) is offered as data too with ``tags_as_data={"timestamp":
    "spike_time_us"}``. The tag keeps its name; the copy is a data column of the
    given name, after the table's own data columns in mapping order, with the key
    column's Arrow type and field metadata, non-nullable (a key is never null). It is
    copied in Arrow as each batch is read, and its source-info value has the form of
    every data column's, ``<source id>::<row token>::<copy name>``.

    Record identity. A row is identified by its primary key: the row's token is the
    data context's Arrow hash of its key columns (in key order, as non-nullable
    fields), written with its algorithm label. A row therefore has the same record ID
    whichever read returned it, and keeps it when other rows are added.

    Args:
        project_id: SpiralDB project identifier (e.g. ``"my-project-123456"``).
        table_name: Name of the SpiralDB table to expose as a source.
        dataset: Dataset within the project. Defaults to ``"default"``.
        tag_columns: Columns to use as tag columns. A bare string is accepted as a
            single column name. If ``None`` (default), the table's primary-key
            (key-schema) columns are used.
        system_tag_columns: Additional system-level tag columns.
        where: Column name to value, or a ``Condition`` (``orcapod.types``) of
            equalities and ranges. Only the rows that meet it are read; SpiralDB
            applies it before any row is transferred. Values follow the same type
            and exactness rules as ``where()``.
        limit: Read at most this many rows: the first in key order (after ``where``).
        asof: Read the table as of this version (microseconds, as SpiralDB's
            transaction timestamps). If ``None`` (default), the version the table has
            when the source is built is used for every read of this source object.
        max_rows: The most rows one read may return (default 100,000); ``None``
            removes the cap. A read that would return more raises
            ``SourceTooLargeError``. A pipeline job keeps every row it processes in
            memory, about 5 KB per row and node.
        batch_rows: The most rows handed on at once while iterating (default 8,192).
            A SpiralDB batch larger than this is held while its slices are handed on.
        source_id: The source's name; its id is ``spiraldb_table_v1:<name>``
            (ITL-1425). The name defaults to
            ``<project_id>.<dataset>.<table_name>@<table id>``.
        label: Human-readable label for this source node.
        data_context: Data context governing type conversion and hashing.
        config: OrcaPod configuration.
        overrides: Optional: ``{"server.url": "https://api.spiraldb.dev"}``, the
            address of the service, passed to ``SpiralDBConnector``.
        tag_only_columns: Tag columns kept as tags only, not data, exactly as in
            v0.3; ``True`` for all of them. Keyword-only. Every other tag column is
            a data-tag: also a data column of the same name (ITL-1023).
        tags_as_data: Maps a key column that is a tag column of this source to the
            name of a data column that receives a copy of it, as in
            ``DerivedSource``. ``None`` (default) or an empty mapping copies nothing.

    Raises:
        ValueError: If the table is not found, has no key schema, a tag column or a
            ``where`` column is not in the table, a ``where`` value is ``None``, or
            ``limit``, ``max_rows`` or ``batch_rows`` is not a positive integer.
        TypeError: If a ``where`` value has the wrong Python type, cannot be held
            exactly by the column's Arrow type, or uses an unsupported condition.
            Float columns without a logical type also take exactly representable
            Python ints. The refusal of inexact float equalities holds for now
            and will be revisited in ITL-1480.
        InputValidationError: If ``tags_as_data`` names a column that is not a tag
            column or not a key column, or a new name that is already a column, is
            given twice, or is reserved for system columns.
    """

    SOURCE_KIND: ClassVar[str] = "spiraldb_table"
    """Config type key and the kind in the source id's kind part (one vocabulary)."""
    SOURCE_VERSION: ClassVar[int] = 1
    """The version of this kind's implementation, in the kind part (ITL-1425)."""

    DISPLAY_PARTS: ClassVar[tuple[str, str]] = ("_dataset", "_project_id")
    TABLE_IS_ROWS: ClassVar[bool] = True
    TAKES_TAG_ONLY_COLUMNS: ClassVar[bool] = True

    # Whether a notebook display reads the first rows. False shows the schema and the
    # table's recorded row count only, with no scan (kept for the ITL-991 ruling on what
    # a preview may read).
    _preview_reads_rows: bool = True

    def __init__(
        self,
        project_id: str,
        table_name: str,
        dataset: str = "default",
        tag_columns: str | Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        where: "Mapping[str, object] | Condition | None" = None,
        limit: int | None = None,
        asof: int | None = None,
        max_rows: int | None = DEFAULT_MAX_ROWS,
        batch_rows: int = DEFAULT_BATCH_ROWS,
        source_id: str | None = None,
        label: str | None = None,
        data_context: "str | contexts.DataContext | None" = None,
        config: "OrcapodConfig | None" = None,
        overrides: dict[str, str] | None = None,
        tags_as_data: Mapping[str, str] | None = None,
        *,
        tag_only_columns: TagOnlyColumns = (),
        record_id_columns: str | Collection[str] | None = None,
    ) -> None:
        given_key = normalize_record_id_columns(record_id_columns)
        if given_key == ():
            raise ValueError(
                "a SpiralDB table's scan order does not identify a row, so "
                "record_id_columns=() (the position) is not offered: name key "
                "columns, or leave record_id_columns unset for the primary key."
            )
        for name, value in (("limit", limit), ("max_rows", max_rows)):
            if value is not None and not _is_positive_int(value):
                raise ValueError(
                    f"{name} must be a positive integer or None, got {value!r}"
                )
        # What needs no table (the values' types, None) is checked before connecting.
        if isinstance(where, Condition):
            requested = where
        else:
            requested = Condition.of(where) if where else Condition()
        if not _is_positive_int(batch_rows):
            raise ValueError(
                f"batch_rows must be a positive integer, got {batch_rows!r}"
            )

        super().__init__(
            source_id=source_id, label=label, data_context=data_context, config=config
        )
        self._project_id = project_id
        self._dataset = dataset
        self._table_name = table_name
        self._overrides = None if overrides is None else dict(overrides)
        self._connection: SpiralDBConnector | None = None
        self._connection_lock = Lock()
        self._pid = os.getpid()
        self._closed = False
        self._condition = Condition()
        self._limit = limit
        self._max_rows = max_rows
        self._batch_rows = batch_rows
        self._given_asof = asof
        self._tags_as_data: dict[str, str] = dict(tags_as_data) if tags_as_data else {}
        location = f"SpiralDB project {project_id!r}, dataset {dataset!r}"

        try:
            try:
                table = self._connector().lookup_table(table_name)
            except SpiralDBTableNotFoundError as exc:
                raise ValueError(
                    f"{location}: Table {table_name!r} not found in database."
                ) from exc
            primary_key = tuple(table.key_columns)
            self._key_columns: tuple[str, ...] = (
                given_key if given_key is not None else primary_key
            )
            if not self._key_columns:
                raise ValueError(
                    f"{location}: Table {table_name!r} has no primary key columns, so "
                    "its rows cannot be identified: name key columns with "
                    "record_id_columns=[...]."
                )
            self._table_id = table.table_id
            schema = table.arrow_schema()
            require_supported_nullable_types(schema)
            # One version for every read of this source object. A given version is
            # used as it is; a table that was never written has no version to pin.
            self._asof = asof if asof is not None else (table.version() or None)

            # The default tags are the primary key, whatever identifies the rows.
            resolved_tags = (
                list(primary_key or self._key_columns)
                if tag_columns is None
                else _normalize_column_list(tag_columns)
            )
            key_problems = [
                f"{c!r} is not a column"
                if c not in schema.names
                else f"{c!r} has type {schema.field(c).type}"
                for c in self._key_columns
                if c not in schema.names
                or not key_column_type_allowed(schema.field(c).type)
            ]
            if key_problems:
                raise ValueError(
                    f"{location}: table {table_name!r} cannot identify its rows by key "
                    f"columns {list(self._key_columns)}: {'; '.join(key_problems)}. A key "
                    "column must hold strings, integers, booleans, timestamps or dates."
                )
            self._record_id_columns = given_key
            if self._source_name is None:
                self._source_name = location_source_name(
                    spiraldb_table_address(
                        project_id, dataset, table_name, self._table_id
                    )
                )
            # Every declared tag is a data-tag unless kept tag-only (ITL-1023).
            self._data_tags = source_data_tags(resolved_tags, tag_only_columns)

            # A zero-row stream with the table's columns answers every schema question.
            self._builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
            try:
                template = self._builder.build(
                    schema.empty_table(),
                    tag_columns=resolved_tags,
                    source_name=self._source_name,
                    kind_part=self.kind_part(),
                    system_tag_columns=tuple(
                        name
                        for name in system_tag_columns
                        if name not in self._tags_as_data.values()
                    ),
                    data_tags=self._data_tags,
                )
            except ValueError as exc:
                raise ValueError(f"{location}, table {table_name!r}: {exc}") from exc
            self._table_schema: pa.Schema = arrow_utils.drop_system_columns(
                schema.empty_table()
            ).schema
            if not requested.is_empty:
                try:
                    self._apply_restriction(requested)
                except (ValueError, TypeError) as exc:
                    raise type(exc)(f"{location}, table {table_name!r}: {exc}") from exc
            if self._tags_as_data:
                try:
                    self._check_tags_as_data(template.stream.keys())
                except InputValidationError as exc:
                    raise InputValidationError(
                        f"{location}, table {table_name!r}: {exc}"
                    ) from exc
                # The copies are columns of the schema: build the template with them.
                template = self._builder.build(
                    self._with_tag_copies(self._table_schema.empty_table()),
                    tag_columns=template.tag_columns,
                    source_name=self._source_name,
                    kind_part=self.kind_part(),
                    system_tag_columns=system_tag_columns,
                    data_tags=self._data_tags,
                )
            self._stream = template.stream
            self._schema_hash = template.schema_hash
            self._tag_columns = template.tag_columns
            self._tag_only_columns = tuple(
                c for c in self._tag_columns if c not in self._data_tags
            )
            self._system_tag_columns = template.system_tag_columns
        except BaseException:
            self.close()
            raise

    def _check_tags_as_data(
        self, keys: tuple[tuple[str, ...], tuple[str, ...]]
    ) -> None:
        """Check ``tags_as_data`` against the source's tag and data columns.

        Args:
            keys: The source's tag and data columns, without the copies.

        Raises:
            InputValidationError: If the mapping fails ``DerivedSource``'s rule, or a
                tag it names is not one of the source's key columns (the
                ``record_id_columns`` given, by default the primary key). Only a key
                column is checked for nulls by every read, so only a key column can
                be copied into a non-nullable data column.
        """
        tag_keys, data_keys = keys
        validate_tags_as_data(self._tags_as_data, tag_keys, data_keys)
        for tag in self._tags_as_data:
            if tag not in self._key_columns:
                raise InputValidationError(
                    f"tags_as_data: {tag!r} is not a key column, so it may hold a "
                    f"null. Key columns, which can be copied: {list(self._key_columns)}."
                )

    def _with_tag_copies(self, table: "pa.Table") -> "pa.Table":
        """Return ``table`` with each ``tags_as_data`` copy appended, in mapping order.

        A copy is the key column's own Arrow column under a field of the new name,
        with the key's type and field metadata and declared non-nullable (a key is
        never null; a read checks it before copying).
        """
        for tag, name in self._tags_as_data.items():
            key = table.schema.field(tag)
            table = table.append_column(
                pa.field(name, key.type, nullable=False, metadata=key.metadata),
                table.column(tag),
            )
        return table

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------

    @property
    def table_id(self) -> str:
        """SpiralDB's own id of the table this source reads."""
        return self._table_id

    # -------------------------------------------------------------------------
    # Restriction (ITL-1218)
    # -------------------------------------------------------------------------

    def where(
        self,
        /,
        *conditions: "Mapping[str, object] | Condition | None",
        **columns: object,
    ) -> "SpiralDBTableSource":
        """Return a new source on the same table that reads only the matching rows.

        A restriction, not an operator: it chooses which of the table's rows flow and
        changes nothing else. The new source has this source's content hash, pipeline
        hash, source id, schema and system-tag columns, and every row it reads is the
        row this source reads, record id included; it adds no step to a pipeline. A
        pipeline that must select rows as part of what it computes uses an operator
        (``SemiJoin``, ``Filter``) instead. SpiralDB applies the conditions in
        the scan, as for the ``where=`` argument: the two are one mechanism, and the
        conditions are added to any the source already has. Everything else is carried
        over (the pinned table version, ``limit``, ``max_rows``, tags, data-tags,
        ``tags_as_data``, key columns, source id, label, data context). With a
        ``limit``, the new source reads the first ``limit`` rows that match.

        Two kinds of condition are taken, all of which must hold: a column equal to a
        value (a keyword, a mapping, ``col(c) == v``), and a range of an integer,
        timestamp or date column (``col("t") >= start``, ``col("t") < stop``,
        ``col("t").between(a, b)``, which includes both ends). Write a window half-open,
        ``>= start`` and ``< stop``: adjacent windows then neither overlap nor leave a
        gap. Ranges of one column add up, and may select no row; a null meets no
        condition. Float ranges are refused for now (ITL-1477). Use ``filter`` for
        ranges of unsupported types such as floats and strings. Anything else
        (``is_in``, a null test, ``!=``, two columns, ``|``, ``~``) is refused too.

        For now, ``where()`` refuses float equality values that the column's Arrow
        type cannot hold exactly. For example, a plain float32 column accepts
        ``0.5`` and ``1`` and raises ``TypeError`` for ``0.1`` at the call.
        An equality sent to SpiralDB uses the service's own comparison. For a float column,
        ``0.0`` does not select the ``-0.0`` rows. An equality to ``-0.0`` or a NaN is
        refused for now (``TypeError``). SpiralDB matches them by their bits, while Orcapod
        counts ``-0.0`` equal to ``0.0`` and NaN equal to NaN, so the rows read would
        depend on which of two equal values a condition kept. ITL-1480 tracks revisiting
        the float refusals. Use ``filter`` to compare these values by Orcapod's rules.

        A convenience: the arguments are given to ``Condition.of``, which builds the
        ``Condition`` (``orcapod.types``); the source checks it against its table and
        adds it to the condition it has (``Condition.combined``). A ``Condition``
        built beforehand is accepted as it is. The source's ``limit`` is its own and
        is not changed.

        This source is not changed. ``where`` is a copy of the source with the
        restriction applied to the copy in place; a later version offers those two
        steps (``copy()`` and ``apply_restriction``) on their own.

        Args:
            *conditions: Mappings of column name to value, and conditions (built with
                ``col`` or ``Condition.of``). Positional only, so every column name
                can also be given as a keyword.
            **columns: Column name to value, added to ``conditions``.

        Returns:
            The restricted source; its modified time is the moment it was made.

        Raises:
            ValueError: If no condition is given, a column is not in the table, a
                value is ``None``, or a column must equal two different values.
            TypeError: If the value has the wrong Python type (for example, a NumPy
                scalar or a date given as a string) or cannot be held exactly by the
                column's Arrow type (for example, ``-1`` for an unsigned column).
                Values are never converted, rounded or clamped. This error also
                applies to unsupported conditions. For now, float equalities with
                ``-0.0``, NaN, or values the column cannot hold exactly raise this
                error (ITL-1480).
                Use ``filter`` for unsupported conditions.
        """
        try:
            # As given, before merging: merging may keep 0.0 of 0.0 and -0.0, or
            # refuse -0.0 beside another value as two values (ITL-1480).
            check_float_equalities(*conditions, **columns)
        except TypeError as exc:
            raise TypeError(f"SpiralDB table {self._qualified!r}: {exc}") from exc
        condition = Condition.of(*conditions, **columns)
        restricted = self._copy()
        try:
            restricted._apply_restriction(condition)
        except (ValueError, TypeError) as exc:
            restricted.close()
            raise type(exc)(f"SpiralDB table {self._qualified!r}: {exc}") from exc
        return restricted

    def _copy(self) -> "SpiralDBTableSource":
        """Return a new source on the same table and configured client.

        The future ``copy()`` (ITL-1219). No metadata is read again, so the copy
        reads the version this source pinned. Shared, because immutable: the table
        location, table id, schema and zero-row template stream, the stream builder,
        the data context and config, the condition (immutable) and limit, the tuples of
        tag, data-tag and key columns and the pinned version. Copied: the
        ``tags_as_data`` and ``overrides`` mappings. Reset: the content and pipeline
        hash caches (they are recomputed, equal, on first use). Each source owns
        a connector that holds the same SDK client and can be closed independently.
        The modified time is carried over until ``_apply_restriction`` moves it.
        A SpiralDB source holds no rows it read.
        """
        require_same_process(self._pid)  # before a lock inherited by a fork
        with self._connection_lock:
            connection = self._connection
            if connection is None:
                raise RuntimeError("SpiralDBTableSource is closed")
            duplicate = copy.copy(self)
            duplicate._connection = connection.share_client()
        duplicate._tags_as_data = dict(self._tags_as_data)
        duplicate._overrides = (
            None if self._overrides is None else dict(self._overrides)
        )
        duplicate._connection_lock = Lock()
        duplicate._content_hash_cache = {}
        duplicate._pipeline_hash_cache = {}
        return duplicate

    def _apply_restriction(self, condition: Condition) -> None:
        """Add ``condition`` to the condition this source reads under, in place.

        The future ``apply_restriction`` (ITL-1219), used by the constructor's
        ``where=`` and by ``where()``. The condition is checked against the table's
        columns, then combined with the one in force (``Condition.combined``). The
        modified time moves: other rows flow from now on. Nothing cached from a read
        needs resetting (the source holds no row it read), and the identity is
        unchanged, so the hash caches stay valid.

        Args:
            condition: The condition to add.

        Raises:
            ValueError: If a column is not in the table, or a column must equal two
                different values.
            TypeError: If the condition holds anything but equalities and ranges, a
                range names a column of another type, or a value is not of its
                column's type or does not fit it.
        """
        check_restriction(condition, self._table_schema)
        combined = self._condition.combined(condition)
        # Again with the one in force: a column equals one value (ranges add up, and
        # a contradiction of ranges is kept: it reads no row).
        check_restriction(combined, self._table_schema)
        self._condition = combined
        self._update_modified_time()

    @property
    def condition(self) -> Condition:
        """The row conditions this source reads under (``where`` and ``where=``
        combined), applied by SpiralDB in the scan: the empty ``Condition`` when it
        reads every row. Part of no identity (``_restriction_identity``)."""
        return self._condition

    @property
    def row_limit(self) -> int | None:
        """The most rows this source reads (``limit=``), or ``None`` for no limit.
        Part of no identity (``_restriction_identity``)."""
        return self._limit

    def _restriction_identity(self) -> None:
        """Return what ``where``, ``limit`` and ``asof`` add to the source's identity.

        Nothing: a source is identified by where its table is, and a restriction
        chooses rows of that same table. This is the one place that decides it.
        """
        return None

    def identity_structure(self) -> Any:
        """Identity from the source id (``spiraldb_table_v1:`` and the table's locator),
        the schema and the key columns (the primary key, or ``record_id_columns``), as
        for every root source; no class name (ITL-1425).

        With ``tags_as_data`` given, its sorted items follow: two mappings can give
        the same schema (two key columns of one type copied to one name) while the
        copy holds different values. Without it the structure is the plain one.
        """
        extra: list[Any] = []
        restriction = self._restriction_identity()
        if restriction is not None:
            extra.append(restriction)
        if self._tags_as_data:
            extra.append(("tags_as_data", tuple(sorted(self._tags_as_data.items()))))
        base = super().identity_structure()
        return (*base, *extra) if extra else base

    # -------------------------------------------------------------------------
    # Reading
    # -------------------------------------------------------------------------

    def _connector(self) -> SpiralDBConnector:
        """Return the connector owned by this source."""
        require_same_process(self._pid)  # before a lock inherited by a fork
        with self._connection_lock:
            if self._closed:
                raise RuntimeError("SpiralDBTableSource is closed")
            if self._connection is None:
                self._connection = SpiralDBConnector(
                    project_id=self._project_id,
                    dataset=self._dataset,
                    overrides=self._overrides,
                )
            return self._connection

    def close(self) -> None:
        """Release this source's reference to the SDK client after its reads finish."""
        require_same_process(self._pid)  # before a lock inherited by a fork
        with self._connection_lock:
            self._closed = True
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    def __enter__(self) -> SpiralDBTableSource:
        """Return this source for use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release the client at context exit."""
        self.close()

    @property
    def _qualified(self) -> str:
        """The table as error messages name it."""
        return f"{self._project_id}/{self._dataset}.{self._table_name}"

    def _lookup_table(self) -> SpiralDBTable:
        """Resolve the name and check that it still identifies this source's table."""
        table = self._connector().lookup_table(self._table_name)
        table_id = table.table_id
        if table_id != self._table_id:
            raise ValueError(
                f"SpiralDB table {self._qualified!r} is now another table (id "
                f"{table_id!r}; the source reads {self._table_id!r}): it was "
                "dropped and created again. Build the source again to read it."
            )
        return table

    @property
    def row_count_estimate(self) -> int:
        """The whole table's row count, exact: every key is read to count it.

        The count is of the version this source reads (the one it was built at, or
        the given ``asof``), and is remembered while the source is open. ``where`` and
        ``limit`` are not taken into account. SpiralDB offers no recorded count, so the
        cost grows with the table.
        """
        asof = self._asof
        return self._lookup_table().row_count_estimate(asof=asof)

    @override
    def check_for_new_data(self) -> bool:
        """Advance the pinned version when the table has a newer one (ITL-1102).

        The source holds no rows: every read scans the table at the pinned version. A
        source built without ``asof`` pinned the version the table had then; this
        check reads the table's current version (metadata only: no row is read, and
        ``max_rows`` does not apply) and, when it is newer, pins it and moves the
        modified time, so the next read returns the new rows. An ``asof`` given by the
        user never moves. Rows keep their record ids (the hash of their key), so only
        rows whose key or data changed are new entries downstream. A version that
        moved without changing the rows (if SpiralDB moves it on compaction) makes the
        nodes below run again and reuse every stored result.

        Returns:
            ``True`` if the pinned version moved.

        Raises:
            ValueError: If the table was dropped and created again (another table id)
                or its columns changed: build the source again to read it.
        """
        if self._given_asof is not None:
            return False
        # One lookup per check (ITL-1350): a table dropped and created again since
        # is found by this lookup, and its schema and version come from it.
        table = self._lookup_table()
        schema = arrow_utils.drop_system_columns(
            table.arrow_schema().empty_table()
        ).schema
        if not schema.equals(self._table_schema, check_metadata=True):
            raise ValueError(
                f"SpiralDB table {self._qualified!r}: "
                f"{_schema_difference(schema, self._table_schema)}. Build the "
                "source again to read it as it is now."
            )
        version = table.version() or None
        if version is None or (self._asof is not None and version <= self._asof):
            return False
        self._asof = version
        self._update_modified_time()
        return True

    def _too_large(self, found: str) -> SourceTooLargeError:
        """Return the error for a read that goes over ``max_rows``."""
        return SourceTooLargeError(
            f"SpiralDB table {self._qualified!r}: {found}, more than "
            f"max_rows={self._max_rows}{self._under_condition()}. Read fewer rows "
            "with limit=<n> or a narrower where= or where(...) (equalities, and ranges "
            "such as col(c) >= start, col(c) < stop), or raise max_rows "
            "(max_rows=None removes the cap; a pipeline job keeps every row in memory)."
        )

    def _under_condition(self) -> str:
        """Return `` under where <condition>`` for a restricted source, else nothing."""
        return (
            f" under where {self._condition.text()}"
            if not self._condition.is_empty
            else ""
        )

    def _key_tokens(self, table: "pa.Table") -> list[str]:
        """Return each row's identifier: the canonical JSON of its primary key.

        The same key form as every other source (``key_row_identifier``, ITL-1154).

        Raises:
            RowIdentifierError: If a key column holds a null or a type that cannot
                identify a row, or two rows of the batch have one key; the message
                names the table.
        """
        try:
            return row_identifiers(table, self._key_columns)
        except RowIdentifierError as exc:
            raise RowIdentifierError(
                f"SpiralDB table {self._qualified!r}: {exc}"
            ) from exc

    def _batch_streams(
        self, *, with_source_info: bool = True
    ) -> Generator["ArrowTableStream", None, None]:
        """Scan the table once and yield its rows as streams of at most ``batch_rows`` rows.

        Each SpiralDB batch is held while its slices are yielded.

        Args:
            with_source_info: As for ``_read_batches``.

        Raises:
            SourceTooLargeError: If the read would return, or returns, more than
                ``max_rows`` rows.
            ValueError: If a batch's columns differ from the source's schema, or a
                key column holds a null.
        """
        asof = self._asof
        table = self._lookup_table()
        yield from self._read_batches(
            table,
            asof=asof,
            limit=self._limit,
            max_rows=self._max_rows,
            with_source_info=with_source_info,
        )

    def _read_batches(
        self,
        table: SpiralDBTable,
        *,
        asof: int | None,
        limit: int | None,
        max_rows: int | None,
        with_source_info: bool = True,
    ) -> Generator["ArrowTableStream", None, None]:
        """Scan the table once, reading at most ``limit`` rows, in streams of ``batch_rows``.

        ``where`` and the pinned version apply as for every read.

        Args:
            table: The handle resolved for this read, also used for the count.
            asof: The pinned version for both the count and the scan.
            limit: Read at most this many rows, the first in key order.
            max_rows: Raise when the read would return more rows; ``None`` for no cap.
            with_source_info: Whether each stream holds its rows' source-info tokens.
                ``False`` only where the caller returns no source-info column and
                hashes no row (``_whole_table``): the streams then hold null tokens.

        Raises:
            SourceTooLargeError: If the read would return, or returns, more than
                ``max_rows`` rows.
            ValueError: If a batch's columns differ from the source's schema, or a
                key column holds a null.
        """
        if (
            max_rows is not None
            and self._condition.is_empty
            and (limit is None or limit > max_rows)
        ):
            # Nothing but limit bounds the read, and the limit does not keep it
            # under the cap: the table's row count decides, before any row is
            # handed on (so no row reaches a function of a job that would fail).
            # Counting stops one past the cap.
            if table.count_rows(asof=asof, at_most=max_rows + 1) > max_rows:
                bound = "" if limit is None else f" (limit={limit:,})"
                raise self._too_large(
                    f"the table has more than {max_rows:,} rows{bound}"
                )
        seen = 0
        # Keys must be unique across the whole read, not only within a slice.
        identifiers: set[str] = set()
        for batch in table.scan_batches(
            where=None if self._condition.is_empty else self._condition,
            limit=limit,
            asof=asof,
        ):
            for start in range(0, max(batch.num_rows, 1), self._batch_rows):
                raw = pa.Table.from_batches([batch.slice(start, self._batch_rows)])
                require_supported_nullable_types(raw.schema)
                rows = arrow_utils.drop_system_columns(raw)
                # Metadata too: field metadata is how a column's logical type is
                # recorded, and the schema hash was computed at construction.
                if not rows.schema.equals(self._table_schema, check_metadata=True):
                    raise ValueError(
                        f"SpiralDB table {self._qualified!r}: "
                        f"{_schema_difference(rows.schema, self._table_schema)}. "
                        "The table changed after the source was built; build the "
                        "source again to read it as it is now."
                    )
                seen += rows.num_rows
                if max_rows is not None and seen > max_rows:
                    raise self._too_large("the read returned more rows")
                # Tokens first: they refuse a null key before it is copied into
                # a non-nullable column.
                tokens = self._key_tokens(rows)
                repeated = identifiers.intersection(tokens)
                if repeated:
                    raise RowIdentifierError(
                        f"SpiralDB table {self._qualified!r}: key "
                        f"{sorted(repeated)[0]} appears in two rows; key values "
                        "must be unique, so each row has its own identifier"
                    )
                rows = self._with_tag_copies(rows)
                require_custom_system_tags(rows, self._system_tag_columns)
                if not rows.num_rows:
                    continue
                identifiers.update(tokens)
                yield self._builder.attach_provenance(
                    rows,
                    tag_columns=self._tag_columns,
                    system_tag_columns=self._system_tag_columns,
                    schema_hash=self._schema_hash,
                    source_id=self.source_id,
                    row_identifiers=tokens,
                    data_tags=self._data_tags,
                    with_source_info=with_source_info,
                )

    def iter_data(self) -> Iterator["tuple[TagProtocol, DataProtocol]"]:
        """Scan the table and yield its ``(tag, data)`` pairs, one batch held at a time."""
        for stream in self._batch_streams():
            yield from stream.iter_data()

    async def async_iter_data(
        self,
    ) -> AsyncIterator["tuple[TagProtocol, DataProtocol]"]:
        """Yield the table's ``(tag, data)`` pairs, reading each batch off the event loop.

        One thread owns the reader for its whole life. When the consumer stops or is
        cancelled, that thread closes it, after a read in progress has ended.
        """
        streams = self._batch_streams()
        owner = ThreadPoolExecutor(max_workers=1, thread_name_prefix="spiraldb-read")
        loop = asyncio.get_running_loop()
        try:
            while True:
                stream = await loop.run_in_executor(owner, next, streams, None)
                if stream is None:
                    break
                for pair in stream.iter_data():
                    yield pair
        finally:
            _ = owner.submit(streams.close)
            owner.shutdown(wait=False)

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Scan the table and return its rows as one PyArrow table (all held in memory)."""
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        return self._whole_table(
            self._batch_streams(with_source_info=_reads_source_info(config)),
            columns=columns,
            all_info=all_info,
        )

    def _whole_table(
        self,
        streams: Iterator["ArrowTableStream"],
        *,
        columns: ColumnConfig | dict[str, Any] | None,
        all_info: bool,
    ) -> "pa.Table":
        """Return the rows of ``streams`` (batches of one read) as one table."""
        # One stream over all the rows, so that a whole-table option (sorting by
        # tags) applies across batches. The source-info tokens are carried only for
        # a result that shows or hashes them (ITL-1464).
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        enriched = {"source": _reads_source_info(config), "system_tags": True}
        tables = [stream.as_table(columns=enriched) for stream in streams]
        if not tables:
            return self._stream.as_table(columns=columns, all_info=all_info)
        whole = ArrowTableStream(
            table=pa.concat_tables(tables),
            tag_columns=self._tag_columns,
            system_tag_columns=self._system_tag_columns,
            data_tags=self._data_tags,
        )
        return whole.as_table(columns=columns, all_info=all_info)

    def _head(
        self,
        n: int,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "_Head":
        """Return the source's first ``n`` rows, read in one bounded scan, and its size.

        At most ``n`` rows are read (fewer when the source's ``limit`` is smaller), with
        ``where`` and the pinned version applied and ``max_rows`` not, so a table of any
        size can be looked at. When these are not all the rows the source reads, the
        size is the table's row count, counted from its keys up to ``max_rows`` (at most
        100,000) rows; above that it is unknown.

        Args:
            n: The most rows to read.
            columns: Column configuration, as for ``as_table``.
            all_info: Include all column groups, as for ``as_table``.
        """
        if not self._preview_reads_rows:
            schema_only = self._stream.as_table(columns=columns, all_info=all_info)
            bound, total = self._bounded_count(PREVIEW_ROWS)
            return _Head(schema_only, False, total, True, bound)
        limit = self._limit
        head_limit = n if limit is None else min(n, limit)
        asof = self._asof
        table = self._lookup_table()
        rows = self._whole_table(
            self._read_batches(table, asof=asof, limit=head_limit, max_rows=None),
            columns=columns,
            all_info=all_info,
        )
        if rows.num_rows < head_limit or head_limit == limit:
            return _Head(rows, True, rows.num_rows, False)
        bound, total = self._bounded_count(n, table)
        if limit is not None and self._condition.is_empty:
            total = limit if total is None and limit <= bound else total
            total = None if total is None else min(limit, total)
        return _Head(rows, True, total, True, bound)

    def _bounded_count(
        self, shown: int, table: SpiralDBTable | None = None
    ) -> tuple[int, int | None]:
        """Return how far the table's rows were counted for a preview, and their number,
        or ``None`` when there are more.

        A preview counts at most ``DEFAULT_MAX_ROWS`` keys: never every key of a table of
        any size. One that shows more rows than that counts none.

        Args:
            shown: The rows the preview shows.
            table: The handle resolved for this read; looked up when not given.
        """
        if shown > DEFAULT_MAX_ROWS:
            return shown, None
        allowed = min(self._max_rows or DEFAULT_MAX_ROWS, DEFAULT_MAX_ROWS)
        bound = max(allowed, shown)
        handle = self._lookup_table() if table is None else table
        total = handle.count_rows(asof=self._asof, at_most=bound + 1)
        return bound, (None if total > bound else total)

    def _head_note(self, head: "_Head", sorted_by_tags: bool = False) -> str:
        """Return the words a notebook display puts after the header: the one place.

        With ``sorted_by_tags`` the rows read are in tag order, which is not the first
        rows of the table in tag order, and the words say so.
        """
        shown = head.rows.num_rows
        total = (
            (
                f"at least {head.counted_to:,}"
                if head.counted_to > DEFAULT_MAX_ROWS
                else f"more than {head.counted_to:,}"
            )
            if head.total_rows is None
            else f"about {head.total_rows:,}"
        )
        if not head.rows_read:
            return f"rows not read; the table has {total} rows"
        if not head.total_is_estimate:
            all_rows = f"all {shown:,} row{'' if shown == 1 else 's'}"
            return f"{all_rows}, sorted by tags" if sorted_by_tags else all_rows
        if not self._condition.is_empty:
            condition = self._condition.text()
            read = (
                f"{shown:,} matching rows read, sorted by tags"
                if sorted_by_tags
                else f"first {shown:,} matching rows"
            )
            return f"{read} (where {condition}); the table has {total} rows"
        if sorted_by_tags:
            return f"{shown:,} rows read, sorted by tags, of {total}"
        return f"first {shown:,} rows of {total}"

    def head(
        self,
        n: int = PREVIEW_ROWS,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the source's first ``n`` rows, read in one bounded scan.

        At most ``n`` rows are read (fewer when the source's ``limit`` is smaller), with
        ``where`` and the pinned version applied and ``max_rows`` not, so a table of any
        size can be looked at. The total is exact when these are all the rows the
        source reads, else the table's row count (an estimate of what is read), unknown
        above the bound that is counted. The notebook
        preview is this head. See ``StreamBase.head``.
        """
        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        head = self._head(n, columns=config)
        tag_columns = self.keys(columns=config)[0]
        if not head.rows_read:
            empty = StreamView.without_rows(
                self, ViewStatus.NOT_COMPUTED, head.rows, tag_columns=tag_columns
            )
            return dataclasses.replace(
                empty,
                total_rows=head.total_rows,
                total_is_estimate=True,
                description=self._head_note(head),
            )
        return StreamView.of_rows(
            self,
            head.rows,
            tag_columns=tag_columns,
            total_rows=head.total_rows,
            total_is_estimate=head.total_is_estimate,
            origin="read",
            description=self._head_note(
                head, config.sort_by_tags and bool(self._tag_columns)
            ),
        )

    def run(self) -> StreamView:
        """Read the whole table into memory and return a ``StreamView`` of every row.

        Does what ``as_table()`` does: one scan of every row the source reads (``where``,
        ``limit``, ``max_rows`` and the pinned version applied), held in memory. The
        source holds no rows, so every call scans again. ``head(n)`` and the notebook
        preview are the bounded read: they read only the first ``n`` rows.
        """
        return super().run()

    def _display_name(self) -> str:
        """The name a preview header shows: the label when one was given, else the
        table's name (the default source id also carries the table's id)."""
        return self.label if self.has_assigned_label else self._table_name

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict.

        The table is saved by its name and by SpiralDB's table id; ``from_config``
        checks that the name still leads to that table. A version pinned by the
        source itself is not saved: a loaded source reads the table's then-current
        version. An ``asof`` the user gave is saved.
        """
        return {
            "source_type": self.SOURCE_KIND,
            "project_id": self._project_id,
            "dataset": self._dataset,
            "overrides": None if self._overrides is None else dict(self._overrides),
            "table_name": self._table_name,
            "table_id": self._table_id,
            "tag_columns": list(self._tag_columns),
            **self._tag_only_config(),
            "system_tag_columns": list(self._system_tag_columns),
            "where": self._condition.to_config(),
            "limit": self._limit,
            "asof": self._given_asof,
            "max_rows": self._max_rows,
            "batch_rows": self._batch_rows,
            "tags_as_data": dict(self._tags_as_data) or None,
            "record_id_columns": None
            if self._record_id_columns is None
            else list(self._record_id_columns),
            **self._source_id_config(),
            **self._identity_config(),
        }

    @classmethod
    def from_config(
        cls, config: dict[str, Any], db_registry=None
    ) -> "SpiralDBTableSource":
        """Reconstruct a SpiralDBTableSource from a config dict, reading no rows.

        Args:
            config: Dict as produced by ``to_config()``.

        Returns:
            A new ``SpiralDBTableSource`` instance.

        Raises:
            ValueError: If the saved table name now leads to a table with another
                SpiralDB table id than the one saved (the table was dropped and
                created again, or another table took the name).
        """
        source = cls._from_saved_name(
            config,
            lambda name: cls(
                project_id=config["project_id"],
                table_name=config["table_name"],
                dataset=config.get("dataset", "default"),
                tag_columns=config.get("tag_columns"),
                tag_only_columns=saved_tag_only_columns(config),
                system_tag_columns=config.get("system_tag_columns", ()),
                where=Condition.from_config(config.get("where") or {}),
                limit=config.get("limit"),
                asof=config.get("asof"),
                max_rows=config.get("max_rows", DEFAULT_MAX_ROWS),
                record_id_columns=config.get("record_id_columns"),
                batch_rows=config.get("batch_rows", DEFAULT_BATCH_ROWS),
                tags_as_data=config.get("tags_as_data"),
                source_id=name,
                label=config.get("label"),
                data_context=config.get("data_context"),
                overrides=config.get("overrides"),
            ),
        )
        saved_id = config.get("table_id")
        if saved_id is not None and saved_id != source.table_id:
            source.close()
            raise ValueError(
                f"SpiralDB table {source._qualified!r} was saved with table id "
                f"{saved_id!r} but that name now leads to table id "
                f"{source.table_id!r}: it is a different table."
            )
        return source
