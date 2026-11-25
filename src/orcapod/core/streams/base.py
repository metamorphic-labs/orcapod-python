from __future__ import annotations

import logging
from abc import abstractmethod
from collections import deque
from collections.abc import (
    AsyncIterator,
    Callable,
    Collection,
    Generator,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from contextlib import closing, contextmanager
from contextvars import ContextVar
from datetime import datetime
from typing import ClassVar, TYPE_CHECKING, Any, cast

import pandas as pd
import polars as pl
import pyarrow as pa

from orcapod.core.unique_keys import NO_UNIQUE_KEYS, UniqueKeys
from orcapod.core.base import TraceableBase
from orcapod.core.data_tags import data_tag_names
from orcapod.core.streams.record_batch_input import SOURCE_BATCH_ROWS, InputChannel
from orcapod.core.streams.view import (
    SCHEMA_NEEDS_POLL_NOTE,
    BoundedStreamView,
    ViewStatus,
    StreamView,
    WindowOfRows,
    default_note,
    preview_html,
)
from orcapod.protocols.core_protocols import (
    DataProtocol,
    PodProtocol,
    StreamProtocol,
    TagProtocol,
)
from orcapod.protocols.core_protocols.streams import RecordBatchInput
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Condition, Schema
from orcapod.utils import arrow_utils, source_tokens

if TYPE_CHECKING:
    from orcapod.types.explanation import Explanation

# TODO: consider using this instead of making copy of dicts
# from types import MappingProxyType

logger = logging.getLogger(__name__)


def reads_in_batches(stream: object) -> bool:
    """Whether ``run()`` reads ``stream``'s rows batch by batch (ITL-1464).

    True for a source that reads its rows on demand and holds none, and for operators
    applied batch by batch over one; decided by the kind of stream and its operators,
    never by its rows.
    """
    check = getattr(stream, "_reads_in_batches", None)
    return bool(check()) if callable(check) else False


def batch_streams_of(stream: object) -> Generator[StreamProtocol, None, None]:
    """The rows of a stream that ``reads_in_batches``, in order, one stream per batch: at
    least one, with no row when there is none."""
    return cast("StreamBase", stream)._batch_streams_of_rows()  # pyright: ignore[reportPrivateUsage]  # shared by the stream kinds


def system_tags_as_rows_give(table: pa.Table) -> bool:
    """Whether every system-tag column is what the row rebuild gives: non-null text.

    ``main`` rebuilt a node's table from its rows, whose system tags are typed from
    their values: a ``large_string`` field, non-nullable, with no field metadata (the
    only kind library code makes). A whole table is its rows only when its system tags
    are already so, in their values as in their fields: a narrow integer, a nullable
    field, a null value (also in a field declared non-nullable) or field metadata (an
    Arrow extension's, which a tag drops) is decided by the rows (ITL-1347).

    Args:
        table: The table.

    Returns:
        Whether every system-tag column is non-nullable ``large_string``, without
        metadata, holding no null.
    """
    return all(
        field.type == pa.large_string()
        and not field.nullable
        and not field.metadata
        and table.column(field.name).null_count == 0
        for field in table.schema
        if field.name.startswith(constants.SYSTEM_TAG_PREFIX)
    )


def schema_requires_io(stream: object, config: ColumnConfig) -> bool:
    """Whether reading ``stream``'s schema with ``config`` would read or poll a source.

    A stream that does not say (``_schema_requires_io``) is taken not to.

    Args:
        stream: The stream whose schema would be read.
        config: The column groups the schema would be read with: source info, meta,
            context and content hashes may be known only from rows.
    """
    check = getattr(stream, "_schema_requires_io", None)
    return bool(check(config)) if callable(check) else False


def short_stream_repr(stream: object) -> str:
    """Name a stream by its class and label only, as a derived stream's ``repr`` lists it.

    A derived stream's ``repr`` names its upstreams this way, never by their own
    ``repr``: that one names their upstreams in turn, so a graph whose stages split
    and join again would print every path, 2 ** stages long (ITL-1111).

    Args:
        stream: An upstream stream.

    Returns:
        ``<ClassName 'label'>``, or ``<ClassName>`` when the stream has no label.
    """
    label = getattr(stream, "label", None)
    name = type(stream).__name__
    return f"<{name} {label!r}>" if label is not None else f"<{name}>"


ZERO_ROW_INPUT_COLUMNS = ColumnConfig(system_tags=True, source=True)
"""The column groups of a zero-row table: what a node's rows carry downstream."""


_ZERO_ROW_MEMO: ContextVar[dict[int, tuple[object, pa.Table | None]] | None] = (
    ContextVar("_ZERO_ROW_MEMO", default=None)
)
"""The zero-row tables derived during one outermost ``typed_zero_row_table`` call."""


@contextmanager
def shared_zero_row_memo() -> Iterator[None]:
    """Share one zero-row memo across every ``typed_zero_row_table`` call in the block.

    A caller that types many streams of one graph (a saved job writes every node's
    Arrow types) derives each stream once, instead of once per call. Inside an outer
    derivation or another such block, the memo already open is used.

    Yields:
        Nothing; the memo is discarded when the block exits.
    """
    if _ZERO_ROW_MEMO.get() is not None:
        yield
        return
    token = _ZERO_ROW_MEMO.set({})
    try:
        yield
    finally:
        _ZERO_ROW_MEMO.reset(token)


def typed_zero_row_table(stream: object) -> pa.Table | None:
    """Return ``stream``'s table with no rows and the Arrow types its rows would have.

    The table has the user tags, system tags, data and source-info columns
    (``ZERO_ROW_INPUT_COLUMNS``; never a meta column) in the order the stream's own table has them, with
    the Arrow types, nullability and logical types the stream's rows have (ITL-912). Getting it never computes, reads or polls a row:
    an in-memory table is sliced, a source answers from the table or schema it already
    holds, and an operator node applies its operator to its inputs' zero-row tables, as
    the sync path applies it to zero-row inputs.

    A stream that does not say (``_typed_zero_row_table``) cannot know its Arrow types
    without rows. The caller then builds the table from the Python output schema.

    Within one outermost call each stream is derived once, so an upstream that several
    inputs share is not derived again per path; the memo is discarded when that call
    returns and is never shared between threads or tasks.

    Args:
        stream: The stream, node or source whose zero-row table is wanted.

    Returns:
        The zero-row table, or ``None`` when ``stream`` cannot type it without rows:
        a function pod's stream or node (ITL-985), an unbound input slot, a source
        that reads, polls or derives its rows later, a pod stream not yet computed.
    """
    memo = _ZERO_ROW_MEMO.get()
    if memo is None:
        # The outermost call: a memo for this derivation only, so an upstream that
        # several inputs share (a diamond) is derived once. ``reset`` discards it, and
        # a ContextVar keeps it apart from other threads and tasks.
        token = _ZERO_ROW_MEMO.set({})
        try:
            return typed_zero_row_table(stream)
        finally:
            _ZERO_ROW_MEMO.reset(token)
    known = memo.get(id(stream))
    if known is not None and known[0] is stream:
        return known[1]
    table = _derive_zero_row_table(stream)
    # The stream is held with its table, so its id is not reused during the call.
    memo[id(stream)] = (stream, table)
    return table


def typed_zero_row_input(stream: object) -> pa.Table | None:
    """Return ``stream``'s zero-row table as a pod reads it: in the large-normalized form.

    What a function pod or an operator takes from an input's zero-row table
    (``typed_zero_row_table``): the input's types in the form its rows have downstream
    (``arrow_utils.normalize_to_large_types`` with ``keep_null=True``: ``string`` to
    ``large_string``, ``binary`` to ``large_binary``, ``list`` to ``large_list``,
    recursively), as the populated path normalizes them (ITL-912). Only layouts change:
    width, nullability, field names and logical types are kept, and no value is cast
    (there is none). A source's own table is not changed: ``typed_zero_row_table`` still
    gives it as it holds it.

    Args:
        stream: The input whose zero-row table a pod reads.

    Returns:
        The normalized zero-row table, or ``None`` when ``stream`` cannot type it
        without rows.
    """
    table = typed_zero_row_table(stream)
    if table is None:
        return None
    return table.cast(
        pa.schema(
            [
                field.with_type(
                    arrow_utils.normalize_to_large_types(field.type, keep_null=True)
                )
                for field in table.schema
            ]
        ).with_metadata(table.schema.metadata or {})
    )


def row_built_columns(
    table: pa.Table, keys: tuple[Sequence[str], Sequence[str]]
) -> pa.Table:
    """Return ``table``'s columns as a table built from its rows lays them out.

    Every step of F (``StaticOutputOperatorPod._row_built_form``) that each row decides on its own, so it
    gives the same result on a whole table and on each slice of it:

    1. The columns a row-built table holds: the user tags, the system tags, the data
       columns that are not data-tags, and the source info of every data column (a
       data-tag's included). ``table`` holds no context, meta or content-hash column
       (``ZERO_ROW_INPUT_COLUMNS``).
    2. In row order: user tags in tag order, system tags in table order, then the data
       columns and their source info, both in the order of the data keys. A row's
       ``Data`` lists its source info in the order of its data keys, data-tags first
       in tag order (``Data._ensure_source_info_table``), so a data-tag's source info
       comes before the other data columns' even when the data-tag is not the first
       column of ``table``.
    3. Normalized to the large types without keeping ``null``: each row's datagram
       stores a ``null`` column as ``large_string`` (``Datagram._init_from_table``).
    4. The system-tag and source-info fields as a row builds them from its values
       (``row_built_provenance``): no field metadata, and a source-info list of the
       form its tokens give.

    Whether a system-tag or source-info field is nullable depends on its values over
    the complete table (``StaticOutputOperatorPod._flag_by_values``), so it is not
    decided here.

    Args:
        table: A table of user tags, system tags, data and source info.
        keys: The tag keys and the data keys of the stream ``table`` belongs to
            (``stream.keys()``).

    Returns:
        The same values, selected and ordered, in their large types.
    """
    return row_built_provenance(
        arrow_utils.normalize_table_to_large_types(
            table.select(row_built_names(table.column_names, keys))
        )
    )


def _source_info_value_type(kind: pa.DataType) -> pa.DataType:
    """The Arrow type a row gives a source-info column of type ``kind``.

    A row's ``Data`` types its tokens from their Python values
    (``arrow_utils.source_info_arrow_type``): a string as ``large_string``, a list as
    ``large_list`` of its element's type, with the item field named ``item`` and
    nullable. A type of any other kind is left as it is.
    """
    if (
        pa.types.is_list(kind)
        or pa.types.is_large_list(kind)
        or pa.types.is_fixed_size_list(kind)
    ):
        return pa.large_list(_source_info_value_type(kind.value_type))  # pyright: ignore[reportAttributeAccessIssue, reportUnknownArgumentType, reportUnknownMemberType]  # a list type has a value type
    if (
        pa.types.is_string(kind)
        or pa.types.is_large_string(kind)
        or pa.types.is_null(kind)
    ):
        return pa.large_string()
    return kind


def row_built_provenance(table: pa.Table) -> pa.Table:
    """Give ``table``'s system-tag and source-info fields the form a row builds.

    A row rebuilds its system tags and its source info from their values, so their
    fields carry no field metadata, and a source-info column has the type its tokens
    give (``_source_info_value_type``): a list's item field named ``item`` and
    nullable at every depth. The values are unchanged: a type that differs only in
    its item fields is cast. The fields' nullability is left as it is
    (``StaticOutputOperatorPod._flag_by_values`` decides it). Other fields are not
    touched.

    Args:
        table: A table of user tags, system tags, data and source info.

    Returns:
        The same values under the rebuilt fields.
    """
    fields: list[pa.Field[pa.DataType]] = []
    columns: list[Any] = []
    changed = False
    for field, column in zip(table.schema, table.columns, strict=True):
        if field.name.startswith(constants.SOURCE_PREFIX):
            # A derived token column stays derived: its text is what the rows give.
            kind = (
                field.type
                if source_tokens.derived_recipe(field.type) is not None
                else _source_info_value_type(field.type)
            )
            if kind != field.type:
                column = column.cast(kind)
            new_field = pa.field(field.name, kind, nullable=field.nullable)
        elif field.name.startswith(constants.SYSTEM_TAG_PREFIX):
            new_field = field.remove_metadata()
        else:
            new_field = field
        changed = changed or not new_field.equals(field, check_metadata=True)
        fields.append(new_field)
        columns.append(column)
    if not changed:
        return table
    return pa.Table.from_arrays(
        columns,
        schema=pa.schema(
            fields, cast("dict[bytes | str, bytes | str] | None", table.schema.metadata)
        ),
    )


def row_built_names(
    names: Sequence[str], keys: tuple[Sequence[str], Sequence[str]]
) -> list[str]:
    """Return the columns of a table built from rows, in row order (F steps 1-2).

    The table built from rows is ``StaticOutputOperatorPod._rows_to_table``'s; this and
    ``row_built_columns`` are module functions so that a stream applies them without
    importing the operators (ITL-1347).

    The user tags in tag order, the system tags in the order of ``names``, the data
    columns that are not data-tags or meta columns, then the source info of every
    such data column and data-tag; data
    and source info in the order of the data keys (a row's ``Data`` lists its source
    info so, data-tags first in tag order: ``Data._ensure_source_info_table``).

    Args:
        names: The column names of a table of user tags, system tags, data and
            source info.
        keys: The tag keys and the data keys of the stream the table belongs to.

    Returns:
        The names a table built from rows holds, in its order.
    """
    tag_keys, data_keys = keys
    present = set(names)
    tags = set(tag_keys)
    system_tags = [n for n in names if n.startswith(constants.SYSTEM_TAG_PREFIX)]
    # A meta (``__``) column is never data: a row's ``Data`` holds it apart, and no
    # node passes it on (ITL-1080).
    data_keys = [n for n in data_keys if not n.startswith(constants.META_PREFIX)]
    data = [n for n in data_keys if n not in tags]
    source_info = [
        f"{constants.SOURCE_PREFIX}{n}"
        for n in data_keys
        if f"{constants.SOURCE_PREFIX}{n}" in present
    ]
    return [*tag_keys, *system_tags, *data, *source_info]


def keys_of_rows(
    table: pa.Table, keys: tuple[Sequence[str], Sequence[str]]
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return the tag and data keys a source's rows have, given its table and ``keys()``.

    A source's rows come from a stream over its table (``ArrowTableStream``), whose rows
    list their tags in the stream's tag order and their data with the data-tags first,
    in tag order, then the other data columns in the table's order. A source whose
    ``keys()`` states its declared order (a ``PollingSource`` whose fetched table has
    another column order) has rows in the table's order all the same, as a table built
    from its rows does (Codex on PR #485).

    Args:
        table: The source's table, as its rows are read from it.
        keys: The source's ``keys()``.

    Returns:
        The tag keys, and the data keys in the order of the rows.
    """
    tag_keys, data_keys = (tuple(keys[0]), tuple(keys[1]))
    data_tags = data_tag_names(tag_keys, data_keys)
    data = set(data_keys) - set(tag_keys)
    in_table = tuple(name for name in table.column_names if name in data)
    return tag_keys, (*data_tags, *in_table)


def stream_arrow_schema(
    stream: object,
    keys: tuple[Sequence[str], Sequence[str]] | None = None,
    *,
    from_table: bool = True,
) -> pa.Schema:
    """Return ``stream``'s declared Arrow schema: what ``arrow_schema()`` answers (ITL-1347).

    The schema of the columns a job node passes on, without reading, computing or
    polling a row: the stream's zero-row input (``typed_zero_row_input``, the Arrow types
    its rows have) with the columns of F, in F's order
    (``row_built_names``: user tags, system tags, data, source
    info, data and source info in data-key order), with no context and no meta column. A
    stream that cannot type itself without rows answers with its Python schema
    converted by its data context (``arrow_types_declared`` is then false), and one
    whose schema is known only by polling (nothing declared, nothing polled:
    ``schema_requires_io``) answers with no column. Each
    system-tag and source-info field is stated nullable: a table decides its flag from
    its values (F's last step). A source-info field's type is stated as the declared
    token type; a complete table decides it from its tokens
    (``StaticOutputOperatorPod._type_source_info_by_values``), so consumers must not
    rely on it. A ``null`` column keeps the zero-row ``null`` type; rows
    carry ``large_string`` (ITL-1319). ITL-1307 later reads it from ``Schema`` instead,
    here.

    Args:
        stream: The stream, source or node.
        keys: The tag and data keys of the stream's rows, when they differ from
            ``keys()`` (a source slot gives its bound source's rows); default ``keys()``.
        from_table: Take the stream's typed zero-row table (``typed_zero_row_input``);
            ``False`` describes it from its declared Python schema only (a narrower
            bind, whose rows ``main``'s row path places).

    Returns:
        The declared Arrow schema.
    """
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod

    typed = cast(StreamProtocol, stream)
    given_keys = keys
    table = typed_zero_row_input(stream) if from_table else None
    if table is None and schema_requires_io(stream, ColumnConfig(system_tags=True)):
        # Nothing declared, nothing polled (a poller with no declared schema): the
        # columns are known only from rows, and a description never polls. No column,
        # as a head says (``SCHEMA_NEEDS_POLL_NOTE``); ``arrow_types_declared`` is false.
        return pa.schema([])
    if table is None:
        # Only what the stream declares: asking for its source-info columns would make
        # a source that has not polled poll (a description never computes, polls or
        # reads). A source's tokens are strings, so each data column's source info is
        # a string field.
        tag_schema, data_schema = typed.output_schema(columns={"system_tags": True})
        stated = getattr(stream, "arrow_schemas", None)
        if isinstance(stated, tuple):
            # The Arrow types the stream declares itself (a slot): never a conversion
            # of its Python types, which may have no Arrow type of their own.
            tag_arrow, data_arrow = cast("tuple[pa.Schema, pa.Schema]", stated)
            fields = [
                *tag_arrow,
                *(
                    pa.field(name, pa.large_string())
                    for name in tag_schema
                    if name not in tag_arrow.names
                ),
                *(f for f in data_arrow if f.name not in tag_arrow.names),
            ]
            table = pa.schema(fields).empty_table()
        else:
            converter = StaticOutputOperatorPod._upstream_context(typed).type_converter  # pyright: ignore[reportPrivateUsage]  # the one place that resolves an upstream's context
            table = arrow_utils.make_empty_table(
                {**tag_schema, **data_schema}, converter
            )
        for name in data_schema:
            table = table.append_column(
                pa.field(f"{constants.SOURCE_PREFIX}{name}", pa.large_string()),
                pa.array([], pa.large_string()),
            )
    # The rows' keys: the data in the order of the stream's table (a poller that
    # fetched its columns in another order than it declares lists them so).
    keys = given_keys if given_keys is not None else keys_of_rows(table, typed.keys())
    names = row_built_names(table.column_names, keys)
    schema = row_built_provenance(table.select(names)).schema
    prefixes = (constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX)
    for index, field in enumerate(schema):
        if field.name.startswith(prefixes):
            schema = schema.set(index, field.with_nullable(True))  # pyright: ignore[reportUnknownMemberType]  # stubs leave Field unparametrized
    return schema


class CompleteStreamFeed:
    """The record batches of a complete stream, read at one reader's own position.

    The feed of a channel opened on a stream (``StreamBase.open_channel()``). Nothing
    is read until the first ``next``; then the stream's table is read once (a fixed
    snapshot: a stream that changes later is not read again), in the row-built form
    (``row_built_columns``), and cut into record batches of at most
    ``SOURCE_BATCH_ROWS`` rows. A batch is dropped by the feed once delivered. The
    stream is complete, so nothing is queued and no producer waits on a reader.
    """

    def __init__(self, stream: StreamProtocol) -> None:
        """Make a feed of ``stream``'s rows; nothing is read until the first ``next``.

        Args:
            stream: The complete stream to read.
        """
        self._stream = stream
        self._batches: deque[pa.RecordBatch] | None = None
        self._failure: BaseException | None = None

    async def next(self) -> pa.RecordBatch | None:
        """Return the stream's next record batch, or ``None`` at the end.

        The stream's table is read on the first call, not when the channel is opened;
        batches with no rows are skipped.
        """
        if self._failure is not None:
            # The stream was read once and preparing its batches failed: the same
            # error again, never a second read of the stream.
            raise self._failure
        if self._batches is None:
            try:
                self._batches = deque(
                    batch
                    for batch in self._row_built_table().to_batches(
                        max_chunksize=SOURCE_BATCH_ROWS
                    )
                    if batch.num_rows
                )
            except BaseException as failure:
                self._failure = failure
                raise
        return self._batches.popleft() if self._batches else None

    def _row_built_table(self) -> pa.Table:
        """The stream's table in the row-built form: its one read.

        The table first: reading it is the stream's one read (an unpolled source polls
        here), and its keys are then known without another. Its system tags must be
        what rows give (``system_tags_as_rows_give``: non-null text, no field
        metadata); otherwise, as on the eager routes of a job, the table is the one
        its rows build (``main``'s row path), so a narrow or nullable system tag is
        retyped as a row retypes it. Only a stream that holds such a column (a raw
        ``ArrowTableStream``) is read that way, and its reads have no effect.
        """
        table = self._stream.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
        if not system_tags_as_rows_give(table):
            from orcapod.core.operators.static_output_pod import (
                StaticOutputOperatorPod,
            )

            rows = list(self._stream.iter_data())
            built = StaticOutputOperatorPod._materialize_upstream(rows, self._stream)  # pyright: ignore[reportPrivateUsage]  # main's row path
            table = built.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
        # The rows' keys: a source whose keys() states its declared order has rows in
        # its table's order (``keys_of_rows``).
        return row_built_columns(table, keys_of_rows(table, self._stream.keys()))

    def close(self) -> None:
        """Stop reading: the batches not read are dropped."""
        self._batches = deque()

    @property
    def opened_on(self) -> StreamProtocol | None:
        """The complete stream this feed reads."""
        return self._stream


def typed_arrow_schemas(
    stream: object, *, own: bool = True
) -> tuple[pa.Schema, pa.Schema]:
    """Return the Arrow schemas of ``stream``'s tag and data columns (ITL-640).

    The Arrow types its rows have, from ``typed_zero_row_table`` (never computing,
    reading or polling a row): the tag schema holds the columns of the stream's Python
    tag schema, the data schema those of its data schema (a data-tag in both), each in
    that order. A
    stream that cannot type itself without rows (an unbound input, a polling source not
    yet polled) answers with the data context's conversion of its declared Python
    schema, the Arrow types its identity is then fixed to.

    A source or an input slot states its Arrow schemas itself (``arrow_schemas``),
    which is what this returns for one.

    Args:
        stream: The stream, source or node.
        own: Whether to return the ``arrow_schemas`` the stream states, when it has
            them; ``False`` derives them (the stream's own derivation).

    Returns:
        The tag and data Arrow schemas.
    """
    if own:
        stated = getattr(stream, "arrow_schemas", None)
        if isinstance(stated, tuple):
            return cast("tuple[pa.Schema, pa.Schema]", stated)
    typed = cast(StreamProtocol, stream)
    # The columns are those of the Python output schema, which the identity has always
    # named; only their types come from the zero-row table.
    tag_schema, data_schema = typed.output_schema()
    table = typed_zero_row_table(stream)
    if table is None:
        from orcapod.utils.schema_utils import declared_arrow_schema

        converter = cast(Any, stream).data_context.type_converter
        return (
            declared_arrow_schema(tag_schema, converter),
            declared_arrow_schema(data_schema, converter),
        )
    schema = table.schema
    return (
        pa.schema([schema.field(name) for name in tag_schema]),
        pa.schema([schema.field(name) for name in data_schema]),
    )


def _derive_zero_row_table(stream: object) -> pa.Table | None:
    """Ask ``stream`` for its zero-row table and drop the meta columns; no memo."""
    method = getattr(stream, "_typed_zero_row_table", None)
    table = cast("pa.Table | None", method()) if callable(method) else None
    if table is None:
        return None
    # A row passed downstream carries no meta column (``_rows_to_table`` reads a row's
    # data with source info only), so neither does its zero-row table: a cached
    # function pod's ``__computed`` stays in that stream's own table.
    from orcapod.core.operators.meta_columns import without_meta_columns

    tag_columns = cast(StreamProtocol, stream).keys()[0]
    return without_meta_columns(table, tag_columns)


def check_head_size(n: int) -> None:
    """Check the row count given to ``head``.

    Raises:
        TypeError: If ``n`` is not an ``int``.
        ValueError: If ``n`` is negative.
    """
    if isinstance(n, bool) or not isinstance(n, int):
        raise TypeError(f"head() takes an int number of rows, got {type(n).__name__}")
    if n < 0:
        raise ValueError(f"head() takes a non-negative number of rows n, got {n}")


def _column_names(columns: tuple[str | Collection[str], ...]) -> tuple[str, ...]:
    """Normalize the positional names of ``drop_columns`` and ``select_columns``.

    The names are given as one or more strings, or as one collection of strings
    that is the only positional argument.

    Args:
        columns: The positional arguments as received.

    Returns:
        The names, in the order given.

    Raises:
        TypeError: When no name is given, a collection is mixed with further
            positional arguments, or a name is not a string.
        ValueError: When the one collection given is empty.
    """
    if not columns:
        raise TypeError("expected at least one column name")
    first = columns[0]
    if isinstance(first, str):
        names: tuple[object, ...] = columns
    elif len(columns) > 1:
        raise TypeError(
            "give the column names either as strings or as one collection of "
            "strings, not both"
        )
    elif isinstance(first, Collection):
        names = tuple(first)
        if not names:
            raise ValueError(
                "expected at least one column name, got an empty collection"
            )
    else:
        raise TypeError(f"expected column names as strings, got {type(first).__name__}")
    not_strings = [name for name in names if not isinstance(name, str)]
    if not_strings:
        raise TypeError(f"column names must be strings, got {not_strings!r}")
    return tuple(str(name) for name in names)


def _rename_map(
    name_map: Mapping[str, str] | None, renames: Mapping[str, object]
) -> dict[str, str]:
    """Normalize the renames of ``map_tags``, ``map_data`` and ``map_columns``.

    The renames are given either as one mapping, the only positional argument, or
    as keyword arguments, ``old="new"`` (ITL-1171). A name that is not a Python
    identifier, or that is one of the methods' options (``drop_unmapped``,
    ``strict``, ``label``), can be renamed only through the mapping.

    Args:
        name_map: The positional mapping, or ``None`` when none was given.
        renames: The keyword renames.

    Returns:
        The renames as one dict, in the order given; empty when there are none.

    Raises:
        TypeError: When both a mapping and keyword renames are given, or an old or
            new name is not a string.
    """
    if name_map is not None and renames:
        raise TypeError(
            "give the renames either as one dict (map_columns({'old': 'new'})) or as "
            "keyword arguments (map_columns(old='new')), not both"
        )
    pairs = dict(name_map if name_map is not None else renames)
    not_strings = {
        old: new
        for old, new in pairs.items()
        if not isinstance(old, str) or not isinstance(new, str)
    }
    if not_strings:
        raise TypeError(
            f"old and new column names must be strings, got {not_strings!r}"
        )
    return {str(old): str(new) for old, new in pairs.items()}


class StreamBase(TraceableBase):
    @property
    @abstractmethod
    def producer(self) -> PodProtocol | None: ...

    @property
    @abstractmethod
    def upstreams(self) -> tuple[StreamProtocol, ...]: ...

    def identity_structure(self) -> Any:
        if self.producer is not None:
            return (self.producer, self.producer.argument_symmetry(self.upstreams))

        raise NotImplementedError("StreamBase.identity_structure")

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    @property
    def is_stale(self) -> bool:
        """
        True if any upstream stream or the source pod has a ``last_modified``
        timestamp strictly newer than this stream's own ``last_modified``,
        indicating that any in-memory cached content should be discarded and
        repopulated.

        Semantics:
        - A ``None`` timestamp on *this* stream means "content not yet
          established" → always stale.
        - A ``None`` timestamp on an upstream or source means "modification
          time unknown" → conservatively treat as stale.
        - Immutable streams with no upstreams and no source (e.g.
          ``ArrowTableStream``) always return ``False``.
        """
        own_time: datetime | None = self.last_modified
        if own_time is None:
            return True
        candidates: list[datetime | None] = [s.last_modified for s in self.upstreams]
        if self.producer is not None:
            candidates.append(self.producer.last_modified)
        return any(t is None or t > own_time for t in candidates)

    def computed_label(self) -> str | None:
        if self.producer is not None:
            # use the invocation operation label
            return self.producer.label
        return None

    def join(
        self, other_stream: StreamProtocol, label: str | None = None
    ) -> StreamBase:
        """
        Joins this stream with another stream, returning a new stream that contains
        the combined data from both streams.
        """
        from orcapod.core.operators import Join

        return Join()(self, other_stream, label=label)

    def rejoin(
        self, *other_streams: StreamProtocol, label: str | None = None
    ) -> StreamBase:
        """Put this stream back together with other branches of one stream.

        Pairs the rows that come from the same origin row, by matching every system-tag
        column, and keeps those columns unchanged (``Rejoin``, provisional). Rows must
        also be equal in every other column the streams share (a tag several have, the
        data context); a row that matches nothing is left out. For a diamond (one
        stream split into branches, then put back together); to combine different rows
        on their tags, use ``join``.

        Args:
            *other_streams: The other branches, one or more.
            label: The result's label.

        Returns:
            The rejoined stream.
        """
        from orcapod.core.operators import Rejoin

        return Rejoin()(self, *other_streams, label=label)

    def semi_join(
        self,
        other_stream: StreamProtocol,
        label: str | None = None,
    ) -> StreamBase:
        """
        Performs a semi-join with another stream, returning a new stream that contains
        only the data from this stream that have matching tags in the other stream.
        """
        from orcapod.core.operators import SemiJoin

        return SemiJoin()(self, other_stream, label=label)

    def map_tags(
        self,
        name_map: Mapping[str, str] | None = None,
        /,
        *,
        drop_unmapped: bool = False,
        strict: bool = True,
        label: str | None = None,
        tag_only: bool = False,
        **renames: str,
    ) -> StreamBase:
        """Rename tag columns. See ``orcapod.core.operators.MapTags``.

        The renames are one dict, the only positional argument (``map_tags({"subject":
        "animal"})``), or keyword arguments (``map_tags(subject="animal")``), not both.
        A name that is not a Python identifier, or a column named ``drop_unmapped``,
        ``strict``, ``label`` or ``tag_only`` (the options), is renamed through the
        dict. A data-tag is renamed in both roles unless ``tag_only`` is
        ``True``; see ``MapTags``.

        Args:
            name_map: Each tag column to rename, mapped to its new name.
            drop_unmapped: When ``True``, the tags not in ``name_map`` are dropped.
            strict: When ``True`` (the default), a key of ``name_map`` that is not
                a tag column raises ``InputValidationError``; when ``False`` it is
                ignored.
            label: Optional node label for the pipeline graph.
            tag_only: Rename data-tags in the tag role only (ITL-1023).
            **renames: Each column to rename, mapped to its new name, as
                keyword arguments.

        Returns:
            The stream with the tag columns renamed.

        Raises:
            TypeError: When both a dict and keyword renames are given, or a
                name is not a string.
            InputValidationError: When a ``strict`` name is not a tag column, or a
                new name collides with an existing column.
        """
        from orcapod.core.operators import MapTags

        return MapTags(
            _rename_map(name_map, renames),
            drop_unmapped,
            strict=strict,
            tag_only=tag_only,
        )(self, label=label)

    def map_data(
        self,
        name_map: Mapping[str, str] | None = None,
        /,
        *,
        drop_unmapped: bool = False,
        strict: bool = True,
        label: str | None = None,
        **renames: str,
    ) -> StreamBase:
        """Rename data columns. See ``orcapod.core.operators.MapData``.

        The renames are one dict, the only positional argument (``map_data({"age":
        "age_weeks"})``), or keyword arguments (``map_data(age="age_weeks")``), not
        both. A name that is not a Python identifier, or a column named
        ``drop_unmapped``, ``strict`` or ``label`` (the options), is renamed through the
        dict.

        Args:
            name_map: Each data column to rename, mapped to its new name.
            drop_unmapped: When ``True``, the data columns not in ``name_map``
                are dropped.
            strict: When ``True`` (the default), a key of ``name_map`` that is not
                a data column raises ``InputValidationError``; when ``False`` it is
                ignored.
            label: Optional node label for the pipeline graph.
            **renames: Each column to rename, mapped to its new name, as
                keyword arguments.

        Returns:
            The stream with the data columns renamed.

        Raises:
            TypeError: When both a dict and keyword renames are given, or a
                name is not a string.
            InputValidationError: When a ``strict`` name is not a data column, a
                new name collides with an existing column, or no data column
                would remain.
        """
        from orcapod.core.operators import MapData

        return MapData(_rename_map(name_map, renames), drop_unmapped, strict=strict)(
            self, label=label
        )

    def map_columns(
        self,
        name_map: Mapping[str, str] | None = None,
        /,
        *,
        drop_unmapped: bool = False,
        strict: bool = True,
        label: str | None = None,
        **renames: str,
    ) -> StreamBase:
        """Rename columns, tag or data, in one step.

        A renamed tag stays a tag and a renamed data column stays data. See
        ``orcapod.core.operators.MapColumns``.

        The renames are one dict, the only positional argument (``map_columns({"age":
        "age_weeks"})``), or keyword arguments (``map_columns(age="age_weeks")``), not
        both. A name that is not a Python identifier, or a column named
        ``drop_unmapped``, ``strict`` or ``label`` (the options), is renamed through the
        dict.

        Args:
            name_map: Each column to rename, tag or data, mapped to its new name.
            drop_unmapped: When ``True``, every tag and data column not in
                ``name_map`` is dropped; system tags are kept.
            strict: When ``True`` (the default), a key of ``name_map`` that is
                neither a tag nor a data column raises ``InputValidationError``;
                when ``False`` it is ignored.
            label: Optional node label for the pipeline graph.
            **renames: Each column to rename, mapped to its new name, as
                keyword arguments.

        Returns:
            The stream with the columns renamed.

        Raises:
            TypeError: When both a dict and keyword renames are given, or a
                name is not a string.
            InputValidationError: When a ``strict`` name is missing, a new name
                collides with another column, or no data column would remain.
        """
        from orcapod.core.operators import MapColumns

        return MapColumns(_rename_map(name_map, renames), drop_unmapped, strict=strict)(
            self, label=label
        )

    def batch(
        self,
        batch_size: int = 0,
        drop_partial_batch: bool = False,
        label: str | None = None,
    ) -> StreamBase:
        """
        Batch stream into fixed-size chunks, each of size batch_size.
        If drop_last is True, any remaining elements that don't fit into a full batch will be dropped.
        """
        from orcapod.core.operators import Batch

        return Batch(batch_size=batch_size, drop_partial_batch=drop_partial_batch)(
            self, label=label
        )

    def group_by(
        self,
        by: str | Collection[str],
        label: str | None = None,
        *,
        member_order: str | Sequence[str] | None = None,
    ) -> StreamBase:
        """Reduce rows sharing a key into one row per group.

        Group-key columns stay scalar tags and are the only tags of the output;
        every other tag is dropped, and every data column becomes list-valued.
        A dropped tag's values are not carried: make the column data before
        grouping if a later step needs it.  The members of a group are ordered
        by the non-key tags in name order, or as ``member_order`` gives.  See
        ``orcapod.core.operators.GroupBy``.

        Args:
            by: Tag column names to group on, or one name as a string.
            label: Optional node label for the pipeline graph.
            member_order: ``None`` or ``"name"`` (the non-key tags in name
                order), or a list of non-key tag names to order the members by
                first.

        Returns:
            A stream with one row per distinct group-key tuple.
        """
        from orcapod.core.operators import GroupBy

        return GroupBy(by=by, member_order=member_order)(self, label=label)

    def polars_filter(
        self,
        *predicates: Any,
        constraint_map: Mapping[str, Any] | None = None,
        label: str | None = None,
        **constraints: Any,
    ) -> StreamBase:
        from orcapod.core.operators import PolarsFilter

        total_constraints = dict(constraint_map) if constraint_map is not None else {}

        total_constraints.update(constraints)

        return PolarsFilter(predicates=predicates, constraints=total_constraints)(
            self, label=label
        )

    def filter(
        self,
        *conditions: Condition | Mapping[str, object],
        label: str | None = None,
        **columns: object,
    ) -> StreamBase:
        """Keep the rows that satisfy every condition; a step of the pipeline.

        ``s.filter(session="s1")``, ``s.filter({"session": "s1"})`` and
        ``s.filter(col("t") >= 1.0, col("unit").is_in([3, 7]), session="s1")``:
        keywords and mappings are equalities, every other condition is built with
        ``col``; all of them must hold. Tag and data columns are named alike. The
        output has this stream's columns and types; only the rows change. See
        ``orcapod.core.operators.Filter``.

        Args:
            *conditions: Conditions (built with ``col`` or ``Condition.of``) and
                mappings of column name to value.
            label: Optional node label for the pipeline graph. A column named
                ``label`` is written ``filter({"label": ...})`` or with ``col``.
            **columns: Column name to the value it must equal.

        Returns:
            A stream over the kept rows, in input order.

        Raises:
            ValueError: If no condition is given.
            TypeError: If an argument is neither a condition nor a mapping, or a
                value is not exactly of a supported type.
            InputValidationError: If a column is missing or a value does not fit
                its column.
        """
        from orcapod.core.operators import Filter

        return Filter(Condition.of(*conditions, **columns))(self, label=label)

    def explain(self) -> Explanation:
        """Say what each source applies itself, and what is applied after reading.

        For every source this stream reads (through a job's input slot to its bound
        source): its own restriction (``where``, ``limit``), or that it reads its whole
        table; and the steps above it that drop rows after they are read (a ``Filter``, a
        ``DropNulls``, a join's unmatched rows), nearest the source first. New in this
        release (ITL-1402).

        Nothing is computed, read, polled or written, and no user code runs: only what the
        chain declares is read.

        Returns:
            An ``Explanation``: plain data, with ``text()`` (also its ``str``).
        """
        from orcapod.core.streams.explain import explain

        return explain(self)

    def drop_nulls(
        self,
        columns: str | Collection[str] | None = None,
        label: str | None = None,
    ) -> StreamBase:
        """Keep the rows where every named column is non-null; narrow those columns to ``T``.

        Each named column, tag or data, is non-nullable afterwards (``T``
        instead of ``T | None``), so a pod whose parameter is a plain ``T``
        can consume it. Without names, every tag and data column of this
        stream is tested and narrowed: a row with a null in any of them is
        dropped. See ``orcapod.core.operators.DropNulls``.

        Args:
            columns: One column name, a collection of names, or ``None`` (the
                default) for every tag and data column. An empty collection
                raises ``ValueError``.
            label: Optional node label for the pipeline graph.

        Returns:
            A stream over the surviving rows, in input order.
        """
        from orcapod.core.operators import DropNulls

        return DropNulls(columns)(self, label=label)

    def tag_by(
        self,
        *columns: str | Iterable[str],
        label: str | None = None,
    ) -> StreamBase:
        """Give the named data columns the tag role, as data-tags.

        ``s.tag_by("x", "y")`` equals ``s.tag_by(["x", "y"])``,
        ``s.tag_by({"y", "x"})`` and ``s.tag_by("y", "x")``. Each named data
        column becomes a data-tag: one column, both a tag and the data column
        it was, with its values and source info unchanged. The new tags follow this
        stream's tags, in name order. See ``orcapod.core.operators.TagBy``.

        Args:
            *columns: One or more data column names, or one iterable of names
                (a list, a tuple, a set) as the only positional argument.
            label: Optional node label for the pipeline graph.

        Returns:
            The stream with the named columns as data-tags.

        Raises:
            TypeError: When a name is not a string.
            ValueError: When a name is repeated.
            InputValidationError: When a name is reserved, is not a data column of
                this stream, is already a tag, or has a type that cannot be a tag.
        """
        from orcapod.core.operators import TagBy

        if len(columns) == 1 and not isinstance(columns[0], str):
            names: str | Iterable[str] = columns[0]
        else:
            names = cast("tuple[str, ...]", columns)
        return TagBy(names)(self, label=label)

    def add_tags(
        self,
        tags: Mapping[str, bool | int | float | str | bytes] | None = None,
        /,
        *,
        label: str | None = None,
        **values: bool | int | float | str | bytes,
    ) -> StreamBase:
        """Add tags with constant values to every row. See ``orcapod.core.operators.AddTags``.

        The tags are one dict, the only positional argument (``add_tags({"cohort":
        "A"})``), or keyword arguments (``add_tags(cohort="A", run=3)``), not both. A
        tag named ``label``, or a name that is not a Python identifier, is given
        through the dict. Each new tag holds the same value on every row and follows
        the existing tags, in name order: the new tags are a set, and the order they are
        given in is not kept (ITL-1269), so ``add_tags(run=3, cohort="A")`` places
        ``cohort`` before ``run``. A value is a ``bool``, ``int``,
        ``float``, ``str`` or ``bytes``, and its type is the tag's type. Data
        columns, their values and their source info are unchanged.

        Args:
            tags: Each new tag's name mapped to its value.
            label: Optional node label for the pipeline graph.
            **values: Each new tag's name mapped to its value, as keyword
                arguments.

        Returns:
            The stream with the new tag columns.

        Raises:
            TypeError: When both a dict and keyword tags are given, a name is not
                a string, or a value's type is not accepted.
            ValueError: When no tag is given, or a value is out of range.
            InputValidationError: When a name is reserved, or already a tag or
                data column of this stream.
        """
        from orcapod.core.operators import AddTags

        if tags is not None and values:
            raise TypeError(
                "give the tags either as one dict (add_tags({'cohort': 'A'})) or as "
                "keyword arguments (add_tags(cohort='A')), not both"
            )
        return AddTags(dict(tags if tags is not None else values))(self, label=label)

    def select_tag_columns(
        self,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> StreamBase:
        """
        Select the specified tag columns from the stream. A ValueError is raised
        if one or more specified tag columns do not exist in the stream unless strict = False.
        """
        from orcapod.core.operators import SelectTagColumns

        return SelectTagColumns(tag_columns, strict=strict)(self, label=label)

    def select_data_columns(
        self,
        data_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> StreamBase:
        """
        Select the specified data columns from the stream. A ValueError is raised
        if one or more specified data columns do not exist in the stream unless strict = False.
        """
        from orcapod.core.operators import SelectDataColumns

        return SelectDataColumns(data_columns, strict=strict)(self, label=label)

    def drop_tag_columns(
        self,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> StreamBase:
        from orcapod.core.operators import DropTagColumns

        return DropTagColumns(tag_columns, strict=strict)(self, label=label)

    def drop_data_columns(
        self,
        data_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> StreamBase:
        from orcapod.core.operators import DropDataColumns

        return DropDataColumns(data_columns, strict=strict)(self, label=label)

    def drop_columns(
        self,
        *columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> StreamBase:
        """Drop the named columns, whether each is a tag or a data column.

        ``s.drop_columns("a", "b")`` equals ``s.drop_columns(["a", "b"])``. A
        dropped data column's source-info column goes with it; system tags are
        kept. See ``orcapod.core.operators.DropColumns``.

        Args:
            *columns: One or more column names, or one collection of names as the
                only positional argument.
            strict: When ``True`` (the default), a name that is neither a tag nor
                a data column raises ``InputValidationError``; when ``False`` it
                is ignored.
            label: Optional node label for the pipeline graph.

        Returns:
            The stream without the named columns.

        Raises:
            TypeError: When no name is given, a collection is mixed with further
                positional arguments, or a name is not a string.
            ValueError: When the one collection given is empty.
            InputValidationError: When a ``strict`` name is missing, or when no
                data column would remain.
        """
        from orcapod.core.operators import DropColumns

        return DropColumns(_column_names(columns), strict=strict)(self, label=label)

    def select_columns(
        self,
        *columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> StreamBase:
        """Keep exactly the named columns, tag or data; drop every other one.

        ``s.select_columns("a", "b")`` equals ``s.select_columns(["a", "b"])``.
        Every user tag and data column not named is dropped, with the source-info
        column of each dropped data column; system tags are kept. Naming no tag
        is allowed. See ``orcapod.core.operators.SelectColumns``.

        Args:
            *columns: One or more column names, or one collection of names as the
                only positional argument.
            strict: When ``True`` (the default), a name that is neither a tag nor
                a data column raises ``InputValidationError``; when ``False`` it
                is ignored.
            label: Optional node label for the pipeline graph.

        Returns:
            The stream with only the named columns.

        Raises:
            TypeError: When no name is given, a collection is mixed with further
                positional arguments, or a name is not a string.
            ValueError: When the one collection given is empty.
            InputValidationError: When a ``strict`` name is missing, or when no
                data column is named.
        """
        from orcapod.core.operators import SelectColumns

        return SelectColumns(_column_names(columns), strict=strict)(self, label=label)

    def pick(
        self,
        column: str,
        key: str,
        out: str | None = None,
        fail_on_miss: bool = False,
        label: str | None = None,
    ) -> StreamBase:
        """Extract a value from a struct- or dict-typed data column by key.

        For ``dict[K, V]`` columns the lookup is per-packet; if the key is absent
        the packet is skipped (or an error is raised when ``fail_on_miss=True``).

        Args:
            column: Name of the data column to project into.
            key: Dict key or struct field name to extract.
            out: Output column name. ``None`` (default) replaces ``column``
                in-place; a string adds a new column alongside the original.
            fail_on_miss: If ``True``, raise ``RuntimeError`` when the key is
                absent in a packet instead of skipping.
            label: Optional label for the operator node.

        Returns:
            A new stream with the projected column.
        """
        from orcapod.core.operators import Pick

        return Pick(column, key, out=out, fail_on_miss=fail_on_miss)(self, label=label)

    def index(
        self,
        column: str,
        i: int,
        out: str | None = None,
        fail_on_miss: bool = False,
        label: str | None = None,
    ) -> StreamBase:
        """Extract an element from a list-typed data column by position.

        Out-of-bounds access causes the packet to be skipped (or an error
        when ``fail_on_miss=True``). Negative indices follow Python semantics
        (``-1`` is the last element).

        Args:
            column: Name of the data column to project into.
            i: Position to extract. Negative indices follow Python semantics.
            out: Output column name. ``None`` (default) replaces ``column``
                in-place; a string adds a new column alongside the original.
            fail_on_miss: If ``True``, raise ``RuntimeError`` on out-of-bounds
                instead of skipping.
            label: Optional label for the operator node.

        Returns:
            A new stream with the projected column.
        """
        from orcapod.core.operators import Index

        return Index(column, i, out=out, fail_on_miss=fail_on_miss)(self, label=label)

    DERIVES_UNIQUE_KEYS: ClassVar[bool] = False
    """Whether ``unique_keys`` is derived for this class: read for the exact class only,
    each declaring it, and only while no member is replaced on the instance
    (``core/unique_keys``)."""

    @property
    def unique_keys(self) -> UniqueKeys:
        """The tag sets no two rows agree on (``core/unique_keys``).

        None by default: a stream that derives or checks them says so itself.
        """
        return NO_UNIQUE_KEYS

    def arrow_schema(self) -> pa.Schema:
        """The Arrow schema of the columns this stream passes between job nodes.

        See ``stream_arrow_schema`` (ITL-1347): never reads, computes or polls a row.
        """
        return stream_arrow_schema(self)

    @property
    def arrow_types_declared(self) -> bool:
        """Whether ``arrow_schema()`` holds the rows' own Arrow types (ITL-1089, ITL-1347)."""
        return typed_zero_row_table(self) is not None

    @abstractmethod
    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]: ...

    @abstractmethod
    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]: ...

    # Predicted output schemas by normalized column config; see
    # ``_memoized_output_schema``. Set on first use, so a subclass that asks for its
    # schema before ``StreamBase.__init__`` runs still works.
    _output_schema_memo: dict[ColumnConfig, tuple[Schema, Schema]] | None = None

    def _memoized_output_schema(
        self,
        columns: ColumnConfig | dict[str, Any] | None,
        all_info: bool,
        predict: Callable[[], tuple[Schema, Schema]],
    ) -> tuple[Schema, Schema]:
        """Return ``predict()``, computed once per normalized column config (ITL-1111).

        A stream derived from upstreams predicts its schema by asking them for
        theirs. Without a memo, a graph in which one stream feeds two branches that
        join again asks the shared upstream once per path, so stacked
        split-and-join stages cost 4x per stage. With it, every graph walk that
        asks for schemas is linear in the nodes.

        Only the prediction is kept: a ``(tag_schema, data_schema)`` pair of
        immutable ``Schema`` objects, never rows or a table. The caller states what
        its prediction depends on, and clears the memo
        (``_forget_output_schema``) when one of those things changes. A config
        that cannot be a dict key (``meta`` given as a list of names) is predicted
        every time.

        Args:
            columns: The column config as the caller received it.
            all_info: The ``all_info`` flag as the caller received it.
            predict: Computes the schema from ``columns`` and ``all_info``.

        Returns:
            The predicted ``(tag_schema, data_schema)``.
        """
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        try:
            hash(config)
        except TypeError:
            return predict()
        if self._output_schema_memo is None:
            self._output_schema_memo = {}
        memo = self._output_schema_memo
        if config not in memo:
            memo[config] = predict()
        return memo[config]

    def _forget_output_schema(self) -> None:
        """Drop the memoized output schemas: an input of the prediction changed."""
        self._output_schema_memo = None

    def __iter__(
        self,
    ) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        return self.iter_data()

    @abstractmethod
    def iter_data(
        self,
    ) -> Iterator[tuple[TagProtocol, DataProtocol]]: ...

    def open_channel(self) -> RecordBatchInput:
        """Open an input channel on this stream, for async computation (ITL-1347).

        The channel delivers this stream's rows as Arrow record batches of at most
        ``SOURCE_BATCH_ROWS`` rows, in the row-built form, read once and in order; its
        description is this stream (``channel.stream``). Several channels may be
        opened on one stream, each with its own position, and the stream stays as it
        is. Nothing is read until the channel is first read; then the stream is read
        once, a fixed snapshot.

        Returns:
            The input channel.
        """
        return InputChannel(self, CompleteStreamFeed(self))

    async def async_iter_data(
        self,
    ) -> AsyncIterator[tuple[TagProtocol, DataProtocol]]:
        """Async iterator over (tag, data) pairs.

        Default implementation wraps ``iter_data`` as an async generator.
        Subclasses override this to provide true async streaming behaviour
        (e.g. ``PollingSource``).
        """
        for item in self.iter_data():
            yield item

    @abstractmethod
    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table": ...

    def as_carried_table(
        self, *, columns: ColumnConfig | dict[str, Any] | None = None
    ) -> "pa.Table":
        """The table of ``as_table(columns=columns)``, as operators pass it on.

        A source-info column may be derived (``utils/source_tokens.py``): a recipe over
        the table's system tags in place of its text, which ``source_tokens.materialize``
        builds. Internal: never stored, hashed or shown (ITL-1464). A stream that holds
        none gives ``as_table``.
        """
        return self.as_table(columns=columns)

    def as_polars_df(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pl.DataFrame":
        """
        Convert the entire stream to a Polars DataFrame.
        """
        return pl.DataFrame(
            self.as_table(
                columns=columns,
                all_info=all_info,
            )
        )

    def as_df(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pl.DataFrame":
        """
        Convert the entire stream to a Polars DataFrame.
        """
        return self.as_polars_df(
            columns=columns,
            all_info=all_info,
        )

    def as_lazy_frame(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pl.LazyFrame":
        """
        Convert the entire stream to a Polars LazyFrame.
        """
        df = self.as_polars_df(
            columns=columns,
            all_info=all_info,
        )
        return df.lazy()

    def as_pandas_df(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        index_by_tags: bool = False,
        all_info: bool = False,
    ) -> "pd.DataFrame":
        df = self.as_polars_df(
            columns=columns,
            all_info=all_info,
        )
        tag_keys, _ = self.keys()
        pdf = df.to_pandas()
        if index_by_tags:
            pdf = pdf.set_index(list(tag_keys))
        return pdf

    def flow(
        self,
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Materialize the stream into a concrete collection of
        ``(TagProtocol, DataProtocol)`` pairs.

        This is implemented by iterating over ``iter_data``. Depending on
        the concrete stream implementation, iterating may trigger computation or
        upstream work, or it may simply materialize already-computed results.
        """
        return [e for e in self.iter_data()]

    # The origin a ``run()`` view gives its rows: computed by this stream.
    _RUN_ORIGIN: str = "computed"

    def run(self) -> StreamView:
        """Compute the whole stream and return a ``StreamView`` of all its rows.

        The one way to compute any stream outside a job (developer, November 13, 2026,
        ITL-1081): ``head()`` and the notebook preview never compute a row (see
        ``head``). ``run()`` computes every row, in the order of ``as_table()``, with
        every effect, and promises nothing about how the rows are produced or held
        (developer, November 24, 2026, ITL-1464). A stream that keeps its computed rows
        is not computed again, and one whose inputs changed is computed again. The view
        has the default column configuration;
        its ``head``, ``tail`` and ``sort_by_tags`` choose what is shown. The view holds
        every row, except over a source that reads its rows on demand
        (``reads_in_batches``): its rows, and those of operators applied batch by batch
        over it, are read in batches, and a result of more than
        ``RUN_VIEW_COMPLETE_ROWS`` rows gives a ``BoundedStreamView``. For other
        column groups, use ``as_table(columns=...)`` or ``head(columns=...)`` after
        ``run()``. On a stream that keeps its rows (a pod's or operator's output stream,
        a job node, a job's side-effect node included, an in-memory source) they read the
        rows ``run()`` gave and deliver no side effect; a stream that keeps none does its
        ``as_table()`` work again (a side-effect stream outside a job delivers again, a
        polling source polls again).

        Returns:
            A ``StreamView`` of all the rows ``as_table()`` returns, with status
            ``ROWS``; bounded as said above.
        """
        if self._reads_in_batches():
            return self._run_in_batches()
        return self._view_of_rows(self.as_table(), self._RUN_ORIGIN)

    def _reads_in_batches(self) -> bool:
        """See ``reads_in_batches``; a stream reads its rows whole unless it says."""
        return False

    def _batch_streams_of_rows(self) -> Generator[StreamProtocol, None, None]:
        """See ``batch_streams_of``; asked only when ``_reads_in_batches``."""
        raise NotImplementedError(f"{type(self).__name__} has no rows in batches")

    def _run_in_batches(self) -> StreamView:
        """The ``run()`` view of rows read batch by batch, in one read."""
        rows = WindowOfRows()
        with closing(self._batch_streams_of_rows()) as batches:
            for batch in batches:
                rows.add(batch.as_table())
        complete = rows.complete
        if complete is not None:
            return self._view_of_rows(complete, self._RUN_ORIGIN)
        first = self._view_of_rows(rows.first, self._RUN_ORIGIN)
        return BoundedStreamView(first, rows.last, rows.total)

    def _view_of_rows(self, table: "pa.Table", origin: str) -> StreamView:
        """Return the ``run()`` view of ``table``, every row of this stream."""
        return StreamView.of_rows(
            self,
            table,
            tag_columns=self.keys()[0],
            total_rows=table.num_rows,
            origin=origin,
            display_all_rows=False,
        )

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return at most ``n`` rows that are readily available, without computing.

        The rows are ones the stream already holds, has cached from an earlier
        computation, has stored, or can read with a bounded read of its table. ``head``
        never computes, polls or delivers a row (it reads only what is held or stored,
        or makes a bounded table read), never calls user code (a
        function, a predicate, a side effect) and never writes. With no rows to show, it
        learns the column types without data (ITL-912): each operator of the stream is
        applied to zero-row inputs, which processes no data, so a chain of ``N``
        operators applies up to ``N`` operators to empty tables (about 60 ms for 20); an
        operator whose output schema is its input's (``OUTPUT_SCHEMA_IS_INPUT_SCHEMA``,
        such as ``PolarsFilter``) is not applied at all. A user-defined operator's
        ``static_process`` is applied to zero-row tables and must not have effects. So a pipeline whose inputs
        disagree on a column's Arrow type raises from ``head`` the
        ``InputValidationError`` its run raises, and a filter with an ill-typed
        predicate shows its input's columns: the error appears at the run. ``head`` takes the column configuration of ``as_table``,
        sorting included, and applies it to the rows the stream has, then takes the first
        ``n``. The default configuration does not sort, as ``as_table()`` does not, so
        the rows are the first ``n`` that ``as_table()`` would return. With
        ``sort_by_tags`` (or ``all_info``) a stream that has all of its rows (a node's
        stored slice, a computed stream, an in-memory source) returns the first ``n``
        of ``as_table`` with that configuration. A stream that has read or computed only
        part of its rows (a source's bounded read, a partly computed stream) sorts those
        rows: the rows read, in tag order, not the first ``n`` in tag order. The
        notebook preview of a stream is its ``head()`` asked for ``sort_by_tags``.

        This default shows no rows: a stream class that can serve rows without computing
        overrides it. The zero-row table then has the stream's columns, typed from its
        declared (Python) schema, so a column's Arrow width can differ from the one the
        computed stream will have (``int`` shows as ``int64``). That zero-row table has
        the declared columns only: never a ``_source_*``, ``_context_key`` or
        content-hash column, whatever ``columns`` asks for, because whether such a column
        exists and, for ``_source_*``, its type are known only from rows.

        Args:
            n: The largest number of rows to return.
            columns: Column groups to include and whether to sort by tags, exactly as
                for ``as_table``.
            all_info: Include every column group and sort by tags, as for ``as_table``.

        Returns:
            A ``StreamView`` with the rows and a ``status`` that tells an empty result
            from one that is not computed.

        Raises:
            TypeError: If ``n`` is not an ``int``.
            ValueError: If ``n`` is negative.
        """
        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        return self._head_without_rows(config, ViewStatus.NOT_COMPUTED)

    def _head_without_rows(
        self,
        config: ColumnConfig,
        status: ViewStatus,
        note: str | None = None,
    ) -> StreamView:
        """Return a head with no rows: the stream's columns, typed as its rows are.

        The one place a head's table of no rows is built (ITL-912): the stream's
        ``_zero_row_table(config)``, which has the columns and Arrow types
        ``as_table(columns=config)`` gives the stream with no rows; getting it
        computes, reads, polls and delivers nothing. Only a stream that cannot say its
        Arrow types without rows (see ``typed_zero_row_table``; ITL-1089) gets a table
        built from its declared Python schema. When reading the schema would poll a source
        (``schema_requires_io``), the head has no columns either, and the note says so.
        """
        if schema_requires_io(self, config):
            return StreamView.without_rows(
                self,
                status,
                pa.table({}),
                tag_columns=(),
                note=f"{note or default_note(status)}; {SCHEMA_NEEDS_POLL_NOTE}",
            )
        tag_schema, data_schema = self.output_schema(columns=config)
        table = self._zero_row_table(config)
        if table is None:
            table = arrow_utils.make_empty_table(
                {**tag_schema, **data_schema}, self.data_context.type_converter
            )
        return StreamView.without_rows(
            self, status, table, tag_columns=tuple(tag_schema.keys()), note=note
        )

    def _display_name(self) -> str:
        """The name a preview header shows: the label. Never used as an identity."""
        return self.label

    def _schema_requires_io(self, config: ColumnConfig) -> bool:
        """Whether reading this stream's schema with ``config`` would read or poll a source.

        True when an upstream's would: a ``PollingSource`` that has not polled yet
        answers its schema only by polling when it declared no schema, or when
        ``config`` asks for a column group known only from rows.

        Each upstream is visited once, however many paths lead to it (ITL-1111): a
        stream that answers like this one is passed through to its own upstreams,
        any other is asked. The answer is not kept, because a source's changes once
        it has polled.
        """
        seen: set[int] = {id(self)}
        pending: list[StreamProtocol] = list(self.upstreams)
        while pending:
            stream = pending.pop()
            if id(stream) in seen:
                continue
            seen.add(id(stream))
            hook = getattr(type(stream), "_schema_requires_io", None)
            if hook is StreamBase._schema_requires_io:
                pending.extend(stream.upstreams)
            elif schema_requires_io(stream, config):
                return True
        return False

    def _zero_row_table(self, config: ColumnConfig) -> pa.Table | None:
        """This stream's table with no rows and the column groups of ``config``.

        What ``as_table(columns=config)`` gives this stream when it has no rows: its
        zero-row table (``typed_zero_row_table``) shaped as an ``ArrowTableStream``
        shapes its table. A kind whose ``as_table`` shapes its table another way
        overrides this with that shaping. Never computes, reads, polls or delivers.

        Returns:
            The table, or ``None`` when the stream cannot type it without rows.
        """
        from orcapod.core.data_tags import stream_data_tags
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream

        typed = typed_zero_row_table(self)
        if typed is None:
            return None
        return ArrowTableStream(
            typed,
            tag_columns=self.keys()[0],
            data_tags=stream_data_tags(self),
            data_context=self.data_context_key,
        ).as_table(columns=config)

    def _typed_zero_row_table(self) -> pa.Table | None:
        """This stream's zero-row table; ``None``: unknown without rows.

        See ``typed_zero_row_table``. A stream whose Arrow types are known only from
        computed rows (a pod's stream) keeps this default.
        """
        return None

    def _repr_html_(self) -> str:
        """Render ``head()`` in tag order for a notebook; never computes."""
        return preview_html(self)
