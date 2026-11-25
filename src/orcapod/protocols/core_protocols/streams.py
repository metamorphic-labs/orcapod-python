from __future__ import annotations

from collections.abc import (
    AsyncIterator,
    Coroutine,
    Collection,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.datagrams import DataProtocol, TagProtocol
from orcapod.protocols.core_protocols.traceable import TraceableProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol
from orcapod.types import ColumnConfig, Condition, Schema

if TYPE_CHECKING:
    from orcapod.types.explanation import Explanation
    import pandas as pd
    import polars as pl
    import pyarrow as pa

    from orcapod.protocols.core_protocols.pod import PodProtocol


class StreamViewProtocol(Protocol):
    """Rows of a stream, as ``head`` and ``run`` return them.

    ``orcapod.core.streams.view.StreamView`` is the implementation.
    """

    @property
    def table(self) -> "pa.Table":
        """The rows, with the exact Arrow types the stream gives them."""
        ...

    @property
    def status(self) -> str:
        """Where the rows came from, or why there are none (a ``ViewStatus``)."""
        ...

    @property
    def total_rows(self) -> int | None:
        """The stream's row count when it is known without computing."""
        ...

    def header(self) -> str:
        """The one-line description shown above the rows."""
        ...

    def as_table(self) -> "pa.Table":
        """The rows as a PyArrow table."""
        ...

    def as_polars_df(self) -> "pl.DataFrame":
        """The rows as a Polars DataFrame."""
        ...

    def __len__(self) -> int:
        """The number of rows in ``table``."""
        ...

    def head(self, n: int = 10) -> "StreamViewProtocol":
        """A view of the first ``n`` rows of this view; never reads or computes."""
        ...

    def tail(self, n: int = 10) -> "StreamViewProtocol":
        """A view of the last ``n`` rows of this view; never reads or computes."""
        ...

    def sort_by_tags(self) -> "StreamViewProtocol":
        """A view of this view's rows in tag order; never reads or computes."""
        ...

    def _repr_html_(self) -> str:
        """The notebook preview of the rows."""
        ...


@runtime_checkable
class StreamProtocol(TraceableProtocol, PipelineElementProtocol, Protocol):
    """
    Base protocol for all streams in Orcapod.

    Streams represent sequences of (TagProtocol, DataProtocol) pairs flowing through the
    computational graph. They are the fundamental data structure connecting
    kernels and carrying both data and metadata.

    Streams can be either:
    - Static: Immutable snapshots created at a specific point in time
    - Live: Dynamic streams that stay current with upstream dependencies

    All streams provide:
    - Iteration over (tag, data) pairs
    - Type information and schema access
    - Lineage information (source kernel and upstream streams)
    - Basic caching and freshness tracking
    - Conversion to common formats (tables, dictionaries)
    """

    # TODO: add substream system

    @property
    def producer(self) -> "PodProtocol | None":
        """
        The pod that produced this stream, if any.

        This provides lineage information for tracking data flow through
        the computational graph. Root streams (like file sources) may
        have no source pod.

        Returns:
            PodProtocol: The source pod that created this stream
            None: This is a root stream with no source pod
        """
        ...

    @property
    def upstreams(self) -> tuple["StreamProtocol", ...]:
        """
        Input streams used to produce this stream.

        These are the streams that were provided as input to the source
        pod when this stream was created. Used for dependency tracking
        and cache invalidation. Note that `source` must be checked for
        upstreams to be meaningfully inspected.

        Returns:
            tuple[StreamProtocol, ...]: Upstream dependency streams (empty for sources)
        """
        ...

    def explain(self) -> "Explanation":
        """
        Say what each source this stream reads applies itself (its restriction, or its
        whole table) and which steps drop rows after reading; computes nothing.
        """
        ...

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """
        Available keys/fields in the stream content.

        Returns the field names present in both tags and data.
        This provides schema information without requiring type details,
        useful for:
        - Schema inspection and exploration
        - Query planning and optimization
        - Field validation and mapping

        Returns:
            tuple[tuple[str, ...], tuple[str, ...]]: (tag_keys, data_keys)
        """
        ...

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        Type specifications for the stream content.

        Returns the type schema for both tags and data in this stream.
        This information is used for:
        - Type checking and validation
        - Schema inference and planning
        - Compatibility checking between kernels

        Returns:
            tuple[Schema, Schema]: (tag_types, data_types)
        """
        ...

    def arrow_schema(self) -> "pa.Schema":
        """The Arrow schema of the columns the stream passes between job nodes.

        The user tags, system tags, data and source-info columns, in row order (user
        tags, system tags, data, source info; data and source info in data-key order),
        with the Arrow types, nullability and field metadata of the rows; no context and
        no meta column (ITL-1347). Known without reading, computing or polling a row.
        Three properties are decided by the values of a complete table, not by the
        description: whether a system-tag or source-info field is nullable (stated
        nullable here), and a source-info field's type (stated as the declared token
        type; a list of ``None`` or of empty lists shows fewer levels, so consumers must
        not rely on it). And a ``null`` column keeps the ``null`` type a zero-row table
        has (rows carry ``large_string``; ITL-1319).

        Returns:
            The declared Arrow schema.
        """
        ...

    @property
    def arrow_types_declared(self) -> bool:
        """Whether ``arrow_schema()`` holds the rows' own Arrow types.

        ``False`` when the stream cannot type itself without rows (an unbound slot
        declared in Python types, a ``PollingSource`` never polled; ITL-1089), or its
        rows decide its types (a narrower bind, an output held as its rows). An unbound
        slot declared with explicit Arrow schemas answers ``True``: its rows will have
        exactly those types, since a bind refuses a source of any other. Its schema is then the best
        description available without reading a row: a slot's or source's explicit
        Arrow declaration when it states one, otherwise its Python schema converted;
        a source whose schema is known only by polling, and which has not polled, has
        an empty schema. So a ``False`` description is not guaranteed to hold the
        stream's column names, and the names it holds come in no guaranteed order; no
        consumer relies on either (ITL-1347).
        """
        ...

    @property
    def unique_keys(self) -> frozenset[frozenset[str]]:
        """The tag sets no two rows of the stream agree on; empty certifies nothing.

        Derived from the inputs when asked, never stored; in no hash, identity, config,
        stored row or equality (``orcapod.core.unique_keys``, ITL-1464).
        """
        ...

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """
        Generates explicit iterator over (tag, data) pairs in the stream.

        Note that multiple invocation of `iter_data` may not always
        return an identical iterator.

        Yields:
            tuple[TagProtocol, DataProtocol]: Sequential (tag, data) pairs
        """
        ...

    def open_channel(self) -> RecordBatchInput:
        """Open an input channel on this stream, for async computation (ITL-1347).

        The channel delivers the stream's rows as Arrow record batches, read once and
        in order; its description is this stream. Several channels may be opened on
        one stream, each with its own position (``to_stream()`` makes a stream of what
        a channel delivers).

        Returns:
            The input channel.
        """
        ...

    def async_iter_data(self) -> AsyncIterator[tuple[TagProtocol, DataProtocol]]:
        """
        Generates asynchronous iterator over (tag, data) pairs in the stream.

        Note that multiple invocation of `async_iter_data` may not always
        return an identical iterator.

        Yields:
            tuple[tagProtocol, DataProtcol]: Asynchrnous sequential (tag, data) pairs

        """
        ...

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """
        Convert the entire stream to a PyArrow Table.

        Materializes all (tag, data) pairs into a single table for
        analysis and processing. This operation may be expensive for
        large streams or live streams that need computation.

        If include_content_hash is True, an additional column called "_content_hash"
        containing the content hash of each data is included. If include_content_hash
        is a string, it is used as the name of the content hash column.

        Returns:
            pa.Table: Complete stream data as a PyArrow Table
        """
        ...

    def run(self) -> "StreamViewProtocol":
        """Compute the whole stream and return a view of all its rows.

        Every row is computed, with every effect; nothing is promised about how the
        rows are produced or held (ITL-1464), and the view of a large result may hold
        only its ends (``BoundedStreamView``). A job node
        runs as it does in a job. Every stream has it, with no argument: it is the one
        way to compute a stream outside a job (ITL-1081). ``head()`` and the notebook
        preview never compute a row.

        Returns:
            StreamView: Every computed row, with the default column configuration.
        """
        ...

    def preview(
        self, n: int = 10, *, compute_all: bool = False
    ) -> "StreamViewProtocol":
        """Compute the first ``n`` rows and only what they need, keeping nothing.

        User functions are called; nothing is stored and nothing has an effect. A step
        that needs its whole input is refused unless ``compute_all`` (ITL-1464).

        Returns:
            StreamView: The first ``n`` rows of ``as_table()``.
        """
        ...

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "StreamViewProtocol":
        """Return at most ``n`` readily available rows, without computing.

        The rows are ones the stream already holds, has cached from an earlier
        computation, has stored, or can read with a bounded read. It never computes,
        polls or delivers a row (it reads only what is held or stored, or makes a bounded
        read), never calls user code (a function, a predicate,
        a side effect) and never writes. With no rows to show, it may apply operators
        to zero-row tables to learn the column types, which processes no data (ITL-912). A
        user-defined operator's ``static_process`` is applied to zero-row tables and must
        not have effects.

        Returns:
            StreamView: The rows, and a status that tells an empty result from one
            that is not computed.
        """
        ...


class StreamWithOperationsProtocol(StreamProtocol, Protocol):
    def as_df(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pl.DataFrame":
        """
        Convert the entire stream to a Polars DataFrame.
        """
        ...

    def as_lazy_frame(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pl.LazyFrame":
        """
        Load the entire stream to a Polars LazyFrame.
        """
        ...

    def as_polars_df(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pl.DataFrame":
        """
        Convert the entire stream to a Polars DataFrame.
        """
        ...

    def as_pandas_df(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pd.DataFrame":
        """
        Convert the entire stream to a Pandas DataFrame.
        """
        ...

    def flow(
        self,
    ) -> Sequence[tuple[TagProtocol, DataProtocol]]:
        """
        Return the entire stream as a collection of (tag, data) pairs.

        This method materializes the stream content into a list or similar
        collection type. It is useful for small streams or when you need
        to process all data at once.

        Args:
            execution_engine: Optional execution engine to use for computation.
                If None, the stream will use its default execution engine.
        """
        ...

    def join(
        self, other_stream: "StreamProtocol", label: str | None = None
    ) -> "StreamProtocol":
        """
        Join this stream with another stream.

        Combines two streams into a single stream by merging their content.
        The resulting stream contains all (tag, data) pairs from both
        streams, preserving their order.

        Args:
            other_stream: The other stream to join with this one.

        Returns:
            Self: New stream containing combined content from both streams.
        """
        ...

    def rejoin(
        self, *other_streams: "StreamProtocol", label: str | None = None
    ) -> "StreamProtocol":
        """Put this stream back together with other branches of one stream (``Rejoin``).

        Args:
            *other_streams: The other branches, one or more.
            label: The result's label.

        Returns:
            The rejoined stream.
        """
        ...

    def semi_join(
        self, other_stream: "StreamProtocol", label: str | None = None
    ) -> "StreamProtocol":
        """
        Perform a semi-join with another stream.

        This operation filters this stream to only include data that have
        corresponding tags in the other stream. The resulting stream contains
        all (tag, data) pairs from this stream that match tags in the other.

        Args:
            other_stream: The other stream to semi-join with this one.

        Returns:
            Self: New stream containing filtered content based on the semi-join.
        """
        ...

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
    ) -> "StreamProtocol":
        """
        Map tag names in this stream to new names based on the provided mapping.
        Tags not in the mapping are kept unless ``drop_unmapped`` is True. With
        ``strict`` (the default), a name that is not a tag column is refused. The
        renames are one positional dict or keyword arguments (``old="new"``). A
        data-tag is renamed in both roles unless ``tag_only`` is True.
        """
        ...

    def map_data(
        self,
        name_map: Mapping[str, str] | None = None,
        /,
        *,
        drop_unmapped: bool = False,
        strict: bool = True,
        label: str | None = None,
        **renames: str,
    ) -> "StreamProtocol":
        """
        Map data names in this stream to new names based on the provided mapping.
        Data columns not in the mapping are kept unless ``drop_unmapped`` is True.
        With ``strict`` (the default), a name that is not a data column is refused.
        The renames are one positional dict or keyword arguments (``old="new"``).
        """
        ...

    def map_columns(
        self,
        name_map: Mapping[str, str] | None = None,
        /,
        *,
        drop_unmapped: bool = False,
        strict: bool = True,
        label: str | None = None,
        **renames: str,
    ) -> "StreamProtocol":
        """Rename columns, tag or data, in one step; a tag stays a tag.

        Columns not in the mapping are kept unless ``drop_unmapped`` is True. With
        ``strict`` (the default), a name that is neither a tag nor a data column
        is refused. The renames are one positional dict or keyword arguments
        (``old="new"``).
        """
        ...

    def polars_filter(
        self,
        *predicates: Any,
        constraint_map: Mapping[str, Any] | None = None,
        label: str | None = None,
        **constraints: Any,
    ) -> "StreamProtocol": ...

    def filter(
        self,
        *conditions: Condition | Mapping[str, object],
        label: str | None = None,
        **columns: object,
    ) -> "StreamProtocol":
        """
        Keep the rows that satisfy every condition, as a step of the pipeline:
        keywords and mappings are equalities, other conditions are built with
        ``col``. The output has this stream's columns and types.
        """
        ...

    def drop_nulls(
        self,
        columns: str | Collection[str] | None = None,
        label: str | None = None,
    ) -> "StreamProtocol":
        """
        Keep the rows where every named column (tag or data) is non-null, and
        narrow each named column from ``T | None`` to ``T``. Without names,
        every tag and data column is tested and narrowed.
        """
        ...

    def tag_by(
        self,
        *columns: str | Iterable[str],
        label: str | None = None,
    ) -> "StreamProtocol":
        """
        Give the named data columns the tag role, as data-tags: each stays
        one column, a tag and the data column it was, with its values and source
        info unchanged. The new tags follow the stream's tags, in name order.
        """
        ...

    def add_tags(
        self,
        tags: Mapping[str, bool | int | float | str | bytes] | None = None,
        /,
        *,
        label: str | None = None,
        **values: bool | int | float | str | bytes,
    ) -> "StreamProtocol":
        """
        Add tags with constant values to every row: the same value on every row,
        after the existing tags. Data columns are unchanged.
        """
        ...

    def select_tag_columns(
        self,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "StreamProtocol":
        """
        Select the specified tag columns from the stream. A ValueError is raised
        if one or more specified tag columns do not exist in the stream unless strict = False.
        """
        ...

    def select_data_columns(
        self,
        data_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "StreamProtocol":
        """
        Select the specified tag columns from the stream. A ValueError is raised
        if one or more specified tag columns do not exist in the stream unless strict = False.
        """
        ...

    def drop_tag_columns(
        self,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "StreamProtocol":
        """
        Drop the specified tag columns from the stream. A ValueError is raised
        if one or more specified tag columns do not exist in the stream unless strict = False.
        """
        ...

    # TODO: check to make sure source columns are also dropped
    def drop_data_columns(
        self,
        data_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "StreamProtocol":
        """
        Drop the specified data columns from the stream. A ValueError is raised
        if one or more specified data columns do not exist in the stream unless strict = False.
        """
        ...

    def drop_columns(
        self,
        *columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "StreamProtocol":
        """Drop the named columns, whether each is a tag or a data column.

        Names are given as one or more strings, or as one collection of strings
        as the only positional argument.
        """
        ...

    def select_columns(
        self,
        *columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "StreamProtocol":
        """Keep exactly the named columns, tag or data; drop every other one.

        Names are given as one or more strings, or as one collection of strings
        as the only positional argument.
        """
        ...

    def batch(
        self,
        batch_size: int = 0,
        drop_partial_batch: bool = False,
        label: str | None = None,
    ) -> "StreamProtocol":
        """
        Batch the stream into groups of the specified size.

        This operation groups (tag, data) pairs into batches for more
        efficient processing. Each batch is represented as a single (tag, data)
        pair where the tag is a list of tags and the data is a list of data.

        Args:
            batch_size: Number of (tag, data) pairs per batch. If 0, all
                        pairs are included in a single batch.
            drop_partial_batch: If True, drop the last batch if it has fewer
                             than batch_size pairs.

        Returns:
            Self: New stream containing batched (tag, data) pairs.
        """
        ...


# ---------------------------------------------------------------------------
# Channels of Arrow record batches (ITL-1347): the stream is the description
# ---------------------------------------------------------------------------


class RecordBatchInput(Protocol):
    """A single-pass reader of a stream's Arrow record batches.

    The Arrow-record-batch form of ``ReadableChannel`` (``orcapod.channels``); ``InputChannel`` of
    ``orcapod.core.streams.record_batch_input`` is the implementation. Its description is
    its ``stream``, known before the first batch.
    """

    @property
    def stream(self) -> StreamProtocol:
        """The stream whose rows this channel delivers: the description."""
        ...

    async def receive(self) -> pa.RecordBatch:
        """Return the next Arrow record batch.

        Raises:
            ChannelClosed: At the end, and after ``close``.
            UpstreamFailedError: If the producer failed or was cancelled.
        """
        ...

    def __aiter__(self) -> AsyncIterator[pa.RecordBatch]:
        """Iterate over the remaining record batches, in arrival order."""
        ...

    def carried(self) -> AsyncIterator[pa.RecordBatch]:
        """Iterate over the remaining record batches as they travel, for an operator.

        Their source-info columns may be derived (``utils/source_tokens.py``): internal.
        Every other read gives their text.
        """
        ...

    async def collect(self) -> list[pa.RecordBatch]:
        """Read to the end and return every remaining record batch, in arrival order."""
        ...

    @property
    def unread(self) -> bool:
        """Whether no read was called and the channel is not closed.

        A reserved channel (``reserve()``) is not unread, even before its first read.
        """
        ...

    @property
    def opened_on(self) -> StreamProtocol | None:
        """The complete stream this channel reads, when it was opened on one; else
        ``None`` (a reader of an output channel, whatever its description)."""
        ...

    def reserve(self, claim: object | None = None) -> object:
        """Take the channel for one consumer that will read all of it.

        Without ``claim``, the unread channel is held for the returned claim, not for
        the calling task: no task, the reserving one included, can read it until a
        task takes it over with that claim (``reserve(claim)``), after which only
        that task reads it. A claim is taken over once; a claim freed before it was
        taken over closes the channel.

        Raises:
            RuntimeError: If a read was called on it already, or ``claim`` is not its
                claim.
            ChannelClosed: If it was closed.
        """
        ...

    def to_stream(self) -> Coroutine[Any, Any, StreamProtocol]:
        """Return what the channel delivers, as a stream: the one such conversion.

        A plain method returning the conversion to await; it reserves the channel
        when called. The conversion reads the channel to its end, raises every read
        error before an operator runs, and builds a fixed snapshot through the one
        settlement path; only an unread channel opened on a stream ``_output_stream``
        built gives that stream as it is.

        Raises:
            RuntimeError: If a read of the channel was already called.
            ChannelClosed: If the channel was closed.
            UpstreamFailedError: If the producer failed or was cancelled.
        """
        ...

    async def close(self) -> None:
        """Stop reading. The producer no longer waits for this reader."""
        ...


class RecordBatchOutput(Protocol):
    """The producer side of a stream's Arrow record batches.

    The Arrow-record-batch form of ``WritableChannel`` (``orcapod.channels``); ``OutputChannel`` of
    ``orcapod.core.record_batch_channels`` is the implementation.
    """

    async def send(self, batch: pa.RecordBatch) -> None:
        """Send one Arrow record batch of the stream's transport columns."""
        ...

    async def close(self) -> None:
        """Complete the output."""
        ...

    def fail(self, error: BaseException) -> None:
        """End the output with the producer's failure or cancellation."""
        ...
