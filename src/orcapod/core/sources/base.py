from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from orcapod import contexts
from orcapod.config import OrcapodConfig
from orcapod.core.streams.base import StreamBase, typed_zero_row_table
from orcapod.system_constants import constants
from orcapod.core.streams.view import StreamView
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.types import ColumnConfig, Condition, ContentHash, Schema
from orcapod.core.sources.provisional import warn_provisional_source
from orcapod.utils.provisional import note_provisional_use, provisional_class_name

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.protocols.hashing_protocols import SemanticHasherProtocol


def saved_tag_only_columns(config: dict[str, Any]) -> bool | list[str]:
    """Return the ``tag_only_columns`` of a saved source config (ITL-1019).

    A config of format 0.3.0 always holds it; the loader of an older file, whose tags
    are all plain, puts ``True`` in first (``serialization.source_config_at_version``).

    Args:
        config: A saved source config.

    Returns:
        ``True`` (every tag plain) or the names of the plain tags.

    Raises:
        ValueError: If the config does not hold ``tag_only_columns``.
    """
    if "tag_only_columns" not in config:
        raise ValueError(
            f"source config {config.get('source_type')!r} has no tag_only_columns: "
            "a saved source config states its plain tags (format 0.3.0)"
        )
    return config["tag_only_columns"]


class RootSource(StreamBase):
    """Abstract base class for all root sources in Orcapod.

    A RootSource is a pure stream — the root of a computational graph, producing
    data from an external source (file, database, in-memory data, etc.) with no
    upstream dependencies.

    As a StreamProtocol:
    - ``source`` returns ``None`` (no upstream source pod)
    - ``upstreams`` is always empty
    - ``keys``, ``output_schema``, ``iter_data``, ``as_table`` delegate to
      ``self._stream`` by default; concrete subclasses may override them.

    As a PipelineElementProtocol:
    - ``pipeline_identity_structure()`` returns ``(tag_schema, data_schema)``
      — schema-only, no data content — forming the base case of the pipeline
      identity Merkle chain.

    Source identity:
        Every source has a ``source_id`` — a canonical name that determines the
        source's content identity and is used in the ``SourceRegistry`` so that
        provenance tokens embedded in downstream data can be resolved back to the
        originating source object.

        Concrete subclasses must ensure ``_source_id`` is set by the end of
        ``__init__``.  File-backed sources (DeltaTableSource, CSVSource) default
        to the file path; ``ArrowTableSource`` defaults to the table's data hash.

    Concrete subclasses must set ``self._stream`` in their ``__init__`` to get
    the default stream delegation behavior. They may also override
    ``identity_structure()``.
    """

    # Whether the source takes ``tag_only_columns`` and saves it in its config
    # (ITL-1019): from format 0.3.0 on such a config always holds it.
    TAKES_TAG_ONLY_COLUMNS: ClassVar[bool] = False

    PROVISIONAL: ClassVar[bool] = False
    """Whether the source is provisional: its identity is not frozen (ITL-1314).

    Set to ``True`` exactly by the sources listed in ``PROVISIONAL_SOURCES``
    (``core/sources/provisional.py``); constructing one issues
    ``ProvisionalHashWarning`` once per source kind per process. It enters no hash,
    identifier, config or stored path.
    """

    def __init__(
        self,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: OrcapodConfig | None = None,
    ) -> None:
        # A provisional source warns once per process at construction (ITL-1314).
        provisional = provisional_class_name(type(self))
        if provisional is not None:
            warn_provisional_source(provisional)
        super().__init__(
            label=label,
            data_context=data_context,
            config=config,
        )
        if source_id == "":
            raise ValueError(
                "source_id is empty: a source id is the source's name, and an empty "
                "one would make every source-info token start with "
                f"{constants.BLOCK_SEPARATOR!r}; give the source a name, or leave "
                "source_id unset for the default"
            )
        if source_id is not None and (
            constants.BLOCK_SEPARATOR in source_id or source_id.endswith(":")
        ):
            raise ValueError(
                f"source_id {source_id!r} contains {constants.BLOCK_SEPARATOR!r} or ends "
                "with ':', which would blur the separator of the parts of a source-info "
                "token (<source id>::<record id>::<column>); give the source another "
                "name"
            )
        self._source_id = source_id

    # -------------------------------------------------------------------------
    # Source identity
    # -------------------------------------------------------------------------

    def _note_provisional_identity_use(self) -> None:
        """Record a use of this source's identity if it is provisional (ITL-1314).

        The warning is issued at construction only; the use is recorded wherever the
        identity is read (``source_id``, ``content_hash``, ``pipeline_hash``, the identity
        structures; ITL-1331), so the
        golden tool refuses a case that reads a provisional source built before the
        case ran. The record enters no hash.
        """
        provisional = provisional_class_name(type(self))
        if provisional is not None:
            note_provisional_use(provisional)

    def content_hash(self, hasher: SemanticHasherProtocol | None = None) -> ContentHash:
        """Return the content hash, recording a provisional source's use (ITL-1314).

        Args:
            hasher: The semantic hasher, as for ``ContentIdentifiableBase.content_hash``.

        Returns:
            The content hash, unchanged by the record.
        """
        self._note_provisional_identity_use()
        return super().content_hash(hasher)

    def pipeline_hash(
        self, hasher: SemanticHasherProtocol | None = None
    ) -> ContentHash:
        """Return the pipeline hash, recording a provisional source's use (ITL-1314).

        Args:
            hasher: The semantic hasher, as for ``PipelineElementBase.pipeline_hash``.

        Returns:
            The pipeline hash, unchanged by the record.
        """
        self._note_provisional_identity_use()
        return super().pipeline_hash(hasher)

    @property
    def source_id(self) -> str:
        """Canonical name for this source used in the registry and provenance
        strings. If not set, raises ``ValueError``.
        """
        self._note_provisional_identity_use()
        if self._source_id is None:
            raise ValueError("source_id is not set")
        return self._source_id

    # -------------------------------------------------------------------------
    # Restriction (ITL-1218)
    # -------------------------------------------------------------------------

    @property
    def condition(self) -> Condition:
        """The row conditions this source reads under (ITL-1218).

        The empty ``Condition``: a source reads every row unless its kind can be
        restricted (``SpiralDBTableSource.where``). A source that reads through
        another (``CachedSource``, ``SourceProxy``) answers for itself only; what
        it reads through is reached by the restriction walk
        (``orcapod.pipeline.restrictions``).
        """
        return Condition()

    @property
    def row_limit(self) -> int | None:
        """The most rows this source reads (ITL-1218): ``None``, no limit, unless
        its kind can be limited (``SpiralDBTableSource(limit=...)``)."""
        return None

    # -------------------------------------------------------------------------
    # Content identity
    # -------------------------------------------------------------------------

    _key_columns: tuple[str, ...]
    """The resolved key columns of the record ids, in key order; ``()`` for the
    position. Every concrete source sets it in ``__init__``."""

    def identity_structure(self) -> Any:
        """Identity from the class name, output schema, ``source_id`` and key columns.

        The key columns are the resolved ``record_id_columns`` (``()`` for the
        position): two sources that differ only in how they name their rows are two
        sources, so their records never share a cache (developer, November 16, 2026,
        ITL-1154). The pipeline hash stays schema-only.
        """
        self._note_provisional_identity_use()  # a direct read is a use (ITL-1331)
        return (
            self.__class__.__name__,
            self.output_schema(),
            self.source_id,
            self._key_columns,
        )

    # -------------------------------------------------------------------------
    # PipelineElementProtocol — schema-only identity (base case of Merkle chain)
    # -------------------------------------------------------------------------

    def _identity_config(self) -> dict[str, Any]:
        """Return identity fields for inclusion in ``to_config()`` output.

        These fields allow ``SourceProxy`` to be constructed when the source
        cannot be reconstructed from config, preserving identity hashes and
        schemas for downstream consumers.
        """
        from orcapod.pipeline.serialization import (
            serialize_nullability,
            serialize_schema,
        )

        tag_schema, data_schema = self.output_schema()
        type_converter = self.data_context.type_converter
        return {
            "content_hash": self.content_hash().to_string(),
            "pipeline_hash": self.pipeline_hash().to_string(),
            "tag_schema": serialize_schema(tag_schema, type_converter),
            "data_schema": serialize_schema(data_schema, type_converter),
            "nullable": serialize_nullability(tag_schema, data_schema, type_converter),
        }

    def _tag_only_config(self) -> dict[str, Any]:
        """Return ``tag_only_columns`` for ``to_config``: always written (ITL-1019).

        The format version decides what a saved file means, never a missing key: a
        config of format 0.3.0 states its plain tags, ``True`` when every tag is plain
        and the names otherwise (``saved_tag_only_columns`` reads it back).
        """
        tag_columns = tuple(getattr(self, "_tag_columns", ()))
        tag_only = tuple(getattr(self, "_tag_only_columns", tag_columns))
        if tag_only == tag_columns:
            return {"tag_only_columns": True}
        return {"tag_only_columns": list(tag_only)}

    def pipeline_identity_structure(self) -> Any:
        """Return (tag_schema, data_schema) as the pipeline identity for this
        source.  Schema-only: no data content is included, so sources with
        identical schemas share the same pipeline hash and therefore the same
        pipeline database table.
        """
        self._note_provisional_identity_use()  # a direct read is a use (ITL-1331)
        tag_schema, data_schema = self.output_schema()
        return (tag_schema, data_schema)

    # -------------------------------------------------------------------------
    # StreamProtocol protocol
    # -------------------------------------------------------------------------

    @property
    def producer(self) -> None:
        """Root sources have no upstream source pod."""
        return None

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        """Sources have no upstream dependencies."""
        return ()

    # -------------------------------------------------------------------------
    # Stream delegation defaults
    # -------------------------------------------------------------------------

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Delegate to the underlying stream's output_schema."""
        return self._stream.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Delegate to the underlying stream's keys."""
        return self._stream.keys(columns=columns, all_info=all_info)

    def iter_data(self):
        """Delegate to the underlying stream's iter_data."""
        return self._stream.iter_data()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Delegate to the underlying stream's as_table."""
        return self._stream.as_table(columns=columns, all_info=all_info)

    # -------------------------------------------------------------------------
    # The start-of-run check (ITL-1102)
    # -------------------------------------------------------------------------

    def check_for_new_data(self) -> bool:
        """Check whether the data behind the source changed; if so, take it in.

        Every run (``job.run()``, ``run_async()``, a job node's ``run()``, a load-only
        run too) calls this once on each source feeding it, before deciding which
        nodes to run (developer, November 14, 2026, ITL-1102). A source that finds new
        data takes it in and moves its modified time to that moment, so every job node
        below it is stale and runs. A source that finds nothing changes nothing, so a
        current job stays current. ``head()`` and the notebook preview never call it.

        A source built over data held in memory (``ArrowTableSource``, ``DictSource``,
        ``ListSource``, ``DataFrameSource``) cannot change: this default returns
        ``False``. A source that raises here does not fail the run: the run issues a
        ``SourceCheckFailedWarning`` and proceeds with the data the source holds, so a
        source must leave its data and modified time as they were when it raises. A
        source whose reads refresh it (``PollingSource``: a read polls) is refreshed
        again by a run that reads it, and a refresh that fails then raises (ITL-1139).

        Returns:
            ``True`` if the source found new data and took it in.
        """
        return False

    # A source's rows are its own, not computed: a run() view says "all N rows".
    _RUN_ORIGIN: str = "held"

    def _typed_zero_row_table(self) -> "pa.Table | None":
        """The source's zero-row table, from the stream it holds; see ``typed_zero_row_table``.

        A source that reads its table when it is built (Arrow table, dict, list, data
        frame, CSV, Delta, SQLite, PostgreSQL) holds it in ``_stream``, and a SpiralDB
        source holds a zero-row stream with the table's schema: either is cut to no
        rows, so no row is read. A source with no such stream (polling, cached,
        derived, proxy) reads or polls its rows later and answers ``None``.
        """
        return typed_zero_row_table(getattr(self, "_stream", None))

    def _display_name(self) -> str:
        """The name a preview header shows: the label when one was given, else the
        source id (a source id is the source's name), e.g. a table's name. The label
        itself is not changed."""
        if self.has_assigned_label or self._source_id is None:
            return self.label
        return self._source_id

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first ``n`` rows of the table the source holds, without computing.

        A source that reads its table when it is built holds it in memory, so this is a
        slice of it. A source that reads its table later overrides this with a bounded
        read. See ``StreamBase.head``.
        """
        # _stream is set by each concrete subclass (see the class docstring), as for
        # the other delegating methods here.
        stream = self._stream  # pyright: ignore[reportAttributeAccessIssue]
        return stream.head(n, columns=columns, all_info=all_info).for_stream(self)

    # -------------------------------------------------------------------------
    # Convenience — caching
    # -------------------------------------------------------------------------

    def cached(
        self,
        cache_database: ArrowDatabaseProtocol,
        cache_path_prefix: tuple[str, ...] = (),
        **kwargs: Any,
    ) -> CachedSource:
        """Return a ``CachedSource`` wrapping this source.

        Args:
            cache_database: Database to store cached records in.
            cache_path_prefix: Path prefix for the cache table.
            **kwargs: Additional keyword arguments passed to ``CachedSource``.

        Returns:
            A ``CachedSource`` that caches this source's output.

        Raises:
            ValueError: If this source is restricted (a condition or a limit): a cache
                is kept under the table's identity, which a restriction is not part of.
        """
        from orcapod.core.sources.cached_source import CachedSource

        return CachedSource(
            source=self,
            cache_database=cache_database,
            cache_path_prefix=cache_path_prefix,
            **kwargs,
        )
