from __future__ import annotations

import os
import warnings
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar

from orcapod import contexts
from orcapod.config import OrcapodConfig
from orcapod.core.streams.base import (
    StreamBase,
    reads_as_its_class,
    typed_arrow_schemas,
    typed_zero_row_table,
)
from orcapod.errors import SourceNameWarning, SourceVersionWarning
from orcapod.core.streams.view import StreamView
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.types import ColumnConfig, Condition, ContentHash, Schema
from orcapod.core.sources.source_ids import (
    build_source_id,
    kind_part,
    register_kind,
    require_source_name,
    warn_if_name_looks_like_an_id,
)
from orcapod.core.sources.provisional import warn_provisional_source
from orcapod.utils.provisional import note_provisional_use, provisional_class_name
from orcapod.utils.schema_utils import arrow_schema_identity

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.protocols.hashing_protocols import SemanticHasherProtocol


def source_path_text(path: str | bytes | os.PathLike[str] | os.PathLike[bytes]) -> str:
    """Return the supplied path text, rejecting byte paths before opening a source."""
    text = os.fspath(path)
    if isinstance(text, bytes):
        raise TypeError("Source paths must be str or os.PathLike[str], not bytes")
    return text


def source_paths(
    path: str | os.PathLike[str], *, pin: Callable[[str], str] = os.path.abspath
) -> tuple[str, str]:
    """Return the supplied text and the path pinned for reads at construction.

    Args:
        path: The supplied file path or address.
        pin: The reader's path pinning function.

    Returns:
        The original text for names and configs, and the pinned access path.
    """
    text = source_path_text(path)
    return text, pin(text)


def saved_tag_only_columns(config: dict[str, Any]) -> bool | list[str]:
    """Return the ``tag_only_columns`` of a saved source config (ITL-1019).

    A saved config always holds it (``serialization.require_tag_only_columns``).

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


def table_is_rows(source: object) -> bool:
    """Whether ``source``'s ``as_table()`` and ``iter_data()`` provably read the same data.

    Positive and exact (ITL-1347): true only when ``source``'s class itself declares
    ``TABLE_IS_ROWS = True`` (a library source kind) and ``source`` reads as that class
    (``reads_as_its_class``: its exact type, no member replaced on the instance). Any
    subclass, a mixin, a replaced reader or a property that changes what the source
    reads through is not proven, and a job node bound to it hands on its rows, as
    ``main`` iterated them.

    Args:
        source: The object bound to a source slot.

    Returns:
        Whether F of its table is the table its rows give.
    """
    kind = type(source)
    return vars(kind).get("TABLE_IS_ROWS") is True and reads_as_its_class(source, kind)


_SourceT = TypeVar("_SourceT", bound="RootSource")


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
        Every source has a ``source_id``, ``<kind>_v<N>:<name>`` (ITL-1425): the kind
        part names the source's kind (``SOURCE_KIND``) and the version of its
        implementation (``SOURCE_VERSION``), and the name is the given ``source_id``
        or the source's default. The source id is in every system tag and source-info
        token of the source's rows, enters its content hash, and is used in the
        ``SourceRegistry`` so that provenance tokens can be resolved back to the
        source object.

        Concrete subclasses must ensure ``_source_name`` is set by the end of
        ``__init__``. File-backed sources (DeltaTableSource, CSVSource) default
        to the file path; ``ArrowTableSource`` defaults to the table's data hash.
        Every concrete class declares its own ``SOURCE_KIND`` and ``SOURCE_VERSION``.

    Concrete subclasses must set ``self._stream`` in their ``__init__`` to get
    the default stream delegation behavior. They may also override
    ``identity_structure()``.
    """

    SOURCE_KIND: ClassVar[str]
    """The source's kind: its config type key and the first part of its source id."""

    SOURCE_VERSION: ClassVar[int]
    """The version of the kind's implementation, in the source id's kind part (ITL-1425).

    Bump it when a row read before a change could read differently after it (other
    values, other Arrow types, another record id): a new version is a new source id.
    """

    # The attributes holding what tells two sources of one label apart in a display,
    # nearest first (``core/sources/display_names.py``); ``""`` for a kind without them.
    DISPLAY_PARTS: ClassVar[tuple[str, str]] = ("", "")

    @classmethod
    def kind_part(cls) -> str:
        """The kind part of this class's source ids: ``<SOURCE_KIND>_v<SOURCE_VERSION>``."""
        return kind_part(cls.SOURCE_KIND, cls.SOURCE_VERSION)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        kind = cls.__dict__.get("SOURCE_KIND")
        if kind is not None:
            register_kind(kind)

    # Whether the source takes ``tag_only_columns`` and saves it in its config
    # (ITL-1019): from format 0.3.0 on such a config always holds it.
    TAKES_TAG_ONLY_COLUMNS: ClassVar[bool] = False

    REPORTS_WHAT_IT_READS: ClassVar[bool] = True
    """Whether the source says what it reads (``explain()``, ITL-1402): its restriction
    (``condition``, ``row_limit``), and with none, its whole table. ``False`` for a source
    that reads rows stored elsewhere (``DerivedSource``, ``CachedSource``): what it reads
    depends on what was stored there."""

    PROVISIONAL: ClassVar[bool] = False
    """Whether the source is provisional: its identity is not frozen (ITL-1314).

    Set to ``True`` exactly by the sources listed in ``PROVISIONAL_SOURCES``
    (``core/sources/provisional.py``); constructing one issues
    ``ProvisionalHashWarning`` once per source kind per process. It enters no hash,
    identifier, config or stored path.
    """

    TABLE_IS_ROWS: ClassVar[bool] = False
    """Whether ``as_table()`` and ``iter_data()`` read the same data, with no effect.

    Declared ``True`` only by the library source kinds that read one stream either way,
    each on its own class: it is read for the exact class only, never inherited
    (``table_is_rows``; ITL-1347). A job node bound to an instance of exactly such a
    class hands on F of its table, which equals the table built from its rows. Any
    other bound stream (a subclass, a side-effect or function pod stream, an operator's
    result, a raw stream, a source kind that polls or delivers on a read) hands on its
    rows (``RowsOnRead``), as ``main`` iterated them.
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
        # A given source_id is the source's name: its kind part is added in front.
        if source_id is not None:
            _ = require_source_name(source_id)
            warn_if_name_looks_like_an_id(source_id)
        self._source_name: str | None = source_id
        # Whether the name was given, not built as a default; saved, so a reload
        # never guesses it (ITL-1425). A subclass that sets a default leaves it.
        self._source_name_given: bool = source_id is not None

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
        """The source id, ``<kind>_v<N>:<name>``: in the registry, system tags and tokens.

        Raises:
            ValueError: If the source has no name yet.
        """
        self._note_provisional_identity_use()
        return build_source_id(self.SOURCE_KIND, self.SOURCE_VERSION, self.source_name)

    @property
    def source_name(self) -> str:
        """The source's name: the given ``source_id``, or the default (ITL-1425).

        Raises:
            ValueError: If the source has no name yet.
        """
        if self._source_name is None:
            raise ValueError("the source has no name yet")
        return self._source_name

    def _source_id_config(self) -> dict[str, Any]:
        """The saved form of the source id: name and version apart (ITL-1425).

        The kind is the config's ``source_type``; ``saved_source_name`` reads it back.
        """
        return {
            "source_name": self.source_name,
            "source_name_given": self._source_name_given,
            "source_version": self.SOURCE_VERSION,
        }

    @classmethod
    def _from_saved_name(
        cls, config: dict[str, Any], build: Callable[[str | None], _SourceT]
    ) -> _SourceT:
        """Rebuild a source from a saved config, with the name it was saved with.

        The config says whether the name was given or a default
        (``source_name_given``; missing means given: the saved name is used exactly).
        A given name is passed as the name, and the source computes no default (its
        location is not read for one). A saved default is passed back as the name
        too, so the id is the saved one exactly (a Delta table saved with a relative
        path keeps its relative default), but it is not checked like a name the user
        gave (``SourceNameWarning``), and it is saved again as a default.

        Args:
            config: A saved source config (``to_config``).
            build: Builds the source with the given name (``source_id``).

        Returns:
            The rebuilt source.
        """
        name = cls.saved_source_name(config)
        if name is None or config.get("source_name_given", True):
            return build(name)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=SourceNameWarning)
            source = build(name)
        source._source_name_given = False  # pyright: ignore[reportPrivateUsage]  # the saved state
        return source

    @classmethod
    def require_saved_name_fields(cls, config: dict[str, Any]) -> None:
        """Refuse a saved config written before source ids named their kind (ITL-1425).

        Such a config holds a ``source_id`` and no ``source_name``. It is not read as
        anything else: ``from_config`` refuses it, and ``PipelineJob.load`` refuses a
        file holding one anywhere (``check_saved_source_config``), instead of loading
        the job with that source missing.

        Args:
            config: A saved source config (``to_config``).

        Raises:
            ValueError: If the config holds a ``source_id`` but no ``source_name``.
        """
        if "source_id" in config and "source_name" not in config:
            raise ValueError(
                f"the saved {cls.SOURCE_KIND} source config holds a source_id but no "
                "source_name: it was saved before every source id named its kind and "
                "implementation version (ITL-1425); build the source again and save "
                "the job again"
            )

    @classmethod
    def saved_source_name(cls, config: dict[str, Any]) -> str | None:
        """Return the name a saved source config holds, warning on a version change.

        A source saved by another version of its kind is rebuilt as the installed
        version, so its source id names that version: a different source id. The saved
        id is not rewritten; ``SourceVersionWarning`` names both versions.

        Args:
            config: A saved source config (``to_config``).

        Returns:
            The saved name, or ``None`` when the source was saved with its default.

        Raises:
            ValueError: If the config holds a ``source_id`` but no ``source_name``: it
                was saved before source ids named their kind (ITL-1425) and is not read
                as anything else.
        """
        cls.require_saved_name_fields(config)
        saved_version = config.get("source_version")
        if saved_version is not None and saved_version != cls.SOURCE_VERSION:
            warnings.warn(
                f"a {cls.SOURCE_KIND} source saved by version {saved_version} of its "
                f"implementation is loaded as version {cls.SOURCE_VERSION}: its source "
                f"id now names version {cls.SOURCE_VERSION}, so it is a different "
                "source, and results stored for the saved source are not this "
                "source's",
                SourceVersionWarning,
                stacklevel=2,
            )
        return config.get("source_name")

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

    _arrow_schemas_cache: "tuple[pa.Schema, pa.Schema] | None" = None

    @property
    def arrow_schemas(self) -> "tuple[pa.Schema, pa.Schema]":
        """The Arrow schemas of this source's tag and data columns (ITL-640).

        The Arrow types its rows have (``_compute_arrow_schemas``), derived once: a
        source's schema is fixed when it is built, whatever rows it reads later.
        """
        if self._arrow_schemas_cache is None:
            self._arrow_schemas_cache = self._compute_arrow_schemas()
        return self._arrow_schemas_cache

    def _compute_arrow_schemas(self) -> "tuple[pa.Schema, pa.Schema]":
        """Derive ``arrow_schemas``: the typed zero-row table's types (``typed_arrow_schemas``)."""
        return typed_arrow_schemas(self, own=False)

    def _schema_identity(self) -> tuple[ContentHash, ContentHash]:
        """The identity of this source's schema: its Arrow tag and data schemas (ITL-640).

        The Arrow schemas its rows have (``typed_arrow_schemas``), hashed by the one rule
        (``schema_utils.arrow_schema_identity``): two sources whose columns differ only
        in an Arrow type (``int32`` and ``int64``) are two identities, with two pipeline
        hashes, stored paths and system-tag column names.
        """
        return arrow_schema_identity(*self.arrow_schemas, self.data_context)

    def identity_structure(self) -> Any:
        """Identity from the ``source_id``, the Arrow schema and the key columns.

        The source id's kind part names the kind and implementation version that read
        the data; the class name is not part of the identity (ITL-1425), so the kind
        enters a datum's identity in exactly one place. The key columns are the
        resolved ``record_id_columns`` (``()`` for the position): two sources that
        differ only in how they name their rows are two sources, so their records
        never share a cache (developer, November 16, 2026, ITL-1154). The pipeline
        hash stays schema-only.
        """
        self._note_provisional_identity_use()  # a direct read is a use (ITL-1331)
        return (
            self.source_id,
            self._schema_identity(),
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
            # The full id, for a proxy when the name is not saved (a default derived
            # name, recomputed from an origin a load may not rebuild). Not
            # ``source_id``: a config with that key and no ``source_name`` is one saved
            # before ITL-1425, and is refused.
            "full_source_id": self.source_id,
            "content_hash": self.content_hash().to_string(),
            "pipeline_hash": self.pipeline_hash().to_string(),
            "tag_schema": serialize_schema(tag_schema, type_converter),
            "data_schema": serialize_schema(data_schema, type_converter),
            "nullable": serialize_nullability(tag_schema, data_schema, type_converter),
        }

    def _tag_only_config(self) -> dict[str, Any]:
        """Return ``tag_only_columns`` for ``to_config``: always written (ITL-1019).

        A missing key is never read as a default: a saved config states its plain
        tags, ``True`` when every tag is plain and the names otherwise
        (``saved_tag_only_columns`` reads it back).
        """
        tag_columns = tuple(getattr(self, "_tag_columns", ()))
        tag_only = tuple(getattr(self, "_tag_only_columns", tag_columns))
        if tag_only == tag_columns:
            return {"tag_only_columns": True}
        return {"tag_only_columns": list(tag_only)}

    def pipeline_identity_structure(self) -> Any:
        """Return the hashes of the Arrow tag and data schemas: the pipeline identity.

        Schema-only: no data content is included, so sources whose Arrow schemas are
        equal share the same pipeline hash and therefore the same pipeline database
        table; sources whose Arrow types differ do not (ITL-640).
        """
        self._note_provisional_identity_use()  # a direct read is a use (ITL-1331)
        return self._schema_identity()

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
        if self.has_assigned_label or self._source_name is None:
            return self.label
        # The header names the class already; the name is the part that is the user's.
        return self.source_name

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
