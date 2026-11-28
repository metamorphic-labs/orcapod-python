from __future__ import annotations


import logging
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, TYPE_CHECKING, cast

import pyarrow as pa
from typing_extensions import override

from orcapod import contexts
from orcapod.core.data_tags import stream_data_tags
from orcapod.config import OrcapodConfig
from orcapod.core.sources.base import RootSource
from orcapod.core.sources.source_ids import parse_source_id
from orcapod.core.sources.stream_builder import (
    require_supported_nullable_types,
    validate_source_table,
)
from orcapod.errors import (
    CachedSourceChangedRowError,
    CachedSourceIdMismatchError,
    InputValidationError,
    UnstorableColumnError,
)
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import (
    typed_arrow_schemas,
    check_head_size,
    schema_requires_io,
    typed_zero_row_table,
)
from orcapod.core.streams.view import (
    ViewStatus,
    StreamView,
    head_of,
)
from orcapod.contexts.stored import (
    data_context_field,
    require_storable,
    keep_rows_of_data_context,
    require_same_data_context,
    account_for_stale_tables,
    find_stale_tables,
    report_rows_of_other_data_contexts,
    with_data_context,
)
from orcapod.protocols.core_protocols import DataProtocol, SourceProtocol, TagProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import SOURCE_DB_SCHEMA_VERSION, constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_utils, schema_utils

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import DatabaseRegistryProtocol

logger = logging.getLogger(__name__)

SOURCE_RECORDED_AT_COL = "__recorded_at"
"""The cache table's column saying when the cache wrote a row (ITL-1396).

Stored only: appended after the record hashes are computed, so it is in no hash, record
id or source-info token, and dropped by every read of the cache, so no stream presents
it. A row delivered again is not written again and keeps its first time. Nothing
selects rows by it today.
"""

_CHANGED_ROWS_NAMED = 5
"""How many record ids a ``CachedSourceChangedRowError`` names before the count."""


@dataclass
class _PreparedCacheRead:
    """A checked delivery and the rows enclosing caches can read from it."""

    cache: CachedSource
    stream: ArrowTableStream
    delivery: pa.Table | None
    seen: datetime | None
    reads_prepared_cache: bool


_PREPARED_CACHE_READS: ContextVar[list[_PreparedCacheRead] | None] = ContextVar(
    "prepared_source_cache_reads", default=None
)


def _recorded_at_field() -> "pa.Field[pa.DataType]":
    """The ``__recorded_at`` field of a cached-source table: a UTC instant, never null."""
    return pa.field(
        SOURCE_RECORDED_AT_COL, pa.timestamp("us", tz="UTC"), nullable=False
    )


def _record_id_column(names: Sequence[str]) -> str | None:
    """Return the source's generated record-id field, if it carries one.

    The source builder appends the generated pair after the input columns,
    including any custom system tags. Use the declaration's order.
    A plain derived source carries no generated pair.
    """
    system_tags = [
        name for name in names if name.startswith(constants.SYSTEM_TAG_PREFIX)
    ]
    if len(system_tags) < 2:
        return None
    source_id, record_id = system_tags[-2:]
    prefix = f"{constants.SYSTEM_TAG_RECORD_ID_PREFIX}{constants.BLOCK_SEPARATOR}"
    if not record_id.startswith(prefix):
        return None
    expected_pair = arrow_utils.system_tag_column_names(record_id.removeprefix(prefix))
    return record_id if (source_id, record_id) == expected_pair else None


def _unambiguous_record_id_column(names: Sequence[str]) -> str | None:
    """Find a sole provenance pair when a source does not declare its system tags."""
    available = set(names)
    prefix = f"{constants.SYSTEM_TAG_RECORD_ID_PREFIX}{constants.BLOCK_SEPARATOR}"
    pairs = [
        name
        for name in names
        if name.startswith(prefix)
        and available.issuperset(
            arrow_utils.system_tag_column_names(name.removeprefix(prefix))
        )
    ]
    return pairs[0] if len(pairs) == 1 else None


class CachedSource(RootSource):
    """DB-backed wrapper around a source that caches every data.

    Accepts any ``SourceProtocol`` implementation as the inner source.
    Implements ``StreamProtocol`` transparently so downstream consumers
    are unaware of caching.  Cache table is scoped to the source's
    ``content_hash()`` — each unique source gets its own table.

    Behavior:
        - Cache is **always on**.
        - On first access, live source data is stored in the cache table
          (deduped by per-row content hash), each row with the time the cache
          wrote it (``__recorded_at``, stored only).
        - Returns the union of all cached data (cumulative across runs).
        - A row delivered under a record id the cache holds with other content is
          refused before anything is written (``CachedSourceChangedRowError``,
          ITL-1396): a record id names one row, whether it is made from key columns
          or from the row's position.

    Semantic guarantee:
        The cache is a correct cumulative record.  The union of cache + live
        data is the full set of data ever available from that source.

    A restricted source (``SpiralDBTableSource.where`` or ``limit=``, ITL-1218) is
    refused: a restriction is not part of the source's identity, so its rows would be
    kept under the whole table's identity and read back by every cache of that table.

    Example::

        source = ArrowTableSource(table, tag_columns=["id"])
        cached = CachedSource(source, cache_database=db)
        # or equivalently:
        cached = source.cached(cache_database=db)
    """

    SOURCE_KIND: ClassVar[str] = "cached"
    """Config type key of this source. Not in its source id: it has its wrapped source's."""
    SOURCE_VERSION: ClassVar[int] = 1
    """The version of this kind's implementation (ITL-1425); not in its source id."""
    TABLE_IS_ROWS: ClassVar[bool] = True

    HASH_COLUMN_NAME = "_record_hash"

    _STORED_ONLY_COLUMNS: ClassVar[list[str]] = [
        HASH_COLUMN_NAME,
        SOURCE_RECORDED_AT_COL,
        constants.DATA_CONTEXT_COL,
    ]
    """The cache table's own columns, which no read of the cache presents."""

    REPORTS_WHAT_IT_READS: ClassVar[bool] = False
    """It reads its cache, which holds what earlier reads stored (``explain()``, ITL-1402)."""

    # Set once a read has reported the cached rows it skipped.
    _skipped_rows_reported: bool = False
    # Set once a cache table found at the older layout has been accounted for.
    _stale_tables_checked: bool = False
    # Whether the cache table was found to hold this source's rows only (ITL-1425).
    _source_ids_checked: bool = False

    def __init__(
        self,
        source: SourceProtocol,
        cache_database: ArrowDatabaseProtocol,
        cache_path_prefix: tuple[str, ...] = (),
        cache_path: tuple[str, ...] | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: OrcapodConfig | None = None,
    ) -> None:
        """Wrap ``source`` in a cache kept in ``cache_database``.

        The cached source has its wrapped source's ``source_id`` (ITL-1425): its rows
        are that source's rows, with that source's system tags and tokens; a cache is
        not another implementation reading the data.

        Raises:
            ValueError: If ``source`` is restricted (a condition or a limit).
        """
        from orcapod.pipeline.restrictions import refuse_restricted_cache_input

        refuse_restricted_cache_input(source)
        if data_context is None:
            data_context = source.data_context_key
        super().__init__(
            label=label,
            data_context=data_context,
            config=config,
        )
        # A database that cannot store the cache's own columns refuses it now,
        # before anything is read (ITL-1162). The source's columns are checked
        # when they are written.
        try:
            require_storable(
                cache_database,
                pa.schema([_recorded_at_field(), data_context_field()]),
            )
        except UnstorableColumnError as exc:
            exc.add_context(node_label=self.label)
            raise
        self._source: SourceProtocol = source
        self._cache_database = cache_database
        self._cache_path_prefix = cache_path_prefix
        self._explicit_cache_path = cache_path
        self._cached_stream: ArrowTableStream | None = None
        # The wrapped source's modified time when the cache last took in its rows, or
        # last found it had them all (ITL-1102): a check compares it with the wrapped
        # source's time, not with what the wrapped source's own check returned.
        self._source_seen: datetime | None = self._source_modified_time()
        # The record hashes of the rows the merged stream presents, recorded when it is
        # built; ``None`` before a build. A cache shared with another source can hold
        # rows this source has not presented yet (ITL-1102).
        self._presented_hashes: set[bytes] | None = None

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_config(
        self, db_registry: DatabaseRegistryProtocol | None = None
    ) -> dict[str, Any]:
        """Serialize this CachedSource configuration to a JSON-compatible dict.

        Args:
            db_registry: Optional registry. When provided, the cache database
                config is registered (deduplicated) and replaced by its key
                string.  When None, the full database config is embedded inline.

        Returns:
            Dict containing the inner source config, cache database config,
            cache path prefix, and resolved cache path (for cache-only loading).
        """
        db_config = self._cache_database.to_config()
        if db_registry is not None:
            cache_db_ref = db_registry.register(db_config)
        else:
            cache_db_ref = db_config

        # All sources accept db_registry — always forward it so nested
        # CachedSources can also deduplicate their cache database configs.
        inner_source_config = self._source.to_config(db_registry=db_registry)

        return {
            "source_type": self.SOURCE_KIND,
            "inner_source": inner_source_config,
            "cache_database": cache_db_ref,
            "cache_path_prefix": list(self._cache_path_prefix),
            # The path without its layout component, as ``cache_path`` takes it.
            "cache_path": list(self._unversioned_cache_path),
            # Note: identity fields (content_hash, pipeline_hash, etc.) intentionally
            # omitted: a cache has its wrapped source's identity, which the inner
            # source's config carries; a proxy for a cache config that cannot be
            # rebuilt takes it from there (serialization._source_proxy_from_config).
        }

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        db_registry: DatabaseRegistryProtocol | None = None,
    ) -> CachedSource:
        """Reconstruct a CachedSource from a config dict.

        If the inner source cannot be resolved (e.g. it requires live data
        that is unavailable), ``resolve_source_from_config`` returns a
        ``SourceProxy`` preserving the original source's identity.  The
        CachedSource can still serve data from its cache database.

        Args:
            config: Dict as produced by ``to_config``.
            db_registry: Optional registry. When ``cache_database`` is a
                string key, it is resolved via ``db_registry.resolve()``.
                When None and the cache_database value is a string key,
                a ValueError is raised.

        Returns:
            A new CachedSource constructed from the config.
        """
        from orcapod.pipeline.serialization import (
            resolve_database_from_config,
            resolve_source_from_config,
        )

        cache_db_ref = config["cache_database"]
        if isinstance(cache_db_ref, str):
            if db_registry is None:
                raise ValueError(
                    f"cache_database is a registry key {cache_db_ref!r} but no "
                    "db_registry was provided."
                )
            cache_db = resolve_database_from_config(db_registry.resolve(cache_db_ref))
        else:
            cache_db = resolve_database_from_config(cache_db_ref)

        # All sources accept db_registry — always forward it so nested
        # CachedSources can resolve their own cache database references.
        # A saved restricted source never produces a cache, also when the source
        # cannot be rebuilt and a proxy (which carries no restriction) stands in.
        from orcapod.pipeline.restrictions import refuse_restricted_cache_config

        refuse_restricted_cache_config(config["inner_source"])
        inner_source = resolve_source_from_config(
            config["inner_source"], fallback_to_proxy=True, db_registry=db_registry
        )

        return cls(
            source=inner_source,
            cache_database=cache_db,
            cache_path_prefix=tuple(config.get("cache_path_prefix", ())),
            cache_path=tuple(config["cache_path"]) if "cache_path" in config else None,
        )

    # -------------------------------------------------------------------------
    # Identity — delegate to wrapped source
    # -------------------------------------------------------------------------

    def identity_structure(self) -> Any:
        return self._source.identity_structure()

    @property
    def source_id(self) -> str:
        """The wrapped source's source id: the cache presents that source's rows."""
        return self._source.source_id

    @property
    def source_name(self) -> str:
        """The name part of the wrapped source's source id."""
        return parse_source_id(self._source.source_id).name

    def _compute_arrow_schemas(self) -> "tuple[pa.Schema, pa.Schema]":
        """The wrapped source's Arrow schemas: the cache stores that source's rows."""
        schemas = typed_arrow_schemas(self._source)
        for schema in schemas:
            require_supported_nullable_types(schema)
        return schemas

    # -------------------------------------------------------------------------
    # Cache path — scoped to source's content hash
    # -------------------------------------------------------------------------

    @property
    def _unversioned_cache_path(self) -> tuple[str, ...]:
        """The cache path without its layout component.

        This is where Orcapod up to v0.2 stored the cache table (the old layout).
        """
        if self._explicit_cache_path is not None:
            return self._explicit_cache_path
        return self._cache_path_prefix + (
            "source",
            f"node:{self._source.content_hash().to_hex()}",
        )

    @property
    def cache_path(self) -> tuple[str, ...]:
        """Cache table path: scoped to the source's content hash, then the layout version.

        The layout version (``sdb_v2``) is always the last component, also when an
        explicit ``cache_path`` was given.
        """
        return self._unversioned_cache_path + (SOURCE_DB_SCHEMA_VERSION,)

    def _account_for_stale_tables(self) -> None:
        """Report, once per source object, cache tables of another layout.

        Without an explicit ``cache_path`` the path holds a hash that can differ between
        builds, so caches of another layout are looked for anywhere under
        ``<cache_path_prefix>/source`` (ITL-954): those are caches of any source stored
        there, and they are reported without a number of rows. With an explicit path,
        list the layouts under that path. Older layouts are left untouched.
        """
        if self._stale_tables_checked:
            return
        self._stale_tables_checked = True
        account_for_stale_tables(
            find_stale_tables(
                self._cache_database,
                name_path=self._unversioned_cache_path
                if self._explicit_cache_path is not None
                else self._cache_path_prefix + ("source",),
            ),
            delivers_side_effects=False,
            current_context=self.data_context_key,
        )

    # -------------------------------------------------------------------------
    # Stream interface — delegate schema, materialize with cache
    # -------------------------------------------------------------------------

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._source.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._source.keys(columns=columns, all_info=all_info)

    @property
    def cached_source(self) -> SourceProtocol:
        """The source this cache reads and keeps the rows of."""
        return self._source

    def _wraps_cache(self) -> bool:
        """Whether the wrapped source passes through another cache's rows unchanged."""
        from orcapod.core.sources.source_proxy import SourceProxy

        source = self._source
        while isinstance(source, SourceProxy) and source.delegate is not None:
            source = source.delegate
        return isinstance(source, CachedSource)

    def _live_records(self) -> pa.Table:
        """Read the wrapped source's rows, each with its record hash in front.

        Raises if the source cannot provide data (e.g. an unbound ``SourceProxy``),
        or reads under a restriction (a proxy bound to a restricted source after the
        cache was built, ITL-1218): nothing is read or stored then.
        """
        from orcapod.pipeline.restrictions import refuse_restricted_cache_input

        refuse_restricted_cache_input(self._source)
        require_same_data_context(self._source.data_context_key, self.data_context_key)
        # Only transparent wrappers can use a would-be cache snapshot here. A
        # source that creates new rows or IDs may depend on its origin's actual
        # read-back order (for example, DerivedSource's positional record IDs).
        token = None if self._wraps_cache() else _PREPARED_CACHE_READS.set(None)
        try:
            live_table = self._source.as_table(
                columns={"source": True, "system_tags": True, "meta": True}
            )
        finally:
            if token is not None:
                _PREPARED_CACHE_READS.reset(token)
        validate_source_table(live_table)
        live_table = live_table.drop(
            [
                name
                for name in live_table.column_names
                if name.startswith(constants.META_PREFIX)
            ]
        )

        # Compute per-row record hashes for dedup: hash the full row including
        # all system tag columns.  The record_id system tag encodes row origin
        # (source_id + provenance token), so two rows that are byte-identical in
        # their data columns but originate from different positions in the source
        # will have different record_ids and must be stored as distinct entries.
        arrow_hasher = self.data_context.arrow_hasher
        record_hashes: list[bytes] = []
        for batch in live_table.to_batches():
            for i in range(len(batch)):
                # A hash stored as a value carries its label: b"<label>:<digest>".
                record_hashes.append(
                    arrow_hasher.hash_table(batch.slice(i, 1)).to_prefixed_digest()
                )

        return live_table.add_column(
            0,
            self.HASH_COLUMN_NAME,
            pa.array(record_hashes, type=pa.large_binary()),
        )

    def _prepare_ingestion(self) -> pa.Table:
        """Read and check a delivery without writing any cache rows."""
        self._cache_database.refresh_table(self.cache_path)
        # Before the wrapped source's rows are read: a cache of another source id is
        # refused without reading or writing any row (ITL-1425).
        self._require_rows_of_this_source()
        live_with_hash = self._live_records()
        self._account_for_stale_tables()
        # Before anything is written: a row whose record id the cache holds with
        # other content refuses the whole read (ITL-1396).
        self._require_no_changed_rows(live_with_hash)
        return live_with_hash

    def _write_delivery(self, live_with_hash: pa.Table, seen: datetime | None) -> None:
        """Write an accepted delivery, deduplicating on its stored row hash."""
        # When the cache wrote these rows: one time for the whole write, appended
        # after the record hashes like the data context, so in no hash. A row the
        # cache already holds is skipped below and keeps its first time.
        recorded_at = _recorded_at_field()
        to_store = live_with_hash.append_column(
            recorded_at,
            pa.array(
                [datetime.now(timezone.utc)] * live_with_hash.num_rows,
                recorded_at.type,
            ),
        )
        self._cache_database.add_records(
            self.cache_path,
            with_data_context(to_store, self.data_context_key),
            record_id_column=self.HASH_COLUMN_NAME,
            skip_duplicates=True,
        )
        self._cache_database.flush()
        self._source_seen = seen

    def _require_no_changed_rows(self, live_with_hash: pa.Table) -> None:
        """Refuse live rows that change a row the cache holds (ITL-1396).

        A record id names one row. A live row whose record id is in the cache (rows of
        the current data context) under another record hash has other content than the
        cached row: a tag, data, system-tag or source-info value differs. The rule is
        the same whether the record id is made from key columns or is the row's
        position. A live row whose record hash the cache holds is the same row again,
        and one under a new record id is a new row: both are accepted. Sources
        without a generated record id still deduplicate by record hash.

        Raises:
            CachedSourceChangedRowError: Naming the source id, the first changed
                record ids and how many rows changed. Nothing was written.
        """
        records = self._read_cache_table()
        if records is None:
            return
        # The source builder's declared order distinguishes its generated pair
        # from similarly named custom tags. Neither table's order identifies it.
        declared = typed_zero_row_table(self._source)
        if declared is not None:
            id_column = _record_id_column(declared.column_names)
        elif not schema_requires_io(self._source, ColumnConfig(system_tags=True)):
            names = self._source.keys(columns={"system_tags": True})[0]
            id_column = _record_id_column(names) or _unambiguous_record_id_column(
                live_with_hash.column_names
            )
        else:
            id_column = _unambiguous_record_id_column(live_with_hash.column_names)
        if (
            id_column is None
            or id_column not in live_with_hash.column_names
            or id_column not in records.column_names
        ):
            return
        records = keep_rows_of_data_context(records, self.data_context_key)
        cached: dict[str, set[bytes | None]] = {}
        for record_id, record_hash in zip(
            cast("list[str]", records.column(id_column).to_pylist()),
            records.column(self.HASH_COLUMN_NAME).to_pylist(),
            strict=True,
        ):
            cached.setdefault(record_id, set()).add(record_hash)
        changed = sorted(
            {
                record_id
                for record_id, record_hash in zip(
                    cast("list[str]", live_with_hash.column(id_column).to_pylist()),
                    live_with_hash.column(self.HASH_COLUMN_NAME).to_pylist(),
                    strict=True,
                )
                if record_id in cached and record_hash not in cached[record_id]
            }
        )
        if not changed:
            return
        named = ", ".join(changed[:_CHANGED_ROWS_NAMED])
        more = len(changed) - _CHANGED_ROWS_NAMED
        raise CachedSourceChangedRowError(
            f"CachedSource over {self.source_id!r}: the source delivered "
            f"{len(changed)} row{'s' if len(changed) != 1 else ''} whose record id "
            f"the cache at {'/'.join(self.cache_path)!r} holds with other content: "
            f"{named}{f' and {more} more' if more > 0 else ''}. Nothing was written "
            "and the cache is unchanged. A record id names one row: give the changed "
            "data a new source id, or keep a new cache for it (another cache_path "
            "or cache database)."
        )

    def _source_modified_time(self) -> datetime | None:
        """The wrapped source's modified time, or ``None`` if it has none."""
        return getattr(self._source, "last_modified", None)

    def _cached_record_hashes(self) -> set[bytes]:
        """The record hashes of the rows cached under the current data context."""
        self._account_for_stale_tables()
        records = self._read_cache_table()
        if records is None:
            return set()
        records = keep_rows_of_data_context(records, self.data_context_key)
        self._require_source_ids(records)
        return {
            h
            for h in records.column(self.HASH_COLUMN_NAME).to_pylist()
            if h is not None
        }

    @override
    def check_for_new_data(self) -> bool:
        """Check the wrapped source; move only if the next read shows new rows (ITL-1102).

        The wrapped source is checked first. When its modified time moved since the
        cache last took in its rows (by this check, or by a check made through another
        source that wraps it), its rows are hashed and compared with the rows this
        source presents (recorded when it built its merged rows; before a build, the
        rows already cached: a read of the cache; nothing is written). The cache keeps
        every row it ever held, so rows that were removed from the wrapped source change
        nothing. When this source would present rows it has not (another source sharing
        the cache may already have written them), the merged rows held in memory are
        dropped (the next read writes the new rows to the cache and merges them) and the
        modified time moves. A comparison that raises leaves everything as it was, so
        the next check compares again.

        The invariant this keeps: the modified time moves exactly when the next read
        would present a different set of rows. That read presents the rows cached
        (every row any source sharing the cache wrote, including rows since deleted from
        the wrapped source) and the live rows, so a row another source wrote to a shared
        cache moves this source too, and a row deleted from the wrapped source moves
        nothing.

        The check guarantees nothing for two cached sources that share one cache (the
        same database and ``cache_path``, or the same table): give each cached source
        its own cache. A change missed there means a run does not compute the new rows;
        nothing wrong is stored (ITL-1143).

        Returns:
            ``True`` if this source will present new rows at the next read.

        Raises:
            CachedSourceIdMismatchError: If the cache holds rows of another source
                id; raised before the wrapped source is checked (ITL-1425).
        """
        if not isinstance(self._source, RootSource):
            return False
        # The cache first: a cache of another source id is refused before the
        # wrapped source is checked or read (ITL-1425).
        self._require_rows_of_this_source()
        # Checking a nested cache needs its would-be rows, but must leave the
        # delivery for the next consuming read.
        token = _PREPARED_CACHE_READS.set([] if self._wraps_cache() else None)
        try:
            _ = self._source.check_for_new_data()
            seen = self._source_modified_time()
            if seen == self._source_seen:
                return False
            live = self._live_records().column(self.HASH_COLUMN_NAME).to_pylist()
        finally:
            _PREPARED_CACHE_READS.reset(token)
        # The next read presents the cache's rows and the live rows together (the
        # cache keeps every row any source sharing it wrote).
        cached = self._cached_record_hashes()
        presented = (
            self._presented_hashes if self._presented_hashes is not None else cached
        )
        gains = not ({h for h in live if h is not None} | cached) <= presented
        self._source_seen = seen
        if not gains:
            return False
        self._cached_stream = None
        self._update_modified_time()
        return True

    def _read_cache_table(self) -> pa.Table | None:
        """Read and validate the physical cache before using any of its columns."""
        records = self._cache_database.get_all_records(
            self.cache_path, record_id_column=self.HASH_COLUMN_NAME
        )
        if records is None:
            return None
        if SOURCE_RECORDED_AT_COL not in records.column_names:
            raise InputValidationError(
                f"CachedSource at {'/'.join(self.cache_path)!r} is missing "
                f"{SOURCE_RECORDED_AT_COL!r}. Rebuild the cache from the source "
                "using a new cache path or database."
            )
        return validate_source_table(records, stored_cache=True)

    def _read_cached_records(self) -> "pa.Table | None":
        """Read the cached rows written under the current data context.

        Returns:
            The cached rows without the stored-only columns (the record hash,
            ``__recorded_at`` and ``__data_context``), or ``None`` when the cache table
            does not exist.
        """
        records = self._read_cached_records_with_hashes()
        if records is None:
            return None
        return records.drop(self._STORED_ONLY_COLUMNS)

    def _read_cached_records_with_hashes(self) -> "pa.Table | None":
        """Read the cached rows written under the current data context, as stored.

        Every read of the cache's rows goes through here: ``get_all_records()`` and
        the merged stream behind ``as_table()`` and every other read of this source.
        (``_cached_record_hashes`` reads the record-hash column only, and returns no
        row value.) Rows of another data context stay in the table untouched, and a
        cache at the older layout is not read (ITL-944). Rows whose Arrow types are
        not the source's are refused here (``_require_declared_types``), so no read
        can present them.

        Returns:
            The cached rows with their record hash, ``__recorded_at`` and
            ``__data_context`` columns, or ``None`` when the cache table does not exist.

        Raises:
            InputValidationError: If a cached column reads back as another Arrow type.
        """
        self._account_for_stale_tables()
        records = self._read_cache_table()
        if records is None:
            return None
        stored_rows = records.num_rows
        records = keep_rows_of_data_context(records, self.data_context_key)
        if records.num_rows < stored_rows and not self._skipped_rows_reported:
            self._skipped_rows_reported = True
            report_rows_of_other_data_contexts(
                self._cache_database.base_path + self.cache_path,
                stored_rows - records.num_rows,
            )
        self._require_source_ids(records)
        self._require_declared_types(records)
        return records

    def _require_rows_of_this_source(self) -> None:
        """Refuse a cache table holding rows of another source id (ITL-1425).

        Called before the wrapped source is checked (``check_for_new_data``) or its rows
        are read, so once a cache is found to hold another id no row of the wrapped
        source is read or returned and nothing is written. Building the wrapped source
        (``from_config``) may read it before the first check. The cache table is read once per
        source object, until it is found to hold this source's rows; a table not yet
        written holds no other id.

        Raises:
            CachedSourceIdMismatchError: If a stored row names another source id.
        """
        if self._source_ids_checked:
            return
        records = self._read_cache_table()
        if records is None:
            return
        self._require_source_ids(
            keep_rows_of_data_context(records, self.data_context_key)
        )

    def _require_source_ids(self, records: pa.Table) -> None:
        """Refuse cached rows written under another source id than this one (ITL-1425).

        A cache keeps one source's rows. Rows stored under another full source id (the
        same data read by another version of its kind, after a load where the class
        declares a newer ``SOURCE_VERSION``) are not this source's rows: presenting
        them, or adding this source's rows beside them, would mix two sources in one
        table. Nothing is converted or moved.

        Raises:
            CachedSourceIdMismatchError: Naming both ids.
        """
        stored_ids: set[str] = set()
        for name in records.column_names:
            if name.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX):
                stored_ids.update(
                    value
                    for value in records.column(name).unique().to_pylist()
                    if value is not None
                )
        others = sorted(stored_ids - {self.source_id})
        if others:
            raise CachedSourceIdMismatchError(
                f"CachedSource at {'/'.join(self.cache_path)!r} reads "
                f"{self.source_id!r}, but its cache holds rows of "
                f"{', '.join(repr(i) for i in others)}; no row of the source was read and nothing was written. "
                "The cache must be rebuilt for the new id (another cache_path or "
                "cache database), or the reader's version pinned to the one that "
                "wrote it."
            )
        if stored_ids:
            self._source_ids_checked = True

    def _prepare_delivery(self, pending: list[_PreparedCacheRead]) -> pa.Table | None:
        """Check the live delivery or, for an unavailable source, serve stored rows."""
        checkpoint = len(pending)
        try:
            return self._prepare_ingestion()
        except NotImplementedError:
            del pending[checkpoint:]
            logger.info(
                "Inner source %r cannot provide data; serving from cache only.",
                self._source.source_id,
            )
            return None

    def _preview_stream(self, delivery: pa.Table | None) -> ArrowTableStream:
        """Present the cached and newly accepted rows while enclosing caches check them."""
        stored = self._read_cached_records_with_hashes()
        rows = stored.drop(self._STORED_ONLY_COLUMNS) if stored is not None else None
        if delivery is not None and self._cache_database.stores_records:
            physical = self._read_cache_table()
            existing = (
                set(physical.column(self.HASH_COLUMN_NAME).to_pylist())
                if physical is not None
                else set()
            )
            # A new hash is stored once, with its last occurrence in the delivery.
            additions = {
                record_hash: i
                for i, record_hash in enumerate(
                    delivery.column(self.HASH_COLUMN_NAME).to_pylist()
                )
                if record_hash not in existing
            }
            if additions:
                new_rows = delivery.take(sorted(additions.values())).drop(
                    [self.HASH_COLUMN_NAME]
                )
                rows = (
                    new_rows
                    if rows is None
                    else pa.concat_tables([rows, new_rows.select(rows.column_names)])
                )
        return self._stream_over(rows if rows is not None else self._empty_table())

    def _publish_stream(
        self, delivery: pa.Table | None, seen: datetime | None
    ) -> ArrowTableStream:
        """Write a checked delivery and hold the rows read back from the database."""
        if delivery is not None:
            self._write_delivery(delivery, seen)
        stored = self._read_cached_records_with_hashes()
        if stored is None:
            self._presented_hashes = set()
            all_records = self._empty_table()
        else:
            self._presented_hashes = {
                h
                for h in stored.column(self.HASH_COLUMN_NAME).to_pylist()
                if h is not None
            }
            all_records = stored.drop(self._STORED_ONLY_COLUMNS)

        self._cached_stream = self._stream_over(all_records)
        self._update_modified_time()
        return self._cached_stream

    def _stream_over(self, records: pa.Table) -> ArrowTableStream:
        """Present cached-source rows with the source's tag columns and context."""
        # Cached rows are this source's: give them its data context, not the default.
        return ArrowTableStream(
            records,
            tag_columns=self._source.keys()[0],
            data_context=self.data_context,
            data_tags=stream_data_tags(self._source),
        )

    def _require_declared_types(self, records: pa.Table) -> None:
        """Refuse cached rows whose Arrow types are not the source's (ITL-640).

        This source's identity is built from the wrapped source's Arrow types
        (``_compute_arrow_schemas``); a cache store that gives a column back as
        another type (Delta reads a fixed-size list back as a list, ITL-1275) would
        present rows of another schema under that identity, and an empty and a
        filled cache would differ. So nothing is read; nothing is converted. The rule
        is ``schema_utils.stored_type_mismatch``, as for a ``DerivedSource``.

        Raises:
            InputValidationError: Naming the column and both types.
        """
        tag_schema, data_schema = self.arrow_schemas
        mismatch = schema_utils.stored_type_mismatch(
            pa.schema([*tag_schema, *data_schema]),
            records.schema,
            self.data_context.arrow_hasher,
        )
        if mismatch is not None:
            field, stored = mismatch
            raise InputValidationError(
                f"CachedSource over {self._source.source_id!r}: the cached column "
                f"{field.name!r} reads back as {stored.type}, but the source's rows "
                f"have {field.type}; nothing was read."
            )

    def _empty_table(self) -> pa.Table:
        """Build an empty Arrow table matching the source's output schema.

        The wrapped source's zero-row table when it gives one (its Arrow types and
        system-tag columns, as ``head()`` shows them; ITL-912), otherwise one built
        from its Python output schema.
        """
        typed = self._typed_zero_row_table()
        if typed is not None:
            return typed
        tag_schema, data_schema = self._source.output_schema()
        merged = dict(tag_schema)
        merged.update(data_schema)
        type_converter = self.data_context.type_converter
        arrow_schema = type_converter.python_schema_to_arrow_schema(merged)
        require_supported_nullable_types(arrow_schema)
        return pa.Table.from_pylist([], schema=arrow_schema)

    @property
    def is_stale(self) -> bool:
        """True if the wrapped source has been modified since the last build.

        Overrides ``StreamBase.is_stale`` because CachedSource is a RootSource
        (no upstreams/producer) yet still depends on the wrapped ``_source``.
        """
        own_time = self.last_modified
        if own_time is None:
            return True
        src_time = self._source.last_modified
        return src_time is None or src_time > own_time

    def _ensure_stream(self) -> ArrowTableStream:
        """Build the merged stream after every cache reached by the read accepts it."""
        pending = _PREPARED_CACHE_READS.get()
        for prepared in pending or ():
            if prepared.cache is self:
                return prepared.stream
        if self._cached_stream is not None and not self.is_stale:
            return self._cached_stream
        seen = self._source_modified_time()
        if pending is not None:
            checkpoint = len(pending)
            delivery = self._prepare_delivery(pending)
            preview = self._preview_stream(delivery)
            pending.append(
                _PreparedCacheRead(
                    self, preview, delivery, seen, len(pending) != checkpoint
                )
            )
            return preview

        pending = []
        token = _PREPARED_CACHE_READS.set(pending)
        try:
            delivery = self._prepare_delivery(pending)
            # Publish from the inside out. A successful read still uses each
            # database's actual presentation order. The preview checked the
            # same record ids and hashes without modifying any store.
            for prepared in pending:
                accepted = (
                    prepared.cache._live_records()
                    if prepared.reads_prepared_cache
                    else prepared.delivery
                )
                prepared.stream = prepared.cache._publish_stream(
                    accepted, prepared.seen
                )
            if pending:
                delivery = self._live_records()
            return self._publish_stream(delivery, seen)
        finally:
            _PREPARED_CACHE_READS.reset(token)

    def clear_cache(self) -> None:
        """Discard in-memory cached stream (forces rebuild on next access)."""
        self._cached_stream = None

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        return self._ensure_stream().iter_data()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        return self._ensure_stream().as_table(columns=columns, all_info=all_info)

    def _typed_zero_row_table(self) -> "pa.Table | None":
        """The wrapped source's zero-row table: the cache stores that source's rows.

        See ``typed_zero_row_table``; neither the cache nor the source is read.
        """
        table = typed_zero_row_table(self._source)
        return None if table is None else validate_source_table(table)

    def _schema_requires_io(self, config: ColumnConfig) -> bool:
        """Whether reading the schema with ``config`` would poll: as for the wrapped source."""
        return schema_requires_io(self._source, config)

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first rows without reading or writing the cache.

        Once the source has been used, its merged stream is in memory and this is a slice
        of it. Before that its rows are not known: they are the cache's earlier rows
        merged with the wrapped source's, and reading them writes the cache. The head
        then has the wrapped source's columns, no rows and ``NOT_COMPUTED`` (or the
        wrapped source's own reason when it has none to give).
        """
        check_head_size(n)
        if self._cached_stream is not None and not self.is_stale:
            return head_of(
                self._cached_stream.head(n, columns=columns, all_info=all_info), self
            )
        wrapped = head_of(
            self._source.head(0, columns=columns, all_info=all_info), self
        )
        if wrapped.status is not ViewStatus.ROWS:
            return wrapped
        return StreamView.without_rows(
            self,
            ViewStatus.NOT_COMPUTED,
            wrapped.table,
            tag_columns=wrapped.tag_columns,
            note="not read yet; run() reads and updates the cache",
        )

    def get_all_records(
        self,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table | None":
        """Retrieve the cached records that the current Arrow hasher wrote."""
        return self._read_cached_records()
