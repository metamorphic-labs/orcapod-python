from __future__ import annotations

from collections.abc import Collection, Mapping
from contextlib import AbstractContextManager, nullcontext
from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.databases._pending_commits import PendingCommit
else:
    pa = LazyModule("pyarrow")


class NoOpArrowDatabase:
    """
    An ArrowDatabaseProtocol implementation that performs no real storage.

    All write operations are silently discarded. All read operations return
    None (empty / not found). Useful as a placeholder where a database
    dependency is required by an interface but persistence is unwanted —
    e.g. dry-run pipelines, testing that code paths execute without I/O,
    or benchmarking pure compute overhead.

    It keeps no Arrow field metadata (``keeps_field_metadata`` is ``False``), so a
    node that would store a data-tag refuses it (``InputValidationError`` when the
    database is attached): a function node over a stream with data-tags, or an
    operator node with data-tags in ``LOG`` or ``REPLAY`` mode (ITL-1019). Keep the
    tags plain (``tag_only_columns``) or use the cache mode ``OFF`` to dry-run such a
    node.
    """

    stores_records = False

    def __init__(
        self,
        _path_prefix: tuple[str, ...] = (),
        _root: NoOpArrowDatabase | None = None,
        _scoped_path: tuple[str, ...] = (),
    ) -> None:
        self._path_prefix = _path_prefix
        self._root = _root
        self._scoped_path = _scoped_path

    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str | bytes,
        record: "pa.Table",
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        pass

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: "pa.Table",
        record_id_column: str | None = None,
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        pass

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: str | bytes,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        return None

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
    ) -> "pa.Table | None":
        return None

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: Collection[str | bytes],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        return None

    def get_records_with_column_value(
        self,
        record_path: tuple[str, ...],
        column_values: Collection[tuple[str, Any]] | Mapping[str, Any],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        return None

    @property
    def keeps_field_metadata(self) -> bool:
        """Whether this database keeps Arrow field metadata: it keeps nothing (ITL-1019)."""
        return False

    def flush(self) -> None:
        pass

    def fail_pending_result(self, record_path: tuple[str, ...]) -> None:
        """There are no buffered writes to withdraw."""

    def refresh_table(self, record_path: tuple[str, ...]) -> None:
        """Nothing to refresh: the no-op store holds no table (ITL-1408)."""

    def flush_result_batch(
        self, record_path: tuple[str, ...], *, publish_records: bool
    ) -> None:
        """A discarded result leaves no batch to commit."""

    def flush_batch(self, record_path: tuple[str, ...]) -> None:
        """Flush the no-op store at the pipeline batch boundary."""
        self.flush()

    def pending_result(self, record_path: tuple[str, ...], record_id: bytes) -> None:
        """Treat explicitly discarded outputs as resolved for bookkeeping."""
        return None

    def guard_record(
        self, record_path: tuple[str, ...], record_id: bytes, dependency: PendingCommit
    ) -> AbstractContextManager[None]:
        """Accept the bookkeeping dependency without retaining the pipeline record."""
        return nullcontext()

    def list_table_paths(self, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
        """Return no path: this no-op implementation never stores a table.

        Args:
            prefix: Leading path components, relative to ``self.base_path``.

        Returns:
            Always an empty list.
        """
        return []

    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Return ``False`` — no tables are ever persisted by this no-op implementation.

        Args:
            record_path: Path components identifying the table, relative to
                ``self.base_path``.

        Returns:
            Always ``False``.
        """
        return False

    @property
    def base_path(self) -> tuple[str, ...]:
        """The current relative root of this database view (always () for root instances)."""
        return self._path_prefix

    def at(self, *path_components: str) -> "NoOpArrowDatabase":
        """Return a new NoOpArrowDatabase scoped to the given sub-path.

        All reads and writes are still discarded; the prefix only affects
        the reported base_path of the returned instance.
        """
        new_root = self._root if self._root is not None else self
        new_scoped_path = self._scoped_path + path_components
        return NoOpArrowDatabase(
            _path_prefix=self._path_prefix + path_components,
            _root=new_root,
            _scoped_path=new_scoped_path,
        )

    def to_config(self, db_registry: Any = None) -> dict[str, Any]:
        """Serialize database configuration to a JSON-compatible dict."""
        return {
            "type": "noop",
            "base_path": list(self._path_prefix),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "NoOpArrowDatabase":
        """Reconstruct a NoOpArrowDatabase from a config dict."""
        return cls(_path_prefix=tuple(config.get("base_path", [])))
