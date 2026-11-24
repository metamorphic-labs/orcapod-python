from __future__ import annotations

import os

from collections.abc import Collection
from pathlib import Path
from typing import Any, ClassVar, TYPE_CHECKING

import pyarrow as pa
from typing_extensions import override

from orcapod.core.sources.base import RootSource, saved_tag_only_columns
from orcapod.core.data_tags import TagOnlyColumns, source_data_tags
from orcapod.core.sources.source_ids import location_source_name
from orcapod.core.sources.reload import same_rows
from orcapod.core.sources.stream_builder import (
    normalize_record_id_columns,
    SourceStreamBuilder,
    SourceStreamResult,
)
from orcapod.databases.storage_utils import delta_to_pyarrow_dataset
from orcapod.types import PathLike
from orcapod.utils.lazy_module import LazyModule
from orcapod.utils.schema_utils import _normalize_column_list

if TYPE_CHECKING:
    import deltalake
else:
    deltalake = LazyModule("deltalake")


class DeltaTableSource(RootSource):
    """A source backed by a Delta Lake table.

    The table is read once at construction time using ``deltalake``'s
    PyArrow integration. The resulting Arrow table is enriched by
    ``SourceStreamBuilder`` (source-info, schema-hash, system tags).

    At the start of every run (``check_for_new_data``, ITL-1102) the source checks the
    table's Delta version. When the version moved, it reads the table again and, if
    the rows changed (in order without a ``record_id_columns``, in any order with one: a
    version that only compacted files or
    changed metadata changes nothing), holds the new rows and moves its modified time.
    Record ids keep their rule: the ``record_id_columns`` value, or the row's position.

    Data-tags (ITL-1023): every declared tag column is a data-tag by default,
    also a data column of the same name. ``tag_only_columns`` (keyword-only) names the
    tags kept plain (tag only, not data, as in v0.3); ``True`` keeps all of them plain.
    """

    SOURCE_KIND: ClassVar[str] = "delta_table"
    """Config type key and the kind in the source id's kind part (one vocabulary)."""
    SOURCE_VERSION: ClassVar[int] = 1
    """The version of this kind's implementation, in the kind part (ITL-1425)."""
    TABLE_IS_ROWS: ClassVar[bool] = True
    TAKES_TAG_ONLY_COLUMNS: ClassVar[bool] = True

    def __init__(
        self,
        delta_table_path: PathLike,
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_columns: str | Collection[str] | None = None,
        source_id: str | None = None,
        *,
        tag_only_columns: TagOnlyColumns = (),
        **kwargs: Any,
    ) -> None:
        resolved = Path(delta_table_path).resolve()

        super().__init__(source_id=source_id, **kwargs)
        if self._source_name is None:
            # The table folder's path or URI as passed: a relative path stays relative.
            # A default name is where the data lives, not a name the user gave: it is
            # not checked for a start shaped like a source id (ITL-1425).
            self._source_name = location_source_name(os.fspath(delta_table_path))

        self._delta_table_path = resolved

        try:
            self._delta_table = deltalake.DeltaTable(str(self._delta_table_path))
        except deltalake.exceptions.TableNotFoundError:
            raise ValueError(f"Delta table not found at {self._delta_table_path}")

        self._tag_columns: Collection[str] = _normalize_column_list(tag_columns)
        self._system_tag_columns: Collection[str] = system_tag_columns
        self._record_id_columns = normalize_record_id_columns(record_id_columns)
        self._key_columns = self._record_id_columns or ()
        self._version: int = self._delta_table.version()
        table = self._read_table()
        # The rows as read, columns and types included: a check compares a new read
        # with them, and refuses a table whose columns changed.
        self._table: pa.Table = table
        # Every declared tag is a data-tag unless kept tag-only (ITL-1023); a
        # rebuild after a check (ITL-1102) applies the same choice.
        data_tags = source_data_tags(self._tag_columns, tag_only_columns)
        self._tag_only_columns: tuple[str, ...] = tuple(
            c for c in self._tag_columns if c not in data_tags
        )
        result = self._build(table)
        self._stream = result.stream
        self._tag_columns = result.tag_columns
        self._system_tag_columns = result.system_tag_columns
        if self._source_name is None:
            self._source_name = result.source_name

    def _read_table(self) -> "pa.Table":
        """Read the table at the Delta version ``self._delta_table`` is loaded at."""
        # The Delta log stores field-level nullable flags; to_pyarrow_dataset()
        # preserves them exactly — no inference needed.
        return delta_to_pyarrow_dataset(
            self._delta_table, as_large_types=True
        ).to_table()

    def _build(self, table: "pa.Table") -> SourceStreamResult:
        """Enrich ``table`` as this source's stream (source info, system tags)."""
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        return builder.build(
            table,
            tag_columns=self._tag_columns,
            data_tags=source_data_tags(self._tag_columns, self._tag_only_columns),
            source_name=self._source_name,
            kind_part=self.kind_part(),
            record_id_columns=self._record_id_columns or (),
            system_tag_columns=self._system_tag_columns,
        )

    @override
    def check_for_new_data(self) -> bool:
        """Take in the table's rows if its Delta version moved and its rows changed.

        Only the Delta log is read when the version did not move. A version that moved
        without changing the rows (``OPTIMIZE``, a metadata change) leaves the held rows
        and the modified time as they are. The rows are compared as read from the
        table, every column included (a ``system_tag_columns`` column too), in any
        order (``same_rows``).

        Returns:
            ``True`` if the source now holds new rows.

        Raises:
            ValueError: If the table's columns or their types changed: the source's
                schema is part of its identity; build a new source to read it.
        """
        self._delta_table.update_incremental()
        version = self._delta_table.version()
        if version == self._version:
            return False
        table = self._read_table()
        if not table.schema.equals(self._table.schema, check_metadata=True):
            raise ValueError(
                f"Delta table {str(self._delta_table_path)!r} changed its columns at "
                f"version {version}: the source was built with {self._table.schema}, "
                f"the table now has {table.schema}. Build a new DeltaTableSource to "
                "read it"
            )
        # Row positions are record ids without a record_id_column: order counts.
        if same_rows(self._table, table, ordered=not self._record_id_columns):
            self._version = version
            return False
        stream = self._build(table).stream
        self._version = version
        self._stream = stream
        self._table = table
        self._update_modified_time()
        return True

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize this source's configuration to a JSON-compatible dict."""
        return {
            "source_type": self.SOURCE_KIND,
            "delta_table_path": str(self._delta_table_path),
            "tag_columns": list(self._tag_columns),
            **self._tag_only_config(),
            "system_tag_columns": list(self._system_tag_columns),
            "record_id_columns": None
            if self._record_id_columns is None
            else list(self._record_id_columns),
            **self._source_id_config(),
            **self._identity_config(),
        }

    @classmethod
    def from_config(
        cls, config: dict[str, Any], db_registry=None
    ) -> "DeltaTableSource":
        """Reconstruct a DeltaTableSource from a config dict."""
        return cls._from_saved_name(
            config,
            lambda name: cls(
                delta_table_path=config["delta_table_path"],
                tag_columns=config.get("tag_columns", ()),
                tag_only_columns=saved_tag_only_columns(config),
                system_tag_columns=config.get("system_tag_columns", ()),
                record_id_columns=config.get("record_id_columns"),
                source_id=name,
            ),
        )
