"""Catalog contract for SQLite and PostgreSQL connector stores."""

from __future__ import annotations

import json
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable

from orcapod.errors import OrcapodDatabaseError
from orcapod.types import ColumnInfo

if TYPE_CHECKING:
    import pyarrow as pa

STORE_PATH_LAYOUT_VERSION = 1
# Quoted as one identifier. The dots keep this name outside the grammar for
# both old and current record table names.
STORE_PATH_CATALOG = f"orcapod.store_paths.v{STORE_PATH_LAYOUT_VERSION}"
TABLE_PATH_ENTRY_KIND = "table"

# A table entry uses an empty entry_name and holds the path as canonical JSON.
# A future column entry can key its mapping by the physical column name.
STORE_PATH_CATALOG_FIELDS = """(
    table_name TEXT NOT NULL,
    entry_kind TEXT NOT NULL,
    entry_name TEXT NOT NULL,
    entry_value TEXT NOT NULL,
    layout_version INTEGER NOT NULL CHECK (layout_version = {layout_version}),
    PRIMARY KEY (table_name, entry_kind, entry_name)
)"""


@runtime_checkable
class SQLStoreConnector(Protocol):
    """SQL operations a connector store needs to track record table ownership."""

    def get_current_store_schema(self) -> str:
        """Return the persistent schema selected for this store operation."""
        ...

    def get_store_table_paths(
        self, name: str, *, exact: bool = False
    ) -> list[tuple[str, str, str]]:
        """Return schema, physical name and stored JSON path for each record table."""
        ...

    def store_table_transaction(
        self, table_name: str, path_json: str
    ) -> AbstractContextManager[tuple[str, bool]]:
        """Verify or register a path and yield its schema and table existence."""
        ...

    def get_column_info(
        self, table_name: str, *, schema: str | None = None
    ) -> list[ColumnInfo]:
        """Inspect the columns of a table in the verified schema."""
        ...

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        pk_column: str,
        *,
        schema: str | None = None,
    ) -> None:
        """Create a record table in the verified schema."""
        ...

    def upsert_records(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool = False,
        *,
        schema: str | None = None,
    ) -> None:
        """Write rows to the record table in the verified schema."""
        ...


def catalog_name_pattern(name: str) -> str:
    """Escape a physical name prefix for a SQL LIKE expression with escape '!'."""
    return name.replace("!", "!!").replace("_", "!_").replace("%", "!%") + "%"


def require_catalog_layout(table_name: str, layout_version: object) -> None:
    """Refuse to interpret a table entry in an unsupported catalog layout."""
    if layout_version != STORE_PATH_LAYOUT_VERSION:
        raise OrcapodDatabaseError(
            f"Table {table_name!r} uses unsupported catalog layout version "
            f"{layout_version!r}; this client supports version {STORE_PATH_LAYOUT_VERSION}"
        )


def stored_path(table_name: str, path_json: str) -> tuple[str, ...]:
    """Decode the original path, refusing an unreadable catalog entry."""
    try:
        value: object = json.loads(path_json)  # pyright: ignore[reportAny]
    except (TypeError, ValueError) as exc:
        raise OrcapodDatabaseError(
            f"Table {table_name!r} has an unreadable stored path"
        ) from exc
    if isinstance(value, list):
        components = cast("list[object]", value)
        if components and all(isinstance(part, str) for part in components):
            return tuple(cast("list[str]", components))
    raise OrcapodDatabaseError(f"Table {table_name!r} has an invalid stored path")


def require_store_table_owner(
    table_name: str,
    expected: tuple[str, ...],
    actual: tuple[str, ...],
) -> None:
    """Refuse to open the table for a different path with the same name."""
    if actual != expected:
        raise OrcapodDatabaseError(
            f"Table {table_name!r} already belongs to path {actual!r} and cannot be used for {expected!r}"
        )


def require_catalog_pair(
    table_name: str, has_table: bool, recorded_path: str | None, path_json: str
) -> None:
    """Refuse inconsistent ownership before creating or changing either object."""
    if recorded_path is None:
        if has_table:
            raise OrcapodDatabaseError(f"Table {table_name!r} has no stored path")
        return
    require_store_table_owner(
        table_name,
        stored_path(table_name, path_json),
        stored_path(table_name, recorded_path),
    )
    if not has_table:
        raise OrcapodDatabaseError(
            f"Stored path for table {table_name!r} has no record table"
        )
