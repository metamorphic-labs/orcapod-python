"""Catalog contract for SQLite and PostgreSQL connector stores."""

from __future__ import annotations

import json
from contextlib import AbstractContextManager
from typing import Protocol, cast, runtime_checkable

from orcapod.errors import OrcapodDatabaseError

# Quoted as one identifier. The dots keep this versioned layout marker outside
# both the old and the current grammar for record table names.
STORE_PATH_CATALOG = "orcapod.store_paths.v1"


@runtime_checkable
class SQLStoreConnector(Protocol):
    """SQL operations a connector store needs to track record table ownership."""

    def get_store_table_paths(
        self, name: str, *, exact: bool = False
    ) -> list[tuple[str, str]]:
        """Return physical names and stored JSON paths for existing record tables."""
        ...

    def store_table_transaction(
        self, table_name: str, path_json: str
    ) -> AbstractContextManager[bool]:
        """Verify or register a path and yield whether its table already exists."""
        ...


def catalog_name_pattern(name: str) -> str:
    """Escape a physical name prefix for a SQL LIKE expression with escape '!'."""
    return name.replace("!", "!!").replace("_", "!_").replace("%", "!%") + "%"


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
