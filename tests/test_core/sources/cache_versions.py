"""Seed a source cache with rows written outside the changed-row rule (ITL-1396).

A ``CachedSource`` refuses a delivery that changes a row its cache holds, so a cache
never comes to hold two contents for one record id through one. A cache table can
still hold them when it was written outside that rule (two writers that both passed
the check, a table written by hand), and is then refused whenever it is read. Tests
of that refusal build the state with ``write_outside_the_rule``.
"""

from __future__ import annotations

from typing import Any

from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import CachedSource


def write_outside_the_rule(
    source: Any, cache_database: Any, cache_path: tuple[str, ...] | None = None
) -> None:
    """Add ``source``'s rows to a cache table without the changed-row check.

    The rows are cached once in a scratch database, as a ``CachedSource`` stores
    them, and that stored table is appended to the cache as it is.

    Args:
        source: The source whose rows to add.
        cache_database: The database holding the cache.
        cache_path: The cache's explicit path, when it has one.
    """
    scratch = InMemoryArrowDatabase()
    seeded = CachedSource(source, cache_database=scratch, cache_path=cache_path)
    _ = seeded.as_table()
    stored = scratch.get_all_records(
        seeded.cache_path, record_id_column=CachedSource.HASH_COLUMN_NAME
    )
    assert stored is not None
    cache_database.add_records(
        seeded.cache_path,
        stored,
        record_id_column=CachedSource.HASH_COLUMN_NAME,
        skip_duplicates=True,
    )
    cache_database.flush()
