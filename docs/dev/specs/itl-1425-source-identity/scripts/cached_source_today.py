"""What ``CachedSource`` stores today and where (ITL-1425, section 1).

Run from ``docs/dev/specs/itl-1425-source-identity``:
``uv run python -m scripts.cached_source_today``.
"""

from __future__ import annotations

import warnings

from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import CachedSource

from ._fixtures import same_rows_sources, scratch

warnings.simplefilter("ignore")


def short(path: tuple[str, ...]) -> str:
    return "/".join(p if len(p) < 24 else p[:20] + "..." for p in path)


def main() -> None:
    sources = same_rows_sources(scratch())
    db = InMemoryArrowDatabase()
    cached = {k: CachedSource(s, cache_database=db) for k, s in sources.items()}

    print("== Cache path per kind (same rows, same source_id, same key) ==")
    for kind, c in cached.items():
        print(f"   {kind:13s} {short(c.cache_path)}")
    paths = {c.cache_path for c in cached.values()}
    print(f"   distinct cache tables: {len(paths)}")

    print("\n== What one cache table holds after a read ==")
    c = cached["csv"]
    c.as_table()
    stored = db.get_all_records(c.cache_path, record_id_column="_record_hash")
    assert stored is not None
    print("   columns:", stored.column_names)
    print("   rows:", stored.num_rows)
    print(
        "   _record_hash row 0 starts with:",
        stored.column("_record_hash")[0].as_py()[:9],
    )

    print("\n== The record hash covers the system tags, so it holds the source id ==")
    other = same_rows_sources(scratch(), source_id="subjects_v2")["csv"]
    db2 = InMemoryArrowDatabase()
    c2 = CachedSource(other, cache_database=db2)
    c2.as_table()
    h1 = set(stored.column("_record_hash").to_pylist())
    stored2 = db2.get_all_records(c2.cache_path, record_id_column="_record_hash")
    assert stored2 is not None
    h2 = set(stored2.column("_record_hash").to_pylist())
    print(
        "   same rows under another source_id share no record hash:", h1.isdisjoint(h2)
    )

    print("\n== When the inner source is read ==")
    inner = sources["arrow_table"]
    reads = {"n": 0}
    original = inner.as_table

    def counting_as_table(*args, **kwargs):
        reads["n"] += 1
        return original(*args, **kwargs)

    inner.as_table = counting_as_table  # type: ignore[method-assign]
    c3 = CachedSource(inner, cache_database=InMemoryArrowDatabase())
    c3.as_table()
    first = reads["n"]
    c3.as_table()
    second = reads["n"] - first
    changed = c3.check_for_new_data()
    print(f"   first read of the cached source reads the inner source: {first} time(s)")
    print(f"   a second read in the same process reads it again: {second} time(s)")
    print(f"   check_for_new_data() with nothing changed reports new data: {changed}")


if __name__ == "__main__":
    main()
