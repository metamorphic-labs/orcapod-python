# ITL-1488 file hash cache algorithm

`CachedFileHasher` keyed entries by path, modification time, and byte size.
After `orcapod warm-cache --force --algorithm md5`, the default SHA-256 file
handler could return the MD5 hash with caching enabled.

## Developer decisions on November 27, 2026

- Key all cachers by `(path, mtime_ns, size, algorithm)`. With
  `match_mtime=False`, reads select the latest matching entry within the
  requested algorithm. Writes always use the full key.
- A duplicate put of the same hash is a no-op. A different well-formed hash
  under the same key raises `ValueError`. The persistent cachers compare and
  repair malformed values in the same transaction as the insert attempt. A
  repair refreshes `cached_at` and warns with the file and algorithm. To
  replace a stale valid hash, clear the cache and rebuild it. An explicit
  replacement operation is tracked separately (ITL-1547).
- `FileHashKey.algorithm` is provisional and carries the hash label supplied
  by `FileContentHasherProtocol.hash_algorithm`. Cache warming uses that field
  for actual writes and dry-run counts.
- Store new rows in `file_hash_cache_v2` on both persistent backends. Keep the
  old table beside it and leave its data and schema untouched. SQLite uses
  `PRAGMA user_version` for the new table's layout. PostgreSQL records version 2
  in `file_hash_cache_v2_meta`. Remove the old SQLite migration helper. ITL-1535
  tracks migration into v2 from both old SQLite layouts (with and without
  `cached_at`).
- Keep strict write validation and treat malformed stored values as misses.
  Refusals from `hashlib.new` surface as `ValueError` for a put.

## Regression coverage

`tests/test_hashing/test_file_hash_cache_algorithm.py` covers both algorithm
orders, the default file handler, warming and dry-run statistics, lookup with
mtime disabled, strict validation, and concurrent SQLite writes.
`tests/test_hashing/test_hash_cachers.py` verifies legacy SQLite schemas and
the unchanged timestamp of an identical put.
`tests/test_hashing/test_postgres_hash_cacher.py` covers both algorithm orders,
legacy-table isolation, and identical and conflicting concurrent writers.
Both persistent cacher suites cover malformed-row repair and concurrent puts
against a malformed row. The PostgreSQL fixture creates a test-owned schema
and checks that cache operations and cleanup leave public cache tables intact.
