# ITL-1488: the file hash cache returns a hash made with another algorithm

## Problem

`CachedFileHasher` keyed a cache entry by `(path, mtime_ns, size)` only and returned a hit
without checking which algorithm made it. After `orcapod warm-cache --force --algorithm md5`,
the default (SHA-256) file handler returned the MD5 value with caching on.

## Fix

- `FileHashKey` gains `label`, the hash label of the algorithm (`ContentHash.method`).
- `FileContentHasherProtocol` gains `hash_label`; `FileHasher` returns its algorithm,
  `CachedFileHasher` its inner hasher's label and builds the key with it.
- The cachers return an entry only for the key's label:
  - `InMemoryHashCacher`: the label is part of the dict key (entries of two algorithms
    coexist).
  - `SqliteHashCacher`, `PostgresHashCacher`: the stored BLOB is already
    `to_prefixed_digest()` (`<label>:<digest>`), so lookups filter on that prefix. No column,
    primary key or schema version changes (no stored layout change). One row per
    `(path, mtime_ns, size)`: a put of another algorithm replaces it (SQLite already
    `INSERT OR REPLACE`; PostgreSQL `ON CONFLICT DO UPDATE ... WHERE hash differs`, which
    stays safe for concurrent writers of one algorithm).
- `populate_hash_cache` (`warm-cache`) builds keys with its hasher's label, also in a dry
  run, so another algorithm's entry counts as not cached.

## Tests

`tests/test_hashing/test_file_hash_cache_algorithm.py`: two algorithms on one file over one
cache, both orders (in-memory and SQLite); warm-cache with one algorithm, then hash with
another; warm-cache statistics; the issue's route through `enable_file_hash_caching`; the
cachers with `match_mtime=False`. PostgreSQL label tests in `test_postgres_hash_cacher.py`
(`--postgres`). The test that pinned the bug as a known limitation is removed.

## Out of scope

Keeping several algorithms' hashes of one file in the persistent caches would need the label
in the primary key, a stored layout change (not made).
