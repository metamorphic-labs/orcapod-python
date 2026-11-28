# File Hash Caching

Orcapod can cache the SHA-256 digest of files so that re-hashing the same
unchanged file — across pipeline runs or between `op.File` and `op.Directory`
accesses — skips the disk read entirely. This page explains how the cache
works, how to turn it on, and when it helps.

If you have not used `op.File` and `op.Directory` yet, read
[File and Path Types](file-types.md) first.

## FileHasher, HashCacher, and CachedFileHasher

`FileHasher` is the base hasher. When you call `hash_file(path)` on it, it
reads the file's bytes from disk and returns a content hash (SHA-256 by
default, configurable via the `algorithm` argument). It does this every time,
with no memory of previous results.

`HashCacher` is a protocol for any object that maps a `FileHashKey` to a
`ContentHash`. The key is a four-field struct:

```
FileHashKey(path, mtime_ns, size, algorithm)
```

An ordinary lookup trusts the file's resolved path, nanosecond modification
time, and byte size. A change to any of these fields gives the file a
different key in the default lookup mode. If a file is rewritten to the same
size at the same path and its original modification time is restored, the
lookup returns the cached hash without reading the file. This hit bypasses
the write and its conflict check.

`FileHashKey.algorithm` is **PROVISIONAL**. Its value is the hash label
(`ContentHash.method`) reported by the file hasher's `hash_algorithm` property.
The cache stores each algorithm's hash separately for a given file state.
For example, `orcapod warm-cache --algorithm md5` fills MD5 entries. A later
SHA-256 lookup can fill its own entries alongside them.

A cache write raises `ValueError` if the hash label or digest length is invalid
for the key, or if the key already holds a different well-formed hash. Writing
an identical hash leaves the existing entry and its timestamp unchanged. Reads
accept values in the exact format `<label>:<digest>`, with the key's label and
that algorithm's digest length. The persistent caches treat malformed values
as misses. The next valid write for the same key replaces the malformed value,
refreshes its timestamp, and logs a warning naming the file and hash algorithm.
An incompatible hash returned by a custom cache is also a miss.

`orcapod warm-cache --force` skips the cache lookup and recomputes each eligible
file's hash. Content-hashing algorithms read the file's contents. The
`hash_path` algorithm checks that the file exists and hashes its path string.
If a new hash differs from a well-formed hash stored for the same key,
the cache retains the original entry and the command reports a file error. If
a rehash and write conflict occurs while a pipeline hashes a file column, it can
surface as `RuntimeError: Failed to process column 'recording'`, with a
`ValueError` cause beginning `Different hash already cached for FileHashKey`
and naming the file and algorithm. To replace a stale valid hash, clear the
[current cache](#cache-storage) and rerun the pipeline or warmer to rebuild the
entries. An explicit operation to replace a valid hash is tracked separately
(ITL-1547). Let file rewrites advance the modification time to avoid stale hits
in the default mode.

The label must name an algorithm whose digest length is fixed: a `hashlib`
algorithm such as `sha256` or `md5`, or `hash_path`, `xxh64` or `crc32`. Every algorithm
`FileHasher` can hash with qualifies. `CachedFileHasher` refuses (`ValueError`) an
inner hasher with any other label when it is built.

`CachedFileHasher` wraps a `FileHasher` with any `HashCacher`. When you call
`hash_file(path)` on it, it first constructs the key from the file's current
metadata and queries the cacher. On a cache hit, it returns the stored digest
immediately. On a miss, it delegates to the inner `FileHasher`, reads the
file, and stores the result in the cacher before returning.

## Activating file hash caching

Call `enable_file_hash_caching()` once at application startup, before any
hashing takes place:

```python
from orcapod.contexts import enable_file_hash_caching

enable_file_hash_caching()
```

`enable_file_hash_caching` lives in `orcapod.contexts` and is also exported from the
top-level `orcapod` namespace, as `orcapod.enable_file_hash_caching`.

You can also supply an explicit path for the SQLite database, as a `pathlib.Path`. Parent
directories are created if they do not exist:

```python
from pathlib import Path

enable_file_hash_caching(db_path=Path("cache/file_hash_cache.db"))
```

Calling the function more than once is safe. Orcapod logs a warning and
replaces the active cacher with the new one (there is always exactly one
caching layer), so the prior cache entries remain accessible at the same path
if you use the same `db_path`. `db_path` and `conninfo` are mutually
exclusive; passing both raises `ValueError`.

## Controlling when the cache is written

By default, every file that passes through `CachedFileHasher` is inserted
into the cache on a miss. Two optional knobs let you restrict this.

### Read-only mode

Use `read_only=True` when you want lookups from a shared or authoritative
cache but must not add new entries to it — for example, when consuming a
cache pre-populated by `populate_hash_cache()` without polluting it with
ad-hoc entries.

```python
from pathlib import Path

from orcapod.contexts import enable_file_hash_caching

enable_file_hash_caching(db_path=Path("shared/cache.db"), read_only=True)
```

Cache hits still work normally. On a miss, the file is hashed directly and
the result is returned to the caller — but it is never written to the cache.

### Minimum file size threshold

Use `min_cache_size_bytes` to skip the cache write overhead for small
files. For small files, the disk I/O bottleneck does not apply, so the
cache lookup and write add latency without meaningful savings.

```python
from orcapod.contexts import enable_file_hash_caching

# Skip caching for files smaller than 1 MB
enable_file_hash_caching(min_cache_size_bytes=1_048_576)
```

Files smaller than the threshold are still hashed and the hash is returned
to the caller — they are simply not inserted into the cache. Files at or
above the threshold behave normally. Set to `None` (the default) or `0`
to disable the threshold.

### Combining both

The two knobs compose independently. `read_only=True` takes precedence:
when enabled, no entry is ever written regardless of file size.
`min_cache_size_bytes` is an additional guard that applies only when the
cacher is writable.

```python
from pathlib import Path

from orcapod.contexts import enable_file_hash_caching

# Read-only + skip files below 512 KB (threshold is moot when read-only,
# but harmless and documents intent)
enable_file_hash_caching(
    db_path=Path("shared/cache.db"),
    read_only=True,
    min_cache_size_bytes=524_288,
)
```

## Ignoring mtime in cache lookups

By default, a cache hit requires **path, mtime_ns, size, and algorithm** to all match the stored
entry. In several common deployment scenarios, mtime is unreliable — it changes even
when file content has not — causing spurious cache misses and unnecessary re-hashing:

- **rsync / file transfer tools** — rsync and similar tools do not preserve mtime by
  default. Even with `--times`, sub-second precision is often lost on destination
  filesystems, producing a different `mtime_ns` for otherwise identical files.
- **Restore from backup** — backup and restore pipelines (tar, restic, Borg, cloud
  storage sync) frequently reset mtime to the restore timestamp rather than the
  original file timestamp.
- **Container bind mounts and volume remounts** — remounting a volume or restarting a
  container can reset or truncate mtime precision depending on the host filesystem and
  container runtime (Docker, Podman, Kubernetes).
- **CI `touch` / build system side-effects** — build scripts, test harnesses, and CI
  pipelines sometimes call `touch` on input files to force rebuilds, or copy files in
  ways that update mtime without changing content.
- **Network filesystems (NFS, CIFS/SMB)** — clock skew between the client and server,
  or coarse mtime granularity on older NFS versions, can produce stale or shifted
  timestamps that differ from the values recorded in the cache.

Set `match_mtime=False` to drop mtime from the lookup criterion. A cache hit then
requires **path, size, and algorithm** to match:

```python
from orcapod.contexts import enable_file_hash_caching

enable_file_hash_caching(match_mtime=False)
```

When multiple cache entries share the same path, size, and algorithm (recorded at different
mtimes), Orcapod returns the hash from the entry with the most recent `mtime_ns`.

**The write path is unchanged.** mtime is always recorded when a new entry is
inserted. Switching `match_mtime=False` on a cache that was already populated under
the default `match_mtime=True` still produces hits — no cache rebuild is needed.

### Known trade-off

With `match_mtime=False`, a file modification that preserves the file's byte count
will **not** be detected by the cache. The stored hash from the previous version will
be returned silently. This is rare in practice (most writes change file size), but
you should be aware of the trade-off before enabling this mode.

Use `match_mtime=False` only in environments where mtime changes are known to be
unreliable. For most local-disk or NFS deployments the default (`match_mtime=True`)
is the right choice.

## Directory hashing (op.Directory)

When Orcapod hashes an `op.Directory`, it traverses the directory tree and
builds a Merkle hash from all the files it finds. Each file's content hash
contributes to the directory's overall hash.

The `DirectoryHandler`'s `BasicDirectoryHasher` uses the **same**
`CachedFileHasher` instance as `FileHandler`. This means:

- If a file was previously hashed as `op.File`, it is a cache hit the next
  time it appears during directory traversal.
- Conversely, if a file was first encountered during directory traversal, it
  is a cache hit the next time it is hashed as `op.File`.

No extra configuration is required. Calling `enable_file_hash_caching()` once
activates the shared cacher for both file and directory hashing automatically.

## Cache storage

By default, Orcapod stores the cache in a SQLite database at:

```
~/.orcapod/file_hash_cache.db
```

You can override this path in two ways:

- Set the `ORCAPOD_HASH_CACHE_DB` environment variable to the desired path.
- Pass the `db_path` argument to `enable_file_hash_caching()`.

SQLite and PostgreSQL use the new `file_hash_cache_v2` table. The old
`file_hash_cache` table remains untouched and the cacher reads only the new
table. SQLite uses `PRAGMA user_version` to record the layout of the new table.
PostgreSQL records version 2 in `file_hash_cache_v2_meta`. ITL-1535 tracks
migration of old caches, including both SQLite layouts (with and without
`cached_at`). Let your next pipeline run or `orcapod warm-cache` hash the files
once more to fill the new table.

To inspect cached entries using the SQLite command-line tool:

```bash
sqlite3 ~/.orcapod/file_hash_cache.db "SELECT path, size, algorithm, cached_at FROM file_hash_cache_v2 LIMIT 10;"
```

To clear all cached entries:

```bash
sqlite3 ~/.orcapod/file_hash_cache.db "DELETE FROM file_hash_cache_v2;"
```

## When caching helps -- and when it doesn't

**Caching helps when:**

- You work with large files (GB-scale recordings, model checkpoints) that are
  rehashed across multiple pipeline runs. Skipping the read saves significant
  time.
- You hash directories where most files are unchanged between runs. The
  majority of per-file lookups become cache hits, and only modified or new
  files require a full read.
- The same files are accessed both as `op.File` and inside an `op.Directory`
  in the same workflow. The shared `CachedFileHasher` turns the second access
  into a cache hit regardless of which path was first.

**Caching does not help much when:**

- **Files are small.** Disk I/O is not the bottleneck for small files, so the
  cache lookup adds overhead without meaningful savings. Use
  ``min_cache_size_bytes`` to skip caching small files automatically.
- Files change on every run. A different `mtime_ns` or `size` means a
  different key, so every access is a cache miss.
- It is the first run on a new machine or a freshly cleared cache. Every file
  is a miss on a cold cache; you pay both the lookup cost and the file read.

## Choosing a backend: SQLite vs Postgres

Orcapod ships two hash cache backends. Pick based on your deployment:

| | SQLite | Postgres |
|---|---|---|
| Setup | Zero — file on disk | Requires a running Postgres server |
| Scope | Per-machine | Shared across machines and pipeline runs |
| Concurrency | One writer at a time | Concurrent writers |
| Best for | Local development, single-machine pipelines | Distributed pipelines, shared caches |

### Using the SQLite backend (default)

```python
from pathlib import Path

from orcapod.contexts import enable_file_hash_caching

enable_file_hash_caching()  # uses ~/.orcapod/file_hash_cache.db
# or specify a path:
enable_file_hash_caching(db_path=Path("shared/nfs/cache.db"))
```

### Using the Postgres backend

Requires `psycopg` (install with `uv add "orcapod[postgresql]"`, or
`pip install "orcapod[postgresql]"`) and a reachable Postgres server:

<!-- docs-test: skip -->
```python
from orcapod.contexts import enable_file_hash_caching

enable_file_hash_caching(
    conninfo="postgresql://user:pass@db-host:5432/orcapod_cache"
)
```

You can also construct `PostgresHashCacher` directly and pass it to
`CachedFileHasher` for advanced configurations:

<!-- docs-test: skip -->
```python
from orcapod.hashing import CachedFileHasher, FileHasher, PostgresHashCacher

cacher = PostgresHashCacher(
    conninfo="postgresql://user:pass@db-host:5432/orcapod_cache",
    read_only=True,           # lookup only, no writes
    min_cache_size_bytes=1_048_576,  # skip files < 1 MB
)
hasher = CachedFileHasher(file_hasher=FileHasher(), cacher=cacher)
```

### Schema

`PostgresHashCacher` creates the following table on first use
(`CREATE TABLE IF NOT EXISTS`):

```sql
CREATE TABLE IF NOT EXISTS file_hash_cache_v2 (
    path      TEXT   NOT NULL,
    mtime_ns  BIGINT NOT NULL,
    size      BIGINT NOT NULL,
    algorithm TEXT   NOT NULL,
    hash      BYTEA  NOT NULL,
    cached_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
    PRIMARY KEY (path, mtime_ns, size, algorithm)
)
```

Minimum supported Postgres version: **14**.
