# Command-line reference

## `orcapod warm-cache`

Run `orcapod warm-cache PATH` to fill the SQLite file-hash cache before a pipeline run.
The command scans the directory at `PATH` recursively, skips symbolic links, and
hashes files that meet the minimum size and lack a cache entry. It reports how many files
it hashed, found in the cache, skipped by size, or could not process.

| Option | Meaning |
| --- | --- |
| `--min-size MB` | Minimum file size, in units of 1,048,576 bytes. Defaults to `500`. |
| `--db-path PATH` | Local SQLite cache path. Defaults to `ORCAPOD_HASH_CACHE_DB` if set, or `~/.orcapod/file_hash_cache.db`. |
| `--buffer-size BYTES` | Read buffer size in bytes. Defaults to `65536`. |
| `--workers COUNT` | Number of hashing threads. Defaults to `4`, with a minimum of `1`. |
| `--dry-run` | Count files that would be hashed. The scan runs serially and writes no hash entries. Opening the database can initialize a new cache file. |
| `--force` | Hash eligible files again, even when they already have a cache entry. With `--dry-run`, include those files in the count. |

!!! note "One file hash algorithm"
    Files are hashed with `sha256`, the only file hash algorithm (ITL-1155), so the
    command has no `--algorithm` option. A cache file that an earlier development
    version filled with another algorithm can be used as it is: those entries are
    left in place and never read, and the files are hashed once more with `sha256`.

To preview and then warm the same cache, run:

```sh
orcapod warm-cache ./recordings --min-size 100 --dry-run
orcapod warm-cache ./recordings --min-size 100
```

Enable [file hash caching](../concepts/file-hash-caching.md) in your Python process
before the pipeline hashes files. Use the same SQLite cache path as the command.
