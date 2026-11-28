## `src/orcapod/databases/delta_lake_databases.py` — read failures

### DR2 — A missing Delta log hid stored rows and allowed a new table at the same path
**Status:** resolved
**Severity:** critical
**Issue:** ITL-916

Removing `_delta_log` from a table with stored rows made reads report an absent
table. A later flush could create a new log that did not reference the original
Parquet files, silently leaving their rows out of the table.

**Fix:** On a Delta table miss, use the local filesystem or the Delta storage
backend to find Parquet files at the table root and in Hive-style partitions.
When files exist, briefly retry opening the log to give a concurrent first
writer time to publish it, then raise `OrcapodDatabaseReadError` if the log
still cannot be opened. Discovery checks for missing logs and existence checks
open the current log even if the handle already holds a row snapshot.
