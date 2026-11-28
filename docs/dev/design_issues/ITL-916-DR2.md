## `src/orcapod/databases/delta_lake_databases.py` — read failures

### DR2 — A missing Delta log hid stored rows and allowed a new table at the same path
**Status:** resolved
**Severity:** critical
**Issue:** ITL-916

Removing `_delta_log` from a table with stored rows made reads report an absent
table. A later flush could create a new log that did not reference the original
Parquet files, silently leaving their rows out of the table.

**Fix:** When a read or write opens a Delta table and the backend reports it
missing, look for Parquet files at any depth outside transaction logs and
separately published child tables. When files exist, briefly retry opening the
log to give a concurrent first writer time to publish it, then raise
`OrcapodDatabaseReadError` if the log still cannot be opened. Discovery and
cached reads keep their existing behavior.
