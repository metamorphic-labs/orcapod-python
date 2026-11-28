## Result cache and pipeline record selection

### RTC1 — A result table's token column type followed the first write
**Status:** resolved
**Severity:** medium
**Issue:** ITL-1563

A function result table stored each output's source-info token column
(`_source_<key>`) with the type of the written row, which a hand-built output takes
from its values. An ordinary call always gives a nullable `large_string`, but a custom
write through `CachedDataFunction.record_data` could carry a list token. The first write
fixed the column's type, and a later write of the other type failed as a database write
error, in either order.

**Fix:** The token column's type is declared once, `RESULT_TOKEN_COLUMN` beside
`COMMON_RESULT_COLUMNS` in `core/result_cache.py`: a nullable `large_string`.
`ResultCache.store`, the one place every result write goes through, refuses
(`InputValidationError`, naming the column and both types) a row whose token column
has another type, before anything is written. Custom token types are not supported.
A row with no token column for an output gets it as a `large_string` null, so every
stored row has every declared token column on every database; a token column that names
no output of the row is refused.
No stored column, path, record id or hash moved.
The stored field is the declared one whatever the row's field says (a non-nullable
token field is wrapped under the nullable field, never cast), and the type checked is
the one the row carries. With ITL-1557, `as_table(columns={"source": True})` preserves
the declared type even for a null token, so list, dictionary-encoded, and binary token
columns are refused before writing. A table that already holds a list token column is
not migrated.
