## `src/orcapod/core/operators/drop_nulls.py` — null rows and nullable columns (ITL-943)

### DN1 — No way to turn a `T | None` column into `T`
**Status:** resolved
**Severity:** medium
**Issue:** ITL-943

Many value columns are nullable by specification (EDI, Spiral tables), and a pod written as
`def f(x: int)` is refused on an `int | None` column. `PolarsFilter` could drop the null rows,
but the column stayed `T | None`, so the pod was still refused; a nullable key column likewise
could not be joined with a non-nullable one.

**Fix:** `DropNulls(columns)` and `stream.drop_nulls(columns)` keep the rows where every
named column (tag or data) is non-null and clear the nullable flag of each named field.
Decisions (coordinator, November 11, 2026): one operator for tag and data columns; only
top-level validity is read; a missing column is `InputValidationError`; an already non-nullable
column is a no-op; identity is the sorted tuple of unique names; sync and async paths share one
table rule (`_narrow_table`), pure Arrow. A surviving row's `Data` (or `Tag`) content hash
changes when a named column was nullable, to the hash of the same row declared non-nullable;
no existing golden value moved.

Open: the class and method names (`DropNulls` / `drop_nulls` versus `DropNulls` /
`drop_nulls`) await the developer's decision. Possible follow-up: a variant that raises on a
null instead of dropping the row.
