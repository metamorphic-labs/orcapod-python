## `src/orcapod/core/operators/merge_join.py` — Arrow types

### MJ6 — `MergeJoin` raised `TypeError` when a colliding data column held struct values
**Status:** resolved
**Severity:** high
**Issue:** ITL-839 (related: ITL-1281)

`MergeJoin` sorted each row's merged values as Python values; a struct value is a `dict`,
which cannot be ordered, so a colliding struct column raised `TypeError`.

**Fix:** resolved by ITL-1281 (MJ3): nothing is compared, and the lists are built from the
inputs' Arrow arrays, so a struct column (and a list of structs, a user class) merges into
`large_list<struct<...>>` in argument order.
