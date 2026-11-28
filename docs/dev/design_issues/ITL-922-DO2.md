## `src/orcapod/semantic_types/canonical_order.py` — dict and set values in canonical order

### DO2 — Sets of logical elements used representation text as a fallback order
**Status:** resolved
**Severity:** medium
**Issue:** ITL-922

Struct storage fell back to representation-text order, which put a field with
`10` before `2`. NaNs and distinct instances with equal declared fields could
enter storage. A scalar-stored logical type could also admit NaNs and equal
storage values.

**Fix:** Apply canonical order after coercion to the declared Arrow element type.
Sort the struct fields in declared order and use a lexicographic key for each
list field. Check adjacent sorted values for duplicates. The five built-in
scalar-stored types keep their native sort. The order is independent of any
class comparison method, so the same rule applies to classes rebuilt from
metadata. Reject NaNs at any depth, incomparable values and duplicate storage
values. The conversion boundary supplies the column name. Rebuilding sets of
user classes can change affected row hashes.
