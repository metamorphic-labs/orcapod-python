## `src/orcapod/semantic_types/canonical_order.py` — dict and set values in canonical order

### DO2 — Sets of logical elements used representation text as a fallback order
**Status:** resolved
**Severity:** medium
**Issue:** ITL-922

Struct storage fell back to representation-text order, which put a field with
`10` before `2`. NaNs and distinct instances with equal declared fields could
enter storage. A scalar-stored logical type could also admit NaNs and equal
storage values.

**Fix:** Apply canonical order to converted storage values. Compare structs in
declared field order and lists in item order, recursing with the scalar rules
at each leaf. Build one sort key per element. The order is independent of any
class comparison method, so the same rule applies to classes rebuilt from
metadata. Reject NaNs at any depth, incomparable values and duplicate storage
values. The conversion boundary supplies the column name. Rebuilding sets of
user classes can change affected row hashes.
