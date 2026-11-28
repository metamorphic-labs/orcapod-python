## Arrow types and schema validation

### NN1 — Implicit nullability for nested Python fields in data context v1
**Status:** resolved
**Severity:** medium
**Issue:** ITL-445, ITL-842

Python container and user-class declarations have been producing nullable nested
Arrow fields even when a child is annotated with plain `T`. Giving the same
column an explicit Arrow schema with a non-null child has always been possible.
That Arrow flag distinguishes schemas, while the Python column hints can appear
identical in diagnostics.

**Decision (developer, November 28, 2026):** Keep implicit nullability for nested
fields produced from Python declarations in data context v1. Keep every
explicit Arrow flag and the boundary rule for whole columns. Enforce function
return annotations at supported nested positions. A strict nested input policy
is a future opt-in under ITL-1584.

**Fix:** Documented the input, output and storage rules. The shared function
output checker now rejects null items in a required set element position.
Joins and slot binding identify a nested flag mismatch and describe how to
match the two declarations. Existing schemas, hash labels and golden values
stay the same.
