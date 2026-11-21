## `src/orcapod/hashing/arrow_hashers.py` — schema identity

### AS1 — A schema was hashed as Python types: sources of different Arrow types shared paths
**Status:** resolved
**Severity:** high
**Issue:** ITL-949 (with ITL-640, ITL-1136)

Every schema that entered an identity (a source's pipeline and content identity, an input
slot's, the system-tag column names, the output schema in a function's `uri`) was hashed as
its Python types, through `SchemaHandler`. `int32` and `int64` are both `int`, so two sources
that differed only in an Arrow type (integer or float width, signedness, timestamp unit or
zone, decimal precision, date32/date64, list item nullability, a struct field's width) had one
pipeline hash, one stored path and one set of system-tag names. Measured on November 18, 2026:
a write of the second source into the first one's table failed, a Delta operator cache cast
`uint8` into `int8` and dropped a list item's non-null flag silently, and a bind accepted a
source of another Arrow type.

**Fix:** developer decision, November 18, 2026 (ITL-640, option B). A schema is hashed as its
Arrow schema by `StarfixArrowHasher.hash_schema` (label `arrow_schema_v1`), everywhere a schema
enters an identity (`schema_utils.arrow_schema_identity`, `compute_schema_hash`,
`DataFunctionBase.output_data_schema_hash`). Sources take their rows' types from their typed
zero-row tables, slots hold and save Arrow schemas, a bind compares Arrow types (only `T` into
`T | None` at the top level), a `PollingSource` refuses a batch of another Arrow type, and a
`DerivedSource` refuses a stored read of another type. Scripts and measurements:
`docs/dev/scripts/itl_640/`; tests: `tests/test_hashing/test_arrow_schema_identity.py`. Two
store defects it surfaced are their own issues: ITL-1275 (Delta reads `fixed_size_list` back as
`large_list`) and ITL-1276 (SQLite loses a non-null flag).
