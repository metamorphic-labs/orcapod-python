## `src/orcapod/logical_types/` — identity of user classes (dataclasses, pydantic models)

### UCI1 — A user class was identified by `module.qualname` only, without its fields
**Status:** resolved
**Severity:** high
**Issue:** ITL-881

A dataclass or pydantic model was identified by `f"{module}.{qualname}"`. The same class
defined in a notebook (`__main__`) and in a module were different types; two classes with
the same name and different fields shared pipeline hashes and tables, so a changed class
read the old table, failed, and the run returned 0 rows with only log warnings; re-running
the notebook cell that defines a class raised `ValueError: … already bound` until a kernel
restart; a subclass was stored as its parent, dropping its extra fields.

**Fix:** a user class is identified by `{kind, name, fields}` (spec
`superpowers/specs/2026-11-10-user-class-identity-v0.3.md`). The identity string
`Name@<full SHA-256 of the canonical record>` is the logical type name and Arrow extension
name, with the record in the extension metadata. `fields` lists `[name, type]` pairs in
declared order and the stored struct follows it: field order is part of the identity
(developer, November 11, 2026), so the same fields in another order are another type.
`op.register_type` declares names without modifying the class; `__main__` classes default
to their class name. The registry rebinds an unchanged identity to a re-defined class and
matches user classes by exact class only. The read path finds the class by kind and name,
checks the identity, and raises `ImportError` or `TypeIdentityError` naming the column;
`DeltaTableDatabase` raises `TypeIdentityError` as is, never wrapped in `StoreReadError`.
Testing a read candidate registers neither the class nor its nested classes
(`probe_record`), so a rejected candidate can still be declared under a name. Resolves the entry PR #288 proposes for
the same issue. Not covered: computed fields (deferred by the developer), local classes,
Enum/NamedTuple/TypedDict.

**Known limitation (ITL-894):** reading a stored column whose storage differs from the
registered one only by layout (`string` vs `large_string`) is accepted at any depth, so
Delta-compacted tables stay readable. `__arrow_ext_deserialize__` then returns the
registered large storage type, which is correct for Parquet and Delta reads (the decoder
converts the data) but not over Arrow IPC or the C stream interface, where Arrow reads
32-bit offsets as 64-bit and the arrays are invalid. Top-level columns had this since
ITL-602; nested struct fields since this fix. Orcapod's own writers and Polars round trips
never produce non-large storage. Pinned by a strict xfail in
`tests/test_user_types/test_read_path.py`.
