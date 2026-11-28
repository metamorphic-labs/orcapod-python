## `src/orcapod/core/nodes/`

### USC1 — Rewiring a node could leave downstream schemas and pipeline hashes stale
**Status:** resolved
**Severity:** high
**Issue:** ITL-1125

Assigning an upstream with a different schema could change the rewired node's output
schema while downstream nodes kept their previous schema predictions and pipeline hashes.
A stale pipeline hash still names the table for the old schema.

**Fix:** Operator and function node setters check the existing and replacement input
schemas before changing the wiring or clearing the schema memo. The comparison checks
tag and data roles, equivalent Python types, canonical Arrow field types and nullable
flags, and the system-tag schema. Replacements with matching schemas remain supported.
