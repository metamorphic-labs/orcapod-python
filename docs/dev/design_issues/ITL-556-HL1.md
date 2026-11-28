## `src/orcapod/config.py` — hash lengths in stored names

### HL1 — Hash truncation changed stored provenance column names and entry ids
**Status:** resolved
**Severity:** critical
**Issue:** ITL-556

The same job could write different system-tag column names and entry ids to the same
table paths when `schema_n_char` or `system_tag_n_char` shortened a digest. Short schema
digests could also collide and silently combine columns for distinct provenance.
The `path_n_char` setting had no callers.

**Fix:** Removed all three length settings and used full digests in system-tag
column names and logical table paths. Programmatic configurations refuse the
removed fields in typed objects and mappings. Loaders identify removed settings
in both tables and arrays of tables. Pipeline saves omit the fields, and the
loader accepts the null defaults older writers included automatically.
