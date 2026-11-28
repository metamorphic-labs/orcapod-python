## `src/orcapod/config.py` — hash lengths in stored names

### HL1 — Hash truncation changed stored provenance column names and entry ids
**Status:** resolved
**Severity:** critical
**Issue:** ITL-556

The same job could write different system-tag column names and entry ids to the same
table paths when `schema_n_char` or `system_tag_n_char` shortened a digest. Short schema
digests could also collide and silently combine columns for distinct provenance.
The `path_n_char` setting had no callers.

**Fix:** Removed all three length settings and used full digests when naming stored
columns and tables. Config readers refuse any configured length, and the saved
pipeline loader accepts the null defaults older writers included automatically.
