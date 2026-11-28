## `src/orcapod/core/nodes/function_node.py`

### NRM1 — The log claimed a recompute when a stored result was served
**Status:** resolved
**Severity:** medium
**Issue:** ITL-1525

When some of a function node's pipeline records named a result that was not in the
persistent result database, the node logged "These inputs will be recomputed". No run
did that whenever a result for the input was still stored: with two records for one
instance and only the older record's result left, a full run and a load-only run, sync
and async, served the stored result without calling the function, and reported it as
reused. Serving the stored result is the intended behaviour (developer, November 28,
2026); the message was wrong.

**Fix:** Nothing is logged for an input that has a record with a stored result. The
warning is kept for the inputs none of whose persistent records has a stored result,
counted by input, and says what was found and what each kind of run does (a full run
takes the result the store holds for the input, or computes it when there is none; a
load-only run skips it) instead of promising a recompute. No behaviour changed.
`tests/test_pipeline/test_newest_result_missing.py` pins the served result in every
combination of full and load-only run, sync and async path, in-memory and Delta
database, and the ephemeral mode as it is.
