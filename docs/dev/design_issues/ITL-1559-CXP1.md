## `src/orcapod/core/function_pod.py` — function pod identity

### CXP1 — Function pod pipeline identity omitted the invocation context parameter
**Status:** resolved
**Severity:** high
**Issue:** ITL-1559

A plain function pod and a context-taking pod with the same underlying data function URI
had the same pipeline identity. Under the three-chain design they would become one graph
node over the same slot, even though one receives an invocation context.

**Fix:** Include the context parameter in the pipeline identity when configured, giving the
pod and its downstream steps distinct pipeline hashes. Plain pods retain the data function's
pipeline identity. Result tables for context-taking pods also include the parameter name in
their paths. The stale-table check looks for exact previous pipeline and result
paths reserved for context-taking functions. Downstream tap logs and decorator
results at shared paths belong to the current plain branch, even when that
branch is absent from the job. A saved job restores its context-taking function
nodes and reads their results from the original paths.
