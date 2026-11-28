## `src/orcapod/core/function_pod.py` — function pod identity

### CXP1 — Function pod pipeline identity omitted the invocation context parameter
**Status:** resolved
**Severity:** high
**Issue:** ITL-1559

A plain function pod and a context-taking pod with the same underlying data function URI
had the same pipeline identity. Under the three-chain design they would become one graph
node over the same slot, even though one receives an invocation context.

**Fix:** Use the same structure for the pod's content and pipeline identities. Include
the context parameter name when configured so the pod and its downstream steps have
distinct pipeline hashes. The stale-table check refuses delivery when a completion table
under the same name is outside the current job's completion paths. A saved job restores its
context-taking function nodes and reads their results using its recorded identities.
