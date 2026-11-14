## `src/orcapod/pipeline/async_orchestrator.py` — error policy on the async path

### EP1 — The async orchestrator ignored `error_policy`; a node failure reached the caller as an `ExceptionGroup`
**Status:** resolved
**Severity:** high
**Issue:** ITL-879

`AsyncPipelineOrchestrator` stored `error_policy` and never used it. The pod's per-row
handler (`_FunctionPodBase.async_execute.process_one`) skipped every row whose function
raised, so under `error_policy="fail_fast"` an async run computed every other row and
finished, with a `RunSummary` and a `RowFailuresWarning`, where the sync run raised the row's
exception after storing the rows before it. Any other node failure that was not a store
error (a side-effect pod with `on_error="raise"`, an operator that rejected its inputs)
reached the caller as the task group's `ExceptionGroup`, where the sync run raised the
exception itself. Under `"continue"` the two orchestrators already agreed: no raise, the
same summary, one `RowFailuresWarning`, the per-row log at DEBUG on both (the issue's
"WARNING" premise predates ITL-997), the same observer events and stored rows.

**Fix:** `error_policy` is a keyword-only argument (default `"continue"`) of
`FunctionNodeProtocol.async_execute`, `FunctionJobNode.async_execute` and
`_FunctionPodBase.async_execute`, as of `execute` (coordinator decision, November 13, 2026),
and the orchestrator passes its own. Under `"fail_fast"` the pod re-raises the row's
exception after `on_data_crash`; the existing task groups cancel the rows and nodes still
running and every node closes its output in its existing `finally`; the failing node calls
`on_node_end` before it raises, as `execute` does. The orchestrator's cancel and close
machinery is unchanged. Its exception-group handler now always raises ONE exception: a store
error, then a `TypeIdentityError` (both as before), then the exception of the node earliest
in topological order (ties: the order the failures happened), the others noted on it
(`_single_failure`). Rows run concurrently, so the row raised is the first to fail, the first
in input order only with `max_concurrency=1`. Storage guarantee, as documented in
`docs/concepts/function-pods.md`: every stored row is a correct, complete result of its input
row; the failing row is never stored; which other rows are stored is not specified. No
hash, golden value or stored layout changed. Regressions:
`tests/test_pipeline/test_async_error_policy.py`. Related, not changed: a pod-level error
policy (F15, ITL-527).
