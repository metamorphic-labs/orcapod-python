## `src/orcapod/pipeline/async_orchestrator.py` — interrupting a run inside a running loop

### INT1 — the second-interrupt warning names a node whose cancelled row's function has returned
**Status:** open
**Severity:** low
**Issue:** ITL-1192

A job run from inside a running event loop (a notebook cell) runs on its own thread. The first
interrupt cancels it, and the second returns control with a `RuntimeWarning` naming the nodes
whose functions have not returned (`_warn_function_still_running`). The nodes come from
`_RowsInFlight`, which counts a row from `on_data_start` until `on_data_end` or
`on_data_crash`. A row task cancelled while in progress (`process_one` in
`FunctionPod.async_execute`, awaiting `run_in_executor`) gets `CancelledError` and fires
neither. So its node stays counted whether its function was queued, still running or had
already returned on its thread, and the warning can name too many nodes ("a function in
nodes 'stuck', 'after' has not returned yet", although `after`'s function returned long
before). The class docstring admits the queued case only.

Effect: only the text of that warning, after a second interrupt. No crash and no wrong data:
the cancelled run writes nothing either way. The race made
`test_second_interrupt_returns_and_a_late_function_writes_nothing[delta]` flaky; it failed
half of the Python 3.11 runs on `main` (ITL-1229, which made that test deterministic).
`test_second_interrupt_does_not_name_a_node_whose_function_returned`
(`tests/test_pipeline/test_job_run_in_event_loop.py`) reproduces the defect without relying on
timing and is a strict xfail until it is fixed.

Proposed fix: count a row as in flight from the worker thread. Wrap the callable given to
`run_in_executor` so the count rises when the function starts on its thread and falls when it
returns or raises, whatever happens to the asyncio task. Then remove the xfail marker.
