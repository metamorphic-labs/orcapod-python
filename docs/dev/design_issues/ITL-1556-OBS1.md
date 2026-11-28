## `src/orcapod/pipeline/status_observer.py`, `logging_observer.py` — failed event writes

### OBS1 — Observers swallowed every failure to write an event; the run said nothing
**Status:** resolved
**Severity:** high
**Issue:** ITL-1556

`StatusObserver` (both write variants, the root and the contextualized one) and the
`DataLogger` of `LoggingObserver` caught every exception of `add_record` and only logged it.
A job whose observer database refused its rows, or failed partway through a run, finished
successfully with its status and log records missing or incomplete, and nothing in the
`RunSummary` or in a warning said so. ITL-1540 refuses a SpiralDB store at setup; that is
one case only (a SQLite-backed store is accepted at setup and refuses every event row).

**Fix:** Developer decision (November 28, 2026): the observers continue, and the loss is
reported. A job creates a counter for its execution (`pipeline/observer_losses.py`,
`RunLosses`). Node-scoped built-in observers and their data loggers carry that counter,
so overlapping executions count their own losses even when they share an observer or
a run ID. The job selects concrete built-in observers that use the original scope
factory. Custom scope hooks keep control of the observers they return. A failed event
encoding or write is counted and logged. Lifecycle hooks run on the caller's original
observers, and the built-ins derive event metadata as before.

The run puts the count and first reported error for each observer kind in
`RunSummary.lost_observer_events`. If the error's message cannot be rendered,
the summary shows its type name. The summary display and warning escape non-ASCII
text and ASCII control characters. If warning delivery raises, the totals are logged.
A filter that promotes `ObserverEventsLostWarning` to an error still raises it.
The run issues one `ObserverEventsLostWarning` after
keeping the completed summary. The warning points to the `run()` call, the
`run_async()` await, or the event loop's resumption site for a separately scheduled
coroutine. A completed run reports observer losses even when preparing or issuing the
row-failure warning raises. The earlier error still propagates.

A run that raises before keeping a summary reports the losses in the logs. A job
node's own `run()` takes no observer. Observers passed directly to an orchestrator or
`node.execute` report failed writes in the logs. Which stores support observer event
rows remains open on the issue.
