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
`RunLosses`) and gives it to copies of the built-in observers. Node-scoped observers and
data loggers carry that counter, so overlapping executions remain independent even when
they share an observer or a run id. A failed event encoding or write is counted and logged.
Custom observer subclasses receive lifecycle hooks on the supplied instance. The
run puts the record in `RunSummary.lost_observer_events` (per observer kind: count and first
error's text) and issues one `ObserverEventsLostWarning` at the end of the run, after the
summary is kept, at the caller's line. The end and not the first loss: the counts are
complete then, the warning turned into an error loses no result, and the first loss happens
inside a node, possibly on a worker thread. A completed run reports observer losses even
when the row-failure warning is promoted to an exception. Additive: no existing field
changed meaning, and the run's result and exit are unchanged. A run that raises before
the summary is kept reports nothing beyond the log lines. A job node's own `run()` takes
no observer, so it has nothing to lose. An observer
handed directly to an orchestrator or to `node.execute` has no job run to report for it: its
failed writes are logged only. A re-contextualized status observer now keeps its run id.
Which stores observers work on (the SQLite case) stays open on the issue.
