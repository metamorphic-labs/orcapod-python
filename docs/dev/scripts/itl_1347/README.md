# ITL-1347 measurement and differential harness

Scripts for the stages of `docs/dev/specs/itl-1347-job-table-transport.md`. None of them
changes the library. They use only the public API, except where `instrument.py` wraps
internals for the duration of a block.

| Script | What it does |
|---|---|
| `instrument.py` | `counting()`: datagrams made, by node kind and class; `_rows_to_table` calls, rows and seconds, by caller; node `execute` / `async_execute` calls and inclusive seconds. `edges_per_node(job)`. |
| `fixtures.py` | Fixture jobs on the tutorial dataset: `tutorial` (the tutorial's pipeline) and `spikes` (one row per spike, every operator kind once, a side effect). `ITL1347_SPIKES_MAX` limits the spike rows for a quick check. |
| `dump.py` | Runs one fixture in one mode (`cold`, `warm`, `held`, `load_only`) on one orchestrator, and dumps the run summary, the warnings, a recording observer's events, every node's `as_table(all_info=True)`, every stored table and its Delta version count, peak RSS and wall time. |
| `compare.py` | Compares two dumps: `--mode exact` (byte for byte, row order included) or `--mode set` (rows as a multiset, columns by name). Floats are compared by their bits (Arrow's `equals` holds NaN unequal to itself). `--calibrate` finds the volatile columns (values a run makes afresh: a `uuid7` datagram id, a write time, the source-info tokens that embed them) from two dumps of the same checkout. Those, and `__pf_var_git_hash` (the checkout's git state), are the only columns left out. Observer events are compared node by node, because `main` starts independent nodes in an order that varies between runs. |
| `differential.py` | Runs the whole differential: every fixture, mode, orchestrator and Polars thread count, under two interpreters, then the three comparisons. `--compare-only` redoes the comparisons on existing dumps. |
| `join_order_plugin.py` | A pytest plugin (ITL-1357, report only): every Polars inner join without `maintain_order` is recomputed with `maintain_order="left_right"` and the row orders are compared. |

## The differential (each stage's gate)

1. Build one environment per Orcapod under test, from a built wheel: pinned `main` and the
   stage branch. `uv run --with <path>` can reuse a cached build of the same version
   string, so install each wheel into its own environment and check the version and file
   it imports before measuring.
2. For each fixture, mode, orchestrator and Polars thread count (`POLARS_MAX_THREADS=1` and
   the default), run `dump.py` with each environment's interpreter, in its own work
   directory. A `warm` or `load_only` dump runs in the work directory a `cold` dump left.
3. Calibrate the volatile columns from two dumps of the same environment.
4. Compare:
   - sync on the branch against sync on `main`, `--mode exact`;
   - async on the branch against sync on the branch, `--mode set`;
   - async on the branch against async on `main`, `--mode set`.

   Only the differences the spec lists by name may remain.

Outputs are raw records: keep them outside the repository.
