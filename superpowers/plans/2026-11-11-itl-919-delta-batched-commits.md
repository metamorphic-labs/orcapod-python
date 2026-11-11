# ITL-919 (part 2): batched Delta commits (D2) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `PipelineJob` on a `DeltaTableDatabase` runs in time linear in its rows, close to
the in-memory store, by committing a function node's writes in batches instead of once per
row, and by serving the per-row lookups from indexed in-memory state instead of re-reading
the Delta table.

**Decision:** developer chose D2 (November 11, 2026). Code starts only when the coordinator
confirms the branch base (see "Base and overlaps").

**Architecture:** (1) Job function nodes stop flushing per row and commit every
`ROWS_PER_COMMIT = 500` rows, at node end, and (unchanged) on run failure and at job end.
(2) `DeltaTableDatabase` lookups read an indexed snapshot of the committed rows, held per
cached table handle and extended in place after its own commits, plus an indexed,
compacted pending batch. The rows a read returns are exactly #293's (stored rows plus
pending rows whose id is not stored).

**Tech Stack:** deltalake 1.x, PyArrow 23, pytest.

---

## What changes for users

| | Today (`main`) | D2 |
|---|---|---|
| When a job node's rows are committed | after every row (one Delta commit per row per table) | every 500 rows, at node end, before a failed run raises, at job end |
| Hard crash (`kill -9`, power loss) mid-node | loses at most the row in flight | loses at most the last 499 rows (and their pipeline records) of the running node; recomputed on rerun, nothing corrupt |
| A row whose record cannot be built/added (`add_record` raises) | `StoreWriteError` naming table, node, row tag | unchanged |
| A commit that fails | `StoreWriteError(flush=True)` naming the store (raised at that row) | `StoreWriteError(flush=True)` naming the store and the node (no row: the batch holds up to 500 rows); raised at the commit point, so up to 499 rows after the last commit were computed but not committed |
| Rows committed before a failure | kept | kept (every earlier batch); the failed batch stays pending and is retried once by the run's best-effort flush (PR #287), as today |
| Files per table | one parquet file per row | one per commit |
| Outside a job (`FunctionNode`, `CachedFunctionPod` used directly) | flush per row | unchanged (still per row) |
| Stored rows, ids, hashes, paths, schemas | — | unchanged (differential check) |

## Flush points (exact)

1. **Per batch:** `FunctionJobNode` counts rows whose result or pipeline record it wrote
   since its last commit; when the count reaches `ROWS_PER_COMMIT` (500) it calls
   `flush()` on its pipeline database view (views of one store share pending batches, so
   this commits the result and pipeline tables together; the ephemeral store is item 3).
   500 is below `DeltaTableDatabase`'s default
   `batch_size` (1000), so the store's own size-triggered auto-flush (inside `add_records`)
   does not fire first in the default configuration; it stays as the store's backstop when
   a user sets a smaller `batch_size` (then that commit happens inside the row's write and
   surfaces as today, as that row's `StoreWriteError`).
2. **Node end:** after the last row of `execute` / `async_execute` (DB path), before
   `on_node_end`.
3. **Ephemeral store:** a node with `is_result_ephemeral=True` also flushes its ephemeral
   store at points 1 and 2 (it is a separate database, not a view of the job store).
4. **Run failure:** unchanged — `PipelineJob.run` calls `_flush_best_effort(pipeline_db,
   result_db)` before re-raising.
5. **Job end:** unchanged — `PipelineJob.run` flushes both views.

Errors at points 1 and 2 are wrapped by the node (the caller, which has the context):
`StoreWriteError(pipeline_db.base_path, node_label=self.label, flush=True)` chained from the
store's exception, with the note that writes still pending may not have been stored. The
run fails whatever `error_policy` is (as for every `StoreWriteError`). `StoreReadError`
(from #290) passes through unwrapped.

## Per-row reads (why batching alone is not enough)

Measured with the per-row flush disabled on `main`: 7.5 / 12.6 / 34 ms per row at
100 / 400 / 1000 rows — still growing — because:

- `get_records_with_column_value` filters the whole pending batch (and, with #293,
  `_pending_not_stored` builds an array of every stored id per call), and otherwise reads
  the whole Delta table (`dataset.to_table()` then filter; no predicate pushdown, ENG-601).
- the pending batch gains one chunk per row (`concat_tables`), so every Arrow op on it
  costs O(rows pending).
- `_refresh_existing_ids_cache` re-reads the whole table after every commit.

D2 read design (in `DeltaTableDatabase`):

- **Committed snapshot** `_snapshots[record_key] = _Snapshot(handle, table, index)`: the
  committed rows of the cached `DeltaTable` handle, read once (`to_pyarrow_dataset(
  as_large_types=True).to_table()` + `normalize_table_view_types`, i.e. exactly the rows and
  types today's read gives) and indexed with the shared `_KeyIndex` (record id + queried
  binary/string columns, lazily). Valid while `handle is self._delta_table_cache[key]`.
  Reads already see only the cached handle's version today (it is reloaded only after the
  family's own flush), so caching its rows changes no visibility.
- **After the family's own commit** (`_flush_by_key`): if the reloaded handle's version is
  the old version + 1 (no other writer committed in between), append the inserted rows (the
  pending rows whose id was not stored — exactly what `when_not_matched_insert_all` inserts)
  to the snapshot and extend its indexes; otherwise drop the snapshot (re-read lazily).
  The existing-ids cache becomes the snapshot's record-id index (no full re-read per
  commit).
- **Pending batch**: compacted on add (binary counter, shared `_compact_tail`) and indexed
  lazily per key (extended on add, dropped on flush).
- **Lookups** (`get_records_with_column_value`, `get_records_by_ids`, `get_record_by_id`):
  committed matches from the snapshot index + pending matches not stored, combined with
  #293's `_combine_stored_and_pending` (same schema rules). Unindexable lookups fall back to
  filtering the snapshot (in memory, no re-read). This also fixes ITL-923 items 1–2 if #293
  has not landed (on top of #293 they are already fixed; the D2 change keeps them fixed).
- **Commit**: MERGE stays (no behaviour change). Optional, measured: append instead of
  MERGE when every pending id is absent from the snapshot's id index (a MERGE scans the
  target, so at 100k rows it dominates); kept only if the differential check is identical.

## Files and overlaps

| File | Change | Overlap |
|---|---|---|
| `src/orcapod/databases/_row_index.py` (new) | `_KeyIndex`, `_is_indexed_lookup`, `_compact_tail`, index build/extend helpers moved out of `in_memory_databases.py` (#304) | none (needs #304 merged or stacked) |
| `src/orcapod/databases/in_memory_databases.py` | import the helpers from `_row_index` | #304 |
| `src/orcapod/databases/delta_lake_databases.py` | `__init__`/`at()` (shared `_snapshots`, pending indexes), `_add_to_pending_batch` (compact + index), `_get_existing_ids`/`_refresh_existing_ids_cache` (snapshot id index), `_pending_not_stored`, `get_records_with_column_value`, `get_records_by_ids`, `get_record_by_id`, `_flush_by_key` (extend snapshot) | **#290** (StoreReadError in every read, `_flush_by_key`), **#293** (rewrites all read methods, shared caches in `__init__`/`at()`, `_flush_by_key`), **#295** (`_read_delta_table`, `get_all_records`, `_refresh_existing_ids_cache` read through a native filesystem) |
| `src/orcapod/core/nodes/function_node.py` | `attach_databases` / ephemeral setup: `CachedFunctionPod(..., auto_flush=False)` for `FunctionJobNode`; `execute` and `async_execute` (`record_and_forward`, end of DB path): commit counter + node-end flush + wrapping | **#290** and **#293** (`execute` except clause, `add_pipeline_record`, `record_and_forward`, `on_data_crash`) |
| `src/orcapod/core/result_cache.py` | none (the `auto_flush` switch exists) | #290 changes `store()` (no conflict) |
| `src/orcapod/pipeline/job.py` | none (flush on failure and at end exist) | #290 |
| `docs/concepts/function-pods.md` ("When the store cannot be written") | commit cadence, crash loss, error wording | #290 edits the same page |
| `CHANGELOG.md`, `DESIGN_ISSUES.md` (PQ1) | entries | parallel workers |

**Base and overlaps:** #293 already contains #290's commits and fixes the pending/committed
read combination; D2's read changes are written against #293's read methods. Recommended:
branch from #293's head (stacked) now and merge `main` after #290/#293/#295 land, or wait for
them. #295 touches three read helpers D2 also changes; the snapshot read should go through
whatever filesystem #295 introduces (one call site).

## Tests (TDD order)

- [ ] **Scaling, Delta, sync + async (fails first):** `CountingDatabase` (from #304) around a
  `DeltaTableDatabase(tmp_path)`; store Arrow bytes per row flat when rows quadruple
  (150 → 600). Deterministic.
- [ ] **Commit count (fails first):** after a 1,200-row single-pod job, each of the result
  and pipeline tables has exactly 3 commits from the node (500, 500, 200: Delta table
  versions via `DeltaTable(path).history()`), not 1,200.
- [ ] **Crash in the middle:** subprocess runs a 1,500-row job whose function calls `os._exit(1)` at
  row 1,234 (hard crash). Reopen the store: 1,000 result rows and 1,000 pipeline records
  committed (two batches). Rerun without the crash on 1,500 rows: the function runs exactly
  500 times (the 234 computed but uncommitted rows plus the 266 never reached; counted via
  a side file), and the final tables equal an uninterrupted run's (differential on stable
  columns).
- [ ] **Commit failure:** a store wrapper whose `flush` raises on its 2nd call: run raises
  `StoreWriteError` with `flush=True`, `node_label` set, `tag is None`, under both
  `error_policy` values and both orchestrators; the first 500 rows are committed and
  readable from a fresh store.
- [ ] **Row write failure unchanged:** existing PR #287 tests pass unchanged.
- [ ] **Read failure unchanged:** #290 tests pass unchanged.
- [ ] **Outside a job unchanged:** a standalone `FunctionNode` with a Delta store still
  commits per row (history shows one commit per row) — documents the boundary.
- [ ] **Lookup equivalence, Delta:** the in-memory equivalence suite parametrized over
  `DeltaTableDatabase` (committed + pending rows, many commits, scoped views sharing
  snapshots, unindexable lookups, a column missing on one side), compared with a filter over
  `get_all_records`.
- [ ] **ITL-923 regressions:** committed match while a pending batch exists; pending ids in
  `get_records_by_ids` (already in #293; keep them green).
- [ ] **Differential vs `main`:** the joined multi-node pipeline (two sources, `Join`, two
  pods, 1,200 rows) on a Delta store, sync and async; dump every table with
  `get_all_records`; identical paths, schemas, row counts, stable columns (pipeline record
  ids, entry ids, content hashes) — same script as #304's (`scratchpad/diff`).
- [ ] **Timings:** 100 / 400 / 1,000 / 5,000 rows, Delta sync + async, before/after, in the
  PR body.
- [ ] Golden hashes unchanged; full suite; ruff; basedpyright.

## Notes / progress log

- 2026-11-11: plan written; waiting for the coordinator's branch-base decision.
