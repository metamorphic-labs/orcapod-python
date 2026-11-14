# ITL-1062: async function node calls the function more than sync for duplicate inputs

Branch `eywalker/itl-1062-async-function-node-calls-the-function`, from `origin/main` `a9b50350`.
Status: cause established, proposal sent to the coordinator (waiting before code).

## Reproduction (public interface, `InMemoryArrowDatabase`, `PipelineJob.run`)

Script: scratchpad `repro.py` / `repro2.py` (not committed). Node `f` = counting `x -> x + 1`,
node `g` = `dbl(f)`. Case A: 12 rows, tags `k = 0..11`, `x = k // 3` (4 distinct inputs, each 3
times under different tags/record ids, adjacent). Case B: exact duplicates (same tag and same
record id, via `ArrowTableSource(record_id_column=...)` with repeated ids): 6 rows, 2 distinct.

| case | max_conc | sync calls / rdb / pdb | async calls / rdb / pdb | async RunSummary (computed, reused) |
|---|---|---|---|---|
| A | 1 | 4 / 4 / 12 | 4 / 4 / 12 | (4, 8) = sync |
| A | 4 | 4 / 4 / 12 | 8-10 / 8-10 / 12 | (8, 4) vs sync (4, 8) |
| A | unlimited | 4 / 4 / 12 | 12 / 12 / 12 | (12, 0) |
| B | 1 | 2 / 2 / 2 | 2 / 2 / **6** | (2, 4) |
| B | 4 | 2 / 2 / 2 | 4-5 / 4-5 / 6 | |
| B | unlimited | 2 / 2 / 2 | 6 / 6 / 6 | (6, 0) |

Node rows (`f`, `g` as_table) are 12 / 6 on both. `f.as_source()` rows: case A 12 = 12;
case B sync 2, async 6. Same with a `ctx_arg` pod (sync delivers once per distinct input,
async 3 times), a function returning a null value (stored, deduplicated like any value), and a
raising function (sync calls it once per row, 3 times: a failure stores nothing).

## Cause

- Sync dedup: `execute` checks `_cached_output_datas[base_entry_id]` (exact duplicate:
  reported `cached=True`, no new pipeline record), else `_process_data_internal` ->
  `CachedFunctionPod.process_data` -> `ResultCache.lookup` by `(INPUT_DATA_HASH_COL,
  __data_context)`. The first row of an input stores its result (`ResultCache.store`), so a
  later row with the same input data hash hits the result store (reported `cached=False`,
  output flagged `__computed=False`, pipeline record `computed=False` pointing at the stored
  record id).
- Async: `route_inputs` checks only the snapshot `cached_by_base_entry_id` loaded at the start
  (never updated in the run), sends every other row into the pod's `async_execute`, which
  starts one task per row (semaphore = max_concurrency). Each task does `lookup` (miss), then
  `await` the function, then `store`. Rows with the same input hash whose tasks start before the
  first stores all miss and all compute; each stores its own result row (a new output record
  id). Exact duplicates also miss the in-run base-entry-id check, so each writes another
  pipeline record at recomputation index +1.
- Extra stored rows: case A, extra result rows = same values under new record ids, one per
  duplicate input row, each referenced by its own pipeline record (`computed=True`). Case B,
  plus extra pipeline records (recomputation index 1, 2) for one base entry id.
- Reads: node rows right; `as_source()` right in case A, 3x rows in case B; later runs reuse
  one (lookup takes the most recent; info log "Cache conflict resolution"); RunSummary counts
  reported computed instead of reused.
- Main since the report: #378 (ITL-1093) added `run_entry_ids`; #369, #367 did not touch the
  function node; none deduplicates. Reproduced on `a9b50350`.

## Proposal (node-level in-flight registry; see the ask)

In `FunctionJobNode.async_execute`, DB path only, one registry per call (= node and run),
keyed by the input data hash (the result store's key; data context fixed per node):

- route(row): (1) base entry id in the run's results (loaded + computed this run) -> cached
  hit, as now, `cached=True`; (2) input hash has a result stored this run -> dispatch (the
  pod's lookup hits); (3) input hash in flight -> park the row (no task, no slot); (4) else mark
  in flight, dispatch.
- On settle of the leader (record_and_forward success -> mark stored; on_data_end with no
  output; on_data_crash) pop its parked rows and route them again in arrival order: after a
  stored result all dispatch and hit; after a failure / no output the first becomes the new
  leader and computes (as sync recomputes the next row), the rest stay parked.
- The compute channel closes only when the input is exhausted and no row is parked.

## Tasks (after approval)

1. Failing tests: `tests/test_pipeline/test_async_duplicate_inputs.py` (events, no sleeps,
   `asyncio.timeout` guard inside the function).
2. Fix in `function_node.py` async DB path.
3. Changelog fragment, design_issues entry, targeted tests on 3.11 and 3.12.
4. PR (draft), CI, CodeRabbit.
