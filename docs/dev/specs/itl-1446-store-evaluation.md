# ITL-1446: end-to-end evaluation of the SpiralDB store (stage 3)

Evidence for ITL-1446's goals, of the store as of PR 595 (ITL-1498, at f5b36074, after its first review): store mode of the
SpiralDB connector, private, no lease, one process. The script is
`docs/dev/scripts/itl_1446_store_evaluation.py` on PR 595's branch (it needs that code). It
was run on the fake and live, on 2026-11-26, with spiral-sdk 0.16.78. Live timings went to
the coordinator and are not kept here; the counts below are the same on the fake and live
(checked).

## The job and how it is compared

One job: a function pod (`firing_rate`), a join, a filter, a group by, a second function
(`mean_rate`) and a side effect (`notify`). Sources have key columns. The same job runs on
three stores at once: Delta, a second Delta store (the **control**), and the SpiralDB store.

Compared after each step, the SpiralDB store against Delta:

- every node's hash and pipeline hash;
- every node's rows, with system tags, source info and content hashes (sync in emitted order,
  async as sets);
- every stored table: the same paths, the same record ids, and every column value but the
  generated ones.

Two Delta stores also differ in generated values: result ids and the tokens holding them, run
ids, data ids, timestamps. So a difference counts against the store only when the control
does not show it.

## Results

| Step | Function calls | Same as Delta | SpiralDB calls |
|---|---|---|---|
| first run | 12 + 3, 3 effects | yes | 111 |
| unchanged rerun | **0**, 0 effects | yes | 19 |
| two rows added | 2 + 2, 2 effects | yes | 53 |
| function version changed (`v1.0` to `v2.0`) | 14 + 0, 3 effects | yes | 118 |
| `run_async`, a new job (as sets) | 12 + 3, 3 effects | yes | 111 |
| save on Delta, load onto the store (its config cannot be saved yet), `run(load_only=True)` | 0 | yes | 10 |
| `clear_cache` on every node, then `run` | 0 | yes | 34 |
| a separate result database (`at("results")`) | 12 + 3, 3 effects | yes | 111 |
| `trace` of a mean back to its sources | - | yes (times and run ids masked) | - |

**Failures: none of the store.** On the fake and live, every compared value equals Delta's.
Two live differences come from sharing the conformance dataset between runs, not from the
store:

- the listing also shows tables of other tests and of an earlier evaluation run, listed
  correctly;
- the second live run's separate result database (`results/...` is not scoped by job) found
  the first run's results. It called no function (`__computed` false), which is correct reuse
  across jobs.

## Where the calls go

Per step, what the store asked of SpiralDB:

| Step | Opens by name | Whole-table scans | Key scans | Listings | Creates, metadata writes, commits |
|---|---|---|---|---|---|
| first run (5 tables) | 91 | 0 | 0 | 5 | 5, 5, 5 |
| unchanged rerun | 0 | 8 | 0 | 3 | 0 |
| two rows added | 0 | 16 | 3 (whole key column) | 5 | 0, 0, 5 |
| version changed (4 new tables) | 89 | 6 | 0 | 5 | 4, 4, 4 |
| load only | 0 | 4 | 0 | 2 | 0 |

What this shows:

1. **Absent tables are opened again and again.** On a first run each new table is opened by
   name about 18 times before it exists: `ConnectorArrowDatabase` asks `has_table` at every
   add, check and flush, and a negative answer is never kept (by decision: never cache a
   negative lookup). That is about 80% of a first run's calls.
2. **Every read of a stored table is a whole-table scan.** `get_all_records`,
   `get_record_by_id`, `get_records_by_ids` and the lookups of a function node read the whole
   table and filter in Arrow. `skip_existing` reads the whole key column. A rerun with no work
   to do is 8 such scans; with rows added, 16. Their cost grows with the table.
3. **Each run lists the dataset several times** (2 to 5): `find_stale_tables` and
   `list_table_paths` (ITL-954), through the deprecated listing endpoint.
4. Writes are cheap in count: one commit per table per run.

## What comes next (recommendation)

- **A keyed-read stage**: reads by key range in place of whole-table scans, for a node's own
  inputs and for `skip_existing` (measured: a key-range scan returns no row for an absent key).
  This is the cost that grows with the data.
- **Fewer existence checks**: one positive or negative existence answer per table and run,
  dropped when the run ends. This needs a decision, because a negative lookup is never
  cached today. It is safe under a one-writer lease (stage 4); without the lease, only within
  one flush.
- **One listing per run** for the stale-layout checks.

## Not covered

Two processes (no lease yet), and a large table (the job is small).

## After ITL-1502 (PR 599): keyed reads, one existence answer and one listing per run

The same evaluation on PR 599, on the fake and live: every compared value still equals Delta's.
SpiralDB calls per step (the same on the fake and live):

| Step | Stage 3 | After ITL-1502 |
|---|---|---|
| first run | 111 | 21 |
| unchanged rerun | 19 | 14 |
| two rows added | 53 | 30 |
| function version changed | 118 | 24 |
| `run_async`, a new job | 111 | 21 |
| load only | 10 | 9 |
| `clear_cache`, then run | 34 | 27 |
| a separate result database | 127 | 45 |

- Each absent table is opened by name once per run, the dataset is listed once per run, and
  each table is opened by id once per run.
- Lookups read only their keys (key ranges) or their pushed-down equality.
- What remains warm is `get_all_records`, a read of every row of a table by definition.
