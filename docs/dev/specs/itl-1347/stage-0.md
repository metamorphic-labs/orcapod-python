# ITL-1347 stage 0: measurements (2026-11-22)

Stage 0 of `docs/dev/specs/itl-1347-job-table-transport.md`: measurement only, no library
change. The scripts are in `docs/dev/scripts/itl_1347/` (`README.md` there). The raw
records stay outside the repository.

## Setup

- **The two Orcapods**:
  - *base*: `main` 8c6fc4f5 (0.4.0rc1, what ITL-1345 profiled);
  - *job-speed*: `eywalker/job-speed` 0e15d0db, which already holds ITL-1348 (rows of a
    table built without normalizing each again), ITL-1349 (modules loaded directly, not
    through `LazyModule`) and ITL-1351 (a held node's rows counted without building them).
- **Environments**: each Orcapod was built as a wheel and installed into its own
  environment. Before measuring, the installed `pipeline/node_run.py` was checked to match
  its source (sha256).
- **Machine**: Python 3.14 on an Apple silicon Mac. One heavy process at a time; the
  1-minute load average is given with each number.
- **Workload**: a workshop-shaped pipeline on a local Delta copy. It has two sources
  (63,349 and 28,780 rows), eight operator nodes (`GroupBy` ×2, `DropNulls`, `MapData` ×2,
  `SelectDataColumns` ×2, `Join`) and two function nodes. Every edge has one successor.

## Workshop-shaped job: time

`job.run()` seconds; best of 3 for the warm runs.

| | cold | warm 1 / 2 / 3 | **warm best** | async `job.run()` with every node held | load (warm runs) |
|---|---:|---:|---:|---:|---|
| base 8c6fc4f5 | 46.39 | 44.96 / 45.97 / 44.59 | **44.59** | 5.73–5.75 | 2.19 / 1.91 / 3.22 |
| job-speed 0e15d0db | 31.45 | 29.00 / 29.70 / 30.68 | **29.00** | 0.004–0.005 | 1.76 / 2.39 / 2.05 |

Peak RSS of the whole notebook process: 5.67–5.78 GB (base), 6.19–6.28 GB (job-speed).

## Workshop-shaped job: where rows are made and unmade (warm run)

Counted by `instrument.counting()` around the `job.run()` cell.

| | base | job-speed |
|---|---:|---:|
| datagrams made in source nodes (`Tag` + `Data`) | 184,258 | 184,258 |
| datagrams made in operator nodes | 57,822 | 57,822 |
| datagrams made in function nodes | 165 | 165 |
| `_rows_to_table` on edges (`_materialize_upstream`): calls / rows / seconds | 11 / 121,041 / 11.93 | 11 / 121,041 / 11.59 |
| `_rows_to_table` for operator nodes' kept output (`_materialize_to_stream`): calls / rows / seconds | 8 / 28,911 / 3.84 | 8 / 28,911 / 3.80 |
| source node `execute`, inclusive seconds | 14.74 | 3.54 |
| operator node `execute`, inclusive seconds | 15.62 | 12.50 |
| function node `execute`, inclusive seconds | 0.25 | 0.19 |

job-speed makes the same datagrams as base: ITL-1348 made each one cheaper, not fewer. (A
first count on job-speed missed them, because ITL-1348 builds rows without
`Datagram.__init__`; `instrument.py` now counts `Datagram._rows_of_table` too.)

The 121,041 edge rows split by producer as follows:
- the two sources: 92,129 rows;
- the operator nodes: 28,911 rows, of which 28,780 come from one node (`DropNulls`);
- the function node feeding the join: 1 row.

On job-speed, `_rows_to_table` still costs about 15.4 s of the 29 s warm run: 11.6 s on
edges, 3.8 s for kept outputs.

## What this means for the staged plan

These numbers replace the spec's estimates, which came from the profile's shares:

| Stage | Estimate in the spec | From these measurements, on job-speed (29.0 s warm) |
|---|---|---|
| 1: reuse an operator node's kept table on its edges | 4–8 s | **about 2.8 s**: operator outputs are 28,911 of the 121,041 edge rows, so about 24% of the 11.6 s edge rebuild |
| 2a: sources hand on their Arrow table | (with 2b) 14–20 s | about 8.8 s of edge rebuild (92,129 source rows) plus most of the 3.5 s source `execute` |
| 2b: operators keep and hand on `F(result)` | (with 2a) | the 3.8 s kept-output rebuild plus the operators' row iteration inside their 12.5 s `execute` |
| 3: function nodes | 8–14 s | small on this workload: two function nodes, 0.2 s in `execute`, 33 rows |

Stage 1 alone is therefore worth about 10% of the warm run here. Most of the remaining gain
is in stage 2. ITL-1348's trusted row path has already removed most of the datagram cost
that the spec's estimates included.

## Same results: base against job-speed

- **Workshop-shaped job (warm run)**: every node's `as_table(all_info=True)` and every
  stored table are equal byte for byte, except columns that each run makes afresh:
  - `__record_id`, `__pod_ts` and `__data_id`;
  - the `_source_*` tokens of function outputs, which embed a result's record id.

  Floats are compared by their bits. Arrow's `equals` holds NaN unequal to itself, and the
  job has 539 NaN values per float column.
- **Fixture differential**: equal; see "Fixture differential" below.

## ITL-1357: Polars' inner-join order (report only)

- **Method**: `join_order_plugin.py` over `tests/test_core/operators/` (6,208 passed, 285
  xfailed). Every Polars inner join without `maintain_order` was recomputed with
  `maintain_order="left_right"`, and the two row orders were compared.

| Polars threads | inner joins without `maintain_order` | same order as `left_right` | different |
|---|---:|---:|---:|
| 1 | 2,912 | 2,265 | 647 |
| default | 2,912 | 2,265 | 647 |

- **Callers of the differing joins**: `join._inner_join` (shared by `Join` and `MergeJoin`)
  628, and a test's own join 19.
- **Result**: `maintain_order="left_right"` does not reproduce today's order, so setting it
  would be a result change, for its own decision (spec, decision 10). The counts are the
  same at one thread and at the default.

## Fixture differential

- **Run**: `differential.py`, base 8c6fc4f5 against job-speed 0e15d0db.
- **Fixtures**: `tutorial` (the tutorial's pipeline), and `spikes` (36,616 spike rows,
  every operator kind and a side effect).
- **Modes**: cold, warm, load-only and held, on both orchestrators, at one Polars thread and
  at the default: 8 sync and 8 async cases per fixture.

| Comparison | Result |
|---|---|
| sync job-speed against sync base, byte for byte | **equal in all 16 cases** (8 per fixture) |
| async job-speed against async base, as multisets | **equal in all 16 cases** |
| async against sync, on job-speed, as multisets | differs only by known `main` behaviours, the same on base: e3 (ITL-1062: duplicate inputs computed and stored again, with the summary's counts) and e8 (the async side-effect node's `__computed` column) |

The run found two properties of the harness's inputs that are not results, and the
comparison now accounts for both. They were applied by re-comparing the same dumps
(`--compare-only`), with no new run:

- `__pf_var_git_hash` (a function's git state) depends on the checkout, which changed
  (`-dirty` to `-dirty-untracked`) when a file was added during the run. It is now an
  environment column, left out like the volatile ones.
- `main` starts independent nodes (two sources, say) in an order that varies between two
  runs of the same code. So observer events are compared node by node, each node's own
  sequence in order, not as one global sequence.
