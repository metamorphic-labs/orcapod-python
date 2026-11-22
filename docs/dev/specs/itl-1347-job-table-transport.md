# Job nodes pass tables, not rows (ITL-1347)

Status: design for the developer's review. No code changes with it. Written against
`origin/main` 8c6fc4f5 (2026-11-22). Every implementation stage below becomes its own
issue after approval.

## The rule

Developer, 2026-11-22: Orcapod was always meant to keep data as Arrow (or Polars) tables
as long as possible and to avoid converting rows to objects and back. This spec makes that
the rule for the job layer as a whole. It is not a special case for one node kind.

- **The unit that moves between job nodes is a table.** In the sync orchestrator it is one
  table per node. In the async orchestrator it is a record batch per channel item.
- **A row exists only where a user's function is called**: a function pod's function, or
  a side effect's delivery. Even there, the node reads its input as a table and writes its
  results as a table. Only the call itself takes one row's values.
- **Observers and the error policy stay per row.** They are told about each row, and a
  failing row is skipped or raised, as today. The transport underneath is not rows.

## Why the job layer is per-row today

The ITL-1347 comment holds the history:

- **March 2026, PLT-922 (the orchestrators).** The node execution protocol was defined
  per row:
  - `execute()` returns `list[tuple[Tag, Data]]`, and `async_execute` reads and writes
    channels of `(Tag, Data)`.
  - The sync orchestrator keeps a buffer of row tuples per node and rebuilds a stream for
    each successor (`_materialize_as_stream`).
  - Each operator node turns its table result back into rows.

  This served the async orchestrator's per-row streaming, concurrent function calls,
  per-row observer hooks and the per-row error policy. Plain streams (operators on
  `ArrowTableStream`s) never adopted it.
- **November 2026: correctness fixes that kept the row protocol and made each row
  costlier.**
  - ITL-845: `_rows_to_table` keeps each row's source info.
  - ITL-1122: large-type normalization at stream construction, now paid three times per
    row datagram.
  - The rule that async output rows equal the sync rows, built from each datagram's Arrow
    table.

## What it costs

The ITL-1345 profile (workshop-scale notebook on 0.4.0rc1, local Delta copy, no network):

| Measure | Value |
|---|---|
| Same pipeline as plain streams | 1.9 s |
| As a `PipelineJob`, warm / cold `job.run()` | 44.7 s / 46.8 s, identical rows |
| Warm run: building a `(Tag, Data)` datagram per row handed between job nodes | ~58% (~26 s) |
| Warm run: rebuilding tables from those rows (`_rows_to_table`, `_materialize_as_stream`) | ~32% (~14 s) |
| Warm run: operator compute / reads / all hashing | ~8% / ~1% / ~0.3% |
| `Data.__init__` calls; `normalize_schema_to_large_types` | 121,139; 363,466 (three per row) |
| One source of 63,349 rows: `as_table()` / `list(iter_data())` | 0.29 s / 9.1 s |

About 90% of a warm run converts tables into rows and back. A warm run in a fresh process
holds no node, so every node runs. Every row then goes through the conversions below even
though every function result is already stored.

### Where rows are made and unmade today

Sync orchestrator (`SyncPipelineOrchestrator.run`):

1. **Source job node** (`SourceNodeBase.execute`): `list(self.iter_data())`, one datagram
   per row. A narrower source given in the declared form gets a second datagram per row
   (`_declared_row`).
2. **Every edge** (`_materialize_as_stream` → `StaticOutputOperatorPod._materialize_upstream`):
   the successor's input is `R(rows)` (`_rows_to_table`: one `Tag.as_table` and one
   `Data.as_table` per row, a concatenation, an `hstack`), wrapped as the predecessor's
   stream by `_as_upstream_stream`. A node with *k* successors is rebuilt *k* times.
3. **Operator job node** (`OperatorJobNode.execute`): the operator computes a table. Then
   `list(result.iter_data())` makes rows of it, and `_kept_output(result, rows)` makes a
   table of those rows again (`R`) to keep and store.
4. **Function job node** (`FunctionJobNode.execute`):
   - It iterates `input_stream.iter_data()`: a datagram per input row, from a table that
     step 2 just built from rows.
   - Stored hits are loaded by `_load_cached_entries` as one datagram per stored row.
   - Each result is a `Data` built from the returned values.
5. **Side-effect job node**: iterates its input's rows and passes them on.
6. **Held nodes** (`node_run.plan_run`): `list(node.iter_data())` for every held node, and
   `sum(1 for _ in node.iter_data())` again to count its rows.

The async orchestrator does the same with channels of rows. A barrier operator rebuilds
each input with `R` (`_materialize_input_stream`), and an operator node rebuilds its
collected output with `R` (`_materialize_to_stream`).

## Definitions

- **R(rows)**: `StaticOutputOperatorPod._rows_to_table(rows)`, the table today's code builds
  from rows. It stays in the code base as the oracle of the proofs below.
- **Row-built form F**: the one function that turns a node's table into `R` of that
  table's rows, without making the rows. It applies, in this order:
  1. Drop the context, meta (`__`) and `_content_hash` columns. A row's `Data.as_table`
     leaves them out, and the context column is added back by `_as_upstream_stream`.
  2. Drop the data-side copy of each data-tag. A data-tag is one column, laid out among
     the tags.
  3. Map `null` to `large_string`. A datagram normalizes its table without `keep_null`
     (`Datagram._init_from_table`), so today every row path does this (ITL-1319 is the
     separate decision on whether it should).
  4. Place the columns in row order: user tags, system tags, data, source info
     (`_in_row_order`, ITL-1131).

  `F` reuses the existing helpers and is the only place the rule is written. It does not
  set source-info nullability, which is a property of a whole table (see "Source-info
  nullability" below).
- **Node table**: what a sync node passes on. It is `F` of its output, with the
  source-info flags applied: the table, its user tag keys, its data-tags and its data
  context key. One internal class, `pipeline/node_table.py`, never exported.
- **Batch**: what an async channel carries. It has the same fields as a node table, over a
  slice of rows. It holds a `pa.RecordBatch` or a small `pa.Table`.

## The node protocol

| | Today | Under this design |
|---|---|---|
| Sync `execute` returns | `list[(Tag, Data)]` | a node table |
| Sync orchestrator buffer | `list[(Tag, Data)]` per node | a node table per node |
| Successor input | `_as_upstream_stream(R(rows), upstream)` | `_as_upstream_stream(node_table.table, upstream)` |
| Async channel item | `(Tag, Data)` | a batch |
| Async `async_execute` | reads and writes row channels | reads and writes batch channels |
| Held node, for its successors | `list(node.iter_data())` | the node's node table |
| Held node, row count | iterate the rows | `num_rows`, or the node's count of run entries |

`_as_upstream_stream` keeps its job unchanged: the predecessor's producer, upstreams,
pipeline hash and data-context column, and `_RowsOfStream` for a node without a producer.
A node table with no rows takes today's zero-row path (`_materialize_upstream([], upstream)`,
typed by `_zero_row_input`, ITL-912). A fan-out wraps one table for every successor.
`materialize_results=False` drops a node table once every successor ran (`_gc_buffers`).

`OrchestratorResult.node_outputs` is public: `SyncPipelineOrchestrator` is exported. Today
it holds rows, and `job.run()` discards it. The proposal is to hold node tables there. A
caller who wants rows calls `ArrowTableStream(...).iter_data()` on one, as for any table.
This is an API change, so the developer decides (question 1).

## Each node kind

### Source job node

- **Sync.** `execute` passes on `F(declared(bound.as_table(all columns)))`, with the
  source-info flags applied. `declared` is `_DeclaredForm.apply`, which already serves a
  whole table. No row is made.
- **Async.** The node sends the same table as batches: one batch for a table read, or one
  batch per poll for a `PollingSource`, which keeps streaming.
- **Unchanged.** A `PollingSource` polls exactly as often as today. A run checks its
  sources for new data first (ITL-1102), as today.

### Operator job node

- **Sync.** The node calls `operator.process(*inputs)`, then keeps
  `F(result.as_table(row columns))` with the source-info flags applied. That table is both
  `_cached_output_stream` and what the node passes on. Today `_kept_output` builds it from
  rows.
  - `_store_output_stream` stores the same kept table, so the stored table is unchanged.
  - REPLAY passes on `F` of the stored stream.
  - With no rows, the node keeps today's form (`_zero_rows_in_row_order`, ITL-1131).
- **Async.**
  - A barrier operator (the default `StaticOutputOperatorPod.async_execute`) concatenates
    each input's batches into the input table. This replaces `_materialize_input_stream`
    over rows. It then calls `static_process` and sends the result as batches.
  - The operators that override `async_execute` (`AddTags`, `DropNulls`, `Batch`,
    `Select`/`Drop` columns, `Join`, `Index`, `Pick`, the mappers, `SemiJoin`, `Rejoin`)
    each get a batch form. A per-row operator (a select, a map, `AddTags`, `DropNulls`)
    applies its table rule to each batch as it arrives. An operator that needs all its
    input (`Batch`, the joins) collects batches.
  - The operator node keeps the concatenation of the batches it sent, and stores it in
    `LOG` mode.
- Operators have no per-row error policy: a failure fails the node, as today.

### Function job node

The function is still called once per row. Everything around the call is a table.

1. **Input as a table.** The node receives its input table, or batches when async. For
   each input row it computes the base entry id from that row's slice of the table: the
   tag side with system tags, plus the input data hash. That is the same one-row preimage
   `_build_record_id_preimage` builds today, so the hash is per row, as today (hashing is
   ~0.3% of the run). No datagram is made.
2. **Lookup as a table.** `get_cached_results` finds the same entries. The loader keeps the
   stored joined table and a map from base entry id to row, instead of a datagram per
   stored row. Hits are taken from that table by row index.
3. **The call.** For each miss, the node takes the parameters' values from the input table
   and calls the function. The values are converted to Python once per column, per table
   or batch, through the data context's type converter, the conversion `Data` applies per
   row today. The node then calls `_process_data_internal`'s logic with the same:
   - logger and invocation context;
   - error policy;
   - commit cadence (`commit_every_rows`, `_row_processed`);
   - database error handling.
4. **Results as a table.** The returned values are collected per output column and
   converted to Arrow once, under the declared output Arrow schema. The per-row items stay
   per row: the output data hash, the source-info tokens and the result's datagram id
   (`uuid7`, the stored `__data_id`). Pipeline records and result rows are written as
   tables per commit window, with today's rows, columns and values. Rows written before a
   failure stay written, as today.
5. **Output.** The node's output table is in input order. It is assembled from the hit
   rows and the computed rows, by row index. It reproduces three details of today:
   - a `None` result or a failed row gives no row;
   - two equal inputs give two rows;
   - the column order is the first output row's, which differs between a stored row and a
     fresh one today (`concat_tables_by_name`). Settling that order is question 2.

   Async keeps today's order: stored hits first, then computed rows as they finish. Async
   equals sync as a set of rows (see "Async equals sync" below).
6. **Load-only** (`_execute_load_only`) is the hits-only case: the same lookup, the same
   `not_stored` count, no call and no write.
7. **The node's own reads** (`iter_data`, `as_table`, `head`) read the run's rows from the
   node's tables instead of `_cached_output_datas` pairs. Today they list rows in the order
   the cache holds them (`_held_pairs`), which can differ from the input order a successor
   receives. Keeping or unifying that order is question 3.
8. **Stored values decoded as a class.** A `TypeIdentityError` raised when a stored value
   is decoded as its class (ITL-881) must stop the run at the same point as today. Stage 4
   first establishes where it is raised today: at datagram construction, or at first
   Python access. If at construction, the node checks the identity of each user-class
   column of the stored table once, before passing a row on.

### Side-effect job node

- **The call.** Delivery calls the user's function with one row's values, taken from the
  table as for a function node.
- **What it passes on.** The input table restricted to the rows passed on: a mask over row
  indices, not a rebuild of the delivered rows.
- **Load-only.** The node passes its input table on unchanged.
- The node's held rows become its held table.

### Held nodes and the plan

- `plan_run` takes each held node's node table.
- It counts rows without making them:
  - an operator: `num_rows` of the kept table;
  - a function node: its run entry counts;
  - a side-effect node: the held table's `num_rows`.
- `RunSummary.held` stays identical, and the plan chooses the same nodes.

## Per-row concerns without a row transport

### Observers

The protocol keeps its per-row hooks and its event order:
- `on_data_start`, `on_data_end` and `on_data_crash` fire per row of a function or
  side-effect node;
- operators and sources call only the node hooks.

What changes is what a hook receives: a **row view** (one internal class, `RowView`) that
satisfies `TagProtocol` or `DataProtocol`. It holds a table and a row index, and it builds
the datagram only when the observer first reads it.

- The run's own `RunCountingObserver` reads only the label and the `cached` flag, so it
  never makes a row.
- The status and logging observers read tag values, so they make the tag datagram of the
  rows they report.
- A user's observer gets exactly what it gets today.

Events keep today's order:
- sync: input order, hits and misses interleaved;
- async: stored hits first, then rows as they finish.

`create_data_logger` is called only around a function call, where a row exists anyway.

The async function node now finds a row's input again through a `_TAG_NODE_INPUT_REF`
meta column on the tag (`_NodeLabelObserver`). It will match by row index within the
batch instead, so no extra column is added to and dropped from each row.

### Error policy

A row can fail only in a user's function or a delivery, which are already per row. The
node wraps each call:
- `continue`: the failed row is left out of the output by mask, counted, and reported
  through `on_data_crash` with its row view;
- `fail_fast`: the node raises at once, as today;
- database errors and `TypeIdentityError` stop the run whatever the policy, as today.

## How each rule holds under table transport

### ITL-845: each row keeps its own source info

Today `R` concatenates each row's own `_source_*` values. A node table takes the
`_source_*` columns of the node's table as they are, row by row (`F` does not touch
values). So every row carries its own tokens, also for list-valued source info after
`Batch`, `GroupBy` or `MergeJoin`. The L1, L2 and L4 tests below compare the tokens of
every row with `R`'s.

**Source-info nullability.** `R` declares a `_source_*` field nullable only when some row's
token is null (`_flag_source_info_nullability`). That is a property of a whole table. It is
applied where today's code builds a whole table from rows:
- the sync node table;
- the kept output of an operator node, sync and async;
- the input table a barrier operator assembles;
- a function node's output table.

It is never applied per batch. Async batches carry the fields as nullable, and the flag is
set once on the assembled table. A per-batch flag would give two batches of one channel
two schemas, and `concat_tables` would refuse them.

### ITL-1122: large types

A table is normalized to large types once, where it becomes a stream
(`ArrowTableStream.__init__`). Every node table and batch is taken from such a table by
selecting columns, slicing or `take`, which keep types. So nothing is normalized per row,
and nothing is normalized again. The three normalizations per datagram go away with the
datagrams. A function node's results are converted under the declared output Arrow schema,
which is already large (the converter's types). Step 3 of `F` keeps the one observable
effect of today's per-row normalization: a `null` column becomes `large_string`.

### Async equals sync

The rule is that async gives the same rows as sync: values, types, nullability, source
info, system tags, content hashes, meta. `test_async_sync_equivalence` compares them as a
set of rows by tag key. Under this design both orchestrators run one logic per node kind:
- the sync node applies it to its table;
- the async node applies it to each batch, for a per-row step;
- or the async node applies it to the assembled table, for a step that needs all its
  input.

So the async rows are the sync rows by construction. The equivalence test stays the check.
It covers every operator, function node and side-effect node, and it gains a case per
batch layout: one batch, one row per batch, batches of mixed sizes.

### ITL-1133 (and ITL-1131): one column order

`F` places every node table's columns in row order. An operator node keeps and stores `F`
of its result, sync and async, so it has one column order in memory and in its stored
table, as `_kept_output` gives today. A zero-row result keeps `_zero_rows_in_row_order`.
The ITL-1131/ITL-1133 column-order tests stay unchanged.

### Row order

Each node passes on its rows in the order its successors receive them today:
- a source: its table order;
- an operator: its result order;
- a function node that ran (sync): input order;
- a held function node: the order it holds them in;
- a side-effect node: delivery order;
- async: as today.

Row order is never re-derived.

### Staleness

Every node calls `_update_modified_time` at the same points and in the same order as
today, so `is_stale` and the next run's hold decision are unchanged. Today an operator's
`iter_data()` runs a lazy `DynamicPodStream`, and its inputs stamp themselves when they
finish. `as_table()` triggers the same computation, so stamps happen in the same order.
Each stage runs the ITL-1095 and ITL-1121 staleness tests.

### Run summary

The counts come from the same three places:
- the counting observer (computed, cached, failed), which still sees one event per row;
- `last_run_not_stored`;
- the plan's held counts, now read without making rows.

Every `RunSummary` field is compared in the differential run (below).

## What must stay byte-identical, and how it is proven

### Must not change

1. **Every hash.** `generate_golden_hashes.py --check` matches. Per node:
   - content hash and pipeline hash;
   - record ids;
   - base entry ids and pipeline entry ids;
   - output data hashes;
   - source-info tokens;
   - system-tag column names and values;
   - folded values (`batch_v1:` / `group_v1:`).
2. **Every stored table.** This covers the function pipeline and result tables, the
   operator cache (`odb_v1`), the side-effect log (`tdb_v1`) and the cached source
   (`sdb_v1`). For each: path, column names and order, types, nullability, field metadata,
   values and the row order written.
3. **Every output a user reads.** For every node:
   - `as_table(all_info=True)` and `iter_data()` rows;
   - the `head()` and `run()` views, with their status and notes;
   - `as_source()` / `DerivedSource`.
4. **Every successor's input.** The table each operator, function node and side-effect node
   receives: columns, order, types, nullability, metadata, values and row order. Items 1–3
   follow from it for everything downstream. When it fails, it names the edge.
5. **Run behaviour.**
   - `RunSummary` and `NodeRunSummary.held`;
   - the warnings issued: `RowFailuresWarning`, `RestrictedAggregationWarning`,
     `SourceCheckFailedWarning`;
   - every error raised, with the point it is raised: database errors, `TypeIdentityError`,
     `fail_fast`, restriction refusals;
   - the observer event sequence and the values an observer reads from a row;
   - what logging capture records;
   - which nodes a second run holds.

Not covered: the Arrow chunk layout (no hash or output reads it; a table built from *n*
rows has *n* chunks today), datagram object identity, and timing.

### Proof

1. **Equality tests**, kept permanently (`tests/test_pipeline/test_node_table_equivalence.py`).
   `R` and `ArrowTableStream.iter_data` remain in the code base as the oracle, so no switch
   and no second implementation are needed. Each comparison is
   `Table.equals(check_metadata=True)` plus field-by-field schema equality (name, type,
   nullability, metadata), on combined chunks.
   - **L0 (idempotence)**: a node table `T`, passed on as its node's stream, gives
     `R(rows of that stream) == T`. A node that passes its input on (side effect,
     load-only) can then pass on its input table.
   - **L1 (operators)**: for each operator result `S`, the node table equals
     `R(list(S.iter_data()))`. This covers every exported operator, and REPLAY of a stored
     cache.
   - **L2 (sources)**: for each source kind bound to a slot, with and without a declared
     form, the node table equals `R(list(node.iter_data()))`. The kinds are in-memory,
     CSV, Delta, SQLite, `CachedSource`, `DerivedSource`, `PollingSource` (also counting its
     polls) and SpiralDB (through its existing fakes). PostgreSQL runs in its own job.
   - **L3 (entry ids)**: per input row, the id from the table slice equals
     `compute_base_entry_id(tag, data)`.
   - **L4 (function node)**: the parameter values given to the function equal (`==`, same
     types) today's `function_input(...).as_dict()` values. The results table equals `R`
     of today's result datagrams. The output table equals `R` of today's output rows,
     including the column order when the first row is a hit and when it is a miss.
   - **L5 (held)**: a held node's node table equals `R(list(node.iter_data()))`, and its
     count equals today's.
   - **L6 (batches)**: for every node kind, the concatenation of the batches it sends,
     with the source-info flags applied, equals its sync node table as a set of rows. For
     sources and operators it also equals the sync table in order.

   The corpus is the several-items family (`tests/test_core/operators/several_items.py`):
   two or more tags and data columns, in more than one arrangement and role (plain tag,
   data-tag). It adds:
   - logical types and user classes;
   - `null`-typed, list, struct and nullable columns;
   - zero rows, one row and many rows;
   - duplicate input rows;
   - a function returning `None`;
   - ephemeral `EmptyData`;
   - failed rows under both error policies;
   - a loaded node (no producer);
   - a stream outside the job (a `node.run()` graph);
   - fan-out to two successors;
   - a `null` source-info token.
2. **Existing suites**, run targeted as each stage lands:
   - `test_async_sync_equivalence`, `test_order_invariance`;
   - the operator node, function node and side-effect node tests;
   - `test_pipeline_job`, the observer and status observer tests;
   - the ITL-1131/ITL-1133 column-order tests;
   - the ITL-1095/ITL-1121 staleness and `load_only` tests;
   - the run summary tests;
   - the golden check.
3. **End-to-end differential**, a script kept in `docs/dev/scripts/itl_1347/`. It runs a
   set of fixture pipelines twice, once on a pinned `origin/main` checkout and once on the
   branch. The pipelines are built on the tutorial dataset (`docs/tutorial/workshop_data.py`,
   deterministic, no network) plus the corpus above. Each pipeline runs four ways: cold,
   warm, held, and load-only. Each way runs on both orchestrators. The script dumps every
   stored table (Arrow IPC), every node's `as_table(all_info=True)`, the `RunSummary` and
   the event sequence of a recording observer (with the tag values it read). It then
   compares them byte for byte. Its output goes on each stage's PR.

## Staged plan

Unit costs from the profile: about 26 s of datagram building over 121,139 rows (about
**0.21 ms per row**), and about 14 s of rebuilding. The profile does not split rows by node
kind or rebuilds by caller. So the savings below are estimates from those shares, and
stage 0 replaces them with measurements before stage 1 merges.

| Stage | What | Proof | Expected saving, warm sync run of 44.7 s |
|---|---|---|---|
| 0 | Measurement only. A script in `docs/dev/scripts/itl_1347/`, on a tutorial-data pipeline of the workshop's shape, counts per node kind: datagrams made, `R` calls and time by caller (kept output vs. edge), edges per node. No library change. | – | 0 s; gives the real split |
| 1 | **First small step: reuse a producer's table when its successor takes it unmodified.** An operator node already keeps `R` of exactly the rows it hands on (`_kept_output(result, output)`). The sync orchestrator wraps that kept table for each successor instead of rebuilding it from the buffer, for held operator nodes too. Held row counts are read without rows. The row buffers stay. | By construction: the same `R` of the same rows. The node keeps the raw row-built table beside its stream, so no context column enters. One test asserts the equality per edge. | The edge rebuilds of operator outputs, times fan-out: about **4–8 s** |
| 2 | Sync node protocol, sources and operators: `execute` returns node tables; sources pass on `F` of their table, operators `F` of their result; held nodes pass on node tables; `OrchestratorResult` holds node tables. Function and side-effect nodes take a node table as input and still iterate it internally. | L0, L1, L2, L5 | The datagrams of source and operator outputs and their remaining rebuilds: about **14–20 s** |
| 3 | Function nodes: table input, entry ids from slices, hits from the stored table, the call on values taken from the table, results converted per column, output table by row index; row views for observers; the node's own reads from its tables. | L3, L4, differential | Input datagrams, stored-hit datagrams and result datagrams of function nodes: about **8–14 s** |
| 4 | Side-effect nodes: masked pass-through, load-only pass-through, held table. | L0, differential | Small unless side effects sit on large streams |
| 5 | Async orchestrator: batch channels; batch forms for sources, barrier and streaming operators, function nodes and side-effect nodes; correlation by row index. | L6, `test_async_sync_equivalence`, differential | The same order of gain for `run_async` / an async `job.run` |

The ranges overlap, because one row can be made into a datagram at several nodes. The
bound for stages 1–4 is the ~40 s (~90%) the profile puts on conversions. The floor is the
plain-stream time (1.9 s) plus the stored lookups and writes a job adds. A warm sync
`job.run()` of about 3–6 s is the realistic target after stage 3.

Stage 5 is in scope by the rule: tables, sync and async. It comes after the sync stages
because the profile measured the sync orchestrator (the default of `job.run()`). Doing
sync first gives the measured gain early, and the sync node logic is then what the async
batch forms apply. Stage 1 lands in a form stage 2 replaces. It is kept because it is
small, proven by construction, and gives a measured part of the gain within a day.

Each stage is one issue and one PR. Each lands with its equality tests, the targeted
suites, the golden check, and the differential output on the PR. Benchmarks follow the
round's rules: script outside the package, best of 3, the 1-minute load average noted,
remeasured when the load is above 4.

Independent of this design: the `LazyModule` attribute cost (42.9 million `__getattr__`
calls; 45.1 s → 40.7 s in the profile's experiment) is its own fix in this round. Its gain
shrinks as fewer datagrams are made.

## Questions for the developer

1. `OrchestratorResult.node_outputs` is public and holds rows today. Should it hold node
   tables (proposed; a caller makes rows with `iter_data()` if needed), or stay rows,
   built on access?
2. A function node's output column order today follows its first output row, which
   differs between a stored row and a fresh one. Keep that exactly (proposed for stages
   1–3, so nothing moves), or open an issue to give it one order? The second option moves
   the column order of downstream stored tables, so it needs its own decision.
3. A function node's own reads list rows in cache order, while its successors receive
   input order. Keep that (proposed), or make the node's reads follow the run's input
   order in stage 3?
4. Stage order: 0 → 1 → 2 → 3 → 4 → 5 is proposed. Stage 2's sources part and operators
   part could go to two workers in parallel.

## Future work (not designed here)

- Batch the side-effect completion lookup (one `get_record_by_id` per row today).
- Whether a `null` column stays `null` through the job layer (ITL-1319).
- Polars instead of Arrow as a node table's backing, where an operator works in Polars.
