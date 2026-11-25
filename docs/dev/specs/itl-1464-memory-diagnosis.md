# Where the memory goes (ITL-1464, phase 1: diagnosis)

Status: measured on `origin/main` 0d1e017c, November 25, 2026. The design that follows from
it is `itl-1464-streaming-design.md`.

## Summary

Three causes account for the memory, in this order of size:

1. **Rows held as `(Tag, Data)` objects cost 18 to 110 KiB each.** A row of the measured
   table is 120 bytes of Arrow data. The async orchestrator passes every row between nodes
   as a pair of datagram objects, and every operator job node keeps every pair of its
   output until the run ends. One async job over 200,000 rows peaks at 19.7 GiB
   (102 KiB per row, 877 times the input); the cost is linear in rows, so the full
   session (about 2.2 million rows) extrapolates to about 210 GiB. This is the size of the reported notebook run.
2. **Source-info tokens are 35 times the data.** A source writes one string per row and
   data column, `<source id>::<record id>::<column>`, about 324 bytes each here. With 13
   data columns (every key column is a data-tag, so it has a token too) that is 4.2 KiB
   per row against 120 bytes of data. A source builds them on every read, also for a
   caller that drops them at once: `source.as_table()` on the full session allocates
   9.3 GiB of Arrow memory to return a 282 MiB table. This is the 18 GB of
   `run()` followed by `as_table()` (the live table has longer ids than the synthetic one).
3. **Every sync operator holds its whole input and its whole output at once, tokens
   included**, and a job node keeps its output. `drop_nulls` peaks at 2 times the
   tokenized table (19.7 GiB), `group_by` at 20.9 GiB, and a join, which adds the other
   input's tokens to every output row, passes 24 GiB.

`run()` returning a view over `as_table()` (the issue's starting point) is real but is the
smallest of these: the view of a bare source holds 60 MiB here. The table it is built
from is not the problem; what is built on the way to it is.

Nothing was measured live. The synthetic tables reproduce the reported sizes closely
enough that no SpiralDB read was needed.

## Method

- Scripts: `docs/dev/specs/itl-1464/measure.py` (one step), `run_measurements.py` (every
  step, each in a fresh process, killed above 24 GiB of resident memory).
- Tables: synthetic, with the column names and Arrow types of three tables of a SpiralDB
  sample dataset, one session each:
  - a time series: 13 columns (5 key columns: two strings, a timestamp, a `uint32`,
    a `uint64`; 8 value columns: two short strings, two `float`, three `double`, a
    `bool`), 120 bytes per row;
  - an event table: 8 columns (7 key columns), 79 bytes per row;
  - a small lookup table: 14 columns (9 key columns), a few hundred rows.
- They are stored in the test suite's disk-backed fake of the SpiralDB client
  (`tests/test_databases/fake_spiral_sdk.py`), so the real `SpiralDBTableSource` reads
  them. The fake memory-maps its file, so the rows it hands over are not process memory
  until copied. No `where` is applied: the stored table is the one session.
- Per step: Arrow bytes in (`Table.nbytes` of the stored rows), Arrow bytes of the result,
  peak resident memory of the process (`ru_maxrss`; 0.14 GiB of it is the import of
  Orcapod), peak of the Arrow memory pool, and, in separate runs, Python's own
  allocations (`tracemalloc`).
- Machine: macOS, Apple silicon, Python 3.13, pyarrow and polars as locked.
- The synthetic ids are shorter than live ones (a 23-character project id, a
  32-character hash key), so a live token is longer and the live numbers are higher.

## The sync path, outside a job (about 2.2 million rows)

"Ratio" is peak resident memory, less the import baseline, over the input's Arrow bytes.

| Step | Input | Result | Peak RSS | Arrow pool peak | Ratio | Why |
|---|---|---|---|---|---|---|
| The rows alone, copied once | 248 MiB | 248 MiB | 0.63 GiB | 0.24 GiB | 2 | the floor |
| `source.as_table()` | 248 MiB | 282 MiB | 10.8 GiB | 9.3 GiB | 44 | tokens for 13 columns built per batch, kept until the batches are joined, then dropped |
| `source.run()` | 248 MiB | (view) | 10.8 GiB | 9.3 GiB | 44 | is `as_table()`; the view keeps 60 MiB |
| `run()` then `as_table()` | 248 MiB | 282 MiB | 12.5 GiB | 9.4 GiB | 51 | the source holds no rows, so the table is read and tokenized twice |
| `source.as_polars_df()` | 248 MiB | 215 MiB | 10.9 GiB | 9.3 GiB | 44 | is `as_table()` |
| `for tag, data in source` | 248 MiB | none | 1.4 GiB | 0.07 GiB | 5 | one batch (8,192 rows) held at a time; the growth is the set of keys seen (below); 88 s |
| `source.map_data(...)` | 248 MiB | 282 MiB | 10.8 GiB | 9.3 GiB | 44 | a rename; the cost is its input |
| `source.drop_nulls(...)` | 248 MiB | 277 MiB | 19.7 GiB | 18.6 GiB | 80 | input with tokens and a filtered copy with tokens, both held |
| `source.polars_filter(...)` | 248 MiB | 141 MiB | 16.6 GiB | 9.3 GiB | 68 | input with tokens, and every column copied into Polars and back |
| `source.group_by(key)` | 248 MiB | 143 MiB | 20.9 GiB | 15.9 GiB | 85 | input with tokens, the sorted copy, and a token list per group and column |
| `events.join(lookup)` | 164 MiB | - | killed, above 24 GiB | - | above 150 | at 200,000 rows: 2.96 GiB, ratio 191. Each output row carries the tokens of both inputs: 22 columns |
| `drop_nulls`, `group_by`, `map_data` | 248 MiB | - | killed, above 24 GiB | - | above 100 | at 200,000 rows: 2.89 GiB, ratio 123 |
| the same, then a function pod | 248 MiB | - | killed, above 24 GiB | - | above 100 | at 200,000 rows: 3.05 GiB, ratio 131 |
| `source.flow()` | 248 MiB | - | killed, above 24 GiB | - | above 100 | at 200,000 rows: 3.58 GiB, 18 KiB per row |
| `source.as_table(all_info=True)` | 248 MiB | - | killed, above 24 GiB | - | above 100 | at 200,000 rows: 5.33 GiB; the content hash column is computed from one datagram per row |

Every step scales linearly: the 200,000-row runs give the same ratios (44 to 45 for
`as_table()`, 81 for `drop_nulls`, 89 for `group_by`).

### What a tokenized table is made of

`source.as_table(all_info=True)` at 200,000 rows, 911 MiB of Arrow data:

| Column group | Bytes | Share |
|---|---|---|
| Tag and data columns (the rows) | 26 MiB | 3% |
| Source-info tokens, 13 columns | 805 MiB | 88% |
| System tags (source id, record id), data context, content hash | 81 MiB | 9% |

A token repeats, for every row and column, the source id (constant over the table) and
the record id (the canonical JSON of the row's key, itself a copy of the key columns with
their names). Everything in a source's token is in the row's two system tags plus the
column name: the 805 MiB are derivable from the 81 MiB.

### What a held row costs

`source.flow()` at 200,000 rows: 3.58 GiB, 18 KiB per row. `tracemalloc` sees 1.75 GiB of
Python objects (8.7 KiB per row; the largest part is the per-row split of the table into
one-row tables, `tag_data.py`), and the rest is Arrow's own objects outside its buffer
pool: a one-row table of 26 columns has a schema, fields and array objects of its own.
The Arrow pool holds 80 MiB throughout: the data is not the cost, the per-row structure
is.

Three places hold every row this way today:

- `flow()`, and any read that iterates a stream and keeps the pairs;
- `ArrowTableStream.iter_data()`, which keeps every pair it yielded
  (`_cached_elements`) for the life of the stream, so iterating an in-memory stream once
  costs the same as `flow()`;
- the async path (next section).

### The keys a source remembers

`SpiralDBTableSource` keeps the record id of every row it has read during one read, to
refuse a repeated key. At about 2.2 million rows that is a Python set of about 130-character
strings: roughly 0.5 GiB, most of the 1.4 GiB of plain iteration.

## Inside a job: sync and async

The same step as the only node of a job, by both orchestrators, at 20,000 rows (2.3 MiB
of input; the join's large input is 1.5 MiB). "Per row" is peak resident memory less the
import baseline, over the rows.

| Job | Sync peak | Sync per row | Async peak | Async per row | Async over sync |
|---|---|---|---|---|---|
| `map_data` | 0.27 GiB | 6.9 KiB | 1.17 GiB | 54 KiB | 7.8 |
| `drop_nulls` | 0.35 GiB | 11.1 KiB | 1.87 GiB | 90 KiB | 8.1 |
| `polars_filter` | 0.35 GiB | 10.8 KiB | 2.23 GiB | 109 KiB | 10.1 |
| `join` (large with small) | 0.55 GiB | 21.3 KiB | 1.45 GiB | 68 KiB | 3.2 |
| `group_by` | 0.37 GiB | 12.1 KiB | 1.23 GiB | 57 KiB | 4.7 |
| `drop_nulls`, `group_by`, `map_data`, function pod | 0.56 GiB | 21.7 KiB | 2.18 GiB | 107 KiB | 4.9 |

The last job at larger sizes:

| Rows | Sync peak | Sync per row | Async peak | Async per row | Async time |
|---|---|---|---|---|---|
| 20,000 | 0.56 GiB | 21.7 KiB | 2.18 GiB | 107 KiB | 13 s |
| 50,000 | 1.09 GiB | 19.8 KiB | 5.05 GiB | 103 KiB | 32 s |
| 100,000 | 1.99 GiB | 19.4 KiB | 9.98 GiB | 103 KiB | 63 s |
| 200,000 | 3.81 GiB | 19.2 KiB | 19.67 GiB | 102 KiB | 127 s |
| about 2.2 million | killed, above 24 GiB (41 GiB extrapolated) | | not run (207 GiB extrapolated) | | (23 min extrapolated) |

### Does the async path stream today?

It moves rows one at a time, but it does not bound memory: every node gathers.

- **Transport.** A channel carries `(Tag, Data)` pairs, one row each, 64 buffered
  (`AsyncPipelineOrchestrator(buffer_size=64)`). A source node sends
  `async_iter_data()` row by row, so a source does not gather. The record-batch channels
  of ITL-1347 exist (`InputChannel`, `OutputChannel`, `async_execute_batches`, the
  streaming join kernels) but no node or orchestrator uses them yet.
- **Operators that pass rows on as they arrive**: the column selections, the mappers,
  `DropNulls`, `Filter`, `AddTags`, `Pick`, `Index`, `SemiJoin` (after its right
  input is read), and `Join` (a symmetric hash join: it keeps the rows of both inputs
  until the other closes).
- **Operators that gather their whole input** (`StaticOutputOperatorPod.async_execute`,
  the barrier: `ch.collect()` on every input, then the sync `static_process`, then one
  row at a time out): `PolarsFilter`, `GroupBy`, `TagBy`, `MergeJoin`, `Rejoin`, `Batch`
  without a size. `PolarsFilter` is a row-by-row operation that gathers only because it
  has no async form of its own; it is the most expensive single step measured.
- **Every operator job node gathers its whole output**, whatever its operator does:
  `OperatorNode.async_execute` appends every pair it forwards to a list (`collected`)
  and, when the run ends, rebuilds one table from the list to keep on the node
  (`_cached_output_stream`), because a job node shows its current run (ITL-1095). This
  is why a pure rename costs 54 KiB per row on the async path: the rows are held twice,
  once as objects and once as the rebuilt table.
- **A function job node** computes one row at a time and writes its results to the
  database, but it too keeps its run's rows.

So async is the more expensive path today, by 3 to 10 times, and the slower one (127 s
against 5 s at 200,000 rows). In theory it is the path that can bound memory; in
practice it has the same gathering as sync, at the per-row object price.

## What each cause is worth

| Cause | Where | Size at 2.2 million rows | What removes it |
|---|---|---|---|
| Rows as objects, kept by every async node | async transport, `OperatorNode.async_execute`, `iter_data` cache, `flow()` | about 100 KiB per row and node chain: 200 GiB | record batches between nodes (ITL-1347 wiring); a node keeps a reference to stored or spilled rows, not the rows |
| Source-info tokens | every source read, every operator's input and output | 4.2 KiB per row per tokenized copy: 9 GiB per copy, 2 to 3 copies per operator | not building them where nothing reads them (prototype, below); deriving a source's tokens from its system tags until an operator changes them (design) |
| Whole input plus whole output per operator | every sync operator, async barriers | 2 to 3 tokenized copies | batch-wise execution for row-by-row operators; a stated bound for the others (design) |
| A source read twice | `run()` then `as_table()` on a source that holds no rows | one more read | a view that does not need the table (design) |
| Keys remembered by a source | `SpiralDBTableSource` | 0.5 GiB | trusting a key the store enforces (developer's decision, in the design) |

## The prototype's effect (phase 3)

A `SpiralDBTableSource` read whose result holds no source-info column and no content hash
(`as_table()`, `run()`, `as_polars_df()` with the default columns) no longer builds
tokens. Same rows, same table, at about 2.2 million rows:

| Step | Peak RSS before | Peak RSS after | Arrow pool peak before | after |
|---|---|---|---|---|
| `source.as_table()` | 10.8 GiB | 1.96 GiB | 9.3 GiB | 0.96 GiB |
| `source.run()` | 10.8 GiB | 1.96 GiB | 9.3 GiB | 0.96 GiB |
| `run()` then `as_table()` | 12.5 GiB | 2.62 GiB | 9.4 GiB | 1.02 GiB |
| `source.as_polars_df()` | 10.9 GiB | 2.21 GiB | 9.3 GiB | 0.96 GiB |
| `source.drop_nulls(...)` (an operator reads the tokens) | 19.7 GiB | 19.7 GiB | 18.6 GiB | 18.6 GiB |

It removes the largest cost of the reported case (a) and nothing of case (b): an
operator and a job still read the tokens. What remains of the 1.96 GiB is the system
tags (0.5 GiB of Arrow), the set of keys seen (0.5 GiB of Python strings) and the copy
made when the batches are joined.

## The first transport step's effect

Async rows kept as Arrow tables of at most 8,192 rows instead of one pair of objects per
row (the orchestrator's collectors and drain, an operator job node's kept output, a
barrier operator's gathered input and its result while sent). Async job, peak resident
memory less the import, per row; before at 20,000 rows, after at 200,000:

| Job | Before | After |
|---|---|---|
| `map_data` | 54 KiB | 7.7 KiB |
| `drop_nulls` | 90 KiB | 8.6 KiB |
| `polars_filter` | 109 KiB | 12.5 KiB |
| `join` (large with small) | 68 KiB | 21 KiB |
| `group_by` | 57 KiB | 15 KiB |
| `drop_nulls`, `group_by`, `map_data`, function pod | 102 KiB (19.7 GiB at 200,000 rows) | 24 KiB (4.8 GiB) |

The async path now costs about what the sync path does. What remains is cause 2 and
cause 3: each node keeps its tokenized table (about 4.5 KiB per row per copy), and
`Join` buffers both inputs in the row form.
