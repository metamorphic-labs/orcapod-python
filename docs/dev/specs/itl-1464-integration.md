# The ITL-1464 tracks together: batch edges, and what a whole analysis costs

## What connects the tracks

- **Edges.** An async job's edge carries `(Tag, Data)` pairs or record batches of a node's
  rows (`core/job_edges.py`, over the channels of `orcapod.channels`). A source node whose
  table is provably its rows, and an operator node, send batches of at most 8,192 rows; a
  node that knows only pairs (a function, a side effect) is given pairs, made as it reads.
- **Operator nodes** run `async_execute_batches` over the edges, when the operator and every
  operator above it are the library's own classes and everything is in the default data
  context. Otherwise they run the row form, unchanged. Each input channel's stream is the
  node's own input, so a uniqueness certificate is the node's. `Filter` is row by row.
- **Token recipes** travel between nodes on both paths; text is made where a row, a store or
  a hash needs it. A join over an aggregation's output runs on its whole inputs: a kernel's
  files cannot share the ids that derived token lists share.
- **Kept rows** (a node's output, a barrier's input, a sync source's table) go through a
  `RowSpool`; rows with derived token lists stay in memory, where they share their ids.
- One async run derives each node's zero-row table and certificate once.

## Measured

`docs/dev/specs/itl-1464/chain.py`: 8.1 million events and 2.2 million series rows at 100%,
read by the real source over the fake client. Peak physical footprint / peak resident memory
in GB, and seconds. Memory beyond the footprint is memory-mapped pages of local files.

Async:

| Step | 25% | 50% | 100% |
|---|---|---|---|
| events semi-joined with the kept items | 0.4 / 1.1, 15 s | 0.5 / 1.8, 32 s | 0.7 / 3.2, 66 s |
| then filtered by time | 0.5 / 1.6, 15 s | 0.7 / 2.9, 32 s | 0.8 / 5.3, 68 s |
| then joined with a lookup table | 0.6 / 2.8, 17 s | 0.8 / 5.2, 34 s | 1.0 / 10.0, 85 s |
| filtered events grouped by item | 2.1 / 3.5, 27 s | 3.9 / 6.8, 55 s | 4.6 / 9.4, 115 s |
| series filtered, grouped, one function | 1.4 / 2.1, 8 s | 2.6 / 3.7, 15 s | 4.5 / 8.2, 33 s |
| the whole job | 3.0 / 4.4, 38 s | 5.3 / 8.2, 78 s | 7.4 / 20.0, 164 s |

The 100% column includes the aggregation step that sorts text by rank and spools token
lists (#573); the 25% and 50% columns were measured before it. It lowers the footprint and
raises resident memory (mapped pages).

Sync:

| Step | 25% | 50% | 100% |
|---|---|---|---|
| semi-join | 1.8 / 2.6, 14 s | 3.2 / 4.6, 32 s | 6.0 / 8.9, 67 s |
| filter | 1.8 / 3.0, 15 s | 3.2 / 5.5, 33 s | 6.0 / 10.6, 69 s |
| lookup join | 2.3 / 5.0, 15 s | 4.2 / 9.3, 32 s | 7.5 / 18.6, 72 s |
| group by | 2.5 / 4.2, 25 s | 4.4 / 7.8, 55 s | 6.0 / 12.0, 115 s |
| series and function | 1.9 / 2.7, 7 s | 3.9 / 5.0, 14 s | 5.8 / 10.8, 30 s |
| the whole job | 4.8 / 7.1, 37 s | 8.7 / 13.3, 71 s | above 24 GB, killed |

`main` at 25%: the semi-join takes 10.7 / 10.9, 12 s on sync; the semi-join on async, and
the lookup join and the whole job on both paths, pass 24 GB and are killed.

Row-by-row steps are slower on sync than on `main` (12 s to 14 s here): the source's rows go
through a local file.

## What is bounded, and what is not

Bounded on async: sources, row-by-row operators, joins with a small or certified side. They
grow by the digests of the keys read (16 bytes a row), and by file pages the system can drop.
Whether a container that counts mapped pages drops them in time is not tested.

Not bounded, in the order of their cost in the whole job at 100%:

1. Writing a function node's stored rows (the peak of the whole job when profiled, before #573): a row holds the source-info text
   of its inputs' token lists, millions of tokens in one row, and the Delta writer holds
   copies. The stored layout decides the text.
2. `GroupBy` over its whole input (4.6 GB for 5.7 million member rows): the sort of its members
   and the copies of what it takes. The members' data it keeps is 64 MB; the rest is ids.
3. A function's input row that is one large group: its token text, then Python lists.
4. On sync, each operator's own whole result while it is moved to local disk.

## Gates

The ITL-1347 differential against `main` (both fixtures, all modes, one and default Polars
threads): sync exact and async as sets, all equal. At the final head: equal, and 24,560 tests pass. Every operator node's rows, system tags
and tokens in the job above, at 0.5%, have `main`'s digests on both paths.
