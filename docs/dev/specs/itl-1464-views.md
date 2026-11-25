# `run()`, its view and `preview(n)` without a full table (ITL-1464, track VIEWS)

Status: V1 to V12 approved by the coordinator, November 25, 2026 (V2, V3 and V8 flagged for
the developer); the bounded view and `preview(n)` are implemented. Written, against `origin/main` 0d1e017c. Companion of
`itl-1464-streaming-design.md` (sections 1, 3 and 4 of it). Nothing here changes a hash,
identity, stored layout, stored byte or golden value.

## Scope

- Now: the bounded view of a plain stream (outside a job). Files: `core/streams/base.py`,
  `core/streams/view.py`, with small hooks in `DynamicPodStream` and
  `SpiralDBTableSource`.
- With TRANSPORT: a job node's view, which pages from the node's reference to its rows.
- After approval of its decisions: `preview(n)` (designed here, not in the first PR).

Outside a job, only `SpiralDBTableSource` reads its rows on demand and holds none. Every
other source holds its table, so a table over those rows is not a new cost (a filter's
result over it is, and stays as today). A function pod's stream keeps its results, so
that no function is called twice: it stays as today. So the first step applies to a
**batched chain**: zero or more operators that declare `BATCHES_GIVE_THE_WHOLE` (the column
selections, the mappers, `AddTags`, `DropNulls`, `Filter`) over a source that reads in
batches. `Pick` and `Index` are row by row but excluded: they build their result from rows,
so its types and nullable flags follow the values of the batch. The flag is read from the
class itself, so a subclass declares it again. Joins, `GroupBy`, `Batch`, `PolarsFilter` and function pods
end the chain: they compute as today.

## The `run()` contract

`run()` computes every row, with every effect, in the order of `as_table()`, and returns a
`StreamView`. It promises nothing about how rows are produced or held. For a batched chain:

1. The source's batches are read in order (one scan, its checks unchanged: `max_rows`,
   schema, repeated keys). Each batch, shaped as `as_table()` shapes the whole, goes
   through each operator's own `static_process`: the one sync body, no second kernel.
2. Result batches are kept whole up to **50,000 rows and 64 MiB of values** (default columns; offsets,
   validity and chunks not counted, so equal data is treated alike however its batches are
   cut). A result that ends
   within it is complete: the view holds it, and an operator stream keeps it, exactly as
   today.
3. A larger result drops the kept batches to a **window**: the first and last 1,000 rows
   (default columns, copied, so no batch stays alive behind a slice), the row count and
   the schema. The operator stream keeps the window, not the rows.

`as_table()`, iteration and conversion are unchanged and still give every row. After a
large `run()` of a batched chain, `as_table()` reads the source again and applies the
operators again. No user function runs twice: a function pod is never in a batched chain.
An error in any batch propagates and nothing is kept, as today. When an operator raises, the
rest of the source is read first: an error of the source, which a whole read raises before
the operator runs, still comes first.

## The view

A *complete* view holds every row and is today's view, unchanged. A *bounded* view holds
the window.

| Member | Bounded view |
|---|---|
| `total_rows`, schema, `status`, `header()` | as the complete view: `all 1,234,567 rows` |
| `head(n)`, `tail(n)`, `n` within the window | the same rows and header as the complete view; the result is an ordinary view |
| `head(n)`, `tail(n)`, larger `n` | raise `BoundedViewError`, naming the window and `stream.as_table()` |
| `sort_by_tags()` | raise `BoundedViewError`: sorting needs every row |
| `as_table()`, `as_polars_df()`, `table`, `len()` | `len()` is the row count; the others raise, never a part presented as the whole |
| display | identical to the complete view's: header, legend, shape line, first and last five rows |

A view that has no cut of its own and is refused tells the caller the one call that does
it. Nothing a view does reads or computes (as today).

`stream.head(n)` after a large run of an operator stream shows the window's first rows
(`n` within the window, default columns) while the inputs are unchanged. The preview, which
asks for tag order, sorts those and says so: the present rule of a stream that has only
some of its rows. Another column group shows `not_computed`, as before a run.

### Inside a job

The same class. The window of a job node's view is filled by one pass over the node's
reference (TRANSPORT's local cache or the stored entries), or, once a reference can be read
by position, `head`, `tail` and `sort_by_tags` page from it and nothing is refused: the
refusals above are the meaning of "rows not held and not addressable". This needs two
operations from TRANSPORT: `row_count` and `read(start, length, columns)`.

## `preview(n)`

`stream.preview(n=10)` computes what the first `n` rows need and returns a `StreamView`
with status `PARTIAL` (or `ROWS`, when the stream has at most `n` rows). `head()` still
never computes.

- A source reads a limit: SpiralDB as a pushed-down scan limit (with its `where`), others a
  slice of what they hold. `max_rows` does not apply, as for `head`.
- Row-by-row operators and function pods take batches until `n` rows have passed. The
  first batch is `n` rows, or 1,024 when a step can drop rows; it doubles while too few
  pass. A pushed-down limit is used only when no step drops rows; otherwise the scan is
  read in batches and closed at `n`.
- `Join`, `MergeJoin`, `SemiJoin`, `Rejoin`, `GroupBy`, `Batch`, `TagBy`, `PolarsFilter`
  and user operators need a whole input. Refused, naming the step and `run()`;
  `preview(n, compute_all=True)` computes that step in full.
- A side-effect pod, or a function that takes the invocation context, is refused: a
  preview has no effects.
- A kept result (an already computed stream) is sliced and nothing is computed.

## Decisions

| Id | Decision | Recommended |
|---|---|---|
| V1 | Bounded window now, disk-backed later, or wait for the local cache (N2) | window now: no file is written, and paging later only lifts refusals |
| V2 | `as_table()` / `table` / `as_polars_df()` of a bounded view: kept rows, recompute, or refuse | refuse, pointing to the stream (a part must never read as the whole; a view never recomputes) |
| V3 | `head`/`tail` beyond the window, `sort_by_tags` | refuse |
| V4 | Threshold and window | 50,000 rows and 64 MiB of values, both layout-independent (`RunSize`, `value_bytes`: Arrow's own `nbytes` differs with the batches); a result that fits in the two windows is always kept; first and last 1,000 rows; constants, no config yet |
| V5 | `as_table()` after a large run of a batched chain reads and applies operators again | yes (no user function runs again) |
| V6 | Streams in the middle of a chain keep nothing after a run of the last one (today each keeps its result) | yes, above the threshold (below it each keeps its result, as today): keeping one table per step is the cost being removed; each `run()`s by itself |
| V7 | `PolarsFilter` in a batched chain | not until B1 decides expressions that read other rows |
| V8 | `preview` calls user functions | yes, at most on the rows it looks at |
| V9 | `preview` stores or keeps anything | no: not in memory, not in a database. A cached pod reads its stored results and writes none |
| V10 | `preview` order | the first `n` rows of the sync order |
| V11 | Whole-input steps in `preview` | refuse; `compute_all=True` |
| V12 | `preview` on a job node | not offered at first: raises, naming `run()` and `head()` |

## Tests

- Differential: for each operator of a batched chain and chains of several, the result
  pieces put together equal the unbatched `as_table()` (values, types, nullability, column
  order, metadata) with several batch sizes, for every column configuration, with several
  tags and data columns, data-tags, zero rows, and all rows dropped.
- Golden display: text and HTML of a bounded view equal those of the complete view of the
  same rows, narrow and wide, with large cells, at sizes 11, 12, 1,000 and more; a small
  stream's display is unchanged.
- The refusals; `head` after `run`; staleness after the source moves.
- Memory, about 2.2 million rows of a time series, before and after, for the source, a rename, a
  `drop_nulls` and a `filter`.

## Measured

`run()` of a synthetic SpiralDB table, a time series (about 2.2 million rows, 13
columns, about 250 MiB of Arrow data), peak resident memory, one process
per row:

| Stream | Before | After | Arrow pool peak before | after | Time before | after |
|---|---|---|---|---|---|---|
| the source | 10.8 GiB | 1.24 GiB | 9.3 GiB | 0.07 GiB | 8.9 s | 10.4 s |
| `map_data` | 10.8 GiB | 1.40 GiB | 9.3 GiB | 0.25 GiB | 8.8 s | 10.5 s |
| `drop_nulls` | 19.7 GiB | 1.44 GiB | 18.6 GiB | 0.28 GiB | 9.8 s | 11.1 s |
| `filter` | 15.1 GiB | 1.45 GiB | 14.1 GiB | 0.29 GiB | 9.2 s | 10.2 s |
| `drop_nulls`, `filter`, `map_data` | 20.1 GiB | 1.55 GiB | 18.6 GiB | 0.39 GiB | 10.2 s | 9.9 s |

The Arrow pool holds nothing large after the run. What remains is mostly the set of keys the
source remembers to refuse a repeat (outside this track), which still grows with the table.

## Staleness of a chain that keeps nothing

An operator stream tells a change of an operator stream beneath it by that stream's reset
generation, which moves when it discards a kept result. A stream of a batched chain may have
been read without keeping one, and then discards nothing. So the state of such a stream is
its generation, its operator's modified time and the state of its own input, down to the
source (`_chain_state`): a change anywhere beneath is seen by every kept result and window
above it, whichever streams of the chain hold rows. A source with no row gives one batch of
none, so its run scans once.

One outermost question (`_upstream_states`) asks each stream once, so the walk is linear in
the graph: settling a stream asks for the states beneath it, and so does its own state.

## `preview(n)` as built

- `StreamBase.preview` pulls `_preview_batches` (`core/streams/preview.py`) and stops at `n`. A
  function pod gives one row per batch, computed when asked for, so no function is called past
  the last row needed, also under a filter; an operator's batch upstream of a function may be
  larger, since it calls nothing.
- The limit is passed down through operators that drop no row (`rows_dropped_text`) and
  through an active Python function; anything else takes batches and the scan is closed.
- `Pick` and `Index` are refused with the whole-input steps, for the reason they are not
  batched by `run()`.
- `compute_all` memoizes each stream it reads in full for the one call, so a stream two
  branches share is computed once. With it, functions beneath one input may have run when
  another input is refused.
- No meta column is shown: a stored row and a computed one have different ones.
- A `DerivedSource` is a whole read (its origin may compute). A filter nothing passes reads the
  whole table, in batches: `max_rows` does not stop it, as it does not stop `head`.

## Job nodes, as built

An operator job node's `run()` view is the table its current run is kept in (memory-mapped
when large), so it holds every row, refuses nothing, and equals
`read_current_run(0, current_run_row_count())`. Cutting it touches only the pages of the rows
cut; it stays valid after the node is cleared, which paging through the node would not. The
display of any long view now gives Polars the eleven rows it needs, not the whole table. A
function job node and a source job node still hold their rows in memory.
