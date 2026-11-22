# Job nodes pass streams of Arrow data, not rows (ITL-1347, with ITL-858)

Status: approved by the developer (2026-11-22). No code changes with it. Written against
`origin/main` 8c6fc4f5 (2026-11-22); the work lands on the integration branch
`eywalker/job-speed`. Every stage is tracked under ITL-1347, one PR per stage against `eywalker/job-speed`.

Revisions, all on 2026-11-22:
- The ITL-858 input object was folded in, with the protocol change and its call sites.
- Second revision: the developer's terminology ("Arrow table", "Arrow record batch"), and
  streams with channels.
- The developer's clarification: the stream is primary and owns the description. Channels
  are separate structures, opened from a stream and closed into one.
- Third revision: the governing rule (results identical to `main`), every difference from
  `main` listed with its bug, and the differential as every stage's gate.
- Fourth revision, the developer's decisions on order: row order is not critical within a
  stream or channel. Async runs keep arrival order, as on `main`; `Batch` grouping by
  arrival is accepted; column order is not a goal. The order rule of the third revision is
  dropped.
- Fifth revision: `Rejoin`'s new rule (ITL-1355) resolves question 9. `Rejoin` matches on
  every shared column, never refuses on data, and drops unmatched rows.

Terms used throughout:
- **Arrow table**: a `pyarrow.Table`.
- **Arrow record batch**: a `pyarrow.RecordBatch`.
- **Stream**: an Orcapod stream (`StreamProtocol`).
- **Channel**: an async, single-pass carrier of Arrow record batches.

## The governing rule

Developer, 2026-11-22: "We must ensure, above all, that all computations results are
identical to as before unless explicit bug/incorrectness in computation is discovered."

- **Sync path.** Every observable result of the branch equals pinned `main`, byte for
  byte:
  - values, types, nullability and column order;
  - the row order of every kept, stored and returned Arrow table;
  - content and pipeline hashes, record ids;
  - stored paths and stored rows;
  - error types and messages.
- **Async path.** It equals the sync path as a multiset of rows (unordered, each row counted
  as often as it occurs), with columns matched by name.
  Row order is not compared: the developer, 2026-11-22, "generally order of rows shouldn't be
  considered critical". The async path also equals `main`'s async path as a multiset of rows.
- **The only allowed difference** is one that fixes a named bug in `main`. Each is listed in
  "Every difference from `main`" below, with its reproducer, and approved by the developer
  one by one.
- **Speed work never changes a result as a side effect.** Every section of this spec is
  subordinate to this rule.
- **No row below changes a sync-path result of `main`.** Every difference is on the async
  path or the failure path.

## The rule

Developer, 2026-11-22: Orcapod was always meant to keep data as Arrow (or Polars) tables as
long as possible and to avoid converting rows to objects and back. This spec makes that the
rule for the job layer as a whole.

- **The stream is the primary concept, and it owns the description**: schemas, Arrow schema,
  keys, data context and pipeline hash. Sync code receives streams and produces streams.
- **Channels serve async computation, as separate structures.**
  - A stream can open an **input channel** for downstream async compute: a single-pass,
    back-pressured reader of Arrow record batches that refers to its stream for the
    description.
  - A node's async output is written into an **output channel**. When its producer closes
    it, it yields an ordinary stream backed by an Arrow table, with the same description.
  - The conversions between streams and channels are defined in one place.

  The developer, 2026-11-22: "I don't think data structure wise a truly synchronous
  table/list must equal the async channel but given how closely related they are, I was
  thinking they ought to be consolidatable."
- **A row exists only where a user's function is called**: a function pod's function, or a
  side effect's delivery. Even there, the node reads its input as Arrow data and writes its
  results as Arrow data.
- **Observers and the error policy stay per row.** They are told about each row, and a
  failing row is skipped or raised, as today. The transport underneath is not rows.

## Why the job layer is per-row today

From the ITL-1347 comment:

- **March 2026, PLT-922 (the orchestrators).** The node execution protocol was defined per
  row:
  - `execute()` returns `list[tuple[Tag, Data]]`, and `async_execute` reads and writes
    channels of `(Tag, Data)`.
  - The sync orchestrator keeps a buffer of row tuples per node and rebuilds a stream for
    each successor (`_materialize_as_stream`).
  - Every operator node turns its result's Arrow table back into rows.

  This served per-row async streaming, concurrent function calls, per-row observer hooks
  and the per-row error policy. Plain streams never adopted it.
- **November 2026: correctness fixes kept the row protocol and made each row costlier.**
  - ITL-845: `_rows_to_table` keeps each row's source info.
  - ITL-1122: large-type normalization at stream construction, paid three times per row
    datagram.
  - The rule that async output rows equal the sync rows.
- **Channels carry rows and nothing else (ITL-858).** So async operators rediscover from
  rows what the sync path is told. Where rows cannot answer, the two paths diverged:
  - key types on an empty input (ITL-856);
  - column order (ITL-854, ITL-1249);
  - data context (ITL-844, ITL-951);
  - zero-row results (ITL-865, ITL-912);
  - system-tag names after a non-source input (ITL-906).

## What it costs

The ITL-1345 profile (workshop-scale notebook on 0.4.0rc1, local Delta copy, no network):

| Measure | Value |
|---|---|
| Same pipeline as plain streams | 1.9 s |
| As a `PipelineJob`, warm / cold `job.run()` | 44.7 s / 46.8 s, identical rows |
| Warm run: building a `(Tag, Data)` datagram per row passed between job nodes | ~58% (~26 s) |
| Warm run: rebuilding Arrow tables from those rows (`_rows_to_table`, `_materialize_as_stream`) | ~32% (~14 s) |
| Warm run: operator compute / reads / all hashing | ~8% / ~1% / ~0.3% |
| `Data.__init__` calls; `normalize_schema_to_large_types` calls | 121,139; 363,466 (three per row) |
| One source of 63,349 rows: `as_table()` / `list(iter_data())` | 0.29 s / 9.1 s |

About 90% of a warm run converts Arrow tables into rows and back.

### Where rows are made and unmade today

Sync orchestrator (`SyncPipelineOrchestrator.run`):

1. **Source job node**: `list(self.iter_data())`, one datagram per row. A narrower source
   given in the declared form gets a second one (`_declared_row`).
2. **Every edge** (`_materialize_as_stream` → `_materialize_upstream`): the successor's input
   is `R(rows)`, wrapped as the predecessor's stream (`_as_upstream_stream`). A node with *k*
   successors is rebuilt *k* times.
3. **Operator job node**: the operator computes an Arrow table, then
   `list(result.iter_data())` makes rows of it, and `_kept_output` makes an Arrow table of
   them again.
4. **Function job node**: iterates its input's rows; loads stored hits as one datagram per
   stored row; builds each result as a `Data`.
5. **Side-effect job node**: iterates its input's rows and passes them on.
6. **Held nodes** (`plan_run`): `list(node.iter_data())`, and a second pass to count.

The async orchestrator does the same with channels of rows. Barrier operators rebuild
inputs with `R` (`_materialize_input_stream`), and operator nodes rebuild their output with
`R` (`_materialize_to_stream`).

## Definitions

- **R(rows)**: `StaticOutputOperatorPod._rows_to_table(rows)`, the Arrow table today's code
  builds from rows. It is the oracle of the proofs below.
- **Row-built form F**: the one function that turns a node's Arrow table into `R` of that
  table's rows, without making the rows. In order:
  1. Drop the context, meta (`__`) and `_content_hash` columns. A row's `Data.as_table`
     leaves them out, and no operator carries meta columns (ITL-1080).
  2. Drop the data-side copy of each data-tag.
  3. Map `null` to `large_string`, as a datagram's normalization does today
     (`Datagram._init_from_table`; whether it should is ITL-1319's decision).
  4. Place the columns in row order: user tags, system tags, data, source info
     (`_in_row_order`, ITL-1131).
  5. On a complete Arrow table only, never on one Arrow record batch: flag each source-info
     field nullable only when a token is null (`_flag_source_info_nullability`).
- **Transport columns**: the columns passed between nodes. They are the columns of F: user
  tags, system tags, data and source info, in row order, with no context and no meta column.

## The design: the stream is primary; channels are opened from it and closed into it

### The stream owns the description

A stream's description is its existing members plus one new one, `arrow_schema()`:

| Member | Today | What it answers |
|---|---|---|
| `keys()`, `output_schema()` | on `StreamProtocol` | names and Python types, in key order |
| `data_context_key` | on every stream (`hashing_protocols.py:19`) | the stream's data context |
| `pipeline_hash()`, `content_hash()` | on `StreamProtocol` | as today |
| `producer`, `upstreams` | on `StreamProtocol` | as today |
| data-tags | `stream_data_tags(stream)`, from `keys()` | unchanged; no new member |
| **`arrow_schema()`** (new) | – | the Arrow schema of the transport columns: types, nullability and field metadata of the rows, in row order. It tells which system-tag and source-info columns are present, and that no meta column is |

None of these reads, computes or polls a row. A job node is a stream today, so a node owns
its own description, also while it runs.

**Where `arrow_schema()` comes from now.** From `typed_zero_row_table`
(`core/streams/base.py:116`, ITL-912), put through F's steps 1, 2 and 4 and normalized to
large types as `typed_zero_row_input` does. That is exactly the schema of today's zero-row
input (`_zero_row_input`) without its context column.

- A source answers from the Arrow table or schema it holds.
- An operator node applies its operator to its inputs' zero-row Arrow tables, which
  processes no data.
- A function node answers with its input's tags and declared outputs
  (`function_zero_row_table`).
- A loaded node answers from the Arrow type texts its saved file holds (ITL-640).

A stream that cannot type itself without rows answers from its Python schema, through the
data context's converter. These are an unbound slot, and a `PollingSource` never polled
(ITL-1089). Such a stream says so through `arrow_types_declared` (a `StreamBase` property).
A run checks its sources first (ITL-1102), which polls them.

**ITL-1307 later.** When `Schema` carries its Arrow schema, `arrow_schema()` reads the two
schemas plus the system-tag and source-info fields, in one place (`StreamBase`).

Two exceptions keep today's bytes:
- **Source-info nullability** is decided by a complete Arrow table's values (F step 5).
  `arrow_schema()` states those fields nullable (question 5).
- **`null` columns.** Rows carry `large_string`, and a zero-row Arrow table carries `null`, as
  today (ITL-1319).

### Sync: streams in, a stream out

A node's `execute` receives streams and returns its **output stream**:
`_as_upstream_stream(F(node's Arrow table), node)`.

- It takes the node's producer, upstreams, pipeline hash and data context. For a node
  without a producer it is a `_RowsOfStream`.
- With no rows, it is today's zero-row input (`_zero_row_input(node)`).
- That is exactly what each successor receives today, so a successor's input is unchanged by
  construction.

The orchestrator passes each output stream to its successors as it is, and a fan-out passes
the same stream to every successor. A stream on the sync path is always complete:
`as_table()`, `iter_data()` and `head()` behave as for any `ArrowTableStream`.

### Async: input channels opened from streams, output channels closed into streams

Two separate structures, in `src/orcapod/channels.py`. They consolidate with the existing
generic channel protocols: an input channel is a `ReadableChannel[pa.RecordBatch]`, and an
output channel a `WritableChannel[pa.RecordBatch]`.

```python
class InputChannel(ReadableChannel[pa.RecordBatch], Protocol):
    """A single-pass, back-pressured reader of a stream's Arrow record batches."""

    @property
    def stream(self) -> StreamProtocol: ...  # the description; never restated here
    async def receive(self) -> pa.RecordBatch: ...  # raises ChannelClosed at the end
    def __aiter__(self) -> AsyncIterator[pa.RecordBatch]: ...
    async def collect(self) -> list[pa.RecordBatch]: ...  # as every ReadableChannel
    async def to_stream(self) -> StreamProtocol: ...  # reads to the end: what it delivered, as a stream
    async def close(self) -> None: ...  # stop reading; the producer stops waiting


class OutputChannel(WritableChannel[pa.RecordBatch], Protocol):
    """The producer side of a node's async output."""

    @property
    def stream(self) -> StreamProtocol: ...  # the producing node: the description
    async def send(self, batch: pa.RecordBatch) -> None: ...
    async def close(self) -> None: ...
    def fail(self, error: BaseException) -> None: ...
    def open_reader(self) -> InputChannel: ...  # one per consumer, before the first send
    def result(self) -> StreamProtocol: ...  # after close: the node's output stream
```

- **Opening an input channel from a stream.** `stream.open_channel()` is new on
  `StreamProtocol` and implemented once in `StreamBase`. It returns an `InputChannel` that
  delivers that complete stream's transport columns, as its Arrow table's record batches.
  Several channels can be opened on one stream, each with its own position. A held node's
  output stream feeds async successors this way.
- **The producer side.** The orchestrator makes one `OutputChannel` per node that runs,
  before any task starts: `OutputChannel(node, retain=..., buffer_size=...)`. The node writes
  Arrow record batches into it. Each successor gets `output.open_reader()`, an `InputChannel`
  whose `stream` is the producing node, so its description is known before the first batch.
- **Closing into a stream.** After `close()`, `output.result()` is the node's output stream:
  an ordinary stream backed by an Arrow table, with the node's description and identity. It
  is the same object the sync path would return. `input_channel.to_stream()` gives the same
  kind of stream from what the channel delivered.
- **Checks.** `send` checks each Arrow record batch's schema against `stream.arrow_schema()`
  (with the two exceptions above), once per batch and always on (ITL-858 decision 4).

### What lives where

| Member | Stream | Input channel | Output channel |
|---|---|---|---|
| keys, schemas, Arrow schema, data context, pipeline hash, content hash, producer, upstreams | yes; owned here | no; read through `channel.stream` | no; read through `channel.stream` |
| `as_table()`, `iter_data()`, `head()`, re-reading | yes | no | no |
| `open_channel()` | yes | – | – |
| `receive()`, `async for`, `collect()`, `to_stream()`, `close()` | no | yes, single pass | no |
| `send()`, `close()`, `fail()`, `open_reader()`, `result()` | no | no | yes |
| modified time, staleness | yes (a node's, as today) | no | no |

### The conversions, in one place

`src/orcapod/channels.py` holds every conversion between streams and channels:

| Conversion | Function | Used by |
|---|---|---|
| stream → input channel | `channel_from_stream(stream, buffer_size)`, called by `StreamBase.open_channel()` | held nodes feeding async successors; tests; any async consumer of a complete stream |
| output channel → stream | `stream_from_batches(stream, batches)`, called by `OutputChannel.result()` | every node's async output |
| input channel → stream | `stream_from_batches(channel.stream, batches)`, called by `InputChannel.to_stream()`: the batches it delivered, in arrival order | barrier operators; operators that need all of an input |

`stream_from_batches` is the only place a stream is made from Arrow record batches. It
concatenates them, applies F step 5, and wraps the result as `_as_upstream_stream` does for
the describing stream. With no batch, it is the describing stream's zero-row input.

### One operator body for both paths

An operator reads its description from its inputs' streams: on the sync path the stream
itself, on the async path `channel.stream`. So validation, canonical input order, key-type
checks and zero-row results are one implementation. For example,
`validate_inputs(*(c.stream for c in channels))` is the sync call.

- **A barrier operator, async**:
  `streams = await asyncio.gather(*(c.to_stream() for c in channels))`, then
  `static_process(*streams)` (the sync call), then it writes the result's Arrow record
  batches.
- **A per-row operator** (select, map, `AddTags`, `DropNulls`, `Index`, `Pick`) has one rule,
  `_apply(arrow_table, stream)`. Sync applies it to `stream.as_table()`. Async applies it to
  each Arrow record batch, with `channel.stream` as the description.
- **`Join`, `MergeJoin`, `Rejoin` and `SemiJoin`, async**: check key types from
  `channel.stream.arrow_schema()` before the first batch (ITL-856), order inputs with the
  sync method, and stream as below.

### Barrier or streaming: every operator

The developer, 2026-11-22: `Join` and `Rejoin` must not be barrier operators; both can run
fully async on Arrow record batches. The test for each operator: can each output row be
decided from the input rows seen so far? If so, the operator streams. If it needs all of an
input, it is a barrier. Row order within a stream or channel is not critical (developer,
2026-11-22), so an operator whose groups follow arrival (`Batch`) keeps `main`'s form.

| Operator | Async form | Why |
|---|---|---|
| `SelectTagColumns`, `SelectDataColumns`, `DropTagColumns`, `DropDataColumns`, `SelectColumns`, `DropColumns`, `MapTags`, `MapData`, `MapColumns`, `AddTags`, `DropNulls`, `Index`, `Pick` | streaming, per Arrow record batch | each output row comes from one input row, and its content does not depend on other rows |
| `Join` | streaming: a symmetric hash join | an output row is a pair of matching rows; it can be emitted as soon as both have arrived |
| `MergeJoin` | streaming, as `Join` | the same inner join (`join._inner_join`). Each matched pair's lists are built from that pair alone, in argument order |
| `Rejoin` | streaming: a symmetric match on every shared column (ITL-1355), with matched rows removed from both sides | an output row is the set of one origin row's rows from every input; each row matches at most once |
| `SemiJoin` | streaming on the left. A left row is emitted as soon as its key is among the right rows seen so far; undecided left rows wait for the right input to close | whether a left row is kept depends on the whole right input only when no match has been seen yet |
| `GroupBy` | barrier | a group needs all its members. Its output does not depend on input order (members are ordered by tags, record id and source id; groups by key) |
| `Batch` | streaming, per group of `batch_size` arrivals, as on `main`; a barrier for `batch_size=0` | groups consecutive arrivals. When the input arrives in another order than on sync, its groups differ from sync: accepted by the developer ("This difference is totally fine in Batch for sync and async") |
| `PolarsFilter` | barrier over arrival order, as on `main` | a predicate may be a window or whole-table expression (`pl.col(...).rank()`), so a row cannot be decided alone. Such a predicate over an input that arrived out of order selects other rows than sync: the same class as `Batch`, kept as `main` |
| one-input `Join` / `Rejoin` | pass-through | as today |

On `main`:
- `Join`'s async path already streams: a symmetric hash join, row by row
  (`join.py:669`, `_binary_streaming_join`), with N inputs as a staggered chain in canonical
  order.
- `Rejoin` is a barrier (`rejoin.py:416` delegates to the default).
- `Batch` streams per batch over arrival order, and is a barrier for `batch_size=0`
  (`batch.py:211`).

The change: `Rejoin` and `MergeJoin` stream. `Batch` and `PolarsFilter` keep `main`'s async
form.

#### Streaming `Join` and `MergeJoin`

- **Hash tables.** Each input keeps an accumulating Arrow table of its rows and a key frame
  of its prepared keys (`arrow_keys.polars_ready`, as the sync join prepares them). The key
  frame holds the shared tag keys and row numbers only.
- **Probing.** When an Arrow record batch arrives on one input, its key frame is joined
  (Polars, keys and row numbers only, as `_inner_join` does) with the other input's
  accumulated key frame. The matches are emitted at once, every value taken from the two
  Arrow tables by row number (`take`), so values are the inputs', bit for bit. The batch is
  then appended to its own side.
- **Matching is vectorized**, one join per arriving batch, never per row. Each pair is
  emitted exactly once: by whichever of its two rows arrives second.
- **N inputs**: a staggered chain of binary joins in canonical order, as today. Each link is
  an `OutputChannel` whose `stream` is that pair's `zero_row_result`.
- **System-tag renaming** uses each input's `stream.pipeline_hash()` and position, as the
  sync path does.
- **`MergeJoin`** builds each emitted pair's merged lists from the pair's two rows
  (`aggregated_list_array` over the taken values), in argument order.

#### Streaming `Rejoin`

It follows `Rejoin`'s rule as ITL-1355 sets it on `main` (developer, 2026-11-22). That
change lands on `main` separately, before stage 5:
- system tags are assumed unique by construction (ITL-1356 makes record ids unique at the
  source);
- `Rejoin` is an inner join on every column its inputs share: the system-tag columns, shared
  user tags, the data context, and a shared data-tag's source info;
- equality is `Join`'s: NaN equals NaN, and `-0.0` equals `0.0`. The output keeps the first
  input's bits;
- each row matches at most once;
- a row that matches nothing is dropped. A differing shared value is a non-match, never an
  error.

How it streams:

- **Remaining sets.** Each input keeps an Arrow table of its rows not yet matched, with a key
  frame of its shared columns (sorted names, `arrow_keys.polars_ready`). Unlike `Join`,
  which holds every row of both sides, a matched row is removed from its remaining set, so
  the memory held is the unmatched rows only. Removal is a mask over row numbers; the table
  is compacted when the removed share passes a threshold, so a batch never copies the whole
  table.
- **Matching an arriving Arrow record batch**, vectorized and never per row:
  1. The batch's key frame is joined (Polars, keys and row numbers only) with the other
     side's remaining key frame **on every shared column**. That gives the matches under
     `Join`'s equality.
  2. Each matched pair is emitted, built as `_rejoin_pair` builds it (the first input's copy
     of each shared column, every value taken by row number), and both rows leave their
     remaining sets.
  3. The batch's unmatched rows join their side's remaining set.
- **The warning, per batch.** A second join of the same batch, on the system-tag columns
  only, against the other side's remaining rows finds the pairs that match on system tags.
  Pairs found by this join but not by the first one matched on system tags and differed on
  another shared column. For each shared column outside the system tags, one vectorized
  comparison over those pairs, with `Join`'s equality on the prepared keys, names the
  columns that differed. The operator issues its warning once per operator and set of
  columns, as ITL-1355 specifies (after the pattern of `SemiJoinIgnoredColumnWarning`).
  These rows are non-matches: they stay in their remaining sets.
- **Early release.** When one input closes, the other input's remaining rows can never match
  anymore (nothing will arrive to match them), so they are dropped at once. From then on, an
  arriving row from the open input either matches the closed input's remaining rows or is
  dropped at once. So after one input closes, only the closed input's unmatched rows are
  held, and that set only shrinks. When both inputs close, whatever is left is dropped.
- **A differing data context.** The data context is per stream. In a job, every row of an
  edge carries its producing node's context. So inputs whose descriptions name different
  contexts can match nothing. The operator knows this before the first batch: it reads every
  input to its end (ITL-1075-AH1), emits nothing, and warns once, as the rule's non-match.
- **Checks: description-only, before the first batch.** These are the same system-tag names
  on every input (`_require_same_system_tags`), the shared tags' Arrow types
  (`require_shared_arrow_type`, `require_matchable_key`), and disjoint data columns.
  `Rejoin` raises on nothing else, so it never refuses after rows have flowed.
- **N inputs** chain pairwise in canonical order, as the sync `static_process` does. Each
  link applies the same rule.
- **Duplicates.** If system tags are not unique after all (a source with non-unique
  `record_id_columns`), "match once" pairs duplicates in arrival order, which can differ
  between sync and async (risks; ITL-1356 removes the case).

### Row order

The developer, 2026-11-22: "For async absolutely fine that rows are stored in arrival order
-- generally order of rows shouldn't be considered critical", and "row orders are also not
critical within stream/channel".

- **Sync path**: every row order stays exactly as on `main`. Nothing in the design needs to
  change it, and keeping it keeps the differential against `main` exact.
- **Async path**: rows in flight, and what a node keeps, stores and gives as `result()`,
  are in arrival order, as on `main` (`operator_node.py:2231` keeps arrival order today).
  There is no recompute at close, and nothing is retained only for ordering. Retention
  follows `main`'s rules (`materialize_results` included).
- `OutputChannel.result()` is the concatenation of the Arrow record batches the node sent,
  in the order sent.
- `InputChannel.to_stream()` reads its channel to the end and returns the concatenation of
  the batches it delivered, in arrival order, as a stream with the producer's description
  and identity (`stream_from_batches`).
- Async results are compared with sync as a multiset of rows (row order ignored, duplicates
  counted), with columns matched by name.

### The hard semantics

| Question | Rule |
|---|---|
| **Re-reading** | A channel is single-pass. To re-read, go back to a stream: the complete stream the channel was opened from, or the stream `result()` / `to_stream()` gives. A stream is always re-readable. |
| **When batches are retained** | An `OutputChannel` made with `retain=True` keeps every Arrow record batch it was sent, so that `result()` can give the output stream. Retained: every operator, function and side-effect node, which keep their output today; the result stream becomes that kept output, so nothing is held twice. Also retained: every node when `materialize_results=True` (the default), because `node_outputs` holds each output. Not retained (`retain=False`): only a source's output when `materialize_results=False`. Its `result()` then raises, and memory is bounded by the readers' queues. |
| **Memory bound** | Each input channel queues at most `buffer_size` Arrow record batches. With `retain=True` the output channel holds the node's whole output, which today's code already holds. A barrier operator's `to_stream()` holds its whole input, as today. |
| **Several readers (fan-out)** | One input channel per consumer, each with its own position: `open_reader()` on an output channel, or `open_channel()` on a stream. With `retain=False`, `send` waits until every reader's queue has room, so the slowest reader sets the pace, as today's `BroadcastChannel` does. With `retain=True`, readers read the retained list at their own position, and `send` never waits. |
| **Reading to the end (ITL-1075-AH1)** | Every input channel is read to its end or closed; `close()` unregisters it. As today, REPLAY reads and discards its inputs while it sends, and `SemiJoin` reads its left input while it collects its right one. The ITL-1075 deadlock tests run with one-row batches as well as full ones. |
| **Late readers** | `open_reader()` after the first `send` replays the retained batches and continues when `retain=True`. Otherwise it is refused (`ValueError`). The orchestrator opens every reader before any task starts. `open_channel()` on a stream works at any time. |
| **Sync reads while a node runs** | There is no open stream: a node's output stream exists only once its output channel closes. Reading the node itself (`as_table()`, `iter_data()`, `head()`, the preview) follows today's rule for a job node whose run has not completed: zero rows typed by its schema, status `not_computed`, never computing and never waiting (ITL-1095, ITL-991). Nothing blocks, and nothing new raises. |
| **Identity** | A channel has no identity. Its `stream` does: the producing node, or the stream it was opened from. `result()` and `to_stream()` give streams with exactly today's successor-input identity (`_as_upstream_stream`). Nothing about the transport enters a hash: not `retain`, not the buffer size, not batch boundaries, not the reader. |
| **Producer failure** | The producing node's `finally` calls `output.fail(error)`, where it closes its output today. Every reader's next read raises `UpstreamFailedError` naming the node, never a normal end, so a barrier operator never computes over a partial input. The run raises the producer's own error, as today: the failure selection (`async_orchestrator.py:708` `_single_failure`, `:742` `_single_database_error`) skips `UpstreamFailedError`. |
| **Cancellation** | A cancelled producer fails its output channel with the cancellation. A cancelled reader closes its input channel. |
| **Close** | `close()` after the last `send` completes the output. A second `close()` does nothing, and `send` after `close` raises. A zero-row output is a close with no batch: `result()` is the zero-row stream typed by `stream.arrow_schema()`, never by rows. |

## Today vs proposed

Each concern, with where it lives today on `origin/main` 8c6fc4f5.

| Concern | Today | Proposed |
|---|---|---|
| **Data unit** | Rows: `list[tuple[Tag, Data]]` per node (node protocols `node_protocols.py:45`–`:175`; the sync buffers, `sync_orchestrator.py:101`–`:141`); async channels of rows | A stream (sync). Arrow record batches through channels (async) |
| **Description** | Sync: the stream's members, with Arrow types from `typed_zero_row_table` (`core/streams/base.py:116`). Async: only `input_pipeline_hashes` (`static_output_pod.py:728`, `base.py:207`, `:321`); everything else is read from rows | The stream's members plus `arrow_schema()`, on both paths. Async reads it through `channel.stream` |
| **Sync input** | Rebuilt from the predecessor's rows for each successor: `_materialize_as_stream` (`sync_orchestrator.py:206`) → `_materialize_upstream` (`static_output_pod.py:570`) → `_rows_to_table` (`:304`) | The predecessor's output stream, passed as it is |
| **Sync output** | A row list: a source's `list(self.iter_data())` (`source_node.py:1191`); an operator's `list(result.iter_data())` plus `_kept_output` (`operator_node.py:1647`, `static_output_pod.py:428`); a function node's appended pairs (`function_node.py:2485`) | The node's output stream, `_as_upstream_stream(F(Arrow table), node)` |
| **Async input** | A row `ReadableChannel` per edge (`async_orchestrator.py:230`–`:250`, `:294`, `:320`, `:340`) | An `InputChannel` opened from the predecessor's `OutputChannel`, or from a held predecessor's stream |
| **Async output** | A row `WritableChannel`. An operator node forwards through an intermediate channel and collects every row (`operator_node.py:2218`, `:2231`), then rebuilds them (`_materialize_to_stream`, `static_output_pod.py:543`) | An `OutputChannel`; `result()` after close is the output stream |
| **Fan-out** | Sync: one rebuild per successor (`sync_orchestrator.py:110`, `:123`). Async: `BroadcastChannel` (`async_orchestrator.py:239`, `channels.py:247`) | Sync: the same stream for every successor. Async: one reader per successor on one `OutputChannel` |
| **Held nodes** | `list(node.iter_data())`, then a second pass to count (`node_run.py:341`–`:346`). Async: `_send_held` re-sends the rows (`async_orchestrator.py:651`), and `_drain` reads inputs (`:645`) | The held node's output stream. Async successors open channels on it. Counts are read without rows |
| **Observers** | Per-row hooks with the row's datagrams (`function_node.py:2579` `on_data_start`, `:2606`/`:2614` `on_data_crash`; side effects per row, `side_effects.py:403`) | The same hooks and order, with `RowView`s that build a datagram only when read |
| **Error policy** | Per row in the function node's loop (`function_node.py:2628` `fail_fast`; a failed row is not appended) and per delivery | Per call, as today; a failed row is left out by mask |
| **Zero rows** | Sync: `_zero_row_input` (`static_output_pod.py:476`). Async: an operator node records rows through `_InputTap` (`operator_node.py:109`) to rebuild empty inputs (`:2275`); barrier operators emit nothing (ITL-865 narrow fix) | Sync: the same zero-row stream. Async: `to_stream()` of an empty channel gives that same stream, typed by `arrow_schema()` |
| **Async row order** | Rows flow in arrival order. `Join` streams in match order (`join.py:669`), and `Rejoin` is a barrier (`rejoin.py:416`). The async operator node keeps and stores rows in arrival order (`operator_node.py:2231`), so they equal sync only as a multiset, in a scheduling-dependent order | As on `main`: arrival or match order, in flight and in what a node keeps and stores (multiset-equal to sync; accepted by the developer). `Join`, `MergeJoin`, `Rejoin` and `SemiJoin` stream; `Batch` streams by arrival as on `main`; `GroupBy` and `PolarsFilter` are barriers |
| **Identity** | The successor's input takes the predecessor's identity: `_as_upstream_stream` (`static_output_pod.py:634`), or `_RowsOfStream` without a producer (`:779`). An async barrier rebuild takes `input_pipeline_hashes` (`_materialize_input_stream`, `:689`) | Output streams are built by the same `_as_upstream_stream`. Channels have no identity |
| **Memory** | Row objects held in several places: the sync buffers (`sync_orchestrator.py:101`), `_CollectingWriter` (`async_orchestrator.py:799`), an operator node's `collected` list plus its kept stream (`operator_node.py:2231`), a function node's `_cached_output_datas` (`function_node.py:1430`), a side effect's `_held_rows` (`side_effects.py:1103`). Channel buffer: 64 rows | Arrow tables in the same places. A retained output channel's result is the node's kept output, not a second copy. Input channel buffer: `buffer_size` Arrow record batches |

## Alternatives considered

| | Stream primary, separate channels (proposed) | (a) Two protocols, `TableInput` + `BatchInput` (first revision) | (b) One class that is both (second revision) |
|---|---|---|---|
| Where the description lives | on the stream only; a channel refers to it | a separate `StreamDescription` that both protocols repeat | on the one class |
| What sync code receives | streams | `TableInput` (a stream) | streams |
| What async code receives | input channels, each referring to its stream | `BatchInput` | streams in a "channel-fed" state |
| States a stream can be in | complete only | complete only | open, complete, consumed, failed |
| Sync read of data still being produced | impossible: no stream exists yet, and the node reads as not run | impossible by type | possible to attempt; must raise |
| Re-reading | go back to the stream | `materialize()` | the same object, if retained |
| Conversions | three, in `channels.py` | `materialize()` only | inside the one class |
| Fits the existing channel protocols | yes: `ReadableChannel` / `WritableChannel` of Arrow record batches | no: a new protocol | no |
| Complexity | two small classes, plus `open_channel()` and `arrow_schema()` on streams | two protocols plus a description protocol | one class with four states, and five members on `StreamProtocol` |
| Operator written once | yes: the description is always a stream (`channel.stream`) | yes, through the description protocol | yes |

The proposed form keeps what the second revision gained:
- one description;
- the sync operator code reused on the async path;
- a node's output as an ordinary stream.

It also removes the second revision's open and consumed stream states, and their raising
sync reads. Nothing blocks it.

The four alternatives, each with what it gains, what it costs, and why it was not chosen:

| Alternative | Gains | Costs | Why not chosen |
|---|---|---|---|
| **(a) Two protocols** (`TableInput`, a stream; `BatchInput`, explicitly not one; a shared `StreamDescription`) | The split is visible in the types. An async input cannot be read synchronously by mistake | A third protocol repeats what a stream already answers. Operators are written against the description protocol rather than streams. A `NodeTable` wrapper is needed for outputs | The developer prefers the stream as the primary concept that owns the description. The proposed form keeps (a)'s safety, since no open stream exists, with one fewer type |
| **(b) One class that is both** (a stream that is fed by and read as a channel) | One type for everything. Sync code runs on async data once it is complete | Four states (open, complete, consumed, failed) on one class. Sync reads must raise on an open stream. Five new members on `StreamProtocol`. It blurs the line between an Arrow table and a queue, the hazard ITL-858 named | The developer: "I don't think data structure wise a truly synchronous table/list must equal the async channel". The proposed form gives the same reuse with separate, convertible structures |
| **(c) Rows with cheaper datagrams only** (ITL-1348-style: no per-row normalization or `prepare_prefixed_columns`, rows built through a trusted path) | The smallest change, with no protocol change and byte identity easy to keep. ITL-1348 estimates about 10 s of the 45 s (the normalizations and `prepare_prefixed_columns`). It is in progress now | 121,139 datagrams are still built, `R` still concatenates one-row Arrow tables on every edge, and fan-out still rebuilds per successor. Its ceiling is the normalization share, not the ~40 s conversion share. The ITL-858 divergences stay, because channels still carry only rows | Not a replacement but a complement. It lands first and keeps paying wherever rows remain: function-node misses, observers that read a `RowView`, side-effect deliveries. It cannot bring a job near the plain-stream floor (1.9 s) |
| **(d) Polars-backed nodes** (nodes pass Polars data frames or lazy frames; operators fused into Polars plans) | Fused, multithreaded operator plans and fast joins and group-bys. Lazy evaluation could skip work | Orcapod's type rules require values bit for bit and Arrow types as given. Polars round trips lose them: `date64` comes back at midnight, `decimal256` panics, and extension (logical) types, `float16` and large-versus-non-large layouts do not survive. The operators were moved off carrying columns through Polars for this reason (ITL-1284, ITL-1281). Hashes are computed over Arrow tables, so every node would convert back | It conflicts with the type-strict, bit-for-bit rules, and it targets the ~8% operator compute rather than the ~90% conversions. It stays future work, only where an operator already works in Polars |

## Every difference from `main`

Every observable difference this design introduces, on any path. A row is allowed only
with the developer's approval, given row by row. Each row gets its own test: it fails on
`main`, passes on the branch, and is the only thing the differential excludes for that
row, by name. The developer has accepted some behaviours of `main` that are not listed as
rows; they are kept as `main` has them, and documented below.

**No row changes a sync-path result.** On the sync path the branch equals `main` byte for
byte. Stages 1–4 hold this by construction and by L0–L5, and a lemma that fails stops its
stage.

| # | What differs | On `main` today | On the branch | Why `main` is wrong (bug, reproducer) | Approval needed |
|---|---|---|---|---|---|
| e1 | Async `Join` / `SemiJoin` with an empty input and disagreeing key types | Returns nothing (no row is there to read a type from) | Raises the sync path's `InputValidationError`, from the descriptions, before the first batch | **ITL-856.** Reproducer: a zero-row source with an `int16` key joined, async, with a source whose key is `int32` | approved (developer, 2026-11-22) |
| e2 | An async input row of another data context | The async function node answers it from the cache (the ITL-858 inventory) | A whole-input error before the first batch, as on sync | **ITL-951** (its comment; ITL-858 decision 9). Reproducer: an async function node fed a stream of another data context, with its result stored | approved (developer, 2026-11-22) |
| e3 | Function calls and stored rows for duplicate inputs, async; `RunSummary` computed counts | About 3× more calls than sync, and more stored rows (4,188 against 4,060 in ITL-1062's measurement) | Misses are deduplicated by base entry id, so calls, stored rows and counts equal sync | **ITL-1062.** Reproducer: ITL-1062's workshop-shaped pipeline with repeated input data, async | approved (developer, 2026-11-22) |
| e6 | Async values that depend on an earlier validation of the same operator object | `SemiJoin._validated_common_keys` and `Index._element_arrow_type_fallback` hold the last validation's values (ITL-858, section 1.2). One operator object applied to two streams can use the other stream's keys or fallback type | Read from each input's description, with no state | The ITL-858 finding; filed as ITL-1358. Reproducer: one `SemiJoin` instance validated on stream B, then run async over stream A with other keys. To be confirmed in stage 5 | ITL-1358 (filed); fixed with stage 5 once a reproducer confirms it |
| e7 | What a consumer does after its producer failed (async, failure path) | The producer closes its output in `finally`, so a consumer sees a normal end and may finish, and a `LOG` operator node may store, a partial result before the task group cancels it | Readers raise `UpstreamFailedError` at the failure point instead of seeing a normal end, so no consumer finishes, or stores its result, as if its input were complete. Batches a consumer processed before the failure are still processed, and a streaming node may already have passed them on or a function node stored results for them, as on `main`; there is no rollback. A barrier operator never computes over a partial input. The run raises the producer's own error, as today | A candidate race, to be confirmed with a reproducer in stage 5: a `fail_fast` function node raising on row 3 of 6, feeding a `Select` node in `LOG` mode; check the operator cache after the failed run. If `main` never stores in practice, this row is no difference | filed by the developer only once a test reproduces it |
| e8 | Meta columns of an async side-effect node's rows | An async side-effect node passes on and holds the rows it receives as they are, so a computed row keeps its `__computed` meta column (from the cached function pod), and the node's `as_table(all_info=True)` shows it. On sync, its input is rebuilt from rows (`_rows_to_table` drops meta), so the column is absent | Async drops input meta columns as sync does. With table transport this follows from the transport columns, which hold no meta column | A `main` bug: the async and sync paths give the node different columns. Found by the stage 0 harness on `job-speed` 0e15d0db, which does not change side-effect nodes; the stage 0 differential checks it on pinned `main`. Reproducer: a function node (with a result store) feeding a `tap_pod`, async against sync, comparing the side-effect node's `as_table(all_info=True)` columns | approved (developer, 2026-11-22) |

**Resolved: refusals after rows have flowed (was c, question 9).** The developer's `Rejoin`
rule (ITL-1355) removes every data-dependent refusal from `Rejoin`: it checks only what its
inputs' descriptions show, before the first batch, so the streaming form never refuses after
rows have flowed. The fallback is gone. `Join`'s refusals were already schema-level.

**A behaviour change on `main`, not made by this design: ITL-1355.** It is decided by the
developer and lands on `main` separately (its own PR), before stage 5. Inputs `main`'s
`Rejoin` refused now give fewer rows plus a warning:
- duplicate origin rows;
- a null system tag;
- rows of different data contexts;
- shared values that differ.

`Rejoin` is provisional (`PROVISIONAL_OPERATORS`), and no golden value moves. Every stage
pins the `main` it is compared with, which includes ITL-1355 once it has landed.

**Accepted by the developer, kept as `main`** (2026-11-22). These are not differences the
branch introduces: the branch keeps `main`'s behaviour, and they are documented here.
- **Async row order (was a).** Kept, stored and returned tables of an async run are in
  arrival order, multiset-equal to sync. This covers operator caches, `node_outputs`, the
  function-node stored tables and the side-effect log, all written as rows complete.
  Developer: "For async absolutely fine that rows are stored in arrival order". This settles
  question 11.
- **`Batch` grouping by arrival (was b).** When `Batch`'s input arrives in another order than
  on sync (behind a concurrent function node, or a streaming join), its groups differ from
  sync, and so do its values, its `batch_v1:` folds and everything downstream. For example,
  rows `t = 0..5` through a function node sleeping `(5 - t) × 10 ms` give batches
  {5,4}, {3,2}, {1,0} against sync's {0,1}, {2,3}, {4,5}. Developer: "This difference is
  totally fine in Batch for sync and async".
- **An order-dependent `PolarsFilter` predicate over arrival order (was e5).** It is the same
  class as `Batch`: a window or cumulative predicate (`rank`, `cum_sum`) over an input that
  arrived out of order selects other rows than sync. Accepted by the developer
  (2026-11-22), like `Batch`: `main`'s behaviour is kept and documented in the
  `PolarsFilter` docstring and the concepts page, with no fix.
- **Async column order (was e4, ITL-1249).** Developer: "generally how column order comes out
  is not as critical". The branch may keep or fix the async column order of
  `ASYNC_ORDER_XFAIL` (`tests/test_core/operators/test_several_items.py:211`); it is neither
  a goal nor a gate. The async comparison matches columns by name.

**Checked, no difference found:**
- **The sync path**, at every stage, by construction and L0–L5.
- **Error types and messages**: `Join`'s schema-level checks keep their messages, asserted
  in L6.
- **Observer events**: the same events in the same order and count. Function and side-effect
  nodes are per row as today; operators and sources call node hooks only.
- **`RunSummary`**: identical on sync; on async it differs only under e3.
- **`head()` and the preview**: identical on sync. On async they show arrival order, as on
  `main`.
- **Warnings**: issued from the same validation calls.
- **Async `Join` key matching**: `main` matches through `_match_key` / `frozen_scalar`,
  which is meant to compare as the sync join does. The branch uses the sync join's prepared
  keys. L6's corpus includes NaN, `0.0` / `-0.0` and `time64[ns]` keys, so a difference here
  would show up and be added as a row.

**Not part of this design:**
- **Polars' join order.** The sync joins take their row order from Polars' inner join without
  `maintain_order` (risks). Making it explicit is allowed only if stage 0 shows that
  `maintain_order="left_right"` gives exactly today's order on every test and in the
  differential. Otherwise it is a result change that needs its own decision.
- **One canonical stored order** (for example, by record id). It would change today's
  stored row order, so it is out of scope under the governing rule.
- **Sync order for async results** (the third revision's order rule), dropped by the
  developer's decision on row order.

## Protocol change

All of these land on `eywalker/job-speed`, which reaches `main` as one merge (decision 7).

### `StreamProtocol` (`src/orcapod/protocols/core_protocols/streams.py:81`)

Two new members:
- `arrow_schema() -> pa.Schema`;
- `open_channel(*, buffer_size: int = 8) -> InputChannel`.

`StreamBase` implements both for every existing stream kind. A class that implements
`StreamProtocol` without `StreamBase` must add them (the release note says so).

### Node protocols (`src/orcapod/protocols/node_protocols.py`)

| Protocol | Before | After |
|---|---|---|
| `SourceNodeProtocol.execute` (:45) | `execute(self, *, observer=None) -> list[tuple[TagProtocol, DataProtocol]]` | `execute(self, *, observer: ExecutionObserverProtocol \| None = None) -> StreamProtocol` |
| `SourceNodeProtocol.async_execute` (:51) | `async_execute(self, output: WritableChannel[tuple[Tag, Data]], *, observer=None) -> None` | `async_execute(self, output: OutputChannel, *, observer=None) -> None` |
| `FunctionNodeProtocol.execute` (:84) | `execute(self, input_stream: StreamProtocol, *, observer, error_policy, run_id, load_only) -> list[tuple[Tag, Data]]` | `execute(self, input_stream: StreamProtocol, *, observer=None, error_policy: Literal["continue", "fail_fast"] = "continue", run_id: str \| None = None, load_only: bool = False) -> StreamProtocol` |
| `FunctionNodeProtocol.async_execute` (:94) | `async_execute(self, input_channel: ReadableChannel[tuple[Tag, Data]], output: WritableChannel[...], *, ...) -> None` | `async_execute(self, input_channel: InputChannel, output: OutputChannel, *, observer=None, error_policy="continue", run_id=None, load_only=False) -> None` |
| `OperatorNodeProtocol.execute` (:127) | `execute(self, *input_streams: StreamProtocol, observer=None, load_only=False) -> list[tuple[Tag, Data]]` | `execute(self, *input_streams: StreamProtocol, observer=None, load_only: bool = False) -> StreamProtocol` |
| `OperatorNodeProtocol.async_execute` (:134) | `async_execute(self, inputs: Sequence[ReadableChannel[...]], output: WritableChannel[...], *, observer, load_only) -> None` | `async_execute(self, inputs: Sequence[InputChannel], output: OutputChannel, *, observer=None, load_only=False) -> None` |
| `SideEffectNodeProtocol.execute` (:158) | `execute(self, input_stream: StreamProtocol, *, observer, run_id, load_only) -> list[tuple[Tag, Data]]` | `execute(self, input_stream: StreamProtocol, *, observer=None, run_id=None, load_only=False) -> StreamProtocol` |
| `SideEffectNodeProtocol.async_execute` (:167) | `async_execute(self, inputs: Sequence[ReadableChannel[...]], output: WritableChannel[...], *, observer, run_id, load_only) -> None` | `async_execute(self, input_channel: InputChannel, output: OutputChannel, *, observer=None, run_id=None, load_only=False) -> None` (one input, as it always had) |

Sync takes streams and returns a stream. Async takes input channels opened from streams,
and an output channel. The sync input type does not change, so every `execute(stream)` call
in the tests keeps its argument.

**Implemented by:**
- `SourceNodeBase` (`core/nodes/source_node.py:1191`, `:1216`);
- `FunctionJobNode` (`core/nodes/function_node.py:2485`, `:4130`);
- `OperatorJobNode` (`core/nodes/operator_node.py:1647`, `:2133`);
- `SideEffectJobNode` (`side_effects.py:1224`, `:1455`);
- `LoadedSideEffectJobNode` (`pipeline/loaded_side_effects.py:259`, `:289`).

**Called by:** the two orchestrators, `FunctionJobNode.run`, `SideEffectJobNode._run_alone`
and the loaded side-effect node.

### Operator and function pod `async_execute`

| | Before | After |
|---|---|---|
| `StaticOutputOperatorPod.async_execute` (`static_output_pod.py:728`) and every override | `async_execute(self, inputs: Sequence[ReadableChannel[tuple[Tag, Data]]], output: WritableChannel[tuple[Tag, Data]], *, input_pipeline_hashes: Sequence[ContentHash]) -> None` | `async_execute(self, inputs: Sequence[InputChannel], output: OutputChannel) -> None` |
| `FunctionPod.async_execute` (`function_pod.py:660`) | `async_execute(self, inputs: Sequence[ReadableChannel[...]], output: WritableChannel[...], pipeline_config=None, *, observer, error_policy, run_id) -> None` | `async_execute(self, inputs: Sequence[InputChannel], output: OutputChannel, pipeline_config: PipelineConfig \| None = None, *, observer=None, error_policy="continue", run_id=None) -> None` |

`input_pipeline_hashes` disappears: an operator reads `inputs[i].stream.pipeline_hash()`.
That removes 65 uses in 13 source files and 84 lines in 17 test files.

**Implemented by:**
- the barrier default (`static_output_pod.py:728`);
- `UnaryOperator` (`base.py:202`) and `BinaryOperator` (`base.py:316`);
- `FunctionPod` (`function_pod.py:660`);
- the 16 overrides:
  - `add_tags.py:336`, `drop_nulls.py:322`, `batch.py:211`, `semijoin.py:308`,
    `join.py:669`, `index.py:448`, `pick.py:444`, `rejoin.py:416`;
  - `column_selection.py:152`, `:305`, `:444`, `:598`, `:825`;
  - `mappers.py:263`, `:538`, `:825`.

**Called by:** `OperatorJobNode.async_execute` (`operator_node.py:2244`), `Rejoin`
(`rejoin.py:428`), the function node (`function_node.py:4467`, `:4485`), and tests.

**Validation state goes**, since the description is always at hand:
`SemiJoin._validated_common_keys` (`semijoin.py:287`, `:306`, `:381`) and
`Index._element_arrow_type_fallback` (`index.py:82`, `:126`).

### Channels (`src/orcapod/channels.py`)

| Class | Before | After |
|---|---|---|
| `ReadableChannel[T]` (:46), `WritableChannel[T]` (:63) | protocols in every node, operator and pod signature, with `T = tuple[Tag, Data]` | unchanged generic protocols. `InputChannel` and `OutputChannel` are their Arrow-record-batch forms. Queues inside a node (a function node's row queue to its pod) use them with rows |
| `Channel[T]` (:172) | the edge for one successor | unchanged generic bounded queue: the queue of each `InputChannel`, and of queues inside nodes |
| `BroadcastChannel[T]` (:247) | the edge for fan-out | removed: an `OutputChannel` with several readers is the fan-out. It has no other user |
| `ChannelClosed` (:36) | raised by `receive` on a closed, drained channel | unchanged; also raised by `InputChannel.receive` |
| **new** `InputChannel`, `OutputChannel` | – | defined above; the conversions (`channel_from_stream`, `stream_from_batches`) live beside them |
| **new** error | – | `UpstreamFailedError`, in `orcapod.errors` |

### Results and orchestrators

| | Before | After |
|---|---|---|
| `OrchestratorResult.node_outputs` (`result.py:21`) | `dict[Any, list[tuple[Tag, Data]]]` | `dict[Any, StreamProtocol]`: each node's output stream, backed by an Arrow table. This refines developer decision 1 (question 8) |
| `SyncPipelineOrchestrator.run(..., held_rows=...)` (`sync_orchestrator.py:58`) | `held_rows: Mapping[Any, list[tuple[Tag, Data]]] \| None` | `held: Mapping[Any, StreamProtocol] \| None` |
| `AsyncPipelineOrchestrator.run` / `run_async` / `_run_async` (`async_orchestrator.py:77`, `:158`, `:196`) | `held_rows: ...` | `held: Mapping[Any, StreamProtocol] \| None` |
| `RunPlan.held_rows` (`node_run.py:267`) | `dict[Any, list[Any]]` | `held: dict[Any, StreamProtocol]` |

Not changed: `FunctionJobNode.execute_data(tag, data)` (`function_node.py:2464`), a per-row
entry point that no orchestrator uses (33 test lines in 10 files). The executors'
`async_execute_callable` is also unchanged.

## Call sites

Every call site on `origin/main` 8c6fc4f5, and what it becomes.

### Sync orchestrator (`src/orcapod/pipeline/sync_orchestrator.py`)

| Line | Today | Becomes |
|---|---|---|
| :58–66 | `run(..., held_rows=...)` | `run(..., held=...)` |
| :104 | `buffers[node] = list(held[node])` | `outputs[node] = held[node]` (the held node's output stream) |
| :106 | `node.execute(observer=...)` (source) | same call; returns the output stream |
| :108–119 | `_gather_upstream`, `_materialize_as_stream(buf, upstream)`, `node.execute(input_stream, ...)` | `node.execute(outputs[pred], ...)` |
| :121–128 | `_gather_upstream_multi`, one `_materialize_as_stream` per input, `node.execute(*input_streams, ...)` | inputs in the same order (`node.upstreams`); `node.execute(*(outputs[p] for p in ordered), ...)` |
| :130–141 | as :108 (side-effect node) | as :108 |
| :173, :187 | `_gather_upstream*` return row buffers | return output streams; order logic unchanged |
| :206 | `_materialize_as_stream` | removed: each output stream already is the successor's input |
| :225 | `_gc_buffers` | unchanged logic over `outputs` |
| :155 | `OrchestratorResult(node_outputs=buffers)` | `OrchestratorResult(node_outputs=outputs)` |

### Async orchestrator (`src/orcapod/pipeline/async_orchestrator.py`)

| Line | Today | Becomes |
|---|---|---|
| :77, :158, :196 | `held_rows=` | `held=` |
| :230–250 | `Channel` per single edge, `BroadcastChannel` + `add_reader()` for fan-out, a sink `Channel` per terminal node | one `OutputChannel(node, retain=..., buffer_size=...)` per node that runs; one `open_reader()` per successor. A terminal node's output channel has no reader |
| :258–265, :799 | `_CollectingWriter` copies every row | removed: `node_outputs` holds each `output.result()` |
| :267–275, :645, :651 | held node: `_drain` each input, `_send_held(rows, writer)` | a held node's output stream opens one input channel per async successor (`held[node].open_channel()`). `_drain` and `_send_held` are removed, since the plan gives a held node no input edge |
| :281 | `node.async_execute(writer, ...)` (source) | `node.async_execute(output, ...)` |
| :294 | `node.async_execute(input_reader, writer, ...)` (function) | `node.async_execute(reader_for(pred, node), output, ...)` |
| :320 | `node.async_execute(input_readers, writer, ...)` (operator; ordered by `node.upstreams`) | `node.async_execute([reader_for(p, node) for p in ordered], output, ...)` |
| :340 | `node.async_execute([input_reader], writer, ...)` (side effect) | `node.async_execute(reader_for(pred, node), output, ...)` |
| :708, :742 (`_single_failure`, `_single_database_error`) | pick the error the run raises | unchanged, but skip `UpstreamFailedError` |

`reader_for(pred, node)` is the reader opened for that edge before tasks start: from the
predecessor's output channel, or from a held predecessor's stream.

### Nodes, operators and pods

| Site | Today | Becomes |
|---|---|---|
| `source_node.py:1191` `execute` | `list(self.iter_data())` | the output stream over `F(declared(bound.as_table(all columns)))` |
| `source_node.py:1216` `async_execute` | sends each row of `async_iter_data()` | sends that Arrow table's record batches, or one batch per poll for a `PollingSource` |
| `operator_node.py:1647` `execute` | `process`, `list(result.iter_data())`, `_kept_output` | `process`; the kept output and return value is `_as_upstream_stream(F(result.as_table(...)), self)` |
| `operator_node.py:2133` `async_execute` | `intermediate` row `Channel` (:2218), `collected`, `_materialize_to_stream` | passes its input channels and its output channel to the operator. After close, its kept output is `output.result()`, stored in `LOG` mode |
| `operator_node.py:2241–2250` | `hashes = [...]`; `operator.async_execute(taps, intermediate.writer, input_pipeline_hashes=hashes)` | `operator.async_execute(inputs, output)` |
| `operator_node.py:109` `_InputTap`, `:2275` `_materialize_upstream(..., zero_rows=True)` | taps record rows to rebuild empty inputs | removed: an empty input's `to_stream()` is its zero-row stream |
| `function_node.py:2485` `execute`, `:2008` `_execute_load_only` | iterate `input_stream.iter_data()` | read `input_stream.as_table()`: entry ids from slices, hits from the stored Arrow table, calls on misses; return the output stream |
| `function_node.py:4130` `async_execute` | three stages over row channels | reads Arrow record batches from its input channel. Hits are sent as batches; misses go row by row to the pod's row executor; results are sent as batches |
| `function_node.py:4259`, `:4262` | `compute_channel`, `result_channel`: row `Channel`s to and from the pod | kept as row queues inside the node, since rows exist only for misses |
| `function_node.py:4467`, `:4485` | `execution_pod.async_execute([compute_channel.reader], result_channel.writer, ...)` | the pod's per-row executor: today's semaphore and hooks, split out so the node and the pod's own `async_execute` share it |
| `function_node.py:4055` `FunctionJobNode.run` | `_ = self.execute(self._input_stream, observer=counter, ...)` | unchanged call; returns a stream |
| `side_effects.py:1224`, `:1455` | iterate and pass on rows | rows for delivery only; the output is the input's Arrow data masked to the rows passed on |
| `side_effects.py:1428` `_run_alone` | `self.execute(self._input_stream, ...)` | unchanged call |
| `loaded_side_effects.py:284`, `:319` | `super().execute(...)`, `super().async_execute(...)` | same forwarding, new signatures |
| `rejoin.py:428` | `StaticOutputOperatorPod.async_execute(self, inputs, output, input_pipeline_hashes=...)` | `StaticOutputOperatorPod.async_execute(self, inputs, output)` |
| `join.py:815`, `:834` | row `Channel` chain inside an N-ary join | an `OutputChannel` per intermediate pair, its `stream` the pair's `zero_row_result`; the next step reads its reader |
| `static_output_pod.py:689`, `:543`, `:570` (`_materialize_input_stream`, `_materialize_to_stream`, `_materialize_upstream`) | rebuild rows into streams | removed: `to_stream()`, `result()` and the output streams replace them |
| `static_output_pod.py:304` `_rows_to_table` | used on every edge | moves to the tests as the oracle `R`, once nothing in the library uses it |

### Plan and held nodes

| Site | Today | Becomes |
|---|---|---|
| `node_run.py:341–345` | `held_rows = {node: list(node.iter_data()) ...}` | `held = {node: node.output_stream() ...}`. That is an operator's kept stream, a function node's held rows as a stream in cache order (developer decision 3), or a side effect's held stream |
| `node_run.py:346` | `held_counts = {node: sum(1 for _ in node.iter_data()) ...}` | `held[node].as_table().num_rows`, or a function node's run entry counts |
| `node_run.py:432`, `job.py:1000`, `job.py:1071` | `held_rows=plan.held_rows` | `held=plan.held` |
| `run_summary.py:532` | reads held counts | unchanged |

### Tests that call these directly

Counted on `origin/main`, by file, excluding database cursors' own `execute`:

| What | Files | Lines | Largest | Change |
|---|---|---|---|---|
| node `.execute(` | 18 | 88 | `test_ephemeral_result.py` 32, `test_side_effect_pod.py` 13, `test_node_execute.py` 8, `test_node_protocols.py` 7 | arguments unchanged; a test that reads the returned list reads `iter_data()` of the returned stream |
| `.async_execute(` | 42 | 171 | `test_native_async_operators.py` 39, `test_node_async_execute.py` 26, `test_async_execute.py` 23 | inputs become `stream.open_channel()`; the output is an `OutputChannel` whose `result()` the test reads |
| `Channel(` | 42 | 329 | `test_native_async_operators.py` 95, `test_node_async_execute.py` 51, `test_async_execute.py` 48 | a helper (`tests/stream_helpers.py`) opens a channel on a stream and reads an output channel's result |
| `input_pipeline_hashes` | 17 | 84 | `test_native_async_operators.py` 42 | removed |
| `node_outputs` | 4 | 5 | `test_core/alignment/records.py` 2 | read the stream |

## Each node kind

### Source job node

- **Sync**: returns the output stream over `F(declared(bound.as_table(all columns)))`.
  `declared` is `_DeclaredForm.apply`, which already serves a whole Arrow table.
- **Async**: sends that Arrow table's record batches, or one batch per poll for a
  `PollingSource`, which keeps streaming.
- A `PollingSource` polls as often as today, and a run checks its sources first (ITL-1102).

### Operator job node

- **Sync**: `process(*input_streams)`. The kept output, which is also the return value, is
  `_as_upstream_stream(F(result.as_table(...)), self)`.
  - `_store_output_stream` stores the same Arrow table.
  - REPLAY returns the stored stream in that form.
  - A result with no rows keeps `_zero_rows_in_row_order` (ITL-1131).
- **Async**: the operator writes into the node's output channel (barrier or streaming, per
  the table above).
  - At close, the node keeps `output.result()`, in arrival order as on `main`, and stores
    it in `LOG` mode.
  - REPLAY sends the stored Arrow table's batches and drains its inputs.
  - A run with no rows closes with no batch.
- Operators have no per-row error policy: a failure fails the node, as today.

### Function job node

The function is still called once per row. Everything around the call is Arrow data.

1. **Input as Arrow data.** For each input row, the node computes the base entry id from that
   row's slice of the Arrow table or record batch: the tag side with system tags, plus the
   input data hash. This is the same one-row preimage `_build_record_id_preimage` builds
   today, still per row (hashing is ~0.3% of the run). No datagram is made.
2. **Lookup as an Arrow table.** `get_cached_results` finds the same entries. The loader keeps
   the stored joined Arrow table and a map from base entry id to row, and hits are taken
   from it by row index.
3. **The call.** For each miss:
   - The parameters' values come from the input Arrow data. They are converted to Python
     once per column per Arrow table or record batch, through the data context's type
     converter: the conversion `Data` applies per row today.
   - The call keeps today's logger, invocation context, error policy, commit cadence
     (`commit_every_rows`, `_row_processed`) and database error handling.
   - Misses that share a base entry id are called once and the result reused, as the sync
     path's in-run lookup does today and as ITL-1062 asks of the async path.
4. **Results as Arrow data.**
   - Returned values are collected per output column and converted to Arrow once, under the
     declared output Arrow schema.
   - The per-row items stay per row: the output data hash, the source-info tokens and the
     datagram id (`uuid7`, stored as `__data_id`).
   - Pipeline records and result rows are written as Arrow tables per commit window, with
     today's rows, columns and values. Rows written before a failure stay written.
5. **Output.** In input order, assembled from hit rows and computed rows by row index:
   - a `None` result or a failed row gives no row;
   - two equal inputs give two rows;
   - the column order is the first output row's (developer decision 2).

   On the async path, rows keep today's order: stored hits first, then computed rows as
   they finish. That is also the order of `result()`, as on `main`. The node's own reads
   keep cache order on both paths (developer decision 3).
6. **Load-only**: the hits-only case, with the same `not_stored` count.
7. **The node's own reads** come from its kept stream, in cache order (developer decision 3).
8. **Stored values decoded as a class (ITL-881).** A `TypeIdentityError` must stop the run at
   the same point as today. Stage 3 first establishes where it is raised today. If at
   datagram construction, the node checks each user-class column of the stored Arrow table
   once, before it passes a row on.
9. **`EmptyData` inputs (ITL-605).** Stage 3 first establishes how such a row crosses an edge
   today, and keeps that behaviour. The stream carries those rows' cached content hashes
   beside its Arrow data, never as a column. ITL-605's materialization thunks later use the
   same record.

### Side-effect job node

- Delivery calls the user's function with one row's values, taken from the Arrow data.
- The output is the input's Arrow data, masked to the rows passed on.
- A load-only run passes its input on unchanged.

## Per-row concerns without a row transport

### Observers

The protocol keeps its per-row hooks and its event order. A hook now receives a **row
view**: one internal class, `RowView`, that satisfies `TagProtocol` or `DataProtocol` over an
Arrow table or record batch and a row index. It builds the datagram only when it is first
read.

- `RunCountingObserver` reads only the label and the `cached` flag, so it makes no row.
- The status and logging observers read tag values, so they make the tag datagram of the
  rows they report.
- A user's observer gets what it gets today.

Event order stays as today:
- sync: input order;
- async: stored hits first, then rows as they finish.

The async function node matches a row's input by row index, instead of by a
`_TAG_NODE_INPUT_REF` meta column.

**ITL-1220 (attachments, future).** A stream lists its attachments, since the stream owns
the description. They run over row views of each Arrow table or record batch, in row order,
where observers are told. A "halt" signal maps onto failing the output channel and
cancelling the run's task group.

### Error policy

A row can fail only in a user's function or a delivery, both per row. The node wraps each
call:
- `continue`: the failed row is left out by mask, counted, and reported through
  `on_data_crash` with its row view;
- `fail_fast`: the node raises at once;
- database errors and `TypeIdentityError` stop the run whatever the policy.

## How each rule holds

- **ITL-845, each row keeps its own source info.**
  - Streams and Arrow record batches keep the `_source_*` values row by row, and F touches no
    value. List-valued tokens after `Batch`, `GroupBy` or `MergeJoin` are kept too.
  - Source-info nullability is set once per complete Arrow table: a sync output, and
    `stream_from_batches` (`result()`, `to_stream()`).
  - It is never set per Arrow record batch, because two batches of one channel would then
    have two schemas.
- **ITL-1122, large types.**
  - An Arrow table is normalized once, where it becomes a stream.
  - Streams and channels pass Arrow data on by selecting, slicing or `take`, which keep
    types.
  - Function results are converted under the declared output schema, which is large.
  - Nothing is normalized per row or twice. F step 3 keeps the one observable effect of
    today's per-row normalization.
- **Async equals sync.**
  - An operator reads its description from a stream on both paths, and a barrier operator
    runs `static_process` on streams on both.
  - Validation, input order, key checks and zero-row results are one implementation.
  - Async results equal the sync rows as a multiset, with columns matched by name; row order is
    not compared (developer, 2026-11-22).
  - `test_async_sync_equivalence` stays the check, with the standing guards below.
- **ITL-1133 and ITL-1131, one column order.**
  - F places every stream's transport columns in row order, and `arrow_schema()` is in the
    same order.
  - An operator node keeps and stores `F` of its result, sync and async.
- **Row order.** Each node passes on its rows in the order its successors receive them today:
  - a source: its Arrow table's order;
  - an operator: its result's order;
  - a function node (sync): input order;
  - a held function node: cache order;
  - a side-effect node: delivery order;
  - async: arrival or match order, in flight and in what a node keeps and stores, as on
    `main` (accepted by the developer).
- **Staleness.**
  - Every node calls `_update_modified_time` at the same points and in the same order.
  - A channel has no modified time; its stream does.
  - The ITL-1095 and ITL-1121 tests run at each stage.
- **Run summary.** The counts come from:
  - the counting observer, which still sees one event per row;
  - `last_run_not_stored`;
  - held counts, read without rows.

## What this closes (ITL-858)

| Case | Status on main | Closed by | Stage |
|---|---|---|---|
| Key types on an empty input, `Join` / `SemiJoin` (ITL-856) | open | checks from `channel.stream.arrow_schema()` before the first batch | 5 |
| Async column order (`ASYNC_ORDER_XFAIL`, ITL-1249, `tests/test_core/operators/test_several_items.py:211`) | strict xfail | not a goal or a gate (developer: column order is not critical). It may pass as a side effect; if so, the xfail is removed | – |
| Data context of an async input (ITL-844) | open | `channel.stream.data_context_key` | 5 |
| A row of another data context (ITL-951) | sync: whole-input error; async: answered from the cache | whole-input error before the first batch (ITL-858 decision 9) | 5 |
| Zero-row results, async (ITL-865) | narrow fix | `to_stream()` of an empty input is the typed zero-row stream | 5 |
| System-tag names after a non-source input, async (ITL-906) | narrow fix through `input_pipeline_hashes` | each input's `stream.pipeline_hash()` | 5 |
| Zero-row read-back types (ITL-912) | fixed (`typed_zero_row_table`) | the same derivation, now `arrow_schema()` | 2b, 5 |
| Operator state written at validation (`SemiJoin`, `Index`) | present | read from the input's stream | 5 |
| Duplicate function calls on async (ITL-1062) | open | misses deduplicated by base entry id | 5 |
| A loaded node's description (ITL-858 decision 7) | – | saved Arrow type texts (ITL-640) | 2a |

Standing guards:
- **Declared against materialized** (stage 2a, before anything depends on it):
  `stream.arrow_schema()` equals the materialized Arrow table's schema, with the two stated
  exceptions. This holds for every operator, source kind and function node, sync and async.
- **In `test_async_sync_equivalence.py` (stage 5)**: one registry of every operator class
  with an `async_execute`. The test fails when a class is missing from it.
- **Four input shapes per case**: rows; zero rows on every input; zero rows on one input;
  inputs from an operator instead of a root source.
- **Batch layouts**: one Arrow record batch, one row per batch, and mixed sizes.

## What must stay byte-identical, and how it is proven

This section applies the governing rule. Everything below holds on the sync path against
pinned `main`, byte for byte. On the async path it holds against the branch's sync path as a
multiset of rows, with columns matched by name. The rows of "Every difference from `main`" are
the only exceptions, each by name.

### Must not change

1. **Every hash.** `generate_golden_hashes.py --check` matches. Per node, this covers:
   - content hash and pipeline hash;
   - record ids;
   - base and pipeline entry ids;
   - output data hashes;
   - source-info tokens;
   - system-tag column names and values;
   - folded values (`batch_v1:` / `group_v1:`).

   Nothing about the transport enters a hash (ITL-858 decision 3).
2. **Every stored table**: the function pipeline and result tables, the operator cache
   (`odb_v1`), the side-effect log (`tdb_v1`) and the cached source (`sdb_v1`). For each:
   path, column names and order, types, nullability, field metadata, values, and the row
   order written.
3. **Every output a user reads.** For every node: `as_table(all_info=True)`, the `iter_data()`
   rows, the `head()` and `run()` views with their status and notes, and `as_source()` /
   `DerivedSource`.
4. **Every successor's input**: the Arrow data each operator, function node and side-effect
   node receives. Columns, order, types, nullability, metadata, values and row order.
5. **Run behaviour.**
   - `RunSummary` and `NodeRunSummary.held`.
   - The warnings issued.
   - Every error raised, at the point it is raised today.
   - The observer event sequence, and the values an observer reads.
   - What logging capture records.
   - Which nodes a second run holds.

Not covered:
- Arrow chunk and record-batch boundaries, datagram object identity, and timing.
- By design, the changed signatures, which are the breaking change.

### Proof

1. **Equality tests**, kept permanently (`tests/test_pipeline/test_stream_transport_equivalence.py`).
   `R` and `ArrowTableStream.iter_data` are the oracle. Each comparison is
   `Table.equals(check_metadata=True)` plus field-by-field schema equality, on combined
   chunks.
   - **L0 (idempotence)**: an output stream `S` gives `R(rows of S) == S.as_table()` on the
     transport columns.
   - **L1 (operators)**: an operator node's output equals `R(list(result.iter_data()))`, for
     every exported operator and for REPLAY.
   - **L2 (sources)**: for each source kind bound to a slot, with and without a declared
     form, the output equals `R(list(node.iter_data()))`. The kinds are in-memory, CSV,
     Delta, SQLite, `CachedSource`, `DerivedSource`, `PollingSource` (counting polls) and
     SpiralDB through its fakes. PostgreSQL runs in its own job.
   - **L3 (entry ids)**: per input row, the id from the Arrow slice equals
     `compute_base_entry_id(tag, data)`, also for a stream outside a job.
   - **L4 (function node)**:
     - the values given to the function equal today's `function_input(...).as_dict()`
       values (`==`, same types);
     - the results equal `R` of today's result datagrams;
     - the output equals `R` of today's output rows, both with the first row a hit and with
       it a miss.
   - **L5 (held)**: a held node's output stream and count equal today's.
   - **L6 (async)**: for every node kind and every operator (streaming and barrier),
     `output.result()` equals the sync output stream's Arrow table as a multiset of rows, with
     columns matched by name. It runs with the three batch layouts and with a predecessor
     that streams in match order. It also covers `Join`'s schema-level refusals (the same
     error type and message as sync) and ITL-1355's non-match cases under streaming:
     - a shared tag that differs;
     - NaN and `-0.0` / `0.0` in a shared column (they match; the first input's bits are
       kept);
     - inputs of different data contexts;
     - rows held until close, and released early once one input closes.

     Each is compared with sync as a multiset, and the warning is checked to be issued once per
     operator and set of columns.
     `Batch` and order-dependent `PolarsFilter` predicates behind an out-of-order input, and
     their descendants, are checked instead by per-node consistency over their recorded
     arrivals (see the differential), as the developer accepted.
   - **L7 (descriptions)**: the declared-against-materialized guard.
   - **L8 (channels)**: unit tests of `InputChannel`, `OutputChannel` and the conversions,
     one per row of the semantics table:
     - retain on and off;
     - late readers;
     - fan-out with a slow reader;
     - a reader that closes early;
     - reading a node while it runs (zero rows, `not_computed`, no wait);
     - identity of `result()` and `to_stream()`, with and without a producer;
     - producer failure and cancellation (readers raise `UpstreamFailedError`, and the run
       raises the producer's error);
     - double close;
     - zero rows;
     - `open_channel()` twice on one stream.

   The corpus is the several-items family (`tests/test_core/operators/several_items.py`).
   It adds:
   - logical types and user classes;
   - `null`-typed, list, struct and nullable columns;
   - zero, one and many rows;
   - duplicate inputs;
   - a function returning `None`;
   - ephemeral `EmptyData`;
   - failed rows under both policies;
   - a loaded node;
   - a stream outside the job;
   - fan-out;
   - a null source-info token.
2. **Existing suites**, run targeted as each stage lands:
   - `test_async_sync_equivalence`, `test_order_invariance`;
   - the operator, function and side-effect node tests;
   - `test_pipeline_job` and the observer tests;
   - the ITL-1131/ITL-1133 column-order tests;
   - the ITL-1095/ITL-1121 staleness and load-only tests;
   - the ITL-1075 deadlock tests;
   - the run summary tests;
   - the golden check.
3. **End-to-end differential: the acceptance gate of every stage**, stage 1 included. A
   script in `docs/dev/scripts/itl_1347/` builds fixture pipelines from
   `docs/tutorial/workshop_data.py`, the corpus, and the arrival-order graphs below. Each
   pipeline runs cold, warm, held and load-only, on both orchestrators.
   - **Sync on the branch against sync on pinned `main`**: byte for byte, with no exception.
   - **Async on the branch against sync on the branch**: equal as multisets of rows (row
     order ignored, duplicates counted), with columns matched by name. The rows of "Every
     difference from `main`" are excluded, each by name only.
   - **Async on the branch against async on `main`**: equal as multisets. Every difference
     beyond that must be a listed row.
   - **Arrival-dependent results and everything downstream of them.** An accepted
     arrival-dependent node is a `Batch`, or a `PolarsFilter` with an order-dependent
     predicate, whose input arrives in another order than on sync in **either** async run:
     the branch's or `main`'s. Its rows legitimately
     differ from sync, and so do the rows of every node downstream of it. The differential
     determines that set mechanically, from the job's graph: the arrival-dependent node and
     all its descendants. That set, and nothing else, is excluded from the row comparison
     against sync and against `main`'s async run. Instead, every node in the set is checked,
     in each async run separately (the branch's and `main`'s), against a sync recomputation
     over exactly the inputs it received in that run:
     - **recorded arrivals**: the differential records, for each node in the set, the rows of
       each of its inputs in the order the node received them on the async path (it wraps
       the node's input channels; the library is not changed);
     - **per-node consistency**: the node is computed again on the sync path of pinned
       `main` (not the branch's), over those inputs as Arrow tables in that recorded order.
       Its async output must equal that recomputation as a multiset of rows, apart from the
       named differences. Using `main`'s operator catches a branch defect that shows only on
       an arrival order the sync run never produces. For a `Batch` this fixes the groups to the
       consecutive `batch_size` segments of its own arrival sequence, with their boundaries
       and member order (lists are compared in order). Interleaved groups such as [A, C] and
       [B, D] from arrivals A, B, C, D fail. For an order-dependent `PolarsFilter` it fixes
       the kept rows to exactly those the predicate selects over the arrival sequence: no
       selected row missing, none extra;
     - **every arrival-dependent descendant too**: a later `Batch` or order-dependent
       `PolarsFilter` in the set (for example behind a concurrent function node fed by the
       first `Batch`) is checked against its own recorded arrivals in the same way, never
       against an order the sync recomputation would choose. Each node's check uses only
       its own recorded inputs, so a valid arrival order anywhere in the set passes;
     - **edge conservation**: for every edge into or inside the set, the node's recorded input
       rows equal its producer's output rows in the same run, as a multiset with
       multiplicity. So a row lost or duplicated between two excluded nodes fails, even
       though each node agrees with its own recomputation;
     - **everything outside the set** is compared as usual.

     This is the complete statement of the gate. The stage 0 harness (`docs/dev/scripts/itl_1347/`)
     implements the comparisons stages 1–4 need; stage 5 adds the recorded-arrival checks
     above (its README lists them).

     A difference anywhere else fails the gate.
   - **What it dumps and compares**: every stored table (Arrow IPC, with Delta version
     counts and per-version row sets), every node's `as_table(all_info=True)`, the
     `RunSummary`, the errors raised (type and message), the warnings, and a recording
     observer's events with the values it read.
   - **Polars threads**: it runs twice, with Polars at one thread (`POLARS_MAX_THREADS=1`,
     set before import, in a subprocess) and at the default.
   - **A stage merges only when its differential output is clean.** That output goes on the
     stage's PR.
4. **Async graphs where arrival order differs from sync order.** The equivalence tests feed
   operators from sources only, so arrival order always equals sync order there, and
   `Batch`'s grouping by arrival went unseen. New cases (L6, and the differential) put a concurrent function
   node in front of the operator under test. The function's delay is set per row (it sleeps
   longer for earlier rows), and the order is forced by awaiting per-row events in a
   scripted order, so the run is deterministic. The function node feeds:
   - `Batch` (`batch_size=2` and `0`);
   - `Join` and `MergeJoin` (the function node on one side);
   - `GroupBy`;
   - `PolarsFilter` with an order-dependent predicate;
   - `Rejoin` of two branches of one source, one branch through the function node;
   - a `Select` operator node in `LOG` mode, so its stored table is checked.

   Each case asserts the rows equal sync as a multiset. `Batch` and the order-dependent
   `PolarsFilter`, and their descendants, are checked as above: per-node consistency over
   recorded arrivals. The same holds against `main`'s async run. The `Rejoin`
   case runs ITL-1355's non-match cases with the late branch behind the function node:
   - a differing shared value;
   - NaN and `-0.0` / `0.0` in a shared column;
   - a different data context.

   It compares with sync as a multiset, and checks the warning once.

## Migration and staging

### On the integration branch

The work lands on `eywalker/job-speed`, which the developer merges into `main` as one PR.
So each signature changes directly in the stage that needs it, with no thin forms of old
signatures kept between stages (CLAUDE.md: no compatibility shims). `main` still sees one
breaking change, at the branch merge (decision 7).

### Stages, with the ITL-858 work merged in

The savings below are estimates from the profile's shares; stage 0 replaces them with
measurements before stage 1 merges. The profile's unit costs:
- about 26 s of datagram building over 121,139 rows (about **0.21 ms per row**);
- about 14 s of rebuilding.

| Stage | What | ITL-858 part | Proof | Expected saving, warm sync run of 44.7 s |
|---|---|---|---|---|
| 0 | Measurement only: datagrams per node kind, `R` calls and time by caller, edges per node, on a tutorial-data pipeline of the workshop's shape | declared against materialized Arrow schema, measured for every operator and source | – | 0 s |
| 1 | **First small step**: the sync orchestrator reuses an operator node's kept Arrow table, already `R` of exactly the rows it passes on, for each successor (held operator nodes included). Held counts are read without rows. No signature changes | – | by construction; one test per edge | about **4–8 s** |
| 2a | Sources, sync: `arrow_schema()` on `StreamBase`; each node's `execute` returns its output stream; held nodes give their output stream; `node_outputs` holds streams; sources return `F` of their Arrow table | `arrow_schema()` on every stream; the declared-against-materialized guard; loaded nodes describe themselves | L0, L2, L5, L7 | with 2b, about **14–20 s** |
| 2b | Operators, sync: kept output and return value `F(result.as_table())`, and REPLAY likewise | – | L0, L1, L5 | (with 2a) |
| 3 | Function nodes, sync: input from the input stream's Arrow table; entry ids from slices; hits from the stored Arrow table; calls on values from Arrow data; results per column; row views for observers; own reads from the kept stream | – | L3, L4, differential | about **8–14 s** |
| 4 | Side-effect nodes, sync: masked pass-through, load-only pass-through, held stream | – | L0, differential | small |
| 5 | Async: `InputChannel`, `OutputChannel`, the conversions and `open_channel()`; every `async_execute` on input channels and an output channel; barrier operators (`GroupBy`, `PolarsFilter`, `Batch(0)`) by `to_stream()`; streaming `Join`, `MergeJoin`, `Rejoin` (ITL-1355's rule: match on every shared column, matched rows removed, early release, the warning per batch) and `SemiJoin` (vectorized symmetric hash joins); `Batch` streaming by arrival as on `main`; key checks before the first batch; per-row operators per Arrow record batch; validation state, `input_pipeline_hashes` and `BroadcastChannel` removed; function node async with dedup | ITL-856, ITL-844/951 async, ITL-865/906 general form, ITL-1062; standing guards | L6, L7, L8, `test_async_sync_equivalence`, differential | the same order of gain for async runs |

The ranges overlap:
- the bound for stages 1–4 is the ~40 s (~90%) the profile puts on conversions;
- the floor is the plain-stream time (1.9 s) plus the lookups and writes a job adds;
- a warm sync `job.run()` of about 3–6 s is the target after stage 3.

The `LazyModule` cost (42.9 million `__getattr__` calls; 45.1 s → 40.7 s in the profile's
experiment) is a separate fix in this round.

Each stage is one issue and one PR against `eywalker/job-speed`, with:
- its equality tests;
- the targeted suites;
- the golden check;
- the differential, which is its acceptance gate.

The differential must come out clean: sync identical to pinned `main`, and async identical to
the branch's sync run except the named rows. A stage that introduces a difference not listed
in "Every difference from `main`" does not merge. The difference is either removed, or added
as a row with its bug and reproducer, for the developer's approval.

Benchmarks follow the round's rules.

### Release note (for the merge into `main`)

In `changelog.d/<issue>.md`, under `### Breaking changes and migration`:

> #### Job nodes and operators exchange streams and channels of Arrow data, not rows
>
> Code that drives nodes or operators directly changes. These stay unchanged: pipelines,
> jobs, `job.run()`, `run_async()`, sources, operators used in pipelines, and every stored
> table and hash.
>
> - A node's `execute()` returns its output stream, not a list of `(Tag, Data)` pairs. Read
>   rows with `iter_data()`, or the Arrow table with `as_table()`.
> - `async_execute()` of nodes, operators and function pods takes input channels and an
>   output channel (`orcapod.channels.InputChannel`, `OutputChannel`) carrying Arrow record
>   batches, instead of row channels.
>   - Open an input channel on a stream with `stream.open_channel()`; its description is
>     `channel.stream`.
>   - An output channel's `result()` is the output stream, once closed.
> - `BroadcastChannel` is removed. An `OutputChannel` with several readers replaces it.
> - The `input_pipeline_hashes` argument of operators' `async_execute()` is removed: each
>   input's stream states its own `pipeline_hash()`.
> - A side-effect node's `async_execute()` takes one input channel, not a list of one.
> - `OrchestratorResult.node_outputs` maps each node to its output stream.
> - The orchestrators' `held_rows=` argument is now `held=`, and takes streams.
> - `StreamProtocol` has two new members, `arrow_schema()` and `open_channel()`.
>   `StreamBase` provides them. A class that implements the protocol without `StreamBase`
>   must add them.

## Risks and unknowns

| Risk | What could go wrong | Mitigation | How we would detect it |
|---|---|---|---|
| **Memory with retained output** | `retain=True` is the default (`materialize_results=True`), so whole outputs are held as Arrow tables. During the transition a node could hold both its old rows and its new Arrow table. A large source, fully retained, could exceed today's footprint | A retained output channel's `result()` is the node's kept output: one copy, enforced by a test that they are the same object. Sources retain only when `materialize_results=True`. Arrow tables are far smaller than datagram objects | Stage 0 records peak RSS (`resource.getrusage`) for the baseline; every stage's benchmark reports it before and after |
| **Back-pressure and deadlock cycles** | Channels now count Arrow record batches. Diamonds, `SemiJoin` over one source, `Join`'s internal chain and slow fan-out readers could block a producer, as ITL-1075 did with rows | The read-to-the-end rule and `close()` on input channels. `retain=True` on node outputs means `send` never waits. REPLAY and `SemiJoin` keep reading their inputs | The ITL-1075 deadlock tests, run with one-row batches and `buffer_size=1`, plus a diamond and fan-out matrix under a pytest timeout (L8) |
| **Error propagation** | `UpstreamFailedError` could become the error a run raises instead of the producer's own. A consumer could take a producer's failure as a normal end and store a partial result | `fail()` in the producer's `finally`. The failure selection skips `UpstreamFailedError`. `LOG` stores only after a complete run | L8, and the existing error tests (`test_async_error_policy.py`, `test_store_write_failure.py`), asserting the raised type and message equal `main`'s. The differential's failing-row cases |
| **Async ordering** | Arrow record batches change the granularity of what async nodes see, so arrival order can differ from `main`'s. `Batch` groups by arrival, so with record batches its groups may differ from `main`'s async groups too | Accepted by the developer (row order is not critical; `Batch` grouping by arrival is fine). Every other result is order-independent and compared as a multiset | L6 and the arrival-order graphs (as multisets; `Batch` and its descendants by per-node consistency over recorded arrivals) |
| **Observer `RowView` cost and fidelity** | With a status or logging observer attached, every row's tag datagram is built again, so part of the cost returns. A lazy view could also differ from a real datagram (`isinstance`, equality, `as_dict()` values, pickling in a logger) | A `RowView` builds the real datagram on first access and then delegates every call to it. ITL-1348 makes that construction cheaper | The differential's recording observer compares every value it reads. A benchmark row "warm run with a status observer" shows the remaining cost |
| **Test migration size** | Stage 5 touches 42 files with 171 `async_execute` lines, 329 `Channel(` lines and 84 `input_pipeline_hashes` lines, and stage 2a touches 18 files with 88 `execute` calls. A large diff is hard to review, and a test could be weakened by accident | A shared helper (`tests/stream_helpers.py`). Plumbing changes go in their own commit, before any behaviour change. Assertions are not edited, only how inputs are fed and outputs read | A per-file count of `assert` statements before and after, reported on the PR. The coordinator's full-suite run and CodeRabbit |
| **Estimates that stage 0 may disprove** | The savings come from profile shares that overlap. The datagram cost may sit mostly in function nodes' stored-hit loading, or in observers, rather than on edges. Stage 1's 4–8 s may be less | Stage 0 measures the split per node kind and per caller before stage 1 merges. If a stage's measured share differs from its estimate by more than half, the plan is revisited with the developer | Stage 0's numbers against the stage table. Each stage's before-and-after benchmark (best of 3, with the load average) |
| **Byte-identity drift in edge cases** | `null` columns, source-info flags, the function node's first-row column order, `EmptyData` rows, and the point where `TypeIdentityError` is raised | F's explicit steps. The stage 3 investigations named in "Function job node". No golden file is regenerated | L0–L7, the golden check, and the differential (stored tables byte for byte) |
| **Memory of a streaming join's held inputs** | A streaming `Join` or `MergeJoin` holds every row of both inputs in its hash tables until they close, as `main`'s streaming `Join` does with rows | The hash tables hold Arrow tables and key frames, not datagrams, so they are smaller than `main`'s row buffers | Peak RSS of async runs against `main`'s async run, in stage 0 and stage 5 |
| **Held unmatched rows (`Rejoin`)** | A streaming `Rejoin` holds every unmatched row until it can rule out a match (developer: "unmatched rows will remain in 'to-be matched' in memory all the way until the operator channel closes"). Branches that drop or delay many rows keep them held longer | Matched rows leave both remaining sets at once. When one input closes, the other input's remaining rows are dropped, and the held set then only shrinks. Removal is a mask, with periodic compaction | Peak RSS in stage 5's async benchmark, with a branch that filters most rows out; a test that the held count falls to the closed side's unmatched rows when one input closes |
| **Non-unique system tags (`Rejoin`)** | With duplicate record ids, "match once" pairs duplicates in arrival order, which can differ between sync and async, so the rows differ even as multisets | Out of the design's scope: ITL-1356 makes record ids unique at the source. The equivalence tests use unique record ids, as the rule assumes | A test documents the duplicate case and is excluded, by name, from the set comparison |
| **Sync join order is Polars' unspecified order** (found while writing this) | The sync `Join`, `MergeJoin` and `Rejoin` take their row order from Polars' inner join without `maintain_order` (`join.py:94`, `rejoin.py`). Polars does not guarantee that order, so a Polars upgrade or thread count could reorder the stored rows of a join node on the sync path too. Hashes would not move; stored row order would | Not changed by this design. Making it explicit (`maintain_order="left_right"`) is a separate decision, because it could change today's stored row order (question 10) | Stage 0 compares the current order with `left_right` order over the corpus. The differential runs with the Polars thread count at 1 and at the default |
| **Hidden consumers of row lists** | Code outside the orchestrators reads `node_outputs` or held rows as lists: tests, alignment helpers, user code | The inventory in "Call sites", and the release note | The full suite, and the grep inventory re-run at each stage |

## Developer decisions (2026-11-22)

1. **`OrchestratorResult.node_outputs` holds each node's output.** With `NodeTable` gone, it
   holds each node's output stream, backed by an Arrow table; `stream.as_table()` gives the
   Arrow table. The developer confirms this refinement (question 8).
2. **A function node's first-row column order is kept exactly.** Every stage reproduces it
   (L4). Giving it one order is future work, ITL-1353.
3. **A function node's own reads keep cache order for now.**
4. **Stages run in order 0 → 1 → 2 → 3 → 4 → 5.** Stage 2 is split into 2a (sources) and 2b
   (operators), which can go to two workers in parallel.

Also from the developer:
- Async execution handles an object that wraps the channel and carries the stream's
  information.
- The protocol change is spelled out in detail.
- The data is named "Arrow table" and "Arrow record batch".
- The stream is primary and owns the description. A stream opens input channels for async
  compute, and an output channel closes into an ordinary stream. These are separate,
  consolidatable structures, with the conversions in one place (this design).
- Results are identical to `main` unless a named bug is fixed (the governing rule).
- Row order is not critical within a stream or channel. Async runs keep and store arrival
  order, as on `main`, and `Batch` grouping by arrival is fine on sync and async. Column order
  is not critical either: the async column order (ITL-1249) is not a goal.

Answers to the spec's questions (developer, 2026-11-22, approving the spec as a whole):

5. **Source-info nullability stays decided by a complete Arrow table's values.** It is the
   one column property `arrow_schema()` does not declare, so stored field flags and the
   operator node's record hashes stay as they are.
6. **Batch and buffer sizes**:
   - a source sends Arrow record batches of at most 65,536 rows;
   - an operator sends its result's own record batches;
   - an input channel queues 8 batches.
7. **Signatures change directly in each stage** on `eywalker/job-speed`, with no thin forms
   of the old ones. `main` sees one breaking change, at the branch merge.
8. **`node_outputs` holds each node's output stream** (refining decision 1).
9. **Resolved by ITL-1355.** `Rejoin` matches on every shared column with `Join`'s equality,
   never refuses on data, and drops unmatched rows. It lands on `main` separately; stage 5's
   streaming `Rejoin` builds on it. ITL-1356 makes record ids unique at the source.
10. **Polars' join order** is filed as ITL-1357, low priority, with a design-issue entry
    (`docs/dev/design_issues/ITL-1357-JRO1.md`). It is not part of this design. Stage 0
    measures `maintain_order="left_right"` against today's order, as a report only. A first
    check already shows that Polars' default order differs from `left_right` in a `MergeJoin`
    case (`join._inner_join`). So `left_right` does not reproduce today's order, and making
    the order explicit would be a result change for its own decision (report only).
11. **Async write order** keeps `main`'s completion order (row order is not critical).
12. **Bugs found while writing this:**
    - e6 is filed as ITL-1358, with a design-issue entry
      (`docs/dev/design_issues/ITL-1358-VST1.md`);
    - e7 is filed only once a test reproduces it; the developer files it;
    - e5 is accepted as documented behaviour, like `Batch`, with no fix.

**Differences approved as bug fixes**: e1 (ITL-856), e2 (ITL-951), e3 (ITL-1062) and e8 (the
async side-effect node drops the `__computed` meta column, as sync does).

**Each stage merges** into `eywalker/job-speed` only after the comparison against `main`
shows no change in any sync result. The coordinator verifies and merges.

## Future work (not designed here)

- One output column order for a function node, whatever its first row (ITL-1353).
- Source-info nullability declared by `arrow_schema()` (question 5).
- A `null` column through the job layer (ITL-1319).
- `Schema` carrying its Arrow schema (ITL-1307), which `arrow_schema()` then reads.
- Attachments on streams (ITL-1220) and materialization thunks (ITL-605), placed above.
- Batching the side-effect completion lookup (one `get_record_by_id` per row today).
- Polars data frames as a stream's backing, where an operator works in Polars.
