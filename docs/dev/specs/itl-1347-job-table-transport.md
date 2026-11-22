# Job nodes pass streams of Arrow data, not rows (ITL-1347, with ITL-858)

Status: design for the developer's review. No code changes with it. Written against
`origin/main` 8c6fc4f5 (2026-11-22); the work lands on the integration branch
`eywalker/job-speed`. Every implementation stage below becomes its own issue after approval.

Revisions, all on 2026-11-22:
- the ITL-858 input object was folded in, with the protocol change and its call sites;
- second revision: the developer's terminology ("Arrow table", "Arrow record batch"), and
  the unit passed between nodes is a stream that can act as a channel.

Terms used throughout:
- **Arrow table**: a `pyarrow.Table`.
- **Arrow record batch**: a `pyarrow.RecordBatch`.
- **Stream**: an Orcapod stream (`StreamProtocol`).

## The rule

Developer, 2026-11-22: Orcapod was always meant to keep data as Arrow (or Polars) tables as
long as possible and to avoid converting rows to objects and back. This spec makes that the
rule for the job layer as a whole.

- **The unit passed between job nodes is a stream.** A node, an operator and a function pod
  each receive streams and produce a stream, sync and async. The developer, 2026-11-22: "I
  always had this thought that it would make sense if streams could serve as a channel if
  possible."
- **A stream holds Arrow data, in one of two backings:**
  - an Arrow table (today's `ArrowTableStream`);
  - Arrow record batches fed through a channel: a producer writes them, and consumers read
    them as they arrive, with back-pressure.

  Either way the stream answers its description (schema, Arrow schema, keys, data context,
  pipeline hash) before any data.
- **A row exists only where a user's function is called**: a function pod's function, or a
  side effect's delivery. Even there, the node reads its input as Arrow data and writes its
  results as Arrow data. Only the call itself takes one row's values.
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

  This served the async orchestrator's per-row streaming, concurrent function calls,
  per-row observer hooks and the per-row error policy. Plain streams never adopted it.
- **November 2026: correctness fixes that kept the row protocol and made each row costlier.**
  - ITL-845: `_rows_to_table` keeps each row's source info.
  - ITL-1122: large-type normalization at stream construction, now paid three times per
    row datagram.
  - The rule that async output rows equal the sync rows, built from each datagram's Arrow
    table.
- **Channels carry rows and nothing else (ITL-858).** So async operators rediscover from
  rows what the sync path is told. Where rows cannot answer, the two paths diverged: key
  types on an empty input (ITL-856), column order (ITL-854, ITL-1249), data context
  (ITL-844, ITL-951), zero-row results (ITL-865, ITL-912), and system-tag names after a
  non-source input (ITL-906). Narrow fixes closed several of these; the general one is this
  design.

## What it costs

The ITL-1345 profile (workshop-scale notebook on 0.4.0rc1, local Delta copy, no network):

| Measure | Value |
|---|---|
| Same pipeline as plain streams | 1.9 s |
| As a `PipelineJob`, warm / cold `job.run()` | 44.7 s / 46.8 s, identical rows |
| Warm run: building a `(Tag, Data)` datagram per row passed between job nodes | ~58% (~26 s) |
| Warm run: rebuilding Arrow tables from those rows (`_rows_to_table`, `_materialize_as_stream`) | ~32% (~14 s) |
| Warm run: operator compute / reads / all hashing | ~8% / ~1% / ~0.3% |
| `Data.__init__` calls; `normalize_schema_to_large_types` | 121,139; 363,466 (three per row) |
| One source of 63,349 rows: `as_table()` / `list(iter_data())` | 0.29 s / 9.1 s |

About 90% of a warm run converts Arrow tables into rows and back.

### Where rows are made and unmade today

Sync orchestrator (`SyncPipelineOrchestrator.run`):

1. **Source job node**: `list(self.iter_data())`, one datagram per row. A narrower source
   given in the declared form gets a second datagram per row (`_declared_row`).
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
  5. On a complete Arrow table only, never on one Arrow record batch: flag each
     source-info field nullable only when a token is null (`_flag_source_info_nullability`).
- **Transport columns**: the columns a stream delivers between nodes. They are the columns
  of F: user tags, system tags, data and source info, in row order, with no context and no
  meta column.

## The design: one type, two backings

### The stream is the unit, and its description is its own members

A node, operator or pod receives streams and produces a stream. There is no separate
description protocol: the description is the stream's existing members, plus one new one.

| Member | Today | What it answers |
|---|---|---|
| `keys()`, `output_schema()` | on `StreamProtocol` | names and Python types, in key order |
| `data_context_key` | on every stream (`hashing_protocols.py:19`) | the stream's data context |
| `pipeline_hash()` | on `StreamProtocol` (`PipelineElementProtocol`) | as today |
| data-tags | `stream_data_tags(stream)`, from `keys()` | unchanged; no new member |
| `producer`, `upstreams` | on `StreamProtocol` | as today |
| **`arrow_schema()`** (new) | – | the Arrow schema of the transport columns: types, nullability and field metadata of the rows, in row order. It answers which system-tag and source-info columns are present, and that no meta column is |

None of these reads, computes or polls a row.

**Where `arrow_schema()` comes from now.** From `typed_zero_row_table`
(`core/streams/base.py:116`, ITL-912), put through F's steps 1, 2 and 4 and normalized to
large types as `typed_zero_row_input` does. That is exactly the schema of today's zero-row
input (`_zero_row_input`) without its context column.
- A source answers from the Arrow table or schema it holds.
- An operator node applies its operator to its inputs' zero-row Arrow tables, which
  processes no data.
- A function node answers from its input's tags and its declared outputs
  (`function_zero_row_table`).
- A loaded node answers from the Arrow type texts its saved file holds (ITL-640).

A stream that cannot type itself without rows answers from its Python schema, through the
data context's converter. These are an unbound slot, and a `PollingSource` that has never
been polled (ITL-1089). Such a stream says so through `arrow_types_declared` (a
`StreamBase` property, `False` only in these cases). A run checks its sources first
(ITL-1102), which polls them.

**ITL-1307 later.** When `Schema` carries its Arrow schema, `arrow_schema()` reads the two
schemas plus the system-tag and source-info fields, in one place (`StreamBase`).

Two exceptions keep today's bytes:
- **Source-info nullability** is decided by a complete Arrow table's values (F step 5), not
  declared. `arrow_schema()` states those fields nullable. Question 5 asks whether to keep
  this.
- **`null` columns**: rows carry `large_string`, while a zero-row Arrow table carries
  `null`, as today. ITL-1319 removes the difference.

### Backing 1: an Arrow table

This is `ArrowTableStream`, as today. A node's sync output, and every input on the sync
path, is an Arrow-table-backed stream.

- A node's output stream is `_as_upstream_stream(F(node's Arrow table), node)`. It takes
  the node's producer, upstreams, pipeline hash and data context, or is a `_RowsOfStream`
  for a node without a producer.
- With no rows, it is today's zero-row input (`_zero_row_input(node)`).
- That is exactly what each successor receives today, so a successor's input is unchanged
  by construction. A fan-out passes the same stream to every successor.

### Backing 2: Arrow record batches through a channel

One new class, `ChannelStream` (`core/streams/channel_stream.py`), a `StreamProtocol`.

- **Made by the orchestrator**, before any task starts, from the producing node:
  `ChannelStream.for_node(node, retain=..., buffer_size=...)`. The stream copies the node's
  description at once (keys, schemas, `arrow_schema()`, data context, pipeline hash,
  producer, upstreams), so every consumer knows it before the first Arrow record batch.
- **Written by the producing node** through `stream.writer`, a `BatchWriter`:
  - `send(batch)`: one Arrow record batch of the transport columns. The batch's schema is
    checked against `arrow_schema()`, with the two exceptions above. The check is once per
    batch, not per row, so it is always on (ITL-858 decision 4).
  - `close()`: the producer is done.
  - `fail(error)`: the producer failed.
- **Read by each consumer** through its own **reader**, `stream.reader()`. A reader is
  itself a stream: a view with the same description and identity and its own read
  position. The orchestrator makes one reader per successor, before tasks start, and passes
  that reader as the successor's input.
- **Read asynchronously** with `async for batch in input.async_iter_batches()`.
  - On a `ChannelStream` reader this yields Arrow record batches as they arrive, with back-pressure.
  - On an Arrow-table-backed stream it yields the Arrow table's own record batches.

  So an operator or node reads either backing the same way.
- **Read whole** with `await input.materialize()`: it waits for the producer's close and
  returns an Arrow-table-backed stream with the same description and identity. Its Arrow
  table is the batches concatenated, with F step 5 applied. On an Arrow-table-backed stream
  it returns the stream itself.

### New members on every stream

```python
class StreamProtocol(...):  # existing members unchanged
    def arrow_schema(self) -> pa.Schema: ...
    def iter_batches(self) -> Iterator[pa.RecordBatch]: ...
    def async_iter_batches(self) -> AsyncIterator[pa.RecordBatch]: ...
    async def materialize(self) -> StreamProtocol: ...
    @property
    def is_complete(self) -> bool: ...
```

- `iter_batches()`: the transport columns as Arrow record batches, synchronously. An
  Arrow-table-backed stream yields its Arrow table's batches. A `ChannelStream` yields only
  when it is complete (see the semantics table).
- `async_iter_batches()`: as above, asynchronously. A `ChannelStream` reader yields batches
  as they arrive.
- `materialize()`: the stream as an Arrow-table-backed stream; it waits for completion.
- `is_complete`: `True` for an Arrow-table-backed stream; `True` for a `ChannelStream` once
  its producer closed and its Arrow record batches are retained.

`StreamBase` implements all five for every existing stream kind from `as_table()`. A source,
an operator's stream, a node and a function pod's stream need no code of their own. Only
`ChannelStream` and `ArrowTableStream` override them.

### The hard semantics

These are the reasons ITL-858 first restricted the idea. Here each one is decided.

| Question | Rule |
|---|---|
| **Re-reading** | A `ChannelStream` made with `retain=True` keeps every Arrow record batch it delivered. Once its producer closes, it is complete: an ordinary, re-readable stream whose Arrow table is its batches concatenated (F step 5 applied once). `iter_data()`, `as_table()`, `iter_batches()` and `head()` all work on it, from any thread. With `retain=False` it keeps nothing. After close it is consumed, and a data read raises `StreamConsumedError` naming the producing node. |
| **When batches are retained** | `retain=True` for the output of every operator, function and side-effect node. Those nodes keep their output today (`_cached_output_stream`, `_cached_output_datas`, `_held_rows`), and the completed stream becomes that kept output, so nothing is held twice. `retain=True` for every node when `materialize_results=True` (the default), because `node_outputs` holds each node's output. `retain=False` only for a source node's output when `materialize_results=False`. Memory is then bounded by the readers' queues. |
| **Memory bound** | With `retain=False`: at most `buffer_size` Arrow record batches per reader. With `retain=True`: the node's whole output, which today's code already holds. A barrier operator's `materialize()` holds its whole input, as today. A held node's output is its kept Arrow table, as today. |
| **Several readers (fan-out)** | One `ChannelStream`, one reader per consumer, each with its own position. With `retain=False`, each reader has a bounded queue and `send` waits until every reader's queue has room, so the slowest reader sets the pace (today's `BroadcastChannel`). With `retain=True`, readers read the shared retained list at their own position and `send` never waits: the memory is the node's kept output in any case. |
| **Reading to the end (ITL-1075-AH1)** | Every reader reads to its end, or calls `reader.close()`, which unregisters it so the producer stops waiting for it. REPLAY reads and discards its inputs while it sends, as today. `SemiJoin` reads its left input while it collects its right one, as today. With `retain=True` no producer can block on an unread reader. With `retain=False` the ITL-1075 rule applies unchanged, and its tests run with one-row batches as well as full ones. |
| **Late readers** | A reader made after the first `send` replays the retained batches and then continues (`retain=True`). With `retain=False` it is refused (`ValueError`). The orchestrator makes every reader before any task starts. |
| **Sync reads of an open stream** | `as_table()`, `iter_data()`, `iter_batches()` and `content_hash()` (see identity) on a `ChannelStream` whose producer has not closed raise `StreamNotCompleteError`, naming the producing node. They never block, inside or outside a running event loop. A blocking read inside the loop would deadlock, because the producer runs on that loop; outside it, it would be a hidden wait. Description members always answer. |
| **`head()` and the preview** | They never compute, as today (ITL-991, ITL-912). On an open `ChannelStream` they show no rows, typed by `arrow_schema()`, with status `not_computed` and the note "still being produced". On a complete one they show its rows, which it holds. They never wait. |
| **Identity** | A `ChannelStream` and its readers answer identity exactly as today's successor input (`_as_upstream_stream`) does for the producing node: the producer and upstreams, `pipeline_hash()` the node's, and the data context the node's. For a node with a producer, `content_hash()` comes from producer and upstreams, as today, and needs no data. For a node without a producer (a loaded function node, a restored side-effect node), today's input is a `_RowsOfStream`, whose content hash is its Arrow table's hash. So it is available once the stream is complete, and raises while open. Nothing about the transport enters a hash: not `retain`, not buffer size, not batch boundaries, not the reader. |
| **Producer failure** | The producer's task calls `writer.fail(error)` (the node's `finally` does it, as it closes its output today). Every reader's next read raises `UpstreamFailedError` naming the producing node, never a normal end. So a barrier operator never computes over a partial input. The run raises the producer's own error, as today: `_single_failure` and `_single_database_error` skip `UpstreamFailedError`. |
| **Cancellation** | A cancelled producer fails its stream with the cancellation, and readers raise `UpstreamFailedError`. A cancelled reader unregisters, so the producer stops waiting for it. |
| **Close** | `close()` after the last `send` completes the stream. A second `close()` does nothing. `send` after `close` raises. A zero-row output is a close with no batch: the complete stream is the zero-row Arrow table typed by `arrow_schema()`, never by rows. |

### Sync and async with one type

- **Sync**: a node's `execute` receives Arrow-table-backed streams and returns one. The
  orchestrator passes each node's output stream to its successors as it is.
- **Async**: a node's `async_execute` receives readers (streams) and the writer of its own
  output stream. The orchestrator makes each node's output `ChannelStream`, and one reader
  per successor, before any task starts.
- **One operator body**:
  - A barrier operator materializes its inputs and calls `static_process` on them, which is
    exactly the sync path's call.
  - A per-row operator applies its Arrow-table rule to each Arrow record batch.
  - `Join` and `SemiJoin` check key types from `arrow_schema()` before the first batch.

## Compared with the two-protocol design (the first revision)

| | One stream type that can act as a channel (proposed) | `TableInput` + `BatchInput` (first revision) |
|---|---|---|
| Types an operator or node sees | one: `StreamProtocol` | two, plus a description protocol |
| Sync code over async data | runs unchanged on a complete stream (`static_process`, validation, `as_table()`) | needs `materialize()` into the other type first |
| Description | the stream's own members, plus `arrow_schema()` | a separate `StreamDescription` |
| A node's output | a stream, already its successors' input | `NodeTable`, wrapped per successor |
| `node_outputs` | complete streams backed by Arrow tables | `NodeTable`s |
| Complexity | in one class, `ChannelStream`: retention, readers, completion, failure. Five members on `StreamProtocol`, implemented once in `StreamBase` | in two small protocols, simpler each |
| Sync read of an open stream | possible to attempt; raises `StreamNotCompleteError` | impossible by type |
| Memory | explicit: `retain` decides; the default matches what today's code holds | the same, implicit |
| Hazard ITL-858 named | the line between an Arrow table and a queue blurs | avoided by type |
| Answer to the hazard | each state (open, complete, consumed, failed) has one defined behaviour, checked by tests (L8) | – |

Nothing blocks the stream form. Its costs are the state rules above and one class that
implements them, and the semantics table and L8 cover both. What it gains is one type: the
sync path's operator code serves the async path unchanged, and a node's output is already
what its successors read.

## Protocol change

All of these land on `eywalker/job-speed`, which reaches `main` as one merge, so `main`
sees one breaking change (question 7).

### `StreamProtocol` (`src/orcapod/protocols/core_protocols/streams.py:81`)

New members: `arrow_schema()`, `iter_batches()`, `async_iter_batches()`, `materialize()`,
`is_complete` (above). They are additive, and `StreamBase` implements them for every
existing stream. A third-party class that implements `StreamProtocol` without `StreamBase`
must add them. The release note says so.

### Node protocols (`src/orcapod/protocols/node_protocols.py`)

| Protocol | Before | After |
|---|---|---|
| `SourceNodeProtocol.execute` (:45) | `execute(self, *, observer=None) -> list[tuple[TagProtocol, DataProtocol]]` | `execute(self, *, observer: ExecutionObserverProtocol \| None = None) -> StreamProtocol` |
| `SourceNodeProtocol.async_execute` (:51) | `async_execute(self, output: WritableChannel[tuple[Tag, Data]], *, observer=None) -> None` | `async_execute(self, output: BatchWriter, *, observer=None) -> None` |
| `FunctionNodeProtocol.execute` (:84) | `execute(self, input_stream: StreamProtocol, *, observer, error_policy, run_id, load_only) -> list[tuple[Tag, Data]]` | `execute(self, input_stream: StreamProtocol, *, observer=None, error_policy: Literal["continue", "fail_fast"] = "continue", run_id: str \| None = None, load_only: bool = False) -> StreamProtocol` |
| `FunctionNodeProtocol.async_execute` (:94) | `async_execute(self, input_channel: ReadableChannel[...], output: WritableChannel[...], *, ...) -> None` | `async_execute(self, input_stream: StreamProtocol, output: BatchWriter, *, observer=None, error_policy="continue", run_id=None, load_only=False) -> None` |
| `OperatorNodeProtocol.execute` (:127) | `execute(self, *input_streams: StreamProtocol, observer=None, load_only=False) -> list[tuple[Tag, Data]]` | `execute(self, *input_streams: StreamProtocol, observer=None, load_only: bool = False) -> StreamProtocol` |
| `OperatorNodeProtocol.async_execute` (:134) | `async_execute(self, inputs: Sequence[ReadableChannel[...]], output: WritableChannel[...], *, observer, load_only) -> None` | `async_execute(self, input_streams: Sequence[StreamProtocol], output: BatchWriter, *, observer=None, load_only=False) -> None` |
| `SideEffectNodeProtocol.execute` (:158) | `execute(self, input_stream: StreamProtocol, *, observer, run_id, load_only) -> list[tuple[Tag, Data]]` | `execute(self, input_stream: StreamProtocol, *, observer=None, run_id=None, load_only=False) -> StreamProtocol` |
| `SideEffectNodeProtocol.async_execute` (:167) | `async_execute(self, inputs: Sequence[ReadableChannel[...]], output: WritableChannel[...], *, observer, run_id, load_only) -> None` | `async_execute(self, input_stream: StreamProtocol, output: BatchWriter, *, observer=None, run_id=None, load_only=False) -> None` (one input, as it always had) |

The sync input type does not change (`StreamProtocol`); only what is returned changes. Every
`execute(stream)` call in the tests keeps its argument.

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
| `StaticOutputOperatorPod.async_execute` (`static_output_pod.py:728`) and every override | `async_execute(self, inputs: Sequence[ReadableChannel[tuple[Tag, Data]]], output: WritableChannel[tuple[Tag, Data]], *, input_pipeline_hashes: Sequence[ContentHash]) -> None` | `async_execute(self, input_streams: Sequence[StreamProtocol], output: BatchWriter) -> None` |
| `FunctionPod.async_execute` (`function_pod.py:660`) | `async_execute(self, inputs: Sequence[ReadableChannel[...]], output: WritableChannel[...], pipeline_config=None, *, observer, error_policy, run_id) -> None` | `async_execute(self, input_streams: Sequence[StreamProtocol], output: BatchWriter, pipeline_config: PipelineConfig \| None = None, *, observer=None, error_policy="continue", run_id=None) -> None` |

`input_pipeline_hashes` disappears: an operator reads `input_streams[i].pipeline_hash()`.
That removes its 65 uses in 13 source files and 84 lines in 17 test files.

**Implemented by:** the barrier default (`static_output_pod.py:728`), `UnaryOperator`
(`base.py:202`), `BinaryOperator` (`base.py:316`), and the 16 overrides:
- `add_tags.py:336`, `drop_nulls.py:322`, `batch.py:211`, `semijoin.py:308`, `join.py:669`,
  `index.py:448`, `pick.py:444`, `rejoin.py:416`;
- `column_selection.py:152`, `:305`, `:444`, `:598`, `:825`;
- `mappers.py:263`, `:538`, `:825`.

`FunctionPod` (`function_pod.py:660`).

**Called by:** `OperatorJobNode.async_execute` (`operator_node.py:2244`), `Rejoin`
(`rejoin.py:428`), the function node (`function_node.py:4467`, `:4485`), and tests.

What each body becomes:
- **Barrier**: `streams = await asyncio.gather(*(s.materialize() for s in input_streams))`,
  then `static_process(*streams)`, then it writes the result's Arrow record batches. This
  is correct for zero rows and for system-tag names.
- **Per-row operators** apply their Arrow-table rule per Arrow record batch.
- **`Join` and `SemiJoin`** check key types from `arrow_schema()` before the first batch
  (ITL-856), and order inputs with the method the sync path uses.
- **Validation state goes**: `SemiJoin._validated_common_keys` (`semijoin.py:287`, `:306`,
  `:381`) and `Index._element_arrow_type_fallback` (`index.py:82`, `:126`).

### Channels (`src/orcapod/channels.py`)

| Class | Before | After |
|---|---|---|
| `ReadableChannel[T]` (:46), `WritableChannel[T]` (:63) | protocols in every node, operator and pod signature | generic queue protocols, in no node, operator or pod signature; used inside nodes (a function node's row queue to its pod) |
| `Channel[T]` (:172) | the edge for one successor | generic bounded queue; a `ChannelStream` reader's queue, and queues inside nodes |
| `BroadcastChannel[T]` (:247) | the edge for fan-out | removed: a `ChannelStream` with several readers is the fan-out. No other user |
| `ChannelClosed` (:36) | raised by `receive` on a closed channel | unchanged for the generic queues |
| **new** `BatchWriter` (protocol) | – | `send(batch: pa.RecordBatch) -> None` (async), `close() -> None` (async), `fail(error: BaseException) -> None`, `stream -> ChannelStream` |
| **new** errors | – | `StreamNotCompleteError`, `StreamConsumedError`, `UpstreamFailedError`, in `orcapod.errors` |

`ChannelStream` lives in `core/streams/channel_stream.py`, beside `ArrowTableStream`.

### Results and orchestrators

| | Before | After |
|---|---|---|
| `OrchestratorResult.node_outputs` (`result.py:21`) | `dict[Any, list[tuple[Tag, Data]]]` | `dict[Any, StreamProtocol]`: each node's complete output stream, backed by an Arrow table. This refines developer decision 1 ("node tables"), since `NodeTable` is gone; `stream.as_table()` gives the Arrow table |
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
| :121–128 | `_gather_upstream_multi`, one `_materialize_as_stream` per input, `node.execute(*input_streams, ...)` | same order rule (`node.upstreams`); `node.execute(*(outputs[p] for p in ordered), ...)` |
| :130–141 | as :108 (side-effect node) | as :108 |
| :173, :187 | `_gather_upstream*` return row buffers | return output streams; order logic unchanged |
| :206 | `_materialize_as_stream` | removed: each output stream already is the successor's input |
| :225 | `_gc_buffers` | unchanged logic over `outputs` |
| :155 | `OrchestratorResult(node_outputs=buffers)` | `OrchestratorResult(node_outputs=outputs)` |

### Async orchestrator (`src/orcapod/pipeline/async_orchestrator.py`)

| Line | Today | Becomes |
|---|---|---|
| :77, :158, :196 | `held_rows=` | `held=` |
| :230–250 | `Channel` per single edge, `BroadcastChannel` + `add_reader()` for fan-out, a sink `Channel` per terminal node | one `ChannelStream.for_node(node, retain=..., buffer_size=...)` per node that runs; one `reader()` per successor; a terminal node's stream needs no reader |
| :258–265, :799 | `_CollectingWriter` copies every row | removed: `node_outputs` holds the complete retained streams |
| :267–275, :645, :651 | held node: `_drain` each input, `_send_held(rows, writer)` | a held node's output is its kept stream (Arrow-table-backed); successors read it with `async_iter_batches()`. `_drain` and `_send_held` are removed (the plan gives a held node no input edge) |
| :281 | `node.async_execute(writer, ...)` (source) | `node.async_execute(stream.writer, ...)` |
| :294 | `node.async_execute(input_reader, writer, ...)` (function) | `node.async_execute(reader_of(pred), stream.writer, ...)` |
| :320 | `node.async_execute(input_readers, writer, ...)` (operator; order by `node.upstreams`) | `node.async_execute([reader_of(p) for p in ordered], stream.writer, ...)` |
| :340 | `node.async_execute([input_reader], writer, ...)` (side effect) | `node.async_execute(reader_of(pred), stream.writer, ...)` |
| :708 `_single_failure`, :742 `_single_database_error` | pick the error the run raises | unchanged, skipping `UpstreamFailedError` |

### Nodes, operators and pods

| Site | Today | Becomes |
|---|---|---|
| `source_node.py:1191` `execute` | `list(self.iter_data())` | the output stream over `F(declared(bound.as_table(all columns)))` |
| `source_node.py:1216` `async_execute` | sends each row of `async_iter_data()` | writes the Arrow table's record batches (one read), or one batch per poll for a `PollingSource` |
| `operator_node.py:1647` `execute` | `process`, `list(result.iter_data())`, `_kept_output` | `process`; kept output and return value `_as_upstream_stream(F(result.as_table(...)), self)` |
| `operator_node.py:2133` `async_execute` | `intermediate` row `Channel` (:2218), `collected`, `_materialize_to_stream` | the operator writes into the node's own output stream (no intermediate). The node's kept output is that stream once complete; it stores it in `LOG` mode |
| `operator_node.py:2241–2250` | `hashes = [...]`; `operator.async_execute(taps, intermediate.writer, input_pipeline_hashes=hashes)` | `operator.async_execute(input_streams, output)` |
| `operator_node.py:109` `_InputTap`, `:2275` `_materialize_upstream(..., zero_rows=True)` | taps record rows to rebuild empty inputs | removed: an empty input materializes from its `arrow_schema()` |
| `function_node.py:2485` `execute`, `:2008` `_execute_load_only` | iterate `input_stream.iter_data()` | read `input_stream.iter_batches()`: entry ids from slices, hits from the stored Arrow table, calls on misses; return the output stream |
| `function_node.py:4130` `async_execute` | three stages over row channels | reads `async_iter_batches()`; hits are written as batches; misses go row by row to the pod's row executor; results are written as batches |
| `function_node.py:4259`, `:4262` | `compute_channel`, `result_channel`: row `Channel`s to and from the pod | kept as row queues inside the node: rows exist only for misses |
| `function_node.py:4467`, `:4485` | `execution_pod.async_execute([compute_channel.reader], result_channel.writer, ...)` | the pod's per-row executor (today's semaphore and hooks, split out so the node and the pod's own `async_execute` share it) |
| `function_node.py:4055` `FunctionJobNode.run` | `_ = self.execute(self._input_stream, observer=counter, ...)` | unchanged call; returns a stream |
| `side_effects.py:1224`, `:1455` | iterate and pass on rows | rows for delivery only; the output stream is the input's Arrow data masked to the rows passed on |
| `side_effects.py:1428` `_run_alone` | `self.execute(self._input_stream, ...)` | unchanged call |
| `loaded_side_effects.py:284`, `:319` | `super().execute(...)`, `super().async_execute(...)` | same forwarding, new signatures |
| `rejoin.py:428` | `StaticOutputOperatorPod.async_execute(self, inputs, output, input_pipeline_hashes=...)` | `StaticOutputOperatorPod.async_execute(self, input_streams, output)` |
| `join.py:815`, `:834` | row `Channel` chain inside an N-ary join | a `ChannelStream` chain; each intermediate described by the pair's `zero_row_result` |
| `static_output_pod.py:689`, `:543`, `:570` (`_materialize_input_stream`, `_materialize_to_stream`, `_materialize_upstream`) | rebuild rows into streams | removed; `materialize()` and the output streams replace them |
| `static_output_pod.py:304` `_rows_to_table` | used on every edge | moves to the tests as the oracle `R` once nothing in the library uses it |

### Plan and held nodes

| Site | Today | Becomes |
|---|---|---|
| `node_run.py:341–345` | `held_rows = {node: list(node.iter_data()) ...}` | `held = {node: node.output_stream() ...}`: an operator's kept stream; a function node's held rows as an Arrow-table-backed stream, in cache order (developer decision 3); a side effect's held stream |
| `node_run.py:346` | `held_counts = {node: sum(1 for _ in node.iter_data()) ...}` | `held[node].as_table().num_rows`, or a function node's run entry counts |
| `node_run.py:432`, `job.py:1000`, `job.py:1071` | `held_rows=plan.held_rows` | `held=plan.held` |
| `run_summary.py:532` | reads held counts | unchanged |

### Tests that call these directly

Counted on `origin/main`, by file, excluding database cursors' own `execute`:

| What | Files | Lines | Largest | Change |
|---|---|---|---|---|
| node `.execute(` | 18 | 88 | `test_ephemeral_result.py` 32, `test_side_effect_pod.py` 13, `test_node_execute.py` 8, `test_node_protocols.py` 7 | arguments unchanged; a test that reads the returned list reads `iter_data()` of the returned stream |
| `.async_execute(` | 42 | 171 | `test_native_async_operators.py` 39, `test_node_async_execute.py` 26, `test_async_execute.py` 23 | inputs become streams; outputs a `ChannelStream` writer |
| `Channel(` | 42 | 329 | `test_native_async_operators.py` 95, `test_node_async_execute.py` 51, `test_async_execute.py` 48 | a helper (`tests/stream_helpers.py`) makes a `ChannelStream` from rows or an Arrow table and collects one |
| `input_pipeline_hashes` | 17 | 84 | `test_native_async_operators.py` 42 | removed |
| `node_outputs` | 4 | 5 | `test_core/alignment/records.py` 2 | read the stream |

## Each node kind

### Source job node

- **Sync**: returns the output stream over `F(declared(bound.as_table(all columns)))`.
  `declared` is `_DeclaredForm.apply`, which already serves a whole Arrow table.
- **Async**: writes that Arrow table's record batches, or one batch per poll for a
  `PollingSource`, which keeps streaming.
- A `PollingSource` polls as often as today, and a run checks its sources first (ITL-1102).

### Operator job node

- **Sync**: `process(*input_streams)`. The kept output, which is also the return value, is
  `_as_upstream_stream(F(result.as_table(...)), self)`.
  - `_store_output_stream` stores the same Arrow table.
  - REPLAY returns the stored stream in that form.
  - A result with no rows keeps `_zero_rows_in_row_order` (ITL-1131).
- **Async**: the operator writes into the node's output stream. The node keeps the complete
  stream and stores it in `LOG` mode. REPLAY writes the stored Arrow table's batches and
  drains its inputs. A run with no rows completes its stream with no batch.
- Operators have no per-row error policy: a failure fails the node, as today.

### Function job node

The function is still called once per row. Everything around the call is Arrow data.

1. **Input as Arrow data.** For each input row the node computes the base entry id from
   that row's slice of the Arrow record batch: the tag side with system tags, plus the input
   data hash. It is the same one-row preimage `_build_record_id_preimage` builds today, still
   per row (hashing is ~0.3% of the run). No datagram is made.
2. **Lookup as an Arrow table.** `get_cached_results` finds the same entries. The loader
   keeps the stored joined Arrow table and a map from base entry id to row. Hits are taken
   from it by row index.
3. **The call.** For each miss, the parameters' values are taken from the input Arrow data,
   converted to Python once per column per Arrow table or record batch, through the data
   context's type converter (the conversion `Data` applies per row today). The call keeps
   today's logger, invocation context, error policy, commit cadence (`commit_every_rows`,
   `_row_processed`) and database error handling. Misses that share a base entry id are
   called once and the result reused, as the sync path's in-run lookup does today, and as
   ITL-1062 asks of the async path.
4. **Results as Arrow data.** The returned values are collected per output column and
   converted to Arrow once, under the declared output Arrow schema. The per-row items stay
   per row: the output data hash, the source-info tokens and the datagram id (`uuid7`,
   stored as `__data_id`). Pipeline records and result rows are written as Arrow tables per
   commit window, with today's rows, columns and values. Rows written before a failure stay
   written.
5. **Output.** In input order, assembled from hit rows and computed rows by row index.
   - A `None` result or a failed row gives no row.
   - Two equal inputs give two rows.
   - The column order is the first output row's (developer decision 2).

   Async keeps today's order: stored hits first, then computed rows as they finish.
6. **Load-only**: the hits-only case, with the same `not_stored` count.
7. **The node's own reads** come from its kept stream, in cache order (developer decision 3).
8. **Stored values decoded as a class (ITL-881).** A `TypeIdentityError` must stop the run at
   the same point as today. Stage 3 first establishes where it is raised today. If at
   datagram construction, the node checks each user-class column of the stored Arrow table
   once, before it passes a row on.
9. **`EmptyData` inputs (ITL-605).** Stage 3 first establishes how such a row crosses an
   edge today, and keeps it. The stream carries those rows' cached content hashes beside
   its Arrow data, never as a column. ITL-605's materialization thunks later use the same
   record.

### Side-effect job node

- Delivery calls the user's function with one row's values taken from the Arrow data.
- The output stream is the input's Arrow data masked to the rows passed on.
- A load-only run passes its input stream on unchanged.

## Per-row concerns without a row transport

### Observers

The protocol keeps its per-row hooks and its event order. A hook now receives a **row
view**: one internal class, `RowView`, that satisfies `TagProtocol` or `DataProtocol` over an
Arrow record batch and a row index. It builds the datagram only when it is first read.

- `RunCountingObserver` reads only the label and the `cached` flag, so it makes no row.
- The status and logging observers read tag values, so they make the tag datagram of the
  rows they report.
- A user's observer gets what it gets today.

Event order stays as today: sync in input order; async with stored hits first, then rows as
they finish. The async function node matches a row's input by row index instead of a
`_TAG_NODE_INPUT_REF` meta column.

**ITL-1220 (attachments, future).** A stream runs its attachments over row views of each
Arrow record batch, in row order, where observers are told. A stream is the natural place
to list them, since the stream is now the unit between nodes. A "halt" signal maps onto
failing the stream and cancelling the run's task group.

### Error policy

A row can fail only in a user's function or a delivery, both per row. The node wraps each
call:
- `continue`: the failed row is left out by mask, counted, and reported through
  `on_data_crash` with its row view;
- `fail_fast`: the node raises at once;
- database errors and `TypeIdentityError` stop the run whatever the policy.

## How each rule holds

- **ITL-845, each row's source info.** A stream's Arrow data keeps the `_source_*` values row
  by row, and F touches no value. List-valued tokens after `Batch`, `GroupBy` or `MergeJoin`
  are kept too. Source-info nullability is set once per complete Arrow table: a sync output,
  `materialize()`, the completion of a retained `ChannelStream`, and a function node's
  output. It is never set per Arrow record batch, because two batches of one stream would
  then have two schemas.
- **ITL-1122, large types.** An Arrow table is normalized once, where it becomes a stream.
  Streams pass Arrow data on by selecting, slicing or `take`, which keep types. Function
  results are converted under the declared output schema, which is large. Nothing is
  normalized per row, or twice. F step 3 keeps the one observable effect of today's
  per-row normalization.
- **Async equals sync.** Both paths give an operator the same type, with the same
  description. A barrier operator runs `static_process` on complete streams on both paths.
  Validation, input order, key checks and zero-row results read the description, once.
  `test_async_sync_equivalence` stays the check, with the standing guards below.
- **ITL-1133 and ITL-1131, one column order.** F places every stream's transport columns in
  row order, and `arrow_schema()` is in the same order. An operator node keeps and stores
  `F` of its result, sync and async.
- **Row order.** Each node passes on its rows in the order its successors receive them
  today:
  - a source: its Arrow table's order;
  - an operator: its result's order;
  - a function node (sync): input order;
  - a held function node: cache order;
  - a side-effect node: delivery order;
  - async: as today.
- **Staleness.** Every node calls `_update_modified_time` at the same points and in the same
  order. A `ChannelStream` has no modified time of its own: it answers its producing node's,
  as today's successor input does. The ITL-1095 and ITL-1121 tests run at each stage.
- **Run summary.** The counts come from the counting observer (one event per row, as today),
  from `last_run_not_stored`, and from held counts read without rows.

## What this closes (ITL-858)

| Case | Status on main | Closed by | Stage |
|---|---|---|---|
| Key types on an empty input, `Join` / `SemiJoin` (ITL-856) | open | checks from `arrow_schema()` before the first batch | 5 |
| Async column order (`ASYNC_ORDER_XFAIL`, ITL-1249, `tests/test_core/operators/test_several_items.py:211`) | strict xfail | async outputs complete in `arrow_schema()`'s order; the xfail is removed if its case passes | 5 |
| Data context of an async input (ITL-844) | open | the input stream's `data_context_key` | 5 |
| A row of another data context (ITL-951) | sync: whole-input error; async: answered from the cache | whole-input error before the first batch (ITL-858 decision 9) | 5 |
| Zero-row results, async (ITL-865) | narrow fix | `materialize()` of an empty input is the typed zero-row stream | 5 |
| System-tag names after a non-source input, async (ITL-906) | narrow fix through `input_pipeline_hashes` | each input stream reports its own pipeline hash | 5 |
| Zero-row read-back types (ITL-912) | fixed (`typed_zero_row_table`) | the same derivation, now `arrow_schema()` | 2b, 5 |
| Operator state written at validation (`SemiJoin`, `Index`) | present | read from the input stream | 5 |
| Duplicate function calls on async (ITL-1062) | open | misses deduplicated by base entry id | 5 |
| A loaded node's description (ITL-858 decision 7) | – | saved Arrow type texts (ITL-640) | 2a |

Standing guards:
- **Declared against materialized** (added in stage 2a, before anything depends on it):
  `stream.arrow_schema()` equals the materialized Arrow table's schema, with the two stated
  exceptions, for every operator, source kind and function node, sync and async.
- **In `test_async_sync_equivalence.py` (stage 5)**: one registry of every operator class
  with an `async_execute`, failing when a class is missing.
- **Four input shapes** per case: rows; zero rows on every input; zero rows on one input;
  inputs from an operator instead of a root source.
- **Batch layouts**: one Arrow record batch, one row per batch, mixed sizes.

## What must stay byte-identical, and how it is proven

### Must not change

1. **Every hash.** `generate_golden_hashes.py --check` matches. Per node:
   - content hash and pipeline hash;
   - record ids;
   - base and pipeline entry ids;
   - output data hashes;
   - source-info tokens;
   - system-tag column names and values;
   - folded values (`batch_v1:` / `group_v1:`).

   Nothing about the transport enters a hash (ITL-858 decision 3).
2. **Every stored table.** This covers the function pipeline and result tables, the
   operator cache (`odb_v1`), the side-effect log (`tdb_v1`) and the cached source
   (`sdb_v1`). For each: path, column names and order, types, nullability, field metadata,
   values, and the row order written.
3. **Every output a user reads.** For every node: `as_table(all_info=True)`, the
   `iter_data()` rows, the `head()` and `run()` views with their status and notes, and
   `as_source()` / `DerivedSource`.
4. **Every successor's input.** The Arrow data each operator, function node and side-effect
   node receives: columns, order, types, nullability, metadata, values and row order.
5. **Run behaviour.**
   - `RunSummary` and `NodeRunSummary.held`.
   - The warnings issued.
   - Every error raised, at the point it is raised today.
   - The observer event sequence, and the values an observer reads.
   - What logging capture records.
   - Which nodes a second run holds.

Not covered: Arrow chunk and record-batch boundaries, datagram object identity, timing. Not
covered either, by design: the changed signatures, which are the breaking change.

### Proof

1. **Equality tests**, kept permanently (`tests/test_pipeline/test_stream_transport_equivalence.py`).
   `R` and `ArrowTableStream.iter_data` are the oracle. Each comparison is
   `Table.equals(check_metadata=True)` plus field-by-field schema equality, on combined
   chunks.
   - **L0 (idempotence)**: an output stream `S` gives `R(rows of S) == S.as_table()` on the
     transport columns.
   - **L1 (operators)**: an operator node's output equals `R(list(result.iter_data()))`.
     This covers every exported operator, and REPLAY.
   - **L2 (sources)**: for each source kind bound to a slot, with and without a declared
     form, the output equals `R(list(node.iter_data()))`. The kinds are in-memory, CSV,
     Delta, SQLite, `CachedSource`, `DerivedSource`, `PollingSource` (counting polls) and
     SpiralDB through its fakes; PostgreSQL runs in its own job.
   - **L3 (entry ids)**: per input row, the id from the Arrow record batch slice equals
     `compute_base_entry_id(tag, data)`, also for a stream outside a job.
   - **L4 (function node)**:
     - the values given to the function equal today's `function_input(...).as_dict()`
       values (`==`, same types);
     - the results equal `R` of today's result datagrams;
     - the output equals `R` of today's output rows, with the first row a hit and with it
       a miss.
   - **L5 (held)**: a held node's output stream and count equal today's.
   - **L6 (async)**: for every node kind, the completed `ChannelStream` equals the sync
     output, as a set of rows (in order for sources and operators).
   - **L7 (descriptions)**: the declared-against-materialized guard.
   - **L8 (stream states)**: unit tests of `ChannelStream`, one per row of the semantics
     table:
     - retain on and off;
     - late readers;
     - fan-out with a slow reader;
     - reader close;
     - sync reads of an open stream (raising, inside and outside an event loop);
     - `head()` on an open stream;
     - identity with and without a producer;
     - producer failure and cancellation (readers raise `UpstreamFailedError`, and the run
       raises the producer's error);
     - double close;
     - zero rows.

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
   - `test_async_sync_equivalence` and `test_order_invariance`;
   - the operator, function and side-effect node tests;
   - `test_pipeline_job` and the observer tests;
   - the ITL-1131/ITL-1133 column-order tests;
   - the ITL-1095/ITL-1121 staleness and load-only tests;
   - the ITL-1075 deadlock tests;
   - the run summary tests;
   - the golden check.
3. **End-to-end differential**, a script in `docs/dev/scripts/itl_1347/`.
   - It runs fixture pipelines once on a pinned `origin/main` checkout and once on the
     branch. The pipelines use `docs/tutorial/workshop_data.py` plus the corpus.
   - Each runs cold, warm, held and load-only, on both orchestrators.
   - It dumps every stored table (Arrow IPC), every node's `as_table(all_info=True)`, the
     `RunSummary` and a recording observer's events, and compares them byte for byte.
   - Its output goes on each stage's PR.

## Migration and staging

### On the integration branch

The work lands on `eywalker/job-speed`, which the developer merges into `main` as one PR.
Each signature therefore changes directly in the stage that needs it, with no thin forms of
old signatures kept between stages (CLAUDE.md: no compatibility shims). `main` still sees
one breaking change: the branch merge. Question 7 asks the developer to confirm.

### Stages, with the ITL-858 work merged in

Unit costs from the profile: about 26 s of datagram building over 121,139 rows (about
**0.21 ms per row**), and about 14 s of rebuilding. The savings are estimates from those
shares, and stage 0 replaces them before stage 1 merges.

| Stage | What | ITL-858 part | Proof | Expected saving, warm sync run of 44.7 s |
|---|---|---|---|---|
| 0 | Measurement only: datagrams per node kind, `R` calls and time by caller, edges per node, on a tutorial-data pipeline of the workshop's shape | declared against materialized Arrow schema, measured for every operator and source | – | 0 s |
| 1 | **First small step**: the sync orchestrator reuses an operator node's kept Arrow table (already `R` of exactly the rows it passes on) for each successor, for held operator nodes too. Held counts are read without rows. No signature changes | – | by construction; one test per edge | about **4–8 s** |
| 2a | Sources, sync. `arrow_schema()`, `iter_batches()`, `async_iter_batches()`, `materialize()` and `is_complete` on `StreamBase`; each node's `execute` returns its output stream; held nodes give their output stream; `node_outputs` holds streams; sources return `F` of their Arrow table | `arrow_schema()` on every stream; the declared-against-materialized guard; loaded nodes describe themselves | L0, L2, L5, L7 | with 2b, about **14–20 s** |
| 2b | Operators, sync: kept output and return value `F(result.as_table())`, REPLAY likewise | – | L0, L1, L5 | (with 2a) |
| 3 | Function nodes, sync: input from `iter_batches()`, entry ids from slices, hits from the stored Arrow table, calls on values from Arrow data, results per column, row views for observers, own reads from the kept stream | – | L3, L4, differential | about **8–14 s** |
| 4 | Side-effect nodes, sync: masked pass-through, load-only pass-through, held stream | – | L0, differential | small |
| 5 | Async: `ChannelStream` and `BatchWriter`; every `async_execute` on streams and a writer; barrier operators by `materialize()`; `Join` / `SemiJoin` checks before the first batch; per-row operators per Arrow record batch; validation state removed; `input_pipeline_hashes` removed; `BroadcastChannel` removed; function node async with dedup | ITL-856, ITL-844/951 async, ITL-865/906 general form, ITL-1249 xfail, ITL-1062; standing guards | L6, L7, L8, `test_async_sync_equivalence`, differential | the same order of gain for async runs |

The ranges overlap. The bound for stages 1–4 is the ~40 s (~90%) the profile puts on
conversions. The floor is the plain-stream time (1.9 s) plus the lookups and writes a job
adds. A warm sync `job.run()` of about 3–6 s is the target after stage 3. The `LazyModule`
cost (42.9 million `__getattr__` calls; 45.1 s → 40.7 s in the profile's experiment) is a
separate fix in this round.

Each stage is one issue and one PR against `eywalker/job-speed`, with:
- its equality tests;
- the targeted suites;
- the golden check;
- the differential output.

Benchmarks follow the round's rules.

### Release note (for the merge into `main`)

In `changelog.d/<issue>.md`, under `### Breaking changes and migration`:

> #### Job nodes and operators exchange streams of Arrow data, not rows
>
> Code that drives nodes or operators directly changes. These are unchanged: pipelines,
> jobs, `job.run()`, `run_async()`, sources, operators used in pipelines, and every stored
> table and hash.
>
> - A node's `execute()` returns its output stream, not a list of `(Tag, Data)` pairs.
>   Read rows with `iter_data()`, or the Arrow table with `as_table()`.
> - `async_execute()` of nodes, operators and function pods takes input streams and a
>   `BatchWriter` (the writer of a `ChannelStream`) instead of row channels. A stream
>   delivers Arrow record batches (`async_iter_batches()`) and reads whole with
>   `materialize()`.
> - `BroadcastChannel` is removed; a `ChannelStream` with several readers replaces it.
> - The `input_pipeline_hashes` argument of operators' `async_execute()` is removed: each
>   input stream states its own `pipeline_hash()`.
> - A side-effect node's `async_execute()` takes one input stream, not a list of one.
> - `OrchestratorResult.node_outputs` maps each node to its complete output stream.
> - The orchestrators' `held_rows=` argument is now `held=` and takes streams.
> - `StreamProtocol` has five new members: `arrow_schema()`, `iter_batches()`,
>   `async_iter_batches()`, `materialize()` and `is_complete`. `StreamBase` provides them.
>   A class that implements the protocol without `StreamBase` must add them.

## Developer decisions (2026-11-22)

1. **`OrchestratorResult.node_outputs` holds each node's output.** Refined in the second
   revision: with `NodeTable` gone, it holds complete output streams backed by Arrow tables.
   `stream.as_table()` gives the Arrow table. The developer confirms the refinement
   (question 8).
2. **A function node's first-row column order is kept exactly.** Every stage reproduces it
   (L4). One order for it is future work, ITL-1353.
3. **A function node's own reads keep cache order for now.**
4. **Stages run in order 0 → 1 → 2 → 3 → 4 → 5.** Stage 2 is split into 2a (sources) and 2b
   (operators), which can go to two workers in parallel.

Also from the developer:
- async execution handles an object that wraps the channel and carries the stream's
  information (first revision);
- the protocol change is spelled out in detail;
- the transported data is named "Arrow table" and "Arrow record batch" (second revision);
- the unit passed around is a stream that can act as a channel (second revision; this
  design).

The developer approves the spec as a whole before stage 0 starts.

## Open questions for the developer

5. **Source-info nullability** stays decided by a complete Arrow table's values: the one
   column property `arrow_schema()` does not declare. This keeps stored field flags and the
   operator node's record hashes. Keep it (proposed), or declare a fixed flag? That would
   move the flags and hashes, so it needs its own decision.
6. **Batch and buffer sizes.** The proposal:
   - a source writes Arrow record batches of at most 65,536 rows;
   - an operator writes its result's own record batches;
   - a reader's queue holds 8 Arrow record batches by default.

   With `retain=False` a reader is bounded at about 0.5 M rows, against 64 rows per
   channel today. Accept the defaults, or set others?
7. **Signature changes on the integration branch.** Proposed: each stage changes the
   signatures it needs directly on `eywalker/job-speed`, with no thin forms of the old
   ones. `main` sees one breaking change, the branch merge. Accept?
8. **`node_outputs` as streams** (refining decision 1): accept?

## Future work (not designed here)

- One output column order for a function node, whatever its first row (ITL-1353).
- Source-info nullability declared by `arrow_schema()` (question 5).
- A `null` column through the job layer (ITL-1319).
- `Schema` carrying its Arrow schema (ITL-1307), which `arrow_schema()` then reads.
- Attachments on streams (ITL-1220) and materialization thunks (ITL-605), placed above.
- Batching the side-effect completion lookup (one `get_record_by_id` per row today).
- Polars data frames as a stream's backing, where an operator works in Polars.
