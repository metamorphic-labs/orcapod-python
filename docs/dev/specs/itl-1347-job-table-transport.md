# Job nodes pass tables, not rows (ITL-1347, with ITL-858)

Status: design for the developer's review. No code changes with it. Written against
`origin/main` 8c6fc4f5 (2026-11-22). Every implementation stage below becomes its own issue
after approval. Revised on 2026-11-22 to fold in the ITL-858 input object and to spell out
the protocol change and its call sites.

## The rule

Developer, 2026-11-22: Orcapod was always meant to keep data as Arrow (or Polars) tables as
long as possible and to avoid converting rows to objects and back. This spec makes that the
rule for the job layer as a whole. It is not a special case for one node kind.

- **The unit that moves between job nodes is a table.** In the sync orchestrator it is one
  table per node. In the async orchestrator it is a record batch per channel item.
- **A row exists only where a user's function is called**: a function pod's function, or a
  side effect's delivery. Even there, the node reads its input as a table and writes its
  results as a table. Only the call itself takes one row's values.
- **Every input carries a description.** The developer said this on ITL-858 (2026-11-12):
  "shouldn't the channel just carry schema info just like pipeline does? It should be set so
  that this is known statically without executing anything." So each input a node, operator
  or pod receives is one object, sync and async. It states the input's description: schema,
  Arrow types, keys, data context, pipeline hash. It delivers the input's data as a table or
  as record batches. The separate `input_pipeline_hashes` argument disappears. The
  developer, 2026-11-22: async execution should "handle more powerful object that wraps the
  channel".
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
  - Every operator node turns its table result back into rows.

  This served the async orchestrator's per-row streaming, concurrent function calls,
  per-row observer hooks and the per-row error policy. Plain streams never adopted it.
- **November 2026, correctness fixes that kept the row protocol and made each row costlier.**
  - ITL-845: `_rows_to_table` keeps each row's source info.
  - ITL-1122: large-type normalization at stream construction, now paid three times per
    row datagram.
  - The rule that async output rows equal the sync rows, built from each datagram's Arrow
    table.
- **Channels carry rows and nothing else (ITL-858, 2026-11-10 onward).** Because of this,
  async operators rediscover from rows what the sync path is told. Where rows cannot answer,
  the two paths diverged:
  - key types on an empty input (ITL-856);
  - column order (ITL-854, ITL-1249);
  - data context (ITL-844, ITL-951);
  - zero-row results (ITL-865, ITL-912);
  - system-tag names after a non-source input (ITL-906).

  Narrow fixes closed several of these; the general one is this design.

## What it costs

The ITL-1345 profile (workshop-scale notebook on 0.4.0rc1, local Delta copy, no network):

| Measure | Value |
|---|---|
| Same pipeline as plain streams | 1.9 s |
| As a `PipelineJob`, warm / cold `job.run()` | 44.7 s / 46.8 s, identical rows |
| Warm run: building a `(Tag, Data)` datagram per row passed between job nodes | ~58% (~26 s) |
| Warm run: rebuilding tables from those rows (`_rows_to_table`, `_materialize_as_stream`) | ~32% (~14 s) |
| Warm run: operator compute / reads / all hashing | ~8% / ~1% / ~0.3% |
| `Data.__init__` calls; `normalize_schema_to_large_types` | 121,139; 363,466 (three per row) |
| One source of 63,349 rows: `as_table()` / `list(iter_data())` | 0.29 s / 9.1 s |

About 90% of a warm run converts tables into rows and back. A warm run in a fresh process
holds no node, so every node runs, and every row goes through these conversions even though
every function result is already stored.

### Where rows are made and unmade today

Sync orchestrator (`SyncPipelineOrchestrator.run`):

1. **Source job node** (`SourceNodeBase.execute`): `list(self.iter_data())`, one datagram
   per row. A narrower source given in the declared form gets a second datagram per row
   (`_declared_row`).
2. **Every edge** (`_materialize_as_stream` → `StaticOutputOperatorPod._materialize_upstream`).
   The successor's input is `R(rows)`: one `Tag.as_table` and one `Data.as_table` per row,
   a concatenation, an `hstack`. It is wrapped as the predecessor's stream
   (`_as_upstream_stream`). A node with *k* successors is rebuilt *k* times.
3. **Operator job node** (`OperatorJobNode.execute`). The operator computes a table.
   `list(result.iter_data())` makes rows of it, and `_kept_output(result, rows)` makes a
   table of them again to keep and store.
4. **Function job node** (`FunctionJobNode.execute`). It iterates `input_stream.iter_data()`:
   a datagram per input row, from a table step 2 just built from rows. Stored hits are
   loaded as one datagram per stored row (`_load_cached_entries`). Each result is a `Data`
   built from the returned values.
5. **Side-effect job node**: iterates its input's rows and passes them on.
6. **Held nodes** (`node_run.plan_run`): `list(node.iter_data())` for every held node, and
   `sum(1 for _ in node.iter_data())` again to count its rows.

The async orchestrator does the same with channels of rows. A barrier operator rebuilds
each input with `R` (`_materialize_input_stream`). An operator node rebuilds its collected
output with `R` (`_materialize_to_stream`).

## Definitions

- **R(rows)**: `StaticOutputOperatorPod._rows_to_table(rows)`, the table today's code builds
  from rows. It is the oracle of the proofs below.
- **Row-built form F**: the one function that turns a node's table into `R` of that table's
  rows without making the rows. In order:
  1. Drop the context, meta (`__`) and `_content_hash` columns. A row's `Data.as_table`
     leaves them out; the context column is added back when an input is made. No operator
     carries meta columns (ITL-1080), so the job transport never does.
  2. Drop the data-side copy of each data-tag. A data-tag is one column, among the tags.
  3. Map `null` to `large_string`, as a datagram's normalization does today
     (`Datagram._init_from_table`). Whether it should stay `null` is ITL-1319's decision.
  4. Place the columns in row order: user tags, system tags, data, source info
     (`_in_row_order`, ITL-1131).
  5. On a whole table (never on a batch), flag each source-info field nullable only when a
     token is null (`_flag_source_info_nullability`).
- **Stream description**: what is known about a stream without any row. Defined below.
- **TableInput / BatchInput**: the input object, sync and async. **BatchOutput**: the async
  output. **NodeTable**: the sync output. All defined below.

## The stream description

### What it states

```python
class StreamDescription(Protocol):
    """What is known about a stream before any row: never computes, reads or polls."""

    def keys(
        self, *, columns: ColumnConfig | dict[str, Any] | None = None, all_info: bool = False
    ) -> tuple[tuple[str, ...], tuple[str, ...]]: ...
    def output_schema(
        self, *, columns: ColumnConfig | dict[str, Any] | None = None, all_info: bool = False
    ) -> tuple[Schema, Schema]: ...
    def arrow_schema(self) -> pa.Schema: ...
    @property
    def data_tags(self) -> tuple[str, ...]: ...
    @property
    def data_context_key(self) -> str: ...
    def pipeline_hash(self) -> ContentHash: ...
    @property
    def arrow_types_declared(self) -> bool: ...
```

- `keys`, `output_schema`: as the stream answers them today (Python types; key order).
- `arrow_schema()`: the schema of the data the input delivers. It lists the user tags,
  system tags, data and source-info columns, in row order, with the Arrow types,
  nullability and field metadata of the rows. It holds no context column and no meta column.
  So it answers which system-tag and source-info columns are present (their names), and that
  no meta column is.
- `data_tags`, `data_context_key`, `pipeline_hash()`: as the stream answers them today.
  `data_context_key` is the stream's key, not a per-row value. A row of another context is
  refused before the first batch (ITL-951 rule, below).
- `arrow_types_declared`: `False` when the stream cannot state its Arrow types without rows,
  so its types were converted from its Python schema. These are the inputs ITL-1089 lists:
  an unbound slot, and a `PollingSource` that has never been polled. A run checks its sources
  first (ITL-1102), which polls a polling source, so inside a job run this is `False` only
  for a source that has never had data.

One function builds a description from any stream, source or node:
`describe(stream) -> StreamDescription` (new module `orcapod/pipeline/stream_input.py`). It
reads only what the stream answers without rows.

### Where the declared Arrow schema comes from now

From `typed_zero_row_table` (`core/streams/base.py`; ITL-912). It is the stream's zero-row
table, put through F's steps 1, 2 and 4, then normalized to large types as
`typed_zero_row_input` does. That is exactly the schema of today's zero-row input
(`_zero_row_input`) without its context column.

- A source answers from the table or schema it holds.
- An operator node applies its operator to its inputs' zero-row tables, which processes no
  data (the rule main has held since ITL-912; CLAUDE.md, "the Arrow schema is never
  computed").
- A function node answers with `function_zero_row_table`: input tags plus declared outputs.
- A loaded node answers from the Arrow type texts its saved file now holds (ITL-640). So a
  loaded job node can describe itself, which closes ITL-858's decision 7 without a format
  change.

Two exceptions keep today's bytes:

1. **Source-info nullability** is decided by a materialized table's values (F step 5), not
   declared. The operator node's per-row record hash covers field flags, so declaring a
   fixed flag would move stored record hashes. The description states source-info fields
   as nullable. The materialized table flags them, as today. Question 5 asks whether to keep
   this.
2. **`null` columns.** Populated rows carry `large_string` (F step 3). A zero-row table
   carries `null` (`keep_null=True`), as today. The batch check knows this one difference,
   and ITL-1319 removes it.

**ITL-1307 later.** When `Schema` carries its Arrow schema, `output_schema()` returns it, and
`arrow_schema()` becomes a read of the two schemas plus the system-tag and source-info
fields. `describe` changes in one place. Nothing that receives a description changes.

## Inputs: one description, two closely related protocols

Decision: **two protocols over one description**, not one protocol.

- The sync path is synchronous and hands operators to `static_process`, which re-reads its
  inputs. It needs a re-readable, table-backed stream.
- The async path needs single-pass delivery with back-pressure.

One protocol would make sync code await, or make async code re-readable, which ITL-858
rejected: a channel stream is not a `StreamProtocol`. The two share the description, and
there is one conversion, `materialize()`. Code that reads only the description is written
once and serves both. That covers validation, canonical input order, key-type checks and
zero-row results.

```python
class TableInput(StreamDescription, StreamProtocol, Protocol):
    """A sync input: the description plus the materialized table, re-readable."""


class BatchInput(StreamDescription, Protocol):
    """An async input: the description, then record batches, read once, back-pressured."""

    def __aiter__(self) -> AsyncIterator[pa.RecordBatch]: ...
    async def receive(self) -> pa.RecordBatch: ...  # raises ChannelClosed at the end
    async def materialize(self) -> TableInput: ...  # reads to the end: a table input
    async def drain(self) -> None: ...  # reads to the end and discards
```

- **`TableInput`** is implemented by one class, `InputTable(ArrowTableStream)`. It is today's
  `_as_upstream_stream` result (the predecessor's producer, upstreams, pipeline hash and
  data context, or `_RowsOfStream` for a node without a producer) with the description's
  members added. With no rows it is today's zero-row input (`_zero_row_input`).
  `TableInput.of(stream)` makes one from any stream (`F` of its table). It is the one
  adapter, used where a node runs on a stream outside a job and by tests.
- **`BatchInput`** is implemented by the reader of a `BatchChannel`.
  - `materialize()` concatenates the batches, applies F step 5, and returns an `InputTable`
    with the same description. With no batches it returns the description's zero-row
    table, typed by the description and not by rows.
  - `drain()` serves a reader that must not block its producer: a held node, and REPLAY.
    Every input is read to its end (ITL-1075-AH1).

## Outputs

- **Sync**: `execute` returns a `NodeTable`:

  ```python
  @dataclass(frozen=True)
  class NodeTable:
      table: pa.Table  # F of the node's rows, flags applied, no context column
      description: StreamDescription
  ```

  The orchestrator makes each successor's input from it:
  `InputTable.over(node_table, upstream)`. A fan-out reuses the same table for every
  successor.
- **Async**: a node writes to a `BatchOutput`, which carries the output's description:

  ```python
  class BatchOutput(Protocol):
      @property
      def description(self) -> StreamDescription: ...
      async def send(self, batch: pa.RecordBatch | pa.Table) -> None: ...
      async def close(self) -> None: ...
  ```

  - The orchestrator creates each node's output channel from `describe(node)` before any
    task starts, so every successor has its description before the first batch.
  - `send` checks each batch's schema against the description: names, order, types,
    nullability, metadata. Source-info nullability and the `null` / `large_string` case
    are excepted. A mismatch is an error naming the column and both types. Checking costs
    one comparison per batch, not per row, so it is always on. This settles ITL-858
    decision 4.
  - A zero-row output is a description and a close, with no batch.

## Protocol change

Every signature that changes, with its new form, who implements it and who calls it. All of
these change together, in one breaking change (see "Migration and staging").

### Node protocols (`src/orcapod/protocols/node_protocols.py`)

| Protocol | Before | After |
|---|---|---|
| `SourceNodeProtocol.execute` (:45) | `execute(self, *, observer=None) -> list[tuple[TagProtocol, DataProtocol]]` | `execute(self, *, observer: ExecutionObserverProtocol \| None = None) -> NodeTable` |
| `SourceNodeProtocol.async_execute` (:51) | `async_execute(self, output: WritableChannel[tuple[Tag, Data]], *, observer=None) -> None` | `async_execute(self, output: BatchOutput, *, observer: ExecutionObserverProtocol \| None = None) -> None` |
| `FunctionNodeProtocol.execute` (:84) | `execute(self, input_stream: StreamProtocol, *, observer=None, error_policy="continue", run_id=None, load_only=False) -> list[tuple[Tag, Data]]` | `execute(self, input: TableInput, *, observer=None, error_policy: Literal["continue", "fail_fast"] = "continue", run_id: str \| None = None, load_only: bool = False) -> NodeTable` |
| `FunctionNodeProtocol.async_execute` (:94) | `async_execute(self, input_channel: ReadableChannel[...], output: WritableChannel[...], *, observer, error_policy, run_id, load_only) -> None` | `async_execute(self, input: BatchInput, output: BatchOutput, *, observer=None, error_policy="continue", run_id=None, load_only=False) -> None` |
| `OperatorNodeProtocol.execute` (:127) | `execute(self, *input_streams: StreamProtocol, observer=None, load_only=False) -> list[tuple[Tag, Data]]` | `execute(self, *inputs: TableInput, observer=None, load_only: bool = False) -> NodeTable` |
| `OperatorNodeProtocol.async_execute` (:134) | `async_execute(self, inputs: Sequence[ReadableChannel[...]], output: WritableChannel[...], *, observer, load_only) -> None` | `async_execute(self, inputs: Sequence[BatchInput], output: BatchOutput, *, observer=None, load_only=False) -> None` |
| `SideEffectNodeProtocol.execute` (:158) | `execute(self, input_stream: StreamProtocol, *, observer, run_id, load_only) -> list[tuple[Tag, Data]]` | `execute(self, input: TableInput, *, observer=None, run_id=None, load_only=False) -> NodeTable` |
| `SideEffectNodeProtocol.async_execute` (:167) | `async_execute(self, inputs: Sequence[ReadableChannel[...]], output: WritableChannel[...], *, observer, run_id, load_only) -> None` | `async_execute(self, input: BatchInput, output: BatchOutput, *, observer=None, run_id=None, load_only=False) -> None` (one input, as a function node's; it always had exactly one) |

**Implemented by:**
- `SourceNodeBase` (`core/nodes/source_node.py:1191`, `:1216`);
- `FunctionJobNode` (`core/nodes/function_node.py:2485`, `:4130`);
- `OperatorJobNode` (`core/nodes/operator_node.py:1647`, `:2133`);
- `SideEffectJobNode` (`side_effects.py:1224`, `:1455`);
- `LoadedSideEffectJobNode` (`pipeline/loaded_side_effects.py:259`, `:289`).

**Called by:** the two orchestrators, `FunctionJobNode.run`, `SideEffectJobNode._run_alone`
and the loaded side-effect node (see "Call sites").

### Operator and function pod `async_execute`

| | Before | After |
|---|---|---|
| `StaticOutputOperatorPod.async_execute` (`core/operators/static_output_pod.py:728`) and every override | `async_execute(self, inputs: Sequence[ReadableChannel[tuple[Tag, Data]]], output: WritableChannel[tuple[Tag, Data]], *, input_pipeline_hashes: Sequence[ContentHash]) -> None` | `async_execute(self, inputs: Sequence[BatchInput], output: BatchOutput) -> None` |
| `FunctionPod.async_execute` (`core/function_pod.py:660`) | `async_execute(self, inputs: Sequence[ReadableChannel[...]], output: WritableChannel[...], pipeline_config=None, *, observer, error_policy, run_id) -> None` | `async_execute(self, inputs: Sequence[BatchInput], output: BatchOutput, pipeline_config: PipelineConfig \| None = None, *, observer=None, error_policy="continue", run_id=None) -> None` |

`input_pipeline_hashes` disappears: an operator reads `inputs[i].pipeline_hash()`. Its 65
uses in 13 source files and 84 lines in 17 test files go.

**Implemented by:** the barrier default (`static_output_pod.py:728`), `UnaryOperator`
(`core/operators/base.py:202`), `BinaryOperator` (`base.py:316`), and the 16 overrides:
- `add_tags.py:336`, `drop_nulls.py:322`, `batch.py:211`, `semijoin.py:308`, `join.py:669`,
  `index.py:448`, `pick.py:444`, `rejoin.py:416`;
- `column_selection.py:152`, `:305`, `:444`, `:598`, `:825`;
- `mappers.py:263`, `:538`, `:825`.

`FunctionPod` (`function_pod.py:660`).

**Called by:** `OperatorJobNode.async_execute` (`operator_node.py:2244`), `Rejoin`'s
delegation (`rejoin.py:428`), the function node (`function_node.py:4467`, `:4485`), and
tests.

What each body becomes:
- A **barrier** operator: `streams = await asyncio.gather(*(i.materialize() for i in inputs))`,
  then `static_process(*streams)`, then it sends the result's batches. That is correct for
  zero rows and for system-tag names, because each materialized input reports its own
  pipeline hash.
- A **per-row operator** (select, map, `AddTags`, `DropNulls`, `Index`, `Pick`) applies its
  table rule to each batch as it arrives.
- **`Join` and `SemiJoin`** check shared-key types from `arrow_schema()` before the first
  batch (ITL-856). They order inputs with the method the sync path uses, read from the
  descriptions. `SemiJoin` keeps reading its left input while it collects its right one
  (ITL-1075-AH1).
- Operator state written at validation goes: `SemiJoin._validated_common_keys`
  (`semijoin.py:287`, `:306`, `:381`) and `Index._element_arrow_type_fallback`
  (`index.py:82`, `:126`). The description answers both at the time they are needed.

### Channels (`src/orcapod/channels.py`)

| Class | Before | After |
|---|---|---|
| `ReadableChannel[T]` (:46), `WritableChannel[T]` (:63) | Protocols in every node, operator and pod signature | Unchanged as generic queue protocols. No node, operator or pod signature uses them. They remain for queues inside a node (a function node's row queue to its pod, `Join`'s chain) |
| `Channel[T]` (:172) | The orchestrator's edge for one successor | Unchanged generic bounded queue; used inside nodes and as `BatchChannel`'s queue |
| `BroadcastChannel[T]` (:247) | The orchestrator's edge for fan-out | Unchanged generic class; used as `BroadcastBatchChannel`'s queues |
| `ChannelClosed` (:36) | Raised by `receive` on a closed, drained channel | Unchanged; also raised by `BatchInput.receive` |
| **new** `BatchChannel` | – | `BatchChannel(description: StreamDescription, buffer_size: int = 8)`; `.reader -> BatchInput`, `.writer -> BatchOutput`; the edge for one successor |
| **new** `BroadcastBatchChannel` | – | `BroadcastBatchChannel(description: StreamDescription, buffer_size: int = 8)`; `.add_reader() -> BatchInput`, `.writer -> BatchOutput`; the edge for fan-out, every reader with the same description |

`buffer_size` counts batches, not rows. Question 6 is the default batch size and buffer size.

### Results and orchestrators

| | Before | After |
|---|---|---|
| `OrchestratorResult.node_outputs` (`pipeline/result.py:21`) | `dict[Any, list[tuple[TagProtocol, DataProtocol]]]` | `dict[Any, NodeTable]` (developer decision 1) |
| `SyncPipelineOrchestrator.run(..., held_rows=...)` (`sync_orchestrator.py:58`) | `held_rows: Mapping[Any, list[tuple[Tag, Data]]] \| None` | `held: Mapping[Any, NodeTable] \| None` |
| `AsyncPipelineOrchestrator.run` / `run_async` / `_run_async` (`async_orchestrator.py:77`, `:158`, `:196`) | `held_rows: Mapping[Any, list[tuple[Tag, Data]]] \| None` | `held: Mapping[Any, NodeTable] \| None` |
| `RunPlan.held_rows` (`pipeline/node_run.py:267`) | `dict[Any, list[Any]]` | `held: dict[Any, NodeTable]` |

Not changed: `FunctionJobNode.execute_data(tag, data)` (`function_node.py:2464`), a per-row
entry point that no orchestrator uses and tests call (33 lines in 10 files). The executors'
`async_execute_callable` is also unchanged.

## Call sites

Every call site on `origin/main` 8c6fc4f5, and what it becomes.

### Sync orchestrator (`src/orcapod/pipeline/sync_orchestrator.py`)

| Line | Today | Becomes |
|---|---|---|
| :58–66 | `run(..., held_rows=...)` | `run(..., held=...)` |
| :104 | `buffers[node] = list(held[node])` | `tables[node] = held[node]` (a held node's `NodeTable`) |
| :106 | `node.execute(observer=...)` (source) | same call; returns a `NodeTable` |
| :108–119 | `_gather_upstream`, `_materialize_as_stream(buf, upstream)`, `node.execute(input_stream, ...)` (function, side-effect function) | `node.execute(InputTable.over(tables[pred], pred), ...)` |
| :121–128 | `_gather_upstream_multi`, one `_materialize_as_stream` per input, `node.execute(*input_streams, ...)` (operator) | same order rule (`node.upstreams`); `node.execute(*(InputTable.over(t, p) for t, p in ...), ...)` |
| :130–141 | as :108 (side-effect node) | as :108 |
| :173, :187 | `_gather_upstream`, `_gather_upstream_multi` return row buffers | return `NodeTable`s; unchanged order logic |
| :206 | `_materialize_as_stream` → `StaticOutputOperatorPod._materialize_upstream` | removed; `InputTable.over` holds `_as_upstream_stream`'s logic and the zero-row path |
| :225 | `_gc_buffers` | unchanged logic over `tables` |
| :155 | `OrchestratorResult(node_outputs=buffers)` | `OrchestratorResult(node_outputs=tables)` |

### Async orchestrator (`src/orcapod/pipeline/async_orchestrator.py`)

| Line | Today | Becomes |
|---|---|---|
| :77, :158, :196 | `held_rows=` | `held=` |
| :230–250 | `Channel(buffer_size=buf)` for one successor, `BroadcastChannel` + `add_reader()` for several, a sink `Channel` for a terminal node | `BatchChannel(describe(node), buffer_size)`, `BroadcastBatchChannel(describe(node), buffer_size)` + `add_reader()`, a sink `BatchChannel` |
| :258–265, :799 | `_CollectingWriter` keeps every row | keeps the batches; at close, one `NodeTable` (concatenated, F step 5) |
| :267–275, :645, :651 | held node: `_drain(reader)` per input, `_send_held(rows, writer)` | `BatchInput.drain()` per input; send the held `NodeTable`'s batches |
| :281 | `node.async_execute(writer, observer=...)` (source) | same call with a `BatchOutput` |
| :294 | `node.async_execute(input_reader, writer, ...)` (function, side-effect function) | `BatchInput`, `BatchOutput` |
| :320 | `node.async_execute(input_readers, writer, ...)` (operator; order by `node.upstreams`) | `Sequence[BatchInput]`, `BatchOutput`; same order |
| :340 | `node.async_execute([input_reader], writer, ...)` (side-effect) | `node.async_execute(input_reader, writer, ...)` |

### Nodes, operators and pods

| Site | Today | Becomes |
|---|---|---|
| `source_node.py:1191` `execute` | `list(self.iter_data())` | `NodeTable` of `F(declared(bound.as_table(all columns)))` |
| `source_node.py:1216` `async_execute` | sends each row of `async_iter_data()` | sends the table's batches (one per read; one per poll for a `PollingSource`) |
| `operator_node.py:1647` `execute` | `process`, `list(result.iter_data())`, `_kept_output` | `process`; kept and returned table `F(result.as_table(...))` |
| `operator_node.py:2133` `async_execute` | forwards rows through `intermediate` (:2218), collects them, `_materialize_to_stream` | forwards batches through a `BatchChannel` described by the node; keeps their concatenation |
| `operator_node.py:2241–2250` | `hashes = [s.pipeline_hash() ...]`; `operator.async_execute(taps, intermediate.writer, input_pipeline_hashes=hashes)` | `operator.async_execute(inputs, intermediate.writer)` |
| `operator_node.py:109` `_InputTap`, `:2275` `_materialize_upstream(tap.rows, upstream, zero_rows=True)` | taps record rows to rebuild empty inputs | removed: an empty input materializes from its description |
| `function_node.py:2485` `execute`, `:2008` `_execute_load_only` | iterate `input_stream.iter_data()` | read the `TableInput`'s table (entry ids from slices, hits from the stored table, calls on misses) |
| `function_node.py:4130` `async_execute` | three stages over row channels | reads batches; hits leave as batches; misses go row by row to the pod's row executor |
| `function_node.py:4259`, `:4262` | `compute_channel`, `result_channel`: row `Channel`s to and from the pod | kept as row queues inside the node: rows exist only for misses |
| `function_node.py:4467`, `:4485` | `execution_pod.async_execute([compute_channel.reader], result_channel.writer, ...)`, `self._function_pod.async_execute(...)` | the pod's per-row executor (the semaphore and hooks of today's `FunctionPod.async_execute`, split out so the node and the pod's own batch form share it) |
| `function_node.py:4055` `FunctionJobNode.run` | `self.execute(self._input_stream, observer=counter, load_only=...)` | `self.execute(TableInput.of(self._input_stream), observer=counter, load_only=...)` |
| `side_effects.py:1224`, `:1455` | iterate and pass on rows | iterate rows for delivery only; pass on the input table masked |
| `side_effects.py:1428` `_run_alone` | `self.execute(self._input_stream, ...)` | `self.execute(TableInput.of(self._input_stream), ...)` |
| `loaded_side_effects.py:284`, `:319` | `super().execute(...)`, `super().async_execute(...)` | same forwarding, new signatures |
| `rejoin.py:428` | `StaticOutputOperatorPod.async_execute(self, inputs, output, input_pipeline_hashes=...)` | `StaticOutputOperatorPod.async_execute(self, inputs, output)` |
| `join.py:815`, `:834` | `Channel` chain inside an N-ary join | `BatchChannel` chain; each intermediate's description from `zero_row_result` of the pair |
| `static_output_pod.py:689` `_materialize_input_stream`, `:543` `_materialize_to_stream`, `:570` `_materialize_upstream` | rebuild rows into streams | removed; `materialize()` and `InputTable.over` replace them |
| `static_output_pod.py:304` `_rows_to_table` | used on every edge | moves to the tests as the oracle `R` once nothing in the library uses it |

### Plan and held rows

| Site | Today | Becomes |
|---|---|---|
| `node_run.py:341–345` | `held_rows = {node: list(node.iter_data()) ...}` | `held = {node: node.node_table() ...}`, the node's `NodeTable` (operator: its kept table; function: its held rows' table; side effect: its held table) |
| `node_run.py:346` | `held_counts = {node: sum(1 for _ in node.iter_data()) ...}` | counts without rows: `num_rows`, or a function node's run entry counts |
| `node_run.py:432`, `job.py:1000`, `job.py:1071` | `held_rows=plan.held_rows` | `held=plan.held` |
| `run_summary.py:532` | reads `held` counts | unchanged |

### Tests that call these directly

Counted on `origin/main`, by file, excluding database cursors' own `execute`:

| What | Files | Lines | Largest |
|---|---|---|---|
| node `.execute(` | 18 | 88 | `test_ephemeral_result.py` 32, `test_side_effect_pod.py` 13, `test_node_execute.py` 8, `test_node_protocols.py` 7, `test_spiraldb_table_source_where.py` 5, `test_materialize_source_info.py` 5 |
| `.async_execute(` | 42 | 171 | `test_native_async_operators.py` 39, `test_node_async_execute.py` 26, `test_async_execute.py` 23, `test_pipeline_example.py` 9, `test_node_protocols.py` 7 |
| `Channel(` | 42 | 329 | `test_native_async_operators.py` 95, `test_node_async_execute.py` 51, `test_async_execute.py` 48 |
| `input_pipeline_hashes` | 17 | 84 | `test_native_async_operators.py` 42, `test_async_execute.py` 13 |
| `node_outputs` | 4 | 5 | `test_core/alignment/records.py` 2 |

A shared test helper (`tests/channel_helpers.py`) builds a `BatchInput` from a stream and
collects a `BatchOutput` into a table, so each test changes by a line or two.

## Each node kind

### Source job node

- **Sync**: returns `NodeTable(F(declared(bound.as_table(all columns))))`. `declared` is
  `_DeclaredForm.apply`, which already serves a whole table. No row is made.
- **Async**: sends the same table as batches: one batch per read, or one batch per poll for
  a `PollingSource`, which keeps streaming.
- A `PollingSource` polls exactly as often as today, and a run checks its sources first
  (ITL-1102), as today.

### Operator job node

- **Sync**:
  - `process(*inputs)`; the node keeps `F(result.as_table(row columns))` and returns it.
  - `_store_output_stream` stores the same kept table, so the stored table is unchanged.
  - REPLAY returns `F` of the stored stream.
  - With no rows, the node keeps `_zero_rows_in_row_order` (ITL-1131).
- **Async**:
  - The node passes its `BatchInput`s straight to the operator's `async_execute` and
    forwards the operator's batches.
  - It keeps their concatenation, and stores it in `LOG` mode.
  - REPLAY sends the stored table's batches and drains every input.
  - A run with no rows keeps the zero-row table of its own description.
- Operators have no per-row error policy: a failure fails the node, as today.

### Function job node

The function is still called once per row. Everything around the call is a table.

1. **Input as a table.** For each input row the node computes the base entry id from that
   row's slice of the table: the tag side with system tags, plus the input data hash. It is
   the same one-row preimage `_build_record_id_preimage` builds today, still per row
   (hashing is ~0.3% of the run). No datagram is made.
2. **Lookup as a table.** `get_cached_results` finds the same entries. The loader keeps the
   stored joined table and a map from base entry id to row. Hits are taken from it by row
   index.
3. **The call.** For each miss, the parameters' values are taken from the input table,
   converted to Python once per column per table or batch, through the data context's type
   converter (the conversion `Data` applies per row today). The function is called with
   today's logger, invocation context, error policy, commit cadence (`commit_every_rows`,
   `_row_processed`) and database error handling. Misses that share a base entry id are
   called once and the result reused, as the sync path's in-run lookup does today. This is
   also what ITL-1062 asks of the async path.
4. **Results as a table.** Returned values are collected per output column and converted to
   Arrow once, under the declared output Arrow schema. The per-row items stay per row: the
   output data hash, source-info tokens and the result's datagram id (`uuid7`, stored as
   `__data_id`). Pipeline records and result rows are written as tables per commit window,
   with today's rows, columns and values. Rows written before a failure stay written.
5. **Output.** In input order, assembled from hit rows and computed rows by row index:
   - a `None` result or a failed row gives no row;
   - two equal inputs give two rows;
   - the column order is the first output row's (developer decision 2).

   Async keeps today's order: stored hits first, then computed rows as they finish.
6. **Load-only**: the hits-only case, with the same `not_stored` count.
7. **The node's own reads** come from its tables, in cache order (developer decision 3).
8. **Stored values decoded as a class.** A `TypeIdentityError` (ITL-881) must stop the run
   at the same point as today. Stage 3 first establishes where it is raised today: at
   datagram construction, or at first Python access. If at construction, the node checks
   each user-class column of the stored table once before it passes a row on.
9. **`EmptyData` inputs** (an ephemeral upstream result that is gone, ITL-605). Stage 3
   first establishes how such a row crosses an edge today, and keeps it. The `NodeTable`
   carries those rows' cached content hashes beside the table, never as a column. ITL-605's
   materialization thunks later use the same record to ask the orchestrator to recompute.

### Side-effect job node

- Delivery calls the user's function with one row's values taken from the table.
- The node passes on the input table restricted to the rows passed on: a mask over row
  indices.
- A load-only run passes its input table on unchanged.
- The node's held rows become a held table.

## Per-row concerns without a row transport

### Observers

The protocol keeps its per-row hooks and its event order. `on_data_start`, `on_data_end`
and `on_data_crash` fire per row of a function or side-effect node, and operators and
sources call only the node hooks. What a hook receives becomes a **row view**: one internal
class, `RowView`, that satisfies `TagProtocol` or `DataProtocol` over a table and a row
index, and builds the datagram only when it is first read.

- The run's own `RunCountingObserver` reads only the label and the `cached` flag, so it
  makes no row.
- The status and logging observers read tag values, so they make the tag datagram of the
  rows they report.
- A user's observer gets what it gets today.

Event order stays as today:
- sync: input order, hits and misses interleaved;
- async: stored hits first, then rows as they finish.

The async function node now finds a row's input again through a `_TAG_NODE_INPUT_REF` meta
column (`_NodeLabelObserver`). It matches by row index instead.

**ITL-1220 (attachments, future).** An attachment is an effect-only function invoked for
every row a stream yields. Under this design the yield is a batch, so a stream runs its
attachments over row views of each batch, in row order, exactly where observers are told.
The description is where a stream's attachments are listed. Nothing here blocks the spike.
Its "halt" signal maps onto closing the output and cancelling the run's task group, which
the orchestrators already do for `fail_fast`.

### Error policy

A row can fail only in a user's function or a delivery, both already per row. The node
wraps each call:
- `continue`: the failed row is left out by mask, counted, and reported through
  `on_data_crash` with its row view;
- `fail_fast`: the node raises at once;
- database errors and `TypeIdentityError` stop the run whatever the policy.

## How each rule holds under table transport

### ITL-845: each row keeps its own source info

A node table and a batch take the `_source_*` columns of the node's table row by row, and F
touches no value. So every row carries its own tokens, also list-valued ones after
`Batch`, `GroupBy` or `MergeJoin`. Source-info nullability is a whole-table property. It is
applied where a whole table is made: a sync node table, an operator node's kept output,
`materialize()`, and a function node's output table. It is never applied per batch. Batches
carry the fields as nullable, as the description states them. A per-batch flag would give
two batches of one channel two schemas, and `concat_tables` would refuse them.

### ITL-1122: large types

A table is normalized once, where it becomes a stream (`ArrowTableStream.__init__`). Node
tables and batches are taken from such tables by selecting, slicing or `take`, which keep
types. A function node's results are converted under the declared output schema, which is
large. Nothing is normalized per row, and nothing is normalized twice. F step 3 keeps the
one observable effect of today's per-row normalization.

### Async equals sync

Both orchestrators run one logic per node kind, and every input carries the same
description on both paths:
- the sync node applies the logic to its table;
- the async node applies it per batch for a per-row step, or to the materialized table for
  a step that needs all of its input.

Validation, input order, key checks and zero-row results read the description, so they are
one implementation. `test_async_sync_equivalence` stays the check, with the standing guards
below.

### ITL-1133 (and ITL-1131): one column order

F places every node table's columns in row order. An operator node keeps and stores `F` of
its result, sync and async, so it has one column order in memory and in its stored table. A
zero-row result keeps `_zero_rows_in_row_order`. The description's `arrow_schema()` is in
the same order, so a zero-row materialized input has the populated order too.

### Row order

Each node passes on its rows in the order its successors receive them today:
- a source: its table order;
- an operator: its result order;
- a function node (sync): input order;
- a held function node: its held order;
- a side-effect node: delivery order;
- async: as today.

### Staleness

Every node calls `_update_modified_time` at the same points, in the same order, so `is_stale`
and the next run's hold decision are unchanged. An operator's `as_table()` triggers the same
lazy computation `iter_data()` does today. Each stage runs the ITL-1095 and ITL-1121 tests.

### Run summary

The counts come from the same three places:
- the counting observer, which still sees one event per row;
- `last_run_not_stored`;
- the plan's held counts, now read without rows.

### Bounded channels (ITL-1075-AH1)

The rule stays: every node reads each input to its end, concurrently. `materialize()` and
`drain()` read to the end. REPLAY drains its inputs while it sends. `SemiJoin` reads its
left input while it collects its right one. A channel now holds `buffer_size` batches, so
what fits in a cycle of channels is counted in batches. The deadlock tests of ITL-1075 run
with one-row batches as well as full ones.

## What this closes (ITL-858)

| Case | Status on main | Closed by | Stage |
|---|---|---|---|
| Key types on an empty input, `Join` / `SemiJoin` (ITL-856) | open | checks from `arrow_schema()` before the first batch | 5 |
| Column order (ITL-854, done; the async column-order xfail `ASYNC_ORDER_XFAIL`, ITL-1249, `tests/test_core/operators/test_several_items.py:211`) | ITL-854 done; ITL-1249 xfail open | async rows materialized in the description's order; the xfail is removed if its case passes | 5 |
| Data context of an async input (ITL-844) | open | `data_context_key` of the description | 5 |
| A row of another data context (ITL-951) | sync: whole-input error; async: answered from the cache | whole-input error before the first batch, from the description (ITL-858 decision 9) | 5 |
| Zero-row results, async (ITL-865) | narrow fix: emits nothing | `materialize()` of an empty input gives the typed empty stream | 5 |
| System-tag names after a non-source input, async (ITL-906) | narrow fix through `input_pipeline_hashes` | a materialized input reports its own pipeline hash; the keyword goes | 5 |
| Zero-row read-back types of an operator node (ITL-912) | fixed (`typed_zero_row_table`) | the same derivation, now the description | 2b (sync), 5 (async) |
| Operator state written at validation (`SemiJoin`, `Index`) | present | read from the description | 5 |
| Duplicate function calls on async (ITL-1062) | open | misses deduplicated by base entry id before dispatch | 5 |
| A loaded node's description (ITL-858 decision 7) | – | saved Arrow type texts (ITL-640) | 2 |

Standing equivalence guards, added in stage 5 to `test_async_sync_equivalence.py`:
- **One registry** of every operator class with an `async_execute`. A test fails when a class
  is missing from it.
- **Four input shapes per case**: rows; zero rows on every input; zero rows on one input;
  inputs from an operator instead of a root source.
- **Declared against materialized**: `describe(stream).arrow_schema()` equals the
  materialized table's schema (with the two stated exceptions), sync and async, for every
  operator, source kind and function node. This guard is added in stage 2, before anything
  depends on it.
- **Batch layouts**: one batch, one row per batch, mixed sizes.

## What must stay byte-identical, and how it is proven

### Must not change

1. **Every hash.** `generate_golden_hashes.py --check` matches. Per node: content hash and
   pipeline hash, record ids, base and pipeline entry ids, output data hashes, source-info
   tokens, system-tag column names and values, folded values (`batch_v1:` / `group_v1:`).
   The description is not fed into any hash (ITL-858 decision 3).
2. **Every stored table.** Function pipeline and result tables, operator cache (`odb_v1`),
   side-effect log (`tdb_v1`), cached source (`sdb_v1`): path, column names and order,
   types, nullability, field metadata, values, and the row order written.
3. **Every output a user reads.** `as_table(all_info=True)`, `iter_data()` rows, the `head()`
   and `run()` views with status and notes, and `as_source()` / `DerivedSource`, for every
   node.
4. **Every successor's input.** The table each operator, function node and side-effect node
   receives: columns, order, types, nullability, metadata, values and row order.
5. **Run behaviour.**
   - `RunSummary` and `NodeRunSummary.held`.
   - The warnings issued: `RowFailuresWarning`, `RestrictedAggregationWarning`,
     `SourceCheckFailedWarning`.
   - Every error raised, and the point where it is raised: database errors,
     `TypeIdentityError`, `fail_fast`, restriction refusals.
   - The observer event sequence and the values an observer reads.
   - What logging capture records.
   - Which nodes a second run holds.

Not covered: the Arrow chunk layout, datagram object identity, timing. Not covered either,
by design: the changed signatures, which are the one breaking change.

### Proof

1. **Equality tests**, kept permanently (`tests/test_pipeline/test_node_table_equivalence.py`).
   `R` and `ArrowTableStream.iter_data` are the oracle. Each comparison is
   `Table.equals(check_metadata=True)` plus field-by-field schema equality on combined
   chunks.
   - **L0 (idempotence)**: a node table `T`, made into its node's input, gives
     `R(rows of it) == T`.
   - **L1 (operators)**: each operator result's node table equals `R(list(S.iter_data()))`.
     This covers every exported operator, and REPLAY.
   - **L2 (sources)**: for each source kind bound to a slot, with and without a declared
     form, the node table equals `R(list(node.iter_data()))`. Kinds: in-memory, CSV, Delta,
     SQLite, `CachedSource`, `DerivedSource`, `PollingSource` (also counting its polls),
     SpiralDB through its fakes. PostgreSQL runs in its own job.
   - **L3 (entry ids)**: per input row, the id from the table slice equals
     `compute_base_entry_id(tag, data)`, also for a stream outside a job (`TableInput.of`).
   - **L4 (function node)**:
     - the values given to the function equal today's `function_input(...).as_dict()` values
       (`==`, same types);
     - the results table equals `R` of today's result datagrams;
     - the output table equals `R` of today's output rows, with the first row a hit, and
       again with it a miss.
   - **L5 (held)**: a held node's `NodeTable` and count equal today's.
   - **L6 (batches)**: for every node kind, the materialized batches equal the sync node
     table, as a set of rows (in order for sources and operators).
   - **L7 (descriptions)**: the declared-against-materialized guard above.

   The corpus is the several-items family (`tests/test_core/operators/several_items.py`):
   two or more tags and data columns, in several arrangements and roles. It adds:
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
2. **Existing suites**, targeted as each stage lands:
   - `test_async_sync_equivalence`, `test_order_invariance`;
   - the operator, function and side-effect node tests;
   - `test_pipeline_job`, the observer tests;
   - the ITL-1131/ITL-1133 column-order tests;
   - the ITL-1095/ITL-1121 staleness and load-only tests;
   - the ITL-1075 deadlock tests;
   - the run summary tests;
   - the golden check.
3. **End-to-end differential**, a script in `docs/dev/scripts/itl_1347/`. It runs fixture
   pipelines once on a pinned `origin/main` checkout and once on the branch. The pipelines
   use the tutorial dataset (`docs/tutorial/workshop_data.py`) plus the corpus. Each runs
   cold, warm, held and load-only, on both orchestrators. The script dumps every stored
   table (Arrow IPC), every node's `as_table(all_info=True)`, the `RunSummary`, and a
   recording observer's events with the values it read. It compares them byte for byte, and
   its output goes on each stage's PR.

## Migration and staging

### One breaking change

Every public signature in "Protocol change" changes in **one** PR, the switch:
- the node protocols' `execute` / `async_execute`;
- operator and pod `async_execute`;
- the batch channels;
- `node_outputs`;
- the orchestrators' `held`;
- the removal of `input_pipeline_hashes`.

Until the switch, the stages ship behind the old public signatures. The orchestrators call
new internal methods, `_execute_table` / `_async_execute_batches`, and each old public method
is a thin form of the new one: `execute` returns the rows of its `NodeTable`. These are not
compatibility shims kept after a change (CLAUDE.md). They exist only between stages of
unreleased work, and the switch deletes them, renaming the internal methods to the public
names. Question 7 asks the developer to confirm this, or prefers switching at stage 2.

The switch lands with stage 5, because the async signatures need batch channels to exist,
and both paths must change together to keep async equal to sync.

### Stages, with the ITL-858 work merged in

Unit costs from the profile: about 26 s of datagram building over 121,139 rows (about
**0.21 ms per row**), and about 14 s of rebuilding. Estimates come from those shares, and
stage 0 replaces them with measurements before stage 1 merges.

| Stage | What | ITL-858 part | Proof | Expected saving, warm sync run of 44.7 s |
|---|---|---|---|---|
| 0 | Measurement only: datagrams per node kind, `R` calls and time by caller, edges per node, on a tutorial-data pipeline of the workshop's shape | measure declared vs. materialized Arrow schema for every operator and source | – | 0 s |
| 1 | **First small step**: the sync orchestrator reuses an operator node's kept table (already `R` of exactly the rows it passes on) for each successor, for held operator nodes too; held counts without rows. Behind the old signatures | – | by construction; one test per edge | about **4–8 s** |
| 2a | Sync, sources: `describe()`, `NodeTable`, `InputTable.over`; sources return `F` of their table; held nodes return node tables. Behind the old signatures | declared description on every sync edge; the declared-against-materialized guard; loaded nodes describe themselves | L0, L2, L5, L7 | together with 2b about **14–20 s** |
| 2b | Sync, operators: kept and returned table `F(result.as_table())`; REPLAY likewise. Behind the old signatures | – | L0, L1, L5 | (with 2a) |
| 3 | Function nodes, sync: table input, entry ids from slices, hits from the stored table, calls on values from the table, results per column, row views for observers, own reads from tables | – | L3, L4, differential | about **8–14 s** |
| 4 | Side-effect nodes, sync: masked pass-through, load-only pass-through, held table | – | L0, differential | small |
| 5 | **The switch**, plus async: batch channels with descriptions; every `async_execute` on `BatchInput` / `BatchOutput`; barrier operators by `materialize()`; `Join` / `SemiJoin` checks before the first batch; per-row operators per batch; validation state removed; `input_pipeline_hashes` removed; function node async with dedup; public signatures renamed; release note | ITL-856, ITL-844/951 async, ITL-865/906 general form, ITL-1249 xfail, ITL-1062; standing guards | L6, L7, `test_async_sync_equivalence`, differential | the same order of gain for async runs |

The ranges overlap. The bound for stages 1–4 is the ~40 s (~90%) the profile puts on
conversions, and the floor is the plain-stream time (1.9 s) plus the lookups and writes a
job adds. A warm sync `job.run()` of about 3–6 s is the target after stage 3. The
`LazyModule` cost (42.9 million `__getattr__` calls; 45.1 s → 40.7 s in the profile's
experiment) is a separate fix in this round.

Each stage is one issue and one PR, with its equality tests, the targeted suites, the golden
check and the differential output. Benchmarks follow the round's rules.

### Release note (for the switch)

In `changelog.d/<issue>.md`, under `### Breaking changes and migration`:

> #### Job nodes and operators exchange tables, not rows
>
> Code that drives nodes or operators directly changes; pipelines, jobs, `job.run()`,
> `run_async()`, sources, operators used in pipelines, and every stored table and hash are
> unchanged.
>
> - A node's `execute()` returns a `NodeTable` (`.table`, a PyArrow table of the node's
>   rows, and `.description`), not a list of `(Tag, Data)` pairs. It takes a `TableInput`:
>   wrap a stream with `TableInput.of(stream)`. To get rows:
>   `ArrowTableStream(result.table, tag_columns=...).iter_data()`.
> - `async_execute()` of nodes, operators and function pods takes `BatchInput`s and a
>   `BatchOutput` (`orcapod.channels.BatchChannel(description, ...)`; `.reader`, `.writer`)
>   instead of row channels. A channel's description is built with `describe(stream)`.
>   Batches are PyArrow record batches.
> - The `input_pipeline_hashes` argument of operators' `async_execute()` is removed: each
>   input states its own `pipeline_hash()`.
> - A side-effect node's `async_execute()` takes one input, not a list of one.
> - `OrchestratorResult.node_outputs` maps each node to its `NodeTable`.
> - The orchestrators' `held_rows=` argument is now `held=` and takes `NodeTable`s.

## Developer decisions (2026-11-22)

The four questions the first version asked, answered by the developer (also recorded on
ITL-1347):

1. **`OrchestratorResult.node_outputs` holds node tables.** A caller makes rows with
   `iter_data()` if needed. It changes with the one breaking change (stage 5).
2. **A function node's first-row column order is kept exactly.** Its output columns follow
   its first output row, which differs between a stored row and a fresh one. Every stage
   reproduces this (L4 checks it). One order for it is future work, ITL-1353.
3. **A function node's own reads keep cache order for now.**
4. **Stages run in order 0 → 1 → 2 → 3 → 4 → 5.** Stage 2 is split into 2a (sources) and 2b
   (operators), which can go to two workers in parallel.

Also from the developer (2026-11-22): async execution handles an object that wraps the
channel and carries the stream's information (this revision), and the protocol change is
spelled out in detail (the sections above).

The developer approves the spec as a whole before stage 0 starts.

## New questions for the developer

5. **Source-info nullability** stays decided by the materialized table's values: the one
   column property the description does not declare. This keeps stored field flags and the
   operator node's record hashes. Keep it (proposed), or declare a fixed flag? A fixed flag
   moves those flags and hashes, so it needs its own decision.
6. **Batch and buffer size.** The proposal: a source sends batches of at most 65,536 rows,
   and an operator sends its result's own record batches. A channel buffers 8 batches by
   default. That bounds a channel at about 0.5 M rows, against 64 rows per channel today. A
   smaller batch keeps memory lower and streams sooner; a larger one costs less per batch.
   Accept the defaults, or set others?
7. **One breaking change.** Proposed: the old public signatures stay until the switch in
   stage 5, as thin forms of the new internal methods that the switch deletes. Accept, or
   switch the sync signatures in stage 2 and the async ones in stage 5 (two breaking
   changes)?

## Future work (not designed here)

- One output column order for a function node, whatever its first row (ITL-1353).
- Source-info nullability declared by the description (question 5).
- A `null` column through the job layer (ITL-1319).
- `Schema` carrying its Arrow schema (ITL-1307): the description reads it.
- Attachments on streams (ITL-1220) and materialization thunks (ITL-605), placed above.
- Batching the side-effect completion lookup (one `get_record_by_id` per row today).
- Polars as a node table's backing, where an operator works in Polars.
