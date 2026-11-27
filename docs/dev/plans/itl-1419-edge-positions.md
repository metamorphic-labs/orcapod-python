# ITL-1419: graph edges carry their input position (stage 1 of ITL-1472)

The design is `docs/dev/specs/itl-1472-edge-registration.md` (draft PR 580) and its plan,
`docs/dev/plans/itl-1472-plan.md`, "Stage 1". This is that stage only.

Developer decision (2026-11-26): `MergeJoin(A, A)` is well defined, because its two input
positions are two edges, and it must run in a job. Refusing it (ITL-1419's other option) is
not taken.

## The bug on `main`

A pipeline's graph is a set of `(upstream key, node key)` pairs (`_hash_graph`, `_graph_edges`,
and the `OrcaDAG` of node objects the orchestrators walk). One upstream given to two inputs of
an operator is one pair, so `MergeJoin()(a, a)` and `SemiJoin()(a, a)` reach the orchestrator
with one predecessor: the sync orchestrator hands the operator one stream ("BinaryOperator
requires exactly two input streams"), the async one one reader. Pinned by
`tests/test_pipeline/test_loaded_argument_order.py::test_one_stream_given_twice_fails_to_run_in_a_job`.

## One stream given twice to a commutative pod: refused for now (a stopgap until ITL-1511)

The refusal below is temporary, not a principle. A commutative operator taking one stream
more than once is meaningful, and ITL-1511 ("Allow a commutative operator to take one stream
more than once: multiset identity and the unique/repeat toggle") is to allow it; it must be
settled before 0.4.0.

Independent review of the first head found that a commutative operator hashes its operands
as a set, so `Join()(d)` and `Join()(d, d)` (where `d` has no data column besides its
data-tags, so no overlap refuses it) shared one content hash and cache path but gave
different rows (5 against 3 for `id = [1, 1, 2]`), and which one a job kept followed the
order the steps were recorded in. The round-2 review found that a check by object identity
missed two separate sources of one identity (standalone hashes still collided, and a step
recorded over them compiled in one order and failed in the other), and that the check
added its own error for an operand that cannot be hashed.

On 2026-11-27 the developer approved refusing repeated operands as a stopgap until
ITL-1511. `core/operators/repeated_operands.read_argument_symmetry` reads the operator's
grouping and raises `RepeatedOperandError` (an `InputValidationError`) if an unordered
group has collapsed repeated operands. The check applies to built-in and custom
operators because it reads their result. The error names the operator whose own
operands repeat. A downstream pod or the function pod whose join received the operands
can add context with `add_context`. Hashing, recording and compiling form the identity.
Set formation completes before checking for collapsed operands, so the existing hashing
error for an unhashable operand wins in either argument order. Standalone row reads such as
`Join()(d, d).as_table()` or a function pod's row read over several inputs can proceed
when they skip identity formation. A job refuses the same invocation. The developer
accepted this discrepancy as part of the stopgap, and ITL-1511 will remove it.

## Change

1. **One rule for positions** (new `src/orcapod/pipeline/edges.py`): `input_positions(pod,
   inputs)` gives each input of an operator its argument index for ordered groups
   (`MergeJoin`, `SemiJoin`: 0 and 1), including nested ordered groups. A flat
   `frozenset` of the inputs gives each position `None` (`Join`, `Rejoin`, even with
   one input). This is the same distinction used by the legacy loader. Changes to how
   operators declare symmetry are tracked separately in ITL-1548. A function or side-effect pod's
   one input is at 0. Positions exist in memory. When ITL-1511 allows repeats, each
   identical edge will get an occurrence number so it can be identified individually. Used by
   `_record_invocation`, `compile`, the `PipelineJob` rebuild of a blueprint and the loader.
2. **Hash-level edges are triples.** `_graph_edges` becomes a sorted list of
   `(upstream key, node key, position)`. `_hash_graph` (networkx, used for topological order and
   ancestors) stays a graph of keys. Every consumer of `_graph_edges` in `job.py` and `graph.py`
   reads the triple.
3. **No saved-format change.** `save()` writes the distinct `(upstream, node)` pairs, sorted, as
   today (byte-equal files). The loader builds the triples from each operator's saved `inputs`
   list (ITL-1417: argument order), position 0 for a function or side-effect node; a legacy
   file without `inputs` keeps today's refusal/commutative rule.
4. **One function gives a node its inputs** (`node_input_edges(node, graph)` in `edges.py`):
   the node's input edges in position order, one per position, each naming the upstream node
   of the graph. It reads the node's own `upstreams` (built from the invocation's inputs in
   argument order, the same order as the triples) and checks each is a predecessor in the
   graph, by identity. It replaces `_single_input` / `_ordered_inputs` (sync) and the
   predecessor sorting (async).
5. **Async: one reader per edge.** Channels are wired per positioned edge, so an upstream feeding
   two positions of one node gets two readers of its broadcast channel. A held node drains
   every one of its edge readers. A source node still executes once per run; its observer
   events are unchanged.
6. **Held rows** (`node.run()`, the state rule) feed every position alike: the sync orchestrator
   reads `outputs[upstream]` per edge, the async one sends the held rows to every edge reader.
7. **Diagram.** `diagram()` draws one line for both inputs of `MergeJoin(A, A)` because
   the diagram keys its edges by node pair and role (`(start, end, constrains)`).
   ITL-1530 tracks routing parallel edges in the layout.
   `SemiJoin(A, A)` already draws two lines (plain and constraining, ITL-1455).

Not in this stage: any public name (`job.edges`), any restriction, and
nothing in the id code of `core/nodes/function_node.py` / `operator_node.py` (ITL-1466).

## Hashes

No content hash, pipeline hash, stored path, stored column or golden value moves: node keys
are unchanged (invocation content hashes), and positions enter no hash.
`generate_golden_hashes.py --check` must report no drift.

## Tests (first, shown failing on `main`)

New file `tests/test_pipeline/test_edge_positions.py`, inputs with two tag and two data columns
(`id`, `key`; `x`, `y`), in two layouts (tag order and column order swapped):

- `MergeJoin(A, A)` and `SemiJoin(A, A)` run, sync and async, in a `Pipeline` bound to a job and
  in a `PipelineJob`; after `save` and `PipelineJob.load` (`load_only`); and through
  `node.run()` (with a held upstream). Rows equal the standalone operator's.
- `MergeJoin(A, B)` and `MergeJoin(B, A)`, and a chain where one upstream is at position 0 and
  another at 1 and the reverse, give the standalone rows.
- A source node's observer events are the same for `MergeJoin(A, A)` as for `MergeJoin(A, B)`
  per source (one start and end per run).
- Exercise `RepeatedOperandError` when hashing and recording `Join`, `Rejoin` and function
  pods with repeated operands (ITL-1511), including both recording orders and sync and
  async jobs.
- The saved file of a job with `MergeJoin(A, A)` has one edge pair and `inputs` `[A, A]`;
  saving a loaded job writes the same bytes.
- `test_one_stream_given_twice_fails_to_run_in_a_job` becomes the test that it runs.

Also run: the golden suite, `test_order_invariance.py`, `test_graph_walk_cost.py`, the
sync/async equivalence tests, `test_sync_orchestrator.py`, `test_rows_on_read.py`,
`tests/test_pipeline/`.
