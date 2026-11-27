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

Developer, November 27, 2026: a stopgap until ITL-1511. Until then, refuse exactly where the identity is formed
from the operands as a set. `core/operators/repeated_operands.operand_set` builds that set
and raises `RepeatedOperandError` (an `InputValidationError`) when two operands have one
identity (they are equal as the set compares them). `Join.argument_symmetry` (so also
`Rejoin`), the commutative branch of `BinaryOperator.argument_symmetry` and the function
pod's `argument_symmetry` call it. The error names the operator whose own operands repeat
(`RepeatedOperandError.named`, set once); a pod downstream, and a function pod whose join
got the operands, add context (`add_context`) and never replace that name (verification
of the third head). Whatever forms the identity meets the refusal: hashing, recording,
compiling; a function pod never forms it on the row path. No separate check remains. An operand whose identity cannot be
computed raises what hashing it raises. Rows computed without forming the identity
(standalone `as_table()`) are computed as before.

A commutative operator's inputs have no position also when it has one input (`Join()(a)`):
only a function or side-effect pod gets position 0 for its one input.

## Change

1. **One rule for positions** (new `src/orcapod/pipeline/edges.py`): `input_positions(pod,
   inputs)` gives each input of an operator its argument index when the operator's
   `argument_symmetry` is a tuple (`MergeJoin`, `SemiJoin`: 0 and 1) and `None` when it is a
   `frozenset` (`Join`, `Rejoin`, with any number of inputs); a function or side-effect pod's
   one input is at 0. Used by
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
7. **Diagram: left to stage 2.** The diagram keys its edges by node pair and role
   (`(start, end, constrains)`) and routes them in the layout code, so two lines for
   `MergeJoin(A, A)` need parallel-edge routing there, not a change in the edge listing
   (coordinator, 2026-11-26: leave it to stage 2 if it reaches layout code).
   `SemiJoin(A, A)` already draws two lines (plain and constraining, ITL-1455).

Not in this stage: any public name (`job.edges`), any restriction, any change to a refusal, and
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
- `Join`, `Rejoin` and a function pod given one stream twice are refused (for now: ITL-1511), standalone and
  at recording, in both recording orders, sync and async; `Join(a)` has position `None`.
- The saved file of a job with `MergeJoin(A, A)` has one edge pair and `inputs` `[A, A]`;
  saving a loaded job writes the same bytes.
- `test_one_stream_given_twice_fails_to_run_in_a_job` becomes the test that it runs.

Also run: the golden suite, `test_order_invariance.py`, `test_graph_walk_cost.py`, the
sync/async equivalence tests, `test_sync_orchestrator.py`, `test_rows_on_read.py`,
`tests/test_pipeline/`.
