# ITL-1472: implementation plan

The design is `docs/dev/specs/itl-1472-edge-registration.md`; its decisions D1 to D11 were decided on the
developer's behalf on 2026-11-26 and are revertible. O34 (the automatic select) is the developer's revision of
the same day: there is no selection node; a function pod reads only the columns its function takes and its
node's identity tuple gets one label when it drops any (the design's section 4a), which is stage 3a.

Each stage is one pull request against `main`, with its own Linear issue. Targeted tests locally, one pytest
process; the suite in CI. **Stage 3a is the only stage that moves a hash**: it changes the golden entries
listed in `docs/dev/specs/itl-1472/golden-changes-o34.md`, exactly those, after the developer approves the
list. In every other stage no content hash, pipeline hash, stored path, stored column or golden value moves;
the golden suite is the check.

| Stage | Issue | Content | Depends on |
|---|---|---|---|
| 1 | ITL-1483 | Graph edges carry their input position | - |
| 2 | to file | Edges as the public and the saved structure | 1 |
| 3a | to file | A function pod reads only its columns; no selection node (moves hashes) | approval of the golden list; independent of 1 and 2 |
| 3b | to file | The restriction on the edge | 2, 3a |
| 4 | to file | The old rules go; cached operators; the edge view; visibility; docs | 3b |
| later | to file | `job.restrict` / `job.unrestrict` on any edge; filter push-down | 4; ITL-1219 |

Stages 1 and 2 touch the lines of both orchestrators that the ITL-1464 integration branch changes: whichever
merges second merges `main` in (no rebase).

## Stage 1 (ITL-1483): graph edges carry their input position

**The bug on `main`.** The pipeline's graph is a set of `(upstream key, node key)` pairs (`_hash_graph`,
`_graph_edges`, the `OrcaDAG` of nodes), so one node feeding two inputs of an operator is one edge.
`MergeJoin(A, A)` and `SemiJoin(A, A)` build a node with two inputs, and the run hands the operator one
stream: "BinaryOperator requires exactly two input streams" (`collapse_on_main.py`, heading 5; pinned today
by `test_one_stream_given_twice_fails_to_run_in_a_job`).

**Change.**

1. An edge is `(upstream key, node key, position)`. `position` is the input's index when the pod's
   `argument_symmetry` is a tuple (a function or side-effect pod: 0; `MergeJoin`, `SemiJoin`: 0 and 1) and
   `None` for a commutative pod (`Join`, `Rejoin`). One rule, in one function, used by `_record_invocation`,
   `compile`, and the loader.
2. `compile` keeps the positioned edges (`_graph_edges`); the node DAG stays a graph of nodes (topological
   order, ancestors), and a node's inputs are read from its positioned edges.
3. Both orchestrators give a node its inputs through one function, "the streams of this node's input edges,
   by position", in place of `_single_input` / `_ordered_inputs` (sync) and the pair-keyed readers (async:
   one reader per edge, so one upstream feeds two positions through two readers). A source node's observer
   events and its one read per run are unchanged.
4. Held rows (`node.run()`, the state rule) feed every position alike.
5. Save and load: no format change in this stage. An operator's saved `inputs` list already holds the
   argument order (ITL-1417); the loader builds the positioned edges from it.
6. `diagram()` draws one line per edge, so two for `MergeJoin(A, A)`.

**Not in this stage:** any public name (`job.edges`), any restriction, any change to a refusal.

**Done when.**

- `MergeJoin(A, A)` and `SemiJoin(A, A)` run, sync and async, in a `Pipeline` bound to a job and in a
  `PipelineJob`, after `PipelineJob.load` (`load_only`), and through `node.run()`. Rows equal the standalone
  `MergeJoin()(A, A)` and `SemiJoin()(A, A)`.
- `MergeJoin(A, B)` against `MergeJoin(B, A)`, and one upstream at position 0 with another at 1 and the
  reverse, give what the standalone operators give.
- Inputs with two or more tag and two or more data columns, in more than one arrangement.
- `Join(A, A)` and `Rejoin(A, A)` stay refused by the operators themselves.
- `test_one_stream_given_twice_fails_to_run_in_a_job` becomes the test that it runs.
- The golden suite, `test_order_invariance.py`, `test_graph_walk_cost.py` and the sync/async equivalence
  tests pass unchanged; observer event order for source nodes is unchanged (`test_sync_orchestrator.py`,
  `test_rows_on_read.py`).

## Stage 2: edges as the public and the saved structure

- `Edge(source, target, position, condition, limit)`, a frozen value in `orcapod.pipeline`; `job.edges`
  (sorted by `(target, position, source)`), `job.edge(source, target, position=None)`; the same on `Pipeline`
  (D1, D3). The restriction fields exist and are always empty in this stage.
- Saved format (D10): `pipeline.edges` as records `{"from", "to", "position"}`; node keys unchanged; both
  format versions raised; a repeated triple or a dangling edge refused on load; the operator's `inputs` list
  goes. Byte-equal files for every recording order.
- The one function of stage 1 becomes "the stream of an edge" (upstream stream, condition, limit), the
  restriction a no-op.

Tests: selection (ambiguous without a position when a node feeds two; unknown edge), order of `job.edges`,
save and load round trip, an earlier format refused, the order-invariance case over recording orders.

## Stage 3a: a function pod reads only its columns

The one hash-moving change, in its own pull request so that its golden diff is exactly the approved list
(`docs/dev/specs/itl-1472/golden-changes-o34.md`, section 2). It does not depend on edges and can go first.

- `_FunctionPodBase.__call__` adds no `SelectDataColumns`. In the default mode, on an input with data columns
  that are neither parameters nor fall-through data-tags, the node's identity tuple gets one
  extra label element: `(pod, inputs, "drops_extra_columns")`, content and pipeline identity alike. The label
  is a constant (`DROPS_EXTRA_COLUMNS`) beside the node's identity code; one function appends it for the
  node, the pod's stream and the invocation key, and a test finds no other literal. In every other case the
  tuple is exactly today's. `strict=True` and `process` refuse an extra column, as today.
- The function node, the plain `FunctionPodStream` and the async path narrow each input row to the function's
  columns (and the fall-through data-tags) at one place, before the input data hash, the entry and instance
  ids and the call. The input data hash must equal the one today's select gives, so stored results are found.
  A node with the label ignores every data column its function does not take; one without it refuses an
  extra column. Source info of an ignored column is not carried.
- Save and load: `"drops_extra_columns": true` in the function node's descriptor, only when the node has the
  label; the loader rebuilds the node with it.
- `explain()` and `diagram()` name the ignored columns on the pod, computed from the input's columns and the
  function's parameters.
- Provenance (`trace`, `why_missing`) narrows an input row to the function's columns before it compares the
  stored input data hash, as the node does (the prototype's test run shows it does not today).
- Golden: `--remove` each entry of the approved list, then the generator writes the new values; the diff of
  the golden folder is the list and nothing else. New cases, added without touching a pinned value: a pod
  ignoring two columns in both input column orders (one identity), beside the hand-written select (another
  pipeline hash, equal input data hashes), under a `ctx` pod and a fall-through pod.
- Docs and rules: `CLAUDE.md` and `.zed/rules` ("Calling a function pod": ITL-807's equivalence with the
  hand-written select is withdrawn, and the default call adds no step), `docs/concepts/function-pods.md`, the
  pages that print a node list holding `SelectDataColumns`, a changelog fragment under "Breaking changes".

Done when: the golden diff equals the approved list; a pod called on a stream with no extra column,
`process` and `strict=True` are unchanged in every hash; results computed before the change, and by the
hand-written select, are served (no function call); the tests counted in the golden document change for the
reasons given there and no other test changes; sync and async agree.

## Stage 3b: the restriction on the edge

- `SourceProtocol.unrestricted()` and `.restricted(condition, limit)`, required members; `SpiralDBTableSource`
  implements both; a source that cannot push down refuses `restricted` (D7).
- Capture: a source stating a restriction at a pod's input becomes the unrestricted leaf and the edge's
  restriction; `EdgeRestrictionConflictError` (in `errors.py`) for two different restrictions on one edge,
  before anything is recorded, in either order, naming the edge, both restrictions and the filter remedy.
  One node feeding a commutative operator twice is one edge (D2).
- No rule for the automatic selection: with stage 3a there is no selection node, so `f(A_x)` and `h(A_y)`
  restrict the two edges `A -> f` and `A -> h`.
- Reads: one pushed-down read per source node and distinct restriction among its edges into nodes that
  run; the full read only for an edge without one; the source node itself reads the whole table when read.
- `bind`: a source stating a restriction is refused (D5); a source that cannot push down is refused for a
  slot with a restricted edge (D7); both before anything is bound.
- Leaf streams of one identity over different restrictions refused (D8), until ITL-1387.
- Save and load of `where` and `limit` on the edge records; the bound source's config holds neither.
- `require_one_restriction` and its calls are replaced by the edge rule in this stage, since the two cannot
  both hold; the operator-cache refusal stays until stage 4.

Tests: the design note's list, items 1 to 6, 8 and 9 (several restrictions, every conflict in every order,
positions, roles of the restricted column, scans counted, state rule, side effects, save, load and bind).
With stage 3a: `f(A_x)` and `h(A_y)` taking the same columns are accepted in both recording orders, with
two pushed-down scans and no full scan; `f(A_x)` and `f(A_y)` are refused on `A -> f`; sync and async.

## Stage 4: the old rules go; visibility

- `refuse_restricted_operator_cache(s)`, `refuse_restriction_conflicts` and `Rebinding` go; cached operators
  behave as the note's section 7; one warning per run for a `REPLAY` node below a restricted edge (D6).
- `job.edge_stream(edge)`: the upstream node's active rows under the edge's restriction, stored nowhere;
  the limit as D9.
- `diagram()` and `explain()` name each restricted edge; `RunSummary.restricted_edges`; the by-slot fields
  stay for a restriction inside a stream bound to a slot (D11); `warn_restricted_aggregation` reads edges.
- Docs: `docs/concepts/sources.md`, `docs/concepts/identity.md`, the API reference entries, `CLAUDE.md` and
  `.zed/rules` (the restriction paragraph).
- Golden: one provisional case pinning that a restricted edge moves no value.

Tests: the note's items 5, 7 and 10; the changed tests of `test_spiraldb_table_source_where.py`, counted by
reason in the note's section 10.

## Later

- `job.restrict(edge, mapping=None, /, *, limit=None, **columns)` and `job.unrestrict(edge)` (D4): replaces
  the edge's restriction, moves the edge's modified time, so the target and everything below are stale;
  nothing stored is touched. On an edge whose upstream is not a source it is applied with the filter
  operator's code. This is how a window is moved on a built job (the cost D5 accepts until then).
- A restricted edge whose source cannot push down (D7), with ITL-1219.
- A filter directly on a source carried out as a pushed-down read (the note's section 9).
- Probes on edges (ITL-1473): the edge triple is their address; what a probe remembers is its own state.
