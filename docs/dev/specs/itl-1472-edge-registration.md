# ITL-1472: a restriction belongs to an edge

Status: proposal, no library code. Round 2 (2026-11-26): this replaces the twin-node design of the first
round. The model (section 1) is the developer's and is not reopened. Section 13 lists the decisions D1 to D11,
each **decided on the developer's behalf, 2026-11-26, revertible**, and O34 (the automatic select), as the
developer settled it the same day: there is no selection node; a function pod reads only the columns its
function takes, and its node's identity tuple gets one label, `drops_extra_columns`, when it drops any
(section 4a).
That is the one part of this design that moves hashes; the exact golden list, for approval, is
`docs/dev/specs/itl-1472/golden-changes-o34.md`. The implementation plan is `docs/dev/plans/itl-1472-plan.md`.

Evidence marks: **[read]** read on `main` (5eb327ad); **[ran N]**
`docs/dev/scripts/itl_1472/collapse_on_main.py`, heading N, on `main` as it is; **[proto N]**
`docs/dev/scripts/itl_1472/prototype.py`, heading N: `main` changed in the script's own process at three places
(the restriction taken off the source and kept per edge when an invocation is recorded; the sync orchestrator
reads per edge; the one-restriction-per-table rule off). The prototype is not the design's code and covers the
sync path only. **[impact]** is the test run of section 10. Each script's output is the `.out` file beside it.

## 1. The model (decided)

- **The graph is built from identity alone**, as if no restriction existed. `f(A.where(x))` is the node `f(A)`
  with `x` on the edge `A -> f(A)`. Two equal sources built separately are one node, as today [ran 4].
- **An edge is `(from node, to node, input position)`** and carries at most one restriction (a `Condition` and
  a limit). A restriction is in no identity.
- **Edges, and the selection of an edge, are the primary concept.** Writing `A.where(x)` at a pod's input is
  one way of stating an edge's restriction; stating it on an edge of a built pipeline (section 9) is expected
  to be the common one.
- **Two different restrictions on one edge are refused, never combined** (also restricted with unrestricted).
  The same restriction written twice is one edge. The remedy is the filter operator, which is a node.
- **A node shows its own rows; an edge is seen as a stream from its upstream node** with the edge's
  restriction applied. Nothing is stored per edge.

## 2. What main does

- `f(A.where(m1))` and `f(A.where(m2))` in one job are refused [ran 1]; the sources and the invocations have
  one hash each [ran 2]. With only the refusal off, the graph is already the model's (one slot, one node), but
  the rows are those of the use recorded first [ran 3]: the restriction stays on the source object the slot
  happened to keep. Holding it on the edge removes that.
- The graph has no positions: `_hash_graph` is a set of `(upstream key, node key)` pairs [read]. So
  `MergeJoin(A, A)` and `SemiJoin(A, A)` build a node with two inputs and one edge, and the run fails
  ("requires exactly two input streams") [ran 5]. Positions fix this as a side effect.
- A job reads a source once per run and hands that read to every node it feeds [ran 6; `SourceJobNode.execute`].
- A stream built before the `with` block is a leaf (a slot bound to that stream), not its steps [ran 7].
- A saved job keeps the restriction in the bound source's config (`bindings.sources.<slot>.where`) and the
  edges as pairs of node keys [ran 8].

## 3. The edge

`Edge(source, target, position, condition, limit)`, a frozen value:

- `source`, `target`: the two nodes' names, the keys of `job.nodes`. Nodes are unique by identity, so a name
  is a node; no new naming rule is needed (the label rule is unchanged). In a saved file an edge names its
  nodes by the keys the file already has (section 8). The triple is a name, not a hash: no label, no row in
  the hash site inventory, in no hash, path or stored value (**D1**).
- `position`: the index of the input when the pod's `argument_symmetry` is a tuple (a function or side-effect
  pod: 0; `MergeJoin`, `SemiJoin`: 0 and 1); `None` for a commutative pod (`Join`, `Rejoin`), whose argument
  order is no identity.
- `condition`, `limit`: the restriction; the empty `Condition` and `None` when there is none. Every edge has
  the two fields, whatever its upstream node is (section 9).

**Ordered operators.** Each position is its own edge: `MergeJoin(A_x, A_y)`, `MergeJoin(A, A)` and
`SemiJoin(A_x, A)` are accepted and well defined [proto 4]. Swapping the arguments is another operator
identity, as today.

**Commutative operators (D2).** One node feeding a commutative operator twice is one edge `(A, join, None)`:
its two uses cannot be told apart, so two different restrictions there are refused like any other conflict.
No operator reaches the case today: `Join(A, A)` and `Rejoin(A, A)` are refused by the operators themselves
(shared data columns) [ran 5].

**Selection.** `job.edges` is every edge, sorted by `(target, position, source)`. `job.edge(source, target,
position=None)` returns one; `position` is needed only when a node feeds two positions of the target.
`pipeline.edges` and `pipeline.edge` are the same on a `Pipeline`, which holds edge restrictions too (a job
made from it starts with them).

## 4. Stating a restriction, and conflicts

**Sugar.** When an invocation is recorded, each input that is a source stating a restriction (`condition`,
`row_limit` of `SourceProtocol`) is recorded as: the source without its restriction (a new required member,
`unrestricted()`; a source with none returns itself) as the pipeline's leaf, and the restriction on the edge
from that leaf to the node. `job.nodes["events"].bound_source` is therefore always unrestricted. The
constructor's `where=` and `limit=` are the same statement.

**Standalone.** A source object outside a pipeline keeps its condition and reads under it, as today. Only the
restriction the given source object itself states is lifted: one inside a `SourceProxy`'s delegate, or beneath
a stream bound to a slot, stays inside that stream (what a slot's binding reads is the binding's own matter).

**The conflict rule** lives where an edge's restriction is set from sugar, at capture:
an edge already written with a different restriction (or with none) raises `EdgeRestrictionConflictError`
(a `ValueError`), naming the edge, both restrictions sorted by their text, and the filter remedy. Restrictions
are compared by `Condition` equality and the limit.

| When | What is checked |
|---|---|
| Capture | Every input edge of the invocation, before anything is recorded [proto 2] |
| Compile | Nothing new: the edges were checked as written; compile only renames keys to node names |
| `bind` | A source that states a restriction is refused (D5), before anything is bound |
| Run | Nothing: an edge holds one restriction by construction and no bound source holds one |

**Order-free (ITL-1271).** The check is symmetric: each of the conflicts is refused with the same message in
both recording orders, and an accepted job has the same nodes, rows and scans in both [proto 1, 2]. Nothing is
"first": `main`'s first-recorded-wins [ran 3] cannot occur, because no restriction is kept on a shared object.

**The first shared node decides.** `op(A_x) -> f` and `op(A_y) -> g` conflict on `A -> op(A)` [proto 2].

**The automatic select.** On `main` a function pod called on a table with columns it does not take adds a
`SelectDataColumns`, shared by every pod that takes the same columns. It would be the first shared node, and
`f(A_x)` and `h(A_y)` would conflict though the user wrote two pods and no shared step [proto 2]. The
developer's answer (O34) is section 4a: there is no such node, so `x` is on `A -> f` and `y` on `A -> h`, and
the plain rule applies with no exception.

With `A.filter(x)` and `A.filter(y)` there is no conflict and one full scan: five nodes (`A`, two filters,
two pod nodes) with section 4a, seven on `main` with its selects [proto 3].

## 4a. A function pod reads only its columns (O34, decided by the developer)

**No selection node.** A function pod's node takes its input stream as given and reads only the data columns
that are parameters of its function (and the data-tags that fall through it, as today). Tags are never
dropped. `f(A_x)` and `h(A_y)` are the edges `A -> f` and `A -> h`, each with its restriction; `f(A_x)` and
`f(A_y)` are one node and are refused. Nothing is hidden, passed through or judged specially: the earlier
answers to O34 (a restriction passing through the select; a selector node of the pod's own) are dropped
with all their machinery (the carry-back to the source read, staleness judged by the source, the selector
kind, its label and its hiding).

**The modes.**

| Call | Extra data columns in the input | What happens |
|---|---|---|
| `pod(a)` | none | as today; today's identity |
| `pod(a)` | some | accepted; they are ignored; one info log names them; the node's identity gets the label |
| `pod(a, strict=True)`, `pod.process(a)` | none | as today; the identity `pod(a)` has |
| `pod(a, strict=True)`, `pod.process(a)` | some | refused, as today |

`strict` is in no identity. A missing required column is an error in every form. A function that accepts
arbitrary keywords takes every column and ignores none. Operators and `SideEffectPod` are unchanged.

**Identity.** One extra pure-label element, `drops_extra_columns`, appended to the function node's identity
tuple exactly when it matters: the default mode **and** an input with data columns the function does not
take. Today the tuple is `(pod, inputs)`; then it is `(pod, inputs, "drops_extra_columns")`, in the content
identity and the pipeline identity alike. No key and value, no class name; the pod's own identity is
untouched. The same tuple is the identity of the pod's stream and the key a pipeline gives the invocation,
so one function appends the label for all three. The label is a constant defined once beside that code and
written as a literal nowhere else. Hashed by `semantic_v1`: no new hash label, no new hash site. The
ignored names are **not** in the identity: they follow from the upstream's schema, which its hash holds, and
the function's parameters, and the input's column order never counts [fold 4]. In every other case the tuple
is exactly today's: the default call, `strict=True` and `process` share one identity and one pipeline table
[fold 3], and nothing moves.

- **The input data hash covers only the columns the function takes**, so the entry and instance ids and the
  result table are those of the hand-written select: a job with `pod(a.select_data_columns(...))` on the same
  store computes nothing after the folded job ran [fold 2].
- The function's `uri` and result table path are unchanged. The node's content hash, pipeline hash and
  **pipeline table path** change for a pod called on an input with extra columns, and for nothing else; so
  does what is keyed by that pipeline hash (the invocation context of a pod taking `ctx`, the system-tag
  column names a later `Join` or `MergeJoin` extends with it).
- A function node with the label ignores every data column its function does not take; one without it
  refuses an extra column, as today. So a compiled or loaded node needs no mode of its own.

**This reverses ITL-807's equivalence.** `pod(a)` is no longer the graph, hashes and stored tables of
`pod(a.select_data_columns(...))`: the first is one node over `a`, the second a select node and a pod node.
They share results, not the pipeline table. `CLAUDE.md` and `.zed/rules` ("Calling a function pod"),
`docs/concepts/function-pods.md` and every page that prints a node list holding `SelectDataColumns` change.

**Shown.** `explain()` says on the pod's line which columns it reads and which it ignores
(`reads age, weight of rejoined; ignores height, score`). `diagram()` lists the ignored columns in the pod's
detail and in the text listing; the picture has one node and one line. Both compute the names from the
input's columns and the function's parameters. A saved job writes `"drops_extra_columns": true` in the
function node's descriptor only when the node has the label; the loader rebuilds the node with it.

**What the edge design gains and loses.** Gains: the edge a user sees is the edge there is (`job.edge("events",
"f")` with no resolution rule); no exception in the conflict rule, the reads or the staleness rule; fewer
nodes; and a place for a later column projection in the read of the edge `A -> f` (the pod's columns are
known where the edge is read). Loses: the select as a node of its own, with rows to look at and an operator
cache; a user who wants that writes the select.

[fold N] is `docs/dev/specs/itl-1472/folded_confirm.py`, heading N, run on the prototype
(`folded-prototype.patch`, variant `O34_MARK=label`); its output is `folded_confirm.out`.

## 5. Reading

A node's input is the stream of its edge (section 6), in both orchestrators, through one function.

- **Edges out of a source node:** each distinct restriction among them is one pushed-down read of the bound
  source, `bound_source.restricted(condition, limit)` (a new required member; a source that cannot push down
  is refused, **D7**). Edges with one restriction share the read [proto 5]; `MergeJoin(A, A)` is one scan [proto 4].
- **Scans in a run:** per source node, one per distinct restriction on its edges into nodes that run, plus
  one full read only if such an edge has no restriction. `f(A_x)`, `g(A_y)`: two scans, both with a predicate,
  none full [proto 1].
- **The node `A` shows the whole table**, and reads it only when something reads `A`
  (`job.nodes["events"].as_table()`: one full scan, then [proto 1]). Being a node triggers no read: a run, the
  diagram and `head()` (a bounded read) never read `A` in full. A table too large to read whole still refuses
  that read (`max_rows`), while its restricted edges run.
- `check_for_new_data()` is still once per source per run.

## 6. The edge view (decided)

`job.edge_stream(edge)` returns a derived, read-only stream: **the upstream node's stream with the edge's
condition and limit applied**. It is stored nowhere and holds nothing.

1. It shows what the restriction selects from the upstream's rows **now**; it is not a record of what
   crossed in a past run.
2. It is over the upstream node's **active rows**, the ones that node itself shows (a job node: its current
   run, empty before a run or while stale); never everything stored. For a source it is a read of the source,
   pushed down where the source can [proto 7: one scan with the predicate and the limit, nothing ran].
3. It never computes, delivers or writes: it runs no pod. `head()` of it is the usual bounded read.
4. This is not the forbidden filter. Applying an edge's restriction to its own upstream's rows is the
   restriction's definition; filtering a *downstream* node's stored rows by it stays forbidden
   (`op(restr(x))` is not `restr(op(x))`).
5. **The limit (D9).** Promised: at most `limit` rows, each satisfying the condition, and for a pushed-down
   read the rows the source's own limited read gives at that moment. Which rows those are is the upstream's
   row order: defined for a source with a defined order and for a sync run's emitted order, not promised
   otherwise. A run and a view made at different moments may show different rows under a limit.

## 7. Nodes below a restricted edge, bind, caches

**Nothing special below the edge.** There are no twins, so every existing rule holds per node: a function node
shows the rows of its run's inputs; `as_source()` serves everything stored for the identity; the instance-id
lookup, staleness, the state-driven `run()`, `load_only` and side-effect delivery (tracked: once per entry;
untracked: once per row of the run) are unchanged. `f` below `A_x` has the content hash, pipeline hash and
stored path of `f` below `A` [proto 1].

**Cached operators.** Never filtered by a restriction. `OFF` runs on the restricted input; `LOG` runs on it
and appends to the node's log; `REPLAY` serves everything logged, whatever restriction logged it [proto 6].
`refuse_restricted_operator_cache(s)` go. **D6**: one warning per run when a `REPLAY` node is below a
restricted edge, since the restriction then chooses nothing.

**Bind.** One slot per source node, as today. The binding carries no restriction. Before anything is bound,
all or nothing:

- A source that states a restriction of its own is **refused** (D5): the error says to bind the source
  without it; the restriction belongs to the edges. The cost: on a built job a restriction cannot be changed
  (a moved window) without building the job again, until `job.restrict` exists (section 9); `main` allows
  that rebind today. A blueprint over declared slots likewise gets its restrictions from `restrict`.
- A source bound to a slot with a restricted edge must be able to push the restriction down and to apply it
  (column known, exact type: the source's own check). One that cannot push down (an in-memory source, a
  `CachedSource`) is **refused** until ITL-1219 (D7): a pushed-down condition follows the table system's
  equality and a local filter Orcapod's, and they differ (NaN, signed zero), so one restriction could select
  different rows depending on what is bound.
- A source that states none leaves every edge as it is.

## 8. `restrictions.py`, visibility, save and load

| Rule | After |
|---|---|
| `require_one_restriction`, `sources_read`, their calls at capture, compile, bind and run | **Go**: "one restriction per table" becomes "one per edge" (section 4). A narrow form stays for D8 |
| `refuse_restricted_operator_cache(s)`, `refuse_restriction_conflicts`, `Rebinding` and their calls | **Go** |
| `refuse_restricted_cache_input`, `refuse_restricted_cache_config` (a `CachedSource` over a restricted source) | **Stay**: a standalone object, kept under the table's identity. A `CachedSource` as a node with a restricted edge is refused (D7) |
| `warn_restricted_aggregation` | **Stays**, reading edges; names each edge and step |
| `restrictions_in_force` | Stays for restrictions inside a stream bound to a slot (**D11**); edges are reported from `job.edges` |
| `restriction_of`, `restriction_text`, `restrictions_text`, `source_name`, `restrictions_beneath` | Stay |

**Visibility.** `diagram()` labels each restricted edge (`where subject_id='m1', limit 100`) in the picture and
the text listing; `explain()` names each input edge of a step with its restriction; `RunSummary.restricted_edges`
is the restricted edges of the run, sorted as `job.edges`, printed `events -> f: where subject_id='m1'`.

**Saved format (D10).** Node keys do not change: they are unique today and stay so. `pipeline.edges` becomes a
list of records, the smallest change that carries a position and a restriction:

```json
"edges": [
  {"from": "<node key>", "to": "<node key>", "position": 0, "where": {"subject_id": "m1"}, "limit": 100},
  {"from": "<node key>", "to": "<node key>", "position": null}
]
```

`where` is `Condition.to_config()`; both are left out when empty. The bound source's config no longer holds
`where` or `limit`. Records are sorted by `(to, position, from)`. Both format versions rise (a reader that
ignored `where` would read the whole table; an earlier file is refused, as the rule is). A repeated triple or
an edge naming a missing node is refused on load. An operator's saved `inputs` list (ITL-1417) is then
redundant with the positions and can go in the same change.

## 9. Restricting an edge of a built pipeline (later; design for it now)

**The call (D4).** `job.restrict(edge, mapping=None, /, *, limit=None, **columns)` and `job.unrestrict(edge)`,
the same on a `Pipeline`. `edge` is an `Edge` or its `(source, target[, position])`; the other arguments are
`where()`'s. It **replaces** the edge's restriction: one explicit statement about one named edge, so there is
nothing to conflict with (sugar never replaces; this call is how a restriction is changed, a moved window
included: the first round's D8).

- **Invalidates:** the edge's target node and every node below it become stale (the edge has a modified time
  that the staleness rule reads, as it reads a slot's bind time). Nodes above, and everything stored, are
  untouched; the next run reuses stored results through the instance lookup.
- **Any edge:** on an edge whose upstream is not a source, the restriction is applied to the upstream node's
  rows where they are handed to the target, by the code the filter operator uses
  (`core/operators/filter_mask.filter_table`), with no node and no identity. The edge view is the same stream.
  That equality is Orcapod's; whether an edge out of a source that cannot push down may use it too is D7's
  question again, with ITL-1219.

**What the first stages must already do so nothing is redone:**

1. Every edge has the record and the two restriction fields, is saved, shown and selected alike; nothing
   assumes a restricted edge leaves a source.
2. One function gives "the stream of an edge" (upstream stream, condition, limit), used by both orchestrators
   and by `edge_stream`; the pushed-down read is its case for a source upstream.
3. One function sets an edge's restriction; sugar (capture) and later `restrict` both call it. The
   conflict rule is the sugar's, not the setter's.
4. The staleness rule asks an edge for its modified time through one place (constant until `restrict`).

**Filter pushed down (later, not designed).** Carrying out `A.filter(x)` as the read `A_x` needs: the run to
see a `Filter` whose input edge leaves a source that can push down; a proof that the pushed-down read gives
exactly the filter's rows (the filter's equality is Orcapod's, the read's is the table system's: NaN and signed
zero differ, so equalities on non-float columns only, or the filter applied again to the read); and an
execution-only restriction on that read which is not an edge restriction and is shown as the filter it is.

## 10. What moves

**The edge design moves no content hash, pipeline hash, stored path, stored column or golden value. Section
4a does**: the content hash, pipeline hash and pipeline-table path of a pod called in the default mode on a
stream with a data column it does not take (the pods that drop columns, and nothing else). Measured with the golden generator in `--check` mode on the
prototype, nothing written: 34 entries, all in 2 cases of the provisional pipeline file (`rejoin_job`,
`rejoin_job_async`: `golden_ratio` drops `height` and `score`), no entry of a frozen file, nothing else among
2480 computed entries. The exact list, old and new values, the reason for each, the tests that change and the
variants not chosen are in `docs/dev/specs/itl-1472/golden-changes-o34.md`.

- [proto 1] `f` below a restricted edge equals `f` of the unrestricted job in all three.
- **[impact]** One test run with the prototype applied (`prototype_plugin.py`, `impact_run.out`): the golden
  suite, `tests/test_pipeline`, the `where` tests, the order-invariance and filter-pipeline tests: 9788 passed,
  162 failed, none of them a golden value. By test class or file, reasons read from the names, not each test:
  - 35 `TestOneRestrictionPerSource`, 11 `TestBindingTwoSlotsOverOneSource`, 7 `TestASlotOverTwoTables`,
    3 `TestRestrictionsCheckedLive`: the one-per-table rule and its checks at bind and run (section 8).
  - 26 `TestOperatorCacheOverARestriction`: the refusal that goes (section 7).
  - 26 `TestRunsAndSavedJobs`, 3 `TestConditionInstances`, 2 `TestRestrictionThroughWrappers`: the run summary
    by slot, the saved `where` and the slot's rows (sections 5 and 8).
  - 32 on `RestrictedAggregationWarning` (three files): the warning stays; the prototype does not carry it
    to edges.
  - 2 in `test_loaded_argument_order.py`: they pin that `MergeJoin(A, A)` and `SemiJoin(A, A)` fail to run.
  - 15 (`test_rows_on_read.py`, observer and transport tests): the prototype's deferred source read, which
    changes when a source node's observer events fire. The design must keep those events (stage 1).
- Saved pipeline and job files: a new format version (section 8).
- Behaviour: a slot's node shows the whole table; pipelines refused today are accepted (several restrictions
  of one table on different edges; a caching operator below a restriction); `MergeJoin(A, A)` runs.

## 11. Crossings, and what probes need (ITL-1473)

An edge is the address of a probe: `job.edges`, the triple, saved and loaded. On the ITL-1464 integration
branch the async transport of an edge is `edge_readers[(upstream, node)]` [read]; it is keyed by the triple
instead, since one node can feed two positions.

**A crossing** is one row handed to the target node as its input at that position, in a run.

| Case | Crosses `(u, v, p)`? |
|---|---|
| `v` runs (sync or async); `u` ran, or is held and feeds its held rows | each row of the edge's stream |
| `v` is held | no: nothing is read |
| `load_only` | yes, for the rows `u` loaded |
| `v` finds its result stored (a hit) | yes: the input row crossed either way |
| `v` is a `REPLAY` operator | no on its input edges (sync reads none, async drains: drained rows are not crossings) |
| a restricted edge | only rows the restriction selects exist to cross |
| `head()`, the preview, `edge_stream`, `as_source()`, any read | never: not a run |

The edge view is not a log of crossings (section 6). Anything a probe must remember per edge (whether a row
crossed before, "first time") is the probe's own state, keyed by the edge triple and a row id; there is no edge
cache to read it from. Still for the probe design: a row's id into an operator (no entry id there), whether
hit or computed should travel with the row, and that the async tap sees record batches.

## 12. Stages and tests

The stages, each one pull request against `main`, are in `docs/dev/plans/itl-1472-plan.md`:

| Stage | Content |
|---|---|
| 1 (ITL-1483) | Graph edges carry their input position; `MergeJoin(A, A)` and `SemiJoin(A, A)` run |
| 2 | Edges as the public and saved structure: `job.edges`, `job.edge`, the `Edge` record, saved records |
| 3a | A function pod reads only its columns (section 4a): the one stage that moves hashes, after the golden list is approved |
| 3b | The restriction on the edge: the sugar, the conflict rule, reads per restriction, bind (D5, D7) |
| 4 | The old rules go; cached operators (D6); `edge_stream`; diagram, `explain()`, run summary; docs |
| later | `restrict` / `unrestrict` on any edge (section 9); filter push-down |

**Tests** (public interface; inputs with two or more tag and two or more data columns; sync and async):

1. Two, three and four restrictions of one table on different edges: rows per node, hashes and paths equal to
   the unrestricted job's, scans counted (one per distinct restriction, none full); one restriction on several
   edges (one scan).
2. Every conflict in every recording order, same error: two restrictions, restricted with unrestricted, a
   limit against none, at the first shared node (an operator; a select the user wrote; one pod called twice), across two `with` blocks; two pods taking
   the same columns accepted;
   the same restriction written twice accepted; nothing recorded after a refusal; the filter remedy.
3. Positions: `MergeJoin(A_x, A_y)` and `(A_y, A_x)`, `MergeJoin(A, A)`, `SemiJoin(A_x, A)` and `(A, A_x)`;
   `Join` and `Rejoin` of branches below two restricted edges; a pod called with two inputs.
4. Each role of the restricted column (tag, data-tag, data); conditions on two columns in both orders; a limit.
5. The node `A` shows the whole table and reads it only when read; `edge_stream` before a run, after one, on a
   stale upstream; it never computes or delivers.
6. `run()` of one node, staleness, `clear_cache()`, `load_only`, a new job on the same store; side effects
   (tracked, untracked, a pod taking `ctx`).
7. Cached operators: `OFF`, `LOG`, `REPLAY` below a restricted edge, for `GroupBy`, `Batch`, `Join`; the warning.
8. Save and load: byte-equal files in every recording order, restrictions kept, an earlier format refused, a
   repeated or dangling edge refused. Bind: a rebind keeps the edges, D5 and D7 refuse all-or-nothing.
9. `test_order_invariance.py`: every permutation of the recording order gives the same edges, file and rows.
10. Golden: unchanged; one provisional case pins that a restricted edge moves no value.

## 13. Decisions

D1 to D11 were **decided on the developer's behalf, 2026-11-26, revertible** (the coordinator, authorized by
the developer). The edge view (section 6, points 1 and 2) was confirmed by the developer the same day.

| # | Question | Decided |
|---|---|---|
| D1 | What names an edge's nodes | The node names (`job.nodes` keys) in the API, the file's node keys in the file. A name, not a hash |
| D2 | One node feeding a commutative operator twice | One edge; two different restrictions refused. No operator reaches it today [ran 5] |
| D3 | Public names | Approved provisionally: `job.edges`, `job.edge(source, target, position=None)`, `Edge(source, target, position, condition, limit)`, `job.edge_stream(edge)`, `EdgeRestrictionConflictError`, `RunSummary.restricted_edges`; `SourceProtocol.unrestricted()` and `.restricted(condition, limit)` |
| D4 | The later call on a built pipeline | `job.restrict(edge, ...)` / `job.unrestrict(edge)` on the pipeline, `Edge` a plain value; replaces; makes the target and below stale. Not implemented now |
| D5 | `bind` of a source that states its own restriction | **Refused.** The binding does not carry the restriction, and when in doubt, refuse. Cost: moving a window needs a rebuilt job until `job.restrict` |
| D6 | A `REPLAY` operator below a restricted edge | Serves everything, one warning per run |
| D7 | A restricted edge whose source cannot push down | **Refused** until ITL-1219: the table system's equality and Orcapod's differ, so one restriction could select different rows depending on the bound source |
| D8 | Two leaf streams of one identity over different restrictions | Refused (the present walk, narrowed) until ITL-1387 (PR 500) |
| D9 | What a limit promises | The count and the condition; which rows only where the upstream's order is defined |
| D10 | Saved format | Edges as records, node keys unchanged, both format versions raised |
| D11 | `RunSummary.conditions`, `limits`, `restricted_source_ids` | Kept, for a restriction inside a stream bound to a slot; edges in `restricted_edges` |

**Decided by the developer, 2026-11-26.**

| # | Question | Decided |
|---|---|---|
| O34 | A function pod's automatic `SelectDataColumns` as the first shared node | There is no selection node: a function pod reads only the columns its function takes. In the default mode, on an input with other data columns, the function node's identity tuple gets one extra label element, `drops_extra_columns`; otherwise the tuple is exactly today's and nothing moves. The ignored names are not in the identity (section 4a). The earlier answers (pass-through; a selector node; the names, or a key and boolean, in the identity) are withdrawn |

**Open, from that decision.**

| # | Question | Options | Recommendation |
|---|---|---|---|
| D12 | The golden entries that move | the list in `docs/dev/specs/itl-1472/golden-changes-o34.md`, section 2: 34 entries in 2 provisional cases, no frozen file | approve the exact list before any golden change |
| D13 | The label's name and constant | `"drops_extra_columns"`, the constant `DROPS_EXTRA_COLUMNS` beside the node's identity code, appended as `(pod, inputs, label)` by one function used by the node, the pod's stream and the invocation key | approve the name (it is in the hash) |
| D14 | How it is shown | the `explain()` line and the `diagram()` detail, names computed; `"drops_extra_columns": true` in the saved descriptor (section 4a) | approve the wording |
