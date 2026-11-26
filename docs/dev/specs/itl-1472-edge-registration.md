# ITL-1472: edge registration

Status: proposal, no library code. The developer's decisions of 2026-11-25/26 (the issue's list) are taken as
given; section 14 lists what is still open, D1 to D13, each with a recommendation. Depends on ITL-1466 stage 1
(the three nested ids).

Evidence marks: **[read]** read on `main` (0d1e017c) or on the branch named; **[ran N]**
`docs/dev/scripts/itl_1472/collapse_on_main.py`, heading N, on `main` as it is; **[proto N]**
`docs/dev/scripts/itl_1472/prototype.py`, heading N: `main` with four replacements made in the script's own
process (use keys in place of content hashes when recording, numbered slot names, job nodes equal by object, the
restriction refusals off). The prototype is not the design: it leaves the restriction on the bound source. Each
script's output is the `.out` file beside it.

## 1. The problem on main

- `f(A.where(m1))` and `f(A.where(m2))` in one job raise `ValueError` at capture [ran 1].
- Why: a restriction is in no identity, so the two sources have one content hash and one pipeline hash, and so do
  the two invocations of `f` [ran 2]. `_record_invocation` keys an invocation, and each leaf, by content hash
  (`pipeline/base.py`) [read].
- With only the refusal switched off, the two uses are one slot and one node, and the rows are those of the use
  registered first: `m1, m2` gives t = 1, 2, 3; `m2, m1` gives t = 4, 5 [ran 3]. The refusal hides an order
  dependence.
- The same collapse happens today, with no refusal, for two separate `DictSource` objects with equal rows: one
  slot, one node [ran 4].
- A compiled node is equal to, and hashes like, any node of the same content [ran 5]. The run's DAG
  (`OrcaDAG`), the orchestrators' `outputs[node]`, `edge_readers[(upstream, node)]` and `held_rows` are keyed by
  node objects [read], so two nodes of one identity would be one key there.

## 2. Terms

- **use** (while building): one leaf stream object given to a pod, with the restriction it reads under. Its
  handle is the Python object (decision 3).
- **slot**: a `SourceJobNode` (`SourceNode` in a blueprint), one per use; named, bound separately.
- **edge** (built): `(upstream node name, downstream node name, input position)`, with the restriction rows
  cross it under. One edge concept for the pipeline, the run and probes (section 11).
- **twins**: two nodes of one identity (content hash, pipeline hash, stored path).

## 3. The edge's built identity

**Recipe.** An edge is the triple `(from, to, position)`:

- `from`, `to`: the two nodes' names, the keys of `job.nodes`. A name is unique in a job (compile's label rule),
  so it is the node's built identity; a content hash no longer is (two nodes share one).
- `position`: the index of the input in the downstream pod's `argument_symmetry` when that is a tuple (a function
  pod: 0; `SemiJoin`, `MergeJoin`: 0 and 1). For a commutative pod (`Join`, `Rejoin`; a frozenset) it is `None`:
  argument order is no part of the pod, so it is no part of the edge.

**A name, not a hash (D3).** It is kept as three fields, never joined into one string and never parsed. It has
no label and no row in the hash site inventory, and it enters no content hash, pipeline hash, stored path,
stored column, source id, record id or token. It survives save and load because names do (section 7).

**Two edges with one identity cannot be built.** Names are unique; one input position takes one stream; a
commutative pod given one node twice has one input (its identity is a set), hence one edge. A saved file that
repeats a triple, or names a node it does not hold, is refused on load. The collision that can happen is of
*names*, and the label rule resolves it (section 4).

**Stability.** A numbered default name (`events_1`) can change when a use is added, as any default label can
today; an explicit `label=` is the stable name. It matters to anything that stores state by edge (section 11).

## 4. Registration

**While building.** `record_*_pod_invocation(pod, inputs, label)` keeps its signature [read `core/tracker.py`].
The pipeline keys what it records by a **build key** in place of the content hash:

- a leaf (a stream no recorded invocation produced): its content hash and its handle, the object
  (`is`, never `==`; the pipeline holds the object, so an `id` is not reused);
- a pod over inputs: the invocation's content hash over its inputs' build keys, in argument order, or sorted for
  a commutative pod. Computed once per stream object: asked once per path it is exponential in stacked diamonds
  (the first prototype stalled `test_graph_walk_cost.py`, ITL-1111).

So `f(m1); f(m1)` is one node (one object, one restriction: the same edge), `f(a.join(b)); g(a.join(b))` shares
one join node as today (object identity counts at the leaves only: **D1**), and `f(m1); f(m2)` is two nodes over
two slots. Two separate objects are two slots even when equal in everything, a restriction included (decision 3;
its consequence for a source built afresh at each use is **D2**). Checked [proto 7]: `f(a), f(b), f(a)` over two
equal `DictSource` objects gives two slots and two nodes.

**Compile.** Slots first, then pods in topological order; each node is created over its upstream *nodes*. Then
names, then every map is re-keyed by name; build keys and handles are dropped. A build key is never saved.

**Names (decision 4).** A node's default label is as today. Nodes that want one label are numbered `_1`, `_2`,
... in this order (today: by content hash only, which twins share):

1. content hash;
2. the restriction: none first, then `Condition.canonical_form()` (order-free [read]), then the limit;
3. for a pod: the names of its upstream nodes, by position (already named: slots are named first);
4. for a slot: the explicit labels and content hashes of the steps it feeds, sorted;
5. last, for uses that nothing above tells apart: the order registered (**D4**).

A slot's name is its label (`SourceJobNode.name`, the `bind` key): `events` for one use, `events_1`, `events_2`
for two. Different sources that share a name stay refused (`SourceSlotNameCollisionError`, ITL-1425): only uses
of one identity are numbered. Step 2 removes what [proto 3] shows without it (names follow registration).

**`job.nodes`.** Name to node, every node, twins included; unchanged in kind. New: `job.edges`, the built edges
as frozen records `Edge(source, target, position, condition, limit)`, sorted by `(target, position, source)`
(**D12**, public surface).

**Node objects.** Compiled nodes (`SourceJobNode`, `FunctionJobNode`, `OperatorJobNode`, `SideEffectJobNode`, and
the blueprint's nodes) compare and hash by object (**D5**); content equality stays `a.content_hash() ==
b.content_hash()`. [proto 1-8] ran sync, async, save and load with this.

## 5. Two nodes, one store

`f_1` and `f_2` have equal content hash, pipeline hash and stored path, equal to the single-use node's [proto 1].

| Topic | Behaviour | Evidence, change needed |
|---|---|---|
| Pipeline table | One table. A row is written once per record id (ITL-1466), whichever node meets it first. No edge column: the store cannot tell which edge a row came through, by design (decision 5) | [proto 1] 4 rows for 4 distinct inputs over two overlapping restrictions, sync. None |
| Instance-id lookup | "Is this instance stored?" is answered from the shared table: an entry one node computed is a hit for the other, in the same run or a later job | [proto 1] 4 calls for 3 + 3 rows; a new job on the store: 0 calls. None |
| Held rows | Per node object: each node shows the rows of its own run's inputs (ITL-1093) | [proto 1] `double_1` t = 1, 2, 3; `double_2` t = 1, 3, 5. Needs D5 |
| Staleness, `run()` | Per node object. `f_1.run()` runs `f_1` and what it depends on; `f_2` stays empty and stale; a later `job.run()` holds `f_1` | [proto 2]. Needs D5 |
| `load_only` | Each node loads the stored entries of its own run's inputs | [proto 2] |
| `as_source()` | Of either node: everything stored for the identity (both restrictions, earlier runs). The two derived sources have one identity; as leaves of another pipeline they are two objects, so two slots | [proto 1] serves t = 1, 2, 3, 5 |
| Side effect, tracked | Once per entry across both edges: the invocation log is keyed by the entry (ITL-1466) and shared. Each node passes on all its rows | [proto 4] 4 deliveries for 3 + 3 rows |
| Side effect, untracked | Once per crossing: each node delivers each of its rows | [proto 5] 6 deliveries |
| Async run | **Twins race.** Both nodes look up, compute and write the entries they share at once | [proto 1] async: 5 calls and 6 rows where sync has 4 and 4. **D10** |
| `expected_rows` in a saved job | A count per node label of the rows under the node's run key [read `job.save`]; twins share the key, so both record the store's count. Keep, documented as per identity | [read] |

**The async race (D10).** Nothing in a run makes two nodes of one identity take turns. For a function that is a
second call and a second result and record for one instance; for a tracked side effect, possibly a second
delivery ([proto 4] async gave one each, by timing only). Recommended: one in-flight table per stored path in a
run (instance id, or entry id for a side effect, to a future); the second node awaits the first's result and
reads it as a hit. The sync path needs nothing: twins run one after the other, in name order.

## 6. Cached operators over a restricted edge (decision 6)

An operator cache is one table per identity; twins share it. Never filtered by a restriction.

| Mode | Does | Why |
|---|---|---|
| `OFF` | **Runs on the restricted input.** Stores nothing | Nothing stored to serve |
| `LOG` | **Runs on the restricted input**, shows its run's rows, appends them to the shared log | `LOG` always recomputes (its definition); the log is a record of every run under the identity, so runs under other restrictions belong in it, as earlier runs do |
| `REPLAY` | **Serves everything in its store**: every logged row of the identity, whatever restriction logged it. Reads no input | `REPLAY` never computes (its definition), and replaying "this restriction's rows" would be a filter of stored rows by the restriction, which is ruled out |

[proto 6] `GroupBy` over `where subject_id='m1'` and over `where parity=1`: `OFF` and `LOG` give each node its own
groups (`m1: [1.5, 2.5, 3.5]`; `m1: [1.5, 3.5]`, `m2: [5.5]`); `REPLAY` gives both nodes all three rows.

So `refuse_restricted_operator_cache(s)` goes. Proposed with it (**D6**): one warning per run when a `REPLAY`
node sits below a restricted edge, naming both, since the restriction then chooses nothing; none for `LOG`.
`RestrictedAggregationWarning` stays. A user who wants a restriction to be part of what is cached uses the
filter operator.

## 7. Save, load, slots and bind

**Format (D11).** Today `nodes` is keyed by content hash and `edges` is a list of hash pairs [read
`Pipeline.save`]; restrictions are in `bindings.sources[slot].where` [read, proto 8]. Proposed, with
`PIPELINE_FORMAT_VERSION` and `PIPELINE_JOB_FORMAT_VERSION` raised (an earlier file is refused, as the rule is):

```json
"nodes": {"events_1": {...}, "events_2": {...}, "double_1": {...}, "double_2": {...}},
"edges": [
  {"from": "events_1", "to": "double_1", "position": 0, "where": {"subject_id": "m1"}},
  {"from": "events_2", "to": "double_2", "position": 0, "where": {"parity": 1}, "limit": 100}
]
```

- A node descriptor keeps `content_hash` and `pipeline_hash` as fields. `where` is `Condition.to_config()`.
- The edge list replaces an operator's saved `inputs` list (ITL-1417): argument order is the position.
- Edges are written sorted by `(to, position, from)`; nodes in today's order (slots first, then by pipeline
  hash, then name). The file is the same bytes for every registration order (section 10).
- A loaded job rebuilds both slots and both nodes from names and edges. `main`'s loader already takes any
  string as a node key: it restored six nodes and each twin read its own rows `load_only` [proto 8]. The version
  must rise all the same: an older Orcapod that ignored `where` on an edge would read the whole table.

**Where the restriction is held (decision 8).** In the pipeline, on the edges out of a slot; never in the
binding. All edges out of one slot carry one restriction (two restrictions are two slots), so it is applied once,
where the slot reads its bound source (`_apply_restriction`, pushed down as today). At compile a leaf
`A.where(c)` becomes: slot bound to `A` without its restriction (a new required `SourceProtocol` member,
`without_restriction()`; `RootSource` returns itself), edge restriction `(c, limit)`. `Edge` has the two fields
on every edge; only edges out of a slot can hold one until restrictions reach general streams (ITL-1219).

**`bind`.** Each slot is bound separately by its name; rebinding keeps the edge's restriction. Before anything
is bound, for every slot of the bind:

- `SourceNodeBase.validate` as today (names, roles, Python types, Arrow types by `hash_schema`, one system-tag
  pair). Its rule does not change.
- New, same all-or-nothing check: the source can apply the slot's restriction (the source's own check: column
  known, exact type, no conversion). A kind that cannot restrict (in-memory sources, `CachedSource`) is refused
  for a restricted slot until ITL-1219 (**D13**).
- A source that brings a restriction of its own is refused, pointing to the edge (**D7**). How a built job's
  restriction is changed (a moved window) is **D8**.

**What a slot shows.** `job.nodes["events_1"]` shows the rows that cross its edges (the restricted read), not
the whole table (**D9**); `bound_source` is the binding.

## 8. `restrictions.py`: what goes, what stays (decision 10)

| Rule | After |
|---|---|
| `require_one_restriction`, with `sources_read` and its calls at capture, compile, bind and run | **Goes.** Several restrictions of one table are several slots |
| `refuse_restricted_operator_cache`, `refuse_restricted_operator_caches`, and their calls (`attach_databases`, the `upstreams` setter, `execute`, `async_execute`, bind, run) | **Go.** Section 6 defines the behaviour |
| `refuse_restriction_conflicts`, `Rebinding` | **Go** (nothing left to refuse) |
| `refuse_restricted_cache_input`, `refuse_restricted_cache_config` (`CachedSource` over a restricted source) | **Stay.** A source cache is kept under the table's identity; standalone use is unchanged. In a job it is met as D13 |
| `warn_restricted_aggregation` | **Stays**, reading edges; names each edge and step |
| `restrictions_in_force` | **Stays**, reading edges: one entry per slot is now exact, which closes the known limit ITL-1293 if D7 is "refuse" |
| `restriction_of`, `restriction_text`, `restrictions_text`, `source_name` | **Stay** |
| `restrictions_beneath`, `_streams_beneath` | Stay only for the `CachedSource` refusal and standalone streams |

## 9. Visibility (decision 9)

- `diagram()`: each restricted edge is labelled with `restriction_text` (`where subject_id='m1', limit 100`), in
  the picture and in the text listing. Twins are two nodes with their numbered names.
- `explain()`: a node names each input edge with its condition (today: per source, what "the source applies"
  [read `test_explain.py`]). A standalone stream keeps today's form.
- `RunSummary`: `conditions`, `limits` and `restricted_source_ids` stay, by slot. New `restricted_edges`: one
  record per restricted edge (`from`, `to`, `position`, condition, limit), sorted as `job.edges`; printed as
  `events_1 -> double_1: where subject_id='m1'`.

## 10. Order rules (ITL-1271)

Nothing follows the order in which invocations were recorded, with one proposed exception (D4):

- Names follow section 4's rule; the order of `job.nodes` and `job.edges`, the saved file's bytes, the run
  summary and every warning's list follow names. No edge or name enters a hash, a path or a stored value.
- Twins run in name order on the sync path; which one computes a shared entry shows in no row, hash or stored
  value. With D10 the same holds on the async path.
- **The exception:** uses that identity, restriction, upstream names and fed steps do not tell apart (`f(a)`,
  `f(b)` over two equal objects, no labels) are numbered in the order registered. With equal rows nothing
  observable follows it. With two in-memory sources given one `source_id` and different rows (one address),
  which rows `f_1` shows does. Today that case is silently one slot holding the first source [ran 4]. An entry
  in `ORDER_EXCEPTIONS` needs the developer's decision.

`test_order_invariance.py` gets a case: every permutation of the recording order of a job with three uses of
one table gives the same names, edges, saved bytes and rows per name.

## 11. Crossings, and what probes need (decision 11; ITL-1473)

**One edge concept.** `core/job_edges.py` on the ITL-1464 integration branch is the *transport* of an edge in an
async run: `EdgeWriter` and `EdgeReader` are its two ends, and the orchestrator holds them as
`edge_readers[(upstream node, downstream node)]` [read, branch `eywalker/itl-1464-integration`]. That pair is
this design's edge without its position. Proposed: `edge_readers` is keyed by the built edge
`(from, to, position)`, and an `EdgeReader` knows its `Edge`. No second class: the `Edge` record names it, the
reader carries its rows in an async run.

**A crossing** is one row taken by the downstream node as its input at that position, in a run.

| Case | Crosses `(u, v, p)`? | Where, in code |
|---|---|---|
| Sync, `v` runs | each row `v` reads from `u`'s output | `v.execute(...)` reading the stream of `outputs[u]` (`sync_orchestrator`) |
| Async, `v` runs | each row, or each row of a record batch | `EdgeReader.receive` / `EdgeFeed.next` |
| `u` held (current, not run), `v` runs | yes: `u`'s held rows are fed to `v` | the same two places (`held_streams`, `held_rows`) |
| `v` held | no: nothing is read | |
| `load_only` | yes, for the rows `u` loaded | the same two places; the run's status is `loaded_only` |
| `v` computes or finds its result stored (a hit) | the input row crosses `(u, v, p)` either way; the output row crosses every edge out of `v` either way | the row carries no "hit" mark today |
| `v` is a `REPLAY` operator | **no** crossing on its input edges: the sync path reads no input, the async path reads and discards [read]; drained rows are not crossings, so the two paths agree. Replayed rows cross the edges out of `v` | `async_execute` drain |
| An edge out of a slot | only rows the restriction lets through exist to cross: it is applied in the read | the slot's read |
| `head()`, the preview, `as_source()`, a read of a node's rows | never a crossing: not a run | |

**What the probe design gets from this one:** an address (`job.edges`, the triple, saved and loaded); one
definition of a crossing for both paths; two tap points (the stream handed to `execute`, the `EdgeReader`); a
probe on `(events_1, double_1, 0)` never sees rows of `events_2`.

**What it will have to decide itself:**

1. A row's id at a crossing: into a function or side-effect node the entry and instance ids exist (ITL-1466);
   into an operator there is none, only tags, system tags and the row's content hash.
2. "First time data ever flows through" cannot be read from the store: twins share it and it has no edge
   column. It needs its own record keyed by edge and row id; the edge key is then stored, and a numbered default
   name can change (section 3), so it must require an explicit label or define a key.
3. Hit or computed: a crossing does not say; `v`'s per-row status would have to travel with the row.
4. A tap on the async path sees record batches, not pairs; making pairs for a probe has a cost.
5. Whether a row drained at a `REPLAY` node, or not read by a held node, should be observable at all.

## 12. What moves

**No content hash, pipeline hash, stored path, stored column or golden value moves.**

- [proto 1] the twins' content hash, pipeline hash and `node_uri` equal the single-use node's.
- [proto] the golden suite run with the prototype applied (`prototype_plugin.py`): 2577 passed, 0 failed.
- Nothing is added to a stored table; no layout marker changes.

What does change, none of it a hash:

- Saved pipeline and job files: a new format version; node keys are names; edges are records (section 7).
- Slot and node names, only in pipelines refused or collapsed before: `events_1`, `events_2`, `f_1`, `f_2`. A
  blueprint's `SourceNode` content hash holds the slot name, as today; it is written to saved files only.
- Two separate equal source objects: one slot today, two after (decision 3; D2).
- `test_one_restriction_twice_in_one_job_is_accepted` and the tests of the refusals change with section 8.

## 13. Stages and tests

Each stage is one pull request against `main`; targeted tests locally, the suite in CI.

| Stage | Content | Depends on |
|---|---|---|
| 1 | Nodes compare by object (D5); every container keyed by node reviewed. No behaviour change | - |
| 2 | Build keys, slots per use, the naming rule, re-keying by name, `job.edges`, the edge triple as the orchestrators' key; the saved format. Restrictions still refused | 1; ITL-1464 integration merged (same orchestrator lines) |
| 3 | The restriction on the edge: `without_restriction()`, the slot's read, `bind` (D7, D13), save and load of `where` and `limit` on edges | 2 |
| 4 | Lift the rules (section 8); the cache modes (section 6); the async in-flight table (D10); `expected_rows` | 3; ITL-1466 stage 1 |
| 5 | `diagram()`, `explain()`, `RunSummary.restricted_edges`; docs (`concepts/identity.md`, the restrictions page, `CLAUDE.md` and `.zed/rules`); the order-invariance case | 4 |

**Tests** (public interface; inputs with two or more tag and two or more data columns; sync and async):

1. Two, three and four restrictions of one table under one pod: names, rows per node, one stored path, one call
   per shared entry when restrictions overlap, `as_source()` of each; restricted and unrestricted use together.
2. One restriction through one object (one node) and through two objects (two nodes); two equal in-memory sources.
3. Several inputs: `Join` and `Rejoin` of branches over two restrictions; `SemiJoin(x, x)` and `MergeJoin` in
   both argument orders (positions 0 and 1); a pod called with two inputs (automatic join and select).
4. Several orders: every permutation of the recording order (section 10); explicit labels; hash seeds.
5. Each role of the restricted column: tag, data-tag, data; conditions on two columns in both orders; a limit.
6. `run()` of one twin, staleness of the other, `clear_cache()`, `load_only`, a new job on the same store.
7. Side effects: tracked (once per entry over both edges), untracked (once per crossing), a pod taking `ctx`.
8. Operators: `OFF`, `LOG`, `REPLAY` over two restrictions, for `GroupBy`, `Batch`, `Join`; the warning.
9. Save and load: byte-equal files, both nodes rebuilt, restrictions kept, an older format refused, a repeated
   or dangling edge refused. `bind`: each slot alone, a rebind keeps the restriction, D7 and D13 refuse whole.
10. Async: the twin race (a slow function, overlapping restrictions): one call per entry.
11. Build keys are computed once per stream object: the stacked-diamond budget of
    `tests/test_pipeline/test_graph_walk_cost.py` (ITL-1111) holds with twins.
12. Golden: unchanged; a provisional case with two restrictions pins that twins share every value.

## 14. Open decisions

| # | Question | Options | Recommendation |
|---|---|---|---|
| D1 | Where does object identity count while building? | (a) at leaves only; a stream above the leaves is keyed by its pod and its inputs. (b) at every stream | **(a)**: `f(a.join(b)); g(a.join(b))` keeps one join node, as today |
| D2 | A source built afresh at each use (`A.where(day=1)` written twice; a helper that returns a new source object per call): one slot today | (a) decision 3 as written: two slots, two reads, numbered names. (b) merge separate objects of one identity and one restriction when the source has a location (not in-memory) | **(a)**: one object, one slot is the rule a user can predict; the docs say to keep a source in a variable. It changes one accepted test and any code that builds a source per use: confirm |
| D3 | Edge identity: name or hash? | (a) the name triple. (b) a labelled hash of the triple | **(a)**: it is a name already; a hash adds a label and an inventory row and identifies nothing more |
| D4 | Numbering uses that nothing else tells apart | (a) registration order, as an `ORDER_EXCEPTIONS` entry. (b) refuse them unless the pods are labelled | **(a)**; (b) refuses `f(a); f(b)` over equal sources, which decision 3 allows |
| D5 | Two nodes of one identity as dictionary keys | (a) compiled nodes compare by object. (b) keep content equality and key every container by name | **(a)**: smaller, and a node is a place in a graph, not a value. Stage 1 alone shows what relied on content equality |
| D6 | A `REPLAY` operator below a restricted edge | (a) serve everything, with one warning per run. (b) serve everything, silently. (c) refuse `REPLAY` there | **(a)** |
| D7 | `bind` of a source that carries its own restriction | (a) refuse. (b) combine with the edge's (`Condition.combined`). (c) replace the edge's | **(a)**: the binding holds no restriction (decision 8) |
| D8 | Changing a built job's restriction (a moved window) | (a) not offered: build the job again; results are reused, the store is shared. (b) `job.restrict(slot, ...)`, which makes the nodes below stale | **(a)** now, **(b)** as a follow-up issue when a long-lived job needs it |
| D9 | What a restricted slot's node shows | (a) the rows that cross its edges. (b) the whole table | **(a)**: reading the whole table is what a restriction avoids |
| D10 | Async twins on one store | (a) one in-flight table per stored path in a run. (b) accept a second call and record. (c) run twins one after the other | **(a)** |
| D11 | Saved format | (a) nodes by name, edges as records, version raised. (b) keep hash keys, add a suffix for twins and a separate `edge_restrictions` section | **(a)**: one identifier for nodes; the version must rise in both |
| D12 | Public surface | `job.edges` and the `Edge` record (`source`, `target`, `position`, `condition`, `limit`); `RunSummary.restricted_edges`; `SourceProtocol.without_restriction()` | approve the names |
| D13 | A restricted slot bound to a source that cannot restrict | (a) refuse at bind until ITL-1219. (b) filter the source's rows in Orcapod | **(a)**: a pushed-down condition is decided by the table system's equality; a local filter could differ |
