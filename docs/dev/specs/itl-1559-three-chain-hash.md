# Three-chain hash design for slot identity (ITL-1559)

Status: draft for the developer's review. No production code is changed by this document.
Measured on `main` at `c1040ffa` (2026-11-28). Related: ITL-1442, ITL-1427, ITL-1470,
ITL-1506, ITL-1472, ITL-1553, ITL-1554, ITL-1560, ITL-1590.

## 1. Summary

The developer decided (2026-11-28) that every node of a pipeline has three parallel chains,
computed separately and combined only where a hash is needed.

| Chain | Leaf (a source slot) | Any other node, operation `op`, upstreams `u1..uk` | Combined |
|---|---|---|---|
| `P` structure | `H(schema)` | `H(op, P(u1)..P(uk))`: today's pipeline hash, unchanged in value | stored table paths, system-tag column names, as today |
| `C` sources | the bound source's identity | the upstreams' `C`, pooled in `P`'s input order | content identity `H(P, C)` |
| `I` names | the slot's name | the upstreams' `I`, pooled in the same order | semantic identity `H(P, I)` |

- **Content identity** is what storage sees. It replaces today's content hash recursion.
  Every node content hash value changes; slot names never reach storage.
- **Semantic identity** is the node as an object of the pipeline: graph keys, `job.nodes`,
  edges, restrictions, the run plan. It holds no source, so it survives a rebind and an
  unbound slot has it.

What this document found, in short:

1. The design holds. A read-only prototype over all 2,504 golden entries moves exactly 104
   of them: 102 content hashes of derived streams and nodes, and 2 default source ids of
   derived sources. No pipeline hash, stored path, system-tag column name, record id, data
   hash, instance id or result-table key moves (section 7).
2. Six points contradict the design as it is worded, or need a decision before it is
   built (section 3). Two are counterexamples measured on `main`:
   - Two inputs with equal `P` DO reach a commutative operator today: `Join` and `Rejoin`
     accept two same-schema inputs when every data column is a data-tag. There the
     pipeline hash itself depends on whether the two inputs are equal by content
     (`Join(A, B)` and `Join(A, A)` have different pipeline hashes). So "`P` unchanged in
     value" and "`P` holds no source" cannot both hold for that case (3.5).
   - Today's content hash distinguishes a context-taking function pod from a plain pod
     of the same uri, and `P` does not, so `H(P, C)` and `H(P, I)` would merge two nodes
     that are two today (3.1).
3. Entry ids and instance ids do not contain the node content hash. They do not change.
   Only record ids and the operator cache's row key do, and no golden entry pins either.
4. Most of the work is not hashing. It is moving about 40 places that use the content hash
   as node identity onto semantic identity (section 5).

## 2. Definitions

### 2.1 Notation and labels

`P(x)` is `x.pipeline_hash()` exactly as today. No definition in this document changes it,
except the proposal in 3.1, which the developer must approve.

Each new hash is defined in one module, proposed `src/orcapod/core/identity_chains.py`, and
carries its own label, defined there as a constant and written nowhere else (the hash label
rule):

| Hash | Proposed label | Preimage |
|---|---|---|
| name leaf | `slot_name_v1` | `("slot", <slot name>)` |
| pooled sources at a multi-input node | `source_chain_v1` | the ordered upstream `C` values |
| pooled names at a multi-input node | `name_chain_v1` | the ordered upstream `I` values |
| content identity | `content_identity_v1` | `(P, C)` |
| semantic identity | `node_identity_v1` | `(P, I)` |

The procedure for all five is the semantic hasher's canonical form of the tuple (nested
hashes embedded with their labels), digested with SHA-256. Only the label differs.

An alternative is to give all five the existing `semantic_v1` label, because the procedure
is the semantic hasher's. I recommend the new labels for one reason: a stored
`__node_content_hash` then says which rule produced it. A row written under today's
recursion carries `semantic_v1:`, a row written under this design carries
`content_identity_v1:`, and the two can never be confused or matched by accident.

### 2.2 Leaves

A leaf is anything with no producer. Its `P` is unchanged. Its `C` is its content hash as
it is computed today, unchanged (see 3.3 for why this is a recommendation and not a given).

| Leaf | `P` | `C` | Notes |
|---|---|---|---|
| Root source (`ArrowTableSource`, `DictSource`, `ListSource`, `DataFrameSource`, `CSVSource`, `DeltaTableSource`, `SQLiteTableSource`, `PostgreSQLTableSource`, `DBTableSource`) | Arrow schema identity (`sources/base.py:481`) | `content_hash()`: `(source_id, schema identity, key columns)` (`sources/base.py:419`) | unchanged |
| `SpiralDBTableSource` | as above | root rule, then `tags_as_data` items when given (`spiraldb_table_source.py:693`) | a restriction is in no identity |
| `PollingSource` | schema | `(source_id, impl.identity(), sorted tag columns, key columns)` (`polling_source.py:430`) | provisional (ITL-1316) |
| `CachedSource` | the wrapped source's | the wrapped source's (`cached_source.py:258`) | transparent; its cache path keeps the source's content hash (`cached_source.py:292`) |
| `DerivedSource`, plain | schema | `H((content identity of the origin node,))` (`derived_source.py:325`) | the value moves, because the origin's content identity moves. It reads stored rows, so it takes the origin's CONTENT identity, never its semantic identity |
| `DerivedSource` with `tags_as_data` | schema | root rule, then the sorted mapping (`derived_source.py:326`) | unchanged; requires a `source_id` |
| `SourceProxy` | the saved pipeline hash (`source_proxy.py:215`) | the saved content hash (`source_proxy.py:197`) | unchanged; a delegate is accepted only with both equal (`source_proxy.py:125`) |
| Raw `ArrowTableStream` | schema identity (`arrow_table_stream.py:276`) | its table in canonical row order (`arrow_table_stream.py:223`) | unchanged; PR 613 refuses it as a job leaf |
| Slot, bound (`SourceJobNode` with a source) | the slot's declared schema | the bound source's `C` | today the slot already delegates (`source_node.py:1830`) |
| Slot, unbound (`SourceNode`, or `SourceJobNode` with none) | the slot's declared schema | none (3.4) | today its content hash is `("source_node", name, schema)` (`source_node.py:824`); that hash is retired |

`I` of a leaf is `H(("slot", name))` where `name` is the slot's name. A source has an `I`
only through a slot. How a plain stream outside a job gets one is open question Q2.

### 2.3 Other nodes

The three chains are computed together, in one walk of the graph (developer,
2026-11-28). At each node the order of its upstreams is fixed once, by `P`, and `C` and
`I` are pooled in that same order, so that names and sources stay aligned position by
position. The only exception is a group of inputs with equal `P` at a commutative
operator (2.4).

For a node with producer `op` and upstreams `u1..uk`:

- `P` = today's `pipeline_hash()`.
- `k = 1`: `C(node) = C(u1)` and `I(node) = I(u1)`, passed through unchanged.
- `k > 1`: `C(node) = H(source_chain_v1; C(u_π(1)), ..., C(u_π(k)))` and
  `I(node) = H(name_chain_v1; I(u_π(1)), ..., I(u_π(k)))`, where `π` is the one input
  order of 2.4. Repeats are kept: `[A, A]` differs from `[A]`.
- content identity = `H(content_identity_v1; P(node), C(node))`.
- semantic identity = `H(node_identity_v1; P(node), I(node))`.

The pooled value is a hash of the upstream values, not a flat list of leaves. Section 3.2
says why; the two are equivalent in what they tell apart.

A node whose ancestry holds an unbound slot has no `C` and no content identity (3.4). It
always has `P`, `I` and a semantic identity.

### 2.4 Input order, by operator

`argument_symmetry` decides the order (measured, every definition on `main`):

| Producer | `argument_symmetry` | Order of `C` and `I` |
|---|---|---|
| Every `UnaryOperator` (`Batch`, `GroupBy`, `TagBy`, `AddTags`, the Select/Drop column operators, `MapTags`, `MapData`, `MapColumns`, `PolarsFilter`, `Filter`, `DropNulls`, `Pick`, `Index`) | one-tuple (`operators/base.py:233`) | pass-through |
| `SemiJoin`, `MergeJoin`, any non-commutative `BinaryOperator` | tuple (`operators/base.py:343`) | argument order |
| `Join`, `Rejoin`, any commutative `BinaryOperator` | frozenset (`join.py:217`, `operators/base.py:343`) | canonical order, below |
| `FunctionPod`, `FallThroughPod`, a function node | the multi-stream handler's, a `Join` (`function_pod.py:861`, `:1957`); the pod stream itself has one input | pass-through. The join in front of a pod is its own node |
| `SideEffectPod`, a side-effect node | tuple of one (`side_effects.py:665`) | pass-through |

No operator on `main` returns a nested or partly symmetric group; the only example is in
a docstring (`static_output_pod.py:259`, `:267`). The rule below extends to one: each
nested group is ordered by its own kind.

Canonical order at a commutative operator:

1. Inputs are ordered by `P` (hex text), as `Join` already orders them (`join.py:215`).
   `C` and `I` both use this order.
2. Only inside a group of inputs with equal `P`: the `C` chain orders the group by `C`
   and the `I` chain orders it by `I`. Each uses its own order and never the other's.

**Can a tie of equal `P` occur on `main`? Yes.** The belief was that it cannot, because
`Join` refuses inputs that share a data column, which two same-schema inputs always do.
Measured, it can:

- Commutative operators: `Join` and `Rejoin` only. `MergeJoin` and `SemiJoin` are ordered
  (`is_commutative` is `False`, `merge_join.py:169`, `semijoin.py:322`). A function pod
  over several inputs goes through a `Join`.
- Two same-schema inputs with a plain data column are refused by both
  (`InputValidationError`, "Datas should not have overlapping keys"), also when the
  inputs additionally have data-tags.
- Two same-schema inputs whose every data column is a data-tag (a source with
  `tag_columns` naming every column and `tag_only_columns=False`) are **accepted** by
  both, as plain streams and in a job: `Join(A, B)`, `Join(A, A)`, `Rejoin(A, B)`,
  `Rejoin(A, A)`. A shared data-tag is a join key, not a collision.

So the per-chain tie rule is needed for an exported operator now, not only for a future
one. The test that pins the state of `main` is in 9.1.

How `main` places two inputs with equal pipeline hash in system-tag column names:

- `Join` sorts its inputs by pipeline hash (`join.py:215`) with a stable sort, so tied
  inputs take canonical positions `:0` and `:1` in argument order
  (`_tag_source_id::<schema>::<P>:0` and `...:1`, `join.py:432`). The names are the same
  for either argument order, since both inputs give the same text. The VALUES are then
  sorted per row across the tied columns, so `Join(A, B)` and `Join(B, A)` give identical
  tables (`dict_v1:A` in `:0`, `dict_v1:B` in `:1` both ways; measured) and identical
  content and pipeline hashes.
- `Rejoin` keeps the system-tag columns unchanged, so there is one pair and no position.
- `MergeJoin` uses the argument index (`merge_join.py:348`): `MergeJoin(A, B)` has `A` in
  `:0`, `MergeJoin(B, A)` has `B` in `:0`, and nothing is sorted.

The per-chain tie rule agrees with what `Join` already does to values: within a tie it
sorts the sources by value, without regard to which argument they came from.

### 2.5 Job nodes against plain streams

- A plain stream and the job node made from it have equal `P` (today's rule, and the
  subject of ITL-1470) and equal `C`, so equal content identity. The node additionally
  has `I`.
- `PodInvocation` (`pipeline/pod_invocation.py:73`) mirrors the stream's identity
  structure and has no `pipeline_hash()` of its own today. It needs `P`, `C` and `I` like
  a node, since the graph is keyed by invocations.
- A loaded node with no pod keeps returning its saved `P` (`function_node.py:574`,
  `operator_node.py:320`). Its `C` comes from its upstream nodes, so its content identity
  can be recomputed without the pod (section 11.3).

### 2.6 What each identity is used for

| Use | Identity |
|---|---|
| Table paths, system-tag column names, `invocation_hash`, layout scoping | `P` |
| `__node_content_hash`, record id, operator cache row key, `as_source()` default id, stored views, run URI | content identity |
| Graph keys, `job.nodes`, edges, restrictions, run plan, held rows, staleness, saved-file node keys, diagram, observers' node key | semantic identity |
| A source as a dataset: registration, one-object rule, cache path | the leaf's `C` |

## 3. Points that contradict the wording, or need a decision

### 3.1 `P` does not hold everything today's content hash holds about the operation

A function pod's content identity is `(data_function, ctx_arg_name)` when it takes the
invocation context, and its pipeline identity is `data_function` alone
(`function_pod.py:178`, `:183`). Two pods can share one uri and differ in that
(`tests/test_hashing/test_wrong_hash_findings.py:759` pins that a
`side_effect_function_pod` and `function_pod(ctx_arg=...)` give one uri).

Counterexample, measured: a plain pod and a context-taking pod with one function name,
one output schema and one major version, over one source.

| | plain | context-taking |
|---|---|---|
| pod pipeline hash | equal | equal |
| pod content hash | differs | differs |
| stream `P` | equal | equal |
| stream content hash today | differs | differs |
| pipeline table path | `shared_name/.../schema:c481…` | `side_effect_function/shared_name/.../schema:c481…` |

Today a job holds two nodes. Under the design both `H(P, C)` and `H(P, I)` are equal, so
the graph would keep one node and drop the other, and every node below them merges too.
Storage does not collide, because the node kind puts the two in different tables.

This is the only such case. Every operator and `SideEffectPod` uses one structure for both
hashes (`static_output_pod.py:223`, `side_effects.py:657`), and a data function's two
structures are both its uri (`data_function.py:241`).

Options:

- (a) Put `ctx_arg_name` into the pod's pipeline identity when it is set. `P` then moves
  for every node at or below a context-taking pod, and with it their `schema:<P>` path
  component. No golden entry holds such a pod (`cases.py` has no `ctx_arg`), so no golden
  value moves. `P` stays unchanged everywhere else. **Recommended**: `P` should name the
  operation completely, and the chains then need no fourth term.
- (b) Refuse two pods of one uri that differ in `ctx_arg_name` in one job.
- (c) Carry the difference in both `C` and `I`. This makes neither chain what its name
  says.

### 3.2 A flat pooled list grows exponentially

The table says "pooled `C` of the upstreams". As a flat list of leaves with repeats, a
node's `C` has one entry per path to a root. `k` diamonds in sequence (split, two
branches, rejoin, repeated) give `2^k` entries: 20 diamonds give 1,048,576 entries at
every node below. The diamond over one slot is the case this design is for.

Proposal: pool by hashing (2.3). Given `P`, which fixes the shape of the graph and so the
number of leaves under each input, the hash of the ordered upstream values tells apart
exactly what the flat list does, and each node holds one fixed-size value. I treat this
as part of the design unless the developer objects.

### 3.3 The `C` leaf: reuse the source's content hash

The table gives the leaf as "source id, key columns". A source's content hash today also
holds its Arrow schema identity. I recommend the leaf be that existing hash, unchanged:

- no root source content hash moves (165 golden entries stay; section 7);
- `CachedSource` paths, `SourceProxy` checks and the one-object rule of PR 577 keep
  working on the value they use now;
- it stays what ITL-1506 calls the reading: source id, schema, key columns.

The schema is then in both `P` (the slot's declared schema) and `C` (the source's own).
They can differ: a source with a non-null column bound to a slot that declares it
nullable. That is a different reading and should give a different `C`, which it does.

### 3.4 An unbound slot has no content identity

Today an unbound slot's content hash is `("source_node", name, schema)`, so the slot name
is in a content hash, and PR 613 keeps that ("the slot name contributes to the content
hash of an unbound slot"). Under the design the name belongs to `I` only.

Proposal: `C` of an unbound slot does not exist, and neither does the content identity of
any node below it. Asking for it raises a dedicated error naming the unbound slots. A run
never needs it, because a run leaves out everything below an unbound slot
(`job.py:1313`). A saved file writes `null` for it. I recommend against a placeholder
value: a placeholder could reach storage, and a name in a content identity is what the
design removes.

### 3.5 `P` depends on content equality at a commutative operator (measured on `main`)

Filed as ITL-1590.

A commutative operator returns `frozenset(streams)` (`join.py:217`,
`operators/base.py:346`). A Python frozenset drops equal members, and streams are equal
by content. So the pipeline hash of a commutative operator over two inputs with equal
`P` holds two items when their content differs and one when it is equal.

Measured, with sources `A` and `B` of one schema whose data columns are all data-tags
(2.4), and `A'` a second object equal to `A`:

| | pipeline hash | content hash | system-tag columns |
|---|---|---|---|
| `Join(A, B)`, `Join(B, A)` | `6b73c2cd…` | `a6438908…` | `…:0`, `…:1` |
| `Join(A, A)`, `Join(A, A')` | `407cfb1d…` | `b4179856…` | `…:0`, `…:1` |
| `Rejoin(A, B)`, `Rejoin(B, A)` | `89d2805a…` | `d09a5c5a…` | one pair |
| `Rejoin(A, A)`, `Rejoin(A, A')` | `384d909d…` | `dd773c9f…` | one pair |

Two pipelines with one structure and one schema get two pipeline hashes, so two table
paths, depending only on whether the two slots hold the same source. That breaks the
rule that the pipeline hash ignores data content, on `main`, today. It is also exactly
the case of this design: two slots of one schema bound to one source, then joined.

Under the design it cannot stand: `P` must hold no source, and when nodes stop being
equal by content the set would drop members by some other equality. `P`, `C` and `I` must
each be built from the inputs as a list (a multiset), never from a set of stream objects.
PR 587's positioned edges (`pipeline/edges.input_positions`) give that list.

Consequence the developer must accept: "`P` unchanged in value" cannot hold for this one
case. With a multiset, `Join(A, A)` takes the value `Join(A, B)` has today (two items),
so the pipeline hash, and the table path, of a commutative operator over inputs that are
equal by content AND have equal `P` moves, with everything below it. Every other
pipeline hash is unchanged, because inputs with distinct `P` are never dropped. No golden
entry holds such a case (all 179 golden pipeline hashes stay). It is a hash change and
needs the developer's approval as one.

### 3.6 Entry ids and instance ids are not content identity

The task statement lists entry ids among what `H(P, C)` feeds. They are not fed by it. An
entry id is the hash of the user tags, system tags and input data hash
(`row_ids.py:60`); an instance id adds the source-info tokens (`row_ids.py:95`). Neither
holds the node content hash, so neither changes. The node content hash enters the record
id (`row_ids.py:136`), the `__node_content_hash` column, and the operator cache's row
key (`operator_node.py:1455`). That is consistent with the design; only the wording
differs.

## 4. Worked examples

Mock values. `P:`, `C:`, `I:` are the three chains; `ci:` is content identity and `ni:`
semantic identity. `S` is a schema, `f`, `g` are function pods.

### 4.1 A chain

Slot `x` bound to source `A`; `x -> f -> g`.

| Node | `P` | `C` | `I` | `ci` | `ni` |
|---|---|---|---|---|---|
| `x` | `P:11` = `H(S)` | `C:aa` (source `A`) | `I:x1` = `H("slot","x")` | the source's own | `ni:01` = `H(P:11, I:x1)` |
| `f` | `P:22` = `H(f, P:11)` | `C:aa` | `I:x1` | `ci:c2` = `H(P:22, C:aa)` | `ni:02` = `H(P:22, I:x1)` |
| `g` | `P:33` = `H(g, P:22)` | `C:aa` | `I:x1` | `ci:c3` = `H(P:33, C:aa)` | `ni:03` = `H(P:33, I:x1)` |

`C` and `I` pass through unchanged; `P` alone tells `f` from `g`.

### 4.2 A diamond over one slot

`x` (source `A`) feeds `f` and `h`; `Rejoin(f, h)`.

| Node | `P` | `C` | `I` |
|---|---|---|---|
| `f` | `P:22` | `C:aa` | `I:x1` |
| `h` | `P:44` | `C:aa` | `I:x1` |
| `Rejoin` | `P:55` = `H(Rejoin, {P:22, P:44})` | `C:d1` = `H([C:aa, C:aa])` | `I:d1` = `H([I:x1, I:x1])` |

The repeat is kept: `C:d1` differs from `C:aa`. The order is by `P` (`P:22` before
`P:44`); no tie.

### 4.3 Two slots with one schema over one source

Slots `x` and `y`, both bound to `A`; `x -> f -> g` and `y -> f -> g`. This is ITL-1442.

| Node | `P` | `C` | `I` | `ci` | `ni` |
|---|---|---|---|---|---|
| `f` under `x` | `P:22` | `C:aa` | `I:x1` | `ci:c2` | `ni:02` |
| `f` under `y` | `P:22` | `C:aa` | `I:y1` | `ci:c2` | `ni:12` |
| `g` under `x` | `P:33` | `C:aa` | `I:x1` | `ci:c3` | `ni:03` |
| `g` under `y` | `P:33` | `C:aa` | `I:y1` | `ci:c3` | `ni:13` |

Two `f` nodes and two `g` nodes with distinct edges. Equal content identity: both write
the same table under the same `__node_content_hash`, so the same stored rows. How the
second node avoids a second write is Q3.

### 4.4 The same over two sources

`x` bound to `A`, `y` bound to `B`.

| Node | `C` | `ci` | `ni` |
|---|---|---|---|
| `f` under `x` | `C:aa` | `ci:c2` | `ni:02` |
| `f` under `y` | `C:bb` | `ci:e2` | `ni:12` |

Same `P`, so one table; different `__node_content_hash`, so the rows are told apart, as
today. The semantic identities are those of 4.3: the graph did not change.

### 4.5 A commutative join of two inputs with equal `P`

`Join` over slots `x` and `y` of one schema whose data columns are all data-tags (the
case of 2.4), `x` bound to `B`, `y` to `A`. Take `C:aa < C:bb` and `I:x1 < I:y1`.

- `P(K) = H(Join, [P:11, P:11])`, two items whatever the slots hold (3.5).
- `C(K) = H([C:aa, C:bb])`: ordered by `C`, so `y`'s source comes first.
- `I(K) = H([I:x1, I:y1])`: ordered by `I`, so `x` comes first.

The two chains order the group differently, and that is intended: swapping which slot
holds which source changes neither `C(K)` nor `I(K)`, which is what commutative means.
What it costs is 10.1.

### 4.6 A rebind

4.1, then `job.bind(sources={"x": B})`.

| Node | before | after |
|---|---|---|
| `f` `ci` | `ci:c2` = `H(P:22, C:aa)` | `ci:e2` = `H(P:22, C:bb)` |
| `f` `ni` | `ni:02` | `ni:02` |
| `f` table path | `.../schema:22` | `.../schema:22` |

Graph keys, `job.nodes`, edges and restrictions keep their keys. The content identity
follows the bind at every depth, which is the goal of ITL-1427. Rows stored before keep
`ci:c2`; rows stored after carry `ci:e2`.

### 4.7 An unbound blueprint

4.1 with `x` unbound: every node has `P` and `ni` as in 4.1, and no `C` or `ci`. The
blueprint's node keys equal the job's node keys, so the remapping between the two that
`as_pipeline` does today (`job.py:816` to `:841`) is no longer needed.

## 5. Inventory

Every line is `src/orcapod/` at `c1040ffa`.

### 5.1 Content hash used as node identity: moves to semantic identity

| Place | What it does |
|---|---|
| `core/base.py:178`, `:189` | `__hash__` and `__eq__` of every stream, node and invocation: content hash and identity structure. Every node-keyed dict and set below goes through these |
| `pipeline/base.py:341`, `:344` | `_record_invocation`: the invocation's key and its upstream keys in `_hash_graph`, `_invocation_lut`, `_source_streams` |
| `pipeline/base.py:360` | `_remember_invocation`: two invocations with one key become one node, keeping the smallest label (Q1) |
| `pipeline/base.py:519`, `:522` | compile: leaves found and ordered by hash key |
| `pipeline/base.py:578` | compile: a node's upstream nodes looked up by the input's content hash |
| `pipeline/base.py:611` | compile: equal labels numbered in content hash order |
| `pipeline/base.py:622` to `:637`, `:656` to `:662` | `_persistent_node_map`, `_node_graph`, `_node_lut`, `_upstreams`, `_graph_edges`, all keyed by the hash key |
| `pipeline/base.py:667` | `_ordered_node_map`: ties broken by the key |
| `pipeline/base.py:727`, `:805`, `:809` | `to_invocations` and `from_invocations` rebuild the graph by content hash |
| `pipeline/dag.py:116` to `:121` | `OrcaDAG` keeps nodes as dict keys (`__hash__`); `networkx_backend.py` the same |
| `pipeline/job.py:192` to `:197`, `:255` to `:264`, `:548` to `:555` | `_sources_by_hash`, and the second index by the schema-based hash of `as_node()` |
| `pipeline/job.py:769` to `:846` | `as_pipeline`: maps job keys to blueprint keys, because the two differ today |
| `pipeline/job.py:953` to `:970` | `is_runnable`: walks `_hash_graph`, with a comment that the object graph breaks after `bind()` because `SourceJobNode.__hash__` changes |
| `pipeline/job.py:1305` to `:1368` | the run: the hash-keyed DAG, the excluded set and the execution DAG of node objects. Equal nodes collapse here (ITL-1442) |
| `pipeline/job.py:2006` to `:2149` | `load`: node map, predecessors and node graph by saved key |
| `pipeline/graph.py:174`, `:175`, `:293`, `:300` | save: nodes keyed by content hash, edges as pairs of those keys |
| `pipeline/graph.py:356` to `:652` | load: nodes and upstream map by saved key |
| `pipeline/sync_orchestrator.py:108`, `:109`, `:114` | `outputs`, `processed` and `held_streams` keyed by node |
| `pipeline/async_orchestrator.py:230` to `:274` | edge maps, output channels, edge readers, collectors, held rows keyed by node |
| `pipeline/node_run.py:276` to `:280` | `RunPlan.held`, `held_counts`, `held_rows` keyed by node |
| `core/nodes/source_node.py:1534`, `:2187` | a leaf that is not a source is named `<label>:<content hash>` (PR 613 removes this) |
| `core/nodes/function_node.py:2111`, `:2174`, `:2667`, `:4154`, `:4563`; `operator_node.py:1757`, `:2336`; `source_node.py:1366`, `:1406` | `node_hash` given to `on_node_start` and `on_node_end` (`protocols/observability_protocols.py:116`). Two nodes with one content identity must not be one node to an observer |
| `pipeline/restrictions.py:213` | restrictions found per source content hash; under ITL-1472 they are kept per edge, keyed by semantic identity |
| `core/operators/static_output_pod.py:248` and every `argument_symmetry` | a frozenset of streams removes members equal by content, which changes the pipeline hash today (3.5) |

Already keyed by `id()` and unaffected: staleness (`core/nodes/staleness.py:98`, `:113`),
the diagram (`pipeline/diagram.py:434`), provenance tracing (`pipeline/provenance.py`),
the run summary (`pipeline/run_summary.py:532`), the sync orchestrator's neighbour map
(`sync_orchestrator.py:102`).

### 5.2 Content hash used as stored identity: moves to `H(P, C)`

| Place | What it stores |
|---|---|
| `core/nodes/function_node.py:3271` | the record id's preimage (`row_ids.py:136`) |
| `core/nodes/function_node.py:3288` | `__node_content_hash` of a pipeline row |
| `core/nodes/function_node.py:2292`, `:3601`, `:3709` | reads that keep this node's rows in a shared table |
| `core/nodes/operator_node.py:1455` to `:1479` | `__node_content_hash` of an operator cache row, and through it the row key `_record_hash` |
| `core/nodes/operator_node.py:1398` | `_filter_by_content_hash` on every cache read |
| `core/nodes/function_node.py:670`, `operator_node.py:536` | the `instance:<hash>` path component of `table_scope="content_hash"` (not the default) |
| `core/sources/derived_source.py:292`, `:325` | a plain derived source's default id and identity, from the origin node |
| `pipeline/job.py:1376` | the run URI's snapshot hash, from the sink nodes |
| `pipeline/job.py:1686` to `:1705`, `:2029` to `:2121` | `run.node_job_content_hashes` in a saved job, by label |
| `pipeline/graph.py:226`; `function_node.py:1130`, `:1879`; `operator_node.py:790`, `:1137`, `:1189`, `:1300`; `loaded_side_effects.py:65`, `:209` | the `content_hash` of a saved node descriptor, and a loaded node returning it |

Stays as it is, because it is a leaf's `C`: `sources/base.py:461` (saved source
identity), `source_proxy.py:125`, `cached_source.py:292`. Stays, because it is a pod's or
a row's hash and no node's: `function_pod.py:497` and `hooks.py:66` (`pod_hash`),
`static_output_pod.py:232` (an operator's uri), and every `data.content_hash()`.

### 5.3 Pipeline hash: stays

| Place | Use |
|---|---|
| `function_node.py:668`, `operator_node.py:534`, `side_effects.py:1203` | the `schema:<P>` component of pipeline, operator cache and side-effect log paths |
| `join.py:215`, `:432`, `:485`, `:496`; `streaming_joins.py:691`, `:716`, `:1525`; `merge_join.py:348`; `batch.py:198`, `:232`; `group_by.py:555`, `:731`; `operator_node.py:2391` | canonical input order and system-tag column names |
| `side_effects.py:208`, `:989`, `:1288`, `:1543`; `function_pod.py:218` | `invocation_hash` (`P :: entry id`) |
| `static_output_pod.py:991`, `:1924`; `semijoin.py:319`; `index.py:140`; `pick.py:151` | per-input caches and declared forms keyed by `P` |
| `pipeline/provenance.py:447`, `:479`, `:516`, `:1198` | finding an input's system-tag columns |
| `pipeline/graph.py:227`, `pipeline/base.py:653`, `:680`; `sources/base.py:462`; `source_proxy.py:131` | saved and compared as is |

## 6. The stopgap refusals of PRs 577, 608 and 613

PRs 608 and 613 are each stacked on 577.

| Refusal | PR | Under this design |
|---|---|---|
| Two different source objects with one content hash in a job (`InconsistentSourceError`) | 577 | **Stays**, reworded: one object per source identity (leaf `C`). Equal `C` must mean the same rows, and nothing in the chains checks that. What changes is that the one object may fill several slots. Relaxing it is ITL-1553 |
| One source id with different Arrow schemas or key columns | 577 | **Unnecessary once each reading has its own slot name**: the readings differ in `C` and in `I`. Stays for two readings that would share a slot name (ITL-1506) |
| Capture refused once any slot is re-bound or unbound | 577 | **Unnecessary**. A recorded step's key is `H(P, I)`, which a bind does not touch, so capture after a bind is well defined |
| A prebuilt stream over the job's own slot refused | 577 | **Unnecessary if Q2 is answered so that a plain stream and the job agree on the slot**; otherwise stays |
| A slot object of another job, or a standalone one, refused | 577 | **Stays**. It is about who owns a mutable slot object (questions 9 to 11 of ITL-1559), which no hash answers. The design makes the remedy possible: a slot is a name and a schema, so the receiving job can make its own |
| Capture into, or recompiling, a loaded job refused | 608 | **Stays**. A loaded job has no recorded invocations; that is ITL-1560. Section 11.3 says what the saved file should hold so that this can be lifted later |
| Two slots reading one source identity refused (`_require_distinct_slot_bindings`), and "one source identity would fill slots X and Y" at capture | 613 | **Unnecessary**. This is the case the design exists for |
| A captured slot cannot be renamed | 613 | **Stays for now**. The name is the `I` leaf; a rename re-keys every node below. Possible later as an explicit graph operation |
| Duplicate slot labels, and a slot label equal to a step label, refused | 613 | **Stays, and becomes load-bearing**: two slots with one name and one schema would have one semantic identity |
| A leaf that is not a source (a raw stream, a joined stream) refused | 613 | **Stays** (ITL-1127); such a leaf has no name |

Build on: PR 577 (the prepare phase that lets every tracker refuse before any records;
`source_consistency.py`; the one-object registration) and PR 613 (slot names from labels,
`as_slot`, label-keyed `bind`, `job.nodes` and saved jobs, ordered inputs in saved
files). PR 613's slot name is exactly the `I` leaf. PR 587 (positioned edges, ITL-1419)
gives the input list that 3.5 needs. PR 608 is independent of the hashing and can land
before or after.

Take out when building: `_require_distinct_slot_bindings` and the capture-time slot
collision of 613, the "slots are not as captured" check of 577, and 613's rule that an
unbound slot's content hash holds its name.

## 7. Golden entries

Derived by reading the golden folder and the manifest, and confirmed with a read-only
prototype kept outside the repository: it computes every golden case twice, once as on
`main` (2,504 entries, none differing from the files) and once with the content hash of
every derived stream, node and invocation replaced by `H(P, C)` as in section 2, and
compares. The generator was not run and no golden file was touched.

### 7.1 What moves: 104 entries

| File | Entries | Class | Reason |
|---|---|---|---|
| `pipeline-02.json` (open revision, frozen at 0.4.0) | 25 | `content_hash` of a derived stream or job node | the definition is replaced |
| `provisional/pipeline.json` | 77 | the same, in cases that use a provisional operator | the same |
| `provisional/pipeline.json` | 2 | default source id of `node.as_source()` | it holds the origin node's content identity |

`pipeline-02.json`, 25 entries, all `<name>.content_hash`:
`column_selection.drop_data`, `column_selection.drop_tag`, `column_selection.select_data`,
`column_selection.select_tag`, `drop_nulls.data`, `drop_nulls.data_many`,
`drop_nulls.tag`, `drop_nulls_all.all`, `drop_nulls_all.method`,
`drop_nulls_job.node[present]`, `drop_nulls_job.node[ratio]`,
`drop_nulls_job.node[renamed]`, `job.node[adjust]`, `job.node[joined]`,
`join_key_nullability.join`, `join_key_nullability.semi_join_left_nullable`,
`join_key_nullability.semi_join_right_nullable`, `join_pod.join`, `join_pod.pod`,
`map_data`, `pod`, `semi_join`, `tag_and_data_columns.drop`,
`tag_and_data_columns.select`, `tag_and_data_columns.select_tag_and_data`.

`provisional/pipeline.json`, 77 entries, all `<name>.content_hash`:

- `add_tags.every_type`, `add_tags.method`, `batch`, `filter.keyword`, `filter.terms`,
  `filter.tree`, `group_by`, `group_by_nan_keys`, `group_by_nan_keys.data_tag_key`,
  `index_pick.index`, `index_pick.pick`, `join_key_nullability.merge_join`,
  `map_columns.drop_unmapped`, `map_columns.rename`, `merge_join`, `polars_filter`,
  `polars_filter_expr.expr`, `polars_filter_expr.mixed` (18);
- `filter_job.node[adjust]`, `filter_job.node[joined]`, `filter_job.node[older]`,
  `polars_filter_expr_job.node[adjust]`, `polars_filter_expr_job.node[joined]`,
  `polars_filter_expr_job.node[older]` (6);
- `group_by_arrangement.{default,grouped_again,member_order_y}.{interleaved,reversed,tags_first}` (9);
- `rejoin.filtered_branch`, `rejoin.method`, `rejoin.pod_branch`, `rejoin.three`,
  `rejoin.three_reordered`, `rejoin.two`, `rejoin.two_reordered` (7);
- `rejoin_arrangement.{job_async,job_sync,stream}.{data_tags,mixed,plain}.{interleaved,reversed,tags_first}` (27);
- `rejoin_job.node[X]` and `rejoin_job_async.node[X]` for `X` in `SelectDataColumns`,
  `ages`, `ratio`, `rejoined`, `weights` (10).

`provisional/pipeline.json`, 2 entries: `default_source_ids.as_source[function_node].source_id`
and `default_source_ids.as_source[operator_node].source_id`.

After PR 609 two more move with them: the two `content_hash` entries it adds for the
plain derived sources, which hold the origin's content identity. The list must be
computed again on the final base before it is put to the developer.

Pairs that are equal today stay equal: `rejoin.two` and `rejoin.two_reordered`,
`rejoin.three` and `rejoin.three_reordered`, and the three arrangements of each
`group_by_arrangement` and `rejoin_arrangement` variant.

### 7.2 What must not move, and does not

Checked entry by entry in the prototype; no violation.

| Class | Entries checked |
|---|---|
| Pipeline hashes (`*.pipeline_hash`) | 179 |
| System-tag column names (`*.system_tag_columns`) | 164 |
| Record id and source id values of rows (`_tag_record_id::*`, `_tag_source_id::*`) | 421 each |
| Row data hashes and tag hashes | 409 and 12 |
| Stored table paths (`*.table_paths`) and pipeline instance ids | 6 and 6 |
| Result-table keys (`*.input_data_hashes`) | 6 |
| Function uris, schema hashes, table hashes | 2, 38, 9 |
| Source ids of root sources (`schema-01.json`, `pipeline-02.json`) | 42 |
| Content hashes of root sources, raw streams and operators | 119 |
| Every entry of the `arrow`, `semantic`, `datagram`, `schema` and `torch` groups | all |

Source-info tokens: no golden row sits below a default-named derived source, so no token
moves. Outside the golden files, a token that names such a source does move, with the
source id.

Not pinned by any golden entry, and moving: record ids (they hold a random result id) and
the operator cache's `_record_hash`.

### 7.3 Other hard-coded values

Nine test files outside the golden folder hold 64-hex literals, and 184 test files call
`content_hash()`. Each must be read when the definition changes (grep `tests/` and
`test-specification/` for 64-hex literals and run those files before reporting).

## 8. Implementation plan

The developer wants this system built first and used for every slot, edge and
source-binding issue (2026-11-28). 8.1 is the steps, 8.2 what happens to the open PRs,
8.3 what each issue gets from the chains and what is left.

### 8.1 Steps

Each step keeps the suite green. Sizes are of the production diff.

| Step | Content | Golden | Size |
|---|---|---|---|
| 0 | Merge PR 577, then 608 and 613 (8.2). PRs 607 and 615 can land at any time before step 5: their golden moves are disjoint from 7.1 (three root source content hashes in `schema-01.json`, instance ids and result paths) | theirs | none here |
| 1 | `core/identity_chains.py`: the five labelled hashes, the canonical order of 2.4, pooling over an input list. Streams, nodes and invocations get `source_chain()`, `name_chain()`, `content_identity()`, `node_identity()` beside the existing hashes. Nothing reads them yet. Tests compare `H(P, C)` across plain stream, invocation and job node | none | small, about 300 lines |
| 2 | Close 3.5: `P` at a commutative operator from an input list (a multiset), on PR 587's positioned inputs. Apply the decision on 3.1. Both are pipeline hash changes for one narrow case each, approved as such. Test that every other `P` is unchanged (the 179 golden pipeline hashes, and stored paths of a job) | no golden entry; stored paths move for a commutative operator over content-equal inputs of equal `P`, and with 3.1(a) for context-taking pods | small |
| 3 | Graph keys to semantic identity: `_hash_graph`, `_invocation_lut`, `_persistent_node_map`, `_sources_by_hash`, compile, `to_invocations`, `from_invocations`, `as_pipeline` (the remap goes), `is_runnable`, the run DAG. Job nodes stop being equal by content: node-keyed containers key by node object or by semantic identity. Observers get the semantic identity. Content hash still the old recursion | none | large: `pipeline/base.py`, `job.py`, `dag.py`, both orchestrators, `node_run.py` |
| 4 | Saved files: node keys become semantic identities; descriptors hold `node_identity` and `content_identity` (nullable). Loader reads the new form | none | medium: `graph.py`, `job.py`, `serialization.py`, the loaded node classes |
| 5 | The switch: `content_hash()` of every derived stream, node and invocation becomes `H(P, C)`; the unbound slot's name leaves the content hash (3.4); a bind invalidates content identity below it (ITL-1427). One golden regeneration, with the list of 7.1 approved entry by entry | 104 entries (106 after PR 609) | small in code, large in tests |
| 6 | Two slots on one source: remove the refusals named in section 6, implement the answer to Q3, tests of 9.2 | none | medium |
| 7 | Docs: `docs/concepts/identity.md`, `docs/reference/hashing.md` (the new labels and the hash site inventory), `CLAUDE.md` and `.zed/rules` ("Two identity chains" becomes three) | none | medium |

Order against PRs 607, 615 and 609, so that each golden value is regenerated once:

- 607 and 615 touch no entry of 7.1. They can land at any time before step 5.
- 609 changes the two `as_source[...]` source ids of 7.1 (it adds the job name to them)
  and adds two entries that step 5 moves. To move those four once, step 5 should be
  stacked on 609 and the two put to the developer as one list, or 609 should land first
  with the four accepted as moving twice in the provisional file (no release freezes
  it). I recommend stacking.
- Step 5 is the last golden-moving change of the series, on a base that holds all three.

Steps 3 and 4 carry the risk and move no hash. Step 5 moves hashes and is mechanical.
Keeping them apart means a failure in step 5's golden check can only be a hash mistake.

### 8.2 The open PRs

| PR | Recommendation | Why |
|---|---|---|
| 577 (one source object per identity, ITL-1469) | **Merge first, as is** | It is ready and mergeable, it is the guard against wrong results on `main` while the steps are built, and its core rule stays (section 6). Its prepare phase and `source_consistency.py` are what steps 3 and 6 build on. Step 6 removes two of its refusals (capture after a rebind; a prebuilt stream over an own slot) |
| 608 (loaded job refuses graph changes, ITL-1560) | **Merge first, as is**, after 577 | Small, and the refusal stays under the design. Needs its conflicts with `main` resolved |
| 613 (slot names from labels, ITL-1565) | **Merge first**, after 577, once its conflicts are resolved | The slot name it defines is the `I` leaf, so step 1 needs it. It temporarily withdraws several slots on one source; step 6 removes that refusal and the two others named in section 6, and step 5 removes its rule that an unbound slot's content hash holds the name |
| 587 (positioned edges, ITL-1419) | **Rework on top of step 3** | Its edge is `(upstream key, node key, position)` with content-hash keys; the keys become semantic identities in step 3, and its dependence on 577 (one object per content hash, so that a key names one input) goes away. `pipeline/edges.py` (`input_positions`, `node_inputs`) is kept as it is and is what step 2 uses for the input list. If it is ready before step 3 starts, merging it first and re-keying it in step 3 is equally good |

None is superseded. Nothing in them is thrown away except the refusals listed in
section 6.

### 8.3 What each issue gets

| Issue | Do the chains resolve it? | What remains | Step |
|---|---|---|---|
| ITL-1442, two slots on one source | Yes. The two branches have distinct semantic identities, so no node collapses; equal content identity, so one set of stored rows | The write rule for two nodes of one content identity (Q3), so that the async run stores one entry per row | 3 (nodes no longer collapse), 6 (Q3, refusals removed) |
| ITL-1427, hashes after a bind | Yes. Content identity is `H(P, C)` with `C` read from the slot, so it follows a bind at every depth, also on a loaded job without its pods (11.3) | Invalidating cached `C` and content identity below a slot when it is bound; the issue's tests | 5 |
| ITL-1204, slot lost from the run graph after a bind | Yes, outright. The slot's key is its semantic identity, which a bind does not change; today `SourceJobNode.__hash__` changes at a bind (`job.py:931`) | Nothing beyond step 3; `is_runnable`'s workaround goes | 3 |
| ITL-1470, a stream built before a job gets other system-tag names | No. It is a difference in `P`: the stream built outside becomes a leaf slot of its own, with its narrower schema, so the schema hash in the column names differs. No chain changes `P` | The capture rule: a stream brings its upstream steps down to its sources (ITL-1387), or is refused (PR 613 refuses a leaf that is not a source). The chains add the check: the job node's `P` must equal the plain stream's (9.1, PR 579) | 6, or its own PR after it |
| ITL-1506, one source id read with different key columns | Yes, once each reading has its own slot name: the readings differ in `C` (key columns are in the leaf) and in `I` | Removing the refusal for distinctly named slots; `bind` and the saved file address slots by name (PR 613) | 6 |
| One slot, two labels | By decision Q1. With (b) they are two nodes of one content identity, the same case as ITL-1442 | Q1; the label enters the `I` chain | 1 (definition), 3 (graph) |
| Binding a slot to something that depends on a slot (cycles; a stale slot-to-slot binding after recompiling) | No, and the chains say why it must stay refused: a slot's `C` is its bound leaf's `C`. If what is bound depends on a slot, `C` is defined through that slot, and through itself in a cycle; and what is bound holds a reference to a slot object that a recompile replaces | One rule at `bind` and at capture: what a slot holds is a leaf with a `C` of its own, with no slot beneath it (walk with `streams_beneath`). PR 577's foreign-slot refusal and PR 613's refusal of a leaf that is not a source already cover it; step 6 states it once and keeps it. Binding a composite properly is ITL-1127 | 6 |
| Edges carrying input position (PR 587) | Needed by the chains, not resolved by them: pooling and 3.5 need each node's inputs as an ordered list with repeats | Re-key its edges by semantic identity | 2 (the list), 3 (the keys) |
| Restrictions on edges (ITL-1472) | Made possible. An edge is `(upstream semantic identity, node semantic identity, position)`, which survives a rebind and exists for an unbound blueprint; two slots on one source are two edges and can carry two restrictions | The edge registry itself (PR 580's design), the rule for stored rows under a restriction (Q4), and Q3 for two restricted nodes of one content identity. Not part of this work | after 6 |
| Capture after a rebind (PR 577) | Yes. A recorded step's key holds no source | Passing a source opens its slot or matches it by object; passing the slot reads it (the developer's direction in ITL-1559). Remove the refusal | 6 |
| Capture into a loaded job (PR 608, ITL-1560) | No. A loaded job has no recorded invocations | The saved file gets semantic keys and ordered inputs (step 4), from which the graph could be rebuilt; lifting the refusal is its own work | 4 prepares; not lifted here |
| Slot names (PR 613, ITL-1565, ITL-1428) | They are the `I` leaf | Duplicate names stay refused; a rename after capture stays refused | 0 (merge), 1 |
| Several slots of one source fail to load (ITL-1554) | Yes. Saved node keys are semantic identities and no longer collide | The new file form | 4 |

## 9. Tests the design needs

All through the public interface, with sources of two or more tag columns and two or more
data columns, `tag_only_columns=True` unless the role is the subject, in more than one
arrangement, and on the sync and the async path.

### 9.1 The chains

- `P` unchanged: every golden pipeline hash, plus stored paths and system-tag column
  names of a job, before and after each step.
- A plain stream, its invocation and its job node have equal `P` and equal content
  identity, built before the job and inside it (with ITL-1470's test, PR 579).
- Pass-through at every unary operator, function pod and side-effect pod; argument order
  at `SemiJoin` and `MergeJoin`; `P` order at `Join` and `Rejoin` with two and three
  inputs in every argument order.
- Repeats kept: a diamond's `C` differs from its branch's `C`.
- The state of `main` on ties, pinned before anything changes: over two sources of one
  schema, `Join` and `Rejoin` refuse the pair when a plain data column is shared and
  accept it when every data column is a data-tag; `MergeJoin` and `SemiJoin` are
  ordered; no exported operator returns a nested symmetry (a test over
  `orcapod.operators` that fails when a new operator is commutative or partly symmetric
  until it has a tie case).
- The equal-`P` group, with `Join` and `Rejoin` over two all-data-tag slots, with two
  tags and two data-tags, plain and in a job, sync and async: `P` is the same whether
  the two slots hold one source, two equal objects or two different sources (3.5); both
  arrangements of two sources over the two slots give one `C`, one `I`, one table of
  rows and the same system-tag column names and values.
- 3.1: a plain and a context-taking pod of one uri over one source are two nodes.
- Unbound: every node has a semantic identity; asking for content identity raises and
  names the slots.
- The label rule: `tests/test_hashing/test_hash_label_rule.py` extended to the five
  labels; `test_hash_grammar.py` and `test_hash_site_inventory.py` hold the new sites.
- Order invariance: `tests/test_core/test_order_invariance.py` gets a case per identity
  (content and semantic) for every operator, source and stream kind, and for a job, so
  that no arrangement of tags, columns or commutative inputs changes either.

### 9.2 Two slots on one source (ITL-1442)

- `A_x -> f -> g` and `A_y -> f -> g` bound to one source: four step nodes in `job.nodes`
  and in the run summary, every node holds the full rows, one stored pipeline entry per
  row and one result per input, sync and async, on a first run and a second.
- The same with a `Join` and with an operator node that caches.
- The same with one slot unbound: the bound branch runs, the other is left out.
- Bound to two sources, then rebound to one, then back: content identities follow at
  every depth, semantic identities never change, rows stored earlier keep their node
  content hash (ITL-1427's criteria, also for a loaded job).
- One dataset read with two key-column choices in two named slots (ITL-1506).
- Save and load of all of the above, including the case of ITL-1554 (one source in
  several slots feeding one function pod).
- `as_source()` of each of two nodes with one content identity gives one source id.
- A side-effect node under each of two slots on one source: the decided number of
  deliveries, with and without `track_completion`.

## 10. Open questions for the developer

### Q1. Is a node's own label part of its semantic identity?

One slot feeding the same pod twice under labels `r1` and `r2`: one node or two? Today
they are one node and the smaller label is kept (`pipeline/base.py:360`).

- (a) Not part: one node. `job.nodes["r2"]` does not exist, and a label the user wrote is
  dropped without notice.
- (b) An explicitly assigned label is part: two nodes with one content identity. The
  label must enter the `I` chain, not only the node's own hash, or the steps below `r1`
  and `r2` would merge again: `I(node) = H(("label", label), pooled I)` when a label was
  assigned, pass-through otherwise. A computed label never enters.

Recommendation: (b). A slot's label is already its identity, so a step's assigned label
being identity is the same rule. Edges, restrictions and probes can then be attached to
`r1` and `r2` apart (ITL-1472). The cost is that a labelled step can no longer be renamed
after capture without re-keying what is below it, as for a slot, and that Q3's case
arises more often, which it must handle anyway.

### Q2. A stream outside a job: how is its semantic identity formed?

- (a) From the source's label by the one rule that names a slot (PR 613: the assigned
  label, else the class name; `as_slot(name)` gives the name). A plain stream then has
  the semantic identity its job node will have.
- (b) A plain stream has no semantic identity. It has `P` and content identity; the job
  assigns `I` when it captures the stream, by walking to its roots.

Recommendation: (a), under three conditions. One function derives the name for the plain
stream and for the job, so they cannot differ. `I` and semantic identity are never cached
on an object whose label can still change (the name is frozen at capture, as PR 613
does); this answers question 10 of ITL-1559 for the name. And two different sources with
one default label (two `DictSource` objects) have one `I`: that is harmless outside a job
only if plain streams are not equal by semantic identity, so `__eq__` and `__hash__` of a
plain stream should stay content-based or become object identity, and only a graph
compares by semantic identity, where duplicate slot names are refused.

### Q3. Two nodes with equal content identity in one job

- (a) Write-if-missing on every pipeline and operator-cache write.
- (b) Compute once and feed both.

Recommendation: (a) as the rule, (b) later as an optimisation. Reasons:

- (b) is wrong when the two nodes sit below different restrictions (ITL-1472: equal
  content identity, different rows), so it needs (a) underneath in any case.
- The pipeline table's key, the record id, holds a random result id. When two nodes
  compute one instance at the same time (the async run of ITL-1442: 4 entries for 2
  rows), both miss the result store and write two results and two pipeline rows. So
  write-if-missing must be keyed by the instance id within one node content hash, not by
  the record id, and within one run the second node must wait for the first: a
  single-flight map keyed by (table path, node content identity, instance id). The
  operator cache's key (`_record_hash`) already covers the whole row, so there the rule
  is the existing skip of a row passed again, made safe for two writers in one run.
- It also covers two jobs that share a database.

To decide with it: whether two side-effect nodes with one content identity deliver once
or twice. The log is keyed by `P` and entry id, so with `track_completion` the second is
already skipped. I recommend once in every mode, by the same single-flight map.

### Q4. What a restricted chain's `as_source()` shows without a membership table

- (a) Every stored row of the node's content identity: a superset of the restricted view.
- (b) Refused, with the remedy to filter in the pipeline.
- (c) The stored rows filtered again by the restriction, where its columns survive.

Recommendation: (b), as a caching operator node and a `CachedSource` over a restricted
source are refused today. (a) shows rows the user restricted away under a name that says
restricted; (c) works for some graphs and not others.

## 11. Risks, cost, and loaded jobs

### 11.1 What cannot be made to hold

- **3.1** as stated: without a decision, two nodes merge (counterexample there).
- **"`P` unchanged in value" together with "`P` holds no source"** (3.5): on `main` the
  pipeline hash of `Join` or `Rejoin` over two inputs of equal `P` depends on whether
  they are equal by content. One of the two statements has to give for that case; I
  recommend that the value moves.
- **An attribution limit at a commutative operator with equal-`P` inputs** (4.5). `C` and
  `I` each order the group on their own, so the pair (`C`, `I`) does not say which slot
  holds which source: `x=A, y=B` and `x=B, y=A` give the same `P`, `C` and `I`. This is
  correct for the operator's output, and it is what the agreed condition asks for. The
  consequence to accept: content and semantic identity together never identify a
  binding. Anything that must know which source a slot holds (the membership table, a
  saved job) records it per slot, and must not try to derive it from the two hashes.
- **Equal `C` means equal rows only by convention.** The chains cannot check that two
  sources with one identity deliver the same data. The one-object rule of PR 577 stays
  for that.
- **Rows stored before the change are orphaned in place.** Table paths do not move, so a
  pipeline or operator cache table keeps rows whose `__node_content_hash` no node will
  ever compute again. They are never returned and never counted. With the new labels
  (2.1) they can be recognised by their `semantic_v1:` prefix. Nobody has such a store
  before 0.4.0. I do not recommend a layout bump for it, because that would move stored
  paths.

### 11.2 Cost

Measured on a 50-node chain (one source, 49 single-input operators), counting calls of
the semantic hasher's `hash_object` and wall time, caches cold:

| | calls | time |
|---|---|---|
| Today, content hash of the last node | 197 | 0.9 ms |
| Today, pipeline hash of the last node | 197 | 0.8 ms |
| Today, both | 394 | 1.6 ms |
| Three chains: `P`, then `H(P, C)` and `H(P, I)` at every node | 297 | 1.2 ms |

The three chains cost less than today's two, because `C` and `I` pass through
single-input nodes and the two combined hashes are flat: today's content hash walks the
pod and the upstream structure a second time. A multi-input node adds one pooled hash to
each of `C` and `I`. I did not get a clean number for a join-heavy graph: building a
`Join` already computes its inputs' pipeline hashes, so the caches are warm when the
measurement starts. With hashed pooling the cost is linear in nodes in every graph; with
a flat list it is exponential in nested diamonds (3.2).

A bind today must clear cached content hashes below the slot and does not (ITL-1427).
Under the design a bind invalidates `C` and content identity below the slot and nothing
else: `P`, `I` and semantic identity are untouched.

### 11.3 What a loaded job does

Today a saved job keys nodes by content hash, stores each node's `content_hash` and
`pipeline_hash`, and stores the run's content hash per label
(`run.node_job_content_hashes`), because a node loaded without its pod cannot compute its
content hash again (`operator_node.py:305`).

Under the design:

- Node keys are semantic identities. The blueprint and the job share them, and several
  slots of one source no longer collide in the file (ITL-1554).
- A loaded node keeps its saved `P`. Its `C` comes from its upstream nodes, so its
  content identity is `H(saved P, C)` computed live whenever every slot above it is bound,
  with no pod. A bind on a loaded job then moves the content identity of every node below
  it correctly, which today it cannot.
- The file still records each node's content identity at the time of the run, for a slot
  whose source cannot be rebuilt (an in-memory source): the node then reads its stored
  rows by the recorded value until a source is bound.
- A saved slot records its name and its bound source's identity per slot (11.1, the
  attribution limit).
- A file written before the change has other keys and content hashes that match no
  stored row written after it. I recommend refusing such a file with a message that says
  so, since no one has one. The developer may prefer a reader for it.
- Capture into and recompiling a loaded job stay refused (PR 608). With semantic keys
  and saved ordered inputs the graph can be rebuilt without the invocations, which is
  what lifting that refusal needs; it is not part of this work.

### 11.4 The later membership table

Nothing here blocks it. Its key would be a node's semantic identity (it survives a
rebind), its rows references by entry or instance id into the table at the node's `P`
path, scoped by the node's content identity. All three exist for every bound node. Since
semantic identity holds no source, the table must also hold the content identity it was
recorded under, or a rebind would show the old binding's members.
