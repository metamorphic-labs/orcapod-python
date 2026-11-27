# ITL-1511: identity of a commutative pod that counts repeated operands

Status: assessment for the developer to decide from. Nothing is implemented. No golden file
was written or edited.

## Summary

- Today a commutative pod holds its operands in a Python `frozenset`. Streams compare by
  content hash, so the set drops a repeated operand: `Join()(d)` and `Join()(d, d)` have one
  content hash and one pipeline hash, with 3 and 5 rows.
- The structure hasher already writes a `frozenset` as a **sorted list of its items' hashes
  with equal items kept**. In the pipeline chain two different sources of one schema already
  give `[h, h]`. So the hashed form is already a multiset form; only the Python `frozenset`
  in front of it drops repeats.
- **Recommended: candidate B2.** Keep the hashed form exactly as it is (tag `frozenset`,
  sorted items) and stop dropping repeats before it. Measured: every identity without a
  repeated operand is byte for byte what it is today, and **0 golden entries move**.
- A new tag or a counted form for all cases (B1, B3) moves **116 golden entries** (18 in a
  frozen-to-be file, 98 provisional). A second form used only on a repeat (A1, A2) moves 0
  golden entries but has a defect each (below).
- With the identity fixed and PR 587's refusal lifted (tried on PR 587's branch), a plain
  stream, a sync job, an async job and a saved and loaded job agree on the rows of
  `Join(d, d)`. What is left is the complete-repeat toggle: without it the two cross pairings
  of a self-join are two rows equal in every column, with one entry id downstream.

## Terms

- `d`: a source with data-tags only (`id`, `key`: tags that are also data columns), rows
  `(1, p)`, `(1, p)`, `(2, q)`. Rows 0 and 1 are equal in every tag.
- `e`: the same table under another `source_id`. Same schema, so the same pipeline hash as
  `d`; another content hash.
- `d2`: a second source object with `d`'s identity (same `source_id`, schema and key columns).
- Repeated operand: two operands with one content hash. One object given twice, or `d` and `d2`.

## 1. Where the operand set is formed today

### The declaration

`argument_symmetry(streams)` returns the operand group. A `frozenset` means commutative, a
`tuple` ordered.

| Pod | Where | Returns |
|---|---|---|
| `Join` | `core/operators/join.py`, `Join.argument_symmetry` | `frozenset(streams)` |
| `Rejoin` | inherits `Join.argument_symmetry` | `frozenset(streams)` |
| commutative binary operator | `core/operators/base.py`, `BinaryOperator.argument_symmetry`, when `is_commutative()` | `frozenset(streams)`. No shipped operator takes this branch (`MergeJoin` and `SemiJoin` are ordered) |
| function pod | `core/function_pod.py`, `argument_symmetry` delegates to `multi_stream_handler()`, a `Join()` | `frozenset(streams)` |

A function pod never holds several operands itself. `pod(a, b)` first runs the automatic
`Join()(a, b)`; the pod's own stream then holds `frozenset({joined})`, one operand. So a
repeated operand of a function pod is a repeated operand of that `Join`.

### Where the set is dropped to unique operands

`ContentIdentifiableBase.__hash__` and `__eq__` (`core/base.py`) compare by content hash. So
`frozenset((d, d))` and `frozenset((d, d2))` hold one member. This happens in Python, before
any hashing, and so for the content hash and the pipeline hash alike.

### Where the group enters a hash

The same structure serves both chains; the resolver picks the hash of each operand.

| Element | Structure |
|---|---|
| plain stream of an operator (`DynamicPodStream`, `core/operators/static_output_pod.py`) | `(pod, pod.argument_symmetry(upstreams))`, in `identity_structure` and `pipeline_identity_structure` |
| `StreamBase.identity_structure` (`core/streams/base.py`) | `(producer, producer.argument_symmetry(upstreams))` |
| operator job node (`core/nodes/operator_node.py`) | `(operator, operator.argument_symmetry(input_streams))` |
| function pod stream and node (`core/function_pod.py`) | `(function_pod, function_pod.argument_symmetry((input_stream,)))` |
| recorded invocation (`pipeline/pod_invocation.py`) | `(pod, pod.argument_symmetry(input_streams))`; its content hash is the graph key |

The structure hasher (`hashing/semantic_hashing/semantic_hasher.py`, `_expand_structure`)
writes a `frozenset` as `{"__type__": "frozenset", "items": [...]}`. Each item is the
operand's labelled hash (content hash, or pipeline hash through the resolver). The items are
sorted by their canonical JSON. **Equal items are not merged.** Measured on `main`:

| | content hash | pipeline hash |
|---|---|---|
| `Join()(d)` | `6147969d…` | `4d9f6f88…` (items `[h]`) |
| `Join()(d, d)`, `Join()(d, d2)`, `Join()(d, d, d)` | `6147969d…` | `4d9f6f88…` (set dropped the repeat) |
| `Join()(d, e)` | `14fd681e…` | `2308d98e…` (items `[h, h]`) |
| `Join()(d, d, e)`, `Join()(d, e, e)` | `14fd681e…` | `2308d98e…` (set dropped the repeat) |

So `Join()(d, e)` already hashes two equal items in the pipeline chain.

### Sync and async

Both paths read the same structures; no path forms an identity of its own. The rows do not
come from the set: the sync path gets every input in argument order, and the async `Join`
gets a positional list of the inputs' pipeline hashes (`_compute_system_tag_suffixes`). So a
standalone `Join()(d, d)` computes 5 rows on `main` while its identity says one operand.

### Graph edges, saved and loaded jobs

- `main`: the graph (`pipeline/base.py`, `_hash_graph`) is a set of `(upstream key, node key)`
  pairs, keys being content hashes. `Join()(d, d)` is one edge. Measured on `main`: a sync job
  gives the join node one input and returns 3 rows (standalone: 5); an async job raises
  `input_pipeline_hashes length (2) must match inputs length (1)`.
- PR 587: an edge is `(upstream, node, position)`. `pipeline/edges.input_positions` gives a
  commutative pod's inputs position `None`, read from `isinstance(argument_symmetry(...),
  frozenset)`. `edges.node_inputs` takes a node's inputs from its own `upstreams`, one entry
  per input. `operand_set` (`core/operators/repeated_operands.py`) is the one place the set is
  built, and it refuses a repeat (`RepeatedOperandError`).
- Saved job: `edges` lists each distinct `(upstream, node)` pair once; an operator node's
  `inputs` list (ITL-1417) holds its inputs in argument order, repeats included. The loader
  rebuilds a node's inputs from `inputs`. The legacy check
  (`pipeline/serialization.py`) also reads `isinstance(symmetry, frozenset)`.

## 2. Candidate identity forms

All candidates hash every operand, in the sorted order, without dropping a repeat. They
differ in what is written around the items.

| | Form without a repeat | Form with a repeat |
|---|---|---|
| A1 | `{"__type__":"frozenset","items":[...]}` (today) | `{"__type__":"multiset","items":[...]}`, chosen when two operands have one content hash |
| A2 | as A1 | as A1, chosen when two items of the list being hashed are equal |
| B1 | `{"__type__":"multiset","items":[...]}` | the same |
| B2 | `{"__type__":"frozenset","items":[...]}` (today) | the same, equal items kept |
| B3 | `{"__type__":"multiset","items":[[item, count], ...]}` | the same |

A is the issue's candidate (a), B its candidate (b). B2 is (b) with today's bytes.

### Experiment

An experimental patch (not committed; described in the appendix) made
`Join.argument_symmetry` and the commutative branch of `BinaryOperator.argument_symmetry`
return a group that remembers every operand, and made the structure hasher write it in each
form. A script computed 58 values under `main` and under each candidate: content and pipeline
hashes of `Join` (two and three inputs, both orders, one input), `Rejoin` (both orders), a
function pod (one and two inputs, both orders), `MergeJoin`, `Join()(d, e)`, and of every node
of a job (sync and async) with its stored table paths.

**Without a repeated operand** (44 of the 58 values):

| Candidate | Values that differ from `main` |
|---|---|
| A1 | 0 |
| A2 | 2: the pipeline hash of `Join()(d, e)` and `Join()(e, d)` |
| B1 | 32 (every commutative pod and every function pod) |
| B2 | **0** |
| B3 | 32 |

**With a repeated operand** (first 8 hex digits):

| content hash | `main` | A1 = A2 | B1 | B2 | B3 |
|---|---|---|---|---|---|
| `Join()(d)` | `6147969d` | `6147969d` | `08c4542a` | `6147969d` | `7260d433` |
| `Join()(d, d)` and `(d, d2)` | `6147969d` | `c2354f9a` | `c2354f9a` | `84d5789b` | `7f2e03f8` |
| `Join()(d, d, d)` | `6147969d` | `f873a98e` | `f873a98e` | `0df53fd6` | `f6208067` |
| `Join()(d, e)` | `14fd681e` | `14fd681e` | `7dff1a51` | `14fd681e` | `85838b09` |
| `Join()(d, d, e)` | `14fd681e` | `18b37c16` | `18b37c16` | `8d3bd48f` | `81e4496f` |
| `Join()(d, e, e)` | `14fd681e` | `10ba532f` | `10ba532f` | `3e5bd083` | `34940e83` |
| `Rejoin()(d)` | `a145e820` | `a145e820` | `1b1e0967` | `a145e820` | `428aefc9` |
| `Rejoin()(d, d)` | `a145e820` | `7a9a4c4d` | `7a9a4c4d` | `5294608d` | `a6492dca` |

| pipeline hash | `main` | A1 | A2 | B1 | B2 | B3 |
|---|---|---|---|---|---|---|
| `Join()(d)` | `4d9f6f88` | `4d9f6f88` | `4d9f6f88` | `d3a22318` | `4d9f6f88` | `67adfcf7` |
| `Join()(d, d)` | `4d9f6f88` | `09d3f298` | `09d3f298` | `09d3f298` | `2308d98e` | `b1fa0065` |
| `Join()(d, e)` | `2308d98e` | `2308d98e` | `09d3f298` | `09d3f298` | `2308d98e` | `b1fa0065` |
| `Join()(d, d, d)`, `(d, d, e)`, `(d, e, e)` | (dropped) | `2030b55a` | `2030b55a` | `2030b55a` | `87f582fb` | `f28ec453` |

Every candidate tells `Join()(d)` from `Join()(d, d)` from `Join()(d, d, d)`, and
`Join()(d, d, e)` from `Join()(d, e, e)`, in the content hash.

### What the experiment shows about each

- **A1** leaves every no-repeat identity unchanged. Defect: `Join()(d, d)` and `Join()(d, e)`
  have one schema and one topology but two pipeline hashes (`09d3f298`, `2308d98e`), so two
  stored tables and two sets of system-tag column names for what the pipeline hash is meant
  to treat as one. It also needs two forms and a rule for choosing between them.
- **A2** repairs that, and for it moves the pipeline hash of an existing no-repeat pod:
  `Join()(d, e)`. No golden entry pins such a join (see section 3), so the golden check stays
  silent, but it is a moved identity.
- **B1, B3** are one clean form, and move every commutative pod and every function pod.
- **B2** is one form, moves nothing without a repeat, and gives `Join()(d, d)` the pipeline
  hash `Join()(d, e)` already has (`2308d98e`): same schema and topology, one pipeline hash.

Is B2 sound? The tag `frozenset` with equal items is not a new encoding: the pipeline chain
writes it today for `Join()(d, e)`. It reads "an unordered group of operands, each hashed,
none dropped". A group without equal items is a set, and is written as today. No two
different operand groups get one encoding: the sorted list of labelled hashes is the
multiset. The label `semantic_v1` stays: no existing input gets another digest (the rule in
"Hash labels").

## 3. Golden entries each candidate moves

Measured with `generate_golden_hashes.py --check` on the experimental patch, once per
candidate. The baseline run reports no drift.

| Candidate | Result of `--check` | Entries moved |
|---|---|---|
| A1 | `golden values ... match` | 0 |
| A2 | `golden values ... match` | 0 |
| B2 | `golden values ... match` | **0** |
| B1 | 122 problems | 116: 110 values, and 6 stored tables that get a new path (each reported twice: old path "pinned but no longer produced", new path "computed but not pinned") |
| B3 | 122 problems | the same 116 names as B1 (compared as sets) |

Why A2 moves no golden entry although it moves `Join()(d, e)`: two inputs of one pipeline
hash have the same data columns, and `Join` and `Rejoin` refuse shared data columns. The only
such join that is accepted has data-tags only, and no golden case pins one. The implementing
PR should add that case (`Join()(d, e)`, content and pipeline hash, system-tag column names);
adding a golden value is allowed in any release.

### The 116 entries B1 and B3 move

One explanation covers every entry: the entry is the identity of a commutative pod (`Join`,
`Rejoin`) or of a function pod (its operand group comes from `Join`), or is built from one (a
node downstream, a stored path that holds the pipeline hash, a default source id that holds
the node's content hash). The tag around the operand list changes, so each moves.

`pipeline-02.json` (not yet released; frozen at 0.4.0), 18 entries:

| Entries | Why |
|---|---|
| `pipeline:pod.content_hash`, `.pipeline_hash` | a function pod's stream |
| `pipeline:join_pod.join.{content,pipeline}_hash` | a `Join` |
| `pipeline:join_pod.pod.{content,pipeline}_hash` | a function pod over that `Join` |
| `pipeline:join_key_nullability.join.{content,pipeline}_hash` | a `Join` |
| `pipeline:job.node[joined].{content,pipeline}_hash`, `job.node[adjust].{content,pipeline}_hash` | a `Join` node and the function node over it |
| `pipeline:job.table_paths`, `job.table[…/schema:<pipeline hash>/pdb_v1].record_ids` | the function node's pipeline table path holds its pipeline hash |
| `pipeline:drop_nulls_job.node[ratio].{content,pipeline}_hash`, `.table_paths`, `.table[…].record_ids` | a function node and its table path |

`provisional/pipeline.json` (outside the freeze), 98 entries:

| Entries | Count | Why |
|---|---|---|
| `default_source_ids.as_source[function_node].source_id`, `[operator_node].source_id` | 2 | the default id holds the node's content hash |
| `filter_job.*`, `polars_filter_expr_job.*`: `node[joined]`, `node[adjust]` hashes, `table_paths`, one table each | 12 | a `Join` node, the function node over it, its table path |
| `rejoin.{two,two_reordered,three,three_reordered,method,pod_branch,filtered_branch}.{content,pipeline}_hash` | 14 | a `Rejoin` |
| `rejoin_arrangement.{stream,job_sync,job_async}.{plain,mixed,data_tags}.{tags_first,interleaved,reversed}.{content,pipeline}_hash` | 54 | a `Rejoin` |
| `rejoin_job.*`, `rejoin_job_async.*`: `node[SelectDataColumns]`, `node[ratio]`, `node[rejoined]` hashes, `table_paths`, one table each | 16 | a `Rejoin` node, the automatic select and the function node over it |

No entry of the `semantic`, `schema`, `arrow`, `datagram` or `torch` groups moves under any
candidate.

## 4. What else must change to lift the refusal

Tried on PR 587's branch with `operand_set` returning the full group instead of raising, in
form B2.

### Already right, nothing to change

- **System-tag column names.** `Join._compute_system_tag_suffixes` gives each input
  `<pipeline hash>:<canonical position>`; inputs of one pipeline hash take positions in a
  stable order. `Join()(d, d)` has columns `…::72ea…:0` and `…::72ea…:1`, the names
  `Join()(d, e)` has today.
- **Per-row pair sorting.** `arrow_utils.sort_system_tag_values` sorts the
  `(source id, record id)` pairs of columns that share a provenance path, per row. Measured:
  `Join` over `(d, d, e)`, over `(d, d, x)` (`x` with two data columns) and over `d` in two
  tag orders gives one set of rows, one content hash and one pipeline hash for every argument
  order, as a plain stream, a sync job and an async job.
- **Shared data columns.** `Join()(w, w)` and `Rejoin()(w, w)` over a source with a data
  column are still refused by the existing overlap check, before any identity is formed. So
  `Join` accepts a repeated input exactly when the shared columns are all data-tags.

### Must change

1. **The operand group.** `operand_set` becomes the one builder of the group (PR 587 already
   routes all three callers through it). It returns a group that keeps every operand and
   still is a `frozenset` for the `isinstance` readers (`edges.input_positions`, the legacy
   loader check), or those readers move to a named predicate. `RepeatedOperandError` and its
   tests go; the changelog entry under "Breaking changes" is reversed.
2. **The structure hasher** writes that group as in B2. `docs/reference/hashing.md` (the
   `frozenset` row, the operator row, the "equal pipeline hashes" row) says that an operand
   group keeps equal items.
3. **Job edges.** On PR 587's branch the job already agrees with the plain stream (below),
   because `node_inputs` reads the node's own `upstreams`. But a commutative edge is
   `(upstream, node, None)`, so two inputs from one upstream are one edge in the edge set.
   Decide whether an edge of a repeated commutative input carries an occurrence number
   (ITL-1472 puts restrictions and probes on edges: two inputs, two edges). The saved format
   needs no change: `edges` holds the pair once and `inputs` holds `[d, d]`, measured.
4. **The toggle** (developer decision, 2026-11-27), in section "The complete-repeat toggle".
5. **Diagram.** `Join(d, d)` draws one line, as `MergeJoin(a, a)` does (left to stage 2 of
   ITL-1472 by PR 587).

### Do a plain stream and a job agree?

| | `main` | PR 587 | PR 587, refusal lifted, B2 |
|---|---|---|---|
| plain `Join()(d, d)` | 5 rows, identity of `Join()(d)` | refused | 5 rows, own identity |
| sync job | 3 rows | refused | 5 rows, equal to the plain stream's in every column |
| async job | raises (`input_pipeline_hashes length`) | refused | 5 rows, equal |
| job holding `Join()(d)` and `Join()(d, d)` | one node (the second is lost) | refused | two nodes, 3 and 5 rows |
| saved, loaded, `run(load_only=True)` | n/a | refused | 5 rows; node hashes equal before and after; equal to the plain stream's hashes |

So with B2 and PR 587's edges, they agree on the join itself. They do not yet agree
downstream:

### Entry-id uniqueness downstream

`Join()(d, d)` has 5 rows: `(r0, r0)`, `(r0, r1)`, `(r1, r0)`, `(r1, r1)`, `(r2, r2)`. After
the per-row pair sort, `(r0, r1)` and `(r1, r0)` are equal in every column: tags, system
tags, source-info tokens, content hash. 4 distinct rows. `Join()(d, d, d)` has 9 rows, 5
distinct.

A function node's entry id hashes the row's tags, system tags and input data hash
(`_build_record_id_preimage`). The two equal rows have one entry id. Measured, a function
pod over `Join()(d, d)` in a job:

| | rows returned | pipeline table | distinct entry ids |
|---|---|---|---|
| sync | 5 | 4 rows | 4 |
| async | 5 | 5 rows | 4 (one entry id stored twice) |

The sync and the async store differ, and one entry id is stored twice. This is what the
toggle removes: "unique" leaves 4 rows with 4 entry ids; "repeat" adds a tag, and a tag is in
the entry id, so 5 rows with 5 entry ids and one data hash.

### The complete-repeat toggle

Decided: part of the operator's identity; unique by default; "repeat" keeps every copy and
adds a tag numbering the copies. Proposed shape, for the developer to confirm:

- `Join(repeats="unique")` (default) and `Join(repeats="number", occurrence_tag="occurrence")`.
  The same two arguments on `Rejoin` if it takes the toggle (open question 3).
- **Identity and config.** Today `Join.identity_structure()` is `"Join"`. Proposed: the toggle
  enters the identity and the config only when it is not the default, as `GroupBy`'s
  `member_order` does: `("Join", ("repeats", "number"), ("occurrence_tag", "occurrence"))`.
  Then no existing `Join` identity moves. Writing the default too would move every `Join`
  (at least the `Join` entries of section 3).
- **Unique.** One row per complete repeat: rows equal in every column, system tags and
  source-info tokens included. The copies are identical, so which is kept cannot matter.
- **Number.** The tag holds 0, 1, … among the copies of one complete repeat (`int64`, not
  nullable; 0 on a row with no copy). Copies are identical, so the numbering needs no order.
  The name is refused if the inputs have a column of that name, and by the reserved-prefix
  rules (`require_names_not_meta`, `require_names_not_system`).
- **Sync equals async.** The streaming join must drop or number copies as the sync path does;
  rows arrive in any order, so it keeps the rows seen so far (or is a barrier) when the
  toggle applies. Add the case to `test_async_sync_equivalence.py`.
- **Schema prediction.** In "number" mode the output schema has the extra tag whether or not
  an operand repeats: the schema never depends on rows.
- `test_order_invariance.py` and the several-item rule apply to the new arguments.

## 5. Test cases

Sources with two tag columns and two more columns, in two layouts (tag order and table
layout swapped), sync and async, plain stream and job, unless a case says otherwise.

Identity:

1. `Join()(d)`, `(d, d)`, `(d, d, d)`: three content hashes, three pipeline hashes.
2. `Join()(d, d)` equals `Join()(d, d2)` (one object or two of one identity).
3. `Join()(d, d, e)` differs from `Join()(d, e, e)` in content hash; one pipeline hash.
4. `Join()(d, d)` and `Join()(d, e)`: one pipeline hash, two content hashes.
5. Every argument order of `(d, d, e)` and `(d, d, x)`: one content hash, one pipeline hash,
   one set of rows, one set of system-tag column names.
6. No-repeat identities are unchanged: the golden check, plus a new golden case for
   `Join()(d, e)` (data-tags only, two sources of one schema).
7. The same for `Rejoin` and for a function pod given `(d, d)` (the repeat is in its `Join`).
8. The toggle: `Join()` and `Join(repeats="unique")` are one identity; `repeats="number"` is
   another; two `occurrence_tag` names are two identities; a saved config round-trips each.

Rows:

9. **Self-join with cross pairings**: `d` has two rows equal in every tag. `Join()(d, d)`,
   unique: 4 rows, pairings `{r0,r0}`, `{r0,r1}`, `{r1,r1}`, `{r2,r2}`. Number: 5 rows, the
   `{r0,r1}` pairing twice with `occurrence` 0 and 1, every other row 0.
10. `Join()(d, d, d)`: 5 rows unique, 9 numbered (`{r0,r0,r1}` and `{r0,r1,r1}` three times
    each).
11. A source with unique tags given twice: the rows of the source, in both modes; `occurrence`
    0 everywhere.
12. `Join()(d, d, x)` with `x` holding two data columns: data columns, source info and data-tags
    first among the data columns, in both layouts.
13. `Join()(w, w)` with a data column: still refused for the overlap, with today's message.
14. `Rejoin()(d, d)`: the rows of `Rejoin()(d)` (each origin row pairs with itself).
15. Number mode over inputs with no repeated operand: the tag is there, 0 on every row.
16. The occurrence tag name colliding with an input column, a meta or system prefix: refused
    at construction or validation.

Jobs:

17. Plain stream, sync job, async job, `node.run()`, held upstream: equal rows and hashes.
18. A job holding `Join()(d)` and `Join()(d, d)`, recorded in either order: two nodes.
19. Save, load, `run(load_only=True)`, save again: equal rows, equal hashes, equal file;
    `inputs` is `[d, d]`.
20. A function pod over `Join()(d, d)`: unique gives 4 rows and 4 stored entries, sync and
    async; number gives 5 rows, 5 entry ids, one input data hash for the two copies.
21. `GroupBy` and `Batch` over a numbered self-join: member order and folds stable.
22. A caching operator node (`LOG`, `REPLAY`) for `Join()(d, d)`: its cache path differs from
    `Join()(d)`'s.
23. The edge model, once decided: two inputs from one upstream as the model says, live and
    after load.

## 6. Recommendation

1. **Take B2**: one operand group that keeps every operand, hashed in today's form. No
   identity without a repeated operand changes, no golden entry moves, the label stays
   `semantic_v1`, and the pipeline hash of `Join()(d, d)` is the one `Join()(d, e)` has.
   Nothing has to move before the 0.4.0 freeze for this issue.
2. Add the missing golden case (`Join()(d, e)`) before the freeze, so the multiset reading of
   the form is pinned.
3. Build on PR 587: replace the refusal inside `operand_set`, keep its edges and
   `node_inputs`.
4. Ship the toggle in the same change as lifting the refusal. Without it a self-join stores
   one entry id twice on the async path.
5. Keep the toggle out of the identity when it is the default.

## Open questions for the developer

1. **B2 or a visible new form?** B2 keeps the tag `frozenset` for a group that may hold equal
   items. If you want the form to say "multiset" by name, that is B1: 116 entries move (18 in
   `pipeline-02.json`, 98 provisional), and it has to happen before 0.4.0.
2. **Toggle spelling**: `repeats="unique" | "number"` and `occurrence_tag="occurrence"`, or
   other names? Is the tag name the user's to choose?
3. **`Rejoin`**: `Rejoin()(d, d)` has the rows of `Rejoin()(d)` (measured: 3 and 3; each row
   pairs with itself, also by occurrence when a system tag repeats). Under B2 they still get
   two identities, as every commutative pod does. Accept two identities for equal rows (one
   rule for all), or let `Rejoin` keep a true set? Does `Rejoin` take the toggle at all?
4. **When does "unique" act?** Complete repeats were only produced here by a repeated operand
   (the pair sort needs two inputs of one pipeline hash and one source id). Apply the unique
   step only when the operand group holds a repeat (decided from identities, no cost for an
   ordinary join), or on every join?
5. **Default written or left out of the identity?** Left out is recommended (no move).
6. **Edges**: does a repeated commutative input become two edges with an occurrence number
   (for restrictions and probes per edge, ITL-1472), or stay one edge with two readers?
7. **Function pods**: `pod(d, d)` runs the automatic `Join`. Does the pod get the toggle
   (an argument of the call), or does it always use the default and the user writes the
   `Join` by hand for "number"?

## Seen in passing

An async job stores one result row per input row even when two input rows have one input
data hash: a function pod over `d` (3 rows, 2 distinct inputs) stores 2 rows in its result
table on the sync path and 3 on the async path. Measured on PR 587's branch without any
repeated operand. Not part of this issue.

## Appendix: the experimental patch

Not committed. Three edits on `main`, switched by an environment variable:

- a `frozenset` subclass that also keeps `tuple(streams)` as `operands`;
- `Join.argument_symmetry` and the commutative branch of `BinaryOperator.argument_symmetry`
  return it;
- `_expand_structure` expands every member of `operands`, sorts by canonical JSON, and
  writes the form of the chosen candidate (for A1 the alternate form when
  `len(set) != len(operands)`, for A2 when two expanded items are equal).

For section 4 the same hasher edit was applied to PR 587's branch, with `operand_set`
returning the subclass instead of raising.
