# `as_source()` of any stream (ITL-1466): design

Design only; the library is unchanged. **Round 2** (2026-11-25, afternoon): the developer's
answers are in, and a prototype was run on a scratch branch outside this pull request.
Evidence: **[ran]** a script in `itl-1466/` on `0d1e017c`; **[proto]** `probe_round2` or
`probe_tokens` on the prototype (they fail on the library as it is); **[read]** the code;
**[inferred]** none of these. Section 12: what is decided, what is open, and **what the
prototype sends back (P1-P5)**.

## 1. What is decided, and the result in one paragraph

Developer, 2026-11-25: (1) `as_source()` of any stream gives exactly one new system tag pair,
for every pod kind and however many pairs the origin has; a source returns itself. (2) The
source id enters the content hash. (3) A record id names an entry uniquely within its source.
What it takes depends on the source: a position, key columns, or, for a derived source, a hash
of the entry's own content: tags, system tags and data, **no source-info tokens** (tokens hold
random result ids). A function **node** uses the pipeline table's own id.

Proposed: `as_source()` is a method of every stream, and every one that is not a source gives
a `DerivedSource`, in **one form** (today there are two): an ordinary root source with a source
id, one pair and the root identity. `tags_as_data` becomes an option of that form. No golden
value moves [proto]. Rows stored downstream of today's `as_source()` need a new store (section
9): today's rows carry no system tags at all.

## 2. Today

| Fact | Evidence |
|---|---|
| Only `FunctionJobNode` and `OperatorJobNode` have `as_source` | ran, today 1; more 6 |
| The plain form's `keys()` and schema declare the origin's system tags, but its **rows have none**, and their tokens are null | ran, today 5 |
| So a step over it stores **no system-tag columns**, at the path any source of that schema uses | ran, today 6 |
| In one in-memory store, that step and the same step over an ordinary source fail to write. In Delta, plain first then one with tags is accepted; the other order fails | ran, today 6; more 2 |
| This breaks today's rule that no accepted source changes a stored table's columns (ITL-1112) | inferred from the above |
| A multi-pair origin, and any node of a loaded job, is refused as an input | ran, today 2 and 7 |
| An operator node with cache `OFF` (the default) gives zero rows, silently, though the node shows its run | ran, today 3 |
| An explicit `source_id` on the plain form changes no content hash | ran, today 4; more 1 |
| A moved store without `database=` gives zero rows **with** `OrcapodDatabaseContentWarning` (the handoff scripts ran with warnings off) | ran, today 7 |

**Does the source id enter the content hash?** Yes for `ArrowTableSource`, `DictSource`,
`ListSource`, `DataFrameSource`, `CSVSource`, `DeltaTableSource`, `SQLiteTableSource` and
`DerivedSource(tags_as_data=...)` [ran, more 1]; `PostgreSQLTableSource`, `DBTableSource`,
`SpiralDBTableSource` (root rule plus extras) and `PollingSource` (its own structure, with the
id) [read]. `CachedSource` uses the wrapped source's identity, so yes [ran]. `SourceProxy`
returns the saved hash, so it holds whatever the saved source held [read]. A bound
`SourceJobNode` delegates to its source; an unbound slot has no source id and hashes its
declared schema and name [read]. **The plain `DerivedSource` is the only one that does not.**

## 3. Which objects, and what each returns

Rule: a source returns itself; a slot asks its bound stream; a side-effect stream or node asks
the stream feeding it; everything else gives a `DerivedSource` whose origin is that object.

| Object | `as_source()` | Its rows |
|---|---|---|
| Every `RootSource` (`CachedSource`, `PollingSource`, `DerivedSource`, `SourceProxy` too) | itself, the same object | its own |
| `SourceJobNode`, bound | `bound.as_source()`: the bound source. Where it gives `T` for a slot's `T \| None`, its schema is the source's, not the slot's | the bound stream's |
| `SourceNode` or `SourceJobNode`, unbound; the non-job nodes of a `Pipeline` (no rows can exist) | refused: bind first, or use the job's node | none |
| Operator stream, function pod stream, raw `ArrowTableStream` | `DerivedSource` | `as_table()` when read |
| `FunctionJobNode` | `DerivedSource` | stored entries |
| `OperatorJobNode`, `LOG` / `REPLAY` | `DerivedSource` | stored rows of its content hash |
| `OperatorJobNode`, `OFF` | `DerivedSource` | its current run's, as `as_table()` shows; none when it has not run or is stale |
| `SideEffectPodStream`, `SideEffectJobNode` | the `as_source()` of the stream feeding it; delivers nothing | that stream's |
| Node of a loaded job | as its kind; an `OFF` operator is **refused** (it holds no rows and never can) | stored entries |

The general rule for a job node is: **what it stores, else what it shows**. The cache mode is
the node's at the read, never changed by `as_source`. A row has the same record id both ways
(section 4). Rows of an earlier `LOG` time are not read in `OFF`. "Caching on" reads the content
of the cache (D7). A node run in `LOG` loads as `OFF`, so a loaded one is refused until
`LOG` is attached again; then its ids are equal [proto 7] (P5).

**Side effects** (decided: treated "like the stream feeding into the side-effect"). Delivered by
`as_source` and reading it: 0, for a stream and a node [proto 6]. Consequences:

- Over a source, `tell(units).as_source()` **is `units`**, the same object. Over a select it is
  the select's: same source id and record ids [proto 6].
- Rows whose delivery failed (dropped downstream by `drop_on_failure`) **are in it**: a node
  passed on 2 rows and its `as_source` has 3 [proto 6]. Surprising, because a step after the
  node in the same job sees 2.
- It works before the node has run, and its rows do not depend on delivery.
- A step **over** a side effect does not have this identity: `rnd.pod(tell(u))` and
  `rnd.pod(u)` have different content hashes [proto 6].
- A **plain** step over a standalone side-effect stream computes through it: reading its
  `as_source` delivered 3 times (`head` 0) [proto 6]. Not covered by the decision: P4.
- A function pod with `ctx_arg` is a function, not this case: its `as_source` is a function's;
  a plain one runs its body, effects included, when read [read].

**Arguments.** On a source: none returns itself. `source_id=` alone is refused, also when equal
(a source's name is given when it is built; "no changes"). `tags_as_data=` asks for another
source, so it builds the `DerivedSource` it builds today, and still requires a `source_id`.
On a stream: `source_id=` and `tags_as_data=` as today; `record_id_columns=` is new here, for
both. No `label` argument: label the returned source as any other. Passing it here would be a
second way to do that.

A stream stays a source only by this explicit call (ITL-1387): `as_source()` inside a `with`
block cuts the graph there, and its upstreams are not steps of that job.

## 4. The pair

**Column names**: `_tag_source_id::<schema hash>` and `_tag_record_id::<schema hash>`, as every
source's, from the derived source's own schemas. The origin's pairs, however many (or none),
are not carried: `Join`, `GroupBy`, `Batch`, `MergeJoin`, select and function origins all gave
one pair, and a join of two `as_source` results (4 columns) gave one again [proto 1, 8].
**Source id**: `derived_v1:<name>` on every row.

**Record id**: text with its label, `arrow_v1:<hex>`:

| Origin | What is hashed |
|---|---|
| Plain stream (operator, function, raw) and operator node (`OFF`, `LOG`, `REPLAY`) | one row: user tags, system tags, data. No tokens, no node hash, no meta or context columns |
| Function **node** | nothing new: the stored `__pipeline_base_entry_id` (user tags, system tags, labelled input data hash) |
| Source origin (`tags_as_data` only) | nothing: the row's record id in that source |
| `record_id_columns` given | nothing: canonical JSON of the key columns |

**Function node: base entry id or entry id?** *Awaiting the developer*; the recommendation is
the base id, as the coordinator has passed on. `__pipeline_entry_id` adds the recomputation
index, which counts attempts in one store, so a fresh store gives another id [read]. The base
id is equal in two fresh stores [proto 2], and 1.1 corrected to 1.2 (both round to 1) gives
two ids: 4 rows, 4 ids [proto 4]. The output is **not** in it. Consequence: a recomputed entry
keeps its id even when its output changed. Run twice with a new result store, the same id
named 1 and then 101 [proto 4b]. `as_source` shows the latest recomputation per base id (the
node's own rule), so ids stay unique: 6 stored entries, 3 rows [proto 4b]. `get_all_records()`,
read today, has no such rule. Downstream results are keyed by data, so a changed output is
still computed; the id does not show the change.

**Node and plain ids of one function differ** [proto 3]: the node's hold the input and the
plain stream's the output. They share the default source id and the content hash, so this is
a problem (P2). For every operator kind tried, plain, `OFF`, `LOG` and `REPLAY` ids are equal
[proto 2, 3].

**Determinism** [proto 2]: equal in a second fresh store, in another process with another hash
seed, sync and async, with `Join` inputs swapped, with rows and tags reordered under keyed
sources. `Batch` differs on reordered input: it groups by position. Under **positional**
sources a reorder moves every id, because the origin's `row_<n>` is in the system tags.

**Uniqueness** (D6, decided: a repeat raises). System tags are distinct per row below sources
in a job: row-by-row operators pass them, joins pair them, folds digest distinct members
[inferred; ran for the kinds above]. A raw stream with two identical rows raises [proto 3], and
so would a user operator that repeats rows, until repeating operations add an index column
(future; nothing now). **But a cache is not one run.** It can hold one content several times,
with other tokens, and then it raises on ordinary use [proto 4]:

- a select over `rnd`, `LOG`, input corrected 1.1 to 1.2: `rnd` gives the same 1, so the
  cache holds 4 rows and 3 contents: `as_source` raises. `OFF` gives 3 rows;
- nothing changed, but the results were recomputed into a new result store: the cache holds
  every row twice: raises.

`LOG` keeps both because its own row hash includes the tokens. This is P1.

**ITL-1154** (D5, decided): amend "a record id is not a hash". Text: "a record id is the row's
identifier as text: its position, its key's canonical JSON, or, for a row of a derived source,
the hash of the entry as `<label>:<hex>`". It holds no own source id and no `::`. It does
depend on the sources **behind** it: renaming an upstream source moves it [ran].

## 5. What it computes and writes

| Call | Plain stream | Job node |
|---|---|---|
| `as_source()`, schema, ids, hashes | nothing | nothing |
| `head()`, preview | the origin's `head()` | the origin's current `head()`, or stored rows |
| `as_table()`, iteration, `run()`, a job run that reads it | computes the stream **in this process, uncached**, calling its functions, once, and holds the rows | reads only |
| Writes | nothing | nothing |

A `head()` that has rows shows them, with a record id computed from what the row holds; it
never fetches anything extra. Schema, hashes, `head` and the check made 0 calls; two reads made
3, one per row [proto 5]. A head does not check uniqueness over
rows it does not read. A user who wants functions cached uses a job; `as_source` is not a cache.

**Run start check.** It checks the origin's root sources, then compares the origin's modified
time. If it moved, the held rows are dropped and the derived source's time moves; nothing is
computed in the check. For a node origin it re-reads stored rows as today.

**Immutable streams, row positions.** The source object's identity never changes; its rows are
read at most once per origin time, as for `CSVSource`. Emission order is the origin's and is in
no id. A load-only run refuses a plain origin that would compute
(`refuse_unloadable_inputs` already has this rule for bound streams).

**Restrictions.** `where()` is in no identity, so a restricted origin has the same default id,
and the same record ids for the rows it keeps (not for `GroupBy` / `Batch`, whose members
change). The walks stop at `DerivedSource` today because it read only stored rows [read]. Over a
**plain** origin they must go on into it: otherwise one table could be read two ways under one
identity, or a restricted read be cached, unnoticed. A cache over such a source is refused.
Over a node the walk still ends. ITL-998's ranges need nothing else.

## 6. Identity, default id and the collision

- **Content hash**: the root rule `(source_id, schema identity, key columns)`, plus the sorted
  mapping when `tags_as_data` is not `None`. Key columns `()` mean "the row's own id" for this
  kind; a position is no longer possible in it, so `()` is not ambiguous.
- `tags_as_data={}` becomes another spelling of the one form; refuse an empty mapping rather
  than keep two content hashes for one source. Code that uses it as the workaround for a joined
  node drops the argument.
- **Pipeline hash**: schema only, unchanged, equal to the fresh form's [ran, today 4].
- **Default name**: kept, `semantic_v1:<hex of the origin's content hash>`; the same after a
  load [ran]. With `tags_as_data` it stays required (ITL-1313).
- **Slot name** and `bind` key: the source's name. A default is 76 characters of hash: give a
  `source_id` to bind by name. `bind` needs no change: one pair fits `validate`.

**Collision** (D10, decided). A content hash is an address, so two authors with the same
pipeline and source names but other data get one default id [ran, today 8; proto 9]. The rule
for all sources, implemented by ITL-1469: one id and another schema is an error
(`InconsistentSourceError`); one id, one schema and content **known** to differ is an error;
otherwise they are the same source, with no warning.

For `as_source`, recommend: **held rows are not known content.** Whether two authors collide
would then depend on whether somebody read first, and a node's rows change by design (history,
recomputation). Nothing is computed to know. What is certain without computing is the
fixed-content sources **beneath a plain origin**: two plain streams of one id over two
`DictSource`s of one name and other rows are over sources known to differ. ITL-1469's check can
go into a plain origin with the walk the restrictions need (section 5) and refuse there. Node
origins are trusted. ITL-1469 is not built yet, so agreement with it is [inferred].

## 7. Source-info tokens and tracing

Decided (D11): a token is kept where a cell has a single one, and fresh
(`<source id>::<record id>::<column>`) where it is a list; `tags_as_data` copies and null
tokens get a fresh one. Decided by the **type** of the token column (`large_string`: kept),
with one exception that is by value: a null inside a kept column is filled. Real rows
[proto, `probe_tokens`; hashes cut]:

**Case 1, single token: kept.** A select of a source, then a function node:

```
_source_x  before  dict_v1:units::{"s":"m1","u":"a"}::x
           after   dict_v1:units::{"s":"m1","u":"a"}::x
_source_r  before  rnd:0db60db7..:v0:python.function.v1::01a214a8..::r
           after   rnd:0db60db7..:v0:python.function.v1::01a214a8..::r
```

**Case 2, list of tokens (`GroupBy`, `Batch`, colliding `MergeJoin` columns): fresh.**

```
_source_x  before  large_list<large_string>
                   ['dict_v1:units::{"s":"m1","u":"a"}::x', 'dict_v1:units::{"s":"m1","u":"b"}::x']
           after   large_string
                   derived_v1:grouped::arrow_v1:000002ab..::x
```

**Case 3, a `tags_as_data` copy, and a null token: fresh.** `x` beside the copy stays kept.

```
_source_x     before  dict_v1:units::{"s":"m1","u":"a"}::x     after  the same
_source_unit  before  (none)   after  derived_v1:copied::arrow_v1:0000021c..::unit
_source_n     before  null     after  derived_v1:raw::arrow_v1:00000221..::n
```

The list that counts is the token's: a `list[float]` data column with one token is kept. A row
of a `Join` keeps each column's own source, so it names two sources in its tokens.

What kept tokens do, against all fresh [proto, `probe_tokens`]:

| | Kept | Fresh |
|---|---|---|
| Memoization | the same: 0 calls with a second upstream store | the same |
| Stored column names and types | the same (`_source_r`, `large_string`) | the same |
| Content hash, pipeline record ids, base entry ids, record ids of the next `as_source` | the same, equal across stores | the same |
| Stored input token values downstream, over a function node | differ between two fresh stores (random result id) | equal |
| Operator `LOG` row hash downstream (it includes tokens) | differs between two fresh stores | equal |
| A downstream operator's `LOG` cache after the upstream store is rebuilt | 2 rows, then **4**; its `as_source` raises (P1) | 2 rows |
| `trace` of a row computed from the `as_source` | **fails**: `ProvenanceInconsistencyError`, the token does not name "the record its system tags name" | works; stops at the derived source |

The last line is P3. It holds over a function node and over a plain select. The tracing check
is the one that makes a token agree with the pair, and the developer's model says system tags
name why and from what, and tokens the ultimate source. Kept tokens need that check relaxed
for a derived source: the pair leads to the `as_source` row, the token past it.

## 8. History, save, load, moved stores

**History (ITL-1129).** A function node shows every stored entry of its content hash. A
corrected input gives another base entry id, so old and new both appear with different ids; so
do shifted rows of a positional source (2 rows, one inserted first: run 3, `as_source` 5;
keyed: 3) [ran, more 5]. They are not duplicate ids and are not refused: they are different
computations. Superseded rows stay out of scope; document it and recommend key columns. A
plain stream and an `OFF` operator show current rows only; a side effect, what its feeding stream shows.

**Save and load.** A `DerivedSource` over a node or a plain stream is saved as its identity
(name, full id, hashes, schema) without its origin, and loads as an unbound slot, as today:
`unbound_sources` names it, stored downstream rows read back, a load-only run gives zero rows
with no warning [ran, more 3]. Keep it, but such a run must warn naming the unbound slot. To
run again: load the origin job, `as_source()`, `bind`. The content hash is then equal, so it
fits. Saving a reference to the origin's table is later work (a store is not portable text).
With a source origin it reloads as today.

**Moved store.** The load's row-count warning stands [ran]. For `as_source` refuse instead:
a loaded node whose job file records rows for its run and whose store has **no table** raises,
naming the store and `database=`. Fewer rows than recorded still warns.

## 9. What moves

| Item | Moves? | Frozen / provisional |
|---|---|---|
| Golden: frozen files | 0 entries | frozen |
| Golden: `pipeline:default_source_ids.as_source[operator_node]`, `[function_node]` (`.source_id`) | no | provisional |
| Golden: `schema:derived_source_tags_as_data.*` (26), `schema:key_policy_identity.derived_tags_as_data.*` (4) | no: origin is positional, so `row_<n>` is also its row's own id | provisional |
| Content hash of every plain `as_source()`, and every node and stored `__node_content_hash` downstream | **yes** | unpinned, provisional |
| Its pipeline hash; downstream pipeline hashes and stored paths | no | |
| Its system-tag columns | none on the rows (declared: the origin's) to one pair; a schema change of downstream **stored tables at an unchanged path** | unpinned, provisional |
| Its tokens | null to kept or fresh text; input token columns stored downstream change value | unpinned |
| Tokens of `tags_as_data` over a source | fresh to kept, for single-token columns; stored values downstream | not pinned by any golden case; 3 tests |
| Downstream entry ids, operator row hashes, join / fold system-tag values | **yes** | unpinned |
| Content hash of the explicit `tags_as_data` form | no | provisional, pinned |
| Its record ids over a node, or over a keyed source without `record_id_columns` | `row_<n>` to the row's own id, **under an unchanged content hash** | not pinned by any case |
| Function result tables, function uris, layout markers, saved job format | no | frozen |
| `ITL-1154` rule 8 and the hash-site inventory text | amended | docs |

[ran, `golden_moves`: root identity on the plain form 0 of the checked entries move; a
separate key policy marker 2; kind version 2, 14. The torch group was not run; it has no
`DerivedSource` case.] **On the prototype: 2577 golden tests pass, torch
included, 0 values move.** Of the existing tests of sources, pipelines and order invariance
selected by name (derived source, `as_source`, proxy, side effect), 496 pass and 5 fail, all in
`test_derived_source_tags_as_data.py`: 3 pin fresh tokens on a `tags_as_data` source over a
**source**, which D11 now keeps; 1 pins the refusal of `record_id_columns` without
`tags_as_data`; 1 the old plain identity. The full suite was not run.

**The kind version.** ITL-1425 says a changed record id needs `SOURCE_VERSION` + 1
(`derived_v2`, 14 provisional entries). No final release carries `derived_v1` and the class is
provisional, so recommend staying at 1 if this lands **before** 0.4.0, as the layout markers
did (ITL-1259); after 0.4.0 it must be 2.

**The Nov 15 conditions on ITL-1127** (D13, decided): "change only multi-pair origins" is
dropped; "keep the id" holds. The cost for a candidate's stores:

- functions are **not** called again: results are keyed by input data (0 calls) [ran, today 6];
- pipeline entries, operator caches and side-effect records are written again under new ids, so
  a tracked side-effect pod would **deliver again** for rows already delivered [inferred];
- new rows do not fit an old table. In memory: refused, loudly. In Delta: accepted when old
  rows are first, mixing both forms, and refused the other way round [ran]. D14 (decided): **no
  preflight**; nothing has used the feature. Document "use a new store".

**Overlap.** ITL-1464: #566 changes how a node holds rows and adds paged reads of the current
run; `as_source` of an `OFF` operator must read through that and not copy the table. #567 adds
row-id-free certificates: a derived source certifies only its `record_id_columns` (checked, all
tags) and nothing for a row's own id, which is not a tag; it must not declare
`KEY_COLUMNS_CHECKED` otherwise. #567 edits `derived_source.py` (carried tokens) and
`stream_builder.py`; #563 makes tokens recipes, so the operator row hash must read token
**text**. Land after that stack. ITL-998: the restriction walk only (section 5).

## 10. Tests

- Every object of section 3, by its public call: itself, the pair, or the refusal; a coverage
  test lists stream and node classes, and fails on a new one without a case.
- Origins with two or more tags **and** data columns; tags plain and data-tags; several
  orders of tags, columns, rows, join inputs; keyed and positional sources; each on sync and
  async, and async `as_source` as an input. `as_source` joins `test_order_invariance.py`.
- Record ids: plain operator equals stored; second process and store; sync equals async; `OFF`
  equals `LOG` equals `REPLAY`; corrected input (1.1, 1.2); recomputed entry keeps its id; `Batch` / `GroupBy` / `MergeJoin` /
  `Rejoin`; several pairs, none; `as_source` of `as_source`'s results; repeated rows refused.
- Never computes: call counters on schema, hash, `head`, preview, check; no delivery; a store
  that refuses writes.
- Identity: each source kind with two ids; `source_id` given, default, through a load.
- Bound: multi-pair and loaded origins accepted; stored columns equal an ordinary source's
  of that schema, both orders, in-memory and Delta.
- Collision: ITL-1469's cases with `as_source` sources; two loads of one job are one source.
- Tokens: the three cases; a null; a list column with one token; `trace` across the boundary.
- Restrictions beneath a plain origin: two restrictions, a cache, `LOG`.
- Save, load, rebind, load-only warning, moved store. History: corrected, shifted.
- New golden cases (provisional) for the plain form's content hash, pair and record ids over
  function, operator and raw origins. They are additions.

## 11. Documents and stages

Documents: `concepts/sources.md` (`DerivedSource`, one form; the provisional table),
`pipeline-job.md`, `function-pods.md`, `troubleshooting.md`, tutorial Part 8 (the "Not yet"
limit), `reference/hashing.md` (rule 8, sites 4 and 5, inventory), `CLAUDE.md` and
`.zed/rules`, the design log.

| Stage | Content |
|---|---|
| 1 | One-form `DerivedSource` for function and operator job nodes: pair, record ids, identity, tokens, uniqueness, collision (ITL-1469). Fixes joined and loaded origins |
| 2 | The method on sources, slots, side-effect streams and nodes, `OFF` operators; arguments; loaded and moved stores; load-only warning |
| 3 | Plain streams: held rows, checks, restriction walks, refusals |
| 4 | Golden additions, documents |
| Later | tracing across the boundary; saved origin reference; superseded rows (ITL-1129); ITL-1127's automatic wrapper of a raw leaf (this gives it its wrapper; automatic use stays a separate decision) |

## 12. Decisions

**What the prototype sends back to the developer**

| # | Question | Recommendation |
|---|---|---|
| P1 | A `LOG` cache holds one content several times (corrected input with an equal output; results recomputed), and D6 makes `as_source` raise [proto 4, 4b] | Rows equal in tags, system tags and data are one entry: show it once, from a cache only. With kept tokens the copies differ in tokens and the cache records no order, so nothing says which token to keep; with fresh tokens there is no question. Raising stays for one run's rows |
| P2 | One function's node and plain stream share the default source id and content hash but give other record ids [proto 3]. Downstream of both in one store, every row is stored twice under one node content hash [inferred] | Let a plain function stream use the base entry id too: it has the input row while it computes. Otherwise give its default id another name |
| P3 | Kept tokens make `trace` fail for every row computed from an `as_source` [proto, `probe_tokens`] | Either relax that check for derived sources, as part of this work, or all tokens fresh (then P1 is also settled). I lean to fresh; if kept, D11 includes the tracing change |
| P4 | A plain step over a standalone side-effect stream delivers when its `as_source` is read [proto 6] | Refuse it, naming a job as the remedy: looking through would give rows under an identity that says the effect ran |
| P5 | A `LOG` node loads as `OFF`, so `as_source` of a loaded operator is refused though its rows are stored [proto 7] | Save the cache mode with the node (its own issue); until then the refusal says to attach `LOG` again |
| | Base or entry id for a function node | Base (section 4); awaiting the developer |
| | Held plain-stream rows as "known content" (D10) | No; the fixed-content sources beneath a plain origin, yes (section 6) |

**Decided 2026-11-25 (round 2)**

| # | Decision |
|---|---|
| D4, D5 | Record id: hash of tags, system tags, data, no tokens; a function node, the pipeline table's own id. ITL-1154's wording is amended |
| D6 | Repeated ids raise; an operation that repeats rows will add an index column later |
| D7 | `OFF`: the current run's rows. Caching on: the content of the cache |
| D8 | Plain streams stream their content, computed when read. Side effects: the feeding stream's `as_source`, delivering nothing, never refused |
| D10 | ITL-1469's rule; no warning |
| D11 | Single tokens kept, lists fresh, copies and nulls fresh; by the column's type (but P3) |
| D13 | The "only multi-pair origins" condition is dropped |
| D14 | No preflight refusal |

**Open, each with its recommendation**

| # | Question | Recommendation |
|---|---|---|
| D1 | One form of `DerivedSource`? | Yes: the root rule always; `tags_as_data` an option; refuse an empty mapping |
| D2 | Which objects? | Section 3. A slot gives its bound source. Refuse unbound slots and non-job nodes |
| D3 | Arguments on a source? | None: itself. `source_id` alone: refused. `tags_as_data`: as today. No `label` |
| D9 | Default id | Keep the origin's content hash |
| D12 | History | Every stored entry, the latest recomputation of each; ITL-1129 later |
| D15 | Kind version | `derived_v1` if before 0.4.0 (0 golden moves), else `derived_v2` (14 provisional) |
| D16 | Save, load, moved store | Loads unbound and rebinds; warn on a load-only run over an unbound slot; raise when the recorded table is absent; restriction walks enter a plain origin |

Found in passing, to be filed: ordinary sources given one name and other rows in one job are
silently one (more 4); the stored-schema conflict of section 2; the loaded load-only run that
is silently empty (more 3). Not examined: `SourceProxy` of a Postgres or Spiral source,
ephemeral results, `REPLAY` by running it, `PollingSource` under async.
