# `as_source()` of any stream (ITL-1466): design

Design only; no code. Evidence: **[ran]** a script in `itl-1466/` on `0d1e017c` (output beside
it), **[read]** the code there, **[inferred]** neither. Section 12 lists the decisions (D1-D16).

## 1. What is decided, and the result in one paragraph

Developer, 2026-11-25: (1) any stream taken as a source gives one new system tag pair that
names the stream and its rows; a source returns itself unchanged. (2) The source id enters the
content hash. (3) The record id comes from the row's own id, which each operator's cache defines.

Proposed: `as_source()` is a method of every stream, and every one that is not a source gives
a `DerivedSource`, in **one form** (today there are two): an ordinary root source with a source
id, one pair, fresh tokens and the root identity. Its record id is the key the row has, or
would have, in its node's table. `tags_as_data` becomes an option of that form.
No golden value moves. Rows stored downstream of today's `as_source()` must be recreated in a
new store (section 9): today's rows carry no system tags at all.

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

Rule: a source returns itself; a slot asks its bound stream; everything else gives a
`DerivedSource` whose origin is that object.

| Object | `as_source()` | Its rows |
|---|---|---|
| Every `RootSource` (`CachedSource`, `PollingSource`, `DerivedSource`, `SourceProxy` too) | itself, the same object | its own |
| `SourceJobNode`, bound | `bound.as_source()`: the bound source. Where it gives `T` for a slot's `T \| None`, its schema is the source's, not the slot's | the bound stream's |
| `SourceNode` or `SourceJobNode`, unbound; the non-job nodes of a `Pipeline` (no rows can exist) | refused: bind first, or use the job's node | none |
| Operator stream, function pod stream, raw `ArrowTableStream` | `DerivedSource` | `as_table()` when read |
| `FunctionJobNode` | `DerivedSource` | stored entries |
| `OperatorJobNode`, `LOG` / `REPLAY` | `DerivedSource` | stored rows of its content hash |
| `OperatorJobNode`, `OFF` | `DerivedSource` | its current run's, as `as_table()` shows; none when it has not run or is stale |
| `SideEffectJobNode` | `DerivedSource` | its current run's passed-on rows; never delivers |
| `SideEffectPodStream` (standalone), or a plain stream with one beneath it | refused: each read delivers. Use a job | none |
| Node of a loaded job | as its kind; an `OFF` operator and a side-effect node are **refused** (they hold no rows and never can) | stored entries |

The general rule for a job node is: **what it stores, else what it shows**. The cache mode is
the node's at the read, never changed by `as_source`. A row has the same record id both ways
(section 4). Rows of an earlier `LOG` time are not read in `OFF`.

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
source's, from the derived source's own tag and data schemas. The origin's pairs, however many
(or none), are not carried. **Source id**: `derived_v1:<name>` on every row.

**Record id**: the row's own id as labelled text, `arrow_v1:<hex>`:

| Origin | The row's own id |
|---|---|
| Function pod stream or node | `__pipeline_base_entry_id`: `hash_table` of user tags, system tags and labelled input data hash |
| Operator stream or node | `_record_hash`: `hash_table` of the whole row (tags, data, source info, system tags) and the node's labelled content hash |
| Side-effect job node, raw stream | the function rule on the row itself (user tags, system tags, data hash): no operator made the row |
| Source origin (`tags_as_data` only) | the row's record id in that source |
| `record_id_columns` given | canonical JSON of the key columns, as today |

Not the result table's `__data_id`: a random `uuid7`, and rows with equal input data share it
[read]. Not `__pipeline_entry_id`: it adds the recomputation index, so the same row gets
another id after a recompute [read]. `CLAUDE.md` says entry ids hash the system tags; they
hash the user tags as well [ran, `row_ids`: two rows of equal data differ by a tag].

**Plain equals stored.** The recipe on a plain `Join`, `GroupBy`, `Batch` and function stream
gives exactly the ids stored by a job over the same sources [ran, `row_ids`]. So an operator's
or function's record id is one definition used by its cache and by `as_source`: make it one
function per kind (`row_ids(stream)`), called by the node's write too, with no second recipe.
A function pod stream needs its input rows for it (after the automatic select) [ran].

**Determinism** [ran, `row_ids`]: equal in a second run, in another process with another hash
seed, sync and async, with `Join`'s inputs swapped, and with rows and tags reordered under
keyed sources, for function, `Join`, `GroupBy`. `Batch` differs when its input is reordered:
it groups by position, which is its meaning. Under **positional** sources a reorder moves every
id, because the origin's `row_<n>` enters it. `as_source` adds no position of its own.

**Uniqueness.** In a job each source's record ids are unique, and by induction each shipped
operator keeps distinct system tags on distinct rows (row-by-row and filtering operators pass
them; joins pair them; folds digest distinct members) [inferred; ran for `Join`, `GroupBy`,
`Batch`]. The entry id and row hash contain the system tags, so they are unique there. This
fails for a raw stream (two identical rows; a function over two equal tags and inputs)
[ran], a user operator that repeats rows, and a `Rejoin` of repeats. So it is **checked when
rows are read and a repeated id is refused**, naming the first row's tags. Never number repeats
(that is a position) and never drop one.

**Duplicates in a store.** A base entry can have several recomputation entries. The node's own
read keeps the greatest index; `get_all_records()`, which `as_source` uses, keeps all [read].
`as_source` takes the node's rule, so one entry is one row. `LOG` already drops exact duplicate
rows when it stores (`skip_duplicates`) [read]; a plain stream refuses them. They cannot occur
below sources in a job, by the argument above.

**Where this meets ITL-1154.** That rule says a record id "is not a hash and carries no label".
Decision 3 makes it one for derived sources. They conflict as written, so the rule must be
amended, not read around. Precedent: `Batch` and `GroupBy` put `batch_v1:<hex>` in the same
column, and rule 5 says a hash stored as a value carries its label. Recommended text: "a record
id is the row's identifier as text: position, key JSON, or, for a row of a derived source, its
stored id as `<label>:<hex>`". The rule's purposes hold: readable kind, no `::`, no random id,
and it does not hold its **own** source id. It does depend on the ids of the sources **behind**
it: renaming an upstream source moves it [ran].

Alternatives: (a) canonical JSON of the origin's system tags: readable, but it omits the input
data, so corrected rows repeat an id and a stored history could not be read, and it holds
source ids in clear and grows with each join; (b) a hash of the system tags only: the same
repeats; (c) positions (today's `tags_as_data={}`): shift with the store's order; (d) the JSON
form of one key column holding the hash: the same content in a longer, less honest form.

## 5. What it computes and writes

| Call | Plain stream | Job node |
|---|---|---|
| `as_source()`, schema, ids, hashes | nothing | nothing |
| `head()`, preview | the origin's `head()` | the origin's current `head()`, or stored rows |
| `as_table()`, iteration, `run()`, a job run that reads it | computes the stream **in this process, uncached**, calling its functions, once, and holds the rows | reads only |
| Writes | nothing | nothing |

A `head()` that has rows shows them, with a record id computed from what the row holds; it
never fetches anything extra. Where a row id needs an input row the head lacks (a function
stream's), the head reports no rows (`not_computed`). A head does not check uniqueness over
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

**Collision.** A content hash is an address, so two authors with the same pipeline and source
names but other data get one default id. Handing both to one job **silently uses one of them
for both steps**, also with two explicit ids on today's plain form [ran, today 8, more 4]. Two
ordinary `DictSource`s given one name by the user do the same [ran, more 4]; there the user
chose the name. In one store, downstream rows share a content hash, so the stored view shows
both authors' (today 8).

| Option | Cost |
|---|---|
| **A. Keep the default; refuse two derived sources with one id and different origins in one job** | one check at capture and bind |
| B. Always require `source_id` (as ITL-1313) | `as_source()` with no argument ends; two authors can still choose one name |
| C. Put the store's address in the id | the id moves with the store, and publishes a path |
| D. Put a digest of the rows in | the id moves with each correction; needs a read |
| E. Put the job name in | does not separate the case found (same job name) |

Recommend A. "Same origin" is certain when it is the same object, or node origins whose
(database address, table path, content hash) are equal; anything else is refused with "give
each a `source_id`". Two loads of one job are then one origin. Two stores across two runs
remain an address collision; document it. Entry ids limit the damage: rows whose inputs differ
have different record ids, equal rows the same.

## 7. Source-info tokens and tracing

Fresh: `<source id>::<record id>::<column>`, as for every root. The handoff (recommendation 4)
and the ITL-1421 study (its D3) would carry the origin's stored output tokens. Recommend
against: the row would name one source in its system tags and another in its tokens, a plain
stream's tokens are lists after `Batch`, and it breaks the token rule for one kind. Nothing is
lost: the record id **is** the stored row's key, so the origin row, its stored tokens and its
lineage are one lookup away. `trace` stops at the boundary and reports the source id and record
id; following it (same process, or a saved origin reference) is later work.

## 8. History, save, load, moved stores

**History (ITL-1129).** A function node shows every stored entry of its content hash. A
corrected input gives another base entry id, so old and new both appear with different ids; so
do shifted rows of a positional source (2 rows, one inserted first: run 3, `as_source` 5;
keyed: 3) [ran, more 5]. They are not duplicate ids and are not refused: they are different
computations. Superseded rows stay out of scope; document it and recommend key columns. A
plain stream, an `OFF` operator and a side-effect node show current rows only.

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
| Its tokens | null to text; input token columns stored downstream change value | unpinned |
| Downstream entry ids, operator row hashes, join / fold system-tag values | **yes** | unpinned |
| Content hash of the explicit `tags_as_data` form | no | provisional, pinned |
| Its record ids over a node, or over a keyed source without `record_id_columns` | `row_<n>` to the row's own id, **under an unchanged content hash** | not pinned by any case |
| Function result tables, function uris, layout markers, saved job format | no | frozen |
| `ITL-1154` rule 8 and the hash-site inventory text | amended | docs |

[ran, `golden_moves`: root identity on the plain form 0 of the checked entries move; a
separate key policy marker 2; kind version 2, 14. The torch group was not run; it has no
`DerivedSource` case.] Record ids of the new form were not prototyped in the library, so "26
and 4 do not move" is read for them.

**The kind version.** ITL-1425 says a changed record id needs `SOURCE_VERSION` + 1
(`derived_v2`, 14 provisional entries). No final release carries `derived_v1` and the class is
provisional, so recommend staying at 1 if this lands **before** 0.4.0, as the layout markers
did (ITL-1259); after 0.4.0 it must be 2.

**The Nov 15 conditions on ITL-1127.** "Keep the id": still holds. "Change only multi-pair
origins": cannot hold with decision 1, and its premise was that single-pair origins bind and
store rows with their own pair. Their rows have no pair [ran], so they are the broken case as
well. ITL-1466 asks for the developer's confirmation here (D13). The cost of breaking it, for a
candidate's stores:

- functions are **not** called again: results are keyed by input data (0 calls) [ran, today 6];
- pipeline entries, operator caches and side-effect records are written again under new ids. A
  tracked side-effect pod would therefore **deliver again** for rows already delivered
  [inferred];
- the new rows do not fit the old table. In memory: refused, loudly. In Delta: accepted when
  old rows are first, mixing both forms, and refused the other way round [ran]. This must not be
  left to the store: before writing, a node whose stored table has no system-tag columns and
  whose rows do refuses (one preflight check), and says to use a new store or delete that table.
  No path change and no migration (no users; provisional).

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
- Record ids: plain equals stored; second process; sync equals async; `OFF` equals `LOG`
  equals `REPLAY`; recomputed entry keeps its id; `Batch` / `GroupBy` / `MergeJoin` /
  `Rejoin`; several pairs, none; `as_source` of `as_source`'s results; repeated rows refused.
- Never computes: call counters on schema, hash, `head`, preview, check; no delivery; a store
  that refuses writes.
- Identity: each source kind with two ids; `source_id` given, default, through a load.
- Bound: multi-pair and loaded origins accepted; stored columns equal an ordinary source's
  of that schema, both orders, in-memory and Delta; the preflight refusal.
- Collision: two authors, default and explicit ids, capture, `bind`, two loads of one job.
- Restrictions beneath a plain origin: two restrictions, a cache, `LOG`.
- Save, load, rebind, load-only warning, moved store. History: corrected, shifted.
- New golden cases (provisional) for the plain form's content hash, pair and record ids over
  function, operator and side-effect origins. They are additions.

## 11. Documents and stages

Documents: `concepts/sources.md` (`DerivedSource`, one form; the provisional table),
`pipeline-job.md`, `function-pods.md`, `troubleshooting.md`, tutorial Part 8 (the "Not yet"
limit), `reference/hashing.md` (rule 8, sites 4 and 5, inventory), `CLAUDE.md` and
`.zed/rules`, the design log.

| Stage | Content |
|---|---|
| 1 | One-form `DerivedSource` for function and operator job nodes: pair, record ids, identity, tokens, uniqueness, preflight, collision check. Fixes joined and loaded origins |
| 2 | The method on sources, slots, side-effect nodes and `OFF` operators; arguments; loaded and moved stores; load-only warning |
| 3 | Plain streams: held rows, checks, restriction walks, refusals |
| 4 | Golden additions, documents |
| Later | tracing across the boundary; saved origin reference; superseded rows (ITL-1129); ITL-1127's automatic wrapper of a raw leaf (this gives it its wrapper; automatic use stays a separate decision) |

## 12. Decisions

| # | Question | Recommendation |
|---|---|---|
| D1 | One form of `DerivedSource`? | Yes: the root rule always; `tags_as_data` an option; refuse an empty mapping |
| D2 | Which objects? | Section 3. A slot gives its bound source. Refuse unbound slots and non-job nodes |
| D3 | Arguments on a source? | None: itself. `source_id` alone: refused. `tags_as_data`: as today. No `label` |
| D4 | Record id | The row's own stored id, `arrow_v1:<hex>`: base entry id / operator row hash; one recipe shared with the cache |
| D5 | The ITL-1154 rule | Amend rule 8 for derived sources; do not disguise the hash as a key |
| D6 | Repeated ids | Refuse at read; never number or drop. Recomputed entries: the node's own latest-entry rule |
| D7 | Job nodes that store nothing (`OFF`, side effect) | Their current run's rows; refused on a loaded job |
| D8 | Plain streams compute when read, uncached, once? Delivering streams? | Yes; refuse a standalone side-effect stream beneath it. `head` and checks never compute |
| D9 | Default id | Keep the origin's content hash |
| D10 | Two authors' collision | A: refuse one id with different origins in one job; document the address rule |
| D11 | Tokens | Fresh; the record id gives the lookup. Differs from the handoff and ITL-1421's D3 |
| D12 | History | Every stored entry, each with its own id; ITL-1129 later |
| D13 | The Nov 15 condition "only multi-pair origins" | Drop it; keep the id. Functions are not recomputed; the rows are stored again; side effects may deliver again |
| D14 | Old downstream tables at the unchanged path | Refuse before writing, naming the remedy; no path change |
| D15 | Kind version | `derived_v1` if before 0.4.0 (0 golden moves), else `derived_v2` (14 provisional) |
| D16 | Save, load, moved store | Loads unbound and rebinds; warn on a load-only run over an unbound slot; raise when the recorded table is absent; restriction walks enter a plain origin |

Found in passing, to be filed: ordinary sources given one name and other rows in one job are
silently one (more 4); the stored-schema conflict of section 2; the loaded load-only run that
is silently empty (more 3). Not examined: `SourceProxy` of a Postgres or Spiral source,
ephemeral results, `REPLAY` by running it, `PollingSource` under async.
