# `as_source()` of any stream (ITL-1466): design

Design only; the library is unchanged. **Round 2** (2026-11-25, afternoon): the developer's
answers are in, and a prototype was run on a scratch branch outside this pull request.
**Round 3** (same day): the function entry, P2 and D1 are decided; the side-effect rule is
amended (section 3); section 4 gives the reasoning for system tags in a function entry.
**Round 4** (same day): a stream's or node's behaviour as a source follows its nature: side-effect
modes (section 3), a derived source over a plain origin reads it as the origin reads (section
5), and the trace boundary (section 7). Round 4 is design only: the prototype is not changed.
**Round 5**: P3 is decided as recommended, the trace boundary; kept tokens stand.
**Round 6**: the store is a complete record, and a derived source's record id is the store's
row key; **section 13 supersedes the earlier sections where they differ** (the function
record id of section 4, P1, P2, D4/D5, D12).
**Round 7** (evening): three nested ids, entry, input and record; **section 14 supersedes
section 13 where they differ** (the passage id is the instance id, computed through the entry id;
the entry id drops the fixed recomputation index column, so `tdb_v2`).
Evidence: **[ran]** a script in `itl-1466/` on `0d1e017c`; **[proto]** `probe_round2` or
`probe_tokens` on the prototype (they fail on the library as it is); **[read]** the code;
**[inferred]** none of these. Round 3's own script is `probe_entries` [proto]. Section 12: what is decided, what is open, and **what the
prototype sends back (P1-P5)**.

## 1. What is decided, and the result in one paragraph

Developer, 2026-11-25: (1) `as_source()` of any stream gives exactly one new system tag pair,
for every pod kind and however many pairs the origin has; a source returns itself. (2) The
source id enters the content hash. (3) A record id names an entry uniquely within its source.
What it takes depends on the source: a position, key columns, or, for a derived source, a hash
of the entry's own content: tags, system tags and data, **no source-info tokens** (tokens hold
random result ids). A function stream or node uses the pipeline table's own id (round 3).

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

**Side effects** (decided, rounds 3 and 4). `as_source` is allowed for every side-effect
stream and node. Its rows are the feeding stream's rows **that passed through** the side effect:
a row whose side effect failed and was dropped is left out. Which rows flow changes; a row's
identity does not: a row that passed keeps the record id it has in the feeding stream (2 of 3
kept their entry ids [proto, `probe_entries` 3]). When it fires follows the kind:

| Kind | Fires | `as_source` reads | Today |
|---|---|---|---|
| Node | once ever per unique input row (persistent invocation log) | the rows its run passed on | exists: a second run calls only the row that failed (1 call, 2 passed) [proto, `probe_entries` 3] |
| Plain stream, **non-caching** | on every iteration: it reads its input again | reads through: each read of the `as_source` fires again | exists, and is today's only plain mode: 3 calls, then 6 [proto, `probe_entries` 3] |
| Plain stream, **caching** | once per input row, the first time the row passes; the rows that passed are held | the held rows; a later read fires nothing | **new**: no such mode exists [read: `SideEffectPodStream` keeps no rows] |

Points to settle with the new mode [inferred]:

- **A failed row in the caching mode** did not pass, so it is not held. Recommend that the next
  iteration tries it again, as a node's next run and a function stream's next read do.
- **`drop_on_failure=False`**: the failed row is passed on (3 of 3 rows [proto, `probe_entries`
  3]). It passed through, so it is in the `as_source`, although its side effect failed. That is
  what the setting says; document it.
- **Load-only run** of a node: it passes every row on and delivers nothing (3 rows, 0 calls
  [proto, `probe_entries` 3]). Those rows passed through without firing, so by the rule they are
  in the `as_source`, failed ones included. Recommend instead: after a load-only run a node's
  `as_source` gives the rows its invocation log records as delivered, which is what "once ever"
  stores; a node with `track_completion=False` keeps no such record and refuses there.
- **Before a run** in this process a node holds nothing, so a fresh or loaded job gives zero rows
  [read]; the same log answers there, by the same recommendation.

**Own source id?** Recommend **yes**, the default from the side-effect stream's own content
hash, as every other origin. Its content and pipeline hashes already differ from the feeding
stream's [proto]. If it had the feeding stream's id, one id would name two contents (3 rows
and 2), and two such sources in one job would be one source under D10 with no way to tell
them apart. The record ids stay the feeding stream's either way.

- A function pod with `ctx_arg` is a function, not this case: its `as_source` is a function's;
  a plain one runs its body, effects included, when read [read].
- A plain step **over** a side-effect stream follows the same rule: over a non-caching one,
  reading its `as_source` fires the effects (3 of them, `head` none) [proto 6]; over a caching one,
  once. P4 is settled by the modes.
- The prototype still gives the feeding stream's `as_source`, failed rows included (3 rows
  against 2 passed) [proto 6].

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
| Plain operator stream, raw stream, operator node (`OFF`, `LOG`, `REPLAY`) | one row: user tags, system tags, data. No tokens, no node hash, no meta or context columns |
| Function stream **and** function node | *(superseded, section 13: the store key)* the pipeline table's base entry id: user tags, system tags and labelled input data hash of the **input** row; stored as `__pipeline_base_entry_id` |
| Source origin (`tags_as_data` only) | nothing: the row's record id in that source |
| `record_id_columns` given | nothing: canonical JSON of the key columns |

**A function entry** (decided, round 3): an entry of a function stream or node is one upstream
entry under this function. Its id is the base entry id, **the same for node and plain stream**.
The answer, the result record and the method (local, Ray) are not part of it. Ran
[proto, `probe_entries` 1]: plain equals node; also when the plain stream was read, fully or for
one row, before `as_source`; with rows and tags reordered (keyed); async; another process;
over `GroupBy`. With another answer (the function changed to add 100: 102, 104, 104), the ids
are unchanged, for node and plain. With `LocalPythonFunctionExecutor`, unchanged. Ray hung
starting a local cluster here and was stopped; it is read only: the id is computed before the
executor is called.

Consequences:

- The entry id is not the recomputation entry id: that one adds the index, which counts
  attempts in one store [read]. A recomputed entry keeps its id even when its output changed
  (1, then 101 [proto 4b]). `as_source` shows the latest recomputation per base id, so ids stay
  unique (6 stored entries, 3 rows [proto 4b]).
- 1.1 corrected to 1.2, both rounding to 1, gives two entries [proto 4].
- The plain stream needs its **input** rows. In the prototype it keeps each input row it reads
  beside its output (`FunctionPodStream`); a design keeps only the entry id. A `head()` of a
  plain stream shows only rows whose input it holds: rows the stream already computed. With none
  it reports not computed, and calls nothing [proto 5: 1 of 3 held gives 1 row, 0 calls].
- A step built **before** a job and captured into it becomes a slot. The slot renames the
  system-tag columns by its own schema hash, so a function node over it and the plain stream
  have the same content hash, but other column names, so other ids [proto, `probe_entries` 1].
  Built inside the job, they are equal. This is today's slot rule, not `as_source`'s; the same
  content hash for other stored columns is to be filed.

**Why the system tags are in a function entry** [proto, `probe_entries` 2]. A function entry
stands for one upstream entry, and an upstream entry is named by its system tags. Without them,
the entry is (user tags, input). Each case: two input rows with one user tag and one input.

| Case | Entries with system tags | Without | Calls | Result records |
|---|---|---|---|---|
| Repeated tag (one tag, two source rows) | 2 | 1 | 1 | 1 |
| Dropped tag (`u` dropped: `a` and `b` become one tag) | 2 | 1 | 1 | 1 |
| Join, one left row matching two right rows (`dur` not an input) | 2 | 1 | 1 | 1 |
| The same, inputs swapped | 2 | 1 | 1 | 1 |
| Control: `u` kept | 2 | 2 | 1 | 1 |

With the system tags there are two entries, one for each upstream entry, and both are shown.
Without them, two upstream entries become one: a row would vanish from `as_source`, a source
id plus record id could not lead back to one upstream entry, and the stream would hold two rows
of one id, which D6 refuses. The function is called **once** and **one result record** is shared
in every case: results are keyed by the input alone. So the system tags cost no computation;
they only keep the entries apart. The upstream's own system tags differ in each case because the
upstream rows are distinct (by key, by position, by the join's pairing), which is the
uniqueness argument below.

For every operator kind tried, and for functions, plain, `OFF`, `LOG` and `REPLAY` ids are equal
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
| `as_table()`, iteration, `run()`, a job run that reads it | reads the origin **as the origin reads** (below) | reads only |
| Writes | nothing | nothing |

**A derived source over a plain origin follows the origin's mode** (decided, round 4). It does not
hold rows of its own: it holds what the origin holds and reads through what the origin reads
through. Over an origin that holds its rows (a function stream, an operator stream, a caching
side effect) a second read computes and fires nothing; over one that does not (a non-caching
side effect, directly or anywhere beneath) each read reads the origin again, with its effects.
The prototype holds rows itself (3 calls for two reads [proto 5]), which equals this only
because its origins hold their rows; it is to change [read]. Record ids are unaffected: a
function stream's entry id comes from the input row, which it keeps with its output while it
holds it.

A `head()` that has rows shows them, with a record id computed from what the row holds; it
never fetches anything extra. Schema, hashes, `head` and the check made 0 calls; two reads made
3, one per row [proto 5]. A head does not check uniqueness over
rows it does not read. `as_source` is never a cache of its own; a user who wants results kept
uses a job.

**Run start check.** It checks the origin's root sources, then compares the origin's modified
time. If it moved, the derived source's time moves; nothing is computed in the check. For a node origin it re-reads stored rows as today.

**Immutable streams, row positions.** The source object's identity never changes. How often its
rows are read is the origin's matter (above). Emission order is the origin's and is in
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
- `tags_as_data={}` is refused (decided, D1): `None` means no copies [proto 5]. Code that uses it as the workaround for a joined
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

**The trace boundary** (P3, decided in round 5; kept tokens, D11, stand). A derived source is a **step** of the trace, as a
function or an operator is. `trace` reaches it through a row's pair and reports it: the source
id, the record id, and the kind of its origin. It then **may continue into the origin's job
through the record id**, which names one entry there: the base entry id for a function node or
stream, the row's content hash for an operator, the row's own record id for a source origin.
It continues when the origin is at hand (the same process, or a job the user gives to the
trace); otherwise it stops at the step, saying where it would continue. The check that a token
names the record its system tags name then applies **within** a job, never across the
boundary: past the derived source the kept token names the ultimate source, and the step
explains why it is not the pair's. This keeps tokens kept (D11) and makes the failure of
`probe_tokens` disappear without a special case in the tokens. Fresh tokens would also make it
disappear, but they would lose the ultimate source from the row.

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
`DerivedSource` case.] **On the prototype (round 3): 2577 golden tests pass, torch
included, 0 values move.** Of the existing tests of sources, pipelines, function pods, side-effect
pods and order invariance selected by name, 2336 pass and 6 fail, all in
`test_derived_source_tags_as_data.py`: 3 pin fresh tokens on a `tags_as_data` source over a
**source**, which D11 now keeps; 1 pins `tags_as_data={}`, now refused (D1); 1 the refusal of
`record_id_columns` without `tags_as_data`; 1 the old plain identity. The full suite was not run.

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

**What the prototype sends back to the developer** (P2 decided in round 3, P4 in round 4, P3 in round 5)

| # | Question | Recommendation |
|---|---|---|
| P1 | *Closed in round 6 (section 13): everything stored is served; one row per content is a restriction mode.* A `LOG` cache holds one content several times (corrected input with an equal output; results recomputed), and D6 makes `as_source` raise [proto 4, 4b] | **With the developer.** The coordinator recommends (b): collapse rows equal in tags, system tags and data to one, keeping the latest passage's tokens; distinct upstream entries are never collapsed, because their system tags differ. Note: an operator cache row records no time today (its columns are the row, its row hash, the node hash and the data context [read]), so "latest" needs a recorded time added to the operator cache layout, or a rule that does not need one. The other option, (a): keep both passages, told apart by the upstream function's result id; that id is random per store, so ids then differ between two fresh stores |
| S1 | Side effects, the rest: the new caching mode; a failed row in it; load-only and before a run; own source id | Retry a failed row on the next iteration; after a load-only run, and before a run, the invocation log's delivered rows (refused without `track_completion`); own source id: yes (section 3) |
| P5 | A `LOG` node loads as `OFF`, so `as_source` of a loaded operator is refused though its rows are stored [proto 7] | Save the cache mode with the node (its own issue); until then the refusal says to attach `LOG` again |
| | Held plain-stream rows as "known content" (D10) | No; the fixed-content sources beneath a plain origin, yes (section 6) |

**Decided 2026-11-25 (round 2)**

| # | Decision |
|---|---|
| D4, D5 | *Function and operator records superseded in round 6 (section 13): the store's row key.* Record id: hash of tags, system tags, data, no tokens. A function stream or node: the base entry id (tags, system tags, input hash), one recipe for both (round 3; P2 closed). ITL-1154's wording is amended |
| D1 | One form; `tags_as_data=None` for none, `{}` refused (round 3) |
| D6 | Repeated ids raise; an operation that repeats rows will add an index column later |
| D7 | `OFF`: the current run's rows. Caching on: the content of the cache |
| D8 | Plain streams stream their content, computed when read; a derived source follows its origin's mode, held or read through (round 4). Side effects: allowed for all; the rows that passed through; a node fires once ever per row, a plain stream in its mode, caching (once per row, held; new) or non-caching (every iteration; today's); identity of a row unchanged (rounds 3, 4; P4 closed) |
| D10 | ITL-1469's rule; no warning |
| D11 | Single tokens kept, lists fresh, copies and nulls fresh; by the column's type; they stand with the trace boundary (P3) |
| D13 | The "only multi-pair origins" condition is dropped |
| P3 | `trace` stops at a derived source and reports its record id; it may continue into the origin's job through it. Kept tokens (D11) stand (round 5) |
| D14 | No preflight refusal |

**Open, each with its recommendation**

| # | Question | Recommendation |
|---|---|---|
| D2 | Which objects? | Section 3. A slot gives its bound source. Refuse unbound slots and non-job nodes |
| D3 | Arguments on a source? | None: itself. `source_id` alone: refused. `tags_as_data`: as today. No `label` |
| D9 | Default id | Keep the origin's content hash |
| D12 | History | *Round 6:* every stored passage by default; one row per content as a restriction (`first`, `latest`, `raise`); ITL-1129 later |
| D15 | Kind version | `derived_v1` if before 0.4.0 (0 golden moves), else `derived_v2` (14 provisional) |
| D16 | Save, load, moved store | Loads unbound and rebinds; warn on a load-only run over an unbound slot; raise when the recorded table is absent; restriction walks enter a plain origin |

Found in passing, to be filed: a stream built before a job becomes a slot that renames its
system-tag columns, so a node over it has the plain stream's content hash but other stored
columns (`repro_slot_renames.py`, on the library as it is); ordinary sources given one name and
other rows in one job are
silently one (more 4); the stored-schema conflict of section 2; the loaded load-only run that
is silently empty (more 3). Not examined: `SourceProxy` of a Postgres or Spiral source,
ephemeral results, `REPLAY` by running it, `PollingSource` under async.

## 13. Round 6: the store is a complete record

Developer, 2026-11-25 (afternoon); **this section supersedes the earlier ones where they
differ**. Evidence: **[proto6]** `probe_round6` on the round 6 prototype (local branch
`scratch/itl-1466-prototype`, not in this pull request); **[timed]** `time_entry_lookup`, on
the library as it is.

### 13.1 The model

1. **The store keeps every distinct passage.** A function node's pipeline row key is the hash
   of: user tags, system tags, the input's source-info tokens (in name order), the labelled
   input data hash, and the **result id** (the result record's `__data_id`). A write is skipped
   only when every one of these matches (`skip_duplicates`). The operator cache keeps its
   whole-row key (tags, system tags, data, tokens, node content hash), as today.
2. **A derived source's record id is the store's row key**, `arrow_v1:<hex>`. One recipe per
   pod and its node: a plain function stream hashes the same five things with the result id
   of its output; a plain operator stream hashes its whole row with tokens and its own content
   hash, as the node does.
3. **By default `as_source` serves everything stored**, duplicates of content included.
4. **A mode serves one row per (tags, system tags, data)**, keeping `first` or `latest` by
   recorded time, or raising (`raise`). Each served row keeps its own store key as record id.
   The mode is a **restriction**, like `where` and `limit`: outside every identity, under the
   restriction rules as they are (whether several restrictions of one table may coexist is a
   separate issue, not this design).
5. **Kept** from earlier rounds: the trace boundary (P3); kept single tokens (D11); side effects
   follow their origin's mode; one pair always; D6, a repeated store key raises.
6. **Superseded**: the base entry id as record id (D4/D5's function row, section 4), node and
   plain as separate rules (P2), the token-free key with first-come storage (P1's options (a)
   and (b)), and "the latest recomputation per base id" (D12, section 4).

### 13.2 Checked before any code

**Two keys, kept apart.** `_build_record_id_preimage` (user tags, system tags, labelled input
hash) is shared today by three users [read]:

| User | Key | Round 6 |
|---|---|---|
| Function node pipeline row | the preimage + a recomputation index | **changes**: the store key above, no index |
| Side-effect invocation log (`side_effects._execute_side_effect_row`) | the preimage + `__pipeline_recomputation_index` fixed at 0 | **unchanged**: the entry |
| `InvocationContext` record id of a pod that takes `ctx` (`FunctionPod._build_invocation_context`) | the same | **unchanged**: the entry |

So the change separates them: the pipeline row gets its own preimage function; the side-effect
and invocation keys keep `_build_record_id_preimage` with the column named
`__pipeline_recomputation_index` and the value 0, **even though the pipeline table no longer has
that column**: renaming it in that preimage would move every invocation hash. The constant
should be renamed in code (it is a fixed preimage column now, not an index), its value kept.
Checked [proto6 2]: after a token-only change upstream (`f`'s results were lost and recomputed,
so its output tokens name new result ids), `g` recorded two new passages, was not called again
(result-store hit on the same input hash), and the tracked side-effect pod over `g` did **not**
fire again (2 deliveries before, 2 after). A token never reaches the side-effect key.

**The recomputation index is no longer needed.** A recompute (a stored row whose result is gone)
writes a new result with a new `__data_id`, so its key differs from the old row's without an
index. Confirmed [proto6 2]: `f` recomputed both rows (4 calls), the table holds the 2 old rows
(result gone) and 2 new ones; nothing queries the table before a write any more (today each
write reads every row of its base entry to find the next index [read]). Where the index chose
between rows (the node's held rows, the ephemeral placeholders), `__recorded_at` chooses
instead. The ITL-508 behaviour "the same input processed twice writes two rows" ends: with the
same result id the second write is the same key and is skipped (the one test of it,
`test_execute_data_second_call_same_input_appends_recomputation_record`, now sees 1 row).

**Lookup cost without a stored entry id** [timed, N inputs all stored, one process, 18 CPUs]:

| N | Hash every input's entry | Filter a stored id column (`is_in`) | Join on (tags, system tags, input hash) | Hash every stored row |
|---|---|---|---|---|
| 10,000 | 0.19 s | 0.018 s | 0.005 s | 0.19 s |
| 100,000 | 1.87 s | 0.009 s | 0.007 s | 1.84 s |
| 1,000,000 | 18.6 s | 0.11 s | 0.035 s | not run |

Hashing dominates (about 19 µs a row); the lookup itself is cheap either way. Rehashing stored
rows to find entries is as costly as hashing the inputs and is not an option. Today's run
already hashes every input. Recommendation: **keep a plain, non-key id column** for the lookup,
and make it the **passage id** (the store key without the result id), not the entry: a run must
decide whether *this passage* is stored, and a stored entry with other tokens is a new passage
to record (without computing). The prototype does this (`__pipeline_passage_id`). A later speed
option: a join on the columns needs no hashing for a warm rerun (0.035 s against 18.6 s per
million rows); it must then match as Orcapod matches (a null equal to a null here, unlike
`Join`), so it is a separate change.

**What moves** [proto6; golden run on the prototype]:

| Item | Moves | Note |
|---|---|---|
| Pipeline table layout | `pdb_v1` to `pdb_v2`: `__pipeline_recomputation_index` removed, `__pipeline_base_entry_id` replaced by `__pipeline_passage_id`, row key recipe changed | every function node's stored path (last component) |
| Operator cache layout (developer, round 6 addition) | `odb_v1` to `odb_v2`: `__recorded_at` (UTC, microseconds, not null) added, outside the row key | every `LOG` / `REPLAY` node's stored path (last component); a re-passed row is skipped and keeps its first time |
| Golden `*.table[.../pdb_v1].record_ids`: `pipeline:job`, `pipeline:drop_nulls_job` (frozen file `pipeline-02`); `filter_job`, `polars_filter_expr_job`, `rejoin_job`, `rejoin_job_async` (provisional) | **6 entries can no longer be pinned**: the key holds the result id, a UUIDv7, new in every fresh store | pin the `__pipeline_passage_id` column instead (deterministic), as result tables pin their input hashes |
| Golden `*.table_paths` of the same 6 cases | **6 entries move**: only `pdb_v1` to `pdb_v2` | |
| Every other golden entry | none (2,756 golden tests pass; 18 fail, the 12 entries above and the 6 tests that check pinning, process independence and the async-equals-sync pin of the same entries) | no golden case caches an operator, so `odb_v2` moves none |
| Side-effect invocation hashes, `tdb_v1`, result tables (`rdb_v1`), function uris, content and pipeline hashes | none | |
| A derived source's record ids over a function origin | base entry id to store key | provisional, unpinned |

The 6 frozen-file and provisional changes need the developer's approval of the exact list
(v1 is open until 0.4.0). The layout bumps: nothing final is released, and ITL-1259 started
every marker at `v1` for 0.4.0, so keeping `v1` is possible; **recommend the bump** anyway:
stores written by a candidate are then reported as another layout and never read, rather than
refused at the first write as an incompatible table.

### 13.3 The prototype and its numbers

Changed (scratch only): the function node's row key, the passage id, `__recorded_at` in place of
the index, `pdb_v2`; the operator cache's `__recorded_at`, `odb_v2`, dropped on every read; the
derived source serving every stored row with the store key as record id; the mode,
`DerivedSource.one_row_per_content(keep=...)` (a copy, outside identity); one recipe for plain
streams. Not done: the mode in `pipeline/restrictions.py` (the walk, one restriction per table,
the caching refusal and the aggregation warning are to be applied to it as written).

[proto6]:

- A rerun in a new job: nothing computed, nothing fired, 2 passages.
- Results of `f` lost, a new job runs: `f` 4 calls; `g` 2 calls (none again); `tell` 2
  deliveries (none again). `g` serves 4 rows by default (2 passages of each entry, the second
  with `f`'s new tokens); `latest` serves the 2 new ones, `first` the 2 old ones, each with its
  own store key; `raise` raises. `f` serves 2: a row whose result is gone has no data to serve.
- Plain function pod: with a result store, two builds give equal record ids; without one, they
  differ (each output is a one-off output of its run).
- Plain operator stream ids equal the operator node's stored row keys; the node's `latest`
  works on `__recorded_at`.

Tests [selected by directory: `test_core/function_pod`, `nodes`, `sources`, the side-effect
directories, `test_empty_data`, `test_caching_integration`, `test_result_cache`,
`test_table_scope`, `test_pipeline`; one process]: 11,078 pass, 600 fail, 4 errors. By cause:

| Cause | Count | What a real change does |
|---|---|---|
| A test reads `__pipeline_base_entry_id` or the index (cross-session miss 384, pipeline record metadata 44, commit dependencies 14, crash recovery 4, ephemeral result 3, batched commits 2, provenance 1) | 452 | read the passage id; drop the index |
| A test names `pdb_v1` / `odb_v1` or crafts a table at that path (stored data context 53, loaded operator cache 13, loaded job readback 8, result database 4, hash step 3, zero-row order 2, arrow types 2, narrower bind 2, provenance review 1) | 88 | the new markers |
| A test injects a failure into the pre-write index query, now gone (store write failure) | 4 | remove |
| A test compares two runs' stored operator tables, now with times (rows on read) | 6 | compare without the time |
| ITL-508's "two rows for one input" (function pod node) | 1 | now one row, by design |
| A rerun whose input tokens were wrong before and are right now "keeps old rows and rewrites none" (tag table source info) | 2 | **behaviour change, by design**: the corrected tokens are a new passage, recorded without computing (0 calls still) |
| Earlier prototype rounds: a loaded `OFF` operator refused (loaded argument order 40, saved job 1); an unregistered class in a loaded slot (4, also on the round 3 prototype); `tags_as_data` tests of D1 / D11 (6) | 51 | already reported (section 9) |

No failure points at a design problem. Open points the prototype shows: an ephemeral
placeholder (`EmptyData`, a gone ephemeral result flowing through) has no readable tokens, so
its passage id is its entry; and the default view can show a row whose result is gone only as
"not served", as today.

### 13.4 Answers to the round 6 checks, in short

- Side effects keep the entry key; the store key is separate; a token change fires nothing.
- The recomputation index is unnecessary: confirmed.
- Lookup: keep a plain passage-id column (hashing, not the lookup, is the cost: about 19 µs a
  row); a column join is a later speed-up.
- Layouts `pdb_v2` and `odb_v2`; golden: 6 record-id entries become unpinnable (pin the
  passage id instead) and 6 table-path entries move; nothing else.
- `first` / `latest` need a recorded time: the pipeline table has `__recorded_at`; the operator
  cache gets one (decided), and keeps the time of a row's first insertion.

## 14. Round 7: three nested ids

Developer, 2026-11-25 (evening); **this section supersedes section 13 where they differ**.
Evidence: **[proto7]** `probe_round7` on the round 7 prototype (local branch
`scratch/itl-1466-prototype`, not in this pull request).

### 14.1 The model

| Id | Hash of | Used by | Stored |
|---|---|---|---|
| **entry id** | user tags, system tags, the labelled input data hash: an entry of the pipeline table | side effects and functions that take the invocation context only: hit or miss, `invocation_hash` | the side-effect log (its row key and `record_id_hash`); in the pipeline table not stored (below). Its name there would be `__pipeline_entry_id`, a new meaning under `pdb_v2`: today's column of that name (base id plus index) goes away |
| **instance id** | the entry id (labelled), the input's source-info tokens in name order: one instance of the entry arriving, with its provenance | the run's lookup "is this instance stored?"; the golden pin | the pipeline table's plain lookup column, `__pipeline_instance_id` |
| **record id** | the instance id (labelled), the result id (`__data_id`) | the pipeline table's row key; a derived source's record id | the pipeline table's row key, `__pipeline_record_id` |

Names decided by the developer (same evening). Side effects are keyed by the entry only; an option to key them by the instance id is not offered
for now. The configurable options (the serving mode of section 13.1 item 4, a side-effect
caching mode) come after the defaults.

The preimages, exactly as prototyped (each a one-row Arrow table hashed by the data context's
Arrow hasher, `arrow_v1`, and fed onward as `to_prefixed_digest()`, so each nested id carries
its label):

- entry: `tag.as_table(columns={"system_tags": True})` plus `__input_data_hash`
  (`large_binary`, labelled digest): `_build_record_id_preimage` as it is today, **without** the
  fixed `__pipeline_recomputation_index = 0` column that side effects and `ctx` append today.
- instance: `__pipeline_entry_id` (`large_binary`) then the `_source_*` columns in name order. An `EmptyData`
  input (a gone ephemeral result flowing through) has no readable tokens: its preimage is the
  entry id alone (open point of section 13.3, unchanged).
- record: `__pipeline_instance_id` (`large_binary`) and `__data_id` (`large_binary`, the result
  UUID's bytes).

The column names inside a preimage are part of its recipe: `__pipeline_entry_id` enters the
instance id (pinned, below), `__pipeline_instance_id` only the record id (not pinned).

**The entry id is not stored in the pipeline table.** Nothing needs it there: the run's lookup is by
instance id (a stored entry with other tokens is a new instance, recorded without computing); the
mode of section 13.1 groups by served content, not by entry; and the entry id can be recomputed
from columns the row already holds (user tags, system tags, `__input_data_hash`), checked
[proto7 1]. If a later need appears (grouping a large table's instances by entry without hashing),
a plain `__pipeline_entry_id` column can be added under the same `pdb_v2` bump before the release.

### 14.2 What changes from section 13

- **The entry id loses the fixed recomputation index column.** Section 13 kept it, so that no
  invocation hash would move; the developer's entry is the three things only. So every
  `invocation_hash` and every side-effect log key changes, and the side-effect log layout moves,
  **`tdb_v1` to `tdb_v2`**: a `tdb_v1` log read under the new keys would match nothing and every
  completed delivery would be made again. With the bump, the stale-table rule
  (`account_for_stale_tables`) refuses a side-effecting node that has a `tdb_v1` log beside it,
  before any delivery: checked [proto7 4], `SideEffectLogDataContextError`, 0 deliveries. The log's
  columns are unchanged; `record_id_hash` now holds the entry id under the new recipe (renaming it
  `entry_id` would ride on the same bump, optional).
- **Section 13's passage id is the instance id**, with the same content (entry plus tokens), now
  computed through the entry id; its values differ. The record id is computed through the input
  id. The flat preimages of section 13 are gone from the prototype.
- Unchanged from section 13: the recomputation index is not needed, the default serves everything
  stored, the operator cache's `__recorded_at` and `odb_v2`, a token-only change records new input
  ids without computing and fires nothing.

Checked [proto7]:

- g's pipeline table: id columns `__data_id`, `__pipeline_instance_id`, `__pipeline_run_id` and the row key
  read as `__pipeline_record_id`; no `__pipeline_entry_id` column. Recomputed from each stored row: instance id =
  hash(entry id, tokens) 2/2 rows; record id = hash(instance id, result id) 2/2 rows.
- The side-effect log (`tdb_v2`): its keys equal the entry ids of `tell`'s input rows, computed by
  the one helper (`entry_id`) that `FunctionPod._build_invocation_context` also calls.
- `f`'s results lost, a new job: `f` 4 calls, `g` 2 (none again), `tell` 2 (none again); g's table
  2 rows to 4, 4 distinct instance ids.
- `probe_round6` still gives section 13.3's results on the round 7 prototype (ids are new values).

### 14.3 What moves in the golden list

The same 12 entries as section 13.2; only what replaces the record-id pins changes:

| Entry | Round 7 |
|---|---|
| `*.table[.../pdb_v1].record_ids`, 6 entries: `pipeline:job`, `pipeline:drop_nulls_job` (frozen file `pipeline-02`); `filter_job`, `polars_filter_expr_job`, `rejoin_job`, `rejoin_job_async` (provisional) | **removed**: the record id holds a random result id. Replaced by 6 **new** entries `*.table[.../pdb_v2].instance_ids` (the golden cases pin `__pipeline_instance_id` for a pipeline table, as they pin `__input_data_hash` for a result table) |
| `*.table_paths` of the same 6 cases | **move**: `pdb_v1` to `pdb_v2` only |
| everything else | none |

The new instance-id values are computed through the entry id, so they are not what a section-13
passage-id pin would have held (never pinned, so nothing to undo). They are deterministic: two
processes with different hash seeds (`PYTHONHASHSEED` 1 and 2) give the same instance ids for
`pipeline:job` and `pipeline:drop_nulls_job`. No golden entry pins an invocation hash, a side-effect
log key or a `tdb` or `odb` path, so `tdb_v2` and `odb_v2` move none. Golden run on the round 7
prototype: 2756 pass, 18 fail, the same 18 as round 6 (the 12 entries and the 6 tests that check
them).

Awaiting the developer: the layout bumps `pdb_v2`, `odb_v2`, `tdb_v2`; the
exact golden list above (6 removed, 6 added, 6 moved).

### 14.4 Tests on the round 7 prototype

Prototype commit `e29ef8df` (names as decided). Golden: 2756 pass, 18 fail, as above. Side-effect,
`ctx` and source tests: no new failure from the entry recipe or `tdb_v2` (195 side-effect and `ctx`
tests pass; no test pins an invocation hash). The directories of section 13.3, run before the
renaming (one process): 11,064 pass, 614 fail, 4 errors. The 14 failures more than round 6 all
name `tdb_v1` (stored data context 13, hash step 1): the "names a layout" row of section 13.3 is
now 102. After the renaming, 2 more tests in `test_provenance_review` fail
(`test_two_entries_for_one_input_and_result_are_refused`): they inject a duplicate entry under the
old column name `__pipeline_entry_id`, the "reads the old columns" row. No failure points at a
design problem.

### 14.5 Open, for the developer

- The layout bumps `pdb_v2`, `odb_v2`, `tdb_v2`.
- The exact golden list of section 14.3: 6 record-id entries removed, 6 instance-id entries added,
  6 table-path entries moved.
- Whether the side-effect log's `record_id_hash` column is renamed to the entry id with `tdb_v2`
  (optional).
- The options after the defaults: the serving mode and a side-effect caching mode.
- An `EmptyData` input's instance id is the hash of its entry id alone (no readable tokens).
