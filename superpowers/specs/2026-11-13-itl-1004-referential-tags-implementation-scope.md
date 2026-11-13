# ITL-1004 — Referential tags (v0.4): implementation scope

**Status:** design audit for the developer's review. No library code changes.
**Issue:** ITL-1004. **Specification:** the Linear document "Orcapod v0.4 — Referential Tags:
design specification and implementation guidance" attached to the issue ("the spec"; §9.3
refers to its section numbers). The spec is the authority. This document scopes how the code
implements what the spec marks Required or Proposed, and raises decisions only for points the
spec marks Open or that the code makes impossible or ambiguous.
**Developer decisions included** (November 12, 2026):
- Every source's declared tags are referential **by default**.
- Tags named in `tag_only_columns` (a subset of `tag_columns`) stay materialized and must
  reproduce v0.3 exactly, so the referential path is additive. This is a requirement and a
  proof obligation: it is proven once the golden fixtures, switched to `tag_only_columns`, pass
  unchanged (ITL-1011, ITL-1012). This audit shows only that no hash input of a tag-only
  stream changes (§3).
- The change is a change of default behaviour.
- `Join`'s three cases (§9.1) have equal standing.
- Pod boundary (D1). With `pod.process(...)` and `pod(..., strict=True)`, a reference whose
  data column does not survive becomes a plain tag. `pod(...)` uses a **variant of the function
  pod** that emits each row's referenced columns beside the function's outputs, so they stay
  referential. It is not a join and not an inserted operator (§11). Full append is deferred
  (ITL-1025).
- `GroupBy` follows spec §8 unchanged; `Join` gets no special rule for list-valued tags.
- Saved jobs carry version information that pins behaviour (§4).
- Renames (§5.4): `map_tags` moves both roles of a reference, `tag_only=True` and `map_data`
  leave a plain tag.

**Code audited:** `origin/main` at `f51ce67c`.
**Measurements:** `superpowers/specs/itl-1004-audit/` holds each script (run with `uv run`) and
its output (`.out`). Every example below comes from those outputs.

| Script | What it measures |
|---|---|
| `01_today_same_name.py` | What v0.3 does when a tag and a data column share a name |
| `02_prototype_unified.py` | Every operator over a **prototype** referential stream: today's stream with one physical column put in both role lists through private attributes. A probe of today's operator code, not an implementation |
| `03_arrow_table_stream_sites.txt` | The 32 places that construct an `ArrowTableStream` |
| `04_identity.py` | Which hashes see a binding |
| `05_migration_and_collisions.py` | Function-result identity across the move from `tags_as_data`; today's output-name checks |
| `06_default_referential.py` | Consequences of the referential default; today's plain-with-plain `Join` |

---

## 1. Verdicts in brief

1. **Representation (spec §13.3, open question "how Arrow storage avoids duplicated values").**
   A referential tag `x` is **one physical column `x` whose name is in both role lists** of the
   stream. The binding is the name intersection `tag_keys ∩ data_keys`, which is the spec's
   §4.3 statement as code.
   - The current table abstraction already supports this. At stream level Orcapod is the
     spec's option B: an `ArrowTableStream` holds one table, tags are a name list, and data is
     every other column (`core/streams/arrow_table_stream.py:85`, `:113-115`). At row level it
     is option A: separate `Tag` and `Data` datagrams.
   - `types.Schema` and the `(tag_schema, data_schema)` tuple do not change. A referential tag
     appears in both halves of `output_schema()` when the graph is built.
2. **Hashes.** No data context version is needed, and no golden entry moves.
   - A stream with only materialized tags produces byte-identical hash inputs. The binding has
     no term of its own, so the pair `(tag_schema, data_schema)` is exactly the v0.3 pair.
   - A referential tag changes the pair, so it enters `pipeline_hash`, the root `content_hash`
     and the source schema hash only when present.
   - A function that does not read the referenced column receives a `Data` whose hash equals
     the v0.3 one (measured, §3.3). Its stored results are reused.
3. **The default, for an unchanged v0.3 script.** Moved: source content hash, pipeline hashes,
   system-tag column names, function-node pipeline record ids, and pipeline/operator/cached
   table paths. Unchanged: source ids, source record ids, row `Data` hashes and results of
   functions that read no key, and result table paths. A user reproduces v0.3 exactly by
   also naming the tags in `tag_only_columns` (§6.2).
4. **Storage.** No stored-table layout changes. A referenced value is written once in every
   table kind. The binding is recovered on read by the same intersection rule from the saved
   `{tag, data}` descriptor.
5. **Work.**
   - The prototype shows filters, `DropNulls`, `SemiJoin`, `Batch` and non-key `GroupBy`
     already produce coherent physical results, ordering included.
   - What breaks is generic: every operator rebuilds its output with
     `ArrowTableStream(table, tag_columns=...)`, which drops the data role (32 sites), and two
     row-to-table helpers `hstack` tag and data (duplicate column).
   - The new rules are local: `Join` cases 2 and 3, materialization on select or drop, the
     `GroupBy` key, and output collisions.
6. **Prerequisite: ITL-940 (Backlog).** Today `Join`, `MergeJoin`, the function pod and
   `Index(out=)` accept a tag `x` beside a data `x` at build time. Under the intersection rule
   such shapes would look referential while holding different values. The async `Join` already
   emits such rows silently (`01 R2`).
7. **One Required rule the code makes impossible as written** (spec §6.1 against
   `core/function_pod.py:747`): a pod's output data is only the function's outputs, so a
   reference's target never survives a pod. Decided (D1): the tag becomes plain at the pod boundary.

---

## 2. Representation (question 1)

### 2.1 What the code is today

- **Stream.**
  - `ArrowTableStream` stores one table. Tags are the given names
    (`arrow_table_stream.py:85`); a missing tag name raises (`:102-105`); data is "every
    column not a tag" (`:113-115`); the schemas are filtered by those sets (`:127-136`).
  - `iter_data` selects each row's tag and data columns from the same record batch
    (`:352-353`). `RecordBatch.select` is zero-copy, so a name in both selections shares one
    buffer.
- **Disjointness is by construction.** A table can hold two columns of one name, but the
  stream then fails on its first lookup (`01 R1`:
  `KeyError: 'Field "x" exists 2 times in schema'`). The source builder splits the same way
  (`data_schema = non_sys.drop(tag_columns)`, `core/sources/stream_builder.py:219-220`) and
  gives source info only to non-tags (`:336-338`).
- **Rows.** `Tag` and `Data` are independent datagrams with their own tables
  (`core/datagrams/tag_data.py:53`, `:274`). A row can already hold `x` in both.
- **Schema.** A plain mapping (`types.py:84`). The `(tag, data)` pair is the only record of a
  column's role. No Arrow field metadata marks a role, and the hasher strips non-extension
  metadata (`hashing/schema_cleaner.py:141-162`).

### 2.2 Every site that assumes disjointness

Legend:
- **OK**: works unchanged.
- **Change**: needs references passed or deduplicated.
- **Reject**: must refuse a shape.

The prototype (`02`) verified the rows marked †.

| Site (file:line) | Assumption | Verdict |
|---|---|---|
| `ArrowTableStream.__init__` (`arrow_table_stream.py:113-115`) | data = non-tags | **Change**: new `reference_tags` argument. Data = non-tags ∪ references; validate references ⊆ tags and present. The "at least one data column" rule (`:120-123`) then also counts references, so an all-tags source with referential tags is valid (`06 H3` shows v0.3 raises for a plain one, and still will) |
| `keys` / `output_schema` (`:182-236`) | — | OK†: `x` in both (`02 P0`) |
| `iter_data` (`:319-353`) | — | OK†: zero-copy selects |
| `as_table` / `_assemble_table` (`:256-310`) | — | OK†: one column, emitted once |
| `identity_structure` without producer (`:146-163`) | `self._tag_columns` alone | **Change**: add `("reference_tags", refs)` only when non-empty (§3) |
| `SourceStreamBuilder.build` (`stream_builder.py:143-295`, `:336-338`, `:346`) | data = table minus tags | **Change**: explicit `reference_tags`; schema hash over the pair with `x` in both; source info for referenced columns |
| `StaticOutputOperatorPod._rows_to_table` (`core/operators/static_output_pod.py:208-252`) | `hstack(tags, data)` | **Change**: drop a data column already present as a tag; return the reference set. `02 P4`: the async `GroupBy` raises `Duplicate column name 'x'` (`utils/arrow_utils.py:678-682`) |
| `_materialize_to_stream` / `_materialize_input_stream` (`static_output_pod.py:256-300`); `pipeline/sync_orchestrator.py:216`, `:236` | `tag_columns` only | **Change**: pass references (the empty branch takes them from `upstream_node.output_schema`) |
| 15 operator rebuilds (`03_arrow_table_stream_sites.txt`) | `ArrowTableStream(table, tag_columns=…)` | **Change**†: today the data role is lost silently. `02 P1`: `PolarsFilter` keys say data `('x','y')`, but rows carry `{'y': 0.2}` |
| `FunctionPodStream._table_from_pairs` (`core/function_pod.py:1111-1152`) | `hstack(tags, data)` | OK, given the output-collision rule (§7) |
| `Join._joined_user_schemas` (`core/operators/join.py:78-119`), `_merge_data` (`:759-790`) | shared data name = error | **Change** (§5.1) |
| `MergeJoin` (`merge_join.py:161-175`, `:325-420`) | shared data name = list merge | **Change** (§5.2, open) |
| `SemiJoin` (`semijoin.py:76-130`) | matches on overlapping tag **and** data names | OK† |
| `GroupBy.unary_output_schema` (`group_by.py:302`) | every data column becomes a list | **Change**: a referenced key stays scalar |
| `column_selection.py` (classes at `:37`, `:158`, `:286`, `:413`; rebuilds at `:76`, `:202`, `:328`, `:459`; async at `:113`, `:241`, `:364`, `:494`) | dropping a name drops the column | **Change**: drop a role (§5.3). The async row paths are already right |
| `MapTags`/`MapData` (`mappers.py:83`, `:104`, `:226`, `:254`) | a rename moves the column | **Change** (§5.4) |
| `Index`/`Pick` (`index.py:365`, `pick.py:379`) | data replaced in place | **Reject** in-place on a referenced column (§5.5) |
| `FunctionPod.output_schema` (`function_pod.py:733-747`) | output data = function outputs | **Change**: reject output names equal to a tag; D1 |
| `FunctionNode` read (`core/nodes/function_node.py:2820-2835`, `:2891`) | tags = pipeline-table columns | OK under D1 (tags are plain after a pod) |
| `OperatorNode` read (`operator_node.py:1116`, `:1228`, `:1317`, `:1581`; loaded stub `:235-238`) | `tag_keys = self.keys()[0]` | **Change**: pass references (from `keys()` or descriptor) |
| `CachedSource` (`cached_source.py:360-363`), `DerivedSource` plain (`derived_source.py:462`, `:490`, `:543`), `PollingSource` (`polling_source.py:288-301`, `:865`), `SpiralDBTableSource` (`spiraldb_table_source.py:623`) | tags vs rest | **Change** (§6) |
| `SourceProxy.keys` (`source_proxy.py:177-185`); node descriptor (`pipeline/graph.py:178-199`) | — | OK: two name→type dicts; a name may be in both |
| `schema_utils.union_schemas` / `intersection_schemas` | — | OK: equal types merge (`02 P1`, `SemiJoin`) |

### 2.3 How a binding is declared (ITL-1009)

**The rule, in one place:** `reference_names(tag_schema, data_schema) = tag ∩ data` (sorted).
For each name the two Python types must be equal. Because there is one physical column, the
Arrow type, nullability, nesting and values are identical by construction, so §4.4 and §4.5
hold without a check on values.

- **Schema model.** No change to `Schema` or the tuple. Add:
  - `schema_utils.reference_names(...)`;
  - `StreamBase.reference_tags()`;
  - one validator for the namespace rules, run on every predicted output schema from the base
    wrappers (`StaticOutputOperatorPod`, `_FunctionPodBase`), per spec §13.4.
- **Why inference is sound.** §4.3: "the presence of a tag and data column with the same name
  must be sufficient to identify the tag as referential". The only unsound inputs are shapes
  made by accident: tag `x` from one input with data `x` from another (ITL-940), or an output
  named like a tag. The validator rejects both when the graph is built.
- **Rows.** A row-level consumer, meaning the async paths, applies the same intersection to a
  row's tag and data keys. It agrees with the sync path without new channel metadata.
- **Cost.** 32 constructor sites pass references, and 2 helpers deduplicate.

**Comparison with the spec's options (§13.3).**
- **Option A** (separate tag/data storage plus binding metadata) does not match the code:
  Orcapod has no separate tag storage at stream, operator-cache or cached-source level.
- **Option B** (one table, role metadata) is what the code already is. The "role metadata"
  is the two name lists.

What remains open is whether to add an *explicit* marker on `Schema` (decision D3).

---

## 3. Identity and hashes (question 2)

### 3.1 Which identities see a binding

| Identity | Hash input (file:line) | Sees a binding? | Source with `tag_only_columns` vs v0.3 |
|---|---|---|---|
| Root `pipeline_hash` | `(tag_schema, data_schema)` (`core/sources/base.py:132-139`) | **Yes**, through the pair. `04 I1`: tag-only `b9e6…`, data-only `95a5…`, referential `cc81…` | identical |
| Root `content_hash` | `(class, output_schema(), source_id)` (`base.py:88-90`) | **Yes** (`04 I3`) | identical |
| Source schema hash (names system-tag columns) | `compute_source_schema_hash(tag, data)` (`stream_builder.py:227`) | **Yes**. `06 H1`: `a7ad…` → `6a4b…` | identical |
| `ArrowTableStream` without producer, `content_hash` | `(class, table, tag_columns)` (`arrow_table_stream.py:146-163`) | **must add** `("reference_tags", refs)` when non-empty, or a referential and a plain stream over one table collide | identical (term absent) |
| Downstream hashes | `(producer, argument_symmetry(upstreams))` (`core/streams/base.py:106-113`; `static_output_pod.py:62-64`) | Yes, transitively. An automatic materialization is a function of (operator identity, input schema), so it is inside the Merkle chain (§12.3: "automatic materialization likewise changes the resulting pipeline structure") | identical |
| Default source id | raw table hash (`stream_builder.py:237`, `:257-265`) | No | identical |
| Source record id | `uuid5(source_id::token)` (`stream_builder.py:97-116`, `:333-335`) | No (no schema input) | identical |
| Function-node record id | `tag.as_table(system_tags)` + input `Data` digest (`function_node.py:115-141`) | Through the system-tag column *names* (schema hash). The table path is scoped by `pipeline_hash` (`function_node.py:475`) | identical |
| Row `Data` hash (memo key, `core/result_cache.py:216-227`) | data table (`core/datagrams/datagram.py:445-454`) | **No** (`04 I4`, `06 H2`) | identical |
| Function-result identity | `DataFunction.uri` (`core/data_function.py:200-212`) | No (`06 H2`: the uri holds name, output-schema hash and version only) | identical |

### 3.2 Additive identity: how it is achieved

The pattern of PRs #336 and #338 adds a term to an identity structure only when the option is
given. Here the pattern needs only one explicit term, the `ArrowTableStream` one; everywhere
else the binding enters as the name in both schema halves, absent in the plain mode. Proof
obligations for ITL-1011:
- every existing golden entry is unchanged, with the fixtures switched to `tag_only_columns`
  (§6.2);
- new golden entries pin the referential default.

A data context version is **not** needed. No algorithm changes, and every new value belongs
to a shape v0.3 cannot build (`01 R1`); the data context key is the global hash version, and
nothing about it moves.

### 3.3 The default, for an unchanged v0.3 script (spec §12.4, §16.3)

| | Moves? | Why / evidence |
|---|---|---|
| Source `content_hash`, `pipeline_hash` | moves | schema pair gains the tags as data (`04 I1`, `I3`) |
| System-tag column names | moves | the source schema hash moves (`06 H1`) |
| Source id, source record ids | stays | built from the table hash and row token only |
| Function-node pipeline record ids | moves | the preimage holds system-tag columns, whose names moved |
| Pipeline table path (`<fn>/schema:<pipeline_hash>/pdb_v2`), operator cache (`odb_v1`), cached source (`sdb_v1`) | moves | the paths contain the pipeline or content hash |
| Result table path (`<function uri>/rdb_v2`) | stays | `06 H2`: `('double_y', '872e…', 'v0', 'python.function.v0')` |
| Row `Data` hash for a function that reads no key | stays | the automatic select drops the referenced keys (materializing them), and what remains hashes as in v0.3 (`06 H2`: `True`) |
| A function that reads a key | new | no v0.3 function could read a key under its own name: tags were never data |
| A function that read a `tags_as_data` copy | moves once | the column name is in the hash (`05 M1`) |

So retagging works as the spec intends (§2.2, §12.2): pipeline records are re-established
under new paths, and results are found again. No migration step is required: the v0.3
pipeline tables stay where they are, with current layouts, so they are not reported stale and
are not read. The upgrade note names `tag_only_columns` for anyone who wants the old paths.

One consequence is not covered by the result cache. `pod.process(source)` and
`pod(source, strict=True)` (ITL-807's exact form) now see every key as an extra data column
and raise, unless the function takes all keys. That is decision D2.

---

## 4. Storage (question 3)

| Table | Columns today | With referential `x` | Layout change |
|---|---|---|---|
| Function pipeline `pdb_v2` (`function_node.py:2337-2461`) | user and system tags; the input's `_source_*` (`:2389-2396`); meta | `x` once, as a tag. `_source_x` once, if `x` was an input | none |
| Function result `rdb_v2` (`result_cache.py:269-347`) | outputs, their `_source_*`, `__input_data_hash`, `__data_context` | unchanged (`x` is never an output, §7) | none |
| Operator cache `odb_v1` (`operator_node.py:930-1019`) | flat `as_table(source, system_tags, context)`, row hash, `__data_context` | `x` once (`02 P0`), plus `_source_x` | none |
| Cached source `sdb_v1` (`cached_source.py:278-311`) | flat source table | `x` once | none |
| Side-effect log `tdb_v2` | ids only | unchanged | none |

**Getting the binding back.**
- A live node passes `reference_tags()` where it now passes `keys()[0]`
  (`operator_node.py:1114-1117`).
- A loaded job computes `tag ∩ data` from the saved descriptor
  (`pipeline/graph.py:178-190`; stub at `operator_node.py:235-238`). `SourceProxy` already
  returns `x` in both `keys()`.
- `DerivedSource` and `CachedSource` take the binding from the live origin.

**Stored-schema changes, named exactly.**
- No stored table changes, and the descriptor format is unchanged.
- Source configs gain `tag_only_columns` (§6.2).
- **Version pinning (developer, November 12, 2026: "saved jobs should clearly come with
  version info to pin behavior").** Measured (`07`), a saved job today holds:
  - `orcapod_pipeline_job_version = "0.1.0"` (written `pipeline/job.py:1356`, checked
    `:1422-1426`);
  - the embedded pipeline's `orcapod_pipeline_version = "0.1.0"` (`pipeline/graph.py:217`,
    checked `:258`);
  - a `data_context_key` per node (`std:v1:default`).

  It holds no Orcapod package version, and the source configs in `bindings.sources` carry
  none. Both format versions were already `"0.1.0"` at v0.2.0, so they were not bumped
  across the v0.2-to-v0.3 hash changes. The context key will not change in v0.4 (§3.2), so
  it cannot tell v0.3 from v0.4.
- **The rule (decision D8):**
  - v0.4 bumps both format versions in the change that flips the source default (the last
    step of ITL-1012).
  - Loading a `"0.1.0"` file pins v0.3 meaning: every declared tag tag-only. The saved
    content and pipeline hashes then match.
  - A standalone source config (a `to_config()` dict outside a job or pipeline file) carries
    no version, so v0.4 adds a `config_version` to source configs if those must reload
    across versions.
  - Sent to the coordinator before the v0.3.0 tag: nothing is strictly required in v0.3.0,
    but writing `orcapod_version` (the package version) beside both format versions costs
    about two lines and no hash. It lets v0.4 tell v0.3.0 files from files written by the
    ITL-1023 integration branch, which uses format `"0.1.0"` with referential tags.

**Legacy data.** A v0.3 table or saved job has disjoint `tag`/`data` sets, so it reads as
plain tags only (§16.1). Nothing is reinterpreted.

---

## 5. Operator by operator (question 4)

Sizes:
- **S**: up to about a day with tests.
- **M**: 2-4 days.
- **L**: a week or more.

"Rebuild" means passing references to the constructed `ArrowTableStream`; ITL-1010 makes
that change at every site.

| Operator | Today (measured) | Spec rule | Change | Size |
|---|---|---|---|---|
| `PolarsFilter` (`filters.py:80`) | prototype: data `x` lost from rows (`02 P1`) | preserve (§10) | rebuild | S |
| `DropNulls` (`drop_nulls.py:306`) | prototype: narrows `x` in both roles, so both read `int` | preserve | rebuild (one column, one flag) | S |
| `SemiJoin` (`semijoin.py:130`) | prototype: rows right, data `x` lost | preserve (§10) | rebuild. v0.3 `SemiJoin` already matches overlapping tag and data names (`:76-83`) | S |
| `Batch` (`batch.py:107`) | prototype: tag and data `x` both `list[int]`, in input order | preserve when both are restructured alike (§10.2) | rebuild. Every member column is listed in input order (`:60-100`), and `x` is one column | S |
| `GroupBy` non-key (`group_by.py:258`) | prototype: predicted list in both (correct) | list in both, same order (§8.2-8.3) | rebuild. Members sort on non-key tags (`:192-215`); one column means one order | S |
| `GroupBy` key | prototype: data **predicted `list[int]`** (`02 P4`) | scalar in both (§8.1) | rebuild, plus `unary_output_schema` keeps a referenced key scalar (`:302`). Physically keys are not member columns (`:187`) | S |
| `GroupBy`/`Batch` async | `Duplicate column name 'x'` (`02 P4`) | as sync | `_rows_to_table` (ITL-1010) | — |
| Select/Drop data (`column_selection.py:158`, `:413`) | raise when dropping referenced `x` (`02 P2`) | materialize (§7.2) | §5.3 | S |
| Select/Drop tag (`:37`, `:286`) | the physical column dropped, so data `x` is lost (`02 P2`) | remove tag, keep data (§10) | §5.3 | S |
| `MapTags`/`MapData` | renaming a referenced tag raises (`02 P3`) | decided (§5.4) | §5.4 | M |
| `Index`/`Pick` | §5.5 | §4.5 | §5.5 | S |
| `Join` | §5.1 | §9 | §5.1 | M |
| `MergeJoin` | §5.2 | open (§17) | §5.2 | M |
| Function pod + automatic join and select (ITL-807) | §7 | §6 | §7 | M |

### 5.1 `Join`: the three cases of §9.1, with equal standing

**Today.** `Join` joins on **all** shared tag names (`common_tag_keys`, `join.py:202`), inner,
through Polars (`:219`), with key types checked (`:206`; async `:648`). It refuses any shared
data name (`:116`; async `:781`). Because every shared tag is a join key, the condition "the
join established equality" (§9.5, point 3) always holds for a shared referential name.

| Case | Today (measured) | v0.4 (spec §9.2-9.3) | Code change |
|---|---|---|---|
| Plain with plain | `06 J1`: tags `('subject',)`, data `('b','a')`, pipeline hash `d12c05799eccd043`, four name-extended system tags, sync = async | **byte-identical** | none. Pin the `06 J1` values as a regression test; the golden join cases already cover it |
| Plain with referential | prototype `02 P5`: `output_schema` already right (tag `x`, data `x`); sync read raises `KeyError` in the column layout (`table.select(tag_order + data_order)`, `:238-246`); async rows already right | referential tag; data `x` from the referential side; nothing made from the plain tag | Polars keeps one key column `x`, which *is* the referential side's data. Lay out each name once; the output's references are the inputs' references among shared keys |
| Referential with referential | prototype: `InputValidationError: Datas should not have overlapping keys` | referential tag, one merged data `x` | `_joined_user_schemas` and `_merge_data` exempt a name that is a tag on both inputs and referential on at least one; one key column survives |
| Referential tag with data `x` but no tag `x` | — | error (§9.5) | none: not a key, so an ordinary collision |
| Plain tag `x` with data `x` but no tag `x` | `01 R2`: build accepts; sync read `KeyError`; **async yields rows where tag `x` ≠ data `x`** | invalid (§4.1) | reject (ITL-940) |

**Provenance of the merged column (§9.4: "must retain the relevant source relationships").**
How the code retains them:
1. **Row level.** The name-extended system-tag columns already keep both inputs' `source_id`
   and `record_id` per row (`06 J1` shows four columns), and they are the pipeline
   provenance. Nothing changes.
2. **Field level.** Each referential input brings `_source_x`; Polars suffixes the right one
   `_source_x_right`. `MergeJoin` already handles this suffix (`merge_join.py:408-409`).
   Retaining both relationships means keeping **both tokens**. The one code-level choice is
   the column's shape (D5):
   - (a) a sorted two-element list, as `MergeJoin` does;
   - (b) two scalar columns.
3. **Plain with referential.** Only the referential side has `_source_x`, so there is nothing
   to merge.

**Under the referential default.** Two sources sharing tag columns now meet case 3 on every
join, including the automatic join of `pod(a, b)` (ITL-807), which builds the same `Join`.
The automatic select that follows then materializes the keys the function does not read, so
the function's `Data` is the v0.3 one (§3.3).

### 5.2 `MergeJoin` (spec §17 open, ITL-1018)

**From the code.** An inner join on shared tags (`merge_join.py:393`); every shared data name
becomes a sorted `large_list<T>` (`:325`, `:404-420`), with source info reordered to match.
- A shared referential name cannot be list-merged: tag `x` would be scalar beside data
  `[v, v]`.
- Materializing first is impossible: a plain tag and data of one name is invalid.

**Options (D4):**
- (a) **Recommended.** An equated referential name stays one scalar column, as in `Join`; a
  referential name colliding with a non-tag data name is rejected.
- (b) Reject every referential input until the user materializes.

There are no unmatched rows: `how="inner"`.

### 5.3 Select and drop: roles, not columns (spec §7, ITL-1015)

`SelectDataColumns`/`DropDataColumns` removing a referenced `x` remove the data role. The
physical column stays as a plain tag, with the same buffer, type and values (§7.2, §4.4). The
tag operators remove the tag role and keep data. The async row paths already behave this way,
because they drop per datagram.

**Diagnostic (§7.4, Proposed).**
- **Config.** A new `OrcapodConfig` section field
  `reference_materialization: Literal["warn", "silent"] = "warn"`, beside `hashing`, `display`
  and `datetime` (`config.py:103-112`). No hash reads it.
- **Where it fires.** Once per operator application, from one helper in the operator base that
  compares input and output reference sets. It emits a `warnings.warn` category the user can
  filter. It is recommended silent for the automatic select of `pod(...)` and at a pod
  boundary (D1).
- **Inspection (§7.3, §13.5).** The output schema shows the transition. The same helper
  exposes `binding_transitions(...)` → `{name: kept | materialized | removed}` for pipeline
  views.

### 5.4 Renames: DECIDED (developer, November 12, 2026; closes spec §10.1)

Today `MapTags` and `MapData` rename within their own namespace and refuse a name of the
other one (`01 R4`; `mappers.py:104`, `:254`). With references (prototype `02 P3`), renaming
either side of a reference raises. The decided rules:

| Call | On a referential tag `x` → `y` | On a plain tag | Diagnostic |
|---|---|---|---|
| `map_tags({x: y})` (default) | renames the column in **both** roles; the reference survives as `y` (one physical column: rename the column, then rename it in both role lists) | as today | none (nothing materializes) |
| `map_tags({x: y}, tag_only=True)` (spelling is the coordinator's; the developer may rename it) | the tag becomes plain `y`, a zero-copy duplicate (`table.append_column(field.with_name(y), table.column(x))`, the `derived_source.py:429-434` idiom); data `x` keeps its name | as today | none (the user asked) |
| `map_data({x: y})` | always a data-only move: data becomes `y`; tag `x` stays as a plain tag under the old name (zero-copy duplicate) | n/a | none (the user asked) |

A rename that would produce a plain tag and a data column of one name, or collide with an
existing column, raises as today (`mappers.py:104`, `:254`). `tag_only` enters `MapTags`'s
identity only when true, so the identity of a plain-tag rename stays v0.3's. The EDI test
renames `timestamp` to `spike_time` / `gaze_time` with `map_tags`. Size M (ITL-1018, or the
ITL-1023 slice).

### 5.5 `Index` and `Pick`

- **`out=None` on a referenced column.** The operator replaces the data in place, which would
  leave tag `x` ≠ data `x`. Materializing would create a plain tag and data of one name (§4.3).
  So per §4.5, **reject**, naming the remedy (`out=`, or drop or select first).
- **`out=` equal to a tag name.** Accepted at build today; reading fails with
  `Duplicate column name 't'` (`05 M2`). Reject at build (ITL-940).

---

## 6. Sources (question 5)

### 6.1 The referential default

A source applies the default; the builder stays explicit.
- `SourceStreamBuilder.build(..., reference_tags=...)` takes the list. It computes the schema
  hash from the pair with references in both halves, gives source info to referenced columns
  (exclude only `tags − references`, `stream_builder.py:336-338`), and passes the list on.
- Each source computes `reference_tags` = its declared tags minus the ones asked for as plain.

**Every source:**

| Source | Declared tags today | v0.4 default |
|---|---|---|
| `ArrowTableSource`, `DictSource`, `CSVSource`, `DataFrameSource`, `DeltaTableSource` | `tag_columns` | referential |
| `DBTableSource`, `SQLiteTableSource`, `PostgreSQLTableSource` | `tag_columns`, else primary keys (`db_table_source.py:98-104`; SQLite `rowid` fallback `sqlite_table_source.py:92-96`) | referential (§11.4: keys are a construction policy) |
| `SpiralDBTableSource` | `tag_columns`, else keys (`spiraldb_table_source.py:307-311`) | referential. Keys are non-null already (`:466-486`); a reference keeps the exact field |
| `PollingSource` | `tag_columns` (`polling_source.py:257-301`) | referential; its own identity `(impl.identity(), tag_columns)` (`:351`) gains the plain list only when used |
| `ListSource` | `element_index` or `tag_function` output (`list_source.py:49-76`): **values computed by the source, not columns** | code-ambiguous, D6 |
| `DerivedSource`, `CachedSource`, `SourceProxy` | the origin's or saved roles | keep the origin's bindings; no default applied afresh |

Two further consequences:
- **All-tags source.** With referential tags the "one data column" rule is met, so it is valid
  (ITL-1012). A plain all-tags source still raises, as in v0.3 (`06 H3`).
- **The automatic join and select (ITL-807)** work as described in §5.1 and §3.3.

### 6.2 The explicit tag-only option: `tag_only_columns` (DECIDED, ITL-1012)

The developer chose the name and shape (November 12, 2026):
- `tag_columns=[...]` lists **all** tags, referential by default.
- `tag_only_columns=[...]` names the subset kept as tags only (materialized, not data).
- A name in `tag_only_columns` that is not in `tag_columns` raises `ValueError`.
- The option has the same name on every source.
- In prose the spec's terms "materialized" and "referential" stay for the concepts.

| Need | Call |
|---|---|
| v0.3 behaviour, byte-identical | `ArrowTableSource(t, tag_columns=["k"], tag_only_columns=["k"])` |
| Mixed (§11.3: `lab` a label, `timestamp` referential) | `tag_columns=["lab", "timestamp"], tag_only_columns=["lab"]` |
| DB sources with primary keys as the default tags | `tag_columns=None` gives the keys, referential. The v0.3 form lists the keys in both arguments. A shorthand for "all declared tags" (for example `tag_only_columns="all"`) would save listing them; that is the ITL-1012 worker's proposal to make |

Implementation: each source computes `reference_tags = tag_columns − tag_only_columns`
and passes it to the builder (§6.1). `to_config` writes `tag_only_columns` only when it is
non-empty.

**Golden fixtures (`tests/test_hashing/golden/cases.py`).**
- Each of the 15 source constructions with `tag_columns=` gains `tag_only_columns=` naming
  the same columns. No fixture moves or is renamed, and every entry stays byte-identical.
- The builder call (`:596`) and the `DerivedSource` case (`:618-633`) need no edit: the
  builder is explicit, and `DerivedSource` keeps its origin's binding once the origin fixture
  is tag-only.
- `ListSource` (`:1699`) depends on D6.
- New entries pin the referential default for each source kind, `Join` case 3, and `GroupBy`
  on a referential key.

**Upgrade note (changelog fragment of ITL-1012), draft.** "Declared tag columns are now
referential by default: each is also a data column of the same name, stored once, so a
function pod can read it. This changes the schemas and hashes of pipelines that use
`tag_columns`; stored function results are reused. To keep a tag as a label only, with the
v0.3 schemas and hashes, also name it in `tag_only_columns=[...]`."

### 6.3 `tags_as_data` (spec §11.5, §16: legacy meaning kept, explicit migration)

**How the code keeps the meaning.**
- `tags_as_data={tag: new_name}` still adds an independent renamed copy
  (`derived_source.py:429-434`; Spiral `:375-388`).
- Its identity terms are unchanged (`derived_source.py:274-293`;
  `spiraldb_table_source.py:407-421`).
- `validate_tags_as_data` already refuses a name that is a column (`derived_source.py:98-101`,
  `01 R6`), so the copy can never collide with a reference.
- Beside the default, a `tags_as_data` copy of a referential tag is simply a second data
  column with a different name.

**Explicit migration (ITL-1020, ITL-1021):**

| v0.3 | v0.4 |
|---|---|
| `SpiralDBTableSource(..., tags_as_data={"spike_time": "spike_time_us"})` | `SpiralDBTableSource(...)`, with keys referential by default |
| pod parameter `spike_time_us` | pod parameter `spike_time` |

Functions that read the copy recompute once, because the column name is in the `Data` hash
(`05 M1`). That is spec §16.3: the computational input changed.

---

## 7. Function pods (question 6)

- **Input binding by name.** It works unchanged (`02 P6`: `uses_x.pod(R)` gives `z = 20.2`,
  …).
- **Automatic select (ITL-807, `function_pod.py:669-727`).** For a function that does not
  read `x`, the automatic select removes data `x`, and the select's materialization rule
  applies (§6.5), as ITL-1013 says. Today it raises on the prototype (`02 P6`); with §5.3 it
  materializes.
- **Tags pass through** (`function_pod.py:739-747`). The output data is only the function's
  outputs (`:747`), so a reference's target never survives a pod, whether or not it was read.
  Measured: `uses_x.pod(R)` outputs tags `(session, x)`, data `(z,)`. Decided (D1): with
  `process` the tag becomes plain at the boundary; `pod(...)` uses the fall-through variant of
  the pod, and the references stay referential (§11).
- **An output name that collides with a tag (§6.2, Required).**
  - Today `output_schema` accepts it (`01 R5`:
    `(Schema({'x': int | None}), Schema({'x': float}))`), and reading fails with
    `Duplicate column name 'x'`. Under the intersection rule it would look like a reference
    with unequal types.
  - Implement the rejection in `_FunctionPodBase.output_schema`, for plain and referential
    tags, with the spec's message. A `PythonDataFunction`'s output schema is static, so
    §6.4's dynamic case does not arise.
- **Replacing plain data with no tag (§6.3).** Unchanged: a pod's outputs are new columns.

---

## 8. The spec's open questions (§17, §9.7)

| Question | Answer |
|---|---|
| Outer and unmatched-row joins (§9.7) | **The code decides:** none exist. `Join` and `MergeJoin` use `how="inner"` (`join.py:219`, `merge_join.py:393`), `SemiJoin` uses `how="semi"` (`semijoin.py:120`). Record the §9.7 policy in the operator contract for future ones |
| Rename both namespaces | **Decided:** `map_tags` on a referential tag renames both roles; the reference survives (§5.4) |
| Rename one side materializes? | **Decided:** `map_tags(..., tag_only=True)` and `map_data` leave a plain tag (zero-copy), with no warning (§5.4) |
| `Batch` | **The code decides:** every member column becomes a list in input order (`batch.py:60-100`). A reference is one column, so it is preserved (`02 P4`) |
| `MergeJoin` | §5.2; D4 |
| Avoiding duplicate stored values | **The code decides:** one physical column, and rows share buffers (`arrow_table_stream.py:352-353`). Option B, which the code already is; D3 asks only whether to add an explicit marker |
| Database columns referential by default | **Developer decided:** every declared tag of every source (§6.1) |
| `tags_as_data` migration | §6.3 |
| Which hashes change | §3: none in plain mode (golden-proven); additive for references; no data context version |
| Operators relying on unique tags per row | None require it. `GroupBy` documents the expectation and falls back to `record_id` (`group_by.py:71-81`, `:192-215`); `Join` is many-to-many. References do not change this |

---

## 9. Delivery plan (question 8), mapped onto the project's issues

The project has ITL-1009 to ITL-1023. Recommended order, with changes to the issue set:

| Order | Issue | Scope from this audit | Size | Depends on | Parallel with |
|---|---|---|---|---|---|
| 0 | **ITL-940** (add to the project) | reject a tag/data name collision across inputs (`Join`, `MergeJoin`, sync and async), pod output = tag name, `Index`/`Pick` `out=` = tag name; regression tests `01 R2`, `R5`, `05 M2` | M | — | — |
| 1 | **ITL-1009 + ITL-1010 (merge)** | `reference_names`, the validator, `StreamBase.reference_tags()`; `ArrowTableStream(reference_tags=)`; its identity term; `_rows_to_table` and `_materialize_*` deduplication; all 32 rebuild sites pass references; spec §15.1 tests | M | 0 | — |
| 2a | **ITL-1011 + ITL-1012, part 1** | builder `reference_tags`; `tag_only_columns` on every source (§6.2); `to_config` per D8; referential sources available but **not yet the default** | M | 1 | 2b-2e |
| 2b | **ITL-1015** | select and drop by role; config, diagnostic, `binding_transitions`; §15.5 tests | S | 1 | 2a, 2c-2e |
| 2c | **ITL-1016** | `Join` three cases, sync and async; field-level provenance per D5; §15.4 tests; `06 J1` pinned | M | 1 | 2a, 2b, 2d, 2e |
| 2d | **ITL-1017** | `GroupBy` key scalar in data; `Batch` tests; §15.3 tests | S | 1 | others |
| 2e | **ITL-1014** | tests only for the filters, `SemiJoin` and `DropNulls`; `Index`/`Pick` in-place rejection | S | 1 | others |
| 3 | **ITL-1013** | pod output collision (shares code with ITL-940); the D1 pod-boundary rule (plain under `process`); automatic join and select with references; the fall-through variant of the pod on `pod(...)` (§11) with its equivalence test; §15.2 and §15.6 result-identity tests | M+M | 1, 2b; 2c and 2d for the automatic join and the fall-through | 4 |
| 4 | **ITL-1019** | nodes, stores and saved jobs pass references; descriptor intersection; reuse across retagging (§15.6) | S | 1 | 3 |
| 4b | **ITL-1012, part 2 (its last step), with ITL-1011** | **flip the default** to referential; golden fixtures switched to `tag_only_columns`, new entries; upgrade note | S | **1013, 1015, 1016, 1017, 1019** | — |
| 5 | **ITL-1018** | `MergeJoin` (D4); the decided rename rules (§5.4), unless the ITL-1023 slice lands them | M | 1 | 6 |
| 6 | **ITL-1020** | `tags_as_data` beside references (§6.3): docs and tests; no behaviour change | S | 2a | 5 |
| 7 | **ITL-1021** | ediop and the notebook, without copies; EDI-shaped integration test (§15.7) | M | 4b | — |
| 8 | **ITL-1022** | operator table in `docs/`; `streams.md:10-25`, `operators.md:31-33`, `sources.md`, `hashing.md`; CLAUDE.md and `.zed/rules` rule | S | all | — |

**ITL-1023 (workshop slice, go/no-go November 14).** The thinnest path is ITL-1010 (+1009),
1012 (sources only), 1015, 1013, 1016 and 1017, plus guard errors ("not supported with
referential tags yet") in:
- `Batch`, `MergeJoin`, `Index`, `Pick` when an input has references (the rename rules of §5.4
  are in the slice);
- `OperatorNode` reload and `DerivedSource`;
- `pod.process`/`strict=True` with references (D2), if D2 is not settled.

Each guard is one check against `reference_tags()` at `validate_inputs`. Estimate: about
5-7 working days for one worker, which is tight for November 14. The audit's measurements
(`02`, `06`) give the test shapes. The riskiest part is the 32-site rebuild; it is mechanical
but touches every operator.

**The default flip is the last step of ITL-1012.** It depends on ITL-1013, 1015, 1016, 1017
and 1019. Flipped earlier, `main` would break: `pod(source)` (the automatic select raises),
`source_a.join(source_b)` (shared keys collide), `GroupBy` on a key, and reloading a node.
ITL-1023's integration branch carries everything at once and is unaffected.

**Splits and merges.** Merge ITL-1009 into ITL-1010: with the intersection rule ITL-1009 is a
helper and a validator, testable only with a stream. Land ITL-1011 with ITL-1012: the golden
fixture switch is the proof ITL-1011 asks for, and it needs `tag_only_columns`. ITL-1013
depends on ITL-1015 (the automatic select). Add ITL-940 to the project as the first issue.

**Blocked by other issues?**
- **ITL-858 (channels):** no. Rows carry both datagrams, and the intersection rule needs
  nothing else.
- **ITL-835 (Arrow types in `Schema`):** no. A reference is one column, so its Arrow types
  cannot disagree.
- **ITL-846 (metadata preservation):** no. The binding is not field metadata.
- **ITL-980:** unrelated.
- **ITL-940:** blocks step 1.

---

## 10. Decisions for the developer (most consequential first)

D0 is stated first, as asked. It is the consequence of the developer's default, for
confirmation. D1 is decided and recorded here; D2 onward are open.

**D0. The hash consequence of the default (measured, §3.3).** An unchanged v0.3 script gets:
- new source content and pipeline hashes, system-tag names, function-node record ids, and
  pipeline/operator/cached-source table paths;
- the same source ids, source record ids, result table paths, and results for every function
  that reads no key.

Plain mode is byte-identical, proven by the golden values with fixtures switched. No data
context version. To confirm:
- (a) **Recommended.** Ship as described. No migration tool; v0.3 tables are left in place,
  unread.
- (b) Also provide a one-off tool that re-keys v0.3 pipeline records to the new paths. The
  results are reused anyway, so this only saves re-establishing records.

**D1. A reference after a function pod: DECIDED (developer, November 12, 2026).** Required
§6.1 cannot hold as written, because a pod outputs only its function's outputs
(`function_pod.py:747`). The §6.1 example is conditional ("assuming the output stream
retains the input data `x`"), and in Orcapod it never does. The decision:
- **`pod.process(stream)` and `pod(stream, strict=True)`:** a reference whose data column
  does not survive becomes a **plain tag** (same value and type), visible in the output
  schema. No automation. The `strict` reading is the coordinator's; the developer may
  correct it.
- **`pod(stream)`:** a **variant of the function pod** (or a thin wrapper) processes one
  (tag, data) pair at a time. It calls the function exactly as the plain pod does, then emits
  the tag unchanged and, as data, the outputs plus the row's own referenced columns. Function
  results are shared with the plain pod. Only referential key columns fall through; full
  append is deferred (ITL-1025). See §11.
- **The diagnostic** is the coordinator's call. Recommended none at the pod boundary or in
  the automatic select: the `reference_materialization` warning is for a select or drop the
  user wrote (a rename never warns, §5.4).

**D2. `pod.process(...)` and `strict=True` with referenced keys (ITL-807).** Under the default,
every key is an extra data column for a function that does not read it.
- (a) **Recommended.** Keep ITL-807's contract: `process` and `strict` reject. `pod(...)` is
  the convenient form and materializes.
- (b) `process` and `strict` ignore referenced columns that are not parameters, materializing
  them like the automatic select. This is friendlier, but `process` would no longer be the
  exact primitive.

**D3. An explicit binding marker on `Schema`?** (§17 "avoid duplicated values" and
§13.1-13.2.)
- (a) **Recommended.** No. The binding is the name intersection (§4.3), with the build-time
  validator; `Schema` and every schema hash are untouched.
- (b) Add a `reference_fields` marker beside `optional_fields`. It is explicit, but it touches
  `Schema` equality and hashing (it must stay out of the hash when empty;
  `hashing/semantic_hashing/builtin_handlers.py:290-293` shows how `optional_fields` is left
  out) and every operator's schema code.

**D4. `MergeJoin` with references (§17 open).**
- (a) **Recommended.** An equated reference stays one scalar column; a reference colliding with
  a non-tag data name is rejected.
- (b) Reject referential inputs.

**D5. Field-level shape of the merged column's provenance (§9.4 says retain both; the code
needs a shape).**
- (a) **Recommended.** `_source_x` becomes a sorted two-element `large_list<large_string>`,
  as `MergeJoin` does. Downstream `GroupBy` then nests one level, as it does for `MergeJoin`
  output.
- (b) Two scalar columns, `_source_x` and a second name. This needs a new naming convention
  for source-info columns.

**D6. `ListSource` under the default.** Its tags are values the source computes
(`element_index`, or `tag_function`), not columns of the user's data. §11.1 speaks of source
*columns*.
- (a) **Recommended.** `ListSource` tags stay plain. Its golden entry is unchanged without a
  fixture edit, and the upgrade note says so.
- (b) Referential like every source, with a plain option added to `ListSource`.

**D7. The tag-only option: DECIDED** (developer, November 12, 2026). It is
`tag_only_columns=[...]`, a subset of `tag_columns` (§6.2). Open for the ITL-1012 worker to
propose: a shorthand for "all declared tags", useful for database sources whose tags default
to primary keys.
**D8. Version pinning of saved files (decided in principle by the developer; the mechanism
needs approval).**
- (a) **Recommended.** v0.4 bumps `PIPELINE_JOB_FORMAT_VERSION` and `PIPELINE_FORMAT_VERSION`
  with the default flip, and `"0.1.0"` loads with v0.3 tag meaning. v0.3.0 adds
  `orcapod_version` before its tag (informational). v0.4 adds `config_version` to standalone
  source configs.
- (b) Rely on the format-version bump alone. Files from the ITL-1023 integration branch
  (format `"0.1.0"`, referential tags) would then be misread as v0.3.

---

## 11. The two rules settled late (developer, November 12)

### B. `GroupBy`: no change

Spec §8.2 stands: a non-key referential tag becomes the same list in both roles and stays
referential, and plain tags behave as in v0.3 (§5). `Join` gets no special refusal for a
shared list-valued tag. `Join` matches same-named tags whatever their type, and streams
whose same-named tags mean different things must be renamed. What `Join` does today with a
shared list-valued tag (correct match, wrong match, or a join-engine error, sync and async)
is for the ITL-1023 worker to measure and report.

### A. Pod boundary: references fall through on `pod(...)`, as a variant of the pod

The developer's ruling: the fall-through is an abbreviated form of a legal composition, done
by a variant of the function pod. It is **not** a join and **not** an inserted operator.

**The composition it abbreviates** (each step obeys the boundary: no tag enters a function;
no operator makes data that is not existing data):
1. Add a unique temporary tag per row.
2. Send one copy through the plain pod. The automatic select makes the unread references
   plain; the read ones are consumed, and either way the pod's output has them as plain tags.
3. Join the pod's output back onto the input, restricted to its tags and referenced columns,
   on the temporary tag. This is `Join` case 2, plain with referential (§5.1), so the
   referenced data comes from the input's data column.
4. Drop the temporary tag.

**The variant, one (tag, data) pair at a time** (`pod(stream)`, the `__call__` path that
already adds ITL-807's automatic join and select):
- **The function sees exactly what the plain pod gives it.** The variant projects the row's
  `Data` to the function's parameters and runs the plain pod's per-row path on that
  projection. The memo key (`result_cache.py:217`) and the result store (`rdb_v2` at the
  function uri) are therefore the plain pod's, so results are shared.
- **What it emits.** The input tag unchanged (system tags included, as the plain pod does,
  `function_pod.py:1043-1054`). As data: the function's outputs plus the row's referenced
  columns, with their `_source_*` taken from the row's `Data`.
- **What carries the referenced columns.** ITL-807's automatic select must keep referenced
  columns in the variant's input. Only non-referenced extra columns are dropped.
- **Rows.** Row count and order are the pod's: a row whose function returned nothing is
  skipped, as in the plain pod. There is no real join, no extended system-tag name, and no
  row multiplication from duplicate user tags.

**Consequences:**
- **Output schema.** Data = outputs ∪ referenced input columns. Tags = input tags. The
  references of the output = those of the input.
- **A function output named like a referenced column (or any tag)** still raises (§7).
- **Pipeline identity.** It differs from the plain pod's **only when references are
  present**. Add the term `("reference_fall_through", sorted(names))` to the pod stream's or
  node's identity only when the set is non-empty (the #336/#338 pattern). With no referential
  tag in the input, the variant is indistinguishable from the plain pod: same schema, same
  hashes, same node path and stored tables, no golden move.
- **Function-result identity and the result store.** Shared with the plain pod. A retagged
  pipeline, or a `process` call over the same rows, finds the same results.
- **`FunctionNode` and the job path.**
  - The pipeline record (`pdb_v2`, `function_node.py:2337-2461`) stores the row's tags,
    which include `x`, plus the input's `_source_*` columns (`:2390-2396`).
  - For the variant those must be the `_source_*` of the full input `Data`, referenced
    columns included, not only the projection's, so `_source_x` is kept.
  - The value of `x` is stored once, as the tag column. On read the node gives that column
    its data role back (`reference_tags` = the input's references, `function_node.py:2891`).
  - Nothing is stored twice, and the table is at its own `pipeline_hash` path whenever
    references are present.
- **`strict=True`.** It uses the plain pod, as `process` does (the coordinator's reading of
  ITL-807; the developer may correct it).
- **Size: M**, in ITL-1013, after ITL-1016, because its equivalence test uses `Join` case 2.

**Tests:**
- **Equivalence.** On a source with duplicate user tags (`session=s1` on two rows, `x`
  referential, `y` data), compare `pod(source)` with the composition above, built with a
  temporary tag-only row-id column, `pod.process`, `Join` and `DropTagColumns`. User tags,
  data, references and rows must match. System-tag *names* differ by construction (the
  composition's `Join` extends them), so compare those separately: the variant keeps the
  pod's names.
- **No references.** `pod(source)` over a tag-only source equals today's `pod(source)` in
  schema, `pipeline_hash`, `content_hash`, node path and stored tables.
- **Result sharing.** Run `pod(source)` and then `pod.process(source)` (or the reverse) over
  one result store. The second run computes nothing.
- **Job path.** Saved, reloaded and read back, the variant node gives the same table with
  `x` referential. Both sync and async.

---

## Appendix — out-of-scope findings (reported to the coordinator, not fixed)

1. **Async `Join` of tag `x` with data `x` from another input** yields rows where the two
   differ, silently. Sync raises `KeyError` on read (`01 R2`). This extends ITL-940 to async.
2. **`Index(out=<tag name>)`** is accepted at build and fails on read (`05 M2`). This extends
   ITL-940.
