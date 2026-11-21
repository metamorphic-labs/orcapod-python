# Data-tag identities free of tag column order, then pinned (ITL-1246, ITL-1227)

Status: implemented. The Step 1 plan below was measured on `origin/main` d81eec8d
(2026-11-18). The measurement script is `docs/dev/scripts/itl_1246_measure_tag_order.py`.

**Decision update, part (b) (developer, 2026-11-18).** The row rule proposed in (b) below
(sort by tag names) was replaced. The developer's words: "the stream's strict identity
should not depend on the row order so making it canonical makes sense", and "I agree with
multiset of rows as the definitive way to sort".

A raw stream's identity now holds its rows as a multiset:

- Each row is hashed alone by the Arrow hasher, over the identity table's columns.
- The digests are taken with their label (`to_prefixed_digest`) and sorted into one
  `large_binary` column `row_hash`.
- There is no special case for no tags, tied tags, list tags or zero rows.

Its names are still sorted tuples. Questions F2 and F3 are moot.

Under this rule 7 pinned entries move:

- `schema:source_stream_builder.{default_source_id,named_source_id,record_id_column}.content_hash`
- `pipeline:list_tag_producerless_stream.{as_given,reversed}`
- `pipeline:tagless_producerless_stream.{as_given,reordered}`

They are regenerated only after the developer approves the list.

The alternative V2 was measured too: tags in name order, then the row digest as a
tiebreak. It moved only `pipeline:list_tag_producerless_stream.as_given`.

Cost: about 160 µs per row, against about 1 µs per row for one table hash. It is paid only
when a raw stream's content hash is asked for.

Issues filed from (f): ITL-1269 (F5, AddTags), ITL-1270 (F4, presentation sort) and ITL-1272
(keyed in-memory default source id). F6 is the existing ITL-1249.

Developer decisions this rests on:

- 2026-11-17: the order of a stream's columns never forms identity (ITL-1205, ITL-1226).
- 2026-11-18: data-tags are not provisional. Their identity is settled now and frozen with
  data context v1 at 0.4.0, so every data-tag identity gets golden pins.
- 2026-11-18: "it shouldn't depend on tag order -- generally nothing should depend on tag
  order -- if any operations COULD vary output by tag order (as in column order of
  operation), then explicit ordering of tags to be used should be an explicit input into
  the function/operator".

## Measurement

One table, three data-tags `a` (int), `b` (str), `c` (bool) and a plain tag `p`, data `v`,
`w`. It is built under every order of `tag_columns` (6) and every physical layout of the
data-tag columns (6), so 36 builds. For each element the script counts distinct values
over the tag orders (one layout), over the layouts (one tag order), and over all 36. 1
means the element does not depend on order.

On `origin/main`, only these elements have more than one value:

| Element | tag order | layout | all 36 |
|---|---|---|---|
| fall-through pod after an explicit select: content and pipeline hash | 1 | **6** | 6 |
| fall-through pod with the automatic select: content and pipeline hash | **6** | 1 | 6 |
| raw `ArrowTableStream` with data-tags: content hash | **6** | 1 | 6 |
| raw `ArrowTableStream`, plain tags: content hash | **6** | 1 | 6 |
| job: function node content and pipeline hash, table paths (sync and async) | **6** | 1 | 6 |
| job: saved `fall_through_tags` | **6** | 1 | 6 |
| `group_by("a")`: rows (member order) | 2 | 1 | 2 (ITL-1226, PR #434) |

Every other element has one value in all 36 builds:

- The source: content and pipeline hash, system-tag column names, rows.
- The fall-through pod's rows (tag hashes, data hashes, system tags).
- Join, SemiJoin, MergeJoin over data-tags, Batch, TagBy, MapTags of a data-tag,
  DropNulls: content and pipeline hash, system-tag names, rows.
- The job's record ids, the result table's input data hashes, the source and select nodes.

With the prototype below applied, every element has one value in all 36 builds, except
the `group_by` rows (#434 fixes those).

The layout-only dependence (first row) comes from operator output keys. Every operator's
output lists its tags in the order of the source's physical table, not in the source's
`keys()` order (question F6). The fall-through tuple takes that order.

## (a) ITL-1246: the fall-through pod

**Today.** `FunctionPod.__call__` (`core/function_pod.py:823-866`) takes
`data_tags = stream_data_tags(input_stream)`, which is the input's tag order. It wraps the
pod as `FallThroughPod(pod, data_tags)`. `FallThroughPod.__init__` (`:2016`) keeps
`tuple(fall_through_tags)` as given. Both `identity_structure` and
`pipeline_identity_structure` (`:2072-2076`) are
`(plain_pod, ("fall_through_tags", <ordered tuple>))`.

**Proposed.** The pod holds its data-tags as a set: `tuple(sorted(set(names)))`. The same
element goes into identity, pipeline identity, `to_config` and the saved descriptor. A
sorted tuple, not a `frozenset`: a pod whose input already lists its data-tags in name
order keeps today's hash. Sorted tuples are also the idiom of `DropNulls`,
`Select*`/`Drop*` and `tags_as_data`.

**Output column order.** The pod's output keeps the data-tags first, in the input's tag
order (the ITL-1205 rule). `output_schema` and `result_data_schema` place them from the
incoming tag schema. Per row, `with_fall_through` places them in the row tag's order. Column
order is presentation only.

## Every place a list of tag or data-tag names enters a hash, path or name

| Where | Enters | Ordered today? |
|---|---|---|
| `core/function_pod.py:2072,2075` `FallThroughPod` identity and pipeline identity | content and pipeline hash, so the function node's pipeline table path and every downstream hash and system-tag column name | **yes**, input tag order. Changed here |
| `core/function_pod.py:2040` `to_config`; `pipeline/graph.py:244` descriptor `fall_through_tags` | the saved job file only (not hashed); a load rebuilds the pod from it | **yes**. Written sorted here |
| `core/streams/arrow_table_stream.py:208-215` raw stream identity: `_tag_columns`, `("data_tags", ...)` | raw stream content hash | **yes**. Changed here (ITL-1227) |
| `core/streams/arrow_table_stream.py:237` `_identity_table` row sort by `_all_tag_columns` | raw stream content hash (row order is a table-hash input) | **yes**: user tags in tag order, then system tags in physical column order. Changed here |
| `core/streams/arrow_table_stream.py:244` raw pipeline identity `(tag_schema, data_schema)` | pipeline hash | no: a `Schema` hashes free of key order (checked: `{'a','b'}` equals `{'b','a'}`) |
| `core/sources/base.py:131` source identity: `output_schema()`, `source_id`, `_key_columns` | source content hash | schema no; key columns yes, but that is the explicit `record_id_columns` argument, so by design |
| `core/sources/base.py:185`, `core/nodes/source_node.py:520-530` | pipeline hash, source node identity | no (schemas) |
| `core/sources/derived_source.py:325` `tags_as_data` | content hash | no (sorted items) |
| `core/operators/group_by.py:138` `by` | content and pipeline hash | **yes**. #434 makes it name order |
| `core/operators/tag_by.py:89` names | content and pipeline hash | **yes**. #434 makes it sorted |
| `core/operators/column_selection.py` (all four `Select*`/`Drop*`) | content and pipeline hash | no (`_canonical_columns` sorts) |
| `core/operators/drop_nulls.py:122` | content and pipeline hash | no (sorted) |
| `core/operators/mappers.py:307,586,655` `name_map` | content and pipeline hash | no (dicts hash free of order) |
| `core/operators/add_tags.py:152` `tuple(items)` | content and pipeline hash (pinned: `pipeline/add_tags`) | **yes**, keyword order. Question F5 |
| system-tag column names `::{pipeline_hash}:{position}` (Join, MergeJoin), `::{pipeline_hash}` (Batch, GroupBy) | stored column names | no: canonical order by pipeline hash |
| `core/nodes/function_node.py:189` record-id preimage; tag and data row hashes | record ids, row hashes | no: `hash_table` ignores column order |
| `stream_builder.key_row_identifier` | source record ids | key columns in key order: the explicit argument, by design |
| `core/operators/join.py:96` `join_v1` fold of a data-tag's source info | stored source-info token | no (sorted tokens) |
| `arrow_utils.fold_system_tag_values` (`batch_v1`, `group_v1`) | folded ids | GroupBy member order is ITL-1226 (#434); Batch is row order, not tag order |

## (b) ITL-1227: the raw `ArrowTableStream`

**Today** (`arrow_table_stream.py:201-240`). The identity is
`("ArrowTableStream", identity_table, tag_columns)`, plus `("data_tags", data_tags)` when
the stream has any. Both name tuples are in tag order. `_identity_table` sorts the rows by
`_all_tag_columns`: the user tags in tag order, then the system tags in the order of the
physical table. A type Arrow cannot sort (a list tag) leaves the rows unsorted. A stream with
nothing to sort by raises, as on main.

**Proposed.**

- `tag_columns` and `data_tags` enter as sorted tuples.
- Rows are sorted by the user tags in name order, then the system tags in name order. Each
  key is ascending with nulls last (Arrow's default, unchanged). Rows tied on every tag keep
  their given order (stable sort, unchanged). A list tag still leaves the rows unsorted
  (unchanged, and pinned by `pipeline/list_tag_producerless_stream`).
- This is the rule #434 uses for GroupBy members: tags in name order, ascending, nulls
  last, then the system columns. #434 orders the system columns record id first, then
  source id. A raw stream's system-tag columns are arbitrary names, so here plain name
  order is the canonical rule.

**What the comment protects.** "THIS ORDER IS A HASH INPUT AND MUST NOT CHANGE" protects
the content hash of every producer-less stream: the row order it hashes has been that row
order since before ITL-1053. `tests/test_core/streams/test_identity_order_regression.py`
holds a literal copy of main's sort. This change is the approved pre-freeze exception once
the developer accepts the moved list. The comment and the regression test then pin the new
rule: name order, with the battery run in every tag order and system-tag order.

## (c) Moved golden entries: none

Measured with the full prototype applied: (a), plus (b) with sorted names and rows ordered
by tag names then system-tag names. The command was
`generate_golden_hashes.py --check`, without regenerating. It reported:
`golden values of semantic, schema, arrow, datagram, pipeline, torch match`. So **0
entries move**, in every group.

Why:

- (a) No golden case has a data-tag. All 25 source fixtures pass `tag_only_columns=True`,
  so no `FallThroughPod` is ever built.
- (b) The only pinned raw streams have zero user tags and one system tag
  (`tagless_producerless_stream`), or one list tag (`list_tag_producerless_stream`).
  With at most one name, sorting changes nothing.

Operators' internal producer-less results are wrapped in a `DynamicPodStream` with its own
identity, so their content hash is never pinned.

So no `--remove` is needed. The developer approves the rule (the pre-freeze exception for
the identity-table comment), not a moved list.

Prototype test run (`tests/test_core/data_tags`, the identity regression test, the saved
data-tag job test, `test_several_items.py`): 39 of 2070 fail.

- 36 are the regression test shapes with tags `("k", "j")` (not in name order) or system
  tags `("s2", "s1")`. These are expected; that test is rewritten to the new rule.
- 3 are output column order (`test_add_tags`, `test_tag_by` job x2). The prototype placed
  the fall-through tags in name order; the implementation keeps the input's tag order, so
  these 3 stay green.

## (d) Pins to add (all `pipeline` group, new cases, `--add` only)

New module-level fixtures go in `tests/test_hashing/golden/cases.py`. Nothing existing is
moved or renamed. Fixtures:

- `DATA_TAG_ROWS`, `DATA_TAG_SCANS`.
- `_data_tag_sessions()`: a `DictSource` with tags `["subject", "session", "site"]`,
  `tag_only_columns=["site"]`. That gives two data-tags and one plain tag, given out of name
  order.
- `_data_tag_scans()`: shares the `subject` and `session` data-tags.
- `golden_dt_score`: a function pod over one data column. DO NOT MOVE.

A new helper, `data_tag_summary`, builds on `stream_summary` and also pins each row's
`_source_<data-tag>` tokens, because source-info tokens are stored. `stream_summary`
itself is not changed, so no existing entry gains keys.

| Case | Pins |
|---|---|
| `pipeline/data_tag_source` | content and pipeline hash, system-tag names, record ids, data hashes, data-tag source tokens; the same source with `tag_columns` in two orders, pinned equal |
| `pipeline/data_tag_mixed` | a plain tag and two data-tags: summary, and the plain-tag-only form beside it (different) |
| `pipeline/data_tag_fall_through` | `golden_dt_score.pod(src)` in memory: pod and stream hashes, rows; two input tag orders, pinned equal; with and without the automatic select |
| `pipeline/data_tag_fall_through_job` | in a `PipelineJob`, sync: node content and pipeline hashes, table paths, pipeline record ids, result input data hashes. An async run pins the same values under `async.*` |
| `pipeline/data_tag_join` | Join, MergeJoin (colliding data column) and SemiJoin over data-tags: summaries including the `join_v1` source-info token |
| `pipeline/data_tag_group_batch` | `group_by` on a data-tag key and `batch(2)`: summaries (folded ids, `group_v1` token) |
| `pipeline/data_tag_tag_by` | `tag_by` of two data columns on a plain-tag source: operator and stream hashes |
| `pipeline/data_tag_map_tags` | `map_tags` renaming a data-tag |
| `pipeline/data_tag_raw_stream` | a raw `ArrowTableStream` with two data-tags and a plain tag, and two system tags, in two tag orders and two system-tag orders: content hash (equal), pipeline hash |
| `pipeline/data_tag_saved_job` | save the job of `data_tag_fall_through_job` and load it: the saved `fall_through_tags` and `tag_only_columns`, and the loaded nodes' content and pipeline hashes |

Each case is checked in tests (not only pinned): several orders, sync equal to async.

## (e) Interactions and order

- **#448 / #449** (function type id, signature). They move every function pod uri, the pod
  and node hashes, and the result paths. The pins that hold a function pod depend on them:
  `data_tag_fall_through`, `data_tag_fall_through_job`, `data_tag_saved_job`.
- **#434** (GroupBy member order, `by` in name order, TagBy sorted). `data_tag_group_batch`
  and `data_tag_tag_by` depend on it. **#441** (NaN keys) changes nothing for these
  fixtures (no NaN), but it shares `pipeline-02.json`.
- **ITL-1266** moves the PolarsFilter cases to a non-frozen file. None of my cases uses
  PolarsFilter. It changes the group's file layout, so `--add` must run on its result.

Proposed order:

1. Implement (a) and (b), tests and docs now. This PR moves no golden value.
2. When #434, #441, #448, #449 (and ITL-1266) are on `main`, merge `main` into this branch
   (a merge, not a rebase). Then add the new cases and run the generator's own `--add` on
   main's golden files.
3. Whoever lands second takes main's golden files and re-runs its own tool steps. Digests
   are never hand-merged.

The PR description proves, with a script comparing the union of entries before and after,
that every existing entry is byte-identical and the new ones are additions only.

## (f) Questions

**F1. Output order of the fall-through columns.** Should the pod's identity and config hold
the sorted set while its output keeps the data-tags in the input's tag order? Recommended:
yes. Presentation follows the ITL-1205 rule, and identity never depends on it.

**F2. System tags in the raw-stream row sort.** Should they also go in name order? Today
they follow the physical column order of the table. Recommended: yes; it is the same rule
and moves nothing pinned.

**F3. Ties and list tags in the raw-stream row sort.** Keep both as they are? Rows tied on
every tag keep their given order. A list tag leaves the rows unsorted, so a raw stream's
content hash depends on its row order there. Recommended: keep. A raw stream's identity is
its rows, and the list-tag behaviour is pinned (`list_tag_producerless_stream`).
Recursively sorting list tags, as `sort_by_tag_columns` does with Polars, would move
that pin.

**F4. The presentation sort follows tag column order.** `arrow_utils.sort_by_tag_columns`
backs `as_table(sort_by_tags=True)`, `head()`, the preview and `StreamView.sort_by_tags`.
It orders rows by the tags in tag order, then the system tags in column order. Which rows
`head(n)` shows and their order therefore depend on tag order. It is not a hash.
Recommended: name order (the same canonical rule) in a separate issue, or an explicit
`order=` argument on `sort_by_tags`.

**F5. `AddTags` identity holds its tags in keyword order.** It is `tuple(items)`, pinned in
`pipeline/add_tags`. The order only places the new tags, so it is presentation.
Recommended: as #434 does for `TagBy`, sorted identity and new tags placed in name order,
in a separate pre-freeze issue. Its `every_type` keys are not in name order, so 2 operator
entries and the stream entries of `pipeline/add_tags` would move and need their own list.

**F6. Operator outputs list tags in the source's physical layout order, not its `keys()`
order.** For a source with `tag_columns=["c", "b", "a", "p"]` over a table laid out
`a, b, c, p`: the source's keys are `c, b, a, p`. Select, DropNulls, MapTags, TagBy, Batch
and DropDataColumns all output `a, b, c, p`. After this PR nothing hash-relevant reads it,
but keys order is inconsistent. Recommended: a low-priority issue. A source's keys should
follow its table, or operators should follow keys. No hash moves either way.

**F7. The `core/data_tags.py` module docstring** still says data-tag identities "are not
frozen yet ... may still change". Recommended: in this PR, state that they are settled
(2026-11-18) and frozen with v1 at 0.4.0.

**F8. Explicit order arguments that stay in identity.** These are already consistent with
the principle: a source's `record_id_columns` (key order: record ids and source identity)
and GroupBy's `member_order` (#434). No change.

## Step 2 outline (after approval)

TDD. First the failing tests, then the code.

- Three data-tags in every order (6), mixed with a plain tag, on the sync and the async
  path, in memory and in a job: same hashes, table paths and saved descriptor for every
  order.
- Raw streams in every tag order and every system-tag order, plain tags and data-tags:
  equal content hashes.
- The regression test rewritten to the new rule.

Then:

- The code: `FallThroughPod`, the `ArrowTableStream` identity and `_identity_table`, and
  the output order helpers.
- Docs: `docs/reference/hashing.md` (fall-through pod identity, raw-stream identity), the
  data-tags concept page, `CLAUDE.md` and `.zed/rules`, a `design_issues` entry if one
  matches, and `changelog.d/ITL-1246.md`.
- Pins after the dependent PRs land, per (e).
