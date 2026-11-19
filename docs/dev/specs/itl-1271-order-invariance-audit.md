# Order invariance: what depends on the order of tags, columns, argument items and inputs (ITL-1271)

Status: audit measured on `origin/main` d81eec8d (2026-11-18); the two findings fixed by this
PR are marked "fixed here". The permanent test is `tests/test_core/test_order_invariance.py`
and the exception list `tests/test_core/order_rules.py`. The script is `docs/dev/scripts/itl_1271_measure_order.py`; it builds on the
inputs of `tests/test_core/operators/several_items.py` (ITL-1236) and on the ITL-1246
measurement (`docs/dev/specs/itl-1246-data-tag-identity-order.md` on that branch).

Developer rule (2026-11-18): "every operator outcomes and all streams are not sensitive
to tag / column orders fed into them UNLESS it is somehow explicitly expected" and "if
any operations COULD vary output by tag order, then explicit ordering of tags to be used
should be an explicit input into the function/operator".

## What was measured

Inputs: `MAIN` (tags `t1` str, `t2` int, `t3` int; data `d1` int, `d2` float with nulls,
`d3` str), the join partners `P` (`t1`, `t2`) and `Q` (`t2`, `t3`), `MAIN_OTHER` (the same
columns, other values, for `MergeJoin`) and `NESTED` (`d4` `dict[str, int]`, `d5`
`list[int]`, for `Pick` and `Index`).

Axes, each measured with the others held:

- **tag order**: the declared `tag_columns` in three orders (declared, reversed, one
  rotation);
- **layout**: the physical column order of the table (tags first; data first and
  reversed; interleaved);
- **roles**, measured separately (never compared with each other): all plain tags, all
  data-tags, mixed (`t2` plain, the others data-tags);
- **argument order**: every permutation of the items of every multi-item argument
  (`by`, `columns`, mapping entries, keyword items, `tags_as_data`, `tag_only_columns`,
  `record_id_columns`, a raw stream's `data_tags`);
- **input order**: every permutation of the inputs of an operator that declares itself
  commutative (`Join` with three inputs, `MergeJoin`), a function pod over two inputs, and
  `Join` inside a job.

Compared (`F` = hash, identity or stored value: freeze relevant; `P` = presentation):

- `F` content hash, pipeline hash, the operator's own identity, system-tag column names,
  `output_schema()` as sets, the rows as a multiset: values, system tags (record ids),
  tag hash, data hash, source info; the async rows (equal to the sync rows, and
  across orders); a source's `source_id` (given and default);
- `F` in a `PipelineJob` (in-memory database; sync run and async run): every node's
  content and pipeline hash, stored table paths, stored rows (columns that differ between
  two identical runs, such as timestamps, are left out), the rows of the function,
  join, group-by and side-effect nodes, `DerivedSource` over a node (`node.as_source()`):
  content hash, pipeline hash, `source_id`; the saved job file; a loaded job's node
  hashes and its function node's stored rows;
- `P` `keys()` order, `as_table()` column order, the stored column order, and which
  rows `head(3)` sorted by tags shows.

A function result's source-info token holds a random record id (uuid7), new on each
computation; the script masks it, as it is not an order effect.

## Result: freeze-relevant elements that depend on an order

| Subject | Depends on | Elements | Owner |
|---|---|---|---|
| `GroupBy("t1")` (two non-key tags) | tag order | member lists: values, folded system tags, row hashes, source info; sync and async | ITL-1226 (PR #434) |
| `GroupBy(["t1", "t2"])` | `by` order | operator identity, content and pipeline hash | ITL-1226 (PR #434: `by` order is not identity) |
| `TagBy(["d3", "d1"])` | names order | operator identity, content and pipeline hash | ITL-1226 (PR #434: sorted) |
| `AddTags({"x": 1, "y": "z"})` | keyword order | operator identity, content and pipeline hash | ITL-1269 (landed in PR 457, 2026-11-19: order free) |
| `PolarsFilter(predicates=[...])` | predicate order | operator identity, content and pipeline hash | ITL-1247 (rule undecided) |
| `pod(source)` over data-tags (data-tags, mixed) | tag order | content and pipeline hash | ITL-1246 |
| `pod(a, b)` over data-tags (automatic join) | tag order **and layout** | content and pipeline hash | ITL-1246 (the fall-through tuple takes the joined stream's tag order, which follows the layout) |
| `PipelineJob`: function node over data-tags | tag order | node content and pipeline hash, stored table paths, `pdb_v1` stored rows, loaded node hashes, `node.as_source()` content hash and `source_id`; sync and async | ITL-1246 |
| raw `ArrowTableStream` (every role) | tag order | content hash | ITL-1227 |
| `PollingSource` (found by the permanent test) | tag order | content hash (identity held `tag_columns` in declared order) | **fixed here** (sorted; 0 pinned entries move) |
| `ArrowTableSource(record_id_columns=...)` | key order | content hash, record ids, source info | intended: the key order is the explicit argument |

Nothing else freeze-relevant depends on any order. In particular, in every role, layout,
argument order and input order:

- every source kind measured: `ArrowTableSource`, `DictSource`, `DataFrameSource` (each
  with a given and a default `source_id`), `ListSource`, `CSVSource` (default location
  id), `DeltaTableSource`, `SQLiteTableSource` (primary key declared in one order),
  `CachedSource`, `DerivedSource(tags_as_data=...)` (mapping order too), `DerivedSource`
  plain over plain-tag nodes; `tag_only_columns` order;
- `Join` (three inputs, all six input orders), `MergeJoin` (both input orders),
  `SemiJoin`, `MapTags`, `MapData`, `MapColumns`, `Batch`, `GroupBy(["t1", "t2"])`'s
  rows, `SelectTagColumns`, `SelectDataColumns`, `DropTagColumns`, `DropDataColumns`,
  `SelectColumns`, `DropColumns`, `PolarsFilter(constraints=...)`, `DropNulls` (named and
  all), `Pick`, `Index`, `TagBy`'s rows, `AddTags`'s rows;
- `pod.process(...)` (no data-tags pass through), `SideEffectPod` outside a job;
- in a job: the operator nodes (`Join`, `GroupBy`, `TagBy`, `Batch`, `AddTags`, with a
  `LOG` cache: `odb_v1` paths and rows), the side-effect node, `Join`'s input order, the
  async run equal to the sync run; a raw stream's `data_tags` order.

**One new freeze-relevant violation was found, `PollingSource`, and fixed here** (golden
`--check` on the fix: every group matches). Each other one is owned by an open issue.

## Result: presentation only

| Element | Depends on | Owner |
|---|---|---|
| `keys()` and `as_table()` column order of every source and operator | tag order (the declared `tag_columns`) | intended: placement follows the declared order (ITL-1205 rule) |
| the same | layout | ITL-1249 |
| `AddTags` column placement | keyword order | ITL-1269 (landed in PR 457: new tags placed in name order) |
| `DerivedSource(tags_as_data=...)` column placement of the new data columns | mapping order | **fixed here**: copies in the order of their new names (no hash moves) |
| rows `head(n)` / `as_table(sort_by_tags=True)` / preview show | tag order; for joins and pods also layout | ITL-1270 (and ITL-1249 for layout) |
| stored column order of `pdb_v1` and `odb_v1` | tag order and layout | presentation: a Delta store written by two tag orders reads back by name (checked: two runs, one path, all rows) |
| saved job file: a bound source's `tag_columns`, a `Join` node's input order | as declared | presentation: the file records the declaration; a load rebuilds the same hashes |

## Not measured

- `PostgreSQLTableSource`, `SpiralDBTableSource`: need a server; not run.
- `SourceProxy` (reads nothing), a generic `DBTableSource` (`SQLiteTableSource` is one).
  `PollingSource` was measured by the permanent test (above).
- Function pod `output_keys` order: decided and tested by ITL-1244 (PR #445).

## Per operator: identity, history, pins, frozen or provisional (proposed)

Identity: the operator's `identity_structure()`; for every exported operator
`pipeline_identity_structure()` is the same. A dict hashes free of key order; a list or
tuple keeps its order. Inputs enter through `argument_symmetry`: `frozenset` (order-free)
for `Join` and `MergeJoin`, a `tuple` for every other operator. Golden pins: the entries
of the golden cases that build the operator (`tests/test_hashing/golden/cases.py`; an
entry downstream of the operator moves with its identity), counted on `origin/main`
(d81eec8d). Criterion (developer, 2026-11-18): provisional when the identity, or the
output for an existing input, is likely to change; frozen when the identity is a plain,
order-free statement of the arguments and the semantics have no open question.

| Operator | Identity today (exact) | Added | Open or recent issues on identity or output | Golden pins | Recommendation |
|---|---|---|---|---|---|
| `Join` | `"Join"`; inputs `frozenset` | 2025-06 | ITL-1251 (float keys `0.0`/`-0.0`/NaN; hash part deferred to the next context), ITL-1117 (collapse identical system tags: would rename system-tag columns), ITL-1016 (data-tags), ITL-1051 (opt-in null match), ITL-855; ITL-829 key nullability done 11-13 | 179 (`join_pod`, `job`, `join_key_nullability`, `polars_filter_expr_job`, `default_source_ids`) | FROZEN: identity is the class alone, order-free, and measured order-free; ITL-1117 must be decided "no" (or v2) before 0.4.0 |
| `MergeJoin` | `"MergeJoin"`; inputs `frozenset` | 2026-03 | ITL-1241 (tied values: source-info list follows argument order, a stored value), ITL-1243 / ITL-1146 (raises on a null), ITL-839 (raises on struct), ITL-1018 (data-tag rules open), ITL-992 | 57 (`merge_join`, `join_key_nullability`) | PROVISIONAL: output for an existing input (ties) is open, and two input kinds still raise |
| `SemiJoin` | `"SemiJoin"`; inputs `tuple` (non-commutative) | 2025-06 | ITL-1088 (match keys = shared tags only, 11-13; ignored-name warning may become an error), ITL-1014 (data-tags), ITL-829 | 53 (`semi_join`, `join_key_nullability`) | FROZEN (borderline: rule from 11-13): an error for ignored names changes no accepted output |
| `MapTags` | `("MapTags", name_map: dict, drop_unmapped, strict)` + `(("tag_only", True),)` only when set | 2025-06 | `strict` ITL-1160 (11-16), `tag_only` (11-17, data-tags), ITL-1242 (data-tag swap refused), ITL-1018 (data-tag rename rules), ITL-833, ITL-1086, ITL-1096 | **0** | PROVISIONAL: two arguments under two weeks old, data-tag rules open, nothing pinned |
| `MapData` | `("MapData", name_map: dict, drop_unmapped, strict)` | 2026-05 (rename) | `strict` ITL-1160 (11-16; only refuses), ITL-833 (predicted schema), ITL-1018 | 57 (`map_data`, `drop_nulls_job`) | FROZEN: plain order-free identity; `strict` only refuses |
| `MapColumns` | `("MapColumns", name_map: dict, drop_unmapped, strict)` | 2026-11-16 | refused with data-tags (ITL-1022) | 24 (`map_columns`) | PROVISIONAL: two days old, data-tag behaviour undefined |
| `Batch` | `("Batch", batch_size, drop_partial_batch)` | 2025-06 | ITL-1017 (data-tag and its data column aggregated as one, In Progress), ITL-1248 (float32 signaling NaN quieted), ITL-1148; tags kept list-valued ITL-1070 (11-13) | 9 (`batch`) | FROZEN if ITL-1017 lands before 0.4.0, else PROVISIONAL (ITL-1017 changes the output for data-tag inputs) |
| `GroupBy` | `("GroupBy", by: tuple in the order given)` | 2025-06 | ITL-1226 / PR 434 (`by` order out of identity, new `member_order`), ITL-1234 / PR 441 (NaN keys), ITL-1250 (sub-precision keys), ITL-1251, ITL-1017, ITL-1071, ITL-1248, ITL-1090 | 9 (`group_by`) | PROVISIONAL: identity changing now, a new argument, four open output questions |
| `SelectTagColumns` | `("SelectTagColumns", sorted names: list, strict)` | 2025-09 | ITL-989 (data context), ITL-1015 (data-tag materialization diagnostic) | 46 (`column_selection`) | FROZEN |
| `SelectDataColumns` | `("SelectDataColumns", sorted names: list, strict)`; also the automatic select of `pod(...)` | 2026-05 (rename) | ITL-989, ITL-1015 | 59 (`column_selection`, `merge_join`; plus every case whose `pod(...)` call drops a column) | FROZEN |
| `DropTagColumns` | `("DropTagColumns", sorted names: list, strict)` | 2025-08 | ITL-989, ITL-1015 | 46 (`column_selection`) | FROZEN |
| `DropDataColumns` | `("DropDataColumns", sorted names: list, strict)` | 2026-05 (rename) | ITL-989, ITL-1015 | 46 (`column_selection`) | FROZEN |
| `SelectColumns` | `("SelectColumns", sorted names: list, strict)` | 2026-11-13 | ITL-1084 (done) | 36 (`tag_and_data_columns`) | FROZEN (borderline: 5 days old): the union of the four settled selections |
| `DropColumns` | `("DropColumns", sorted names: list, strict)` | 2026-11-13 | ITL-1084 (done) | 36 (`tag_and_data_columns`) | FROZEN (borderline, as `SelectColumns`) |
| `PolarsFilter` | `("PolarsFilter", predicates: tuple in the order given, constraints: dict)` | 2025-09 | ITL-1247 (predicate order), ITL-1266 / PR 450, ITL-1265 | 76 (moved to provisional by PR 450) | PROVISIONAL (already, PR 450) |
| `DropNulls` | `("DropNulls", sorted unique names: tuple)`, or `("DropNulls", None)` for all | 2026-11-12 | ITL-976 (async drops `__` meta in a tag), ITL-1014 (data-tags) | 109 (`drop_nulls`, `drop_nulls_all`, `drop_nulls_job`) | FROZEN (borderline: 6 days old): reads validity only, plain sorted identity, heavily pinned |
| `AddTags` | `("AddTags", ((name, value), ...) in keyword order)` | 2026-11-17 | ITL-1269 (landed in PR 457, 2026-11-19: identity sorted, tags placed in name order), function form future | 32 (`add_tags`; ITL-1269 moves them) | PROVISIONAL: one day old, identity about to change |
| `Pick` | `("Pick", column, key, out)` (`fail_on_miss` not in identity) | 2026-06 | ITL-439 (miss policy open: changes the output for a miss), ITL-850, ITL-1165, ITL-985, ITL-1014, ITL-440/441 | 18 (`index_pick`) | PROVISIONAL: the miss policy is an open question on output |
| `Index` | `("Index", column, i, out)` (`fail_on_miss` not in identity) | 2026-06 | ITL-439, ITL-866 (list-valued source-info token stringified: a stored value), ITL-850, ITL-1165, ITL-985 | 18 (`index_pick`) | PROVISIONAL: as `Pick`, plus a stored token known wrong |
| `TagBy` | `("TagBy", names: list in the order given)` | 2026-11-17 | ITL-1226 / PR 434 (names sorted) | **0** | PROVISIONAL: one day old, identity changing, nothing pinned |

## Intended order dependence (exception list)

As approved (2026-11-18); the one list is `ORDER_EXCEPTIONS` in
`tests/test_core/order_rules.py`, rendered in `docs/concepts/identity.md`.

1. `record_id_columns` key order (every source; the primary key order of a database
   table): the record id is the canonical JSON of the key columns in key order.
2. `GroupBy`'s `member_order` (PR #434): the explicit order of the members.
3. Input order of a non-commutative operator (`SemiJoin`; `argument_symmetry` returns a
   tuple).
4. Row order where record ids are positional (no key columns): out of scope by decision.
5. `Batch` groups consecutive rows: row order, not column order.
6. The fields of a user class, in declared order: reordering them declares another type.
7. Column placement follows the declared `tag_columns` order and the table's data column
   order (presentation only; its consistency across layouts and paths is ITL-1249).
