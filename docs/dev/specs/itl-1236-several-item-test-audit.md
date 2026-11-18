# ITL-1236: tests of every argument that takes several items

Status: inventory of 2026-11-17 (phase 1), on `main` at 4da42681. Kept up to date as the
gaps are closed.

## Why

`GroupBy` was tested with one tag and one data column. That hid a result that depended on
the order of the input's tag columns (ITL-1226). The same pattern can hide a defect in any
argument that takes several items: column names, a mapping, several input streams.

## The rule

Every argument or input that can take several items is tested:

- **(a)** with two or more items;
- **(b)** with the items in more than one order, with an assertion of what the order does or
  must not do (rows, output schema and column order, content hash, pipeline hash,
  system-tag column names);
- **(c)** in each role the argument accepts: plain tag, data, data-tag;
- **(d)** on inputs with two or more tag columns AND two or more data columns, in more than
  one arrangement (tag order and table layout);
- **(e)** on the sync and the async path.

A test whose streams have one tag and one data column does not test an operator.

## How the inventory was made

1. Every test that uses the argument was found by searching for the class and the stream
   method (`grep -rn` over `tests/`), and read.
2. A probe ran every operator on one stream, three tags and three data columns, in 18
   arrangements (6 tag orders x 3 layouts), for plain tags, data-tags and a mix. It compared
   rows, row hashes, system tags, source info, content and pipeline hashes. A second probe
   compared each argument given in two orders. On `main` only `GroupBy` depended on the
   arrangement (ITL-1226, PR #434). The other differences found are the defects below.

Legend: **yes** (with the best test as evidence), **no** (with what the closest test does),
**n/a**. Paths: `ops/` = `tests/test_core/operators/`, `dt/` = `tests/test_core/data_tags/`,
`ASE` = `ops/test_async_sync_equivalence.py`, `WHF` = `tests/test_hashing/test_wrong_hash_findings.py`,
`NEW` = `ops/test_several_items.py` (this issue).

## Inventory

### Operators

| Argument | (a) several | (b) order | (c) roles | (d) 2+ tags × 2+ data, arrangements | (e) sync / async |
|---|---|---|---|---|---|
| `Join` inputs, 2 | yes: `ops/test_operators.py::TestJoinBehavior::test_join_combines_streams_on_shared_tags` | yes: `ops/test_join_tag_order.py::test_operand_order_does_not_change_columns`; hashes `TestArgumentSymmetryIdentity::test_join_swapped_inputs_same_*` | yes: ASE `join_2`, `ref_join_2`, `ref_join_2_plain_left/right` | no (one arrangement) → NEW `test_join_result_does_not_depend_on_the_arrangement_of_its_inputs` | yes: ASE `join_2`, `join_2_reversed` |
| `Join` inputs, 3 | yes: `TestJoinSystemTagCanonicalOrdering::test_three_way_join_produces_six_system_tag_columns` | partial: system-tag names for 3 of 6 permutations; `dt/test_merged_source_info.py::test_join_token_does_not_depend_on_input_order`. No content/pipeline hash → NEW `test_join_result_does_not_depend_on_the_order_of_its_inputs[join_3/*]` (all 6) | yes: ASE `join_3`, `ref_join_3` | no → NEW | yes: ASE `join_3`, `join_3_rotated` |
| `Join` inputs, 4+ | async only: `tests/test_channels/test_native_async_operators.py::TestJoinNativeAsync::test_four_way_join` | no → NEW (all 24 permutations) | no → NEW (plain, data-tags, mixed) | no → NEW | no sync comparison → NEW |
| `Join` shared keys, several | schema only: `tests/test_pipeline/test_zero_row_column_order.py` step `join` | no (keys never in another order in one input) → NEW (tag orders per input) | plain, data-tag | no → NEW | schema only → NEW |
| `MergeJoin` colliding columns | yes: `ops/test_merge_join.py::TestMergeJoinBasic::test_values_sorted_independently_per_column`; ASE `merge_source` | left/right only, distinct values → NEW `test_merge_join_result_does_not_depend_on_the_order_of_its_inputs`; ties: **defect ITL-1241** | yes: ASE `merge_join`, `ref_merge_join_*` | no → NEW | yes: ASE |
| `MergeJoin` nullable colliding column with a null | no | n/a | n/a | n/a | n/a: **ITL-1243** (raises `TypeError`) → NEW xfail |
| `SemiJoin` match keys | rows with 1 key only (`ops/test_semijoin_tag_keys.py::test_semijoin_keys_on_shared_tags_only`) | no → NEW `test_semijoin_matches_on_every_shared_tag_in_any_arrangement` | yes: plain, `ref_semijoin`, mixed | no → NEW | 1 key: `test_semijoin_tag_keys.py::test_async_rows_equal_sync_rows` → NEW (2 keys) |
| `Batch` (tags and data become lists) | yes: ASE `batch_2` (2 tags, 9 data) | n/a (no column argument); member order is identity: `WHF::TestBatchGroupByFold::test_member_order_is_identity` | yes: `dt/test_batch.py` | no → NEW `test_unary_result_does_not_depend_on_the_input_arrangement[batch*]` | yes: ASE |
| `GroupBy` `by`, member order | yes: `ops/test_group_by.py::TestGroupByShape::test_one_row_per_distinct_key` | keys only on `main` | yes | no on `main` | 1 key on `main` |
| ↳ covered by PR #434 (ITL-1226) | | `test_group_by_member_order.py::test_the_order_of_by_is_not_identity` | plain, data-tag, mixed | `test_no_arrangement_of_the_input_changes_the_result` (24 × 4, source and raw stream) | `test_a_job_gives_the_same_rows_sync_and_async_for_any_arrangement` |
| `TagBy` columns | yes: `dt/test_tag_by.py::test_named_data_columns_become_data_tags_in_the_order_given` | yes: `test_the_order_of_the_names_is_part_of_the_identity` (PR #434 reverses this decision) | n/a / yes / refused: `test_an_existing_data_tag_is_refused` | no (1 tag) → NEW `[tag_by/*]` | yes: ASE `tag_by`, `tag_by_reversed` |
| `AddTags` tags | yes: `ops/test_add_tags.py::TestValues::test_arrow_type_of_each_accepted_value_type` | operator hash and keys: `TestIdentity::test_order_is_part_of_the_identity`; stream hashes → NEW `test_add_tags_order_is_the_order_of_the_new_tags_and_its_identity` | yes: `dt/test_add_tags.py` | no → NEW | yes: ASE `add_tags`, `ref_add_tags` |
| `SelectTagColumns` columns | **no** (only one name, or all) → NEW | **no** (`WHF::test_tag_column_spellings_share_identity` uses one name) → NEW | yes: `dt/test_roles.py::test_each_operator_acts_by_role` | no → NEW | yes |
| `DropTagColumns` columns | **no** → NEW | **no** → NEW | yes | no → NEW | yes |
| `SelectDataColumns` columns | yes: ASE `select_data_columns` | hash: `WHF::TestColumnSelectionIdentity`; keys, rows → NEW | yes | no → NEW | yes |
| `DropDataColumns` columns | yes: ASE `drop_data_columns` | hash only → NEW | yes | no → NEW | yes |
| `SelectColumns` columns | yes: `ops/test_tag_and_data_columns.py::test_select_columns_keeps_only_the_named_columns` | yes (keys and pipeline hash) | yes: `dt/test_operators.py::test_select_and_drop_columns_act_on_the_one_column_in_both_roles` | no → NEW | yes |
| `DropColumns` columns | yes: `test_drop_columns_drops_tags_and_data` | hash only → NEW | yes | no → NEW | yes |
| `MapTags` name_map | **almost none**: one chained rename with `tag_only` (`dt/test_operators.py::test_map_tags_tag_only_keeps_a_chained_rename_of_another_tag`) → NEW | **no** → NEW | yes; a swap of data-tags is refused: **defect ITL-1242** → NEW xfail | no → NEW | single rename only → NEW |
| `MapData` name_map | yes: `dt/test_operators.py::test_map_data_chained_rename_does_not_reach_the_data_tag_copy` | **no** → NEW (incl. a swap) | yes | no → NEW | single rename → NEW |
| `MapColumns` name_map | yes: `ops/test_mappers_strict.py::test_map_columns_renames_tags_and_data`; ASE `map_columns` | **no** → NEW | plain; refused with data-tags (ITL-1022): `dt/test_operators.py::test_uncovered_operators_raise` | no → NEW | yes |
| `PolarsFilter` predicates | **no** (always one) → NEW | **no**; the order IS in the identity (`filters.py` hashes an ordered tuple): **ITL-1247** | data | no → NEW | yes |
| `PolarsFilter` constraints | **no** (never two keys) → NEW | **no** → NEW (not in identity) | tag, data, data-tag | no → NEW | yes |
| `stream.polars_filter(*predicates, constraint_map, **constraints)` | **no** | no | — | no | sync only |
| `DropNulls` columns | yes: `ops/test_drop_nulls.py::test_tag_and_data_columns_in_one_call` | hashes: `test_identity_is_the_set_of_names`; rows → NEW | tag, data; a nullable data-tag holding a null: **no** | one layout → NEW | yes: ASE `drop_nulls_many` |
| `Pick` / `Index` | one item each (n/a); inputs with 2+ tags and 2+ data: `ops/test_index_pick_arrow_types.py::test_materialized_schema_matches_prediction` | n/a | data-tag with `out=`: **no**; ASE has no `ref_pick` | no | yes |
| Stream methods (`select_*`, `drop_*`, `map_*`, `join`, `semi_join`, `drop_nulls`) | partly (`test_stream_convenience_methods.py`, plain only) | no | plain only | no | sync only |

### Sources, function pods, binding

| Argument | (a) | (b) | (c) | (d) | (e) |
|---|---|---|---|---|---|
| `tag_columns` | yes: `sources/test_source_protocol_conformance.py::test_dict_source_multiple_tag_columns` (membership) | **no** source test (raw stream keys only: `dt/test_data_tag_order.py::test_data_tags_follow_the_tag_order`) | plain, data-tag | no | sync |
| `tag_only_columns` | True: `sources/test_tag_only_columns.py` (9 kinds, 1 tag); a list: `dt/test_error_messages.py::test_the_named_remedy_binds` | no (list order) | mixed: `dt/test_add_tags.py::test_a_mixed_stream_keeps_its_plain_tags_plain` | no | `dt/test_persistence.py` |
| `record_id_columns` | one composite key: `sources/test_stream_builder.py::TestKeyRowIdentifiers::test_record_id_and_token[composite]` | **no**: two key orders never compared (the order is in the identity, `sources/base.py`) | tag key only | no | no |
| schema arguments (`data_schema`, `schema`) | yes: `sources/test_large_normalization_sources.py` | **no**: schema always in the table's order | plain | no | no |
| `CachedSource` | 1 tag | no | **plain only** (data-tag read-back untested) | no | no |
| `DerivedSource(tags_as_data=)` | yes: `sources/test_derived_source_tags_as_data.py::TestColumns::test_several_copies_follow_map_order` | content hash only: `TestIdentity::test_mapping_order_does_not_change_content_hash` | plain; data-tag origin `dt/test_derived_source.py::test_tags_as_data_over_a_data_tag_node` | one arrangement | yes |
| function pod, several inputs | 2: `function_pod/test_function_pod_auto_select.py::test_call_selects_once_after_the_join`; 3: `dt/test_edi_end_to_end.py` | **no**: `pod(b, a)` never compared with `pod(a, b)` | data-tag parameter (EDI) | no | yes |
| `output_keys`, several | yes: `data_function/test_data_function.py::test_multiple_output_keys` | **no**; order is not in the identity: **ITL-1244** | — | — | value only |
| automatic select of several extra columns | yes: `test_dropping_logs_one_info_message_naming_the_columns` | no (kept list sorted) | one data-tag falls through | no | yes |
| `PipelineJob.bind(sources=)`, several | yes: `test_pipeline/test_pipeline_job.py::TestPipelineJobBind::test_bind_sources_mutates_job` (membership) | n/a (dict order unused) | plain only | — | — |
| bind a source in another column order | yes: `test_bind_type_check.py::test_columns_match_by_name_within_each_group_whatever_their_order` (plain, run ok only) | yes (plain) | **mixed roles: ITL-1245** (accepted, then the node's table breaks) | partly | async read in `test_bind_narrower_source.py::test_columns_follow_the_declared_order` |
| `ColumnConfig` groups | all_info: `tests/test_types_column_config_all_info.py`; each group alone: `test_zero_row_column_order.py` | n/a | data-tag variants | one arrangement | yes |

## Defects and questions found

| # | What | Reaches | Issue |
|---|---|---|---|
| 1 | `MergeJoin(A, B)` and `MergeJoin(B, A)` give the source-info lists of tied values in argument order | stored source info (operator cache, downstream tables); not the hashes | ITL-1241 |
| 2 | `MapTags` refuses a swap or cycle of data-tags that it accepts for plain tags (`MapData` is right: a data-only move) | error only | ITL-1242 |
| 3 | `MergeJoin` raises `TypeError` on a null in a colliding column | error only | ITL-1243 |
| 4 | Swapping `output_keys` keeps the function's uri, so a job serves stored values under swapped names | result cache: wrong values served | ITL-1244 (rule undecided) |
| 5 | Binding a source with mixed tag roles in another column order is accepted, then the source node's `as_table` raises | error on read; the run is right | ITL-1245 |
| 6 | A function pod over data-tags (`FallThroughPod`) hashes its fall-through tags in the input's tag order: two sources that differ only in tag order give the function node another content and pipeline hash | pipeline hash, pipeline table path | ITL-1246 |
| 7 | Input whose table layout differs from its tag order: every unary operator's result keys list the tags in the physical order while its rows use the tag order; Join, SemiJoin, MergeJoin (and, with data-tags, the tag select/drop and `MapTags` forms) give the async rows another column order than the sync rows | column order only; no hash | pending |
| Q1 | `PolarsFilter`: the order of the predicates is in the identity (and the stored path); the order of the constraints is not | pipeline hash | ITL-1247 (rule undecided) |

## Gaps, ranked

Order- or role-dependent behaviour that reaches a hash or a stored table first.

1. `output_keys` order: not in identity, stored values served under swapped names (ITL-1244).
2. `MergeJoin` tied values: source-info order follows the inputs (ITL-1241).
3. `PolarsFilter` predicate order in identity, constraint order not; never tested with two (ITL-1247).
4. `record_id_columns` key order: moves record ids, tokens and the content hash; never compared.
5. `Join` with 3 and 4 inputs: content and pipeline hash under every permutation; 4 inputs never run sync. Closed by NEW.
6. `MapTags` / `MapData` / `MapColumns`: several renames, mapping order, swaps, sync and async; `MapTags` has no golden entry. Closed by NEW (except golden: out of scope).
7. Column arrangement (d) never varied for any operator but `GroupBy` (PR #434). Closed by NEW for every operator.
8. Bind of a mixed-role source in another column order (ITL-1245); several sources with one refused.
9. `SelectTagColumns` / `DropTagColumns` with two or more names, in two orders. Closed by NEW.
10. `function_pod` over several inputs: `pod(b, a)` against `pod(a, b)` (hashes, system-tag names, stored rows).
11. `SemiJoin` with two or more keys, rows sync and async. Closed by NEW.
12. `DropNulls` on a nullable data-tag holding a null, sync and async.
13. `tag_columns` order on a source: keys follow, hashes and system tags must not.
14. `DerivedSource(tags_as_data=)` reversed: default `source_id` and pipeline hash.
15. `CachedSource` over a data-tag source; `Pick` / `Index` from a data-tag with `out=`; schema arguments in another field order; the stream methods' several-argument forms (`polars_filter` with `constraint_map` and keywords).

## Checked and found as documented (part 2)

- A source's `tag_columns` order and table layout: keys follow the tag order; rows, record
  ids, system tags, content and pipeline hashes do not change (plain, data-tags, mixed).
- `tag_only_columns` given in two orders: no change.
- `record_id_columns` in another key order: record ids follow the key order; the content
  hash changes, the pipeline hash and the source id do not; a set is refused.
- `DerivedSource(tags_as_data=)` reversed: only the new columns' order changes.
- `CachedSource` over a source of each role and arrangement: reads back as the source.
- `pod(a, b, c)` in every input order: same rows, hashes and columns; a job over the inputs
  in another order finds every result stored (sync and async). The in-memory output's
  source info names a fresh random record per computation, so comparisons leave it out.
- `bind(sources=)` with two sources: both bound, nothing moves; one refused, neither bound.
  A plain or data-tag source in any arrangement binds and reads; a mixed one is ITL-1245.

## Tests added

`tests/test_core/operators/several_items.py` is the shared family: `MAIN` (tags `t1`,
`t2`, `t3`; data `d1`, `d2`, `d3`) and join partners `P`, `Q`, `R`, each in any tag order,
three layouts and three role settings. `ops/test_several_items.py` uses it for every
operator (gaps 5, 6, 7, 9, 11 and the operator half of 1, 2, 3). Part 2:
`tests/test_core/sources/test_several_items.py` (gaps 4, 13, 14, 15: CachedSource),
`tests/test_core/function_pod/test_several_items.py` (gaps 1, 10; ITL-1244, ITL-1246) and
`tests/test_pipeline/test_bind_several_items.py` (gap 8; ITL-1245).
