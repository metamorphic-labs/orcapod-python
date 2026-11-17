# ITL-1023 / ITL-1017: merge main (ITL-1070 GroupBy drops non-key tags) into the slice

Coordinator task `task_9facbf9373cf`, PR #342 (draft), branch
`eywalker/itl-1023-workshop-slice-referential-tags-working`, base head `c78ba80a`, main `163e5ddc`.

The spec is the coordinator's task block: main's GroupBy rule wins.

## Rule after the merge

- `GroupBy(by)` keeps only its `by` tags. A non-`by` tag is dropped, plain or referential.
- A referential non-`by` tag `x` is one column in both roles, so it leaves the data group
  too, together with its source-info column `_source_x` (sync rows, schema prediction,
  `reference_tags` of the output stream).
- A referential `by` tag keeps the one folded token `merged:source_fold_v1:<hex>`
  (`arrow_utils.fold_source_info_tokens`), unchanged from `c78ba80a`.
- `Batch` unchanged (still raises "not supported with referential tags yet").

## Pinned at c78ba80a (scratch script, DictSource rows m1/2/0.7, m1/1/0.5, m2/1/0.9,
## tags subject, session (both referential), source_id "sessions", group_by(["subject"]))

- `_source_subject` = `['merged:source_fold_v1:0e4f6dafc12b16f4daf438baa263f3a2bcefba8c5248dbccc3bd0dfd9c242b13', 'merged:source_fold_v1:ec856d114f3bb89b2f626c9b1bc2a195e133da8c9b828d54f99a77dfa4332b40']`
- `v` = `[[0.5, 0.7], [0.9]]`, `_source_v` = `[['sessions::row_1::v', 'sessions::row_0::v'], ['sessions::row_2::v']]`
- At c78ba80a `session` was kept as a list in both roles; after the merge it must be gone.

## Steps

1. `git merge --no-commit origin/main`; conflicts in `group_by.py` (resolved as above) and
   `tests/test_core/operators/test_group_by.py` (main's version + `tag_only_columns=True` on
   every `ArrowTableSource`, as the slice did to the base version).
2. Run targeted tests (one pytest process, Python 3.12): group_by, chained aggregation,
   async/sync equivalence, aggregation job, referential_tags tests, golden hashes, docs
   samples for referential-tags/operators pages. Fix auto-merged tests from main that now
   build referential tags by default (add `tag_only_columns=True` where the test is about
   plain tags).
3. Remove slice code/tests that only served "non-key referential tag becomes a list";
   record each removal below.
4. New tests: GroupBy with a referential by-tag and a referential non-by tag, sync and async:
   schema prediction == `static_process(...).output_schema(...)`, non-by tag and
   `_source_<tag>` absent, by-tag token equal to the pinned value.
5. Golden: main's values must pass unchanged; list slice-added keys that changed and why.
6. Docs (`referential-tags.md` operator table), changelog fragment `ITL-1023.md`.
7. Commit merge, push, report head sha; worker_done when CI green.

## Status log

- Merge resolved. `group_by.py`: main's rule plus the slice's key token; a referential
  non-key and its `_source_<tag>` are dropped (rows, `output_schema`, `reference_tags`).
- Removed (served only "non-key referential tag becomes a list"):
  `test_operators.py::test_group_by_referential_non_key_is_the_same_list_in_both_roles`
  (replaced by `test_group_by_drops_a_referential_non_key_in_both_roles`);
  `test_merged_source_info.py::test_join_on_a_list_valued_reference_folds_element_by_element`
  (no operator can now make a referential tag with list-shaped source info; the generic
  shape rule of `merge_source_info` and its unit test stay); the docs sentence on a
  list-valued merged key; old `test_group_by.py::test_non_key_tag_keeps_its_arrow_type`
  (main removed it). Old slice `GroupBy` branch "non-key same list in both roles" gone.
- Rewritten: `test_edi_end_to_end.py`, `timestamp` declared data at the source,
  `map_data` instead of `map_tags`, function sorts the lists (members ordered by record id).
- Integration fixes for main's new code under referential-by-default sources:
  `side_effects._table_of_rows` shows a referential column once (ITL-1066 test failed with
  duplicate `k`); `test_provisional_hash_warning.py` sources `tag_only_columns=True`
  (saving a pipeline is not supported with referential tags yet).
- Golden: every golden test passes unchanged; no key changed.
- New: `tests/test_core/referential_tags/test_group_by_non_key_dropped.py` (4 of 5 fail with
  the c78ba80a `GroupBy`, all pass after).
