# ITL-1023 / ITL-1017: GroupBy keeps the data of a referential non-key tag

Coordinator task `task_dc0771547320` (dispatch `ctx_8d25db659cad`), PR #342 (draft), branch
`eywalker/itl-1023-workshop-slice-referential-tags-working`, base head `0bfd72a5`.

Corrects `2026-11-13-itl-1023-merge-main-groupby-drops-non-keys.md`, which (as briefed)
dropped a referential non-`by` tag in both roles. Developer decision, November 13, 2026:
"groupby should actually 'drop' referential tag (while keeping the data, obviously), if
referential tag did not participate in the 'by' key for groupby".

## Rule

- A referential tag NOT in `by` loses its tag role only. Its data column stays and is
  aggregated like every other data column: `list[T]`, one element per member, source info
  one token per member (`build_aggregated_table`). Not in the output's tag keys nor in
  `reference_tags`.
- A referential tag in `by`: unchanged (scalar key, one `merged:source_fold_v1` token).
- A plain non-`by` tag: dropped entirely (main, ITL-1070).
- Members are ordered by every non-key tag (plain or referential), then record ids, as on
  main; so the kept list of a referential non-key is ordered by its own values.
- `Batch` unchanged.

## Steps

1. Failing tests: replace `tests/test_core/referential_tags/test_group_by_non_key_dropped.py`
   with `test_group_by_referential_non_key.py` (sync, async, schema prediction vs
   `static_process`, pinned key token, kept list + per-member tokens ordered by value, plain
   non-key dropped). Fix `test_operators.py::test_group_by_drops_a_referential_non_key_in_both_roles`.
2. `group_by.py`: drop only plain non-keys; referential non-keys are member columns (their
   `_source_` too); schema prediction `list[T]` for every non-key data column.
3. `test_edi_end_to_end.py`: timestamp referential at the source, rename before grouping
   with `map_tags` (both roles), group by keys, no `map_data`, no sort in the function.
4. Docs table row in `docs/concepts/referential-tags.md`, docstrings, changelog fragment.
5. Targeted tests (one pytest process, 3.12), golden tests, ruff, basedpyright.
6. Merge origin/main if moved, push, send head sha, wait for CI.

## Status log

- (start) plan saved.
- Failing tests written: 8 failed before the fix (referential non-key kept as list data,
  per-member tokens, value order, schema prediction), plain non-key tests passed.
- `group_by.py`: only plain non-keys are dropped; a referential non-key and its `_source_`
  column are member columns (aggregated by `build_aggregated_table`); members still sorted
  by every non-key tag, then record ids; `reference_tags` = referential keys only; schema
  prediction `list[T]` for every non-key data column.
- EDI test restored: timestamp referential at the source, `map_tags` before `group_by`
  (renames both roles; after grouping it is data only, so the rename must come first),
  no `map_data`, no sort in the function; timestamp lists asserted in time order.
- Docs (operators.md GroupBy bullet + warning, referential-tags.md table row), changelog.
- Targeted run (3.12): 2933 passed, 13 xfailed; golden unchanged; ruff, basedpyright clean.
