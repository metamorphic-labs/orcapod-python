# ITL-984 plan: second commit to a Delta table holding a list of a binary-stored type

Diagnosis: `.untracked/2026-11-12-itl-984-delta-second-commit-diagnosis.md` (copied from the
ITL-858 worktree) and the third Linear comment. Scope: `DeltaTableDatabase` only.

## Fix (src/orcapod/databases/delta_lake_databases.py)
1. `_stores_as_is`: reduce extension types to their storage types before `_as_delta_writer_stores`
   (logical list `list[uuid]` -> `large_list<large_binary>` -> Delta `list<binary>`), so new ids append.
2. In `_flush_by_key`, a batch that `_stores_as_is` but holds stored ids: drop the rows whose id is in
   `base.ids` and append the rest (no write if none is left). Chosen over a cast MERGE source because
   it keeps today's semantics (an id already stored is skipped, as `when_not_matched_insert_all`
   does; rows with a null id never match, so they are kept), writes the batch exactly as the first
   commit does (stored field metadata of logical columns kept), and does not read the stored table.
   The MERGE stays for batches that are not stored as-is (null-typed columns, schema evolution).

## Tests first (tests/test_databases/test_delta_list_binary_commits.py)
- pod `-> list[uuid.UUID]`: SyncPipelineOrchestrator().run(dag), AsyncPipelineOrchestrator().run(dag),
  second job.run() adding rows; stored table equals a single commit; read back has logical type.
- plain `list[bytes]` with an id already stored (add_records twice).
- regression: scalar uuid, `list[str]`.

## Finish
Golden suite unchanged, full suite, changelog.d/ITL-984.md, design_issues entry only if one exists.
PR body: ITL-871 (#300, tests/test_torch/test_tensor_pipeline_stores.py) / ITL-981 (#328) xfails.
