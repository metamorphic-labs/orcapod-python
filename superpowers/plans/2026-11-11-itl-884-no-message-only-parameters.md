# ITL-884 — remove message-only parameters (developer review of PR #290)

**Review:** comment 4536812218 on `src/orcapod/databases/delta_lake_databases.py`
(`_read_delta_table`'s new `record_path`): a function must not receive a value used only to
build an error message; the caller, which has the context, wraps the error.

**Goal:** no helper in the PR diff takes a parameter it uses only for a message. User-visible
behaviour and every `StoreReadError` / `InputValidationError` message stay exactly as they are.

## Audit of the PR diff (`git diff origin/main...HEAD`)

| Function | Parameter | Used for | Action |
|---|---|---|---|
| `DeltaTableDatabase._read_delta_table` | `record_path` | message only (`_read_failed`) | Remove; restore main's signature. The function raises a module-private `_DeltaTableReadError` (backend error as `__cause__`) for a failed scan; its three callers catch it and raise `self._read_failed(record_path) from exc.__cause__`. A bad filter still propagates as is. |
| `DeltaTableDatabase._require_compatible_append` | `table_uri` | message only | Becomes `_incompatible_append_column(delta_table, batch)`, returning `(column, stored_type, incoming_type)` or `None`; `flush_batch` (owns `table_uri`) raises the `InputValidationError` with the same message. |
| `_store_error_to_raise` | `record_key` in the `(record_key, error)` pairs | message only (the notes) | Takes `list[Exception]` and only selects; `flush` adds the notes naming the other batches' keys. |
| `async_orchestrator._single_store_error` | `topo_order` | nothing: ranks by `node_label`, which `StoreReadError` lacks, so the first error in the group always wins | Remove the parameter and the ranking (and the `topo_order` pre-init in `run`); select the first store error in group order. Same behaviour. |
| `DeltaTableDatabase._read_failed` | `record_path` | builds the error (its job); called only by owners of `record_path` | Keep. |
| `_get_delta_table`, `_refresh_existing_ids_cache`, `get_all_records` | `record_path` | locating the table (work) | Keep. |
| `flush_batch` | `table_uri`, `abs_path_components` | write location (work) | Keep. |
| `StoreReadError.__init__` | `record_path` | public attribute; not in the message, not read in src/tests | Keep as error payload (coordinator approved). |
| `FunctionJobNode` / `_FunctionPodBase` `StoreReadError` handlers | `node_label`, `pod_label` | observer callbacks (work) | Keep. |

## Steps

1. Ask the coordinator to approve the table (repo review process). **Status: approved** (item 8 kept:
   payload for a caller that catches the error, same shape as `StoreWriteError` on PR #287).
2. Edit `delta_lake_databases.py`, `async_orchestrator.py`; restore
   `tests/test_databases/test_delta_table_database.py:564` to main's call.
3. Tests unchanged in behaviour: `tests/test_databases/test_delta_read_failures.py`,
   `tests/test_pipeline/test_store_read_failure.py`, full suite on 3.11–3.13, ruff,
   basedpyright.
4. Update the earlier ITL-884 plans' descriptions where they name the removed parameters.
5. One commit, push, reply on thread 4536812218 (do not resolve), CodeRabbit round, CI.

## Notes for successors

- PR #293 calls `_read_delta_table`; after this it no longer passes `record_path`.

## Status

- Steps 2–4 done: suite 6926 passed / 96 skipped / 15 xfailed on 3.11, 3.12 and 3.13; ruff clean;
  basedpyright 0 errors (warnings 1595 → 1587).
- `_single_store_error` here is now `(group) -> StoreReadError | None`, picking
  `StoreReadError.in_group(group)[0]`. PR #287's version takes `topo_order` and ranks
  `StoreWriteError` by `node_label`; whichever PR merges second reconciles the two.
