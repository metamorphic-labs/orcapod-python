# ITL-1034 Row-alignment test suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A test suite that proves, by recomputation, that every per-row derived column of every
table Orcapod produces belongs to its own row, on sync and async paths, under forced completion
orders; plus a code sweep for the "built in one order, attached by position in another" pattern.

**Spec:** Linear ITL-1034 plus the coordinator's brief (task_a7a8603f2f52). The brief is the design;
no separate spec. ITL-1029 (the `_shape_table` site) is fixed on another branch: this suite must
FAIL on main because of it and pass once that fix is merged. No xfail, no fix of that site here.

**Architecture:** `tests/test_core/alignment/` holds three helper modules and the test modules.
`gate.py` forces completion order with asyncio events (no sleeps). `oracle.py` derives, from the
Python rows the test itself wrote, every origin row and its record id by the documented recipe
(`docs/reference/hashing.md` sites 5 and 6b). `checker.py` walks every row of a produced table and
recomputes each derived column from that row alone (content hash, source-info tokens, system tags,
record ids, context key, function outputs), falling back to an identity-keyed comparison with the
stored records only where a column embeds a per-run uuid.

**Tech stack:** pytest, pyarrow, asyncio; public Orcapod API (`ArrowTableSource`, `function_pod`,
operators, `PipelineJob`, `node.as_table / head / get_all_records / as_source`).

---

## Measured facts (prototype in the scratchpad, Nov 12 2026)

* ITL-1029 reproduces deterministically: async job, completion order reversed, 5 rows,
  `node.as_table(all_info=True)` shows each row with the hash of the mirrored row.
* The sync path is affected too whenever `sort_by_tags` permutes the rows (input not in tag
  order): `_shape_table` sorts, then appends hashes in `iter_data` order. Same site, same fix.
* Node `head()` goes through `_shape_table` too (`_head_of_pairs`): same site.
* `_content_hash` recomputes as `data_context.arrow_hasher.hash_table(row.select(data_keys)).to_string()`
  on a one-row slice of the produced table (verified equal on source and node tables).
* Source tokens: `"<source_id>::<token>::<column>"`, token `"<col>=<value>"` with
  `record_id_column`, else `"row_<i>"`. Record id: `uuid5(_SOURCE_RECORD_ID_NAMESPACE,
  f"{source_id}::{token}")`, namespace `uuid5(NAMESPACE_URL, "https://orcapod.org/namespaces/source-record-id")`.
  (docs/reference/hashing.md site 5 states a different namespace string: a docs defect, report it.)
* Function output token: `"<pod uri>::<output datagram uuid hex>::<column>"`; the stored record's
  `__data_id` is that uuid. `__output_data_hash` / `__input_data_hash` are prefixed digests.
* Async concurrency is unlimited by default (`resolve_concurrency` -> None), so gating every row
  on an event cannot deadlock for small tables; each wait still has a 30 s safety timeout that
  raises a named error (a deadlock guard, never the mechanism of an assertion).

## File structure

* `tests/test_core/alignment/__init__.py` — empty.
* `tests/test_core/alignment/gate.py` — `CompletionGate`, `ORDERS` (natural, reversed,
  interleaved, one_slow, random seeds), the gated function pods (`async def` functions keyed on a
  unique data value).
* `tests/test_core/alignment/oracle.py` — `Origin`, `Oracle` (built from the test's own rows and
  source ids; record id -> origin, token -> origin), recipe functions `source_record_id`,
  `fold_record_ids`, `fold_values`.
* `tests/test_core/alignment/checker.py` — `assert_rows_aligned(table, *, data_keys, tag_keys,
  oracle, data_context_key, ...)` and the per-column rules; `assert_same_rows_by_identity(a, b)`
  for sync == async keyed on record ids.
* `tests/test_core/alignment/fixtures.py` — sources for the three tie scenarios (unique tags in
  shuffled order, duplicate tags, null tags), `COLUMN_CONFIGS`.
* `tests/test_core/alignment/test_checker.py` — the checker is not vacuous: each rule rejects a
  table with that one column permuted.
* `tests/test_core/alignment/test_gate.py` — the gate realises each order (observed via the
  node's `iter_data` order).
* `tests/test_core/alignment/test_streams.py` — `ArrowTableStream`, sources, `FunctionPodStream`
  (sync and the concurrent async-function path), `head()`.
* `tests/test_core/alignment/test_operators.py` — every operator x column config x tie scenario
  (pairwise reduction over operators x configs, full over scenarios).
* `tests/test_core/alignment/test_nodes.py` — function and operator job nodes: sync/async x
  orders x stores (in-memory, Delta) x fresh/readback (rerun, `PipelineJob.load`) x column
  configs x scenarios (full product), `get_all_records`, `head()`, `as_source()` (DerivedSource).
* `tests/test_core/alignment/test_job.py` — `run`, `run_async`, run inside a running loop; run
  summary under forced orders with failing rows; sync == async by identity.

## Checker rules (per produced row)

1. `_content_hash` (or a custom name): recompute from the row's data columns.
2. `_context_key`: equals the stream's `data_context_key`.
3. `_source_<c>`: scalar token -> its origin (oracle by `(source_id, token)`) -> row[c] must equal
   the origin's value of the column `c` came from (rename map), or the declared derivation
   (function output: `y == f(origin)`). A list token (Batch, GroupBy, MergeJoin) is checked element
   by element against the list value. A token whose prefix is a function uri is checked by
   identity against the stored record (`__data_id` hex) when records are given.
4. System tags, grouped by column-name suffix: `_tag_source_id::S` / `_tag_record_id::S`. A scalar
   record id must be an oracle record id whose source id equals the row's source-id value, and
   every source token of a column of that source must give the same record id. Folded (aggregated)
   ones must equal the documented fold of the member record ids recomputed from the row's
   list-valued tokens.
5. User tags and data without provenance columns: (tag, data) must match the row's origin (via
   record id when system tags are present, else by the set of origins with equal tags; with
   duplicate tags the value must be one of the candidates' values).
6. Stored records (`get_all_records(all_info=True)`): `__output_data_hash` recomputed from the
   data, `__input_data_hash` = content hash of the origin's input data (recomputed from the
   origin's own source row through `Data.content_hash` of the source stream), `__data_context`
   = context key, `_source_y` embeds `__data_id`.

## Coverage matrix (as designed)

| Path | sync/async | orders | ColumnConfig | scenarios | stores |
|---|---|---|---|---|---|
| ArrowTableStream / source as_table, head | sync | n/a | all 8 | 3 | n/a |
| FunctionPodStream (`pod(src)`), head | sync + async gather | 5 | all 8 | 3 | n/a |
| FunctionJobNode as_table/head/get_all_records/as_source | sync + async (run, run_async, in-loop) | 5 (+3 seeds) | all 8 | 3 | mem, delta; fresh + rerun + load |
| OperatorJobNode (Join, GroupBy downstream of the gated fn node) | sync + async | 5 | all 8 | 3 | mem, delta; fresh + rerun |
| Each operator on a source stream | sync (+ async via job for 3 ops) | n/a | pairwise | 3 | n/a |
| Run summary (failing rows) | sync + async | 5 | n/a | 1 | mem |

ColumnConfig set: `{}` (default), `sort_by_tags=True`, `content_hash=True`,
`content_hash="h"`, `source=True`, `system_tags=True`, `context=True`, `meta=True`,
`all_info=True`, and `all_info` with `sort_by_tags=False`.

## Tasks

### Task 1: gate + oracle + checker + checker self-test
- [ ] Write `gate.py`, `oracle.py`, `checker.py`, `fixtures.py`.
- [ ] `test_checker.py`: build a correct source table; assert the checker passes; for each rule,
      permute one column (`table.set_column(i, name, column.take(perm))`) and assert the checker
      raises `AssertionError` naming that column. Run, expect PASS.
- [ ] `test_gate.py`: run the gated node for each order; assert `iter_data` order == the order.
- [ ] Commit `test(alignment): row-alignment checker and completion-order gate (ITL-1034)`.

### Task 2: node paths (reproduces ITL-1029)
- [ ] `test_nodes.py` with the full product above. Run; expect FAILURES only on rows with
      `_content_hash` under `sort_by_tags` / async orders (the ITL-1029 site). Record the failing
      ids in this plan.
- [ ] Commit, push, open DRAFT PR (Fixes ITL-1034), status message to coordinator.

### Task 3: streams, operators, job, run summary
- [ ] `test_streams.py`, `test_operators.py`, `test_job.py`. Any failure outside the ITL-1029 site
      is a new defect: send it to the coordinator at once with a minimal repro, then fix here if a
      few lines (failing-first test) or wait for the coordinator.
- [ ] Commit.

### Task 4: sweep
- [ ] Verify every suspect line of the sweep report; put the table in the PR body.

### Task 5: finish
- [ ] changelog.d/ITL-1034.md (`### Added`: the suite; no library change).
- [ ] ruff format/check, basedpyright, full `uv run pytest tests/ -n 4` (or fewer processes).
- [ ] Merge origin/main when the coordinator says the ITL-1029 fix is in; rerun; PR ready;
      CodeRabbit rounds; worker_done.

## Progress notes
(Successors: append here.)
