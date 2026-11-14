# ITL-1080 Sync and async give the same rows — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Sync and async runs give identical rows: the zero-row async operator node keeps the
sync path's columns and Arrow types, and (once the developer decides the meta rule) every
operator treats input `Data` meta columns alike on both paths.

**Architecture:** The zero-row fix lives in `OperatorJobNode.async_execute`: it taps each input
channel (first row, plus the first row with a `None` source-info token per column), and on a
run with no output rows rebuilds zero-row inputs exactly as the sync orchestrator materializes
its buffers (one shared helper, `StaticOutputOperatorPod._materialize_upstream`), then keeps
`operator.process(*inputs)` as the result, as `execute()` does. No channel protocol or
`async_execute` signature change. The meta fix (rule B, pending developer decision) drops
input meta in every operator on both paths.

**Tech Stack:** Python 3.12, PyArrow, pytest via `uv run`.

---

## Status / notes for a successor

- Diagnosis sent to coordinator 2026-11-13 (scratch notes in the worker transcript):
  - All 8 meta xfails: Data/Tag content hashes identical on both paths; only meta columns differ.
  - Main's Join DROPS meta (ITL-854/857), not keeps. Rule B (no operator carries input meta)
    recommended; coordinator agrees, **developer decision pending: do not change operator meta
    handling until answered.**
  - Q2 (zero-row node) approved: implement now, test first, nothing stored for zero rows.
- Step-4 matrix (scratchpad `matrix.py`): source → fn pod → 17 operators → fn pod, sync vs async
  job, in-memory store: no difference. MergeJoin over a dict column raises TypeError on BOTH
  paths (report, out of scope).

## Task 1: Zero-row async operator node (approved)

**Files:**
- Modify: `src/orcapod/core/operators/static_output_pod.py` (add `_materialize_upstream`)
- Modify: `src/orcapod/pipeline/sync_orchestrator.py` (`_materialize_as_stream` delegates)
- Modify: `src/orcapod/core/nodes/operator_node.py` (`async_execute`: input taps, zero-row result)
- Test: `tests/test_pipeline/test_operator_node_async_readback.py`

- [ ] Remove the strict xfail on
  `test_operator_node_with_no_rows_keeps_columns_and_types_after_async_run`, parametrize it over
  zero-row cases (filter no match, DropNulls dropping all, Join/SemiJoin/MergeJoin no match,
  operator after an input that delivered no rows), comparing `as_table(all_info=True)` schema
  and rows and `as_table()` of sync vs async runs. Run: fails.
- [ ] Add `_materialize_upstream(rows, upstream, *, zero_rows)` beside `_rows_to_table`: the body
  of the sync orchestrator's `_materialize_as_stream`, optionally sliced to zero rows. Make the
  sync orchestrator call it (no behaviour change).
- [ ] In `OperatorJobNode.async_execute`, wrap each input reader in `_InputTap` (keeps the first
  row and, per source-info column, the first row whose token is `None`). With no output rows,
  `self._cached_output_stream = operator.process(*[_materialize_upstream(tap.rows, upstream,
  zero_rows=True) ...])`.
- [ ] Run targeted tests: readback, equivalence, sync orchestrator, golden. Commit.

## Task 2: Meta rule B (BLOCKED on developer decision)

- [ ] Prepare failing tests over both meta origins (raw `ArrowTableStream` with `__` columns;
  `CachedFunctionPod` stream rows with `__computed`) for every operator, sync and async:
  output `Data.meta_columns == ()` and identical schemas on both paths.
- [ ] After approval: drop input meta in each operator (sync: input tables via the Join helper
  `_without_meta`, moved to the operator base; predictions drop `__` fields; async: per-row
  operators drop `Data` meta). Remove the 8 META_XFAIL marks; update
  `test_drop_nulls::test_no_argument_leaves_meta_columns_alone`, docs/concepts/operators.md.
- [ ] Golden suite unchanged.

## Task 3: Equivalence matrix coverage

- [ ] `CASES` in `test_async_sync_equivalence.py` already holds SemiJoin, MapTags, MapData, the
  four selections, DropNulls, Index, Pick. Add the automatic select in front of a function pod
  (sync vs async job, stored records) if not covered by `test_sync_async_stored_records.py`.

## Task 4: Finish

- [ ] `changelog.d/ITL-1080.md`, `design_issues/ITL-1080-SA1.md`.
- [ ] Merge origin/main, push, PR "Fixes ITL-1080", CodeRabbit, CI on 3.11/3.12/3.13.
