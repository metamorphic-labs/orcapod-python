# ITL-578 Refresh onto main's v0.3 Hash Step — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring PR #286 (`pl.DataFrame` / `pl.Series` logical types) up to `origin/main`
(c8e9eb21) and to the v0.3 logical-type rules, so it can merge before the v0.3 tag.

**Architecture:** One merge commit of `origin/main` (no rebase), then conformance commits:
identifiers `{"version":1}`, registration in `contexts/logical_types.json`, hash labels as
single constants, starfix ordered mode for the values digest. One push.

**Tech Stack:** pyarrow, polars, starfix 0.5.0, pytest, ruff, basedpyright.

**Coordinator decisions (ask 1, November 12, 2026):**

- A: library resolutions approved as listed in Task 1.
- B: Option 1. `_values_digest` moves to starfix ordered mode
  (`ordered_columns=True, ordered_struct_fields=True`); the struct-validity wrapper is
  deleted (starfix 0.5.0 tells a null struct from a struct of nulls itself); the descriptor
  is kept unchanged. Add a public-interface test that a null struct and a struct of nulls
  hash differently. Before/after definition goes in the PR body.
- C: labels stay `polars_dataframe_v1` / `polars_series_v1`; constants renamed
  `POLARS_DATAFRAME_HASH_LABEL` / `POLARS_SERIES_HASH_LABEL`; descriptor `kind` unchanged.
- D: add a sentence to ITL-966 gap 3 naming the two new types.
- PR body: state that polars treats column order as identity while pandas on main ignores
  it, with the evidence command. Do not change pandas.

---

### Task 1: Merge origin/main

- [ ] `git merge origin/main --no-commit`. 11 conflicts expected.
- [ ] `logical_types/pandas_type.py`: main's file; its private `_table_to_ipc_bytes` /
      `_ipc_bytes_to_table` replaced by imports from `logical_types/arrow_ipc.py`.
- [ ] `hashing/semantic_hashing/builtin_handlers.py`: main's file plus the PR's
      `PolarsDataFrameHandler` / `PolarsSeriesHandler`; all three Polars handlers registered
      in `register_builtin_python_type_handlers`.
- [ ] `contexts/data/v0.1.json`: accept deletion. Register the two logical types and two
      value handlers in `contexts/logical_types.json` after the pandas entries.
- [ ] `CHANGELOG.md`: main's; PR text to `changelog.d/ITL-578.md`.
- [ ] `DESIGN_ISSUES.md`: deleted; PL1 to `design_issues/ITL-578-PL1.md`.
- [ ] `docs/reference/hashing.md`, `docs/concepts/function-pods.md`: both sides; the PR's
      hashing section renumbered after main's 1c / 1d.
- [ ] `tests/test_hashing/golden/cases.py`: both sides. Golden data: main's files.
- [ ] `CLAUDE.md`, `.zed/rules`: delete the PR's `<type>_v<N>` bullet.
- [ ] Commit the merge. Scripts used: scratchpad `resolve.py`, `resolve_rest.py`,
      `conform.py` (re-derivable from this list).

### Task 2: Identifier conformance (test first)

- [ ] Test (red): `logical_type_identifier(LogicalPolarsDataFrame()).text ==
      'polars.dataframe;' + b'{"version":1}'.hex()`, same for Series; an empty and an
      all-null column of each type hash differently from each other, from
      `pandas.dataframe` and from plain `large_binary` (same shape as the #324 test).
- [ ] `polars_type.py`: `_IDENTIFIER_METADATA = version_metadata(1)` on Arrow and Polars
      extension classes of both types; no `also_reads`.

### Task 3: Labels

- [ ] Rename `POLARS_*_HASH_METHOD` to `POLARS_*_HASH_LABEL`; no label literal elsewhere in
      `src/`. Add both to the label table in `docs/reference/hashing.md`.
- [ ] `tests/test_hashing/test_hash_label_rule.py` passes with the Polars cases.

### Task 4: starfix ordered mode (test first)

- [ ] Test (public interface, semantic hasher and through a cached pod): null struct vs
      struct of nulls differ at top level, in a list, in a struct. These exist as property
      tests; confirm they stay green and add the pod-level one.
- [ ] Test (red before the change): the values digest equals
      `ArrowDigester.hash_table(projection, ordered_columns=True, ordered_struct_fields=True)`.
- [ ] `_values_digest`: call starfix with both ordered flags; delete
      `_struct_validity_type`, `_struct_fields`, `_with_struct_validity`,
      `_with_table_struct_validity`. Keep `_hash_projection` (null to bool, dictionary
      decode, fixed-size list to large list) and the descriptor. Check a zero-field struct,
      bytes under null slots, sliced and chunked frames still hash equal to their compact form.
- [ ] Update module docstring, `docs/reference/hashing.md`, PL1.

### Task 5: End-to-end checks through the public interface

- [ ] Source column, pod input and output, cached re-run (no recomputation), sync and async,
      in-memory and Delta, `list[...]` via Batch and GroupBy: existing tests in
      `tests/test_core/function_pod/test_polars_dataframe_values.py`; add what is missing.
- [ ] Saved `PipelineJob` loaded back: test against what main does today (PR #303 is open).
- [ ] Wrong-typed return from a `-> pl.DataFrame` / `-> pl.Series` pod raises; a subclass is
      accepted.
- [ ] Fix the fixture in `test_polars_extension_dtype` (ndarray storage is a struct now).

### Task 6: Golden, docs, checks, push

- [ ] `uv run python tests/test_hashing/golden/generate_golden_hashes.py`; the diff against
      `origin/main` must add lines only. Report added entries by file and count.
- [ ] `scripts/assemble_changelog.py --check`, `scripts/design_issues.py --check`.
- [ ] Full suite on 3.12, then 3.11 and 3.13; ruff check, ruff format --check, basedpyright
      exit 0.
- [ ] PR body rewritten to what is true on main; Linear ITL-966 sentence. ONE push.
- [ ] Readiness: CI, CodeRabbit on the new head, all review threads answered, worker_done.

## Progress notes

(appended while working)
