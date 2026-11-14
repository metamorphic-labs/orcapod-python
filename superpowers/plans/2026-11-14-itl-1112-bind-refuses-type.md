# ITL-1112 bind refuses a source whose column types differ — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `PipelineJob.bind(sources=...)` and the `SourceJobNode.bound_source` setter refuse a
source whose column types differ from the slot's declared schema, so an accepted bind never
moves a pipeline hash and never sends rows of another type to a stored path.

**Architecture:** `SourceNodeBase.validate` (the node owns the declared schema and its name)
compares, column by column, the source's `output_schema()` with the declared tag and data
schemas: names, role (tag/data), Python type. `bind` validates every slot before it changes
anything (sources and database); the setter calls the same `validate` before assigning.
`SourceSpecMismatchError` becomes a subclass of `InputValidationError` and `ValueError` (E1).

**Tech Stack:** Python, pytest, `uv run`.

---

## Decisions (coordinator, 2026-11-14)

- Developer: "Definitely refuse". No recompute of downstream identities.
- A1: compare names, roles, Python types (the pipeline-hash inputs); `optional_fields` and
  column order are not compared.
- E1: `SourceSpecMismatchError(InputValidationError, ValueError)` for name and type mismatches.
- Move `self._database = database` in `bind` after source validation.
- Setter calls `validate` (setting `None` unbinds, unchecked).
- **Nullability (T vs T | None): PENDING with the developer.** No tests or code may fix either
  answer until the coordinator replies. Tests use int vs str only.
- ITL-949 (int32 vs int64) out of scope.

## Reproduction (main a9b50350)

Scratchpad scripts `repro.py`, `repro3.py`: slot `src` (id:int, x:int) -> MapData(x->y) ->
`const(y: int) -> int`. Binding a str source is accepted by bind and the setter, fresh and
loaded job; pipeline hashes unchanged (src 70bf0c37, renamed e777d53b); a run sends str values
into the int-declared paths (`schema:f289a35f.../pdb_v2`, `_result/.../rdb_v2`).

## Files

- Modify: `src/orcapod/errors.py` (`SourceSpecMismatchError` bases)
- Modify: `src/orcapod/core/nodes/source_node.py` (`validate`, `bound_source` setter)
- Modify: `src/orcapod/pipeline/job.py` (`bind`: database assignment after validation; docstring)
- Test: `tests/test_pipeline/test_bind_type_check.py` (new)
- Add: `changelog.d/ITL-1112.md`, `design_issues/ITL-1112-BT1.md`
- Docs: `docs/concepts/function-pods.md` near line 783 (one sentence on bind)

## Tasks

- [ ] 1. Failing tests (`tests/test_pipeline/test_bind_type_check.py`), public interface:
  - bind with x: str in place of x: int raises `SourceSpecMismatchError`, which is an
    `InputValidationError`; message names slot `src`, column `x`, `int` and `str`.
  - same through `job.nodes["src"].bound_source = ...`.
  - both on a fresh job and on a job from `PipelineJob.load`.
  - after the refusal the previous binding is kept (`job.nodes["src"].bound_source is old`),
    and `bind(database=new, sources={bad})` leaves `job.database` unchanged.
  - a tag column given as str for a tag declared int is refused too.
  - after an accepted rebind (same types, other values/source id) every node's
    `pipeline_hash()` equals its value before the bind (fresh and loaded).
  - an accepted bind still works and the run stores rows.
  Run: `uv run pytest tests/test_pipeline/test_bind_type_check.py -v` -> FAIL.
- [ ] 2. Implement: errors.py bases; `validate` compares types after names; setter calls
  `validate`; `bind` moves database assignment. Run tests -> PASS. Commit.
- [ ] 3. Nullability: after the coordinator's reply, add tests and the rule.
- [ ] 4. Changelog fragment (Breaking changes and migration), design_issues entry, docs
  sentence; `uv run python scripts/assemble_changelog.py --check`,
  `uv run python scripts/design_issues.py --check`.
- [ ] 5. ruff, basedpyright, full `uv run pytest tests/`; merge origin/main; push; PR.

## Notes for a successor

- Conflicts: PR #384 (ITL-1102) edits the `bound_source` setter (adds `_update_modified_time()`);
  ITL-1111 adds a memo clear inside `bind`. Whoever merges second resolves.
