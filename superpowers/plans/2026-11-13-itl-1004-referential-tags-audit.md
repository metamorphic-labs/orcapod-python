# ITL-1004 referential tags: implementation-scope audit (plan)

> **For agentic workers:** this plan produces a design document, not library code. No change under
> `src/` or `tests/`, no golden file.

**Goal:** One document, `superpowers/specs/2026-11-13-itl-1004-referential-tags-implementation-scope.md`,
that makes the v0.4 referential-tags specification (Linear document attached to ITL-1004)
dispatchable: where the code assumes tag/data disjointness, which hashes would see a binding,
what is stored, operator-by-operator rules and change sizes, sources, function pods, answers to
the specification's open questions, a sliced delivery plan, and the decisions for the developer.

**Architecture:** read the code; measure today's behaviour with small scripts under
`superpowers/specs/itl-1004-audit/` (run with `uv run`, outputs saved beside them); write the
document from the evidence.

**Branch:** `eywalker/itl-1004-referential-tags-v04-a-tag-may-refer-to` from `origin/main`
(`f51ce67c`). PR: docs only, `Refs ITL-1004`.

---

### Task 1: Representation sites
- [ ] Read `core/streams/arrow_table_stream.py`, `core/datagrams/*`, `types.py` (Schema, ColumnConfig),
  `utils/schema_utils.py`, `core/streams/base.py`.
- [ ] grep every `keys()` / `output_schema` / `identity_structure` / `pipeline_identity_structure`
  definition; classify each site as works / change / reject.
- [ ] Script `01_representation.py`: build an `ArrowTableStream` with a column in both tag and
  data lists (if constructible) and record what each public method returns.

### Task 2: Identity and hashes
- [ ] Read `core/base.py`, `core/sources/base.py`, `core/nodes/function_node.py`
  (`_build_record_id_preimage`), `core/function_pod.py`, `core/datagrams` content hash,
  `derived_source.py` (PR #336/#338 pattern).
- [ ] Script `02_identity.py`: show which hash inputs contain the tag/data split.

### Task 3: Storage
- [ ] Read FunctionNode / OperatorNode / CachedSource / DerivedSource write and read paths.
- [ ] Script `03_storage.py`: stored columns of each table kind for a small pipeline.

### Task 4: Operators
- [ ] For every file in `core/operators` and the FunctionPod call path, measure today's behaviour
  with a same-named tag and data column (`04_operators.py`), sync and async.
- [ ] Join source-info of merged column: options.

### Task 5–6: Sources and function pods
- [ ] Read `arrow_table_source.py`, `stream_builder.py`, `spiraldb_table_source.py`,
  `db_table_source.py`, `derived_source.py`; `function_pod.py` output collision (`05_function_pod.py`).

### Task 7–9: Open questions, delivery plan, decisions
- [ ] Answer section 17 from code; slice plan with sizes, dependencies, tests; decisions list.

### Task 10: Finish
- [ ] Doc under ~900 lines; ruff on audit scripts; `uv run pytest tests/test_docs` not affected
  (superpowers is not a docs page); merge origin/main; push; PR `Refs ITL-1004`;
  `orca linear attach`; `orca worktree set --pr`; worker_done.

## Progress notes
(updated as work proceeds, for a successor)
- 2026-11-13: Tasks 1-9 done. Measurements 01-06 in superpowers/specs/itl-1004-audit/ (outputs .out).
  Coordinator redirects applied (default referential sources; plain-tag mode = v0.3 exactly;
  three Join cases equal; decisions only for Open/code-impossible points; slices mapped onto
  ITL-1009..1023). Representation + hash verdict sent to the coordinator (msg_a8cdbe370954).
  Pod-boundary rule (D1) asked; coordinator: keep (a) recommended, put to developer.
  Full suite: 11029 passed, 117 skipped, 23 xfailed. Remaining: PR, linear attach, worker_done.
