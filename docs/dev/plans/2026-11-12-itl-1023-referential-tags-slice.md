# ITL-1023 Referential tags workshop slice — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. A successor picking this up
> after a crash: read the "Status log" at the bottom first.

**Goal:** The thinnest working path of referential tags (specification "Orcapod v0.4 —
Referential Tags", attached to ITL-1004) on branch
`eywalker/itl-1023-workshop-slice-referential-tags-working`, usable in the workshop of
November 17, 2026: sources (referential by default), function pods, `Join`, `GroupBy`,
select/drop, row-preserving operators, function-node storage. Everything else raises
"not supported with referential tags yet (ITL-xxxx)".

**Architecture:** A referential tag `x` is ONE physical column `x` in the stream's single Arrow
table whose name is in both role lists (spec 4.3 as code: binding = `tag_keys ∩ data_keys`).
No change to `types.Schema` or to the `(tag_schema, data_schema)` tuple: `x` appears in both
halves of `output_schema()` / `keys()` at graph-build time. `ArrowTableStream(table,
tag_columns, reference_tags=...)` makes data = non-tags ∪ references. Sources default every
declared tag to referential; `tag_only_columns=[...]` (approved name, coordinator Nov 12)
keeps chosen tags plain, exactly v0.3. Hash terms enter only when a reference exists.

**Tech Stack:** Python 3.11-3.13, PyArrow, Polars, pytest, uv.

---

## Decisions (go into the PR body's DECISIONS section)

1. Representation: name intersection, one physical column, no Schema change (audit, firm).
2. Option name `tag_only_columns: Collection[str]` on sources (developer's choice, Nov 12); subset of `tag_columns`; unknown
   name → `ValueError`. Builder (`SourceStreamBuilder.build`) takes explicit `reference_tags`
   (default `()` = plain); sources compute `reference_tags = tag_columns - tag_only_columns`.
3. ListSource / DerivedSource / CachedSource tags stay plain (computed values, or stored records).
4. Pod boundary (developer decision, November 12, 2026, replaces my first plan): a pod's output
   data stays the function's outputs; every referential tag of the input becomes a PLAIN tag
   (same value and type) in the pod's output schema. No diagnostic at a pod boundary
   (coordinator). Output name equal to any tag name raises at build time. No change to what a
   node stores or to function-result identity. The 'fall-through / append' convenience is a
   separate issue, out of scope.
5. Join merged source-info (spec 9.4): the merged column's `_source_x` keeps the tokens of
   every referential input, as `large_list<large_string>` in canonical input order (a list
   input contributes its elements). Plain+referential keeps the referential side's column as is.
6. Materialization diagnostic: module-level `orcapod.config`-free setting
   `orcapod.core.referential_tags.reference_materialization = "warn" | "silent"`, default
   `"warn"`, emitted with `warnings.warn(..., ReferenceMaterializationWarning)` once per
   structural operation at graph build. Never changes a result or a hash.
7. Unsupported operators: one guard in the operator base (`StaticOutputOperatorPod.validate_inputs`)
   raises `NotImplementedError("<Op>: not supported with referential tags yet (ITL-nnnn)")`
   unless the class declares `SUPPORTS_REFERENTIAL_TAGS = True`. MapTags/MapData raise only
   when a rename touches a referenced name.
8. Identity: `ArrowTableStream.identity_structure` (no producer) adds `("reference_tags", refs)`
   only when non-empty. Everything else gets it through the schema halves. Identities of
   referential streams are NOT frozen by this slice (docs say so).

## Files

- Create `src/orcapod/core/referential_tags.py`: `reference_tags(tag_schema_or_keys, data_...)`,
  `validate_tag_bindings(...)` (shared namespace validator, spec 4/13.4),
  `require_referential_support(op, streams)` guard, materialization setting + warning class.
- Modify `src/orcapod/core/streams/arrow_table_stream.py`: `reference_tags` arg, data columns,
  identity term, `iter_data`.
- Modify `src/orcapod/core/sources/stream_builder.py` (+ `arrow_table_source.py`,
  `dict_source.py`, `csv_source.py`, `data_frame_source.py`, `delta_table_source.py`,
  `db_table_source.py`, `spiraldb_table_source.py`, `polling_source.py`): default referential,
  `tag_only_columns` option, to_config.
- Modify `src/orcapod/core/operators/static_output_pod.py` (guard, `_rows_to_table` dedupe),
  `pipeline/sync_orchestrator.py` (dedupe, pass references).
- Modify operators: `join.py`, `group_by.py`, `column_selection.py`, `filters.py`,
  `drop_nulls.py`, `semijoin.py`, `pick.py`, `index.py`, `mappers.py`; guard others.
- Modify `src/orcapod/core/function_pod.py` + `core/nodes/function_node.py`: pass-through,
  collision check, table building dedupe, stored-row stream references.
- Tests: `tests/test_core/referential_tags/` (new): `test_edi_end_to_end.py` (step 0),
  `test_schema_rules.py`, `test_sources.py`, `test_function_pods.py`, `test_join.py`,
  `test_group_by.py`, `test_select_drop.py`, `test_row_preserving.py`, `test_not_yet.py`,
  `test_hashes.py`; additions to `tests/test_core/operators/test_async_sync_equivalence.py`.
- Golden fixtures: `tests/test_hashing/golden/cases.py` switch to `tag_only_columns=` — values
  must stay byte-identical (run golden test before and after).
- `changelog.d/ITL-1023.md`, `design_issues/ITL-1023-RT1.md` (slice limitations),
  `docs/concepts/referential-tags.md` (short; states identities not frozen).

## Tasks (in the order of the brief; keep every step green; push after each)

### Task 0: failing end-to-end EDI-shaped test
- [ ] `tests/test_core/referential_tags/test_edi_end_to_end.py`: spikes (k1..k5, spike_time,
  amplitude) and gaze (k1..k5, gaze_time, gx) as `DictSource` with tag_columns keys + time
  (referential by default); `trials` keyed on (k1, k2) with explicit `tag_only_columns`;
  `GroupBy(k1..k5)` on both; `pod(spikes_g, gaze_g, trials)` (automatic join + select) with
  `def align(spike_time: list[int], gaze_time: list[int], k1: str) -> int`; `PipelineJob` on
  `InMemoryArrowDatabase` and on `DeltaTableDatabase(tmp_path)`; sync and async orchestrators;
  assert values, that `k1` is a tag and data, call counter = n groups on run 1 and 0 on run 2.
- [ ] Run → fails (`tag_only_columns` unknown / tags not data). Commit, push, open draft PR
  (Refs ITL-1023), send progress message.

### Task 1: representation + shared validator
- [ ] Unit tests: ArrowTableStream with `reference_tags` → keys/schema both halves, `as_table`
  has the column once, rows carry it in Tag and Data, unknown reference → ValueError.
- [ ] Implement in `arrow_table_stream.py` and `referential_tags.py`.

### Task 2: sources
- [ ] Tests: DictSource default referential; `tag_only_columns` gives v0.3 schema/hashes
  equal to the builder's plain form; unknown name raises.
- [ ] Implement builder `reference_tags`, each source's option, to_config.

### Task 3: hashes
- [ ] Golden fixtures switch to `tag_only_columns=` (all sites with tag_columns in cases.py);
  run golden suite: no value moves.
- [ ] Identity term in ArrowTableStream; test referential vs plain over one table differ;
  spec 12.4 test: function not reading `x` has equal input data hash / result store hit
  across plain vs referential source.

### Task 4: function pods
- [ ] Tests: read referenced column by name; tags pass through and `x` stays referential in
  output; output name == tag name raises (referential and materialized); auto-select drops
  `x` → plain tag + warning; `silent` → no warning, same hashes.
- [ ] Implement pass-through in `_FunctionPodBase` (one helper, used by sync/async/node paths),
  `output_schema`, collision check, table dedupe in `FunctionPodStream._table_from_pairs` and
  `FunctionNodeBase._table_from_pairs`.

### Task 5: Join
- [ ] Tests: plain+plain unchanged; plain+ref and ref+ref → one data column, tag referential;
  other collisions raise (data/data, mat tag vs data); type mismatch raises; async == sync.
- [ ] Implement `_joined_user_schemas`, `static_process` (source-info merge), async merge.

### Task 6: GroupBy
- [ ] Tests: key ref stays scalar both roles; non-key list in both with same order; async == sync.
- [ ] Implement (column appears once in table; pass `reference_tags`).

### Task 7: row-preserving + select/drop + guards
- [ ] Filters, SemiJoin, DropNulls, Pick, Index: pass references; Select/Drop data
  materializes; Select/Drop tag leaves data. Guard Batch, MergeJoin, renames touching refs,
  OperatorNode storage, DerivedSource, Pipeline save.

### Task 8: node storage
- [ ] FunctionNode stores/reads back with bindings (in-memory + Delta), head() shows `x` once,
  second run computes nothing (covered by Task 0 test + unit tests).

### Finish
- [ ] Full suite on 3.11/3.12/3.13, ruff, format, basedpyright; changelog fragment; design
  issue entry; docs page; merge origin/main; PR body with DECISIONS; worker_done.

## Status log
- 2026-11-12: plan written; branch created from origin/main f51ce67c.
- 2026-11-12: developer: option is `tag_only_columns`; references become plain tags at a pod boundary.
- Task 0 done: fc22de6a, draft PR #342.
- 2026-11-12: Tasks 1-8 implemented (02b62358, c3c5e995, c42eece8, 853da828). Developer rename
  rule implemented (map_tags both roles, tag_only=True, map_data data-only). EDI test uses
  `timestamp` in both series. Existing tests pinned to `tag_only_columns=True` (79 files, 569
  call sites). Remaining: full suite on 3.11/3.12/3.13, basedpyright, final report.
- 2026-11-13: fall-through (ReferenceFallThroughPod) per developer ruling; format 0.3.0 with
  f51ce67c fixture; merged main through #349 (ITL-940 check exempts references). Final code
  head 4473e2ef: CI green; local full suite 3.11/3.12/3.13 green (one intermittent
  crash-recovery async test, unchanged from main); basedpyright 0 errors. Reported to the
  coordinator (worker_done). Remaining work belongs to ITL-1009..1022.
