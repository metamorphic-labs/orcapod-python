# ITL-910 — Adopt starfix 0.5.0 (hash format 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Orcapod's Arrow hashing onto starfix 0.5.0 (hash format 2, prefix `00 00 02`), bump the
Arrow hash label `arrow_v0.1` → `arrow_v0.2`, and regenerate the golden hashes once, explicitly.

**Architecture:** `StarfixArrowHasher` (`src/orcapod/hashing/arrow_hashers.py`) keeps calling
`ArrowDigester.hash_table` / `hash_schema` with the default (order-independent) options; only the
dependency, the label and the tests/docs/golden data change, plus whatever the new starfix rejections
require. No `ordered_columns` / `ordered_struct_fields` (developer decision 5). No legacy 0.4 mode.

**Tech Stack:** starfix 0.5.0 (pure Python, `pyarrow>=19`), pyarrow 23.0.1 (locked; Orcapod floor already
`>=20`, so no pyarrow floor change), pytest, uv.

Branch: `eywalker/itl-910-adopt-starfix-05-hash-format-2-in-orcapod` (Linear gitBranchName).

---

## Status log (for a successor)

- 2026-11-11: investigation done (below). Plan sent to coordinator via `ask` — WAIT for the answer
  before Task 4 (golden regeneration) and before any migration-related code.

## Investigation results

### starfix 0.5.0 API (installed wheel, README, `arrow_digester.py`)

- Same entry points as 0.4: `ArrowDigester(schema, ...)`, `update`, `finalize`, `hash_schema`,
  `hash_record_batch`, `hash_table`, `hash_array`. New keyword-only options `ordered_columns`,
  `ordered_struct_fields` (not used here). `include_metadata` unchanged.
- Every hash starts `00 00 02`; all values differ from 0.4.
- `ValueError` (was `NotImplementedError` in 0.4) for data of Null/Map/Interval/Utf8View/BinaryView,
  any row count; `ValueError` for ListView/Union/RunEndEncoded; new `ValueError`s: ambiguous node
  paths (duplicate names, `"a/b"` aliasing struct `a` child `b`), non-nullable field with a visible null,
  duplicate metadata keys with `include_metadata`. `TypeError` for wrong container type (Orcapod
  converts RecordBatch → Table first, unaffected).
- FixedSizeList data now hashes (any depth), distinct from `List`/`LargeList` of the same values.
- Canonical extension types (`arrow.uuid`, `arrow.fixed_shape_tensor`) now hash.

### Orcapod callers

`grep -rn starfix src`: only `src/orcapod/hashing/arrow_hashers.py` (`hash_schema`, `hash_table`).
Everything else (sources' default `source_id`, record ids, `__input_data_hash`, base/pipeline entry
ids, OperatorNode record hashes, `Data`/`Tag` content hashes, `ArrowTableHandler` for `pa.Table`,
pandas DataFrame/Series handlers) goes through `StarfixArrowHasher`.

### Behaviour changes visible to an Orcapod user (probe: `ArrowTableSource(t, tag_columns=["k"])`,
`hasher.hash_table`, `hasher.hash_schema`, 0.4 vs 0.5)

| Input | 0.4 | 0.5 |
|---|---|---|
| `fixed_size_list` column (top level, in struct, in list, with null slot) | `NotImplementedError` (source fails) | hashes; source constructs |
| homogeneous `tuple[T, T]` pod output (maps to `fixed_size_list`) | fails to hash | works |
| `arrow.uuid`, `arrow.fixed_shape_tensor` extension columns | `NotImplementedError` | hashes |
| `hash_schema` with null / view / interval types | `NotImplementedError` | hashes |
| **zero-row** table with a `map` column | hashes | `ValueError` |
| non-nullable field with a visible null | hashes silently | `ValueError: Non-nullable field 'x' contains nulls` |
| duplicate column names (raw hasher, e.g. a `pa.Table` value) | hashes | `ValueError: Ambiguous node path` (sources already failed with `KeyError`) |
| column `"a/b"` next to struct `a{b}` | hashes | `ValueError: Ambiguous node path 'a/b'` |
| `map` (rows), `null`, view, interval data | `NotImplementedError` | `ValueError` (same rejection, new class) |
| `CachedSource._build_merged_stream` | `except NotImplementedError` silently served from cache when live ingestion hit an unhashable type | now raises (the hashing error is no longer mistaken for "source cannot provide data") |

ITL-848 collisions through the public interface (`ArrowTableSource.iter_data()` row content hashes):
nested list lengths `[[[1]],[[2],[3]]]` vs `[[[1],[2]],[[3]]]` collide on 0.4, differ on 0.5; null struct vs
`{"a": None}` collide on 0.4, differ on 0.5; Bool column hash depends on chunking on 0.4, not on 0.5;
non-nullable `[True]` vs `[True, False, False]` collide on 0.4, differ on 0.5. Slice offsets are canonical
on both.

### Where the label lives

- `src/orcapod/hashing/versioned_hashers.py:40` `_CURRENT_ARROW_HASHER_ID`
- `src/orcapod/contexts/data/v0.1.json` `arrow_hasher._config.hasher_id` (+ changelog list)
- `src/orcapod/contexts/data/schemas/context_schema.json:178` (example value)
- tests: `tests/test_hashing/test_starfix_arrow_hasher.py` (label + three pinned format-1 digests),
  `tests/test_hashing/test_pandas_handlers.py` (label), `tests/test_core/operators/test_materialize_source_info.py`
  (three pinned format-1 digests), `tests/test_core/operators/test_async_sync_equivalence.py`
  (`PINNED_ROW_HASHES`, `PINNED_STREAM_CONTENT_HASH`),
  `tests/test_hashing/golden/cases.py` (docstring), golden data files.
- docs: `docs/reference/hashing.md` §3/§4 worked-example tables (regenerate with
  `superpowers/scripts/hash_audit_example.py`); `notebooks/tutorials/01_introduction_to_orcapod.ipynb`
  (outputs). Historical plans/specs under `superpowers/` are left as they are.
- Stored names: no table path embeds an Arrow hash under the default `table_scope="pipeline_hash"`.
  With `table_scope="content_hash"`, the path embeds the node content hash, which moves when a source's
  content hash moves.

### Golden estimate (simulated: starfix 0.5 overlay + label patched to `arrow_v0.2`)

| File | Entries | Changed | Unchanged | Keys added/removed |
|---|---|---|---|---|
| arrow.json | 53 | 53 | 0 | 0 |
| datagram.json | 171 | 40 | 131 | 0 |
| pipeline.json | 305 | 68 | 237 | 0 |
| schema.json | 92 | 22 | 70 | 0 |
| semantic.json | 64 | 1 (`value.dataframe.basic`, pandas via StarfixArrowHasher) | 63 | 0 |

Every changed entry is an Arrow-derived value (row data hashes, record/entry ids, source content hashes
and default source ids, `pa.Table`/DataFrame hashes). No pipeline hash, schema hash, system-tag column
name or table path moved.

### Migration finding (needs a decision)

Default `table_scope` paths do not move, so a v0.3 job pointed at a v0.2 store writes into the same
tables. Observed with `DeltaTableDatabase` and the docs' `bmi_demo` job: v0.3 `run()` recomputes both rows
(old record ids miss), and a later read without `run()` returns **4 rows for 2 inputs** (v0.2 + v0.3 mixed);
before the first v0.3 run, a read returns the v0.2 rows. Stored rows carry the label
(`__input_data_hash`, `__pipeline_base_entry_id` start `b"arrow_v0.1:"` vs `b"arrow_v0.2:"`). OperatorNode
REPLAY reads all rows matching the node content hash, so a v0.3 LOG-mode write next to v0.2 rows can
duplicate too.

### Full suite under starfix 0.5 overlay (label unchanged): 201 failed / 7400 passed / 96 skipped

- 186 golden entries (all Arrow-derived, see above).
- Pinned format-1 digests: `test_starfix_arrow_hasher.py` (5 tests), `test_materialize_source_info.py` (1),
  `test_async_sync_equivalence.py::test_issue_example_types_and_pinned_hashes`,
  `test_struct_layout_identity.py::TestStructHashesUnchanged` (4 params).
- `test_group_by.py::test_falls_back_to_record_id_when_key_covers_all_tags`: member order comes from
  record ids (uuid5 of the Arrow-hashed default `source_id`), so `["a","b"]` becomes `["b","a"]`. Arrow-derived.
- `test_array_v02_store.py` (2): the ITL-888 claim "an array-free pod reuses its v0.2 results" stops being
  true: every function node recomputes (input data hashes move) into the same tables (mixing finding).
- `test_async_sync_equivalence.py::test_arrow_hasher_hash_schema_with_extension_type` (strict xfail, ITL-846)
  now XPASSes: starfix 0.5 accepts a live extension type in `hash_schema`, but hashes it as its storage
  type, so `hash_schema(Path column) == hash_schema(large_string column)`.
- Same identity loss in `hash_table` for pyarrow's canonical extension types (`arrow.uuid`,
  `arrow.fixed_shape_tensor`): they are `pa.BaseExtensionType`, not `pa.ExtensionType`, so
  `normalize_extension_columns` skips them, `include_metadata` stays off, and an `arrow.uuid` table hashes
  equal to a `fixed_size_binary(16)` table. (On 0.4 they raised.)

## Tasks (after approval)

### Task 1: failing tests first
- [ ] New `tests/test_hashing/test_starfix_format2.py` (public interface: `ArrowTableSource`, `iter_data`,
  `content_hash`; the hasher itself via `resolve_context(None).arrow_hasher` for table-level checks):
  label `arrow_v0.2` and digest prefix `00 00 02`; the four ITL-848 cases above; Bool chunking; slice/offset
  and `combine_chunks` canonicality; `fixed_size_list` column hashes and differs from the same values as
  `large_list`; `ArrowTableSource` with `fixed_size_list` columns constructs, keeps the type, and runs through
  a function pod; pod returning `tuple[float, float]`; rejections with `ValueError` (zero-row map,
  non-nullable null, duplicate names via `pa.Table` value hash, `"a/b"` alias); `arrow.uuid` column hashes.
- [ ] Update `test_starfix_arrow_hasher.py` / `test_pandas_handlers.py` label assertions to `arrow_v0.2`.
- [ ] Run, confirm failures on starfix 0.4. WIP commit.

### Task 2: bump
- [ ] `pyproject.toml`: `starfix>=0.5.0,<0.6` (decision 6 floor); `uv lock`; report what moved in `uv.lock`.
- [ ] Label: `versioned_hashers.py`, `v0.1.json` (+ changelog line: "Hard cut (v0.3): arrow_v0.2 = starfix
  hash format 2 (starfix 0.5) …"), `context_schema.json` example.
- [ ] `StarfixArrowHasher` docstring: format 2, prefix `00 00 02`, rejections.
- [ ] Re-pin the format-1 digests in `test_starfix_arrow_hasher.py` and `test_materialize_source_info.py`
  (list old/new in the PR).
- [ ] Remove the generator header's "Known gaps (ITL-848)" items that are now hashable (fixed-size list,
  `arrow.uuid`); add golden cases for them (adding entries is allowed).

### Task 3: migration handling — per coordinator decision (see ask)

### Task 4: regenerate golden (approved regeneration)
- [ ] `uv run python tests/test_hashing/golden/generate_golden_hashes.py`; compare with
  `scratchpad/scripts/cmp.py` against main's files; every changed value must be Arrow-derived.

### Task 5: docs
- [ ] CHANGELOG `[0.3.0] — unreleased` → Breaking changes and migration: starfix format 2 entry.
- [ ] `docs/reference/hashing.md` worked-example tables, prefix note; notebook outputs.
- [ ] DESIGN_ISSUES: one appended entry (resolved, Fix note).

### Task 6: verification
- [ ] Full suite on 3.11, 3.12, 3.13, one at a time; ruff check/format; basedpyright exit code.
- [ ] Perf: 100k-row table hash, 0.4 vs 0.5; golden-test wall time before/after.

### Task 7: PR
- [ ] Merge origin/main, push, PR (`Fixes ITL-910`, mentions ITL-848, ITL-927), CodeRabbit rounds.
