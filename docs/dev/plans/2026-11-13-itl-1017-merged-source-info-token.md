# ITL-1017: one source-info token for a value merged from equal elements — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On the workshop slice (PR #342), when a data value is the merge of several equal elements
(the scalar key of a `GroupBy` on a referential tag; the shared key of a `Join` that is referential on
more than one input), its source-info value is one scalar token, a deterministic digest of the member
tokens. Provenance keeps the shape of its data column.

**Architecture:** One general helper, beside `arrow_utils.fold_system_tag_values`, folds a sequence of
source-info tokens into one token. It does not know about tags. `GroupBy` calls it for every
`_source_<c>` whose data column `c` is a group key; `Join` calls it (per leaf, so a list-valued key keeps
its list shape) for every merged referential column, once over all inputs' values.

**Status log (successor notes):**
- 2026-11-13: plan written; baseline measurements taken (`ids_before.json`, `bench_before_*.json` in the
  session scratchpad; numbers copied below once known). Waiting for coordinator approval (STEP 1 ask).
- 2026-11-13: approved by the coordinator (kind `merged`, sorted members, one flat fold for N-ary joins,
  per-element fold for list keys; nested token fed as-is accepted for the slice, recorded as an open point).
  Done: 5e8028e1 (fix + tests), aaff7da8 (docs, changelog, `design_issues/ITL-1017-RT2.md`), 8d7e3c9e
  (merge of origin/main; new main tests pinned to `tag_only_columns=True`). PR #342 body updated.
  Hash/id diff before/after (also on the merged head): identical except `_source_<key>` types.
- Linear comments were refused by the session's permission classifier: the texts below are for the
  coordinator to post.
  - ITL-1017: implementation summary + OPEN POINT (nested merged token fed with its label vs bare hex
    with a structural marker `{"merged": "<hex>"}`; see RT2).
  - ITL-1019: `OperatorNode._store_output_stream` (`operator_node.py` ~943-990) hashes each stored row
    with `source=True` into `_record_hash`; once referential streams can be stored, a group membership
    change (new merged token) moves that row's dedup key. Decide there whether `_record_hash` keeps
    hashing source info.

---

## Findings for the plan (STEP 1 of the brief)

### (a) Model: how system tags fold or extend today

- `arrow_utils.fold_system_tag_values` (`src/orcapod/utils/arrow_utils.py:1358`): `source_id` members fold
  to SHA-256 hex of the canonical JSON array of the members (`separators=(",", ":")`,
  `ensure_ascii=False`, `null` for a missing member), in the member order the caller gives; `record_id`
  members fold through `uuid5`. Stable across processes; no `hash()`, no set.
- `GroupBy` (`core/operators/group_by.py:213-243`) sorts members by non-key tags then every `record_id`
  column and folds each system tag over that order. `Batch` (`batch.py:91`) folds in batch order.
- `Join` extends names by `::{pipeline_hash}:{canonical position}` and then sorts the values of
  same-hash system-tag columns per row (`arrow_utils.sort_system_tag_values`), which is what makes it
  commutative when two inputs tie on pipeline hash.

### Proposed token

- Preimage: the canonical JSON array (same `json.dumps` settings as `fold_system_tag_values`) of the
  member tokens **sorted** (`None` last), each token fed as it is (a nested merged token included).
- Text: `merged:<label>:<64 hex>`, with the label `source_fold_v1` a constant defined once beside the
  algorithm, built through `ContentHash(label, digest).to_string()` (no label literal elsewhere, the
  label never in its own preimage).
- Sorting inside the helper (rather than the caller's member order): the provenance of N equal values is
  a multiset; sorting makes one algorithm for every caller, makes the join commutative even when inputs
  tie on pipeline hash (the reason system tags are sorted after a join), and makes the group token
  independent of member order. The group's member order is itself deterministic, so this is a choice,
  not a fix.
- Always a digest, also for one member (a group of one; a type or form must not depend on data values).
- N-ary join: fold once over all inputs' values (flat), not pairwise, because a pairwise fold
  `f(f(x, y), z)` depends on the chain order, which ties on pipeline hash leave to the argument order.
  The chain keeps the inputs' values in a temporary list and folds when the join emits (sync: tracked per
  merged column in the loop; async: the staggered chain's last step folds).
- A list-valued merged key (two grouped streams sharing a non-key referential tag): the values are
  equal lists, so the fold is per element (zip), and the source-info keeps the list shape. Unequal shapes
  raise.

### (b) Does source info enter any hash or key today?

- Data content hash: `Datagram.identity_structure` is the data table only
  (`core/datagrams/datagram.py:445-454`); source info is held apart. Not in it.
- Function node entry id / record id: `_build_record_id_preimage` (`core/nodes/function_node.py:124-150`)
  = system tags + input data hash; `compute_pipeline_entry_id` (`:2297`) adds the index. Not in it. The
  pipeline table STORES the input's `_source_*` columns (`:2437-2443`): stored, not hashed (the size
  problem).
- **Operator node `_record_hash`** (`core/nodes/operator_node.py:943-990`): the row is read with
  `source=True` and hashed whole: source info IS in the operator cache's per-row dedup key. On the slice,
  storing a referential stream in an operator node raises (ITL-1019, `operator_node.py:937`), and in plain
  mode no merged column exists, so no stored key moves today; when ITL-1019 lands, a membership change of
  a group moves that row's `_record_hash`.
- A producer-less `ArrowTableStream`'s `identity_structure` hashes its table with source columns
  (`core/streams/arrow_table_stream.py:181-191`). An operator's output stream has a producer (pod identity
  + upstreams), so it is not reached through operators.
- Pipeline hash: schema only (source columns not in `output_schema`).
- Measured: `ids_before.json` (referential GroupBy + Join + function node, every content/pipeline hash,
  tag/data hash per row, system tags, every stored id column) will be diffed against the build with the
  fix: expected identical.

### (c) Who parses source-info tokens

No code parses or resolves a token (`SourceRegistry` is keyed by source id, never fed a token; no
`split("::")` on source info in `src/`). Docs describe tokens (`docs/concepts/sources.md:420`), and
`docs/concepts/referential-tags.md:81` says the join's source info "lists both inputs' tokens": update.
Tests asserting the list form: `tests/test_core/referential_tags/test_operators.py`, the async
equivalence cases. An aggregate token is recognised by its kind prefix `merged:`; resolution answers
"merged value" (nothing to resolve to one row).

### (d) Idempotence and nesting

One member: a digest. Grouping a grouped stream again on a coarser key: the key's members are digests
(or tokens), folded again: digest of digests, deterministic. Join after GroupBy: members are the group
digest and the other input's token. `Batch`: no key; every column and its tokens become lists; no fold
(and `Batch` still refuses referential input, ITL-1017 rest). `MergeJoin` refuses referential input
(ITL-1018); its merged columns are lists of possibly different values, not a merge of equal elements.
`SemiJoin` keeps the left only: no merged column.

### (e) Plain mode

A plain tag has no source-info column, and plain+plain joins merge nothing: the helper is never called;
no golden value moves; no new term in any hash.

### (f) Measurement

The workshop notebook needs Spiral: not run here. Synthetic stream of the same shape
(`bench_workshop_shape.py`, scratchpad): gaze 28,780 rows by 4 experiment keys, `drop_nulls`,
`group_by` to 1 row, spikes 63,349 rows to 32 neurons, `onsets` and `psth` pods, Delta store, sync, one
job run twice; wall time first / re-run, store size, `_source_*` token count in `psth`. Referential before
/ after, plain as reference.

---

## Tasks (after approval)

### Task 1: failing tests (public interface)

**Files:** `tests/test_core/referential_tags/test_merged_source_info.py` (new),
`tests/test_core/operators/test_async_sync_equivalence.py`.

- Shape rule: for `GroupBy` (key referential, non-key referential, grouped twice), `Join` (ref+ref,
  plain+ref, 3 inputs, after GroupBy), `SemiJoin`, select/drop/map/filter/drop_nulls: every `_source_<c>`
  in `as_table(columns={"source": True})` has the list depth of `c`.
- Token form: `merged:source_fold_v1:` + 64 hex; equal for reordered input rows; equal for permuted join
  arguments (including a pipeline-hash tie); equal across two processes (subprocess); one-member group
  gives a token; grouping twice gives digest of digests (equals the helper over the first fold's tokens).
- Async equals sync (equivalence file).
- Run, see them fail.

### Task 2: the helper

`src/orcapod/utils/arrow_utils.py`: `MERGED_SOURCE_INFO_KIND = "merged"`, `SOURCE_FOLD_LABEL =
"source_fold_v1"`, `fold_source_info_tokens(tokens) -> str`. `src/orcapod/core/referential_tags.py`:
replace `merged_source_tokens` by a shape-aware `merge_source_info(values) -> SourceInfoValue`.

### Task 3: GroupBy

Key source columns fold (`_source_<k>` for `k` in `by`), typed as the input's field (zero rows too).

### Task 4: Join sync and async

Flat accumulation, fold at emit; async `_merge_data` final step folds.

### Task 5: docs, changelog fragment, design-issue entry; merge origin/main; measure; push; Copilot.
