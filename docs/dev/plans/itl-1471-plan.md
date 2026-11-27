# ITL-1471 plan: three nested row ids, v2 layouts (ITL-1466 stage 1)

Stage 1 of ITL-1466. The design is `docs/dev/specs/itl-1466-as-source.md` (sections 13 and 14)
and the stage list is `docs/dev/plans/itl-1466-plan.md`, both on the ITL-1466 pull request. This
plan covers stage 1 only, with the decisions made after that plan was written (listed below).
Nothing of stages 2 to 5 is touched: `DerivedSource` serving, `as_source()` on other objects and
plain streams stay as they are.

## Decisions this plan follows

Developer, November 25 and 26, 2026:

- Three nested ids: entry id, instance id, record id. Columns `__pipeline_entry_id`,
  `__pipeline_instance_id`, `__pipeline_record_id`.
- Layouts `pdb_v2`, `odb_v2`, `tdb_v2`.
- The side-effect log column `record_id_hash` becomes `entry_id`;
  `InvocationContext._record_id_hash_ch` becomes `_entry_id_hash_ch`.
- `PostRunPayload.record_id_hash` becomes `result_id` (it holds the result's id).
- The operator cache stores an entry-id column beside its row key, outside the key.
- An empty-data placeholder carries source-info tokens with a sentinel in place of the result id.
- A function node identifies a row by its instance id. Only side effects and functions that take
  the invocation context key by the entry id.
- The 18 golden changes of `docs/dev/specs/itl-1466/golden-changes.md`, and no other.

## The id module

`src/orcapod/core/row_ids.py`, a leaf module (it imports the constants, `EmptyData` and pyarrow,
no node or pod module), so `function_pod.py`, `side_effects.py`, `function_node.py` and
`operator_node.py` import it at module level. It holds every preimage; no other place builds one.

| Name | What it is |
|---|---|
| `ENTRY_ID_COL`, `INSTANCE_ID_COL`, `RECORD_ID_COL` | the three column names |
| `entry_id_preimage(tag, data)`, `entry_id(hasher, tag, data)` | user tags and system tags (`tag.as_table(columns={"system_tags": True})`), then `__input_data_hash` (`large_binary`, the labelled digest of `data.content_hash()`). Today's `_build_record_id_preimage`, moved; no index column |
| `instance_id_preimage(entry, data)`, `instance_id(hasher, tag, data)` | `__pipeline_entry_id` (`large_binary`, the entry id's labelled digest), then the input's `_source_*` columns in name order |
| `record_id_preimage(node_content_hash, instance, result_id)`, `record_id(hasher, node_content_hash, instance, result_id)` | `__node_content_hash` (`large_binary`, the labelled digest of the recording node's content hash), then `__pipeline_instance_id` (`large_binary`), then `__data_id` (`large_binary`, the result UUID's bytes). The node's content hash is in the record id by the developer's decision of November 26, 2026: the record id alone is the table's key, and two nodes sharing a table each keep their rows |

Each id is a `ContentHash` of the data context's Arrow hasher (`arrow_v1`); it is fed onward as
`to_prefixed_digest()` and stored the same way. No new hash label.

## Proposals to confirm

1. **Operator cache entry-id column:** `__pipeline_entry_id` (`large_binary`, not null), the
   coordinator's suggestion. It is `entry_id(hasher, tag, data)` of the output row: its tags,
   system tags and the content hash of its data. It is computed in the loop that computes the row
   key, and appended after the key with `__recorded_at`, so neither enters the key.
2. **Sentinel:** the text `EPHEMERAL_RESULT_GONE`, one constant in `core/row_ids.py`
   (`EPHEMERAL_RESULT_GONE`). A placeholder's token for output column `c` is
   `<function uri joined by ":">::EPHEMERAL_RESULT_GONE::<c>`: a real output's token with the
   sentinel where the result id (32 hex characters) stands. It is not hexadecimal, so it names no
   result row, and it holds no `::`. The token is built by the function that builds a real
   output's tokens (taken out of `PythonDataFunction` into one helper both call).
3. **Where the placeholder keeps its tokens:** `EmptyData` takes a one-row Arrow table of
   `_source_*` columns and returns it from a new method; `source_info()` returns the same tokens.
   Its unused field `empty_source_info` ("write logic deferred") is replaced by it. The emitting
   node gives a token for each function output, and for a data-tag that falls through, the input's
   stored token of that column (a real output keeps it too).
4. **A downstream node given a placeholder** computes its instance id the normal way from those
   tokens. It still writes no pipeline row for a placeholder input, as today (the result comes
   from the result store by input hash). Without tokens (an `EmptyData` built with none) the
   instance id is the hash of the entry id alone.
5. **`FunctionRun.entry_id`** (`types/provenance.py`, public) holds the row key: renamed
   `record_id`. No hash moves.

## Files

Library:

- `core/row_ids.py` (new).
- `system_constants.py`: the three layout constants. `contexts/stored.py` follows through
  `CURRENT_LAYOUTS`; its texts that name a layout are updated.
- `core/nodes/function_node.py`:
  - `_pipeline_meta_schema`: `__pipeline_instance_id` in place of `__pipeline_base_entry_id`; no
    `__pipeline_recomputation_index`.
  - `compute_base_entry_id` becomes `compute_instance_id`; `compute_pipeline_entry_id`,
    `_build_entry_id_preimage` and `_build_record_id_preimage` go.
  - `_add_pipeline_record`: no query before the write; the row key is the record id.
  - `__recorded_at` chooses among the rows of one instance (held rows, placeholders,
    cross-session misses), where the index did.
  - Every `base_entry_id` name becomes `instance_id`.
  - The placeholder gets its tokens.
- `side_effects.py`, `core/function_pod.py`, `invocation.py`: the entry id from the module;
  `_SIDE_EFFECT_RECOMPUTATION_INDEX_COL` goes; the log column and the context field are renamed.
- `hooks.py`, `core/function_pod.py`: `PostRunPayload.result_id`.
- `core/nodes/operator_node.py`: `__recorded_at` and `__pipeline_entry_id`, added after the row
  key; one list of the bookkeeping columns every serving read drops.
- `core/data_function.py`: the token helper. `core/datagrams/tag_data.py`: `EmptyData`.
- `pipeline/provenance.py`, `types/provenance.py`: the row key read as `__pipeline_record_id`.
- `scripts/hash_audit_example.py`: the entry, instance and record ids in place of sites 7 and 8.

Documents:

- `docs/reference/hashing.md`: sites 7 to 10, the label table, the hash site inventory, the
  worked example values.
- `docs/concepts/function-pods.md`, `docs/reference/data-function-types.md`, and every page under
  `docs/` outside `docs/dev/` that names an old column, layout or `record_id_hash`.
- `CLAUDE.md` and `.zed/rules` together: the stored-tables table, the
  `_build_record_id_preimage` mention.
- `changelog.d/ITL-1471.md`.
- The design issues log: an entry is edited only if one covers the recomputation index.

Golden: `tests/test_hashing/golden/cases.py`, the five places that pin a table; then the commands
of `golden-changes.md`, exactly.

## Tests

New (`tests/test_core/test_row_ids.py`, `tests/test_core/nodes/`, the side-effect and operator
node directories), each with several rows, two or more tags and two or more data columns, token
columns in two orders, sync and async:

- each id recomputed from a stored row (instance and record id of every pipeline row);
- the side-effect log's keys and its `entry_id` column equal the entry ids;
- a token-only change upstream records new instances, with 0 calls and 0 deliveries;
- `tdb_v1` and `pdb_v1` beside a side-effecting node are refused before any delivery;
- a recompute writes a new row; an identical repeat is skipped;
- the operator cache's `__pipeline_entry_id` equals the recomputed entry id, a re-passed row
  keeps its first `__recorded_at`, and no serving read shows either column;
- the placeholder's tokens hold the sentinel, and a downstream instance id is computed from them;
- `PostRunPayload.result_id` equals the output's result id.

Updated, counted by class in the pull request: tests that read the old columns, tests that name a
layout, tests that inject into the removed index query (removed), tests that compare stored
operator tables (compared without the time and the entry id), and the two behaviour changes by
design (one row for a repeated input; a corrected token recorded as a new instance).

Locally: targeted tests only, one pytest process at a time. CI runs the full suite.

## Order of commits

1. `docs(plans)`: this plan.
2. `feat(nodes)`: `core/row_ids.py` with its unit tests.
3. `feat(nodes)!`: the function node on instance and record ids, `pdb_v2`.
4. `feat(side-effects)!`: the entry key, `tdb_v2`, the `entry_id` and `result_id` renames.
5. `feat(nodes)!`: the operator cache's `__recorded_at` and entry id, `odb_v2`.
6. `feat(nodes)`: the placeholder's sentinel tokens.
7. `test`: the existing tests updated.
8. `test(golden)`: the cases pin instance ids; the approved golden changes.
9. `docs`: the reference and concept pages, `CLAUDE.md`, `.zed/rules`, the changelog fragment.

## Stop conditions

- `generate_golden_hashes.py --check` shows an entry outside the approved list moving, or an
  approved value differing from the listed one: no golden file is changed, and the entries and
  values go to the coordinator.
- A design point this plan does not settle: asked before it is implemented.
