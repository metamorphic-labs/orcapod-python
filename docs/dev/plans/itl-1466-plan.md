# ITL-1466 implementation plan: `as_source()` of any stream

The design is `docs/dev/specs/itl-1466-as-source.md`; where it differs, its section 14 (round 7)
overrides section 13, and section 13 overrides earlier sections. This plan covers the defaults first:
serve everything stored, and key side effects by the entry. The options (a serving mode, a
side-effect caching mode) come after.

**Approved by the developer (2026-11-25):**
- Option A: a clean entry id without the recomputation index, so every `invocation_hash` and
  side-effect log key moves once.
- Moving to the layouts `pdb_v2`, `odb_v2` and `tdb_v2`.
- Renaming the side-effect log's `record_id_hash` column to `entry_id`, and `InvocationContext`'s
  `_record_id_hash_ch` with it.

**Awaiting approval:** the exact golden list, `docs/dev/specs/itl-1466/golden-changes.md`. No
golden file is written or regenerated until the developer approves it.

The prototype (local branch `scratch/itl-1466-prototype`, rounds 2 to 7) is evidence only. Each
stage is written again from `main`, with tests; nothing is copied from the prototype.

## Order and dependencies

| Stage | Content | Hash or stored-schema change | Depends on |
|---|---|---|---|
| 1 | The three nested ids, the layouts, the side-effect entry key, the operator cache's insertion time, the approved golden changes | **yes** (all of it approved, golden list pending) | approval of the golden list |
| 2 | `DerivedSource` over function and operator job nodes, serving everything stored | provisional only | stage 1; the ITL-1464 stack (#563, #566, #567) |
| 3 | `as_source()` on every object of spec section 3, with arguments, save, load and moved stores | provisional only | stage 2; D2, D3, D16 |
| 4 | Plain streams: the same recipe as their nodes | provisional only | stage 2 |
| 5 | Golden additions (provisional) and documents | additions only | stages 2 to 4 |
| Later | The serving mode; the side-effect caching mode; the remaining items of spec section 11 | none decided | the developer, after the defaults |

Each stage is one pull request against `main`. A stacked PR is updated by merging its base in,
never by rebasing. Whether stage 1 gets its own Linear issue (and its own `changelog.d/` fragment
name) is the coordinator's call: two PRs cannot both add `changelog.d/ITL-1466.md`.

## Stage 1: three nested ids and the layouts

**Ids.** All three are defined in one module (for example `core/nodes/entry_ids.py`), each
beside its preimage, and imported by every user. No other place builds a preimage.

| Id | Preimage (one-row Arrow table, hashed by the data context's Arrow hasher, labelled) | Column |
|---|---|---|
| entry id | user tags and system tags (`tag.as_table(columns={"system_tags": True})`), then `__input_data_hash` (`large_binary`, labelled digest). No recomputation index column | `__pipeline_entry_id`; written only into the side-effect log (as `entry_id`) |
| instance id | `__pipeline_entry_id` (`large_binary`, the entry id's labelled digest), then the input's `_source_*` columns in name order. An `EmptyData` input: the entry id alone | `__pipeline_instance_id`, the pipeline table's plain lookup column |
| record id | `__pipeline_instance_id` (`large_binary`), `__data_id` (`large_binary`, the result UUID's bytes) | the pipeline table's row key, read as `__pipeline_record_id` |

The column names inside a preimage are part of its recipe. Changing `__pipeline_entry_id` or the
token order moves the instance-id golden values of the approved list, so they are fixed before
the list is generated.

**Function node** (`core/nodes/function_node.py`):
- Rows are keyed by record id.
- `__pipeline_base_entry_id` and `__pipeline_recomputation_index` are removed, and so are the
  meta schema entries, the next-index query before every write, and `compute_pipeline_entry_id`.
- Every comparison that used the index to choose among rows (held rows, ephemeral placeholders,
  cross-session misses) uses `__recorded_at`.
- The lookup is by instance id.
- A write skips only an identical record id. So the same input processed twice with the same
  result writes one row (ITL-508's second row ends, by design).
- A recompute writes a new row because its result id is new.
- An input whose tokens changed is recorded as a new instance without computing.

**Side effects and `ctx`** (`side_effects.py`, `core/function_pod.py`, `invocation.py`):
- Both compute the entry id through the stage-1 module.
- `_SIDE_EFFECT_RECOMPUTATION_INDEX_COL` goes.
- The log column `record_id_hash` becomes `entry_id`, and `InvocationContext._record_id_hash_ch`
  becomes `_entry_id_hash_ch`.
- `invocation_hash` is `pipeline_hash :: entry id`. Its value moves once, by option A; the renames
  move nothing.
- Not renamed: `PostRunPayload.record_id_hash` (`hooks.py`). It holds the result UUID, not a
  pipeline record id. Its name now reads as the wrong id, so flag it to the developer in the PR
  (a public hook payload).

**Operator cache** (`core/nodes/operator_node.py`):
- `__recorded_at` (`timestamp[us, tz=UTC]`, not null) is added after the row hash is computed, so
  it is outside the key.
- Every read drops it.
- A skipped duplicate keeps its first time.

**Layouts:**
- `PIPELINE_DB_SCHEMA_VERSION = "pdb_v2"`, `OPERATOR_DB_SCHEMA_VERSION = "odb_v2"` and
  `TRACKING_DB_SCHEMA_VERSION = "tdb_v2"` (`system_constants.py`). `CURRENT_LAYOUTS` follows.
- The stale-table rule refuses a side-effecting node that has a `tdb_v1` log beside it, before
  any delivery (checked on the prototype, `probe_round7` 4). It refuses a function node that takes
  `ctx` and has a `pdb_v1` table beside it in the same way (the existing rule).
- Update `CLAUDE.md` and `.zed/rules` (the stored-tables table, the `_build_record_id_preimage`
  mention under the system tag rules).

**Golden:**
- `tests/test_hashing/golden/cases.py`: in each of the five places that pin a table, a pipeline
  table (`__pipeline_instance_id` present) pins `table[<path>].instance_ids` instead of
  `record_ids`. Fixtures are neither moved nor renamed.
- After the developer approves `golden-changes.md`, run its commands exactly (12 `--remove`, then
  the generator, then `--check`).
- The diff must equal the list: 4 entries in `pipeline-02.json`, 8 in `provisional/pipeline.json`,
  and the open revision of `v1.json`. Any other difference stops the stage.

**Provenance** (`pipeline/provenance.py`): reads the row key as `__pipeline_record_id`. Its
`entry_id` field holds the record id today; rename it `record_id`, or keep it and document it.
Decide in the PR, without moving a hash.

**Documents:**
- `reference/hashing.md`: the three ids in the hash site inventory, each with its class, and rule
  text; `test_hash_site_inventory.py` must pass.
- `concepts/function-pods.md`: the pipeline table's columns and the end of the recomputation
  index.
- `reference/data-function-types.md`, where it names `pdb_v1`.
- The design issues log, if an entry covers the recomputation index.

**Tests (stage 1):**
- Update the tests the prototype classified (spec 13.3 and 14.4): those that read the old columns
  (about 452), name a layout (about 102), inject into the removed index query (4), or compare
  stored operator tables with their times (6), plus the two behaviour changes by design.
- New tests:
  - each id recomputed from a stored row (instance and record, several rows);
  - the side-effect log keys equal the entry ids;
  - a token-only change upstream records new instances, with 0 calls and 0 deliveries;
  - `tdb_v1` and `pdb_v1` beside a side-effecting node are refused before delivery;
  - a recompute writes a new row;
  - an identical repeat is skipped;
  - the operator cache keeps the first insertion time;
  - `EmptyData`'s instance id.
- Several items: inputs with two or more tags and two or more data columns, several token
  columns in two orders, sync and async.
- Run locally: golden, then the directories of spec 13.3, one process. CI runs the full suite.

## Stage 2: `DerivedSource` over job nodes, serving everything

- **Function job node:** every stored row (every instance, duplicates of content included), with
  record id = the stored record id. A row whose result is gone is not served, as today.
- **Operator job node, `LOG` / `REPLAY`:** every stored row of its content hash, with record id =
  its row key (`_record_hash`). **`OFF`:** its current run's rows, with the same recipe as the
  plain operator stream.
- **One pair always:**
  - the source id is the derived source's;
  - the record id is as above;
  - a repeated record id raises (D6).
- **Identity:** the default id is the origin's content hash (D9, open); kind version `derived_v1`
  if it lands before 0.4.0 (D15).
- **Tokens:** single tokens kept, lists, copies and nulls fresh (D11).
- **Trace:** stops at the derived source and reports its record id (P3).
- **Collision:** the rule of ITL-1469.
- **Restriction walks:** a `DerivedSource` is a root.
- **ITL-1464 overlap:**
  - `OFF` reads through #566's paged reads and never copies the table;
  - #567's certificates: a derived source certifies only its `record_id_columns`;
  - #563's token recipes: the operator row hash reads token text.
- **Tests:** spec section 10, records and identity parts, for function and operator origins;
  `as_source` joins `test_order_invariance.py`.

## Stage 3: the method on every object

- **Spec section 3, the objects:**
  - a source returns itself;
  - a bound slot returns its bound source;
  - an unbound slot and the non-job nodes are refused;
  - a node of a loaded job behaves as its kind, except an `OFF` operator, which is refused (P5,
    its own issue).
- **Side effects, default:**
  - a side-effect stream or node gives a source over the feeding stream's rows that passed
    through, delivering nothing;
  - its source id is its own;
  - a node fires once ever per entry (the invocation log, keyed by entry id);
  - a plain stream keeps today's non-caching mode.
- **Arguments (D3, open):** on a source, `source_id=` alone is refused and `tags_as_data=` works
  as today; on a stream, `source_id=`, `tags_as_data=`, and the new `record_id_columns=`; no
  `label`.
- **Save and load (D16, open):** loads unbound and rebinds; a load-only run over an unbound slot
  warns; a missing recorded table raises.
- **Tests:** the coverage test of section 10 (each stream and node class has a case), loaded and
  moved stores.

## Stage 4: plain streams

- **Function pod stream:** record id = hash(instance id, result id). With a result store the ids
  are stable across builds; without one, each build's results are new, so the ids are too.
- **Operator stream:** the node's row recipe (the whole row with tokens, plus its content hash).
- **Raw `ArrowTableStream`:** its own rows.
- **Behaviour:**
  - streams are read when the source is read, never by `head`, a check or a preview;
  - restriction walks go into a plain origin;
  - the refusals of section 3 apply.
- **Tests:**
  - a plain operator's ids equal its node's stored ids;
  - a plain function pod's ids equal its node's with the same result store;
  - sync equals async;
  - a second process.

## Stage 5: golden additions and documents

- **Golden:** new provisional cases for the plain form's content hash, the pair, and record ids
  over function, operator and raw origins. These are additions only.
- **Documents (spec section 11):**
  - `concepts/sources.md` (one form; the provisional table), `pipeline-job.md`,
    `function-pods.md`, `troubleshooting.md`;
  - the tutorial Part 8 "Not yet" limit;
  - `reference/hashing.md` rule 8;
  - `CLAUDE.md` and `.zed/rules`.

## Later: the options, after the defaults

- **Serving mode:** one row per content (tags, system tags, data), keeping `first` or `latest` by
  `__recorded_at`, or `raise`. Each served row keeps its own record id. It is a restriction under
  `pipeline/restrictions.py` as written: one restriction per table, the caching refusal, the
  aggregation warning, and the state reported in `RunSummary`. Whether several restrictions of
  one table may coexist is a separate issue.
- **Side-effect caching mode** for plain streams (S1): fire once per row and hold the rows that
  passed; retry a failed row on the next iteration; after a load-only run or before a run, serve
  the invocation log's delivered rows (refused without `track_completion`). Design first.
- **Keying side effects by instance id:** not offered for now.
- **Later items of spec section 11:**
  - tracing across the boundary;
  - a saved origin reference;
  - superseded rows (ITL-1129);
  - ITL-1127's automatic wrapper of a raw leaf.

## Open decisions, by stage

| Stage | Open |
|---|---|
| 1 | the golden list; `PostRunPayload.record_id_hash`'s name (flag only); provenance's `entry_id` field name |
| 2 | D9 (default id), D15 (kind version, by landing date) |
| 3 | D2 (objects), D3 (arguments), D16 (save, load, moved store), P5 (its own issue) |
| Later | the serving mode's surface; S1 |
