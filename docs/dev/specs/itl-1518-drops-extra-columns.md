# ITL-1518: a function pod reads only its own columns (`drops_extra_columns`)

Status: **measurement and plan only. The implementation is deferred** (developer, November 27, 2026).
Nothing in the library, the tests, the golden files or the agent rules changes with this document:
`pod(a)` still places the automatic `SelectDataColumns` step (ITL-807). The accepted limitation
meanwhile: two functions that take the same columns of one stream share that automatic selection
node, so they cannot take function-specific restrictions on its edge (a filter still works).

No golden file was written or edited (`--check` and `--stdout` on a local prototype only).

## 1. The design measured

Developer, November 26, 2026 (ITL-1472, "stage 3a"): there is no separate selection node. A function
pod's node takes its input stream as given and reads only the data columns that are parameters of its
function. In the default mode, on an input with data columns the function does not take, the node's
identity gets one extra element, the label `drops_extra_columns`. In every other case (`strict=True`,
`process`, no extra column) the identity is exactly today's. The dropped names are not in the identity.
The input data hash covers only the columns the function takes, so stored results stay shared.

Developer, November 27, 2026: the behaviour of the automatic-drop mode and the golden values that
pertain to it are provisional.

## 2. How it was measured

- Base: commit `3298bf5c`, the head of PR 583 (ITL-1471: three row ids, `pdb_v2`, the record id
  covering the node's content hash). On this base `generate_golden_hashes.py --check` reports that
  every group matches.
- Prototype: `itl-1518/prototype.patch`, the prototype of PR 580 unchanged (7 library files). It
  applies cleanly to the base. `O34_MARK=label` selects the developer's variant. It is a measuring
  tool, not the implementation (section 6).
- Commands, on a tree with the patch applied and not committed:

  ```
  O34_MARK=label uv run python tests/test_hashing/golden/generate_golden_hashes.py --check
  O34_MARK=label uv run python tests/test_hashing/golden/generate_golden_hashes.py --stdout > computed.json
  uv run python docs/dev/specs/itl-1518/compare_golden.py computed.json
  ```

- Evidence beside this file, in `itl-1518/`: `golden_check.out` (the check's own list),
  `golden_differences.out` (every pinned entry of every golden file against the computed value, with
  both values), `compare_golden.py`.

## 3. (a) The golden entries that move

**34 entries differ, all in one provisional file, in two cases. No frozen file has an entry that moves.**
The count and the entries are those of the earlier measurement; what PR 583 changed is the name and
value of one kind of entry (the pipeline table now pins `instance_ids` at a `pdb_v2` path, where it
pinned `record_ids` at `pdb_v1`) and the size of the cases (75 entries each, 88 before).

| File | Kind | Entries that move |
|---|---|---|
| `provisional/pipeline.json` | provisional | 34 |
| `pipeline-01.json`, `pipeline-02.json` | frozen at 0.4.0 | 0 |
| every other file (`arrow`, `datagram`, `schema`, `semantic`, `torch`; frozen and provisional) | | 0 |

2480 entries were computed (arrow 222, datagram 329, pipeline 1372, schema 335, semantic 190, torch
32). `compare_golden.py` also lists three entries pinned as `null` in a frozen file that `--stdout`
does not print; they are not differences (the base's own `--check` passes with them, and the
prototype's `--check` reports 34).

The cases are `rejoin_job` and `rejoin_job_async` (both declare `Rejoin`), the only golden cases in
which a pod is called on a stream with a data column its function does not take:
`golden_ratio(age, weight)` over the rejoined stream, whose data columns are `age`, `score`, `weight`,
`height`. The two cases pin equal values, sync and async. Per case, 17 of 75 entries:

| Mark | Entries per case | Why |
|---|---|---|
| S | 12 | There is no select node between the rejoined stream and `ratio` |
| H | 2 | `ratio`'s node is the pod over the rejoined stream (it was the pod over a select of it), and its identity tuple ends with `drops_extra_columns` |
| P-old, P-new | 2 | A pipeline table is at `.../schema:<pipeline hash>/pdb_v2`; the pipeline hash moved. The two entries hold equal instance ids |
| L | 1 | `table_paths` names the new pipeline-table path; the result table path is in the list unchanged |

The list for `rejoin_job`; `rejoin_job_async` has the same 17 entries with the same values, under
`pipeline:rejoin_job_async.` (its tables are under `golden_rejoin_job/` too). `<st>` is
`b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4`, `<uri>` is
`golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1`.

| # | Entry (`pipeline:rejoin_job.`...) | Pinned today | New | Why |
|---|---|---|---|---|
| 1 | `node[SelectDataColumns].content_hash` | `semantic_v1:96904fb283607c88e95e96842137d3683aea92ddb26978b2cd1c075eb5f8ebf7` | (none) | S |
| 2 | `node[SelectDataColumns].pipeline_hash` | `semantic_v1:646655f033644fd96e1c1ea0fd56af969e3afe16c76b1cb176aa343a3d2ebbe5` | (none) | S |
| 3 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}]._tag_record_id::<st>` | `row_0` | (none) | S |
| 4 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}]._tag_source_id::<st>` | `dict_v1:rejoin_origin` | (none) | S |
| 5 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}].data_hash` | `arrow_v1:000002d1c5b49de38ef6efc552c2a47b8ab59dd267f8eb55d91814edee99b1b52acc65` | (none) | S |
| 6 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}]._tag_record_id::<st>` | `row_1` | (none) | S |
| 7 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}]._tag_source_id::<st>` | `dict_v1:rejoin_origin` | (none) | S |
| 8 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}].data_hash` | `arrow_v1:000002338ce8710f5ecb156a72c8446651adb7b0c21635d0aa92d298f09da56faed0f8` | (none) | S |
| 9 | `node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}]._tag_record_id::<st>` | `row_2` | (none) | S |
| 10 | `node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}]._tag_source_id::<st>` | `dict_v1:rejoin_origin` | (none) | S |
| 11 | `node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}].data_hash` | `arrow_v1:0000023401f1ce40c8cfdd6e4f343d7ffdcd51c1f0521f44cf6030e27e1ca0da154616` | (none) | S |
| 12 | `node[SelectDataColumns].system_tag_columns` | `_tag_source_id::<st>`, `_tag_record_id::<st>` | (none) | S |
| 13 | `node[ratio].content_hash` | `semantic_v1:84f5f8ce9de0dac0e361d0b0d69f186075974bd359ccbee04ca58f2cc6fefbac` | `semantic_v1:511d3c57512025445c23773ce648ee29b277882027ddf8a1e5080b1e2adc06a3` | H |
| 14 | `node[ratio].pipeline_hash` | `semantic_v1:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba` | `semantic_v1:64b6c04f3dd104e05264b806b1b24c1e20ac153ec8afd6df94d2afec64fe6b87` | H |
| 15 | `table[golden_rejoin_job/<uri>/schema:2940d188...e7ba/pdb_v2].instance_ids` | `arrow_v1:00000264d51dd01edfbb777f7047d25e7cdb50e4f52e6b01454ffc56b70aa5b166b920`, `arrow_v1:0000029b103c78b5c06ac0fe36d793fe576563a8609b835d662cf78a5828436f9b8aa3`, `arrow_v1:000002b71bb228680318fe2cc0fc599241136f0fedc28658bda79e2a90915b87c82ef0` | (none) | P-old: the path is no longer produced |
| 16 | `table[golden_rejoin_job/<uri>/schema:64b6c04f...6b87/pdb_v2].instance_ids` | (not pinned) | the same three instance ids as entry 15 | P-new: the same rows at the new path |
| 17 | `table_paths` | `golden_rejoin_job/_result/<uri>/rev1/rdb_v1`, `golden_rejoin_job/<uri>/schema:2940d188...e7ba/pdb_v2` | `golden_rejoin_job/_result/<uri>/rev1/rdb_v1`, `golden_rejoin_job/<uri>/schema:64b6c04f...6b87/pdb_v2` | L |

The full keys and values are in `itl-1518/golden_differences.out`.

Unchanged in the two cases: the result table path and its `input_data_hashes` entry (the input data
hash covers `age` and `weight` only, as after today's select), the pipeline table's instance ids,
every row's record-id and source-id system tag and data hash, every system-tag column name, and every
other node of the job.

A record id (`__pipeline_record_id`) covers the node's content hash since PR 583, so the record ids
of the moved node's rows change with entry 13. No golden entry pins a record id (it holds a random
result id), so this adds no entry to the list.

The new values of entries 13, 14, 16 and 17 are those of the prototype. They are the final values
only if the implementation hashes the same structure: `(pod, inputs, "drops_extra_columns")`. Measure
again before the golden step.

## 4. (b) How the mode is declared provisional

Modelled on the provisional operators (ITL-1266) and sources (ITL-1314). The mode is neither an
operator nor a source, so it needs a list of its own. Proposal:

- **One list**, `PROVISIONAL_MODES` in a new leaf module `core/provisional_modes.py`: name to the
  issue that decides it, today `{"drops_extra_columns": "ITL-1518"}`. A test checks the list against
  the one place that uses each name, as the operator test checks `PROVISIONAL = True`.
- **Warning**: `ProvisionalHashWarning`, once per process, through the existing helper
  `utils/provisional.warn_provisional_use("drops_extra_columns", ...)`. It is issued where the mode
  is entered: a pod call in the default mode that drops at least one column, and the load of a saved
  job whose function node carries the mark. A pod call that drops nothing, `strict=True` and
  `process` never warn.
- **Use record**: the marked node notes its use again where its identity is read (`content_hash`,
  `pipeline_hash`, the identity structures), with `note_provisional_use`, as an operator and a source
  do. That is what lets the golden guard see a use behind an object built inside the case.
- **The mark is not the label.** The list's key is the mode's name and never enters a hash, an
  identifier, a config or a stored path. The identity label is a separate constant
  (`DROPS_EXTRA_COLUMNS`), defined once beside the node's identity code. They happen to have the
  same text.
- **Golden cases**: a case in which a pod drops columns declares
  `@case(..., provisional=("drops_extra_columns",))`; the name joins the names a case may declare
  (as `DECLARABLE_PROVISIONAL_SOURCES` does for `DerivedSource`). `compute_group_kinds` refuses a case
  that uses the mode undeclared. Its entries live in `data/v1/provisional/<group>.json`, outside the
  history; no release freezes them; they change only with the developer's approval of the exact list.
- **Everything downstream is provisional**, as for an operator: a node fed by a marked node has
  hashes that hold the label.

What must be declared, and what is relocated:

- `rejoin_job` and `rejoin_job_async` become `provisional=("Rejoin", "drops_extra_columns")`. Their
  entries are already in the provisional file.
- **No frozen case has to be relocated.** A node in the mode has other hashes than today, so a frozen
  case using it would show in section 3; none does.
- New cases for the mode (section 6) are provisional from the start.

Two consequences to decide knowingly:

1. Calling a pod on a table wider than its parameters is the common call. With this proposal most
   user pipelines issue the warning once per process and have provisional hashes below that node,
   until ITL-1518 is decided. A user avoids it with a hand-written `select_data_columns`, `process` or
   `strict=True`.
2. When `Rejoin` leaves the provisional list, the two rejoin cases stay provisional for as long as
   the mode is. A frozen golden case can never call a pod on a wider stream; it writes the select by
   hand.

## 5. (c) What a user with an existing store sees

Measured with `itl-1518/store_run.py` (functions in `store_functions.py`; each call appends a line to
a file, so calls are counted across processes): one job on a Delta store with five function nodes,
run on the base code, run on the base code again (control), then run with the prototype on the same
store. Outputs: `store_run_before.out`, `store_run_before_again.out`, `store_run_after.out`.

| Node | Input | Before | After |
|---|---|---|---|
| `double_wide` | 3 data columns, 1 taken | select node + pod | no select node; content and pipeline hash move; pipeline table at a new path |
| `triple_down` | the output of `double_wide` (no extra column) | pod | hashes move (downstream of a moved node); pipeline table at a new path |
| `add_two` | two sources, each with an extra column | `Join` + select node + pod | `Join` unchanged; no select node; hashes move; pipeline table at a new path |
| `scale_datatags` | 2 data columns, 1 taken, 2 data-tags | select node + fall-through pod | no select node; hashes move; pipeline table at a new path |
| `halve_narrow` | 1 data column, taken | pod | unchanged: same hashes, same pipeline table |

- **No function is called again.** Calls in the first run: 3 per function (15). In the control run: 0.
  In the run with the prototype: **0**. Every node returns its 3 rows.
- **Result tables do not move and get no new row**: the five `_result/.../rev1/rdb_v1` tables hold 3
  rows before and after. The input data hash of a row is the one today's select gives.
- **Four pipeline tables move** (`.../schema:<pipeline hash>/pdb_v2`): the three nodes whose pod drops
  columns, and the node downstream of one. Each new table gets 3 rows on the first run. The four old
  tables stay where they are, with their rows, unread and unchanged. They are at the current layout,
  so nothing reports them as stale tables.
- **The select node had no table.** An operator job node stores nothing unless it has a cache mode;
  the store of the run holds no table for the three select nodes. With `LOG` or `REPLAY` on such a
  node its operator cache table would be left unused. That case was not run.
- The source nodes and the `Join` keep their hashes.
- Not measured: a saved job written before the change and loaded after it (its file names the select
  node, so it would load as the old graph: section 6), `as_source()` over a moved node (it reads the
  new pipeline table, which is empty until the first run), a side-effect pod downstream of a moved
  node (its log is keyed by `pipeline_hash :: entry id`, so the pipeline hash moving would deliver
  again: **to be measured before the implementation**, since a delivery is not a function result).

Also on the prototype (`folded_confirm.py` of PR 580, run on this base): a job with the hand-written
select on the same store makes 0 function calls after the folded job computed, with another pipeline
hash and pipeline table; with no extra column the default call, `strict=True` and `process` have one
content hash and one pipeline hash; `strict=True` and `process` refuse an extra column; the input's
columns in another order give the same hashes.

## 6. (d) Implementation plan (for when the work is taken up)

The prototype narrows the input with a hidden select whose stream reports its input's identity. The
implementation does not keep that: the node reads the columns itself.

Library:

- `core/function_pod.py`
  - `DROPS_EXTRA_COLUMNS = "drops_extra_columns"` and one function returning the identity tail
    (`()` or `(DROPS_EXTRA_COLUMNS,)`); no other literal anywhere (a test greps).
  - `_FunctionPodBase.__call__`: no `SelectDataColumns`. It joins as `process` does, computes the
    dropped names (data columns that are neither parameters nor fall-through data-tags), validates
    the kept columns, logs the dropped names once at info, and builds the stream or node with the
    mark. Nothing dropped, `strict=True` or `accepts_arbitrary_keywords`: exactly `process`.
  - The mark is a property of the invocation (the output stream and its node), not of the pod
    object: the pod's own identity, its `uri` and the result table path are untouched.
  - `FunctionPodStream`: identity tail; rows narrowed before the call.
  - `FallThroughPod`: the kept columns are the parameters and its fall-through tags; a data-tag is
    never counted as dropped and keeps its source-info token as today.
- `core/streams/base.py` (`StreamBase.identity_structure`) and `pipeline/pod_invocation.py`
  (`PodInvocation.identity_structure`): the same tail, from the one function, so the invocation's
  hash equals the compiled node's.
- `core/nodes/function_node.py`
  - `FunctionNodeBase`: the input schema check ignores the dropped columns when the node has the mark,
    and refuses them when it has not.
  - One narrowing step for an input row, used by the sync path (`execute`, load-only), the async path
    (`async_execute`, load-only) and the placeholder path, placed before the input data hash, the
    entry id, the instance id and the call. The source-info table the instance id reads holds the
    kept columns only, as after today's select, so instance ids are those of entries 15 and 16.
  - The typed zero-row table (`function_zero_row_table`) and `head()` take the input's tags and the
    declared outputs; check they never read a dropped column.
- `pipeline/graph.py`, `pipeline/serialization.py`: `"drops_extra_columns": true` in a function
  node's descriptor, only when marked; the loader rebuilds the node with it. A job saved before the
  change still names its select node and loads as that graph, with its old hashes: nothing is
  rewritten. A descriptor with the key on a node with no extra column is refused.
- `pipeline/provenance.py` (`trace`, `why_missing`): narrow the input row as the node does before
  comparing the stored input data hash; a trace no longer passes through a select node.
- `explain()`, `diagram()`: name the ignored columns on the pod, computed from the input's columns and
  the function's parameters (the names are not stored).
- `core/provisional_modes.py` and the warning and use notes of section 4.
- Edge restrictions and probes (ITL-1472) then see one edge per function.

Behaviour to hold, each with a test:

| Case | Expected |
|---|---|
| `pod(a)`, no extra column | today's identity, no warning |
| `pod(a)`, extra columns | label appended; one info log; `ProvisionalHashWarning` once |
| `pod(a, strict=True)`, `pod.process(a)`, no extra column | equal to `pod(a)` in every hash |
| `pod(a, strict=True)`, `pod.process(a)`, extra columns | refused, as today |
| `pod(a, b)` | the automatic `Join` stays an ordinary step; the pod's node is over the join, marked when the joined stream has extra columns; a data column shared by two inputs fails at the join |
| data-tags | fall through with their token; never dropped; never counted as extra |
| missing required column, type mismatch | an error in every form, before any node is built |
| hand-written `select_data_columns` | a select node and today's pod: another pipeline hash, the same result table and entries |
| saved job, marked node | round trip keeps the mark and every hash; a load issues the warning |
| existing store | no function call; results served by input hash |

Tests (targeted; public interface; CLAUDE.md "Testing conventions"):

- Inputs with two or more tag columns and three or more data columns, two or more dropped, two or
  more kept; from `tests/test_core/operators/several_items.py` where it fits.
- Orders: the input's data columns, tag columns and table layout in several arrangements, and the
  function's parameters in another order than the columns: one content hash, one pipeline hash, one
  set of instance ids, equal rows. A case in `tests/test_core/test_order_invariance.py`.
- Roles: plain tags, data-tags, data; a dropped column that is nullable, a list, a logical type.
- Sync and async: equal rows, hashes, stored tables and events
  (`test_async_sync_equivalence.py` covers operators; the pod case goes beside the function node's
  own sync and async tests).
- Two functions over one stream that take the same columns: two nodes on two edges, no shared node.
- The replacement of ITL-807's tests (`test_function_pod_auto_select.py`,
  `test_fall_through_pod_wiring.py`): the equality with the hand-written select is withdrawn; the
  equality of input data hashes and of results is kept.
- Tests that name `SelectDataColumns` in a node list, a diagram or printed output
  (`test_node_staleness_after_run.py`, `test_source_display_names.py`, `test_diagram.py`,
  `test_doc_samples.py`), provenance (`test_provenance_review.py`), and the paths the prototype does
  not narrow (`test_result_database.py`, `test_saved_job_with_data_tag_operator_nodes.py`,
  `test_function_commit_dependencies.py`). The earlier run on the prototype counted 74 failing tests
  in these areas; **that run was not repeated on this base**.
- The provisional list test, the warning (once per process; none in the strict forms), the golden
  guard refusing an undeclared use.

Golden step, only with the developer's approval of the list measured again at that time:
`--remove` each entry of the list, then the generator writes the new values; the diff of the golden
folder is the list and nothing else. New provisional cases: a pod dropping two columns in both input
column orders (one identity), beside the hand-written select (another pipeline hash, equal input data
hashes), under a `ctx` pod and a fall-through pod.

Documents: `CLAUDE.md` and `.zed/rules` ("Calling a function pod": the automatic step and its
equality with the hand-written select are withdrawn), `docs/concepts/function-pods.md` and the pages
that show `SelectDataColumns` after a pod call, the provisional mode and what an existing store sees
(section 5), `changelog.d/ITL-1518.md` (breaking change; known limitation: provisional). No hash site
is added: the label is a plain string in a structure `semantic_v1` hashes, and it names no algorithm.

## 7. Open points

- The side-effect question of section 5 (does a moved pipeline hash deliver again downstream).
- Whether the warning on the common call (section 4, consequence 1) is wanted, or the mode should be
  provisional in the documents and golden files only.
- A function revision with the same `uri` and other parameters drops other columns under one
  identity, as two such revisions are one identity today.
