# ITL-1472, O34: golden entries that move when a function pod ignores extra columns

Status: measurement for approval. **No golden file was written or edited** (`--check` and `--stdout` only).
The developer approves the exact list of section 2 before any golden change.

## 1. What was measured

The developer's direction (2026-11-26): there is no separate selection node. A function pod's node takes its
input stream as given and reads only the data columns that are parameters of its function. In the default
mode, on an input with data columns the function does not take, the function node's identity tuple gets one
extra element, the label **`drops_extra_columns`**; in every other case (strict mode, `process`, or no extra
column) the tuple is exactly today's. The ignored names are not in the identity: they follow from the upstream's
schema, which its hash holds, and the function's parameters.

Prototyped on a local scratch branch, not pushed: `folded-prototype.patch` beside this file (7 library files;
`O34_MARK` chooses the variant, `label` being the developer's). Measured with

```
O34_MARK=label uv run python tests/test_hashing/golden/generate_golden_hashes.py --check
O34_MARK=label uv run python tests/test_hashing/golden/generate_golden_hashes.py --stdout
```

and every pinned entry of every golden file compared with the computed value (2480 computed entries in six
groups: arrow 222, datagram 329, pipeline 1372, schema 335, semantic 190, torch 32).

**Result: 34 entries differ, all in two cases of one provisional file. Nothing else moves, and no frozen
golden file has an entry that moves.**

- File: `tests/test_hashing/golden/data/v1/provisional/pipeline.json` (the cases declare `Rejoin`; no release
  freezes it).
- Cases: `rejoin_job` and `rejoin_job_async`, the only golden cases in which a pod is called on a stream with a
  data column it does not take: `golden_ratio(age, weight)` over the rejoined stream, whose data columns are
  `age`, `score`, `weight`, `height`. **The default mode drops columns `height` and `score`**, so the node's
  identity tuple ends with `drops_extra_columns`. The two cases
  pin the same values, sync and async.
- Per case 17 of 88 entries: the 12 entries of the `SelectDataColumns` node leave (S), the pod node's 2 hashes
  move (H), its pipeline table moves to a new path with the same rows (P-old, P-new), and the list of table
  paths names the new path (L).

| Mark | Entries per case | Why |
|---|---|---|
| S | 12 | There is no select node between the rejoined stream and `ratio` |
| H | 2 | `ratio`'s node is the pod over the rejoined stream (it was the pod over a select of it), and the node's identity tuple ends with `drops_extra_columns` |
| P-old, P-new | 2 | A pipeline table is at `.../schema:<pipeline hash>/pdb_v1`; the pipeline hash moved. The rows are the same (the two entries hold equal record ids) |
| L | 1 | `table_paths` lists the new pipeline-table path. The result table path is in the list unchanged |

**Unchanged in the two cases, which is the confirmation asked for:** the result table path
(`golden_rejoin_job/_result/golden_ratio/.../rev1/rdb_v1`) and its `input_data_hashes` entry (the input data
hash covers `age` and `weight` only, as after today's select), the pipeline table's record ids, every row's
record id, source id and data hash, every system-tag column name, and every other node of the job.

Also confirmed on the prototype (`folded_confirm.py`, `folded_confirm.out`): a job with the hand-written
select on the same store makes 0 function calls after the folded job computed (results are shared), with
another pipeline hash and another pipeline table; with no extra column the default call, `strict=True` and
`process` have one content hash and one pipeline hash; the input's columns in another order give the same
hashes.

## 2. The exact list (the developer's variant)

| # | Entry | File | Pinned today | New | Why |
|---|---|---|---|---|---|
| 1 | `pipeline:rejoin_job.node[SelectDataColumns].content_hash` | provisional/pipeline.json | `semantic_v1:96904fb283607c88e95e96842137d3683aea92ddb26978b2cd1c075eb5f8ebf7` | (none) | S: there is no select node |
| 2 | `pipeline:rejoin_job.node[SelectDataColumns].pipeline_hash` | provisional/pipeline.json | `semantic_v1:646655f033644fd96e1c1ea0fd56af969e3afe16c76b1cb176aa343a3d2ebbe5` | (none) | S: there is no select node |
| 3 | `pipeline:rejoin_job.node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}]._tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | provisional/pipeline.json | `row_0` | (none) | S: there is no select node |
| 4 | `pipeline:rejoin_job.node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}]._tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | provisional/pipeline.json | `dict_v1:rejoin_origin` | (none) | S: there is no select node |
| 5 | `pipeline:rejoin_job.node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}].data_hash` | provisional/pipeline.json | `arrow_v1:000002d1c5b49de38ef6efc552c2a47b8ab59dd267f8eb55d91814edee99b1b52acc65` | (none) | S: there is no select node |
| 6 | `pipeline:rejoin_job.node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}]._tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | provisional/pipeline.json | `row_1` | (none) | S: there is no select node |
| 7 | `pipeline:rejoin_job.node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}]._tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | provisional/pipeline.json | `dict_v1:rejoin_origin` | (none) | S: there is no select node |
| 8 | `pipeline:rejoin_job.node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}].data_hash` | provisional/pipeline.json | `arrow_v1:000002338ce8710f5ecb156a72c8446651adb7b0c21635d0aa92d298f09da56faed0f8` | (none) | S: there is no select node |
| 9 | `pipeline:rejoin_job.node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}]._tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | provisional/pipeline.json | `row_2` | (none) | S: there is no select node |
| 10 | `pipeline:rejoin_job.node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}]._tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | provisional/pipeline.json | `dict_v1:rejoin_origin` | (none) | S: there is no select node |
| 11 | `pipeline:rejoin_job.node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}].data_hash` | provisional/pipeline.json | `arrow_v1:0000023401f1ce40c8cfdd6e4f343d7ffdcd51c1f0521f44cf6030e27e1ca0da154616` | (none) | S: there is no select node |
| 12 | `pipeline:rejoin_job.node[SelectDataColumns].system_tag_columns` | provisional/pipeline.json | `_tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4`<br>`_tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | (none) | S: there is no select node |
| 13 | `pipeline:rejoin_job.node[ratio].content_hash` | provisional/pipeline.json | `semantic_v1:84f5f8ce9de0dac0e361d0b0d69f186075974bd359ccbee04ca58f2cc6fefbac` | `semantic_v1:511d3c57512025445c23773ce648ee29b277882027ddf8a1e5080b1e2adc06a3` | H: the node is the pod over the rejoined stream, not over a select of it, and its identity tuple ends with `drops_extra_columns` (it drops `height`, `score`) |
| 14 | `pipeline:rejoin_job.node[ratio].pipeline_hash` | provisional/pipeline.json | `semantic_v1:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba` | `semantic_v1:64b6c04f3dd104e05264b806b1b24c1e20ac153ec8afd6df94d2afec64fe6b87` | H: the node is the pod over the rejoined stream, not over a select of it, and its identity tuple ends with `drops_extra_columns` (it drops `height`, `score`) |
| 15 | `pipeline:rejoin_job.table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1].record_ids` | provisional/pipeline.json | `arrow_v1:000002373cad79440bb534ae0b85745bfbe97278bcf7dac4ad2718df0a13f9b4fc51b9`<br>`arrow_v1:0000025d8f7a2f2a2ec39599d0f52172f223126d006a65d4f6ca87b7fd231bad7ce6e8`<br>`arrow_v1:000002e5737179621e804294b78ea1190ce0c0e915058c536137b50d85c8e447c6d2ab` | (none) | P-old: path no longer produced |
| 16 | `pipeline:rejoin_job.table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:64b6c04f3dd104e05264b806b1b24c1e20ac153ec8afd6df94d2afec64fe6b87/pdb_v1].record_ids` | (not pinned) | (none) | `arrow_v1:000002373cad79440bb534ae0b85745bfbe97278bcf7dac4ad2718df0a13f9b4fc51b9`<br>`arrow_v1:0000025d8f7a2f2a2ec39599d0f52172f223126d006a65d4f6ca87b7fd231bad7ce6e8`<br>`arrow_v1:000002e5737179621e804294b78ea1190ce0c0e915058c536137b50d85c8e447c6d2ab` | P-new: the same rows at the new path |
| 17 | `pipeline:rejoin_job.table_paths` | provisional/pipeline.json | `golden_rejoin_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1`<br>`golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1` | `golden_rejoin_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1`<br>`golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:64b6c04f3dd104e05264b806b1b24c1e20ac153ec8afd6df94d2afec64fe6b87/pdb_v1` | L: the list names the new pipeline-table path |
| 18 | `pipeline:rejoin_job_async.node[SelectDataColumns].content_hash` | provisional/pipeline.json | `semantic_v1:96904fb283607c88e95e96842137d3683aea92ddb26978b2cd1c075eb5f8ebf7` | (none) | S: there is no select node |
| 19 | `pipeline:rejoin_job_async.node[SelectDataColumns].pipeline_hash` | provisional/pipeline.json | `semantic_v1:646655f033644fd96e1c1ea0fd56af969e3afe16c76b1cb176aa343a3d2ebbe5` | (none) | S: there is no select node |
| 20 | `pipeline:rejoin_job_async.node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}]._tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | provisional/pipeline.json | `row_0` | (none) | S: there is no select node |
| 21 | `pipeline:rejoin_job_async.node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}]._tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | provisional/pipeline.json | `dict_v1:rejoin_origin` | (none) | S: there is no select node |
| 22 | `pipeline:rejoin_job_async.node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}].data_hash` | provisional/pipeline.json | `arrow_v1:000002d1c5b49de38ef6efc552c2a47b8ab59dd267f8eb55d91814edee99b1b52acc65` | (none) | S: there is no select node |
| 23 | `pipeline:rejoin_job_async.node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}]._tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | provisional/pipeline.json | `row_1` | (none) | S: there is no select node |
| 24 | `pipeline:rejoin_job_async.node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}]._tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | provisional/pipeline.json | `dict_v1:rejoin_origin` | (none) | S: there is no select node |
| 25 | `pipeline:rejoin_job_async.node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}].data_hash` | provisional/pipeline.json | `arrow_v1:000002338ce8710f5ecb156a72c8446651adb7b0c21635d0aa92d298f09da56faed0f8` | (none) | S: there is no select node |
| 26 | `pipeline:rejoin_job_async.node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}]._tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | provisional/pipeline.json | `row_2` | (none) | S: there is no select node |
| 27 | `pipeline:rejoin_job_async.node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}]._tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | provisional/pipeline.json | `dict_v1:rejoin_origin` | (none) | S: there is no select node |
| 28 | `pipeline:rejoin_job_async.node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}].data_hash` | provisional/pipeline.json | `arrow_v1:0000023401f1ce40c8cfdd6e4f343d7ffdcd51c1f0521f44cf6030e27e1ca0da154616` | (none) | S: there is no select node |
| 29 | `pipeline:rejoin_job_async.node[SelectDataColumns].system_tag_columns` | provisional/pipeline.json | `_tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4`<br>`_tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | (none) | S: there is no select node |
| 30 | `pipeline:rejoin_job_async.node[ratio].content_hash` | provisional/pipeline.json | `semantic_v1:84f5f8ce9de0dac0e361d0b0d69f186075974bd359ccbee04ca58f2cc6fefbac` | `semantic_v1:511d3c57512025445c23773ce648ee29b277882027ddf8a1e5080b1e2adc06a3` | H: the node is the pod over the rejoined stream, not over a select of it, and its identity tuple ends with `drops_extra_columns` (it drops `height`, `score`) |
| 31 | `pipeline:rejoin_job_async.node[ratio].pipeline_hash` | provisional/pipeline.json | `semantic_v1:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba` | `semantic_v1:64b6c04f3dd104e05264b806b1b24c1e20ac153ec8afd6df94d2afec64fe6b87` | H: the node is the pod over the rejoined stream, not over a select of it, and its identity tuple ends with `drops_extra_columns` (it drops `height`, `score`) |
| 32 | `pipeline:rejoin_job_async.table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1].record_ids` | provisional/pipeline.json | `arrow_v1:000002373cad79440bb534ae0b85745bfbe97278bcf7dac4ad2718df0a13f9b4fc51b9`<br>`arrow_v1:0000025d8f7a2f2a2ec39599d0f52172f223126d006a65d4f6ca87b7fd231bad7ce6e8`<br>`arrow_v1:000002e5737179621e804294b78ea1190ce0c0e915058c536137b50d85c8e447c6d2ab` | (none) | P-old: path no longer produced |
| 33 | `pipeline:rejoin_job_async.table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:64b6c04f3dd104e05264b806b1b24c1e20ac153ec8afd6df94d2afec64fe6b87/pdb_v1].record_ids` | (not pinned) | (none) | `arrow_v1:000002373cad79440bb534ae0b85745bfbe97278bcf7dac4ad2718df0a13f9b4fc51b9`<br>`arrow_v1:0000025d8f7a2f2a2ec39599d0f52172f223126d006a65d4f6ca87b7fd231bad7ce6e8`<br>`arrow_v1:000002e5737179621e804294b78ea1190ce0c0e915058c536137b50d85c8e447c6d2ab` | P-new: the same rows at the new path |
| 34 | `pipeline:rejoin_job_async.table_paths` | provisional/pipeline.json | `golden_rejoin_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1`<br>`golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1` | `golden_rejoin_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1`<br>`golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:64b6c04f3dd104e05264b806b1b24c1e20ac153ec8afd6df94d2afec64fe6b87/pdb_v1` | L: the list names the new pipeline-table path |

## 3. The identity structure proposed

- **Today** a function node's identity tuple, the identity of the pod's stream and the key a pipeline gives
  the invocation are one structure: `(pod, inputs)` (`StreamBase.identity_structure`,
  `FunctionPodStream.identity_structure`, `PodInvocation.identity_structure`; the invocation's hash must equal
  the compiled node's).
- **With the marker:** one extra pure-label element appended, **`(pod, inputs, "drops_extra_columns")`**, in the
  content identity and the pipeline identity alike. No key and value, no boolean, no class name. The pod's
  own identity, and so its `uri` and the result table path, are untouched.
- **Present only** when the pod was called in the default mode and its input has a data column that is neither
  a parameter nor a fall-through data-tag. **Absent otherwise:** the tuple is exactly today's two elements, so
  the default call with no extra column, `strict=True` and `process` have one identity and one pipeline table.
- **Defined once:** a constant beside the node's identity code (proposed name `DROPS_EXTRA_COLUMNS`, value
  `"drops_extra_columns"`), with one function that returns the tuple's tail (`()` or the one label) and is
  the only caller of the constant; the three structures above call that function. The label is never written
  as a literal elsewhere; a test greps for it, as the hash-label rule's test does.
- **Encoding and label:** a plain string in a structure hashed by the semantic hasher (`semantic_v1`). It is
  not a hash label (it names no algorithm), and there is no new hash site.
- **The names are not in the identity.** They are computed where they are shown, from the input's data
  columns and the function's parameters. One thing to know: the parameter names are in no part of the `uri`,
  so two revisions of a function with one `uri` that drop different columns of one input are one identity,
  as they are one identity today when their parameters differ.
- **How the node knows:** the pod's call records that it drops columns (the prototype keeps a flag on the pod
  object recorded, which enters no pod identity); a function node with the label reads its function's columns
  and ignores every other data column; one without it refuses an extra column, as today.

## 4. The modes

| Call | Extra data columns in the input | Identity |
|---|---|---|
| `pod(a)` | none | today's |
| `pod(a)` | some | today's tuple with `drops_extra_columns` appended; one info log names the dropped columns |
| `pod(a, strict=True)`, `pod.process(a)` | none | today's, equal to `pod(a)` |
| `pod(a, strict=True)`, `pod.process(a)` | some | refused, as today |
| `pod(a.select_data_columns(kept))` | - | a select node and today's pod over it: another pipeline hash and pipeline table than `pod(a)`, the same result table and entries |

`strict` itself is in no identity. A missing required column is an error in every form; tags are never
dropped; data-tags fall through as today; a pod whose function accepts arbitrary keywords ignores nothing.

**Shown:** `explain()` says on the pod's line `reads age, weight of <input>; ignores height, score`;
`diagram()` lists the ignored columns in the pod's detail and its text listing (the picture has no extra
node or line); both compute the names from the input's columns and the function's parameters. A saved job
writes `"drops_extra_columns": true` in the function node's descriptor only when the node has the label,
and the loader rebuilds the node with it; the names are not saved.

## 5. Tests that change

One pytest process on the prototype over the areas a pod call reaches (the golden suite, `tests/test_pipeline`,
`tests/test_core/function_pod`, `data_tags`, `streams`, `nodes`, `tests/test_docs`, the rejoin, filter-pipeline,
order-invariance and `where` tests): **14036 passed, 74 failed** (`folded_test_run.out`). Not the full suite.

How far this count can be trusted: the run was started with an earlier encoding of the marker (the ignored
names on the pod, `O34_MARK=ignored`), and the library files were edited for the later encodings while it
ran. The encodings differ only in the value hashed, so the same tests are expected to change with the final
one, but that was not rerun. Reasons are read from each failure's error line, not from each test's body.

| Reason | Tests | Where |
|---|---|---|
| The golden entries of section 2, and the golden suite's own checks that follow them (unpinned entries, hash-seed independence, the store's file layout) | 37 | `test_golden_hashes.py` 35, `test_golden_store.py` 2 |
| ITL-807's equivalence: `pod(a)` equal to the hand-written select in graph, hashes and stored tables | 10 | `test_function_pod_auto_select.py` 9, `test_fall_through_pod_wiring.py` 1 |
| A node list, diagram or printed output that names `SelectDataColumns` | 11 | `test_node_staleness_after_run.py` 6, `test_source_display_names.py` 3, `test_diagram.py` 1, `test_doc_samples.py` 1 (`docs/concepts/function-pods.md`) |
| Provenance tracing through a pod that drops columns: it compares the stored input data hash with the whole input row's (8), or highlights the select node (2). **Implementation work, not only a test change:** the trace must narrow the row to the function's columns as the node does | 10 | `test_provenance_review.py` |
| Paths the prototype does not narrow, to be examined in the implementation (a separate result database, a saved job with data-tag operator nodes, commit dependencies) | 6 | `test_result_database.py` 2, `test_saved_job_with_data_tag_operator_nodes.py` 2, `test_function_commit_dependencies.py` 2 |

For comparison, the same run on the selector-node prototype: 119 failed.

**Text that changes:** `CLAUDE.md` and `.zed/rules`, "Calling a function pod" (the automatic `SelectDataColumns`
step and its equality with the hand-written select are withdrawn; the default call ignores the columns and
the node's identity gets the label; `strict` and `process` unchanged); `docs/concepts/function-pods.md`
(line 142's sample prints the select node) and the other pages that show `SelectDataColumns` in a node list
or a diagram of a pod call (`docs/tutorial/03-operators.md`, `05-pipeline-job.md`,
`08-inspecting-and-tracing.md`, `docs/concepts/operators.md`, `sources.md`, `provenance.md`, `notebooks.md`,
`data-tags.md`, `docs/troubleshooting.md`: each to be checked, they name the operator and not all of them the
automatic step); a changelog fragment under "Breaking changes".

## 6. For comparison: the variants not chosen

All measured the same way. The entries that leave (S) are the same 24 in every folded variant.

| Variant | Entries that differ | Files | Notes |
|---|---|---|---|
| **One label appended to the node's identity tuple where columns are dropped (the developer's; section 2)** | 34 | provisional pipeline file only | 2 cases |
| Folded, a key and value on the pod, `("drop_extra", True)` | 34 | provisional pipeline file only | the same 34 entries with other new values |
| Folded, the ignored names in the identity | 34 | provisional pipeline file only | the same 34 entries with other new values |
| Folded, nothing recorded (strictness out of the identity) | 34 | provisional pipeline file only | the same 34 entries with other new values; a pod over `A` is one identity whatever it ignores |
| Folded, the strict mode marked | 34 | provisional pipeline file only | equal, value for value, to "nothing recorded": no golden case calls `process` or `strict=True` in a pipeline; the default call with no extra column keeps today's values; `pod(A)` and `pod(A, strict=True)` split their pipeline table |
| Folded, the default mode marked | 61 | provisional pipeline file 43, **frozen `pipeline-02.json` 12**, 6 new | every pod called in the default mode moves; `process` and `strict=True` keep today's values; `pod(A)` and `pod(A, strict=True)` split their pipeline table |
| A selector node of the pod's own (the earlier revision) | 58 | provisional pipeline file only | the select node stays under another class name: 24 entries renamed with equal values or new hashes, 10 moved |

`node[ratio]` of `rejoin_job`, in each:

| Variant | `content_hash` | `pipeline_hash` |
|---|---|---|
| Pinned today | `semantic_v1:84f5f8ce9de0dac0e361d0b0d69f186075974bd359ccbee04ca58f2cc6fefbac` | `semantic_v1:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba` |
| The label appended (the developer's) | `semantic_v1:511d3c57512025445c23773ce648ee29b277882027ddf8a1e5080b1e2adc06a3` | `semantic_v1:64b6c04f3dd104e05264b806b1b24c1e20ac153ec8afd6df94d2afec64fe6b87` |
| `("drop_extra", True)` on the pod | `semantic_v1:d80ee52efbc4ce5c0d45e6f591caf2f7405af874c0b96e2f1d0866f1774dc563` | `semantic_v1:fc7b10c63c32b6154da3de4b99a5053c84b7001ec8660749aaf0650973cf0816` |
| Ignored names | `semantic_v1:93e8a930c3f85442faf41f7fe582894a82f81459fd2931cebc3367565b7db417` | `semantic_v1:ddf2a3242abc6f757b2841b7392cc48c5d729c111905b4dcfe93aa76033e5404` |
| Nothing recorded; strict marked | `semantic_v1:b0dfaeba0c527d35479900042ea8ebe39253b972a2730c781156adf1250068be` | `semantic_v1:1417650fd91de65932bc092a00a438d3465bf8a796bda7e43af06c9dfa320b3f` |
| Default mode marked | `semantic_v1:e9a83017ef380bdfe33e50482542bc96858037a5c6a10a04478cf01faa3cdba1` | `semantic_v1:64761f9da7fc0e9205aada8711a06c4dabbc0ffc382c9a659d92634f73f54309` |
| Selector node | `semantic_v1:7c3fdb9d460d6b148dc2f56a5e160b95144142dc8774b397d68bca2062abe128` | `semantic_v1:8964c1658544ac8df6b79b3b599a06fd38730d2d06a4755e8e1992d7f4f2ec18` |

### 6a. Default mode marked: the 27 entries beyond the 34

| # | Entry | File | Pinned today | New | Why |
|---|---|---|---|---|---|
| 1 | `pipeline:default_source_ids.as_source[function_node].source_id` | provisional/pipeline.json | `derived_v1:semantic_v1:2b0bbea125de8660f08433ab9b8083ce706f92de396aee9044243bfa24fd820c` | `derived_v1:semantic_v1:8c8873737f77d3af7c5d7a1661b39a564d9bba3e66bf396870189002b1d12c43` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 2 | `pipeline:drop_nulls_job.node[ratio].content_hash` | pipeline-02.json | `semantic_v1:ee65e90278766e2425be4869d1b08df3789c1c161146b8e75faf78e9d0f36645` | `semantic_v1:491e3e16027ac6fdc4a50195739f15c25938c9463ca4c72abaf90164eb917748` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 3 | `pipeline:drop_nulls_job.node[ratio].pipeline_hash` | pipeline-02.json | `semantic_v1:fbe5ea222e164ffeeb59b89b9c4836a3f1b7e5f580dce814091fe75bb60bff20` | `semantic_v1:03a14b94ffee051887f6d373c6a7b549cacf3e905c5be6a0e4a65b0846f80ca3` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 4 | `pipeline:drop_nulls_job.table[golden_drop_null_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:03a14b94ffee051887f6d373c6a7b549cacf3e905c5be6a0e4a65b0846f80ca3/pdb_v1].record_ids` | (not pinned) | (none) | `arrow_v1:00000208e68d47a17985bf24008c03ed0b445b040297a6895090867117a175faa6a9cc`<br>`arrow_v1:0000025960b646e835f382c9fb9b3bcb207eee5f3ca389d86fc20f6486e53e6dfad1f7` | P-new (same rows) |
| 5 | `pipeline:drop_nulls_job.table[golden_drop_null_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:fbe5ea222e164ffeeb59b89b9c4836a3f1b7e5f580dce814091fe75bb60bff20/pdb_v1].record_ids` | pipeline-02.json | `arrow_v1:00000208e68d47a17985bf24008c03ed0b445b040297a6895090867117a175faa6a9cc`<br>`arrow_v1:0000025960b646e835f382c9fb9b3bcb207eee5f3ca389d86fc20f6486e53e6dfad1f7` | (none) | P-old |
| 6 | `pipeline:drop_nulls_job.table_paths` | pipeline-02.json | `golden_drop_null_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1`<br>`golden_drop_null_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:fbe5ea222e164ffeeb59b89b9c4836a3f1b7e5f580dce814091fe75bb60bff20/pdb_v1` | `golden_drop_null_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1`<br>`golden_drop_null_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:03a14b94ffee051887f6d373c6a7b549cacf3e905c5be6a0e4a65b0846f80ca3/pdb_v1` | L: a list that names a moved value |
| 7 | `pipeline:filter_job.node[adjust].content_hash` | provisional/pipeline.json | `semantic_v1:d9a2cf1f4e539f8472c81f242133769e24fc12f98b1d5cd2fed365400a1f874c` | `semantic_v1:e07d28fa506a9fba052e27dc5df95f98a99f650d1f4f051c449fb3955e684c77` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 8 | `pipeline:filter_job.node[adjust].pipeline_hash` | provisional/pipeline.json | `semantic_v1:1b55d0d2cce6651fc52aed3a13227619ae3ba68ecd21f99ebfde5d68e56f4fa2` | `semantic_v1:8493465da39ec44d8f5930e4ab07ae5f33488a77261c4952c9e1caf5b34fd7c3` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 9 | `pipeline:filter_job.table[golden_filter_step_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:1b55d0d2cce6651fc52aed3a13227619ae3ba68ecd21f99ebfde5d68e56f4fa2/pdb_v1].record_ids` | provisional/pipeline.json | `arrow_v1:000002fd7d9af35a038e4d9201d0f1c0dc1a8ed63f800e52eb937da2a4993ea1b51971` | (none) | P-old |
| 10 | `pipeline:filter_job.table[golden_filter_step_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:8493465da39ec44d8f5930e4ab07ae5f33488a77261c4952c9e1caf5b34fd7c3/pdb_v1].record_ids` | (not pinned) | (none) | `arrow_v1:000002fd7d9af35a038e4d9201d0f1c0dc1a8ed63f800e52eb937da2a4993ea1b51971` | P-new (same rows) |
| 11 | `pipeline:filter_job.table_paths` | provisional/pipeline.json | `golden_filter_step_job/_result/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/rev1/rdb_v1`<br>`golden_filter_step_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:1b55d0d2cce6651fc52aed3a13227619ae3ba68ecd21f99ebfde5d68e56f4fa2/pdb_v1` | `golden_filter_step_job/_result/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/rev1/rdb_v1`<br>`golden_filter_step_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:8493465da39ec44d8f5930e4ab07ae5f33488a77261c4952c9e1caf5b34fd7c3/pdb_v1` | L: a list that names a moved value |
| 12 | `pipeline:job.node[adjust].content_hash` | pipeline-02.json | `semantic_v1:2b0bbea125de8660f08433ab9b8083ce706f92de396aee9044243bfa24fd820c` | `semantic_v1:8c8873737f77d3af7c5d7a1661b39a564d9bba3e66bf396870189002b1d12c43` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 13 | `pipeline:job.node[adjust].pipeline_hash` | pipeline-02.json | `semantic_v1:b1212cbf9e4025086e622df7d8e9931ddde18946f29dda37568a13f36dfb39ef` | `semantic_v1:a5fd3f4641fba0e0f10875fedf394d48e43a80101bed10aa25604d9fe18ce583` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 14 | `pipeline:job.table[golden_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:a5fd3f4641fba0e0f10875fedf394d48e43a80101bed10aa25604d9fe18ce583/pdb_v1].record_ids` | (not pinned) | (none) | `arrow_v1:000002439c8a6f4f392cd14e4e407536566a3e2036d1f5839bd77ad1d99ce86637af05`<br>`arrow_v1:0000024e73709dcc24e69e8fed821be20e818f559e8cd26e4532e7d00ad0e2f83404f3` | P-new (same rows) |
| 15 | `pipeline:job.table[golden_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:b1212cbf9e4025086e622df7d8e9931ddde18946f29dda37568a13f36dfb39ef/pdb_v1].record_ids` | pipeline-02.json | `arrow_v1:000002439c8a6f4f392cd14e4e407536566a3e2036d1f5839bd77ad1d99ce86637af05`<br>`arrow_v1:0000024e73709dcc24e69e8fed821be20e818f559e8cd26e4532e7d00ad0e2f83404f3` | (none) | P-old |
| 16 | `pipeline:job.table_paths` | pipeline-02.json | `golden_job/_result/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/rev1/rdb_v1`<br>`golden_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:b1212cbf9e4025086e622df7d8e9931ddde18946f29dda37568a13f36dfb39ef/pdb_v1` | `golden_job/_result/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/rev1/rdb_v1`<br>`golden_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:a5fd3f4641fba0e0f10875fedf394d48e43a80101bed10aa25604d9fe18ce583/pdb_v1` | L: a list that names a moved value |
| 17 | `pipeline:join_pod.pod.content_hash` | pipeline-02.json | `semantic_v1:2b0bbea125de8660f08433ab9b8083ce706f92de396aee9044243bfa24fd820c` | `semantic_v1:8c8873737f77d3af7c5d7a1661b39a564d9bba3e66bf396870189002b1d12c43` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 18 | `pipeline:join_pod.pod.pipeline_hash` | pipeline-02.json | `semantic_v1:b1212cbf9e4025086e622df7d8e9931ddde18946f29dda37568a13f36dfb39ef` | `semantic_v1:a5fd3f4641fba0e0f10875fedf394d48e43a80101bed10aa25604d9fe18ce583` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 19 | `pipeline:pod.content_hash` | pipeline-02.json | `semantic_v1:0481bc80663cf0d15cdf8e5f25926d246d761f2b64f372ef0528ae31c3598afb` | `semantic_v1:4b14810092525d0cd432f373da55eaaf69047848ca5981a722d2d6db5d41d582` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 20 | `pipeline:pod.pipeline_hash` | pipeline-02.json | `semantic_v1:a2edfb298d2330576b8c7b1f05685154af463b5bd2259c92f3ec12341f83906e` | `semantic_v1:65faaba483ee74c0490b5fb0e47c147c27d75d9548dbee40bd7c14a9f395c58d` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 21 | `pipeline:polars_filter_expr_job.node[adjust].content_hash` | provisional/pipeline.json | `semantic_v1:2286a79cf7ea84441950371adfc6f033903c9e0e49a851ee61c90ccb81bcdf26` | `semantic_v1:2201588a8f66faad99cfab11cc0ee1121f0896f244eaa6268e7200593162d8ea` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 22 | `pipeline:polars_filter_expr_job.node[adjust].pipeline_hash` | provisional/pipeline.json | `semantic_v1:d41530868bebc0f11662b61d07ec45e0ae6bcee9b9602116392a103444a02cb7` | `semantic_v1:680f644f57535de5208e389eae42f33ec85ed4939a4fffc38bd6a6e544ee2d16` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 23 | `pipeline:polars_filter_expr_job.table[golden_filter_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:680f644f57535de5208e389eae42f33ec85ed4939a4fffc38bd6a6e544ee2d16/pdb_v1].record_ids` | (not pinned) | (none) | `arrow_v1:0000023efdc614d3a51bc82f2efc0a7355bf905da5dea9b679e66b9873e367a6593449` | P-new (same rows) |
| 24 | `pipeline:polars_filter_expr_job.table[golden_filter_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:d41530868bebc0f11662b61d07ec45e0ae6bcee9b9602116392a103444a02cb7/pdb_v1].record_ids` | provisional/pipeline.json | `arrow_v1:0000023efdc614d3a51bc82f2efc0a7355bf905da5dea9b679e66b9873e367a6593449` | (none) | P-old |
| 25 | `pipeline:polars_filter_expr_job.table_paths` | provisional/pipeline.json | `golden_filter_job/_result/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/rev1/rdb_v1`<br>`golden_filter_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:d41530868bebc0f11662b61d07ec45e0ae6bcee9b9602116392a103444a02cb7/pdb_v1` | `golden_filter_job/_result/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/rev1/rdb_v1`<br>`golden_filter_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:680f644f57535de5208e389eae42f33ec85ed4939a4fffc38bd6a6e544ee2d16/pdb_v1` | L: a list that names a moved value |
| 26 | `pipeline:rejoin.pod_branch.content_hash` | provisional/pipeline.json | `semantic_v1:bf4883d6659e89cb81d678d058c66ea0fdd6a1c2f50fd153e4c60a49a03197f7` | `semantic_v1:7844732f856b77b3fc4685ff192690883165f54b6be9e7311b47c899a129ca8b` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
| 27 | `pipeline:rejoin.pod_branch.pipeline_hash` | provisional/pipeline.json | `semantic_v1:41a6a64d73167b135e38865b98ba5cdc0c9aca55dd72e904bbbdd3237d549d6f` | `semantic_v1:2b54fac8a0bbda0c7555076570947d421ad7691c68c63d0e12d0cdf9964291e2` | H: a pod called in the default mode has the mode in its identity; its hash and every hash downstream move |
### 6b. The selector node (the earlier revision): its list

Measured with `selector-prototype.patch`. `R`: key renamed, value equal. `V`: value moved.

### `rejoin_job`: 29 entries differ, 59 unchanged

| # | Entry | Pinned today | With the selector kind | Kind |
|---|---|---|---|---|
| 1 | `node[SelectDataColumns].content_hash` -> `node[FunctionInputSelect]...` | `semantic_v1:96904fb283607c88e95e96842137d3683aea92ddb26978b2cd1c075eb5f8ebf7` | `semantic_v1:fdd464c5a494c287e738739273da68c128c67f2ccdffe1ea7424ca5abe8e3337` | V (key renamed, value moved) |
| 2 | `node[SelectDataColumns].pipeline_hash` -> `node[FunctionInputSelect]...` | `semantic_v1:646655f033644fd96e1c1ea0fd56af969e3afe16c76b1cb176aa343a3d2ebbe5` | `semantic_v1:2decdf7fe7fda4cc6ffcf3ff77f316ff9e5d02270fdc70b825c6b85451f22c65` | V (key renamed, value moved) |
| 3 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}]._tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` -> `node[FunctionInputSelect]...` | `row_0` | `row_0` | R (key renamed, value equal) |
| 4 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}]._tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` -> `node[FunctionInputSelect]...` | `dict_v1:rejoin_origin` | `dict_v1:rejoin_origin` | R (key renamed, value equal) |
| 5 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}].data_hash` -> `node[FunctionInputSelect]...` | `arrow_v1:000002d1c5b49de38ef6efc552c2a47b8ab59dd267f8eb55d91814edee99b1b52acc65` | `arrow_v1:000002d1c5b49de38ef6efc552c2a47b8ab59dd267f8eb55d91814edee99b1b52acc65` | R (key renamed, value equal) |
| 6 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}]._tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` -> `node[FunctionInputSelect]...` | `row_1` | `row_1` | R (key renamed, value equal) |
| 7 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}]._tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` -> `node[FunctionInputSelect]...` | `dict_v1:rejoin_origin` | `dict_v1:rejoin_origin` | R (key renamed, value equal) |
| 8 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}].data_hash` -> `node[FunctionInputSelect]...` | `arrow_v1:000002338ce8710f5ecb156a72c8446651adb7b0c21635d0aa92d298f09da56faed0f8` | `arrow_v1:000002338ce8710f5ecb156a72c8446651adb7b0c21635d0aa92d298f09da56faed0f8` | R (key renamed, value equal) |
| 9 | `node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}]._tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` -> `node[FunctionInputSelect]...` | `row_2` | `row_2` | R (key renamed, value equal) |
| 10 | `node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}]._tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` -> `node[FunctionInputSelect]...` | `dict_v1:rejoin_origin` | `dict_v1:rejoin_origin` | R (key renamed, value equal) |
| 11 | `node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}].data_hash` -> `node[FunctionInputSelect]...` | `arrow_v1:0000023401f1ce40c8cfdd6e4f343d7ffdcd51c1f0521f44cf6030e27e1ca0da154616` | `arrow_v1:0000023401f1ce40c8cfdd6e4f343d7ffdcd51c1f0521f44cf6030e27e1ca0da154616` | R (key renamed, value equal) |
| 12 | `node[SelectDataColumns].system_tag_columns` -> `node[FunctionInputSelect]...` | `_tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4`<br>`_tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | `_tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4`<br>`_tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | R (key renamed, value equal) |
| 13 | `node[ratio].content_hash` | `semantic_v1:84f5f8ce9de0dac0e361d0b0d69f186075974bd359ccbee04ca58f2cc6fefbac` | `semantic_v1:7c3fdb9d460d6b148dc2f56a5e160b95144142dc8774b397d68bca2062abe128` | V (value moved) |
| 14 | `node[ratio].pipeline_hash` | `semantic_v1:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba` | `semantic_v1:8964c1658544ac8df6b79b3b599a06fd38730d2d06a4755e8e1992d7f4f2ec18` | V (value moved) |
| 15 | `table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1].record_ids` | `arrow_v1:000002373cad79440bb534ae0b85745bfbe97278bcf7dac4ad2718df0a13f9b4fc51b9`<br>`arrow_v1:0000025d8f7a2f2a2ec39599d0f52172f223126d006a65d4f6ca87b7fd231bad7ce6e8`<br>`arrow_v1:000002e5737179621e804294b78ea1190ce0c0e915058c536137b50d85c8e447c6d2ab` | - | P-old (path no longer produced) |
| 16 | `table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:8964c1658544ac8df6b79b3b599a06fd38730d2d06a4755e8e1992d7f4f2ec18/pdb_v1].record_ids` | - | `arrow_v1:000002373cad79440bb534ae0b85745bfbe97278bcf7dac4ad2718df0a13f9b4fc51b9`<br>`arrow_v1:0000025d8f7a2f2a2ec39599d0f52172f223126d006a65d4f6ca87b7fd231bad7ce6e8`<br>`arrow_v1:000002e5737179621e804294b78ea1190ce0c0e915058c536137b50d85c8e447c6d2ab` | P-new (same rows at the new path) |
| 17 | `table_paths` | `golden_rejoin_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1`<br>`golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1` | `golden_rejoin_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1`<br>`golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:8964c1658544ac8df6b79b3b599a06fd38730d2d06a4755e8e1992d7f4f2ec18/pdb_v1` | V (value moved) |

### `rejoin_job_async`: 29 entries differ, 59 unchanged

| # | Entry | Pinned today | With the selector kind | Kind |
|---|---|---|---|---|
| 1 | `node[SelectDataColumns].content_hash` -> `node[FunctionInputSelect]...` | `semantic_v1:96904fb283607c88e95e96842137d3683aea92ddb26978b2cd1c075eb5f8ebf7` | `semantic_v1:fdd464c5a494c287e738739273da68c128c67f2ccdffe1ea7424ca5abe8e3337` | V (key renamed, value moved) |
| 2 | `node[SelectDataColumns].pipeline_hash` -> `node[FunctionInputSelect]...` | `semantic_v1:646655f033644fd96e1c1ea0fd56af969e3afe16c76b1cb176aa343a3d2ebbe5` | `semantic_v1:2decdf7fe7fda4cc6ffcf3ff77f316ff9e5d02270fdc70b825c6b85451f22c65` | V (key renamed, value moved) |
| 3 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}]._tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` -> `node[FunctionInputSelect]...` | `row_0` | `row_0` | R (key renamed, value equal) |
| 4 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}]._tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` -> `node[FunctionInputSelect]...` | `dict_v1:rejoin_origin` | `dict_v1:rejoin_origin` | R (key renamed, value equal) |
| 5 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s1"}].data_hash` -> `node[FunctionInputSelect]...` | `arrow_v1:000002d1c5b49de38ef6efc552c2a47b8ab59dd267f8eb55d91814edee99b1b52acc65` | `arrow_v1:000002d1c5b49de38ef6efc552c2a47b8ab59dd267f8eb55d91814edee99b1b52acc65` | R (key renamed, value equal) |
| 6 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}]._tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` -> `node[FunctionInputSelect]...` | `row_1` | `row_1` | R (key renamed, value equal) |
| 7 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}]._tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` -> `node[FunctionInputSelect]...` | `dict_v1:rejoin_origin` | `dict_v1:rejoin_origin` | R (key renamed, value equal) |
| 8 | `node[SelectDataColumns].row[{"cohort":"a","subject":"s2"}].data_hash` -> `node[FunctionInputSelect]...` | `arrow_v1:000002338ce8710f5ecb156a72c8446651adb7b0c21635d0aa92d298f09da56faed0f8` | `arrow_v1:000002338ce8710f5ecb156a72c8446651adb7b0c21635d0aa92d298f09da56faed0f8` | R (key renamed, value equal) |
| 9 | `node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}]._tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` -> `node[FunctionInputSelect]...` | `row_2` | `row_2` | R (key renamed, value equal) |
| 10 | `node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}]._tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` -> `node[FunctionInputSelect]...` | `dict_v1:rejoin_origin` | `dict_v1:rejoin_origin` | R (key renamed, value equal) |
| 11 | `node[SelectDataColumns].row[{"cohort":"b","subject":"s3"}].data_hash` -> `node[FunctionInputSelect]...` | `arrow_v1:0000023401f1ce40c8cfdd6e4f343d7ffdcd51c1f0521f44cf6030e27e1ca0da154616` | `arrow_v1:0000023401f1ce40c8cfdd6e4f343d7ffdcd51c1f0521f44cf6030e27e1ca0da154616` | R (key renamed, value equal) |
| 12 | `node[SelectDataColumns].system_tag_columns` -> `node[FunctionInputSelect]...` | `_tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4`<br>`_tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | `_tag_source_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4`<br>`_tag_record_id::b5bbcf9ae6104dc1b569e22587fbde690abf127944838a4e1a1c96bf347db0e4` | R (key renamed, value equal) |
| 13 | `node[ratio].content_hash` | `semantic_v1:84f5f8ce9de0dac0e361d0b0d69f186075974bd359ccbee04ca58f2cc6fefbac` | `semantic_v1:7c3fdb9d460d6b148dc2f56a5e160b95144142dc8774b397d68bca2062abe128` | V (value moved) |
| 14 | `node[ratio].pipeline_hash` | `semantic_v1:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba` | `semantic_v1:8964c1658544ac8df6b79b3b599a06fd38730d2d06a4755e8e1992d7f4f2ec18` | V (value moved) |
| 15 | `table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1].record_ids` | `arrow_v1:000002373cad79440bb534ae0b85745bfbe97278bcf7dac4ad2718df0a13f9b4fc51b9`<br>`arrow_v1:0000025d8f7a2f2a2ec39599d0f52172f223126d006a65d4f6ca87b7fd231bad7ce6e8`<br>`arrow_v1:000002e5737179621e804294b78ea1190ce0c0e915058c536137b50d85c8e447c6d2ab` | - | P-old (path no longer produced) |
| 16 | `table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:8964c1658544ac8df6b79b3b599a06fd38730d2d06a4755e8e1992d7f4f2ec18/pdb_v1].record_ids` | - | `arrow_v1:000002373cad79440bb534ae0b85745bfbe97278bcf7dac4ad2718df0a13f9b4fc51b9`<br>`arrow_v1:0000025d8f7a2f2a2ec39599d0f52172f223126d006a65d4f6ca87b7fd231bad7ce6e8`<br>`arrow_v1:000002e5737179621e804294b78ea1190ce0c0e915058c536137b50d85c8e447c6d2ab` | P-new (same rows at the new path) |
| 17 | `table_paths` | `golden_rejoin_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1`<br>`golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1` | `golden_rejoin_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1`<br>`golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:8964c1658544ac8df6b79b3b599a06fd38730d2d06a4755e8e1992d7f4f2ec18/pdb_v1` | V (value moved) |
