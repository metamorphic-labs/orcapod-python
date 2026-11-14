# ITL-1094: a loaded job restores its side-effect nodes, read-only

Branch `eywalker/itl-1094-a-loaded-job-has-no-side-effect-nodes`, from `origin/main` `a9b50350`.
Status: PROPOSAL sent to the coordinator; waiting for the reply before any code.

## Reproduction (main a9b50350, public interface)

Job `s -> delivered (SideEffectPod) -> down (function pod) -> tail (SideEffectPod)`, run on a
Delta store, saved, loaded.

- The saved file records each side-effect node like every other node, under the key of its
  blueprint content hash, with exactly these fields: `node_type: "side_effect"`, `label`,
  `content_hash` (blueprint hash), `pipeline_hash` (equal to the job node's), `output_schema`
  (`tag`, `data`, `nullable`), `node_uri` (`["side_effects", <pod name>]`), `data_context_key`,
  `config` (the Orcapod config, `null` when default). The edges `s -> delivered` and
  `delivered -> down` are saved. `run.node_job_content_hashes` and `run.expected_rows` hold
  no entry for a side-effect node. The pod config (`on_error`, `track_completion`,
  `drop_on_failure`, `max_concurrency`) is not saved anywhere.
- `Pipeline.load` has no `side_effect` branch: the node is skipped. `loaded.nodes` is
  `['s', 'down']`; `loaded.nodes['delivered']` and `['tail']` are `KeyError`.
- `down` after the load: its `upstreams` is `(None,)`, and the job DAG has neither edge
  `s -> delivered` nor `delivered -> down`. Its content hash and pipeline hash EQUAL the saved
  ones (both come from the file: the run block's live hash and the descriptor), so it reads
  the same table: 3 rows. No silent identity change.
- Re-saving the loaded job writes a file WITHOUT the side-effect nodes but WITH their edges
  (3 dangling edges); loading that file gives `['s', 'down']` again.
- On the #380 branch (head 0b4c2316), `loaded.bind(sources=...)` then
  `loaded.run(load_only=True)` raises `ValueError: FunctionNode expects exactly 1 upstream,
  got 0` at `down`: the dropped node breaks a load-only run of anything below it.
- v0.2 (format 0.1.0) saved side-effect nodes with the same fields minus
  `output_schema.nullable`, with `semantic_v0.1` hashes; v0.2's load dropped them too. No test
  fixture holds a v0.2 job file with a side-effect node.

## Proposal

A read-only side-effect node, built from its descriptor, as the function and operator stubs are.

1. `SideEffectNode.from_descriptor(descriptor, input_stream)` (blueprint, used by
   `Pipeline.load`) and `SideEffectJobNode.from_descriptor(descriptor, input_stream)` (used by
   `PipelineJob.load`): no pod (`_pod = None`); label, content hash, pipeline hash, output
   schema (with the saved nullability, or unrecorded), node uri, data context and Orcapod
   config from the descriptor; upstream = the restored upstream node, so the DAG edges
   `s -> delivered -> down` are restored and `down.upstreams == (delivered,)`.
2. Identity: `pipeline_hash()` = the saved one (equal to the job node's; it names the
   invocation log table `side_effects/<pod>/schema:<hex>/tdb_v2`). `content_hash()` = the
   descriptor's `content_hash` (the blueprint hash the file records). The live job node's
   content hash (data-inclusive) is not in the file. Nothing stored is keyed by a side-effect
   node's content hash (log path and record ids use the pipeline hash and the row), so no
   table depends on it. Option B (not recommended for the tag): record it in
   `run.node_job_content_hashes` at save (an existing label -> hash map; no new key; older
   loaders ignore extra entries; older files fall back to the descriptor hash). It changes
   what the file records, so it is the coordinator's call.
3. Never delivers: no pod function. `execute`, `async_execute` and `node.run()` raise a
   `RuntimeError` saying the node was loaded and has no side-effect function. Reads follow
   `SideEffectJobNode` (#377): no held rows, so zero rows; `head()` not computed.
   `loaded.run()` raises as today (before any node runs).
4. Pod config and completion log: not restored; the file does not hold the pod config, and
   restoring it would be a saved-format change (not proposed). The database is attached as for
   any `SideEffectJobNode` (path known from node uri + pipeline hash), but the node never
   reads or writes the log. No public reader is added.
5. Re-save of a loaded job writes the node's descriptor back unchanged (and the edges become
   consistent again).
6. No new field in the saved file; no hash, golden value or stored table changes.

## After #380 (expected to merge first; then merge main here)

- #380's orchestrators pass a side-effect node's rows on in a load-only run without calling it
  (sync: `buffers[node] = upstream_buf`; async: `_pass_through`), so the restored node needs
  no code change: `loaded.run(load_only=True)` passes the rows through `delivered` to `down`,
  delivers nothing, writes nothing. On #380 head 0b4c2316 the side-effect node itself is NOT
  filled ("node reads empty", its D4); the brief's "holds them as loaded only" is not what
  #380 does yet.
- At the merge: docs section "Saving and loading a job" conflicts (both rewrite it); tests
  gain a load-only case (bind source, `loaded.run(load_only=True)`, `down` loads 3 rows,
  zero deliveries, side-effect node empty).

## Tasks (after approval)

1. Failing tests, public interface: `tests/test_pipeline/loaded_job/test_loaded_side_effect_nodes.py`
   - side-effect node in the middle and at the end: `loaded.nodes` lists them with label,
     content hash (= saved descriptor), pipeline hash (= saved job node), upstream.
   - `down`'s content/pipeline hash equal the saved job node's, `down.upstreams` is the
     restored node.
   - no read (`iter_data`, `as_table`, `head`, dataframes, `flow`) or run (`loaded.run()`,
     `node.run()`) delivers: count deliveries; reads give zero rows; runs raise.
   - re-save and reload keeps them; `Pipeline.load` lists them.
   - a 0.1.0-shaped file (no nullable) restores them.
2. Fix: `from_descriptor` on both classes; `Pipeline.load` and `PipelineJob.load` branches.
3. Docs: `docs/concepts/function-pods.md` "Saving and loading a job"; changelog fragment
   `changelog.d/ITL-1094.md` (Fixed); `design_issues/ITL-1094-<id>.md`.
4. Targeted tests (loaded_job, side_effect_pod, serialization, golden), ruff, basedpyright.
