# ITL-1111: defining stacked split-and-join stages is exponential

Branch `eywalker/itl-1111-defining-a-pipeline-with-stacked-split-and`, from `origin/main` `a9b50350`.
Cost fix only: nothing hashed, stored or golden changes.

## Cause (measured, cProfile + per-node call counts)

Scratch tools (not committed): `walks.py` in the session scratchpad wraps `output_schema`, `keys`,
`content_hash`, `pipeline_hash`, identity structures, `argument_symmetry`, `validate_inputs`,
`is_stale`, `last_modified`, `_update_cache_status`, `_predict_system_tag_schema` on every orcapod
class and counts calls per object.

- No derived stream keeps its output schema. `DynamicPodStream.output_schema()` / `keys()`
  (`core/operators/static_output_pod.py`), `FunctionPodStream.output_schema()`
  (`core/function_pod.py`), `FunctionNode.output_schema()` (`core/nodes/function_node.py`) and
  `OperatorNode.output_schema()` (`core/nodes/operator_node.py`) recompute from their pod over
  their upstreams on every call.
- `Join.output_schema(a, b)` asks each input several times: `keys()` in
  `_require_tag_data_roles_agree`, `output_schema()` in `_joined_user_schemas`, again
  `output_schema(system_tags)` in `_predict_system_tag_schema`. A split-and-join stage therefore
  calls its upstream about 4 times per call of its own: 4x per stage.
- 8-stage operator build (41 nodes): 101,930 `Join.output_schema`, 509,572
  `DynamicPodStream.output_schema`, 203,890 `keys`, 305,830 source `output_schema` calls.
  Max calls on one node: 4x per stage (294, 1190, 4762 for 3, 4, 5 stages).
- `content_hash` and `pipeline_hash` are already memoized (`core/base.py`): constant per node.
- Same 4x growth for job compile, `job.save()`, `PipelineJob.load()` (operators), async
  `job.run()` (function pods), `head()`, preview, stream `run()`: all through `output_schema`.
- Staleness (`last_modified` / `is_stale`) in stream `run()` of a function-pod diamond is also
  exponential on main: PR #380's territory; measure on its head, do not fix here.

## Proposal (sent to coordinator; wait for reply)

Memoize the output schema on each derived stream that owns a fan-in walk, keyed by the
normalized `ColumnConfig` (skip the memo when the config is unhashable), `keys()` derived from
it. A shared private helper on `StreamBase`.

Depends on: the pod (immutable configuration, incl. `system_tag_n_char`), the upstream tuple,
and the upstreams' schemas and pipeline hashes. Never on rows: a schema is predicted, never read
from computed data, so a `DynamicPodStream` recomputing its rows when stale gives the same
schema. Invalidation: node `upstreams` setter; `PipelineJob.bind(sources=...)` (and
`SourceJobNode.bound_source` setter) because bind validates column names only, so a rebound
source of other types changes downstream schemas (downstream pipeline-hash memos already have
this same latent gap: report it).

## Steps after approval

1. Failing tests (call counts, linear in nodes): 20 stacked stages as streams and in a
   `PipelineJob`; per-node call count of `output_schema` bounded by a constant.
2. Helper + memo in the four owners; invalidation hooks.
3. Re-measure every walk at 20 stages; table for the PR and worker_done.
4. Golden + hash label rule tests unchanged; full suite; changelog fragment; design_issues entry.
