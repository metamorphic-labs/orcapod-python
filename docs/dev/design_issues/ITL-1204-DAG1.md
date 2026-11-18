## `src/orcapod/pipeline/dag.py` — the node DAG of a `PipelineJob`

### DAG1 — a source slot's node is lost from `job.dag` once a source is bound to it
**Status:** open
**Severity:** medium
**Issue:** ITL-1204

`PipelineJob.dag` is an `OrcaDAG` keyed by the node objects (a dict per node). A node's
`__hash__` comes from its content hash, and a `SourceJobNode`'s content hash includes its
bound source. Binding a source to an unbound slot (a job made by
`PipelineJob.from_pipeline(pipeline, database=...)`, then `job.bind(sources=...)`) changes
the node's hash after it was put in the DAG: `node in job.dag` is `False`, and
`predecessors`, `successors`, `ancestors` and `in_degree` raise `KeyError`. Iterating
`nodes()` and `edges()` still yields it. A job recorded with its sources is not affected.

No library code looks a node up in `job.dag` after binding, so `job.run()` works; user code
and tools that use the public `job.dag` by node break. The notebook diagram (ITL-1197) builds
its adjacency from `edges()` for this reason. Options: key the DAG by a stable key (the
compile-time hash key, or identity), give job nodes an identity-based `__hash__`, or rebuild
the DAG on bind; each needs a check of other places that put job nodes in sets or dict keys.
