# ITL-1474 — Bound async function pod concurrency by default

## Proposed default

Use 16 in-flight calls per function pod during async execution, matching the existing
side-effect pod default. The limit applies to each pod execution, including the compute
phase of an async job, and controls how far the pod reads ahead. The pod acquires its
semaphore before creating a row task and holds the permit through invocation and output
delivery. The async job orchestrator starts nodes concurrently and bounds each edge's
queue separately. A bounded channel alone cannot bound calls because an unrestricted pod
can drain it into tasks.

The local executor awaits coroutine functions on the job's event loop. It submits
synchronous functions to that loop's default thread pool, which already caps active
threads but can queue submitted work. The no-executor Python function path uses the same
event-loop and thread-pool split. A synchronous call into a coroutine has a separate,
single-thread bridge when a loop is already running. The optional Ray executor submits
remote tasks and waits on their futures. Ray schedules those tasks using its own capacity
and task resource options. There is no local process pool for function invocations.

CPython's default thread pool uses up to `min(32, (cpu_count or 1) + 4)` workers. In
Python 3.11 and 3.12, it uses `os.cpu_count()`. Since 3.13, it uses
`os.process_cpu_count()`. A machine count of 18 thus yields a pool of 22 under the
corresponding default. With the proposed cap, one pod on that loop can submit at most
16 calls at once. Where the pool has fewer available workers, fewer calls run and the
rest wait in the pool. The pool is shared by the loop, while the semaphore is per pod.

- **Constant (recommended).** Sixteen bounds waiting row tasks and executor submissions
  on every host while allowing overlap for I/O. Different workloads can override it.
- **Host logical CPUs (`os.cpu_count()`).** Easy to obtain, but can count CPUs the process
  cannot use. It says little about coroutine I/O or capacity on a remote Ray cluster.
- **Process-visible logical CPUs.** Process affinity on supported platforms, or
  `os.process_cpu_count()` on Python 3.13 and later, better reflects usable CPUs for local
  CPU-bound work. It still does not reliably describe a container's CPU-time quota, or
  the capacity and queueing behavior of the selected executor.
- **Container quota.** Reading platform-specific quotas would estimate local CPU-time
  capacity. It adds platform logic and still cannot predict I/O capacity or remote worker
  resources. Local CPU-heavy work can choose an explicit limit based on its executor.

## Configuration and storage

Compute the default for `PipelineConfig.default_max_concurrency` with a dedicated
default factory so there is one place to select the limit. Use the distinct
`"inherit"` default for `PodConfig.max_concurrency`. An
explicit positive integer or `None` on the pod wins over the pipeline config. `None` means
unlimited pod dispatch at either level. If the pod setting is omitted, the resolver uses
the supplied pipeline config, or the default pipeline config when none is supplied. Keep
existing validation of the effective positive limit.

| Pod setting | Pipeline setting | Effective limit |
| --- | --- | --- |
| Omitted or `"inherit"` | Omitted | Built-in default |
| Omitted or `"inherit"` | Positive integer | Pipeline integer |
| Omitted or `"inherit"` | Explicit `None` | Unlimited |
| Positive integer | Any | Pod integer |
| Explicit `None` | Any | Unlimited |

`PipelineConfig` currently reaches the pod through the `pipeline_config` argument of
`FunctionPod.async_execute`. `PipelineJob.run_async` and `AsyncPipelineOrchestrator` do
not expose this argument today. Job nodes delegate their compute work to the pod and
therefore pick up the default. Preserve those entry points and describe the pipeline-level
precedence at the pod API that supports it.

Keep `"pod_config": null` in the existing position for an inherited setting. Write an
explicit integer as before and write `{"max_concurrency": null}` for explicit unlimited
dispatch. Existing saved configs with a null or missing `pod_config` load as inherited.
Earlier versions collapsed explicit `None` into omission, so those saves cannot carry the
original intent. Keep the default and settings out of identity and stored-path logic. Leave
the sync execution path and the async semaphore's lifecycle alone. The effective limit
only changes the number of in-flight rows. Async completion order remains unspecified.

## Verification and delivery

1. Write regressions first, using several rows, two tag columns and two data columns in
   multiple layouts. Count concurrent calls in the real async job path with an async
   function that yields while active. Test the default, an explicit positive limit and an
   explicit unlimited setting, then verify the results and the equivalent synchronous
   run. Exercise pipeline precedence via `FunctionPod.async_execute` with bounded
   channels, including integer overrides, explicit unlimited overrides and inheritance.
2. Round-trip the public function pod config through JSON for omitted, numeric and null
   values. Compare the untouched pod's serialized bytes before and after. Check pod and
   pipeline hashes with `tests/test_hashing/golden/generate_golden_hashes.py --check` and stop if there is
   drift. Run the new regressions on unmodified library code and record the failures.
   Cases that demonstrate existing behavior serve as controls in the regressions.
3. Update the function pod and pipeline job docs and link to the async concurrency
   guidance where appropriate. Cover the migration to explicit unlimited dispatch in
   `changelog.d/ITL-1474.md`. Run the affected documentation samples.
4. Run targeted tests serially, Ruff lint and format checks, basedpyright, the design
   issues check and the changelog check. Check clean three-way merge compatibility against
   PRs #559–#571 by head without changing any stack branch. Open a draft PR against
   `main` and use CI for the full suite.
