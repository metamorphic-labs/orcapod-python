# Reuse completed work across pipeline runs

Status: approved November 13, 2026; implementation tracked as ITL-1063.

## Problem and evidence

On main at 89160c47, a repeated job can hit every function-result cache while repeating
source reads, operators, row reconstruction and hashes. A 10,000-row source grouped into
100 function inputs took 329 ms on a warm job run, with one GroupBy execution and 200 Arrow
hashes; rereading the retained function output took 38 microseconds and performed neither.
The sync orchestrator reconstructs Data objects whose per-object hash cache is empty.
The async function path unconditionally reloads stored results. SpiralDB sources pin a
version at construction but scan it and rebuild provenance on every read.

## Recommended approach

Reuse completed node outputs against the upstream state that produced them, on both sync
and async execution paths. Keep the existing result database cache for new job objects,
changed inputs and recovery. Preserve row objects and their memoized hashes across sync
orchestration boundaries; table materialization remains available for operators that need
it. A cached result must retain its original schema, data context, system tags and per-row
source information.

An execution cache is valid only after successful completion and only for the same inputs
and configuration. Rebinding a source, clearing a cache, changing an upstream generation
or using a dynamic source must prevent inappropriate whole-node reuse. Partial or failed
runs retain successful per-row work through the existing recovery mechanism but must not
be marked complete. Empty successful outputs are valid completed results.

Side-effect completion checks, pending-write failures, observer lifecycle notifications
and RunSummary accounting must still run as required. An optimization must not skip a
side-effect safety check or silently turn a failed run into a successful cached run.

For SpiralDB, keep construction lazy and retain a successfully completed read of the
source object's pinned version. Repeated iteration and table access reuse that snapshot.
Constructing and binding a new source reads a new snapshot. A failed or interrupted read
must not publish a truncated cache. The implementation must explicitly handle concurrent
sync/async readers and the unversioned empty-table case; unknown freshness is not proof
that a source is unchanged. Document the memory cost of retaining the read rather than
claiming bounded-memory streaming after caching is introduced.

Do not change hash definitions, stored record identifiers or database layouts. Avoid
rewriting database indexing or query pushdown as part of this PR.

## Alternatives considered

1. Preserve hashes only: small, but sources, grouping and joins still repeat, leaving the
   dominant ediop costs intact.
2. Skip the entire job after its first run: fast, but obscures per-node invalidation,
   dynamic sources, side effects and failure recovery.
3. Reuse completed node/stream work (recommended): matches the intended stream semantics
   and makes invalidation local and testable.

## Verification

Use counters and object identity assertions rather than elapsed-time thresholds:

- An unchanged job run twice performs no new function calls, operator computation or
  hashes of previously processed rows; exercise sync, async and switching orchestrators.
- Retained output rows preserve values, schemas, provenance and hashes, including joins,
  grouping, filtering, empty output and logical types.
- A source rebind or explicit invalidation recomputes affected work and serves changed
  results; dynamic/polling sources continue to observe new input.
- A failed or partial run retries missing work without losing completed rows.
- A pinned Spiral source is constructed without scans; its completed snapshot is scanned
  once across repeated reads; interrupted/failed reads do not masquerade as complete.
- Side-effect guards and observer summaries retain their existing behavior.
- New job objects continue to recover results from the persistent store.

Repeat the general flat and grouped benchmarks, then run relevant regression suites and
the repository's required lint, format, type and test checks. Include measured results and
the snapshot-memory tradeoff in the PR description.

## Tracking and delivery

ITL-1063 tracks the approved implementation. The branch is
`eywalker/itl-1063-reuse-completed-pipeline-work-and-row`; the implementation plan is
`superpowers/plans/2026-11-13-itl-1063-cache-reuse.md`. No deployment, release or merge is
included.

## Decisions made during implementation

- Keep concurrent initial Spiral reads independent rather than holding a lock across
  consumer yields. The first completed read publishes its snapshot atomically; incomplete
  or cancelled reads never publish. Later readers reuse its row objects. This avoids
  deadlocks when iterators overlap or a sync consumer runs on the async event-loop thread.
- Retain batch streams rather than eagerly constructing rows for a table-only read.
  Row iteration initializes each retained batch's row memo under a short lock. Unknown
  table versions are not cached. A retained head uses the snapshot without another scan.
- Scope complete-execution reuse to trusted orchestrator graph execution. Direct node
  execution may receive inputs different from its declared upstreams, so declared graph
  state alone cannot certify those calls.
- Use conservative known snapshot support. Custom and polling sources must not be
  considered unchanged merely because their identity hash or construction timestamp is
  unchanged. Preserve ordinary execution for them.
- Continue per-row function observer callbacks and summary accounting instead of skipping
  the entire job. Warm runs may still perform linear iteration; the regression criterion
  is no repeated user computation, operator computation, or row hashing for unchanged
  supported snapshots.
- The developer explicitly authorized judgment calls while away; review findings are
  resolved within this scope and decisions are recorded here and in the PR.
- Freeze graph definitions before callbacks and require a one-use orchestrator invocation
  ticket. Source rebinding during callbacks and nested direct calls cannot certify stale
  buffered rows as current work.
- Warm async operators drain their input channels and wait for upstream task success
  before replaying retained output. Channel closure alone also occurs on failure and is
  insufficient evidence. Cold and unknown-source execution retain the streaming path.
- Preserve actual object references in identity comparisons and entry-ID memoization;
  do not rely on reusable integer object addresses or transfer hashes to new values.
- Preserve the sync path's historical Arrow-decoded values with a retained storage view
  for each Tag/Data object. Keep the original object untouched, so this fix does not alter
  async/standalone subclass behavior tracked by ITL-978. The view shares the original
  exact Arrow representation and hashes, and identity memoization recognizes that origin.
  Explicit invalidation clears the retained views; replaced buffers discard old entries.

## Measured result

CodeRabbit review follow-up: snapshot construction now memoizes each node within one
traversal, and definition comparison visits each shared ancestor pair once. Proof
validation also memoizes upstream checks within that call. Fresh traversals still detect
callback changes. A nested untrusted node execution advances the invalidation generation
so an outer trusted invocation cannot certify a replacement cache. Regression coverage
includes twenty split/join stages and both sync and async nested execution.

The 1,000-row benchmark compared base `89160c47` with this implementation using the
same environment and in-memory result database. First warm runs (milliseconds):

| Pipeline | Before | After | Arrow hash calls before / after | GroupBy calls before / after |
| --- | ---: | ---: | ---: | ---: |
| Flat sync | 275.82 | 19.58 | 2000 / 0 | 0 / 0 |
| Flat async | 233.72 | 5.40 | 1000 / 0 | 0 / 0 |
| Grouped sync | 29.22 | 16.86 | 20 / 0 | 1 / 0 |
| Grouped async | 31.24 | 2.38 | 20 / 0 | 1 / 0 |

Warm user-function calls were zero both before and after. These are local illustrative
timings, not performance guarantees; operation counts are regression-tested. Sync still
materializes Arrow tables at node boundaries, and both paths retain traversal/callback
cost. No constant-time, cross-process or newly constructed job guarantee is claimed.

## Claude/eywalker review and latest-main integration

Merged main `5c7a058f` (including stored data-ID readback, stale table-cache,
sorted previews, interleaved stream reads, crash-recovery isolation and Join fixes)
without rewriting the published branch.

- Completed-work reuse now also requires the existing `not node.is_stale` gate.
  Proven no-op function/operator executions preserve timestamps; actual work, failures,
  unknown sources and changed bindings keep normal freshness updates. Source nodes
  follow their bound source freshness and explicitly invalidate on rebinding/clearing.
- `Datagram.storage_view()` owns conversion to declared stored values. Nodes own
  memoized views through a release method; `ArrowTableStream.from_rows()` owns its
  row buffer. Orchestration no longer mutates datagram internals or stream buffers.
  Source-field nullability still normalizes across a combined table, whereas each
  retained row keeps its own source schema. Regression coverage verifies identical
  source values, payload hashes and materialized stored schemas.
- Retention remains the default, matching the requested reusable streams. It is O(N),
  not bounded streaming memory. `job.clear_cache()` releases node and bound-source
  memory without deleting persistent results; `include_sources=False` keeps shared
  source snapshots. Spiral `retain_snapshot=False` opts out; source `clear_cache()`
  does not repin and generation checks prevent an in-flight read from republishing.
- Keep the exact supported-source allowlist, document it and log one INFO explanation
  per run for unsupported sources. A general source snapshot capability remains a
  separately designed follow-up in ITL-1064; timestamps alone are not sufficient.
- Restore Index/Pick metadata expected-failure markers and historical metadata
  exclusion at materialization boundaries; no stored hash/schema migration is added.
- Added store-write failure, dangling-reference repair/failure and true None-datagram
  regressions for both orchestrators. A function returning a Python None value is
  distinct from producing no datagram. All decorated execute methods take observer
  keyword-only, so positional-observer wrapping is not a supported missing path.

The original benchmark above remains a historical comparison against `89160c47`.
A new run after integrating main again records zero warm Arrow hashes, user function
calls and GroupBy calls in all four cases. Timing collected alongside validation is
not a replacement for isolated measurements on the actual ediop dataset.

## Further CodeRabbit review

- Activation changes advance a function generation and freshness timestamp. A job
  removes retained absent-output entries when that generation changes, preserving
  successful cached results. Completion proofs include activation state so downstream
  empty outputs cannot hide a reactivated function. Repeated unchanged activation
  preserves the warm cache.
- Async Spiral reads construct batch rows and acquire the snapshot lock in a worker
  thread, keeping the event loop responsive, including during cancellation.
- The row factory exports the retained context explicitly and requires consistent row
  and stream contexts. It rejects provenance overrides and system-tag relabeling that
  would silently make table and row views disagree; callers transform rows first.
  Empty tags inherit their stream context. Storage views initialize a lazy UUID before
  copying so original and view retain the same identity.
