# Reuse completed work across pipeline runs

Status: proposed for developer approval. Implementation and PR requested November 13, 2026.

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

No matching open Linear issue was found in the cache/rerun searches. Create a dedicated
issue and design-issue entry once approved, use the issue's branch name, write the detailed
implementation plan, add failing regressions, implement, review and open a PR against main.
No deployment, release or merge is included.
