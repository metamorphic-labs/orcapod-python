# ITL-1063 Pipeline Cache Reuse Implementation Plan

> **For agentic workers:** Use sensei:executing-plans task by task; independently owned source and execution changes may run in parallel under sensei:dispatching-parallel-agents.

**Goal:** Unchanged pipeline reruns reuse retained rows and their hashes while changed or failed work is processed correctly.

**Architecture:** Completed reads and node execution retain results against the exact upstream state. Unknown freshness, changed bindings/configuration and incomplete runs must never qualify as complete. Existing persistent per-row caching remains the recovery path.

**Tech Stack:** Python, Arrow, pytest, uv, existing sync and channel-based async orchestration.

## 1. Execution reuse and row preservation

Files: `src/orcapod/pipeline/sync_orchestrator.py`, `async_orchestrator.py`, focused internal cache helper if needed, `src/orcapod/core/nodes/{function_node,operator_node,source_node}.py`, `tests/test_pipeline/test_execution_cache_reuse.py`.

- [ ] Add regression cases for repeated flat/grouped jobs, sync/async/cross-orchestrator execution. Count `StarfixArrowHasher.hash_table`, `GroupBy.unary_static_process`, and user function calls after the first successful run. Assert unchanged row values, provenance and summary counts.
- [ ] Run `uv run pytest tests/test_pipeline/test_execution_cache_reuse.py -q` and confirm redundant computation failures.
- [ ] Preserve `(Tag, Data)` references when wrapping execution buffers, including data context; do not transfer hashes across transformations or drop source/system columns.
- [ ] Add a completed-execution reuse path with conservative upstream validity checks. Reuse empty outputs too; do not use timestamps or source identity hashes alone as proof for mutable/dynamic sources. Preserve lifecycle hooks and side-effect stale-store checks.
- [ ] Avoid unconditional async DB loads when the same valid completed result is retained. Do not change streaming behavior for first runs or force a barrier onto dynamic paths.
- [ ] Test source rebinding, explicit clear/invalidation, partial and failed runs, observer counts, side effects, and materialize_results=False. Preserve per-row recovery for incomplete runs.
- [ ] Run new tests plus `tests/test_pipeline`, `tests/test_core/nodes`, `tests/test_side_effects` and channel tests. Fix failures in scope.

## 2. Pinned Spiral snapshot retention

Files: `src/orcapod/core/sources/spiraldb_table_source.py`, `tests/test_core/sources/test_spiraldb_table_source_streaming.py`, `docs/concepts/sources.md`.

- [ ] Add fake-connector scan-count regressions: lazy construction, complete repeat reads, mixed table/row/async reads, failed/partial reads, empty pinned snapshot and concurrent readers.
- [ ] Run focused tests to demonstrate repeated scans.
- [ ] Retain successfully completed pinned-source read buffers with reusable row objects or Arrow streams. Publish only on completion; concurrent readers must not expose partial cache state. Unpinned/unknown table versions must stay conservative. No hidden repinning or hash changes.
- [ ] Keep head bounded and lazy before a full read; allow it to use retained rows afterward. Document retention memory cost and constructing a new source to select a new snapshot.
- [ ] Run Spiral source tests and the existing live-test collection without requiring live credentials.

## 3. Integration, documentation and delivery

Files: `design_issues/ITL-1063-CR1.md`, `changelog.d/ITL-1063.md`, approved design and this plan.

- [ ] Review interactions between source snapshots, node invalidation, sync/async switching and existing row readback caches.
- [ ] Record implementation decisions and limitations in the approved design; add the authorized design-issue entry and changelog.
- [ ] Benchmark flat and grouped repeated jobs with function/operator/hash counters and compare to baseline. Use operation-count assertions, not flaky timing thresholds, in tests.
- [ ] Run `uv run ruff check .`, `uv run ruff format --check .`, `uv run basedpyright`, and `uv run pytest tests/`; inspect exit codes and logs.
- [ ] Request independent review; resolve correctness findings and rerun affected checks.
- [ ] Commit with conventional message, push issue branch, create PR against main linking ITL-1063, and inspect CI. Include measurements and decisions; do not merge.

## Authorization and decisions

Developer approved the design, issue creation and implementation through PR. Existing isolated cowfish workspace is used. No additional confirmation for routine implementation decisions; record them for the developer's return.
