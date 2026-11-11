# ITL-578 — merge main after #291 (SemiJoin via Polars)

PR #286 conflicts with `main` (b197544e) after #291 (ITL-847) merged. Mechanical merge, no
design decisions.

## Steps

1. `git merge origin/main` (no rebase). Only conflict: `DESIGN_ISSUES.md`, where both sides
   appended a new section at the end. Keep main's SemiJoin SJ1 section, then ours (Polars PL1)
   last, each exactly once.
2. No CHANGELOG change: the PR has no CHANGELOG entry, and main has no `[0.3.0] — unreleased`
   section (brief: no other changes). Flag in worker_done.
3. Golden check: `git diff origin/main -- tests/test_hashing/golden/data` must be additions
   only (the `polars_*` entries). Run `tests/test_hashing/golden` without regenerating.
4. SemiJoin over a polars-typed column: #291 casts the result to the left table's schema, so a
   `polars.dataframe` logical column on the left must survive. Add one public-interface test
   in `tests/test_core/function_pod/test_polars_dataframe_values.py` if it adds coverage.
5. Full suite on 3.11/3.12/3.13, ruff check, ruff format --check, basedpyright.
6. Push, wait for CI, request CodeRabbit once, handle its round (test/doc/wording on merit;
   anything touching storage bytes, hash definition, dtype grammar, names, behaviour: ask).
7. worker_done with the final head.

## Progress notes

- (update as steps complete)
- Merge fbf2ed66, SemiJoin test 2abaf4c5; suite green 3.11/3.12/3.13; CI green.
- CodeRabbit round on 2abaf4c5: column-order cache-key finding declined (coordinator approved,
  developer decision Nov 10), docs caveat added in hashing.md and function-pods.md; body-only
  items answered as no-change in one top-level comment.
