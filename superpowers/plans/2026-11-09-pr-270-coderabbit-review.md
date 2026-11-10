# PR #270 (ITL-816) — CodeRabbit review log

PR: https://github.com/metamorphic-labs/orcapod-python/pull/270

Each CodeRabbit finding is verified against the code before it is fixed or dismissed.
Verdicts: **FIX** (finding is right), **PARTIAL** (right problem, different remedy),
**DISMISS** (wrong, out of scope, or contradicts a project rule).

## Findings

| Round | Finding | Location | Verdict | Reason | Commit / reply |
|---|---|---|---|---|---|
| 1 | Pre-merge check "Docstring Coverage": 55.56% of the functions touched by the diff have docstrings, threshold 80% | `tests/test_core/streams/test_iter_data_chunk_layouts.py` (review of `a0fcdcff`) | FIX | Verified: six test functions and the test class in the new file had no docstring, and `CLAUDE.md` asks for Google-style docstrings everywhere. Added a one-line docstring to each. | Round 1 commit |
| 1 | Security architecture note (labelled "observed", explicitly "not a verified denial-of-service finding"): `iter_data` converts the whole source-info table to Python rows before the first yield, where the base converted one slice per row | `src/orcapod/core/streams/arrow_table_stream.py`, `iter_data` | DISMISS | Not raised as an actionable finding, and the peak memory is unchanged in order: `iter_data` already retains every `(Tag, Data)` pair, each holding its own source-info dict, in `_cached_elements`. The up-front `to_pylist()` holds the same dicts the pairs go on to reference, and replaces a per-row `slice(i, 1).to_pylist()` that was both slower and the origin of the batch-local indexing bug. | Round 1 summary comment |

## Developer review comment (not a CodeRabbit finding)

| Round | Comment | Location | Action | Commit / reply |
|---|---|---|---|---|
| 1 | Are system tags preserved when a stream has no ordinary tag columns? | `src/orcapod/core/streams/arrow_table_stream.py:275` | They are: `iter_data` selects `self._all_tag_columns`, which is `self._tag_columns + self._system_tag_columns`, so a `Tag` is still built from the system-tag columns and `Tag.__init__` extracts them. No test pinned it; added `test_system_tags_survive_without_ordinary_tag_columns` (system tags only) and `test_system_tags_pair_with_ordinary_tags_and_data` (both), over mismatched layouts including zero-length chunks. | Round 1 commit; thread left open for the developer |

## Other changes made in round 1

- `tests/test_core/streams/test_iter_data_chunk_layouts.py` was not `ruff format` clean; it
  is now (formatting only, no behaviour change).

## Rounds

| Round | Head pushed | CodeRabbit review of that head |
|---|---|---|
| 0 | `a0fcdcff` | No inline findings; Docstring Coverage pre-merge check failed (warning). |
