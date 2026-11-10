# PR #271 — CodeRabbit review log

PR: https://github.com/metamorphic-labs/orcapod-python/pull/271 (PLT-4816, docs audit).

Every CodeRabbit finding, checked against the code before acting on it. Verdicts: **FIX** (the
finding is right), **PARTIAL** (right problem, different remedy), **DISMISS** (wrong, out of
scope, or against a project rule).

## Round 1 — review of `39b58ea8`

Review: https://github.com/metamorphic-labs/orcapod-python/pull/271#pullrequestreview-5834637363

| # | Finding | Location | Verdict | Reason | Commit / reply |
|---|---------|----------|---------|--------|----------------|
| 1.1 | `HOME` does not isolate `Path.home()` on Windows, so the default `enable_file_hash_caching()` sample could write the user's real cache | `docs/concepts/file-hash-caching.md:42` (fix is in `tests/test_docs/test_doc_samples.py`) | FIX | True: `SqliteHashCacher.DEFAULT_DB_PATH` is `Path.home() / ".orcapod" / ...`, and `Path.home()` reads `USERPROFILE` on Windows. The test subprocess now sets both. | `ccb088c0`, [thread](https://github.com/metamorphic-labs/orcapod-python/pull/271#discussion_r4519474009) |
| 1.2 | "`get_all_records()` -- every record stored for this function" overstates the scope | `docs/concepts/function-pods.md:219-220` | PARTIAL | Right problem. Measured on `main`: a node returns rows from earlier runs of the same job over the same source (other inputs included), but not rows the same function produced under a different job name or a different `source_id`. Reworded to say exactly that instead of the suggested "joined records for this node", which names an internal detail and drops the true, useful part (earlier runs are included). The suggested "returns `None`" clause was left out: the sentence is about a node of a job that has run, where it does not happen. | `ccb088c0`, [thread](https://github.com/metamorphic-labs/orcapod-python/pull/271#discussion_r4519474018) |
| 1.3 | Prose says pass `drop_partial_batch=True`, the sample passes `False` | `docs/concepts/operators.md:177-181` | FIX | True, the sample contradicted its own sentence. Sample now passes `True` (the signature is `Batch(batch_size=0, drop_partial_batch=False)`). | `ccb088c0`, [thread](https://github.com/metamorphic-labs/orcapod-python/pull/271#discussion_r4519474037) |
| 1.4 | README says every Python sample in the docs is executed; two are skipped | `README.md:6` | FIX | True, and the claim was also too wide in a second way: `docs/api/` is not in the run. README now lists the pages that are executed and names the two PostgreSQL samples that are not. | `ccb088c0`, [thread](https://github.com/metamorphic-labs/orcapod-python/pull/271#discussion_r4519474041) |
| 1.5 | A `python` fence written with four backticks or with tildes is silently not tested | `tests/test_docs/_doc_samples.py:38-39` | FIX | True: the regex only knew a three-backtick fence, so such a sample would drop out of the run without a failure. `extract_blocks` now tracks the fence character and length. No page uses such a fence today, so no sample changed status. Also accepts `python` followed by attributes (`python title="x"`), which the old regex dropped the same way. A longer run of the fence character inside a fence raises: GitHub (CommonMark) closes the fence there and MkDocs SuperFences (exact match, checked in `pymdownx/superfences.py`) does not. | `ccb088c0`, [thread](https://github.com/metamorphic-labs/orcapod-python/pull/271#discussion_r4519474046) |
| 1.6 | A `python` fence line inside a non-Python fence is extracted and executed | `tests/test_docs/_doc_samples.py:81-83` | FIX | True: other-language fences were not tracked, so their content was scanned for openers. They are now consumed to their own closing fence. The file CodeRabbit cites (`docs/metamorphic/plans/...`) is not in `DOC_PAGES`, so the test run was never affected; the fix matters for the CLI and for future pages. | `ccb088c0`, [thread](https://github.com/metamorphic-labs/orcapod-python/pull/271#discussion_r4519474056) |
| 1.7 | `proc.stdout.splitlines()[-1]` raises a bare `IndexError` when the runner prints nothing (review body, marked unverified) | `tests/test_docs/test_doc_samples.py:228` | FIX | True but narrow: it needs exit code 0 with empty stdout. One assertion with the runner's stderr in its message. | `ccb088c0`, round comment |
| 1.8 | Pre-merge check "Linked Issues": the PR does not deliver the tutorial PLT-4816 asks for | PR | DISMISS | Out of scope by design. The PR body says "First part of PLT-4816" and "Part of PLT-4816" (not "Fixes"), and states the tutorial is not in this PR; it is written in PR #274. | round comment |
| 1.9 | Pre-merge check "Docstring Coverage": 70 % of the touched functions, threshold 80 % | `tests/test_docs/` | FIX | CLAUDE.md asks for Google-style docstrings everywhere. Added them to the helpers and tests of this PR that had none or a one-liner without `Args`/`Returns` (`_normalize`, `_to_regex`, `main`, `test_mkdocs_build_strict`); the new `_fence_language` has one. | `ccb088c0`, round comment |

Local suite after the round: `uv run pytest tests/` (without `test_graph_rendering.py`, which needs the Graphviz `dot` binary) — 4827 passed, 93 skipped, 2 xfailed.

### Coordinator instruction in the same push

Not a CodeRabbit finding. The repository is private and Orcapod is distributed only through
PyPI, so commit `c19e71ba` makes `pip install orcapod` / `uv add orcapod` the only install path
in `docs/index.md` and removes `repo_url` from `mkdocs.yml` (the published site no longer
links to a repository readers cannot open).

## Round 2 — review of `13d5834c`

The push was first rate-limited; the review ran after `@coderabbitai review` once the limit
reset. Review: https://github.com/metamorphic-labs/orcapod-python/pull/271#pullrequestreview-5834990800

| # | Finding | Location | Verdict | Reason | Commit / reply |
|---|---------|----------|---------|--------|----------------|
| 2.1 | The design-issue entry says every Python block is run; two PostgreSQL blocks are skipped | `DESIGN_ISSUES.md:1703-1705` | FIX | True, same over-claim as 1.4. Entry now names the two skipped samples. | `ccc29cd8`, [thread](https://github.com/metamorphic-labs/orcapod-python/pull/271#discussion_r4519765747) |
| 2.2 | The exact-match rule ignores parameters with defaults | `docs/concepts/function-pods.md:91-94` and summary line 27 | FIX | True, checked on `main`: `def bmi(weight, height=0.1)` applied to a stream with only `weight` runs and uses the default. Both places now say a defaulted parameter may be left out; extra columns, missing required columns and wrong types are still rejected. | `ccc29cd8`, [thread](https://github.com/metamorphic-labs/orcapod-python/pull/271#discussion_r4519765752) |
| 2.3 | After rebinding a job to fewer rows and rerunning, `iter_data()` / `as_table()` can still show the removed rows | `docs/concepts/identity.md:108-119` | DISMISS | Measured on `main`: a `PipelineJob` run over 3 rows, then rebuilt over the same store and source with 1 row, gives `iter_data()` = `as_table()` = 1 row, while `get_all_records()` returns all 3. The page's statement ("its current inputs only") is what the code does; the removed rows are visible only through `get_all_records()`, which the page now says (2.4). | [thread](https://github.com/metamorphic-labs/orcapod-python/pull/271#discussion_r4519765764) |
| 2.4 | "`get_all_records()` returns everything stored in the shared table" is wrong: rows of other nodes are filtered out | `docs/concepts/identity.md:118-119` | PARTIAL | Right problem (the node filters on its own content hash, measured in 1.2). Reworded in user terms rather than the suggested "this node's content hash": rows from earlier runs of the same job and source are included, rows other nodes wrote to the shared table (e.g. another `source_id`) are not. Same wording as `function-pods.md` after 1.2. | `ccc29cd8`, [thread](https://github.com/metamorphic-labs/orcapod-python/pull/271#discussion_r4519765776) |
| 2.5 | The plan allows skipping "deliberately schematic" samples, against the CLAUDE.md rule (review body, marked unverified) | `superpowers/plans/2026-11-09-plt-4816-docs-audit.md:54-55` | FIX | True: CLAUDE.md allows the marker only for missing infrastructure. No page uses it otherwise (the only two markers are the PostgreSQL samples), so only the plan's wording changed. | `ccc29cd8`, round comment |
| 2.6 | Pre-merge check "Linked Issues": no tutorial (repeat of 1.8) | PR | DISMISS (2nd time) | Same as 1.8: this PR is "Part of PLT-4816"; the tutorial is in #274. The check will keep failing on this PR by construction. | round comment |

Local suite after the round: same command as round 1 — 4827 passed, 93 skipped, 2 xfailed.

## Round 3 — review of `4e1b7f8d`

Review: https://github.com/metamorphic-labs/orcapod-python/pull/271#pullrequestreview-5835110890.
No inline comments; two review-body items and the repeated pre-merge check.

| # | Finding | Location | Verdict | Reason | Commit / reply |
|---|---------|----------|---------|--------|----------------|
| 3.1 | Pass `check=False` explicitly to `subprocess.run` when the return code is inspected (review body, marked unverified, from an org "learning") | `tests/test_docs/test_mkdocs_build.py:26-40` | FIX | Both calls inspect `proc.returncode` themselves; an explicit `check=False` states that. Ruff in this repo does not enable `PLW1510`, so this is a stated org preference, not a lint error. No behaviour change. | `aa902e23`, round comment |
| 3.2 | Same, second call | `tests/test_docs/test_doc_samples.py:273-280` | FIX | As 3.1. | `aa902e23`, round comment |
| 3.3 | Pre-merge check "Linked Issues": no tutorial (third time) | PR | DISMISS (3rd time) | As 1.8 and 2.6. This is the stop condition in the brief (a finding dismissed twice is repeated); it is reported to the coordinator instead of being argued again. | round comment |

Local suite after the round: same command as round 1 — 4827 passed, 93 skipped, 2 xfailed.
