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
