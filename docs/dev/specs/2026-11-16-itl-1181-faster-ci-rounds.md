# Faster CI rounds: parallel tests, one coverage job per pull request (ITL-1181)

Developer decision, November 16, 2026: "run CI in parallel with only one coverage to be
reported and full three versions only on pushes to main". The test suite is not split; the
split ITL-1181 first asked for is kept below as a reserve plan.

## The problem, measured

Before this change every pull request ran the whole suite (about 25,000 tests) twice on each
of Python 3.11, 3.12 and 3.13, serially:

- the organization's required check, `pytest / pytest (3.x)` (shared workflow
  `metamorphic-labs/shared-workflows`, `ci-pytest.yml`, running `uv run pytest tests`): 10 to
  19 minutes per job. This repository cannot change its command; only `pytest.ini` and
  `conftest.py` reach it.
- this repository's `run-tests.yml`, `test (3.x)`, with coverage: 19 to 37 minutes per job.
  Coverage cost +55 % on Python 3.11 and about 2.5 times on 3.12 and 3.13 with the default
  tracer (`COVERAGE_CORE=sysmon` removes most of it on 3.12 and later).

A round of review on PR #416 (the last pull request before this one) took 30 minutes from
push to the last check: the `test (3.12)` coverage job, 29 minutes.

Locally, serial: 963 s. With pytest-xdist on four workers: 261 s, 24,842 passed, no failure
that only the parallel run shows (measured by the previous worker on ITL-1181).

## What changed

1. pytest-xdist is a development dependency. `tests/conftest.py` (`pytest_cmdline_main`)
   sets `-n logical` when `CI` is `true` and no `-n` was given, and never on an xdist worker
   (xdist clears `-n` in a worker before calling the hook there; setting it again would
   make every worker start workers). That is the one place it is turned on, and the way the
   organization's required check runs in parallel. Local runs stay serial.
2. `run-tests.yml`: a pull request runs one job, `test (3.13)`, with
   `COVERAGE_CORE=sysmon` and `-n logical`, whose coverage is the one reported to Codecov. A
   push (to `main`, `dev`, `extension-type-system`) and a manual run use Python 3.11, 3.12
   and 3.13, with coverage as before (3.11 with the C tracer, 3.12 and 3.13 with `sysmon`).
   The PostgreSQL job and the SpiralDB integration job stay serial (`-n 0`): their tests
   share one server or project.
3. The tests in `tests/test_pipeline/test_job_run_in_event_loop.py` that press the
   notebook's interrupt button (`_thread.interrupt_main`) install Python's default SIGINT
   handler for their duration (fixture `interrupt_raises_keyboard_interrupt`). In a process
   that ignores SIGINT (started in the background) `interrupt_main` does nothing, and the six
   tests failed.
4. `lint.yml` (job `Ruff and basedpyright`) runs on this repository too. Once the repository
   is public (ITL-1193) the organization ruleset no longer applies to it, and the
   repository's own checks are all that remain.
5. `release.yml`: the test step runs with `-n logical`; nothing else changed.

## Results on this pull request

A GitHub runner for a private repository has two logical CPUs and one physical core, so
`-n auto` (physical cores) started one worker on the first round; `-n logical` starts two.
Test time is pytest's own figure; a job adds two to three minutes of setup. Runner speed
varies between rounds by up to a third (the same serial job took 1580 s and 1195 s).

| Job | Before (PR #416, serial) | Round 1 (1 worker) | Round 2 (2 workers) |
|---|---|---|---|
| `pytest / pytest (3.11)` (required) | 17.7 min job | 852 s | 697 s |
| `pytest / pytest (3.12)` (required) | 12.2 min job | 1071 s | 669 s |
| `pytest / pytest (3.13)` (required) | 13.8 min job | 799 s | 835 s |
| `test (3.13)`, coverage | 20.7 min job (C tracer) | 737 s (`sysmon`) | 848 s (`sysmon`) |
| `test (3.11)` / `test (3.12)`, coverage | 25.2 / 28.8 min jobs | not run on a pull request | not run on a pull request |

A round (push to the last test check): 30 minutes on PR #416, 16 minutes on round 2 of this
pull request. Most of the gain is the coverage jobs: one job instead of three, and
`sysmon` halves the 3.13 job (serial, C tracer: 1580 s and 1195 s; parallel, `sysmon`:
737 s and 848 s). Two workers on one physical core gain 15 to 35 % on the required check.
A public repository's runners have four logical CPUs, so the gain grows there.

Coverage, measured once on this pull request against a temporary serial job on Python 3.13
with the C tracer (today's configuration): 3,306 of 27,404 statements missed serially and
3,307 in the parallel `sysmon` job, both 88 %. The one line is
`src/orcapod/hashing/cache_population.py:393`, a timing-dependent branch; no file differs
otherwise.

The pull-request job on Python 3.11 with the C tracer instead of 3.13 with `sysmon`
(temporary job): 1256 s (round 1, one worker) and 1849 s (round 2, two workers) of test time, against
737 s and 848 s; about twice as long, 21 to 31 minutes per round instead of 12 to 14.

ITL-1192 (`test_second_interrupt_returns_and_a_late_function_writes_nothing[delta]`, the
warning naming nodes `stuck`, `after`) failed once in round 2, in the temporary 3.11 job
(two workers with the C tracer, the most loaded job); it failed before this change too,
serially, on 3.13. More load per CPU makes the race more likely; the fix is ITL-1192.

## Check names

A pull request reports, while the organization ruleset applies (private repository):

- Python — Required CI: `resolve-matrix / resolve`, `pytest / pytest (3.11)`,
  `pytest / pytest (3.12)`, `pytest / pytest (3.13)`, `ruff / ruff`,
  `typecheck / typecheck`, `license-check / license-check` (the required ones).
- Run Tests: `license-check / License check`, `Dependency review`, `test (3.13)`,
  `spiral-integration` (skipped on a pull request).
- Lint and type check: `Ruff and basedpyright` (new here; skipped before this change).
- Run Objective Tests: `test (3.11)`, `test (3.12)`, `test (3.13)`.
- Run PostgreSQL Tests: `test (3.11)`, `test (3.12)`, `test (3.13)`.
- `codecov/patch`, `codecov/project`, `CodeRabbit`.

Before this change the pull request also reported Run Tests `test (3.11)` and
`test (3.12)`. After the repository is public (ITL-1193) the Python — Required CI checks are
gone and the rest remain; the developer then chooses which to require. Tests then run on a
pull request on Python 3.13 only (`test (3.13)`), and a failure only 3.11 or 3.12 shows is
first seen on `main`.

## Reserve plan: a full-only list (not implemented)

If a pull-request round is still too slow, the slowest test files can run only on pushes to
`main` and in the Release workflow, kept out of the pull-request job by one list. The 19
slowest files that protect nothing CI must always check hold 55 % of the test time (JUnit
reports of PR #416's required check, all three versions,
`docs/dev/scripts/itl_1181_per_file_times.py`):

| # | File | Seconds per run | Share |
|---|---|---|---|
| 1 | `tests/test_core/alignment/test_nodes.py` | 85.2 | 10.7 % |
| 2 | `tests/test_pipeline/test_async_fan_out_does_not_hang.py` | 54.9 | 6.9 % |
| 3 | `tests/test_user_types/test_workshop_scenarios.py` | 39.9 | 5.0 % |
| 4 | `tests/test_pipeline/test_zero_row_column_order.py` | 38.3 | 4.8 % |
| 5 | `tests/test_core/alignment/test_operators.py` | 24.3 | 3.1 % |
| 6 | `tests/test_pipeline/test_loaded_slot_unregistered_class.py` | 22.3 | 2.8 % |
| 7 | `tests/test_databases/test_delta_exit.py` | 21.1 | 2.6 % |
| 8 | `tests/test_torch/test_tensor_conversion_warning.py` | 17.4 | 2.2 % |
| 9 | `tests/test_logical_types/test_set_of_primitives.py` | 16.3 | 2.0 % |
| 10 | `tests/test_core/alignment/test_streams.py` | 15.9 | 2.0 % |
| 11 | `tests/test_core/operators/test_large_normalization.py` | 15.4 | 1.9 % |
| 12 | `tests/test_pipeline/test_job_store_scaling.py` | 14.9 | 1.9 % |
| 13 | `tests/test_pipeline/test_job_crash_recovery.py` | 13.2 | 1.7 % |
| 14 | `tests/test_hashing/test_pandas_storage_matrix.py` | 11.8 | 1.5 % |
| 15 | `tests/test_torch/test_tensor_cached_rerun.py` | 10.6 | 1.3 % |
| 16 | `tests/test_torch/test_tensor_hash_stability.py` | 10.2 | 1.3 % |
| 17 | `tests/test_pipeline/test_job_batched_commits.py` | 10.1 | 1.3 % |
| 18 | `tests/test_core/function_pod/test_output_conformance.py` | 10.0 | 1.3 % |
| 19 | `tests/test_pipeline/test_node_head_sorting.py` | 9.6 | 1.2 % |

The protected files are never on the list: the golden hash tests, the hash label rule, the
hash site inventory, the documentation samples and build, and the changelog and
design-issue checks. Implementing it means one list (a marker applied by `conftest.py` from
the list, deselected with `-m "not full_only"` in the pull-request job), and it cannot reach
the organization's required check, which runs `uv run pytest tests` unchanged. A file
on the list would be checked on a pull request only by that required check, while it
applies.
