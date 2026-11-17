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

TBD

## Check names

TBD

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
