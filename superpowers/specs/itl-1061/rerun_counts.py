"""Count the work of a first and a second run of one workshop-shaped pipeline (ITL-1061).

Usage (one mode per process; run them one after the other, never in parallel):

    uv run python superpowers/specs/itl-1061/rerun_counts.py lazy
    uv run python superpowers/specs/itl-1061/rerun_counts.py job-sync
    uv run python superpowers/specs/itl-1061/rerun_counts.py job-async
    uv run python superpowers/specs/itl-1061/rerun_counts.py spiral-sync
    uv run python superpowers/specs/itl-1061/rerun_counts.py spiral-async
    uv run python superpowers/specs/itl-1061/rerun_counts.py delta <dir>   # twice: new process

The pipeline has the workshop's shape: two sources (sessions: 2,000 rows; experiments:
6,000 rows, some ``total`` null), ``drop_nulls`` -> ``group_by`` -> ``map_tags`` on the
experiments, ``join`` with the sessions, then two function pods. ``ArrowTableSource`` by
default; the ``spiral-*`` modes read the same tables through ``SpiralDBTableSource`` over the
repository's disk-backed pyspiral fake (``tests/test_databases/fake_pyspiral.py``).

Each run prints a table of counters per node (see ``counters.py``). Nothing under ``src/``
is changed: the counters are monkeypatched in at start.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import counters  # noqa: E402  # pyright: ignore[reportImplicitRelativeImport]  # a sibling script, run from this folder's path
import pyarrow as pa  # noqa: E402

N_SUBJECTS = 200
N_SESSIONS = 10  # per subject: 2,000 session rows
N_EXPERIMENTS = 3  # per session: 6,000 experiment rows
PROJECT = "itl1061"


def tables() -> tuple[pa.Table, pa.Table]:
    """Return the sessions and experiments tables."""
    subj, sess, booth = [], [], []
    e_subj, e_sess, e_idx, correct, total = [], [], [], [], []
    for s in range(N_SUBJECTS):
        for k in range(N_SESSIONS):
            subj.append(f"m{s:04d}")
            sess.append(k)
            booth.append(f"b{(s + k) % 7}")
            for e in range(N_EXPERIMENTS):
                e_subj.append(f"m{s:04d}")
                e_sess.append(k)
                e_idx.append(e)
                correct.append((s + k + e) % 10)
                total.append(None if (s + k + e) % 13 == 0 else 10)
    sessions = pa.table(
        {
            "subject_id": pa.array(subj, pa.large_string()),
            "session": pa.array(sess, pa.int64()),
            "booth": pa.array(booth, pa.large_string()),
        }
    )
    experiments = pa.table(
        {
            "subject_id": pa.array(e_subj, pa.large_string()),
            "session": pa.array(e_sess, pa.int64()),
            "experiment_index": pa.array(e_idx, pa.int64()),
            "correct": pa.array(correct, pa.int64()),
            "total": pa.array(total, pa.int64()),
        }
    )
    return sessions, experiments


def make_pods():
    """Return the two function pods; each body counts its calls."""
    from orcapod import function_pod

    @function_pod(output_keys="accuracy")
    def accuracy(correct: list[int], total: list[int]) -> float:
        counters.bump("fn.call")
        return sum(correct) / sum(total)

    @function_pod(output_keys="grade")
    def grade(accuracy: float) -> str:
        counters.bump("fn.call")
        return "pass" if accuracy >= 0.5 else "fail"

    return accuracy, grade


def arrow_sources():
    """Return the two sources as ``ArrowTableSource``s with fixed ids."""
    from orcapod.sources import ArrowTableSource

    sessions, experiments = tables()
    return (
        ArrowTableSource(
            sessions, tag_columns=["subject_id", "session"], source_id="sessions"
        ),
        ArrowTableSource(
            experiments,
            tag_columns=["subject_id", "session", "experiment_index"],
            source_id="experiments",
        ),
    )


def spiral_sources(root: Path):
    """Write both tables to a pyspiral fake under ``root`` and return Spiral sources."""
    from orcapod.sources import SpiralDBTableSource
    from tests.test_databases import fake_pyspiral

    sessions, experiments = tables()
    # The fake hands out 2-row batches by default; SpiralDB's follow its stored
    # fragments. 1,024 rows keeps per-batch counts from swamping per-row ones.
    fake_pyspiral.scan_batch_rows = 1024
    project = fake_pyspiral.FakeSpiral().project(PROJECT)
    s = project.create_table(
        "default.sessions", key_schema=sessions.select(["subject_id", "session"]).schema
    )
    s.write(sessions)
    e = project.create_table(
        "default.experiments",
        key_schema=experiments.select(
            ["subject_id", "session", "experiment_index"]
        ).schema,
    )
    e.write(experiments)
    return (
        SpiralDBTableSource(PROJECT, "sessions", source_id="sessions"),
        SpiralDBTableSource(PROJECT, "experiments", source_id="experiments"),
    )


def build(sessions, experiments, pods):
    """Apply the workshop-shaped steps; return the last stream."""
    accuracy, grade = pods
    exp = experiments.drop_nulls(["correct", "total"], label="drop_nulls")
    grouped = exp.group_by(["subject_id", "session"], label="group_by")
    mapped = grouped.map_tags(
        {"experiment_index": "experiment_indices"}, label="map_tags"
    )
    joined = sessions.join(mapped, label="join")
    acc = accuracy.pod(joined, label="accuracy")
    return grade.pod(acc, label="grade")


def run_counted(title: str, action) -> None:
    """Run ``action`` with fresh counters and print the table."""
    counters.reset()
    action()
    print(counters.report(title, counters.snapshot()), flush=True)


def mode_lazy(sources) -> None:
    """Lazy streams, no job: read the last stream twice (as_table, iter_data, head)."""
    pods = make_pods()
    out = build(sources[0], sources[1], pods)
    run_counted("lazy: first as_table()", lambda: out.as_table())
    run_counted("lazy: second as_table()", lambda: out.as_table())
    run_counted("lazy: list(iter_data())", lambda: list(out.iter_data()))
    run_counted("lazy: head()", lambda: out.head())
    # A second chain built over the same source objects (what a notebook re-run does).
    out2 = build(sources[0], sources[1], pods)
    run_counted("lazy: same steps rebuilt, as_table()", lambda: out2.as_table())


def mode_job(sources, orchestrator_factory, store=None, runs: int = 2) -> None:
    """PipelineJob.run() ``runs`` times on one job, then once on a new job, same store."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase

    store = store if store is not None else InMemoryArrowDatabase()
    pods = make_pods()

    def new_job():
        job = PipelineJob(name="ws", store=store)
        with job:
            build(sources[0], sources[1], pods)
        return job

    job = new_job()
    for i in range(runs):
        run_counted(
            f"job.run() #{i + 1}, same job object",
            lambda: job.run(orchestrator=orchestrator_factory()),
        )
    run_counted(
        "reading job.nodes['grade'].as_table() after the runs",
        lambda: job.nodes["grade"].as_table(),
    )
    job2 = new_job()
    run_counted(
        "job.run() on a NEW job object, same store",
        lambda: job2.run(orchestrator=orchestrator_factory()),
    )


def main() -> None:
    mode = sys.argv[1]
    counters.install()
    from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
    from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

    if mode == "lazy":
        mode_lazy(arrow_sources())
    elif mode == "job-sync":
        mode_job(arrow_sources(), SyncPipelineOrchestrator)
    elif mode == "job-async":
        mode_job(arrow_sources(), AsyncPipelineOrchestrator)
    elif mode in ("spiral-sync", "spiral-async", "spiral-lazy"):
        from tests.test_databases import fake_pyspiral

        with tempfile.TemporaryDirectory() as tmp, fake_pyspiral.install(Path(tmp)):
            sources = spiral_sources(Path(tmp))
            if mode == "spiral-lazy":
                mode_lazy(sources)
            else:
                orch = (
                    SyncPipelineOrchestrator
                    if mode == "spiral-sync"
                    else AsyncPipelineOrchestrator
                )
                mode_job(sources, orch)
    elif mode == "delta":
        from orcapod.databases import DeltaTableDatabase

        store = DeltaTableDatabase(sys.argv[2])
        mode_job(arrow_sources(), SyncPipelineOrchestrator, store=store, runs=1)
    else:
        raise SystemExit(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
