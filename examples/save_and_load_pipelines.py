"""Save a pipeline job to a file, load it back, and read its stored results.

A ``PipelineJob`` records the steps of a pipeline, runs them against a store,
and can write a description of itself to a JSON file. Loading that file later
gives a job whose stored results can be inspected without re-running anything
and without the code of the functions being available.

What this script shows:

1. Build and run a job over an on-disk ``DeltaTableDatabase``.
2. ``job.save(path)`` after the run.
3. ``PipelineJob.load(path, store=...)`` in place of the original job, and
   ``get_all_records()`` on a loaded node.
4. What a loaded job cannot do: ``run()`` raises, because the file holds the
   names of the functions, not their code.

Usage:
    uv run python examples/save_and_load_pipelines.py

The script writes ``./local_database/`` and ``./sum_job.json`` in the current
directory. The same material is explained step by step in the documentation
(``docs/concepts/pipeline-job.md``) and the tutorial (``docs/tutorial/``).
"""

from __future__ import annotations

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource

DATABASE_DIR = "./local_database"
JOB_FILE = "./sum_job.json"


@function_pod(output_keys="sum")
def take_sum(x: int, y: int) -> int:
    """Add the two input columns."""
    return x + y


def build_job(store: DeltaTableDatabase) -> PipelineJob:
    """Record the pipeline: join two sources on ``id`` and add their values.

    Args:
        store: The database the job's results are written to.

    Returns:
        The recorded job. Nothing has been computed yet.
    """
    job = PipelineJob(name="sum_demo", store=store)
    with job:
        source1 = DictSource(
            data=[{"id": 0, "x": 5}, {"id": 1, "x": 10}, {"id": 2, "x": 15}, {"id": 3, "x": 20}],
            tag_columns=["id"],
            source_id="source1",
        )
        source2 = DictSource(
            data=[{"id": 0, "y": 3}, {"id": 2, "y": 6}, {"id": 3, "y": 9}, {"id": 4, "y": 12}],
            tag_columns=["id"],
            source_id="source2",
        )
        # Only ids 0, 2 and 3 are in both sources, so the join has three rows.
        take_sum.pod(source1.join(source2), label="sum")
    return job


def main() -> None:
    """Run the example."""
    # 1. Build and run. Every result is written under DATABASE_DIR.
    job = build_job(DeltaTableDatabase(DATABASE_DIR))
    job.run()
    rows = sorted(job.nodes["sum"].as_table().to_pylist(), key=lambda row: row["id"])
    print("computed:", rows)

    # 2. Save a description of the job (its nodes, their connections, its run status).
    job.save(JOB_FILE)
    print("saved job to", JOB_FILE)

    # 3. Load it, as a later session would, and read the stored results.
    loaded = PipelineJob.load(JOB_FILE, store=DeltaTableDatabase(DATABASE_DIR))
    records = loaded.nodes["sum"].get_all_records()
    print(f"loaded job: {records.num_rows} stored results")
    print("stored sums:", sorted(records.column("sum").to_pylist()))

    # 4. A loaded job is read-only: it has the functions' names, not their code.
    try:
        loaded.run()
    except RuntimeError as err:
        print("loaded.run() raised RuntimeError:", str(err).split(":")[0])

    # To compute again, build the job from code. Nothing is recomputed, because
    # the results are already in the store.
    again = build_job(DeltaTableDatabase(DATABASE_DIR))
    again.run()
    print("rebuilt job rows:", again.nodes["sum"].as_table().num_rows)


if __name__ == "__main__":
    main()
