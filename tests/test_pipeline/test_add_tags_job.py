"""``add_tags`` in a ``PipelineJob``: runs, stores, and a saved job restores it (ITL-1196)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa

from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import PipelineJob


def _readings() -> ArrowTableSource:
    """Tag ``subject``; data ``x``."""
    table = pa.table(
        {
            "subject": pa.array(["a", "b"], pa.large_string()),
            "x": pa.array([1, 2], pa.int64()),
        },
        schema=pa.schema(
            [
                pa.field("subject", pa.large_string(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["subject"], tag_only_columns=True, source_id="readings"
    )


def _job(database: InMemoryArrowDatabase) -> PipelineJob:
    job = PipelineJob(name="labelled", database=database)
    with job:
        _readings().add_tags(cohort="A", run=3, label="labelled")
    return job


def test_job_runs_the_operator() -> None:
    job = _job(InMemoryArrowDatabase())
    _ = job.run()
    table = job.nodes["labelled"].as_table()
    assert table.column("cohort").to_pylist() == ["A", "A"]
    assert table.column("run").to_pylist() == [3, 3]
    assert job.nodes["labelled"].output_schema()[0] == {
        "subject": str,
        "cohort": str,
        "run": int,
    }


def test_job_save_and_load_round_trips_the_operator(tmp_path: Path) -> None:
    database = InMemoryArrowDatabase()
    job = _job(database)
    _ = job.run()
    path = tmp_path / "job.json"
    job.save(str(path))

    saved = [
        node["operator_config"]
        for node in json.loads(path.read_text())["pipeline"]["nodes"].values()
        if node.get("operator_config")
    ]
    assert [c["class_name"] for c in saved] == ["AddTags"]
    assert saved[0]["config"] == {
        "tags": [
            {"name": "cohort", "type": "str", "value": "A"},
            {"name": "run", "type": "int", "value": 3},
        ]
    }

    loaded = PipelineJob.load(str(path), database=database)
    node = loaded.nodes["labelled"]
    assert node.output_schema() == (
        {"subject": str, "cohort": str, "run": int},
        {"x": int},
    )
    assert node.content_hash() == job.nodes["labelled"].content_hash()
