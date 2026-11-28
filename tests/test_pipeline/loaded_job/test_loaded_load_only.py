"""A loaded job is empty until ``loaded.run(load_only=True)`` fills it (ITL-1095).

Developer, November 13, 2026: "the initially loaded job is empty", and a load-only run
fills the nodes from what is stored. A loaded job has no function pods, so it can never
compute; with its sources available (rebuilt from files, or bound), a load-only run reads
them, runs the rebuilt operators, and gives each function node the results its database
holds for those inputs. ``loaded.run()`` without ``load_only`` still raises.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.source_node import SourceJobNode
from orcapod.core.operators import Join
from orcapod.core.streams import ViewStatus
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource, CSVSource, DictSource
from orcapod.types import CacheMode

JOB = "loaded_load_only"


@function_pod(output_keys="dbl")
def double(x: int) -> int:
    """Double *x*."""
    return 2 * x


@function_pod(output_keys="cc")
def increment(c: int) -> int:
    """Add one to *c*."""
    return c + 1


def _dict_source() -> DictSource:
    return DictSource(
        [{"id": 0, "x": 5}, {"id": 1, "x": 10}],
        tag_columns=["id"],
        tag_only_columns=True,
        source_id="dsrc",
    )


def _arrow_source() -> ArrowTableSource:
    table = pa.table(
        {"id": [0, 1], "y": [3, 4]},
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("y", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["id"], tag_only_columns=True, source_id="asrc"
    )


def _save(tmp_path: Path, db: Any, cache_mode: CacheMode | None = None) -> Path:
    """Run and save a job: ``dbl`` over an in-memory source joined with another one
    (``joined``), and ``inc`` over a CSV source."""
    csv = tmp_path / "c.csv"
    csv.write_text("id,c\n0,7\n1,8\n")
    job = PipelineJob(name=JOB, database=db)
    with job:
        Join()(double.pod(_dict_source(), label="dbl"), _arrow_source(), label="joined")
        increment.pod(
            CSVSource(
                str(csv), tag_columns=["id"], tag_only_columns=True, source_id="csrc"
            ),
            label="inc",
        )
    if cache_mode is not None:
        job.nodes["joined"].attach_databases(
            pipeline_database=db.at(JOB), cache_mode=cache_mode
        )
    job.run()
    path = tmp_path / "job.json"
    job.save(path)
    return path


def _snapshot(db: Any) -> dict[tuple[str, ...], int]:
    db.flush()
    counts: dict[tuple[str, ...], int] = {}
    for path in db.list_table_paths():
        records = db.get_all_records(path)
        counts[path] = 0 if records is None else records.num_rows
    return counts


def _bound(path: Path, db: Any) -> PipelineJob:
    loaded = PipelineJob.load(path, database=db)
    loaded.bind(sources={"dsrc": _dict_source(), "asrc": _arrow_source()})
    return loaded


@pytest.fixture
def loaded_csv_job(tmp_path: Path) -> tuple[PipelineJob, CSVSource, CSVSource]:
    """Load a two-slot job saved after one of its CSV sources was replaced."""
    sources = []
    for source_id, value in (("first", 2), ("second", 3), ("replacement", 5)):
        path = tmp_path / f"{source_id}.csv"
        path.write_text(f"id,x\n0,{value}\n")
        sources.append(
            CSVSource(
                str(path),
                tag_columns=["id"],
                tag_only_columns=True,
                source_id=source_id,
            )
        )
    first, second, replacement = sources
    database = InMemoryArrowDatabase()
    job = PipelineJob(name=JOB, database=database)
    with job:
        double.pod(first, label="left")
        double.pod(second, label="right")
    job.run()
    job.bind(sources={"first": replacement})
    job.run()
    saved = tmp_path / "two-slots.json"
    job.save(saved)
    return PipelineJob.load(saved, database=database), first, replacement


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("operation", ["capture", "record", "compile"])
def test_loaded_job_refuses_graph_changes(
    loaded_csv_job: tuple[PipelineJob, CSVSource, CSVSource],
    operation: str,
    orchestrator: str,
) -> None:
    """A refused edit keeps both slots, their bindings, and held results."""
    loaded, first, _ = loaded_csv_job
    held_run = loaded.run(load_only=True)
    nodes = dict(loaded.nodes)
    dag = loaded.dag
    sources = loaded.sources
    bindings = {
        label: node.bound_source
        for label, node in nodes.items()
        if isinstance(node, SourceJobNode)
    }
    assert set(bindings) == {"first", "second"}
    assert nodes["left"].as_table()["dbl"].to_pylist() == [10]
    assert nodes["right"].as_table()["dbl"].to_pylist() == [6]

    with pytest.raises(RuntimeError, match=r"ITL-1560"):
        if operation == "capture":
            with loaded:
                double.pod(first, label="extra")
        elif operation == "record":
            pod = double.pod
            assert isinstance(pod, FunctionPod)
            loaded.record_function_pod_invocation(pod, first, label="extra")
        else:
            loaded.compile()

    assert not loaded.is_active()
    assert loaded.nodes.keys() == nodes.keys()
    assert all(loaded.nodes[label] is node for label, node in nodes.items())
    assert loaded.dag is dag
    assert loaded.sources.keys() == sources.keys()
    assert all(loaded.sources[name] is source for name, source in sources.items())
    for label, binding in bindings.items():
        node = loaded.nodes[label]
        assert isinstance(node, SourceJobNode)
        assert node.bound_source is binding
    assert loaded.last_run is held_run
    assert loaded.nodes["left"].as_table()["dbl"].to_pylist() == [10]
    assert loaded.nodes["right"].as_table()["dbl"].to_pylist() == [6]

    if orchestrator == "sync":
        summary = loaded.run(load_only=True)
    else:
        summary = asyncio.run(loaded.run_async(load_only=True))
    assert summary.load_only
    assert summary.nodes["left"].held == summary.nodes["right"].held == 1
    assert loaded.nodes["left"].as_table()["dbl"].to_pylist() == [10]
    assert loaded.nodes["right"].as_table()["dbl"].to_pylist() == [6]


def test_loaded_job_can_still_bind_sources(
    loaded_csv_job: tuple[PipelineJob, CSVSource, CSVSource],
) -> None:
    """Both binding paths keep the loaded graph usable for load-only runs."""
    loaded, first, replacement = loaded_csv_job
    nodes = dict(loaded.nodes)
    slot = nodes["first"]
    assert isinstance(slot, SourceJobNode)
    loaded.bind(sources={"first": first})
    assert slot.bound_source is loaded.sources["first"] is first
    loaded.run(load_only=True)
    assert loaded.nodes["left"].as_table()["dbl"].to_pylist() == [4]
    assert loaded.nodes["right"].as_table()["dbl"].to_pylist() == [6]

    slot.bound_source = replacement
    assert slot.bound_source is replacement
    loaded.run(load_only=True)
    assert loaded.nodes["left"].as_table()["dbl"].to_pylist() == [10]
    assert loaded.nodes["right"].as_table()["dbl"].to_pylist() == [6]
    assert loaded.nodes.keys() == nodes.keys()
    assert all(loaded.nodes[label] is node for label, node in nodes.items())


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_a_load_only_run_fills_a_loaded_job_and_writes_nothing(
    tmp_path: Path, orchestrator: str
) -> None:
    db = InMemoryArrowDatabase()
    path = _save(tmp_path, db)
    before = _snapshot(db)
    loaded = _bound(path, db)
    for label in ("dbl", "joined", "inc"):
        assert loaded.nodes[label].as_table().num_rows == 0, label
    if orchestrator == "sync":
        summary = loaded.run(load_only=True)
    else:
        summary = asyncio.run(loaded.run_async(load_only=True))
    assert summary.load_only
    assert summary.nodes["dbl"].reused == 2
    assert summary.nodes["inc"].reused == 2
    assert _snapshot(db) == before
    dbl = {r["id"]: r["dbl"] for r in loaded.nodes["dbl"].as_table().to_pylist()}
    assert dbl == {0: 10, 1: 20}
    joined = loaded.nodes["joined"].as_table().to_pylist()
    assert sorted((r["id"], r["dbl"], r["y"]) for r in joined) == [
        (0, 10, 3),
        (1, 20, 4),
    ]
    assert loaded.nodes["dbl"].head().status is ViewStatus.LOADED_ONLY


def test_nodes_below_an_unbound_source_stay_empty(tmp_path: Path) -> None:
    db = InMemoryArrowDatabase()
    loaded = PipelineJob.load(_save(tmp_path, db), database=db)
    summary = loaded.run(load_only=True)
    # The CSV source is rebuilt from its file; the in-memory ones are not saved.
    assert loaded.nodes["inc"].as_table().num_rows == 2
    assert loaded.nodes["dbl"].as_table().num_rows == 0
    assert loaded.nodes["joined"].as_table().num_rows == 0
    assert "dbl" not in summary.nodes


def test_a_loaded_job_still_cannot_compute(tmp_path: Path) -> None:
    db = InMemoryArrowDatabase()
    loaded = _bound(_save(tmp_path, db), db)
    with pytest.raises(RuntimeError, match=r"run\(load_only=True\)"):
        loaded.run()
    with pytest.raises(RuntimeError, match=r"run\(load_only=True\)"):
        loaded.nodes["dbl"].run()


def test_a_loaded_node_runs_load_only_with_what_it_depends_on(tmp_path: Path) -> None:
    db = InMemoryArrowDatabase()
    loaded = _bound(_save(tmp_path, db), db)
    view = loaded.nodes["joined"].run(load_only=True)
    assert view.as_table().num_rows == 2
    assert loaded.nodes["dbl"].as_table().num_rows == 2
    summary = loaded.nodes["joined"].last_run
    assert summary is not None and summary.load_only


def test_a_loaded_replay_operator_replays_its_run_in_a_load_only_run(
    tmp_path: Path,
) -> None:
    db = InMemoryArrowDatabase()
    path = _save(tmp_path, db, cache_mode=CacheMode.LOG)
    loaded = _bound(path, db)
    loaded.nodes["joined"].attach_databases(
        pipeline_database=db.at(JOB), cache_mode=CacheMode.REPLAY
    )
    before = _snapshot(db)
    loaded.run(load_only=True)
    assert _snapshot(db) == before
    assert loaded.nodes["joined"].as_table().num_rows == 2
