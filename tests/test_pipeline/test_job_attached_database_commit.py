"""A run commits the database objects its nodes actually write to (ITL-1383)."""

from __future__ import annotations

from pathlib import Path

import deltalake
import pytest
import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import GroupBy, SelectDataColumns
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import OrcapodDatabaseWriteError
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import DictSource
from orcapod.types import CacheMode

RUNNERS = ("sync", "async", "run_async", "node")


@function_pod(output_keys="y")
def increment(x: int) -> int:
    """Return the next integer."""
    return x + 1


async def _run(job: PipelineJob, runner: str, *, load_only: bool = False) -> None:
    """Run the job or its operator node via the selected entry point."""
    if runner == "node":
        job.nodes["grouped"].run(load_only=load_only)
    elif runner == "run_async":
        await job.run_async(load_only=load_only)
    else:
        orchestrator = AsyncPipelineOrchestrator() if runner == "async" else None
        job.run(orchestrator=orchestrator, load_only=load_only)


def _make_job(store: DeltaTableDatabase, attached: DeltaTableDatabase) -> PipelineJob:
    """Route an operator's log to an independently constructed database."""
    source = DictSource(
        [{"group": 0, "x": 1}, {"group": 1, "x": 2}],
        tag_columns=["group"],
        tag_only_columns=True,
        source_id="source",
    )
    job = PipelineJob(name="job", database=store)
    with job:
        selected = SelectDataColumns(["x"])(source, label="other")
        GroupBy(by=["group"])(selected, label="grouped")
    job.nodes["other"].attach_databases(
        pipeline_database=store.at("job"), cache_mode=CacheMode.LOG
    )
    job.nodes["grouped"].attach_databases(
        pipeline_database=attached, cache_mode=CacheMode.LOG
    )
    return job


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", RUNNERS)
async def test_a_run_commits_a_manually_attached_operator_log(
    tmp_path: Path, runner: str
) -> None:
    """Read the two logged operator rows from disk as soon as the run returns."""
    job = _make_job(
        DeltaTableDatabase(tmp_path), DeltaTableDatabase(tmp_path).at("mine")
    )
    await _run(job, runner)
    node = job.nodes["grouped"]
    assert node.as_table().num_rows == node.as_source().as_table().num_rows == 2
    for prefix in ("job", "mine"):
        logs = list((tmp_path / prefix).rglob("_delta_log"))
        assert len(logs) == 1
        (log,) = logs
        assert deltalake.DeltaTable(str(log.parent)).to_pyarrow_table().num_rows == 2


@pytest.mark.parametrize("asynchronous", [False, True])
def test_a_manually_attached_function_store_is_committed(
    tmp_path: Path, *, asynchronous: bool
) -> None:
    """Function nodes already commit both their result and pipeline tables."""
    source = DictSource(
        [{"k": 0, "x": 1}, {"k": 1, "x": 2}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="source",
    )
    job = PipelineJob(name="job", database=DeltaTableDatabase(tmp_path))
    with job:
        increment.pod(source, label="inc")
    job.nodes["inc"].attach_databases(
        pipeline_database=DeltaTableDatabase(tmp_path).at("mine")
    )
    orchestrator = AsyncPipelineOrchestrator() if asynchronous else None
    job.run(orchestrator=orchestrator)
    logs = list((tmp_path / "mine").rglob("_delta_log"))
    assert {log.parent.name for log in logs} == {"pdb_v2", "rdb_v1"}
    assert all(
        deltalake.DeltaTable(str(log.parent)).to_pyarrow_table().num_rows == 2
        for log in logs
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", RUNNERS)
async def test_load_only_does_not_commit_a_manually_attached_store(
    tmp_path: Path, runner: str
) -> None:
    """A buffered write from outside the job stays pending during load-only runs."""
    attached = DeltaTableDatabase(tmp_path).at("mine")
    job = _make_job(DeltaTableDatabase(tmp_path), attached)
    attached.add_record(("pending",), b"pending", pa.table({"x": [1]}))
    await _run(job, runner, load_only=True)
    assert job.nodes["grouped"].as_table().num_rows == 2
    assert not list(tmp_path.rglob("_delta_log"))


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", RUNNERS)
async def test_a_failed_attached_flush_clears_only_the_nodes_that_ran(
    tmp_path: Path, runner: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The new run loses its rows and an already current node keeps its rows."""
    attached = DeltaTableDatabase(tmp_path).at("mine")
    job = _make_job(DeltaTableDatabase(tmp_path), attached)
    held = job.nodes["other"]
    held.run()

    def fail() -> None:
        raise OSError("disk full")

    monkeypatch.setattr(attached, "flush", fail)
    with pytest.raises(OrcapodDatabaseWriteError, match="mine") as exc_info:
        await _run(job, runner)
    assert exc_info.value.flush
    assert isinstance(exc_info.value.__cause__, OSError)
    assert job.nodes["grouped"].as_table().num_rows == 0
    assert held.as_table().num_rows == 2
