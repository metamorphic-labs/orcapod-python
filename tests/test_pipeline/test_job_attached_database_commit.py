"""A run commits the database objects its nodes actually write to (ITL-1383)."""

from __future__ import annotations

from pathlib import Path

import deltalake
import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import GroupBy, SelectDataColumns
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import OrcapodDatabaseWriteError
from orcapod.invocation import InvocationContext
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
from orcapod.side_effects import SideEffectPod
from orcapod.sources import ArrowTableSource
from orcapod.types import CacheMode, NodeConfig

JOB_RUNNERS = ("sync", "async", "run_async")
RUNNERS = (*JOB_RUNNERS, "node")


@pytest.fixture(params=["tags_first", "data_first"])
def source(request: pytest.FixtureRequest) -> ArrowTableSource:
    """Give the same input in two physical layouts and tag orders."""
    schema = pa.schema(
        [
            pa.field("group", pa.int64(), nullable=False),
            pa.field("site", pa.string(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
            pa.field("y", pa.int64(), nullable=False),
        ]
    )
    table = pa.table(
        {"group": [0, 1], "site": ["a", "b"], "x": [1, 2], "y": [3, 4]}, schema=schema
    )
    tags = ["group", "site"]
    if request.param == "data_first":
        table = table.select(["y", "x", "site", "group"])
        tags.reverse()
    return ArrowTableSource(
        table, tag_columns=tags, tag_only_columns=True, source_id="source"
    )


@function_pod(output_keys="total")
def total(x: int, y: int) -> int:
    """Add the two inputs."""
    return x + y


async def _run(
    job: PipelineJob,
    runner: str,
    *,
    load_only: bool = False,
    observer: ExecutionObserverProtocol | None = None,
) -> None:
    """Run the job or its operator node via the selected entry point."""
    if runner == "node":
        job.nodes["grouped"].run(load_only=load_only)
    elif runner == "run_async":
        await job.run_async(load_only=load_only, observer=observer)
    else:
        orchestrator = AsyncPipelineOrchestrator() if runner == "async" else None
        job.run(orchestrator=orchestrator, load_only=load_only, observer=observer)


def _make_job(
    store: DeltaTableDatabase, attached: DeltaTableDatabase, source: ArrowTableSource
) -> PipelineJob:
    """Route an operator's log to an independently constructed database."""
    job = PipelineJob(name="job", database=store)
    with job:
        selected = SelectDataColumns(list(source.output_schema()[1].keys()))(
            source, label="other"
        )
        GroupBy(by=list(source.output_schema()[0].keys()))(selected, label="grouped")
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
    tmp_path: Path, runner: str, source: ArrowTableSource
) -> None:
    """Read the two logged operator rows from disk as soon as the run returns."""
    job = _make_job(
        DeltaTableDatabase(tmp_path), DeltaTableDatabase(tmp_path).at("mine"), source
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
    tmp_path: Path, source: ArrowTableSource, *, asynchronous: bool
) -> None:
    """Function nodes already commit both their result and pipeline tables."""
    job = PipelineJob(name="job", database=DeltaTableDatabase(tmp_path))
    with job:
        total.pod(source, label="sum")
    job.nodes["sum"].attach_databases(
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
    tmp_path: Path, runner: str, source: ArrowTableSource
) -> None:
    """A buffered write from outside the job stays pending during load-only runs."""
    attached = DeltaTableDatabase(tmp_path).at("mine")
    job = _make_job(DeltaTableDatabase(tmp_path), attached, source)
    attached.add_record(("pending",), b"pending", pa.table({"x": [1]}))
    await _run(job, runner, load_only=True)
    assert job.nodes["grouped"].as_table().num_rows == 2
    assert not list(tmp_path.rglob("_delta_log"))


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", RUNNERS)
async def test_a_failed_attached_flush_clears_only_the_nodes_that_ran(
    tmp_path: Path,
    runner: str,
    source: ArrowTableSource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The new run loses its rows and an already current node keeps its rows."""
    attached = DeltaTableDatabase(tmp_path).at("mine")
    job = _make_job(DeltaTableDatabase(tmp_path), attached, source)
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


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", JOB_RUNNERS)
async def test_reattaching_an_operator_flushes_the_store_that_received_its_rows(
    tmp_path: Path,
    runner: str,
    source: ArrowTableSource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A run-end observer can change the node's attachment after it writes."""
    store = DeltaTableDatabase(tmp_path / "owned")
    attached = DeltaTableDatabase(tmp_path / "attached")
    job = _make_job(store, attached, source)
    replacement = DeltaTableDatabase(tmp_path / "unused")
    owned_view = store.at("job")
    job.nodes["other"].attach_databases(
        pipeline_database=owned_view, cache_mode=CacheMode.LOG
    )

    def flush_unused_store() -> None:
        pytest.fail("The replacement store received no write")

    def flush_owned_view() -> None:
        pytest.fail("The scheduled flush already covers this view")

    monkeypatch.setattr(replacement, "flush", flush_unused_store)
    monkeypatch.setattr(owned_view, "flush", flush_owned_view)

    class Reattach(NoOpObserver):
        def on_run_end(self, run_id: str) -> None:
            job.nodes["grouped"].attach_databases(
                pipeline_database=replacement, cache_mode=CacheMode.LOG
            )
            job.nodes["other"].attach_databases(
                pipeline_database=replacement, cache_mode=CacheMode.LOG
            )

    await _run(job, runner, observer=Reattach())
    for store_path in (tmp_path / "owned", tmp_path / "attached"):
        (log,) = store_path.rglob("_delta_log")
        assert deltalake.DeltaTable(str(log.parent)).to_pyarrow_table().num_rows == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", JOB_RUNNERS)
async def test_execution_error_still_flushes_a_manually_attached_store(
    tmp_path: Path, runner: str, source: ArrowTableSource
) -> None:
    """An observer exception propagates after the completed operator log commits."""
    attached = DeltaTableDatabase(tmp_path / "attached")
    job = _make_job(DeltaTableDatabase(tmp_path / "owned"), attached, source)

    class FailRun(NoOpObserver):
        def on_run_end(self, run_id: str) -> None:
            raise RuntimeError("observer failed")

    with pytest.raises(RuntimeError, match="observer failed"):
        await _run(job, runner, observer=FailRun())
    (log,) = (tmp_path / "attached").rglob("_delta_log")
    assert deltalake.DeltaTable(str(log.parent)).to_pyarrow_table().num_rows == 2
    assert job.nodes["grouped"].as_table().num_rows == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", JOB_RUNNERS)
async def test_persistent_cache_hits_leave_the_unused_ephemeral_store_alone(
    tmp_path: Path,
    runner: str,
    source: ArrowTableSource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A persistent hit leaves even another pending table in the ephemeral store alone."""
    job = PipelineJob(name="job", database=InMemoryArrowDatabase())
    with job:
        total.pod(source, label="sum")
    await _run(job, runner)
    node = job.nodes["sum"]
    node.clear_cache()

    ephemeral = DeltaTableDatabase(tmp_path)
    ephemeral.add_record(("pending",), b"pending", pa.table({"value": [3]}))
    job.set_ephemeral_database(ephemeral)
    job.apply_node_config(NodeConfig(is_result_ephemeral=True))

    def fail() -> None:
        raise OSError("the unused store cannot commit")

    monkeypatch.setattr(ephemeral, "flush", fail)
    await _run(job, runner)
    assert node.as_table().num_rows == 2
    assert not (tmp_path / "pending" / "_delta_log").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", JOB_RUNNERS)
async def test_owned_pipeline_flush_precedes_a_failing_split_result_flush(
    tmp_path: Path,
    runner: str,
    source: ArrowTableSource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ready pipeline write survives a failed commit of the split result store."""
    pipeline = DeltaTableDatabase(tmp_path / "pipeline")
    result = DeltaTableDatabase(tmp_path / "result")
    job = PipelineJob(name="job", database=pipeline, result_database=result)
    with job:
        total.pod(source, label="sum")

    def fail() -> None:
        raise OSError("result commit failed")

    class QueueReadyWrite(NoOpObserver):
        def on_run_end(self, run_id: str) -> None:
            pipeline.at("job").add_record(
                ("ready",), b"ready", pa.table({"value": [3]})
            )
            monkeypatch.setattr(result, "flush", fail)

    with pytest.raises(OrcapodDatabaseWriteError) as error:
        await _run(job, runner, observer=QueueReadyWrite())
    assert isinstance(error.value.__cause__, OSError)
    assert (tmp_path / "pipeline" / "job" / "ready" / "_delta_log").is_dir()


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", JOB_RUNNERS)
async def test_logging_nodes_sharing_one_database_flush_it_once(
    tmp_path: Path,
    runner: str,
    source: ArrowTableSource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both logs are durable after one shared flush following the owned flush."""
    store = DeltaTableDatabase(tmp_path / "owned")
    attached = DeltaTableDatabase(tmp_path / "attached")
    job = _make_job(store, attached, source)
    job.nodes["other"].attach_databases(
        pipeline_database=attached, cache_mode=CacheMode.LOG
    )
    flush = attached.flush
    calls = 0

    def count_flush() -> None:
        nonlocal calls
        assert (tmp_path / "owned" / "job" / "ready" / "_delta_log").is_dir()
        calls += 1
        flush()

    class QueueReadyWrite(NoOpObserver):
        def on_run_end(self, run_id: str) -> None:
            store.at("job").add_record(("ready",), b"ready", pa.table({"value": [3]}))

    monkeypatch.setattr(attached, "flush", count_flush)
    await _run(job, runner, observer=QueueReadyWrite())
    assert calls == 1
    logs = list((tmp_path / "attached").rglob("_delta_log"))
    assert len(logs) == 2
    assert all(
        deltalake.DeltaTable(str(log.parent)).to_pyarrow_table().num_rows == 2
        for log in logs
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", JOB_RUNNERS)
async def test_duplicate_operator_logs_do_not_flush_an_attached_store(
    tmp_path: Path, runner: str, source: ArrowTableSource
) -> None:
    """An all-duplicate insert leaves unrelated pending writes alone on a rerun."""
    attached = DeltaTableDatabase(tmp_path / "attached")
    job = _make_job(DeltaTableDatabase(tmp_path / "owned"), attached, source)
    await _run(job, runner)
    for label in ("other", "grouped"):
        job.nodes[label].clear_cache()

    attached.add_record(("pending",), b"pending", pa.table({"value": [3]}))
    await _run(job, runner)
    assert job.nodes["grouped"].as_table().num_rows == 2
    assert not (tmp_path / "attached" / "pending" / "_delta_log").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", JOB_RUNNERS)
async def test_a_side_effect_log_is_committed_to_its_attachment(
    tmp_path: Path, runner: str, source: ArrowTableSource
) -> None:
    """A node that records delivery completion also enrolls its attached store."""
    job = PipelineJob(name="job", database=InMemoryArrowDatabase())

    def deliver(ctx: InvocationContext, x: int, y: int) -> None:
        """Successfully deliver the row."""

    with job:
        _ = SideEffectPod(deliver)(source, label="delivery")
    job.nodes["delivery"].attach_databases(DeltaTableDatabase(tmp_path))

    await _run(job, runner)
    (log,) = tmp_path.rglob("_delta_log")
    assert deltalake.DeltaTable(str(log.parent)).to_pyarrow_table().num_rows == 2
