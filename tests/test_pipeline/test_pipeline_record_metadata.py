"""Pipeline entries retain the run and recording time of their first write."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.channels import Channel
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import OrcapodDatabaseError, UnstorableColumnError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.sources import DictSource
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION
from orcapod.types import NodeConfig

RUN_ID = "__pipeline_run_id"
RECORDED_AT = "__recorded_at"
INDEX = "__pipeline_recomputation_index"
BASE_ENTRY = "__pipeline_base_entry_id"
RECORD_ID = "__record_id"
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


@function_pod(output_keys=("total", "doubled"))
def summarize(x: int, y: int) -> tuple[int, int]:
    """Compute two columns from two inputs."""
    return x + y, 2 * y


def source(count: int = 2, *, reverse: bool = False) -> DictSource:
    """Build rows with two tags and two data columns."""
    rows = [
        {"site": f"site_{i}", "group": f"group_{i % 2}", "x": i, "y": i + 10}
        for i in range(count)
    ]
    return DictSource(
        [dict(reversed(tuple(row.items()))) for row in rows] if reverse else rows,
        tag_columns=["group", "site"] if reverse else ["site", "group"],
        tag_only_columns=True,
        source_id="record_metadata_source",
    )


def job_and_node(
    store: ArrowDatabaseProtocol,
    *,
    count: int = 2,
    reverse: bool = False,
    ephemeral: InMemoryArrowDatabase | None = None,
) -> tuple[PipelineJob, FunctionJobNode]:
    """Build a fresh job over the same function and pipeline table."""
    job = PipelineJob(name="record_metadata", database=store)
    with job:
        _ = summarize.pod(source(count, reverse=reverse), label="summarize")
    if ephemeral is not None:
        job.set_ephemeral_database(ephemeral)
        job.apply_node_config(NodeConfig(is_result_ephemeral=True))
    node = job.nodes["summarize"]
    assert isinstance(node, FunctionJobNode)
    return job, node


def pipeline_path(store: ArrowDatabaseProtocol) -> tuple[str, ...]:
    """Find this test's one pipeline table."""
    (path,) = [
        path
        for path in store.list_table_paths()
        if path[-1] == PIPELINE_DB_SCHEMA_VERSION
    ]
    return path


def pipeline_rows(store: ArrowDatabaseProtocol) -> pa.Table:
    """Read the pipeline table with stable ordering and its physical row ID."""
    table = store.get_all_records(pipeline_path(store), record_id_column=RECORD_ID)
    assert table is not None
    return table.sort_by([("site", "ascending"), (INDEX, "ascending")])


class RunObserver(NoOpObserver):
    """Capture the same identifier reported to callers at job start."""

    run_id: str | None = None

    def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
        """Keep the ID of the job run."""
        self.run_id = run_id


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
@pytest.mark.parametrize("ephemeral", [False, True], ids=["persistent", "ephemeral"])
def test_job_records_keep_their_original_metadata(
    orchestrator: str, ephemeral: bool
) -> None:
    """Fresh writes, reuses, load-only misses and lost results have distinct behavior."""
    store = InMemoryArrowDatabase()
    results = InMemoryArrowDatabase() if ephemeral else None
    run = ORCHESTRATORS[orchestrator]
    first, node = job_and_node(store, ephemeral=results)
    observer = RunObserver()
    before = datetime.now(timezone.utc)
    _ = first.run(orchestrator=run(), observer=observer)
    after = datetime.now(timezone.utc)
    assert observer.run_id

    original = pipeline_rows(store)
    assert original.schema.field(RUN_ID) == pa.field(RUN_ID, pa.large_string())
    assert original.schema.field(RECORDED_AT) == pa.field(
        RECORDED_AT, pa.timestamp("us", tz="UTC"), nullable=False
    )
    assert original[RUN_ID].to_pylist() == [observer.run_id] * 2
    assert original[INDEX].to_pylist() == [0, 0]
    original_times = cast(list[datetime], original[RECORDED_AT].to_pylist())
    assert all(before <= recorded <= after for recorded in original_times)

    joined = node.get_all_records(columns={"meta": True})
    assert joined is not None
    assert (
        joined.sort_by("site")
        .select([RUN_ID, RECORDED_AT])
        .equals(original.select([RUN_ID, RECORDED_AT]))
    )
    assert sorted(zip(joined["total"].to_pylist(), joined["doubled"].to_pylist())) == [
        (10, 20),
        (12, 22),
    ]
    plain = node.get_all_records()
    assert plain is not None and RUN_ID not in plain.column_names

    second, _ = job_and_node(store, reverse=True, ephemeral=results)
    _ = second.run(orchestrator=run())
    assert pipeline_rows(store).equals(original)

    load, _ = job_and_node(store, count=3, reverse=True, ephemeral=results)
    loaded = load.run(orchestrator=run(), load_only=True)
    assert loaded.nodes["summarize"].reused == 2
    assert pipeline_rows(store).equals(original)

    if ephemeral:
        recompute, _ = job_and_node(store, ephemeral=InMemoryArrowDatabase())
        next_run = RunObserver()
        before_recompute = datetime.now(timezone.utc)
        _ = recompute.run(orchestrator=run(), observer=next_run)
        after_recompute = datetime.now(timezone.utc)
        assert next_run.run_id and next_run.run_id != observer.run_id
        records = pipeline_rows(store)
        assert records.filter(pc.field(INDEX) == 0).equals(original)
        new = records.filter(pc.field(INDEX) == 1)
        assert new[BASE_ENTRY].to_pylist() == original[BASE_ENTRY].to_pylist()
        assert set(new[RECORD_ID].to_pylist()).isdisjoint(
            original[RECORD_ID].to_pylist()
        )
        assert new[RUN_ID].to_pylist() == [next_run.run_id] * 2
        new_times = cast(list[datetime], new[RECORDED_AT].to_pylist())
        assert all(
            before_recompute <= recorded <= after_recompute for recorded in new_times
        )


async def execute_standalone_async(node: FunctionJobNode, stream: DictSource) -> None:
    """Run the standalone node through its channel interface."""
    inputs = Channel[tuple[TagProtocol, DataProtocol]](buffer_size=2)
    outputs = Channel[tuple[TagProtocol, DataProtocol]](buffer_size=2)
    for row in stream.iter_data():
        await inputs.writer.send(row)
    await inputs.writer.close()
    await node.async_execute(inputs.reader, outputs.writer)
    assert len([row async for row in outputs.reader]) == 2


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_standalone_nodes_with_a_separate_result_database(orchestrator: str) -> None:
    """Both newly computed and reused results get a null standalone run ID."""
    results = InMemoryArrowDatabase()
    pod = summarize.pod
    assert isinstance(pod, FunctionPod)
    for computed in (True, False):
        store = InMemoryArrowDatabase()
        stream = source(reverse=not computed)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=store,
            result_database=results,
        )
        if orchestrator == "async":
            asyncio.run(execute_standalone_async(node, stream))
            results.flush()
            store.flush()
        else:
            _ = node.run()

        assert len(results.list_table_paths()) == 1
        table = pipeline_rows(store)
        assert table[RUN_ID].to_pylist() == [None, None]
        assert table[RECORDED_AT].null_count == 0
        assert table["__computed"].to_pylist() == [computed, computed]
        joined = node.get_all_records(all_info=True)
        assert joined is not None and joined[RUN_ID].to_pylist() == [None, None]


@pytest.mark.parametrize("column", [RUN_ID, RECORDED_AT])
def test_attach_checks_pipeline_metadata_support(column: str) -> None:
    """A pipeline database can refuse either field before the node runs."""

    class RejectsColumn(InMemoryArrowDatabase):
        """Reject writes of one pipeline metadata column."""

        def require_storable(self, schema: pa.Schema) -> None:
            """Raise when the column appears in an attached table's schema."""
            if column in schema.names:
                raise UnstorableColumnError(
                    database_kind=type(self).__name__,
                    column=column,
                    arrow_type=schema.field(column).type,
                    reason="unsupported by this test database",
                )

    pod = summarize.pod
    assert isinstance(pod, FunctionPod)
    node = FunctionJobNode(function_pod=pod, input_stream=source())
    with pytest.raises(UnstorableColumnError) as info:
        node.attach_databases(RejectsColumn(), result_database=InMemoryArrowDatabase())
    assert info.value.column == column and info.value.node_label == node.label


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_prerelease_pipeline_table_must_be_rebuilt(
    orchestrator: str, tmp_path: Path
) -> None:
    """Refuse an append even when the backend allows schema evolution."""
    store = InMemoryArrowDatabase()
    job, _ = job_and_node(store)
    _ = job.run()
    path = pipeline_path(store)
    legacy = pipeline_rows(store).drop([RUN_ID, RECORDED_AT])
    destination = DeltaTableDatabase(tmp_path, allow_schema_evolution=True)
    destination.add_records(path, legacy, record_id_column=RECORD_ID, flush=True)

    writer, _ = job_and_node(destination, count=3)
    with pytest.raises((OrcapodDatabaseError, ExceptionGroup)) as info:
        _ = writer.run(orchestrator=ORCHESTRATORS[orchestrator]())
    error = info.value
    if isinstance(error, ExceptionGroup):
        (error,) = OrcapodDatabaseError.in_group(error)
    assert "Rebuild the pipeline table" in str(error)
    assert RUN_ID in str(error) and RECORDED_AT in str(error)
    assert pipeline_rows(destination).equals(legacy)
    assert destination.list_table_paths() == [path]
