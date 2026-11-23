"""Pipeline entries retain the run and recording time of their first write."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import deltalake
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.channels import Channel
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
)
from orcapod.errors import OrcapodDatabaseError, UnstorableColumnError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.sources import DictSource
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION
from orcapod.types import NodeConfig
from tests.test_databases.test_connector_arrow_database import MockDBConnector

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


async def execute_standalone_async(
    node: FunctionJobNode, stream: StreamProtocol
) -> int:
    """Run the standalone node through its channel interface."""
    rows = list(stream.iter_data())
    inputs = Channel[tuple[TagProtocol, DataProtocol]](buffer_size=len(rows))
    outputs = Channel[tuple[TagProtocol, DataProtocol]](buffer_size=len(rows))
    for row in rows:
        await inputs.writer.send(row)
    await inputs.writer.close()
    await node.async_execute(inputs.reader, outputs.writer)
    return len([row async for row in outputs.reader])


def execute_standalone(
    node: FunctionJobNode, stream: StreamProtocol, orchestrator: str
) -> int:
    """Execute over the supplied rows without changing the node's path."""
    if orchestrator == "async":
        return asyncio.run(execute_standalone_async(node, stream))
    return len(list(node.execute(stream).iter_data()))


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
            assert asyncio.run(execute_standalone_async(node, stream)) == 2
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
@pytest.mark.parametrize(
    "variant", ["nonempty", "empty", "wrong-type", "nullable-time", "required-run-id"]
)
def test_pipeline_table_with_incompatible_schema_must_be_rebuilt(
    orchestrator: str, variant: str, tmp_path: Path
) -> None:
    """Refuse an append even when the backend allows schema evolution."""
    store = InMemoryArrowDatabase()
    job, _ = job_and_node(store)
    _ = job.run()
    path = pipeline_path(store)
    current = pipeline_rows(store)
    if variant == "wrong-type":
        legacy = current.set_column(
            current.schema.get_field_index(RECORDED_AT),
            pa.field(RECORDED_AT, pa.int64(), nullable=False),
            pa.array([0, 1], type=pa.int64()),
        )
    elif variant == "nullable-time":
        time = current.schema.field(RECORDED_AT)
        legacy = current.set_column(
            current.schema.get_field_index(RECORDED_AT),
            time.with_nullable(True),
            pa.nulls(current.num_rows, type=time.type),
        )
    elif variant == "required-run-id":
        run_id = current.schema.field(RUN_ID)
        legacy = current.set_column(
            current.schema.get_field_index(RUN_ID),
            run_id.with_nullable(False),
            current[RUN_ID],
        )
    else:
        legacy = current.drop([RUN_ID, RECORDED_AT])
    destination = DeltaTableDatabase(tmp_path, allow_schema_evolution=True)
    destination.add_records(path, legacy, record_id_column=RECORD_ID, flush=True)
    if variant == "empty":
        delta = deltalake.DeltaTable(str(tmp_path.joinpath(*path)))
        _ = delta.delete()
        assert (
            deltalake.DeltaTable(str(tmp_path.joinpath(*path)))
            .to_pyarrow_table()
            .num_rows
            == 0
        )
        destination = DeltaTableDatabase(tmp_path, allow_schema_evolution=True)

    writer, _ = job_and_node(destination, count=3)
    with pytest.raises((OrcapodDatabaseError, ExceptionGroup)) as info:
        _ = writer.run(orchestrator=ORCHESTRATORS[orchestrator]())
    error = info.value
    if isinstance(error, ExceptionGroup):
        (error,) = OrcapodDatabaseError.in_group(error)
    assert "Rebuild the pipeline table" in str(error)
    if variant == "wrong-type":
        assert f"{RECORDED_AT} has type int64, expected timestamp[us, tz=UTC]" in str(
            error
        )
    elif variant == "nullable-time":
        assert f"{RECORDED_AT} is nullable, expected non-nullable" in str(error)
    elif variant == "required-run-id":
        assert f"{RUN_ID} is non-nullable, expected nullable" in str(error)
    else:
        assert RUN_ID in str(error) and RECORDED_AT in str(error)
    if variant == "empty":
        assert destination.get_all_records(path) is None
    else:
        assert pipeline_rows(destination).equals(legacy)
    assert destination.list_table_paths() == [path]


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
@pytest.mark.parametrize("flush", [False, True], ids=["queued", "committed"])
@pytest.mark.parametrize("backend", ["memory", "delta", "connector"])
def test_legacy_schema_is_refused_before_evaluation(
    orchestrator: str,
    flush: bool,
    backend: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An incompatible schema shared across views prevents calls and result writes."""
    seed = InMemoryArrowDatabase()
    pod = summarize.pod
    assert isinstance(pod, FunctionPod)
    stream = source()
    seeder = FunctionJobNode(pod, stream, pipeline_database=seed)
    _ = seeder.run()
    path = pipeline_path(seed)
    legacy = pipeline_rows(seed).drop([RUN_ID, RECORDED_AT])

    root: ArrowDatabaseProtocol
    if backend == "delta":
        root = DeltaTableDatabase(tmp_path)
    elif backend == "connector":
        root = ConnectorArrowDatabase(MockDBConnector())
    else:
        root = InMemoryArrowDatabase()
    root.at("pipeline").add_records(
        path, legacy, record_id_column=RECORD_ID, flush=flush
    )
    pipeline = root.at("pipeline")
    results = InMemoryArrowDatabase()
    writer = FunctionJobNode(
        pod, stream, pipeline_database=pipeline, result_database=results
    )
    data_function = pod.data_function

    def unexpected_call(*args: object, **kwargs: object) -> None:
        """Fail if the schema check lets the function evaluate."""
        pytest.fail("The function evaluated before the pipeline schema was refused")

    monkeypatch.setattr(data_function, "call", unexpected_call)
    monkeypatch.setattr(data_function, "async_call", unexpected_call)
    with pytest.raises((OrcapodDatabaseError, ExceptionGroup)) as info:
        _ = execute_standalone(writer, stream, orchestrator)
    error = info.value
    if isinstance(error, ExceptionGroup):
        (error,) = OrcapodDatabaseError.in_group(error)
    assert "Rebuild the pipeline table" in str(error)
    assert RUN_ID in str(error) and RECORDED_AT in str(error)
    assert results.list_table_paths() == []
    retained = pipeline.get_all_records(path, record_id_column=RECORD_ID)
    assert retained is not None
    assert retained.sort_by([("site", "ascending"), (INDEX, "ascending")]).equals(
        legacy
    )


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_the_same_node_refuses_a_restored_prerelease_schema(
    orchestrator: str, tmp_path: Path
) -> None:
    """Restoring version zero replaces the schema under a live writer."""
    store = InMemoryArrowDatabase()
    job, _ = job_and_node(store)
    _ = job.run()
    path = pipeline_path(store)
    current = pipeline_rows(store)
    legacy = current.drop([RUN_ID, RECORDED_AT])
    destination = DeltaTableDatabase(tmp_path, allow_schema_evolution=True)
    destination.add_records(path, legacy, record_id_column=RECORD_ID, flush=True)

    uri = str(tmp_path.joinpath(*path))
    deltalake.write_deltalake(uri, current, mode="overwrite", schema_mode="overwrite")
    destination = DeltaTableDatabase(tmp_path, allow_schema_evolution=True)
    writer, node = job_and_node(destination)
    _ = writer.run(orchestrator=ORCHESTRATORS[orchestrator]())
    delta = deltalake.DeltaTable(uri)
    _ = delta.restore(0)

    node.clear_cache()
    with pytest.raises((OrcapodDatabaseError, ExceptionGroup)) as info:
        _ = execute_standalone(node, source(3), orchestrator)
    error = info.value
    if isinstance(error, ExceptionGroup):
        (error,) = OrcapodDatabaseError.in_group(error)
    assert "Rebuild the pipeline table" in str(error)
    assert RUN_ID in str(error) and RECORDED_AT in str(error)
    restored = deltalake.DeltaTable(uri).to_pyarrow_table()
    assert restored.num_rows == legacy.num_rows
    assert (
        RUN_ID not in restored.column_names and RECORDED_AT not in restored.column_names
    )


@function_pod(output_keys="total")
def nullable_total(x: int, y: int) -> int | None:
    """Return a null output value for these positive inputs."""
    return x + y if y < 0 else None


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
@pytest.mark.parametrize(
    "produces_output", [False, True], ids=["no-output", "null-value"]
)
def test_pipeline_record_requires_an_output(
    orchestrator: str, produces_output: bool
) -> None:
    """An absent output produces no entry, while a null output value does."""
    store = InMemoryArrowDatabase()
    job = PipelineJob(name="record_metadata", database=store)
    with job:
        _ = nullable_total.pod(source(), label="filtered")
    data_function = nullable_total.pod.data_function
    assert isinstance(data_function, PythonDataFunction)
    data_function.set_active(produces_output)
    try:
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    finally:
        data_function.set_active()
    if produces_output:
        assert pipeline_rows(store).num_rows == 2
    else:
        assert store.list_table_paths() == []


@pytest.mark.parametrize(
    ("orchestrator", "indices"), [("sync", [0]), ("async", [0, 1])]
)
def test_identical_inputs_without_system_tags(
    orchestrator: str, indices: list[int]
) -> None:
    """Sync reuses the first entry, while async can schedule both copies as misses."""
    table = source().as_table().select(["site", "group", "x", "y"]).take([0, 0])
    stream = ArrowTableStream(table, tag_columns=["site", "group"])
    store = InMemoryArrowDatabase()
    pod = summarize.pod
    assert isinstance(pod, FunctionPod)
    node = FunctionJobNode(pod, stream, pipeline_database=store)
    assert execute_standalone(node, stream, orchestrator) == 2
    assert pipeline_rows(store)[INDEX].to_pylist() == indices
