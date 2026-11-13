"""A completed no-op job rerun does not invalidate external lazy readers."""

from __future__ import annotations

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes import SourceJobNode
from orcapod.core.function_pod import FunctionPodStream
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.operators import GroupBy
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DictSource

READS: list[int] = []


@function_pod(output_keys="x")
def twice(x: int) -> int:
    """Transform source values before grouping."""
    return 2 * x


@function_pod(output_keys="total")
def total(x: list[int]) -> int:
    """Reduce a grouped row."""
    return sum(x)


@function_pod(output_keys="read")
def read_total(total: int) -> int:
    """Record actual lazy-reader computations."""
    READS.append(total)
    return total


def make_job(source=None, function=twice):
    """Build a source/function/operator/function chain."""
    source = source or DictSource(
        [{"g": 1, "x": 2}, {"g": 1, "x": 3}], tag_columns=["g"], source_id="rows"
    )
    job = PipelineJob(name="freshness", store=InMemoryArrowDatabase())
    with job:
        doubled = function.pod(source, label="doubled")
        grouped = GroupBy(by=["g"])(doubled, label="grouped")
        total.pod(grouped, label="total")
    return job


@pytest.mark.parametrize("first", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator])
@pytest.mark.parametrize(
    "second", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_noop_rerun_preserves_node_times_and_lazy_reader(first, second) -> None:
    """Proven cache hits retain completed timestamps across orchestrator changes."""
    job = make_job()
    before_first = {name: node.last_modified for name, node in job.nodes.items()}
    job.run(orchestrator=first())
    after_first = {name: node.last_modified for name, node in job.nodes.items()}
    assert after_first["total"] > before_first["total"]
    reader = read_total.pod(job.nodes["total"])
    assert isinstance(reader, FunctionPodStream)
    assert [data.as_dict()["read"] for _, data in reader.iter_data()] == [10]
    assert not reader.is_stale
    READS.clear()

    summary = job.run(orchestrator=second())

    assert {name: node.last_modified for name, node in job.nodes.items()} == after_first
    assert not reader.is_stale
    assert [data.as_dict()["read"] for _, data in reader.iter_data()] == [10]
    assert READS == []
    assert summary.nodes["doubled"].reused == 2
    assert summary.nodes["total"].reused == 1


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_changed_source_advances_time_and_invalidates_lazy_reader(orchestrator) -> None:
    """A new binding changes freshness even if its source id is unchanged."""
    job = make_job()
    job.run(orchestrator=orchestrator())
    reader = read_total.pod(job.nodes["total"])
    assert isinstance(reader, FunctionPodStream)
    list(reader.iter_data())
    before = job.nodes["total"].last_modified
    source = next(
        node for node in job.nodes.values() if isinstance(node, SourceJobNode)
    )
    source_time = source.last_modified
    source.bound_source = DictSource(
        [{"g": 1, "x": 9}], tag_columns=["g"], source_id="rows"
    )
    rebound_time = source.last_modified
    assert rebound_time is not None and source_time is not None
    assert rebound_time > source_time
    assert job.nodes["doubled"].is_stale
    job.run(orchestrator=orchestrator())
    assert job.nodes["total"].last_modified > before
    assert reader.is_stale
    READS.clear()
    list(reader.iter_data())
    assert 18 in READS


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_unknown_source_rerun_remains_conservatively_freshened(orchestrator) -> None:
    """A stable-looking timestamp never proves a custom source is unchanged."""

    class UnknownSource(DictSource):
        pass

    job = make_job(UnknownSource([{"g": 1, "x": 2}], tag_columns=["g"]))
    job.run(orchestrator=orchestrator())
    before = job.nodes["total"].last_modified
    job.run(orchestrator=orchestrator())
    assert job.nodes["total"].last_modified > before


def test_source_node_follows_bound_source_timestamp() -> None:
    """Unknown bound freshness remains unknown, including an unbound source slot."""
    source = DictSource([{"x": 1}], source_id="rows")
    node = SourceJobNode(name="rows", bound_source=source)
    source._set_modified_time(None)
    assert node.last_modified is None
    node.bound_source = None
    assert node.last_modified is None


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_failed_warm_run_cannot_keep_noop_freshness(orchestrator) -> None:
    """An interrupted invocation loses completion, even when its rows were cache hits."""
    from orcapod.pipeline.observer import NoOpObserver

    job = make_job()
    job.run(orchestrator=orchestrator())
    before = job.nodes["doubled"].last_modified

    class Interrupt(NoOpObserver):
        def on_data_start(self, node_label, tag, data):
            if node_label == "doubled":
                raise ValueError("interrupted cached read")

    with pytest.raises((ValueError, ExceptionGroup)):
        job.run(orchestrator=orchestrator(), observer=Interrupt())
    assert job.nodes["doubled"]._completed_execution is None
    job.run(orchestrator=orchestrator())
    assert job.nodes["doubled"].last_modified > before


@function_pod(output_keys="x")
def disabled(x: int) -> int:
    """A pod deactivated in the empty-output regression."""
    return x


@pytest.mark.parametrize("first", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator])
@pytest.mark.parametrize(
    "second", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_completed_empty_outputs_keep_timestamp(first, second) -> None:
    """A successfully empty result is complete, rather than perpetually modified."""
    data_function = disabled.pod.data_function
    assert isinstance(data_function, PythonDataFunction)
    data_function.set_active(False)
    try:
        job = make_job(function=disabled)
        job.run(orchestrator=first())
        before = {name: node.last_modified for name, node in job.nodes.items()}
        job.run(orchestrator=second())
        assert job.nodes["total"].as_table().num_rows == 0
        assert {name: node.last_modified for name, node in job.nodes.items()} == before
    finally:
        data_function.set_active(True)
