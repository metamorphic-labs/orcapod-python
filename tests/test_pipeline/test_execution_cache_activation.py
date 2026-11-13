"""Reactivating a function discards filtered completions, including downstream empties."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.nodes import FunctionJobNode
from orcapod.core.operators import GroupBy
from orcapod.databases import InMemoryArrowDatabase
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DictSource

CALLS: list[int] = []


@function_pod(output_keys="x")
def doubled(x: int) -> int:
    """Record work performed after activation."""
    CALLS.append(x)
    return x * 2


@function_pod(output_keys="total")
def total(x: list[int]) -> int:
    """Sum a group downstream of the toggled function."""
    return sum(x)


def make_job(pod=doubled.pod) -> PipelineJob:
    """Build an activation-sensitive function/operator/function chain."""
    job = PipelineJob(name="activation", store=InMemoryArrowDatabase())
    with job:
        rows = pod(
            DictSource([{"g": 1, "x": 2}, {"g": 1, "x": 3}], tag_columns=["g"]),
            label="doubled",
        )
        grouped = GroupBy(by=["g"])(rows, label="grouped")
        total.pod(grouped, label="total")
    return job


@pytest.fixture
def data_function():
    """Leave the shared pod active after every test, including a failed assertion."""
    function = doubled.pod.data_function
    assert isinstance(function, PythonDataFunction)
    CALLS.clear()
    try:
        yield function
    finally:
        function.set_active(True)


@pytest.mark.parametrize("first", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator])
@pytest.mark.parametrize(
    "second", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_reactivated_rows_and_downstream_empty_results_are_recomputed(
    data_function, first, second
) -> None:
    """A prior inactive invocation cannot suppress work after reactivation."""
    data_function.set_active(False)
    job = make_job()
    job.run(orchestrator=first())
    assert job.nodes["total"].as_table().num_rows == 0
    assert CALLS == []
    data_function.set_active(True)

    summary = job.run(orchestrator=second())

    assert sorted(CALLS) == [2, 3]
    assert summary.nodes["doubled"].computed == 2
    assert job.nodes["total"].as_table().column("total").to_pylist() == [10]
    CALLS.clear()
    with patch.object(
        StarfixArrowHasher, "hash_table", side_effect=AssertionError("rehashed")
    ):
        job.run(orchestrator=second())
    assert CALLS == []


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_unchanged_inactive_state_keeps_filtered_cache(
    data_function, orchestrator
) -> None:
    """An identical set_active call does not force DB reads or invalidate completed rows."""
    data_function.set_active(False)
    job = make_job()
    job.run(orchestrator=orchestrator())
    before = job.nodes["doubled"].last_modified
    data_function.set_active(False)
    with patch.object(
        FunctionJobNode, "_load_cached_entries", side_effect=AssertionError("loaded")
    ):
        summary = job.run(orchestrator=orchestrator())
    assert summary.nodes["doubled"].reused == 2
    assert job.nodes["doubled"].last_modified == before
    assert job.nodes["total"].as_table().num_rows == 0
    assert CALLS == []


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_activation_during_completion_callback_cannot_certify_empty_output(
    data_function, orchestrator
) -> None:
    """The run-start proof notices activation changes before downstream completion."""
    from orcapod.pipeline.observer import NoOpObserver

    class Reactivate(NoOpObserver):
        def on_node_end(self, node_label, node_hash):
            if node_label == "doubled":
                data_function.set_active(True)

    data_function.set_active(False)
    job = make_job()
    job.run(orchestrator=orchestrator(), observer=Reactivate())
    assert job.nodes["doubled"]._completed_execution is None
    assert job.nodes["grouped"]._completed_execution is None
    assert job.nodes["total"].as_table().num_rows == 0
    job.run(orchestrator=orchestrator())
    assert sorted(CALLS) == [2, 3]
    assert job.nodes["total"].as_table().column("total").to_pylist() == [10]


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_activation_changes_preserve_successful_results(
    data_function, orchestrator
) -> None:
    """Only filtered entries are incompatible: positive stored results remain reusable."""
    job = make_job()
    job.run(orchestrator=orchestrator())
    CALLS.clear()
    data_function.set_active(False)
    data_function.set_active(True)
    summary = job.run(orchestrator=orchestrator())
    assert CALLS == []
    assert summary.nodes["doubled"].reused == 2
    assert job.nodes["total"].as_table().column("total").to_pylist() == [10]


def test_noop_activation_setter_keeps_modification_time(data_function) -> None:
    """Repeatedly setting the same state is not a semantic modification."""
    before = data_function.last_modified
    data_function.set_active(data_function.is_active())
    assert data_function.last_modified == before
    data_function.set_active(False)
    assert data_function.last_modified > before


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
@pytest.mark.parametrize("wrapper", ["pod", "data_function"])
def test_wrapped_function_activation_reaches_node_cache(
    data_function, orchestrator, wrapper
) -> None:
    """Both supported cache wrappers retain the leaf function's activation revision."""
    from orcapod.core.cached_function_pod import CachedFunctionPod
    from orcapod.core.data_function import CachedDataFunction
    from orcapod.core.function_pod import FunctionPod

    store = InMemoryArrowDatabase()
    original_pod = doubled.pod
    assert isinstance(original_pod, FunctionPod)
    if wrapper == "pod":
        pod = CachedFunctionPod(original_pod, result_database=store)
    else:
        pod = FunctionPod(CachedDataFunction(data_function, result_database=store))
    data_function.set_active(False)
    job = make_job(pod)
    job.run(orchestrator=orchestrator())
    assert CALLS == []
    data_function.set_active(True)
    job.run(orchestrator=orchestrator())
    assert sorted(CALLS) == [2, 3]
    assert job.nodes["total"].as_table().column("total").to_pylist() == [10]
