"""Completion accounting for failures and successful filtered rows (ITL-1063)."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import cast

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import StoreWriteError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DictSource
from tests.test_pipeline.test_store_write_failure import (
    RESULT_TABLE,
    ROWS,
    CrashingObserver,
    Faults,
    FaultyStore,
    build_job,
)

ORCHESTRATORS = [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
CALLS: list[int] = []
fail_repair = False


@function_pod(output_keys="y")
def recover(x: int) -> int:
    """Count every recovery attempt and optionally fail it."""
    CALLS.append(x)
    if fail_repair:
        raise ValueError("repair failed")
    return x * 2


@function_pod(output_keys="y")
def filter_even(x: int) -> int | None:
    """Return no row for even inputs, counting successful filtering too."""
    CALLS.append(x)
    return x if x % 2 else None


def make_job(
    store: ArrowDatabaseProtocol, pod: FunctionPod = cast(FunctionPod, recover.pod)
) -> PipelineJob:
    """Build the same stable two-row job over the supplied store."""
    job = PipelineJob(name="review_recovery", store=store)
    with job:
        _ = pod(
            DictSource([{"k": 1, "x": 1}, {"k": 2, "x": 2}], tag_columns=["k"]),
            label="work",
        )
    return job


@pytest.fixture(autouse=True)
def reset() -> Iterator[None]:
    """Keep deliberate row failures and call counts local to each test."""
    global fail_repair
    fail_repair = False
    CALLS.clear()
    assert isinstance(recover.pod.data_function, PythonDataFunction)
    recover.pod.data_function.set_active(True)
    yield
    fail_repair = False
    CALLS.clear()
    assert isinstance(recover.pod.data_function, PythonDataFunction)
    recover.pod.data_function.set_active(True)


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_kept_store_error_never_certifies_completion(
    orchestrator: type[SyncPipelineOrchestrator] | type[AsyncPipelineOrchestrator],
) -> None:
    """A failing observer cannot turn a propagating write error into completion."""
    faults = Faults(write_marker=RESULT_TABLE)
    job = build_job(FaultyStore(InMemoryArrowDatabase(), faults), ROWS)
    with pytest.raises(StoreWriteError) as info:
        job.run(orchestrator=orchestrator(), observer=CrashingObserver())
    assert any("on_data_crash" in note for note in info.value.__notes__)
    assert job.nodes["dbl"]._completed_execution is None


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
@pytest.mark.parametrize("repair_fails", [False, True])
def test_dangling_result_is_repaired_before_completion(
    orchestrator: type[SyncPipelineOrchestrator] | type[AsyncPipelineOrchestrator],
    repair_fails: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Missing results are misses; only successful recovery certifies the new job."""
    global fail_repair
    store = InMemoryArrowDatabase()
    make_job(store).run(orchestrator=orchestrator())
    # Retain pipeline references but remove their result tables, as external loss can.
    for tables in (store._tables, store._pending_batches):
        for key in list(tables):
            if RESULT_TABLE in key:
                del tables[key]
    CALLS.clear()
    fail_repair = repair_fails
    job = make_job(store)
    with caplog.at_level(logging.WARNING, logger="orcapod.core.nodes.function_node"):
        summary = job.run(orchestrator=orchestrator())
    assert any("have no match in persistent result DB" in m for m in caplog.messages)
    assert sorted(CALLS) == [1, 2]
    assert summary.nodes["work"].failed == (2 if repair_fails else 0)
    assert (job.nodes["work"]._completed_execution is None) == repair_fails
    assert job.nodes["work"].as_table().num_rows == (0 if repair_fails else 2)
    if repair_fails:
        fail_repair = False
        CALLS.clear()
        assert job.run(orchestrator=orchestrator()).ok
        assert sorted(CALLS) == [1, 2]
        assert job.nodes["work"]._completed_execution is not None


@pytest.mark.parametrize("first", ORCHESTRATORS)
@pytest.mark.parametrize("second", ORCHESTRATORS)
def test_none_results_are_successful_cached_filters(
    first: type[SyncPipelineOrchestrator] | type[AsyncPipelineOrchestrator],
    second: type[SyncPipelineOrchestrator] | type[AsyncPipelineOrchestrator],
) -> None:
    """Both paths count filtered rows as success and never forward a None datagram."""
    job = make_job(InMemoryArrowDatabase())
    assert isinstance(recover.pod.data_function, PythonDataFunction)
    recover.pod.data_function.set_active(False)
    cold = job.run(orchestrator=first())
    assert CALLS == []
    assert cold.nodes["work"].computed == 2
    assert job.nodes["work"].as_table().num_rows == 0
    assert all(
        data is None for _, data in job.nodes["work"]._cached_output_datas.values()
    )
    CALLS.clear()
    warm = job.run(orchestrator=second())
    assert CALLS == []
    assert warm.nodes["work"].computed == warm.nodes["work"].failed == 0
    assert warm.nodes["work"].reused == 2
    assert job.nodes["work"].as_table().num_rows == 0


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_nullable_output_is_a_row_not_a_filtered_result(
    orchestrator: type[SyncPipelineOrchestrator] | type[AsyncPipelineOrchestrator],
) -> None:
    """A Python function returning a nullable value produces Data containing None."""
    job = make_job(InMemoryArrowDatabase(), cast(FunctionPod, filter_even.pod))
    job.run(orchestrator=orchestrator())
    assert job.nodes["work"].as_table().column("y").to_pylist() == [1, None]
    assert all(
        data is not None for _, data in job.nodes["work"]._cached_output_datas.values()
    )
