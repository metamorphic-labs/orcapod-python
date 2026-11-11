"""Regression tests for ITL-884 at the job level: an unreadable store fails the run.

Before ITL-884 a Delta result table that existed but could not be read was
returned as "no rows": the job warned that the data "may have been deleted
externally", swallowed the per-row read errors under ``error_policy="continue"``,
finished without raising, and the node showed no records. Both orchestrators
must now raise ``StoreReadError`` instead, and a table that does not exist must
still be computed.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import StoreReadError
from orcapod.pipeline import PipelineJob
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

_CALLS: list[int] = []


@function_pod(output_keys="y")
def double(x: int) -> int:
    _CALLS.append(x)
    return 2 * x


ORCHESTRATORS: dict[str, Callable[[], object]] = {
    "sync": lambda: SyncPipelineOrchestrator(error_policy="continue"),
    "async": lambda: AsyncPipelineOrchestrator(error_policy="continue"),
}


@pytest.fixture(autouse=True)
def _reset_calls():
    _CALLS.clear()
    yield
    _CALLS.clear()


@pytest.fixture(params=list(ORCHESTRATORS))
def make_orchestrator(request) -> Callable[[], object]:
    return ORCHESTRATORS[request.param]


def build_job(store_path: Path) -> PipelineJob:
    source = ArrowTableSource(
        pa.table({"k": ["a", "b", "c"], "x": [1, 2, 3]}),
        tag_columns=["k"],
        infer_nullable=True,
    )
    job = PipelineJob(name="itl884", store=DeltaTableDatabase(base_path=store_path))
    with job:
        _ = double.pod(source, label="dbl")
    return job


def table_dirs(store_path: Path) -> tuple[Path, Path]:
    """The (result table, pipeline table) directories the job wrote."""
    dirs = {p.parent for p in store_path.rglob("*.parquet")}
    result = [d for d in dirs if "_result" in d.parts]
    pipeline = [d for d in dirs if "_result" not in d.parts]
    assert len(result) == 1 and len(pipeline) == 1, sorted(dirs)
    return result[0], pipeline[0]


def delete_data_files(table: Path) -> None:
    for f in table.glob("*.parquet"):
        f.unlink()


@pytest.fixture
def stored(tmp_path: Path, make_orchestrator) -> Path:
    """A store in which the job has computed and stored all three rows."""
    store_path = tmp_path / "store"
    build_job(store_path).run(orchestrator=make_orchestrator())
    assert len(_CALLS) == 3
    _CALLS.clear()
    return store_path


def test_unreadable_result_table_fails_the_run(stored, make_orchestrator):
    result_table, _ = table_dirs(stored)
    delete_data_files(result_table)

    job = build_job(stored)
    with pytest.raises(StoreReadError) as info:
        job.run(orchestrator=make_orchestrator())

    assert Path(info.value.table) == result_table
    assert _CALLS == []


def test_unreadable_result_table_fails_the_run_under_continue(
    stored, make_orchestrator
):
    # With the pipeline table gone, the job has no stored tag rows to join, so the
    # first read of the result table is the per-row cache lookup, which runs inside
    # the error_policy="continue" handler.
    result_table, pipeline_table = table_dirs(stored)
    delete_data_files(result_table)
    shutil.rmtree(pipeline_table)

    job = build_job(stored)
    with pytest.raises(StoreReadError) as info:
        job.run(orchestrator=make_orchestrator())

    assert Path(info.value.table) == result_table
    assert _CALLS == []


def test_absent_tables_are_computed(stored, make_orchestrator):
    result_table, pipeline_table = table_dirs(stored)
    shutil.rmtree(result_table)
    shutil.rmtree(pipeline_table)

    build_job(stored).run(orchestrator=make_orchestrator())
    assert sorted(_CALLS) == [1, 2, 3]

    # The recomputed rows were stored: a fresh job over the same store computes
    # nothing. (Reading the node right after its own run can miss rows, ITL-864.)
    _CALLS.clear()
    build_job(stored).run(orchestrator=make_orchestrator())
    assert _CALLS == []
