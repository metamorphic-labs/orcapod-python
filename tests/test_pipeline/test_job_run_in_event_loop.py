"""A job runs with the async orchestrator from inside a running event loop (ITL-994).

A Jupyter kernel always runs an event loop, so ``asyncio.run`` cannot be called there. A job
run from inside a running loop must work, and must store exactly what a run outside a loop
stores: the same rows, record ids and hashes.
"""

from __future__ import annotations

import _thread
import asyncio
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DictSource

# Columns that differ between any two runs by design, also between two script runs: the
# result's datagram uuid, the output's source-info token (which carries that uuid) and the
# time the result was computed.
PER_RUN_COLUMNS = ("__data_id", "_source_y", "__pod_ts")

STORES: dict[str, Callable[[Path], Any]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda path: DeltaTableDatabase(path),
}


def run_inside_running_loop(job: PipelineJob) -> None:
    """Run ``job`` with the async orchestrator from inside a running event loop.

    The one place that names the call an attendee types in a notebook cell.
    """

    async def notebook_cell() -> None:
        job.run(orchestrator=AsyncPipelineOrchestrator())

    asyncio.run(notebook_cell())


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Return twice ``x``."""
    return 2 * x


def make_job(store: Any) -> PipelineJob:
    """A source of five rows feeding ``double``, recorded in a fresh job."""
    rows = [{"k": f"r{i}", "x": i} for i in range(5)]
    job = PipelineJob(name="loop_demo", store=store)
    with job:
        source = DictSource(data=rows, tag_columns=["k"], source_id="rows")
        double.pod(source, label="doubled")
    return job


def stored_rows(job: PipelineJob) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """The node's stored records and its table, minus per-run columns, sorted by tag."""
    records = job.nodes["doubled"].get_all_records(all_info=True)
    assert records is not None
    records = records.drop_columns(
        [c for c in PER_RUN_COLUMNS if c in records.column_names]
    )
    table = job.nodes["doubled"].as_table(all_info=True)

    def by_tag(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(rows, key=lambda row: row["k"])

    return by_tag(records.to_pylist()), by_tag(table.to_pylist())


@pytest.mark.parametrize("store_kind", list(STORES))
def test_run_inside_running_loop_stores_what_a_script_run_stores(
    store_kind: str, tmp_path: Path
) -> None:
    script_job = make_job(STORES[store_kind](tmp_path / "script"))
    script_job.run(orchestrator=AsyncPipelineOrchestrator())

    notebook_job = make_job(STORES[store_kind](tmp_path / "notebook"))
    run_inside_running_loop(notebook_job)

    assert stored_rows(notebook_job) == stored_rows(script_job)
    ys = sorted(notebook_job.nodes["doubled"].as_table().column("y").to_pylist())
    assert ys == [0, 2, 4, 6, 8]


def test_rerun_inside_running_loop_reuses_stored_rows(tmp_path: Path) -> None:
    store = DeltaTableDatabase(tmp_path / "store")
    first_job = make_job(store)
    run_inside_running_loop(first_job)
    first = stored_rows(first_job)

    second_job = make_job(store)
    run_inside_running_loop(second_job)

    assert stored_rows(second_job) == first


def test_store_error_inside_running_loop_reaches_the_caller(tmp_path: Path) -> None:
    """A failure inside the run reaches the cell with its own type, not wrapped."""
    from orcapod.errors import StoreWriteError

    class FailingWrites:
        """A store whose every write fails."""

        def __init__(self, inner: Any) -> None:
            self._inner = inner

        def at(self, *path_components: str) -> FailingWrites:
            return FailingWrites(self._inner.at(*path_components))

        def add_record(self, *args: Any, **kwargs: Any) -> Any:
            raise OSError("disk unavailable")

        def add_records(self, *args: Any, **kwargs: Any) -> Any:
            raise OSError("disk unavailable")

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    job = make_job(FailingWrites(InMemoryArrowDatabase()))
    with pytest.raises(StoreWriteError):
        run_inside_running_loop(job)


def test_run_inside_running_loop_returns_to_the_loop(tmp_path: Path) -> None:
    """The loop that called the run keeps working after it (a notebook's next cell)."""

    async def two_cells() -> int:
        job = make_job(InMemoryArrowDatabase())
        job.run(orchestrator=AsyncPipelineOrchestrator())
        await asyncio.sleep(0)
        return pa.table({"n": [1]}).num_rows

    assert asyncio.run(two_cells()) == 1


@pytest.mark.parametrize("store_kind", list(STORES))
def test_await_run_async_stores_what_a_script_run_stores(
    store_kind: str, tmp_path: Path
) -> None:
    script_job = make_job(STORES[store_kind](tmp_path / "script"))
    script_job.run(orchestrator=AsyncPipelineOrchestrator())

    async_job = make_job(STORES[store_kind](tmp_path / "async"))

    async def async_code() -> None:
        await async_job.run_async()

    asyncio.run(async_code())

    assert stored_rows(async_job) == stored_rows(script_job)


def test_run_async_refuses_the_sync_orchestrator() -> None:
    job = make_job(InMemoryArrowDatabase())
    with pytest.raises(TypeError, match=r"job\.run"):
        _ = asyncio.run(
            job.run_async(
                orchestrator=SyncPipelineOrchestrator()  # pyright: ignore[reportArgumentType]  # the wrong orchestrator is the case under test
            )
        )


N_SLOW_ROWS = 200  # more than the default thread pool runs at once
slow_calls: list[int] = []


@function_pod(output_keys="y")
def slow_and_interrupted(x: int) -> int:
    """Press the interrupt button during the first row; take a while for every row."""
    slow_calls.append(x)
    if len(slow_calls) == 1:
        _thread.interrupt_main()
    time.sleep(0.05)
    return x


def test_interrupt_while_running_inside_a_loop_stops_the_run() -> None:
    """The notebook's interrupt button stops the run; nothing keeps running after it."""
    slow_calls.clear()
    rows = [{"k": f"r{i:02d}", "x": i} for i in range(N_SLOW_ROWS)]
    job = PipelineJob(name="interrupt_demo", store=InMemoryArrowDatabase())
    with job:
        source = DictSource(data=rows, tag_columns=["k"], source_id="rows")
        slow_and_interrupted.pod(source, label="slow")

    async def notebook_cell() -> None:
        job.run(orchestrator=AsyncPipelineOrchestrator())

    # A bare loop, as a kernel has: asyncio.run would install its own SIGINT handler.
    loop = asyncio.new_event_loop()
    try:
        with pytest.raises(KeyboardInterrupt):
            loop.run_until_complete(notebook_cell())
    finally:
        loop.close()

    calls_when_stopped = len(slow_calls)
    assert calls_when_stopped < N_SLOW_ROWS
    time.sleep(0.2)
    assert len(slow_calls) == calls_when_stopped
