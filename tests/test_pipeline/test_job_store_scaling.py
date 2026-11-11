"""A PipelineJob run must do work per row independent of the rows already stored (ITL-919).

On an ``InMemoryArrowDatabase`` a function node's per-row lookups, writes and flushes used
to filter, copy or rebuild the whole stored table, so a run was quadratic in its rows
(400 rows took 7 s; the same pod outside a job took 0.04 s). The tests count the Arrow
bytes the store's calls allocate during a run, which is deterministic, and require the
cost per row to stay flat when the rows quadruple.
"""

from __future__ import annotations

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import DictSource
from tests.test_databases.arrow_work_counter import CountingDatabase


@function_pod(output_keys="y")
def double(x: int) -> int:
    return 2 * x


def _store_bytes_per_row(n_rows: int, orchestrator: str) -> float:
    store = CountingDatabase(InMemoryArrowDatabase())
    job = PipelineJob(name="scaling", store=store)  # pyright: ignore[reportArgumentType]  # counting wrapper of a real store
    with job:
        src = DictSource(
            data=[{"k": i, "x": i} for i in range(n_rows)],
            tag_columns=["k"],
            source_id="src",
        )
        double.pod(src, label="dbl")
    job.run(orchestrator=AsyncPipelineOrchestrator() if orchestrator == "async" else None)
    assert job.nodes["dbl"].as_table().num_rows == n_rows
    return store.counter.bytes_allocated / n_rows


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_store_work_per_row_does_not_grow_with_rows(orchestrator: str) -> None:
    small = _store_bytes_per_row(150, orchestrator)
    large = _store_bytes_per_row(600, orchestrator)
    # Four times the rows: a per-row scan of the stored rows costs about four times as
    # much per row; work proportional to the rows touched stays flat.
    assert large < 2 * small, (small, large)
