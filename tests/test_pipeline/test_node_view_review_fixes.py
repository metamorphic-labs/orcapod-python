"""Defects in the job node current-run view found by a full review (ITL-1121).

The job node view (ITL-1095): a job node shows its current run; a run that raises leaves the
nodes it ran empty and stale; a node that holds a current run is not run again; a load-only
run computes, delivers and writes nothing. Each test reproduces one review comment through the
public interface (``superpowers/scripts/itl_1121`` holds the scripts).
"""

from __future__ import annotations

from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.sources import DictSource

ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


@function_pod(output_keys="y")
def inc(x: int) -> int:
    """Return ``x + 1``."""
    return x + 1


def _source() -> DictSource:
    """Three rows, tag ``k``, data ``x``."""
    return DictSource(
        [{"k": i, "x": i} for i in range(3)], tag_columns=["k"], source_id="s"
    )


# ---------------------------------------------------------------------------
# Comment 1: a function node is stamped before on_node_end
# ---------------------------------------------------------------------------


class _ReadingObserver(NoOpObserver):
    """Reads each job node in ``on_node_end``: the row count it sees, by label."""

    def __init__(self, job: PipelineJob) -> None:
        self.job = job
        self.seen: dict[str, int] = {}

    def contextualize(self, *identity_path: str) -> _ReadingObserver:
        """The same observer for every node."""
        return self

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        """Read the node that just ended."""
        node = self.job.nodes.get(node_label)
        if node is not None:
            self.seen[node_label] = node.as_table().num_rows


def _rowwise_job(db: Any) -> PipelineJob:
    """``rowwise`` = ``inc`` over three rows."""
    job = PipelineJob(name="j", database=db)
    with job:
        inc.pod(_source(), label="rowwise")
    return job


@pytest.mark.parametrize("load_only", [False, True])
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_an_observer_reading_a_node_in_on_node_end_sees_its_rows(
    orchestrator: str, load_only: bool
) -> None:
    """The node is current when ``on_node_end`` fires, and keeps its rows after the run."""
    db = InMemoryArrowDatabase()
    if load_only:
        _rowwise_job(db).run()  # store the results a load-only run loads
    job = _rowwise_job(db)
    observer = _ReadingObserver(job)
    job.run(
        orchestrator=ORCHESTRATORS[orchestrator](),
        observer=observer,
        load_only=load_only,
    )
    node = job.nodes["rowwise"]
    assert observer.seen["rowwise"] == 3
    assert node.as_table().num_rows == 3
    assert node.holds_current_run(load_only=load_only)
