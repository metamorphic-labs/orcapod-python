"""A run counts the rows of the nodes it holds without building them (ITL-1351).

``plan_run`` gives the run summary every held node's row count. It used to iterate the
node's rows, which builds a ``(Tag, Data)`` pair per row of an operator's output only to
count it. Each kind of held node now counts what it holds (``held_row_count``); the
counts, and so the summaries, must be those of iterating the rows, for every kind of node
that holds a run (operator, function, side effect), after a full and a load-only run, on
the sync and the async orchestrator.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

import orcapod.pipeline.job as job_module
from orcapod import PipelineJob, function_pod
from orcapod.core.datagrams import tag_data
from orcapod.core.nodes import FunctionJobNode, OperatorJobNode
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.side_effects import SideEffectJobNode, SideEffectPod, side_effect_pod
from orcapod.sources import ArrowTableSource

ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}
# The kinds of job node that hold a run (``RunHoldingNode``).
HOLDING = (OperatorJobNode, FunctionJobNode, SideEffectJobNode)


@function_pod(output_keys="y")
def scaled(x: int, w: float) -> float:
    """Return ``x * w``."""
    return x * w


@function_pod(output_keys="total")
def summed(x: list[int]) -> int:
    """Return the sum of a group's values."""
    return sum(x)


def _note(x: int, ctx: Any) -> None:  # pyright: ignore[reportExplicitAny]  # the invocation context
    """A side effect that does nothing."""


def _source() -> ArrowTableSource:
    """Two tags (``g``, ``k``) and two data columns (``x``, ``w``, one null ``w``)."""
    table = pa.table(
        {
            "k": pa.array([f"r{i}" for i in range(8)], pa.large_string()),
            "x": pa.array(range(8), pa.int64()),
            "g": pa.array([i % 3 for i in range(8)], pa.int64()),
            "w": pa.array([0.5, 1.5, None, 2.5, 3.5, 4.5, 5.5, 6.5], pa.float64()),
        },
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
                pa.field("g", pa.int64(), nullable=False),
                pa.field("w", pa.float64()),
            ]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["g", "k"], tag_only_columns=True, source_id="held"
    )


def _job(db: Any) -> PipelineJob:  # pyright: ignore[reportExplicitAny]  # a database
    """Operators (drop_nulls, select, group_by), function pods and a side effect."""
    tap = side_effect_pod(_note)
    assert isinstance(tap, SideEffectPod)
    job = PipelineJob(name="held", database=db)
    with job:
        kept: Any = _source().drop_nulls(["w"], label="kept")  # pyright: ignore[reportExplicitAny]
        scaled.pod(kept, label="scaled")
        tap.process(kept.select_data_columns(["x"], label="xs"), label="tapped")
        summed.pod(
            kept.select_data_columns(["x"]).group_by("g", label="grouped"), label="sums"
        )
    return job


def _iterated(job: PipelineJob) -> dict[str, int]:
    """Every held node's row count by iterating its rows (the old count)."""
    return {
        label: sum(1 for _ in node.iter_data())
        for label, node in job.nodes.items()
        if isinstance(node, HOLDING)
    }


@pytest.mark.parametrize("orchestrator", sorted(ORCHESTRATORS))
@pytest.mark.parametrize("load_only", [False, True], ids=["full", "load_only"])
def test_a_held_run_counts_every_held_node_as_its_rows_and_builds_none(
    orchestrator: str, load_only: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = InMemoryArrowDatabase()
    # The held rows come from runs on the orchestrator under test (CodeRabbit, #476).
    _job(db).run(orchestrator=ORCHESTRATORS[orchestrator]())
    job = _job(db)
    job.run(orchestrator=ORCHESTRATORS[orchestrator](), load_only=load_only)

    plans: list[Any] = []  # pyright: ignore[reportExplicitAny]  # RunPlan
    plan_run = job_module.plan_run

    def spying(*args: Any, **kwargs: Any) -> Any:  # pyright: ignore[reportExplicitAny]
        plans.append(plan_run(*args, **kwargs))
        return plans[-1]

    built: list[int] = []
    data_init = tag_data.Data.__init__

    def counting(self: Any, *args: Any, **kwargs: Any) -> None:  # pyright: ignore[reportExplicitAny]
        built.append(1)
        data_init(self, *args, **kwargs)

    monkeypatch.setattr(job_module, "plan_run", spying)
    monkeypatch.setattr(tag_data.Data, "__init__", counting)
    summary = job.run(orchestrator=ORCHESTRATORS[orchestrator](), load_only=load_only)
    monkeypatch.undo()

    # Every node held: nothing ran, and counting the held rows built no datagram.
    assert repr(summary).splitlines()[0].endswith("every node holds a current run.")
    assert built == []
    (plan,) = plans
    counts = {node.label: count for node, count in plan.held_counts.items()}
    expected = _iterated(job)
    assert counts == expected
    # Two equal selects over one input are one node, so its label is the default one.
    assert {"kept", "scaled", "tapped", "grouped", "sums"} <= set(counts)
    assert len(counts) == len(expected) == 6
    assert expected["kept"] == 7 and expected["grouped"] == 3
    for label in ("scaled", "sums"):
        assert summary.nodes[label].held == expected[label]


def test_a_node_counts_its_rows_as_iterating_them_does_before_and_after_a_run() -> None:
    job = _job(InMemoryArrowDatabase())
    held = [n for n in job.nodes.values() if isinstance(n, HOLDING)]
    assert {type(n).__name__ for n in held} == {
        "OperatorJobNode",
        "FunctionJobNode",
        "SideEffectJobNode",
    }
    assert all(n.held_row_count() == 0 == sum(1 for _ in n.iter_data()) for n in held)
    job.run()
    for node in held:
        assert node.held_row_count() == sum(1 for _ in node.iter_data()), node.label
    for node in held:
        node.clear_cache()
        assert node.held_row_count() == 0 == sum(1 for _ in node.iter_data())
