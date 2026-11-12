"""Tensor values through ``PipelineJob`` on in-memory and Delta stores, sync and async (ITL-871)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("torch")

import torch  # noqa: E402

from orcapod import PipelineJob, function_pod  # noqa: E402
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase  # noqa: E402
from orcapod.operators import GroupBy, MergeJoin  # noqa: E402
from orcapod.pipeline import AsyncPipelineOrchestrator  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402
from tests.test_torch.tensor_cases import (  # noqa: E402
    NUMERIC,
    TENSORS,
    assert_same_tensors_any_order,
    assert_tensors_identical,
)


@function_pod(output_keys="y")
def identity(x: torch.Tensor) -> torch.Tensor:
    """Return a copy of the input."""
    return x.clone()


@function_pod(output_keys="d")
def double(y: torch.Tensor) -> torch.Tensor:
    """Return twice the input."""
    return y * 2


@function_pod(output_keys="total")
def total(d: torch.Tensor) -> float:
    """Return the sum of the real parts as a float."""
    return float(d.to(torch.complex128).real.sum())


@function_pod(output_keys="count")
def count(x: list[torch.Tensor]) -> int:
    """Return the number of tensors in the group."""
    return len(x)


@function_pod(output_keys="pair")
def pair(x: list[torch.Tensor]) -> list[torch.Tensor]:
    """Return the merged tensors as a list."""
    return list(x)


def _build(store: Any) -> PipelineJob:
    """Build a job with pods, GroupBy and MergeJoin over tensor sources on ``store``."""
    numeric = DictSource(
        data=[{"k": k, "x": TENSORS[k]} for k in NUMERIC],
        tag_columns=["k"],
        source_id="numeric",
    )
    every = DictSource(
        data=[
            {"k": k, "g": f"g{i % 2}", "x": v}
            for i, (k, v) in enumerate(TENSORS.items())
        ],
        tag_columns=["k", "g"],
        source_id="every",
    )
    shifted = DictSource(
        data=[
            {"k": k, "x": TENSORS[NUMERIC[(i + 1) % len(NUMERIC)]]}
            for i, k in enumerate(NUMERIC)
        ],
        tag_columns=["k"],
        source_id="shifted",
    )
    job = PipelineJob(name="tensors", store=store)
    with job:
        identity.pod(every, label="identity_all")
        y = identity.pod(numeric, label="identity_numeric")
        d = double.pod(y, label="double")
        total.pod(d, label="total")
        count.pod(GroupBy(by=["g"])(every, label="grouped"), label="count")
        pair.pod(MergeJoin()(numeric, shifted, label="merge_join"), label="merged")
    return job


def _run_sync(job: PipelineJob) -> None:
    """Run the job synchronously."""
    job.run()


def _run_async(job: PipelineJob) -> None:
    """Run the job with the async orchestrator."""
    AsyncPipelineOrchestrator().run(job.dag)


STORES: dict[str, Callable[[Path], Any]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda tmp: DeltaTableDatabase(base_path=tmp / "store"),
}
RUNNERS: dict[str, Callable[[PipelineJob], None]] = {
    "sync": _run_sync,
    "async": _run_async,
}


def _node(job: PipelineJob, label: str, column: str, key: str = "k") -> dict[str, Any]:
    """Map each row's ``key`` tag to ``column`` of the node labelled ``label``."""
    return {
        str(t.as_dict()[key]): d.as_dict()[column]
        for t, d in job.nodes[label].iter_data()
    }


@pytest.mark.parametrize(
    ("store", "runner"),
    [(store, runner) for store in sorted(STORES) for runner in sorted(RUNNERS)],
)
def test_pipeline_keeps_tensors_exact(tmp_path: Path, store: str, runner: str) -> None:
    """Every node of the job keeps tensors exact on each store and runner."""
    job = _build(STORES[store](tmp_path))
    RUNNERS[runner](job)

    every = _node(job, "identity_all", "y")
    assert sorted(every) == sorted(TENSORS)
    for name, expected in TENSORS.items():
        assert_tensors_identical(every[name], expected)

    doubled = _node(job, "double", "d")
    totals = _node(job, "total", "total")
    for name in NUMERIC:
        expected = TENSORS[name] * 2
        assert_tensors_identical(doubled[name], expected)
        assert totals[name] == pytest.approx(
            float(expected.to(torch.complex128).real.sum()), nan_ok=True
        )

    counts = _node(job, "count", "count", key="g")
    assert sum(counts.values()) == len(TENSORS)

    # The operator node itself and the pod downstream of it hold the same lists.
    for label, column in (("merge_join", "x"), ("merged", "pair")):
        merged = _node(job, label, column)
        assert sorted(merged) == sorted(NUMERIC)
        for i, name in enumerate(NUMERIC):
            assert_same_tensors_any_order(
                list(merged[name]),
                [TENSORS[name], TENSORS[NUMERIC[(i + 1) % len(NUMERIC)]]],
            )
