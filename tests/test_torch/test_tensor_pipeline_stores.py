"""Tensor values through ``PipelineJob`` on in-memory and Delta stores, sync and async (ITL-871)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("torch")

import torch  # noqa: E402  # pyright: ignore[reportMissingImports]  # optional peer dependency, importorskip above

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
    return x.clone()


@function_pod(output_keys="d")
def double(y: torch.Tensor) -> torch.Tensor:
    return y * 2


@function_pod(output_keys="total")
def total(d: torch.Tensor) -> float:
    return float(d.to(torch.complex128).real.sum())


@function_pod(output_keys="count")
def count(k: list[str], x: list[torch.Tensor]) -> int:
    # GroupBy moves the non-key tag ``k`` into the data as a list.
    assert len(k) == len(x)
    return len(x)


@function_pod(output_keys="pair")
def pair(x: list[torch.Tensor]) -> list[torch.Tensor]:
    return list(x)


def _build(store: Any) -> PipelineJob:
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
        # Operator results are read through a function pod: a terminal operator
        # node yields no rows after an async run (pre-existing, not tensor-specific).
        pair.pod(MergeJoin()(numeric, shifted, label="merge_join"), label="merged")
    return job


def _run_sync(job: PipelineJob) -> None:
    job.run()


def _run_async(job: PipelineJob) -> None:
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
    return {
        str(t.as_dict()[key]): d.as_dict()[column]
        for t, d in job.nodes[label].iter_data()
    }


@pytest.mark.parametrize("runner", sorted(RUNNERS))
@pytest.mark.parametrize("store", sorted(STORES))
def test_pipeline_keeps_tensors_exact(tmp_path: Path, store: str, runner: str) -> None:
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

    merged = _node(job, "merged", "pair")
    for i, name in enumerate(NUMERIC):
        assert_same_tensors_any_order(
            list(merged[name]),
            [TENSORS[name], TENSORS[NUMERIC[(i + 1) % len(NUMERIC)]]],
        )
