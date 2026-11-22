"""Held streams and async outputs are built when read (ITL-1360; Codex on PR #485).

A run plan's held streams (``RunPlan.held``) and the async orchestrator's
``node_outputs`` are ``StreamsOnRead``: a node's stream is built from its rows when it
is read. Membership, length and iteration never build one; the plan keeps no held
stream it has built, so the sync orchestrator releases it once its successors ran.
"""

from __future__ import annotations

import gc
import weakref
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.observer import NoOpObserver
from orcapod.pipeline.result import StreamsOnRead
from orcapod.sources import DictSource


def test_membership_length_and_iteration_build_nothing() -> None:
    """``in``, ``len`` and iteration read the nodes only; reading a value builds it."""

    def build(rows: Any, node: Any) -> Any:
        raise AssertionError(f"built {node}")

    outputs = StreamsOnRead({"a": [], "b": []}, build)
    assert "a" in outputs
    assert "c" not in outputs
    assert len(outputs) == 2
    assert list(outputs) == ["a", "b"]
    with pytest.raises(AssertionError, match="built a"):
        _ = outputs["a"]


def test_a_kept_stream_is_built_once_and_an_unkept_one_each_read() -> None:
    built: list[Any] = []

    def build(rows: Any, node: Any) -> Any:
        built.append(node)
        return object()

    kept = StreamsOnRead({"a": []}, build)
    assert kept["a"] is kept["a"]
    unkept = StreamsOnRead({"a": []}, build, keep=False)
    _ = unkept["a"]
    _ = unkept["a"]
    assert built == ["a", "a", "a"]


@function_pod(output_keys="y")
def step(x: int) -> int:
    return x + 1


def test_a_held_stream_is_released_once_its_successors_ran(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``source -> a -> b -> c -> d`` with ``a`` held: its stream is gone when ``c`` starts."""
    job = PipelineJob(name="held_lifetime", database=InMemoryArrowDatabase())
    with job:
        source = DictSource(
            [{"k": i, "x": i} for i in range(3)],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="s",
        )
        a = step.pod(source, label="a")
        b = step.pod(a.map_data({"y": "x"}), label="b")
        c = step.pod(b.map_data({"y": "x"}), label="c")
        step.pod(c.map_data({"y": "x"}), label="d")
    job.run()
    for label in ("b", "c", "d"):
        job.nodes[label].clear_cache()
    for node in job.nodes.values():
        if node.label.startswith("MapData"):
            node.clear_cache()

    held: list[weakref.ref[Any]] = []
    original = StaticOutputOperatorPod._materialize_upstream  # pyright: ignore[reportPrivateUsage]  # the held stream's build

    def materialize(rows: Any, upstream: Any, **kwargs: Any) -> Any:
        stream = original(rows, upstream, **kwargs)
        if getattr(upstream, "label", None) == "a":
            held.append(weakref.ref(stream))
        return stream

    monkeypatch.setattr(
        StaticOutputOperatorPod, "_materialize_upstream", staticmethod(materialize)
    )

    alive_at_c: list[bool] = []

    class AtC(NoOpObserver):
        def contextualize(self, *identity_path: str) -> AtC:
            return self

        def on_node_start(
            self, node_label: str, node_hash: str, tag_schema: Any = None
        ) -> None:
            if node_label == "c":
                gc.collect()
                alive_at_c.append(any(ref() is not None for ref in held))

    summary = job.run(observer=AtC(), materialize_results=False)
    assert summary.nodes["a"].held == 3
    assert held
    assert alive_at_c == [False]
