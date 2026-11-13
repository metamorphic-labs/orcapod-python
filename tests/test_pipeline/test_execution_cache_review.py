"""Completion bookkeeping stays bounded and rejects replaced cache ownership."""

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from orcapod.core.nodes import _execution_cache as cache


class Snapshot:
    """A stable snapshot for exercising graph bookkeeping without row costs."""

    upstreams: tuple[()] = ()
    last_modified = 0
    data_context_key = "test"


class Node:
    """A completion-aware node with observable definition reads."""

    data_context_key = "test"

    def __init__(self, *upstreams: Any) -> None:
        self.upstreams = upstreams
        self.reads = 0
        self._producer = SimpleNamespace(last_modified=0)
        self.output = ""
        self._completed_execution: object | None = None

    @property
    def producer(self) -> Any:
        self.reads += 1
        assert self.reads <= 100, "shared upstream was expanded repeatedly"
        return self._producer

    @cache.track_completion
    def execute(
        self, value: str, *, nested: bool = False, observer: Any = None
    ) -> None:
        self.output = value
        if nested:
            self.execute("replacement")

    @cache.track_completion
    async def async_execute(
        self, value: str, *, nested: bool = False, observer: Any = None
    ) -> None:
        self.output = value
        if nested:
            await self.async_execute("replacement")


class Graph:
    """Minimal graph surface used by the orchestrator's execution bookkeeping."""

    def __init__(self, nodes: list[Any]) -> None:
        self.nodes = nodes

    def topological_sort(self) -> list[Any]:
        return self.nodes

    def predecessors(self, node: Any) -> tuple[Any, ...]:
        return node.upstreams


KINDS = {
    "source_node": (),
    "arrow": (Snapshot,),
    "memory_source": (),
    "spiral": (),
    "computed": (Node,),
}


def test_shared_dag_snapshot_and_proof_visit_each_definition_once() -> None:
    """Twenty diamonds must not expand into a million upstream paths."""
    nodes: list[Any] = [Snapshot()]
    for _ in range(20):
        left, right = Node(nodes[-1]), Node(nodes[-1])
        nodes.extend([left, right, Node(left, right)])
    run = cache._Run(Graph(nodes), KINDS)
    comparisons = 0
    original_equal = cache._Identity.__eq__

    def counted_equal(left: cache._Identity, right: object) -> bool:
        nonlocal comparisons
        comparisons += 1
        assert comparisons <= 8 * len(nodes), "shared definitions compared repeatedly"
        return original_equal(left, right)

    with patch.object(cache._Identity, "__eq__", counted_equal):
        assert run.definition(nodes[-1]) == run.initial[id(nodes[-1])]
    for node in nodes[1:]:
        assert node.reads == 2
        node._completed_execution = run.initial[id(node)]
        node.reads = 0
    assert run.proof(nodes[-1]) == run.initial[id(nodes[-1])]
    assert all(node.reads == 1 for node in nodes[1:])
    # A fresh validity traversal must see changes below a shared branch.
    nodes[1]._execution_generation = 1
    assert run.proof(nodes[-1]) is None


@pytest.mark.parametrize("asynchronous", [False, True])
def test_nested_direct_execution_cannot_be_certified_by_outer_call(
    asynchronous: bool,
) -> None:
    """A nested call replaces the output but cannot inherit the outer graph proof."""
    source = Snapshot()
    node = Node(source)
    run = cache._Run(Graph([source, node]), KINDS)
    token = cache._RUN.set(run)
    try:
        if asynchronous:
            asyncio.run(
                cache.async_execute_node(
                    node, node.async_execute, "original", nested=True
                )
            )
        else:
            cache.execute_node(node, node.execute, "original", nested=True)
    finally:
        cache._RUN.reset(token)
    assert node.output == "replacement"
    assert node._completed_execution is None
