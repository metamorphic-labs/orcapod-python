"""The edges of a pipeline's graph carry their input position (ITL-1419; stage 1 of ITL-1472).

An edge is ``(upstream, node, position)``. One upstream given to two inputs of an ordered
operator (``MergeJoin()(a, a)``) is two edges, at positions 0 and 1; it was one
``(upstream, node)`` pair, so the operator was handed one input when the job ran.

Two rules live here and nowhere else:

- ``input_positions``: the position of each input of a pod, from its
  ``argument_symmetry``. The compile, the recording of an invocation, the rebuild of a
  job's blueprint and the loader take positions from it.
- ``node_inputs``: the upstream of each input of a compiled node, in position order, as
  the orchestrators give it its inputs.

Positions enter no hash, no stored path and no saved file: a saved file lists each
distinct ``(upstream, node)`` pair once, as before, and an operator's ``inputs`` list
(ITL-1417) gives the argument order its positions are rebuilt from.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, TypeVar, cast

from orcapod.core.operators.repeated_operands import (
    SymmetryDeclaring,
    is_flat_commutative,
    read_argument_symmetry,
)

if TYPE_CHECKING:
    from orcapod.pipeline.dag import GraphProtocol
    from orcapod.protocols.core_protocols import StreamProtocol

KeyT = TypeVar("KeyT")

Position = int | None
"""An input's position: its argument index for an ordered pod, ``None`` for a
commutative one, whose inputs have no position."""


def input_positions(
    pod: SymmetryDeclaring | None, inputs: Sequence[object]
) -> tuple[Position, ...]:
    """The position of each of ``inputs`` of ``pod``.

    A flat set of the inputs (commutative: ``Join``, ``Rejoin``, even with one input)
    gives every input ``None``. Other groups, including nested groups, preserve
    each input's argument index. A function pod is recorded with its one
    (joined) input and a side-effect pod takes one input: position 0. A pod that is not
    known (``None``: a node a loaded file keeps read-only, or of a kind this version
    does not restore) has its inputs in the argument order the file gives, so each its
    index.

    Args:
        pod: The pod the inputs are given to, or ``None`` when it is not known.
        inputs: The input streams, in argument order: the pod's ``argument_symmetry``
            may read them, so never their keys (keys only when ``pod`` is ``None``).

    Returns:
        One position per input, in argument order.
    """
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod

    if not isinstance(pod, StaticOutputOperatorPod) or not inputs:
        return tuple(range(len(inputs)))
    streams = cast("Sequence[StreamProtocol]", inputs)
    if is_flat_commutative(read_argument_symmetry(pod, streams), streams):
        return (None,) * len(inputs)
    return tuple(range(len(inputs)))


def positioned_edges(
    node: KeyT, upstreams: Sequence[KeyT], positions: Sequence[Position]
) -> list[tuple[KeyT, KeyT, Position]]:
    """The edges ``(upstream, node, position)`` into ``node``, in argument order.

    Args:
        node: The node's key.
        upstreams: Its inputs' keys, in argument order.
        positions: Their positions, from ``input_positions``.

    Returns:
        One edge per input.
    """
    return [
        (upstream, node, position)
        for upstream, position in zip(upstreams, positions, strict=True)
    ]


def edge_sort_key(edge: tuple[str, str, Position]) -> tuple[str, str, int]:
    """The order of a list of edges: by upstream, then node, then position (``None``
    first)."""
    upstream, node, position = edge
    return (upstream, node, -1 if position is None else position)


def distinct_pairs(
    edges: Iterable[tuple[KeyT, KeyT, Position]],
) -> list[tuple[KeyT, KeyT]]:
    """The distinct ``(upstream, node)`` pairs of ``edges``, in first-seen order: what a
    graph of nodes (and a saved file) holds."""
    return list(dict.fromkeys((upstream, node) for upstream, node, _ in edges))


def node_inputs(node: Any, graph: GraphProtocol[Any]) -> list[Any]:
    """The upstream node of each input of ``node``, in position order.

    One entry per input: an upstream given to two inputs is listed twice. The positions
    are the node's own ``upstreams`` (built from its pod's inputs in argument order); each
    must be a predecessor of ``node`` in ``graph``, by identity.

    Args:
        node: A compiled node that is not a source.
        graph: The graph being run.

    Returns:
        The node's inputs, as nodes of ``graph``.

    Raises:
        ValueError: If an input of the node is not a predecessor of it in ``graph``.
    """
    predecessors = list(graph.predecessors(node))
    inputs: list[Any] = []
    for upstream in node.upstreams:
        found = next((p for p in predecessors if p is upstream), None)
        if found is None:
            raise ValueError(
                f"Node {node.label!r}: its input {getattr(upstream, 'label', upstream)!r} "
                "is not a node of the graph being run."
            )
        inputs.append(found)
    if not inputs:
        raise ValueError(f"Node {node.label!r} has no input in the graph being run.")
    return inputs
