"""Staleness of job nodes across the job graph (ITL-1095).

A stream is stale when an upstream or its producer was modified after it
(``StreamBase.is_stale``), which looks one level up only. A job node holds the rows of
its current run, so it is stale also when any job node it depends on is stale: a source
rebound two nodes up makes every node below it stale, not only its direct consumer.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def upstream_job_node_stale(upstreams: Iterable[Any]) -> bool:
    """Return whether any function or operator job node in ``upstreams`` is stale.

    Each such node answers with its own ``is_stale``, which applies this rule to its
    own upstreams in turn, so the whole ancestry is covered. Side-effect job nodes are
    not followed yet: their own ``is_stale`` is redefined by ITL-1091.

    Args:
        upstreams: The direct upstream streams of a job node.

    Returns:
        ``True`` if any upstream function or operator job node is stale.
    """
    from orcapod.core.nodes.function_node import FunctionJobNode
    from orcapod.core.nodes.operator_node import OperatorJobNode

    return any(
        isinstance(upstream, (FunctionJobNode, OperatorJobNode)) and upstream.is_stale
        for upstream in upstreams
    )
