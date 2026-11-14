"""Staleness of job nodes across the job graph (ITL-1095).

A stream is stale when an upstream or its producer was modified after it
(``StreamBase.is_stale``), which looks one level up only. A job node holds the rows of
its current run, so it is stale also when any job node it depends on is stale: a source
rebound two nodes up makes every node below it stale, not only its direct consumer.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


class RunHoldingNode:
    """Mixin of the job nodes that hold the rows of their current run.

    ``FunctionJobNode`` and ``OperatorJobNode`` mix it in ahead of their stream base, so
    their ``is_stale`` covers their whole ancestry. Side-effect job nodes do not yet:
    their own ``is_stale`` is redefined by ITL-1091.
    """

    # The summary of the node's last ``run()`` (with the nodes it ran first); a class
    # default so nodes rebuilt from a saved descriptor start without one.
    _last_run: Any = None

    @property
    def last_run(self) -> Any:
        """The ``RunSummary`` of this node's last ``run()``, or ``None``.

        ``node.run()`` runs the node and the job nodes it depends on (ITL-1095); this is
        what that run did, per function node, as ``job.last_run`` is for ``job.run()``.
        A ``run()`` that found the node current ran nothing and leaves it unchanged.
        """
        return self._last_run

    @property
    def is_stale(self) -> bool:
        """Whether the rows this node holds are out of date, or it holds none.

        True before the node's first run in this process and after ``clear_cache()``
        (no modified time), when an input or its producer was modified after the
        node's run (``StreamBase.is_stale``), or when any job node it depends on is
        stale (ITL-1095). A stale node reads empty until it runs again.
        """
        own: bool = super().is_stale  # pyright: ignore[reportAttributeAccessIssue]  # the stream base after this mixin in the MRO defines it
        return own or upstream_job_node_stale(self.upstreams)  # pyright: ignore[reportAttributeAccessIssue]  # every node class this is mixed into has upstreams

    def holds_current_run(self, *, load_only: bool = False) -> bool:
        """Whether the node holds the rows of a run that ``run()`` would not redo.

        A node that is not stale and holds the rows of a completed run in this process
        is current: ``run()`` returns them without running it (developer, November 13,
        2026, ITL-1095). A node filled by a load-only run is current only for another
        load-only run: a full run computes the inputs it skipped. So is every node
        whose rows came from such a node: a node is current only if every job node
        above it is current for the same kind of run.

        Args:
            load_only: Whether the question is for a load-only run.

        Returns:
            ``True`` if the node holds a current run for such a run.
        """
        if not self._holds_run(load_only=load_only) or self.is_stale:
            return False
        upstreams: tuple[Any, ...] = getattr(self, "upstreams", ())
        return all(
            upstream.holds_current_run(load_only=load_only)
            for upstream in upstreams
            if isinstance(upstream, RunHoldingNode)
        )

    def _holds_run(self, *, load_only: bool) -> bool:
        """Whether the node holds the rows of a completed run good for such a run."""
        raise NotImplementedError


def upstream_job_node_stale(upstreams: Iterable[Any]) -> bool:
    """Return whether any job node in ``upstreams`` that holds run rows is stale.

    Each such node answers with its own ``is_stale``, which applies this rule to its
    own upstreams in turn, so the whole ancestry is covered.

    Args:
        upstreams: The direct upstream streams of a job node.

    Returns:
        ``True`` if any upstream ``RunHoldingNode`` is stale.
    """
    return any(
        isinstance(upstream, RunHoldingNode) and upstream.is_stale
        for upstream in upstreams
    )
