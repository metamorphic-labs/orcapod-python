"""ITL-1347 stage 0: count where a job run makes and unmakes rows.

Wraps, for the duration of a ``with counting():`` block and nowhere else:

- ``Datagram.__init__`` (every ``Tag`` and ``Data``), counted by class and by the node kind
  whose ``execute`` / ``async_execute`` is running (``edge`` when none is: the orchestrator
  rebuilding a successor's input, or the run plan);
- ``StaticOutputOperatorPod._rows_to_table`` (``R`` in the spec), counted and timed by its
  caller (``_kept_output``/``_materialize_to_stream``, ``_materialize_upstream``,
  ``_materialize_input_stream``);
- each node kind's ``execute`` and ``async_execute``, timed inclusively.

Nothing in the library changes; the wrappers are removed when the block ends. The counts
are what the spec's stage table needs to replace its estimates (rows made per node kind,
rebuilds per caller, edges per node).
"""

from __future__ import annotations

import contextlib
import contextvars
import functools
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator
from typing import Any

_NODE_KIND: contextvars.ContextVar[str] = contextvars.ContextVar(
    "itl1347_node_kind", default="edge"
)


class Counts:
    """What one counted block saw."""

    def __init__(self) -> None:
        self.datagrams: Counter[tuple[str, str]] = Counter()
        self.rows_to_table_calls: Counter[str] = Counter()
        self.rows_to_table_rows: Counter[str] = Counter()
        self.rows_to_table_seconds: defaultdict[str, float] = defaultdict(float)
        self.execute_calls: Counter[str] = Counter()
        self.execute_seconds: defaultdict[str, float] = defaultdict(float)

    def as_dict(self) -> dict[str, Any]:
        """The counts as plain JSON-ready data."""
        return {
            "datagrams_by_node_kind_and_class": {
                f"{kind}/{cls}": n for (kind, cls), n in sorted(self.datagrams.items())
            },
            "datagrams_total": sum(self.datagrams.values()),
            "rows_to_table_by_caller": {
                caller: {
                    "calls": self.rows_to_table_calls[caller],
                    "rows": self.rows_to_table_rows[caller],
                    "seconds": round(self.rows_to_table_seconds[caller], 4),
                }
                for caller in sorted(self.rows_to_table_calls)
            },
            "execute_by_node_kind": {
                kind: {
                    "calls": self.execute_calls[kind],
                    "seconds_inclusive": round(self.execute_seconds[kind], 4),
                }
                for kind in sorted(self.execute_calls)
            },
        }


def _patch(owner: Any, name: str, make: Callable[[Any], Any]) -> Callable[[], None]:
    """Replace ``owner.name`` with ``make(original)``; return the undo."""
    original = owner.__dict__[name]
    setattr(owner, name, make(original))

    def undo() -> None:
        setattr(owner, name, original)

    return undo


@contextlib.contextmanager
def counting() -> Iterator[Counts]:
    """Count datagrams, ``R`` calls and node executions inside the block."""
    from orcapod.core.datagrams.datagram import Datagram
    from orcapod.core.nodes.function_node import FunctionJobNode
    from orcapod.core.nodes.operator_node import OperatorJobNode
    from orcapod.core.nodes.source_node import SourceNodeBase
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
    from orcapod.side_effects import SideEffectJobNode

    counts = Counts()
    undo: list[Callable[[], None]] = []

    def wrap_init(original: Any) -> Any:
        @functools.wraps(original)
        def __init__(self: Any, *args: Any, **kwargs: Any) -> None:
            counts.datagrams[(_NODE_KIND.get(), type(self).__name__)] += 1
            original(self, *args, **kwargs)

        return __init__

    undo.append(_patch(Datagram, "__init__", wrap_init))

    def wrap_rows_to_table(original: Any) -> Any:
        func = original.__func__

        @functools.wraps(func)
        def _rows_to_table(rows: Any) -> Any:
            caller = sys._getframe(1).f_code.co_name  # pyright: ignore[reportPrivateUsage]  # the caller's name is the measurement
            start = time.perf_counter()
            try:
                return func(rows)
            finally:
                counts.rows_to_table_calls[caller] += 1
                counts.rows_to_table_rows[caller] += len(rows)
                counts.rows_to_table_seconds[caller] += time.perf_counter() - start

        return staticmethod(_rows_to_table)

    undo.append(_patch(StaticOutputOperatorPod, "_rows_to_table", wrap_rows_to_table))

    def wrap_execute(kind: str) -> Callable[[Any], Any]:
        def make(original: Any) -> Any:
            @functools.wraps(original)
            def execute(self: Any, *args: Any, **kwargs: Any) -> Any:
                token = _NODE_KIND.set(kind)
                start = time.perf_counter()
                try:
                    return original(self, *args, **kwargs)
                finally:
                    counts.execute_calls[kind] += 1
                    counts.execute_seconds[kind] += time.perf_counter() - start
                    _NODE_KIND.reset(token)

            return execute

        return make

    def wrap_async_execute(kind: str) -> Callable[[Any], Any]:
        def make(original: Any) -> Any:
            @functools.wraps(original)
            async def async_execute(self: Any, *args: Any, **kwargs: Any) -> Any:
                token = _NODE_KIND.set(kind)
                start = time.perf_counter()
                try:
                    return await original(self, *args, **kwargs)
                finally:
                    counts.execute_calls[f"{kind} (async)"] += 1
                    counts.execute_seconds[f"{kind} (async)"] += (
                        time.perf_counter() - start
                    )
                    _NODE_KIND.reset(token)

            return async_execute

        return make

    for cls, kind in (
        (SourceNodeBase, "source"),
        (OperatorJobNode, "operator"),
        (FunctionJobNode, "function"),
        (SideEffectJobNode, "side_effect"),
    ):
        undo.append(_patch(cls, "execute", wrap_execute(kind)))
        undo.append(_patch(cls, "async_execute", wrap_async_execute(kind)))

    try:
        yield counts
    finally:
        for step in reversed(undo):
            step()


def edges_per_node(job: Any) -> dict[str, dict[str, int]]:
    """Each node's label with its number of predecessors and successors."""
    graph = job.dag
    return {
        str(getattr(node, "label", node)): {
            "inputs": len(list(graph.predecessors(node))),
            "successors": len(list(graph.successors(node))),
        }
        for node in graph.topological_sort()
    }
