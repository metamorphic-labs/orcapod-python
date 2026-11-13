"""Count what a pipeline run does, per node, by monkeypatching orcapod at run time (ITL-1061).

Investigation aid only: nothing under ``src/`` is changed. ``install()`` wraps library
functions with counting wrappers; every count is attributed to the node whose
``execute`` / ``async_execute`` is running (a ``ContextVar``, so async tasks keep their
own node), or to ``-`` outside any node (lazy streams, the orchestrator itself).

Metrics (one row each in the report):

- ``node.execute``: calls of a node's ``execute`` / ``async_execute``.
- ``rows.out``: rows a sync ``execute`` returned (the orchestrator's buffer).
- ``source.iter``: ``iter_data`` calls on a concrete source (a read of the source).
- ``spiral.scan``: scans a ``SpiralDBTableSource`` started (``_batch_streams``).
- ``record_id``: per-row source record ids built (``stream_builder._make_record_id``).
- ``base_entry_id``: per-row function-node entry ids (``compute_base_entry_id``).
- ``hash_table``: Arrow table hashes (``SemanticArrowHasher.hash_table``); per-row
  data hashes, entry ids and source table hashes all go through it.
- ``Tag.new`` / ``Data.new``: datagram objects constructed.
- ``ats.new``: ``ArrowTableStream`` objects constructed.
- ``ats.iter.build`` / ``ats.iter.cached``: ``ArrowTableStream.iter_data`` calls that
  built per-row Tag/Data objects vs. served its ``_cached_elements``.
- ``op.static_process``: operator computations (``static_process`` of any operator).
- ``op.process``: ``StaticOutputOperatorPod.process`` (a new ``DynamicPodStream``).
- ``rows_to_table`` / ``materialize_to_stream`` / ``hstack_tables``: table rebuilds.
- ``fn.call``: user function bodies run (counted by the driver's functions).
- ``db.read`` / ``db.write`` / ``db.flush``: store calls.
"""

from __future__ import annotations

import contextvars
import functools
import inspect
from collections import Counter, defaultdict
from typing import Any, Callable

NODE: contextvars.ContextVar[str] = contextvars.ContextVar("node", default="-")
COUNTS: defaultdict[str, Counter[str]] = defaultdict(Counter)


def bump(metric: str, n: int = 1) -> None:
    """Add ``n`` to ``metric`` for the node now running."""
    COUNTS[NODE.get()][metric] += n


def reset() -> None:
    """Forget every count."""
    COUNTS.clear()


def snapshot() -> dict[str, dict[str, int]]:
    """Return a copy of the counts, per node."""
    return {node: dict(c) for node, c in COUNTS.items()}


def _wrap(owner: Any, name: str, metric: str | Callable[..., str]) -> None:
    """Replace ``owner.name`` with a wrapper that counts each call."""
    raw = owner.__dict__[name]
    is_static = isinstance(raw, staticmethod)
    func = raw.__func__ if is_static else raw

    if inspect.isgeneratorfunction(func):

        @functools.wraps(func)
        def gen_wrapper(*args, **kwargs):
            bump(metric if isinstance(metric, str) else metric(*args, **kwargs))
            yield from func(*args, **kwargs)

        wrapper: Callable[..., Any] = gen_wrapper
    else:

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            bump(metric if isinstance(metric, str) else metric(*args, **kwargs))
            return func(*args, **kwargs)

    setattr(owner, name, staticmethod(wrapper) if is_static else wrapper)


def _wrap_node(owner: Any, name: str) -> None:
    """Count a node's ``execute`` / ``async_execute`` and attribute nested counts to it."""
    func = owner.__dict__[name]
    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            token = NODE.set(self.label)
            try:
                bump("node.execute")
                return await func(self, *args, **kwargs)
            finally:
                NODE.reset(token)

        setattr(owner, name, async_wrapper)
        return

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        token = NODE.set(self.label)
        try:
            bump("node.execute")
            out = func(self, *args, **kwargs)
            if isinstance(out, list):
                bump("rows.out", len(out))
            return out
        finally:
            NODE.reset(token)

    setattr(owner, name, wrapper)


def install() -> None:
    """Patch the library with the counting wrappers (idempotent per process: call once)."""
    from orcapod.core.datagrams.tag_data import Data, Tag
    from orcapod.core.nodes import function_node, operator_node, source_node
    from orcapod.core.operators import static_output_pod as sop
    from orcapod.core.operators.base import (
        BinaryOperator,
        NonZeroInputOperator,
        UnaryOperator,
    )
    from orcapod.core.operators.join import Join
    from orcapod.core.sources import stream_builder
    from orcapod.core.sources.base import RootSource
    from orcapod.core.sources.spiraldb_table_source import SpiralDBTableSource
    from orcapod.core.streams.arrow_table_stream import ArrowTableStream
    from orcapod.databases.delta_lake_databases import DeltaTableDatabase
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    from orcapod.hashing import arrow_hashers
    from orcapod.utils import arrow_utils

    node_classes = [
        getattr(module, name)
        for module in (function_node, operator_node, source_node)
        for name in dir(module)
        if name.endswith(("Node", "NodeBase"))
        and isinstance(getattr(module, name), type)
        and getattr(module, name).__module__ == module.__name__
    ]
    for cls in node_classes:
        for name in ("execute", "async_execute"):
            if name in cls.__dict__:
                _wrap_node(cls, name)

    _wrap(Tag, "__init__", "Tag.new")
    _wrap(Data, "__init__", "Data.new")
    _wrap(ArrowTableStream, "__init__", "ats.new")
    _wrap(
        ArrowTableStream,
        "iter_data",
        lambda self: "ats.iter.cached"
        if self._cached_elements is not None
        else "ats.iter.build",
    )
    _wrap(RootSource, "iter_data", "source.iter")
    _wrap(SpiralDBTableSource, "iter_data", "source.iter")
    _wrap(SpiralDBTableSource, "_batch_streams", "spiral.scan")

    original_record_id = stream_builder._make_record_id

    def counted_record_id(*args, **kwargs):
        bump("record_id")
        return original_record_id(*args, **kwargs)

    stream_builder._make_record_id = counted_record_id

    for cls in node_classes:
        if "compute_base_entry_id" in cls.__dict__:
            _wrap(cls, "compute_base_entry_id", "base_entry_id")
    for cls_name in dir(arrow_hashers):
        cls = getattr(arrow_hashers, cls_name)
        if (
            isinstance(cls, type)
            and cls.__module__ == arrow_hashers.__name__  # not starfix's digester
            and "hash_table" in cls.__dict__
        ):
            _wrap(cls, "hash_table", "hash_table")

    for cls in (UnaryOperator, BinaryOperator, NonZeroInputOperator, Join):
        if "static_process" in cls.__dict__:
            _wrap(cls, "static_process", "op.static_process")
    _wrap(sop.StaticOutputOperatorPod, "process", "op.process")
    _wrap(sop.StaticOutputOperatorPod, "_rows_to_table", "rows_to_table")
    _wrap(sop.StaticOutputOperatorPod, "_materialize_to_stream", "materialize_to_stream")

    original_hstack = arrow_utils.hstack_tables

    def counted_hstack(*args, **kwargs):
        bump("hstack_tables")
        return original_hstack(*args, **kwargs)

    arrow_utils.hstack_tables = counted_hstack

    for db in (InMemoryArrowDatabase, DeltaTableDatabase):
        for name in ("add_record", "add_records"):
            if name in db.__dict__:
                _wrap(db, name, "db.write")
        for name in (
            "get_all_records",
            "get_records_by_ids",
            "get_record_by_id",
            "get_records_with_column_value",
        ):
            if name in db.__dict__:
                _wrap(db, name, "db.read")
        if "flush" in db.__dict__:
            _wrap(db, "flush", "db.flush")


METRICS = (
    "node.execute",
    "rows.out",
    "source.iter",
    "spiral.scan",
    "record_id",
    "base_entry_id",
    "hash_table",
    "Tag.new",
    "Data.new",
    "ats.new",
    "ats.iter.build",
    "ats.iter.cached",
    "op.process",
    "op.static_process",
    "rows_to_table",
    "materialize_to_stream",
    "hstack_tables",
    "fn.call",
    "db.read",
    "db.write",
    "db.flush",
)


def report(title: str, counts: dict[str, dict[str, int]]) -> str:
    """Format ``counts`` as a table: one column per node, one row per metric."""
    nodes = sorted(counts, key=lambda n: (n == "-", n))
    width = max(12, *(len(n) for n in nodes)) if nodes else 12
    lines = [f"### {title}", ""]
    header = f"{'metric':<22}" + "".join(f"{n:>{width + 2}}" for n in nodes)
    header += f"{'TOTAL':>{width + 2}}"
    lines.append(header)
    for metric in METRICS:
        values = [counts[n].get(metric, 0) for n in nodes]
        if not any(values):
            continue
        row = f"{metric:<22}" + "".join(f"{v:>{width + 2}}" for v in values)
        row += f"{sum(values):>{width + 2}}"
        lines.append(row)
    return "\n".join(lines) + "\n"
