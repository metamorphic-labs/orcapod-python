"""Completion proofs scoped to the exact graph inputs an orchestrator delivers.

Source identity hashes do not prove freshness. Only known immutable snapshots
qualify; direct node calls, unknown sources and incomplete runs never qualify.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import Callable
from contextvars import ContextVar
from typing import Any, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])


class _Identity:
    """Retain an object and compare by identity, never by its content hash."""

    def __init__(self, value: object) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Identity) and self.value is other.value


class _Run:
    """Freeze source bindings, configuration and graph edges before any callbacks."""

    def __init__(self, graph: Any, kinds: dict[str, tuple[type, ...]]) -> None:
        self.kinds = kinds
        self.succeeded: set[int] = set()
        self.done: dict[int, asyncio.Event] = {}
        self.nodes = {id(node): node for node in graph.topological_sort()}
        self.aligned = {}
        for node in self.nodes.values():
            upstreams = getattr(node, "upstreams", None)
            if upstreams is None:
                self.aligned[id(node)] = False
                continue
            order = {id(upstream): index for index, upstream in enumerate(upstreams)}
            predecessors = sorted(
                graph.predecessors(node), key=lambda p: order.get(id(p), 0)
            )
            self.aligned[id(node)] = tuple(id(p) for p in predecessors) == tuple(
                id(p) for p in upstreams
            )
        self.initial = {key: self.definition(node) for key, node in self.nodes.items()}

    def definition(self, stream: Any) -> object | None:
        """Describe the graph independently of whether its work has completed."""
        kind = type(stream)
        if kind in self.kinds["source_node"]:
            bound = self.definition(stream.bound_source)
            return None if bound is None else (_Identity(stream), bound)
        if kind in self.kinds["arrow"]:
            return (_Identity(stream), stream.last_modified, stream.data_context_key)
        if kind in self.kinds["memory_source"]:
            underlying = self.definition(stream._stream)
            return None if underlying is None else (_Identity(stream), underlying)
        if kind in self.kinds["spiral"] and stream._asof is not None:
            return (_Identity(stream), stream._asof, stream.data_context_key)
        if kind not in self.kinds["computed"]:
            return None
        upstreams = tuple(self.definition(s) for s in stream.upstreams)
        if not upstreams or any(s is None for s in upstreams):
            return None
        producer = stream.producer
        return (
            upstreams,
            getattr(stream, "_execution_generation", 0),
            _Identity(producer),
            producer.last_modified if producer is not None else None,
            _Identity(getattr(stream, "_node_config", None)),
            _Identity(getattr(stream, "_pipeline_database", None)),
            _Identity(getattr(stream, "_ephemeral_cached_pod", None)),
            stream.data_context_key,
        )

    def proof(self, node: Any) -> object | None:
        """Require unchanged run-start inputs and successfully completed predecessors."""
        key = id(node)
        if key not in self.nodes or not self.aligned[key]:
            return None
        definition = self.definition(node)
        if definition is None or definition != self.initial[key]:
            return None
        for upstream in node.upstreams:
            if type(upstream) in self.kinds["computed"]:
                proof = self.proof(upstream)
                if proof is None or proof != getattr(
                    upstream, "_completed_execution", None
                ):
                    return None
        return definition


_RUN: ContextVar[_Run | None] = ContextVar("execution_run", default=None)
_TICKET: ContextVar[object | None] = ContextVar("execution_ticket", default=None)
_ACTIVE: ContextVar[object | None] = ContextVar("execution_active", default=None)


def verified_graph_execution(method: F) -> F:
    """Freeze a graph before execution; node calls still require a one-use ticket."""

    def setup(args: tuple[Any, ...], kwargs: dict[str, Any]) -> _Run:
        orchestrator = args[0]
        graph = args[1] if len(args) > 1 else kwargs["graph"]
        return _Run(graph, orchestrator._execution_snapshot_types())

    if inspect.iscoroutinefunction(method):

        @functools.wraps(method)
        async def asynchronous(*args: Any, **kwargs: Any) -> Any:
            token = _RUN.set(setup(args, kwargs))
            try:
                return await method(*args, **kwargs)
            finally:
                _RUN.reset(token)

        return cast(F, asynchronous)

    @functools.wraps(method)
    def synchronous(*args: Any, **kwargs: Any) -> Any:
        token = _RUN.set(setup(args, kwargs))
        try:
            return method(*args, **kwargs)
        finally:
            _RUN.reset(token)

    return cast(F, synchronous)


def execute_node(
    node: Any, method: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    """Authorize one orchestrator-owned synchronous invocation."""
    token = _TICKET.set(node)
    try:
        result = method(*args, **kwargs)
        run = _RUN.get()
        if run is not None:
            run.succeeded.add(id(node))
        return result
    finally:
        _TICKET.reset(token)


async def async_execute_node(
    node: Any, method: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    """Authorize one orchestrator-owned asynchronous invocation."""
    token = _TICKET.set(node)
    run = _RUN.get()
    try:
        result = await method(*args, **kwargs)
        if run is not None:
            run.succeeded.add(id(node))
        return result
    finally:
        if run is not None:
            run.done.setdefault(id(node), asyncio.Event()).set()
        _TICKET.reset(token)


async def upstreams_succeeded(node: Any) -> bool:
    """Wait for producer tasks, since channel EOF can also mean producer failure."""
    run = _RUN.get()
    if run is None:
        return False
    for upstream in node.upstreams:
        await run.done.setdefault(id(upstream), asyncio.Event()).wait()
    return all(id(upstream) in run.succeeded for upstream in node.upstreams)


def is_current(node: Any) -> bool:
    """Whether retained work matches this exact trusted graph invocation."""
    run = _RUN.get()
    previous = getattr(node, "_completed_execution", None)
    return (
        _ACTIVE.get() is node
        and run is not None
        and previous is not None
        and previous == run.proof(node)
    )


def can_retain_rows(node: Any) -> bool:
    """Keep row memoization only within an unchanged, known graph snapshot."""
    run = _RUN.get()
    if run is None or _ACTIVE.get() is not node:
        return False
    initial = run.initial.get(id(node))
    return initial is not None and initial == run.definition(node)


def invalidate(node: Any) -> None:
    """Forget completion and row memoization after an explicit cache clear."""
    node._completed_execution = None
    node._execution_generation = getattr(node, "_execution_generation", 0) + 1
    node._sync_row_views = {}
    node._entry_id_rows = {}


class _TrackingObserver:
    """Forward hooks while remembering row failures across contextual observers."""

    def __init__(self, observer: Any, failures: list[bool]) -> None:
        self.observer = observer
        self.failures = failures

    def __getattr__(self, name: str) -> Any:
        return (
            getattr(self.observer, name)
            if self.observer is not None
            else lambda *a, **kw: None
        )

    def contextualize(self, *path: str) -> _TrackingObserver:
        return (
            _TrackingObserver(self.observer.contextualize(*path), self.failures)
            if self.observer is not None
            else self
        )

    def on_data_crash(self, *args: Any, **kwargs: Any) -> None:
        self.failures.append(True)
        if self.observer is not None:
            self.observer.on_data_crash(*args, **kwargs)


def publish_completion(node: Any, observer: Any) -> None:
    """Publish success before output closes, never after failed or changed work."""
    run = _RUN.get()
    node._completed_execution = (
        run.proof(node)
        if run is not None and _ACTIVE.get() is node and not observer.failures
        else None
    )
    if node._completed_execution is None:
        node._entry_id_rows = {}


def track_completion(method: F) -> F:
    """Track completion without changing per-row execution, persistence or hooks."""

    def setup(node: Any, kwargs: dict[str, Any]) -> _TrackingObserver:
        if not is_current(node):
            node._entry_id_rows = {}
        observer = _TrackingObserver(kwargs.get("observer"), [])
        kwargs["observer"] = observer
        return observer

    if inspect.iscoroutinefunction(method):

        @functools.wraps(method)
        async def asynchronous(node: Any, *args: Any, **kwargs: Any) -> Any:
            trusted = _TICKET.get() is node
            ticket = _TICKET.set(None)
            active = _ACTIVE.set(node if trusted else None)
            observer = setup(node, kwargs)
            try:
                result = await method(node, *args, **kwargs)
                publish_completion(node, observer)
                return result
            except BaseException:
                node._completed_execution = None
                node._entry_id_rows = {}
                raise
            finally:
                _ACTIVE.reset(active)
                _TICKET.reset(ticket)

        return cast(F, asynchronous)

    @functools.wraps(method)
    def synchronous(node: Any, *args: Any, **kwargs: Any) -> Any:
        trusted = _TICKET.get() is node
        ticket = _TICKET.set(None)
        active = _ACTIVE.set(node if trusted else None)
        observer = setup(node, kwargs)
        try:
            result = method(node, *args, **kwargs)
            publish_completion(node, observer)
            return result
        except BaseException:
            node._completed_execution = None
            node._entry_id_rows = {}
            raise
        finally:
            _ACTIVE.reset(active)
            _TICKET.reset(ticket)

    return cast(F, synchronous)
