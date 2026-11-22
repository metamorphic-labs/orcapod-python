"""Restrictions of sources: what a pipeline, a bind or a run does about them (ITL-1218).

A restriction is attached to a source and chooses which of its rows flow; it changes no
row, schema, DAG node or identity. A source states its own (``SourceProtocol.condition``
and ``SourceProtocol.row_limit``); only ``SpiralDBTableSource`` can be restricted today.
This module holds every rule that reads them:

- the walk from streams down to the sources they read (``restrictions_beneath``,
  ``sources_read``), through a slot's bound source, a proxy's delegate and a cache's
  source;
- the refusals: one source read under two restrictions in one pipeline
  (``require_one_restriction``), an operator node that caches over a restricted source
  (``refuse_restricted_operator_cache``), a ``CachedSource`` over one
  (``refuse_restricted_cache_input``, ``refuse_restricted_cache_config``);
- what a run reports (``restrictions_in_force``, ``restriction_text``) and warns about
  (``warn_restricted_aggregation``).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, TypeAlias

from orcapod.types import CacheMode, Condition

if TYPE_CHECKING:
    from orcapod.core.nodes.operator_node import OperatorNodeBase
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.sources.base import RootSource
    from orcapod.pipeline.dag import OrcaDAG
    from orcapod.protocols.core_protocols import StreamProtocol


Restriction: TypeAlias = tuple[Condition, int | None]
"""What a source reads under: its condition (the empty ``Condition`` for none) and its
row limit (``None`` for none). Two are one when the conditions are equal (``Condition``
equality) and the limits are the same. A pair, not a class: ``Condition`` is the one
restriction class, and a limit stays the source's own (developer, November 18, 2026)."""

RestrictedSource: TypeAlias = tuple[str, Condition, int | None]
"""A restricted source a run reads, as the run summary records it: its source id
(``source_name``), its condition and its limit."""


Rebinding: TypeAlias = "Sequence[tuple[SourceJobNode, StreamProtocol]]"
"""The sources a bind is about to bind: ``(slot, source)`` pairs, one per slot of the
job that the bind changes. A slot is matched by object identity, never by name: a
source being bound can itself be a slot of another job, of the same name."""


def source_name(source: RootSource) -> str:
    """A source's name for a report: its source id, or its label without one."""
    try:
        return str(source.source_id)
    except ValueError:
        return str(source.label)


def restriction_of(stream: StreamProtocol) -> Restriction | None:
    """Return the restriction a source reads under, or ``None`` for none.

    Read from the source's own ``condition`` and ``row_limit``. A stream that is not
    a source has no restriction of its own (the walks below reach the sources it
    reads); ``StreamProtocol`` gets the two members when restrictions reach general
    streams (ITL-1219).
    """
    from orcapod.core.sources.base import RootSource

    # Narrowed by class, not by the runtime ``SourceProtocol`` check: on Python 3.11
    # that check evaluates every member of the protocol.
    if not isinstance(stream, RootSource):
        return None
    condition, limit = stream.condition, stream.row_limit
    if not condition and limit is None:
        return None
    return (condition, limit)


def restriction_text(condition: Condition | None, limit: int | None) -> str:
    """A source's restriction as words: ``where a='x', b=1, limit 3``.

    Args:
        condition: The condition the source reads under, if any.
        limit: The source's limit, if any.

    Returns:
        The condition's text (``Condition.text``) after ``where``, then the limit.
    """
    parts: list[str] = []
    if condition:
        parts.append(f"where {condition.text()}")
    if limit is not None:
        parts.append(f"limit {limit}")
    return ", ".join(parts) if parts else "no restriction"


def _inputs(stream: StreamProtocol) -> tuple[StreamProtocol, ...]:
    """Return ``stream``'s ``upstreams`` that exist.

    A node of a loaded job whose input was not restored (the saved file records a node
    of a kind ``PipelineJob.load`` does not restore) holds ``None`` in its place; it
    reads nothing there, so the walks skip it, and the run refuses that node with its
    own error (``PipelineJob`` names the node that lost its input).
    """
    upstreams: tuple[StreamProtocol | None, ...] = stream.upstreams
    return tuple(upstream for upstream in upstreams if upstream is not None)


def _streams_beneath(
    current: StreamProtocol, rebinding: Rebinding | None = None
) -> tuple[StreamProtocol, ...]:
    """Return what ``current`` reads: the streams a restriction walk goes on to.

    A ``SourceJobNode`` reads its bound source (or, when it is one of the slots of
    ``rebinding``, the source a bind is about to bind there), a bound
    ``SourceProxy`` its delegate, a ``CachedSource`` the source it caches; any other
    stream its ``upstreams``. A ``DerivedSource`` is a root: it reads the rows stored
    for its origin, never its origin's sources, so the walk ends there, as at every
    other root source.
    """
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.source_proxy import SourceProxy

    if isinstance(current, SourceJobNode):
        bound = current.bound_source
        for slot, source in rebinding or ():
            if slot is current:
                bound = source
        return () if bound is None else (bound,)
    if isinstance(current, SourceProxy):
        return () if current.delegate is None else (current.delegate,)
    if isinstance(current, CachedSource):
        return (current.cached_source,)
    return _inputs(current)


def restrictions_beneath(
    streams: Iterable[StreamProtocol],
    rebinding: Rebinding | None = None,
) -> frozenset[tuple[str, Restriction]]:
    """Return the restrictions of every source the streams read, order-free.

    Walks down to the root sources (``_streams_beneath``: through a slot's bound
    source, a proxy's delegate and a cache's source); each restricted root gives
    ``(source name, restriction)``. Two results are equal when the same sources are
    restricted equally.

    Args:
        streams: Where the walk starts.
        rebinding: The job's slots a bind is about to bind, each with its source,
            read in place of the slot's current source (``PipelineJob.bind`` checks
            before it binds).
    """
    from orcapod.core.sources.base import RootSource

    found: set[tuple[str, Restriction]] = set()
    visited: list[StreamProtocol] = []
    pending = list(streams)
    while pending:
        current = pending.pop()
        if any(current is seen for seen in visited):
            continue
        visited.append(current)
        restriction = restriction_of(current)
        if restriction is not None and isinstance(current, RootSource):
            found.add((source_name(current), restriction))
        pending.extend(_streams_beneath(current, rebinding))
    return frozenset(found)


def restrictions_text(found: frozenset[tuple[str, Restriction]]) -> str:
    """The restrictions found beneath streams, as words for a message."""
    if not found:
        return "no restriction"
    return "; ".join(
        sorted(
            f"{name}: {restriction_text(*restriction)}" for name, restriction in found
        )
    )


def sources_read(
    streams: Iterable[StreamProtocol], rebinding: Rebinding | None = None
) -> list[tuple[str, str, Restriction | None]]:
    """Return every root source the streams read, with its restriction.

    ``(content hash, source name, restriction or None)`` for each root source met by
    ``_streams_beneath``, once per object. The content hash is the source's identity,
    which a restriction leaves unchanged, so it is the key of the one-restriction
    rule. ``rebinding``: the job's slots a bind is about to bind, with their sources.
    """
    from orcapod.core.sources.base import RootSource
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.source_proxy import SourceProxy

    found: list[tuple[str, str, Restriction | None]] = []
    visited: list[StreamProtocol] = []
    pending = list(streams)
    while pending:
        current = pending.pop()
        if any(current is seen for seen in visited):
            continue
        visited.append(current)
        beneath = _streams_beneath(current, rebinding)
        if (
            isinstance(current, RootSource)
            and not isinstance(current, (CachedSource, SourceProxy))
            and not beneath
        ):
            found.append(
                (
                    current.content_hash().to_string(),
                    source_name(current),
                    restriction_of(current),
                )
            )
        pending.extend(beneath)
    return found


def require_one_restriction(
    streams: Iterable[StreamProtocol], rebinding: Rebinding | None = None
) -> None:
    """Refuse a source read under two different restrictions in one pipeline.

    A restriction (ITL-1218) does not change a source's identity, so a pipeline
    holds one restriction (or none) per source identity, whatever streams lead to
    it (projections, operator chains, caches, proxies). The check reads what each
    stream reads now (``sources_read``) and keeps nothing between calls: a slot
    rebound since an earlier check changes what the streams over it read, so a
    remembered answer could be wrong. Checked at capture, at compile, by a bind
    before it binds and at the start of every run (``refuse_restriction_conflicts``),
    since a slot bound to a stream over a source can read a source another slot
    reads.

    Args:
        streams: Every stream the pipeline holds, with the inputs of the invocation
            being recorded; or the nodes of a job or a run.
        rebinding: The job's slots a bind is about to bind, with their sources.

    Raises:
        ValueError: If one source is read under two different restrictions (or
            with and without one).
    """
    seen: dict[str, Restriction | None] = {}
    for key, name, restriction in sources_read(streams, rebinding):
        if key not in seen:
            seen[key] = restriction
            continue
        earlier = seen[key]
        if earlier != restriction:
            first = restriction_text(*earlier) if earlier else "no restriction"
            second = restriction_text(*restriction) if restriction else "no restriction"
            raise ValueError(
                "Two streams read different rows in one pipeline: the source "
                f"{name!r} is read with two different restrictions ({first} and "
                f"{second}). A restriction does not change a source's identity, so a "
                "pipeline holds one restriction per source: use one restriction for "
                "the table in this pipeline, or build a separate pipeline."
            )


# TODO(ITL-1219): the redesign of how an operator node logs and replays: load the
# cached rows and then apply the restriction. Until then a caching operator node over a
# restricted source is refused (developer, November 19, 2026).
def refuse_restricted_operator_cache(
    node: OperatorNodeBase,
    cache_mode: CacheMode,
    rebinding: Rebinding | None = None,
    *,
    streams: Iterable[StreamProtocol] | None = None,
) -> None:
    """Refuse an operator node that caches (LOG or REPLAY) over a restricted source.

    An operator node keeps its cache under its own identity, which a restriction
    (ITL-1218) is not part of, so a log written from a restricted read would be
    replayed for another restriction or for none. The developer's decision
    (November 19, 2026): the refusal is the behaviour for now; how a node logs and
    replays is to be redesigned so that it loads its cache and then applies the
    restriction (ITL-1219). ``CacheMode.OFF`` caches nothing and is never refused;
    function nodes store per row and are not concerned.

    Args:
        node: The operator node.
        cache_mode: The cache mode the node has or is about to get.
        rebinding: The job's slots a bind is about to bind, with their sources.
        streams: The inputs the node is about to read, when they are not (yet) its
            ``upstreams``: streams handed to ``execute``, or a new ``upstreams``.

    Raises:
        ValueError: If ``cache_mode`` caches and a source feeding the node is
            restricted (a condition or a limit), naming the node and each
            restriction.
    """
    if cache_mode == CacheMode.OFF:
        return
    inputs = streams if streams is not None else _inputs(node)
    found = restrictions_beneath(inputs, rebinding)
    if not found:
        return
    raise ValueError(
        f"An operator cache keeps its rows under the node's identity; operator node "
        f"{node.label!r} (CacheMode.{cache_mode.name}) cannot cache a restricted read "
        f"({restrictions_text(found)}): a log written from it would be replayed for "
        "another restriction or none. Set the node's cache mode to CacheMode.OFF, or "
        "read the unrestricted source."
    )


def refuse_restricted_operator_caches(
    streams: Iterable[StreamProtocol],
    rebinding: Rebinding | None = None,
) -> None:
    """Refuse every caching operator job node at or above ``streams`` over a restriction.

    Walks ``upstreams`` (not into a slot's bound source, whose own nodes are not
    the job's) and checks each ``OperatorJobNode`` found with
    ``refuse_restricted_operator_cache``. Called before a job distributes its
    databases, and through ``refuse_restriction_conflicts`` by ``PipelineJob.bind``
    before it binds and at the start of ``job.run()``, ``run_async()`` and a job
    node's ``run()``, before any source is checked or anything computed or written.

    Args:
        streams: The nodes of a run or a job, or the node whose ``run()`` it is.
        rebinding: The job's slots a bind is about to bind, with their sources.

    Raises:
        ValueError: For the first caching operator node over a restricted source.
    """
    from orcapod.core.nodes.operator_node import OperatorJobNode
    from orcapod.core.nodes.source_node import SourceJobNode

    visited: list[StreamProtocol] = []
    pending = list(streams)
    while pending:
        current = pending.pop(0)
        if any(current is seen for seen in visited):
            continue
        visited.append(current)
        if isinstance(current, OperatorJobNode):
            refuse_restricted_operator_cache(current, current.cache_mode, rebinding)
        if not isinstance(current, SourceJobNode):
            pending.extend(_inputs(current))


def refuse_restriction_conflicts(
    streams: Iterable[StreamProtocol], rebinding: Rebinding | None = None
) -> None:
    """Refuse what a bind or a run cannot do over restricted sources (ITL-1218).

    One source read under two restrictions (``require_one_restriction``) and a
    caching operator node over a restricted source
    (``refuse_restricted_operator_caches``). Called by ``PipelineJob.bind`` before it
    binds, and at the start of ``job.run()``, ``run_async()`` and a job node's
    ``run()``, before any source is checked or anything computed or written.

    Args:
        streams: The nodes of a job or a run, or the node whose ``run()`` it is.
        rebinding: The job's slots a bind is about to bind, with their sources.

    Raises:
        ValueError: For the first conflict found.
    """
    streams = list(streams)
    require_one_restriction(streams, rebinding)
    refuse_restricted_operator_caches(streams, rebinding)


def _restricted_cache_error(text: str) -> ValueError:
    """The refusal of a cache over a restricted read, ``text`` naming the restriction."""
    return ValueError(
        "A cache keeps one table's rows under the table's identity; it cannot hold a "
        f"restricted read ({text}): cache the unrestricted source, or read without a "
        "cache."
    )


def refuse_restricted_cache_input(source: StreamProtocol) -> None:
    """Refuse a cache over a source that reads under a restriction.

    Also through a proxy or a slot: a ``CachedSource`` keeps its rows under the
    table's identity, which a restriction is not part of (developer, November 18,
    2026).

    Raises:
        ValueError: If ``source``, or a source it reads (a proxy's delegate, a
            cached source), is restricted (ITL-1218).
    """
    found = restrictions_beneath([source])
    if found:
        raise _restricted_cache_error(restrictions_text(found))


def refuse_restricted_cache_config(config: Mapping[str, object]) -> None:
    """Refuse a saved source config that names a restriction (a ``where`` or a limit).

    Read from the config itself, before the source is rebuilt (or replaced by a
    ``SourceProxy``, which carries no restriction).

    Raises:
        ValueError: If the config names a non-empty ``where`` or a ``limit``.
    """
    where = config.get("where")
    condition = Condition.from_config(where) if isinstance(where, Mapping) else None
    saved_limit = config.get("limit")
    limit = saved_limit if isinstance(saved_limit, int) else None
    if condition or saved_limit is not None:
        name = config.get("source_id") or config.get("source_type", "source")
        raise _restricted_cache_error(f"{name}: {restriction_text(condition, limit)}")


def restrictions_in_force(
    streams: Iterable[StreamProtocol],
) -> dict[str, RestrictedSource]:
    """The restriction of each restricted source feeding ``streams`` (ITL-1218).

    A restriction belongs to the source object bound to a slot, so the key is the
    slot's name (the ``SourceJobNode``'s, the name ``bind`` takes); a source read
    directly by the node whose ``run()`` it is has no slot and is keyed by its label.
    A slot bound to a stream over a source (a pod's or an operator's stream, a proxy,
    a cache) reports that source under the slot's name. Two slots bound to one table
    with different conditions are both reported. A slot keeps its name; a direct
    source is numbered (``"<label> (2)"``) only when its label is a slot's name or
    another direct source's, whatever the order the walk meets them.

    Known limit (ITL-1293): a slot keeps one entry. A slot bound to a stream that
    reads two restricted tables (a ``semi_join`` of two restricted sources) reports
    one source id, one condition and one limit: those of the first table the walk
    meets. Every check (the operator-cache refusal, the
    one-restriction rule) still reads both.

    Only a ``SpiralDBTableSource`` can be restricted today (``restriction_of``;
    every source kind when restrictions are generalized, ITL-1219).

    Args:
        streams: The nodes of a run, or the node whose ``run()`` it is.

    Returns:
        Slot name to the restricted source (its id, condition and limit), for the
        sources that restrict their rows: slots in the order first met, then direct
        sources. ``RunSummary`` keeps the three apart (``restricted_source_ids``,
        ``conditions``, ``limits``).
    """
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.sources.base import RootSource

    slots: dict[str, RestrictedSource] = {}
    direct: list[tuple[str, RestrictedSource]] = []
    visited: list[tuple[StreamProtocol, str | None]] = []
    pending: list[tuple[StreamProtocol, str | None]] = [
        (stream, None) for stream in streams
    ]
    while pending:
        stream, slot = pending.pop(0)
        # One object reached through two slots is reported under each.
        if any(stream is seen and slot == seen_slot for seen, seen_slot in visited):
            continue
        visited.append((stream, slot))
        restriction = restriction_of(stream)
        if restriction is not None and isinstance(stream, RootSource):
            entry: RestrictedSource = (source_name(stream), *restriction)
            if slot is not None:
                _ = slots.setdefault(slot, entry)
            elif (str(stream.label), entry) not in direct:
                direct.append((str(stream.label), entry))
        if isinstance(stream, SourceJobNode):
            pending.extend((inner, stream.name) for inner in _streams_beneath(stream))
        else:
            # A slot bound to a stream over a source keeps its name down to it.
            pending.extend((inner, slot) for inner in _streams_beneath(stream))
    found = dict(slots)
    for label, entry in direct:
        if found.get(label) == entry:
            continue  # the slot's own source, also read directly: reported once
        key, n = label, 2
        while key in found:
            key, n = f"{label} ({n})", n + 1
        found[key] = entry
    return found


def warn_restricted_aggregation(dag: OrcaDAG[StreamProtocol]) -> None:
    """Warn once when a restricted source feeds a ``GroupBy``, ``Batch`` or ``MergeJoin``.

    Called by ``job.run()`` and ``run_async()`` before the run (ITL-1218). The
    warning names each source slot and aggregating step, at the user's line that
    started the run, at every run.

    Args:
        dag: The job's graph of the run.
    """
    import sys

    from orcapod.core.operators import Batch, GroupBy, MergeJoin
    from orcapod.errors import RestrictedAggregationWarning
    from orcapod.utils.warning_utils import outside_orcapod_stacklevel, warn_at_frame

    pairs: list[str] = []
    for node in dag.topological_sort():
        step = node.producer
        if not isinstance(step, (GroupBy, Batch, MergeJoin)):
            continue
        restricted = sorted(restrictions_in_force(dag.ancestors(node)))
        pairs.extend(
            f"{slot!r} feeds {type(step).__name__} {node.label!r}"
            for slot in restricted
        )
    if not pairs:
        return
    caller = sys._getframe(outside_orcapod_stacklevel() - 1)  # pyright: ignore[reportPrivateUsage]  # as the warnings module does
    warn_at_frame(
        "A restricted source feeds an aggregating step: "
        + "; ".join(pairs)
        + ". Its groups are built from the rows the restriction lets through only "
        "(each aggregated row lists its members).",
        RestrictedAggregationWarning,
        caller,
    )
