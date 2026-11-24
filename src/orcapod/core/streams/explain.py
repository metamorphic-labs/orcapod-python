"""``stream.explain()``: what each source applies itself, and what is applied after reading.

A user cannot tell from a pipeline whether a selection is applied by the source (a
``where`` restriction, which reads little) or after the source's rows are read (a
``filter`` step, which on an unrestricted table reads every row). ``explain()`` says which
(ITL-1402, decision 9 of ITL-1374).

It walks the stream's chain down to its sources and reads only what the chain declares:
each source's restriction (``condition``, ``row_limit``), each operator's own arguments
and the names of the steps. It never computes, reads a row, polls, writes or calls user
code. The result is plain data (``Explanation``) with a text form; the text is the same
whatever order the sources, the inputs or an argument's items were given in.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from orcapod.types.condition import Condition
from orcapod.types.explanation import Explanation, SourceReading, Step

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import StreamProtocol


def stream_name(stream: StreamProtocol) -> str:
    """Return how a stream is named in an explanation: ``Kind[label]``."""
    return f"{type(stream).__name__}[{stream.label}]"


def _beneath(
    stream: StreamProtocol,
) -> tuple[str, tuple[StreamProtocol, ...] | SourceReading]:
    """Return what ``stream`` reads: its inputs, or the reading it ends in.

    A job's input slot reads its bound source, a proxy its delegate; a root source, a
    cached source and a stream with no producer and no input end the walk.
    """
    from orcapod.core.nodes.source_node import SourceJobNode, SourceNodeBase
    from orcapod.core.sources.base import RootSource
    from orcapod.core.sources.source_proxy import SourceProxy
    from orcapod.pipeline.restrictions import source_name

    if isinstance(stream, (SourceNodeBase, SourceProxy)):
        # A job's slot reads its bound source, a proxy its delegate; a pipeline's
        # ``SourceNode`` declares a slot and never holds a source.
        if isinstance(stream, SourceJobNode):
            bound = stream.bound_source
        elif isinstance(stream, SourceProxy):
            bound = stream.delegate
        else:
            bound = None
        if bound is None:
            return "unbound", SourceReading(
                # A slot by its name, the key a source is bound under; a proxy by its
                # label.
                name=stream.name
                if isinstance(stream, SourceNodeBase)
                else stream.label,
                kind=type(stream).__name__,
                known=False,
                bound=False,
                condition=Condition(),
                limit=None,
                steps=(),
            )
        return "inputs", (bound,)
    if isinstance(stream, RootSource):
        return "source", SourceReading(
            name=source_name(stream),
            kind=type(stream).__name__,
            known=stream.REPORTS_WHAT_IT_READS,
            bound=True,
            condition=stream.condition,
            limit=stream.row_limit,
            steps=(),
        )
    upstreams = tuple(u for u in stream.upstreams if u is not None)
    if not upstreams:
        # A raw stream: rows held as given, from nowhere it names.
        return "source", SourceReading(
            name=stream.label,
            kind=type(stream).__name__,
            known=False,
            bound=True,
            condition=Condition(),
            limit=None,
            steps=(),
        )
    return "inputs", upstreams


_ReadingKey = tuple[str, str, bool, bool, Condition, "int | None"]
"""A source as an explanation tells sources apart: name, kind, whether it says what it
reads, whether it is bound, and its restriction (two restrictions of one table are two
readings)."""


def _reading_key(reading: SourceReading) -> _ReadingKey:
    return (
        reading.name,
        reading.kind,
        reading.known,
        reading.bound,
        reading.condition,
        reading.limit,
    )


def _sort_key(key: _ReadingKey) -> tuple[str, str, bool, bool, str, int]:
    name, kind, known, bound, condition, limit = key
    return (
        name,
        kind,
        known,
        bound,
        condition.canonical_text(),
        -1 if limit is None else limit,
    )


# Per source a node reaches: the source's reading, the shortest path its rows take
# to the node's output (None when they only choose rows), and the steps on the way,
# each by the node and text that made it, with that node's distance from the source.
_Reach = dict[
    _ReadingKey,
    tuple[SourceReading, int | None, dict[tuple[int, str], tuple[int, Step]]],
]


def _shown_names(stream: StreamProtocol) -> dict[str, str]:
    """Return the text each labelled source ``stream`` reads is shown by, by source id.

    A source read under two labels (or with and without one) is shown by its id.
    """
    from orcapod.core.sources.display_names import (
        LabelledSource,
        display_names,
        labelled_source,
    )

    found: dict[str, set[LabelledSource | None]] = {}
    seen: set[int] = set()
    pending = [stream]
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        _, beneath = _beneath(current)
        if isinstance(beneath, SourceReading):
            found.setdefault(beneath.name, set()).add(labelled_source(current))
        else:
            pending.extend(beneath)
    one = {name: next(iter(found[name])) for name in found if len(found[name]) == 1}
    return display_names(
        {name: source for name, source in one.items() if source is not None},
        set(found),
    )


def _reach(
    stream: StreamProtocol, memo: dict[int, _Reach], shown: Mapping[str, str]
) -> _Reach:
    """Return the sources ``stream`` reads and the steps above each, once per node.

    Each node is expanded once (``memo``), so a graph whose nodes share inputs costs one
    visit per node, not one per path. A step is identified by the node that takes it
    (and its text, as a ``SemiJoin`` drops differently on each input), so a step reached
    by several paths is listed once, at its shortest distance from the source.
    """
    from orcapod.core.operators.semijoin import SemiJoin

    known = memo.get(id(stream))
    if known is not None:
        return known
    kind, found = _beneath(stream)
    if isinstance(found, SourceReading):
        result: _Reach = {_reading_key(found): (found, 0, {})}
        memo[id(stream)] = result
        return result
    inputs = found
    reaches = [_reach(upstream, memo, shown) for upstream in inputs]
    producer = stream.producer if kind == "inputs" else None
    result = {}
    for position, reach in enumerate(reaches):
        step = _step_of(stream, producer, position, inputs, reaches, shown)
        chooses_only = isinstance(producer, SemiJoin) and position == 1
        for key, (reading, distance, steps) in reach.items():
            here = None if distance is None else distance + 1
            _, kept, merged = result.get(key, (reading, None, {}))
            merged = dict(merged)
            for step_key, (step_distance, kept_step) in steps.items():
                previous = merged.get(step_key)
                if previous is None or step_distance < previous[0]:
                    merged[step_key] = (step_distance, kept_step)
            if step is not None and here is not None:
                step_key = (id(stream), step.text)
                previous = merged.get(step_key)
                if previous is None or here < previous[0]:
                    merged[step_key] = (here, step)
            if not chooses_only and here is not None and (kept is None or here < kept):
                kept = here
            result[key] = (reading, kept, merged)
    memo[id(stream)] = result
    return result


def _step_of(
    stream: StreamProtocol,
    producer: object,
    position: int,
    inputs: Sequence[StreamProtocol],
    reaches: Sequence[_Reach],
    shown: Mapping[str, str],
) -> Step | None:
    """Return the step ``stream``'s operator takes on its input at ``position``; the
    other inputs are named by the sources they read (sorted, joined)."""
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod

    if not isinstance(producer, StaticOutputOperatorPod):
        return None
    others = sorted(
        ", ".join(
            shown.get(name, name)
            for name in sorted({reading.name for reading, _, _ in reach.values()})
        )
        for i, reach in enumerate(reaches)
        if i != position
    )
    text = producer.rows_dropped_text(position, others, inputs)
    if text is None:
        return None
    return Step(operator=type(producer).__name__, label=stream.label, text=text)


def explain(stream: StreamProtocol) -> Explanation:
    """Return what ``stream``'s sources apply themselves and what is applied after.

    Args:
        stream: The stream (a stream, a source or a job node).

    Returns:
        The explanation; nothing is computed, read, polled or written.
    """
    shown = _shown_names(stream)
    reach = _reach(stream, {}, shown)
    sources = []
    for key in sorted(reach, key=_sort_key):
        reading, _, steps = reach[key]
        # Nearest the source first; ties in a fixed order.
        ordered = sorted(
            steps.values(),
            key=lambda item: (item[0], item[1].operator, item[1].label, item[1].text),
        )
        sources.append(
            SourceReading(
                name=reading.name,
                kind=reading.kind,
                known=reading.known,
                bound=reading.bound,
                condition=reading.condition,
                limit=reading.limit,
                steps=tuple(step for _, step in ordered),
                shown=shown.get(reading.name, ""),
            )
        )
    return Explanation(stream=stream_name(stream), sources=tuple(sources))


def preview_line(stream: StreamProtocol) -> str | None:
    """Return the one-line split a preview shows for a ``Filter`` directly over a source.

    ``applied by the source: where session='s1'; after reading: t >= 1.0``, or
    ``applied by the source: nothing; after reading: t >= 1.0 (the whole table is
    read)``. ``None`` for any other stream. Reads what ``explain()`` reads, nothing more.

    Args:
        stream: The stream whose head is shown.

    Returns:
        The line, or ``None``.
    """
    from orcapod.core.operators.filter import Filter

    producer = stream.producer
    upstreams = stream.upstreams
    if not isinstance(producer, Filter) or len(upstreams) != 1:
        return None
    kind, found = _beneath(upstreams[0])
    if kind != "source" or not isinstance(found, SourceReading):
        return None
    after = producer.condition.canonical_text()
    if not found.known:
        return f"applied by the source: not known for this source kind; after reading: {after}"
    if not found.condition and found.limit is None:
        return (
            f"applied by the source: nothing; after reading: {after} "
            "(the whole table is read)"
        )
    applied = found.lines()[1].removeprefix("  applied by the source: ")
    return f"applied by the source: {applied}; after reading: {after}"
