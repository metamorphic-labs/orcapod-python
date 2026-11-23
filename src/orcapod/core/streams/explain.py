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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from orcapod.types.condition import Condition
from orcapod.types.explanation import Explanation, SourceReading, Step

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import StreamProtocol


def stream_name(stream: StreamProtocol) -> str:
    """Return how a stream is named in an explanation: ``Kind[label]``."""
    return f"{type(stream).__name__}[{stream.label}]"


def input_name(stream: StreamProtocol) -> str:
    """Return how an input of a step is named: by the sources it reads (their names,
    sorted; a slot's name for an unbound slot)."""
    names: set[str] = set()

    def walk(current: StreamProtocol) -> None:
        _, found = _beneath(current)
        if isinstance(found, SourceReading):
            names.add(found.name)
            return
        for upstream in found:
            walk(upstream)

    walk(stream)
    return ", ".join(sorted(names))


def _beneath(
    stream: StreamProtocol,
) -> tuple[str, tuple[StreamProtocol, ...] | SourceReading]:
    """Return what ``stream`` reads: its inputs, or the reading it ends in.

    A job's input slot reads its bound source, a proxy its delegate; a root source, a
    cached source and a stream with no producer and no input end the walk.
    """
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.sources.base import RootSource
    from orcapod.core.sources.source_proxy import SourceProxy
    from orcapod.pipeline.restrictions import source_name

    if isinstance(stream, (SourceJobNode, SourceProxy)):
        bound = (
            stream.bound_source
            if isinstance(stream, SourceJobNode)
            else stream.delegate
        )
        if bound is None:
            return "unbound", SourceReading(
                name=stream.label,
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


def _step_of(
    stream: StreamProtocol, position: int, inputs: Sequence[StreamProtocol]
) -> Step | None:
    """Return the step ``stream``'s operator takes on its input at ``position``."""
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod

    producer = stream.producer
    if not isinstance(producer, StaticOutputOperatorPod):
        return None
    others = [input_name(s) for i, s in enumerate(inputs) if i != position]
    text = producer.rows_dropped_text(position, sorted(others))
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
    # Each path from ``stream`` down to a source contributes the steps on it. A step
    # reached by several paths (a diamond) is listed once.
    readings: dict[tuple[str, str], tuple[SourceReading, list[tuple[int, Step]]]] = {}

    def walk(
        current: StreamProtocol, above: list[tuple[int, Step]], depth: int
    ) -> None:
        kind, found = _beneath(current)
        if isinstance(found, SourceReading):
            key = (found.name, found.kind)
            _, steps = readings.setdefault(key, (found, []))
            for item in above:
                if item not in steps:
                    steps.append(item)
            return
        inputs = found
        for position, upstream in enumerate(inputs):
            step = _step_of(current, position, inputs) if kind == "inputs" else None
            walk(
                upstream,
                above if step is None else [*above, (depth, step)],
                depth + 1,
            )

    walk(stream, [], 0)
    sources = []
    for key in sorted(readings):
        reading, steps = readings[key]
        # Nearest the source first: the deepest step first; ties in a fixed order.
        ordered = sorted(
            steps,
            key=lambda item: (-item[0], item[1].operator, item[1].label, item[1].text),
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
