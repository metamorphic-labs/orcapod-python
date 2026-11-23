"""What ``stream.explain()`` returns (ITL-1402): plain data with a text form.

``Explanation`` holds, for each source a stream reads, what the source applies itself and
which steps drop rows after reading. Built by ``orcapod.core.streams.explain``; nothing here
computes anything.
"""

from __future__ import annotations

from dataclasses import dataclass

from orcapod.types.condition import Condition


def plain_repr(value: object) -> str:
    """Return ``value`` as ``explain()`` writes a name or a key: by the ``repr`` of its
    built-in base type (``str``, ``bytes``, ``int``, ``float``, ``bool``), so a subclass's
    own ``__repr__`` never runs; any other value as ``<type name>``."""
    if isinstance(value, str):
        return str.__repr__(value)
    if isinstance(value, bytes):
        return bytes.__repr__(value)
    if isinstance(value, bool):  # before int: a bool is an int
        return bool.__repr__(value)
    if isinstance(value, int):
        return int.__repr__(value)
    if isinstance(value, float):
        return float.__repr__(value)
    return f"<{type(value).__name__}>"


@dataclass(frozen=True)
class Step:
    """A step above a source that drops some of the rows read.

    Attributes:
        operator: The operator's class name (``Filter``, ``DropNulls``, ``Join``).
        label: The step's label (the operator's name when none was given).
        text: What the step keeps or drops, in words (``t >= 1.0``; ``rows without a
            match in 'probes' are dropped``).
    """

    operator: str
    label: str
    text: str

    def describe(self) -> str:
        """Return the step as one phrase: ``t >= 1.0 (Filter[kept])``."""
        name = (
            self.operator
            if self.label == self.operator
            else f"{self.operator}[{self.label}]"
        )
        return f"{self.text} ({name})"


@dataclass(frozen=True)
class SourceReading:
    """One source of a stream: what it applies itself and what is applied after reading.

    Attributes:
        name: The source's name (its source id, or the slot's name for an unbound slot).
        kind: The source's class name (``SourceJobNode`` for an unbound slot).
        known: Whether the source kind says what it reads. A source that reads rows
            stored or held elsewhere (a derived or a cached source, a raw stream) does
            not: what it reads depends on what was stored there.
        bound: ``False`` for a job's input slot with no source bound.
        condition: The condition the source applies itself (``where``), empty for none.
        limit: The source's row limit, ``None`` for none.
        steps: The steps above the source that drop rows of it, nearest the source first.
    """

    name: str
    kind: str
    known: bool
    bound: bool
    condition: Condition
    limit: int | None
    steps: tuple[Step, ...]

    def lines(self) -> list[str]:
        """Return the reading as text lines (``Explanation.text``)."""
        after = "; ".join(step.describe() for step in self.steps) or "nothing"
        if not self.bound:
            # The steps are declared: show them although no source is bound yet.
            return [
                f"slot {self.name!r}: no source bound",
                f"  applied after reading: {after}",
            ]
        if not self.known:
            applied = "not known for this source kind"
        elif self.condition or self.limit is not None:
            parts = (
                [f"where {self.condition.canonical_text()}"] if self.condition else []
            )
            if self.limit is not None:
                parts.append(f"limit {self.limit}")
            applied = ", ".join(parts)
        else:
            applied = "nothing (the whole table is read)"
        return [
            f"source {self.name!r} ({self.kind})",
            f"  applied by the source: {applied}",
            f"  applied after reading: {after}",
        ]


@dataclass(frozen=True)
class Explanation:
    """What a stream's sources apply themselves, and what is applied after reading.

    New in this release (ITL-1402). Returned by ``stream.explain()``; plain data, built
    without computing, reading, polling or calling user code.

    Attributes:
        stream: The explained stream, as ``Kind[label]``.
        sources: One reading per source the stream reads, in source-name order.
    """

    stream: str
    sources: tuple[SourceReading, ...]

    def text(self) -> str:
        """Return the explanation as text: the stream, then one block per source."""
        lines = [f"{self.stream} reads:"]
        for reading in self.sources:
            lines.extend(f"  {line}" for line in reading.lines())
        return "\n".join(lines)

    def __str__(self) -> str:
        return self.text()

    def _repr_pretty_(self, printer: object, cycle: bool) -> None:  # pyright: ignore[reportUnusedParameter]  # IPython's display hook
        printer.text(self.text())  # pyright: ignore[reportAttributeAccessIssue]  # an IPython pretty printer
