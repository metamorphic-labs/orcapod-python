"""What the provenance tools return (ITL-1432): plain data with a text form.

``node.trace(row, column)`` returns a ``Trace``: one cell of a job node traced back, step
by step, through the job's nodes to the source cells it was made from. Each function step
says which function ran and whether this row's entry computed the result or reused one
(``FunctionRun``). ``node.why_missing(source, record_id)`` returns an ``Absence``: whether
rows of one source record reached the node, and if not, which step removed them and why
(a ``Step``, in ``explain()``'s words). Both offer a ``Highlight`` for
``job.diagram(highlight=...)``.

Built by ``orcapod.pipeline.provenance``; nothing here computes anything.
"""

from __future__ import annotations

import html
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from orcapod.types.explanation import Step, plain_repr


def _value_text(value: Any) -> str:
    """A cell value as the text form shows it: a list by its items, a float by repr."""
    if isinstance(value, list):
        items = [_value_text(v) for v in value]  # pyright: ignore[reportUnknownVariableType]
        return "[" + ", ".join(items) + "]"
    if isinstance(value, (str, bytes, bool, int, float)):
        return plain_repr(value)
    if value is None:
        return "None"
    return f"<{type(value).__name__}>"


def _tags_text(tags: tuple[tuple[str, Any], ...]) -> str:
    return ", ".join(f"{name}={_value_text(value)}" for name, value in tags)


def _time_text(moment: datetime | None) -> str:
    return "unknown time" if moment is None else moment.strftime("%Y-%m-%d %H:%M:%S %Z")


@dataclass(frozen=True)
class Highlight:
    """Nodes and edges to emphasize on a job's diagram, with a note.

    In development: this interface may change.

    Attributes:
        nodes: The labels of the nodes to emphasize.
        edges: The streams to emphasize, as ``(upstream label, downstream label)``.
        note: A line shown with the diagram (its listing and its picture's title).
    """

    nodes: frozenset[str] = frozenset()
    edges: frozenset[tuple[str, str]] = frozenset()
    note: str = ""


@dataclass(frozen=True)
class SourceCell:
    """One cell of a source, where a traced value came from.

    In development: this interface may change.

    Attributes:
        node: The label of the job's source node.
        source_id: The source's id (the first part of the source-info token).
        record_id: The record id of the source row (``row_<n>``, or the key columns as
            JSON).
        column: The source's column.
        value: The value, as the row that carried the token holds it.
    """

    node: str
    source_id: str
    record_id: str
    column: str
    value: Any

    def text(self) -> str:
        """Return the cell as one phrase: ``units row_40 n_spikes = 575``."""
        return f"{self.source_id} {self.record_id} {self.column} = {_value_text(self.value)}"


@dataclass(frozen=True)
class FunctionRun:
    """Which function produced a function node's value for one input row, and how.

    In development: this interface may change.

    Read from the node's stored pipeline entry (why: which input row) and its stored
    result (which function, which computation).

    Attributes:
        function: The function's name.
        version: The function's version (``v0.0``).
        body_hash: The hash of the function's body when it ran, with its label.
        executor: The executor that ran it (``local``).
        python_version: The Python version it ran on.
        computed_at: When the result was computed.
        computed_here: ``True`` if this row's entry ran the function; ``False`` if it
            reused a result stored for equal input values (memoized).
        result_id: The stored result's id (hex), as in the output's source-info token.
        entry_id: The pipeline entry's id, with its label.
        run_id: The id of the job run that recorded the entry, when stored.
        recorded_at: When the entry was recorded, when stored.
    """

    function: str
    version: str
    body_hash: str
    executor: str
    python_version: str
    computed_at: datetime | None
    computed_here: bool
    result_id: str
    entry_id: str
    run_id: str | None = None
    recorded_at: datetime | None = None

    def text(self) -> str:
        """Return the run as one phrase."""
        how = "computed here" if self.computed_here else "reused a stored result"
        body = self.body_hash.rpartition(":")
        short_body = f"{body[0]}:{body[2][:8]}" if body[1] else self.body_hash[:8]
        parts = [
            f"{self.function} {self.version} (body {short_body})",
            how,
            f"computed {_time_text(self.computed_at)} on {self.executor}",
        ]
        if self.run_id is not None:
            parts.append(f"run {self.run_id}")
        if self.recorded_at is not None:
            parts.append(f"recorded {_time_text(self.recorded_at)}")
        return ", ".join(parts)


@dataclass(frozen=True)
class TraceStep:
    """One node's part in a trace: the cell there and the cells it came from.

    In development: this interface may change.

    Attributes:
        node: The node's label.
        kind: ``"source"``, ``"operator"`` or ``"function"``.
        operator: The operator's class name for an operator step, else ``None``.
        column: The column of the cell at this node.
        value: The cell's value at this node.
        tags: The row's user tags, in the node's tag order.
        member: For a member of a group or batch, its position in the folded lists;
            else ``None``.
        status: ``"traced"``; ``"not_computed"`` when the node holds no run, so
            nothing below it can be read (``run()`` it first); or ``"unavailable"``
            when it is a source that is not read (a cached, polling, derived or
            user-defined source), so the cell's value is not known.
        function: For a function step, which function produced the value and how.
        source: For a source step, the source cell.
        inputs: The steps the value came from, at the nodes feeding this one.
    """

    node: str
    kind: str
    operator: str | None
    column: str
    value: Any
    tags: tuple[tuple[str, Any], ...]
    member: int | None = None
    status: str = "traced"
    function: FunctionRun | None = None
    source: SourceCell | None = None
    inputs: tuple[TraceStep, ...] = ()

    def walk(self) -> Iterator[TraceStep]:
        """Yield this step, then every step below it, depth first."""
        yield self
        for step in self.inputs:
            yield from step.walk()

    def passes_through(self) -> bool:
        """Whether the step only passes one input's cell on (a row-wise operator)."""
        return (
            self.kind == "operator"
            and self.status == "traced"
            and len(self.inputs) == 1
            and self.inputs[0].member is None
        )

    def lines(self, depth: int = 0, via: tuple[str, ...] = ()) -> list[str]:
        """Return the step and the steps below it as indented text lines.

        A row-wise operator that only passes a cell on is not given a line of its own:
        the line of the cell it passes names it (``via Join, Filter``).

        Args:
            depth: The indentation level.
            via: The operators passed through above this step, nearest first.

        Returns:
            The lines.
        """
        if self.passes_through():
            return self.inputs[0].lines(depth, (*via, self.operator or self.node))
        pad = "  " * depth
        where = f"[{self.member}] " if self.member is not None else ""
        through = f"  (via {', '.join(via)})" if via else ""
        if self.source is not None:
            head = f"{pad}{where}{self.source.text()}{through}"
        else:
            name = self.node
            if self.operator is not None and self.operator != self.node:
                name = f"{self.operator}[{self.node}]"
            tags = f" ({_tags_text(self.tags)})" if self.tags else ""
            head = f"{pad}{where}{name}{tags}: {self.column} = {_value_text(self.value)}{through}"
        out = [head]
        if self.status == "not_computed":
            out.append(f"{pad}  not computed: run() the job first")
        elif self.status == "unavailable":
            out.append(f"{pad}  not read: the source's rows are not held")
        if self.function is not None:
            out.append(f"{pad}  {self.function.text()}")
        for step in self.inputs:
            out.extend(step.lines(depth + 1))
        return out


@dataclass(frozen=True)
class Trace:
    """One cell of a job node traced back to the source cells it was made from.

    In development: this interface may change.

    Returned by ``node.trace(row, column)``; plain data, built by reading what the job's
    nodes hold and what their databases store, without computing, calling user code,
    polling or writing.

    Attributes:
        root: The traced cell's step; its ``inputs`` lead to the sources.
        edges: Every stream the trace follows, as ``(upstream label, downstream
            label)``, sorted.
    """

    root: TraceStep
    edges: tuple[tuple[str, str], ...] = field(default=())

    @property
    def status(self) -> str:
        """``"traced"``; ``"not_computed"`` when a node on the way holds no run; or
        ``"unavailable"`` when a source cell on the way is not read."""
        statuses = {step.status for step in self.root.walk()}
        if "not_computed" in statuses:
            return "not_computed"
        if "unavailable" in statuses:
            return "unavailable"
        return "traced"

    def source_cells(self) -> tuple[SourceCell, ...]:
        """Return every source cell the value came from, once each, sorted.

        Cells are told apart by where they are (source, record, column), never by
        their values, so any value (a list, a struct) is fine.
        """
        cells: dict[tuple[str, str, str, str], SourceCell] = {}
        for step in self.root.walk():
            cell = step.source
            if cell is not None:
                key = (cell.source_id, cell.record_id, cell.column, cell.node)
                cells.setdefault(key, cell)
        return tuple(cells[key] for key in sorted(cells))

    def function_runs(self) -> tuple[tuple[str, FunctionRun], ...]:
        """Return every function step's run, as ``(node label, run)``, in trace order."""
        return tuple(
            (step.node, step.function)
            for step in self.root.walk()
            if step.function is not None
        )

    def nodes(self) -> tuple[str, ...]:
        """Return the labels of the nodes the trace passes, sorted."""
        return tuple(sorted({step.node for step in self.root.walk()}))

    def highlight(self) -> Highlight:
        """Return the trace's path, for ``job.diagram(highlight=...)``."""
        return Highlight(
            nodes=frozenset(self.nodes()),
            edges=frozenset(self.edges),
            note=f"trace of {self.root.node}.{self.root.column}",
        )

    def text(self) -> str:
        """Return the trace as an indented tree, the traced cell first."""
        return "\n".join(self.root.lines())

    def __str__(self) -> str:
        return self.text()

    def _repr_pretty_(self, printer: object, cycle: bool) -> None:  # pyright: ignore[reportUnusedParameter]  # IPython's display hook
        printer.text(self.text())  # pyright: ignore[reportAttributeAccessIssue]  # an IPython pretty printer

    def _repr_html_(self) -> str:
        return f"<pre>{html.escape(self.text())}</pre>"


@dataclass(frozen=True)
class Absence:
    """Whether rows of one source record reached a job node, and if not, why.

    In development: this interface may change.

    Returned by ``node.why_missing(source, record_id)``; plain data, built by reading
    what the job's nodes hold and what their databases store, without computing,
    calling user code, polling or writing.

    Attributes:
        node: The label of the node asked about.
        source: The source id.
        record_id: The record id.
        missing: ``False`` if the node holds a row from the record.
        status: ``"read"``; ``"not_computed"`` when a node on the way holds no run; or
            ``"unavailable"`` when a fold on the way reads a source that is not read here.
        reached_function: For the first function node on the way from the source:
            ``True`` if its stored pipeline entries hold an input row from the record,
            ``False`` if none does (the record never reached it, in any run); ``None``
            when there is no function node on the way or it stores nothing.
        function_node: The label of that function node, or ``None``.
        present_in: The nodes on the way whose rows hold a row from the record, in
            topological order (ties by label).
        removed_by: The step that removed the record's rows, or ``None``.
        because: Why, in words.
        path: The nodes from the source to the asked node, in topological order.
        edges: The streams of the job's graph between the nodes holding a row from the
            record, and into the step that removed it, as ``(upstream, downstream)``.
    """

    node: str
    source: str
    record_id: str
    missing: bool
    status: str
    reached_function: bool | None
    function_node: str | None
    present_in: tuple[str, ...]
    removed_by: Step | None
    because: str
    path: tuple[str, ...] = ()
    edges: tuple[tuple[str, str], ...] = ()

    def highlight(self) -> Highlight:
        """Return the path, the removing step included, for ``job.diagram``."""
        kept = set(self.present_in)
        if self.removed_by is not None:
            kept.add(self.removed_by.label)
        return Highlight(
            nodes=frozenset(kept), edges=frozenset(self.edges), note=self.headline()
        )

    def headline(self) -> str:
        """Return the answer as one line."""
        record = f"{self.source} {self.record_id}"
        if self.status == "unavailable":
            return f"{record}: not known: a step on the way reads a source that is not read here"
        if self.status != "read":
            return f"{record}: not computed: run() the job first"
        if not self.missing:
            return f"{record} reaches {self.node}"
        if self.removed_by is not None:
            step = self.removed_by
            name = (
                step.operator
                if step.label == step.operator
                else f"{step.operator}[{step.label}]"
            )
            return f"{record} is removed by {name}: {step.text}"
        return f"{record} does not reach {self.node}: {self.because}"

    def text(self) -> str:
        """Return the answer, then the details, one per line."""
        lines = [self.headline()]
        if self.function_node is not None and self.reached_function is not None:
            if self.reached_function:
                lines.append(f"  reached function node {self.function_node!r}")
            else:
                lines.append(
                    f"  never reached function node {self.function_node!r} "
                    "(no entry in its pipeline store)"
                )
        if self.present_in:
            lines.append(f"  present in: {', '.join(self.present_in)}")
        # The headline states the reason when no step removed the record.
        repeated = self.removed_by is None or self.because == self.removed_by.text
        if self.missing and self.because and not repeated:
            lines.append(f"  because: {self.because}")
        return "\n".join(lines)

    def __str__(self) -> str:
        return self.text()

    def _repr_pretty_(self, printer: object, cycle: bool) -> None:  # pyright: ignore[reportUnusedParameter]  # IPython's display hook
        printer.text(self.text())  # pyright: ignore[reportAttributeAccessIssue]  # an IPython pretty printer

    def _repr_html_(self) -> str:
        return f"<pre>{html.escape(self.text())}</pre>"
