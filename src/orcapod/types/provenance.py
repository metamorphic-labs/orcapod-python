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
from typing import Any, cast

from orcapod.types.explanation import Step, plain_repr


def _value_text(value: Any) -> str:
    """A cell value as the text form shows it: a list by its items, a float by repr."""
    if isinstance(value, list):
        items = [_value_text(v) for v in value]  # pyright: ignore[reportUnknownVariableType]
        return "[" + ", ".join(items) + "]"
    if isinstance(value, tuple):
        # An Arrow map entry reads as a (key, value) tuple: shown as a tuple.
        parts = [_value_text(v) for v in cast(tuple[Any, ...], value)]
        return "(" + ", ".join(parts) + ("," if len(parts) == 1 else "") + ")"
    if isinstance(value, dict):
        items = cast(dict[Any, Any], value)
        inner = ", ".join(
            f"{_value_text(k)}: {_value_text(items[k])}" for k in sorted(items, key=str)
        )
        return "{" + inner + "}"
    if isinstance(value, (str, bytes, bool, int, float)):
        return plain_repr(value)
    if value is None:
        return "None"
    return _typed_text(value)


def _typed_text(value: Any) -> str:
    """A value of another supported type, by its type and its exact text, so two
    different values never read the same: ``date('2026-01-01')``, ``datetime(...)``
    in ISO form, ``Decimal('1.50')``, ``UUID(...)``, ``Path(...)``, a dataclass by its
    fields; anything else as ``<type name>``."""
    import dataclasses
    import datetime as dt
    import decimal
    import pathlib
    import uuid

    name = type(value).__name__
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return f"{name}({value.isoformat()!r})"
    if isinstance(value, dt.timedelta):
        return f"{name}({value.days}d {value.seconds}s {value.microseconds}us)"
    if isinstance(value, (decimal.Decimal, uuid.UUID, pathlib.PurePath)):
        return f"{name}({str(value)!r})"
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        fields = ", ".join(
            f"{f.name}={_value_text(getattr(value, f.name))}"
            for f in dataclasses.fields(value)
        )
        return f"{name}({fields})"
    return f"<{name}>"


def _value_key(value: Any) -> Any:
    """A hashable, typed key for a cell value: two values have one key only when they
    are one value of one type (a list or a mapping by its items). Used to tell versions
    of a record apart; never a placeholder (a ``date`` keys by its own value)."""
    if isinstance(value, list):
        return ("list", tuple(_value_key(v) for v in cast(list[Any], value)))
    if isinstance(value, dict):
        items = cast(dict[Any, Any], value)
        return (
            "dict",
            tuple(sorted(((repr(k), _value_key(v)) for k, v in items.items()))),
        )
    if isinstance(value, float) and value != value:
        return ("builtins", "float", "nan")  # every NaN is one value
    try:
        hash(value)
    except TypeError:
        return (type(value).__qualname__, repr(value))
    return (type(value).__module__, type(value).__qualname__, value)


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
        note: A line shown with the diagram (its listing, and its picture's caption and
            title).
        marks: A word shown on a node, as ``(label, mark)``: for a trace, each
            function node on the path is marked ``"computed"`` (its rows on the path
            computed their results), ``"reused"`` (they reused results stored for
            equal inputs) or ``"computed + reused"`` (some did each).
        asked: The label of the node the question was asked at (the node ``trace`` or
            ``why_missing`` was called on), or ``""``: the diagram marks it apart from
            the path.
        asked_text: The words that mark it: ``"traced from here"`` for a trace,
            ``"asked here"`` for ``why_missing``.
    """

    nodes: frozenset[str] = frozenset()
    edges: frozenset[tuple[str, str]] = frozenset()
    note: str = ""
    marks: frozenset[tuple[str, str]] = frozenset()
    asked: str = ""
    asked_text: str = ""

    def mark(self, label: str) -> str:
        """Return the mark of the node ``label``, or ``""`` when it has none."""
        return next((mark for node, mark in self.marks if node == label), "")


@dataclass(frozen=True)
class SourceCell:
    """One cell of a source, where a traced value came from.

    Provisional in 0.4.0: answers may be refined; a case that cannot be proven is
    reported unavailable or refused (the known limits are in the provenance concept
    page).

    Attributes:
        node: The label of the job's source node.
        source_id: The source's id (the first part of the source-info token).
        record_id: The record id of the source row (``row_<n>``, or the key columns as
            JSON).
        column: The source's column.
        value: The value, as the row that carried the token holds it.
        shown: The name the text shows for a source with an assigned label (the text of
            a ``Trace`` gives the source id once, at the end); ``""`` for the source id.
    """

    node: str
    source_id: str
    record_id: str
    column: str
    value: Any
    shown: str = field(default="", compare=False)

    def text(self) -> str:
        """Return the cell as one phrase: ``units row_40 n_spikes = 575``."""
        return f"{self.shown or self.source_id} {self.record_id} {self.column} = {_value_text(self.value)}"


@dataclass(frozen=True)
class FunctionRun:
    """Which function produced a function node's value for one input row, and how.

    Provisional in 0.4.0: answers may be refined; a case that cannot be proven is
    reported unavailable or refused (the known limits are in the provenance concept
    page).

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

    Provisional in 0.4.0: answers may be refined; a case that cannot be proven is
    reported unavailable or refused (the known limits are in the provenance concept
    page).

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

    def lines(
        self, depth: int = 0, via: tuple[str, ...] = (), member: int | None = None
    ) -> list[str]:
        """Return the step and the steps below it as indented text lines.

        A row-wise operator that only passes a cell on is not given a line of its own:
        the line of the cell it passes names it (``via Join, Filter``), with the fold
        member position the skipped step carried.

        Args:
            depth: The indentation level.
            via: The operators passed through above this step, nearest first.
            member: The member position of a skipped step above, if any.

        Returns:
            The lines.
        """
        position = self.member if self.member is not None else member
        if self.passes_through():
            return self.inputs[0].lines(
                depth, (*via, self.operator or self.node), position
            )
        pad = "  " * depth
        where = f"[{position}] " if position is not None else ""
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
            if self.kind == "source":
                out.append(f"{pad}  not read: the source's rows are not held")
            else:
                out.append(f"{pad}  not traced: this link cannot be proven here")
        if self.function is not None:
            out.append(f"{pad}  {self.function.text()}")
        for step in self.inputs:
            out.extend(step.lines(depth + 1))
        return out


@dataclass(frozen=True)
class Trace:
    """Cells of a job node traced back to the source cells they were made from.

    Provisional in 0.4.0: answers may be refined; a case that cannot be proven is
    reported unavailable or refused (the known limits are in the provenance concept
    page).

    Returned by ``node.trace(row, column)``; plain data, built by reading what the job's
    nodes hold and what their databases store, without computing, calling user code,
    polling or writing. Every held row of the node whose columns equal the selector's
    values is traced, one root each.

    Attributes:
        node: The label of the node the trace was asked at.
        column: The traced data column.
        selector: The column values given, as ``(column, value)`` pairs in the order
            given (none: every held row).
        roots: One step per matching row, ordered by the rows' tags; its ``inputs``
            lead to the sources. When the node holds no run, the one step that says so.
        matched: How many held rows match, or ``None`` when the node holds no run (or
            is a source whose rows are not read).
        edges: Every stream the trace follows, as ``(upstream label, downstream
            label)``, sorted.
    """

    node: str
    column: str
    selector: tuple[tuple[str, Any], ...]
    roots: tuple[TraceStep, ...]
    matched: int | None
    edges: tuple[tuple[str, str], ...] = field(default=())

    @property
    def root(self) -> TraceStep:
        """The one root, when exactly one row matched (or the node holds no run).

        Raises:
            ValueError: If no row or several rows matched: read ``roots``.
        """
        if len(self.roots) != 1:
            raise ValueError(f"{self.count_text()}: read roots, one per row")
        return self.roots[0]

    @property
    def status(self) -> str:
        """``"traced"``; ``"no_match"`` when no held row matches the selector;
        ``"not_computed"`` when a node on the way holds no run; or ``"unavailable"``
        when a source cell on the way is not read."""
        if not self.roots:
            return "no_match"
        statuses = {step.status for step in self.walk()}
        if "not_computed" in statuses:
            return "not_computed"
        if "unavailable" in statuses:
            return "unavailable"
        return "traced"

    def walk(self) -> Iterator[TraceStep]:
        """Yield every step of every root, each root depth first."""
        for root in self.roots:
            yield from root.walk()

    def question(self) -> str:
        """Return what was asked, written out: ``trace of rate_hz at node rates, for
        subject_id='mouse_02', session='s1'``."""
        rows = f"for {_tags_text(self.selector)}" if self.selector else "for every row"
        return f"trace of {self.column} at node {self.node}, {rows}"

    def count_text(self) -> str:
        """Return how many held rows match: ``1 row matches``, ``3 rows match``, ``no
        held row matches``; or that the node holds no run."""
        if self.matched is None:
            return "the node holds no rows to trace here"
        if self.matched == 0:
            return "no held row matches"
        return (
            f"{self.matched} row matches"
            if self.matched == 1
            else (f"{self.matched} rows match")
        )

    def header(self) -> str:
        """Return the first line of the text: the question and how many rows match."""
        return f"{self.question()}: {self.count_text()}"

    def source_cells(self) -> tuple[SourceCell, ...]:
        """Return every source cell the value came from, once each, sorted.

        Cells are told apart by where they are (source, record, column) and by their
        value (typed: ``_value_key``), so two versions of one record are both listed;
        a list or a struct is keyed by its items, so any value is fine.
        """
        cells: dict[tuple[Any, ...], SourceCell] = {}
        for step in self.walk():
            cell = step.source
            if cell is not None:
                # Two versions of one record (a cache holding both) are two cells.
                key = (
                    cell.source_id,
                    cell.record_id,
                    cell.column,
                    cell.node,
                    _value_key(cell.value),
                )
                cells.setdefault(key, cell)
        ordered = sorted(cells, key=lambda k: (k[:4], repr(k[4])))
        return tuple(cells[key] for key in ordered)

    def function_runs(self) -> tuple[tuple[str, FunctionRun], ...]:
        """Return every function step's run, as ``(node label, run)``, in trace order."""
        return tuple(
            (step.node, step.function)
            for step in self.walk()
            if step.function is not None
        )

    def nodes(self) -> tuple[str, ...]:
        """Return the labels of the nodes the trace passes, sorted."""
        return tuple(sorted({step.node for step in self.walk()}))

    def highlight(self) -> Highlight:
        """Return the trace's path, for ``job.diagram(highlight=...)``.

        The union of every matching row's path. Each function node on it is marked
        with how its rows on the path got their results: ``"computed"``, ``"reused"``
        or ``"computed + reused"``; the node the trace was asked at is marked
        ``"traced from here"``.
        """
        how: dict[str, set[bool]] = {}
        for node, run in self.function_runs():
            how.setdefault(node, set()).add(run.computed_here)
        marks = frozenset(
            (
                node,
                "computed + reused"
                if len(seen) > 1
                else "computed"
                if True in seen
                else "reused",
            )
            for node, seen in how.items()
        )
        return Highlight(
            nodes=frozenset(self.nodes()),
            edges=frozenset(self.edges),
            note=self.header(),
            marks=marks,
            asked=self.node,
            asked_text="traced from here",
        )

    def text(self) -> str:
        """Return the header, then each matching row's trace as an indented tree."""
        lines = [self.header()]
        for root in self.roots:
            lines.extend(root.lines())
        named = {(c.shown, c.source_id) for c in self.source_cells() if c.shown}
        lines.extend(f"source {shown}: {full}" for shown, full in sorted(named))
        return "\n".join(lines)

    def __str__(self) -> str:
        """Return ``text()``: what ``print`` shows."""
        return self.text()

    def _repr_pretty_(self, printer: object, cycle: bool) -> None:  # pyright: ignore[reportUnusedParameter]  # IPython's display hook
        """Show ``text()`` in IPython's plain display."""
        printer.text(self.text())  # pyright: ignore[reportAttributeAccessIssue]  # an IPython pretty printer

    def _repr_html_(self) -> str:
        """Show ``text()`` in a notebook, HTML-escaped inside a ``<pre>`` element."""
        return f"<pre>{html.escape(self.text())}</pre>"


@dataclass(frozen=True)
class Absence:
    """Whether rows of one source record reached a job node, and if not, why.

    Provisional in 0.4.0: answers may be refined; a case that cannot be proven is
    reported unavailable or refused (the known limits are in the provenance concept
    page).

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
        reached_function: ``True`` when a stored entry of the first function node on
            the way (one of its own entries: this pipeline bound to these sources, in
            any of its runs) proves that an input row from the record reached it;
            otherwise
            ``None`` (unknown). Never ``False``: a function that raised on an input
            stores no entry for it, so a missing entry does not prove the input never
            reached the function.
        function_node: The label of that function node, or ``None``.
        present_in: The nodes on the way whose rows hold a row from the record, in
            topological order (ties by label).
        removed_by: The step that removed the record's rows, or ``None``.
        because: Why, in words.
        path: The nodes from the source to the asked node, in topological order.
        edges: The streams of the job's graph between the nodes holding a row from the
            record, and into the step that removed it, as ``(upstream, downstream)``.
        entry_recorded: What that function node's store records: ``True`` when it
            stores an entry for an input row from the record, ``False`` when it stores
            entries of the source but none for the record (the function raised on it,
            or never received it), ``None`` when the store cannot tell.
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
    entry_recorded: bool | None = None

    def highlight(self) -> Highlight:
        """Return the path, the removing step included, for ``job.diagram``."""
        kept = set(self.present_in)
        if self.removed_by is not None:
            kept.add(self.removed_by.label)
        return Highlight(
            nodes=frozenset(kept),
            edges=frozenset(self.edges),
            note=f"{self.question()}: {self.headline()}",
            asked=self.node,
            asked_text="asked here",
        )

    def question(self) -> str:
        """Return what was asked, written out: ``why record row_4 of units is missing
        at node imec0_rates``."""
        return f"why record {self.record_id} of {self.source} is missing at node {self.node}"

    def headline(self) -> str:
        """Return the answer as one line."""
        record = f"{self.source} {self.record_id}"
        if self.status == "unavailable":
            reason = self.because or "a step on the way is not read here"
            return f"{record}: not known: {reason}"
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
        """Return the question, the answer, then the details, one per line."""
        lines = [self.question(), self.headline()]
        if self.function_node is not None and self.entry_recorded is not None:
            if self.entry_recorded:
                lines.append(
                    f"  reached function node {self.function_node!r} (a stored entry)"
                )
            else:
                lines.append(
                    f"  no successful entry of function node {self.function_node!r} is "
                    "stored for it (the function raised on it, or never received it)"
                )
        if self.present_in:
            lines.append(f"  present in: {', '.join(self.present_in)}")
        # The headline states the reason when no step removed the record.
        repeated = self.removed_by is None or self.because == self.removed_by.text
        if self.missing and self.because and not repeated:
            lines.append(f"  because: {self.because}")
        return "\n".join(lines)

    def __str__(self) -> str:
        """Return ``text()``: what ``print`` shows."""
        return self.text()

    def _repr_pretty_(self, printer: object, cycle: bool) -> None:  # pyright: ignore[reportUnusedParameter]  # IPython's display hook
        """Show ``text()`` in IPython's plain display."""
        printer.text(self.text())  # pyright: ignore[reportAttributeAccessIssue]  # an IPython pretty printer

    def _repr_html_(self) -> str:
        """Show ``text()`` in a notebook, HTML-escaped inside a ``<pre>`` element."""
        return f"<pre>{html.escape(self.text())}</pre>"
