"""What a job run did with each row: the summary ``PipelineJob.run`` returns.

With the default ``error_policy="continue"``, a row whose function raises is skipped and
the run goes on. ``RunSummary`` tells the user, per function node, how many rows were
computed, how many were read from the database, and how many failed with which first error.
"""

from __future__ import annotations

import html
import json
import threading
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from orcapod.core.result_cache import ResultCache
from orcapod.pipeline.observer import NoOpLogger
from orcapod.pipeline.restrictions import restriction_text

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from types import FrameType

    from orcapod.pipeline.observer_losses import LostObserverEvents
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
    from orcapod.pipeline.restrictions import RestrictedSource
    from orcapod.types import Condition, SchemaLike
    from orcapod.utils.warning_utils import WarningLocation

_NOOP_LOGGER = NoOpLogger()
_ASYNC_ATTRIBUTION_NOTE = (
    "Computed and from-store counts reflect this run. Equal inputs may arrive in a "
    "different order or be computed concurrently."
)


@dataclass(frozen=True)
class NodeRunSummary:
    """What one function node did with its rows in a run.

    Attributes:
        label: The node's label, its key in ``job.nodes``.
        computed: Rows whose function ran and returned.
        reused: Rows whose result was read from the database instead.
        failed: Rows whose function raised; the run skipped them.
        first_error: The exception of the first failed row, with its traceback, or
            ``None`` when no row failed.
        not_stored: Inputs a load-only run skipped because no result is stored for
            them (``job.run(load_only=True)``; always 0 in a full run).
        inputs_incomplete: The labels of the function nodes upstream of this one that
            skipped inputs in a load-only run, in run order: this node's inputs lack
            the rows those results would have given (a group may be partial).
        held: The rows of a node the run did not run because it held a current run
            from its previous run (ITL-1095): it computed, read and wrote nothing.
    """

    label: str
    computed: int
    reused: int
    failed: int
    first_error: BaseException | None
    not_stored: int = 0
    inputs_incomplete: tuple[str, ...] = ()
    held: int = 0

    @property
    def rows(self) -> int:
        """All rows of the node: computed, reused, failed, not stored and held."""
        return self.computed + self.reused + self.failed + self.not_stored + self.held


class RunSummary:
    """What a job run did, per function node; returned by ``PipelineJob.run``.

    Shown at the end of a notebook cell, it lists every function node with its counts and,
    for a node with failed rows, the first error and how to see its full traceback. The job
    also keeps it as ``job.last_run``.

    Counts attribute work to the nodes that actually did it. In an async run, whichever
    equal inputs arrive before a result is available may compute it. Later inputs can
    reuse it, so the attribution and counts can vary with scheduling.

    Attributes:
        job_name: The job's name.
        nodes: Per function node, in run order, keyed by label.
        load_only: Whether the run was a load-only run (``job.run(load_only=True)``):
            it computed nothing; ``reused`` counts the rows it loaded.
        target: For the run of one node and the nodes it depends on
            (``node.run()``), that node's label; ``None`` for a job run.
        unchecked_sources: The source ids of the sources that could not be checked
            for new data at the start of the run (ITL-1102): each raised in
            ``check_for_new_data``, was reported by a ``SourceCheckFailedWarning``,
            and fed the run with the data it already held.
        conditions: The ``Condition`` each source of the run read under, by slot
            name (the name ``bind`` takes; a source read directly by a node's
            ``run()`` by its label), for the slots whose source has one (ITL-1218).
            A restriction (a condition and/or a limit) chooses which rows flow and is
            not part of any identity, so this and ``limits`` are the record of which
            rows a run saw. In memory only: a run summary is not saved (a saved job
            keeps each source's ``where`` and ``limit`` in its config).
        limits: The limit each source of the run read with, by slot name, for the
            slots whose source has one.
        restricted_source_ids: The source id of every restricted source (a slot in
            ``conditions``, ``limits`` or both), by slot name.
        ran_nothing: Whether the run ran no node: every node held a current run
            (ITL-1095). Decided by the run's plan, which counts every kind of node, so a
            run that ran only a side-effect or operator node is not "nothing to do".
        lost_observer_events: The observer events the run could not record, by
            observer kind (``"status"``, ``"logging"``), each with its count and its
            first error's text (ITL-1556); empty when every event was recorded. A
            failed write of a ``StatusObserver`` or ``LoggingObserver`` does not stop
            the run and changes none of its results or other counts; an
            ``ObserverEventsLostWarning`` reports the same at the end of the run. A
            job node's own ``run()`` takes no observer, so its summary lists none.
    """

    def __init__(
        self,
        job_name: str,
        nodes: Sequence[NodeRunSummary],
        *,
        load_only: bool = False,
        target: str | None = None,
        unchecked_sources: Sequence[str] = (),
        ran_nothing: bool = False,
        restricted_sources: Mapping[str, RestrictedSource] | None = None,
        asynchronous: bool = False,
        lost_observer_events: Mapping[str, LostObserverEvents] | None = None,
    ) -> None:
        """Build the summary.

        Args:
            job_name: The job's name.
            nodes: One entry per function node of the run, in run order.
            load_only: Whether the run loaded stored results only.
            target: The label of the node whose ``run()`` this was, if any.
            unchecked_sources: The sources that could not be checked for new data.
            ran_nothing: Whether the run ran no node (every node held a current run).
            restricted_sources: Slot name to the source id, the condition (empty
                for none) and the limit of each restricted source of the run
                (``restrictions.restrictions_in_force``). Displayed by slot name
                or direct source label.
            asynchronous: Whether the async orchestrator executed the run.
            lost_observer_events: The observer events the run could not record, by
                observer kind.
        """
        self.job_name: str = job_name
        self.nodes: Mapping[str, NodeRunSummary] = {node.label: node for node in nodes}
        self.load_only: bool = load_only
        self.target: str | None = target
        self.unchecked_sources: tuple[str, ...] = tuple(unchecked_sources)
        self.ran_nothing: bool = ran_nothing
        self.lost_observer_events: Mapping[str, LostObserverEvents] = dict(
            sorted((lost_observer_events or {}).items())
        )
        self._attribution_note: str = (
            _ASYNC_ATTRIBUTION_NOTE
            if asynchronous and not load_only and not ran_nothing and nodes
            else ""
        )
        # One mapping split three ways, so the keys cannot disagree.
        restricted = dict(sorted((restricted_sources or {}).items()))
        self.conditions: Mapping[str, Condition] = {
            slot: condition
            for slot, (_, condition, _) in restricted.items()
            if not condition.is_empty
        }
        self.limits: Mapping[str, int] = {
            slot: limit
            for slot, (_, _, limit) in restricted.items()
            if limit is not None
        }
        self.restricted_source_ids: Mapping[str, str] = {
            slot: source_id for slot, (source_id, _, _) in restricted.items()
        }

    @property
    def failed_rows(self) -> int:
        """The number of rows that failed, over every node."""
        return sum(node.failed for node in self.nodes.values())

    @property
    def ok(self) -> bool:
        """``True`` when no row failed."""
        return self.failed_rows == 0

    def failures_message(self) -> str:
        """One sentence per node with failed rows, for the run's warning.

        Returns:
            The sentences joined, or ``""`` when no row failed.
        """
        return " ".join(
            f"{node.failed} of {_rows(node.rows)} failed in node {node.label!r} "
            f"(first error: {_error_line(node.first_error)}). "
            f"Full traceback: {_traceback_call(node.label, self.target)}."
            for node in self.nodes.values()
            if node.failed
        )

    def print_traceback(self, label: str | None = None) -> None:
        """Print the full traceback of a node's first failed row to standard error.

        Args:
            label: The node; by default the first node with a failed row.

        Raises:
            KeyError: If no function node of the run has that label.
        """
        if label is not None and label not in self.nodes:
            raise KeyError(
                f"No function node {label!r} in this run; its function nodes are "
                f"{sorted(self.nodes)}."
            )
        candidates = [self.nodes[label]] if label is not None else self.nodes.values()
        for node in candidates:
            if node.first_error is not None:
                traceback.print_exception(node.first_error)
                return
        where = f" in node {label!r}" if label is not None else ""
        print(f"No rows failed{where} in this run.")

    def _headline(self) -> str:
        """The first line: the job (or node) and whether any row failed."""
        subject = (
            f"job {self.job_name!r}"
            if self.target is None
            else f"node {self.target!r} and the nodes it depends on"
        )
        # From what the run ran, not from the function nodes' counts: a run that ran
        # only a side-effect or operator node did something (ITL-1121).
        if self.ran_nothing:
            run = "Load-only run" if self.load_only else "Run"
            return f"{run} of {subject}: nothing to do; every node holds a current run."
        if self.load_only:
            skipped = sum(node.not_stored for node in self.nodes.values())
            outcome = (
                f"{_rows(skipped)} without a stored result skipped."
                if skipped
                else "every input had a stored result."
            )
            return f"Load-only run of {subject}: {outcome}"
        if self.failed_rows:
            outcome = f"{_rows(self.failed_rows)} failed."
        else:
            outcome = "all rows succeeded."
        return f"Run of {subject}: {outcome}"

    def _unchecked_line(self) -> str | None:
        """The line naming the sources that could not be checked, if any."""
        if not self.unchecked_sources:
            return None
        return "sources not checked for new data (see the warning): " + ", ".join(
            self.unchecked_sources
        )

    def _restrictions_line(self) -> str | None:
        """The line naming each restricted source and its restriction, if any."""
        if not self.restricted_source_ids:
            return None
        return "restricted sources: " + "; ".join(
            f"{slot} ({source_id}): "
            + restriction_text(self.conditions.get(slot), self.limits.get(slot))
            for slot, source_id in self.restricted_source_ids.items()
        )

    def lost_observer_events_message(self) -> str:
        """Each observer kind's lost events and first error, for the run's warning.

        Returns:
            One clause per kind, joined, or ``""`` when no event was lost.
        """
        return "; ".join(
            f"{lost.count} {lost.kind} {'event' if lost.count == 1 else 'events'} "
            f"(first error: {_first_line(lost.first_error)})"
            for lost in self.lost_observer_events.values()
        )

    def _lost_events_line(self) -> str | None:
        """The line counting the observer events that were not recorded, if any."""
        if not self.lost_observer_events:
            return None
        return "observer events not recorded: " + self.lost_observer_events_message()

    def _source_lines_html(self) -> str:
        """The source and lost-event lines as HTML paragraphs, or ``""``."""
        restricted = self._restrictions_line()
        lost = self._lost_events_line()
        return (
            self._unchecked_html()
            + ("" if restricted is None else f"<p>{html.escape(restricted)}</p>")
            + ("" if lost is None else f"<p>{html.escape(lost)}</p>")
        )

    def _unchecked_html(self) -> str:
        """The unchecked-sources line as an HTML paragraph, or ``""``."""
        unchecked = self._unchecked_line()
        return "" if unchecked is None else f"<p>{html.escape(unchecked)}</p>"

    def __repr__(self) -> str:
        """The summary as text: one line per node, and the first error of a failing one."""
        lines = [self._headline()]
        unchecked = self._unchecked_line()
        if unchecked is not None:
            lines.append(f"  {unchecked}")
        restricted = self._restrictions_line()
        if restricted is not None:
            lines.append(f"  {restricted}")
        lost = self._lost_events_line()
        if lost is not None:
            lines.append(f"  {lost}")
        if self._attribution_note:
            lines.append(f"  {self._attribution_note}")
        for node in self.nodes.values():
            lines.append(
                f"  {node.label}: {_rows(node.rows)}: {_counts(node, self.load_only)}"
            )
            if node.failed:
                lines.append(f"    first error: {_error_line(node.first_error)}")
                lines.append(
                    f"    Full traceback: {_traceback_call(node.label, self.target)}"
                )
        return "\n".join(lines)

    def _repr_html_(self) -> str:
        """The notebook display: the same content as ``repr``, as a table."""
        if self.load_only:
            return self._load_only_html()
        header = "".join(
            f"<th style='text-align:left'>{name}</th>"
            for name in (
                "node",
                "rows",
                "computed",
                "from store",
                "failed",
                "first error",
            )
        )
        body = []
        for node in self.nodes.values():
            failed_style = (
                " style='color:#c62828;font-weight:bold'" if node.failed else ""
            )
            error = ""
            if node.failed:
                error = (
                    f"{html.escape(_error_line(node.first_error))}<br>"
                    f"<code>{html.escape(_traceback_call(node.label, self.target))}</code>"
                )
            body.append(
                "<tr>"
                f"<td><code>{html.escape(node.label)}</code></td>"
                f"<td>{node.rows}</td><td>{node.computed}</td><td>{node.reused}</td>"
                f"<td{failed_style}>{node.failed}</td><td>{error}</td>"
                "</tr>"
            )
        attribution = (
            f"<p>{html.escape(self._attribution_note)}</p>"
            if self._attribution_note
            else ""
        )
        return (
            f"<p>{html.escape(self._headline())}</p>{self._source_lines_html()}"
            f"{attribution}"
            f"<table><thead><tr>{header}</tr></thead>"
            f"<tbody>{''.join(body)}</tbody></table>"
        )

    def _load_only_html(self) -> str:
        """The notebook display of a load-only run: rows loaded and skipped per node."""
        header = "".join(
            f"<th style='text-align:left'>{name}</th>"
            for name in ("node", "rows", "loaded", "not stored", "inputs incomplete")
        )
        body = [
            "<tr>"
            f"<td><code>{html.escape(node.label)}</code></td>"
            f"<td>{node.rows}</td><td>{node.reused}</td><td>{node.not_stored}</td>"
            f"<td>{html.escape(', '.join(node.inputs_incomplete))}</td>"
            "</tr>"
            for node in self.nodes.values()
        ]
        return (
            f"<p>{html.escape(self._headline())}</p>{self._source_lines_html()}"
            f"<table><thead><tr>{header}</tr></thead>"
            f"<tbody>{''.join(body)}</tbody></table>"
        )


def _counts(node: NodeRunSummary, load_only: bool) -> str:
    """A node's counts as text: what a full run did, or what a load-only run loaded."""
    if node.held and not (node.computed or node.reused or node.failed):
        text = "held from the previous run"
        if node.not_stored:
            text += f" ({node.not_stored} inputs without a stored result)"
        return text
    if not load_only:
        return (
            f"{node.computed} computed, {node.reused} from store, {node.failed} failed"
        )
    text = f"{node.reused} loaded"
    if node.not_stored:
        text += f", {node.not_stored} not stored (skipped)"
    if node.inputs_incomplete:
        text += f" (inputs incomplete: {', '.join(node.inputs_incomplete)})"
    return text


def _rows(n: int) -> str:
    """``1 row`` or ``n rows``."""
    return f"{n} row" if n == 1 else f"{n} rows"


def _first_line(text: str) -> str:
    """The first line of ``text``, marked with ``...`` when more lines follow."""
    lines = text.splitlines()
    first = lines[0] if lines else ""
    return f"{first} ..." if len(lines) > 1 else first


def _error_line(error: BaseException | None) -> str:
    """``Type: first line of the message``."""
    if error is None:
        return ""
    first = _first_line(str(error))
    return f"{type(error).__name__}: {first}" if first else type(error).__name__


def _traceback_call(label: str, target: str | None = None) -> str:
    """The call that prints a node's traceback, with the label as a valid string literal.

    A JSON string is a valid Python string literal, and keeps double quotes. A node
    run's summary is kept on its node (``target``), a job run's on the job.
    """
    owner = "job" if target is None else f"job.nodes[{json.dumps(target)}]"
    return f"{owner}.last_run.print_traceback({json.dumps(label)})"


def warn_row_failures(summary: RunSummary, caller: FrameType) -> None:
    """Issue the run's ``RowFailuresWarning``, shown after every run with failed rows.

    Python's default filter shows a warning once per text and line, so a second identical
    failing run from the same line (re-running a notebook cell) would show nothing.
    ``warn_at_frame`` keeps every run's warning, while the user's filters (``ignore``,
    ``error``) still apply.

    Args:
        summary: The run's summary, with failed rows.
        caller: The frame that called ``run``; the warning is shown at its line.
    """
    from orcapod.errors import RowFailuresWarning
    from orcapod.utils.warning_utils import warn_at_frame

    warn_at_frame(summary.failures_message(), RowFailuresWarning, caller)


def warn_lost_observer_events(summary: RunSummary, location: WarningLocation) -> None:
    """Issue the run's ``ObserverEventsLostWarning``, at the end of the run (ITL-1556).

    At the end and not at the first loss: the count and each kind's first error are
    complete then, the run's rows and summary are already kept (the warning turned into
    an error loses nothing), and it is shown at the line that started the run, where
    the first loss happens inside a node, possibly on a worker thread. Each failed
    write is still logged by its observer when it happens.

    Args:
        summary: The run's summary, with lost observer events.
        location: The line that called ``run`` or ``run_async``.
    """
    from orcapod.errors import ObserverEventsLostWarning

    location.warn(
        "Observer events were not recorded in this run: "
        + f"{summary.lost_observer_events_message()}. The run's results are not "
        + "affected, but its status and log records are incomplete. Each failed "
        + "write was logged with its traceback; the counts are in the run "
        + "summary's lost_observer_events.",
        ObserverEventsLostWarning,
    )


class RunCountingObserver:
    """Counts, per node label, the rows computed, reused and failed in one run.

    ``PipelineJob.run`` runs it beside the user's observer. Every contextualized copy is
    this observer itself, so all nodes count into one place. Hooks may be called from
    several threads, so the counts are updated under a lock.
    """

    def __init__(self) -> None:
        """Start with no counts."""
        self._lock: threading.Lock = threading.Lock()
        self._computed: dict[str, int] = {}
        self._reused: dict[str, int] = {}
        self._failed: dict[str, int] = {}
        self._first_error: dict[str, BaseException] = {}

    def node_summary(self, label: str) -> NodeRunSummary:
        """The counts of node ``label``; zeros when it saw no rows.

        Args:
            label: The node's label.

        Returns:
            The node's summary.
        """
        with self._lock:
            return NodeRunSummary(
                label=label,
                computed=self._computed.get(label, 0),
                reused=self._reused.get(label, 0),
                failed=self._failed.get(label, 0),
                first_error=self._first_error.get(label),
            )

    def contextualize(self, *identity_path: str) -> RunCountingObserver:
        """Return this observer: counts are keyed by node label, not identity path."""
        return self

    def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
        """Nothing to count."""

    def on_run_end(self, run_id: str) -> None:
        """Nothing to count."""

    def on_node_start(
        self, node_label: str, node_hash: str, tag_schema: SchemaLike | None = None
    ) -> None:
        """Nothing to count."""

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        """Nothing to count."""

    def on_data_start(
        self, node_label: str, tag: TagProtocol, data: DataProtocol
    ) -> None:
        """Nothing to count until the row ends."""

    def on_data_end(
        self,
        node_label: str,
        tag: TagProtocol,
        input_data: DataProtocol,
        output_data: DataProtocol | None,
        cached: bool,
    ) -> None:
        """Count the row as reused (from the pipeline or result database) or computed."""
        from_store = cached or (
            output_data is not None
            and output_data.get_meta_value(ResultCache.RESULT_COMPUTED_FLAG, True)
            is False
        )
        counts = self._reused if from_store else self._computed
        with self._lock:
            counts[node_label] = counts.get(node_label, 0) + 1

    def on_data_crash(
        self, node_label: str, tag: TagProtocol, data: DataProtocol, error: Exception
    ) -> None:
        """Count the row as failed; keep the node's first error."""
        with self._lock:
            self._failed[node_label] = self._failed.get(node_label, 0) + 1
            _ = self._first_error.setdefault(node_label, error)

    def create_data_logger(self, tag: TagProtocol, data: DataProtocol) -> Any:
        """No logging of its own."""
        return _NOOP_LOGGER


def run_node_summaries(
    dag: Any, counter: RunCountingObserver, held: Mapping[Any, int] | None = None
) -> list[NodeRunSummary]:
    """One summary per function node of a finished run, in run order.

    The counts come from ``counter``; a load-only run's skipped inputs come from each
    node (``last_run_not_stored``), and every node below a function node that skipped
    inputs names it in ``inputs_incomplete`` (ITL-1095).

    Args:
        dag: The run's node graph (``topological_sort``, ``predecessors``).
        counter: The observer that counted the run's rows.
        held: Nodes of ``dag`` the run did not run because they hold a current run,
            with their row counts: reported as held (ITL-1095). Inputs they skipped in
            an earlier load-only run still make the nodes below them incomplete.

    Returns:
        The summaries of the run's function nodes, in run order.
    """
    from dataclasses import replace

    from orcapod.protocols.node_protocols import (
        is_function_node,
        is_side_effect_function_node,
    )

    order = list(dag.topological_sort())
    position = {id(node): index for index, node in enumerate(order)}
    skipping_above: dict[int, set[int]] = {}
    summaries: list[NodeRunSummary] = []
    for node in order:
        above: set[int] = set()
        for upstream in dag.predecessors(node):
            above |= skipping_above.get(id(upstream), set())
            if getattr(upstream, "last_run_not_stored", 0):
                above.add(id(upstream))
        skipping_above[id(node)] = above
        # A stream outside the job (in a node.run() graph) is no node of the run.
        if getattr(node, "node_type", None) is None:
            continue
        if not (is_function_node(node) or is_side_effect_function_node(node)):
            continue
        held_rows = next(
            (count for other, count in (held or {}).items() if other is node), None
        )
        labels = tuple(
            order[index].label for index in sorted(position[key] for key in above)
        )
        label: str = getattr(node, "label")  # every graph node is labelled
        if held_rows is not None:
            # A held node keeps what its run skipped: a load-only run's missing
            # inputs are still missing.
            summaries.append(
                NodeRunSummary(
                    label,
                    0,
                    0,
                    0,
                    None,
                    not_stored=getattr(node, "last_run_not_stored", 0),
                    inputs_incomplete=labels,
                    held=held_rows,
                )
            )
            continue
        summaries.append(
            replace(
                counter.node_summary(label),
                not_stored=getattr(node, "last_run_not_stored", 0),
                inputs_incomplete=labels,
            )
        )
    return summaries
