"""A diagram of a pipeline's DAG for notebooks: inline SVG laid out in Python (ITL-1197).

``pipeline.diagram()`` and ``job.diagram()`` return a ``PipelineDiagram``; a pipeline or
job displayed at the end of a notebook cell shows one. The diagram is made in two steps:

1. A model (``PipelineDiagram.to_dict()``): plain, JSON-able nodes and edges with labels,
   kinds, schemas and, for a job, each node's run status. It is read without computing
   anything: each node's ``output_schema()`` (predicted, never computed), its kind and
   label, the run state a job node holds in memory and the job's last ``RunSummary``. It
   calls no user function, reads no source and no database, and writes nothing (the
   rule of ``head()`` and the notebook preview).
2. A renderer that turns the model into a document. The one here lays the graph out in
   layers (a small Sugiyama layout, standard library only) and draws an SVG picture. The
   picture is embedded as an image (a ``data:`` URI), which every notebook shows: a
   notebook that does not trust an output (JupyterLab for a notebook its reader did not
   run) removes inline SVG, style sheets and scripts from HTML but keeps an image. Where
   the front end runs scripts, one inline script replaces the image by the same SVG
   drawn inline in the page's colours and adds pan, zoom and a details panel. Nothing is
   fetched from a network and no system binary is needed.

Every text that comes from the user (labels, column names, type names, error messages,
the pipeline name) is escaped where it enters the document, and the model reaches the
script as JSON with ``<``, ``>`` and ``&`` escaped, read with ``textContent`` only.
"""

from __future__ import annotations

import base64
import functools
import heapq
import html
import importlib.resources
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol

from typing_extensions import override

from orcapod.core.sources.display_names import node_display_names
from orcapod.types.provenance import Highlight
from orcapod.utils.in_development import warn_in_development

if TYPE_CHECKING:
    from orcapod.pipeline.dag import OrcaDAG
    from orcapod.pipeline.run_summary import NodeRunSummary

__all__ = ["PipelineDiagram"]

Direction = Literal["LR", "TB"]


class _Drawable(Protocol):
    """What a diagram reads from a compiled ``Pipeline`` or ``PipelineJob``."""

    @property
    def name(self) -> tuple[str, ...]: ...

    @property
    def dag(self) -> OrcaDAG[Any]: ...


# The kind of each ``node_type``, as the diagram writes it.
_KIND_TEXT: dict[str, str] = {
    "source": "source",
    "operator": "operator",
    "function": "function pod",
    "side_effect_function": "function pod with side effects",
    "side_effect": "side-effect pod",
}


@dataclass(frozen=True)
class _Node:
    """One node of the model, with everything shown about it."""

    index: int
    label: str
    kind: str
    detail: str
    tag_schema: tuple[tuple[str, str], ...]
    data_schema: tuple[tuple[str, str], ...]
    status: str
    facts: tuple[tuple[str, str], ...]
    highlighted: bool = False
    mark: str = ""
    asked: str = ""
    shown: str = ""

    @property
    def text(self) -> str:
        """The name drawn: the bound source's assigned label (``shown``), else the
        node's label."""
        return self.shown or self.label

    @property
    def kind_line(self) -> str:
        """The kind, the detail and the status, as written under the label."""
        parts = [_KIND_TEXT.get(self.kind, self.kind), self.detail, self.status]
        return " · ".join(part for part in parts if part)


@dataclass(frozen=True)
class _Edge:
    """A stream from one node to another, by node index."""

    source: int
    target: int
    folded: tuple[str, ...] = ()
    highlighted: bool = False


# ---------------------------------------------------------------------------
# The model, read without computing
# ---------------------------------------------------------------------------


def _type_text(tp: Any) -> str:
    """A short text for a schema type: ``int``, ``list[float]``, ``str | None``."""
    if isinstance(tp, type) and not getattr(tp, "__args__", None):
        return tp.__name__
    return str(tp).replace("typing.", "")


def _schema_pairs(schema: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    """A schema's column names and type texts, in schema order."""
    return tuple((name, _type_text(tp)) for name, tp in schema.items())


def _node_detail(node: Any) -> str:
    """What the node is beyond its label: a source's class, an operator's or function's name."""
    if node.node_type == "source":
        bound = getattr(node, "bound_source", None)
        return type(bound).__name__ if bound is not None else ""
    name = _producer_name(node)
    if name is None:
        return ""
    # A label that is the name, numbered when several steps share it
    # (``SelectDataColumns_2``), says it already.
    label: str = node.label
    numbered = label.startswith(f"{name}_") and label[len(name) + 1 :].isdigit()
    return "" if label == name or numbered else name


def _producer_name(node: Any) -> str | None:
    """The operator's class name or the function's name, or ``None``.

    An operator is named by its class, never by its label, which the user chooses
    (``Join(label="joined")`` is a ``Join``).
    """
    producer = getattr(node, "producer", None)
    if producer is None:
        return None
    if node.node_type == "operator":
        return type(producer).__name__
    name = getattr(producer, "label", None)
    return name if isinstance(name, str) else None


def _collapsible(node: Any) -> bool:
    """Whether a node's operator only joins, chooses or renames columns.

    ``collapse=True`` folds such a node into the edge it lies on. Decided by the
    operator's class, never by a label, which the user chooses.
    """
    from orcapod.core.operators import (
        DropColumns,
        DropDataColumns,
        DropTagColumns,
        Join,
        MapColumns,
        MapData,
        MapTags,
        SelectColumns,
        SelectDataColumns,
        SelectTagColumns,
    )

    return node.node_type == "operator" and isinstance(
        getattr(node, "producer", None),
        (
            Join,
            SelectDataColumns,
            SelectTagColumns,
            SelectColumns,
            DropDataColumns,
            DropTagColumns,
            DropColumns,
            MapData,
            MapTags,
            MapColumns,
        ),
    )


def _run_facts(summary: NodeRunSummary | None) -> tuple[tuple[str, str], ...]:
    """What the job's last run did with a node's rows, from its ``NodeRunSummary``."""
    if summary is None:
        return ()
    counts = [f"{summary.rows} rows"]
    for name in ("computed", "reused", "failed", "held", "not_stored"):
        value = getattr(summary, name)
        if value:
            counts.append(f"{value} {name.replace('_', ' ')}")
    facts = [("job's last run", ", ".join(counts))]
    if summary.first_error is not None:
        error = summary.first_error
        facts.append(("first error", f"{type(error).__name__}: {error}"))
    return tuple(facts)


def _job_status(node: Any, upstream_statuses: Iterable[str]) -> str:
    """A job node's run state, from what it holds in memory; never reads or computes.

    Sources are ``bound`` or ``unbound``. Any other node is ``not run`` (no run in this
    process, or cleared), ``stale`` (an input changed since its run), ``failed`` (its
    last run, by the job or its own ``run()``, had failed rows), ``current`` (it holds
    a run that a run need not redo, whether the job's last run ran it or kept it),
    ``loaded only`` (filled by a load-only run) or ``partial`` (it ran, without the
    failed rows of a node above it, so the next run computes it again). Whether the
    job's last run kept a node is not a status: ``job.last_run`` records it for
    function nodes only, so a function node's details give its held rows instead.

    Args:
        node: The job node.
        upstream_statuses: The statuses of the nodes it reads from.
    """
    if node.node_type == "source":
        return "bound" if getattr(node, "bound_source", None) is not None else "unbound"
    if node.last_modified is None:
        return "not run"
    if node.is_stale:
        return "stale"
    if _failed_rows(node):
        return "failed"
    if node.holds_current_run():
        return "current"
    if node.holds_current_run(load_only=True):
        return "loaded only"
    if any(status in ("failed", "partial") for status in upstream_statuses):
        return "partial"
    return "not run"


def _failed_rows(node: Any) -> int:
    """The failed rows of a function job node's last run; 0 for any other node.

    The node's own count, not the job's summary: a node's own ``run()`` changes it and
    leaves ``job.last_run`` as it was. Read through one internal helper until a job
    node keeps the summary of its own last run (ITL-1209).
    """
    from orcapod.core.nodes.function_node import (
        FunctionJobNode,
        _last_run_failed,  # pyright: ignore[reportPrivateUsage]  # ITL-1209: internal until a node keeps its own run summary
    )

    return _last_run_failed(node) if isinstance(node, FunctionJobNode) else 0


def _topological_labels(
    preds: Mapping[str, Sequence[str]], succs: Mapping[str, Sequence[str]]
) -> list[str]:
    """The labels in topological order, ties broken by label: the same order every time."""
    indegree = {label: len(preds[label]) for label in preds}
    ready = [label for label, count in indegree.items() if count == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        label = heapq.heappop(ready)
        order.append(label)
        for successor in succs[label]:
            indegree[successor] -= 1
            if indegree[successor] == 0:
                heapq.heappush(ready, successor)
    return order


def _is_job(pipeline: _Drawable) -> bool:
    """Whether the diagram is a job's: decided by the object drawn, not by its nodes.

    An empty compiled job has no nodes, and is still a job.
    """
    from orcapod.pipeline.job import PipelineJob

    return isinstance(pipeline, PipelineJob)


def _build_model(
    pipeline: _Drawable,
    *,
    is_job: bool,
    collapse: bool,
    highlight: Highlight | None = None,
) -> tuple[list[_Node], list[_Edge]]:
    """Read a compiled pipeline or job into the model's nodes and edges.

    Args:
        pipeline: A compiled ``Pipeline`` or ``PipelineJob``.
        is_job: Whether it is a job: its nodes then show their run status, and the
            job's ``last_run`` gives row counts.
        collapse: Whether to fold the column-choosing, renaming and join operators that
            feed exactly one node into the edges they lie on.
        highlight: The nodes and edges to emphasize, by label. A folded edge is
            emphasized when its ends and every step folded into it are.

    Returns:
        The nodes in topological order (ties by label) and the edges, sorted.
    """
    dag = pipeline.dag
    # A job's ``last_run`` is a property of ``PipelineJob``; read only for a job.
    last_run = getattr(pipeline, "last_run", None) if is_job else None
    summaries: Mapping[str, NodeRunSummary] = (
        last_run.nodes if last_run is not None else {}
    )
    by_label: dict[str, Any] = {node.label: node for node in dag.nodes()}
    # From the edge list, never by looking a node up: a source node's hash changes
    # when a source is bound to it, so a lookup by node can miss.
    preds: dict[str, list[str]] = {label: [] for label in by_label}
    succs: dict[str, list[str]] = {label: [] for label in by_label}
    for upstream, downstream in dag.edges():
        preds[downstream.label].append(upstream.label)
        succs[upstream.label].append(downstream.label)
    for names in (*preds.values(), *succs.values()):
        names.sort()

    folded: set[str] = set()
    if collapse:
        folded = {
            label
            for label, node in by_label.items()
            if _collapsible(node) and len(succs[label]) == 1
        }
        if highlight is not None:
            # The node the question was asked at is always shown, and so is a
            # highlighted node no highlighted edge leaves (where a path ends, such as
            # the step that removed a record): folded, its emphasis would be lost.
            # The edge decides, not the successor: a highlighted successor reached by
            # an edge that is not highlighted does not carry the path on.
            folded.discard(highlight.asked)
            folded -= {
                label
                for label in highlight.nodes
                if not any((label, s) in highlight.edges for s in succs.get(label, ()))
            }

    shown = [
        label for label in _topological_labels(preds, succs) if label not in folded
    ]
    index = {label: i for i, label in enumerate(shown)}

    def ends(label: str, via: tuple[str, ...]) -> list[tuple[str, tuple[str, ...]]]:
        """The shown nodes upstream of ``label`` and the folded steps on the way."""
        if label not in folded:
            return [(label, via)]
        found: list[tuple[str, tuple[str, ...]]] = []
        for upstream in preds[label]:
            found.extend(ends(upstream, (label, *via)))
        return found

    edges: dict[tuple[int, int], tuple[str, ...]] = {}
    folded_into: dict[str, list[str]] = {}
    for label in shown:
        for upstream in preds[label]:
            for end, via in ends(upstream, ()):
                key = (index[end], index[label])
                edges[key] = tuple(dict.fromkeys((*edges.get(key, ()), *via)))
                folded_into.setdefault(label, []).extend(via)

    # Every node's status, folded ones too, in topological order: a node's status can
    # depend on the statuses of the nodes it reads from.
    statuses: dict[str, str] = {}
    if is_job:
        for label in _topological_labels(preds, succs):
            statuses[label] = _job_status(
                by_label[label], (statuses[upstream] for upstream in preds[label])
            )

    source_names = node_display_names(by_label.values())
    nodes: list[_Node] = []
    for label in shown:
        node = by_label[label]
        summary = summaries.get(label)
        facts = _run_facts(summary) if is_job else ()
        source_text, source_id = source_names.get(label, ("", ""))
        if source_id:
            facts = (("source id", source_id), *facts)
        try:
            tag_schema, data_schema = node.output_schema()
        except ValueError as error:
            # A loaded node whose saved column types cannot be restored (a class only
            # the writer registered) refuses to give its schema; the diagram still
            # draws the graph and says why the columns are missing.
            tag_schema, data_schema = {}, {}
            facts = (*facts, ("schema", f"not available: {error}"))
        steps = list(dict.fromkeys(folded_into.get(label, [])))
        if steps:
            facts = (*facts, ("folded steps", ", ".join(steps)))
        nodes.append(
            _Node(
                index=index[label],
                label=label,
                kind=node.node_type,
                detail=_node_detail(node),
                tag_schema=_schema_pairs(tag_schema),
                data_schema=_schema_pairs(data_schema),
                # A highlight shows the provenance question only: no run status.
                status="" if highlight is not None else statuses.get(label, ""),
                facts=facts,
                shown=source_text,
                highlighted=highlight is not None and label in highlight.nodes,
                mark="" if highlight is None else highlight.mark(label),
                asked=highlight.asked_text or "asked here"
                if highlight is not None and highlight.asked == label
                else "",
            )
        )

    def lit(source: int, target: int, via: tuple[str, ...]) -> bool:
        """Whether a displayed edge is emphasized: some highlighted path leads from
        its start to its end through the steps folded into it (several folded paths
        merge into one displayed edge, so their labels are not one chain)."""
        if highlight is None:
            return False
        start, end = shown[source], shown[target]
        if not via:
            return (start, end) in highlight.edges
        inner = set(via)
        frontier, seen = [start], {start}
        while frontier:
            current = frontier.pop()
            for upstream, downstream in highlight.edges:
                if upstream != current:
                    continue
                if downstream == end:
                    return True
                if downstream in inner and downstream not in seen:
                    seen.add(downstream)
                    frontier.append(downstream)
        return False

    edge_list = [
        _Edge(
            source=source,
            target=target,
            folded=via,
            highlighted=lit(source, target, via),
        )
        for (source, target), via in sorted(edges.items())
    ]
    return nodes, edge_list


# ---------------------------------------------------------------------------
# Layout: layered (Sugiyama), deterministic
# ---------------------------------------------------------------------------

_FONT_PX = 13
_SMALL_PX = 11
_CHAR_W = 7.4  # average advance of a 13 px sans-serif character
_SMALL_CHAR_W = _CHAR_W * _SMALL_PX / _FONT_PX
_NODE_H = 44.0  # a node with one kind line
_LINE_H = 13.0  # each further kind line
_PAD = 14.0
_MIN_W = 96.0
_MAX_W = 260.0
_LAYER_GAP = 56.0
_NODE_GAP = 22.0
_DUMMY_GAP = 10.0
_MARGIN = 12.0
_SWEEPS = 12
_PLACEMENT_ROUNDS = 8


@dataclass
class _Box:
    """A laid-out vertex: a node, or a point of a long edge (negative key)."""

    key: int
    layer: int
    width: float
    height: float
    pos: float = 0.0  # centre along the layer: y for LR, x for TB


@dataclass
class _Layout:
    """Where each node and edge is drawn."""

    width: float
    height: float
    boxes: dict[int, tuple[float, float, float, float]]  # node index: x, y, w, h
    edges: list[tuple[_Edge, list[tuple[float, float]]]] = field(default_factory=list)


def _text_room(node: _Node) -> float:
    """The width of a node's text: its label or the widest part of its kind line (the
    kind, the detail, the status), within the node width limits (ITL-1232)."""
    parts = [*node.kind_line.split(" · "), node.mark, node.asked]
    text = max(
        len(node.text) * _CHAR_W, max(len(part) for part in parts) * _SMALL_CHAR_W
    )
    return max(_MIN_W - 2 * _PAD, min(_MAX_W - 2 * _PAD, text))


def _kind_lines(node: _Node) -> tuple[str, ...]:
    """The kind line wrapped at its separators to the node's text width.

    The parts are kept in order and joined with `` · `` while they fit; a part wider
    than the largest node is clipped when drawn. Nothing is dropped: the joined lines
    are the kind line.
    """
    room = _text_room(node)
    lines: list[str] = []
    for part in node.kind_line.split(" · "):
        joined = f"{lines[-1]} · {part}" if lines else part
        if lines and len(joined) * _SMALL_CHAR_W <= room:
            lines[-1] = joined
        else:
            lines.append(part)
    if node.mark:
        # A highlight's mark (a trace's computed / reused) on a line of its own.
        lines.append(node.mark)
    if node.asked:
        # The node the question was asked at ("traced from here"), on its own line.
        lines.append(node.asked)
    return tuple(lines)


def _node_size(node: _Node) -> tuple[float, float]:
    """A node's width and height: room for its label and its wrapped kind line."""
    return (
        _text_room(node) + 2 * _PAD,
        _NODE_H + _LINE_H * (len(_kind_lines(node)) - 1),
    )


def _crossings(
    upper: Sequence[_Box], lower: Sequence[_Box], links: Mapping[int, list[int]]
) -> int:
    """The number of edge crossings between two adjacent layers."""
    position = {box.key: i for i, box in enumerate(lower)}
    pairs = sorted(
        (i, position[target])
        for i, box in enumerate(upper)
        for target in links.get(box.key, ())
    )
    count = 0
    for a, (_, low_a) in enumerate(pairs):
        for upper_b, low_b in pairs[a + 1 :]:
            if low_b < low_a and upper_b != pairs[a][0]:
                count += 1
    return count


def _layout(
    nodes: Sequence[_Node], edges: Sequence[_Edge], direction: Direction
) -> _Layout:
    """Lay the nodes out in layers and route the edges through them.

    Layers by longest path (a root sits just before its first consumer); a point per
    layer crossed by a long edge; barycenter sweeps keeping the order with the fewest
    crossings; positions pulled towards the neighbours' average with the order and gaps
    kept. Every step iterates in a fixed order, so the same graph gives the same layout.

    Args:
        nodes: The nodes, in topological order.
        edges: The edges, sorted.
        direction: ``"LR"`` (left to right) or ``"TB"`` (top to bottom).

    Returns:
        The box of each node and the points of each edge.
    """
    across = direction == "LR"  # layers advance along x
    preds: dict[int, list[int]] = {node.index: [] for node in nodes}
    succs: dict[int, list[int]] = {node.index: [] for node in nodes}
    for edge in edges:
        preds[edge.target].append(edge.source)
        succs[edge.source].append(edge.target)

    layer: dict[int, int] = {}
    for node in nodes:
        layer[node.index] = max((layer[p] + 1 for p in preds[node.index]), default=0)
    for node in nodes:
        if not preds[node.index] and succs[node.index]:
            layer[node.index] = min(layer[s] for s in succs[node.index]) - 1

    boxes: dict[int, _Box] = {}
    for node in nodes:
        width, height = _node_size(node)
        boxes[node.index] = _Box(node.index, layer[node.index], width, height)
    chains: dict[tuple[int, int], list[int]] = {}
    links: dict[int, list[int]] = {}
    back: dict[int, list[int]] = {}
    dummy = -1
    for edge in edges:
        chain = [edge.source]
        for crossed in range(layer[edge.source] + 1, layer[edge.target]):
            boxes[dummy] = _Box(dummy, crossed, 0.0, 0.0)
            chain.append(dummy)
            dummy -= 1
        chain.append(edge.target)
        chains[(edge.source, edge.target)] = chain
        for upper, lower in zip(chain, chain[1:]):
            links.setdefault(upper, []).append(lower)
            back.setdefault(lower, []).append(upper)

    n_layers = max((box.layer for box in boxes.values()), default=-1) + 1
    layers: list[list[_Box]] = [[] for _ in range(n_layers)]
    for key in sorted(boxes, key=lambda k: (k < 0, abs(k))):
        layers[boxes[key].layer].append(boxes[key])

    def total_crossings() -> int:
        return sum(
            _crossings(layers[i], layers[i + 1], links) for i in range(n_layers - 1)
        )

    def sweep(downward: bool) -> None:
        indices = range(1, n_layers) if downward else range(n_layers - 2, -1, -1)
        neighbours = back if downward else links
        for i in indices:
            reference = layers[i - 1] if downward else layers[i + 1]
            position = {box.key: j for j, box in enumerate(reference)}

            def barycenter(item: tuple[int, _Box]) -> tuple[float, int]:
                j, box = item
                found = [
                    position[k] for k in neighbours.get(box.key, ()) if k in position
                ]
                return (sum(found) / len(found) if found else float(j), j)

            layers[i] = [box for _, box in sorted(enumerate(layers[i]), key=barycenter)]

    best = [list(row) for row in layers]
    best_count = total_crossings()
    for sweep_number in range(_SWEEPS):
        sweep(downward=sweep_number % 2 == 0)
        count = total_crossings()
        if count < best_count:
            best, best_count = [list(row) for row in layers], count
    layers = best

    def extent(box: _Box) -> float:
        return (box.height if across else box.width) if box.key >= 0 else 0.0

    def separation(row: Sequence[_Box], j: int) -> float:
        both_nodes = row[j - 1].key >= 0 and row[j].key >= 0
        gap = _NODE_GAP if both_nodes else _DUMMY_GAP
        return (extent(row[j - 1]) + extent(row[j])) / 2 + gap

    for row in layers:
        cursor = 0.0
        for j, box in enumerate(row):
            if j:
                cursor = row[j - 1].pos + separation(row, j)
            else:
                cursor = extent(box) / 2
            box.pos = cursor

    def place(row: Sequence[_Box], wanted: Sequence[float]) -> None:
        # The average of the solution pushed downwards and the one pushed upwards: both
        # keep the order and the gaps, so their average does, and neither side is
        # favoured (pushing one way only drifts the whole graph into a staircase).
        down = list(wanted)
        for j in range(1, len(row)):
            down[j] = max(down[j], down[j - 1] + separation(row, j))
        up = list(wanted)
        for j in range(len(row) - 2, -1, -1):
            up[j] = min(up[j], up[j + 1] - separation(row, j + 1))
        for box, low, high in zip(row, down, up):
            box.pos = (low + high) / 2

    for round_number in range(_PLACEMENT_ROUNDS):
        downward = round_number % 2 == 0
        indices = range(1, n_layers) if downward else range(n_layers - 2, -1, -1)
        neighbours = back if downward else links
        for i in indices:
            wanted: list[float] = []
            for box in layers[i]:
                found = [boxes[k].pos for k in neighbours.get(box.key, ())]
                wanted.append(sum(found) / len(found) if found else box.pos)
            place(layers[i], wanted)

    lowest = min((box.pos - extent(box) / 2 for box in boxes.values()), default=0.0)
    for box in boxes.values():
        box.pos -= lowest
    breadth = max((box.pos + extent(box) / 2 for box in boxes.values()), default=0.0)

    depth = [
        max(
            ((box.width if across else box.height) for box in row if box.key >= 0),
            default=0.0,
        )
        for row in layers
    ]
    starts: list[float] = []
    cursor = _MARGIN
    for layer_depth in depth:
        starts.append(cursor)
        cursor += layer_depth + _LAYER_GAP
    total_depth = (cursor - _LAYER_GAP + _MARGIN) if layers else 2 * _MARGIN

    def point(along: float, at: float) -> tuple[float, float]:
        """A point from its coordinate along the layers and its position in a layer."""
        return (along, _MARGIN + at) if across else (_MARGIN + at, along)

    placed: dict[int, tuple[float, float, float, float]] = {}
    for node in nodes:
        box = boxes[node.index]
        middle = starts[box.layer] + depth[box.layer] / 2
        x, y = point(middle, box.pos)
        placed[node.index] = (
            x - box.width / 2,
            y - box.height / 2,
            box.width,
            box.height,
        )

    def node_end(key: int, leaving: bool) -> tuple[float, float]:
        x, y, w, h = placed[key]
        if across:
            return (x + w if leaving else x, y + h / 2)
        return (x + w / 2, y + h if leaving else y)

    routed: list[tuple[_Edge, list[tuple[float, float]]]] = []
    for edge in edges:
        chain = chains[(edge.source, edge.target)]
        points = [node_end(edge.source, leaving=True)]
        for key in chain[1:-1]:
            box = boxes[key]
            start = starts[box.layer]
            points.extend(
                [point(start, box.pos), point(start + depth[box.layer], box.pos)]
            )
        points.append(node_end(edge.target, leaving=False))
        routed.append((edge, points))

    breadth_total = breadth + 2 * _MARGIN
    width, height = (
        (total_depth, breadth_total) if across else (breadth_total, total_depth)
    )
    return _Layout(width=width, height=height, boxes=placed, edges=routed)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

# A solid stroke and a 14 % fill of each kind's colour read on light and dark notebook
# themes alike; text and edges use ``currentColor``, the notebook's own text colour.
_HIGHLIGHT_COLOUR = "currentColor"
"""The colour of a highlight's nodes and edges (``diagram(highlight=...)``): the text
colour, which no kind or status uses (red and amber mean failed and stale), dark on a
light page and light on a dark one."""
_CAPTION_H = 24.0
"""The height of the caption line a highlight with a note adds above the picture."""


def _layout_edges(layout: _Layout) -> list[_Edge]:
    """The edges a layout draws."""
    return [edge for edge, _ in layout.edges]


_KIND_COLOUR: dict[str, str] = {
    "source": "#2e9e5b",
    "operator": "#d08a16",
    "function": "#4c6ef5",
    "side_effect_function": "#9b4cf5",
    "side_effect": "#c2417a",
}
_STATUS_COLOUR: dict[str, str] = {
    "current": "#2e9e5b",
    "bound": "#2e9e5b",
    "loaded only": "#3b8fd1",
    "stale": "#d08a16",
    "partial": "#d08a16",
    "failed": "#d63c3c",
    "not run": "#8a8a8a",
    "unbound": "#8a8a8a",
}
# Statuses drawn with a dashed outline, so they are not told by colour alone.
_DASHED_STATUSES = frozenset({"not run", "stale", "partial", "unbound"})
_ARROW = 7.0


# Code points XML 1.0 does not allow (C0 controls other than tab, line feed and carriage
# return; surrogates; U+FFFE and U+FFFF): one in an SVG makes the whole picture fail to
# parse, so each is shown as U+FFFD. An error message with terminal colours (ESC) is
# the usual source.
_XML_INVALID = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]")


_SURROGATE = re.compile("[\ud800-\udfff]")


def _escape(text: str) -> str:
    """Text escaped for HTML and SVG content and attribute values.

    Characters XML does not allow are replaced by U+FFFD first, so the SVG (an image
    and inline) always parses; the model block keeps the text as it is, JSON-escaped.
    """
    return html.escape(_XML_INVALID.sub("\ufffd", text), quote=True)


def _num(value: float) -> str:
    """A coordinate with at most one decimal, without trailing zeros."""
    text = f"{value:.1f}"
    return text[:-2] if text.endswith(".0") else text


def _edge_path(points: Sequence[tuple[float, float]], across: bool) -> str:
    """A smooth path through the points, leaving and entering along the layer axis."""
    out = [f"M{_num(points[0][0])},{_num(points[0][1])}"]
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        if across:
            mid = (x1 - x0) / 2
            controls = (x0 + mid, y0, x1 - mid, y1)
        else:
            mid = (y1 - y0) / 2
            controls = (x0, y0 + mid, x1, y1 - mid)
        cx0, cy0, cx1, cy1 = (_num(v) for v in controls)
        out.append(f"C{cx0},{cy0} {cx1},{cy1} {_num(x1)},{_num(y1)}")
    return " ".join(out)


def _arrow_head(end: tuple[float, float], across: bool) -> str:
    """A filled triangle pointing along the layer axis, its tip at ``end``."""
    x, y = end
    half = _ARROW / 2
    if across:
        corners = [(x - _ARROW, y - half), (x, y), (x - _ARROW, y + half)]
    else:
        corners = [(x - half, y - _ARROW), (x, y), (x + half, y - _ARROW)]
    return "M" + " L".join(f"{_num(a)},{_num(b)}" for a, b in corners) + " Z"


def _clip(text: str, width: float, char_width: float) -> str:
    """The text shortened with an ellipsis to fit the node's width."""
    room = int((width - 2 * _PAD + 8) / char_width)
    return text if len(text) <= room else text[: max(room - 1, 1)] + "…"


def _schema_text(pairs: Sequence[tuple[str, str]]) -> str:
    return ", ".join(f"{name}: {tp}" for name, tp in pairs) or "none"


def _tooltip(node: _Node, inputs: Sequence[str]) -> str:
    """The node's details as plain text, shown on hover over the inline picture."""
    lines = [f"{node.text} ({node.kind_line})"]
    lines.append(f"tags: {_schema_text(node.tag_schema)}")
    lines.append(f"data: {_schema_text(node.data_schema)}")
    if inputs:
        lines.append(f"inputs: {', '.join(inputs)}")
    lines.extend(f"{name}: {text}" for name, text in node.facts)
    return "\n".join(lines)


def _svg(
    title: str,
    nodes: Sequence[_Node],
    inputs: Mapping[int, Sequence[str]],
    layout: _Layout,
    across: bool,
    standalone: bool,
    caption: str = "",
) -> str:
    """The SVG picture; complete without any style sheet or script.

    Args:
        title: The diagram's title, the picture's accessible name.
        nodes: The nodes.
        inputs: The labels of each node's inputs, by node index.
        layout: Where the nodes and edges are drawn.
        across: Whether the layers advance left to right.
        standalone: Draw it for an image of its own: on a light background, in a fixed
            text colour, since an image does not take the page's colours. Otherwise
            the text and edges take the page's text colour and the background is
            transparent.
        caption: A line drawn above the picture (a highlight's note), or ``""``.

    Returns:
        The ``<svg>`` element.
    """
    top = _CAPTION_H if caption else 0.0
    width, height = _num(layout.width), _num(layout.height + top)
    colour = ' color="#1f1f1f"' if standalone else ' style="max-width:100%;height:auto"'
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" class="op-dag-svg" role="group" '
        f'width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
        'font-family="system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif"'
        f"{colour}>",
        f"<title>{_escape(title)}</title>",
    ]
    if standalone:
        parts.append(
            f'<rect class="op-dag-bg" width="{width}" height="{height}" rx="6" '
            'fill="#ffffff"/>'
        )
    if caption:
        parts.append(
            f'<text class="op-dag-caption" x="{_num(_MARGIN)}" y="{_num(top - 6)}" '
            f'font-size="{_FONT_PX}" font-weight="600" fill="currentColor">'
            f"{_escape(_clip(caption, layout.width - 2 * _MARGIN, _CHAR_W))}</text>"
            f'<g transform="translate(0,{_num(top)})">'
        )
    # A highlight (``diagram(highlight=...)``) draws its edges in its colour and dims
    # the other edges and the nodes outside it.
    lit = any(node.highlighted for node in nodes) or any(
        edge.highlighted for edge in _layout_edges(layout)
    )
    parts.append(
        '<g class="op-dag-edges" fill="none" stroke="currentColor" '
        f'stroke-opacity="{"0.3" if lit else "0.5"}" stroke-width="1.4">'
    )
    for edge, points in layout.edges:
        tip = f"{nodes[edge.source].text} → {nodes[edge.target].text}"
        if edge.folded:
            tip += f" (through {', '.join(edge.folded)})"
        dash = ' stroke-dasharray="5 3"' if edge.folded else ""
        accent = (
            f' class="op-dag-highlight" stroke="{_HIGHLIGHT_COLOUR}" '
            'stroke-opacity="1" stroke-width="2.6"'
            if edge.highlighted
            else ""
        )
        head_fill = _HIGHLIGHT_COLOUR if edge.highlighted else "currentColor"
        parts.append(
            f'<g data-source="{edge.source}" data-target="{edge.target}"{accent}>'
            f"<title>{_escape(tip)}</title>"
            f'<path d="{_edge_path(points, across)}"{dash}/>'
            f'<path d="{_arrow_head(points[-1], across)}" fill="{head_fill}" '
            'stroke="none"/></g>'
        )
    parts.append('</g><g class="op-dag-nodes">')
    for node in nodes:
        x, y, w, h = layout.boxes[node.index]
        colour = _KIND_COLOUR.get(node.kind, "#888888")
        radius = (
            min(h, _NODE_H) / 2
            if node.kind == "source"
            else 3.0
            if node.kind == "operator"
            else 8.0
        )
        stroke_width = "1.4" if node.kind == "operator" else "2.2"
        dash = ' stroke-dasharray="4 3"' if node.status in _DASHED_STATUSES else ""
        label = f"{node.text}, {node.kind_line}"
        emphasis = ""
        stroke = colour
        if node.highlighted:
            # The kind's fill is kept; the outline is the highlight's.
            emphasis = " op-dag-highlight"
            stroke_width = "3.2"
            stroke = _HIGHLIGHT_COLOUR
        elif lit and not node.asked:
            emphasis = '" opacity="0.4'
        parts.append(
            f'<g class="op-dag-node{emphasis}" data-index="{node.index}" tabindex="0" '
            f'role="button" aria-label="{_escape(label)}" '
            f'transform="translate({_num(x)},{_num(y)})">'
            f"<title>{_escape(_tooltip(node, inputs.get(node.index, ())))}</title>"
            f'<rect width="{_num(w)}" height="{_num(h)}" rx="{_num(radius)}" '
            f'fill="{colour}" fill-opacity="0.14" stroke="{stroke}" '
            f'stroke-width="{stroke_width}"{dash}/>'
        )
        if node.asked:
            # The node the question was asked at: a second outline around it, in the
            # text colour (readable on a light and a dark page).
            parts.append(
                f'<rect class="op-dag-asked" x="-5" y="-5" width="{_num(w + 10)}" '
                f'height="{_num(h + 10)}" rx="{_num(radius + 5)}" fill="none" '
                'stroke="currentColor" stroke-width="1.6"/>'
            )
        if node.kind in ("side_effect", "side_effect_function"):
            parts.append(
                f'<rect x="3" y="3" width="{_num(w - 6)}" height="{_num(h - 6)}" '
                f'rx="{_num(max(radius - 3, 1.0))}" fill="none" stroke="{colour}" '
                'stroke-width="1"/>'
            )
        status_colour = _STATUS_COLOUR.get(node.status)
        if status_colour is not None:
            parts.append(
                f'<circle cx="{_num(w - 9)}" cy="9" r="4" fill="{status_colour}"/>'
            )
        kind_lines = _kind_lines(node)
        for line_number, line in enumerate(kind_lines):
            # A highlight's mark and the asked-at line (the last lines) are drawn in
            # bold at full strength.
            emphasized = bool(node.mark) + bool(node.asked)
            is_mark = line_number >= len(kind_lines) - emphasized
            weight = (
                ' font-weight="600" class="op-dag-mark"'
                if is_mark
                else ' fill-opacity="0.75"'
            )
            parts.append(
                f'<text x="{_num(w / 2)}" y="{_num(16 + _LINE_H * line_number)}" '
                f'text-anchor="middle" font-size="{_SMALL_PX}" fill="currentColor"'
                f"{weight}>{_escape(_clip(line, w, _SMALL_CHAR_W))}</text>"
            )
        label_y = 34 + _LINE_H * (len(kind_lines) - 1)
        parts.append(
            f'<text x="{_num(w / 2)}" y="{_num(label_y)}" text-anchor="middle" '
            f'font-size="{_FONT_PX}" font-weight="600" fill="currentColor">'
            f"{_escape(_clip(node.text, w, _CHAR_W))}</text></g>"
        )
    parts.append("</g></g></svg>" if caption else "</g></svg>")
    return "".join(parts)


def _table(nodes: Sequence[_Node], inputs: Mapping[int, Sequence[str]]) -> str:
    """Every node with its inputs, schema and run facts: shown where scripts do not run."""
    rows = "".join(
        "<tr>"
        f"<td><b>{_escape(node.text)}</b></td>"
        f"<td>{_escape(node.kind_line)}</td>"
        f"<td>{_escape(', '.join(inputs.get(node.index, ())))}</td>"
        f"<td>{_escape(_schema_text(node.tag_schema))}</td>"
        f"<td>{_escape(_schema_text(node.data_schema))}</td>"
        f"<td>{_escape('; '.join(f'{n}: {t}' for n, t in node.facts))}</td>"
        "</tr>"
        for node in nodes
    )
    return (
        '<details class="op-dag-table"><summary>Nodes</summary><table>'
        "<thead><tr><th>node</th><th>kind</th><th>inputs</th><th>tags</th>"
        "<th>data</th><th>run</th></tr></thead>"
        f"<tbody>{rows}</tbody></table></details>"
    )


# The style sheet and the script of the notebook view are package files beside this
# module, ``diagram_assets/diagram.css`` and ``diagram_assets/diagram.js``, written as
# ordinary source with their comments. Each output embeds them inline (a notebook
# cannot fetch a package file), compacted by ``_compact``.
_ASSETS = "diagram_assets"
# Text that would end the ``<style>`` or ``<script>`` element an asset is embedded in,
# or start an HTML comment there; an asset holding one is refused.
_UNSAFE_IN_ELEMENT = re.compile(r"</(?:style|script)|<!--", re.IGNORECASE)
_CSS_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)


def _compact(text: str, *, css: bool) -> str:
    """An asset as embedded: comments, indentation and blank lines removed.

    A deterministic text step, not a minifier: every remaining line is kept as written,
    and line breaks are kept, so a script's statements end where they did. CSS comments
    are ``/* ... */`` blocks; script comments are whole lines starting with ``//`` (the
    only kind ``diagram.js`` uses).

    Args:
        text: The file's text.
        css: Whether it is the style sheet; otherwise the script.

    Returns:
        The lines left, stripped, joined with line breaks.
    """
    if css:
        text = _CSS_COMMENT.sub("", text)
    lines = (line.strip() for line in text.splitlines())
    return "\n".join(
        line for line in lines if line and (css or not line.startswith("//"))
    )


@functools.cache
def _asset(name: str) -> str:
    """The embedded text of one asset file, read once per process, on first use.

    Args:
        name: ``"diagram.css"`` or ``"diagram.js"``.

    Returns:
        The compacted text.

    Raises:
        ValueError: If the file holds text that would end its element or start an HTML
            comment (it never does as shipped; this keeps an edit from breaking out).
    """
    text = (
        importlib.resources.files("orcapod.pipeline")
        .joinpath(_ASSETS)
        .joinpath(name)
        .read_text(encoding="utf-8")
    )
    found = _UNSAFE_IN_ELEMENT.search(text)
    if found is not None:
        raise ValueError(f"{name} contains {found.group(0)!r}, unsafe inline")
    return _compact(text, css=name.endswith(".css"))


def _inputs(nodes: Sequence[_Node], edges: Sequence[_Edge]) -> dict[int, list[str]]:
    """The drawn names of each node's inputs, by node index, in edge order."""
    inputs: dict[int, list[str]] = {}
    for edge in edges:
        inputs.setdefault(edge.target, []).append(nodes[edge.source].text)
    return inputs


def _json_for_script(value: object) -> str:
    """JSON that is safe inside a ``<script>`` element: no ``<``, ``>`` or ``&`` in it."""
    # ASCII only: a lone surrogate in a label stays an escape, so the page can be saved
    # as UTF-8.
    text = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return text.replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")


# The largest scale at which the notebook shows a picture (ITL-1231): the image is as
# wide as its container but never wider than this times its own width, so a two-node
# job is not blown up. At 1.25 the node labels (13 px) are drawn at 16 px and the kind
# line (11 px) at 13.75 px, about the size of a notebook's own text. The script has the
# same maximum (``MAX_SCALE`` in ``diagram_assets/diagram.js``).
_MAX_SCALE = 1.25


# Above this many nodes, the output embeds only the shown layout, without the other
# direction or the other collapse state, so the view has no direction or collapse
# control (ITL-1232). Each further layout adds about the size of the first: the
# tutorial job's output is 31 KiB with one layout and 61 KiB with four; at this limit
# four layouts make about 0.5 MiB (one layout of 400 nodes, the default max_nodes,
# about 0.65 MiB).
_MAX_EMBEDDED_NODES = 100


# Shown next to the title, in the rendered view and in the no-script fallback: the
# diagram and its interface are in development (developer, November 17, 2026). Plain
# text in the text colour. The first diagram of a process also issues
# ``InDevelopmentWarning`` (``__init__``).
_PREVIEW_NOTE = "In development: the diagram and its interface may change"
# The separator shows only where the style sheet is removed (an untrusted notebook),
# where the tag is plain text.
_PREVIEW_TAG = (
    '<span class="op-dag-sep">· </span>'
    f'<span class="op-dag-preview" title="{_PREVIEW_NOTE}" '
    f'aria-label="{_PREVIEW_NOTE}">preview</span>'
)


class PipelineDiagram:
    """The DAG of a pipeline or job, drawn for a notebook.

    In development: this interface may change.

    Shown at the end of a notebook cell it is an SVG picture of the nodes (sources,
    operators, function pods, side-effect pods) and the streams between them. Where the
    notebook runs scripts, the picture can be dragged and zoomed, and selecting a node
    shows its tag and data schema and, for a job, its run status and the last run's row
    counts. Without scripts the picture and a table of the nodes remain.
    ``print`` gives a plain-text listing.

    Making a diagram computes nothing: it reads each node's predicted ``output_schema()``,
    and for a job what each node holds in memory and ``job.last_run``.

    Use ``Pipeline.diagram()`` or ``PipelineJob.diagram()`` to make one. The first
    diagram made in a process issues ``InDevelopmentWarning`` (once per process).

    Args:
        pipeline: A compiled ``Pipeline`` or ``PipelineJob``.
        direction: ``"LR"`` lays the graph out left to right, ``"TB"`` top to bottom.
        collapse: Fold the operators that only choose, rename or join columns (joins,
            column selections, drops and maps) and feed exactly one node into the edges
            they lie on: such an edge is dashed, and the folded steps are listed in the
            details of the node it leads to.
        max_nodes: Above this many nodes (after folding), the notebook shows the
            plain-text listing instead of the picture.
        highlight: Nodes and edges to emphasize, by label, with a note
            (``Trace.highlight()``, ``Absence.highlight()``): outlined in the text
            colour (no status uses it), the other nodes and edges dimmed, the note as
            a caption above the picture, and a node's mark (a trace's ``computed`` /
            ``reused``) on a line of its own; the listing marks them with ``*`` and
            ``[mark]``.

    Raises:
        ValueError: If ``direction`` is not ``"LR"`` or ``"TB"``, ``max_nodes`` is not
            positive, or ``highlight`` names a node the pipeline does not have.
        RuntimeError: If the pipeline has not been compiled.
    """

    def __init__(
        self,
        pipeline: _Drawable,
        *,
        direction: Direction = "LR",
        collapse: bool = False,
        max_nodes: int = 400,
        highlight: Highlight | None = None,
    ) -> None:
        if direction not in ("LR", "TB"):
            raise ValueError(f"direction must be 'LR' or 'TB', not {direction!r}")
        if max_nodes < 1:
            raise ValueError(f"max_nodes must be at least 1, not {max_nodes!r}")
        warn_in_development(
            "pipeline diagram",
            "The pipeline diagram (Pipeline.diagram(), PipelineJob.diagram(), "
            "PipelineDiagram and the notebook display of a pipeline or job) is in "
            "development: its interface and appearance may change in any release.",
        )
        if highlight is not None:
            labels = {node.label for node in pipeline.dag.nodes()}
            unknown = sorted(
                {
                    *highlight.nodes,
                    *(end for edge in highlight.edges for end in edge),
                    *(label for label, _ in highlight.marks),
                    *((highlight.asked,) if highlight.asked else ()),
                }
                - labels
            )
            if unknown:
                raise ValueError(
                    f"highlight names {unknown[0]!r}, which is not a node of this "
                    "pipeline"
                )
        self._highlight: Highlight | None = highlight
        is_job = _is_job(pipeline)
        self._kind: str = "job" if is_job else "pipeline"
        self._name: str = ".".join(pipeline.name)
        self._direction: Direction = direction
        self._max_nodes: int = max_nodes
        self._collapse: bool = collapse
        nodes, edges = _build_model(
            pipeline, is_job=is_job, collapse=collapse, highlight=highlight
        )
        self._nodes: list[_Node] = nodes
        self._edges: list[_Edge] = edges
        # The other layout, for the notebook's collapse checkbox: kept only when folding
        # changes something. Built from the same read of the graph, computing nothing.
        other_nodes, other_edges = _build_model(
            pipeline, is_job=is_job, collapse=not collapse, highlight=highlight
        )
        self._other: tuple[list[_Node], list[_Edge]] | None = (
            None if len(other_nodes) == len(nodes) else (other_nodes, other_edges)
        )

    @property
    def title(self) -> str:
        """``Pipeline '<name>'`` or ``Job '<name>'``.

        In development: this interface may change.
        """
        return f"{self._kind.capitalize()} {self._name!r}"

    def _inputs(self) -> dict[int, list[str]]:
        """The labels of each node's inputs, in edge order."""
        return _inputs(self._nodes, self._edges)

    def to_dict(self) -> dict[str, Any]:
        """The diagram's model: plain data that any renderer can draw.

        In development: this interface may change.

        Its shape may change: it carries no version marker yet.

        Returns:
            ``{"kind", "name", "title", "nodes", "edges"}``. Each node has ``index``,
            ``label``, ``kind`` (its ``node_type``), ``kind_text``, ``detail``,
            ``status`` (``""`` for a pipeline), ``tag_schema`` and ``data_schema``
            (lists of ``[column, type text]``) and ``facts`` (lists of ``[name, text]``);
            a source node whose bound source has an assigned label also has ``text``, the
            name drawn, and the fact ``source id``;
            each edge has ``source`` and ``target`` (node indices) and ``folded``
            (the labels of the steps folded into it). With a highlight, every node and
            edge also has ``highlighted``, and the model has the highlight's ``note``.
        """
        return self._model(self._nodes, self._edges)

    def _model(self, nodes: list[_Node], edges: list[_Edge]) -> dict[str, Any]:
        """The model of one layout: the given nodes and edges."""
        model = self._plain_model(nodes, edges)
        if self._highlight is not None:
            model["note"] = self._highlight.note
            for entry, node in zip(model["nodes"], nodes):
                entry["highlighted"] = node.highlighted
                if node.mark:
                    entry["mark"] = node.mark
                if node.asked:
                    entry["asked"] = node.asked
            for entry, edge in zip(model["edges"], edges):
                entry["highlighted"] = edge.highlighted
        return model

    def _plain_model(self, nodes: list[_Node], edges: list[_Edge]) -> dict[str, Any]:
        """The model of one layout without a highlight."""
        return {
            "kind": self._kind,
            "name": self._name,
            "title": self.title,
            "nodes": [
                {
                    "index": node.index,
                    "label": node.label,
                    "kind": node.kind,
                    "kind_text": _KIND_TEXT.get(node.kind, node.kind),
                    "detail": node.detail,
                    "status": node.status,
                    "tag_schema": [list(pair) for pair in node.tag_schema],
                    "data_schema": [list(pair) for pair in node.data_schema],
                    "facts": [list(fact) for fact in node.facts],
                    # Only where a source's assigned label is drawn (ITL-1454).
                    **({"text": node.shown} if node.shown else {}),
                }
                for node in nodes
            ],
            "edges": [
                {"source": e.source, "target": e.target, "folded": list(e.folded)}
                for e in edges
            ],
        }

    def to_text(self) -> str:
        """The nodes as plain text, one per line, each with its inputs.

        In development: this interface may change.

        Returns:
            A title line, then ``  <name> (<kind> · <detail> · <status>) <- <inputs>``
            per node, in topological order with ties broken by label; the name is the
            bound source's assigned label for a source that has one, else the node's
            label. With a
            highlight, a line with its note follows the title, no status is shown, each
            highlighted node's line starts with ``*`` and the node the question was
            asked at with ``>`` (and ends with ``[traced from here]`` or
            ``[asked here]``); a node's mark follows in brackets.
        """
        inputs = self._inputs()
        lines = [f"{self.title}: {len(self._nodes)} nodes"]
        if self._highlight is not None and self._highlight.note:
            lines.append(f"  highlight (*): {self._highlight.note}")
        for node in self._nodes:
            if self._highlight is None:
                mark = "  "
            else:
                mark = (">" if node.asked else " ") + (
                    "* " if node.highlighted else "  "
                )
            line = f"{mark}{node.text} ({node.kind_line})"
            if node.mark:
                line += f" [{node.mark}]"
            if node.asked:
                line += f" [{node.asked}]"
            if node.index in inputs:
                line += " <- " + ", ".join(inputs[node.index])
            lines.append(line)
        # A lone surrogate cannot be printed or saved as UTF-8: shown as U+FFFD.
        return _SURROGATE.sub("\ufffd", "\n".join(lines))

    def to_svg(self) -> str:
        """The picture alone, as an SVG element.

        In development: this interface may change.

        Returns:
            The ``<svg>`` element; it uses no style sheet and no script.
        """
        return self._svg(
            self._nodes, self._edges, direction=self._direction, standalone=False
        )

    def _svg(
        self,
        nodes: list[_Node],
        edges: list[_Edge],
        *,
        direction: Direction,
        standalone: bool,
    ) -> str:
        layout = _layout(nodes, edges, direction)
        inputs = _inputs(nodes, edges)
        return _svg(
            self._picture_title(),
            nodes,
            inputs,
            layout,
            direction == "LR",
            standalone,
            self._caption(),
        )

    def _caption(self) -> str:
        """The line drawn above the picture: what the highlight shows, or ``""``."""
        if self._highlight is not None and self._highlight.note:
            return f"highlighted: {self._highlight.note}"
        return ""

    def _picture_title(self) -> str:
        """The picture's title: the diagram's, with the highlight's note."""
        if self._highlight is not None and self._highlight.note:
            return f"{self.title}: {self._highlight.note}"
        return self.title

    def _views(self) -> dict[str, Any] | None:
        """The other layouts, for the view's direction and collapse controls.

        The shown layout is the one ``direction=`` and ``collapse=`` ask for; this adds
        the other direction and, when folding changes something and the other model has
        at most ``max_nodes`` nodes, both directions of the other model: up to three
        pictures (the standalone SVG, base64-encoded, as the image's) and the other
        model. ``None`` above ``_MAX_EMBEDDED_NODES`` nodes, which keeps a large graph's
        output at one layout: the controls then do not appear (ITL-1232).
        """
        if len(self._nodes) > _MAX_EMBEDDED_NODES:
            return None
        other_direction: Direction = "TB" if self._direction == "LR" else "LR"
        layouts: list[tuple[Direction, bool, list[_Node], list[_Edge]]] = [
            (other_direction, self._collapse, self._nodes, self._edges)
        ]
        models: list[dict[str, Any]] = []
        if self._other is not None and len(self._other[0]) <= min(
            self._max_nodes, _MAX_EMBEDDED_NODES
        ):
            nodes, edges = self._other
            models.append(self._model(nodes, edges))
            layouts.append((self._direction, not self._collapse, nodes, edges))
            layouts.append((other_direction, not self._collapse, nodes, edges))
        views = [
            {
                "direction": direction,
                "collapse": collapse,
                "picture": base64.b64encode(
                    self._svg(
                        nodes, edges, direction=direction, standalone=True
                    ).encode("utf-8")
                ).decode("ascii"),
            }
            for direction, collapse, nodes, edges in layouts
        ]
        return {
            "direction": self._direction,
            "collapse": self._collapse,
            "models": models,
            "views": views,
        }

    def _picture(self) -> str:
        """The picture as an image element, which every notebook shows.

        A notebook that does not trust an output (JupyterLab and the classic Notebook,
        for a notebook not run by its reader) removes inline SVG and style sheets from
        its HTML but keeps an image. Where scripts run, the diagram's script replaces
        the image by the same SVG drawn inline, in the page's colours. Without
        scripts the image is as wide as its container, up to ``_MAX_SCALE`` times its
        own width; its ``width`` and ``height`` attributes keep its proportions where a
        notebook removes the style.
        """
        layout = _layout(self._nodes, self._edges, self._direction)
        svg = _svg(
            self._picture_title(),
            self._nodes,
            self._inputs(),
            layout,
            self._direction == "LR",
            standalone=True,
            caption=self._caption(),
        )
        encoded = base64.b64encode(svg.encode("utf-8")).decode("ascii")
        alt = f"Diagram of {self.title}: {len(self._nodes)} nodes"
        return (
            f'<img class="op-dag-picture" alt="{_escape(alt)}" '
            f'width="{_num(layout.width)}" '
            f'height="{_num(layout.height + (_CAPTION_H if self._caption() else 0))}" '
            f'style="width:100%;max-width:{_num(layout.width * _MAX_SCALE)}px;'
            'height:auto" '
            f'src="data:image/svg+xml;base64,{encoded}">'
        )

    def to_html(self) -> str:
        """The HTML fragment a notebook shows: picture, node table, model and script.

        In development: this interface may change.

        Returns:
            The fragment. Above ``max_nodes`` nodes, the plain-text listing in a
            ``<pre>`` element instead.
        """
        if len(self._nodes) > self._max_nodes:
            note = (
                f"{len(self._nodes)} nodes is more than max_nodes={self._max_nodes}: "
                "the picture is not drawn. Pass a larger max_nodes to diagram() to "
                "draw it, or collapse=True to fold column steps."
            )
            return (
                f"<div>{_PREVIEW_TAG}</div>"
                f"<pre>{_escape(self.to_text())}\n\n{_escape(note)}</pre>"
            )
        views = self._views()
        views_block = (
            ""
            if views is None
            else '<script type="application/json" class="op-dag-views">'
            f"{_json_for_script(views)}</script>"
        )
        return (
            f'<div class="op-dag"><style>{_asset("diagram.css")}</style>'
            f'<div class="op-dag-head">{_escape(self.title)} · '
            f'<span class="op-dag-count">{len(self._nodes)}</span> nodes '
            f"{_PREVIEW_TAG}</div>"
            f'<div class="op-dag-view">{self._picture()}</div>'
            '<div class="op-dag-panel" aria-live="polite"></div>'
            f"{_table(self._nodes, self._inputs())}"
            '<script type="application/json" class="op-dag-model">'
            f"{_json_for_script(self.to_dict())}</script>"
            f"{views_block}</div><script>{_asset('diagram.js')}</script>"
        )

    def save(self, path: str | Path) -> None:
        """Write the diagram as a standalone HTML page, viewable in any browser offline.

        In development: this interface may change.

        Args:
            path: The file to write.
        """
        page = (
            '<!doctype html><html lang="en"><head><meta charset="utf-8">'
            f"<title>{_escape(self.title)}</title></head>"
            '<body style="font-family:system-ui,sans-serif">'
            f"{self.to_html()}</body></html>"
        )
        Path(path).write_text(page, encoding="utf-8")

    def _repr_mimebundle_(
        self,
        include: Iterable[str] | None = None,
        exclude: Iterable[str] | None = None,
        **kwargs: Any,
    ) -> dict[str, str]:
        """The notebook display: the HTML diagram, and the text listing as fallback."""
        return {"text/html": self.to_html(), "text/plain": self.to_text()}

    @override
    def __str__(self) -> str:
        return self.to_text()

    @override
    def __repr__(self) -> str:
        return self.to_text()
