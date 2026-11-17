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
import heapq
import html
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol

from typing_extensions import override

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


def _job_status(
    node: Any, summary: NodeRunSummary | None, upstream_statuses: Iterable[str]
) -> str:
    """A job node's run state, from what it holds in memory; never reads or computes.

    Sources are ``bound`` or ``unbound``. Any other node is ``not run`` (no run in this
    process, or cleared), ``stale`` (an input changed since its run), ``failed`` (its
    last run, by the job or its own ``run()``, had failed rows), ``held`` (current, and
    the job's last run kept it without running it), ``current``, ``loaded only``
    (filled by a load-only run) or ``partial`` (it ran, without the failed rows of a
    node above it, so the next run computes it again).

    Args:
        node: The job node.
        summary: What the job's last run did with the node, if it ran in it.
        upstream_statuses: The statuses of the nodes it reads from.
    """
    if node.node_type == "source":
        return "bound" if getattr(node, "bound_source", None) is not None else "unbound"
    if node.last_modified is None:
        return "not run"
    if node.is_stale:
        return "stale"
    # The node's own count, not the job's summary: a node's own ``run()`` changes it
    # and leaves ``job.last_run`` as it was.
    if getattr(node, "last_run_failed", 0):
        return "failed"
    if node.holds_current_run():
        return "held" if summary is not None and summary.held else "current"
    if node.holds_current_run(load_only=True):
        return "loaded only"
    if any(status in ("failed", "partial") for status in upstream_statuses):
        return "partial"
    return "not run"


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
    """Whether the graph is a job's: its nodes are job nodes, which hold run state."""
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.nodes.staleness import RunHoldingNode

    return any(
        isinstance(node, (RunHoldingNode, SourceJobNode))
        for node in pipeline.dag.nodes()
    )


def _build_model(
    pipeline: _Drawable, *, is_job: bool, collapse: bool
) -> tuple[list[_Node], list[_Edge]]:
    """Read a compiled pipeline or job into the model's nodes and edges.

    Args:
        pipeline: A compiled ``Pipeline`` or ``PipelineJob``.
        is_job: Whether it is a job: its nodes then show their run status, and the
            job's ``last_run`` gives row counts.
        collapse: Whether to fold the column-choosing, renaming and join operators that
            feed exactly one node into the edges they lie on.

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
                by_label[label],
                summaries.get(label),
                (statuses[upstream] for upstream in preds[label]),
            )

    nodes: list[_Node] = []
    for label in shown:
        node = by_label[label]
        tag_schema, data_schema = node.output_schema()
        summary = summaries.get(label)
        facts = _run_facts(summary) if is_job else ()
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
                status=statuses.get(label, ""),
                facts=facts,
            )
        )
    edge_list = [
        _Edge(source=source, target=target, folded=via)
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
_NODE_H = 44.0
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


def _node_width(node: _Node) -> float:
    """A node's width: room for its label and kind line, within limits."""
    text = max(len(node.label) * _CHAR_W, len(node.kind_line) * _SMALL_CHAR_W)
    return max(_MIN_W, min(_MAX_W, text + 2 * _PAD))


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
        width = _node_width(node)
        boxes[node.index] = _Box(node.index, layer[node.index], width, _NODE_H)
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
_KIND_COLOUR: dict[str, str] = {
    "source": "#2e9e5b",
    "operator": "#d08a16",
    "function": "#4c6ef5",
    "side_effect_function": "#9b4cf5",
    "side_effect": "#c2417a",
}
_STATUS_COLOUR: dict[str, str] = {
    "current": "#2e9e5b",
    "held": "#2e9e5b",
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


def _escape(text: str) -> str:
    """Text escaped for HTML and SVG content and attribute values."""
    return html.escape(text, quote=True)


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
    lines = [f"{node.label} ({node.kind_line})"]
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

    Returns:
        The ``<svg>`` element.
    """
    width, height = _num(layout.width), _num(layout.height)
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
    parts.append(
        '<g class="op-dag-edges" fill="none" stroke="currentColor" '
        'stroke-opacity="0.5" stroke-width="1.4">'
    )
    for edge, points in layout.edges:
        tip = f"{nodes[edge.source].label} → {nodes[edge.target].label}"
        if edge.folded:
            tip += f" (through {', '.join(edge.folded)})"
        dash = ' stroke-dasharray="5 3"' if edge.folded else ""
        parts.append(
            f'<g data-source="{edge.source}" data-target="{edge.target}">'
            f"<title>{_escape(tip)}</title>"
            f'<path d="{_edge_path(points, across)}"{dash}/>'
            f'<path d="{_arrow_head(points[-1], across)}" fill="currentColor" '
            'stroke="none"/></g>'
        )
    parts.append('</g><g class="op-dag-nodes">')
    for node in nodes:
        x, y, w, h = layout.boxes[node.index]
        colour = _KIND_COLOUR.get(node.kind, "#888888")
        radius = (
            h / 2 if node.kind == "source" else 3.0 if node.kind == "operator" else 8.0
        )
        stroke_width = "1.4" if node.kind == "operator" else "2.2"
        dash = ' stroke-dasharray="4 3"' if node.status in _DASHED_STATUSES else ""
        label = f"{node.label}, {node.kind_line}"
        parts.append(
            f'<g class="op-dag-node" data-index="{node.index}" tabindex="0" '
            f'role="button" aria-label="{_escape(label)}" '
            f'transform="translate({_num(x)},{_num(y)})">'
            f"<title>{_escape(_tooltip(node, inputs.get(node.index, ())))}</title>"
            f'<rect width="{_num(w)}" height="{_num(h)}" rx="{_num(radius)}" '
            f'fill="{colour}" fill-opacity="0.14" stroke="{colour}" '
            f'stroke-width="{stroke_width}"{dash}/>'
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
        kind_line = _clip(node.kind_line, w, _SMALL_CHAR_W)
        parts.append(
            f'<text x="{_num(w / 2)}" y="16" text-anchor="middle" '
            f'font-size="{_SMALL_PX}" fill="currentColor" fill-opacity="0.75">'
            f"{_escape(kind_line)}</text>"
            f'<text x="{_num(w / 2)}" y="34" text-anchor="middle" '
            f'font-size="{_FONT_PX}" font-weight="600" fill="currentColor">'
            f"{_escape(_clip(node.label, w, _CHAR_W))}</text></g>"
        )
    parts.append("</g></svg>")
    return "".join(parts)


def _table(nodes: Sequence[_Node], inputs: Mapping[int, Sequence[str]]) -> str:
    """Every node with its inputs, schema and run facts: shown where scripts do not run."""
    rows = "".join(
        "<tr>"
        f"<td><b>{_escape(node.label)}</b></td>"
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


_STYLE = """
.op-dag{position:relative;border:1px solid rgba(127,127,127,.35);border-radius:6px;
padding:6px 8px;max-width:100%;box-sizing:border-box;overflow:hidden}
.op-dag .op-dag-head{font:600 13px system-ui,sans-serif;margin:0 0 4px;padding-right:96px}
.op-dag .op-dag-view{overflow:auto;max-height:560px}
.op-dag.op-dag-live .op-dag-view{overflow:hidden;cursor:grab;height:var(--op-dag-h,auto);
touch-action:none}
.op-dag .op-dag-node{cursor:pointer}
.op-dag .op-dag-node:focus{outline:none}
.op-dag .op-dag-node:focus-visible rect:first-of-type,
.op-dag .op-dag-node.op-dag-selected rect:first-of-type{stroke-width:3.4}
.op-dag .op-dag-edges g.op-dag-lit path:first-of-type{stroke-opacity:1;stroke-width:2.4}
.op-dag .op-dag-tools{position:absolute;top:5px;right:8px;display:flex;gap:4px}
.op-dag .op-dag-tools button{font:12px system-ui,sans-serif;padding:1px 8px;border-radius:4px;
border:1px solid rgba(127,127,127,.5);background:transparent;color:inherit;cursor:pointer}
.op-dag .op-dag-panel{display:none;font:12px system-ui,sans-serif;margin-top:6px;
padding-top:6px;border-top:1px solid rgba(127,127,127,.35)}
.op-dag .op-dag-panel.op-dag-open{display:block}
.op-dag .op-dag-panel dl{display:grid;grid-template-columns:max-content 1fr;gap:2px 12px;
margin:4px 0 0}
.op-dag .op-dag-panel dt{opacity:.7}
.op-dag .op-dag-panel dd{margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;
overflow-wrap:anywhere}
.op-dag .op-dag-table{font:12px system-ui,sans-serif;margin-top:6px}
.op-dag.op-dag-live .op-dag-table{display:none}
.op-dag .op-dag-table td,.op-dag .op-dag-table th{text-align:left;vertical-align:top;
padding:1px 8px}
"""

# An enhancement only: the SVG is complete without it. It finds its own container (the
# element just before it; or, where scripts run without ``document.currentScript``, as
# in the classic Notebook, the last container not set up yet), reads the model from the
# JSON block and adds pan (drag), zoom (buttons, Ctrl or Cmd with the wheel), fit, and a
# details panel built with ``textContent`` only.
_SCRIPT = r"""
(function(){
var s=document.currentScript,root=s&&s.previousElementSibling;
if(!root||!root.classList||!root.classList.contains('op-dag')){
var all=document.querySelectorAll('.op-dag:not([data-op-dag-ready])');root=all[all.length-1];}
if(!root||root.hasAttribute('data-op-dag-ready'))return;
root.setAttribute('data-op-dag-ready','');
var model=JSON.parse(root.querySelector('script.op-dag-model').textContent);
var img=root.querySelector('img.op-dag-picture');if(!img)return;
var bytes=Uint8Array.from(atob(img.getAttribute('src').split(',')[1]),
function(c){return c.charCodeAt(0);});
var svg=document.importNode(new DOMParser().parseFromString(
new TextDecoder().decode(bytes),'image/svg+xml').documentElement,true);
var bg=svg.querySelector('.op-dag-bg');if(bg)bg.parentNode.removeChild(bg);
svg.removeAttribute('color');img.parentNode.replaceChild(svg,img);
var tools=document.createElement('div');tools.className='op-dag-tools';
[['in','+','Zoom in'],['out','\u2212','Zoom out'],['fit','fit','Fit to view']].forEach(
function(b){var e=document.createElement('button');e.type='button';
e.setAttribute('data-op-dag-act',b[0]);e.textContent=b[1];e.title=b[2];
e.setAttribute('aria-label',b[2]);tools.appendChild(e);});
root.appendChild(tools);
var view=root.querySelector('.op-dag-view'),
panel=root.querySelector('.op-dag-panel');
var W=+svg.getAttribute('width'),H=+svg.getAttribute('height');
var cw=view.clientWidth||W,k0=Math.max(W/cw,1);
root.classList.add('op-dag-live');
root.style.setProperty('--op-dag-h',Math.round(Math.min(Math.max(H/k0,120),560))+'px');
svg.removeAttribute('width');svg.removeAttribute('height');
svg.style.width='100%';svg.style.height='100%';svg.style.maxWidth='none';
var vb={x:0,y:0,w:W,h:H};
function apply(){svg.setAttribute('viewBox',[vb.x,vb.y,vb.w,vb.h].join(' '));}
function fit(){var r=view.getBoundingClientRect(),rw=r.width||W,rh=r.height||H;
var k=Math.max(W/rw,H/rh,1);vb.w=rw*k;vb.h=rh*k;vb.x=(W-vb.w)/2;vb.y=(H-vb.h)/2;apply();}
function zoom(f,cx,cy){var r=view.getBoundingClientRect();
var px=cx===undefined?.5:(cx-r.left)/r.width,py=cy===undefined?.5:(cy-r.top)/r.height;
var nw=vb.w*f,nh=vb.h*f;vb.x+=(vb.w-nw)*px;vb.y+=(vb.h-nh)*py;vb.w=nw;vb.h=nh;apply();}
tools.addEventListener('click',function(ev){
var a=ev.target.getAttribute('data-op-dag-act');
if(a==='in')zoom(1/1.25);else if(a==='out')zoom(1.25);else if(a==='fit')fit();});
view.addEventListener('wheel',function(ev){if(!(ev.ctrlKey||ev.metaKey))return;
ev.preventDefault();zoom(ev.deltaY>0?1.1:1/1.1,ev.clientX,ev.clientY);},{passive:false});
var drag=null;
view.addEventListener('pointerdown',function(ev){
if(ev.target.closest&&ev.target.closest('.op-dag-node'))return;
drag={x:ev.clientX,y:ev.clientY,vx:vb.x,vy:vb.y};
view.setPointerCapture(ev.pointerId);view.style.cursor='grabbing';});
view.addEventListener('pointermove',function(ev){if(!drag)return;
var k=vb.w/(view.getBoundingClientRect().width||W);
vb.x=drag.vx-(ev.clientX-drag.x)*k;vb.y=drag.vy-(ev.clientY-drag.y)*k;apply();});
function stop(){drag=null;view.style.cursor='';}
view.addEventListener('pointerup',stop);view.addEventListener('pointercancel',stop);
function schema(cols){return cols.map(function(c){return c[0]+': '+c[1];}).join(', ')||'none';}
function row(dl,name,text){var dt=document.createElement('dt'),dd=document.createElement('dd');
dt.textContent=name;dd.textContent=text;dl.appendChild(dt);dl.appendChild(dd);}
function select(i){var n=model.nodes[i],inputs=[];
model.edges.forEach(function(e){if(e.target===i)inputs.push(model.nodes[e.source].label);});
root.querySelectorAll('.op-dag-node').forEach(function(g){
g.classList.toggle('op-dag-selected',+g.getAttribute('data-index')===i);});
root.querySelectorAll('.op-dag-edges > g').forEach(function(g){
g.classList.toggle('op-dag-lit',+g.getAttribute('data-source')===i||
+g.getAttribute('data-target')===i);});
panel.textContent='';var head=document.createElement('div');head.style.fontWeight='600';
head.textContent=n.label+' — '+[n.kind_text,n.detail,n.status].filter(Boolean).join(' · ');
panel.appendChild(head);var dl=document.createElement('dl');
row(dl,'tags',schema(n.tag_schema));row(dl,'data',schema(n.data_schema));
row(dl,'inputs',inputs.join(', ')||'none');
n.facts.forEach(function(f){row(dl,f[0],f[1]);});
panel.appendChild(dl);panel.classList.add('op-dag-open');}
root.querySelectorAll('.op-dag-node').forEach(function(g){var i=+g.getAttribute('data-index');
g.addEventListener('click',function(){select(i);});
g.addEventListener('keydown',function(ev){
if(ev.key==='Enter'||ev.key===' '){ev.preventDefault();select(i);}});});
fit();
})();
"""


def _json_for_script(value: object) -> str:
    """JSON that is safe inside a ``<script>`` element: no ``<``, ``>`` or ``&`` in it."""
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return text.replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")


class PipelineDiagram:
    """The DAG of a pipeline or job, drawn for a notebook.

    Shown at the end of a notebook cell it is an SVG picture of the nodes (sources,
    operators, function pods, side-effect pods) and the streams between them. Where the
    notebook runs scripts, the picture can be dragged and zoomed, and selecting a node
    shows its tag and data schema and, for a job, its run status and the last run's row
    counts. Without scripts the picture and a table of the nodes remain.
    ``print`` gives a plain-text listing.

    Making a diagram computes nothing: it reads each node's predicted ``output_schema()``,
    and for a job what each node holds in memory and ``job.last_run``.

    Use ``Pipeline.diagram()`` or ``PipelineJob.diagram()`` to make one.

    Args:
        pipeline: A compiled ``Pipeline`` or ``PipelineJob``.
        direction: ``"LR"`` lays the graph out left to right, ``"TB"`` top to bottom.
        collapse: Fold the operators that only choose, rename or join columns (joins,
            column selections, drops and maps) and feed exactly one node into the edges
            they lie on: such an edge is dashed, and the folded steps are listed in the
            details of the node it leads to.
        max_nodes: Above this many nodes (after folding), the notebook shows the
            plain-text listing instead of the picture.

    Raises:
        ValueError: If ``direction`` is not ``"LR"`` or ``"TB"``, or ``max_nodes`` is not
            positive.
        RuntimeError: If the pipeline has not been compiled.
    """

    def __init__(
        self,
        pipeline: _Drawable,
        *,
        direction: Direction = "LR",
        collapse: bool = False,
        max_nodes: int = 400,
    ) -> None:
        if direction not in ("LR", "TB"):
            raise ValueError(f"direction must be 'LR' or 'TB', not {direction!r}")
        if max_nodes < 1:
            raise ValueError(f"max_nodes must be at least 1, not {max_nodes!r}")
        is_job = _is_job(pipeline)
        self._kind: str = "job" if is_job else "pipeline"
        self._name: str = ".".join(pipeline.name)
        self._direction: Direction = direction
        self._max_nodes: int = max_nodes
        nodes, edges = _build_model(pipeline, is_job=is_job, collapse=collapse)
        self._nodes: list[_Node] = nodes
        self._edges: list[_Edge] = edges

    @property
    def title(self) -> str:
        """``Pipeline '<name>'`` or ``Job '<name>'``."""
        return f"{self._kind.capitalize()} {self._name!r}"

    def _inputs(self) -> dict[int, list[str]]:
        """The labels of each node's inputs, in edge order."""
        inputs: dict[int, list[str]] = {}
        for edge in self._edges:
            inputs.setdefault(edge.target, []).append(self._nodes[edge.source].label)
        return inputs

    def to_dict(self) -> dict[str, Any]:
        """The diagram's model: plain data that any renderer can draw.

        Returns:
            ``{"kind", "name", "title", "nodes", "edges"}``. Each node has ``index``,
            ``label``, ``kind`` (its ``node_type``), ``kind_text``, ``detail``,
            ``status`` (``""`` for a pipeline), ``tag_schema`` and ``data_schema``
            (lists of ``[column, type text]``) and ``facts`` (lists of ``[name, text]``);
            each edge has ``source`` and ``target`` (node indices) and ``folded``
            (the labels of the steps folded into it).
        """
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
                }
                for node in self._nodes
            ],
            "edges": [
                {"source": e.source, "target": e.target, "folded": list(e.folded)}
                for e in self._edges
            ],
        }

    def to_text(self) -> str:
        """The nodes as plain text, one per line, each with its inputs.

        Returns:
            A title line, then ``  <label> (<kind> · <detail> · <status>) <- <inputs>``
            per node, in topological order with ties broken by label.
        """
        inputs = self._inputs()
        lines = [f"{self.title}: {len(self._nodes)} nodes"]
        for node in self._nodes:
            line = f"  {node.label} ({node.kind_line})"
            if node.index in inputs:
                line += " <- " + ", ".join(inputs[node.index])
            lines.append(line)
        return "\n".join(lines)

    def to_svg(self) -> str:
        """The picture alone, as an SVG element.

        Returns:
            The ``<svg>`` element; it uses no style sheet and no script.
        """
        layout = _layout(self._nodes, self._edges, self._direction)
        return self._svg(layout, standalone=False)

    def _svg(self, layout: _Layout, *, standalone: bool) -> str:
        across = self._direction == "LR"
        return _svg(self.title, self._nodes, self._inputs(), layout, across, standalone)

    def _picture(self) -> str:
        """The picture as an image element, which every notebook shows.

        A notebook that does not trust an output (JupyterLab and the classic Notebook,
        for a notebook not run by its reader) removes inline SVG and style sheets from
        its HTML but keeps an image. Where scripts run, the diagram's script replaces
        the image by the same SVG drawn inline, in the page's colours.
        """
        layout = _layout(self._nodes, self._edges, self._direction)
        svg = self._svg(layout, standalone=True)
        encoded = base64.b64encode(svg.encode("utf-8")).decode("ascii")
        alt = f"Diagram of {self.title}: {len(self._nodes)} nodes"
        return (
            f'<img class="op-dag-picture" alt="{_escape(alt)}" '
            f'width="{_num(layout.width)}" height="{_num(layout.height)}" '
            'style="max-width:100%;height:auto" '
            f'src="data:image/svg+xml;base64,{encoded}">'
        )

    def to_html(self) -> str:
        """The HTML fragment a notebook shows: picture, node table, model and script.

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
            return f"<pre>{_escape(self.to_text())}\n\n{_escape(note)}</pre>"
        return (
            f'<div class="op-dag"><style>{_STYLE}</style>'
            f'<div class="op-dag-head">{_escape(self.title)} · '
            f"{len(self._nodes)} nodes</div>"
            f'<div class="op-dag-view">{self._picture()}</div>'
            '<div class="op-dag-panel" aria-live="polite"></div>'
            f"{_table(self._nodes, self._inputs())}"
            '<script type="application/json" class="op-dag-model">'
            f"{_json_for_script(self.to_dict())}</script>"
            f"</div><script>{_SCRIPT}</script>"
        )

    def save(self, path: str | Path) -> None:
        """Write the diagram as a standalone HTML page, viewable in any browser offline.

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
