"""The diagram draws an input that only constrains the others apart (ITL-1455)."""

from __future__ import annotations

import re
import shutil
import warnings
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SemiJoinIgnoredColumnWarning
from orcapod.operators import Join, MergeJoin, SemiJoin
from orcapod.pipeline import PipelineDiagram
from orcapod.pipeline.diagram import _Edge, _layout, _Node
from orcapod.sources import DictSource
from orcapod.types.provenance import Highlight

from .test_diagram import _run_view

CALLS: list[str] = []


@function_pod(output_keys="rate")
def rate(x: int) -> float:
    """Return the value as a float."""
    CALLS.append("rate")
    return float(x)


def _source(name: str, tags: list[str], *data: str) -> DictSource:
    return DictSource(
        [{**dict.fromkeys(tags, f"t{i}"), **dict.fromkeys(data, i)} for i in range(3)],
        tag_columns=tags,
        tag_only_columns=True,
        source_id=name,
    )


def _job(*, reverse: bool = False) -> PipelineJob:
    """Nested semi joins, recorded in either order of the sources."""
    job = PipelineJob(name="nested", database=InMemoryArrowDatabase())
    made = {
        name: _source(name, tags, *data)
        for name, tags, data in (
            ("spikes", ["session", "unit"], ("x",)),
            ("units", ["unit", "session"], ("snr", "depth")),
            ("approved", ["session"], ("note",)),
        )[:: -1 if reverse else 1]
    }
    with job:
        kept = made["units"].semi_join(made["approved"], label="kept_units")
        chosen = kept.select_data_columns(["snr"])
        rate.pod(made["spikes"].semi_join(chosen, label="kept"), label="rates")
    return job


def _edges(diagram: PipelineDiagram) -> set[tuple[str, str, bool, tuple[str, ...]]]:
    model = diagram.to_dict()
    label = [node["label"] for node in model["nodes"]]
    return {
        (
            label[e["source"]],
            label[e["target"]],
            e.get("constrains", False),
            tuple(e["folded"]),
        )
        for e in model["edges"]
    }


def _facts(diagram: PipelineDiagram, label: str) -> dict[str, str]:
    (node,) = [n for n in diagram.to_dict()["nodes"] if n["label"] == label]
    return dict(node["facts"])


def test_only_semi_join_declares_a_constraining_input() -> None:
    assert SemiJoin.CONSTRAINING_INPUTS == {1}
    assert Join.CONSTRAINING_INPUTS == MergeJoin.CONSTRAINING_INPUTS == frozenset()


@pytest.mark.parametrize("blueprint", [False, True])
def test_the_right_input_constrains_in_model_text_and_details(blueprint: bool) -> None:
    job = _job()
    diagram = (job.pipeline if blueprint else job).diagram()
    assert _edges(diagram) == {
        ("units", "kept_units", False, ()),
        ("approved", "kept_units", True, ()),
        ("kept_units", "SelectDataColumns", False, ()),
        ("spikes", "kept", False, ()),
        ("SelectDataColumns", "kept", True, ()),
        ("kept", "rates", False, ()),
    }
    # The key is in the model only where it applies.
    assert {e.get("constrains", True) for e in diagram.to_dict()["edges"]} == {True}
    text = diagram.to_text()
    assert "<- units; constrained by approved" in text
    assert "<- spikes; constrained by SelectDataColumns" in text
    assert _facts(diagram, "kept_units")["keeps the rows that match on"] == "session"
    assert _facts(diagram, "kept")["keeps the rows that match on"] == "session, unit"


def test_folding_keeps_the_constraint() -> None:
    diagram = _job().diagram(collapse=True)
    assert ("kept_units", "kept", True, ("SelectDataColumns",)) in _edges(diagram)
    assert "<- spikes; constrained by kept_units" in diagram.to_text()
    assert 'stroke-dasharray="8 3 2 3"' in diagram.to_svg()


def test_a_folded_join_beneath_a_constraint_constrains_from_each_of_its_inputs() -> (
    None
):
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        limits = _source("b", ["k"], "y").join(_source("c", ["k"], "z"))
        rate.pod(_source("a", ["k"], "x").semi_join(limits, label="s"), label="r")
    edges = _edges(job.diagram(collapse=True))
    assert {e for e in edges if e[1] == "s"} == {
        ("a", "s", False, ()),
        ("b", "s", True, ("Join",)),
        ("c", "s", True, ("Join",)),
    }


class _ConstrainedJoin(Join):
    CONSTRAINING_INPUTS = frozenset({1})


def test_a_foldable_step_with_a_constraining_input_is_not_folded() -> None:
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        joined = _ConstrainedJoin(label="limited")(
            _source("a", ["k"], "x"), _source("b", ["k"], "y")
        )
        rate.pod(joined.select_data_columns(["x"]), label="r")
    edges = _edges(job.diagram(collapse=True))
    assert {e for e in edges if e[1] == "limited"} == {
        ("a", "limited", False, ()),
        ("b", "limited", True, ()),
    }


def test_one_stream_as_both_inputs_has_two_edges() -> None:
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        a = _source("a", ["k"], "x")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SemiJoinIgnoredColumnWarning)
            rate.pod(a.semi_join(a, label="s"), label="r")
    diagram = job.diagram()
    assert {e for e in _edges(diagram) if e[1] == "s"} == {
        ("a", "s", False, ()),
        ("a", "s", True, ()),
    }
    assert "s (operator · SemiJoin · not run) <- a; constrained by a" in (
        diagram.to_text()
    )


def test_no_shared_tag_column_is_stated() -> None:
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        s = _source("a", ["k"], "x").semi_join(_source("b", ["m"], "y"), label="s")
        rate.pod(s, label="r")
    facts = _facts(job.diagram(), "s")
    assert facts["keeps every row"] == "the inputs share no tag column"


def test_a_diagram_without_a_constraint_has_no_new_key_or_drawing() -> None:
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        joined = _source("a", ["k"], "x").join(_source("b", ["k"], "y"))
        rate.pod(joined, label="r")
    diagram = job.diagram()
    assert all(
        set(e) == {"source", "target", "folded"} for e in diagram.to_dict()["edges"]
    )
    assert "constrain" not in diagram.to_svg() + diagram.to_text()


def test_making_the_diagram_calls_no_function_and_moves_no_identity() -> None:
    job = _job()
    before = {label: node.content_hash() for label, node in job.nodes.items()}
    CALLS.clear()
    for collapse in (False, True):
        job.diagram(collapse=collapse).to_html()
    assert CALLS == []
    assert {label: n.content_hash() for label, n in job.nodes.items()} == before


def test_a_loaded_job_keeps_the_roles(tmp_path: Any) -> None:
    job = _job()
    job.run()
    path = tmp_path / "job.json"
    job.save(str(path))
    loaded = PipelineJob.load(str(path))
    assert _edges(loaded.diagram()) == _edges(job.diagram())


@pytest.mark.parametrize("direction", ["LR", "TB"])
def test_the_drawing_does_not_depend_on_the_recording_order(direction: Any) -> None:
    pictures = {
        _job(reverse=reverse).diagram(direction=direction).to_svg()
        for reverse in (False, True)
    }
    assert len(pictures) == 1


def test_a_highlight_lights_a_constraining_edge_by_its_own_path() -> None:
    highlight = Highlight(
        nodes=frozenset({"approved", "kept_units"}),
        edges=frozenset({("approved", "kept_units")}),
    )
    svg = _job().diagram(highlight=highlight).to_svg()
    lit = [g for g in _edge_groups(svg) if "op-dag-highlight" in g]
    assert len(lit) == 1 and "op-dag-constraint" in lit[0]


def test_a_folded_constraint_is_not_lit_by_the_data_edge_beside_it() -> None:
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        a = _source("a", ["k"], "x")
        rate.pod(a.semi_join(a.map_data({"x": "z"}), label="s"), label="r")
    highlight = Highlight(nodes=frozenset({"a", "s"}), edges=frozenset({("a", "s")}))
    svg = job.diagram(collapse=True, highlight=highlight).to_svg()
    lit = [g for g in _edge_groups(svg) if "op-dag-highlight" in g]
    assert len(lit) == 1 and "op-dag-constraint" not in lit[0]


def _edge_groups(svg: str) -> list[str]:
    return re.findall(r"<g data-source=.*?</g>", svg, re.DOTALL)


def test_the_edge_tooltip_says_what_is_kept_and_labels_are_escaped() -> None:
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        b = rate.pod(_source("b", ["k"], "x"), label="<b>" + "long" * 40)
        rate.pod(_source("a", ["k"], "x").semi_join(b, label="s"), label="r")
    root = ET.fromstring(job.diagram().to_svg())
    titles = [t.text or "" for t in root.iter("{http://www.w3.org/2000/svg}title")]
    assert any(
        t.startswith("<b>long")
        and t.endswith("only decides which rows pass\nkeeps the rows that match on: k")
        for t in titles
    )


# ---------------------------------------------------------------------------
# The layout
# ---------------------------------------------------------------------------


def _node(index: int, label: str) -> _Node:
    return _Node(index, label, "operator", "", (), (), "", ())


@pytest.mark.parametrize("direction", ["LR", "TB"])
def test_a_constraint_enters_from_the_side_and_crosses_no_box_of_its_layer(
    direction: Any,
) -> None:
    labels = ["a", "b" * 30, "c", "s" * 20, "t", "u"]
    nodes = [_node(i, label) for i, label in enumerate(labels)]
    for flags in range(2**6):
        pairs = [(0, 3), (1, 3), (1, 4), (2, 4), (2, 5), (0, 5)]
        edges = [
            _Edge(s, t, constrains=bool(flags >> n & 1))
            for n, (s, t) in enumerate(pairs)
        ]
        layout = _layout(nodes, sorted(edges, key=lambda e: e.key), direction)
        for edge, points in layout.edges:
            if not edge.constrains:
                continue
            x, y, w, h = layout.boxes[edge.target]
            (cx, cy), (ex, ey) = points[-2:]
            if direction == "TB":
                assert ey == cy == pytest.approx(y + h / 2)
                assert any(ex == pytest.approx(side) for side in (x, x + w))
                assert not x <= cx <= x + w
            else:
                assert ex == cx == pytest.approx(x + w / 2)
                assert any(ey == pytest.approx(side) for side in (y, y + h))
                assert not y <= cy <= y + h
            for px, py in points[-3:-1]:
                assert 0 <= px <= layout.width and 0 <= py <= layout.height
                for bx, by, bw, bh in layout.boxes.values():
                    assert not (bx <= px <= bx + bw and by <= py <= by + bh)


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is not on PATH")
def test_the_details_panel_lists_constraining_inputs_apart(tmp_path: Path) -> None:
    (seen,) = _run_view(
        _job().diagram().to_html(),
        [
            {"op": "run", "width": 400},
            {"op": "select", "label": "kept"},
            {"op": "observe"},
        ],
        tmp_path,
    )
    assert "inputsspikesconstrained bySelectDataColumns" in seen["panelText"]
    assert "keeps the rows that match onsession, unit" in seen["panelText"]
