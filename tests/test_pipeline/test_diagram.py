"""Tests for the notebook diagram of a pipeline or job (ITL-1197).

The diagram is checked as the document it generates: its model (``to_dict``), its text
listing, its HTML parsed with ``html.parser``, and its layout. No browser is needed.
"""

from __future__ import annotations

import json
import time
from html.parser import HTMLParser
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline, PipelineDiagram
from orcapod.pipeline.diagram import _Edge, _layout, _Node
from orcapod.side_effects import SideEffectPod, side_effect_pod
from orcapod.sources import DictSource

CALLS: list[str] = []
FAIL_ON: set[int] = set()


@function_pod(output_keys="y")
def inc(x: int) -> int:
    """Return ``x + 1``; raise for a value in ``FAIL_ON``."""
    CALLS.append("inc")
    if x in FAIL_ON:
        raise ValueError(f"x is {x}")
    return x + 1


@function_pod(output_keys="total")
def total(y: list[int]) -> int:
    """Return the sum of a group's values."""
    CALLS.append("total")
    return sum(y)


def _deliver(y: int, ctx: Any) -> None:
    """A side effect that does nothing."""


def _units(rows: int = 4, source_id: str = "units") -> DictSource:
    return DictSource(
        [{"g": i % 2, "k": i, "x": 10 * i} for i in range(rows)],
        tag_columns=["g", "k"],
        tag_only_columns=True,
        source_id=source_id,
    )


def _scales(source_id: str = "scales") -> DictSource:
    return DictSource(
        [{"g": 0, "scale": 1.0}, {"g": 1, "scale": 2.0}],
        tag_columns=["g"],
        tag_only_columns=True,
        source_id=source_id,
    )


def _job(database: Any = None, units: DictSource | None = None) -> PipelineJob:
    """``units`` -> select -> ``rates`` (inc) -> group_by -> ``sums`` (total)."""
    job = PipelineJob(name="demo", database=database or InMemoryArrowDatabase())
    with job:
        rates: Any = inc.pod(units or _units(), label="rates")
        total.pod(rates.group_by(["g"]), label="sums")
    return job


def _statuses(diagram: PipelineDiagram) -> dict[str, str]:
    return {node["label"]: node["status"] for node in diagram.to_dict()["nodes"]}


def _facts(diagram: PipelineDiagram, label: str) -> dict[str, str]:
    for node in diagram.to_dict()["nodes"]:
        if node["label"] == label:
            return dict(node["facts"])
    raise KeyError(label)


# ---------------------------------------------------------------------------
# The model
# ---------------------------------------------------------------------------


def test_the_model_lists_every_node_with_its_kind_schema_and_inputs() -> None:
    job = _job()
    model = job.diagram().to_dict()
    nodes = {node["label"]: node for node in model["nodes"]}
    assert sorted(nodes) == sorted(job.nodes)
    assert nodes["units"]["kind"] == "source"
    assert nodes["units"]["detail"] == "DictSource"
    assert nodes["rates"]["kind"] == "function"
    assert nodes["rates"]["kind_text"] == "function pod"
    assert nodes["rates"]["detail"] == "inc"
    assert nodes["GroupBy"]["kind"] == "operator"
    assert nodes["rates"]["tag_schema"] == [["g", "int"], ["k", "int"]]
    assert nodes["rates"]["data_schema"] == [["y", "int"]]
    assert nodes["GroupBy"]["data_schema"] == [["y", "list[int]"]]
    labels = [node["label"] for node in model["nodes"]]
    edges = {(labels[e["source"]], labels[e["target"]]) for e in model["edges"]}
    expected = {(str(up.label), str(down.label)) for up, down in job.dag.edges()}
    assert edges == expected
    assert model["title"] == "Job 'demo'"


def test_a_pipeline_blueprint_has_no_status() -> None:
    pipeline = _job().pipeline
    assert isinstance(pipeline, Pipeline)
    model = pipeline.diagram().to_dict()
    assert model["kind"] == "pipeline"
    assert {node["status"] for node in model["nodes"]} == {""}
    assert all(node["facts"] == [] for node in model["nodes"])


def test_side_effect_nodes_are_their_own_kind() -> None:
    tap = side_effect_pod(_deliver)
    assert isinstance(tap, SideEffectPod)
    job = PipelineJob(name="taps", database=InMemoryArrowDatabase())
    with job:
        rates: Any = inc.pod(_units(), label="rates")
        tap.process(rates, label="tap")
    nodes = {node["label"]: node for node in job.diagram().to_dict()["nodes"]}
    assert nodes["tap"]["kind"] == "side_effect"
    assert nodes["tap"]["kind_text"] == "side-effect pod"


# ---------------------------------------------------------------------------
# Job status, read from memory
# ---------------------------------------------------------------------------


def test_job_status_follows_the_run() -> None:
    database = InMemoryArrowDatabase()
    job = _job(database)
    assert _statuses(job.diagram()) == {
        "units": "bound",
        "rates": "not run",
        "GroupBy": "not run",
        "sums": "not run",
    }
    job.run()
    assert set(_statuses(job.diagram()).values()) == {"bound", "current"}
    assert _facts(job.diagram(), "rates") == {"last run": "4 rows, 4 computed"}

    # A second run holds every node: nothing is run again.
    job.run()
    statuses = _statuses(job.diagram())
    assert statuses["rates"] == "held"
    assert _facts(job.diagram(), "rates") == {"last run": "4 rows, 4 held"}

    # A cleared node reads as never run; its successors are stale.
    job.nodes["rates"].clear_cache()
    statuses = _statuses(job.diagram())
    assert statuses["rates"] == "not run"
    assert statuses["sums"] == "stale"


def test_a_rebound_source_makes_its_dependants_stale() -> None:
    job = _job()
    job.run()
    job.bind(sources={"units": _units(rows=6)})
    statuses = _statuses(job.diagram())
    assert statuses["rates"] == "stale"
    assert statuses["sums"] == "stale"


def test_failed_rows_show_as_failed_with_the_first_error() -> None:
    FAIL_ON.add(20)
    try:
        job = _job()
        with pytest.warns(Warning):
            job.run()
    finally:
        FAIL_ON.clear()
    diagram = job.diagram()
    assert _statuses(diagram)["rates"] == "failed"
    assert _facts(diagram, "rates") == {
        "last run": "4 rows, 3 computed, 1 failed",
        "first error": "ValueError: x is 20",
    }


def test_an_unbound_source_and_a_load_only_run() -> None:
    database = InMemoryArrowDatabase()
    job = _job(database)
    job.run()
    unbound = PipelineJob.from_pipeline(job.pipeline, database=database)
    assert _statuses(unbound.diagram())["units"] == "unbound"
    unbound.bind(sources={"units": _units()})
    unbound.run(load_only=True)
    assert _statuses(unbound.diagram())["rates"] == "loaded only"


# ---------------------------------------------------------------------------
# Making a diagram computes nothing
# ---------------------------------------------------------------------------


def _forbid(monkeypatch: pytest.MonkeyPatch, cls: type, names: list[str]) -> None:
    def boom(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("the diagram must not read or write")

    for name in names:
        monkeypatch.setattr(cls, name, boom)


@pytest.mark.parametrize("after_run", [False, True])
def test_making_a_diagram_calls_no_function_and_reads_nothing(
    monkeypatch: pytest.MonkeyPatch, after_run: bool
) -> None:
    job = _job()
    if after_run:
        job.run()
    CALLS.clear()
    database_methods = [
        name
        for name in dir(InMemoryArrowDatabase)
        if not name.startswith("_") and callable(getattr(InMemoryArrowDatabase, name))
    ]
    _forbid(monkeypatch, InMemoryArrowDatabase, database_methods)
    _forbid(
        monkeypatch,
        DictSource,
        ["as_table", "iter_data", "check_for_new_data", "head", "run"],
    )
    for target in (job, job.pipeline):
        bundle = target._repr_mimebundle_()
        assert "<svg" in bundle["text/html"]
        _ = target.diagram(collapse=True).to_html()
    assert CALLS == []


# ---------------------------------------------------------------------------
# Text, HTML, determinism, escaping
# ---------------------------------------------------------------------------


def test_the_text_listing() -> None:
    job = _job()
    assert str(job.diagram()) == "\n".join(
        [
            "Job 'demo': 4 nodes",
            "  units (source · DictSource · bound)",
            "  rates (function pod · inc · not run) <- units",
            "  GroupBy (operator · not run) <- rates",
            "  sums (function pod · total · not run) <- GroupBy",
        ]
    )


def test_the_notebook_display() -> None:
    job = _job()
    bundle = job._repr_mimebundle_()
    assert set(bundle) == {"text/html", "text/plain"}
    assert bundle["text/plain"] == job.diagram().to_text()
    assert bundle["text/html"] == job.diagram().to_html()


def test_an_uncompiled_pipeline_displays_as_text() -> None:
    pipeline = Pipeline(name="later", auto_compile=False)
    with pipeline:
        inc.pod(_units(), label="rates")
    assert pipeline._repr_mimebundle_() == {"text/plain": repr(pipeline)}
    with pytest.raises(RuntimeError, match="not been compiled"):
        pipeline.diagram()


def _record(order: list[str]) -> PipelineJob:
    job = PipelineJob(name="order", database=InMemoryArrowDatabase())
    with job:
        for name in order:
            inc.pod(_units(source_id=name), label=f"from_{name}")
    return job


def test_the_document_is_the_same_whatever_the_recording_order() -> None:
    first = _record(["a", "b", "c"]).diagram().to_html()
    second = _record(["c", "a", "b"]).diagram().to_html()
    assert first == second
    assert (
        _record(["b", "a"]).diagram().to_html()
        == _record(["a", "b"]).diagram().to_html()
    )


HOSTILE = "</script><script>alert(1)</script><img src=x onerror=alert(2)>\"'&"


class _Collector(HTMLParser):
    """Collects every element and attribute of an HTML document."""

    def __init__(self) -> None:
        super().__init__()
        self.tags: list[str] = []
        self.attributes: list[tuple[str, str | None]] = []
        self.scripts: list[str] = []
        self._in_script = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.append(tag)
        self.attributes.extend(attrs)
        self._in_script = tag == "script"
        if self._in_script:
            self.scripts.append("")

    def handle_endtag(self, tag: str) -> None:
        if tag == "script":
            self._in_script = False

    def handle_data(self, data: str) -> None:
        if self._in_script:
            self.scripts[-1] += data


ALLOWED_TAGS = {
    "div", "style", "button", "svg", "title", "g", "path", "rect", "circle", "text",
    "details", "summary", "table", "thead", "tbody", "tr", "th", "td", "b", "script",
}  # fmt: skip


def test_user_text_cannot_inject_markup_or_script() -> None:
    @function_pod(output_keys="out")
    def ident(x: int) -> int:
        return x

    source = DictSource(
        [{"k": "a", HOSTILE: 1}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="source " + HOSTILE,
    )
    # A pipeline name is a path component: no "/" in it.
    name = HOSTILE.replace("/", "")
    job = PipelineJob(name=name, database=InMemoryArrowDatabase())
    with job:
        ident.pod(source.map_data({HOSTILE: "x"}), label=HOSTILE)
    document = job.diagram().to_html()

    collector = _Collector()
    collector.feed(document)
    assert set(collector.tags) <= ALLOWED_TAGS
    assert "img" not in collector.tags
    assert not [name for name, _ in collector.attributes if name.startswith("on")]
    # Two scripts only: the model and the diagram's own; the model holds no markup.
    assert len(collector.scripts) == 2
    model_text = collector.scripts[0]
    assert "<" not in model_text and ">" not in model_text and "&" not in model_text
    model = json.loads(model_text)
    assert HOSTILE in [node["label"] for node in model["nodes"]]
    assert model["name"] == name
    assert HOSTILE not in document


# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------


def test_collapse_folds_column_steps_and_joins_into_edges() -> None:
    job = PipelineJob(name="folded", database=InMemoryArrowDatabase())
    with job:
        rates: Any = inc.pod(_units(), _scales(), label="rates")
        total.pod(rates.group_by(["g"]), label="sums")
    full = job.diagram().to_dict()
    assert {"Join", "SelectDataColumns"} <= {n["label"] for n in full["nodes"]}

    folded = job.diagram(collapse=True)
    model = folded.to_dict()
    labels = [node["label"] for node in model["nodes"]]
    assert sorted(labels) == ["GroupBy", "rates", "scales", "sums", "units"]
    edges = {
        (labels[e["source"]], labels[e["target"]]): e["folded"] for e in model["edges"]
    }
    assert edges[("units", "rates")] == ["Join", "SelectDataColumns"]
    assert edges[("scales", "rates")] == ["Join", "SelectDataColumns"]
    assert edges[("rates", "GroupBy")] == []
    assert _facts(folded, "rates")["folded steps"] == "Join, SelectDataColumns"
    assert 'stroke-dasharray="5 3"' in folded.to_svg()


def test_max_nodes_shows_the_listing_instead() -> None:
    diagram = _job().diagram(max_nodes=2)
    document = diagram.to_html()
    assert document.startswith("<pre>")
    assert "<svg" not in document
    assert "more than max_nodes=2" in document


def test_options_are_checked() -> None:
    job = _job()
    with pytest.raises(ValueError, match="direction"):
        job.diagram(direction="RL")  # pyright: ignore[reportArgumentType]  # the wrong value is the test
    with pytest.raises(ValueError, match="max_nodes"):
        job.diagram(max_nodes=0)


def test_save_writes_a_standalone_page(tmp_path: Any) -> None:
    path = tmp_path / "dag.html"
    _job().diagram().save(path)
    page = path.read_text(encoding="utf-8")
    assert page.startswith("<!doctype html>")
    assert "<svg" in page and "http" not in page.replace(
        "http://www.w3.org/2000/svg", ""
    )


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------


def _synthetic(count: int, seed: int) -> tuple[list[_Node], list[_Edge]]:
    """A random layered DAG of ``count`` nodes, the first ten of them roots."""
    import random

    rng = random.Random(seed)
    nodes = [
        _Node(i, f"node_{i}", "source" if i < 10 else "function", "", (), (), "", ())
        for i in range(count)
    ]
    edges: set[tuple[int, int]] = set()
    for i in range(10, count):
        for parent in rng.sample(range(max(0, i - 25), i), k=rng.choice([1, 1, 2, 3])):
            edges.add((parent, i))
    return nodes, [_Edge(s, t) for s, t in sorted(edges)]


@pytest.mark.parametrize("direction", ["LR", "TB"])
def test_the_layout_keeps_nodes_apart_and_edges_forward(direction: Any) -> None:
    nodes, edges = _synthetic(120, seed=3)
    layout = _layout(nodes, edges, direction)
    boxes = layout.boxes
    across = direction == "LR"
    for i, (x, y, w, h) in boxes.items():
        assert 0 <= x and x + w <= layout.width + 1e-6
        assert 0 <= y and y + h <= layout.height + 1e-6
        for j, (x2, y2, w2, h2) in boxes.items():
            if i < j:
                overlap = x < x2 + w2 and x2 < x + w and y < y2 + h2 and y2 < y + h
                assert not overlap, (i, j)
    for edge, points in layout.edges:
        start = points[0][0 if across else 1]
        end = points[-1][0 if across else 1]
        assert end > start, edge


def test_the_layout_is_deterministic_and_fast() -> None:
    nodes, edges = _synthetic(300, seed=5)
    started = time.perf_counter()
    first = _layout(nodes, edges, "LR")
    assert time.perf_counter() - started < 5
    second = _layout(nodes, edges, "LR")
    assert first == second
