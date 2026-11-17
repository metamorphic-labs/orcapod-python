"""Tests for the notebook diagram of a pipeline or job (ITL-1197).

The diagram is checked as the document it generates: its model (``to_dict``), its text
listing, its HTML parsed with ``html.parser``, and its layout. No browser is needed.
"""

from __future__ import annotations

import base64
import json
import re
import subprocess
import sys
import time
import warnings
from collections.abc import Iterator
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InDevelopmentWarning
from orcapod.pipeline import Pipeline, PipelineDiagram
from orcapod.pipeline.diagram import _Edge, _layout, _Node
from orcapod.side_effects import SideEffectPod, side_effect_pod
from orcapod.sources import DictSource
from orcapod.utils.in_development import reset_in_development_warnings

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
    assert _facts(job.diagram(), "rates") == {"job's last run": "4 rows, 4 computed"}

    # A second run holds every node: nothing is run again.
    job.run()
    statuses = _statuses(job.diagram())
    # Kept, not run: every node is current; a function node counts the held rows.
    assert set(statuses.values()) == {"bound", "current"}
    assert _facts(job.diagram(), "rates") == {"job's last run": "4 rows, 4 held"}

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
        "job's last run": "4 rows, 3 computed, 1 failed",
        "first error": "ValueError: x is 20",
    }
    # The nodes below ran on the rows that did not fail: partial, not "not run".
    assert _statuses(diagram)["GroupBy"] == "partial"
    assert _statuses(diagram)["sums"] == "partial"
    assert _facts(diagram, "sums") == {"job's last run": "2 rows, 2 computed"}
    assert "partial" in job.diagram(collapse=True).to_text()
    job.run()
    assert set(_statuses(job.diagram()).values()) == {"bound", "current"}


def test_a_node_s_own_run_sets_its_status_not_the_job_s_last_run() -> None:
    # A failed run of the node alone: the job has no summary, the node failed.
    FAIL_ON.add(20)
    try:
        job = _job()
        with pytest.warns(Warning):
            job.nodes["rates"].run()
        assert job.last_run is None
        assert _statuses(job.diagram())["rates"] == "failed"

        # A job run that failed, then a node run that succeeds: current again,
        # although the job's last run still lists the failure.
        retried = _job()
        with pytest.warns(Warning):
            retried.run()
    finally:
        FAIL_ON.clear()
    retried.nodes["rates"].run()
    diagram = retried.diagram()
    assert _statuses(diagram)["rates"] == "current"
    assert _facts(diagram, "rates")["job's last run"] == "4 rows, 3 computed, 1 failed"
    job.nodes["rates"].clear_cache()
    assert _statuses(job.diagram())["rates"] == "not run"


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
        assert "op-dag-picture" in bundle["text/html"]
        _ = target.diagram(collapse=True).to_html()
    assert CALLS == []


def test_a_tags_as_data_source_is_drawn_without_reading_its_origin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source over a job node that has stored nothing yet: its columns are predicted."""
    origin = _job()
    derived = origin.nodes["rates"].as_source(tags_as_data={"k": "k_copy"})
    job = PipelineJob(name="second", database=InMemoryArrowDatabase())
    with job:
        derived.select_data_columns(["k_copy"])
    database_methods = [
        name
        for name in dir(InMemoryArrowDatabase)
        if not name.startswith("_") and callable(getattr(InMemoryArrowDatabase, name))
    ]
    _forbid(monkeypatch, InMemoryArrowDatabase, database_methods)
    for _ in range(2):
        nodes = job.diagram().to_dict()["nodes"]
        selected = next(n for n in nodes if n["kind"] == "operator")
        assert selected["data_schema"] == [["k_copy", "int"]]
        _ = job._repr_mimebundle_()


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


def test_the_view_is_marked_as_a_preview() -> None:
    """A small "preview" tag next to the title: the diagram is in development."""
    document = _job().diagram().to_html()
    collector = _Collector()
    collector.feed(document)
    note = "In development: the diagram and its interface may change"
    assert ("title", note) in collector.attributes
    assert ("aria-label", note) in collector.attributes
    head = document.split('<div class="op-dag-head">', 1)[1].split("</div>", 1)[0]
    assert head.endswith('">preview</span>')
    # Outside the script: the tag stays where scripts are removed.
    without_scripts = document.split("<script", 1)[0]
    assert ">preview</span>" in without_scripts


# ---------------------------------------------------------------------------
# In development: warned once per process
# ---------------------------------------------------------------------------


def _in_development(record: list[warnings.WarningMessage]) -> list[str]:
    return [str(w.message) for w in record if w.category is InDevelopmentWarning]


@pytest.fixture
def fresh_warnings() -> Iterator[None]:
    """The test starts as a new process would: no feature has warned yet."""
    reset_in_development_warnings()
    yield
    reset_in_development_warnings()


@pytest.mark.usefixtures("fresh_warnings")
def test_the_first_diagram_warns_once_per_process() -> None:
    job = _job()
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        _ = job.diagram()
        _ = job.diagram(collapse=True).to_html()
        _ = job._repr_mimebundle_()
        _ = job.pipeline.diagram()
    messages = _in_development(record)
    assert len(messages) == 1
    assert messages[0].startswith("The pipeline diagram (Pipeline.diagram(), ")
    assert "may change in any release" in messages[0]
    assert "category=orcapod.errors.InDevelopmentWarning" in messages[0]
    # Attributed to the code that made the diagram.
    warned = [w for w in record if w.category is InDevelopmentWarning][0]
    assert warned.filename == __file__


@pytest.mark.usefixtures("fresh_warnings")
def test_the_notebook_display_warns_when_it_makes_the_first_diagram() -> None:
    job = _job()
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        _ = job._repr_mimebundle_()
    assert len(_in_development(record)) == 1


@pytest.mark.usefixtures("fresh_warnings")
def test_no_warning_without_a_diagram() -> None:
    job = _job()
    pipeline = Pipeline(name="uncompiled")
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        _ = repr(job)
        _ = str(job)
        _ = pipeline._repr_mimebundle_()
        job.run()
        _ = job.nodes["sums"].head()
        _ = job.nodes["sums"]._repr_html_()
    assert _in_development(record) == []


@pytest.mark.usefixtures("fresh_warnings")
def test_under_an_error_filter_every_diagram_raises() -> None:
    job = _job()
    with warnings.catch_warnings():
        warnings.simplefilter("error", InDevelopmentWarning)
        for _ in range(2):
            with pytest.raises(InDevelopmentWarning):
                _ = job.diagram()


def test_importing_orcapod_does_not_warn() -> None:
    code = (
        "import warnings\n"
        "warnings.simplefilter('error')\n"
        "import orcapod, orcapod.pipeline, orcapod.pipeline.diagram\n"
        "print('ok')\n"
    )
    done = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "ok"


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
    "div", "style", "img", "details", "summary", "table", "thead", "tbody", "tr", "th",
    "td", "b", "script", "span",
}  # fmt: skip
SVG_TAGS = {"svg", "title", "g", "path", "rect", "circle", "text"}
SVG_NS = "{http://www.w3.org/2000/svg}"


def _picture(document: str) -> str:
    """The SVG text of the diagram's image."""
    collector = _Collector()
    collector.feed(document)
    sources = [value for name, value in collector.attributes if name == "src"]
    assert len(sources) == 1 and sources[0] is not None
    prefix = "data:image/svg+xml;base64,"
    assert sources[0].startswith(prefix)
    return base64.b64decode(sources[0][len(prefix) :]).decode("utf-8")


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
    assert collector.tags.count("img") == 1  # the picture, not the label's
    assert not [name for name, _ in collector.attributes if name.startswith("on")]
    # Three scripts only: the model, the other layout (the map folds) and the
    # diagram's own; the two JSON blocks hold no markup.
    assert len(collector.scripts) == 3
    for block in collector.scripts[:2]:
        assert "<" not in block and ">" not in block and "&" not in block
    model_text, other_text = collector.scripts[:2]
    other_svg = base64.b64decode(json.loads(other_text)["picture"]).decode("utf-8")
    other_root = ET.fromstring(other_svg)
    assert {e.tag.removeprefix(SVG_NS) for e in other_root.iter()} <= SVG_TAGS
    model = json.loads(model_text)
    assert HOSTILE in [node["label"] for node in model["nodes"]]
    assert model["name"] == name
    assert HOSTILE not in document

    # The picture parses as XML: every user text in it is escaped, as text only.
    for svg_text in (_picture(document), job.diagram().to_svg()):
        root = ET.fromstring(svg_text)
        elements = list(root.iter())
        assert {e.tag.removeprefix(SVG_NS) for e in elements} <= SVG_TAGS
        assert not [a for e in elements for a in e.attrib if a.startswith("on")]
        texts = [e.text for e in elements if e.tag == f"{SVG_NS}text"]
        assert any(text and text.startswith("</script>") for text in texts)


def test_control_characters_in_labels_and_errors_keep_the_svg_valid() -> None:
    @function_pod(output_keys="out")
    def coloured(x: int) -> int:
        raise ValueError("\x1b[31mfailed\x1b[0m")

    label = "bad\x00label\x1b"
    job = PipelineJob(name="controls", database=InMemoryArrowDatabase())
    with job:
        coloured.pod(_units(), label=label)
    with pytest.warns(Warning):
        job.run()
    diagram = job.diagram()
    assert _facts(diagram, label)["first error"] == "ValueError: \x1b[31mfailed\x1b[0m"
    document = diagram.to_html()
    # No raw control character anywhere in the document: the model block escapes them
    # as JSON, the picture and the node table replace them.
    assert not re.search("[\x00-\x08\x0b\x0c\x0e-\x1f]", document)
    for svg_text in (_picture(document), diagram.to_svg()):
        root = ET.fromstring(svg_text)  # parses: XML allows every character left
        texts = " ".join(e.text or "" for e in root.iter())
        assert "bad\ufffdlabel\ufffd" in texts
        assert "\ufffd[31mfailed\ufffd[0m" in texts
    model_text = document.split('class="op-dag-model">', 1)[1].split("</script>", 1)[0]
    model = json.loads(model_text)
    assert label in [node["label"] for node in model["nodes"]]


def test_a_lone_surrogate_in_a_label_can_be_saved(tmp_path: Any) -> None:
    label = "bad\ud800label"
    job = PipelineJob(name="surrogate", database=InMemoryArrowDatabase())
    with job:
        inc.pod(_units(), label=label)
    diagram = job.diagram()
    document = diagram.to_html()
    _ = document.encode("utf-8")  # no lone surrogate left: it can be written
    _ = diagram.to_text().encode("utf-8")
    assert "bad\ufffdlabel" in diagram.to_text()
    diagram.save(tmp_path / "page.html")
    model_text = document.split('class="op-dag-model">', 1)[1].split("</script>", 1)[0]
    assert label in [node["label"] for node in json.loads(model_text)["nodes"]]


def test_a_loaded_node_whose_types_cannot_be_restored_is_drawn(tmp_path: Any) -> None:
    """A class only the writer registered: the node's schema is refused, not the graph."""
    script = tmp_path / "save_unknown.py"
    script.write_text(
        "from dataclasses import dataclass\n"
        "from pathlib import Path\n"
        "import sys\n"
        "from orcapod import PipelineJob, function_pod\n"
        "from orcapod.databases import DeltaTableDatabase\n"
        "from orcapod.sources import DictSource\n"
        "@dataclass\n"
        "class WriterOnly:\n"
        "    value: int\n"
        "@function_pod(output_keys='payload')\n"
        "def make_payload(x: int) -> WriterOnly:\n"
        "    return WriterOnly(x)\n"
        "root = Path(sys.argv[1])\n"
        "job = PipelineJob(name='unknown', database=DeltaTableDatabase(root / 'store'))\n"
        "with job:\n"
        "    source = DictSource([{'id': 1, 'x': 2}], tag_columns=['id'],\n"
        "                        tag_only_columns=True, source_id='numbers')\n"
        "    make_payload.pod(source, label='producer')\n"
        "job.run()\n"
        "job.save(root / 'job.json')\n"
    )
    subprocess.run([sys.executable, str(script), str(tmp_path)], check=True)
    loaded = PipelineJob.load(tmp_path / "job.json")
    with pytest.raises(ValueError, match="producer"):
        loaded.nodes["producer"].output_schema()
    diagram = loaded.diagram()
    nodes = {node["label"]: node for node in diagram.to_dict()["nodes"]}
    assert set(nodes) == {"numbers", "producer"}
    assert nodes["producer"]["tag_schema"] == []
    assert nodes["producer"]["data_schema"] == []
    assert _facts(diagram, "producer")["schema"].startswith("not available: ")
    assert nodes["numbers"]["data_schema"] == [["x", "int"]]
    _ = diagram.to_html()


def test_a_notebook_that_strips_scripts_and_svg_still_shows_the_picture() -> None:
    """An untrusted JupyterLab output keeps an image but drops inline SVG and styles."""
    document = _job().diagram().to_html()
    assert "<svg" not in document  # inline SVG is made by the script only
    svg_text = _picture(document)
    root = ET.fromstring(svg_text)
    # On its own background, in a fixed text colour: an image takes no page colours.
    assert root.get("color") == "#1f1f1f"
    assert root.find(f"{SVG_NS}rect").get("class") == "op-dag-bg"  # pyright: ignore[reportOptionalMemberAccess]  # asserted by the test
    # The inline picture, made where scripts run, takes the page's colours instead.
    inline = ET.fromstring(_job().diagram().to_svg())
    assert inline.get("color") is None
    assert inline.find(f"{SVG_NS}rect[@class='op-dag-bg']") is None
    # The image has its alternative text; the inline picture holds focusable node
    # buttons, so it is a group of controls, not one image (ARIA).
    collector = _Collector()
    collector.feed(document)
    assert any(name == "alt" and value for name, value in collector.attributes)
    assert inline.get("role") == "group"
    buttons = [g for g in inline.iter(f"{SVG_NS}g") if g.get("role") == "button"]
    assert len(buttons) == 4


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


def test_collapse_goes_by_the_operator_class_not_its_label() -> None:
    from orcapod.core.operators import DropNulls, Join

    job = PipelineJob(name="labels", database=InMemoryArrowDatabase())
    with job:
        joined: Any = Join(label="joined")(_units(), _scales())
        kept: Any = DropNulls(label="Join")(joined)
        inc.pod(kept, label="rates")
    nodes = {n["label"]: n for n in job.diagram().to_dict()["nodes"]}
    assert nodes["joined"]["detail"] == "Join"
    assert nodes["Join"]["detail"] == "DropNulls"

    labels = [n["label"] for n in job.diagram(collapse=True).to_dict()["nodes"]]
    assert "joined" not in labels  # a Join, whatever its label
    assert "Join" in labels  # a DropNulls is never folded


def _other_view(document: str) -> dict[str, Any] | None:
    """The other layout embedded for the collapse checkbox, or ``None``."""
    marker = '<script type="application/json" class="op-dag-other">'
    if marker not in document:
        return None
    return json.loads(document.split(marker, 1)[1].split("</script>", 1)[0])


@pytest.mark.parametrize("collapse", [False, True])
def test_the_output_holds_the_other_layout_for_the_collapse_checkbox(
    collapse: bool,
) -> None:
    job = PipelineJob(name="folded", database=InMemoryArrowDatabase())
    with job:
        rates: Any = inc.pod(_units(), _scales(), label="rates")
        total.pod(rates.group_by(["g"]), label="sums")
    diagram = job.diagram(collapse=collapse)
    document = diagram.to_html()
    other = _other_view(document)
    assert other is not None
    assert other["collapse"] is not collapse
    shown, hidden = job.diagram(collapse=collapse), job.diagram(collapse=not collapse)
    assert other["model"] == hidden.to_dict()
    assert base64.b64decode(other["picture"]).decode("utf-8") == _picture(
        hidden.to_html()
    )
    # Without scripts, the picture and the node table are those of collapse=.
    assert _picture(document) == _picture(shown.to_html())
    labels = {n["label"] for n in shown.to_dict()["nodes"]}
    table = document.split('class="op-dag-table"', 1)[1].split("</table>", 1)[0]
    assert ("<b>Join</b>" in table) == ("Join" in labels) == (not collapse)
    head = document.split('<span class="op-dag-count">', 1)[1].split("<", 1)[0]
    assert head == str(len(labels))
    # The same output every time.
    assert job.diagram(collapse=collapse).to_html() == document


def test_no_collapse_checkbox_when_folding_changes_nothing() -> None:
    job = _job()  # a GroupBy only: nothing to fold
    assert len(job.diagram(collapse=True).to_dict()["nodes"]) == len(job.nodes)
    assert _other_view(job.diagram().to_html()) is None


def test_no_other_layout_beyond_max_nodes() -> None:
    job = PipelineJob(name="folded", database=InMemoryArrowDatabase())
    with job:
        inc.pod(_units(), _scales(), label="rates")
    full = len(job.diagram().to_dict()["nodes"])
    folded = len(job.diagram(collapse=True).to_dict()["nodes"])
    assert folded < full
    # Folded fits, the full layout does not: no checkbox.
    assert _other_view(job.diagram(collapse=True, max_nodes=folded).to_html()) is None
    assert _other_view(job.diagram(collapse=True, max_nodes=full).to_html()) is not None


def test_max_nodes_shows_the_listing_instead() -> None:
    diagram = _job().diagram(max_nodes=2)
    document = diagram.to_html()
    assert document.startswith('<div><span class="op-dag-sep">')
    assert "<pre>" in document
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
    assert "data:image/svg+xml;base64," in page
    assert "http" not in page  # nothing is fetched


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
