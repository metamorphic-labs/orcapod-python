"""The preview tells a plain tag from a data-tag (ITL-1304).

A data-tag is one column in both roles: a tag and the data column of the same name. Every
view of a stream knows which of its tag columns are data-tags (``data_tag_columns``,
taken from the stream's public ``keys()``), however the view is built, and only the
display marks them: never ``StreamView.table``, ``as_table()``, ``as_polars_df()`` or a
schema. Building such a view computes nothing, calls no user code and writes nothing.

The streams here have two plain tags (``session``, ``trial``), two data-tags (``unit``,
``time``) and two data columns (``amp``, ``label``), in several tag orders and table
layouts.
"""

from __future__ import annotations

import csv
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from unittest import mock

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.operators import Join, SelectDataColumns
from orcapod.core.sources import CSVSource, DerivedSource, DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.side_effects import SideEffectPod
from orcapod.streams import StreamView, ViewStatus

PLAIN = ("session", "trial")
DATA_TAGS = ("unit", "time")
DATA = ("amp", "label")

ROWS = [
    {"session": "s2", "unit": 5, "trial": 2, "time": 31, "amp": 0.125, "label": "n"},
    {"session": "s1", "unit": 3, "trial": 1, "time": 10, "amp": 0.5, "label": "s"},
    {"session": "s2", "unit": 3, "trial": 2, "time": 20, "amp": 0.75, "label": "s"},
    {"session": "s1", "unit": 4, "trial": 1, "time": 12, "amp": 0.25, "label": "b"},
]

# Tag orders: plain and data-tags interleaved, data-tags first, and reversed.
TAG_ORDERS = {
    "interleaved": ("session", "unit", "trial", "time"),
    "data_tags_first": ("time", "unit", "trial", "session"),
    "plain_first": ("trial", "session", "unit", "time"),
}
# Table layouts: data before tags, and tags in another order than declared.
LAYOUTS = {
    "data_first": ("label", "amp", "time", "trial", "unit", "session"),
    "tags_first": ("unit", "session", "time", "trial", "amp", "label"),
}

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}

CALLS: list[float] = []


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    """Clear the record of function calls before each test."""
    CALLS.clear()


@function_pod(output_keys="scaled")
def scale(amp: float) -> float:
    """Double ``amp``, recording the call."""
    CALLS.append(amp)
    return 2 * amp


def _rows(layout: tuple[str, ...]) -> list[dict[str, Any]]:
    return [{name: row[name] for name in layout} for row in ROWS]


def _source(
    tag_order: tuple[str, ...] = TAG_ORDERS["interleaved"],
    layout: tuple[str, ...] = LAYOUTS["data_first"],
    plain: tuple[str, ...] = PLAIN,
) -> DictSource:
    """Four rows with two plain tags, two data-tags and two data columns."""
    return DictSource(
        _rows(layout),
        tag_columns=list(tag_order),
        tag_only_columns=list(plain),
        source_id="spikes",
        label="spikes",
    )


def _expected(tag_order: tuple[str, ...]) -> tuple[str, ...]:
    """The data-tags of ``_source(tag_order)``, in tag order."""
    return tuple(c for c in tag_order if c in DATA_TAGS)


def _data_tags_of(stream: Any) -> tuple[str, ...]:
    """The data-tags ``stream`` declares, in its own tag order; both of ``DATA_TAGS``."""
    tags, data = stream.keys()
    found = tuple(c for c in tags if c in data)
    assert sorted(found) == sorted(DATA_TAGS)
    return found


def _in_view_order(view: StreamView) -> tuple[str, ...]:
    """``DATA_TAGS`` in the order ``view`` shows its tags (which column order a view
    shows is ITL-1305); checks the view shows both."""
    found = tuple(c for c in view.tag_columns if c in DATA_TAGS)
    assert sorted(found) == sorted(DATA_TAGS)
    return found


def _view(view: object) -> StreamView:
    """``view`` as the ``StreamView`` every stream's ``head`` and ``run`` return."""
    assert isinstance(view, StreamView)
    return view


def _views(view: object) -> dict[str, StreamView]:
    """Every view a view builds of its own rows, and views of those views."""
    view = _view(view)
    return {
        "view": view,
        "head": view.head(2),
        "tail": view.tail(2),
        "sorted": view.sort_by_tags(),
        "head_of_tail": view.tail(3).head(1),
        "sorted_head": view.head(3).sort_by_tags(),
    }


# ---------------------------------------------------------------------------
# Every way a view is built knows the data-tags
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("layout", list(LAYOUTS))
@pytest.mark.parametrize("tag_order", list(TAG_ORDERS))
def test_every_view_of_a_source_knows_its_data_tags(
    tag_order: str, layout: str
) -> None:
    """head, run and the view's own head, tail and sort all carry the data-tags."""
    source = _source(TAG_ORDERS[tag_order], LAYOUTS[layout])
    expected = _expected(TAG_ORDERS[tag_order])
    for built in (
        source.head(),
        source.head(3),
        source.head(columns={"sort_by_tags": True}),
        source.run(),
    ):
        for name, view in _views(built).items():
            assert view.tag_columns == TAG_ORDERS[tag_order], name
            assert view.data_tag_columns == expected, name


def test_a_raw_stream_view_knows_its_data_tags() -> None:
    """A raw ``ArrowTableStream`` with ``data_tags`` marks exactly those."""
    table = pa.Table.from_pylist(_rows(LAYOUTS["tags_first"]))
    for data_tags in (("unit", "time"), ("time", "unit")):
        stream = ArrowTableStream(
            table, tag_columns=list(TAG_ORDERS["interleaved"]), data_tags=data_tags
        )
        assert stream.head().data_tag_columns == ("unit", "time")
        assert stream.run().tail(1).data_tag_columns == ("unit", "time")
    plain = ArrowTableStream(table, tag_columns=list(TAG_ORDERS["interleaved"]))
    assert plain.head().data_tag_columns == ()


def test_only_plain_tags_and_only_data_tags() -> None:
    """All tags plain: no data-tag; no tag plain: every tag is a data-tag."""
    all_plain = _source(plain=PLAIN + DATA_TAGS)
    assert all_plain.head().data_tag_columns == ()
    assert all_plain.run().sort_by_tags().data_tag_columns == ()
    none_plain = _source(plain=())
    assert none_plain.head().data_tag_columns == TAG_ORDERS["interleaved"]
    assert none_plain.run().tail(2).data_tag_columns == TAG_ORDERS["interleaved"]


@pytest.mark.parametrize("tag_order", list(TAG_ORDERS))
def test_operator_results_know_their_data_tags(tag_order: str) -> None:
    """An operator's result, before and after it is computed, carries its data-tags,
    for one input and for two inputs in both orders."""
    source = _source(TAG_ORDERS[tag_order])
    expected = _expected(TAG_ORDERS[tag_order])
    other = DictSource(
        [{"trial": r["trial"], "session": r["session"], "w": 1.0} for r in ROWS[:2]],
        tag_columns=["trial", "session"],
        tag_only_columns=True,
        source_id="other",
    )
    for stream in (
        source.drop_data_columns(["label"]),
        source.select_data_columns(["amp", "unit", "time"]),
        Join()(source, other),
        Join()(other, source),
    ):
        tags = stream.keys()[0]
        expect = tuple(c for c in tags if c in expected)
        assert stream.head().status is ViewStatus.NOT_COMPUTED
        assert stream.head().data_tag_columns == expect
        for name, view in _views(stream.run()).items():
            assert view.data_tag_columns == expect, name


def test_dropping_the_data_role_leaves_a_plain_tag() -> None:
    """A data-tag whose data role is dropped is shown as a plain tag."""
    source = _source()
    with pytest.warns(Warning, match="data-tag"):
        stream = source.drop_data_columns(["time"])
    assert stream.run().data_tag_columns == ("unit",)
    assert source.drop_tag_columns(["unit"]).run().data_tag_columns == ("time",)


def test_function_pod_results_know_their_data_tags() -> None:
    """``pod(stream)`` lets data-tags fall through, and the view says so;
    ``pod.process`` makes every output tag plain."""
    source = _source()
    stream = scale.pod(source)
    # The data-tags in the stream's own tag order (which column order a stream shows
    # is ITL-1305).
    expected = tuple(c for c in stream.keys()[0] if c in DATA_TAGS)
    assert sorted(expected) == sorted(DATA_TAGS)
    head = _view(stream.head())
    assert CALLS == []
    assert head.status is ViewStatus.NOT_COMPUTED
    assert head.data_tag_columns == expected
    for name, view in _views(stream.run()).items():
        assert view.data_tag_columns == expected, name
    with pytest.warns(Warning, match="data-tag"):
        selected = source.select_data_columns(["amp"])
    exact = scale.pod.process(selected)
    assert _view(exact.run()).data_tag_columns == ()


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_job_nodes_know_their_data_tags(orchestrator: str) -> None:
    """Function and operator job nodes, before and after a run on either
    orchestrator, and their stored view."""
    job = PipelineJob(name="itl1304", database=InMemoryArrowDatabase())
    with job:
        scaled = scale.pod(_source(), label="scaled")
        _ = SelectDataColumns(["unit", "time", "scaled"])(scaled, label="sel")
    for label in ("scaled", "sel"):
        head = job.nodes[label].head()
        assert head.status is ViewStatus.NOT_COMPUTED
        assert head.data_tag_columns == _data_tags_of(job.nodes[label]), label
    assert CALLS == []
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    for label in ("scaled", "sel"):
        node = job.nodes[label]
        for name, view in _views(node.head()).items():
            assert view.data_tag_columns == _in_view_order(view), (label, name)
        view = node.run()
        assert view.data_tag_columns == _in_view_order(view), label
    stored = job.nodes["scaled"].as_source()
    assert stored.head().data_tag_columns == _data_tags_of(stored)


def test_a_loaded_job_knows_its_data_tags(tmp_path: Path) -> None:
    """A loaded job's nodes, before and after a load-only run, and their stored view."""
    path = tmp_path / "spikes.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(LAYOUTS["data_first"]))
        writer.writeheader()
        writer.writerows(_rows(LAYOUTS["data_first"]))
    source = CSVSource(
        str(path),
        tag_columns=list(TAG_ORDERS["interleaved"]),
        tag_only_columns=list(PLAIN),
        source_id="spikes",
    )
    job = PipelineJob(name="itl1304", database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        _ = scale.pod(source, label="scaled")
    _ = job.run()
    saved = tmp_path / "job.json"
    job.save(saved)
    CALLS.clear()

    loaded = PipelineJob.load(str(saved))
    node = loaded.nodes["scaled"]
    assert node.head().status is ViewStatus.NOT_COMPUTED
    assert node.head().data_tag_columns == _data_tags_of(node)
    stored = node.as_source().head()
    assert stored.data_tag_columns == _in_view_order(stored)
    _ = loaded.run(load_only=True)
    assert CALLS == []
    for name, view in _views(node.head()).items():
        assert view.status is ViewStatus.LOADED_ONLY, name
        assert view.data_tag_columns == _in_view_order(view), name


def test_a_derived_source_view_knows_its_data_tags() -> None:
    """A derived source over a computed stream keeps the data-tags its keys name."""
    job = PipelineJob(name="itl1304", database=InMemoryArrowDatabase())
    with job:
        _ = scale.pod(_source(), label="scaled")
    _ = job.run()
    derived = job.nodes["scaled"].as_source()
    assert isinstance(derived, DerivedSource)
    expected = _data_tags_of(derived)
    assert derived.run().data_tag_columns == expected
    assert derived.head().tail(1).data_tag_columns == expected


# ---------------------------------------------------------------------------
# Showing a stream with data-tags computes nothing (ITL-991)
# ---------------------------------------------------------------------------


def test_preview_of_a_function_pod_with_data_tags_calls_no_function() -> None:
    """Neither head, the preview nor the text form calls the function."""
    stream: Any = scale.pod(_source())
    _ = stream.head()
    _ = stream._repr_html_()
    _ = str(stream.head())
    assert CALLS == []


def test_preview_of_an_operator_with_data_tags_runs_no_operator() -> None:
    """The preview of a join over data-tags never runs the join over rows."""
    other = DictSource(
        [{"trial": 1, "session": "s1", "w": 1.0}],
        tag_columns=["trial", "session"],
        tag_only_columns=True,
        source_id="other",
    )
    stream = Join()(_source(), other)
    original = Join.static_process
    with mock.patch.object(
        Join, "static_process", autospec=True, side_effect=original
    ) as spy:
        head = stream.head()
        _ = stream._repr_html_()
        _ = str(head)
        over_rows = sum(
            any(s.as_table().num_rows for s in call.args[1:])
            for call in spy.call_args_list
        )
    assert over_rows == 0
    assert head.data_tag_columns == _in_view_order(head)


def test_preview_of_a_side_effect_with_data_tags_delivers_nothing() -> None:
    """Showing a side-effect stream over data-tags delivers nothing."""
    delivered: list[float] = []

    def deliver(amp: float) -> None:
        """Record a delivery."""
        delivered.append(amp)

    # The side effect reads amp only; the data-tags stay tags (plain ones here).
    with pytest.warns(Warning, match="data-tag"):
        selected = _source().select_data_columns(["amp"])
    stream = SideEffectPod(deliver)(selected)
    _ = stream.head()
    _ = stream._repr_html_()
    assert delivered == []


def test_job_preview_with_data_tags_writes_nothing() -> None:
    """Showing the nodes of a job that has not run writes nothing to its database."""
    store = InMemoryArrowDatabase()
    job = PipelineJob(name="itl1304", database=store)
    with job:
        _ = scale.pod(_source(), label="scaled")
    with (
        mock.patch.object(
            InMemoryArrowDatabase, "add_records", side_effect=AssertionError("write")
        ),
        mock.patch.object(
            InMemoryArrowDatabase, "add_record", side_effect=AssertionError("write")
        ),
    ):
        _ = job.nodes["scaled"]._repr_html_()
        _ = str(job.nodes["scaled"].head())
    assert CALLS == []


# ---------------------------------------------------------------------------
# Only the display is marked
# ---------------------------------------------------------------------------


def test_only_the_display_is_marked() -> None:
    """The view's table, as_table and the data frames carry no mark."""
    source = _source()
    view = source.run()
    names = list(source.as_table().column_names)
    assert view.table.column_names == names
    assert view.as_table().column_names == names
    assert list(view.as_polars_df().columns) == names
    assert source.keys() == (
        TAG_ORDERS["interleaved"],
        ("unit", "time", "label", "amp"),
    )


class _Cells(HTMLParser):
    """Collects every table row of an HTML display: each cell's text and style."""

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[tuple[str, str]]] = []
        self._cell: list[str] | None = None
        self._style = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self.rows.append([])
        elif tag in ("th", "td"):
            self._cell = []
            self._style = dict(attrs).get("style") or ""

    def handle_endtag(self, tag: str) -> None:
        if tag in ("th", "td") and self._cell is not None:
            self.rows[-1].append(("".join(self._cell), self._style))
            self._cell = None

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)


def _cells(html_text: str) -> list[list[tuple[str, str]]]:
    parser = _Cells()
    parser.feed(html_text)
    return parser.rows


BOLD = "font-weight:bold"
ACCENT = "color:#2a7fb8"
UNDERLINE = "text-decoration:underline"
TEXT_LEGEND = {"tag": "* = tag", "data-tag": "+ = data-tag (tag and data)"}
HTML_LEGEND = {
    "tag": "bold = tag",
    "data": "underlined = data",
    "data-tag": "bold underlined = data-tag (a tag that is also data)",
}


def _check_display(shown: object) -> None:
    """Both displays of ``shown`` (a view of the default columns) mark each plain tag,
    each data-tag and each data column, with one rule after the tags, and a legend
    naming exactly the marks shown: bold in the accent colour for a tag, an underline
    for data, both for a data-tag."""
    view = _view(shown)
    data_tags = set(view.data_tag_columns)
    plain = [c for c in view.tag_columns if c not in data_tags]
    others = [c for c in view.table.column_names if c not in view.tag_columns]
    # A view of the default columns shows user columns only: every other one is data.
    assert list(view.data_columns) == others
    roles = [
        *(["tag"] if plain else []),
        *(["data"] if others else []),
        *(["data-tag"] if data_tags else []),
    ]

    html_text = view._repr_html_()
    legend = "; ".join(HTML_LEGEND[role] for role in roles)
    if legend:
        assert f"<small>{legend}</small>" in html_text
    if view.table.num_columns:
        rows = _cells(html_text)
        header = rows[0]
        assert [name for name, _ in header] == [*view.tag_columns, *others]
        for name, style in header:
            if name in data_tags:
                assert BOLD in style and ACCENT in style and UNDERLINE in style, name
            elif name in view.tag_columns:
                assert BOLD in style and ACCENT in style, name
                assert UNDERLINE not in style, name
            else:
                assert "font-weight:normal" in style and UNDERLINE in style, name
                assert ACCENT not in style, name
        # Only the first header row is marked: the type row and the rows of values
        # carry no mark.
        for row in rows[1:]:
            assert all(UNDERLINE not in s and BOLD not in s for _, s in row)
        if view.tag_columns and others:
            for row in rows:
                ruled = [i for i, (_, s) in enumerate(row) if "border-right" in s]
                assert ruled == [len(view.tag_columns) - 1]

    lines = str(view).split("\n")
    text_legend = "; ".join(TEXT_LEGEND[role] for role in roles if role in TEXT_LEGEND)
    if text_legend:
        assert lines[1] == text_legend
    else:
        assert "= tag" not in str(view)
    if view.table.num_columns:
        names_line = next(line for line in lines if line.startswith("│"))
        names = [cell.strip() for cell in names_line.strip("│").split("┆")]
        assert names == [
            *(("+" if c in data_tags else "*") + c for c in view.tag_columns),
            *others,
        ]


# ---------------------------------------------------------------------------
# The marks: a plain tag, a data-tag and a data column
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("layout", list(LAYOUTS))
@pytest.mark.parametrize("tag_order", list(TAG_ORDERS))
def test_both_displays_mark_plain_tags_and_data_tags(
    tag_order: str, layout: str
) -> None:
    """Two plain tags, two data-tags and two data columns, in several tag orders and
    table layouts: every view's displays mark each role and say so."""
    source = _source(TAG_ORDERS[tag_order], LAYOUTS[layout])
    for built in (source.head(), source.run()):
        for name, view in _views(built).items():
            assert set(view.data_tag_columns) == set(DATA_TAGS), name
            _check_display(view)


def test_the_marks_of_a_known_stream() -> None:
    """The exact header styles, text names and legends of one stream: tags in tag order
    (plain and data-tags interleaved), then two data columns."""
    view = _source(TAG_ORDERS["interleaved"], LAYOUTS["tags_first"]).run()
    header = _cells(view._repr_html_())[0]
    assert header == [
        ("session", "font-weight:bold;color:#2a7fb8"),
        ("unit", "font-weight:bold;text-decoration:underline;color:#2a7fb8"),
        ("trial", "font-weight:bold;color:#2a7fb8"),
        (
            "time",
            "font-weight:bold;text-decoration:underline;color:#2a7fb8;"
            "border-right:2px solid currentColor",
        ),
        ("amp", "font-weight:normal;text-decoration:underline"),
        ("label", "font-weight:normal;text-decoration:underline"),
    ]
    assert (
        "<small>bold = tag; underlined = data; "
        "bold underlined = data-tag (a tag that is also data)</small>"
    ) in view._repr_html_()
    text = str(view)
    assert text.split("\n")[1] == "* = tag; + = data-tag (tag and data)"
    assert "*session ┆ +unit ┆ *trial ┆ +time ┆ amp" in text
    # The legend's order is the marks' order, whatever the first column's role.
    first = _source(TAG_ORDERS["data_tags_first"]).run()
    assert str(first).split("\n")[1] == "* = tag; + = data-tag (tag and data)"
    assert (
        "<small>bold = tag; underlined = data; bold underlined = data-tag"
        in first._repr_html_()
    )


def test_a_stream_with_only_plain_tags_shows_no_data_tag_mark() -> None:
    """No data-tag: no header is both bold and underlined, and the legend names the
    tag and data marks only."""
    view = _source(plain=PLAIN + DATA_TAGS).run()
    _check_display(view)
    assert all(
        not ("font-weight:bold" in s and UNDERLINE in s)
        for _, s in _cells(view._repr_html_())[0]
    )
    assert "<small>bold = tag; underlined = data</small>" in view._repr_html_()
    assert "data-tag" not in view._repr_html_()
    assert str(view).split("\n")[1] == "* = tag"


def test_a_stream_with_only_data_tags_shows_no_plain_tag_mark() -> None:
    """Every tag a data-tag: the legend names the data and data-tag marks only; with
    no other data column, the data-tag mark only."""
    view = _source(plain=()).run()
    _check_display(view)
    assert "<small>underlined = data; bold underlined = data-tag" in view._repr_html_()
    assert str(view).split("\n")[1] == "+ = data-tag (tag and data)"
    only = (
        _source(plain=())
        .select_data_columns(["session", "unit", "trial", "time"])
        .run()
    )
    _check_display(only)
    assert only.data_columns == ()
    assert (
        "<small>bold underlined = data-tag (a tag that is also data)</small>"
        in only._repr_html_()
    )


def test_a_stream_without_tags_marks_its_data_only() -> None:
    """No tag: every column is underlined and the legend names the data mark only."""
    view = ArrowTableStream(pa.table({"amp": [0.5, 0.25], "label": ["s", "b"]})).run()
    _check_display(view)
    assert "<small>underlined = data</small>" in view._repr_html_()
    assert "= tag" not in str(view)


def test_hidden_column_groups_keep_their_look() -> None:
    """With every column group shown, only the user data columns are underlined:
    system-tag columns keep the tag mark they had (they are tag columns of the view),
    and source-info, context and content-hash columns stay unmarked."""
    source = _source(TAG_ORDERS["interleaved"], LAYOUTS["data_first"])
    computed = scale.pod(source)
    _ = computed.run()
    for head in (
        _view(source.head(all_info=True)),
        _view(computed.head(all_info=True)),
    ):
        user_tags = [c for c in head.tag_columns if not c.startswith("_")]
        system_tags = [c for c in head.tag_columns if c.startswith("_tag")]
        unmarked = [
            c
            for c in head.table.column_names
            if c not in head.tag_columns and c not in head.data_columns
        ]
        assert sorted(head.data_tag_columns) == sorted(DATA_TAGS)
        assert len(system_tags) == 2
        assert any(c.startswith("_source_") for c in unmarked)
        assert "_context_key" in unmarked
        assert all(c.startswith("_") for c in unmarked)
        assert not any(c.startswith("_") for c in head.data_columns)
        header = dict(_cells(head._repr_html_())[0])
        for name in system_tags:
            assert header[name].startswith("font-weight:bold;color:#2a7fb8"), name
            assert UNDERLINE not in header[name], name
        for name in user_tags:
            assert BOLD in header[name], name
        for name in head.data_columns:
            assert header[name] == "font-weight:normal;text-decoration:underline"
        for name in unmarked:
            assert header[name] == "", name
        # The rule still follows the last tag.
        ruled = [name for name, style in header.items() if "border-right" in style]
        assert ruled == [head.tag_columns[-1]]


def test_every_kind_of_stream_shows_the_marks() -> None:
    """A raw stream, operator results, a function pod result and the preview."""
    source = _source()
    other = DictSource(
        [{"trial": r["trial"], "session": r["session"], "w": 1.0} for r in ROWS[:2]],
        tag_columns=["trial", "session"],
        tag_only_columns=True,
        source_id="other",
    )
    raw = ArrowTableStream(
        pa.Table.from_pylist(_rows(LAYOUTS["data_first"])),
        tag_columns=list(TAG_ORDERS["data_tags_first"]),
        data_tags=["unit", "time"],
    )
    streams: tuple[Any, ...] = (
        raw,
        source.drop_data_columns(["label"]),
        Join()(source, other),
        Join()(other, source),
        scale.pod(source),
    )
    for stream in streams:
        _check_display(stream.head())
        _check_display(stream.run())
        _check_display(stream.run().tail(2).sort_by_tags())
        preview = stream._repr_html_()
        assert HTML_LEGEND["data-tag"] in preview
        assert HTML_LEGEND["data"] in preview


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_job_nodes_show_the_marks(orchestrator: str) -> None:
    """A job's function and operator nodes, before and after a run, and their
    stored view, show the marks."""
    job = PipelineJob(name="itl1304", database=InMemoryArrowDatabase())
    with job:
        scaled = scale.pod(_source(), label="scaled")
        _ = SelectDataColumns(["unit", "time", "scaled"])(scaled, label="sel")
    for label in ("scaled", "sel"):
        _check_display(job.nodes[label].head())
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    for label in ("scaled", "sel"):
        node = job.nodes[label]
        _check_display(node.head())
        _check_display(node.run())
        assert HTML_LEGEND["data-tag"] in node._repr_html_()
    _check_display(job.nodes["scaled"].as_source().head())
    # Only the run called the function, once per row.
    assert len(CALLS) == len(ROWS)
