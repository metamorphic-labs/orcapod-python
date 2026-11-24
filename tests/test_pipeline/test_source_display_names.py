"""A pipeline shows a source by its assigned label, display only (ITL-1454)."""

from __future__ import annotations

import asyncio
import itertools
import warnings
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.sources.display_names import (
    LabelledSource,
    display_names,
    labelled_source,
)
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import RestrictedAggregationWarning
from orcapod.sources import DictSource, SpiralDBTableSource
from tests.test_databases import fake_spiral_sdk


@function_pod(output_keys="z")
def add(x: int, y: int) -> int:
    """Return ``x + y``."""
    return x + y


def _left(label: str | None = "units", source_id: str = "left") -> DictSource:
    return DictSource(
        [{"g": i % 2, "k": i, "x": i, "x2": -i} for i in range(4)],
        tag_columns=["g", "k"],
        tag_only_columns=True,
        source_id=source_id,
        label=label,
    )


def _right(label: str | None = "scales", source_id: str = "right") -> DictSource:
    return DictSource(
        [{"k": i, "g": i % 2, "y": 10 * i, "y2": -i} for i in range(4)],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id=source_id,
        label=label,
    )


def _job(
    left: Any,
    right: Any,
    pod_label: str = "sum",
    database: Any = None,
    swap: bool = False,
) -> PipelineJob:
    job = PipelineJob(name="j", database=database or InMemoryArrowDatabase())
    with job:
        first, second = (right, left) if swap else (left, right)
        add.pod(first, second, label=pod_label)
    return job


def _texts(job: PipelineJob) -> dict[str, str]:
    nodes = job.diagram().to_dict()["nodes"]
    return {n["label"]: n.get("text", n["label"]) for n in nodes}


def test_the_diagram_draws_the_label_and_gives_the_id_in_the_details() -> None:
    job = _job(_left(), _right())
    diagram = job.diagram()
    assert str(diagram).splitlines()[1:3] == [
        "  units (source · DictSource · bound)",
        "  scales (source · DictSource · bound)",
    ]
    assert "<- scales, units" in str(diagram) or "<- units, scales" in str(diagram)
    by_label = {n["label"]: n for n in diagram.to_dict()["nodes"]}
    assert by_label["left"]["facts"] == [["source id", "dict_v1:left"]]
    assert by_label["right"]["facts"] == [["source id", "dict_v1:right"]]
    for picture in (diagram.to_svg(), diagram.to_html()):
        assert ">units</" in picture and "source id: dict_v1:left" in picture
        assert ">left</" not in picture


def test_the_node_preview_shows_the_label() -> None:
    job = _job(_left(), _right(label=None))
    job_html: Any = job.nodes["left"]._repr_html_()
    assert "units" in job_html
    plain: Any = _job(_left(None), _right(None)).nodes["left"]._repr_html_()
    assert "units" not in plain and "left" in plain


def test_explain_and_trace_name_the_label_and_the_id_once() -> None:
    job = _job(_left(), _right())
    job.run()
    text = job.nodes["sum"].explain().text()
    assert "source 'units' (DictSource, dict_v1:left)" in text
    assert "rows without a match in 'scales' are dropped" in text
    assert text.count("dict_v1:left") == text.count("dict_v1:right") == 1
    readings = job.nodes["sum"].explain().sources
    assert [r.name for r in readings] == ["dict_v1:left", "dict_v1:right"]
    trace = job.nodes["sum"].trace({"k": 1}, "z")
    assert "units row_1 x = 1" in trace.text()
    assert trace.text().splitlines()[-2:] == [
        "source scales: dict_v1:right",
        "source units: dict_v1:left",
    ]
    plain_job = _job(_left(None), _right(None))
    plain_job.run()
    plain = plain_job.nodes["sum"].trace({"k": 1}, "z")
    assert plain.source_cells() == trace.source_cells()
    assert plain.highlight() == trace.highlight()


def test_one_table_under_two_labels_is_explained_by_its_id() -> None:
    joined = _left("a").join(_right("b")).semi_join(_left("other"))
    text = joined.explain().text()
    assert "source 'dict_v1:left' (DictSource)" in text
    assert "source 'b' (DictSource, dict_v1:right)" in text


@pytest.mark.parametrize("swap", [False, True])
def test_only_the_shown_text_changes(swap: bool) -> None:
    """Names, hashes, ids, stored paths, order and topology are those without labels."""
    labelled_db, plain_db = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    labelled = _job(_left(), _right(), database=labelled_db, swap=swap)
    plain = _job(_left(None), _right(None), database=plain_db, swap=swap)
    assert list(labelled.nodes) == list(plain.nodes)
    for name, node in plain.nodes.items():
        other: Any = labelled.nodes[name]
        assert other.label == node.label
        assert other.content_hash() == node.content_hash()
        assert other.pipeline_hash() == node.pipeline_hash()
        assert other.node_uri == node.node_uri
        assert other.output_schema(all_info=True) == node.output_schema(all_info=True)
    labelled.run(), plain.run()
    for name, node in plain.nodes.items():
        # A function's source info holds a random result id, so it is not compared.
        info = {"system_tags": True, "source": node.node_type != "function"}
        want = node.as_table(columns=info)
        got = labelled.nodes[name].as_table(columns=info)
        assert got.equals(want)
    assert sorted(labelled_db.list_table_paths(())) == sorted(
        plain_db.list_table_paths(())
    )
    ours, theirs = labelled.diagram().to_dict(), plain.diagram().to_dict()
    assert ours["edges"] == theirs["edges"]
    for a, b in zip(ours["nodes"], theirs["nodes"], strict=True):
        a.pop("text", None)
        a["facts"] = [f for f in a["facts"] if f[0] != "source id"]
        assert a == b
    assert labelled.left is labelled.nodes["left"]
    labelled.bind(sources={"left": _left("renamed")})
    assert _texts(labelled)["left"] == "renamed"


def test_without_a_label_the_texts_are_unchanged() -> None:
    job = _job(_left(None), _right(None))
    job.run()
    assert job.diagram().to_text().splitlines()[1] == (
        "  left (source · DictSource · bound)"
    )
    assert all("text" not in n for n in job.diagram().to_dict()["nodes"])
    assert "source id" not in job.diagram().to_html().split("<script>")[0]
    assert "source 'dict_v1:left' (DictSource)\n" in job.nodes["sum"].explain().text()
    assert "\nsource " not in job.nodes["sum"].trace({"k": 1}, "z").text()


def test_a_loaded_job_shows_the_slot_names(tmp_path: Path) -> None:
    job = _job(_left(), _right(), database=DeltaTableDatabase(tmp_path / "db"))
    job.run()
    path = str(tmp_path / "job.json")
    job.save(path)
    loaded = PipelineJob.load(path)
    assert set(_texts(loaded)) == {"left", "right", *job.nodes}
    assert _texts(loaded)["left"] == "left" and _texts(loaded)["right"] == "right"


@pytest.mark.parametrize("swap", [False, True])
def test_shared_labels_are_told_apart(swap: bool) -> None:
    assert _texts(_job(_left("t"), _right("t"), swap=swap)) == {
        "left": "t (left)",
        "right": "t (right)",
        "Join": "Join",
        "SelectDataColumns": "SelectDataColumns",
        "sum": "sum",
    }


def test_a_label_equal_to_a_pods_or_another_slots_is_told_apart() -> None:
    assert _texts(_job(_left("sum"), _right()))["left"] == "sum (left)"
    assert _texts(_job(_left("sum"), _right()))["sum"] == "sum"
    crossed = _texts(_job(_left("right"), _right("left")))
    assert (crossed["left"], crossed["right"]) == ("right (left)", "left (right)")
    assert _texts(_job(_left("left"), _right()))["left"] == "left"


def test_no_two_texts_are_ever_equal() -> None:
    def source(label: str, name: str) -> LabelledSource:
        return LabelledSource(label, f"k_v1:{name}", ("", "", "", name, f"k_v1:{name}"))

    sources = {
        "a": source("t", "a"),
        "b": source("t", "b"),
        "c": source("t (a)", "c"),
        "d": source("u", "e (f)"),
        "e": source("pod", "x"),
    }
    names = {*sources, "pod", "u (e (f))"}
    expected = display_names(sources, names)
    shown = [expected.get(n, n) for n in names]
    assert len(set(shown)) == len(shown)
    for order in itertools.permutations(sources):
        assert display_names({n: sources[n] for n in order}, names) == expected


# --- SpiralDB: dataset, then project -----------------------------------------

_KEYS = pa.schema([pa.field("g", pa.int64(), False), pa.field("k", pa.int64(), False)])


@pytest.fixture
def spiral(tmp_path: Path) -> Iterator[None]:
    """Tables ``d1.units``, ``d2.units`` of ``p1`` and ``d1.units`` of ``p2``."""
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        for project, dataset, a, b in (
            ("p1", "d1", "x", "x2"),
            ("p1", "d2", "y", "y2"),
            ("p2", "d1", "y", "y2"),
        ):
            table = fake_spiral_sdk.ExternalProject(project).create_table(
                f"{dataset}.units", key_schema=_KEYS
            )
            schema = pa.schema(
                [*_KEYS, pa.field(a, pa.int64()), pa.field(b, pa.int64())]
            )
            table.write(
                pa.table(
                    {"g": [0, 1], "k": [0, 1], a: [1, 2], b: [3, 4]}, schema=schema
                )
            )
            table.record(schema)
        yield


def _spiral(project: str, dataset: str, tag_only: Any = True) -> SpiralDBTableSource:
    return SpiralDBTableSource(
        project_id=project,
        dataset=dataset,
        table_name="units",
        label="units",
        tag_only_columns=tag_only,
    )


@pytest.mark.parametrize(
    ("other", "shown"),
    [
        (("p1", "d2"), {"units (d1)", "units (d2)"}),
        (("p2", "d1"), {"units (p1)", "units (p2)"}),
    ],
)
@pytest.mark.parametrize("swap", [False, True])
def test_one_table_of_two_datasets_or_projects(
    spiral: None, other: tuple[str, str], shown: set[str], swap: bool
) -> None:
    sources = [_spiral("p1", "d1"), _spiral(*other)]
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        first, second = reversed(sources) if swap else sources
        first.join(second, label="both")
    texts = _texts(job)
    assert {t for n, t in texts.items() if n != "both"} == shown
    assert not {"units (d1)", "units (p1)"} & set(job.nodes)


@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("by", [["g", "k"], ["k", "g"]])
@pytest.mark.parametrize("tag_only", [True, ()])
def test_the_restricted_aggregation_warning_names_the_label(
    spiral: None, use_async: bool, by: list[str], tag_only: Any
) -> None:
    sources = [_spiral("p1", dataset, tag_only).where(g=0) for dataset in ("d1", "d2")]
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        for source, label in zip(sources, ("one", "two"), strict=True):
            source.group_by(by, label=label)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        if use_async:
            asyncio.run(job.run_async())
        else:
            job.run()
    (message,) = [
        str(w.message)
        for w in caught
        if issubclass(w.category, RestrictedAggregationWarning)
    ]
    one, two = sources
    assert (
        "'units (d1)' feeds GroupBy 'one'; 'units (d2)' feeds GroupBy 'two'" in message
    )
    assert message.endswith(
        f" 'units (d1)' is {one.source_id}. 'units (d2)' is {two.source_id}."
    )


# --- no code of a source runs (CodeRabbit on PR 551) ---------------------------


class _Hooked(DictSource):
    """A source whose attribute access runs user code while ``armed``."""

    SOURCE_KIND = "hooked_display_test"
    SOURCE_VERSION = 1
    armed = False

    def __getattribute__(self, name: str) -> Any:
        if _Hooked.armed:
            raise AssertionError(f"read {name}")
        return super().__getattribute__(name)


class _Slotted:
    """A source-like object without an instance dictionary."""

    __slots__ = ()


def test_reading_the_label_runs_no_code_of_the_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _Hooked(
        [{"g": 0, "k": 1, "x": 1, "x2": 2}],
        tag_columns=["g", "k"],
        tag_only_columns=True,
        source_id="left",
        label="units",
    )
    monkeypatch.setattr(_Hooked, "armed", True)
    found = labelled_source(source)
    monkeypatch.setattr(_Hooked, "armed", False)
    assert found is not None
    assert (found.label, found.source_id) == ("units", "hooked_display_test_v1:left")


@pytest.mark.parametrize("label", [None, "cache"])
def test_a_cache_over_an_object_without_a_dict_has_no_shown_name(
    label: str | None,
) -> None:
    cached = _left(None).cached(InMemoryArrowDatabase(), label=label)
    found = labelled_source(cached)
    assert (found and (found.label, found.source_id)) == (
        ("cache", "dict_v1:left") if label else None
    )
    vars(cached)["_source"] = _Slotted()
    assert labelled_source(cached) is None
