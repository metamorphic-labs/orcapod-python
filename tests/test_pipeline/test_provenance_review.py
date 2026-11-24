"""Regression tests for the independent review of PR 530 (ITL-1432).

Each test reproduces one finding of the review (head 42d5e932) and fails without its fix;
together they also cover the review's list of missing coverage. The rules they hold:

- reading only: a trace never calls a source's ``as_table`` / ``iter_data``, never lets
  a ``CachedSource`` materialize or write; a source with no held snapshot is reported as
  not read (``unavailable``);
- no guessed link: an ambiguous link is refused, and every reported value equals the
  held row it came from;
- element lineage: ``Index`` after ``Batch`` follows one element; a token is matched
  exactly before it is read as an access path;
- ``why_missing`` agrees with ``trace``: folds resolved by refold, a failed refold
  raises, the answer is about the requested node;
- strict selector values; result types usable with any value; highlight edges from the
  real graph, the note in the picture's title.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import ProvenanceInconsistencyError
from orcapod.operators import (
    Batch,
    DropTagColumns,
    Filter,
    GroupBy,
    Index,
    Join,
    MergeJoin,
    SelectDataColumns,
    SemiJoin,
)
from orcapod.sources import ArrowTableSource, CachedSource, DictSource

pytestmark = pytest.mark.filterwarnings("ignore::Warning")


def table_source(
    rows: list[dict[str, Any]], schema: pa.Schema, tags: list[str], source_id: str
) -> ArrowTableSource:
    """An Arrow table source whose listed tags are plain tags (``tag_only_columns``)."""
    return ArrowTableSource(
        pa.Table.from_pylist(rows, schema=schema),
        tag_columns=tags,
        tag_only_columns=True,
        source_id=source_id,
    )


TWO_BY_TWO = pa.schema(
    [
        pa.field("k", pa.large_string(), nullable=False),
        pa.field("g", pa.large_string(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
        pa.field("y", pa.int64(), nullable=False),
    ]
)


def rows_kgxy(*values: tuple[str, str, int, int]) -> list[dict[str, Any]]:
    """Rows of ``TWO_BY_TWO`` from value tuples in its column order (k, g, x, y)."""
    return [dict(zip(TWO_BY_TWO.names, v)) for v in values]


# ---------------------------------------------------------------------------
# Blocker: reading only
# ---------------------------------------------------------------------------


def test_a_cached_source_is_never_materialized_or_written(tmp_path: Path) -> None:
    cache = DeltaTableDatabase(tmp_path / "cache")
    inner = DictSource(
        [{"k": "a", "g": "1", "x": 1, "y": 2}, {"k": "b", "g": "1", "x": 3, "y": 4}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="inner",
    )
    cached = CachedSource(inner, cache_database=cache)
    job = PipelineJob(name="c", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(cached)
    assert cache.list_table_paths() == []
    # The cache's slot is named after the source it wraps (ITL-1425).
    trace = job.nodes["inner"].trace({"k": "a", "g": "1"}, "x")
    assert trace.status == "unavailable"
    assert cache.list_table_paths() == []  # nothing was materialized or written


def test_an_overridden_source_read_is_never_called(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    class Spied(DictSource):
        def as_table(self, *args: Any, **kwargs: Any) -> Any:
            calls.append("as_table")
            return super().as_table(*args, **kwargs)

    source = Spied(
        [{"k": "a", "g": "1", "x": 1, "y": 2}, {"k": "b", "g": "1", "x": 4, "y": 5}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="spied",
    )
    job = PipelineJob(name="s", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(source)
    job.run()
    calls.clear()
    trace = job.nodes["SelectDataColumns"].trace({"k": "a", "g": "1"}, "x")
    assert calls == []
    (cell,) = trace.source_cells()
    assert (cell.source_id, cell.record_id, cell.value) == ("dict_v1:spied", "row_0", 1)


# ---------------------------------------------------------------------------
# Majors: ambiguous links, values, element lineage, exact tokens, any value
# ---------------------------------------------------------------------------


def test_merge_join_of_inputs_with_equal_pipeline_hashes() -> None:
    left = table_source(rows_kgxy(("a", "1", 1, 2)), TWO_BY_TWO, ["k", "g"], "same")
    right = table_source(
        rows_kgxy(("a", "1", 100, 200)), TWO_BY_TWO, ["k", "g"], "same_more"
    )
    job = PipelineJob(name="m", database=InMemoryArrowDatabase())
    with job:
        MergeJoin()(left, right)
    job.run()
    node = job.nodes["MergeJoin"]
    trace = node.trace({"k": "a", "g": "1"}, "x")
    assert trace.root.value == [1, 100]
    assert [(c.source_id, c.value) for c in trace.source_cells()] == [
        ("arrow_table_v1:same", 1),
        ("arrow_table_v1:same_more", 100),
    ]
    assert not node.why_missing("same", "row_0").missing
    assert not node.why_missing("same_more", "row_0").missing


def test_two_cached_versions_of_one_record(tmp_path: Path) -> None:
    cache = DeltaTableDatabase(tmp_path / "cache")
    first = DictSource(
        [{"k": "a", "g": "1", "x": 1, "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="v",
    )
    CachedSource(first, cache_database=cache, cache_path=("shared",)).as_table()
    second = DictSource(
        [{"k": "a", "g": "1", "x": 2, "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="v",
    )
    cached = CachedSource(second, cache_database=cache, cache_path=("shared",))
    job = PipelineJob(name="v", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(cached)
    job.run()
    node = job.nodes["SelectDataColumns"]
    held = sorted(r["x"] for r in node.as_table().to_pylist())
    assert held == [1, 2]  # both versions are held, so both are tested
    for x in held:
        trace = node.trace({"k": "a", "g": "1", "x": x}, "x")
        # The held version, never the other one.
        assert [c.value for c in trace.source_cells()] == [x]


def test_a_value_the_held_rows_do_not_hold_is_refused() -> None:
    source = table_source(
        rows_kgxy(("a", "1", 1, 0), ("b", "1", 4, 0)), TWO_BY_TWO, ["k", "g"], "src"
    )
    job = PipelineJob(name="gv", database=InMemoryArrowDatabase())
    with job:
        GroupBy(["g"])(source)
    job.run()
    node = job.nodes["GroupBy"]
    assert [c.value for c in node.trace({"g": "1"}, "x").source_cells()] == [1, 4]
    # The source's held snapshot now disagrees with what the run read.
    stream = vars(source)["_stream"]
    table = stream.as_table(columns={"system_tags": True, "source": True})
    changed = table.set_column(
        table.schema.get_field_index("x"), "x", pa.array([101, 104], pa.int64())
    )
    vars(source)["_stream"] = ArrowTableStream(
        changed, tag_columns=["k", "g"], data_context=stream.data_context
    )
    with pytest.raises(ProvenanceInconsistencyError):
        node.trace({"g": "1"}, "x")


def test_a_source_read_that_would_return_other_values_is_not_used(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = DictSource(
        [{"k": "a", "g": "1", "x": 1, "y": 0}, {"k": "b", "g": "1", "x": 4, "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="src",
    )
    job = PipelineJob(name="gs", database=InMemoryArrowDatabase())
    with job:
        GroupBy(["g"])(source)
    job.run()
    calls: list[int] = []

    def other_values(self: Any, *args: Any, **kwargs: Any) -> Any:
        calls.append(1)
        raise AssertionError("a source read")

    monkeypatch.setattr(DictSource, "as_table", other_values)
    trace = job.nodes["GroupBy"].trace({"g": "1"}, "x")
    assert calls == []
    # The fold reads its members from the source: not held, so not read.
    assert trace.status == "unavailable"
    assert all(c.value in (1, 4, None) for c in trace.source_cells())


def test_index_after_batch_follows_one_element() -> None:
    source = table_source(
        rows_kgxy(("a", "1", 1, 0), ("b", "1", 4, 0)), TWO_BY_TWO, ["k", "g"], "src"
    )
    job = PipelineJob(name="ib", database=InMemoryArrowDatabase())
    with job:
        Index("x", 0)(Batch(batch_size=2)(source))
    job.run()
    node = job.nodes["Index"]
    (row,) = node.as_table().to_pylist()
    assert row["x"] == 1
    trace = node.trace({"k": row["k"], "g": row["g"]}, "x")
    assert [(c.record_id, c.value) for c in trace.source_cells()] == [("row_0", 1)]


def test_a_token_is_matched_exactly_before_an_access_path() -> None:
    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("g", pa.large_string(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
            pa.field("x.b", pa.int64(), nullable=False),
        ]
    )
    source = table_source(
        [{"k": "a", "g": "1", "x": 1, "x.b": 2}], schema, ["k", "g"], "dots"
    )
    job = PipelineJob(name="xb", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x.b"])(source)
    job.run()
    trace = job.nodes["SelectDataColumns"].trace({"k": "a", "g": "1"}, "x.b")
    assert [(c.column, c.value) for c in trace.source_cells()] == [("x.b", 2)]


def test_source_cells_of_a_list_value() -> None:
    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("g", pa.large_string(), nullable=False),
            pa.field("xs", pa.large_list(pa.int64()), nullable=False),
            pa.field("y", pa.int64(), nullable=False),
        ]
    )
    source = table_source(
        [{"k": "a", "g": "1", "xs": [1, 2], "y": 3}], schema, ["k", "g"], "lists"
    )
    job = PipelineJob(name="lv", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["xs"])(source)
    job.run()
    trace = job.nodes["SelectDataColumns"].trace({"k": "a", "g": "1"}, "xs")
    (cell,) = trace.source_cells()
    assert cell.value == [1, 2]


# ---------------------------------------------------------------------------
# why_missing agrees with trace
# ---------------------------------------------------------------------------


@function_pod(output_keys=["total", "count"])
def totals(x: list[int]) -> tuple[int, int]:
    return sum(x), len(x)


def grouped_job() -> PipelineJob:
    source = table_source(
        rows_kgxy(("a", "1", 1, 0), ("b", "1", 4, 0), ("c", "2", 7, 0)),
        TWO_BY_TWO,
        ["k", "g"],
        "src",
    )
    job = PipelineJob(name="gf", database=InMemoryArrowDatabase())
    with job:
        totals.pod(SelectDataColumns(["x"])(GroupBy(["g"])(source)), label="totals")
    job.run()
    return job


def test_why_missing_after_a_fold_agrees_with_the_stored_answer() -> None:
    job = grouped_job()
    answer = job.nodes["totals"].why_missing("src", "row_1")
    assert not answer.missing
    assert answer.reached_function is True
    assert "never reached" not in answer.text()


def test_why_missing_raises_when_a_refold_disagrees(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = grouped_job()
    real = GroupBy.fold_members

    def reversed_members(self: GroupBy, table: Any, tags: Any) -> list[list[int]]:
        return [m[::-1] for m in real(self, table, tags)]

    monkeypatch.setattr(GroupBy, "fold_members", reversed_members)
    with pytest.raises(ProvenanceInconsistencyError):
        job.nodes["totals"].why_missing("src", "row_0")


def branched_job() -> PipelineJob:
    source = table_source(
        rows_kgxy(("a", "1", 1, 10), ("b", "1", 5, 50)), TWO_BY_TWO, ["k", "g"], "src"
    )
    job = PipelineJob(name="br", database=InMemoryArrowDatabase())
    with job:
        kept = SelectDataColumns(["x"])(Filter({"x": 1})(source))
        whole = SelectDataColumns(["y"])(source)
        Join()(DropTagColumns(["k"])(kept), DropTagColumns(["k"])(whole))
    job.run()
    return job


def test_why_missing_answers_about_the_requested_node() -> None:
    job = branched_job()
    answer = job.nodes["Join"].why_missing("src", "row_1")
    # row_1 is filtered on one branch but reaches Join on the other.
    assert not answer.missing
    assert answer.removed_by is None and answer.because == ""


def test_highlight_edges_are_edges_of_the_graph() -> None:
    job = branched_job()
    answer = job.nodes["Join"].why_missing("src", "row_0")
    real = {
        (upstream.label, label)
        for label, node in job.nodes.items()
        for upstream in node.upstreams
    }
    highlight = answer.highlight()
    assert highlight.edges <= real
    assert ("src", "Filter") in highlight.edges or (
        "src",
        "SelectDataColumns_2",
    ) in highlight.edges
    svg = job.diagram(highlight=highlight).to_svg()
    assert highlight.note in svg


# ---------------------------------------------------------------------------
# Strict selectors
# ---------------------------------------------------------------------------


def test_selectors_use_orcapods_value_rules() -> None:
    schema = pa.schema(
        [
            pa.field("k", pa.float64(), nullable=False),
            pa.field("n", pa.int64(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
            pa.field("y", pa.int64(), nullable=False),
        ]
    )
    source = table_source(
        [
            {"k": float("nan"), "n": 0, "x": 1, "y": 0},
            {"k": float("nan"), "n": 1, "x": 2, "y": 0},
            {"k": 1.0, "n": 0, "x": 3, "y": 0},
        ],
        schema,
        ["k", "n"],
        "nan",
    )
    job = PipelineJob(name="sel", database=InMemoryArrowDatabase())
    with job:
        GroupBy(["k"])(source)
        SelectDataColumns(["x"])(source)
    job.run()
    trace = job.nodes["GroupBy"].trace({"k": float("nan")}, "x")
    assert sorted(c.value for c in trace.source_cells()) == [1, 2]
    # A bool is never 0: no held row matches.
    assert (
        job.nodes["SelectDataColumns"].trace({"k": 1.0, "n": False}, "x").matched == 0
    )


# ---------------------------------------------------------------------------
# Second review (head 725a026f)
# ---------------------------------------------------------------------------


@function_pod(output_keys="s")
def add(x: int, y: int) -> int:
    return x + y


def test_a_function_reading_a_source_refuses_a_changed_snapshot() -> None:
    source = table_source(rows_kgxy(("a", "1", 1, 2)), TWO_BY_TWO, ["k", "g"], "src")
    job = PipelineJob(name="fs", database=InMemoryArrowDatabase())
    with job:
        add.pod(source, label="sums")
    job.run()
    node = job.nodes["sums"]
    assert [c.value for c in node.trace({"k": "a", "g": "1"}, "s").source_cells()] == [
        1,
        2,
    ]
    stream = vars(source)["_stream"]
    table = stream.as_table(columns={"system_tags": True, "source": True})
    changed = table.set_column(
        table.schema.get_field_index("x"), "x", pa.array([101], pa.int64())
    )
    vars(source)["_stream"] = ArrowTableStream(
        changed, tag_columns=["k", "g"], data_context=stream.data_context
    )
    with pytest.raises(ProvenanceInconsistencyError, match="not the input"):
        node.trace({"k": "a", "g": "1"}, "s")


def test_a_data_tag_key_after_group_by_traces_to_its_members() -> None:
    source = DictSource(
        [
            {"k": "a", "g": "1", "x": 1, "y": 0},
            {"k": "a", "g": "2", "x": 4, "y": 0},
            {"k": "b", "g": "1", "x": 7, "y": 0},
        ],
        tag_columns=["k", "g"],
        source_id="src",
    )  # the default role: k and g are data-tags
    job = PipelineJob(name="dt", database=InMemoryArrowDatabase())
    with job:
        GroupBy(["k"])(source)
    job.run()
    trace = job.nodes["GroupBy"].trace({"k": "a"}, "k")
    assert trace.root.value == "a"
    assert [(c.record_id, c.column, c.value) for c in trace.source_cells()] == [
        ("row_0", "k", "a"),
        ("row_1", "k", "a"),
    ]


def aliased_job(tmp_path: Path) -> tuple[PipelineJob, DeltaTableDatabase]:
    cache = DeltaTableDatabase(tmp_path / "cache")
    inner = DictSource(
        [{"k": "a", "g": "1", "x": 1, "y": 0}, {"k": "b", "g": "1", "x": 2, "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="inner",
    )
    cached = CachedSource(inner, cache_database=cache)
    job = PipelineJob(name="al", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(cached)
    job.run()
    return job, cache


def test_a_cache_traces_to_the_wrapped_source(tmp_path: Path) -> None:
    """A cache presents its wrapped source's id (ITL-1425): the cell is the wrapped
    source's, and why_missing takes that id or the slot's label."""
    job, _ = aliased_job(tmp_path)
    node = job.nodes["SelectDataColumns"]
    trace = node.trace({"k": "a", "g": "1"}, "x")
    assert trace.status == "traced"
    assert [
        (c.node, c.source_id, c.record_id, c.value) for c in trace.source_cells()
    ] == [("inner", "dict_v1:inner", "row_0", 1)]
    for name in ("inner", "dict_v1:inner"):
        assert not node.why_missing(name, "row_0").missing


def test_no_root_without_a_snapshot_is_taken_to_hold_a_record(tmp_path: Path) -> None:
    job, _ = aliased_job(tmp_path)
    root = job.nodes["inner"].why_missing("inner", "row_999")
    assert root.status == "unavailable" and root.missing
    below = job.nodes["SelectDataColumns"].why_missing("inner", "row_999")
    assert below.missing and below.present_in == ()
    assert below.removed_by is None and "not known" in below.because


def test_no_user_code_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class Spied(DictSource):
        def output_schema(self, *args: Any, **kwargs: Any) -> Any:
            calls.append("output_schema")
            return super().output_schema(*args, **kwargs)

        def keys(self, *args: Any, **kwargs: Any) -> Any:
            calls.append("keys")
            return super().keys(*args, **kwargs)

    class SpiedFilter(Filter):
        def rows_dropped_text(self, *args: Any, **kwargs: Any) -> Any:
            calls.append("rows_dropped_text")
            return super().rows_dropped_text(*args, **kwargs)

    source = Spied(
        [{"k": "a", "g": "1", "x": 1, "y": 0}, {"k": "b", "g": "1", "x": 2, "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="spied",
    )
    job = PipelineJob(name="uc", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(SpiedFilter({"x": 1})(source))
    job.run()
    calls.clear()
    node = job.nodes["SelectDataColumns"]
    trace = node.trace({"k": "a", "g": "1"}, "x")
    # A subclass of a known operator may do anything: its link is not traced.
    assert trace.status == "unavailable" and trace.source_cells() == ()
    answer = node.why_missing("spied", "row_1")
    assert calls == []
    # A user-defined source is not read, so the step after it is not blamed.
    assert answer.missing and answer.removed_by is None

    # Over a library source the filter is blamed, without calling its user hook.
    plain = table_source(
        rows_kgxy(("a", "1", 1, 0), ("b", "1", 2, 0)), TWO_BY_TWO, ["k", "g"], "plain"
    )
    other = PipelineJob(name="uc2", database=InMemoryArrowDatabase())
    with other:
        SelectDataColumns(["x"])(SpiedFilter({"x": 1})(plain))
    other.run()
    calls.clear()
    blamed = other.nodes["SelectDataColumns"].why_missing("plain", "row_1")
    assert calls == []
    # A subclass of a known operator may do anything: not followed, not blamed.
    assert blamed.status == "unavailable" and blamed.removed_by is None


def test_two_versions_of_one_record_are_both_source_cells(tmp_path: Path) -> None:
    cache = DeltaTableDatabase(tmp_path / "cache")
    first = DictSource(
        [{"k": "a", "g": "1", "x": 1, "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="v",
    )
    CachedSource(first, cache_database=cache, cache_path=("shared",)).as_table()
    second = DictSource(
        [{"k": "a", "g": "1", "x": 2, "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="v",
    )
    cached = CachedSource(second, cache_database=cache, cache_path=("shared",))
    job = PipelineJob(name="vv", database=InMemoryArrowDatabase())
    with job:
        GroupBy(["g"])(SelectDataColumns(["x"])(cached))
    job.run()
    trace = job.nodes["GroupBy"].trace({"g": "1"}, "x")
    assert sorted(trace.root.value) == [1, 2]
    assert sorted(c.value for c in trace.source_cells()) == [1, 2]


def test_a_collapsed_edge_over_merged_paths_is_highlighted() -> None:
    """CodeRabbit thread on PR 530: S feeds selections A and B, both feed a Join J, J
    feeds F. Collapsed, the edge S -> F folds the steps of both paths."""
    source = table_source(rows_kgxy(("a", "1", 1, 2)), TWO_BY_TWO, ["k", "g"], "src")
    job = PipelineJob(name="cl", database=InMemoryArrowDatabase())
    with job:
        joined = Join()(
            SelectDataColumns(["x"])(source), SelectDataColumns(["y"])(source)
        )
        add.pod(joined, label="sums")
    job.run()
    trace = job.nodes["sums"].trace({"k": "a", "g": "1"}, "s")
    model = job.diagram(collapse=True, highlight=trace.highlight()).to_dict()
    labels = {n["index"]: n["label"] for n in model["nodes"]}
    lit = {
        (labels[e["source"]], labels[e["target"]])
        for e in model["edges"]
        if e["highlighted"]
    }
    assert ("src", "sums") in lit


class _CountingRows(list):  # pyright: ignore[reportMissingTypeArgument]  # a test probe over row dicts
    """A list of rows that counts every row a loop goes through."""

    seen = 0

    def __iter__(self):  # pyright: ignore[reportImplicitOverride]
        for item in list.__iter__(self):  # pyright: ignore[reportUnknownVariableType]
            _CountingRows.seen += 1
            yield item


def _rows_visited_tracing_a_batch(n: int, monkeypatch: pytest.MonkeyPatch) -> int:
    """Rows a trace of one batch of ``n`` function outputs goes through (held rows and
    stored entries, counted once per call each)."""
    from orcapod.pipeline import provenance

    rows = [(f"k{i}", "1", i, 1) for i in range(n)]
    source = table_source(rows_kgxy(*rows), TWO_BY_TWO, ["k", "g"], "src")
    job = PipelineJob(name=f"n{n}", database=InMemoryArrowDatabase())
    with job:
        Batch(batch_size=0)(
            add.pod(SelectDataColumns(["x", "y"])(source), label="sums")
        )
    job.run()
    (row,) = job.nodes["Batch"].as_table().to_pylist()

    def held(self: Any, node: Any) -> Any:
        key = id(node)
        if key not in self.rows:
            table = self.table(node)
            self.rows[key] = None if table is None else _CountingRows(table.to_pylist())
        return self.rows[key]

    real_provenance = provenance._Reader.provenance

    def stored(self: Any, node: Any) -> Any:
        key = id(node)
        if key not in self.stored:
            entries, by_id = real_provenance(self, node)
            self.stored[key] = (_CountingRows(entries), by_id)
        return self.stored[key]

    monkeypatch.setattr(provenance._Reader, "held", held)
    monkeypatch.setattr(provenance._Reader, "provenance", stored)
    _CountingRows.seen = 0
    trace = job.nodes["Batch"].trace({"g": row["g"]}, "s")
    assert len(trace.source_cells()) == 2 * n
    return _CountingRows.seen


def test_tracing_many_members_scales_linearly(monkeypatch: pytest.MonkeyPatch) -> None:
    """CodeRabbit thread on PR 530: each member's lookups (input rows, stored entries,
    source records) are indexed once per call, not scanned again per member. Work is
    counted (rows gone through), not timed."""
    n = 300
    visited = _rows_visited_tracing_a_batch(n, monkeypatch)
    # Each table is gone through a few times in all: linear. Scanning a table again
    # for each of the n members would go through about n * n rows.
    assert visited < 20 * n


# ---------------------------------------------------------------------------
# Third review (head bf2e471f)
# ---------------------------------------------------------------------------


class _Armed(DictSource):
    """A source whose own methods raise once armed: after the run, provenance must not
    call them (``output_schema``, ``keys``, the ``source_id`` property)."""

    armed = False

    def output_schema(self, *args: Any, **kwargs: Any) -> Any:
        if self.armed:
            raise RuntimeError("user output_schema")
        return super().output_schema(*args, **kwargs)

    def keys(self, *args: Any, **kwargs: Any) -> Any:
        if self.armed:
            raise RuntimeError("user keys")
        return super().keys(*args, **kwargs)

    @property
    def source_id(self) -> str:
        if self.armed:
            raise RuntimeError("user source_id")
        return super().source_id


def test_no_source_method_runs_after_the_run() -> None:
    source = _Armed(
        [{"k": "x", "i": 0, "x": 1, "y": 2}, {"k": "x", "i": 1, "x": 4, "y": 5}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="s",
    )
    job = PipelineJob(name="armed", database=InMemoryArrowDatabase())
    with job:
        selected = SelectDataColumns(["x"])(source)
        totals.pod(GroupBy(["k"])(selected), label="totals")
    job.run()
    source.armed = True
    node = job.nodes["SelectDataColumns"]
    trace = node.trace({"k": "x", "i": 0}, "x")
    assert [(c.source_id, c.record_id, c.value) for c in trace.source_cells()] == [
        ("dict_v1:s", "row_0", 1)
    ]
    assert not node.why_missing("s", "row_0").missing
    total = job.nodes["totals"].trace({"k": "x"}, "total")
    assert [c.value for c in total.source_cells()] == [1, 4]
    assert not job.nodes["totals"].why_missing("s", "row_1").missing


@pytest.mark.parametrize("cache_first", [True, False])
def test_two_inputs_carrying_one_id_are_told_apart_by_position(
    cache_first: bool,
) -> None:
    """A cache keeps the id of the source it wraps; at a MergeJoin of it and another
    source of one schema, each input is read at its own position."""
    inner = DictSource(
        [{"k": "x", "i": 0, "x": 1, "y": 2}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="s",
    )
    cache = CachedSource(inner, cache_database=InMemoryArrowDatabase(), label="cache")
    other = DictSource(
        [{"k": "x", "i": 0, "x": 101, "y": 0}, {"k": "x", "i": 0, "x": 102, "y": 0}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="other",
        label="plain",
    )
    job = PipelineJob(name="collide", database=InMemoryArrowDatabase())
    with job:
        MergeJoin()(cache, other) if cache_first else MergeJoin()(other, cache)
    job.run()
    node = job.nodes["MergeJoin"]
    value = [1, 101] if cache_first else [101, 1]
    trace = node.trace({"x": value}, "x")
    leaves = sorted((c.source_id, c.record_id, c.value) for c in trace.source_cells())
    assert leaves == [("dict_v1:other", "row_0", 101), ("dict_v1:s", "row_0", 1)]
    assert node.why_missing("s", "row_1").missing  # only the other source has row_1
    assert not node.why_missing("other", "row_1").missing
    assert not node.why_missing("s", "row_0").missing


def test_date_versions_of_one_record_are_both_source_cells(tmp_path: Path) -> None:
    from datetime import date

    cache = DeltaTableDatabase(tmp_path / "cache")
    first = DictSource(
        [{"k": "a", "g": "1", "d": date(2025, 1, 1), "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="v",
    )
    CachedSource(first, cache_database=cache, cache_path=("shared",)).as_table()
    second = DictSource(
        [{"k": "a", "g": "1", "d": date(2025, 1, 2), "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="v",
    )
    cached = CachedSource(second, cache_database=cache, cache_path=("shared",))
    job = PipelineJob(name="dates", database=InMemoryArrowDatabase())
    with job:
        GroupBy(["g"])(SelectDataColumns(["d"])(cached))
    job.run()
    trace = job.nodes["GroupBy"].trace({"g": "1"}, "d")
    assert sorted(c.value for c in trace.source_cells()) == [
        date(2025, 1, 1),
        date(2025, 1, 2),
    ]


def test_a_source_output_schema_is_not_called_through_a_schema_prediction() -> None:
    """Only ``output_schema`` armed: a node's columns come from what it holds, not
    from a schema prediction that re-enters the bound source."""

    class SchemaOnly(DictSource):
        armed = False

        def output_schema(self, *args: Any, **kwargs: Any) -> Any:
            if self.armed:
                raise RuntimeError("user output_schema")
            return super().output_schema(*args, **kwargs)

    source = SchemaOnly(
        [{"k": "x", "i": 0, "x": 1, "y": 2}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="s",
    )
    job = PipelineJob(name="schema", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(source)
    job.run()
    source.armed = True
    trace = job.nodes["SelectDataColumns"].trace({"k": "x", "i": 0}, "x")
    assert [c.value for c in trace.source_cells()] == [1]


def test_a_source_root_without_the_record_says_so() -> None:
    source = table_source(rows_kgxy(("a", "1", 1, 0)), TWO_BY_TWO, ["k", "g"], "src")
    job = PipelineJob(name="root", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(source)
    job.run()
    answer = job.nodes["src"].why_missing("src", "row_9")
    assert answer.missing and answer.status == "read"
    assert answer.because == "source 'src' holds no record 'row_9'"


# ---------------------------------------------------------------------------
# Fourth review (head 68a269ae)
# ---------------------------------------------------------------------------


def test_a_source_known_by_structure_feeds_a_node_with_no_rows_from_it() -> None:
    """A cache over source ``s``, then a Filter rejecting every row: no held row
    carries ``s``, yet ``s`` feeds the node (by its id or its slot's label)."""
    inner = DictSource(
        [{"k": "a", "g": "1", "x": 1, "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="s",
    )
    cache = CachedSource(inner, cache_database=InMemoryArrowDatabase())
    job = PipelineJob(name="noneleft", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(Filter({"x": 999})(cache))
    node = job.nodes["SelectDataColumns"]
    assert node.why_missing("s", "row_0").status == "not_computed"
    job.run()
    for name in ("s", "dict_v1:s"):
        answer = node.why_missing(name, "row_0")
        assert answer.status == "read" and answer.missing
        assert answer.removed_by is None and "not known" in answer.because


@pytest.mark.parametrize("left_first", [True, False])
def test_merge_join_elements_come_from_their_own_argument(left_first: bool) -> None:
    """Two caches keeping the same source id and record id: element i of a merged list
    is attributed only to argument i."""

    def cached(name: str, x: int, extra: bool) -> CachedSource:
        row: dict[str, Any] = {"k": "a", "i": 0, "x": x, "y": 0}
        if extra:
            row["z"] = 1
        inner = DictSource(
            [row], tag_columns=["k", "i"], tag_only_columns=True, source_id=name
        )
        return CachedSource(inner, cache_database=InMemoryArrowDatabase())

    left, right = cached("left", 1, False), cached("right", 100, True)
    job = PipelineJob(name="slots", database=InMemoryArrowDatabase())
    with job:
        MergeJoin()(left, right) if left_first else MergeJoin()(right, left)
    job.run()
    node = job.nodes["MergeJoin"]
    value = [1, 100] if left_first else [100, 1]
    trace = node.trace({"x": value}, "x")
    assert sorted((c.node, c.value) for c in trace.source_cells()) == [
        ("left", 1),
        ("right", 100),
    ]


def test_a_completed_run_with_no_rows_has_its_columns() -> None:
    source = table_source(rows_kgxy(("a", "1", 1, 2)), TWO_BY_TWO, ["k", "g"], "src")
    job = PipelineJob(name="empty", database=InMemoryArrowDatabase())
    with job:
        add.pod(Filter({"x": 999})(source), label="sums")
    job.run()
    empty = job.nodes["sums"].trace({"k": "a", "g": "1"}, "s")
    assert (empty.status, empty.matched, empty.roots) == ("no_match", 0, ())
    with pytest.raises(ValueError, match="not a data column"):
        job.nodes["sums"].trace({"k": "a", "g": "1"}, "nowhere")


# ---------------------------------------------------------------------------
# Fifth review (head f97048f6)
# ---------------------------------------------------------------------------


def _two_caches_of_one_record(name: str, left_first: bool, select: bool) -> PipelineJob:
    def cached(source_id: str, x: int, extra: bool) -> CachedSource:
        row: dict[str, Any] = {"k": "a", "i": 0, "x": x, "y": 0}
        if extra:
            row["z"] = 1
        inner = DictSource(
            [row], tag_columns=["k", "i"], tag_only_columns=True, source_id=source_id
        )
        return CachedSource(inner, cache_database=InMemoryArrowDatabase())

    left, right = cached("left", 1, False), cached("right", 100, True)
    job = PipelineJob(name=name, database=InMemoryArrowDatabase())
    with job:
        merged = MergeJoin()(left, right) if left_first else MergeJoin()(right, left)
        if select:
            merged = SelectDataColumns(["x"])(merged)
        Index("x", 0)(merged)
    job.run()
    return job


@pytest.mark.parametrize("select", [False, True])
@pytest.mark.parametrize("left_first", [True, False])
def test_a_selected_merged_element_traces_to_its_own_argument(
    left_first: bool, select: bool
) -> None:
    """Index after MergeJoin of two caches keeping one source and record id: the
    selected element keeps its position, through a select in between too."""
    job = _two_caches_of_one_record(f"ix{left_first}{select}", left_first, select)
    node = job.nodes["Index"]
    trace = node.trace({"k": "a", "i": 0}, "x")
    expected = ("left", 1) if left_first else ("right", 100)
    assert trace.root.value == expected[1]
    assert [(c.node, c.value) for c in trace.source_cells()] == [expected]


def test_an_empty_run_rejects_a_column_that_is_not_data() -> None:
    source = table_source(rows_kgxy(("a", "1", 1, 2)), TWO_BY_TWO, ["k", "g"], "src")
    job = PipelineJob(name="emptytag", database=InMemoryArrowDatabase())
    with job:
        add.pod(Filter({"x": 999})(source), label="sums")
    job.run()
    for column in ("k", "g"):
        with pytest.raises(ValueError, match="not a data column"):
            job.nodes["sums"].trace({"k": "a", "g": "1"}, column)
    empty = job.nodes["sums"].trace({"k": "a", "g": "1"}, "s")
    assert (empty.status, empty.matched, empty.roots) == ("no_match", 0, ())


# ---------------------------------------------------------------------------
# Sixth review (head dbb1f6fe): one path model
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("group_first", [True, False])
def test_a_folded_list_passing_through_merge_join_keeps_its_members(
    group_first: bool,
) -> None:
    """GroupBy builds x = [1, 4]; MergeJoin passes x through from that argument (the
    other input has only z and w); Index("x", 1) is member 1 of the group, whatever
    the argument order."""
    source = DictSource(
        [{"k": "a", "i": 0, "x": 1, "y": 0}, {"k": "a", "i": 1, "x": 4, "y": 0}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="s",
    )
    extra = DictSource(
        [{"k": "a", "z": 5, "w": 6}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="extra",
    )
    job = PipelineJob(name=f"gm{group_first}", database=InMemoryArrowDatabase())
    with job:
        grouped = GroupBy(["k"])(SelectDataColumns(["x"])(source))
        merged = (
            MergeJoin()(grouped, extra) if group_first else MergeJoin()(extra, grouped)
        )
        Index("x", 1)(merged)
    job.run()
    trace = job.nodes["Index"].trace({"k": "a"}, "x")
    assert trace.root.value == 4
    assert [(c.source_id, c.record_id, c.value) for c in trace.source_cells()] == [
        ("dict_v1:s", "row_1", 4)
    ]


def test_an_inner_list_of_nested_folds_traces_its_members() -> None:
    """Grouped twice, x = [[1, 4], [7, 10]]; Index("x", 1) selects the inner list
    [7, 10], whose members are rows 2 and 3."""
    source = DictSource(
        [
            {"a": "p", "b": "u", "c": 0, "x": 1},
            {"a": "p", "b": "u", "c": 1, "x": 4},
            {"a": "p", "b": "v", "c": 0, "x": 7},
            {"a": "p", "b": "v", "c": 1, "x": 10},
        ],
        tag_columns=["a", "b", "c"],
        tag_only_columns=True,
        source_id="s",
    )
    job = PipelineJob(name="nested", database=InMemoryArrowDatabase())
    with job:
        inner = GroupBy(["a", "b"])(source)
        Index("x", 1)(GroupBy(["a"])(inner))
    job.run()
    node = job.nodes["Index"]
    trace = node.trace({"a": "p"}, "x")
    assert trace.root.value == [7, 10]
    assert [(c.record_id, c.value) for c in trace.source_cells()] == [
        ("row_2", 7),
        ("row_3", 10),
    ]


@function_pod(output_keys="_total")
def underscored(x: int, y: int) -> int:
    return x + y


def test_an_underscore_data_column_of_an_empty_run_is_a_data_column() -> None:
    source = table_source(rows_kgxy(("a", "1", 1, 2)), TWO_BY_TWO, ["k", "g"], "src")
    job = PipelineJob(name="under", database=InMemoryArrowDatabase())
    with job:
        underscored.pod(Filter({"x": 999})(source), label="sums")
    job.run()
    empty = job.nodes["sums"].trace({"k": "a", "g": "1"}, "_total")
    assert (empty.status, empty.matched) == ("no_match", 0)
    with pytest.raises(ValueError, match="not a data column"):
        job.nodes["sums"].trace({"k": "a", "g": "1"}, "k")


@pytest.mark.parametrize("fold", ["batch", "group"])
def test_pick_after_index_after_a_fold_traces_one_member(fold: str) -> None:
    """A map column, folded, then Index("x", -1) and Pick("x", "a"): the path is a
    member position, then a key inside the source cell (an Arrow map reads as a list of
    key/value entries)."""
    from orcapod.operators import Pick

    rows = [
        {"k": "x", "i": 0, "x": {"a": 1}, "y": 0},
        {"k": "x", "i": 1, "x": {"a": 4}, "y": 0},
    ]
    schema = {"k": str, "i": int, "x": dict[str, int], "y": int}
    source = DictSource(
        rows,
        data_schema=schema,
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="input",
    )
    folding = Batch(batch_size=2) if fold == "batch" else GroupBy(["k"])
    job = PipelineJob(name=f"pick{fold}", database=InMemoryArrowDatabase())
    with job:
        Pick("x", "a")(Index("x", -1)(SelectDataColumns(["x"])(folding(source))))
    job.run()
    (row,) = job.nodes["Pick"].as_table().to_pylist()
    trace = job.nodes["Pick"].trace({"k": row["k"]}, "x")
    assert trace.root.value == 4
    assert [(c.record_id, c.value) for c in trace.source_cells()] == [("row_1", 4)]


# ---------------------------------------------------------------------------
# Seventh review (head e1c4f3fb): columns mapped by each operator's semantics
# ---------------------------------------------------------------------------


def test_a_column_named_like_an_access_path_is_not_an_input() -> None:
    """Index("x", 0, out="chosen") then a select of "chosen": the input is x only, not
    an unrelated column literally named "x[0]" holding the same value."""
    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("g", pa.large_string(), nullable=False),
            pa.field("x", pa.large_list(pa.int64()), nullable=False),
            pa.field("x[0]", pa.int64(), nullable=False),
        ]
    )
    source = table_source(
        [{"k": "a", "g": "1", "x": [1, 2], "x[0]": 1}], schema, ["k", "g"], "s"
    )
    job = PipelineJob(name="literal", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["chosen"])(Index("x", 0, out="chosen")(source))
    job.run()
    for label in ("Index", "SelectDataColumns"):
        trace = job.nodes[label].trace({"k": "a", "g": "1"}, "chosen")
        assert [(c.column, c.value) for c in trace.source_cells()] == [("x", 1)]


@pytest.mark.parametrize("left_first", [True, False])
def test_renamed_branches_of_one_source_are_told_apart(left_first: bool) -> None:
    """x renamed to "left" on one branch and "right" on another, then MergeJoin:
    "left" comes from its own branch only."""
    from orcapod.operators import MapData

    source = table_source(rows_kgxy(("a", "1", 1, 0)), TWO_BY_TWO, ["k", "g"], "s")
    job = PipelineJob(name=f"renamed{left_first}", database=InMemoryArrowDatabase())
    with job:
        left = MapData({"x": "left"})(SelectDataColumns(["x"])(source))
        right = MapData({"x": "right"})(SelectDataColumns(["x"])(source))
        MergeJoin()(left, right) if left_first else MergeJoin()(right, left)
    job.run()
    trace = job.nodes["MergeJoin"].trace({"k": "a", "g": "1"}, "left")
    # Only the branch renaming x to "left" is on the way.
    renames = {step.node for step in trace.root.walk() if step.operator == "MapData"}
    assert len(renames) == 1
    assert [(c.column, c.value) for c in trace.source_cells()] == [("x", 1)]


@pytest.mark.parametrize("a_first", [True, False])
def test_a_shared_data_tag_at_merge_join_traces_to_both_inputs(a_first: bool) -> None:
    """A data-tag k of both inputs is one key (a scalar with a merged token): it comes
    from both inputs' rows, as a tag join key does."""
    a = DictSource([{"k": "x", "p": 1}], tag_columns=["k"], source_id="a")
    b = DictSource([{"k": "x", "q": 2}], tag_columns=["k"], source_id="b")
    job = PipelineJob(name=f"key{a_first}", database=InMemoryArrowDatabase())
    with job:
        MergeJoin()(a, b) if a_first else MergeJoin()(b, a)
    job.run()
    trace = job.nodes["MergeJoin"].trace({"k": "x"}, "k")
    assert trace.status == "traced"
    assert [(c.source_id, c.record_id, c.value) for c in trace.source_cells()] == [
        ("dict_v1:a", "row_0", "x"),
        ("dict_v1:b", "row_0", "x"),
    ]


# ---------------------------------------------------------------------------
# Eighth review (head 6502920c)
# ---------------------------------------------------------------------------


def _xy_source(source_id: str = "a") -> DictSource:
    return DictSource(
        [{"k": "a", "x": 1, "y": 2}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id=source_id,
    )


@pytest.mark.parametrize(
    "mapper",
    [
        lambda: __import__("orcapod.operators", fromlist=["MapTags"]).MapTags(
            {"x": "y"}, strict=False
        ),
        lambda: __import__("orcapod.operators", fromlist=["MapData"]).MapData(
            {"ghost": "y"}, strict=False
        ),
        lambda: __import__("orcapod.operators", fromlist=["MapColumns"]).MapColumns(
            {"ghost": "y"}, strict=False
        ),
    ],
    ids=["MapTags", "MapData", "MapColumns"],
)
def test_a_non_strict_rename_that_applies_to_nothing_is_no_rename(mapper: Any) -> None:
    """A strict=False entry naming no column of its role is ignored by the mapper:
    x and y pass unchanged, and trace to x and y."""
    job = PipelineJob(name="nonstrict", database=InMemoryArrowDatabase())
    with job:
        mapper()(_xy_source())
    job.run()
    (label,) = [k for k in job.nodes if k.startswith("Map")]
    for column, value in (("x", 1), ("y", 2)):
        trace = job.nodes[label].trace({"k": "a"}, column)
        assert [(c.column, c.value) for c in trace.source_cells()] == [(column, value)]


def test_a_subclass_of_a_known_operator_is_not_traced() -> None:
    from orcapod.operators import Pick

    class OwnSelect(SelectDataColumns):
        pass

    class OwnPick(Pick):
        pass

    schema = {"k": str, "x": dict[str, int], "y": int}
    source = DictSource(
        [{"k": "a", "x": {"a": 7}, "y": 0}],
        data_schema=schema,
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="a",
    )
    job = PipelineJob(name="subclasses", database=InMemoryArrowDatabase())
    with job:
        OwnSelect(["y"])(source)
        OwnPick("x", "a", out="picked")(source)
    job.run()
    assert job.nodes["OwnSelect"].trace({"k": "a"}, "y").status == "unavailable"
    assert job.nodes["OwnPick"].trace({"k": "a"}, "picked").status == "unavailable"


def test_a_held_token_that_contradicts_the_mapping_raises() -> None:
    """A select maps x to x; a held row whose x token names y is refused."""
    source = table_source(rows_kgxy(("a", "1", 1, 1)), TWO_BY_TWO, ["k", "g"], "s")
    job = PipelineJob(name="contradict", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(source)
    job.run()
    node = job.nodes["SelectDataColumns"]
    table = node.as_table(columns={"system_tags": True, "source": True})
    changed = table.set_column(
        table.schema.get_field_index("_source_x"),
        "_source_x",
        pa.array(["s::row_0::y"], pa.large_string()),
    )
    vars(node)["_cached_output_stream"] = ArrowTableStream(
        changed, tag_columns=["k", "g"]
    )
    with pytest.raises(ProvenanceInconsistencyError, match="another column"):
        node.trace({"k": "a", "g": "1"}, "x")


_TIED_JOIN = """
import warnings
warnings.filterwarnings("ignore")
from orcapod import PipelineJob
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import Join
from orcapod.sources import DictSource
a = DictSource([{"k": "none", "i": 0}, {"k": "shared", "i": 1}], tag_columns=["k", "i"], source_id="a")
b = DictSource([{"k": "shared", "i": 1}], tag_columns=["k", "i"], source_id="b")
job = PipelineJob(name="tied", database=InMemoryArrowDatabase())
with job:
    Join()(b, a) if REVERSED else Join()(a, b)
job.run()
node = job.nodes["Join"]
trace = node.trace({"k": "shared", "i": 1}, "k")
print(sorted((c.source_id, c.record_id) for c in trace.source_cells()))
print(node.why_missing("a", "row_0").missing, node.why_missing("a", "row_1").missing)
print(trace.text())
"""


@pytest.mark.parametrize("reversed_args", [False, True])
def test_tied_join_inputs_are_resolved_by_value(reversed_args: bool) -> None:
    """Two sources of one schema (one pipeline hash) at a commutative Join: the held
    slots are canonical by value, not by argument order; each is resolved by the
    values it holds, in both argument orders and whatever the process's hash seed."""
    import os
    import subprocess
    import sys

    outputs = set()
    for seed in ("1", "2", "7"):
        result = subprocess.run(
            [sys.executable, "-c", _TIED_JOIN.replace("REVERSED", str(reversed_args))],
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONHASHSEED": seed},
            check=True,
        )
        cells, missing, _ = result.stdout.split("\n", 2)
        assert cells == "[('dict_v1:a', 'row_1'), ('dict_v1:b', 'row_0')]"
        assert missing == "True False"
        outputs.add(result.stdout)
    assert len(outputs) == 1  # the same answer and text for every seed


def test_a_commutative_join_lists_its_inputs_in_one_order() -> None:
    a = DictSource([{"k": "x", "p": 1}], tag_columns=["k"], source_id="a")
    b = DictSource([{"k": "x", "q": 2}], tag_columns=["k"], source_id="b")
    texts = []
    for swap in (False, True):
        job = PipelineJob(name=f"order{swap}", database=InMemoryArrowDatabase())
        with job:
            Join()(b, a) if swap else Join()(a, b)
        job.run()
        texts.append(job.nodes["Join"].trace({"k": "x"}, "k").text())
    assert texts[0] == texts[1]


# ---------------------------------------------------------------------------
# Ninth review (head beae55fb)
# ---------------------------------------------------------------------------


@function_pod(output_keys="length")
def key_length(k: str) -> int:
    return len(k)


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_stored_history_of_tied_bindings_with_one_id_is_unknown(mode: str) -> None:
    """Two bindings with one pipeline hash and one source id s at a Join: a stored
    entry naming s::row_0 cannot tell which binding it came through, so the stored
    answer for the binding whose row_0 the Join removed is unknown, not reached."""
    a = DictSource(
        [{"k": "unmatched", "i": 0}, {"k": "shared", "i": 1}],
        tag_columns=["k", "i"],
        source_id="s",
    )
    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("i", pa.int64(), nullable=False),
        ]
    )
    b = ArrowTableSource(
        pa.Table.from_pylist([{"k": "shared", "i": 1}], schema=schema),
        tag_columns=["k", "i"],
        source_id="t",
    )
    job = PipelineJob(name=f"hist{mode}", database=InMemoryArrowDatabase())
    with job:
        key_length.pod(Join()(a, b), label="length")
    run(job, mode)
    left = next(
        u.label
        for u in job.nodes["Join"].upstreams
        if type(u.bound_source) is DictSource
    )
    answer = job.nodes["length"].why_missing(left, "row_0")
    assert answer.missing
    assert answer.reached_function is None
    assert "reached function" not in answer.text()


def run(job: PipelineJob, mode: str) -> None:
    from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

    job.run(
        orchestrator=AsyncPipelineOrchestrator()
        if mode == "async"
        else SyncPipelineOrchestrator()
    )


@pytest.mark.parametrize("projection", [False, True])
def test_a_selection_token_that_contradicts_the_operator_raises(
    projection: bool,
) -> None:
    """Index("x", 0, out="picked") with x = y = [7]: a held token y[0] contradicts
    the configured input column x, directly over the source and through a projection."""
    source = DictSource(
        [{"k": "x", "i": 0, "x": [7], "y": [7]}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="a",
    )
    job = PipelineJob(name=f"tok{projection}", database=InMemoryArrowDatabase())
    with job:
        upstream = SelectDataColumns(["x", "y"])(source) if projection else source
        Index("x", 0, out="picked")(upstream)
    job.run()
    node = job.nodes["Index"]
    assert [
        c.column for c in node.trace({"k": "x", "i": 0}, "picked").source_cells()
    ] == ["x"]
    held = vars(node)["_cached_output_stream"]
    table = held.as_table(columns={"system_tags": True, "source": True})
    at = table.schema.get_field_index("_source_picked")
    changed = table.set_column(
        at,
        table.schema.field(at),
        pa.array(["a::row_0::y[0]"], table.schema.field(at).type),
    )
    vars(node)["_cached_output_stream"] = ArrowTableStream(
        changed, tag_columns=["k", "i"], data_context=held.data_context
    )
    with pytest.raises(ProvenanceInconsistencyError):
        node.trace({"k": "x", "i": 0}, "picked")


# ---------------------------------------------------------------------------
# Tenth review (head 9b278cbc)
# ---------------------------------------------------------------------------


@function_pod(output_keys="result")
def same_x(x: int) -> int:
    return x


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_semi_join_right_input_never_reaches_its_output(mode: str) -> None:
    """SemiJoin of two bindings with one source id and schema: the right input's rows
    only choose; asked by the right binding, no node past the SemiJoin holds its row,
    and the SemiJoin is named as the step that does not pass it on."""
    left = DictSource(
        [{"k": "x", "i": 0, "x": 7, "y": 9}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="same",
    )
    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("i", pa.int64(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
            pa.field("y", pa.int64(), nullable=False),
        ]
    )
    right = ArrowTableSource(
        pa.Table.from_pylist([{"k": "x", "i": 0, "x": 70, "y": 90}], schema=schema),
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="other",
    )
    job = PipelineJob(name=f"semi{mode}", database=InMemoryArrowDatabase())
    with job:
        same_x.pod(SelectDataColumns(["x"])(SemiJoin()(left, right)), label="result")
    run(job, mode)
    semi = job.nodes["SemiJoin"]
    right_label = semi.upstreams[1].label
    trace = semi.trace({}, "x")
    assert [c.value for c in trace.source_cells()] == [7]
    for label in ("SemiJoin", "result"):
        answer = job.nodes[label].why_missing(right_label, "row_0")
        assert answer.missing and answer.reached_function is not True
        assert answer.present_in == (right_label,)
        assert answer.removed_by is not None
        assert answer.removed_by.operator == "SemiJoin"
        assert "only choose" in answer.removed_by.text


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_an_input_row_must_keep_the_tags_the_step_keeps(
    mode: str, tmp_path: Path
) -> None:
    """A cache holding two versions of one record that differ only in a tag (equal data
    and tokens): the k='new' output row comes from the k='new' input row."""
    cache = InMemoryArrowDatabase()

    def data(key: str) -> DictSource:
        return DictSource(
            [{"k": key, "i": 0, "x": 7, "y": 9}],
            tag_columns=["k", "i"],
            tag_only_columns=True,
            source_id="same",
        )

    CachedSource(data("old"), cache_database=cache, cache_path=("shared",)).as_table()
    current = CachedSource(data("new"), cache_database=cache, cache_path=("shared",))
    job = PipelineJob(name=f"tagged{mode}", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(SelectDataColumns(["x", "y"])(current))
    run(job, mode)
    target = next(
        n
        for n in job.nodes.values()
        if n.label.startswith("SelectDataColumns")
        and n.upstreams[0].label.startswith("SelectDataColumns")
    )
    trace = target.trace({"k": "new", "i": 0}, "x")
    (step,) = trace.root.inputs
    assert dict(step.tags)["k"] == "new"


# ---------------------------------------------------------------------------
# Eleventh review (head 38f666d7)
# ---------------------------------------------------------------------------

FAILED_CALLS: list[int] = []


@function_pod(output_keys="result")
def fails_on_one(x: int) -> int:
    """Record each call, then raise for x == 1 (so that input stores no entry)."""
    FAILED_CALLS.append(x)
    if x == 1:
        raise ValueError("fails on the first record")
    return x


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_failed_call_is_not_reported_as_never_reached(mode: str) -> None:
    """The function ran on x=1 and raised (continue on error): no successful entry is
    stored for it, which does not prove the input never reached the function."""
    FAILED_CALLS.clear()
    source = DictSource(
        [{"k": "x", "i": 0, "x": 1}, {"k": "x", "i": 1, "x": 2}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="a",
    )
    job = PipelineJob(name=f"fails{mode}", database=InMemoryArrowDatabase())
    with job:
        fails_on_one.pod(source, label="may_fail")
    run(job, mode)
    assert sorted(FAILED_CALLS) == [1, 2]  # the function did receive x=1
    answer = job.nodes["may_fail"].why_missing("a", "row_0")
    assert answer.missing
    assert answer.reached_function is None
    assert answer.entry_recorded is False
    assert "never reached" not in answer.text()
    assert "no successful entry" in answer.text()


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_why_missing_does_not_follow_an_operator_subclass(mode: str) -> None:
    """A user subclass of SemiJoin (unchanged): why_missing asked by the right binding
    does not follow it by its system tags; the answer is unavailable."""

    class CustomSemiJoin(SemiJoin):
        pass

    left = DictSource(
        [{"k": "unmatched", "i": 1, "x": 7}, {"k": "shared", "i": 2, "x": 8}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="same",
    )
    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("i", pa.int64(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    )
    right = ArrowTableSource(
        pa.Table.from_pylist([{"k": "shared", "i": 2, "x": 90}], schema=schema),
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="other",
    )
    job = PipelineJob(name=f"customsemi{mode}", database=InMemoryArrowDatabase())
    with job:
        CustomSemiJoin()(left, right)
    run(job, mode)
    node = job.nodes["CustomSemiJoin"]
    assert node.trace({}, "x").status == "unavailable"
    answer = node.why_missing(node.upstreams[1].label, "row_1")
    assert answer.status == "unavailable"
    assert "reaches" not in answer.text()


# ---------------------------------------------------------------------------
# CodeRabbit review of 38f666d7
# ---------------------------------------------------------------------------


def _kg_source(source_id: str, rows: list[dict[str, Any]]) -> DictSource:
    """A source whose tags k and i are data-tags (the default role) and its only data."""
    return DictSource(rows, tag_columns=["k", "i"], source_id=source_id)


def test_many_tied_join_inputs_are_resolved_without_trying_every_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ten inputs of one schema at a Join: each fits one slot, found by a search that
    stops early, never by enumerating all 10! orders."""
    import itertools
    import time

    def no_enumeration(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("every order of the tied inputs was enumerated")

    monkeypatch.setattr(itertools, "permutations", no_enumeration)

    sources = [_kg_source(f"s{n:02d}", [{"k": "x", "i": 0}]) for n in range(10)]
    job = PipelineJob(name="tied10", database=InMemoryArrowDatabase())
    with job:
        Join()(*sources)
    job.run()
    start = time.perf_counter()
    trace = job.nodes["Join"].trace({"k": "x", "i": 0}, "k")
    assert time.perf_counter() - start < 20  # 10! orders would take minutes
    assert sorted(c.source_id for c in trace.source_cells()) == [
        f"dict_v1:s{n:02d}" for n in range(10)
    ]


def test_a_data_tag_falling_through_a_function_traces_to_its_input() -> None:
    source = DictSource(
        [{"k": "x", "i": 0, "x": 1, "y": 2}], tag_columns=["k", "i"], source_id="s"
    )
    job = PipelineJob(name="fallthrough", database=InMemoryArrowDatabase())
    with job:
        add.pod(source, label="sums")
    job.run()
    trace = job.nodes["sums"].trace({"k": "x", "i": 0}, "k")
    assert [
        (c.source_id, c.record_id, c.column, c.value) for c in trace.source_cells()
    ] == [("dict_v1:s", "row_0", "k", "x")]


def test_a_merged_key_token_is_verified() -> None:
    a = _kg_source("a", [{"k": "x", "i": 0}])
    b = _kg_source("b", [{"k": "x", "i": 0}])
    job = PipelineJob(name="mergedtoken", database=InMemoryArrowDatabase())
    with job:
        MergeJoin()(a, b)
    job.run()
    node = job.nodes["MergeJoin"]
    assert node.trace({"k": "x", "i": 0}, "k").status == "traced"
    held = vars(node)["_cached_output_stream"]
    table = held.as_table(columns={"system_tags": True, "source": True})
    at = table.schema.get_field_index("_source_k")
    changed = table.set_column(
        at,
        table.schema.field(at),
        pa.array(["a::row_0::k"], table.schema.field(at).type),
    )
    vars(node)["_cached_output_stream"] = ArrowTableStream(
        changed,
        tag_columns=["k", "i"],
        data_tags=["k", "i"],
        data_context=held.data_context,
    )
    with pytest.raises(ProvenanceInconsistencyError, match="fold"):
        node.trace({"k": "x", "i": 0}, "k")


def test_a_semi_join_right_edge_is_not_highlighted_as_row_flow() -> None:
    """One source feeds both SemiJoin inputs: its record reaches the SemiJoin through
    the left only; the right edge carries no row."""
    source = table_source(rows_kgxy(("a", "1", 1, 2)), TWO_BY_TWO, ["k", "g"], "s")
    job = PipelineJob(name="semiedge", database=InMemoryArrowDatabase())
    with job:
        SemiJoin()(SelectDataColumns(["x"])(source), SelectDataColumns(["y"])(source))
    job.run()
    answer = job.nodes["SemiJoin"].why_missing("s", "row_0")
    assert not answer.missing
    right = job.nodes["SemiJoin"].upstreams[1].label
    assert (right, "SemiJoin") not in answer.edges


@function_pod(output_keys="z")
def x_and_ys(x: int, y: list[int]) -> int:
    """Add one scalar x (the direct branch) to the grouped y values (the other branch)."""
    return x + sum(y)


def test_a_fold_witness_proves_receipt_despite_direct_names() -> None:
    """The function's input names s's records directly through one branch (row_1)
    and holds row_0 through a group on the other: row_0 reached it."""
    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("g", pa.large_string(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
            pa.field("y", pa.int64(), nullable=False),
        ]
    )
    source = table_source(
        [{"k": "a", "g": "1", "x": 1, "y": 10}, {"k": "b", "g": "1", "x": 2, "y": 20}],
        schema,
        ["k", "g"],
        "s",
    )
    job = PipelineJob(name="witness", database=InMemoryArrowDatabase())
    with job:
        direct = SelectDataColumns(["x"])(Filter({"k": "b"})(source))
        grouped = SelectDataColumns(["y"])(GroupBy(["g"])(source))
        x_and_ys.pod(Join()(direct, grouped), label="z")
    job.run()
    answer = job.nodes["z"].why_missing("s", "row_0")
    assert not answer.missing
    assert answer.reached_function is True and answer.entry_recorded is True


def test_a_function_after_an_unread_source_is_not_blamed(tmp_path: Path) -> None:
    cache = DeltaTableDatabase(tmp_path / "cache")
    inner = table_source(rows_kgxy(("a", "1", 1, 2)), TWO_BY_TWO, ["k", "g"], "s")
    cached = CachedSource(inner, cache_database=cache)
    job = PipelineJob(name="cachedfn", database=InMemoryArrowDatabase())
    with job:
        add.pod(cached, label="sums")
    job.run()
    answer = job.nodes["sums"].why_missing("s", "row_99")
    assert answer.missing and answer.removed_by is None
    assert "not known" in answer.because


def test_a_join_diagnostic_calls_no_source_method() -> None:
    a = _Armed(
        [{"k": "x", "i": 0, "x": 1, "y": 0}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="a",
    )
    b = DictSource(
        [{"k": "z", "i": 0, "p": 5}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="b",
    )
    job = PipelineJob(name="armedjoin", database=InMemoryArrowDatabase())
    with job:
        Join()(a, b)
    job.run()
    a.armed = True
    answer = job.nodes["Join"].why_missing("b", "row_0")
    assert answer.missing and answer.removed_by is not None


def test_an_unread_join_input_is_not_named_as_lacking_the_key(tmp_path: Path) -> None:
    """Three inputs: the first lacks the key, an unread cached input may hold it, the
    third holds the record. Only the first is named."""
    a = DictSource(
        [{"k": "B", "p": 1}], tag_columns=["k"], tag_only_columns=True, source_id="a"
    )
    cached = CachedSource(
        DictSource(
            [{"k": "A", "q": 2}],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="b",
        ),
        cache_database=DeltaTableDatabase(tmp_path / "cache"),
    )
    c = DictSource(
        [{"k": "A", "r": 3}], tag_columns=["k"], tag_only_columns=True, source_id="c"
    )
    job = PipelineJob(name="threejoin", database=InMemoryArrowDatabase())
    with job:
        Join()(a, cached, c)
    job.run()
    answer = job.nodes["Join"].why_missing("c", "row_0")
    assert answer.missing
    assert "'a'" in answer.because and "'bc'" not in answer.because


def test_an_unproven_operator_link_is_not_called_an_unread_source() -> None:
    class OwnSelect(SelectDataColumns):
        pass

    source = table_source(rows_kgxy(("a", "1", 1, 2)), TWO_BY_TWO, ["k", "g"], "s")
    job = PipelineJob(name="ownselect", database=InMemoryArrowDatabase())
    with job:
        OwnSelect(["x"])(source)
    job.run()
    text = job.nodes["OwnSelect"].trace({"k": "a", "g": "1"}, "x").text()
    assert "source's rows are not held" not in text
    assert "cannot be proven" in text


# ---------------------------------------------------------------------------
# Twelfth review (head c3c0d81d): the Join diagnostic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("order", ["acb", "abc", "bac", "cab"])
@pytest.mark.parametrize("mode", ["sync", "async"])
def test_an_unread_join_input_is_unknown_never_a_miss(order: str, mode: str) -> None:
    """a's record is removed by a Join: c's held rows prove the key absent; b, an
    unread cache that does hold the key, is never named, wherever it stands."""
    a = DictSource(
        [{"k": "wanted", "i": 0, "x": 7}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="a",
    )
    b = CachedSource(
        DictSource(
            [{"k": "wanted", "z": 4}],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="b",
        ),
        cache_database=InMemoryArrowDatabase(),
    )
    c = DictSource(
        [{"k": "other", "w": 5}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="c",
    )
    inputs = {"a": a, "b": b, "c": c}
    job = PipelineJob(name=f"unread{order}{mode}", database=InMemoryArrowDatabase())
    with job:
        Join()(*(inputs[name] for name in order))
    run(job, mode)
    answer = job.nodes["Join"].why_missing("a", "row_0")
    assert answer.missing
    assert "in 'c'" in answer.because
    assert "in 'b'" not in answer.because


def test_an_unread_join_input_alone_is_named_as_not_read() -> None:
    """No held input proves the key missing: the unread input is named as not read."""
    a = DictSource(
        [{"k": "wanted", "i": 0, "x": 7}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="a",
    )
    b = CachedSource(
        DictSource(
            [{"k": "other", "z": 4}],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="b",
        ),
        cache_database=InMemoryArrowDatabase(),
    )
    job = PipelineJob(name="unreadalone", database=InMemoryArrowDatabase())
    with job:
        Join()(a, b)
    job.run()
    answer = job.nodes["Join"].why_missing("a", "row_0")
    assert answer.missing and "not read here" in answer.because


def test_every_join_input_proving_the_miss_is_named() -> None:
    a = DictSource(
        [{"k": "wanted", "i": 0, "x": 7}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="a",
    )
    c = DictSource(
        [{"k": "other", "w": 5}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="c",
    )
    d = DictSource(
        [{"k": "other", "v": 6}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="d",
    )
    job = PipelineJob(name="twoprove", database=InMemoryArrowDatabase())
    with job:
        Join()(a, c, d)
    job.run()
    because = job.nodes["Join"].why_missing("a", "row_0").because
    assert "in 'c'" in because and "in 'd'" in because


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_the_join_diagnostic_calls_no_method_of_the_other_source(mode: str) -> None:
    class Armed(DictSource):
        armed = False

        def output_schema(self, *args: Any, **kwargs: Any) -> Any:
            if self.armed:
                raise RuntimeError("why_missing called the source's schema")
            return super().output_schema(*args, **kwargs)

        def keys(self, *args: Any, **kwargs: Any) -> Any:
            if self.armed:
                raise RuntimeError("why_missing called the source's keys")
            return super().keys(*args, **kwargs)

    a = DictSource(
        [{"k": "wanted", "i": 0, "x": 7}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="a",
    )
    b = Armed(
        [{"k": "other", "z": 4}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="b",
    )
    job = PipelineJob(name=f"armedright{mode}", database=InMemoryArrowDatabase())
    with job:
        Join()(a, b)
    run(job, mode)
    b.armed = True
    answer = job.nodes["Join"].why_missing("a", "row_0")
    assert answer.missing and answer.removed_by is not None


# ---------------------------------------------------------------------------
# Thirteenth review (head c41f88d3)
# ---------------------------------------------------------------------------


@function_pod(output_keys="length")
def k_length(k: str) -> int:
    return len(k)


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_stored_witness_must_be_the_held_input(mode: str) -> None:
    """Two jobs of one name over two bindings at a tied Join, with record ids swapped
    between the jobs: the first job's entry is no witness for the second job's row.
    (Since ITL-1425 two different sources cannot share one source id in a job, so the
    bindings carry two ids; the input-hash check itself is
    test_an_untied_witness_must_match_the_stored_input_hash.)"""
    database = InMemoryArrowDatabase()
    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("i", pa.int64(), nullable=False),
            pa.field("rid", pa.int64(), nullable=False),
        ]
    )

    def make(current: bool) -> tuple[Any, Any]:
        key = "shared-new" if current else "shared-old"
        id_a, id_b = (0, 1) if current else (1, 0)
        a = DictSource(
            [{"k": key, "i": 0, "rid": id_a}],
            tag_columns=["k", "i"],
            source_id="s",
            record_id_columns=["rid"],
        )
        b = ArrowTableSource(
            pa.Table.from_pylist([{"k": key, "i": 0, "rid": id_b}], schema=schema),
            tag_columns=["k", "i"],
            source_id="t",
            record_id_columns=["rid"],
        )
        job = PipelineJob(name="history", database=database)
        with job:
            k_length.pod(
                Join()(
                    SelectDataColumns(["k", "i"])(a), SelectDataColumns(["k", "i"])(b)
                ),
                label="length",
            )
        from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

        orchestrator = (
            AsyncPipelineOrchestrator()
            if mode == "async"
            else SyncPipelineOrchestrator()
        )
        job.run(orchestrator=orchestrator, load_only=current)
        binding = next(
            u.upstreams[0]
            for u in job.nodes["Join"].upstreams
            if type(u.upstreams[0].bound_source) is DictSource
        )
        return binding, job.nodes["length"]

    make(False)
    binding, node = make(True)
    answer = node.why_missing(binding.label, '{"rid":0}')
    # Not proven by the other job's entry (no stored entry names this binding's row).
    assert answer.reached_function is None
    assert answer.entry_recorded is not True


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_rejoin_keeps_the_common_token_of_a_shared_column(mode: str) -> None:
    source = DictSource(
        [{"k": "x", "i": 0, "x": 1, "y": 2}], tag_columns=["k", "i"], source_id="a"
    )
    from orcapod.operators import Rejoin

    job = PipelineJob(name=f"rejoin{mode}", database=InMemoryArrowDatabase())
    with job:
        Rejoin()(
            SelectDataColumns(["k", "x"])(source), SelectDataColumns(["k", "y"])(source)
        )
    run(job, mode)
    trace = job.nodes["Rejoin"].trace({}, "k")
    assert trace.status == "traced"
    assert [(c.source_id, c.record_id, c.column) for c in trace.source_cells()] == [
        ("dict_v1:a", "row_0", "k")
    ]


def test_a_falling_through_data_tag_token_must_name_its_own_column() -> None:
    """A data-tag i falling through a function over the source directly: a held
    token naming q (same value) is refused."""
    source = DictSource(
        [{"k": "x", "i": 7, "q": 7}], tag_columns=["k", "i", "q"], source_id="a"
    )
    job = PipelineJob(name="ownname", database=InMemoryArrowDatabase())
    with job:
        k_length.pod(source, label="length")
    job.run()
    node = job.nodes["length"]
    assert [c.column for c in node.trace({}, "i").source_cells()] == ["i"]
    for key, (tag, data) in list(node._cached_output_datas.items()):
        if data is not None:
            node._cached_output_datas[key] = (
                tag,
                data.with_source_info(i="a::row_0::q"),
            )
    node._cached_output_table = None
    with pytest.raises(ProvenanceInconsistencyError, match="another column"):
        node.trace({}, "i")


# ---------------------------------------------------------------------------
# Fourteenth review (head 66193439): why_missing explanations
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_semi_join_right_record_is_explained_by_row_flow_alone(mode: str) -> None:
    left = DictSource(
        [{"k": "retain", "x": 1}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="left",
    )
    right = DictSource(
        [{"k": "retain", "y": 2}, {"k": "absent", "y": 3}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="right",
    )
    job = PipelineJob(name=f"semiright{mode}", database=InMemoryArrowDatabase())
    with job:
        SemiJoin()(left, right)
    run(job, mode)
    for record in ("row_0", "row_1"):
        answer = job.nodes["SemiJoin"].why_missing("right", record)
        assert answer.missing
        assert (
            answer.because == "its rows only choose rows of 'left' and do not flow on"
        )


@pytest.mark.parametrize("first,second", [("old", "new"), ("new", "old")])
@pytest.mark.parametrize("mode", ["sync", "async"])
def test_every_held_version_of_a_record_gets_its_reason(
    mode: str, first: str, second: str
) -> None:
    """A cache holds two versions of s's row_0; b matches only old, c only new. Both
    versions' reasons are given, in one order whichever was cached first."""
    cache = InMemoryArrowDatabase()

    def data(key: str) -> DictSource:
        return DictSource(
            [{"k": key, "x": 1}],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="s",
        )

    CachedSource(data(first), cache_database=cache, cache_path=("shared",)).as_table()
    a = CachedSource(data(second), cache_database=cache, cache_path=("shared",))
    b = DictSource(
        [{"k": "old", "y": 2}], tag_columns=["k"], tag_only_columns=True, source_id="b"
    )
    c = DictSource(
        [{"k": "new", "z": 3}], tag_columns=["k"], tag_only_columns=True, source_id="c"
    )
    job = PipelineJob(name=f"versions{mode}{first}", database=InMemoryArrowDatabase())
    with job:
        Join()(SelectDataColumns(["x"])(a), b, c)
    run(job, mode)
    answer = job.nodes["Join"].why_missing("s", "row_0")
    assert answer.missing
    assert answer.because == (
        "no row with k='new' in 'b' (reads b); no row with k='old' in 'c' (reads c)"
    )


@function_pod(output_keys="length")
def null_key_length(k: str | None) -> int:
    """The length of k, or 0 for a null."""
    return len(k or "")


def test_a_null_join_key_matches_nothing_in_the_explanation() -> None:
    """Both inputs hold k = null: the Join drops the row (a null key matches nothing),
    and the reason says so: the null key, never the other input passed over as a
    match (nor said to lack a row it holds)."""
    schema = pa.schema(
        [pa.field("k", pa.large_string(), nullable=True), pa.field("x", pa.int64())]
    )
    other = pa.schema(
        [pa.field("k", pa.large_string(), nullable=True), pa.field("y", pa.int64())]
    )
    a = ArrowTableSource(
        pa.Table.from_pylist([{"k": None, "x": 1}], schema=schema),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="a",
    )
    b = ArrowTableSource(
        pa.Table.from_pylist([{"k": None, "y": 2}], schema=other),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="b",
    )
    job = PipelineJob(name="nullkey", database=InMemoryArrowDatabase())
    with job:
        Join()(a, b)
    job.run()
    assert job.nodes["Join"].as_table().num_rows == 0
    answer = job.nodes["Join"].why_missing("a", "row_0")
    assert answer.missing
    assert (
        answer.because == "its key k is null, and a null key matches nothing in a join"
    )


@pytest.mark.parametrize("same_input", [True, False])
def test_an_untied_witness_must_match_the_stored_input_hash(same_input: bool) -> None:
    """No tied Join: the witness check runs. A load-only job whose group holds s's
    row_0 with the system tags of a stored entry proves receipt only when the input
    data hash is the stored one too."""
    database = InMemoryArrowDatabase()
    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("g", pa.large_string(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
            pa.field("y", pa.int64(), nullable=False),
        ]
    )

    def make(y: int, load_only: bool) -> Any:
        source = table_source(
            [
                {"k": "a", "g": "1", "x": 1, "y": y},
                {"k": "b", "g": "1", "x": 2, "y": 20},
            ],
            schema,
            ["k", "g"],
            "s",
        )
        job = PipelineJob(name="untiedwitness", database=database)
        with job:
            direct = SelectDataColumns(["x"])(Filter({"k": "b"})(source))
            grouped = SelectDataColumns(["y"])(GroupBy(["g"])(source))
            x_and_ys.pod(Join()(direct, grouped), label="z")
        job.run(load_only=load_only)
        return job.nodes["z"]

    make(10, load_only=False)
    node = make(10 if same_input else 11, load_only=True)
    answer = node.why_missing("s", "row_0")
    if same_input:
        assert answer.reached_function is True and answer.entry_recorded is True
    else:
        assert answer.reached_function is None


# ---------------------------------------------------------------------------
# Developer feedback on the trace picture: computed or reused on the diagram
# ---------------------------------------------------------------------------


@function_pod(output_keys="score")
def hit_rate(hits: int, misses: int) -> float:
    """The fraction of hits; a1 trial 1 and a2 trial 1 are equal inputs (one run)."""
    return hits / (hits + misses)


@function_pod(output_keys="mean")
def mean_score(score: list[float]) -> float:
    """The mean of an animal's scores."""
    return sum(score) / len(score)


TRIALS = [
    {"animal": "a1", "trial": 1, "hits": 8, "misses": 2},
    {"animal": "a1", "trial": 2, "hits": 6, "misses": 4},
    {"animal": "a2", "trial": 1, "hits": 8, "misses": 2},
    {"animal": "a2", "trial": 2, "hits": 3, "misses": 7},
]


def _trials_job(database: Any, rows: list[dict[str, Any]] = TRIALS) -> PipelineJob:
    trials = DictSource(
        rows,
        tag_columns=["animal", "trial"],
        tag_only_columns=True,
        source_id="trials",
    )
    job = PipelineJob(name="marks", database=database)
    with job:
        scores = hit_rate.pod(trials, label="scores")
        mean_score.pod(
            SelectDataColumns(["score"])(GroupBy(["animal"])(scores)), label="means"
        )
    return job


def _marks(job: PipelineJob) -> dict[str, dict[str, str]]:
    scores, means = job.nodes["scores"], job.nodes["means"]
    which = {
        "a1 trial 1": scores.trace({"animal": "a1", "trial": 1}, "score"),
        "a2 trial 1": scores.trace({"animal": "a2", "trial": 1}, "score"),
        "a2": means.trace({"animal": "a2"}, "mean"),
    }
    return {name: dict(t.highlight().marks) for name, t in which.items()}


EXPECTED_MARKS = {
    "a1 trial 1": {"scores": "computed"},
    "a2 trial 1": {"scores": "reused"},
    "a2": {"scores": "computed + reused", "means": "computed"},
}


def _run_after_a1(database: Any, mode: str) -> PipelineJob:
    """A first run stores a1's scores; the job over every trial then runs in ``mode``,
    so a2 trial 1 (a1 trial 1's input) reuses a stored result on either orchestrator
    (the async one runs equal inputs of one run concurrently: both compute)."""
    _ = _trials_job(database, TRIALS[:2]).run()
    job = _trials_job(database)
    run(job, mode)
    return job


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_trace_highlight_marks_each_function_node(mode: str) -> None:
    assert _marks(_run_after_a1(InMemoryArrowDatabase(), mode)) == EXPECTED_MARKS


def test_equal_inputs_computed_at_once_are_both_marked_computed() -> None:
    """In one async run equal inputs are computed concurrently, and the mark says so."""
    job = _trials_job(InMemoryArrowDatabase())
    run(job, "async")
    assert _marks(job)["a2 trial 1"] == {"scores": "computed"}


def test_a_trace_highlight_marks_on_a_loaded_job(tmp_path: Path) -> None:
    job = _run_after_a1(DeltaTableDatabase(tmp_path / "store"), "sync")
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(
        tmp_path / "job.json", database=DeltaTableDatabase(tmp_path / "store")
    )
    loaded.bind(
        sources={
            "trials": DictSource(
                TRIALS,
                tag_columns=["animal", "trial"],
                tag_only_columns=True,
                source_id="trials",
            )
        }
    )
    loaded.run(load_only=True)
    assert _marks(loaded) == EXPECTED_MARKS


def test_the_diagram_shows_the_marks_and_the_caption() -> None:
    job = _trials_job(InMemoryArrowDatabase())
    job.run()
    trace = job.nodes["means"].trace({"animal": "a2"}, "mean")
    diagram = job.diagram(highlight=trace.highlight())
    listing = str(diagram)
    assert " * scores (function pod · hit_rate) [computed + reused]" in listing
    assert (
        ">* means (function pod · mean_score) [computed] [traced from here]" in listing
    )
    svg = diagram.to_svg()
    assert '<text class="op-dag-caption"' in svg
    assert (
        "highlighted: trace of mean at node means, for animal=&#x27;a2&#x27;: "
        "1 row matches"
    ) in svg
    assert 'class="op-dag-mark">computed + reused</text>' in svg
    assert 'class="op-dag-mark">computed</text>' in svg
    model = diagram.to_dict()
    assert {n["label"]: n.get("mark") for n in model["nodes"]}["scores"] == (
        "computed + reused"
    )


def test_a_mark_on_an_unknown_node_is_refused() -> None:
    from orcapod.types import Highlight

    job = _trials_job(InMemoryArrowDatabase())
    with pytest.raises(ValueError, match="nowhere"):
        job.diagram(highlight=Highlight(marks=frozenset({("nowhere", "computed")})))


def test_the_highlight_colour_is_no_status_or_kind_colour() -> None:
    """Highlighted must not read as failed or stale: the highlight is drawn in the text
    colour, which no status and no kind uses."""
    from orcapod.pipeline import diagram

    colour = diagram._HIGHLIGHT_COLOUR
    assert colour == "currentColor"
    assert colour not in diagram._STATUS_COLOUR.values()
    assert colour not in diagram._KIND_COLOUR.values()
    job = _trials_job(InMemoryArrowDatabase())
    job.run()
    svg = job.diagram(
        highlight=job.nodes["scores"]
        .trace({"animal": "a1", "trial": 1}, "score")
        .highlight()
    ).to_svg()
    for status_colour in {"#d63c3c", "#d08a16"}:
        assert f'stroke="{status_colour}" stroke-width="3.2"' not in svg
    assert 'class="op-dag-highlight" stroke="currentColor"' in svg


# ---------------------------------------------------------------------------
# Developer feedback on 85448829: the asked node, no status, written-out questions,
# several rows
# ---------------------------------------------------------------------------


def _several(job: PipelineJob) -> dict[str, Any]:
    scores = job.nodes["scores"]
    two = scores.trace({"animal": "a2"}, "score")
    every = scores.trace({}, "score")
    none = scores.trace({"animal": "a9"}, "score")
    return {
        "two": (two.matched, [dict(r.tags) for r in two.roots], two.header()),
        "two marks": dict(two.highlight().marks),
        "two cells": [c.text() for c in two.source_cells()],
        "every": (every.matched, len(every.roots), every.header()),
        "none": (none.status, none.matched, none.text()),
    }


EXPECTED_SEVERAL = {
    "two": (
        2,
        [{"animal": "a2", "trial": 1}, {"animal": "a2", "trial": 2}],
        "trace of score at node scores, for animal='a2': 2 rows match",
    ),
    "two marks": {"scores": "computed + reused"},
    "every": (4, 4, "trace of score at node scores, for every row: 4 rows match"),
    "none": (
        "no_match",
        0,
        "trace of score at node scores, for animal='a9': no held row matches",
    ),
}


def _check_several(found: dict[str, Any]) -> None:
    assert found["two"] == EXPECTED_SEVERAL["two"]
    assert found["two marks"] == EXPECTED_SEVERAL["two marks"]
    # Both rows' source cells, from the two records of a2 (rows 2 and 3).
    assert found["two cells"] == [
        "dict_v1:trials row_2 hits = 8",
        "dict_v1:trials row_2 misses = 2",
        "dict_v1:trials row_3 hits = 3",
        "dict_v1:trials row_3 misses = 7",
    ]
    assert found["every"] == EXPECTED_SEVERAL["every"]
    assert found["none"] == EXPECTED_SEVERAL["none"]


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_selector_traces_every_matching_row(mode: str) -> None:
    _check_several(_several(_run_after_a1(InMemoryArrowDatabase(), mode)))


def test_several_rows_on_a_loaded_job(tmp_path: Path) -> None:
    job = _run_after_a1(DeltaTableDatabase(tmp_path / "store"), "sync")
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(
        tmp_path / "job.json", database=DeltaTableDatabase(tmp_path / "store")
    )
    loaded.bind(
        sources={
            "trials": DictSource(
                TRIALS,
                tag_columns=["animal", "trial"],
                tag_only_columns=True,
                source_id="trials",
            )
        }
    )
    loaded.run(load_only=True)
    _check_several(_several(loaded))


def test_several_rows_text_and_union_highlight() -> None:
    job = _run_after_a1(InMemoryArrowDatabase(), "sync")
    means = job.nodes["means"].trace({}, "mean")
    lines = means.text().splitlines()
    assert lines[0] == "trace of mean at node means, for every row: 2 rows match"
    # Each matching row's tree follows, in tag order.
    roots = [line for line in lines if line.startswith("means (")]
    assert roots == [
        line for line in roots if "animal='a1'" in line or "animal='a2'" in line
    ]
    assert [r.tags for r in means.roots] == [(("animal", "a1"),), (("animal", "a2"),)]
    highlight = means.highlight()
    assert highlight.nodes == {
        "trials",
        "scores",
        "GroupBy",
        "SelectDataColumns",
        "means",
    }
    # a1's rows computed, a2 trial 1 reused: merged on the node.
    assert dict(highlight.marks) == {
        "scores": "computed + reused",
        "means": "computed",
    }
    with pytest.raises(ValueError, match="2 rows match: read roots"):
        _ = means.root
    with pytest.raises(ValueError, match="no column"):
        job.nodes["means"].trace({"nowhere": 1}, "mean")


def test_the_asked_node_is_marked_apart_from_the_path() -> None:
    job = _run_after_a1(InMemoryArrowDatabase(), "sync")
    # A trace asked at scores: the node is on the path and is where it was asked.
    trace = job.nodes["scores"].trace({"animal": "a1", "trial": 1}, "score")
    diagram = job.diagram(highlight=trace.highlight())
    listing = str(diagram)
    assert (
        ">* scores (function pod · hit_rate) [computed] [traced from here]" in listing
    )
    assert " * trials (source · DictSource)" in listing
    assert "   means (function pod · mean_score)" in listing  # not on the path
    svg = diagram.to_svg()
    assert svg.count('class="op-dag-asked"') == 1
    assert 'class="op-dag-mark">traced from here</text>' in svg
    assert {n["label"]: n.get("asked") for n in diagram.to_dict()["nodes"]} == {
        "trials": None,
        "scores": "traced from here",
        "GroupBy": None,
        "SelectDataColumns": None,
        "means": None,
    }
    # why_missing at a node the record does not reach: asked here, not on the path,
    # and not dimmed.
    filtered = DictSource(
        TRIALS, tag_columns=["animal", "trial"], tag_only_columns=True, source_id="t"
    )
    other = PipelineJob(name="asked", database=InMemoryArrowDatabase())
    with other:
        hit_rate.pod(Filter({"animal": "a1"})(filtered), label="scores")
    other.run()
    answer = other.nodes["scores"].why_missing("t", "row_2")
    assert answer.text().splitlines()[0] == (
        "why record row_2 of t is missing at node scores"
    )
    shown = other.diagram(highlight=answer.highlight())
    listing = str(shown)
    assert listing.splitlines()[1].startswith(
        "  highlight (*): why record row_2 of t is missing at node scores: "
    )
    assert ">  scores (function pod · hit_rate) [asked here] <- Filter" in listing
    svg = shown.to_svg()
    assert 'class="op-dag-mark">asked here</text>' in svg
    asked = svg[svg.index('aria-label="scores') - 200 : svg.index('aria-label="scores')]
    assert 'opacity="0.4' not in asked


def test_a_highlight_shows_no_status() -> None:
    job = _run_after_a1(InMemoryArrowDatabase(), "sync")
    plain = str(job.diagram())
    assert "current" in plain and "bound" in plain  # the plain diagram keeps them
    highlight = job.nodes["means"].trace({"animal": "a1"}, "mean").highlight()
    shown = job.diagram(highlight=highlight)
    import re

    shown_text = " ".join(re.findall(r">([^<>]+)<", shown.to_svg()))  # drawn text
    for word in ("current", "bound", "stale", "not run", "failed", "partial"):
        assert word not in shown.to_text()
        assert word not in shown_text
    assert "<circle" not in shown.to_svg()  # no status dot


# ---------------------------------------------------------------------------
# Review of 37cc4100: operator equality in the reason, collapse, NaN order, dates
# ---------------------------------------------------------------------------


def _null_key_peer(name: str) -> ArrowTableSource:
    """A source of one row whose tag k is null."""
    table = pa.Table.from_pylist(
        [{"k": None, "x": 1, "y": 2}],
        schema=pa.schema(
            [
                pa.field("k", pa.large_string()),
                pa.field("x", pa.int64()),
                pa.field("y", pa.int64()),
            ]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["k"], tag_only_columns=True, source_id=name
    )


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_the_reason_follows_each_operators_equality(mode: str) -> None:
    """Join: a null key matches nothing (the peer is not said to lack a row). Rejoin
    matches the nulls (its rows differ by origin), so no input is said to lack k."""
    from orcapod.operators import Rejoin

    for operator in (Join, Rejoin):
        left, right = _null_key_peer("left"), _null_key_peer("right")
        job = PipelineJob(
            name=f"null{operator.__name__}{mode}", database=InMemoryArrowDatabase()
        )
        with job:
            operator()(SelectDataColumns(["x"])(left), SelectDataColumns(["y"])(right))
        run(job, mode)
        node = job.nodes[operator.__name__]
        answer = node.why_missing("left", "row_0")
        assert answer.missing
        assert "no row with" not in answer.because
        if operator is Join:
            assert answer.because == (
                "its key k is null, and a null key matches nothing in a join"
            )


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_collapsed_diagram_keeps_the_asked_node(mode: str) -> None:
    from orcapod.operators import MapData

    source = DictSource(
        [{"k": "one", "i": 1, "x": 7}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="a",
    )
    job = PipelineJob(name=f"collapse{mode}", database=InMemoryArrowDatabase())
    with job:
        MapData({"x": "y"})(SelectDataColumns(["x"])(source))
    run(job, mode)
    target = job.nodes["SelectDataColumns"]
    # Without a highlight, collapse folds the selection.
    assert "SelectDataColumns" not in job.diagram(collapse=True).to_text()
    for selector in ({"k": "one"}, {"k": "absent"}):
        diagram = job.diagram(
            collapse=True, highlight=target.trace(selector, "x").highlight()
        )
        assert "SelectDataColumns (operator) [traced from here]" in diagram.to_text()
        assert diagram.to_svg().count('class="op-dag-asked"') == 1


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_roots_follow_a_total_order_with_nan(mode: str) -> None:
    orders = []
    for held in (
        (float("nan"), 2.0, 1.0),
        (2.0, float("nan"), 1.0),
        (1.0, 2.0, float("nan")),
    ):
        source = DictSource(
            [{"key": v, "value": 0} for v in held],
            tag_columns=["key"],
            tag_only_columns=True,
            source_id="numbers",
        )
        job = PipelineJob(name=f"nan{mode}", database=InMemoryArrowDatabase())
        with job:
            SelectDataColumns(["value"])(source)
        run(job, mode)
        keys = [
            dict(r.tags)["key"]
            for r in job.nodes["SelectDataColumns"].trace({}, "value").roots
        ]
        orders.append([("nan" if k != k else k) for k in keys])
    assert orders == [[1.0, 2.0, "nan"]] * 3


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_question_shows_a_date_selector_value(mode: str) -> None:
    from datetime import date

    from orcapod.types.provenance import _value_text

    source = DictSource(
        [
            {"when": date(2026, 1, 1), "value": 1},
            {"when": date(2026, 1, 2), "value": 2},
        ],
        tag_columns=["when"],
        tag_only_columns=True,
        source_id="dates",
    )
    job = PipelineJob(name=f"dates{mode}", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["value"])(source)
    run(job, mode)
    node = job.nodes["SelectDataColumns"]
    headers = [
        node.trace({"when": d}, "value").header()
        for d in (date(2026, 1, 1), date(2026, 1, 2))
    ]
    assert headers == [
        "trace of value at node SelectDataColumns, for when=date('2026-01-01'): 1 row matches",
        "trace of value at node SelectDataColumns, for when=date('2026-01-02'): 1 row matches",
    ]
    # Other supported types read by type and exact text, never as one shared <type>.
    from datetime import datetime, time, timedelta, timezone
    from decimal import Decimal
    from uuid import UUID

    assert _value_text(datetime(2026, 1, 1, 12, tzinfo=timezone.utc)) == (
        "datetime('2026-01-01T12:00:00+00:00')"
    )
    assert _value_text(time(9, 30)) == "time('09:30:00')"
    assert _value_text(timedelta(days=1, seconds=2)) == "timedelta(1d 2s 0us)"
    assert _value_text(Decimal("1.50")) == "Decimal('1.50')"
    assert _value_text(UUID(int=1)) == "UUID('00000000-0000-0000-0000-000000000001')"


# ---------------------------------------------------------------------------
# Review of 583c4b25: one rule for the source argument of why_missing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_source_argument_naming_two_sources_is_refused(
    tmp_path: Path, mode: str
) -> None:
    """The right source is named a (id dict_v1:a); the left is NAMED dict_v1:a (id
    dict_v1:dict_v1:a). 'dict_v1:a' is the right's id and the left's name: refused,
    never the two bindings combined. Each unique spelling answers for its own source,
    live and after a load-only run."""
    import warnings

    from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

    def orchestrator() -> Any:
        return (
            AsyncPipelineOrchestrator()
            if mode == "async"
            else SyncPipelineOrchestrator()
        )

    def sources() -> tuple[DictSource, DictSource]:
        right = DictSource(
            [{"k": "match", "y": 2}],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="a",
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # a name shaped like an id warns
            left = DictSource(
                [{"k": "match", "x": 1}],
                tag_columns=["k"],
                tag_only_columns=True,
                source_id="dict_v1:a",
            )
        return left, right

    left, right = sources()
    assert (left.source_id, right.source_id) == ("dict_v1:dict_v1:a", "dict_v1:a")
    job = PipelineJob(name="twoa", database=DeltaTableDatabase(tmp_path / "db"))
    with job:
        SemiJoin()(left, right)
    job.run(orchestrator=orchestrator())
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(
        tmp_path / "job.json", database=DeltaTableDatabase(tmp_path / "db")
    )
    again_left, again_right = sources()
    loaded.bind(sources={"dict_v1:a": again_left, "a": again_right})
    loaded.run(orchestrator=orchestrator(), load_only=True)
    for current in (job, loaded):
        node = current.nodes["SemiJoin"]
        with pytest.raises(ValueError, match="names several sources") as refused:
            node.why_missing("dict_v1:a", "row_0")
        assert "node 'a' (id dict_v1:a)" in str(refused.value)
        assert "node 'dict_v1:a' (id dict_v1:dict_v1:a)" in str(refused.value)
        # The right source, by its label: its rows only choose, never flow on.
        right_answer = node.why_missing("a", "row_0")
        assert right_answer.missing and right_answer.present_in == ("a",)
        # The left source, by its full id: its row reaches the output.
        left_answer = node.why_missing("dict_v1:dict_v1:a", "row_0")
        assert not left_answer.missing
        assert left_answer.present_in == ("dict_v1:a", "SemiJoin")
