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
    cached = CachedSource(inner, cache_database=cache, source_id="cached")
    job = PipelineJob(name="c", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(cached)
    assert cache.list_table_paths() == []
    trace = job.nodes["cached"].trace({"k": "a", "g": "1"}, "x")
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
    assert (cell.source_id, cell.record_id, cell.value) == ("spied", "row_0", 1)


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
        ("same", 1),
        ("same_more", 100),
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
    CachedSource(
        first, cache_database=cache, cache_path=("shared",), source_id="v"
    ).as_table()
    second = DictSource(
        [{"k": "a", "g": "1", "x": 2, "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="v",
    )
    cached = CachedSource(
        second, cache_database=cache, cache_path=("shared",), source_id="v"
    )
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
    with pytest.raises(ValueError, match="no row"):
        job.nodes["SelectDataColumns"].trace({"k": 1.0, "n": False}, "x")


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
    cached = CachedSource(inner, cache_database=cache, source_id="cache_alias")
    job = PipelineJob(name="al", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(cached)
    job.run()
    return job, cache


def test_a_cache_alias_traces_to_the_wrapped_source(tmp_path: Path) -> None:
    job, _ = aliased_job(tmp_path)
    node = job.nodes["SelectDataColumns"]
    trace = node.trace({"k": "a", "g": "1"}, "x")
    assert trace.status == "traced"
    assert [
        (c.node, c.source_id, c.record_id, c.value) for c in trace.source_cells()
    ] == [("cache_alias", "inner", "row_0", 1)]
    for name in ("inner", "cache_alias"):
        assert not node.why_missing(name, "row_0").missing


def test_no_root_without_a_snapshot_is_taken_to_hold_a_record(tmp_path: Path) -> None:
    job, _ = aliased_job(tmp_path)
    root = job.nodes["cache_alias"].why_missing("inner", "row_999")
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
    CachedSource(
        first, cache_database=cache, cache_path=("shared",), source_id="v"
    ).as_table()
    second = DictSource(
        [{"k": "a", "g": "1", "x": 2, "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="v",
    )
    cached = CachedSource(
        second, cache_database=cache, cache_path=("shared",), source_id="v"
    )
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
        Batch(batch_size=0)(add.pod(source, label="sums"))
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
        ("s", "row_0", 1)
    ]
    assert not node.why_missing("s", "row_0").missing
    total = job.nodes["totals"].trace({"k": "x"}, "total")
    assert [c.value for c in total.source_cells()] == [1, 4]
    assert not job.nodes["totals"].why_missing("s", "row_1").missing


@pytest.mark.parametrize("cache_first", [True, False])
def test_two_inputs_carrying_one_id_are_told_apart_by_position(
    cache_first: bool,
) -> None:
    """A cache labelled with the id of another real source keeps the ids of the source
    it wraps; at a MergeJoin of the two, each input is read at its own position."""
    inner = DictSource(
        [{"k": "x", "i": 0, "x": 1, "y": 2}],
        tag_columns=["k", "i"],
        tag_only_columns=True,
        source_id="s",
    )
    cache = CachedSource(
        inner, cache_database=InMemoryArrowDatabase(), source_id="other", label="cache"
    )
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
    assert leaves == [("other", "row_0", 101), ("s", "row_0", 1)]
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
    CachedSource(
        first, cache_database=cache, cache_path=("shared",), source_id="v"
    ).as_table()
    second = DictSource(
        [{"k": "a", "g": "1", "d": date(2025, 1, 2), "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="v",
    )
    cached = CachedSource(
        second, cache_database=cache, cache_path=("shared",), source_id="v"
    )
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
    """A cache named ``alias`` over source ``s``, then a Filter rejecting every row:
    no held row carries ``s``, yet ``s`` feeds the node (its stored id)."""
    inner = DictSource(
        [{"k": "a", "g": "1", "x": 1, "y": 0}],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="s",
    )
    cache = CachedSource(
        inner, cache_database=InMemoryArrowDatabase(), source_id="alias"
    )
    job = PipelineJob(name="noneleft", database=InMemoryArrowDatabase())
    with job:
        SelectDataColumns(["x"])(Filter({"x": 999})(cache))
    node = job.nodes["SelectDataColumns"]
    assert node.why_missing("s", "row_0").status == "not_computed"
    job.run()
    for name in ("s", "alias"):
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
            [row], tag_columns=["k", "i"], tag_only_columns=True, source_id="s"
        )
        return CachedSource(
            inner, cache_database=InMemoryArrowDatabase(), source_id=name
        )

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
    with pytest.raises(ValueError, match="holds no row"):
        job.nodes["sums"].trace({"k": "a", "g": "1"}, "s")
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
            [row], tag_columns=["k", "i"], tag_only_columns=True, source_id="s"
        )
        return CachedSource(
            inner, cache_database=InMemoryArrowDatabase(), source_id=source_id
        )

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
    with pytest.raises(ValueError, match="holds no row"):
        job.nodes["sums"].trace({"k": "a", "g": "1"}, "s")


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
        ("s", "row_1", 4)
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
    with pytest.raises(ValueError, match="holds no row"):
        job.nodes["sums"].trace({"k": "a", "g": "1"}, "_total")
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
        ("a", "row_0", "x"),
        ("b", "row_0", "x"),
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
        assert cells == "[('a', 'row_1'), ('b', 'row_0')]"
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
        source_id="s",
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
        source_id="same",
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
        source_id="same",
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
        f"s{n:02d}" for n in range(10)
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
    ] == [("s", "row_0", "k", "x")]


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
    cached = CachedSource(inner, cache_database=cache, source_id="cached")
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
        source_id="bc",
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
