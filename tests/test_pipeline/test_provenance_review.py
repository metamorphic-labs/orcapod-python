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
)
from orcapod.sources import ArrowTableSource, CachedSource, DictSource

pytestmark = pytest.mark.filterwarnings("ignore::Warning")


def table_source(
    rows: list[dict[str, Any]], schema: pa.Schema, tags: list[str], source_id: str
) -> ArrowTableSource:
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
    assert [c.value for c in trace.source_cells()] == [1]
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
    assert blamed.removed_by is not None
    assert "user-defined" in blamed.removed_by.text


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


def _batch_trace_seconds(n: int) -> float:
    """Seconds to trace one batch of ``n`` function outputs (best of three)."""
    import time

    rows = [(f"k{i}", "1", i, 1) for i in range(n)]
    source = table_source(rows_kgxy(*rows), TWO_BY_TWO, ["k", "g"], "src")
    job = PipelineJob(name=f"n{n}", database=InMemoryArrowDatabase())
    with job:
        Batch(batch_size=0)(add.pod(source, label="sums"))
    job.run()
    (row,) = job.nodes["Batch"].as_table().to_pylist()
    best = float("inf")
    cells = 0
    for _ in range(3):
        start = time.perf_counter()
        trace = job.nodes["Batch"].trace({"g": row["g"]}, "s")
        best = min(best, time.perf_counter() - start)
        cells = len(trace.source_cells())
    assert cells == 2 * n
    return best


def test_tracing_many_members_scales_linearly() -> None:
    """CodeRabbit thread on PR 530: each member's lookups (input rows, stored entries,
    source records) are indexed once per call, not scanned again per member."""
    small, large = _batch_trace_seconds(400), _batch_trace_seconds(1600)
    # Linear work grows about 4x; scanning per member grows about 16x.
    assert large / small < 8


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
