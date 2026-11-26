"""A job's graph edges carry their input position (ITL-1419, stage 1 of ITL-1472).

An edge is ``(upstream, node, position)``: one upstream given to two inputs of an ordered
operator is two edges, so ``MergeJoin()(a, a)`` and ``SemiJoin()(a, a)`` run in a job as
they run standalone. ``Join``, ``Rejoin`` and a function pod given one stream twice are
refused by the operator (or the pod's join) itself, standalone and in a job.

Inputs have two tag columns (``id``, ``key``) and two data columns (``x``, ``y``), in two
layouts (tag order and column order swapped).
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import Pipeline, PipelineJob, function_pod, sources
from orcapod.core.operators import Join, MergeJoin, Rejoin, SemiJoin
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator


LAYOUTS = ["tags_id_key", "tags_key_id"]
MODES = ["sync", "async"]
OPERATORS = {"MergeJoin": MergeJoin, "SemiJoin": SemiJoin}


def _source(name: str, layout: str, rows: list[tuple[int, str, int, int]]) -> Any:
    """A source over ``rows`` of ``(id, key, x, y)``, laid out by ``layout``."""
    columns = {
        "id": pa.array([r[0] for r in rows], pa.int64()),
        "key": pa.array([r[1] for r in rows], pa.large_string()),
        "x": pa.array([r[2] for r in rows], pa.int64()),
        "y": pa.array([r[3] for r in rows], pa.int64()),
    }
    if layout == "tags_id_key":
        order, tags = ["id", "key", "x", "y"], ["id", "key"]
    else:
        order, tags = ["y", "key", "x", "id"], ["key", "id"]
    table = pa.table({name: columns[name] for name in order})
    return sources.ArrowTableSource(
        table, tag_columns=tags, tag_only_columns=True, source_id=name
    )


def _csv_a(tmp_path: Path, layout: str) -> Any:
    """``_a`` as a CSV file, which a loaded job reads again from its saved config."""
    return _csv(
        tmp_path, "a", layout, [(1, "p", 1, 10), (2, "q", 2, 20), (3, "r", 3, 30)]
    )


def _csv(
    tmp_path: Path, name: str, layout: str, rows: list[tuple[int, str, int, int]]
) -> Any:
    if layout == "tags_id_key":
        header, tags = "id,key,x,y", ["id", "key"]
        lines = [f"{i},{k},{x},{y}" for i, k, x, y in rows]
    else:
        header, tags = "y,key,x,id", ["key", "id"]
        lines = [f"{y},{k},{x},{i}" for i, k, x, y in rows]
    path = tmp_path / f"{name}.csv"
    path.write_text("\n".join([header, *lines]) + "\n")
    return sources.CSVSource(
        str(path), tag_columns=tags, tag_only_columns=True, source_id=name
    )


def _a(layout: str) -> Any:
    return _source("a", layout, [(1, "p", 1, 10), (2, "q", 2, 20), (3, "r", 3, 30)])


def _b(layout: str) -> Any:
    return _source("b", layout, [(1, "p", 100, 1000), (2, "q", 101, 1001)])


def _rows(table: Any) -> list[str]:
    return sorted(repr(sorted(row.items())) for row in table.to_pylist())


def _run(job: Any, mode: str, **kwargs: Any) -> None:
    if mode == "sync":
        job.run(**kwargs)
    else:
        asyncio.run(job.run_async(**kwargs))


def _standalone(operator: str, first: Any, second: Any) -> list[str]:
    return _rows(OPERATORS[operator]()(first, second).as_table(all_info=True))


# ── one stream given to both inputs ───────────────────────────────────────────


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("operator", sorted(OPERATORS))
def test_one_stream_twice_runs_in_a_pipeline_job(
    operator: str, layout: str, mode: str
) -> None:
    a = _a(layout)
    job = PipelineJob(name="twice", database=InMemoryArrowDatabase())
    with job:
        OPERATORS[operator]()(a, a, label="step")
    _run(job, mode)
    assert _rows(job.step.as_table(all_info=True)) == _standalone(operator, a, a)
    assert job.step.as_table().num_rows == 3


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("operator", sorted(OPERATORS))
def test_one_stream_twice_runs_in_a_job_made_from_a_pipeline(
    operator: str, layout: str, mode: str
) -> None:
    a = _a(layout)
    pipeline = Pipeline(name="twice")
    with pipeline:
        OPERATORS[operator]()(a, a, label="step")
    job = PipelineJob.from_pipeline(
        pipeline, sources={"a": a}, database=InMemoryArrowDatabase()
    )
    _run(job, mode)
    assert _rows(job.step.as_table(all_info=True)) == _standalone(operator, a, a)


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("operator", sorted(OPERATORS))
def test_one_stream_twice_runs_after_save_and_load(
    operator: str, layout: str, mode: str, tmp_path: Path
) -> None:
    a = _csv_a(tmp_path, layout)
    db_path = tmp_path / "db"
    job = PipelineJob(name="twice", database=DeltaTableDatabase(db_path))
    with job:
        OPERATORS[operator]()(a, a, label="step")
    _run(job, mode)
    live = _rows(job.step.as_table(all_info=True))
    job.save(tmp_path / "job.json")
    saved = json.loads((tmp_path / "job.json").read_text())["pipeline"]
    # No format change: the edge list holds the pair once; the operator's inputs
    # list holds the stream twice, in argument order.
    step_key = next(k for k, d in saved["nodes"].items() if d.get("label") == "step")
    a_key = next(k for k, d in saved["nodes"].items() if d.get("label") == "a")
    assert saved["edges"] == [[a_key, step_key]]
    assert saved["nodes"][step_key]["inputs"] == [a_key, a_key]

    loaded = PipelineJob.load(
        tmp_path / "job.json", database=DeltaTableDatabase(db_path)
    )
    assert [up.label for up in loaded.step.upstreams] == ["a", "a"]
    _run(loaded, mode, load_only=True)
    assert _rows(loaded.step.as_table(all_info=True)) == live
    # Saving the loaded job writes the same pipeline.
    loaded.save(tmp_path / "again.json")
    assert json.loads((tmp_path / "again.json").read_text())["pipeline"] == saved


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("operator", sorted(OPERATORS))
def test_one_stream_twice_runs_through_node_run(operator: str, layout: str) -> None:
    """``node.run()`` with the upstream run first and held, and with nothing run."""
    a = _a(layout)
    job = PipelineJob(name="twice", database=InMemoryArrowDatabase())
    with job:
        selected = a.select_data_columns(["x", "y"], label="selected")
        OPERATORS[operator]()(selected, selected, label="step")
    expected = _standalone(
        operator, a.select_data_columns(["x", "y"]), a.select_data_columns(["x", "y"])
    )
    job.step.run()
    assert _rows(job.step.as_table(all_info=True)) == expected
    job.step.clear_cache()
    job.step.run()  # ``selected`` holds a current run: its rows feed both positions
    assert _rows(job.step.as_table(all_info=True)) == expected


@pytest.mark.parametrize("operator", sorted(OPERATORS))
def test_one_stream_twice_holds_and_feeds_both_positions_async(operator: str) -> None:
    """A held upstream feeds both positions on the async orchestrator."""
    a = _a("tags_id_key")
    job = PipelineJob(name="twice", database=InMemoryArrowDatabase())
    with job:
        selected = a.select_data_columns(["x", "y"], label="selected")
        OPERATORS[operator]()(selected, selected, label="step")
    expected = _standalone(
        operator, a.select_data_columns(["x", "y"]), a.select_data_columns(["x", "y"])
    )
    job.selected.run()
    job.run(orchestrator=AsyncPipelineOrchestrator())
    assert _rows(job.step.as_table(all_info=True)) == expected


# ── two streams, both orders ─────────────────────────────────────────────────


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("reverse", [False, True], ids=["a_b", "b_a"])
@pytest.mark.parametrize("operator", sorted(OPERATORS))
def test_two_streams_in_either_order_give_the_standalone_rows(
    operator: str, reverse: bool, mode: str
) -> None:
    a, b = _a("tags_id_key"), _b("tags_key_id")
    first, second = (b, a) if reverse else (a, b)
    job = PipelineJob(name="two", database=InMemoryArrowDatabase())
    with job:
        OPERATORS[operator]()(first, second, label="step")
    _run(job, mode)
    assert _rows(job.step.as_table(all_info=True)) == _standalone(
        operator, first, second
    )


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("reverse", [False, True], ids=["a_first", "b_first"])
def test_one_upstream_at_both_positions_of_two_nodes(reverse: bool, mode: str) -> None:
    """``a`` at position 0 of one node and 1 of another, ``b`` the reverse."""
    a, b = _a("tags_id_key"), _b("tags_id_key")
    job = PipelineJob(name="cross", database=InMemoryArrowDatabase())
    with job:
        if reverse:
            MergeJoin()(b, a, label="ba")
            MergeJoin()(a, b, label="ab")
        else:
            MergeJoin()(a, b, label="ab")
            MergeJoin()(b, a, label="ba")
        MergeJoin()(a, a, label="aa")
    _run(job, mode)
    assert _rows(job.ab.as_table(all_info=True)) == _standalone("MergeJoin", a, b)
    assert _rows(job.ba.as_table(all_info=True)) == _standalone("MergeJoin", b, a)
    assert _rows(job.aa.as_table(all_info=True)) == _standalone("MergeJoin", a, a)


# ── observer events of a source node ─────────────────────────────────────────


class _Events:
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def contextualize(self, *identity_path: str) -> _Events:
        return self

    def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
        pass

    def on_run_end(self, run_id: str) -> None:
        pass

    def on_node_start(
        self, node_label: str, node_hash: str, tag_schema: Any = None
    ) -> None:
        self.events.append(("start", node_label))

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        self.events.append(("end", node_label))

    def on_data_start(self, node_label: str, tag: Any, data: Any) -> None:
        pass

    def on_data_end(self, node_label: str, *args: Any, **kwargs: Any) -> None:
        pass

    def on_data_crash(self, node_label: str, *args: Any, **kwargs: Any) -> None:
        pass

    def create_data_logger(self, tag: Any, data: Any) -> Any:
        from orcapod.pipeline.observer import NoOpObserver

        return NoOpObserver().create_data_logger(tag, data)


@pytest.mark.parametrize("mode", MODES)
def test_a_source_fed_twice_has_the_events_of_a_source_fed_once(mode: str) -> None:
    def events_of(twice: bool) -> list[tuple[str, str]]:
        a, b = _a("tags_id_key"), _b("tags_id_key")
        job = PipelineJob(name="events", database=InMemoryArrowDatabase())
        with job:
            MergeJoin()(a, a if twice else b, label="step")
        observer = _Events()
        _run(job, mode, observer=observer)
        return [event for event in observer.events if event[1] == "a"]

    assert events_of(twice=True) == events_of(twice=False)
    assert events_of(twice=True) == [("start", "a"), ("end", "a")]


# ── what stays refused ───────────────────────────────────────────────────────


def _data_tags_only(name: str = "d") -> Any:
    """A source whose one tag is a data-tag and which has no other data column: no
    data column overlaps when it is given twice. ``id`` repeats a value, so
    ``Join(d, d)`` would give 5 rows where ``Join(d)`` gives 3."""
    table = pa.table({"id": pa.array([1, 1, 2], pa.int64())})
    return sources.ArrowTableSource(
        table, tag_columns=["id"], tag_only_columns=False, source_id=name
    )


def _data_tags_and_data(name: str = "w") -> Any:
    """Two data-tags (``id``, ``key``) and two data columns (``x``, ``y``)."""
    table = pa.table(
        {
            "key": pa.array(["p", "p", "q"], pa.large_string()),
            "x": pa.array([1, 2, 3], pa.int64()),
            "id": pa.array([1, 1, 2], pa.int64()),
            "y": pa.array([10, 20, 30], pa.int64()),
        }
    )
    return sources.ArrowTableSource(
        table, tag_columns=["id", "key"], tag_only_columns=False, source_id=name
    )


@function_pod(output_keys="s")
def _identity(id: int | None) -> int | None:
    return id


COMMUTATIVE = {
    "Join": lambda *streams, **kw: Join()(*streams, **kw),
    "Rejoin": lambda *streams, **kw: Rejoin()(*streams, **kw),
    "function_pod": lambda *streams, **kw: _identity.pod(*streams, **kw),
}
INPUTS = {"data_tags_only": _data_tags_only, "data_tags_and_data": _data_tags_and_data}


REPEATS = {
    "same_object": lambda make: (lambda s: (s, s))(make()),
    "equal_sources": lambda make: (make(), make()),
}
REFUSAL = r"same stream.*more than once.*MergeJoin"


def _refusal(pod: str) -> str:
    """The refusal: it names the operator that formed the set (a function pod's join),
    and a function pod adds its function as context."""
    if pod == "function_pod":
        return rf"Join: the {REFUSAL}.*in the inputs of the function pod of '_identity'"
    return rf"{pod}: the {REFUSAL}"


@pytest.mark.parametrize("repeat", sorted(REPEATS))
@pytest.mark.parametrize("inputs", sorted(INPUTS))
@pytest.mark.parametrize("pod", sorted(COMMUTATIVE))
def test_a_repeated_operand_is_refused_where_the_identity_is_formed(
    pod: str, inputs: str, repeat: str
) -> None:
    """A commutative operator, or a function pod (which joins its inputs), forms its
    identity from its operands as a set, so ``Join(d)`` and ``Join(d, d2)`` (``d2`` a
    separate source of ``d``'s identity) would have one identity but different rows.
    Forming that identity refuses two operands of one identity, one object or not,
    naming the operator or function and the stream."""
    first, second = REPEATS[repeat](INPUTS[inputs])
    assert first.content_hash() == second.content_hash()
    # With data columns besides the data-tags, the operator's own check (a function
    # pod's join's) refuses the overlapping columns first, when the step is built.
    refusal = _refusal(pod) if inputs == "data_tags_only" else "overlapping"
    with pytest.raises(InputValidationError, match=refusal):
        COMMUTATIVE[pod](first, second).content_hash()


@pytest.mark.parametrize("pod", ["Join", "Rejoin"])
def test_a_repeat_among_three_operands_is_refused(pod: str) -> None:
    d = _data_tags_only("d")
    # A second branch of ``d`` (Rejoin takes branches of one stream), and a separate
    # source of ``d``'s identity as the third operand.
    branch = d.add_tags(cohort="A")
    with pytest.raises(InputValidationError, match=rf"{pod}.*{REFUSAL}"):
        COMMUTATIVE[pod](d, branch, _data_tags_only("d")).content_hash()


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("repeated_first", [False, True])
@pytest.mark.parametrize("repeat", sorted(REPEATS))
@pytest.mark.parametrize("pod", sorted(COMMUTATIVE))
def test_a_repeated_operand_is_refused_when_recorded_in_either_order(
    pod: str, repeat: str, repeated_first: bool, mode: str
) -> None:
    """Recording forms the identity: refused before anything is recorded, whether the
    one-operand step is recorded before or after; the job keeps only that step, and
    runs it (sync and async) with the rows of the standalone step."""
    first, second = REPEATS[repeat](_data_tags_only)
    expected = _rows(COMMUTATIVE[pod](first).as_table())
    job = PipelineJob(name="refused", database=InMemoryArrowDatabase())
    with job:
        if not repeated_first:
            COMMUTATIVE[pod](first, label="once")
        with pytest.raises(InputValidationError, match=_refusal(pod)):
            COMMUTATIVE[pod](first, second, label="twice")
        if repeated_first:
            COMMUTATIVE[pod](first, label="once")
    assert "twice" not in job.nodes
    _run(job, mode)
    assert _rows(job.once.as_table()) == expected
    assert len(expected) == 3


@pytest.mark.parametrize("repeated_first", [False, True])
def test_a_step_over_a_repeated_join_is_refused_in_either_order(
    repeated_first: bool,
) -> None:
    """Round-2 finding P1: a downstream step over ``Join(d, d2)`` ran in one recording
    order and failed compilation in the other. The join is refused when its identity
    is formed, in both orders."""
    d, d2 = _data_tags_only(), _data_tags_only()
    job = PipelineJob(name="refused", database=InMemoryArrowDatabase())
    with job:
        if not repeated_first:
            d.select_data_columns(["id"], label="once")
        with pytest.raises(InputValidationError, match=REFUSAL):
            Join()(d, d2).select_data_columns(["id"], label="after")
        if repeated_first:
            d.select_data_columns(["id"], label="once")
    assert "after" not in job.nodes
    job.run()
    assert job.once.as_table().num_rows == 3


def _raw_with_map(values: list[int]) -> Any:
    """A raw stream (no source) with a map data column: its identity hashes its rows,
    and a map value cannot be hashed."""
    from orcapod.core.streams.arrow_table_stream import ArrowTableStream

    table = pa.table(
        {
            "id": pa.array(values, pa.int64()),
            "m": pa.array(
                [[("k", v)] for v in values], pa.map_(pa.large_string(), pa.int64())
            ),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


def test_an_operand_whose_identity_cannot_be_formed_raises_the_hashing_error() -> None:
    """Round-2 finding P2: the rule adds no error of its own. A raw stream whose
    identity cannot be computed raises, when a commutative pod's identity is formed,
    exactly what hashing that stream raises; its rows are computed as before."""
    raw = _raw_with_map([1, 2])
    other = _data_tags_and_data()
    joined = Join()(raw, other.select_data_columns(["x"]))
    assert joined.as_table().num_rows == 3
    with pytest.raises(ValueError) as own:
        raw.content_hash()
    with pytest.raises(type(own.value)) as formed:
        joined.content_hash()
    assert str(formed.value) == str(own.value)


def test_an_ordered_operator_still_takes_one_stream_twice() -> None:
    """The refusal is for commutative pods only: ``MergeJoin(d, d)`` is two edges."""
    d = _data_tags_only()
    assert MergeJoin()(d, d).as_table().num_rows == 5
    assert SemiJoin()(d, d).as_table().num_rows == 3


# ── the positioned edges ─────────────────────────────────────────────────────


def _edges_by_label(job: Any) -> list[tuple[str, str, int | None]]:
    """The job's positioned edges, by node label (the graph is keyed by hashes)."""
    label = {
        key: node.label
        for key, node in job._persistent_node_map.items()  # pyright: ignore[reportPrivateUsage]
    }
    return sorted(
        (label[up], label[down], position)
        for up, down, position in job._graph_edges  # pyright: ignore[reportPrivateUsage]
        if down in label
        and label[down] in {"aa", "ab", "ba", "joined", "fn", "join1", "rejoin1"}
    )


def test_each_input_is_one_edge_at_its_position(tmp_path: Path) -> None:
    """An ordered operator's inputs are at their argument index, a commutative one's
    have no position (also with one input), a function pod's input is at 0; the same
    after save and load."""
    a = _csv_a(tmp_path, "tags_id_key")
    b = _csv(tmp_path, "b", "tags_key_id", [(1, "p", 100, 1000), (2, "q", 101, 1001)])

    @function_pod(output_keys="s")
    def total(x: int, y: int) -> int:
        return x + y

    db_path = tmp_path / "db"
    job = PipelineJob(name="edges", database=DeltaTableDatabase(db_path))
    with job:
        MergeJoin()(a, a, label="aa")
        MergeJoin()(b, a, label="ba")
        Join()(
            a.select_data_columns(["x"], label="ax"),
            b.select_data_columns(["y"], label="by"),
            label="joined",
        )
        total.pod(a, label="fn")
        Join()(a, label="join1")
        Rejoin()(b, label="rejoin1")
    expected = [
        ("a", "aa", 0),
        ("a", "aa", 1),
        ("a", "ba", 1),
        ("a", "fn", 0),
        ("a", "join1", None),
        ("ax", "joined", None),
        ("b", "ba", 0),
        ("b", "rejoin1", None),
        ("by", "joined", None),
    ]
    assert _edges_by_label(job) == expected
    job.run()
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(
        tmp_path / "job.json", database=DeltaTableDatabase(db_path)
    )
    assert _edges_by_label(loaded) == expected


@pytest.mark.parametrize("repeat", [False, True], ids=["a_b", "a_a"])
def test_the_saved_edge_list_is_the_one_main_wrote(
    repeat: bool, tmp_path: Path
) -> None:
    """No format change: a saved file lists each ``(upstream, node)`` pair once, sorted,
    exactly as ``main`` wrote it (``sorted(_hash_graph.edges())``); the positions are
    rebuilt from the operators' ``inputs`` lists."""
    a = _csv_a(tmp_path, "tags_id_key")
    b = _csv(tmp_path, "b", "tags_id_key", [(1, "p", 100, 1000)])
    job = PipelineJob(name="saved", database=InMemoryArrowDatabase())
    with job:
        b.select_data_columns(["y"], label="other")
        MergeJoin()(a, a if repeat else b, label="step")
        SemiJoin()(b, a, label="semi")
    pipeline = job.compiled_pipeline
    pipeline.save(tmp_path / "pipeline.json")
    saved = json.loads((tmp_path / "pipeline.json").read_text())
    main_edges = sorted(pipeline._hash_graph.edges())  # pyright: ignore[reportPrivateUsage]
    assert saved["edges"] == [list(edge) for edge in main_edges]


def test_a_function_pod_over_two_inputs_computes_rows_without_an_identity() -> None:
    """Verification of 79e86762 (P2): a function pod forms its operand set only where
    its identity is formed. Its rows over two distinct inputs, one a raw stream with an
    unused map column, are computed as before; asking its identity raises the existing
    hashing error, as hashing that raw stream does."""
    raw = _raw_with_map([1, 2])
    other = _data_tags_only()
    out = _identity.pod(raw, other)
    assert out.as_table().num_rows == 3
    with pytest.raises(ValueError) as own:
        raw.content_hash()
    with pytest.raises(type(own.value)) as formed:
        out.content_hash()
    assert str(formed.value) == str(own.value)


def test_a_downstream_pod_keeps_the_name_of_the_pod_given_the_repeat() -> None:
    """Verification of 79e86762 (P3): the refusal names the operator that got the
    repeated operand; a pod downstream of it adds no name of its own in its place."""
    d, d2 = _data_tags_only(), _data_tags_only()
    w = _data_tags_and_data()
    downstream = {
        "Join over Rejoin": (
            lambda: Join()(Rejoin()(d, d2), w.select_data_columns(["x"])),
            "Rejoin",
            "Join",
        ),
        "function pod over Rejoin": (
            lambda: _identity.pod(Rejoin()(d, d2)),
            "Rejoin",
            "Join",
        ),
        "Rejoin over Join": (lambda: Rejoin()(Join()(d, d2)), "Join", "Rejoin"),
    }
    for build, given, downstream_name in downstream.values():
        with pytest.raises(InputValidationError) as err:
            build().content_hash()
        message = str(err.value)
        assert f"{given}: the same stream" in message
        assert f"{downstream_name}: the same stream" not in message


def test_a_function_pod_given_one_stream_twice_names_the_function_and_its_join() -> (
    None
):
    """The function pod's own inputs repeat: the message names the function as context
    and keeps the name of the join that formed the set."""
    d, d2 = _data_tags_only(), _data_tags_only()
    with pytest.raises(InputValidationError) as err:
        _identity.pod(d, d2).content_hash()
    assert "'_identity'" in str(err.value)
    assert "Join: the same stream" in str(err.value)


OVER_REJOIN = {
    "Join over Rejoin": lambda rejoined, w, **kw: Join()(
        rejoined, w.select_data_columns(["x"]), **kw
    ),
    "two-input function pod over Rejoin": lambda rejoined, w, **kw: _identity.pod(
        rejoined, w.select_data_columns(["x"]), **kw
    ),
    "SemiJoin over Rejoin": lambda rejoined, w, **kw: SemiJoin()(rejoined, w, **kw),
}


@pytest.mark.parametrize("in_job", [False, True], ids=["standalone", "in_job"])
@pytest.mark.parametrize("case", sorted(OVER_REJOIN))
def test_a_join_over_a_repeated_rejoin_raises_the_rejoin_refusal(
    case: str, in_job: bool
) -> None:
    """Verification of a59c55b7 (P3): a join whose input check meets the upstream
    refusal lets it through as it is (its type and the pod that got the repeat), not
    wrapped in a plain ``InputValidationError``; a function pod adds its context. The
    ``Rejoin`` is built outside the job (inside, recording it is refused first, before
    any step over it)."""
    from orcapod.core.operators.repeated_operands import RepeatedOperandError

    d, d2, w = _data_tags_only(), _data_tags_only(), _data_tags_and_data()
    rejoined = Rejoin()(d, d2)
    with pytest.raises(RepeatedOperandError) as err:
        if in_job:
            job = PipelineJob(name="refused", database=InMemoryArrowDatabase())
            with job:
                OVER_REJOIN[case](rejoined, w, label="after")
        else:
            OVER_REJOIN[case](rejoined, w).content_hash()
    assert err.value.pod == "Rejoin"
    message = str(err.value)
    assert message.startswith(f"Rejoin: the {REFUSAL.split('.*')[0]}")
    assert "Join: the same stream" not in message.replace("Rejoin: the same", "")
    if case.startswith("two-input function pod"):
        assert "in the inputs of the function pod of '_identity'" in message
