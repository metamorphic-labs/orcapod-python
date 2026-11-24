"""A stream given to a job comes in with its pod and every upstream step (ITL-1387).

``job.register(*streams, **named)``, a ``with`` block over streams built before it, and a
job that takes in another job's nodes must give exactly the job of the same pipeline
written by hand in one ``with`` block: node names and types, content and pipeline hashes,
stored table paths and tables, record ids, results and the saved file, on the sync and the
async path, in every registration order. The design is
``docs/dev/specs/itl-1387-job-from-streams.md``.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import re
from collections import Counter
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.function_pod import FallThroughPod
from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
from orcapod.core.operators import MapData
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import SourceSpecMismatchError
from orcapod.pipeline import PipelineJob
from orcapod.side_effects import SideEffectPod
from tests.test_core.operators.several_items import (
    DATA_TAGS,
    DATA_FIRST_REVERSED,
    MAIN,
    ROLES,
    TAGS_FIRST,
    P,
    Q,
    Arrangement,
    make_source,
)
from tests.test_core.test_order_invariance import OPERATOR_CASES, OperatorCase

# Stored metadata that can vary across runs, including which async node computes a
# row when multiple nodes share one function's result table.
_RUN_DEPENDENT = {
    "__pod_ts",
    "__data_id",
    "__computed",
    "__recorded_at",
    "__pipeline_run_id",
}
# A result's random record id inside a source-info token differs on every run.
_RANDOM_ID = re.compile(r"::[0-9a-f]{32}::")

Mode = str  # "sync" or "async"


def _rows(table: pa.Table, mode: Mode) -> list[str] | set[tuple[str, int]]:
    """Compare ordered sync rows or an async set of row and count pairs."""
    keep = [name for name in table.column_names if name not in _RUN_DEPENDENT]
    rows = [
        _RANDOM_ID.sub("::<id>::", repr(sorted(row.items())))
        for row in table.select(keep).to_pylist()
    ]
    return rows if mode == "sync" else set(Counter(rows).items())


def test_async_row_comparison_checks_each_rows_multiplicity() -> None:
    """Async comparisons ignore order and detect a changed count per row."""
    original = pa.table({"x": [1, 1, 2]})
    reordered = pa.table({"x": [2, 1, 1]})
    changed = pa.table({"x": [1, 2, 2]})
    assert _rows(original, "async") == _rows(reordered, "async")
    assert _rows(original, "async") != _rows(changed, "async")
    assert _rows(original, "sync") != _rows(reordered, "sync")


def _run(job: PipelineJob, mode: Mode) -> None:
    if mode == "sync":
        job.run()
    else:
        asyncio.run(job.run_async())


def _saved(job: PipelineJob, tmp_path: Path, name: str) -> dict[str, Any]:
    path = tmp_path / f"{name}.json"
    job.save(path)
    saved = json.loads(path.read_text())
    # Edges are sorted and operators save their own argument order.
    return {"pipeline": saved["pipeline"], "bindings": saved["bindings"]}


def fingerprint(
    job: PipelineJob, db: Any, tmp_path: Path, name: str, mode: Mode
) -> dict[str, Any]:
    """Everything the property compares, after the job has run."""
    nodes = {
        label: (
            node.node_type,
            node.content_hash().to_string(),
            node.pipeline_hash().to_string(),
        )
        for label, node in job.nodes.items()
    }
    paths = sorted(db.list_table_paths(()))
    tables = {"/".join(path): _rows(db.get_all_records(path), mode) for path in paths}
    results = {
        label: _rows(node.as_table(columns={"system_tags": True, "source": True}), mode)
        for label, node in job.nodes.items()
        if node.node_type != "source"
    }
    return {
        "names": sorted(job.nodes),
        "nodes": nodes,
        "paths": paths,
        "tables": tables,
        "results": results,
        "saved": _saved(job, tmp_path, name),
    }


def _new_job(name: str = "job") -> tuple[PipelineJob, InMemoryArrowDatabase]:
    db = InMemoryArrowDatabase()
    return PipelineJob(name=name, database=db), db


def _assert_same(expected: dict[str, Any], actual: dict[str, Any], way: str) -> None:
    for key in expected:
        assert actual[key] == expected[key], f"{way}: {key} differs"


# ── every exported operator ─────────────────────────────────────────────────


def _prepare(sources: Sequence[Any]) -> list[Any]:
    """One step over each source, kept outside the job in the pre-built ways."""
    return [
        source.select_data_columns(list(source.keys()[1]), label=f"prepared_{i}")
        for i, source in enumerate(sources)
    ]


def _apply(case: OperatorCase, prepared: Sequence[Any]) -> Any:
    return case.build()(*prepared)


OPERATOR_PARAMS = [
    pytest.param(case, id=case.label)
    for cases in OPERATOR_CASES.values()
    for case in cases
]


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize(
    "tag_order, layout",
    [(0, TAGS_FIRST), (1, TAGS_FIRST), (0, DATA_FIRST_REVERSED)],
    ids=["declared", "tags_reversed", "data_first"],
)
@pytest.mark.parametrize("case", OPERATOR_PARAMS)
def test_every_operator_by_register_and_block_equals_hand_written(
    case: OperatorCase,
    tag_order: int,
    layout: str,
    role: str,
    mode: Mode,
    tmp_path: Path,
) -> None:
    """Each exported operator, after one step over each input: registered (the end
    stream alone, and with the prepared streams in two orders) and recorded in a block
    over prepared streams built before it, against the same steps written by hand."""
    if role not in case.roles:
        pytest.skip(f"{case.label} does not take {role} tags")
    hand, hand_db = _new_job()
    with hand:
        _apply(case, _prepare(case.sources(role, tag_order, layout)))
    _run(hand, mode)
    expected = fingerprint(hand, hand_db, tmp_path, "hand", mode)

    def registered(order: Callable[[Any, list[Any]], list[Any]]) -> None:
        prepared = _prepare(case.sources(role, tag_order, layout))
        end = _apply(case, prepared)
        job, db = _new_job()
        job.register(*order(end, prepared))
        _run(job, mode)
        _assert_same(expected, fingerprint(job, db, tmp_path, "reg", mode), "register")

    registered(lambda end, prepared: [end])
    registered(lambda end, prepared: [end, *prepared])
    registered(lambda end, prepared: [*reversed(prepared), end])

    prepared = _prepare(case.sources(role, tag_order, layout))
    job, db = _new_job()
    with job:
        _apply(case, prepared)
    _run(job, mode)
    _assert_same(expected, fingerprint(job, db, tmp_path, "block", mode), "with block")


# ── function pod forms, several streams, several orders ─────────────────────


@op.function_pod(output_keys="f")
def combine(d1: int, p1: int) -> int:
    """``d1 + p1``."""
    return d1 + p1


@op.function_pod(output_keys="g")
def scale(q1: float) -> float:
    """``2 * q1``."""
    return 2 * q1


@op.function_pod(output_keys="h")
def shift(q2: int) -> int:
    """``q2 + 1``."""
    return q2 + 1


@op.function_pod(output_keys="k", ctx_arg="ctx")
def tagged(d1: int, ctx: Any) -> int:
    """``d1``, with the invocation context."""
    return d1


@op.function_pod(output_keys="m")
def through(d3: str) -> str:
    """``d3`` upper-cased."""
    return d3.upper()


# The fall-through form has a function of its own: two nodes of one function share one
# result table, and which of them computes a row follows the run order of independent
# nodes, which is not specified (ITL-1404); the property is about registering, not that.
@op.function_pod(output_keys="m")
def through_lower(d3: str) -> str:
    """``d3`` lower-cased."""
    return d3.lower()


def _forms(sources: dict[str, Any], cache_db: Any) -> dict[str, Any]:
    """The end streams of every function pod form, labelled and unlabelled."""
    main, p, q = sources["main"], sources["p"], sources["q"]
    return {
        # Automatic join of two inputs and automatic select (ITL-807).
        "combined": combine.pod(main, p, label="combined"),
        # The exact primitive over an explicit select (unlabelled steps).
        "scaled": scale.pod.process(q.select_data_columns(["q1"])),
        # strict=True over an explicit select.
        "shifted": shift.pod(
            q.select_data_columns(["q2"]), strict=True, label="shifted"
        ),
        # A pod that takes the invocation context.
        "tagged": tagged.pod(main.select_data_columns(["d1"]), label="tagged"),
        # A pod-cached pod.
        "cached": CachedFunctionPod(
            through.pod,  # pyright: ignore[reportArgumentType]  # the decorator's pod is a FunctionPod
            cache_db,
        )(main, label="cached"),
        # A fall-through pod over a source whose tags are data-tags.
        "fell": FallThroughPod(
            through_lower.pod,  # pyright: ignore[reportArgumentType]  # the decorator's pod is a FunctionPod
            ["t1"],
        )(
            sources["main_data_tags"].select_data_columns(["t1", "d3"]),
            label="fell",
        ),
    }


def _sources() -> dict[str, Any]:
    return {
        "main": make_source(MAIN),
        "p": make_source(P),
        "q": make_source(Q),
        "main_data_tags": make_source(
            MAIN, Arrangement(MAIN.tags, TAGS_FIRST, DATA_TAGS), source_id="s2"
        ),
    }


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_function_pod_forms_registered_in_every_order_equal_hand_written(
    mode: Mode, tmp_path: Path
) -> None:
    """Every function pod form, its six end streams registered in every order of a
    sample of permutations, in one call and split over two, against the hand-written
    job."""
    hand, hand_db = _new_job()
    with hand:
        _forms(_sources(), InMemoryArrowDatabase())
    _run(hand, mode)
    expected = fingerprint(hand, hand_db, tmp_path, "hand", mode)
    labels = sorted(_forms(_sources(), InMemoryArrowDatabase()))
    orders = [
        tuple(labels),
        tuple(reversed(labels)),
        *itertools.islice(itertools.permutations(labels), 1, 4),
    ]
    for order in orders:
        streams = _forms(_sources(), InMemoryArrowDatabase())
        job, db = _new_job()
        job.register(*(streams[label] for label in order[:3]))
        job.register(*(streams[label] for label in order[3:]))
        _run(job, mode)
        _assert_same(
            expected, fingerprint(job, db, tmp_path, "reg", mode), f"order {order}"
        )


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_function_pod_forms_in_a_block_over_prebuilt_streams_equal_hand_written(
    mode: Mode, tmp_path: Path
) -> None:
    """Half the forms built before the job and used by a step in the block, half
    registered, after reopening: the hand-written job."""

    @op.function_pod(output_keys="n")
    def final(f: int, k: int) -> int:
        return f + k

    def build(streams: dict[str, Any]) -> Any:
        return final.pod(streams["combined"], streams["tagged"], label="final")

    hand, hand_db = _new_job()
    with hand:
        streams = _forms(_sources(), InMemoryArrowDatabase())
        build(streams)
    _run(hand, mode)
    expected = fingerprint(hand, hand_db, tmp_path, "hand", mode)

    streams = _forms(_sources(), InMemoryArrowDatabase())
    job, db = _new_job()
    with job:
        build(streams)
    job.register(streams["scaled"], streams["shifted"])
    with job:
        pass
    job.register(fell=streams["fell"], cached=streams["cached"])
    _run(job, mode)
    _assert_same(
        expected, fingerprint(job, db, tmp_path, "mixed", mode), "block + register"
    )


# ── another job's nodes ─────────────────────────────────────────────────────


def _first_part(sources: dict[str, Any]) -> tuple[Any, Any]:
    joined = combine.pod(sources["main"], sources["p"], label="combined")
    other = scale.pod(sources["q"].select_data_columns(["q1"]))
    return MapData({"f": "f2"})(joined, label="renamed"), other


@op.function_pod(output_keys="z")
def last(f2: int, g: float) -> float:
    """``f2 + g``."""
    return f2 + g


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("how", ["register", "block"])
def test_a_job_that_takes_in_another_jobs_nodes_equals_hand_written(
    how: str, mode: Mode, tmp_path: Path
) -> None:
    """A node of another job is walked: its step and every upstream step come in."""
    hand, hand_db = _new_job()
    with hand:
        last.pod(*_first_part(_sources()), label="last")
    _run(hand, mode)
    expected = fingerprint(hand, hand_db, tmp_path, "hand", mode)

    other, _ = _new_job("other")
    with other:
        _first_part(_sources())
    _run(other, mode)
    other_names = sorted(other.nodes)
    function_nodes = [n for n in other.nodes.values() if n.node_type == "function"]
    scaled = next(n for n in function_nodes if n.label != "combined")
    job, db = _new_job()
    if how == "register":
        job.register(last.pod(other.renamed, scaled, label="last"))
    else:
        with job:
            last.pod(other.renamed, scaled, label="last")
    _run(job, mode)
    _assert_same(expected, fingerprint(job, db, tmp_path, "absorbed", mode), how)
    assert other.renamed is not job.renamed
    assert sorted(other.nodes) == other_names


def test_absorbed_steps_see_stored_results_only_under_one_database_and_name() -> None:
    """Tables live under each job's own database and name: the absorbing job reads the
    other job's stored results only when both are the same."""
    calls: list[str] = []

    @op.function_pod(output_keys="y")
    def counted(d1: int) -> int:
        calls.append("counted")
        return d1

    shared = InMemoryArrowDatabase()
    other = PipelineJob(name="j", database=shared)
    with other:
        counted.pod(make_source(MAIN).select_data_columns(["d1"]), label="counted")
    other.run()
    for name, database, expected_calls in (
        ("j", shared, 0),
        ("k", shared, len(MAIN.columns["d1"])),
        ("j", InMemoryArrowDatabase(), len(MAIN.columns["d1"])),
    ):
        calls.clear()
        job = PipelineJob(name=name, database=database)
        job.register(other.counted.map_data({"y": "y2"}))
        job.run()
        assert len(calls) == expected_calls, (name, database is shared)


# ── names ───────────────────────────────────────────────────────────────────


def test_keyword_names_name_the_nodes_and_leave_the_streams_unchanged() -> None:
    main, p = make_source(MAIN), make_source(P)
    first = combine.pod(main, p, label="combined")
    second = MapData({"f": "f2"})(first)
    job, _ = _new_job()
    job.register(renamed=second, total=first)
    assert {"renamed", "total"} <= set(job.nodes)
    assert "combined" not in job.nodes
    assert first.label == "combined" and not second.has_assigned_label


@pytest.mark.parametrize(
    "call, match",
    [
        (lambda job, s: job.register(), "at least one stream"),
        (lambda job, s: job.register(s["main"]), "is a source"),
        (lambda job, s: job.register(_x=s["step"]), "cannot start with"),
        (lambda job, s: job.register(a=s["step"], b=s["step"]), "two names"),
        (lambda job, s: job.register(held=s["held"]), "already in the pipeline"),
        (
            lambda job, s: job.register(name=s["held_unlabelled"]),
            "already in the pipeline as an unlabelled step",
        ),
        (lambda job, s: job.register(held_label=s["other"]), "taken by another node"),
        (lambda job, s: job.register(s["x"], s["y"]), "two labels"),
    ],
    ids=[
        "none",
        "source",
        "underscore",
        "two_names",
        "relabel",
        "relabel_unlabelled",
        "taken",
        "two_labels",
    ],
)
def test_refused_registrations_leave_the_job_unchanged(
    call: Callable[[PipelineJob, dict[str, Any]], None], match: str
) -> None:
    main = make_source(MAIN)
    job, _ = _new_job()
    with job:
        held = main.select_data_columns(["d1", "d2"], label="held_label")
        held_unlabelled = main.select_data_columns(["d1", "d3"])
    streams = {
        "main": main,
        "step": main.select_data_columns(["d1"]),
        "held": held,
        "held_unlabelled": held_unlabelled,
        "other": main.select_data_columns(["d3"]),
        "x": main.select_data_columns(["d2"], label="x"),
        "y": main.select_data_columns(["d2"], label="y"),
    }
    before = (sorted(job.nodes), sorted(job.graph.nodes))
    with pytest.raises(ValueError, match=match):
        call(job, streams)
    assert (sorted(job.nodes), sorted(job.graph.nodes)) == before


def test_names_do_not_follow_the_order_of_registration() -> None:
    def streams() -> list[Any]:
        main = make_source(MAIN)
        return [
            main.select_data_columns(["d1"]),
            main.select_data_columns(["d2"]),
            main.select_data_columns(["d3"]),
        ]

    names = set()
    for order in itertools.permutations(range(3)):
        job, _ = _new_job()
        made = streams()
        for index in order:
            job.register(made[index])
        names.add(
            tuple(
                sorted((k, v.content_hash().to_string()) for k, v in job.nodes.items())
            )
        )
    assert len(names) == 1


# ── what must not happen ────────────────────────────────────────────────────


def test_registering_computes_nothing_stores_nothing_and_changes_no_stream() -> None:
    calls: list[str] = []

    @op.function_pod(output_keys="y")
    def watched(d1: int) -> int:
        calls.append("watched")
        return d1

    main = make_source(MAIN)
    step = main.select_data_columns(["d1"], label="step")
    end = watched.pod(step, label="end")
    watched_streams: list[Any] = [step, end]
    before = [
        (s.label, s.has_assigned_label, s.content_hash(), s.pipeline_hash())
        for s in watched_streams
    ]
    job, db = _new_job()
    job.register(end)
    assert calls == []
    assert db.list_table_paths(()) == []
    after = [
        (s.label, s.has_assigned_label, s.content_hash(), s.pipeline_hash())
        for s in watched_streams
    ]
    assert after == before


# ── loaded jobs, refused blocks, slots ──────────────────────────────────────


def test_a_loaded_operator_node_is_walked_and_an_unattached_function_refused(
    tmp_path: Path,
) -> None:
    db = DeltaTableDatabase(tmp_path / "db")
    (tmp_path / "data.csv").write_text("k,x\n1,1.0\n2,2.0\n")
    source = op.sources.CSVSource(
        str(tmp_path / "data.csv"),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="csv",
    )

    @op.function_pod(output_keys="y")
    def double(x: float) -> float:
        return 2 * x

    job = PipelineJob(name="j", database=db)
    with job:
        MapData({"y": "y2"})(double.pod(source, label="doubled"), label="renamed")
    job.run()
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(tmp_path / "job.json", database=db)
    # A loaded operator node has its operator, so the walk goes through it to the
    # function node beneath, whose function is not attached (ITL-1407 will attach a
    # loadable one; the walk then needs no change).
    assert loaded.renamed.producer is not None
    assert loaded.doubled.producer is None
    for stream, column in ((loaded.doubled, "y"), (loaded.renamed, "y2")):
        target, _ = _new_job()
        with pytest.raises(
            ValueError, match="function of step 'doubled' is not attached"
        ):
            target.register(MapData({column: "w"})(stream))
        assert target.nodes == {}


def test_a_refused_block_leaves_the_job_as_it_was() -> None:
    table = pa.table({"k": [1, 2], "x": [1.0, 2.0]})
    raw = ArrowTableStream(table, tag_columns=["k"])
    job, _ = _new_job()
    with job:
        make_source(MAIN).select_data_columns(["d1"], label="kept")
    with pytest.raises(SourceSpecMismatchError):
        with job:
            raw.map_data({"x": "y"})
    assert sorted(job.nodes) == ["kept", "s"]
    with job:
        pass
    assert sorted(job.nodes) == ["kept", "s"]


def test_a_slot_is_made_from_a_pod_stream_only_when_named() -> None:
    stream = make_source(MAIN).select_data_columns(["d1"])
    with pytest.raises(ValueError, match="only when you name the slot"):
        SourceJobNode.from_stream(stream)
    with pytest.raises(ValueError, match="only when you name the slot"):
        SourceNode.from_stream(stream)
    assert SourceJobNode.from_stream(stream, name="cut").name == "cut"


# ── review round 1 (independent Codex review of PR 500) ─────────────────────


def _two_rows(name: str, start: int) -> Any:
    return op.sources.DictSource(
        [
            {"id": 1, "x": start, "y": start + 10},
            {"id": 2, "x": start + 1, "y": start + 11},
        ],
        source_id=name,
        tag_columns=["id"],
        tag_only_columns=True,
    )


@op.function_pod(output_keys="f")
def doubled_x(x: int) -> int:
    """``2 * x``."""
    return 2 * x


@pytest.mark.parametrize("way", ["register", "block"])
@pytest.mark.parametrize("other_use_first", [False, True])
def test_an_ordered_operator_keeps_its_argument_order_through_save_and_load(
    other_use_first: bool, way: str, tmp_path: Path
) -> None:
    """Finding 2: ``MergeJoin(left, right)`` saved and loaded keeps ``left, right``
    whatever order the steps were recorded in (another use of ``right`` recorded
    first or after), and a job that takes in the loaded node gets the same rows."""
    from orcapod.core.operators import MergeJoin

    for name, shift in (("left", 1), ("right", 100)):
        (tmp_path / f"{name}.csv").write_text(f"id,x,y\n1,{shift},0\n2,{shift + 1},0\n")
    left, right = (
        op.sources.CSVSource(
            str(tmp_path / f"{name}.csv"),
            tag_columns=["id"],
            tag_only_columns=True,
            source_id=name,
        )
        for name in ("left", "right")
    )
    other_use = right.select_data_columns(["y"], label="right_pick")
    merged = MergeJoin()(left, right, label="merged")
    db = DeltaTableDatabase(tmp_path / "db")
    job = PipelineJob(name="ordered", database=db)
    order = (other_use, merged) if other_use_first else (merged, other_use)
    if way == "register":
        job.register(*order)
    else:
        with job:  # recorded in a block: the merge and the other use rebuilt here
            if other_use_first:
                right.select_data_columns(["y"], label="right_pick")
            MergeJoin()(left, right, label="merged")
            if not other_use_first:
                right.select_data_columns(["y"], label="right_pick")
    job.run()
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(tmp_path / "job.json", database=db)
    assert [u.label for u in loaded.merged.upstreams] == ["left", "right"]
    target, _ = _new_job()
    target.register(loaded.merged)
    target.run()
    assert target.merged.as_table()["x"].to_pylist() == [[1, 100], [2, 101]]


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_saved_files_do_not_follow_registration_order(
    mode: Mode, tmp_path: Path
) -> None:
    """Finding 2: the saved edge list is canonical, so jobs registered in two orders
    save equal files (compared as written)."""
    saved = []
    for reverse in (False, True):
        streams = _forms(_sources(), InMemoryArrowDatabase())
        job, _ = _new_job()
        labels = sorted(streams)
        job.register(
            *(streams[label] for label in (reversed(labels) if reverse else labels))
        )
        _run(job, mode)
        saved.append(_saved(job, tmp_path, f"order{reverse}"))
    assert saved[0] == saved[1]


def _sqlite_job(tmp_path: Path) -> PipelineJob:
    from orcapod.databases import ConnectorArrowDatabase, SQLiteConnector

    return PipelineJob(
        name="j",
        database=ConnectorArrowDatabase(SQLiteConnector(str(tmp_path / "db.sqlite"))),
    )


def _state_of(job: PipelineJob) -> tuple[Any, ...]:
    return (
        sorted(job.nodes),
        {label: id(node) for label, node in job.nodes.items()},
        sorted(job.sources),
        sorted(job.graph.nodes),
    )


@pytest.mark.parametrize("warm", [False, True], ids=["empty_job", "job_with_nodes"])
@pytest.mark.parametrize(
    "failure", ["block_unstorable", "register_unstorable", "register_name_taken"]
)
def test_a_refused_way_in_leaves_the_job_exactly_as_it_was(
    failure: str, warm: bool, tmp_path: Path
) -> None:
    """Finding 3: a refusal raised after compilation began (a database that cannot
    store a result table) or by a name check leaves the job's nodes (the same
    objects), bindings and recorded steps as they were."""
    from orcapod.errors import UnstorableColumnError

    @op.function_pod(output_keys="f")
    def nested(x: int) -> list[dict[str, int]]:
        return [{"x": x}]

    source = _two_rows("s", 1)
    job = _sqlite_job(tmp_path) if failure != "register_name_taken" else _new_job()[0]
    if warm:
        job.register(source.select_data_columns(["x"], label="kept"))
    before = _state_of(job)
    if failure == "block_unstorable":
        with pytest.raises(UnstorableColumnError):
            with job:
                nested.pod(source, label="nested")
    elif failure == "register_unstorable":
        with pytest.raises(UnstorableColumnError):
            job.register(nested.pod(source, label="nested"))
    else:
        taken = source.select_data_columns(["y"], label="exact")
        with pytest.raises(ValueError, match="taken by another node"):
            job.register(taken, exact=source.map_data({"x": "z"}))
    assert _state_of(job) == before


def test_a_job_keeps_its_bindings_when_the_job_it_took_nodes_from_is_rebound() -> None:
    """Finding 4: after the other job's slot is rebound, the absorbing job still reads
    the source it took in, and can take new steps over its own nodes."""
    other, _ = _new_job("other")
    with other:
        MapData({"x": "x2"})(_two_rows("s", 1), label="absorbed")
    target, _ = _new_job("target")
    target.register(other.absorbed)
    other.bind(sources={"s": _two_rows("replacement", 10)})
    target.register(MapData({"x2": "x3"})(target.absorbed, label="more"))
    target.run()
    assert target.more.as_table()["x3"].to_pylist() == [1, 2]
    assert sorted(target.nodes) == ["absorbed", "more", "s"]
    kept: Any = target.sources["s"]
    assert kept.source_name == "s"


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="pre-existing (main: a <label>:<hash> leaf): reopening a job after a bind "
    "does not find the rebound nodes as held; to be filed",
)
def test_reopening_a_job_after_a_bind_adds_only_the_new_step() -> None:
    """Finding 4, same job (pre-existing): after ``job.bind`` the job's own nodes have
    new identities, so a step over them is not seen as built on held steps."""
    job, _ = _new_job()
    job.register(doubled_x.pod(_two_rows("s", 1), label="double"))
    job.bind(sources={"s": _two_rows("new", 10)})
    job.register(MapData({"f": "g"})(job.double, label="tail"))
    assert sorted(job.nodes) == ["SelectDataColumns", "double", "s", "tail"]


@pytest.mark.parametrize(
    "form",
    ["in_block", "auto_compile_off", "named_then_positional", "positional_then_named"],
)
def test_a_keyword_name_is_exact_in_every_form(form: str) -> None:
    """Finding 5: a keyword name never takes a suffix: another step labelled the same
    is refused, in a block, with ``auto_compile=False`` and across two calls in either
    order."""
    source = _two_rows("s", 1)
    named = source.select_data_columns(["x"])
    other = source.select_data_columns(["y"], label="exact")
    job = PipelineJob(
        name="j",
        database=InMemoryArrowDatabase(),
        auto_compile=form != "auto_compile_off",
    )
    with pytest.raises(ValueError, match="'exact'"):
        if form == "in_block":
            with job:
                job.register(other, exact=named)
        elif form == "auto_compile_off":
            job.register(other, exact=named)
        elif form == "named_then_positional":
            job.register(exact=named)
            job.register(other)
        else:
            job.register(other)
            job.register(exact=named)
    if form == "named_then_positional":
        assert sorted(job.nodes) == ["exact", "s"]
        assert job.exact.content_hash() == job.nodes["exact"].content_hash()


@pytest.mark.parametrize("held_by", ["positional", "keyword"])
def test_registering_held_steps_again_changes_nothing(held_by: str) -> None:
    """Finding 6: registering a stream whose steps the job holds is a no-op: the
    nodes, their objects and a held run stay (a side-effect node keeps its rows)."""
    delivered: list[int] = []

    def deliver(x: int, ctx: Any) -> None:
        delivered.append(x)

    picked = _two_rows("s", 1).select_data_columns(["x"], label="old")
    end = SideEffectPod(deliver)(picked, label="side")
    job, _ = _new_job()
    if held_by == "keyword":
        job.register(new=picked)
    job.register(end)
    job.run()
    before = _state_of(job)
    held_rows = len(job.side.as_table())
    job.register(end)
    job.register(picked)
    assert _state_of(job) == before
    assert len(job.side.as_table()) == held_rows == 2
    assert job.last_run is not None


@pytest.mark.parametrize("labels", [("first", "second"), ("second", "first")])
def test_one_step_built_twice_in_a_block_keeps_the_smallest_label(
    labels: tuple[str, str],
) -> None:
    """The same step keeps the smallest explicit label in either order (ITL-1436)."""
    source = _two_rows("s", 1)
    job, _ = _new_job()
    with job:
        first = source.select_data_columns(["x"], label=labels[0])
        source.select_data_columns(["x"], label=labels[1])
        MapData({"x": "z"})(first)
    assert sorted(job.nodes) == ["MapData", "first", "s"]


def test_an_exact_keyword_name_survives_a_smaller_label_in_the_block() -> None:
    """A keyword name remains exact when the block records the same step again."""
    source = _two_rows("s", 1)
    step = source.select_data_columns(["x"])
    job, _ = _new_job()
    with job:
        job.register(z=step)
        source.select_data_columns(["x"], label="a")
    assert list(job.nodes) == ["s", "z"]


@pytest.mark.parametrize("reverse", [False, True])
def test_equal_end_steps_check_labels_on_both_paths(reverse: bool) -> None:
    """Equal downstream hashes can hide different labels on the upstream objects."""
    source = _two_rows("s", 1)
    left = source.select_data_columns(["x"], label="left")
    right = source.select_data_columns(["x"], label="right")
    rename = MapData({"x": "renamed"})
    ends = (rename(left), rename(right))
    assert ends[0].content_hash() == ends[1].content_hash()
    job, _ = _new_job()
    with pytest.raises(ValueError, match=r"two labels.*(left.*right|right.*left)"):
        job.register(*ends[::-1] if reverse else ends)
    assert job.nodes == {}


def test_keyword_registration_reserves_a_held_positional_name() -> None:
    """Reserving a held name keeps the compiled node and its completed run."""
    source = _two_rows("s", 1)
    chosen = source.select_data_columns(["x"], label="chosen")
    job, _ = _new_job()
    job.register(chosen)
    run = job.run()
    node = job.chosen
    job.register(chosen=chosen)
    assert job.chosen is node
    assert job.last_run is run
    with pytest.raises(ValueError, match="'chosen'"):
        job.register(source.select_data_columns(["y"], label="chosen"))
    assert job.chosen is node


def test_a_refused_block_restores_a_rebound_slot_and_its_current_results() -> None:
    """A refused block restores the slot's identity and existing result freshness."""
    source = _two_rows("s", 1)
    replacement = _two_rows("replacement", 10)
    job, _ = _new_job()
    job.register(doubled_x.pod(source, label="double"))
    job.run()
    slot = job.s
    original_hash = slot.content_hash()
    original_modified = slot.last_modified
    original_rows = job.double.as_table().to_pylist()
    assert original_rows
    with pytest.raises(ValueError, match="refuse the block"):
        with job:
            job.bind(sources={"s": replacement})
            assert slot.bound_source is replacement
            assert slot.content_hash() != original_hash
            raise ValueError("refuse the block")
    assert job.s is slot
    assert job.sources["s"] is source
    assert slot.bound_source is source
    assert slot.content_hash() == original_hash
    assert slot.last_modified == original_modified
    assert job.double.as_table().to_pylist() == original_rows


@pytest.mark.parametrize("assigned_first", [False, True])
def test_an_assigned_label_wins_over_none_in_either_order(assigned_first: bool) -> None:
    """Finding 7: one step registered once unlabelled and once labelled takes the
    label, in either order."""
    source = _two_rows("s", 1)
    unlabelled = source.select_data_columns(["x"])
    assigned = source.select_data_columns(["x"], label="assigned")
    job, _ = _new_job()
    job.register(
        *((assigned, unlabelled) if assigned_first else (unlabelled, assigned))
    )
    assert sorted(job.nodes) == ["assigned", "s"]


def _loaded_job(tmp_path: Path) -> PipelineJob:
    db = DeltaTableDatabase(tmp_path / "db")
    (tmp_path / "a.csv").write_text("id,x\n1,1\n2,2\n")
    source = op.sources.CSVSource(
        str(tmp_path / "a.csv"),
        tag_columns=["id"],
        tag_only_columns=True,
        source_id="s",
    )
    job = PipelineJob(name="j", database=db)
    with job:
        doubled_x.pod(source, label="saved")
    job.run()
    job.save(tmp_path / "j.json")
    return PipelineJob.load(tmp_path / "j.json", database=db)


def test_register_on_a_loaded_job_is_refused(tmp_path: Path) -> None:
    """Finding 8: a loaded job is read-only; ``register`` refuses and keeps its nodes."""
    loaded = _loaded_job(tmp_path)
    before = sorted(loaded.nodes)
    with pytest.raises(ValueError, match="loaded job is read-only"):
        loaded.register(_two_rows("t", 1).select_data_columns(["x"], label="new"))
    assert sorted(loaded.nodes) == before


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="pre-existing (main too): a with block on a loaded job replaces its nodes; "
    "to be filed",
)
def test_a_block_on_a_loaded_job_keeps_its_nodes(tmp_path: Path) -> None:
    """Finding 8 (pre-existing): reopening a loaded job with a block."""
    loaded = _loaded_job(tmp_path)
    with loaded:
        _two_rows("t", 1).select_data_columns(["x"], label="new")
    assert {"s", "saved"} <= set(loaded.nodes)


@pytest.mark.parametrize("target", ["Pipeline", "PipelineJob"])
def test_a_pipeline_that_takes_in_a_job_node_keeps_the_slot_name(target: str) -> None:
    """Finding 9: a job's slot keeps its name when another pipeline or job takes in
    its nodes, so the blueprint binds by that name."""
    other, _ = _new_job("other")
    source = _two_rows("s", 1)
    with other:
        MapData({"x": "x2"})(source, label="mapped")
    if target == "Pipeline":
        blueprint = op.Pipeline(name="bp")
        blueprint.register(other.mapped)
        assert sorted(blueprint.nodes) == ["mapped", "s"]
        job = PipelineJob.from_pipeline(blueprint, sources={"s": source})
    else:
        job, _ = _new_job()
        job.register(other.mapped)
    assert sorted(job.nodes) == ["mapped", "s"]


@pytest.mark.parametrize("given_as", ["positional", "keyword"])
def test_a_stream_that_is_neither_a_source_nor_made_by_a_pod_is_refused(
    given_as: str,
) -> None:
    """Finding 10: a raw stream given to ``register`` is refused; it used to leave an
    empty compiled job."""
    raw = ArrowTableStream(pa.table({"id": [1, 2], "x": [3, 4]}), tag_columns=["id"])
    job, _ = _new_job()
    with pytest.raises(ValueError, match="neither a source nor made by a pod"):
        if given_as == "positional":
            job.register(raw)
        else:
            job.register(name=raw)
    assert job.nodes == {}


def test_a_job_saved_with_an_implicit_slot_still_loads(tmp_path: Path) -> None:
    """Review round 1, saved form: a job saved with a ``<label>:<hash>`` slot (the old
    implicit leaf, made here explicitly) loads with that slot unbound, its downstream
    node's stored view readable, and the slot readable once bound (spec 9.4)."""
    source = _two_rows("s", 1)
    pre = source.select_data_columns(["x"], label="pre")
    name = f"pre:{pre.content_hash().to_hex()}"
    db = DeltaTableDatabase(tmp_path / "db")
    job = PipelineJob(name="legacy", database=db)
    with job:
        doubled_x.pod(SourceJobNode.from_stream(pre, name=name), label="double")
    job.run()
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(tmp_path / "job.json", database=db)
    slot: Any = loaded.nodes[name]
    assert slot.bound_source is None
    assert loaded.double.as_table().num_rows == 0  # the current run: none
    assert sorted(loaded.double.as_source().as_table()["f"].to_pylist()) == [2, 4]
    loaded.bind(sources={name: pre})
    assert sorted(loaded.nodes[name].as_table()["x"].to_pylist()) == [1, 2]


@pytest.mark.parametrize("way", ["register", "block"])
def test_one_stream_given_twice_to_a_merge_join_is_refused_as_by_hand(way: str) -> None:
    """Review round 1 (pre-existing, pinned): ``MergeJoin(s, s)`` does not run, by hand
    as when registered; a change to that is noticed here."""
    from orcapod.core.operators import MergeJoin

    picked = _two_rows("s", 1).select_data_columns(["x"])
    job, _ = _new_job()
    if way == "register":
        job.register(MergeJoin()(picked, picked, label="self_merge"))
    else:
        with job:
            MergeJoin()(picked, picked, label="self_merge")
    with pytest.raises(ValueError):
        job.run()


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_prebuilt_side_effect_stream_delivers_when_the_job_runs(mode: Mode) -> None:
    """Review round 1, behaviour change: a side-effect stream built before a block comes
    in as a side-effect node: recording delivers nothing, the job's run delivers each
    row once."""
    delivered: list[int] = []

    def deliver(x: int, ctx: Any) -> None:
        delivered.append(x)

    # A side-effect stream is a stream; its class is not typed as one (ITL-1387).
    sink: Any = SideEffectPod(deliver)(
        _two_rows("s", 1).select_data_columns(["x"]), label="sent"
    )
    job, _ = _new_job()
    with job:
        MapData({"x": "z"})(sink, label="after")
    assert delivered == []
    assert sorted(job.nodes) == ["SelectDataColumns", "after", "s", "sent"]
    _run(job, mode)
    assert sorted(delivered) == [1, 2]
