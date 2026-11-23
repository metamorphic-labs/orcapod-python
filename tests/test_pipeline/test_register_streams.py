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
from tests.test_core.operators.several_items import (
    DATA_TAGS,
    MAIN,
    MIXED,
    PLAIN,
    TAGS_FIRST,
    P,
    Q,
    Arrangement,
    make_source,
)
from tests.test_core.test_order_invariance import OPERATOR_CASES, OperatorCase

# Stored columns whose values differ between two runs of one pipeline: run times, each
# stored row's random identifier (a uuid7, not a hash), and ``__computed``: which of two
# nodes sharing one function's result table computes a row and which reads it follows
# the run order of independent nodes, which is not specified (it varies with the hash
# seed for one hand-written job alone).
_RUN_DEPENDENT = {"__pod_ts", "__data_id", "__computed"}
# A result's random record id inside a source-info token differs on every run.
_RANDOM_ID = re.compile(r"::[0-9a-f]{32}::")

Mode = str  # "sync" or "async"


def _rows(table: pa.Table) -> list[str]:
    """A table's rows as sorted text, without the run-dependent columns."""
    keep = [name for name in table.column_names if name not in _RUN_DEPENDENT]
    return sorted(
        _RANDOM_ID.sub("::<id>::", repr(sorted(row.items())))
        for row in table.select(keep).to_pylist()
    )


def _run(job: PipelineJob, mode: Mode) -> None:
    if mode == "sync":
        job.run()
    else:
        asyncio.run(job.run_async())


def _saved(job: PipelineJob, tmp_path: Path, name: str) -> dict[str, Any]:
    path = tmp_path / f"{name}.json"
    job.save(path)
    saved = json.loads(path.read_text())
    pipeline = dict(saved["pipeline"])
    # The edge list follows the order the steps were recorded in, which the file
    # does not promise (a load reads it as a set).
    pipeline["edges"] = sorted(map(json.dumps, pipeline["edges"]))
    return {"pipeline": pipeline, "bindings": saved["bindings"]}


def fingerprint(job: PipelineJob, db: Any, tmp_path: Path, name: str) -> dict[str, Any]:
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
    tables = {"/".join(path): _rows(db.get_all_records(path)) for path in paths}
    results = {
        label: _rows(node.as_table(columns={"system_tags": True, "source": True}))
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
@pytest.mark.parametrize("role", [PLAIN, MIXED])
@pytest.mark.parametrize("case", OPERATOR_PARAMS)
def test_every_operator_by_register_and_block_equals_hand_written(
    case: OperatorCase, role: str, mode: Mode, tmp_path: Path
) -> None:
    """Each exported operator, after one step over each input: registered (the end
    stream alone, and with the prepared streams in two orders) and recorded in a block
    over prepared streams built before it, against the same steps written by hand."""
    if role not in case.roles:
        pytest.skip(f"{case.label} does not take {role} tags")
    hand, hand_db = _new_job()
    with hand:
        _apply(case, _prepare(case.sources(role, 0, TAGS_FIRST)))
    _run(hand, mode)
    expected = fingerprint(hand, hand_db, tmp_path, "hand")

    def registered(order: Callable[[Any, list[Any]], list[Any]]) -> None:
        prepared = _prepare(case.sources(role, 0, TAGS_FIRST))
        end = _apply(case, prepared)
        job, db = _new_job()
        job.register(*order(end, prepared))
        _run(job, mode)
        _assert_same(expected, fingerprint(job, db, tmp_path, "reg"), "register")

    registered(lambda end, prepared: [end])
    registered(lambda end, prepared: [end, *prepared])
    registered(lambda end, prepared: [*reversed(prepared), end])

    prepared = _prepare(case.sources(role, 0, TAGS_FIRST))
    job, db = _new_job()
    with job:
        _apply(case, prepared)
    _run(job, mode)
    _assert_same(expected, fingerprint(job, db, tmp_path, "block"), "with block")


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
    expected = fingerprint(hand, hand_db, tmp_path, "hand")
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
        _assert_same(expected, fingerprint(job, db, tmp_path, "reg"), f"order {order}")


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
    expected = fingerprint(hand, hand_db, tmp_path, "hand")

    streams = _forms(_sources(), InMemoryArrowDatabase())
    job, db = _new_job()
    with job:
        build(streams)
    job.register(streams["scaled"], streams["shifted"])
    with job:
        pass
    job.register(fell=streams["fell"], cached=streams["cached"])
    _run(job, mode)
    _assert_same(expected, fingerprint(job, db, tmp_path, "mixed"), "block + register")


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
    expected = fingerprint(hand, hand_db, tmp_path, "hand")

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
    _assert_same(expected, fingerprint(job, db, tmp_path, "absorbed"), how)
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
