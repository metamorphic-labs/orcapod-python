"""Graph walks over stacked split-and-join stages are linear in the nodes (ITL-1111).

Each stage splits a stream into two branches and joins them again, so every
stage is a diamond over the previous one. A walk that asks a shared upstream
once per path visits the source 2**stages times or more; a walk that keeps
what it has computed asks each node a bounded number of times.

The tests count calls of the schema queries (``output_schema`` and ``keys``)
and of the other graph walks (``__repr__``, ``_schema_requires_io``) on every
stream, source and node class, per object, never wall-clock time. A
counter stops the walk with ``WalkBudgetExceeded`` as soon as the total passes
a budget linear in the node count, so an exponential walk fails fast.

Walks over the system tags (``all_info``, a run) are tested at fewer stages:
every join keeps both inputs' system-tag columns, so a stage doubles their
number, and at ``STAGES`` a row would be millions of columns wide. That width is
the output, not a walk; only the number of schema queries per node is asserted.
"""

from __future__ import annotations

import collections
import functools
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.sources import DictSource
from orcapod.core.streams.base import StreamBase
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

STAGES = 20

# Stages for the walks whose output carries the system tags (2 ** (stages + 1)
# system-tag columns).
SYSTEM_TAG_STAGES = 8

# Upper bound on the schema queries one node may receive in one walk.
MAX_CALLS_PER_NODE = 30

# The schema queries, plus the two other walks found at construction (ITL-1111):
# ``__repr__`` nests every upstream (it was formatted eagerly into a debug log on
# every invocation), and the private ``_schema_requires_io`` hook, counted because
# ``head()`` and the preview walk the graph through it.
_COUNTED_METHODS = ("output_schema", "keys", "__repr__", "_schema_requires_io")


class WalkBudgetExceeded(AssertionError):
    """More schema queries than the linear budget allows: the walk revisits nodes."""


def _all_stream_classes() -> set[type]:
    seen: set[type] = set()
    pending: list[type] = [StreamBase]
    while pending:
        cls = pending.pop()
        if cls in seen:
            continue
        seen.add(cls)
        pending.extend(cls.__subclasses__())
    return seen


class _CallCounter:
    def __init__(self, budget: int) -> None:
        self.budget = budget
        self.per_object: collections.Counter[int] = collections.Counter()
        self.total = 0

    def record(self, obj: object) -> None:
        self.per_object[id(obj)] += 1
        self.total += 1
        if self.total > self.budget:
            raise WalkBudgetExceeded(
                f"more than {self.budget} schema queries: the walk revisits nodes"
            )

    @property
    def max_per_object(self) -> int:
        return max(self.per_object.values(), default=0)


@contextmanager
def _count_schema_queries(
    monkeypatch: pytest.MonkeyPatch, budget: int
) -> Iterator[_CallCounter]:
    """Count ``output_schema`` / ``keys`` calls on every stream class, per object."""
    counter = _CallCounter(budget)

    def wrap(method: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(method)
        def counted(self: object, *args: Any, **kwargs: Any) -> Any:
            counter.record(self)
            return method(self, *args, **kwargs)

        return counted

    with monkeypatch.context() as patch:
        for cls in _all_stream_classes():
            for name in _COUNTED_METHODS:
                if name in cls.__dict__:
                    patch.setattr(cls, name, wrap(cls.__dict__[name]))
        yield counter


@function_pod(output_keys="a")
def _branch_a(x: int) -> int:
    return x + 1


@function_pod(output_keys="b")
def _branch_b(x: int) -> int:
    return x + 2


@function_pod(output_keys="x")
def _merge(a: int, b: int) -> int:
    return a + b


def _operator_stage(stream: Any) -> Any:
    a = stream.map_data({"x": "a"})
    b = stream.map_data({"x": "b"})
    joined = a.join(b)
    return joined.map_data({"a": "x"}).drop_data_columns("b")


def _function_pod_stage(stream: Any) -> Any:
    return _merge.pod(_branch_a.pod(stream), _branch_b.pod(stream))


# shape -> (stage, nodes per stage)
SHAPES: dict[str, tuple[Callable[[Any], Any], int]] = {
    "operators": (_operator_stage, 5),
    "function_pods": (_function_pod_stage, 3),
}


def _node_count(shape: str, stages: int) -> int:
    return SHAPES[shape][1] * stages + 1


def _budget(shape: str, stages: int = STAGES) -> int:
    return MAX_CALLS_PER_NODE * _node_count(shape, stages)


def _source() -> DictSource:
    return DictSource(
        [{"id": i, "x": i} for i in range(3)], tag_columns=["id"], source_id="src"
    )


def _build(shape: str, stages: int = STAGES) -> Any:
    stage, _ = SHAPES[shape]
    stream = _source()
    for _ in range(stages):
        stream = stage(stream)
    return stream


def _build_job(shape: str, stages: int = STAGES) -> PipelineJob:
    job = PipelineJob(name="stacked", database=InMemoryArrowDatabase())
    with job:
        _build(shape, stages)
    return job


def _guarded_build(
    shape: str, monkeypatch: pytest.MonkeyPatch, stages: int = STAGES
) -> Any:
    """Build the stream graph, failing fast if building is not linear."""
    with _count_schema_queries(monkeypatch, _budget(shape, stages)):
        return _build(shape, stages)


def _guarded_build_job(
    shape: str, monkeypatch: pytest.MonkeyPatch, stages: int = STAGES
) -> PipelineJob:
    """Record and compile the job, failing fast if that is not linear."""
    with _count_schema_queries(monkeypatch, _budget(shape, stages)):
        return _build_job(shape, stages)


def _assert_linear(counter: _CallCounter, shape: str, stages: int = STAGES) -> None:
    assert counter.max_per_object <= MAX_CALLS_PER_NODE
    assert counter.total <= _budget(shape, stages)


@pytest.fixture(params=sorted(SHAPES))
def shape(request: pytest.FixtureRequest) -> str:
    return request.param


class TestStreams:
    def test_building_the_graph_is_linear(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with _count_schema_queries(monkeypatch, _budget(shape)) as counter:
            _build(shape)
        _assert_linear(counter, shape)

    @pytest.mark.parametrize(
        "walk",
        [
            pytest.param(lambda s: s.output_schema(), id="output_schema"),
            pytest.param(lambda s: s.keys(), id="keys"),
            pytest.param(lambda s: s.head(), id="head"),
            # The notebook preview is the Jupyter display hook.
            pytest.param(lambda s: s._repr_html_(), id="preview"),
        ],
    )
    def test_a_schema_walk_is_linear(
        self,
        shape: str,
        walk: Callable[[Any], Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stream = _guarded_build(shape, monkeypatch)
        with _count_schema_queries(monkeypatch, _budget(shape)) as counter:
            walk(stream)
        _assert_linear(counter, shape)

    def test_the_system_tag_schema_walk_is_linear(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stream = _guarded_build(shape, monkeypatch, SYSTEM_TAG_STAGES)
        budget = _budget(shape, SYSTEM_TAG_STAGES)
        with _count_schema_queries(monkeypatch, budget) as counter:
            stream.output_schema(all_info=True)
        _assert_linear(counter, shape, SYSTEM_TAG_STAGES)

    def test_hashes_are_unchanged_by_asking_twice(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        first = _guarded_build(shape, monkeypatch)
        with _count_schema_queries(monkeypatch, _budget(shape)):
            first.output_schema()
        second = _guarded_build(shape, monkeypatch)
        assert first.content_hash() == second.content_hash()
        assert first.pipeline_hash() == second.pipeline_hash()
        assert first.output_schema() == second.output_schema()


class TestPipelineJob:
    def test_recording_and_compiling_is_linear(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with _count_schema_queries(monkeypatch, _budget(shape)) as counter:
            _build_job(shape)
        _assert_linear(counter, shape)

    def test_save_is_linear(
        self, shape: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        job = _guarded_build_job(shape, monkeypatch)
        with _count_schema_queries(monkeypatch, _budget(shape)) as counter:
            job.save(tmp_path / "job.json")
        _assert_linear(counter, shape)

    def test_load_is_linear(
        self, shape: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        job = _guarded_build_job(shape, monkeypatch)
        with _count_schema_queries(monkeypatch, _budget(shape)):
            job.save(tmp_path / "job.json")
        with _count_schema_queries(monkeypatch, _budget(shape)) as counter:
            PipelineJob.load(tmp_path / "job.json")
        _assert_linear(counter, shape)

    @pytest.mark.parametrize(
        "orchestrator",
        [
            pytest.param(SyncPipelineOrchestrator, id="sync"),
            pytest.param(AsyncPipelineOrchestrator, id="async"),
        ],
    )
    def test_run_is_linear(
        self,
        shape: str,
        orchestrator: type,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        job = _guarded_build_job(shape, monkeypatch, SYSTEM_TAG_STAGES)
        budget = _budget(shape, SYSTEM_TAG_STAGES)
        with _count_schema_queries(monkeypatch, budget) as counter:
            job.run(orchestrator=orchestrator())
        _assert_linear(counter, shape, SYSTEM_TAG_STAGES)

    def test_a_node_schema_walk_is_linear(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        job = _guarded_build_job(shape, monkeypatch)
        last = list(job.nodes.values())[-1]
        with _count_schema_queries(monkeypatch, _budget(shape)) as counter:
            last.output_schema()
            last.keys()
        _assert_linear(counter, shape)


# ---------------------------------------------------------------------------
# A memoized schema is predicted again when an input of the prediction changes
# ---------------------------------------------------------------------------


def _one_row_source(value: int | float | str) -> DictSource:
    return DictSource([{"id": 1, "x": value}], tag_columns=["id"], source_id="src")


def _rename_job() -> PipelineJob:
    job = PipelineJob(name="rebind", database=InMemoryArrowDatabase())
    with job:
        renamed = _one_row_source(1).map_data({"x": "y"}, label="renamed")
        renamed.map_data({"y": "z"}, label="renamed_again")
    return job


def _function_job() -> PipelineJob:
    job = PipelineJob(name="rebind_function", database=InMemoryArrowDatabase())
    with job:
        _branch_a.pod(_one_row_source(1), label="plus_one")
    return job


class TestMemoInvalidation:
    def test_bind_to_a_source_of_another_column_type_changes_downstream_schemas(
        self,
    ) -> None:
        job = _rename_job()
        assert job.nodes["renamed"].output_schema()[1]["y"] is int
        assert job.nodes["renamed_again"].output_schema()[1]["z"] is int
        assert job.nodes["renamed_again"].keys() == (("id",), ("z",))

        job.bind(sources={"src": _one_row_source("a")})

        assert job.nodes["renamed"].output_schema()[1]["y"] is str
        assert job.nodes["renamed_again"].output_schema()[1]["z"] is str

    def test_setting_an_operator_nodes_upstreams_changes_its_schema(self) -> None:
        job = _rename_job()
        node = job.nodes["renamed"]
        assert node.output_schema()[1]["y"] is int

        node.upstreams = (_one_row_source(1.5),)

        assert node.output_schema()[1]["y"] is float

    def test_setting_a_function_nodes_upstreams_changes_its_schema(self) -> None:
        job = _function_job()
        node = job.nodes["plus_one"]
        assert node.output_schema()[0]["id"] is int
        other = DictSource(
            [{"id": "one", "x": 1}], tag_columns=["id"], source_id="other"
        )

        node.upstreams = (other,)

        assert node.output_schema()[0]["id"] is str

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "A direct bound_source assignment reaches no downstream node, so their "
            "memoized schemas are not predicted again; use job.bind (ITL-1111). "
            "Their pipeline hashes are equally stale today."
        ),
    )
    def test_assigning_bound_source_directly_changes_downstream_schemas(
        self,
    ) -> None:
        job = _rename_job()
        assert job.nodes["renamed_again"].output_schema()[1]["z"] is int

        job.nodes["src"].bound_source = _one_row_source("a")

        assert job.nodes["renamed_again"].output_schema()[1]["z"] is str
