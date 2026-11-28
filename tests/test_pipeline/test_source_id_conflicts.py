"""Source registration and source ID conflicts (ITL-1469)."""

from __future__ import annotations

import itertools
import json
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod import Pipeline, PipelineJob, function_pod
from orcapod.core.function_pod import _FunctionPodBase
from orcapod.core.sources.base import RootSource
from orcapod.core.sources.source_proxy import SourceProxy
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import (
    InconsistentSourceError,
    SourceSlotNameCollisionError,
)
from orcapod.operators import SemiJoin
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.source_consistency import require_consistent_sources
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.sources import (
    ArrowTableSource,
    CSVSource,
    DataFrameSource,
    DerivedSource,
    DictSource,
    ListSource,
)

from ..test_core.operators.several_items import (
    DATA_FIRST_REVERSED,
    MIXED,
    P,
    PLAIN,
    ROLES,
    TAGS_FIRST,
    Arrangement,
    make_table,
)


@function_pod(output_keys="out")
def increment(x: int) -> int:
    return x + 1


@function_pod(output_keys="out")
def increment_again(x: int) -> int:
    return x + 2


@function_pod(output_keys="out")
def both(x: int, y: int) -> int:
    return x + y


@function_pod(output_keys="out")
def with_pair(p1: int, p2: str) -> int:
    return p1 + len(p2)


@function_pod(output_keys="out")
def with_copy(x: int, tag_value: str) -> int:
    return x + len(tag_value)


@function_pod(output_keys="out")
def with_string(x: str) -> int:
    return len(x)


@function_pod(output_keys="next")
def consume_output(out: int) -> int:
    return out * 2


def _dict(x: int, name: str = "shared") -> DictSource:
    return DictSource(data=[{"x": x}], source_id=name)


def _refused(
    first: RootSource,
    second: RootSource,
    graph: Pipeline | PipelineJob,
    pod: _FunctionPodBase = increment.pod,
) -> None:
    with graph:
        pod(first, label="fa")
        with pytest.raises(
            InconsistentSourceError, match="distinct source objects"
        ) as error:
            pod(second, label="fb")
    for part in (first.source_id, "fa", "fb", "reuse"):
        assert part in str(error.value)
    assert "fb" not in graph.nodes


@pytest.mark.parametrize("kind", ["arrow", "dict", "frame"])
@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_in_memory_sources_register_by_object(
    kind: str, role: str, reverse: bool, orchestrator: type
) -> None:
    def source(row: int, arrangement: Arrangement) -> RootSource:
        table = make_table(P, arrangement, rows=[row])
        tag_only = True if role == PLAIN else [P.mixed_plain] if role == MIXED else []
        if kind == "arrow":
            return ArrowTableSource(
                table,
                tag_columns=arrangement.tag_order,
                tag_only_columns=tag_only,
                source_id="shared",
            )
        cls = DictSource if kind == "dict" else DataFrameSource
        return cls(
            data=table.to_pylist(),
            tag_columns=arrangement.tag_order,
            tag_only_columns=tag_only,
            source_id="shared",
        )

    arrangements = [
        Arrangement(P.tags, TAGS_FIRST, role),
        Arrangement(tuple(reversed(P.tags)), DATA_FIRST_REVERSED, role),
    ]
    first, second = arrangements[::-1] if reverse else arrangements
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        with_pair.pod(source(0, first), label="fa")
        with pytest.raises(InconsistentSourceError, match="distinct source objects"):
            with_pair.pod(source(0, second), label="same")
        with pytest.raises(InconsistentSourceError, match="distinct source objects"):
            with_pair.pod(source(1, second), label="conflict")
    assert set(job.function_pods) == {"fa"}
    job.run(orchestrator=orchestrator())
    assert job.fa.as_table()["out"].to_pylist() == [8]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_list_sources_register_by_object(reverse: bool, orchestrator: type) -> None:
    def source(x: int, tag_order: tuple[str, ...]) -> ListSource:
        tags = {"t1": "a", "t2": "b"}
        return ListSource(
            "x",
            [x],
            tag_function=lambda _x, _i: {name: tags[name] for name in tag_order},
            source_id="shared",
        )

    tags = ("t2", "t1") if reverse else ("t1", "t2")
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        increment.pod(source(1, tags), label="fa")
        with pytest.raises(InconsistentSourceError, match="distinct source objects"):
            increment.pod(source(1, tags[::-1]), label="same")
        with pytest.raises(InconsistentSourceError, match="distinct source objects"):
            increment.pod(source(100, tags[::-1]), label="conflict")
    assert set(job.function_pods) == {"fa"}
    job.run(orchestrator=orchestrator())
    assert job.fa.as_table()["out"].to_pylist() == [2]


def test_proxy_and_delegate_register_as_distinct_objects() -> None:
    right = _dict(100)
    wrapper = SourceProxy(
        right.source_id,
        right.content_hash().to_string(),
        right.pipeline_hash().to_string(),
        *right.output_schema(),
    )
    wrapper.bind(right)
    assert wrapper.content_hash() == right.content_hash()
    _refused(right, wrapper, PipelineJob())


@pytest.mark.parametrize("value", [1, 100])
def test_bound_proxy_uses_its_live_arrow_schema_for_collision_checks(
    value: int,
) -> None:
    def source(x: int, keys: str | None = None) -> ArrowTableSource:
        return ArrowTableSource(
            pa.table({"x": pa.array([x], type=pa.int32())}),
            infer_nullable=True,
            source_id="shared",
            record_id_columns=keys,
        )

    first, delegate = source(1), source(value, "x")
    proxy = SourceProxy(
        delegate.source_id,
        delegate.content_hash().to_string(),
        delegate.pipeline_hash().to_string(),
        *delegate.output_schema(),
    )
    # An unbound proxy knows only the Python type, which defaults to int64.
    assert proxy.arrow_schemas[1].field("x").type == pa.int64()
    proxy.bind(delegate)

    assert first.content_hash() != proxy.content_hash()
    message = _capture(first, proxy, increment.pod)
    assert "different key columns" in message
    assert "different schemas" not in message


def test_implicit_projection_conflict_names_both_function_steps() -> None:
    first = DictSource([{"x": 1, "y": 2}], source_id="shared")
    second = DictSource([{"x": 100, "y": 2}], source_id="shared")
    _refused(first, second, PipelineJob())


def test_derived_from_fixed_sources() -> None:
    def derived(x: int) -> DerivedSource:
        origin = DictSource(
            [{"t": "a", "x": x}],
            tag_columns="t",
            tag_only_columns=True,
            source_id="origin",
        )
        return DerivedSource(
            origin, tags_as_data={"t": "tag_value"}, source_id="shared"
        )

    _refused(derived(1), derived(100), PipelineJob(), with_copy.pod)


def test_same_name_across_kinds_still_collides_at_capture_but_can_bind_separate_slots() -> (
    None
):
    table = ArrowTableSource(
        pa.table({"x": [100]}), infer_nullable=True, source_id="shared"
    )
    dictionary = _dict(1)
    with pytest.raises(SourceSlotNameCollisionError):
        with Pipeline("conflicts"):
            increment.pod(dictionary, label="fa")
            increment.pod(table, label="fb")
    job = _job()
    job.bind(sources={"left": dictionary, "right": table})
    assert job.sources["left"] is dictionary
    assert job.sources["right"] is table


@pytest.mark.parametrize("reverse", [False, True])
def test_same_source_id_with_different_key_columns_is_refused_at_build(
    tmp_path: Path, reverse: bool
) -> None:
    path = tmp_path / "shared.csv"
    path.write_text("t1,t2,x,y\na,b,1,2\nc,d,3,4\n")
    sources = [
        CSVSource(
            str(path),
            source_id="shared",
            tag_columns=["t1", "t2"],
            tag_only_columns=True,
            record_id_columns=keys,
        )
        for keys in (["t1", "x"], ["t2", "y"])
    ]
    first, second = sources[::-1] if reverse else sources
    assert first.source_id == second.source_id
    assert first.arrow_schemas == second.arrow_schemas
    assert first.content_hash() != second.content_hash()

    with pytest.raises(InconsistentSourceError, match="ITL-1506"):
        with Pipeline("conflicts"):
            both.pod(first, label="fa")
            both.pod(second, label="fb")


@pytest.mark.parametrize("reverse", [False, True])
def test_bind_refuses_same_rows_with_different_keys(reverse: bool) -> None:
    # Refused in any slots for now: distinct slots for distinct readings of one
    # dataset are ITL-1506, after the multi-slot binding work (ITL-1442).
    positional = _dict(1)
    keyed = DictSource([{"x": 1}], record_id_columns="x", source_id="shared")
    pair = (keyed, positional) if reverse else (positional, keyed)
    job = _job()
    before = dict(job.sources)
    with pytest.raises(InconsistentSourceError) as error:
        job.bind(sources={"left": pair[0], "right": pair[1]})
    message = str(error.value)
    assert "different key columns" in message
    assert "ITL-1506" in message
    assert "slot 'left'" in message and "slot 'right'" in message
    assert dict(job.sources) == before


@pytest.mark.parametrize("reverse", [False, True])
def test_bind_refuses_different_rows_with_different_keys(reverse: bool) -> None:
    positional = _dict(1)
    other = DictSource([{"x": 100}], record_id_columns="x", source_id="shared")
    pair = (other, positional) if reverse else (positional, other)
    with pytest.raises(InconsistentSourceError, match="different key columns"):
        _job().bind(sources={"left": pair[0], "right": pair[1]})


@pytest.mark.parametrize("reverse", [False, True])
def test_bind_refuses_live_sources_with_different_keys(
    tmp_path: Path, reverse: bool
) -> None:
    path = tmp_path / "shared.csv"
    path.write_text("x\n1\n2\n")
    positional = CSVSource(str(path), source_id="shared")
    keyed = CSVSource(str(path), source_id="shared", record_id_columns="x")
    pair = (keyed, positional) if reverse else (positional, keyed)
    with pytest.raises(InconsistentSourceError) as error:
        _job().bind(sources={"left": pair[0], "right": pair[1]})
    message = str(error.value)
    assert "ITL-1506" in message


def _job() -> PipelineJob:
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        increment.pod(_dict(2, "left"), label="fa")
        increment.pod(_dict(3, "right"), label="fb")
    return job


@pytest.mark.parametrize("loaded", [False, True])
def test_bind_checks_all_prospective_slots_atomically(
    tmp_path: Path, loaded: bool
) -> None:
    job = _job()
    if loaded:
        path = tmp_path / "job.json"
        job.save(path)
        job = PipelineJob.load(path)
    first, second = _dict(1), _dict(100)
    before = dict(job.sources)
    with pytest.raises(
        InconsistentSourceError, match="distinct source objects"
    ) as error:
        job.bind(sources={"left": first, "right": second})
    for part in (first.source_id, "left", "right", "source_id"):
        assert part in str(error.value)
    assert job.sources == before
    job.bind(sources={"left": first})
    job.bind(sources={"left": second})
    assert job.sources["left"] is second
    with pytest.raises(InconsistentSourceError, match="distinct source objects"):
        job.bind(sources={"right": first})
    job.bind(sources={"right": second})
    assert job.sources["right"] is second
    with pytest.raises(InconsistentSourceError, match="distinct source objects"):
        job.bind(sources={"left": _dict(100)})


def test_from_pipeline_checks_the_initial_bind() -> None:
    with Pipeline("conflicts") as graph:
        increment.pod(_dict(2, "left"), label="fa")
        increment.pod(_dict(3, "right"), label="fb")
    with pytest.raises(InconsistentSourceError, match="distinct source objects"):
        PipelineJob.from_pipeline(
            graph, sources={"left": _dict(1), "right": _dict(100)}
        )


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_run_checks_slots_changed_outside_job_bind(orchestrator: type) -> None:
    job = _job()
    job.nodes["left"]._bind(_dict(1))
    job.nodes["right"]._bind(_dict(100))
    with pytest.raises(InconsistentSourceError, match="distinct source objects"):
        job.run(orchestrator=orchestrator())
    assert not job.fa.as_table().num_rows
    assert not job.fb.as_table().num_rows


def test_additive_recording_resumes_when_original_binding_is_restored() -> None:
    original = _dict(1)
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        increment.pod(original, label="fa")

    replacement = _dict(100)
    job.bind(sources={"shared": replacement})
    with job:
        with pytest.raises(InconsistentSourceError, match="not as captured"):
            consume_output.pod(job.fa, label="conflict")
    job.run()
    assert job.fa.as_table()["out"].to_pylist() == [101]

    job.bind(sources={"shared": original})
    with job:
        consume_output.pod(job.fa, label="next")
    job.run()
    assert "conflict" not in job.nodes
    assert job.fa.as_table()["out"].to_pylist() == [2]
    assert job.next.as_table()["next"].to_pylist() == [4]


def test_load_checks_schema_of_reconstructed_sources(tmp_path: Path) -> None:
    left, right = tmp_path / "left.csv", tmp_path / "right.csv"
    left.write_text("x\n1\n")
    right.write_text("x\ntext\n")
    with PipelineJob() as job:
        increment.pod(CSVSource(str(left), source_id="left"), label="fa")
        with_string.pod(CSVSource(str(right), source_id="right"), label="fb")
    path = tmp_path / "job.json"
    job.save(path)
    saved = json.loads(path.read_text())
    saved["bindings"]["sources"]["right"]["source_name"] = "left"
    path.write_text(json.dumps(saved))
    with pytest.raises(InconsistentSourceError, match="different schemas") as error:
        PipelineJob.load(path)
    for part in ("csv_v1:left", "slot 'left'", "slot 'right'", "source_id"):
        assert part in str(error.value)


def test_schema_conflicts_at_capture_and_bind() -> None:
    def source(kind: pa.DataType, name: str) -> ArrowTableSource:
        return ArrowTableSource(
            pa.table({"x": pa.array([1], type=kind)}),
            infer_nullable=True,
            source_id=name,
        )

    first, second = source(pa.int32(), "shared"), source(pa.int64(), "shared")
    with pytest.raises(InconsistentSourceError, match="different schemas"):
        with Pipeline("conflicts"):
            increment.pod(first, label="fa")
            increment.pod(second, label="fb")

    with PipelineJob() as job:
        increment.pod(source(pa.int32(), "left"), label="fa")
        increment.pod(source(pa.int64(), "right"), label="fb")
    with pytest.raises(InconsistentSourceError, match="different schemas") as error:
        job.bind(sources={"left": first, "right": second})
    for part in (first.source_id, "left", "right", "source_id"):
        assert part in str(error.value)


@pytest.mark.parametrize("graph_type", [Pipeline, PipelineJob])
def test_one_object_is_allowed_in_several_steps(
    graph_type: type[Pipeline] | type[PipelineJob],
) -> None:
    source = _dict(1)
    with graph_type("reuse") as job:
        increment.pod(source, label="fa")
        increment_again.pod(source, label="fb")
    assert set(job.nodes) == {"shared", "fa", "fb"}


def test_cache_and_its_source_register_as_distinct_objects() -> None:
    source = _dict(1)
    cached = source.cached(InMemoryArrowDatabase())
    assert cached.content_hash() == source.content_hash()
    _refused(source, cached, PipelineJob())
    _refused(cached, source.cached(InMemoryArrowDatabase()), PipelineJob())


def test_stored_derived_sources_register_by_object() -> None:
    def derived(x: int) -> DerivedSource:
        with PipelineJob(database=InMemoryArrowDatabase()) as job:
            increment.pod(_dict(x), label="result")
        job.run()
        return job.result.as_source()

    first, second = derived(1), derived(100)
    assert first.source_id == second.source_id
    assert first.as_table()["out"].to_pylist() == [2]
    assert second.as_table()["out"].to_pylist() == [101]

    @function_pod(output_keys="value")
    def downstream(out: int) -> int:
        return out + 1

    _refused(first, second, PipelineJob(), downstream.pod)


_ROWS = [
    {"t1": "a", "t2": "p", "x": 1, "y": 10},
    {"t1": "b", "t2": "q", "x": 2, "y": 20},
    {"t1": "c", "t2": "r", "x": 3, "y": 30},
]


def _keyed(
    rows: list[dict[str, str | int]],
    keys: list[str] | None,
    tags: list[str] | None = None,
) -> DictSource:
    return DictSource(
        rows,
        tag_columns=tags or ["t1", "t2"],
        tag_only_columns=True,
        record_id_columns=keys,
        source_id="shared",
    )


def _capture(first: RootSource, second: RootSource, pod: _FunctionPodBase) -> str:
    """Refused when the second step is captured, not later at compile."""
    graph = Pipeline("conflicts")
    with graph:
        _ = pod(first, label="fa")
        with pytest.raises(InconsistentSourceError) as error:
            _ = pod(second, label="fb")
    assert "fb" not in graph.nodes
    return str(error.value)


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "keys", [(["t1"], ["t2", "x"]), (None, ["t2", "t1"]), (["x", "y"], None)]
)
def test_capture_refuses_materialized_sources_with_different_key_columns(
    reverse: bool, keys: tuple[list[str] | None, list[str] | None]
) -> None:
    first = _keyed(_ROWS, keys[0])
    # Another row order and tag order: the same dataset, read with other keys.
    second = _keyed(list(reversed(_ROWS)), keys[1], tags=["t2", "t1"])
    if reverse:
        first, second = second, first
    assert first.content_hash() != second.content_hash()
    message = _capture(first, second, both.pod)
    assert "different key columns" in message
    assert "ITL-1506" in message
    assert "source_id" in message


@function_pod(output_keys="out")
def with_copies(x: int, y: int, c1: str, c2: str) -> int:
    return x + y + len(c1) + len(c2)


def _derived(
    origin: RootSource, mapping: dict[str, str], keys: list[str] | None
) -> DerivedSource:
    return DerivedSource(
        origin, tags_as_data=mapping, record_id_columns=keys, source_id="shared"
    )


# The second mapping changes the order. The third changes which column c2 copies.
_COPIES = {"t1": "c1", "t2": "c2"}
_COPIES_REVERSED = {"t2": "c2", "t1": "c1"}
_SECOND_COPY_CHANGED = {"t1": "c1", "t3": "c2"}
_ORIGIN_TEXT = "t1,t2,t3,x,y\na,p,u,1,10\nb,q,v,2,20\n"


def test_derived_sources_over_live_origins_with_other_mapping_and_keys(
    tmp_path: Path,
) -> None:
    path = tmp_path / "origin.csv"
    path.write_text(_ORIGIN_TEXT)
    origin = CSVSource(
        str(path),
        source_id="origin",
        tag_columns=["t1", "t2", "t3"],
        tag_only_columns=True,
    )
    for mapping in (_COPIES_REVERSED, _SECOND_COPY_CHANGED):
        first = _derived(origin, _COPIES, ["t1", "t2"])
        second = _derived(origin, mapping, ["t3", "t1"])
        assert first.arrow_schemas == second.arrow_schemas
        message = _capture(first, second, with_copies.pod)
        assert "different key columns" in message
        assert "ITL-1506" in message


def test_unknown_key_columns_never_hide_a_conflict_between_known_ones() -> None:
    # An unbound proxy of the same id and schema has unknown key columns: it neither
    # causes nor suppresses a refusal between the sources whose keys are known.
    positional = DictSource([{"x": 1}, {"x": 2}], source_id="shared")
    keyed = DictSource([{"x": 1}, {"x": 2}], record_id_columns="x", source_id="shared")
    proxy = SourceProxy(
        positional.source_id,
        _dict(0, "other").content_hash().to_string(),
        positional.pipeline_hash().to_string(),
        *positional.output_schema(),
    )
    assert proxy._resolved_key_columns is None  # pyright: ignore[reportPrivateUsage]  # unbound proxy state
    for uses in itertools.permutations(
        [("slot 'p'", proxy), ("slot 'a'", positional), ("slot 'b'", keyed)]
    ):
        with pytest.raises(InconsistentSourceError) as error:
            require_consistent_sources(uses)
        assert "different key columns" in str(error.value)
    require_consistent_sources([("slot 'p'", proxy), ("slot 'a'", positional)])
    require_consistent_sources([("slot 'p'", proxy), ("slot 'b'", keyed)])


@function_pod(output_keys="out")
def increment_matched(x: int) -> int:
    return x + 1


def _semi_join_job() -> PipelineJob:
    def source(name: str) -> DictSource:
        return DictSource(
            [{"k": "a", "x": 1}, {"k": "b", "x": 2}],
            tag_columns="k",
            tag_only_columns=True,
            source_id=name,
        )

    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        matched = SemiJoin()(source("left"), source("right"), label="matched")
        _ = increment_matched.pod(matched, label="fm")
    return job


def _rebind_outside_bind(job: PipelineJob, right_keys: str | None, x: int) -> None:
    # Inject invalid job state to test the run-time checks.
    job.nodes["left"]._bind(
        DictSource(
            [{"k": "a", "x": 1}, {"k": "b", "x": 2}],
            tag_columns="k",
            tag_only_columns=True,
            source_id="shared",
        )
    )
    job.nodes["right"]._bind(
        DictSource(
            [{"k": "a", "x": 1}, {"k": "b", "x": x}],
            tag_columns="k",
            tag_only_columns=True,
            record_id_columns=right_keys,
            source_id="shared",
        )
    )


@pytest.mark.parametrize("node", ["matched", "fm"])
@pytest.mark.parametrize(
    ("right_keys", "x", "message"),
    [("k", 2, "different key columns"), (None, 200, "distinct source objects")],
)
def test_node_run_checks_its_sources_like_job_run(
    node: str, right_keys: str | None, x: int, message: str
) -> None:
    job = _semi_join_job()
    _rebind_outside_bind(job, right_keys, x)
    with pytest.raises(InconsistentSourceError, match=message):
        _ = job.run()
    with pytest.raises(InconsistentSourceError, match=message):
        _ = job.nodes[node].run()
    for name in ("matched", "fm"):
        assert job.nodes[name].as_table().num_rows == 0


def _arranged(
    arrangement: Arrangement,
    source_id: str,
    keys: list[str] | None,
    changed: bool = False,
) -> ArrowTableSource:
    """``P`` (two tags, two data columns) in ``arrangement``; ``changed`` alters p2."""
    table = make_table(P, arrangement)
    if changed:
        index = table.schema.get_field_index("p2")
        values = table["p2"].to_pylist()
        values[-1] = "changed"
        table = table.set_column(
            index, table.schema.field(index), pa.array(values, pa.large_string())
        )
    role = arrangement.roles
    return ArrowTableSource(
        table,
        tag_columns=list(arrangement.tag_order),
        tag_only_columns=True
        if role == PLAIN
        else [P.mixed_plain]
        if role == MIXED
        else [],
        record_id_columns=keys,
        source_id=source_id,
    )


@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize(
    ("left_keys", "right_keys", "changed", "message"),
    [
        (["t1", "t2"], ["t2", "t1"], False, "different key columns"),
        (None, ["t2", "t1"], False, "different key columns"),
        (["t1", "t2"], ["t2", "t1"], True, "different key columns"),
        (None, None, True, "distinct source objects"),
    ],
)
@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_node_run_checks_its_sources_on_several_columns(
    role: str,
    left_keys: list[str] | None,
    right_keys: list[str] | None,
    changed: bool,
    message: str,
    orchestrator: type,
) -> None:
    left = Arrangement(P.tags, TAGS_FIRST, role)
    right = Arrangement(tuple(reversed(P.tags)), DATA_FIRST_REVERSED, role)
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        matched = SemiJoin()(
            _arranged(left, "left", None), _arranged(right, "right", None), label="m"
        )
        _ = with_pair.pod(matched, label="fm")
    # Rebound outside bind, so only the run-time checks can refuse.
    job.nodes["left"]._bind(_arranged(left, "shared", left_keys))
    job.nodes["right"]._bind(_arranged(right, "shared", right_keys, changed))
    with pytest.raises(InconsistentSourceError, match=message):
        _ = job.run(orchestrator=orchestrator())
    for node in ("m", "fm"):
        with pytest.raises(InconsistentSourceError, match=message):
            _ = job.nodes[node].run()
        assert job.nodes[node].as_table().num_rows == 0
