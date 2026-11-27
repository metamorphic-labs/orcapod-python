"""Sources sharing an address must agree on any rows held in memory (ITL-1469)."""

from __future__ import annotations

import itertools
import json
from collections import Counter
from pathlib import Path
import struct

import numpy as np
import pyarrow as pa
import pytest

from orcapod import Pipeline, PipelineJob, function_pod
from orcapod.core.function_pod import _FunctionPodBase
from orcapod.core.sources.base import RootSource
from orcapod.core.sources.source_proxy import SourceProxy
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import (
    InconsistentSourceError,
    InputValidationError,
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
def with_nested(x: int, y: list[float | None] | None) -> int:
    return x + len(y or [])


@function_pod(output_keys="out")
def with_array(x: list[np.ndarray]) -> int:  # pyright: ignore[reportMissingTypeArgument]  # the logical ndarray type is under test
    return len(x)


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
        with pytest.raises(InconsistentSourceError, match="different rows") as error:
            pod(second, label="fb")
    for part in (first.source_id, "fa", "fb", "source_id"):
        assert part in str(error.value)
    assert "fb" not in graph.nodes


@pytest.mark.parametrize("graph", [Pipeline, PipelineJob])
def test_issue_reproduction(graph: type[Pipeline] | type[PipelineJob]) -> None:
    first, second = _dict(1), _dict(100)
    assert first.content_hash() == second.content_hash()
    _refused(first, second, graph("conflicts"))


@pytest.mark.parametrize("kind", ["arrow", "dict", "frame"])
@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_fixed_types(kind: str, role: str, reverse: bool, orchestrator: type) -> None:
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
        with_pair.pod(source(0, second), label="same")
        with pytest.raises(InconsistentSourceError, match="different rows"):
            with_pair.pod(source(1, second), label="conflict")
    assert set(job.function_pods) == {"fa"}
    job.run(orchestrator=orchestrator())
    assert job.fa.as_table()["out"].to_pylist() == [8]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_fixed_list_source(reverse: bool, orchestrator: type) -> None:
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
        increment.pod(source(1, tags[::-1]), label="same")
        with pytest.raises(InconsistentSourceError, match="different rows"):
            increment.pod(source(100, tags[::-1]), label="conflict")
    assert set(job.function_pods) == {"fa"}
    job.run(orchestrator=orchestrator())
    assert job.fa.as_table()["out"].to_pylist() == [2]


@pytest.mark.parametrize("column", ["t1", "t2", "x", "y"])
def test_all_tag_and_data_columns_are_checked(column: str) -> None:
    row = {"t1": 1, "t2": 2, "x": 3, "y": 4}
    layout = {"t2": 2, "y": 4, "t1": 1, "x": 3}
    left = DictSource(
        [row], tag_columns=["t1", "t2"], tag_only_columns=True, source_id="shared"
    )
    right = DictSource(
        [{**layout, column: 100}],
        tag_columns=["t2", "t1"],
        tag_only_columns=True,
        source_id="shared",
    )
    _refused(left, right, Pipeline("conflicts"), both.pod)


def test_fixed_sources_can_be_wrapped() -> None:
    left, right = _dict(1), _dict(100)
    wrapper = SourceProxy(
        right.source_id,
        right.content_hash().to_string(),
        right.pipeline_hash().to_string(),
        *right.output_schema(),
    )
    wrapper.bind(right)
    _refused(left, wrapper, PipelineJob())


@pytest.mark.parametrize("value", [1, 100])
def test_bound_proxy_uses_its_live_arrow_schema_for_collision_checks(
    value: int,
) -> None:
    def source(x: int) -> ArrowTableSource:
        return ArrowTableSource(
            pa.table({"x": pa.array([x], type=pa.int32())}),
            infer_nullable=True,
            source_id="shared",
        )

    first, delegate = source(1), source(value)
    proxy = SourceProxy(
        delegate.source_id,
        delegate.content_hash().to_string(),
        delegate.pipeline_hash().to_string(),
        *delegate.output_schema(),
    )
    # An unbound proxy knows only the Python type, which defaults to int64.
    assert proxy.arrow_schemas[1].field("x").type == pa.int64()
    proxy.bind(delegate)

    if value != 1:
        _refused(first, proxy, PipelineJob())
        return

    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        increment.pod(first, label="fa")
        increment.pod(proxy, label="fb")
    assert set(job.function_pods) == {"fa"}
    job.run()
    assert job.fa.as_table()["out"].to_pylist() == [2]


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


@pytest.mark.parametrize("keyed", [False, True])
def test_row_order_counts_only_for_positional_ids(keyed: bool) -> None:
    rows = [{"k": "a", "t": 10, "x": 1, "y": 3}, {"k": "b", "t": 20, "x": 2, "y": 4}]

    def source(data: list[dict[str, str | int]], tags: list[str]) -> DictSource:
        return DictSource(
            data,
            tag_columns=tags,
            tag_only_columns=True,
            record_id_columns=["k", "t"] if keyed else None,
            source_id="shared",
        )

    first = source(rows, ["k", "t"])
    second = source(list(reversed(rows)), ["t", "k"])
    if keyed:
        with PipelineJob() as job:
            both.pod(first, label="fa")
            both.pod(second, label="fb")
        assert set(job.function_pods) == {"fa"}
        _refused(
            first,
            source([rows[1], {**rows[0], "y": 100}], ["t", "k"]),
            Pipeline("conflicts"),
            both.pod,
        )
    else:
        _refused(first, second, Pipeline("conflicts"), both.pod)


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
    assert "differ only in key columns" in message
    assert "ITL-1506" in message
    assert "slot 'left'" in message and "slot 'right'" in message
    assert dict(job.sources) == before


@pytest.mark.parametrize("reverse", [False, True])
def test_bind_refuses_different_rows_with_different_keys(reverse: bool) -> None:
    positional = _dict(1)
    other = DictSource([{"x": 100}], record_id_columns="x", source_id="shared")
    pair = (other, positional) if reverse else (positional, other)
    with pytest.raises(InconsistentSourceError, match="different rows"):
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
    assert "differ only" not in message
    assert "not compared" in message
    assert "ITL-1506" in message


def test_nulls_nan_signed_zero_and_nested_values() -> None:
    nan = struct.unpack("d", struct.pack("Q", 0x7FF8000000000001))[0]
    alternate_nan = struct.unpack("d", struct.pack("Q", 0x7FF8000000000002))[0]
    layout = pa.schema(
        [
            pa.field("x", pa.int64(), nullable=False),
            pa.field("y", pa.list_(pa.float64())),
        ]
    )

    def source(values: list[float | None]) -> ArrowTableSource:
        table = pa.Table.from_pylist([{"x": 1, "y": values}], schema=layout)
        return ArrowTableSource(table, source_id="shared")

    first, second = source([nan, -0.0, None]), source([alternate_nan, 0.0, None])
    with PipelineJob() as job:
        with_nested.pod(first, label="fa")
        with_nested.pod(second, label="fb")
    assert set(job.function_pods) == {"fa"}
    _refused(first, source([nan, -0.0, 1.0]), PipelineJob(), with_nested.pod)


def test_dictionary_columns_are_already_unsupported() -> None:
    table = pa.table({"x": pa.array([1, 2]).dictionary_encode()})
    with pytest.raises(InputValidationError, match="dictionary"):
        ArrowTableSource(table, source_id="shared")


def test_nested_logical_values_are_compared_as_encoded() -> None:
    def source(value: int) -> DictSource:
        return DictSource(
            [{"x": [np.array([value], dtype=np.int64)]}], source_id="shared"
        )

    with Pipeline("conflicts") as pipeline:
        with_array.pod(source(1), label="fa")
        with_array.pod(source(1), label="fb")
    assert set(pipeline.function_pods) == {"fa"}
    _refused(source(1), source(100), Pipeline("conflicts"), with_array.pod)


def test_zero_rows_use_the_declared_schema() -> None:
    def source(values: list[int]) -> ArrowTableSource:
        return ArrowTableSource(
            pa.Table.from_arrays(
                [pa.array(values, type=pa.int64())],
                schema=pa.schema([pa.field("x", pa.int64(), nullable=False)]),
            ),
            source_id="shared",
        )

    with PipelineJob() as job:
        increment.pod(source([]), label="fa")
        increment.pod(source([]), label="fb")
    assert set(job.function_pods) == {"fa"}
    _refused(source([]), source([1]), Pipeline("conflicts"))


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
    with pytest.raises(InconsistentSourceError, match="different rows") as error:
        job.bind(sources={"left": first, "right": second})
    for part in (first.source_id, "left", "right", "source_id"):
        assert part in str(error.value)
    assert job.sources == before
    job.bind(sources={"left": first})
    job.bind(sources={"left": second})
    assert job.sources["left"] is second
    with pytest.raises(InconsistentSourceError, match="different rows"):
        job.bind(sources={"right": first})
    job.bind(sources={"right": second})
    assert job.sources["right"] is second
    job.bind(sources={"left": _dict(100)})


def test_from_pipeline_checks_the_initial_bind() -> None:
    with Pipeline("conflicts") as graph:
        increment.pod(_dict(2, "left"), label="fa")
        increment.pod(_dict(3, "right"), label="fb")
    with pytest.raises(InconsistentSourceError, match="different rows"):
        PipelineJob.from_pipeline(
            graph, sources={"left": _dict(1), "right": _dict(100)}
        )


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_run_checks_slots_changed_outside_job_bind(orchestrator: type) -> None:
    job = _job()
    job.nodes["left"].bound_source = _dict(1)
    job.nodes["right"].bound_source = _dict(100)
    with pytest.raises(InconsistentSourceError, match="different rows"):
        job.run(orchestrator=orchestrator())
    assert not job.fa.as_table().num_rows
    assert not job.fb.as_table().num_rows


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_run_rechecks_derived_rows_after_refresh(orchestrator: type) -> None:
    def origin(x: int) -> DictSource:
        return DictSource(
            [{"t1": "a", "t2": "b", "x": x, "y": 2}],
            tag_columns=["t1", "t2"],
            tag_only_columns=True,
            source_id="origin",
        )

    def derived(source: RootSource, name: str) -> DerivedSource:
        return DerivedSource(source, tags_as_data={"t1": "tag_value"}, source_id=name)

    original = origin(1)
    proxy = SourceProxy(
        original.source_id,
        original.content_hash().to_string(),
        original.pipeline_hash().to_string(),
        *original.output_schema(),
    )
    proxy.bind(original)
    first, second = derived(original, "shared"), derived(proxy, "shared")
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        with_copy.pod(derived(original, "left"), label="fa")
        with_copy.pod(derived(original, "right"), label="fb")

    job.bind(sources={"left": first, "right": second})
    proxy.bind(origin(100))
    assert second.as_table()["x"].to_pylist() == [1]
    with pytest.raises(InconsistentSourceError, match="different rows") as error:
        job.run(orchestrator=orchestrator())
    assert "slot 'left'" in str(error.value)
    assert "slot 'right'" in str(error.value)
    assert second.as_table()["x"].to_pylist() == [100]
    assert job.last_run is None


def test_additive_recording_uses_the_current_binding() -> None:
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        increment.pod(_dict(1), label="fa")

    replacement = _dict(100)
    job.bind(sources={"shared": replacement})
    with job:
        with pytest.raises(InconsistentSourceError, match="different rows"):
            increment.pod(_dict(2), label="conflict")
        consume_output.pod(job.fa, label="next")

    assert job.nodes["shared"].bound_source is replacement
    job.run()
    assert job.fa.as_table()["out"].to_pylist() == [101]
    assert job.next.as_table()["next"].to_pylist() == [202]


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


def test_one_object_is_allowed_in_several_steps() -> None:
    source = _dict(1)
    with PipelineJob() as job:
        increment.pod(source, label="fa")
        increment_again.pod(source, label="fb")
    assert set(job.nodes) == {"shared", "fa", "fb"}


def test_dynamic_csv_and_cached_sources_are_allowed(tmp_path: Path) -> None:
    first_path, second_path = tmp_path / "first.csv", tmp_path / "second.csv"
    first_path.write_text("x\n1\n")
    second_path.write_text("x\n100\n")
    with PipelineJob() as job:
        increment.pod(CSVSource(str(first_path), source_id="shared"), label="fa")
        increment.pod(CSVSource(str(second_path), source_id="shared"), label="fb")
    assert set(job.function_pods) == {"fa"}

    with PipelineJob() as job:
        increment.pod(_dict(1).cached(InMemoryArrowDatabase()), label="fa")
        increment.pod(_dict(100).cached(InMemoryArrowDatabase()), label="fb")
    assert "fa" in job.nodes


def test_a_live_source_does_not_hide_two_conflicting_fixed_sources() -> None:
    live = _dict(2).cached(InMemoryArrowDatabase())
    with PipelineJob():
        increment.pod(live, label="fa")
        increment.pod(_dict(1), label="fb")
        with pytest.raises(InconsistentSourceError, match="different rows") as error:
            increment.pod(_dict(100), label="fc")
    assert "step 'fb'" in str(error.value)
    assert "step 'fc'" in str(error.value)


def test_issue_derived_job_sources_are_dynamic() -> None:
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

    with PipelineJob() as job:
        downstream.pod(first, label="fa")
        downstream.pod(second, label="fb")
    assert "fa" in job.nodes


# ITL-1469 review: a source id names a dataset; key columns only name its rows.

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
def test_fixed_same_rows_with_different_keys_differ_only_in_key_columns(
    reverse: bool, keys: tuple[list[str] | None, list[str] | None]
) -> None:
    first = _keyed(_ROWS, keys[0])
    # Another row order and tag order: the same dataset, read with other keys.
    second = _keyed(list(reversed(_ROWS)), keys[1], tags=["t2", "t1"])
    if reverse:
        first, second = second, first
    assert first.content_hash() != second.content_hash()
    message = _capture(first, second, both.pod)
    assert "differ only in key columns" in message
    assert "ITL-1506" in message
    assert "source_id=" in message


def test_fixed_same_rows_with_signed_zero_and_nan_differ_only_in_key_columns() -> None:
    @function_pod(output_keys="out")
    def with_float(x: int, y: float) -> float:
        return x + y

    def source(ys: list[float], keys: list[str] | None) -> DictSource:
        return DictSource(
            [{"t": f"r{i}", "x": i, "y": y} for i, y in enumerate(ys)],
            tag_columns="t",
            tag_only_columns=True,
            record_id_columns=keys,
            source_id="shared",
        )

    nan = struct.unpack(">d", bytes.fromhex("7ff8000000000001"))[0]
    first = source([0.0, float("nan"), 1.5], ["t"])
    second = source([-0.0, nan, 1.5], None)
    assert "differ only in key columns" in _capture(first, second, with_float.pod)


@pytest.mark.parametrize("reverse", [False, True])
def test_fixed_different_rows_with_different_keys_are_different_rows(
    reverse: bool,
) -> None:
    changed = [*_ROWS[:2], {**_ROWS[2], "y": 300}]
    first, second = _keyed(_ROWS, ["t1"]), _keyed(changed, ["t2", "x"])
    if reverse:
        first, second = second, first
    _refused(first, second, Pipeline("conflicts"), both.pod)


def test_fixed_row_multiset_counts_duplicates_and_ignores_order() -> None:
    a, b = _ROWS[0], _ROWS[1]
    assert (
        _keyed([a, a, b], None).fixed_row_multiset
        == _keyed([a, b, a], None).fixed_row_multiset
    )
    assert (
        _keyed([a, a, b], None).fixed_row_multiset
        != _keyed([a, b, b], None).fixed_row_multiset
    )
    assert (
        _keyed([a, b], None).fixed_row_multiset != _keyed([a], None).fixed_row_multiset
    )


@pytest.mark.parametrize("reverse", [False, True])
def test_live_sources_with_different_keys_do_not_claim_only_key_columns(
    tmp_path: Path, reverse: bool
) -> None:
    first_path, second_path = tmp_path / "first.csv", tmp_path / "second.csv"
    first_path.write_text("t1,t2,x,y\na,p,1,2\n")
    second_path.write_text("t1,t2,x,y\nb,q,3,4\n")
    sources = [
        CSVSource(
            str(path),
            source_id="shared",
            tag_columns=["t1", "t2"],
            tag_only_columns=True,
            record_id_columns=keys,
        )
        for path, keys in ((first_path, ["t1", "x"]), (second_path, ["t2", "y"]))
    ]
    first, second = sources[::-1] if reverse else sources
    message = _capture(first, second, both.pod)
    assert "differ only" not in message
    assert "different key columns" in message
    assert "not compared" in message
    assert "ITL-1506" in message
    assert "source_id=" in message


@function_pod(output_keys="out")
def with_copies(x: int, y: int, c1: str, c2: str) -> int:
    return x + y + len(c1) + len(c2)


def _derived(
    origin: RootSource, mapping: dict[str, str], keys: list[str] | None
) -> DerivedSource:
    return DerivedSource(
        origin, tags_as_data=mapping, record_id_columns=keys, source_id="shared"
    )


# Two copies, given in both orders; the conflict changes only the second copy (c2
# copies t3 instead of t2: same type, other values).
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
        assert "differ only" not in message
        assert "not compared" in message
        assert "ITL-1506" in message


@pytest.mark.parametrize("reverse", [False, True])
def test_derived_sources_over_fixed_origins_compare_rows_without_record_ids(
    reverse: bool,
) -> None:
    origin = DictSource(
        [
            {"t1": "a", "t2": "p", "t3": "u", "x": 1, "y": 10},
            {"t1": "b", "t2": "q", "t3": "v", "x": 2, "y": 20},
        ],
        tag_columns=["t1", "t2", "t3"],
        tag_only_columns=True,
        source_id="origin",
    )

    def pair(mapping: dict[str, str]) -> tuple[DerivedSource, DerivedSource]:
        first = _derived(origin, _COPIES, ["t1", "t2"])
        second = _derived(origin, mapping, ["t3", "t1"])
        return (second, first) if reverse else (first, second)

    # The mapping given in the other order copies the same values.
    same = _capture(*pair(_COPIES_REVERSED), with_copies.pod)
    assert "differ only in key columns" in same
    # Only the second copy changed: the rows differ.
    _refused(*pair(_SECOND_COPY_CHANGED), Pipeline("conflicts"), with_copies.pod)


def test_equal_keys_still_compare_record_ids() -> None:
    # Equal key columns: today's comparison, record ids included, row order counting
    # for positional ids.
    _refused(
        _keyed(_ROWS, None),
        _keyed(list(reversed(_ROWS)), None),
        Pipeline("conflicts"),
        both.pod,
    )
    with PipelineJob() as job:
        both.pod(_keyed(_ROWS, ["t1"]), label="fa")
        both.pod(_keyed(list(reversed(_ROWS)), ["t1"]), label="fb")
    assert set(job.function_pods) == {"fa"}


# Re-review of b197756f.


def test_unknown_key_columns_never_hide_a_conflict_between_known_ones() -> None:
    # An unbound proxy of the same id and schema has unknown key columns: it neither
    # causes nor suppresses a refusal between the sources whose keys are known.
    positional = DictSource([{"x": 1}, {"x": 2}], source_id="shared")
    keyed = DictSource([{"x": 1}, {"x": 2}], record_id_columns="x", source_id="shared")
    proxy = SourceProxy(
        positional.source_id,
        positional.content_hash().to_string(),
        positional.pipeline_hash().to_string(),
        *positional.output_schema(),
    )
    assert proxy.key_columns is None
    for uses in itertools.permutations(
        [("slot 'p'", proxy), ("slot 'a'", positional), ("slot 'b'", keyed)]
    ):
        with pytest.raises(InconsistentSourceError) as error:
            require_consistent_sources(uses)
        # The proxy's rows were not compared, so the rows are not claimed equal.
        message = str(error.value)
        assert "different key columns" in message
        assert "differ only" not in message
        assert "not compared" in message
    # Alone with either known source, the proxy causes nothing.
    require_consistent_sources([("slot 'p'", proxy), ("slot 'a'", positional)])
    require_consistent_sources([("slot 'p'", proxy), ("slot 'b'", keyed)])


def _three_slot_job() -> PipelineJob:
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        _ = increment.pod(_dict(2, "a"), label="fa")
        _ = increment.pod(_dict(3, "b"), label="fb")
        _ = increment.pod(_dict(4, "c"), label="fc")
    return job


@pytest.mark.parametrize(
    "order", list(itertools.permutations(["keyed", "forward", "backward"]))
)
def test_three_way_diagnosis_does_not_follow_slot_order(order: tuple[str, ...]) -> None:
    sources = {
        "keyed": DictSource(
            [{"x": 1}, {"x": 2}], record_id_columns="x", source_id="shared"
        ),
        "forward": DictSource([{"x": 1}, {"x": 2}], source_id="shared"),
        # Positional ids: another order is other rows than "forward".
        "backward": DictSource([{"x": 2}, {"x": 1}], source_id="shared"),
    }
    job = _three_slot_job()
    with pytest.raises(InconsistentSourceError, match="different rows"):
        job.bind(sources=dict(zip("abc", (sources[name] for name in order))))


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
    job.nodes["left"].bound_source = DictSource(
        [{"k": "a", "x": 1}, {"k": "b", "x": 2}],
        tag_columns="k",
        tag_only_columns=True,
        source_id="shared",
    )
    job.nodes["right"].bound_source = DictSource(
        [{"k": "a", "x": 1}, {"k": "b", "x": x}],
        tag_columns="k",
        tag_only_columns=True,
        record_id_columns=right_keys,
        source_id="shared",
    )


@pytest.mark.parametrize("node", ["matched", "fm"])
@pytest.mark.parametrize(
    ("right_keys", "x", "message"),
    [("k", 2, "differ only in key columns"), (None, 200, "different rows")],
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


def _count_digests(monkeypatch: pytest.MonkeyPatch) -> Counter[tuple[str, int]]:
    """Count each kind of fixed-row digest by the source object that computes it."""
    calls: Counter[tuple[str, int]] = Counter()
    exact = RootSource._digest_fixed_rows  # pyright: ignore[reportPrivateUsage]  # counted
    multiset = RootSource._digest_fixed_row_multiset  # pyright: ignore[reportPrivateUsage]  # counted

    def counted_exact(self: RootSource) -> bytes:
        calls["exact", id(self)] += 1
        return exact(self)

    def counted_multiset(self: RootSource) -> bytes:
        calls["multiset", id(self)] += 1
        return multiset(self)

    monkeypatch.setattr(RootSource, "_digest_fixed_rows", counted_exact)
    monkeypatch.setattr(RootSource, "_digest_fixed_row_multiset", counted_multiset)
    return calls


def test_each_digest_is_computed_at_most_once_per_source_per_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _count_digests(monkeypatch)
    rows = [{"x": 1}, {"x": 2}]
    first = DictSource(rows, source_id="shared")
    second = DictSource(rows, source_id="shared")
    keyed = DictSource(rows, record_id_columns="x", source_id="shared")
    uses = [("slot 'a'", first), ("slot 'b'", second), ("slot 'c'", keyed)]
    for _ in range(2):
        # Every check digests again (a held table can share memory with the
        # caller's data), but each digest at most once per source.
        calls.clear()
        with pytest.raises(InconsistentSourceError, match="differ only in key"):
            require_consistent_sources(uses)
        # Equal keys need the exact digest, different keys the multiset digest.
        assert calls == {
            ("exact", id(first)): 1,
            ("exact", id(second)): 1,
            ("multiset", id(first)): 1,
            ("multiset", id(second)): 1,
            ("multiset", id(keyed)): 1,
        }

    # Two sources whose key columns differ need only the multiset digest.
    calls.clear()
    pair = [
        ("slot 'a'", DictSource(rows, source_id="shared")),
        ("slot 'b'", DictSource(rows, record_id_columns="x", source_id="shared")),
    ]
    with pytest.raises(InconsistentSourceError, match="differ only in key"):
        require_consistent_sources(pair)
    assert sorted(calls.values()) == [1, 1]
    assert {kind for kind, _ in calls} == {"multiset"}


def test_a_proxy_and_its_source_are_digested_once_per_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def derived() -> DerivedSource:
        origin = DictSource(
            [{"t": "a", "x": 1}, {"t": "b", "x": 2}],
            tag_columns="t",
            tag_only_columns=True,
            source_id="origin",
        )
        return DerivedSource(
            origin, tags_as_data={"t": "tag_value"}, source_id="shared"
        )

    source, other = derived(), derived()
    proxy = SourceProxy(
        source.source_id,
        source.content_hash().to_string(),
        source.pipeline_hash().to_string(),
        *source.output_schema(),
    )
    proxy.bind(source)
    calls = _count_digests(monkeypatch)
    for uses in itertools.permutations(
        [("slot 'p'", proxy), ("slot 'd'", source), ("slot 'o'", other)]
    ):
        calls.clear()
        require_consistent_sources(uses)
        assert calls == {("exact", id(source)): 1, ("exact", id(other)): 1}


def test_a_mutated_frame_is_compared_again_at_the_next_bind() -> None:
    import pandas as pd

    def frame() -> pd.DataFrame:
        return pd.DataFrame({"x": np.array([1, 2], dtype=np.int64)})

    first_frame, second_frame = frame(), frame()
    first = DataFrameSource(first_frame, source_id="shared")
    second = DataFrameSource(second_frame, source_id="shared")
    job = _job()
    job.bind(sources={"left": first, "right": second})
    first_frame.iloc[0, 0] = 100
    if first.as_table()["x"].to_pylist() == [1, 2]:
        pytest.skip("this pandas copies the frame; nothing is shared to mutate")
    with pytest.raises(InconsistentSourceError, match="different rows"):
        job.bind(sources={"left": first, "right": second})


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
        (["t1", "t2"], ["t2", "t1"], False, "differ only in key columns"),
        (None, ["t2", "t1"], False, "differ only in key columns"),
        (["t1", "t2"], ["t2", "t1"], True, "different rows"),
        (None, None, True, "different rows"),
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
    job.nodes["left"].bound_source = _arranged(left, "shared", left_keys)
    job.nodes["right"].bound_source = _arranged(right, "shared", right_keys, changed)
    with pytest.raises(InconsistentSourceError, match=message):
        _ = job.run(orchestrator=orchestrator())
    for node in ("m", "fm"):
        with pytest.raises(InconsistentSourceError, match=message):
            _ = job.nodes[node].run()
        assert job.nodes[node].as_table().num_rows == 0
