"""Sources sharing an address must agree on any rows held in memory (ITL-1469)."""

from __future__ import annotations

import json
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
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.sources import (
    ArrowTableSource,
    CSVSource,
    DataFrameSource,
    DerivedSource,
    DictSource,
    ListSource,
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


@pytest.mark.parametrize("kind", ["arrow", "dict", "frame", "list"])
@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_fixed_types(kind: str, orchestrator: type) -> None:
    def source(x: int) -> RootSource:
        rows = [{"t1": "a", "t2": "b", "x": x}]
        if kind == "list":
            return ListSource(
                "x",
                [x],
                tag_function=lambda _x, _i: {"t1": "a", "t2": "b"},
                source_id="shared",
            )
        rows[0]["y"] = 10
        if kind == "arrow":
            return ArrowTableSource(
                pa.Table.from_pylist(rows),
                infer_nullable=True,
                tag_columns=["t1", "t2"],
                tag_only_columns=True,
                source_id="shared",
            )
        cls = DictSource if kind == "dict" else DataFrameSource
        return cls(
            data=rows,
            tag_columns=["t1", "t2"],
            tag_only_columns=True,
            source_id="shared",
        )

    job = PipelineJob(database=InMemoryArrowDatabase())
    pod = increment.pod if kind == "list" else both.pod
    _refused(source(1), source(100), job, pod)
    job.run(orchestrator=orchestrator())
    assert job.fa.as_table()["out"].to_pylist() == ([2] if kind == "list" else [11])


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


def test_record_ids_count_at_bind() -> None:
    job = _job()
    positional = _dict(1)
    keyed = DictSource([{"x": 1}], record_id_columns="x", source_id="shared")
    with pytest.raises(InconsistentSourceError, match="different rows"):
        job.bind(sources={"left": positional, "right": keyed})


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
