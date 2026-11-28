# tests/test_core/function_pod/test_function_pod_ctx_arg.py
"""Tests for the ctx_arg parameter on the @function_pod decorator.

Full parity with tests/test_core/side_effect_function/test_side_effect_function_pod.py,
exercising @function_pod(ctx_arg=...) as the preferred side-effect pod entry point.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
import pyarrow as pa

from orcapod.core.streams import ArrowTableStream
from orcapod.side_effects import InvocationContext
from orcapod.system_constants import (
    PIPELINE_DB_SCHEMA_VERSION,
    TRACKING_DB_SCHEMA_VERSION,
)


def _make_stream(n: int = 3) -> ArrowTableStream:
    """Simple stream: tag=id (int), data=value (int)."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("value", pa.int64(), nullable=False),
        ]
    )
    table = pa.table(
        {"id": list(range(n)), "value": list(range(n))},
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


def _make_pipeline_db():
    """Return a fresh in-memory ArrowDatabase."""
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase

    return InMemoryArrowDatabase()


@pytest.mark.parametrize("async_path", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("reverse_columns", [False, True])
@pytest.mark.filterwarnings(
    "ignore:Batch is provisional:orcapod.errors.ProvisionalHashWarning"
)
def test_context_parameter_distinguishes_job_branches(
    tmp_path: Path, async_path: bool, reverse_columns: bool
) -> None:
    """Two pods sharing a data function URI have separate identities and both run."""
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.operators import Batch, Join
    from orcapod.side_effects import SideEffectPod
    from orcapod.sources import DictSource
    from orcapod.system_constants import constants

    delivered: list[tuple[int, int]] = []

    @function_pod(function_name="combine", output_keys=("sum", "difference"))
    def plain(x: int, y: int) -> tuple[int, int]:
        return x + y, x - y

    @function_pod(
        function_name="combine", output_keys=("sum", "difference"), ctx_arg="ctx"
    )
    def contextual(x: int, y: int, ctx: InvocationContext) -> tuple[int, int]:
        assert isinstance(ctx, InvocationContext)
        delivered.append((x, y))
        return x + y + 100, x - y

    tapped: list[tuple[int, int]] = []

    def tap(sum: int, difference: int, ctx: InvocationContext) -> None:
        tapped.append((sum, difference))

    assert plain.pod.data_function.uri == contextual.pod.data_function.uri
    assert plain.pod.pipeline_hash() != contextual.pod.pipeline_hash()

    rows = [
        {"id": 1, "x": 10, "group": "a", "y": 2},
        {"id": 2, "x": 20, "group": "b", "y": 3},
    ]
    tags = ["id", "group"]
    if reverse_columns:
        rows = [dict(reversed(tuple(row.items()))) for row in rows]
        tags.reverse()
    source = DictSource(
        rows, tag_columns=tags, tag_only_columns=True, source_id="input"
    )
    right = DictSource(
        [
            {"id": row["id"], "group": row["group"], "other": 5, "label": "joined"}
            for row in rows
        ],
        tag_columns=tags,
        tag_only_columns=True,
        source_id="right",
    )
    root = tmp_path / "store"

    def build_job() -> PipelineJob:
        with PipelineJob(
            name="context_identity", database=DeltaTableDatabase(root)
        ) as job:
            plain_stream = plain.pod(source, label="plain")
            context_stream = contextual.pod(source, label="contextual")
            Batch(batch_size=1).process(plain_stream, label="plain_batch")
            Batch(batch_size=1).process(context_stream, label="context_batch")
            Join()(plain_stream, right, label="plain_join")
            Join()(context_stream, right, label="context_join")
            SideEffectPod(tap).process(context_stream, label="tap")
        return job

    job = build_job()
    plain_node = job.nodes["plain"]
    context_node = job.nodes["contextual"]
    assert plain_node.pipeline_hash() != context_node.pipeline_hash()
    assert plain_node.content_hash() != context_node.content_hash()
    assert plain_node.node_identity_path[-1] != context_node.node_identity_path[-1]

    summary = asyncio.run(job.run_async()) if async_path else job.run()
    assert summary.ok
    stored_paths = {path[1:-1] for path in DeltaTableDatabase(root).list_sources()}
    assert {
        plain_node.node_identity_path,
        context_node.node_identity_path,
    } <= stored_paths
    assert sorted(plain_node.as_table()["sum"].to_pylist()) == [12, 23]
    assert sorted(context_node.as_table()["sum"].to_pylist()) == [112, 123]

    plain_batch = job.nodes["plain_batch"]
    context_batch = job.nodes["context_batch"]
    assert plain_batch.pipeline_hash() != context_batch.pipeline_hash()
    plain_columns = {
        col
        for col in plain_batch.as_table(columns={"system_tags": True}).column_names
        if col.startswith(constants.SYSTEM_TAG_PREFIX)
    }
    context_columns = {
        col
        for col in context_batch.as_table(columns={"system_tags": True}).column_names
        if col.startswith(constants.SYSTEM_TAG_PREFIX)
    }
    assert len(plain_columns) == len(context_columns) == 2
    assert plain_columns.isdisjoint(context_columns)

    plain_join = job.nodes["plain_join"].as_table(columns={"system_tags": True})
    context_join = job.nodes["context_join"].as_table(columns={"system_tags": True})
    assert sorted(plain_join["sum"].to_pylist()) == [12, 23]
    assert sorted(context_join["sum"].to_pylist()) == [112, 123]
    plain_tags, contextual_tags = (
        {
            name
            for name in table.column_names
            if name.startswith(constants.SYSTEM_TAG_PREFIX)
        }
        for table in (plain_join, context_join)
    )
    assert len(plain_tags) == len(contextual_tags) == 4
    assert len(plain_tags & contextual_tags) == 2

    assert sorted(delivered) == [(10, 2), (20, 3)]
    assert sorted(tapped) == [(112, 8), (123, 17)]
    reopened = build_job()
    assert (asyncio.run(reopened.run_async()) if async_path else reopened.run()).ok
    assert sorted(delivered) == [(10, 2), (20, 3)]
    assert sorted(tapped) == [(112, 8), (123, 17)]
    assert sorted(reopened.nodes["contextual"].as_table()["sum"].to_pylist()) == [
        112,
        123,
    ]


def test_context_parameter_name_enters_pipeline_identity() -> None:
    """Renaming the context parameter changes both hashes with the same pod URI."""
    from orcapod import function_pod

    @function_pod(function_name="combine", output_keys="sum", ctx_arg="ctx")
    def original(x: int, y: int, ctx: InvocationContext) -> int:
        assert isinstance(ctx, InvocationContext)
        return x + y

    @function_pod(function_name="combine", output_keys="sum", ctx_arg="invocation")
    def renamed(x: int, y: int, invocation: InvocationContext) -> int:
        assert isinstance(invocation, InvocationContext)
        return x + y

    assert original.pod.uri == renamed.pod.uri
    assert original.pod.input_data_schema == renamed.pod.input_data_schema
    assert original.pod.pipeline_hash() != renamed.pod.pipeline_hash()
    assert original.pod.content_hash() != renamed.pod.content_hash()


class TestCtxArgSchema:
    """Scenarios 1–2: schema inference and ctx stripping via @function_pod(ctx_arg=...)."""

    def test_ctx01_ctx_stripped_from_input_schema(self):
        """Scenario 1: ctx param absent from pod.input_data_schema; present in data function."""
        from orcapod.core.function_pod import function_pod

        @function_pod(output_keys="result", ctx_arg="ctx")
        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"result_{value}"

        pod = my_fn.pod

        # Pod's exposed input schema excludes 'ctx'
        assert "ctx" not in pod.input_data_schema
        assert "value" in pod.input_data_schema
        assert pod.input_data_schema["value"] is int

        # Underlying data function retains full schema (ctx included for hashing)
        assert "ctx" in pod._data_function.input_data_schema

        # Output schema has the declared key
        assert "result" in pod._data_function.output_data_schema
        assert pod._data_function.output_data_schema["result"] is str

    def test_ctx02_ctx_arg_name_property(self):
        """Scenario 2: pod.ctx_arg_name reflects the configured arg name."""
        from orcapod.core.function_pod import function_pod

        @function_pod(output_keys="result", ctx_arg="invocation_ctx")
        def my_fn(value: int, invocation_ctx: InvocationContext) -> str:
            return f"r_{value}"

        pod = my_fn.pod
        assert pod.ctx_arg_name == "invocation_ctx"
        assert "invocation_ctx" not in pod.input_data_schema
        assert "value" in pod.input_data_schema


class TestCtxArgStandaloneExecution:
    """Scenarios 3–4: standalone execution via pod.process() / FunctionPodStream."""

    def test_ctx03_sync_execution_injects_ctx_per_row(self):
        """Scenario 3: ctx injected per row; output values correct; tags pass through."""
        from orcapod.core.function_pod import function_pod

        received_ctx: list[InvocationContext] = []

        @function_pod(output_keys="result", ctx_arg="ctx")
        def my_fn(value: int, ctx: InvocationContext) -> str:
            received_ctx.append(ctx)
            return f"v{value}"

        pod = my_fn.pod
        stream = _make_stream(3)
        rows = list(pod.process(stream).iter_data())

        assert len(rows) == 3
        assert len(received_ctx) == 3

        # Each ctx is a proper InvocationContext
        for ctx in received_ctx:
            assert isinstance(ctx, InvocationContext)
            assert isinstance(ctx.invocation_hash, str)
            assert len(ctx.invocation_hash) > 0
            assert "::" in ctx.invocation_hash

        # Standalone execution: no pipeline run id; pod_name matches pod label
        assert received_ctx[0].pipeline_run_id is None
        assert received_ctx[0].pod_name == pod.label

        # Output values match expected
        for i, (tag, data) in enumerate(rows):
            assert data.as_dict()["result"] == f"v{i}"

        # Tags pass through unchanged
        assert rows[0][0].as_dict()["id"] == 0
        assert rows[2][0].as_dict()["id"] == 2

    def test_ctx04_async_fn_routed_through_sync_path(self):
        """Scenario 4: async user function runs correctly via synchronous wrapper."""
        import asyncio
        from orcapod.core.function_pod import function_pod

        @function_pod(output_keys="result", ctx_arg="ctx")
        async def my_async_fn(value: int, ctx: InvocationContext) -> str:
            await asyncio.sleep(0)
            return f"async_{value}"

        pod = my_async_fn.pod
        stream = _make_stream(2)
        rows = list(pod.process(stream).iter_data())

        assert len(rows) == 2
        assert rows[0][1].as_dict()["result"] == "async_0"
        assert rows[1][1].as_dict()["result"] == "async_1"


class TestCtxArgDBBacked:
    """Scenarios 5–6: DB-backed execution via FunctionJobNode and PipelineJob."""

    def test_ctx05_function_job_node_caches_output(self):
        """Scenario 5: second run uses cached result; fn not called again."""
        from orcapod.core.function_pod import function_pod
        from orcapod.core.nodes.function_node import FunctionJobNode

        call_count = 0

        @function_pod(output_keys="result", ctx_arg="ctx")
        def my_fn(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"r{value}"

        pod = my_fn.pod
        stream = _make_stream(2)
        pipeline_db = _make_pipeline_db()

        node1 = FunctionJobNode(function_pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=pipeline_db)
        results1 = list(node1.execute(stream).iter_data())
        assert len(results1) == 2
        assert call_count == 2

        # Second run — same pod, same data, same DB — must NOT call fn again
        node2 = FunctionJobNode(function_pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=pipeline_db)
        results2 = list(node2.execute(stream).iter_data())
        assert len(results2) == 2
        assert call_count == 2  # NOT incremented

        # Both runs produce identical result values
        for (_, d1), (_, d2) in zip(results1, results2):
            assert d1.as_dict()["result"] == d2.as_dict()["result"]

    def test_ctx06_pipeline_job_integration(self):
        """Scenario 6: end-to-end PipelineJob compilation + execution with ctx_arg pod."""
        from orcapod.core.function_pod import function_pod
        from orcapod.pipeline.job import PipelineJob
        from orcapod.core.sources.dict_source import DictSource

        received_ctx: list[InvocationContext] = []

        @function_pod(output_keys="result", ctx_arg="ctx")
        def transform(value: int, ctx: InvocationContext) -> str:
            received_ctx.append(ctx)
            return f"result_{value}"

        pod = transform.pod
        db = _make_pipeline_db()

        with PipelineJob(name="test_ctx_pod", database=db) as job:
            source = DictSource(
                [{"id": 0, "value": 10}, {"id": 1, "value": 20}],
                tag_columns=["id"],
                tag_only_columns=True,
            )
            pod.process(source)

        job.run()

        # fn called once per row, ctx injected each time
        assert len(received_ctx) == 2
        for ctx in received_ctx:
            assert isinstance(ctx, InvocationContext)


class TestCtxArgDecoratorForms:
    """Scenarios 7–8: decorator usage with ctx_arg."""

    def test_ctx07_decorator_minimal_form(self):
        """Scenario 7: @function_pod(output_keys='result', ctx_arg='ctx') — pod attached correctly."""
        from orcapod.core.function_pod import function_pod, FunctionPod

        @function_pod(output_keys="result", ctx_arg="ctx")
        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"v{value}"

        # my_fn is still callable (wraps original fn)
        assert callable(my_fn)
        # .pod is attached
        pod = my_fn.pod
        assert isinstance(pod, FunctionPod)
        assert pod.ctx_arg_name == "ctx"
        # Output key is set correctly
        assert "result" in pod._data_function.output_data_schema

    def test_ctx08_decorator_with_explicit_output_keys(self):
        """Scenario 8: @function_pod(output_keys=['out'], ctx_arg='ctx') — explicit output_keys."""
        from orcapod.core.function_pod import function_pod, FunctionPod

        @function_pod(output_keys=["out"], ctx_arg="ctx")
        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"v{value}"

        pod = my_fn.pod
        assert isinstance(pod, FunctionPod)
        assert pod.ctx_arg_name == "ctx"
        assert "out" in pod._data_function.output_data_schema
        assert "ctx" not in pod.input_data_schema


class TestCtxArgInvalidParam:
    """Scenario 9: ValueError at construction time when ctx_arg names a parameter not present in the function."""

    def test_ctx09_invalid_ctx_arg_raises_at_decoration(self):
        """Scenario 9: ValueError at decoration time if ctx_arg is not a parameter of the function.

        ``_FunctionPodBase.__init__`` raises immediately when ``ctx_arg_name`` is not
        in the underlying data function's input schema.
        """
        from orcapod.core.function_pod import function_pod

        with pytest.raises(ValueError, match="ctx_arg_name"):

            @function_pod(output_keys="result", ctx_arg="nonexistent_param")
            def my_fn(value: int) -> str:
                return str(value)


class TestCtxArgCachedPodWrapping:
    """Scenario 10: @function_pod(ctx_arg=..., pod_cache_database=...) — CachedFunctionPod wrapping."""

    def test_ctx10_cached_pod_injects_ctx_on_miss_skips_on_hit(self):
        """Scenario 10: CachedFunctionPod wrapping a ctx_arg pod:
        - ctx stripped from exposed schema
        - InvocationContext injected on cache miss
        - fn NOT called on cache hit; output same
        """
        from orcapod.core.function_pod import function_pod
        from orcapod.core.cached_function_pod import CachedFunctionPod

        received_ctx: list[InvocationContext] = []
        call_count = 0

        db = _make_pipeline_db()

        @function_pod(output_keys="result", ctx_arg="ctx", pod_cache_database=db)
        def my_fn(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            received_ctx.append(ctx)
            return f"v{value}"

        pod = my_fn.pod
        assert isinstance(pod, CachedFunctionPod)

        # Schema: ctx stripped from wrapped pod's exposed input schema
        assert "ctx" not in pod.input_data_schema
        assert "value" in pod.input_data_schema
        assert pod.ctx_arg_name == "ctx"

        stream = _make_stream(2)

        # First run: cache miss → fn called, InvocationContext injected
        rows1 = list(pod.process(stream).iter_data())
        assert call_count == 2
        assert len(received_ctx) == 2
        assert len(rows1) == 2
        for ctx in received_ctx:
            assert isinstance(ctx, InvocationContext)
            assert len(ctx.invocation_hash) > 0

        # Second run: cache hit → fn NOT called
        rows2 = list(pod.process(stream).iter_data())
        assert call_count == 2  # unchanged — cache hit

        # Output values must match
        for (_, d1), (_, d2) in zip(rows1, rows2):
            assert d1.as_dict()["result"] == d2.as_dict()["result"]


@pytest.mark.parametrize("async_path", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("only_downstream_log", [False, True], ids=["function", "tap"])
def test_reopening_pre_context_identity_completions_refuses_delivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    async_path: bool,
    only_downstream_log: bool,
) -> None:
    """Completions at the previous schema path refuse a run before redelivery."""
    from orcapod import FunctionPod, PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
    from orcapod.errors import SideEffectLogDataContextError
    from orcapod.side_effects import SideEffectPod
    from orcapod.sources import DictSource

    delivered: list[tuple[str, int]] = []
    root = tmp_path / "store"

    def build_job(root: Path) -> PipelineJob:
        @function_pod(output_keys="result", ctx_arg="ctx")
        def contextual(value: int, ctx: InvocationContext) -> int:
            delivered.append(("function", value))
            return value + 100

        def tap(result: int, ctx: InvocationContext) -> None:
            delivered.append(("tap", result))

        with PipelineJob(
            name="context_identity",
            database=DeltaTableDatabase(root),
            # Exercise direct function redelivery when only its pipeline log persists.
            result_database=None if only_downstream_log else InMemoryArrowDatabase(),
        ) as job:
            source = DictSource(
                [{"id": 1, "value": 0}],
                tag_columns=["id"],
                tag_only_columns=True,
                source_id="input",
            )
            rows = contextual.pod(source, label="contextual")
            if only_downstream_log:
                SideEffectPod(tap).process(rows, label="tap")
        return job

    def run(job: PipelineJob) -> None:
        summary = asyncio.run(job.run_async()) if async_path else job.run()
        assert summary.ok

    with monkeypatch.context() as previous:
        # Recreate main's pipeline identity while writing real completion records.
        previous.setattr(
            FunctionPod, "pipeline_identity_structure", lambda pod: pod.data_function
        )
        run(build_job(root))

    store = DeltaTableDatabase(root)
    table_paths = store.list_table_paths()
    (function_log,) = (
        path for path in table_paths if path[-1] == PIPELINE_DB_SCHEMA_VERSION
    )
    if only_downstream_log:
        # Keep the shared result cache and the prior tap log. This lets the run
        # reach the tap, even though the function would otherwise refuse first.
        root = tmp_path / "tap_store"
        tap_store = DeltaTableDatabase(root)
        for path in table_paths:
            if path != function_log:
                rows = store.get_all_records(path, record_id_column="__rid")
                assert rows is not None
                tap_store.add_records(path, rows, record_id_column="__rid")
        tap_store.flush()
        store = tap_store
        (expected_path,) = (
            path for path in table_paths if path[-1] == TRACKING_DB_SCHEMA_VERSION
        )
        expected_label = "tap"
        expected_deliveries = [("function", 0), ("tap", 100)]
    else:
        expected_path = function_log
        expected_label = "contextual"
        expected_deliveries = [("function", 0)]
    assert delivered == expected_deliveries

    before = store.get_all_records(expected_path)
    assert before is not None and before.num_rows == 1
    with pytest.raises(SideEffectLogDataContextError) as info:
        run(build_job(root))
    assert info.value.node_label == expected_label
    assert expected_path in info.value.record_paths
    assert delivered == expected_deliveries
    assert DeltaTableDatabase(root).get_all_records(expected_path) == before


@pytest.mark.parametrize(
    "previous_identity", [False, True], ids=["current", "previous"]
)
def test_saved_context_function_keeps_its_results_and_downstream_node(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, previous_identity: bool
) -> None:
    """Load a saved context-taking function and its downstream side-effect node."""
    from orcapod import FunctionPod, PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.side_effects import SideEffectPod
    from orcapod.sources import DictSource

    delivered: list[int] = []

    @function_pod(output_keys="result", ctx_arg="ctx")
    def contextual(value: int, ctx: InvocationContext) -> int:
        delivered.append(value)
        return value + 100

    def tap(result: int, ctx: InvocationContext) -> None:
        delivered.append(result)

    source = DictSource(
        [{"id": 1, "value": 0}],
        tag_columns=["id"],
        tag_only_columns=True,
        source_id="input",
    )
    saved = tmp_path / "job.json"
    with monkeypatch.context() as previous:
        if previous_identity:
            previous.setattr(
                FunctionPod,
                "pipeline_identity_structure",
                lambda pod: pod.data_function,
            )
        with PipelineJob(
            name="saved_context", database=DeltaTableDatabase(tmp_path / "store")
        ) as job:
            SideEffectPod(tap).process(
                contextual.pod(source, label="contextual"), label="tap"
            )
        assert job.run().ok
        job.save(saved)

    loaded = PipelineJob.load(saved)
    contextual_node = loaded.nodes["contextual"]
    assert contextual_node.node_type == "side_effect_function"
    assert (
        contextual_node.node_identity_path == job.nodes["contextual"].node_identity_path
    )
    assert contextual_node.as_source().as_table()["result"].to_pylist() == [100]
    loaded.bind(sources={"input": source})
    assert loaded.run(load_only=True).ok
    for label in ("contextual", "tap"):
        assert loaded.nodes[label].pipeline_hash() == job.nodes[label].pipeline_hash()
        assert loaded.nodes[label].content_hash() == job.nodes[label].content_hash()
        assert loaded.nodes[label].as_table()["result"].to_pylist() == [100]
    assert delivered == [0, 100]

    malformed = json.loads(saved.read_text())
    descriptor = next(
        node
        for node in malformed["pipeline"]["nodes"].values()
        if node["label"] == "contextual"
    )
    del descriptor["fall_through_tags"]
    saved.write_text(json.dumps(malformed))
    with pytest.raises(ValueError, match="saved node 'contextual'.*fall_through_tags"):
        PipelineJob.load(saved)
