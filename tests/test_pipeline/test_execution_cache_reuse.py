"""Completed jobs retain operator work and row hashes across orchestrators."""

from unittest.mock import patch
from dataclasses import dataclass
from typing import cast

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import GroupBy
from orcapod.databases import InMemoryArrowDatabase
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DictSource


@function_pod(output_keys="total")
def total(x: list[int]) -> int:
    """Sum a group."""
    return sum(x)


@pytest.mark.parametrize("first", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator])
@pytest.mark.parametrize(
    "second", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_warm_grouped_job_reuses_work(first, second) -> None:
    """A warm run does no grouping or Arrow hashing and preserves every output."""
    job = PipelineJob(name="reuse", store=InMemoryArrowDatabase())
    with job:
        grouped = GroupBy(by=["g"])(
            DictSource([{"g": 1, "x": 2}, {"g": 1, "x": 3}], tag_columns=["g"]),
            label="grouped",
        )
        total.pod(grouped, label="total")
    job.run(orchestrator=first())
    before = job.nodes["total"].as_table(all_info=True)
    with (
        patch.object(
            GroupBy, "unary_static_process", side_effect=AssertionError("regrouped")
        ),
        patch.object(
            StarfixArrowHasher, "hash_table", side_effect=AssertionError("rehashed")
        ),
    ):
        job.run(orchestrator=second())
    assert job.nodes["total"].as_table(all_info=True).equals(before)


@function_pod(output_keys="x")
def doubled(x: int) -> int:
    """Double a source row before grouping."""
    return 2 * x


@function_pod(output_keys="x")
def omitted(x: int) -> int | None:
    """Record a successful computation that produces no row."""
    return None


fail_row = False
CALLS: list[int] = []


@function_pod(output_keys="x")
def fallible(x: int) -> int:
    """Fail one row on demand to exercise partial-run recovery."""
    CALLS.append(x)
    if fail_row and x == 3:
        raise ValueError("retry this row")
    return x


def make_chain(pod=doubled):
    """Build a function/operator/function chain with a reusable source."""
    job = PipelineJob(name="chain_reuse", store=InMemoryArrowDatabase())
    with job:
        rows = pod.pod(
            DictSource(
                [{"g": 1, "x": 2}, {"g": 1, "x": 3}],
                tag_columns=["g"],
                source_id="same",
            ),
            label="rows",
        )
        grouped = GroupBy(by=["g"])(rows, label="grouped")
        if pod is not omitted:
            total.pod(grouped, label="total")
    return job


@pytest.mark.parametrize("first", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator])
@pytest.mark.parametrize(
    "second", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
@pytest.mark.parametrize("materialize", [True, False])
def test_warm_chain_reuses_work(first, second, materialize) -> None:
    """Function cache hits preserve the completion proof used by downstream operators."""
    job = make_chain()
    job.run(orchestrator=first(), materialize_results=materialize)
    before = job.nodes["total"].as_table(all_info=True)
    with (
        patch.object(
            GroupBy, "unary_static_process", side_effect=AssertionError("regrouped")
        ),
        patch.object(
            StarfixArrowHasher, "hash_table", side_effect=AssertionError("rehashed")
        ),
    ):
        summary = job.run(orchestrator=second(), materialize_results=materialize)
    assert (summary.nodes["rows"].computed, summary.nodes["rows"].reused) == (0, 2)
    assert (summary.nodes["total"].computed, summary.nodes["total"].reused) == (0, 1)
    assert job.nodes["total"].as_table(all_info=True).equals(before)


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_rebinding_same_source_id_invalidates_completion(orchestrator) -> None:
    """Source identity does not make a replacement source's changed rows reusable."""
    from orcapod.core.nodes.source_node import SourceJobNode

    job = make_chain()
    job.run(orchestrator=orchestrator())
    source = next(
        node for node in job.nodes.values() if isinstance(node, SourceJobNode)
    )
    source.bound_source = DictSource(
        [{"g": 1, "x": 9}], tag_columns=["g"], source_id="same"
    )
    job.run(orchestrator=orchestrator())
    assert job.nodes["grouped"].as_table().column("x").to_pylist() == [[18]]


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_explicit_operator_clear_recomputes(orchestrator) -> None:
    """Explicit cache clearing invalidates completion even for unchanged inputs."""
    job = make_chain()
    job.run(orchestrator=orchestrator())
    job.nodes["grouped"].clear_cache()
    original = GroupBy.unary_static_process
    calls = []

    def grouped(self, stream):
        calls.append(True)
        return original(self, stream)

    with patch.object(GroupBy, "unary_static_process", grouped):
        job.run(orchestrator=orchestrator())
    assert calls == [True]


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_partial_failure_retries_and_regroups(orchestrator) -> None:
    """Continue-policy failures never publish a reusable completed result."""
    global fail_row
    fail_row = True
    CALLS.clear()
    job = make_chain(fallible)
    try:
        with pytest.warns(Warning, match="failed"):
            job.run(orchestrator=orchestrator())
    finally:
        fail_row = False
    CALLS.clear()
    job.run(orchestrator=orchestrator())
    assert CALLS == [3]
    assert sorted(job.nodes["grouped"].as_table().column("x").to_pylist()[0]) == [2, 3]


@pytest.mark.parametrize("first", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator])
@pytest.mark.parametrize(
    "second", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_empty_results_are_completed(first, second) -> None:
    """Successful None outputs and the empty downstream result are reusable."""
    job = make_chain()
    job.nodes["rows"]._function_pod.data_function.set_active(False)
    job.run(orchestrator=first())
    with patch.object(
        StarfixArrowHasher, "hash_table", side_effect=AssertionError("rehashed")
    ):
        job.run(orchestrator=second())
    assert job.nodes["grouped"].as_table().num_rows == 0
    job.nodes["rows"]._function_pod.data_function.set_active(True)


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_source_callback_rebinding_does_not_certify_old_rows(orchestrator) -> None:
    """A source callback cannot relabel an old buffer as a new source snapshot."""
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.pipeline.observer import NoOpObserver

    job = make_chain()
    source = next(
        node for node in job.nodes.values() if isinstance(node, SourceJobNode)
    )

    class Rebind(NoOpObserver):
        def on_node_end(self, node_label, node_hash):
            if node_label == source.label:
                source.bound_source = DictSource(
                    [{"g": 1, "x": 9}], tag_columns=["g"], source_id="same"
                )

    job.run(orchestrator=orchestrator())
    job.run(orchestrator=orchestrator(), observer=Rebind())
    job.run(orchestrator=orchestrator())
    assert job.nodes["grouped"].as_table().column("x").to_pylist() == [[18]]


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_callback_replacing_operator_cache_invalidates_outer_completion(
    orchestrator,
) -> None:
    """A direct call from on_node_end cannot poison the next unchanged job run."""
    from orcapod.pipeline.observer import NoOpObserver

    job = make_chain()
    node = job.nodes["grouped"]
    job.run(orchestrator=orchestrator())
    expected = node.as_table()

    class ReplaceCache(NoOpObserver):
        def on_node_end(self, node_label, node_hash):
            if node_label == "grouped":
                node.execute(DictSource([{"g": 1, "x": 99}], tag_columns=["g"]))

    job.run(orchestrator=orchestrator(), observer=ReplaceCache())
    assert node._completed_execution is None
    job.run(orchestrator=orchestrator())
    assert node.as_table().equals(expected)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_direct_operator_call_consumes_actual_inputs(asynchronous) -> None:
    """Declared graph inputs never authorize reuse for a direct node invocation."""
    import asyncio

    from orcapod.channels import Channel

    job = make_chain()
    job.run(orchestrator=SyncPipelineOrchestrator())
    node = job.nodes["grouped"]
    replacement = DictSource([{"g": 1, "x": 99}], tag_columns=["g"])
    if asynchronous:

        async def run():
            inputs = Channel(buffer_size=1)
            output = Channel(buffer_size=1)

            async def feed():
                for row in replacement.iter_data():
                    await inputs.writer.send(row)
                await inputs.writer.close()

            async with asyncio.TaskGroup() as group:
                group.create_task(feed())
                group.create_task(node.async_execute([inputs.reader], output.writer))
                collected = group.create_task(output.reader.collect())
            return collected.result()

        rows = asyncio.run(run())
    else:
        rows = node.execute(replacement)
    assert rows[0][1].as_dict()["x"] == [99]


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_unknown_source_never_reuses_operator_completion(orchestrator) -> None:
    """A custom source is not assumed immutable from its class ancestry or name."""
    from orcapod.core.nodes.source_node import SourceJobNode

    class ChangingSource(DictSource):
        value = 7

        def iter_data(self):
            yield from DictSource(
                [{"g": 1, "x": self.value}], tag_columns=["g"], source_id="same"
            ).iter_data()

        async def async_iter_data(self):
            for row in self.iter_data():
                yield row

    job = make_chain()
    source = next(
        node for node in job.nodes.values() if isinstance(node, SourceJobNode)
    )
    changing = ChangingSource([{"g": 1, "x": 7}], tag_columns=["g"], source_id="same")
    source.bound_source = changing
    job.run(orchestrator=orchestrator())
    changing.value = 8
    job.run(orchestrator=orchestrator())
    assert job.nodes["grouped"].as_table().column("x").to_pylist() == [[16]]
    assert job.nodes["rows"]._entry_id_rows == {}


@pytest.mark.parametrize("failure_at", ["source", "rows"])
def test_warm_async_upstream_failure_emits_no_cached_rows(failure_at) -> None:
    """Failed producers close channels, but that never authorizes downstream replay."""
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.pipeline.observer import NoOpObserver

    job = make_chain()
    job.run(orchestrator=AsyncPipelineOrchestrator())
    source = next(
        node for node in job.nodes.values() if isinstance(node, SourceJobNode)
    )
    failed_label = source.label if failure_at == "source" else "rows"
    downstream = []

    class FailingObserver(NoOpObserver):
        def on_node_start(self, node_label, node_hash, tag_schema=None):
            if node_label == failed_label:
                raise ValueError("upstream never produced")

        def on_data_start(self, node_label, tag, data):
            if node_label == "total":
                downstream.append(data)

    with pytest.raises(ExceptionGroup):
        job.run(orchestrator=AsyncPipelineOrchestrator(), observer=FailingObserver())
    assert downstream == []


@dataclass
class Base:
    value: int


@dataclass
class Child(Base):
    extra: int


def test_sync_storage_views_are_retained_without_mutating_originals() -> None:
    """The sync boundary decodes the declared type once while preserving live originals."""
    from types import SimpleNamespace

    from orcapod.core.datagrams import Data, Tag

    from orcapod.types import DataValue

    # DataValue's static union does not enumerate registered user logical types.
    original = Data(
        {"x": cast(DataValue, cast(object, Child(1, 99)))}, python_schema={"x": Base}
    )
    tag = Tag(
        {"k": cast(DataValue, cast(object, Child(2, 88)))}, python_schema={"k": Base}
    )
    upstream = SimpleNamespace(
        producer=None, upstreams=(), data_context=original.data_context
    )
    first = SyncPipelineOrchestrator._materialize_as_stream([(tag, original)], upstream)
    second = SyncPipelineOrchestrator._materialize_as_stream(
        [(tag, original)], upstream
    )
    first_tag, first_data = next(first.iter_data())
    second_tag, second_data = next(second.iter_data())
    assert first_tag is second_tag
    assert type(first_tag.as_dict()["k"]) is Base
    assert type(tag.as_dict()["k"]) is Child
    assert first_tag.content_hash() == tag.content_hash()
    assert first_data is second_data
    assert type(first_data.as_dict()["x"]) is Base
    assert type(original.as_dict()["x"]) is Child
    assert cast(Child, cast(object, original.as_dict()["x"])).extra == 99
    assert first_data.content_hash() == original.content_hash()
