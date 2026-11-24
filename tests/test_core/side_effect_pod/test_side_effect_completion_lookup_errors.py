"""A failed prior-completion lookup names the side-effect node and the row (ITL-919).

A side-effect pod that tracks completion looks up the batch's invocation records before
delivering it. When that lookup cannot read the database, the ``OrcapodDatabaseReadError`` names the
pod and the row's tag, as a failed write of the same record does (CodeRabbit on #290).
"""

from __future__ import annotations

import asyncio
from collections.abc import Collection
from pathlib import Path
from typing import Any

import pytest

from orcapod.channels import Channel
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.errors import OrcapodDatabaseReadError
from orcapod.pipeline.job import PipelineJob
from orcapod.side_effects import SideEffectJobNode, SideEffectPod, SideEffectPodConfig
from orcapod.sources import DictSource


class UnreadableCompletions:
    """A store whose batched lookups (the completion check) cannot read."""

    def __init__(self, inner: Any) -> None:
        """Wrap ``inner``."""
        self._inner = inner

    def at(self, *path_components: str) -> UnreadableCompletions:
        """A view with the same failure."""
        return UnreadableCompletions(self._inner.at(*path_components))

    def get_records_by_column(
        self, record_path: tuple[str, ...], *args: Any, **kwargs: Any
    ):
        """Fail as an unreadable stored table would."""
        raise OrcapodDatabaseReadError(
            "/store/" + "/".join(record_path), tuple(record_path)
        )

    def __getattr__(self, name: str) -> Any:
        """Delegate everything else."""
        return getattr(self._inner, name)


def test_completion_lookup_failure_names_the_node_and_row() -> None:
    """The lookup's ``OrcapodDatabaseReadError`` carries the pod's label and the row's tag."""
    delivered: list[int] = []

    def deliver(value: int, ctx: Any) -> None:
        delivered.append(value)

    pod = SideEffectPod(deliver, config=SideEffectPodConfig(track_completion=True))
    store = UnreadableCompletions(InMemoryArrowDatabase())
    job = PipelineJob(name="effects", database=store)  # pyright: ignore[reportArgumentType]  # failing wrapper of a real store
    with job:
        pod.process(
            DictSource(
                [{"id": 0, "value": 10}], tag_columns=["id"], tag_only_columns=True
            )
        )

    with pytest.raises(OrcapodDatabaseReadError) as info:
        job.run()
    assert info.value.tag == {"id": 0}
    assert info.value.node_label is not None
    assert delivered == []


@pytest.mark.parametrize("asynchronous", [False, True])
def test_delta_reads_the_completion_batch_once(
    asynchronous: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Batch sync completion reads and preserve the async point-read path and rows."""
    delivered: list[int] = []

    def deliver(value: int, ctx: Any) -> None:
        delivered.append(value)

    values = list(reversed(range(100)))
    source = DictSource(
        [{"id": value, "value": value} for value in values],
        tag_columns=["id"],
        tag_only_columns=True,
    )
    node = SideEffectJobNode(SideEffectPod(deliver), source)
    store = DeltaTableDatabase(str(tmp_path))
    node.attach_databases(store)
    assert len(list(node.execute(source).iter_data())) == len(values)
    assert delivered == values
    store.flush()

    single_reads = 0
    batch_sizes: list[int] = []
    single = store.get_record_by_id
    batch = store.get_records_by_column

    def count_single(*args: Any, **kwargs: Any):
        nonlocal single_reads
        single_reads += 1
        return single(*args, **kwargs)

    def count_batch(
        path: tuple[str, ...], column: str, ids: Collection[Any], **kwargs: Any
    ):
        batch_sizes.append(len(ids))
        return batch(path, column, ids, **kwargs)

    monkeypatch.setattr(store, "get_record_by_id", count_single)
    monkeypatch.setattr(store, "get_records_by_column", count_batch)

    async def run_async():
        input_channel: Channel[tuple[TagProtocol, DataProtocol]] = Channel(
            buffer_size=100
        )
        output_channel: Channel[tuple[TagProtocol, DataProtocol]] = Channel(
            buffer_size=100
        )
        for row in source.iter_data():
            await input_channel.writer.send(row)
        await input_channel.writer.close()
        await node.async_execute([input_channel.reader], output_channel.writer)
        return await output_channel.reader.collect()

    if asynchronous:
        output = asyncio.run(run_async())
        assert {tag.as_dict()["id"] for tag, _ in output} == set(values)
        assert len(output) == len(values)
    else:
        output = list(node.execute(source).iter_data())
        assert [tag.as_dict()["id"] for tag, _ in output] == values

    assert delivered == values
    if asynchronous:
        assert single_reads == len(values)
        assert batch_sizes == []
    else:
        assert single_reads == 0
        assert batch_sizes == [len(values)]


def test_duplicate_in_the_batch_retries_a_failure_and_reuses_the_pending_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts: list[int] = []

    def deliver(value: int, ctx: Any) -> None:
        attempts.append(value)
        if len(attempts) == 1:
            raise RuntimeError("first delivery failed")

    source = DictSource(
        [{"id": 0, "value": 10}], tag_columns=["id"], tag_only_columns=True
    )
    row = next(source.iter_data())
    monkeypatch.setattr(source, "iter_data", lambda: iter([row, row, row]))
    pod = SideEffectPod(
        deliver, config=SideEffectPodConfig(on_error="log", drop_on_failure=False)
    )
    node = SideEffectJobNode(pod, source)
    node.attach_databases(InMemoryArrowDatabase())

    output = list(node.execute(source).iter_data())
    assert [tag.as_dict()["id"] for tag, _ in output] == [0, 0, 0]
    assert attempts == [10, 10]


def test_a_partial_batch_delivers_its_prefix_before_raising_the_input_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delivered: list[int] = []

    def deliver(value: int, ctx: Any) -> None:
        delivered.append(value)

    source = DictSource(
        [{"id": 0, "value": 10}], tag_columns=["id"], tag_only_columns=True
    )
    row = next(source.iter_data())

    def rows():
        yield row
        raise RuntimeError("input failed")

    monkeypatch.setattr(source, "iter_data", rows)
    node = SideEffectJobNode(SideEffectPod(deliver), source)
    node.attach_databases(InMemoryArrowDatabase())
    with pytest.raises(RuntimeError, match="input failed"):
        node.execute(source)
    assert delivered == [10]
