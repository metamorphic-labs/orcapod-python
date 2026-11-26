"""Function pod dispatch limits through async jobs and the channel API."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, cast

import pytest

from orcapod import PipelineJob
from orcapod.channels import Channel
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource
from orcapod.types import PipelineConfig, PodConfig

if TYPE_CHECKING:
    from collections.abc import Iterator

    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol

ROW_COUNT = 24


def _source(reverse: bool) -> DictSource:
    """Build rows with independent data and tag columns in either layout."""
    rows = [
        {"group": f"g{i % 3}", "row": i, "x": i * 2, "y": i + 1}
        for i in range(ROW_COUNT)
    ]
    tag_columns = ["group", "row"]
    if reverse:
        rows = [dict(reversed(list(row.items()))) for row in rows]
        tag_columns.reverse()
    return DictSource(
        data=rows, tag_columns=tag_columns, tag_only_columns=True, source_id="rows"
    )


class _Calls:
    def __init__(self) -> None:
        self.active: int = 0
        self.peak: int = 0

    async def add(self, x: int, y: int) -> int:
        """Yield while active so work from the job's next channel batch overlaps."""
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            return x + y
        finally:
            self.active -= 1


@pytest.mark.parametrize("reverse", [False, True])
def test_async_job_bounds_calls_and_preserves_sync_results(reverse: bool) -> None:
    """Exercise default, numeric and unlimited dispatch through a real job."""
    expected_rows = [
        {"group": f"g{i % 3}", "row": i, "total": 3 * i + 1} for i in range(ROW_COUNT)
    ]
    for config, expected_peak in [
        (PodConfig(max_concurrency=3), 3),
        (PodConfig(max_concurrency=None), ROW_COUNT),
        (None, 16),
    ]:
        calls = _Calls()
        pod = FunctionPod.from_fn(calls.add, output_keys="total", pod_config=config)
        job = PipelineJob(name="concurrency", database=InMemoryArrowDatabase())
        with job:
            _ = pod(_source(reverse), label="add")

        async def run() -> None:
            async with asyncio.timeout(60):
                _ = await job.run_async()

        asyncio.run(run())
        async_node = cast(FunctionJobNode, job.nodes["add"])
        async_rows = async_node.as_table().select(["group", "row", "total"])
        assert async_rows.sort_by("row").to_pylist() == expected_rows
        assert calls.peak == expected_peak, config

        # A fresh store ensures that the sync run calls the function again.
        calls.peak = 0
        sync_job = PipelineJob(name="concurrency", database=InMemoryArrowDatabase())
        with sync_job:
            _ = pod(_source(reverse), label="add")
        _ = sync_job.run()
        sync_node = cast(FunctionJobNode, sync_job.nodes["add"])
        sync_rows = sync_node.as_table().select(["group", "row", "total"])
        assert sync_rows.sort_by("row").to_pylist() == expected_rows
        assert calls.peak == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse", [False, True])
async def test_pod_concurrency_takes_precedence_over_pipeline(reverse: bool) -> None:
    """Count actual calls through the async API that accepts PipelineConfig."""
    for config, pipeline_limit, expected_peak in [
        (PodConfig(), 4, 4),
        (PodConfig(max_concurrency=3), 4, 3),
        (PodConfig(), None, ROW_COUNT),
        (PodConfig(max_concurrency=None), 4, ROW_COUNT),
    ]:
        calls = _Calls()
        pod = FunctionPod.from_fn(calls.add, output_keys="total", pod_config=config)
        input_channel: Channel[tuple[TagProtocol, DataProtocol]] = Channel(
            buffer_size=ROW_COUNT
        )
        output_channel: Channel[tuple[TagProtocol, DataProtocol]] = Channel(
            buffer_size=ROW_COUNT
        )
        for row in cast(
            "Iterator[tuple[TagProtocol, DataProtocol]]", _source(reverse).iter_data()
        ):
            await input_channel.writer.send(row)
        await input_channel.writer.close()

        async with asyncio.timeout(60):
            await pod.async_execute(
                [input_channel.reader],
                output_channel.writer,
                pipeline_config=PipelineConfig(default_max_concurrency=pipeline_limit),
            )
        rows = await output_channel.reader.collect()
        actual = [
            (tag.as_dict()["row"], tag.as_dict()["group"], data.as_dict()["total"])
            for tag, data in rows
        ]
        assert sorted(actual) == [(i, f"g{i % 3}", 3 * i + 1) for i in range(ROW_COUNT)]
        assert calls.peak == expected_peak, (config, pipeline_limit)


def add(x: int, y: int) -> int:
    """Importable function for the public config round trip."""
    return x + y


def test_function_pod_concurrency_config_round_trip() -> None:
    """Preserve omission and numbers while round-tripping explicit unlimited."""
    cases: list[tuple[PodConfig | None, dict[str, int | None] | None]] = [
        (None, None),
        (PodConfig(max_concurrency=3), {"max_concurrency": 3}),
        (PodConfig(max_concurrency=None), {"max_concurrency": None}),
    ]
    for config, expected in cases:
        pod = FunctionPod.from_fn(add, output_keys="total", pod_config=config)
        saved = json.dumps(pod.to_config())
        payload = cast(dict[str, object], json.loads(saved))
        assert payload["pod_config"] == expected
        restored = FunctionPod.from_config(payload)
        assert json.dumps(restored.to_config()) == saved
        assert restored.pod_config == pod.pod_config
