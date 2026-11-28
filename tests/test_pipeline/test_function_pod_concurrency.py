"""Function pod dispatch limits through async jobs and the channel API."""

from __future__ import annotations

import asyncio
import json
import os
from itertools import product
from typing import TYPE_CHECKING, cast

import pytest

from orcapod import PipelineJob
from orcapod.channels import Channel
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource
from orcapod.system_constants import constants
from orcapod.types import (
    DEFAULT_FUNCTION_POD_CONCURRENCY_CAP,
    PipelineConfig,
    PodConfig,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pyarrow as pa

    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol

ROW_COUNT = 24


def _source(reverse: bool, *, tag_only: bool | list[str] = True) -> DictSource:
    """Build rows with the requested tag roles in either layout."""
    rows = [
        {"group": f"g{i % 3}", "row": i, "x": i * 2, "y": i + 1}
        for i in range(ROW_COUNT)
    ]
    tag_columns = ["group", "row"]
    if reverse:
        rows = [dict(reversed(list(row.items()))) for row in rows]
        tag_columns.reverse()
    return DictSource(
        data=rows, tag_columns=tag_columns, tag_only_columns=tag_only, source_id="rows"
    )


class _Calls:
    def __init__(self, *, reverse_outputs: bool = False) -> None:
        self.active: int = 0
        self.peak: int = 0
        self.reverse_outputs: bool = reverse_outputs

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

    async def add_and_subtract(self, x: int, y: int) -> tuple[int, int]:
        """Return the same named outputs in either declaration order."""
        total = await self.add(x, y)
        difference = x - y
        return (difference, total) if self.reverse_outputs else (total, difference)


def _node_identity(
    node: FunctionJobNode, table: pa.Table
) -> tuple[str, str, tuple[str, ...]]:
    """Capture the two hashes and the source's system-tag column names."""
    system_tags = tuple(
        name
        for name in table.column_names
        if name.startswith(constants.SYSTEM_TAG_PREFIX)
    )
    assert len(system_tags) == 2
    return (
        node.content_hash().to_string(),
        node.pipeline_hash().to_string(),
        system_tags,
    )


@pytest.mark.parametrize(
    ("cpu_count", "default_peak"), [(4, 4), (64, DEFAULT_FUNCTION_POD_CONCURRENCY_CAP)]
)
@pytest.mark.parametrize(
    ("tag_only", "multiple_outputs"),
    [
        pytest.param(True, False, id="single-output"),
        pytest.param(True, True, id="plain-tags"),
        pytest.param(["row"], True, id="mixed-tags"),
        pytest.param([], True, id="data-tags"),
    ],
)
def test_async_job_bounds_calls_and_preserves_sync_results(
    monkeypatch: pytest.MonkeyPatch,
    cpu_count: int,
    default_peak: int,
    tag_only: bool | list[str],
    multiple_outputs: bool,
) -> None:
    """Exercise default, numeric and unlimited dispatch through a real job."""
    monkeypatch.setattr(os, "process_cpu_count", lambda: cpu_count, raising=False)
    expected_rows = [
        {"group": f"g{i % 3}", "row": i, "total": 3 * i + 1} for i in range(ROW_COUNT)
    ]
    if multiple_outputs:
        for i, row in enumerate(expected_rows):
            row["difference"] = i - 1

    dispatch_settings = [
        (PodConfig(max_concurrency=3), 3),
        (PodConfig(max_concurrency=None), ROW_COUNT),
        (None, default_peak),
    ]
    baseline_identity: tuple[str, str, tuple[str, ...]] | None = None
    for reverse, (config, expected_peak) in product((False, True), dispatch_settings):
        output_keys = ["total", "difference"] if multiple_outputs else ["total"]
        if reverse:
            output_keys.reverse()
        tag_order = ["row", "group"] if reverse else ["group", "row"]
        data_tags = (
            [tag for tag in tag_order if tag not in tag_only]
            if isinstance(tag_only, list)
            else []
        )
        expected_data_keys = [*data_tags, *output_keys]
        columns = [*tag_order, *output_keys]

        calls = _Calls(reverse_outputs=reverse)
        function = calls.add_and_subtract if multiple_outputs else calls.add
        pod = FunctionPod.from_fn(function, output_keys=output_keys, pod_config=config)
        job = PipelineJob(name="concurrency", database=InMemoryArrowDatabase())
        with job:
            _ = pod(_source(reverse, tag_only=tag_only), label="add")

        async def run() -> None:
            async with asyncio.timeout(60):
                _ = await job.run_async()

        asyncio.run(run())
        async_node = cast(FunctionJobNode, job.nodes["add"])
        assert list(async_node.keys()[1]) == expected_data_keys
        async_table = async_node.as_table(columns={"system_tags": True})
        assert async_table.select(columns).sort_by("row").to_pylist() == expected_rows
        async_identity = _node_identity(async_node, async_table)
        if baseline_identity is None:
            baseline_identity = async_identity
        # Only presentation order changes when the layout and equally typed outputs reverse.
        assert async_identity == baseline_identity, (reverse, config)
        assert calls.peak == expected_peak, config

        # A fresh store ensures that the sync run calls the function again.
        calls.peak = 0
        sync_job = PipelineJob(name="concurrency", database=InMemoryArrowDatabase())
        with sync_job:
            _ = pod(_source(reverse, tag_only=tag_only), label="add")
        _ = sync_job.run()
        sync_node = cast(FunctionJobNode, sync_job.nodes["add"])
        assert list(sync_node.keys()[1]) == expected_data_keys
        sync_table = sync_node.as_table(columns={"system_tags": True})
        assert sync_table.select(columns).sort_by("row").to_pylist() == expected_rows
        assert _node_identity(sync_node, sync_table) == baseline_identity, (
            reverse,
            config,
        )
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
