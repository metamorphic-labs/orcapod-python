"""An ``EmptyData`` input of a fall-through node is refused (ITL-1023, CodeRabbit on PR #342).

``EmptyData`` stands for an upstream ephemeral result that is gone, so only the result
cache answers. The result store holds the function's plain output; the data columns of
the data-tags that fall through come from the input row, which is gone with their
source info. A hit would return a row without the columns the node declares, so the
node raises ``EphemeralResultMissingError`` instead, on the sync and the async path.
"""

from __future__ import annotations

import asyncio

import pytest

import orcapod as op
from orcapod.core.datagrams.tag_data import EmptyData
from orcapod.core.function_pod import FallThroughPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import EphemeralResultMissingError
from orcapod.pipeline import PipelineJob
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol


def _run_fall_through_node() -> tuple[FunctionJobNode, TagProtocol, EmptyData]:
    """A fall-through node that has computed its one row, and an EmptyData its key hits."""

    @op.function_pod(output_keys=["half"])
    def half(amp: float) -> float:
        return amp / 2

    source = op.sources.DictSource(
        [{"t": 10, "amp": 0.5}], tag_columns=["t"], source_id="s"
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = half.pod(source, label="half")
    job.run()
    node = job.nodes["half"]
    assert isinstance(node, FunctionJobNode)
    assert node.keys() == (("t",), ("t", "half"))
    pod = node._function_pod  # pyright: ignore[reportPrivateUsage]  # the test is about the node's pod
    assert isinstance(pod, FallThroughPod)
    tag, data = next(iter(source.iter_data()))
    # The key the result store holds: the function's own input.
    empty = EmptyData(cached_content_hash=pod.function_input(data).content_hash())
    return node, tag, empty


def test_a_hit_is_refused_on_the_sync_path() -> None:
    node, tag, empty = _run_fall_through_node()
    with pytest.raises(EphemeralResultMissingError, match="fall through"):
        _ = node.execute_data(tag, empty)


def test_a_hit_is_refused_on_the_async_path() -> None:
    node, tag, empty = _run_fall_through_node()

    async def run() -> tuple[TagProtocol, DataProtocol | None]:
        return await node._async_process_data_internal(tag, empty)  # pyright: ignore[reportPrivateUsage]  # the async row path

    with pytest.raises(EphemeralResultMissingError, match="fall through"):
        asyncio.run(run())
