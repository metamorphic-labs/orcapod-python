"""The barrier ``async_execute`` keeps its input rows' data context (CodeRabbit on PR 472).

A barrier operator collects its input rows and rebuilds each input as a stream
(``StaticOutputOperatorPod._materialize_input_stream``). The rebuilt stream must be in
the rows' data context, with each row's context, as the sync path is; otherwise every
emitted row would name the default context. One data context ships, so a copy of it
under another key stands in for a second one. The cases are the barrier operators whose
sync path keeps the data context already.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

import orcapod.contexts as contexts
from orcapod.channels import Channel
from orcapod.core.operators import Batch, GroupBy, Rejoin
from orcapod.core.streams import ArrowTableStream
from orcapod.system_constants import constants

OTHER = "std:v1:test-barrier-other"


@pytest.fixture
def other_context(monkeypatch: pytest.MonkeyPatch) -> contexts.DataContext:
    """A copy of the default data context, registered under another key."""
    other = dataclasses.replace(contexts.get_default_context(), context_key=OTHER)
    registry = contexts._get_registry()  # pyright: ignore[reportPrivateUsage]  # the test registers a second context
    original = registry.get_context

    def get_context(key: Any = None) -> contexts.DataContext:  # pyright: ignore[reportExplicitAny]  # the registry's own argument
        return other if key == OTHER else original(key)

    monkeypatch.setattr(registry, "get_context", get_context)
    return other


def _stream(context: contexts.DataContext, data: tuple[str, str]) -> ArrowTableStream:
    """A raw stream of ``context``: tags ``g`` and ``h``, two data columns, one system
    tag."""
    table = pa.table(
        {
            "h": pa.array(["a", "b", "c"], pa.large_string()),
            "g": pa.array([3, 1, 2], pa.int64()),
            data[0]: pa.array([0.5, 0.25, 0.125], pa.float64()),
            data[1]: pa.array([1, 2, 3], pa.int64()),
            "origin": pa.array(["r1", "r2", "r3"], pa.large_string()),
            constants.CONTEXT_KEY: pa.array(
                [context.context_key] * 3, pa.large_string()
            ),
        }
    )
    return ArrowTableStream(
        table,
        tag_columns=["h", "g"],
        system_tag_columns=["origin"],
        data_context=context,
    )


# (label, operator factory, input data column pairs)
CASES: list[tuple[str, Callable[[], Any], list[tuple[str, str]]]] = [  # pyright: ignore[reportExplicitAny]  # operator factories
    ("rejoin", Rejoin, [("x", "n"), ("y", "m")]),
    ("group_by", lambda: GroupBy(["g", "h"]), [("x", "n")]),
    ("batch", lambda: Batch(2), [("x", "n")]),
]


@pytest.mark.parametrize(("label", "make_op", "data"), CASES, ids=[c[0] for c in CASES])
def test_the_async_rows_keep_the_inputs_data_context(
    other_context: contexts.DataContext,
    label: str,
    make_op: Callable[[], Any],  # pyright: ignore[reportExplicitAny]  # operator factory
    data: list[tuple[str, str]],
) -> None:
    streams = [_stream(other_context, columns) for columns in data]
    sync = make_op().static_process(*streams)
    assert {d.data_context_key for _, d in sync.iter_data()} == {OTHER}

    async def run() -> list[Any]:  # pyright: ignore[reportExplicitAny]  # (Tag, Data) rows
        inputs = []
        for stream in streams:
            channel: Channel[Any] = Channel()  # pyright: ignore[reportExplicitAny]  # (Tag, Data) rows
            for row in stream.iter_data():
                await channel.writer.send(row)
            await channel.writer.close()
            inputs.append(channel.reader)
        out: Channel[Any] = Channel()  # pyright: ignore[reportExplicitAny]  # (Tag, Data) rows
        await make_op().async_execute(
            inputs,
            out.writer,
            input_pipeline_hashes=[stream.pipeline_hash() for stream in streams],
        )
        return await out.reader.collect()

    rows = asyncio.run(run())
    assert rows
    assert {tag.data_context_key for tag, _ in rows} == {OTHER}
    assert {d.data_context_key for _, d in rows} == {OTHER}
