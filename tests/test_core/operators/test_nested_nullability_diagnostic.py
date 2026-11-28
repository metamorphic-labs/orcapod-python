"""Shared Arrow columns with different child flags need an actionable error."""

import asyncio
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, SourceNode
from orcapod.channels import Channel
from orcapod.errors import InputValidationError, SourceSpecMismatchError
from orcapod.operators import Join, MapData, MergeJoin, SemiJoin
from orcapod.sources import ArrowTableSource


def _source(
    name: str,
    data_name: str,
    *,
    nullable: bool,
    structured: bool = True,
    scalar: pa.DataType = pa.int64(),
):
    item: pa.Field[Any] = (
        pa.field("item", pa.struct([pa.field("code", scalar, nullable=nullable)]))
        if structured
        else pa.field("item", scalar, nullable=nullable)
    )
    key = pa.large_list(item)
    schema = pa.schema([pa.field("k", key), pa.field(data_name, pa.int64())])
    value = [{"code": 1}] if structured else [1]
    table = pa.Table.from_pylist([{"k": value, data_name: 2}], schema=schema)
    return ArrowTableSource(
        table, tag_columns=["k"], tag_only_columns=True, source_id=name
    )


async def _execute(operator, left, right):
    """Feed the async row path with the same explicit Arrow schemas."""
    operator.validate_inputs(left, right)
    inputs = [Channel(buffer_size=4), Channel(buffer_size=4)]
    output = Channel(buffer_size=4)
    for stream, channel in zip((left, right), inputs, strict=True):
        for row in stream.iter_data():
            await channel.writer.send(row)
        await channel.writer.close()
    await operator.async_execute(
        [channel.reader for channel in inputs],
        output.writer,
        input_pipeline_hashes=[left.pipeline_hash(), right.pipeline_hash()],
    )


def _assert_remedy(message, field):
    """Both repairs must be available in the error."""
    assert f"nullability of nested field {field!r}" in message
    assert "declare the same nested flags" in message.lower()
    assert "drop the explicit non-null declaration" in message


@pytest.mark.parametrize("operator_class", [Join, MergeJoin, SemiJoin])
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("structured", [False, True], ids=["list", "struct-item"])
def test_joins_name_the_nested_flag(operator_class, asynchronous, structured):
    """A nested-only mismatch gets the same remedy on both paths."""
    left = _source("a", "v", nullable=True, structured=structured)
    right = _source("b", "w", nullable=False, structured=structured)
    operator = operator_class()
    with pytest.raises((InputValidationError, ExceptionGroup)) as error:
        if asynchronous:
            asyncio.run(_execute(operator, left, right))
        else:
            operator(left, right).as_table()
    value = error.value
    if isinstance(value, ExceptionGroup):
        [value] = value.exceptions
    assert isinstance(value, InputValidationError)
    _assert_remedy(str(value), "k.item.code" if structured else "k.item")


def test_another_type_difference_keeps_the_generic_message():
    """Do not suggest changing flags alone when a scalar width differs."""
    left = _source("a", "v", nullable=True)
    right = _source("b", "w", nullable=False, scalar=pa.int8())
    with pytest.raises(InputValidationError, match="not compatible") as error:
        Join()(left, right).as_table()
    assert "drop the explicit non-null declaration" not in str(error.value)


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("structured", [False, True], ids=["list", "struct-item"])
def test_slot_binding_names_the_nested_flag(asynchronous, structured):
    """Both job routes reject this declaration at their shared binding boundary."""
    slot = SourceNode.from_stream(
        _source("a", "v", nullable=True, structured=structured), name="src"
    )
    other = _source("b", "v", nullable=False, structured=structured)
    with PipelineJob(name="nested_flags") as job:
        MapData({"v": "out"})(slot, label="op")

    async def bind_and_run():
        job.bind(sources={"src": other})
        await job.run_async()

    with pytest.raises(SourceSpecMismatchError) as error:
        if asynchronous:
            asyncio.run(bind_and_run())
        else:
            job.bind(sources={"src": other})
    _assert_remedy(str(error.value), "k.item.code" if structured else "k.item")
