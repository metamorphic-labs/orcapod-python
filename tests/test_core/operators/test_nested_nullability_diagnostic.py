"""Shared Arrow columns with different child flags need an actionable error."""

import asyncio
from itertools import combinations
from typing import Any
from unittest.mock import Mock

import pyarrow as pa
import pytest

from orcapod import PipelineJob, SourceNode
from orcapod.channels import Channel
from orcapod.errors import InputValidationError, SourceSpecMismatchError
from orcapod.operators import Join, MapData, MergeJoin, SemiJoin
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.sources import ArrowTableSource
from orcapod.utils.arrow_utils import nested_nullability_differences


class _ListExtension(pa.ExtensionType):
    """An extension whose identifier is independent of its list storage flags."""

    def __init__(self, storage_type: pa.DataType, metadata: bytes = b"") -> None:
        self._metadata = metadata
        super().__init__(storage_type, "test.nested_nullability_list")

    def __arrow_ext_serialize__(self) -> bytes:
        return self._metadata

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "_ListExtension":
        return cls(storage_type, serialized)


def _extension_list(nullable: bool, metadata: bytes = b"") -> _ListExtension:
    return _ListExtension(
        pa.large_list(pa.field("item", pa.int64(), nullable=nullable)), metadata
    )


def _source(
    name: str,
    data_name: str,
    *,
    nullable: bool,
    shape: str = "struct-item",
    scalar: pa.DataType = pa.int64(),
    data_tag: bool = False,
):
    item: pa.Field[Any] = (
        pa.field("item", pa.struct([pa.field("code", scalar, nullable=nullable)]))
        if shape == "struct-item"
        else pa.field("item", scalar, nullable=nullable)
    )
    key = pa.large_list(item)
    value = [{"code": 1}] if shape == "struct-item" else [1]
    keys: pa.Array[Any] = pa.array([value], type=key)
    if shape == "extension-list":
        keys = pa.ExtensionArray.from_storage(_ListExtension(key), keys)
    table = pa.table({"k": keys, data_name: pa.array([2], type=pa.int64())})
    return ArrowTableSource(
        table,
        tag_columns=["k"],
        tag_only_columns=not data_tag,
        # ArrowTableSource infers data-tags from tag_only_columns.
        source_id=name,
    )


async def _execute(
    operator: Join | MergeJoin | SemiJoin, *streams: ArrowTableSource
) -> None:
    """Feed the async row path with the same explicit Arrow schemas."""
    operator.validate_inputs(*streams)
    inputs = [Channel[tuple[TagProtocol, DataProtocol]](buffer_size=4) for _ in streams]
    output = Channel[tuple[TagProtocol, DataProtocol]](buffer_size=4)
    for stream, channel in zip(streams, inputs, strict=True):
        for row in stream.iter_data():
            await channel.writer.send(row)
        await channel.writer.close()
    await operator.async_execute(
        [channel.reader for channel in inputs],
        output.writer,
        input_pipeline_hashes=[stream.pipeline_hash() for stream in streams],
    )


def _assert_remedy(message: str, field: str) -> None:
    """Both repairs must be available in the error."""
    assert f"nullability of nested field {field!r}" in message
    assert "declare the same nested flags" in message.lower()
    assert "drop the explicit non-null declaration" in message


@pytest.mark.parametrize("operator_class", [Join, MergeJoin, SemiJoin])
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("shape", ["list", "struct-item", "extension-list"])
def test_joins_name_the_nested_flag(operator_class, asynchronous, shape):
    """A nested-only mismatch gets the same remedy on both paths."""
    left = _source("a", "v", nullable=True, shape=shape)
    right = _source("b", "w", nullable=False, shape=shape)
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
    _assert_remedy(str(value), "k.item.code" if shape == "struct-item" else "k.item")


def test_another_type_difference_keeps_the_generic_message():
    """Do not suggest changing flags alone when a scalar width differs."""
    left = _source("a", "v", nullable=True)
    right = _source("b", "w", nullable=False, scalar=pa.int8())
    with pytest.raises(InputValidationError, match="not compatible") as error:
        Join()(left, right).as_table()
    assert "drop the explicit non-null declaration" not in str(error.value)


def test_join_names_nested_flag_in_shared_data_tag():
    """A merged data-tag type mismatch gets the same nested-field remedy."""
    left = _source("a", "v", nullable=True, shape="struct-item", data_tag=True)
    right = _source("b", "w", nullable=False, shape="struct-item", data_tag=True)
    with pytest.raises(InputValidationError) as error:
        Join()(left, right).as_table()
    _assert_remedy(str(error.value), "k.item.code")


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("reverse_inputs", [False, True], ids=["forward", "reverse"])
@pytest.mark.parametrize(
    ("arity", "pair"),
    [(arity, pair) for arity in (3, 4) for pair in combinations(range(arity), 2)],
)
def test_join_names_nested_flag_in_any_data_tag_pair(
    monkeypatch: pytest.MonkeyPatch,
    arity: int,
    pair: tuple[int, int],
    reverse_inputs: bool,
    asynchronous: bool,
) -> None:
    """Join diagnoses the conflicting data-tag at any two canonical positions."""
    streams: list[ArrowTableSource] = []
    for position in range(arity):
        columns: dict[str, pa.Array[Any]] = {
            "batch": pa.array([1]),
            "group": pa.array([2]),
            f"value_{position}": pa.array([3]),
        }
        tags = ["batch", "group"]
        if position in pair:
            item = pa.struct(
                [pa.field("code", pa.int64(), nullable=position == pair[0])]
            )
            columns["k"] = pa.array([[{"code": 1}]], type=pa.large_list(item))
            tags.append("k")
        if reverse_inputs:
            columns = dict(reversed(list(columns.items())))
            tags.reverse()
        streams.append(
            ArrowTableSource(
                pa.Table.from_arrays(list(columns.values()), names=list(columns)),
                tag_columns=tags,
                tag_only_columns=["batch"],
                source_id=f"input_{position}",
            )
        )

    # Assign the real hashes by rank so the pair occupies the specified
    # positions while Join still sorts the inputs itself.
    hashes = sorted(
        (stream.pipeline_hash() for stream in streams), key=lambda h: h.to_hex()
    )
    for stream, pipeline_hash in zip(streams, hashes, strict=True):
        monkeypatch.setattr(stream, "pipeline_hash", Mock(return_value=pipeline_hash))
    if reverse_inputs:
        streams.reverse()

    operator = Join()
    with pytest.raises(InputValidationError) as error:
        if asynchronous:
            asyncio.run(_execute(operator, *streams))
        else:
            _ = operator(*streams).as_table()
    _assert_remedy(str(error.value), "k.item.code")


def test_a_flag_and_a_logical_identifier_are_two_differences():
    """Logical identifiers in Arrow field metadata can also distinguish types."""
    left = pa.struct(
        [
            pa.field(
                "value", pa.large_string(), metadata={b"ARROW:extension:name": b"path"}
            )
        ]
    )
    right = pa.struct(
        [
            pa.field(
                "value",
                pa.large_string(),
                nullable=False,
                metadata={b"ARROW:extension:name": b"file"},
            )
        ]
    )
    assert nested_nullability_differences(left, right) == ()


def test_an_extension_flag_and_different_serialized_metadata_are_two_differences():
    """Extension storage is comparable only when its full identifier agrees."""
    assert (
        nested_nullability_differences(
            _extension_list(True, b"first"), _extension_list(False, b"second")
        )
        == ()
    )


def test_an_extension_flag_and_a_storage_layout_are_two_differences():
    """Changing flags alone cannot reconcile a storage layout mismatch."""
    left = _ListExtension(pa.list_(pa.int64()))
    assert nested_nullability_differences(left, _extension_list(False)) == ()


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("shape", ["list", "struct-item", "extension-list"])
def test_slot_binding_names_the_nested_flag(asynchronous, shape):
    """Both job routes reject this declaration at their shared binding boundary."""
    slot = SourceNode.from_stream(
        _source("a", "v", nullable=True, shape=shape), name="src"
    )
    other = _source("b", "v", nullable=False, shape=shape)
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
    _assert_remedy(
        str(error.value), "k.item.code" if shape == "struct-item" else "k.item"
    )
