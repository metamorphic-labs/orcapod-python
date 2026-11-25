"""An async job's edges carry record batches (ITL-1464): the rows are the row form's."""

from __future__ import annotations

import asyncio
from typing import Any

import pyarrow as pa
import pytest

from orcapod import FunctionPod, PipelineJob, col
from orcapod.channels import Channel
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.job_edges import EdgeFeed, EdgeReader, EdgeWriter, KeptBatches
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.operators import static_output_pod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.utils import source_tokens

_ALL = {"system_tags": True, "source": True}


def _events(tag_only: bool, rows: int = 23) -> ArrowTableSource:
    """Events with two tags and two data columns, some values null."""
    return ArrowTableSource(
        pa.table(
            {
                "time": pa.array(range(rows), pa.int32()),
                "item": pa.array([i % 4 for i in range(rows)], pa.int16()),
                "size": pa.array(
                    [None if i % 5 == 0 else i / 2 for i in range(rows)], pa.float32()
                ),
                "name": [f"n{i % 3}" for i in range(rows)],
            }
        ),
        tag_columns=["item", "time"],
        tag_only_columns=tag_only,
        source_id="events",
    )


def _items(tag_only: bool) -> ArrowTableSource:
    """One row per item, one item without events."""
    return ArrowTableSource(
        pa.table({"item": pa.array([0, 1, 2, 9], pa.int16()), "depth": [1, 2, 3, 4]}),
        tag_columns=["item"],
        tag_only_columns=tag_only,
        source_id="items",
    )


def _double(size: float) -> float:
    return size * 2


def _build(chain: str, tag_only: bool) -> Any:
    events, items = _events(tag_only), _items(tag_only)
    if chain == "rows":
        kept = events.drop_nulls(["size"]).filter(col("time") > 3)
        return kept.map_data({"name": "label"})
    if chain == "semi_join":
        return events.semi_join(items.filter(col("depth") > 1))
    if chain == "join":
        return events.drop_nulls(["size"]).join(items)
    if chain == "group_then_join":
        return events.group_by(["item"]).join(items).map_data({"size": "sizes"})
    if chain == "batch":
        return events.select_data_columns(["size"]).batch(batch_size=4)
    if chain == "function_between":
        pod = FunctionPod(PythonDataFunction(_double, output_keys="twice"))  # pyright: ignore[reportArgumentType]  # its class is a data function
        return pod(events.drop_nulls(["size"])).join(items)
    if chain == "diamond":
        small = events.select_data_columns(["size"])
        named = events.select_data_columns(["name"])
        return small.join(named)
    assert chain == "no_rows"
    return events.filter(col("time") > 1000).join(items)


def _run(chain: str, tag_only: bool) -> dict[str, Any]:
    with PipelineJob(name="edges", database=InMemoryArrowDatabase()) as job:
        _ = _build(chain, tag_only)
    _ = job.run(orchestrator=AsyncPipelineOrchestrator())
    found: dict[str, Any] = {}
    for name, node in job.nodes.items():
        if not isinstance(node, OperatorJobNode):
            continue
        table = node.as_table(columns=_ALL)
        # The token of a function's output holds an id of the run.
        table = table.drop_columns(
            [n for n in table.column_names if n == "_source_twice"]
        )
        rows = sorted(map(repr, table.to_pylist()))
        found[name] = (str(table.schema), rows, str(node.content_hash()))
    return found


@pytest.mark.parametrize("tag_only", [False, True], ids=["data_tags", "tag_only"])
@pytest.mark.parametrize("chunk", [1, 3, 8192])
@pytest.mark.parametrize(
    "chain",
    [
        "rows",
        "semi_join",
        "join",
        "group_then_join",
        "function_between",
        "diamond",
        "no_rows",
    ],
)
def test_every_node_holds_what_the_row_form_gives(
    chain: str, chunk: int, tag_only: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", chunk)
    found = _run(chain, tag_only)
    monkeypatch.setattr(OperatorJobNode, "_runs_over_batches", lambda *_: False)
    assert found == _run(chain, tag_only)


@pytest.mark.parametrize("chunk", [1, 3, 8192])
def test_batches_of_rows_hold_every_row(
    chunk: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", chunk)
    with PipelineJob(name="edges", database=InMemoryArrowDatabase()) as job:
        _ = _build("batch", False)
    _ = job.run(orchestrator=AsyncPipelineOrchestrator())
    (node,) = [n for n in job.nodes.values() if n.node_type == "operator"][-1:]
    lists = node.as_table()["time"].to_pylist()
    assert sorted(len(members) for members in lists) == [3, 4, 4, 4, 4, 4]
    assert sorted(t for members in lists for t in members) == list(range(23))


def test_a_reader_of_pairs_and_one_of_batches_get_the_same_rows() -> None:
    source = _events(False)
    (batch,) = asyncio.run(source.open_channel().collect())

    async def read() -> tuple[list[Any], list[pa.RecordBatch]]:
        async def filled() -> EdgeReader:
            channel: Channel[Any] = Channel()
            writer = EdgeWriter(channel.writer)
            rows = list(source.iter_data())
            for row in rows[:5]:
                await writer.send(row)
            await writer.send_batch(batch.slice(5), source)
            await writer.close()
            return EdgeReader(channel.reader)

        pairs = await (await filled()).collect()
        feed, batches = EdgeFeed(await filled(), source), []
        while (found := await feed.next()) is not None:
            batches.append(found)
        return pairs, batches

    pairs, batches = asyncio.run(read())
    assert [len(batches), batches[0].num_rows] == [2, 5]
    assert [t["time"] for t, _ in pairs] == list(range(23))
    text = [
        row
        for b in batches
        for row in source_tokens.materialize(pa.Table.from_batches([b])).to_pylist()
    ]
    expected = [
        {
            **t.as_dict(all_info=False),
            **t.system_tags(),
            **d.as_dict(),
            **{f"_source_{k}": v for k, v in d.source_info().items()},
        }
        for t, d in source.iter_data()
    ]
    assert text == expected
    for (tag, data), (wanted_tag, wanted_data) in zip(
        pairs, source.iter_data(), strict=True
    ):
        assert tag.content_hash() == wanted_tag.content_hash()
        assert data.content_hash() == wanted_data.content_hash()
        assert data.source_info() == wanted_data.source_info()


@pytest.mark.parametrize("derived_first", [False, True])
def test_kept_batches_of_two_token_forms_become_text(derived_first: bool) -> None:
    source = _events(True)
    (derived,) = asyncio.run(source.open_channel().collect())
    (text,) = (
        source_tokens.materialize(pa.Table.from_batches([derived]))
        .combine_chunks()
        .to_batches()
    )
    halves = (derived, text) if derived_first else (text, derived)
    kept = KeptBatches(source)
    kept.add(halves[0].slice(0, 10))
    kept.add(halves[1].slice(10))

    table = kept.output_stream().as_table(columns=_ALL)
    kept.close()

    assert table.equals(pa.Table.from_batches([text]).select(table.column_names))
