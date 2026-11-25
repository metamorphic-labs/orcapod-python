"""Rows kept as Arrow tables build exactly what the row list built (ITL-1464).

The async path kept every row a node passed on, or a barrier operator gathered, as a
``(Tag, Data)`` pair. ``RowChunks`` keeps them as Arrow tables instead; these tests hold
it to the table and streams the row list gave, over several chunks.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.channels import Channel
from orcapod.core.operators import static_output_pod
from orcapod.core.operators.static_output_pod import (
    RowChunks,
    StaticOutputOperatorPod,
    collect_row_chunks,
    rows_once,
)
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource

_ROWS = 11


def _table() -> pa.Table:
    """Eleven rows: two tags, two data columns (one with nulls), a list column."""
    numbers = list(range(_ROWS))
    return pa.table(
        {
            "subject": pa.array([f"s{n % 3}" for n in numbers], pa.large_string()),
            "trial": pa.array(numbers, pa.int32()),
            "value": pa.array([n + 0.5 for n in numbers], pa.float32()),
            "note": pa.array(
                [None if n % 4 == 0 else f"n{n}" for n in numbers], pa.large_string()
            ),
            "samples": pa.array(
                [[n, n + 1] for n in numbers], pa.large_list(pa.int8())
            ),
        }
    )


def _source(tag_only: bool) -> ArrowTableSource:
    return ArrowTableSource(
        _table(),
        tag_columns=["subject", "trial"],
        tag_only_columns=tag_only,
        record_id_columns=["trial"],
        source_id="rows",
    )


def _rows(tag_only: bool) -> list[tuple[Any, Any]]:
    return list(_source(tag_only).iter_data())


def _chunks(rows: list[tuple[Any, Any]]) -> RowChunks:
    chunks = RowChunks()
    for tag, data in rows:
        chunks.add(tag, data)
    return chunks


@pytest.fixture(params=[1, 3, 4, 11, 8192], ids=lambda n: f"chunk{n}")
def chunk_rows(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> int:
    """Every chunk size: one row, several chunks with and without a rest, one chunk."""
    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", request.param)
    return request.param


@pytest.mark.parametrize("tag_only", [False, True], ids=["data_tags", "tag_only"])
def test_the_row_table_is_the_one_built_from_the_row_list(
    chunk_rows: int, tag_only: bool
) -> None:
    rows = _rows(tag_only)
    expected, tag_keys, data_tags = StaticOutputOperatorPod._rows_to_table(rows)  # pyright: ignore[reportPrivateUsage]  # the rule RowChunks must equal

    table, got_keys, got_data_tags = _chunks(rows).row_table()

    assert (got_keys, got_data_tags) == (tag_keys, data_tags)
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert table.column_names == expected.column_names
    assert table.equals(expected)
    # A token column with no null is declared non-nullable, as from the row list.
    assert not table.schema.field("_source_value").nullable


@pytest.mark.parametrize("tag_only", [False, True], ids=["data_tags", "tag_only"])
def test_the_streams_are_the_ones_built_from_the_row_list(
    chunk_rows: int, tag_only: bool
) -> None:
    rows = _rows(tag_only)
    pipeline_hash = _source(tag_only).pipeline_hash()
    chunks = _chunks(rows)
    everything = {"system_tags": True, "source": True, "context": True}

    output = chunks.output_stream()
    expected_output = StaticOutputOperatorPod._materialize_to_stream(rows)  # pyright: ignore[reportPrivateUsage]  # the builder RowChunks must equal
    assert output.keys() == expected_output.keys()
    assert output.as_table(columns=everything).equals(
        expected_output.as_table(columns=everything)
    )
    assert output.content_hash() == expected_output.content_hash()

    given = chunks.input_stream(pipeline_hash)
    expected_input = StaticOutputOperatorPod._materialize_input_stream(  # pyright: ignore[reportPrivateUsage]  # the builder RowChunks must equal
        rows, pipeline_hash
    )
    assert given.keys() == expected_input.keys()
    assert given.pipeline_hash() == expected_input.pipeline_hash() == pipeline_hash
    assert given.data_context_key == expected_input.data_context_key
    assert given.as_table(columns=everything).equals(
        expected_input.as_table(columns=everything)
    )


def test_rows_with_a_missing_token_keep_their_field_nullable(chunk_rows: int) -> None:
    rows = _rows(False)
    tag, data = rows[5]
    # One row whose token for ``value`` is None, in the middle of a chunk.
    rows[5] = (tag, data.with_source_info(value=None))
    expected = StaticOutputOperatorPod._rows_to_table(rows)[0]  # pyright: ignore[reportPrivateUsage]  # the rule RowChunks must equal

    table = _chunks(rows).row_table()[0]

    assert table.schema.field("_source_value").nullable
    assert not table.schema.field("_source_note").nullable
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert table.equals(expected)


def test_no_rows_is_refused_as_an_empty_row_list_is() -> None:
    chunks = RowChunks()
    assert chunks.num_rows == 0
    with pytest.raises(ValueError, match="empty list of rows"):
        chunks.row_table()


def test_at_most_one_chunk_of_rows_is_held_as_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", 4)
    chunks = _chunks(_rows(False))

    assert chunks.num_rows == _ROWS
    assert len(chunks._pending) == _ROWS % 4  # pyright: ignore[reportPrivateUsage]  # the bound this class exists for
    assert [t.num_rows for t in chunks._tag_chunks] == [4, 4]  # pyright: ignore[reportPrivateUsage]  # the bound this class exists for


def test_collecting_a_channel_keeps_its_rows_in_order(chunk_rows: int) -> None:
    rows = _rows(False)

    async def read() -> RowChunks:
        channel: Channel[tuple[Any, Any]] = Channel(buffer_size=2)

        async def send() -> None:
            for row in rows:
                await channel.writer.send(row)
            await channel.writer.close()

        async with asyncio.TaskGroup() as group:
            _ = group.create_task(send())
            collected = group.create_task(collect_row_chunks(channel.reader))
        return collected.result()

    chunks = asyncio.run(read())

    assert chunks.num_rows == _ROWS
    assert chunks.row_table()[0].equals(
        StaticOutputOperatorPod._rows_to_table(rows)[0]  # pyright: ignore[reportPrivateUsage]  # the rule RowChunks must equal
    )


def test_rows_read_once_are_not_kept_by_the_stream() -> None:
    stream = _source(False).map_data({"value": "amplitude"})
    result = ArrowTableStream(
        stream.as_table(columns={"system_tags": True, "source": True}),
        tag_columns=["subject", "trial"],
        data_tags=["subject", "trial"],
    )

    once = [(t.as_dict(), d.as_dict(), d.source_info()) for t, d in rows_once(result)]

    assert result._cached_elements is None  # pyright: ignore[reportPrivateUsage]  # what rows_once must not fill
    kept = [(t.as_dict(), d.as_dict(), d.source_info()) for t, d in result.iter_data()]
    assert once == kept
    assert len(once) == _ROWS
    # Pairs an earlier iteration kept are the ones a later single read gives.
    assert [id(d) for _, d in rows_once(result)] == [
        id(d) for _, d in result.iter_data()
    ]


def _job(tmp_path: Path, name: str, chain: str, tag_only: bool) -> PipelineJob:
    database = DeltaTableDatabase(str(tmp_path / name))
    with PipelineJob(name="chunks", database=database) as job:
        source = _source(tag_only)
        if chain == "map":
            _ = source.map_data({"value": "amplitude"}, label="out")
        elif chain == "group":
            _ = source.group_by(["subject"], label="out")
        else:
            _ = source.drop_nulls(["note"]).batch(4, label="out")
    return job


def _sorted_rows(table: pa.Table) -> list[dict[str, Any]]:
    names = sorted(table.column_names)
    return sorted(table.select(names).to_pylist(), key=repr)


@pytest.mark.parametrize("tag_only", [False, True], ids=["data_tags", "tag_only"])
@pytest.mark.parametrize("chain", ["map", "group", "batch"])
def test_an_async_job_over_several_chunks_gives_the_sync_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, chain: str, tag_only: bool
) -> None:
    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", 3)
    sync_job = _job(tmp_path, "sync", chain, tag_only)
    _ = sync_job.run()
    async_job = _job(tmp_path, "async", chain, tag_only)
    _ = async_job.run(orchestrator=AsyncPipelineOrchestrator())

    expected = sync_job.nodes["out"].as_table(all_info=True)
    found = async_job.nodes["out"].as_table(all_info=True)

    assert found.num_rows == expected.num_rows > 0
    assert sorted(found.column_names) == sorted(expected.column_names)
    assert {n: found.schema.field(n) for n in found.column_names} == {
        n: expected.schema.field(n) for n in expected.column_names
    }
    if chain == "batch":
        # ``Batch`` groups by arrival on the async path: the rows in order, as sync.
        assert found.select(expected.column_names).equals(expected)
    else:
        assert _sorted_rows(found) == _sorted_rows(expected)
    assert async_job.nodes["out"].content_hash() == sync_job.nodes["out"].content_hash()


def test_an_async_job_run_asks_the_orchestrator_to_keep_no_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asked: list[bool] = []
    original = AsyncPipelineOrchestrator._run_async  # pyright: ignore[reportPrivateUsage]  # the one place the rows are collected

    async def recording(self: Any, graph: Any, materialize_results: bool, **kw: Any):
        asked.append(materialize_results)
        return await original(self, graph, materialize_results, **kw)

    monkeypatch.setattr(AsyncPipelineOrchestrator, "_run_async", recording)
    job = _job(tmp_path, "kept", "map", False)

    _ = job.run(orchestrator=AsyncPipelineOrchestrator())
    assert asked == [False]
    assert job.nodes["out"].as_table().num_rows == _ROWS

    again = _job(tmp_path, "kept2", "map", False)
    _ = asyncio.run(again.run_async())
    assert asked == [False, False]
    assert again.nodes["out"].as_table().num_rows == _ROWS


@pytest.mark.parametrize("chain", ["map", "group", "batch"])
def test_a_jobs_nodes_read_the_same_whatever_materialize_results_says(
    tmp_path: Path, chain: str
) -> None:
    """Decision T-1: the async orchestrator keeps no rows for a job, flag or not."""
    tables = []
    for flag in (True, False):
        job = _job(tmp_path, f"flag_{flag}", chain, False)
        _ = job.run(orchestrator=AsyncPipelineOrchestrator(), materialize_results=flag)
        node = job.nodes["out"]
        rows = [(t.as_dict(), d.as_dict()) for t, d in node.iter_data()]
        tables.append((node.as_table(all_info=True), rows, node.content_hash()))
    (kept, kept_rows, kept_hash), (unkept, unkept_rows, unkept_hash) = tables

    assert kept.num_rows > 0
    assert _sorted_rows(kept) == _sorted_rows(unkept)
    assert sorted(kept_rows, key=repr) == sorted(unkept_rows, key=repr)
    assert kept_hash == unkept_hash
