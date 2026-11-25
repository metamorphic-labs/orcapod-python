"""Rows kept as Arrow tables build exactly what the row list built (ITL-1464).

The async path kept every row a node passed on, or a barrier operator gathered, as a
``(Tag, Data)`` pair. ``RowChunks`` keeps them as Arrow tables instead; these tests hold
it to the table and streams the row list gave, over several chunks.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

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
    assert chunks._spool.size == 8  # pyright: ignore[reportPrivateUsage]  # the bound this class exists for


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


@pytest.fixture()
def spilled(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Every sealed chunk goes to disk at once, three rows a chunk."""
    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", 3)
    monkeypatch.setattr(static_output_pod, "KEPT_ROWS_MEMORY_BYTES", 0)
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    return tmp_path


def _spool_files(root: Path) -> list[Path]:
    return [p for p in root.rglob("*") if p.is_file() and not p.name.endswith("lock")]


@pytest.mark.parametrize("tag_only", [False, True], ids=["data_tags", "tag_only"])
def test_rows_spilled_to_disk_build_the_same_table(
    spilled: Path, tag_only: bool
) -> None:
    rows = _rows(tag_only)
    expected = StaticOutputOperatorPod._rows_to_table(rows)[0]  # pyright: ignore[reportPrivateUsage]  # the rule RowChunks must equal

    chunks = _chunks(rows)
    table = chunks.row_table()[0]

    assert _spool_files(spilled), "the chunks were not written to disk"
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert table.equals(expected)


def test_a_table_read_from_spilled_rows_outlives_what_wrote_it(spilled: Path) -> None:
    import gc

    rows = _rows(False)
    expected = StaticOutputOperatorPod._rows_to_table(rows)[0]  # pyright: ignore[reportPrivateUsage]  # the rule RowChunks must equal
    chunks = _chunks(rows)
    stream = chunks.output_stream()
    table = stream.as_table(columns={"system_tags": True, "source": True})
    wanted = expected.select(table.column_names)

    del chunks, stream
    _ = gc.collect()

    # The spool is gone and its files deleted; the table still reads every value.
    assert not _spool_files(spilled)
    assert table.equals(wanted)
    assert table.to_pylist() == wanted.to_pylist()


@pytest.mark.parametrize("chain", ["map", "group", "batch"])
def test_a_job_node_over_spilled_rows_reads_as_one_in_memory(
    spilled: Path, tmp_path: Path, chain: str
) -> None:
    import gc

    job = _job(tmp_path, "spilled", chain, False)
    _ = job.run(orchestrator=AsyncPipelineOrchestrator())
    node = job.nodes["out"]
    table = node.as_table(all_info=True)
    rows = [(t.as_dict(), d.as_dict()) for t, d in node.iter_data()]
    content_hash = node.content_hash()
    values = table.to_pylist()

    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(static_output_pod, "KEPT_ROWS_MEMORY_BYTES", 1 << 40)
        in_memory = _job(tmp_path, "in_memory", chain, False)
        _ = in_memory.run(orchestrator=AsyncPipelineOrchestrator())
    expected = in_memory.nodes["out"]

    assert _sorted_rows(table) == _sorted_rows(expected.as_table(all_info=True))
    assert sorted(rows, key=repr) == sorted(
        [(t.as_dict(), d.as_dict()) for t, d in expected.iter_data()], key=repr
    )
    assert content_hash == expected.content_hash()

    # A table a user holds stays valid after the node lets its run go and the job
    # is gone.
    node.clear_cache()
    del node, job
    _ = gc.collect()
    assert table.to_pylist() == values


def test_a_table_of_spilled_rows_survives_interpreter_shutdown(tmp_path: Path) -> None:
    """A table held at exit, when the spool directory is removed, raises nothing."""
    import subprocess
    import sys
    import textwrap

    script = textwrap.dedent(
        """
        import sys, tempfile
        tempfile.tempdir = sys.argv[1]
        import pyarrow as pa
        from orcapod.core.row_spool import RowSpool
        spool = RowSpool(memory_bytes=0)
        spool.append(pa.table({"a": list(range(1000))}))
        held = spool.table()
        import atexit
        atexit.register(lambda: print("sum", sum(held.column("a").to_pylist())))
        """
    )
    done = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "sum 499500"
    assert done.stderr == ""
    assert not _spool_files(tmp_path)


def _other_rows() -> list[tuple[Any, Any]]:
    """Rows whose data columns are not those of ``_rows``: no table holds both."""
    source = ArrowTableSource(
        pa.table({"subject": ["x", "y"], "trial": [100, 101], "other": [1.0, 2.0]}),
        tag_columns=["subject", "trial"],
        source_id="other",
    )
    return list(source.iter_data())


def test_rows_that_cannot_make_one_table_raise_when_it_is_read_not_when_added(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review of PR 562: receiving a row never raised before the table was built."""
    import pyarrow.lib

    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", 3)
    rows = _rows(False)[:4] + _other_rows() + _rows(False)[4:]

    chunks = _chunks(rows)

    assert chunks.num_rows == len(rows)
    with pytest.raises(pyarrow.lib.ArrowInvalid):
        chunks.row_table()
    with pytest.raises(pyarrow.lib.ArrowInvalid):
        StaticOutputOperatorPod._rows_to_table(rows)  # pyright: ignore[reportPrivateUsage]  # the rule RowChunks must equal


async def _closed(rows: list[tuple[Any, Any]]) -> Any:
    channel: Channel[tuple[Any, Any]] = Channel(buffer_size=len(rows) + 1)
    for row in rows:
        await channel.writer.send(row)
    await channel.writer.close()
    return channel.reader


def test_a_barrier_with_an_empty_input_never_reads_the_other_as_a_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review of PR 562: rows no table can hold, beside an empty input, give no rows
    and no error, and no reader task is left behind."""
    from orcapod.operators import MergeJoin

    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", 3)
    rows = _rows(False)[:4] + _other_rows() + _rows(False)[4:]
    pipeline_hash = _source(False).pipeline_hash()

    async def run() -> tuple[list[Any], int]:
        output: Channel[tuple[Any, Any]] = Channel(buffer_size=4)
        before = len(asyncio.all_tasks())
        await MergeJoin().async_execute(
            [await _closed(rows), await _closed([])],
            output.writer,
            input_pipeline_hashes=[pipeline_hash, pipeline_hash],
        )
        sent = await output.reader.collect()
        return sent, len(asyncio.all_tasks()) - before

    sent, left_behind = asyncio.run(run())

    assert sent == []
    assert left_behind == 0


def test_a_result_streams_own_iterator_is_never_bypassed() -> None:
    """Review of PR 562: a subclass's, or an instance's, ``iter_data`` yields the rows."""
    table = _source(False).as_table(columns={"system_tags": True, "source": True})

    class Odd(ArrowTableStream):
        def iter_data(self):  # pyright: ignore[reportIncompatibleMethodOverride]  # a narrower iterator on purpose
            for tag, data in super().iter_data():
                if cast("int", tag.as_dict()["trial"]) % 2:
                    yield tag, data

    keys: dict[str, Any] = {
        "tag_columns": ["subject", "trial"],
        "data_tags": ["subject", "trial"],
    }
    subclassed = Odd(table, **keys)
    assert [t.as_dict()["trial"] for t, _ in rows_once(subclassed)] == [1, 3, 5, 7, 9]

    shadowed = ArrowTableStream(table, **keys)
    first = next(iter(shadowed.iter_data()))
    shadowed.iter_data = lambda: iter([first])  # pyright: ignore[reportAttributeAccessIssue]  # an instance's own iterator
    assert [t.as_dict()["trial"] for t, _ in rows_once(shadowed)] == [0]
