"""Rows kept as Arrow tables build exactly what the row list built (ITL-1464).

The async path kept every row a node passed on, or a barrier operator gathered, as a
``(Tag, Data)`` pair. ``RowChunks`` keeps them as Arrow tables instead; these tests hold
it to the table and streams the row list gave, over several chunks.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.channels import Channel
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.operators import MapData, static_output_pod
from orcapod.core.operators.static_output_pod import (
    RowChunks,
    StaticOutputOperatorPod,
    collect_row_chunks,
    rows_once,
)
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource
from orcapod.streams import StreamView
from orcapod.utils import arrow_utils

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


def test_a_small_table_is_kept_as_it_is() -> None:
    table = _table()
    assert static_output_pod.kept_table(table) is table


def test_a_large_table_is_kept_on_disk_with_the_same_schema_and_values(
    spilled: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import gc

    monkeypatch.setattr(static_output_pod, "SPILL_SLICE_ROWS", 4)
    table = _source(False).as_table(all_info=True)

    kept = static_output_pod.kept_table(table)

    assert kept is not table
    # Its spool is gone with the call and the file deleted: the mapping is the table's.
    assert not _spool_files(spilled)
    assert kept.schema.equals(table.schema, check_metadata=True)
    assert kept.equals(table)
    _ = gc.collect()
    # The spool that wrote it is gone; the table still reads.
    assert kept.to_pylist() == table.to_pylist()


@pytest.mark.parametrize("tag_only", [False, True], ids=["data_tags", "tag_only"])
@pytest.mark.parametrize("chain", ["map", "group", "batch"])
def test_a_sync_job_node_over_spilled_rows_reads_exactly_as_one_in_memory(
    spilled: Path, tmp_path: Path, chain: str, tag_only: bool
) -> None:
    job = _job(tmp_path, "spilled_sync", chain, tag_only)
    _ = job.run()
    node = job.nodes["out"]
    table = node.as_table(all_info=True)

    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(static_output_pod, "KEPT_ROWS_MEMORY_BYTES", 1 << 40)
        in_memory = _job(tmp_path, "in_memory_sync", chain, tag_only)
        _ = in_memory.run()
    expected = in_memory.nodes["out"]

    # Sync: the same rows in the same order, the same schema, the same hashes.
    wanted = expected.as_table(all_info=True)
    assert table.schema.equals(wanted.schema, check_metadata=True)
    assert table.equals(wanted)
    assert node.content_hash() == expected.content_hash()
    assert [(t.as_dict(), d.as_dict()) for t, d in node.iter_data()] == [
        (t.as_dict(), d.as_dict()) for t, d in expected.iter_data()
    ]
    assert node.head(3).table.equals(expected.head(3).table)

    node.clear_cache()
    assert table.to_pylist() == wanted.to_pylist()


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("on_disk", [False, True], ids=["in_memory", "on_disk"])
def test_a_job_nodes_current_run_is_read_a_page_at_a_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, orchestrator: str, on_disk: bool
) -> None:
    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", 3)
    if on_disk:
        monkeypatch.setattr(static_output_pod, "KEPT_ROWS_MEMORY_BYTES", 0)
        monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    job = _job(tmp_path, "paged", "map", False)
    node = job.nodes["out"]

    # No current run: nothing to count or read.
    assert node.current_run_row_count() is None
    assert node.read_current_run(0, 5) is None

    if orchestrator == "sync":
        _ = job.run()
    else:
        _ = job.run(orchestrator=AsyncPipelineOrchestrator())

    assert node.current_run_row_count() == _ROWS
    configurations: list[dict[str, Any]] = [
        {},
        {"columns": {"system_tags": True, "source": True}},
        {"columns": {"sort_by_tags": True}},
        {"all_info": True},
    ]
    for given in configurations:
        whole = node.as_table(**given)
        pages = [node.read_current_run(start, 4, **given) for start in (0, 4, 8, 12)]
        assert [page.num_rows for page in pages] == [4, 4, 3, 0]
        assert pa.concat_tables(pages).equals(whole)
        assert pages[0].schema.equals(whole.schema, check_metadata=True)

    node.clear_cache()
    assert node.current_run_row_count() is None
    assert node.read_current_run(0, 5) is None


class _FailOnce:
    """A callable that raises ``error`` the first time ``when`` holds, then delegates."""

    def __init__(self, real: Any, error: Exception, when: Any) -> None:
        self.real, self.error, self.when, self.failures = real, error, when, 0

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if not self.failures and self.when(*args):
            self.failures += 1
            raise self.error
        return self.real(*args, **kwargs)


def _fail_one_allocation(monkeypatch: pytest.MonkeyPatch, step: str) -> _FailOnce:
    """Make one allocation of a chunk's build fail: of its tags, data or contexts."""
    error = MemoryError(f"synthetic allocation failure ({step})")
    if step == "context":
        real = pa.array
        fault = _FailOnce(
            real,
            error,
            lambda values, *_: (
                isinstance(values, list)
                and bool(values)
                and str(values[0]).startswith("std:")
            ),
        )

        class _Arrow:
            """``pyarrow`` as ``static_output_pod`` sees it, with the failing ``array``."""

            array = staticmethod(fault)

            def __getattr__(self, name: str) -> Any:
                return getattr(pa, name)

        monkeypatch.setattr(static_output_pod, "pa", _Arrow())
    else:
        column = "value" if step == "data" else "subject"
        fault = _FailOnce(
            arrow_utils.concat_tables_by_name,
            error,
            lambda tables: column in tables[0].column_names,
        )
        monkeypatch.setattr(arrow_utils, "concat_tables_by_name", fault)
    return fault


@pytest.mark.parametrize("step", ["tags", "data", "context"])
def test_a_chunk_that_failed_once_is_never_built_or_given_again(
    step: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review of PR 562, round 2: a retried chunk gave its rows twice."""
    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", 4)
    rows = _rows(True)
    fault = _fail_one_allocation(monkeypatch, step)
    chunks = RowChunks()
    for tag, data in rows[:3]:
        chunks.add(tag, data)

    with pytest.raises(MemoryError, match="synthetic"):
        chunks.add(*rows[3])

    for tag, data in rows[4:]:
        chunks.add(tag, data)
    for _ in range(2):
        with pytest.raises(MemoryError, match="synthetic"):
            chunks.row_table()
    assert fault.failures == 1


@pytest.mark.parametrize("step", ["tags", "data", "context"])
@pytest.mark.parametrize("size", [1, 2, 8192])
def test_a_job_never_succeeds_with_rows_twice_after_a_failed_allocation(
    step: str, size: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review of PR 562, round 2: the job succeeded with ``[3, 4, 3, 4]``."""
    # The node's row form, which keeps rows in ``RowChunks``.
    monkeypatch.setattr(OperatorJobNode, "_runs_over_batches", lambda *_: False)
    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", size)
    with PipelineJob(name="one_failure", database=InMemoryArrowDatabase()) as job:
        _ = ArrowTableSource(
            pa.table({"subject": [1, 2], "trial": [1, 2], "value": [3, 4]}),
            tag_columns=["subject", "trial"],
            tag_only_columns=True,
        ).map_data({"value": "value"}, label="out", drop_unmapped=True)
    fault = _fail_one_allocation(monkeypatch, step)

    with contextlib.suppress(MemoryError):
        _ = job.run(orchestrator=AsyncPipelineOrchestrator())

    assert fault.failures == 1
    assert job.nodes["out"].as_table().num_rows == 0
    # Nothing of the failed run is left: the next one gives every row once.
    _ = job.run(orchestrator=AsyncPipelineOrchestrator())
    values = job.nodes["out"].as_table()["value"].to_pylist()
    assert sorted(values) == [3, 4]


def test_rows_after_a_chunk_that_cannot_be_built_are_not_kept(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review of PR 562, round 2: after a failed seal the pairs accumulated."""
    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", 3)
    rows = _rows(False)[:2] + _other_rows() + _rows(False)

    chunks = _chunks(rows)

    assert chunks.num_rows == len(rows)
    assert chunks._pending == []  # pyright: ignore[reportPrivateUsage]  # the bound on rows held as objects
    with pytest.raises(pa.ArrowInvalid):
        _ = chunks.output_stream()


class _NoKeys:
    """A row's data whose keys cannot be looked up."""

    def __init__(self, data: Any) -> None:
        self._data = data

    def __getattr__(self, name: str) -> Any:
        return getattr(self._data, name)

    def keys(self, **_: Any) -> Any:
        raise RuntimeError("synthetic key lookup failure")


def test_a_row_is_not_read_when_it_is_added() -> None:
    """Review of PR 562, round 2: the first row's keys were read as it arrived."""
    tag, data = _rows(False)[0]
    chunks = _chunks([(tag, _NoKeys(data))])
    with pytest.raises(RuntimeError, match="synthetic key lookup"):
        _ = chunks.row_table()


def _barriers() -> list[Any]:
    """Every barrier that reads two inputs: ``BinaryOperator``'s and the default one."""
    from orcapod.operators import MergeJoin

    class Default(MergeJoin):
        async_execute = StaticOutputOperatorPod.async_execute

    return [pytest.param(MergeJoin, id="binary"), pytest.param(Default, id="default")]


@pytest.mark.parametrize("operator", _barriers())
@pytest.mark.parametrize("failing", [0, 1])
@pytest.mark.parametrize("failure", ["lookup", "memory", "channel", "cancel"])
def test_a_barrier_leaves_no_reader_behind(
    operator: Any, failing: int, failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review of PR 562, round 2: a sibling reader outlived the barrier's error."""
    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", 1)
    rows = _rows(False)[:2]
    pipeline_hash = _source(False).pipeline_hash()
    if failure == "lookup":
        rows = [(tag, _NoKeys(data)) for tag, data in rows]
    elif failure == "memory":
        _ = _fail_one_allocation(monkeypatch, "data")

    class Broken:
        def __aiter__(self) -> Any:
            return self

        async def __anext__(self) -> Any:
            raise OSError("synthetic channel failure")

    async def run() -> None:
        inputs: list[Any] = [None, None]
        inputs[failing] = Broken() if failure == "channel" else await _closed(rows)
        waiting: Channel[tuple[Any, Any]] = Channel(buffer_size=1)
        inputs[1 - failing] = waiting.reader
        output: Channel[tuple[Any, Any]] = Channel(buffer_size=4)
        before = asyncio.all_tasks()
        task = asyncio.create_task(
            operator().async_execute(
                inputs, output.writer, input_pipeline_hashes=[pipeline_hash] * 2
            )
        )
        for _ in range(20):
            await asyncio.sleep(0)
        if failure == "cancel":
            _ = task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        elif failure == "lookup":
            # The row is never read: the other input turns out empty.
            await waiting.writer.close()
            await task
        else:
            with pytest.raises((MemoryError, OSError), match="synthetic"):
                await task
        assert await output.reader.collect() == []
        assert asyncio.all_tasks() - before == set()

    asyncio.run(run())


class _FullDisk:
    """``pa.ipc.new_file`` on a disk that fills up after ``room`` files."""

    def __init__(self, room: int) -> None:
        self.room, self.calls, self.real = room, 0, pa.ipc.new_file

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        if self.calls > self.room:
            raise OSError(errno.ENOSPC, "synthetic full disk")
        return self.real(*args, **kwargs)


@pytest.mark.parametrize("room", [0, 1, 2])
def test_rows_that_cannot_be_written_are_all_let_go(
    spilled: Path, monkeypatch: pytest.MonkeyPatch, room: int
) -> None:
    """Review of PR 566: a failed spill raises at once and leaves no file."""
    disk = _FullDisk(room)
    monkeypatch.setattr(pa.ipc, "new_file", disk)
    rows = _rows(False)
    chunks = RowChunks()
    with pytest.raises(OSError, match="synthetic full disk"):
        for tag, data in rows:
            chunks.add(tag, data)

    # The disk is asked once more, and no more rows are kept.
    assert disk.calls == room + 1
    chunks.add(*rows[0])
    assert chunks._pending == []  # pyright: ignore[reportPrivateUsage]  # nothing is kept after a failure
    assert not _spool_files(spilled)
    with pytest.raises(OSError, match="synthetic full disk"):
        _ = chunks.row_table()


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("room", [0, 1])
def test_a_job_that_fills_the_disk_leaves_no_file_while_its_error_is_kept(
    spilled: Path,
    tmp_path_factory: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
    orchestrator: str,
    room: int,
) -> None:
    """Review of PR 566: the kept error pinned the files written before it."""
    monkeypatch.setattr(static_output_pod, "SPILL_SLICE_ROWS", 3)
    job = _job(tmp_path_factory.mktemp("store"), "full", "map", False)
    disk = _FullDisk(room)
    with monkeypatch.context() as patched:
        patched.setattr(pa.ipc, "new_file", disk)
        with pytest.raises(BaseException) as held:
            if orchestrator == "sync":
                _ = job.run()
            else:
                _ = job.run(orchestrator=AsyncPipelineOrchestrator())

    assert disk.calls > room
    assert not _spool_files(spilled), f"left behind with {held.value!r} kept"
    assert job.nodes["out"].as_table().num_rows == 0
    # With room again the same job gives every row once.
    _ = job.run()
    assert job.nodes["out"].as_table().num_rows == _ROWS


@pytest.mark.parametrize("failure", ["disk", "cancel", "compute"])
def test_a_barrier_that_fails_leaves_no_file_of_either_input(
    spilled: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    """Review of PR 566: a barrier's gathered inputs are spilled too."""
    from orcapod.operators import MergeJoin

    rows = _rows(False)
    pipeline_hash = _source(False).pipeline_hash()
    disk = _FullDisk(4)
    if failure == "disk":
        monkeypatch.setattr(pa.ipc, "new_file", disk)
    operator = MergeJoin()

    def refuse(*_: Any) -> Any:
        raise RuntimeError("synthetic computation failure")

    if failure == "compute":
        monkeypatch.setattr(operator, "static_process", refuse)

    async def run() -> BaseException:
        left: Channel[tuple[Any, Any]] = Channel(buffer_size=_ROWS + 1)
        for row in rows:
            await left.writer.send(row)
        if failure != "cancel":
            await left.writer.close()
        output: Channel[tuple[Any, Any]] = Channel(buffer_size=4)
        before = asyncio.all_tasks()
        task = asyncio.create_task(
            operator.async_execute(
                [left.reader, await _closed(rows)],
                output.writer,
                input_pipeline_hashes=[pipeline_hash] * 2,
            )
        )
        if failure == "cancel":
            while len(_spool_files(spilled)) < 6:
                await asyncio.sleep(0)
            _ = task.cancel()
        with pytest.raises((OSError, RuntimeError, asyncio.CancelledError)) as held:
            await task
        assert asyncio.all_tasks() - before == set()
        return held.value

    error = asyncio.run(run())

    assert not _spool_files(spilled), f"left behind with {error!r} kept"


def test_a_cancelled_node_leaves_no_file_while_its_cancellation_is_kept(
    spilled: Path,
) -> None:
    """Review of PR 566: the kept cancellation pinned three files."""
    node = OperatorJobNode(
        operator=MapData({"value": "amplitude"}), input_streams=[_source(False)]
    )

    async def run() -> BaseException:
        given: Channel[tuple[Any, Any]] = Channel(buffer_size=_ROWS)
        output: Channel[tuple[Any, Any]] = Channel(buffer_size=_ROWS)
        task = asyncio.create_task(node.async_execute([given.reader], output.writer))
        for row in _rows(False):
            await given.writer.send(row)
        for _ in range(_ROWS):
            _ = await output.reader.receive()
        assert _spool_files(spilled)
        _ = task.cancel()
        with pytest.raises(asyncio.CancelledError) as held:
            await task
        return held.value

    error = asyncio.run(run())

    assert not _spool_files(spilled), f"left behind with {error!r} kept"
    assert node.as_table().num_rows == 0


def test_a_finished_async_run_leaves_no_file_either(
    spilled: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    job = _job(tmp_path_factory.mktemp("store"), "done", "group", False)
    _ = job.run(orchestrator=AsyncPipelineOrchestrator())
    assert not _spool_files(spilled)
    assert job.nodes["out"].as_table().num_rows == 3


@pytest.mark.parametrize("orchestrator", ["sync", "async", "node"])
@pytest.mark.parametrize("on_disk", [False, True], ids=["in_memory", "on_disk"])
def test_a_job_nodes_run_view_is_its_current_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, orchestrator: str, on_disk: bool
) -> None:
    """The view pages through the rows the node keeps; nothing is refused, whatever the
    size, and it stays valid once the node lets its rows go (ITL-1464)."""
    monkeypatch.setattr(static_output_pod, "ROW_CHUNK_ROWS", 3)
    monkeypatch.setattr("orcapod.core.streams.view.RUN_VIEW_COMPLETE_ROWS", 1)
    monkeypatch.setattr("orcapod.core.streams.view.RUN_VIEW_WINDOW_ROWS", 0)
    if on_disk:
        monkeypatch.setattr(static_output_pod, "KEPT_ROWS_MEMORY_BYTES", 0)
        monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    job = _job(tmp_path, "viewed", "map", False)
    node = job.nodes["out"]
    if orchestrator == "sync":
        _ = job.run()
    elif orchestrator == "async":
        _ = job.run(orchestrator=AsyncPipelineOrchestrator())
    view = node.run()

    assert type(view) is StreamView
    assert view.total_rows == len(view) == node.current_run_row_count() == _ROWS
    whole = node.as_table()
    page = node.read_current_run(0, _ROWS)
    assert page is not None
    assert view.as_table().equals(whole)
    assert view.as_table().equals(page)
    assert view.head(4).table.equals(node.read_current_run(0, 4))
    assert view.tail(4).table.equals(node.read_current_run(_ROWS - 4, 4))
    assert view.sort_by_tags().table.equals(
        node.as_table(columns={"sort_by_tags": True})
    )
    assert node.head(3).header() == view.head(3).header()

    shown, html = str(view), view._repr_html_()
    expected = whole.to_pylist()
    node.clear_cache()
    assert view.as_table().to_pylist() == expected
    assert (str(view), view._repr_html_()) == (shown, html)
    # A cleared node pages no row; the view is a snapshot of the run it was given.
    assert node.read_current_run(0, 4) is None
