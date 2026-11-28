"""Function pipeline records follow the durability of the results they reference."""

from __future__ import annotations

import asyncio
from pathlib import Path
import uuid
from collections import Counter
from collections.abc import Callable
from typing import cast

import deltalake
import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import InputValidationError, OrcapodDatabaseWriteError
from orcapod.invocation import InvocationContext
from orcapod.sources import DictSource
from orcapod.system_constants import (
    PIPELINE_DB_SCHEMA_VERSION,
    RESULT_DB_SCHEMA_VERSION,
    constants,
)

JOB = "ordered"
PIPELINE = PIPELINE_DB_SCHEMA_VERSION
RESULT = RESULT_DB_SCHEMA_VERSION
# The name the pipeline table's row key is read under in these tests.
ENTRY = "__row_key"
BASE_ENTRY = "__pipeline_instance_id"
CALLS: list[int] = []
DELIVERIES: list[int] = []


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Count calls and double the input."""
    CALLS.append(x)
    return 2 * x


@function_pod(output_keys="z")
def triple(x: int) -> int:
    """Triple the input."""
    return 3 * x


@function_pod(output_keys="y", ctx_arg="ctx")
def deliver(x: int, ctx: InvocationContext) -> int:
    """Record a delivery."""
    DELIVERIES.append(x)
    return x


def source(start: int, *, reversed_tags: bool = False) -> DictSource:
    """Give each branch four rows and its own source identity."""
    rows = [
        {"tag": i, "group": "a", "x": i, "other": i + 10}
        for i in range(start, start + 4)
    ]
    if reversed_tags:
        rows = [dict(reversed(tuple(row.items()))) for row in rows]
    return DictSource(
        data=rows,
        tag_columns=["group", "tag"] if reversed_tags else ["tag", "group"],
        tag_only_columns=True,
        label=f"source_{start}",
        source_id=f"source_{start}",
    )


def node_of(job: PipelineJob, name: str) -> FunctionJobNode:
    """Get a public function node with its narrowed type."""
    node = job.nodes[name]
    assert isinstance(node, FunctionJobNode)
    return node


def run(job: PipelineJob, asynchronous: bool, every: int) -> None:
    """Run via the chosen public entry point."""
    if asynchronous:
        asyncio.run(job.run_async(commit_every_rows=every))
    else:
        job.run(commit_every_rows=every)


def read_tables(root: Path, version: str) -> dict[tuple[str, ...], pa.Table]:
    """Read only durable tables through a newly opened handle."""
    reader = DeltaTableDatabase(root)
    tables: dict[tuple[str, ...], pa.Table] = {}
    for path in reader.list_table_paths():
        if path[-1] == version:
            table = reader.get_all_records(path, record_id_column="rid")
            assert table is not None
            tables[path] = table
    return tables


def inject_result_failure(
    monkeypatch: pytest.MonkeyPatch, failure: type[Exception]
) -> list[Path]:
    """Fail only result table writes, leaving pipeline table writes available."""
    original = cast(Callable[..., None], deltalake.write_deltalake)
    attempted: list[Path] = []

    def write(
        table_or_uri: str | Path | deltalake.DeltaTable,
        data: pa.Table,
        **kwargs: object,
    ) -> None:
        uri = (
            table_or_uri.table_uri
            if isinstance(table_or_uri, deltalake.DeltaTable)
            else table_or_uri
        )
        path = Path(uri)
        if path.name == RESULT:
            attempted.append(path)
            raise failure("injected result table write failure")
        original(table_or_uri, data, **kwargs)

    monkeypatch.setattr(deltalake, "write_deltalake", write)
    return attempted


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("every", [1, 10])
@pytest.mark.parametrize(
    "separate", [False, True], ids=["shared-store", "split-stores"]
)
@pytest.mark.parametrize(
    "failure", [OSError, InputValidationError], ids=["retryable", "dropped"]
)
def test_result_failure_withdraws_records_and_rerun_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    asynchronous: bool,
    every: int,
    separate: bool,
    failure: type[Exception],
) -> None:
    """A failed or dropped result leaves no queued or durable references."""
    CALLS.clear()
    pipeline_root = tmp_path / "pipeline"
    result_root = tmp_path / "results" if separate else pipeline_root
    pipeline_db = DeltaTableDatabase(pipeline_root)
    result_db = (
        DeltaTableDatabase(result_root).at("external")
        if separate
        else pipeline_db.at(JOB, "_result")
    )
    job = PipelineJob(name=JOB, database=pipeline_db)
    with job:
        double.pod(source(0), label="out")
    node = node_of(job, "out")
    if separate:
        node.attach_databases(
            pipeline_database=pipeline_db.at(JOB), result_database=result_db
        )
    pipeline_path = (JOB, *node.node_identity_path, PIPELINE)
    with monkeypatch.context() as fault:
        attempts = inject_result_failure(fault, failure)
        with pytest.raises(OrcapodDatabaseWriteError) as raised:
            run(job, asynchronous, every)
        assert attempts and not read_tables(result_root, RESULT)
        assert not read_tables(pipeline_root, PIPELINE)
        assert pipeline_db.get_all_records(pipeline_path) is None
        assert node.as_table().num_rows == 0
        assert str(attempts[0]) in str(raised.value)
        assert isinstance(raised.value.__cause__, failure)

    # An unrelated flush after the injection ends cannot publish any of the failed records.
    pipeline_db.flush()
    assert not read_tables(pipeline_root, PIPELINE)
    if failure is InputValidationError:
        assert not read_tables(result_root, RESULT)
        CALLS.clear()
    run(job, asynchronous, every)
    assert Counter(node.as_source().as_table()["y"].to_pylist()) == Counter(
        [0, 2, 4, 6]
    )
    assert Counter(node.as_table()["y"].to_pylist()) == Counter([0, 2, 4, 6])
    if failure is InputValidationError and every > 1:
        assert sorted(CALLS) == [0, 1, 2, 3]


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
def test_failed_side_effect_result_leaves_no_completion_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, asynchronous: bool
) -> None:
    """Delivery can precede a failed write, but its completion cannot persist alone."""
    DELIVERIES.clear()
    root = tmp_path / "store"
    db = DeltaTableDatabase(root)
    job = PipelineJob(name=JOB, database=db)
    with job:
        deliver.pod(source(0), label="out")
    with monkeypatch.context() as fault:
        inject_result_failure(fault, InputValidationError)
        with pytest.raises(OrcapodDatabaseWriteError):
            run(job, asynchronous, 10)
    assert sorted(DELIVERIES) == [0, 1, 2, 3]
    db.flush()
    assert not read_tables(root, PIPELINE)
    assert not read_tables(root, RESULT)
    assert node_of(job, "out").as_source().as_table().num_rows == 0


@pytest.mark.parametrize(
    "separate", [False, True], ids=["shared-store", "split-stores"]
)
@pytest.mark.parametrize(
    "shared_table", [False, True], ids=["distinct-tables", "shared-table"]
)
def test_async_flush_exposes_only_records_with_durable_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, separate: bool, shared_table: bool
) -> None:
    """Read each table from a fresh handle immediately after each effective flush."""
    pipeline_root = tmp_path / "pipeline"
    pipeline_db = DeltaTableDatabase(pipeline_root)
    result_roots = (
        [tmp_path / "left", tmp_path / "right"] if separate else [pipeline_root]
    )
    job = PipelineJob(name=JOB, database=pipeline_db)
    with job:
        double.pod(source(0), label="left")
        second = double if shared_table else triple
        second.pod(source(4, reversed_tags=True), label="right")
    left, right = (node_of(job, name) for name in ("left", "right"))
    assert (left.node_identity_path == right.node_identity_path) == shared_table
    if separate:
        for node, root in zip((left, right), result_roots, strict=True):
            node.attach_databases(
                pipeline_database=pipeline_db.at(JOB),
                result_database=DeltaTableDatabase(root),
            )

    publication_counts: list[int] = []
    dangling: list[set[bytes]] = []
    original_flush = DeltaTableDatabase.flush
    original_batch = DeltaTableDatabase.flush_batch

    def observe() -> None:
        committed = {
            rid
            for root in result_roots
            for table in read_tables(root, RESULT).values()
            for rid in cast(list[bytes], table["rid"].to_pylist())
        }
        referenced = {
            rid
            for table in read_tables(pipeline_root, PIPELINE).values()
            for rid in cast(list[bytes], table[constants.DATA_RECORD_ID].to_pylist())
        }
        missing = referenced - committed
        if missing:
            dangling.append(missing)
        if referenced and (
            not publication_counts or len(referenced) != publication_counts[-1]
        ):
            publication_counts.append(len(referenced))

    def flush(db: DeltaTableDatabase) -> None:
        original_flush(db)
        observe()

    def flush_batch(db: DeltaTableDatabase, path: tuple[str, ...]) -> None:
        original_batch(db, path)
        observe()

    monkeypatch.setattr(DeltaTableDatabase, "flush", flush)
    monkeypatch.setattr(DeltaTableDatabase, "flush_batch", flush_batch)
    run(job, asynchronous=True, every=10)
    assert not dangling
    assert publication_counts == ([8] if shared_table and not separate else [4, 8])
    for node in (left, right):
        assert node.as_table().num_rows == node.as_source().as_table().num_rows == 4
    if shared_table and not separate:
        (log,) = pipeline_root.rglob(f"{PIPELINE}/_delta_log")
        assert [
            item["operation"]
            for item in deltalake.DeltaTable(str(log.parent)).history()
        ] == ["WRITE"]


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    "reader",
    ["point", "scan", "filtered", "recorded_again", "cache", "source", "joined"],
)
def test_pending_records_remain_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, asynchronous: bool, reader: str
) -> None:
    """Read queued pipeline records from the live handle before result commit."""
    db = DeltaTableDatabase(tmp_path)
    src = source(0)

    def build() -> PipelineJob:
        job = PipelineJob(name=JOB, database=db)
        with job:
            double.pod(src, label="out")
        return job

    job, reader_job = build(), build()
    writer = node_of(job, "out")
    reader_node = node_of(reader_job, "out")
    path = (JOB, *writer.node_identity_path, PIPELINE)
    original = DeltaTableDatabase.flush_batch
    observed = False

    def flush_batch(view: DeltaTableDatabase, record_path: tuple[str, ...]) -> None:
        nonlocal observed
        if record_path[-1] == RESULT:
            assert not read_tables(tmp_path, PIPELINE)
            assert not read_tables(tmp_path, RESULT)
            queued = db.get_all_records(path, record_id_column=ENTRY)
            assert queued is not None and queued.num_rows == 4
            entry_id: bytes = queued[ENTRY][0].as_py()
            base_entry: bytes = queued[BASE_ENTRY][0].as_py()
            if reader == "point":
                pending = db.get_record_by_id(path, entry_id, record_id_column=ENTRY)
                assert pending is not None and pending[ENTRY].to_pylist() == [entry_id]
            elif reader == "scan":
                assert Counter(queued["tag"].to_pylist()) == Counter([0, 1, 2, 3])
            elif reader == "filtered":
                pending = db.get_records_with_column_value(
                    path, {BASE_ENTRY: base_entry}, record_id_column=ENTRY
                )
                assert pending is not None and pending[ENTRY].to_pylist() == [entry_id]
            elif reader == "cache":
                assert base_entry in reader_node.get_cached_results([base_entry])
            elif reader == "source":
                assert Counter(
                    reader_node.as_source().as_table()["y"].to_pylist()
                ) == Counter([0, 2, 4, 6])
            elif reader == "joined":
                joined = reader_node.get_all_records()
                assert joined is not None and joined.num_rows == 4
            else:
                (upstream,) = writer.upstreams
                tag, data = next(
                    (tag, data)
                    for tag, data in upstream.iter_data()
                    if writer.compute_instance_id(tag, data) == base_entry
                )
                # The same instance and result recorded again is the pending record
                # itself (ITL-1471: the row key is the record id): the write reads
                # the queued record and is skipped.
                result_id = uuid.UUID(bytes=queued[constants.DATA_RECORD_ID][0].as_py())
                writer.add_pipeline_record(tag, data, result_id, computed=False)
                found = db.get_records_with_column_value(
                    path, {BASE_ENTRY: base_entry}, record_id_column=ENTRY
                )
                assert found is not None
                assert found[ENTRY].to_pylist() == [entry_id]
                assert found["__computed"].to_pylist() == [True]
            observed = True
        original(view, record_path)

    monkeypatch.setattr(DeltaTableDatabase, "flush_batch", flush_batch)
    run(job, asynchronous, 10)
    assert observed
