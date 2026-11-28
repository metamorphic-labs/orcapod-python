"""An operator node stores, and reads back, a stream with data-tags (ITL-1019, gap audit S2).

Developer decision, November 16, 2026: the stored table fully expresses what it stores. The
operator output table marks its data-tag columns with the same field-metadata marker
as the function pipeline table (``data_tags.TAG_KIND_METADATA_KEY``), written
from the operator's own output binding. A read checks the marker against the node and drops
it when the rows become a stream. Source info stays in the record hash (gap audit Q10); the
marker never enters it.
"""

from __future__ import annotations

import dataclasses
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.contexts import DataContext
from orcapod.core.nodes import OperatorJobNode
from orcapod.core.operators import Join
from orcapod.core.data_tags import (
    TAG_KIND_METADATA_KEY,
    DataTagMaterializationWarning,
    stored_data_tags,
    without_tag_kind_marks,
)
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import OPERATOR_DB_SCHEMA_VERSION, constants
from orcapod.types import CacheMode

JOB = "ops"


def _in_memory(_: Path) -> ArrowDatabaseProtocol:
    return InMemoryArrowDatabase()


def _delta(tmp_path: Path) -> ArrowDatabaseProtocol:
    return DeltaTableDatabase(tmp_path / "store")


STORES = pytest.mark.parametrize(
    "make_store", [_in_memory, _delta], ids=["memory", "delta"]
)
ORCHESTRATORS = pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
Orchestrator = type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator]


def _source(name: str, column: str, ids: list[int], tag_only: object = ("t",)):
    """``id`` a data-tag and ``t`` plain unless ``tag_only`` says otherwise."""
    table = pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "t": pa.array([10 * i for i in ids], pa.int64()),
            column: pa.array([float(i) for i in ids], pa.float64()),
        }
    )
    return op.sources.ArrowTableSource(
        table,
        tag_columns=["id", "t"],
        tag_only_columns=tag_only,  # pyright: ignore[reportArgumentType]  # the test varies it
        source_id=name,
    )


def _join_job(
    store: ArrowDatabaseProtocol,
    ids: list[int],
    mode: CacheMode = CacheMode.LOG,
    tag_only: object = ("t",),
) -> PipelineJob:
    """A job joining two sources on ``id`` and ``t``; its join node stores in ``mode``."""
    job = PipelineJob(name=JOB, database=store)
    with job:
        _ = Join()(
            _source("a", "x", ids, tag_only),
            _source("b", "z", ids, tag_only),
            label="joined",
        )
    job.nodes["joined"].attach_databases(
        pipeline_database=store.at(JOB), cache_mode=mode
    )
    return job


def _operator_table(store: ArrowDatabaseProtocol) -> pa.Table:
    """The one stored operator cache table (``odb_v3``)."""
    paths = [p for p in store.list_table_paths() if p[-1] == OPERATOR_DB_SCHEMA_VERSION]
    assert len(paths) == 1, paths
    table = store.get_all_records(paths[0])
    assert table is not None
    return table


def _marked(schema: pa.Schema) -> list[str]:
    """The columns carrying the marker key, whatever its value."""
    return [f.name for f in schema if TAG_KIND_METADATA_KEY in (f.metadata or {})]


def _rows(table: pa.Table) -> list[dict[str, object]]:
    return sorted(table.to_pylist(), key=lambda row: str(row["id"]))


# ---------------------------------------------------------------------------
# Writing: the table marks the operator's own output binding
# ---------------------------------------------------------------------------


@STORES
@ORCHESTRATORS
def test_the_operator_table_marks_only_its_data_tags(
    tmp_path: Path,
    make_store: Callable[[Path], ArrowDatabaseProtocol],
    orchestrator: Orchestrator,
) -> None:
    store = make_store(tmp_path)
    job = _join_job(store, [1, 2])
    job.run(orchestrator=orchestrator())
    node = job.nodes["joined"]
    assert node.keys() == (("id", "t"), ("id", "x", "z"))
    table = _operator_table(store)
    # Stored once, marked; the plain tag and the data columns carry no marker.
    assert table.column_names.count("id") == 1
    assert _marked(table.schema) == ["id"]
    assert stored_data_tags(table.schema) == ("id",)
    # The rows a stream holds carry no marker.
    assert _marked(node.as_table(all_info=True).schema) == []


@STORES
def test_a_plain_operator_table_carries_no_marker(
    tmp_path: Path, make_store: Callable[[Path], ArrowDatabaseProtocol]
) -> None:
    store = make_store(tmp_path)
    _join_job(store, [1, 2], tag_only=True).run()
    table = _operator_table(store)
    assert _marked(table.schema) == []
    # Nothing to strip: the table is exactly what it was before markers existed.
    assert without_tag_kind_marks(table).schema.equals(
        table.schema, check_metadata=True
    )


def test_a_data_tag_the_operator_materializes_is_not_marked() -> None:
    """The marker is the operator's output binding, never one that came from upstream."""
    store = InMemoryArrowDatabase()
    job = PipelineJob(name=JOB, database=store)
    with job, warnings.catch_warnings():
        warnings.simplefilter("ignore", DataTagMaterializationWarning)
        _ = _source("a", "x", [1, 2]).select_data_columns(["x"], label="selected")
    node = job.nodes["selected"]
    node.attach_databases(pipeline_database=store.at(JOB), cache_mode=CacheMode.LOG)
    job.run()
    assert node.keys() == (("id", "t"), ("x",))
    assert _marked(_operator_table(store).schema) == []


# ---------------------------------------------------------------------------
# Reading: REPLAY and the stored view check the binding and drop the marker
# ---------------------------------------------------------------------------


@STORES
@ORCHESTRATORS
def test_replay_reads_the_binding_back(
    tmp_path: Path,
    make_store: Callable[[Path], ArrowDatabaseProtocol],
    orchestrator: Orchestrator,
) -> None:
    store = make_store(tmp_path)
    logged = _join_job(store, [1, 2])
    logged.run(orchestrator=orchestrator())
    expected = _rows(logged.nodes["joined"].as_table())

    replayed = _join_job(store, [1, 2], mode=CacheMode.REPLAY)
    replayed.run(orchestrator=orchestrator())
    node = replayed.nodes["joined"]
    table = node.as_table(all_info=True)
    assert table.column_names.count("id") == 1
    assert _marked(table.schema) == []
    assert _rows(node.as_table()) == expected
    # Every row keeps the binding: ``id`` is a tag and a data column.
    for tag, data in node.iter_data():
        assert tag["id"] == data["id"]


def test_replayed_rows_carry_the_node_data_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The context column of replayed rows is the node's, not the default.

    One data context ships, so the node is given a copy of it under another key. The
    test is about ``_stream_over_stored``, which every replay read goes through.
    """
    job = _join_job(_in_memory(tmp_path), [1, 2], mode=CacheMode.REPLAY)
    node = cast(OperatorJobNode, job.nodes["joined"])
    other = dataclasses.replace(node.data_context, context_key="std:v1:test-other")

    def other_context(_: OperatorJobNode) -> DataContext:
        return other

    def other_key(_: OperatorJobNode) -> str:
        return other.context_key

    monkeypatch.setattr(OperatorJobNode, "data_context", property(other_context))
    monkeypatch.setattr(OperatorJobNode, "data_context_key", property(other_key))
    rows = pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "t": pa.array([10], pa.int64()),
            "x": pa.array([1.0], pa.float64()),
            "z": pa.array([1.0], pa.float64()),
        }
    )
    stream = node._stream_over_stored(rows)  # pyright: ignore[reportPrivateUsage]  # the test is about it
    table = stream.as_table(columns={"context": True})
    assert table.column(constants.CONTEXT_KEY).to_pylist() == ["std:v1:test-other"]
    assert [data.data_context_key for _, data in stream.iter_data()] == [
        "std:v1:test-other"
    ]


@STORES
def test_the_raw_records_keep_the_marker(
    tmp_path: Path, make_store: Callable[[Path], ArrowDatabaseProtocol]
) -> None:
    store = make_store(tmp_path)
    job = _join_job(store, [1, 2])
    job.run()
    records = job.nodes["joined"].get_all_records()
    assert records is not None
    assert stored_data_tags(records.schema) == ("id",)


@ORCHESTRATORS
def test_a_table_that_disagrees_with_the_operator_is_refused(
    orchestrator: Orchestrator,
) -> None:
    written = InMemoryArrowDatabase()
    _join_job(written, [1, 2]).run(orchestrator=orchestrator())
    # The same table with the marker removed: as if another pipeline had written it.
    tampered = InMemoryArrowDatabase()
    for path in written.list_table_paths():
        table = written.get_all_records(path, record_id_column="__copy_id")
        assert table is not None
        tampered.add_records(
            path,
            without_tag_kind_marks(table),
            record_id_column="__copy_id",
            flush=True,
        )
    job = _join_job(tampered, [1, 2], mode=CacheMode.REPLAY)
    with pytest.raises(
        InputValidationError,
        match=r"marks the data-tags \[\], but the node's operator declares \['id'\]",
    ):
        job.run(orchestrator=orchestrator())


def test_a_marker_with_another_value_is_refused() -> None:
    store = InMemoryArrowDatabase()
    _join_job(store, [1, 2]).run()
    tampered = InMemoryArrowDatabase()
    for path in store.list_table_paths():
        table = store.get_all_records(path, record_id_column="__copy_id")
        assert table is not None
        index = table.schema.get_field_index("id")
        field = table.schema.field(index).with_metadata(
            {TAG_KIND_METADATA_KEY: b"materialized"}
        )
        table = pa.Table.from_arrays(
            table.columns, schema=table.schema.set(index, field)
        )
        tampered.add_records(path, table, record_id_column="__copy_id", flush=True)
    node = _join_job(tampered, [1, 2]).nodes["joined"]
    with pytest.raises(InputValidationError, match="the only valid value"):
        _ = node.get_all_records()


# ---------------------------------------------------------------------------
# Appends, schema merges and runs that store nothing new
# ---------------------------------------------------------------------------


@STORES
def test_the_marker_survives_an_append_and_a_repeated_run(
    tmp_path: Path, make_store: Callable[[Path], ArrowDatabaseProtocol]
) -> None:
    store = make_store(tmp_path)
    _join_job(store, [1, 2]).run()
    assert _operator_table(store).num_rows == 2
    # The same rows again: the table is unchanged (per-row dedup).
    _join_job(store, [1, 2]).run()
    assert _operator_table(store).num_rows == 2
    assert stored_data_tags(_operator_table(store).schema) == ("id",)
    # New rows under the same node: appended, still marked.
    job = _join_job(store, [1, 2, 3])
    job.run()
    table = _operator_table(store)
    assert table.num_rows == 3
    assert stored_data_tags(table.schema) == ("id",)
    replayed = _join_job(store, [1, 2, 3], mode=CacheMode.REPLAY)
    replayed.run()
    assert sorted(replayed.nodes["joined"].as_table().column("id").to_pylist()) == [
        1,
        2,
        3,
    ]


def test_the_marker_survives_a_schema_merge(tmp_path: Path) -> None:
    store = DeltaTableDatabase(tmp_path / "store", allow_schema_evolution=True)
    _join_job(store, [1, 2]).run()
    path = next(
        p for p in store.list_table_paths() if p[-1] == OPERATOR_DB_SCHEMA_VERSION
    )
    stored = store.get_all_records(path, record_id_column="_record_hash")
    assert stored is not None
    # A row with one more column: Delta evolves the stored schema.
    extra = stored.slice(0, 1).append_column("extra", pa.array([1], pa.int64()))
    extra = extra.set_column(
        extra.schema.get_field_index("_record_hash"),
        "_record_hash",
        pa.array([b"another"], pa.large_binary()),
    )
    store.add_records(path, extra, record_id_column="_record_hash", flush=True)
    reopened = DeltaTableDatabase(tmp_path / "store")
    table = reopened.get_all_records(path)
    assert table is not None and "extra" in table.column_names
    assert stored_data_tags(table.schema) == ("id",)


# ---------------------------------------------------------------------------
# Zero rows
# ---------------------------------------------------------------------------


@STORES
@ORCHESTRATORS
def test_a_run_with_no_rows_keeps_the_binding(
    tmp_path: Path,
    make_store: Callable[[Path], ArrowDatabaseProtocol],
    orchestrator: Orchestrator,
) -> None:
    store = make_store(tmp_path)
    job = PipelineJob(name=JOB, database=store)
    with job:
        # Disjoint ids: the join has no rows.
        _ = Join()(_source("a", "x", [1]), _source("b", "z", [2]), label="joined")
    job.nodes["joined"].attach_databases(
        pipeline_database=store.at(JOB), cache_mode=CacheMode.LOG
    )
    job.run(orchestrator=orchestrator())
    node = job.nodes["joined"]
    table = node.as_table(all_info=True)
    assert table.num_rows == 0
    assert table.column_names.count("id") == 1
    assert node.keys() == (("id", "t"), ("id", "x", "z"))
    source = node.as_source()
    assert source.keys() == (("id", "t"), ("id", "x", "z"))
    assert source.as_table().num_rows == 0


# ---------------------------------------------------------------------------
# A database that cannot keep the marker is refused up front
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", [CacheMode.LOG, CacheMode.REPLAY])
def test_a_database_without_field_metadata_is_refused_before_any_write(
    mode: CacheMode,
) -> None:
    from orcapod.databases.connector_arrow_database import ConnectorArrowDatabase
    from orcapod.databases.sqlite_connector import SQLiteConnector

    database = ConnectorArrowDatabase(SQLiteConnector(":memory:"))
    job = PipelineJob(name=JOB, database=InMemoryArrowDatabase())
    with job:
        _ = Join()(_source("a", "x", [1]), _source("b", "z", [1]), label="joined")
    node = job.nodes["joined"]
    with pytest.raises(InputValidationError) as raised:
        node.attach_databases(pipeline_database=database, cache_mode=mode)
    message = str(raised.value)
    assert "SQLiteConnector does not preserve Arrow metadata" in message
    assert "keeps field metadata" in message and "tag_only_columns" in message
    assert database.list_table_paths() == []


def test_a_plain_operator_or_an_uncached_one_asks_nothing_of_the_database() -> None:
    from orcapod.databases.connector_arrow_database import ConnectorArrowDatabase
    from orcapod.databases.sqlite_connector import SQLiteConnector

    class KeepsTimestamps(SQLiteConnector):
        """SQLite's rules, but a timestamp is taken: an operator cache's insertion
        time is refused by SQLite itself when the database is attached (ITL-1471),
        and this test is about the data-tag marker, which SQLite does not keep."""

        def unstorable_column_reason(
            self, name: str, arrow_type: pa.DataType, *, primary_key: bool
        ) -> str | None:
            if pa.types.is_timestamp(arrow_type):
                return None
            return super().unstorable_column_reason(
                name, arrow_type, primary_key=primary_key
            )

    database = ConnectorArrowDatabase(KeepsTimestamps(":memory:"))
    plain = _join_job(InMemoryArrowDatabase(), [1], tag_only=True).nodes["joined"]
    plain.attach_databases(pipeline_database=database, cache_mode=CacheMode.LOG)
    with_data_tags = _join_job(InMemoryArrowDatabase(), [1]).nodes["joined"]
    # OFF writes nothing, so there is nothing to refuse.
    with_data_tags.attach_databases(
        pipeline_database=database, cache_mode=CacheMode.OFF
    )


@STORES
@pytest.mark.parametrize("mode", ["sync", "async", "run"])
@pytest.mark.parametrize("mixed_first", [False, True])
@pytest.mark.parametrize("nested", [False, True])
def test_operator_storage_keeps_one_declared_token_schema(
    make_store: Callable[[Path], ArrowDatabaseProtocol],
    tmp_path: Path,
    mode: str,
    mixed_first: bool,
    nested: bool,
) -> None:
    """A repeated row has the same stored key in both batches."""
    import asyncio

    from orcapod.channels import Channel
    from orcapod.operators import Filter
    from orcapod.streams import ArrowTableStream
    from orcapod.types import col

    op_filter = Filter(col("x") > 0)
    origin = op.sources.DictSource(
        [{"k": 0, "x": 1}], tag_columns=["k"], tag_only_columns=True, source_id="seed"
    )
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
            pa.field(
                "_source_x",
                pa.large_list(pa.large_list(pa.large_string()))
                if nested
                else pa.large_string(),
            ),
        ]
    )

    def stream(mixed: bool) -> ArrowTableStream:
        rows: list[dict[str, int | str | list[list[str]] | None]] = [
            {"k": 0, "x": 1, "_source_x": [["seed::0::x"]] if nested else "seed::0::x"}
        ]
        if mixed:
            rows.append({"k": 1, "x": 2, "_source_x": [] if nested else None})
        return ArrowTableStream(
            pa.Table.from_pylist(rows, schema=schema),
            tag_columns=["k"],
            producer=op_filter,
            upstreams=(origin,),
        )

    class Snapshots(ArrowTableStream):
        """Keep the producer identity while a source snapshot is replaced."""

        current: ArrowTableStream = stream(mixed_first)

        def as_table(self, **kwargs):
            return self.current.as_table(**kwargs)

        def iter_data(self):
            return self.current.iter_data()

    stream_value = Snapshots(
        schema.empty_table(), tag_columns=["k"], producer=op_filter, upstreams=(origin,)
    )
    store = make_store(tmp_path)
    node = OperatorJobNode(
        op_filter, [stream_value], pipeline_database=store, cache_mode=CacheMode.LOG
    )

    async def execute_async() -> None:
        incoming, outgoing = Channel(buffer_size=8), Channel(buffer_size=8)
        for row in stream_value.iter_data():
            await incoming.writer.send(row)
        await incoming.writer.close()
        await node.async_execute([incoming.reader], outgoing.writer)
        _ = await outgoing.reader.collect()

    for mixed in (mixed_first, not mixed_first):
        stream_value.current = stream(mixed)
        node.clear_cache()
        if mode == "run":
            node.run()
        elif mode == "sync":
            node.execute(stream_value)
        else:
            asyncio.run(execute_async())
        store.flush()

    assert len(store.list_table_paths()) == 1
    path = store.list_table_paths()[0]
    assert path[-1] == "odb_v3"
    stored = store.get_all_records(path, record_id_column="_record_hash")
    assert stored is not None
    assert stored.num_rows == 2
    assert stored.schema.field("_source_x") == schema.field("_source_x")
    preimage = stored.select([*schema.names, constants.NODE_CONTENT_HASH_COL])
    for i, saved in enumerate(stored["_record_hash"].to_pylist()):
        assert (
            node.data_context.arrow_hasher.hash_table(
                preimage.slice(i, 1)
            ).to_prefixed_digest()
            == saved
        )
    assert node.as_source().as_table().sort_by("k").to_pylist() == [
        {"k": 0, "x": 1},
        {"k": 1, "x": 2},
    ]
    node.attach_databases(pipeline_database=store, cache_mode=CacheMode.REPLAY)
    replayed = node.execute(stream_value).as_table(columns={"source": True})
    assert replayed.schema.field("_source_x") == schema.field("_source_x")
