# tests/test_core/nodes/test_function_node_fetch_joined.py
"""Tests for FunctionJobNode._fetch_joined_records."""

from __future__ import annotations

import asyncio
from typing import cast

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.channels import Channel
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import (
    FunctionJobNode,
    _PIPELINE_ENTRY_ID_COL,
    _PIPELINE_BASE_ENTRY_ID_COL,
)
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.protocols.core_protocols import (
    DataProtocol,
    FunctionPodProtocol,
    TagProtocol,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import DictSource
from orcapod.system_constants import constants


def double_value(value: int) -> int:
    return value * 2


@pytest.fixture
def node_without_db():
    """FunctionJobNode with no databases attached."""
    table = pa.table(
        {
            "key": pa.array(["a"], type=pa.large_string()),
            "value": pa.array([1], type=pa.int64()),
        }
    )
    src = ArrowTableSource(
        table, tag_columns=["key"], tag_only_columns=True, infer_nullable=True
    )
    pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
    return FunctionJobNode(pod, src)


@pytest.fixture
def node_with_empty_db():
    """FunctionJobNode with databases attached but no data executed."""
    table = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        }
    )
    src = ArrowTableSource(
        table, tag_columns=["key"], tag_only_columns=True, infer_nullable=True
    )
    pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
    return FunctionJobNode(
        pod,
        src,
        pipeline_database=InMemoryArrowDatabase(),
        result_database=InMemoryArrowDatabase(),
    )


@pytest.fixture
def executed_node(node_with_empty_db):
    """FunctionJobNode with databases attached and both input rows executed."""
    node = node_with_empty_db
    for tag, data in node._input_stream.iter_data():
        node.execute_data(tag, data)
    return node


class TestFetchJoinedRecords:
    def test_returns_none_when_no_db(self, node_without_db):
        """Returns None when no databases are attached."""
        assert node_without_db._fetch_joined_records() is None

    def test_returns_none_when_db_fetch_returns_none(self, node_with_empty_db):
        """Returns None when the pipeline DB returns None (no records written yet)."""
        assert node_with_empty_db._fetch_joined_records() is None

    def test_returned_table_includes_pipeline_entry_id_column(self, executed_node):
        """The returned table always contains __pipeline_entry_id."""
        result = executed_node._fetch_joined_records()
        assert result is not None
        assert _PIPELINE_ENTRY_ID_COL in result.table.column_names

    def test_taginfo_columns_present_in_result(self, executed_node):
        """taginfo_columns contains the pre-join pipeline DB column names as strings.

        Specifically, _PIPELINE_ENTRY_ID_COL must be present because it is added
        by the pipeline DB fetch (record_id_column=_PIPELINE_ENTRY_ID_COL) and
        captured before the join — confirming the pre-filter snapshot is correct.
        """
        result = executed_node._fetch_joined_records()
        assert result is not None
        assert isinstance(result.taginfo_columns, tuple)
        assert all(isinstance(c, str) for c in result.taginfo_columns)
        assert _PIPELINE_ENTRY_ID_COL in result.taginfo_columns

    def test_no_base_entry_ids_returns_all_rows(self, executed_node):
        """Calling with base_entry_ids=None returns all executed rows."""
        result = executed_node._fetch_joined_records()
        assert result is not None
        assert result.table.num_rows == 2

    def test_entry_ids_filter_narrows_rows(self, executed_node):
        """Passing a single base_entry_id returns only that row."""
        node = executed_node
        input_pairs = list(node._input_stream.iter_data())
        base_entry_id_0 = node.compute_base_entry_id(
            input_pairs[0][0], input_pairs[0][1]
        )

        result = node._fetch_joined_records(base_entry_ids=[base_entry_id_0])
        assert result is not None
        assert result.table.num_rows == 1
        assert (
            result.table.column(_PIPELINE_BASE_ENTRY_ID_COL)[0].as_py()
            == base_entry_id_0
        )

    def test_empty_base_entry_ids_returns_zero_rows(self, executed_node):
        """Passing base_entry_ids=[] returns a 0-row table, not None."""
        result = executed_node._fetch_joined_records(base_entry_ids=[])
        assert result is not None
        assert result.table.num_rows == 0

    def test_targeted_reads_still_warn_about_an_unrequested_missing_result(
        self, caplog
    ):
        source = DictSource(
            [{"id": i, "value": i + 1} for i in range(3)],
            tag_columns=["id"],
            tag_only_columns=True,
        )
        pipeline_database = InMemoryArrowDatabase()
        results = InMemoryArrowDatabase()
        pod = function_pod(output_keys="result")(double_value).pod
        assert isinstance(pod, FunctionPodProtocol)
        node = FunctionJobNode(
            pod, source, pipeline_database=pipeline_database, result_database=results
        )
        rows = list(source.iter_data())
        for row in rows:
            node.execute_data(*row)
        entry_ids = [node.compute_base_entry_id(tag, data) for tag, data in rows[:2]]
        entries = pipeline_database.get_all_records(node._versioned_pipeline_path)
        cached_pod = node._cached_function_pod
        assert entries is not None and cached_pod is not None
        result_ids = [
            row[constants.DATA_RECORD_ID]
            for row in entries.to_pylist()
            if row[_PIPELINE_BASE_ENTRY_ID_COL] in entry_ids
        ]
        kept = results.get_records_by_ids(
            cached_pod.record_path, result_ids, record_id_column="rid"
        )
        assert kept is not None
        partial_results = InMemoryArrowDatabase()
        partial_results.add_records(
            cached_pod.record_path, kept, record_id_column="rid"
        )
        node.attach_databases(pipeline_database, result_database=partial_results)

        for requested in (entry_ids, list(reversed(entry_ids))):
            caplog.clear()
            found = node._fetch_joined_records(base_entry_ids=requested)
            assert found is not None
            assert found.table.num_rows == 2
            assert set(found.table[_PIPELINE_BASE_ENTRY_ID_COL].to_pylist()) == set(
                entry_ids
            )
            assert (
                "1 pipeline DB entries have no match in persistent result DB"
                in caplog.text
            )


@pytest.mark.parametrize("asynchronous", [False, True])
def test_delta_stored_and_sync_order_agree_with_the_original_read_path(
    tmp_path, asynchronous: bool
):
    source = DictSource(
        [{"id": i, "value": i + 1} for i in range(5)],
        tag_columns=["id"],
        tag_only_columns=True,
    )
    pipeline_db = InMemoryArrowDatabase()
    result_db = DeltaTableDatabase(str(tmp_path))
    pod = function_pod(output_keys="result")(double_value).pod
    assert isinstance(pod, FunctionPodProtocol)
    node = FunctionJobNode(
        pod, source, pipeline_database=pipeline_db, result_database=result_db
    )
    rows = list(source.iter_data())
    for row in rows[:2]:
        node.execute_data(*row)
    result_db.flush()
    assert node.get_all_records() is not None
    for row in rows[2:]:
        node.execute_data(*row)
        result_db.flush()
        assert node.get_all_records() is not None

    fresh = FunctionJobNode(
        pod,
        source,
        pipeline_database=pipeline_db,
        result_database=DeltaTableDatabase(str(tmp_path)),
    )
    expected = fresh.get_all_records()
    actual = node.get_all_records()
    assert actual is not None and expected is not None
    assert actual.to_pylist() == expected.to_pylist()

    if asynchronous:

        async def run_async():
            inputs: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=5)
            output: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=5)
            for row in rows:
                await inputs.writer.send(row)
            await inputs.writer.close()
            await node.async_execute(inputs.reader, output.writer)
            return await output.reader.collect()

        emitted = asyncio.run(run_async())
        assert len(emitted) == len(rows)
        assert {
            (tag.as_dict()["id"], data.as_dict()["result"]) for tag, data in emitted
        } == {(i, 2 * (i + 1)) for i in range(5)}
    else:
        sync_rows = node.execute(source)
        assert [tag.as_dict()["id"] for tag, _ in sync_rows.iter_data()] == list(
            range(5)
        )


def test_readback_uses_existing_reads_with_a_legacy_result_wrapper():
    class LegacyResultStore:
        def __init__(self, inner):
            self.inner = inner

        def at(self, *parts):
            return LegacyResultStore(self.inner.at(*parts))

        def __getattr__(self, name):
            if name == "get_records_by_column":
                raise AttributeError(name)
            return getattr(self.inner, name)

    source = DictSource(
        [{"id": 0, "value": 10}], tag_columns=["id"], tag_only_columns=True
    )
    store = LegacyResultStore(InMemoryArrowDatabase())
    assert not isinstance(store, ArrowDatabaseProtocol)
    pod = function_pod(output_keys="result")(double_value).pod
    assert isinstance(pod, FunctionPodProtocol)
    node = FunctionJobNode(
        pod,
        source,
        pipeline_database=InMemoryArrowDatabase(),
        result_database=cast("ArrowDatabaseProtocol", cast(object, store)),
    )
    node.execute_data(*next(source.iter_data()))
    result = node.get_all_records()
    assert result is not None
    assert result["result"].to_pylist() == [20]
