"""Tests for FunctionJobNode ephemeral result database — ITL-507."""

from __future__ import annotations

import asyncio
import uuid

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import NodeConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def double(x: int) -> int:
    return x * 2


def _make_pod():
    pf = PythonDataFunction(double, output_keys="result")
    return FunctionPod(pf)


def _make_stream(
    rows: list[dict], tag_columns: list[str] | None = None
) -> ArrowTableStream:
    if tag_columns is None:
        tag_columns = ["id"]
    keys = list(rows[0].keys())
    schema = pa.schema([pa.field(k, pa.int64(), nullable=False) for k in keys])
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in keys},
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=tag_columns)


def _make_source_stream(
    rows: list[dict], tag_columns: list[str] | None = None
) -> ArrowTableSource:
    if tag_columns is None:
        tag_columns = ["id"]
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in rows[0]}
    )
    source = ArrowTableSource(
        table,
        tag_columns=tag_columns,
        source_id="test_src",
        infer_nullable=True,
        tag_only_columns=True,
    )
    return source


def _make_node(
    stream, pipeline_db=None, result_db=None, is_result_ephemeral: bool = False
):
    """Create a FunctionJobNode with given DB configuration."""
    pod = _make_pod()
    if pipeline_db is None:
        pipeline_db = InMemoryArrowDatabase()
    node = FunctionJobNode(
        function_pod=pod,
        input_stream=stream,
        pipeline_database=pipeline_db,
        result_database=result_db if result_db is not None else pipeline_db,
    )
    if is_result_ephemeral:
        node.node_config = NodeConfig(is_result_ephemeral=True)
    return node, pipeline_db


# ---------------------------------------------------------------------------
# Task 4 test: no-op set_ephemeral_database on blueprint node classes
# ---------------------------------------------------------------------------


class TestNoOpSetEphemeralStore:
    def test_function_node_has_set_ephemeral_database(self):
        """FunctionNode (blueprint) must have set_ephemeral_database as a no-op."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(function_pod=pod, input_stream=stream)
        store = InMemoryArrowDatabase()
        # Must not raise
        node.set_ephemeral_database(store)
        node.set_ephemeral_database(None)

    def test_operator_node_has_set_ephemeral_database(self):
        """OperatorNode (blueprint) must have set_ephemeral_database as a no-op."""
        from orcapod.core.nodes import OperatorNode
        from orcapod.core.operators import Join

        stream_a = _make_stream([{"id": 0, "x": 10}])
        stream_b = _make_stream([{"id": 0, "y": 20}])
        op = Join()
        node = OperatorNode(operator=op, input_streams=(stream_a, stream_b))
        store = InMemoryArrowDatabase()
        # Must not raise
        node.set_ephemeral_database(store)
        node.set_ephemeral_database(None)


# ---------------------------------------------------------------------------
# Task 5 test: FunctionJobNode set_ephemeral_database real override
# ---------------------------------------------------------------------------


class TestSetEphemeralStore:
    def test_set_ephemeral_database_assigns_store(self):
        """set_ephemeral_database(store) assigns the ephemeral_result_database attribute."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        store = InMemoryArrowDatabase()
        node.set_ephemeral_database(store)
        assert node.ephemeral_result_database is store

    def test_set_ephemeral_database_none_detaches(self):
        """set_ephemeral_database(None) removes the ephemeral database."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        store = InMemoryArrowDatabase()
        node.set_ephemeral_database(store)
        node.set_ephemeral_database(None)
        assert node.ephemeral_result_database is None


# ---------------------------------------------------------------------------
# Task 6 test: IS_EPHEMERAL_COL written to pipeline DB
# ---------------------------------------------------------------------------


class TestAddPipelineRecord:
    def test_is_ephemeral_false_written_to_pipeline_db(self):
        """add_pipeline_record(is_ephemeral=False) stores IS_EPHEMERAL_COL = False."""
        from orcapod.system_constants import constants

        stream = _make_stream([{"id": 0, "x": 10}])
        node, db = _make_node(stream)
        results = list(node.execute(stream).iter_data())
        assert len(results) == 1

        all_records = db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert constants.IS_EPHEMERAL_COL in all_records.column_names
        vals = all_records.column(constants.IS_EPHEMERAL_COL).to_pylist()
        assert all(v is False for v in vals)

    def test_is_ephemeral_true_written_to_pipeline_db(self):
        """When is_result_ephemeral=True, IS_EPHEMERAL_COL=True is stored in the tag table."""
        from orcapod.system_constants import constants

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db, is_result_ephemeral=True)
        node.set_ephemeral_database(ephemeral_store)

        results = list(node.execute(stream).iter_data())
        assert len(results) == 1

        all_records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert constants.IS_EPHEMERAL_COL in all_records.column_names
        vals = all_records.column(constants.IS_EPHEMERAL_COL).to_pylist()
        assert all(v is True for v in vals)


# ---------------------------------------------------------------------------
# Task 7 tests: two-store join
# ---------------------------------------------------------------------------


class TestBulkResolution:
    def test_ephemeral_false_unchanged(self):
        """is_result_ephemeral=False: execute() behaves identically to current implementation."""
        stream = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        node, _ = _make_node(stream)
        results = list(node.execute(stream).iter_data())
        assert len(results) == 2
        vals = {tag.as_dict()["id"]: data.as_dict()["result"] for tag, data in results}
        assert vals == {0: 20, 1: 40}

    def test_ephemeral_result_written_to_memory_not_persistent_db(self):
        """With is_result_ephemeral=True, persistent DB has no result rows; ephemeral database does."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        pod = _make_pod()
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_database(ephemeral_store)
        results = list(node.execute(stream).iter_data())

        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20

        # Persistent result DB must be empty (no writes there)
        eph_cache = node._ephemeral_cached_pod
        assert eph_cache is not None
        assert (
            eph_cache.result_database.get_all_records(eph_cache.record_path) is not None
        )
        assert result_db.get_all_records(node._cached_function_pod.record_path) is None

    def test_within_session_ephemeral_hit(self):
        """Same node called twice: second call hits ephemeral database — no recomputation."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_database(ephemeral_store)

        node.execute(stream)
        assert call_count["n"] == 1

        # Second execution — same entry_id — must hit cache
        node._cached_output_datas.clear()  # clear in-memory cache to force DB lookup
        node.execute(stream)
        assert call_count["n"] == 1  # function must NOT have been called again

    def test_cross_session_miss_recomputes(self):
        """Fresh InMemoryArrowDatabase (new session): ephemeral miss triggers recomputation."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()

        # Session 1: execute with ephemeral database
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_database(InMemoryArrowDatabase())
        node.execute(stream)
        assert call_count["n"] == 1

        # Session 2: fresh in-memory node with a fresh ephemeral database
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2)
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True)
        node2.set_ephemeral_database(InMemoryArrowDatabase())  # fresh database
        node2.execute(stream)
        assert call_count["n"] == 2  # recomputed

    def test_persistent_hit_served_when_ephemeral_true(self):
        """A persistent result is still served from cache when ephemeral database is also set."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)  # is_result_ephemeral=False (default)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        # Attach an ephemeral database — must NOT break persistent reads
        node.set_ephemeral_database(InMemoryArrowDatabase())

        # Run 1: writes to persistent DB (is_result_ephemeral=False)
        results = list(node.execute(stream).iter_data())
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 1

        # Clear in-memory cache to force DB lookup on Run 2
        node._cached_output_datas.clear()

        # Run 2: Phase 1 must find persistent result — no recompute
        results2 = list(node.execute(stream).iter_data())
        assert len(results2) == 1
        assert results2[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 1  # NOT recomputed

    def test_bulk_resolution_mixed_stores(self):
        """Tag table has both persistent and ephemeral entries; both resolve correctly."""
        stream_a = _make_stream([{"id": 0, "x": 10}])
        stream_b = _make_stream([{"id": 1, "x": 20}])
        pipeline_db = InMemoryArrowDatabase()

        # id=0 → persistent
        node_p, _ = _make_node(
            stream_a, pipeline_db=pipeline_db, is_result_ephemeral=False
        )
        node_p.execute(stream_a)

        # id=1 → ephemeral, reusing same pipeline_db
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        ephemeral_store = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node_e = FunctionJobNode(
            function_pod=pod,
            input_stream=stream_b,
            pipeline_database=pipeline_db,
        )
        node_e.node_config = NodeConfig(is_result_ephemeral=True)
        node_e.set_ephemeral_database(ephemeral_store)
        node_e.execute(stream_b)

        # Verify id=1 result is in ephemeral_store (not persistent)
        eph_cache = node_e._ephemeral_cached_pod
        assert eph_cache is not None
        assert (
            eph_cache.result_database.get_all_records(eph_cache.record_path) is not None
        )

        # Now load both results via a combined stream
        stream_both = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        node_both = FunctionJobNode(
            function_pod=pod,
            input_stream=stream_both,
            pipeline_database=pipeline_db,
        )
        node_both.node_config = NodeConfig(is_result_ephemeral=True)
        node_both.set_ephemeral_database(ephemeral_store)
        node_both._cached_output_datas.clear()

        results = list(node_both.execute(stream_both).iter_data())
        assert len(results) == 2
        vals = {tag.as_dict()["id"]: data.as_dict()["result"] for tag, data in results}
        assert vals == {0: 20, 1: 40}

    @pytest.mark.parametrize("later", ["persistent", "ephemeral"])
    def test_of_two_records_of_one_instance_the_one_recorded_last_wins(self, later):
        """A persistent and an ephemeral record of one instance: the later one is read.

        Among the records of one instance, the one recorded last
        (``__recorded_at``) is the current one, whichever database holds its
        result. The two results have the same value and different ids, so the
        id of the result read back says which record won. Nothing is computed
        again.

        The node's writer records one instance once per result, so the second
        record is written through the database: a valid record of the same
        instance (a new result id, the record id made from the two, the row key
        made from the node and the record id), one minute before or after the
        first.
        """
        import datetime

        import pyarrow as pa

        from orcapod.contexts import get_default_context
        from orcapod.core import row_ids
        from orcapod.system_constants import constants
        from orcapod.types import ContentHash

        ROW_KEY = "__row_key"  # the name the row key is read under here
        hasher = get_default_context().arrow_hasher

        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()

        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)  # is_result_ephemeral=False (default)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.set_ephemeral_database(ephemeral_store)

        # The persistent record, written by the node.
        ((_, persistent_output),) = list(node.execute(stream).iter_data())
        assert call_count["n"] == 1
        pipeline_db.flush()
        existing = pipeline_db.get_all_records(
            node._versioned_pipeline_path, record_id_column=ROW_KEY
        )
        assert existing is not None and existing.num_rows == 1
        persistent_result_id = uuid.UUID(
            bytes=existing[constants.DATA_RECORD_ID][0].as_py()
        )

        # A result of the same input in the ephemeral store, with its own id.
        tag_obj, data_obj = next(iter(stream.iter_data()))
        _, eph_output = node._ephemeral_cached_pod.process_data(tag_obj, data_obj)
        assert eph_output is not None
        ephemeral_result_id = eph_output.datagram_uuid
        assert ephemeral_result_id != persistent_result_id
        count_after_setup = call_count["n"]

        # The ephemeral record of the same instance.
        instance = ContentHash.from_prefixed_digest(
            existing["__pipeline_instance_id"][0].as_py()
        )
        record = row_ids.record_id(
            hasher, node.content_hash(), instance, ephemeral_result_id
        )
        recorded = existing["__recorded_at"][0].as_py()
        shift = datetime.timedelta(minutes=1 if later == "ephemeral" else -1)
        ephemeral_row = existing.drop([ROW_KEY])
        for column, value in {
            constants.IS_EPHEMERAL_COL: True,
            constants.DATA_RECORD_ID: ephemeral_result_id.bytes,
            "__pipeline_record_id": record.to_prefixed_digest(),
            "__recorded_at": recorded + shift,
        }.items():
            index = ephemeral_row.schema.get_field_index(column)
            field = ephemeral_row.schema.field(index)
            ephemeral_row = ephemeral_row.set_column(
                index, field, pa.array([value], type=field.type)
            )
        pipeline_db.add_record(
            node._versioned_pipeline_path,
            record.to_prefixed_digest(),
            ephemeral_row,
        )
        pipeline_db.flush()
        both = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert both is not None and both.num_rows == 2
        assert len(set(both["__pipeline_instance_id"].to_pylist())) == 1
        assert len(set(both["__pipeline_record_id"].to_pylist())) == 2

        # A new node over the same stores reads the record recorded last.
        reader = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        reader.set_ephemeral_database(ephemeral_store)
        ((_, read),) = list(reader.execute(stream).iter_data())
        assert read.as_dict()["result"] == 20
        expected = ephemeral_result_id if later == "ephemeral" else persistent_result_id
        assert read.datagram_uuid == expected
        assert call_count["n"] == count_after_setup


# ---------------------------------------------------------------------------
# Task 8 tests: ephemeral write path
# ---------------------------------------------------------------------------


class TestEphemeralWritePath:
    def test_store_not_assigned_raises(self):
        """is_result_ephemeral=True but ephemeral_result_database=None raises RuntimeError."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pod = _make_pod()
        pipeline_db = InMemoryArrowDatabase()
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        # set_ephemeral_database never called → ephemeral_result_database is None
        with pytest.raises(RuntimeError, match="is_result_ephemeral=True"):
            node.execute(stream, error_policy="fail_fast")

    def test_ephemeral_only_node(self):
        """result_database=None, is_result_ephemeral=True: end-to-end works."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        pod = _make_pod()
        # Pass pipeline_database but no result_database — ephemeral only
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_database(ephemeral_store)
        results = list(node.execute(stream).iter_data())
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20

    def test_recompute_after_ephemeral_miss_no_infinite_cycle(self):
        """Cross-session ephemeral miss → recomputed → served on next call (no cycle)."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()

        # Session 1
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node1 = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node1.node_config = NodeConfig(is_result_ephemeral=True)
        node1.set_ephemeral_database(InMemoryArrowDatabase())
        node1.execute(stream)
        assert call_count["n"] == 1

        # Session 2: fresh database → miss → recompute
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2)
        ephemeral2 = InMemoryArrowDatabase()
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True)
        node2.set_ephemeral_database(ephemeral2)
        node2.execute(stream)
        assert call_count["n"] == 2

        # Session 3: same ephemeral database as session 2 → hit → no recompute
        pf3 = PythonDataFunction(counting_double, output_keys="result")
        pod3 = FunctionPod(pf3)
        node3 = FunctionJobNode(
            function_pod=pod3,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node3.node_config = NodeConfig(is_result_ephemeral=True)
        node3.set_ephemeral_database(ephemeral2)  # reuse session 2's store
        node3._cached_output_datas.clear()
        node3.execute(stream)
        assert call_count["n"] == 2  # NOT recomputed

    def test_iter_data_serves_result_after_cross_session_recompute(self):
        """After a cross-session ephemeral miss triggers recompute via execute(),
        iter_data() serves the fresh result without triggering additional computation.

        Verifies that:
        - after clearing the node, its next run loads the recomputed result from the
          pipeline + ephemeral DBs (exercises _load_cached_entries)
        - repeated iter_data() calls return the same result without recomputing
        """
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()

        # Session 1: compute with ephemeral database 1 → pipeline DB gets index-0 record
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node1 = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node1.node_config = NodeConfig(is_result_ephemeral=True)
        node1.set_ephemeral_database(InMemoryArrowDatabase())
        node1.execute(stream)
        assert call_count["n"] == 1

        # Session 2: fresh ephemeral database → cross-session miss → recompute → index-1 record written
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2)
        ephemeral2 = InMemoryArrowDatabase()
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True)
        node2.set_ephemeral_database(ephemeral2)
        node2.execute(stream)
        assert call_count["n"] == 2  # recomputed once due to cross-session miss

        # Clearing the node drops its in-memory results; running it again loads the
        # recomputed result through the DB join path (_load_cached_entries →
        # _fetch_joined_records) rather than the in-memory dict fast-path. A node shows
        # rows only after a run (ITL-1095).
        node2.clear_cache()
        node2.execute(stream)

        # iter_data() must serve the recomputed result — no additional computation
        results = list(node2.iter_data())
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 2  # NOT recomputed again

        # A second iter_data() call must also return the same result without recomputing
        results2 = list(node2.iter_data())
        assert len(results2) == 1
        assert results2[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 2  # still NOT recomputed


# ---------------------------------------------------------------------------
# Task 9 tests: pipeline propagation
# ---------------------------------------------------------------------------


class TestPipelineInjectsStore:
    def test_pipeline_job_set_ephemeral_database_propagates(self):
        """PipelineJob.set_ephemeral_database propagates to all compiled nodes."""
        from orcapod.pipeline.job import PipelineJob

        # A job's input must be a source (ITL-1126: a raw stream has no system tags).
        stream = ArrowTableSource(
            _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}]).as_table(),
            tag_columns=["id"],
            tag_only_columns=True,
        )
        pod = _make_pod()

        job = PipelineJob(name="test_pipeline")
        with job:
            pod(stream)

        ephemeral_store = InMemoryArrowDatabase()
        job.set_ephemeral_database(ephemeral_store)

        # Every function node in the compiled pipeline should now have the database
        for label, node in job.function_pods.items():
            assert node.ephemeral_result_database is ephemeral_store, (
                f"Node '{label}' did not receive the ephemeral database"
            )

    def test_pipeline_job_set_ephemeral_database_none_detaches(self):
        """PipelineJob.set_ephemeral_database(None) detaches the database from all nodes."""
        from orcapod.pipeline.job import PipelineJob

        # A job's input must be a source (ITL-1126: a raw stream has no system tags).
        stream = ArrowTableSource(
            _make_stream([{"id": 0, "x": 10}]).as_table(),
            tag_columns=["id"],
            tag_only_columns=True,
        )
        pod = _make_pod()

        job = PipelineJob(name="test_pipeline")
        with job:
            pod(stream)

        store = InMemoryArrowDatabase()
        job.set_ephemeral_database(store)
        job.set_ephemeral_database(None)

        for label, node in job.function_pods.items():
            assert node.ephemeral_result_database is None, (
                f"Node '{label}' ephemeral database was not detached"
            )


# ---------------------------------------------------------------------------
# Task 10 tests: persistent miss warning and ephemeral-only node
# ---------------------------------------------------------------------------


class TestPersistentMissWarning:
    def test_persistent_miss_warns_and_recomputes(self, caplog):
        """Tag table has a regular record_id but persistent DB was trimmed: WARNING emitted."""
        import logging

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Write a persistent record
        node, _ = _make_node(
            stream,
            pipeline_db=pipeline_db,
            result_db=result_db,
            is_result_ephemeral=False,
        )
        node.execute(stream)

        # Wipe the result DB to simulate data loss
        result_db._tables.clear()
        result_db._pending_batches.clear()

        # Recreate node with same pipeline_db (tag entry still there) but empty result_db
        node2, _ = _make_node(
            stream,
            pipeline_db=pipeline_db,
            result_db=result_db,
            is_result_ephemeral=False,
        )

        with caplog.at_level(
            logging.WARNING, logger="orcapod.core.nodes.function_node"
        ):
            results = list(node2.execute(stream).iter_data())

        assert len(results) == 1  # recomputed
        assert any(
            "have no match in persistent result DB" in msg for msg in caplog.messages
        )

    def test_recompute_after_persistent_miss_appends_new_pipeline_record(self):
        """After persistent miss and recompute, tag table has two rows; next call hits."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: write persistent record
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node1 = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node1.execute(stream)
        assert call_count["n"] == 1

        # Simulate data loss
        result_db._tables.clear()
        result_db._pending_batches.clear()

        # Session 2: miss → recompute → appends new record to pipeline_db
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2)
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node2.execute(stream)
        assert call_count["n"] == 2  # recomputed

        # Session 3: tag table now has two rows (stale + new); inner join resolves correctly
        pf3 = PythonDataFunction(counting_double, output_keys="result")
        pod3 = FunctionPod(pf3)
        node3 = FunctionJobNode(
            function_pod=pod3,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node3._cached_output_datas.clear()
        node3.execute(stream)
        assert call_count["n"] == 2  # NOT recomputed — new row was found


class TestEphemeralOnlyNode:
    def test_ephemeral_only_node_no_persistent_db(self):
        """NodeConfig(is_result_ephemeral=True) with no result_database works end-to-end."""
        stream = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)

        # No result_database — pipeline_db doubles as both
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_database(ephemeral_store)
        results = list(node.execute(stream).iter_data())

        assert len(results) == 2
        vals = {tag.as_dict()["id"]: data.as_dict()["result"] for tag, data in results}
        assert vals == {0: 20, 1: 40}


# ---------------------------------------------------------------------------
# Task 11 tests: async ephemeral execution path
# ---------------------------------------------------------------------------


class TestAsyncEphemeralExecution:
    @pytest.mark.asyncio
    async def test_async_execute_ephemeral_happy_path(self):
        """async_execute with is_result_ephemeral=True writes to ephemeral database and emits results."""
        stream = _make_stream([{"id": 0, "x": 5}, {"id": 1, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_database(ephemeral_store)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        for tag, data in stream.iter_data():
            await input_ch.writer.send((tag, data))
        await input_ch.writer.close()

        await node.async_execute(input_ch.reader, output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 2
        values = sorted(data.as_dict()["result"] for _, data in results)
        assert values == [10, 20]

        # Result records must be in the ephemeral database, not the persistent database
        eph_records = ephemeral_store.get_all_records(
            node._ephemeral_cached_pod.record_path,
        )
        assert eph_records is not None
        assert eph_records.num_rows == 2

    @pytest.mark.asyncio
    async def test_async_process_data_internal_raises_when_no_store(self):
        """_async_process_data_internal raises RuntimeError when is_result_ephemeral=True but no database."""
        stream = _make_stream([{"id": 0, "x": 5}])
        pipeline_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        # set_ephemeral_database never called → _ephemeral_cached_pod is None

        tag, data = next(iter(stream.iter_data()))
        with pytest.raises(RuntimeError, match="is_result_ephemeral=True"):
            await node._async_process_data_internal(tag, data)


# ---------------------------------------------------------------------------
# add_pipeline_record: one row per record id (ITL-1471)
# ---------------------------------------------------------------------------


class TestAddPipelineRecordIndexed:
    def test_first_call_writes_one_row_keyed_by_its_record_id(self):
        """The first row's record id is hash(instance id, result id); no index column."""
        from orcapod.core import row_ids
        from orcapod.types import ContentHash

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))

        result_id = uuid.uuid4()
        node.add_pipeline_record(tag, data, data_record_id=result_id, computed=True)
        pipeline_db.flush()

        all_records = pipeline_db.get_all_records(
            node._versioned_pipeline_path, record_id_column="__row_key"
        )
        assert all_records is not None
        assert all_records.num_rows == 1
        assert "__pipeline_recomputation_index" not in all_records.column_names
        expected = row_ids.record_id(
            node.data_context.arrow_hasher,
            node.content_hash(),
            ContentHash.from_prefixed_digest(node.compute_instance_id(tag, data)),
            result_id,
        ).to_prefixed_digest()
        assert all_records.column(row_ids.RECORD_ID_COL).to_pylist() == [expected]
        # The table's key is the record id, which covers the node's content hash.
        assert all_records.column("__row_key").to_pylist() == [expected]

    def test_second_call_with_a_new_result_writes_a_second_row(self):
        """A recompute (a new result id) for the same instance is a new row."""
        from orcapod.core.nodes.function_node import INSTANCE_ID_COL

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))

        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()
        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()

        all_records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert all_records.num_rows == 2
        assert len(set(all_records.column(INSTANCE_ID_COL).to_pylist())) == 1
        assert len(set(all_records.column("__data_id").to_pylist())) == 2

    def test_second_call_with_the_same_result_is_skipped(self):
        """The same instance and result recorded again is one row, with its first time."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))
        result_id = uuid.uuid4()

        node.add_pipeline_record(tag, data, data_record_id=result_id, computed=True)
        pipeline_db.flush()
        first = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert first is not None
        node.add_pipeline_record(tag, data, data_record_id=result_id, computed=False)
        pipeline_db.flush()

        again = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert again is not None
        assert again.num_rows == 1
        assert again.column("__recorded_at").to_pylist() == (
            first.column("__recorded_at").to_pylist()
        )
        assert again.column("__computed").to_pylist() == [True]

    def test_instance_id_column_written(self):
        """add_pipeline_record writes INSTANCE_ID_COL to the pipeline DB row."""
        from orcapod.core.nodes.function_node import INSTANCE_ID_COL

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))

        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()

        all_records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert INSTANCE_ID_COL in all_records.column_names
        expected_base_id = node.compute_instance_id(tag, data)
        assert all_records.column(INSTANCE_ID_COL)[0].as_py() == expected_base_id

    def test_skip_cache_lookup_parameter_removed(self):
        """add_pipeline_record no longer accepts skip_cache_lookup — raises TypeError."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag, data = next(iter(stream.iter_data()))
        with pytest.raises(TypeError, match="skip_cache_lookup"):
            node.add_pipeline_record(
                tag,
                data,
                data_record_id=uuid.uuid4(),
                computed=True,
                skip_cache_lookup=False,  # removed parameter
            )


# ---------------------------------------------------------------------------
# Task 13 tests: _fetch_joined_records guard and backward-compat branch
# ---------------------------------------------------------------------------


class TestFetchJoinedRecordsGuards:
    def test_get_all_records_no_db_returns_none(self):
        """get_all_records() on a FunctionJobNode with no pipeline_database returns None."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        # Intentionally no pipeline_database → _cached_function_pod is None
        node = FunctionJobNode(function_pod=pod, input_stream=stream)

        result = node.get_all_records()
        assert result is None

    def test_legacy_records_without_ephemeral_col_treated_as_persistent(self):
        """Records lacking IS_EPHEMERAL_COL are treated as persistent (backward compat)."""
        from orcapod import system_constants as sc

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: write a normal persistent record
        node1, _ = _make_node(
            stream,
            pipeline_db=pipeline_db,
            result_db=result_db,
            is_result_ephemeral=False,
        )
        node1.execute(stream)
        pipeline_db.flush()

        # Drop IS_EPHEMERAL_COL from the committed table to simulate a legacy record
        is_eph_col = sc.constants.IS_EPHEMERAL_COL
        record_key = "/".join(node1._versioned_pipeline_path)
        old_table = pipeline_db._tables[record_key]
        col_idx = old_table.schema.get_field_index(is_eph_col)
        assert col_idx >= 0, "IS_EPHEMERAL_COL must exist before we drop it"
        pipeline_db._tables[record_key] = old_table.remove_column(col_idx)

        # Session 2: new node with same DBs — should handle missing column gracefully
        pf2 = PythonDataFunction(double, output_keys="result")
        pod2 = FunctionPod(pf2)
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        results = list(node2.execute(stream).iter_data())

        # Result must be served (from result DB cache or recomputed)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20


# ---------------------------------------------------------------------------
# Task 6 tests: concurrent asyncio Phase 2 serialisation
# ---------------------------------------------------------------------------


class TestConcurrentMissSerialization:
    @pytest.mark.asyncio
    async def test_two_concurrent_phase2_misses_produce_valid_pipeline_records(self):
        """Two asyncio coroutines that simultaneously execute Phase 2 for the same input
        each produce a valid pipeline record. A subsequent Phase 1 lookup finds a result
        and does NOT recompute."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )

        tag, data = next(iter(stream.iter_data()))

        # Two concurrent _async_process_data_internal calls on the same (tag, data).
        # In asyncio cooperative multitasking, these serialise at add_pipeline_record
        # (synchronous); each computed its own result, so each row has its own key.
        async with asyncio.TaskGroup() as tg:
            tg.create_task(node._async_process_data_internal(tag, data))
            tg.create_task(node._async_process_data_internal(tag, data))

        # At least one pipeline record must exist for this instance_id
        pipeline_db.flush()
        all_records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert all_records.num_rows >= 1

        # Session 2: new node with the same DBs — Phase 1 must find a valid result
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2)
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        count_before_session2 = call_count["n"]
        results = list(node2.execute(stream).iter_data())
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20
        assert call_count["n"] == count_before_session2  # NOT recomputed

    @pytest.mark.asyncio
    async def test_sequential_add_pipeline_record_writes_a_row_per_result(self):
        """Two sequential add_pipeline_record calls for the same instance_id, each
        with its own result, write two rows (not blocked by the existing row)."""
        from orcapod.core.nodes.function_node import INSTANCE_ID_COL

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))

        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()

        all_records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert all_records.num_rows == 2

        base_ids = all_records.column(INSTANCE_ID_COL).to_pylist()
        assert base_ids[0] == base_ids[1]  # same instance_id

        assert len(set(all_records.column("__data_id").to_pylist())) == 2
