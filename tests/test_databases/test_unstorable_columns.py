"""A connector database refuses a table it cannot store, before anything is written (ITL-1162).

Each connector states which columns it stores: those it reads back as written
(``unstorable_column_reason``). ``ConnectorArrowDatabase`` refuses any other column
before anything of the table is buffered, and a job refuses a function or
side-effect node whose stored table holds one when the database is attached, before
anything runs. A function node's result table always holds list-of-struct executor
columns and a UTC timestamp, so no function node runs on SQLite or PostgreSQL.
"""

from __future__ import annotations

import datetime
import pathlib
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import SelectDataColumns
from orcapod.databases import (
    ConnectorArrowDatabase,
    InMemoryArrowDatabase,
    PostgreSQLConnector,
    SQLiteConnector,
)
from orcapod.errors import OrcapodDatabaseWriteError, UnstorableColumnError
from orcapod.side_effects import SideEffectPod, SideEffectPodConfig
from orcapod.sources import ArrowTableSource, DictSource
from orcapod.types import CacheMode

CALLS: list[int] = []
DELIVERED: list[int] = []

EXECUTOR_INFO = "__pf_exec_executor_info"
EXECUTOR_INFO_TYPE = pa.large_list(
    pa.struct([("key", pa.large_string()), ("value", pa.large_string())])
)


@pytest.fixture(autouse=True)
def _reset() -> None:
    """Start every test with no recorded calls or deliveries."""
    CALLS.clear()
    DELIVERED.clear()


@function_pod(output_keys="double")
def double(x: int) -> int:
    """Double ``x``, recording the call."""
    CALLS.append(x)
    return 2 * x


def _deliver(x: int, ctx: Any) -> None:
    """A side effect: record the delivered value."""
    DELIVERED.append(x)


deliver = SideEffectPod(_deliver, SideEffectPodConfig(track_completion=True))


def _source(rows: list[dict[str, Any]] | None = None) -> DictSource:
    """A small in-memory source with tag ``k``."""
    return DictSource(
        data=rows if rows is not None else [{"k": "a", "x": 1}, {"k": "b", "x": 2}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="src",
    )


def _sqlite_database(tmp_path: pathlib.Path) -> ConnectorArrowDatabase:
    return ConnectorArrowDatabase(SQLiteConnector(str(tmp_path / "store.db")))


# ---------------------------------------------------------------------------
# A job refuses a node up front
# ---------------------------------------------------------------------------


class TestJobRefusesUpFront:
    def test_function_node_on_sqlite_refused_before_anything_runs(
        self, tmp_path: pathlib.Path
    ):
        """The reported case: one function node on a SQLite job database."""
        database = _sqlite_database(tmp_path)
        job = PipelineJob(name="p", database=database)

        with pytest.raises(UnstorableColumnError) as exc_info:
            with job:
                _ = double.pod(_source(), label="dbl")

        err = exc_info.value
        assert err.node_label == "dbl"
        assert err.database_kind == "SQLiteConnector"
        assert err.column == EXECUTOR_INFO
        assert err.arrow_type == EXECUTOR_INFO_TYPE
        message = str(err)
        assert "node 'dbl'" in message
        assert "SQLiteConnector" in message
        assert EXECUTOR_INFO in message
        assert "InMemoryArrowDatabase" in message
        assert "DeltaTableDatabase" in message
        # Nothing ran and nothing was written.
        assert CALLS == []
        assert database.list_table_paths() == []

    def test_side_effect_node_on_sqlite_refused_before_delivery(
        self, tmp_path: pathlib.Path
    ):
        """The invocation log's UTC timestamp is refused before anything is delivered."""
        database = _sqlite_database(tmp_path)
        job = PipelineJob(name="p", database=database)

        with pytest.raises(UnstorableColumnError) as exc_info:
            with job:
                _ = deliver.process(_source(), label="out")

        err = exc_info.value
        assert err.node_label == "out"
        assert err.column == "executed_at"
        assert err.arrow_type == pa.timestamp("us", tz="UTC")
        assert DELIVERED == []
        assert database.list_table_paths() == []

    def test_function_node_on_postgresql_refused(self):
        """PostgreSQL has no column type for the list-of-struct executor columns."""
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
        try:
            job = PipelineJob(name="p", database=ConnectorArrowDatabase(connector))
            with pytest.raises(UnstorableColumnError) as exc_info:
                with job:
                    _ = double.pod(_source(), label="dbl")
        finally:
            connector._conn = None  # nothing to close
        err = exc_info.value
        assert err.node_label == "dbl"
        assert err.database_kind == "PostgreSQLConnector"
        assert err.column == EXECUTOR_INFO
        assert CALLS == []

    def test_in_memory_database_still_runs_function_node(self):
        """A database that states no rule stores every column."""
        job = PipelineJob(name="p", database=InMemoryArrowDatabase())
        with job:
            _ = double.pod(_source(), label="dbl")
        _ = job.run()
        assert sorted(CALLS) == [1, 2]


# ---------------------------------------------------------------------------
# The write itself refuses, before anything is buffered
# ---------------------------------------------------------------------------


UNSTORABLE_ON_SQLITE = [
    pytest.param(pa.int8(), [1], id="int8"),
    pytest.param(pa.int32(), [1], id="int32"),
    pytest.param(pa.uint64(), [1], id="uint64"),
    pytest.param(pa.float32(), [1.5], id="float32"),
    pytest.param(pa.date32(), [datetime.date(2026, 1, 1)], id="date32"),
    pytest.param(
        pa.timestamp("us", tz="UTC"),
        [datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)],
        id="timestamp_utc",
    ),
    pytest.param(
        pa.timestamp("us"), [datetime.datetime(2026, 1, 1)], id="timestamp_naive"
    ),
    pytest.param(pa.large_list(pa.int64()), [[1]], id="large_list"),
    pytest.param(EXECUTOR_INFO_TYPE, [[{"key": "a", "value": "b"}]], id="list_struct"),
    pytest.param(pa.struct([("a", pa.int64())]), [{"a": 1}], id="struct"),
    pytest.param(pa.null(), [None], id="null"),
]

STORABLE_ON_SQLITE = [
    pytest.param(pa.bool_(), [True], id="bool"),
    pytest.param(pa.int64(), [1], id="int64"),
    pytest.param(pa.float64(), [1.5], id="float64"),
    pytest.param(pa.string(), ["a"], id="string"),
    pytest.param(pa.large_string(), ["a"], id="large_string"),
    pytest.param(pa.binary(), [b"a"], id="binary"),
    pytest.param(pa.large_binary(), [b"a"], id="large_binary"),
]


class TestWriteRefuses:
    @pytest.mark.parametrize("arrow_type, values", UNSTORABLE_ON_SQLITE)
    def test_sqlite_refuses_column_before_buffering(
        self, tmp_path: pathlib.Path, arrow_type, values
    ):
        database = _sqlite_database(tmp_path)
        table = pa.table({"v": pa.array(values, type=arrow_type)})

        with pytest.raises(UnstorableColumnError) as exc_info:
            database.add_record(("t",), b"1", table)

        err = exc_info.value
        assert err.column == "v"
        assert err.arrow_type == arrow_type
        assert err.node_label is None
        assert str(err).startswith("a SQLiteConnector database cannot store column")
        # Nothing was buffered, so nothing is written or read back.
        database.flush()
        assert database.get_all_records(("t",)) is None
        assert database.list_table_paths() == []

    @pytest.mark.parametrize("arrow_type, values", STORABLE_ON_SQLITE)
    def test_sqlite_stores_and_reads_back(
        self, tmp_path: pathlib.Path, arrow_type, values
    ):
        """What SQLite accepts reads back as written, and a second write succeeds."""
        from orcapod.utils.arrow_utils import normalize_to_large_types

        database = _sqlite_database(tmp_path)
        table = pa.table({"v": pa.array(values, type=arrow_type)})
        database.add_record(("t",), b"1", table, flush=True)
        database.add_record(("t",), b"2", table, flush=True)

        stored = database.get_all_records(("t",))
        assert stored is not None
        assert stored.schema.field("v").type == normalize_to_large_types(arrow_type)
        assert stored["v"].to_pylist() == values * 2

    def test_operator_cache_with_storable_columns_runs_on_sqlite(
        self, tmp_path: pathlib.Path
    ):
        """Operator caches hold only the operator's columns plus binary/text ones."""
        database = _sqlite_database(tmp_path)
        job = PipelineJob(name="p", database=database)
        with job:
            _ = SelectDataColumns(["x"])(_source(), label="sel")
        job.nodes["sel"].attach_databases(
            pipeline_database=database.at("p"), cache_mode=CacheMode.LOG
        )
        _ = job.run()
        job.nodes["sel"].clear_cache()
        _ = job.run()  # the second write meets the stored table's read-back types
        assert sorted(job.nodes["sel"].as_table()["x"].to_pylist()) == [1, 2]

    def test_operator_cache_with_unstorable_column_refused_at_write(
        self, tmp_path: pathlib.Path
    ):
        """An operator cache holding an int32 column is refused when it is written."""
        database = _sqlite_database(tmp_path)
        source = ArrowTableSource(
            pa.table(
                {
                    "k": pa.array(["a"], type=pa.large_string()),
                    "x": pa.array([1], type=pa.int32()),
                }
            ),
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="src",
        )
        job = PipelineJob(name="p", database=database)
        with job:
            _ = SelectDataColumns(["x"])(source, label="sel")
        job.nodes["sel"].attach_databases(
            pipeline_database=database.at("p"), cache_mode=CacheMode.LOG
        )

        with pytest.raises(OrcapodDatabaseWriteError) as exc_info:
            _ = job.run()

        assert exc_info.value.node_label == "sel"
        cause = exc_info.value.__cause__
        assert isinstance(cause, UnstorableColumnError)
        assert cause.column == "x"
        assert cause.arrow_type == pa.int32()
        assert database.list_table_paths() == []


# ---------------------------------------------------------------------------
# The rule each connector states
# ---------------------------------------------------------------------------


class TestConnectorRule:
    def test_sqlite_rule(self):
        connector = SQLiteConnector(":memory:")
        try:
            for arrow_type in (
                pa.bool_(),
                pa.int64(),
                pa.float64(),
                pa.string(),
                pa.large_string(),
                pa.binary(),
                pa.large_binary(),
            ):
                assert (
                    connector.unstorable_column_reason(
                        "c", arrow_type, primary_key=False
                    )
                    is None
                ), arrow_type
            reason = connector.unstorable_column_reason(
                "c", pa.int32(), primary_key=False
            )
            assert reason == "SQLite stores it as INTEGER, which reads back as int64"
            reason = connector.unstorable_column_reason(
                "c", pa.timestamp("us", tz="UTC"), primary_key=False
            )
            assert reason == "SQLite has no column type for it"
        finally:
            connector.close()

    @pytest.mark.parametrize(
        "arrow_type, storable",
        [
            (pa.bool_(), True),
            (pa.int8(), False),
            (pa.int16(), False),
            (pa.int32(), True),
            (pa.int64(), True),
            (pa.uint32(), False),
            (pa.float32(), True),
            (pa.float64(), True),
            (pa.string(), True),
            (pa.large_string(), True),
            (pa.binary(), True),
            (pa.large_binary(), True),
            (pa.date32(), True),
            (pa.timestamp("us"), True),
            (pa.timestamp("us", tz="UTC"), True),
            (pa.timestamp("ns", tz="UTC"), False),
            (pa.timestamp("us", tz="America/New_York"), False),
            (pa.large_list(pa.int64()), False),
            (EXECUTOR_INFO_TYPE, False),
            (pa.struct([("a", pa.int64())]), False),
            (pa.null(), False),
        ],
    )
    def test_postgresql_rule(self, arrow_type, storable):
        """By code reading: the type it writes reads back as the same type."""
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
        try:
            reason = connector.unstorable_column_reason(
                "c", arrow_type, primary_key=False
            )
        finally:
            connector._conn = None
        assert (reason is None) is storable, reason
