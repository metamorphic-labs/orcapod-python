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


class _TimestampKeepingConnector(SQLiteConnector):
    """A SQLite connector that states it stores timestamps; every other rule is SQLite's.

    The tests that use it are refused before a row is written, so it never has to
    store one.
    """

    def unstorable_column_reason(
        self, name: str, arrow_type: pa.DataType, *, primary_key: bool
    ) -> str | None:
        if pa.types.is_timestamp(arrow_type):
            return None
        return super().unstorable_column_reason(
            name, arrow_type, primary_key=primary_key
        )


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
        # Only the database that keeps every type is suggested (CodeRabbit on PR 471).
        assert "docs/reference/database-types.md" in message
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

    def test_operator_cache_is_refused_on_sqlite_when_it_is_attached(
        self, tmp_path: pathlib.Path
    ):
        """An operator cache stores an insertion time, which SQLite cannot hold.

        Every row of an operator cache is stored with ``__recorded_at``
        (``timestamp[us, tz=UTC]``, ITL-1471), a type a SQLite database has no
        column for, as for a function node's pipeline table. The database is
        refused when it is attached, naming the node and the column, before
        anything runs. (Before the insertion time, a cache of storable columns
        ran on SQLite.)
        """
        database = _sqlite_database(tmp_path)
        job = PipelineJob(name="p", database=database)
        with job:
            _ = SelectDataColumns(["x"])(_source(), label="sel")
        with pytest.raises(UnstorableColumnError) as exc_info:
            job.nodes["sel"].attach_databases(
                pipeline_database=database.at("p"), cache_mode=CacheMode.LOG
            )
        assert exc_info.value.node_label == "sel"
        assert exc_info.value.column == "__recorded_at"
        assert exc_info.value.arrow_type == pa.timestamp("us", tz="UTC")
        assert database.list_table_paths() == []
        # Without caching nothing is stored, so the database is accepted.
        job.nodes["sel"].attach_databases(
            pipeline_database=database.at("p"), cache_mode=CacheMode.OFF
        )
        _ = job.run()
        assert sorted(job.nodes["sel"].as_table()["x"].to_pylist()) == [1, 2]

    def test_operator_cache_with_unstorable_column_refused_at_write(
        self, tmp_path: pathlib.Path
    ):
        """An operator cache holding an int32 column is refused when it is written.

        The cache's own columns are checked when the database is attached; the
        operator's output columns only when they are written. SQLite is refused
        at attach (the insertion time, above), so the write is reached through a
        connector that takes a timestamp and, like SQLite, no ``int32``.
        """
        database = ConnectorArrowDatabase(
            _TimestampKeepingConnector(str(tmp_path / "store.db"))
        )
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


# SpiralDB refuses every write for now: tests/test_databases/test_spiraldb_reading.py.


# ---------------------------------------------------------------------------
# Every stored table of a node is checked, also through a wrapper
# ---------------------------------------------------------------------------


class TestEveryTableChecked:
    def test_function_node_pipeline_database_checked(self, tmp_path: pathlib.Path):
        """A SQLite pipeline database is refused even when results go elsewhere."""
        job = PipelineJob(name="p", database=InMemoryArrowDatabase())
        with job:
            _ = double.pod(_source(), label="dbl")
        node = job.nodes["dbl"]

        with pytest.raises(UnstorableColumnError) as exc_info:
            node.attach_databases(
                pipeline_database=_sqlite_database(tmp_path),
                result_database=InMemoryArrowDatabase(),
            )

        assert exc_info.value.node_label == "dbl"
        assert exc_info.value.column == "__recorded_at"
        assert exc_info.value.arrow_type == pa.timestamp("us", tz="UTC")

    def test_extension_aware_wrapper_checked_before_delivery(
        self, tmp_path: pathlib.Path
    ):
        """The check reaches a connector database inside ``ExtensionAwareDatabase``."""
        from orcapod.contexts import get_default_context
        from orcapod.databases import ExtensionAwareDatabase

        # The wrapper's annotations do not match the protocols (existing debt).
        converter: Any = get_default_context().type_converter
        wrapped: Any = ExtensionAwareDatabase(
            _sqlite_database(tmp_path), converter=converter
        )
        job = PipelineJob(name="p", database=wrapped)

        with pytest.raises(UnstorableColumnError) as exc_info:
            with job:
                _ = deliver.process(_source(), label="out")

        assert exc_info.value.node_label == "out"
        assert exc_info.value.column == "executed_at"
        assert DELIVERED == []

    def test_extension_aware_wrapper_over_in_memory_not_checked(self):
        """A wrapped database that states no rule stores every column."""
        from orcapod.contexts import get_default_context
        from orcapod.databases import ExtensionAwareDatabase

        converter: Any = get_default_context().type_converter
        wrapped: Any = ExtensionAwareDatabase(
            InMemoryArrowDatabase(), converter=converter
        )
        job = PipelineJob(name="p", database=wrapped)
        with job:
            _ = double.pod(_source(), label="dbl")
        _ = job.run()
        assert sorted(CALLS) == [1, 2]


class TestRefusedBindLeavesJobAsItWas:
    def test_refused_database_bind_changes_nothing(self, tmp_path: pathlib.Path):
        """Every node is checked before any is attached (CodeRabbit, ITL-1162)."""
        in_memory = InMemoryArrowDatabase()
        job = PipelineJob(name="p", database=in_memory)
        with job:
            out = double.pod(_source(), label="dbl")
            _ = SelectDataColumns(["double"])(out, label="sel")
        _ = job.run()
        assert sorted(CALLS) == [1, 2]
        sqlite = _sqlite_database(tmp_path)

        with pytest.raises(UnstorableColumnError) as exc_info:
            job.bind(database=sqlite)

        assert exc_info.value.node_label == "dbl"
        # The job keeps its database: a new run reads the stored results.
        job.nodes["dbl"].clear_cache()
        CALLS.clear()
        _ = job.run()
        assert CALLS == []
        assert sorted(job.nodes["dbl"].as_table()["double"].to_pylist()) == [2, 4]
        assert sqlite.list_table_paths() == []


class TestWriteChecksOrder:
    def test_extension_type_refused_as_unstorable_column(self, tmp_path: pathlib.Path):
        """The type rule runs before the metadata check (CodeRabbit, ITL-1162)."""
        database = _sqlite_database(tmp_path)
        table = pa.table({"u": pa.array([b"0123456789abcdef"], type=pa.uuid())})

        with pytest.raises(UnstorableColumnError) as exc_info:
            database.add_record(("t",), b"1", table)

        assert exc_info.value.column == "u"

    def test_buffered_text_reads_with_committed_text(self, tmp_path: pathlib.Path):
        """Buffered ``string`` rows read together with committed ``large_string`` rows."""
        database = _sqlite_database(tmp_path)
        table = pa.table({"v": pa.array(["a"], type=pa.string())})
        database.add_record(("t",), b"1", table, flush=True)
        database.add_record(("t",), b"2", table)

        stored = database.get_all_records(("t",))

        assert stored is not None
        assert stored.schema.field("v").type == pa.large_string()
        assert stored["v"].to_pylist() == ["a", "a"]

    @pytest.mark.parametrize(
        "arrow_type, values",
        [
            pytest.param(pa.string_view(), ["a"], id="string_view"),
            pytest.param(pa.binary_view(), [b"a"], id="binary_view"),
        ],
    )
    def test_view_types_stored_in_large_variant(
        self, tmp_path: pathlib.Path, arrow_type, values
    ):
        """View layouts are normalized before the check, as every table is."""
        database = _sqlite_database(tmp_path)
        table = pa.table({"v": pa.array(values, type=arrow_type)})
        database.add_record(("t",), b"1", table, flush=True)

        stored = database.get_all_records(("t",))

        assert stored is not None
        assert stored["v"].to_pylist() == values


# ---------------------------------------------------------------------------
# PostgreSQL truncates identifiers to 63 bytes; SQLite has no such limit
# ---------------------------------------------------------------------------


def _mocked_postgresql() -> tuple[PostgreSQLConnector, MagicMock]:
    """A ``PostgreSQLConnector`` over a mocked connection (no server)."""
    with patch("psycopg.connect") as mock_connect:
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value.fetchone.return_value = None
        mock_connect.return_value = conn
        return PostgreSQLConnector("postgresql://localhost/test"), conn


class TestIdentifierLength:
    @pytest.mark.parametrize(
        "name, storable",
        [
            ("a" * 63, True),
            ("a" * 64, False),
            ("é" * 31, True),  # 62 bytes
            ("é" * 32, False),  # 64 bytes
            ("_tag_source_id::" + "0" * 64, False),  # a system-tag column name
        ],
    )
    def test_postgresql_rule(self, name, storable):
        connector, _ = _mocked_postgresql()
        try:
            reason = connector.unstorable_column_reason(
                name, pa.int64(), primary_key=False
            )
        finally:
            connector._conn = None
        assert (reason is None) is storable, reason
        if not storable:
            assert reason is not None and "63 bytes" in reason

    def test_postgresql_refuses_long_name_before_anything_is_written(self):
        connector, conn = _mocked_postgresql()
        try:
            database = ConnectorArrowDatabase(connector)
            name = "_tag_source_id::" + "0" * 64
            table = pa.table({name: pa.array(["s"], type=pa.large_string())})
            calls_before = len(conn.mock_calls)

            with pytest.raises(UnstorableColumnError) as exc_info:
                database.add_record(("t",), b"1", table)
            database.flush()  # nothing was buffered

            assert exc_info.value.column == name
            assert exc_info.value.database_kind == "PostgreSQLConnector"
            assert len(conn.mock_calls) == calls_before  # no statement was sent
        finally:
            connector._conn = None

    def test_sqlite_keeps_long_names(self, tmp_path: pathlib.Path):
        """SQLite has no identifier length limit: a long name reads back as written."""
        database = _sqlite_database(tmp_path)
        name = "_tag_source_id::" + "0" * 200
        table = pa.table({name: pa.array(["s"], type=pa.large_string())})
        database.add_record(("t",), b"1", table, flush=True)

        stored = database.get_all_records(("t",))

        assert stored is not None
        assert name in stored.column_names


class TestPipelineRecordsAndEphemeralResults:
    def test_ephemeral_database_checked_before_anything_changes(
        self, tmp_path: pathlib.Path
    ):
        job = PipelineJob(name="p", database=InMemoryArrowDatabase())
        with job:
            _ = double.pod(_source(), label="dbl")

        with pytest.raises(UnstorableColumnError) as exc_info:
            job.set_ephemeral_database(_sqlite_database(tmp_path))

        assert exc_info.value.node_label == "dbl"
        assert exc_info.value.column == EXECUTOR_INFO
        # No node took the database: a run computes and stores as before.
        assert job.nodes["dbl"].ephemeral_result_database is None
        _ = job.run()
        assert sorted(CALLS) == [1, 2]

    def test_attach_with_refused_ephemeral_database_changes_nothing(
        self, tmp_path: pathlib.Path
    ):
        in_memory = InMemoryArrowDatabase()
        job = PipelineJob(name="p", database=in_memory)
        with job:
            _ = double.pod(_source(), label="dbl")
        _ = job.run()
        node = job.nodes["dbl"]

        with pytest.raises(UnstorableColumnError):
            node.attach_databases(
                pipeline_database=InMemoryArrowDatabase(),
                ephemeral_database=_sqlite_database(tmp_path),
            )

        # The node kept its databases and its run: nothing is recomputed.
        CALLS.clear()
        assert sorted(node.as_table()["double"].to_pylist()) == [2, 4]
        assert node.ephemeral_result_database is None
        assert CALLS == []


# ---------------------------------------------------------------------------
# In-PR CodeRabbit review on 807166aa
# ---------------------------------------------------------------------------


class TestReviewFollowUps:
    def test_error_survives_pickling(self):
        import copy
        import pickle

        err = UnstorableColumnError(
            database_kind="SQLiteConnector",
            column="v",
            arrow_type=pa.int32(),
            reason="r",
            node_label="dbl",
        )
        for again in (pickle.loads(pickle.dumps(err)), copy.copy(err)):
            assert isinstance(again, UnstorableColumnError)
            assert (again.column, again.arrow_type, again.node_label) == (
                "v",
                pa.int32(),
                "dbl",
            )
            assert str(again) == str(err)

    @pytest.mark.parametrize(
        "name", ["xmin", "xmax", "cmin", "cmax", "ctid", "tableoid"]
    )
    def test_postgresql_refuses_system_column_names(self, name):
        connector, _ = _mocked_postgresql()
        try:
            reason = connector.unstorable_column_reason(
                name, pa.int64(), primary_key=False
            )
        finally:
            connector._conn = None
        assert reason is not None and "system column" in reason

    def test_union_refused_before_any_cast(self, tmp_path: pathlib.Path):
        """A refused column raises the refusal, not an Arrow cast error."""
        database = _sqlite_database(tmp_path)
        union = pa.UnionArray.from_dense(
            pa.array([0], type=pa.int8()),
            pa.array([0], type=pa.int32()),
            [pa.array(["a"], type=pa.string())],
        )
        table = pa.table({"u": union})

        with pytest.raises(UnstorableColumnError) as exc_info:
            database.add_record(("t",), b"1", table)

        assert exc_info.value.column == "u"

    def test_nested_cached_pod_checked(self, tmp_path: pathlib.Path):
        """A pod with its own cache database is checked with the job's (ITL-1162)."""
        sqlite = _sqlite_database(tmp_path)

        @function_pod(output_keys="double", pod_cache_database=sqlite)
        def cached_double(x: int) -> int:
            CALLS.append(x)
            return 2 * x

        job = PipelineJob(name="p", database=InMemoryArrowDatabase())
        with pytest.raises(UnstorableColumnError) as exc_info:
            with job:
                _ = cached_double.pod(_source(), label="dbl")

        assert exc_info.value.node_label == "dbl"
        assert exc_info.value.database_kind == "SQLiteConnector"
        assert CALLS == []

    def test_sql_type_maps_accept_view_types(self, tmp_path: pathlib.Path):
        """CREATE TABLE maps view layouts as the rule reads them (CLI run 12)."""
        from orcapod.types import ColumnInfo

        connector = SQLiteConnector(str(tmp_path / "views.db"))
        try:
            connector.create_table_if_not_exists(
                "t",
                [
                    ColumnInfo("k", pa.string_view(), nullable=False),
                    ColumnInfo("b", pa.binary_view(), nullable=True),
                ],
                pk_column="k",
            )
            types = {c.name: c.arrow_type for c in connector.get_column_info("t")}
        finally:
            connector.close()
        assert types == {"k": pa.large_string(), "b": pa.large_binary()}
        from orcapod.databases.postgresql_connector import _arrow_type_to_pg_sql

        assert _arrow_type_to_pg_sql(pa.string_view()) == "TEXT"
        assert _arrow_type_to_pg_sql(pa.binary_view()) == "BYTEA"


# ---------------------------------------------------------------------------
# PostgreSQL table names over 63 bytes (coordinator ruling, ITL-1162)
# ---------------------------------------------------------------------------


class TestTableNameLength:
    def test_connector_rules(self, tmp_path: pathlib.Path):
        connector, _ = _mocked_postgresql()
        try:
            assert connector.unstorable_table_name_reason("t" * 63) is None
            reason = connector.unstorable_table_name_reason("t" * 64)
            assert reason is not None and "63 bytes" in reason
        finally:
            connector._conn = None
        sqlite = SQLiteConnector(str(tmp_path / "names.db"))
        try:
            assert sqlite.unstorable_table_name_reason("t" * 500) is None
        finally:
            sqlite.close()

    def test_long_paths_can_be_buffered_and_read(self):
        connector, _ = _mocked_postgresql()
        try:
            database = ConnectorArrowDatabase(connector)
            path = ("fn", "x" * 70)
            table = pa.table({"v": pa.array([1], type=pa.int64())})
            database.add_record(path, b"1", table)
            assert database.table_exists(path)
            assert database.list_table_paths() == [path]
            stored = database.get_all_records(path)
            assert stored is not None and stored.equals(table)
        finally:
            connector._conn = None

    def test_connector_entries_refuse_a_long_name(self):
        from orcapod.errors import UnstorableTableNameError
        from orcapod.types import ColumnInfo

        connector, conn = _mocked_postgresql()
        name = "t" * 64
        try:
            calls_before = len(conn.mock_calls)
            with pytest.raises(UnstorableTableNameError):
                connector.create_table_if_not_exists(
                    name, [ColumnInfo("k", pa.large_string(), False)], pk_column="k"
                )
            with pytest.raises(UnstorableTableNameError):
                connector.get_column_info(name)
            with pytest.raises(UnstorableTableNameError):
                connector.get_pk_columns(name)
            with pytest.raises(UnstorableTableNameError):
                connector.upsert_records(name, pa.table({"k": ["a"]}), id_column="k")
            assert len(conn.mock_calls) == calls_before
        finally:
            connector._conn = None

    def test_cached_source_on_postgresql_refused(self):
        """A cached source's system-tag column names exceed 63 bytes."""
        from orcapod.core.sources.cached_source import CachedSource

        connector, conn = _mocked_postgresql()
        try:
            cached = CachedSource(
                _source(), cache_database=ConnectorArrowDatabase(connector)
            )
            calls_before = len(conn.mock_calls)
            with pytest.raises(Exception) as exc_info:
                _ = cached.as_table()
            chain: list[BaseException] = []
            err: BaseException | None = exc_info.value
            while err is not None:
                chain.append(err)
                err = err.__cause__ or err.__context__
            assert any(isinstance(e, UnstorableColumnError) for e in chain), chain
            statements = " ".join(
                str(call.args[0]).upper()
                for call in conn.mock_calls[calls_before:]
                if call.args and isinstance(call.args[0], str)
            )
            assert "CREATE" not in statements and "INSERT" not in statements
        finally:
            connector._conn = None

    def test_pickling(self):
        import pickle

        from orcapod.errors import UnstorableTableNameError

        err = UnstorableTableNameError(
            database_kind="PostgreSQLConnector", table_name="t", reason="r"
        )
        again = pickle.loads(pickle.dumps(err))
        assert str(again) == str(err)


class TestInPRReviewOn45e5e44e:
    def test_data_function_result_database_checked(self, tmp_path: pathlib.Path):
        """``function_pod(result_database=...)`` caches in its own database too."""
        sqlite = _sqlite_database(tmp_path)

        @function_pod(output_keys="double", result_database=sqlite)
        def cached_double(x: int) -> int:
            CALLS.append(x)
            return 2 * x

        job = PipelineJob(name="p", database=InMemoryArrowDatabase())
        with pytest.raises(UnstorableColumnError) as exc_info:
            with job:
                _ = cached_double.pod(_source(), label="dbl")

        assert exc_info.value.node_label == "dbl"
        assert exc_info.value.database_kind == "SQLiteConnector"
        assert CALLS == []

    @pytest.mark.parametrize("name", ["sqlite_results", "SQLite_x", "SQLITE_"])
    def test_path_can_start_with_a_reserved_sqlite_prefix(
        self, tmp_path: pathlib.Path, name
    ):
        database = _sqlite_database(tmp_path)
        table = pa.table({"v": pa.array([1], type=pa.int64())})
        database.add_record((name,), b"1", table, flush=True)
        assert database.list_table_paths() == [(name,)]
        stored = database.get_all_records((name,))
        assert stored is not None and stored.equals(table)
