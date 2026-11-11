"""A failed store write must fail ``PipelineJob.run()`` (ITL-878).

Before the fix, a node whose write to the store failed was skipped like a row whose
function had raised: ``run()`` returned normally and the node read back fewer rows (often
zero). These tests drive ``PipelineJob`` through its public interface on both orchestrators,
with a test-double store that fails chosen writes, and with the SQLite trigger from the
report (a logical-type output on ``SQLiteConnector``).
"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass, field
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import SelectDataColumns
from orcapod.databases import (
    ConnectorArrowDatabase,
    InMemoryArrowDatabase,
    SQLiteConnector,
)
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DictSource
from orcapod.types import CacheMode

JOB_NAME = "itl878"

# Markers that select a table by its stored path.
RESULT_TABLE = "rdb_v1"  # a function node's result table
PIPELINE_TABLE = "pdb_v1"  # a function node's pipeline (tag) table
OPERATOR_TABLE = "SelectDataColumns"  # the operator node's LOG table


# ---------------------------------------------------------------------------
# Test-double store
# ---------------------------------------------------------------------------


@dataclass
class Faults:
    """Which writes a ``FaultyStore`` fails; shared by every view of one store."""

    write_marker: str | None = None
    succeed_first: int = 0
    on_flush: bool = False
    matching_writes: int = 0
    failed_paths: list[str] = field(default_factory=list)


class FaultyStore:
    """An ``InMemoryArrowDatabase`` that raises on chosen writes or on flush."""

    def __init__(self, inner: Any, faults: Faults) -> None:
        self._inner = inner
        self._faults = faults

    def at(self, *path_components: str) -> FaultyStore:
        return FaultyStore(self._inner.at(*path_components), self._faults)

    def _check_write(self, record_path: tuple[str, ...]) -> None:
        faults = self._faults
        full_path = "/".join(self._inner.base_path + tuple(record_path))
        if faults.write_marker is None or faults.write_marker not in full_path:
            return
        faults.matching_writes += 1
        if faults.matching_writes > faults.succeed_first:
            faults.failed_paths.append("/".join(record_path))
            raise OSError(f"disk unavailable while writing {full_path}")

    def add_record(
        self, record_path, record_id, record, skip_duplicates=False, flush=False
    ):
        self._check_write(record_path)
        self._inner.add_record(
            record_path, record_id, record, skip_duplicates=skip_duplicates, flush=flush
        )

    def add_records(
        self,
        record_path,
        records,
        record_id_column=None,
        skip_duplicates=False,
        flush=False,
    ):
        self._check_write(record_path)
        self._inner.add_records(
            record_path,
            records,
            record_id_column=record_id_column,
            skip_duplicates=skip_duplicates,
            flush=flush,
        )

    def flush(self) -> None:
        if self._faults.on_flush:
            raise OSError("disk unavailable while flushing")
        self._inner.flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


# ---------------------------------------------------------------------------
# Pipeline under test
# ---------------------------------------------------------------------------

ROWS = [{"k": f"r{i}", "x": i} for i in range(3)]

calls: list[int] = []


@function_pod(output_keys="double")
def double(x: int) -> int:
    calls.append(x)
    return x * 2


@function_pod(output_keys="double")
def double_or_fail(x: int) -> int:
    if x == 1:
        raise RuntimeError("function failed for x == 1")
    return x * 2


@function_pod(output_keys="where")
def to_path(x: int) -> pathlib.Path:
    return pathlib.Path(f"/data/{x}.txt")


def build_job(store: Any, rows: list[dict], *, pod=double, with_operator: bool = False):
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        source = DictSource(data=rows, tag_columns=["k"], source_id="src")
        out = pod.pod(source, label="dbl")
        if with_operator:
            SelectDataColumns(["double"])(out, label="sel")
    if with_operator:
        # OperatorNode.attach_databases is the public way to turn on LOG mode.
        job.nodes["sel"].attach_databases(
            pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.LOG
        )
    return job


@pytest.fixture(autouse=True)
def _reset_calls():
    calls.clear()


@pytest.fixture(params=["sync", "async"])
def orchestrator_cls(request):
    return {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}[
        request.param
    ]


def assert_store_error(err: BaseException, node_label: str | None, faults: Faults):
    """The run's error is a single error (not a group) naming the node and the table."""
    assert not isinstance(err, BaseExceptionGroup), f"{type(err).__name__}: {err}"
    message = str(err)
    if node_label is not None:
        assert repr(node_label) in message, message
    if faults.failed_paths:
        assert faults.failed_paths[0] in message, message


def stored_rows(store: Any, label: str = "dbl") -> int:
    """Rows a fresh job over ``store`` reads back for ``label`` without running."""
    records = build_job(store, ROWS).nodes[label].get_all_records()
    return 0 if records is None else records.num_rows


# ---------------------------------------------------------------------------
# Function node writes
# ---------------------------------------------------------------------------


class TestFunctionNodeWriteFailure:
    def test_result_table_failure_on_first_row_raises(self, orchestrator_cls):
        inner = InMemoryArrowDatabase()
        faults = Faults(write_marker=RESULT_TABLE)
        job = build_job(FaultyStore(inner, faults), ROWS)

        with pytest.raises(Exception) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, "dbl", faults)

    def test_result_table_failure_on_later_row_keeps_earlier_rows(
        self, orchestrator_cls
    ):
        inner = InMemoryArrowDatabase()
        faults = Faults(write_marker=RESULT_TABLE, succeed_first=1)
        job = build_job(FaultyStore(inner, faults), ROWS)

        with pytest.raises(Exception) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, "dbl", faults)
        assert stored_rows(inner) == 1

        # A re-run on a healthy store computes only the rows that are missing.
        calls.clear()
        rerun = build_job(inner, ROWS)
        rerun.run(orchestrator=orchestrator_cls())
        assert rerun.nodes["dbl"].as_table().num_rows == 3
        assert len(calls) == 2

    def test_pipeline_table_failure_raises(self, orchestrator_cls):
        inner = InMemoryArrowDatabase()
        faults = Faults(write_marker=PIPELINE_TABLE)
        job = build_job(FaultyStore(inner, faults), ROWS)

        with pytest.raises(Exception) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, "dbl", faults)

        # The result that was computed is stored; a healthy re-run reuses it.
        calls.clear()
        rerun = build_job(inner, ROWS)
        rerun.run(orchestrator=orchestrator_cls())
        assert rerun.nodes["dbl"].as_table().num_rows == 3
        assert len(calls) == 2

    def test_failure_on_new_row_keeps_previously_stored_rows(self, orchestrator_cls):
        inner = InMemoryArrowDatabase()
        build_job(inner, ROWS[:2]).run(orchestrator=orchestrator_cls())
        assert stored_rows(inner) == 2

        faults = Faults(write_marker=RESULT_TABLE)
        job = build_job(FaultyStore(inner, faults), ROWS)
        with pytest.raises(Exception) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, "dbl", faults)
        assert stored_rows(inner) == 2

    @pytest.mark.parametrize("error_policy", ["continue", "fail_fast"])
    def test_store_failure_raises_under_any_error_policy(
        self, orchestrator_cls, error_policy
    ):
        faults = Faults(write_marker=RESULT_TABLE)
        job = build_job(FaultyStore(InMemoryArrowDatabase(), faults), ROWS)

        with pytest.raises(Exception) as exc_info:
            job.run(orchestrator=orchestrator_cls(error_policy=error_policy))

        assert_store_error(exc_info.value, "dbl", faults)

    def test_flush_failure_raises(self, orchestrator_cls):
        faults = Faults(on_flush=True)
        job = build_job(FaultyStore(InMemoryArrowDatabase(), faults), ROWS)

        with pytest.raises(Exception) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, None, faults)

    def test_function_failure_is_still_skipped_under_continue(self, orchestrator_cls):
        """``error_policy="continue"`` keeps skipping rows whose function raised."""
        inner = InMemoryArrowDatabase()
        job = build_job(inner, ROWS, pod=double_or_fail)

        job.run(orchestrator=orchestrator_cls(error_policy="continue"))

        assert sorted(job.nodes["dbl"].as_table()["k"].to_pylist()) == ["r0", "r2"]


# ---------------------------------------------------------------------------
# Operator node writes (cache_mode=LOG)
# ---------------------------------------------------------------------------


class TestOperatorNodeWriteFailure:
    def test_operator_log_failure_raises_and_commits_upstream_rows(
        self, orchestrator_cls
    ):
        inner = InMemoryArrowDatabase()
        faults = Faults(write_marker=OPERATOR_TABLE)
        job = build_job(FaultyStore(inner, faults), ROWS, with_operator=True)

        with pytest.raises(Exception) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, "sel", faults)
        # The upstream function node wrote all its rows before the operator failed;
        # none of them may look dropped.
        assert stored_rows(inner) == 3


# ---------------------------------------------------------------------------
# The reported trigger: a logical-type output on SQLite (ITL-877 is the limit itself)
# ---------------------------------------------------------------------------


class TestSQLiteTrigger:
    def test_logical_type_output_on_sqlite_raises(self, orchestrator_cls, tmp_path):
        store = ConnectorArrowDatabase(SQLiteConnector(str(tmp_path / "store.db")))
        job = build_job(store, ROWS, pod=to_path)

        with pytest.raises(Exception) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert not isinstance(exc_info.value, BaseExceptionGroup)
        assert "'dbl'" in str(exc_info.value)
        assert "does not preserve Arrow metadata" in str(exc_info.value)
