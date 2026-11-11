"""A failed store write must fail ``PipelineJob.run()`` (ITL-878).

Before the fix, a node whose write to the store failed was skipped like a row whose
function had raised: ``run()`` returned normally and the node read back fewer rows (often
zero). These tests drive ``PipelineJob`` through its public interface on both orchestrators,
with a test-double store that fails chosen writes, and with the SQLite trigger from the
report (a logical-type output on ``SQLiteConnector``).
"""

from __future__ import annotations

import asyncio
import pathlib
from dataclasses import dataclass, field
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.channels import Channel
from orcapod.core.operators import SelectDataColumns
from orcapod.errors import StoreWriteError
from orcapod.databases import (
    ConnectorArrowDatabase,
    InMemoryArrowDatabase,
    SQLiteConnector,
)
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
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


def build_job(
    store: Any, rows: list[dict[str, Any]], *, pod=double, with_operator: bool = False
):
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


def assert_store_error(
    err: BaseException,
    faults: Faults,
    *,
    node_label: str | None,
    names_row: bool,
) -> None:
    """The run's error is a plain ``StoreWriteError`` naming node, row and table."""
    assert isinstance(err, StoreWriteError), f"{type(err).__name__}: {err}"
    message = str(err)
    if node_label is not None:
        assert err.node_label == node_label
        assert f"node {node_label!r}" in message, message
    if names_row:
        assert err.tag is not None and err.tag["k"] in {row["k"] for row in ROWS}
        assert f"for row {err.tag!r}" in message, message
    if faults.failed_paths:
        assert "/".join(err.record_path) == faults.failed_paths[0]
        assert faults.failed_paths[0] in message, message
    assert isinstance(err.__cause__, OSError)
    assert "rows that are missing" in message


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

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, faults, node_label="dbl", names_row=True)

    def test_result_table_failure_on_later_row_keeps_earlier_rows(
        self, orchestrator_cls
    ):
        inner = InMemoryArrowDatabase()
        faults = Faults(write_marker=RESULT_TABLE, succeed_first=1)
        job = build_job(FaultyStore(inner, faults), ROWS)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, faults, node_label="dbl", names_row=True)
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

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, faults, node_label="dbl", names_row=True)

        # The failing row's result was stored before its pipeline record failed, so a
        # healthy re-run reuses it instead of computing it again. (On the async
        # orchestrator a row still computing when the run stopped was never stored
        # and is computed again, so the total call count is not fixed.)
        failed_tag = exc_info.value.tag
        assert failed_tag is not None
        failed_x = next(row["x"] for row in ROWS if row["k"] == failed_tag["k"])
        calls.clear()
        rerun = build_job(inner, ROWS)
        rerun.run(orchestrator=orchestrator_cls())
        assert rerun.nodes["dbl"].as_table().num_rows == 3
        assert failed_x not in calls

    def test_failure_on_new_row_keeps_previously_stored_rows(self, orchestrator_cls):
        inner = InMemoryArrowDatabase()
        build_job(inner, ROWS[:2]).run(orchestrator=orchestrator_cls())
        assert stored_rows(inner) == 2

        faults = Faults(write_marker=RESULT_TABLE)
        job = build_job(FaultyStore(inner, faults), ROWS)
        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, faults, node_label="dbl", names_row=True)
        assert stored_rows(inner) == 2

    @pytest.mark.parametrize("error_policy", ["continue", "fail_fast"])
    def test_store_failure_raises_under_any_error_policy(
        self, orchestrator_cls, error_policy
    ):
        faults = Faults(write_marker=RESULT_TABLE)
        job = build_job(FaultyStore(InMemoryArrowDatabase(), faults), ROWS)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls(error_policy=error_policy))

        assert_store_error(exc_info.value, faults, node_label="dbl", names_row=True)

    def test_flush_failure_raises(self, orchestrator_cls):
        faults = Faults(on_flush=True)
        job = build_job(FaultyStore(InMemoryArrowDatabase(), faults), ROWS)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, faults, node_label=None, names_row=False)

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

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, faults, node_label="sel", names_row=False)
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

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert exc_info.value.node_label == "dbl"
        assert "node 'dbl'" in str(exc_info.value)
        assert "does not preserve Arrow metadata" in str(exc_info.value)


# ---------------------------------------------------------------------------
# A failing hook or observer callback never replaces a store error
# ---------------------------------------------------------------------------


def failing_hook(payload: Any) -> None:
    raise ValueError("post-run hook failed")


def make_hooked_triple(store: Any):
    """A cached pod with a fail-loud post-run hook, writing its results to ``store``."""

    @function_pod(
        output_keys="tripled", pod_cache_database=store, post_run_hooks=[failing_hook]
    )
    def triple(x: int) -> int:
        return x * 3

    return triple


def make_outer(inner: Any):
    """A function pod whose function runs ``inner`` through its stream."""

    @function_pod(output_keys="double")
    def via_inner(x: int) -> int:
        stream = DictSource(data=[{"i": "i", "x": x}], tag_columns=["i"])
        return inner.pod(stream).as_table()["tripled"][0].as_py() - x

    return via_inner


def assert_hook_failure_noted(err: BaseException) -> None:
    assert isinstance(err, StoreWriteError), f"{type(err).__name__}: {err}"
    assert isinstance(err.__cause__, OSError)
    notes = getattr(err, "__notes__", [])
    assert any("post-run hook failed" in note for note in notes), notes


class CrashingObserver(NoOpObserver):
    """An observer whose crash and end callbacks raise once a row has crashed."""

    def __init__(self) -> None:
        self.crashed = False

    def on_data_crash(self, node_label, tag, data, error) -> None:
        self.crashed = True
        raise RuntimeError("observer failed in on_data_crash")

    def on_node_end(self, node_label, node_hash) -> None:
        if self.crashed:
            raise RuntimeError("observer failed in on_node_end")

    def on_run_end(self, run_id) -> None:
        if self.crashed:
            raise RuntimeError("observer failed in on_run_end")


class TestCallbackFailureKeepsStoreError:
    def test_failing_hook_keeps_store_error_standalone(self):
        faults = Faults(write_marker="")
        triple = make_hooked_triple(FaultyStore(InMemoryArrowDatabase(), faults))
        stream = DictSource(data=ROWS, tag_columns=["k"])

        with pytest.raises(StoreWriteError) as exc_info:
            triple.pod(stream).as_table()

        assert_hook_failure_noted(exc_info.value)

    def test_failing_hook_keeps_store_error_standalone_async(self):
        faults = Faults(write_marker="")
        triple = make_hooked_triple(FaultyStore(InMemoryArrowDatabase(), faults))
        stream = DictSource(data=ROWS, tag_columns=["k"])
        input_ch: Channel[Any] = Channel(buffer_size=16)
        output_ch: Channel[Any] = Channel(buffer_size=16)

        async def feed() -> None:
            for tag, data in stream.iter_data():
                await input_ch.writer.send((tag, data))
            await input_ch.writer.close()

        async def run() -> None:
            await asyncio.gather(
                feed(), triple.pod.async_execute([input_ch.reader], output_ch.writer)
            )

        with pytest.raises(ExceptionGroup) as exc_info:
            asyncio.run(run())

        leaves = exc_info.value.exceptions
        assert all(isinstance(leaf, StoreWriteError) for leaf in leaves), leaves
        for leaf in leaves:
            assert_hook_failure_noted(leaf)

    @pytest.mark.parametrize("error_policy", ["continue", "fail_fast"])
    def test_failing_hook_in_nested_pod_fails_the_run(
        self, orchestrator_cls, error_policy
    ):
        """A node function running a cached pod whose write fails cannot skip the row."""
        faults = Faults(write_marker="")
        inner = make_hooked_triple(FaultyStore(InMemoryArrowDatabase(), faults))
        job = build_job(InMemoryArrowDatabase(), ROWS, pod=make_outer(inner))

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls(error_policy=error_policy))

        assert_hook_failure_noted(exc_info.value)

    def test_failing_observer_keeps_store_error(self, orchestrator_cls):
        faults = Faults(write_marker=RESULT_TABLE)
        job = build_job(FaultyStore(InMemoryArrowDatabase(), faults), ROWS)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls(), observer=CrashingObserver())

        err = exc_info.value
        assert_store_error(err, faults, node_label="dbl", names_row=True)
        notes = getattr(err, "__notes__", [])
        assert any("observer failed in on_data_crash" in note for note in notes), notes
        assert any("observer failed in on_run_end" in note for note in notes), notes


# ---------------------------------------------------------------------------
# Concurrent failures in the async orchestrator
# ---------------------------------------------------------------------------

# ``async def`` functions run on the event loop rather than in worker threads, so
# the order in which concurrent node tasks fail is the same on every run.


@function_pod(output_keys="double")
async def double_async(x: int) -> int:
    return x * 2


@function_pod(output_keys="tripled")
async def triple_async(x: int) -> int:
    return x * 3


@function_pod(output_keys="tripled")
async def fail_async(x: int) -> int:
    raise RuntimeError("function failed")


class FailingCrashObserver(NoOpObserver):
    """An observer whose ``on_data_crash`` raises for one node, failing that node."""

    def on_data_crash(self, node_label, tag, data, error) -> None:
        if node_label == "other":
            raise KeyError("observer failed for node 'other'")


def build_siblings(store: Any, other_pod: Any):
    """Two function nodes reading the same source: ``dbl`` and ``other``."""
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        source = DictSource(data=ROWS, tag_columns=["k"], source_id="src")
        double_async.pod(source, label="dbl")
        other_pod.pod(source, label="other")
    return job


class TestConcurrentFailures:
    def test_store_error_and_other_failure_raise_one_store_error(self):
        faults = Faults(write_marker=RESULT_TABLE)
        job = build_siblings(FaultyStore(InMemoryArrowDatabase(), faults), fail_async)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(
                orchestrator=AsyncPipelineOrchestrator(),
                observer=FailingCrashObserver(),
            )

        err = exc_info.value
        assert_store_error(err, faults, node_label="dbl", names_row=True)
        notes = getattr(err, "__notes__", [])
        assert any("observer failed for node 'other'" in note for note in notes), notes
        # The whole group stays reachable for diagnosis.
        assert isinstance(err.__context__, ExceptionGroup)

    def test_several_store_errors_raise_the_first_in_node_order(
        self, orchestrator_cls, caplog
    ):
        faults = Faults(write_marker=RESULT_TABLE)
        job = build_siblings(FaultyStore(InMemoryArrowDatabase(), faults), triple_async)

        with caplog.at_level("WARNING"):
            with pytest.raises(StoreWriteError) as exc_info:
                job.run(orchestrator=orchestrator_cls())

        # The sync orchestrator meets ``dbl`` first; the async one raises the same.
        assert_store_error(exc_info.value, faults, node_label="dbl", names_row=True)
        if orchestrator_cls is AsyncPipelineOrchestrator:
            others = [
                record
                for record in caplog.records
                if record.levelname == "WARNING"
                and "Another store write failed" in record.getMessage()
            ]
            assert any("node 'other'" in r.getMessage() for r in others), others
