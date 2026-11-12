"""Rows hashed under another Arrow hash label are ignored, never used or changed (ITL-910).

v0.3 hashes Arrow data with starfix hash format 2 under the label ``arrow_v0.2``. Rows a
v0.2 store holds were keyed under ``arrow_v0.1`` (format 1). Table paths do not move for a
value-only change, so v0.3 writes its ``arrow_v0.2`` rows into the same tables, next to the
old ones. It must then use rows of the current label only: key lookups cannot match an old
row (the labelled bytes differ), and every read of a whole table drops the rows of another
label. The old rows stay on disk unchanged, and ``stored_hash_labels`` lists which labels a
table holds, so a later migration can convert them.

Rows "written by v0.2" are produced here by running the same pipeline while the Arrow hasher
labels its hashes ``arrow_v0.1`` and stamps the format-1 prefix ``00 00 01``, as starfix 0.4
did. The real v0.2 store written by Orcapod v0.2.0 is covered in
``tests/test_arrays/test_array_v02_store.py``.
"""

from __future__ import annotations

import contextlib
import logging
import pathlib
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import SelectDataColumns
from orcapod.core.data_function import CachedDataFunction
from orcapod.core.datagrams import Data
from orcapod.core.sources.cached_source import CachedSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import SideEffectLogHashLabelError, StoreError
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.hashing.stored_hash_labels import stored_hash_labels
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.side_effects import SideEffectPod, sink_pod
from orcapod.sources import ArrowTableSource, DictSource
from orcapod.types import CacheMode, ContentHash

Orchestrator = SyncPipelineOrchestrator | AsyncPipelineOrchestrator
Store = InMemoryArrowDatabase | DeltaTableDatabase

JOB_NAME = "itl910"
ROWS = [{"k": "a", "x": 1}, {"k": "b", "x": 2}]

calls: list[int] = []
delivered: list[int] = []


@function_pod(output_keys="double")
def double(x: int) -> int:
    calls.append(x)
    return 2 * x


def _deliver(double: int, ctx: Any) -> None:
    delivered.append(double)


@pytest.fixture(autouse=True)
def _reset() -> None:
    calls.clear()
    delivered.clear()


@contextlib.contextmanager
def writing_as_v02() -> Iterator[None]:
    """Make the Arrow hasher label its hashes ``arrow_v0.1`` with the format-1 prefix."""
    # Patching the hasher is the only way to reproduce a v0.2 writer inside this process.
    original = StarfixArrowHasher.hash_table

    def v02_hash_table(self: StarfixArrowHasher, table: Any) -> ContentHash:
        digest = original(self, table).digest
        return ContentHash(method="arrow_v0.1", digest=b"\x00\x00\x01" + digest[3:])

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(StarfixArrowHasher, "hash_table", v02_hash_table)
        patch.setattr(
            StarfixArrowHasher, "hasher_id", property(lambda self: "arrow_v0.1")
        )
        yield


def build_job(
    store: Store,
    *,
    with_operator: bool = False,
    with_sink: bool = False,
    cache_mode: CacheMode = CacheMode.LOG,
) -> PipelineJob:
    """A job with function node ``dbl`` and optionally a stored operator ``sel`` and a sink."""
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        source = DictSource(data=ROWS, tag_columns=["k"], source_id="src")
        out = double.pod(source, label="dbl")
        if with_operator:
            SelectDataColumns(["double"])(out, label="sel")
        if with_sink:
            sink = sink_pod(_deliver)
            assert isinstance(sink, SideEffectPod)
            sink.process(out, label="sink")
    if with_operator:
        # OperatorNode.attach_databases is the public way to turn on LOG / REPLAY mode.
        job.nodes["sel"].attach_databases(
            pipeline_database=store.at(JOB_NAME), cache_mode=cache_mode
        )
    return job


# Root directory of each Delta store handed out by the ``store`` fixture.
_delta_roots: dict[int, pathlib.Path] = {}


def reopened(store: Store) -> Store:
    """A database object that sees everything written to ``store`` so far.

    A ``DeltaTableDatabase`` keeps the table version it read first, so rows written
    later through another view of the same directory need a new object to be seen.
    """
    if isinstance(store, DeltaTableDatabase):
        return DeltaTableDatabase(_delta_roots[id(store)])
    return store


def labels_in(store: Store, path: tuple[str, ...]) -> dict[str | None, int]:
    return stored_hash_labels(reopened(store), path)


def snapshot(store: Store) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    """Every table of ``store`` with its rows, record ids included."""
    store = reopened(store)
    tables: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for path in sorted(store.list_sources()):
        records = store.get_all_records(path, record_id_column="__rid")
        tables[path] = [] if records is None else records.to_pylist()
    return tables


def delta_files(root: pathlib.Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()
    }


@pytest.fixture(params=["in_memory", "delta"])
def store(request: pytest.FixtureRequest, tmp_path: pathlib.Path) -> Store:
    if request.param == "in_memory":
        return InMemoryArrowDatabase()
    database = DeltaTableDatabase(tmp_path / "store")
    _delta_roots[id(database)] = tmp_path / "store"
    return database


@pytest.fixture(params=["sync", "async"])
def orchestrator(request: pytest.FixtureRequest) -> Orchestrator:
    return {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}[
        request.param
    ]()


def write_v02_store(
    store: Store, *, with_operator: bool = False, with_sink: bool = False
) -> None:
    with writing_as_v02():
        _ = build_job(store, with_operator=with_operator, with_sink=with_sink).run()
    calls.clear()
    delivered.clear()


def write_both_labels(store: Store, *, with_operator: bool = False) -> None:
    """A store written by v0.2 and then run by this version: both labels in every table."""
    write_v02_store(store, with_operator=with_operator)
    _ = build_job(store, with_operator=with_operator).run()
    calls.clear()
    delivered.clear()


def rows_of(table: pa.Table | None, column: str = "double") -> list[Any]:
    return [] if table is None else sorted(table[column].to_pylist())


def table_path(store: Store, kind: str) -> tuple[str, ...]:
    """The one stored table of ``kind``: pipeline, result, operator or side-effect log."""
    matches = {
        "pipeline": lambda p: p[-1] == "pdb_v1",
        "result": lambda p: p[-1] == "rdb_v1",
        "operator": lambda p: "SelectDataColumns" in p,
        "log": lambda p: p[-1] == "tdb_v1",
    }[kind]
    (path,) = [p for p in reopened(store).list_sources() if matches(p)]
    return path


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


class TestRun:
    def test_v02_rows_are_ignored_and_inputs_recomputed(
        self, store: Store, orchestrator: Orchestrator
    ):
        write_v02_store(store, with_operator=True)

        job = build_job(store, with_operator=True)
        _ = job.run(orchestrator=orchestrator)

        assert sorted(calls) == [1, 2]
        assert rows_of(job.nodes["dbl"].as_table()) == [2, 4]
        assert rows_of(job.nodes["sel"].as_table()) == [2, 4]

    def test_v02_rows_stay_in_place_unchanged(
        self, store: Store, orchestrator: Orchestrator
    ):
        write_v02_store(store, with_operator=True)
        before = snapshot(store)

        _ = build_job(store, with_operator=True).run(orchestrator=orchestrator)

        after = snapshot(store)
        assert set(before) <= set(after)
        for path, old_rows in before.items():
            assert len(old_rows) == 2
            assert len(after[path]) == 4, path
            for row in old_rows:
                assert row in after[path], path

    def test_delta_files_written_by_v02_are_byte_identical(
        self, tmp_path: pathlib.Path
    ):
        root = tmp_path / "store"
        write_v02_store(DeltaTableDatabase(root), with_operator=True)
        before = delta_files(root)

        build_job(DeltaTableDatabase(root), with_operator=True).run()

        after = delta_files(root)
        for name, content in before.items():
            assert after.get(name) == content, name
        assert len(after) > len(before)

    def test_rows_of_this_version_are_reused_next_to_v02_rows(
        self, store: Store, orchestrator: Orchestrator
    ):
        write_both_labels(store, with_operator=True)
        before = snapshot(store)

        job = build_job(store, with_operator=True)
        _ = job.run(orchestrator=orchestrator)

        assert calls == []
        assert rows_of(job.nodes["dbl"].as_table()) == [2, 4]
        assert snapshot(store) == before

    def test_fresh_store_runs(self, store: Store, orchestrator: Orchestrator):
        build_job(store, with_operator=True, with_sink=True).run(
            orchestrator=orchestrator
        )
        assert sorted(calls) == [1, 2]
        assert sorted(delivered) == [2, 4]

    def test_store_written_by_this_version_is_reused(
        self, store: Store, orchestrator: Orchestrator
    ):
        build_job(store, with_operator=True, with_sink=True).run(
            orchestrator=orchestrator
        )
        calls.clear()
        delivered.clear()

        job = build_job(store, with_operator=True, with_sink=True)
        _ = job.run(orchestrator=orchestrator)

        assert calls == []
        assert delivered == []
        assert sorted(job.nodes["dbl"].as_table()["double"].to_pylist()) == [2, 4]


# ---------------------------------------------------------------------------
# Whole-table reads return rows of the current label only
# ---------------------------------------------------------------------------


class TestFunctionNodeReads:
    def test_v02_rows_alone_are_not_returned(self, store: Store):
        """Before this version has run, a node whose table holds only v0.2 rows is empty."""
        write_v02_store(store)
        node = build_job(store).nodes["dbl"]
        assert node.as_table().num_rows == 0
        assert node.get_all_records() is None
        assert list(node.iter_data()) == []
        assert calls == []

    def test_table_with_both_labels_gives_one_row_per_input(self, store: Store):
        write_both_labels(store)
        node = build_job(store).nodes["dbl"]
        assert rows_of(node.as_table()) == [2, 4]
        assert rows_of(node.get_all_records()) == [2, 4]
        assert sorted(d.as_dict()["double"] for _, d in node.iter_data()) == [2, 4]

    def test_run_yields_one_row_per_input(
        self, store: Store, orchestrator: Orchestrator
    ):
        """Phase 1 of a run (stored results first) does not replay the v0.2 rows."""
        write_both_labels(store)
        job = build_job(store)
        _ = job.run(orchestrator=orchestrator)
        assert rows_of(job.nodes["dbl"].as_table()) == [2, 4]
        assert calls == []

    def test_derived_source(self, store: Store):
        write_both_labels(store)
        source = build_job(store).nodes["dbl"].as_source()
        assert rows_of(source.as_table()) == [2, 4]

    def test_loaded_job(self, store: Store, tmp_path: pathlib.Path):
        """A job file saved by v0.2 and loaded by this version reads current rows only."""
        saved = tmp_path / "job.json"
        with writing_as_v02():
            completed = build_job(store, with_operator=True).run()
            completed.save(str(saved))
        calls.clear()

        loaded = PipelineJob.load(str(saved), store=store)
        assert loaded.nodes["dbl"].get_all_records() is None
        assert loaded.nodes["sel"].get_all_records() is None

        _ = build_job(store, with_operator=True).run()
        loaded = PipelineJob.load(str(saved), store=store)
        assert rows_of(loaded.nodes["dbl"].get_all_records()) == [2, 4]
        assert rows_of(loaded.nodes["sel"].get_all_records()) == [2, 4]


class TestResultCacheReads:
    def test_cached_function_outputs(self, store: Store):
        """``get_all_cached_outputs`` reads the whole result table: current label only."""
        inputs = [Data({"x": 1}), Data({"x": 2})]
        with writing_as_v02():
            old = CachedDataFunction(double.pod.data_function, result_database=store)
            for data in [Data({"x": 1}), Data({"x": 2})]:
                _ = old.call(data)
            assert rows_of(old.get_all_cached_outputs()) == [2, 4]
        calls.clear()

        cached = CachedDataFunction(double.pod.data_function, result_database=store)
        assert cached.get_all_cached_outputs() is None

        for data in inputs:
            _ = cached.call(data)
        assert sorted(calls) == [1, 2]
        assert rows_of(cached.get_all_cached_outputs()) == [2, 4]
        (path,) = reopened(store).list_sources()
        assert labels_in(store, path) == {"arrow_v0.1": 2, "arrow_v0.2": 2}


class TestOperatorNodeReads:
    def test_v02_rows_alone_are_not_returned(self, store: Store):
        write_v02_store(store, with_operator=True)
        node = build_job(store, with_operator=True).nodes["sel"]
        assert node.get_all_records() is None

    def test_logged_table_with_both_labels(self, store: Store):
        write_both_labels(store, with_operator=True)
        node = build_job(store, with_operator=True).nodes["sel"]
        assert rows_of(node.get_all_records()) == [2, 4]
        assert rows_of(node.as_source().as_table()) == [2, 4]

    def test_replay_reads_current_rows_only(
        self, store: Store, orchestrator: Orchestrator
    ):
        write_both_labels(store, with_operator=True)
        job = build_job(store, with_operator=True, cache_mode=CacheMode.REPLAY)
        node = job.nodes["sel"]
        assert rows_of(node.as_table()) == [2, 4]
        assert sorted(d.as_dict()["double"] for _, d in node.iter_data()) == [2, 4]

        _ = job.run(orchestrator=orchestrator)
        assert rows_of(job.nodes["sel"].as_table()) == [2, 4]

    def test_replay_of_v02_rows_alone_is_empty(self, store: Store):
        write_v02_store(store, with_operator=True)
        job = build_job(store, with_operator=True, cache_mode=CacheMode.REPLAY)
        assert job.nodes["sel"].as_table().num_rows == 0


# ---------------------------------------------------------------------------
# Cached sources
# ---------------------------------------------------------------------------


def _cached_source(store: Store) -> CachedSource:
    table = pa.table(
        {
            "k": pa.array(["a", "b"], pa.large_string()),
            "x": pa.array([1, 2], pa.int64()),
        }
    )
    return CachedSource(
        ArrowTableSource(table, tag_columns=["k"], source_id="fixed"),
        cache_database=store,
    )


class TestCachedSource:
    def test_rows_cached_by_v02_are_ignored_and_kept(self, store: Store):
        with writing_as_v02():
            _ = _cached_source(store).as_table()
        before = snapshot(store)

        source = _cached_source(store)
        assert rows_of(source.as_table(), "x") == [1, 2]
        assert rows_of(source.get_all_records(), "x") == [1, 2]

        after = snapshot(store)
        (path,) = after
        assert len(after[path]) == 4
        for row in before[path]:
            assert row in after[path]
        assert labels_in(store, path) == {"arrow_v0.1": 2, "arrow_v0.2": 2}

    def test_cache_written_by_this_version_is_read(self, store: Store):
        _ = _cached_source(store).as_table()
        assert _cached_source(store).as_table().num_rows == 2


# ---------------------------------------------------------------------------
# Side effects
# ---------------------------------------------------------------------------


def _sink_job(store: Store) -> PipelineJob:
    """A job whose only stored node is a sink reading the source directly."""
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        source = DictSource(
            data=[{"k": "a", "double": 2}, {"k": "b", "double": 4}],
            tag_columns=["k"],
            source_id="src",
        )
        sink = sink_pod(_deliver)
        assert isinstance(sink, SideEffectPod)
        sink.process(source, label="sink")
    return job


class TestSideEffects:
    """A side effect is an external action, so an old log fails instead of delivering again."""

    def test_completions_logged_by_v02_raise_and_nothing_is_delivered(
        self, store: Store, orchestrator: Orchestrator
    ):
        with writing_as_v02():
            _ = _sink_job(store).run()
        assert sorted(delivered) == [2, 4]
        delivered.clear()
        before = snapshot(store)

        with pytest.raises(SideEffectLogHashLabelError) as info:
            _ = _sink_job(store).run(orchestrator=orchestrator)

        assert delivered == []
        assert snapshot(store) == before
        error = info.value
        assert isinstance(error, StoreError)
        assert error.found_label == "arrow_v0.1"
        assert error.current_label == "arrow_v0.2"
        assert error.node_label == "sink"
        assert error.record_path[-1] == "tdb_v1"
        message = str(error)
        assert "/".join(error.record_path) in message
        assert "'arrow_v0.1'" in message
        assert "'arrow_v0.2'" in message
        assert "delivered" in message
        assert "migrat" in message

    def test_delta_log_files_are_untouched(self, tmp_path: pathlib.Path):
        root = tmp_path / "store"
        with writing_as_v02():
            _ = _sink_job(DeltaTableDatabase(root)).run()
        delivered.clear()
        before = delta_files(root)

        with pytest.raises(SideEffectLogHashLabelError):
            _ = _sink_job(DeltaTableDatabase(root)).run()

        assert delivered == []
        assert delta_files(root) == before

    def test_function_node_upstream_of_a_refused_sink_is_not_refused(
        self, store: Store
    ):
        """Only the side-effect node fails; function and operator tables are filtered."""
        write_v02_store(store, with_sink=True)

        job = build_job(store, with_sink=True)
        with pytest.raises(SideEffectLogHashLabelError):
            _ = job.run()

        assert delivered == []
        assert rows_of(build_job(store).nodes["dbl"].as_table()) == [2, 4]

    def test_completions_logged_by_this_version_are_not_delivered_again(
        self, store: Store, orchestrator: Orchestrator
    ):
        _ = _sink_job(store).run(orchestrator=orchestrator)
        delivered.clear()
        _ = _sink_job(store).run(orchestrator=orchestrator)
        assert delivered == []


# ---------------------------------------------------------------------------
# Old rows stay distinguishable: listing the labels a table holds
# ---------------------------------------------------------------------------


class TestStoredHashLabels:
    @pytest.mark.parametrize("kind", ["pipeline", "result", "operator"])
    def test_lists_both_labels_after_a_run_on_a_v02_store(
        self, store: Store, kind: str
    ):
        write_v02_store(store, with_operator=True)
        path = table_path(store, kind)
        assert labels_in(store, path) == {"arrow_v0.1": 2}

        _ = build_job(store, with_operator=True).run()
        assert labels_in(store, path) == {"arrow_v0.1": 2, "arrow_v0.2": 2}

    def test_lists_the_label_of_a_v02_side_effect_log(self, store: Store):
        write_v02_store(store, with_sink=True)
        assert labels_in(store, table_path(store, "log")) == {"arrow_v0.1": 2}

    def test_fresh_table_holds_the_current_label_only(self, store: Store):
        _ = build_job(store).run()
        assert labels_in(store, table_path(store, "pipeline")) == {"arrow_v0.2": 2}

    def test_missing_table_holds_nothing(self, store: Store):
        assert labels_in(store, ("no", "such", "table")) == {}


# ---------------------------------------------------------------------------
# Skipped rows are reported: a warning with the total, once per reader and table
# ---------------------------------------------------------------------------


def skipped_row_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING
        and record.name == "orcapod.hashing.stored_hash_labels"
    ]


class TestSkippedRowsAreReported:
    def test_function_node_read(self, store: Store, caplog: pytest.LogCaptureFixture):
        write_v02_store(store)
        node = build_job(store).nodes["dbl"]
        with caplog.at_level(logging.WARNING):
            _ = node.as_table()
            _ = node.get_all_records()

        (message,) = skipped_row_warnings(caplog)
        assert "2 rows" in message
        assert "/".join(table_path(store, "pipeline")) in message
        assert "another hash label" in message
        assert "left in place" in message
        assert "migrated" in message

    def test_run_reports_each_table_once(
        self, store: Store, orchestrator: Orchestrator, caplog: pytest.LogCaptureFixture
    ):
        write_v02_store(store, with_operator=True)
        job = build_job(store, with_operator=True, cache_mode=CacheMode.REPLAY)
        with caplog.at_level(logging.WARNING):
            _ = job.run(orchestrator=orchestrator)

        messages = skipped_row_warnings(caplog)
        assert len(messages) == len(set(messages))
        tables = {
            "/".join(table_path(store, kind)) for kind in ("pipeline", "operator")
        }
        for table in tables:
            assert sum(table in message for message in messages) == 1, table

    def test_operator_node_read(self, store: Store, caplog: pytest.LogCaptureFixture):
        write_v02_store(store, with_operator=True)
        node = build_job(store, with_operator=True).nodes["sel"]
        with caplog.at_level(logging.WARNING):
            _ = node.get_all_records()
            _ = node.get_all_records()

        (message,) = skipped_row_warnings(caplog)
        assert "2 rows" in message
        assert "/".join(table_path(store, "operator")) in message

    def test_cached_source_read(self, store: Store, caplog: pytest.LogCaptureFixture):
        with writing_as_v02():
            _ = _cached_source(store).as_table()
        source = _cached_source(store)
        with caplog.at_level(logging.WARNING):
            _ = source.as_table()
            _ = source.get_all_records()

        (message,) = skipped_row_warnings(caplog)
        assert "2 rows" in message

    def test_cached_function_outputs(
        self, store: Store, caplog: pytest.LogCaptureFixture
    ):
        with writing_as_v02():
            old = CachedDataFunction(double.pod.data_function, result_database=store)
            _ = old.call(Data({"x": 1}))
        cached = CachedDataFunction(double.pod.data_function, result_database=store)
        with caplog.at_level(logging.WARNING):
            assert cached.get_all_cached_outputs() is None
            assert cached.get_all_cached_outputs() is None

        (message,) = skipped_row_warnings(caplog)
        assert "1 row " in message

    def test_nothing_is_reported_for_rows_of_this_version(
        self, store: Store, orchestrator: Orchestrator, caplog: pytest.LogCaptureFixture
    ):
        with caplog.at_level(logging.WARNING):
            _ = build_job(store, with_operator=True).run(orchestrator=orchestrator)
            job = build_job(store, with_operator=True)
            _ = job.run(orchestrator=orchestrator)
            _ = job.nodes["dbl"].get_all_records()
            _ = job.nodes["sel"].get_all_records()

        assert skipped_row_warnings(caplog) == []
