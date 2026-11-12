"""A store holding rows hashed under another Arrow hash label fails explicitly (ITL-910).

v0.3 hashes Arrow data with starfix hash format 2 under the label ``arrow_v0.2``. Rows a
v0.2 store holds were keyed under ``arrow_v0.1`` (format 1), so v0.3 can never match them
with current inputs. Rather than recomputing next to them (which mixes old and new rows in
one table) or ignoring them (which makes stored results silently look dropped), every read
and every run of a node whose table holds such rows raises ``StoreHashLabelError`` before
anything is computed or written.

Rows "written by v0.2" are produced here by running the same pipeline while the Arrow hasher
labels its hashes ``arrow_v0.1`` and stamps the format-1 prefix ``00 00 01``, as starfix 0.4
did. The real v0.2 store written by Orcapod v0.2.0 is covered in
``tests/test_arrays/test_array_v02_store.py``.
"""

from __future__ import annotations

import contextlib
import pathlib
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import SelectDataColumns
from orcapod.core.sources.cached_source import CachedSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import StoreError, StoreHashLabelError
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.side_effects import SideEffectPod, sink_pod
from orcapod.sources import ArrowTableSource, DictSource
from orcapod.types import CacheMode, ContentHash, NodeConfig

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
) -> PipelineJob:
    """A job with function node ``dbl`` and optionally a LOG operator ``sel`` and a sink."""
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
        # OperatorNode.attach_databases is the public way to turn on LOG mode.
        job.nodes["sel"].attach_databases(
            pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.LOG
        )
    return job


def snapshot(store: Store) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    """Every table of ``store`` with its rows, record ids included."""
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
    return DeltaTableDatabase(tmp_path / "store")


@pytest.fixture(params=["sync", "async"])
def orchestrator(request: pytest.FixtureRequest) -> Orchestrator:
    return {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}[
        request.param
    ]()


def write_v02_store(store: Store, **kwargs: bool) -> None:
    with writing_as_v02():
        _ = build_job(store, **kwargs).run()
    calls.clear()
    delivered.clear()


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


class TestRun:
    def test_run_raises_before_computing_or_writing(
        self, store: Store, orchestrator: Orchestrator
    ):
        write_v02_store(store, with_operator=True, with_sink=True)
        before = snapshot(store)

        job = build_job(store, with_operator=True, with_sink=True)
        with pytest.raises(StoreHashLabelError) as info:
            _ = job.run(orchestrator=orchestrator)

        assert calls == []
        assert delivered == []
        assert snapshot(store) == before
        assert isinstance(info.value, StoreError)
        assert info.value.found_label == "arrow_v0.1"
        assert info.value.current_label == "arrow_v0.2"

    def test_delta_store_files_are_untouched(self, tmp_path: pathlib.Path):
        root = tmp_path / "store"
        write_v02_store(DeltaTableDatabase(root), with_operator=True, with_sink=True)
        before = delta_files(root)

        with pytest.raises(StoreHashLabelError):
            build_job(
                DeltaTableDatabase(root), with_operator=True, with_sink=True
            ).run()

        assert delta_files(root) == before
        assert calls == []

    def test_error_names_table_labels_and_remedy(self, store: Store):
        write_v02_store(store)
        with pytest.raises(StoreHashLabelError) as info:
            _ = build_job(store).run()
        message = str(info.value)
        assert JOB_NAME in "/".join(info.value.record_path)
        assert "/".join(info.value.record_path) in message
        assert "'arrow_v0.1'" in message
        assert "'arrow_v0.2'" in message
        assert "fresh store" in message
        assert "migration" in message

    def test_ignore_schema_does_not_disable_the_check(self, store: Store):
        write_v02_store(store)
        job = build_job(store)
        job.nodes["dbl"].node_config = NodeConfig(ignore_schema=("v0",))
        with pytest.raises(StoreHashLabelError):
            _ = job.run()
        assert calls == []

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
# Reads
# ---------------------------------------------------------------------------


class TestRead:
    def test_reading_a_function_node_raises(self, store: Store):
        write_v02_store(store)
        node = build_job(store).nodes["dbl"]
        with pytest.raises(StoreHashLabelError):
            _ = node.as_table()
        with pytest.raises(StoreHashLabelError):
            _ = node.get_all_records()
        assert calls == []

    def test_reading_a_logged_operator_node_raises(self, store: Store):
        write_v02_store(store, with_operator=True)
        node = build_job(store, with_operator=True).nodes["sel"]
        with pytest.raises(StoreHashLabelError) as info:
            _ = node.get_all_records()
        assert info.value.found_label == "arrow_v0.1"

    def test_reading_a_loaded_job_raises(self, store: Store, tmp_path: pathlib.Path):
        """A job saved and stored by v0.2, loaded by this version, refuses its rows."""
        saved = tmp_path / "job.json"
        with writing_as_v02():
            job = build_job(store)
            completed = job.run()
            completed.save(str(saved))
        loaded = PipelineJob.load(str(saved), store=store)
        with pytest.raises(StoreHashLabelError) as info:
            loaded.nodes["dbl"].get_all_records()
        assert info.value.node_label == "dbl"

    def test_mixed_table_raises(self, store: Store):
        """A table holding rows under both labels (built by hand) is refused."""
        old_store = InMemoryArrowDatabase()
        write_v02_store(old_store)
        build_job(store).run()  # this version's rows
        calls.clear()

        # Copy the old rows into this store's pipeline table, next to the new ones.
        (path,) = [
            p
            for p in old_store.list_sources()
            if "_result" not in p and p[-1] == "pdb_v1"
        ]
        old_rows = old_store.get_all_records(
            path, record_id_column="__pipeline_entry_id"
        )
        assert old_rows is not None
        store.add_records(
            path, old_rows, record_id_column="__pipeline_entry_id", flush=True
        )

        node = build_job(store).nodes["dbl"]
        with pytest.raises(StoreHashLabelError):
            _ = node.as_table()
        with pytest.raises(StoreHashLabelError):
            _ = build_job(store).run()
        assert calls == []


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
    def test_cache_written_by_v02_raises(self, store: Store):
        with writing_as_v02():
            _ = _cached_source(store).as_table()
        before = snapshot(store)
        with pytest.raises(StoreHashLabelError) as info:
            _ = _cached_source(store).as_table()
        assert info.value.found_label == "arrow_v0.1"
        assert snapshot(store) == before

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
    def test_completions_logged_by_v02_raise_instead_of_delivering_again(
        self, store: Store, orchestrator: Orchestrator
    ):
        with writing_as_v02():
            _ = _sink_job(store).run()
        assert sorted(delivered) == [2, 4]
        delivered.clear()
        before = snapshot(store)

        with pytest.raises(StoreHashLabelError) as info:
            _ = _sink_job(store).run(orchestrator=orchestrator)

        assert delivered == []
        assert snapshot(store) == before
        assert info.value.found_label == "arrow_v0.1"

    def test_completions_logged_by_this_version_are_not_delivered_again(
        self, store: Store, orchestrator: Orchestrator
    ):
        _ = _sink_job(store).run(orchestrator=orchestrator)
        delivered.clear()
        _ = _sink_job(store).run(orchestrator=orchestrator)
        assert delivered == []


# ---------------------------------------------------------------------------
# Whole jobs are checked before their first node runs
# ---------------------------------------------------------------------------

early_calls: list[int] = []


@function_pod(output_keys="tripled")
def triple(x: int) -> int:
    early_calls.append(x)
    return 3 * x


def _two_node_job(store: Store, *, with_early: bool) -> PipelineJob:
    """``early`` (fresh tables, recorded first) and ``dbl`` (v0.2 rows in the test)."""
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        if with_early:
            other = DictSource(
                data=[{"j": "z", "x": 9}], tag_columns=["j"], source_id="other"
            )
            triple.pod(other, label="early")
        source = DictSource(data=ROWS, tag_columns=["k"], source_id="src")
        double.pod(source, label="dbl")
    return job


class TestPreflight:
    def test_no_node_runs_when_a_later_node_holds_v02_rows(
        self, store: Store, orchestrator: Orchestrator
    ):
        with writing_as_v02():
            _ = _two_node_job(store, with_early=False).run()
        calls.clear()
        early_calls.clear()
        before = snapshot(store)

        with pytest.raises(StoreHashLabelError):
            _ = _two_node_job(store, with_early=True).run(orchestrator=orchestrator)

        assert early_calls == []
        assert calls == []
        assert snapshot(store) == before
