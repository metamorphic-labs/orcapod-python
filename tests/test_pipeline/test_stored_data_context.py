"""Every stored row names its data context; reads use rows of the current one (ITL-944).

Orcapod v0.3 writes the data context key ``std:v1:default`` into the column
``__data_context`` of every table it stores, and every table moved to a new layout
(the last path component): ``pdb_v2``, ``rdb_v2``, ``tdb_v2``, ``odb_v1``, ``sdb_v1``.

- Rows of the current data context are the current entries. Rows of another data context
  in the same table are never returned or reused, are left unchanged, and are reported
  once per table.
- A table at an older layout (written before v0.3, without the column) is counted and
  reported once, never read as current and never modified.
- Side-effect logs are the exception: an old-layout log, or logged deliveries of another
  data context, raise before anything is delivered.

Stores "written by v0.2" and rows "of another data context" are produced here from a store
this version wrote, through the public database interface: the same rows are stored at the
old-layout paths without the column, or under another data context key with other keys.
"""

from __future__ import annotations

import logging
import pathlib
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.contexts import stored_data_contexts
from orcapod.core.data_function import CachedDataFunction
from orcapod.core.datagrams import Data
from orcapod.core.operators import SelectDataColumns
from orcapod.core.sources.cached_source import CachedSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import SideEffectLogDataContextError, StoreError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.side_effects import SideEffectPod, sink_pod
from orcapod.sources import ArrowTableSource, DictSource
from orcapod.types import CacheMode

Orchestrator = SyncPipelineOrchestrator | AsyncPipelineOrchestrator
Store = InMemoryArrowDatabase | DeltaTableDatabase

JOB_NAME = "itl944"
ROWS = [{"k": "a", "x": 1}, {"k": "b", "x": 2}]
CURRENT = "std:v1:default"
PRE_V1 = "std:v0.1:default"
OTHER = "std:v0.9:other"
COLUMN = "__data_context"
RID = "__rid"

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


def _cached_source(store: Store) -> CachedSource:
    table = pa.table({"k": ["a", "b"], "x": [1, 2]})
    return CachedSource(
        ArrowTableSource(table, tag_columns=["k"], source_id="fixed"),
        cache_database=store,
    )


# Root directory of each Delta store handed out by the ``new_store`` fixture.
_delta_roots: dict[int, pathlib.Path] = {}


def reopened(store: Store) -> Store:
    """A database object that sees everything written to ``store`` so far.

    A ``DeltaTableDatabase`` keeps the table version it read first, so rows written
    later through another view of the same directory need a new object to be seen.
    """
    if isinstance(store, DeltaTableDatabase):
        return DeltaTableDatabase(_delta_roots[id(store)])
    return store


def delta_at(root: pathlib.Path) -> DeltaTableDatabase:
    """A Delta store rooted at ``root`` that ``reopened`` can reopen."""
    store = DeltaTableDatabase(root)
    _delta_roots[id(store)] = root
    return store


def snapshot(store: Store) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    """Every table of ``store`` with its rows, record ids included."""
    store = reopened(store)
    tables: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for path in sorted(store.list_sources()):
        records = store.get_all_records(path, record_id_column=RID)
        tables[path] = [] if records is None else records.to_pylist()
    return tables


def delta_files(root: pathlib.Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()
    }


@pytest.fixture(params=["in_memory", "delta"])
def new_store(
    request: pytest.FixtureRequest, tmp_path: pathlib.Path
) -> Callable[[], Store]:
    """A factory of fresh, empty stores of one backend."""
    made: list[Store] = []

    def make() -> Store:
        if request.param == "in_memory":
            store: Store = InMemoryArrowDatabase()
        else:
            store = delta_at(tmp_path / f"store{len(made)}")
        made.append(store)
        return store

    return make


@pytest.fixture
def store(new_store: Callable[[], Store]) -> Store:
    return new_store()


@pytest.fixture(params=["sync", "async"])
def orchestrator(request: pytest.FixtureRequest) -> Orchestrator:
    return {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}[
        request.param
    ]()


def old_layout_path(path: tuple[str, ...]) -> tuple[str, ...]:
    """Where Orcapod v0.2 stored the table that this version stores at ``path``."""
    layout = path[-1]
    renamed = {"pdb_v2": "pdb_v1", "rdb_v2": "rdb_v1", "tdb_v2": "tdb_v1"}
    if layout in renamed:
        return path[:-1] + (renamed[layout],)
    assert layout in ("odb_v1", "sdb_v1"), path
    return path[:-1]


def _other_key(value: Any) -> Any:
    """A stored key as another generation of hashing would have written it."""
    if isinstance(value, bytes) and value.startswith(b"arrow_v0.2:"):
        return b"arrow_v9.9:" + value[len(b"arrow_v0.2:") :]
    if isinstance(value, bytes) and value.startswith(b"000002"):
        return b"ffffff" + value[6:]
    return value


def copy_as_v02(written: Store, target: Store) -> None:
    """Store the rows of ``written`` in ``target`` the way Orcapod v0.2 laid them out."""
    source = reopened(written)
    for path in sorted(source.list_sources()):
        rows = source.get_all_records(path, record_id_column=RID)
        assert rows is not None
        target.add_records(
            old_layout_path(path), rows.drop([COLUMN]), record_id_column=RID
        )
    target.flush()


def copy_as_other_context(written: Store, target: Store) -> None:
    """Store the rows of ``written`` in ``target`` as rows of another data context.

    Same tables, same layout; the rows carry the key ``OTHER`` and the Arrow-derived
    keys another generation of hashing would have produced.
    """
    source = reopened(written)
    for path in sorted(source.list_sources()):
        rows = source.get_all_records(path, record_id_column=RID)
        assert rows is not None
        columns = {
            name: pa.array(
                [_other_key(value) for value in rows.column(name).to_pylist()],
                type=rows.schema.field(name).type,
            )
            for name in rows.column_names
        }
        columns[COLUMN] = pa.array([OTHER] * rows.num_rows, type=pa.large_string())
        target.add_records(
            path, pa.table(columns, schema=rows.schema), record_id_column=RID
        )
    target.flush()


def run_everything(store: Store) -> None:
    """Write every table kind: function, operator cache, side-effect log, cached source."""
    _ = build_job(store, with_operator=True, with_sink=True).run()
    _ = _cached_source(store).as_table()
    calls.clear()
    delivered.clear()


def v02_store(new_store: Callable[[], Store], *, with_sink: bool = False) -> Store:
    """A store holding only what Orcapod v0.2 would have written."""
    written = new_store()
    _ = build_job(written, with_operator=True, with_sink=with_sink).run()
    _ = _cached_source(written).as_table()
    target = new_store()
    copy_as_v02(written, target)
    calls.clear()
    delivered.clear()
    return target


def other_context_store(
    new_store: Callable[[], Store], *, with_sink: bool = False
) -> Store:
    """A store at the current layout holding only rows of another data context."""
    written = new_store()
    _ = build_job(written, with_operator=True, with_sink=with_sink).run()
    _ = _cached_source(written).as_table()
    target = new_store()
    copy_as_other_context(written, target)
    calls.clear()
    delivered.clear()
    return target


def both_contexts_store(new_store: Callable[[], Store]) -> Store:
    """A store whose tables hold rows of another data context and of the current one."""
    store = other_context_store(new_store)
    _ = build_job(store, with_operator=True).run()
    _ = _cached_source(store).as_table()
    calls.clear()
    return store


def rows_of(table: pa.Table | None, column: str = "double") -> list[Any]:
    return [] if table is None else sorted(table[column].to_pylist())


def table_path(store: Store, layout: str) -> tuple[str, ...]:
    """The one stored table whose layout component is ``layout``."""
    (path,) = [p for p in reopened(store).list_sources() if p[-1] == layout]
    return path


LAYOUTS = ("pdb_v2", "rdb_v2", "odb_v1", "sdb_v1", "tdb_v2")


# ---------------------------------------------------------------------------
# The column and the layouts
# ---------------------------------------------------------------------------


class TestColumnAndLayout:
    def test_every_table_is_at_the_new_layout(self, store: Store):
        run_everything(store)
        layouts = sorted(path[-1] for path in reopened(store).list_sources())
        assert layouts == sorted(LAYOUTS)

    def test_explicit_cache_path_ends_with_the_layout_component(self, store: Store):
        table = pa.table({"k": ["a"], "x": [1]})
        source = CachedSource(
            ArrowTableSource(table, tag_columns=["k"], source_id="fixed"),
            cache_database=store,
            cache_path=("my", "cache"),
        )
        _ = source.as_table()
        assert source.cache_path == ("my", "cache", "sdb_v1")
        assert reopened(store).list_sources() == [("my", "cache", "sdb_v1")]

    @pytest.mark.parametrize("layout", LAYOUTS)
    def test_every_row_names_the_current_data_context(self, store: Store, layout: str):
        run_everything(store)
        records = reopened(store).get_all_records(table_path(store, layout))
        assert records is not None
        assert records.schema.field(COLUMN).type == pa.large_string()
        assert records.column(COLUMN).to_pylist() == [CURRENT, CURRENT]

    def test_the_old_context_columns_are_gone(self, store: Store):
        run_everything(store)
        for path in reopened(store).list_sources():
            records = reopened(store).get_all_records(path)
            assert records is not None
            assert "_context_key" not in records.column_names, path
            assert "__input_data_context_key" not in records.column_names, path

    def test_node_tables_show_the_stored_column_only_as_a_meta_column(
        self, store: Store
    ):
        """``__data_context`` is bookkeeping: hidden by default, like every ``__`` column."""
        _ = build_job(store, with_operator=True).run()
        job = build_job(reopened(store), with_operator=True)
        for label in ("dbl", "sel"):
            assert COLUMN not in job.nodes[label].as_table().column_names
            records = job.nodes[label].get_all_records()
            assert records is not None
            assert COLUMN not in records.column_names
        with_meta = job.nodes["dbl"].get_all_records(all_info=True)
        assert with_meta is not None
        assert with_meta.column(COLUMN).to_pylist() == [CURRENT, CURRENT]

    def test_store_written_by_this_version_is_reused(
        self, store: Store, orchestrator: Orchestrator
    ):
        _ = build_job(store, with_operator=True, with_sink=True).run(
            orchestrator=orchestrator
        )
        calls.clear()
        delivered.clear()

        job = build_job(store, with_operator=True, with_sink=True)
        _ = job.run(orchestrator=orchestrator)

        assert calls == []
        assert delivered == []
        assert rows_of(job.nodes["dbl"].as_table()) == [2, 4]
        for _, data in job.nodes["dbl"].iter_data():
            assert data.data_context_key == CURRENT


# ---------------------------------------------------------------------------
# Tables at an older layout
# ---------------------------------------------------------------------------


class TestOldLayout:
    def test_old_tables_are_not_read_and_inputs_are_recomputed(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        store = v02_store(new_store)

        job = build_job(store, with_operator=True)
        _ = job.run(orchestrator=orchestrator)

        assert sorted(calls) == [1, 2]
        assert rows_of(job.nodes["dbl"].as_table()) == [2, 4]
        assert rows_of(job.nodes["sel"].as_table()) == [2, 4]

    def test_old_tables_alone_give_no_rows(self, new_store: Callable[[], Store]):
        store = v02_store(new_store)
        job = build_job(store, with_operator=True, cache_mode=CacheMode.REPLAY)
        node = job.nodes["dbl"]
        assert node.as_table().num_rows == 0
        assert node.get_all_records() is None
        assert job.nodes["sel"].get_all_records() is None
        assert calls == []

    def test_old_tables_stay_unchanged(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        store = v02_store(new_store)
        before = snapshot(store)

        _ = build_job(store, with_operator=True).run(orchestrator=orchestrator)
        _ = _cached_source(store).as_table()

        after = snapshot(store)
        for path, old_rows in before.items():
            assert after[path] == old_rows, path
        assert set(before) < set(after)

    def test_delta_files_written_by_v02_are_byte_identical(
        self, tmp_path: pathlib.Path
    ):
        written = DeltaTableDatabase(tmp_path / "written")
        _ = build_job(written, with_operator=True).run()
        root = tmp_path / "store"
        copy_as_v02(delta_at(tmp_path / "written"), delta_at(root))
        before = delta_files(root)

        _ = build_job(DeltaTableDatabase(root), with_operator=True).run()

        after = delta_files(root)
        for name, content in before.items():
            assert after.get(name) == content, name
        assert len(after) > len(before)

    def test_cached_source_ignores_and_keeps_an_old_cache(
        self, new_store: Callable[[], Store]
    ):
        store = v02_store(new_store)
        before = snapshot(store)

        source = _cached_source(store)
        assert rows_of(source.as_table(), "x") == [1, 2]
        assert rows_of(source.get_all_records(), "x") == [1, 2]

        after = snapshot(store)
        for path, old_rows in before.items():
            assert after[path] == old_rows, path

    def test_each_old_table_is_reported_once_with_its_row_count(
        self,
        new_store: Callable[[], Store],
        orchestrator: Orchestrator,
        caplog: pytest.LogCaptureFixture,
    ):
        store = v02_store(new_store)
        old_paths = ["/".join(path) for path in sorted(reopened(store).list_sources())]

        with caplog.at_level(logging.WARNING, logger="orcapod"):
            job = build_job(store, with_operator=True)
            _ = job.run(orchestrator=orchestrator)
            _ = job.nodes["dbl"].as_table()
            _ = _cached_source(store).as_table()

        reports = [
            r.getMessage()
            for r in caplog.records
            if "older table layout" in r.getMessage()
        ]
        assert len(reports) == len(old_paths) == 4
        for path in old_paths:
            (report,) = [r for r in reports if repr(path) in r]
            assert report.startswith("Found 2 rows in stored table")
            assert PRE_V1 in report
            assert "left unchanged" in report

    def test_nothing_is_reported_for_a_store_of_this_version(
        self, store: Store, caplog: pytest.LogCaptureFixture
    ):
        run_everything(store)
        with caplog.at_level(logging.WARNING, logger="orcapod"):
            run_everything(store)
        assert [r for r in caplog.records if "data context" in r.getMessage()] == []


# ---------------------------------------------------------------------------
# Rows of another data context in a table at the current layout
# ---------------------------------------------------------------------------


class TestRowsOfAnotherDataContext:
    def test_they_are_ignored_and_inputs_are_recomputed(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        store = other_context_store(new_store)
        before = snapshot(store)

        job = build_job(store, with_operator=True)
        _ = job.run(orchestrator=orchestrator)
        assert rows_of(_cached_source(store).as_table(), "x") == [1, 2]

        assert sorted(calls) == [1, 2]
        assert rows_of(job.nodes["dbl"].as_table()) == [2, 4]
        assert rows_of(job.nodes["sel"].as_table()) == [2, 4]
        after = snapshot(store)
        for path, old_rows in before.items():
            assert len(after[path]) == 2 * len(old_rows), path
            for row in old_rows:
                assert row in after[path], path

    def test_alone_they_give_no_rows(self, new_store: Callable[[], Store]):
        store = other_context_store(new_store)
        job = build_job(store, with_operator=True, cache_mode=CacheMode.REPLAY)
        node = job.nodes["dbl"]
        assert node.as_table().num_rows == 0
        assert node.get_all_records() is None
        assert list(node.iter_data()) == []
        assert job.nodes["sel"].get_all_records() is None
        assert calls == []

    def test_function_node_reads_current_rows_only(
        self, new_store: Callable[[], Store]
    ):
        store = both_contexts_store(new_store)
        node = build_job(store).nodes["dbl"]
        assert rows_of(node.as_table()) == [2, 4]
        assert rows_of(node.get_all_records()) == [2, 4]
        assert sorted(d.as_dict()["double"] for _, d in node.iter_data()) == [2, 4]
        assert rows_of(node.as_source().as_table()) == [2, 4]

    def test_current_rows_are_reused(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        store = both_contexts_store(new_store)
        before = snapshot(store)

        job = build_job(store, with_operator=True)
        _ = job.run(orchestrator=orchestrator)

        assert calls == []
        assert rows_of(job.nodes["dbl"].as_table()) == [2, 4]
        assert snapshot(store) == before

    def test_operator_node_reads_current_rows_only(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        store = both_contexts_store(new_store)
        job = build_job(store, with_operator=True, cache_mode=CacheMode.REPLAY)
        assert rows_of(job.nodes["sel"].get_all_records()) == [2, 4]
        _ = job.run(orchestrator=orchestrator)
        assert rows_of(job.nodes["sel"].as_table()) == [2, 4]

    def test_cached_source_reads_current_rows_only(
        self, new_store: Callable[[], Store]
    ):
        store = both_contexts_store(new_store)
        source = _cached_source(store)
        assert rows_of(source.as_table(), "x") == [1, 2]
        assert rows_of(source.get_all_records(), "x") == [1, 2]

    def test_result_lookup_matches_current_rows_only(self, store: Store):
        """A result stored under another data context for the same input hash is not used."""
        cached = CachedDataFunction(double.pod.data_function, result_database=store)
        _ = cached.call(Data({"x": 1}))
        (path,) = reopened(store).list_sources()
        rows = reopened(store).get_all_records(path, record_id_column=RID)
        assert rows is not None
        # The same input hash under another data context, newer, with another output.
        forged = rows.to_pylist()[0] | {
            RID: b"another-row-id16",
            COLUMN: OTHER,
            "double": 999,
            "__pod_ts": rows.column("__pod_ts")[0].as_py().replace(year=2099),
        }
        store.add_records(
            path,
            pa.Table.from_pylist([forged], schema=rows.schema),
            record_id_column=RID,
        )
        store.flush()
        calls.clear()

        again = CachedDataFunction(
            double.pod.data_function, result_database=reopened(store)
        )
        output = again.call(Data({"x": 1}))

        assert output is not None
        assert output.as_dict()["double"] == 2
        assert calls == []
        assert rows_of(again.get_all_cached_outputs()) == [2]

    def test_skipped_rows_are_reported_once_per_table(
        self,
        new_store: Callable[[], Store],
        orchestrator: Orchestrator,
        caplog: pytest.LogCaptureFixture,
    ):
        store = both_contexts_store(new_store)
        caplog.clear()  # the store was prepared by a run that reported too
        with caplog.at_level(logging.WARNING, logger="orcapod"):
            job = build_job(store, with_operator=True, cache_mode=CacheMode.REPLAY)
            _ = job.run(orchestrator=orchestrator)

        reports = [
            r.getMessage()
            for r in caplog.records
            if "another data context" in r.getMessage()
        ]
        for layout in ("pdb_v2", "odb_v1"):
            path = "/".join(table_path(store, layout))
            (report,) = [r for r in reports if repr(path) in r]
            assert report.startswith("Skipped 2 rows of stored table")
            assert "left unchanged" in report


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


def _sink_store(
    new_store: Callable[[], Store], copy: Callable[[Store, Store], None]
) -> Store:
    written = new_store()
    _ = _sink_job(written).run()
    assert sorted(delivered) == [2, 4]
    delivered.clear()
    target = new_store()
    copy(written, target)
    return target


class TestSideEffects:
    """A side effect is an external action, so an old log fails instead of delivering again."""

    def test_an_old_layout_log_raises_and_nothing_is_delivered(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        store = _sink_store(new_store, copy_as_v02)
        before = snapshot(store)

        with pytest.raises(SideEffectLogDataContextError) as info:
            _ = _sink_job(store).run(orchestrator=orchestrator)

        assert delivered == []
        assert snapshot(store) == before
        error = info.value
        assert isinstance(error, StoreError)
        assert error.found_context == PRE_V1
        assert error.current_context == CURRENT
        assert error.node_label == "sink"
        assert error.record_path[-1] == "tdb_v1"
        message = str(error)
        assert "/".join(error.record_path) in message
        assert repr(PRE_V1) in message
        assert repr(CURRENT) in message
        assert "delivered" in message
        assert "migrat" in message

    def test_logged_deliveries_of_another_data_context_raise(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        store = _sink_store(new_store, copy_as_other_context)
        before = snapshot(store)

        with pytest.raises(SideEffectLogDataContextError) as info:
            _ = _sink_job(store).run(orchestrator=orchestrator)

        assert delivered == []
        assert snapshot(store) == before
        assert info.value.found_context == OTHER
        assert info.value.current_context == CURRENT
        assert info.value.record_path[-1] == "tdb_v2"

    def test_delta_log_files_are_untouched(self, tmp_path: pathlib.Path):
        _ = _sink_job(DeltaTableDatabase(tmp_path / "written")).run()
        delivered.clear()
        root = tmp_path / "store"
        copy_as_v02(delta_at(tmp_path / "written"), delta_at(root))
        before = delta_files(root)

        with pytest.raises(SideEffectLogDataContextError):
            _ = _sink_job(DeltaTableDatabase(root)).run()

        assert delivered == []
        assert delta_files(root) == before

    def test_function_node_upstream_of_a_refused_sink_is_not_refused(
        self, new_store: Callable[[], Store]
    ):
        """Only the side-effect node fails; old function and operator tables are skipped."""
        store = v02_store(new_store, with_sink=True)

        with pytest.raises(SideEffectLogDataContextError):
            _ = build_job(store, with_sink=True).run()

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
# Listing the data contexts a table holds
# ---------------------------------------------------------------------------


class TestStoredDataContexts:
    @pytest.mark.parametrize("layout", LAYOUTS)
    def test_table_of_this_version(self, store: Store, layout: str):
        run_everything(store)
        path = table_path(store, layout)
        assert stored_data_contexts(reopened(store), path) == {CURRENT: 2}

    @pytest.mark.parametrize("layout", ["pdb_v2", "rdb_v2", "odb_v1", "sdb_v1"])
    def test_table_holding_two_data_contexts(
        self, new_store: Callable[[], Store], layout: str
    ):
        store = both_contexts_store(new_store)
        path = table_path(store, layout)
        assert stored_data_contexts(reopened(store), path) == {CURRENT: 2, OTHER: 2}

    def test_old_layout_table_is_the_pre_v1_data_context(
        self, new_store: Callable[[], Store]
    ):
        store = v02_store(new_store, with_sink=True)
        paths = reopened(store).list_sources()
        assert len(paths) == 5
        for path in paths:
            assert stored_data_contexts(reopened(store), path) == {PRE_V1: 2}

    def test_missing_table_holds_nothing(self, store: Store):
        assert stored_data_contexts(store, ("no", "such", "table")) == {}
