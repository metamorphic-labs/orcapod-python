"""A job stores function results in a separate result database when given one (ITL-1408).

With no ``result_database`` every path is as before: results under
``<job name>/_result``. With one, results are stored in it as given, with no job name,
so several jobs share them: a second job computes nothing that a first job stored.
Pipeline records, operator caches and side-effect logs stay under each job's own
database and name.
"""

from __future__ import annotations

import json
import logging
import pathlib
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import SelectDataColumns
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import SideEffectLogDataContextError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import ArrowTableSource

Orchestrator = SyncPipelineOrchestrator | AsyncPipelineOrchestrator

calls: list[tuple[str, Any]] = []
delivered: list[float] = []


@function_pod(output_keys="doubled")
def double(x: float) -> float:
    """First step: counted."""
    calls.append(("double", x))
    return x * 2


@function_pod(output_keys=["shifted", "label"])
def shift(doubled: float) -> tuple[float, str]:
    """Second step, two outputs: counted."""
    calls.append(("shift", doubled))
    return doubled + 1, f"v{doubled}"


@function_pod(output_keys="doubled", function_name="double")
def double_as_text(x: float) -> str:
    """Same function name as ``double``, another output schema: another uri."""
    calls.append(("double_as_text", x))
    return str(x * 2)


@function_pod(output_keys="sent", ctx_arg="ctx")
def notify(x: float, ctx: Any) -> float:
    """A function that takes the invocation context: its call is an external action."""
    delivered.append(x)
    return x


@pytest.fixture(autouse=True)
def _reset() -> None:
    calls.clear()
    delivered.clear()


@pytest.fixture(params=["sync", "async"])
def orchestrator(request: pytest.FixtureRequest) -> Orchestrator:
    if request.param == "sync":
        return SyncPipelineOrchestrator()
    return AsyncPipelineOrchestrator()


def make_source(xs: list[float], source_id: str = "s") -> ArrowTableSource:
    """Two tag columns and two data columns; ``x`` is what the functions read."""
    n = len(xs)
    schema = pa.schema(
        [
            pa.field("subject", pa.large_string(), nullable=False),
            pa.field("session", pa.int64(), nullable=False),
            pa.field("x", pa.float64(), nullable=False),
            pa.field("note", pa.large_string(), nullable=False),
        ]
    )
    table = pa.table(
        [
            pa.array([f"m{i}" for i in range(n)], pa.large_string()),
            pa.array(list(range(n)), pa.int64()),
            pa.array(xs, pa.float64()),
            pa.array([f"n{i}" for i in range(n)], pa.large_string()),
        ],
        schema=schema,
    )
    return ArrowTableSource(
        table,
        tag_columns=["subject", "session"],
        tag_only_columns=True,
        source_id=source_id,
    )


def two_step_job(
    name: str,
    database: ArrowDatabaseProtocol,
    xs: list[float],
    result_database: ArrowDatabaseProtocol | None = None,
    source_id: str = "s",
) -> PipelineJob:
    """A job of two chained function nodes, then an operator."""
    job = PipelineJob(name=name, database=database, result_database=result_database)
    with job:
        doubled = double.pod(make_source(xs, source_id), label="double")
        shifted = shift.pod(doubled, label="shift")
        _ = SelectDataColumns(["shifted"])(shifted)
    return job


def table_paths(database: ArrowDatabaseProtocol) -> list[str]:
    return sorted("/".join(path) for path in database.list_table_paths(()))


def rows(job: PipelineJob, label: str, column: str) -> list[Any]:
    """A node's values of ``column``, sorted (no order is promised)."""
    return sorted(job.nodes[label].as_table().column(column).to_pylist())


def stored_rows(job: PipelineJob, label: str, column: str) -> list[Any]:
    """The values of ``column`` in a node's stored view, sorted."""
    return sorted(job.nodes[label].as_source().as_table().column(column).to_pylist())


class TestPaths:
    def test_without_a_result_database_results_are_under_the_job(self) -> None:
        db = InMemoryArrowDatabase()
        job = two_step_job("a", db, [1.0, 2.0])
        _ = job.run()
        assert job.result_database is None
        result_tables = [p for p in table_paths(db) if p.endswith("rdb_v1")]
        assert len(result_tables) == 2
        assert all(p.startswith("a/_result/") for p in result_tables)

    def test_with_a_result_database_results_carry_no_job_name(self) -> None:
        db = InMemoryArrowDatabase()
        results = InMemoryArrowDatabase()
        job = two_step_job("a", db, [1.0, 2.0], result_database=results)
        _ = job.run()
        assert job.result_database is results
        assert sorted(p.split("/")[0] for p in table_paths(results)) == [
            "double",
            "shift",
        ]
        assert all(p.endswith("rev1/rdb_v1") for p in table_paths(results))
        # Pipeline records stay under the job's own name.
        own = table_paths(db)
        assert own and all(p.startswith("a/") for p in own)
        assert not any("_result" in p or p.endswith("rdb_v1") for p in own)

    def test_a_scoped_view_is_used_as_given(self) -> None:
        results = InMemoryArrowDatabase()
        job = two_step_job(
            "a", InMemoryArrowDatabase(), [1.0], result_database=results.at("shared")
        )
        _ = job.run()
        assert all(p.startswith("shared/") for p in table_paths(results))


class TestSharing:
    @pytest.mark.parametrize("names", [("a", "b"), ("b", "a")])
    def test_a_second_job_computes_nothing_the_first_stored(
        self, names: tuple[str, str], orchestrator: Orchestrator
    ) -> None:
        db = InMemoryArrowDatabase()
        results = InMemoryArrowDatabase()
        first = two_step_job(names[0], db, [1.0, 2.0, 3.0], result_database=results)
        _ = first.run(orchestrator=orchestrator)
        assert len(calls) == 6
        calls.clear()

        second = two_step_job(names[1], db, [1.0, 2.0, 3.0], result_database=results)
        _ = second.run(orchestrator=orchestrator)
        assert calls == []
        assert rows(second, "shift", "shifted") == [3.0, 5.0, 7.0]
        # Each job keeps its own pipeline records.
        assert {p.split("/")[0] for p in table_paths(db)} == {"a", "b"}

    def test_only_inputs_not_stored_are_computed(
        self, orchestrator: Orchestrator
    ) -> None:
        results = InMemoryArrowDatabase()
        _ = two_step_job(
            "a", InMemoryArrowDatabase(), [1.0, 2.0], result_database=results
        ).run(orchestrator=orchestrator)
        calls.clear()
        second = two_step_job(
            "b", InMemoryArrowDatabase(), [2.0, 1.0, 4.0], result_database=results
        )
        _ = second.run(orchestrator=orchestrator)
        assert sorted(calls) == [("double", 4.0), ("shift", 8.0)]
        assert rows(second, "double", "doubled") == [2.0, 4.0, 8.0]

    def test_without_a_result_database_jobs_do_not_share(self) -> None:
        db = InMemoryArrowDatabase()
        _ = two_step_job("a", db, [1.0, 2.0]).run()
        calls.clear()
        _ = two_step_job("b", db, [1.0, 2.0]).run()
        assert len(calls) == 4

    def test_another_output_schema_is_another_table(
        self, orchestrator: Orchestrator
    ) -> None:
        results = InMemoryArrowDatabase()
        _ = two_step_job(
            "a", InMemoryArrowDatabase(), [1.0, 2.0], result_database=results
        ).run(orchestrator=orchestrator)
        calls.clear()
        job = PipelineJob(
            name="b", database=InMemoryArrowDatabase(), result_database=results
        )
        with job:
            double_as_text.pod(make_source([1.0, 2.0]), label="double")
        _ = job.run(orchestrator=orchestrator)
        assert sorted(calls) == [("double_as_text", 1.0), ("double_as_text", 2.0)]
        assert rows(job, "double", "doubled") == ["2.0", "4.0"]
        double_tables = [p for p in table_paths(results) if p.startswith("double/")]
        assert len(double_tables) == 2
        assert double_tables[0].split("/")[1] != double_tables[1].split("/")[1]


class TestEachJobReadsItsOwnRows:
    """Two jobs over different sources share one result table; each reads its own."""

    @pytest.fixture
    def jobs(
        self, tmp_path: pathlib.Path, orchestrator: Orchestrator
    ) -> tuple[PipelineJob, PipelineJob, pathlib.Path]:
        db = DeltaTableDatabase(tmp_path / "jobs")
        results = DeltaTableDatabase(tmp_path / "results")
        a = two_step_job("a", db, [1.0, 2.0], result_database=results, source_id="sa")
        b = two_step_job(
            "b", db, [2.0, 10.0, 20.0], result_database=results, source_id="sb"
        )
        _ = a.run(orchestrator=orchestrator)
        _ = b.run(orchestrator=orchestrator)
        assert len([p for p in table_paths(results) if "double" in p]) == 1
        return a, b, tmp_path

    def test_as_table_and_as_source(
        self, jobs: tuple[PipelineJob, PipelineJob, pathlib.Path]
    ) -> None:
        a, b, _ = jobs
        assert rows(a, "double", "doubled") == [2.0, 4.0]
        assert rows(b, "double", "doubled") == [4.0, 20.0, 40.0]
        assert stored_rows(a, "shift", "shifted") == [3.0, 5.0]
        assert stored_rows(b, "shift", "shifted") == [5.0, 21.0, 41.0]

    def test_a_loaded_job(
        self, jobs: tuple[PipelineJob, PipelineJob, pathlib.Path]
    ) -> None:
        a, b, tmp_path = jobs
        a.save(tmp_path / "a.json")
        b.save(tmp_path / "b.json")
        loaded_a = PipelineJob.load(tmp_path / "a.json")
        loaded_b = PipelineJob.load(tmp_path / "b.json")
        assert isinstance(loaded_a.result_database, DeltaTableDatabase)
        assert stored_rows(loaded_a, "shift", "shifted") == [3.0, 5.0]
        assert stored_rows(loaded_b, "shift", "shifted") == [5.0, 21.0, 41.0]

    def test_a_load_only_run(
        self, jobs: tuple[PipelineJob, PipelineJob, pathlib.Path]
    ) -> None:
        a, b, tmp_path = jobs
        a.save(tmp_path / "a.json")
        b.save(tmp_path / "b.json")
        loaded_a = PipelineJob.load(tmp_path / "a.json")
        loaded_b = PipelineJob.load(tmp_path / "b.json")
        calls.clear()
        _ = loaded_a.bind(sources={"sa": make_source([1.0, 2.0], "sa")})
        _ = loaded_b.bind(sources={"sb": make_source([2.0, 10.0, 20.0], "sb")})
        _ = loaded_a.run(load_only=True)
        _ = loaded_b.run(load_only=True)
        assert calls == []
        assert rows(loaded_a, "shift", "shifted") == [3.0, 5.0]
        assert rows(loaded_b, "shift", "shifted") == [5.0, 21.0, 41.0]


class TestSaveAndLoad:
    def test_without_a_result_database_the_file_names_none(
        self, tmp_path: pathlib.Path
    ) -> None:
        job = two_step_job("a", DeltaTableDatabase(tmp_path / "db"), [1.0])
        _ = job.run()
        job.save(tmp_path / "job.json")
        saved = json.loads((tmp_path / "job.json").read_text())
        assert saved["bindings"]["result_database"] is None
        loaded = PipelineJob.load(tmp_path / "job.json")
        assert loaded.result_database is None
        assert stored_rows(loaded, "double", "doubled") == [2.0]

    def test_an_explicit_result_database_wins_over_the_file(
        self, tmp_path: pathlib.Path
    ) -> None:
        results = DeltaTableDatabase(tmp_path / "results")
        job = two_step_job(
            "a", DeltaTableDatabase(tmp_path / "db"), [1.0], result_database=results
        )
        _ = job.run()
        job.save(tmp_path / "job.json")
        # The results moved: the file's location now holds none.
        (tmp_path / "results").rename(tmp_path / "moved")
        loaded = PipelineJob.load(
            tmp_path / "job.json",
            result_database=DeltaTableDatabase(tmp_path / "moved"),
        )
        assert stored_rows(loaded, "shift", "shifted") == [3.0]

    def test_a_result_database_that_cannot_be_rebuilt_is_not_replaced(
        self, tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        results = DeltaTableDatabase(tmp_path / "results")
        job = two_step_job(
            "a", DeltaTableDatabase(tmp_path / "db"), [1.0], result_database=results
        )
        _ = job.run()
        job.save(tmp_path / "job.json")
        saved = json.loads((tmp_path / "job.json").read_text())
        saved["bindings"]["result_database"] = {"type": "no_such_database"}
        (tmp_path / "job.json").write_text(json.dumps(saved))

        with caplog.at_level(logging.WARNING, logger="orcapod.pipeline.job"):
            loaded = PipelineJob.load(tmp_path / "job.json")
        assert "bindings.result_database" in caplog.text
        assert "result_database=" in caplog.text
        # The function nodes get no database, never the job's own _result location.
        with pytest.raises(RuntimeError):
            loaded.nodes["double"].as_source()
        _ = loaded.bind(result_database=results)
        assert stored_rows(loaded, "shift", "shifted") == [3.0]


class TestBind:
    def test_bind_a_result_database(self) -> None:
        db = InMemoryArrowDatabase()
        results = InMemoryArrowDatabase()
        _ = two_step_job("a", db, [1.0], result_database=results).run()
        calls.clear()
        job = two_step_job("b", db, [1.0])
        _ = job.bind(result_database=results)
        _ = job.run()
        assert calls == []
        assert not any(p.startswith("b/_result") for p in table_paths(db))

    def test_the_batch_size_of_a_result_database_is_checked(
        self, tmp_path: pathlib.Path
    ) -> None:
        job = two_step_job(
            "a",
            InMemoryArrowDatabase(),
            [1.0],
            result_database=DeltaTableDatabase(tmp_path / "r", batch_size=5),
        )
        with pytest.raises(ValueError, match="batch_size=5"):
            _ = job.run(commit_every_rows=10)


class TestSideEffectingFunctions:
    """A function that takes the invocation context shares its completions."""

    @staticmethod
    def notify_job(
        name: str, xs: list[float], results: ArrowDatabaseProtocol
    ) -> PipelineJob:
        job = PipelineJob(
            name=name, database=InMemoryArrowDatabase(), result_database=results
        )
        with job:
            notify.pod(make_source(xs), label="notify")
        return job

    def test_only_inputs_no_job_delivered_are_delivered(
        self, orchestrator: Orchestrator
    ) -> None:
        results = InMemoryArrowDatabase()
        _ = self.notify_job("a", [1.0, 2.0], results).run(orchestrator=orchestrator)
        assert sorted(delivered) == [1.0, 2.0]
        delivered.clear()
        job = self.notify_job("b", [2.0, 3.0, 1.0], results)
        _ = job.run(orchestrator=orchestrator)
        assert delivered == [3.0]
        assert rows(job, "notify", "sent") == [1.0, 2.0, 3.0]

    def test_an_old_result_table_in_the_result_database_refuses(self) -> None:
        written = InMemoryArrowDatabase()
        _ = self.notify_job("a", [1.0], written).run()
        delivered.clear()
        # The same results as an earlier build laid them out (no revision, rdb_v2).
        old = InMemoryArrowDatabase()
        for path in written.list_table_paths(()):
            stored = written.get_all_records(path, record_id_column="__rid")
            assert stored is not None
            assert path[-2:] == ("rev1", "rdb_v1")
            old.add_records(path[:-2] + ("rdb_v2",), stored, record_id_column="__rid")
        old.flush()

        with pytest.raises(SideEffectLogDataContextError) as info:
            _ = self.notify_job("b", [1.0], old).run()
        assert delivered == []
        assert [path[-1] for path in info.value.record_paths] == ["rdb_v2"]


# ---------------------------------------------------------------------------
# Review findings (PR 507): each has a test that fails without its fix, or pins
# the behaviour stated for it.
# ---------------------------------------------------------------------------


@function_pod(output_keys="doubled", function_name="double")
def double_changed(x: float) -> float:
    """``double``'s name, output schema and major with another body: one uri."""
    calls.append(("double_changed", x))
    return x * 3


def one_step_job(
    name: str,
    database: ArrowDatabaseProtocol,
    xs: list[float],
    pod: Any = double,
    result_database: ArrowDatabaseProtocol | None = None,
    source_id: str = "s",
) -> PipelineJob:
    """A job of one function node, labelled ``out``."""
    job = PipelineJob(name=name, database=database, result_database=result_database)
    with job:
        pod.pod(make_source(xs, source_id), label="out")
    return job


class TestEveryRunSeesCommittedResults:
    """B1: two handles on one Delta directory; a warm handle sees the other's commits.

    The run reads the result tables from the store's current version when it starts.
    Parametrized over where the results are: a separate result database, or the job's
    own database (one job name, two handles; the same staleness before ITL-1408).
    """

    @pytest.fixture(params=["result_database", "job_database"])
    def placement(self, request: pytest.FixtureRequest) -> str:
        return request.param

    def handles_job(
        self,
        placement: str,
        handle: DeltaTableDatabase,
        name: str,
        xs: list[float],
        pod: Any = double,
    ) -> PipelineJob:
        if placement == "result_database":
            return one_step_job(name, InMemoryArrowDatabase(), xs, pod, handle)
        return one_step_job("shared", handle, xs, pod)

    def test_a_changed_body_reuses_what_another_handle_committed(
        self, tmp_path: pathlib.Path, placement: str, orchestrator: Orchestrator
    ) -> None:
        warm = DeltaTableDatabase(tmp_path / "store")
        other = DeltaTableDatabase(tmp_path / "store")
        _ = self.handles_job(placement, warm, "warm", [1.0]).run(
            orchestrator=orchestrator
        )
        _ = self.handles_job(placement, other, "writer", [2.0, 3.0]).run(
            orchestrator=orchestrator
        )
        calls.clear()
        job = self.handles_job(placement, warm, "reader", [2.0, 3.0], double_changed)
        _ = job.run(orchestrator=orchestrator)
        # The stored results of the same uri are reused, not computed by the new body.
        assert calls == []
        assert rows(job, "out", "doubled") == [4.0, 6.0]

    def test_a_ctx_function_does_not_deliver_again(
        self, tmp_path: pathlib.Path, placement: str, orchestrator: Orchestrator
    ) -> None:
        warm = DeltaTableDatabase(tmp_path / "store")
        other = DeltaTableDatabase(tmp_path / "store")
        for handle, name, xs in (
            (warm, "warm", [1.0]),
            (other, "writer", [3.0]),
            (warm, "reader", [3.0, 1.0]),
        ):
            _ = self.handles_job(placement, handle, name, xs, notify).run(
                orchestrator=orchestrator
            )
        assert sorted(delivered) == [1.0, 3.0]


class TestForeignCompletionsAfterTheFirstRun:
    """M1: completions marked with another data context after a node's first run.

    The check holds on every run, before anything is delivered. Parametrized over the
    store kind and where the results are.
    """

    @pytest.mark.parametrize("store", ["in_memory", "delta"])
    @pytest.mark.parametrize("separate", [True, False])
    def test_a_later_run_refuses_before_delivering(
        self,
        tmp_path: pathlib.Path,
        store: str,
        separate: bool,
        orchestrator: Orchestrator,
    ) -> None:
        def new_store(name: str) -> ArrowDatabaseProtocol:
            if store == "delta":
                return DeltaTableDatabase(tmp_path / name)
            return InMemoryArrowDatabase()

        database = new_store("db")
        results = new_store("results") if separate else None
        job = one_step_job("ctx", database, [1.0, 2.0], notify, results)
        _ = job.run(orchestrator=orchestrator)
        assert sorted(delivered) == [1.0, 2.0]
        delivered.clear()

        # Every stored completion is now marked with another data context.
        holder = results if results is not None else database
        (path,) = [p for p in holder.list_table_paths(()) if p[-1] == "rdb_v1"]
        stored = holder.get_all_records(path, record_id_column="__rid")
        assert stored is not None
        column = stored.schema.get_field_index("__data_context")
        foreign = stored.set_column(
            column,
            stored.schema.field(column),
            pa.array(["std:v0.9:other"] * stored.num_rows, pa.large_string()),
        )
        if store == "delta":
            # The Delta database appends; rewrite the table with the foreign rows.
            import deltalake

            deltalake.write_deltalake(
                str(tmp_path / ("results" if separate else "db") / "/".join(path)),
                foreign,
                mode="overwrite",
                schema_mode="overwrite",
            )
        else:
            holder.add_records(path, foreign, record_id_column="__rid")
            holder.flush()

        job.bind(sources={"s": make_source([1.0, 2.0, 3.0], "new")})
        with pytest.raises(SideEffectLogDataContextError):
            _ = job.run(orchestrator=orchestrator)
        assert delivered == []


class TestUnavailableSavedResultDatabase:
    """M2 and M3: a saved result database that is not available at load."""

    @pytest.fixture(params=["unknown_type", "in_memory"])
    def saved(
        self, request: pytest.FixtureRequest, tmp_path: pathlib.Path
    ) -> tuple[pathlib.Path, ArrowDatabaseProtocol]:
        """A job file whose result database cannot be had from the file alone."""
        if request.param == "in_memory":
            results: ArrowDatabaseProtocol = InMemoryArrowDatabase()
        else:
            results = DeltaTableDatabase(tmp_path / "results")
        job = one_step_job(
            "a",
            DeltaTableDatabase(tmp_path / "db"),
            [1.0, 2.0],
            result_database=results,
        )
        _ = job.run()
        path = tmp_path / "job.json"
        job.save(path)
        if request.param == "unknown_type":
            saved = json.loads(path.read_text())
            saved["bindings"]["result_database"] = {"type": "no_such_database"}
            _ = path.write_text(json.dumps(saved))
        return path, results

    def test_load_refuses_and_says_what_to_pass(
        self,
        saved: tuple[pathlib.Path, ArrowDatabaseProtocol],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        path, results = saved
        with caplog.at_level(logging.WARNING, logger="orcapod.pipeline.job"):
            loaded = PipelineJob.load(path)
        assert "bindings.result_database" in caplog.text
        assert "result_database=" in caplog.text
        assert loaded.result_database is None
        with pytest.raises(RuntimeError):
            _ = loaded.nodes["out"].as_source()
        given = PipelineJob.load(path, result_database=results)
        assert stored_rows(given, "out", "doubled") == [2.0, 4.0]

    def test_saving_the_loaded_job_keeps_the_binding(
        self,
        saved: tuple[pathlib.Path, ArrowDatabaseProtocol],
        tmp_path: pathlib.Path,
    ) -> None:
        path, _ = saved
        before = json.loads(path.read_text())["bindings"]["result_database"]
        loaded = PipelineJob.load(path)
        again = tmp_path / "again.json"
        loaded.save(again)
        assert json.loads(again.read_text())["bindings"]["result_database"] == before
        reloaded = PipelineJob.load(again)
        with pytest.raises(RuntimeError):
            _ = reloaded.nodes["out"].as_source()


class TestClearingTheResultDatabase:
    """M4: ``bind(result_database=None)`` puts the results back in the job's database."""

    def test_before_a_run(self) -> None:
        db = InMemoryArrowDatabase()
        results = InMemoryArrowDatabase()
        job = one_step_job("a", db, [1.0], result_database=results)
        job.bind(result_database=None)
        assert job.result_database is None
        _ = job.run()
        assert table_paths(results) == []
        assert any(p.startswith("a/_result/") for p in table_paths(db))

    def test_after_a_run(self) -> None:
        db = InMemoryArrowDatabase()
        results = InMemoryArrowDatabase()
        job = one_step_job("a", db, [1.0], result_database=results)
        _ = job.run()
        calls.clear()
        job.bind(result_database=None)
        assert job.result_database is None
        _ = job.run()
        # The job's own database held no result: computed again, stored there.
        assert calls == [("double", 1.0)]
        assert any(p.startswith("a/_result/") for p in table_paths(db))
        assert rows(job, "out", "doubled") == [2.0]

    def test_leaving_it_out_keeps_the_binding(self) -> None:
        results = InMemoryArrowDatabase()
        job = one_step_job("a", InMemoryArrowDatabase(), [1.0], result_database=results)
        job.bind(sources={"s": make_source([1.0])})
        assert job.result_database is results


class TestACachedSourceIsNotTheJobs:
    """Docs item: a ``CachedSource`` keeps its cache where it was created to, for any job."""

    def test_two_jobs_share_one_cached_source(self) -> None:
        from orcapod.sources import CachedSource

        db = InMemoryArrowDatabase()
        cache = InMemoryArrowDatabase()
        cached = CachedSource(
            make_source([1.0, 2.0]), cache_database=cache, cache_path_prefix=("c",)
        )
        for name in ("a", "b"):
            job = PipelineJob(
                name=name, database=db, result_database=InMemoryArrowDatabase()
            )
            with job:
                double.pod(cached, label="out")
            _ = job.run()
        cache_tables = table_paths(cache)
        assert len(cache_tables) == 1 and cache_tables[0].startswith("c/")
        assert not any(p.endswith("sdb_v1") for p in table_paths(db))


# B2 and B3 were fixed on main for every case by ITL-1412: these check that the order
# holds across two databases (a separate result database).


def _dangling_record_ids(pipeline_root: pathlib.Path, result_root: pathlib.Path) -> int:
    """Pipeline record ids, read through fresh handles, with no committed result."""
    from orcapod.system_constants import constants

    pipeline = DeltaTableDatabase(pipeline_root)
    results = DeltaTableDatabase(result_root)
    result_ids: set[Any] = set()
    for path in results.list_table_paths(()):
        if path[-1] == "rdb_v1":
            stored = results.get_all_records(path, record_id_column="__rid")
            assert stored is not None
            result_ids.update(stored.column("__rid").to_pylist())
    record_ids: set[Any] = set()
    for path in pipeline.list_table_paths(()):
        if path[-1] == "pdb_v1":
            stored = pipeline.get_all_records(path)
            assert stored is not None
            record_ids.update(stored.column(constants.DATA_RECORD_ID).to_pylist())
    return len(record_ids - result_ids)


class TestCommitOrderAcrossTwoDatabases:
    """B2 (async) and B3: results are committed before the records naming them."""

    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_no_record_is_published_before_its_result(
        self, tmp_path: pathlib.Path, mode: str
    ) -> None:
        from unittest.mock import patch

        pipeline_root = tmp_path / "pipeline"
        result_root = tmp_path / "results"
        job = PipelineJob(
            name="two",
            database=DeltaTableDatabase(pipeline_root),
            result_database=DeltaTableDatabase(result_root),
        )
        with job:
            stream = make_source([0.0, 1.0, 2.0, 3.0])
            double.pod(stream, label="first")
            double_as_text.pod(stream, label="second")
        flush = DeltaTableDatabase.flush
        dangling: list[int] = []

        def checked_flush(self: DeltaTableDatabase) -> None:
            flush(self)
            if self.to_config()["root_uri"] == str(pipeline_root):
                dangling.append(_dangling_record_ids(pipeline_root, result_root))

        orchestrator = (
            SyncPipelineOrchestrator()
            if mode == "sync"
            else AsyncPipelineOrchestrator()
        )
        with patch.object(DeltaTableDatabase, "flush", checked_flush):
            _ = job.run(orchestrator=orchestrator)
        assert dangling and not any(dangling)

    def test_a_failed_result_commit_publishes_no_record(
        self, tmp_path: pathlib.Path, orchestrator: Orchestrator
    ) -> None:
        from unittest.mock import patch

        from orcapod.errors import OrcapodDatabaseWriteError

        pipeline_root = tmp_path / "pipeline"
        result_root = tmp_path / "results"
        job = one_step_job(
            "bad",
            DeltaTableDatabase(pipeline_root),
            [7.0, 8.0],
            result_database=DeltaTableDatabase(result_root),
        )
        flush_by_key = DeltaTableDatabase._flush_by_key  # pyright: ignore[reportPrivateUsage]  # fault injection

        def failing(self: DeltaTableDatabase, record_key: str) -> None:
            if self.to_config()["root_uri"] == str(result_root):
                raise OSError("result store unavailable")
            flush_by_key(self, record_key)

        with patch.object(DeltaTableDatabase, "_flush_by_key", failing):
            with pytest.raises(OrcapodDatabaseWriteError) as info:
                _ = job.run(orchestrator=orchestrator)
        published = DeltaTableDatabase(pipeline_root)
        assert [p for p in published.list_table_paths(()) if p[-1] == "pdb_v1"] == []
        assert "result" in str(info.value) and "rdb_v1" in "/".join(
            info.value.record_path
        )


# ---------------------------------------------------------------------------
# Second review round (PR 507): R1-R5.
# ---------------------------------------------------------------------------


class TestForeignPipelineRowsFromAnotherHandle:
    """R1: completions marked with another data context, written to the node's
    PIPELINE table through another handle after its first run, are found before
    anything is delivered: the run refreshes every table the check reads.
    """

    @pytest.mark.parametrize("separate", [True, False])
    def test_a_later_run_refuses_before_delivering(
        self, tmp_path: pathlib.Path, separate: bool, orchestrator: Orchestrator
    ) -> None:
        import uuid

        pipeline = DeltaTableDatabase(tmp_path / "db")
        results = InMemoryArrowDatabase() if separate else None
        job = one_step_job("ctx", pipeline, [1.0, 2.0], notify, results)
        _ = job.run(orchestrator=orchestrator)
        delivered.clear()

        (path,) = [p for p in pipeline.list_table_paths(()) if p[-1] == "pdb_v1"]
        stored = pipeline.get_all_records(path, record_id_column="__rid")
        assert stored is not None
        context = stored.schema.get_field_index("__data_context")
        foreign = stored.set_column(
            context,
            stored.schema.field(context),
            pa.array(["std:v0.9:other"] * stored.num_rows, pa.large_string()),
        )
        ids = foreign.schema.get_field_index("__rid")
        foreign = foreign.set_column(
            ids,
            foreign.schema.field(ids),
            pa.array(
                [uuid.uuid4().bytes for _ in range(foreign.num_rows)],
                foreign.schema.field(ids).type,
            ),
        )
        other_handle = DeltaTableDatabase(tmp_path / "db")
        other_handle.add_records(path, foreign, record_id_column="__rid")
        other_handle.flush()

        job.bind(sources={"s": make_source([1.0, 2.0, 3.0])})
        with pytest.raises(SideEffectLogDataContextError):
            _ = job.run(orchestrator=orchestrator)
        assert delivered == []


class TestStoredViewsReadTheStoreNow:
    """R2: a read of what is stored reads the store's current version, per read call."""

    def test_both_views_before_and_after_an_external_append(
        self, tmp_path: pathlib.Path
    ) -> None:
        pipeline_root = tmp_path / "db"
        result_root = tmp_path / "results"
        warm = DeltaTableDatabase(result_root)
        # The warm handle has loaded the result table before the writer commits.
        _ = one_step_job(
            "warm", DeltaTableDatabase(pipeline_root), [1.0], result_database=warm
        ).run()

        def write(xs: list[float]) -> PipelineJob:
            job = one_step_job(
                "load",
                DeltaTableDatabase(pipeline_root),
                xs,
                result_database=DeltaTableDatabase(result_root),
                source_id="other",
            )
            _ = job.run()
            return job

        written = write([2.0, 3.0])
        written.save(tmp_path / "job.json")
        loaded = PipelineJob.load(tmp_path / "job.json", result_database=warm)
        node = loaded.nodes["out"]

        def views() -> tuple[list[Any], list[Any]]:
            records = node.get_all_records()
            assert records is not None
            return (
                sorted(node.as_source().as_table().column("doubled").to_pylist()),
                sorted(records.column("doubled").to_pylist()),
            )

        assert views() == ([4.0, 6.0], [4.0, 6.0])
        _ = write([2.0, 3.0, 5.0])  # appended through other handles
        assert views() == ([4.0, 6.0, 10.0], [4.0, 6.0, 10.0])


class TestRefreshIsPerNodePerRun:
    """R3: each node refreshes its own tables once per run, never per row.

    Two nodes of one job that share a result table each look at it once (the
    documented cost); a job run does not share the refresh between nodes.
    """

    def test_two_nodes_sharing_a_result_table(self, tmp_path: pathlib.Path) -> None:
        from unittest.mock import patch

        results = DeltaTableDatabase(tmp_path / "results")
        job = PipelineJob(
            name="two", database=InMemoryArrowDatabase(), result_database=results
        )
        xs = [float(x) for x in range(50)]
        with job:
            double.pod(make_source(xs, "a"), label="first")
            double.pod(make_source(xs, "b"), label="second")
        refresh = DeltaTableDatabase.refresh_table
        refreshed: list[tuple[str, ...]] = []

        def counted(self: DeltaTableDatabase, record_path: tuple[str, ...]) -> None:
            refreshed.append(record_path)
            refresh(self, record_path)

        with patch.object(DeltaTableDatabase, "refresh_table", counted):
            _ = job.run()
        (result_table,) = {p for p in refreshed if p[-1] == "rdb_v1"}
        assert refreshed.count(result_table) == 2  # one per node, not per row


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="ITL-1413: a Delta table deleted and recreated at the same path with the "
    "same version number is not seen by a handle that loaded the old one",
)
def test_a_recreated_table_at_the_same_version_is_seen(tmp_path: pathlib.Path) -> None:
    """R4: the refresh compares versions, not the table's identity (limit, ITL-1413)."""
    import shutil

    import deltalake

    warm = DeltaTableDatabase(tmp_path / "shared")
    _ = one_step_job("warm", InMemoryArrowDatabase(), [1.0], result_database=warm).run()
    replacement = DeltaTableDatabase(tmp_path / "replacement")
    _ = one_step_job(
        "write", InMemoryArrowDatabase(), [1.0], double_changed, replacement
    ).run()
    (path,) = [p for p in warm.list_table_paths(()) if p[-1] == "rdb_v1"]
    old = tmp_path / "shared" / "/".join(path)
    new = tmp_path / "replacement" / "/".join(path)
    assert deltalake.DeltaTable(str(old)).version() == 0
    assert deltalake.DeltaTable(str(new)).version() == 0
    # Recreate the table at the old path (its old data files left behind).
    _ = shutil.move(str(old / "_delta_log"), str(tmp_path / "old_log"))
    for item in new.iterdir():
        _ = shutil.move(str(item), str(old / item.name))
    calls.clear()
    job = one_step_job("read", InMemoryArrowDatabase(), [1.0], double_changed, warm)
    _ = job.run()
    assert calls == []
    assert rows(job, "out", "doubled") == [3.0]  # the stored value now at that path


class TestTwoOverlappingJobs:
    """R5: a row another job commits after this node refreshed is not seen in this run.

    The window opens when the node refreshes its tables at its start and closes at
    its next refresh (its next run or stored read) or its own commit of that table.
    A side-effecting function can be delivered by each of two overlapping jobs.
    This pins that documented behaviour; reserving an input is not designed yet.
    """

    @pytest.mark.parametrize("separate", [True, False])
    def test_a_ctx_action_committed_during_the_run_is_delivered_again(
        self, tmp_path: pathlib.Path, separate: bool, orchestrator: Orchestrator
    ) -> None:
        from orcapod.pipeline.observer import NoOpObserver

        warm = DeltaTableDatabase(tmp_path / "store")
        writer = DeltaTableDatabase(tmp_path / "store")

        def job(handle: DeltaTableDatabase, name: str, xs: list[float]) -> PipelineJob:
            if separate:
                return one_step_job(
                    name, InMemoryArrowDatabase(), xs, notify, handle, source_id=name
                )
            return one_step_job("same", handle, xs, notify, source_id=name)

        _ = job(warm, "warm", [1.0]).run(orchestrator=orchestrator)
        reader = job(warm, "reader", [3.0])
        other = job(writer, "writer", [3.0])

        class CommitsAfterTheRefresh(NoOpObserver):
            committed: bool = False

            def contextualize(self, *identity_path: str) -> CommitsAfterTheRefresh:
                return self

            def on_node_start(
                self, node_label: str, node_hash: str, tag_schema: Any = None
            ) -> None:
                if node_label == "out" and not self.committed:
                    self.committed = True
                    _ = other.run()

        _ = reader.run(orchestrator=orchestrator, observer=CommitsAfterTheRefresh())
        assert sorted(delivered) == [1.0, 3.0, 3.0]
