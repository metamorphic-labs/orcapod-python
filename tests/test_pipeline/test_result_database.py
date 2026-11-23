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
