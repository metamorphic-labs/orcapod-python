"""What each table-backed source does when a run checks it for new data (ITL-1102).

``check_for_new_data()`` is called once per source at the start of every run. A source
that finds new data takes it in and moves its modified time; one that finds nothing
moves nothing. Tested through the source and through ``PipelineJob.run()`` on both
orchestrators.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import deltalake
import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.databases.spiraldb_nullability import write_nullability_record
from orcapod.errors import SourceCheckFailedWarning
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DeltaTableSource, SpiralDBTableSource
from tests.test_databases import fake_pyspiral

CALLS: list[int] = []
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


@function_pod(output_keys="y")
def inc(x: int) -> int:
    """Return ``x + 1``."""
    CALLS.append(x)
    return x + 1


@pytest.fixture(autouse=True)
def _reset() -> None:
    CALLS.clear()


_SCHEMA = pa.schema(
    [pa.field("k", pa.int64(), False), pa.field("x", pa.int64(), False)]
)


def _rows(keys: list[int]) -> pa.Table:
    return pa.table(
        {
            "k": pa.array(keys, pa.int64()),
            "x": pa.array([10 * k for k in keys], pa.int64()),
        },
        schema=_SCHEMA,
    )


def _job(source: Any) -> PipelineJob:
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        inc.pod(source, label="rowwise")
    return job


def _run(job: PipelineJob, orchestrator: str) -> Any:
    return job.run(orchestrator=ORCHESTRATORS[orchestrator]())


def _keys(source: Any) -> list[int]:
    return sorted(k for k in source.as_table().column("k").to_pylist() if k is not None)


def _ys(job: PipelineJob) -> list[int]:
    return sorted(r["y"] for r in job.nodes["rowwise"].as_table().to_pylist())


# ---------------------------------------------------------------------------
# DeltaTableSource
# ---------------------------------------------------------------------------


@pytest.fixture()
def delta_path(tmp_path: Path) -> Path:
    path = tmp_path / "table"
    deltalake.write_deltalake(str(path), _rows([0, 1, 2]))
    return path


def _delta(path: Path) -> DeltaTableSource:
    return DeltaTableSource(path, tag_columns="k", record_id_column="k")


def test_delta_unchanged_finds_nothing(delta_path: Path) -> None:
    source = _delta(delta_path)
    before = source.last_modified
    assert source.check_for_new_data() is False
    assert source.last_modified == before


def test_delta_appended_rows_are_taken_in(delta_path: Path) -> None:
    source = _delta(delta_path)
    before = source.last_modified
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    assert source.check_for_new_data() is True
    assert source.last_modified is not None and before is not None
    assert source.last_modified > before
    assert _keys(source) == [0, 1, 2, 3]


def test_delta_new_version_with_the_same_rows_changes_nothing(
    delta_path: Path,
) -> None:
    """A version that only compacts files (OPTIMIZE) holds the same rows."""
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    source = _delta(delta_path)
    before = source.last_modified
    deltalake.DeltaTable(str(delta_path)).optimize.compact()
    assert source.check_for_new_data() is False
    assert source.last_modified == before


def test_delta_changed_columns_fail_the_check(delta_path: Path) -> None:
    source = _delta(delta_path)
    wider = _rows([3]).append_column(
        pa.field("z", pa.int64(), False), pa.array([1], pa.int64())
    )
    deltalake.write_deltalake(
        str(delta_path), wider, mode="append", schema_mode="merge"
    )
    with pytest.raises(ValueError, match="changed its columns"):
        source.check_for_new_data()
    assert _keys(source) == [0, 1, 2]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_delta_job_runs_only_the_new_rows(delta_path: Path, orchestrator: str) -> None:
    job = _job(_delta(delta_path))
    _run(job, orchestrator)
    CALLS.clear()
    assert _run(job, orchestrator).nodes["rowwise"].held == 3
    assert CALLS == []
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    summary = _run(job, orchestrator)
    assert CALLS == [30]
    assert summary.nodes["rowwise"].computed == 1
    assert _ys(job) == [1, 11, 21, 31]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_delta_failed_check_warns_and_runs_on_held_rows(
    delta_path: Path, orchestrator: str
) -> None:
    job = _job(_delta(delta_path))
    _run(job, orchestrator)
    CALLS.clear()
    wider = _rows([3]).append_column(
        pa.field("z", pa.int64(), False), pa.array([1], pa.int64())
    )
    deltalake.write_deltalake(
        str(delta_path), wider, mode="append", schema_mode="merge"
    )
    with pytest.warns(SourceCheckFailedWarning, match="changed its columns"):
        summary = _run(job, orchestrator)
    assert CALLS == []
    assert summary.unchecked_sources == ("table",)
    assert _ys(job) == [1, 11, 21]


# ---------------------------------------------------------------------------
# SpiralDBTableSource (disk-backed pyspiral fake; no live SpiralDB)
# ---------------------------------------------------------------------------

_PROJECT = "fake-project"
_KEYS = pa.schema([pa.field("k", pa.int64())])


@pytest.fixture()
def spiral(tmp_path: Path):
    """A fake project with table ``default.t`` holding k = 0, 1, 2."""
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        table = project.create_table("default.t", key_schema=_KEYS)
        table.write(_rows([0, 1, 2]))
        # SpiralDB reports every column nullable; the record says k and x are not.
        write_nullability_record(table, _SCHEMA)
        yield project


def _spiral(**kwargs: Any) -> SpiralDBTableSource:
    return SpiralDBTableSource(project_id=_PROJECT, table_name="t", **kwargs)


def test_spiral_unchanged_finds_nothing(spiral: Any) -> None:
    source = _spiral()
    before = source.last_modified
    assert source.check_for_new_data() is False
    assert source.last_modified == before


def test_spiral_new_version_is_pinned_without_reading_rows(spiral: Any) -> None:
    source = _spiral(max_rows=2)  # the table holds more rows than one read may return
    before = source.last_modified
    spiral.table("default.t").write(_rows([3]))
    scans = len(fake_pyspiral.scan_log)
    assert source.check_for_new_data() is True  # no SourceTooLargeError: no row read
    assert len(fake_pyspiral.scan_log) == scans
    assert source.last_modified is not None and before is not None
    assert source.last_modified > before
    assert source.check_for_new_data() is False


def test_spiral_reads_the_new_version_after_the_check(spiral: Any) -> None:
    source = _spiral()
    spiral.table("default.t").write(_rows([3]))
    assert source.check_for_new_data() is True
    _ = source.as_table()
    assert (
        fake_pyspiral.scan_log[-1].kwargs["asof"]
        == spiral.table("default.t").last_modified_at()
    )


def test_spiral_explicit_asof_never_moves(spiral: Any) -> None:
    version = spiral.table("default.t").last_modified_at()
    source = _spiral(asof=version)
    before = source.last_modified
    spiral.table("default.t").write(_rows([3]))
    assert source.check_for_new_data() is False
    assert source.last_modified == before
    _ = source.as_table()
    assert fake_pyspiral.scan_log[-1].kwargs["asof"] == version


def test_spiral_recreated_table_fails_the_check(spiral: Any) -> None:
    source = _spiral()
    spiral.drop_table("default.t")
    recreated = spiral.create_table("default.t", key_schema=_KEYS)
    recreated.write(_rows([0, 1, 2, 3]))
    write_nullability_record(recreated, _SCHEMA)
    with pytest.raises(ValueError, match="dropped and created again"):
        source.check_for_new_data()


def test_spiral_changed_columns_fail_the_check(spiral: Any) -> None:
    source = _spiral()
    # Recording x as nullable changes the schema the source reads.
    write_nullability_record(
        spiral.table("default.t"),
        pa.schema([pa.field("k", pa.int64(), False), pa.field("x", pa.int64(), True)]),
    )
    with pytest.raises(ValueError, match="Build the source again"):
        source.check_for_new_data()


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_spiral_job_runs_only_the_new_rows(spiral: Any, orchestrator: str) -> None:
    job = _job(_spiral())
    _run(job, orchestrator)
    CALLS.clear()
    assert _run(job, orchestrator).nodes["rowwise"].held == 3
    assert CALLS == []
    spiral.table("default.t").write(_rows([3]))
    summary = _run(job, orchestrator)
    assert CALLS == [30]  # keyed record ids: the other rows' entries are stored
    assert summary.nodes["rowwise"].computed == 1
    assert _ys(job) == [1, 11, 21, 31]
