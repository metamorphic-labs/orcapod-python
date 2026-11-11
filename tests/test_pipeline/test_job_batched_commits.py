"""Function nodes inside ``PipelineJob.run`` commit their writes in batches (ITL-919).

``PipelineJob.run(commit_every_rows=N)`` makes each function node commit its results and
pipeline records every N rows and when it finishes, instead of after every row. These
tests check, on a ``DeltaTableDatabase`` whose table history counts commits:

- the commit count (one per N rows per table, appends only),
- the option's validation,
- that a node or cached pod used on its own still commits after every row,
- a failed commit: ``StoreWriteError`` naming the node and no row, earlier batches kept,
- that a failing node commits its ephemeral results before the run's error propagates,
- that each row's base entry id is computed once,
- that a node's records read right after the run are all there (ITL-930).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import deltalake
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.databases.storage_utils import delta_to_pyarrow_dataset
from orcapod.errors import StoreWriteError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.sources import DictSource
from orcapod.types import NodeConfig

ORCHESTRATORS: dict[str, Callable[..., Any]] = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Return twice ``x``."""
    return 2 * x


@function_pod(output_keys="y")
def double_until_7(x: int) -> int:
    """Return twice ``x``, failing for ``x == 7``."""
    if x == 7:
        raise ValueError("x is 7")
    return 2 * x


def build_job(store: Any, n_rows: int, pod: Any = double) -> PipelineJob:
    """A job with one function node ``dbl`` over ``n_rows`` rows."""
    job = PipelineJob(name="batched", store=store)
    with job:
        source = DictSource(
            data=[{"k": i, "x": i} for i in range(n_rows)],
            tag_columns=["k"],
            source_id="src",
        )
        pod.pod(source, label="dbl")
    return job


def table_dirs(root: Path) -> dict[str, Path]:
    """The Delta tables under ``root``, keyed ``result`` or ``pipeline``."""
    tables = {log.parent for log in root.rglob("_delta_log")}
    by_kind = {
        ("result" if "_result" in t.relative_to(root).parts else "pipeline"): t
        for t in tables
    }
    assert len(by_kind) == len(tables) == 2, sorted(tables)
    return by_kind


def operations(table: Path) -> list[str]:
    """The table's commit operations, oldest first."""
    history = deltalake.DeltaTable(str(table)).history()
    return [str(entry["operation"]) for entry in reversed(history)]


def stored_rows(table: Path) -> int:
    """The number of rows committed to ``table``, read from a fresh handle."""
    return delta_to_pyarrow_dataset(deltalake.DeltaTable(str(table))).count_rows()


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize(
    ("n_rows", "every", "commits"), [(250, 100, 3), (250, None, 3), (40, 1000, 1)]
)
def test_each_table_is_committed_once_per_batch(
    tmp_path: Path, orchestrator: str, n_rows: int, every: int | None, commits: int
) -> None:
    """250 rows at 100 per commit (the default) give 3 commits per table, all appends."""
    job = build_job(DeltaTableDatabase(tmp_path), n_rows)
    kwargs = {} if every is None else {"commit_every_rows": every}
    job.run(orchestrator=ORCHESTRATORS[orchestrator](), **kwargs)
    for kind, table in table_dirs(tmp_path).items():
        assert operations(table) == ["WRITE"] * commits, kind
        assert stored_rows(table) == n_rows, kind
    assert job.nodes["dbl"].as_table().num_rows == n_rows


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "10", None])
def test_commit_every_rows_must_be_a_positive_integer(
    tmp_path: Path, value: Any
) -> None:
    """Anything but a positive ``int`` is refused before the run starts."""
    job = build_job(DeltaTableDatabase(tmp_path), 3)
    with pytest.raises(
        ValueError, match="commit_every_rows must be a positive integer"
    ):
        job.run(commit_every_rows=value)
    assert not list(tmp_path.rglob("_delta_log"))


def test_a_node_run_on_its_own_commits_every_row(tmp_path: Path) -> None:
    """Outside ``PipelineJob.run`` a node's results are committed row by row."""
    job = build_job(DeltaTableDatabase(tmp_path), 5)
    job.nodes["dbl"].run()
    result_table = table_dirs(tmp_path)["result"]
    assert operations(result_table) == ["WRITE"] * 5


def test_a_cached_pod_used_on_its_own_commits_every_row(tmp_path: Path) -> None:
    """A ``CachedFunctionPod`` used directly commits each stored result."""
    cached = CachedFunctionPod(double.pod, result_database=DeltaTableDatabase(tmp_path))
    source = DictSource(
        data=[{"k": i, "x": i} for i in range(4)], tag_columns=["k"], source_id="src"
    )
    for tag, data in source.iter_data():
        cached.process_data(tag, data)
    (table,) = {log.parent for log in tmp_path.rglob("_delta_log")}
    assert operations(table) == ["WRITE"] * 4


class FailingCommits:
    """A ``DeltaTableDatabase`` whose commits fail after the first ``ok`` succeed.

    A node's commit flushes its result table (``flush_batch``) and then the store
    (``flush``); every view shares the counts.
    """

    def __init__(self, inner: DeltaTableDatabase, state: dict[str, int]) -> None:
        """Wrap ``inner``; ``state["ok"]`` commit calls succeed, later ones fail."""
        self._inner = inner
        self._state = state

    def at(self, *path_components: str) -> FailingCommits:
        """A view sharing the commit counts."""
        return FailingCommits(self._inner.at(*path_components), self._state)

    def _commit(self, commit: Callable[[], None]) -> None:
        self._state["calls"] += 1
        if self._state["calls"] > self._state["ok"]:
            raise OSError("disk unavailable while committing")
        commit()

    def flush(self) -> None:
        """Commit every pending write, unless commits fail by now."""
        self._commit(self._inner.flush)

    def flush_batch(self, record_path: tuple[str, ...]) -> None:
        """Commit one table's pending writes, unless commits fail by now."""
        self._commit(lambda: self._inner.flush_batch(record_path))

    def __getattr__(self, name: str) -> Any:
        """Delegate everything else."""
        return getattr(self._inner, name)


@pytest.mark.parametrize("error_policy", ["continue", "fail_fast"])
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_failed_commit_names_the_node_and_keeps_earlier_batches(
    tmp_path: Path, orchestrator: str, error_policy: str
) -> None:
    """The second batch's commit fails: the run raises, the first batch is kept."""
    # The first commit is two calls: the result table, then the store.
    store = FailingCommits(DeltaTableDatabase(tmp_path), {"calls": 0, "ok": 2})
    job = build_job(store, 250)
    with pytest.raises(StoreWriteError) as info:
        job.run(
            orchestrator=ORCHESTRATORS[orchestrator](error_policy=error_policy),
            commit_every_rows=100,
        )
    err = info.value
    assert err.flush is True
    assert err.node_label == "dbl"
    assert err.tag is None
    assert "node 'dbl'" in str(err) and "for row" not in str(err)
    assert isinstance(err.__cause__, OSError)
    # The job's flush after the failure retried the batch and failed again.
    assert any("Flushing pending writes" in n for n in getattr(err, "__notes__", []))
    tables = table_dirs(tmp_path)
    for kind, table in tables.items():
        assert operations(table) == ["WRITE"], kind
    assert stored_rows(tables["pipeline"]) == 100
    if orchestrator == "sync":
        assert stored_rows(tables["result"]) == 100
    else:
        # Results go first: results of rows still on their way to being recorded
        # when the first batch was committed are stored too.
        assert 100 <= stored_rows(tables["result"]) < 150


def ephemeral_rows(path: Path) -> int:
    """Rows committed to the ephemeral store at ``path`` (read afresh)."""
    (table,) = {log.parent for log in path.rglob("_delta_log")}
    return stored_rows(table)


def test_a_node_failing_on_a_row_commits_its_ephemeral_results(
    tmp_path: Path,
) -> None:
    """A function error under ``fail_fast``: rows before it are in the ephemeral store."""
    job = build_job(DeltaTableDatabase(tmp_path / "store"), 10, pod=double_until_7)
    job.set_ephemeral_store(DeltaTableDatabase(tmp_path / "ephemeral"))
    job.apply_node_config(NodeConfig(is_result_ephemeral=True))
    # The sync orchestrator only: the async one does not apply error_policy and
    # skips a row whose function raised.
    with pytest.raises(ValueError, match="x is 7"):
        job.run(
            orchestrator=SyncPipelineOrchestrator(error_policy="fail_fast"),
            commit_every_rows=100,
        )
    assert ephemeral_rows(tmp_path / "ephemeral") == 7  # rows 0 to 6


class FailPipelineRecords:
    """A store whose pipeline-record writes fail after the first ``ok``."""

    def __init__(self, inner: Any, state: dict[str, int]) -> None:
        """Wrap ``inner``; every view shares ``state``."""
        self._inner = inner
        self._state = state

    def at(self, *path_components: str) -> FailPipelineRecords:
        """A view sharing the write count."""
        return FailPipelineRecords(self._inner.at(*path_components), self._state)

    def add_record(self, record_path: tuple[str, ...], *args: Any, **kwargs: Any):
        """Fail pipeline-record writes after the first ``ok``."""
        self._state["n"] += 1
        if self._state["n"] > self._state["ok"]:
            raise OSError("disk unavailable")
        return self._inner.add_record(record_path, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        """Delegate everything else."""
        return getattr(self._inner, name)


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_node_failing_on_a_store_write_commits_its_ephemeral_results(
    tmp_path: Path, orchestrator: str
) -> None:
    """A failed pipeline-record write: earlier rows' results are in the ephemeral store."""
    # With ephemeral results, the job store only receives pipeline records.
    store = FailPipelineRecords(
        DeltaTableDatabase(tmp_path / "store"), {"n": 0, "ok": 5}
    )
    job = build_job(store, 50)
    job.set_ephemeral_store(DeltaTableDatabase(tmp_path / "ephemeral"))
    job.apply_node_config(NodeConfig(is_result_ephemeral=True))
    with pytest.raises(StoreWriteError) as info:
        job.run(orchestrator=ORCHESTRATORS[orchestrator](), commit_every_rows=100)
    assert info.value.node_label == "dbl"
    committed = ephemeral_rows(tmp_path / "ephemeral")
    if orchestrator == "sync":
        assert committed == 6  # rows 0 to 5: row 5's record was the failed write
    else:
        assert committed >= 6  # results of rows computed concurrently too


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_base_entry_id_is_computed_once_per_row(
    orchestrator: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each row's base entry id is computed once, not once per use."""
    # Counts calls of a public node method through a class-level spy; the test is
    # about how often the node computes the id.
    calls = {"n": 0}
    original = FunctionJobNode.compute_base_entry_id

    def counting(self: FunctionJobNode, tag: Any, input_data: Any) -> bytes:
        calls["n"] += 1
        return original(self, tag, input_data)

    monkeypatch.setattr(FunctionJobNode, "compute_base_entry_id", counting)
    job = build_job(InMemoryArrowDatabase(), 20)
    job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert calls["n"] == 20


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("source_id", ["s", None])
def test_records_read_right_after_the_run_are_complete(
    tmp_path: Path, orchestrator: str, source_id: str | None
) -> None:
    """``get_all_records`` right after ``run`` returns every row (ITL-930)."""
    job = PipelineJob(name="itl930", store=DeltaTableDatabase(tmp_path))
    with job:
        source = DictSource(
            data=[{"k": "b", "x": 2}, {"k": "c", "x": 3}],
            tag_columns=["k"],
            source_id=source_id,
        )
        double.pod(source, label="double")
    job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    records = job.nodes["double"].get_all_records()
    assert records is not None and records.num_rows == 2
    assert job.nodes["double"].as_table().num_rows == 2
