"""Report skipped nodes and reopen saved stores without creating directories."""

from pathlib import Path

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes import FunctionJobNode, OperatorJobNode
from orcapod.core.operators import Join
from orcapod.databases import (
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    NoOpArrowDatabase,
)
from orcapod.pipeline.run_summary import RunSummary
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import CSVSource, DictSource


@function_pod(output_keys="total")
def total(x: int | None, y: int | None) -> int:
    """Add the two data values."""
    return (x or 0) + (y or 0)


def save_job(
    database: ArrowDatabaseProtocol,
    *,
    result_database: ArrowDatabaseProtocol | None = None,
) -> Path:
    """Save a job whose file-backed source can be restored independently."""
    _ = Path("rows.csv").write_text("id,group,x,y\n1,a,5,6\n")
    file = CSVSource(
        "rows.csv",
        tag_columns=["id", "group"],
        tag_only_columns=True,
        source_id="file",
    )
    memory = DictSource(
        [{"id": 1, "group": "a", "x": 2, "y": 3}],
        tag_columns=["id", "group"],
        tag_only_columns=True,
        source_id="memory",
    )
    job = PipelineJob(name="small", database=database, result_database=result_database)
    with job:
        calculated = total.pod(memory, label="memory_total")
        _ = Join()(calculated, file, label="joined")
        _ = total.pod(file, label="file_total")
    _ = job.run()
    path = Path("job.json")
    job.save(path)
    return path


async def load_only(job: PipelineJob, *, asynchronous: bool) -> RunSummary:
    """Exercise both public job run forms."""
    if asynchronous:
        return await job.run_async(load_only=True)
    return job.run(load_only=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("durable", [False, True], ids=["memory", "delta"])
async def test_runs_report_excluded_nodes_and_their_unbound_slots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, asynchronous: bool, durable: bool
) -> None:
    """A skipped branch and a held branch report different outcomes."""
    monkeypatch.chdir(tmp_path)
    store = DeltaTableDatabase("store") if durable else InMemoryArrowDatabase()
    path = save_job(store)
    loaded = PipelineJob.load(path, database=None if durable else store)

    summary = await load_only(loaded, asynchronous=asynchronous)
    expected_skips = {
        "joined": ("memory",),
        "memory": ("memory",),
        "memory_total": ("memory",),
    }
    assert loaded.unbound_sources == ["memory"]
    file_total = loaded.nodes["file_total"]
    memory_total = loaded.nodes["memory_total"]
    joined = loaded.nodes["joined"]
    assert isinstance(file_total, FunctionJobNode)
    assert isinstance(memory_total, FunctionJobNode)
    assert isinstance(joined, OperatorJobNode)
    assert file_total.as_table().num_rows == 1
    assert memory_total.as_table().num_rows == 0
    assert joined.as_table().num_rows == 0
    assert loaded.last_run is summary
    assert summary.ran_nothing is False
    assert summary.skipped_unbound_sources == expected_skips
    assert list(summary.skipped_unbound_sources) == sorted(expected_skips)
    assert set(summary.nodes) == {"file_total"}
    assert summary.nodes["file_total"].reused == 1
    assert summary.nodes["file_total"].held == 0
    assert str(summary) == (
        "Load-only run of job 'small': every available input had a stored result.\n"
        "  'joined': skipped (unbound sources: 'memory')\n"
        "  'memory': skipped (unbound sources: 'memory')\n"
        "  'memory_total': skipped (unbound sources: 'memory')\n"
        "  file_total: 1 row: 1 loaded"
    )
    assert (
        "&#x27;joined&#x27;: skipped (unbound sources: &#x27;memory&#x27;)"
        in summary._repr_html_()  # pyright: ignore[reportPrivateUsage]  # notebook display protocol
    )

    held = await load_only(loaded, asynchronous=asynchronous)
    assert held.ran_nothing is True
    assert held.skipped_unbound_sources == expected_skips
    assert held.nodes["file_total"].held == 1
    assert held.nodes["file_total"].reused == 0
    assert str(held) == (
        "Load-only run of job 'small': no nodes ran.\n"
        "  'joined': skipped (unbound sources: 'memory')\n"
        "  'memory': skipped (unbound sources: 'memory')\n"
        "  'memory_total': skipped (unbound sources: 'memory')\n"
        "  file_total: 1 row: held from the previous run"
    )

    _ = Path("rows.csv").rename("moved.csv")
    blocked = PipelineJob.load(path, database=None if durable else store)
    skipped = await load_only(blocked, asynchronous=asynchronous)
    assert skipped.ran_nothing is True
    assert not skipped.nodes
    assert skipped.skipped_unbound_sources == {
        "file": ("file",),
        "file_total": ("file",),
        "joined": ("file", "memory"),
        "memory": ("memory",),
        "memory_total": ("memory",),
    }
    assert str(skipped) == (
        "Load-only run of job 'small': no nodes ran.\n"
        "  'file': skipped (unbound sources: 'file')\n"
        "  'file_total': skipped (unbound sources: 'file')\n"
        "  'joined': skipped (unbound sources: 'file', 'memory')\n"
        "  'memory': skipped (unbound sources: 'memory')\n"
        "  'memory_total': skipped (unbound sources: 'memory')"
    )


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
@pytest.mark.parametrize("binding", ["database", "result_database"])
def test_loading_a_moved_delta_store_does_not_create_its_old_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    binding: str,
) -> None:
    """A present store reads its rows, and a missing root needs an override."""
    monkeypatch.chdir(tmp_path)
    store = DeltaTableDatabase("store")
    results = DeltaTableDatabase("results") if binding == "result_database" else None
    path = save_job(store, result_database=results)

    present = PipelineJob.load(path)
    present_node = present.nodes["file_total"]
    assert isinstance(present_node, FunctionJobNode)
    assert present_node.as_source().as_table().num_rows == 1
    assert not caplog.records

    original = Path("results" if results is not None else "store")
    moved = original.rename("moved")
    missing = PipelineJob.load(path)
    assert not original.exists()
    assert "Could not reconstruct" in caplog.text
    assert f"{binding}=..." in caplog.text
    if results is None:
        assert missing.database is None
        restored = PipelineJob.load(path, database=DeltaTableDatabase(moved))
    else:
        assert missing.result_database is None
        restored = PipelineJob.load(path, result_database=DeltaTableDatabase(moved))
    restored_node = restored.nodes["file_total"]
    assert isinstance(restored_node, FunctionJobNode)
    assert restored_node.as_source().as_table().num_rows == 1
    assert not original.exists()


@pytest.mark.parametrize("store_type", [InMemoryArrowDatabase, NoOpArrowDatabase])
def test_loading_a_non_filesystem_store_leaves_the_working_directory_alone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    store_type: type[InMemoryArrowDatabase] | type[NoOpArrowDatabase],
) -> None:
    """The other built-in job stores have no filesystem root."""
    monkeypatch.chdir(tmp_path)
    path = save_job(store_type())
    before = set(tmp_path.iterdir())
    loaded = PipelineJob.load(path)
    assert isinstance(loaded.database, store_type)
    assert set(tmp_path.iterdir()) == before
