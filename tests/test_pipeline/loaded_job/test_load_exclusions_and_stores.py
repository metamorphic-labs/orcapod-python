"""Report skipped nodes and reopen saved stores without creating directories."""

import json
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
from orcapod.errors import OrcapodDatabaseContentWarning
from orcapod.pipeline.run_summary import RunSummary
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import CSVSource, DictSource

SOURCE_ORDERS = pytest.mark.parametrize(
    "memory_first", [False, True], ids=["file-first", "memory-first"]
)


@function_pod(output_keys="total")
def total(x: int | None, y: int | None) -> int:
    """Add the two data values."""
    return (x or 0) + (y or 0)


def save_job(
    database: ArrowDatabaseProtocol,
    *,
    result_database: ArrowDatabaseProtocol | None = None,
    cache_database: ArrowDatabaseProtocol | None = None,
    memory_first: bool = False,
) -> Path:
    """Save a job whose file-backed source can be restored independently."""
    _ = Path("rows.csv").write_text("id,group,x,y\n1,a,5,6\n2,b,7,8\n3,c,9,10\n")
    csv = CSVSource(
        "rows.csv",
        tag_columns=["id", "group"],
        tag_only_columns=True,
        source_id="file",
    )
    memory = DictSource(
        [
            {"id": 1, "group": "a", "x": 2, "y": 3},
            {"id": 2, "group": "b", "x": 4, "y": 5},
            {"id": 3, "group": "c", "x": 6, "y": 7},
        ],
        tag_columns=["id", "group"],
        tag_only_columns=True,
        source_id="memory",
    )
    file = csv if cache_database is None else csv.cached(cache_database=cache_database)
    recorded = PipelineJob(name="small")
    with recorded:
        calculated = total.pod(memory, label="memory_total")
        _ = Join()(calculated, file, label="joined")
        _ = total.pod(file, label="file_total")
    sources: dict[str, StreamProtocol] = {"file": file, "memory": memory}
    if memory_first:
        sources = dict(reversed(sources.items()))
    job = PipelineJob.from_pipeline(
        recorded.pipeline,
        database=database,
        result_database=result_database,
        sources=sources,
    )
    assert list(job.sources) == list(sources)
    _ = job.run()
    path = Path("job.json")
    job.save(path)
    # Save orders source configs by name. Exercise the loader with either JSON order.
    if memory_first:
        saved = json.loads(path.read_text())
        bindings = saved["bindings"]
        bindings["sources"] = {name: bindings["sources"][name] for name in sources}
        _ = path.write_text(json.dumps(saved))
    return path


async def load_only(job: PipelineJob, *, asynchronous: bool) -> RunSummary:
    """Exercise both public job run forms."""
    if asynchronous:
        return await job.run_async(load_only=True)
    return job.run(load_only=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("durable", [False, True], ids=["memory", "delta"])
@SOURCE_ORDERS
async def test_runs_report_excluded_nodes_and_their_unbound_slots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    asynchronous: bool,
    durable: bool,
    memory_first: bool,
) -> None:
    """A skipped branch and a held branch report different outcomes."""
    monkeypatch.chdir(tmp_path)
    store = DeltaTableDatabase("store") if durable else InMemoryArrowDatabase()
    path = save_job(store, memory_first=memory_first)
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
    assert file_total.as_table().num_rows == 3
    assert memory_total.as_table().num_rows == 0
    assert joined.as_table().num_rows == 0
    assert loaded.last_run is summary
    assert summary.ran_nothing is False
    assert summary.skipped_unbound_sources == expected_skips
    assert list(summary.skipped_unbound_sources) == sorted(expected_skips)
    assert set(summary.nodes) == {"file_total"}
    assert summary.nodes["file_total"].reused == 3
    assert summary.nodes["file_total"].held == 0
    assert str(summary) == (
        "Load-only run of job 'small': every available input had a stored result.\n"
        "  'joined': skipped (unbound sources: 'memory')\n"
        "  'memory': skipped (unbound sources: 'memory')\n"
        "  'memory_total': skipped (unbound sources: 'memory')\n"
        "  file_total: 3 rows: 3 loaded"
    )
    assert (
        "&#x27;joined&#x27;: skipped (unbound sources: &#x27;memory&#x27;)"
        in summary._repr_html_()  # pyright: ignore[reportPrivateUsage]  # notebook display protocol
    )

    held = await load_only(loaded, asynchronous=asynchronous)
    assert held.ran_nothing is True
    assert held.skipped_unbound_sources == expected_skips
    assert held.nodes["file_total"].held == 3
    assert held.nodes["file_total"].reused == 0
    assert str(held) == (
        "Load-only run of job 'small': no nodes ran.\n"
        "  'joined': skipped (unbound sources: 'memory')\n"
        "  'memory': skipped (unbound sources: 'memory')\n"
        "  'memory_total': skipped (unbound sources: 'memory')\n"
        "  file_total: 3 rows: held from the previous run"
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
@pytest.mark.parametrize("replacement", ["absent", "shared-cache", "file"])
@SOURCE_ORDERS
def test_loading_a_moved_delta_store_refuses_an_unusable_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    binding: str,
    replacement: str,
    memory_first: bool,
) -> None:
    """A present store reads its rows, and a missing root needs an override."""
    monkeypatch.chdir(tmp_path)
    store = DeltaTableDatabase("store")
    results = DeltaTableDatabase("results") if binding == "result_database" else None
    shared_cache = (results or store) if replacement == "shared-cache" else None
    path = save_job(
        store,
        result_database=results,
        cache_database=shared_cache,
        memory_first=memory_first,
    )

    present = PipelineJob.load(path)
    present_node = present.nodes["file_total"]
    assert isinstance(present_node, FunctionJobNode)
    assert present_node.as_source().as_table().num_rows == 3
    assert present.unbound_sources == ["memory"]
    assert not caplog.records

    original = Path("results" if results is not None else "store")
    moved = original.rename("moved")
    if replacement == "file":
        _ = original.write_text("regular file")
    missing = PipelineJob.load(path)
    if replacement == "file":
        assert original.read_text() == "regular file"
    else:
        assert not original.exists()
    assert "Could not reconstruct" in caplog.text
    assert f"{binding}=..." in caplog.text
    if shared_cache is not None:
        assert set(missing.unbound_sources) == {"file", "memory"}
        assert "Could not reconstruct source 'file' (cached)" in caplog.text
    if results is None:
        assert missing.database is None
        restored = PipelineJob.load(path, database=DeltaTableDatabase(moved))
    else:
        assert missing.result_database is None
        restored = PipelineJob.load(path, result_database=DeltaTableDatabase(moved))
    restored_node = restored.nodes["file_total"]
    assert isinstance(restored_node, FunctionJobNode)
    assert restored_node.as_source().as_table().num_rows == 3
    assert original.exists() == (replacement == "file")


@SOURCE_ORDERS
def test_loading_a_missing_source_cache_does_not_create_its_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    memory_first: bool,
) -> None:
    """Losing a source's own cache leaves its slot unbound and the job store open."""
    monkeypatch.chdir(tmp_path)
    cache = DeltaTableDatabase("cache")
    path = save_job(
        DeltaTableDatabase("store"), cache_database=cache, memory_first=memory_first
    )
    _ = Path("cache").rename("moved")

    loaded = PipelineJob.load(path)
    assert not Path("cache").exists()
    assert isinstance(loaded.database, DeltaTableDatabase)
    assert set(loaded.unbound_sources) == {"file", "memory"}
    assert "Could not reconstruct source 'file' (cached)" in caplog.text


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
def test_loading_a_delta_root_without_tables_opens_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A store root can exist before any Delta table has been written to it."""
    monkeypatch.chdir(tmp_path)
    path = save_job(DeltaTableDatabase("store"))
    root = Path("store")
    _ = root.rename("moved")
    root.mkdir()

    loaded = PipelineJob.load(path)
    assert isinstance(loaded.database, DeltaTableDatabase)
    assert not caplog.records
    node = loaded.nodes["file_total"]
    assert isinstance(node, FunctionJobNode)
    with pytest.warns(OrcapodDatabaseContentWarning):
        assert node.as_source().as_table().num_rows == 0
    assert list(root.iterdir()) == []


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
