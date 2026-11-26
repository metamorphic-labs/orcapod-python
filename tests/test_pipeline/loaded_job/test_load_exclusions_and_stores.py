"""Report skipped nodes and reopen saved stores without creating directories."""

import json
import sqlite3
import sys
import tempfile
from contextlib import closing
from inspect import signature
from pathlib import Path

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes import FunctionJobNode, OperatorJobNode
from orcapod.core.operators import Join, SelectDataColumns
from orcapod.databases import (
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    NoOpArrowDatabase,
)
from orcapod.errors import OrcapodDatabaseContentWarning
from orcapod.pipeline.run_summary import RunSummary
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.side_effects import SideEffectPod
from orcapod.sources import CSVSource, DictSource, SQLiteTableSource

SOURCE_ORDERS = pytest.mark.parametrize(
    "memory_first", [False, True], ids=["file-first", "memory-first"]
)


@function_pod(output_keys="total")
def total(x: int | None, y: int | None) -> int:
    """Add the two data values."""
    return (x or 0) + (y or 0)


def deliver(ctx: object, x: int | None, y: int | None) -> None:
    """Represent an external delivery without doing any I/O."""
    del ctx, x, y


def save_job(
    database: ArrowDatabaseProtocol,
    *,
    result_database: ArrowDatabaseProtocol | None = None,
    cache_database: ArrowDatabaseProtocol | None = None,
    memory_first: bool = False,
    file_source: StreamProtocol | None = None,
    extra_nodes: bool = False,
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
    file = file_source if file_source is not None else csv
    if cache_database is not None:
        file = csv.cached(cache_database=cache_database)
    recorded = PipelineJob(name="small")
    with recorded:
        calculated = total.pod(memory, label="memory_total")
        _ = Join()(calculated, file, label="joined")
        _ = total.pod(file, label="file_total")
        if extra_nodes:
            _ = SelectDataColumns("x")(file, label="selected")
            _ = SideEffectPod(deliver)(file, label="delivered")
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


@pytest.mark.parametrize("binding", ["database", "result_database", "cache"])
@pytest.mark.parametrize("absolute", [False, True], ids=["relative", "absolute"])
@SOURCE_ORDERS
def test_loading_a_scoped_delta_store_requires_a_directory_at_the_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    binding: str,
    absolute: bool,
    memory_first: bool,
) -> None:
    """The stored prefix must name a directory even when the root is present."""
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "root" if absolute else Path("root")
    scoped = DeltaTableDatabase(root).at("scope")
    path = save_job(
        scoped if binding == "database" else DeltaTableDatabase("store"),
        result_database=scoped if binding == "result_database" else None,
        cache_database=scoped if binding == "cache" else None,
        memory_first=memory_first,
    )
    _ = (root / "scope").rename(root / "moved")
    _ = (root / "scope").write_text("regular file")

    loaded = PipelineJob.load(path)
    if binding == "cache":
        assert "file" in loaded.unbound_sources
    elif binding == "database":
        assert loaded.database is None
    else:
        assert loaded.result_database is None
    assert "not an existing directory" in caplog.text
    assert (root / "scope").read_text() == "regular file"


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("binding", ["database", "result_database", "cache", "all"])
@SOURCE_ORDERS
async def test_resaving_a_job_preserves_the_location_of_an_unavailable_delta_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    binding: str,
    asynchronous: bool,
    memory_first: bool,
) -> None:
    """A resaved job can use each saved binding when its store returns."""
    monkeypatch.chdir(tmp_path)
    unavailable = (
        {"database", "result_database", "cache"} if binding == "all" else {binding}
    )
    targets = {name: DeltaTableDatabase(name) for name in sorted(unavailable)}
    path = save_job(
        targets["database"] if "database" in targets else DeltaTableDatabase("store"),
        result_database=targets.get("result_database"),
        cache_database=targets.get("cache"),
        memory_first=memory_first,
    )
    moved = {name: Path(name).rename(f"{name}-moved") for name in targets}
    loaded = PipelineJob.load(path)
    copy_path = Path("copy.json")
    loaded.save(copy_path)

    saved_bindings = json.loads(path.read_text())["bindings"]
    copied_bindings = json.loads(copy_path.read_text())["bindings"]
    if "cache" in targets:
        assert copied_bindings["sources"]["file"] == saved_bindings["sources"]["file"]
    for name in ("database", "result_database"):
        if name in targets:
            assert copied_bindings[name] == saved_bindings[name]
    for name, moved_path in moved.items():
        assert not Path(name).exists()
        _ = moved_path.rename(name)
    restored = PipelineJob.load(copy_path)
    summary = await load_only(restored, asynchronous=asynchronous)
    assert summary.nodes["file_total"].reused == 3
    assert restored.unbound_sources == ["memory"]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("absolute", [False, True], ids=["relative", "absolute"])
@pytest.mark.parametrize("journal_mode", ["delete", "wal"])
@SOURCE_ORDERS
async def test_loading_a_sqlite_source_reads_existing_rows_without_creating_a_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    asynchronous: bool,
    absolute: bool,
    journal_mode: str,
    memory_first: bool,
) -> None:
    """Read an existing SQLite input in either journal mode; never recreate it."""
    monkeypatch.chdir(tmp_path)
    original = Path("input #.sqlite")
    with closing(sqlite3.connect(original)) as connection:
        assert connection.execute(f"PRAGMA journal_mode={journal_mode}").fetchone() == (
            journal_mode,
        )
        _ = connection.execute(
            'CREATE TABLE samples (id INTEGER PRIMARY KEY, "group" TEXT NOT NULL, '
            "x INTEGER NOT NULL, y INTEGER NOT NULL)"
        )
        _ = connection.executemany(
            "INSERT INTO samples VALUES (?, ?, ?, ?)",
            [(1, "a", 4, 5), (2, "b", 6, 7), (3, "c", 8, 9)],
        )
        connection.commit()
    source = SQLiteTableSource(
        original.absolute() if absolute else original,
        "samples",
        tag_columns=["id", "group"],
        tag_only_columns=True,
        source_id="file",
    )
    path = save_job(
        DeltaTableDatabase("store"), memory_first=memory_first, file_source=source
    )
    present = PipelineJob.load(path)
    summary = await load_only(present, asynchronous=asynchronous)
    assert summary.nodes["file_total"].reused == 3

    _ = original.rename("moved.sqlite")
    missing = PipelineJob.load(path)
    assert not original.exists()
    assert set(missing.unbound_sources) == {"file", "memory"}
    summary = await load_only(missing, asynchronous=asynchronous)
    assert summary.skipped_unbound_sources["file_total"] == ("file",)


def test_loading_a_job_does_not_create_a_temporary_blueprint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A job's embedded blueprint can be read directly from the saved file."""
    monkeypatch.chdir(tmp_path)
    path = save_job(DeltaTableDatabase("store"))

    def refuse_tempfile(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("load tried to create a temporary file")

    monkeypatch.setattr(tempfile, "NamedTemporaryFile", refuse_tempfile)
    assert PipelineJob.load(path).unbound_sources == ["memory"]


def write_sqlite_input(path: Path, value: int) -> None:
    """Create three rows with a value that identifies this database."""
    with closing(sqlite3.connect(path)) as connection:
        _ = connection.execute(
            'CREATE TABLE samples (id INTEGER PRIMARY KEY, "group" TEXT NOT NULL, '
            '"x" INTEGER NOT NULL, y INTEGER NOT NULL)'
        )
        _ = connection.executemany(
            "INSERT INTO samples VALUES (?, ?, ?, ?)",
            [(1, "a", value, 5), (2, "b", value, 7), (3, "c", value, 9)],
        )
        connection.commit()


@pytest.mark.skipif(sys.platform == "win32", reason="colon in filename")
def test_sqlite_source_load_preserves_filename_interpretation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The file: prefix is part of the filename."""
    monkeypatch.chdir(tmp_path)
    write_sqlite_input(tmp_path / "file:input.sqlite", 2)
    write_sqlite_input(tmp_path / "input.sqlite", 99)
    source = SQLiteTableSource(
        "file:input.sqlite",
        "samples",
        tag_columns=["id", "group"],
        tag_only_columns=True,
        source_id="file",
    )
    expected = [2] * 3
    assert source.as_table().column("x").to_pylist() == expected
    path = save_job(DeltaTableDatabase("store"), file_source=source)
    loaded = PipelineJob.load(path)
    assert loaded.nodes["file"].as_table().column("x").to_pylist() == expected
    Path("input.sqlite").unlink()
    loaded = PipelineJob.load(path)
    assert loaded.nodes["file"].as_table().column("x").to_pylist() == expected


def test_relative_sqlite_source_pins_its_file_at_construction_and_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refresh resolves the same file after chdir, for live and restored sources."""
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    write_sqlite_input(first / "input.sqlite", 2)
    write_sqlite_input(second / "input.sqlite", 99)
    monkeypatch.chdir(first)
    source = SQLiteTableSource("input.sqlite", "samples")
    assert source.source_id == "sqlite_table_v1:input.sqlite:main.samples"
    named = SQLiteTableSource(
        "input.sqlite",
        "samples",
        tag_columns=["id", "group"],
        tag_only_columns=True,
        source_id="file",
    )
    path = first / save_job(DeltaTableDatabase(first / "store"), file_source=named)
    monkeypatch.chdir(second)
    assert source.check_for_new_data() is False
    assert source.as_table().column("x").to_pylist() == [2] * 3
    loaded = PipelineJob.load(path)
    restored = loaded.sources["file"]
    assert isinstance(restored, SQLiteTableSource)
    assert restored.as_table().column("x").to_pylist() == [99] * 3
    monkeypatch.chdir(first)
    assert restored.check_for_new_data() is False
    assert restored.as_table().column("x").to_pylist() == [99] * 3
    assert "uri" not in signature(SQLiteTableSource).parameters


@pytest.mark.parametrize("binding", ["database", "result_database", "cache"])
def test_loading_a_delta_scope_uses_the_same_path_mapping_as_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, binding: str
) -> None:
    """Simulate Windows's colon mapping while accessing a real store."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        DeltaTableDatabase,
        "_sanitize_path_component",
        staticmethod(lambda component: component.replace(":", "!")),
    )
    scoped = DeltaTableDatabase("root").at("experiment:one")
    path = save_job(
        scoped if binding == "database" else DeltaTableDatabase("store"),
        result_database=scoped if binding == "result_database" else None,
        cache_database=scoped if binding == "cache" else None,
    )
    assert (Path("root") / "experiment!one").is_dir()
    loaded = PipelineJob.load(path)
    assert loaded.unbound_sources == ["memory"]
    summary = loaded.run(load_only=True)
    assert summary.nodes["file_total"].reused == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_load_only_rechecks_a_source_cache_that_reappears(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, asynchronous: bool
) -> None:
    """A missing cache on the first run cannot hide restored rows on the second."""
    monkeypatch.chdir(tmp_path)
    root = Path("cache")
    path = save_job(
        DeltaTableDatabase("store"), cache_database=DeltaTableDatabase(root)
    )
    loaded = PipelineJob.load(path)
    moved = root.rename("moved")
    empty = await load_only(loaded, asynchronous=asynchronous)
    assert empty.nodes["file_total"].rows == 0
    assert not root.exists()

    _ = moved.rename(root)
    restored = await load_only(loaded, asynchronous=asynchronous)
    assert restored.ran_nothing is False
    assert restored.nodes["file_total"].reused == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    "cache_state", ["moved", "moved-after-load", "empty", "present"]
)
async def test_load_only_uses_only_stored_cache_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    cache_state: str,
    asynchronous: bool,
) -> None:
    """Load-only never ingests live rows, even when the cache could accept them."""
    monkeypatch.chdir(tmp_path)
    root = Path("cache")
    path = save_job(
        DeltaTableDatabase("store"),
        cache_database=DeltaTableDatabase(root),
        extra_nodes=True,
    )
    if cache_state in {"moved", "empty"}:
        _ = root.rename("moved")
    if cache_state == "empty":
        root.mkdir()
    with Path("rows.csv").open("a") as csv:
        _ = csv.write("4,d,11,12\n")
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    loaded = PipelineJob.load(path)
    if cache_state == "moved-after-load":
        _ = root.rename("moved")
        before = {}
    summary = await load_only(loaded, asynchronous=asynchronous)

    assert {p: p.read_bytes() for p in root.rglob("*") if p.is_file()} == before
    assert root.exists() == (cache_state in {"empty", "present"})
    assert loaded.nodes["file_total"].as_table().num_rows == (
        3 if cache_state == "present" else 0
    )
    for label in ("selected", "delivered"):
        assert loaded.nodes[label].as_table().num_rows == (
            3 if cache_state == "present" else 0
        )
    if cache_state == "moved":
        assert summary.skipped_unbound_sources["file_total"] == ("file",)
        assert "Could not reconstruct source 'file'" in caplog.text
    else:
        assert summary.nodes["file_total"].not_stored == 0
        if cache_state != "present":
            assert "has no stored rows" in caplog.text


@pytest.mark.parametrize("label", ["file", "file_total", "selected", "delivered"])
def test_node_load_only_uses_only_stored_source_cache_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    label: str,
) -> None:
    """A direct node run has the same source-cache guarantee as a job run."""
    monkeypatch.chdir(tmp_path)
    root = Path("cache")
    path = save_job(
        DeltaTableDatabase("store"),
        cache_database=DeltaTableDatabase(root),
        extra_nodes=True,
    )
    _ = root.rename("moved")
    root.mkdir()
    loaded = PipelineJob.load(path)

    _ = loaded.nodes[label].run(load_only=True)
    assert list(root.iterdir()) == []
