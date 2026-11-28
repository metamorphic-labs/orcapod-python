"""The data function type declaration decides what a result table stores (ITL-1259).

A function result table is at
``<function>/<output schema hash>/v<function major>/python.function.v1/rev1/rdb_v1``:

* ``python.function.v1``: the type and its major, the last part of the uri (identity);
* ``rev1``: the revision of the type's own record (``pf_var_*``, ``pf_exec_*``), declared
  by the type, in the path only: no uri, hash, token or record id holds it;
* ``rdb_v1``: the common result columns (``COMMON_RESULT_COLUMNS``), path only.

Pipeline tables carry no revision. A saved job names the type and its major only.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core import data_function
from orcapod.core.data_function import (
    DECLARATIONS,
    PYTHON_FUNCTION,
    CachedDataFunction,
    PythonDataFunction,
    declaration_for,
)
from orcapod.core.datagrams import Data
from orcapod.core.executors import LocalPythonFunctionExecutor
from orcapod.core.executors.ray import RayExecutor
from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function_types import snapshot_path
from orcapod.core.result_cache import COMMON_RESULT_COLUMNS
from orcapod.core.sources import DictSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.system_constants import (
    PIPELINE_DB_SCHEMA_VERSION,
    RESULT_DB_SCHEMA_VERSION,
    constants,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "data_function_types.py"
GATE = REPO_ROOT / "scripts" / "check_golden_tags.py"
SNAPSHOTS = REPO_ROOT / "src" / "orcapod" / "data_function_types"
RECORD_ID = "_record_id"


# Functions with one, two and three outputs of different types.
def one(x: int, y: int) -> int:
    return x + y


def two(x: int, y: int) -> tuple[int, str]:
    return x * y, f"{x}-{y}"


def three(x: int, y: int) -> tuple[int, float, str]:
    return x - y, x / 4, str(y)


FUNCTIONS = {
    "one": (one, ["total"]),
    "two": (two, ["product", "label"]),
    "three": (three, ["difference", "quarter", "text"]),
}


def _source() -> DictSource:
    return DictSource(
        [
            {"k": "a", "j": 1, "x": 3, "y": 4},
            {"k": "b", "j": 2, "x": 5, "y": 6},
        ],
        tag_columns=["k", "j"],
        tag_only_columns=True,
        source_id="pairs",
    )


def _expected_result_columns(
    data_function: PythonDataFunction,
) -> list[tuple[str, str, bool]]:
    """Every column of the result table, in stored order: (name, Arrow type, nullable).

    The record id, the common columns and the type's declared columns come from
    their declarations; the outputs from the function's output schema.
    """
    common = {column.name: column for column in COMMON_RESULT_COLUMNS}
    declaration = data_function.declaration

    def entry(name: str, column: Any) -> tuple[str, str, bool]:
        return (name, column.arrow_type, column.nullable)

    output_types = (
        data_function.data_context.type_converter.python_schema_to_arrow_schema(
            data_function.output_data_schema
        )
    )
    return [
        (RECORD_ID, "large_binary", True),
        entry(constants.INPUT_DATA_HASH_COL, common[constants.INPUT_DATA_HASH_COL]),
        *(
            entry(f"{constants.PF_VARIATION_PREFIX}{c.name}", c)
            for c in declaration.variation
        ),
        *(
            entry(f"{constants.PF_EXECUTION_PREFIX}{c.name}", c)
            for c in declaration.execution
        ),
        *((f.name, str(f.type), f.nullable) for f in output_types),
        *(
            (f"{constants.SOURCE_PREFIX}{f.name}", "large_string", True)
            for f in output_types
        ),
        entry(constants.POD_TIMESTAMP, common[constants.POD_TIMESTAMP]),
        entry(constants.DATA_CONTEXT_COL, common[constants.DATA_CONTEXT_COL]),
    ]


def _stored_columns(
    database: Any, path: tuple[str, ...]
) -> list[tuple[str, str, bool]]:
    table = database.get_all_records(path, record_id_column=RECORD_ID)
    assert table is not None
    return [(f.name, str(f.type), f.nullable) for f in table.schema]


# ---------------------------------------------------------------------------
# The declaration
# ---------------------------------------------------------------------------


def test_the_python_function_type_is_v1_rev1() -> None:
    assert PYTHON_FUNCTION.type_id == "python.function.v1"
    assert PYTHON_FUNCTION.revision_component == "rev1"
    assert declaration_for("python.function.v1") is PYTHON_FUNCTION
    assert set(DECLARATIONS) == {"python.function.v1"}
    with pytest.raises(ValueError, match="python.function.v0"):
        declaration_for("python.function.v0")


def test_the_declared_executor_type_meaning_lists_the_stored_values() -> None:
    column = next(c for c in PYTHON_FUNCTION.execution if c.name == "executor_type")
    declared_values = column.meaning.split("(", 1)[1].removesuffix(")").split(", ")
    # Reading the identifier does not require initializing a Ray executor.
    ray_executor = RayExecutor.__new__(RayExecutor)
    assert set(declared_values) == {
        LocalPythonFunctionExecutor().executor_type_id,
        ray_executor.executor_type_id,
        "none",
    }


@pytest.mark.parametrize("name", list(FUNCTIONS))
def test_the_data_function_reports_exactly_the_declared_columns(name: str) -> None:
    fn, keys = FUNCTIONS[name]
    data_function = PythonDataFunction(fn, output_keys=keys)
    assert data_function.data_function_type_id == "python.function.v1"
    assert data_function.declaration is PYTHON_FUNCTION
    variation = data_function.get_function_variation_data()
    execution = data_function.get_execution_data()
    assert list(variation) == [c.name for c in PYTHON_FUNCTION.variation]
    assert list(execution) == [c.name for c in PYTHON_FUNCTION.execution]
    assert dict(data_function.get_function_variation_data_schema()) == {
        c.name: c.python_type for c in PYTHON_FUNCTION.variation
    }
    assert dict(data_function.get_execution_data_schema()) == {
        c.name: c.python_type for c in PYTHON_FUNCTION.execution
    }
    # A hash column's stored value carries the label its declaration names.
    for column in PYTHON_FUNCTION.variation:
        if column.hash_label is not None:
            assert column.covers
            assert variation[column.name].startswith(f"{column.hash_label}:".encode())


# ---------------------------------------------------------------------------
# A really written result table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("name", list(FUNCTIONS))
def test_a_written_result_row_is_the_declaration_plus_the_common_columns(
    name: str, mode: str
) -> None:
    fn, keys = FUNCTIONS[name]
    database = InMemoryArrowDatabase()
    cached = CachedDataFunction(
        PythonDataFunction(fn, output_keys=keys),  # pyright: ignore[reportArgumentType]  # PythonDataFunction is not yet typed as matching DataFunctionProtocol; it works at runtime
        result_database=database,
    )
    data = Data({"x": 3, "y": 4})
    if mode == "async":
        asyncio.run(cached.async_call(data))
    else:
        cached.call(data)
    assert cached.record_path[-3:] == (
        "python.function.v1",
        "rev1",
        RESULT_DB_SCHEMA_VERSION,
    )
    assert RESULT_DB_SCHEMA_VERSION == "rdb_v1"
    assert _stored_columns(database, cached.record_path) == _expected_result_columns(
        PythonDataFunction(fn, output_keys=keys)
    )


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
@pytest.mark.parametrize("name", list(FUNCTIONS))
def test_a_job_writes_its_result_and_pipeline_tables_at_the_declared_paths(
    name: str, orchestrator: type
) -> None:
    fn, keys = FUNCTIONS[name]
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="types", database=database)
    pod = function_pod(output_keys=keys)(fn).pod
    with job:
        pod(_source(), label="fn")
    job.run(orchestrator=orchestrator())
    paths = database.list_table_paths()
    uri = pod.uri
    result = [p for p in paths if "_result" in p]
    pipeline = [p for p in paths if "_result" not in p]
    assert len(result) == 1 and len(pipeline) == 1
    assert result[0][-(len(uri) + 2) :] == (*uri, "rev1", "rdb_v1")
    # The pipeline table carries no revision.
    assert pipeline[0][-(len(uri) + 2) : -2] == uri[:-1] + ("python.function.v1",)
    assert pipeline[0][-2].startswith("schema:")
    assert pipeline[0][-1] == PIPELINE_DB_SCHEMA_VERSION == "pdb_v3"
    assert "rev1" not in pipeline[0]
    assert _stored_columns(database, result[0]) == _expected_result_columns(
        PythonDataFunction(fn, output_keys=keys)
    )


# ---------------------------------------------------------------------------
# The revision is in the path only
# ---------------------------------------------------------------------------


def _run_and_fingerprint(name: str) -> dict[str, Any]:
    fn, keys = FUNCTIONS[name]
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="types", database=database)
    pod = function_pod(output_keys=keys)(fn).pod
    with job:
        pod(_source(), label="fn")
    job.run()
    node = job.nodes["fn"]
    paths = database.list_table_paths()
    result_path = next(p for p in paths if "_result" in p)
    pipeline_path = next(p for p in paths if "_result" not in p)
    pipeline_table = database.get_all_records(pipeline_path, record_id_column="_id")
    rows = sorted(
        (
            json.dumps(tag.as_dict(), sort_keys=True, default=str),
            json.dumps(data.as_dict(), sort_keys=True, default=str),
            # The token is <uri>::<result record id>::<key>; the record id is random.
            json.dumps(
                {
                    k: (v.split("::")[0], v.split("::")[-1])
                    for k, v in data.source_info().items()
                },
                sort_keys=True,
            ),
            json.dumps(tag.system_tags(), sort_keys=True, default=str),
        )
        for tag, data in node.iter_data()
    )
    assert pipeline_table is not None
    return {
        "uri": pod.uri,
        "pod_content_hash": pod.content_hash().to_string(),
        "pod_pipeline_hash": pod.pipeline_hash().to_string(),
        "node_content_hash": node.content_hash().to_string(),
        "node_pipeline_hash": node.pipeline_hash().to_string(),
        "rows": rows,
        "pipeline_path": pipeline_path,
        # A record id holds the result id, which is random: the instance ids are
        # compared (ITL-1471).
        "pipeline_instance_ids": sorted(
            str(instance)
            for instance in pipeline_table.column("__pipeline_instance_id").to_pylist()
        ),
        "result_path": result_path,
    }


@pytest.mark.parametrize("name", list(FUNCTIONS))
def test_a_new_revision_is_a_new_result_table_and_moves_no_hash(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = _run_and_fingerprint(name)
    rev2 = dataclasses.replace(PYTHON_FUNCTION, revision=2)
    monkeypatch.setitem(data_function.DECLARATIONS, rev2.type_id, rev2)
    after = _run_and_fingerprint(name)

    assert before["result_path"][-2:] == ("rev1", "rdb_v1")
    assert after["result_path"][-2:] == ("rev2", "rdb_v1")
    assert before["result_path"][:-2] == after["result_path"][:-2]
    for key in before:
        if key != "result_path":
            assert before[key] == after[key], key


# ---------------------------------------------------------------------------
# A saved job names the type and its major only
# ---------------------------------------------------------------------------


def test_a_saved_job_names_the_type_and_major_and_loads_with_the_librarys_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fn, keys = FUNCTIONS["three"]
    database = DeltaTableDatabase(tmp_path / "store")
    job = PipelineJob(name="types", database=database)
    with job:
        function_pod(output_keys=keys)(fn).pod(_source(), label="fn")
    job.run()
    path = tmp_path / "job.json"
    job.save(path)
    text = path.read_text()
    assert '"data_function_type_id": "python.function.v1"' in text
    assert "rev1" not in text and "rdb_v1" not in text

    loaded = PipelineJob.load(path, database=DeltaTableDatabase(tmp_path / "store"))
    rows = sorted(
        tuple(data[k] for k in keys)
        for _, data in loaded.nodes["fn"].as_source().iter_data()
    )
    assert rows == sorted(three(x, y) for x, y in ((3, 4), (5, 6)))

    # The revision comes from the installed library's declaration: under rev2 the
    # loaded job looks for a rev2 table and finds no stored result.
    rev2 = dataclasses.replace(PYTHON_FUNCTION, revision=2)
    monkeypatch.setitem(data_function.DECLARATIONS, rev2.type_id, rev2)
    reloaded = PipelineJob.load(path, database=DeltaTableDatabase(tmp_path / "store"))
    assert list(reloaded.nodes["fn"].as_source().iter_data()) == []


# ---------------------------------------------------------------------------
# The snapshot
# ---------------------------------------------------------------------------


def _run(*args: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *map(str, args)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )


def test_the_snapshots_match_the_code_and_the_pins() -> None:
    result = _run("--check")
    assert result.returncode == 0, result.stdout + result.stderr


def test_the_snapshot_holds_the_declaration() -> None:
    snapshot = json.loads(
        (SNAPSHOTS / snapshot_path(PYTHON_FUNCTION)).read_text(encoding="utf-8")
    )
    assert snapshot == PYTHON_FUNCTION.to_snapshot()
    assert snapshot["type_id"] == "python.function.v1"
    assert snapshot["revision_component"] == "rev1"


CURRENT = snapshot_path(PYTHON_FUNCTION)
"""The snapshot the current declaration writes (``python.function/v1-rev1.json`` today)."""


def _open_copy(tmp_path: Path) -> Path:
    """A copy of the snapshots whose current snapshots are open, whatever was released.

    The tests below write and pin the snapshot the current declaration writes; a
    released pin copied from the repository would refuse both (CodeRabbit and Copilot,
    #448). Only snapshots a current declaration writes are reopened: a released
    snapshot of an earlier revision stays pinned, as it is in the repository.
    """
    snapshots = tmp_path / "snapshots"
    shutil.copytree(SNAPSHOTS, snapshots)
    pins = json.loads((snapshots / "released.json").read_text())
    current = {snapshot_path(declaration) for declaration in DECLARATIONS.values()}
    for relative in current:
        pins[relative] = {"released_in": None, "sha256": None}
    (snapshots / "released.json").write_text(
        json.dumps(pins, indent=2, sort_keys=True) + "\n"
    )
    return snapshots


def test_an_open_snapshot_that_differs_from_the_code_fails_and_write_fixes_it(
    tmp_path: Path,
) -> None:
    snapshots = _open_copy(tmp_path)
    target = snapshots / CURRENT
    snapshot = json.loads(target.read_text())
    snapshot["run_rules"] = ["edited by hand"]
    target.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    failed = _run("--check", "--snapshots", snapshots)
    assert failed.returncode == 1 and "differs from the code" in failed.stdout
    assert _run("--write", "--snapshots", snapshots).returncode == 0
    assert _run("--check", "--snapshots", snapshots).returncode == 0


def test_release_pins_and_a_pinned_snapshot_never_changes(tmp_path: Path) -> None:
    snapshots = _open_copy(tmp_path)
    released = _run("--release", "9.9.9", "--snapshots", snapshots)
    assert released.returncode == 0, released.stdout + released.stderr
    assert f"pinned {CURRENT} for 9.9.9" in released.stdout
    pins = json.loads((snapshots / "released.json").read_text())
    pin = pins[CURRENT]
    assert pin["released_in"] == "9.9.9" and len(pin["sha256"]) == 64
    assert _run("--check", "--snapshots", snapshots).returncode == 0

    target = snapshots / CURRENT
    target.write_text(target.read_text() + " ")
    failed = _run("--check", "--snapshots", snapshots)
    assert failed.returncode == 1
    assert (
        "never changes" in failed.stdout or "declares it differently" in failed.stdout
    )


# ---------------------------------------------------------------------------
# The reference page states the declaration
# ---------------------------------------------------------------------------

PAGE = REPO_ROOT / "docs" / "reference" / "data-function-types.md"


def _page_rows(heading: str, columns: int) -> list[list[str]]:
    """The body rows of the tables after ``heading``, up to the next ``##`` heading."""
    text = PAGE.read_text(encoding="utf-8")
    section = text.split(heading, 1)[1].split("\n## ", 1)[0]
    rows = []
    for line in section.splitlines():
        # An escaped pipe (``str \\| None``) belongs to its cell.
        escaped = line.strip().strip("|").replace("\\|", "\x00")
        cells = [cell.strip().replace("\x00", "|") for cell in escaped.split("|")]
        if line.startswith("| `") and len(cells) == columns:
            rows.append([cell.strip("`") for cell in cells])
    return rows


def test_the_reference_page_lists_the_declared_record() -> None:
    record = list(PYTHON_FUNCTION.variation) + list(PYTHON_FUNCTION.execution)
    rows = _page_rows("### Its record, revision 1", 5)
    record_rows = [row for row in rows if row[0] in {c.name for c in record}]
    assert [
        (row[0], row[1], row[2], row[3] == "yes", row[4].replace("`", ""))
        for row in record_rows
    ] == [
        (
            c.name,
            c.to_snapshot()["python_type"],
            c.arrow_type,
            c.nullable,
            c.meaning,
        )
        for c in record
    ]
    hashes = [row for row in _page_rows("### Its record, revision 1", 3)]
    assert [(row[0], row[1]) for row in hashes] == [
        (c.name, c.hash_label) for c in record if c.hash_label is not None
    ]


def test_the_reference_page_lists_the_common_columns() -> None:
    rows = _page_rows("## The common columns, `rdb_v1`", 4)
    fixed = {row[0]: row for row in rows}
    for column in COMMON_RESULT_COLUMNS:
        row = fixed[column.name]
        assert row[1] == column.arrow_type
        assert (row[2] == "yes") == column.nullable


def test_a_release_with_an_unsound_snapshot_writes_nothing(tmp_path: Path) -> None:
    """The snapshots are checked before the golden release step writes (Copilot, #448)."""
    from orcapod.contexts import get_default_context
    from tests.test_hashing.golden import store
    from tests.test_hashing.golden.generate_golden_hashes import main

    version = get_default_context().version
    root = tmp_path / "repo"
    store.copy_tree(store.PROJECT_ROOT, root, version)
    store.reopen_tree(root, version, "2026-11-18")
    (root / "scripts").mkdir(exist_ok=True)
    shutil.copy(SCRIPT, root / "scripts" / SCRIPT.name)
    shutil.copy(GATE, root / "scripts" / GATE.name)
    shutil.copytree(
        _open_copy(tmp_path), root / "src" / "orcapod" / "data_function_types"
    )
    snapshot = root / "src" / "orcapod" / "data_function_types" / CURRENT
    snapshot.write_text(snapshot.read_text() + " ")
    manifest = store.manifest_path(root, version)
    pins = store.pins_path(root)
    before = (manifest.read_bytes(), pins.read_bytes())

    with pytest.raises(SystemExit, match="nothing was released"):
        main(["--root", str(root), "--release", "9.9.9"])
    assert (manifest.read_bytes(), pins.read_bytes()) == before


# ---------------------------------------------------------------------------
# A revision move never makes a side effect run again (CodeRabbit and Copilot, #448)
# ---------------------------------------------------------------------------

DELIVERED: list[int] = []


def deliver(x: int, ctx: Any) -> int:
    """A function whose call is an external action: it takes the invocation context."""
    DELIVERED.append(x)
    return x * 2


def compute(x: int) -> int:
    """A plain function: computing it again is harmless."""
    DELIVERED.append(x)
    return x * 2


def _revision_job(database: Any, fn: Any, ctx: bool) -> PipelineJob:
    job = PipelineJob(name="revisions", database=database)
    pod = (
        FunctionPod.from_fn(fn, output_keys="y", ctx_arg_name="ctx")
        if ctx
        else FunctionPod.from_fn(fn, output_keys="y")
    )
    with job:
        pod(
            DictSource(
                [{"k": "a", "j": 1, "x": 1}, {"k": "b", "j": 2, "x": 2}],
                tag_columns=["k", "j"],
                tag_only_columns=True,
                source_id="s",
            ),
            label="fn",
        )
    return job


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
def test_a_side_effecting_pod_refuses_results_of_another_revision(
    orchestrator: type, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Under rev2, the rev1 result table is of another layout: refused, not repeated."""
    from orcapod.errors import SideEffectLogDataContextError

    database = InMemoryArrowDatabase()
    DELIVERED.clear()
    _revision_job(database, deliver, ctx=True).run(orchestrator=orchestrator())
    assert sorted(DELIVERED) == [1, 2]

    rev2 = dataclasses.replace(PYTHON_FUNCTION, revision=2)
    monkeypatch.setitem(data_function.DECLARATIONS, rev2.type_id, rev2)
    with pytest.raises(SideEffectLogDataContextError) as info:
        _revision_job(database, deliver, ctx=True).run(orchestrator=orchestrator())
    assert sorted(DELIVERED) == [1, 2]
    assert any(path[-2:] == ("rev1", "rdb_v1") for path in info.value.record_paths)


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
def test_a_plain_pod_reports_results_of_another_revision_and_recomputes(
    orchestrator: type,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    database = InMemoryArrowDatabase()
    DELIVERED.clear()
    _revision_job(database, compute, ctx=False).run(orchestrator=orchestrator())
    rev2 = dataclasses.replace(PYTHON_FUNCTION, revision=2)
    monkeypatch.setitem(data_function.DECLARATIONS, rev2.type_id, rev2)
    with caplog.at_level(logging.WARNING, logger="orcapod.contexts.stored"):
        job = _revision_job(database, compute, ctx=False)
        job.run(orchestrator=orchestrator())
    assert sorted(DELIVERED) == [1, 1, 2, 2]
    assert sorted(d["y"] for _, d in job.nodes["fn"].iter_data()) == [2, 4]
    assert any("rev1/rdb_v1" in record.getMessage() for record in caplog.records)


# ---------------------------------------------------------------------------
# A wrapper's declaration, and saved python.function.v0 functions
# ---------------------------------------------------------------------------


class _OtherRevision(PythonDataFunction):
    """A data function that declares its own record revision (rev7)."""

    @property
    def declaration(self) -> Any:
        return dataclasses.replace(PYTHON_FUNCTION, revision=7)


def test_a_wrapper_takes_the_wrapped_functions_declaration() -> None:
    """``CachedDataFunction`` stores at the wrapped function's revision (CodeRabbit, #448)."""
    inner = _OtherRevision(one, output_keys=["total"])
    cached = CachedDataFunction(inner, result_database=InMemoryArrowDatabase())  # pyright: ignore[reportArgumentType]  # PythonDataFunction is not yet typed as matching DataFunctionProtocol; it works at runtime
    assert cached.declaration.revision == 7
    assert cached.record_path[-2:] == ("rev7", RESULT_DB_SCHEMA_VERSION)


def test_a_saved_python_function_v0_cannot_bind_a_v1_function() -> None:
    """A job saved before 0.4 names python.function.v0: its function is not bound.

    Deliberate (nobody used a version before 0.4): such a job loads read only; to run
    it, rebuild the pipeline (CodeRabbit, #448).
    """
    from orcapod.core.data_function_proxy import DataFunctionProxy

    function = PythonDataFunction(one, output_keys=["total"])
    config = function.to_config()
    config["data_function_type_id"] = "python.function.v0"
    config["uri"] = [*function.uri[:-1], "python.function.v0"]
    proxy = DataFunctionProxy(config=config)
    with pytest.raises(ValueError, match="data_function_type_id"):
        proxy.bind(function)  # pyright: ignore[reportArgumentType]  # as above


def _release_tree(tmp_path: Path, *, with_script: bool = True) -> Path:
    """A copy of the golden store, open, with the snapshots (open) and their tool."""
    from orcapod.contexts import get_default_context
    from tests.test_hashing.golden import store

    version = get_default_context().version
    root = tmp_path / "repo"
    store.copy_tree(store.PROJECT_ROOT, root, version)
    store.reopen_tree(root, version, "2026-11-18")
    if with_script:
        (root / "scripts").mkdir(exist_ok=True)
        shutil.copy(SCRIPT, root / "scripts" / SCRIPT.name)
        shutil.copy(GATE, root / "scripts" / GATE.name)
    shutil.copytree(
        _open_copy(tmp_path), root / "src" / "orcapod" / "data_function_types"
    )
    return root


def _golden_state(root: Path) -> tuple[bytes, bytes]:
    from orcapod.contexts import get_default_context
    from tests.test_hashing.golden import store

    version = get_default_context().version
    return (
        store.manifest_path(root, version).read_bytes(),
        store.pins_path(root).read_bytes(),
    )


def test_a_release_refuses_snapshots_without_their_tool(tmp_path: Path) -> None:
    """Snapshots without ``scripts/data_function_types.py``: nothing is released."""
    from tests.test_hashing.golden.generate_golden_hashes import main

    root = _release_tree(tmp_path, with_script=False)
    before = _golden_state(root)
    with pytest.raises(SystemExit, match="is missing"):
        main(["--root", str(root), "--release", "9.9.9"])
    assert _golden_state(root) == before


def test_a_release_whose_snapshot_pinning_failed_is_finished_by_running_it_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The golden step stays recorded; the same command then pins the snapshots only."""
    from tests.test_hashing.golden import generate_golden_hashes as generator

    root = _release_tree(tmp_path)
    pins_path = root / "src" / "orcapod" / "data_function_types" / "released.json"
    real_pin = generator.pin_data_function_types

    def failing_pin(_root: Path, _release: str) -> None:
        raise OSError("released.json cannot be written")

    monkeypatch.setattr(generator, "pin_data_function_types", failing_pin)
    with pytest.raises(OSError, match="cannot be written"):
        generator.main(["--root", str(root), "--release", "9.9.9"])
    recorded = _golden_state(root)
    assert json.loads(pins_path.read_text())[CURRENT]["sha256"] is None

    monkeypatch.setattr(generator, "pin_data_function_types", real_pin)
    generator.main(["--root", str(root), "--release", "9.9.9"])
    assert _golden_state(root) == recorded
    pin = json.loads(pins_path.read_text())[CURRENT]
    assert pin["released_in"] == "9.9.9" and len(pin["sha256"]) == 64

    with pytest.raises(SystemExit, match="already released"):
        generator.main(["--root", str(root), "--release", "9.9.9"])


def test_a_retry_after_the_golden_history_changed_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from orcapod.contexts import get_default_context
    from tests.test_hashing.golden import generate_golden_hashes as generator
    from tests.test_hashing.golden import store

    root = _release_tree(tmp_path)

    def failing_pin(_root: Path, _release: str) -> None:
        raise OSError("released.json cannot be written")

    monkeypatch.setattr(generator, "pin_data_function_types", failing_pin)
    with pytest.raises(OSError):
        generator.main(["--root", str(root), "--release", "9.9.9"])
    # Something after the release step: the release revision is no longer the last.
    manifest_path = store.manifest_path(root, get_default_context().version)
    manifest = store.read_json(manifest_path)
    manifest["golden"]["history"].append({"added": {}, "digest": "0" * 64})
    store.write_json(manifest_path, manifest)
    assert not generator.golden_release_recorded(
        root, get_default_context().version, "9.9.9"
    )
    # Refused (the golden state is not the recorded release's), and nothing pinned.
    with pytest.raises(SystemExit):
        generator.main(["--root", str(root), "--release", "9.9.9"])
    pins = json.loads(
        (root / "src" / "orcapod" / "data_function_types" / "released.json").read_text()
    )
    assert pins[CURRENT]["sha256"] is None


@pytest.mark.parametrize("version", ["0.4.0rc1", "0.4.0.dev2", "v0.4.0b1"])
def test_release_refuses_a_pre_release_and_pins_nothing(
    tmp_path: Path, version: str
) -> None:
    """A pre-release pins no snapshot (ITL-1329): only a final release does."""
    snapshots = _open_copy(tmp_path)
    before = (snapshots / "released.json").read_bytes()
    result = _run("--release", version, "--snapshots", snapshots)
    assert result.returncode != 0
    assert "is a pre-release: a pre-release pins no snapshot" in (
        result.stdout + result.stderr
    )
    assert (snapshots / "released.json").read_bytes() == before
