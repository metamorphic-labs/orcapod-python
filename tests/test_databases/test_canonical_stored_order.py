"""A new stored table is written in the canonical column order (ITL-1370).

The record id, then every other column by name: the order the shared step
(``databases/utils.in_held_column_order``) gives a table nothing holds yet. One job
writes every stored table kind: a function pipeline table (``pdb``), its result table
(``rdb``), a side-effect log (``tdb``), an operator cache (``odb``) and a cached
source (``sdb``). Each test runs it twice through the shared step's two modes: as
before ITL-1370 (the step's canonical order switched off, so a table keeps the order
of its first write) and today. Nothing that identifies anything moves (content and
pipeline hashes, stored paths, record ids), and every reader presents the two stores
identically.

The connector databases (SQLite, PostgreSQL, SpiralDB) cannot hold Orcapod's own
stored tables yet (ITL-1162); their canonical order is tested on plain tables in
``test_append_by_name.py``.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pyarrow as pa
import pytest

import orcapod.databases.utils as database_utils
from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.cached_source import CachedSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.side_effects import SideEffectPod
from orcapod.types import CacheMode

pytestmark = pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")

DATABASES = ("in-memory", "Delta")
KINDS = ("pdb_v1", "rdb_v1", "tdb_v1", "odb_v1", "sdb_v1")
RECORD_ID = "__record_id"


def _database(kind: str, root: Path) -> Any:
    return InMemoryArrowDatabase() if kind == "in-memory" else DeltaTableDatabase(root)


def _before_itl_1370() -> AbstractContextManager[Any]:
    """Tables created in this context are written as before ITL-1370: in the order
    of their first write."""
    return patch.object(
        database_utils, "canonical_column_order", lambda records, record_id: records
    )


def _source(name: str, data: list[str], rows: list[int]) -> ArrowTableSource:
    """Two tags declared ``z_tag, a_tag`` (not in name order) and ``data`` floats."""
    fields = [
        pa.field("z_tag", pa.large_string(), nullable=False),
        pa.field("a_tag", pa.int64(), nullable=False),
        *(pa.field(n, pa.float64(), nullable=False) for n in data),
    ]
    arrays = [
        pa.array([f"s{n}" for n in rows], pa.large_string()),
        pa.array(rows, pa.int64()),
        *(pa.array([float(n) for n in rows]) for _ in data),
    ]
    return ArrowTableSource(
        pa.Table.from_arrays(arrays, schema=pa.schema(fields)),
        tag_columns=["z_tag", "a_tag"],
        tag_only_columns=True,
        record_id_columns=["a_tag"],
        source_id=name,
    )


@function_pod(output_keys=["w_out", "c_out"])
def compute(y: float, b: float) -> tuple[float, float]:
    return y + b, y - b


@function_pod(output_keys=["z_sum"])
def total(y: float, b: float) -> float:
    return y + b


def _notify(ctx: Any, y: float, b: float) -> None:
    return None


@contextmanager
def _job(
    database: Any, rows: list[int], extra_node: bool = False
) -> Iterator[tuple[PipelineJob, CachedSource]]:
    job = PipelineJob(name="itl1370", database=database)
    cached = CachedSource(_source("left", ["y", "b"], rows), cache_database=database)
    with job:
        compute.pod(cached, label="fn")
        Join()(cached, _source("right", ["x", "e"], rows), label="joined")
        SideEffectPod(_notify)(cached, label="notify")
        if extra_node:
            total.pod(cached, label="fn2")
    job.nodes["joined"].attach_databases(
        pipeline_database=database, cache_mode=CacheMode.LOG
    )
    job.run()
    yield job, cached


def _table(table: pa.Table | None) -> tuple[list[str], list[str]] | None:
    if table is None:
        return None
    return table.column_names, sorted(map(str, table.to_pylist()))


# Function result rows are named by a fresh uuid7 each run (``__data_id``, the result
# table's record id, and the source-info tokens of the outputs, which name it) and
# carry the time they were computed (``__pod_ts``): not hashes, different in any two
# runs, so compared by column only.
_RUN_SCOPED = ("fn.as_table", "fn.get_all_records", "fn.as_source", "cached outputs")


def _observe(database: Any) -> dict[str, Any]:
    """Everything a user can see of the job, and everything that identifies it."""
    seen: dict[str, Any] = {}
    with _job(database, [1, 2]) as (job, cached):
        for label in ("fn", "joined", "notify"):
            node = job.nodes[label]
            seen[f"{label}.content_hash"] = node.content_hash().to_string()
            seen[f"{label}.pipeline_hash"] = node.pipeline_hash().to_string()
            seen[f"{label}.as_table"] = _table(node.as_table(all_info=True))
            seen[f"{label}.as_table plain"] = _table(node.as_table())
        for label in ("fn", "joined"):
            node = job.nodes[label]
            seen[f"{label}.get_all_records"] = _table(
                node.get_all_records(all_info=True)
            )
            source = node.as_source()
            seen[f"{label}.as_source"] = _table(source.as_table(all_info=True))
            seen[f"{label}.as_source.content_hash"] = source.content_hash().to_string()
        seen["cached.as_table"] = _table(cached.as_table(all_info=True))
        seen["cached.get_all_records"] = _table(cached.get_all_records())
        seen["cached.content_hash"] = cached.content_hash().to_string()
        seen["cached outputs"] = _table(
            job.nodes["fn"]._cached_function_pod.get_all_cached_outputs()  # pyright: ignore[reportOptionalMemberAccess]  # the node's own result cache
        )
    paths = sorted(database.list_table_paths())
    seen["paths"] = paths
    for path in paths:
        stored = database.get_all_records(path, record_id_column=RECORD_ID)
        assert stored is not None
        seen[f"stored {path[-1]}"] = stored.column_names
        if path[-1] != "rdb_v1":
            seen[f"record ids {path[-1]}"] = sorted(
                stored.column(RECORD_ID).to_pylist()
            )
    return seen


@pytest.fixture(params=DATABASES)
def database_kind(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def both(database_kind: str, tmp_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """The job observed with its tables written as before ITL-1370, and today."""
    with _before_itl_1370():
        before = _observe(_database(database_kind, tmp_path / "before"))
    today = _observe(_database(database_kind, tmp_path / "today"))
    return before, today


def test_every_table_kind_is_written(both: tuple[dict[str, Any], dict[str, Any]]):
    _, today = both
    assert sorted(path[-1] for path in today["paths"]) == sorted(KINDS)


def test_no_hash_record_id_or_stored_path_moves(
    both: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    before, today = both
    identity = [
        key
        for key in today
        if key.endswith("hash") or key == "paths" or key.startswith("record ids")
    ]
    assert len([k for k in identity if k.startswith("record ids")]) == len(KINDS) - 1
    for key in identity:
        assert before[key] == today[key], key


def test_every_reader_presents_both_stores_identically(
    both: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    """A table written before ITL-1370 (its first write's order) and one written
    today (the canonical order) present the same columns, in the same order, with
    the same rows."""
    before, today = both
    for key, value in today.items():
        if key.startswith(("stored", "record ids")) or key.endswith("hash"):
            continue
        if key in _RUN_SCOPED:
            assert before[key][0] == value[0], key
        else:
            assert before[key] == value, key


def test_the_stores_differ_only_in_their_stored_column_order(
    both: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    before, today = both
    for key in (k for k in today if k.startswith("stored")):
        assert set(before[key]) == set(today[key]), key
        assert before[key] != today[key], key  # the job's first writes are not sorted


def test_each_table_kind_is_stored_in_the_canonical_order(
    both: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    """The record id, then every other column by name, for every table kind."""
    _, today = both
    for kind in KINDS:
        names = today[f"stored {kind}"]
        assert names == [RECORD_ID, *sorted(n for n in names if n != RECORD_ID)], kind
    # Pinned where no hash enters a column name.
    assert today["stored tdb_v1"] == [
        RECORD_ID,
        "__data_context",
        "executed_at",
        "pipeline_run_id",
        "record_id_hash",
    ]
    assert today["stored rdb_v1"] == [
        RECORD_ID,
        "__data_context",
        "__input_data_hash",
        "__pf_exec_executor_info",
        "__pf_exec_executor_type",
        "__pf_exec_extra_info",
        "__pf_exec_python_version",
        "__pf_var_function_content_hash",
        "__pf_var_function_module",
        "__pf_var_function_name",
        "__pf_var_function_signature_hash",
        "__pf_var_function_version",
        "__pf_var_git_hash",
        "__pod_ts",
        "_source_c_out",
        "_source_w_out",
        "c_out",
        "w_out",
    ]


def test_a_store_written_before_takes_new_rows_and_new_tables(
    database_kind: str, tmp_path: Path
) -> None:
    """Mixed generations: a store whose tables were written before ITL-1370 is run
    again by today's code with new rows and a new node. Every old table keeps its
    order and takes the new rows by name (ITL-1367); the new node's tables are
    canonical; the nodes present every row."""
    database = _database(database_kind, tmp_path)
    with _before_itl_1370(), _job(database, [1, 2]):
        pass
    old_orders = {
        path: database.get_all_records(path, record_id_column=RECORD_ID).column_names
        for path in database.list_table_paths()
    }
    with _job(database, [1, 2, 3], extra_node=True) as (job, cached):
        tags = cast(list[int], cached.as_table().column("a_tag").to_pylist())
        assert sorted(tags) == [1, 2, 3]
        for label in ("fn", "joined", "fn2"):
            records = job.nodes[label].get_all_records()
            assert records is not None
            assert sorted(records.column("a_tag").to_pylist()) == [1, 2, 3], label
            assert records.column_names[:2] == ["z_tag", "a_tag"], label
        fn = job.nodes["fn"].get_all_records()
        assert fn is not None
        assert sorted(zip(fn["a_tag"].to_pylist(), fn["w_out"].to_pylist())) == [
            (1, 2.0),
            (2, 4.0),
            (3, 6.0),
        ]
    for path in database.list_table_paths():
        names = database.get_all_records(path, record_id_column=RECORD_ID).column_names
        if path in old_orders:
            assert names == old_orders[path], path
        else:
            assert names == [RECORD_ID, *sorted(n for n in names if n != RECORD_ID)]
    new_tables = set(database.list_table_paths()) - set(old_orders)
    assert {path[-1] for path in new_tables} == {"pdb_v1", "rdb_v1"}


def test_the_before_context_is_the_identity(tmp_path: Path) -> None:
    """The comparison's 'before' writes a table in its first write's order."""
    database = InMemoryArrowDatabase()
    table = pa.table(
        {"b": [1], RECORD_ID: pa.array([b"r"], pa.large_binary()), "a": [2]}
    )
    with _before_itl_1370():
        database.add_records(("t",), table, flush=True, record_id_column=RECORD_ID)
    stored = database.get_all_records(("t",), record_id_column=RECORD_ID)
    assert stored is not None and stored.column_names == ["b", RECORD_ID, "a"]


def _loaded_presentation(database: Any, root: Path) -> dict[str, Any]:
    """What a loaded job's nodes present of their stored rows (no source bound)."""
    with _job(database, [1, 2]) as (job, _):
        job.save(str(root / "job.json"))
    loaded = PipelineJob.load(str(root / "job.json"), database=database)
    seen: dict[str, Any] = {}
    fn = loaded.nodes["fn"].get_all_records()
    assert fn is not None
    seen["fn"] = fn.column_names
    for label in ("fn", "joined"):
        seen[f"{label} source"] = _table(loaded.nodes[label].as_source().as_table())
    return seen


@pytest.mark.filterwarnings("ignore")
def test_a_loaded_job_presents_both_stores_identically(
    database_kind: str, tmp_path: Path
) -> None:
    """A loaded node knows its columns from the saved job, not from a live input:
    its stored rows present the same whatever order they are stored in."""
    (tmp_path / "before").mkdir()
    (tmp_path / "today").mkdir()
    with _before_itl_1370():
        before = _loaded_presentation(
            _database(database_kind, tmp_path / "before" / "db"), tmp_path / "before"
        )
    today = _loaded_presentation(
        _database(database_kind, tmp_path / "today" / "db"), tmp_path / "today"
    )
    assert (
        before["fn"]
        == today["fn"]
        == ["z_tag", "a_tag", "w_out", "c_out", "_context_key"]
    )
    assert before["fn source"][0] == today["fn source"][0]
    assert before["joined source"] == today["joined source"]
    assert today["joined source"][0] == ["z_tag", "a_tag", "y", "b", "x", "e"]


def _observer_columns(database: Any) -> tuple[list[str], list[str]]:
    from orcapod.pipeline.composite_observer import CompositeObserver
    from orcapod.pipeline.logging_observer import LoggingObserver
    from orcapod.pipeline.status_observer import StatusObserver

    job = PipelineJob(name="observed", database=InMemoryArrowDatabase())
    with job:
        compute.pod(_source("left", ["y", "b"], [1, 2]), label="fn")
    logs = LoggingObserver(log_database=database.at("observed", "_log"))
    status = StatusObserver(status_database=database.at("observed", "_status"))
    job.run(observer=CompositeObserver(logs, status))
    log_table, status_table = logs.get_logs(), status.get_status()
    assert log_table is not None and status_table is not None
    return log_table.column_names, status_table.column_names


def test_the_observers_present_both_stores_identically(
    database_kind: str, tmp_path: Path
) -> None:
    """The status and log tables an observer stores present in the observer's own
    order (its fixed columns, then the rest by name), however they are stored."""
    with _before_itl_1370():
        before = _observer_columns(_database(database_kind, tmp_path / "before"))
    today = _observer_columns(_database(database_kind, tmp_path / "today"))
    assert before == today
    logs, status = today
    assert logs[:3] == ["_log_id", "_log_run_id", "_log_timestamp"]
    assert status[:6] == [
        "_status_id",
        "_status_run_id",
        "_status_pipeline_uri",
        "_status_state",
        "_status_timestamp",
        "_status_error_summary",
    ]
