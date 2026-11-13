"""Reading the stored cache of an operator node of a job loaded with ``PipelineJob.load``.

ITL-1043. The job joins two in-memory ``ArrowTableSource``s and stores the join's output
(``CacheMode.LOG``). In-memory sources are not saved with a job, so the loaded node cannot
reproduce the content hash its run stored the rows under. It must still look for them
under the path its run wrote, also when a column is nullable, and it must not present the
stored rows as zero rows.
"""

from __future__ import annotations

import pathlib
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.core.operators import Join
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import ArrowTableSource
from orcapod.types import CacheMode

JOB_NAME = "loaded_operator_cache"
NULLABILITY = {
    "non_null": (False, False),
    "nullable_tag": (True, False),
    "nullable_data": (False, True),
    "nullable_tag_and_data": (True, True),
}


def _source(column: str, tag_nullable: bool, data_nullable: bool) -> ArrowTableSource:
    """A two-row source with tag ``id`` and data ``column``, nulls allowed as declared."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=tag_nullable),
            pa.field(column, pa.int64(), nullable=data_nullable),
        ]
    )
    table = pa.table({"id": [0, 1], column: [1, 2]}, schema=schema)
    return ArrowTableSource(table, tag_columns=["id"], source_id=f"src_{column}")


def _store(kind: str, tmp_path: pathlib.Path) -> ArrowDatabaseProtocol:
    """An empty store of the given kind."""
    if kind == "delta":
        return DeltaTableDatabase(base_path=tmp_path / "store")
    return InMemoryArrowDatabase()


def _run_and_load(
    store: ArrowDatabaseProtocol, nullability: str, tmp_path: pathlib.Path
) -> tuple[Any, Any]:
    """Run the job with the join stored, save and load it; the run and loaded joins."""
    tag_nullable, data_nullable = NULLABILITY[nullability]
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        left = _source("a", tag_nullable, data_nullable)
        right = _source("b", tag_nullable, data_nullable)
        _ = Join()(left, right, label="joined")
    run_node = job.nodes["joined"]
    # OperatorNode.attach_databases is the public way to turn on LOG / REPLAY mode.
    run_node.attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.LOG
    )
    _ = job.run()
    saved = tmp_path / "job.json"
    job.save(saved)
    loaded_node = PipelineJob.load(str(saved), store=store).nodes["joined"]
    loaded_node.attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.REPLAY
    )
    return run_node, loaded_node


@pytest.mark.parametrize("store_kind", ["memory", "delta"])
@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_loaded_operator_node_keeps_the_pipeline_hash_and_path_of_its_run(
    store_kind: str, nullability: str, tmp_path: pathlib.Path
) -> None:
    """The loaded node's pipeline hash and table path are its run's, nullable or not."""
    run_node, loaded_node = _run_and_load(
        _store(store_kind, tmp_path), nullability, tmp_path
    )
    assert loaded_node.pipeline_hash() == run_node.pipeline_hash()
    assert loaded_node.node_identity_path == run_node.node_identity_path


@pytest.mark.parametrize("read", ["as_table", "iter_data", "head"])
@pytest.mark.parametrize("store_kind", ["memory", "delta"])
@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_loaded_replay_node_raises_rather_than_show_no_rows(
    store_kind: str, nullability: str, read: str, tmp_path: pathlib.Path
) -> None:
    """Two stored rows the loaded node cannot identify as its run's never read as none."""
    _, loaded_node = _run_and_load(_store(store_kind, tmp_path), nullability, tmp_path)
    with pytest.raises(
        RuntimeError,
        match=r"'joined': its cache table \(.*\) holds 2 stored rows, none of them",
    ):
        result = getattr(loaded_node, read)()
        if read == "iter_data":
            _ = list(result)
