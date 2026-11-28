"""Provenance of function outputs whose names overlap function inputs."""

import asyncio
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.row_ids import INSTANCE_ID_COL, RECORD_ID_COL
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.types import NodeConfig
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import ArrowTableSource


@function_pod(output_keys=["x", "y", "w"])
def _xyw(x: int, y: int, z: int) -> tuple[int, int, int]:
    return x + 10, y + 20, z + 30


@function_pod(output_keys=["w", "y", "x"])
def _wyx(x: int, y: int, z: int) -> tuple[int, int, int]:
    return z + 30, y + 20, x + 10


@function_pod(output_keys="out")
def _consume(x: int) -> int:
    return x * 10


def _source() -> ArrowTableSource:
    schema = pa.schema(
        [
            pa.field("cohort", pa.large_string(), nullable=False),
            pa.field("subject", pa.large_string(), nullable=False),
            *(pa.field(name, pa.int64(), nullable=False) for name in ("x", "y", "z")),
        ]
    )
    rows = pa.Table.from_arrays(
        [
            pa.array(["a", "a", "b"], type=pa.large_string()),
            pa.array(["s1", "s2", "s3"], type=pa.large_string()),
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array([4, 5, 6], type=pa.int64()),
            pa.array([7, 8, 9], type=pa.int64()),
        ],
        schema=schema,
    )
    return ArrowTableSource(
        rows,
        tag_columns=["cohort", "subject"],
        tag_only_columns=["subject"],
        source_id="stored_tokens",
    )


def _job(
    database: ArrowDatabaseProtocol,
    reverse: bool = False,
    ephemeral: InMemoryArrowDatabase | None = None,
    *,
    downstream: bool = False,
) -> PipelineJob:
    job = PipelineJob(name="stored_tokens", database=database)
    with job:
        node = (_wyx if reverse else _xyw).pod(_source(), label="collide")
        if downstream:
            _ = _consume.pod(node, label="consumer")
    if ephemeral is not None:
        job.set_ephemeral_database(ephemeral)
        job.apply_node_config(NodeConfig(is_result_ephemeral=True))
    return job


def _run(job: PipelineJob, mode: str, *, load_only: bool = False) -> None:
    if mode == "async":
        _ = asyncio.run(job.run_async(load_only=load_only))
    else:
        _ = job.run(load_only=load_only)


def _node(job: PipelineJob, label: str) -> FunctionJobNode:
    return cast(FunctionJobNode, job.nodes[label])


def _output_with_tokens(table: pa.Table) -> pa.Table:
    """Select output values and tokens in a fixed column and row order."""
    return table.select(
        [
            "cohort",
            "subject",
            "x",
            "y",
            "w",
            "_source_cohort",
            "_source_x",
            "_source_y",
            "_source_w",
        ]
    ).sort_by("subject")


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("backend", ["memory", "delta"])
@pytest.mark.parametrize("reverse", [False, True], ids=["xyw", "wyx"])
@pytest.mark.parametrize(
    "keep_ephemeral", [False, True], ids=["persistent", "ephemeral"]
)
def test_stored_results_keep_the_delivered_output_tokens(
    tmp_path: Path, mode: str, backend: str, reverse: bool, keep_ephemeral: bool
) -> None:
    database = (
        InMemoryArrowDatabase()
        if backend == "memory"
        else DeltaTableDatabase(tmp_path / "tables")
    )
    ephemeral = InMemoryArrowDatabase() if keep_ephemeral else None
    first = _job(database, reverse, ephemeral)
    _run(first, mode)

    first_node = _node(first, "collide")
    delivered = _output_with_tokens(first_node.as_table(columns={"source": True}))
    inputs = _source().as_table(columns={"source": True}).sort_by("subject")
    assert delivered.column("_source_cohort").equals(inputs.column("_source_cohort"))
    for name in ("x", "y", "w"):
        assert delivered.column(f"_source_{name}").null_count == 0
    for name in ("x", "y"):
        assert not delivered.column(f"_source_{name}").equals(
            inputs.column(f"_source_{name}")
        )

    for load_only in (False, True):
        fresh = _job(database, reverse, ephemeral)
        _run(fresh, mode, load_only=load_only)
        read = _node(fresh, "collide").as_table(columns={"source": True})
        assert _output_with_tokens(read).equals(delivered), f"{load_only=}"

    records = first_node.get_all_records(columns={"source": True})
    assert records is not None
    assert _output_with_tokens(records).equals(delivered)

    if ephemeral is None:
        saved = tmp_path / "job.json"
        first.save(saved)
        loaded = PipelineJob.load(saved, database=database)
        loaded.bind(sources={"stored_tokens": _source()})
        _run(loaded, mode, load_only=True)
        read = _node(loaded, "collide").as_table(columns={"source": True})
        assert _output_with_tokens(read).equals(delivered)


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_downstream_node_reuses_its_original_instances_and_records(mode: str) -> None:
    database = InMemoryArrowDatabase()
    first = _job(database, downstream=True)
    _run(first, mode)

    consumer = _node(first, "consumer")
    [path] = [
        path
        for path in database.list_table_paths()
        if path[1:] == (*consumer.node_identity_path, "pdb_v2")
    ]
    stored = database.get_all_records(path)
    assert stored is not None
    columns = ["subject", "_source_x", INSTANCE_ID_COL, RECORD_ID_COL]
    original = stored.select(columns).sort_by("subject")
    delivered = _node(first, "collide").as_table(columns={"source": True})
    assert original.column("_source_x").equals(
        delivered.sort_by("subject").column("_source_x")
    )

    fresh = _job(database, downstream=True)
    _run(fresh, mode)
    fresh_consumer = _node(fresh, "consumer")
    assert sorted(fresh_consumer.held_instance_ids()) == sorted(
        consumer.held_instance_ids()
    )
    stored = database.get_all_records(path)
    assert stored is not None
    assert stored.select(columns).sort_by("subject").equals(original)
