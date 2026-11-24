"""A function job node's held rows follow the order its run passed them downstream
(ITL-1441, fixed in PR 530).

On the synchronous path a run passes its rows downstream in input order, whether a
row's result was computed or read from the store; every read of the node afterwards
(``iter_data``, ``as_table``, ``head``, the data frames, a downstream node's own
``run()``) gives the rows in that same order, after a fresh run, a run over a partly
stored history, a load-only run and a run after ``clear_cache()``. So a downstream
node run on its own gives exactly what the whole job gave. Before the fix the node
held stored rows first, and a ``Batch`` run on its own grouped other members (other
sums, folded record ids and stored entries) than the whole job.

The asynchronous path promises no order (ITL-1437): a read follows the order that
run emitted, and a downstream node run on its own still gives what the whole job
gave.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.operators import Batch, GroupBy
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import CachedSource, DictSource


@function_pod(output_keys="y")
def times_ten(x: int) -> int:
    """Ten times x."""
    return x * 10


@function_pod(output_keys="total")
def total(y: list[int]) -> int:
    """The sum of a batch."""
    return sum(y)


ROWS = [{"k": i, "g": "shared", "x": i + 1} for i in range(4)]
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


def _source(rows: list[dict[str, Any]], tag_order: list[str]) -> DictSource:
    return DictSource(
        rows,
        tag_columns=tag_order,
        tag_only_columns=True,
        record_id_columns=["k"],
        source_id="input",
    )


def _job(
    database: Any, rows: list[dict[str, Any]], tag_order: list[str]
) -> PipelineJob:
    job = PipelineJob(name="held_order", database=database)
    with job:
        node = times_ten.pod(_source(rows, tag_order), label="f")
        total.pod(Batch(batch_size=2)(node), label="batch_total")
        GroupBy(["g"])(node)
    return job


def _ks(rows: list[dict[str, Any]]) -> list[int]:
    return [r["k"] for r in rows]


def _record_ids(table: Any) -> list[Any]:
    name = next(c for c in table.column_names if c.startswith("_tag_record_id"))
    return table.column(name).to_pylist()


@pytest.mark.parametrize("tag_order", [["k", "g"], ["g", "k"]])
@pytest.mark.parametrize("scenario", ["run", "load_only", "clear_cache"])
@pytest.mark.parametrize("stored", [(), (1, 3), (3, 1), (0, 2), (2,), (0, 1, 2, 3)])
@pytest.mark.parametrize("mode", ["sync", "async"])
def test_held_rows_follow_what_the_run_passed_downstream(
    tmp_path: Path,
    mode: str,
    stored: tuple[int, ...],
    scenario: str,
    tag_order: list[str],
) -> None:
    """Every read of the function node gives its rows in the order the run passed
    them downstream (input order on the sync path), and a downstream node run on
    its own (Batch and its sum, GroupBy) gives what the whole job gave: memberships,
    sums, folded record ids and stored entries, for several stored histories."""
    orchestrator = ORCHESTRATORS[mode]
    database = DeltaTableDatabase(tmp_path / "db")
    if stored:
        _job(database, [ROWS[i] for i in stored], tag_order).run(
            orchestrator=orchestrator()
        )
    job = _job(database, ROWS, tag_order)
    load_only = scenario == "load_only"
    if load_only:
        job.save(tmp_path / "job.json")
        job = PipelineJob.load(tmp_path / "job.json", database=database)
        job.bind(sources={"input": _source(ROWS, tag_order)})
    job.run(orchestrator=orchestrator(), load_only=load_only)
    if scenario == "clear_cache":
        job.nodes["f"].clear_cache()
        job.run(orchestrator=orchestrator())
    node = job.nodes["f"]

    passed = [t.as_dict()["k"] for t, _ in node.iter_data()]
    if mode == "sync":
        # Input order, of the inputs the run passed on (a load-only run passes the
        # stored ones only).
        expected = sorted(stored) if load_only else [0, 1, 2, 3]
        assert passed == expected
    assert _ks(node.as_table(columns={"sort_by_tags": False}).to_pylist()) == passed
    assert _ks(node.head(len(passed) or 1).table.to_pylist()) == passed
    assert node.as_polars_df()["k"].to_list() == passed
    assert node.as_pandas_df()["k"].to_list() == passed

    # A downstream node run on its own, from the held rows, gives what the job gave.
    batch, leaf = job.nodes["Batch"], job.nodes["batch_total"]
    job_batches = batch.as_table(columns={"system_tags": True})
    job_totals = leaf.as_table().to_pylist()
    job_groups = job.nodes["GroupBy"].as_table().to_pylist()
    job_entries, _ = leaf._stored_provenance(results=False)
    held_batches = Batch(batch_size=2)(node).as_table(columns={"system_tags": True})
    assert held_batches.column("k").to_pylist() == job_batches.column("k").to_pylist()
    assert held_batches.column("y").to_pylist() == job_batches.column("y").to_pylist()
    assert _record_ids(held_batches) == _record_ids(job_batches)
    assert GroupBy(["g"])(node).as_table().to_pylist() == job_groups
    batch.clear_cache()
    leaf.clear_cache()
    leaf.run(load_only=load_only)
    assert leaf.as_table().to_pylist() == job_totals
    after, _ = leaf._stored_provenance(results=False)
    count = 0 if job_entries is None else job_entries.num_rows
    assert (0 if after is None else after.num_rows) == count
    if mode == "sync" and not load_only:
        assert [t["total"] for t in job_totals] == [30, 70]


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_tied_group_members_follow_the_held_order(mode: str) -> None:
    """Two cached versions of one record (identical system tags, so GroupBy's member
    order ties) with only the newer result stored beforehand: GroupBy over the held
    rows gives the members in the order the whole job's GroupBy received them."""
    orchestrator = ORCHESTRATORS[mode]
    store, cache = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    old = ROWS[0]
    new = {**old, "x": 9}
    tags = ["k", "g"]
    CachedSource(
        _source([old], tags), cache_database=cache, cache_path=("joint",)
    ).as_table()
    versions = CachedSource(
        _source([new], tags), cache_database=cache, cache_path=("joint",)
    )
    _job(store, [new], tags).run(orchestrator=orchestrator())
    job = _job(store, [new], tags)
    job.bind(sources={"input": versions})
    job.run(orchestrator=orchestrator())
    node = job.nodes["f"]
    held = [d.as_dict()["y"] for _, d in node.iter_data()]
    if mode == "sync":
        assert held == [10, 90]  # the cache's order: the old version first
    groups = job.nodes["GroupBy"].as_table().to_pylist()
    assert [g["y"] for g in groups] == [held]
    assert GroupBy(["g"])(node).as_table().to_pylist() == groups
