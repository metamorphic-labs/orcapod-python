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


@function_pod(output_keys=["y", "w"])
def times_ten(x: int, z: int) -> tuple[int, int]:
    """Ten times x, and z negated."""
    return x * 10, -z


@function_pod(output_keys=["total_y", "total_w"])
def total(y: list[int], w: list[int]) -> tuple[int, int]:
    """The sums of a batch's two columns."""
    return sum(y), sum(w)


# Two plain tags and two data columns with distinct values (z is not a function of x).
ROWS: list[dict[str, Any]] = [
    {"k": i, "g": "shared", "x": i + 1, "z": (7 * i + 3) % 11} for i in range(4)
]
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


def _source(
    rows: list[dict[str, Any]],
    tag_order: list[str],
    data_order: tuple[str, ...] = ("x", "z"),
) -> DictSource:
    """The rows as a keyed source, tags and data columns laid out in the given orders."""
    return DictSource(
        [{c: r[c] for c in (*tag_order, *data_order)} for r in rows],
        tag_columns=tag_order,
        tag_only_columns=True,
        record_id_columns=["k"],
        source_id="input",
    )


def _job(
    database: Any,
    rows: list[dict[str, Any]],
    tag_order: list[str],
    data_order: tuple[str, ...] = ("x", "z"),
) -> PipelineJob:
    job = PipelineJob(name="held_order", database=database)
    with job:
        node = times_ten.pod(_source(rows, tag_order, data_order), label="f")
        total.pod(Batch(batch_size=2)(node), label="batch_total")
        GroupBy(["g"])(node)
    return job


def _ks(rows: list[dict[str, Any]]) -> list[int]:
    return [r["k"] for r in rows]


def _record_ids(table: Any) -> list[Any]:
    name = next(c for c in table.column_names if c.startswith("_tag_record_id"))
    return table.column(name).to_pylist()


@pytest.mark.parametrize(
    ("tag_order", "data_order"), [(["k", "g"], ("x", "z")), (["g", "k"], ("z", "x"))]
)
@pytest.mark.parametrize("scenario", ["run", "load_only", "clear_cache"])
@pytest.mark.parametrize("stored", [(), (1, 3), (3, 1), (0, 2), (2,), (0, 1, 2, 3)])
@pytest.mark.parametrize("mode", ["sync", "async"])
def test_held_rows_follow_what_the_run_passed_downstream(
    tmp_path: Path,
    mode: str,
    stored: tuple[int, ...],
    scenario: str,
    tag_order: list[str],
    data_order: tuple[str, ...],
) -> None:
    """Every read of the function node gives its rows in the order the run passed
    them downstream (input order on the sync path), and a downstream node run on
    its own (Batch and its sum, GroupBy) gives what the whole job gave: memberships,
    sums, folded record ids and stored entries, for several stored histories, with
    two tags and two data columns laid out two ways. Each row's two outputs (y and w,
    of distinct values) stay together in every read and every fold."""
    orchestrator = ORCHESTRATORS[mode]
    database = DeltaTableDatabase(tmp_path / "db")
    if stored:
        _job(database, [ROWS[i] for i in stored], tag_order, data_order).run(
            orchestrator=orchestrator()
        )
    job = _job(database, ROWS, tag_order, data_order)
    load_only = scenario == "load_only"
    if load_only:
        job.save(tmp_path / "job.json")
        job = PipelineJob.load(tmp_path / "job.json", database=database)
        job.bind(sources={"input": _source(ROWS, tag_order, data_order)})
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
    # Both outputs of each row, read together, are that row's own.
    by_k = {r["k"]: r for r in ROWS}
    assert [(d.as_dict()["y"], d.as_dict()["w"]) for _, d in node.iter_data()] == [
        (by_k[k]["x"] * 10, -by_k[k]["z"]) for k in passed
    ]

    # A downstream node run on its own, from the held rows, gives what the job gave.
    batch, leaf = job.nodes["Batch"], job.nodes["batch_total"]
    job_batches = batch.as_table(columns={"system_tags": True})
    job_totals = leaf.as_table().to_pylist()
    job_groups = job.nodes["GroupBy"].as_table().to_pylist()
    job_entries, _ = leaf._stored_provenance(results=False)
    held_batches = Batch(batch_size=2)(node).as_table(columns={"system_tags": True})
    assert held_batches.column("k").to_pylist() == job_batches.column("k").to_pylist()
    for column in ("y", "w"):
        assert (
            held_batches.column(column).to_pylist()
            == job_batches.column(column).to_pylist()
        )
    for members, ys, ws in zip(
        *(job_batches.column(c).to_pylist() for c in ("k", "y", "w")), strict=True
    ):
        assert ys == [by_k[k]["x"] * 10 for k in members]
        assert ws == [-by_k[k]["z"] for k in members]
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
        assert [(t["total_y"], t["total_w"]) for t in job_totals] == [
            (30, -(ROWS[0]["z"] + ROWS[1]["z"])),
            (70, -(ROWS[2]["z"] + ROWS[3]["z"])),
        ]


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_tied_group_members_follow_the_held_order(mode: str) -> None:
    """Two cached versions of one record (identical system tags, so GroupBy's member
    order ties) with only the newer result stored beforehand: GroupBy over the held
    rows gives the members in the order the whole job's GroupBy received them."""
    orchestrator = ORCHESTRATORS[mode]
    store, cache = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    old = ROWS[0]
    new = {**old, "x": 9, "z": 5}
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
    held = [(d.as_dict()["y"], d.as_dict()["w"]) for _, d in node.iter_data()]
    if mode == "sync":
        # The cache's order: the old version first.
        assert held == [(10, -old["z"]), (90, -5)]
    groups = job.nodes["GroupBy"].as_table().to_pylist()
    assert [list(zip(g["y"], g["w"], strict=True)) for g in groups] == [held]
    assert GroupBy(["g"])(node).as_table().to_pylist() == groups


def test_async_held_order_is_the_order_the_output_took_the_rows() -> None:
    """Under backpressure a parked send can complete after a later one. The node
    records a row once its output took it, so the held order is the order downstream
    received. Here the inputs arrive k=1 (computed) then k=0 (stored): the stored
    row's send is parked until the computed row's send has completed, so downstream
    receives k=1 first, and the node holds k=1 first."""
    import asyncio

    from orcapod.channels import Channel

    database = InMemoryArrowDatabase()
    _job(database, [ROWS[0]], ["k", "g"]).run(orchestrator=SyncPipelineOrchestrator())
    job = _job(database, ROWS[:2], ["k", "g"])
    node = job.nodes["f"]

    class ParkStored:
        """An output that holds k=0's send until k=1's send has completed."""

        def __init__(self) -> None:
            self.received: list[int] = []
            self.computed_taken = asyncio.Event()

        async def send(self, item: Any) -> None:
            k = item[0].as_dict()["k"]
            if k == 0:
                await self.computed_taken.wait()
            self.received.append(k)
            if k == 1:
                self.computed_taken.set()

        async def close(self) -> None:
            return None

    async def run() -> list[int]:
        inputs: Channel[Any] = Channel(buffer_size=16)
        for item in reversed(list(node.upstreams[0].iter_data())):
            await inputs.writer.send(item)
        await inputs.writer.close()
        output = ParkStored()
        await asyncio.wait_for(
            node.async_execute(inputs.reader, output),  # pyright: ignore[reportArgumentType]  # a test double of the writer
            timeout=30,
        )
        return output.received

    received = asyncio.run(run())
    assert received == [1, 0]
    assert [t.as_dict()["k"] for t, _ in node.iter_data()] == received
