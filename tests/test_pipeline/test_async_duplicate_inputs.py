"""An async run calls a function as often as a sync run for duplicate inputs (ITL-1062).

A function node's sync path calls its function once per distinct input: the first row of an
input stores its result, and every later row with the same input data hash finds it in the
result store (an exact duplicate, same tag and record id, finds it in the node's own rows of
the run). The async path computes rows concurrently, so rows with equal inputs used to miss
the store and all call the function. Now a row whose input is being computed waits for that
computation, without holding a concurrency slot, and both orchestrators make the same calls
and store the same rows.

Concurrency is forced with events, never with sleeps: the first call waits until as many
distinct inputs have started as may run at once. Each wait has an ``asyncio.timeout`` guard,
so a run that cannot reach that point fails its rows instead of hanging.
"""

from __future__ import annotations

import asyncio
import math
from collections import Counter
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.channels import Channel
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.run_summary import RunSummary
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.sources import ArrowTableSource, DictSource
from orcapod.types import NodeConfig, PodConfig

# The longest a forced wait may take before the call gives up (a guard, not a delay).
GUARD_SECONDS = 2.0
CONCURRENCY = [1, 4, None]
ORCHESTRATORS = ["sync", "async"]


class Gate:
    """Counts calls per input; the first calls wait until ``threshold`` inputs started.

    Attributes:
        calls: Calls made, per input value.
    """

    def __init__(self, threshold: int) -> None:
        """Open once ``threshold`` distinct inputs have started."""
        self.calls: Counter[int] = Counter()
        self._threshold = threshold
        self._opened = asyncio.Event()

    def open(self) -> None:
        """Open the gate."""
        self._opened.set()

    async def enter(self, x: int) -> None:
        """Count a call of ``x``; wait (guarded) until the gate is open."""
        self.calls[x] += 1
        if len(self.calls) >= self._threshold:
            self._opened.set()
        async with asyncio.timeout(GUARD_SECONDS):
            _ = await self._opened.wait()


def gated_pod(gate: Gate, max_concurrency: int | None) -> FunctionPod:
    """Return a pod ``x -> x + 1`` whose calls pass ``gate``."""

    async def inc(x: int) -> int:
        await gate.enter(x)
        return x + 1

    return FunctionPod.from_fn(
        inc, "y", pod_config=PodConfig(max_concurrency=max_concurrency)
    )


def same_data_other_tags(xs: list[int]) -> DictSource:
    """Rows ``k = 0..`` (distinct tags and record ids) with the data values ``xs``."""
    return DictSource(
        [{"k": k, "x": x} for k, x in enumerate(xs)], tag_columns=["k"], source_id="s"
    )


def exact_duplicates() -> ArrowTableSource:
    """6 rows: 3 equal rows, then 3 other equal rows (same tag, record id and data)."""
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("rid", pa.large_string(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    )
    table = pa.table(
        {"k": [0, 0, 0, 1, 1, 1], "rid": ["a"] * 3 + ["b"] * 3, "x": [5] * 3 + [6] * 3},
        schema=schema,
    )
    return ArrowTableSource(
        table, tag_columns=["k"], source_id="s", record_id_column="rid"
    )


# Case A: 4 distinct inputs, each 3 times under different tags, the copies adjacent.
CASE_A = [k // 3 for k in range(12)]
CASES = {
    "same_data_other_tags": (lambda: same_data_other_tags(CASE_A), 4),
    "exact_duplicates": (exact_duplicates, 2),
}


def orchestrator(kind: str) -> SyncPipelineOrchestrator | AsyncPipelineOrchestrator:
    """Return the ``sync`` or ``async`` orchestrator."""
    if kind == "sync":
        return SyncPipelineOrchestrator()
    return AsyncPipelineOrchestrator()


def threshold(kind: str, max_concurrency: int | None, distinct: int) -> int:
    """How many distinct inputs run at once: 1 on the sync path."""
    if kind == "sync":
        return 1
    return min(max_concurrency or math.inf, distinct)  # pyright: ignore[reportReturnType]


def build(
    pod: FunctionPod, source: StreamProtocol, db: InMemoryArrowDatabase
) -> PipelineJob:
    """A job over ``db``: node ``f`` applies ``pod`` to ``source``."""
    job = PipelineJob(name="itl1062", database=db)
    with job:
        pod(source, label="f")
    return job


def tables(db: InMemoryArrowDatabase, layout: str) -> list[pa.Table]:
    """Every stored table of ``db`` at ``layout`` (``rdb_v2`` or ``pdb_v2``)."""
    found = []
    for path in db.list_table_paths():
        if path[-1] == layout:
            table = db.get_all_records(path, record_id_column="_record_id")
            if table is not None:
                found.append(table)
    return found


def stored(db: InMemoryArrowDatabase) -> dict[str, list[Any]]:
    """The stored result rows and pipeline records of ``db``, comparable across runs.

    A result row's record id is a fresh UUID and its timestamp the time of the call, so
    neither is compared: a result row is named by its input data hash (the store's key),
    and a pipeline record keeps its own record id, a hash of its input, and names the
    result row it points to by that row's input data hash.
    """
    results: list[Any] = []
    hash_of_record: dict[bytes, bytes] = {}
    for table in tables(db, "rdb_v2"):
        for row in table.to_pylist():
            record_id = row.pop("_record_id")
            row.pop("__pod_ts")
            input_hash = row["__input_data_hash"]
            hash_of_record[record_id] = input_hash
            # Source-info tokens name the record id of the row.
            row = {
                key: value.replace(record_id.hex(), "<record>")
                if isinstance(value, str)
                else value
                for key, value in row.items()
            }
            results.append(sorted(row.items(), key=lambda item: item[0]))
    pipeline: list[Any] = []
    for table in tables(db, "pdb_v2"):
        for row in table.to_pylist():
            row["__data_id"] = hash_of_record[row["__data_id"]]
            pipeline.append(sorted(row.items(), key=lambda item: item[0]))
    return {
        "results": sorted(results, key=repr),
        "pipeline": sorted(pipeline, key=repr),
    }


def run_case(
    kind: str, case: str, max_concurrency: int | None
) -> tuple[Gate, RunSummary, PipelineJob, InMemoryArrowDatabase]:
    """Run ``case`` once on ``kind``; return the gate, summary, job and store."""
    make_source, distinct = CASES[case]
    gate = Gate(threshold(kind, max_concurrency, distinct))
    db = InMemoryArrowDatabase()
    job = build(gated_pod(gate, max_concurrency), make_source(), db)
    summary = job.run(orchestrator=orchestrator(kind))
    return gate, summary, job, db


def counts(summary: RunSummary) -> tuple[int, int, int]:
    """The node's ``(computed, reused, failed)`` row counts."""
    node = summary.nodes["f"]
    return node.computed, node.reused, node.failed


def node_rows(job: PipelineJob) -> list[tuple[int, int]]:
    """The ``(k, y)`` rows node ``f`` returns, sorted."""
    return sorted((row["k"], row["y"]) for row in job.nodes["f"].as_table().to_pylist())


@pytest.mark.parametrize("max_concurrency", CONCURRENCY)
@pytest.mark.parametrize("case", list(CASES))
class TestSameCallsAndRowsAsSync:
    """For every case and concurrency, async makes sync's calls and stores sync's rows."""

    def test_calls_once_per_distinct_input(
        self, case: str, max_concurrency: int | None
    ) -> None:
        """Each orchestrator calls the function once per distinct input."""
        distinct = CASES[case][1]
        for kind in ORCHESTRATORS:
            gate, summary, _, _ = run_case(kind, case, max_concurrency)
            assert counts(summary)[2] == 0, kind
            assert sorted(gate.calls.values()) == [1] * distinct, kind

    def test_stored_tables_equal_sync(
        self, case: str, max_concurrency: int | None
    ) -> None:
        """The result rows and pipeline records stored are sync's, row for row."""
        runs = {kind: run_case(kind, case, max_concurrency) for kind in ORCHESTRATORS}
        assert stored(runs["async"][3]) == stored(runs["sync"][3])

    def test_run_summary_and_rows_equal_sync(
        self, case: str, max_concurrency: int | None
    ) -> None:
        """Computed, reused and failed counts, and the node's rows, are sync's."""
        runs = {kind: run_case(kind, case, max_concurrency) for kind in ORCHESTRATORS}
        assert counts(runs["async"][1]) == counts(runs["sync"][1])
        assert node_rows(runs["async"][2]) == node_rows(runs["sync"][2])


def test_a_waiting_row_holds_no_concurrency_slot() -> None:
    """With 4 slots, the leader and 3 other inputs run while 2 copies of it wait.

    The input is ``0, 0, 0, 1, 2, 3``. The first call waits until 4 distinct inputs have
    started, which is possible only if the two waiting copies of ``0`` hold no slot.
    """
    gate = Gate(threshold=4)
    db = InMemoryArrowDatabase()
    job = build(gated_pod(gate, 4), same_data_other_tags([0, 0, 0, 1, 2, 3]), db)
    summary = job.run(orchestrator=AsyncPipelineOrchestrator())
    assert counts(summary) == (4, 2, 0)
    assert gate.calls == {0: 1, 1: 1, 2: 1, 3: 1}


def test_a_waiting_row_holds_no_slot_with_one_slot() -> None:
    """With 1 slot, rows run one at a time; the copies reuse the first result."""
    gate = Gate(threshold=1)
    db = InMemoryArrowDatabase()
    job = build(gated_pod(gate, 1), same_data_other_tags([0, 0, 0, 1]), db)
    summary = job.run(orchestrator=AsyncPipelineOrchestrator())
    assert counts(summary) == (2, 2, 0)
    assert gate.calls == {0: 1, 1: 1}


class LeaderOutcome:
    """A pod ``x -> x + 1`` whose first call of ``0`` raises ``error``.

    On the async path (``concurrent``) every call of ``0`` first waits for the call of
    ``1``. The input is ``0, 0, 0, 1``, so the copies of ``0`` have arrived by then, and
    the first call raises only after they did. On the sync path nothing waits: rows run
    one at a time. Both paths use the same function, so they store the same rows.
    """

    def __init__(self, error: BaseException, *, concurrent: bool) -> None:
        """Fail the first call of ``0`` with ``error``."""
        self.calls: Counter[int] = Counter()
        self._error = error
        self._concurrent = concurrent
        self._other_started = asyncio.Event()

    def pod(self) -> FunctionPod:
        """The pod."""

        async def inc(x: int) -> int:
            self.calls[x] += 1
            if x == 1:
                self._other_started.set()
            elif self._concurrent:
                async with asyncio.timeout(GUARD_SECONDS):
                    _ = await self._other_started.wait()
            if x == 0 and self.calls[0] == 1:
                raise self._error
            return x + 1

        return FunctionPod.from_fn(inc, "y")


def test_a_failed_leader_lets_the_next_copy_compute() -> None:
    """The first call of an input fails: the next copy calls the function, as on sync.

    Sync: row 1 fails, row 2 is a store miss and computes, row 3 reuses row 2's result.
    """
    results = {}
    for kind in ORCHESTRATORS:
        outcome = LeaderOutcome(
            ValueError("first call fails"), concurrent=kind == "async"
        )
        db = InMemoryArrowDatabase()
        job = build(outcome.pod(), same_data_other_tags([0, 0, 0, 1]), db)
        summary = job.run(orchestrator=orchestrator(kind))
        results[kind] = (
            dict(outcome.calls),
            counts(summary),
            node_rows(job),
            stored(db),
        )
    assert results["sync"][:3] == ({0: 2, 1: 1}, (2, 1, 1), [(1, 1), (2, 1), (3, 2)])
    assert results["async"] == results["sync"]


def test_a_leader_cancelled_by_its_function_releases_the_copies() -> None:
    """The first call of an input raises ``CancelledError``: the copies do not hang.

    The row of that call is dropped, as an async row whose function raises
    ``CancelledError`` was before (no observer report, no row); the next copy calls the
    function and the last reuses its result.
    """
    outcome = LeaderOutcome(asyncio.CancelledError(), concurrent=True)
    db = InMemoryArrowDatabase()
    job = build(outcome.pod(), same_data_other_tags([0, 0, 0, 1]), db)
    summary = job.run(orchestrator=AsyncPipelineOrchestrator())
    assert dict(outcome.calls) == {0: 2, 1: 1}
    assert counts(summary) == (2, 1, 0)
    assert node_rows(job) == [(1, 1), (2, 1), (3, 2)]


@pytest.mark.asyncio
async def test_input_ends_while_the_leader_is_computing() -> None:
    """The input closes while the only input is computed: its copies still get a row.

    Driven through the node's ``async_execute`` with channels, so the test closes the
    input while the first call is still running.
    """
    leader_started = asyncio.Event()
    release = asyncio.Event()
    calls: Counter[int] = Counter()

    async def inc(x: int) -> int:
        calls[x] += 1
        leader_started.set()
        async with asyncio.timeout(GUARD_SECONDS):
            _ = await release.wait()
        return x + 1

    source = same_data_other_tags([0, 0, 0])
    node = FunctionJobNode(
        FunctionPod.from_fn(inc, "y"),
        source,
        pipeline_database=InMemoryArrowDatabase(),
    )
    input_channel: Channel[Any] = Channel(buffer_size=16)
    output_channel: Channel[Any] = Channel(buffer_size=16)
    for row in source.iter_data():
        await input_channel.writer.send(row)
    async with asyncio.timeout(GUARD_SECONDS):
        run = asyncio.create_task(
            node.async_execute(input_channel.reader, output_channel.writer)
        )
        _ = await leader_started.wait()
        await input_channel.writer.close()
        release.set()
        await run
        rows = await output_channel.reader.collect()
    assert calls == {0: 1}
    assert sorted((tag["k"], data["y"]) for tag, data in rows) == [
        (0, 1),
        (1, 1),
        (2, 1),
    ]


def test_ephemeral_results_wait_for_the_running_call() -> None:
    """With results in the ephemeral store, copies wait for the running call too."""
    distinct = 4
    for kind in ORCHESTRATORS:
        gate = Gate(threshold(kind, None, distinct))
        db = InMemoryArrowDatabase()
        job = build(gated_pod(gate, None), same_data_other_tags(CASE_A), db)
        job.set_ephemeral_store(InMemoryArrowDatabase())
        job.apply_node_config(NodeConfig(is_result_ephemeral=True))
        summary = job.run(orchestrator=orchestrator(kind))
        assert counts(summary) == (4, 8, 0), kind
        assert sorted(gate.calls.values()) == [1] * distinct, kind


def test_a_pod_that_takes_the_invocation_context_is_called_as_on_sync() -> None:
    """A ``ctx_arg`` pod is called once per distinct input data on both paths.

    That is the sync path's behaviour (a later row with equal data is a result-store
    hit, whatever its tag); the async path now does the same.
    """
    distinct = 4
    for kind in ORCHESTRATORS:
        gate = Gate(threshold(kind, None, distinct))

        async def notify(x: int, ctx: Any) -> int:
            await gate.enter(x)
            return x + 1

        pod = FunctionPod.from_fn(notify, "y", ctx_arg_name="ctx")
        db = InMemoryArrowDatabase()
        job = build(pod, same_data_other_tags(CASE_A), db)
        summary = job.run(orchestrator=orchestrator(kind))
        assert counts(summary) == (4, 8, 0), kind
        assert sorted(gate.calls.values()) == [1] * distinct, kind
