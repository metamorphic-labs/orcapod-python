"""Regression tests for the second review of the row ids (ITL-1471, PR #583).

1. Two nodes that share one pipeline table (the default pipeline-hash scope) and
   meet one instance with one stored result each keep their own row: the table's
   key is the node's content hash with the record id.
2. A trace of a function that reads its source directly finds the record of the
   row's own instance after a rerun whose tokens changed.
3. On the async route a placeholder whose result is not stored is a failed row
   under the row failure policy, as on the sync route: the other rows are sent on.

Every test has several rows, two tags and two data columns, runs on the sync and
the async route, and where two sessions are involved, in both orders.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import Counter
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.channels import Channel
from orcapod.contexts import get_default_context
from orcapod.core import row_ids
from orcapod.core.datagrams import EmptyData
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import EphemeralResultMissingError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.types import ContentHash

HASHER = get_default_context().arrow_hasher
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}
PATH = pytest.mark.parametrize("path", ["sync", "async"])
KEY = "__key"

calls: Counter[str] = Counter()


@pytest.fixture(autouse=True)
def _reset() -> None:
    calls.clear()


@function_pod(output_keys=["total", "scaled"])
def combine(a: int, b: float) -> tuple[float, float]:
    calls["combine"] += 1
    return a + b, a * b


@function_pod(output_keys="y")
def add(x: int, w: int) -> int:
    calls["add"] += 1
    return x + w


def _through_channels(
    node: Any, rows: list[tuple[Any, Any]], **options: Any
) -> list[tuple[Any, Any]]:
    """Feed ``rows`` to ``node.async_execute`` through channels; return what it sends."""

    async def run() -> list[tuple[Any, Any]]:
        incoming: Channel[Any] = Channel()
        outgoing: Channel[Any] = Channel()
        for row in rows:
            await incoming.writer.send(row)
        await incoming.writer.close()
        await node.async_execute(incoming.reader, outgoing.writer, **options)
        return await outgoing.reader.collect()

    return asyncio.run(run())


def _values(table: pa.Table, column: str) -> list[Any]:
    return cast("list[Any]", table.column(column).to_pylist())


# ---------------------------------------------------------------------------
# 1. Two nodes sharing one pipeline table
# ---------------------------------------------------------------------------

# Two sessions over streams of one schema: rows 0 and 1 are the same in both, row 2
# differs. The streams are two streams (two content hashes), so the nodes are two
# nodes, with one pipeline hash and so one pipeline table.
B_VALUES = {"first": [0.5, 1.5, 2.5], "second": [0.5, 1.5, 9.5]}
TOTALS = {"first": [10.5, 21.5, 32.5], "second": [10.5, 21.5, 39.5]}


def _session_stream(session: str) -> ArrowTableStream:
    schema = pa.schema(
        [
            pa.field("subject", pa.large_string(), nullable=False),
            pa.field("session", pa.int64(), nullable=False),
            pa.field("a", pa.int64(), nullable=False),
            pa.field("b", pa.float64(), nullable=False),
        ]
    )
    return ArrowTableStream(
        pa.Table.from_arrays(
            [
                pa.array(["m1", "m1", "m2"], pa.large_string()),
                pa.array([1, 2, 1], pa.int64()),
                pa.array([10, 20, 30], pa.int64()),
                pa.array(B_VALUES[session], pa.float64()),
            ],
            schema=schema,
        ),
        tag_columns=["subject", "session"],
    )


def _run_node(node: Any, stream: ArrowTableStream, path: str) -> list[float]:
    if path == "sync":
        out = list(node.execute(stream).iter_data())
    else:
        out = _through_channels(node, list(stream.iter_data()))
    return sorted(cast("list[float]", [data.as_dict()["total"] for _, data in out]))


@PATH
@pytest.mark.parametrize("order", [("first", "second"), ("second", "first")])
def test_two_nodes_sharing_a_table_each_keep_their_row_of_a_shared_instance(
    path: str, order: tuple[str, str]
) -> None:
    """Both nodes store all of their rows, also the instances they share.

    The second node meets two instances the first already recorded, with the same
    stored results, so the same record ids. Keyed by the record id alone its two
    rows were skipped as duplicates, and its stored view lost them (review of PR
    #583; ``main`` kept both). The table's key is (node content hash, record id).
    """
    database = InMemoryArrowDatabase()
    pod = combine.pod
    assert isinstance(pod, FunctionPod)
    nodes: dict[str, Any] = {}
    for session in order:
        stream = _session_stream(session)
        node = FunctionJobNode(pod, stream, pipeline_database=database)
        nodes[session] = node
        assert _run_node(node, stream, path) == TOTALS[session]
        database.flush()
    first, second = (nodes[name] for name in order)
    assert first.node_identity_path == second.node_identity_path
    assert first.content_hash() != second.content_hash()
    # Rows 0 and 1 were computed once; row 2 once per session.
    assert calls["combine"] == 4

    for session, node in nodes.items():
        stored = node.get_all_records()
        assert stored is not None
        assert sorted(_values(stored, "total")) == TOTALS[session], session

    (table_path,) = [p for p in database.list_table_paths() if p[-1] == "pdb_v2"]
    table = database.get_all_records(table_path, record_id_column=KEY)
    assert table is not None
    assert table.num_rows == 6
    assert len(set(_values(table, KEY))) == 6
    by_node: dict[bytes, list[bytes]] = {}
    for row in table.to_pylist():
        by_node.setdefault(row["__node_content_hash"], []).append(
            row[row_ids.RECORD_ID_COL]
        )
    assert set(by_node) == {
        first.content_hash().to_prefixed_digest(),
        second.content_hash().to_prefixed_digest(),
    }
    records = [set(ids) for ids in by_node.values()]
    assert [len(ids) for ids in records] == [3, 3]
    # The two shared instances have one record id in both nodes' rows.
    assert len(records[0] & records[1]) == 2
    # Every stored row gives its record id again, and its key is the node's
    # content hash with it.
    for row in table.to_pylist():
        record = row_ids.record_id(
            HASHER,
            ContentHash.from_prefixed_digest(row["__pipeline_instance_id"]),
            uuid.UUID(bytes=row["__data_id"]),
        )
        assert record.to_prefixed_digest() == row[row_ids.RECORD_ID_COL]
        assert row[KEY] == row_ids.pipeline_row_key(
            ContentHash.from_prefixed_digest(row["__node_content_hash"]), record
        )

    # A new node over the first session's stream is that node again: every row is a
    # hit, nothing is computed and nothing is written.
    calls.clear()
    again = FunctionJobNode(pod, _session_stream(order[0]), pipeline_database=database)
    assert again.content_hash() == first.content_hash()
    assert _run_node(again, _session_stream(order[0]), path) == TOTALS[order[0]]
    database.flush()
    assert calls == Counter()
    after = database.get_all_records(table_path)
    assert after is not None and after.num_rows == 6


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("order", [("first", "second"), ("second", "first")])
def test_two_jobs_binding_one_slot_each_keep_their_stored_rows(
    orchestrator: str, order: tuple[str, str]
) -> None:
    """Two jobs bind different rows to one slot; each stored view has its own rows."""
    x_values = {"first": [5, 6, 7], "second": [5, 6, 9]}
    sums = {"first": [15, 26, 37], "second": [15, 26, 39]}

    def seed() -> DictSource:
        return DictSource(
            [
                {"k": 1, "j": 3, "x": 5, "w": 10},
                {"k": 2, "j": 4, "x": 6, "w": 20},
                {"k": 3, "j": 5, "x": 7, "w": 30},
            ],
            tag_columns=["k", "j"],
            tag_only_columns=True,
            source_id="slot",
        )

    database = InMemoryArrowDatabase()
    jobs: dict[str, PipelineJob] = {}
    for session in order:
        table = seed().as_table(columns={"source": True, "system_tags": True})
        field = table.schema.field("x")
        table = table.set_column(
            table.schema.get_field_index("x"),
            field,
            pa.array(x_values[session], type=field.type),
        )
        job = PipelineJob(name="scope", database=database)
        with job:
            add.pod(seed(), label="f")
        job.bind(sources={"slot": ArrowTableStream(table, tag_columns=["k", "j"])})
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        jobs[session] = job
    assert calls["add"] == 4
    for session, job in jobs.items():
        node: Any = job.nodes["f"]
        assert sorted(_values(node.as_table(), "y")) == sums[session], session
        stored = node.get_all_records()
        assert stored is not None
        assert sorted(_values(stored, "y")) == sums[session], session


# ---------------------------------------------------------------------------
# 2. A trace of a function over its source, after a rerun with other tokens
# ---------------------------------------------------------------------------


class _RetoldSource(DictSource):
    """A source whose rows give column ``x`` the token of ``w`` when ``retold``."""

    def __init__(self, retold: bool) -> None:
        super().__init__(
            [
                {"k": 1, "j": 3, "x": 5, "w": 5},
                {"k": 2, "j": 4, "x": 6, "w": 6},
                {"k": 3, "j": 5, "x": 7, "w": 7},
            ],
            tag_columns=["k", "j"],
            tag_only_columns=True,
            source_id="slot",
        )
        self.retold = retold

    def iter_data(self) -> Any:
        for tag, data in super().iter_data():
            if self.retold:
                data = data.with_source_info(x=data.source_info()["w"])
            yield tag, data


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("order", [(False, True), (True, False)])
def test_a_trace_over_the_source_finds_the_record_of_the_rows_own_instance(
    orchestrator: str, order: tuple[bool, bool]
) -> None:
    """Two jobs over one store whose source rows carry different tokens: both trace.

    The function reads the source directly. The second job's rows are the first's
    entries with other tokens: new instances, each naming the stored result. A
    trace resolves a held row by the instance id the node holds it under, so each
    job's trace reports its own record (review of PR #583: this branch took the one
    entry before choosing an instance, found two and refused).
    """
    database = InMemoryArrowDatabase()
    jobs: list[PipelineJob] = []
    for retold in order:
        job = PipelineJob(name="trace_src", database=database)
        with job:
            add.pod(_RetoldSource(retold), label="f")
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        jobs.append(job)
    assert calls["add"] == 3
    first: Any = jobs[0].nodes["f"]
    stored = first.get_all_records()
    assert stored is not None and stored.num_rows == 6

    records: list[set[str]] = []
    for index, job in enumerate(jobs):
        node: Any = job.nodes["f"]
        trace = node.trace({}, "y")
        assert trace.matched == 3 and len(trace.roots) == 3, index
        found = set()
        for root in trace.roots:
            assert root.function is not None
            found.add(root.function.record_id)
            assert root.function.computed_here is (index == 0)
        assert len(found) == 3
        records.append(found)
        # The stored instance of every traced record is one this job's node holds.
        held = {
            ContentHash.from_prefixed_digest(
                node.compute_instance_id(tag, data)
            ).to_string()
            for tag, data in node._input_stream.iter_data()
        }
        assert len(held) == 3
    assert records[0].isdisjoint(records[1])
    (root,) = jobs[0].nodes["f"].trace({"k": 2, "j": 4}, "y").roots  # pyright: ignore[reportAttributeAccessIssue]
    assert root.value == 12


# ---------------------------------------------------------------------------
# 3. A placeholder whose result is not stored, under the row failure policy
# ---------------------------------------------------------------------------


class _Events(NoOpObserver):
    """Records the row events of one node: (event, subject, session)."""

    def __init__(self) -> None:
        self.events: list[tuple[str, Any, Any]] = []

    def contextualize(self, *identity_path: str) -> "_Events":
        return self

    def _note(self, event: str, tag: Any) -> None:
        values = tag.as_dict()
        self.events.append((event, values["subject"], values["session"]))

    def on_data_start(self, node_label: str, tag: Any, data: Any) -> None:
        self._note("start", tag)

    def on_data_end(
        self, node_label: str, tag: Any, input_data: Any, output_data: Any, cached: bool
    ) -> None:
        self._note("end", tag)

    def on_data_crash(self, node_label: str, tag: Any, data: Any, error: Any) -> None:
        self._note(f"crash:{type(error).__name__}", tag)


class _Placeholders(ArrowTableStream):
    """A stream that gives placeholders: its rows' tags with the given data."""

    def give(self, data: list[Any]) -> "_Placeholders":
        self._given = data
        return self

    def iter_data(self) -> Any:
        for (tag, _), data in zip(super().iter_data(), self._given, strict=True):
            yield tag, data


def _stored_then_placeholders(
    bad: int,
) -> tuple[Any, _Placeholders, InMemoryArrowDatabase]:
    """A node whose three results are stored, and placeholders for its three rows.

    The placeholder at ``bad`` stands for an input the node never saw.
    """
    database = InMemoryArrowDatabase()
    pod = combine.pod
    assert isinstance(pod, FunctionPod)
    stream = _session_stream("first")
    first = FunctionJobNode(pod, stream, pipeline_database=database)
    assert _run_node(first, stream, "sync") == TOTALS["first"]
    database.flush()
    calls.clear()
    unseen = next(iter(_session_stream("second").iter_data()))[1].update(a=999)
    given = [
        EmptyData(cached_content_hash=(unseen if index == bad else data).content_hash())
        for index, (_, data) in enumerate(stream.iter_data())
    ]
    schema = pa.schema(
        [
            pa.field("subject", pa.large_string(), nullable=False),
            pa.field("session", pa.int64(), nullable=False),
            pa.field("a", pa.int64(), nullable=False),
            pa.field("b", pa.float64(), nullable=False),
        ]
    )
    placeholders = _Placeholders(
        stream.as_table().cast(schema), tag_columns=["subject", "session"]
    ).give(given)
    later = FunctionJobNode(pod, stream, pipeline_database=database)
    return later, placeholders, database


def _run_placeholders(
    node: Any, placeholders: _Placeholders, path: str, **options: Any
) -> list[float]:
    if path == "sync":
        out = list(node.execute(placeholders, **options).iter_data())
    else:
        out = _through_channels(node, list(placeholders.iter_data()), **options)
    return sorted(cast("list[float]", [data.as_dict()["total"] for _, data in out]))


@PATH
@pytest.mark.parametrize("bad", [0, 1, 2])
def test_a_placeholder_without_a_result_is_one_failed_row_under_continue(
    path: str, bad: int
) -> None:
    """The other rows are sent on and held; the function is not called."""
    node, placeholders, database = _stored_then_placeholders(bad)
    before = database.get_all_records(node._versioned_pipeline_path)
    assert before is not None
    observer = _Events()
    totals = _run_placeholders(
        node, placeholders, path, observer=observer, error_policy="continue"
    )
    expected = sorted(t for index, t in enumerate(TOTALS["first"]) if index != bad)
    assert totals == expected
    assert node.held_row_count() == 2
    assert calls == Counter()
    database.flush()
    after = database.get_all_records(node._versioned_pipeline_path)
    assert after is not None and after.num_rows == before.num_rows

    rows = [("m1", 1), ("m1", 2), ("m2", 1)]
    crash = "crash:EphemeralResultMissingError"
    wanted = sorted(
        event
        for index, row in enumerate(rows)
        for event in [("start", *row), (crash if index == bad else "end", *row)]
    )
    assert sorted(observer.events) == wanted
    # Each row starts before it ends or fails.
    for row in rows:
        mine = [event for event, *where in observer.events if tuple(where) == row]
        assert mine[0] == "start" and len(mine) == 2


@pytest.mark.parametrize("bad", [0, 2])
def test_the_events_of_placeholder_rows_are_the_same_on_both_routes(bad: int) -> None:
    """Sync and async give the same events for every row, and the same rows."""
    seen = {}
    for path in ("sync", "async"):
        node, placeholders, _ = _stored_then_placeholders(bad)
        observer = _Events()
        totals = _run_placeholders(
            node, placeholders, path, observer=observer, error_policy="continue"
        )
        seen[path] = (totals, sorted(observer.events), node.held_row_count())
    assert seen["sync"] == seen["async"]


@PATH
def test_a_placeholder_without_a_result_stops_the_run_under_fail_fast(
    path: str,
) -> None:
    node, placeholders, _ = _stored_then_placeholders(1)
    observer = _Events()
    with pytest.raises((EphemeralResultMissingError, ExceptionGroup)) as raised:
        _run_placeholders(
            node, placeholders, path, observer=observer, error_policy="fail_fast"
        )
    error = raised.value
    if isinstance(error, ExceptionGroup):
        assert error.subgroup(EphemeralResultMissingError) is not None
    assert ("crash:EphemeralResultMissingError", "m1", 2) in observer.events
    assert calls == Counter()
