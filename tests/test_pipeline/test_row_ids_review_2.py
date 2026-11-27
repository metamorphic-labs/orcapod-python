"""Regression tests for the second review of the row ids (ITL-1471, PR #583).

1. Two nodes that share one pipeline table (the default pipeline-hash scope) and
   meet one instance with one stored result each keep their own row: the record
   id, the table's key, covers the node's content hash.
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
    stored results. With a record id made of the instance and the result alone its
    two rows were skipped as duplicates, and its stored view lost them (review of
    PR #583; ``main`` kept both). The record id covers the node's content hash, so
    the two nodes' records of one instance differ, and the record id is the key.
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
    instances: dict[bytes, set[bytes]] = {}
    for row in table.to_pylist():
        by_node.setdefault(row["__node_content_hash"], []).append(
            row[row_ids.RECORD_ID_COL]
        )
        instances.setdefault(row["__node_content_hash"], set()).add(
            row["__pipeline_instance_id"]
        )
    assert set(by_node) == {
        first.content_hash().to_prefixed_digest(),
        second.content_hash().to_prefixed_digest(),
    }
    records = [set(ids) for ids in by_node.values()]
    assert [len(ids) for ids in records] == [3, 3]
    # The two nodes share two instances, each with one stored result, and no
    # record id: a record id covers the node that recorded the row.
    shared = set.intersection(*instances.values())
    assert len(shared) == 2
    assert not records[0] & records[1]
    for instance in shared:
        of_instance = [
            row
            for row in table.to_pylist()
            if row["__pipeline_instance_id"] == instance
        ]
        assert len(of_instance) == 2
        assert len({row["__data_id"] for row in of_instance}) == 1
        assert len({row[row_ids.RECORD_ID_COL] for row in of_instance}) == 2
    # Every stored row gives its record id again from its own columns (the node's
    # content hash, the instance id, the result id), and the record id is its key.
    for row in table.to_pylist():
        record = row_ids.record_id(
            HASHER,
            ContentHash.from_prefixed_digest(row["__node_content_hash"]),
            ContentHash.from_prefixed_digest(row["__pipeline_instance_id"]),
            uuid.UUID(bytes=row["__data_id"]),
        )
        assert record.to_prefixed_digest() == row[row_ids.RECORD_ID_COL]
        assert row[KEY] == row[row_ids.RECORD_ID_COL]

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

    # The stored instance id of every record id, read from the pipeline table.
    (path,) = [p for p in database.list_sources() if p[-1] == "pdb_v2"]
    pipeline_rows = database.get_all_records(path)
    assert pipeline_rows is not None
    instance_of_record = {
        ContentHash.from_prefixed_digest(
            record
        ).to_string(): ContentHash.from_prefixed_digest(instance).to_string()
        for record, instance in zip(
            cast(
                "list[bytes]", pipeline_rows.column("__pipeline_record_id").to_pylist()
            ),
            cast(
                "list[bytes]",
                pipeline_rows.column("__pipeline_instance_id").to_pylist(),
            ),
        )
    }
    assert len(instance_of_record) == 6

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
        assert {instance_of_record[record] for record in found} == held
    assert records[0].isdisjoint(records[1])
    (root,) = jobs[0].nodes["f"].trace({"k": 2, "j": 4}, "y").roots  # pyright: ignore[reportAttributeAccessIssue]  # job.nodes is typed by the node protocol; "f" is a function node
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


# ---------------------------------------------------------------------------
# 4. One canonical type per token value
# ---------------------------------------------------------------------------

_LIST = pa.large_list(pa.large_string())
# Token columns ``_source_a`` of three rows, by kind.
TOKEN_KINDS: dict[str, "pa.Array[Any]"] = {
    "null_typed_list": pa.array([None, None, None], _LIST),
    "null_typed_string": pa.array([None, None, None], pa.large_string()),
    "strings": pa.array(["s::r0::a", "s::r1::a", "s::r2::a"], pa.large_string()),
    "lists": pa.array([["s::r0::a"], ["s::r1::a", "t::r1::a"], ["s::r2::a"]], _LIST),
    "null_and_lists": pa.array([None, ["s::r1::a"], None], _LIST),
}


def _token_stream(kind: str, session: str = "first") -> ArrowTableStream:
    """``_session_stream`` with a token column ``_source_a`` of the given kind."""
    table = _session_stream(session).as_table()
    return ArrowTableStream(
        table.append_column(
            pa.field("_source_a", TOKEN_KINDS[kind].type), TOKEN_KINDS[kind]
        ),
        tag_columns=["subject", "session"],
    )


def _pipeline_table(database: InMemoryArrowDatabase) -> pa.Table:
    database.flush()
    (path,) = [p for p in database.list_table_paths() if p[-1] == "pdb_v2"]
    table = database.get_all_records(path)
    assert table is not None
    return table


@PATH
@pytest.mark.parametrize(
    "kinds",
    [
        ("null_typed_list", "null_typed_string"),
        ("null_typed_string", "null_typed_list"),
        ("null_typed_list", "strings"),
        ("strings", "null_typed_list"),
    ],
)
def test_sessions_whose_null_tokens_are_typed_differently_share_a_table(
    path: str, kinds: tuple[str, str]
) -> None:
    """A null token is a ``large_string`` null, whatever type its column declares.

    Two sessions over one pipeline table, in both orders: a token column that holds
    only nulls, declared as a list in one session and as a string in the other (or
    holding strings in the other). Both complete and the table keeps one column
    type (review of PR #583: hashed and stored with the declared type, the second
    session failed with ``ArrowInvalid``; ``main`` completed both).
    """
    database = InMemoryArrowDatabase()
    pod = combine.pod
    assert isinstance(pod, FunctionPod)
    for kind, session in zip(kinds, ("first", "second")):
        stream = _token_stream(kind, session)
        node = FunctionJobNode(pod, stream, pipeline_database=database)
        assert _run_node(node, stream, path) == TOTALS[session]
        stored = node.get_all_records()
        assert stored is not None
        assert sorted(_values(stored, "total")) == TOTALS[session]
    table = _pipeline_table(database)
    assert table.num_rows == 6
    assert table.schema.field("_source_a").type == pa.large_string()


@pytest.mark.parametrize("tokens", ["null_typed_list", "null_typed_string"])
def test_a_null_tokens_instance_id_does_not_follow_its_columns_type(
    tokens: str,
) -> None:
    """The id is a function of the row alone: its tags, its data and its tokens."""
    ids = {}
    for kind in ("null_typed_list", "null_typed_string"):
        stream = _token_stream(kind)
        ids[kind] = [
            row_ids.instance_id(HASHER, tag, data) for tag, data in stream.iter_data()
        ]
        for tag, data in stream.iter_data():
            preimage = row_ids.instance_id_preimage(
                row_ids.entry_id(HASHER, tag, data), data
            )
            assert preimage.schema.field("_source_a").type == pa.large_string()
            assert preimage.column("_source_a").to_pylist() == [None]
    assert ids["null_typed_list"] == ids["null_typed_string"]
    assert len(set(ids[tokens])) == 3


def test_a_non_null_token_keeps_the_type_of_its_column() -> None:
    """Rows of a list-typed column: a list token is a list, a null a string null."""
    types = [
        str(data.source_info_table().schema.field("_source_a").type)
        for _, data in _token_stream("null_and_lists").iter_data()
    ]
    assert types == ["large_string", str(_LIST), "large_string"]
    assert [
        str(data.source_info_table().schema.field("_source_a").type)
        for _, data in _token_stream("lists").iter_data()
    ] == [str(_LIST)] * 3


def _refusal(error: BaseException) -> str:
    """The text of the clear refusal behind a failed write, with its cause."""
    from orcapod.errors import InputValidationError, OrcapodDatabaseWriteError

    if isinstance(error, ExceptionGroup):
        (error,) = OrcapodDatabaseWriteError.in_group(error)
    assert isinstance(error, OrcapodDatabaseWriteError), repr(error)
    assert isinstance(error.__cause__, InputValidationError), repr(error.__cause__)
    return str(error.__cause__)


@PATH
@pytest.mark.parametrize("kinds", [("lists", "strings"), ("strings", "lists")])
def test_sessions_whose_tokens_differ_in_type_are_refused_naming_the_column(
    path: str, kinds: tuple[str, str]
) -> None:
    """List tokens against string tokens in one table: refused, clearly, in any order.

    The first session's rows stay as they are. (``main`` fails here too, with an
    ``ArrowInvalid`` about schemas.)
    """
    from orcapod.errors import OrcapodDatabaseWriteError

    database = InMemoryArrowDatabase()
    pod = combine.pod
    assert isinstance(pod, FunctionPod)
    first = _token_stream(kinds[0], "first")
    node = FunctionJobNode(pod, first, pipeline_database=database)
    assert _run_node(node, first, path) == TOTALS["first"]
    before = _pipeline_table(database)

    second = _token_stream(kinds[1], "second")
    later = FunctionJobNode(pod, second, pipeline_database=database)
    with pytest.raises((OrcapodDatabaseWriteError, ExceptionGroup)) as raised:
        _run_node(later, second, path)
    message = _refusal(raised.value)
    assert "_source_a" in message
    assert "large_string" in message and "large_list<" in message
    assert _pipeline_table(database).equals(before)


@PATH
def test_a_session_mixing_null_and_list_tokens_in_one_column_is_refused(
    path: str,
) -> None:
    """A null token is a string null and a list token a list: two types, one column.

    As on ``main``, where the rows were stored with those two types and the write
    failed; the refusal now names the column and both types.
    """
    from orcapod.errors import OrcapodDatabaseWriteError

    database = InMemoryArrowDatabase()
    pod = combine.pod
    assert isinstance(pod, FunctionPod)
    stream = _token_stream("null_and_lists")
    node = FunctionJobNode(pod, stream, pipeline_database=database)
    with pytest.raises((OrcapodDatabaseWriteError, ExceptionGroup)) as raised:
        _run_node(node, stream, path)
    message = _refusal(raised.value)
    assert "_source_a" in message
    assert "large_string" in message and "large_list<" in message


class _AllEvents(_Events):
    """``_Events`` that also records the node's start and end."""

    def on_node_start(
        self, node_label: str, node_hash: str, tag_schema: Any = None
    ) -> None:
        self.events.append(("node_start", None, None))

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        self.events.append(("node_end", None, None))


def _run_with_a_failing_lookup(
    path: str, error: Exception, policy: str
) -> tuple[BaseException, list[tuple[str, Any, Any]]]:
    """Run three placeholders with the result lookup raising ``error``.

    Returns:
        The error the run raised (the leaf of an exception group) and the events.
    """
    from unittest.mock import patch

    node, placeholders, _ = _stored_then_placeholders(1)
    observer = _AllEvents()
    with (
        patch.object(
            type(node._cached_function_pod), "lookup_cached_data", side_effect=error
        ),
        pytest.raises((type(error), ExceptionGroup)) as raised,
    ):
        _run_placeholders(
            node, placeholders, path, observer=observer, error_policy=policy
        )
    leaf: BaseException = raised.value
    while isinstance(leaf, BaseExceptionGroup):
        (leaf,) = leaf.exceptions
    return leaf, observer.events


@pytest.mark.parametrize("policy", ["continue", "fail_fast"])
@pytest.mark.parametrize("kind", ["database_read", "type_identity"])
def test_a_fatal_error_in_a_placeholders_lookup_gives_the_same_events_on_both_routes(
    policy: str, kind: str
) -> None:
    """A database read error (or an identity error) in the lookup stops the run.

    It is never a row failure, whatever the policy. On both routes the row starts,
    its crash is reported with the error, and the error is the one raised (third
    review of PR #583: the async route reported the start only).
    """
    from orcapod.errors import OrcapodDatabaseReadError, TypeIdentityError

    seen = {}
    for path in ("sync", "async"):
        error: Exception = (
            OrcapodDatabaseReadError("injected", ("result",))
            if kind == "database_read"
            else TypeIdentityError("injected")
        )
        raised, events = _run_with_a_failing_lookup(path, error, policy)
        assert raised is error, path
        crash = f"crash:{type(error).__name__}"
        # The first row starts and crashes; no other row is started, none ends.
        assert [e for e in events if e[0] not in ("node_start", "node_end")] == [
            ("start", "m1", 1),
            (crash, "m1", 1),
        ], path
        seen[path] = events
    assert seen["sync"] == seen["async"]


# ---------------------------------------------------------------------------
# A data-tag's token through two function nodes: one type on both routes
# ---------------------------------------------------------------------------


@function_pod(output_keys="y")
def plus_one(x: int) -> int:
    """Return ``x + 1``."""
    calls["plus_one"] += 1
    return x + 1


@function_pod(output_keys="z")
def doubled(y: int) -> int:
    """Return ``2 * y``."""
    calls["doubled"] += 1
    return 2 * y


_FIXED = pa.list_(pa.large_string(), 2)


def _data_tag_stream(tags: tuple[str, ...]) -> ArrowTableStream:
    """Three rows; ``d`` and ``e`` are data-tags, ``d``'s token a fixed-size list."""
    columns: dict[str, tuple[Any, pa.DataType, bool]] = {
        "k": (["a", "b", "c"], pa.large_string(), False),
        "d": ([1, 2, 3], pa.int64(), False),
        "e": ([0.5, 1.5, 2.5], pa.float64(), False),
        "x": ([10, 20, 30], pa.int64(), False),
        "_source_d": ([["p", "q"], ["r", "s"], ["t", "u"]], _FIXED, True),
        "_source_e": (["s::0::e", None, "s::2::e"], pa.large_string(), True),
        "_source_x": (["s::0::x", "s::1::x", "s::2::x"], pa.large_string(), True),
    }
    names = [*tags, *(name for name in columns if name not in tags)]
    table = pa.Table.from_arrays(
        [pa.array(columns[n][0], columns[n][1]) for n in names],
        schema=pa.schema(
            [pa.field(n, columns[n][1], nullable=columns[n][2]) for n in names]
        ),
    )
    return ArrowTableStream(table, tag_columns=list(tags), data_tags=["d", "e"])


def _two_nodes_on(
    route: str, database: InMemoryArrowDatabase, tags: tuple[str, ...]
) -> list[Any]:
    """Run ``plus_one`` then ``doubled`` over the stream on one route; return the rows."""
    from orcapod.core.function_pod import FallThroughPod

    def pod(function: Any) -> Any:
        return FallThroughPod(function.pod, ["d", "e"])

    stream = _data_tag_stream(tags)
    first = FunctionJobNode(pod(plus_one), stream, pipeline_database=database)
    if route == "sync":
        between = first.execute(stream)
        second = FunctionJobNode(pod(doubled), between, pipeline_database=database)
        return list(second.execute(between).iter_data())

    def through(node: Any, rows: list[Any]) -> list[Any]:
        async def run() -> list[Any]:
            incoming: Channel[Any] = Channel(buffer_size=16)
            outgoing: Channel[Any] = Channel(buffer_size=16)
            for row in rows:
                await incoming.writer.send(row)
            await incoming.writer.close()
            await node.async_execute(incoming.reader, outgoing.writer)
            return await outgoing.reader.collect()

        return asyncio.run(run())

    rows = through(first, list(stream.iter_data()))
    second = FunctionJobNode(pod(doubled), first, pipeline_database=database)
    return through(second, rows)


@pytest.mark.parametrize(
    "routes",
    [
        ("sync", "async"),
        ("async", "sync"),
        ("sync", "sync", "async", "async"),
        ("async", "async", "sync", "sync"),
    ],
)
@pytest.mark.parametrize("tags", [("k", "d", "e"), ("e", "k", "d")])
def test_both_routes_record_one_instance_per_row_in_a_shared_store(
    routes: tuple[str, ...], tags: tuple[str, ...]
) -> None:
    """Sessions on either route over one store: one instance id per row, no refusal.

    A data-tag falls through both functions with its token. The token's type is
    the same whether the row was just computed or is served from the stored
    result, and on the sync as on the async route (review of PR #583: the async
    route kept a fixed-size list where the sync edge gave a large list, so one
    function output had two instance ids, and the second route's write was
    refused for the token column's type). Each later session finds the first
    one's records: nothing is computed again and no row is added.
    """
    database = InMemoryArrowDatabase()
    stored: list[tuple[Any, ...]] = []
    for index, route in enumerate(routes):
        out = _two_nodes_on(route, database, tags)
        assert sorted(data.as_dict()["z"] for _, data in out) == [22, 42, 62]
        for _, data in out:
            assert data.source_info_table().schema.field(
                "_source_d"
            ).type == pa.large_list(pa.large_string())
        database.flush()
        (path,) = [
            p
            for p in database.list_sources()
            if p[-1] == "pdb_v2" and p[0] == "doubled"
        ]
        table = database.get_all_records(path)
        assert table is not None
        assert table.num_rows == 3, (index, route)
        assert table.schema.field("_source_d").type == pa.large_list(pa.large_string())
        assert table.column("_source_e").null_count == 1
        stored.append(
            tuple(
                sorted(
                    cast(
                        "list[bytes]",
                        table.column("__pipeline_instance_id").to_pylist(),
                    )
                )
            )
        )
    assert len(set(stored)) == 1 and len(set(stored[0])) == 3
    assert calls["plus_one"] == 3 and calls["doubled"] == 3
