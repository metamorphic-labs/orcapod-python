"""The three row ids as a job stores them (ITL-1471, stage 1 of ITL-1466).

A function node's pipeline table (``pdb_v2``) holds, for every row, its instance id
(``__pipeline_instance_id``) and, as its key, its record id. A side-effect log
(``tdb_v2``) is keyed by the entry id. The operator cache (``odb_v2``) stores the entry
id and an insertion time beside its row key.

The job is ``rows -> f -> g -> tell``: ``f`` has two outputs, so ``g``'s input has two
source-info tokens; ``f``'s results are ephemeral, so a later session over the same
store with a new, empty ephemeral database computes them again and ``g``'s inputs
arrive with new tokens: the same entries, new instances. ``tell`` is a side effect
that tracks completion.

The source has two tags and two data columns, three rows, in two arrangements (tag
order and data order both reversed). Every job test runs on the sync and the async
orchestrator.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import Counter
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.channels import Channel
from orcapod.contexts import get_default_context
from orcapod.core import row_ids
from orcapod.core.datagrams import EmptyData
from orcapod.core.operators import SelectDataColumns
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import EphemeralResultMissingError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.side_effects import SideEffectPod
from orcapod.system_constants import constants
from orcapod.types import CacheMode, ContentHash, NodeConfig

HASHER = get_default_context().arrow_hasher
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}
ORCHESTRATOR = pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
# (tag order, data order): two arrangements of the same rows.
ARRANGEMENTS = {
    "as_declared": (("subject", "session"), ("x", "w")),
    "reversed": (("session", "subject"), ("w", "x")),
}
ARRANGEMENT = pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
COLUMNS: dict[str, "pa.Array[Any]"] = {
    "subject": pa.array(["m1", "m1", "m2"], pa.large_string()),
    "session": pa.array([1, 2, 1], pa.int64()),
    "x": pa.array([10, 20, 30], pa.int64()),
    "w": pa.array([100, 200, 300], pa.int64()),
}
KEY = "__key"

calls: Counter[str] = Counter()
delivered: list[int] = []
invocation_hashes: list[str] = []


@function_pod(output_keys=["y", "v"])
def inc(x: int, w: int) -> tuple[int, int]:
    calls["f"] += 1
    return x + 1, w + 1


@function_pod(output_keys="z")
def add(y: int, v: int) -> int:
    calls["g"] += 1
    return y + v


def _tell(z: int, ctx: Any) -> None:
    delivered.append(z)
    invocation_hashes.append(ctx.invocation_hash)


tell = SideEffectPod(_tell)


@function_pod(output_keys="sent", ctx_arg="ctx")
def notify(y: int, v: int, ctx: Any) -> int:
    invocation_hashes.append(ctx.invocation_hash)
    return y + v


@pytest.fixture(autouse=True)
def _reset() -> None:
    calls.clear()
    delivered.clear()
    invocation_hashes.clear()


def _source(arrangement: str) -> ArrowTableSource:
    tags, data = ARRANGEMENTS[arrangement]
    names = [*tags, *data]
    schema = pa.schema(pa.field(n, COLUMNS[n].type, nullable=False) for n in names)
    return ArrowTableSource(
        pa.Table.from_arrays([COLUMNS[n] for n in names], schema=schema),
        tag_columns=list(tags),
        tag_only_columns=True,
        source_id="rows",
    )


def _job(
    database: InMemoryArrowDatabase,
    ephemeral: InMemoryArrowDatabase,
    arrangement: str = "as_declared",
) -> PipelineJob:
    """``rows -> f -> g -> tell``, ``f``'s results kept in ``ephemeral``."""
    job = PipelineJob(name="ids", database=database)
    with job:
        g: Any = add.pod(inc.pod(_source(arrangement), label="f"), label="g")
        tell.process(g, label="tell")
    job.set_ephemeral_database(ephemeral)
    job.nodes["f"].node_config = NodeConfig(is_result_ephemeral=True)  # pyright: ignore[reportAttributeAccessIssue]
    return job


def _run(job: PipelineJob, orchestrator: str) -> None:
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())


def _table(database: InMemoryArrowDatabase, *parts: str) -> pa.Table:
    """The one stored table whose path holds every one of ``parts``, with its key."""
    (path,) = [p for p in database.list_sources() if all(part in p for part in parts)]
    records = database.get_all_records(path, record_id_column=KEY)
    assert records is not None
    return records


def _user_and_system_tags(row: pa.Table) -> list[str]:
    return [
        name
        for name in row.column_names
        if name in ("subject", "session")
        or name.startswith(constants.SYSTEM_TAG_PREFIX)
    ]


def _entry_id_of_stored_row(row: pa.Table) -> ContentHash:
    """The entry id, from what a pipeline row stores: tags, system tags, input hash."""
    preimage = row.select(_user_and_system_tags(row)).append_column(
        "__input_data_hash", row.column("__input_data_hash")
    )
    return HASHER.hash_table(preimage)


def _instance_id_of_stored_row(row: pa.Table) -> ContentHash:
    """The instance id, from a stored row: its entry id and its tokens in name order."""
    preimage = pa.table(
        {
            "__pipeline_entry_id": pa.array(
                [_entry_id_of_stored_row(row).to_prefixed_digest()], pa.large_binary()
            )
        }
    )
    for name in sorted(n for n in row.column_names if n.startswith("_source_")):
        preimage = preimage.append_column(row.schema.field(name), row.column(name))
    return HASHER.hash_table(preimage)


def _record_id_of_stored_row(row: pa.Table) -> ContentHash:
    """The record id, from a stored row: its instance id and its result id."""
    return HASHER.hash_table(row.select(["__pipeline_instance_id", "__data_id"]))


def _values(table: pa.Table, column: str) -> list[Any]:
    """The values of one column, in row order."""
    return cast("list[Any]", table.column(column).to_pylist())


def _rows(table: pa.Table) -> list[pa.Table]:
    return [table.slice(i, 1) for i in range(table.num_rows)]


# ---------------------------------------------------------------------------
# The pipeline table: instance id and record id of every row
# ---------------------------------------------------------------------------


@ORCHESTRATOR
@ARRANGEMENT
@pytest.mark.parametrize("function", ["inc", "add"])
def test_every_pipeline_row_gives_its_instance_and_record_id_again(
    orchestrator: str, arrangement: str, function: str
) -> None:
    """Recomputed from the columns a row stores, each id is the stored one."""
    database = InMemoryArrowDatabase()
    _run(_job(database, InMemoryArrowDatabase(), arrangement), orchestrator)
    stored = _table(database, function, "pdb_v2")
    assert stored.num_rows == 3
    assert "__pipeline_entry_id" not in stored.column_names
    assert "__pipeline_recomputation_index" not in stored.column_names
    assert "__pipeline_base_entry_id" not in stored.column_names
    tokens = sorted(n for n in stored.column_names if n.startswith("_source_"))
    assert (
        tokens
        == {"inc": ["_source_w", "_source_x"], "add": ["_source_v", "_source_y"]}[
            function
        ]
    )
    for row in _rows(stored):
        (instance,) = _values(row, "__pipeline_instance_id")
        (key,) = _values(row, KEY)
        assert _instance_id_of_stored_row(row).to_prefixed_digest() == instance
        assert _record_id_of_stored_row(row).to_prefixed_digest() == key
        # The same through the one module, from the stored instance and result ids.
        (result_id,) = _values(row, "__data_id")
        assert (
            row_ids.record_id(
                HASHER,
                ContentHash.from_prefixed_digest(instance),
                uuid.UUID(bytes=result_id),
            ).to_prefixed_digest()
            == key
        )
    assert len(set(_values(stored, KEY))) == 3
    assert len(set(_values(stored, "__pipeline_instance_id"))) == 3


@ORCHESTRATOR
@ARRANGEMENT
def test_the_stored_instance_ids_are_those_of_the_rows_the_node_was_given(
    orchestrator: str, arrangement: str
) -> None:
    """``g``'s stored instance ids are ``row_ids.instance_id`` of ``f``'s output rows."""
    database = InMemoryArrowDatabase()
    job = _job(database, InMemoryArrowDatabase(), arrangement)
    _run(job, orchestrator)
    given = {
        row_ids.instance_id(HASHER, tag, data).to_prefixed_digest()
        for tag, data in job.nodes["f"].iter_data()
    }
    stored = _table(database, "add", "pdb_v2")
    assert set(_values(stored, "__pipeline_instance_id")) == given
    assert len(given) == 3


def test_the_two_arrangements_store_the_same_instance_ids() -> None:
    """Neither the tag order nor the order of the token columns counts."""
    seen = []
    for arrangement in ARRANGEMENTS:
        database = InMemoryArrowDatabase()
        _run(_job(database, InMemoryArrowDatabase(), arrangement), "sync")
        seen.append(
            sorted(_values(_table(database, "inc", "pdb_v2"), "__pipeline_instance_id"))
        )
    assert seen[0] == seen[1]


# ---------------------------------------------------------------------------
# Side effects and the invocation context: the entry id
# ---------------------------------------------------------------------------


@ORCHESTRATOR
@ARRANGEMENT
def test_the_side_effect_log_is_keyed_by_the_entry_ids_of_its_rows(
    orchestrator: str, arrangement: str
) -> None:
    """The log's keys and its ``entry_id`` column are the entry ids of ``g``'s rows."""
    database = InMemoryArrowDatabase()
    job = _job(database, InMemoryArrowDatabase(), arrangement)
    _run(job, orchestrator)
    entries = {
        row_ids.entry_id(HASHER, tag, data).to_prefixed_digest()
        for tag, data in job.nodes["g"].iter_data()
    }
    assert len(entries) == 3
    log = _table(database, "tdb_v2")
    assert log.column_names[:1] == [KEY]
    assert "entry_id" in log.column_names
    assert "record_id_hash" not in log.column_names
    assert _values(log, KEY) == _values(log, "entry_id")
    assert set(_values(log, KEY)) == entries
    assert sorted(delivered) == [112, 222, 332]


@ORCHESTRATOR
def test_the_invocation_hash_of_a_side_effect_ends_in_the_entry_id(
    orchestrator: str,
) -> None:
    """``invocation_hash`` is ``pipeline hash :: entry id``, both with their labels."""
    database = InMemoryArrowDatabase()
    job = _job(database, InMemoryArrowDatabase())
    _run(job, orchestrator)
    entries = {
        row_ids.entry_id(HASHER, tag, data).to_string()
        for tag, data in job.nodes["g"].iter_data()
    }
    assert len(invocation_hashes) == 3
    parts = [value.split("::") for value in invocation_hashes]
    assert {len(p) for p in parts} == {2}
    assert {p[0] for p in parts} == {job.nodes["tell"].pipeline_hash().to_string()}
    assert {p[1] for p in parts} == entries


@ORCHESTRATOR
def test_the_invocation_hash_of_a_function_with_ctx_ends_in_the_entry_id(
    orchestrator: str,
) -> None:
    """A function that takes the invocation context is keyed by the entry id too."""
    job = PipelineJob(name="ctx", database=InMemoryArrowDatabase())
    with job:
        f: Any = inc.pod(_source("as_declared"), label="f")
        notify.pod(f, label="notify")
    _run(job, orchestrator)
    entries = {
        row_ids.entry_id(HASHER, tag, data).to_string()
        for tag, data in job.nodes["f"].iter_data()
    }
    assert len(entries) == 3
    assert {value.split("::")[1] for value in invocation_hashes} == entries
    assert {len(value.split("::")) for value in invocation_hashes} == {2}


# ---------------------------------------------------------------------------
# A token-only change upstream: new instances, nothing computed, nothing delivered
# ---------------------------------------------------------------------------


@ORCHESTRATOR
@ARRANGEMENT
def test_a_token_only_change_records_new_instances_and_computes_and_delivers_nothing(
    orchestrator: str, arrangement: str
) -> None:
    database = InMemoryArrowDatabase()
    _run(_job(database, InMemoryArrowDatabase(), arrangement), orchestrator)
    assert (calls["f"], calls["g"], len(delivered)) == (3, 3, 3)
    g_before = _table(database, "add", "pdb_v2")
    f_before = _table(database, "inc", "pdb_v2")
    log_before = _table(database, "tdb_v2")

    # A later session: the ephemeral results of ``f`` are gone, so ``f`` computes
    # them again; ``g``'s inputs have the same values and new tokens.
    later = InMemoryArrowDatabase()
    _run(_job(database, later, arrangement), orchestrator)
    assert calls["f"] == 6
    assert calls["g"] == 3, "g was called again for an input it has a result for"
    assert len(delivered) == 3, "a side effect was delivered again"

    g_after = _table(database, "add", "pdb_v2")
    assert g_after.num_rows == 6
    assert len(set(_values(g_after, "__pipeline_instance_id"))) == 6
    assert len(set(_values(g_after, KEY))) == 6
    # Three entries, each with two instances; both instances name one result.
    entries = Counter(
        _entry_id_of_stored_row(row).to_prefixed_digest() for row in _rows(g_after)
    )
    assert sorted(entries.values()) == [2, 2, 2]
    assert len(set(_values(g_after, "__data_id"))) == 3
    new = [
        row
        for row in _rows(g_after)
        if _values(row, KEY)[0] not in _values(g_before, KEY)
    ]
    assert len(new) == 3
    for row in new:
        assert _values(row, "__computed") == [False]
        assert (
            _instance_id_of_stored_row(row).to_prefixed_digest()
            == (_values(row, "__pipeline_instance_id")[0])
        )
    # The first session's rows are untouched.
    kept = g_after.filter(pc.is_in(g_after.column(KEY), value_set=g_before.column(KEY)))
    assert sorted(_values(kept, "__recorded_at")) == sorted(
        _values(g_before, "__recorded_at")
    )

    # ``f`` recomputed: a new row per instance, under the same instance id.
    f_after = _table(database, "inc", "pdb_v2")
    assert f_after.num_rows == 6
    assert set(_values(f_after, "__pipeline_instance_id")) == set(
        _values(f_before, "__pipeline_instance_id")
    )
    assert len(set(_values(f_after, KEY))) == 6
    assert len(set(_values(f_after, "__data_id"))) == 6

    # The log holds the three entries, as before.
    log_after = _table(database, "tdb_v2")
    assert sorted(_values(log_after, KEY)) == sorted(_values(log_before, KEY))


@ORCHESTRATOR
def test_an_identical_repeat_writes_no_row(orchestrator: str) -> None:
    """A later session whose inputs are the stored instances writes nothing."""
    database, ephemeral = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    _run(_job(database, ephemeral), orchestrator)
    before = {
        name: _table(database, name, "pdb_v2").sort_by(KEY) for name in ("inc", "add")
    }
    _run(_job(database, ephemeral), orchestrator)
    assert (calls["f"], calls["g"], len(delivered)) == (3, 3, 3)
    for name, table in before.items():
        assert _table(database, name, "pdb_v2").sort_by(KEY).equals(table), name


def test_recording_one_instance_and_result_again_is_skipped() -> None:
    """The same instance and result written again keeps the one row and its time."""
    database = InMemoryArrowDatabase()
    job = _job(database, InMemoryArrowDatabase())
    _run(job, "sync")
    node: Any = job.nodes["g"]
    before = _table(database, "add", "pdb_v2").sort_by(KEY)
    result_ids = {
        instance: uuid.UUID(bytes=result_id)
        for instance, result_id in zip(
            _values(before, "__pipeline_instance_id"),
            _values(before, "__data_id"),
        )
    }
    for tag, data in job.nodes["f"].iter_data():
        node.add_pipeline_record(
            tag,
            data,
            data_record_id=result_ids[node.compute_instance_id(tag, data)],
            computed=False,
        )
    database.flush()
    assert _table(database, "add", "pdb_v2").sort_by(KEY).equals(before)


# ---------------------------------------------------------------------------
# The operator cache: an entry id and an insertion time beside the row key
# ---------------------------------------------------------------------------


def _operator_job(database: InMemoryArrowDatabase, arrangement: str) -> PipelineJob:
    """``rows -> f -> sel``, ``sel`` a stored (``LOG``) operator over both outputs."""
    job = PipelineJob(name="ops", database=database)
    with job:
        f: Any = inc.pod(_source(arrangement), label="f")
        SelectDataColumns(["v", "y"])(f, label="sel")
    job.nodes["sel"].attach_databases(  # pyright: ignore[reportAttributeAccessIssue]
        pipeline_database=database.at("ops"), cache_mode=CacheMode.LOG
    )
    return job


@ORCHESTRATOR
@ARRANGEMENT
def test_the_operator_cache_stores_the_entry_id_of_every_row(
    orchestrator: str, arrangement: str
) -> None:
    database = InMemoryArrowDatabase()
    job = _operator_job(database, arrangement)
    _run(job, orchestrator)
    stored = _table(database, "odb_v2")
    assert stored.num_rows == 3
    assert stored.schema.field("__pipeline_entry_id").type == pa.large_binary()
    assert not stored.schema.field("__pipeline_entry_id").nullable
    assert stored.schema.field("__recorded_at").type == pa.timestamp("us", tz="UTC")
    assert not stored.schema.field("__recorded_at").nullable
    by_subject_session = {
        (tag.as_dict()["subject"], tag.as_dict()["session"]): row_ids.entry_id(
            HASHER, tag, data
        ).to_prefixed_digest()
        for tag, data in job.nodes["sel"].iter_data()
    }
    assert len(set(by_subject_session.values())) == 3
    for row in stored.to_pylist():
        assert (
            row["__pipeline_entry_id"]
            == by_subject_session[(row["subject"], row["session"])]
        )


@ORCHESTRATOR
def test_neither_stored_column_is_part_of_the_row_key(orchestrator: str) -> None:
    """The row key is the hash of the row without the entry id and the time."""
    database = InMemoryArrowDatabase()
    _run(_operator_job(database, "as_declared"), orchestrator)
    stored = _table(database, "odb_v2")
    hashed = stored.drop(
        [KEY, "__pipeline_entry_id", "__recorded_at", constants.DATA_CONTEXT_COL]
    )
    for i in range(stored.num_rows):
        assert (
            HASHER.hash_table(hashed.slice(i, 1)).to_prefixed_digest()
            == _values(stored, KEY)[i]
        )


@ORCHESTRATOR
def test_a_row_passed_again_keeps_its_first_insertion_time(orchestrator: str) -> None:
    database = InMemoryArrowDatabase()
    job = _operator_job(database, "as_declared")
    _run(job, orchestrator)
    first = _table(database, "odb_v2").sort_by(KEY)
    # The same job runs again over the same rows (cleared, or nothing would run).
    for label in ("f", "sel"):
        job.nodes[label].clear_cache()  # pyright: ignore[reportAttributeAccessIssue]
    _run(job, orchestrator)
    assert _table(database, "odb_v2").sort_by(KEY).equals(first)
    assert calls["f"] == 3


@ORCHESTRATOR
def test_no_read_of_the_operator_node_shows_the_stored_only_columns(
    orchestrator: str,
) -> None:
    database = InMemoryArrowDatabase()
    job = _operator_job(database, "as_declared")
    _run(job, orchestrator)
    node: Any = job.nodes["sel"]
    all_records = node.get_all_records(all_info=True)
    assert all_records is not None
    tables = {
        "as_table": node.as_table(all_info=True),
        "get_all_records": all_records,
        "as_source": node.as_source().as_table(all_info=True),
        "head": node.head().table,
    }
    for name, table in tables.items():
        assert "__pipeline_entry_id" not in table.column_names, name
        assert "__recorded_at" not in table.column_names, name
        assert table.num_rows == 3, name

    # A REPLAY node over the same cache serves the rows without them too.
    replay_job = _operator_job(database, "as_declared")
    replayed: Any = replay_job.nodes["sel"]
    replayed.attach_databases(
        pipeline_database=database.at("ops"), cache_mode=CacheMode.REPLAY
    )
    _run(replay_job, orchestrator)
    replay_table = replayed.as_table(all_info=True)
    assert "__pipeline_entry_id" not in replay_table.column_names
    assert "__recorded_at" not in replay_table.column_names
    assert sorted(_values(replay_table, "y")) == [11, 21, 31]
    assert sorted(_values(replay_table, "v")) == [101, 201, 301]


# ---------------------------------------------------------------------------
# The placeholder of an ephemeral result that is gone
# ---------------------------------------------------------------------------


def _placeholders(job: PipelineJob) -> list[tuple[Any, Any]]:
    """``f``'s entries for its input rows, as its cache facade returns them."""
    node: Any = job.nodes["f"]
    ids = [
        node.compute_instance_id(tag, data)
        for tag, data in node._input_stream.iter_data()
    ]
    return list(node.get_cached_results(ids).values())


@ARRANGEMENT
def test_a_placeholder_carries_a_real_outputs_tokens_with_the_sentinel(
    arrangement: str,
) -> None:
    database = InMemoryArrowDatabase()
    first = _job(database, InMemoryArrowDatabase(), arrangement)
    _run(first, "sync")
    real_tokens = [data.source_info() for _, data in first.nodes["f"].iter_data()]

    later = _job(database, InMemoryArrowDatabase(), arrangement)
    placeholders = _placeholders(later)
    assert len(placeholders) == 3
    uri = ":".join(later.nodes["f"].node_uri)  # pyright: ignore[reportAttributeAccessIssue]
    for _, data in placeholders:
        assert isinstance(data, EmptyData)
        assert data.source_info() == {
            "y": f"{uri}::EPHEMERAL_RESULT_GONE::y",
            "v": f"{uri}::EPHEMERAL_RESULT_GONE::v",
        }
    # A real output's token differs only where its result id stands.
    for tokens in real_tokens:
        assert list(tokens) == ["y", "v"]
        for column, token in tokens.items():
            source, result_id, last = token.split("::")
            assert (source, last) == (uri, column)
            assert len(result_id) == 32 and result_id != "EPHEMERAL_RESULT_GONE"
            assert uuid.UUID(hex=result_id).hex == result_id


@ARRANGEMENT
def test_a_node_given_a_placeholder_finds_its_result_by_the_input_hash(
    arrangement: str,
) -> None:
    """0 calls, the stored result, no pipeline row, and another instance id.

    Row by row, through ``execute_data``; the async route is the test below.
    """
    database = InMemoryArrowDatabase()
    first = _job(database, InMemoryArrowDatabase(), arrangement)
    _run(first, "sync")
    expected = {
        (tag.as_dict()["subject"], tag.as_dict()["session"]): data.as_dict()["z"]
        for tag, data in first.nodes["g"].iter_data()
    }
    stored_before = _table(database, "add", "pdb_v2").sort_by(KEY)
    stored_instances = set(_values(stored_before, "__pipeline_instance_id"))
    calls.clear()

    later = _job(database, InMemoryArrowDatabase(), arrangement)
    node: Any = later.nodes["g"]
    for tag, placeholder in _placeholders(later):
        # What looks up what: the instance id of the placeholder is not a stored
        # one (its tokens hold the sentinel), so the lookup by instance id misses.
        instance = node.compute_instance_id(tag, placeholder)
        assert instance not in stored_instances
        assert (
            instance
            == row_ids.instance_id(HASHER, tag, placeholder).to_prefixed_digest()
        )
        assert node.get_cached_results([instance]) == {}
        # The result store answers by the input hash the placeholder keeps.
        _, result = node.execute_data(tag, placeholder)
        assert result is not None
        key = (tag.as_dict()["subject"], tag.as_dict()["session"])
        assert result.as_dict()["z"] == expected[key]
    assert calls == Counter(), "a function was called for a placeholder input"
    database.flush()
    # No pipeline row is written for a placeholder input (ITL-1475 reconsiders it).
    assert _table(database, "add", "pdb_v2").sort_by(KEY).equals(stored_before)


def _through_channels(node: Any, rows: list[tuple[Any, Any]]) -> list[tuple[Any, Any]]:
    """Feed ``rows`` to ``node.async_execute`` through channels; return what it sends."""

    async def run() -> list[tuple[Any, Any]]:
        incoming: Channel[Any] = Channel()
        outgoing: Channel[Any] = Channel()
        for row in rows:
            await incoming.writer.send(row)
        await incoming.writer.close()
        await node.async_execute(incoming.reader, outgoing.writer)
        return await outgoing.reader.collect()

    return asyncio.run(run())


@ARRANGEMENT
def test_placeholders_fed_through_the_async_route_find_their_results(
    arrangement: str,
) -> None:
    """``async_execute`` given placeholders: the stored results, 0 calls, no new row.

    Review of PR #583: the async route sent a placeholder whose instance id is not
    stored to the function like any miss, and the pipeline writer then read the
    placeholder as a table and raised ``EmptyDataAccessError``, although the result
    was stored. It now finds the result by the input hash, as the sync route does,
    and writes no pipeline row (ITL-1475).
    """
    database = InMemoryArrowDatabase()
    first = _job(database, InMemoryArrowDatabase(), arrangement)
    _run(first, "sync")
    expected = {
        (tag.as_dict()["subject"], tag.as_dict()["session"]): data.as_dict()["z"]
        for tag, data in first.nodes["g"].iter_data()
    }
    stored_before = _table(database, "add", "pdb_v2").sort_by(KEY)
    calls.clear()

    later = _job(database, InMemoryArrowDatabase(), arrangement)
    node: Any = later.nodes["g"]
    placeholders = _placeholders(later)
    assert len(placeholders) == 3
    assert all(isinstance(data, EmptyData) for _, data in placeholders)
    sent = _through_channels(node, placeholders)
    assert {
        (tag.as_dict()["subject"], tag.as_dict()["session"]): data.as_dict()["z"]
        for tag, data in sent
    } == expected
    assert len(sent) == 3
    assert calls == Counter(), "a function was called for a placeholder input"
    database.flush()
    assert _table(database, "add", "pdb_v2").sort_by(KEY).equals(stored_before)


@pytest.mark.parametrize("path", ["sync", "async"])
def test_a_placeholder_whose_result_is_not_stored_is_refused(path: str) -> None:
    """No stored result for the placeholder's input hash: an error, 0 calls, no row."""
    database = InMemoryArrowDatabase()
    _run(_job(database, InMemoryArrowDatabase()), "sync")
    stored_before = _table(database, "add", "pdb_v2").sort_by(KEY)
    calls.clear()

    later = _job(database, InMemoryArrowDatabase())
    node: Any = later.nodes["g"]
    # Placeholders that stand for results ``g`` never saw: ``f``'s own input hashes.
    unknown = [
        (tag, EmptyData(cached_content_hash=data.content_hash()))
        for tag, data in later.nodes["f"]._input_stream.iter_data()
    ]
    assert len(unknown) == 3
    if path == "sync":
        with pytest.raises(EphemeralResultMissingError):
            node.execute_data(*unknown[0])
    else:
        with pytest.raises((EphemeralResultMissingError, ExceptionGroup)) as raised:
            _through_channels(node, unknown)
        error = raised.value
        if isinstance(error, ExceptionGroup):
            assert error.subgroup(EphemeralResultMissingError) is not None
    assert calls == Counter()
    database.flush()
    assert _table(database, "add", "pdb_v2").sort_by(KEY).equals(stored_before)


# ---------------------------------------------------------------------------
# A trace finds the record of the row's own instance
# ---------------------------------------------------------------------------


@ORCHESTRATOR
@ARRANGEMENT
def test_each_jobs_trace_finds_its_own_record_after_a_token_only_rerun(
    orchestrator: str, arrangement: str
) -> None:
    """Two jobs over one store, the second with new tokens: both still trace.

    After the second job, ``g``'s table holds two records of every entry: the first
    job's instance and the second's, both naming one result. A trace resolves a held
    row by its instance (the entry with the row's own input tokens), so each job's
    trace reports its own record. Review of PR #583: the trace matched on system
    tags and result id alone, found both records and raised
    ``ProvenanceInconsistencyError`` for every row of the first job.
    """
    database = InMemoryArrowDatabase()
    first = _job(database, InMemoryArrowDatabase(), arrangement)
    _run(first, orchestrator)
    before = {
        tuple(sorted(root.tags)): root for root in first.nodes["g"].trace({}, "z").roots
    }
    assert len(before) == 3
    second = _job(database, InMemoryArrowDatabase(), arrangement)
    _run(second, orchestrator)
    stored = _table(database, "add", "pdb_v2")
    assert stored.num_rows == 6

    records: dict[str, set[str]] = {}
    for name, job in (("first", first), ("second", second)):
        trace = job.nodes["g"].trace({}, "z")
        assert trace.matched == 3, name
        assert len(trace.roots) == 3, name
        records[name] = set()
        for root in trace.roots:
            assert root.node == "g" and root.function is not None, name
            records[name].add(root.function.record_id)
            # The first job computed its rows; the second took the stored results.
            assert root.function.computed_here is (name == "first"), name
            # Every input of the step is traced down to the source.
            assert {step.kind for step in root.walk()} >= {"function", "source"}
        assert len(records[name]) == 3, name
    assert records["first"].isdisjoint(records["second"])
    labelled = {
        ContentHash.from_prefixed_digest(k).to_string() for k in _values(stored, KEY)
    }
    assert records["first"] | records["second"] == labelled
    # The first job's trace is what it was before the second job ran.
    again = {
        tuple(sorted(root.tags)): root for root in first.nodes["g"].trace({}, "z").roots
    }
    assert again == before

    # One row, selected by its tags, traces in both jobs too.
    selector = {"subject": "m1", "session": 2}
    for job in (first, second):
        (root,) = job.nodes["g"].trace(selector, "z").roots
        assert root.value == 222
